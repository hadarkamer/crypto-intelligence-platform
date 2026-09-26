"""One durable Testnet worker for delivered LONG cards.

The producer selects formulas. This worker resumes every owned market bucket
before considering fresh cards. It never takes execution instructions from the
application or an HTTP request. Disabling new entries keeps exits running.
"""
from copy import deepcopy
from datetime import datetime, timezone
import json
import os
import threading

from . import approved_alert_selection as selection, card_lifecycle as life
from . import filled_quantity_dispatch as dispatch, two_account_execution as roles
from .filled_dispatch_store import DispatchError
from .source_window import source_fresh, timestamp
from .trade_card_store import CardStore
from . import trade_cards

MODE = 'long_stream_testnet_v1'
RELEASE = 'approved_long_stream_v1'
SOURCE = 'approved_alerts_v1'
_stop = threading.Event()
_lock = threading.Lock()
_thread = None
_app_thread = None
_health = dict(configured=False, running=False, last_status='DISABLED',
               cycles=0, order_requests_sent=0, new_entries_enabled=False,
               app_delivery_status='DISABLED')


def configuration(env):
    if (env.get('RENDER_SERVICE_ID') != roles.SERVICE
            or env.get('HL_TESTNET_RUNTIME_MODE') != MODE
            or env.get('HL_TESTNET_FILLED_DISPATCH') != RELEASE
            or env.get('HL_TESTNET_LONG_STREAM') != SOURCE
            or env.get('HL_TESTNET_LONG_ENTRY_ENABLED') not in ('true','false')
            or env.get('HL_TESTNET_TWO_ACCOUNT_EXECUTION') != 'disabled'
            or env.get('HL_TESTNET_JOURNAL_BACKEND') != 'staging_postgres_v1'
            or env.get('HL_TESTNET_FILLED_AFTER_EXIT_POLICY') != dispatch.AFTER_EXIT
            or env.get('HL_TESTNET_SAFETY_PIPELINE')
            or env.get('HL_TESTNET_CARD_SYNC')
            or env.get('HL_TESTNET_FILLED_AUTOWAIT')
            or env.get('HL_TESTNET_FILLED_CARD_ID')
            or env.get('HL_TESTNET_CARDS_PHASE1') != 'record_only_v1'
            or env.get('HL_TESTNET_CARDS_INTAKE') != 'record_only_v1'):
        raise DispatchError('LONG_STREAM_CONFIGURATION_REQUIRED')
    from .alert_cards_intake import enabled as intake_enabled
    if not intake_enabled(env):
        raise DispatchError('LONG_STREAM_AUTHENTICATED_INTAKE_REQUIRED')
    try:
        start = timestamp(env['HL_TESTNET_LONG_NOT_BEFORE'])
    except (KeyError, TypeError, ValueError):
        raise DispatchError('LONG_STREAM_START_TIME_REQUIRED') from None
    if start > datetime.now(timezone.utc):
        raise DispatchError('LONG_STREAM_START_IN_FUTURE')
    route = roles.route_for(env, 'long_account', side='LONG')
    return route, start


def _unfinished(state, now):
    if state['pending'] is not None:
        return True
    if state['bindings']:
        if state['evidence'] is None:
            return True
        snap = state['evidence']['snapshot']
        view = life.review(state['bindings'],snap,now_ms=snap['at_ms'])
        if (view['bucket_issues'] or snap['open_orders']
                or life.number(snap['position_quantity'],signed=True) != 0
                or any(c['state'] not in ('CLOSED','CANCELED_WITHOUT_FILL')
                       or c['issues'] for c in view['cards'])):
            return True
    bound = {b['card_id'] for b in state['bindings']}
    return any(cid not in bound and
        'source_expires_at' in original['card'] and
        source_fresh(timestamp(original['card']['prepared']['source']['at']),
                     original['card']['source_expires_at'], now=now)
        for cid,original in state['originals'].items())


def _account_owned(venue, account, states):
    """Unknown positions/orders block *new* entries, never exit maintenance."""
    from .card_sync_evidence import PublicReader
    reader = PublicReader()
    orders = reader.read('frontendOpenOrders',account)
    positions = reader.read('clearinghouseState',account)
    if (not isinstance(orders,list) or len(orders)>10000
            or not isinstance(positions,dict)
            or not isinstance(positions.get('assetPositions'),list)):
        raise DispatchError('LONG_ACCOUNT_INVENTORY_INVALID')
    by_symbol = {s['symbol']:s for s in states}
    if len(by_symbol) != len(states):
        raise DispatchError('DUPLICATE_MARKET_BUCKET')
    known = {(s['symbol'],oid) for s in states for b in s['bindings']
             for leg in life.LEGS for oid in b['orders'][leg]}
    if any(s['pending'] is not None for s in states):
        raise DispatchError('UNRESOLVED_ACCOUNT_REQUEST_NO_NEW_ENTRY')
    for row in orders:
        if (not isinstance(row,dict) or not isinstance(row.get('coin'),str)
                or type(row.get('oid')) is not int
                or row['coin'] not in by_symbol
                or (row['coin'],str(row['oid'])) not in known):
            raise DispatchError('UNOWNED_ACCOUNT_ORDER_NO_NEW_ENTRY')
    seen=set()
    for row in positions['assetPositions']:
        if (not isinstance(row,dict) or not isinstance(row.get('position'),dict)
                or not isinstance(row['position'].get('coin'),str)):
            raise DispatchError('LONG_ACCOUNT_POSITION_INVALID')
        p=row['position']
        if p['coin'] in seen:
            raise DispatchError('DUPLICATE_ACCOUNT_POSITION')
        seen.add(p['coin'])
        if (life.number(p.get('szi'),signed=True)!=0
                and (p['coin'] not in by_symbol or not by_symbol[p['coin']]['bindings'])):
            raise DispatchError('UNOWNED_ACCOUNT_POSITION_NO_NEW_ENTRY')
    return True


def tick(controller, route, not_before, *, new_entries):
    """One bounded sweep. Every old exposure is serviced before new cards."""
    now = datetime.fromtimestamp(controller.venue.now()/1000,timezone.utc)
    states = controller.store.for_account(route['account'])
    sent = 0
    errors = 0
    for state in states:
        try:
            if not _unfinished(state,now):
                continue
            result = controller.cycle(state['bucket'],send=True,
                                      allow_new_entries=False)
            sent += result['order_requests_sent']
        except Exception:
            errors += 1
    if errors or not new_entries:
        return dict(status='EXISTING_RECONCILIATION_REQUIRED' if errors else 'ENTRIES_DISABLED',
                    active_buckets=len(states),
                    order_requests_sent=sent,new_cards_registered=0)
    states = controller.store.for_account(route['account'])
    _account_owned(controller.venue,route['account'],states)
    known_cards = {cid for state in states for cid in state['originals']}
    touched = [state['bucket'] for state in states if state['pending'] is None
               and any(cid not in {b['card_id'] for b in state['bindings']}
                       and 'source_expires_at' in original['card']
                       and source_fresh(timestamp(original['card']['prepared']['source']['at']),
                           original['card']['source_expires_at'],now=now)
                       for cid,original in state['originals'].items())]
    cursor = None
    registered = 0
    for _ in range(100):
        candidates,cursor = selection.page(controller.store.journal,
            not_before=not_before.isoformat(),now=now,after=cursor)
        for cid,role in candidates:
            if role!='long_account' or cid in known_cards:
                continue
            card=CardStore(controller.store.journal).load(cid)
            if card['account_role']!='long_account':
                raise DispatchError('LONG_SOURCE_ROLE_CHANGED')
            try:
                state=controller.register(cid)
            except DispatchError as exc:
                if str(exc) in ('ORIGINAL_SOURCE_EXPIRED',
                                'REGISTER_WHILE_REQUEST_UNRESOLVED'):
                    continue
                raise
            if state['account']!=route['account']:
                raise DispatchError('LONG_CARD_ACCOUNT_MISMATCH')
            known_cards.add(cid)
            registered += 1
            if state['bucket'] not in touched:
                touched.append(state['bucket'])
        if cursor is None:
            break
    else:
        raise DispatchError('LONG_ALERT_SCAN_BUDGET_REQUIRES_REVIEW')
    for bucket in touched:
        result=controller.cycle(bucket,send=True)
        sent += result['order_requests_sent']
    return dict(status='SWEEP_COMPLETE',active_buckets=len(states),
                order_requests_sent=sent,new_cards_registered=registered)


def _loop(controller, route, start):
    while not _stop.is_set():
        before=getattr(controller.venue,'sent',0)
        try:
            result=tick(controller,route,start,
                        new_entries=controller.venue.env['HL_TESTNET_LONG_ENTRY_ENABLED']=='true')
        except Exception:
            result=dict(status='RECONCILIATION_REQUIRED_NO_BLIND_RETRY',
                order_requests_sent=max(0,getattr(controller.venue,'sent',0)-before),
                new_cards_registered=0)
        with _lock:
            _health['cycles'] += 1
            _health['last_status']=result['status']
            _health['order_requests_sent']=getattr(controller.venue,'sent',0)
        print(json.dumps({'testnet_long_stream':result},sort_keys=True),flush=True)
        _stop.wait(2 if result['status']=='SWEEP_COMPLETE' else 10)
    with _lock:
        _health['running']=False


def _app_loop(controller, route, private_key):
    from .app_card_delivery import Publisher
    publisher = Publisher()
    while not _stop.is_set():
        try:
            count = publisher.pass_once(controller, route, private_key)
            status = publisher.last_status
        except Exception:
            count = 0
            status = 'DELIVERY_UNAVAILABLE_RETRY'
        with _lock:
            _health['app_delivery_status'] = status
        print(json.dumps({'testnet_app_delivery': {'status':status,
            'cards_sent':count,'order_requests_sent':0}},sort_keys=True),flush=True)
        _stop.wait(10 if status != 'DELIVERY_UNAVAILABLE_RETRY' else 30)


def start():
    global _thread,_app_thread
    env=dict(os.environ)
    route,not_before=configuration(env)
    app_mode=env.get('HL_TESTNET_APP_DELIVERY','')
    if app_mode not in ('','ed25519_signed_v1'):
        raise DispatchError('APP_DELIVERY_MODE_INVALID')
    private_key=env.get('HL_TESTNET_APP_SIGNING_KEY','') if app_mode else ''
    if app_mode and not private_key:
        raise DispatchError('APP_DELIVERY_SIGNING_KEY_REQUIRED')
    controller=dispatch.controller_from_env(env)
    with controller.store.journal._transaction() as conn:
        controller.store.ready(conn)
        CardStore(controller.store.journal).ready(conn)
    from .alert_cards_intake import initialize as initialize_intake
    initialize_intake(controller.store.journal)
    controller.store.for_account(route['account'])
    with _lock:
        if _thread is not None and _thread.is_alive():
            return False
        _stop.clear()
        _health.update(configured=True,running=True,last_status='STARTING',cycles=0,
                       order_requests_sent=0,
                       new_entries_enabled=env['HL_TESTNET_LONG_ENTRY_ENABLED']=='true',
                       app_delivery_status='STARTING' if app_mode else 'DISABLED')
        _thread=threading.Thread(target=_loop,args=(controller,route,not_before),
                                 daemon=True,name='long-testnet-card-stream')
        _thread.start()
        if app_mode:
            _app_thread=threading.Thread(target=_app_loop,args=(controller,route,private_key),
                daemon=True,name='testnet-app-card-delivery')
            _app_thread.start()
    return True


def stop():
    _stop.set()


def health():
    with _lock:
        return deepcopy(_health)
