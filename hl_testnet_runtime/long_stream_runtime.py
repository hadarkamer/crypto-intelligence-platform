"""One durable Testnet worker for delivered cards in two separate accounts.

The producer selects formulas. This worker resumes every owned market bucket
before considering fresh cards. It never takes execution instructions from the
application or an HTTP request. Disabling new entries keeps exits running.
"""
from copy import deepcopy
from datetime import datetime, timezone
import json
import os
import re
import threading
import traceback

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
               short_entries_enabled=False, short_stream_configured=False,
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


def short_configuration(env):
    """Absence preserves the deployed long-only release; a partial opt-in fails."""
    keys = ('HL_TESTNET_SHORT_STREAM', 'HL_TESTNET_SHORT_ENTRY_ENABLED',
            'HL_TESTNET_SHORT_NOT_BEFORE')
    if not any(env.get(key) for key in keys):
        return None
    if (env.get('HL_TESTNET_SHORT_STREAM') != SOURCE
            or env.get('HL_TESTNET_SHORT_ENTRY_ENABLED') not in ('true', 'false')):
        raise DispatchError('SHORT_STREAM_CONFIGURATION_REQUIRED')
    try:
        start = timestamp(env['HL_TESTNET_SHORT_NOT_BEFORE'])
    except (KeyError, TypeError, ValueError):
        raise DispatchError('SHORT_STREAM_START_TIME_REQUIRED') from None
    if start > datetime.now(timezone.utc):
        raise DispatchError('SHORT_STREAM_START_IN_FUTURE')
    route = roles.route_for(env, 'short_account', side='SHORT')
    if env['HL_TESTNET_SHORT_ENTRY_ENABLED'] == 'true':
        # Check the signer before the worker can reserve a durable request.
        roles.wallet_for_role(env, 'short_account', route['account'], route['agent'])
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
    expected = {s['symbol']:(life.number(s['evidence']['snapshot']['position_quantity'],signed=True)
               if s['evidence'] is not None else 0) for s in states}
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
        actual=life.number(p.get('szi'),signed=True)
        if actual!=expected.get(p['coin'],0):
            raise DispatchError('UNOWNED_ACCOUNT_POSITION_NO_NEW_ENTRY')
    if any(quantity!=0 and symbol not in seen for symbol,quantity in expected.items()):
        raise DispatchError('ACCOUNT_POSITION_DISAPPEARED_RECONCILE_FIRST')
    return True


def tick(controller, route, not_before, *, new_entries, role='long_account'):
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
        for cid,card_role in candidates:
            if role != card_role or cid in known_cards:
                continue
            card=CardStore(controller.store.journal).load(cid)
            if card['account_role'] != card_role:
                raise DispatchError('STREAM_SOURCE_ROLE_CHANGED')
            try:
                state=controller.register(cid)
            except DispatchError as exc:
                if str(exc) in ('ORIGINAL_SOURCE_EXPIRED',
                                'REGISTER_WHILE_REQUEST_UNRESOLVED'):
                    continue
                raise
            if state['account']!=route['account']:
                raise DispatchError('STREAM_CARD_ACCOUNT_MISMATCH')
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


def observed_trades(controller, route, *, role='long_account'):
    """Read durable fill evidence for monitoring; never authorize an order."""
    result = []
    for state in controller.store.for_account(route['account']):
        if not state['bindings'] or state['evidence'] is None:
            continue
        snap = state['evidence']['snapshot']
        view = life.review(state['bindings'], snap, now_ms=controller.venue.now())
        if view['bucket_issues']:
            continue
        for row in view['cards']:
            if life.number(row['entry_quantity']) <= 0:
                continue
            original = state['originals'].get(row['card_id'])
            if original is None:
                raise DispatchError('OBSERVED_TRADE_SOURCE_MISSING')
            card = trade_cards.validate_card(original['card'])
            if (card['card_id'] != row['card_id'] or
                    card['record_kind'] != 'received_alert' or
                    card['account_role'] != role):
                raise DispatchError('OBSERVED_TRADE_SOURCE_MISMATCH')
            remaining = life.number(row['remaining_quantity'])
            protected = (remaining > 0 and not row['issues'] and
                life.number(row['stop_quantity_observed']) >= remaining and
                life.number(row['take_profit_quantity_observed']) >= remaining)
            result.append(dict(card_id=row['card_id'],
                source_event_id=card['prepared']['source']['event_id'],
                symbol=state['symbol'], state=row['state'],
                entry_quantity=row['entry_quantity'],
                protection_verified=protected,
                closure_verified=row['closure_verified'],
                evidence_at_ms=snap['at_ms'],order_requests_sent=0))
    return result


def _loop(controller, streams):
    reported = set()
    while not _stop.is_set():
        results = []
        for role,route,start,enabled_key in streams:
            before=getattr(controller.venue,'sent',0)
            try:
                result=tick(controller,route,start,
                            new_entries=controller.venue.env[enabled_key]=='true',role=role)
            except Exception as exc:
                code = str(exc)
                known = isinstance(exc, (DispatchError, roles.checks.Blocked))
                safe_code = code if known and re.fullmatch(r'[A-Z][A-Z0-9_]{2,99}', code) else type(exc).__name__
                origin = traceback.extract_tb(exc.__traceback__)[-1]
                result=dict(status='RECONCILIATION_REQUIRED_NO_BLIND_RETRY',
                    order_requests_sent=max(0,getattr(controller.venue,'sent',0)-before),
                    new_cards_registered=0, failure_code=safe_code,
                    failure_origin=f'{os.path.basename(origin.filename)}:{origin.lineno}')
            results.append(result)
            with _lock:
                _health['cycles'] += 1
                _health['last_status']=result['status']
                _health['order_requests_sent']=getattr(controller.venue,'sent',0)
            print(json.dumps({('testnet_long_stream' if role=='long_account' else 'testnet_short_stream'):result},sort_keys=True),flush=True)
            if result.get('active_buckets',0) or result.get('new_cards_registered',0):
                try:
                    for trade in observed_trades(controller,route,role=role):
                        marker = (trade['card_id'],trade['state'],
                                  trade['protection_verified'],trade['closure_verified'])
                        if marker not in reported:
                            label='testnet_long_trade_observed' if role=='long_account' else 'testnet_short_trade_observed'
                            print(json.dumps({label:trade},sort_keys=True),flush=True)
                            reported.add(marker)
                except Exception:
                    label='testnet_long_trade_observation' if role=='long_account' else 'testnet_short_trade_observation'
                    print(json.dumps({label:'UNAVAILABLE_RETRY'}),flush=True)
        _stop.wait(2 if all(r['status']=='SWEEP_COMPLETE' for r in results) else 10)
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


def _short_account_readiness(route):
    """One public, read-only snapshot of the approved second account."""
    report = dict(status='READ_ONLY_REVIEW_UNAVAILABLE', order_requests_sent=0,
                  account_settings_changes=0, transfers_sent=0)
    try:
        from .checks import InfoReader, Blocked
        result = roles.default_native_snapshot(
            route, InfoReader(), 'BTC', allow_owned_exposure=True)
        report.update(status=result['status'], account_mode=result['account_mode'],
                      balance_usd=result['balance_usd'],
                      exchange_reported_available_usd=result['exchange_reported_available_usd'],
                      account_mapping_verified=result['account_mapping_verified'])
    except Blocked as exc:
        code = str(exc)
        if re.fullmatch(r'[A-Z][A-Z0-9_]{2,99}', code):
            report['status'] = code
    except Exception:
        pass
    print(json.dumps({'testnet_short_account_readiness': report}, sort_keys=True), flush=True)


def start():
    global _thread,_app_thread
    env=dict(os.environ)
    route,not_before=configuration(env)
    short=short_configuration(env)
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
    if short:
        controller.store.for_account(short[0]['account'])
    with _lock:
        if _thread is not None and _thread.is_alive():
            return False
        _stop.clear()
        _health.update(configured=True,running=True,last_status='STARTING',cycles=0,
                       order_requests_sent=0,
                       new_entries_enabled=env['HL_TESTNET_LONG_ENTRY_ENABLED']=='true',
                       short_entries_enabled=bool(short and env['HL_TESTNET_SHORT_ENTRY_ENABLED']=='true'),
                       short_stream_configured=bool(short),
                       app_delivery_status='STARTING' if app_mode else 'DISABLED')
        streams=[('long_account',route,not_before,'HL_TESTNET_LONG_ENTRY_ENABLED')]
        if short:
            streams.append(('short_account',short[0],short[1],'HL_TESTNET_SHORT_ENTRY_ENABLED'))
        _thread=threading.Thread(target=_loop,args=(controller,streams),
                                 daemon=True,name='testnet-card-stream')
        _thread.start()
        if short:
            threading.Thread(target=_short_account_readiness,args=(short[0],),
                             daemon=True,name='testnet-short-account-readiness').start()
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
