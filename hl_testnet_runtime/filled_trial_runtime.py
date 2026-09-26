"""Prepare the approved second-account trial; execution remains separately gated.

Preparation uses public reads and explicit dispatch-schema initialization only.
The future controlled worker is mutually exclusive with legacy workers, consumes
one immutable server-side card, and never accepts app/UI commands. Changing the
after-exit POLICY alone cannot start it. No account writes are performed on import.
"""
from copy import deepcopy
import json
import os
import threading
from datetime import datetime, timezone

from . import filled_quantity_dispatch as dispatch, card_lifecycle as life
from . import two_account_execution as roles
from .filled_dispatch_store import DispatchStore, DispatchError, SCHEMA
from .postgres_journal import PostgresJournal
from .trade_card_store import CardStore
from .source_window import source_fresh, timestamp

VERSION = 'second-account-controlled-runtime-v1'
PREPARE = 'prepare_second_account_no_orders_v1'
EXECUTE = 'filled_card_controlled_v1'
AUTOWAIT = 'approved_next_u21_once_v1'
POLICY = dispatch.AFTER_EXIT
DELAY_SECONDS = 2
_lock = threading.Lock()
_stop = threading.Event()
_thread = None
_health = dict(configured=False, running=False, last_status='DISABLED', cycles=0,
               order_requests_sent=0, app_controls=False)


def configuration(env, *, sending=False):
    if (env.get('RENDER_SERVICE_ID') != roles.SERVICE
            or env.get('HL_TESTNET_JOURNAL_BACKEND') != 'staging_postgres_v1'
            or env.get('HL_TESTNET_TWO_ACCOUNT_EXECUTION') != 'disabled'
            or env.get('HL_TESTNET_FILLED_AFTER_EXIT_POLICY') != POLICY):
        raise DispatchError('EXPLICIT_APPROVED_AFTER_EXIT_POLICY_REQUIRED')
    if sending:
        if (env.get('HL_TESTNET_RUNTIME_MODE') != EXECUTE
                or env.get('HL_TESTNET_FILLED_DISPATCH') != 'approved_single_card_v1'
                or env.get('HL_TESTNET_SAFETY_PIPELINE')
                or env.get('HL_TESTNET_CARD_SYNC')):
            raise DispatchError('CONTROLLED_WORKER_REQUIRES_EXCLUSIVE_EXPLICIT_RELEASE')
        if env.get('HL_TESTNET_FILLED_AUTOWAIT') == AUTOWAIT:
            from .alert_cards_intake import enabled as intake_enabled
            if (env.get('HL_TESTNET_FILLED_CARD_ID') or not intake_enabled(env)):
                raise DispatchError('ONE_FRESH_U21_INTAKE_CONFIGURATION_REQUIRED')
            try:
                timestamp(env['HL_TESTNET_FILLED_NOT_BEFORE'])
            except (KeyError, TypeError, ValueError):
                raise DispatchError('EXPLICIT_U21_START_TIME_REQUIRED') from None
        else:
            life.ident(env.get('HL_TESTNET_FILLED_CARD_ID'), r'[0-9a-f]{64}')
    else:
        if (env.get('HL_TESTNET_RUNTIME_MODE') != 'read_only'
                or env.get('HL_TESTNET_FILLED_DISPATCH', 'disabled') != 'disabled'
                or env.get('HL_TESTNET_SAFETY_PIPELINE') != 'integrated_readonly_v1'):
            raise DispatchError('PREPARATION_REQUIRES_LOCKED_READ_ONLY_MODE')
    # Scope of this trial, not a permanent restriction on the future strategy.
    return roles.route_for(env, 'short_account', account=roles.PHANTOM, side='SHORT')


def inspect_preparation(env, *, journal=None, public_venue=None, capacity_reader=None):
    """One startup check; no register/reserve/begin/send or secret access.

    DDL only adds our empty schema on the already-selected staging database.
    No existing card or execution record is edited. It is never run every tick.
    Test injection must use both a disposable journal and a software venue.
    """
    report = dict(version=VERSION, status='DISABLED', environment='testnet',
        selected_after_exit_policy=POLICY, cancel_after_first_confirmed_exit=True,
        keep_pending_entry_before_exit=True, policy_is_order_authorization=False,
        storage_ready=False, empty_account_verified=False, balance_usd=None,
        specific_trade_checked=False, dispatch_enabled=False, signing_tested=False,
        order_requests_sent=0, app_delivery_enabled=False)
    if env.get('HL_TESTNET_FILLED_PREPARATION') != PREPARE:
        return report
    route = configuration(env, sending=False)
    journal = PostgresJournal.from_env(env) if journal is None else journal
    venue = dispatch.TestnetVenue(env) if public_venue is None else public_venue
    store = DispatchStore(journal)
    if store.domain != venue.domain:
        raise DispatchError('SOFTWARE_AND_ACCOUNT_STORAGE_MUST_NOT_MIX')
    started = venue.now()
    store.initialize()
    with journal._transaction() as conn:
        store.ready(conn)
        pending = conn.execute(f'''SELECT count(*) FROM {SCHEMA}.requests r
            JOIN {SCHEMA}.buckets b USING(bucket)
            WHERE b.value->>'account'=%s AND r.phase NOT IN ('OBSERVED','ABORTED_UNSENT')''',
            (route['account'],)).fetchone()[0]
        tracked = conn.execute(f'''SELECT count(*) FROM {SCHEMA}.buckets
            WHERE value->>'account'=%s AND (value->'bindings') <> '[]'::jsonb''',
            (route['account'],)).fetchone()[0]
    report.update(storage_ready=True, unresolved_requests=pending,
                  already_registered_execution_buckets=tracked)
    if pending or tracked:
        return {**report, 'status':'EXISTING_EXECUTION_REQUIRES_RECONCILIATION'}
    reader = roles.checks.InfoReader() if capacity_reader is None else capacity_reader
    funds = roles.default_native_snapshot(route, reader, 'BTC')
    empty = venue.empty_snapshot(route['account'], 'BTC')
    life.validate_snapshot(empty)
    now = venue.now()
    if (empty['account'] != route['account'] or empty['symbol'] != 'BTC'
            or not empty['history_complete'] or not empty['orders_complete']
            or life.number(empty['position_quantity'], signed=True) != 0
            or empty['open_orders'] or empty['fills'] or empty['terminal_orders']
            or not 0 <= now-empty['at_ms'] <= 15000 or not 0 <= now-started <= 30000):
        raise DispatchError('PREPARATION_ACCOUNT_OBSERVATION_INVALID_OR_STALE')
    report.update(status='PREPARED_FOR_SPECIFIC_PLAN_REVIEW_ONLY',
        empty_account_verified=True, balance_usd=funds['balance_usd'],
        account_mode=funds['account_mode'],
        checked_at_utc=datetime.fromtimestamp(now/1000, timezone.utc).isoformat())
    return report


def tick(controller, bucket, *, send=False):
    """Reuse the actual controller, storage and reconciliation; never invent fills."""
    result = controller.cycle(bucket, send=send)
    state = controller.store.load(bucket)
    done = False
    if state['bindings'] and state['evidence'] is not None and state['pending'] is None:
        view = life.review(state['bindings'], state['evidence']['snapshot'], now_ms=controller.venue.now())
        done = (not view['needs_review'] and all(
            c['closure_verified'] or c['state'] == 'CANCELED_WITHOUT_FILL' for c in view['cards'])
            and len(state['bindings']) == len(state['originals']))
    return dict(status=result['status'], finished=done,
                order_requests_sent=result['order_requests_sent'], app_controls=False)


def _loop(controller, bucket):
    errors = 0
    while not _stop.is_set():
        before = getattr(controller.venue, 'sent', 0)
        try:
            result = tick(controller, bucket, send=True)
            errors = 0
        except Exception:
            # A database/reconciliation failure may occur AFTER an HTTP attempt.
            # Report the observed attempt counter, never falsely report zero.
            # Never expose raw exceptions, actions, keys or connection strings.
            result = dict(status='RECONCILIATION_REQUIRED_NO_BLIND_RETRY', finished=False,
                order_requests_sent=max(0, getattr(controller.venue, 'sent', 0)-before),
                app_controls=False)
            errors += 1
        with _lock:
            _health['cycles'] += 1
            _health['last_status'] = result['status']
            _health['order_requests_sent'] = getattr(controller.venue, 'sent', 0)
        print(json.dumps({'testnet_controlled_card':result}, sort_keys=True), flush=True)
        if result['finished']:
            break
        # Backoff affects reads only; it never clears a pending request or stops
        # managing an open position just because entry approval has expired.
        if _stop.wait(min(30, DELAY_SECONDS * (2 ** min(errors, 4)))):
            break
    with _lock:
        _health['running'] = False


def _next_u21_card(controller, not_before):
    """Read authenticated, stored cards; never interpret the display text anew."""
    from . import trade_cards as cards
    journal=controller.store.journal
    now=datetime.fromtimestamp(controller.venue.now()/1000,timezone.utc)
    with journal._transaction() as conn:
        CardStore(journal).ready(conn)
        rows=conn.execute('''SELECT card_id,manifest,digest FROM hl_testnet_cards_v1.cards
            WHERE source_stream LIKE %s AND created_at >= %s
              AND (manifest->>'source_expires_at')::timestamptz > %s
              AND EXISTS (SELECT 1 FROM hl_testnet_cards_v1.delivery_receipts r
                  WHERE r.card_id=cards.card_id AND r.status='RECORDED')
            ORDER BY created_at,card_id LIMIT 32''',('u21_xrp_short:%',not_before,now)).fetchall()
    for cid,raw,checksum in rows:
        if cards.checksum(raw)!=checksum:
            raise DispatchError('STORED_CARD_CHECKSUM_MISMATCH')
        card=cards.validate_card(raw)
        if (card['card_id']==cid and card['rule']['id']=='U21_XRP_SHORT'
                and card['record_kind']=='received_alert'
                and card['state']=='RECORDED_ONLY'
                and card['account_role']=='short_account'
                and timestamp(card['prepared']['source']['at']) >= not_before
                and source_fresh(timestamp(card['prepared']['source']['at']),
                    card['source_expires_at'],now=now)):
            return cid
    return None


def _wait_for_one(controller, bucket, env, not_before):
    """One persistent account/market slot; restart resumes the same card."""
    errors=0
    while not _stop.is_set():
        try:
            state=controller.store.load(bucket)
            originals=state['originals']
            if len(originals)>1 or state['bindings'] and not originals:
                raise DispatchError('ONE_EXPLICIT_SECOND_ACCOUNT_TRIAL_REQUIRED')
            if originals:
                cid=next(iter(originals))
                card=CardStore(controller.store.journal).load(cid)
                if (card!=originals[cid]['card'] or card['rule']['id']!='U21_XRP_SHORT'
                        or card['account_role']!='short_account'):
                    raise DispatchError('IMMUTABLE_ORIGINAL_CHANGED')
            else:
                try:
                    deadline=int(env.get('HL_TESTNET_FILLED_APPROVAL_EXPIRES_MS',''))
                except (ValueError,TypeError):
                    raise DispatchError('EXACT_APPROVAL_DEADLINE_REQUIRED') from None
                now_ms=controller.venue.now()
                if deadline<=now_ms:
                    with _lock:
                        _health.update(last_status='APPROVAL_EXPIRED_WITHOUT_ENTRY',running=False)
                    return
                if deadline-now_ms>86400000:
                    raise DispatchError('TRIAL_ENTRY_APPROVAL_WINDOW_INVALID')
                cid=_next_u21_card(controller,not_before)
                if cid is None:
                    with _lock: _health['last_status']='WAITING_FOR_NEW_U21'
                    _stop.wait(DELAY_SECONDS)
                    continue
                state=controller.register(cid,single_card=True)
                if set(state['originals'])!={cid}:
                    raise DispatchError('ONE_EXPLICIT_SECOND_ACCOUNT_TRIAL_REQUIRED')
            # The exact card identity is local to this controller, not an env
            # mutation or a way to reuse the release for a second alert.
            controller.venue.env={**env,'HL_TESTNET_FILLED_CARD_ID':cid}
            _loop(controller,bucket)
            return
        except Exception:
            with _lock: _health['last_status']='WAITING_RECONCILIATION_REQUIRED'
            errors+=1
            _stop.wait(min(30,DELAY_SECONDS*(2**min(errors,4))))
    with _lock: _health['running']=False


def start():
    """Future explicit release only. NOT called in the deployed read-only mode."""
    global _thread
    env = os.environ
    route = configuration(env, sending=True)
    controller = dispatch.controller_from_env(env)
    with controller.store.journal._transaction() as conn:
        controller.store.ready(conn)  # No recurring/startup migration during execution.
        if env.get('HL_TESTNET_FILLED_AUTOWAIT')==AUTOWAIT:
            CardStore(controller.store.journal).ready(conn)
    if env.get('HL_TESTNET_FILLED_AUTOWAIT')==AUTOWAIT:
        not_before=timestamp(env['HL_TESTNET_FILLED_NOT_BEFORE'])
        state=controller.store.create_bucket(route['account'],'XRP')
        if state['account']!=route['account'] or state['symbol']!='XRP':
            raise DispatchError('ONE_EXPLICIT_SECOND_ACCOUNT_TRIAL_REQUIRED')
        target=_wait_for_one;args=(controller,state['bucket'],dict(env),not_before)
    else:
        cid = env['HL_TESTNET_FILLED_CARD_ID']
        card = CardStore(controller.store.journal).load(cid)
        if card['record_kind'] != 'received_alert' or card['account_role'] != 'short_account':
            raise DispatchError('RECEIVED_SECOND_ACCOUNT_CARD_REQUIRED')
        state = controller.register(cid)
        if state['account'] != route['account'] or set(state['originals']) != {cid}:
            raise DispatchError('ONE_EXPLICIT_SECOND_ACCOUNT_TRIAL_REQUIRED')
        target=_loop;args=(controller,state['bucket'])
    with _lock:
        if _thread is not None and _thread.is_alive():
            return False
        _stop.clear()
        _health.update(configured=True, running=True, last_status='STARTING',
                       cycles=0, order_requests_sent=0, app_controls=False)
        _thread = threading.Thread(target=target, args=args,
                                   name='explicit-filled-card-trial', daemon=True)
        _thread.start()
    return True


def stop():
    """No implicit account actions during process shutdown."""
    _stop.set()


def health():
    with _lock:
        return deepcopy(_health)
