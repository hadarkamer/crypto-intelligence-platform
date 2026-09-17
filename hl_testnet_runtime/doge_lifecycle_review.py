"""One-shot public Testnet evidence -> CLOSED historical DOGE shadow card.

No signer, exchange write, new alert, execution reservation or app delivery.
Only the already executed experiment is eligible. Original records stay intact.
This is NOT a live/partial-position adapter or authority for future trading.
"""
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import http.client
import json
import time

from . import card_lifecycle as life, checks
from .account_ledger_review import WEEK_MS
from .approved_account_assignment import public_route_env
from .card_lifecycle_store import LifecycleStore
from .postgres_journal import PostgresJournal, JournalError, validate_prepared, validate_action
from .saved_trade_review import SOURCE_ID
from .trade_cards import account_routes

MODE = 'saved_doge_shadow_v1'
SERVICE = 'srv-dakptbh594qs7395460g'
HOST = 'api.hyperliquid-testnet.xyz'


def now_ms():
    return time.time_ns() // 1_000_000


class ReviewError(life.LifecycleError):
    """Fixed diagnostics, never account keys or remote exception text."""


class PublicReader:
    """Exactly /info on Testnet; fixed read types, bounds, no redirects/retries."""
    def __init__(self):
        self.calls = 0

    def read(self, kind, account, *, oid=None, start=None, end=None):
        account = life.address(account)
        body = {'type': kind, 'user': account}
        if kind == 'orderStatus' and start is None and end is None:
            life.ident(oid, r'0x[0-9a-f]{32}')
            body['oid'] = oid
        elif kind == 'userFillsByTime' and oid is None:
            life.moment(start); life.moment(end)
            if not 0 < end-start <= WEEK_MS:
                raise ReviewError('BOUNDED_HISTORY_REQUIRED')
            body.update(startTime=start, endTime=end, aggregateByTime=False)
        elif kind in ('clearinghouseState', 'frontendOpenOrders') and oid is None and start is None and end is None:
            pass
        else:
            raise ReviewError('PUBLIC_READ_TYPE_NOT_ALLOWED')
        if HOST != 'api.hyperliquid-testnet.xyz' or self.calls >= 12:
            raise ReviewError('PUBLIC_READ_BOUND_EXCEEDED')
        conn = http.client.HTTPSConnection(HOST, timeout=4)
        self.calls += 1
        try:
            conn.request('POST', '/info', json.dumps(body).encode(), {'Content-Type': 'application/json'})
            response = conn.getresponse()
            if response.status != 200:
                raise ReviewError('PUBLIC_READ_UNAVAILABLE')
            raw = response.read(checks.MAX_BYTES+1)
            if len(raw) > checks.MAX_BYTES:
                raise ReviewError('PUBLIC_RESPONSE_TOO_LARGE')
            return checks.decode(raw)
        except (OSError, http.client.HTTPException):
            raise ReviewError('PUBLIC_READ_UNAVAILABLE') from None
        finally:
            conn.close()


def load_original(journal, account):
    """Read exact persisted source/action/result; do not update legacy status."""
    with journal._transaction() as conn:
        journal._ready(conn)
        rows = conn.execute('''SELECT p.plan_key,p.manifest,a.action,a.result
            FROM hl_testnet_execution_v1.prepared p
            JOIN hl_testnet_execution_v1.attempts a ON a.plan_key=p.plan_key AND a.account=p.account
            WHERE p.account=%s AND p.source_id=%s''', (account, SOURCE_ID)).fetchall()
    if len(rows) != 1:
        raise ReviewError('EXACT_SAVED_DOGE_REQUIRED')
    key, prepared, action, result = rows[0]
    prepared = validate_prepared(prepared)
    action = validate_action(action, prepared, account)
    source = prepared['execution']
    if (source['event_id'] != SOURCE_ID or prepared['source']['event_id'] != SOURCE_ID
            or source['symbol'] != 'DOGE' or source['side'] != 'LONG'):
        raise ReviewError('SAVED_SOURCE_MISMATCH')
    return dict(plan_key=key, prepared=prepared, action=action, result=result)


def order_evidence(raw, sent, leg, start, end):
    if not isinstance(raw, dict) or raw.get('status') != 'order':
        raise ReviewError('TERMINAL_ORDER_EVIDENCE_REQUIRED')
    envelope = raw.get('order', {})
    order = envelope.get('order', {})
    status = envelope.get('status')
    allowed = ('siblingFilledCanceled', 'canceled') if leg == 'STOP' else ('filled',)
    if status not in allowed:
        raise ReviewError('SAVED_TRADE_NOT_FULLY_TP_CLOSED')
    oid = order.get('oid')
    if (type(oid) is not int or oid <= 0 or order.get('cloid') != sent['c']
            or order.get('coin') != 'DOGE' or order.get('side') != ('B' if leg == 'ENTRY' else 'A')
            or type(order.get('reduceOnly')) is not bool or order['reduceOnly'] != (leg != 'ENTRY')):
        raise ReviewError('ORDER_OWNERSHIP_OR_SIDE_MISMATCH')
    if life.number(order.get('origSz'), positive=True) != life.number(sent['s'], positive=True):
        raise ReviewError('ORDER_ORIGINAL_SIZE_MISMATCH')
    if life.number(order.get('limitPx'), positive=True) != life.number(sent['p'], positive=True):
        raise ReviewError('ORDER_LIMIT_PRICE_MISMATCH')
    if leg != 'ENTRY' and life.number(order.get('triggerPx'), positive=True) != life.number(sent['t']['trigger']['triggerPx'], positive=True):
        raise ReviewError('ORDER_TRIGGER_MISMATCH')
    at = life.moment(envelope.get('statusTimestamp'))
    if not start <= at <= end:
        raise ReviewError('ORDER_TIME_OUTSIDE_HISTORY')
    return dict(oid=str(oid), raw_status=status,
                state='CANCELED' if leg == 'STOP' else 'FILLED', at_ms=at)


def normalize_fills(raw, account, ids, start, end, quantity):
    """Require all original size on ENTRY and TP, no stop fill or other DOGE fill.

    A short response alone does not prove completeness: exact terminal quantities
    and all order identities are independently checked. Unknown activity blocks.
    """
    if not isinstance(raw, list) or len(raw) >= 2000:
        raise ReviewError('HISTORY_TRUNCATED_OR_INVALID')
    roles = {item['oid']: leg for leg, item in ids.items()}
    if len(roles) != 3:
        raise ReviewError('ORDER_IDS_NOT_DISTINCT')
    seen, normalized, totals = {}, [], {leg: Decimal(0) for leg in life.LEGS}
    reported_gross = Decimal(0)
    for row in raw:
        if not isinstance(row, dict) or not start <= life.moment(row.get('time')) <= end:
            raise ReviewError('FILL_TIME_OUTSIDE_HISTORY')
        if row.get('coin') != 'DOGE':
            continue
        tid, oid = row.get('tid'), row.get('oid')
        if type(tid) is not int or tid < 0 or type(oid) is not int or oid <= 0:
            raise ReviewError('EXACT_FILL_AND_ORDER_IDS_REQUIRED')
        if tid in seen:
            if seen[tid] != row:
                raise ReviewError('CONFLICTING_RAW_FILL')
            continue
        seen[tid] = row
        leg = roles.get(str(oid))
        if leg is None:
            raise ReviewError('OTHER_DOGE_ACTIVITY_REQUIRES_FULL_BINDINGS')
        if (row.get('side') != ('B' if leg == 'ENTRY' else 'A')
                or row.get('dir') != ('Open Long' if leg == 'ENTRY' else 'Close Long')
                or row.get('feeToken') != 'USDC'):
            raise ReviewError('FILL_DIRECTION_OR_FEE_TOKEN_MISMATCH')
        q = life.number(row.get('sz'), positive=True)
        life.number(row.get('px'), positive=True); life.number(row.get('fee'), signed=True)
        reported_gross += life.number(row.get('closedPnl'), signed=True)
        if row['time'] > ids[leg]['at_ms']:
            raise ReviewError('FILL_AFTER_TERMINAL_STATUS')
        totals[leg] += q
        normalized.append(dict(account=account, symbol='DOGE', oid=str(oid), fill_id='hl:'+str(tid),
            quantity=row['sz'], price=row['px'], fee=row['fee'], fee_token='USDC',
            side=row['side'], at_ms=row['time']))
    if totals != {'ENTRY': life.number(quantity), 'TAKE_PROFIT': life.number(quantity), 'STOP': Decimal(0)}:
        raise ReviewError('HISTORY_DOES_NOT_COVER_TERMINAL_QUANTITIES')
    return sorted(normalized, key=lambda x:(x['at_ms'], x['fill_id'])), life.text(reported_gross)


def flat_inventory(position, orders):
    if not isinstance(position, dict) or not isinstance(position.get('assetPositions'), list):
        raise ReviewError('POSITION_EVIDENCE_REQUIRED')
    matches = [row['position'] for row in position['assetPositions'] if row['position'].get('coin') == 'DOGE']
    if len(matches) > 1 or any(life.number(p.get('szi'), signed=True) != 0 for p in matches):
        raise ReviewError('DOGE_NOT_FLAT')
    if not isinstance(orders, list) or len(orders) > 10000 or any(not isinstance(x,dict) for x in orders):
        raise ReviewError('OPEN_ORDER_INVENTORY_REQUIRED')
    if any(row.get('coin') == 'DOGE' for row in orders):
        raise ReviewError('DOGE_STILL_HAS_WORKING_ORDERS')


def collect(record, account, reader, *, clock=now_ms, elapsed=time.monotonic):
    """Two agreeing bounded public observations of an already terminal trade."""
    started = elapsed()
    source, action = record['prepared']['execution'], record['action']
    start = int(datetime.fromisoformat(source['at'].replace('Z','+00:00')).timestamp()*1000)
    quantity = action['orders'][0]['s']  # Historical actual plan; NEVER resize to today's $10.
    observations = []
    for _ in range(2):
        end = clock()
        if not 0 < end-start <= WEEK_MS:
            raise ReviewError('SAVED_HISTORY_WINDOW_EXCEEDED')
        ids = {leg: order_evidence(reader.read('orderStatus',account,oid=sent['c']),sent,leg,start,end)
               for leg,sent in zip(life.LEGS,action['orders'])}
        raw = reader.read('userFillsByTime',account,start=start,end=end)
        fills, reported = normalize_fills(raw,account,ids,start,end,quantity)
        flat_inventory(reader.read('clearinghouseState',account),reader.read('frontendOpenOrders',account))
        observations.append(dict(ids=ids,fills=fills,exchange_reported_gross=reported))
    if observations[0] != observations[1]:
        raise ReviewError('EXCHANGE_OBSERVATIONS_CHANGED')
    at = clock()
    if elapsed()-started > 15:
        raise ReviewError('OBSERVATION_TOO_SLOW')
    evidence = observations[-1]
    identity = dict(plan_key=record['plan_key'],prepared=record['prepared'],action=action)
    binding = dict(card_id=life.digest(['legacy_executed_testnet',account,record['plan_key']]),
        card_digest=life.digest(identity),environment='testnet',account=account,
        role='long_account',symbol='DOGE',side='LONG',planned_quantity=quantity,
        prices={key:source[key] for key in ('entry','stop','take_profit')},
        orders={leg:[evidence['ids'][leg]['oid']] for leg in life.LEGS})
    terminals = [dict(account=account,symbol='DOGE',oid=item['oid'],state=item['state'],
        filled_quantity='0' if leg=='STOP' else quantity,at_ms=item['at_ms'])
        for leg,item in evidence['ids'].items()]
    snapshot = dict(environment='testnet',account=account,symbol='DOGE',at_ms=at,
        history_complete=True,orders_complete=True,position_quantity='0',
        fills=evidence['fills'],open_orders=[],terminal_orders=terminals)
    report = life.review([binding],snapshot,now_ms=at)
    if report['needs_review'] or not report['cards'][0]['closure_verified']:
        raise ReviewError('LIFECYCLE_REQUIRES_REVIEW')
    gross = Decimal(report['cards'][0]['gross_pnl_usdc'])
    exchange = Decimal(evidence['exchange_reported_gross'])
    comparison = dict(cash_flow_gross_usdc=str(gross),exchange_reported_gross_usdc=str(exchange),
        exchange_minus_cash_flow_usdc=str(exchange-gross),accounting_review_required=exchange!=gross,
        source='raw_fills_cash_flow_compared_with_exchange_closedPnl',funding_included=False)
    return binding,snapshot,report,comparison


def run(env, *, journal=None, reader=None, clock=now_ms):
    result = dict(status='DISABLED',environment='testnet',order_requests_sent=0,
        dispatch_enabled=False,signing_tested=False,app_delivery_sent=False,
        legacy_record_unchanged=None,continuous_sync_enabled=False)
    if env.get('HL_TESTNET_CLOSED_CARD_REVIEW') != MODE:
        return result
    if (env.get('RENDER_SERVICE_ID') != SERVICE
            or env.get('HL_TESTNET_RUNTIME_MODE') not in ('read_only','cancel_monitor_testnet_v1')
            or env.get('HL_TESTNET_JOURNAL_BACKEND') != 'staging_postgres_v1'
            or env.get('HL_TESTNET_TWO_ACCOUNT_EXECUTION') == 'approved_single_attempt_v1'):
        return {**result,'status':'READ_ONLY_MODE_REQUIRED'}
    stage = 'LOAD_ORIGINAL'
    try:
        routes = account_routes(public_route_env(env))
        account = routes['long_account']['account']
        if not account or not account.endswith('2b88'):
            raise ReviewError('APPROVED_FIRST_ACCOUNT_REQUIRED')
        journal = PostgresJournal.from_env(env) if journal is None else journal
        original = load_original(journal,account)
        original_hash = life.digest(original)
        reader = PublicReader() if reader is None else reader
        stage = 'COLLECT_PUBLIC_EVIDENCE'
        binding,snapshot,view,comparison = collect(original,account,reader,clock=clock)
        if life.digest(load_original(journal,account)) != original_hash:
            raise ReviewError('ORIGINAL_CHANGED_DURING_OBSERVATION')
        stage = 'SAVE_SHADOW_ONLY'
        store = LifecycleStore(journal)
        store.initialize()  # Explicit opt-in migration, not a repeating timer.
        previous_revision = 0
        try:
            previous_revision = store.load(account,'DOGE',now_ms=clock())['revision']
        except life.LifecycleError as exc:
            if str(exc) != 'NO_LIFECYCLE_OBSERVATION':
                raise
        saved = store.save([binding],snapshot,expected_revision=previous_revision,now_ms=clock())
        replay = store.save([binding],snapshot,expected_revision=saved['revision'],now_ms=clock())
        loaded = store.load(account,'DOGE',now_ms=clock())  # A separate database connection.
        if not replay['duplicate'] or loaded['revision'] != saved['revision'] or loaded['report'] != view:
            raise ReviewError('SHADOW_READBACK_OR_REPLAY_FAILED')
        result['legacy_record_unchanged'] = life.digest(load_original(journal,account)) == original_hash
        if not result['legacy_record_unchanged']:
            raise ReviewError('ORIGINAL_CHANGED_DURING_OBSERVATION')
        card = view['cards'][0]
        result.update(status='CLOSED_DOGE_SHADOW_SAVED_AND_VERIFIED',card_id=card['card_id'],
            source_event_id=SOURCE_ID,card_state=card['state'],closure_verified=card['closure_verified'],
            entry_quantity=card['entry_quantity'],exit_quantity=card['exit_quantity'],
            remaining_quantity=card['remaining_quantity'],fees_by_token=card['fees_by_token'],
            net_before_funding_usdc=card['net_before_funding_usdc'],funding_usdc=None,final_net_usdc=None,
            accounting_comparison=comparison,shadow_revision=saved['revision'],
            replay_duplicate_verified=True,separate_connection_readback_verified=True,
            previous_shadow_seen=previous_revision>0,observations_agreed=True,
            original_result_status=(original['result'] or {}).get('status'),new_alert_cards_created=0)
    except (life.LifecycleError,JournalError,checks.Blocked) as exc:
        result.update(status=str(exc),failed_stage=stage)
    except Exception:
        result.update(status='CLOSED_DOGE_REVIEW_UNAVAILABLE',failed_stage=stage)
    result['public_reads'] = getattr(reader,'calls',0)
    result['checked_at_utc'] = datetime.now(timezone.utc).isoformat()
    return result
