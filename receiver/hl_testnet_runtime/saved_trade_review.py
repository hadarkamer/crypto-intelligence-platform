"""One explicit read-only review of the existing DOGE Testnet experiment.

Does not close/cancel any order, create a trade card, update a result, or send to
Lovable. Unknown attribution stays unknown. Fees are subtracted once from raw
API closedPnl; funding is separated and only attributed in a clean window.
"""
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import time

from . import checks
from .account_ledger_review import HistoryReader, _rows, WEEK_MS
from .postgres_journal import PostgresJournal, validate_action
from .two_account_execution import route_for, SERVICE

SOURCE_ID = 'manual-b1c1f0800dc64399aee17427074512a7'


def amount(value):
    return checks.number(value, signed=True)


def summarize_fills(fills, expected_ids, quantity):
    """Exact exchange OID attribution; other same-coin trades are not ours."""
    role_by_oid = {oid: role for role, oid in expected_ids.items()}
    if len(role_by_oid) != len(expected_ids):
        raise checks.Blocked('DUPLICATE_EXCHANGE_ORDER_ID')
    seen, selected, unmatched = {}, [], 0
    for item in fills:
        if item.get('coin') != 'DOGE':
            continue
        tid = item.get('tid')
        if type(tid) is not int:
            raise checks.Blocked('FILL_ID_REQUIRED')
        if tid in seen:
            if seen[tid] != item:
                raise checks.Blocked('CONFLICTING_DUPLICATE_FILL')
            continue
        seen[tid] = item
        if item.get('oid') not in role_by_oid:
            unmatched += 1
            continue
        role = role_by_oid[item['oid']]
        expected_direction = 'Open Long' if role == 'ENTRY' else 'Close Long'
        if (item.get('feeToken') != 'USDC' or item.get('dir') != expected_direction
                or item.get('side') != ('B' if role == 'ENTRY' else 'A')
                or amount(item.get('sz')) <= 0 or amount(item.get('px')) <= 0):
            raise checks.Blocked('UNSUPPORTED_OR_MISMATCHED_FILL')
        selected.append(dict(role=role, time=item['time'], price=item['px'], quantity=item['sz'],
                             gross_closed_pnl=item['closedPnl'], fee=item['fee']))
    entries = [x for x in selected if x['role'] == 'ENTRY']
    exits = [x for x in selected if x['role'] != 'ENTRY']
    with localcontext() as ctx:
        ctx.prec = 60
        entry_size = sum((amount(x['quantity']) for x in entries), Decimal(0))
        exit_size = sum((amount(x['quantity']) for x in exits), Decimal(0))
        fees = sum((amount(x['fee']) for x in selected), Decimal(0))
        gross = sum((amount(x['gross_closed_pnl']) for x in selected), Decimal(0))
        vw = lambda rows, q: (str(sum((amount(x['price'])*amount(x['quantity']) for x in rows), Decimal(0))/q) if q else None)
        return dict(matched_fills=selected, unmatched_doge_fills=unmatched,
            entry_quantity=str(entry_size), exit_quantity=str(exit_size),
            complete_expected_quantity=entry_size == exit_size == amount(quantity),
            average_entry_price=vw(entries, entry_size), average_exit_price=vw(exits, exit_size),
            gross_pnl_usdc=str(gross), trade_fees_usdc=str(fees),
            net_before_funding_usdc=str(gross-fees),
            first_fill_ms=min((x['time'] for x in entries), default=None),
            last_exit_ms=max((x['time'] for x in exits), default=None))


def review_saved_trade(env, *, journal=None, http=None, history=None):
    report = dict(status='DISABLED', environment='testnet', order_requests_sent=0,
        signing_tested=False, app_delivery_sent=False, stored_trade_changed=False,
        new_trade_cards_created=0, continuous_trade_result_sync=False)
    if env.get('HL_TESTNET_CONNECTION_REVIEW') != 'two_account_no_orders_v1':
        return report
    if (env.get('RENDER_SERVICE_ID') != SERVICE
            or env.get('HL_TESTNET_RUNTIME_MODE') not in ('read_only', 'cancel_monitor_testnet_v1')):
        return {**report, 'status': 'REVIEW_MODE_NOT_ALLOWED'}
    try:
        import hyperliquid_testnet_executor as sender
        account = route_for(env, 'long_account')['account']
        journal = PostgresJournal.from_env(env) if journal is None else journal
        with journal._transaction() as conn:
            rows = conn.execute('''SELECT p.manifest,a.action,a.result
                FROM hl_testnet_execution_v1.prepared p
                JOIN hl_testnet_execution_v1.attempts a ON a.plan_key=p.plan_key
                WHERE p.account=%s AND p.source_id=%s''', (account, SOURCE_ID)).fetchall()
        if len(rows) != 1:
            raise checks.Blocked('EXACT_HISTORICAL_TRADE_NOT_FOUND')
        prepared, action, previous = rows[0]
        action = validate_action(action, prepared, account)
        source = prepared['execution']
        if source['symbol'] != 'DOGE' or source['side'] != 'LONG':
            raise checks.Blocked('HISTORICAL_SOURCE_MISMATCH')
        report['previous_stored_status'] = previous.get('status')
        report['source_event_id'] = SOURCE_ID
        http = sender.TestnetHTTP() if http is None else http
        history = HistoryReader() if history is None else history
        statuses, ids = {}, {}
        for role, order in zip(sender.ROLES, action['orders']):
            value = http.info('orderStatus', user=account, oid=order['c'])
            if value.get('status') != 'order':
                raise checks.Blocked('ORDER_STATUS_UNAVAILABLE')
            actual = value['order']['order']
            if (actual.get('cloid') != order['c'] or actual.get('coin') != 'DOGE'
                    or type(actual.get('oid')) is not int or actual['oid'] <= 0):
                raise checks.Blocked('ORDER_IDENTITY_MISMATCH')
            statuses[role] = value['order']['status']
            ids[role] = actual['oid']
        report['exchange_order_states'] = statuses
        current = http.info('clearinghouseState', user=account)
        report['current_doge_quantity'] = str(sender._position(current, 'DOGE'))
        open_orders = http.info('frontendOpenOrders', user=account)
        if not isinstance(open_orders, list):
            raise checks.Blocked('OPEN_ORDER_STATE_UNAVAILABLE')
        report['remaining_doge_orders'] = sum(x.get('coin') == 'DOGE' for x in open_orders)
        start = int(datetime.fromisoformat(source['at'].replace('Z','+00:00')).timestamp()*1000)
        end = time.time_ns()//1_000_000
        if not 0 < end-start <= WEEK_MS:
            raise checks.Blocked('BOUNDED_HISTORICAL_WINDOW_EXCEEDED')
        fills = _rows(history.read('userFillsByTime', account, start, end), start, end)
        if len(fills) >= 2000:
            raise checks.Blocked('FILL_HISTORY_PAGINATION_REQUIRED')
        pnl = summarize_fills(fills, ids, action['orders'][0]['s'])
        report['fills_and_pnl'] = pnl
        report['funding_attributed_usdc'] = None
        report['net_including_funding_usdc'] = None
        if pnl['complete_expected_quantity'] and pnl['unmatched_doge_fills'] == 0:
            fs, fe = pnl['first_fill_ms'], pnl['last_exit_ms']
            funds = _rows(history.read('userFunding', account, fs, max(fs+1,fe)), fs, max(fs+1,fe))
            if len(funds) < 500:
                funding = sum((amount(x['delta']['usdc']) for x in funds if x.get('delta',{}).get('coin')=='DOGE'),Decimal(0))
                report['funding_attributed_usdc'] = str(funding)
                report['net_including_funding_usdc'] = str(Decimal(pnl['net_before_funding_usdc'])+funding)
        report['status'] = ('CLOSED_QUANTITY_AND_PNL_OBSERVED' if pnl['complete_expected_quantity']
            and report['current_doge_quantity'] in ('0','0.0') and report['remaining_doge_orders']==0
            else 'TRADE_REVIEW_INCOMPLETE')
    except checks.Blocked as exc:
        report['status'] = str(exc)
    except Exception:
        report['status'] = 'HISTORICAL_TRADE_REVIEW_UNAVAILABLE'
    report['checked_at_utc'] = datetime.now(timezone.utc).isoformat()
    return report
