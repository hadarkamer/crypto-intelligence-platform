"""Dedicated trade cards projected from the durable execution journal.

There is one identity per occurrence, not a second copy of trading state. The
same card can be reconstructed from a hot journal or its lossless archive after
a restart. Reporting performs no exchange I/O and never authorizes an order.
"""
from copy import deepcopy
from decimal import Decimal

from . import card_lifecycle as life

VERSION = 'execution-trade-card-v1'


class CardError(ValueError):
    pass


def _card(state, cid):
    from .experimental_execution_reporting import _project_rows
    trade = state['trades'][cid]
    source = state.get('sources', {}).get(cid)
    msg = trade['source']
    role = 'long_account' if trade['side'] == 'LONG' else 'short_account'
    if (trade['cid'] != cid or msg['occurrence_id'] != cid
            or trade['role'] != role or msg['symbol'] != trade['symbol']
            or msg['side'] != trade['side']
            or state.get('routes', {}).get(role, trade['account']) != trade['account']):
        raise CardError('EXACT_TRADE_CARD_IDENTITY_REQUIRED')
    if source:
        from approved_alert_contract import plan_digest
        if (source['occurrence_id'] != cid
                or plan_digest(msg) != source['plan_digest']):
            raise CardError('IMMUTABLE_TRADE_CARD_SOURCE_REQUIRED')
    requests = {rid: row for rid, row in state['requests'].items()
                if row['proposal']['card_id'] == cid}
    attempts = []
    for rid, row in sorted(requests.items(), key=lambda item: (
            item[1]['attempt_at_ms'], item[1].get('nonce', 0), item[0])):
        proposal = row['proposal']
        if (row['request_id'] != rid or proposal['account'] != trade['account']
                or proposal['symbol'] != trade['symbol']
                or row.get('domain') != state['domain']):
            raise CardError('EXACT_TRADE_CARD_ATTEMPT_IDENTITY_REQUIRED')
        attempts.append(dict(sequence=len(attempts)+1, request_id=rid,
            operation=proposal['operation'], leg=proposal['leg'],
            attempted_at_ms=row['attempt_at_ms'], prepared_at_ms=row.get('prepared_at_ms'),
            phase=row['phase'], observed_order_id=row.get('observed_oid'),
            terminal_state=row.get('terminal_state'),
            definitely_not_submitted=row['phase'] == 'ABORTED_UNSENT',
            unsent_reason=row.get('unsent_reason'),
            certified_unsent=row.get('certified_unsent') is True,
            proposal=deepcopy(proposal), exchange_reply=deepcopy(row.get('reply'))))
    events = [deepcopy(event) for event in state['events']
              if event.get('occurrence_id') == cid or event.get('request_id') in requests]
    events.sort(key=lambda event: event['at_ms'])  # Stable ties retain journal order.
    summary = _project_rows(dict(trades={cid: trade}, requests=requests,
                                 events=events))[0]
    orders, protections = [], []
    for oid, order in sorted(trade['orders'].items(), key=lambda item: (
            item[1]['at_ms'], int(item[0]))):
        if order['oid'] != oid or oid not in trade['order_legs']:
            raise CardError('EXACT_TRADE_CARD_ORDER_IDENTITY_REQUIRED')
        item = deepcopy(order)
        item['leg'] = trade['order_legs'][oid]
        orders.append(item)
        if item['leg'] in ('STOP', 'TAKE_PROFIT'):
            filled = sum((Decimal(row['quantity']) for row in order['fills']), Decimal(0))
            protections.append(dict(order_id=oid, leg=item['leg'],
                status=order['status'], order_at_ms=order['at_ms'],
                quantity=order['wire_order']['s'],
                unfilled_quantity=life.text(Decimal(order['wire_order']['s'])-filled),
                wire_order=deepcopy(order['wire_order'])))
    lane = life.digest([trade['account'], trade['symbol']])
    reasons = []
    for scope, reason in (
            ('occurrence', state.get('entry_blocked', {}).get(cid)),
            ('source', state.get('source_condition_errors', {}).get(cid)),
            ('market', state.get('blocked_lanes', {}).get(lane)),
            ('account', state.get('account_entry_blocked', {}).get(trade['account'])),
            ('trade', trade.get('emergency_reason')),
            ('trade', trade.get('shared_market_blocked_reason')),
            ('entry', trade.get('entry_retired_reason'))):
        if reason:
            reasons.append(dict(scope=scope, reason=deepcopy(reason)))
    # A fill belongs to exactly one owned order; expose observed exit legs,
    # without inventing a single close reason for a mixed partial-exit trade.
    def fills(key):
        result = []
        for row in sorted(trade[key].values(), key=lambda value: (value['at_ms'], value['fill_id'])):
            oid = row['order_id']
            if oid not in trade['orders'] or oid not in trade['order_legs']:
                raise CardError('EXACT_TRADE_CARD_FILL_OWNERSHIP_REQUIRED')
            result.append({**deepcopy(row), 'leg': trade['order_legs'][oid]})
        return result
    entry_fills, exit_fills = fills('entry_fills'), fills('exit_fills')
    return dict(version=VERSION, card_id=cid, domain=state['domain'],
        account=trade['account'], account_role=trade['role'], symbol=trade['symbol'],
        side=trade['side'], formula_id=msg['rule_id'], family=msg['family'],
        source=deepcopy(msg), source_digest=source.get('plan_digest') if source else None,
        original_prices=deepcopy(summary['planned_source_prices']),
        execution_prices=deepcopy(trade['prices']), status=trade['phase'],
        source_tracking=deepcopy(source), execution_audit=deepcopy(trade.get('execution_audit')),
        summary=summary, attempts=attempts, observed_orders=orders,
        entry_fills=entry_fills, exit_fills=exit_fills, protections=protections,
        lifecycle=dict(source_at=msg.get('source_at'), approved_at=msg.get('approved_at'),
            received_at=source.get('created_at') if source else None,
            first_attempt_at_ms=min((r['attempted_at_ms'] for r in attempts), default=None),
            first_fill_at_ms=summary['first_fill_at_ms'],
            last_exit_fill_at_ms=summary['last_exit_fill_at_ms'],
            reconciled_closed_at_ms=summary['reconciled_closed_at_ms']),
        actual_exit_legs=sorted({row['leg'] for row in exit_fills}),
        entry_decision=deepcopy(state.get('entry_decisions', {}).get(cid)),
        wait_reasons=reasons, timeline=events,
        last_exchange_snapshot_at_ms=state.get('snapshots', {}).get(lane, {}).get('at_ms'),
        observation_status='DURABLE_OBSERVED_FACTS_NOT_A_FRESH_EXCHANGE_QUERY')


def cards_from_state(state, *, domain):
    """Project a verified domain-specific snapshot; do not mutate or reload it."""
    from .experimental_execution_state import VERSION as SOFTWARE_VERSION
    from .experimental_live_state import VERSION as TESTNET_VERSION
    versions = {'software': SOFTWARE_VERSION, 'testnet': TESTNET_VERSION}
    if domain not in versions or state.get('domain') != domain or state.get('version') != versions[domain]:
        raise CardError('VERIFIED_TRADE_CARD_DOMAIN_REQUIRED')
    return [_card(state, cid) for cid in sorted(state['trades'])]


def card_from_record(record, *, domain):
    """Project the original immutable archive bundle, including pre-close waits."""
    if domain not in ('software', 'testnet') or record.get('domain') != domain:
        raise CardError('VERIFIED_TRADE_CARD_DOMAIN_REQUIRED')
    cid = record['occurrence_id']
    if record['trade'] is None:
        return None
    state = dict(domain=domain, sources={cid: record['source']},
        trades={cid: record['trade']}, requests=record['requests'], events=record['events'])
    snapshot = record.get('snapshots', {}).get('snapshots')
    if snapshot is not None:
        lane = life.digest([record['trade']['account'], record['trade']['symbol']])
        state['snapshots'] = {lane: snapshot}
    for name, value in record.get('auxiliary', {}).items():
        state[name] = {cid: value}
    return _card(state, cid)
