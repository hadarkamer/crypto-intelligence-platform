"""Read persisted Testnet trade observations for a private reporting surface.

No venue requests, wallets, signers or trading methods are instantiated.
"""
import os
from decimal import Decimal

from .filled_dispatch_store import DispatchStore
from .long_stream_runtime import observed_trades
from .postgres_journal import PostgresJournal
from .trade_cards import account_routes

ROLES = {'L': 'long_account', 'S': 'short_account'}


def load_trades(env=None):
    env = dict(os.environ if env is None else env)
    routes = account_routes(env)
    journal = PostgresJournal.from_env(env)
    store = DispatchStore(journal)
    class Reader:
        pass
    reader = Reader()
    reader.store = store
    reader.venue = Reader()
    current = []
    if env.get('HL_TESTNET_RUNTIME_MODE') == 'experimental_testnet_v1':
        from .experimental_live_state import TestnetExecutionState
        current = _execution_trades(TestnetExecutionState(journal))
    result = {}
    for key, role in ROLES.items():
        route = routes[role]
        if route['status'] != 'CONFIGURED_NOT_VERIFIED' or not route['account']:
            raise ValueError('ACCOUNT_REPORT_MAPPING_UNAVAILABLE')
        rows = {row['card_id']: row for row in observed_trades(reader, route, role=role, historical=True)}
        for row in current:
            if row['account_role'] == role:
                if row['account'] != route['account']:
                    raise ValueError('ACCOUNT_REPORT_MAPPING_UNAVAILABLE')
                rows[row['card_id']] = row
        result[key] = sorted(rows.values(), key=lambda row: row['first_entry_at_ms'], reverse=True)
    return result


def _execution_trades(store):
    """Read hot and archived cards in one existing read-only snapshot."""
    from .experimental_execution_cards import cards_from_state, card_from_record
    from .experimental_execution_archive import _checked
    with store._history_snapshot() as (db, state):
        cards = cards_from_state(state, domain=store.domain)
        if state.get('history'):
            cursor = ''
            while True:
                rows = db.execute(f'SELECT occurrence_id,value,checksum FROM {db.table("history")} '
                    'WHERE occurrence_id>? ORDER BY occurrence_id LIMIT 100', (cursor,)).fetchall()
                if not rows:
                    break
                for cid, raw, checksum in rows:
                    card = card_from_record(_checked(raw, checksum), domain=store.domain)
                    if card is not None:
                        cards.append(card)
                cursor = rows[-1][0]
        return [_report_row(card) for card in cards
                if Decimal(card['summary']['filled_entry_quantity']) > 0]


def _report_row(card):
    """Adapt existing journal facts to the existing Telegram report fields."""
    summary = card['summary']
    issues = [item['reason'] for item in card['wait_reasons']]
    remaining = summary['remaining_quantity']
    if card.get('manual_management'):
        actual = summary.get('actual_position_quantity')
        remaining = (str(abs(Decimal(actual))) if actual is not None
                     else summary['historical_bot_remaining_quantity'])
        issues.append('MANUAL_MANAGEMENT' if actual is not None else 'CURRENT_POSITION_QUANTITY_UNKNOWN')
    if summary['unresolved_attempts']:
        issues.append('UNRESOLVED_ORDER_ATTEMPT')
    active = [order['oid'] for order in card['observed_orders'] if order['status'] == 'OPEN']
    exits = [order for order in card['protections'] if order['status'] == 'OPEN']
    protected = (not issues and Decimal(remaining) > 0 and all(
        len(legs := [order for order in exits if order['leg'] == leg]) == 1
        and legs[0]['wire_order']['r'] is True
        and Decimal(legs[0]['unfilled_quantity']) == Decimal(remaining)
        for leg in ('STOP', 'TAKE_PROFIT')))
    return dict(card_id=card['card_id'], account=card['account'], account_role=card['account_role'],
        symbol=card['symbol'], state=card['status'], planned_prices=card['original_prices'],
        entry_quantity=summary['filled_entry_quantity'], exit_quantity=summary['filled_exit_quantity'],
        remaining_quantity=remaining, actual_entry_price=summary['actual_average_entry'],
        actual_exit_price=summary['actual_average_exit'], first_entry_at_ms=summary['first_fill_at_ms'],
        last_exit_at_ms=summary['last_exit_fill_at_ms'], evidence_at_ms=card['last_exchange_snapshot_at_ms'],
        protection_verified=protected, closure_verified=card['status'] in ('CLOSED', 'MANUALLY_CLOSED'),
        issues=sorted(set(issues)), active_order_ids=active)
