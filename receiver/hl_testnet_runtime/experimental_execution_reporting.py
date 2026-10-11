"""Read-only isolated execution ledger; planned prices never become fill prices.

Only a completely reconciled closed trade has a final gross P/L. Fees/funding
are not part of the isolated exchange evidence, so net P/L remains unknown.
This projection is not registered with any live or Telegram reporting route.
"""
from decimal import Decimal

from . import card_lifecycle as life
from .experimental_execution_state import VERSION


class ReportError(ValueError):
    pass


def _fills(rows):
    quantity = value = Decimal(0)
    times = []
    for identity, row in rows.items():
        if row['fill_id'] != identity:
            raise ReportError('EXACT_FILL_IDENTITY_REQUIRED')
        q = life.number(row['quantity'], positive=True)
        p = life.number(row['price'], positive=True)
        life.moment(row['at_ms'])
        quantity += q
        value += q*p
        times.append(row['at_ms'])
    return quantity, value, (life.text(value/quantity) if quantity else None), times


def project(state):
    """Project the runtime's verified durable snapshot, without mutating it."""
    if state.get('version') != VERSION or state.get('domain') != 'software':
        raise ReportError('VERIFIED_ISOLATED_STATE_REQUIRED')
    from .experimental_shared_market import assess
    from .experimental_execution_cards import cards_from_state
    rows = _project_rows(state)
    return dict(domain='software', real_exchange_activity=False, snapshot_revision=state['revision'],
        history=dict(state.get('history', {})), shared_market=assess(state),
        trades=rows, cards=cards_from_state(state, domain='software'),
        counts=dict(total=len(rows), closed=sum(r['status'] in ('CLOSED','MANUALLY_CLOSED') for r in rows),
            open=sum(r['status'] not in ('CLOSED','MANUALLY_CLOSED','CANCELED_WITHOUT_FILL')
                     and Decimal(r.get('actual_position_quantity') or r['remaining_quantity'] or '0')!=0 for r in rows),
            unresolved_attempts=sum(r['unresolved_attempts'] for r in rows)))


def _project_rows(state):
    """Project already verified facts; domain checking belongs to the caller."""
    rows = []
    for cid, trade in sorted(state['trades'].items()):
        if trade['cid'] != cid:
            raise ReportError('EXACT_TRADE_IDENTITY_REQUIRED')
        entry_qty, entry_value, entry_avg, entry_times = _fills(trade['entry_fills'])
        exit_qty, exit_value, exit_avg, exit_times = _fills(trade['exit_fills'])
        if exit_qty > entry_qty:
            raise ReportError('EXIT_EXCEEDS_OWNED_ENTRY')
        requests = [r for r in state['requests'].values() if r['proposal']['card_id'] == cid]
        unresolved = sum(r['phase'] not in ('OBSERVED', 'ABORTED_UNSENT') for r in requests)
        final = trade['phase'] in ('CLOSED', 'CANCELED_WITHOUT_FILL')
        if final and (unresolved or exit_qty != entry_qty
                or any(o['status'] not in ('FILLED', 'CANCELED', 'REJECTED') for o in trade['orders'].values())):
            raise ReportError('FINAL_RECONCILIATION_REQUIRED')
        finalized = [e['at_ms'] for e in state['events']
            if e.get('occurrence_id') == cid and e.get('kind') == trade['phase'] and final]
        gross = None
        if trade['phase'] == 'CLOSED':
            if not entry_qty or not finalized:
                raise ReportError('OBSERVED_CLOSED_TRADE_REQUIRED')
            gross = life.text((exit_value-entry_value)*(1 if trade['side'] == 'LONG' else -1))
        source = trade['source']
        rows.append(dict(occurrence_id=cid, formula=source['rule_id'], family=source['family'],
            account_role=trade['role'], symbol=trade['symbol'], side=trade['side'], status=trade['phase'],
            planned_source_prices={k: source[k] for k in ('entry', 'stop', 'take_profit')},
            rounded_execution_prices=dict(trade['prices']),
            filled_entry_quantity=life.text(entry_qty), filled_exit_quantity=life.text(exit_qty),
            remaining_quantity=life.text(entry_qty-exit_qty),
            actual_average_entry=entry_avg, actual_average_exit=exit_avg,
            first_fill_at_ms=min(entry_times) if entry_times else None,
            last_exit_fill_at_ms=max(exit_times) if exit_times else None,
            reconciled_closed_at_ms=max(finalized) if finalized else None,
            gross_pnl_before_costs=gross, fees=None, funding=None, net_pnl=None,
            net_pnl_status='ACTUAL_FEES_AND_FUNDING_NOT_AVAILABLE',
            submitted_attempts=len(requests), unresolved_attempts=unresolved,
            owned_order_ids=sorted(trade['orders'])))
        manual=trade.get('manual_management')
        if manual:
            row=rows[-1]
            row.update(management_mode=manual['status'],historical_bot_remaining_quantity=row['remaining_quantity'],
                remaining_quantity=None,actual_position_quantity=manual.get('actual_position_quantity'),
                actual_position_observed_at_ms=manual.get('observed_at_ms'),
                gross_pnl_before_costs=None,net_pnl=None,net_pnl_status='EXTERNAL_ACTIVITY_UNALLOCATED',
                automatic_position_management=False)
            if trade['phase']=='MANUALLY_CLOSED':
                from .experimental_external_activity import closure_verified
                if not closure_verified(trade):raise ReportError('MANUAL_CLOSURE_NOT_VERIFIED')
                row['reconciled_closed_at_ms']=trade['manual_closure']['confirmed_at_ms']
    return rows


def project_history(page, *, domain):
    """Project one checksummed archive page, never all historical state."""
    if domain not in ('software', 'testnet'):
        raise ReportError('VERIFIED_HISTORY_DOMAIN_REQUIRED')
    from .experimental_execution_cards import card_from_record
    rows, occurrences, cards = [], [], []
    for record in page['records']:
        if record['domain'] != domain:
            raise ReportError('HISTORY_DOMAIN_MISMATCH')
        cid=record['occurrence_id']
        occurrences.append(cid)
        if record['trade'] is None:
            continue
        rows.extend(_project_rows(dict(trades={cid:record['trade']},
            requests=record['requests'], events=record['events'])))
        cards.append(card_from_record(record, domain=domain))
    return dict(domain=domain, trades=rows, cards=cards, archived_occurrences=occurrences,
        archived_count=page['archived_count'], next_cursor=page['next_cursor'])
