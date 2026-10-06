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
    return dict(domain='software', real_exchange_activity=False, snapshot_revision=state['revision'],
        trades=rows, counts=dict(total=len(rows), closed=sum(r['status']=='CLOSED' for r in rows),
            open=sum(Decimal(r['remaining_quantity'])>0 for r in rows),
            unresolved_attempts=sum(r['unresolved_attempts'] for r in rows)))
