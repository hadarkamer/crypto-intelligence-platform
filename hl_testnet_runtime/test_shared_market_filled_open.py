"""The 21-Nov SOL fill/status interleaving must not abort an XRP decision.

Only a frozen projection of the observed ledger is used. An OPEN status remains
OPEN; a fully allocated fill proves zero outstanding size, not finality.
"""
from copy import deepcopy
from decimal import Decimal
import unittest

from . import experimental_execution_runtime as runtime
from . import experimental_shared_market as shared
from .test_experimental_execution_runtime import ROUTES
from .test_experimental_shared_market import state, trade


SOL_CID = 'a449231f4b2e6f02034e7e514fbde34751c97cdd135f3a9923b4af2f14dbd2ee'
XRP_CID = 'd356b1064f4688182030971aab5d0e78670e315fc4c46c91598408742518c364'
SOL_NEXT_CID = 'c177ef6c3c7dca0565c4c5941bf443e5be4c06f66c18a66d1b9fd5ee14242cf3'
ENTRY_OID, STOP_OID, TP_OID = '612268082509', '612268286101', '612268333021'


def filled_open_sol():
    """Narrow historical quantity/order projection; not a new trading signal."""
    row = trade(SOL_CID, quantity='3.05', symbol='SOL', take=True)
    row.update(account=ROUTES['short_account']['account'], role='short_account',
        side='SHORT', prices=dict(entry='65.469', stop='66.124', take_profit='64.16'),
        desired_stop='66.124', asset=dict(index=5, decimals=2))
    entry = dict(fill_id='hl:48260705842792', order_id=ENTRY_OID,
        quantity='3.05', price='65.469', at_ms=1795239521421)
    stop = dict(fill_id='hl:431460069390707', order_id=STOP_OID,
        quantity='3.05', price='66.188', at_ms=1795242817949)
    row['entry_fills'] = {entry['fill_id']: entry}
    row['exit_fills'] = {stop['fill_id']: stop}
    row['order_legs'] = {ENTRY_OID: 'ENTRY', STOP_OID: 'STOP', TP_OID: 'TAKE_PROFIT'}
    row['orders'] = {
        ENTRY_OID: dict(oid=ENTRY_OID, status='FILLED', fills=[entry],
            wire_order=dict(a=5, s='3.05', r=False, b=False, p='65.469')),
        STOP_OID: dict(oid=STOP_OID, status='OPEN', fills=[stop],
            wire_order=dict(a=5, s='3.05', r=True, b=True, p='66.124')),
        TP_OID: dict(oid=TP_OID, status='OPEN', fills=[],
            wire_order=dict(a=5, s='3.05', r=True, b=True, p='64.16')),
    }
    return row


def entry_proposal(symbol, cid):
    return dict(card_id=cid, symbol=symbol,
        account=ROUTES['short_account']['account'], operation='ENTRY', leg='ENTRY')


class FilledOpenIsolationTests(unittest.TestCase):
    def setUp(self):
        self.sol = filled_open_sol()
        self.state = state(self.sol)

    def lane(self):
        return shared.assess(self.state)['lanes'][runtime._lane(self.sol['account'], 'SOL')]

    def test_complete_stop_fill_with_open_status_does_not_abort_xrp(self):
        before = deepcopy(self.state)
        self.assertIsNone(shared.proposal_reason(self.state, entry_proposal('XRP', XRP_CID)))
        self.assertEqual(self.state, before)

    def test_same_sol_lane_stays_blocked_and_live_tp_keeps_capacity(self):
        lane = self.lane()
        self.assertEqual(lane['active_occurrences'], 1)
        own = lane['trades'][0]
        self.assertEqual(Decimal(own['remaining_quantity']), 0)
        self.assertEqual(own['independent_exit_capacity'], '3.05')
        self.assertEqual(own['excess_independent_capacity'], '3.05')
        self.assertEqual(own['open_order_ids'], [STOP_OID, TP_OID])
        self.assertFalse(lane['capacity_safe_for_arbitrary_fills'])
        self.assertEqual(shared.proposal_reason(self.state,
            entry_proposal('SOL', SOL_NEXT_CID)), shared.ENTRY_BLOCK)
        self.assertEqual(self.sol['orders'][STOP_OID]['status'], 'OPEN')
        self.assertEqual(self.sol['phase'], 'OPEN')

    def test_zero_capacity_alone_is_not_terminal_order_proof(self):
        self.sol['orders'][TP_OID]['status'] = 'CANCELED'
        lane = self.lane()
        self.assertEqual(Decimal(lane['trades'][0]['independent_exit_capacity']), 0)
        self.assertEqual(lane['trades'][0]['open_order_ids'], [STOP_OID])
        self.assertEqual(lane['active_occurrences'], 1)
        self.assertEqual(shared.proposal_reason(self.state,
            entry_proposal('SOL', SOL_NEXT_CID)), shared.ENTRY_BLOCK)

    def test_order_overfill_still_rejects_even_if_other_market_is_proposed(self):
        # Per-order overfill is independently invalid even if the ledger's
        # allocated exit total has not been updated by the same broken input.
        self.sol['orders'][STOP_OID]['fills'] = [dict(
            self.sol['orders'][STOP_OID]['fills'][0], quantity='3.06')]
        with self.assertRaisesRegex(shared.SharedMarketError,
                'EXACT_SHARED_REDUCING_CAPACITY_REQUIRED'):
            shared.proposal_reason(self.state, entry_proposal('XRP', XRP_CID))

    def test_zero_outstanding_does_not_excuse_non_reducing_order(self):
        self.sol['orders'][STOP_OID]['wire_order']['r'] = False
        with self.assertRaisesRegex(shared.SharedMarketError,
                'EXACT_SHARED_REDUCING_CAPACITY_REQUIRED'):
            shared.proposal_reason(self.state, entry_proposal('XRP', XRP_CID))

    def test_unresolved_exit_still_reserves_its_full_quantity(self):
        self.sol['orders'][TP_OID]['status'] = 'CANCELED'
        self.state['requests']['unknown'] = dict(request_id='unknown', phase='OUTCOME_UNKNOWN',
            proposal=dict(card_id=SOL_CID, leg='TAKE_PROFIT', action=dict(type='order',
                grouping='na', orders=[dict(a=5, r=True, b=True, s='3.05', p='64.16')])))
        own = self.lane()['trades'][0]
        self.assertEqual(own['independent_exit_capacity'], '3.05')
        self.assertEqual(own['unresolved_request_ids'], ['unknown'])
        self.assertEqual(shared.proposal_reason(self.state,
            entry_proposal('SOL', SOL_NEXT_CID)), shared.ENTRY_BLOCK)


if __name__ == '__main__':
    unittest.main()
