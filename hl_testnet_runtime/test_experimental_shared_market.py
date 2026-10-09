"""Adversarial pooled-position safety; no venue assumptions or network I/O."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from approved_alert_fixtures import BASE
from . import experimental_execution_runtime as runtime
from . import experimental_shared_market as shared
from .experimental_execution_state import ExecutionState
from .test_approved_alert_lifecycle import alert, CurrentOnlyExchange
from .test_experimental_execution_runtime import ROUTES, META


ACCOUNT = ROUTES['long_account']['account']


def fill(fid, oid, quantity):
    return dict(fill_id=fid, order_id=oid, quantity=str(quantity), price='93.544', at_ms=BASE)


def trade(cid, *, quantity='2', stop=True, take=False, symbol='HYPE'):
    source = alert(symbol=symbol, cycle=cid)
    source['occurrence_id'] = cid
    row = dict(cid=cid, source=source, account=ACCOUNT, role='long_account',
        symbol=symbol, side='LONG', phase='OPEN', quantity=quantity,
        prices=dict(entry='93.544', stop='91.405', take_profit='95.326'),
        asset=dict(index=1 if symbol == 'HYPE' else 4, decimals=2),
        entry_fills={}, exit_fills={}, orders={}, order_legs={},
        entry_request=None, condition=None, desired_stop='91.405')
    oid = str(100 * (1 + int(cid[0], 16)))
    entry = fill('entry-' + cid, oid, quantity)
    row['entry_fills'][entry['fill_id']] = entry
    row['orders'][oid] = dict(oid=oid, status='FILLED', fills=[entry],
        wire_order=dict(s=quantity, r=False, b=True))
    row['order_legs'][oid] = 'ENTRY'
    for offset, leg, enabled in ((1, 'STOP', stop), (2, 'TAKE_PROFIT', take)):
        if not enabled:
            continue
        exit_oid = str(int(oid) + offset)
        row['orders'][exit_oid] = dict(oid=exit_oid, status='OPEN', fills=[],
            wire_order=dict(s=quantity, r=True, b=False,
                p=row['prices']['stop' if leg == 'STOP' else 'take_profit']))
        row['order_legs'][exit_oid] = leg
    return row


def state(*trades):
    return dict(trades={t['cid']: t for t in trades}, requests={}, snapshots={},
        sources={t['cid']: dict(source=t['source'], cancellation=None,
            entry_permission='WAITING') for t in trades})


def proposal(owner, *, leg='TAKE_PROFIT', quantity=None, operation='CREATE_EXIT'):
    quantity = quantity or owner['quantity']
    price = owner['prices']['stop' if leg == 'STOP' else 'take_profit']
    return dict(card_id=owner['cid'], account=owner['account'], symbol=owner['symbol'],
        leg=leg, operation=operation, quantity=quantity,
        action=dict(type='order', grouping='na', orders=[dict(a=owner['asset']['index'],
            b=False, r=True, s=quantity, p=price)]))


class CapacityTests(unittest.TestCase):
    def setUp(self):
        self.first = trade('a' * 64)
        self.second = trade('b' * 64)
        self.state = state(self.first, self.second)

    def lane(self):
        return shared.assess(self.state)['lanes'][runtime._lane(ACCOUNT, 'HYPE')]

    def test_two_owned_stops_fit_each_own_allocation(self):
        lane = self.lane()
        self.assertTrue(lane['shared'])
        self.assertTrue(lane['capacity_safe_for_arbitrary_fills'])
        self.assertEqual([r['independent_exit_capacity'] for r in lane['trades']], ['2', '2'])
        report = shared.assess(self.state)
        self.assertFalse(report['native_per_trade_oco_verified'])
        self.assertFalse(report['shared_symbol_parallel_enabled'])

    def test_account_total_cannot_authorize_two_sibling_exits(self):
        # Own 2 + peer 2 = net 4. STOP 2 then TP 2 would consume the peer,
        # even though both orders are reduce-only and account never reverses.
        p = proposal(self.first)
        self.assertEqual(shared.proposal_reason(self.state, p),
                         'SHARED_CARD_INDEPENDENT_EXIT_CAPACITY_EXCEEDED')
        self.assertEqual(Decimal('4') - Decimal('2') - Decimal('2'), 0)
        self.assertEqual(runtime._remaining(self.second), Decimal('2'))

    def test_partial_exit_does_not_free_extra_capacity(self):
        oid = next(oid for oid, leg in self.first['order_legs'].items() if leg == 'STOP')
        f = fill('partial-stop', oid, '.75')
        self.first['orders'][oid]['fills'] = [f]
        self.first['exit_fills'][f['fill_id']] = f
        self.assertEqual(self.lane()['trades'][0]['independent_exit_capacity'], '1.25')
        self.assertEqual(shared.proposal_reason(self.state,
            proposal(self.first, quantity='1.25')), 'SHARED_CARD_INDEPENDENT_EXIT_CAPACITY_EXCEEDED')

    def test_unknown_submission_reserves_full_exit_capacity(self):
        self.first['orders'] = {oid: row for oid, row in self.first['orders'].items()
                                if self.first['order_legs'][oid] == 'ENTRY'}
        p = proposal(self.first, leg='STOP')
        self.state['requests']['r'] = dict(request_id='r', phase='OUTCOME_UNKNOWN', proposal=p)
        own = self.lane()['trades'][0]
        self.assertEqual(own['independent_exit_capacity'], '2')
        self.assertEqual(shared.proposal_reason(self.state, p),
                         'SHARED_EXIT_UNCERTAINTY_REQUIRES_RECONCILIATION')

    def test_pending_cancel_does_not_release_capacity(self):
        oid = next(oid for oid, leg in self.first['order_legs'].items() if leg == 'STOP')
        p = proposal(self.first, leg='STOP', operation='CANCEL')
        p['action'] = dict(type='cancel', cancels=[dict(a=1, o=int(oid))])
        self.state['requests']['r'] = dict(request_id='r', phase='OUTCOME_UNKNOWN', proposal=p)
        self.assertEqual(self.lane()['trades'][0]['independent_exit_capacity'], '2')
        self.assertEqual(shared.proposal_reason(self.state, proposal(self.first)),
                         'SHARED_EXIT_UNCERTAINTY_REQUIRES_RECONCILIATION')

    def test_exact_owned_cancel_can_reduce_existing_unsafe_capacity(self):
        p = proposal(self.first, leg='STOP', operation='CANCEL')
        oid = next(oid for oid, leg in self.first['order_legs'].items() if leg == 'STOP')
        p['action'] = dict(type='cancel', cancels=[dict(a=1, o=int(oid))])
        self.assertIsNone(shared.proposal_reason(self.state, p))
        foreign = next(oid for oid, leg in self.second['order_legs'].items() if leg == 'STOP')
        p['action']['cancels'][0]['o'] = int(foreign)
        self.assertEqual(shared.proposal_reason(self.state, p), 'EXACT_SHARED_CANCEL_OWNER_REQUIRED')

    def test_replacement_cannot_assume_no_fill_between_snapshot_and_modify(self):
        p = proposal(self.first, leg='STOP', operation='AMEND_EXIT')
        order = p['action']['orders'][0]
        oid = next(oid for oid, leg in self.first['order_legs'].items() if leg == 'STOP')
        p['action'] = dict(type='batchModify', modifies=[dict(oid=int(oid), order=order)])
        self.assertEqual(shared.proposal_reason(self.state, p),
                         'SHARED_EXIT_MODIFICATION_FILL_RACE_UNPROVEN')

    def test_emergency_close_cannot_run_alongside_live_owned_stop(self):
        p = proposal(self.first, leg='STOP', operation='EMERGENCY_CLOSE')
        self.assertEqual(shared.proposal_reason(self.state, p),
                         'SHARED_CARD_INDEPENDENT_EXIT_CAPACITY_EXCEEDED')

    def test_unprotected_owned_quantity_can_still_get_stop(self):
        self.first['orders'] = {oid: row for oid, row in self.first['orders'].items()
                                if self.first['order_legs'][oid] == 'ENTRY'}
        self.assertIsNone(shared.proposal_reason(self.state, proposal(self.first, leg='STOP')))

    def test_existing_unsafe_pair_is_reported_not_mistaken_for_oco(self):
        self.first = trade('a' * 64, stop=True, take=True)
        self.state = state(self.first, self.second)
        self.assertFalse(self.lane()['capacity_safe_for_arbitrary_fills'])
        self.assertEqual(self.lane()['trades'][0]['excess_independent_capacity'], '2')

    def test_single_owner_retains_existing_two_exit_behavior(self):
        self.assertIsNone(shared.proposal_reason(state(self.first), proposal(self.first)))

    def test_other_account_and_symbol_are_independent(self):
        self.second['account'] = ROUTES['short_account']['account']
        self.assertIsNone(shared.proposal_reason(self.state, proposal(self.first)))
        self.second['account'], self.second['symbol'] = ACCOUNT, 'ETH'
        self.assertIsNone(shared.proposal_reason(self.state, proposal(self.first)))

    def test_closed_but_residual_order_is_still_active_peer(self):
        self.second['phase'] = 'CLOSED'
        self.assertEqual(shared.proposal_reason(self.state, proposal(self.first)),
                         'SHARED_CARD_INDEPENDENT_EXIT_CAPACITY_EXCEEDED')

    def test_oversized_exit_accounting_is_rejected(self):
        f = fill('overclose', '9999', '3')
        self.first['exit_fills'][f['fill_id']] = f
        with self.assertRaisesRegex(shared.SharedMarketError, 'EXIT_EXCEEDED'):
            shared.assess(self.state)

    def test_duplicate_order_and_fill_ownership_cannot_be_reassigned(self):
        oid = next(iter(self.first['orders']))
        self.second['orders'][oid] = deepcopy(self.first['orders'][oid])
        self.second['order_legs'][oid] = 'ENTRY'
        with self.assertRaisesRegex(shared.SharedMarketError, 'ORDER_OWNERSHIP'):
            shared.assess(self.state)
        self.second['orders'].pop(oid)
        f = next(iter(self.first['entry_fills'].values()))
        self.second['entry_fills'][f['fill_id']] = deepcopy(f)
        with self.assertRaisesRegex(shared.SharedMarketError, 'MULTIPLE_SHARED_OWNERS'):
            shared.assess(self.state)

    def test_partial_fill_interleavings_keep_capacity_bound(self):
        # Every prefix, on either card independently: consumed + still
        # executable <= original allocation. No account-level cap substitutes.
        for a in ('0', '.25', '1', '1.75'):
            for b in ('0', '.5', '1', '1.5'):
                with self.subTest(first=a, second=b):
                    rows = [trade('a' * 64), trade('b' * 64)]
                    for owner, q in zip(rows, (a, b)):
                        if Decimal(q):
                            oid = next(k for k, leg in owner['order_legs'].items() if leg == 'STOP')
                            f = fill('partial-' + owner['cid'], oid, q)
                            owner['orders'][oid]['fills'].append(f)
                            owner['exit_fills'][f['fill_id']] = f
                    s = state(*rows)
                    for owner in rows:
                        p = proposal(owner, quantity=str(runtime._remaining(owner)))
                        self.assertEqual(shared.proposal_reason(s, p),
                                         'SHARED_CARD_INDEPENDENT_EXIT_CAPACITY_EXCEEDED')

    def test_maintenance_skips_unsafe_sibling_but_protects_another_owner(self):
        self.second = trade('b' * 64, stop=False)
        s = state(self.first, self.second)
        worker = object.__new__(runtime.IsolatedExecutionRuntime)
        marks = {runtime._lane(ACCOUNT, 'HYPE'): dict(environment='testnet',
            account=ACCOUNT, symbol='HYPE', at_ms=BASE, mark_price='93.544')}
        p = worker._maintain(s, dict(metadata=META, marks=marks), BASE)
        self.assertEqual(p['card_id'], self.second['cid'])
        self.assertEqual(p['leg'], 'STOP')
        self.assertEqual(self.first['shared_market_blocked_reason'],
                         'SHARED_CARD_INDEPENDENT_EXIT_CAPACITY_EXCEEDED')


class AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        for name in ('socket.create_connection', 'http.client.HTTPSConnection',
                     'hyperliquid_testnet_executor._wallet', 'hyperliquid_testnet_executor._signed_body'):
            blocker = patch(name, side_effect=AssertionError('NO_NETWORK_OR_SIGNER'))
            blocker.start(); self.addCleanup(blocker.stop)
        self.path = Path(self.tmp.name) / 'shared.isolated-experimental.sqlite3'
        self.venue = CurrentOnlyExchange(BASE + 1000)
        self.store = ExecutionState(self.path)
        self.store.initialize(ROUTES, not_before_ms=BASE - 60000)
        self.worker = runtime.IsolatedExecutionRuntime(self.store, self.venue, mode=runtime.MODE)

    def test_block_reason_survives_restart_and_duplicate_receipts(self):
        values = [alert(cycle='first'), alert(cycle='second')]
        self.worker.receive(values)
        for _ in range(3): self.worker.run_once()
        current = self.store.load()
        blocked = next(cid for cid in current['sources'] if cid not in current['trades'])
        self.assertEqual(current['entry_blocked'][blocked], shared.ENTRY_BLOCK)
        self.worker = runtime.IsolatedExecutionRuntime(ExecutionState(self.path), self.venue, mode=runtime.MODE)
        self.worker.receive(values)
        self.worker.run_once()
        self.assertEqual(len(self.store.load()['trades']), 1)
        self.assertEqual(len(self.venue.requests), 1)

    def test_concurrent_same_market_alerts_never_bypass_durable_fence(self):
        self.worker.receive([alert(cycle='one'), alert(cycle='two')])
        second = runtime.IsolatedExecutionRuntime(ExecutionState(self.path), self.venue, mode=runtime.MODE)
        def run(worker):
            try: return worker.run_once()['status']
            except runtime.RuntimeError as error: return str(error)
        with ThreadPoolExecutor(2) as pool:
            list(pool.map(run, [self.worker, second]))
        self.worker.run_once()
        self.assertEqual(len(self.store.load()['trades']), 1)
        self.assertEqual(sum(r['proposal']['operation'] == 'ENTRY' for r in self.venue.requests), 1)

    def test_next_same_symbol_alert_admits_after_exact_predecessor_finality(self):
        self.worker.receive([alert(cycle='one'), alert(cycle='two')])
        self.worker.run_once(); self.worker.run_once()
        current = self.store.load()
        first = next(iter(current['trades']))
        second = next(cid for cid in current['sources'] if cid != first)
        entry_oid = self.venue.oid('ENTRY')
        quantity = self.venue.orders[entry_oid]['view']['wire_order']['s']
        self.venue.fill(entry_oid, quantity)
        for _ in range(3): self.worker.run_once()
        self.assertEqual(len(self.store.load()['trades']), 1)
        self.venue.fill(self.venue.oid('TAKE_PROFIT'), quantity)
        for _ in range(4): self.worker.run_once()
        current = self.store.load()
        self.assertEqual(current['trades'][first]['phase'], 'CLOSED')
        self.assertIn(second, current['trades'])
        self.assertNotIn(second, current.get('entry_blocked', {}))
        self.assertEqual(sum(r['proposal']['operation'] == 'ENTRY' for r in self.venue.requests), 2)


if __name__ == '__main__':
    unittest.main()
