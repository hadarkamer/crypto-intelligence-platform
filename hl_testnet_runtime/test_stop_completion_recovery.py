"""Terminal market-stop recovery through real provider/runtime/sender adapters.

Only storage and external HTTP/signing are synthetic, as in the shared fixture.
These cases preserve the venue's changed post-trigger representation rather
than making its terminal response look like the original signed stop.
"""
from copy import deepcopy
from decimal import Decimal
import unittest
from unittest.mock import patch

from approved_alert_fixtures import formula_alert
from . import experimental_execution_runtime as core
from . import test_experimental_live_service as connection
from .test_experimental_execution_runtime import T


class StopCompletionRecoveryTests(unittest.TestCase):
    setUp = connection.FullConnectionTests.setUp
    build = connection.FullConnectionTests.build
    reconnect = connection.FullConnectionTests.reconnect
    ingest = connection.FullConnectionTests.ingest
    seed = connection.FullConnectionTests.seed

    def stopped_sol(self, *, gap_ms=0, cancel_take=True):
        approved = (T // 1800000 + 1) * 1800000 + 120000
        self.oracle.t = approved + 10000
        self.build(approved - 180000)
        msg = formula_alert('sol_g65', approved_ms=approved)
        self.seed(msg)
        self.assertEqual(self.service.tick().get('operation'), 'ENTRY')
        entry = self.oracle.oid('ENTRY')
        self.oracle.fill(entry, self.oracle.orders[entry]['view']['wire_order']['s'])
        account = self.release['routes']['short_account']
        self.oracle.mark[core._lane(account, 'SOL')] = msg['entry']
        for _ in range(3):
            self.service.tick()
        trade = self.store.load()['trades'][msg['occurrence_id']]
        self.assertEqual(trade['phase'], 'OPEN')
        stop, take = self.oracle.oid('STOP'), self.oracle.oid('TAKE_PROFIT')
        self.oracle.t += 192000
        self.oracle.fill(stop, trade['quantity'], price='101.31')
        if cancel_take:
            self.oracle.orders[take]['view'].update(status='CANCELED', at_ms=self.oracle.t)
        self.oracle.t += gap_ms
        self.release['entry_expires_at_ms'] = self.oracle.t + 86400000
        original = self.raw.status

        def terminal_status(oid):
            raw = original(oid)
            if str(oid) == stop:
                raw['order']['order'].update(
                    isTrigger=False, triggerPx='0.0', sz='0.0', limitPx='111.37')
            if str(oid) == take and self.oracle.orders[take]['view']['status'] == 'CANCELED':
                raw['order']['status'] = 'reduceOnlyCanceled'
            return raw

        # Recreate the durable lane fence left by the pre-fix process, then
        # recover with a newly composed worker without editing any journal row.
        lane = core._lane(account, 'SOL')
        self.store.mutate(lambda state: state['blocked_lanes'].update(
            {lane: 'ORDER_TERMS_NOT_BOUND_TO_INTENT'}))
        self.reconnect()
        return msg, stop, take, terminal_status

    def recover_and_reopen(self, *, gap_ms=0):
        msg, stop, take, terminal_status = self.stopped_sol(gap_ms=gap_ms)
        old_requests = deepcopy(self.store.load()['requests'])
        with patch.object(self.raw, 'status', side_effect=terminal_status):
            for _ in range(3):
                self.service.tick()
            self.reconnect()
            self.service.tick()
        value = self.store.load()
        closed = value['trades'][msg['occurrence_id']]
        self.assertEqual(closed['phase'], 'CLOSED')
        self.assertEqual(closed['orders'][stop]['status'], 'FILLED')
        self.assertEqual(closed['orders'][take]['status'], 'CANCELED')
        self.assertEqual(core._remaining(closed), Decimal(0))
        self.assertEqual(value['blocked_lanes'], {})
        self.assertEqual(self.service.last_entry_decisions['account_blocks'], 0)
        self.assertEqual(len(self.http), 3)  # No repair trade or repeated exit.
        self.assertEqual(value['requests'], old_requests)
        self.assertIn('settlement', closed)
        settlement = deepcopy(closed['settlement'])
        self.assertLess(Decimal(settlement['net_usd']), 0)
        portfolio = deepcopy(value['formula_accounting']['portfolios'][msg['rule_id']])

        approved = (self.oracle.t // 1800000 + 1) * 1800000 + 120000
        self.oracle.t = approved + 10000
        later = formula_alert('sol_g65', approved_ms=approved)
        self.seed(later)
        self.assertEqual(self.service.tick().get('operation'), 'ENTRY')
        value = self.store.load()
        self.assertEqual(value['trades'][msg['occurrence_id']]['settlement'], settlement)
        self.assertEqual(value['formula_accounting']['portfolios'][msg['rule_id']], portfolio)
        self.assertEqual(value['trades'][later['occurrence_id']]['entry_risk']['balance_usd'],
                         settlement['balance_after_usd'])
        self.assertEqual(sum(r['proposal']['operation'] == 'ENTRY' for r in self.oracle.requests), 2)
        self.assertEqual(len(self.http), 4)

    def test_filled_stop_clears_old_fence_settles_once_and_next_alert_can_enter(self):
        self.recover_and_reopen()

    def test_fifty_hour_restart_recovers_filled_stop_without_replaying_orders(self):
        self.recover_and_reopen(gap_ms=50 * 3600000)

    def test_leftover_take_is_canceled_only_after_full_stop_fill_is_proved(self):
        msg, stop, take, terminal_status = self.stopped_sol(cancel_take=False)
        with patch.object(self.raw, 'status', side_effect=terminal_status):
            for _ in range(4):
                self.service.tick()
        closed = self.store.load()['trades'][msg['occurrence_id']]
        self.assertEqual(closed['phase'], 'CLOSED')
        self.assertEqual(self.oracle.orders[take]['view']['status'], 'CANCELED')
        self.assertEqual(len(self.http), 4)
        last = self.oracle.requests[-1]['proposal']
        self.assertEqual(last['operation'], 'CANCEL')
        self.assertEqual(str(last['action']['cancels'][0]['o']), take)
        self.assertEqual(sum(r['proposal']['leg'] == 'STOP' for r in self.oracle.requests), 1)


if __name__ == '__main__':
    unittest.main()
