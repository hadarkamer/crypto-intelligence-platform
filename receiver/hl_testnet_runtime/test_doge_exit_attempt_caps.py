"""Terminal partial IOC results retain native protection after the retry cap."""
from decimal import Decimal
import unittest
from unittest.mock import patch

import approved_alert_contract as contract
from . import experimental_execution_runtime as runtime
from . import test_doge_partial_execution as support


class DogeExitAttemptCapTests(unittest.TestCase):
    def setUp(self):
        self.h = support.DogePartialRuntimeTests()
        self.h.setUp()
        self.addCleanup(self.h.doCleanups)

    def test_timeout_cap_still_resizes_original_stop_for_proven_remainder(self):
        h = self.h
        h.start()
        original_stop = h.trade()['prices']['stop']
        h.venue.instant_fraction = Decimal('.5')
        h.venue.t = contract.moment_ms(h.msg['policy']['hold_until'])
        with patch('hl_testnet_runtime.emergency_close.MAX_REQUESTS', 2):
            h.run_cycles(8)
        attempts = [r for r in h.venue.requests if r['proposal']['operation']=='TIME_EXIT']
        self.assertEqual(len(attempts), 2)
        self.assertGreater(runtime._remaining(h.trade()), 0)
        self.assertEqual(h.active('STOP')[0][1]['wire_order']['p'], original_stop)
        h.assert_protected()
        self.assertEqual(h.trade()['exit_attempt_limit_reason'], 'TIME_EXIT_RECONCILIATION_OR_LIMIT_REQUIRED')

    def test_partial_cap_does_not_starve_final_target(self):
        h = self.h
        h.start(fill=False)
        h.venue.fill(h.venue.oid('ENTRY'), h.trade()['quantity'])
        h.worker.run_once(entries_enabled=False)  # Original stop first.
        h.mark(h.msg['policy']['partial_take_price'])
        h.venue.instant_fraction = Decimal('.5')
        with patch('hl_testnet_runtime.emergency_close.MAX_REQUESTS', 2):
            h.run_cycles(8)
        attempts = [r for r in h.venue.requests if r['proposal']['operation']=='PARTIAL_TAKE']
        self.assertEqual(len(attempts), 2)
        self.assertEqual(h.trade()['exit_attempt_limit_reason'], 'PARTIAL_TAKE_RECONCILIATION_OR_LIMIT_REQUIRED')
        final_oid, final = h.take(False)
        self.assertGreater(Decimal(final['wire_order']['s']), 0)
        stop = h.active('STOP')[0][1]
        self.assertEqual(Decimal(stop['wire_order']['s']), runtime._remaining(h.trade()))
        # Once price returns below the partial target, its original native
        # order can protect the residual share without replaying an IOC.
        h.mark(h.msg['entry'])
        with patch('hl_testnet_runtime.emergency_close.MAX_REQUESTS', 2):
            h.run_cycles(4)
        h.assert_protected()
        self.assertEqual(h.take(False)[0], final_oid)


if __name__ == '__main__':
    unittest.main()
