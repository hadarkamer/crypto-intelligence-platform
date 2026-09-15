"""Actual budget -> actual existing sender -> fake Testnet HTTP integration.

Executed in CI where existing executor files are present. No real credentials,
account, signatures or order requests leave the process.
"""
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

import hyperliquid_testnet_executor as sender
from hyperliquid_testnet_executor_selftest import FakeExchange, ACCOUNT, AGENT, signal
from . import guarded_execution as guard
from .test_guarded_execution import Reader


class IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.journal = Path(self.temp.name) / 'case.hl-testnet.sqlite3'
        self.signal = signal()
        self.reader = Reader()
        self.exchange = FakeExchange()
        patches = [
            patch.object(sender.http.client, 'HTTPSConnection', side_effect=self.exchange.connection),
            patch.object(guard.checks, 'InfoReader', return_value=self.reader),
            patch.object(sender, '_wallet', return_value=SimpleNamespace(address=AGENT)),
            patch.object(sender, '_signed_body', side_effect=lambda wallet, action, nonce: {
                'action': action, 'nonce': nonce, 'signature': {'r':'0x1','s':'0x2','v':27},
                'expiresAfter':nonce+30000}),
        ]
        for item in patches:
            item.start(); self.addCleanup(item.stop)
        self.addCleanup(self.temp.cleanup)
    def run_case(self):
        return guard.submit_checked(self.signal, account=ACCOUNT, agent=AGENT,
                                    journal=self.journal, exit_type='limit', enable_testnet=True)
    def order_calls(self):
        return [x for x in self.exchange.calls if x[0] == '/exchange']
    def test_budget_then_three_orders_then_readback(self):
        result = self.run_case()
        self.assertTrue(result['budget_gate_passed'])
        self.assertEqual(result['status'], 'VERIFIED_OPEN_PROTECTED')
        self.assertEqual(len(self.order_calls()), 1)
        self.assertEqual(len(self.order_calls()[0][1]['action']['orders']), 3)
    def test_unchanged_signal_retry_not_submitted_twice(self):
        self.run_case()
        result = self.run_case()
        self.assertTrue(result['replayed'])
        self.assertEqual(len(self.order_calls()), 1)
    def test_budget_failure_before_wallet_or_journal(self):
        self.reader.balance = '1'
        result = self.run_case()
        self.assertEqual(result['status'], 'BLOCKED_BEFORE_SIGNING')
        self.assertEqual(len(self.order_calls()), 0)
        self.assertFalse(self.journal.exists())
        sender._wallet.assert_not_called()
    def test_uncertain_receipt_persisted_not_retried(self):
        self.exchange.timeout = True
        self.assertEqual(self.run_case()['status'], 'UNCERTAIN_REQUIRES_REVIEW')
        result = self.run_case()
        self.assertTrue(result['replayed'])
        self.assertEqual(len(self.order_calls()), 1)
    def test_unprotected_partial_not_declared_success(self):
        self.exchange.partial = True
        result = self.run_case()
        self.assertEqual(result['status'], 'PARTIAL_ENTRY_REQUIRES_REVIEW')
        self.assertFalse(result['protection_active'])
        self.assertFalse(result['verified'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
