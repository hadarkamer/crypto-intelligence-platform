"""Offline tests; real budget code, network replies and final transport mocked."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from . import guarded_execution as guard

A = '0x' + '1' * 40
B = '0x' + '2' * 40


def message():
    return {'kind': 'SIGNAL', 'event_id': 'fixture-1', 'symbol': 'DEMO', 'side': 'LONG',
            'entry': '100.00', 'stop': '98', 'take_profit': '104',
            'at': (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()}


class Reader:
    def __init__(self):
        self.calls = 0
        self.balance = '1000'
        self.mark = '100'
        self.available = '1000'
        self.leverage = 10
        self.max_size = '100'
        self.role = {'role': 'agent', 'data': {'user': A}}
    def read(self, kind, *, user=None, coin=None):
        self.calls += 1
        if kind == 'userRole':
            return {'role': 'user'} if user == A else self.role
        if kind == 'userAbstraction':
            return 'unifiedAccount'
        if kind == 'spotClearinghouseState':
            return {'balances': [{'coin': 'USDC', 'token': 0, 'total': self.balance, 'hold': '0'}]}
        if kind == 'activeAssetData':
            return {'coin': 'DEMO', 'user': A, 'markPx': self.mark,
                'availableToTrade': [self.available] * 2, 'maxTradeSzs': [self.max_size] * 2,
                'leverage': {'type': 'cross', 'value': self.leverage}}
        if kind == 'meta':
            return {'universe': [{'name': 'DEMO', 'szDecimals': 2, 'maxLeverage': 10}]}
        raise AssertionError('Unexpected read type')


class GuardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        os.chmod(self.root, 0o700)
        self.journal = self.root / 'one.hl-testnet.sqlite3'
        self.signal = message()
        self.reader = Reader()
        self.network = patch('http.client.HTTPSConnection', side_effect=AssertionError('Offline tests only'))
        self.network.start()
    def tearDown(self):
        self.network.stop()
        self.temp.cleanup()
    def review(self, **kw):
        args = dict(account=A, agent=B, journal=self.journal, exit_type='limit', client=self.reader, env={})
        args.update(kw)
        return guard.review_only(self.signal, **args)
    def test_exact_budget_digest_and_unchanged_source(self):
        before = deepcopy(self.signal)
        result = self.review()
        self.assertTrue(result['eligible_for_controlled_attempt'])
        self.assertTrue(result['budget_plan_bound'])
        self.assertEqual(self.signal, before)
        self.assertFalse(self.journal.exists())
    def test_current_leverage_used_without_changing_it(self):
        result = self.review()
        # 10 units * 100 = 1000; the old full-cash +1% gate fails at balance 1000.
        self.assertTrue(result['budget_passed'])
        self.assertEqual(self.reader.leverage, 10)
    def test_low_budget_blocks(self):
        self.reader.available = '1'
        self.assertFalse(self.review()['eligible_for_controlled_attempt'])
    def test_quantity_cap_blocks(self):
        self.reader.max_size = '0.01'
        self.assertFalse(self.review()['budget_passed'])
    def test_outside_price_range_blocks_even_when_budget_passes(self):
        self.reader.mark = '105'
        result = self.review()
        self.assertTrue(result['budget_passed'])
        self.assertIn('TESTNET_PRICE_OUTSIDE_SUPPLIED_EXIT_RANGE', result['blockers'])
    def test_price_on_stop_is_not_safe(self):
        self.reader.mark = '98'
        self.assertFalse(self.review()['eligible_for_controlled_attempt'])
    def test_stale_and_future_source_not_retimestamped(self):
        for seconds in (-120, 10):
            self.signal['at'] = (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()
            original = self.signal['at']
            result = self.review()
            self.assertIn('SOURCE_NOT_FRESH_FOR_ONE_SHOT_TEST', result['blockers'])
            self.assertEqual(original, self.signal['at'])
    def test_unknown_exit_type_blocked(self):
        for value in (None, 'auto', '', True):
            self.assertIn('EXPLICIT_EXIT_TYPE_REQUIRED', self.review(exit_type=value)['blockers'])
    def test_no_journal_means_no_dispatch(self):
        result = self.review(journal=None)
        self.assertIn('PERSISTENT_JOURNAL_NOT_CONFIGURED', result['blockers'])
    def test_render_ephemeral_directory_refused(self):
        self.assertEqual(guard.journal_status(self.journal, env={'RENDER':'true'}),
                         'RENDER_PERSISTENT_STORAGE_REQUIRED')
    def test_private_path_and_symlink_refused(self):
        self.assertNotEqual(guard.journal_status('relative.hl-testnet.sqlite3', env={}), 'JOURNAL_PATH_CHECKED')
        os.chmod(self.root, 0o755)
        self.assertNotEqual(guard.journal_status(self.journal, env={}), 'JOURNAL_PATH_CHECKED')
        os.chmod(self.root, 0o700)
        self.journal.symlink_to(self.root / 'other')
        self.assertNotEqual(guard.journal_status(self.journal, env={}), 'JOURNAL_PATH_CHECKED')
    def test_changed_private_permissions_on_existing_file_refused(self):
        self.journal.write_text('x'); os.chmod(self.journal, 0o644)
        self.assertNotEqual(guard.journal_status(self.journal, env={}), 'JOURNAL_PATH_CHECKED')
    def test_no_precision_rounding_and_specific_field_report(self):
        self.signal['stop'] = '98.123456'
        result = self.review()
        self.assertEqual(result['price_precision_fields'], ['stop'])
        self.assertEqual(self.signal['stop'], '98.123456')
        self.assertFalse(result['eligible_for_controlled_attempt'])
    def test_actual_format_precision_limitation(self):
        # Generic decimal example, not a user's strategy record.
        self.signal.update(entry='96.88', stop='94.9424', take_profit='98.8176')
        result = self.review()
        self.assertEqual(result['price_precision_fields'], ['stop', 'take_profit'])
    def test_missing_extra_secret_fields_block_before_reads(self):
        self.signal['secret'] = 'DO_NOT_STORE'
        result = self.review()
        self.assertFalse(result['eligible_for_controlled_attempt'])
        self.assertEqual(self.reader.calls, 0)
        self.assertNotIn('DO_NOT_STORE', json.dumps(result))
    def test_no_private_key_needed_for_review(self):
        with patch.object(guard.checks, 'key_matches', side_effect=AssertionError('No key use')):
            self.assertTrue(self.review()['budget_passed'])
    def test_review_never_includes_account_or_prices(self):
        body = json.dumps(self.review())
        for text in (A, B, '100.00', '"entry":', '"take_profit":'):
            self.assertNotIn(text, body)
    def test_wrong_account_mapping_blocks(self):
        self.reader.role['data']['user'] = B
        self.assertFalse(self.review()['budget_passed'])
    def test_error_cannot_echo_upstream_text(self):
        self.reader.read = Mock(side_effect=RuntimeError('DO_NOT_ECHO'))
        body = json.dumps(self.review())
        self.assertNotIn('DO_NOT_ECHO', body)
    def test_changed_budget_digest_blocks(self):
        original = guard.checks.run_check
        def corrupted(*args, **kw):
            r = original(*args, **kw); r['budget_diagnostics']['plan_sha256'] = 'wrong'; return r
        with patch.object(guard.checks, 'run_check', side_effect=corrupted):
            self.assertIn('BUDGET_NOT_BOUND_TO_EXACT_PLAN', self.review()['blockers'])
    def test_disabled_does_no_work(self):
        with patch.object(guard, 'review_only', side_effect=AssertionError('No review')):
            for value in (False, 'true', 1, None):
                r = guard.submit_checked(self.signal, account=A, agent=B, journal=self.journal,
                                         exit_type='limit', enable_testnet=value)
                self.assertEqual(r['status'], 'DISABLED')
    def test_blocked_review_never_calls_sender(self):
        sender = SimpleNamespace(submit_once=Mock(side_effect=AssertionError('Cannot send')))
        with patch.dict('sys.modules', {'hyperliquid_testnet_executor':sender}), \
                patch.object(guard.checks, 'InfoReader', return_value=self.reader):
            r = guard.submit_checked(self.signal, account=A, agent=B, journal=None,
                                     exit_type='limit', enable_testnet=True)
        self.assertEqual(r['status'], 'BLOCKED_BEFORE_SIGNING')
        sender.submit_once.assert_not_called()
    def test_success_hands_exact_source_once_to_sender(self):
        sender = SimpleNamespace(submit_once=Mock(return_value={'status':'MOCK_RECEIPT', 'order_batches_sent':1}))
        with patch.dict('sys.modules', {'hyperliquid_testnet_executor':sender}), \
                patch.object(guard.checks, 'InfoReader', return_value=self.reader):
            r = guard.submit_checked(self.signal, account=A, agent=B, journal=self.journal,
                                     exit_type='limit', enable_testnet=True)
        sender.submit_once.assert_called_once_with(self.signal, account=A, journal=self.journal,
                                                  exit_type='limit', enable_testnet=True)
        self.assertTrue(r['budget_gate_passed'])
    def test_slow_budget_not_dispatched(self):
        review = self.review()
        with patch.object(guard, 'review_only', return_value=review), \
                patch.object(guard.time, 'monotonic', side_effect=[0, 9]):
            r = guard.submit_checked(self.signal, account=A, agent=B, journal=self.journal,
                                     exit_type='limit', enable_testnet=True)
        self.assertEqual(r['status'], 'BUDGET_SAMPLE_EXPIRED_BEFORE_DISPATCH')


if __name__ == '__main__':
    unittest.main(verbosity=2)
