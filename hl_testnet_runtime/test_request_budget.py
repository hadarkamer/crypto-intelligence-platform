"""Offline weight tests and disposable PostgreSQL cross-process admission races."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import os
import subprocess
import sys
import time
import unittest
from unittest.mock import Mock, patch

from . import postgres_journal as pg, request_budget as budget

CI_URL = os.environ.get('HL_JOURNAL_CI_URL')


class PureTests(unittest.TestCase):
    def test_documented_weights_and_conservative_fill_upper_bound(self):
        for kind in budget.LIGHT:
            self.assertEqual(budget.request_weight('/info', {'type': kind}), 2)
        self.assertEqual(budget.request_weight('/info', {'type': 'userRole'}), 60)
        for kind in budget.NORMAL:
            self.assertEqual(budget.request_weight('/info', {'type': kind}), 20)
        for kind in budget.SIZED:
            self.assertEqual(budget.request_weight('/info', {'type': kind}), 120)
        for length, expected in ((1, 1), (39, 1), (40, 2), (79, 2), (80, 3)):
            for kind, key in (('order', 'orders'), ('cancel', 'cancels'),
                              ('batchModify', 'modifies'), ('cancelByCloid', 'cancels')):
                self.assertEqual(budget.request_weight('/exchange',
                    {'action': {'type': kind, key: [{}] * length}}), expected)

    def test_other_host_unknown_types_and_unbounded_bodies_fail_closed(self):
        cases = (('/info', {'type': 'unknown'}), ('/info', {'type': []}),
                 ('/foo', {}), ('/info', []), ('/exchange', {'action': {'type': 'withdraw3'}}),
                 ('/exchange', {'action': {'type': []}}),
                 ('/exchange', {'action': {'type': 'order', 'orders': []}}),
                 ('/exchange', {'action': {'type': 'order', 'orders': [{}] * 1001}}))
        for path, body in cases:
            with self.assertRaises(budget.BudgetError):
                budget.request_weight(path, body)
        with self.assertRaises(budget.BudgetError):
            budget.request_weight('/info', {'type': 'meta'}, host='api.hyperliquid.xyz')

    def test_only_unconfigured_isolated_readonly_tools_are_optional(self):
        self.assertIsNone(budget.Budget.from_env({}))
        self.assertIsNone(budget.Budget.from_env({'HL_TESTNET_RUNTIME_MODE': 'read_only'}))
        for env in ({'HL_TESTNET_JOURNAL_BACKEND': 'staging_postgres_v1'},
                    {'HL_TESTNET_RUNTIME_MODE': 'long_stream_testnet_v1'},
                    {'RENDER_SERVICE_ID': 'srv-dakptbh594qs7395460g'},
                    {'HL_TESTNET_DATABASE_URL': 'DO_NOT_EXPOSE'}):
            with self.assertRaises(budget.BudgetError) as caught:
                budget.Budget.from_env(env)
            self.assertNotIn('DO_NOT_EXPOSE', str(caught.exception))

    def permit(self, kind='userFillsByTime', *, deadline=None):
        owner = Mock()
        owner._settle.return_value = True
        return budget.Permit(owner, 'a' * 32, kind, 120,
            time.monotonic_ns() + 1000000000 if deadline is None else deadline), owner

    def test_one_permit_cannot_authorize_two_transports_even_concurrently(self):
        permit, _ = self.permit()
        def consume(_):
            try:
                permit.check()
                return True
            except budget.BudgetError:
                return False
        with ThreadPoolExecutor(max_workers=4) as pool:
            self.assertEqual(sum(pool.map(consume, range(12))), 1)

    def test_expiry_cannot_reset_time_or_refund_an_unsent_request(self):
        permit, owner = self.permit(deadline=time.monotonic_ns() - 1)
        with self.assertRaisesRegex(budget.BudgetError, 'PERMIT_EXPIRED'):
            permit.check()
        self.assertFalse(permit.finish([]))
        owner._settle.assert_not_called()

    def test_refund_uses_ceiling_including_incomplete_twenty_item_group(self):
        for count, weight in ((0, 20), (1, 21), (20, 21), (21, 22), (2000, 120)):
            permit, owner = self.permit()
            permit.check()
            self.assertTrue(permit.finish([{}] * count))
            owner._settle.assert_called_once_with('a' * 32, weight)

    def test_invalid_or_oversized_response_never_refunds(self):
        for response in (None, {}, 'INVALID', [{}] * 2001, [1]):
            permit, owner = self.permit()
            permit.check()
            self.assertFalse(permit.finish(response))
            owner._settle.assert_not_called()
        permit, owner = self.permit(kind='clearinghouseState')
        permit.check()
        self.assertFalse(permit.finish({}))
        owner._settle.assert_not_called()


class TransportAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.network = patch('http.client.HTTPSConnection', side_effect=AssertionError('NO_HTTP_BEFORE_ADMISSION'))
        self.http = self.network.start()
        self.addCleanup(self.network.stop)

    def denied(self):
        denied = Mock()
        denied.acquire.side_effect = budget.BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED')
        return denied

    def test_legacy_entry_denied_without_recording_an_http_attempt(self):
        import hyperliquid_testnet_executor as sender
        from hyperliquid_testnet_executor_selftest import action
        client = sender.TestnetHTTP(allow_orders=True, budget=self.denied())
        with self.assertRaisesRegex(sender.TestnetError, 'BUDGET_EXHAUSTED'):
            client._post('/exchange', {'action': action()})
        self.assertEqual(client.order_attempts, 0)
        self.http.assert_not_called()

    def test_legacy_exchange_requires_budget_even_in_isolated_unconfigured_process(self):
        import hyperliquid_testnet_executor as sender
        from hyperliquid_testnet_executor_selftest import action
        with patch.object(budget.Budget, 'from_env', return_value=None):
            client = sender.TestnetHTTP(allow_orders=True)
        with self.assertRaisesRegex(sender.TestnetError, 'SHARED_REQUEST_BUDGET_REQUIRED'):
            client._post('/exchange', {'action': action()})
        self.assertEqual(client.order_attempts, 0)
        self.http.assert_not_called()

    def test_all_public_raw_transport_classes_admit_before_http(self):
        from . import checks, card_sync_evidence, account_ledger_review, doge_lifecycle_review
        account = '0x' + '1' * 40
        cases = (
            (checks.InfoReader(budget=self.denied()), lambda reader: reader.read('meta')),
            (card_sync_evidence.PublicReader(budget=self.denied()),
             lambda reader: reader.read('clearinghouseState', account)),
            (account_ledger_review.HistoryReader(budget=self.denied()),
             lambda reader: reader.read('userFillsByTime', account, 1, 2)),
            (doge_lifecycle_review.PublicReader(budget=self.denied()),
             lambda reader: reader.read('clearinghouseState', account)),
        )
        for reader, call in cases:
            with self.assertRaisesRegex(ValueError, 'BUDGET_EXHAUSTED'):
                call(reader)
        self.http.assert_not_called()

@unittest.skipUnless(CI_URL, 'Requires disposable PostgreSQL CI service')
class PostgresTests(unittest.TestCase):
    def setUp(self):
        self.journal = pg.PostgresJournal.for_ci(CI_URL)
        with self.journal._transaction() as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS {pg.SCHEMA} CASCADE')
        self.journal.bootstrap()
        self.budget = budget.Budget(self.journal)
        self.budget.initialize()

    def weights(self):
        with self.journal._transaction() as conn:
            return conn.execute(f'SELECT COALESCE(sum(weight),0) FROM {budget.TICKETS}').fetchone()[0]

    def fill_background(self):
        for _ in range(40):
            self.budget.acquire('/info', {'type': 'meta'})
        self.assertEqual(self.weights(), 800)

    def test_background_cannot_consume_reserved_protection_capacity(self):
        self.fill_background()
        other = budget.Budget(pg.PostgresJournal.for_ci(CI_URL))
        with self.assertRaisesRegex(budget.BudgetError, 'EXHAUSTED'):
            other.acquire('/info', {'type': 'clearinghouseState'})
        for _ in range(20):
            other.acquire('/info', {'type': 'meta'}, priority='protection')
        self.assertEqual(self.weights(), 1200)
        with self.assertRaisesRegex(budget.BudgetError, 'EXHAUSTED'):
            self.budget.acquire('/exchange', {'action': {'type': 'cancel', 'cancels': [{}]}},
                                priority='protection')

    def test_concurrent_workers_never_overspend_and_busy_is_bounded(self):
        self.fill_background()
        with self.journal._transaction() as conn:
            conn.execute(f'UPDATE {budget.TICKETS} SET weight=weight-1')
        self.assertEqual(self.weights(), 760)
        def admit(_):
            try:
                budget.Budget(pg.PostgresJournal.for_ci(CI_URL)).acquire('/info', {'type': 'meta'})
                return True
            except budget.BudgetError as exc:
                self.assertIn(str(exc), ('TESTNET_REQUEST_BUDGET_BUSY', 'TESTNET_REQUEST_BUDGET_EXHAUSTED'))
                return False
        with ThreadPoolExecutor(max_workers=8) as pool:
            granted = sum(pool.map(admit, range(24)))
        self.assertLessEqual(granted, 2)
        self.assertEqual(self.weights(), 760 + 20 * granted)

    def test_new_process_shares_old_admissions_without_reinitializing_or_reading_keys(self):
        self.fill_background()
        program = '''import os
from hl_testnet_runtime.postgres_journal import PostgresJournal
from hl_testnet_runtime.request_budget import Budget,BudgetError
b=Budget(PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL']))
try: b.acquire('/info',{'type':'meta'})
except BudgetError as e: assert str(e)=='TESTNET_REQUEST_BUDGET_EXHAUSTED'
else: raise AssertionError('Separate process overspent shared IP allowance')
b.acquire('/exchange',{'action':{'type':'cancel','cancels':[{}]}},priority='protection')
'''
        subprocess.run([sys.executable, '-c', program], check=True, timeout=10,
                       env={'HL_JOURNAL_CI_URL': CI_URL, 'PATH': os.environ.get('PATH', '')})
        self.assertEqual(self.weights(), 801)

    def test_dynamic_size_refund_commits_once_and_lost_refund_retains_full_charge(self):
        permit = self.budget.acquire('/info', {'type': 'userFillsByTime'}, priority='protection')
        permit.check()
        self.assertEqual(self.weights(), 120)
        self.assertTrue(permit.finish([{}] * 21))
        self.assertEqual(self.weights(), 22)
        self.assertFalse(permit.finish([]))
        self.assertEqual(self.weights(), 22)
        permit = self.budget.acquire('/info', {'type': 'userFillsByTime'}, priority='protection')
        permit.check()
        with patch.object(self.budget, '_transaction', side_effect=budget.BudgetError('TESTNET_SHARED_REQUEST_BUDGET_UNAVAILABLE')):
            self.assertFalse(permit.finish([]))
        self.assertEqual(self.weights(), 142)

    def test_uncertain_admission_commit_returns_no_permit(self):
        original = self.budget._transaction
        @contextmanager
        def lost_ack():
            with original() as conn:
                yield conn
            raise budget.BudgetError('TESTNET_SHARED_REQUEST_BUDGET_UNAVAILABLE')
        with patch.object(self.budget, '_transaction', lost_ack):
            with self.assertRaises(budget.BudgetError):
                self.budget.acquire('/info', {'type': 'meta'})
        self.assertEqual(self.weights(), 20)

    def test_bounded_lock_does_not_wait_for_quota_or_another_process(self):
        with self.journal._transaction() as conn:
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (budget.LOCK,))
            started = time.monotonic()
            with self.assertRaisesRegex(budget.BudgetError, 'BUDGET_BUSY'):
                self.budget.acquire('/info', {'type': 'meta'}, priority='protection')
            self.assertLess(time.monotonic() - started, 2)
        self.assertEqual(self.weights(), 0)

    def test_four_parallel_read_admissions_have_no_spurious_denial_with_headroom(self):
        from threading import Barrier
        barrier = Barrier(4)
        def admit(_):
            local = budget.Budget(pg.PostgresJournal.for_ci(CI_URL))
            barrier.wait(timeout=2)
            return local.acquire('/info', {'type': 'meta'}, priority='protection')
        with ThreadPoolExecutor(max_workers=4) as pool:
            permits = list(pool.map(admit, range(4)))
        self.assertEqual(len(permits), 4)
        self.assertEqual(self.weights(), 80)

    def test_missing_or_changed_schema_is_not_repaired_by_admission(self):
        with self.journal._transaction() as conn:
            conn.execute(f'UPDATE {budget.POLICY} SET maximum=1201')
        with self.assertRaisesRegex(budget.BudgetError, 'SCHEMA_REQUIRES_REVIEW'):
            self.budget.acquire('/info', {'type': 'meta'})
        with self.journal._transaction() as conn:
            conn.execute(f'DROP TABLE {budget.POLICY}')
        with self.assertRaisesRegex(budget.BudgetError, 'UNAVAILABLE'):
            self.budget.acquire('/info', {'type': 'meta'})
        with self.journal._transaction() as conn:
            self.assertIsNone(conn.execute('SELECT to_regclass(%s)', (budget.POLICY,)).fetchone()[0])

    def test_rolling_window_keeps_60_seconds_plus_admission_tcp_and_tls_margin(self):
        self.fill_background()
        with self.journal._transaction() as conn:
            conn.execute(f"UPDATE {budget.TICKETS} SET admitted_at=clock_timestamp()-interval '68 seconds'")
        with self.assertRaisesRegex(budget.BudgetError, 'EXHAUSTED'):
            self.budget.acquire('/info', {'type': 'meta'})
        with self.journal._transaction() as conn:
            conn.execute(f"UPDATE {budget.TICKETS} SET admitted_at=clock_timestamp()-interval '69.1 seconds'")
        self.budget.acquire('/info', {'type': 'meta'})
        self.assertEqual(self.weights(), 20)

    def test_slow_commit_does_not_restart_freshness_clock(self):
        with patch.object(budget.time, 'monotonic_ns', side_effect=(1000000000, 2000000001)):
            with self.assertRaisesRegex(budget.BudgetError, 'PERMIT_EXPIRED'):
                self.budget.acquire('/info', {'type': 'meta'})
        self.assertEqual(self.weights(), 20)


if __name__ == '__main__':
    unittest.main()
