"""History catch-up must leave live protection capacity and never bypass funding.

Only fake HTTP and a disposable PostgreSQL journal are used. These tests cover
the reservation boundary while recovery tests cover the durable cursor itself.
"""
from copy import deepcopy
import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from . import card_sync_evidence as evidence, postgres_journal as pg
from . import request_budget as budget
from . import history_gap_recovery as gap
from .test_card_sync import evidence as closed_evidence
from .test_card_lifecycle import A, T


CI_URL = os.environ.get('HL_JOURNAL_CI_URL')


def history_plan(start, end):
    body = dict(type='userFillsByTime', user=A, startTime=start, endTime=end,
                aggregateByTime=False)
    return [body, deepcopy(body)]


def retained_history_plan(start, end):
    anchor = dict(fill=dict(at_ms=T - 1000))
    return gap.planned_chunk_reads(A, anchor, start, end)


class HistoryGapPlanTests(unittest.TestCase):
    def test_three_day_monolithic_replay_cannot_spend_protection_reserve(self):
        old = closed_evidence()
        end = T + 3 * evidence.DAY_MS + 55 * 60 * 1000
        bodies = evidence._planned_observation_reads(
            old['bindings'], old['snapshot'], T, end,
            reuse_verified_terminals=True)
        self.assertEqual(sum(budget.request_weight('/info', body)
                             for body in bodies), 1004)
        owner = budget.Budget(pg.PostgresJournal.for_ci(
            'postgresql://offline:unused@localhost/hl_journal_ci'))
        with patch.object(owner, '_transaction',
                          side_effect=AssertionError('DATABASE_MUST_NOT_BE_OPENED')), \
             patch('http.client.HTTPSConnection',
                   side_effect=AssertionError('HTTP_MUST_NOT_BEGIN')):
            with self.assertRaises(budget.BudgetError) as caught:
                owner.reserve_observation(bodies, priority='background')
        self.assertEqual(str(caught.exception), 'TESTNET_REQUEST_BUDGET_EXHAUSTED')
        self.assertEqual(caught.exception.requested_weight, 1004)
        self.assertEqual(caught.exception.ceiling, 800)
        self.assertIsNone(caught.exception.retry_after_ms)

    def test_one_day_history_and_final_checkpoint_have_bounded_fixed_plans(self):
        stop = T + evidence.DAY_MS - 2 * evidence.OVERLAP_MS
        bodies = history_plan(T - evidence.OVERLAP_MS, stop)
        self.assertEqual(sum(budget.request_weight('/info', body)
                             for body in bodies), 240)
        self.assertTrue(all(0 <= body['endTime'] - body['startTime']
                            <= evidence.DAY_MS for body in bodies))
        old = closed_evidence()
        final = evidence._planned_observation_reads(
            old['bindings'], old['snapshot'], stop,
            stop + 55 * 60 * 1000, reuse_verified_terminals=True)
        self.assertEqual(sum(budget.request_weight('/info', body)
                             for body in final), 284)
        self.assertEqual(sum(body['type'] == 'userFillsByTime'
                             for body in final), 2)
        self.assertEqual((budget.LIMIT, budget.BACKGROUND_LIMIT), (1200, 800))

    def test_denied_opening_retention_phase_starts_no_http(self):
        start, end = T - evidence.OVERLAP_MS, T + evidence.DAY_MS // 2
        bodies = retained_history_plan(start, end)
        self.assertEqual(sum(budget.request_weight('/info', body)
                             for body in bodies), 480)
        self.assertTrue(all(body['endTime'] - body['startTime'] <= evidence.DAY_MS
                            for body in bodies))
        owner = Mock()
        owner.reserve_observation.side_effect = budget.BudgetError(
            'TESTNET_REQUEST_BUDGET_EXHAUSTED')
        reader = evidence.PublicReader(budget=owner, priority='background')
        old = closed_evidence()
        cursor = T + 3 * evidence.DAY_MS
        with patch('http.client.HTTPSConnection') as connection:
            with self.assertRaises(budget.BudgetError):
                evidence._collect_retained(old, reader, cursor_ms=cursor,
                    anchor=old['snapshot']['fills'][0], clock=lambda:cursor + 60000)
        plan = owner.reserve_observation.call_args.args[0]
        self.assertEqual(sum(budget.request_weight('/info', body)
                             for body in plan), 120)
        self.assertEqual(owner.reserve_observation.call_args.kwargs['priority'],
                         'background')
        self.assertEqual(sum(body['type'] == 'userFillsByTime' for body in plan), 1)
        self.assertEqual(reader.calls, 0)
        owner.acquire.assert_not_called()
        connection.assert_not_called()
        self.assertEqual(old, closed_evidence())


class HistoryGapRetrySchedulingTests(unittest.TestCase):
    """Failure clocks suppress only recovery reads, never grant entry authority."""
    def setUp(self):
        from . import long_stream_runtime as stream
        self.stream = stream
        self.clock = Mock(return_value=T)
        self.controller = SimpleNamespace(venue=SimpleNamespace(now=self.clock))
        self.state = dict(bucket='a' * 64, revision=7,
                          evidence=dict(snapshot=dict(at_ms=T - 3 * evidence.DAY_MS)))
        self.original = deepcopy(self.state)
        self.progress = dict(status='HISTORY_GAP_RECOVERY_PROGRESS',
                             cursor_ms=T - 2 * evidence.DAY_MS,
                             remaining_ms=2 * evidence.DAY_MS,
                             order_requests_sent=0)

    def test_changed_observation_waits_before_retry_and_clears_hint_on_success(self):
        with patch.object(self.stream.gap, 'step', side_effect=[
                evidence.SyncError('OBSERVATION_CHANGED_RETRY'), self.progress]) as step:
            with self.assertRaisesRegex(evidence.SyncError, 'CHANGED_RETRY'):
                self.stream._recover_history(self.controller, self.state)
            self.clock.return_value = T + 1999
            waiting = self.stream._recover_history(self.controller, self.state)
            self.assertEqual(waiting['status'], 'HISTORY_GAP_RECOVERY_WAIT')
            self.assertEqual(waiting['retry_after_ms'], 1)
            self.assertEqual(waiting['cursor_ms'], self.original['evidence']['snapshot']['at_ms'])
            self.assertEqual(waiting['order_requests_sent'], 0)
            self.assertEqual(waiting['failure_code'], 'OBSERVATION_CHANGED_RETRY')
            self.assertEqual(step.call_count, 1)
            self.clock.return_value = T + 2000
            self.assertEqual(self.stream._recover_history(self.controller, self.state),
                             self.progress)
            self.assertEqual(step.call_count, 2)
        self.assertEqual(vars(self.controller)['_history_gap_retries'], {})
        self.assertEqual(self.state, self.original)

    def test_repeated_nonquota_failure_is_graded_and_bounded(self):
        with patch.object(self.stream.gap, 'step',
                          side_effect=evidence.SyncError('OBSERVATION_CHANGED_RETRY')) as step:
            expected_delays = (2000, 4000, 8000, 16000, 30000, 30000, 30000)
            now = T
            for delay in expected_delays:
                self.clock.return_value = now
                with self.assertRaises(evidence.SyncError):
                    self.stream._recover_history(self.controller, self.state)
                self.clock.return_value = now + delay - 1
                result = self.stream._recover_history(self.controller, self.state)
                self.assertEqual(result['status'], 'HISTORY_GAP_RECOVERY_WAIT')
                self.assertEqual(result['retry_after_ms'], 1)
                now += delay
            self.assertEqual(step.call_count, len(expected_delays))
        self.assertEqual(self.state, self.original)

    def test_exact_quota_expiry_is_used_without_impossible_immediate_retry(self):
        quota = budget.BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED',
                                  used_weight=600, requested_weight=480,
                                  ceiling=800, retry_after_ms=69000)
        with patch.object(self.stream.gap, 'step', side_effect=[quota, self.progress]) as step:
            with self.assertRaises(budget.BudgetError):
                self.stream._recover_history(self.controller, self.state)
            self.clock.return_value = T + 68999
            result = self.stream._recover_history(self.controller, self.state)
            self.assertEqual(result['retry_after_ms'], 1)
            self.assertEqual(step.call_count, 1)
            self.clock.return_value = T + 69000
            self.assertEqual(self.stream._recover_history(self.controller, self.state),
                             self.progress)
        self.assertEqual(self.state, self.original)

    def test_new_durable_revision_and_restart_do_not_reuse_old_scheduling_hint(self):
        with patch.object(self.stream.gap, 'step', side_effect=[
                evidence.SyncError('OBSERVATION_CHANGED_RETRY'),
                self.progress, self.progress]) as step:
            with self.assertRaises(evidence.SyncError):
                self.stream._recover_history(self.controller, self.state)
            changed = deepcopy(self.state)
            changed['revision'] += 1
            self.assertEqual(self.stream._recover_history(self.controller, changed),
                             self.progress)
            restarted = SimpleNamespace(venue=SimpleNamespace(now=self.clock))
            self.assertEqual(self.stream._recover_history(restarted, self.state),
                             self.progress)
            self.assertEqual(step.call_count, 3)
        self.assertEqual(self.state, self.original)

    def test_wait_reports_its_committed_stage_cursor_and_never_claims_completion(self):
        self.state['history_gap_recovery'] = dict(cursor_ms=T - evidence.DAY_MS)
        with patch.object(self.stream.gap, 'step',
                          side_effect=evidence.SyncError('OBSERVATION_CHANGED_RETRY')) as step:
            with self.assertRaises(evidence.SyncError):
                self.stream._recover_history(self.controller, self.state)
            waiting = self.stream._recover_history(self.controller, self.state)
            self.assertEqual(waiting['cursor_ms'], T - evidence.DAY_MS)
            report = self.stream._history_recovery_report([waiting])
            self.assertEqual(report['status'], 'HISTORY_GAP_RECOVERY_WAIT')
            self.assertEqual(report['recovery_cursor_ms'], T - evidence.DAY_MS)
            self.assertEqual(report['retry_after_ms'], 2000)
            self.assertEqual(report['failure_code'], 'OBSERVATION_CHANGED_RETRY')
            self.assertEqual(step.call_count, 1)
        report = self.stream._history_recovery_report([self.progress, waiting])
        self.assertEqual(report['status'], 'HISTORY_GAP_RECOVERY_PROGRESS')
        self.assertEqual(report['retry_after_ms'], 2000)


@unittest.skipUnless(CI_URL, 'Requires disposable PostgreSQL CI service')
class HistoryGapBudgetPostgresTests(unittest.TestCase):
    def setUp(self):
        self.journal = pg.PostgresJournal.for_ci(CI_URL)
        with self.journal._transaction() as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS {pg.SCHEMA} CASCADE')
        self.journal.bootstrap()
        self.budget = budget.Budget(self.journal)
        self.budget.initialize()
        self.http = patch('http.client.HTTPSConnection',
                          side_effect=AssertionError('REAL_HTTP_FORBIDDEN'))
        self.connection = self.http.start()
        self.addCleanup(self.http.stop)

    def seed(self, weight):
        with self.journal._transaction() as conn:
            conn.execute(f'''INSERT INTO {budget.TICKETS}(token,weight)
                             VALUES(%s,%s)''', ('e' * 32, weight))

    def total(self):
        with self.journal._transaction() as conn:
            return conn.execute(f'SELECT COALESCE(sum(weight),0) '
                                f'FROM {budget.TICKETS}').fetchone()[0]

    def test_retained_history_reserves_all_reads_and_keeps_400_for_live_protection(self):
        self.seed(320)
        bodies = retained_history_plan(T - evidence.OVERLAP_MS,
                                       T + evidence.DAY_MS // 2)
        batch = self.budget.reserve_observation(bodies, priority='background')
        self.addCleanup(batch.close)
        self.assertEqual(self.total(), 800)
        # A complete protection request can still receive all reserved capacity
        # while the recovery HTTP has not started at all.
        other = budget.Budget(pg.PostgresJournal.for_ci(CI_URL))
        for _ in range(20):
            other.acquire('/info', {'type': 'meta'}, priority='protection').check()
        self.assertEqual(self.total(), 1200)
        with self.assertRaisesRegex(budget.BudgetError, 'EXHAUSTED'):
            other.acquire('/info', {'type': 'clearinghouseState'},
                          priority='background')
        for body in bodies:
            permit = batch.acquire('/info', body, priority='background')
            permit.check()
            self.assertTrue(permit.finish([]))
        batch.close()
        # Empty responses are charged 20 each; they are not free reads.
        self.assertEqual(self.total(), 800)
        self.connection.assert_not_called()

    def test_denied_chunk_starts_no_http_and_cannot_fund_only_one_pass(self):
        self.seed(321)
        reader = evidence.PublicReader(budget=self.budget, priority='background')
        bodies = retained_history_plan(T, T + evidence.DAY_MS // 2)
        with self.assertRaises(budget.BudgetError) as caught:
            with reader.observation_batch(bodies):
                evidence.history(reader, A, bodies[1]['startTime'],
                                 bodies[1]['endTime'])
        self.assertEqual(caught.exception.used_weight, 321)
        self.assertEqual(caught.exception.requested_weight, 480)
        self.assertEqual(caught.exception.ceiling, 800)
        self.assertEqual(self.total(), 321)
        self.assertEqual(reader.calls, 0)
        self.connection.assert_not_called()
        self.assertIsNone(reader._observation_budget)

    def test_truncated_parent_cannot_send_unfunded_children_or_return_partial_history(self):
        self.seed(320)
        bodies = retained_history_plan(T, T + evidence.DAY_MS // 2)
        calls = []
        class Connection:
            status = 200
            def __init__(self, *args, **kwargs):
                self.body = None
            def request(self, method, path, raw, headers):
                self.body = json.loads(raw)
                calls.append(deepcopy(self.body))
            def getresponse(self):
                return self
            def read(self, *args):
                return json.dumps([dict(time=T + i) for i in range(2000)]).encode()
            def close(self):
                pass
        reader = evidence.PublicReader(budget=self.budget, priority='background')
        with patch('http.client.HTTPSConnection', Connection):
            with self.assertRaises(budget.BudgetError) as caught:
                with reader.observation_batch(bodies):
                    evidence.history(reader, A, bodies[1]['startTime'],
                                     bodies[1]['endTime'])
        self.assertEqual(str(caught.exception), 'TESTNET_REQUEST_BUDGET_EXHAUSTED')
        self.assertEqual(caught.exception.requested_weight, 240)
        self.assertEqual(caught.exception.ceiling, 800)
        self.assertEqual(calls, [bodies[1]])
        self.assertEqual(reader.calls, 1)
        self.assertIsNone(reader._observation_budget)
        # The 2000-row parent remains fully charged. The untouched second
        # parent and anchor credits can be released; neither child reached HTTP.
        self.assertEqual(self.total(), 440)


if __name__ == '__main__':
    unittest.main()
