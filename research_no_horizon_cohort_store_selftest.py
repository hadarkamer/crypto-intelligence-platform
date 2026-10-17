"""Real SQLite global-cohort resume, identity, budget and fencing regressions.

The common-anchor captures and all prices are synthetic fixtures. These checks
do not read a market database or change the existing child-store schema.
"""
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch

import research_no_horizon_cohort_outcomes as preparation
import research_no_horizon_cohort_store as coordinator
import research_no_horizon_contract as contracts
import research_no_horizon_first_touch as first_touch
import research_no_horizon_gate as gate
import research_no_horizon_store as child
from research_no_horizon_cohort_coverage_selftest import cross_part_rows
from research_no_horizon_cohort_outcomes_selftest import outcome_fixture, _seal_exports, _entry_time
from research_no_horizon_parent_coverage_selftest import scope


class GlobalCohortStoreTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original = outcome_fixture()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / 'cohort.sqlite'
        self.store = coordinator.LocalCohortStore(self.path)
        self.declaration, self.anchor, self.exports = deepcopy(self.original)

    def tearDown(self):
        self.store.close()
        self.directory.cleanup()

    def submit(self, *, fixture=None, **kwargs):
        declaration, anchor, exports = fixture or (self.declaration, self.anchor, self.exports)
        return self.store.submit_cohort(declaration, anchor, lambda ordinal: exports[ordinal], **kwargs)

    def finish(self, plan, **kwargs):
        budgets = {'scope_budget':64, 'candle_budget':100000, 'entry_budget':128, 'batch_size':128}
        budgets.update(kwargs)
        for _ in range(100):
            result = self.store.run_cohort(plan, 'worker', **budgets)
            if result['all_scopes_processed']:
                return result
        self.fail('bounded synthetic global cohort did not converge')

    def count(self, table):
        return self.store.connection.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0]

    def link_pending_scope(self, plan, ordinal=0):
        identity, payload, _ = self.store._load(plan)
        item = payload['scope_plans'][ordinal]
        job_id = self.store.children.submit_snapshot(self.store._snapshot(payload, item))
        self.store._link(plan, ordinal, job_id)
        return identity, payload, item, job_id

    def test_complete_plan_and_global_representatives_precede_any_child_job(self):
        plan = self.submit()
        self.assertEqual(self.count('cohort_plans'), 1)
        self.assertEqual(self.count('local_jobs'), 0)
        report = self.store.report(plan)
        self.assertEqual(report['declared_scopes'], 1)
        self.assertFalse(report['all_scopes_processed'])
        self.assertEqual(report['outcome_trials_executed'], 0)
        row = report['scopes'][0]
        self.assertEqual(row['status'], 'NOT_SUBMITTED')
        self.assertIsNone(row['gate'])
        self.assertIsNone(row['computation_complete'])
        self.assertEqual(row['candle_evaluations'], 0)
        self.assertEqual(len(row['representatives']), 5)
        self.assertEqual(report['coverage_receipt']['accepted_source_rows'], 6)
        self.assertFalse(report['parts_are_trials'])
        self.assertFalse(report['scope_pooling'])
        self.assertFalse(report['validated_discovery'])
        for key in ('runtime_authorized', 'telegram_authorized', 'trading_authorized'):
            self.assertFalse(report[key])

    def test_cross_part_duplicate_runs_once_and_earliest_loser_is_not_replaced(self):
        report = self.finish(self.submit())
        self.assertEqual(self.count('local_jobs'), 1)
        row = report['scopes'][0]
        self.assertEqual(row['candle_evaluations'], 5)
        self.assertEqual(row['status_counts']['SUCCESS'], 4)
        self.assertEqual(row['status_counts']['FAILURE'], 1)
        self.assertEqual(row['gate']['selected_parents'], 5)
        self.assertEqual(row['gate']['resolved_parents'], 5)
        self.assertEqual(row['gate']['successes'], 4)
        self.assertEqual(row['gate']['failures'], 1)
        outcomes = {item['entry_id']:item['outcome']['status'] for item in row['outcomes']}
        self.assertEqual(outcomes['watch:3:BTC:SHORT'], 'FAILURE')
        self.assertNotIn('watch:4:BTC:SHORT', outcomes)
        self.assertFalse(row['gate']['experimental_eligible'])
        self.assertEqual(report['outcome_trials_executed'], 1)
        self.assertTrue(report['computation_complete'])

    def test_reopened_tiny_budgets_equal_one_shot_outcomes_gates_and_counts(self):
        data = outcome_fixture(scopes=[scope(threshold=.25), scope(threshold=.2)])
        plan = self.submit(fixture=data)
        other_path = Path(self.directory.name) / 'one-shot.sqlite'
        with coordinator.LocalCohortStore(other_path) as other:
            declaration, anchor, exports = data
            other_plan = other.submit_cohort(declaration, anchor, lambda ordinal: exports[ordinal])
            self.assertEqual(other_plan, plan)
            other.run_cohort(other_plan, 'one-shot', scope_budget=64,
                candle_budget=100000, entry_budget=128, batch_size=128)
            expected = other.report(other_plan)
        attempts, consumed = [], 0
        for _ in range(20):
            result = self.store.run_cohort(plan, 'tiny', scope_budget=1,
                candle_budget=1, entry_budget=1, batch_size=1)
            self.assertLessEqual(result['candle_evaluations_this_run'], 1)
            consumed += result['candle_evaluations_this_run']
            attempts.extend(result['attempted_ordinals'])
            self.store.close()
            self.store = coordinator.LocalCohortStore(self.path)
            if result['all_scopes_processed']:
                break
        else:
            self.fail('tiny-budget cohort did not finish')
        self.assertEqual(attempts[:4], [0, 1, 0, 1])
        actual = self.store.report(plan)
        self.assertTrue(actual['computation_complete'])
        self.assertEqual(consumed, 10)
        self.assertEqual(self.count('local_jobs'), 2)
        self.assertEqual(actual['report_sha256'], expected['report_sha256'])
        for left, right in zip(actual['scopes'], expected['scopes']):
            for field in ('outcomes', 'gate', 'status_counts', 'candle_evaluations',
                          'snapshot_sha256', 'receipt_sha256', 'representatives'):
                self.assertEqual(left[field], right[field], field)

    def test_shared_candle_budget_bounds_all_attempted_global_scopes(self):
        data = outcome_fixture(scopes=[scope(threshold=.25), scope(threshold=.2), scope(threshold=.3)])
        report = self.store.run_cohort(self.submit(fixture=data), 'budgeted',
            scope_budget=3, candle_budget=3, entry_budget=1, batch_size=1)
        self.assertEqual(report['attempted_scopes'], 3)
        self.assertEqual(report['candle_evaluations_this_run'], 3)
        self.assertEqual(sum(row['candle_evaluations'] for row in report['scopes']), 3)
        self.assertEqual(self.count('local_jobs'), 3)
        self.assertFalse(report['all_scopes_processed'])
        self.assertTrue(all(row['gate'] is None for row in report['scopes']))
        self.assertEqual(report['report_sha256'], contracts.digest({key:value
            for key,value in report.items() if key!='report_sha256'}))

    def test_zero_match_scope_stays_visible_and_is_not_a_second_part_trial(self):
        data = outcome_fixture(scopes=[scope('SHORT'), scope('LONG')], mode='success')
        report = self.finish(self.submit(fixture=data))
        self.assertEqual(report['declared_scopes'], 2)
        self.assertEqual(self.count('local_jobs'), 2)
        empty = next(row for row in report['scopes'] if row['base_direction'] == 'LONG')
        self.assertEqual(empty['status'], 'COMPLETE')
        self.assertEqual(empty['outcomes'], [])
        self.assertEqual(empty['representatives'], [])
        self.assertEqual(empty['candle_evaluations'], 0)
        self.assertEqual(empty['gate']['selected_parents'], 0)
        self.assertFalse(empty['gate']['experimental_eligible'])
        self.assertTrue(report['source_coverage_complete'])
        self.assertTrue(report['computation_complete'])

    def test_missing_selected_entry_is_input_blocked_and_never_uses_later_match(self):
        declaration, _, exports = outcome_fixture(scopes=[scope('SHORT'), scope('LONG')])
        selected_time = _entry_time(exports[0]['source_rows'][2]['intake']['usable_from_utc'])
        for export in exports:
            export['candles'] = [bar for bar in export['candles']
                if contracts.utc(bar['open_time_utc']) != selected_time]
        data = _seal_exports(declaration, exports)
        report = self.finish(self.submit(fixture=data))
        blocked = next(row for row in report['scopes'] if row['base_direction'] == 'SHORT')
        empty = next(row for row in report['scopes'] if row['base_direction'] == 'LONG')
        self.assertEqual(blocked['status'], 'INPUT_BLOCKED')
        self.assertIn('watch:3:BTC:SHORT', blocked['missing_entry_ids'])
        self.assertEqual(len(blocked['representatives']), 5)
        self.assertIsNone(blocked['job_id'])
        self.assertIsNone(blocked['gate'])
        self.assertEqual(blocked['candle_evaluations'], 0)
        self.assertEqual(empty['status'], 'COMPLETE')
        self.assertEqual(self.count('local_jobs'), 1)
        self.assertFalse(report['computation_complete'])
        self.assertTrue(report['all_scopes_processed'])

    def test_open_ambiguous_and_price_gap_never_become_resolved_successes(self):
        for mode in ('open', 'ambiguous', 'gap'):
            with self.subTest(mode=mode):
                declaration, anchor, exports = outcome_fixture(mode='open' if mode=='gap' else mode)
                if mode == 'gap':
                    entry = contracts.utc(exports[0]['source_rows'][0]['intake']['usable_from_utc'])
                    missing = entry.replace(second=0, microsecond=0) + timedelta(minutes=2)
                    for export in exports:
                        export['candles'] = [bar for bar in export['candles']
                            if contracts.utc(bar['open_time_utc']) != missing]
                    declaration, anchor, exports = _seal_exports(declaration, exports)
                result = self.finish(self.submit(fixture=(declaration, anchor, exports)))
                row = result['scopes'][0]
                self.assertFalse(row['gate']['experimental_eligible'])
                self.assertEqual(row['gate']['resolved_parents'], 0)
                self.assertEqual(row['gate']['successes'], 0)
                if mode == 'gap':
                    self.assertEqual(row['status'], 'BLOCKED')
                    self.assertEqual(row['status_counts']['DATA_MISSING'], 1)
                    self.assertFalse(result['computation_complete'])
                else:
                    self.assertEqual(row['status_counts'][mode.upper()], 5)
                    self.assertTrue(result['computation_complete'])

    def test_successful_global_research_gate_does_not_authorize_runtime(self):
        result = self.finish(self.submit(fixture=outcome_fixture(mode='success')))
        row = result['scopes'][0]
        self.assertTrue(row['gate']['experimental_eligible'])
        self.assertEqual(row['gate']['resolved_parents'], 5)
        self.assertEqual(row['gate']['successes'], 5)
        for value in (result, row, row['gate']):
            self.assertFalse(value['runtime_authorized'])
            self.assertFalse(value['telegram_authorized'])
            self.assertFalse(value['trading_authorized'])
        self.assertFalse(result['is_prospective_formula_evidence'])

    def test_bad_last_part_or_blocked_coverage_leaves_no_plan_or_child(self):
        malformed = deepcopy(self.exports[1])
        malformed['source_rows'] = []
        loader = Mock(side_effect=lambda ordinal: self.exports[0] if ordinal == 0 else malformed)
        with self.assertRaises(ValueError):
            self.store.submit_cohort(self.declaration, self.anchor, loader)
        self.assertEqual([call.args for call in loader.call_args_list], [(0,), (1,)])
        rows = cross_part_rows()
        rows[1][1]['stored_source'] = None
        with self.assertRaises(preparation.CohortInputBlocked) as blocked:
            self.submit(fixture=outcome_fixture(rows))
        self.assertEqual(blocked.exception.receipt['scopes'][0]['coverage_status'], 'BLOCKED')
        for table in ('cohort_plans', 'cohort_payloads', 'cohort_scopes', 'local_jobs', 'local_snapshots'):
            self.assertEqual(self.count(table), 0, table)

    def test_idempotency_and_key_conflicts_bind_input_policy_and_cutoff(self):
        plan = self.submit(cohort_key='frozen-global')
        self.assertEqual(self.submit(cohort_key='frozen-global'), plan)
        self.assertEqual(self.submit(), plan)
        variants = [outcome_fixture(mode='success'), outcome_fixture(gate_policy=gate.make_policy(
            policy_version='selftest-global-p75-v1', hit_rate_min_pct=75))]
        declaration, _, exports = deepcopy(self.original)
        declaration['cutoff_utc'] = (contracts.utc(declaration['cutoff_utc']) + timedelta(minutes=1)).isoformat()
        for export in exports:
            export['cutoff_utc'] = declaration['cutoff_utc']
        variants.append(_seal_exports(declaration, exports))
        for index, data in enumerate(variants):
            with self.subTest(revision=index):
                with self.assertRaisesRegex(ValueError, 'cohort_key'):
                    self.submit(fixture=data, cohort_key='frozen-global')
                revision = self.submit(fixture=data, cohort_key='revision-' + str(index))
                self.assertNotEqual(revision, plan)
        self.assertEqual(self.count('cohort_plans'), 4)
        self.assertEqual(self.count('local_jobs'), 0)

    def test_crash_after_child_creation_before_link_reuses_exact_global_child(self):
        plan = self.submit()
        with patch.object(self.store, '_link', side_effect=RuntimeError('crash after child submit')):
            with self.assertRaisesRegex(RuntimeError, 'crash'):
                self.store.run_cohort(plan, 'crashed')
        self.assertEqual(self.count('local_jobs'), 1)
        self.assertEqual(self.store.report(plan)['scopes'][0]['status'], 'NOT_SUBMITTED')
        self.store.close()
        self.store = coordinator.LocalCohortStore(self.path)
        result = self.finish(plan)
        self.assertEqual(self.count('local_jobs'), 1)
        self.assertEqual(result['scopes'][0]['candle_evaluations'], 5)
        self.assertEqual(result['scopes'][0]['gate']['selected_parents'], 5)

    def test_implementation_fence_blocks_resume_but_preserves_historical_report(self):
        plan = self.submit()
        self.finish(plan)
        original = self.store.report(plan)
        modified = deepcopy(preparation.implementation())
        modified['implementation_sha256'] = '0' * 64
        with patch.object(preparation, 'implementation', return_value=modified):
            with self.assertRaisesRegex(ValueError, 'implementation/policy mismatch'):
                self.store.run_cohort(plan, 'different-code')
            self.assertEqual(self.store.report(plan)['report_sha256'], original['report_sha256'])

    def test_previously_processed_orphan_cannot_import_unaccounted_outcomes(self):
        plan = self.submit()
        with patch.object(self.store, '_link', side_effect=RuntimeError('crash before link')):
            with self.assertRaises(RuntimeError):
                self.store.run_cohort(plan, 'crashed')
        job_id = self.store.connection.execute('SELECT job_id FROM local_jobs').fetchone()[0]
        self.store.children.run_once('outside-coordinator', job_id=job_id)
        self.assertEqual(self.count('cohort_work_commits'), 0)
        with self.assertRaisesRegex(ValueError, 'unlinked global child already claimed or processed'):
            self.store.run_cohort(plan, 'recovering')
        self.assertIsNone(self.store.report(plan)['scopes'][0]['job_id'])
        self.assertEqual(self.store.report(plan)['outcome_trials_executed'], 0)

    def test_future_cohort_schema_rejected_before_unchanged_child_bootstrap(self):
        path = Path(self.directory.name) / 'future.sqlite'
        with sqlite3.connect(path) as connection:
            connection.execute('CREATE TABLE cohort_store_version(singleton INTEGER, version TEXT)')
            connection.execute("INSERT INTO cohort_store_version VALUES(1, 'future')")
        before = path.read_bytes()
        with self.assertRaisesRegex(ValueError, 'unsupported global cohort store version'):
            coordinator.LocalCohortStore(path)
        self.assertEqual(path.read_bytes(), before)
        with sqlite3.connect(path) as connection:
            self.assertIsNone(connection.execute("SELECT 1 FROM sqlite_master WHERE name='local_jobs'").fetchone())
        with child.LocalResearchStore(':memory:') as plain:
            expected = plain.connection.execute("SELECT name,sql FROM sqlite_master WHERE name LIKE 'local_%' ORDER BY name").fetchall()
        actual = self.store.connection.execute("SELECT name,sql FROM sqlite_master WHERE name LIKE 'local_%' ORDER BY name").fetchall()
        self.assertEqual([tuple(row) for row in actual], [tuple(row) for row in expected])

    def test_mutated_prepared_submission_and_invalid_budgets_do_not_write_work(self):
        prepared = preparation.prepare_cohort_submission(self.declaration, self.anchor, lambda n:self.exports[n])
        prepared.payload['candles'][0]['open'] = 101
        with self.assertRaises(ValueError):
            self.store._submit_prepared(prepared)
        self.assertEqual(self.count('cohort_plans'), 0)
        plan = self.submit()
        for name in ('scope_budget', 'candle_budget', 'entry_budget', 'batch_size'):
            with self.subTest(budget=name):
                with self.assertRaises(ValueError):
                    self.store.run_cohort(plan, 'worker', **{name:0})
        self.assertEqual(self.count('local_jobs'), 0)
        self.assertEqual(self.store.connection.execute('SELECT next_ordinal FROM cohort_schedule').fetchone()[0], 0)

    def test_scope_manifest_and_immutable_records_reject_corruption(self):
        plan = self.submit()
        for table in ('cohort_plans', 'cohort_payloads', 'cohort_scopes', 'cohort_scope_state'):
            with self.assertRaises(sqlite3.IntegrityError):
                self.store.connection.execute('DELETE FROM ' + table)
        self.store.connection.execute('DROP TRIGGER cohort_scopes_update_immutable')
        self.store.connection.execute("UPDATE cohort_scopes SET scope_plan_json='{}'")
        with self.assertRaisesRegex(ValueError, 'scope manifest integrity'):
            self.store.report(plan)
        with self.assertRaisesRegex(ValueError, 'scope manifest integrity'):
            self.store.run_cohort(plan, 'worker')
        self.assertEqual(self.count('local_jobs'), 0)

    def test_foreign_child_link_is_rejected_before_more_work(self):
        plan = self.submit()
        original = self.finish(plan)
        other_plan = self.submit(fixture=outcome_fixture(scopes=[scope(threshold=.4)]))
        other = self.finish(other_plan)
        self.assertNotEqual(original['scopes'][0]['job_id'], other['scopes'][0]['job_id'])
        self.store.connection.execute('DROP TRIGGER cohort_immutable_link')
        self.store.connection.execute('UPDATE cohort_scope_state SET job_id=? WHERE plan_id=?',
            (other['scopes'][0]['job_id'], plan))
        with self.assertRaisesRegex(ValueError, 'linked child identity'):
            self.store.report(plan)
        with self.assertRaisesRegex(ValueError, 'linked child identity'):
            self.store.run_cohort(plan, 'worker')

    def test_rehashed_foreign_receipt_is_rejected_by_global_binding(self):
        plan = self.submit()
        result = self.finish(plan)
        job_id = result['scopes'][0]['job_id']
        receipt = self.store.children.get_receipt(job_id)
        receipt['cutoff_utc'] = (contracts.utc(receipt['cutoff_utc']) + timedelta(minutes=1)).isoformat()
        receipt['receipt_sha256'] = contracts.digest({key:value for key,value in receipt.items() if key!='receipt_sha256'})
        self.store.connection.execute('DROP TRIGGER local_receipts_update_immutable')
        self.store.connection.execute('UPDATE local_receipts SET payload_json=?,receipt_sha256=? WHERE job_id=?',
            (contracts.canonical(receipt), receipt['receipt_sha256'], job_id))
        with self.assertRaisesRegex(ValueError, 'receipt/representative binding'):
            self.store.report(plan)

    def test_inserting_price_into_frozen_gap_cannot_repair_materialized_evidence(self):
        declaration, _, exports = outcome_fixture(mode='open')
        entry_time = _entry_time(exports[0]['source_rows'][0]['intake']['usable_from_utc'])
        missing_time = entry_time + timedelta(minutes=1)
        missing_raw = next(deepcopy(bar) for bar in exports[0]['candles']
            if contracts.utc(bar['open_time_utc']) == missing_time)
        for export in exports:
            export['candles'] = [bar for bar in export['candles']
                if contracts.utc(bar['open_time_utc']) != missing_time]
        plan = self.submit(fixture=_seal_exports(declaration, exports))
        _, _, item, job_id = self.link_pending_scope(plan)
        snapshot_sha = item['snapshot_sha256']
        frozen_before = tuple(self.store.connection.execute(
            'SELECT payload_json,metadata_json,source_candles FROM local_snapshots WHERE snapshot_sha256=?',
            (snapshot_sha,)).fetchone())
        inserted_bar = first_touch._bar(missing_raw, cutoff=contracts.utc(declaration['cutoff_utc']))
        self.store.connection.execute('INSERT INTO local_candles VALUES(?,?,?)',
            (snapshot_sha, missing_time.isoformat(timespec='microseconds'), contracts.canonical(inserted_bar)))
        frozen_after = tuple(self.store.connection.execute(
            'SELECT payload_json,metadata_json,source_candles FROM local_snapshots WHERE snapshot_sha256=?',
            (snapshot_sha,)).fetchone())
        self.assertEqual(frozen_after, frozen_before)
        with self.assertRaisesRegex(ValueError, 'materialized price integrity'):
            self.store.report(plan)
        with self.assertRaisesRegex(ValueError, 'materialized price integrity'):
            self.store.run_cohort(plan, 'must-not-evaluate')
        job = self.store.children.get_job(job_id)
        self.assertEqual(job['fencing_token'], 0)
        self.assertEqual(job['candle_evaluations'], 0)
        self.assertEqual(self.count('cohort_work_commits'), 0)

    def test_child_verification_uses_one_snapshot_across_concurrent_progress_commit(self):
        plan = self.submit()
        identity, payload, item, job_id = self.link_pending_scope(plan)
        original = self.store.children.get_job
        commits = []
        with child.LocalResearchStore(self.path) as other:
            def commit_after_job_read(requested_job):
                old_job = original(requested_job)
                self.assertTrue(self.store.connection.in_transaction)
                self.assertEqual(old_job['candle_evaluations'], 0)
                self.assertFalse(commits)
                committed = other.run_once('concurrent-worker', job_id=requested_job,
                    candle_budget=1, entry_budget=1, batch_size=1)
                self.assertEqual(committed['candle_evaluations'], 1)
                commits.append(committed)
                return old_job
            with patch.object(self.store.children, 'get_job', side_effect=commit_after_job_read):
                verified = self.store._verify_child(identity, payload, item, job_id)
        self.assertEqual(len(commits), 1)
        self.assertEqual(verified['candle_evaluations'], 0)
        # The next read sees the committed checkpoint; the verification above
        # must not combine its old job counter with this newer progress row.
        fresh = self.store.report(plan)
        self.assertEqual(fresh['scopes'][0]['candle_evaluations'], 1)
        self.assertEqual(fresh['scopes'][0]['status'], 'PENDING')
        self.assertEqual(self.count('cohort_work_commits'), 1)

    def test_expired_owner_is_fenced_and_resume_never_double_counts(self):
        plan = self.submit()
        first = self.store.run_cohort(plan, 'initial', candle_budget=1)
        job_id = first['scopes'][0]['job_id']
        stale = self.store.children.claim_job('crashed', job_id=job_id)
        self.assertIsNotNone(stale)
        waiting = self.store.run_cohort(plan, 'waiting', candle_budget=1)
        self.assertEqual(waiting['candle_evaluations_this_run'], 0)
        self.assertIsNone(waiting['scopes'][0]['gate'])
        self.store.connection.execute('UPDATE local_jobs SET lease_until=0 WHERE job_id=?', (job_id,))
        with coordinator.LocalCohortStore(self.path) as other:
            resumed = other.run_cohort(plan, 'replacement', candle_budget=1)
            self.assertEqual(resumed['candle_evaluations_this_run'], 1)
            self.assertEqual(resumed['scopes'][0]['candle_evaluations'], 2)
        with self.assertRaises(child.LeaseLost):
            self.store.children.process_claim(stale)
        complete = self.finish(plan)
        self.assertEqual(complete['scopes'][0]['candle_evaluations'], 5)
        self.assertEqual(self.count('local_receipts'), 1)

    def test_lease_expiry_during_compute_commits_no_progress_or_work_accounting(self):
        plan = self.submit()
        original = first_touch.advance
        def expire(*args, **kwargs):
            result = original(*args, **kwargs)
            self.store.connection.execute('UPDATE local_jobs SET lease_until=0')
            return result
        with patch.object(first_touch, 'advance', side_effect=expire):
            with self.assertRaises(child.LeaseLost):
                self.store.run_cohort(plan, 'expires', candle_budget=1)
        row = self.store.report(plan)['scopes'][0]
        self.assertEqual(row['candle_evaluations'], 0)
        self.assertEqual(self.count('cohort_work_commits'), 0)
        self.assertEqual(self.count('local_receipts'), 0)
        self.assertEqual(self.store.children.get_job(row['job_id'])['next_ordinal'], 0)
        complete = self.finish(plan)
        self.assertEqual(complete['scopes'][0]['candle_evaluations'], 5)

    def test_concurrent_counter_return_accounts_only_current_fencing_token(self):
        plan = self.submit()
        original = self.store.children.get_job
        raced = []
        def racing_get(job_id):
            value = original(job_id)
            if value['status']=='PENDING' and value['candle_evaluations']==1 and not raced:
                raced.append(True)
                with child.LocalResearchStore(self.path) as other:
                    other.run_once('other-worker', job_id=job_id, candle_budget=1)
                return original(job_id)
            return value
        with patch.object(self.store.children, 'get_job', side_effect=racing_get):
            result = self.store.run_cohort(plan, 'current-worker', candle_budget=1)
        self.assertTrue(raced)
        self.assertEqual(result['candle_evaluations_this_run'], 1)
        self.assertEqual(result['scopes'][0]['candle_evaluations'], 2)
        rows = self.store.connection.execute('SELECT * FROM cohort_work_commits').fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(sum(row['ending_evaluations']-row['starting_evaluations'] for row in rows), 2)


if __name__ == '__main__':
    unittest.main()
