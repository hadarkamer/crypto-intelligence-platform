"""Opt-in parent coverage admission across the local experiment API and CLI.

Genuine captured-source fixtures, disposable SQLite and temporary JSON only.
These tests create no production connection, network request or historical
research trial. Admission feasibility does not evaluate outcomes or a gate.
"""
from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import research_no_horizon_contract as contracts
import research_no_horizon_experiment as experiment
import research_no_horizon_experiment_cli as cli
import research_no_horizon_parent_coverage as coverage
import research_no_horizon_preflight as feature_preflight
import research_no_horizon_replay as replay
import research_no_horizon_source as source
import research_watch_scan_formula as formulas
from research_no_horizon_parent_coverage_selftest import population, scope
from research_no_horizon_source_selftest import fixture


POLICY = 'REQUIRE_ALL_SCOPES_MATCHED_PARENT_COVERAGE_V1'
AUTHORITY = ('runtime_authorized', 'telegram_authorized', 'trading_authorized')
EMPTY_TABLES = ('experiment_sources', 'experiment_plans', 'experiment_scopes',
    'experiment_scope_state', 'experiment_schedule', 'experiment_work_commits',
    'local_snapshots', 'local_jobs', 'local_receipts')


def receipt_hash(receipt):
    return contracts.digest({key: value for key, value in receipt.items() if key != 'receipt_sha256'})


class ParentCoverageAdmission(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / 'experiment.sqlite'
        self.store = experiment.LocalExperimentStore(self.database)
        self.addCleanup(self.store.close)

    def assert_empty(self):
        for table in EMPTY_TABLES:
            self.assertEqual(self.store.connection.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0],
                             0, table)

    def test_possible_scope_does_not_admit_another_insufficient_scope(self):
        export = population(5)
        scopes = [scope('SHORT'), scope('LONG')]
        with patch.object(source, 'build_snapshot', side_effect=AssertionError('entry construction forbidden')), \
             patch.object(self.store.children, 'claim_job', side_effect=AssertionError('child work forbidden')):
            with self.assertRaises(experiment.ParentCoverageBlocked) as caught:
                self.store.submit_plan(export, scopes, plan_key='no-partial-admission',
                                       require_parent_coverage=True)
        receipt = caught.exception.receipt
        self.assertEqual(receipt['receipt_sha256'], receipt_hash(receipt))
        self.assertEqual(caught.exception.source_receipt, receipt['source_preflight'])
        self.assertTrue(caught.exception.source_receipt['ready_for_outcome_research'])
        rows = {row['base_direction']: row for row in receipt['scopes']}
        self.assertEqual(set(rows), {'SHORT', 'LONG'})
        self.assertEqual(rows['SHORT']['coverage_status'], 'POSSIBLE')
        self.assertEqual(rows['SHORT']['matched_parent_count'], 5)
        self.assertEqual(rows['LONG']['coverage_status'], 'INSUFFICIENT')
        self.assertEqual(rows['LONG']['matched_parent_count'], 0)
        self.assertTrue(all(receipt[key] is False for key in AUTHORITY))
        self.assertFalse(receipt['outcomes_evaluated'])
        self.assert_empty()

    def test_required_admission_binds_exact_receipts_and_validates_source_once(self):
        export, scopes = population(5), [scope()]
        expected = coverage.preflight_parent_coverage(export, scopes)
        with patch.object(feature_preflight, 'preflight_source_features',
                          wraps=feature_preflight.preflight_source_features) as check, \
             patch.object(source, '_validate_row', wraps=source._validate_row) as row_check, \
             patch.object(source, 'build_snapshot', side_effect=AssertionError('no entry during admission')):
            prepared = experiment.prepare_submission(export, scopes, require_parent_coverage=True)
            self.assert_empty()
            plan = self.store._submit_prepared(prepared, plan_key='five-causal-parents')
        self.assertEqual(check.call_count, 1)
        self.assertEqual(row_check.call_count, 5)
        self.assertEqual(plan, prepared.plan_id)
        self.assertEqual(plan, contracts.digest(prepared.identity))
        self.assertEqual(prepared.identity['source_export_sha256'],
                         contracts.digest(json.loads(formulas.canonical(export))))
        admission = prepared.identity['parent_coverage_admission']
        self.assertEqual(admission, {'policy': POLICY, 'preflight_receipt': expected})
        self.assertEqual(admission['preflight_receipt']['receipt_sha256'], receipt_hash(expected))
        self.assertEqual(prepared.receipt, expected['source_preflight'])
        report = self.store.report(plan)
        self.assertEqual(report['plan_identity'], prepared.identity)
        self.assertEqual(report['scopes'][0]['status'], 'NOT_SUBMITTED')
        self.assertIsNone(report['scopes'][0]['gate'])
        self.assertTrue(all(report[key] is False for key in AUTHORITY))
        self.assertEqual(self.store.connection.execute('SELECT COUNT(*) FROM local_jobs').fetchone()[0], 0)

    def test_diagnostic_source_override_cannot_bypass_required_parent_coverage(self):
        export, scopes = population(5, unavailable=True), [scope()]
        for diagnostic in (False, True):
            with self.subTest(diagnostic=diagnostic):
                with self.assertRaises(experiment.ParentCoverageBlocked) as caught:
                    self.store.submit_plan(export, scopes, allow_incomplete_source=diagnostic,
                                           require_parent_coverage=True)
                receipt = caught.exception.receipt
                self.assertFalse(caught.exception.source_receipt['ready_for_outcome_research'])
                self.assertEqual(receipt['source_preflight'], caught.exception.source_receipt)
                self.assertEqual(len(receipt['scopes']), 1)
                self.assertEqual(receipt['scopes'][0]['coverage_status'], 'BLOCKED')
                self.assertEqual(receipt['scopes'][0]['source_counts']['UNKNOWN'], 5)
                self.assertFalse(receipt['outcomes_evaluated'])
                self.assert_empty()

    def test_nonboolean_requirement_rejected_before_source_work_or_persistence(self):
        export, scopes = fixture(), [scope()]
        for value in (None, 0, 1, 'true', [], {}):
            with self.subTest(value=value), \
                 patch.object(feature_preflight, 'preflight_source_features',
                              side_effect=AssertionError('invalid policy must fail before source work')):
                with self.assertRaises(ValueError):
                    experiment.prepare_submission(export, scopes, require_parent_coverage=value)
                with self.assertRaises(ValueError):
                    self.store.submit_plan(export, scopes, require_parent_coverage=value)
                self.assert_empty()

    def test_required_policy_changes_immutable_identity_and_cannot_rebind_plan_key(self):
        export, scopes = population(5), [scope()]
        ordinary = self.store.submit_plan(export, scopes, plan_key='descriptive')
        required = self.store.submit_plan(export, scopes, plan_key='covered', require_parent_coverage=True)
        self.assertNotEqual(ordinary, required)
        self.assertEqual(required, self.store.submit_plan(export, scopes, plan_key='covered',
                                                        require_parent_coverage=True))
        plain_identity = self.store.report(ordinary)['plan_identity']
        guarded_identity = self.store.report(required)['plan_identity']
        self.assertNotIn('parent_coverage_admission', plain_identity)
        self.assertEqual(guarded_identity['parent_coverage_admission']['policy'], POLICY)
        with self.assertRaisesRegex(ValueError, 'plan_key'):
            self.store.submit_plan(export, scopes, plan_key='descriptive', require_parent_coverage=True)

    def test_changed_prepared_coverage_receipt_fails_identity_hash_before_insert(self):
        prepared = experiment.prepare_submission(population(5), [scope()], require_parent_coverage=True)
        changed = deepcopy(prepared)
        changed.identity['parent_coverage_admission']['preflight_receipt']['scopes'][0]['matched_parent_count'] = 6
        with self.assertRaisesRegex(ValueError, 'integrity'):
            self.store._submit_prepared(changed)
        self.assert_empty()

    def test_default_one_parent_descriptive_replay_keeps_existing_result(self):
        export, scopes = fixture(), [scope(threshold=1.5)]
        default = experiment.prepare_submission(export, scopes)
        explicit = experiment.prepare_submission(export, scopes, require_parent_coverage=False)
        self.assertEqual(default, explicit)
        self.assertNotIn('parent_coverage_admission', default.identity)
        expected = replay.replay_snapshot(source.build_snapshot(export,
            'FUTURES_CVD_TOTAL_65', 'SHORT', 'BTC', 1.5))
        plan = self.store._submit_prepared(default)
        for _ in range(12):
            report = self.store.run_plan(plan, 'descriptive-fixture', scope_budget=1,
                candle_budget=2, entry_budget=1, batch_size=1)
            if report['all_scopes_processed']:
                break
        self.assertTrue(report['all_scopes_processed'])
        actual = self.store.children.get_receipt(report['scopes'][0]['job_id'])
        for key in ('outcomes', 'gate', 'status_counts', 'computation_complete', 'candle_evaluations'):
            self.assertEqual(actual[key], expected[key], key)
        self.assertEqual(actual['gate']['selected_parents'], 1)
        self.assertFalse(actual['gate']['experimental_eligible'])


class ParentCoverageCommands(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.database = self.root / 'must-not-exist-until-admitted.sqlite'
        self.export = self.root / 'source.json'
        self.scopes = self.root / 'scopes.json'
        self.write_inputs(population(), [scope()])

    def write_inputs(self, export, scopes):
        self.export.write_text(formulas.canonical(export), encoding='utf-8')
        self.scopes.write_text(json.dumps(scopes), encoding='utf-8')

    def command(self, *args):
        return subprocess.run([sys.executable, cli.__file__, *args],
                              capture_output=True, text=True, timeout=60)

    def assert_no_database(self):
        self.assertFalse(self.database.exists())
        self.assertFalse(list(self.root.glob('*.sqlite*')))

    def test_database_free_coverage_reports_full_insufficient_receipt_and_is_exclusive(self):
        export, scopes = population(), [scope('SHORT'), scope('LONG')]
        self.write_inputs(export, scopes)
        output = self.root / 'coverage.json'
        args = ('coverage', str(self.export), '--scopes', str(self.scopes), '--output', str(output))
        result = self.command(*args)
        self.assertEqual(result.returncode, 2, result.stderr)
        original = output.read_bytes()
        receipt = json.loads(original)
        self.assertEqual(receipt, coverage.preflight_parent_coverage(export, scopes))
        self.assertEqual(len(receipt['scopes']), 2)
        self.assertEqual({row['coverage_status'] for row in receipt['scopes']}, {'INSUFFICIENT'})
        self.assert_no_database()
        retry = self.command(*args)
        self.assertEqual(retry.returncode, 2)
        self.assertEqual(output.read_bytes(), original)
        self.assert_no_database()

    def test_database_free_coverage_succeeds_only_for_every_possible_scope(self):
        export, scopes = population(5), [scope()]
        self.write_inputs(export, scopes)
        output = self.root / 'possible.json'
        result = self.command('coverage', str(self.export), '--scopes', str(self.scopes), '--output', str(output))
        self.assertEqual(result.returncode, 0, result.stderr)
        receipt = json.loads(output.read_text())
        self.assertEqual(receipt, coverage.preflight_parent_coverage(export, scopes))
        self.assertEqual(receipt['scopes'][0]['coverage_status'], 'POSSIBLE')
        self.assertFalse(receipt['outcomes_evaluated'])
        self.assert_no_database()

    def test_blocked_submit_retains_coverage_and_feature_receipts_without_database(self):
        for unavailable in (False, True):
            with self.subTest(diagnostic=unavailable):
                export = population(5 if unavailable else 1, unavailable=unavailable)
                self.write_inputs(export, [scope()])
                feature_path = self.root / f'feature-{unavailable}.json'
                coverage_path = self.root / f'coverage-{unavailable}.json'
                args = ['--database', str(self.database), 'submit', str(self.export),
                    '--scopes', str(self.scopes), '--require-parent-coverage',
                    '--parent-coverage-output', str(coverage_path), '--preflight-output', str(feature_path)]
                if unavailable:
                    args.append('--allow-incomplete-source')
                result = self.command(*args)
                self.assertEqual(result.returncode, 2, result.stderr)
                receipt = json.loads(coverage_path.read_text())
                features = json.loads(feature_path.read_text())
                self.assertEqual(receipt, json.loads(result.stdout))
                self.assertEqual(receipt, coverage.preflight_parent_coverage(export, [scope()]))
                self.assertEqual(features, receipt['source_preflight'])
                self.assertEqual(features['ready_for_outcome_research'], not unavailable)
                self.assertEqual(receipt['scopes'][0]['coverage_status'],
                                 'BLOCKED' if unavailable else 'INSUFFICIENT')
                self.assertTrue(result.stderr)
                self.assert_no_database()

    def test_coverage_output_requires_explicit_admission_flag_before_database(self):
        output = self.root / 'not-authorized.json'
        result = self.command('--database', str(self.database), 'submit', str(self.export),
            '--scopes', str(self.scopes), '--parent-coverage-output', str(output))
        self.assertEqual(result.returncode, 2)
        self.assertIn('--require-parent-coverage', result.stderr)
        self.assertFalse(output.exists())
        self.assert_no_database()

    def test_required_submit_saves_exact_admission_without_creating_child_jobs(self):
        export, scopes = population(5), [scope()]
        self.write_inputs(export, scopes)
        feature_path, coverage_path = self.root / 'feature.json', self.root / 'coverage.json'
        result = self.command('--database', str(self.database), 'submit', str(self.export),
            '--scopes', str(self.scopes), '--require-parent-coverage',
            '--parent-coverage-output', str(coverage_path), '--preflight-output', str(feature_path))
        self.assertEqual(result.returncode, 0, result.stderr)
        plan = json.loads(result.stdout)['plan_id']
        receipt = json.loads(coverage_path.read_text())
        with experiment.LocalExperimentStore(self.database) as store:
            identity = store.report(plan)['plan_identity']
            self.assertEqual(plan, contracts.digest(identity))
            self.assertEqual(identity['parent_coverage_admission'], {'policy': POLICY, 'preflight_receipt': receipt})
            self.assertEqual(identity['source_admission']['preflight_receipt'], json.loads(feature_path.read_text()))
            self.assertEqual(receipt['source_preflight'], json.loads(feature_path.read_text()))
            self.assertEqual(store.connection.execute('SELECT COUNT(*) FROM local_jobs').fetchone()[0], 0)

    def test_feature_and_parent_receipts_cannot_share_an_output_path(self):
        self.write_inputs(population(5), [scope()])
        output = self.root / 'conflicting-receipts.json'
        result = self.command('--database', str(self.database), 'submit', str(self.export),
            '--scopes', str(self.scopes), '--require-parent-coverage',
            '--preflight-output', str(output), '--parent-coverage-output', str(output))
        self.assertEqual(result.returncode, 2)
        self.assertIn('distinct', result.stderr)
        self.assertFalse(output.exists())
        self.assert_no_database()

    def test_existing_coverage_output_is_preserved_before_any_database_creation(self):
        self.write_inputs(population(5), [scope()])
        output = self.root / 'already-frozen.json'
        output.write_bytes(b'previous immutable audit\n')
        result = self.command('--database', str(self.database), 'submit', str(self.export),
            '--scopes', str(self.scopes), '--require-parent-coverage', '--parent-coverage-output', str(output))
        self.assertEqual(result.returncode, 2)
        self.assertEqual(output.read_bytes(), b'previous immutable audit\n')
        self.assert_no_database()


if __name__ == '__main__':
    unittest.main()
