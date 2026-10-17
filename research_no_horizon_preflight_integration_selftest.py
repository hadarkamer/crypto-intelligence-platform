"""Strict experiment admission and explicit diagnostic replay on genuine captures.

Only disposable local SQLite and fixture prices are used. No production DB,
network, environment credential, captured historical corpus or new trial scope.
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
import research_no_horizon_preflight as preflight
import research_no_horizon_replay as replay
import research_no_horizon_source as source
import research_watch_scan_formula as formulas
from research_no_horizon_source_selftest import fixture


def scope(candidate='FUTURES_CVD_TOTAL_65', direction='SHORT', threshold=1.5):
    return {'candidate_key':candidate, 'base_direction':direction, 'threshold_pct':threshold}


def by_candidate(receipt):
    return {row['candidate_key']:row for row in receipt['scopes']}


class SourceAdmissionIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original = fixture()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.database = self.root/'experiment.sqlite'
        self.data = deepcopy(self.original)
        self.store = experiment.LocalExperimentStore(self.database)

    def tearDown(self):
        self.store.close()
        self.directory.cleanup()

    def assert_no_admission_or_work(self):
        for table in ('experiment_sources', 'experiment_plans', 'experiment_scopes',
                      'experiment_scope_state', 'local_snapshots', 'local_jobs', 'local_receipts'):
            self.assertEqual(self.store.connection.execute('SELECT COUNT(*) FROM '+table).fetchone()[0], 0, table)

    def finish(self, plan):
        for _ in range(20):
            result = self.store.run_plan(plan, 'preflight-fixture', scope_budget=2,
                candle_budget=2, entry_budget=1, batch_size=1)
            self.assertLessEqual(result['candle_evaluations_this_run'], 2)
            if result['all_scopes_processed']:
                return result
        self.fail('fixture plan did not finish within its bounded calls')

    def test_later_unknown_scope_blocks_every_child_before_snapshot_or_prices(self):
        scopes = [scope(), scope('SPOT_CVD_TOTAL_65')]
        # Real capture: Futures SHORT matches, later sorted Spot is unavailable.
        # These guards catch accidental partial execution before all scopes pass.
        with patch.object(source, 'build_snapshot', side_effect=AssertionError('no snapshot before admission')), \
             patch.object(self.store.children, 'claim_job', side_effect=AssertionError('no child claim before admission')):
            with self.assertRaises(experiment.SourcePreflightBlocked) as caught:
                self.store.submit_plan(self.data, scopes, plan_key='must-not-exist')
        receipt = caught.exception.receipt
        self.assertFalse(receipt['ready_for_outcome_research'])
        self.assertEqual(receipt['receipt_sha256'], contracts.digest({k:v for k,v in receipt.items() if k != 'receipt_sha256'}))
        rows = by_candidate(receipt)
        self.assertEqual(set(rows), {s['candidate_key'] for s in scopes})
        self.assertEqual(rows['FUTURES_CVD_TOTAL_65']['counts']['MATCH'], 1)
        self.assertTrue(rows['FUTURES_CVD_TOTAL_65']['ready_for_outcome_research'])
        self.assertEqual(rows['SPOT_CVD_TOTAL_65']['counts']['UNKNOWN'], 1)
        self.assertFalse(rows['SPOT_CVD_TOTAL_65']['ready_for_outcome_research'])
        self.assertTrue(rows['SPOT_CVD_TOTAL_65']['decision_ledger'][0]['missing_feature_reasons'])
        self.assert_no_admission_or_work()

    def test_explicit_diagnostic_mode_preserves_known_labels_and_unknown_population(self):
        scopes = [scope(), scope('SPOT_CVD_TOTAL_65')]
        baseline = replay.replay_snapshot(source.build_snapshot(self.data,
            'FUTURES_CVD_TOTAL_65', 'SHORT', 'BTC', 1.5))
        plan = self.store.submit_plan(self.data, scopes, allow_incomplete_source=True)
        report = self.finish(plan)
        admission = report['plan_identity']['source_admission']
        self.assertFalse(admission['preflight_receipt']['ready_for_outcome_research'])
        self.assertTrue(report['all_scopes_processed'])
        self.assertTrue(report['computation_complete'])
        self.assertFalse(report['source_coverage_complete'])
        rows = by_candidate(report)
        known = rows['FUTURES_CVD_TOTAL_65']
        unknown = rows['SPOT_CVD_TOTAL_65']
        receipt = self.store.children.get_receipt(known['job_id'])
        for key in ('outcomes', 'gate', 'status_counts', 'computation_complete', 'candle_evaluations'):
            self.assertEqual(receipt[key], baseline[key], key)
        self.assertEqual(unknown['source_counts']['UNKNOWN'], 1)
        self.assertEqual(unknown['source_counts']['NO_MATCH'], 0)
        self.assertEqual(unknown['emitted_opportunities'], 0)
        self.assertTrue(unknown['source_blockers'])
        self.assertFalse(unknown['source_coverage_complete'])
        self.assertFalse(unknown['gate']['experimental_eligible'])
        for row in report['scopes']:
            self.assertFalse(row['runtime_authorized'])
            self.assertFalse(row['telegram_authorized'])
            self.assertFalse(row['trading_authorized'])

    def test_admission_mode_is_immutable_identity_and_idempotent(self):
        scopes = [scope()]
        strict = self.store.submit_plan(self.data, scopes, plan_key='strict')
        diagnostic = self.store.submit_plan(self.data, scopes, plan_key='diagnostic', allow_incomplete_source=True)
        self.assertNotEqual(strict, diagnostic)
        self.assertEqual(strict, self.store.submit_plan(self.data, scopes, plan_key='strict'))
        self.assertEqual(diagnostic, self.store.submit_plan(self.data, scopes, plan_key='diagnostic', allow_incomplete_source=True))
        policies = [self.store.report(plan)['plan_identity']['source_admission'] for plan in (strict, diagnostic)]
        self.assertNotEqual(policies[0]['policy'], policies[1]['policy'])
        self.assertEqual(policies[0]['preflight_receipt'], policies[1]['preflight_receipt'])
        self.assertTrue(policies[0]['preflight_receipt']['ready_for_outcome_research'])
        with self.assertRaisesRegex(ValueError, 'plan_key'):
            self.store.submit_plan(self.data, scopes, plan_key='strict', allow_incomplete_source=True)
        for plan in (strict, diagnostic):
            self.assertFalse(self.finish(plan)['scopes'][0]['gate']['experimental_eligible'])

    def test_known_false_conjunct_admits_despite_unavailable_other_features(self):
        scopes = [scope('STRICT_TRIPLE_TOTAL_65', 'LONG')]
        receipt = preflight.preflight_source_features(self.data, scopes)
        self.assertTrue(receipt['ready_for_outcome_research'])
        row = receipt['scopes'][0]
        self.assertEqual(row['counts'], {'MATCH':0, 'NO_MATCH':1, 'UNKNOWN':0, 'UNKNOWN_SOURCE':0})
        self.assertTrue(row['decision_ledger'][0]['missing_features'])
        self.assertTrue(row['decision_ledger'][0]['missing_feature_reasons'])
        report = self.finish(self.store.submit_plan(self.data, scopes))
        self.assertTrue(report['source_coverage_complete'])
        self.assertTrue(report['computation_complete'])
        self.assertEqual(report['scopes'][0]['emitted_opportunities'], 0)
        self.assertEqual(report['scopes'][0]['gate']['selected_parents'], 0)

    def test_mutated_capture_is_unknown_source_and_unsupported_scope_never_admitted(self):
        self.data['source_rows'][0]['scores']['coins']['BTC']['models']['futures_flow']['score'] = 0
        with self.assertRaises(experiment.SourcePreflightBlocked) as caught:
            self.store.submit_plan(self.data, [scope()])
        counts = caught.exception.receipt['scopes'][0]['counts']
        self.assertEqual(counts['UNKNOWN_SOURCE'], 1)
        self.assertEqual(counts['NO_MATCH'], 0)
        self.assert_no_admission_or_work()
        with self.assertRaises(ValueError):
            self.store.submit_plan(self.original, [scope('made-up-formula')])
        self.assert_no_admission_or_work()

    def test_prepare_then_private_submit_reuses_one_heavy_preflight(self):
        # Spy calls the genuine implementation; it does not replace validation.
        with patch.object(preflight, 'preflight_source_features', wraps=preflight.preflight_source_features) as check:
            prepared = experiment.prepare_submission(self.data, [scope()])
            self.assert_no_admission_or_work()
            plan = self.store._submit_prepared(prepared, plan_key='prepared-once')
        self.assertEqual(check.call_count, 1)
        self.assertTrue(self.store.report(plan)['plan_identity']['source_admission']['preflight_receipt']['ready_for_outcome_research'])


class PreflightCommands(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original = fixture()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.database = self.root/'must-not-exist.sqlite'
        self.export = self.root/'source.json'
        self.scopes = self.root/'scopes.json'
        self.export.write_text(formulas.canonical(self.original), encoding='utf-8')
        self.scopes.write_text(json.dumps([scope()]), encoding='utf-8')

    def tearDown(self):
        self.directory.cleanup()

    def command(self, *args):
        return subprocess.run([sys.executable, cli.__file__, *args], capture_output=True, text=True, timeout=60)

    def test_database_free_preflight_reports_ready_and_blocked_and_never_overwrites(self):
        output = self.root/'preflight.json'
        command = ('preflight', str(self.export), '--scopes', str(self.scopes), '--output', str(output))
        result = self.command(*command)
        self.assertEqual(result.returncode, 0, result.stderr)
        original_bytes = output.read_bytes()
        self.assertTrue(json.loads(original_bytes)['ready_for_outcome_research'])
        self.assertFalse(self.database.exists())
        self.assertEqual(self.command(*command).returncode, 2)
        self.assertEqual(output.read_bytes(), original_bytes)
        self.scopes.write_text(json.dumps([scope(),scope('SPOT_CVD_TOTAL_65')]), encoding='utf-8')
        blocked_path = self.root/'blocked.json'
        blocked = self.command('preflight', str(self.export), '--scopes', str(self.scopes), '--output', str(blocked_path))
        self.assertEqual(blocked.returncode, 2, blocked.stderr)
        receipt = json.loads(blocked_path.read_text())
        self.assertFalse(receipt['ready_for_outcome_research'])
        self.assertEqual(len(receipt['scopes']), 2)
        self.assertFalse(self.database.exists())
        self.assertFalse(list(self.root.glob('*.sqlite')))

    def test_blocked_submit_prints_full_receipt_before_creating_database(self):
        self.scopes.write_text(json.dumps([scope(),scope('SPOT_CVD_TOTAL_65')]), encoding='utf-8')
        preflight_path = self.root/'blocked-submit-preflight.json'
        result = self.command('--database', str(self.database), 'submit', str(self.export),
            '--scopes', str(self.scopes), '--preflight-output', str(preflight_path))
        self.assertEqual(result.returncode, 2)
        receipt = json.loads(result.stdout)
        self.assertFalse(receipt['ready_for_outcome_research'])
        self.assertEqual(len(receipt['scopes']), 2)
        self.assertEqual(receipt, json.loads(preflight_path.read_text()))
        self.assertTrue(result.stderr)
        self.assertFalse(self.database.exists())

    def test_diagnostic_submit_requires_explicit_flag_and_keeps_receipt(self):
        self.scopes.write_text(json.dumps([scope('SPOT_CVD_TOTAL_65')]), encoding='utf-8')
        receipt_path = self.root/'diagnostic-preflight.json'
        result = self.command('--database', str(self.database), 'submit', str(self.export),
            '--scopes', str(self.scopes), '--allow-incomplete-source', '--preflight-output', str(receipt_path))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(json.loads(receipt_path.read_text())['ready_for_outcome_research'])
        plan_id = json.loads(result.stdout)['plan_id']
        with experiment.LocalExperimentStore(self.database) as store:
            report = store.report(plan_id)
            self.assertFalse(report['plan_identity']['source_admission']['preflight_receipt']['ready_for_outcome_research'])
            self.assertEqual(store.connection.execute('SELECT COUNT(*) FROM local_jobs').fetchone()[0], 0)


if __name__ == '__main__':
    unittest.main()
