"""Discovery command routing: offline plans, explicit writes and read-only ranks."""
from contextlib import redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import research_no_horizon_discovery as discovery
import research_no_horizon_discovery_cli as cli
import research_no_horizon_contract as contracts
from research_no_horizon_cohort_coverage_selftest import cohort_fixture


class DiscoveryCLITests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.declaration = cohort_fixture()[0]

    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.input = self.root / 'input.json'
        self.input.write_text(json.dumps(self.declaration), encoding='utf-8')
        self.output = self.root / 'output.json'
        env = patch.dict(os.environ, {}, clear=True)
        env.start()
        self.addCleanup(env.stop)

    def invoke(self, args):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            status = cli.main([*args, '--output', str(self.output)])
        return status, out.getvalue(), err.getvalue()

    def save_plan(self):
        plan = discovery.build_plan(self.declaration, base_directions=['SHORT'],
            thresholds_pct=[.25], candidate_keys=['FUTURES_CVD_TOTAL_65'])
        self.input.write_text(contracts.canonical(plan), encoding='utf-8')
        return plan

    def test_build_needs_no_database_or_source_and_keeps_full_catalog(self):
        with patch.object(cli, '_connect', side_effect=AssertionError('write connection')) as write, \
             patch.object(cli, '_connect_readonly', side_effect=AssertionError('read connection')) as read:
            status, _, err = self.invoke(['build', '--declaration', str(self.input),
                '--base-direction', 'SHORT', '--threshold-pct', '.25',
                '--candidate-key', 'FUTURES_CVD_TOTAL_65'])
        self.assertEqual((status, err), (0, ''))
        write.assert_not_called()
        read.assert_not_called()
        saved = json.loads(self.output.read_text())
        self.assertEqual(discovery.validate_plan(saved), saved)
        self.assertEqual(len(saved['calendar_plan']['windows'][0]['declaration']['scopes']), 1)
        self.assertEqual(len(saved['catalog_manifest']), 298)
        self.assertEqual(saved['summary']['supported_candidates'], 82)
        self.assertEqual(saved['summary']['omitted_supported_candidates'], 81)
        self.assertEqual(saved['summary']['unsupported_candidates'], 216)

    def test_all_supported_default_rejects_before_offline_output_or_database(self):
        with patch.object(cli, '_connect') as write, patch.object(cli, '_connect_readonly') as read:
            status, out, err = self.invoke(['build', '--declaration', str(self.input),
                '--base-direction', 'SHORT', '--threshold-pct', '.25'])
        self.assertEqual((status, out, err), (1, '', 'DISCOVERY_FAILED: ValueError\n'))
        write.assert_not_called()
        read.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_maxpain_candidate_builds_offline_with_current_feature_binding(self):
        key = 'captured-question-search-v3-experimental-binding:average_score_all_timeframes_GE65'
        with patch.object(cli, '_connect') as write, patch.object(cli, '_connect_readonly') as read:
            status, _, err = self.invoke(['build', '--declaration', str(self.input),
                '--base-direction', 'SHORT', '--threshold-pct', '.25', '--candidate-key', key])
        self.assertEqual((status, err), (0, ''))
        write.assert_not_called()
        read.assert_not_called()
        saved = json.loads(self.output.read_text())
        self.assertEqual(discovery.validate_plan(saved), saved)
        self.assertEqual(saved['candidate_keys'], [key])
        bindings = saved['calendar_plan']['windows'][0]['declaration']['version_bindings']
        self.assertEqual(bindings['feature_version'], 'watch-captured-total-and-maxpain-features-v2')

    def test_old_frozen_support_manifest_rejects_before_database_or_output(self):
        plan = self.save_plan()
        record = next(row for row in plan['catalog_manifest']
                      if row['candidate_key'].endswith(':average_score_all_timeframes_GE65'))
        record['supported'] = False
        record['unsupported_features'] = ['event.direction_mapping_valid',
                                          'max_pain.average_score_all_timeframes']
        plan['plan_sha256'] = contracts.digest({key: value for key, value in plan.items() if key != 'plan_sha256'})
        self.input.write_text(contracts.canonical(plan), encoding='utf-8')
        os.environ['RESEARCH_NO_HORIZON_DATABASE_URL'] = 'postgresql://research'
        with patch.object(cli, '_connect') as write, patch.object(cli, '_connect_readonly') as read:
            self.assertEqual(self.invoke(['register', '--plan', str(self.input)])[0], 1)
        write.assert_not_called()
        read.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_oversized_grid_and_nonfinite_threshold_reject_before_connect(self):
        with patch.object(cli, '_connect') as write, patch.object(cli, '_connect_readonly') as read:
            self.assertEqual(self.invoke(['build', '--declaration', str(self.input),
                '--base-direction', 'SHORT', '--base-direction', 'LONG',
                '--threshold-pct', '.25'])[0], 1)
            self.assertEqual(self.invoke(['build', '--declaration', str(self.input),
                '--base-direction', 'SHORT', '--threshold-pct', 'nan'])[0], 1)
        write.assert_not_called()
        read.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_invalid_plan_output_collision_and_window_fail_before_connection(self):
        with patch.object(cli, '_connect') as write, patch.object(cli, '_connect_readonly') as read:
            self.input.write_text('{}')
            self.assertEqual(self.invoke(['register', '--plan', str(self.input)])[0], 1)
            self.save_plan()
            self.assertEqual(self.invoke(['rank', '--plan', str(self.input),
                '--executor-plan-id', 'a' * 64, '--window-ordinal', '1'])[0], 1)
            self.output.write_text('immutable evidence')
            self.assertEqual(self.invoke(['register', '--plan', str(self.input)])[0], 1)
        write.assert_not_called()
        read.assert_not_called()
        self.assertEqual(self.output.read_text(), 'immutable evidence')

    def test_no_fallback_to_primary_or_source_database(self):
        self.save_plan()
        os.environ['DATABASE_URL'] = 'postgresql://primary'
        os.environ['RESEARCH_NO_HORIZON_READ_DATABASE_URL'] = 'postgresql://source'
        with patch.object(cli, '_connect') as write, patch.object(cli, '_connect_readonly') as read:
            self.assertEqual(self.invoke(['register', '--plan', str(self.input)])[0], 1)
            self.assertEqual(self.invoke(['rank', '--plan', str(self.input),
                '--executor-plan-id', 'a' * 64])[0], 1)
        write.assert_not_called()
        read.assert_not_called()

    def test_rank_uses_only_readonly_destination_and_never_executes_jobs(self):
        plan = self.save_plan()
        os.environ['RESEARCH_NO_HORIZON_DATABASE_URL'] = 'postgresql://research'
        backend = MagicMock()
        backend.report.return_value = {'frozen': 'report'}
        ranked = {'global_ranking_complete': True, 'plan_sha256': plan['plan_sha256']}
        with patch.object(cli, '_connect') as write, \
             patch.object(cli, '_connect_readonly', return_value=MagicMock()) as read, \
             patch.object(cli.executor, 'schema_status', return_value={'schema_present': True}), \
             patch.object(cli.executor, 'PostgresCohortStore', return_value=backend), \
             patch.object(cli.ranking, 'rank_report', return_value=ranked) as rank:
            status, _, err = self.invoke(['rank', '--plan', str(self.input),
                '--executor-plan-id', 'a' * 64])
        self.assertEqual((status, err), (0, ''))
        write.assert_not_called()
        read.assert_called_once_with('postgresql://research')
        backend.report.assert_called_once_with('a' * 64)
        backend.run_once.assert_not_called()
        rank.assert_called_once_with(plan, backend.report.return_value, window_ordinal=0)
        self.assertEqual(json.loads(self.output.read_text()), ranked)

    def test_pending_rank_exports_diagnostic_with_exit_two(self):
        self.save_plan()
        os.environ['RESEARCH_NO_HORIZON_DATABASE_URL'] = 'configured'
        with patch.object(cli, '_connect_readonly', return_value=MagicMock()), \
             patch.object(cli.executor, 'schema_status', return_value={'schema_present': True}), \
             patch.object(cli.executor, 'PostgresCohortStore', return_value=MagicMock()), \
             patch.object(cli.ranking, 'rank_report', return_value={'global_ranking_complete': False}):
            self.assertEqual(self.invoke(['rank', '--plan', str(self.input),
                '--executor-plan-id', 'a' * 64])[0], 2)
        self.assertFalse(json.loads(self.output.read_text())['global_ranking_complete'])

    def test_registration_routes_exact_plan_to_existing_queue(self):
        plan = self.save_plan()
        os.environ['RESEARCH_NO_HORIZON_DATABASE_URL'] = 'postgresql://research'
        backend = MagicMock()
        receipt = {'registration_complete': True, 'plan_sha256': plan['plan_sha256']}
        with patch.object(cli, '_connect', return_value=MagicMock()) as write, \
             patch.object(cli, '_connect_readonly') as read, \
             patch.object(cli.acquisition, 'schema_status', return_value={'schema_present': True}), \
             patch.object(cli.acquisition, 'AcquisitionStore', return_value=backend), \
             patch.object(cli.discovery, 'register_plan', return_value=receipt) as register:
            self.assertEqual(self.invoke(['register', '--plan', str(self.input)])[0], 0)
        write.assert_called_once_with('postgresql://research')
        read.assert_not_called()
        register.assert_called_once_with(backend, plan)

    def test_driver_failure_is_sanitized_and_success_receipt_not_written(self):
        self.save_plan()
        os.environ['RESEARCH_NO_HORIZON_DATABASE_URL'] = 'configured'
        with patch.object(cli, '_connect', side_effect=RuntimeError('postgresql://secret/source-payload')):
            status, out, err = self.invoke(['register', '--plan', str(self.input)])
        self.assertEqual((status, out, err), (1, '', 'DISCOVERY_FAILED: RuntimeError\n'))
        self.assertFalse(self.output.exists())


if __name__ == '__main__':
    unittest.main()
