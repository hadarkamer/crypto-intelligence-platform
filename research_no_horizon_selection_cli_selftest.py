"""Selection command boundaries: explicit plans, ordered reports and read-only use."""
from contextlib import redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, call, patch

import research_no_horizon_selection as selection
import research_no_horizon_selection_cli as cli
import research_no_horizon_contract as contracts
from research_no_horizon_cohort_coverage_selftest import cohort_fixture


class SelectionCLITests(unittest.TestCase):
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

    def invoke(self, args, *, output=None):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            status = cli.main([*args, '--output', str(output or self.output)])
        return status, out.getvalue(), err.getvalue()

    def save_plan(self):
        plan = selection.build_plan(self.declaration, base_directions=['SHORT'],
            thresholds_pct=[.25], candidate_keys=['FUTURES_CVD_TOTAL_65'],
            window_count=2, top_k=1, required_eligible_windows=1)
        self.input.write_text(contracts.canonical(plan), encoding='utf-8')
        return plan

    def build_args(self):
        return ['build', '--declaration', str(self.input), '--base-direction', 'SHORT',
            '--threshold-pct', '.25', '--candidate-key', 'FUTURES_CVD_TOTAL_65',
            '--windows', '2', '--top-k', '1', '--required-eligible-windows', '1']

    def select_args(self, ids=None):
        args = ['select', '--plan', str(self.input)]
        for plan_id in (['a' * 64, 'b' * 64] if ids is None else ids):
            args.extend(['--executor-plan-id', plan_id])
        return args

    def test_build_is_offline_and_freezes_explicit_selection_policy(self):
        with patch.object(cli, '_connect') as write, patch.object(cli, '_connect_readonly') as read:
            status, _, err = self.invoke(self.build_args())
        self.assertEqual((status, err), (0, ''))
        write.assert_not_called()
        read.assert_not_called()
        saved = json.loads(self.output.read_text())
        expected = selection.build_plan(self.declaration, base_directions=['SHORT'],
            thresholds_pct=[.25], candidate_keys=['FUTURES_CVD_TOTAL_65'],
            window_count=2, top_k=1, required_eligible_windows=1)
        self.assertEqual(saved, expected)
        self.assertEqual(selection.validate_plan(saved), saved)

    def test_invalid_selection_policy_and_input_size_fail_before_connection(self):
        with patch.object(cli, '_connect') as write, patch.object(cli, '_connect_readonly') as read:
            for flag, value in (('--top-k', '0'), ('--required-eligible-windows', '3'),
                                ('--threshold-pct', 'nan')):
                args = self.build_args()
                args[args.index(flag) + 1] = value
                with self.subTest(flag=flag):
                    self.assertEqual(self.invoke(args)[0], 1)
            with patch.object(cli, 'MAX_PLAN_BYTES', 1):
                self.assertEqual(self.invoke(self.build_args())[0], 1)
        write.assert_not_called()
        read.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_missing_duplicate_or_malformed_executor_ids_fail_before_connection(self):
        self.save_plan()
        os.environ['RESEARCH_NO_HORIZON_DATABASE_URL'] = 'configured'
        with patch.object(cli, '_connect') as write, patch.object(cli, '_connect_readonly') as read:
            for ids in (['a' * 64], ['a' * 64, 'a' * 64],
                        ['a' * 64, 'B' * 64], ['a' * 64, 'b' * 63],
                        ['a' * 64, 'b' * 64, 'c' * 64]):
                with self.subTest(ids=ids):
                    self.assertEqual(self.invoke(self.select_args(ids))[0], 1)
        write.assert_not_called()
        read.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_invalid_plan_and_input_output_collisions_preserve_files_before_connection(self):
        with patch.object(cli, '_connect') as write, patch.object(cli, '_connect_readonly') as read:
            self.input.write_text('{}')
            self.assertEqual(self.invoke(['register', '--plan', str(self.input)])[0], 1)
            self.save_plan()
            original = self.input.read_bytes()
            self.assertEqual(self.invoke(['register', '--plan', str(self.input)], output=self.input)[0], 1)
            self.assertEqual(self.input.read_bytes(), original)
            self.output.write_text('retained receipt')
            self.assertEqual(self.invoke(self.select_args())[0], 1)
        write.assert_not_called()
        read.assert_not_called()
        self.assertEqual(self.output.read_text(), 'retained receipt')

    def test_primary_and_source_settings_never_substitute_for_explicit_destination(self):
        self.save_plan()
        os.environ['DATABASE_URL'] = 'postgresql://primary'
        os.environ['RESEARCH_NO_HORIZON_READ_DATABASE_URL'] = 'postgresql://source'
        with patch.object(cli, '_connect') as write, patch.object(cli, '_connect_readonly') as read:
            self.assertEqual(self.invoke(['register', '--plan', str(self.input)])[0], 1)
            self.assertEqual(self.invoke(self.select_args())[0], 1)
        write.assert_not_called()
        read.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_registration_routes_exact_plan_to_existing_queue_only(self):
        plan = self.save_plan()
        os.environ['RESEARCH_NO_HORIZON_DATABASE_URL'] = 'postgresql://research'
        backend = MagicMock()
        receipt = {'registration_complete': True, 'plan_sha256': plan['plan_sha256']}
        with patch.object(cli, '_connect', return_value=MagicMock()) as write, \
             patch.object(cli, '_connect_readonly') as read, \
             patch.object(cli.acquisition, 'schema_status', return_value={'schema_present': True}), \
             patch.object(cli.acquisition, 'AcquisitionStore', return_value=backend), \
             patch.object(cli.selection, 'register_plan', return_value=receipt) as register:
            status, _, err = self.invoke(['register', '--plan', str(self.input)])
        self.assertEqual((status, err), (0, ''))
        write.assert_called_once_with('postgresql://research')
        read.assert_not_called()
        register.assert_called_once_with(backend, plan)
        self.assertEqual(json.loads(self.output.read_text()), receipt)

    def test_selection_loads_every_raw_report_in_order_readonly_and_complete_empty_is_success(self):
        plan = self.save_plan()
        os.environ['RESEARCH_NO_HORIZON_DATABASE_URL'] = 'postgresql://research'
        ids = ['b' * 64, 'a' * 64]
        reports = [{'plan_id': ids[0], 'raw': ['first']}, {'plan_id': ids[1], 'raw': ['second']}]
        backend = MagicMock()
        backend.report.side_effect = reports
        result = {'selection_complete': True, 'selected_scope_ids': []}
        with patch.object(cli, '_connect') as write, \
             patch.object(cli, '_connect_readonly', return_value=MagicMock()) as read, \
             patch.object(cli.executor, 'schema_status', return_value={'schema_present': True}), \
             patch.object(cli.executor, 'PostgresCohortStore', return_value=backend), \
             patch.object(cli.selection, 'select_reports', return_value=result) as select:
            status, _, err = self.invoke(self.select_args(ids))
        self.assertEqual((status, err), (0, ''))
        write.assert_not_called()
        read.assert_called_once_with('postgresql://research')
        self.assertEqual(backend.report.call_args_list, [call(ids[0]), call(ids[1])])
        backend.run_once.assert_not_called()
        backend.submit_cohort.assert_not_called()
        select.assert_called_once_with(plan, reports)
        self.assertEqual(json.loads(self.output.read_text()), result)

    def test_pending_selection_exports_diagnostic_and_returns_two(self):
        self.save_plan()
        os.environ['RESEARCH_NO_HORIZON_DATABASE_URL'] = 'configured'
        backend = MagicMock()
        backend.report.return_value = {'pending': True}
        result = {'selection_complete': False, 'reason': 'WAITING_FOR_ALL_WINDOWS'}
        with patch.object(cli, '_connect_readonly', return_value=MagicMock()), \
             patch.object(cli.executor, 'schema_status', return_value={'schema_present': True}), \
             patch.object(cli.executor, 'PostgresCohortStore', return_value=backend), \
             patch.object(cli.selection, 'select_reports', return_value=result):
            self.assertEqual(self.invoke(self.select_args())[0], 2)
        self.assertEqual(json.loads(self.output.read_text()), result)

    def test_missing_schema_stops_both_paths_before_constructing_store_or_registering(self):
        self.save_plan()
        os.environ['RESEARCH_NO_HORIZON_DATABASE_URL'] = 'configured'
        with patch.object(cli, '_connect', return_value=MagicMock()), \
             patch.object(cli, '_connect_readonly', return_value=MagicMock()), \
             patch.object(cli.acquisition, 'schema_status', return_value={'schema_present': False}), \
             patch.object(cli.executor, 'schema_status', return_value={'schema_present': False}), \
             patch.object(cli.acquisition, 'AcquisitionStore') as intake, \
             patch.object(cli.executor, 'PostgresCohortStore') as executor, \
             patch.object(cli.selection, 'register_plan') as register, \
             patch.object(cli.selection, 'select_reports') as select:
            self.assertEqual(self.invoke(['register', '--plan', str(self.input)])[0], 1)
            self.assertEqual(self.invoke(self.select_args())[0], 1)
        intake.assert_not_called()
        executor.assert_not_called()
        register.assert_not_called()
        select.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_aggregate_report_budget_rejects_before_selection_without_partial_output(self):
        self.save_plan()
        os.environ['RESEARCH_NO_HORIZON_DATABASE_URL'] = 'configured'
        reports = [{'payload': 'a' * 32}, {'payload': 'b' * 32}]
        maximum = sum(len(contracts.canonical(report).encode('utf-8')) for report in reports) - 1
        backend = MagicMock()
        backend.report.side_effect = reports
        with patch.object(cli, '_connect_readonly', return_value=MagicMock()), \
             patch.object(cli.executor, 'schema_status', return_value={'schema_present': True}), \
             patch.object(cli.executor, 'PostgresCohortStore', return_value=backend), \
             patch.object(cli, 'MAX_REPORTS_BYTES', maximum), \
             patch.object(cli.selection, 'select_reports') as select:
            self.assertEqual(self.invoke(self.select_args())[0], 1)
        self.assertEqual(backend.report.call_count, 2)
        select.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_result_byte_limit_rejects_before_creating_output(self):
        self.save_plan()
        os.environ['RESEARCH_NO_HORIZON_DATABASE_URL'] = 'configured'
        backend = MagicMock()
        backend.report.return_value = {'raw': 'report'}
        result = {'selection_complete': True, 'payload': 'x' * 128}
        with patch.object(cli, '_connect_readonly', return_value=MagicMock()), \
             patch.object(cli.executor, 'schema_status', return_value={'schema_present': True}), \
             patch.object(cli.executor, 'PostgresCohortStore', return_value=backend), \
             patch.object(cli.selection, 'select_reports', return_value=result), \
             patch.object(cli, 'MAX_RESULT_BYTES', 64):
            self.assertEqual(self.invoke(self.select_args())[0], 1)
        self.assertFalse(self.output.exists())

    def test_driver_errors_from_both_connections_are_sanitized_without_receipt(self):
        self.save_plan()
        os.environ['RESEARCH_NO_HORIZON_DATABASE_URL'] = 'configured'
        with patch.object(cli, '_connect', side_effect=RuntimeError('postgresql://secret/write-data')), \
             patch.object(cli, '_connect_readonly', side_effect=RuntimeError('postgresql://secret/read-data')):
            for args in (['register', '--plan', str(self.input)], self.select_args()):
                with self.subTest(command=args[0]):
                    self.assertEqual(self.invoke(args), (1, '', 'SELECTION_FAILED: RuntimeError\n'))
        self.assertFalse(self.output.exists())


if __name__ == '__main__':
    unittest.main()
