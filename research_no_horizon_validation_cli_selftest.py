"""Validation command boundaries and training-read-before-registration sequencing."""
from contextlib import ExitStack, contextmanager, redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, call, patch

import research_no_horizon_contract as contracts
import research_no_horizon_selection as selection
import research_no_horizon_validation_cli as cli
from research_no_horizon_cohort_coverage_selftest import cohort_fixture


class ValidationCLITests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.declaration = cohort_fixture()[0]
        cls.selection_plan = selection.build_plan(cls.declaration, base_directions=['SHORT'],
            thresholds_pct=[.25], candidate_keys=['FUTURES_CVD_TOTAL_65'],
            window_count=2, top_k=1, required_eligible_windows=1)

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.input = self.root / 'input.json'
        self.input.write_text(contracts.canonical(self.declaration), encoding='utf-8')
        self.selection_input = self.root / 'selection.json'
        self.selection_input.write_text(contracts.canonical(self.selection_plan), encoding='utf-8')
        self.output = self.root / 'output.json'
        self.ids = ['b' * 64, 'a' * 64]
        self.reports = [{'plan_id': self.ids[0], 'payload': 'first'},
                        {'plan_id': self.ids[1], 'payload': 'second'}]
        self.plan = {'frozen_validation': 'fixture'}
        self.evidence = {'selection_plan': self.selection_plan, 'selection_reports': self.reports}
        environment = patch.dict(os.environ, {'RESEARCH_NO_HORIZON_DATABASE_URL':
                                              'postgresql://explicit-research'}, clear=True)
        environment.start()
        self.addCleanup(environment.stop)

    def invoke(self, command, *, ids=None, output=None):
        args = [command, '--declaration' if command == 'build' else '--plan', str(self.input),
                '--selection-plan', str(self.selection_input), '--output', str(output or self.output)]
        for plan_id in self.ids if ids is None else ids:
            args.extend(['--training-executor-plan-id', plan_id])
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            status = cli.main(args)
        return status, out.getvalue(), err.getvalue()

    @contextmanager
    def wired(self):
        events, read_conn, write_conn = [], object(), object()
        backend, intake = MagicMock(), MagicMock()
        backend.report.side_effect = self.reports

        @contextmanager
        def session(label, conn):
            events.append(label + ':enter')
            try:
                yield conn
            finally:
                events.append(label + ':exit')

        with ExitStack() as stack:
            def install(target, name, **kwargs):
                return stack.enter_context(patch.object(target, name, **kwargs))
            mocks = {
                'events': events, 'read_conn': read_conn, 'write_conn': write_conn,
                'backend': backend, 'intake': intake,
                'read': install(cli, '_connect_readonly', side_effect=lambda url: session('read', read_conn)),
                'write': install(cli, '_connect', side_effect=lambda url: session('write', write_conn)),
                'executor_schema': install(cli.executor, 'schema_status', return_value={'schema_present': True}),
                'acquisition_schema': install(cli.acquisition, 'schema_status', return_value={'schema_present': True}),
                'executor_store': install(cli.executor, 'PostgresCohortStore', return_value=backend),
                'acquisition_store': install(cli.acquisition, 'AcquisitionStore', return_value=intake),
                'build': install(cli.validation, 'build_plan', return_value=self.plan),
                'validate': install(cli.validation, 'validate_plan', return_value=self.plan),
                'register': install(cli.validation, 'register_plan', return_value={'registration_complete': True}),
                'evaluate': install(cli.validation, 'evaluate_plan',
                                    return_value={'validation_complete': True, 'qualified': False}),
            }
            yield mocks

    def test_build_reads_exact_ordered_training_evidence_in_readonly_destination(self):
        with self.wired() as mocks:
            status, _, err = self.invoke('build')
        self.assertEqual((status, err), (0, ''))
        mocks['read'].assert_called_once_with('postgresql://explicit-research')
        mocks['write'].assert_not_called()
        self.assertEqual(mocks['events'], ['read:enter', 'read:exit'])
        self.assertEqual(mocks['backend'].report.call_args_list,
                         [call(self.ids[0]), call(self.ids[1])])
        mocks['build'].assert_called_once_with(self.declaration, **self.evidence)
        mocks['backend'].run_once.assert_not_called()
        mocks['backend'].submit_cohort.assert_not_called()
        self.assertEqual(json.loads(self.output.read_text()), self.plan)

    def test_registration_validates_before_read_session_closes_and_write_session_opens(self):
        with self.wired() as mocks:
            def validated(value, **evidence):
                self.assertEqual(mocks['events'], ['read:enter'])
                mocks['events'].append('validated')
                return self.plan
            mocks['validate'].side_effect = validated
            status, _, err = self.invoke('register')
        self.assertEqual((status, err), (0, ''))
        self.assertEqual(mocks['events'],
                         ['read:enter', 'validated', 'read:exit', 'write:enter', 'write:exit'])
        mocks['validate'].assert_called_once_with(self.declaration, **self.evidence)
        mocks['write'].assert_called_once_with('postgresql://explicit-research')
        mocks['acquisition_store'].assert_called_once_with(mocks['write_conn'])
        mocks['register'].assert_called_once_with(mocks['intake'], self.plan, **self.evidence)
        mocks['evaluate'].assert_not_called()

    def test_invalid_validation_binding_never_opens_write_session(self):
        with self.wired() as mocks:
            mocks['validate'].side_effect = ValueError('foreign training evidence')
            self.assertEqual(self.invoke('register')[0], 1)
        self.assertEqual(mocks['events'], ['read:enter', 'read:exit'])
        mocks['write'].assert_not_called()
        mocks['register'].assert_not_called()
        self.assertFalse(self.output.exists())

    def test_evaluation_is_readonly_derived_from_plan_and_complete_nonqualification_succeeds(self):
        with self.wired() as mocks:
            status, _, err = self.invoke('evaluate')
        self.assertEqual((status, err), (0, ''))
        mocks['write'].assert_not_called()
        mocks['acquisition_store'].assert_called_once_with(mocks['read_conn'])
        mocks['evaluate'].assert_called_once_with(
            mocks['intake'], mocks['backend'], self.plan, **self.evidence)
        mocks['backend'].run_once.assert_not_called()
        mocks['intake'].run_once.assert_not_called()
        self.assertEqual(json.loads(self.output.read_text()),
                         {'validation_complete': True, 'qualified': False})

    def test_pending_evaluation_retains_diagnostic_and_returns_two(self):
        with self.wired() as mocks:
            mocks['evaluate'].return_value = {'validation_complete': False, 'status': 'PENDING'}
            self.assertEqual(self.invoke('evaluate')[0], 2)
        self.assertEqual(json.loads(self.output.read_text()),
                         {'validation_complete': False, 'status': 'PENDING'})

    def test_no_primary_or_source_database_fallback_for_any_command(self):
        del os.environ['RESEARCH_NO_HORIZON_DATABASE_URL']
        os.environ['DATABASE_URL'] = 'postgresql://primary'
        os.environ['RESEARCH_NO_HORIZON_READ_DATABASE_URL'] = 'postgresql://source'
        with self.wired() as mocks:
            for command in ('build', 'register', 'evaluate'):
                with self.subTest(command=command):
                    self.assertEqual(self.invoke(command)[0], 1)
        mocks['read'].assert_not_called()
        mocks['write'].assert_not_called()
        self.assertFalse(self.output.exists())

    def test_invalid_selection_or_training_id_denominator_fails_before_database(self):
        with self.wired() as mocks:
            for ids in ([self.ids[0]], [self.ids[0], self.ids[0]], [self.ids[0], 'C' * 64]):
                with self.subTest(ids=ids):
                    self.assertEqual(self.invoke('build', ids=ids)[0], 1)
            self.selection_input.write_text('{}')
            self.assertEqual(self.invoke('build')[0], 1)
        mocks['read'].assert_not_called()
        mocks['write'].assert_not_called()

    def test_output_collisions_and_input_size_reject_without_mutation_or_database(self):
        original = self.selection_input.read_bytes()
        with self.wired() as mocks:
            self.assertEqual(self.invoke('build', output=self.selection_input)[0], 1)
            self.assertEqual(self.selection_input.read_bytes(), original)
            self.output.write_text('existing evidence')
            self.assertEqual(self.invoke('register')[0], 1)
            self.assertEqual(self.output.read_text(), 'existing evidence')
            with patch.object(cli, 'MAX_PLAN_BYTES', 1):
                self.assertEqual(self.invoke('build', output=self.root / 'new.json')[0], 1)
        mocks['read'].assert_not_called()
        mocks['write'].assert_not_called()

    def test_aggregate_training_report_limit_stops_before_validation_or_write(self):
        maximum = sum(len(contracts.canonical(report).encode()) for report in self.reports) - 1
        with self.wired() as mocks, patch.object(cli, 'MAX_REPORTS_BYTES', maximum):
            self.assertEqual(self.invoke('register')[0], 1)
        self.assertEqual(mocks['backend'].report.call_count, 2)
        mocks['validate'].assert_not_called()
        mocks['write'].assert_not_called()
        self.assertFalse(self.output.exists())

    def test_output_limit_prevents_partial_success_file(self):
        with self.wired() as mocks, patch.object(cli, 'MAX_RESULT_BYTES', 8):
            self.assertEqual(self.invoke('evaluate')[0], 1)
        mocks['evaluate'].assert_called_once()
        self.assertFalse(self.output.exists())

    def test_missing_read_or_write_schema_prevents_dependent_operations(self):
        with self.wired() as mocks:
            mocks['executor_schema'].return_value = {'schema_present': False}
            self.assertEqual(self.invoke('register')[0], 1)
        mocks['backend'].report.assert_not_called()
        mocks['write'].assert_not_called()
        with self.wired() as mocks:
            mocks['acquisition_schema'].return_value = {'schema_present': False}
            self.assertEqual(self.invoke('register')[0], 1)
        mocks['register'].assert_not_called()
        mocks['acquisition_store'].assert_not_called()
        with self.wired() as mocks:
            mocks['acquisition_schema'].return_value = {'schema_present': False}
            self.assertEqual(self.invoke('evaluate')[0], 1)
        mocks['backend'].report.assert_not_called()
        mocks['evaluate'].assert_not_called()
        self.assertFalse(self.output.exists())

    def test_driver_failures_are_sanitized_for_read_and_write_sessions(self):
        with self.wired() as mocks:
            mocks['read'].side_effect = RuntimeError('postgresql://secret/read-payload')
            self.assertEqual(self.invoke('build'), (1, '', 'VALIDATION_FAILED: RuntimeError\n'))
        with self.wired() as mocks:
            mocks['write'].side_effect = RuntimeError('postgresql://secret/write-payload')
            self.assertEqual(self.invoke('register'), (1, '', 'VALIDATION_FAILED: RuntimeError\n'))
        self.assertFalse(self.output.exists())


if __name__ == '__main__':
    unittest.main()
