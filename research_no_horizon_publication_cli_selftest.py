"""Publication command boundaries: trusted read-only inputs and create-only artifacts."""
from contextlib import ExitStack, contextmanager, redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, call, patch

import research_no_horizon_contract as contracts
import research_no_horizon_publication_cli as cli
import research_no_horizon_selection as selection
from research_no_horizon_cohort_coverage_selftest import cohort_fixture


class PublicationCLITests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.selection_plan = selection.build_plan(cohort_fixture()[0],
            base_directions=['SHORT'], thresholds_pct=[.25],
            candidate_keys=['FUTURES_CVD_TOTAL_65'], window_count=2,
            top_k=1, required_eligible_windows=1)

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.input = self.root / 'validation.json'
        self.plan = {'frozen_validation': 'trusted fixture'}
        self.input.write_text(contracts.canonical(self.plan), encoding='utf-8')
        self.selection_input = self.root / 'selection.json'
        self.selection_input.write_text(contracts.canonical(self.selection_plan), encoding='utf-8')
        self.output = self.root / 'publication'
        self.ids = ['b' * 64, 'a' * 64]
        self.reports = [{'plan_id': self.ids[0], 'payload': 'first'},
                        {'plan_id': self.ids[1], 'payload': 'second'}]
        self.evidence = {'selection_plan': self.selection_plan, 'selection_reports': self.reports}
        self.result = {
            'state': 'COMPLETE_NO_QUALIFICATION',
            'publication_sha256': 'd' * 64,
            'validation_result': {'validation_complete': True, 'qualified_scope_ids': []},
            'database_origin_authenticated_by_this_tool': False,
            'source_provenance_verified_by_this_tool': False,
            'runtime_authorized': False, 'telegram_authorized': False,
            'trading_authorized': False,
            'evidence': {'raw_hash': 'e' * 64, 'all_candidates': ['a', 'b']},
        }
        self.markdown = '# דוח מחקר\n\nאין מועמדים שעברו את הסף.\n'
        environment = patch.dict(os.environ, {'RESEARCH_NO_HORIZON_DATABASE_URL':
                                              'postgresql://explicit-research'}, clear=True)
        environment.start()
        self.addCleanup(environment.stop)

    def args(self, *, ids=None, output=None):
        args = ['--plan', str(self.input), '--selection-plan', str(self.selection_input),
                '--output-directory', str(output or self.output)]
        for plan_id in self.ids if ids is None else ids:
            args.extend(['--training-executor-plan-id', plan_id])
        return args

    def invoke(self, *, ids=None, output=None):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            status = cli.main(self.args(ids=ids, output=output))
        return status, out.getvalue(), err.getvalue()

    @contextmanager
    def wired(self):
        events, conn = [], object()
        backend, intake = MagicMock(), MagicMock()
        backend.report.side_effect = self.reports

        @contextmanager
        def session(url):
            events.append('read:enter')
            try:
                yield conn
            finally:
                events.append('read:exit')

        with ExitStack() as stack:
            def install(target, name, **kwargs):
                return stack.enter_context(patch.object(target, name, **kwargs))
            mocks = {
                'events': events, 'conn': conn, 'backend': backend, 'intake': intake,
                'read': install(cli, '_connect_readonly', side_effect=session),
                'schema': install(cli.acquisition, 'schema_status', return_value={'schema_present': True}),
                'executor_store': install(cli.executor, 'PostgresCohortStore', return_value=backend),
                'acquisition_store': install(cli.acquisition, 'AcquisitionStore', return_value=intake),
                'publish': install(cli.publication, 'publish_plan', return_value=self.result),
                'render': install(cli.publication, 'render_markdown', return_value=self.markdown),
            }
            yield mocks

    def test_reads_every_ordered_training_report_and_publishes_derived_evidence_readonly(self):
        with self.wired() as mocks:
            def publish(*args, **kwargs):
                self.assertEqual(mocks['events'], ['read:enter'])
                self.assertFalse(self.output.exists())
                mocks['events'].append('published')
                return self.result
            mocks['publish'].side_effect = publish

            def render(value):
                self.assertEqual(mocks['events'], ['read:enter', 'published', 'read:exit'])
                self.assertFalse(self.output.exists())
                return self.markdown
            mocks['render'].side_effect = render
            status, _, err = self.invoke()
        self.assertEqual((status, err), (0, ''))
        mocks['read'].assert_called_once_with('postgresql://explicit-research')
        mocks['schema'].assert_called_once_with(mocks['conn'])
        mocks['executor_store'].assert_called_once_with(mocks['conn'])
        mocks['acquisition_store'].assert_called_once_with(mocks['conn'])
        self.assertEqual(mocks['backend'].report.call_args_list,
                         [call(self.ids[0]), call(self.ids[1])])
        mocks['publish'].assert_called_once_with(
            mocks['intake'], mocks['backend'], self.plan, **self.evidence)
        mocks['render'].assert_called_once_with(self.result)
        for store in (mocks['backend'], mocks['intake']):
            store.run_once.assert_not_called()
            store.submit_cohort.assert_not_called()
            store.register_request.assert_not_called()

    def test_canonical_json_and_markdown_preserve_all_evidence_and_false_provenance(self):
        with self.wired():
            self.assertEqual(self.invoke()[0], 0)
        self.assertEqual(sorted(path.name for path in self.output.iterdir()),
                         ['publication.json', 'report.md'])
        self.assertEqual((self.output / 'publication.json').read_bytes(),
                         (contracts.canonical(self.result) + '\n').encode('utf-8'))
        self.assertEqual(json.loads((self.output / 'publication.json').read_text()), self.result)
        self.assertEqual((self.output / 'report.md').read_bytes(), self.markdown.encode('utf-8'))

    def test_incomplete_diagnostic_still_exports_both_files_and_returns_two(self):
        self.result['state'] = 'INCOMPLETE'
        self.result['validation_result']['validation_complete'] = False
        with self.wired():
            self.assertEqual(self.invoke()[0], 2)
        self.assertEqual(json.loads((self.output / 'publication.json').read_text()), self.result)
        self.assertEqual((self.output / 'report.md').read_text(), self.markdown)

    def test_qualified_complete_result_returns_zero_without_adding_authority(self):
        self.result['state'] = 'COMPLETE_QUALIFIED'
        self.result['validation_result']['qualified_scope_ids'] = ['c' * 64]
        with self.wired():
            self.assertEqual(self.invoke()[0], 0)
        self.assertEqual(json.loads((self.output / 'publication.json').read_text()), self.result)

    def test_no_primary_or_source_database_fallback(self):
        del os.environ['RESEARCH_NO_HORIZON_DATABASE_URL']
        os.environ['DATABASE_URL'] = 'postgresql://primary'
        os.environ['RESEARCH_NO_HORIZON_READ_DATABASE_URL'] = 'postgresql://source'
        with self.wired() as mocks:
            self.assertEqual(self.invoke()[0], 1)
        mocks['read'].assert_not_called()
        mocks['publish'].assert_not_called()
        self.assertFalse(self.output.exists())

    def test_invalid_selection_and_training_denominator_reject_before_database(self):
        with self.wired() as mocks:
            for ids in ([self.ids[0]], [self.ids[0], self.ids[0]],
                        [self.ids[0], 'C' * 64], [self.ids[0], 'a' * 63],
                        [*self.ids, 'c' * 64]):
                with self.subTest(ids=ids):
                    self.assertEqual(self.invoke(ids=ids)[0], 1)
            self.selection_input.write_text('{}')
            self.assertEqual(self.invoke()[0], 1)
        mocks['read'].assert_not_called()
        mocks['publish'].assert_not_called()
        self.assertFalse(self.output.exists())

    def test_no_caller_override_for_future_request_or_executor_identity(self):
        with self.wired() as mocks:
            for flag in ('--executor-plan-id', '--future-executor-plan-id', '--request-id'):
                with self.subTest(flag=flag), redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit) as raised:
                        cli.main([*self.args(), flag, 'f' * 64])
                    self.assertEqual(raised.exception.code, 2)
        mocks['read'].assert_not_called()
        self.assertFalse(self.output.exists())

    def test_existing_targets_symlinks_and_missing_parents_never_overwrite_or_read_database(self):
        self.output.mkdir()
        retained = self.output / 'publication.json'
        retained.write_text('retained evidence')
        symlink = self.root / 'link'
        symlink.symlink_to(self.output, target_is_directory=True)
        dangling = self.root / 'dangling'
        dangling.symlink_to(self.root / 'absent', target_is_directory=True)
        with self.wired() as mocks:
            for target in (self.output, symlink, dangling, self.input,
                           self.selection_input, self.root / 'missing' / 'new'):
                with self.subTest(target=target):
                    self.assertEqual(self.invoke(output=target)[0], 1)
        mocks['read'].assert_not_called()
        self.assertEqual(retained.read_text(), 'retained evidence')
        self.assertEqual(json.loads(self.input.read_text()), self.plan)
        self.assertEqual(json.loads(self.selection_input.read_text()), self.selection_plan)
        self.assertTrue(symlink.is_symlink())
        self.assertTrue(dangling.is_symlink())
        self.assertFalse((self.root / 'missing').exists())

    def test_malformed_or_oversized_inputs_reject_before_database(self):
        with self.wired() as mocks:
            for raw in (b'{', b'{"same":1,"same":2}', b'{"v":NaN}', b'\xff'):
                with self.subTest(raw=raw):
                    self.input.write_bytes(raw)
                    self.assertEqual(self.invoke()[0], 1)
            self.input.write_text(contracts.canonical(self.plan))
            with patch.object(cli, 'MAX_PLAN_BYTES', 1):
                self.assertEqual(self.invoke()[0], 1)
            with patch.object(cli, 'MAX_PLAN_BYTES', self.input.stat().st_size):
                self.assertEqual(self.invoke()[0], 1)  # Larger selection input is also bounded.
        mocks['read'].assert_not_called()
        self.assertFalse(self.output.exists())

    def test_missing_schema_stops_before_store_construction_or_report_reads(self):
        with self.wired() as mocks:
            mocks['schema'].return_value = {'schema_present': False}
            self.assertEqual(self.invoke()[0], 1)
        self.assertEqual(mocks['events'], ['read:enter', 'read:exit'])
        mocks['executor_store'].assert_not_called()
        mocks['acquisition_store'].assert_not_called()
        mocks['publish'].assert_not_called()
        self.assertFalse(self.output.exists())

    def test_aggregate_training_budget_rejects_before_publication_or_output_creation(self):
        maximum = sum(len(contracts.canonical(report).encode('utf-8')) for report in self.reports) - 1
        with self.wired() as mocks, patch.object(cli, 'MAX_REPORTS_BYTES', maximum):
            self.assertEqual(self.invoke()[0], 1)
        self.assertEqual(mocks['backend'].report.call_count, 2)
        self.assertEqual(mocks['events'], ['read:enter', 'read:exit'])
        mocks['publish'].assert_not_called()
        mocks['render'].assert_not_called()
        self.assertFalse(self.output.exists())

    def test_json_and_utf8_markdown_budgets_reject_before_creating_directory(self):
        with self.wired() as mocks, patch.object(cli, 'MAX_RESULT_BYTES', 1):
            self.assertEqual(self.invoke()[0], 1)
        mocks['publish'].assert_called_once()
        self.assertFalse(self.output.exists())
        self.assertGreater(len(self.markdown.encode('utf-8')), len(self.markdown))
        with self.wired() as mocks, patch.object(cli, 'MAX_MARKDOWN_BYTES', len(self.markdown)):
            self.assertEqual(self.invoke()[0], 1)
        mocks['render'].assert_called_once()
        self.assertFalse(self.output.exists())

    def test_database_and_publication_errors_are_sanitized_without_output(self):
        with self.wired() as mocks:
            mocks['read'].side_effect = RuntimeError('postgresql://secret/db-payload')
            self.assertEqual(self.invoke(), (1, '', 'PUBLICATION_FAILED: RuntimeError\n'))
        with self.wired() as mocks:
            mocks['publish'].side_effect = ValueError('private source evidence')
            self.assertEqual(self.invoke(), (1, '', 'PUBLICATION_FAILED: ValueError\n'))
        with self.wired() as mocks:
            mocks['render'].side_effect = ValueError('private report evidence')
            self.assertEqual(self.invoke(), (1, '', 'PUBLICATION_FAILED: ValueError\n'))
        self.assertFalse(self.output.exists())

    def test_second_file_write_failure_cleans_only_created_files_and_empty_owned_directory(self):
        original_open = Path.open

        def open_file(path, *args, **kwargs):
            if path == self.output / 'report.md':
                raise OSError('disk failure contains sensitive source payload')
            return original_open(path, *args, **kwargs)

        with self.wired(), patch.object(Path, 'open', new=open_file):
            self.assertEqual(self.invoke(), (1, '', 'PUBLICATION_FAILED: OSError\n'))
        self.assertFalse(self.output.exists())
        self.assertEqual(json.loads(self.input.read_text()), self.plan)

    def test_directory_creation_race_does_not_remove_foreign_directory(self):
        original_mkdir = os.mkdir

        def mkdir(path, *args, **kwargs):
            if Path(path) == self.output:
                original_mkdir(path, *args, **kwargs)
                (self.output / 'foreign').write_text('concurrent writer')
                raise FileExistsError('another writer won')
            return original_mkdir(path, *args, **kwargs)

        with self.wired(), patch.object(os, 'mkdir', new=mkdir):
            self.assertEqual(self.invoke()[0], 1)
        self.assertEqual((self.output / 'foreign').read_text(), 'concurrent writer')
        self.assertFalse((self.output / 'publication.json').exists())

    def test_exclusive_second_file_collision_preserves_foreign_file_during_cleanup(self):
        original_open = Path.open

        def open_file(path, *args, **kwargs):
            if path == self.output / 'report.md' and args and args[0] == 'x':
                with original_open(path, 'w', encoding='utf-8') as stream:
                    stream.write('concurrent evidence')
                raise FileExistsError('another writer won')
            return original_open(path, *args, **kwargs)

        with self.wired(), patch.object(Path, 'open', new=open_file):
            self.assertEqual(self.invoke()[0], 1)
        self.assertEqual((self.output / 'report.md').read_text(), 'concurrent evidence')
        self.assertFalse((self.output / 'publication.json').exists())


if __name__ == '__main__':
    unittest.main()
