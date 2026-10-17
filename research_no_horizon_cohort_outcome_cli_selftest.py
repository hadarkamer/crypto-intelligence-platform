"""Local CLI integration with genuine cohort proofs and synthetic prices.

These exercise admission before SQLite creation, immutable file boundaries,
restartable bounded outcomes and visible incomplete scope reports. No network.
"""
from contextlib import ExitStack, redirect_stderr, redirect_stdout
from copy import deepcopy
from datetime import timedelta
import io
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import research_no_horizon_cohort_outcome_cli as cli
import research_no_horizon_contract as contracts
from research_no_horizon_cohort_outcomes_selftest import outcome_fixture
from research_no_horizon_cohort_coverage_selftest import cross_part_rows
from research_no_horizon_parent_coverage_selftest import scope, source_row
from research_watch_score_capture_selftest import BASE


class CohortOutcomeCliTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original = outcome_fixture()

    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.db = self.root / 'outcomes.sqlite'
        self.install(deepcopy(self.original))

    def write_json(self, name, value):
        path = self.root / name
        path.write_text(contracts.canonical(value), encoding='utf-8')
        return path

    def install(self, fixture):
        self.declaration, self.anchor, self.exports = fixture
        self.declaration_path = self.write_json('declaration.json', self.declaration)
        self.anchor_path = self.write_json('anchor.json', self.anchor)
        self.parts = [self.write_json(f'part_{ordinal}.json', value)
            for ordinal, value in enumerate(self.exports)]

    def arguments(self, command, output, *, database=None, parts=None, plan_id=None, extra=()):
        args = [command, '--db', str(database or self.db), '--output', str(output)]
        if command == 'submit':
            args.extend(['--declaration', str(self.declaration_path), '--anchor', str(self.anchor_path)])
            for path in self.parts if parts is None else parts:
                args.extend(['--part', str(path)])
        else:
            args.append(plan_id or self.plan_id)
            if command == 'run':
                args.extend(['--worker-id', 'cli-selftest'])
        return args + list(extra)

    @staticmethod
    def invoke(arguments):
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            status = cli.main(arguments)
        return status, stdout.getvalue(), stderr.getvalue()

    def submit(self, *, name='submitted.json', database=None):
        output = self.root / name
        status, stdout, stderr = self.invoke(self.arguments('submit', output, database=database))
        self.assertEqual((status, stderr), (0, ''))
        receipt = json.loads(output.read_text())
        self.plan_id = receipt['plan_id']
        self.assertEqual(json.loads(stdout)['plan_id'], self.plan_id)
        return receipt

    def read_database(self, query):
        connection = sqlite3.connect(self.db)
        try:
            return connection.execute(query).fetchall()
        finally:
            connection.close()

    def test_submit_pending_run_complete_report_and_immutable_resubmission(self):
        submitted = self.submit()
        self.assertTrue(self.db.is_file())
        self.assertFalse(submitted['all_scopes_processed'])
        self.assertFalse(submitted['computation_complete'])
        self.assertEqual(submitted['scopes'][0]['status'], 'NOT_SUBMITTED')
        self.assertEqual(submitted['outcome_trials_executed'], 0)
        output = self.root / 'completed.json'
        status, _, stderr = self.invoke(self.arguments('run', output,
            extra=['--scope-budget', '1', '--candle-budget', '10000']))
        self.assertEqual((status, stderr), (0, ''))
        completed = json.loads(output.read_text())
        self.assertTrue(completed['all_scopes_processed'])
        self.assertTrue(completed['computation_complete'])
        self.assertEqual(completed['outcome_trials_executed'], 1)
        self.assertEqual(completed['candle_evaluations_this_run'], 5)
        self.assertEqual(completed['scopes'][0]['status'], 'COMPLETE')
        self.assertEqual(len(completed['scopes'][0]['outcomes']), 5)
        self.assertNotIn('watch:4:BTC:SHORT', {row['entry_id'] for row in completed['scopes'][0]['outcomes']})
        report = self.root / 'report.json'
        status, _, stderr = self.invoke(self.arguments('report', report))
        self.assertEqual((status, stderr), (0, ''))
        persisted = json.loads(report.read_text())
        repeated = self.submit(name='resubmitted.json')
        self.assertEqual(persisted, repeated)
        self.assertEqual(self.read_database('SELECT COUNT(*) FROM cohort_plans'), [(1,)])
        self.assertEqual(self.read_database('SELECT COUNT(*) FROM local_jobs'), [(1,)])
        for flag in ('runtime_authorized', 'telegram_authorized', 'trading_authorized'):
            self.assertFalse(persisted[flag])
        self.assertFalse(persisted['scope_pooling'])
        self.assertFalse(persisted['parts_are_trials'])
        self.assertFalse(persisted['validated_discovery'])

    def test_unknown_source_rejection_saves_all_scopes_without_opening_sqlite(self):
        rows = cross_part_rows()
        rows[1][1] = source_row(parent_offset=3, snapshot_set_id=5, unavailable=True)
        self.install(outcome_fixture(rows, scopes=[scope(), scope('LONG')]))
        output = self.root / 'blocked_source.json'
        with patch('sqlite3.connect', side_effect=AssertionError('Source rejection must precede SQLite')) as connect:
            status, stdout, stderr = self.invoke(self.arguments('submit', output))
        self.assertEqual((status, stdout, stderr), (2, '', ''))
        connect.assert_not_called()
        self.assertFalse(self.db.exists())
        receipt = json.loads(output.read_text())
        self.assertEqual(receipt['status'], 'INPUT_BLOCKED')
        self.assertFalse(receipt['database_opened'])
        self.assertEqual(len(receipt['coverage_receipt']['scopes']), 2)
        self.assertTrue(any(item['source_counts']['UNKNOWN'] == 1
            for item in receipt['coverage_receipt']['scopes']))

    def test_semantically_invalid_source_row_with_valid_proof_is_rejected_before_sqlite(self):
        rows = cross_part_rows()
        rows[1][1]['stored_source'] = None
        self.install(outcome_fixture(rows))
        output = self.root / 'invalid_source.json'
        with patch('sqlite3.connect', side_effect=AssertionError('No SQLite for invalid source')):
            status, _, stderr = self.invoke(self.arguments('submit', output))
        self.assertEqual((status, stderr), (2, ''))
        self.assertFalse(self.db.exists())
        receipt = json.loads(output.read_text())
        self.assertEqual(receipt['coverage_receipt']['scopes'][0]['source_counts']['UNKNOWN_SOURCE'], 1)

    def test_invalid_last_part_never_creates_database_or_success_output(self):
        variants = ({}, None, self.exports[0], {'candles': []})
        for number, value in enumerate(variants):
            with self.subTest(case=number):
                self.write_json(self.parts[1].name, value)
                output = self.root / f'invalid_last_{number}.json'
                with patch('sqlite3.connect', side_effect=AssertionError('Invalid part must precede SQLite')):
                    status, _, stderr = self.invoke(self.arguments('submit', output))
                self.assertEqual(status, 1)
                self.assertTrue(stderr)
                self.assertFalse(self.db.exists())
                self.assertFalse(output.exists())

    def test_missing_foreign_reordered_extra_parts_reject_before_sqlite(self):
        variants = ([self.parts[0]], [self.parts[0], self.root / 'missing.json'],
            list(reversed(self.parts)), [self.parts[0], self.parts[0]], self.parts + [self.parts[0]])
        for number, paths in enumerate(variants):
            with self.subTest(case=number):
                output = self.root / f'wrong_parts_{number}.json'
                with patch('sqlite3.connect', side_effect=AssertionError('Invalid part set must precede SQLite')):
                    status, _, stderr = self.invoke(self.arguments('submit', output, parts=paths))
                self.assertEqual(status, 1)
                self.assertTrue(stderr)
                self.assertFalse(self.db.exists())
                self.assertFalse(output.exists())

    def test_missing_selected_entry_retains_blocked_scope_and_processes_empty_scope(self):
        missing_open = BASE + timedelta(hours=4, minutes=7)
        candles = [bar for bar in self.exports[0]['candles']
            if contracts.utc(bar['open_time_utc']) != missing_open]
        self.install(outcome_fixture(scopes=[scope(), scope('LONG')], candles=candles))
        submitted = self.submit()
        blocked = next(row for row in submitted['scopes'] if row['base_direction'] == 'SHORT')
        self.assertEqual(blocked['status'], 'INPUT_BLOCKED')
        self.assertEqual(blocked['missing_entry_ids'], ['watch:3:BTC:SHORT'])
        self.assertEqual(blocked['error'], 'MISSING_SELECTED_ENTRY_OPEN')
        self.assertEqual(len(blocked['representatives']), 5)
        self.assertIsNone(blocked['job_id'])
        self.assertFalse(submitted['all_scopes_processed'])
        output = self.root / 'processed_with_blocked.json'
        status, stdout, stderr = self.invoke(self.arguments('run', output, extra=['--scope-budget', '2']))
        self.assertEqual((status, stderr), (2, ''))
        result = json.loads(output.read_text())
        self.assertTrue(result['all_scopes_processed'])
        self.assertFalse(result['computation_complete'])
        self.assertFalse(json.loads(stdout)['computation_complete'])
        self.assertEqual({row['status'] for row in result['scopes']}, {'INPUT_BLOCKED', 'COMPLETE'})
        self.assertEqual(self.read_database('SELECT COUNT(*) FROM local_jobs'), [(1,)])
        report = self.root / 'blocked_report.json'
        status, _, stderr = self.invoke(self.arguments('report', report))
        self.assertEqual((status, stderr), (2, ''))
        self.assertEqual(json.loads(report.read_text())['scopes'], result['scopes'])

    def test_all_input_blocked_submit_is_terminal_exit_two_with_no_jobs(self):
        missing_open = BASE + timedelta(hours=4, minutes=7)
        candles = [bar for bar in self.exports[0]['candles']
            if contracts.utc(bar['open_time_utc']) != missing_open]
        self.install(outcome_fixture(candles=candles))
        output = self.root / 'all_blocked.json'
        status, _, stderr = self.invoke(self.arguments('submit', output))
        self.assertEqual((status, stderr), (2, ''))
        receipt = json.loads(output.read_text())
        self.assertTrue(receipt['all_scopes_processed'])
        self.assertFalse(receipt['computation_complete'])
        self.assertEqual(receipt['scopes'][0]['status'], 'INPUT_BLOCKED')
        self.assertEqual(self.read_database('SELECT COUNT(*) FROM local_jobs'), [(0,)])

    def test_repeated_bounded_runs_restart_and_equal_uninterrupted_report(self):
        self.submit()
        reference_db = self.root / 'reference.sqlite'
        self.submit(name='reference_submit.json', database=reference_db)
        full_output = self.root / 'reference_run.json'
        status, _, stderr = self.invoke(self.arguments('run', full_output, database=reference_db,
            extra=['--candle-budget', '10000', '--scope-budget', '64']))
        self.assertEqual((status, stderr), (0, ''))
        full = json.loads(full_output.read_text())
        consumed = 0
        for attempt in range(8):
            output = self.root / f'restarted_run_{attempt}.json'
            status, _, stderr = self.invoke(self.arguments('run', output,
                extra=['--candle-budget', '1', '--entry-budget', '1', '--batch-size', '1']))
            self.assertEqual((status, stderr), (0, ''))
            current = json.loads(output.read_text())
            self.assertLessEqual(current['candle_evaluations_this_run'], 1)
            consumed += current['candle_evaluations_this_run']
            if current['all_scopes_processed']:
                break
        else:
            self.fail('Five immediate first-touch outcomes failed to complete within eight bounded invocations')
        self.assertEqual(consumed, 5)
        self.assertEqual(current['scopes'], full['scopes'])
        # Per-invocation receipts bind their own work counts. Compare durable
        # reports to establish outcome parity across restart strategies.
        durable = []
        for number, database in enumerate((self.db, reference_db)):
            output = self.root / f'durable_report_{number}.json'
            status, _, stderr = self.invoke(self.arguments('report', output, database=database))
            self.assertEqual((status, stderr), (0, ''))
            durable.append(json.loads(output.read_text()))
        self.assertEqual(durable[0], durable[1])
        after = self.root / 'already_finished.json'
        status, _, stderr = self.invoke(self.arguments('run', after, extra=['--candle-budget', '1']))
        self.assertEqual((status, stderr), (0, ''))
        self.assertEqual(json.loads(after.read_text())['candle_evaluations_this_run'], 0)

    def test_missing_database_run_and_report_do_not_create_database_or_output(self):
        self.plan_id = '0' * 64
        for command in ('run', 'report'):
            with self.subTest(command=command):
                output = self.root / f'missing_db_{command}.json'
                with patch('sqlite3.connect', side_effect=AssertionError('Missing DB must not be opened')):
                    status, _, stderr = self.invoke(self.arguments(command, output))
                self.assertEqual(status, 1)
                self.assertIn('EXISTING_COHORT_DATABASE_REQUIRED', stderr)
                self.assertFalse(self.db.exists())
                self.assertFalse(output.exists())

    def test_strict_json_duplicate_nonfinite_and_invalid_encoding_never_open_sqlite(self):
        invalid = (b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":Infinity}', b'{"x":1e999}', b'\xff', b'{')
        for path in (self.declaration_path, self.anchor_path, self.parts[1]):
            original = path.read_bytes()
            for number, bad in enumerate(invalid):
                with self.subTest(path=path.name, case=number):
                    path.write_bytes(bad)
                    output = self.root / f'bad_json_{path.stem}_{number}.json'
                    with patch('sqlite3.connect', side_effect=AssertionError('Bad JSON must precede SQLite')):
                        status, _, stderr = self.invoke(self.arguments('submit', output))
                    self.assertEqual(status, 1)
                    self.assertTrue(stderr)
                    self.assertFalse(self.db.exists())
                    self.assertFalse(output.exists())
            path.write_bytes(original)

    def test_existing_output_is_never_overwritten_for_any_command(self):
        self.submit()
        for command in ('submit', 'run', 'report'):
            with self.subTest(command=command):
                output = self.root / f'existing_{command}.json'
                output.write_bytes(b'prior evidence\n')
                with patch('sqlite3.connect', side_effect=AssertionError('Output rejection must precede SQLite')):
                    status, _, stderr = self.invoke(self.arguments(command, output))
                self.assertEqual(status, 1)
                self.assertIn('OUTPUT_ALREADY_EXISTS', stderr)
                self.assertEqual(output.read_bytes(), b'prior evidence\n')

    def test_output_symlink_is_not_followed_even_when_target_does_not_exist(self):
        target = self.root / 'missing_target.json'
        output = self.root / 'output_link.json'
        output.symlink_to(target)
        with patch('sqlite3.connect', side_effect=AssertionError('No DB for output symlink')):
            status, _, stderr = self.invoke(self.arguments('submit', output))
        self.assertEqual(status, 1)
        self.assertIn('OUTPUT_ALREADY_EXISTS', stderr)
        self.assertFalse(self.db.exists())
        self.assertFalse(target.exists())
        self.assertTrue(output.is_symlink())

    def test_database_output_and_input_database_collisions_preserve_inputs(self):
        variants = [(self.db, self.db)]
        variants += [(path, self.root / f'input_db_{number}.json')
            for number, path in enumerate((self.declaration_path, self.anchor_path, *self.parts))]
        variants += [(self.db, path) for path in (self.declaration_path, self.anchor_path, *self.parts)]
        original = {path: path.read_bytes() for path in (self.declaration_path, self.anchor_path, *self.parts)}
        for database, output in variants:
            with self.subTest(database=database.name, output=output.name):
                with patch('sqlite3.connect', side_effect=AssertionError('Collision must precede SQLite')):
                    status, _, stderr = self.invoke(self.arguments('submit', output, database=database))
                self.assertEqual(status, 1)
                self.assertTrue('PATH_COLLISION' in stderr or 'OUTPUT_ALREADY_EXISTS' in stderr)
                self.assertFalse(self.db.exists())
                for path, data in original.items():
                    self.assertEqual(path.read_bytes(), data)
                if output not in original:
                    self.assertFalse(output.exists())

    def test_input_database_symlink_alias_is_rejected(self):
        alias = self.root / 'input_alias.sqlite'
        alias.symlink_to(self.parts[1])
        original = self.parts[1].read_bytes()
        output = self.root / 'alias.json'
        with patch('sqlite3.connect', side_effect=AssertionError('Resolved input collision must precede SQLite')):
            status, _, stderr = self.invoke(self.arguments('submit', output, database=alias))
        self.assertEqual(status, 1)
        self.assertIn('PATH_COLLISION', stderr)
        self.assertEqual(self.parts[1].read_bytes(), original)
        self.assertFalse(output.exists())

    def test_missing_output_directory_rejected_before_database_creation(self):
        output = self.root / 'not_created' / 'receipt.json'
        with patch('sqlite3.connect', side_effect=AssertionError('No DB before output path validity')):
            status, _, stderr = self.invoke(self.arguments('submit', output))
        self.assertEqual(status, 1)
        self.assertIn('OUTPUT_DIRECTORY_MUST_EXIST', stderr)
        self.assertFalse(self.db.exists())
        self.assertFalse(output.parent.exists())

    def test_invalid_budgets_do_not_create_jobs_or_change_schedule(self):
        self.submit()
        before = self.read_database('SELECT * FROM cohort_schedule')
        for option, values in (('--scope-budget', ('0', '-1', '65')),
                ('--candle-budget', ('0', '-1', '100000001')),
                ('--entry-budget', ('0', '-1')), ('--batch-size', ('0', '-1', '100001'))):
            for value in values:
                with self.subTest(option=option, value=value):
                    output = self.root / f'invalid_{option[2:]}_{value}.json'
                    status, _, stderr = self.invoke(self.arguments('run', output, extra=[option, value]))
                    self.assertEqual(status, 1)
                    self.assertTrue(stderr)
                    self.assertFalse(output.exists())
                    self.assertEqual(self.read_database('SELECT COUNT(*) FROM local_jobs'), [(0,)])
                    self.assertEqual(self.read_database('SELECT * FROM cohort_schedule'), before)

    def test_unknown_plan_reports_error_without_creating_output_or_jobs(self):
        self.submit()
        for command in ('run', 'report'):
            output = self.root / f'unknown_{command}.json'
            status, _, stderr = self.invoke(self.arguments(command, output, plan_id='f' * 64))
            self.assertEqual(status, 1)
            self.assertIn('UNKNOWN_COHORT_PLAN', stderr)
            self.assertFalse(output.exists())
        self.assertEqual(self.read_database('SELECT COUNT(*) FROM local_jobs'), [(0,)])

    def test_foreign_sqlite_run_and_report_leave_file_and_schema_unchanged(self):
        self.plan_id = '0' * 64
        connection = sqlite3.connect(self.db)
        connection.execute('CREATE TABLE unrelated(value TEXT)')
        connection.execute("INSERT INTO unrelated VALUES('preserved')")
        connection.commit()
        connection.close()
        before = self.db.read_bytes()
        schema = self.read_database('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY name')
        for command in ('run', 'report'):
            with self.subTest(command=command):
                output = self.root / f'foreign_{command}.json'
                status, _, stderr = self.invoke(self.arguments(command, output))
                self.assertEqual(status, 1)
                self.assertIn('EXISTING_COHORT_DATABASE_REQUIRED', stderr)
                self.assertFalse(output.exists())
                self.assertEqual(self.db.read_bytes(), before)
                self.assertEqual(self.read_database('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY name'), schema)
                self.assertEqual(self.read_database('SELECT value FROM unrelated'), [('preserved',)])

    def test_malformed_database_run_and_report_preserve_original_bytes(self):
        self.plan_id = '0' * 64
        data = b'This is a pre-existing file, not SQLite.\n'
        self.db.write_bytes(data)
        for command in ('run', 'report'):
            with self.subTest(command=command):
                output = self.root / f'malformed_{command}.json'
                status, _, stderr = self.invoke(self.arguments(command, output))
                self.assertEqual(status, 1)
                self.assertTrue(stderr)
                self.assertFalse(output.exists())
                self.assertEqual(self.db.read_bytes(), data)

    def test_future_cohort_schema_refused_before_bootstrap_and_without_changes(self):
        self.plan_id = '0' * 64
        connection = sqlite3.connect(self.db)
        connection.execute('CREATE TABLE cohort_store_version(singleton INTEGER,version TEXT)')
        connection.execute("INSERT INTO cohort_store_version VALUES(1,'future-global-cohort-v99')")
        connection.execute('CREATE TABLE cohort_plans(plan_id TEXT)')
        connection.execute('INSERT INTO cohort_plans VALUES(?)', (self.plan_id,))
        connection.commit()
        connection.close()
        before = self.db.read_bytes()
        for command in ('run', 'report'):
            with self.subTest(command=command):
                output = self.root / f'future_{command}.json'
                status, _, stderr = self.invoke(self.arguments(command, output))
                self.assertEqual(status, 1)
                self.assertIn('UNSUPPORTED_COHORT_DATABASE_VERSION', stderr)
                self.assertFalse(output.exists())
                self.assertEqual(self.db.read_bytes(), before)

    def test_memory_database_refused_for_all_commands_before_opening_connection(self):
        self.plan_id = '0' * 64
        for command in ('submit', 'run', 'report'):
            with self.subTest(command=command):
                output = self.root / f'memory_{command}.json'
                with patch('sqlite3.connect', side_effect=AssertionError('CLI requires durable local storage')):
                    status, _, stderr = self.invoke(self.arguments(command, output, database=Path(':memory:')))
                self.assertEqual(status, 1)
                self.assertIn('PERSISTENT_COHORT_DATABASE_REQUIRED', stderr)
                self.assertFalse(output.exists())

    def test_submit_run_and_report_use_no_external_database_or_network(self):
        targets = ('psycopg.connect', 'socket.socket', 'socket.create_connection',
            'requests.sessions.Session.request')
        with ExitStack() as stack:
            mocks = [stack.enter_context(patch(target, side_effect=AssertionError('CLI attempted external I/O')))
                for target in targets]
            self.submit()
            for command in ('run', 'report'):
                status, _, stderr = self.invoke(self.arguments(command, self.root / f'local_{command}.json',
                    extra=['--candle-budget', '10000'] if command == 'run' else []))
                self.assertEqual((status, stderr), (0, ''))
            for mocked in mocks:
                mocked.assert_not_called()


if __name__ == '__main__':
    unittest.main()
