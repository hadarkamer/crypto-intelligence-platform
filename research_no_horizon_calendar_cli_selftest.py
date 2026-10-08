"""Calendar command boundaries: offline planning and explicit future registration."""
from contextlib import redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import research_no_horizon_calendar as calendar
import research_no_horizon_calendar_cli as cli
import research_no_horizon_contract as contracts
from research_no_horizon_cohort_coverage_selftest import cohort_fixture


class CalendarCLITests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        value = cohort_fixture()[0]
        value['declared_at_utc'] = value['source_start_utc']
        cls.declaration = value

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
        plan = calendar.build_plan(self.declaration, window_count=2)
        self.input.write_text(contracts.canonical(plan), encoding='utf-8')
        return plan

    def test_build_is_offline_and_writes_exact_plan(self):
        with patch.object(cli, '_connect', side_effect=AssertionError('database opened')) as connect:
            status, _, err = self.invoke(['build', '--declaration', str(self.input), '--windows', '2'])
        self.assertEqual((status, err), (0, ''))
        connect.assert_not_called()
        saved = json.loads(self.output.read_text())
        self.assertEqual(calendar.validate_plan(saved), saved)
        self.assertEqual(len(saved['windows']), 2)

    def test_bad_bounds_invalid_plan_and_output_collision_fail_before_database(self):
        with patch.object(cli, '_connect', side_effect=AssertionError('database opened')) as connect:
            self.assertEqual(self.invoke(['build', '--declaration', str(self.input), '--windows', '33'])[0], 1)
            self.input.write_text('{}')
            self.assertEqual(self.invoke(['register', '--plan', str(self.input)])[0], 1)
            self.output.write_text('preserve evidence')
            self.assertEqual(self.invoke(['register', '--plan', str(self.input)])[0], 1)
        connect.assert_not_called()
        self.assertEqual(self.output.read_text(), 'preserve evidence')

    def test_register_requires_explicit_destination_and_never_falls_back(self):
        self.save_plan()
        os.environ['DATABASE_URL'] = 'postgresql://do-not-use'
        with patch.object(cli, '_connect', side_effect=AssertionError('database opened')) as connect:
            self.assertEqual(self.invoke(['register', '--plan', str(self.input)])[0], 1)
        connect.assert_not_called()

    def test_register_validates_before_connect_and_exports_receipt(self):
        plan = self.save_plan()
        os.environ['RESEARCH_NO_HORIZON_DATABASE_URL'] = 'postgresql://research-only'
        receipt = {'plan_sha256': plan['plan_sha256'], 'registrations': []}
        backend = MagicMock()
        with patch.object(cli, '_connect', return_value=MagicMock()) as connect, \
             patch.object(cli.acquisition, 'schema_status', return_value={'schema_present': True}), \
             patch.object(cli.acquisition, 'AcquisitionStore', return_value=backend), \
             patch.object(cli.calendar, 'register_plan', return_value=receipt) as register:
            status, _, err = self.invoke(['register', '--plan', str(self.input)])
        self.assertEqual((status, err), (0, ''))
        connect.assert_called_once_with('postgresql://research-only')
        register.assert_called_once_with(backend, plan)
        self.assertEqual(json.loads(self.output.read_text()), receipt)

    def test_missing_schema_never_registers_or_installs(self):
        self.save_plan()
        os.environ['RESEARCH_NO_HORIZON_DATABASE_URL'] = 'configured'
        with patch.object(cli, '_connect', return_value=MagicMock()), \
             patch.object(cli.acquisition, 'schema_status', return_value={'schema_present': False}), \
             patch.object(cli.calendar, 'register_plan') as register:
            self.assertEqual(self.invoke(['register', '--plan', str(self.input)])[0], 1)
        register.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_connection_error_is_sanitized_and_does_not_create_success_receipt(self):
        self.save_plan()
        os.environ['RESEARCH_NO_HORIZON_DATABASE_URL'] = 'configured'
        with patch.object(cli, '_connect', side_effect=RuntimeError('postgresql://secret/private-data')):
            status, out, err = self.invoke(['register', '--plan', str(self.input)])
        self.assertEqual((status, out, err), (1, '', 'CALENDAR_FAILED: RuntimeError\n'))
        self.assertFalse(self.output.exists())


if __name__ == '__main__':
    unittest.main()
