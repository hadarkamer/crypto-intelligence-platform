"""PostgreSQL admission boundaries without a database or network dependency.

These are synthetic, genuinely sealed capture fixtures, not market evidence.
The adapter must finish every source check before delegating persistence, and
must never inspect outcomes while deciding whether a source cohort is valid.
"""
from contextlib import ExitStack
from copy import deepcopy
import unittest
from unittest.mock import Mock, patch

import research_no_horizon_cohort_outcomes as preparation
import research_no_horizon_postgres_store as postgres
from research_no_horizon_cohort_coverage_selftest import cross_part_rows
from research_no_horizon_cohort_outcomes_selftest import outcome_fixture


class PostgresAdmissionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original = outcome_fixture()

    def setUp(self):
        self.declaration, self.anchor, self.exports = deepcopy(self.original)
        # Bypass connection setup deliberately: any accidental SQL during
        # source admission fails, rather than being hidden by a database mock.
        self.store = object.__new__(postgres.PostgresCohortStore)
        self.store.connection = Mock()
        self.store.connection.info.transaction_status = 0
        self.store.connection.execute.side_effect = AssertionError('Admission touched SQL')
        self.store._submit_prepared = Mock(return_value='persisted-plan')

    def submit(self, loader=None):
        return self.store.submit_cohort(self.declaration, self.anchor,
            loader or (lambda ordinal: self.exports[ordinal]), cohort_key='frozen-cohort')

    def test_all_parts_and_global_representatives_admitted_without_outcome_calls(self):
        loader = Mock(side_effect=lambda ordinal: self.exports[ordinal])
        with ExitStack() as stack:
            for name in ('research_no_horizon_first_touch.initialize',
                         'research_no_horizon_first_touch.advance',
                         'research_no_horizon_gate.evaluate_gate',
                         'socket.create_connection'):
                stack.enter_context(patch(name, side_effect=AssertionError(name)))
            self.assertEqual(self.submit(loader), 'persisted-plan')
        self.assertEqual([call.args for call in loader.call_args_list], [(0,), (1,)])
        prepared = self.store._submit_prepared.call_args.args[0]
        self.assertEqual(self.store._submit_prepared.call_args.kwargs,
                         {'cohort_key': 'frozen-cohort', '_transaction_guard': None})
        preparation.validate_prepared(prepared)
        selected = {item['entry_id'] for item in prepared.payload['scope_plans'][0]['representatives']}
        self.assertEqual(len(selected), 5)
        self.assertIn('watch:3:BTC:SHORT', selected)
        self.assertNotIn('watch:4:BTC:SHORT', selected)
        self.assertEqual(prepared.payload['coverage_receipt']['outcome_trials_executed'], 0)
        self.store.connection.execute.assert_not_called()

    def test_missing_captured_source_in_last_part_never_delegates_a_partial_plan(self):
        rows = cross_part_rows()
        rows[1][1]['stored_source'] = None
        self.declaration, self.anchor, self.exports = outcome_fixture(rows)
        with self.assertRaises(preparation.CohortInputBlocked) as caught:
            self.submit()
        self.assertEqual(caught.exception.receipt['outcome_trials_executed'], 0)
        self.store._submit_prepared.assert_not_called()
        self.store.connection.execute.assert_not_called()

    def test_corrupt_final_part_cannot_persist_valid_prefix(self):
        self.exports[1]['source_rows'] = []
        loader = Mock(side_effect=lambda ordinal: self.exports[ordinal])
        with self.assertRaises(ValueError):
            self.submit(loader)
        self.assertEqual([call.args for call in loader.call_args_list], [(0,), (1,)])
        self.store._submit_prepared.assert_not_called()
        self.store.connection.execute.assert_not_called()

    def test_failed_loader_does_not_create_a_plan(self):
        def interrupted(ordinal):
            if ordinal:
                raise OSError('interrupted fixture read')
            return self.exports[ordinal]
        with self.assertRaisesRegex(OSError, 'interrupted'):
            self.submit(interrupted)
        self.store._submit_prepared.assert_not_called()
        self.store.connection.execute.assert_not_called()

    def test_caller_mutation_cannot_change_prepared_persistence_input(self):
        before = deepcopy((self.declaration, self.anchor, self.exports))
        self.submit()
        prepared = self.store._submit_prepared.call_args.args[0]
        frozen_payload = deepcopy(prepared.payload)
        self.assertEqual((self.declaration, self.anchor, self.exports), before)
        self.declaration['cohort_key'] = 'mutated'
        self.exports[0]['candles'][0]['open'] = 999
        self.assertEqual(prepared.payload, frozen_payload)
        preparation.validate_prepared(prepared)


if __name__ == '__main__':
    unittest.main()
