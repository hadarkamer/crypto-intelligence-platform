"""The rolling 24h panel may advance while every input to the current map stays fixed."""
from copy import deepcopy
from unittest import TestCase

from model1_execution import StageFailure
from modern_map_basic import prove_observations
from modern_map_basic_numbers import REALIZED_24H, build_basic_snapshot, validate_basic_snapshot
from test_fresh_range_evidence import inputs


class RollingLiquidationTests(TestCase):
    def setUp(self):
        self.opening, self.args = inputs()
        self.closing = self.args['end_observation']

    def build(self):
        return build_basic_snapshot(self.opening, **self.args, require_fresh_evidence=True)

    def test_stable_column_and_different_coherent_counters(self):
        baseline = self.build()
        # Deliberately change the size by a large amount. Source membership,
        # printed precision and same-column proof decide; there is no drift cap.
        updated = dict(raw_realized_total='200.0K', raw_realized_longs='145.0K', raw_realized_shorts='55.0K')
        after = dict(self.opening, **updated)
        prove_observations(self.opening, after, base_only=True)
        with self.assertRaises(StageFailure):
            prove_observations(self.opening, after)  # preserve the enriched route
        self.closing.update(updated)
        result = self.build()
        expected = deepcopy(baseline)
        expected['source_evidence']['closing'].update(updated)
        self.assertEqual(result, expected)  # includes every price, amount and region
        self.assertEqual(validate_basic_snapshot(result, require_fresh_evidence=True), result)
        self.assertTrue(all(k not in baseline['source_evidence']['closing'] for k in REALIZED_24H))

    def test_equal_counters_do_not_authorize_a_neighboring_column(self):
        self.closing['column_time'] = self.args['column_time_after'] = '2026-11-26T14:55:00Z'
        with self.assertRaises(StageFailure):
            self.build()

    def test_changed_or_ambiguous_map_data_still_fails(self):
        after = dict(self.opening, raw_realized_shorts='35.1K')
        for key, value in [('raw_total','101M'), ('raw_updated_at','14:59'),
                           ('sidebar_levels',self.opening['sidebar_levels'][:1]),
                           ('ambiguous_fields',['raw_realized_shorts'])]:
            with self.subTest(field=key), self.assertRaises(StageFailure):
                prove_observations(self.opening, dict(after, **{key:value}), base_only=True)

    def test_incomplete_or_inconsistent_counter_update_fails(self):
        for short in (None, 'missing', '500.0K'):
            self.closing['raw_realized_shorts'] = short
            with self.subTest(short=short), self.assertRaises(StageFailure):
                self.build()
        self.closing.update(raw_realized_total='101.0K',raw_realized_shorts='36.0K')
        del self.args['cursor_proof']
        with self.assertRaises(StageFailure):
            self.build()
