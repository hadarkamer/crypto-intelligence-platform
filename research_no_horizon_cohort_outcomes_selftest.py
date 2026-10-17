"""Global outcome intake tests using genuine anchored capture fixtures.

The prices and outcomes below are synthetic test data, never market evidence.
Source captures, parent membership and manifest proofs use their real builders.
"""
from contextlib import ExitStack
from copy import deepcopy
from datetime import timedelta
import json
import unittest
from unittest.mock import Mock, patch

import research_no_horizon_cohort as cohort
import research_no_horizon_cohort_outcomes as outcomes
import research_no_horizon_contract as contracts
import research_no_horizon_gate as gate
import research_no_horizon_manifest as manifest
import research_no_horizon_manifest_sql as manifest_sql
import research_no_horizon_replay as replay
import research_no_horizon_source as source
from research_no_horizon_cohort_coverage_selftest import cohort_fixture, cross_part_rows
from research_no_horizon_experiment import _scopes
from research_no_horizon_manifest_selftest import manifest_fixture
from research_no_horizon_parent_coverage_selftest import scope, source_row
from research_watch_score_capture_selftest import BASE


def _entry_time(value):
    decision = contracts.utc(value)
    minute = decision.replace(second=0, microsecond=0)
    return minute if minute == decision else minute + timedelta(minutes=1)


def _seal_exports(declaration, exports):
    """Rebuild raw-byte proofs after changing synthetic prices or captures."""
    declaration = cohort.normalize_declaration(declaration)
    parts, payloads = [], []
    for ordinal, export in enumerate(exports):
        part = declaration['parts'][ordinal]
        child, sources, candles = manifest_fixture(export)
        child['receipt'].update(parent_snapshot_mode=manifest_sql.PARENT_SNAPSHOT_MODE,
            query_identity='no-horizon-pinned-parent-manifest-select-v2',
            serializer='POSTGRESQL_JSONB_TEXT_UTF8_V1', serializer_timezone='UTC',
            database_writes=False, mvcc_snapshot='123:125:',
            extracted_at_utc=declaration['cutoff_utc'], source_cap=part['source_row_limit'],
            candle_cap=part['max_candle_rows'], page_size=part['page_size'])
        child['receipt'].pop('query_sha256')
        for entry, chunk in zip(child['source_entries'], sources):
            entry['btc_parent_payload_text'] = contracts.canonical(json.loads(chunk['payload_text'])['btc_parent'])
        parts.append({'ordinal':ordinal, 'manifest':child})
        payloads.append((sources, candles))
    raw = {'anchor_version':cohort.ANCHOR_VERSION,
        'declaration_sha256':contracts.digest(declaration), 'parts':parts,
        'extraction_receipt':{'transaction_mode':'SINGLE_STATEMENT_READ_ONLY',
            'consistent_read':'ONE_STATEMENT_SNAPSHOT', 'read_only':'on',
            'mvcc_snapshot':'123:125:', 'extracted_at_utc':declaration['cutoff_utc'],
            'query_identity':'no-horizon-global-part-anchor-select-v1',
            'serializer':'POSTGRESQL_JSONB_TEXT_UTF8_V1', 'serializer_timezone':'UTC',
            'database_writes':False}}
    anchor = cohort.seal_anchor(declaration, raw)
    assembled = [manifest.assemble_export(part['manifest'], *payloads[ordinal])
        for ordinal, part in enumerate(anchor['parts'])]
    return declaration, anchor, assembled


def outcome_fixture(rows=None, *, scopes=None, gate_policy=None, mode='mixed',
                    candles=None, part_candles=None):
    """One five-parent cohort with a later, favorable duplicate of parent three.

    ``mixed`` makes the globally selected watch:3 lose and watch:4 win;
    ``success``, ``open`` and ``ambiguous`` control all matched entry candles.
    Each part gets the exact common series suffix from its interval start.
    Caller overrides are resealed and therefore test semantic, not hash errors.
    """
    if mode not in ('mixed', 'success', 'open', 'ambiguous'):
        raise ValueError('Unsupported synthetic outcome mode')
    declaration, _, exports = cohort_fixture(rows, scopes=scopes, gate_policy=gate_policy)
    if candles is None:
        candles = []
        opened = _entry_time(declaration['source_start_utc'])
        cutoff = contracts.utc(declaration['cutoff_utc'])
        while opened < cutoff:
            candles.append({'route':source.ARCHIVE_ROUTE, 'symbol':'BTC',
                'open_time_utc':opened.isoformat(),
                'close_time_utc':(opened+timedelta(minutes=1,milliseconds=-1)).isoformat(),
                'open':100., 'high':100.01, 'low':99.99, 'close':100.})
            opened += timedelta(minutes=1)
        by_open = {contracts.utc(bar['open_time_utc']):bar for bar in candles}
        direction = _scopes(declaration['scopes'])[0]['analysis_direction']
        width = max(item['threshold_pct'] for item in declaration['scopes']) + .1
        for export in exports:
            for row in export['source_rows']:
                bar = by_open[_entry_time(row['intake']['usable_from_utc'])]
                if mode == 'open':
                    continue
                if mode == 'ambiguous':
                    bar.update(high=100.+width, low=100.-width)
                else:
                    success = mode == 'success' or row['intake']['snapshot_set_id'] != 3
                    up = (direction == 'LONG') == success
                    bar.update(high=100.+width if up else 100.01,
                               low=99.99 if up else 100.-width)
    for ordinal, export in enumerate(exports):
        start = _entry_time(declaration['parts'][ordinal]['source_start_utc'])
        export['candles'] = deepcopy(part_candles[ordinal] if part_candles is not None
            else [bar for bar in candles if contracts.utc(bar['open_time_utc']) >= start])
    return _seal_exports(declaration, exports)


class CohortOutcomePreparationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original = outcome_fixture()

    def setUp(self):
        self.declaration, self.anchor, self.exports = deepcopy(self.original)

    def prepare(self, loader=None):
        return outcomes.prepare_cohort_submission(self.declaration, self.anchor,
            loader or (lambda ordinal:self.exports[ordinal]))

    def snapshot(self, prepared, ordinal=0):
        plan = prepared.payload['scope_plans'][ordinal]
        return {**plan['snapshot_metadata'], 'candles':prepared.payload['candles']}

    def test_complete_population_is_selected_globally_without_outcome_or_db_calls(self):
        forbidden = ('research_no_horizon_first_touch.initialize',
            'research_no_horizon_first_touch.advance', 'research_no_horizon_gate.evaluate_gate',
            'sqlite3.connect', 'socket.create_connection', 'requests.sessions.Session.request')
        with ExitStack() as stack:
            for name in forbidden:
                stack.enter_context(patch(name, side_effect=AssertionError('Pure preparation: '+name)))
            prepared = self.prepare()
        receipt = prepared.payload['coverage_receipt']
        plan = prepared.payload['scope_plans'][0]
        self.assertEqual(receipt['accepted_source_rows'], 6)
        self.assertEqual(receipt['scopes'][0]['source_counts']['MATCH'], 6)
        self.assertEqual(len(plan['representatives']), 5)
        self.assertEqual(len(plan['snapshot_metadata']['opportunities']), 5)
        self.assertEqual(len({row['btc_parent_movement_id'] for row in plan['representatives']}), 5)
        self.assertFalse(receipt['outcomes_evaluated'])
        self.assertEqual(receipt['outcome_trials_executed'], 0)
        for field in ('runtime_authorized', 'telegram_authorized', 'trading_authorized'):
            self.assertIs(receipt[field], False)

    def test_earliest_loser_is_never_replaced_by_later_same_parent_winner(self):
        prepared = self.prepare()
        snapshot = self.snapshot(prepared)
        ids = {item['contract']['entry_id'] for item in snapshot['opportunities']}
        self.assertIn('watch:3:BTC:SHORT', ids)
        self.assertNotIn('watch:4:BTC:SHORT', ids)
        report = replay.replay_snapshot(snapshot)
        self.assertEqual((report['status_counts']['SUCCESS'], report['status_counts']['FAILURE']), (4,1))
        self.assertEqual(report['gate']['selected_parents'], 5)
        later = source.build_snapshot(self.exports[1], 'FUTURES_CVD_TOTAL_65', 'SHORT', 'BTC', .25)
        later_result = replay.replay_snapshot(later)
        status = {item['entry_id']:item['outcome']['status'] for item in later_result['outcomes']}
        self.assertEqual(status['watch:4:BTC:SHORT'], 'SUCCESS')

    def test_deterministic_plan_does_not_mutate_or_alias_inputs(self):
        before = deepcopy((self.declaration, self.anchor, self.exports))
        loader = Mock(side_effect=lambda ordinal:self.exports[ordinal])
        first = self.prepare(loader)
        second = self.prepare()
        self.assertEqual([call.args for call in loader.call_args_list], [(0,), (1,)])
        self.assertEqual(first, second)
        self.assertEqual((self.declaration, self.anchor, self.exports), before)
        self.assertIs(outcomes.validate_prepared(first), first)
        self.exports[0]['candles'][0]['open'] = 999.
        self.declaration['cohort_key'] = 'caller-mutated-later'
        self.assertEqual(first, second)
        self.assertIs(outcomes.validate_prepared(first), first)

    def test_exact_minute_decision_enters_that_minute_in_normalized_direction(self):
        prepared = self.prepare()
        plan = prepared.payload['scope_plans'][0]
        event = next(row for row in plan['snapshot_metadata']['opportunities']
            if row['contract']['entry_id'] == 'watch:1:BTC:SHORT')
        contract = contracts.make_contract(**event['contract'])
        self.assertEqual(contract['entry_time_utc'], (BASE+timedelta(minutes=7)).isoformat())
        self.assertEqual(contract['entry_time_utc'], contract['decision_time_utc'])
        self.assertEqual(contract['initial_signal_to_entry_seconds'], 0)
        self.assertEqual(contract['reference_price'], 100.)
        self.assertEqual(contract['direction'], plan['scope']['analysis_direction'])

    def test_midminute_decision_uses_next_exact_open_not_earlier_minute(self):
        rows = cross_part_rows()
        rows[0] = rows[0][:2]
        self.declaration, self.anchor, self.exports = outcome_fixture(rows)
        prepared = self.prepare()
        event = next(row for row in self.snapshot(prepared)['opportunities']
            if row['contract']['entry_id'] == 'watch:4:BTC:SHORT')
        contract = contracts.make_contract(**event['contract'])
        self.assertEqual(contract['entry_time_utc'], (BASE+timedelta(hours=4,minutes=9)).isoformat())
        self.assertEqual(contract['initial_signal_to_entry_seconds'], 30)

    def test_insufficient_population_stays_a_valid_descriptive_plan(self):
        rows = cross_part_rows(); rows[1] = []
        self.declaration, self.anchor, self.exports = outcome_fixture(rows)
        prepared = self.prepare()
        self.assertEqual(prepared.payload['coverage_receipt']['scopes'][0]['coverage_status'], 'INSUFFICIENT')
        self.assertEqual(len(self.snapshot(prepared)['opportunities']), 3)
        self.assertIsNone(prepared.payload['scope_plans'][0]['input_error'])
        result = replay.replay_snapshot(self.snapshot(prepared))
        self.assertFalse(result['gate']['experimental_eligible'])
        self.assertEqual(result['gate']['selected_parents'], 3)

    def test_empty_population_and_no_match_scope_remain_explicit(self):
        self.declaration, self.anchor, self.exports = outcome_fixture([[],[]])
        empty = self.prepare()
        self.assertEqual(len(empty.payload['scope_plans']), 1)
        self.assertEqual(self.snapshot(empty)['opportunities'], [])
        self.assertIsNone(empty.payload['scope_plans'][0]['input_error'])
        self.declaration, self.anchor, self.exports = outcome_fixture(scopes=[scope(), scope(direction='LONG')])
        prepared = self.prepare()
        rows = {plan['scope']['base_direction']:plan for plan in prepared.payload['scope_plans']}
        self.assertEqual(len(rows['SHORT']['snapshot_metadata']['opportunities']), 5)
        self.assertEqual(rows['LONG']['snapshot_metadata']['opportunities'], [])
        self.assertIsNone(rows['LONG']['input_error'])

    def test_unknown_feature_or_source_in_any_part_blocks_whole_submission(self):
        for invalid_source in (False, True):
            with self.subTest(invalid_source=invalid_source):
                rows = cross_part_rows()
                if invalid_source:
                    rows[1][1]['stored_source'] = None
                else:
                    rows[1][1] = source_row(parent_offset=3, snapshot_set_id=5, unavailable=True)
                self.declaration, self.anchor, self.exports = outcome_fixture(rows,
                    scopes=[scope(), scope(direction='LONG')])
                with self.assertRaises(outcomes.CohortInputBlocked) as caught:
                    self.prepare()
                receipt = caught.exception.receipt
                self.assertEqual(receipt['accepted_source_rows'], 6)
                self.assertTrue(any(item['coverage_status']=='BLOCKED' for item in receipt['scopes']))
                self.assertEqual(receipt['outcome_trials_executed'], 0)

    def test_invalid_shared_parent_blocks_before_any_entry_can_replace_it(self):
        rows = cross_part_rows()
        invalid = deepcopy(rows[0][2]['btc_parent'])
        invalid['confirmed_at_utc'] = BASE+timedelta(hours=4,minutes=10)
        rows[0][2]['btc_parent'] = deepcopy(invalid)
        rows[1][0]['btc_parent'] = deepcopy(invalid)
        self.declaration, self.anchor, self.exports = outcome_fixture(rows)
        with self.assertRaises(outcomes.CohortInputBlocked) as caught:
            self.prepare()
        item = caught.exception.receipt['scopes'][0]
        self.assertEqual(item['source_counts']['MATCH'], 6)
        self.assertEqual(item['unverified_matched_rows'], 2)
        self.assertEqual(item['representatives'], [])

    def test_missing_selected_entry_preserves_identity_without_later_replacement(self):
        missing = (BASE+timedelta(hours=4,minutes=7)).isoformat()
        candles = [bar for bar in self.exports[0]['candles'] if bar['open_time_utc'] != missing]
        self.declaration, self.anchor, self.exports = outcome_fixture(candles=candles)
        prepared = self.prepare()
        plan = prepared.payload['scope_plans'][0]
        self.assertEqual(plan['input_error'], 'MISSING_SELECTED_ENTRY_OPEN')
        self.assertEqual(plan['missing_entry_ids'], ['watch:3:BTC:SHORT'])
        self.assertEqual(plan['missing_entry_locators'][0]['part_ordinal'], 0)
        self.assertIsNone(plan['snapshot_metadata'])
        ids = {row['entry_id'] for row in plan['representatives']}
        self.assertIn('watch:3:BTC:SHORT', ids)
        self.assertNotIn('watch:4:BTC:SHORT', ids)
        self.assertEqual(len(ids), 5)
        outcomes.validate_prepared(prepared)

    def test_later_part_price_change_or_gap_cannot_be_combined_with_first_part(self):
        for mutation in ('changed_price', 'missing_bar', 'extra_bar'):
            with self.subTest(mutation=mutation):
                exports = deepcopy(self.original[2])
                if mutation == 'changed_price':
                    exports[1]['candles'][3]['high'] += 1
                elif mutation == 'missing_bar':
                    del exports[1]['candles'][3]
                else:
                    del exports[0]['candles'][250]
                self.declaration, self.anchor, self.exports = _seal_exports(self.original[0], exports)
                with self.assertRaisesRegex(ValueError, 'COHORT_PRICE_SUFFIX_MISMATCH'):
                    self.prepare()

    def test_shared_path_gap_is_retained_for_outcome_missing_data(self):
        declaration, _, exports = outcome_fixture(mode='open')
        absent = (BASE+timedelta(hours=4,minutes=10)).isoformat()
        for export in exports:
            export['candles'] = [bar for bar in export['candles'] if bar['open_time_utc'] != absent]
        self.declaration, self.anchor, self.exports = _seal_exports(declaration, exports)
        prepared = self.prepare()
        self.assertIsNone(prepared.payload['scope_plans'][0]['input_error'])
        result = replay.replay_snapshot(self.snapshot(prepared))
        self.assertGreater(result['status_counts']['DATA_MISSING'], 0)
        self.assertFalse(result['computation_complete'])
        self.assertFalse(result['gate']['experimental_eligible'])

    def test_invalid_ohlc_is_rejected_even_with_consistent_resealed_part_proofs(self):
        exports = deepcopy(self.exports)
        # The earliest bar occurs before all opportunities, but is still validated.
        exports[0]['candles'][0]['high'] = 1.
        self.declaration, self.anchor, self.exports = _seal_exports(self.declaration, exports)
        with self.assertRaises(ValueError):
            self.prepare()

    def test_global_identity_changes_with_declaration_and_versioned_gate_policy(self):
        first = self.prepare()
        changed = deepcopy(self.declaration)
        changed['cohort_key'] = 'another-declared-cohort'
        self.declaration, self.anchor, self.exports = _seal_exports(changed, self.exports)
        second = self.prepare()
        self.assertNotEqual(first.plan_id, second.plan_id)
        self.assertNotEqual(first.payload['cohort_id'], second.payload['cohort_id'])
        policy = gate.make_policy(policy_version='selftest-six-parent-policy-v1', minimum_waves=6)
        self.declaration, self.anchor, self.exports = outcome_fixture(gate_policy=policy)
        third = self.prepare()
        self.assertNotEqual(first.plan_id, third.plan_id)
        self.assertEqual(third.identity['gate_policy'], policy)
        self.assertEqual(self.snapshot(third)['gate_policy'], policy)
        self.assertEqual(third.payload['coverage_receipt']['scopes'][0]['coverage_status'], 'INSUFFICIENT')

    def test_prepared_payload_identity_and_implementation_mutations_fail_validation(self):
        prepared = self.prepare()
        for change in ('price', 'representative', 'policy', 'versions'):
            with self.subTest(change=change):
                modified = deepcopy(prepared)
                if change == 'price':
                    modified.payload['candles'][0]['open'] += 1
                elif change == 'representative':
                    modified.payload['scope_plans'][0]['representatives'].pop()
                elif change == 'policy':
                    modified.identity['gate_policy']['minimum_waves'] += 1
                else:
                    modified.identity['versions']['preparation_version'] = 'changed'
                with self.assertRaises(ValueError):
                    outcomes.validate_prepared(modified)

    def test_global_expansion_and_prepared_payload_budgets_are_enforced(self):
        self.declaration, self.anchor, self.exports = outcome_fixture(scopes=[scope(), scope(threshold=.5)])
        prepared = self.prepare()
        sizes = [len(contracts.canonical(self.snapshot(prepared, ordinal)).encode('utf-8'))
            for ordinal in range(2)]
        limit = sum(sizes)-1
        self.assertGreater(limit, max(sizes))
        with patch.object(outcomes, 'MAX_EXPANDED_BYTES', limit):
            with self.assertRaisesRegex(ValueError, 'GLOBAL_OUTCOME_EXPANDED_BYTE_LIMIT_EXCEEDED'):
                self.prepare()
        with patch.object(outcomes, 'MAX_PREPARED_BYTES', prepared.identity['payload_bytes']-1):
            with self.assertRaisesRegex(ValueError, 'GLOBAL_OUTCOME_SERIALIZED_BYTE_LIMIT_EXCEEDED'):
                self.prepare()


if __name__ == '__main__':
    unittest.main()
