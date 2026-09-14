"""Actual selected timeframe semantics and original liquidity predicate parity."""
from copy import deepcopy
from datetime import timedelta
import json
import unittest
from unittest.mock import patch

import research_ordered_question_catalog as original
import research_watch_scan_formula_timeframe as adapter
from research_watch_scan_formula_maxpain_selftest import observation as prior_observation

PREFIX = original.VERSION+':'


def observation(symbol='BTC'):
    obs = prior_observation(symbol)
    for row in obs['sources']['maxpain_operational_rows']:
        row.update(short_liquidation_amount=70, long_liquidation_amount=30)
    for slot in obs['maxpain_slots']:
        near, far = (70, 30) if slot['source_side'] == 'SHORT' else (30, 70)
        slot.update(near_amount=near, far_amount=far, near_share_pct=near)
    return obs


def source(obs, tf='12h'):
    return next(row for row in obs['sources']['maxpain_operational_rows'] if row['timeframe'] == tf)


def slot(obs, side='SHORT', tf='12h'):
    return next(item for item in obs['maxpain_slots'] if item['timeframe'] == tf and item['source_side'] == side)


def set_score(item, score):
    item['score'] = score
    item['components'] = dict.fromkeys(adapter.maxpain.ADDITIVE_COMPONENTS, 0)
    item['components']['directional_alignment'] = score


def amounts(obs, near, far, *, tf='12h', side='SHORT', share=None):
    row, item = source(obs, tf), slot(obs, side, tf)
    near_key = 'short_liquidation_amount' if side == 'SHORT' else 'long_liquidation_amount'
    far_key = 'long_liquidation_amount' if side == 'SHORT' else 'short_liquidation_amount'
    row.update({near_key: near, far_key: far})
    item.update(near_amount=near, far_amount=far,
        near_share_pct=share if share is not None else 100*near/(near+far) if near+far else None)


def decision(payload, suffix='LIQUIDITY_SUPPORTS', *, tf='12h', base='LONG'):
    return next(row for row in payload['evaluations'] if row['candidate_key'] == PREFIX+suffix
        and row['timeframe'] == tf and row['base_direction'] == base)


class TimeframeTests(unittest.TestCase):
    def test_34_new_original_definitions_only_and_complete_catalog_guard(self):
        previous, current = adapter.previous.catalog_records(), adapter.catalog_records()
        self.assertEqual((len(previous), len(current)), (298, 34))
        self.assertEqual(sum(row['orientation'] == 'INVERSE' for row in current), 17)
        self.assertTrue(all(row['supported'] and not row['unsupported_features'] for row in current))
        old = {row['candidate_key']: row for row in previous}
        for row in current:
            self.assertFalse(old[row['candidate_key']]['supported'])
            for key in ('candidate_key', 'definition', 'definition_sha256', 'orientation'):
                self.assertEqual(row[key], old[row['candidate_key']][key])
        self.assertFalse(any('COMBINED' in row['candidate_key'] for row in current))
        full = adapter.legacy.existing.candidate_catalog(include_extended=True)
        with patch.object(adapter.legacy.existing, 'candidate_catalog', return_value=full[:-1]):
            with self.assertRaisesRegex(ValueError, 'CATALOG_DRIFT'):
                adapter.catalog_records()

    def test_fixed476_coverage_and_only_actual_selected_base_is_applicable(self):
        result = adapter.evaluate_coin(observation())
        self.assertEqual(len(result['evaluations']), 476)
        expected = {(row['candidate_key'], tf, base) for row in adapter.catalog_records()
            for tf in adapter.TIMEFRAMES for base in adapter.DIRECTIONS}
        self.assertEqual({(row['candidate_key'], row['timeframe'], row['base_direction']) for row in result['evaluations']}, expected)
        self.assertEqual(sum(row['selection_status'] == 'SELECTED' for row in result['evaluations']), 238)
        for row in result['evaluations']:
            if row['base_direction'] == 'SHORT':
                self.assertEqual((row['selection_status'], row['match_status']), ('NOT_SELECTED', 'NOT_APPLICABLE'))

    def test_original_selected_event_liquidity_and_difference_predicates_are_identical(self):
        obs = observation('HYPE')
        result = adapter.evaluate_coin(obs)
        model_features = adapter.legacy.evaluate_coin(obs)['features_by_direction']['LONG']['features']
        for tf in adapter.TIMEFRAMES:
            own, other, row = slot(obs, tf=tf), slot(obs, 'LONG', tf), source(obs, tf)
            # Compare with the actual original feature builder; these test-only
            # event-shaped inputs are never persisted or used by the adapter.
            event = {'event_type': 'MAX_PAIN_ALERT', 'direction': 'LONG', 'source_side': 'SHORT',
                'current_price': row['current_price'], 'target_price': own['target_price'], 'score': own['score'],
                'engine_snapshot': {'alert_side': 'SHORT', 'near_amount': own['near_amount'],
                    'far_amount': own['far_amount'], 'near_share_pct': own['near_share_pct'], 'opposite_score': other['score']}}
            expected_features = {**model_features, **original.extended_features(event)}
            actual_features = result['timeframe_provenance'][tf]['features_by_direction']['LONG']['features']
            for key in (adapter.MAPPING, adapter.SHARE, adapter.ALIGNMENT, adapter.DIFFERENCE):
                self.assertEqual(actual_features[key], expected_features[key])
            for candidate in adapter.catalog_records():
                expected = adapter.legacy.evaluate_candidate(candidate['definition'], expected_features)
                actual = next(r for r in result['evaluations'] if r['candidate_key'] == candidate['candidate_key']
                    and r['timeframe'] == tf and r['base_direction'] == 'LONG')
                self.assertEqual({key: actual[key] for key in expected}, expected)

    def test_original_share_band_edges_and_alignment_thresholds(self):
        for value, band in ((0, '0_40'), (39.999, '0_40'), (40, '40_50'), (50, '50_60'),
                (59.999, '50_60'), (60, '60_70'), (70, '70_80'), (80, '80_101'), (100, '80_101')):
            obs = observation()
            amounts(obs, value, 100-value)
            result = adapter.evaluate_coin(obs)
            matches = [row for row in result['evaluations'] if row['base_direction'] == 'LONG' and row['timeframe'] == '12h'
                and row['candidate_key'].startswith(PREFIX+'LIQUIDITY_') and row['candidate_key'].split('_')[-1].isdigit()
                and row['match_status'] == 'MATCH']
            self.assertEqual([row['candidate_key'] for row in matches], [PREFIX+'LIQUIDITY_'+band])
            alignment = result['timeframe_provenance']['12h']['features_by_direction']['LONG']['features'][adapter.ALIGNMENT]
            self.assertEqual(alignment, 'SUPPORTS' if value >= 60 else 'OPPOSES' if value <= 40 else 'BALANCED')

    def test_exact_captured_share_is_retained_with_original_tolerance(self):
        obs = observation()
        amounts(obs, 60, 40, share=59.98)
        result = adapter.evaluate_coin(obs)
        features = result['timeframe_provenance']['12h']['features_by_direction']['LONG']['features']
        self.assertEqual(features[adapter.SHARE], 59.98)
        self.assertEqual(features[adapter.ALIGNMENT], 'BALANCED')
        self.assertEqual(decision(result, 'LIQUIDITY_50_60')['match_status'], 'MATCH')

    def test_selected_long_source_maps_to_price_short_with_correct_amounts(self):
        obs = observation()
        set_score(slot(obs, 'LONG'), 80)
        slot(obs, 'LONG')['selected'] = True
        slot(obs)['selected'] = False
        amounts(obs, 70, 30, side='LONG')
        result = adapter.evaluate_coin(obs)
        proof = result['timeframe_provenance']['12h']
        self.assertEqual((proof['selected_source_side'], proof['selected_base_direction']), ('LONG', 'SHORT'))
        self.assertEqual(decision(result, base='SHORT')['match_status'], 'MATCH')
        self.assertEqual(decision(result, base='LONG')['match_status'], 'NOT_APPLICABLE')
        self.assertEqual(proof['features_by_direction']['SHORT']['features'][adapter.DIFFERENCE], 10)

    def test_winner_is_highest_score_then_nearest_distance_then_long_exact_tie(self):
        obs = observation()
        # LONG is farther yet wins on score; closest-consensus is irrelevant.
        set_score(slot(obs, 'LONG'), 80)
        slot(obs, 'LONG')['selected'], slot(obs)['selected'] = True, False
        self.assertEqual(adapter.evaluate_coin(obs)['timeframe_provenance']['12h']['selected_source_side'], 'LONG')
        # Equal scores choose closer SHORT.
        set_score(slot(obs, 'LONG'), 70)
        slot(obs, 'LONG')['selected'], slot(obs)['selected'] = False, True
        self.assertEqual(adapter.evaluate_coin(obs)['timeframe_provenance']['12h']['selected_source_side'], 'SHORT')
        # Equal scores and exact equal distance choose LONG, unlike consensus.
        source(obs)['short_max_pain'] = slot(obs)['target_price'] = 102
        slot(obs, 'LONG')['selected'], slot(obs)['selected'] = True, False
        self.assertEqual(adapter.evaluate_coin(obs)['timeframe_provenance']['12h']['selected_source_side'], 'LONG')

    def test_unproven_selection_forces_unknown_even_with_known_failed_model_conjunct(self):
        obs = observation()
        obs['models']['spot_flow']['score'] = 0
        slot(obs, 'LONG')['selected'] = True
        result = adapter.evaluate_coin(obs)
        rows = [r for r in result['evaluations'] if r['timeframe'] == '12h']
        self.assertEqual(len(rows), 68)
        self.assertTrue(all(r['selection_status'] == 'UNKNOWN' and r['match_status'] == 'UNKNOWN' for r in rows))
        self.assertEqual(decision(result, 'spot_cvd_65_LIQUIDITY_SUPPORTS')['match_status'], 'UNKNOWN')
        self.assertEqual(decision(result, tf='24h')['match_status'], 'MATCH')

    def test_both_known_inactive_targets_are_not_applicable(self):
        obs = observation()
        source(obs).update(short_max_pain=None, long_max_pain=None)
        for side in adapter.DIRECTIONS:
            slot(obs, side).update(status='INACTIVE_TARGET', score=None, selected=False)
        result = adapter.evaluate_coin(obs)
        self.assertEqual(result['timeframe_provenance']['12h']['selection_status'], 'NO_ACTIVE_TARGET')
        self.assertTrue(all(row['match_status'] == 'NOT_APPLICABLE' for row in result['evaluations'] if row['timeframe'] == '12h'))

    def test_inactive_opposite_preserves_liquidity_but_not_score_difference(self):
        obs = observation()
        source(obs)['long_max_pain'] = None
        slot(obs, 'LONG').update(status='INACTIVE_TARGET', score=None, selected=False)
        result = adapter.evaluate_coin(obs)
        self.assertEqual(decision(result)['match_status'], 'MATCH')
        self.assertEqual(decision(result, 'selected_opposite_difference_POS')['match_status'], 'UNKNOWN')

    def test_missing_negative_raw_mismatch_and_zero_denominator_remain_unknown(self):
        for mutation in ('missing_raw', 'missing_slot', 'negative', 'wrong_share', 'wrong_amount', 'zero_total'):
            obs = observation()
            if mutation == 'missing_raw':
                source(obs)['short_liquidation_amount'] = None
                slot(obs)['near_amount'] = 0
            elif mutation == 'missing_slot':
                slot(obs).pop('near_amount')
            elif mutation == 'negative':
                amounts(obs, -1, 100)
            elif mutation == 'wrong_share':
                slot(obs)['near_share_pct'] = 99
            elif mutation == 'wrong_amount':
                slot(obs)['near_amount'] = 1
            else:
                amounts(obs, 0, 0)
            result = adapter.evaluate_coin(obs)
            self.assertEqual(decision(result)['match_status'], 'UNKNOWN', mutation)
            self.assertEqual(decision(result, 'selected_opposite_difference_POS')['match_status'], 'MATCH')
            self.assertEqual(decision(result, tf='24h')['match_status'], 'MATCH')

    def test_missing_liquidity_selected_low_model_remains_original_known_no_match(self):
        obs = observation()
        slot(obs).pop('near_amount')
        obs['models']['spot_flow']['score'] = 10
        result = adapter.evaluate_coin(obs)
        self.assertEqual(decision(result, 'spot_cvd_65_LIQUIDITY_SUPPORTS')['match_status'], 'NO_MATCH')
        self.assertEqual(decision(result)['match_status'], 'UNKNOWN')

    def test_local_source_clocks_and_missing_or_duplicate_slots_do_not_poison_other_timeframes(self):
        for mutation in ('future', 'source_error', 'missing_row', 'duplicate_row', 'missing_slot', 'duplicate_slot'):
            obs = observation()
            if mutation == 'future':
                source(obs)['price_fetched_at_utc'] = (obs['observed_at_utc']+timedelta(seconds=1)).isoformat()
            elif mutation == 'source_error':
                obs['source_time_errors'].append('12h/source_observed_at_utc:FUTURE_TIME')
            elif mutation == 'missing_row':
                obs['sources']['maxpain_operational_rows'].remove(source(obs))
            elif mutation == 'duplicate_row':
                obs['sources']['maxpain_operational_rows'].append(deepcopy(source(obs)))
            elif mutation == 'missing_slot':
                obs['maxpain_slots'].remove(slot(obs))
            else:
                obs['maxpain_slots'].append(deepcopy(slot(obs)))
            result = adapter.evaluate_coin(obs)
            self.assertEqual(decision(result)['match_status'], 'UNKNOWN', mutation)
            self.assertEqual(decision(result, tf='24h')['match_status'], 'MATCH')

    def test_original_item_validation_guards_and_unverified_quote(self):
        for mutation in ('components', 'consensus', 'cluster', 'quote', 'selected_type', 'target'):
            obs = observation()
            if mutation == 'components':
                slot(obs)['components']['relative_gap'] += 1
            elif mutation == 'consensus':
                slot(obs)['consensus_hits'] = 8
            elif mutation == 'cluster':
                slot(obs)['cluster_count'] = 8
            elif mutation == 'quote':
                source(obs)['price_pair'] = ''
            elif mutation == 'selected_type':
                slot(obs)['selected'] = 1
            else:
                slot(obs)['target_price'] = 99
            self.assertEqual(decision(adapter.evaluate_coin(obs))['match_status'], 'UNKNOWN', mutation)

    def test_zero_scores_are_known_and_inverse_uses_same_selected_features(self):
        obs = observation()
        for side in adapter.DIRECTIONS:
            set_score(slot(obs, side), 0)
        result = adapter.evaluate_coin(obs)
        self.assertEqual(decision(result, 'selected_opposite_difference_POS')['match_status'], 'NO_MATCH')
        inverse = next(row for row in adapter.catalog_records() if row['definition'].get('base_candidate_key') == PREFIX+'LIQUIDITY_SUPPORTS')
        normal = decision(result)
        inverted = next(row for row in result['evaluations'] if row['candidate_key'] == inverse['candidate_key']
            and row['timeframe'] == '12h' and row['base_direction'] == 'LONG')
        self.assertEqual(inverted['match_status'], normal['match_status'])
        self.assertEqual((normal['analysis_direction'], inverted['analysis_direction']), ('LONG', 'SHORT'))

    def test_source_immutable_and_canonical_payload_identity_survives_json_roundtrip(self):
        obs = observation('HYPE')
        before = deepcopy(obs)
        result = adapter.evaluate_coin(obs)
        self.assertEqual(obs, before)
        self.assertEqual(adapter.evaluate_coin(json.loads(adapter.canonical(obs))), result)
        self.assertEqual(result['feature_sha256'], adapter.digest({key: value for key, value in result.items() if key != 'feature_sha256'}))
        for key in ('consumer_version', 'population_version', 'snapshot_set_id', 'symbol', 'bundle_sha256', 'parent_payload_sha256'):
            self.assertEqual(result[key], obs[key])
        bad = deepcopy(obs)
        bad['bundle_sha256'] = 'invalid'
        with self.assertRaises(ValueError):
            adapter.evaluate_coin(bad)


if __name__ == '__main__':
    unittest.main()
