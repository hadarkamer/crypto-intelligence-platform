"""Pure checks of unchanged catalog predicates and frozen feature availability."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import unittest
from unittest.mock import patch

import research_formula_ordered_v7 as existing
import research_watch_scan_formula as formulas
from research_watch_scan_measurement_selftest import observation as receipt


def observation(symbol='BTC'):
    obs = receipt(symbol)
    observed = obs['observed_at_utc']
    latest = observed.replace(second=0, microsecond=0)-timedelta(milliseconds=1)
    window = {'available': True, 'latest_time': latest.isoformat(),
              'reference_time': (latest-timedelta(minutes=30)).isoformat()}
    positioning_window = {'available': True, 'comparison_source': 'live_snapshot',
                          'reference_time': window['reference_time'],
                          'reference_target_time': (observed-timedelta(minutes=30)).isoformat()}
    obs.update(models={key: {'available': True, 'capture_status': 'AVAILABLE',
                           'score': 70.0, 'direction': 'BULLISH'}
                       for key in ('positioning', 'futures_flow', 'spot_flow')},
        sources={'positioning': {'price_fetched_at': observed.isoformat(),
                    'oi_fetched_at': observed.isoformat(), 'window_references': {'30m': positioning_window}},
                 'futures': {'quality': {'candle_close': latest.isoformat()}, 'window_references': {'30m': deepcopy(window)}},
                 'spot': {'quality': {'candle_close': latest.isoformat()}, 'window_references': {'30m': deepcopy(window)}},
                 'timing_observation': {'cvd_observed_at_utc': observed.isoformat()}},
        source_time_errors=[])
    return obs


def evaluation(payload, key, base='LONG'):
    return next(row for row in payload['evaluations'] if row['candidate_key']==key and row['base_direction']==base)


class FormulaTests(unittest.TestCase):
    def test_all_298_definitions_preserved_only_34_supported(self):
        records = formulas.catalog_records()
        originals = existing.candidate_catalog(include_extended=True)
        self.assertEqual(len(records), 298)
        self.assertEqual(sum(row['supported'] for row in records), 34)
        self.assertEqual(sum(row['orientation']=='INVERSE' for row in records), 149)
        self.assertEqual(len({feature for row in records for feature in row['unsupported_features']}), 41)
        for original, record in zip(originals, records):
            self.assertEqual(record['definition'], original)
            encoded = json.dumps(original, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)
            self.assertEqual(record['definition_sha256'], hashlib.sha256(encoded.encode()).hexdigest())
            self.assertEqual(record['candidate_key'], original['formula_id'])
        legacy = next(r for r in records if r['candidate_key']=='FUTURES_CVD_TOTAL_65')
        self.assertIs(type(legacy['definition']['conditions'][0]['value']), float)

    def test_catalog_addition_removal_or_condition_change_requires_new_adapter_version(self):
        original = existing.candidate_catalog(include_extended=True)
        changed = deepcopy(original)
        changed[0]['conditions'][0]['value'] += 1
        for candidates in (original[:-1], original+[deepcopy(original[-1])], changed):
            with patch.object(existing, 'candidate_catalog', return_value=candidates):
                with self.assertRaisesRegex(ValueError, 'CATALOG_DRIFT_REQUIRES_NEW_ADAPTER_VERSION'):
                    formulas.catalog_records()

    def test_supported_evaluations_never_read_outcomes_or_add_features(self):
        obs = observation()
        obs['outcomes'] = {'future_success': True}
        with patch.object(existing, 'summarize_scope', side_effect=AssertionError('No outcome aggregation')):
            payload = formulas.evaluate_coin(obs)
        self.assertEqual(len(payload['evaluations']), 68)
        for record in payload['features_by_direction'].values():
            self.assertEqual(set(record['features']), formulas.SUPPORTED_FEATURES)
            self.assertFalse(record['unavailable_features'])
        changed = deepcopy(obs)
        changed['outcomes'] = {'future_success': False}
        self.assertEqual(formulas.evaluate_coin(changed), payload)

    def test_known_false_conjunct_wins_over_unknown_without_losing_coverage(self):
        candidate = next(c for c in existing.candidate_catalog() if c['formula_id']=='STRICT_TRIPLE_TOTAL_65')
        failed = formulas.evaluate_candidate(candidate, {'futures_cvd.aligned_score': 0})
        self.assertEqual(failed['match_status'], 'NO_MATCH')
        self.assertEqual(failed['missing_features'], ['price_oi.aligned_score', 'spot_cvd.aligned_score'])
        unknown = formulas.evaluate_candidate(candidate, {'futures_cvd.aligned_score': 65})
        self.assertEqual(unknown['match_status'], 'UNKNOWN')
        self.assertEqual(formulas.evaluate_candidate(candidate, {name: 65 for name in (
            'price_oi.aligned_score', 'futures_cvd.aligned_score', 'spot_cvd.aligned_score')})['match_status'], 'MATCH')

    def test_boolean_false_is_known_but_numeric_zero_is_not_a_boolean(self):
        candidate = {'conditions': [{'feature': 'time.weekend', 'operator': '==', 'value': False}], 'repeat_count': 1}
        self.assertEqual(formulas.evaluate_candidate(candidate, {'time.weekend': False})['match_status'], 'MATCH')
        self.assertEqual(formulas.evaluate_candidate(candidate, {'time.weekend': True})['match_status'], 'NO_MATCH')
        self.assertEqual(formulas.evaluate_candidate(candidate, {'time.weekend': 0})['match_status'], 'UNKNOWN')
        self.assertEqual(formulas.evaluate_candidate(candidate, {})['match_status'], 'UNKNOWN')

    def test_inverse_preserves_base_predicate_and_only_flips_outcome_direction(self):
        payload = formulas.evaluate_coin(observation())
        records = formulas.catalog_records()
        inverse_key = next(r['candidate_key'] for r in records if r['definition'].get('base_candidate_key')=='FUTURES_CVD_TOTAL_65')
        for base, opposite in (('LONG', 'SHORT'), ('SHORT', 'LONG')):
            normal = evaluation(payload, 'FUTURES_CVD_TOTAL_65', base)
            inverse = evaluation(payload, inverse_key, base)
            self.assertEqual(normal['match_status'], inverse['match_status'])
            self.assertEqual(normal['analysis_direction'], base)
            self.assertEqual(inverse['analysis_direction'], opposite)
        self.assertEqual(evaluation(payload, inverse_key)['match_status'], 'MATCH')
        self.assertEqual(evaluation(payload, inverse_key, 'SHORT')['match_status'], 'NO_MATCH')
        normal = next(r for r in records if r['candidate_key']=='FUTURES_CVD_TOTAL_65')
        inverse = next(r for r in records if r['candidate_key']==inverse_key)
        self.assertNotEqual(normal['definition_sha256'], inverse['definition_sha256'])

    def test_available_zero_is_distinct_from_hype_unavailable_zero(self):
        obs = observation('HYPE')
        self.assertEqual(evaluation(formulas.evaluate_coin(obs), 'SPOT_CVD_TOTAL_65')['match_status'], 'MATCH')
        obs['models']['spot_flow'].update(score=0.0, direction='NEUTRAL')
        known = formulas.evaluate_coin(obs)
        self.assertEqual(known['features_by_direction']['LONG']['features']['spot_cvd.aligned_score'], 0)
        self.assertEqual(evaluation(known, 'SPOT_CVD_TOTAL_65')['match_status'], 'NO_MATCH')
        obs['models']['spot_flow'].update(available=False, capture_status='UNAVAILABLE')
        unknown = formulas.evaluate_coin(obs)
        self.assertNotIn('spot_cvd.aligned_score', unknown['features_by_direction']['LONG']['features'])
        self.assertIn('spot_cvd.aligned_score', unknown['features_by_direction']['LONG']['unavailable_features'])
        self.assertEqual(evaluation(unknown, 'SPOT_CVD_TOTAL_65')['match_status'], 'UNKNOWN')
        self.assertEqual(evaluation(unknown, 'STRICT_TRIPLE_TOTAL_65')['match_status'], 'UNKNOWN')
        self.assertEqual(evaluation(unknown, 'STRICT_TRIPLE_TOTAL_65', 'SHORT')['match_status'], 'NO_MATCH')

    def test_positioning_reference_only_windows_use_both_real_current_fetch_times(self):
        obs = observation()
        window = obs['sources']['positioning']['window_references']['30m']
        self.assertNotIn('latest_time', window)
        self.assertEqual(evaluation(formulas.evaluate_coin(obs), 'PRICE_OI_TOTAL_65')['match_status'], 'MATCH')
        for field, wrong in (('reference_time', None),
                              ('reference_time', '2027-01-01T00:00:00+00:00'),
                              ('reference_target_time', '2027-01-01T00:00:00+00:00'),
                              ('latest_time', '2027-01-01T00:00:00+00:00')):
            bad = deepcopy(obs)
            bad['sources']['positioning']['window_references']['30m'][field] = wrong
            self.assertEqual(evaluation(formulas.evaluate_coin(bad), 'PRICE_OI_TOTAL_65')['match_status'], 'UNKNOWN')
        for field in ('price_fetched_at', 'oi_fetched_at'):
            for value in (None, '2027-01-01T00:00:00+00:00',
                          (obs['observed_at_utc']-timedelta(hours=1)).isoformat()):
                bad = deepcopy(obs)
                bad['sources']['positioning'][field] = value
                self.assertEqual(evaluation(formulas.evaluate_coin(bad), 'PRICE_OI_TOTAL_65')['match_status'], 'UNKNOWN')

    def test_invalid_scores_and_unvalidated_directions_are_unavailable(self):
        for value in (None, True, '70', float('nan'), float('inf'), 101, -101):
            with self.subTest(score=value):
                obs = observation()
                obs['models']['futures_flow']['score'] = value
                payload = formulas.evaluate_coin(obs)
                self.assertEqual(evaluation(payload, 'FUTURES_CVD_TOTAL_65')['match_status'], 'UNKNOWN')
        for value in (True, 3, 'UNKNOWN', 'arbitrary'):
            obs = observation()
            obs['models']['futures_flow']['direction'] = value
            self.assertEqual(evaluation(formulas.evaluate_coin(obs), 'FUTURES_CVD_TOTAL_65')['match_status'], 'UNKNOWN')

    def test_direction_aliases_and_known_neutral_sign_match_existing_extractor(self):
        for direction, score in (('BUY', 70), ('UP', 70), ('BULLISH', 70), ('LONG', 70),
                                 ('SELL', -70), ('DOWN', -70), ('BEARISH', -70), ('SHORT', -70),
                                 ('NEUTRAL', -5), ('', 5), (None, 0)):
            obs = observation()
            obs['models']['futures_flow'].update(direction=direction, score=score)
            payload = formulas.evaluate_coin(obs)
            for base in formulas.DIRECTIONS:
                expected = existing.extract_event_features({'direction': base,
                    'engine_snapshot': {'market_evidence': {'modules': obs['models']}}})
                self.assertEqual(payload['features_by_direction'][base]['features']['futures_cvd.aligned_score'], expected['futures_cvd.aligned_score'])

    def test_causal_errors_are_scoped_and_never_reweight_other_models(self):
        obs = observation()
        obs['source_time_errors'] = ['12h/price_fetched_at_utc:FUTURE_TIME', 'futures/candle_close:FUTURE_TIME']
        payload = formulas.evaluate_coin(obs)
        features = payload['features_by_direction']['LONG']['features']
        self.assertNotIn('futures_cvd.aligned_score', features)
        self.assertEqual(features['price_oi.aligned_score'], 70)
        self.assertEqual(features['spot_cvd.aligned_score'], 70)
        self.assertIn('time.weekend', features)
        obs['source_time_errors'] = ['derivatives/observed:MISSING_TIME']
        payload = formulas.evaluate_coin(obs)
        self.assertEqual(set(payload['features_by_direction']['LONG']['features']), {'price_oi.aligned_score', 'time.weekend'})

    def test_future_or_unknown_window_source_is_rejected_without_relying_on_error_list(self):
        for field, wrong in (('latest_time', '2027-01-01T00:00:00+00:00'),
                              ('reference_time', '2027-01-01T00:00:00+00:00'),
                              ('reference_time', None), ('latest_time', '2026-09-13T12:00:00')):
            obs = observation()
            obs['sources']['futures']['window_references']['30m'][field] = wrong
            self.assertEqual(evaluation(formulas.evaluate_coin(obs), 'FUTURES_CVD_TOTAL_65')['match_status'], 'UNKNOWN')
        obs = observation()
        obs['sources']['spot']['quality']['candle_close'] = '2027-01-01T00:00:00+00:00'
        self.assertEqual(evaluation(formulas.evaluate_coin(obs), 'SPOT_CVD_TOTAL_65')['match_status'], 'UNKNOWN')
        obs = observation()
        obs['sources']['positioning'].pop('oi_fetched_at')
        self.assertEqual(evaluation(formulas.evaluate_coin(obs), 'PRICE_OI_TOTAL_65')['match_status'], 'UNKNOWN')

    def test_weekend_uses_original_source_time_in_israel_not_later_entry(self):
        # Friday 23:59 in Israel followed by durable availability on Saturday.
        observed = datetime(2026, 9, 11, 20, 59, tzinfo=timezone.utc)
        obs = observation()
        obs.update(observed_at_utc=observed, source_available_at_utc=observed+timedelta(seconds=10),
                   source_created_at_utc=observed+timedelta(minutes=2), usable_from_utc=observed+timedelta(minutes=2))
        payload = formulas.evaluate_coin(obs)
        self.assertIs(payload['features_by_direction']['LONG']['features']['time.weekend'], False)

    def test_immutable_payload_hash_is_deterministic_and_survives_jsonb_numeric_forms(self):
        obs = observation()
        obs['models']['spot_flow'].update(score=-0.0, direction='NEUTRAL')
        original = deepcopy(obs)
        first = formulas.evaluate_coin(obs)
        self.assertEqual(obs, original)
        obs['models']['positioning']['score'] = 70
        obs['models']['spot_flow']['score'] = 0
        second = formulas.evaluate_coin(dict(reversed(list(obs.items()))))
        self.assertEqual(first, second)
        self.assertEqual(first['feature_sha256'], formulas.digest({k: v for k, v in first.items() if k!='feature_sha256'}))
        serialized = json.loads(formulas.canonical(first))
        self.assertEqual(serialized, first)

    def test_invalid_receipt_or_future_feature_predicate_fails_closed(self):
        for key, wrong in (('intake_status', 'REJECTED'), ('source_version', 'v1'),
                           ('bundle_sha256', 'invalid'), ('usable_from_utc', '2020-01-01T00:00:00+00:00')):
            obs = observation()
            obs[key] = wrong
            with self.assertRaises(ValueError):
                formulas.evaluate_coin(obs)
        with self.assertRaises(ValueError):
            formulas.evaluate_candidate({'conditions': [{'feature': 'historical.future_outcome', 'operator': '>', 'value': 0}]}, {})


if __name__ == '__main__':
    unittest.main()
