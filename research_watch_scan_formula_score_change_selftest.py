"""Finite all-scan score-change policy and original predicate preservation."""
from copy import deepcopy
from datetime import datetime, timedelta
import json
import unittest
from unittest.mock import patch

import research_ordered_question_catalog as questions
import research_watch_scan_formula_score_change as adapter
from research_watch_scan_formula_maxpain_selftest import observation as source_observation
from research_watch_scan_formula_asset_context_selftest import build as asset_contexts

PREFIX = questions.VERSION+':'


def observation(symbol='ETH', *, source_id=10, age=0, score=70, direction='BULLISH'):
    source = source_observation(symbol)
    shift = timedelta(minutes=age)
    def shifted(value):
        if isinstance(value, datetime):
            return value-shift
        if isinstance(value, dict):
            return {key: shifted(item) for key, item in value.items()}
        if isinstance(value, list):
            return [shifted(item) for item in value]
        if isinstance(value, str):
            try:
                return (datetime.fromisoformat(value)-shift).isoformat()
            except ValueError:
                pass
        return value
    source = shifted(source)
    source.update(snapshot_set_id=source_id, watch_scan_id='watch-'+str(source_id),
        bundle_sha256=f'{source_id:064x}', parent_payload_sha256=f'{source_id+100:064x}')
    for model in source['models'].values():
        model.update(score=score, direction=direction)
    return source


def build(current=None, prior=None):
    current = current or observation()
    return adapter.build_score_change_context(current,
        [observation(source_id=9, age=10, score=60)] if prior is None else prior,
        computed_at=current['usable_from_utc'])


def delta(context, model='spot_cvd', base='LONG'):
    return context['features_by_direction'][base].get('sequence.30m.'+model+'.score_change')


def decision(payload, model='spot_cvd', label='STRENGTHENING', base='LONG'):
    return next(row for row in payload['evaluations'] if row['candidate_key'] ==
        PREFIX+model+'_'+label+'_30m' and row['base_direction'] == base)


def rehash(context):
    context['context_sha256'] = adapter.digest({key: value for key, value in context.items() if key != 'context_sha256'})
    return context


class ScoreChangeTests(unittest.TestCase):
    def test_exact_298_definitions_170_supported_and_only12_added(self):
        before, after = adapter.previous.catalog_records(), adapter.catalog_records()
        added = [row for old, row in zip(before, after) if row['supported'] and not old['supported']]
        self.assertEqual((len(after), sum(row['supported'] for row in after), len(added)), (298, 170, 12))
        self.assertEqual(sum(row['orientation'] == 'INVERSE' for row in added), 6)
        self.assertEqual({condition['feature'] for row in added for condition in
            adapter.legacy.existing._conditions(row['definition'])}, adapter.SCORE_CHANGE_FEATURES)
        for old, row in zip(before, after):
            for key in ('candidate_key', 'definition', 'definition_sha256', 'orientation'):
                self.assertEqual(old[key], row[key])
        originals = adapter.legacy.existing.candidate_catalog(include_extended=True)
        with patch.object(adapter.legacy.existing, 'candidate_catalog', return_value=originals[:-1]):
            with self.assertRaisesRegex(ValueError, 'CATALOG_DRIFT'):
                adapter.catalog_records()

    def test_original_delta_algebra_on_same_supplied_predecessor(self):
        current, old = observation(), observation(source_id=9, age=10, score=60)
        context = build(current, [old])
        self.assertEqual(context['selection_status'], 'SELECTED')
        self.assertEqual(context['status'], 'READY')
        self.assertEqual(context['predecessors'][0]['snapshot_set_id'], old['snapshot_set_id'])
        for base in adapter.DIRECTIONS:
            current_totals = adapter.legacy.evaluate_coin(current)['features_by_direction'][base]['features']
            old_totals = adapter.legacy.evaluate_coin(old)['features_by_direction'][base]['features']
            # Algebra parity with the original pure helper given the SAME
            # observations, not a claim of delivered-alert population parity.
            event = {'symbol': 'ETH', 'direction': base, 'alert_time_utc': current['usable_from_utc']}
            old_event = {**event, 'alert_time_utc': old['usable_from_utc'],
                'engine_snapshot': {'watch_scan_id': old['watch_scan_id']}}
            expected = questions.sequence_features(event, current_totals, [(old_event, old_totals)])
            self.assertEqual(context['features_by_direction'][base], {key: expected[key] for key in adapter.SCORE_CHANGE_FEATURES})
        self.assertEqual(delta(context), 10)
        self.assertEqual(delta(context, base='SHORT'), -10)

    def test_exact_30minute_boundary_inclusive_start_exclusive_end(self):
        current = observation()
        self.assertEqual(delta(build(current, [observation(source_id=9, age=30, score=60)])), 10)
        for age in (30+1/60, 0, -1):
            with self.assertRaisesRegex(ValueError, 'NOT_CAUSAL_30M'):
                build(current, [observation(source_id=9, age=age, score=60)])
        with self.assertRaisesRegex(ValueError, 'COMPUTED_BEFORE'):
            adapter.build_score_change_context(current, [], computed_at=current['usable_from_utc']-timedelta(seconds=1))

    def test_source_observed_order_and_late_availability_are_both_required(self):
        current = observation()
        late = observation(source_id=9, age=10, score=60)
        late.update(source_created_at_utc=current['usable_from_utc']+timedelta(seconds=1),
            usable_from_utc=current['usable_from_utc']+timedelta(seconds=1))
        with self.assertRaisesRegex(ValueError, 'NOT_CAUSAL_30M'):
            build(current, [late])
        equal_observed = observation(source_id=9, age=3/60, score=60)
        equal_observed['observed_at_utc'] = current['observed_at_utc']
        with self.assertRaisesRegex(ValueError, 'NOT_CAUSAL_30M'):
            build(current, [equal_observed])
        wrong_usable = observation(source_id=9, age=10)
        wrong_usable['usable_from_utc'] += timedelta(seconds=1)
        with self.assertRaisesRegex(ValueError, 'SOURCE_AVAILABILITY'):
            build(current, [wrong_usable])

    def test_nearest_missing_module_never_falls_back_to_older_numeric_value(self):
        current = observation()
        near, older = observation(source_id=9, age=5, score=60), observation(source_id=8, age=20, score=50)
        near['models']['spot_flow'].update(available=False, capture_status='UNAVAILABLE')
        context = build(current, [older, near])
        self.assertEqual(len(context['predecessors']), 1)
        self.assertEqual(context['predecessors'][0]['snapshot_set_id'], 9)
        self.assertEqual(context['status'], 'PARTIAL')
        self.assertIsNone(delta(context))
        self.assertEqual(delta(context, 'price_oi'), 10)
        self.assertEqual(adapter.validate_score_change_context(current, context), context)
        payload = adapter.evaluate_coin(current, score_change_context=context)
        self.assertEqual(decision(payload)['match_status'], 'UNKNOWN')

    def test_latest_timestamp_ties_remain_ambiguous_even_when_scores_agree(self):
        current = observation()
        first, second = observation(source_id=9, age=5, score=60), observation(source_id=8, age=5, score=60)
        context = build(current, [first, second])
        self.assertEqual(context, build(current, [second, first]))
        self.assertEqual(context['selection_status'], 'AMBIGUOUS')
        self.assertEqual(context['status'], 'DATA_MISSING')
        self.assertEqual(len(context['predecessors']), 2)
        self.assertEqual(context['features_by_direction'], {'LONG': {}, 'SHORT': {}})
        self.assertEqual(adapter.validate_score_change_context(current, context), context)

    def test_no_predecessor_is_unknown_not_zero_or_a_first_entry(self):
        current = observation()
        context = build(current, [])
        self.assertEqual((context['selection_status'], context['predecessors']), ('NO_PREDECESSOR', []))
        self.assertEqual(context['features_by_direction'], {'LONG': {}, 'SHORT': {}})
        self.assertEqual(adapter.validate_score_change_context(current, context), context)
        payload = adapter.evaluate_coin(current, score_change_context=context)
        self.assertEqual(decision(payload)['match_status'], 'UNKNOWN')
        self.assertFalse(any('.entry_ordinal' in key for key in payload['features_by_direction']['LONG']['features']))

    def test_current_unavailability_and_invalid_source_time_remain_explicit(self):
        current = observation()
        current['models']['positioning'].update(available=False, capture_status='UNAVAILABLE')
        current['sources']['spot']['quality']['candle_close'] = (current['observed_at_utc']+timedelta(minutes=1)).isoformat()
        context = build(current)
        self.assertIsNone(delta(context, 'price_oi'))
        self.assertIsNone(delta(context, 'spot_cvd'))
        self.assertEqual(delta(context, 'futures_cvd'), 10)
        self.assertEqual(context['status'], 'PARTIAL')
        self.assertTrue(all(reason.startswith('CURRENT:') for key, reasons in
            context['unavailable_features_by_direction']['LONG'].items() for reason in reasons))

    def test_zero_and_direction_change_use_signed_totals_and_inverse_only_changes_outcome(self):
        current = observation(score=0, direction='NEUTRAL')
        zero = build(current, [observation(source_id=9, age=10, score=0, direction='NEUTRAL')])
        self.assertEqual(delta(zero), 0)
        payload = adapter.evaluate_coin(current, score_change_context=zero)
        for label in ('STRENGTHENING', 'WEAKENING'):
            self.assertEqual(decision(payload, label=label)['match_status'], 'NO_MATCH')
        current = observation(score=70)
        context = build(current, [observation(source_id=9, age=10, score=60, direction='BEARISH')])
        self.assertEqual((delta(context), delta(context, base='SHORT')), (130, -130))
        payload = adapter.evaluate_coin(current, score_change_context=context)
        inverse = next(row for row in adapter.catalog_records() if row['definition'].get('base_candidate_key') ==
            PREFIX+'spot_cvd_STRENGTHENING_30m')
        for base in adapter.DIRECTIONS:
            normal = decision(payload, base=base)
            flipped = next(row for row in payload['evaluations'] if row['candidate_key'] == inverse['candidate_key']
                and row['base_direction'] == base)
            self.assertEqual(flipped['match_status'], normal['match_status'])
            self.assertNotEqual(flipped['analysis_direction'], normal['analysis_direction'])

    def test_hype_captured_cvd_remains_valid_without_own_spot_price(self):
        current, old = observation('HYPE'), observation('HYPE', source_id=9, age=10, score=60)
        context = build(current, [old])
        self.assertEqual(context['status'], 'READY')
        self.assertEqual(delta(context), 10)
        payload = adapter.evaluate_coin(current, score_change_context=context)
        self.assertEqual(decision(payload)['match_status'], 'MATCH')
        self.assertEqual(payload['features_by_direction']['LONG']['features']['spot_cvd.aligned_score'], 70)

    def test_source_identity_distinct_watch_and_bounded_input_are_validated(self):
        current, old = observation(), observation(source_id=9, age=10)
        for key, value in (('symbol', 'SOL'), ('watch_scan_id', current['watch_scan_id']),
                ('snapshot_set_id', current['snapshot_set_id']), ('consumer_version', 'other'), ('watch_scan_id', '')):
            bad = deepcopy(old)
            bad[key] = value
            with self.assertRaises(ValueError):
                build(current, [bad])
        with self.assertRaisesRegex(ValueError, 'DUPLICATE'):
            build(current, [old, old])
        with self.assertRaisesRegex(ValueError, 'BUDGET'):
            build(current, [old, old, old])

    def test_context_hash_identity_raw_source_and_derived_proof_revalidate(self):
        current = observation()
        context = build(current)
        bad = deepcopy(context)
        bad['features_by_direction']['LONG']['sequence.30m.spot_cvd.score_change'] = 999
        with self.assertRaisesRegex(ValueError, 'HASH_MISMATCH'):
            adapter.validate_score_change_context(current, bad)
        with self.assertRaisesRegex(ValueError, 'PROOF_MISMATCH'):
            adapter.validate_score_change_context(current, rehash(bad))
        for key, value in (('snapshot_set_id', 11), ('watch_scan_id', 'other'), ('bundle_sha256', 'c'*64)):
            with self.assertRaisesRegex(ValueError, 'IDENTITY_MISMATCH'):
                adapter.validate_score_change_context(current, rehash({**context, key: value}))
        bad = deepcopy(context)
        bad['predecessors'][0]['observation']['models']['spot_flow']['score'] = 1
        with self.assertRaisesRegex(ValueError, 'PROOF_MISMATCH'):
            adapter.validate_score_change_context(current, rehash(bad))
        bad = deepcopy(context)
        bad['predecessors'][0]['snapshot_set_id'] = 99
        with self.assertRaisesRegex(ValueError, 'PROOF_MISMATCH'):
            adapter.validate_score_change_context(current, rehash(bad))
        bad = deepcopy(context)
        bad['selection_version'] = 'delivered-alert-history'
        with self.assertRaisesRegex(ValueError, 'PROOF_MISMATCH'):
            adapter.validate_score_change_context(current, rehash(bad))

    def test_all158_existing_decisions_features_and_contexts_unchanged(self):
        for symbol in ('ETH', 'BTC', 'HYPE'):
            current = observation(symbol)
            btc, asset = asset_contexts(current)
            before = adapter.previous.evaluate_coin(current, btc_context=btc, asset_context=asset)
            context = build(current, [observation(symbol, source_id=9, age=10, score=60)])
            for score in (None, context):
                after = adapter.evaluate_coin(current, btc_context=btc, asset_context=asset, score_change_context=score)
                self.assertEqual(len(after['evaluations']), 340)
                ids = {row['candidate_key'] for row in before['evaluations']}
                self.assertEqual([row for row in after['evaluations'] if row['candidate_key'] in ids], before['evaluations'])
                for base in adapter.DIRECTIONS:
                    for category in ('features', 'unavailable_features'):
                        for key, value in before['features_by_direction'][base][category].items():
                            self.assertEqual(after['features_by_direction'][base][category][key], value)
                self.assertEqual(after['btc_context_provenance'], before['btc_context_provenance'])
                self.assertEqual(after['asset_context_provenance'], before['asset_context_provenance'])
                self.assertEqual(after['feature_sha256'], adapter.digest({key: value for key, value in after.items()
                    if key != 'feature_sha256'}))
            encoded = json.loads(adapter.canonical(context))
            self.assertEqual(adapter.validate_score_change_context(current, encoded), context)


if __name__ == '__main__':
    unittest.main()
