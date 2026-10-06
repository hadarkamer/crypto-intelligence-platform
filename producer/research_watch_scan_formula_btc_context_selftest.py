"""Causal BTC context, exact existing method parity and frozen-catalog checks."""
from copy import deepcopy
from datetime import timedelta
import json
import unittest
from unittest.mock import patch

import research_past_price_features as past
import research_watch_scan_formula_btc_context as adapter
import research_watch_scan_formula_maxpain as previous
from research_watch_scan_formula_maxpain_selftest import observation
from research_watch_scan_measurement_selftest import candle

PREFIX = 'captured-question-search-v3-experimental-binding:'


def btc_path(obs, *, minutes=240, price=100, high=101, low=99, close=101):
    boundary = obs['usable_from_utc'].replace(second=0, microsecond=0)
    bars = [candle(boundary-timedelta(minutes=minutes-i), price=price,
        high=high, low=low, close=price) for i in range(minutes)]
    if bars:
        bars[-1]['close'] = close
    return {'symbol': 'BTC', 'pair': 'BTCUSDT', 'exchange': 'binance', 'market': 'spot',
        'interval': '1m', 'interval_seconds': 60, 'multiplier': 1.0,
        'archive_route': adapter.measurement.BINANCE_SPOT, 'candles': bars,
        'complete': True, 'expected_candles': minutes}


def build(obs=None, path=None):
    obs = obs or observation()
    return adapter.build_btc_context(obs, btc_path(obs) if path is None else path,
        computed_at=obs['usable_from_utc']+timedelta(minutes=5))


def decision(payload, suffix='BTC_PRIOR_1h_UP', base='LONG'):
    return next(row for row in payload['evaluations']
        if row['candidate_key'] == PREFIX+suffix and row['base_direction'] == base)


def rehash(context):
    context['context_sha256'] = adapter.digest({key: value for key, value in context.items()
        if key != 'context_sha256'})
    return context


class BtcContextTests(unittest.TestCase):
    def test_exact_298_catalog_106_supported_and_catalog_drift_guard(self):
        before, after = previous.catalog_records(), adapter.catalog_records()
        self.assertEqual(len(after), 298)
        self.assertEqual(sum(row['supported'] for row in after), 106)
        newly = [new for old, new in zip(before, after) if not old['supported'] and new['supported']]
        self.assertEqual(len(newly), 24)
        self.assertEqual(sum(row['orientation'] == 'INVERSE' for row in newly), 12)
        for old, new in zip(before, after):
            for key in ('candidate_key', 'definition', 'definition_sha256', 'orientation'):
                self.assertEqual(old[key], new[key])
        originals = adapter.legacy.existing.candidate_catalog(include_extended=True)
        with patch.object(adapter.legacy.existing, 'candidate_catalog', return_value=originals[:-1]):
            with self.assertRaisesRegex(ValueError, 'CATALOG_DRIFT'):
                adapter.catalog_records()

    def test_windows_and_features_match_public_original_past_method(self):
        obs, path = observation(), btc_path(observation())
        context = build(obs, path)
        original = past.calculate_past_price_features(symbol='BTC', event_time=obs['usable_from_utc'],
            candles=path['candles'], path_result=path, computed_at=obs['usable_from_utc'])
        flat = past.flatten_event_features(original, 'LONG')
        self.assertEqual(context['features'], {key: flat[key] for key in adapter.CONTEXT_FEATURES})
        for name in ('1h', '4h'):
            self.assertEqual(context['windows'][name],
                adapter.legacy._normalized_payload(original['windows'][name]['btc']))
        self.assertEqual(context['method_version'], past.METHOD_VERSION)
        self.assertEqual(context['market_regime_version'], past.REGIME_VERSION)
        self.assertEqual(context['status'], 'READY')

    def test_exact_and_intraminute_cutoff_excludes_future_and_delayed_entry_minute(self):
        for seconds in (0, 18, 59):
            obs = observation()
            usable = obs['usable_from_utc'].replace(second=seconds, microsecond=0)
            obs.update(usable_from_utc=usable, source_created_at_utc=usable,
                source_available_at_utc=usable-timedelta(seconds=3),
                observed_at_utc=usable-timedelta(seconds=6))
            path = btc_path(obs)
            context = build(obs, path)
            boundary = usable.replace(second=0, microsecond=0)
            self.assertEqual(context['cutoff_utc'], boundary.isoformat())
            self.assertEqual(context['windows']['1h']['window_end_utc'],
                (boundary-timedelta(milliseconds=1)).isoformat())
            for offset in (0, 1, 2):
                poisoned = candle(boundary+timedelta(minutes=offset))
                poisoned.update(open='MUST_NOT_INSPECT', high=float('nan'), low=-100, close=None)
                path['candles'].append(poisoned)
            later = adapter.build_btc_context(obs, path, computed_at=usable+timedelta(days=30))
            self.assertEqual(context, later)

    def test_direction_flat_and_market_range_are_distinct(self):
        obs = observation()
        flat = build(obs, btc_path(obs, close=100))
        self.assertEqual(flat['features'], {adapter.BTC_DIRECTION: 'FLAT', adapter.BTC_REGIME: 'RANGE'})
        ranged = build(obs, btc_path(obs, close=100.5, high=110, low=90))
        self.assertEqual(ranged['features'], {adapter.BTC_DIRECTION: 'UP', adapter.BTC_REGIME: 'RANGE'})
        payload = adapter.evaluate_coin(obs, btc_context=ranged)
        self.assertEqual(decision(payload, 'BTC_PRIOR_1h_FLAT')['match_status'], 'NO_MATCH')

    def test_exact_efficiency_half_is_directional_for_both_signs(self):
        obs = observation()
        for close, high, low, expected in ((130, 132, 128, 'UP'), (126, 128, 124, 'DOWN')):
            context = build(obs, btc_path(obs, price=128, close=close, high=high, low=low))
            self.assertEqual(context['windows']['4h']['market_regime_efficiency'], 0.5)
            self.assertEqual(context['features'][adapter.BTC_REGIME], expected)

    def test_valid_hour_survives_missing_four_hour_history(self):
        obs = observation()
        context = build(obs, btc_path(obs, minutes=60))
        self.assertEqual(context['status'], 'PARTIAL')
        self.assertEqual(context['features'], {adapter.BTC_DIRECTION: 'UP'})
        self.assertIn(adapter.BTC_REGIME, context['unavailable_features'])
        payload = adapter.evaluate_coin(obs, btc_context=context)
        self.assertEqual(decision(payload)['match_status'], 'MATCH')
        self.assertEqual(decision(payload, 'price_oi_65_BTC_4h_UP')['match_status'], 'UNKNOWN')
        # The original three-valued conjunction still knows a failed score.
        self.assertEqual(decision(payload, 'price_oi_65_BTC_4h_UP', 'SHORT')['match_status'], 'NO_MATCH')

    def test_duplicates_gaps_and_invalid_in_window_prices_are_unavailable(self):
        obs = observation()
        for mutation in ('duplicate', 'gap', 'invalid'):
            path = btc_path(obs)
            if mutation == 'duplicate':
                path['candles'].append(deepcopy(path['candles'][-1]))
            elif mutation == 'gap':
                path['candles'].pop()
            else:
                path['candles'][-1]['high'] = -1
            context = build(obs, path)
            self.assertEqual(context['status'], 'DATA_MISSING')
            self.assertEqual(context['features'], {})
            self.assertEqual(set(context['unavailable_features']), adapter.CONTEXT_FEATURES)

    def test_wrong_route_pair_instrument_and_hype_prices_never_become_btc_context(self):
        obs = observation('HYPE')
        for key, value in (('archive_route', adapter.measurement.HYPE_PERP), ('market', 'futures'),
                ('symbol', 'HYPE'), ('pair', 'HYPE-PERP'), ('api_coin', '@107'), ('price_kind', 'MARK')):
            path = btc_path(obs)
            path[key] = value
            context = build(obs, path)
            self.assertEqual(context['route_validation_reason'], 'INVALID_BTC_SPOT_ARCHIVE_ROUTE')
            self.assertEqual(context['features'], {})
            self.assertEqual(context['status'], 'DATA_MISSING')
        context = build(obs)
        self.assertEqual(context['features'][adapter.BTC_DIRECTION], 'UP')
        self.assertEqual(context['windows']['1h']['source']['symbol'], 'BTC')
        self.assertEqual(adapter.evaluate_coin(obs, btc_context=context)['btc_context_provenance'], context)

    def test_context_identity_hash_and_semantic_window_proof_are_checked(self):
        obs, context = observation(), build()
        for field, value in (('snapshot_set_id', 2), ('bundle_sha256', 'c'*64),
                ('parent_payload_sha256', 'c'*64), ('consumer_version', 'other')):
            bad = rehash({**context, field: value})
            with self.assertRaisesRegex(ValueError, 'IDENTITY_MISMATCH'):
                adapter.evaluate_coin(obs, btc_context=bad)
        bad = deepcopy(context)
        bad['features'][adapter.BTC_DIRECTION] = 'DOWN'
        with self.assertRaisesRegex(ValueError, 'HASH_MISMATCH'):
            adapter.evaluate_coin(obs, btc_context=bad)
        with self.assertRaisesRegex(ValueError, 'FEATURE_PROOF_MISMATCH'):
            adapter.evaluate_coin(obs, btc_context=rehash(bad))
        for field, value in (('market_regime', 'RANGE'), ('path_samples', 239),
                ('window_end_utc', obs['usable_from_utc'].isoformat())):
            bad = deepcopy(context)
            bad['windows']['4h'][field] = value
            with self.assertRaisesRegex(ValueError, 'FEATURE_PROOF_MISMATCH'):
                adapter.evaluate_coin(obs, btc_context=rehash(bad))

    def test_source_availability_and_computation_cannot_precede_source(self):
        obs = observation()
        with self.assertRaisesRegex(ValueError, 'COMPUTED_BEFORE'):
            adapter.build_btc_context(obs, btc_path(obs), computed_at=obs['usable_from_utc']-timedelta(seconds=1))
        bad = deepcopy(obs)
        bad['source_created_at_utc'] += timedelta(minutes=1)
        with self.assertRaisesRegex(ValueError, 'SOURCE_AVAILABILITY'):
            build(bad)
        later = deepcopy(obs)
        later.update(usable_from_utc=obs['usable_from_utc']+timedelta(minutes=1),
            source_created_at_utc=obs['source_created_at_utc']+timedelta(minutes=1))
        with self.assertRaisesRegex(ValueError, 'IDENTITY_MISMATCH'):
            adapter.evaluate_coin(later, btc_context=build(obs))

    def test_existing_82_decisions_unchanged_with_missing_or_valid_context(self):
        for symbol in ('BTC', 'HYPE'):
            obs = observation(symbol)
            old = previous.evaluate_coin(obs)
            ids = {row['candidate_key'] for row in old['evaluations']}
            for context in (None, build(obs)):
                result = adapter.evaluate_coin(obs, btc_context=context)
                self.assertEqual(len(result['evaluations']), 212)
                self.assertEqual([row for row in result['evaluations'] if row['candidate_key'] in ids],
                    old['evaluations'])
                for base in adapter.DIRECTIONS:
                    before = old['features_by_direction'][base]
                    after = result['features_by_direction'][base]
                    for key, value in before['features'].items():
                        self.assertEqual(after['features'][key], value)
                    for key, value in before['unavailable_features'].items():
                        self.assertEqual(after['unavailable_features'][key], value)
                if context is None:
                    self.assertEqual(decision(result)['match_status'], 'UNKNOWN')

    def test_btc_states_are_absolute_and_inverse_only_flips_outcome(self):
        obs, context = observation(), build()
        result = adapter.evaluate_coin(obs, btc_context=context)
        inverse = next(row for row in adapter.catalog_records()
            if row['definition'].get('base_candidate_key') == PREFIX+'BTC_PRIOR_1h_UP')
        for base, opposite in (('LONG', 'SHORT'), ('SHORT', 'LONG')):
            normal = decision(result, base=base)
            inverted = next(row for row in result['evaluations']
                if row['candidate_key'] == inverse['candidate_key'] and row['base_direction'] == base)
            self.assertEqual(normal['match_status'], 'MATCH')
            self.assertEqual(inverted['match_status'], normal['match_status'])
            self.assertEqual(inverted['analysis_direction'], opposite)
            self.assertEqual(result['features_by_direction'][base]['features'][adapter.BTC_DIRECTION], 'UP')
        encoded = json.loads(adapter.canonical(context))
        self.assertEqual(adapter.evaluate_coin(obs, btc_context=encoded), result)
        self.assertEqual(result['feature_sha256'], adapter.digest({key: value for key, value in result.items()
            if key != 'feature_sha256'}))


if __name__ == '__main__':
    unittest.main()
