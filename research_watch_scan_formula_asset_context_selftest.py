"""Prior Spot asset contracts, causal boundaries and exact retained decisions."""
from copy import deepcopy
from datetime import timedelta
import json
import unittest
from unittest.mock import patch

import research_watch_scan_formula_asset_context as adapter
import research_watch_scan_formula_btc_context as previous
from research_watch_scan_formula_btc_context_selftest import btc_path
from research_watch_scan_formula_maxpain_selftest import observation
from research_watch_scan_measurement_selftest import candle

PREFIX = 'captured-question-search-v3-experimental-binding:'


def asset_path(obs, *, minutes=1440, close=101, high=101, low=99):
    result = btc_path(obs, minutes=minutes, close=close, high=high, low=low)
    result.update(symbol=obs['symbol'], pair=obs['symbol']+'USDT')
    return result


def build(obs=None, path=None, btc_context=None):
    obs = obs or observation('ETH')
    btc_context = btc_context or previous.build_btc_context(obs, btc_path(obs), computed_at=obs['usable_from_utc'])
    context = adapter.build_asset_context(obs, asset_path(obs) if path is None else path,
        btc_context=btc_context, computed_at=obs['usable_from_utc'])
    return btc_context, context


def evaluate(obs, context, btc):
    return adapter.evaluate_coin(obs, btc_context=btc, asset_context=context)


def decision(payload, suffix='PRIOR_1h_UP', base='LONG'):
    return next(row for row in payload['evaluations']
        if row['candidate_key'] == PREFIX+suffix and row['base_direction'] == base)


def rehash(context):
    context['context_sha256'] = adapter.digest({key: value for key, value in context.items() if key != 'context_sha256'})
    return context


class AssetContextTests(unittest.TestCase):
    def test_exact_catalog_158_supported_52_added_and_original_definitions(self):
        before, after = previous.catalog_records(), adapter.catalog_records()
        self.assertEqual((len(after), sum(row['supported'] for row in after)), (298, 158))
        new = [row for old, row in zip(before, after) if row['supported'] and not old['supported']]
        self.assertEqual((len(new), sum(row['orientation'] == 'INVERSE' for row in new)), (52, 26))
        self.assertEqual(len(adapter.ASSET_FEATURES), 9)
        for old, row in zip(before, after):
            for key in ('candidate_key', 'definition', 'definition_sha256', 'orientation'):
                self.assertEqual(old[key], row[key])
        originals = adapter.legacy.existing.candidate_catalog(include_extended=True)
        with patch.object(adapter.legacy.existing, 'candidate_catalog', return_value=originals[:-1]):
            with self.assertRaisesRegex(ValueError, 'CATALOG_DRIFT'):
                adapter.catalog_records()

    def test_all_six_windows_and_nine_features_match_original_past_method(self):
        obs = observation('ETH')
        path, btc_source = asset_path(obs, close=102, high=102), btc_path(obs, minutes=1440)
        btc = previous.build_btc_context(obs, btc_source, computed_at=obs['usable_from_utc'])
        context = adapter.build_asset_context(obs, path, btc_context=btc, computed_at=obs['usable_from_utc'])
        original = adapter.past.calculate_past_price_features(symbol='ETH', event_time=obs['usable_from_utc'],
            candles=path['candles'], path_result=path, btc_candles=btc_source['candles'],
            btc_path_result=btc_source, computed_at=obs['usable_from_utc'])
        for name in adapter.WINDOWS:
            self.assertEqual(context['windows'][name], adapter.legacy._normalized_payload(original['windows'][name]['asset']))
        for base in adapter.DIRECTIONS:
            flattened = adapter.past.flatten_event_features(original, base)
            self.assertEqual(context['features_by_direction'][base],
                {key: flattened[key] for key in adapter.ASSET_FEATURES})
        self.assertEqual(context['status'], 'READY')
        self.assertEqual(context['btc_context_sha256'], btc['context_sha256'])

    def test_cutoff_excludes_current_future_and_delayed_entry_candles(self):
        for seconds in (0, 18, 59):
            obs = observation('ETH')
            usable = obs['usable_from_utc'].replace(second=seconds, microsecond=0)
            obs.update(usable_from_utc=usable, source_created_at_utc=usable,
                source_available_at_utc=usable-timedelta(seconds=3), observed_at_utc=usable-timedelta(seconds=6))
            path = asset_path(obs)
            btc, context = build(obs, path)
            boundary = usable.replace(second=0, microsecond=0)
            for offset in (0, 1, 2):
                poisoned = candle(boundary+timedelta(minutes=offset))
                poisoned.update(open='DO_NOT_INSPECT', high=float('nan'), low=-1, close=None)
                path['candles'].append(poisoned)
            after = adapter.build_asset_context(obs, path, btc_context=btc, computed_at=usable+timedelta(days=30))
            self.assertEqual(context, after)
            self.assertEqual(context['cutoff_utc'], boundary.isoformat())
            self.assertTrue(all(window['window_end_utc'] == (boundary-timedelta(milliseconds=1)).isoformat()
                for window in context['windows'].values()))

    def test_each_lookback_has_independent_complete_minute_requirement(self):
        obs = observation('ETH')
        for minutes in (15, 30, 60, 240, 720, 1440):
            btc, context = build(obs, asset_path(obs, minutes=minutes))
            for name, required in adapter.WINDOWS.items():
                self.assertEqual(context['windows'][name]['status'], 'READY' if required <= minutes else 'DATA_MISSING')
                self.assertEqual(adapter.PREFIX+name+'.direction' in context['features_by_direction']['LONG'], required <= minutes)
            evaluate(obs, context, btc)

    def test_duplicates_gaps_and_invalid_closed_prices_never_mean_flat(self):
        obs = observation('ETH')
        for mutation in ('duplicate', 'gap', 'invalid'):
            path = asset_path(obs)
            if mutation == 'duplicate':
                path['candles'].append(deepcopy(path['candles'][-1]))
            elif mutation == 'gap':
                path['candles'].pop()
            else:
                path['candles'][-1]['low'] = -1
            btc, context = build(obs, path)
            self.assertEqual(context['status'], 'DATA_MISSING')
            self.assertEqual(context['features_by_direction'], {'LONG': {}, 'SHORT': {}})
            self.assertEqual(decision(evaluate(obs, context, btc))['match_status'], 'UNKNOWN')

    def test_alignment_uses_base_direction_and_inverse_flips_outcome_only(self):
        obs = observation('ETH')
        btc, context = build(obs)
        payload = evaluate(obs, context, btc)
        inverse = next(row for row in adapter.catalog_records()
            if row['definition'].get('base_candidate_key') == PREFIX+'SPOT65_PRIOR_1h_SUPPORTS')
        for base, expected in (('LONG', 'SUPPORTS'), ('SHORT', 'OPPOSES')):
            self.assertEqual(context['features_by_direction'][base][adapter.PREFIX+'1h.alignment'], expected)
            normal = decision(payload, 'SPOT65_PRIOR_1h_SUPPORTS', base)
            inverted = next(row for row in payload['evaluations']
                if row['candidate_key'] == inverse['candidate_key'] and row['base_direction'] == base)
            self.assertEqual(inverted['match_status'], normal['match_status'])
            self.assertNotEqual(inverted['analysis_direction'], normal['analysis_direction'])

    def test_relative_strength_is_signed_asset_minus_frozen_btc(self):
        obs = observation('ETH')
        for close, expected, matching in ((102, 1, 'STRONGER'), (100, -1, 'WEAKER'), (101, 0, None)):
            btc, context = build(obs, asset_path(obs, close=close, high=102))
            relative = context['features_by_direction']['LONG'][adapter.PREFIX+'1h.relative_strength_pct']
            self.assertAlmostEqual(relative, expected)
            payload = evaluate(obs, context, btc)
            for side in ('STRONGER', 'WEAKER'):
                self.assertEqual(decision(payload, 'RELATIVE_BTC_1h_'+side)['match_status'],
                    'MATCH' if side == matching else 'NO_MATCH')
        missing_btc = previous.build_btc_context(obs, {}, computed_at=obs['usable_from_utc'])
        btc, context = build(obs, btc_context=missing_btc)
        self.assertEqual(context['features_by_direction']['LONG'][adapter.PREFIX+'1h.direction'], 'UP')
        self.assertNotIn(adapter.PREFIX+'1h.relative_strength_pct', context['features_by_direction']['LONG'])
        self.assertEqual(decision(evaluate(obs, context, btc), 'RELATIVE_BTC_1h_STRONGER')['match_status'], 'UNKNOWN')

    def test_btc_reuses_both_frozen_windows_and_is_exactly_relative_zero(self):
        obs = observation()
        btc = previous.build_btc_context(obs, btc_path(obs, minutes=60), computed_at=obs['usable_from_utc'])
        # The new complete archive even has a different last close. Its values
        # cannot replace the established BTC1h/4h evidence from earlier v3.
        context = adapter.build_asset_context(obs, asset_path(obs, close=102, high=102),
            btc_context=btc, computed_at=obs['usable_from_utc'])
        for name in ('1h', '4h'):
            self.assertEqual(context['windows'][name], btc['windows'][name])
        features = context['features_by_direction']['LONG']
        self.assertEqual(features[adapter.PREFIX+'1h.relative_strength_pct'], 0)
        self.assertNotIn(adapter.PREFIX+'4h.direction', features)
        self.assertEqual(features[adapter.PREFIX+'24h.direction'], 'UP')
        evaluate(obs, context, btc)

    def test_hype_own_spot_unavailable_preserves_cvd_and_known_false_conjunction(self):
        obs = observation('HYPE')
        btc, _ = build(obs)
        context = adapter.build_asset_context(obs, None, btc_context=btc, computed_at=obs['usable_from_utc'])
        self.assertEqual(context['route_validation_reason'], adapter.HYPE_UNAVAILABLE)
        self.assertEqual(context['features_by_direction'], {'LONG': {}, 'SHORT': {}})
        for missing in context['unavailable_features_by_direction'].values():
            self.assertEqual(set(missing), adapter.ASSET_FEATURES)
            self.assertTrue(all(reasons == [adapter.HYPE_UNAVAILABLE] for reasons in missing.values()))
        payload, old = evaluate(obs, context, btc), previous.evaluate_coin(obs, btc_context=btc)
        for base in adapter.DIRECTIONS:
            score = old['features_by_direction'][base]['features']['spot_cvd.aligned_score']
            self.assertEqual(payload['features_by_direction'][base]['features']['spot_cvd.aligned_score'], score)
            self.assertEqual(decision(payload, 'SPOT65_PRIOR_1h_SUPPORTS', base)['match_status'],
                'NO_MATCH' if score < 65 else 'UNKNOWN')
        # A supplied perpetual path is never relabelled as the original Spot feature.
        forged = asset_path(obs)
        forged.update(market='perpetual', archive_route=adapter.measurement.HYPE_PERP)
        self.assertEqual(adapter.build_asset_context(obs, forged, btc_context=btc,
            computed_at=obs['usable_from_utc']), context)

    def test_wrong_symbol_pair_route_instrument_and_market_are_unavailable(self):
        obs = observation('ETH')
        for key, value in (('symbol', 'BTC'), ('pair', 'BTCUSDT'), ('market', 'perpetual'),
                ('archive_route', adapter.measurement.HYPE_PERP), ('api_coin', '@107'), ('price_kind', 'MARK')):
            path = asset_path(obs)
            path[key] = value
            btc, context = build(obs, path)
            self.assertEqual(context['route_validation_reason'], 'INVALID_ASSET_SPOT_ARCHIVE_ROUTE')
            self.assertEqual(context['status'], 'DATA_MISSING')
            evaluate(obs, context, btc)

    def test_context_identity_hash_btc_binding_and_window_contract(self):
        obs = observation('ETH')
        btc, context = build(obs)
        for key, value in (('symbol', 'SOL'), ('snapshot_set_id', obs['snapshot_set_id']+1), ('bundle_sha256', 'c'*64)):
            with self.assertRaisesRegex(ValueError, 'IDENTITY_MISMATCH'):
                evaluate(obs, rehash({**context, key: value}), btc)
        bad = deepcopy(context)
        bad['features_by_direction']['LONG'][adapter.PREFIX+'1h.direction'] = 'DOWN'
        with self.assertRaisesRegex(ValueError, 'HASH_MISMATCH'):
            evaluate(obs, bad, btc)
        with self.assertRaisesRegex(ValueError, 'FEATURE_PROOF_MISMATCH'):
            evaluate(obs, rehash(bad), btc)
        with self.assertRaisesRegex(ValueError, 'CONTRACT_MISMATCH'):
            evaluate(obs, rehash({**context, 'btc_context_sha256': 'c'*64}), btc)
        for key, value in (('path_samples', 59), ('window_end_utc', obs['usable_from_utc'].isoformat()),
                ('market_regime', 'FLAT')):
            bad = deepcopy(context)
            bad['windows']['1h'][key] = value
            with self.assertRaisesRegex(ValueError, 'FEATURE_PROOF_MISMATCH'):
                evaluate(obs, rehash(bad), btc)
        later = deepcopy(obs)
        later.update(usable_from_utc=obs['usable_from_utc']+timedelta(minutes=1),
            source_created_at_utc=obs['source_created_at_utc']+timedelta(minutes=1))
        with self.assertRaisesRegex(ValueError, 'IDENTITY_MISMATCH'):
            evaluate(later, context, btc)
        with self.assertRaisesRegex(ValueError, 'COMPUTED_BEFORE'):
            adapter.build_asset_context(obs, asset_path(obs), btc_context=btc,
                computed_at=obs['usable_from_utc']-timedelta(seconds=1))

    def test_all_106_decisions_and_feature_values_remain_unchanged(self):
        for symbol in ('BTC', 'ETH', 'HYPE'):
            obs = observation(symbol)
            for btc_path_value in (btc_path(obs), {}):
                btc = previous.build_btc_context(obs, btc_path_value, computed_at=obs['usable_from_utc'])
                old = previous.evaluate_coin(obs, btc_context=btc)
                _, context = build(obs, btc_context=btc)
                for asset in (None, context):
                    payload = evaluate(obs, asset, btc)
                    old_ids = {row['candidate_key'] for row in old['evaluations']}
                    self.assertEqual(len(payload['evaluations']), 316)
                    self.assertEqual([row for row in payload['evaluations'] if row['candidate_key'] in old_ids], old['evaluations'])
                    for base in adapter.DIRECTIONS:
                        for category in ('features', 'unavailable_features'):
                            for key, value in old['features_by_direction'][base][category].items():
                                self.assertEqual(payload['features_by_direction'][base][category][key], value)
                    self.assertEqual(payload['btc_context_provenance'], btc)
        payload = adapter.evaluate_coin(observation('ETH'))
        self.assertEqual(len(payload['evaluations']), 316)
        self.assertEqual(decision(payload)['match_status'], 'UNKNOWN')

    def test_flat_range_and_direction_are_distinct_and_json_roundtrip_is_stable(self):
        obs = observation('ETH')
        for close, direction in ((100, 'FLAT'), (100.5, 'UP'), (99.5, 'DOWN')):
            btc, context = build(obs, asset_path(obs, close=close, high=110, low=90))
            features = context['features_by_direction']['LONG']
            self.assertEqual(features[adapter.PREFIX+'1h.direction'], direction)
            self.assertEqual(features[adapter.PREFIX+'4h.market_regime'], 'RANGE')
            payload = evaluate(obs, context, btc)
            self.assertEqual(evaluate(obs, json.loads(adapter.canonical(context)), json.loads(adapter.canonical(btc))), payload)
            self.assertEqual(payload['feature_sha256'], adapter.digest({key: value for key, value in payload.items()
                if key != 'feature_sha256'}))


if __name__ == '__main__':
    unittest.main()
