"""Inclusive dual-CVD agreement, source integrity and deduplicated identity."""
import ast
from copy import deepcopy
from datetime import timedelta
import hashlib
import json
from pathlib import Path
from typing import Any
import unittest

import dual_cvd65_alert as detector
from research_watch_scan_formula_selftest import observation


def fixture():
    sample = observation()
    now = sample['usable_from_utc']
    coins = {}
    for symbol in detector.SYMBOLS:
        raw = observation(symbol)
        for module in raw['models'].values():
            module.update(quality_status='PASS', freshness_status='FRESH')
        coins[symbol] = {'status': 'CAPTURED', **{key: raw[key]
            for key in ('models', 'sources', 'source_time_errors')}}
    result = {'version': detector.CAPTURE_VERSION, 'hash_version': detector.CAPTURE_HASH_VERSION,
        'population': detector.CAPTURE_POPULATION, 'status': 'COMPLETE',
        'cycle_id': 'shared-watch:test', 'computed_at_utc': sample['observed_at_utc'].isoformat(),
        'symbols_expected': list(detector.SYMBOLS), 'coins': coins}
    return rehash(result), now


def rehash(bundle):
    bundle['payload_sha256'] = detector._digest({key: value for key, value in bundle.items() if key != 'payload_sha256'})
    return bundle


def coin_result(bundle, now, symbol='BTC'):
    return next(row for row in detector.evaluate_bundle(bundle, now)['observations'] if row['symbol'] == symbol)


def set_scores(bundle, futures=65, spot=65, *, symbol='BTC', futures_direction=None, spot_direction=None):
    for module, value, direction in (('futures_flow', futures, futures_direction), ('spot_flow', spot, spot_direction)):
        bundle['coins'][symbol]['models'][module].update(score=value, direction=direction or
            ('BULLISH' if value > 0 else 'BEARISH' if value < 0 else 'NEUTRAL'))
    return rehash(bundle)


class DualCvdTests(unittest.TestCase):
    def test_existing_normal_definition_and_capture_hash_contract_are_exact(self):
        candidates = detector.source_features.catalog_records()
        match = next(row for row in candidates if row['candidate_key'] == detector.RULE_ID)
        self.assertEqual(match['orientation'], 'NORMAL')
        self.assertEqual(match['definition']['conditions'], [
            {'feature': 'futures_cvd.aligned_score', 'operator': '>=', 'value': 65},
            {'feature': 'spot_cvd.aligned_score', 'operator': '>=', 'value': 65}])
        # Execute only the actual producer's pure canonical functions; importing
        # its collection/scoring dependencies would defeat a pure selector test.
        tree = ast.parse((Path(__file__).parent/'research_watch_score_capture.py').read_text())
        names = {'_numeric_normalized', 'canonical', 'digest'}
        module = ast.Module(body=[node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names], type_ignores=[])
        environment = {'Any': Any, 'json': json, 'hashlib': hashlib}
        exec(compile(module, '<actual-capture-canonical>', 'exec'), environment)
        bundle, now = fixture()
        bundle['coins']['BTC']['models']['spot_flow']['score'] = 70.0
        bundle['payload_sha256'] = environment['digest']({key: value for key, value in bundle.items() if key != 'payload_sha256'})
        self.assertEqual(coin_result(bundle, now)['status'], 'MATCH')

    def test_inclusive65_both_directions_and_one_score_below(self):
        for futures, spot, expected, direction in ((65, 65, 'MATCH', 'LONG'), (-65, -65, 'MATCH', 'SHORT'),
                (64.9999, 70, 'NO_MATCH', None), (-70, -64.9999, 'NO_MATCH', None),
                (100, 100, 'MATCH', 'LONG'), (-100, -100, 'MATCH', 'SHORT')):
            bundle, now = fixture()
            result = coin_result(set_scores(bundle, futures, spot), now)
            self.assertEqual((result['status'], result['direction']), (expected, direction))
            self.assertEqual((result['futures_score'], result['spot_score']), (futures, spot))

    def test_opposed_scores_and_low_neutral_are_known_no_match(self):
        for futures, spot in ((70, -70), (-70, 70), (0, 0), (10, 10)):
            bundle, now = fixture()
            set_scores(bundle, futures, spot)
            if futures == spot == 10:
                set_scores(bundle, futures, spot, futures_direction='NEUTRAL', spot_direction='NEUTRAL')
            result = coin_result(bundle, now)
            self.assertEqual(result['status'], 'NO_MATCH')
            self.assertIsNone(result['direction'])
            self.assertIsNone(result['generation_key'])

    def test_high_neutral_missing_or_contradicting_direction_is_unknown(self):
        for direction in ('NEUTRAL', '', 'UNKNOWN', 'BEARISH'):
            bundle, now = fixture()
            bundle['coins']['BTC']['models']['spot_flow']['direction'] = direction
            self.assertEqual(coin_result(rehash(bundle), now)['status'], 'UNKNOWN')
        bundle, now = fixture()
        set_scores(bundle, -70, -70)
        bundle['coins']['BTC']['models']['futures_flow']['direction'] = 'BULLISH'
        self.assertEqual(coin_result(rehash(bundle), now)['status'], 'UNKNOWN')

    def test_unavailable_and_bad_quality_do_not_reset_even_if_other_score_low(self):
        changes = ({'available': False}, {'capture_status': 'UNAVAILABLE'}, {'quality_status': 'NO_DATA'},
            {'freshness_status': 'STALE'}, {'freshness_status': 'UNKNOWN'})
        for change in changes:
            bundle, now = fixture()
            set_scores(bundle, 1, 70)
            bundle['coins']['BTC']['models']['spot_flow'].update(change)
            self.assertEqual(coin_result(rehash(bundle), now)['status'], 'UNKNOWN')

    def test_warning_already_adjusted_score_is_not_penalized_again(self):
        bundle, now = fixture()
        set_scores(bundle, 65, 65)
        for name in ('futures_flow', 'spot_flow'):
            bundle['coins']['BTC']['models'][name]['quality_status'] = 'WARNING'
        result = coin_result(rehash(bundle), now)
        self.assertEqual((result['status'], result['futures_score'], result['spot_score']), ('MATCH', 65, 65))

    def test_exact30minute_freshness_and_stale_capture_flags_cannot_override_clock(self):
        bundle, _ = fixture()
        closed = detector._utc(bundle['coins']['BTC']['sources']['spot']['quality']['candle_close'])
        self.assertEqual(coin_result(bundle, closed+timedelta(minutes=30))['status'], 'MATCH')
        self.assertEqual(coin_result(bundle, closed+timedelta(minutes=30, microseconds=1))['status'], 'UNKNOWN')
        self.assertEqual(coin_result(bundle, closed+timedelta(hours=24))['status'], 'UNKNOWN')

    def test_future_candle_window_reference_and_observation_sources_are_unknown(self):
        for location in ('candle', 'window', 'observed', 'capture_error'):
            bundle, now = fixture()
            sources = bundle['coins']['BTC']['sources']
            future = (now+timedelta(minutes=1)).isoformat()
            if location == 'candle':
                sources['spot']['quality']['candle_close'] = future
            elif location == 'window':
                sources['spot']['window_references']['30m']['latest_time'] = future
            elif location == 'observed':
                sources['timing_observation']['cvd_observed_at_utc'] = future
            else:
                bundle['coins']['BTC']['source_time_errors'].append('spot/candle_close:FUTURE_TIME')
            self.assertEqual(coin_result(rehash(bundle), now)['status'], 'UNKNOWN')

    def test_invalid_bundle_hash_version_population_and_clock_fail_closed_all8(self):
        for change in ({'payload_sha256': 'a'*64}, {'version': 'wrong'}, {'population': 'delivered-only'},
                {'hash_version': 'wrong'}, {'computed_at_utc': 'no-time'}, {'cycle_id': ''}):
            bundle, now = fixture()
            bundle.update(change)
            if 'payload_sha256' not in change:
                rehash(bundle)
            result = detector.evaluate_bundle(bundle, now)
            self.assertEqual(result['counts'], {'MATCH': 0, 'NO_MATCH': 0, 'UNKNOWN': 8})
            self.assertIsNone(result['source_at_utc'])
            self.assertIsNone(result['bundle_sha256'])
        bundle, now = fixture()
        bundle['computed_at_utc'] = (now+timedelta(seconds=1)).isoformat()
        self.assertEqual(detector.evaluate_bundle(rehash(bundle), now)['counts']['UNKNOWN'], 8)
        self.assertEqual(detector.evaluate_bundle(None, now)['counts']['UNKNOWN'], 8)
        self.assertEqual(detector.evaluate_bundle(fixture()[0], now.replace(tzinfo=None))['counts']['UNKNOWN'], 8)

    def test_boolean_nonfinite_and_out_of_range_scores_never_match(self):
        for score in (True, '70', None, 101, -101):
            bundle, now = fixture()
            bundle['coins']['BTC']['models']['spot_flow']['score'] = score
            self.assertEqual(coin_result(rehash(bundle), now)['status'], 'UNKNOWN')
        for score in (float('nan'), float('inf'), -float('inf')):
            bundle, now = fixture()
            bundle['coins']['BTC']['models']['spot_flow']['score'] = score
            self.assertEqual(detector.evaluate_bundle(bundle, now)['counts']['UNKNOWN'], 8)

    def test_generation_identity_ignores_watch_bundle_and_score_but_tracks_direction_and_both_closes(self):
        bundle, now = fixture()
        original = coin_result(bundle, now)['generation_key']
        bundle['cycle_id'] = 'shared-watch:restart'
        set_scores(bundle, 80, 90)
        self.assertEqual(coin_result(bundle, now)['generation_key'], original)
        set_scores(bundle, -80, -90)
        self.assertNotEqual(coin_result(bundle, now)['generation_key'], original)
        set_scores(bundle, 80, 90)
        for family in ('futures', 'spot'):
            changed = deepcopy(bundle)
            closed = detector._utc(changed['coins']['BTC']['sources'][family]['quality']['candle_close'])
            changed['coins']['BTC']['sources'][family]['quality']['candle_close'] = (closed-timedelta(minutes=1)).isoformat()
            self.assertNotEqual(coin_result(rehash(changed), now)['generation_key'], original)

    def test_hype_and_missing_maxpain_or_oi_are_independent_of_dual_cvd(self):
        bundle, now = fixture()
        for coin in bundle['coins'].values():
            coin['models']['positioning'] = {'available': False}
            coin['sources'].pop('positioning')
            coin['status'] = 'PARTIAL'
            coin['source_time_errors'] = ['12h/price_fetched_at_utc:MISSING_TIME', 'positioning/oi_fetched_at:MISSING_TIME']
        bundle['status'] = 'PARTIAL'
        result = detector.evaluate_bundle(rehash(bundle), now)
        self.assertEqual(result['counts']['MATCH'], 8)
        self.assertEqual(coin_result(bundle, now, 'HYPE')['status'], 'MATCH')
        self.assertEqual(set(row['symbol'] for row in result['observations']), set(detector.SYMBOLS))

    def test_render_uses_direct_shared_direction_magnitude_and_experimental_wording(self):
        bundle, now = fixture()
        set_scores(bundle, -65, -75, symbol='ZEC')
        result = detector.evaluate_bundle(bundle, now)
        row = next(row for row in result['observations'] if row['symbol'] == 'ZEC')
        text = detector.render_message(row, result['source_at_utc'])
        self.assertTrue(text.startswith('<b>סף 2%</b>\n'))
        self.assertIn('ירידה — SHORT', text)
        self.assertIn('65/100', text)
        self.assertIn('75/100', text)
        self.assertIn('טרם הוכחה', text)
        self.assertIn('ולא הוראת מסחר', text)
        self.assertNotIn('Max Pain', text)
        self.assertNotIn('היפוך', text)
        with self.assertRaises(ValueError):
            detector.render_message({**row, 'status': 'UNKNOWN'}, result['source_at_utc'])
        for symbol in set(detector.SYMBOLS) - {'ZEC'}:
            with self.assertRaises(ValueError):
                detector.render_message({**row, 'symbol': symbol}, result['source_at_utc'])


if __name__ == '__main__':
    unittest.main()
