"""Network/database-free tests of causal all-Watch measurement semantics."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest

import research_btc_parent_movement as btc
import research_ordered_first_touch as ordered
import research_watch_scan_measurement as measurement

BASE = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
MINUTE = timedelta(minutes=1)
MS = timedelta(milliseconds=1)


def observation(symbol='BTC', *, usable=None):
    usable = usable or BASE + timedelta(seconds=18)
    return {'snapshot_set_id': 1, 'consumer_version': measurement.CONSUMER_VERSION,
        'population_version': measurement.POPULATION, 'symbol': symbol,
        'source_version': measurement.SOURCE_VERSION, 'bundle_sha256': 'a'*64,
        'parent_payload_sha256': 'b'*64, 'capture_phase': 'HISTORICAL_CAPTURE',
        'intake_status': 'ACCEPTED', 'observed_at_utc': usable - timedelta(seconds=6),
        'source_available_at_utc': usable - timedelta(seconds=3),
        'source_created_at_utc': usable, 'usable_from_utc': usable,
        'operational_price': 900.0}


def candle(opened, *, price=100.0, high=None, low=None, close=None):
    return {'open_time_utc': opened, 'close_time_utc': opened+MINUTE-MS,
            'open': price, 'high': price if high is None else high,
            'low': price if low is None else low, 'close': price if close is None else close}


def path(obs, count, *, mutate=None):
    symbol = obs['symbol']
    if symbol == 'HYPE':
        source = {'symbol': 'HYPE', 'pair': 'HYPE-PERP', 'exchange': 'hyperliquid',
            'market': 'perpetual', 'price_kind': 'TRADE', 'instrument': 'HYPE',
            'margin_currency': 'USDC', 'interval': '1m', 'interval_seconds': 60,
            'source_url': 'https://api.hyperliquid.xyz/info',
            'method_version': 'hyperliquid-hype-perp-trade-1m-v1',
            'provenance': 'OFFICIAL_HYPERLIQUID_HYPE_PERPETUAL_TRADE_CANDLES_1M'}
    else:
        source = {'symbol': symbol, 'pair': symbol+'USDT', 'exchange': 'binance',
                  'market': 'spot', 'interval': '1m', 'interval_seconds': 60, 'multiplier': 1.0}
    entry = measurement.entry_time(obs['usable_from_utc'])
    bars = [candle(entry+i*MINUTE) for i in range(count)]
    if mutate:
        mutate(bars)
    return {**source, 'archive_route': measurement.route_for_symbol(symbol),
            'candles': bars, 'complete': True, 'expected_candles': count}


def record(result, direction='LONG', window=60):
    return next(r for r in result['records'] if r['direction']==direction and r['window_minutes']==window)


def label(result, direction='LONG', window=60, bps=50):
    return next(r for r in record(result, direction, window)['labels'] if r['threshold_bps']==bps)


def parent_fixture(obs, *, eligible=True):
    decision = obs['usable_from_utc']
    opened = decision.replace(second=0, microsecond=0)-MINUTE
    return {'btc_parent_movement_id': 'c'*64, 'episode_policy_version': btc.POLICY_VERSION,
        'start_time_utc': BASE-timedelta(hours=1), 'end_time_utc': None,
        'confirmed_at_utc': BASE-timedelta(hours=1) if eligible else None,
        'evidence_eligible': eligible, 'price_source': btc.SOURCE,
        'observed_through_utc': BASE+timedelta(days=1)}, candle(opened)


class MeasurementTests(unittest.TestCase):
    def test_strict_next_minute_uses_route_open_after_durable_availability(self):
        for usable in (BASE, BASE+timedelta(seconds=18), BASE+timedelta(seconds=59, microseconds=999999)):
            with self.subTest(usable=usable):
                obs = observation(usable=usable)
                entry = BASE+MINUTE
                result = measurement.measure(obs, path(obs, 1), observed_at=entry+MINUTE)
                self.assertEqual(result['entry_time_utc'], entry)
                self.assertGreater(entry, usable)
                self.assertEqual(result['reference_price'], 100.0)
                self.assertEqual(result['source_observed_at_utc'], obs['observed_at_utc'])
                self.assertEqual(result['usable_from_utc'], usable)
                self.assertEqual(result['membership']['decision_time_utc'], usable)
                self.assertEqual(result['entry_version'], measurement.ENTRY_VERSION)
                self.assertNotIn('event_id', result['membership'])

    def test_unclosed_or_absent_entry_never_fabricates_a_price(self):
        obs = observation()
        entry = measurement.entry_time(obs['usable_from_utc'])
        pending = measurement.measure(obs, {}, observed_at=entry+timedelta(seconds=59))
        self.assertEqual(pending['status'], 'NOT_YET_ENTRY')
        self.assertIsNone(pending['reference_price'])
        missing = measurement.measure(obs, path(obs, 0), observed_at=entry+MINUTE)
        self.assertEqual(missing['status'], 'DATA_MISSING_ENTRY')
        self.assertFalse(missing['records'])
        later_only = path(obs, 2)
        later_only['candles'].pop(0)
        missing = measurement.measure(obs, later_only, observed_at=entry+2*MINUTE)
        self.assertEqual(missing['status'], 'DATA_MISSING_ENTRY')
        self.assertIsNone(missing['reference_price'])

    def test_reuses_exact_v7_labels_and_keeps_timeframe_multiplicity_out(self):
        obs = observation()
        entry = measurement.entry_time(obs['usable_from_utc'])
        supplied = path(obs, 60)
        supplied['candles'][1].update(high=100.7, close=100.5)
        result = measurement.measure(obs, supplied, observed_at=entry+60*MINUTE)
        self.assertEqual(len(result['records']), 8)
        self.assertEqual(sum(len(r['labels']) for r in result['records']), 64)
        expected = ordered.calculate_all_ordered_first_touch_outcomes(reference_price=100.0,
            direction='LONG', event_time=entry, candles=supplied['candles'],
            observation_closed=True, path_complete=True)
        for original, actual in zip(expected, record(result)['labels']):
            self.assertEqual(actual, {key: original[key] for key in actual})
        self.assertEqual(label(result)['status'], 'SUCCESS')
        self.assertEqual(label(result)['decision_time_utc'], entry+2*MINUTE-MS)
        self.assertEqual(label(result)['time_to_decision_seconds'], 119)
        self.assertEqual(label(result)['observed_from_utc'], entry)
        self.assertEqual(label(result)['observed_through_utc'], entry+2*MINUTE-MS)
        self.assertEqual(label(result, 'SHORT')['status'], 'FAILURE')
        self.assertFalse(any('timeframe' in r for r in result['records']))

    def test_short_window_matures_first_and_ready_result_never_changes(self):
        obs = observation()
        entry = measurement.entry_time(obs['usable_from_utc'])
        early = measurement.measure(obs, path(obs, 60), observed_at=entry+60*MINUTE)
        self.assertEqual(early['status'], 'OPEN')
        self.assertEqual(record(early, window=60)['status'], 'READY')
        self.assertEqual(record(early, window=240)['status'], 'OPEN')
        self.assertIsNone(record(early, window=240)['mfe_pct'])
        later_path = path(obs, 120)
        later_path['candles'][90].update(high=110.0, low=90.0)
        later = measurement.measure(obs, later_path, observed_at=entry+120*MINUTE+timedelta(seconds=40))
        self.assertEqual(record(early, window=60), record(later, window=60))

    def test_same_candle_both_is_ambiguous_in_both_directions(self):
        obs = observation()
        entry = measurement.entry_time(obs['usable_from_utc'])
        supplied = path(obs, 1)
        supplied['candles'][0].update(high=101.0, low=99.0)
        result = measurement.measure(obs, supplied, observed_at=entry+MINUTE)
        for direction in measurement.DIRECTIONS:
            outcome = label(result, direction)
            self.assertEqual(outcome['status'], 'UNRESOLVED')
            self.assertEqual(outcome['first_touch_side'], 'AMBIGUOUS')
            self.assertEqual(outcome['terminal_reason'], 'SAME_CANDLE_BOTH')
            self.assertIsNone(outcome['success'])

    def test_observed_zero_excursion_is_not_missing_or_infinite_asymmetry(self):
        obs = observation()
        entry = measurement.entry_time(obs['usable_from_utc'])
        result = measurement.measure(obs, path(obs, 1440), observed_at=entry+1440*MINUTE)
        self.assertEqual(result['status'], 'READY')
        self.assertEqual(result['membership']['membership_status'], 'BTC_DATA_MISSING')
        for item in result['records']:
            self.assertEqual((item['mfe_pct'], item['mae_pct']), (0.0, 0.0))
            self.assertIsNone(item['asymmetry_ratio'])
            self.assertEqual(item['asymmetry_status'], 'UNDEFINED_ZERO_MAE')
            self.assertTrue(all(o['terminal_reason']=='OBSERVATION_WINDOW_CLOSED_NO_TOUCH' for o in item['labels']))

    def test_gap_is_window_local_and_never_later_first_touch_evidence(self):
        obs = observation()
        entry = measurement.entry_time(obs['usable_from_utc'])
        supplied = path(obs, 120)
        supplied['candles'][90].update(high=102.1)
        supplied['candles'].pop(65)
        # A cached provider-wide complete flag is not authoritative.
        self.assertTrue(supplied['complete'])
        result = measurement.measure(obs, supplied, observed_at=entry+120*MINUTE)
        self.assertEqual(record(result, window=60)['status'], 'READY')
        self.assertEqual(record(result, window=240)['status'], 'DATA_MISSING')
        self.assertEqual(label(result, window=240)['status'], 'DATA_MISSING')
        self.assertIsNone(label(result, window=240)['success'])
        self.assertIsNone(record(result, window=240)['mfe_pct'])

    def test_malformed_duplicate_nan_zero_and_future_bars_fail_closed(self):
        obs = observation()
        entry = measurement.entry_time(obs['usable_from_utc'])
        mutations = (
            lambda p: p['candles'][0].update(open=float('nan')),
            lambda p: p['candles'][0].update(open=0),
            lambda p: p['candles'][0].update(high=99.0),
            lambda p: p['candles'][0].update(open=True),
            lambda p: p['candles'][0].update(open_time_utc=entry.replace(tzinfo=None)),
            lambda p: p['candles'].append(deepcopy(p['candles'][0])),
            lambda p: p['candles'].append(candle(entry+MINUTE)),
            lambda p: p['candles'].append(candle(entry-MINUTE)),
        )
        for index, mutate in enumerate(mutations):
            with self.subTest(index=index):
                supplied = path(obs, 1)
                mutate(supplied)
                result = measurement.measure(obs, supplied, observed_at=entry+MINUTE)
                self.assertEqual(result['status'], 'INVALID_PATH')
                self.assertFalse(result['records'])
                self.assertIsNone(result['reference_price'])

    def test_hype_perpetual_trade_contract_is_preserved_without_spot_fallback(self):
        obs = observation('HYPE')
        entry = measurement.entry_time(obs['usable_from_utc'])
        supplied = path(obs, 1)
        result = measurement.measure(obs, supplied, observed_at=entry+MINUTE)
        self.assertEqual(result['status'], 'OPEN')
        self.assertEqual(result['price_route'], measurement.HYPE_PERP)
        self.assertEqual(result['price_source']['market'], 'perpetual')
        self.assertEqual(result['price_source']['price_kind'], 'TRADE')
        self.assertEqual(result['price_source']['instrument'], 'HYPE')
        for key, wrong in (('market', 'spot'), ('price_kind', 'MARK'), ('instrument', '@107'),
                           ('archive_route', 'BINANCE_HYPE_FUTURES_MARK_1M'), ('pair', 'HYPE/USDT'),
                           ('interval_seconds', 60.0), ('symbol', 'BTC')):
            with self.subTest(key=key):
                bad = deepcopy(supplied)
                bad[key] = wrong
                rejected = measurement.measure(obs, bad, observed_at=entry+MINUTE)
                self.assertEqual(rejected['status'], 'INVALID_SOURCE')
                self.assertFalse(rejected['records'])

    def test_seven_spot_routes_reject_conflicting_instruments_or_mark_prices(self):
        for symbol in measurement.SPOT_SYMBOLS:
            obs = observation(symbol)
            entry = measurement.entry_time(obs['usable_from_utc'])
            self.assertEqual(measurement.measure(obs, path(obs, 1), observed_at=entry+MINUTE)['status'], 'OPEN')
        obs = observation()
        entry = measurement.entry_time(obs['usable_from_utc'])
        for key, wrong in (('instrument', '@107'), ('price_kind', 'MARK'), ('pair', 'ETHUSDT')):
            bad = path(obs, 1)
            bad[key] = wrong
            self.assertEqual(measurement.measure(obs, bad, observed_at=entry+MINUTE)['status'], 'INVALID_SOURCE')

    def test_btc_membership_is_causal_and_independent_of_price_measurement(self):
        obs = observation()
        entry = measurement.entry_time(obs['usable_from_utc'])
        parent, bar = parent_fixture(obs)
        good = measurement.measure(obs, path(obs, 60), observed_at=entry+60*MINUTE, parent=parent, btc_bar=bar)
        self.assertEqual(good['membership']['membership_status'], 'LIVE')
        self.assertEqual(good['membership']['btc_parent_movement_id'], parent['btc_parent_movement_id'])
        self.assertNotIn('event_id', good['membership'])
        # The parent's subsequently extended observed_through is irrelevant;
        # its established boundary and the historical BTC close are causal.
        self.assertGreater(parent['observed_through_utc'], obs['usable_from_utc'])
        for bad_parent, bad_bar in (
            ({**parent, 'confirmed_at_utc': obs['usable_from_utc']+MINUTE}, bar),
            (parent, candle(BASE)),
            ({**parent, 'price_source': 'FUTURE_GUESSED_SOURCE'}, bar),
            (parent, {**bar, 'high': float('nan')}),
        ):
            bad = measurement.measure(obs, path(obs, 60), observed_at=entry+60*MINUTE, parent=bad_parent, btc_bar=bad_bar)
            self.assertEqual(bad['membership']['membership_status'], 'BTC_DATA_MISSING')
            self.assertEqual(record(bad)['status'], 'READY')
            self.assertEqual(record(bad), record(good))
        unverified, bar = parent_fixture(obs, eligible=False)
        pending = measurement.measure(obs, path(obs, 60), observed_at=entry+60*MINUTE, parent=unverified, btc_bar=bar)
        self.assertEqual(pending['membership']['membership_status'], 'BOUNDARY_UNVERIFIED')
        self.assertEqual(record(pending)['status'], 'READY')

    def test_bad_observation_provenance_or_time_never_produces_labels(self):
        obs = observation()
        entry = measurement.entry_time(obs['usable_from_utc'])
        for key, wrong in (('bundle_sha256', 'not-a-hash'), ('source_version', 'watch-operational-scores-v1'),
                           ('intake_status', 'REJECTED'), ('population_version', 'delivered-alerts'),
                           ('usable_from_utc', BASE), ('observed_at_utc', entry), ('snapshot_set_id', True)):
            with self.subTest(key=key):
                invalid = {**obs, key: wrong}
                result = measurement.measure(invalid, path(obs, 1), observed_at=entry+MINUTE)
                self.assertEqual(result['status'], 'INVALID_OBSERVATION')
                self.assertFalse(result['records'])


if __name__ == '__main__':
    unittest.main()
