"""Own-price rollout, frozen BTC reuse and causal reads on disposable PostgreSQL.

Only the guarded TEST_DATABASE_URL fixture is used. Archive access is real SQL;
provider requests, outcome reads during evaluation and alert delivery are forbidden.
"""
from copy import deepcopy
from datetime import timedelta
import os
from pathlib import Path
import unittest
from unittest.mock import patch

import research_price_archive as archive
import research_watch_scan_formula as legacy
import research_watch_scan_formula_maxpain as maxpain
import research_watch_scan_formula_btc_context as btc
import research_watch_scan_formula_asset_context as asset
import research_watch_scan_formula_worker as worker
import research_watch_scan_formula_btc_context_postgres_selftest as btc_tests
import research_watch_scan_intake as intake
import research_watch_scan_measurement_worker as measurement_worker
from research_watch_score_capture_selftest import BASE


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'), 'Explicit local/CI PostgreSQL required')
class WatchScanAssetContextPostgresTests(unittest.TestCase):
    # Reuse guarded fixture helpers, never inherit earlier regression methods.
    connect = btc_tests.WatchScanBtcContextPostgresTests.connect
    drop_database = btc_tests.WatchScanBtcContextPostgresTests.drop_database
    consume_intake = btc_tests.WatchScanBtcContextPostgresTests.consume_intake
    seed_bars = btc_tests.WatchScanBtcContextPostgresTests.seed_bars
    seed_wave = btc_tests.WatchScanBtcContextPostgresTests.seed_wave
    formula_bundle = btc_tests.WatchScanBtcContextPostgresTests.formula_bundle
    persist_on = btc_tests.WatchScanBtcContextPostgresTests.persist_on
    persist = btc_tests.WatchScanBtcContextPostgresTests.persist
    samples = btc_tests.WatchScanBtcContextPostgresTests.samples
    evaluations = btc_tests.WatchScanBtcContextPostgresTests.evaluations
    active_version = btc_tests.WatchScanBtcContextPostgresTests.active_version
    prior_bars = btc_tests.WatchScanBtcContextPostgresTests.prior_bars
    boundary = staticmethod(btc_tests.WatchScanBtcContextPostgresTests.boundary)
    decision_map = staticmethod(btc_tests.WatchScanBtcContextPostgresTests.decision_map)

    def setUp(self):
        btc_tests.WatchScanBtcContextPostgresTests.setUp(self)
        with self.connect() as conn:
            conn.execute((Path(__file__).parent / 'migrations/051_watch_scan_asset_context_formulas.sql').read_text(),
                         prepare=False)

    def process(self, *, version=asset, limit=16, now=None):
        with patch.object(worker, 'formula', version), self.connect() as conn:
            return worker.process_page(conn, now=now or BASE + timedelta(days=2), limit=limit)

    def own_bars(self, *, symbols=('BTC', 'ETH'), delay_minutes=0, omit=()):
        """One day of monotonic Spot bars; final 240 match the BTC fixture."""
        boundary = self.boundary(delay_minutes)
        rows = []
        for minute in range(1440):
            if minute in omit:
                continue
            opened = boundary + timedelta(minutes=minute - 1440)
            first = 100. + (minute - 1200) / 100.
            last = first + 0.01
            rows.append(dict(open_time_utc=opened,
                close_time_utc=opened + timedelta(minutes=1, milliseconds=-1),
                open=first, high=last, low=first, close=last, volume=1.))
        with self.connect() as conn:
            for symbol in symbols:
                self.assertNotEqual(symbol, 'HYPE')
                archive.write_bars(conn, archive.BINANCE_SPOT, symbol, rows)
        return rows

    def previous_versions(self, cycle_id, *, block=None):
        snapshot_id = self.persist(cycle_id, block=block)
        self.assertEqual(self.consume_intake()['accepted'], 1)
        with patch.object(archive, 'read_path', side_effect=AssertionError('Legacy v1/v2 must not read prices')):
            self.assertEqual(self.process(version=legacy)['ready'], 8)
            self.assertEqual(self.process(version=maxpain)['ready'], 8)
        self.prior_bars()
        self.assertEqual(self.process(version=btc)['ready'], 8)
        self.assertEqual(self.active_version(), btc.VERSION)
        return snapshot_id

    def assert_previous_decisions(self, old, current):
        old_map, new_map = self.decision_map(old), self.decision_map(current)
        self.assertEqual({key: new_map[key] for key in old_map}, old_map)

    def test_partial_v4_keeps_v3_current_then_activates_and_preserves_all_old_payloads(self):
        self.previous_versions('asset-version-switch')
        self.own_bars()
        versions = (legacy.VERSION, maxpain.VERSION, btc.VERSION)
        before = {version: self.samples(version) for version in versions}
        current = self.evaluations()
        self.assertEqual(len(current), 8 * 212)
        result = self.process(limit=3)
        self.assertEqual((result['ready'], result['errors']), (3, 0))
        self.assertEqual(self.active_version(), btc.VERSION)
        self.assertCountEqual(self.evaluations(), current)
        self.assertEqual(sum(row['status'] == 'READY' for row in self.samples(asset.VERSION)), 3)
        result = self.process()
        self.assertEqual((result['ready'], result['errors']), (5, 0))
        self.assertEqual(self.active_version(), asset.VERSION)
        current = self.evaluations()
        self.assertEqual(len(current), 8 * 316)
        self.assertEqual({row['evaluation_version'] for row in current}, {asset.VERSION})
        self.assertEqual(len(self.evaluations(by_version=True)), 8 * (68 + 164 + 212 + 316))
        for version, old in before.items():
            self.assertEqual(self.samples(version), old)
        self.assert_previous_decisions(before[btc.VERSION], self.samples(asset.VERSION))
        with self.connect() as conn:
            catalogs = conn.execute('''SELECT evaluation_version,count(*) AS total,
                count(*) FILTER (WHERE supported) AS supported
                FROM research_watch_scan_formula_catalog GROUP BY evaluation_version''').fetchall()
            self.assertEqual({r['evaluation_version']: (r['total'], r['supported']) for r in catalogs},
                {legacy.VERSION: (298, 34), maxpain.VERSION: (298, 82),
                 btc.VERSION: (298, 106), asset.VERSION: (298, 158)})
            conn.execute((Path(__file__).parent / 'migrations/051_watch_scan_asset_context_formulas.sql').read_text(),
                         prepare=False)
        self.assertEqual(self.process(version=btc)['processed'], 0)
        self.assertEqual(self.active_version(), asset.VERSION)
        self.assertEqual(len(self.evaluations()), 2528)

    def test_repaired_archive_cannot_replace_previous_btc_context_or_create_false_self_relative_zero(self):
        ids = [self.persist('asset-frozen-btc-' + str(delay), delay_minutes=delay) for delay in (0, 3000)]
        self.assertEqual(self.consume_intake()['accepted'], 2)
        self.assertEqual(self.process(version=legacy)['ready'], 16)
        self.assertEqual(self.process(version=maxpain)['ready'], 16)
        self.prior_bars(omit=(10,))  # 1h known, 4h missing.
        self.prior_bars(delay_minutes=3000, omit=(239,))  # Both missing.
        now = BASE + timedelta(days=4)
        self.assertEqual(self.process(version=btc, now=now)['ready'], 16)
        previous = self.samples(btc.VERSION)
        frozen = {row['snapshot_set_id']: row['payload']['btc_context_provenance'] for row in previous}
        for delay in (0, 3000):
            self.own_bars(delay_minutes=delay)
        original_read = archive.read_path
        with patch.object(archive, 'read_path', wraps=original_read) as reader:
            result = self.process(now=now)
        self.assertEqual((result['ready'], result['errors']), (16, 0))
        # v4 may read own BTC 24h but must not rebuild the frozen shared 1h/4h.
        self.assertEqual(reader.call_count, 14)
        self.assertTrue(all(call.args[3] - call.args[2] == timedelta(minutes=1440, milliseconds=-1)
                            for call in reader.call_args_list))
        current = self.samples(asset.VERSION)
        self.assert_previous_decisions(previous, current)
        self.assertEqual(self.samples(btc.VERSION), previous)
        for row in current:
            payload = row['payload']
            self.assertEqual(payload['btc_context_provenance'], frozen[row['snapshot_set_id']])
            for base in ('LONG', 'SHORT'):
                evidence = payload['features_by_direction'][base]
                if row['symbol'] == 'BTC':
                    context = payload['asset_context_provenance']
                    for name in ('1h', '4h'):
                        self.assertEqual(context['windows'][name], frozen[row['snapshot_set_id']]['windows'][name])
                    self.assertNotIn('historical.closed_1m.4h.market_regime', evidence['features'])
                    feature = 'historical.closed_1m.1h.relative_strength_pct'
                    if frozen[row['snapshot_set_id']]['windows']['1h']['status'] == 'READY':
                        self.assertEqual(evidence['features'][feature], 0)
                    else:
                        self.assertNotIn(feature, evidence['features'])
                        self.assertIn(feature, evidence['unavailable_features'])
                if row['snapshot_set_id'] == ids[1]:
                    self.assertNotIn('historical.closed_1m.1h.relative_strength_pct', evidence['features'])
        self.assertEqual({row['payload']['btc_context_provenance']['context_sha256'] for row in current},
                         {context['context_sha256'] for context in frozen.values()})

    def test_bounded_spot_reads_exclude_hype_outcomes_and_current_or_future_extrema(self):
        self.persist('asset-causal-read-bound')
        self.consume_intake()
        spot_symbols = tuple(symbol for symbol in intake.capture.SYMBOLS if symbol != 'HYPE')
        self.own_bars(symbols=spot_symbols)
        original_read = archive.read_path
        calls = []

        def poisoned_path(route, symbol, start, end, **kwargs):
            calls.append((route, symbol, start, end))
            path = deepcopy(original_read(route, symbol, start, end, **kwargs))
            for minute in (0, 1):
                opened = end + timedelta(milliseconds=1, minutes=minute)
                path['candles'].append(dict(open_time_utc=opened,
                    close_time_utc=opened + timedelta(minutes=1, milliseconds=-1),
                    open=100., high=1000000., low=0.01, close=0.01, volume=1.))
            return path

        with patch.object(archive, 'read_path', side_effect=poisoned_path), self.connect() as conn:
            recording = btc_tests.RecordingConnection(conn)
            with patch.object(worker, 'formula', asset):
                result = worker.process_page(recording, now=BASE + timedelta(days=2))
        self.assertEqual((result['ready'], result['errors']), (8, 0))
        boundary = self.boundary()
        self.assertCountEqual(calls, [(archive.BINANCE_SPOT, 'BTC', boundary - timedelta(minutes=240),
            boundary - timedelta(milliseconds=1))] + [(archive.BINANCE_SPOT, symbol,
            boundary - timedelta(minutes=1440), boundary - timedelta(milliseconds=1)) for symbol in spot_symbols])
        sql = ' '.join(recording.sql).lower()
        for forbidden in ('research_watch_scan_measurements', 'research_events',
                          'research_event_btc_movements', 'research_watch_scan_formula_wave_outcomes'):
            self.assertNotIn(forbidden, sql)
        samples = self.samples(asset.VERSION)
        self.assertEqual(len({row['payload']['btc_context_provenance']['context_sha256'] for row in samples}), 1)
        for row in samples:
            if row['symbol'] == 'HYPE':
                continue
            own = row['payload']['asset_context_provenance']['windows']
            self.assertEqual(own['24h']['path_samples'], 1440)
            self.assertLess(own['24h']['high'], 103.)
            self.assertAlmostEqual(own['24h']['return_pct'], 100 * (102.4 / 88. - 1))
            for base in ('LONG', 'SHORT'):
                features = row['payload']['features_by_direction'][base]['features']
                self.assertEqual(features['historical.closed_1m.1h.direction'], 'UP')
                self.assertEqual(features['historical.closed_1m.4h.market_regime'], 'UP')
                self.assertAlmostEqual(features['historical.closed_1m.1h.relative_strength_pct'], 0.)

    def test_hype_own_history_is_unknown_but_captured_spot_and_known_false_conjuncts_survive(self):
        block = self.formula_bundle('asset-hype', scores={'HYPE': 64.})
        self.previous_versions('asset-hype', block=block)
        before = self.samples(btc.VERSION)
        self.own_bars()
        self.assertEqual(self.process()['ready'], 8)
        after = self.samples(asset.VERSION)
        self.assert_previous_decisions(before, after)
        hype = next(row['payload'] for row in after if row['symbol'] == 'HYPE')
        new_features = asset.SUPPORTED_FEATURES - btc.SUPPORTED_FEATURES
        for base, score in (('LONG', 64.), ('SHORT', -64.)):
            evidence = hype['features_by_direction'][base]
            self.assertEqual(evidence['features']['spot_cvd.aligned_score'], score)
            self.assertTrue(new_features.isdisjoint(evidence['features']))
            self.assertTrue(new_features.issubset(evidence['unavailable_features']))
        candidates = {row['candidate_key']: row for row in asset.catalog_records() if row['supported']}
        relevant = []
        for evaluation in hype['evaluations']:
            conditions = legacy.existing._conditions(candidates[evaluation['candidate_key']]['definition'])
            has_new = any(item['feature'] in new_features for item in conditions)
            has_spot = any(item['feature'] == 'spot_cvd.aligned_score' for item in conditions)
            if evaluation['base_direction'] == 'LONG' and has_new and has_spot:
                relevant.append(evaluation)
        self.assertTrue(relevant)
        self.assertTrue(all(row['match_status'] == 'NO_MATCH' and row['missing_features'] for row in relevant))
        self.assertTrue(any(row['match_status'] == 'UNKNOWN' for row in hype['evaluations']))
        # Future outcomes remain a separate operation using HYPE PERP TRADE.
        self.seed_wave()
        self.seed_bars(symbols=('HYPE',), count=1440)
        with self.connect() as conn:
            self.assertEqual(measurement_worker.process_page(conn, now=BASE + timedelta(days=2))['ready'], 1)
            outcome = conn.execute("SELECT payload FROM research_watch_scan_measurements WHERE symbol='HYPE'").fetchone()['payload']
            self.assertEqual(outcome['price_route'], archive.HYPERLIQUID_PERP)
            self.assertEqual(conn.execute('SELECT count(*) AS n FROM research_events').fetchone()['n'], 0)
        self.assertEqual(self.samples(asset.VERSION), after)

    def test_partial_own_history_keeps_independent_short_windows_and_never_turns_gaps_into_flat(self):
        self.persist('asset-partial-history')
        self.consume_intake()
        self.own_bars(symbols=('BTC', 'BNB'))
        self.own_bars(symbols=('ETH',), omit=(10,))
        self.own_bars(symbols=('SOL',), omit=(1439,))
        result = self.process()
        self.assertEqual((result['ready'], result['errors']), (8, 0))
        rows = {row['symbol']: row['payload'] for row in self.samples(asset.VERSION)}
        for base in ('LONG', 'SHORT'):
            eth = rows['ETH']['features_by_direction'][base]
            self.assertEqual(eth['features']['historical.closed_1m.1h.direction'], 'UP')
            self.assertEqual(eth['features']['historical.closed_1m.4h.market_regime'], 'UP')
            self.assertNotIn('historical.closed_1m.24h.direction', eth['features'])
            self.assertIn('historical.closed_1m.24h.direction', eth['unavailable_features'])
            sol = rows['SOL']['features_by_direction'][base]
            for key in asset.SUPPORTED_FEATURES - btc.SUPPORTED_FEATURES:
                self.assertNotIn(key, sol['features'])
                self.assertIn(key, sol['unavailable_features'])
            bnb = rows['BNB']['features_by_direction'][base]
            self.assertEqual(bnb['features']['historical.closed_1m.24h.direction'], 'UP')
        before = self.samples(asset.VERSION)
        self.own_bars(symbols=('ETH', 'SOL'))
        self.assertEqual(self.process(now=BASE + timedelta(days=2, minutes=6))['processed'], 0)
        self.assertEqual(self.samples(asset.VERSION), before)

    def test_real_own_price_sql_failure_isolated_per_coin_then_retry_and_legacy_reads_stay_bounded(self):
        self.previous_versions('asset-sql-error')
        self.own_bars()
        original_read = archive.read_path
        calls = []

        def fail_eth(route, symbol, start, end, **kwargs):
            calls.append((symbol, start, end))
            if symbol == 'ETH':
                kwargs['connection'].execute('SELECT 1 / 0')
            return original_read(route, symbol, start, end, **kwargs)

        with patch.object(archive, 'read_path', side_effect=fail_eth):
            result = self.process()
        self.assertEqual((result['ready'], result['errors']), (7, 1))
        self.assertEqual(len(calls), 7)
        self.assertEqual(self.active_version(), btc.VERSION)
        before = self.samples(asset.VERSION)
        failed = next(row for row in before if row['symbol'] == 'ETH')
        self.assertEqual((failed['status'], failed['last_error']), ('ERROR', 'DivisionByZero'))
        peers = [row for row in before if row['symbol'] != 'ETH']
        self.assertTrue(all(row['status'] == 'READY' for row in peers))
        self.assertEqual(self.process(now=BASE + timedelta(days=2, minutes=4))['processed'], 0)
        with patch.object(archive, 'read_path', wraps=original_read) as reader:
            result = self.process(now=BASE + timedelta(days=2, minutes=5))
        self.assertEqual((result['ready'], result['errors']), (1, 0))
        self.assertEqual(reader.call_count, 1)
        self.assertEqual(reader.call_args.args[:2], (archive.BINANCE_SPOT, 'ETH'))
        self.assertEqual(reader.call_args.args[3] - reader.call_args.args[2], timedelta(minutes=1440, milliseconds=-1))
        after = self.samples(asset.VERSION)
        self.assertEqual([row for row in after if row['symbol'] != 'ETH'], peers)
        self.assertEqual(self.active_version(), asset.VERSION)
        self.assertTrue(all(row['status'] == 'READY' and row['last_error'] is None for row in after))
        # An additional scan proves old adapters still run their original paths.
        self.persist('asset-old-adapters', delay_minutes=300)
        self.consume_intake()
        with patch.object(archive, 'read_path', side_effect=AssertionError('No v1/v2 price read')):
            self.assertEqual(self.process(version=legacy)['ready'], 8)
            self.assertEqual(self.process(version=maxpain)['ready'], 8)
        with patch.object(archive, 'read_path', wraps=original_read) as reader:
            self.assertEqual(self.process(version=btc)['ready'], 8)
        self.assertEqual(reader.call_count, 1)
        self.assertEqual(reader.call_args.args[:2], (archive.BINANCE_SPOT, 'BTC'))
        self.assertEqual(reader.call_args.args[3] - reader.call_args.args[2], timedelta(minutes=240, milliseconds=-1))
        self.assertEqual(self.active_version(), asset.VERSION)


if __name__ == '__main__':
    unittest.main()
