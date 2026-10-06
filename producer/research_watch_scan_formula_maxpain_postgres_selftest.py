"""Versioned MaxPain rollout and real producer parity on disposable PostgreSQL.

Only the guarded TEST_DATABASE_URL fixture is used. Legacy records, current
views, and all-version audit views are exercised without live network calls.
"""
from datetime import timedelta
import os
from pathlib import Path
import unittest
from unittest.mock import patch

import alert_engine
import research_watch_scan_formula as legacy
import research_watch_scan_formula_maxpain as maxpain
import research_watch_scan_formula_worker as worker
import research_watch_scan_formula_postgres_selftest as formula_tests
import research_watch_scan_measurement_postgres_selftest as measurement_tests
import research_watch_scan_measurement_worker as measurement_worker
import research_watch_scan_intake as intake
from research_watch_score_capture_selftest import BASE, bundle, inputs

CATALOG_PREFIX = 'captured-question-search-v3-experimental-binding:'
AVERAGE_65 = CATALOG_PREFIX + 'average_score_all_timeframes_GE65'
OPPOSITE_65 = CATALOG_PREFIX + 'opposite_average_score_all_timeframes_GE65'
CONSENSUS_TRUE = CATALOG_PREFIX + 'CONSENSUS_True'
CONSENSUS_FALSE = CATALOG_PREFIX + 'CONSENSUS_False'
INVERSE_AVERAGE_65 = CATALOG_PREFIX + 'INVERSE:' + AVERAGE_65


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'), 'Explicit local/CI PostgreSQL required')
class WatchScanMaxPainPostgresTests(unittest.TestCase):
    # Reuse fixture functions only; do not run legacy setUp's worker patch or
    # inherit any existing tests. Each test still owns a new isolated database.
    connect = measurement_tests.WatchScanMeasurementPostgresTests.connect
    drop_database = measurement_tests.WatchScanMeasurementPostgresTests.drop_database
    consume_intake = measurement_tests.WatchScanMeasurementPostgresTests.consume_intake
    seed_bars = measurement_tests.WatchScanMeasurementPostgresTests.seed_bars
    seed_wave = measurement_tests.WatchScanMeasurementPostgresTests.seed_wave
    formula_bundle = formula_tests.WatchScanFormulaPostgresTests.formula_bundle
    persist_on = formula_tests.WatchScanFormulaPostgresTests.persist_on
    persist = formula_tests.WatchScanFormulaPostgresTests.persist

    def setUp(self):
        measurement_tests.WatchScanMeasurementPostgresTests.setUp(self)
        with self.connect() as conn:
            for name in ('048_watch_scan_formulas.sql', '049_watch_scan_maxpain_formulas.sql'):
                conn.execute((Path(__file__).parent / 'migrations' / name).read_text(), prepare=False)

    def process(self, *, version=None, limit=16):
        with patch.object(worker, 'formula', version or maxpain), self.connect() as conn:
            return worker.process_page(conn, now=BASE + timedelta(days=2), limit=limit)

    def samples(self, version):
        with self.connect() as conn:
            return conn.execute('''SELECT * FROM research_watch_scan_formula_samples
                WHERE evaluation_version=%s ORDER BY snapshot_set_id,symbol''', (version,)).fetchall()

    def active_version(self):
        with self.connect() as conn:
            return conn.execute('''SELECT active_evaluation_version FROM research_watch_scan_formula_runtime
                WHERE singleton=true''').fetchone()['active_evaluation_version']

    def evaluations(self, *, by_version=False):
        view = 'research_watch_scan_formula_evaluations' + ('_by_version' if by_version else '')
        with self.connect() as conn:
            return conn.execute('SELECT * FROM ' + view).fetchall()

    def seed_legacy(self, cycle_id):
        snapshot_id = self.persist(cycle_id)
        self.assertEqual(self.consume_intake()['accepted'], 1)
        result = self.process(version=legacy)
        self.assertEqual((result['ready'], result['errors']), (8, 0))
        self.assertEqual(self.active_version(), legacy.VERSION)
        return snapshot_id

    def test_partial_v2_keeps_legacy_current_then_atomically_switches_without_mixing(self):
        self.seed_legacy('version-switch')
        before = self.samples(legacy.VERSION)
        legacy_evaluations = self.evaluations()
        self.assertEqual(len(legacy_evaluations), 8 * 68)
        self.assertTrue(all(len(s['payload']['evaluations']) == 68 for s in before))
        self.seed_wave()
        self.seed_bars(symbols=intake.capture.SYMBOLS, count=1440)
        with self.connect() as conn:
            self.assertEqual(measurement_worker.process_page(conn, now=BASE + timedelta(days=2))['ready'], 8)
            old_comparison = conn.execute('''SELECT * FROM research_watch_scan_formula_comparisons
                WHERE candidate_key='FUTURES_CVD_TOTAL_65' AND symbol_scope='ALL'
                    AND base_direction='LONG' ORDER BY window_minutes,threshold_bps''').fetchall()
        self.assertEqual(len(old_comparison), 32)

        partial = self.process(limit=3)
        self.assertEqual((partial['ready'], partial['errors']), (3, 0))
        self.assertEqual(self.active_version(), legacy.VERSION)
        self.assertEqual(len(self.evaluations()), 544)
        self.assertEqual(self.samples(legacy.VERSION), before)
        self.assertEqual(sum(s['status'] == 'READY' for s in self.samples(maxpain.VERSION)), 3)
        with self.connect() as conn:
            current = conn.execute('''SELECT * FROM research_watch_scan_formula_comparisons
                WHERE candidate_key='FUTURES_CVD_TOTAL_65' AND symbol_scope='ALL'
                    AND base_direction='LONG' ORDER BY window_minutes,threshold_bps''').fetchall()
        self.assertEqual(current, old_comparison)

        self.assertEqual(self.process()['ready'], 5)
        self.assertEqual(self.active_version(), maxpain.VERSION)
        current = self.evaluations()
        self.assertEqual(len(current), 8 * 164)
        self.assertEqual({e['evaluation_version'] for e in current}, {maxpain.VERSION})
        self.assertEqual(self.samples(legacy.VERSION), before)
        audit = self.evaluations(by_version=True)
        self.assertEqual(len(audit), 8 * (68 + 164))
        self.assertEqual({e['evaluation_version'] for e in audit}, {legacy.VERSION, maxpain.VERSION})
        old_map = {(e['symbol'], e['candidate_key'], e['base_direction']):
                   (e['match_status'], e['analysis_direction']) for e in legacy_evaluations}
        current_map = {(e['symbol'], e['candidate_key'], e['base_direction']):
                       (e['match_status'], e['analysis_direction']) for e in current}
        self.assertEqual({k: current_map[k] for k in old_map}, old_map)
        with self.connect() as conn:
            catalogs = conn.execute('''SELECT evaluation_version,count(*) AS total,
                count(*) FILTER (WHERE supported) AS supported
                FROM research_watch_scan_formula_catalog GROUP BY evaluation_version''').fetchall()
            self.assertEqual({r['evaluation_version']: (r['total'], r['supported']) for r in catalogs},
                             {legacy.VERSION: (298, 34), maxpain.VERSION: (298, 82)})
            comparisons = conn.execute('''SELECT * FROM research_watch_scan_formula_comparisons
                WHERE candidate_key='FUTURES_CVD_TOTAL_65' AND symbol_scope='ALL'
                    AND base_direction='LONG' ''').fetchall()
            self.assertEqual(len(comparisons), 32)
            self.assertTrue(all(r['evaluation_version'] == maxpain.VERSION and r['distinct_waves'] == 1
                                and r['matched_waves'] == 1 and r['matched_success'] == 1 for r in comparisons))
            hype = conn.execute("SELECT DISTINCT price_route FROM research_watch_scan_formula_wave_anchors WHERE symbol='HYPE'").fetchall()
            self.assertEqual(hype, [{'price_route': 'HYPERLIQUID_HYPE_PERP_TRADE_1M'}])
            self.assertEqual(conn.execute('SELECT count(*) AS n FROM research_events').fetchone()['n'], 0)
            conn.execute((Path(__file__).parent / 'migrations/049_watch_scan_maxpain_formulas.sql').read_text(), prepare=False)
        self.assertEqual(self.active_version(), maxpain.VERSION)
        self.assertEqual(self.samples(legacy.VERSION), before)
        self.assertEqual(self.process(version=legacy)['processed'], 0)
        self.assertEqual(self.active_version(), maxpain.VERSION)
        self.assertEqual(len(self.evaluations()), 1312)

    def test_activation_and_last_sample_roll_back_together_then_restart_finishes(self):
        self.seed_legacy('activation-rollback')
        legacy_before = self.samples(legacy.VERSION)
        self.assertEqual(self.process(limit=7)['ready'], 7)
        before = self.samples(maxpain.VERSION)
        self.assertEqual(self.active_version(), legacy.VERSION)
        with self.connect() as conn, patch.object(worker, 'formula', maxpain):
            with self.assertRaisesRegex(RuntimeError, 'after active version switch'):
                with conn.transaction():
                    result = worker.process_page(conn, now=BASE + timedelta(days=2), limit=1)
                    self.assertEqual(result['ready'], 1)
                    active = conn.execute('''SELECT active_evaluation_version FROM research_watch_scan_formula_runtime
                        WHERE singleton=true''').fetchone()['active_evaluation_version']
                    self.assertEqual(active, maxpain.VERSION)
                    raise RuntimeError('fixture after active version switch before commit')
        self.assertEqual(self.active_version(), legacy.VERSION)
        self.assertEqual(self.samples(maxpain.VERSION), before)
        self.assertEqual(self.samples(legacy.VERSION), legacy_before)
        self.assertEqual(len(self.evaluations()), 544)
        self.assertEqual(self.process(limit=1)['ready'], 1)
        self.assertEqual(self.active_version(), maxpain.VERSION)
        self.assertEqual(len(self.evaluations()), 1312)

    def test_accepted_scan_not_yet_enqueued_blocks_activation_even_when_queue_is_ready(self):
        ids = [self.persist('unenqueued-' + str(i)) for i in range(4)]
        self.assertEqual(self.consume_intake()['accepted'], 4)
        self.assertEqual(self.process(version=legacy)['ready'], 16)
        self.assertEqual(self.process(version=legacy)['ready'], 16)
        legacy_before = self.samples(legacy.VERSION)
        with self.connect() as conn, patch.object(worker, 'formula', maxpain), patch.object(worker, 'SCAN_PAGE', 1):
            worker.register_catalog(conn)
            # The first finite page and two recent IDs intentionally leave
            # the second accepted source outside the currently enqueued jobs.
            self.assertEqual(worker.enqueue(conn, now=BASE + timedelta(days=2)), 24)
        queued = self.samples(maxpain.VERSION)
        self.assertEqual({s['snapshot_set_id'] for s in queued}, {ids[0], ids[2], ids[3]})
        with patch.object(worker, 'enqueue', return_value=0):
            self.assertEqual(self.process()['ready'], 16)
            self.assertEqual(self.process()['ready'], 8)
        self.assertTrue(all(s['status'] == 'READY' for s in self.samples(maxpain.VERSION)))
        self.assertEqual(self.active_version(), legacy.VERSION)
        self.assertEqual(len(self.evaluations()), 4 * 544)
        self.assertEqual(self.samples(legacy.VERSION), legacy_before)
        # Resume normal durable discovery; the accepted gap is then filled.
        self.assertEqual(self.process()['ready'], 8)
        self.assertEqual(self.active_version(), maxpain.VERSION)
        self.assertEqual(len(self.evaluations()), 4 * 1312)

    def test_exact_real_producer_averages_consensus_and_inactive_denominator_survive_jsonb(self):
        rows = inputs()
        for row in rows:
            if row['symbol'] == 'HYPE':
                row.update(price_source='binance_futures', price_pair='HYPEUSDT',
                           price_market='PERP', price_instrument='HYPEUSDT')
            if row['symbol'] == 'BTC' and row['timeframe'] == '12h':
                row.update(long_max_pain=105., distance_long_pct=5.)
        expected = alert_engine.build_opportunities(rows, limit=500)
        block, _, frozen, _ = bundle(rows=rows, cycle_id='real-maxpain-parity')
        self.persist('real-maxpain-parity', block=block)
        self.assertEqual(self.consume_intake()['accepted'], 1)
        # Compute expected behavior using the real producer above. The reader
        # below must use committed frozen slots and never rerun the scorer.
        with patch.object(alert_engine, 'build_opportunities', side_effect=AssertionError('No score replay')):
            result = self.process()
        self.assertEqual((result['ready'], result['errors']), (8, 0))
        samples = {s['symbol']: s['payload'] for s in self.samples(maxpain.VERSION)}
        self.assertEqual(set(samples), set(intake.capture.SYMBOLS))
        for item in expected:
            base = 'SHORT' if item['side'] == 'LONG' else 'LONG'
            features = samples[item['symbol']]['features_by_direction'][base]['features']
            self.assertEqual(features[maxpain.AVERAGE], item['average_score_all_timeframes'])
            self.assertEqual(features[maxpain.OPPOSITE_AVERAGE], item['opposite_average_score_all_timeframes'])
            self.assertEqual(features[maxpain.CONSENSUS], item['consensus_hits'] == item['consensus_total'])

        for symbol, payload in samples.items():
            representative = next(item for item in expected if item['symbol'] == symbol)
            for base, side in (('LONG', 'SHORT'), ('SHORT', 'LONG')):
                features = payload['features_by_direction'][base]['features']
                provenance = payload['maxpain_provenance_by_direction'][base]
                scored = [slot for slot in frozen[symbol] if slot['source_side'] == side]
                self.assertEqual(provenance['validation_status'], 'VALID')
                self.assertEqual(provenance['source_side'], side)
                self.assertEqual(provenance['denominator'], len(scored))
                self.assertEqual(features[maxpain.AVERAGE], representative['average_score_' + side.lower()])
                self.assertEqual(provenance['timeframe_values'], {slot['timeframe']: slot['score'] for slot in scored})
                self.assertEqual(features[maxpain.CONSENSUS], scored[0]['consensus_hits'] == scored[0]['consensus_total'])
        inactive = samples['BTC']['maxpain_provenance_by_direction']['SHORT']
        self.assertEqual(inactive['denominator'], 6)
        self.assertEqual(inactive['inactive_timeframes'], ['12h'])
        self.assertEqual(inactive['missing_timeframes'], [])
        self.assertNotIn('12h', inactive['timeframe_values'])
        self.assertEqual(samples['BTC']['maxpain_provenance_by_direction']['LONG']['denominator'], 7)
        evaluations = {(e['symbol'], e['candidate_key'], e['base_direction']): e for e in self.evaluations()}
        for symbol in intake.capture.SYMBOLS:
            for base in ('LONG', 'SHORT'):
                normal = evaluations[symbol, AVERAGE_65, base]
                inverse = evaluations[symbol, INVERSE_AVERAGE_65, base]
                self.assertEqual(normal['match_status'], inverse['match_status'])
                self.assertIn(normal['match_status'], {'MATCH', 'NO_MATCH'})
                self.assertEqual(normal['analysis_direction'], base)
                self.assertEqual(inverse['analysis_direction'], 'SHORT' if base == 'LONG' else 'LONG')
        with self.connect() as conn:
            self.assertEqual(conn.execute('SELECT count(*) AS n FROM research_watch_scan_measurements').fetchone()['n'], 0)
            self.assertEqual(conn.execute('SELECT count(*) AS n FROM research_events').fetchone()['n'], 0)

    def test_missing_real_timeframe_stays_unknown_instead_of_zero_or_control(self):
        rows = [r for r in inputs() if not (r['symbol'] == 'ETH' and r['timeframe'] == '12h')]
        block, *_ = bundle(rows=rows, cycle_id='missing-maxpain-tf')
        missing = [slot for slot in block['coins']['ETH']['maxpain'] if slot['timeframe'] == '12h']
        self.assertEqual(len(missing), 2)
        self.assertTrue(all(slot['status'] == 'MISSING_INPUT' and slot['score'] is None for slot in missing))
        self.persist('missing-maxpain-tf', block=block)
        self.assertEqual(self.consume_intake()['accepted'], 1)
        self.assertEqual(self.process()['ready'], 8)
        samples = {s['symbol']: s['payload'] for s in self.samples(maxpain.VERSION)}
        for base in ('LONG', 'SHORT'):
            evidence = samples['ETH']['features_by_direction'][base]
            for feature in (maxpain.MAPPING, maxpain.AVERAGE, maxpain.OPPOSITE_AVERAGE, maxpain.CONSENSUS):
                self.assertNotIn(feature, evidence['features'])
                self.assertIn(feature, evidence['unavailable_features'])
            provenance = samples['ETH']['maxpain_provenance_by_direction'][base]
            self.assertEqual(provenance['validation_status'], 'UNAVAILABLE')
            self.assertEqual(provenance['missing_timeframes'], ['12h'])
        selected_keys = {AVERAGE_65, OPPOSITE_65, CONSENSUS_TRUE, CONSENSUS_FALSE, INVERSE_AVERAGE_65}
        affected = [e for e in self.evaluations() if e['symbol'] == 'ETH' and e['candidate_key'] in selected_keys]
        self.assertEqual(len(affected), len(selected_keys) * 2)
        self.assertTrue(all(e['match_status'] == 'UNKNOWN' for e in affected))
        peers = [e for e in self.evaluations() if e['symbol'] == 'BTC' and e['candidate_key'] in selected_keys]
        self.assertEqual(len(peers), len(selected_keys) * 2)
        self.assertTrue(all(e['match_status'] in {'MATCH', 'NO_MATCH'} for e in peers))


if __name__ == '__main__':
    unittest.main()
