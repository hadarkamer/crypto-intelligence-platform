"""All-scan formula semantics and durable recovery on disposable local/CI PostgreSQL.

Only TEST_DATABASE_URL is accepted. Reuse fixture helpers, never old test
methods; no external price provider, production database, or message sender.
"""
from copy import deepcopy
from datetime import timedelta
import os
from pathlib import Path
import unittest
from unittest.mock import patch

import research_watch_scan_formula_worker as worker
import research_watch_scan_measurement_worker as measurement_worker
import research_watch_scan_measurement_postgres_selftest as measurement_tests
import research_watch_scan_intake as intake
from research_watch_scan_intake_selftest import rehash
from research_watch_score_capture_selftest import BASE, bundle, inputs

FUTURES = 'FUTURES_CVD_TOTAL_65'
SPOT = 'SPOT_CVD_TOTAL_65'
TRIPLE = 'STRICT_TRIPLE_TOTAL_65'
INVERSE_FUTURES = 'captured-question-search-v3-experimental-binding:INVERSE:FUTURES_CVD_TOTAL_65'


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'), 'Explicit local/CI PostgreSQL required')
class WatchScanFormulaPostgresTests(unittest.TestCase):
    connect = measurement_tests.WatchScanMeasurementPostgresTests.connect
    drop_database = measurement_tests.WatchScanMeasurementPostgresTests.drop_database
    consume_intake = measurement_tests.WatchScanMeasurementPostgresTests.consume_intake
    seed_bars = measurement_tests.WatchScanMeasurementPostgresTests.seed_bars
    seed_wave = measurement_tests.WatchScanMeasurementPostgresTests.seed_wave

    def setUp(self):
        measurement_tests.WatchScanMeasurementPostgresTests.setUp(self)
        with self.connect() as conn:
            conn.execute((Path(__file__).parent / 'migrations/048_watch_scan_formulas.sql').read_text(), prepare=False)

    def formula_bundle(self, cycle_id, *, score=70., unknown=(), scores=None):
        rows = inputs()
        for row in rows:
            if row['symbol'] == 'HYPE':
                row.update(price_source='binance_futures', price_pair='HYPEUSDT',
                           price_market='PERP', price_instrument='HYPEUSDT')
        block, *_ = bundle(rows=rows, cycle_id=cycle_id)
        for symbol, coin in block['coins'].items():
            value = (scores or {}).get(symbol, score)
            for key in ('positioning', 'futures_flow', 'spot_flow'):
                missing = (symbol, key) in unknown
                coin['models'][key].update(available=not missing,
                    capture_status='UNAVAILABLE' if missing else 'AVAILABLE',
                    score=0. if missing else value,
                    direction='BULLISH' if value > 0 else 'BEARISH' if value < 0 else 'NEUTRAL')
            coin['sources']['positioning'].update(price_fetched_at=BASE.isoformat(), oi_fetched_at=BASE.isoformat())
            for key in ('futures', 'spot'):
                coin['sources'][key]['quality']['candle_close'] = BASE.isoformat()
            coin['sources']['timing_observation']['cvd_observed_at_utc'] = (BASE + timedelta(minutes=4)).isoformat()
            coin['source_time_errors'] = []
        return rehash(block)

    def persist_on(self, conn, cycle_id, *, block=None, delay_minutes=0):
        # Keep the actual captured observation time fixed; a later initial
        # source commit makes availability later without altering frozen data.
        with patch.object(measurement_tests, 'BASE', BASE + timedelta(minutes=delay_minutes)):
            return measurement_tests.WatchScanMeasurementPostgresTests.persist_on(self, conn, cycle_id,
                block=block if block is not None else self.formula_bundle(cycle_id))

    def persist(self, cycle_id, **kwargs):
        with self.connect() as conn:
            return self.persist_on(conn, cycle_id, **kwargs)

    def process(self, *, now=None, limit=16):
        with self.connect() as conn:
            return worker.process_page(conn, now=now or BASE + timedelta(days=2), limit=limit)

    def measure(self, *, now=None):
        with self.connect() as conn:
            return measurement_worker.process_page(conn, now=now or BASE + timedelta(days=2))

    def samples(self):
        with self.connect() as conn:
            return conn.execute('SELECT * FROM research_watch_scan_formula_samples ORDER BY snapshot_set_id,symbol').fetchall()

    def evaluations(self, candidate_key=None):
        with self.connect() as conn:
            if candidate_key is None:
                return conn.execute('SELECT * FROM research_watch_scan_formula_evaluations').fetchall()
            return conn.execute('SELECT * FROM research_watch_scan_formula_evaluations WHERE candidate_key=%s',
                                (candidate_key,)).fetchall()

    def seed_later_btc_bar(self, delay_minutes):
        opened = BASE + timedelta(minutes=6 + delay_minutes)
        with self.connect() as conn:
            conn.execute('''INSERT INTO research_btc_price_bars
                (open_time_utc,close_time_utc,open,high,low,close)
                VALUES (%s,%s,100,101,99,100)''',
                (opened, opened + timedelta(minutes=1, milliseconds=-1)))

    def anchors(self, *, candidate_key=FUTURES, arm='MATCH'):
        with self.connect() as conn:
            return conn.execute('''SELECT * FROM research_watch_scan_formula_wave_anchors
                WHERE candidate_key=%s AND symbol_scope='ALL' AND base_direction='LONG' AND match_status=%s
                ORDER BY snapshot_set_id,symbol''', (candidate_key, arm)).fetchall()

    def wave_outcomes(self, *, candidate_key=FUTURES, arm='MATCH'):
        with self.connect() as conn:
            return conn.execute('''SELECT * FROM research_watch_scan_formula_wave_outcomes
                WHERE candidate_key=%s AND symbol_scope='ALL' AND base_direction='LONG' AND match_status=%s
                ORDER BY window_minutes,threshold_bps''', (candidate_key, arm)).fetchall()

    def test_catalog_is_complete_versioned_and_immutable(self):
        with self.connect() as conn:
            self.assertEqual(worker.register_catalog(conn), 298)
            counts = conn.execute('''SELECT count(*) AS total,
                count(*) FILTER (WHERE supported) AS supported,
                count(*) FILTER (WHERE NOT supported) AS unsupported
                FROM research_watch_scan_formula_catalog''').fetchone()
            self.assertEqual(counts, {'total': 298, 'supported': 34, 'unsupported': 264})
            self.assertEqual(worker.register_catalog(conn), 298)
        definitions = deepcopy(worker.formula.catalog_records())
        definitions[0]['definition_sha256'] = 'f' * 64
        with self.connect() as conn, patch.object(worker.formula, 'catalog_records', return_value=definitions):
            with self.assertRaisesRegex(ValueError, 'changed without a new version'):
                worker.register_catalog(conn)
        with self.connect() as conn:
            with self.assertRaises(self.psycopg.Error):
                with conn.transaction():
                    conn.execute("UPDATE research_watch_scan_formula_catalog SET definition=jsonb_set(definition,'{fixture_mutation}','true') WHERE candidate_key=%s", (FUTURES,))
            self.assertEqual(conn.execute('SELECT count(*) AS n FROM research_watch_scan_formula_catalog').fetchone()['n'], 298)

    def test_all_eight_coins_evaluate_before_prices_without_timeframe_multiplication(self):
        block = self.formula_bundle('formula-all-coins', unknown={('HYPE', 'spot_flow')}, scores={'SOL': 64.})
        snapshot_id = self.persist('formula-all-coins', block=block)
        self.assertEqual(self.consume_intake()['accepted'], 1)
        first = self.process()
        self.assertEqual((first['enqueued'], first['processed'], first['ready'], first['errors']), (8, 8, 8, 0))
        before = self.samples()
        self.assertEqual(len(before), 8)
        evaluations = self.evaluations()
        self.assertEqual(len(evaluations), 8 * 34 * 2)
        self.assertEqual(len({(e['snapshot_set_id'], e['symbol'], e['candidate_key'], e['base_direction'])
                             for e in evaluations}), 544)
        self.assertTrue(all(e['snapshot_set_id'] == snapshot_id for e in evaluations))
        futures = {(e['symbol'], e['base_direction']): e for e in self.evaluations(FUTURES)}
        inverse = {(e['symbol'], e['base_direction']): e for e in self.evaluations(INVERSE_FUTURES)}
        self.assertEqual(len(futures), 16)
        self.assertEqual(set(inverse), set(futures))
        for key, normal in futures.items():
            self.assertEqual(normal['match_status'], inverse[key]['match_status'])
            self.assertEqual(normal['analysis_direction'], normal['base_direction'])
            self.assertEqual(inverse[key]['analysis_direction'], 'SHORT' if normal['base_direction'] == 'LONG' else 'LONG')
        self.assertEqual(futures['BTC', 'LONG']['match_status'], 'MATCH')
        self.assertEqual(futures['BTC', 'SHORT']['match_status'], 'NO_MATCH')
        self.assertEqual(futures['SOL', 'LONG']['match_status'], 'NO_MATCH')
        hype_spot = [e for e in self.evaluations(SPOT) if e['symbol'] == 'HYPE']
        self.assertEqual(len(hype_spot), 2)
        self.assertTrue(all(e['match_status'] == 'UNKNOWN' for e in hype_spot))
        with self.connect() as conn:
            self.assertEqual(conn.execute('SELECT count(*) AS n FROM research_watch_scan_measurements').fetchone()['n'], 0)
            self.assertEqual(conn.execute('SELECT count(*) AS n FROM research_events').fetchone()['n'], 0)
            self.assertEqual(conn.execute('SELECT count(*) AS n FROM research_event_btc_movements').fetchone()['n'], 0)
        self.assertEqual(self.process()['processed'], 0)
        self.assertEqual(self.samples(), before)

    def test_catalog_queue_cursor_and_results_roll_back_atomically(self):
        self.persist('formula-rollback')
        self.consume_intake()
        with self.connect() as conn:
            with self.assertRaisesRegex(RuntimeError, 'after formula results'):
                with conn.transaction():
                    result = worker.process_page(conn, now=BASE + timedelta(days=2))
                    self.assertEqual((result['enqueued'], result['ready']), (8, 8))
                    raise RuntimeError('fixture interruption after formula results before commit')
        with self.connect() as conn:
            for table in ('research_watch_scan_formula_catalog', 'research_watch_scan_formula_state',
                          'research_watch_scan_formula_samples'):
                self.assertEqual(conn.execute('SELECT count(*) AS n FROM ' + table).fetchone()['n'], 0)
        self.assertEqual(self.process()['ready'], 8)

    def test_late_lower_id_recovers_after_restart_without_duplicate_samples(self):
        with self.connect() as late:
            late_id = self.persist_on(late, 'formula-late-low')
            high_ids = [self.persist('formula-high-' + str(i)) for i in (1, 2)]
            self.assertEqual(self.consume_intake()['accepted'], 2)
            first = self.process()
            self.assertEqual((first['enqueued'], first['ready'], first['errors']), (16, 16, 0))
            with self.connect() as conn:
                state = conn.execute('SELECT * FROM research_watch_scan_formula_state').fetchone()
                self.assertEqual(state['scan_cursor'], max(high_ids))
            before = self.samples()
            late.commit()
        self.assertEqual(self.consume_intake()['accepted'], 1)
        # The two recent IDs are still the higher IDs. A new DB connection
        # recovers the late lower ID through the next finite cursor lap.
        recovered = self.process()
        self.assertEqual((recovered['enqueued'], recovered['ready'], recovered['errors']), (8, 8, 0))
        samples = self.samples()
        self.assertEqual(len(samples), 24)
        self.assertEqual({s['snapshot_set_id'] for s in samples}, {late_id, *high_ids})
        self.assertEqual([s for s in samples if s['snapshot_set_id'] in high_ids], before)
        self.assertEqual(self.process()['processed'], 0)

    def test_completed_payload_is_immutable_and_migration_does_not_reset_it(self):
        self.persist('formula-frozen')
        self.consume_intake()
        self.assertEqual(self.process()['ready'], 8)
        before = self.samples()
        with self.connect() as conn:
            with self.assertRaises(self.psycopg.Error):
                with conn.transaction():
                    conn.execute("UPDATE research_watch_scan_formula_samples SET payload=jsonb_set(payload,'{evaluations,0,match_status}','\"UNKNOWN\"') WHERE symbol='BTC'")
            conn.execute((Path(__file__).parent / 'migrations/048_watch_scan_formulas.sql').read_text(), prepare=False)
        self.assertEqual(self.samples(), before)

    def test_failed_coin_rolls_back_to_savepoint_while_peers_finish(self):
        self.persist('formula-peer-progress')
        self.consume_intake()
        write = worker.write_result

        def interrupted(conn, job, result, *, now):
            write(conn, job, result, now=now)
            if job['symbol'] == 'BTC':
                raise RuntimeError('fixture after BTC formula write')

        with patch.object(worker, 'write_result', side_effect=interrupted):
            result = self.process()
        self.assertEqual((result['processed'], result['ready'], result['errors']), (8, 7, 1))
        samples = {s['symbol']: s for s in self.samples()}
        self.assertEqual((samples['BTC']['status'], samples['BTC']['payload'], samples['BTC']['attempts']), ('ERROR', {}, 1))
        self.assertEqual(samples['BTC']['last_error'], 'RuntimeError')
        self.assertEqual(self.process()['processed'], 0)
        resumed = self.process(now=BASE + timedelta(days=2, minutes=6))
        self.assertEqual((resumed['processed'], resumed['ready'], resumed['errors']), (1, 1, 0))
        for sample in self.samples():
            if sample['symbol'] != 'BTC':
                self.assertEqual(sample, samples[sample['symbol']])

    def test_shared_lock_and_rejected_capture_do_not_create_extra_evaluations(self):
        self.persist('formula-lock')
        self.persist('formula-rejected', block=intake.capture.failure('formula-rejected', 'fixture invalid source'))
        admitted = self.consume_intake()
        self.assertEqual((admitted['accepted'], admitted['rejected']), (1, 1))
        with self.connect() as owner:
            owner.execute('SELECT pg_advisory_xact_lock(%s)', (worker.LOCK_ID,))
            blocked = self.process()
            self.assertTrue(blocked['locked'])
            self.assertEqual(blocked['processed'], 0)
            self.assertEqual(self.samples(), [])
        result = self.process()
        self.assertEqual((result['enqueued'], result['ready'], result['errors']), (8, 8, 0))
        self.assertEqual(len(self.evaluations()), 544)

    def test_earliest_tied_cohort_is_selected_before_labels_and_counts_one_shared_wave(self):
        first_id = self.persist('anchor-earliest')
        later_id = self.persist('anchor-later', delay_minutes=10,
                                block=self.formula_bundle('anchor-later', score=64.))
        self.assertEqual(self.consume_intake()['accepted'], 2)
        self.assertEqual(self.process()['ready'], 16)
        self.seed_wave()
        self.seed_later_btc_bar(10)
        self.assertEqual(self.measure()['processed'], 16)
        selected = self.anchors()
        self.assertEqual(len(selected), 8)
        self.assertEqual({a['snapshot_set_id'] for a in selected}, {first_id})
        self.assertEqual({a['symbol'] for a in selected}, set(intake.capture.SYMBOLS))
        self.assertTrue(all(a['anchor_eligible'] for a in selected))
        controls = self.anchors(arm='NO_MATCH')
        self.assertEqual(len(controls), 8)
        self.assertEqual({a['snapshot_set_id'] for a in controls}, {later_id})

        # Only the later cohort can currently be measured. Its complete path
        # must not replace the original cohort whose exact entry is missing.
        self.seed_bars(symbols=intake.capture.SYMBOLS, first=10, count=1440)
        self.assertEqual(self.measure(now=BASE + timedelta(days=2, minutes=31))['ready'], 8)
        self.assertEqual(self.anchors(), selected)
        missing = self.wave_outcomes()
        self.assertEqual(len(missing), 32)
        self.assertTrue(all(r['outcome_status'] == 'DATA_MISSING' and r['cohort_members'] == 8 for r in missing))
        self.assertTrue(all(r['outcome_status'] == 'NO_TOUCH' for r in self.wave_outcomes(arm='NO_MATCH')))

        # Fill only the actual missing minutes. The same frozen cohort then
        # becomes measurable, with one BTC parent despite eight tied coins.
        self.seed_bars(symbols=intake.capture.SYMBOLS, first=0, count=10)
        self.assertEqual(self.measure(now=BASE + timedelta(days=2, minutes=62))['ready'], 8)
        self.assertEqual(self.anchors(), selected)
        outcomes = self.wave_outcomes()
        self.assertTrue(all(r['outcome_status'] == 'SUCCESS' and r['cohort_members'] == 8 for r in outcomes))
        inverse = self.wave_outcomes(candidate_key=INVERSE_FUTURES)
        self.assertEqual(len(inverse), 32)
        self.assertTrue(all(r['outcome_status'] == 'FAILURE' and r['analysis_direction'] == 'SHORT' for r in inverse))
        with self.connect() as conn:
            comparisons = conn.execute('''SELECT * FROM research_watch_scan_formula_comparisons
                WHERE candidate_key=%s AND symbol_scope='ALL' AND base_direction='LONG' ''', (FUTURES,)).fetchall()
        self.assertEqual(len(comparisons), 32)
        for comparison in comparisons:
            self.assertEqual((comparison['distinct_waves'], comparison['matched_waves'],
                              comparison['control_waves'], comparison['shared_waves']), (1, 1, 1, 1))
            self.assertEqual(comparison['matched_success'], 1)
            self.assertEqual(comparison['control_success'], 0)
            self.assertFalse(comparison['statistical_test_performed'])
            self.assertFalse(comparison['qualifies_as_prospective_formula_evidence'])

    def test_early_target_touch_stays_open_until_measurement_window_is_frozen(self):
        self.persist('early-touch-open-window')
        self.consume_intake()
        self.assertEqual(self.process()['ready'], 8)
        self.seed_wave()
        self.seed_bars(symbols=intake.capture.SYMBOLS, count=30)
        # Entry is BASE+8. Thirty closed minutes contain a favorable first
        # touch, while even the shortest 60-minute measurement remains open.
        measured = self.measure(now=BASE + timedelta(minutes=38))
        self.assertEqual((measured['processed'], measured['open'], measured['errors']), (8, 8, 0))
        with self.connect() as conn:
            raw = conn.execute('''SELECT window_status,outcome_status
                FROM research_watch_scan_threshold_results WHERE analysis_direction='LONG' ''').fetchall()
            self.assertEqual(len(raw), 8 * 4 * 8)
            self.assertTrue(all(r['window_status'] == 'OPEN' and r['outcome_status'] == 'SUCCESS' for r in raw))
            comparisons = conn.execute('''SELECT * FROM research_watch_scan_formula_comparisons
                WHERE candidate_key=%s AND symbol_scope='ALL' AND base_direction='LONG' ''', (FUTURES,)).fetchall()
        outcomes = self.wave_outcomes()
        self.assertEqual(len(outcomes), 32)
        self.assertTrue(all(r['outcome_status'] == 'OPEN' and r['cohort_members'] == 8 for r in outcomes))
        self.assertEqual(len(comparisons), 32)
        self.assertTrue(all(r['matched_success'] == 0 and r['matched_failure'] == 0
                            and r['open_arms'] == 1 and r['matched_decisive_success_pct'] is None
                            for r in comparisons))

    def test_pending_tied_feature_job_and_earlier_unknown_block_later_matches(self):
        first_id = self.persist('unknown-earlier', block=self.formula_bundle('unknown-earlier',
            score=64., unknown={('BTC', 'futures_flow')}))
        later_id = self.persist('known-later', delay_minutes=10)
        self.consume_intake()
        self.seed_wave()
        self.seed_later_btc_bar(10)
        self.assertEqual(self.measure()['processed'], 16)
        self.assertEqual(self.process(limit=1)['ready'], 1)
        unfinished = self.anchors(arm='NO_MATCH')
        self.assertEqual(len(unfinished), 1)
        self.assertEqual(unfinished[0]['snapshot_set_id'], first_id)
        self.assertFalse(unfinished[0]['anchor_eligible'])
        self.assertEqual(unfinished[0]['earliest_unknown_utc'], BASE + timedelta(minutes=7))

        self.assertEqual(self.process()['ready'], 15)
        matched = self.anchors()
        self.assertEqual(len(matched), 8)
        self.assertEqual({a['snapshot_set_id'] for a in matched}, {later_id})
        self.assertTrue(all(not a['anchor_eligible'] and a['earliest_unknown_utc'] == BASE + timedelta(minutes=7)
                            for a in matched))
        known_controls = self.anchors(arm='NO_MATCH')
        self.assertEqual(len(known_controls), 7)
        self.assertTrue(all(not a['anchor_eligible'] and a['snapshot_set_id'] == first_id for a in known_controls))
        self.assertTrue(all(r['outcome_status'] == 'DATA_MISSING' and not r['anchor_eligible']
                            for r in self.wave_outcomes()))
        with self.connect() as conn:
            separate_coin = conn.execute('''SELECT * FROM research_watch_scan_formula_wave_anchors
                WHERE candidate_key=%s AND symbol_scope='BNB' AND base_direction='LONG'
                    AND match_status='MATCH' ''', (FUTURES,)).fetchall()
        self.assertEqual(len(separate_coin), 1)
        self.assertTrue(separate_coin[0]['anchor_eligible'])
        self.assertEqual(separate_coin[0]['snapshot_set_id'], later_id)

    def test_provenance_mismatch_is_excluded_or_unknown_before_wave_selection(self):
        from psycopg.types.json import Jsonb
        snapshot_id = self.persist('hash-mismatch')
        self.consume_intake()
        self.seed_wave()
        self.assertEqual(self.measure()['processed'], 8)
        with self.connect() as conn:
            worker.register_catalog(conn)
            self.assertEqual(worker.enqueue(conn, now=BASE + timedelta(days=2)), 8)
            observation = conn.execute("SELECT * FROM research_watch_scan_observations WHERE symbol='ETH'").fetchone()
            inconsistent = worker.formula.evaluate_coin(observation)
            inconsistent['bundle_sha256'] = '0' * 64
            inconsistent['feature_sha256'] = worker.formula.digest({k: v for k, v in inconsistent.items() if k != 'feature_sha256'})
            job = conn.execute('''SELECT f.*,i.bundle_sha256,i.parent_payload_sha256
                FROM research_watch_scan_formula_samples f JOIN research_watch_scan_intakes i
                USING(consumer_version,snapshot_set_id) WHERE f.symbol='ETH' ''').fetchone()
            with self.assertRaisesRegex(ValueError, 'source identity mismatch'):
                worker.write_result(conn, job, inconsistent, now=BASE + timedelta(days=2))
            # Simulate preexisting corrupted stored evidence, bypassing only
            # the writer in this disposable fixture. No production guard is
            # disabled: the read view must still refuse the mismatched source.
            conn.execute("UPDATE research_watch_scan_formula_samples SET status='READY',payload=%s,next_attempt_at_utc=NULL WHERE symbol='ETH'",
                         (Jsonb(inconsistent),))
        self.assertEqual(self.process()['ready'], 7)
        with self.connect() as conn:
            population = conn.execute('''SELECT * FROM research_watch_scan_formula_wave_population
                WHERE candidate_key=%s AND symbol_scope='ALL' AND base_direction='LONG' ''', (FUTURES,)).fetchall()
        self.assertEqual(len(population), 8)
        self.assertEqual(next(p for p in population if p['symbol'] == 'ETH')['match_status'], 'UNKNOWN')
        self.assertEqual(len(self.anchors()), 7)
        self.assertTrue(all(not a['anchor_eligible'] for a in self.anchors()))

        # A missing-entry measurement has not frozen a reference price. An
        # inconsistent source hash must remove it from the wave population.
        with self.connect() as conn:
            conn.execute("UPDATE research_watch_scan_measurements SET payload=jsonb_set(payload,'{bundle_sha256}',%s::jsonb) WHERE symbol='BTC'", ('"' + 'f' * 64 + '"',))
            population = conn.execute('''SELECT * FROM research_watch_scan_formula_wave_population
                WHERE candidate_key=%s AND symbol_scope='ALL' AND base_direction='LONG' ''', (FUTURES,)).fetchall()
        self.assertEqual(len(population), 7)
        self.assertNotIn('BTC', {p['symbol'] for p in population})
        self.assertEqual(len(self.anchors()), 6)
        self.assertTrue(all(a['snapshot_set_id'] == snapshot_id and not a['anchor_eligible'] for a in self.anchors()))


if __name__ == '__main__':
    unittest.main()
