"""Selected-timeframe research isolation and cohort semantics on PostgreSQL.

Only the guarded disposable TEST_DATABASE_URL fixture runs. Frozen observations
supply decisions, while the existing coin measurements supply all TF outcomes.
"""
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
import os
import unittest
from unittest.mock import patch

import live_price_provider
import research_price_archive as archive
import research_watch_scan_formula as legacy
import research_watch_scan_formula_maxpain as maxpain
import research_watch_scan_formula_btc_context as btc
import research_watch_scan_formula_asset_context as asset
import research_watch_scan_formula_score_change as score_change
import research_watch_scan_formula_worker as old_worker
import research_watch_scan_formula_timeframe as adapter
import research_watch_scan_formula_timeframe_worker as worker
import research_watch_scan_formula_score_change_postgres_selftest as score_tests
import research_watch_scan_formula_postgres_selftest as formula_tests
import research_watch_scan_formula_btc_context_postgres_selftest as btc_tests
import research_watch_scan_measurement_postgres_selftest as measurement_tests
import research_watch_scan_measurement_worker as measurement_worker
import research_watch_scan_intake as intake
from research_watch_scan_intake_selftest import rehash
from research_watch_score_capture_selftest import BASE, bundle, inputs

PREFIX = 'captured-question-search-v3-experimental-binding:'
SUPPORTS = PREFIX + 'LIQUIDITY_SUPPORTS'
OPPOSES = PREFIX + 'LIQUIDITY_OPPOSES'
INVERSE_SUPPORTS = PREFIX + 'INVERSE:' + SUPPORTS


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'), 'Explicit local/CI PostgreSQL required')
class WatchScanTimeframePostgresTests(unittest.TestCase):
    # Reuse guarded fixture helpers, never inherit earlier regression methods.
    connect = score_tests.WatchScanScoreChangePostgresTests.connect
    drop_database = score_tests.WatchScanScoreChangePostgresTests.drop_database
    consume_intake = score_tests.WatchScanScoreChangePostgresTests.consume_intake
    formula_bundle = score_tests.WatchScanScoreChangePostgresTests.formula_bundle
    persist_on = score_tests.WatchScanScoreChangePostgresTests.persist_on
    persist = score_tests.WatchScanScoreChangePostgresTests.persist
    old_samples = score_tests.WatchScanScoreChangePostgresTests.samples
    old_evaluations = score_tests.WatchScanScoreChangePostgresTests.evaluations
    old_active_version = score_tests.WatchScanScoreChangePostgresTests.active_version
    seed_wave = measurement_tests.WatchScanMeasurementPostgresTests.seed_wave
    seed_bars = measurement_tests.WatchScanMeasurementPostgresTests.seed_bars
    seed_later_btc_bar = formula_tests.WatchScanFormulaPostgresTests.seed_later_btc_bar

    def setUp(self):
        score_tests.WatchScanScoreChangePostgresTests.setUp(self)
        with self.connect() as conn:
            conn.execute((Path(__file__).parent/'migrations/054_watch_scan_timeframe_formulas.sql').read_text(),
                         prepare=False)

    def process(self, *, now=None, limit=32):
        with self.connect() as conn:
            return worker.process_page(conn, now=now or BASE+timedelta(days=2), limit=limit)

    def samples(self):
        with self.connect() as conn:
            return conn.execute('SELECT * FROM research_watch_scan_tf_formula_samples '
                'ORDER BY snapshot_set_id,symbol').fetchall()

    def active_version(self):
        with self.connect() as conn:
            return conn.execute('SELECT active_evaluation_version FROM research_watch_scan_tf_formula_runtime '
                'WHERE singleton=true').fetchone()['active_evaluation_version']

    def rows(self, suffix, *, candidate=SUPPORTS, timeframe=None, base='LONG', scope='ALL', arm=None):
        where, params = ['candidate_key=%s'], [candidate]
        if timeframe is not None:
            where.append('timeframe=%s')
            params.append(timeframe)
        if base is not None:
            where.append('base_direction=%s')
            params.append(base)
        if suffix != 'evaluations':
            where.append('symbol_scope=%s')
            params.append(scope)
        if arm is not None:
            where.append('match_status=%s')
            params.append(arm)
        with self.connect() as conn:
            return conn.execute('SELECT * FROM research_watch_scan_tf_formula_' + suffix +
                ' WHERE ' + ' AND '.join(where), params).fetchall()

    def tf_bundle(self, cycle_id, *, opposed_timeframes=(), unknown_selection=(), inactive=(), long_selected=()):
        # Recompute the real frozen scores and selected flags after changing
        # raw liquidity/targets. A near SHORT target gives a known LONG base.
        rows = inputs()
        for row in rows:
            row.update(short_max_pain=101., long_max_pain=95.)
            if (row['symbol'], row['timeframe']) in long_selected:
                row.update(short_max_pain=110., long_max_pain=99.)
            if row['symbol'] == 'HYPE':
                row.update(price_source='binance_futures', price_pair='HYPEUSDT', price_market='PERP', price_instrument='HYPEUSDT')
            if row['timeframe'] in opposed_timeframes:
                row.update(short_liquidation_amount=100., long_liquidation_amount=200.)
            if (row['symbol'], row['timeframe']) in inactive:
                row.update(short_max_pain=None, long_max_pain=None)
            distance = live_price_provider.recalculate_distances(row['current_price'], row['short_max_pain'], row['long_max_pain'])
            row.update(distance_short_pct=distance['short_signed_pct'], distance_long_pct=distance['long_signed_pct'])
        block, *_ = bundle(rows=rows, cycle_id=cycle_id)
        model_source = self.formula_bundle(cycle_id)
        for symbol, coin in block['coins'].items():
            coin['models'] = deepcopy(model_source['coins'][symbol]['models'])
            for key in ('positioning', 'futures', 'spot', 'timing_observation'):
                coin['sources'][key] = deepcopy(model_source['coins'][symbol]['sources'][key])
            coin['source_time_errors'] = []
            for slot in coin['maxpain']:
                pair = (symbol, slot['timeframe'])
                if pair in unknown_selection:
                    slot.pop('selected', None)
        return rehash(block)

    def seed_previous(self, cycle_id):
        snapshot_id = self.persist(cycle_id, block=self.tf_bundle(cycle_id))
        self.assertEqual(self.consume_intake()['accepted'], 1)
        for version in (legacy, maxpain, btc, asset, score_change):
            with self.connect() as conn, patch.object(old_worker, 'formula', version):
                self.assertEqual(old_worker.process_page(conn, now=BASE+timedelta(days=2))['ready'], 8)
        self.assertEqual(self.old_active_version(), score_change.VERSION)
        return snapshot_id

    def measure(self, *, now=None):
        with self.connect() as conn:
            return measurement_worker.process_page(conn, now=now or BASE+timedelta(days=2))

    def test_exact_catalog_grid_activation_and_old_v5_evidence_are_preserved_without_implicit_ddl(self):
        self.seed_previous('tf-complete-grid')
        old = self.old_samples(score_change.VERSION)
        old_evaluations = self.old_evaluations()
        self.assertEqual(len(old_evaluations), 8*340)
        self.assertIsNone(self.active_version())
        with patch.object(archive, 'read_path', side_effect=AssertionError('TF formulas do not read price paths')), self.connect() as conn:
            recording = btc_tests.RecordingConnection(conn)
            result = worker.process_page(recording, now=BASE+timedelta(days=2), limit=3)
        self.assertEqual((result['ready'], result['errors']), (3, 0))
        self.assertIsNone(self.active_version())
        self.assertEqual(self.rows('evaluations', base=None), [])
        self.assertFalse(any(query.lstrip().upper().startswith(('CREATE ', 'ALTER ', 'DROP ')) for query in recording.sql))
        with self.connect() as conn:
            self.assertEqual(conn.execute('SELECT count(*) FILTER (WHERE supported) AS n FROM research_watch_scan_formula_coverage').fetchone()['n'], 170)
        self.assertEqual(self.process()['ready'], 5)
        self.assertEqual(self.active_version(), adapter.VERSION)
        current = self.samples()
        self.assertEqual(len(current), 8)
        for row in current:
            evaluations = row['payload']['evaluations']
            self.assertEqual(len(evaluations), 476)
            self.assertEqual(len({(item['candidate_key'], item['timeframe'], item['base_direction']) for item in evaluations}), 476)
            self.assertEqual(len({item['candidate_key'] for item in evaluations}), 34)
            self.assertEqual({item['timeframe'] for item in evaluations}, set(adapter.TIMEFRAMES))
            self.assertEqual(sum(item['match_status'] == 'NOT_APPLICABLE' for item in evaluations), 238)
        self.assertEqual(self.old_samples(score_change.VERSION), old)
        self.assertCountEqual(self.old_evaluations(), old_evaluations)
        with self.connect() as conn:
            counts = conn.execute('SELECT count(*) AS n FROM research_watch_scan_tf_formula_catalog').fetchone()
            self.assertEqual(counts['n'], 34)
            self.assertEqual(conn.execute('SELECT count(*) FILTER (WHERE supported) AS n FROM research_watch_scan_formula_coverage').fetchone()['n'], 204)
            overlap = conn.execute('''SELECT count(*) AS n FROM research_watch_scan_tf_formula_catalog t
                JOIN research_watch_scan_formula_catalog c USING(candidate_key)
                WHERE c.evaluation_version=%s AND c.supported''', (score_change.VERSION,)).fetchone()['n']
            self.assertEqual(overlap, 0)
            conn.execute((Path(__file__).parent/'migrations/054_watch_scan_timeframe_formulas.sql').read_text(), prepare=False)
        self.assertEqual(self.samples(), current)
        self.assertEqual(self.old_samples(score_change.VERSION), old)

    def test_unknown_inactive_and_unselected_sides_are_not_false_controls_and_timeframes_remain_separate(self):
        block = self.tf_bundle('tf-three-state', opposed_timeframes=('24h',),
            unknown_selection={('BTC', '12h')}, inactive={('ETH', '12h')}, long_selected={('SOL', '12h')})
        self.persist('tf-three-state', block=block)
        self.assertEqual(self.consume_intake()['accepted'], 1)
        self.assertEqual(self.process()['ready'], 8)
        twelve = {(row['symbol'], row['base_direction']): row for row in
                  self.rows('evaluations', timeframe='12h', base=None)}
        for base in ('LONG', 'SHORT'):
            self.assertEqual(twelve['BTC', base]['match_status'], 'UNKNOWN')
            self.assertEqual(twelve['ETH', base]['match_status'], 'NOT_APPLICABLE')
        self.assertEqual(twelve['SOL', 'LONG']['match_status'], 'NOT_APPLICABLE')
        self.assertEqual(twelve['SOL', 'SHORT']['match_status'], 'NO_MATCH')
        self.assertTrue(all(row['match_status'] == 'NO_MATCH' for row in self.rows('evaluations', timeframe='24h')))
        self.assertTrue(all(row['match_status'] == 'MATCH' for row in self.rows('evaluations', timeframe='48h')))
        self.seed_wave()
        self.assertEqual(self.measure()['processed'], 8)
        population = self.rows('wave_population', timeframe='12h', base=None)
        self.assertFalse(any(row['match_status'] == 'NOT_APPLICABLE' for row in population))
        self.assertFalse(any(row['symbol'] == 'ETH' for row in population))
        self.assertFalse(any(row['symbol'] == 'SOL' and row['base_direction'] == 'LONG' for row in population))
        first = self.rows('wave_anchors', timeframe='12h', arm='MATCH')
        self.assertEqual(len(first), 5)
        self.assertTrue(all(not row['anchor_eligible'] and row['earliest_unknown_utc'] == BASE+timedelta(minutes=7) for row in first))
        controls = self.rows('wave_anchors', timeframe='24h', arm='NO_MATCH')
        self.assertEqual(len(controls), 8)
        self.assertTrue(all(row['anchor_eligible'] for row in controls))
        unaffected = self.rows('wave_anchors', timeframe='48h', arm='MATCH')
        self.assertEqual(len(unaffected), 8)
        self.assertTrue(all(row['anchor_eligible'] for row in unaffected))
        separate = self.rows('wave_anchors', timeframe='12h', scope='BNB', arm='MATCH')
        self.assertEqual(len(separate), 1)
        self.assertTrue(separate[0]['anchor_eligible'])

    def test_all_timeframes_reuse_coin_outcomes_inverse_direction_and_one_independent_wave(self):
        self.persist('tf-shared-outcomes', block=self.tf_bundle('tf-shared-outcomes'))
        self.consume_intake()
        self.assertEqual(self.process()['ready'], 8)
        frozen = self.samples()
        self.seed_wave()
        self.seed_bars(symbols=intake.capture.SYMBOLS, count=1440)
        self.assertEqual(self.measure()['ready'], 8)
        with self.connect() as conn:
            self.assertEqual(conn.execute('SELECT count(*) AS n FROM research_watch_scan_measurements').fetchone()['n'], 8)
            self.assertEqual(conn.execute('SELECT count(*) AS n FROM research_watch_scan_threshold_results').fetchone()['n'], 512)
            self.assertEqual(conn.execute('SELECT count(*) AS n FROM research_events').fetchone()['n'], 0)
        outcomes = self.rows('wave_outcomes', arm='MATCH')
        self.assertEqual(len(outcomes), 7*32)
        self.assertTrue(all(row['outcome_status'] == 'SUCCESS' and row['cohort_members'] == 8 for row in outcomes))
        inverse = self.rows('wave_outcomes', candidate=INVERSE_SUPPORTS, arm='MATCH')
        self.assertEqual(len(inverse), 7*32)
        self.assertTrue(all(row['outcome_status'] == 'FAILURE' and row['analysis_direction'] == 'SHORT' for row in inverse))
        for timeframe in adapter.TIMEFRAMES:
            anchors = self.rows('wave_anchors', timeframe=timeframe, arm='MATCH')
            self.assertEqual(len(anchors), 8)
            self.assertEqual({row['symbol'] for row in anchors}, set(intake.capture.SYMBOLS))
            self.assertTrue(all(row['anchor_eligible'] for row in anchors))
            hype = next(row for row in anchors if row['symbol'] == 'HYPE')
            self.assertEqual(hype['price_route'], archive.HYPERLIQUID_PERP)
        comparisons = self.rows('comparisons')
        self.assertEqual(len(comparisons), 7*32)
        for row in comparisons:
            self.assertEqual((row['distinct_waves'], row['matched_waves'], row['matched_success']), (1, 1, 1))
            self.assertFalse(row['statistical_test_performed'])
            self.assertFalse(row['qualifies_as_prospective_formula_evidence'])
        self.assertEqual(self.samples(), frozen)

    def test_activation_requires_every_accepted_coin_even_if_intake_is_not_yet_enqueued(self):
        ids = [self.persist('tf-enqueue-'+str(i), block=self.tf_bundle('tf-enqueue-'+str(i))) for i in range(4)]
        self.assertEqual(self.consume_intake()['accepted'], 4)
        with self.connect() as conn, patch.object(worker, 'SCAN_PAGE', 1):
            worker.register_catalog(conn)
            self.assertEqual(worker.enqueue(conn, now=BASE+timedelta(days=2)), 24)
        self.assertEqual({row['snapshot_set_id'] for row in self.samples()}, {ids[0], ids[2], ids[3]})
        with patch.object(worker, 'enqueue', return_value=0):
            self.assertEqual(self.process()['ready'], 24)
        self.assertTrue(all(row['status'] == 'READY' for row in self.samples()))
        self.assertIsNone(self.active_version())
        self.assertEqual(self.process()['ready'], 8)
        self.assertEqual(self.active_version(), adapter.VERSION)
        self.assertEqual(len(self.samples()), 32)
        self.assertEqual(self.process()['processed'], 0)

    def test_final_sample_and_activation_roll_back_together_and_restart_recovers(self):
        self.persist('tf-rollback', block=self.tf_bundle('tf-rollback'))
        self.consume_intake()
        self.assertEqual(self.process(limit=7)['ready'], 7)
        before = self.samples()
        with self.connect() as conn:
            with self.assertRaisesRegex(RuntimeError, 'after activation'):
                with conn.transaction():
                    result = worker.process_page(conn, now=BASE+timedelta(days=2), limit=1)
                    self.assertEqual(result['ready'], 1)
                    active = conn.execute('SELECT active_evaluation_version FROM research_watch_scan_tf_formula_runtime WHERE singleton=true').fetchone()
                    self.assertEqual(active['active_evaluation_version'], adapter.VERSION)
                    raise RuntimeError('fixture after activation')
        self.assertIsNone(self.active_version())
        self.assertEqual(self.samples(), before)
        self.assertEqual(self.process(limit=1)['ready'], 1)
        self.assertEqual(self.active_version(), adapter.VERSION)

    def test_real_sql_failure_isolated_per_coin_and_retry_preserves_ready_evidence_and_hashes(self):
        self.persist('tf-sql-retry', block=self.tf_bundle('tf-sql-retry'))
        self.consume_intake()
        original_write = worker.write_result
        def broken(conn, job, result, *, now):
            if job['symbol'] == 'ETH':
                conn.execute('SELECT 1 / 0')
            return original_write(conn, job, result, now=now)
        with patch.object(worker, 'write_result', side_effect=broken):
            result = self.process()
        self.assertEqual((result['ready'], result['errors']), (7, 1))
        self.assertIsNone(self.active_version())
        before = self.samples()
        peers = [row for row in before if row['symbol'] != 'ETH']
        failed = next(row for row in before if row['symbol'] == 'ETH')
        self.assertEqual((failed['status'], failed['last_error']), ('ERROR', 'DivisionByZero'))
        self.assertEqual(self.process(now=BASE+timedelta(days=2, minutes=4))['processed'], 0)
        self.assertEqual(self.process(now=BASE+timedelta(days=2, minutes=5))['ready'], 1)
        after = self.samples()
        self.assertEqual([row for row in after if row['symbol'] != 'ETH'], peers)
        self.assertEqual(self.active_version(), adapter.VERSION)
        payload = after[0]['payload']
        self.assertEqual(payload['feature_sha256'], adapter.digest({key: value for key, value in payload.items() if key != 'feature_sha256'}))
        with self.connect() as conn:
            with self.assertRaises(self.psycopg.Error):
                with conn.transaction():
                    conn.execute("UPDATE research_watch_scan_tf_formula_samples SET payload=jsonb_set(payload,'{evaluations,0,match_status}','\"UNKNOWN\"') WHERE symbol='BTC'")
            with self.assertRaises(self.psycopg.Error):
                with conn.transaction():
                    conn.execute("UPDATE research_watch_scan_tf_formula_catalog SET definition_sha256=%s", ('f'*64,))
        self.assertEqual(self.samples(), after)

    def test_late_committed_lower_intake_recovers_after_cursor_pass_without_duplicate_grids(self):
        with self.connect() as late:
            low = self.persist_on(late, 'tf-late-low', block=self.tf_bundle('tf-late-low'))
            higher = [self.persist('tf-high-'+str(i), block=self.tf_bundle('tf-high-'+str(i))) for i in (1, 2)]
            self.assertEqual(self.consume_intake()['accepted'], 2)
            self.assertEqual(self.process()['ready'], 16)
            self.assertNotIn(low, {row['snapshot_set_id'] for row in self.samples()})
            late.commit()
        self.assertEqual(self.consume_intake()['accepted'], 1)
        self.assertEqual(self.process()['ready'], 8)
        current = self.samples()
        self.assertEqual({row['snapshot_set_id'] for row in current}, {low, *higher})
        self.assertEqual(len(current), 24)
        self.assertTrue(all(len(row['payload']['evaluations']) == 476 for row in current))
        self.assertEqual(self.process()['processed'], 0)
        self.assertEqual(self.samples(), current)


if __name__ == '__main__':
    unittest.main()
