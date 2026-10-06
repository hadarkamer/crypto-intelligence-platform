"""Real PostgreSQL all-scan measurement recovery on disposable local/CI data.

Only TEST_DATABASE_URL is accepted. Every test creates and drops its own
database; no application database, network price reader, or alert sender runs.
"""
from datetime import timedelta
import os
from pathlib import Path
import unittest
from unittest.mock import patch

import research_price_archive as price_archive
import research_watch_scan_intake as intake
import research_watch_scan_intake_selftest as intake_tests
import research_watch_scan_measurement_worker as worker
from research_watch_score_capture_selftest import BASE, archive_payload, bundle, inputs


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'), 'Explicit local/CI PostgreSQL required')
class WatchScanMeasurementPostgresTests(unittest.TestCase):
    # Reuse database creation/cleanup only, without inheriting the intake tests.
    connect = intake_tests.PostgreSQLIntakeTests.connect
    drop_database = intake_tests.PostgreSQLIntakeTests.drop_database

    def setUp(self):
        intake_tests.PostgreSQLIntakeTests.setUp(self)
        request_guard = patch('requests.sessions.Session.request',
                              side_effect=AssertionError('Measurement must use local archived prices only'))
        request_guard.start()
        self.addCleanup(request_guard.stop)
        root = Path(__file__).parent / 'migrations'
        with self.connect() as conn:
            conn.execute('CREATE TABLE research_events (event_id BIGINT PRIMARY KEY, alert_time_utc TIMESTAMPTZ NOT NULL)')
            for name in ('044_continuous_price_archive.sql',
                         '021_btc_parent_movements_v1.sql',
                         '047_watch_scan_measurements.sql'):
                conn.execute((root / name).read_text(), prepare=False)

    def persist_on(self, conn, cycle_id, *, block=None):
        """Insert the real immutable archive atomically, with fixed source time."""
        from psycopg.types.json import Jsonb
        if block is None:
            rows = inputs()
            for row in rows:
                if row['symbol'] == 'HYPE':
                    row.update(price_source='binance_futures', price_pair='HYPEUSDT',
                               price_market='PERP', price_instrument='HYPEUSDT')
            block, *_ = bundle(rows=rows, cycle_id=cycle_id)
        payload = archive_payload(block, cycle_id=cycle_id)
        parent = {**payload['set'], 'created_at_utc': BASE + timedelta(minutes=7)}
        columns = list(parent)
        values = [Jsonb(parent[k]) if k in {'source_metadata', 'validation_errors', 'completeness_report'}
                  else parent[k] for k in columns]
        snapshot_id = conn.execute('INSERT INTO research_max_pain_snapshot_sets (' + ','.join(columns) +
            ') VALUES (' + ','.join(['%s'] * len(columns)) + ') RETURNING snapshot_set_id',
            values).fetchone()['snapshot_set_id']
        for table, children, json_fields in (
            ('research_max_pain_snapshot_symbols', payload['symbols'], {'validation_errors'}),
            ('research_max_pain_snapshot_rows', payload['rows'], {'validation_errors', 'raw_provenance'}),
        ):
            for child in children:
                names = list(child)
                child_values = [Jsonb(child[k]) if k in json_fields else child[k] for k in names]
                conn.execute('INSERT INTO ' + table + ' (snapshot_set_id,' + ','.join(names) + ') VALUES (' +
                    ','.join(['%s'] * (len(names) + 1)) + ')', [snapshot_id, *child_values])
        return snapshot_id

    def persist(self, cycle_id, *, block=None):
        with self.connect() as conn:
            return self.persist_on(conn, cycle_id, block=block)

    def consume_intake(self):
        with self.connect() as conn:
            return intake.consume_page(conn)

    def process(self, *, now=None, limit=worker.JOB_LIMIT):
        with self.connect() as conn:
            return worker.process_page(conn, now=now or BASE + timedelta(days=2), limit=limit)

    def jobs(self):
        with self.connect() as conn:
            return conn.execute('SELECT * FROM research_watch_scan_measurements ORDER BY snapshot_set_id,symbol').fetchall()

    def seed_bars(self, *, symbols=('BTC',), first=0, count=1441, omit=()):
        """Closed causal-entry bars with a first favorable-only LONG touch."""
        entry = BASE + timedelta(minutes=8)
        with self.connect() as conn:
            for symbol in symbols:
                route = price_archive.HYPERLIQUID_PERP if symbol == 'HYPE' else price_archive.BINANCE_SPOT
                bars = []
                for minute in range(first, first + count):
                    if minute in omit:
                        continue
                    opened = entry + timedelta(minutes=minute)
                    bars.append(dict(open_time_utc=opened,
                        close_time_utc=opened + timedelta(minutes=1, milliseconds=-1),
                        open=100., high=103. if minute == 0 else 100.1,
                        low=100. if minute == 0 else 99.9, close=100.,
                        volume=None if symbol == 'HYPE' else 1.))
                price_archive.write_bars(conn, route, symbol, bars)

    def seed_wave(self):
        with self.connect() as conn:
            for minute in (6, 7):
                opened = BASE + timedelta(minutes=minute)
                conn.execute('''INSERT INTO research_btc_price_bars
                    (open_time_utc, close_time_utc, open, high, low, close)
                    VALUES (%s,%s,100,101,99,100)''',
                    (opened, opened + timedelta(minutes=1, milliseconds=-1)))
            conn.execute('''INSERT INTO research_btc_parent_movements
                (btc_parent_movement_id, episode_policy_version, start_time_utc,
                 confirmed_at_utc, direction, evidence_eligible, boundary_reason,
                 observed_through_utc, price_source, state_json)
                VALUES ('fixture-parent', 'btc-parent-close-reversal-200bps-v1',
                    %s,%s,'UP',true,'CAUSAL_CLOSE_REVERSAL',%s,
                    'BINANCE_SPOT_BTCUSDT_1M','{}'::jsonb)''',
                (BASE, BASE, BASE + timedelta(days=1)))

    def test_all_scans_have_eight_samples_112_slot_links_and_explicit_hype_perp(self):
        snapshot_id = self.persist('all-scan-views')
        self.assertEqual(self.consume_intake()['accepted'], 1)
        self.seed_bars(symbols=intake.capture.SYMBOLS, count=1440)
        self.seed_wave()
        first = self.process()
        self.assertEqual((first['enqueued'], first['processed'], first['ready'], first['errors']), (8, 8, 8, 0))
        before = self.jobs()
        self.assertEqual(len(before), 8)
        self.assertTrue(all(j['status'] == 'READY' and j['next_attempt_at_utc'] is None for j in before))
        self.assertEqual(self.process()['processed'], 0)
        self.assertEqual(self.jobs(), before)
        with self.connect() as conn:
            windows = conn.execute('SELECT * FROM research_watch_scan_window_results').fetchall()
            thresholds = conn.execute('SELECT * FROM research_watch_scan_threshold_results').fetchall()
            links = conn.execute('SELECT * FROM research_watch_scan_measured_score_slots').fetchall()
            self.assertEqual((len(windows), len(thresholds), len(links)), (64, 512, 448))
            # The 112 original score slots share 4 directional windows, each
            # carrying 8 thresholds. These 3584 links are not independent events.
            self.assertEqual(sum(len(r['labels']) for r in links), 112 * 4 * 8)
            self.assertEqual(len({(r['symbol'], r['timeframe'], r['source_side']) for r in links}), 112)
            self.assertTrue(all(r['snapshot_set_id'] == snapshot_id and r['reference_price'] == 100.
                                and r['entry_time_utc'] == BASE + timedelta(minutes=8) for r in windows))
            self.assertTrue(all(r['btc_parent_movement_id'] == 'fixture-parent'
                                and r['btc_membership_status'] == 'LIVE' for r in windows))
            self.assertTrue(all(not r['qualifies_as_prospective_formula_evidence'] for r in thresholds))
            self.assertTrue(all(r['analysis_direction'] == ('SHORT' if r['source_side'] == 'LONG' else 'LONG')
                                and not r['is_delivered_alert'] for r in links))
            self.assertTrue(any(r['score'] is not None and r['score'] < 65 for r in links))
            hype = [r for r in windows if r['symbol'] == 'HYPE']
            self.assertEqual(len(hype), 8)
            self.assertTrue(all(r['price_route'] == price_archive.HYPERLIQUID_PERP
                                and r['price_source']['market'] == 'perpetual' for r in hype))
            source = conn.execute("SELECT sources FROM research_watch_scan_observations WHERE symbol='HYPE'").fetchone()
            self.assertEqual(source['sources']['maxpain_operational_rows'][0]['price_source'], 'binance_futures')
            self.assertEqual(conn.execute('SELECT count(*) AS n FROM research_events').fetchone()['n'], 0)
            self.assertEqual(conn.execute('SELECT count(*) AS n FROM research_event_btc_movements').fetchone()['n'], 0)
            # Reapplying the additive migration preserves completed evidence.
            conn.execute((Path(__file__).parent / 'migrations/047_watch_scan_measurements.sql').read_text(), prepare=False)
        self.assertEqual(self.jobs(), before)

    def test_enqueue_results_and_cursor_roll_back_together_then_restart_recovers(self):
        self.persist('measure-rollback')
        self.consume_intake()
        self.seed_bars(count=1440)
        self.seed_wave()
        with self.connect() as conn:
            with self.assertRaisesRegex(RuntimeError, 'after measurements'):
                with conn.transaction():
                    result = worker.process_page(conn, now=BASE + timedelta(days=2))
                    self.assertEqual((result['enqueued'], result['ready']), (8, 1))
                    raise RuntimeError('fixture failure after measurements before commit')
        with self.connect() as conn:
            self.assertEqual(conn.execute('SELECT count(*) AS n FROM research_watch_scan_measurements').fetchone()['n'], 0)
            self.assertEqual(conn.execute('SELECT count(*) AS n FROM research_watch_scan_measurement_state').fetchone()['n'], 0)
        result = self.process()
        self.assertEqual((result['enqueued'], result['ready'], result['errors']), (8, 1, 0))

    def test_late_committed_lower_source_id_recovers_by_cursor_lap(self):
        with self.connect() as late:
            late_id = self.persist_on(late, 'measurement-late-low')
            high_ids = [self.persist('measurement-high-' + str(i)) for i in (1, 2)]
            self.assertEqual(self.consume_intake()['accepted'], 2)
            first = self.process(now=BASE + timedelta(minutes=7))
            self.assertEqual((first['enqueued'], first['processed']), (16, 0))
            with self.connect() as conn:
                state = conn.execute('SELECT * FROM research_watch_scan_measurement_state').fetchone()
                self.assertEqual(state['scan_cursor'], max(high_ids))
                self.assertNotIn(late_id, [j['snapshot_set_id'] for j in self.jobs()])
            late.commit()
        self.assertEqual(self.consume_intake()['accepted'], 1)
        # Both reserved recent IDs remain the higher IDs, so recovery of the
        # lower ID proves the finite cyclic scan rather than the recent reserve.
        recovered = self.process(now=BASE + timedelta(minutes=7))
        self.assertEqual((recovered['enqueued'], recovered['processed']), (8, 0))
        jobs = self.jobs()
        self.assertEqual(len(jobs), 24)
        self.assertEqual({j['snapshot_set_id'] for j in jobs}, {late_id, *high_ids})
        self.assertEqual(self.process(now=BASE + timedelta(minutes=7))['enqueued'], 0)

    def test_gap_repair_retries_without_rewriting_completed_shorter_windows(self):
        self.persist('gap-repair')
        self.consume_intake()
        self.seed_wave()
        self.seed_bars(count=1440, omit=(100,))
        first = self.process()
        self.assertEqual((first['processed'], first['errors']), (8, 0))
        btc_before = next(j for j in self.jobs() if j['symbol'] == 'BTC')
        self.assertEqual(btc_before['status'], 'DATA_MISSING')
        ready_before = [r for r in btc_before['payload']['records'] if r['status'] == 'READY']
        self.assertEqual(len(ready_before), 2)
        self.assertEqual({r['window_minutes'] for r in ready_before}, {60})
        self.seed_bars(first=100, count=1)
        repaired = self.process(now=BASE + timedelta(days=2, minutes=31))
        self.assertEqual((repaired['ready'], repaired['errors']), (1, 0))
        btc_after = next(j for j in self.jobs() if j['symbol'] == 'BTC')
        self.assertEqual(btc_after['status'], 'READY')
        for record in ready_before:
            self.assertIn(record, btc_after['payload']['records'])
        self.assertEqual(btc_after['payload']['reference_price'], btc_before['payload']['reference_price'])
        self.assertEqual(btc_after['payload']['membership'], btc_before['payload']['membership'])

    def test_database_rejects_rewriting_ready_records_or_entry_provenance(self):
        self.persist('frozen-completed')
        self.consume_intake()
        self.seed_bars(count=1440)
        self.seed_wave()
        self.assertEqual(self.process()['ready'], 1)
        original = next(j for j in self.jobs() if j['symbol'] == 'BTC')
        for path, value, error in (
            ('{records,0,mfe_pct}', '99', 'Completed Watch measurement'),
            ('{reference_price}', '99', 'entry provenance'),
            ('{membership,btc_parent_movement_id}', '"different-parent"', 'BTC membership'),
        ):
            with self.subTest(path=path), self.connect() as conn:
                with self.assertRaisesRegex(self.psycopg.Error, error):
                    with conn.transaction():
                        conn.execute("UPDATE research_watch_scan_measurements SET payload=jsonb_set(payload,%s::text[],%s::jsonb) WHERE symbol='BTC'",
                                     (path, value))
        self.assertEqual(next(j for j in self.jobs() if j['symbol'] == 'BTC'), original)

    def test_failed_coin_savepoint_preserves_peer_progress_and_retries(self):
        self.persist('peer-progress')
        self.consume_intake()
        self.seed_bars(symbols=('BTC', 'HYPE'), count=1440)
        self.seed_wave()
        original_write = worker.write_result

        def fail_after_write(conn, job, result, *, now):
            original_write(conn, job, result, now=now)
            if job['symbol'] == 'BTC':
                raise RuntimeError('fixture interruption after BTC write')

        with patch.object(worker, 'write_result', side_effect=fail_after_write):
            result = self.process()
        self.assertEqual((result['processed'], result['errors'], result['ready']), (8, 1, 1))
        jobs = {j['symbol']: j for j in self.jobs()}
        self.assertEqual((jobs['BTC']['status'], jobs['BTC']['payload'], jobs['BTC']['attempts']), ('ERROR', {}, 1))
        self.assertEqual(jobs['BTC']['last_error'], 'RuntimeError')
        self.assertEqual(jobs['HYPE']['status'], 'READY')
        retried = self.process(now=BASE + timedelta(days=2, minutes=6))
        self.assertEqual((retried['ready'], retried['errors']), (1, 0))
        self.assertEqual(next(j for j in self.jobs() if j['symbol'] == 'HYPE'), jobs['HYPE'])

    def test_shared_worker_lock_prevents_duplicate_processing(self):
        self.persist('measurement-lock')
        self.persist('measurement-rejected', block=intake.capture.failure('measurement-rejected', 'fixture source failure'))
        admitted = self.consume_intake()
        self.assertEqual((admitted['accepted'], admitted['rejected']), (1, 1))
        with self.connect() as owner:
            owner.execute('SELECT pg_advisory_xact_lock(%s)', (worker.LOCK_ID,))
            result = self.process()
            self.assertTrue(result['locked'])
            self.assertEqual(result['processed'], 0)
            self.assertEqual(self.jobs(), [])
        result = self.process()
        self.assertEqual((result['enqueued'], result['processed'], result['errors']), (8, 8, 0))


if __name__ == '__main__':
    unittest.main()
