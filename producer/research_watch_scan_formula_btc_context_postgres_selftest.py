"""Causal shared BTC context and versioned rollout on disposable PostgreSQL.

Only the guarded TEST_DATABASE_URL fixture is used. Real archive reads and
immutable formula writes run without external prices, outcome reads, or alerts.
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
import research_watch_scan_formula_worker as worker
import research_watch_scan_formula_maxpain_postgres_selftest as maxpain_tests
import research_watch_scan_measurement_worker as measurement_worker
from research_watch_score_capture_selftest import BASE


class RecordingConnection:
    """Retain SQL text only; preserve the real transaction/savepoint behavior."""
    def __init__(self, conn):
        self.conn = conn
        self.sql = []

    def execute(self, query, *args, **kwargs):
        self.sql.append(str(query))
        return self.conn.execute(query, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self.conn, name)


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'), 'Explicit local/CI PostgreSQL required')
class WatchScanBtcContextPostgresTests(unittest.TestCase):
    # Reuse fixture helpers, never inherit the previous version's test methods.
    connect = maxpain_tests.WatchScanMaxPainPostgresTests.connect
    drop_database = maxpain_tests.WatchScanMaxPainPostgresTests.drop_database
    consume_intake = maxpain_tests.WatchScanMaxPainPostgresTests.consume_intake
    seed_bars = maxpain_tests.WatchScanMaxPainPostgresTests.seed_bars
    seed_wave = maxpain_tests.WatchScanMaxPainPostgresTests.seed_wave
    formula_bundle = maxpain_tests.WatchScanMaxPainPostgresTests.formula_bundle
    persist_on = maxpain_tests.WatchScanMaxPainPostgresTests.persist_on
    persist = maxpain_tests.WatchScanMaxPainPostgresTests.persist
    samples = maxpain_tests.WatchScanMaxPainPostgresTests.samples
    evaluations = maxpain_tests.WatchScanMaxPainPostgresTests.evaluations
    active_version = maxpain_tests.WatchScanMaxPainPostgresTests.active_version

    def setUp(self):
        maxpain_tests.WatchScanMaxPainPostgresTests.setUp(self)
        with self.connect() as conn:
            conn.execute((Path(__file__).parent / 'migrations/050_watch_scan_btc_context_formulas.sql').read_text(),
                         prepare=False)

    def process(self, *, version=btc, limit=16, now=None):
        with patch.object(worker, 'formula', version), self.connect() as conn:
            return worker.process_page(conn, now=now or BASE + timedelta(days=2), limit=limit)

    @staticmethod
    def boundary(delay_minutes=0):
        return BASE + timedelta(minutes=7 + delay_minutes)

    def prior_bars(self, *, delay_minutes=0, direction='UP', omit=(), poison=False):
        """A 240-minute monotonic path has independently known sign/regime."""
        boundary = self.boundary(delay_minutes)
        rows = []
        for minute in range(240):
            if minute in omit:
                continue
            opened = boundary + timedelta(minutes=minute - 240)
            first = 100. + (minute if direction == 'UP' else 240 - minute) / 100.
            last = first + (0.01 if direction == 'UP' else -0.01)
            rows.append(dict(open_time_utc=opened,
                close_time_utc=opened + timedelta(minutes=1, milliseconds=-1),
                open=first, high=max(first, last), low=min(first, last), close=last, volume=1.))
        if poison:
            for minute in (0, 1):
                opened = boundary + timedelta(minutes=minute)
                rows.append(dict(open_time_utc=opened,
                    close_time_utc=opened + timedelta(minutes=1, milliseconds=-1),
                    open=100., high=1000000., low=0.01, close=0.01, volume=1.))
        with self.connect() as conn:
            archive.write_bars(conn, archive.BINANCE_SPOT, 'BTC', rows)
        return rows

    def seed_previous_versions(self, cycle_id):
        snapshot_id = self.persist(cycle_id)
        self.assertEqual(self.consume_intake()['accepted'], 1)
        with patch.object(archive, 'read_path', side_effect=AssertionError('Old versions must not read BTC paths')):
            self.assertEqual(self.process(version=legacy)['ready'], 8)
            self.assertEqual(self.process(version=maxpain)['ready'], 8)
        self.assertEqual(self.active_version(), maxpain.VERSION)
        return snapshot_id

    @staticmethod
    def decision_map(rows):
        return {(r['snapshot_set_id'], r['symbol'], e['candidate_key'], e['base_direction']):
                (e['match_status'], e['analysis_direction'], e['missing_features'])
                for r in rows for e in r['payload']['evaluations']}

    def test_partial_v3_keeps_v2_current_then_switches_without_rewriting_either_old_version(self):
        self.seed_previous_versions('btc-version-switch')
        self.prior_bars()
        old = {version: self.samples(version) for version in (legacy.VERSION, maxpain.VERSION)}
        previous_evaluations = self.evaluations()
        self.assertEqual(len(previous_evaluations), 8 * 164)

        partial = self.process(limit=3)
        self.assertEqual((partial['ready'], partial['errors']), (3, 0))
        self.assertEqual(self.active_version(), maxpain.VERSION)
        self.assertCountEqual(self.evaluations(), previous_evaluations)
        self.assertEqual(sum(s['status'] == 'READY' for s in self.samples(btc.VERSION)), 3)
        completed = self.process()
        self.assertEqual((completed['ready'], completed['errors']), (5, 0))
        self.assertEqual(self.active_version(), btc.VERSION)
        current = self.evaluations()
        self.assertEqual(len(current), 8 * 212)
        self.assertEqual({e['evaluation_version'] for e in current}, {btc.VERSION})
        self.assertEqual(len(self.evaluations(by_version=True)), 8 * (68 + 164 + 212))
        for version, before in old.items():
            self.assertEqual(self.samples(version), before)
        old_map = self.decision_map(old[maxpain.VERSION])
        new_map = self.decision_map(self.samples(btc.VERSION))
        self.assertEqual({key: new_map[key] for key in old_map}, old_map)
        with self.connect() as conn:
            counts = conn.execute('''SELECT evaluation_version,count(*) AS total,
                count(*) FILTER (WHERE supported) AS supported
                FROM research_watch_scan_formula_catalog GROUP BY evaluation_version''').fetchall()
            self.assertEqual({r['evaluation_version']: (r['total'], r['supported']) for r in counts},
                {legacy.VERSION: (298, 34), maxpain.VERSION: (298, 82), btc.VERSION: (298, 106)})
            conn.execute((Path(__file__).parent / 'migrations/050_watch_scan_btc_context_formulas.sql').read_text(),
                         prepare=False)
        self.assertEqual(self.process(version=maxpain)['processed'], 0)
        self.assertEqual(self.active_version(), btc.VERSION)
        self.assertEqual(len(self.evaluations()), 1696)

    def test_one_btc_read_is_shared_by_all_coins_without_outcome_sql_and_hype_keeps_perp(self):
        self.persist('shared-btc-context')
        self.consume_intake()
        self.prior_bars()
        original_read = archive.read_path
        with patch.object(archive, 'read_path', wraps=original_read) as reader, self.connect() as conn:
            recording = RecordingConnection(conn)
            with patch.object(worker, 'formula', btc):
                result = worker.process_page(recording, now=BASE + timedelta(days=2))
        self.assertEqual((result['ready'], result['errors']), (8, 0))
        self.assertEqual(reader.call_count, 1)
        route, symbol, start, end = reader.call_args.args[:4]
        self.assertEqual((route, symbol, start, end), (archive.BINANCE_SPOT, 'BTC',
            self.boundary() - timedelta(minutes=240), self.boundary() - timedelta(milliseconds=1)))
        sql = ' '.join(recording.sql).lower()
        for forbidden in ('research_watch_scan_measurements', 'research_events',
                          'research_event_btc_movements', 'research_watch_scan_formula_wave_outcomes'):
            self.assertNotIn(forbidden, sql)
        rows = self.samples(btc.VERSION)
        contexts = [r['payload']['btc_context_provenance'] for r in rows]
        self.assertTrue(all(context == contexts[0] for context in contexts))
        for row in rows:
            for base in ('LONG', 'SHORT'):
                features = row['payload']['features_by_direction'][base]['features']
                self.assertEqual(features[btc.BTC_DIRECTION], 'UP')
                self.assertEqual(features[btc.BTC_REGIME], 'UP')
        context = contexts[0]
        self.assertEqual(context['windows']['1h']['path_samples'], 60)
        self.assertEqual(context['windows']['4h']['path_samples'], 240)
        self.assertAlmostEqual(context['windows']['1h']['return_pct'], 100 * (102.4 / 101.8 - 1))
        self.assertAlmostEqual(context['windows']['4h']['return_pct'], 2.4)
        self.assertAlmostEqual(context['windows']['4h']['market_regime_efficiency'], 1.)
        # Outcome computation is a separate later operation with its own route.
        self.seed_wave()
        self.seed_bars(symbols=('HYPE',), count=1440)
        with self.connect() as conn:
            self.assertEqual(measurement_worker.process_page(conn, now=BASE + timedelta(days=2))['ready'], 1)
            hype = conn.execute("SELECT payload FROM research_watch_scan_measurements WHERE symbol='HYPE'").fetchone()['payload']
            self.assertEqual(hype['price_route'], archive.HYPERLIQUID_PERP)
            self.assertEqual(conn.execute('SELECT count(*) AS n FROM research_events').fetchone()['n'], 0)
        self.assertEqual(self.samples(btc.VERSION), rows)

    def test_missing_old_bar_preserves_one_hour_context_and_all_82_legacy_decisions(self):
        self.seed_previous_versions('partial-btc-lookback')
        self.prior_bars(omit=(10,))
        old = self.samples(maxpain.VERSION)
        self.assertEqual(self.process()['ready'], 8)
        current = self.samples(btc.VERSION)
        old_map, new_map = self.decision_map(old), self.decision_map(current)
        self.assertEqual({key: new_map[key] for key in old_map}, old_map)
        for row in current:
            context = row['payload']['btc_context_provenance']
            self.assertEqual(context['windows']['1h']['status'], 'READY')
            self.assertEqual(context['windows']['4h']['status'], 'DATA_MISSING')
            self.assertEqual(context['windows']['4h']['path_samples'], 239)
            for base in ('LONG', 'SHORT'):
                evidence = row['payload']['features_by_direction'][base]
                self.assertEqual(evidence['features'][btc.BTC_DIRECTION], 'UP')
                self.assertNotIn(btc.BTC_REGIME, evidence['features'])
                self.assertIn(btc.BTC_REGIME, evidence['unavailable_features'])
            affected = [e for e in row['payload']['evaluations'] if btc.BTC_REGIME in e['missing_features']]
            self.assertTrue(affected)
            self.assertTrue(any(e['match_status'] == 'UNKNOWN' for e in affected))
        # Late archive repair cannot rewrite already-frozen feature decisions.
        self.prior_bars()
        self.assertEqual(self.process(now=BASE + timedelta(days=2, minutes=6))['processed'], 0)
        self.assertEqual(self.samples(btc.VERSION), current)

    def test_partial_pass_reuses_first_frozen_context_after_archive_gap_is_repaired(self):
        self.seed_previous_versions('btc-partial-pass-repair')
        self.prior_bars(omit=(10,))
        self.assertEqual(self.process(limit=3)['ready'], 3)
        partial = [row for row in self.samples(btc.VERSION) if row['status'] == 'READY']
        self.assertEqual(len(partial), 3)
        frozen = partial[0]['payload']['btc_context_provenance']
        self.assertTrue(all(row['payload']['btc_context_provenance'] == frozen for row in partial))
        self.assertEqual(frozen['windows']['1h']['status'], 'READY')
        self.assertEqual(frozen['windows']['4h']['status'], 'DATA_MISSING')
        self.assertEqual(self.active_version(), maxpain.VERSION)

        self.prior_bars()
        with self.connect() as conn:
            repaired = archive.read_path(archive.BINANCE_SPOT, 'BTC',
                self.boundary() - timedelta(minutes=240),
                self.boundary() - timedelta(milliseconds=1), connection=conn)
            self.assertTrue(repaired['complete'])
            self.assertEqual(len(repaired['candles']), 240)
        with patch.object(archive, 'read_path', side_effect=AssertionError('A frozen sibling owns the scan context')) as reader:
            result = self.process(now=BASE + timedelta(days=2, minutes=1))
        self.assertEqual((result['ready'], result['errors']), (5, 0))
        self.assertEqual(reader.call_count, 0)
        completed = self.samples(btc.VERSION)
        self.assertEqual(len(completed), 8)
        self.assertTrue(all(row['status'] == 'READY' for row in completed))
        self.assertTrue(all(row['payload']['btc_context_provenance'] == frozen for row in completed))
        self.assertEqual({row['payload']['btc_context_provenance']['context_sha256'] for row in completed},
                         {frozen['context_sha256']})
        for row in completed:
            for base in ('LONG', 'SHORT'):
                evidence = row['payload']['features_by_direction'][base]
                self.assertEqual(evidence['features'][btc.BTC_DIRECTION], 'UP')
                self.assertNotIn(btc.BTC_REGIME, evidence['features'])
                self.assertIn(btc.BTC_REGIME, evidence['unavailable_features'])
        self.assertEqual(self.active_version(), btc.VERSION)
        self.assertEqual(len(self.evaluations()), 8 * 106 * 2)

    def test_distinct_receipts_have_distinct_past_bounds_and_ignore_current_future_extrema(self):
        first = self.persist('btc-up')
        second = self.persist('btc-down', delay_minutes=300)
        self.assertEqual(self.consume_intake()['accepted'], 2)
        self.prior_bars(direction='UP', poison=True)
        self.prior_bars(delay_minutes=300, direction='DOWN', poison=True)
        original_read = archive.read_path
        calls = []

        def read_with_future_poison(route, symbol, start, end, **kwargs):
            calls.append((route, symbol, start, end))
            path = deepcopy(original_read(route, symbol, start, end, **kwargs))
            # Even an overbroad supplied path must not contaminate past features.
            opened = end + timedelta(milliseconds=1)
            path['candles'].append(dict(open_time_utc=opened,
                close_time_utc=opened + timedelta(minutes=1, milliseconds=-1),
                open=100., high=1000000., low=0.01, close=0.01, volume=1.))
            return path

        with patch.object(archive, 'read_path', side_effect=read_with_future_poison):
            result = self.process()
        self.assertEqual((result['ready'], result['errors']), (16, 0))
        self.assertEqual(calls, [(archive.BINANCE_SPOT, 'BTC', self.boundary(delay) - timedelta(minutes=240),
            self.boundary(delay) - timedelta(milliseconds=1)) for delay in (0, 300)])
        contexts = {}
        for row in self.samples(btc.VERSION):
            expected = 'UP' if row['snapshot_set_id'] == first else 'DOWN'
            self.assertIn(row['snapshot_set_id'], (first, second))
            context = row['payload']['btc_context_provenance']
            contexts.setdefault(row['snapshot_set_id'], context)
            self.assertEqual(context, contexts[row['snapshot_set_id']])
            for base in ('LONG', 'SHORT'):
                features = row['payload']['features_by_direction'][base]['features']
                self.assertEqual(features[btc.BTC_DIRECTION], expected)
                self.assertEqual(features[btc.BTC_REGIME], expected)
            self.assertEqual(context['windows']['4h']['path_samples'], 240)
            self.assertLess(context['windows']['4h']['high'], 103.)
        self.assertNotEqual(contexts[first]['context_sha256'], contexts[second]['context_sha256'])

    def test_sql_read_failure_isolated_by_receipt_and_retry_recovers_after_savepoint(self):
        first = self.persist('btc-read-error')
        second = self.persist('btc-read-peer', delay_minutes=300)
        self.consume_intake()
        self.prior_bars()
        self.prior_bars(delay_minutes=300, direction='DOWN')
        original_read = archive.read_path
        calls = []

        def fail_first_receipt(route, symbol, start, end, **kwargs):
            calls.append(start)
            if end == self.boundary() - timedelta(milliseconds=1):
                kwargs['connection'].execute('SELECT 1 / 0')
            return original_read(route, symbol, start, end, **kwargs)

        with patch.object(archive, 'read_path', side_effect=fail_first_receipt):
            result = self.process()
        self.assertEqual((result['ready'], result['errors']), (8, 8))
        self.assertEqual(len(calls), 2)
        before = self.samples(btc.VERSION)
        self.assertTrue(all(row['status'] == ('ERROR' if row['snapshot_set_id'] == first else 'READY')
                            for row in before))
        peers = [row for row in before if row['snapshot_set_id'] == second]
        self.assertTrue(all(row['last_error'] == 'DivisionByZero' for row in before
                            if row['snapshot_set_id'] == first))
        self.assertEqual(self.process(now=BASE + timedelta(days=2, minutes=4))['processed'], 0)
        with patch.object(archive, 'read_path', wraps=original_read) as reader:
            result = self.process(now=BASE + timedelta(days=2, minutes=5))
        self.assertEqual((result['ready'], result['errors']), (8, 0))
        self.assertEqual(reader.call_count, 1)
        after = self.samples(btc.VERSION)
        self.assertTrue(all(row['status'] == 'READY' and row['last_error'] is None for row in after))
        self.assertEqual([row for row in after if row['snapshot_set_id'] == second], peers)


if __name__ == '__main__':
    unittest.main()
