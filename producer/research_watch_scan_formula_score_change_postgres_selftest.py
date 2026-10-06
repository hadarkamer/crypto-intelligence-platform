"""Causal prior-scan score changes and rollout on disposable PostgreSQL.

Only the guarded TEST_DATABASE_URL fixture runs. Frozen source observations,
not outcome or formula readiness, determine score-change availability.
"""
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
import research_watch_scan_formula_score_change as change
import research_watch_scan_formula_worker as worker
import research_watch_scan_formula_asset_context_postgres_selftest as asset_tests
import research_watch_scan_formula_btc_context_postgres_selftest as btc_tests
import research_watch_scan_formula_postgres_selftest as formula_tests
import research_watch_score_capture_selftest as capture_tests
from research_watch_score_capture_selftest import BASE


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'), 'Explicit local/CI PostgreSQL required')
class WatchScanScoreChangePostgresTests(unittest.TestCase):
    # Reuse the guarded disposable fixture only, without inheriting its tests.
    connect = asset_tests.WatchScanAssetContextPostgresTests.connect
    drop_database = asset_tests.WatchScanAssetContextPostgresTests.drop_database
    consume_intake = asset_tests.WatchScanAssetContextPostgresTests.consume_intake
    formula_bundle = asset_tests.WatchScanAssetContextPostgresTests.formula_bundle
    persist_on = asset_tests.WatchScanAssetContextPostgresTests.persist_on
    persist = asset_tests.WatchScanAssetContextPostgresTests.persist
    samples = asset_tests.WatchScanAssetContextPostgresTests.samples
    evaluations = asset_tests.WatchScanAssetContextPostgresTests.evaluations
    active_version = asset_tests.WatchScanAssetContextPostgresTests.active_version
    own_bars = asset_tests.WatchScanAssetContextPostgresTests.own_bars
    boundary = staticmethod(asset_tests.WatchScanAssetContextPostgresTests.boundary)
    decision_map = staticmethod(asset_tests.WatchScanAssetContextPostgresTests.decision_map)
    assert_previous_decisions = asset_tests.WatchScanAssetContextPostgresTests.assert_previous_decisions

    def setUp(self):
        asset_tests.WatchScanAssetContextPostgresTests.setUp(self)
        with self.connect() as conn:
            conn.execute((Path(__file__).parent / 'migrations/052_watch_scan_score_change_formulas.sql').read_text(),
                         prepare=False)

    def process(self, *, version=change, limit=16, now=None):
        with patch.object(worker, 'formula', version), self.connect() as conn:
            return worker.process_page(conn, now=now or BASE + timedelta(days=2), limit=limit)

    def finish(self, *, version=change, now=None):
        ready = 0
        for _ in range(12):
            result = self.process(version=version, now=now)
            self.assertEqual(result['errors'], 0)
            ready += result['ready']
            if not result['processed']:
                return ready
        self.fail('Fixture did not finish within the bounded scan budget')

    def persist_scan(self, cycle_id, *, delay_minutes=0, observed_delay_minutes=None,
                     score=70., scores=None, unknown=()):
        # Unlike the old delayed-commit helper, move the actual capture time
        # as well. A separate observation offset exercises delayed source use.
        observed_delay = delay_minutes if observed_delay_minutes is None else observed_delay_minutes
        shifted = BASE + timedelta(minutes=observed_delay)
        with patch.object(capture_tests, 'BASE', shifted), patch.object(formula_tests, 'BASE', shifted):
            block = self.formula_bundle(cycle_id, score=score, scores=scores, unknown=unknown)
            return self.persist(cycle_id, block=block, delay_minutes=delay_minutes-observed_delay)

    def previous_versions(self, cycle_id, **kwargs):
        snapshot_id = self.persist_scan(cycle_id, **kwargs)
        self.assertEqual(self.consume_intake()['accepted'], 1)
        for version in (legacy, maxpain, btc, asset):
            self.assertEqual(self.process(version=version)['ready'], 8)
        self.assertEqual(self.active_version(), asset.VERSION)
        return snapshot_id

    def current_payloads(self, snapshot_id):
        return {row['symbol']: row['payload'] for row in self.samples(change.VERSION)
                if row['snapshot_set_id'] == snapshot_id and row['status'] == 'READY'}

    @staticmethod
    def context_ids(payload):
        return [row['snapshot_set_id'] for row in payload['score_change_context_provenance']['predecessors']]

    def test_partial_v5_keeps_v4_current_and_preserves_158_decisions_and_missing_price_proof(self):
        self.previous_versions('score-version-switch')
        versions = (legacy.VERSION, maxpain.VERSION, btc.VERSION, asset.VERSION)
        before = {version: self.samples(version) for version in versions}
        self.assertEqual(len(self.evaluations()), 8 * 316)
        # Enrich previously missing historical prices before upgrading. v5
        # must preserve the frozen v4 missing proof, not silently repair it.
        self.own_bars()
        with patch.object(archive, 'read_path', side_effect=AssertionError('Reuse frozen v4 price context')):
            result = self.process(limit=3)
            self.assertEqual((result['ready'], result['errors']), (3, 0))
            self.assertEqual(self.active_version(), asset.VERSION)
            self.assertEqual(len(self.evaluations()), 8 * 316)
            result = self.process()
        self.assertEqual((result['ready'], result['errors']), (5, 0))
        self.assertEqual(self.active_version(), change.VERSION)
        current = self.evaluations()
        self.assertEqual(len(current), 8 * 340)
        self.assertEqual({row['evaluation_version'] for row in current}, {change.VERSION})
        self.assertEqual(len(self.evaluations(by_version=True)), 8 * (68 + 164 + 212 + 316 + 340))
        for version, old in before.items():
            self.assertEqual(self.samples(version), old)
        after = self.samples(change.VERSION)
        self.assert_previous_decisions(before[asset.VERSION], after)
        old_by_coin = {row['symbol']: row['payload'] for row in before[asset.VERSION]}
        for row in after:
            for key in ('btc_context_provenance', 'asset_context_provenance'):
                self.assertEqual(row['payload'][key], old_by_coin[row['symbol']][key])
        with self.connect() as conn:
            counts = conn.execute('''SELECT count(*) AS total,count(*) FILTER (WHERE supported) AS supported
                FROM research_watch_scan_formula_catalog WHERE evaluation_version=%s''', (change.VERSION,)).fetchone()
            self.assertEqual(counts, {'total': 298, 'supported': 170})
            conn.execute((Path(__file__).parent / 'migrations/052_watch_scan_score_change_formulas.sql').read_text(),
                         prepare=False)
        self.assertEqual(self.process(version=asset)['processed'], 0)
        self.assertEqual(self.active_version(), change.VERSION)

    def test_newly_admitted_predecessor_is_used_before_any_formula_ready_without_outcome_reads(self):
        current = self.previous_versions('score-current-first', delay_minutes=30, score=70.)
        prior = self.persist_scan('score-prior-arrives-later', score=50.)
        self.assertEqual(self.consume_intake()['accepted'], 1)
        with self.connect() as conn:
            self.assertEqual(conn.execute('''SELECT count(*) AS n FROM research_watch_scan_formula_samples
                WHERE snapshot_set_id=%s AND status='READY' ''', (prior,)).fetchone()['n'], 0)
            recording = btc_tests.RecordingConnection(conn)
            with patch.object(worker, 'formula', change):
                result = worker.process_page(recording, now=BASE + timedelta(days=2), limit=8)
        self.assertEqual((result['ready'], result['errors']), (8, 0))
        sql = ' '.join(recording.sql).lower()
        for forbidden in ('research_watch_scan_measurements', 'research_events',
                          'research_event_btc_movements', 'research_watch_scan_formula_wave_outcomes'):
            self.assertNotIn(forbidden, sql)
        discovery = [query for query in recording.sql if 'FROM research_watch_scan_intakes WHERE' in query
                     and 'ORDER BY usable_from_utc DESC' in query]
        self.assertEqual(len(discovery), 1)
        self.assertIn('LIMIT 2', discovery[0])
        prior_reads = [query for query in recording.sql if 'research_watch_scan_observations' in query
                       and 'symbol=%s' in query]
        self.assertEqual(len(prior_reads), 8)
        payloads = self.current_payloads(current)
        self.assertEqual(len(payloads), 8)
        for payload in payloads.values():
            self.assertEqual(self.context_ids(payload), [prior])
            self.assertEqual(payload['score_change_context_provenance']['selection_status'], 'SELECTED')
            for base, expected in (('LONG', 20.), ('SHORT', -20.)):
                features = payload['features_by_direction'][base]['features']
                for feature in change.SCORE_CHANGE_FEATURES:
                    self.assertEqual(features[feature], expected)
        with self.connect() as conn:
            self.assertEqual(conn.execute('''SELECT count(*) AS n FROM research_watch_scan_formula_samples
                WHERE snapshot_set_id=%s AND status='READY' ''', (prior,)).fetchone()['n'], 0)

    def test_exact_30m_boundary_latest_ties_and_strictly_earlier_observation_are_enforced(self):
        lower = self.persist_scan('score-boundary-prior', score=40.)
        at_boundary = self.persist_scan('score-boundary-current', delay_minutes=30, score=70.)
        self.persist_scan('score-outside-prior', delay_minutes=120, score=40.)
        outside = self.persist_scan('score-outside-current', delay_minutes=151, score=70.)
        tied = [self.persist_scan('score-tie-' + str(i), delay_minutes=240,
                                 observed_delay_minutes=238+i, score=40.+i) for i in (0, 1)]
        tie_current = self.persist_scan('score-tie-current', delay_minutes=270, score=70.)
        self.persist_scan('score-equal-observation-prior', delay_minutes=380,
                          observed_delay_minutes=370, score=40.)
        same_observed = self.persist_scan('score-equal-observation-current', delay_minutes=390,
                                         observed_delay_minutes=370, score=70.)
        accepted = 0
        for _ in range(4):
            page = self.consume_intake()
            self.assertEqual(page['rejected'], 0)
            accepted += page['accepted']
            if accepted == 9:
                break
        self.assertEqual(accepted, 9)
        self.assertEqual(self.finish(), 9 * 8)
        cases = ((at_boundary, 'SELECTED', [lower]), (outside, 'NO_PREDECESSOR', []),
                 (tie_current, 'AMBIGUOUS', tied), (same_observed, 'NO_PREDECESSOR', []))
        for snapshot, status, ids in cases:
            with self.subTest(snapshot=snapshot, status=status):
                payloads = self.current_payloads(snapshot)
                self.assertEqual(len(payloads), 8)
                for payload in payloads.values():
                    self.assertEqual(payload['score_change_context_provenance']['selection_status'], status)
                    self.assertCountEqual(self.context_ids(payload), ids)
                    for base in ('LONG', 'SHORT'):
                        evidence = payload['features_by_direction'][base]
                        if status != 'SELECTED':
                            self.assertTrue(change.SCORE_CHANGE_FEATURES.isdisjoint(evidence['features']))
                            self.assertTrue(change.SCORE_CHANGE_FEATURES.issubset(evidence['unavailable_features']))
        for payload in self.current_payloads(at_boundary).values():
            self.assertEqual(payload['features_by_direction']['LONG']['features'][
                'sequence.30m.spot_cvd.score_change'], 30.)

    def test_split_pass_keeps_frozen_no_predecessor_when_eligible_source_arrives_later(self):
        current = self.persist_scan('score-split-current', delay_minutes=30, score=70.)
        self.assertEqual(self.consume_intake()['accepted'], 1)
        self.assertEqual(self.process(limit=3)['ready'], 3)
        first = self.current_payloads(current)
        self.assertEqual(len(first), 3)
        self.assertTrue(all(payload['score_change_context_provenance']['selection_status'] == 'NO_PREDECESSOR'
                            for payload in first.values()))
        prior = self.persist_scan('score-split-late-prior', score=50.)
        self.assertEqual(self.consume_intake()['accepted'], 1)
        now = BASE + timedelta(days=2, minutes=1)
        result = self.process(limit=5, now=now)
        self.assertEqual((result['ready'], result['errors']), (5, 0))
        completed = self.current_payloads(current)
        self.assertEqual(len(completed), 8)
        self.assertEqual({symbol: completed[symbol] for symbol in first}, first)
        for payload in completed.values():
            self.assertEqual(self.context_ids(payload), [])
            self.assertEqual(payload['score_change_context_provenance']['selection_status'], 'NO_PREDECESSOR')
            for base in ('LONG', 'SHORT'):
                self.assertTrue(change.SCORE_CHANGE_FEATURES.isdisjoint(
                    payload['features_by_direction'][base]['features']))
        self.assertEqual(self.finish(now=now), 8)
        self.assertEqual(self.current_payloads(current), completed)
        self.assertEqual(len(self.current_payloads(prior)), 8)

    def test_nearest_missing_module_does_not_fall_back_and_hype_captured_score_changes_work(self):
        oldest = self.persist_scan('score-oldest-valid', score=10.)
        nearest = self.persist_scan('score-nearest-missing', delay_minutes=10, score=60.,
                                    unknown={('ETH', 'spot_flow')})
        current = self.persist_scan('score-nearest-current', delay_minutes=20, score=70.)
        self.assertEqual(self.consume_intake()['accepted'], 3)
        self.assertEqual(self.finish(), 24)
        payloads = self.current_payloads(current)
        for payload in payloads.values():
            self.assertEqual(self.context_ids(payload), [nearest])
            self.assertNotIn(oldest, self.context_ids(payload))
        eth = payloads['ETH']
        missing = 'sequence.30m.spot_cvd.score_change'
        for base, expected in (('LONG', 10.), ('SHORT', -10.)):
            evidence = eth['features_by_direction'][base]
            self.assertNotIn(missing, evidence['features'])
            self.assertIn(missing, evidence['unavailable_features'])
            self.assertEqual(evidence['features']['sequence.30m.price_oi.score_change'], expected)
            self.assertEqual(evidence['features']['sequence.30m.futures_cvd.score_change'], expected)
            hype = payloads['HYPE']['features_by_direction'][base]
            for feature in change.SCORE_CHANGE_FEATURES:
                self.assertEqual(hype['features'][feature], expected)
            self.assertEqual(hype['features']['spot_cvd.aligned_score'], 70. if base == 'LONG' else -70.)
            self.assertTrue(asset.ASSET_FEATURES.isdisjoint(hype['features']))
        self.assertTrue(any(row['match_status'] == 'UNKNOWN' and missing in row['missing_features']
                            for row in eth['evaluations']))

    def test_prior_observation_sql_failure_isolated_per_coin_retry_recovers_and_old_adapters_do_not_read_prior_scores(self):
        prior = self.previous_versions('score-read-prior', score=50.)
        current = self.persist_scan('score-read-current', delay_minutes=20, score=70.)
        self.assertEqual(self.consume_intake()['accepted'], 1)
        for version in (legacy, maxpain, btc, asset):
            self.assertEqual(self.process(version=version)['ready'], 8)
        old = self.samples(asset.VERSION)
        # Complete the earlier source, so this pass contains only current jobs.
        self.assertEqual(self.process(limit=8)['ready'], 8)

        class FaultConnection(btc_tests.RecordingConnection):
            def execute(inner, query, *args, **kwargs):
                query_text = str(query).lower()
                params = args[0] if args else ()
                if ('research_watch_scan_observations' in query_text and 'symbol=%s' in query_text
                        and 'ETH' in params):
                    inner.conn.execute('SELECT 1 / 0')
                return super().execute(query, *args, **kwargs)

        with self.connect() as conn, patch.object(worker, 'formula', change):
            recording = FaultConnection(conn)
            result = worker.process_page(recording, now=BASE + timedelta(days=2))
        self.assertEqual((result['ready'], result['errors']), (7, 1))
        self.assertEqual(self.active_version(), asset.VERSION)
        before = self.samples(change.VERSION)
        failed = next(row for row in before if row['snapshot_set_id'] == current and row['symbol'] == 'ETH')
        self.assertEqual((failed['status'], failed['last_error']), ('ERROR', 'DivisionByZero'))
        peers = [row for row in before if (row['snapshot_set_id'], row['symbol']) != (current, 'ETH')]
        self.assertEqual(self.process(now=BASE + timedelta(days=2, minutes=4))['processed'], 0)
        result = self.process(now=BASE + timedelta(days=2, minutes=5))
        self.assertEqual((result['ready'], result['errors']), (1, 0))
        self.assertEqual(self.active_version(), change.VERSION)
        after = self.samples(change.VERSION)
        self.assertEqual([row for row in after if (row['snapshot_set_id'], row['symbol']) != (current, 'ETH')], peers)
        self.assertEqual(self.samples(asset.VERSION), old)
        payload = self.current_payloads(current)['ETH']
        self.assertEqual(self.context_ids(payload), [prior])
        self.assertEqual(payload['features_by_direction']['LONG']['features'][
            'sequence.30m.spot_cvd.score_change'], 20.)
        # New sources exercise old adapters; they may use their own historical
        # price paths, but never this new predecessor-score lookup.
        self.persist_scan('score-old-adapters', delay_minutes=40, score=80.)
        self.consume_intake()
        for version in (legacy, maxpain, btc, asset):
            with self.connect() as conn, patch.object(worker, 'formula', version):
                recording = btc_tests.RecordingConnection(conn)
                self.assertEqual(worker.process_page(recording, now=BASE + timedelta(days=2))['ready'], 8)
            sql = ' '.join(recording.sql).lower()
            self.assertNotIn('score_change_context_provenance', sql)
            self.assertFalse(any('research_watch_scan_observations' in query.lower()
                                 and 'symbol=%s' in query.lower() for query in recording.sql))
        self.assertEqual(self.active_version(), change.VERSION)


if __name__ == '__main__':
    unittest.main()
