"""Source export integration on isolated local/CI PostgreSQL only.

The fixtures enter through the real immutable Watch capture and intake APIs.
BTC parents are produced by the causal policy from eight bars, rather than
fabricated membership attestations. Four archived XRP minutes suffice to
exercise the complete export -> snapshot -> no-horizon replay path.

No production connection, price network request, alert sender or live worker
is used. Each test creates and drops a private database under an explicitly
supplied local TEST_DATABASE_URL (or RESEARCH_TEST_POSTGRES_URL).
"""
from copy import deepcopy
from datetime import timedelta
import os
import unittest
from unittest.mock import patch

import research_btc_parent_movement as btc_policy
import research_no_horizon_export as exporter
import research_no_horizon_preflight as preflight
import research_no_horizon_source as source
import research_no_horizon_replay as replay
import research_price_archive as price_archive
import research_watch_scan_measurement_postgres_selftest as fixtures
from research_watch_score_capture_selftest import BASE, bundle, derivatives, inputs


TEST_DSN = os.environ.get('TEST_DATABASE_URL') or os.environ.get('RESEARCH_TEST_POSTGRES_URL')


@unittest.skipUnless(TEST_DSN, 'Explicit local/CI PostgreSQL test database required')
class NoHorizonSourcePostgresTests(unittest.TestCase):
    connect = fixtures.WatchScanMeasurementPostgresTests.connect
    drop_database = fixtures.WatchScanMeasurementPostgresTests.drop_database
    persist_on = fixtures.WatchScanMeasurementPostgresTests.persist_on
    persist = fixtures.WatchScanMeasurementPostgresTests.persist
    consume_intake = fixtures.WatchScanMeasurementPostgresTests.consume_intake

    def setUp(self):
        selected = patch.dict(os.environ, {'TEST_DATABASE_URL': TEST_DSN})
        selected.start()
        self.addCleanup(selected.stop)
        # Creates a disposable database; checks host and test-only database name.
        # Reuses setup/fixture methods only, not the older test methods.
        fixtures.WatchScanMeasurementPostgresTests.setUp(self)
        self.start = BASE + timedelta(minutes=6)
        self.end = BASE + timedelta(minutes=8)
        self.cutoff = BASE + timedelta(minutes=11)

    def captured_bundle(self, cycle_id, *, futures_available=True):
        rows = inputs()
        for row in rows:
            if row['symbol'] == 'HYPE':
                row.update(price_source='binance_futures', price_pair='HYPEUSDT',
                           price_market='PERP', price_instrument='HYPEUSDT')
        snapshot = derivatives()
        for data in snapshot.values():
            for window in data['flow']['futures']['windows'].values():
                window['continuous_strength'] = 1.
            if not futures_available:
                data['flow']['futures'].update(available=False, windows={},
                                               quality={'status': 'NO_DATA'})
        block, *_ = bundle(rows=rows, snapshot=snapshot, cycle_id=cycle_id)
        if futures_available:
            self.assertEqual(block['coins']['XRP']['models']['futures_flow']['score'], -75)
        return block

    def admit(self, cycle_id='nh-source', *, futures_available=True):
        ident = self.persist(cycle_id, block=self.captured_bundle(
            cycle_id, futures_available=futures_available))
        counts = self.consume_intake()
        self.assertEqual((counts['accepted'], counts['rejected']), (1, 0))
        return ident

    def seed_causal_parent(self):
        from psycopg.types.json import Jsonb
        candles = []
        for minute, close in enumerate((100., 103., 100., 100., 100., 100., 100., 100.)):
            opened = BASE + timedelta(minutes=minute)
            candles.append(dict(open_time_utc=opened,
                close_time_utc=opened + timedelta(minutes=1, milliseconds=-1),
                open=close, high=close + .1, low=close - .1, close=close))
        parents = btc_policy.advance_parents(candles, as_of_utc=self.cutoff)
        self.assertEqual(sum(p['evidence_eligible'] for p in parents), 1)
        with self.connect() as conn:
            for bar in candles:
                conn.execute('''INSERT INTO research_btc_price_bars
                    (open_time_utc,close_time_utc,open,high,low,close)
                    VALUES (%s,%s,%s,%s,%s,%s)''',
                    tuple(bar[k] for k in ('open_time_utc','close_time_utc','open','high','low','close')))
            for parent in parents:
                columns = list(parent)
                values = [Jsonb(parent[k]) if k == 'state_json' else parent[k] for k in columns]
                conn.execute('INSERT INTO research_btc_parent_movements (' + ','.join(columns) +
                             ') VALUES (' + ','.join(['%s']*len(columns)) + ')', values)

    def seed_prices(self):
        bars = []
        for minute in range(4):
            opened = BASE + timedelta(minutes=7+minute)
            bars.append(dict(open_time_utc=opened,
                close_time_utc=opened + timedelta(minutes=1, milliseconds=-1),
                open=100., high=100.2, low=98.4 if minute == 1 else 99.8,
                close=100., volume=1.))
        with self.connect() as conn:
            price_archive.write_bars(conn, price_archive.BINANCE_SPOT, 'XRP', bars)

    def export(self, **overrides):
        kwargs = dict(start_utc=self.start, end_utc=self.end, cutoff_utc=self.cutoff,
                      symbol='XRP', row_limit=8, byte_limit=4*1024*1024, page_size=1)
        kwargs.update(overrides)
        with self.connect() as conn:
            result = exporter.export_source(conn, **kwargs)
            from psycopg.pq import TransactionStatus
            self.assertEqual(conn.info.transaction_status, TransactionStatus.IDLE)
            return result

    def snapshot(self, exported):
        return source.build_snapshot(exported, candidate_key='FUTURES_CVD_TOTAL_65',
                                     base_direction='SHORT', symbol='XRP', threshold_pct=1.5)

    def prepared(self):
        ident = self.admit()
        self.seed_causal_parent()
        self.seed_prices()
        return ident

    def assert_blocked(self, exported, expected_blocker):
        snapshot = self.snapshot(exported)
        self.assertIs(snapshot['source_coverage_complete'], False)
        self.assertIn(expected_blocker, snapshot['source_receipt']['source_blockers'])
        receipt = replay.replay_snapshot(snapshot)
        self.assertIs(receipt['gate']['experimental_eligible'], False)
        return snapshot

    def test_complete_real_capture_export_snapshot_and_replay(self):
        ident = self.prepared()

        def capture_state():
            with self.connect() as conn:
                counts = {table: conn.execute('SELECT count(*) AS n FROM ' + table).fetchone()['n']
                    for table in ('research_max_pain_snapshot_sets', 'research_max_pain_snapshot_rows',
                                  'research_max_pain_snapshot_symbols', 'research_watch_scan_intakes')}
                parent = conn.execute('SELECT * FROM research_max_pain_snapshot_sets WHERE snapshot_set_id=%s',
                                      (ident,)).fetchone()
                admitted = conn.execute('SELECT * FROM research_watch_scan_intakes WHERE snapshot_set_id=%s',
                                        (ident,)).fetchone()
                return counts, parent, admitted

        before = capture_state()
        exported = self.export()
        self.assertEqual(exported['export_version'], 'no-horizon-watch-source-export-v1')
        self.assertEqual(len(exported['source_rows']), 1)
        self.assertEqual(len(exported['candles']), 4)
        self.assertEqual(exported['source_receipt']['expected_accepted_rows'], 1)
        self.assertIs(exported['source_receipt']['rows_complete'], True)
        self.assertIs(exported['source_receipt']['truncated'], False)
        self.assertEqual(exported['source_rows'][0]['stored_source']['source'], 'WATCH_SHARED')
        snapshot = self.snapshot(exported)
        self.assertIs(snapshot['source_coverage_complete'], True)
        self.assertEqual(len(snapshot['opportunities']), 1)
        receipt = replay.replay_snapshot(snapshot, batch_size=1)
        self.assertIs(receipt['computation_complete'], True)
        self.assertEqual(receipt['status_counts']['SUCCESS'], 1)
        self.assertEqual(receipt['outcomes'][0]['outcome']['processed_candles'], 2)
        self.assertIs(receipt['gate']['experimental_eligible'], False)  # One wave is below minimum five.
        self.assertIs(receipt['trading_authorized'], False)

        # The new predicates must consume the original JSONB capture through
        # native intake/export SQL, without an auxiliary formula result table.
        self.assertEqual(exported['source_rows'][0]['scores'],
                         before[1]['source_metadata']['capture_metadata']['operational_scores'])
        prefix = 'captured-question-search-v3-experimental-binding:'
        average = prefix + 'average_score_all_timeframes_GE55'
        consensus = prefix + 'CONSENSUS_True'
        scopes = [{'candidate_key': candidate, 'base_direction': 'SHORT', 'threshold_pct': 1.5}
                  for candidate in (average, consensus)]
        features = preflight.preflight_source_features(exported, scopes)
        self.assertIs(features['ready_for_outcome_research'], True)
        self.assertEqual(features['feature_availability']['max_pain.average_score_all_timeframes']['SHORT']['available_rows'], 1)
        self.assertEqual(features['feature_availability']['max_pain.consensus_hits_full']['SHORT']['available_rows'], 1)
        by_candidate = {row['candidate_key']: row for row in features['scopes']}
        self.assertEqual(by_candidate[average]['counts'],
                         {'MATCH': 0, 'NO_MATCH': 1, 'UNKNOWN': 0, 'UNKNOWN_SOURCE': 0})
        self.assertEqual(by_candidate[consensus]['counts'],
                         {'MATCH': 1, 'NO_MATCH': 0, 'UNKNOWN': 0, 'UNKNOWN_SOURCE': 0})
        selected = source.build_snapshot(exported, candidate_key=consensus,
            base_direction='SHORT', symbol='XRP', threshold_pct=1.5)
        self.assertIs(selected['source_coverage_complete'], True)
        self.assertEqual(selected['source_receipt']['selection']['adapter_version'],
                         'no-horizon-accepted-watch-source-v3-maxpain')
        self.assertEqual(selected['source_receipt']['selection']['feature_version'],
                         'watch-captured-total-and-maxpain-features-v2')
        self.assertEqual(selected['source_receipt']['decision_ledger'][0]['feature_sha256'],
                         by_candidate[consensus]['decision_ledger'][0]['feature_sha256'])
        result = replay.replay_snapshot(selected, batch_size=1)
        self.assertIs(result['computation_complete'], True)
        self.assertEqual(result['status_counts']['SUCCESS'], 1)
        self.assertIs(result['trading_authorized'], False)
        self.assertEqual(capture_state(), before)

    def test_export_owns_a_real_readonly_repeatable_read_transaction(self):
        self.prepared()
        with self.connect() as conn:
            conn.execute('CREATE TABLE export_write_probe(value integer)')
        checked = []
        test = self

        class ProbeConnection:
            def __init__(self, wrapped):
                self.wrapped = wrapped

            def __getattr__(self, name):
                return getattr(self.wrapped, name)

            def execute(self, query, params=None, **kwargs):
                if not checked and str(query).lstrip().upper().startswith('SELECT'):
                    readonly = self.wrapped.execute('SHOW transaction_read_only').fetchone()
                    isolation = self.wrapped.execute('SHOW transaction_isolation').fetchone()
                    test.assertEqual(next(iter(readonly.values())), 'on')
                    test.assertEqual(next(iter(isolation.values())), 'repeatable read')
                    with test.assertRaises(test.psycopg.errors.ReadOnlySqlTransaction):
                        with self.wrapped.transaction():
                            self.wrapped.execute('INSERT INTO export_write_probe VALUES (1)')
                    checked.append(True)
                return self.wrapped.execute(query, params, **kwargs)

        with self.connect() as conn:
            result = exporter.export_source(ProbeConnection(conn), start_utc=self.start,
                end_utc=self.end, cutoff_utc=self.cutoff, symbol='XRP')
            self.assertEqual(checked, [True])
            self.assertEqual(len(result['source_rows']), 1)
            # Transaction-local mode must not poison the caller's next transaction.
            conn.execute('INSERT INTO export_write_probe VALUES (2)')
            self.assertEqual(conn.execute('SELECT count(*) AS n FROM export_write_probe').fetchone()['n'], 1)

    def test_overflow_fails_whole_export_and_exact_row_limit_succeeds(self):
        self.prepared()
        self.admit('nh-second')
        with self.assertRaisesRegex(ValueError, 'SOURCE_ROW_LIMIT_EXCEEDED'):
            self.export(row_limit=1)
        exported = self.export(row_limit=2)
        self.assertEqual(len(exported['source_rows']), 2)
        self.assertEqual(exported['source_receipt']['expected_accepted_rows'], 2)
        self.assertIs(exported['source_receipt']['rows_complete'], True)

    def test_small_byte_budget_cannot_yield_a_partial_success(self):
        self.prepared()
        with self.assertRaisesRegex(ValueError, 'BYTE_LIMIT|byte_limit'):
            self.export(byte_limit=1024)

    def test_rejected_feature_remains_accounted_and_blocks_population(self):
        self.admit(futures_available=False)
        self.seed_causal_parent()
        self.seed_prices()
        exported = self.export()
        self.assertEqual(len(exported['source_rows']), 1)
        self.assertEqual(exported['source_receipt']['expected_accepted_rows'], 1)
        snapshot = self.assert_blocked(exported, 'UNKNOWN_POTENTIALLY_MATCHING_FEATURES')
        self.assertEqual(snapshot['source_receipt']['counts']['UNKNOWN'], 1)
        self.assertEqual(len(snapshot['source_receipt']['decision_ledger']), 1)

    def test_known_no_match_is_distinct_from_unknown_without_inventing_prices(self):
        self.admit()
        exported = self.export()
        snapshot = source.build_snapshot(exported, candidate_key='FUTURES_CVD_TOTAL_65',
                                         base_direction='LONG', symbol='XRP', threshold_pct=1.5)
        self.assertIs(snapshot['source_coverage_complete'], True)
        self.assertEqual(snapshot['source_receipt']['counts']['NO_MATCH'], 1)
        self.assertEqual(snapshot['source_receipt']['counts']['UNKNOWN'], 0)
        self.assertEqual(len(snapshot['source_receipt']['decision_ledger']), 1)
        self.assertEqual(snapshot['opportunities'], [])
        self.assertEqual(snapshot['candles'], [])

    def test_missing_btc_membership_is_not_silently_dropped(self):
        self.admit()
        self.seed_prices()
        exported = self.export()
        self.assertEqual(len(exported['source_rows']), 1)
        self.assertEqual(exported['source_receipt']['expected_accepted_rows'], 1)
        self.assertIsNone(exported['source_rows'][0]['btc_parent'])
        snapshot = self.assert_blocked(exported, 'UNVERIFIED_MATCHED_BTC_PARENT')
        self.assertEqual(snapshot['source_receipt']['counts']['MATCH'], 1)
        self.assertEqual(len(snapshot['opportunities']), 1)

    def test_original_intake_hash_mismatch_is_retained_then_blocked(self):
        ident = self.prepared()
        with self.connect() as conn:
            conn.execute('UPDATE research_watch_scan_intakes SET parent_payload_sha256=%s WHERE snapshot_set_id=%s',
                         ('f'*64, ident))
            # The old convenience view hides the inconsistent row. Exporting
            # directly from accepted intake identities must retain its existence.
            self.assertEqual(conn.execute('SELECT count(*) AS n FROM research_watch_scan_observations').fetchone()['n'], 0)
        exported = self.export()
        self.assertEqual(len(exported['source_rows']), 1)
        self.assertEqual(exported['source_receipt']['expected_accepted_rows'], 1)
        snapshot = self.assert_blocked(exported, 'INVALID_OR_MISSING_FROZEN_SOURCE')
        self.assertEqual(snapshot['source_receipt']['counts']['UNKNOWN_SOURCE'], 1)
        self.assertEqual(len(snapshot['source_receipt']['decision_ledger']), 1)

    def test_transport_tamper_and_false_complete_receipt_fail_closed(self):
        self.prepared()
        exported = self.export()
        tampered = deepcopy(exported)
        tampered['source_rows'][0]['stored_source']['source'] = 'NOT_WATCH_SHARED'
        self.assert_blocked(tampered, 'INVALID_OR_MISSING_FROZEN_SOURCE')
        dropped = deepcopy(exported)
        dropped['source_rows'] = []
        self.assert_blocked(dropped, 'INCOMPLETE_OR_INCONSISTENT_SOURCE_EXTRACTION')

    def test_existing_transaction_is_rejected_without_committing_caller_work(self):
        self.prepared()
        with self.connect() as conn:
            conn.execute('CREATE TABLE caller_pending(value integer)')
            with self.assertRaisesRegex(ValueError, 'IDLE|idle|transaction'):
                exporter.export_source(conn, start_utc=self.start, end_utc=self.end,
                                       cutoff_utc=self.cutoff, symbol='XRP')
            conn.rollback()
            self.assertIsNone(conn.execute("SELECT to_regclass('caller_pending') AS t").fetchone()['t'])


if __name__ == '__main__':
    unittest.main()
