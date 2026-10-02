"""Single-statement multipart cohort anchors on disposable local PostgreSQL.

Reuse fixture methods by composition, not the older PostgreSQL test methods.
Every source is a genuine captured Watch intake with causal parent and archived
price fixtures. No production connection, provider request or outcome run.
"""
from copy import deepcopy
from datetime import timedelta
import hashlib
import json
import os
import unittest

import research_no_horizon_cohort as cohort
import research_no_horizon_contract as contracts
import research_no_horizon_manifest as transport
import research_no_horizon_manifest_sql_postgres_selftest as fixtures
import research_no_horizon_parent_coverage as coverage
import research_watch_scan_intake as intake
from research_watch_score_capture_selftest import BASE


TEST_DSN = os.environ.get('TEST_DATABASE_URL') or os.environ.get('RESEARCH_TEST_POSTGRES_URL')


def sql_hash(query):
    return hashlib.sha256(query.encode('utf-8')).hexdigest()


@unittest.skipUnless(TEST_DSN, 'Explicit local/CI PostgreSQL test database required')
class CohortPostgresTests(unittest.TestCase):
    setUp = fixtures.NoHorizonManifestSqlPostgresTests.setUp
    connect = fixtures.NoHorizonManifestSqlPostgresTests.connect
    drop_database = fixtures.NoHorizonManifestSqlPostgresTests.drop_database
    persist_on = fixtures.NoHorizonManifestSqlPostgresTests.persist_on
    persist = fixtures.NoHorizonManifestSqlPostgresTests.persist
    consume_intake = fixtures.NoHorizonManifestSqlPostgresTests.consume_intake
    captured_bundle = fixtures.NoHorizonManifestSqlPostgresTests.captured_bundle
    admit = fixtures.NoHorizonManifestSqlPostgresTests.admit
    seed_causal_parent = fixtures.NoHorizonManifestSqlPostgresTests.seed_causal_parent
    seed_prices = fixtures.NoHorizonManifestSqlPostgresTests.seed_prices
    prepared = fixtures.NoHorizonManifestSqlPostgresTests.prepared
    snapshot = fixtures.NoHorizonManifestSqlPostgresTests.snapshot
    read_only = fixtures.NoHorizonManifestSqlPostgresTests.read_only
    source_chunks = fixtures.NoHorizonManifestSqlPostgresTests.source_chunks
    candle_chunks = fixtures.NoHorizonManifestSqlPostgresTests.candle_chunks
    live_source = fixtures.NoHorizonManifestSqlPostgresTests.live_source
    store_parents = fixtures.NoHorizonManifestSqlPostgresTests.store_parents
    advance_parent = fixtures.NoHorizonManifestSqlPostgresTests.advance_parent

    def declaration(self, *, part_overrides=None):
        # All genuine source intakes have usable_from exactly minute seven.
        # An adjacent empty first part proves half-open partition boundaries
        # without inventing additional timestamps or source captures.
        boundary = BASE + timedelta(minutes=7)
        bounds = ((self.start, boundary), (boundary, self.end))
        parts = []
        for ordinal, (start, end) in enumerate(bounds):
            part = dict(ordinal=ordinal, source_start_utc=start.isoformat(),
                source_end_utc=end.isoformat(), source_row_limit=8,
                max_candle_rows=16, page_size=2, source_byte_limit=4 * 1024 * 1024)
            part.update((part_overrides or {}).get(ordinal, {}))
            parts.append(part)
        return dict(cohort_version=cohort.VERSION, cohort_key='postgres-multipart-fixture',
            declared_at_utc=self.cutoff.isoformat(), prior_outcomes_observed=False,
            symbol='XRP', price_route=transport.ROUTE,
            source_start_utc=self.start.isoformat(), source_end_utc=self.end.isoformat(),
            cutoff_utc=self.cutoff.isoformat(), gate_policy=coverage._policy(None),
            scopes=[dict(candidate_key='FUTURES_CVD_TOTAL_65', base_direction='SHORT', threshold_pct=1.5)],
            parts=parts)

    def capture(self, declaration, *, seal=True):
        query = cohort.anchor_sql(declaration)
        raw = self.read_only(query)['anchor']
        return cohort.seal_anchor(declaration, raw) if seal else raw

    def assembled_parts(self, anchor):
        exports = []
        for part in anchor['parts']:
            manifest = part['manifest']
            exports.append(transport.assemble_export(manifest,
                self.source_chunks(manifest), self.candle_chunks(manifest)))
        return exports

    def expected_source_ids(self):
        with self.connect() as conn:
            rows = conn.execute('''SELECT snapshot_set_id FROM research_watch_scan_intakes
                WHERE consumer_version=%s AND intake_status='ACCEPTED'
                  AND usable_from_utc>=%s AND usable_from_utc<%s
                ORDER BY usable_from_utc,snapshot_set_id''',
                (intake.VERSION, self.start, self.end)).fetchall()
        return [row['snapshot_set_id'] for row in rows]

    def test_one_statement_shared_snapshot_and_complete_half_open_partition_union(self):
        first = self.prepared()
        second = self.admit('multipart-second-capture')
        declaration = self.declaration()
        normalized = cohort.normalize_declaration(declaration)
        query = cohort.anchor_sql(declaration)
        raw = self.read_only(query)['anchor']
        self.assertEqual(raw['anchor_version'], cohort.ANCHOR_VERSION)
        self.assertEqual(raw['declaration_sha256'], contracts.digest(normalized))
        root = raw['extraction_receipt']
        self.assertEqual(root['transaction_mode'], 'SINGLE_STATEMENT_READ_ONLY')
        self.assertEqual(root['consistent_read'], 'ONE_STATEMENT_SNAPSHOT')
        self.assertEqual(root['read_only'], 'on')
        self.assertIs(root['database_writes'], False)
        self.assertEqual(root['serializer'], 'POSTGRESQL_JSONB_TEXT_UTF8_V1')
        self.assertEqual(root['serializer_timezone'], 'UTC')
        self.assertTrue(root['mvcc_snapshot'])
        self.assertEqual([part['ordinal'] for part in raw['parts']], [0, 1])
        for part in raw['parts']:
            receipt = part['manifest']['receipt']
            self.assertEqual(receipt['mvcc_snapshot'], root['mvcc_snapshot'])
            self.assertEqual(receipt['extracted_at_utc'], root['extracted_at_utc'])
            self.assertEqual(receipt['read_only'], root['read_only'])
            self.assertEqual(receipt['serializer'], root['serializer'])
            self.assertEqual(receipt['serializer_timezone'], root['serializer_timezone'])
        self.assertEqual(raw['parts'][0]['manifest']['receipt']['source_count'], 0)
        self.assertEqual(raw['parts'][1]['manifest']['receipt']['source_count'], 2)

        anchor = cohort.seal_anchor(declaration, raw)
        cohort.validate_anchor(declaration, anchor)
        self.assertEqual(anchor['extraction_receipt']['query_sha256'], sql_hash(query))
        for part in anchor['parts']:
            self.assertEqual(part['manifest']['receipt']['query_sha256'], sql_hash(query))
        exported = self.assembled_parts(anchor)
        self.assertEqual(exported[0]['source_rows'], [])
        ids = [row['intake']['snapshot_set_id'] for item in exported for row in item['source_rows']]
        self.assertEqual(ids, self.expected_source_ids())
        self.assertEqual(ids, [first, second])
        self.assertEqual(len(set(ids)), len(ids))
        for part, item in zip(anchor['parts'], exported):
            self.assertEqual(len(item['source_rows']), part['manifest']['receipt']['source_count'])
            self.assertEqual(len(item['candles']), part['manifest']['receipt']['candle_count'])
            self.assertIsNone(transport.validate_export_binding(item))
        # Shared tail prices deliberately overlap across source partitions;
        # neither source IDs nor the minute-seven source boundary may overlap.
        self.assertEqual(exported[0]['candles'], exported[1]['candles'])

    def test_open_parent_progress_and_closure_preserve_all_anchored_part_payloads(self):
        ident = self.prepared()
        declaration = self.declaration()
        live_before = self.live_source(ident)
        anchor = self.capture(declaration)
        before = self.assembled_parts(anchor)
        self.assertEqual(before[1]['source_rows'][0], json.loads(live_before['payload_text']))
        before_snapshot = self.snapshot(before[1])
        self.assertIs(before_snapshot['source_coverage_complete'], True)
        frozen_parent = before[1]['source_rows'][0]['btc_parent']
        self.assertIsNone(frozen_parent['end_time_utc'])
        entry = anchor['parts'][1]['manifest']['source_entries'][0]
        self.assertEqual(entry['btc_parent_payload_text'], live_before['parent_payload_text'])

        for minute, close, closed in ((8, 99., False), (9, 102., True)):
            with self.subTest(closed=closed):
                changed = self.advance_parent(minute, close)
                original = next(parent for parent in changed
                    if parent['btc_parent_movement_id'] == frozen_parent['btc_parent_movement_id'])
                self.assertEqual(original['end_time_utc'] is not None, closed)
                live_now = self.live_source(ident)
                self.assertNotEqual(sql_hash(live_now['payload_text']), entry['sha256'])
                manifest = anchor['parts'][1]['manifest']
                with self.assertRaisesRegex(ValueError, 'raw chunk hash/byte length mismatch'):
                    transport.assemble_export(manifest,
                        [{'ordinal': 0, 'payload_text': live_now['payload_text']}], self.candle_chunks(manifest))
                cohort.validate_anchor(declaration, anchor)
                after = self.assembled_parts(anchor)
                self.assertEqual(after, before)
                self.assertEqual(self.snapshot(after[1]), before_snapshot)

    def test_source_cap_plus_one_in_one_part_blocks_the_whole_anchor(self):
        self.prepared()
        self.admit('multipart-source-overflow')
        declaration = self.declaration(part_overrides={1: {'source_row_limit': 1}})
        raw = self.capture(declaration, seal=False)
        empty, overflowing = [part['manifest'] for part in raw['parts']]
        self.assertIs(empty['receipt']['source_overflow'], False)
        self.assertEqual(empty['receipt']['source_count'], 0)
        self.assertIs(overflowing['receipt']['source_overflow'], True)
        self.assertEqual(overflowing['receipt']['source_count_capped_plus_one'], 2)
        self.assertEqual(overflowing['source_entries'], [])
        with self.assertRaises(ValueError):
            cohort.seal_anchor(declaration, raw)
        exact = self.declaration(part_overrides={1: {'source_row_limit': 2}})
        accepted = self.capture(exact)
        cohort.validate_anchor(exact, accepted)
        self.assertEqual(sum(len(part['manifest']['source_entries']) for part in accepted['parts']), 2)

    def test_candle_cap_plus_one_cannot_silently_drop_a_part(self):
        self.prepared()
        declaration = self.declaration(part_overrides={0: {'max_candle_rows': 3}, 1: {'max_candle_rows': 4}})
        raw = self.capture(declaration, seal=False)
        bad, good = [part['manifest'] for part in raw['parts']]
        self.assertIs(bad['receipt']['candle_overflow'], True)
        self.assertEqual(bad['receipt']['candle_count_capped_plus_one'], 4)
        self.assertEqual(bad['candle_pages'], [])
        self.assertIs(good['receipt']['candle_overflow'], False)
        self.assertEqual(good['receipt']['candle_count'], 4)
        with self.assertRaises(ValueError):
            cohort.seal_anchor(declaration, raw)
        exact = self.declaration(part_overrides={0: {'max_candle_rows': 4}, 1: {'max_candle_rows': 4}})
        anchor = self.capture(exact)
        self.assertEqual([len(item['candles']) for item in self.assembled_parts(anchor)], [4, 4])

    def test_mixed_snapshot_or_incomplete_child_set_rejected_from_real_anchor(self):
        self.prepared()
        declaration = self.declaration()
        anchor = self.capture(declaration)
        for mutation in ('missing', 'duplicate', 'mvcc', 'timestamp', 'timezone', 'declaration'):
            with self.subTest(mutation=mutation):
                changed = deepcopy(anchor)
                if mutation == 'missing':
                    changed['parts'].pop()
                elif mutation == 'duplicate':
                    changed['parts'][1] = deepcopy(changed['parts'][0])
                elif mutation == 'mvcc':
                    changed['parts'][1]['manifest']['receipt']['mvcc_snapshot'] += '999'
                elif mutation == 'timestamp':
                    receipt = changed['parts'][1]['manifest']['receipt']
                    receipt['extracted_at_utc'] = (
                        contracts.utc(receipt['extracted_at_utc']) + timedelta(seconds=1)).isoformat()
                elif mutation == 'timezone':
                    changed['parts'][1]['manifest']['receipt']['serializer_timezone'] = 'Etc/UTC'
                else:
                    changed['declaration_sha256'] = 'f' * 64
                # Pass the outer content checksum so each mutation exercises
                # its completeness/provenance rule, not only the envelope hash.
                changed['anchor_sha256'] = contracts.digest({key: value
                    for key, value in changed.items() if key != 'anchor_sha256'})
                with self.assertRaises(ValueError):
                    cohort.validate_anchor(declaration, changed)

    def test_writable_single_statement_cannot_be_sealed_as_readonly(self):
        self.prepared()
        declaration = self.declaration()
        with self.connect() as conn:
            conn.execute("SET LOCAL TIME ZONE 'UTC'")
            raw = conn.execute(cohort.anchor_sql(declaration)).fetchone()['anchor']
        self.assertEqual(raw['extraction_receipt']['read_only'], 'off')
        self.assertTrue(all(part['manifest']['receipt']['read_only'] == 'off' for part in raw['parts']))
        with self.assertRaises(ValueError):
            cohort.seal_anchor(declaration, raw)
        cohort.validate_anchor(declaration, self.capture(declaration))

    def test_later_accepted_source_is_not_added_to_the_frozen_partition_population(self):
        first = self.prepared()
        declaration = self.declaration()
        anchor = self.capture(declaration)
        before = self.assembled_parts(anchor)
        second = self.admit('accepted-after-cohort-anchor')
        self.assertEqual(self.expected_source_ids(), [first, second])
        self.assertEqual(self.assembled_parts(anchor), before)
        frozen_ids = [entry['snapshot_set_id'] for part in anchor['parts']
                      for entry in part['manifest']['source_entries']]
        self.assertEqual(frozen_ids, [first])
        fresh = self.capture(declaration)
        fresh_ids = [entry['snapshot_set_id'] for part in fresh['parts']
                     for entry in part['manifest']['source_entries']]
        self.assertEqual(fresh_ids, [first, second])
        cohort.validate_anchor(declaration, anchor)
        cohort.validate_anchor(declaration, fresh)


if __name__ == '__main__':
    unittest.main()
