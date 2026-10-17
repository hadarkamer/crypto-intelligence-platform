"""Real PostgreSQL regressions for future anchors with frozen BTC parents.

Each test owns a disposable database behind an explicitly local/CI TEST_DSN.
Watch rows enter through capture/intake, parents through the causal policy, and
prices through the archive writer. Only fixture methods are reused; no older
test suite is inherited. No production database or network price reader runs.
"""
from copy import deepcopy
from datetime import timedelta
import hashlib
import json
import os
import unittest

import research_btc_parent_movement as btc_policy
import research_no_horizon_export as exporter
import research_no_horizon_manifest as transport
import research_no_horizon_manifest_sql as sql_builder
import research_no_horizon_source_postgres_selftest as fixtures
import research_watch_scan_intake as intake
from research_watch_score_capture_selftest import BASE


TEST_DSN = os.environ.get('TEST_DATABASE_URL') or os.environ.get('RESEARCH_TEST_POSTGRES_URL')


def proof(text):
    raw = text.encode('utf-8')
    return {'sha256': hashlib.sha256(raw).hexdigest(), 'byte_length': len(raw)}


@unittest.skipUnless(TEST_DSN, 'Explicit local/CI PostgreSQL test database required')
class NoHorizonManifestSqlPostgresTests(unittest.TestCase):
    # Composition of genuine fixture helpers, not inheritance of old tests.
    setUp = fixtures.NoHorizonSourcePostgresTests.setUp
    connect = fixtures.NoHorizonSourcePostgresTests.connect
    drop_database = fixtures.NoHorizonSourcePostgresTests.drop_database
    persist_on = fixtures.NoHorizonSourcePostgresTests.persist_on
    persist = fixtures.NoHorizonSourcePostgresTests.persist
    consume_intake = fixtures.NoHorizonSourcePostgresTests.consume_intake
    captured_bundle = fixtures.NoHorizonSourcePostgresTests.captured_bundle
    admit = fixtures.NoHorizonSourcePostgresTests.admit
    seed_causal_parent = fixtures.NoHorizonSourcePostgresTests.seed_causal_parent
    seed_prices = fixtures.NoHorizonSourcePostgresTests.seed_prices
    prepared = fixtures.NoHorizonSourcePostgresTests.prepared
    snapshot = fixtures.NoHorizonSourcePostgresTests.snapshot

    def plan(self, **overrides):
        result = dict(source_start_utc=self.start.isoformat(),
            source_end_utc=self.end.isoformat(), cutoff_utc=self.cutoff.isoformat(),
            symbol='XRP', source_row_limit=8, max_candle_rows=16, page_size=2)
        result.update(overrides)
        return result

    def read_only(self, query, params=None):
        with self.connect() as conn:
            with conn.transaction():
                conn.execute('SET TRANSACTION READ ONLY')
                conn.execute("SET LOCAL TIME ZONE 'UTC'")
                return conn.execute(query, params).fetchone()

    def anchor(self, *, validate=True, **overrides):
        query = sql_builder.manifest_sql(self.plan(**overrides))
        anchor = self.read_only(query)['manifest']
        # SQL cannot embed its own hash. Attach the exact query hash first,
        # then validate the whole anchor instead of trusting selected fields.
        anchor['receipt']['query_sha256'] = proof(query)['sha256']
        if validate:
            transport._manifest(anchor)
            self.assertEqual(anchor['receipt']['parent_snapshot_mode'],
                             sql_builder.PARENT_SNAPSHOT_MODE)
        return anchor

    def source_chunks(self, anchor):
        result = []
        for start in range(0, len(anchor['source_entries']), sql_builder.SOURCE_PAGE_SIZE):
            ordinals = list(range(start, min(start + sql_builder.SOURCE_PAGE_SIZE,
                                            len(anchor['source_entries']))))
            chunk = self.read_only(sql_builder.source_chunk_sql(anchor, ordinals))['chunk']
            self.assertEqual(chunk['requested_count'], len(ordinals))
            self.assertEqual(chunk['returned_count'], len(chunk['entries']))
            self.assertIs(chunk['overflow'], False)
            self.assertEqual(chunk['receipt']['read_only'], 'on')
            for entry in chunk['entries']:
                self.assertEqual(proof(entry['payload_text']),
                    {key: entry[key] for key in ('sha256', 'byte_length')})
                result.append({'ordinal': entry['ordinal'], 'payload_text': entry['payload_text']})
        return result

    def candle_chunks(self, anchor):
        result = []
        for page in anchor['candle_pages']:
            chunk = self.read_only(sql_builder.candle_chunk_sql(anchor, page['ordinal']))['chunk']
            self.assertEqual(chunk['ordinal'], page['ordinal'])
            self.assertIs(chunk['overflow'], False)
            self.assertEqual(chunk['receipt']['read_only'], 'on')
            self.assertEqual(proof(chunk['payload_text']),
                             {key: chunk[key] for key in ('sha256', 'byte_length')})
            result.append({'ordinal': chunk['ordinal'], 'payload_text': chunk['payload_text']})
        return result

    def live_source(self, ident):
        # An independent reference query retains the original live parent join.
        # PostgreSQL, not Python reserialization, supplies the expected raw text.
        query = '''SELECT to_jsonb(r)::text AS payload_text,
            (to_jsonb(r)->'btc_parent')::text AS parent_payload_text
            FROM (''' + exporter.ROW_SQL + ') r'
        return self.read_only(query, (btc_policy.POLICY_VERSION, intake.VERSION, [ident]))

    def store_parents(self, conn, parents):
        from psycopg.types.json import Jsonb
        names = ('btc_parent_movement_id', 'episode_policy_version', 'start_time_utc',
            'end_time_utc', 'confirmed_at_utc', 'direction', 'evidence_eligible',
            'boundary_reason', 'observed_through_utc', 'price_source', 'state_json')
        for parent in parents:
            values = [Jsonb(parent[key]) if key == 'state_json' else parent[key] for key in names]
            conn.execute('INSERT INTO research_btc_parent_movements (' + ','.join(names) +
                ') VALUES (' + ','.join(['%s'] * len(names)) +
                ') ON CONFLICT (btc_parent_movement_id) DO UPDATE SET ' +
                ','.join(key + '=EXCLUDED.' + key for key in names[1:]) +
                ',updated_at_utc=clock_timestamp()', values)

    def advance_parent(self, minute, close):
        opened = BASE + timedelta(minutes=minute)
        bar = dict(open_time_utc=opened,
            close_time_utc=opened + timedelta(minutes=1, milliseconds=-1),
            open=close, high=close + .1, low=close - .1, close=close)
        with self.connect() as conn:
            previous = conn.execute('''SELECT * FROM research_btc_parent_movements
                WHERE end_time_utc IS NULL''').fetchone()
            parents = btc_policy.advance_parents([bar], previous=previous,
                                                 as_of_utc=bar['close_time_utc'])
            conn.execute('''INSERT INTO research_btc_price_bars
                (open_time_utc,close_time_utc,open,high,low,close)
                VALUES (%s,%s,%s,%s,%s,%s)''',
                tuple(bar[key] for key in ('open_time_utc', 'close_time_utc',
                                          'open', 'high', 'low', 'close')))
            self.store_parents(conn, parents)
        return parents

    def test_open_parent_progress_and_closure_keep_full_source_bytes_and_snapshot(self):
        ident = self.prepared()
        expected = self.live_source(ident)
        parent = json.loads(expected['parent_payload_text'])
        self.assertIs(parent['evidence_eligible'], True)
        self.assertIsNone(parent['end_time_utc'])
        anchor = self.anchor()
        self.assertEqual((anchor['receipt']['source_count'], anchor['receipt']['candle_count']), (1, 4))
        entry = anchor['source_entries'][0]
        self.assertEqual(entry['btc_parent_payload_text'], expected['parent_payload_text'])
        self.assertEqual({key: entry[key] for key in ('sha256', 'byte_length')},
                         proof(expected['payload_text']))
        before_sources, before_candles = self.source_chunks(anchor), self.candle_chunks(anchor)
        self.assertEqual(before_sources[0]['payload_text'], expected['payload_text'])
        before_export = transport.assemble_export(anchor, before_sources, before_candles)
        before_snapshot = self.snapshot(before_export)
        self.assertIs(before_snapshot['source_coverage_complete'], True)
        self.assertEqual(len(before_snapshot['opportunities']), 1)
        original_parent_id = parent['btc_parent_movement_id']

        for minute, close, should_close in ((8, 99., False), (9, 102., True)):
            with self.subTest(parent_state='CLOSED' if should_close else 'ADVANCED_OPEN'):
                changed = self.advance_parent(minute, close)
                original = next(row for row in changed
                                if row['btc_parent_movement_id'] == original_parent_id)
                self.assertEqual(original['end_time_utc'] is not None, should_close)
                self.assertGreater(btc_policy.utc(original['observed_through_utc']),
                                   btc_policy.utc(parent['observed_through_utc']))
                self.assertEqual(original['state_json']['last_close'], 99.)
                live = self.live_source(ident)
                self.assertNotEqual(live['payload_text'], expected['payload_text'])
                self.assertNotEqual(proof(live['payload_text'])['sha256'], entry['sha256'])
                with self.assertRaisesRegex(ValueError, 'raw chunk hash/byte length mismatch'):
                    transport.assemble_export(anchor,
                        [{'ordinal': 0, 'payload_text': live['payload_text']}], before_candles)
                pinned_sources, pinned_candles = self.source_chunks(anchor), self.candle_chunks(anchor)
                self.assertEqual(pinned_sources, before_sources)
                self.assertEqual(pinned_candles, before_candles)
                export = transport.assemble_export(anchor, pinned_sources, pinned_candles)
                self.assertEqual(export, before_export)
                self.assertIsNone(transport.validate_export_binding(export))
                self.assertEqual(self.snapshot(export), before_snapshot)

    def test_absent_parent_is_pinned_null_even_if_parent_arrives_later(self):
        ident = self.prepared()
        with self.connect() as conn:
            parents = conn.execute('''SELECT * FROM research_btc_parent_movements
                ORDER BY start_time_utc''').fetchall()
            conn.execute('DELETE FROM research_btc_parent_movements')
        # Prior bars already exist: only parent availability changes after the
        # anchor, so a frozen null cannot be confused with prior-bar drift.
        anchor = self.anchor()
        self.assertEqual(anchor['source_entries'][0]['btc_parent_payload_text'], 'null')
        before_sources, before_candles = self.source_chunks(anchor), self.candle_chunks(anchor)
        export = transport.assemble_export(anchor, before_sources, before_candles)
        self.assertIsNone(export['source_rows'][0]['btc_parent'])
        self.assertIsNotNone(export['source_rows'][0]['btc_prior_bar'])
        snapshot = self.snapshot(export)
        self.assertIs(snapshot['source_coverage_complete'], False)
        self.assertIn('UNVERIFIED_MATCHED_BTC_PARENT', snapshot['source_receipt']['source_blockers'])
        with self.connect() as conn:
            self.store_parents(conn, parents)
        self.assertIsNotNone(json.loads(self.live_source(ident)['payload_text'])['btc_parent'])
        after_sources, after_candles = self.source_chunks(anchor), self.candle_chunks(anchor)
        self.assertEqual(after_sources, before_sources)
        rebuilt = transport.assemble_export(anchor, after_sources, after_candles)
        self.assertEqual(rebuilt, export)
        self.assertEqual(self.snapshot(rebuilt), snapshot)

    def test_changed_parent_pin_does_not_bypass_full_source_leaf_hash(self):
        self.prepared()
        anchor = self.anchor()
        changed = deepcopy(anchor)
        changed['source_entries'][0]['btc_parent_payload_text'] = 'null'
        altered_sources = self.source_chunks(changed)
        self.assertIsNone(json.loads(altered_sources[0]['payload_text'])['btc_parent'])
        with self.assertRaisesRegex(ValueError, 'raw chunk hash/byte length mismatch'):
            transport.assemble_export(changed, altered_sources, self.candle_chunks(anchor))

    def test_score_and_candle_transport_mutations_fail_original_raw_proofs(self):
        self.prepared()
        anchor = self.anchor()
        sources, candles = self.source_chunks(anchor), self.candle_chunks(anchor)
        transport.assemble_export(anchor, sources, candles)
        mutations = (
            ('source', ['scores', 'coins', 'XRP', 'models', 'futures_flow', 'score'], '-74'),
            ('candle', ['0', 'high'], '100.3'),
        )
        for kind, path, replacement in mutations:
            with self.subTest(kind=kind):
                changed_sources, changed_candles = deepcopy(sources), deepcopy(candles)
                chunks = changed_sources if kind == 'source' else changed_candles
                text = chunks[0]['payload_text']
                changed = self.read_only('''SELECT jsonb_set(%s::jsonb,%s::text[],
                    %s::jsonb,false)::text AS payload_text''', (text, path, replacement))['payload_text']
                self.assertNotEqual(changed, text)
                chunks[0]['payload_text'] = changed
                with self.assertRaisesRegex(ValueError, 'raw chunk hash/byte length mismatch'):
                    transport.assemble_export(anchor, changed_sources, changed_candles)

    def test_source_deleted_after_anchor_cannot_become_a_complete_empty_export(self):
        ident = self.prepared()
        anchor = self.anchor()
        sources, candles = self.source_chunks(anchor), self.candle_chunks(anchor)
        transport.assemble_export(anchor, sources, candles)
        with self.connect() as conn:
            deleted = conn.execute('''DELETE FROM research_watch_scan_intakes
                WHERE consumer_version=%s AND snapshot_set_id=%s''', (intake.VERSION, ident))
            self.assertEqual(deleted.rowcount, 1)
        missing_sources = self.source_chunks(anchor)
        self.assertEqual(missing_sources, [])
        self.assertEqual(anchor['receipt']['source_count'], 1)
        with self.assertRaisesRegex(ValueError, 'complete exact chunk set required'):
            transport.assemble_export(anchor, missing_sources, candles)

    def test_source_and_candle_caps_probe_plus_one_and_reject_truncation(self):
        self.prepared()
        self.admit('nh-manifest-second')
        for bounds, kind in ((dict(source_row_limit=1), 'source'),
                             (dict(max_candle_rows=3), 'candle')):
            with self.subTest(kind=kind):
                anchor = self.anchor(validate=False, **bounds)
                receipt = anchor['receipt']
                self.assertIs(receipt[kind + '_overflow'], True)
                self.assertEqual(receipt[kind + '_count_capped_plus_one'],
                                 receipt[kind + '_cap'] + 1)
                self.assertEqual(anchor['source_entries' if kind == 'source' else 'candle_pages'], [])
                with self.assertRaises(ValueError):
                    transport._manifest(anchor)
                with self.assertRaises(ValueError):
                    sql_builder.source_chunk_sql(anchor, [0])
                with self.assertRaises(ValueError):
                    transport.assemble_export(anchor, [], [])
        exact = self.anchor(source_row_limit=2, max_candle_rows=4)
        sources, candles = self.source_chunks(exact), self.candle_chunks(exact)
        self.assertEqual(len(candles), 2)
        export = transport.assemble_export(exact, sources, candles)
        self.assertEqual((len(export['source_rows']), len(export['candles'])), (2, 4))
        with self.assertRaisesRegex(ValueError, 'complete exact chunk set required'):
            transport.assemble_export(exact, sources, candles[:-1])

    def test_anchor_requires_readonly_receipt_and_attached_query_hash(self):
        self.prepared()
        query = sql_builder.manifest_sql(self.plan())
        with self.connect() as conn:
            with conn.transaction():
                conn.execute('SET TRANSACTION READ ONLY')
                conn.execute("SET LOCAL TIME ZONE 'UTC'")
                anchor = conn.execute(query).fetchone()['manifest']
                self.assertEqual(anchor['receipt']['read_only'], 'on')
                with self.assertRaises(self.psycopg.errors.ReadOnlySqlTransaction):
                    with conn.transaction():
                        conn.execute('UPDATE research_watch_scan_intakes SET scored_slots=scored_slots')
            # Transaction-local protection must not alter the caller's next
            # transaction; a writable read cannot produce a valid anchor.
            writable = conn.execute(query).fetchone()['manifest']
            self.assertEqual(writable['receipt']['read_only'], 'off')
        with self.assertRaises(ValueError):
            sql_builder.source_chunk_sql(anchor, [0])
        anchor['receipt']['query_sha256'] = proof(query)['sha256']
        transport._manifest(anchor)
        sql_builder.source_chunk_sql(anchor, [0])
        writable['receipt']['query_sha256'] = proof(query)['sha256']
        with self.assertRaisesRegex(ValueError, 'writable anchor'):
            sql_builder.source_chunk_sql(writable, [0])


if __name__ == '__main__':
    unittest.main()
