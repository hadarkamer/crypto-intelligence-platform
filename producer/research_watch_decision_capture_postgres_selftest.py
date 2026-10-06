"""Real PostgreSQL projection of immutable operational decision captures.

Only an explicitly guarded disposable local/CI database is used. These tests
exercise the existing archive and intake, without formula workers or providers.
"""
from copy import deepcopy
from datetime import timedelta
import os
from pathlib import Path
import unittest
from unittest.mock import patch

import research_max_pain_archive as archive
from research_max_pain_archive_selftest import BASE, _raw_rows, _enriched_rows
import research_watch_decision_capture as decisions
import research_watch_decision_capture_selftest as decision_tests
import research_watch_scan_intake as intake
import research_watch_scan_intake_selftest as intake_tests
import research_watch_score_capture as scores
from research_watch_score_capture_selftest import bundle as score_bundle


_MISSING = object()
MIGRATION = Path(__file__).parent / 'migrations' / '055_watch_scan_decision_captures.sql'


def decision_fixture(cycle_id):
    arguments = decision_tests.inputs(cycle_id=cycle_id)
    return arguments['score_bundle'], decisions.build_bundle(**arguments)


def archive_payload(score_block, cycle_id, decision_block=_MISSING):
    metadata = {'operational_scores': deepcopy(score_block)}
    if decision_block is not _MISSING:
        metadata['operational_decisions'] = deepcopy(decision_block)
    # Both siblings enter the actual archive builder before its parent hash.
    return archive.build_snapshot_payload(
        cycle_id=cycle_id, cycle_time_utc=BASE,
        collection_started_at_utc=BASE,
        collection_completed_at_utc=BASE + timedelta(minutes=6),
        source='WATCH_SHARED', collector_version='selftest-v1',
        snapshot={'ok': True, 'rows': _raw_rows(scores.SYMBOLS),
                  'missing_timeframes': [], 'duplicate_pairs': []},
        enriched_rows=_enriched_rows(scores.SYMBOLS),
        live_result={'skipped_symbols': []}, capture_metadata=metadata,
    )


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'), 'Explicit local/CI PostgreSQL required')
class PostgreSQLDecisionCaptureTests(unittest.TestCase):
    connect = intake_tests.PostgreSQLIntakeTests.connect
    drop_database = intake_tests.PostgreSQLIntakeTests.drop_database
    consume = intake_tests.PostgreSQLIntakeTests.consume

    def setUp(self):
        intake_tests.PostgreSQLIntakeTests.setUp(self)
        with self.connect() as conn:
            conn.execute(MIGRATION.read_text(), prepare=False)

    def persist_payload(self, payload):
        with patch.dict(os.environ, {'MAX_PAIN_ARCHIVE_ENABLED': '1'}):
            result = archive.persist_snapshot_payload(payload, database_url=self.dsn)
        self.assertTrue(result['persisted'])
        return result

    def consume_all(self, expected):
        accepted = rejected = 0
        for _ in range(expected + 2):
            result = self.consume()
            accepted += result['accepted']
            rejected += result['rejected']
            if accepted + rejected >= expected:
                break
        self.assertEqual((accepted, rejected), (expected, 0))

    def base_rows(self, conn):
        return (
            conn.execute('SELECT * FROM research_watch_scan_observations '
                         'ORDER BY snapshot_set_id,symbol').fetchall(),
            conn.execute('SELECT * FROM research_watch_scan_score_slots '
                         'ORDER BY snapshot_set_id,symbol,timeframe,source_side').fetchall(),
        )

    def projection(self, conn, snapshot_id=None):
        if snapshot_id is None:
            return conn.execute('SELECT * FROM research_watch_scan_decision_captures '
                                'ORDER BY snapshot_set_id,symbol').fetchall()
        return conn.execute('SELECT * FROM research_watch_scan_decision_captures '
                            'WHERE snapshot_set_id=%s ORDER BY symbol', (snapshot_id,)).fetchall()

    def assert_evidence_flags(self, rows):
        self.assertTrue(rows)
        self.assertTrue(all(row['consumer_validation_required'] is True
                            and row['is_delivered_alert'] is False
                            and row['qualifies_as_prospective_formula_evidence'] is False
                            for row in rows))

    def test_old_absent_capture_survives_idempotent_view_install_without_changing_base(self):
        block, *_ = score_bundle(cycle_id='old-missing')
        self.persist_payload(archive_payload(block, 'old-missing'))
        self.consume_all(1)
        with self.connect() as conn:
            before = self.base_rows(conn)
            relations = conn.execute("SELECT relname,relkind FROM pg_class "
                "WHERE relnamespace='public'::regnamespace AND relkind IN ('r','p','S') "
                'ORDER BY relname').fetchall()
            old_definitions = conn.execute("SELECT viewname,definition FROM pg_views "
                "WHERE schemaname='public' AND viewname IN "
                "('research_watch_scan_observations','research_watch_scan_score_slots') "
                'ORDER BY viewname').fetchall()
            conn.execute(MIGRATION.read_text(), prepare=False)
            conn.execute(MIGRATION.read_text(), prepare=False)
            self.assertEqual(self.base_rows(conn), before)
            self.assertEqual(conn.execute("SELECT relname,relkind FROM pg_class "
                "WHERE relnamespace='public'::regnamespace AND relkind IN ('r','p','S') "
                'ORDER BY relname').fetchall(), relations)
            self.assertEqual(conn.execute("SELECT viewname,definition FROM pg_views "
                "WHERE schemaname='public' AND viewname IN "
                "('research_watch_scan_observations','research_watch_scan_score_slots') "
                'ORDER BY viewname').fetchall(), old_definitions)
            rows = self.projection(conn)
            self.assertEqual((len(before[0]), len(before[1]), len(rows)), (8, 112, 8))
            self.assertEqual({row['symbol'] for row in rows}, set(scores.SYMBOLS))
            for row in rows:
                self.assertEqual((row['capture_status'], row['binding_status']),
                                 ('NOT_CAPTURED', 'NOT_CAPTURED'))
                self.assertIsNone(row['decision_bundle'])
                self.assertIsNone(row['coin_context'])
                self.assertIsNone(row['decision_payload_sha256'])
            self.assert_evidence_flags(rows)
            self.assertIsNone(conn.execute("SELECT to_regclass('research_events') AS relation").fetchone()['relation'])

    def test_real_builder_survives_jsonb_and_preserves_all_eight_source_identities(self):
        cycle_id = 'real-decisions'
        score_block, decision_block = decision_fixture(cycle_id)
        payload = archive_payload(score_block, cycle_id, decision_block)
        parent_without_hash = {key: value for key, value in payload['set'].items()
                               if key != 'payload_sha256'}
        self.assertEqual(payload['set']['payload_sha256'], archive._sha256(
            {'set': parent_without_hash, 'symbols': payload['symbols'], 'rows': payload['rows']}))
        snapshot_id = self.persist_payload(payload)['snapshot_set_id']
        self.consume_all(1)
        with self.connect() as conn:
            source = conn.execute('SELECT * FROM research_max_pain_snapshot_sets '
                                  'WHERE snapshot_set_id=%s', (snapshot_id,)).fetchone()
            receipt = conn.execute('SELECT * FROM research_watch_scan_intakes '
                                   'WHERE snapshot_set_id=%s', (snapshot_id,)).fetchone()
            rows = self.projection(conn, snapshot_id)
            self.assertEqual(len(rows), 8)
            self.assertEqual({row['symbol'] for row in rows}, set(scores.SYMBOLS))
            reloaded_scores = source['source_metadata']['capture_metadata']['operational_scores']
            for row in rows:
                self.assertEqual(row['binding_status'], 'SOURCE_BOUND')
                self.assertEqual(row['capture_status'], decision_block['status'])
                self.assertEqual(row['capture_version'], decisions.VERSION)
                self.assertEqual(row['decision_bundle'], decision_block)
                self.assertEqual(row['coin_context'], decision_block['coins'][row['symbol']])
                self.assertEqual(row['decision_payload_sha256'], decision_block['payload_sha256'])
                self.assertEqual(row['decision_computed_at_utc'], decision_block['computed_at_utc'])
                self.assertIsInstance(row['decision_computed_at_utc'], str)
                self.assertEqual(row['bundle_sha256'], reloaded_scores['payload_sha256'])
                self.assertEqual(row['parent_payload_sha256'], source['payload_sha256'])
                self.assertEqual(row['watch_scan_id'], cycle_id)
                for key in ('consumer_version', 'snapshot_set_id', 'snapshot_key',
                            'parent_payload_sha256', 'bundle_sha256', 'watch_scan_id',
                            'observed_at_utc', 'source_available_at_utc', 'source_created_at_utc',
                            'usable_from_utc', 'capture_phase'):
                    self.assertEqual(row[key], receipt[key])
                self.assertEqual(decisions.validate_bundle(row['decision_bundle'], reloaded_scores,
                    cycle_id=cycle_id, available_at_utc=source['available_at_utc']), row['decision_bundle'])
                self.assertEqual(scores.digest({key: value for key, value in row['decision_bundle'].items()
                    if key != 'payload_sha256'}), row['decision_payload_sha256'])
            self.assert_evidence_flags(rows)
            self.assertEqual(tuple(map(len, self.base_rows(conn))), (8, 112))

    def test_malformed_sibling_blocks_never_reject_original_scores_or_break_projection(self):
        cases = (
            ('null', lambda block: None, 'NOT_CAPTURED'),
            ('array', lambda block: ['unexpected'], 'INVALID_BINDING'),
            ('scalar', lambda block: 17, 'INVALID_BINDING'),
            ('version', lambda block: {**block, 'version': 'future-v999'}, 'UNSUPPORTED_VERSION'),
            ('hash-version', lambda block: {**block, 'hash_version': 'future-hash'}, 'UNSUPPORTED_VERSION'),
            ('population', lambda block: {**block, 'population': 'other-population'}, 'UNSUPPORTED_VERSION'),
            ('cycle', lambda block: {**block, 'cycle_id': 'different-watch'}, 'INVALID_BINDING'),
            ('source-hash', lambda block: {**block, 'source_score_sha256': '0' * 64}, 'INVALID_BINDING'),
            ('universe-hash', lambda block: {**block, 'input_universe_sha256': '0' * 64}, 'INVALID_BINDING'),
            ('payload-hash', lambda block: {**block, 'payload_sha256': 'malformed'}, 'INVALID_BINDING'),
            ('coins', lambda block: {**block, 'coins': []}, 'INVALID_BINDING'),
            ('failed', lambda block: {'version': decisions.VERSION, 'cycle_id': block['cycle_id'],
                                      'status': 'FAILED', 'reason': 'fixture failure'}, 'CAPTURE_FAILED'),
        )
        expected = {}
        for label, mutate, status in cases:
            cycle_id = 'invalid-' + label
            score_block, valid = decision_fixture(cycle_id)
            invalid = mutate(deepcopy(valid))
            snapshot_id = self.persist_payload(archive_payload(score_block, cycle_id, invalid))['snapshot_set_id']
            expected[snapshot_id] = (status, invalid, score_block)
        self.consume_all(len(cases))
        with self.connect() as conn:
            original = self.base_rows(conn)
            self.assertEqual(tuple(map(len, original)), (8 * len(cases), 112 * len(cases)))
            rows = self.projection(conn)
            self.assertEqual(len(rows), 8 * len(cases))
            for row in rows:
                status, invalid, score_block = expected[row['snapshot_set_id']]
                self.assertEqual(row['binding_status'], status, row['watch_scan_id'])
                self.assertEqual(row['decision_bundle'], invalid)
                self.assertEqual(row['bundle_sha256'], score_block['payload_sha256'])
            self.assert_evidence_flags(rows)
            self.assertEqual(self.base_rows(conn), original)
            for observation in original[0]:
                score_block = expected[observation['snapshot_set_id']][2]
                coin = score_block['coins'][observation['symbol']]
                self.assertEqual(observation['maxpain_slots'], coin['maxpain'])
                self.assertEqual(observation['models'], coin['models'])
                self.assertEqual(observation['sources'], coin['sources'])

    def test_source_binding_keeps_corrupt_and_future_time_as_text_until_consumer_validation(self):
        expected = {}
        for label, computed_at in (
            ('malformed', 'not-a-timestamp'),
            ('future', (BASE + timedelta(minutes=7)).isoformat()),
        ):
            cycle_id = 'invalid-time-' + label
            score_block, decision_block = decision_fixture(cycle_id)
            decision_block['computed_at_utc'] = computed_at
            decision_block['payload_sha256'] = scores.digest({key: value for key, value
                in decision_block.items() if key != 'payload_sha256'})
            snapshot_id = self.persist_payload(archive_payload(score_block, cycle_id, decision_block))['snapshot_set_id']
            expected[snapshot_id] = (computed_at, score_block)
        cycle_id = 'stale-canonical-hash'
        score_block, decision_block = decision_fixture(cycle_id)
        decision_block['payload_sha256'] = '0' * 64
        snapshot_id = self.persist_payload(archive_payload(score_block, cycle_id, decision_block))['snapshot_set_id']
        expected[snapshot_id] = (decision_block['computed_at_utc'], score_block)
        self.consume_all(3)
        with self.connect() as conn:
            rows = self.projection(conn)
            self.assertEqual(len(rows), 24)
            for row in rows:
                computed_at, score_block = expected[row['snapshot_set_id']]
                self.assertEqual(row['binding_status'], 'SOURCE_BOUND')
                self.assertEqual(row['decision_computed_at_utc'], computed_at)
                with self.assertRaises(ValueError):
                    decisions.validate_bundle(row['decision_bundle'], score_block,
                        cycle_id=row['watch_scan_id'], available_at_utc=row['source_available_at_utc'])
            self.assert_evidence_flags(rows)
            self.assertEqual(tuple(map(len, self.base_rows(conn))), (24, 336))

    def test_parent_hash_commits_decisions_and_replay_cannot_replace_immutable_evidence(self):
        cycle_id = 'immutable-decisions'
        score_block, decision_block = decision_fixture(cycle_id)
        payload = archive_payload(score_block, cycle_id, decision_block)
        snapshot_id = self.persist_payload(payload)['snapshot_set_id']
        self.consume_all(1)
        with self.connect() as conn:
            original_projection = self.projection(conn)
            original_base = self.base_rows(conn)
        replay = self.persist_payload(payload)
        self.assertTrue(replay['idempotent_existing'])
        self.assertEqual(replay['snapshot_set_id'], snapshot_id)
        self.assertEqual(self.consume()['processed'], 0)
        changed = deepcopy(decision_block)
        changed['computed_at_utc'] = (BASE + timedelta(minutes=5, seconds=50)).isoformat()
        changed['payload_sha256'] = scores.digest({key: value for key, value in changed.items()
            if key != 'payload_sha256'})
        alternate = archive_payload(score_block, cycle_id, changed)
        self.assertEqual(alternate['set']['snapshot_key'], payload['set']['snapshot_key'])
        self.assertNotEqual(alternate['set']['payload_sha256'], payload['set']['payload_sha256'])
        with self.assertRaisesRegex(RuntimeError, 'different payload hash'):
            self.persist_payload(alternate)
        with self.connect() as conn:
            with self.assertRaises(self.psycopg.Error):
                with conn.transaction():
                    conn.execute("UPDATE research_max_pain_snapshot_sets SET source_metadata="
                        "jsonb_set(source_metadata,'{capture_metadata,operational_decisions}', 'null'::jsonb) "
                        'WHERE snapshot_set_id=%s', (snapshot_id,))
            self.assertEqual(self.projection(conn), original_projection)
            self.assertEqual(self.base_rows(conn), original_base)

    def test_real_archive_error_and_intake_crash_rollback_whole_capture_then_retry(self):
        cycle_id = 'rollback-decisions'
        score_block, decision_block = decision_fixture(cycle_id)
        payload = archive_payload(score_block, cycle_id, decision_block)
        with self.connect() as conn:
            conn.execute("CREATE FUNCTION fixture_fail_decision_row() RETURNS trigger LANGUAGE plpgsql "
                         "AS $$ BEGIN RAISE EXCEPTION 'fixture child insert failure'; END $$")
            conn.execute('CREATE TRIGGER fixture_fail_decision_row BEFORE INSERT '
                         'ON research_max_pain_snapshot_rows FOR EACH ROW '
                         'EXECUTE FUNCTION fixture_fail_decision_row()')
        with self.assertRaisesRegex(self.psycopg.Error, 'fixture child insert failure'):
            self.persist_payload(payload)
        with self.connect() as conn:
            for table in ('research_max_pain_snapshot_sets', 'research_max_pain_snapshot_symbols',
                          'research_max_pain_snapshot_rows', 'research_watch_scan_intakes'):
                self.assertEqual(conn.execute('SELECT COUNT(*) AS n FROM ' + table).fetchone()['n'], 0)
            self.assertEqual(self.projection(conn), [])
            conn.execute('DROP TRIGGER fixture_fail_decision_row ON research_max_pain_snapshot_rows')
            conn.execute('DROP FUNCTION fixture_fail_decision_row()')
            conn.execute('INSERT INTO research_watch_scan_intake_state(consumer_version) VALUES (%s)',
                         (intake.VERSION,))
        self.persist_payload(payload)
        with self.connect() as conn:
            with self.assertRaisesRegex(RuntimeError, 'after receipt'):
                with conn.transaction():
                    self.assertEqual(intake.consume_page(conn)['accepted'], 1)
                    self.assertEqual(len(self.projection(conn)), 8)
                    raise RuntimeError('fixture crash after receipt')
        with self.connect() as conn:
            self.assertEqual(self.projection(conn), [])
            state = conn.execute('SELECT * FROM research_watch_scan_intake_state').fetchone()
            self.assertEqual((state['scan_cursor'], state['scan_high_water'], state['completed_laps']), (0, 0, 0))
            self.assertEqual(conn.execute('SELECT COUNT(*) AS n FROM research_watch_scan_intakes').fetchone()['n'], 0)
        self.consume_all(1)
        with self.connect() as conn:
            rows = self.projection(conn)
            self.assertEqual(len(rows), 8)
            self.assertTrue(all(row['decision_bundle'] == decision_block
                                and row['binding_status'] == 'SOURCE_BOUND' for row in rows))
            self.assertEqual(tuple(map(len, self.base_rows(conn))), (8, 112))


if __name__ == '__main__':
    unittest.main()
