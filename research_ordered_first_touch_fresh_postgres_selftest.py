"""Real PostgreSQL gate for source-time fairness and durable outcome claims.

Requires explicit TEST_DATABASE_URL pointing only to a local/CI test database.
Every test uses real migrations in a unique temporary schema. No production
connection, transport or Telegram send is used. CI supplies PostgreSQL 18.
"""
from __future__ import annotations

from datetime import timedelta
import os
from pathlib import Path
import unittest
from uuid import uuid4

import research_ordered_first_touch as ordered
import research_ordered_first_touch_worker_selftest as fixtures
import research_outcome_worker as worker

ROOT = Path(__file__).resolve().parent
MIGRATIONS = (
    '001_research_archive_v1.sql', '020_ordered_first_touch_v7.sql',
    '022_ordered_formula_research.sql',
    '025_ordered_first_touch_sync_claim_queue.sql',
    '026_research_sheet_fresh_delivery.sql',
    '039_ordered_first_touch_fresh_delivery.sql',
)


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'), 'TEST_DATABASE_URL required for PostgreSQL integration')
class PostgreSQLOutcomeFreshnessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict
        from psycopg.rows import dict_row
        cls.psycopg, cls.sql, cls.dict_row = psycopg, sql, staticmethod(dict_row)
        cls.dsn = os.environ['TEST_DATABASE_URL']
        info = conninfo_to_dict(cls.dsn)
        if (info.get('host') not in {'localhost', '127.0.0.1', '::1', 'postgres'}
                or not (info.get('dbname', '').startswith('test_')
                        or info.get('dbname', '').endswith('_test'))):
            raise ValueError('Only explicit local/CI test databases are allowed')
        cls.worker = worker.ResearchOutcomeWorker

    def connect(self):
        conn = self.psycopg.connect(self.dsn, row_factory=self.dict_row,
            connect_timeout=5, options='-c statement_timeout=10000 -c lock_timeout=1000')
        conn.execute(self.sql.SQL('SET search_path TO {}').format(self.sql.Identifier(self.schema)))
        conn.commit()
        return conn

    def setUp(self):
        self.schema = 'test_outcome_fresh_' + uuid4().hex
        self.conn = self.psycopg.connect(self.dsn, row_factory=self.dict_row, connect_timeout=5)
        self.conn.execute(self.sql.SQL('CREATE SCHEMA {}').format(self.sql.Identifier(self.schema)))
        self.conn.execute(self.sql.SQL('SET search_path TO {}').format(self.sql.Identifier(self.schema)))
        for name in MIGRATIONS:
            self.conn.execute((ROOT/'migrations'/name).read_text(), prepare=False)
        self.conn.commit()
        self.now = self.conn.execute("SELECT date_trunc('minute',NOW())-interval '2 minutes' AS now").fetchone()['now']
        self.conn.commit()

    def tearDown(self):
        self.conn.close()
        with self.psycopg.connect(self.dsn, connect_timeout=5, autocommit=True) as cleanup:
            cleanup.execute(self.sql.SQL('DROP SCHEMA {} CASCADE').format(self.sql.Identifier(self.schema)))

    def write_outcome(self, event_id, start, *, candles=1):
        event = {**fixtures._event(start), 'event_id': event_id,
                 'event_fingerprint': f'{event_id:064x}'}
        self.conn.execute('''INSERT INTO research_events(event_id,schema_version,event_kind,event_type,
            alert_time_utc,symbol,direction,setup_key,event_fingerprint,strategy_version,code_version,
            runtime_session_id,delivery_status,current_price,engine_snapshot)
            VALUES(%s,'test','ALERT','COMBINED_CONFIRMATION',%s,'BTC','LONG',%s,%s,
            'test','test','test','DELIVERED',100,'{}') ON CONFLICT DO NOTHING''',
            (event_id, start, f'{event_id:064x}', f'{event_id:064x}'))
        path = [fixtures._candle(start+timedelta(minutes=i)) for i in range(candles)]
        outcome = ordered.calculate_ordered_first_touch_outcome(
            reference_price=100.0, direction='LONG', event_time=start, candles=path,
            threshold_pct=0.25, observation_closed=False)
        path_result = {'symbol': 'BTC', 'pair': 'BTCUSDT', 'exchange': 'binance',
            'market': 'spot', 'interval': '1m', 'interval_seconds': 60,
            'complete': True, 'expected_candles': candles, 'provenance': 'SELFTEST', 'candles': path}
        self.assertTrue(self.worker._write_ordered_first_touch_outcome(self.conn,
            event=event, window_minutes=60, reference_source='binance_spot',
            path_result=path_result, outcome=outcome, expected_candles=candles))

    def seed(self):
        for event_id in range(1, 13):
            self.write_outcome(event_id, self.now-timedelta(days=4, minutes=event_id))
            self.conn.execute('''UPDATE research_ordered_first_touch_sync_outbox
                SET created_at_utc=%s,next_attempt_at_utc=%s,updated_at_utc=NOW()
                WHERE event_id=%s''', (self.now-timedelta(days=3)+timedelta(seconds=event_id),)*2+(event_id,))
        for event_id in range(101, 107):
            self.write_outcome(event_id, self.now-timedelta(minutes=107-event_id))
        self.conn.commit()

    def claim(self, limit):
        rows = self.worker._claim_ordered_first_touch_outbox(self.conn, limit)
        self.conn.commit()
        return rows

    def test_eight_claims_reserve_four_fresh_four_backlog_and_restart_alternates(self):
        self.seed()
        first = self.claim(8)
        self.assertEqual({r['event_id'] for r in first}, {1, 2, 3, 4, 103, 104, 105, 106})
        self.assertTrue(all(r['attempts'] == 1 for r in first))
        self.assertEqual(len({str(r['claim_token']) for r in first}), 8)
        # New connection represents a restarted worker. No module-local counter.
        self.conn.close()
        self.conn = self.connect()
        self.assertEqual([r['event_id'] for r in self.claim(1)], [5])
        self.conn.close()
        self.conn = self.connect()
        self.assertEqual([r['event_id'] for r in self.claim(1)], [102])

    def test_source_observation_precedes_bookkeeping_and_start_is_tie_breaker(self):
        self.write_outcome(1, self.now-timedelta(days=3))
        self.write_outcome(2, self.now-timedelta(minutes=10), candles=2)
        self.write_outcome(3, self.now-timedelta(minutes=9))
        # 2 and 3 have equal observed-through, so later measurement-start wins.
        self.conn.execute("UPDATE research_ordered_first_touch_sync_outbox SET updated_at_utc=NOW()+interval '1 day' WHERE event_id=1")
        rows = self.worker._claim_ordered_first_touch_lane(self.conn, 1, recent=True)
        self.assertEqual([r['event_id'] for r in rows], [3])
        self.conn.commit()

    def test_due_retry_and_expired_lease_eligible_but_live_lease_and_future_retry_excluded(self):
        for i in range(1, 5):
            self.write_outcome(i, self.now-timedelta(minutes=10-i))
        claimed = self.worker._claim_ordered_first_touch_lane(self.conn, 2, recent=True)
        self.assertEqual({r['event_id'] for r in claimed}, {3, 4})
        self.conn.execute('''UPDATE research_ordered_first_touch_sync_outbox
            SET claimed_at_utc=NOW()-interval '4 minutes',
                lease_expires_at_utc=NOW()-interval '2 minutes',
                next_attempt_at_utc=NOW()+interval '1 day' WHERE event_id=3''')
        self.conn.execute('''UPDATE research_ordered_first_touch_sync_outbox
            SET sync_status='RETRY',last_error='fixture',next_attempt_at_utc=NOW()+interval '1 day'
            WHERE event_id=2''')
        self.conn.commit()
        self.assertEqual({r['event_id'] for r in self.claim(8)}, {1, 3})
        self.assertEqual(self.claim(8), [])

    def test_stale_generation_ack_and_retry_cannot_consume_recalculated_outcome(self):
        start = self.now-timedelta(minutes=10)
        self.write_outcome(1, start)
        self.conn.commit()
        original = self.claim(1)
        # Actual writer revises an open outcome and invalidates the earlier lease.
        self.write_outcome(1, start, candles=2)
        self.conn.commit()
        self.assertEqual(self.worker._finish_ordered_first_touch_outbox(self.conn, original, delivered=True), 0)
        self.assertEqual(self.worker._finish_ordered_first_touch_outbox(self.conn, original,
            delivered=False, error='late failure'), 0)
        self.conn.commit()
        newer = self.claim(1)
        self.assertNotEqual(newer[0]['claimed_payload_sha256'], original[0]['claimed_payload_sha256'])
        self.assertEqual(self.worker._finish_ordered_first_touch_outbox(self.conn, newer, delivered=True), 1)
        self.conn.commit()
        row = self.conn.execute('SELECT sync_status FROM research_ordered_first_touch_sync_outbox').fetchone()
        self.assertEqual(row['sync_status'], 'SYNCED')

    def test_rollback_preserves_turn_and_other_sender_skips_held_lane_row(self):
        self.seed()
        first = self.worker._claim_ordered_first_touch_outbox(self.conn, 1)
        self.conn.rollback()
        replay = self.worker._claim_ordered_first_touch_outbox(self.conn, 1)
        self.assertEqual(replay[0]['event_id'], first[0]['event_id'])
        self.assertNotEqual(replay[0]['claim_token'], first[0]['claim_token'])
        with self.connect() as other:
            second = self.worker._claim_ordered_first_touch_lane(other, 1, recent=True)
            self.assertNotEqual(second[0]['event_id'], replay[0]['event_id'])
        self.conn.commit()

    def test_failed_concurrent_index_build_keeps_fifo_until_valid_rebuild(self):
        self.seed()
        index = 'idx_ordered_first_touch_sync_fresh_observed'
        self.conn.execute(self.sql.SQL('DROP INDEX {}').format(self.sql.Identifier(index)))
        self.conn.commit()
        # PostgreSQL itself leaves the invalid index behind after this failed
        # concurrent build. No catalog writes or production connections occur.
        with self.connect() as ddl:
            ddl.autocommit = True
            with self.assertRaises(self.psycopg.errors.UniqueViolation):
                ddl.execute(self.sql.SQL('CREATE UNIQUE INDEX CONCURRENTLY {} ON '
                    'research_ordered_first_touch_sync_outbox(destination)').format(
                    self.sql.Identifier(index)))
        state = self.conn.execute('''SELECT i.indisvalid, i.indisready,
                i.indrelid='research_ordered_first_touch_sync_outbox'::regclass AS correct_table
            FROM pg_index i WHERE i.indexrelid=to_regclass(%s)''', (index,)).fetchone()
        self.assertIsNotNone(state, 'Failed concurrent build must leave the named index')
        self.assertFalse(state['indisvalid'])
        self.assertTrue(state['correct_table'])
        before = self.conn.execute('SELECT next_slot FROM research_ordered_first_touch_delivery_cursor').fetchone()
        self.conn.commit()
        self.assertEqual([r['event_id'] for r in self.claim(1)], [1])
        after = self.conn.execute('SELECT next_slot FROM research_ordered_first_touch_delivery_cursor').fetchone()
        self.assertEqual(before, after, 'Invalid index must not advance the freshness cursor')
        # Removing the failed artifact lets the real migration build its index.
        self.conn.execute(self.sql.SQL('DROP INDEX {}').format(self.sql.Identifier(index)))
        self.conn.execute((ROOT/'migrations'/MIGRATIONS[-1]).read_text(), prepare=False)
        self.conn.commit()
        self.assertEqual([r['event_id'] for r in self.claim(1)], [106])
        ready = self.conn.execute('SELECT indisvalid AND indisready AS ready FROM pg_index '
            'WHERE indexrelid=to_regclass(%s)', (index,)).fetchone()
        self.assertTrue(ready['ready'])

    def test_migration_reapply_preserves_cursor_and_payloads(self):
        self.seed()
        self.claim(1)
        before = self.conn.execute('SELECT next_slot FROM research_ordered_first_touch_delivery_cursor').fetchone()
        self.conn.execute((ROOT/'migrations'/MIGRATIONS[-1]).read_text(), prepare=False)
        after = self.conn.execute('SELECT next_slot FROM research_ordered_first_touch_delivery_cursor').fetchone()
        self.assertEqual(before, after)
        self.assertEqual(self.conn.execute('SELECT count(*) AS n FROM research_ordered_first_touch_sync_outbox').fetchone()['n'], 18)
        self.conn.commit()


if __name__ == '__main__':
    unittest.main(verbosity=2)
