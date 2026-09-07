"""Exercise actual outbox selection/lease SQL using SQLite compatibility syntax.

Checks source-time semantics, sheet fairness, old/new progress with one-row
batches, due retries, expired leases and partial-index selection. SQLite does
not simulate PostgreSQL concurrent row locking; its clauses are checked here.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

import research_formula_schema_admin as schema
import research_sheet_outbox as outbox

NOW = 100_000
ROOT = Path(__file__).resolve().parent


class Database:
    def __init__(self, conn):
        self.conn = conn
        self.calls = []

    def execute(self, query, params=()):
        self.calls.append((query, params))
        if 'WITH due AS (' in query:
            assert "FOR UPDATE SKIP LOCKED LIMIT %s" in query
            assert "sync_status IN ('PENDING','RETRY','IN_FLIGHT')" in query
            assert 'q.sheet_name=due.sheet_name AND q.row_key=due.row_key' in query
            assert 'claimed_payload_sha256=q.payload_sha256' in query
        sql = query.replace("NOW()+INTERVAL '120 seconds'", str(NOW + 120))
        sql = sql.replace("NOW()+(%s * INTERVAL '1 second')", f'({NOW} + %s)')
        sql = sql.replace('NOW()', str(NOW)).replace('%s', '?').replace('::uuid', '')
        sql = sql.replace('FOR UPDATE SKIP LOCKED', '')
        if 'RETURNING q.' in sql:
            prefix, returning = sql.split('RETURNING ', 1)
            sql = prefix + 'RETURNING ' + returning.replace('q.', '')
        return self.conn.execute(sql, params)


def _database():
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    conn.executescript('''
        CREATE TABLE research_sheet_upsert_outbox(
            sheet_name TEXT, row_key TEXT, payload TEXT DEFAULT '{}',
            payload_sha256 TEXT DEFAULT 'generation-1',
            source_time_utc INTEGER, sync_status TEXT DEFAULT 'PENDING',
            next_attempt_at_utc INTEGER DEFAULT 1, created_at_utc INTEGER DEFAULT 1,
            updated_at_utc INTEGER DEFAULT 1, lease_expires_at_utc INTEGER,
            attempts INTEGER DEFAULT 0, claim_token TEXT, claimed_payload_sha256 TEXT,
            synced_at_utc INTEGER, last_error TEXT,
            PRIMARY KEY(sheet_name,row_key)
        );
        CREATE TABLE research_sheet_delivery_cursor(singleton BOOLEAN PRIMARY KEY,
            next_slot INTEGER DEFAULT 0);
        INSERT INTO research_sheet_delivery_cursor(singleton) VALUES (TRUE);
    ''')
    migration = ROOT / 'migrations/026_research_sheet_fresh_delivery.sql'
    assert migration in schema.MIGRATION_PATHS
    source = migration.read_text(encoding='utf-8')
    for index in re.findall(r'CREATE INDEX IF NOT EXISTS.*?;', source, re.S):
        conn.executescript(index)
        conn.executescript(index)
    return Database(conn)


def _add(db, sheet, key, source, **overrides):
    values = dict(sheet_name=sheet, row_key=key, source_time_utc=source, **overrides)
    db.conn.execute(
        f"INSERT INTO research_sheet_upsert_outbox({','.join(values)}) "
        f"VALUES ({','.join('?' for _ in values)})", tuple(values.values())
    )


def _stage_test():
    rows = []
    class Cursor:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def executemany(self, query, params):
            assert 'payload_sha256 IS DISTINCT FROM EXCLUDED.payload_sha256' in query
            assert 'INSERT INTO research_sheet_upsert_outbox(sheet_name,row_key,payload,payload_sha256)' in query
            rows.extend(params)
    instant = datetime(2026, 9, 7, 1, 2, 3, tzinfo=timezone.utc)
    items = [
        {'sheet': 'תצוגת לייב', 'key': 'snapshot_id', 'row': {
            'snapshot_id': 's1', 'זמן סריקה': '07/09/2026 04:02:03'}},
        {'sheet': 'Snapshots', 'key': 'snapshot_id', 'row': {
            'snapshot_id': 's1', 'timestamp_utc': '2026-09-07T01:02:03Z'}},
        {'sheet': 'Telegram_Events', 'key': 'event_id', 'row': {
            'event_id': 'e1', 'timestamp_utc': '2026-09-07T04:02:03+03:00'}},
        {'sheet': 'Episodes', 'key': 'episode_id', 'row': {
            'episode_id': 'p1', 'opened_at_utc': instant}},
        {'sheet': 'Formula_Results', 'key': 'formula_id', 'row': {
            'formula_id': 'f1', 'last_evaluated_at': instant}},
    ]
    assert outbox.stage_upserts(SimpleNamespace(cursor=Cursor), items) == 5
    assert all(outbox._row_source_time(item) == instant for item in items[1:])
    assert outbox._source_time(json.loads(rows[0][2])['source_time_utc']) == instant
    assert 'source_time_utc' not in items[0], 'staging must not mutate caller payload'
    rows.clear()
    assert outbox.stage_upserts(SimpleNamespace(cursor=Cursor), [items[0]]) == 1
    assert json.loads(rows[0][2])['source_time_utc'] is None, 'display date must not stand in for source UTC'
    for bad in ('', '2026-09-07', '2026-09-07T01:02:03', '2026-02-30T00:00:00Z',
                '20260907T010203Z', '2026-09-07T01:02+03:00',
                '0001-01-01T00:00:00+14:00', 'today', 'infinity', None, 0):
        assert outbox._source_time(bad) is None


def _delivery_ack_test():
    """A stale HTTP confirmation cannot consume a newer source generation."""
    class LockConnection:
        def __init__(self, ready):
            self.ready = ready
            self.unlocked = False
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def commit(self):
            pass
        def execute(self, query, params=()):
            if 'pg_try_advisory_lock' in query:
                return SimpleNamespace(fetchone=lambda: {'acquired': True})
            if 'pg_advisory_unlock' in query:
                self.unlocked = True
                return SimpleNamespace(fetchone=lambda: {})
            assert 'to_regclass' in query
            return SimpleNamespace(fetchone=lambda: {'ready': self.ready})

    for ready, result_kind in ((False, 'deferred'), (True, 'delivered'),
                               (True, 'changed-generation'), (True, 'retry')):
        db = _database()
        _add(db, outbox._SHEET_ROTATION[0], 'event', NOW)
        db.conn.commit()
        lock = LockConnection(ready)
        connection_count = 0
        open_transactions = 0
        http_calls = 0

        class Transaction(Database):
            def __enter__(self):
                nonlocal open_transactions
                open_transactions += 1
                return self
            def __exit__(self, kind, *args):
                nonlocal open_transactions
                open_transactions -= 1
                self.conn.rollback() if kind else self.conn.commit()

        def connect(url):
            nonlocal connection_count
            connection_count += 1
            return lock if connection_count == 1 else Transaction(db.conn)

        def deliver(payload, **kwargs):
            nonlocal http_calls
            http_calls += 1
            assert open_transactions == 0, 'HTTP must follow committed claim'
            assert len(payload['upserts']) == 1
            if result_kind == 'changed-generation':
                db.conn.execute('''UPDATE research_sheet_upsert_outbox SET
                    payload_sha256='generation-2',sync_status='PENDING',attempts=0,
                    claim_token=NULL,claimed_payload_sha256=NULL,
                    lease_expires_at_utc=NULL''')
                db.conn.commit()
            return result_kind != 'retry'

        with patch.object(outbox, '_connect', connect), \
             patch.object(outbox, 'psycopg', object()), \
             patch.object(outbox.google_sheets_sync, 'enabled', return_value=True), \
             patch.object(outbox.google_sheets_sync, 'ordered_outcome_batch_limit', return_value=1), \
             patch.object(outbox.google_sheets_sync, 'deliver_now', deliver):
            summary = outbox._drain_locked('postgresql://selftest', max_rows=1)
        assert lock.unlocked
        row = db.conn.execute('SELECT * FROM research_sheet_upsert_outbox').fetchone()
        if not ready:
            assert summary['deferred'] and summary['claimed'] == 0
            assert summary['reason'] == 'MISSING_FRESH_DELIVERY_MIGRATION_026'
            assert http_calls == 0 and row['sync_status'] == 'PENDING'
        elif result_kind == 'changed-generation':
            assert summary['claimed'] == 1 and summary['synced'] == 0
            assert row['sync_status'] == 'PENDING' and row['payload_sha256'] == 'generation-2'
        elif result_kind == 'delivered':
            assert summary['synced'] == 1 and row['sync_status'] == 'SYNCED'
            assert row['claim_token'] is None and row['synced_at_utc'] == NOW
        else:
            assert summary['failed'] == 1 and row['sync_status'] == 'RETRY'
            assert row['next_attempt_at_utc'] == NOW+30 and row['claim_token'] is None


def run():
    _stage_test()
    _delivery_ack_test()
    db = _database()
    for sheet in outbox._SHEET_ROTATION:
        for index in range(200):
            _add(db, sheet, f'old-{index:03}', index, created_at_utc=index)
        _add(db, sheet, 'fresh', NOW, next_attempt_at_utc=NOW-1,
             created_at_utc=NOW-1)
        _add(db, sheet, 'future-retry', NOW+1, sync_status='RETRY',
             next_attempt_at_utc=NOW+60)
        _add(db, sheet, 'active-lease', NOW+2, sync_status='IN_FLIGHT',
             lease_expires_at_utc=NOW+30)
        _add(db, sheet, 'already-synced', NOW+3, sync_status='SYNCED')

    # New wrapper for each call models process restarts: cursor lives in DB.
    first_cycle = []
    for index in range(2 * len(outbox._SHEET_ROTATION)):
        fresh_process = Database(db.conn)
        batch = outbox._claim_batch(fresh_process, 1, f'token-{index}')
        assert len(batch) == 1
        first_cycle.extend((row['sheet_name'], row['row_key']) for row in batch)
    assert first_cycle[:len(outbox._SHEET_ROTATION)] == [(sheet, 'fresh') for sheet in outbox._SHEET_ROTATION]
    assert first_cycle[len(outbox._SHEET_ROTATION):] == [(sheet, 'old-000') for sheet in outbox._SHEET_ROTATION]
    assert len(set(first_cycle)) == 2 * len(outbox._SHEET_ROTATION)

    # Larger batches reserve both shares and cannot select their own new leases.
    db.conn.execute('UPDATE research_sheet_delivery_cursor SET next_slot=0')
    sheet = outbox._SHEET_ROTATION[0]
    for index in range(6):
        _add(db, sheet, f'new-{index}', NOW+index+10,
             next_attempt_at_utc=NOW-1, created_at_utc=NOW-1)
    batch = outbox._claim_batch(db, 8, 'balanced')
    keys = {row['row_key'] for row in batch}
    assert keys == {'new-5', 'new-4', 'new-3', 'new-2',
                    'old-001', 'old-002', 'old-003', 'old-004'}, keys
    claimed = db.conn.execute('''SELECT attempts,claim_token,claimed_payload_sha256,
        lease_expires_at_utc FROM research_sheet_upsert_outbox
        WHERE claim_token='balanced' ''').fetchall()
    assert len(claimed) == 8
    assert all(tuple(row) == (1, 'balanced', 'generation-1', NOW+120) for row in claimed)

    # Newest retry may be ineligible, while an expired lease with a future
    # retry deadline must remain reclaimable. Undated rows still make progress.
    isolated = _database()
    _add(isolated, sheet, 'expired', NOW, sync_status='IN_FLIGHT',
         lease_expires_at_utc=NOW-1, next_attempt_at_utc=NOW+100)
    _add(isolated, sheet, 'undated', None)
    _add(isolated, sheet, 'future', NOW+1, sync_status='RETRY', next_attempt_at_utc=NOW+1)
    assert {row['row_key'] for row in outbox._claim_batch(isolated, 8, 'reclaim')} == {
        'expired', 'undated'}
    assert outbox._claim_batch(isolated, 8, 'empty') == []
    other = _database()
    _add(other, 'Additional_Sheet', 'undated', None)
    assert outbox._claim_batch(other, 1, 'fallback')[0]['row_key'] == 'undated'

    # Explain the actual due selection, with the same partial-index predicate.
    for recent, expected_index in ((True, 'idx_research_sheet_fresh_claim'),
                                    (False, 'idx_research_sheet_backlog_claim')):
        outbox._claim_lane(db, count=2, token='explain', sheet=sheet, recent=recent)
        query, params = db.calls[-1]
        selection = query.split('WITH due AS (', 1)[1].split(')\n        UPDATE', 1)[0]
        selection = selection.replace('NOW()', str(NOW)).replace('%s', '?')
        selection = selection.replace('FOR UPDATE SKIP LOCKED', '')
        plan = db.conn.execute('EXPLAIN QUERY PLAN ' + selection, params[:-1]).fetchall()
        assert expected_index in str([tuple(row) for row in plan]), plan
    print('research Sheet fresh/backlog delivery selftest: PASS')


if __name__ == '__main__':
    run()
