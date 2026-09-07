"""Execute real fresh/backlog lease SQL with SQLite compatibility syntax.

SQLite checks selected identities, source ordering, durable cursor fairness,
lease exclusions and actual index selection. It does not emulate concurrent
PostgreSQL row locks; the companion PostgreSQL test exercises those directly.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sqlite3
from types import SimpleNamespace
from uuid import uuid4

import research_outcome_worker as worker

ROOT = Path(__file__).resolve().parent
NOW = 100_000
METHOD = 'ordered-first-touch-v7'
TABLE = 'research_ordered_first_touch_sync_outbox'


def source_time(value):
    if not value or not re.fullmatch(r'\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})', value):
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()
    except ValueError:
        return None


def timestamp(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat() if value is not None else None


class Database:
    def __init__(self, conn, *, ready=True):
        self.conn, self.ready, self.calls = conn, ready, []

    def translate(self, query):
        query = query.replace("NOW() + INTERVAL '2 minutes'", str(NOW+120))
        query = query.replace('NOW()', str(NOW)).replace('%s', '?')
        query = query.replace('FOR UPDATE SKIP LOCKED', '')
        query = query.replace(f'UPDATE {TABLE} queued', f'UPDATE {TABLE} AS queued')
        if 'RETURNING queued.' in query:
            prefix, suffix = query.split('RETURNING ', 1)
            query = prefix + 'RETURNING ' + suffix.replace('queued.', '')
        return query

    def execute(self, query, params=()):
        self.calls.append((query, params))
        if 'to_regclass' in query:
            return SimpleNamespace(fetchone=lambda: {'ready': self.ready})
        if 'WITH picked AS (' in query:
            assert query.count('FOR UPDATE SKIP LOCKED') == 1
            assert 'claimed_payload_sha256=queued.payload_sha256' in query
            assert 'attempts=queued.attempts + 1' in query
            assert "method_version='ordered-first-touch-v7'" in query
            for key in ('event_id', 'window_minutes', 'threshold_bps', 'method_version', 'destination'):
                assert f'queued.{key}=picked.{key}' in query
        return self.conn.execute(self.translate(query), params)


def database():
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    conn.create_function('research_sheet_source_timestamp', 1, source_time, deterministic=True)
    conn.create_function('gen_random_uuid', 0, lambda: str(uuid4()))
    conn.executescript(f'''CREATE TABLE {TABLE} (
        event_id INTEGER,window_minutes INTEGER DEFAULT 60,threshold_bps INTEGER DEFAULT 25,
        method_version TEXT DEFAULT '{METHOD}',destination TEXT DEFAULT 'GOOGLE_SHEETS',
        sync_status TEXT DEFAULT 'PENDING',payload TEXT,payload_sha256 TEXT DEFAULT 'generation-1',
        next_attempt_at_utc INTEGER DEFAULT 1,created_at_utc INTEGER DEFAULT 1,
        updated_at_utc INTEGER DEFAULT 1,lease_expires_at_utc INTEGER,
        attempts INTEGER DEFAULT 0,last_attempt_at_utc INTEGER,claim_token TEXT,
        claimed_at_utc INTEGER,claimed_payload_sha256 TEXT,synced_at_utc INTEGER,last_error TEXT,
        PRIMARY KEY(event_id,window_minutes,threshold_bps,method_version,destination));
        CREATE TABLE research_ordered_first_touch_delivery_cursor (
            singleton BOOLEAN PRIMARY KEY,next_slot INTEGER DEFAULT 0);
        INSERT INTO research_ordered_first_touch_delivery_cursor(singleton) VALUES(TRUE);''')
    for name in ('025_ordered_first_touch_sync_claim_queue.sql', '039_ordered_first_touch_fresh_delivery.sql'):
        source = (ROOT/'migrations'/name).read_text()
        for sql in re.findall(r'CREATE INDEX IF NOT EXISTS.*?;', source, re.S):
            conn.executescript(sql)
            conn.executescript(sql)
    return Database(conn)


def add(db, event_id, observed, *, measured=None, **overrides):
    values = dict(event_id=event_id,
        payload=json.dumps({'row': {'observed_through_utc': timestamp(observed),
            'measurement_start_utc': timestamp(measured if measured is not None else observed)}}),
        **overrides)
    db.conn.execute(f"INSERT INTO {TABLE}({','.join(values)}) VALUES ({','.join('?' for _ in values)})", tuple(values.values()))


def ids(rows):
    return [row['event_id'] for row in rows]


def run():
    service = worker.ResearchOutcomeWorker
    db = database()
    for i in range(1, 201):
        add(db, i, i, created_at_utc=i, updated_at_utc=NOW+1000)
    for i in range(501, 507):
        add(db, i, NOW+i, created_at_utc=NOW-1)
    add(db, 600, NOW+1000, sync_status='RETRY', next_attempt_at_utc=NOW+1)
    add(db, 601, NOW+1000, sync_status='IN_FLIGHT', lease_expires_at_utc=NOW+1)
    add(db, 602, NOW+1000, sync_status='SYNCED')
    add(db, 603, NOW+1000, sync_status='DEAD_LETTER')
    add(db, 604, NOW+1000, method_version='audit-version')
    add(db, 605, NOW+1000, destination='OTHER')
    rows = service._claim_ordered_first_touch_outbox(db, 8)
    assert set(ids(rows)) == {1, 2, 3, 4, 503, 504, 505, 506}, ids(rows)
    assert len({row['claim_token'] for row in rows}) == 8
    assert all(row['attempts'] == 1 and row['claimed_payload_sha256'] == 'generation-1' for row in rows)
    leases = db.conn.execute(f'SELECT lease_expires_at_utc FROM {TABLE} WHERE claim_token IS NOT NULL').fetchall()
    assert all(tuple(row) == (NOW+120,) for row in leases)
    # A fresh wrapper after every call models process restarts: only DB state persists.
    assert ids(service._claim_ordered_first_touch_outbox(Database(db.conn), 1)) == [5]
    assert ids(service._claim_ordered_first_touch_outbox(Database(db.conn), 1)) == [502]
    assert ids(service._claim_ordered_first_touch_outbox(Database(db.conn), 1)) == [6]
    assert ids(service._claim_ordered_first_touch_outbox(Database(db.conn), 1)) == [501]

    isolated = database()
    add(isolated, 1, NOW-20, sync_status='IN_FLIGHT', lease_expires_at_utc=NOW-1,
        next_attempt_at_utc=NOW+500)
    add(isolated, 2, None)
    add(isolated, 3, NOW, sync_status='RETRY', next_attempt_at_utc=NOW+1)
    add(isolated, 4, NOW, sync_status='IN_FLIGHT', lease_expires_at_utc=NOW+1)
    assert set(ids(service._claim_ordered_first_touch_outbox(isolated, 8))) == {1, 2}
    assert service._claim_ordered_first_touch_outbox(isolated, 8) == []
    # A missing migration continues via the old backlog lane without source time.
    fallback = database()
    add(fallback, 1, None)
    assert ids(service._claim_ordered_first_touch_outbox(Database(fallback.conn, ready=False), 1)) == [1]

    tied = database()
    add(tied, 1, NOW, measured=NOW-100, updated_at_utc=NOW+1000)
    add(tied, 2, NOW, measured=NOW-10, updated_at_utc=1)
    add(tied, 3, NOW-1, updated_at_utc=NOW+9999)
    assert ids(service._claim_ordered_first_touch_lane(tied, 1, recent=True)) == [2]
    assert service._claim_ordered_first_touch_lane(tied, 0, recent=False) == []
    capture = Database(tied.conn)
    service._claim_ordered_first_touch_lane(capture, 99999, recent=False)
    assert capture.calls[-1][1] == (worker._ORDERED_FIRST_TOUCH_OUTBOX_LIMIT,)

    # Exhaust oldest-only selection to prove all supported identities progress once.
    exhausted = database()
    for i in range(1, 90):
        add(exhausted, i, i, created_at_utc=i)
    consumed = []
    while batch := service._claim_ordered_first_touch_lane(exhausted, 8, recent=False):
        consumed.extend(ids(batch))
    assert consumed == list(range(1, 90))
    assert len(consumed) == len(set(consumed))

    # Explain each actual selection, with the production partial index predicate.
    for recent, expected in ((True, 'idx_ordered_first_touch_sync_fresh_observed'),
                             (False, 'idx_ordered_first_touch_sync_claim_queue')):
        service._claim_ordered_first_touch_lane(db, 2, recent=recent)
        query, params = db.calls[-1]
        selection = query.split('WITH picked AS (', 1)[1].split(')\n            UPDATE', 1)[0]
        plan = db.conn.execute('EXPLAIN QUERY PLAN '+db.translate(selection), params).fetchall()
        assert expected in str([tuple(row) for row in plan]), plan
    print('ordered First Touch fresh/backlog claim selftest passed')


if __name__ == '__main__':
    run()
