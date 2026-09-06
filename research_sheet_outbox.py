"""Durable generic Sheet upserts; source transactions stage, one sender drains."""
from __future__ import annotations
import hashlib
import json
import time
import uuid
from typing import Any, Mapping
import google_sheets_sync
try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:
    psycopg = None
    dict_row = None

_LOCK_ID = 702094113543720211
_SHEET_ROTATION = (
    'Telegram_Events',
    'Snapshots',
    'תצוגת לייב',
    'Episodes',
    'Formula_Results',
)
_rotation_index = 0


def _next_preferred_sheet() -> str:
    global _rotation_index
    sheet = _SHEET_ROTATION[_rotation_index % len(_SHEET_ROTATION)]
    _rotation_index = (_rotation_index + 1) % len(_SHEET_ROTATION)
    return sheet


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, default=str, allow_nan=False)


def stage_upserts(conn: Any, upserts: list[Mapping[str, Any]]) -> int:
    """Stage complete Sheet rows in the caller's transaction, idempotently."""
    records = {}
    for raw in upserts:
        item = dict(raw)
        sheet = str(item['sheet'])
        columns = [name.strip() for name in str(item['key']).split(',')]
        row = item['row']
        if any(row.get(name) in (None, '') for name in columns):
            raise ValueError('Sheet upsert has missing composite key value')
        row_key = _json([str(row[name]) for name in columns])
        payload = _json(item)
        records[(sheet, row_key)] = (sheet, row_key, payload, hashlib.sha256(payload.encode()).hexdigest())
    if not records:
        return 0
    with conn.cursor() as cur:
        cur.executemany('''
            INSERT INTO research_sheet_upsert_outbox(sheet_name,row_key,payload,payload_sha256)
            VALUES (%s,%s,%s::jsonb,%s)
            ON CONFLICT(sheet_name,row_key) DO UPDATE SET
                payload=EXCLUDED.payload, payload_sha256=EXCLUDED.payload_sha256,
                sync_status='PENDING',attempts=0,next_attempt_at_utc=NOW(),
                claim_token=NULL,claimed_payload_sha256=NULL,lease_expires_at_utc=NULL,
                synced_at_utc=NULL,last_error=NULL,updated_at_utc=NOW()
            WHERE research_sheet_upsert_outbox.payload_sha256 IS DISTINCT FROM EXCLUDED.payload_sha256
        ''', list(records.values()))
    return len(records)


def _connect(url: str):
    return psycopg.connect(url, row_factory=dict_row, connect_timeout=5,
                           options='-c statement_timeout=15000 -c lock_timeout=1000')


def _drain_locked(database_url: str, *, max_rows: int = 32, max_seconds: float = 45) -> dict[str, Any]:
    """Acknowledge only a claimed exact generation, never while holding a txn."""
    summary = {'claimed': 0, 'synced': 0, 'failed': 0, 'locked': False}
    if not google_sheets_sync.enabled() or not database_url or psycopg is None:
        return summary
    deadline = time.monotonic() + max(1.0, float(max_seconds))
    with _connect(database_url) as lock_conn:
        acquired = lock_conn.execute('SELECT pg_try_advisory_lock(%s) AS acquired', (_LOCK_ID,)).fetchone()['acquired']
        lock_conn.commit()
        if not acquired:
            summary['locked'] = True
            return summary
        try:
            while summary['claimed'] < max(1, int(max_rows)) and time.monotonic() < deadline:
                token = str(uuid.uuid4())
                count = min(8, google_sheets_sync.ordered_outcome_batch_limit(), int(max_rows)-summary['claimed'])
                preferred_sheet = _next_preferred_sheet()
                with _connect(database_url) as conn:
                    rows = conn.execute('''
                        WITH due AS (
                            SELECT sheet_name,row_key FROM research_sheet_upsert_outbox
                            WHERE (sync_status IN ('PENDING','RETRY') AND next_attempt_at_utc <= NOW())
                               OR (sync_status='IN_FLIGHT' AND lease_expires_at_utc < NOW())
                            ORDER BY
                                CASE WHEN sheet_name=%s THEN 0 ELSE 1 END,
                                next_attempt_at_utc,updated_at_utc,sheet_name,row_key
                            FOR UPDATE SKIP LOCKED LIMIT %s
                        )
                        UPDATE research_sheet_upsert_outbox AS q SET
                            sync_status='IN_FLIGHT',attempts=q.attempts+1,
                            claim_token=%s::uuid,claimed_payload_sha256=q.payload_sha256,
                            lease_expires_at_utc=NOW()+INTERVAL '120 seconds',
                            synced_at_utc=NULL,last_error=NULL
                        FROM due WHERE q.sheet_name=due.sheet_name AND q.row_key=due.row_key
                        RETURNING q.sheet_name,q.row_key,q.payload,q.payload_sha256,q.attempts
                    ''', (preferred_sheet,count,token)).fetchall()
                if not rows:
                    break
                summary['claimed'] += len(rows)
                delivered = google_sheets_sync.deliver_now({'kind':'research_sheet_upserts','upserts':[row['payload'] for row in rows]}, attempts=1)
                with _connect(database_url) as conn:
                    for row in rows:
                        retry_seconds = min(3600, 30 * 2 ** min(7, int(row['attempts'])-1))
                        changed = conn.execute('''
                            UPDATE research_sheet_upsert_outbox SET
                                sync_status=%s,synced_at_utc=CASE WHEN %s THEN NOW() ELSE NULL END,
                                next_attempt_at_utc=NOW()+(%s * INTERVAL '1 second'),
                                last_error=%s,claim_token=NULL,claimed_payload_sha256=NULL,
                                lease_expires_at_utc=NULL,updated_at_utc=NOW()
                            WHERE sheet_name=%s AND row_key=%s AND claim_token=%s::uuid
                              AND payload_sha256=%s AND claimed_payload_sha256=%s
                        ''', ('SYNCED' if delivered else 'RETRY',delivered,retry_seconds,
                              None if delivered else 'Google Sheets did not confirm delivery',
                              row['sheet_name'],row['row_key'],token,row['payload_sha256'],row['payload_sha256']))
                        summary['synced' if delivered else 'failed'] += changed.rowcount
                if not delivered:
                    break
        finally:
            lock_conn.execute('SELECT pg_advisory_unlock(%s)', (_LOCK_ID,))
            lock_conn.commit()
    return summary


def drain(database_url:str,*,max_rows:int=32,max_seconds:float=45)->dict[str,Any]:
    # Coordinate with the ordered-outcome sender BEFORE claiming a DB lease.
    with google_sheets_sync.delivery_slot() as acquired:
        if not acquired:
            return {'claimed':0,'synced':0,'failed':0,'locked':False,'deferred':True}
        return _drain_locked(database_url,max_rows=max_rows,max_seconds=max_seconds)
