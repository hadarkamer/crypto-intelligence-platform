"""Durable generic Sheet upserts; source transactions stage, one sender drains."""
from __future__ import annotations
import hashlib
import json
import re
import time
import uuid
from datetime import datetime, timezone
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
    'MaxPain_TF',
    'Snapshots',
    'תצוגת לייב',
    'Episodes',
    'Formula_Results',
)
_WAVE_REPORT_SHEET = 'MaxPain_Wave_Live'
_SOURCE_TIMESTAMP = re.compile(
    r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$'
)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, default=str, allow_nan=False)


def _source_time(value: Any) -> datetime | None:
    """Only timezone-qualified source timestamps can prioritize fresh data."""
    if isinstance(value, str):
        if not _SOURCE_TIMESTAMP.fullmatch(value):
            return None
        try:
            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            return None
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        return None
    try:
        return value.astimezone(timezone.utc)
    except (OverflowError, ValueError):
        return None


def _row_source_time(item: Mapping[str, Any]) -> datetime | None:
    field = {
        'Snapshots': 'timestamp_utc',
        'Telegram_Events': 'timestamp_utc',
        'MaxPain_TF': 'timestamp_utc',
        'Episodes': 'opened_at_utc',
        'Formula_Results': 'last_evaluated_at',
        'MaxPain_Wave_Live': 'last_evaluated_at',
    }.get(str(item['sheet']))
    return _source_time(item['row'].get(field)) if field else None


def stage_upserts(conn: Any, upserts: list[Mapping[str, Any]]) -> int:
    """Stage complete Sheet rows in the caller's transaction, idempotently."""
    records = {}
    snapshot_times = {
        str(item['row'].get('snapshot_id')): _row_source_time(item)
        for item in upserts if item['sheet'] == 'Snapshots'
    }
    for raw in upserts:
        item = dict(raw)
        sheet = str(item['sheet'])
        columns = [name.strip() for name in str(item['key']).split(',')]
        row = item['row']
        if any(row.get(name) in (None, '') for name in columns):
            raise ValueError('Sheet upsert has missing composite key value')
        row_key = _json([str(row[name]) for name in columns])
        if sheet == 'תצוגת לייב':
            # The visible Israel date is never parsed or mistaken for UTC.
            source_time = snapshot_times.get(str(row.get('snapshot_id')))
            item['source_time_utc'] = source_time.isoformat() if source_time else None
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
    # Migration 026 predates this tab and its trigger yields NULL for it.
    # Apply its frozen event timestamp through the existing indexed row key;
    # no global backfill, implicit time inference, or DDL in the live worker.
    source_times = [
        (sheet, row_key, _row_source_time(json.loads(record[2])))
        for (sheet, row_key), record in records.items()
        if sheet in {'MaxPain_TF', 'MaxPain_Wave_Live'}
    ]
    if source_times:
        values_sql = ','.join(['(%s,%s,%s::timestamptz)'] * len(source_times))
        conn.execute(f"""
            UPDATE research_sheet_upsert_outbox AS queued
            SET source_time_utc=source.source_time_utc
            FROM (VALUES {values_sql}) AS source(sheet_name,row_key,source_time_utc)
            WHERE queued.sheet_name=source.sheet_name AND queued.row_key=source.row_key
              AND queued.source_time_utc IS DISTINCT FROM source.source_time_utc
        """, tuple(value for row in source_times for value in row))
    return len(records)


def _connect(url: str):
    return psycopg.connect(url, row_factory=dict_row, connect_timeout=5,
                           options='-c statement_timeout=15000 -c lock_timeout=1000')


def _claim_lane(conn: Any, *, count: int, token: str, sheet: str | None,
                recent: bool, exclude_sheet: str | None = None) -> list[Mapping[str, Any]]:
    if count <= 0:
        return []
    sheet_filter = 'AND sheet_name=%s' if sheet is not None else ''
    exclusion_filter = 'AND sheet_name<>%s' if exclude_sheet is not None else ''
    source_filter = 'AND source_time_utc IS NOT NULL' if recent else ''
    order = ('source_time_utc DESC, next_attempt_at_utc, created_at_utc, row_key'
             if recent else 'next_attempt_at_utc, created_at_utc, row_key')
    params = (*((sheet,) if sheet is not None else ()),
              *((exclude_sheet,) if exclude_sheet is not None else ()),count,token)
    return conn.execute(f'''
        WITH due AS (
            SELECT sheet_name,row_key FROM research_sheet_upsert_outbox
            WHERE sync_status IN ('PENDING','RETRY','IN_FLIGHT')
              AND ((sync_status IN ('PENDING','RETRY') AND next_attempt_at_utc <= NOW())
                OR (sync_status='IN_FLIGHT' AND lease_expires_at_utc < NOW()))
              {sheet_filter} {exclusion_filter} {source_filter}
            ORDER BY {order}
            FOR UPDATE SKIP LOCKED LIMIT %s
        )
        UPDATE research_sheet_upsert_outbox AS q SET
            sync_status='IN_FLIGHT',attempts=q.attempts+1,
            claim_token=%s::uuid,claimed_payload_sha256=q.payload_sha256,
            lease_expires_at_utc=NOW()+INTERVAL '120 seconds',
            synced_at_utc=NULL,last_error=NULL
        FROM due WHERE q.sheet_name=due.sheet_name AND q.row_key=due.row_key
        RETURNING q.sheet_name,q.row_key,q.payload,q.payload_sha256,q.attempts
    ''', params).fetchall()


def _claim_batch(conn: Any, count: int, token: str) -> list[Mapping[str, Any]]:
    # Persist the turn so restarts and one-row HTTP batches retain both shares.
    slot = conn.execute('''
        UPDATE research_sheet_delivery_cursor SET next_slot=next_slot+1
        WHERE singleton=TRUE RETURNING next_slot-1 AS slot
    ''').fetchone()['slot']
    # A finite 136-row report must finish before its next 15-minute generation.
    # Three of four batch turns first serve that report; the fourth always
    # rotates ordinary sheets. Once the report drains, all turns immediately
    # return to ordinary work. The HTTP size and exact-generation ACK do not
    # change. This cursor survives restarts, including single-row fallback.
    preferred_sheet = _SHEET_ROTATION[(slot // 4) % len(_SHEET_ROTATION)]
    recent_first = (slot // (4 * len(_SHEET_ROTATION))) % 2 == 0
    rows = []
    if slot % 4 < 3:
        rows = _claim_lane(conn, count=count, token=token,
                           sheet=_WAVE_REPORT_SHEET, recent=False)
    first_count = (count-len(rows)+1) // 2
    rows += _claim_lane(conn, count=first_count, token=token,
                       sheet=preferred_sheet, recent=recent_first)
    rows += _claim_lane(conn, count=count-len(rows), token=token,
                        sheet=preferred_sheet, recent=not recent_first)
    # A sheet with only undated/backlog rows must still use a recent turn.
    if len(rows) < count:
        rows += _claim_lane(conn, count=count-len(rows), token=token,
                            sheet=preferred_sheet, recent=False)
    # Empty preferred sheets do not waste capacity; the next turn still rotates.
    # A reserved ordinary turn must check *all* ordinary work before returning
    # to the report: an empty preferred tab cannot give its guarantee away to
    # older report rows while another ordinary tab has a due backlog.
    if len(rows) < count and slot % 4 == 3:
        rows += _claim_lane(conn,count=count-len(rows),token=token,
                            sheet=None,recent=False,exclude_sheet=_WAVE_REPORT_SHEET)
    if len(rows) < count:
        rows += _claim_lane(conn, count=count-len(rows), token=token,
                            sheet=None, recent=False)
    return rows


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
            ready = lock_conn.execute('''
                SELECT to_regclass('research_sheet_delivery_cursor') IS NOT NULL
                    AND EXISTS (SELECT 1 FROM pg_attribute
                        WHERE attrelid=to_regclass('research_sheet_upsert_outbox')
                          AND attname='source_time_utc' AND NOT attisdropped) AS ready
            ''').fetchone()['ready']
            lock_conn.commit()
            if not ready:
                return dict(summary, deferred=True,
                            reason='MISSING_FRESH_DELIVERY_MIGRATION_026')
            while summary['claimed'] < max(1, int(max_rows)) and time.monotonic() < deadline:
                token = str(uuid.uuid4())
                count = min(8, google_sheets_sync.ordered_outcome_batch_limit(), int(max_rows)-summary['claimed'])
                with _connect(database_url) as conn:
                    rows = _claim_batch(conn, count, token)
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
