"""Bounded latest evaluated outcome details, separate from statistical evidence.

Each slot can refer to a different source alert. The full historical research
stays canonical in PostgreSQL; this view never changes its labels or cohorts.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Any, Mapping

VERSION = 'outcomes-current-latest-evaluated-slot-v1'
SHEET = 'Outcomes_Current'
METHOD = 'ordered-first-touch-v7'
KEY = 'symbol,direction,window_minutes,threshold_bps'
SYMBOLS = ('BTC', 'ETH', 'SOL', 'HYPE', 'DOGE', 'ZEC', 'BNB', 'XRP')
DIRECTIONS = ('LONG', 'SHORT')
HORIZONS = (60, 240, 720, 1440)
THRESHOLDS_BPS = tuple(range(25, 201, 25))
MAX_DATA_ROWS = 512
MAX_COLUMNS = 34
HEADERS = (
    'event_id', 'snapshot_id', 'symbol', 'direction', 'threshold_pct',
    'measurement_start_utc', 'status', 'first_touch_side', 'decision_time_utc',
    'minutes_to_decision', 'mfe_pct', 'mae_pct', 'favorable_touch_price',
    'adverse_touch_price', 'max_favorable_price', 'max_adverse_price',
    'market_source', 'market_pair', 'candle_interval', 'candle_count',
    'data_quality_status', 'outcome_method_version', 'outcome_id',
    'window_minutes', 'threshold_bps', 'observed_from_utc',
    'observed_through_utc', 'terminal_reason', 'initial_gap_seconds',
    'favorable_barrier_price', 'adverse_barrier_price', 'initial_gap_unobserved',
    'data_quality_note', 'path_complete',
)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, default=str, allow_nan=False)


def source_time(value: Any) -> datetime | None:
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return None
        return parsed.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        return None


def valid_row(row: Mapping[str, Any]) -> bool:
    if set(row) != set(HEADERS):
        return False
    event_id = row.get('event_id')
    if not isinstance(event_id, str) or not re.fullmatch(r'[1-9][0-9]{0,18}', event_id):
        return False
    if int(event_id) > 9223372036854775807:
        return False
    window, bps = row.get('window_minutes'), row.get('threshold_bps')
    return bool(row.get('symbol') in SYMBOLS and row.get('direction') in DIRECTIONS
                and type(window) is int and window in HORIZONS
                and type(bps) is int and bps in THRESHOLDS_BPS
                and type(row.get('threshold_pct')) in (int, float)
                and row['threshold_pct'] == bps / 100
                and row.get('outcome_method_version') == METHOD
                and row.get('outcome_id') == '|'.join((event_id, str(window), str(bps), METHOD))
                and source_time(row.get('measurement_start_utc')) is not None)


def serialized_row(row: Mapping[str, Any]) -> dict[str, Any] | None:
    """Use interoperable ISO(T) timestamps without changing their instants."""
    result = dict(row)
    for field in ('measurement_start_utc', 'decision_time_utc',
                  'observed_from_utc', 'observed_through_utc'):
        value = result.get(field)
        if value not in (None, ''):
            parsed = source_time(value)
            if parsed is None:
                return None
            result[field] = parsed.isoformat()
    return result


def project(event: Mapping[str, Any], row: Mapping[str, Any]) -> dict[str, Any] | None:
    row = serialized_row(row)
    if row is None:
        return None
    if (event.get('event_kind') != 'ALERT' or event.get('delivery_status') != 'DELIVERED'
            or str(event.get('event_id')) != row.get('event_id')
            or event.get('symbol') != row.get('symbol')
            or event.get('direction') != row.get('direction')
            or source_time(event.get('alert_time_utc')) != source_time(row.get('measurement_start_utc'))
            or not valid_row(row)):
        return None
    return {'sheet': SHEET, 'key': KEY, 'row': dict(row)}


def stage_projected(conn: Any, item: Mapping[str, Any]) -> int:
    """Stage one slot atomically; older events cannot replace newer events.

    For the same event, accept the generation already approved by canonical
    persistence. A complete gap repair may legitimately decide earlier than
    an incomplete diagnostic prefix. Publication therefore never compares
    decision_time or observed_through to reject same-event repairs.
    """
    raw = item.get('row')
    row = serialized_row(raw) if isinstance(raw, Mapping) else None
    if item.get('sheet') != SHEET or item.get('key') != KEY or row is None or not valid_row(row):
        return 0
    payload = _json({'sheet': SHEET, 'key': KEY, 'row': dict(row)})
    row_key = _json([str(row[name]) for name in KEY.split(',')])
    sha = hashlib.sha256(payload.encode()).hexdigest()
    written = conn.execute('''INSERT INTO research_sheet_upsert_outbox
        (sheet_name,row_key,payload,payload_sha256)
        VALUES(%s,%s,%s::jsonb,%s)
        ON CONFLICT(sheet_name,row_key) DO UPDATE SET
            payload=EXCLUDED.payload,payload_sha256=EXCLUDED.payload_sha256,
            sync_status='PENDING',attempts=0,next_attempt_at_utc=NOW(),
            claim_token=NULL,claimed_payload_sha256=NULL,lease_expires_at_utc=NULL,
            synced_at_utc=NULL,last_error=NULL,updated_at_utc=NOW()
        WHERE research_sheet_upsert_outbox.payload_sha256 IS DISTINCT FROM EXCLUDED.payload_sha256
          AND (
            research_sheet_source_timestamp(EXCLUDED.payload->'row'->>'measurement_start_utc'),
            (EXCLUDED.payload->'row'->>'event_id')::bigint
          ) >= (
            research_sheet_source_timestamp(research_sheet_upsert_outbox.payload->'row'->>'measurement_start_utc'),
            (research_sheet_upsert_outbox.payload->'row'->>'event_id')::bigint
          )
        RETURNING row_key''', (SHEET, row_key, payload, sha)).fetchone()
    if not written:
        return 0
    # Existing trigger026 does not know this tab. Only the accepted exact
    # generation receives its immutable source time; an ignored old backfill
    # cannot regress the newer slot's source-time priority.
    conn.execute('''UPDATE research_sheet_upsert_outbox SET source_time_utc=%s
        WHERE sheet_name=%s AND row_key=%s AND payload_sha256=%s''',
        (source_time(row['measurement_start_utc']), SHEET, row_key, sha))
    return 1


def stage(conn: Any, event: Mapping[str, Any], row: Mapping[str, Any]) -> int:
    item = project(event, row)
    return stage_projected(conn, item) if item is not None else 0


def status() -> dict[str, Any]:
    return {'policy_version': VERSION, 'destination': SHEET,
            'state': 'PARTIAL_PUBLICATION', 'method_version': METHOD,
            'max_data_rows': MAX_DATA_ROWS, 'max_columns': MAX_COLUMNS,
            'max_grid_cells': (MAX_DATA_ROWS + 1) * MAX_COLUMNS,
            'source_population': 'Native delivered alerts; naturally evaluated rows only',
            'selection': 'Latest measurement_start_utc then event_id per coin/direction/horizon/threshold',
            'interpretation': 'Current details; slots may reference different events; not a statistical cohort',
            'legacy_destination': 'Outcomes',
            'legacy_publication': 'HELD_IN_DATABASE; existing queue states and sheet rows preserved',
            'sender': 'GENERIC_EXACT_GENERATION_OUTBOX',
            'canonical_research': 'FULL_DATABASE_EVIDENCE_RETAINED',
            'research_effect': 'NONE'}
