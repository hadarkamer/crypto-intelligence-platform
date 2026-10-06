"""Finite latest-state projections; canonical historical evidence stays in PostgreSQL.

Snapshots and the Hebrew live view share a frozen source snapshot. MaxPain slots
retain each observed timeframe and liquidation side, including legacy timeframes.
This is a current detail view, never a research cohort or a fresh market scan.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

from research_outcome_publication import source_time, SYMBOLS, DIRECTIONS
from research_maxpain_sheet_rows import TIMEFRAMES

VERSION = 'bounded-live-publication-v1'
TELEGRAM_MAX_ROWS = 32_000
TELEGRAM_RETENTION_DAYS = 16
AUDIT_MAX_SECONDS = 24 * 60 * 60
CONFIG = {
    "Snapshots": {
        "sheet": "Snapshots_Current",
        "key": "symbol,direction",
        "headers": [
            "snapshot_id",
            "timestamp_utc",
            "timestamp_israel",
            "watch_scan_id",
            "parent_event_id",
            "btc_parent_movement_id",
            "symbol",
            "direction",
            "no_alert_snapshot",
            "reference_price",
            "market_session",
            "is_weekend",
            "data_quality_status",
            "alert_sent",
            "alert_types",
            "primary_alert_type",
            "telegram_event_count",
            "price_oi_total_direction",
            "price_oi_total_score",
            "futures_cvd_total_direction",
            "futures_cvd_total_score",
            "spot_cvd_total_direction",
            "spot_cvd_total_score",
            "all_three_aligned",
            "strict_triple_65_match",
            "maxpain_selected_timeframe",
            "maxpain_selected_score",
            "maxpain_opposite_score",
            "maxpain_score_edge",
            "maxpain_score_ratio",
            "maxpain_direction_average",
            "maxpain_opposite_average",
            "maxpain_average_edge",
            "maxpain_average_ratio",
            "consensus_hits",
            "consensus_total",
            "target_price",
            "target_distance_pct",
            "liquidity_balance_pct",
            "selected_liquidity_usd",
            "opposite_liquidity_usd",
            "magnet_exists",
            "magnet_side",
            "magnet_rank",
            "magnet_low",
            "magnet_high",
            "magnet_quality",
            "magnet_spread_pct",
            "liquidity_edge_pct",
            "strategy_version",
            "code_version",
            "snapshot_written_at",
            "displayed_direction",
            "analysis_direction",
            "liquidity_long_pct",
            "liquidity_short_pct",
            "liquidity_timeframe",
            "liquidity_data_source",
            "liquidity_by_timeframe_json"
        ],
        "max_rows": 16,
        "identity": "snapshot_id",
        "symbol": "symbol",
        "side": "direction"
    },
    "תצוגת לייב": {
        "sheet": "Live_Current",
        "key": "מטבע,כיוון נבדק",
        "headers": [
            "זמן סריקה",
            "מטבע",
            "כיוון נבדק",
            "מחיר ייחוס",
            "נשלחה התראה",
            "סוג התראה",
            "Price/OI כולל",
            "כיוון Price/OI",
            "Futures CVD כולל",
            "כיוון Futures",
            "Spot CVD כולל",
            "כיוון Spot",
            "שלישייה 65+",
            "MaxPain נבחר",
            "MaxPain נגדי",
            "פער MaxPain",
            "ממוצע לכיוון",
            "ממוצע נגדי",
            "יעד",
            "מרחק ליעד",
            "מאזן נזילות",
            "סטטוס נתונים",
            "snapshot_id",
            "כיוון מוצג",
            "כיוון ניתוח",
            "timestamp_utc"
        ],
        "max_rows": 16,
        "identity": "snapshot_id",
        "symbol": "מטבע",
        "side": "כיוון נבדק"
    },
    "MaxPain_TF": {
        "sheet": "MaxPain_Current",
        "key": "symbol,timeframe,source_side",
        "headers": [
            "snapshot_id",
            "symbol",
            "direction",
            "timeframe",
            "current_price",
            "long_maxpain_price",
            "short_maxpain_price",
            "long_score",
            "short_score",
            "selected_side",
            "selected_score",
            "opposite_score",
            "score_edge",
            "score_ratio",
            "long_liquidity_usd",
            "short_liquidity_usd",
            "selected_liquidity_usd",
            "opposite_liquidity_usd",
            "gap_score",
            "proximity_score",
            "cluster_score",
            "quality_status",
            "calculation_version",
            "event_id",
            "timestamp_utc",
            "source_side",
            "score_direction_basis",
            "consensus_score",
            "components_json",
            "is_alert_timeframe",
            "target_distance_pct",
            "selected_liquidity_share_pct",
            "consensus_hits",
            "consensus_total",
            "source_record_type"
        ],
        "max_rows": 176,
        "identity": "event_id",
        "symbol": "symbol",
        "side": "source_side"
    }
}
SHEETS = tuple(config['sheet'] for config in CONFIG.values())
BY_SHEET = {config['sheet']: config for config in CONFIG.values()}


def project(item: Mapping[str, Any], *, snapshot_time: Any = None) -> dict[str, Any]:
    config = CONFIG.get(str(item.get('sheet'))) or BY_SHEET.get(str(item.get('sheet')))
    if config is None or not isinstance(item.get('row'), Mapping):
        raise ValueError('INVALID_CURRENT_PUBLICATION_SOURCE')
    raw = dict(item['row'])
    if config['sheet'] == 'Live_Current':
        raw['timestamp_utc'] = raw.get('timestamp_utc') or snapshot_time or item.get('source_time_utc')
    timestamp = source_time(raw.get('timestamp_utc'))
    if (raw.get(config['symbol']) not in SYMBOLS or raw.get(config['side']) not in DIRECTIONS
            or not isinstance(raw.get(config['identity']), str) or not raw[config['identity']]
            or timestamp is None or set(raw) - set(config['headers'])
            or (config['sheet'] == 'MaxPain_Current' and raw.get('timeframe') not in TIMEFRAMES)):
        raise ValueError('INVALID_CURRENT_PUBLICATION_SOURCE')
    raw['timestamp_utc'] = timestamp.isoformat()
    row = {name: raw.get(name) for name in config['headers']}
    return {'sheet': config['sheet'], 'key': config['key'], 'row': row,
            'source_time_utc': timestamp.isoformat(), 'source_id': raw[config['identity']]}


def stage_projected(conn: Any, item: Mapping[str, Any]) -> int:
    item = project(item)
    config = BY_SHEET[item['sheet']]
    payload = json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(',', ':'), default=str, allow_nan=False)
    row_key = json.dumps([str(item['row'][name]) for name in config['key'].split(',')],
                         ensure_ascii=False, separators=(',', ':'))
    sha = hashlib.sha256(payload.encode()).hexdigest()
    accepted = conn.execute('''INSERT INTO research_sheet_upsert_outbox
        (sheet_name,row_key,payload,payload_sha256)
        VALUES(%s,%s,%s::jsonb,%s)
        ON CONFLICT(sheet_name,row_key) DO UPDATE SET
            payload=EXCLUDED.payload,payload_sha256=EXCLUDED.payload_sha256,
            sync_status='PENDING',attempts=0,next_attempt_at_utc=NOW(),
            claim_token=NULL,claimed_payload_sha256=NULL,lease_expires_at_utc=NULL,
            synced_at_utc=NULL,last_error=NULL,updated_at_utc=NOW()
        WHERE research_sheet_upsert_outbox.payload_sha256 IS DISTINCT FROM EXCLUDED.payload_sha256
          AND (
            research_sheet_source_timestamp(EXCLUDED.payload->>'source_time_utc'),
            EXCLUDED.payload->>'source_id'
          ) >= (
            research_sheet_source_timestamp(research_sheet_upsert_outbox.payload->>'source_time_utc'),
            research_sheet_upsert_outbox.payload->>'source_id'
          )
        RETURNING row_key''', (item['sheet'], row_key, payload, sha)).fetchone()
    if not accepted:
        return 0
    conn.execute('''UPDATE research_sheet_upsert_outbox SET source_time_utc=%s
        WHERE sheet_name=%s AND row_key=%s AND payload_sha256=%s''',
        (source_time(item['source_time_utc']), item['sheet'], row_key, sha))
    return 1


def seed_legacy(conn: Any) -> int:
    """One bounded bootstrap per process, from committed legacy exports only.

    No source marker reset or replay of the 14-day event population. The query
    stays in each indexed legacy lane. Newer current slots win atomically even
    when seeding overlaps a live source transaction. Legacy ACKs are untouched.
    """
    count = 0
    for legacy in ('Snapshots', 'MaxPain_TF'):
        config = CONFIG[legacy]
        names = config['key'].split(',')
        # Fixed contract identifiers only, never caller-provided SQL fragments.
        keys = ','.join("payload->'row'->>'" + name + "'" for name in names)
        rows = conn.execute(f'''WITH newest AS MATERIALIZED (
            SELECT DISTINCT ON ({keys}) row_key
            FROM research_sheet_upsert_outbox
            WHERE sheet_name=%s AND source_time_utc IS NOT NULL
              AND payload->'row'->>%s=ANY(%s) AND payload->'row'->>%s=ANY(%s)
            ORDER BY {keys},source_time_utc DESC,payload->'row'->>%s DESC
            LIMIT %s)
            SELECT queued.payload FROM newest
            JOIN research_sheet_upsert_outbox queued ON queued.sheet_name=%s
              AND queued.row_key=newest.row_key''',
            (legacy, config['symbol'], list(SYMBOLS), config['side'],
             list(DIRECTIONS), config['identity'], config['max_rows'], legacy)).fetchall()
        for stored in rows:
            original = stored['payload']
            item = project(original)
            count += stage_projected(conn, item)
            if legacy == 'Snapshots':
                old_key = json.dumps([original['row']['snapshot_id']], separators=(',', ':'))
                live = conn.execute('''SELECT payload FROM research_sheet_upsert_outbox
                    WHERE sheet_name=%s AND row_key=%s''', ('תצוגת לייב', old_key)).fetchone()
                if live:
                    count += stage_projected(conn, project(live['payload'], snapshot_time=item['source_time_utc']))
    return count


def status() -> dict[str, Any]:
    return {'policy_version': VERSION, 'destinations': {
        name: {'max_data_rows': config['max_rows'], 'max_columns': len(config['headers']),
               'key': config['key']} for name, config in BY_SHEET.items()},
        'selection': 'Latest frozen source timestamp then source identity per slot',
        'interpretation': 'Slots may refer to different source events; not a statistical cohort',
        'legacy_destinations': list(CONFIG), 'legacy_publication': 'HELD; queue states and sheet history retained',
        'telegram': {'destination': 'Telegram_Events', 'max_data_rows': TELEGRAM_MAX_ROWS,
                     'protected_days': TELEGRAM_RETENTION_DAYS, 'max_columns': 15,
                     'policy': 'Reuse only expired physical rows at capacity; no row shifting',
                     'expired_pending': 'HELD_IN_DATABASE_NOT_ACKNOWLEDGED'},
        'audit': {'window_days': 14, 'max_cycle_seconds': AUDIT_MAX_SECONDS},
        'canonical_research': 'FULL_DATABASE_EVIDENCE_RETAINED', 'research_effect': 'NONE'}
