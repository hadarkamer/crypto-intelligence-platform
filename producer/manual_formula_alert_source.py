"""Read-only, fresh native-event features for the four owner-requested alerts.

This lane reads immutable source captures, never the ordered-research backlog,
outcomes, qualification tables or a latest-market lookup.  The caller owns the
transaction and durable processed-ID receipts.  A fresh overlapping tail avoids
losing a smaller serial ID whose source transaction committed late.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import research_formula_ordered_v7 as totals
import research_ordered_question_catalog as questions

MAX_BATCH = 32
MAX_PROCESSED_IDS = 8192
MAX_SEQUENCE_ROWS = 4096
MAX_WATCH_EVENTS = 256
SOURCE_TTL = timedelta(minutes=10)

# Preserve actual provenance keys, including explicit JSON nulls.  Synthesizing
# an absent inverse_analysis key would misclassify every native event as derived.
_SNAPSHOT_KEYS = (
    'watch_scan_id', 'sheet_snapshot_id', 'alert_side',
    'experimental_price_references',
    'calculation_validation_errors', 'consensus_hits', 'consensus_total',
    'magnet', 'magnet_confirmation', 'market_evidence', 'inverse_analysis', 'record_mode',
    'source_scope', 'source_kind', 'data_mode', 'state', 'mode',
    'archive_import', 'archive_reconstruction', 'telegram_archive_import',
    'archive_only', 'telegram_archive', 'archive_run_key',
)

_CURRENT_SQL = '''WITH picked AS MATERIALIZED (
    SELECT e.event_id FROM research_events e
    WHERE e.alert_time_utc >= %s AND e.alert_time_utc <= %s
      AND e.event_kind='ALERT' AND e.delivery_status='DELIVERED'
      AND e.direction IN ('LONG','SHORT')
      AND NOT (e.event_id=ANY(%s::bigint[]))
      AND (%s::bigint[] IS NULL OR e.event_id=ANY(%s::bigint[]))
    ORDER BY e.event_id LIMIT %s
)
SELECT e.event_id,e.symbol,e.direction,e.alert_time_utc,e.event_fingerprint,
       e.current_price,e.event_type,e.event_kind,e.delivery_status,e.score,
       e.source_side,e.target_price,e.timeframe,e.strategy_version,
       e.code_version,e.capture_stage,e.runtime_session_id,
       projected.snapshot AS engine_snapshot
FROM picked JOIN research_events e USING(event_id)
CROSS JOIN LATERAL (
  SELECT COALESCE(jsonb_object_agg(field.key,
    CASE WHEN field.key='market_evidence' THEN
      jsonb_build_object('modules',jsonb_build_object(
        'positioning',jsonb_build_object(
          'score',field.value#>'{modules,positioning,score}',
          'direction',field.value#>'{modules,positioning,direction}'),
        'futures_flow',jsonb_build_object(
          'score',field.value#>'{modules,futures_flow,score}',
          'direction',field.value#>'{modules,futures_flow,direction}',
          'available',field.value#>'{modules,futures_flow,available}',
          'time_families',jsonb_build_object('long',jsonb_build_object(
            'direction',field.value#>'{modules,futures_flow,time_families,long,direction}',
            'quality',field.value#>'{modules,futures_flow,time_families,long,quality}'))),
        'spot_flow',jsonb_build_object(
          'score',field.value#>'{modules,spot_flow,score}',
          'direction',field.value#>'{modules,spot_flow,direction}')))
    ELSE field.value END),'{}'::jsonb) AS snapshot
  FROM jsonb_each(CASE WHEN jsonb_typeof(e.engine_snapshot)='object'
    THEN e.engine_snapshot ELSE '{}'::jsonb END) field(key,value)
  WHERE field.key=ANY(%s::text[])
) projected
ORDER BY e.event_id'''


def _utc(value):
    result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('Manual alert source timestamps require a timezone')
    return result.astimezone(timezone.utc)


def _sequence_history(conn, candidates):
    """Complete 30m causal union or an explicit overflow, never a truncated count."""
    if not candidates:
        return [], False
    windows = sorted({(event['symbol'], event['direction'],
                       _utc(event['alert_time_utc']) - timedelta(minutes=30),
                       _utc(event['alert_time_utc'])) for event in candidates})
    values = ','.join(['(%s,%s,%s,%s)'] * len(windows))
    sql = '''WITH wanted(symbol,direction,lower_utc,upper_utc) AS (VALUES ''' + values + ''')
      SELECT e.event_id,e.symbol,e.direction,e.alert_time_utc,e.event_type,
             e.current_price,
             jsonb_build_object(
               'watch_scan_id',e.engine_snapshot->'watch_scan_id',
               'sheet_snapshot_id',e.engine_snapshot->'sheet_snapshot_id',
               'market_evidence',jsonb_build_object('modules',jsonb_build_object(
                 'positioning',jsonb_build_object(
                   'score',e.engine_snapshot#>'{market_evidence,modules,positioning,score}',
                   'direction',e.engine_snapshot#>'{market_evidence,modules,positioning,direction}'))))
               AS engine_snapshot
      FROM research_events e
      WHERE e.event_kind='ALERT' AND e.delivery_status='DELIVERED'
        AND e.direction IN ('LONG','SHORT')
        AND EXISTS (SELECT 1 FROM wanted w WHERE e.symbol=w.symbol AND e.direction=w.direction
          AND e.alert_time_utc>=w.lower_utc AND e.alert_time_utc<w.upper_utc)
        AND (NULLIF(e.engine_snapshot->>'watch_scan_id','') IS NOT NULL
          OR NULLIF(e.engine_snapshot->>'sheet_snapshot_id','') IS NOT NULL)
      ORDER BY e.event_id LIMIT %s'''
    params = tuple(value for interval in windows for value in interval) + (MAX_SEQUENCE_ROWS + 1,)
    rows = conn.execute(sql, params).fetchall()
    if len(rows) > MAX_SEQUENCE_ROWS:
        return [], True
    return [(row, totals.extract_event_features(row)) for row in rows], False


def prepare_watch_pairs(conn, events, now):
    """Use exact live planned native captures plus strictly earlier evidence.

    Planned sources have an explicit non-database identity; they are never
    inserted into the delivered research population. Equal timestamps do not
    create a sequence entry, as in the existing research sequence contract.
    """
    import manual_formula_alert as rules
    if not isinstance(events, (list, tuple)) or len(events) > MAX_WATCH_EVENTS:
        raise ValueError('Invalid or oversized planned Watch batch')
    pairs, seen = [], set()
    for event in events:
        features = totals.extract_event_features(event)
        features.update(questions.extended_features(event))
        if not rules.source_is_eligible(event, features, now, planned=True):
            continue
        if event['event_id'] in seen:
            continue
        seen.add(event['event_id'])
        pairs.append((event, features))
    stats = {'selected_events': len(pairs), 'source_lane': 'WATCH_PLANNED',
             'sequence_source_rows': 0, 'sequence_error_type': None,
             'sequence_lookup_attempts': 0, 'sequence_retry_recovered': False}
    candidates = [event for event, features in pairs
                  if rules._score65(features, 'price_oi')
                  and event['symbol'] in rules.RULES['PRICE_OI_ENTRY2']['symbols']]
    if not candidates:
        return pairs, stats
    # One immediate retry in a fresh savepoint preserves priority on a transient
    # lookup error. Persistent failures retain an explicit native ENTRY2-only
    # recovery lane in the outbox; other rules remain usable immediately.
    for attempt in range(2):
        stats['sequence_lookup_attempts'] += 1
        try:
            with conn.transaction():
                history, overflow = _sequence_history(conn, candidates)
            stats['sequence_retry_recovered'] = bool(attempt and not overflow)
            break
        except Exception as exc:
            if not attempt:
                stats['sequence_first_error_type'] = type(exc).__name__
                continue
            stats['sequence_error_type'] = type(exc).__name__
            for _, features in pairs:
                features['sequence.capture_status'] = 'SOURCE_UNAVAILABLE'
            return pairs, stats
    if overflow:
        for _, features in pairs:
            features['sequence.capture_status'] = 'SOURCE_OVERFLOW'
        return pairs, stats
    stats['sequence_source_rows'] = len(history)
    # Include earlier captured siblings of this same scan, never later ones.
    # The shared sequence function deduplicates real scan IDs and excludes ties.
    for event, features in pairs:
        features.update(questions.sequence_features(event, features, history + pairs))
    return pairs, stats


def load_batch(conn, *, activated_at, now, processed_ids, limit=MAX_BATCH, retry_event_ids=None):
    """Return ``([(source_event, frozen_features)], statistics)``; perform no writes.

    ``processed_ids`` is the caller's durable, bounded receipt set, not a serial
    high-water mark.  ``retry_event_ids`` selects an exact, separately budgeted
    sequence retry lane capped at eight IDs; it never reopens completed source
    IDs implicitly.  The lower time fence and overlap permit late source commits
    without replaying a completed event.  An unavailable/oversized sequence only
    disables ENTRY2 for this batch, leaving the three non-sequence rules usable.
    """
    moment, activation = _utc(now), _utc(activated_at)
    if isinstance(limit, bool) or not 1 <= int(limit) <= MAX_BATCH:
        raise ValueError('Manual alert source batch limit must be between 1 and 32')
    ids = list(processed_ids)
    if len(ids) > MAX_PROCESSED_IDS or any(type(value) is not int or value <= 0 for value in ids):
        raise ValueError('Invalid or oversized processed source receipt set')
    retry = None if retry_event_ids is None else list(retry_event_ids)
    if retry is not None:
        if len(retry) > 8 or any(type(value) is not int or value <= 0 for value in retry):
            raise ValueError('Invalid or oversized source retry lane')
        retry = sorted(set(retry))
        limit = min(int(limit), 8)
    lower = max(activation, moment - SOURCE_TTL)
    stats = {'selected_events': 0, 'sequence_source_rows': 0,
             'sequence_overflow_events': 0, 'sequence_error_type': None,
             'source_lane': 'RETRY' if retry is not None else 'FRESH',
             'source_from_utc': lower.isoformat(), 'source_through_utc': moment.isoformat()}
    if lower > moment or retry == []:
        return [], stats
    rows = conn.execute(_CURRENT_SQL, (lower, moment, sorted(set(ids)), retry, retry, int(limit), list(_SNAPSHOT_KEYS))).fetchall()
    selected = [int(row['event_id']) for row in rows]
    if (len(rows) > int(limit) or selected != sorted(set(selected)) or set(selected).intersection(ids)
            or (retry is not None and set(selected).difference(retry))):
        raise RuntimeError('Fresh source projection lost its finite receipt contract')
    pairs = []
    for row in rows:
        if (not lower <= _utc(row['alert_time_utc']) <= moment
                or row.get('event_kind') != 'ALERT' or row.get('delivery_status') != 'DELIVERED'
                or row.get('direction') not in ('LONG', 'SHORT')):
            raise RuntimeError('Fresh source projection changed its native time/delivery contract')
        features = totals.extract_event_features(row)
        features.update(questions.extended_features(row))
        pairs.append((row, features))
    stats['selected_events'] = len(pairs)
    candidates = [event for event, features in pairs
                  if questions.number(features.get('price_oi.aligned_score')) is not None
                  and features['price_oi.aligned_score'] >= 65]
    if not candidates:
        return pairs, stats
    try:
        # Nested psycopg transaction = savepoint; a sequence statement timeout
        # does not poison the caller's transaction or erase other rule matches.
        with conn.transaction():
            history, overflow = _sequence_history(conn, candidates)
    except Exception as exc:
        stats['sequence_error_type'] = type(exc).__name__
        for _, features in pairs:
            features['sequence.capture_status'] = 'SOURCE_UNAVAILABLE'
        return pairs, stats
    stats['sequence_source_rows'] = len(history)
    if overflow:
        stats['sequence_overflow_events'] = len(candidates)
        for _, features in pairs:
            features['sequence.capture_status'] = 'SOURCE_OVERFLOW'
        return pairs, stats
    wanted = {event['event_id'] for event in candidates}
    for event, features in pairs:
        if event['event_id'] in wanted:
            features.update(questions.sequence_features(event, features, history))
    return pairs, stats
