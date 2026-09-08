"""Bounded durable priority lane; canonical writers remain authoritative."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterable, Mapping, Any

from research_common_window_metrics import WINDOWS, utc, METHOD_VERSION as COMMON_VERSION

V7 = 'ordered-first-touch-v7'
THRESHOLDS = (25, 50, 75, 100, 125, 150, 175, 200)


def available(conn) -> bool:
    row = conn.execute("SELECT to_regclass('research_ordered_outcome_recovery') IS NOT NULL "
        "AND to_regclass('research_ordered_outcome_recovery_state') IS NOT NULL AS available").fetchone()
    return bool(row and row['available'])


def normalize_ids(values: Iterable[int]) -> list[int]:
    ids = sorted(set(int(value) for value in values))
    if not ids or len(ids)>1000 or ids[0]<=0:
        raise ValueError('Recovery requires 1..1000 positive archived event IDs')
    return ids


def enqueue(conn, event_ids: Iterable[int], *, as_of: datetime, request_key: str, priority: int=1) -> int:
    """Snapshot a finite request; concurrent extensions cannot be ACKed early.

    Repeating the same request never resets attempts or reopens a finished or
    blocked request. A later explicit cutoff may request another check, while
    immutable source provenance is still checked by the normal worker.
    """
    ids, cutoff = normalize_ids(event_ids), utc(as_of)
    if not str(request_key).strip() or len(str(request_key))>200:
        raise ValueError('Recovery request_key must contain 1..200 characters')
    if type(priority) is not int or priority not in (0,1,2):
        raise ValueError('Recovery priority must be 0,1,2')
    sources = conn.execute("""SELECT event_id FROM research_events
        WHERE event_id=ANY(%s::bigint[]) AND event_kind='ALERT'
          AND delivery_status='DELIVERED' AND direction IN ('LONG','SHORT')
          AND alert_time_utc<=%s ORDER BY event_id""", (ids,cutoff)).fetchall()
    if [int(row['event_id']) for row in sources] != ids:
        raise ValueError('Every recovery ID must be a delivered native directional alert before the cutoff')
    return conn.execute("""INSERT INTO research_ordered_outcome_recovery
        (event_id,request_key,requested_through_utc,priority)
        SELECT id,%s,%s,%s FROM unnest(%s::bigint[]) id
        ON CONFLICT(event_id) DO UPDATE SET
          request_key=EXCLUDED.request_key,
          requested_through_utc=GREATEST(research_ordered_outcome_recovery.requested_through_utc,EXCLUDED.requested_through_utc),
          priority=GREATEST(research_ordered_outcome_recovery.priority,EXCLUDED.priority),
          status='PENDING',next_attempt_at_utc=NOW(),last_error=NULL,updated_at_utc=NOW()
        WHERE research_ordered_outcome_recovery.requested_through_utc<EXCLUDED.requested_through_utc
           OR (research_ordered_outcome_recovery.priority<EXCLUDED.priority
               AND research_ordered_outcome_recovery.status IN ('PENDING','OPEN','RETRY','DERIVED_OPEN'))
        """, (str(request_key),cutoff,priority,ids)).rowcount


def intake(conn, *, now: datetime, backfill_days: int, limit: int=128) -> int:
    """An indexed finite source page, independent of the general latest-128 tail.

    Selection and enqueue commit together. Each ID enters automatically once;
    OPEN requests subsequently refresh themselves until their horizons mature.
    """
    state = conn.execute("""SELECT last_source_event_id,high_water_event_id FROM
        research_ordered_outcome_recovery_state WHERE singleton FOR UPDATE""").fetchone()
    cursor,high_water=int(state['last_source_event_id']),int(state['high_water_event_id'])
    if cursor>=high_water:
        high=conn.execute("""SELECT event_id FROM research_events
            WHERE event_kind='ALERT' AND delivery_status='DELIVERED'
              AND event_type='MAX_PAIN_ALERT' AND score>=65 AND direction IN ('LONG','SHORT')
            ORDER BY event_id DESC LIMIT 1""").fetchone()
        cursor,high_water=0,int(high['event_id']) if high else 0
        conn.execute("""UPDATE research_ordered_outcome_recovery_state
            SET last_source_event_id=0,high_water_event_id=%s WHERE singleton""",(high_water,))
    rows = conn.execute("""SELECT event_id FROM research_events
        WHERE event_id>%s AND event_id<=%s AND event_kind='ALERT' AND delivery_status='DELIVERED'
          AND event_type='MAX_PAIN_ALERT' AND score>=65 AND direction IN ('LONG','SHORT')
          AND alert_time_utc>=%s AND alert_time_utc<=%s
        ORDER BY event_id LIMIT %s""", (cursor,high_water,
        utc(now)-timedelta(days=max(1,min(int(backfill_days),90))),utc(now),
        max(1,min(int(limit),256)))).fetchall()
    if not rows:
        conn.execute("""UPDATE research_ordered_outcome_recovery_state
            SET last_source_event_id=high_water_event_id WHERE singleton""")
        return 0
    ids=[row['event_id'] for row in rows]
    existing={row['event_id'] for row in conn.execute("""SELECT event_id
        FROM research_ordered_outcome_recovery WHERE event_id=ANY(%s::bigint[])""",(ids,)).fetchall()}
    missing=[event_id for event_id in ids if event_id not in existing]
    changed = enqueue(conn,missing,as_of=now,
        request_key='automatic-max-pain-alert-score-ge65',priority=0) if missing else 0
    conn.execute("""UPDATE research_ordered_outcome_recovery_state
        SET last_source_event_id=%s WHERE singleton""",(rows[-1]['event_id'],))
    return changed


def load_due(conn, *, limit: int) -> list[dict[str,Any]]:
    if limit<=0:
        return []
    return [dict(row) for row in conn.execute("""WITH picked AS MATERIALIZED (
        SELECT event_id,requested_through_utc,next_attempt_at_utc,priority
        FROM research_ordered_outcome_recovery
        WHERE status IN ('PENDING','OPEN','RETRY','DERIVED_OPEN') AND next_attempt_at_utc<=NOW()
        ORDER BY priority DESC,next_attempt_at_utc,event_id LIMIT %s)
        SELECT e.*,picked.requested_through_utc AS _recovery_requested_through
        FROM picked JOIN research_events e USING(event_id)
        ORDER BY picked.priority DESC,picked.next_attempt_at_utc,e.event_id""",(min(int(limit),32),)).fetchall()]


def next_requested_budget(conn, limit: int) -> int:
    """Reserve ordinary slots, including fair turns with a one-event budget."""
    cap = max(1,min(int(limit),64))
    if cap>1:
        return min(32,cap//2)
    row=conn.execute("""UPDATE research_ordered_outcome_recovery_state
        SET next_lane=1-next_lane WHERE singleton RETURNING next_lane""").fetchone()
    return int(row['next_lane'])


def combine(requested: list[dict], ordinary: list[dict], *, limit: int) -> list[dict]:
    """Metadata cursors saw only the ordinary capacity actually reserved."""
    result, seen = [], set()
    for index in range(max(len(requested),len(ordinary))):
        for lane in (requested,ordinary):
            if index<len(lane) and lane[index]['event_id'] not in seen:
                seen.add(lane[index]['event_id'])
                result.append(lane[index])
    # A duplicate selected by the ordinary lane must retain the request token.
    tokens={row['event_id']:row['_recovery_requested_through'] for row in requested}
    return [{**row,**({'_recovery_requested_through':tokens[row['event_id']]}
        if row['event_id'] in tokens else {})} for row in result[:limit]]


def finish_error(conn, event: Mapping, *, error: str, blocked: bool=False) -> bool:
    token=event.get('_recovery_requested_through')
    if token is None:
        return False
    row=conn.execute("""UPDATE research_ordered_outcome_recovery
        SET status=%s,attempts=attempts+1,last_error=%s,
          next_attempt_at_utc=NOW()+INTERVAL '15 minutes',updated_at_utc=NOW()
        WHERE event_id=%s AND requested_through_utc=%s RETURNING event_id""",
        ('BLOCKED' if blocked else 'RETRY',str(error)[:1000],int(event['event_id']),token)).fetchone()
    return bool(row)


def coverage_state(event: Mapping, outcomes: Iterable[Mapping], common: Iterable[Mapping],
                   *, now: datetime) -> str:
    """Check the entire 32+4 grid; terminal v7 evidence stops at first touch."""
    start, clock = utc(event['alert_time_utc']), utc(now)
    expected = {(window,bps) for window in WINDOWS for bps in THRESHOLDS}
    found={}
    for row in outcomes:
        if row.get('method_version')!=V7:
            continue
        found[(int(row['window_minutes']),int(row['threshold_bps']))]=row
    metrics={int(row['window_minutes']):row for row in common if row.get('method_version')==COMMON_VERSION}
    if set(found)!=expected or set(metrics)!=set(WINDOWS):
        return 'RETRY'
    ready=True
    for window in WINDOWS:
        end=start+timedelta(minutes=window)
        closed_cutoff=clock.replace(second=0,microsecond=0)-timedelta(milliseconds=1)
        cutoff=(min(end,closed_cutoff)+timedelta(milliseconds=1)).replace(second=0,microsecond=0)-timedelta(milliseconds=1)
        metric=metrics[window]
        result=metric.get('result') or {}
        observed=result.get('observed_through_utc')
        if (metric['status'] not in ('READY','OPEN') or not result.get('observed_prefix_complete')
                or not observed or utc(observed)<cutoff):
            return 'RETRY'
        ready=ready and metric['status']=='READY'
        for bps in THRESHOLDS:
            row=found[(window,bps)]
            if not row.get('path_complete') or row['status']=='DATA_MISSING':
                return 'RETRY'
            if row['status']=='OPEN' and (not row.get('observed_through_utc') or utc(row['observed_through_utc'])<cutoff):
                return 'RETRY'
            if row['status']=='OPEN' and clock>=end:
                return 'RETRY'
            if row['status'] not in ('OPEN','SUCCESS','FAILURE','UNRESOLVED'):
                return 'RETRY'
    return 'COMPLETE' if ready else 'OPEN'


def finish_success(conn, event: Mapping, *, now: datetime) -> str | None:
    token=event.get('_recovery_requested_through')
    if token is None:
        return None
    event_id=int(event['event_id'])
    outcomes=conn.execute("""SELECT window_minutes,threshold_bps,method_version,status,
        path_complete,observed_through_utc FROM research_ordered_first_touch_outcomes
        WHERE event_id=%s AND method_version=%s""",(event_id,V7)).fetchall()
    common=conn.execute("""SELECT window_minutes,method_version,status,result
        FROM research_common_window_metrics WHERE event_id=%s AND method_version=%s""",
        (event_id,COMMON_VERSION)).fetchall()
    state=coverage_state(event,outcomes,common,now=now)
    error='Canonical 32-label / 4-window coverage is incomplete' if state=='RETRY' else None
    row=conn.execute("""UPDATE research_ordered_outcome_recovery SET status=%s,
        attempts=attempts+1,last_error=%s,sheet_pending=TRUE,
        refreshed_through_utc=CASE WHEN %s IN ('OPEN','COMPLETE') THEN %s ELSE refreshed_through_utc END,
        next_attempt_at_utc=NOW()+INTERVAL '15 minutes',updated_at_utc=NOW()
        WHERE event_id=%s AND requested_through_utc=%s RETURNING event_id""",
        (state,error,state,utc(now),event_id,token)).fetchone()
    return state if row else None


def finish_derived(conn,event: Mapping,*,result: Mapping,now: datetime) -> str | None:
    """A MARK supplement never ACKs the canonical Spot evidence grid."""
    event_id=int(event['event_id'])
    measurements=[row for row in result.get('measurements',[]) if int(row.get('event_id',0))==event_id]
    if not measurements:
        error='; '.join(str(row.get('error','')) for row in result.get('errors',[])) or 'HYPE supplement returned no measurement'
        finish_error(conn,event,error=error)
        return 'RETRY'
    measurement=measurements[0]
    if (measurement.get('source_scope')!='DERIVED_NATIVE_HYPE_MARK'
            or measurement.get('live_union_eligible') is not False):
        finish_error(conn,event,error='HYPE supplement source contract mismatch',blocked=True)
        return 'BLOCKED'
    state='SUPPLEMENTED' if measurement.get('complete') is True else 'DERIVED_OPEN'
    measured_at=utc(measurement['observed_at_utc'])
    if state=='SUPPLEMENTED' and (measurement.get('calculation_status')!='COMPLETE_32_LABELS'
            or sorted(measurement.get('ready_windows',[]))!=list(WINDOWS)):
        state='RETRY'
    if not measurement.get('current_prefix_complete'):
        state='RETRY'
    if (state=='DERIVED_OPEN' and measured_at.replace(second=0,microsecond=0)
            < utc(now).replace(second=0,microsecond=0)):
        state='RETRY'
    row=conn.execute("""UPDATE research_ordered_outcome_recovery SET status=%s,
        attempts=attempts+1,
        refreshed_through_utc=CASE WHEN %s IN ('DERIVED_OPEN','SUPPLEMENTED') THEN %s ELSE refreshed_through_utc END,
        derivation_reference=%s,last_error=%s,
        next_attempt_at_utc=NOW()+INTERVAL '15 minutes',updated_at_utc=NOW()
        WHERE event_id=%s AND requested_through_utc=%s RETURNING event_id""",
        (state,state,measured_at,'DERIVED_NATIVE_HYPE_MARK:'+str(event_id),
         'Native Spot provenance unavailable; isolated MARK supplement only' if state!='RETRY'
         else 'Isolated MARK supplement path is incomplete',event_id,event['_recovery_requested_through'])).fetchone()
    return state if row else None


def delivery_ids(conn, *, limit: int=16) -> list[int]:
    """Clear ACKed requests, then return a finite remaining event prefix."""
    rows=conn.execute("""SELECT event_id FROM research_ordered_outcome_recovery
        WHERE sheet_pending ORDER BY priority DESC,updated_at_utc,event_id LIMIT %s""",
        (max(1,min(int(limit),32)),)).fetchall()
    ids=[int(row['event_id']) for row in rows]
    if not ids:
        return []
    synced={int(row['event_id']) for row in conn.execute("""SELECT event_id
        FROM research_ordered_first_touch_sync_outbox
        WHERE event_id=ANY(%s::bigint[]) AND method_version=%s AND destination='GOOGLE_SHEETS'
        GROUP BY event_id HAVING COUNT(*)=32 AND BOOL_AND(sync_status='SYNCED')""",(ids,V7)).fetchall()}
    if synced:
        conn.execute("""UPDATE research_ordered_outcome_recovery SET sheet_pending=FALSE
            WHERE event_id=ANY(%s::bigint[])""",(sorted(synced),))
    return [event_id for event_id in ids if event_id not in synced]


def delivery_budget(conn,limit: int) -> int:
    if limit>1:
        return limit//2
    row=conn.execute("""UPDATE research_ordered_outcome_recovery_state
        SET next_delivery_lane=1-next_delivery_lane WHERE singleton RETURNING next_delivery_lane""").fetchone()
    return int(row['next_delivery_lane'])
