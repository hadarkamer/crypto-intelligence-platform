"""Bounded pre-entry enrichment queue and explicit stale-search refresh."""
from __future__ import annotations

from datetime import timedelta
import json
from typing import Any, Iterable, Mapping
from research_past_price_features import METHOD_VERSION
import research_past_price_features as features


def available(conn) -> bool:
    row=conn.execute("SELECT to_regclass('research_past_price_features') IS NOT NULL AS available").fetchone()
    return bool(row and row["available"])


def seed_events(conn,events:Iterable[Mapping[str,Any]]) -> int:
    rows=[{"event_id":int(e["event_id"]),"event_time_utc":str(e["alert_time_utc"])} for e in events]
    if not rows:return 0
    return conn.execute("""INSERT INTO research_past_price_features(event_id,method_version,event_time_utc,status)
        SELECT event_id,%s,event_time_utc,'PENDING' FROM jsonb_to_recordset(%s::jsonb)
            AS item(event_id BIGINT,event_time_utc TIMESTAMPTZ)
        ON CONFLICT DO NOTHING""",(METHOD_VERSION,json.dumps(rows))).rowcount


def seed_bounded_history(conn,*,limit:int=128) -> int:
    conn.execute("INSERT INTO research_past_price_features_cursor(method_version) VALUES(%s) ON CONFLICT DO NOTHING",(METHOD_VERSION,))
    cursor=conn.execute("SELECT last_event_id FROM research_past_price_features_cursor WHERE method_version=%s FOR UPDATE",(METHOD_VERSION,)).fetchone()
    rows=conn.execute("""SELECT event_id,alert_time_utc FROM research_events
        WHERE event_id>%s AND event_kind='ALERT' AND delivery_status='DELIVERED'
          AND direction IN ('LONG','SHORT') AND alert_time_utc>='2026-08-15T21:00:00Z'::timestamptz
          AND alert_time_utc<=NOW()
        ORDER BY event_id LIMIT %s""",(cursor['last_event_id'],max(1,min(int(limit),256)))).fetchall()
    conn.execute("UPDATE research_past_price_features_cursor SET last_event_id=%s WHERE method_version=%s",(rows[-1]['event_id'] if rows else 0,METHOD_VERSION))
    return seed_events(conn,rows)


def load_due_event(conn) -> dict[str,Any]|None:
    cursor=conn.execute("SELECT next_lane FROM research_past_price_features_cursor WHERE method_version=%s FOR UPDATE",(METHOD_VERSION,)).fetchone()
    lane=int(cursor['next_lane'])
    ordering='event_time_utc DESC,next_attempt_at_utc,event_id' if lane==0 else 'next_attempt_at_utc,event_id'
    row=conn.execute("""WITH picked AS MATERIALIZED (
        SELECT event_id FROM research_past_price_features
        WHERE method_version=%s AND status IN ('PENDING','PARTIAL','DATA_MISSING')
            AND next_attempt_at_utc<=NOW()
        ORDER BY """+ordering+""" LIMIT 1)
        SELECT e.event_id,e.symbol,e.direction,e.alert_time_utc,e.event_kind,e.delivery_status
        FROM picked JOIN research_events e USING(event_id)""",(METHOD_VERSION,)).fetchone()
    if row:conn.execute("UPDATE research_past_price_features_cursor SET next_lane=%s WHERE method_version=%s",(1-lane,METHOD_VERSION))
    return dict(row) if row else None


def seed_recent_events(conn,*,limit:int=16) -> int:
    rows=conn.execute("""SELECT event_id,alert_time_utc FROM research_events
        WHERE event_kind='ALERT' AND delivery_status='DELIVERED'
          AND direction IN ('LONG','SHORT') AND alert_time_utc>='2026-08-15T21:00:00Z'::timestamptz
          AND alert_time_utc<=NOW()
        ORDER BY alert_time_utc DESC,event_id DESC LIMIT %s""",(max(1,min(int(limit),32)),)).fetchall()
    return seed_events(conn,rows)


def write_result(conn,*,event_id:int,result:Mapping[str,Any]) -> bool:
    if result.get('method_version')!=METHOD_VERSION:raise ValueError('Wrong past feature version')
    existing=conn.execute("SELECT status,event_time_utc,result FROM research_past_price_features WHERE event_id=%s AND method_version=%s FOR UPDATE",(event_id,METHOD_VERSION)).fetchone()
    if not existing or existing['status']=='READY':return False
    if features.utc(existing['event_time_utc'])!=features.utc(result['event_time_utc']):
        raise ValueError('Immutable past-feature event timestamp changed')
    old=existing['result'] or {}
    if old.get('computed_at_utc') and features.utc(old['computed_at_utc'])>features.utc(result['computed_at_utc']):
        return False
    # A transient error cannot erase a previously verified lookback or mutate
    # a predicate already used by a discovery trial. Complete missing windows
    # only; new calculation rules require another method version.
    windows={}
    for name,_ in features.LOOKBACKS:
        cell=dict(result['windows'][name])
        previous=(old.get('windows') or {}).get(name) or {}
        for side in ('asset','btc'):
            if (previous.get(side) or {}).get('status')=='READY':
                cell[side]=previous[side]
        cell['relative_strength_pct']=None
        cell['relative_strength_direction']=None
        if cell['asset']['status']==cell['btc']['status']=='READY':
            relative=cell['asset']['return_pct']-cell['btc']['return_pct']
            cell.update(relative_strength_pct=relative,relative_strength_direction=features.direction(relative))
        windows[name]=cell
    result={**features.record_from_windows(symbol=result['symbol'],event_time=result['event_time_utc'],
        windows=windows,computed_at=result['computed_at_utc']),'fetch_errors':result.get('fetch_errors') or {}}
    row=conn.execute("""UPDATE research_past_price_features SET status=%s,result=%s::jsonb,
        search_refresh_pending=search_refresh_pending OR feature_sha256 IS DISTINCT FROM %s,
        feature_sha256=%s,next_attempt_at_utc=NOW()+INTERVAL '6 hours',updated_at_utc=NOW()
        WHERE event_id=%s AND method_version=%s AND status IN ('PENDING','PARTIAL','DATA_MISSING')
        RETURNING event_id""",(result['status'],json.dumps({**result,'event_id':event_id},default=str,allow_nan=False),
        result['feature_sha256'],result['feature_sha256'],event_id,METHOD_VERSION)).fetchone()
    return bool(row)


def defer_event(conn,event_id:int,*,reason:str) -> None:
    conn.execute("""UPDATE research_past_price_features SET next_attempt_at_utc=NOW()+INTERVAL '15 minutes',
        status=CASE WHEN status='PENDING' THEN 'DATA_MISSING' ELSE status END,
        result=result || jsonb_build_object('worker_error',%s::text),updated_at_utc=NOW()
        WHERE event_id=%s AND method_version=%s AND status IN ('PENDING','PARTIAL','DATA_MISSING')""",
        (str(reason)[:300],int(event_id),METHOD_VERSION))


def load_by_event_ids(conn,event_ids:Iterable[int]) -> dict[int,dict[str,Any]]:
    ids=sorted(set(int(value) for value in event_ids))
    if len(ids)>10000:raise ValueError('Unbounded past feature lookup')
    if not ids:return {}
    rows=conn.execute("SELECT event_id,method_version,event_time_utc,feature_sha256,status,result FROM research_past_price_features WHERE event_id=ANY(%s::bigint[]) AND method_version=%s",(ids,METHOD_VERSION)).fetchall()
    return {int(row['event_id']):{**dict(row['result'] or {}),**{key:row[key] for key in ('event_id','method_version','event_time_utc','feature_sha256','status')}} for row in rows}


def pending_refresh_ids(conn,*,limit:int=32) -> list[dict[str,Any]]:
    """Return bounded late-enrichment IDs for the searcher's source projection."""
    return conn.execute("""SELECT event_id,feature_sha256 FROM research_past_price_features
        WHERE search_refresh_pending=TRUE AND method_version=%s
        ORDER BY updated_at_utc,event_id LIMIT %s""",(METHOD_VERSION,max(1,min(int(limit),128)))).fetchall()


def ack_refresh(conn,event_id:int,feature_sha256:str) -> None:
    """Commit with feature screens; a newer concurrent enrichment cannot be lost."""
    conn.execute("""UPDATE research_past_price_features SET search_refresh_pending=FALSE
        WHERE event_id=%s AND method_version=%s AND feature_sha256=%s""",(int(event_id),METHOD_VERSION,feature_sha256))
