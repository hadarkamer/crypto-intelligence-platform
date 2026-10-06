"""On-demand inverse-label provenance, without manufacturing another alert."""
from __future__ import annotations

from copy import deepcopy
from decimal import Decimal
from datetime import datetime, timezone
import hashlib
import json
import math
from typing import Any, Iterable, Mapping

VERSION = 'ordered-inverse-analysis-v1'
EVENT_TYPE = 'ORDERED_INVERSE_ANALYSIS'
METHOD_VERSION = 'ordered-first-touch-v7'
WINDOWS = (60, 240, 720, 1440)
THRESHOLDS = (25, 50, 75, 100, 125, 150, 175, 200)
SOURCE_FIELDS = ('event_id','schema_version','event_kind','event_type','alert_time_utc',
    'symbol','direction','source_side','timeframe','score','current_price','target_price',
    'initial_target_distance_pct','categories','setup_key','event_fingerprint',
    'strategy_version','code_version','runtime_session_id','engine_snapshot','delivery_status')


def canonical(value: Any) -> str:
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,default=str,allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def utc(value: Any) -> datetime:
    value = value if isinstance(value,datetime) else datetime.fromisoformat(str(value).replace('Z','+00:00'))
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError('Inverse source requires timezone-qualified immutable time')
    return value.astimezone(timezone.utc)


def source_contract(source: Mapping[str, Any]) -> dict[str, Any]:
    contract = {key:deepcopy(source.get(key)) for key in SOURCE_FIELDS}
    contract['alert_time_utc'] = utc(source['alert_time_utc']).isoformat()
    return contract


def _rejection_audit_value(value: Any) -> Any:
    """Retain invalid source values as explicit tags, never usable numbers."""
    if ((isinstance(value,float) and not math.isfinite(value))
            or (isinstance(value,Decimal) and not value.is_finite())):
        return {'invalid_nonfinite_number':str(value)}
    if isinstance(value, Mapping):
        return {str(key):_rejection_audit_value(item) for key,item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_rejection_audit_value(item) for item in value]
    if isinstance(value, datetime):
        return {'source_datetime':value.isoformat()}
    return value


def rejected_source_contract(source: Mapping[str, Any], reason: str) -> dict[str, Any]:
    return {'audit_kind':'REJECTED_IMMUTABLE_SOURCE','inverse_version':VERSION,
        'source_event_id':source.get('event_id'),'source_event_fingerprint':source.get('event_fingerprint'),
        'rejection_reason':reason,
        'source_fields':_rejection_audit_value({key:source.get(key) for key in SOURCE_FIELDS})}


def source_error(source: Mapping[str, Any]) -> str | None:
    if source.get('event_kind') != 'ALERT' or source.get('delivery_status') != 'DELIVERED':
        return 'Inverse source must be an original delivered ALERT'
    if source.get('event_type') == EVENT_TYPE or source.get('direction') not in ('LONG','SHORT'):
        return 'Inverse source is already derived or has no canonical research direction'
    snapshot = source.get('engine_snapshot')
    if not isinstance(snapshot,Mapping):
        return 'Missing immutable engine snapshot'
    if 'inverse_analysis' in snapshot:
        return 'An inverse source cannot be inverted again'
    if any(str(snapshot.get(key,'')).upper() == 'DEMO' for key in ('data_mode','state','mode')):
        return 'DEMO is not inverse research evidence'
    try:
        if isinstance(source.get('current_price'),bool):
            raise ValueError('boolean price')
        price = float(source['current_price'])
        if not math.isfinite(price) or price<=0:
            raise ValueError('nonpositive or nonfinite price')
        utc(source['alert_time_utc'])
        if int(source['event_id'])<=0 or len(str(source.get('event_fingerprint') or ''))!=64:
            raise ValueError('invalid source identity')
    except (KeyError,ValueError,TypeError,OverflowError) as exc:
        return 'Invalid immutable inverse entry: '+str(exc)
    from research_outcome_worker import _alert_reference_provenance_error
    return _alert_reference_provenance_error(source)


def available(conn: Any) -> bool:
    row = conn.execute("SELECT to_regclass('research_ordered_inverse_requests') IS NOT NULL AS available").fetchone()
    return bool(row and row['available'])


def request_inverse(conn: Any, original_event: Mapping[str, Any], *, now: datetime) -> int | None:
    """Queue only an actually matched source; materialization stays bounded.

    The original is reread by primary key: a caller cannot provide a modified
    price, timestamp, direction or raw snapshot as the archived source.
    """
    source = conn.execute('SELECT * FROM research_events WHERE event_id=%s',
                          (int(original_event['event_id']),)).fetchone()
    if source is None:
        return None
    source = dict(source)
    error = source_error(source)
    if error:
        contract = rejected_source_contract(source,error)
    else:
        try:
            contract = source_contract(source)
            canonical(contract)
        except (TypeError,ValueError,OverflowError) as exc:
            error = 'Invalid immutable inverse source payload: '+str(exc)
            contract = rejected_source_contract(source,error)
    conn.execute('''INSERT INTO research_ordered_inverse_requests(
        linked_source_event_id,inverse_version,source_fingerprint,source_contract_sha256,
        source_contract,queue_status,requested_at_utc,next_attempt_at_utc,last_error)
        VALUES(%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s)
        ON CONFLICT(linked_source_event_id,inverse_version) DO NOTHING''',
        (source['event_id'],VERSION,source['event_fingerprint'],digest(contract),canonical(contract),
         'REJECTED' if error else 'PENDING',utc(now),utc(now),error))
    row = conn.execute('''SELECT outcome_event_id,source_contract_sha256,queue_status FROM research_ordered_inverse_requests
        WHERE linked_source_event_id=%s AND inverse_version=%s''',(source['event_id'],VERSION)).fetchone()
    # A rejected source is an immutable audit decision. A subsequent caller
    # cannot repair its payload and silently reuse this inverse version.
    if row and row.get('queue_status') == 'REJECTED':
        return None
    if row and row['source_contract_sha256'] != digest(contract):
        raise ValueError('Immutable inverse source changed; keep the original request for audit')
    return int(row['outcome_event_id']) if row and row.get('outcome_event_id') else None


def derived_event(source: Mapping[str, Any]) -> dict[str, Any]:
    error = source_error(source)
    if error:
        raise ValueError(error)
    contract = source_contract(source)
    direction = {'LONG':'SHORT','SHORT':'LONG'}[source['direction']]
    fingerprint = digest([VERSION,int(source['event_id']),source['event_fingerprint'],direction])
    snapshot = deepcopy(dict(source['engine_snapshot']))
    snapshot['inverse_analysis'] = {
        'version':VERSION,'linked_source_event_id':int(source['event_id']),
        'source_event_fingerprint':source['event_fingerprint'],'source_contract_sha256':digest(contract),
        'source_analysis_direction':source['direction'],'analysis_direction':direction,
        'entry_policy':'EXACT_ORIGINAL_TIME_AND_REFERENCE_PRICE',
        'independence_policy':'ORIGINAL_SOURCE_EVENT_AND_BTC_PARENT_ONLY',
        'original_target_price':source.get('target_price'),'telegram_delivery':False,
        'prospective_anchor':False,'live_effect':'NONE',
    }
    return {
        'schema_version':source['schema_version'],'event_kind':'DECISION_SAMPLE',
        'event_type':EVENT_TYPE,'alert_time_utc':utc(source['alert_time_utc']),
        'symbol':source['symbol'],'direction':direction,'source_side':source.get('source_side'),
        'timeframe':source.get('timeframe'),'score':None,'current_price':source['current_price'],
        'target_price':None,'initial_target_distance_pct':None,'categories':['RESEARCH_ONLY','INVERSE'],
        'setup_key':digest([VERSION,source['setup_key'],direction]),'event_fingerprint':fingerprint,
        'strategy_version':source['strategy_version'],'code_version':VERSION,
        'runtime_session_id':VERSION,'capture_stage':'DERIVED_RESEARCH_INVERSE',
        'delivery_status':'NOT_APPLICABLE','delivery_attempted_at_utc':None,
        'delivered_at_utc':None,'engine_snapshot':snapshot,
    }


def verify_derived(source: Mapping[str, Any], derived: Mapping[str, Any]) -> None:
    expected = derived_event(source)
    for field in ('event_kind','event_type','direction','current_price','event_fingerprint',
                  'delivery_status','delivery_attempted_at_utc','delivered_at_utc','symbol'):
        if derived.get(field) != expected[field]:
            raise ValueError('Derived inverse identity mismatch: '+field)
    if utc(derived['alert_time_utc']) != expected['alert_time_utc']:
        raise ValueError('Derived inverse entry time differs from original')
    if canonical(derived.get('engine_snapshot')) != canonical(expected['engine_snapshot']):
        raise ValueError('Derived inverse provenance differs from immutable source')


def materialize(conn: Any, job: Mapping[str, Any], source: Mapping[str, Any]) -> dict[str, Any]:
    if digest(source_contract(source)) != job['source_contract_sha256']:
        raise ValueError('Immutable source differs from queued inverse contract')
    event = derived_event(source)
    from research_event_store import _INSERT_SQL
    record = {**event,'categories':canonical(event['categories']),
              'engine_snapshot':canonical(event['engine_snapshot'])}
    conn.execute(_INSERT_SQL, record)
    stored = conn.execute('SELECT * FROM research_events WHERE event_fingerprint=%s',
                           (event['event_fingerprint'],)).fetchone()
    if stored is None:
        raise ValueError('Derived event insertion was not confirmed')
    stored = dict(stored)
    verify_derived(source,stored)
    if job.get('outcome_event_id') is not None and int(job['outcome_event_id'])!=int(stored['event_id']):
        raise ValueError('Inverse request is already linked to a different outcome event')
    conn.execute('''UPDATE research_ordered_inverse_requests SET outcome_event_id=%s
        WHERE linked_source_event_id=%s AND inverse_version=%s AND claim_token=%s::uuid''',
        (stored['event_id'],source['event_id'],VERSION,job['claim_token']))
    return stored


def outcome_event_ids(conn: Any, original_ids: Iterable[int]) -> dict[int,int]:
    ids = sorted(set(int(value) for value in original_ids))
    if len(ids)>10000:
        raise ValueError('Unbounded inverse event lookup')
    if not ids:
        return {}
    rows = conn.execute('''SELECT linked_source_event_id,outcome_event_id
        FROM research_ordered_inverse_requests WHERE linked_source_event_id=ANY(%s::bigint[])
          AND inverse_version=%s AND outcome_event_id IS NOT NULL AND queue_status<>'REJECTED' ''',
        (ids,VERSION)).fetchall()
    return {int(row['linked_source_event_id']):int(row['outcome_event_id']) for row in rows}


def load_inverse_outcomes(conn: Any, original_ids: Iterable[int], window_minutes: int,
                          threshold_bps: int) -> dict[int,dict[str,Any]]:
    if window_minutes not in WINDOWS or threshold_bps not in THRESHOLDS:
        raise ValueError('Unsupported inverse outcome window/threshold')
    mapping = outcome_event_ids(conn,original_ids)
    if not mapping:
        return {}
    reverse = {derived:original for original,derived in mapping.items()}
    rows = conn.execute('''SELECT * FROM research_ordered_first_touch_outcomes
        WHERE event_id=ANY(%s::bigint[]) AND window_minutes=%s AND threshold_bps=%s
          AND method_version='ordered-first-touch-v7' ''',
        (list(reverse),window_minutes,threshold_bps)).fetchall()
    return {reverse[int(row['event_id'])]:{**dict(row),'outcome_event_id':int(row['event_id']),
        'linked_source_event_id':reverse[int(row['event_id'])],'inverse_version':VERSION,
        'outcome_id':f"{row['event_id']}|{window_minutes}|{threshold_bps}|{METHOD_VERSION}"} for row in rows}
