"""Passive Watch-only batch bridge; native functions remain unchanged.

Sources contain only fields consumed by the five native decision/formatting
paths. This is a projected input contract, not a full transport-item digest.
All diagnostic work surrounds, and is isolated from, the single native call.
"""
from __future__ import annotations

from contextvars import ContextVar
from datetime import datetime, timezone
from functools import lru_cache
import hashlib
import json
import math
import os
from pathlib import Path
import re

import research_alert_decision_capture as capture
import research_alert_decision_store as store

VERSION = 'watch-alert-decision-bridge-v1'
SOURCE_VERSION = 'watch-native-batch-projection-v1'
NONEMPTY_MAPPING_MARKER = '__projection_nonempty_mapping__'
MAX_ITEMS = 256
MAX_CANDIDATES = 128
MAX_SOURCE_BYTES = 72 * 1024
_CURRENT = ContextVar('watch_alert_decision_bridge', default=None)
_METRICS = dict(started=0, finished=0, capture_failures=0, last_error_type=None)


def _gap(error):
    _METRICS['capture_failures'] += 1
    _METRICS['last_error_type'] = type(error).__name__ if isinstance(error, BaseException) else str(error)[:80]


def _safe(function, *args, **kwargs):
    try:
        return function(*args, **kwargs)
    except BaseException as error:
        _gap(error)
        try:
            handle=_CURRENT.get()
            if handle is not None and handle['capture_token'] is not None:
                capture.gap('BRIDGE_CAPTURE_OPERATION_FAILED')
        except BaseException: pass
        return None


@lru_cache(maxsize=1)
def code_versions():
    root = Path(__file__).parent
    return {name: hashlib.sha256((root/name).read_bytes()).hexdigest() for name in (
        'main.py', 'research_alert_decision_watch.py', 'research_alert_decision_capture.py',
        'research_alert_decision_store.py',
    )}


def _plain(value, depth=0, budget=None):
    """Bounded builtin-only copy; no custom repr/get/iteration hooks."""
    if budget is None: budget=[0,0]
    budget[0]+=1
    if depth > 6 or budget[0]>2048: raise ValueError('SOURCE_DEPTH_BOUND')
    kind = type(value)
    if kind is str:
        if len(value)>4096 or len(value.encode('utf-8'))>4096: raise ValueError('SOURCE_STRING_BOUND')
        budget[1]+=len(value.encode('utf-8'))
    elif kind is int:
        if value.bit_length()>1024: raise ValueError('SOURCE_INTEGER_BOUND')
        budget[1]+=len(str(value))
    elif kind is float and not math.isfinite(value): raise ValueError('SOURCE_NONFINITE')
    else: budget[1]+=8
    if budget[1]>MAX_SOURCE_BYTES: raise ValueError('SOURCE_VALUE_BYTE_BOUND')
    if value is None or kind in (str, bool, int, float): return value
    if kind is dict:
        if len(value) > 256 or any(type(k) is not str for k in value): raise ValueError('SOURCE_MAPPING_BOUND')
        return {_plain(k,depth+1,budget): _plain(v,depth+1,budget) for k,v in value.items()}
    if kind in (list, tuple):
        if len(value) > 256: raise ValueError('SOURCE_LIST_BOUND')
        return [_plain(v, depth+1,budget) for v in value]
    if kind in (set, frozenset):
        if len(value) > 256 or any(type(v) is not str for v in value): raise ValueError('SOURCE_SET_BOUND')
        return sorted(_plain(v,depth+1,budget) for v in value)
    raise ValueError('SOURCE_TYPE_UNAVAILABLE')


def _pick(value, fields):
    if type(value) is not dict: return _plain(value)
    result={key:_plain(value[key]) for key in fields if key in value}
    # Explicit projection encoding, not an original source key: preserve the
    # native truthiness of a nonempty mapping with no consumed fields.
    if value and not result: result[NONEMPTY_MAPPING_MARKER]=True
    return result


def _bounded_source(value):
    if len(capture.canonical_bytes(value))>MAX_SOURCE_BYTES: raise ValueError('SOURCE_AGGREGATE_BYTE_BOUND')
    return value


def _models(value):
    if type(value) is not dict: return _plain(value)
    result = {}
    for key in ('positioning','futures_flow','spot_flow'):
        if key not in value: continue
        module = value[key]
        if type(module) is not dict:
            result[key] = _plain(module)
            continue
        model = _pick(module, ('available','score','direction'))
        if key == 'spot_flow' and 'time_families' in module:
            families = module['time_families']
            model['families'] = ([dict(key=name, value=_pick(families[name], ('quality','direction','label','windows')))
                                  for name in ('now','short','medium','long') if name in families]
                                 if type(families) is dict else _plain(families))
        result[key] = model
    return result


def special_source(items, *, include_score65=False):
    if type(items) is not list or len(items) > MAX_ITEMS: raise ValueError('SOURCE_ITEMS_BOUND')
    records, models, used = [], {}, 0
    for item in items:
        if type(item) is not dict: raise ValueError('SOURCE_ITEM_NOT_MAPPING')
        value = _pick(item, ('symbol','timeframe','side','score','priority','target_price','distance_pct'))
        if 'maxpain_confirmation' in item:
            value['maxpain_confirmation'] = _pick(item['maxpain_confirmation'], ('status','label'))
        if 'market_evidence' in item:
            evidence = item['market_evidence']
            if type(evidence) is dict:
                if 'confirmation' in evidence:
                    value['evidence_confirmation'] = _pick(evidence['confirmation'], ('status','label'))
                if 'modules' in evidence:
                    projected = _models(evidence['modules'])
                    raw=capture.canonical_bytes(projected)
                    if len(raw)>12*1024: raise ValueError('SOURCE_MODEL_BYTE_BOUND')
                    key = hashlib.sha256(raw).hexdigest()
                    if key not in models: used+=len(raw)+len(key)+4
                    models[key] = projected
                    value['models_ref'] = key
            else:
                value['market_evidence_value'] = _plain(evidence)
        used+=len(capture.canonical_bytes(value))+2
        if used>MAX_SOURCE_BYTES: raise ValueError('SOURCE_AGGREGATE_BYTE_BOUND')
        records.append(value)
    return _bounded_source(dict(version=SOURCE_VERSION, include_score65=include_score65, item_count=len(items),
                items=records, models_by_sha256=models, source_scope='NATIVE_CONSUMED_FIELDS_ONLY',
                nonempty_mapping_marker=NONEMPTY_MAPPING_MARKER))


def restore_special_source(source):
    """Pure expansion for finite offline replay of this projection contract."""
    if source['version'] != SOURCE_VERSION: raise ValueError('SOURCE_VERSION')
    values=[]
    for row in source['items']:
        item={k:_plain(v) for k,v in row.items() if k not in ('models_ref','evidence_confirmation','market_evidence_value')}
        if 'market_evidence_value' in row: item['market_evidence']=_plain(row['market_evidence_value'])
        if 'evidence_confirmation' in row or 'models_ref' in row:
            evidence={}
            if 'evidence_confirmation' in row: evidence['confirmation']=_plain(row['evidence_confirmation'])
            if 'models_ref' in row:
                key=row['models_ref']; model=_plain(source['models_by_sha256'][key])
                if hashlib.sha256(capture.canonical_bytes(model)).hexdigest()!=key: raise ValueError('MODEL_REF_HASH')
                spot=model.get('spot_flow') if type(model) is dict else None
                if type(spot) is dict and 'families' in spot:
                    raw=spot.pop('families')
                    spot['time_families']={part['key']:part['value'] for part in raw} if type(raw) is list else raw
                evidence['modules']=model
            item['market_evidence']=evidence
        values.append(item)
    return values


def combined_source(candidates, *, event_time=None):
    if type(candidates) is not list or len(candidates)>MAX_CANDIDATES: raise ValueError('SOURCE_CANDIDATES_BOUND')
    entries=[];used=0;wire_used=0
    lists={'normal_confirmations':('timeframe',), 'strong_confirmations':('timeframe',),
           'high_scores':('timeframe','score'), 'anomaly_setups':('timeframe','count'),
           'liquidity_imbalances':('timeframe','share_pct'), 'derivatives_high':('title','score')}
    for candidate in candidates:
        if type(candidate) is not dict: raise ValueError('SOURCE_CANDIDATE_NOT_MAPPING')
        result=_pick(candidate, ('key','symbol','side','signal_count','signal_keys'))
        used+=len(capture.canonical_bytes(result))+2
        if used>MAX_SOURCE_BYTES: raise ValueError('SOURCE_AGGREGATE_BYTE_BOUND')
        for name,fields in lists.items():
            if name not in candidate: continue
            raw=candidate[name]
            if type(raw) is list:
                if len(raw)>MAX_ITEMS: raise ValueError('SOURCE_COMPONENTS_BOUND')
                projected=[]
                for row in raw:
                    value=_pick(row,fields);used+=len(capture.canonical_bytes(value))+2
                    if used>MAX_SOURCE_BYTES: raise ValueError('SOURCE_AGGREGATE_BYTE_BOUND')
                    projected.append(value)
                result[name]=projected
            else: result[name]=_plain(raw)
        if 'top_item' in candidate: result['top_item']=_pick(candidate['top_item'], ('score','timeframe'))
        if 'magnet' in candidate:
            result['magnet']=_pick(candidate['magnet'], ('status','magnet_quality','liquidity_edge_pct','members'))
        wire_used+=len(capture.canonical_bytes(result))+2
        if wire_used+512>MAX_SOURCE_BYTES: raise ValueError('SOURCE_AGGREGATE_BYTE_BOUND')
        used=max(used,wire_used)
        entries.append(result)
    return _bounded_source(dict(version=SOURCE_VERSION,candidates=entries,candidate_count=len(entries),
                event_time_utc=event_time.isoformat() if type(event_time) is datetime else _plain(event_time),
                source_scope='NATIVE_MESSAGE_AND_STATE_FIELDS_RESEARCH_MIRROR_EXCLUDED', include_metadata=True, persist_research=True,
                nonempty_mapping_marker=NONEMPTY_MAPPING_MARKER))


def begin(cycle_id, *, general_enabled, top8_only):
    """Start one Watch-only scope. No recipient identifier is retained."""
    token=None;handle=None
    try:
        token=capture.begin(cycle_id, metadata=dict(bridge_version=VERSION,
            population='ACTUAL_WATCH_NATIVE_BATCH_INVOCATIONS', general_enabled=bool(general_enabled),
            top8_only=bool(top8_only), individual_negative_reasons='UNKNOWN',
            source_projection_contract=SOURCE_VERSION, transport_permission_history_complete=False,
            research_lifecycle_mirror_captured=False))
        if token is None: _gap('COLLECTOR_START_UNAVAILABLE')
        handle=dict(cycle_id=cycle_id,capture_token=token,score_payload_sha256=None,actual_batch_invocations=0)
        handle['bridge_token']=_CURRENT.set(handle)
        _METRICS['started']+=1
        if token is None: return handle
        pins=_safe(code_versions)
        if pins is None:
            _safe(capture.gap,'SOURCE_PINS_UNAVAILABLE')
        else:
            _safe(capture.bind, source_code_sha256=pins)
        return handle
    except BaseException as error:
        _gap(error)
        if handle is not None and 'bridge_token' in handle:
            try: _CURRENT.reset(handle['bridge_token'])
            except BaseException as cleanup_error: _gap(cleanup_error)
        if token is not None: _safe(capture.reset,token)
        return None


def bind_score(bundle):
    try:
        handle=_CURRENT.get()
        if handle is None: return
        if type(bundle) is not dict or bundle.get('cycle_id')!=handle['cycle_id']:
            raise ValueError('SCORE_BINDING_UNAVAILABLE')
        pin=bundle.get('payload_sha256')
        if type(pin) is not str or not re.fullmatch('[0-9a-f]{64}',pin): raise ValueError('SCORE_HASH_UNAVAILABLE')
        handle['score_payload_sha256']=pin
        if handle['capture_token'] is None: return
        _safe(capture.bind,score_payload_sha256=pin,metadata=dict(score_capture_status=bundle.get('status'),
            input_universe_sha256=bundle.get('input_universe_sha256'),
            runtime_git_commit=os.getenv('RENDER_GIT_COMMIT','UNKNOWN')[:80]))
    except BaseException as error:
        _gap(error); _safe(capture.gap,'SCORE_BINDING_UNAVAILABLE')
        _safe(capture.stage,'source_binding','ERROR',reason_code='SCORE_BINDING_UNAVAILABLE')


def gate(ordinary_lifecycle):
    handle=_CURRENT.get()
    if handle is None or handle['capture_token'] is None: return
    _safe(capture.stage,'ordinary_lifecycle_gate','OPEN' if ordinary_lifecycle else 'SKIPPED_GATE',
          reason_code='ACTUAL_NATIVE_GATE',details=dict(ordinary_lifecycle=bool(ordinary_lifecycle)))
    if not ordinary_lifecycle:
        for family in ('special_native_batch','combined_native_batch'):
            _safe(capture.stage,family,'SKIPPED_GATE',reason_code='ORDINARY_LIFECYCLE_FALSE')


def _invoke(family, native, states, source):
    handle=_CURRENT.get()
    if handle is None or handle['capture_token'] is None: return native()
    decision=None
    _safe(capture.stage,family,'STARTED',reason_code='ACTUAL_INVOCATION')
    try:
        projected=source()
    except BaseException as error:
        _gap(error); _safe(capture.gap,'BEFORE_CAPTURE_FAILED')
        _safe(capture.stage,family,'CAPTURE_ERROR',reason_code='BEFORE_CAPTURE_FAILED')
        projected={'status':'UNKNOWN','reason_code':'SOURCE_PROJECTION_UNAVAILABLE'}
    decision=_safe(capture.begin_decision,family,state=states,source=projected)
    handle['actual_batch_invocations']+=1
    try:
        result=native()
    except BaseException as error:
        _safe(capture.end_decision,decision,error=error)
        _safe(capture.stage,family,'ERROR',reason_code='NATIVE_EXCEPTION',details=dict(exception_type=type(error).__name__))
        raise
    _safe(capture.end_decision,decision,result=result)
    _safe(capture.stage,family,'COMPLETED',reason_code='ACTUAL_NATIVE_RETURN')
    return result


def special_batch(native, items, states):
    return _invoke('special_native_batch',native,states,lambda:special_source(items,include_score65=False))


def combined_batch(native, candidates, states, *, event_time):
    return _invoke('combined_native_batch',native,states,lambda:combined_source(candidates,event_time=event_time))


def finish(handle, *, cycle_status):
    """Enqueue a separate post-decision capsule; acceptance is not persistence."""
    if handle is None: return None
    try:
        if handle['capture_token'] is not None:
            _safe(capture.stage,'cycle','FINISHED',reason_code='ACTUAL_WATCH_FINALLY',
                  details=dict(cycle_status=cycle_status,bridge_actual_batch_invocations=handle['actual_batch_invocations']))
            capsule=_safe(capture.seal)
        else: capsule=None
        if capsule is None:
            _gap('CAPSULE_SEAL_UNAVAILABLE')
            capsule=dict(version=capture.VERSION,cycle_id=handle['cycle_id'],status='UNAVAILABLE',
                payload=dict(reason_code='CAPSULE_SEAL_UNAVAILABLE',source_code_sha256=_safe(code_versions) or {},
                             score_payload_sha256=handle['score_payload_sha256']))
            capsule['payload_sha256']=hashlib.sha256(capture.canonical_bytes(capsule)).hexdigest()
        return _safe(store.submit,capsule)
    except BaseException as error:
        _gap(error)
        return None
    finally:
        if handle['capture_token'] is not None: _safe(capture.reset,handle['capture_token'])
        try: _CURRENT.reset(handle['bridge_token'])
        except BaseException as error: _gap(error)
        _METRICS['finished']+=1


def status():
    return dict(version=VERSION,source_projection_version=SOURCE_VERSION,
                **_METRICS,persistence=_safe(store.status) or {'status':'UNAVAILABLE'},
                scope='NATIVE_BATCH_DECISIONS_ONLY_NOT_FULL_TRANSPORT_OR_PROVIDER_HISTORY')
