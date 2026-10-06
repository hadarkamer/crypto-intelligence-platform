"""Bounded, restart-safe admission of frozen all-Watch observations.

No market requests, scoring, labels, formula evaluation or message delivery.
Receipts and a finite cyclic PK scan commit together. Repeating complete laps
recovers low IDs that commit late; a small recent reserve keeps new data fresh.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import math
import os
from typing import Any

import research_watch_score_capture as capture

VERSION = 'watch-all-scan-intake-v1'
POPULATION = 'watch-all-scan-observations-v1'
TIMEFRAMES = ('12h','24h','48h','3d','1w','2w','1m')
LOCK_ID = 46260913081341
PAGE_SIZE = 64
BUNDLE_LIMIT = 4
RECENT_SIZE = 2
POLL_SECONDS = 30
_TRUE = {'1','true','yes','on'}


def _utc(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace('Z','+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('NAIVE_TIME')
    return parsed.astimezone(timezone.utc)


def _number(value: Any) -> bool:
    return isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(value)


def validate_source(source: dict, *, now: datetime, activated_at: datetime) -> dict:
    """Keep feature availability separate from admission of the observation."""
    available,created = _utc(source['available_at_utc']),_utc(source['created_at_utc'])
    block = source.get('bundle')
    result = dict(snapshot_set_id=source['snapshot_set_id'],snapshot_key=source['snapshot_key'],
        parent_payload_sha256=source['payload_sha256'],watch_scan_id=source['cycle_id'],
        source_version=block.get('version') if isinstance(block,dict) else None,
        bundle_sha256=block.get('payload_sha256') if isinstance(block,dict) else None,
        capture_status=block.get('status') if isinstance(block,dict) else None,
        intake_status='REJECTED',rejection_reason=None,observed_at_utc=None,
        source_available_at_utc=available,source_created_at_utc=created,usable_from_utc=None,
        capture_phase='HISTORICAL_CAPTURE' if max(available,created)<=activated_at else 'FORWARD_CAPTURE',
        coin_count=0,score_slot_count=0,scored_slots=0,below_65_slots=0,
        unavailable_models=0,source_time_error_count=0)
    def reject(reason):
        result['rejection_reason']=reason
        return result
    if source.get('source') != 'WATCH_SHARED':
        return reject('NOT_SHARED_WATCH')
    if not isinstance(block,dict):
        return reject('INVALID_CAPTURE')
    if block.get('version') != capture.VERSION or block.get('hash_version') != capture.HASH_VERSION:
        return reject('UNSUPPORTED_CAPTURE_VERSION')
    if block.get('status') not in {'COMPLETE','PARTIAL'}:
        return reject('CAPTURE_FAILED')
    if block.get('population') != capture.POPULATION or block.get('cycle_id') != source['cycle_id']:
        return reject('SOURCE_IDENTITY_MISMATCH')
    try:
        unhashed={k:v for k,v in block.items() if k!='payload_sha256'}
        if len(capture.canonical(block).encode()) > capture.MAX_BYTES+100:
            return reject('CAPTURE_TOO_LARGE')
        if capture.digest(unhashed) != block.get('payload_sha256'):
            return reject('CAPTURE_HASH_MISMATCH')
        observed=_utc(block['computed_at_utc'])
        result['observed_at_utc']=observed
        if observed <= activated_at:
            result['capture_phase']='HISTORICAL_CAPTURE'
        result['usable_from_utc']=max(available,created,observed)
        if result['usable_from_utc'] > now:
            return reject('FUTURE_AVAILABILITY')
        if observed > available:
            return reject('COMPUTED_AFTER_SOURCE_AVAILABILITY')
        coins=block['coins']
        if set(coins)!=set(capture.SYMBOLS) or set(block['symbols_expected'])!=set(capture.SYMBOLS):
            return reject('COIN_COVERAGE_MISMATCH')
        for coin in coins.values():
            if coin['status'] not in {'CAPTURED','PARTIAL','ABSENT'}:
                return reject('INVALID_COIN_STATUS')
            slots=coin['maxpain']
            expected={(tf,side) for tf in TIMEFRAMES for side in ('LONG','SHORT')}
            if len(slots)!=14 or {(s['timeframe'],s['source_side']) for s in slots} != expected:
                return reject('SCORE_SLOT_COVERAGE_MISMATCH')
            if not isinstance(coin['models'],dict) or not isinstance(coin['sources'],dict):
                return reject('INVALID_MODEL_OR_SOURCE_BLOCK')
            if not isinstance(coin['source_time_errors'],list):
                return reject('INVALID_SOURCE_TIME_ERRORS')
            result['source_time_error_count']+=len(coin['source_time_errors'])
            for model in coin['models'].values():
                if not isinstance(model,dict) or type(model.get('available')) is not bool:
                    return reject('INVALID_MODEL_AVAILABILITY')
                required='AVAILABLE' if model['available'] else 'UNAVAILABLE'
                if model.get('capture_status') != required:
                    return reject('MODEL_AVAILABILITY_MISMATCH')
                result['unavailable_models']+=int(not model['available'])
            for slot in slots:
                if slot['status']=='SCORED':
                    score=slot.get('score')
                    if not _number(score) or not 0<=score<=100:
                        return reject('INVALID_SCORE')
                    components=slot.get('components') or {}
                    if not all(_number(components.get(k)) for k in capture.ADDITIVE_COMPONENTS):
                        return reject('INVALID_SCORE_COMPONENTS')
                    if abs(round(sum(components[k] for k in capture.ADDITIVE_COMPONENTS),2)-score)>0.011:
                        return reject('SCORE_COMPONENT_MISMATCH')
                    result['scored_slots']+=1
                    result['below_65_slots']+=int(score<65)
                elif slot['status'] not in {'INACTIVE_TARGET','MISSING_INPUT'} or slot.get('score') is not None:
                    return reject('INVALID_UNSCORED_SLOT')
        result.update(intake_status='ACCEPTED',coin_count=8,score_slot_count=112)
        return result
    except (ValueError,TypeError,KeyError,OverflowError):
        return reject('MALFORMED_CAPTURE')


def consume_page(conn) -> dict:
    """Caller owns transaction. No receipt or cursor survives a rollback."""
    if not conn.execute('SELECT pg_try_advisory_xact_lock(%s) AS held',(LOCK_ID,)).fetchone()['held']:
        return {'locked':True,'accepted':0,'rejected':0,'processed':0}
    conn.execute('INSERT INTO research_watch_scan_intake_state(consumer_version) VALUES (%s) ON CONFLICT DO NOTHING',(VERSION,))
    state=conn.execute('SELECT * FROM research_watch_scan_intake_state WHERE consumer_version=%s FOR UPDATE',(VERSION,)).fetchone()
    now=conn.execute('SELECT clock_timestamp() AS t').fetchone()['t']
    cursor,high=state['scan_cursor'],state['scan_high_water']
    if cursor>=high:
        cursor=0
        high=conn.execute('SELECT COALESCE(MAX(snapshot_set_id),0) AS id FROM research_max_pain_snapshot_sets').fetchone()['id']
    # Read IDs/one boolean first. JSON loading is limited independently below.
    page=conn.execute('''SELECT snapshot_set_id,
        source_metadata#>'{capture_metadata,operational_scores}' IS NOT NULL AS has_capture
        FROM research_max_pain_snapshot_sets WHERE snapshot_set_id>%s AND snapshot_set_id<=%s
        AND source_metadata#>'{capture_metadata,operational_scores}' IS NOT NULL
        ORDER BY snapshot_set_id LIMIT %s''',(cursor,high,PAGE_SIZE)).fetchall()
    candidates=[]
    for item in page:
        if item['has_capture']:
            candidates.append(item['snapshot_set_id'])
        cursor=item['snapshot_set_id']
        if len(candidates)>=BUNDLE_LIMIT:
            break
    recent=conn.execute('''SELECT snapshot_set_id FROM research_max_pain_snapshot_sets
        ORDER BY snapshot_set_id DESC LIMIT %s''',(RECENT_SIZE,)).fetchall()
    candidates=sorted(set(candidates)|{r['snapshot_set_id'] for r in recent})
    rows=conn.execute('''SELECT s.snapshot_set_id,s.snapshot_key,s.payload_sha256,s.cycle_id,
        s.source,s.available_at_utc,s.created_at_utc,
        s.source_metadata#>'{capture_metadata,operational_scores}' AS bundle
        FROM research_max_pain_snapshot_sets s WHERE s.snapshot_set_id=ANY(%s::bigint[])
        AND s.source_metadata#>'{capture_metadata,operational_scores}' IS NOT NULL
        AND NOT EXISTS(SELECT 1 FROM research_watch_scan_intakes i
            WHERE i.consumer_version=%s AND i.snapshot_set_id=s.snapshot_set_id)
        ORDER BY s.snapshot_set_id''',(candidates,VERSION)).fetchall()
    counts={'locked':False,'accepted':0,'rejected':0,'processed':0,'scanned_ids':len(page),
            'cursor':cursor,'high_water':high}
    for source in rows:
        receipt=validate_source(source,now=now,activated_at=state['activated_at_utc'])
        columns=list(receipt)
        conn.execute('INSERT INTO research_watch_scan_intakes(consumer_version,'+','.join(columns)+') VALUES ('+
                     ','.join(['%s']*(len(columns)+1))+')',(VERSION,*[receipt[k] for k in columns]))
        counts[receipt['intake_status'].lower()]+=1
        counts['processed']+=1
    finished=cursor>=high or not page
    if finished:
        cursor=high
    conn.execute('''UPDATE research_watch_scan_intake_state SET scan_cursor=%s,scan_high_water=%s,
        completed_laps=completed_laps+%s,updated_at_utc=clock_timestamp() WHERE consumer_version=%s''',
        (cursor,high,int(finished),VERSION))
    counts['completed_laps']=state['completed_laps']+int(finished)
    return counts


def _database_url() -> str:
    dedicated=os.getenv('RESEARCH_DATABASE_URL','').strip()
    if dedicated:
        return dedicated
    if os.getenv('RESEARCH_USE_PRIMARY_DATABASE','').strip().lower() in _TRUE:
        return os.getenv('DATABASE_URL','').strip()
    return ''


def _enabled() -> bool:
    return os.getenv('RESEARCH_WATCH_SCAN_INTAKE_ENABLED',os.getenv(
        'RESEARCH_OUTCOME_ENRICHMENT_ENABLED','')).strip().lower() in _TRUE


def _connect():
    import psycopg
    from psycopg.rows import dict_row
    return psycopg.connect(_database_url(),row_factory=dict_row,connect_timeout=5,
                          options='-c statement_timeout=15000 -c lock_timeout=1000')


def schema_ready() -> bool:
    with _connect() as conn:
        return all(conn.execute('SELECT to_regclass(%s) AS relation',('public.'+name,)).fetchone()['relation']
                   for name in ('research_watch_scan_intake_state','research_watch_scan_intakes',
                                'research_watch_scan_observations','research_watch_scan_score_slots'))


def run_once() -> dict:
    with _connect() as conn:
        return consume_page(conn)


class WatchScanIntakeWorker:
    def __init__(self):
        self._task=None
        self._metrics={'cycles':0,'failures':0,'last_error':None,'last':None,'last_run_utc':None}

    def status(self):
        return {'version':VERSION,'population':POPULATION,'enabled':_enabled(),
                'running':bool(self._task and not self._task.done()),
                'page_size':PAGE_SIZE,'max_bundles_per_pass':BUNDLE_LIMIT+RECENT_SIZE,
                'poll_seconds':POLL_SECONDS,'outcomes_enabled':False,'formula_evaluation_enabled':False,
                **self._metrics}

    async def start(self):
        if self._task and not self._task.done():
            return True
        if not _enabled() or not _database_url():
            return False
        if not await asyncio.to_thread(schema_ready):
            self._metrics['last_error']='SCHEMA_UNAVAILABLE'
            return False
        self._task=asyncio.create_task(self._loop(),name='watch-scan-research-intake')
        return True

    async def _loop(self):
        while True:
            try:
                result=await asyncio.to_thread(run_once)
                self._metrics.update(cycles=self._metrics['cycles']+1,last=result,last_error=None,
                    last_run_utc=datetime.now(timezone.utc).isoformat())
                if result['processed']:
                    print('[watch-scan-intake] '+str(result),flush=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Exception type is enough for diagnosis without leaking DSNs.
                self._metrics.update(failures=self._metrics['failures']+1,last_error=type(exc).__name__)
                print('[watch-scan-intake] failed: '+type(exc).__name__,flush=True)
            await asyncio.sleep(POLL_SECONDS)

    async def stop(self):
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task=None


WORKER=WatchScanIntakeWorker()
