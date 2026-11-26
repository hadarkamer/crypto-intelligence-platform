"""Commit one BTC/1/12H capture and its interpretation in the bridge's existing private evidence row."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, contextmanager
import hashlib
import json
import os
import re
from pathlib import Path
import socket
import tempfile

KEY = 'measured_btc_v1'
PARTIAL = 'measured_btc_partial_v1'
FD = 'COINGLASS_MEASURED_ACK_FD'
SOURCE = 'measured-source.json'
ANSWER = 'measured-interpretation.json'
CLAIM = 'measured-analysis-started.json'
MAX_RECORD = 2 * 1024 * 1024
MAX_IMAGE = 4 * 1024 * 1024
RETAIN_DIAGNOSTIC = """INSERT INTO public.ai_collection_bridge_evidence AS e(job_id,image_png,diagnostic)
    VALUES(%s,%s,%s) ON CONFLICT(job_id) DO UPDATE SET
    image_png=CASE WHEN e.diagnostic ?| ARRAY['measured_btc_v1','measured_btc_partial_v1']
                   THEN e.image_png ELSE EXCLUDED.image_png END,
    diagnostic=EXCLUDED.diagnostic || coalesce(
        (SELECT jsonb_object_agg(k,v) FROM jsonb_each(e.diagnostic) AS item(k,v)
         WHERE k IN ('measured_btc_v1','measured_btc_partial_v1')), '{}'::jsonb)"""
EXPIRE_IMAGES = """UPDATE public.ai_collection_bridge_evidence AS e SET image_png=NULL
    WHERE created_at<now()-interval '6 hours' AND image_png IS NOT NULL AND
    (NOT (e.diagnostic ?| ARRAY['measured_btc_v1','measured_btc_partial_v1']) OR EXISTS
        (SELECT 1 FROM public.ai_collection_bridge_jobs j WHERE j.id=e.job_id AND j.status='ready'))"""


def _observation_receipts(stages):
    receipts = {}
    iso_utc = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z'
    amount = r'(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?[KMBT]?'
    clock_fields = {'post_failure_observed_at':iso_utc, 'post_failure_source_time':iso_utc,
                    'post_failure_raw_updated_at':r'(?:[01]\d|2[0-3]):[0-5]\d',
                    **{'post_failure_raw_'+k:amount for k in ('total','above_total','below_total')}}
    for step in stages:
        name, status = step.get('name'), step.get('status')
        if name == 'post_failure_market_observation' and status in ('observed','unavailable','stale_or_unverified_time'):
            receipts[name] = {'status':status, **{k:step[k] for k,pattern in clock_fields.items() if k in step
                and (step[k] is None or isinstance(step[k],str) and len(step[k])<=40 and re.fullmatch(pattern,step[k]))}}
        elif name == 'chart_obstruction' and status in ('clear','partial'):
            receipt = {'status':status}
            for key in ('cards','eligible','close_attempts'):
                if type(step.get(key)) is int and step[key] >= 0:
                    receipt[key] = step[key]
            for key in ('covered','unknown'):
                if isinstance(step.get(key),list):
                    receipt[key] = [rect for rect in step[key] if isinstance(rect,list) and len(rect)==4
                                    and all(type(n) is int for n in rect) and rect[0]<rect[2] and rect[1]<rect[3]]
            receipts[name] = receipt
    return receipts


def _measure_detail(detail):
    if not isinstance(detail,dict):
        return None
    reason = detail.get('reason')
    if reason in ('native_price_not_printed','box_bound_precision_missing','native_step_invalid',
                  'price_not_on_native_grid','box_not_within_native_bin','prominent_bin_amount_missing',
                  'maximum_price_not_confirmed','single_bin_amount_uncorroborated'):
        return {'reason':reason}
    if reason not in ('right_edge_inactive','label_missing','number_missing','conflicting_values'):
        return None
    labels = ('box','leader','reference')
    result = {'reason':detail['reason']}
    if detail.get('field') in labels:
        result['field'] = detail['field']
    for kind in ('label_counts','reading_counts'):
        counts = detail.get(kind)
        if isinstance(counts,dict):
            result[kind] = {k:counts[k] for k in labels if type(counts.get(k)) is int and 0<=counts[k]<=220}
    return result


def partial_record(job, overview, hovers, stages, error, image_path):
    """Keep validated public market fields and the failing step. A partial cannot authorize analysis."""
    from modern_map_overview import _FIELDS
    allowed = (*_FIELDS,'source_time','sidebar_levels','missing_fields','ambiguous_fields',
               'display_values','parsed_values')
    typed = {k: overview[k] for k in allowed if k in overview}
    dims = {'symbol':'BTC','heatmap_model':1,'timeframe':'12H','mode':'Coin'}
    safe = re.compile(r'[a-zA-Z0-9_]{1,100}\Z')
    failed = [row for row in stages if row.get('status')=='failed']
    last = failed[-1] if failed else {}
    condition = next((token for token in (getattr(error,'reason',None),last.get('condition'),getattr(error,'code',None),
                                         type(error).__name__) if isinstance(token,str) and safe.fullmatch(token)), 'reader_failed')
    failure = {k:last[k] for k in ('name','expected','received','missing','elapsed_ms') if k in last}
    failure['condition'] = condition
    detail = _measure_detail(last.get('details'))
    if detail is not None:
        failure['details'] = detail
    # Every event comes from the reader's market-only projection. Do not store page text or exceptions.
    rows = [{k:event[k] for k in ('source','rows','counts','raw_step') if k in event} for event in hovers]
    return dict(run_id=job,status='partial',identity=dims,overview=typed,hover=rows,failure=failure,
                **_observation_receipts(stages),
                source_time=overview.get('source_time'),image_sha256=(hashlib.sha256(image_path.read_bytes()).hexdigest()
                if image_path.is_file() else None))


def _json(path, limit=MAX_RECORD):
    with path.open('rb') as stream:
        data = stream.read(limit + 1)
    if len(data) > limit:
        raise ValueError('measured_record_size')
    return json.loads(data)


def _identity(source, job, digest):
    snap = source['snapshot']
    if (source.get('run_id'),source.get('image_sha256'),snap.get('symbol'),snap.get('heatmap_model'),
            snap.get('timeframe'),snap.get('mode'),snap.get('source_url')) != (
            job,digest,'BTC',1,'12H','Coin','https://www.coinglass.com/LiquidationHeatMap/BTC?mode=coin'):
        raise ValueError('measured_record_identity')


class CheckpointStore:
    def __init__(self, job_store, job):
        self.job_store, self.job = job_store, job

    def read(self):
        with self.job_store.connect() as connection:
            row = connection.execute("""SELECT e.diagnostic->'measured_btc_v1' AS checkpoint,e.image_png
                FROM public.ai_collection_bridge_evidence e
                JOIN public.ai_collection_bridge_jobs j ON j.id=e.job_id
                WHERE e.job_id=%s AND j.symbol='BTC' AND j.heatmap_model=1 AND j.timeframe='12H'
                  AND e.diagnostic ? 'measured_btc_v1'""",(self.job,)).fetchone()
        if not row: return None
        return row['checkpoint'],bytes(row['image_png'])

    def commit(self, action, root):
        """A job lock serializes claim/response; acknowledgement follows a committed transaction."""
        with self.job_store.connect() as connection:
            job = connection.execute("""SELECT id FROM public.ai_collection_bridge_jobs WHERE id=%s
                AND status='running' AND symbol='BTC' AND heatmap_model=1 AND timeframe='12H'
                FOR UPDATE""",(self.job,)).fetchone()
            if not job:
                raise ValueError('measured_job_not_active')
            row = connection.execute("""SELECT diagnostic->'measured_btc_v1' AS checkpoint
                FROM public.ai_collection_bridge_evidence WHERE job_id=%s FOR UPDATE""",(self.job,)).fetchone()
            previous = row and row['checkpoint']
            if action == b'P':
                if previous: return
                partial = _json(root/'measured-partial.json')
                image = (root/'image.png').read_bytes() if (root/'image.png').is_file() else None
                digest = hashlib.sha256(image).hexdigest() if image is not None else None
                if (partial.get('run_id') != self.job or partial.get('status') != 'partial' or
                        partial.get('image_sha256') != digest or image is not None and
                        (len(image)>MAX_IMAGE or not image.startswith(b'\x89PNG\r\n\x1a\n'))):
                    raise ValueError('measured_partial_identity')
                # Retain the first usable partial for this job. A checked full
                # source can supersede it; a later partial cannot alter either.
                connection.execute("""INSERT INTO public.ai_collection_bridge_evidence AS e(job_id,image_png,diagnostic)
                    VALUES (%s,%s,%s::jsonb) ON CONFLICT(job_id) DO UPDATE SET image_png=EXCLUDED.image_png,
                    diagnostic=e.diagnostic || EXCLUDED.diagnostic WHERE NOT
                    (e.diagnostic ?| ARRAY['measured_btc_v1','measured_btc_partial_v1'])""",
                    (self.job,image,json.dumps({PARTIAL:partial},allow_nan=False)))
                return
            if action == b'S':
                source = _json(root/SOURCE)
                image = (root/'image.png').read_bytes()
                if len(image)>MAX_IMAGE or not image.startswith(b'\x89PNG\r\n\x1a\n'):
                    raise ValueError('measured_record_image')
                digest = hashlib.sha256(image).hexdigest()
                _identity(source,self.job,digest)
                if previous:
                    if previous.get('source') != source: raise ValueError('measured_source_changed')
                    return
                connection.execute("""INSERT INTO public.ai_collection_bridge_evidence(job_id,image_png,diagnostic)
                    VALUES(%s,%s,%s::jsonb) ON CONFLICT(job_id) DO UPDATE SET image_png=EXCLUDED.image_png,
                    diagnostic=ai_collection_bridge_evidence.diagnostic || EXCLUDED.diagnostic""",
                    (self.job,image,json.dumps({KEY:{'source':source}},allow_nan=False)))
                return
            if not previous:
                raise ValueError('measured_source_missing')
            if action == b'C':
                if previous.get('claimed'):
                    raise ValueError('measured_analysis_outcome_pending')
                previous['claimed'] = True
            elif action == b'V':
                answer = _json(root/ANSWER,32768)
                if (not previous.get('claimed') or (answer.get('run_id'),answer.get('image_sha256')) !=
                        (self.job,previous['source']['image_sha256']) or previous.get('answer',answer) != answer):
                    raise ValueError('measured_analysis_binding')
                previous['answer'] = answer
            else:
                raise ValueError('measured_checkpoint_action')
            connection.execute("""UPDATE public.ai_collection_bridge_evidence
                SET diagnostic=jsonb_set(diagnostic,'{measured_btc_v1}',%s::jsonb)
                WHERE job_id=%s""",(json.dumps(previous,allow_nan=False),self.job))


def restore(root, job, entry):
    if entry is None:
        return False
    from modern_map_task import _write_once
    record, image = entry
    digest = hashlib.sha256(image).hexdigest()
    source = record['source']
    _identity(source,job,digest)
    (root/'image.png').write_bytes(image)
    _write_once(root/SOURCE,source)
    if record.get('claimed'):
        _write_once(root/CLAIM,dict(run_id=job,image_sha256=digest))
    if record.get('answer'):
        _write_once(root/ANSWER,record['answer'])
    return True


class CheckpointChannel:
    def __init__(self, store, root):
        self.store, self.root = store, root
        self.channel,self.child = socket.socketpair()
        self.channel.setblocking(False)
        self.restored = False
        self.failed = None

    async def serve(self):
        loop = asyncio.get_running_loop()
        while action := await loop.sock_recv(self.channel,1):
            try:
                await asyncio.to_thread(self.store.commit,action,self.root)
            except Exception:
                self.failed = action
                await loop.sock_sendall(self.channel,b'E')
                return
            await loop.sock_sendall(self.channel,b'Y')

    def attach(self, env, pass_fds):
        if self.restored or not pass_fds:
            from session_runtime import WORKER_ENV
            for key in set(env)-WORKER_ENV:
                del env[key]
        fd = self.child.fileno()
        env[FD] = str(fd)
        return (*pass_fds,fd)


@asynccontextmanager
async def workspace(job,tf,model,symbol,store_factory,enabled):
    with tempfile.TemporaryDirectory(prefix='collection-model1-') as tmp:
        if not enabled or (symbol,model,tf) != ('BTC',1,'12H'):
            yield tmp,None
            return
        store = CheckpointStore(store_factory(os.getenv('DATABASE_URL','')),job)
        channel = CheckpointChannel(store,Path(tmp))
        task = None
        try:
            channel.restored = restore(Path(tmp),job,await asyncio.to_thread(store.read))
            task = asyncio.create_task(channel.serve())
            yield tmp,channel
        finally:
            channel.child.close()
            if task:
                await task
            channel.channel.close()
            # If acknowledging a known local snapshot/answer failed, make one
            # idempotent save before the private working directory is released.
            if channel.failed in (b'P',b'S',b'V'):
                await asyncio.to_thread(store.commit,channel.failed,Path(tmp))


@contextmanager
def acknowledgements():
    raw = os.environ.pop(FD,None)
    if raw is None:
        yield lambda _: None
        return
    from model1_execution import StageFailure
    with socket.socket(fileno=int(raw)) as channel:
        channel.set_inheritable(False)
        channel.settimeout(20)
        def acknowledge(kind):
            try:
                channel.sendall(kind)
                accepted = channel.recv(1) == b'Y'
            except OSError:
                accepted = False
            if not accepted:
                raise StageFailure('evidence_store_failed')
        yield acknowledge
