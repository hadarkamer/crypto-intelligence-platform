"""Commit one BTC/1/12H capture and its protected numerical evidence in the bridge's existing private evidence row."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, contextmanager
import hashlib
import json
import math
import os
import re
from pathlib import Path
import socket
import tempfile

KEY = 'measured_btc_v1'
PARTIAL = 'measured_btc_partial_v1'
FD = 'COINGLASS_MEASURED_ACK_FD'
SOURCE = 'measured-source.json'
UPGRADE = 'measured-enriched.json'
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


def _numeric_price_receipt(step):
    """Allow only printed axis numbers and chart geometry, never arbitrary OCR text."""
    from modern_map_evidence import PRICE
    def price(value):
        return isinstance(value,str) and len(value)<=40 and value.isascii() and re.fullmatch(PRICE,value)
    def coordinate(value):
        return type(value) in (float,int) and -32768<=value<=32768
    receipt={'status':step['status']}
    for key in ('reference','raw_direct_reference','opening_reference','closing_reference'):
        if key in step and (step[key] is None or price(step[key])):
            receipt[key]=step[key]
    for key in ('ticks','direct_marks','ocr_marks'):
        if isinstance(step.get(key),list):
            receipt[key]=[{axis:m[axis] for axis in ('text','x','y') if axis in m}
                          for m in step[key][:1600] if isinstance(m,dict) and price(m.get('text'))
                          and coordinate(m.get('y')) and ('x' not in m or coordinate(m['x']))]
    if isinstance(step.get('reference_candidates'),list):
        receipt['reference_candidates']=[value for value in step['reference_candidates'][:1600] if price(value)]
    if type(step.get('ocr_cache_hit')) is bool:
        receipt['ocr_cache_hit']=step['ocr_cache_hit']
    if step.get('method') in ('dom_canvas','tesseract'):
        receipt['method']=step['method']
    for key in ('tag_y','tick_residual','tag_residual'):
        if key in step and (step[key] is None or coordinate(step[key])):
            receipt[key]=step[key]
    if isinstance(step.get('tag_bands'),list):
        receipt['tag_bands']=[list(v) for v in step['tag_bands'][:1600] if isinstance(v,(tuple,list))
                              and len(v)==2 and all(coordinate(n) for n in v)]
    rect=step.get('plot_rect')
    if isinstance(rect,(list,tuple)) and len(rect)==4 and all(coordinate(v) for v in rect):
        receipt['plot_rect']=list(rect)
    for key in ('ocr_calls','elapsed_ms','ocr_elapsed_ms','timeout_ms','exit_code'):
        value=step.get(key)
        if (type(value) in (int,float) and -255<=value<=3600000) or (key=='exit_code' and key in step and value is None):
            receipt[key]=value
    if step.get('condition') in ('verified','axis_geometry_invalid','tag_missing_or_ambiguous','reference_conflict',
            'reference_missing','tick_position_conflict','ticks_insufficient','tick_span_insufficient',
            'axis_position_conflict','axis_crop_outside_budget','reference_line_missing','ocr_process_failed','ocr_timed_out','ocr_runtime_unavailable'):
        receipt['condition']=step['condition']
    return receipt


def _comparison_operand(operand):
    """Preserve the operands of a failed epoch comparison, limited to typed market fields."""
    from modern_map_overview import _FIELDS
    keys={*_FIELDS,'source_time','column_time','reference','sidebar_levels','raw_sidebar_rows','unreadable_sidebar_containers',
          'duplicate_sidebar_levels','ambiguous_fields','raw_price','raw_value','raw_distance_pct','display_values',
          'chart_regions','chart_groups','sidebar_rows','native_bins_inspected','side','colour','prices'}
    token=re.compile(r'(?:\$?\s*[+−-]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?\s*(?:[KMBT%]|s|m|h|min|minutes?)?'
                     r'|\d{2}:\d{2}|\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z)')
    def project(value,depth=0):
        if depth>5:
            return None
        if isinstance(value,dict):
            return {k:project(v,depth+1) for k,v in value.items() if k in keys}
        if isinstance(value,list):
            return [project(item,depth+1) for item in value[:1600]]
        if type(value) in (int,float) and math.isfinite(value):
            return value
        if isinstance(value,str) and len(value)<=64 and (token.fullmatch(value)
                or value in {*keys,'above','below','green','yellow','blue','sidebar_shape'}):
            return value
        return None
    return project(operand)


def _observation_receipts(stages):
    receipts = {}
    iso_utc = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z'
    amount = r'(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?[KMBT]?'
    clock_fields = {'post_failure_observed_at':iso_utc, 'post_failure_source_time':iso_utc,
                    'post_failure_raw_updated_at':r'(?:[01]\d|2[0-3]):[0-5]\d',
                    **{'post_failure_raw_'+k:amount for k in ('total','above_total','below_total')}}
    for step in stages:
        name, status = step.get('name'), step.get('status')
        if name in ('basic_opening_price_readout','basic_closing_price_readout') and status in ('observed','failed'):
            receipts[name]=_numeric_price_receipt(step)
        elif name in ('basic_start_observation','basic_end_observation') and status=='ok':
            receipts[name]=_comparison_operand(step)
        elif name in ('basic_compare_reference','basic_compare_column','basic_compare_regions','basic_compare_market') and status=='failed':
            receipts[name]={k:_comparison_operand(step.get(k)) for k in ('expected','received')}
        elif name in ('opening_column_from_visible_crosshair','closing_column_from_visible_crosshair',
                      'opening_column_from_visible_tooltip','closing_column_from_visible_tooltip') and status=='ok':
            value=step.get('source_timestamp')
            if isinstance(value,str) and re.fullmatch(r'\d{1,2}/\d{1,2} \d{1,2}:\d{2}',value):
                receipts[name]={'source_timestamp':value, **{k:step[k] for k in ('pointer_x','pointer_y')
                                if type(step.get(k)) is int and 0<=step[k]<32768}}
        elif name == 'post_failure_market_observation' and status in ('observed','unavailable','stale_or_unverified_time'):
            receipts[name] = {'status':status, **{k:step[k] for k,pattern in clock_fields.items() if k in step
                and (step[k] is None or isinstance(step[k],str) and len(step[k])<=40 and re.fullmatch(pattern,step[k]))}}
        elif name == 'basic_native_domain_readout' and status == 'observed':
            from modern_map_evidence import AMOUNT, PRICE
            receipt = {'status':status}
            fields = {**dict.fromkeys(('raw_box_total','raw_market_total','raw_above_total','raw_below_total'),AMOUNT),
                      **dict.fromkeys(('raw_reference','raw_price_low','raw_price_high'),PRICE),
                      **dict.fromkeys(('absolute_delta','rounding_tolerance'),r'[0-9]+(?:\.[0-9]+)?'),
                      'source_time':iso_utc}
            for key,pattern in fields.items():
                raw=step.get(key)
                if isinstance(raw,str) and raw.isascii() and len(raw)<=64 and re.fullmatch(pattern,raw):
                    receipt[key]=raw
            if type(step.get('totals_match')) is bool:
                receipt['totals_match']=step['totals_match']
            if step.get('count_mode')=='Right edge':
                receipt['count_mode']='Right edge'
            rect=step.get('plot_rect')
            if (isinstance(rect,list) and len(rect)==4 and all(type(v) is int for v in rect)
                    and 0<=rect[0]<rect[2] and 0<=rect[1]<rect[3]):
                receipt['plot_rect']=rect
            receipts[name]=receipt
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
    condition = next((token for token in (getattr(error,'basic_reason',None),last.get('condition'),getattr(error,'reason',None),getattr(error,'code',None),
                                         type(error).__name__) if isinstance(token,str) and safe.fullmatch(token)), 'reader_failed')
    failure = {k:last[k] for k in ('name','missing','elapsed_ms') if k in last}
    for key in ('expected','received'):
        if key in last:
            failure[key]=_comparison_operand(last[key])
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
        """Acknowledge verified source or partial evidence only after the database commit."""
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
            if action in (b'S',b'U'):
                source = _json(root/(SOURCE if action == b'S' else UPGRADE))
                image = (root/'image.png').read_bytes()
                if len(image)>MAX_IMAGE or not image.startswith(b'\x89PNG\r\n\x1a\n'):
                    raise ValueError('measured_record_image')
                digest = hashlib.sha256(image).hexdigest()
                _identity(source,self.job,digest)
                from modern_map_task import numeric_result, same_basic_with_enrichment
                restored=previous is not None and previous.get('source')==source
                numeric_result(source['snapshot'],self.job,image,restored=restored)
                if restored:
                    return
                if action == b'U':
                    if not previous or not same_basic_with_enrichment(previous['source']['snapshot'],source['snapshot']):
                        raise ValueError('measured_source_changed')
                elif previous:
                    raise ValueError('measured_source_changed')
                connection.execute("""INSERT INTO public.ai_collection_bridge_evidence(job_id,image_png,diagnostic)
                    VALUES(%s,%s,%s::jsonb) ON CONFLICT(job_id) DO UPDATE SET image_png=EXCLUDED.image_png,
                    diagnostic=ai_collection_bridge_evidence.diagnostic || EXCLUDED.diagnostic""",
                    (self.job,image,json.dumps({KEY:{'source':source}},allow_nan=False)))
                return
            raise ValueError('measured_checkpoint_action')


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
        for key in ('OPENAI_API_KEY','OPENAI_MARKET_SCANNER_MODEL'):
            env.pop(key,None)
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
            # If acknowledging a known local snapshot failed, make one
            # idempotent save before the private working directory is released.
            if channel.failed in (b'P',b'S',b'U'):
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
