"""One private, restored Firefox collection initiated by the native application's accepted job."""
import base64
from contextlib import contextmanager
from hashlib import sha256
import hmac
from io import BytesIO
import json
import os
from pathlib import Path
import re
import sys
import time
import traceback

from pilot_telemetry import PilotTelemetry

ROOT = Path('/tmp/cg-budget')
PREVIOUS = 'aa3ab31f-828f-4465-95da-60a462e802a5'
TARGET = 'https://www.coinglass.com/LiquidationHeatMap/BTC?mode=coin'
CONTEXT = dict(locale='en-US', timezone_id='UTC', color_scheme='dark',
               device_scale_factor=1, viewport={'width':1440,'height':1280})


def atomic(name, data):
    import tempfile
    with tempfile.NamedTemporaryFile(dir=ROOT,delete=False) as output:
        output.write(data)
        output.flush()
        os.fsync(output.fileno())
        stage=output.name
    os.replace(stage,ROOT/name)


def put(name, data):
    atomic(name, json.dumps(data,allow_nan=False,separators=(',',':')).encode())


def failure(error):
    def word(val):
        return val if isinstance(val,str) and re.fullmatch(r'[a-zA-Z0-9_]{1,85}',val) else None
    return {'type':word(type(error).__name__), 'code':word(getattr(error,'code',None)) or word(str(error)),
            'reason':word(getattr(error,'basic_reason',None)) or word(getattr(error,'reason',None)),
            'frames':[[Path(f.filename).name,f.name,f.lineno] for f in traceback.extract_tb(error.__traceback__)[-7:]]}


def retain_supplement(db,key,job):
    """Keep the market-only partial checkpoint, including the plot, under the existing owner-only LOs."""
    from qa_aesgcm import AESGCM
    from market_vault import _owner_only
    info=ROOT/'market-supplement.json'
    if not info.is_file():return None
    raw=info.read_bytes()
    plot=ROOT/'chart-observation.png'
    image=plot.read_bytes() if plot.is_file() else b''
    if len(raw)>2*1024**2 or len(image)>4*1024**2:raise ValueError('supplement_too_large')
    body=len(raw).to_bytes(4,'big') + raw + image
    digest=sha256(body).hexdigest()
    tag=b'coinglass.qa.single-market-supplement.v1\0'
    aad=tag+job.encode()+b'\0'+digest.encode()
    codec=AESGCM(hmac.digest(key,tag,'sha256'))
    oid=0x80000000 | int.from_bytes(hmac.digest(key,aad,'sha256')[:4],'big')
    with db.transaction():
        if not _owner_only(db,oid):
            nonce=os.urandom(12)
            sealed=nonce+codec.encrypt(nonce,body,aad)
            result=db.execute('SELECT pg_catalog.lo_from_bytea(%s::oid,%s::bytea) AS oid',(oid,sealed)).fetchone()['oid']
            if oid!=result or not _owner_only(db,oid):raise ValueError('owner_check_failed')
        recovered=bytes(db.execute('SELECT pg_catalog.lo_get(%s::oid,0,%s) AS data',(oid,len(body)+29)).fetchone()['data'])
        if not hmac.compare_digest(body,codec.decrypt(recovered[:12],recovered[12:],aad)):
            raise ValueError('supplement_readback_failed')
    return dict(oid=oid,sha256=digest,bytes=len(body),png_bytes=len(image),private_readback=True)


def source(job, meter, result):
    from psycopg import connect
    from psycopg.rows import dict_row
    from account_large_object import load,_id
    from session_state import WORK_LOCK
    from dedicated_auth_state import account_slot
    from ff_state import wire_type,is_login
    from wire_evidence import _endpoint
    from modern_map_reader import open_verified_map,read_selected_map,map_access_budget
    from model1_execution import capture_operation
    from dedicated_map_flow import PROTECTED_PROBE,guard_surface
    from market_vault import _binding,store_verified_bytes
    from retained_evidence import pack,checked_png
    from playwright.sync_api import sync_playwright

    dsn,keytext,email=[os.environ.pop(k,None) for k in
        ('DATABASE_URL','COINGLASS_SESSION_KEY','COINGLASS_LOGIN_EMAIL')]
    if not dsn or not keytext or not email:raise ValueError('missing_server_configuration')
    key=base64.b64decode(keytext,validate=True)
    digest=sha256(email.strip().lower().encode()).hexdigest()
    extra={'job_id':job,'source_time':None,'overview':None,'hover':[],'stages':[]}
    with connect(dsn,autocommit=True,connect_timeout=5,row_factory=dict_row) as db:
        meter.count('db_connections')
        if not db.execute('SELECT pg_try_advisory_lock(%s) AS ok',(WORK_LOCK,)).fetchone()['ok']:
            raise ValueError('worker_busy')
        with meter.phase('restore'):
            result['stage']='restore'
            rows=db.execute("""SELECT i.slot FROM collector_private.coinglass_dedicated_auth i JOIN
              collector_private.coinglass_dedicated_auth_jobs j ON j.incident_id=i.id WHERE j.job_id=%s""",(PREVIOUS,)).fetchall()
            slot=account_slot(key,email)
            if len(rows)!=1 or not hmac.compare_digest(rows[0]['slot'],slot):raise ValueError('restored_account_binding_mismatch')
            restored=load(db,key,slot,PREVIOUS,_id(key,slot,PREVIOUS))
            result['restored_owner_only_state']=True
        try:
            with sync_playwright() as pw:
                result['stage']='browser'
                with meter.phase('browser'):
                    browser=pw.firefox.launch(headless=True,timeout=20000,env={k:os.environ[k] for k in
                        ('HOME','LANG','PATH','LD_LIBRARY_PATH') if k in os.environ})
                    meter.count('browser_launch')
                try:
                    context=browser.new_context(storage_state=restored,**CONTEXT)
                    del restored
                    meter.attach(context)
                    wire=wire_type()(context,digest)
                    wire.observe_account_only=True
                    wire.attach()
                    def readonly(route):
                        req=route.request
                        if req.method not in ('GET','HEAD','OPTIONS'):
                            if is_login(_endpoint(req.url),('www.coinglass.com','/login')):result['unsolicited_login_blocked']=True
                            route.abort()
                        else:route.fallback()
                    from pub import ADS
                    context.route(ADS,lambda route:route.abort())
                    context.route('**/*',readonly)
                    page=context.new_page()
                    def guard():
                        guard_surface(page,PROTECTED_PROBE)
                        wire.account_envelope()
                        if wire.different or wire.provider_error:raise ValueError('account_mismatch_or_denial')
                    def opening():
                        with meter.phase('map'),map_access_budget(65):
                            return open_verified_map(page,'BTC',1,'12H',guard=guard)
                    result['stage']='map_open'
                    raw=opening()
                    result.update(protected_map=True,source_identity=['BTC',1,'Coin','12H'],access_metrics=raw)
                    result['account_http']=wire.account_reply_status
                    result['account_positive']=wire.account_envelope()
                    if wire.different or wire.provider_error:raise ValueError('account_mismatch_or_denial')
                    base_ref=None
                    base_bytes=None
                    def record(kind,value):
                        nonlocal base_ref,base_bytes
                        if kind=='verified_basic':
                            selected()
                            if not wire.account_envelope() or wire.different or wire.provider_error:
                                raise ValueError('account_not_verified')
                            from qa_job_server import write_once
                            png=(ROOT/'chart-observation.png').read_bytes()
                            blob=pack(value,png,job)
                            if value.get('collection_tier')!='basic':
                                raise ValueError('base_tier_required')
                            digest=sha256(blob).hexdigest()
                            oid=_binding(key,job,digest)[2]
                            # The receipt alone cannot authorize publication. The
                            # parent will authenticate the committed object anew.
                            write_once('basic-checkpoint.json',dict(job=job,oid=oid,sha256=digest,
                                chart_sha256=sha256(png).hexdigest(),source_time=value['source_time'],
                                column_time=value['column_time']))
                            base_ref=store_verified_bytes(db,key,job,digest,blob)
                            base_bytes=json.dumps(value,allow_nan=False,separators=(',',':')).encode()
                            result['basic_checkpoint']=base_ref
                            return True
                        if kind=='png':
                            checked_png(value);atomic('chart-observation.png',value)
                        elif kind=='overview':
                            extra['overview']=value
                            extra['source_time']=value.get('source_time')
                        elif kind in ('hover','stage'):
                            values=extra['hover' if kind=='hover' else 'stages']
                            if len(values)<260:values.append(value)
                        else:return
                        put('market-supplement.json',extra)
                        if kind=='stage':
                            label=value.get('name','')
                            ms=value.get('elapsed_ms')
                            if type(ms) in (int,float) and 0<=ms<=210000:
                                end=time.monotonic()
                                meter.spans.append(('reader.'+label,end-ms/1000,end))
                            if label in ('read_market_overview','recheck_market_update'):meter.count('dom_batch')
                            if label in ('read_hover','basic_optional_hover'):
                                meter.count('hover_try')
                                if value.get('status')=='ok':meter.count('hover_ok')
                            if label.startswith('measure_') or label in ('basic_native_price_domain','basic_optional_measure','basic_known_amount_control'):
                                meter.count('measure_try')
                                if value.get('status')=='ok':meter.count('measure_ok')
                            # Stage names and typed numerical conditions are safe for private service logs.
                            print('QA_READER',value,flush=True)
                    result['stage']='map_read'
                    bound=page.evaluate_handle('()=>document')
                    def selected():
                        if (page.is_closed() or page.url!=TARGET or not bound.evaluate('d=>d===document')):
                            raise ValueError('selected_document_replaced')
                        guard()
                    with BytesIO() as crop,map_access_budget(190),capture_operation('capture'):
                        try:
                            def capture():
                                return read_selected_map(page,'BTC',1,'12H',crop,guard=selected,observe=record)
                            try:
                                snapshot=capture()
                            except Exception as first:
                                result['reader_failure']=failure(first)
                                if getattr(first,'code',None)!='source_not_readable':
                                    raise
                                if not any(s.get('name')=='post_failure_market_observation' for s in extra['stages']):
                                    from modern_map_overview import after_read_failure
                                    after_read_failure(page,selected,record)
                                before=extra.get('source_time')
                                post=next((v.get('post_failure_source_time') for v in reversed(extra['stages'])
                                    if v.get('name')=='post_failure_market_observation' and v.get('status')=='observed'),None)
                                if not before or not post or post<=before:
                                    raise
                                selected()
                                result['prior_epoch_checkpoint']=retain_supplement(db,key,job)
                                result['bounded_recovery']='source_update_changed'
                                result['previous_source_time']=before
                                result['observed_new_source_time']=post
                                meter.count('retry')
                                extra.update(source_time=None,overview=None,hover=[],stages=[])
                                (ROOT/'chart-observation.png').unlink(missing_ok=True)
                                crop.seek(0)
                                crop.truncate(0)
                                snapshot=capture()
                                if snapshot.get('source_time')!=post:
                                    raise ValueError('source_update_changed_again')
                            selected()
                            if not wire.account_envelope() or wire.different or wire.provider_error:
                                raise ValueError('account_not_verified')
                            result['account_positive']=True
                            result['account_http']=wire.account_reply_status
                            image=crop.getvalue()
                            closing=snapshot.get('source_evidence',{}).get('closing',{})
                            if (closing.get('same_document') is not True or closing.get('settings_verified') is not True
                                or (closing.get('symbol'),closing.get('heatmap_model'),closing.get('timeframe'),closing.get('mode'))
                                   !=('BTC',1,'12H','Coin')
                                or not all(closing.get(field)==snapshot.get(field) and snapshot.get(field) is not None
                                           for field in ('source_time','column_time'))):
                                raise ValueError('basic_closing_observation_incomplete')
                            from modern_map_evidence import number
                            if number(closing['raw_last_price'])[0]!=number(snapshot['raw_last_price'])[0]:
                                raise ValueError('basic_closing_reference_mismatch')
                            result['end_observation']=closing
                        except Exception as error:
                            result['reader_failure']=failure(error)
                            raise
                        finally:
                            bound.dispose()
                    result['stage']='source_save'
                    with meter.phase('source_save'):
                        blob=pack(snapshot,image,job)
                        if not base_ref:
                            raise ValueError('missing_verified_basic_checkpoint')
                        market_digest=sha256(blob).hexdigest()
                        receipt=json.loads((ROOT/'basic-checkpoint.json').read_text())
                        if (receipt['chart_sha256']!=sha256(image).hexdigest()
                            or receipt['source_time']!=snapshot['source_time']
                            or receipt['column_time']!=snapshot['column_time']):
                            raise ValueError('enrichment_unbound_to_basic')
                        ref=base_ref
                        if base_ref['sha256']!=market_digest:
                            from modern_map_task import same_basic_with_enrichment
                            if not same_basic_with_enrichment(json.loads(base_bytes),snapshot):
                                raise ValueError('enrichment_unbound_to_basic')
                            ref=store_verified_bytes(db,key,job,market_digest,blob)
                        result['market_vault']=ref
                        meter.count('db_writes')
                        meter.count('db_reads')
                        meter.count('source_bytes',len(blob))
                        result['chart_sha256']=sha256(image).hexdigest()
                        atomic('chart-final.png',image)
                        put('snapshot.json',snapshot)
                        result['numeric_read']='verified_basic'
                finally:
                    with meter.phase('close'):
                        browser.close();meter.count('browser_close')
        finally:
            with meter.phase('source_save'):
                result['supplement_vault']=retain_supplement(db,key,job)


def main():
    job=sys.argv[1]
    result=dict(job_id=job,protected_map=False,actual_login_posts=0)
    with PilotTelemetry() as meter, meter.browser_actions():
        try:source(job,meter,result)
        except Exception as exc:result['failure']=failure(exc)
    put('source-metrics.json',meter.report())
    put('source-receipt.json',result)
    print('QA_SOURCE',job,result.get('stage'),result.get('numeric_read'),result.get('failure'),flush=True)


if __name__=='__main__':
    main()
