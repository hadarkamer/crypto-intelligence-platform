"""Bounded one-service QA listener. Secrets are passed only to their respective worker."""
import base64
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from threading import Thread
import time
from zipfile import ZipFile

HERE=Path(__file__).resolve().parent
STAND=HERE.parent
ROOT=Path('/tmp/cg-budget')
SERVICE='decision-hub-coinglass-pilot-20261125'
BRANCH='qa/coinglass-basic-numeric-20261126f'
CLEAN=('PATH','HOME','LANG','LD_LIBRARY_PATH','VIRTUAL_ENV','PLAYWRIGHT_BROWSERS_PATH','XDG_CACHE_HOME')
SOURCE=('DATABASE_URL','COINGLASS_SESSION_KEY','COINGLASS_LOGIN_EMAIL')

def load(name,default=None):
    f=ROOT/name
    return json.loads(f.read_text()) if f.is_file() else default

def emit(k,data=None):
    print('SINGLE_QA_'+k, json.dumps(data,separators=(',',':')) if data is not None else '',flush=True)

def run_child(script,args,env,timeout):
    import signal
    with (ROOT/(script.stem+'.log')).open('wb') as log:
        proc=subprocess.Popen([sys.executable,'-B','-u',str(script),*args],env=env,stdin=subprocess.DEVNULL,
                      stdout=log,stderr=subprocess.STDOUT,close_fds=True,start_new_session=True)
        try:return proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid,signal.SIGKILL);proc.wait(timeout=5)
            raise ValueError('bounded_worker_timeout') from None

def settle(job,clean,source_env,deadline):
    from qa_job_server import write_once
    source=None; result=None; publication=None
    err='source_not_readable'
    begun=time.monotonic()
    try:
        emit('SOURCE_START',job)
        try:
            run_child(HERE/'single_source.py',[job],clean|source_env,min(340,max(1,deadline-time.time()-35)))
        except ValueError as error:
            if str(error)!='bounded_worker_timeout':raise
        source=load('source-receipt.json') or {'missing_receipt':True}
        emit('SOURCE_DONE',dict(job=job,stage=source.get('stage'),numeric=source.get('numeric_read'),
                                failure=source.get('failure')))
        ref=source.get('market_vault') or {}
        if (source.get('failure') or source.get('numeric_read')!='verified_basic' or not source.get('account_positive')
                or ref.get('job')!=job or ref.get('readback') is not True):
            from qa_job_server import read_checked
            ref=read_checked('basic-checkpoint.json',2048)
            if ref.get('job')!=job:
                raise ValueError('invalid_basic_checkpoint_job')
            source['recovery']='reopen_closed_basic_on_fresh_connection'
            source['chart_sha256']=ref['chart_sha256']
        err='numeric_publication_failed'
        db_only={k:source_env[k] for k in ('DATABASE_URL','COINGLASS_SESSION_KEY')}
        run_child(HERE/'numeric_publish.py',[job,ref['sha256'],str(ref['oid'])],
                  clean|db_only,min(20,max(1,deadline-time.time()-2)))
        publication=load('numeric-publication.json')
        if (not publication or publication.get('job_id')!=job or publication.get('source_sha256')!=ref['sha256']
                or publication.get('chart_sha256')!=source.get('chart_sha256')):
            raise ValueError('numeric_publication_incomplete')
        result=publication['result']
        if (result.get('run_id')!=job or (result.get('symbol'),result.get('heatmap_model'),result.get('timeframe'))
                !=('BTC',1,'12H')):raise ValueError('numeric_result_identity_invalid')
    except Exception as exc:
        result=None
        emit('SETTLE_ERROR',{'stage':err,'class':type(exc).__name__,
             'code':str(exc) if re.fullmatch('[a-z_]{1,76}',str(exc)) else None})
    source=source or {'missing_receipt':True}
    out=dict(job_id=job,source_receipt=source,snapshot=(result or {}).get('basic_snapshot') or load('snapshot.json'),
             supplement=load('market-supplement.json'),metrics=dict(source=load('source-metrics.json'),
                collector_wall_seconds=round(time.monotonic()-begun,3)),analysis=None,
             model_requests=0,numeric_publication=publication,application_state='ready' if result else 'failed',
             chart_png_b64=None,chart_sha256=None)
    png=next((p for n in ('chart-final.png','chart-observation.png') if (p:=ROOT/n).is_file()),None)
    if png and png.stat().st_size<=1300000:
        raw=png.read_bytes();out.update(chart_png_b64=base64.b64encode(raw).decode(),chart_sha256=sha256(raw).hexdigest())
    if len(json.dumps(out).encode())>3*1024**2:out['chart_png_b64']=None
    write_once('qa-export.json',out)
    write_once('ui_final.json',dict(job_id=job,status=out['application_state'],result=result,
        error=None if result else {'code':err,'message':'Verified collection is incomplete. See the job-bound QA receipt.'}))
    emit('FINISHED',{'job_id':job,'state':out['application_state'],'model_calls':0})

def serve():
    os.umask(0o077)
    if os.environ.get('RENDER_SERVICE_NAME')!=SERVICE or os.environ.get('RENDER_GIT_BRANCH')!=BRANCH:
        raise ValueError('isolated_service_required')
    service=os.environ['RENDER_SERVICE_ID']
    if service=='srv-dakhacou01pc73f58aug' or not re.fullmatch('srv-[a-z0-9]+',service):raise ValueError('unexpected_service')
    ROOT.mkdir(mode=0o700,exist_ok=True)
    for key in ('source','native'):
        if (ROOT/('started-'+key)).exists():raise ValueError('same_instance_restart')
    (ROOT/'started-native').touch(mode=0o600,exist_ok=False)
    sys.path[:0]=[str(HERE),str(STAND),str(STAND.parent)]
    from qa_job_server import make_server,write_once
    pin=json.loads((HERE/'app-pin.json').read_text())
    now=time.time()
    lifespan=11.5*60
    deadline=now+lifespan
    write_once('trial.json',{'service':service,'created_epoch':now-1})
    write_once('ui-ready.json',dict(pin,service=service,deadline=deadline))
    clean={k:os.environ[k] for k in CLEAN if k in os.environ}
    clean['PYTHONPATH']=os.pathsep.join((str(HERE),str(STAND),str(STAND.parent)))
    clean['PYTHONDONTWRITEBYTECODE']='1'
    clean['TZ']='UTC'
    source_env={k:os.environ.pop(k) for k in SOURCE if k in os.environ}
    port=int(os.environ.get('PORT','10000'))
    def wait_one():
        from cycle_job import read_job
        while time.time()<deadline-145:
            try:job=read_job(ROOT,service);break
            except FileNotFoundError:time.sleep(.15)
        else:return
        (ROOT/'trial-started').touch(mode=0o600,exist_ok=False)
        settle(job,clean,source_env,deadline)
    with make_server(port,service) as server:
        Thread(target=wait_one,daemon=True).start()
        emit('ARMED',{'run_id':pin['run_id'],'service':service,'revision':os.environ.get('RENDER_GIT_COMMIT')})
        server.serve_forever()

if __name__=='__main__':serve()
