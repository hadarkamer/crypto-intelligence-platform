"""Bounded source acceptance, isolated static-site laboratory only.

No collector worker, application database, AI key, source API or public evidence.
The unchanged source-only probe is used. Full records stay in private /tmp files.
"""
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

SERVICE='srv-dbaf3grbc2fs73b5iogg'
REVISION='d9ff5089a6865eb434047d9e34c60ca485c28c17'
BRANCH='fix/model3-render-lifecycle-20261029'
REPOSITORY='https://github.com/hadarkamer/crypto-intelligence-platform.git'
HERE=Path(__file__).resolve().parent
AUTH_KEYS=('COINGLASS_COOKIE_HEADER','COINGLASS_STORAGE_STATE_JSON')


def environment(source=False):
    keys=('PATH','HOME','TMPDIR','LANG','LC_ALL','TZ','SSL_CERT_FILE','SSL_CERT_DIR',
          'REQUESTS_CA_BUNDLE','PLAYWRIGHT_BROWSERS_PATH')
    clean={k:os.environ[k] for k in keys if k in os.environ}
    clean.update(PYTHONUNBUFFERED='1',GIT_TERMINAL_PROMPT='0',COLLECTION_BRIDGE_ENABLED='0',
        DATABASE_URL='',OPENAI_API_KEY='',SUPABASE_SERVICE_ROLE_KEY='',COINGLASS_COLLECTOR_TOKEN='')
    if source:clean.update({k:os.environ[k] for k in AUTH_KEYS if k in os.environ})
    return clean


def emit(event,payload):
    print('CG29_NATIVE_INTEGRATION '+json.dumps({'event':event,**payload},separators=(',',':'),allow_nan=False),flush=True)


def probe(source,env,model,tf,destination,attempt):
    destination.mkdir(mode=0o700,parents=True,exist_ok=False)
    command=[sys.executable,'standalone_collector/hover_source_check.py','--single',str(model),tf,'--output',str(destination)]
    child=dict(env,COLLECTION_HEATMAP_MODEL=str(model))
    logfile=destination/'process-private.log'
    report={'model':model,'timeframe':tf,'attempt':attempt,'status':'failed','code':'probe_process_failed'}
    timed_out=False;started=time.monotonic()
    with logfile.open('w') as log:
        process=subprocess.Popen(command,cwd=source,env=child,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        try:
            process.wait(timeout=180)
        except subprocess.TimeoutExpired:
            timed_out=True
        finally:
            try:os.killpg(process.pid,signal.SIGTERM)
            except ProcessLookupError:pass
            try:process.wait(timeout=5)
            except subprocess.TimeoutExpired:pass
            try:os.killpg(process.pid,signal.SIGKILL)
            except ProcessLookupError:pass
            process.wait(timeout=5)
    path=destination/'report.json'
    if path.is_file() and not timed_out:
        report=json.loads(path.read_text());report['attempt']=attempt
    else:report['elapsed_seconds']=round(time.monotonic()-started,3)
    records=[]
    for line in logfile.read_text(errors='replace').splitlines():
        if line.startswith('MODEL3_RENDER '):
            try:value=json.loads(line[len('MODEL3_RENDER '):])
            except ValueError:continue
            records.append({k:value[k] for k in ('status','reason') if k in value})
    report['render_policy']=records
    if report.get('status')=='captured':
        raw=json.loads((destination/'hover.json').read_text())
        report['row_digest']=hashlib.sha256(json.dumps(raw['rows'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
        report['saved_row_count']=len(raw['rows'])
        if model==3:
            active=sum(r=={'status':'active','reason':'verified_pixi_init_hook'} for r in records)
            if active!=1 or any(r.get('status')!='active' for r in records):
                report.update(status='failed',code='integrated_acceleration_not_verified')
    emit('map_completed',report)
    return report


def main():
    os.umask(0o077)
    if os.environ.get('RENDER_SERVICE_ID')!=SERVICE or os.environ.get('RENDER_SERVICE_TYPE')!='static':
        raise RuntimeError('native_lab_wrong_service')
    mode=os.environ.get('CG_RENDER_LAB_MODE','prepare')
    if mode not in ('prepare','source'):raise RuntimeError('native_lab_invalid_mode')
    clean=environment();source_env=environment(True)
    configured=any(source_env.get(k,'').strip() for k in AUTH_KEYS)
    emit('preflight',{'revision':REVISION,'mode':mode,'source_session_configured':configured,
                     'worker_started':False,'ai_calls':0,'database_writes':0})
    if mode=='source' and not configured:raise RuntimeError('native_lab_session_missing')
    with tempfile.TemporaryDirectory(prefix='cg29-native-code-') as temporary:
        source=Path(temporary)/'source'
        subprocess.run(['git','clone','--no-checkout','--filter=blob:none','--single-branch','--depth','1',
            '--branch',BRANCH,REPOSITORY,str(source)],env=clean,check=True,timeout=180)
        actual=subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,env=clean,text=True,timeout=20).strip()
        if actual!=REVISION:raise RuntimeError('native_lab_revision_changed')
        subprocess.run(['git','checkout','--detach',REVISION],cwd=source,env=clean,check=True,timeout=180)
        subprocess.run([sys.executable,'standalone_collector/prepare.py'],cwd=source,env=clean,check=True,timeout=900)
        runtime=source/'standalone_collector/runtime'
        hashes={name:hashlib.sha256((runtime/name).read_bytes()).hexdigest() for name in
            ('heatmap_hover.py','market_vision/coinglass_heatmap_capture.py','model3_rendering.py',
             'model3_clean_frame.py','model3_render_gate.js')}
        if hashes['heatmap_hover.py']!='693aa6739a8b1f50d0550cd802dde99cff4c68edfad6c643c1adf8d6bc05f820':
            raise RuntimeError('native_lab_numerical_reader_changed')
        emit('build_passed',{'revision':REVISION,'runtime_hashes':hashes,'source_scans':0})
        reports=[];latest={};passed=False
        if mode=='source':
            if hasattr(os,'sched_getaffinity') and hasattr(os,'sched_setaffinity'):
                available=os.sched_getaffinity(0)
                if not available:raise RuntimeError('native_lab_empty_affinity')
                os.sched_setaffinity(0,{min(available)})
            out=Path(tempfile.mkdtemp(prefix='cg29-native-source-'))
            emit('source_started',{'cpu_affinity':sorted(os.sched_getaffinity(0)) if hasattr(os,'sched_getaffinity') else None,
                'requested_maps':9,'ai_calls':0,'database_writes':0})
            for model in (3,1,2):
                for tf in ('12H','24H','48H'):
                    r=probe(source,source_env,model,tf,out/f'model{model}-{tf}',1)
                    reports.append(r);latest[(model,tf)]=r
                    if (r.get('source_capture_state') or {}).get('challenge_visible') or r.get('blocking_condition')=='challenge':
                        emit('blocked',{'reason':'source_access_gate'});raise RuntimeError('native_lab_access_gate')
            # One later repeat only for a source that explicitly returned no-data.
            # Keep the failed attempt. Never retry around access gates or math checks.
            for key,r in list(latest.items()):
                if r.get('status')!='captured' and (r.get('source_readiness') or {}).get('reason')=='no-data':
                    model,tf=key
                    again=probe(source,source_env,model,tf,out/f'model{model}-{tf}-repeat',2)
                    reports.append(again);latest[key]=again
                    if (again.get('source_capture_state') or {}).get('challenge_visible'):
                        raise RuntimeError('native_lab_access_gate')
            expected={(m,tf) for m in (1,2,3) for tf in ('12H','24H','48H')}
            passed=set(latest)==expected and all(r.get('status')=='captured' for r in latest.values())
            emit('finished',{'revision':REVISION,'passed':passed,'reports':reports,
                'runtime_hashes':hashes,'ai_calls':0,'database_writes':0,'production_changed':False})
            if not passed:raise RuntimeError('native_lab_acceptance_not_passed')
        public=HERE/'hover-check-public'
        if public.is_symlink():raise RuntimeError('native_lab_public_symlink')
        public.mkdir(exist_ok=True)
        if any(p.name!='index.html' or p.is_symlink() for p in public.iterdir()):
            raise RuntimeError('native_lab_unexpected_public_artifact')
        message=('All nine integrated source maps passed. No application UI acceptance has been performed.'
                 if passed else 'Preparation passed. No source scan in this build.')
        (public/'index.html').write_text('<!doctype html><meta charset="utf-8"><meta name="robots" content="noindex,nofollow">'
            '<title>Isolated collector verification</title><h1>Isolated collector verification</h1><p>'+message+'</p>'
            '<p>No AI calls, application database writes or production deployment.</p>')
        if mode=='prepare':emit('finished',{'mode':mode,'source_scans':0,'production_changed':False})


if __name__=='__main__':main()
