"""One focused Model 3/48H source check in the existing isolated lab.

The earlier full matrix and its failures remain in the logs. This is not a
replacement all-nine success report. No AI, app storage, worker or source API.
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


def environment(source=False):
    keys=('PATH','HOME','TMPDIR','LANG','LC_ALL','TZ','SSL_CERT_FILE','SSL_CERT_DIR',
          'REQUESTS_CA_BUNDLE','PLAYWRIGHT_BROWSERS_PATH')
    clean={k:os.environ[k] for k in keys if k in os.environ}
    clean.update(PYTHONUNBUFFERED='1',GIT_TERMINAL_PROMPT='0',COLLECTION_BRIDGE_ENABLED='0',
        DATABASE_URL='',OPENAI_API_KEY='',SUPABASE_SERVICE_ROLE_KEY='',COINGLASS_COLLECTOR_TOKEN='')
    if source:clean.update({k:os.environ[k] for k in ('COINGLASS_COOKIE_HEADER','COINGLASS_STORAGE_STATE_JSON') if k in os.environ})
    return clean


def emit(event,**payload):
    print('CG29_FOCUSED_INTEGRATION '+json.dumps({'event':event,**payload},separators=(',',':'),allow_nan=False),flush=True)


def main():
    os.umask(0o077)
    if os.environ.get('RENDER_SERVICE_ID')!=SERVICE or os.environ.get('RENDER_SERVICE_TYPE')!='static':
        raise RuntimeError('focused_lab_wrong_service')
    mode=os.environ.get('CG_RENDER_LAB_MODE','prepare')
    if mode not in ('prepare','source'):raise RuntimeError('focused_lab_invalid_mode')
    clean=environment();source_env=environment(True)
    if mode=='source' and not any(source_env.get(k,'').strip() for k in ('COINGLASS_COOKIE_HEADER','COINGLASS_STORAGE_STATE_JSON')):
        raise RuntimeError('focused_lab_session_missing')
    emit('preflight',revision=REVISION,mode=mode,worker_started=False,ai_calls=0,database_writes=0)
    passed=False
    with tempfile.TemporaryDirectory(prefix='cg29-focused-code-') as temporary:
        source=Path(temporary)/'source'
        subprocess.run(['git','clone','--no-checkout','--filter=blob:none','--single-branch','--depth','1',
            '--branch',BRANCH,REPOSITORY,str(source)],env=clean,check=True,timeout=180)
        actual=subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,env=clean,text=True,timeout=20).strip()
        if actual!=REVISION:raise RuntimeError('focused_lab_revision_changed')
        subprocess.run(['git','checkout','--detach',REVISION],cwd=source,env=clean,check=True,timeout=180)
        subprocess.run([sys.executable,'standalone_collector/prepare.py'],cwd=source,env=clean,check=True,timeout=900)
        runtime=source/'standalone_collector/runtime'
        hashes={name:hashlib.sha256((runtime/name).read_bytes()).hexdigest() for name in
            ('heatmap_hover.py','market_vision/coinglass_heatmap_capture.py','model3_rendering.py',
             'model3_clean_frame.py','model3_render_gate.js')}
        if hashes['heatmap_hover.py']!='693aa6739a8b1f50d0550cd802dde99cff4c68edfad6c643c1adf8d6bc05f820':
            raise RuntimeError('focused_lab_numerical_reader_changed')
        if mode=='source':
            if hasattr(os,'sched_getaffinity') and hasattr(os,'sched_setaffinity'):
                os.sched_setaffinity(0,{min(os.sched_getaffinity(0))})
            out=Path(tempfile.mkdtemp(prefix='cg29-focused-48h-'))
            logfile=out/'process-private.log'
            child=dict(source_env,COLLECTION_HEATMAP_MODEL='3')
            command=[sys.executable,'standalone_collector/hover_source_check.py','--single','3','48H','--output',str(out)]
            emit('source_started',model=3,timeframe='48H',requested_maps=1)
            timed_out=False
            with logfile.open('w') as log:
                process=subprocess.Popen(command,cwd=source,env=child,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
                try:process.wait(timeout=180)
                except subprocess.TimeoutExpired:timed_out=True
                finally:
                    try:os.killpg(process.pid,signal.SIGTERM)
                    except ProcessLookupError:pass
                    try:process.wait(timeout=5)
                    except subprocess.TimeoutExpired:pass
                    try:os.killpg(process.pid,signal.SIGKILL)
                    except ProcessLookupError:pass
                    process.wait(timeout=5)
            report_path=out/'report.json'
            report=json.loads(report_path.read_text()) if report_path.is_file() else {'status':'failed','code':'report_missing'}
            records=[]
            for line in logfile.read_text(errors='replace').splitlines():
                if line.startswith('MODEL3_RENDER '):
                    try:value=json.loads(line[len('MODEL3_RENDER '):])
                    except ValueError:continue
                    records.append({k:value[k] for k in ('status','reason') if k in value})
            passed=(not timed_out and report.get('status')=='captured' and report.get('model')==3
                    and report.get('timeframe')=='48H' and records==[{'status':'active','reason':'verified_pixi_init_hook'}])
            if report.get('status')=='captured':
                raw=json.loads((out/'hover.json').read_text())
                report['row_digest']=hashlib.sha256(json.dumps(raw['rows'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
                report['saved_row_count']=len(raw['rows'])
            emit('finished',revision=REVISION,passed=passed,report=report,render_policy=records,
                 runtime_hashes=hashes,timed_out=timed_out,ai_calls=0,database_writes=0,production_changed=False)
            if not passed:raise RuntimeError('focused_lab_acceptance_not_passed')
        public=HERE/'hover-check-public'
        if public.is_symlink():raise RuntimeError('focused_lab_public_symlink')
        public.mkdir(exist_ok=True)
        if any(p.name!='index.html' or p.is_symlink() for p in public.iterdir()):
            raise RuntimeError('focused_lab_unexpected_public_artifact')
        message=('Focused Model 3/48H integrated source check passed. Earlier full-matrix attempts remain in private logs.'
                 if passed else 'Preparation passed. No source scan in this build.')
        (public/'index.html').write_text('<!doctype html><meta charset="utf-8"><meta name="robots" content="noindex,nofollow">'
            '<title>Isolated collector verification</title><h1>Isolated collector verification</h1><p>'+message+'</p>'
            '<p>No AI calls, application database writes or production deployment. Not application UI acceptance.</p>')
        if mode=='prepare':emit('finished',mode=mode,source_scans=0,production_changed=False)


if __name__=='__main__':main()
