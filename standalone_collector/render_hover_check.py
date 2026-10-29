"""Build-only source acceptance of one immutable integrated collector revision.

This launcher belongs only to the existing isolated static-site laboratory.
It starts no collector worker, queue, database or AI service and publishes only
one generic status page. Full source images remain in a private temp directory.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

SERVICE='srv-dbaf3grbc2fs73b5iogg'
REVISION='a6feaae1158b6ec7dd2b6f54e54ed4b40c5eb2d0'
BRANCH='fix/model3-render-lifecycle-20261029'
REPOSITORY='https://github.com/hadarkamer/crypto-intelligence-platform.git'
HERE=Path(__file__).resolve().parent
OS_KEYS=('PATH','HOME','TMPDIR','LANG','LC_ALL','TZ','SSL_CERT_FILE','SSL_CERT_DIR',
         'REQUESTS_CA_BUNDLE','PLAYWRIGHT_BROWSERS_PATH')
AUTH_KEYS=('COINGLASS_COOKIE_HEADER','COINGLASS_STORAGE_STATE_JSON')


def child_environment(source=False):
    clean={key:os.environ[key] for key in OS_KEYS if key in os.environ}
    clean.update(PYTHONUNBUFFERED='1',GIT_TERMINAL_PROMPT='0',COLLECTION_BRIDGE_ENABLED='0',
                 DATABASE_URL='',OPENAI_API_KEY='',SUPABASE_SERVICE_ROLE_KEY='',COINGLASS_COLLECTOR_TOKEN='')
    if source:
        clean.update({key:os.environ[key] for key in AUTH_KEYS if key in os.environ})
    return clean


def publish_status(passed,mode):
    public=HERE/'hover-check-public'
    if public.is_symlink():raise RuntimeError('integration_lab_public_symlink')
    public.mkdir(exist_ok=True)
    if any(p.name!='index.html' or p.is_symlink() for p in public.iterdir()):
        raise RuntimeError('integration_lab_unexpected_public_artifact')
    text=('Integrated source acceptance passed for all three models and 12H/24H/48H.'
          if passed else 'Integrated build preparation passed; no source scans in this build.')
    (public/'index.html').write_text('<!doctype html><html lang="en"><meta charset="utf-8">'
        '<meta name="robots" content="noindex,nofollow"><title>Isolated collector verification</title>'
        '<main><h1>Isolated collector verification</h1><p>'+text+'</p>'
        '<p>No application database writes, AI calls or running worker. '
        'This is not application UI acceptance or a production capacity measurement.</p></main></html>',encoding='utf-8')


def main():
    os.umask(0o077)
    if os.environ.get('RENDER_SERVICE_ID')!=SERVICE or os.environ.get('RENDER_SERVICE_TYPE')!='static':
        raise RuntimeError('integration_lab_wrong_service')
    mode=os.environ.get('CG_RENDER_LAB_MODE','prepare')
    if mode not in ('prepare','source'):raise RuntimeError('integration_lab_invalid_mode')
    clean=child_environment();source_env=child_environment(True)
    configured=any(source_env.get(key,'').strip() for key in AUTH_KEYS)
    print('CG29_INTEGRATION_PREFLIGHT '+json.dumps({'revision':REVISION,'mode':mode,
        'source_session_configured':configured,'worker_started':False,'ai_calls':0,'database_writes':0}),flush=True)
    if mode=='source' and not configured:raise RuntimeError('integration_lab_session_missing')
    with tempfile.TemporaryDirectory(prefix='cg29-integration-code-') as temporary:
        source=Path(temporary)/'source'
        subprocess.run(['git','clone','--no-checkout','--filter=blob:none','--single-branch','--depth','1',
                        '--branch',BRANCH,REPOSITORY,str(source)],env=clean,check=True,timeout=180)
        actual=subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,env=clean,text=True,timeout=20).strip()
        if actual!=REVISION:raise RuntimeError('integration_lab_revision_changed')
        subprocess.run(['git','checkout','--detach',REVISION],cwd=source,env=clean,check=True,timeout=180)
        subprocess.run([sys.executable,'standalone_collector/prepare.py'],cwd=source,
                       env=clean,check=True,timeout=900)
        runtime=source/'standalone_collector/runtime'
        reader=runtime/'heatmap_hover.py'
        reader_sha=hashlib.sha256(reader.read_bytes()).hexdigest()
        if reader_sha!='693aa6739a8b1f50d0550cd802dde99cff4c68edfad6c643c1adf8d6bc05f820':
            raise RuntimeError('integration_lab_numerical_reader_changed')
        hashes={name:hashlib.sha256((runtime/name).read_bytes()).hexdigest() for name in
            ('heatmap_hover.py','market_vision/coinglass_heatmap_capture.py','model3_rendering.py',
             'model3_clean_frame.py','model3_render_gate.js')}
        print('CG29_INTEGRATION_BUILD '+json.dumps({'passed':True,'revision':REVISION,
              'runtime_hashes':hashes,'source_scans':0}),flush=True)
        if mode=='prepare':
            publish_status(False,mode)
            print('CG29_INTEGRATION_FINISHED mode=prepare source_scans=0 production_changed=false',flush=True)
            return
        if hasattr(os,'sched_getaffinity') and hasattr(os,'sched_setaffinity'):
            available=os.sched_getaffinity(0)
            if not available:raise RuntimeError('integration_lab_empty_affinity')
            os.sched_setaffinity(0,{min(available)})
        output=Path('/tmp')/('cg29-integrated-source-'+str(time.time_ns()))
        print('CG29_INTEGRATION_STARTED '+json.dumps({'revision':REVISION,
              'cpu_affinity':sorted(os.sched_getaffinity(0)) if hasattr(os,'sched_getaffinity') else None,
              'requested_maps':9,'ai_calls':0,'database_writes':0}),flush=True)
        completed=subprocess.run([sys.executable,'standalone_collector/hover_source_check.py',
            '--output',str(output)],cwd=source,env=source_env,timeout=1770)
        report_path=output/'summary.json'
        if not report_path.is_file():raise RuntimeError('integration_lab_summary_missing')
        reports=json.loads(report_path.read_text())
        expected={(m,tf) for m in (1,2,3) for tf in ('12H','24H','48H')}
        identities=[(r.get('model'),r.get('timeframe')) for r in reports]
        passed=(completed.returncode==0 and len(reports)==9 and set(identities)==expected
                and all(r.get('status')=='captured' for r in reports))
        print('CG29_INTEGRATION_REPORT '+json.dumps({'revision':REVISION,'passed':passed,
            'reports':reports,'runtime_hashes':hashes,'database_writes':0,'ai_calls':0},separators=(',',':')),flush=True)
        if not passed:raise RuntimeError('integration_lab_acceptance_not_passed')
        publish_status(True,mode)
        print('CG29_INTEGRATION_FINISHED mode=source passed=true production_changed=false',flush=True)


if __name__=='__main__':main()
