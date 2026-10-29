"""Build-only Model 3 lab. Default mode validates without a source scan."""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
LAB_SERVICE = 'srv-dbaf3grbc2fs73b5iogg'
OS_KEYS = ('PATH','HOME','TMPDIR','LANG','LC_ALL','TZ','SSL_CERT_FILE',
           'SSL_CERT_DIR','REQUESTS_CA_BUNDLE','PLAYWRIGHT_BROWSERS_PATH')
AUTH_KEYS = ('COINGLASS_COOKIE_HEADER','COINGLASS_STORAGE_STATE_JSON')


def child_environment(source=False):
    env = {key: os.environ[key] for key in OS_KEYS if key in os.environ}
    env.update(PYTHONUNBUFFERED='1',COLLECTION_BRIDGE_ENABLED='0',
               DATABASE_URL='',OPENAI_API_KEY='',SUPABASE_SERVICE_ROLE_KEY='',
               COINGLASS_COLLECTOR_TOKEN='',MODEL1_NETWORK_DIAGNOSTICS='0')
    if source:
        env.update({key: os.environ[key] for key in AUTH_KEYS if key in os.environ})
    return env


def check_target():
    if os.environ.get('RENDER_SERVICE_ID') != LAB_SERVICE or os.environ.get('RENDER_SERVICE_TYPE') != 'static':
        raise RuntimeError('render_lab_wrong_service')
    mode = os.environ.get('CG_RENDER_LAB_MODE','prepare')
    if mode not in ('prepare','source'):
        raise RuntimeError('render_lab_invalid_mode')
    return mode


def publish_status(source_passed):
    public = HERE/'hover-check-public'
    if public.is_symlink():raise RuntimeError('render_lab_public_symlink')
    public.mkdir(exist_ok=True)
    if any(item.name != 'index.html' or item.is_symlink() for item in public.iterdir()):
        raise RuntimeError('render_lab_unexpected_public_artifact')
    message = ('Paired 12H and 24H source checks, exact rows, map pixels and restoration passed.'
               if source_passed else 'Preparation and isolated tests passed. No live source comparison has been run by this build.')
    (public/'index.html').write_text(
        '<!doctype html><html lang="en"><meta charset="utf-8">'
        '<meta name="robots" content="noindex,nofollow">'
        '<title>Isolated Model 3 rendering check</title><main>'
        '<h1>Isolated Model 3 rendering check</h1><p>'+message+'</p>'
        '<p>No application database, AI calls or background worker.</p>'
        '<p>Laboratory only. Production remains unchanged. This is not a production capacity benchmark.</p>'
        '</main></html>',encoding='utf-8')


def main():
    mode=check_target()
    source_env=child_environment(source=True)
    has_session=any(source_env.get(key,'').strip() for key in AUTH_KEYS)
    print('CG29_LAB_PREFLIGHT '+json.dumps({'mode':mode,'service':LAB_SERVICE,
          'source_session_configured':has_session,'worker_started':False,
          'database_writes':0,'ai_calls':0}),flush=True)
    if mode=='source' and not has_session:
        raise RuntimeError('render_lab_source_session_missing')
    build_env=child_environment()
    subprocess.run([sys.executable,str(HERE/'prepare.py')],cwd=ROOT,
                   env=build_env,check=True,timeout=900)
    lab=HERE/'rendering_lab'
    node=shutil.which('node')
    if not node:raise RuntimeError('render_lab_node_unavailable')
    # Public code assets only; no account requests, cookies, or market data.
    sys.path.insert(0,str(lab))
    import candidate
    import requests
    assets=Path('/tmp/cg29-m3-review');assets.mkdir(mode=0o700,exist_ok=True)
    for url,expected in ((candidate.ASSET_URL,candidate.ASSET_SHA256),
                         (candidate.PIXI_URL,candidate.PIXI_SHA256)):
        response=requests.get(url,timeout=30,allow_redirects=False)
        if response.status_code!=200:raise RuntimeError('render_lab_asset_download_failed')
        if hashlib.sha256(response.content).hexdigest()!=expected:
            raise RuntimeError('render_lab_public_asset_changed')
        target=assets/url.rsplit('/',1)[-1]
        target.write_bytes(response.content);target.chmod(0o600)
    subprocess.run([sys.executable,'test_candidate_offline.py','-v'],cwd=lab,
                   env=build_env,check=True,timeout=30)
    subprocess.run([node,'--check','patched_chart_for_syntax.js'],cwd=lab,
                   env=build_env,check=True,timeout=30)
    subprocess.run([node,'test_render_gate.cjs'],cwd=lab,
                   env=build_env,check=True,timeout=30)
    print('CG29_LAB_OFFLINE passed=true actual_source_assets_verified=true',flush=True)
    if mode=='prepare':
        publish_status(False)
        print('CG29_LAB_FINISHED mode=prepare source_scans=0 production_changed=false',flush=True)
        return
    output=Path('/tmp')/('cg29-rendering-ab-'+str(time.time_ns()))
    command=[sys.executable,str(lab/'rendering_ab_check.py'),'--output',str(output),
             '--timeframes','12h,24h','--fps','0.5']
    affinity=None
    if hasattr(os,'sched_getaffinity') and hasattr(os,'sched_setaffinity'):
        current=os.sched_getaffinity(0)
        if not current:raise RuntimeError('render_lab_empty_affinity')
        os.sched_setaffinity(0,{min(current)});affinity=sorted(os.sched_getaffinity(0))
    print('CG29_LAB_ISOLATION '+json.dumps({'diagnostic_cpu_affinity':affinity,
          'worker_started':False,'database_writes':0,'ai_calls':0}),flush=True)
    result=subprocess.run(command,cwd=ROOT,env=source_env,timeout=900)
    report_path=output/'report.json'
    if not report_path.exists():raise RuntimeError('render_lab_report_missing')
    report=json.loads(report_path.read_text())
    print('CG29_LAB_REPORT '+json.dumps(report,separators=(',',':')),flush=True)
    if result.returncode or report.get('status')!='passed':
        raise RuntimeError('render_lab_acceptance_not_passed')
    publish_status(True)
    print('CG29_LAB_FINISHED mode=source passed=true production_changed=false',flush=True)


if __name__=='__main__':main()
