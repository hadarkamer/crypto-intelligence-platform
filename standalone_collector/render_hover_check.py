"""One bounded source-asset diagnostic in the existing isolated laboratory."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

SERVICE='srv-dbaf3grbc2fs73b5iogg'
REVISION='f585af04841e2e81738bdda4783214f38952b4f5'
BRANCH='fix/model3-render-lifecycle-20261029'
REPOSITORY='https://github.com/hadarkamer/crypto-intelligence-platform.git'
HERE=Path(__file__).resolve().parent
DIAGNOSTIC=r'''
import contextlib,hashlib,json,os,sys,time
from pathlib import Path
from urllib.parse import urlsplit
os.environ['COLLECTION_HEATMAP_MODEL']='3'
sys.path.insert(0,str(Path.cwd()/'standalone_collector/runtime'))
from playwright.sync_api import Browser
import model3_rendering as rendering
from market_vision.coinglass_heatmap_capture import capture_heatmaps
seen={};original=Browser.new_context
class DiagnosticComplete(RuntimeError):pass

def record(request):
    if request.resource_type!='script' or len(seen)>=120:return
    parsed=urlsplit(request.url)
    if parsed.hostname not in ('s3.coinglass.com','www.coinglass.com','coinglass.com'):return
    path=parsed.path
    if '/_next/' not in path or not path.endswith('.js') or len(path)>256:return
    key=parsed.scheme+'://'+parsed.netloc+path
    seen[key]={'url_without_query':key,'has_query':bool(parsed.query)}

def new_context(browser,*args,**kwargs):
    context=original(browser,*args,**kwargs)
    context.on('request',record)
    return context

@contextlib.contextmanager
def observe(page,model,geometry):
    state=getattr(page.context,rendering._STATE,{})
    entries=page.evaluate('() => globalThis.__cg29RenderLab ? globalThis.__cg29RenderLab.snapshot() : null')
    print('CG29_ASSET_DIAGNOSTIC '+json.dumps({'model':model,'route_state':state,
        'hook_present':entries is not None,'chart_hooks':len(entries) if isinstance(entries,list) else None,
        'script_requests':list(seen.values()),'numerical_scans':0,'ai_calls':0,'database_writes':0},separators=(',',':')),flush=True)
    raise DiagnosticComplete('diagnostic_completed_before_numerical_scan')
    yield None

Browser.new_context=new_context
rendering.observation=observe
out=Path('/tmp/cg29-assets-private')
out.mkdir(mode=0o700,exist_ok=True)
try:
    capture_heatmaps(out,timeframes=('12h',))
except DiagnosticComplete:
    print('CG29_ASSET_FINISHED captured_source_routes=true numerical_scans=0',flush=True)
else:
    raise RuntimeError('asset_diagnostic_observation_missing')
'''


def env(source=False):
    keys=('PATH','HOME','TMPDIR','LANG','LC_ALL','TZ','SSL_CERT_FILE','SSL_CERT_DIR',
          'REQUESTS_CA_BUNDLE','PLAYWRIGHT_BROWSERS_PATH')
    clean={k:os.environ[k] for k in keys if k in os.environ}
    clean.update(PYTHONUNBUFFERED='1',GIT_TERMINAL_PROMPT='0',COLLECTION_BRIDGE_ENABLED='0',
        DATABASE_URL='',OPENAI_API_KEY='',SUPABASE_SERVICE_ROLE_KEY='',COINGLASS_COLLECTOR_TOKEN='')
    if source:
        clean.update({k:os.environ[k] for k in ('COINGLASS_COOKIE_HEADER','COINGLASS_STORAGE_STATE_JSON') if k in os.environ})
    return clean


def main():
    os.umask(0o077)
    if os.environ.get('RENDER_SERVICE_ID')!=SERVICE or os.environ.get('RENDER_SERVICE_TYPE')!='static':
        raise RuntimeError('asset_lab_wrong_service')
    mode=os.environ.get('CG_RENDER_LAB_MODE','prepare')
    if mode not in ('prepare','source'):raise RuntimeError('asset_lab_invalid_mode')
    clean=env();source_env=env(True)
    if mode=='source' and not any(source_env.get(k,'').strip() for k in ('COINGLASS_COOKIE_HEADER','COINGLASS_STORAGE_STATE_JSON')):
        raise RuntimeError('asset_lab_session_missing')
    with tempfile.TemporaryDirectory(prefix='cg29-asset-code-') as temporary:
        source=Path(temporary)/'source'
        subprocess.run(['git','clone','--no-checkout','--filter=blob:none','--single-branch','--depth','1',
            '--branch',BRANCH,REPOSITORY,str(source)],env=clean,check=True,timeout=180)
        actual=subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,env=clean,text=True,timeout=20).strip()
        if actual!=REVISION:raise RuntimeError('asset_lab_revision_changed')
        subprocess.run(['git','checkout','--detach',REVISION],cwd=source,env=clean,check=True,timeout=180)
        subprocess.run([sys.executable,'standalone_collector/prepare.py'],cwd=source,env=clean,check=True,timeout=900)
        if mode=='source':
            subprocess.run([sys.executable,'-c',DIAGNOSTIC],cwd=source,env=source_env,check=True,timeout=240)
        public=HERE/'hover-check-public'
        if public.is_symlink():raise RuntimeError('asset_lab_public_symlink')
        public.mkdir(exist_ok=True)
        if any(p.name!='index.html' or p.is_symlink() for p in public.iterdir()):
            raise RuntimeError('asset_lab_unexpected_public_artifact')
        (public/'index.html').write_text('<!doctype html><meta charset="utf-8"><meta name="robots" content="noindex,nofollow">'
            '<title>Isolated diagnostic</title><h1>Isolated diagnostic</h1>'
            '<p>Source asset diagnostic completed. This is not collection acceptance.</p>'
            '<p>No numerical scan, AI call, database write or production deployment.</p>')


if __name__=='__main__':main()
