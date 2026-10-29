"""Build-only dispatch to one immutable Model 3 lab revision; never production."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

SERVICE='srv-dbaf3grbc2fs73b5iogg'
REVISION='307d55c1187a86f0ac66981be6ab36623fe0ac4c'
BRANCH='test/model3-rendering-lab-20261029'
REPOSITORY='https://github.com/hadarkamer/crypto-intelligence-platform.git'
HERE=Path(__file__).resolve().parent


def main():
    if os.environ.get('RENDER_SERVICE_ID')!=SERVICE or os.environ.get('RENDER_SERVICE_TYPE')!='static':
        raise RuntimeError('render_lab_wrong_service')
    mode=os.environ.get('CG_RENDER_LAB_MODE','prepare')
    if mode not in ('prepare','source'):raise RuntimeError('render_lab_invalid_mode')
    allowed=('PATH','HOME','TMPDIR','LANG','LC_ALL','TZ','SSL_CERT_FILE','SSL_CERT_DIR',
             'REQUESTS_CA_BUNDLE','PLAYWRIGHT_BROWSERS_PATH')
    clean={key:os.environ[key] for key in allowed if key in os.environ}
    clean.update(PYTHONUNBUFFERED='1',GIT_TERMINAL_PROMPT='0',COLLECTION_BRIDGE_ENABLED='0',
                 DATABASE_URL='',OPENAI_API_KEY='',SUPABASE_SERVICE_ROLE_KEY='',COINGLASS_COLLECTOR_TOKEN='')
    with tempfile.TemporaryDirectory(prefix='cg29-lab-build-') as temporary:
        source=Path(temporary)/'source'
        subprocess.run(['git','clone','--no-checkout','--filter=blob:none','--single-branch','--depth','1',
                        '--branch',BRANCH,REPOSITORY,str(source)],env=clean,check=True,timeout=180)
        actual=subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,env=clean,text=True,timeout=20).strip()
        if actual!=REVISION:raise RuntimeError('render_lab_revision_changed')
        subprocess.run(['git','checkout','--detach',REVISION],cwd=source,env=clean,check=True,timeout=180)
        experiment=dict(clean,RENDER_SERVICE_ID=SERVICE,RENDER_SERVICE_TYPE='static',CG_RENDER_LAB_MODE=mode)
        for key in ('COINGLASS_COOKIE_HEADER','COINGLASS_STORAGE_STATE_JSON'):
            if key in os.environ:experiment[key]=os.environ[key]
        print('CG29_LAB_PIN '+json.dumps({'revision':REVISION,'mode':mode,'worker_started':False,
              'database_writes':0,'ai_calls':0}),flush=True)
        subprocess.run([sys.executable,'standalone_collector/render_hover_check.py'],cwd=source,
                       env=experiment,check=True,timeout=1900)
        page=source/'standalone_collector/hover-check-public/index.html'
        if not page.is_file() or page.is_symlink():raise RuntimeError('render_lab_status_page_missing')
        public=HERE/'hover-check-public'
        if public.is_symlink():raise RuntimeError('render_lab_public_symlink')
        public.mkdir(exist_ok=True)
        if any(item.name!='index.html' or item.is_symlink() for item in public.iterdir()):
            raise RuntimeError('render_lab_unexpected_public_artifact')
        shutil.copyfile(page,public/'index.html')


if __name__=='__main__':main()
