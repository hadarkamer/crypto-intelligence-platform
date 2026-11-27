"""Build and run one frozen QA package, using no account secrets during build."""
import base64
import hashlib
from io import BytesIO
import json
import os
from pathlib import Path
import subprocess
import sys
from zipfile import ZipFile

HERE=Path(__file__).resolve().parent
ROOT=HERE/'unpacked'
BROWSERS=HERE/'browsers'

def main():
    if os.getenv('RENDER_GIT_BRANCH')!='qa/coinglass-btc-once-20261127':raise ValueError('wrong_branch')
    if not os.getenv('RENDER_SERVICE_NAME','').startswith('cg-btc-once-20261127'):raise ValueError('wrong_service')
    if sys.argv[1]=='build':
        if any(os.environ.get(n) for n in ('DATABASE_URL','COINGLASS_SESSION_KEY','COINGLASS_LOGIN_EMAIL','COINGLASS_LOGIN_PASSWORD','OPENAI_API_KEY')):
            raise ValueError('secret_in_build')
        parts=sorted(HERE.glob('package-*.b64'))
        raw=base64.b64decode(''.join(p.read_text() for p in parts),validate=True)
        digest=hashlib.sha256(raw).hexdigest()
        if digest!=(HERE/'package.sha256').read_text().strip():raise ValueError('package_digest_mismatch')
        ROOT.mkdir(mode=0o700,exist_ok=True)
        with ZipFile(BytesIO(raw)) as archive:
            for item in archive.infolist():
                p=Path(item.filename)
                if p.is_absolute() or '..' in p.parts or item.file_size>2_000_000 or p.suffix not in ('.py','.json','.sql','.js'):
                    raise ValueError('unsafe_package_member')
                target=ROOT/p;target.parent.mkdir(parents=True,exist_ok=True)
                target.write_bytes(archive.read(item));target.chmod(0o600)
        for target,expected in json.loads((ROOT/'package-manifest.json').read_text()).items():
            if hashlib.sha256((ROOT/target).read_bytes()).hexdigest()!=expected:raise ValueError('source_digest_mismatch')
        subprocess.run([sys.executable,'-m','pip','install','--disable-pip-version-check','--no-input',
            'playwright==1.55.0','requests==2.32.5','Pillow==12.3.0','pycryptodome==3.21.0',
            'psycopg[binary]==3.2.3','aiohttp==3.12.15','pgserver==0.1.4'],check=True,timeout=180)
        source=HERE.parents[1]/'standalone_collector'
        subprocess.run([sys.executable,'-I','-B',str(source/'prepare_ocr_runtime.py'),
            '--destination',str(ROOT/'product/runtime/ocr_runtime')],check=True,timeout=75)
        os.environ['PLAYWRIGHT_BROWSERS_PATH']=str(BROWSERS)
        subprocess.run([sys.executable,'-m','playwright','install','firefox'],check=True,timeout=150)
        print('BTC_QA_BUILD_READY '+digest,flush=True)
    elif sys.argv[1]=='serve':
        os.environ['PLAYWRIGHT_BROWSERS_PATH']=str(BROWSERS)
        log=Path('/tmp/coinglass-btc-once');log.mkdir(mode=0o700,exist_ok=True)
        with (log/'metrics.log').open('ab',buffering=0) as stream:
            proc=subprocess.Popen([sys.executable,'-B','-u',str(ROOT/'qa_host.py')],stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
            for line in proc.stdout:
                stream.write(line)
                sys.stdout.buffer.write(line);sys.stdout.buffer.flush()
            raise SystemExit(proc.wait())
    else:raise ValueError('unknown_phase')

if __name__=='__main__':main()
