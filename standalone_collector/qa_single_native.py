"""Prepare the reviewed market-only trial; the build is invoked with no service secrets."""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from zipfile import ZipFile

ROOT=Path(__file__).resolve().parent
DEST=ROOT/'qa_single_runtime'
BROWSERS=ROOT.parent/'.qa_firefox'
def unpack():
    os.umask(0o077)
    DEST.mkdir(mode=0o700,exist_ok=True)
    blob=ROOT/'qa_single_pilot.zip'
    expected=(ROOT/'qa_single_pilot.sha256').read_text().strip()
    if not re.fullmatch('[0-9a-f]{64}',expected) or hashlib.sha256(blob.read_bytes()).hexdigest()!=expected:
        raise ValueError('unexpected_qa_archive')
    with ZipFile(blob) as z:
        for tag in z.infolist():
            name=Path(tag.filename)
            if name.is_absolute() or '..' in name.parts or name.suffix not in ('.py','.json') or tag.file_size>600000:
                raise ValueError('invalid_qa_archive_file')
            dst=DEST/name
            dst.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
            dst.write_bytes(z.read(tag));dst.chmod(0o600)
    return expected
def main():
    if len(sys.argv)!=2 or os.environ.get('RENDER_SERVICE_NAME')!='decision-hub-coinglass-ocr-i-20261127' or os.environ.get('RENDER_GIT_BRANCH')!='qa/coinglass-tesseract-i-isolated-20261127':
        raise SystemExit('wrong_source_or_service')
    if sys.argv[1]=='build':
        # The product modules come directly from the exact reviewed repository tree.
        digest=unpack()
        for name,expected in json.loads((DEST/'collector-manifest.json').read_text()).items():
            if Path(name).name!=name or not name.endswith('.py'):raise ValueError('invalid_source_name')
            data=(ROOT/name).read_bytes()
            if hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()!=expected:
                raise ValueError('candidate_product_diff')
        if any(k in os.environ for k in ('DATABASE_URL','COINGLASS_SESSION_KEY','COINGLASS_LOGIN_PASSWORD','OPENAI_API_KEY')):raise RuntimeError('secret_in_build')
        subprocess.run([sys.executable,'-I','-B',str(ROOT/'prepare_ocr_runtime.py'),
                        '--destination',str(ROOT/'ocr_runtime')],check=True,timeout=75)
        subprocess.run([sys.executable,'-m','pip','install','--disable-pip-version-check','--no-input',
              'playwright==1.55.0','requests==2.32.5','Pillow==12.3.0','pycryptodome==3.21.0','psycopg[binary]==3.2.3'],check=True,timeout=235)
        os.environ['PLAYWRIGHT_BROWSERS_PATH']=str(BROWSERS)
        subprocess.run([sys.executable,'-B','-m','playwright','install','firefox'],check=True,timeout=150)
        path=BROWSERS/'firefox-1490/firefox/firefox'
        if not path.is_file() or not os.access(path,os.X_OK):raise RuntimeError('missing_firefox_1490')
        print('QA_SINGLE_PACKAGED',digest,'Firefox 141.0 rev1490',flush=True)
    elif sys.argv[1]=='serve':
        os.environ['PLAYWRIGHT_BROWSERS_PATH']=str(BROWSERS)
        os.execve(sys.executable,[sys.executable,'-I','-B','-u',str(DEST/'single_host.py')],os.environ)
    else:raise SystemExit('unknown_stage')
if __name__=='__main__':main()
