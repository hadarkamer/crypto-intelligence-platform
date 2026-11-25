"""Prepare the verified selector revision in an isolated copy on the same QA host."""
import hashlib
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

BASE_QA = 'ae032a47fcbda94ef21b53d4e56353f81504510f'
OLD_PRODUCT = 'f8412b67417f8636f9bebb11a028096bbc0ed1eb'
OLD_DIGEST = 'a0fdcfbbf446a426ae0f2adcbd62c46718a3ad816e8ff1926bf178cfeed71070'


def main(product, blob, digest):
    if not all(re.fullmatch(pattern,s) for pattern,s in (
            (r'[0-9a-f]{40}',product), (r'[0-9a-f]{40}',blob), (r'[0-9a-f]{64}',digest))):
        raise ValueError('invalid_revision')
    origin = Path.cwd()/'standalone_collector'
    sys.path.insert(0,str(origin))
    from native_pilot_host import require_runtime, require_service
    require_service(os.environ)
    if os.environ.get('RENDER_GIT_COMMIT') != BASE_QA:
        raise ValueError('host_revision_changed')
    require_runtime(origin)
    folder = Path(tempfile.mkdtemp(prefix='cg-reviewed-'))
    unit = folder/'standalone_collector'
    shutil.copytree(origin,unit,ignore=shutil.ignore_patterns('__pycache__','*.pyc','diagnostics'))
    with tempfile.TemporaryDirectory() as git_home:
        clean = {'PATH':os.environ['PATH'],'HOME':git_home,'GIT_TERMINAL_PROMPT':'0'}
        def git(*args):
            return subprocess.check_output(['git','-C',git_home,*args], env=clean,timeout=60)
        git('init','-q')
        git('fetch','-q','--depth=1','https://github.com/hadarkamer/crypto-intelligence-platform.git',product)
        if git('rev-parse','FETCH_HEAD').decode().strip() != product:
            raise ValueError('revision_mismatch')
        src=git('show','FETCH_HEAD:standalone_collector/dedicated_login_form.py')
    if hashlib.sha1(b'blob '+str(len(src)).encode()+b'\0'+src).hexdigest()!=blob:
        raise ValueError('selector_blob_mismatch')
    for file in (unit/'dedicated_login_form.py',unit/'runtime'/'dedicated_login_form.py'):
        file.write_bytes(src)
    host=unit/'native_pilot_host.py'
    data=host.read_text()
    if data.count(OLD_PRODUCT)!=1 or data.count(OLD_DIGEST)!=1:
        raise ValueError('qa_scope_changed')
    host.write_text(data.replace(OLD_PRODUCT,product).replace(OLD_DIGEST,digest))
    safe={'PATH':os.environ['PATH'],'HOME':str(folder),'PYTHONDONTWRITEBYTECODE':'1'}
    subprocess.run([sys.executable,'-B','-c',
        'from native_pilot_host import require_runtime; from pathlib import Path; require_runtime(Path.cwd())'],cwd=unit,env=safe,check=True)
    path=Path('/tmp')/('cg-pilot-checked-'+product)
    path.symlink_to(unit,target_is_directory=True)
    print('checked_product='+product+' runtime_sha256='+digest)


if __name__=='__main__':
    os.umask(0o077)
    main(*sys.argv[1:])
