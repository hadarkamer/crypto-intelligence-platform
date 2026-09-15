"""Build and test app-owned collector adapters. No source or inference calls."""
from pathlib import Path
import hashlib
import json
import shutil
import subprocess
import sys
from importlib.metadata import version, PackageNotFoundError

HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
RUNTIME=HERE/'runtime'
PINNED={
    'market_vision/coinglass_heatmap_capture.py':'d1d75f2c99ea3c1fd72c7e1f9cfb1ec29a0890ab',
    'market_vision/openai_heatmap_scanner.py':'9dd18b993d6d87cf326848720fa08d1d6a554087',
}
BROWSER_REQUIREMENTS={'playwright':'1.55.0','requests':'2.32.5'}


def prepare_browser():
    needs_install=False
    for package,expected in BROWSER_REQUIREMENTS.items():
        try:
            needs_install=needs_install or version(package)!=expected
        except PackageNotFoundError:
            needs_install=True
    if needs_install:
        subprocess.run([sys.executable,'-m','pip','install',
            *[f'{p}=={v}' for p,v in BROWSER_REQUIREMENTS.items()]],check=True,timeout=180)
    subprocess.run([sys.executable,'-m','playwright','install','chromium'],check=True,timeout=240)
    for package,expected in BROWSER_REQUIREMENTS.items():
        if version(package)!=expected:raise RuntimeError('Standalone browser dependency mismatch')
    print('MODEL1_BROWSER_RUNTIME playwright=1.55.0 requests=2.32.5',flush=True)


def prepare():
    prepare_browser()
    (RUNTIME/'market_vision').mkdir(parents=True,exist_ok=True)
    manifest={}
    for name,expected in PINNED.items():
        data=(ROOT/name).read_bytes()
        actual=hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()
        if actual!=expected:raise RuntimeError('Source version changed: '+name)
        (RUNTIME/name).write_bytes(data)
        manifest[name]=actual
    (RUNTIME/'market_vision/__init__.py').write_text('"""App-owned Model1 source copy."""\n')
    for name in ('collection_bridge.py','collection_model1_task.py'):
        shutil.copy2(ROOT/name,RUNTIME/name)
    for name in ('model1_readiness.py','model1_diagnostics.py','model1_page_flow.py','model1_execution.py'):
        shutil.copy2(HERE/name,RUNTIME/name)
    from install_original_flow import install, expand_capture
    install(RUNTIME)
    original=(ROOT/'market_vision/coinglass_heatmap_capture.py').read_text()
    if (RUNTIME/'market_vision/coinglass_heatmap_capture.py').read_text()!=expand_capture(original):
        raise RuntimeError('Unexpected change outside the explicit timeframe selector')
    (RUNTIME/'provenance.json').write_text(json.dumps({
        'source_blobs':manifest,'browser_requirements':BROWSER_REQUIREMENTS,
        'capture_flow':'original 12h then 24h; append actual 48h only when requested',
        'supported_timeframes':['12H','24H','48H'],
        'analysis':'one requested screenshot, observed settings and readability required',
        'acceptance':'strict image evidence plus existing numeric validator',
        'failure_diagnostics':'stage and allowlisted code, no raw errors',
        'bot_started':False,'automatic_source_checks':False},indent=2))
    subprocess.run([sys.executable,'-m','unittest','test_original_flow','test_august_replay',
                   'test_page_flow','test_48h_execution','-q'],cwd=HERE,check=True,timeout=45)
    # Old experiment flags are ignored. A deployment never starts a source job.
    print('MODEL1_ORIGINAL_FLOW_INSTALLED horizons=12H,24H,48H requested_analysis=one automatic_source_calls=0',flush=True)

if __name__=='__main__':prepare()
