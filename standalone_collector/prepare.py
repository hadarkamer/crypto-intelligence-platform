"""Package the app-owned collector without importing/starting any bot process."""
from pathlib import Path
import hashlib
import json
import shutil
import subprocess
import sys
from importlib.metadata import version, PackageNotFoundError

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
RUNTIME = HERE / 'runtime'
PINNED = {
    'market_vision/coinglass_heatmap_capture.py': 'd1d75f2c99ea3c1fd72c7e1f9cfb1ec29a0890ab',
    'market_vision/openai_heatmap_scanner.py': '9dd18b993d6d87cf326848720fa08d1d6a554087',
}
# Match the two dependencies recorded in the successful August source run.
# This build hook affects only this service's virtual environment. Original
# bot requirements and source files remain unchanged. No source/model calls.
BROWSER_REQUIREMENTS = {'playwright': '1.55.0', 'requests': '2.32.5'}


def prepare_browser():
    needs_install = False
    for package, expected in BROWSER_REQUIREMENTS.items():
        try:
            needs_install = needs_install or version(package) != expected
        except PackageNotFoundError:
            needs_install = True
    if needs_install:
        subprocess.run([sys.executable, '-m', 'pip', 'install',
                        *[f'{p}=={v}' for p, v in BROWSER_REQUIREMENTS.items()]],
                       check=True, timeout=180)
    subprocess.run([sys.executable, '-m', 'playwright', 'install', 'chromium'],
                   check=True, timeout=240)
    for package, expected in BROWSER_REQUIREMENTS.items():
        if version(package) != expected:
            raise RuntimeError('Standalone browser dependency mismatch')
    print('MODEL1_BROWSER_RUNTIME playwright=1.55.0 requests=2.32.5', flush=True)


def prepare():
    prepare_browser()
    (RUNTIME / 'market_vision').mkdir(parents=True, exist_ok=True)
    manifest = {}
    for name, expected in PINNED.items():
        data = (ROOT / name).read_bytes()
        actual = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
        if actual != expected:
            raise RuntimeError('Source version changed: ' + name)
        (RUNTIME / name).write_bytes(data)
        manifest[name] = actual
    (RUNTIME / 'market_vision/__init__.py').write_text('"""App-owned Model1 source copy."""\n')
    for name in ['collection_bridge.py', 'collection_model1_task.py']:
        shutil.copy2(ROOT / name, RUNTIME / name)
    for name in ['model1_readiness.py','model1_diagnostics.py']:
        shutil.copy2(HERE / name, RUNTIME / name)
    # Original repository source remains unchanged. Only its app-owned copy
    # receives readiness checks and optional passive network-error observations.
    path = RUNTIME / 'market_vision/coinglass_heatmap_capture.py'
    text = path.read_text()
    page_marker='        page = context.new_page()\n'
    if text.count(page_marker)!=1:
        raise RuntimeError('Expected page creation stage not found')
    text=text.replace(page_marker, page_marker +
        '        from model1_diagnostics import attach\n        attach(page)\n')
    marker = '            page.wait_for_timeout(500)\n\n            path = out /'
    if text.count(marker) != 1:
        raise RuntimeError('Expected final screenshot stage not found')
    replacement = ('            from model1_readiness import ensure_ready\n'
                   '            ensure_ready(page, timeframe, out / "not-ready.png")\n\n'
                   '            path = out /')
    path.write_text(text.replace(marker, replacement), encoding='utf-8')
    (RUNTIME / 'provenance.json').write_text(json.dumps({'source_blobs': manifest,
        'browser_requirements': BROWSER_REQUIREMENTS,
        'app_copy_change': 'bounded read-only readiness check and optional passive diagnostics',
        'bot_started': False}, indent=2))
    print('Standalone package prepared; source SHA checks passed; no bot imported.')

if __name__ == '__main__':
    prepare()
