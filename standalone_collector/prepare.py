"""Package the app-owned collector without importing/starting any bot process."""
from pathlib import Path
import hashlib
import json
import os
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


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise RuntimeError('Legacy capture structure changed; refusing partial adaptation')
    return text.replace(old, new, 1)


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
    for name in ['model1_readiness.py','model1_diagnostics.py','model1_page_flow.py']:
        shutil.copy2(HERE / name, RUNTIME / name)

    # Keep the source loaders/context and original navigation/control helpers.
    # Only the isolated app copy gains bounded UI initialization synchronization.
    # Both the real job and the source probe import THIS SAME prepared module.
    path = RUNTIME / 'market_vision/coinglass_heatmap_capture.py'
    text = path.read_text()
    page_marker='        page = context.new_page()\n'
    text=replace_once(text, page_marker, page_marker +
        '        from model1_diagnostics import attach\n        attach(page)\n')
    control_marker='        _select_model_one(page)\n'
    text=replace_once(text, control_marker,
        '        from model1_page_flow import before_controls, after_render_check\n'
        '        before_controls(page)\n' + control_marker)
    marker = '            page.wait_for_timeout(500)\n\n            path = out /'
    replacement = ('            from model1_readiness import ensure_ready\n'
                   '            try:\n'
                   '                ensure_ready(page, timeframe, out / "not-ready.png")\n'
                   '            except Exception:\n'
                   '                after_render_check(page, False)\n'
                   '                raise\n'
                   '            after_render_check(page, True)\n\n'
                   '            path = out /')
    text=replace_once(text,marker,replacement)
    compile(text,str(path),'exec')
    path.write_text(text, encoding='utf-8')
    (RUNTIME / 'provenance.json').write_text(json.dumps({'source_blobs': manifest,
        'browser_requirements': BROWSER_REQUIREMENTS,
        'app_copy_change': 'wait for site UI initialization before controls; bounded chart readiness',
        'same_module_for_probe_and_jobs':True,'bot_started':False}, indent=2))
    subprocess.run([sys.executable,'-m','unittest','test_page_flow','-q'],
                   cwd=HERE,check=True,timeout=30)
    print('Standalone package prepared; source SHA checks passed; no bot imported.',flush=True)

    # Explicit maintenance opt-in, not an automatic hourly task. It must be
    # cleared after this one validation deploy. Never calls a model or saves sheets.
    if os.getenv('MODEL1_FLOW_VALIDATION','') == '20260915-ui-sequence':
        subprocess.run([sys.executable,'-m','unittest','test_readiness','test_configuration_check','-q'],
                       cwd=HERE,check=True,timeout=45)
        subprocess.run([sys.executable,str(HERE/'app.py'),'--source-probe'],
                       cwd=ROOT,check=True,timeout=200)

if __name__ == '__main__':
    prepare()
