"""Package the app-owned collector without importing/starting any bot process."""
from pathlib import Path
import hashlib
import json
import shutil

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
RUNTIME = HERE / 'runtime'
PINNED = {
    'market_vision/coinglass_heatmap_capture.py': 'd1d75f2c99ea3c1fd72c7e1f9cfb1ec29a0890ab',
    'market_vision/openai_heatmap_scanner.py': '9dd18b993d6d87cf326848720fa08d1d6a554087',
}

def prepare():
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
    shutil.copy2(HERE / 'model1_readiness.py', RUNTIME / 'model1_readiness.py')
    # The original repository source is NOT changed. Only the app-owned copy
    # receives a bounded readiness check immediately before its screenshot.
    path = RUNTIME / 'market_vision/coinglass_heatmap_capture.py'
    text = path.read_text()
    marker = '            page.wait_for_timeout(500)\n\n            path = out /'
    if text.count(marker) != 1:
        raise RuntimeError('Expected final screenshot stage not found')
    replacement = ('            from model1_readiness import ensure_ready\n'
                   '            ensure_ready(page, timeframe, out / "not-ready.png")\n\n'
                   '            path = out /')
    path.write_text(text.replace(marker, replacement), encoding='utf-8')
    (RUNTIME / 'provenance.json').write_text(json.dumps({'source_blobs': manifest,
        'app_copy_change': 'bounded read-only readiness check before screenshot',
        'bot_started': False}, indent=2))
    print('Standalone package prepared; source SHA checks passed; no bot imported.')

if __name__ == '__main__':
    prepare()
