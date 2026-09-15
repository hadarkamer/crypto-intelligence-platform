"""Build interval-aware app collector; no external source scans during deployment."""
from pathlib import Path
import ast
import hashlib
import json
import shutil
import subprocess
import sys
from importlib.metadata import version,PackageNotFoundError

HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
RUNTIME=HERE/'runtime'
PINNED={
    'market_vision/coinglass_heatmap_capture.py':'d1d75f2c99ea3c1fd72c7e1f9cfb1ec29a0890ab',
    'market_vision/openai_heatmap_scanner.py':'9dd18b993d6d87cf326848720fa08d1d6a554087',
}
BROWSER_REQUIREMENTS={'playwright':'1.55.0','requests':'2.32.5','Pillow':'12.3.0'}


def prepare_browser():
    needs_install=False
    for package,expected in BROWSER_REQUIREMENTS.items():
        try:needs_install=needs_install or version(package)!=expected
        except PackageNotFoundError:needs_install=True
    if needs_install:
        subprocess.run([sys.executable,'-m','pip','install',
            *[f'{p}=={v}' for p,v in BROWSER_REQUIREMENTS.items()]],check=True,timeout=180)
    subprocess.run([sys.executable,'-m','playwright','install','chromium'],check=True,timeout=240)
    for package,expected in BROWSER_REQUIREMENTS.items():
        if version(package)!=expected:raise RuntimeError('Standalone dependency mismatch')
    print('MODEL1_BROWSER_RUNTIME playwright=1.55.0 requests=2.32.5 Pillow=12.3.0',flush=True)


def validator_ast(text):
    node=next(n for n in ast.parse(text).body if isinstance(n,ast.FunctionDef) and n.name=='normalize')
    return ast.dump(node,include_attributes=False)


def prepare():
    prepare_browser()
    (RUNTIME/'market_vision').mkdir(parents=True,exist_ok=True)
    manifest={}
    for name,expected in PINNED.items():
        data=(ROOT/name).read_bytes()
        actual=hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()
        if actual!=expected:raise RuntimeError('Source version changed: '+name)
        (RUNTIME/name).write_bytes(data);manifest[name]=actual
    (RUNTIME/'market_vision/__init__.py').write_text('"""App-owned Model1 source copy."""\n')
    for name in ('collection_bridge.py','collection_model1_task.py'):
        shutil.copy2(ROOT/name,RUNTIME/name)
    for name in ('model1_readiness.py','model1_diagnostics.py','model1_page_flow.py',
                 'model1_execution.py','image_detail.py','price_detail_input.py',
                 'model1_price_range.py','model1_evidence_format.py','model1_legend.py',
                 'model1_cache_policy.py'):
        shutil.copy2(HERE/name,RUNTIME/name)
    from install_original_flow import install,expand_capture
    from install_price_detail import install as install_detail,expand_detail_capture
    from install_price_range import install as install_range
    from model1_legend import install as install_legend,expand_legend_capture
    from model1_cache_policy import install as install_cache
    install(RUNTIME)
    task_path=RUNTIME/'collection_model1_task.py'
    point_validator=validator_ast(task_path.read_text())
    install_detail(RUNTIME)
    if validator_ast(task_path.read_text())!=point_validator:
        raise RuntimeError('Image detail changed the point validator')
    install_range(RUNTIME)
    install_legend(RUNTIME)
    install_cache(RUNTIME)
    original=(ROOT/'market_vision/coinglass_heatmap_capture.py').read_text()
    actual=(RUNTIME/'market_vision/coinglass_heatmap_capture.py').read_text()
    expected=expand_legend_capture(expand_detail_capture(expand_capture(original)))
    if actual!=expected:raise RuntimeError('Unexpected capture adaptation')
    if actual.count('page.screenshot(')!=original.count('page.screenshot('):
        raise RuntimeError('Range support must not add a source screenshot')
    (RUNTIME/'provenance.json').write_text(json.dumps({
        'source_blobs':manifest,'dependencies':BROWSER_REQUIREMENTS,
        'capture_flow':'one selected horizon; normally close Legend help card; same PNG plus crop',
        'supported_timeframes':['12H','24H','48H'],
        'analysis':'one model request, full screenshot and same-image detail, one scan',
        'price_reference':'explicit visual range or validated legacy point; never fake midpoint',
        'initial_max_range_fraction':0.01,'range_limit_is_accuracy_claim':False,
        'legacy_point_validator_unchanged':True,'numeric_axis_order':'smaller price then larger price',
        'failed_evidence':'private original PNG and numeric metadata; authenticated image access6h',
        'auto_reuse_minutes':15,'saved_review_max_hours':6,'saved_review_starts_capture':False,
        'capture_limit_unchanged':True,'bot_started':False,'automatic_source_checks':False},indent=2))
    subprocess.run([sys.executable,'-m','unittest','test_original_flow','test_august_replay',
        'test_page_flow','test_48h_execution','test_image_detail','test_price_range','test_legend_popup',
        'test_cache_policy','-q'],cwd=HERE,check=True,timeout=60)
    print('MODEL1_CACHE_POLICY_INSTALLED auto_reuse_minutes=15 saved_read=GET_ONLY source_scans=0',flush=True)

if __name__=='__main__':prepare()
