"""Bounded, source-only diagnostic. Never imports the app, worker, DB or AI.

Run only in an explicitly authorized disposable Render one-off instance.
Uses the deployed reader unchanged, with observations and a reversible 3-second
render-gate experiment before timeframe selection. No product files are edited.
"""
from __future__ import annotations
import argparse
import base64
import contextlib
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

BASE = 'f6adfc6fea05bfd82a59098433ad1c2f31f6267f'
CADENCE = 'e3fd4d116eaecaf43e870a8b5ae3aa676a334e71'
SERVICE = 'srv-dakhacou01pc73f58aug'
SESSION = ('COINGLASS_COOKIE_HEADER', 'COINGLASS_STORAGE_STATE_JSON')
SYSTEM = ('PATH','HOME','TMPDIR','LANG','LC_ALL','TZ','SSL_CERT_FILE',
          'SSL_CERT_DIR','REQUESTS_CA_BUNDLE','PLAYWRIGHT_BROWSERS_PATH','LD_LIBRARY_PATH')
FORBIDDEN = {'app','worker','collection_bridge','psycopg','psycopg2','openai',
             'market_vision.openai_heatmap_scanner','collection_model1_task'}
spec = importlib.util.spec_from_file_location('resource_observer', Path(__file__).with_name('resources.py'))
resources = importlib.util.module_from_spec(spec)
spec.loader.exec_module(resources)

def safe_env(source, model):
    result = {k: source[k] for k in SYSTEM + SESSION if source.get(k)}
    result.update(COLLECTION_HEATMAP_MODEL=str(model), COLLECTION_BRIDGE_ENABLED='false',
                  PYTHONDONTWRITEBYTECODE='1', PYTHONUNBUFFERED='1')
    return result

def require_limits(group):
    quota = str(group.get('cpu.max','')).split()
    if (len(quota)!=2 or not all(v.isdigit() for v in quota) or int(quota[1])<=0
        or not 1.95 <= int(quota[0])/int(quota[1]) <= 2.05
        or not isinstance(group.get('memory.max'),int)
        or not 3.8*1024**3 <= group['memory.max'] <= 4.2*1024**3):
        raise RuntimeError('isolated_2cpu_4gib_required')

def verify_runtime(runtime):
    expected=json.loads(Path(__file__).with_name('runtime-hashes.json').read_text())
    actual={name:hashlib.sha256((runtime/name).read_bytes()).hexdigest() for name in expected}
    if actual!=expected:raise RuntimeError('deployed_runtime_mismatch')
    return actual

def summarize_delta(before, after, seconds):
    cpu = (after['cpu.stat']['usage_usec']-before['cpu.stat']['usage_usec'])/1e6
    return {'seconds':seconds,'cpu_seconds':cpu,'two_cpu_fraction':cpu/(2*seconds),
            'throttled_periods':after['cpu.stat']['nr_throttled']-before['cpu.stat']['nr_throttled'],
            'memory_after_bytes':after['memory.current']}

TOOLTIP_OBSERVATION = r'''() => {
  const visible=e=>{const r=e.getBoundingClientRect();if(!r.width||!r.height||r.bottom<0||r.top>innerHeight)return false;
    for(let n=e;n;n=n.parentElement){const s=getComputedStyle(n);if(s.display==='none'||s.visibility==='hidden'||Number(s.opacity)===0)return false}return true};
  const boxes=[...document.querySelectorAll('.cg-toolti-box,[role="tooltip"]')].filter(visible);
  const extra=[...document.querySelectorAll('div,span')].filter(e=>e.children.length===0&&visible(e)&&/\bOpen\b/.test(e.innerText||'')&&/\bClose\b/.test(e.innerText||''));
  return {visible_boxes:boxes.length,observations:[...new Set([...boxes,...extra])].slice(0,8).map(e=>{
    const t=(e.innerText||'').trim();const ohlc=/\bOpen\b/.test(t)&&/\bClose\b/.test(t);
    const liquidity=t.includes('Liquidation Leverage');
    return {kind:ohlc?'ohlc':liquidity?'liquidity':'other',length:t.length,
      text:(ohlc||liquidity)&&t.length<=220?t:null};})};
}'''

def model3_points(image_path, bounds):
    from PIL import Image
    import math
    with Image.open(image_path) as im:
        im=im.convert('RGB'); right=math.floor(bounds['right'])
        points=[(x,y) for x in range(right-3,right+1) for y in range(bounds['top']+1,bounds['bottom'])
                if im.getpixel((x,y)) in ((234,51,35),(55,126,34))]
    if not points: raise RuntimeError('no_verified_model3_candle_pixels')
    x=max(p[0] for p in points); ys=sorted(p[1] for p in points if p[0]==x)
    if any(b-a>2 for a,b in zip(ys,ys[1:])): raise RuntimeError('ambiguous_model3_candle_pixels')
    return list(dict.fromkeys((x,y) for y in (ys[0],ys[len(ys)//2],ys[-1])))

def candle_probe(page, image_path, geometry, snapshot, model):
    import heatmap_hover as hover
    bounds=hover.plot_bounds(image_path,geometry)
    points=model3_points(image_path,bounds) if model==3 else hover.candle_points(image_path,bounds)[:6]
    scroll=page.evaluate('() => ({x:scrollX,y:scrollY})')
    output=[]
    try:
        for x,y in points:
            page.mouse.move(x-scroll['x'],y-scroll['y']);page.wait_for_timeout(400)
            item=page.evaluate(TOOLTIP_OBSERVATION)
            item['point']=[x,y]; item['exact_reader_accepted']=False
            text=page.evaluate(hover.READ_TOOLTIP,'candle')
            if text:
                try:
                    candle=hover.parse_candle(text)
                    hover.validate_candle(candle,snapshot['source_time'])
                    item.update(exact_reader_accepted=True,candle=candle)
                except hover.HoverError as e:
                    item['reader_rejection']=str(e) if str(e).startswith('hover_') else 'invalid'
            output.append(item)
    finally:
        page.mouse.move(0,0)
    return {'observed_points':output,'source_time':snapshot['source_time'],
            'caveat':'No OHLC at these points does not prove that no other map control exposes it.'}

def preparation_probe(page):
    from capture_readiness import STATE_SCRIPT,geometry_from_state
    initial=page.evaluate(STATE_SCRIPT); geometry=geometry_from_state(initial)
    if not geometry: return {'status':'not_tested','reason':'geometry_unverified'}
    scroll=page.evaluate('() => ({x:scrollX,y:scrollY})')
    rect={k:geometry[k] for k in ('x','y','width','height')}
    rect['x']-=scroll['x'];rect['y']-=scroll['y']
    discovery=page.evaluate('r=>globalThis.__cgModel3Discovery?.attachSelected(r)||null',rect)
    if not discovery or discovery.get('supported') is not True:
        return {'status':'not_tested','reason':(discovery or {}).get('reason','hook_missing')}
    entry=discovery['entry']; key=entry['key'];count=entry['ticker_count']
    if discovery.get('version')!='8.21.0' or entry.get('enabled') is not False:
        raise RuntimeError('early_hook_contract_mismatch')
    results=[]
    def snap():
        return page.evaluate('key=>globalThis.__cg29RenderLab.snapshot().find(x=>x.key===key)',key)
    try:
        for mode in ('original','limited','restored'):
            if mode!='original':
                action='enable' if mode=='limited' else 'disable'
                state=page.evaluate('a=>globalThis.__cg29RenderLab[a.action](a.key,0.5)',{'key':key,'action':action})
                if state['ticker_count']!=count or state['enabled']!=(mode=='limited'):
                    raise RuntimeError('early_hook_transition_unverified')
            a=snap(); cpu_a=resources.cgroup_snapshot();at=time.monotonic()
            page.wait_for_timeout(3000)
            elapsed=time.monotonic()-at;cpu_b=resources.cgroup_snapshot();b=snap()
            results.append({'mode':mode,**summarize_delta(cpu_a,cpu_b,elapsed),
                            'render_calls':b['stats']['render_calls']-a['stats']['render_calls'],
                            'render_ms':b['stats']['render_ms']-a['stats']['render_ms'],
                            'skipped_paints':b['stats']['skipped_paints']-a['stats']['skipped_paints']})
    finally:
        final=page.evaluate('key=>globalThis.__cg29RenderLab.disable(key)',key)
        if final.get('enabled') is not False or final.get('ticker_count')!=count:
            raise RuntimeError('early_hook_restore_unverified')
    after=page.evaluate(STATE_SCRIPT)
    same_label=initial.get('label')==after.get('label')
    same_geometry=geometry_from_state(initial)==geometry_from_state(after)
    return {'status':'completed' if same_label and same_geometry else 'inconclusive',
            'windows':results,'same_label':same_label,'same_geometry':same_geometry,
            'caveat':'Three-second within-page intervention; not a full earlier-gate implementation.'}

def child(runtime, output, model):
    inherited=safe_env(os.environ,model);os.environ.clear();os.environ.update(inherited)
    sys.path.insert(0,str(runtime))
    import model1_execution as execution
    original=execution.capture_operation
    report={'model':model,'timeframe':'12H','operations':[],'source_success':False,
            'ai_calls':0,'database_writes':0}
    observed=False; capfile=str(runtime/'market_vision/coinglass_heatmap_capture.py')
    def frame():
        f=sys._getframe()
        while f:
            if f.f_code.co_filename==capfile and f.f_code.co_name=='capture_heatmaps':return f
            f=f.f_back
        return None
    @contextlib.contextmanager
    def operation(name):
        nonlocal observed
        f=frame();page=f.f_locals.get('page') if f else None
        if name=='timeframe_selection' and model==3 and not observed:
            observed=True;report['preparation_experiment']=preparation_probe(page)
        a=resources.cgroup_snapshot();started=time.monotonic();success=False
        try:
            with original(name):yield
            success=True
        finally:
            seconds=time.monotonic()-started;b=resources.cgroup_snapshot()
            report['operations'].append({'name':name,'success':success,**summarize_delta(a,b,seconds)})
        if name=='hover_collection' and success:
            report['candle_probe']=candle_probe(page,f.f_locals['path'],f.f_locals['chart_rect'],
                                               f.f_locals['hover_snapshot'],model)
    execution.capture_operation=operation
    started=time.monotonic();code=1
    try:
        from market_vision.coinglass_heatmap_capture import capture_heatmaps
        images=capture_heatmaps(output,timeframes=('12h',))
        snap=images[0]['hover_snapshot']
        if not snap.get('complete') or snap['heatmap_model']!=model or snap['timeframe']!='12H':
            raise RuntimeError('source_result_identity_invalid')
        report.update(source_success=True,row_count=len(snap['rows']),source_time=snap['source_time'],
                      image_sha256=hashlib.sha256(Path(images[0]['image']).read_bytes()).hexdigest())
        code=0
    except Exception as e:
        report['failure']=execution.failure_detail(e)
        report['exception_class']=type(e).__name__
    finally:
        report['elapsed_seconds']=time.monotonic()-started
        report['forbidden_modules_loaded']=sorted(set(sys.modules)&FORBIDDEN)
        if report['forbidden_modules_loaded']:code=1
        output.mkdir(exist_ok=True)
        (output/'report.json').write_text(json.dumps(report,allow_nan=False))
    return code

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime',type=Path,required=True)
    parser.add_argument('--child',type=int,choices=(2,3))
    parser.add_argument('--output',type=Path)
    args=parser.parse_args();runtime=args.runtime.resolve();os.umask(0o077)
    if args.child:return child(runtime,args.output,args.child)
    if (os.getenv('CG3_DIAGNOSTIC_ONE_OFF')!='1' or os.getenv('RENDER_SERVICE_ID')!=SERVICE
        or os.getenv('RENDER_GIT_COMMIT')!=BASE):raise RuntimeError('isolated_base_guard_failed')
    require_limits(resources.cgroup_snapshot())
    hashes=verify_runtime(runtime)
    env=safe_env(os.environ,3);os.environ.clear();os.environ.update(env)
    if not any(env.get(k) for k in SESSION):raise RuntimeError('source_session_missing')
    summary={'collector_sha':BASE,'cadence_pr':120,'cadence_sha':CADENCE,'runtime_hashes':hashes,
             'source_calls_limit':2,'ai_calls':0,'database_writes':0,'runs':[]}
    with tempfile.TemporaryDirectory(prefix='cg3-preparation-') as folder:
        for model in (3,2):
            out=Path(folder)/str(model);out.mkdir()
            tracked={'seen_processes':{}};samples=[];start=time.monotonic();stop=None;cleanup_errors=[]
            with (out/'child.log').open('w') as log:
                p=subprocess.Popen([sys.executable,str(Path(__file__).resolve()),'--runtime',str(runtime),
                                    '--child',str(model),'--output',str(out)],env=safe_env(env,model),
                                   stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
                try:
                    while p.poll() is None:
                        stats=resources.proc_stats()
                        for pid in resources.process_members(p.pid,stats):tracked['seen_processes'][(pid,stats[pid]['start'])]=True
                        group=resources.cgroup_snapshot();samples.append({'seconds':time.monotonic()-start,'cgroup':group})
                        if group['memory.current']>group['memory.max']*.85:stop='memory_limit';break
                        if time.monotonic()-start>300:stop='timeout';break
                        time.sleep(.5)
                finally:
                    for cleanup in (lambda:resources.clean_group(p),lambda:resources.clean_tracked_children(tracked)):
                        try:cleanup()
                        except Exception as exc:cleanup_errors.append(type(exc).__name__)
            data=json.loads((out/'report.json').read_text()) if (out/'report.json').is_file() else {'source_success':False}
            current=resources.proc_stats()
            survivors=sum(1 for pid,st in tracked['seen_processes'] if pid in current and current[pid]['start']==st and current[pid]['state']!='Z')
            render_events=[]
            for line in (out/'child.log').read_text().splitlines():
                if line.startswith('MODEL3_RENDER '):
                    event=json.loads(line.removeprefix('MODEL3_RENDER '))
                    render_events.append({k:event[k] for k in ('status','reason') if k in event})
            summary['runs'].append({'model':model,'stop':stop,'returncode':p.returncode,'measurement':data,
                                    'samples':samples,'render_events':render_events,'survivors':survivors,
                                    'cleanup_errors':cleanup_errors})
            if stop or p.returncode or not data.get('source_success') or survivors or cleanup_errors:break
    captures_ok=len(summary['runs'])==2 and all(x['returncode']==0 and not x['stop'] and
        x['measurement'].get('source_success') and not x['survivors'] and not x['cleanup_errors'] for x in summary['runs'])
    measurements={x['model']:x['measurement'] for x in summary['runs']}
    summary['checks']={'captures_completed':captures_ok,
        'gate_experiment_completed':measurements.get(3,{}).get('preparation_experiment',{}).get('status')=='completed',
        'model2_ohlc_positive_control':any(x.get('exact_reader_accepted') for x in
            measurements.get(2,{}).get('candle_probe',{}).get('observed_points',[]))}
    encoded=json.dumps(summary,separators=(',',':'),allow_nan=False).encode()
    packed=base64.b64encode(gzip.compress(encoded)).decode()
    chunks=[packed[i:i+7000] for i in range(0,len(packed),7000)]
    for i,chunk in enumerate(chunks):print('CG3DIAG '+json.dumps({'index':i,'count':len(chunks),'data':chunk}),flush=True)
    print('CG3DIAG '+json.dumps({'event':'complete','sha256':hashlib.sha256(encoded).hexdigest(),'bytes':len(encoded)}),flush=True)
    return 0 if all(summary['checks'].values()) else 1

if __name__=='__main__':raise SystemExit(main())
