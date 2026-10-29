"""Lab-only clean A/B/B/restore checks, using the unmodified numerical reader."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import sys
import tempfile
import time

READER_SHA='693aa6739a8b1f50d0550cd802dde99cff4c68edfad6c643c1adf8d6bc05f820'
CAPTURE_SHA='6b05e7dd62b4a2ef228c2cf31eb75d9598aa36ef0a15c4665871cda085e40b35'
ROW_KEYS=('id','price','value','unit','source_time','raw_price','raw_value','rounding_error')

class CheckFailure(RuntimeError):pass

def require(value,code):
    if not value:raise CheckFailure(code)

def safe_error(exc):
    value=getattr(exc,'reason',None) or getattr(exc,'code',None) or (str(exc) if isinstance(exc,(CheckFailure,ValueError)) else '')
    return value if re.fullmatch(r'[a-z][a-z0-9_]{0,100}',str(value)) else type(exc).__name__

def rows_key(s):return [[r[k] for k in ROW_KEYS] for r in s['rows']]

def digest(s):return hashlib.sha256(json.dumps(s,sort_keys=True,separators=(',',':')).encode()).hexdigest()

def cpu_usage():
    try:return int(dict(x.split() for x in Path('/sys/fs/cgroup/cpu.stat').read_text().splitlines())['usage_usec'])/1e6
    except (OSError,KeyError,ValueError):return None

def image_compare(first,second,bounds):
    from PIL import Image,ImageChops
    box=tuple(int(bounds[k]) for k in ('left','top','right','bottom'))
    with Image.open(first) as a,Image.open(second) as b:
        require(a.size==b.size,'image_dimensions_changed')
        a=a.convert('RGB').crop((box[0],box[1],box[2]+1,box[3]+1))
        b=b.convert('RGB').crop((box[0],box[1],box[2]+1,box[3]+1))
        r,g,bl=ImageChops.difference(a,b).split()
        delta=ImageChops.lighter(ImageChops.lighter(r,g),bl)
        changed=a.width*a.height-delta.histogram()[0]
        return {'equal':changed==0,'changed_pixels':changed,'pixels':a.width*a.height,
                'difference_bbox':delta.getbbox(),'first_rgb_sha256':hashlib.sha256(a.tobytes()).hexdigest(),
                'second_rgb_sha256':hashlib.sha256(b.tobytes()).hexdigest()}

def run(timeframes,runtime,fps,requested_output=None):
    os.umask(0o077)
    require(not any(os.getenv(k,'').strip() for k in ('DATABASE_URL','OPENAI_API_KEY','SUPABASE_SERVICE_ROLE_KEY')),
            'lab_service_credentials_must_be_absent')
    os.environ['COLLECTION_HEATMAP_MODEL']='3';sys.path.insert(0,str(runtime))
    import heatmap_hover as reader
    import model1_execution as execution
    import capture_readiness as readiness
    from market_vision.coinglass_heatmap_capture import capture_heatmaps
    from playwright.sync_api import Browser,BrowserContext,Page
    import candidate,clean_frame
    reader_path=Path(reader.__file__);capture_path=runtime/'market_vision/coinglass_heatmap_capture.py'
    require(hashlib.sha256(reader_path.read_bytes()).hexdigest()==READER_SHA,'reader_hash_mismatch')
    require(hashlib.sha256(capture_path.read_bytes()).hexdigest()==CAPTURE_SHA,'capture_hash_mismatch')
    if requested_output is None:output=Path(tempfile.mkdtemp(prefix='cg29-clean-ab-'))
    else:
        output=requested_output.resolve();require(output.parent==Path('/tmp'),'lab_output_must_be_private_tmp_child')
        output.mkdir(mode=0o700,exist_ok=False)
    report={'schema':'model3-clean-render-ab.v2','status':'running','timeframes':list(timeframes),
            'candidate_repetitions':2,'experimental_fps':fps,'pairs':[],'attempts':[],'cleanup':[],'clean_captures':[],
            'database_writes':0,'openai_calls':0,'reader_sha256':READER_SHA,'capture_sha256':CAPTURE_SHA,
            'cpu_affinity':sorted(os.sched_getaffinity(0)),
            'cpu_max':Path('/sys/fs/cgroup/cpu.max').read_text().strip(),
            'memory_max':Path('/sys/fs/cgroup/memory.max').read_text().strip()}
    installs={};restored_contexts=set()
    originals=(Browser.new_context,Browser.close,BrowserContext.close,Page.screenshot,reader.capture_column)
    new_context_original,browser_close_original,context_close_original,screenshot_original,read_original=originals
    def persist():(output/'report.json').write_text(json.dumps(report,indent=2,allow_nan=False))
    def emit(event,**fields):print('CG29_RENDER_AB '+json.dumps({'event':event,**fields},separators=(',',':'),allow_nan=False),flush=True)
    def new_context(browser,*args,**kwargs):
        context=new_context_original(browser,*args,**kwargs)
        restored_contexts.discard(id(context))
        installs[id(context)]=candidate.install_context(context);return context
    def restore_context(context):
        if id(context) not in installs or id(context) in restored_contexts:return
        restored_contexts.add(id(context))
        for page in context.pages:
            if page.is_closed():continue
            try:
                if candidate.snapshot(page):report['cleanup'].append({'restoration':candidate.restore_all(page)})
            except Exception as exc:report['cleanup'].append({'error':safe_error(exc)})
    def close_context(context,*args,**kwargs):
        try:restore_context(context)
        finally:return context_close_original(context,*args,**kwargs)
    def close_browser(browser,*args,**kwargs):
        try:
            for context in browser.contexts:restore_context(context)
        finally:return browser_close_original(browser,*args,**kwargs)
    def screenshot(page,*args,**kwargs):
        if id(page.context) in installs and candidate.snapshot(page):
            candidate.require_ready(page,installs[id(page.context)])
            state=clean_frame.prepare_frame(page,candidate.force_paint)
            report['clean_captures'].append(state)
        return screenshot_original(page,*args,**kwargs)
    def evidence(name,snapshot,image):
        reader.validate_rows(snapshot['rows']);require(snapshot['complete'] is True,'incomplete_column')
        value={'snapshot':snapshot,'candidates':reader.candidate_pools(snapshot,image),
               'source_price_range':reader.source_price_range(snapshot,image)}
        (output/(name+'.json')).write_text(json.dumps(value,indent=2,allow_nan=False));return value
    def measure(page,image,geometry,kwargs):
        before=cpu_usage();s=read_original(page,image,geometry,**kwargs);after=cpu_usage()
        return s,{'hover_seconds':s['duration_seconds'],'rows':len(s['rows']),'tooltip_reads':s['tooltip_reads'],
                  'row_digest':digest(rows_key(s)),'source_time':s['source_time'],
                  'cpu_seconds':None if before is None or after is None else round(after-before,6)}
    def own_image(page,tf,image,geometry,axes):
        state=readiness.wait_for_render(page,tf,3)
        require(state.get('ready') is True,'render_not_ready')
        require(state.get('price_axis_ticks')==axes,'source_axis_changed')
        require(readiness.geometry_from_state(state)==geometry,'chart_geometry_changed')
        reader.require_cookie_banner_hidden(page)
        page.screenshot(path=str(image),full_page=False)
        readiness.verify_saved_render(page,tf,3,state,image,readiness.CaptureNetworkDiagnostics(page))
    def anchors(page,s):
        bounds=s['plot_bounds'];scroll=page.evaluate('()=>window.scrollY')
        for i in (0,len(s['rows'])//2,len(s['rows'])-1):
            expected=s['rows'][i]
            y=bounds['top']+expected['y_fraction']*(bounds['bottom']-bounds['top'])-scroll
            page.mouse.move(bounds['right']-.25,y)
            script=r'''({expected})=>new Promise(resolve=>{const read=('''+reader.READ_EXPECTED_TOOLTIP+r''');
              const end=performance.now()+700;function check(){const v=read(expected);
              if(v||performance.now()>=end){resolve(v);return}setTimeout(check,25)}check()})'''
            text=page.evaluate(script,{'expected':expected['price']});require(bool(text),'restored_anchor_timeout')
            actual=reader.parse_tooltip(text)
            require(all(actual[k]==expected[k] for k in ('price','value','unit','source_time')),'restored_snapshot_changed')
        reader.verify_source_axis(page,s['price_axis_ticks'],s['rows'])
    def paired(page,image,geometry,**kwargs):
        tf=kwargs['timeframe'];pair={'timeframe':tf,'status':'running','B':[]};report['pairs'].append(pair)
        entry=candidate.require_ready(page,installs[id(page.context)])
        require(not entry['enabled'],'baseline_render_gate_enabled')
        require(entry['canvas']['rect']=={k:geometry[k] for k in ('x','y','width','height')},'candidate_canvas_is_not_selected_map')
        if 'graphics_backend' not in report:
            session=page.context.browser.new_browser_cdp_session()
            try:report['graphics_backend']=session.send('SystemInfo.getInfo').get('gpu',{}).get('auxAttributes',{}).get('glRenderer')
            finally:session.detach()
        emit('pair_started',timeframe=tf)
        a,pair['A']=measure(page,image,geometry,kwargs);first=evidence(tf+'-A',a,image)
        emit('A_completed',timeframe=tf,**pair['A']);persist()
        control=output/(tf+'-control.png');own_image(page,tf,control,geometry,a['price_axis_ticks'])
        pair['baseline_control_pixels']=image_compare(image,control,a['plot_bounds'])
        require(pair['baseline_control_pixels']['equal'],'clean_baseline_changed_before_candidate')
        try:
            enabled=candidate.enable(page,fps=fps)
            require(enabled['enabled'] and enabled['ticker_count']==entry['ticker_count'],'candidate_listener_contract_changed')
            for repetition in (1,2):
                second_image=output/(tf+'-B'+str(repetition)+'.png')
                own_image(page,tf,second_image,geometry,a['price_axis_ticks'])
                b,measurement=measure(page,second_image,geometry,kwargs)
                second=evidence(tf+'-B'+str(repetition),b,second_image)
                checks={'row_values_exact':rows_key(a)==rows_key(b),
                        'identity_exact':all(a.get(k)==b.get(k) for k in ('heatmap_model','timeframe','metric','unit','source_time','scope')),
                        'axis_exact':a['price_axis_ticks']==b['price_axis_ticks'],'plot_bounds_exact':a['plot_bounds']==b['plot_bounds'],
                        'total_exact':a['total_value']==b['total_value'],
                        'pool_membership_and_values_exact':first['candidates']==second['candidates'],
                        'candle_range_exact':first['source_price_range']==second['source_price_range']}
                item={**measurement,'repetition':repetition,'checks':checks,
                      'pixels':image_compare(image,second_image,a['plot_bounds'])}
                pair['B'].append(item);persist();emit('B_completed',timeframe=tf,**item)
                require(all(checks.values()),'paired_snapshot_or_output_mismatch')
                require(item['pixels']['equal'],'paired_map_pixels_changed')
            pair['gate_after_B']=candidate.snapshot(page)
        finally:
            restored=candidate.disable(page)
            pair['restored']=not restored['enabled'] and restored['ticker_count']==entry['ticker_count'];persist()
        require(pair['restored'],'render_listener_not_restored');anchors(page,a)
        restored_image=output/(tf+'-restored.png');own_image(page,tf,restored_image,geometry,a['price_axis_ticks'])
        pair['restored_pixels']=image_compare(image,restored_image,a['plot_bounds'])
        require(pair['restored_pixels']['equal'],'restored_map_pixels_changed')
        pair.update(status='passed',restored_anchors=3);persist();emit('pair_completed',**pair)
        return a
    Browser.new_context,Browser.close,BrowserContext.close,Page.screenshot,reader.capture_column=new_context,close_browser,close_context,screenshot,paired
    begun=time.monotonic();old_alarm=signal.getsignal(signal.SIGALRM)
    def deadline(_signum,_frame):raise CheckFailure('lab_diagnostic_deadline')
    signal.signal(signal.SIGALRM,deadline);signal.alarm(1400)
    emit('started',timeframes=list(timeframes),experimental_fps=fps,original_reader=True,candidate_repetitions=2)
    try:
        for timeframe in timeframes:
            attempt={'timeframe':timeframe,'status':'running'};report['attempts'].append(attempt)
            execution._capture_events.clear()
            try:
                capture_heatmaps(output,timeframes=(timeframe,))
                attempt['status']='passed'
            except Exception as exc:
                detail=execution.failure_detail(exc)
                attempt.update(status='failed',error=safe_error(exc),source_failure=detail,
                               operations=list(execution._capture_events))
                for pair in report['pairs']:
                    if pair['status']=='running':pair.update(status='failed',error=safe_error(exc))
                emit('timeframe_failed',**attempt);persist()
                reasons=[detail.get('source_readiness',{}).get('reason')]
                state=detail.get('source_capture_state') or {}
                if (getattr(exc,'code',None)=='source_login_required' or
                    detail.get('blocking_condition') in ('login','challenge') or
                    any(r in ('request-limit','challenge','login-required') for r in reasons) or
                    state.get('challenge_visible') is True or state.get('request_limit_visible') is True):
                    raise
        require(all(a['status']=='passed' for a in report['attempts']),'source_timeframe_not_passed')
        require(len(report['pairs'])==len(timeframes) and all(p['status']=='passed' for p in report['pairs']),'pair_not_passed')
        require(bool(report['cleanup']) and not any(x.get('error') for x in report['cleanup']),'final_restore_failed')
        report['status']='passed'
    except Exception as exc:
        report['status']='partial' if any(p['status']=='passed' for p in report['pairs']) else 'failed'
        report['error']=safe_error(exc)
        report['source_failure']=execution.failure_detail(exc)
        for pair in report['pairs']:
            if pair['status']=='running':pair.update(status='failed',error=report['error'])
    finally:
        signal.alarm(0);signal.signal(signal.SIGALRM,old_alarm)
        Browser.new_context,Browser.close,BrowserContext.close,Page.screenshot,reader.capture_column=originals
        report['wall_seconds']=round(time.monotonic()-begun,3)
        report['reader_unchanged']=hashlib.sha256(reader_path.read_bytes()).hexdigest()==READER_SHA
        report['capture_unchanged']=hashlib.sha256(capture_path.read_bytes()).hexdigest()==CAPTURE_SHA
        persist();emit('finished',status=report['status'],error=report.get('error'),wall_seconds=report['wall_seconds'],
                       reader_unchanged=report['reader_unchanged'],capture_unchanged=report['capture_unchanged'])
    return 0 if report['status']=='passed' else 1

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--timeframes',default='48h,12h,24h')
    parser.add_argument('--runtime',type=Path,default=Path.cwd()/'standalone_collector/runtime')
    parser.add_argument('--output',type=Path);parser.add_argument('--fps',type=float,default=.5)
    args=parser.parse_args();tfs=tuple(args.timeframes.split(','))
    require(1<=len(tfs)<=3 and len(set(tfs))==len(tfs) and all(t in ('12h','24h','48h') for t in tfs),'invalid_lab_timeframes')
    require(.1<=args.fps<=5,'invalid_lab_fps')
    return run(tfs,args.runtime.resolve(),args.fps,args.output)

if __name__=='__main__':sys.exit(main())
