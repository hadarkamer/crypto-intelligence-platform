"""Test-only source timing and resource sampling; no AI, database or worker."""
import argparse
import base64
import io
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time
import threading
HERE=Path(__file__).resolve().parent
FOCUSED_CASES=((2,'12H'),(3,'48H'))

def emit(kind,value):
    print(json.dumps({'diagnostic':kind,**value},separators=(',',':')),flush=True)

def install_observer():
    sys.path.insert(0,str(HERE/'runtime'))
    import heatmap_hover as hover
    from capture_failure_observer import STATE_SCRIPT
    original,original_close=hover.capture_column,hover.close_heatmap_settings
    original_cookie=hover.dismiss_cookie_banner
    def observed_cookie(page):
        start=time.monotonic()
        control=page.locator('#termly-code-snippet-support').get_by_role('button',name='Decline',exact=True)
        before=control.count()==1 and control.is_visible()
        try:
            original_cookie(page)
            emit('cookie_gate',{'visible_before':before,'hidden_after':True,'seconds':round(time.monotonic()-start,3)})
        except hover.HoverError:
            emit('cookie_gate',{'visible_before':before,'hidden_after':False})
            raise
    hover.dismiss_cookie_banner=observed_cookie
    def cpu_state():
        for path in ('/sys/fs/cgroup/cpu.stat','/sys/fs/cgroup/cpu/cpu.stat'):
            try:return {k:int(v) for k,v in (line.split() for line in Path(path).read_text().splitlines())}
            except (OSError,ValueError):pass
        return {}
    def control_state(page):
        panel=page.get_by_role('dialog',name='Heatmap settings',exact=True)
        result={'panel_count':panel.count()}
        if result['panel_count']!=1:return result
        result['panel_visible']=panel.is_visible()
        close=panel.get_by_role('button',name='Close heatmap settings',exact=True)
        result['close_count']=close.count()
        if result['close_count']==1:
            result['close']=close.evaluate('''e=>{
                const r=e.getBoundingClientRect(),s=getComputedStyle(e);
                const h=document.elementFromPoint(r.x+r.width/2,r.y+r.height/2);
                return {tag:e.tagName,disabled:!!e.disabled,display:s.display,
                    visibility:s.visibility,pointerEvents:s.pointerEvents,
                    rect:{x:r.x,y:r.y,width:r.width,height:r.height},
                    hitTarget:!!h&&(h===e||e.contains(h)),
                    hit:h?{tag:h.tagName,role:h.getAttribute('role'),label:h.getAttribute('aria-label')}:null,
                    scrollY,viewport:{width:innerWidth,height:innerHeight}};
            }''')
        return result
    def observed_close(page):
        start=time.monotonic()
        try:return original_close(page)
        except Exception as exc:
            lines=[line.strip()[:400] for line in str(exc).splitlines()
                if any(w in line.lower() for w in ('timeout','locator','waiting','element','click','scroll','intercept','stable','viewport'))]
            emit('close_failure',{'seconds':round(time.monotonic()-start,3),
                'after':control_state(page),'action_log':lines[:20]})
            raise
        finally:emit('close_timing',{'seconds':round(time.monotonic()-start,3)})
    class ObservedMouse:
        def __init__(self,mouse,samples):self.mouse,self.samples=mouse,samples
        def __getattr__(self,name):return getattr(self.mouse,name)
        def move(self,x,y,**kwargs):
            start=time.monotonic()
            try:return self.mouse.move(x,y,**kwargs)
            finally:self.samples.append({'operation':'move','ms':(time.monotonic()-start)*1000})
    class ObservedPage:
        def __init__(self,page):
            self.page,self.samples=page,[]
            self.mouse=ObservedMouse(page.mouse,self.samples)
        def __getattr__(self,name):return getattr(self.page,name)
        def wait_for_timeout(self,timeout):
            start=time.monotonic()
            try:return self.page.wait_for_timeout(timeout)
            finally:self.samples.append({'operation':'driver_event_wait','ms':(time.monotonic()-start)*1000})
        def evaluate(self,expression,arg=None):
            start=time.monotonic()
            step='clear' if '=== null' in expression else 'expected' if 'replaceAll' in expression else 'show'
            sample={'operation':step if 'new Promise' in expression else 'other_evaluate'}
            try:
                if 'new Promise' not in expression:return self.page.evaluate(expression,arg)
                result=self.page.evaluate('''arg=>{
                    const start=performance.now();
                    return Promise.resolve(('''+expression+''')(arg)).then(value=>
                        ({value,browser_ms:performance.now()-start}));
                }''',arg)
                sample['browser_ms']=result['browser_ms'];sample['present']=bool(result['value'])
                return result['value']
            finally:
                sample['ms']=(time.monotonic()-start)*1000
                self.samples.append(sample)
    def observed_capture(page,image,geometry,**kwargs):
        probe=ObservedPage(page);start=time.monotonic();cpu_before=cpu_state()
        emit('before_reader',{'model':kwargs['model'],'timeframe':kwargs['timeframe'],'state':page.evaluate(STATE_SCRIPT,kwargs['model'])})
        try:
            result=original(probe,image,geometry,**kwargs)
            # Keep evidence only in private build logs; never serve it publicly.
            from PIL import Image
            with Image.open(image) as source:
                picture=source.convert('RGB');picture.thumbnail((1400,1400));buffer=io.BytesIO()
                picture.save(buffer,format='JPEG',quality=80);picture.close()
            for name,data in (('map.jpg',buffer.getvalue()),('input-hover.json',json.dumps(result).encode())):
                if len(data)>262144:raise ValueError('Diagnostic evidence too large')
                encoded=base64.b64encode(data).decode();chunks=[encoded[i:i+6000] for i in range(0,len(encoded),6000)]
                for index,chunk in enumerate(chunks):
                    emit('source_artifact',{'model':kwargs['model'],'timeframe':kwargs['timeframe'],'name':name,'index':index,'count':len(chunks),'data':chunk})
            emit('reader_completed',{'timeframe':kwargs['timeframe'],'rows':len(result['rows']),
                'top_price':result['rows'][0]['price'],'bottom_price':result['rows'][-1]['price'],
                'price_step':result['price_step'],'bounds':result['plot_bounds']})
            return result
        except hover.HoverError as exc:
            trace,progress=exc.__traceback__,{}
            while trace is not None:
                if trace.tb_frame.f_code.co_name in ('capture_column','_capture_column'):
                    scope=trace.tb_frame.f_locals
                    progress={'read_count':scope.get('read_count'),'rows_collected':len(scope.get('rows',[])),
                        'grid_count':scope.get('count'),'calibration_prices':[r['price'] for r in scope.get('calibration',[])],
                        'top_price':(scope.get('top_row') or {}).get('price'),'bottom_price':(scope.get('bottom_row') or {}).get('price'),
                        'inner_bottom_price':(scope.get('last_inner') or {}).get('price'),'expected_price':scope.get('expected'),
                        'calibration_times':[r['source_time'] for r in scope.get('calibration',[])]}
                    del scope
                trace=trace.tb_next
            emit('reader_failure',{'timeframe':kwargs['timeframe'],'reason':exc.reason,**progress})
            raise
        finally:
            totals={}
            for operation in sorted({s['operation'] for s in probe.samples}):
                selected=[s for s in probe.samples if s['operation']==operation];values=[s['ms'] for s in selected]
                totals[operation]={'count':len(values),'sum_ms':round(sum(values),1),
                    'median_ms':round(statistics.median(values),1),'max_ms':round(max(values),1),
                    'browser_sum_ms':round(sum(s.get('browser_ms',0) for s in selected),1)}
            emit('latency_profile',{'timeframe':kwargs['timeframe'],'total_seconds':round(time.monotonic()-start,3),
                'viewport':page.viewport_size,'cpu_delta':{k:v-cpu_before.get(k,v) for k,v in cpu_state().items()},
                'operations':totals,'slowest':sorted(probe.samples,key=lambda s:s['ms'],reverse=True)[:5]})
    hover.close_heatmap_settings=observed_close
    hover.capture_column=observed_capture

class ResourceSample:
    """Sample the isolated BUILD cgroup, not production or one Chrome process."""
    def __init__(self):
        self.stop=threading.Event();self.peak={};self.samples=0
        self.thread=threading.Thread(target=self.observe,daemon=True)
    def observe(self):
        while not self.stop.is_set():
            try:
                root=Path('/sys/fs/cgroup')
                values={'current':int((root/'memory.current').read_text())}
                values.update({k:int(v) for k,v in (line.split() for line in (root/'memory.stat').read_text().splitlines()) if k in ('anon','file')})
                for key,value in values.items():self.peak[key]=max(self.peak.get(key,0),value)
                self.samples+=1
            except (OSError,ValueError):pass
            self.stop.wait(.25)
    def __enter__(self):self.thread.start();return self
    def __exit__(self,*_):self.stop.set();self.thread.join(timeout=1)
    def report(self):
        result={'scope':'isolated_build_cgroup','sample_interval_seconds':.25,'samples':self.samples,
                'peak_mib':{k:round(v/1024**2,2) for k,v in self.peak.items()}}
        for name in ('cpu.max','memory.max'):
            try:result[name]=Path('/sys/fs/cgroup',name).read_text().strip()
            except OSError:pass
        return result

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--single',choices=('12H','24H','48H'))
    parser.add_argument('--model',type=int,choices=(1,2,3),default=3)
    parser.add_argument('--models12',action='store_true',help='Only the six still-pending Model 1/2 cases')
    parser.add_argument('--focused',action='store_true',help='Only Model 2/12H and Model 3/48H repair acceptance')
    args=parser.parse_args()
    from hover_source_check import single,run_probe
    if args.single:
        if os.environ.get('COLLECTION_HEATMAP_MODEL')!=str(args.model):raise RuntimeError('Diagnostic model mismatch')
        install_observer()
        single(args.model,args.single,args.output);return 0
    args.output.mkdir(parents=True,exist_ok=False);reports=[]
    cases=FOCUSED_CASES if args.focused else [(m,t) for m in (1,2) for t in ('12H','24H','48H')] if args.models12 else [(3,'48H'),(3,'12H'),(3,'24H')]
    for model,tf in cases:
        dest=args.output/f'model{model}-{tf}'
        env={**os.environ,'COLLECTION_HEATMAP_MODEL':str(model),'OPENAI_API_KEY':'','DATABASE_URL':'','COLLECTION_BRIDGE_ENABLED':'0'}
        with ResourceSample() as memory:
            try:
                run_probe([sys.executable,str(Path(__file__).resolve()),'--single',tf,'--model',str(model),'--output',str(dest)],env)
                report=json.loads((dest/'report.json').read_text())
            except (subprocess.SubprocessError,OSError,ValueError):
                report={'model':model,'timeframe':tf,'status':'failed','code':'probe_process_failed','ai_calls':0,'database_writes':0}
        report['resources']=memory.report()
        reports.append(report);emit('case_result',report)
        (args.output/'summary.json').write_text(json.dumps(reports,indent=2))
        if (report.get('source_capture_state') or {}).get('challenge_visible'):break
    return int(any(r['status']!='captured' for r in reports))
if __name__=='__main__':sys.exit(main())
