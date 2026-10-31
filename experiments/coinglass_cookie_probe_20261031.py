"""One source-only probe of the unchanged 24H collector; private observations."""
from __future__ import annotations
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time

from coinglass_concurrency_20261031 import (
    BASE_SHA, SERVICE, safe_environment, cgroup_snapshot, clean_group,
    emit_evidence, proc_stats, process_members, clean_tracked_children, stamp,
)


def consent_state(page):
    decline = page.locator('#termly-code-snippet-support').get_by_role('button', name='Decline', exact=True)
    banner = page.get_by_text(re.compile(r'We use essential cookies to make our site work'))
    count = decline.count()
    return {
        'scoped_decline_count': count,
        'scoped_decline_visible': decline.is_visible() if count == 1 else None,
        'scoped_decline_enabled': decline.is_enabled() if count == 1 else None,
        'banner_count': banner.count(),
        'banner_visible': [banner.nth(i).is_visible() for i in range(min(banner.count(), 3))],
        'consent_controls': page.evaluate('''() => {
          const labels = /^(Decline|Accept|Accept all|Allow all|Cookie Preferences)$/i;
          const visible = e => { const r=e.getBoundingClientRect(),s=getComputedStyle(e);
            return !!(r.width && r.height && s.visibility!=='hidden' && s.display!=='none'); };
          return [...document.querySelectorAll('button,[role="button"]')]
            .filter(e => labels.test((e.innerText||e.textContent||'').trim())).slice(0,12).map(e => {
              const r=e.getBoundingClientRect(); let p=e, ancestors=[];
              for(let i=0;p && i<6;i++,p=p.parentElement) ancestors.push({tag:p.tagName,id:p.id.slice(0,150)});
              const hit=document.elementFromPoint(r.x+r.width/2,r.y+r.height/2);
              return {label:(e.innerText||e.textContent||'').trim(),visible:visible(e),
                disabled:!!e.disabled,aria_disabled:e.getAttribute('aria-disabled'),
                rect:{x:r.x,y:r.y,width:r.width,height:r.height},
                center_hits_control:hit===e||e.contains(hit),ancestors};
            }); }'''),
    }


def observe_call(original, page, record, snapshot, screenshot, **kwargs):
    """Call the original once; diagnostics cannot replace its outcome."""
    def safe(label, fn):
        began=time.monotonic()
        try: record[label]=fn()
        except Exception as exc: record[label]={'observation_error_class':type(exc).__name__}
        record[label+'_seconds']=time.monotonic()-began
    safe('before', snapshot)
    safe('before_image', lambda:screenshot('before'))
    safe('immediately_before_call', snapshot)
    began=time.monotonic()
    try:
        result=original(page, **kwargs)
        record['original_succeeded']=True
        return result
    except Exception as exc:
        record['original_succeeded']=False
        record['exception_class']=type(exc).__name__
        raise
    finally:
        record['original_seconds']=time.monotonic()-began
        safe('after', snapshot)
        safe('after_image', lambda:screenshot('after'))


def child(runtime, output):
    env=safe_environment(os.environ)
    for key in ('OPENAI_API_KEY','OPENAI_MARKET_SCANNER_MODEL'):env.pop(key,None)
    os.environ.clear();os.environ.update(env)
    sys.path.insert(0,str(runtime))
    import model1_execution as execution
    import heatmap_hover as hover
    from market_vision.coinglass_heatmap_capture import capture_heatmaps
    original=hover.dismiss_cookie_banner
    hidden=hover.require_cookie_banner_hidden
    report={'started_at':stamp(),'mode':'source_only_24H_cookie_probe','original_calls':[],
            'trace':[],'ai_called':False,'database_credentials_passed':False}
    epoch=time.monotonic()
    original_trace=sys.gettrace()
    tracked_codes={original.__code__,hidden.__code__}
    def trace(frame,event,arg):
        if frame.f_code not in tracked_codes:return None
        if event in ('call','return','exception') and len(report['trace'])<100:
            item={'function':frame.f_code.co_name,'event':event,'line':frame.f_lineno,
                  'seconds':time.monotonic()-epoch}
            if event=='exception':item['exception_class']=arg[0].__name__
            report['trace'].append(item)
        return trace
    def wrapped(page, *, model=1):
        item={'model':model,'seconds':time.monotonic()-epoch}
        report['original_calls'].append(item)
        def picture(label):
            name=f'cookie-{len(report["original_calls"])}-{label}.jpg'
            data=page.screenshot(type='jpeg',quality=65,full_page=False,timeout=6000)
            if len(data)>1500000:raise ValueError('diagnostic_image_limit')
            (output/name).write_bytes(data)
            return {'name':name,'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)}
        sys.settrace(trace)
        try:return observe_call(original,page,item,lambda:consent_state(page),picture,model=model)
        finally:sys.settrace(original_trace)
    hover.dismiss_cookie_banner=wrapped
    execution.stage('capture')
    try:
        images=capture_heatmaps(output/'capture',timeframes=('24h',))
        report['success']=len(images)==1
        report['capture_results']=[{'timeframe':x.get('timeframe'),'hover_snapshot':x.get('hover_snapshot'),
            'image_sha256':hashlib.sha256(Path(x['image']).read_bytes()).hexdigest()} for x in images]
    except Exception as exc:
        report['success']=False
        report['failure']=execution.failure_detail(exc)
    finally:
        hover.dismiss_cookie_banner=original
        sys.settrace(original_trace)
        report.update(finished_at=stamp(),elapsed_seconds=time.monotonic()-epoch,
                      forbidden_modules_loaded=sorted(set(sys.modules)&{'collection_bridge','psycopg','psycopg2',
                        'market_vision.openai_heatmap_scanner'}))
        (output/'probe.json').write_text(json.dumps(report,ensure_ascii=False,allow_nan=False))
    return 0 if report['success'] else 1


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--runtime',type=Path,required=True)
    p.add_argument('--child',action='store_true')
    p.add_argument('--output',type=Path)
    args=p.parse_args();os.umask(0o077)
    runtime=args.runtime.resolve()
    if args.child:return child(runtime,args.output)
    if (os.getenv('CG2_ISOLATED_ONE_OFF')!='1' or os.getenv('RENDER_SERVICE_ID')!=SERVICE
        or os.getenv('RENDER_GIT_COMMIT')!=BASE_SHA):raise RuntimeError('isolated_base_guard')
    env=safe_environment(os.environ)
    for key in ('OPENAI_API_KEY','OPENAI_MARKET_SCANNER_MODEL'):env.pop(key,None)
    os.environ.clear();os.environ.update(env)
    group=cgroup_snapshot()
    if (group.get('cpu.max')!='200000 100000' or group.get('memory.max')!=4294967296
        or int(Path('/proc/self/stat').read_text().split()[0])!=os.getpid()):
        raise RuntimeError('probe_hardware_or_namespace_guard')
    report={'experiment':'cookie-reproduction','started_at':stamp(),'hardware':group,'source_scans':1,
            'ai_calls':0,'runtime_cookie_reader_sha256':hashlib.sha256((runtime/'heatmap_hover.py').read_bytes()).hexdigest()}
    with tempfile.TemporaryDirectory(prefix='cg2-cookie-private-') as temporary:
        root=Path(temporary); log=(root/'private.log').open('w')
        proc=subprocess.Popen([sys.executable,__file__,'--child','--runtime',str(runtime),'--output',str(root)],
            env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        job={'seen_processes':{}};deadline=time.monotonic()+360;reason=None
        try:
            while proc.poll() is None:
                stats=proc_stats()
                for pid in process_members(proc.pid,stats):job['seen_processes'][(pid,stats[pid]['start'])]=True
                g=cgroup_snapshot()
                if time.monotonic()>=deadline:reason='360_second_deadline'
                if g.get('memory.current',0)>.85*group['memory.max']:reason='memory_stop'
                if reason:break
                time.sleep(.5)
        finally:
            clean_group(proc);clean_tracked_children(job);log.close()
        remaining=proc_stats()
        report['surviving_processes']=sum(1 for (pid,start) in job['seen_processes']
            if pid in remaining and remaining[pid]['start']==start and remaining[pid]['state']!='Z')
        report.update(returncode=proc.returncode,stop_reason=reason,finished_at=stamp(),cgroup_after=cgroup_snapshot())
        if (root/'probe.json').is_file():report['probe']=json.loads((root/'probe.json').read_text())
        else:report['probe']={'missing':True}
        report['images']=[{'name':x.name,'sha256':hashlib.sha256(x.read_bytes()).hexdigest(),
            'base64':base64.b64encode(x.read_bytes()).decode()} for x in sorted(root.glob('cookie-*.jpg'))]
        emit_evidence(report)
    return int(bool(reason) or report['surviving_processes']>0 or not report['probe'].get('success'))


if __name__=='__main__':raise SystemExit(main())
