"""Prepared, not automatically run: five source-only captures in a Render one-off.

Use the previously verified diagnostic helpers, and private copies of the base
and candidate runtimes. No AI, queue, DB, deployment, settings or daily job.
"""
import argparse
import base64
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager

BASE='f6adfc6fea05bfd82a59098433ad1c2f31f6267f'
PAIRS=(('base',3,'12H'),('candidate',3,'12H'),('candidate',3,'24H'),
       ('candidate',3,'48H'),('candidate',2,'12H'))


def load(path,name):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def child(args,diagnostic):
    env=diagnostic.safe_env(os.environ,args.model);os.environ.clear();os.environ.update(env)
    sys.path.insert(0,str(args.runtime))
    import model1_execution as execution
    original=execution.capture_operation
    operations=[]
    @contextmanager
    def operation(name):
        before=diagnostic.resources.cgroup_snapshot();at=time.monotonic();success=False
        try:
            with original(name):yield
            success=True
        finally:
            operations.append({'name':name,'success':success,**diagnostic.summarize_delta(
                before,diagnostic.resources.cgroup_snapshot(),time.monotonic()-at)})
    execution.capture_operation=operation
    probe=load(args.source_check,'source_check');probe.HERE=args.runtime.parent
    probe.single(args.model,args.timeframe,args.output)
    report=json.loads((args.output/'report.json').read_text())
    forbidden=sorted(set(sys.modules)&diagnostic.FORBIDDEN)
    report.update(operations=operations,forbidden_modules_loaded=forbidden)
    (args.output/'report.json').write_text(json.dumps(report,allow_nan=False))
    return int(report['status']!='captured' or bool(forbidden))


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--diagnostic',type=Path,required=True)
    parser.add_argument('--source-check',type=Path,required=True)
    parser.add_argument('--base-runtime',type=Path);parser.add_argument('--candidate-runtime',type=Path)
    parser.add_argument('--manifest',type=Path);parser.add_argument('--runtime',type=Path)
    parser.add_argument('--model',type=int,choices=(2,3));parser.add_argument('--timeframe',choices=('12H','24H','48H'))
    parser.add_argument('--output',type=Path);args=parser.parse_args();os.umask(0o077)
    diagnostic=load(args.diagnostic,'accepted_diagnostic')
    if args.model:return child(args,diagnostic)
    if (os.getenv('CG3_EARLY_SOURCE_ONE_OFF')!='1' or os.getenv('RENDER_GIT_COMMIT')!=BASE
        or os.getenv('RENDER_SERVICE_ID')!=diagnostic.SERVICE):raise RuntimeError('one_off_base_guard_failed')
    diagnostic.require_limits(diagnostic.resources.cgroup_snapshot())
    manifest=json.loads(args.manifest.read_text())
    for variant,root in (('base',args.base_runtime),('candidate',args.candidate_runtime)):
        actual={name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in manifest[variant]}
        if actual!=manifest[variant]:raise RuntimeError('runtime_hash_mismatch')
    env=diagnostic.safe_env(os.environ,3);os.environ.clear();os.environ.update(env)
    if not any(env.get(k) for k in diagnostic.SESSION):raise RuntimeError('source_session_missing')
    summary={'base':BASE,'source_calls_limit':5,'ai_calls':0,'database_writes':0,'runs':[]}
    with tempfile.TemporaryDirectory(prefix='cg3-early-source-') as folder:
        for index,(variant,model,tf) in enumerate(PAIRS):
            root=args.base_runtime if variant=='base' else args.candidate_runtime
            out=Path(folder)/str(index);out.mkdir();tracked={'seen_processes':{}}
            samples=[];stop=None;cleanup=[];started=time.monotonic()
            with (out/'child.log').open('w') as log:
                p=subprocess.Popen([sys.executable,str(Path(__file__).resolve()),'--diagnostic',str(args.diagnostic),
                    '--source-check',str(args.source_check),'--runtime',str(root),'--model',str(model),
                    '--timeframe',tf,'--output',str(out)],env=diagnostic.safe_env(env,model),
                    stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
                try:
                    while p.poll() is None:
                        stats=diagnostic.resources.proc_stats()
                        for pid in diagnostic.resources.process_members(p.pid,stats):tracked['seen_processes'][(pid,stats[pid]['start'])]=True
                        group=diagnostic.resources.cgroup_snapshot();samples.append({'seconds':time.monotonic()-started,'cgroup':group})
                        if group['memory.current']>group['memory.max']*.85:stop='memory_limit';break
                        if time.monotonic()-started>300:stop='timeout';break
                        time.sleep(.5)
                finally:
                    for clean in (lambda:diagnostic.resources.clean_group(p),lambda:diagnostic.resources.clean_tracked_children(tracked)):
                        try:clean()
                        except Exception as error:cleanup.append(type(error).__name__)
            report=json.loads((out/'report.json').read_text()) if (out/'report.json').exists() else {'status':'missing'}
            stats=diagnostic.resources.proc_stats()
            survivors=sum(pid in stats and stats[pid]['start']==at and stats[pid]['state']!='Z' for pid,at in tracked['seen_processes'])
            events=[json.loads(line.removeprefix('MODEL3_RENDER ')) for line in (out/'child.log').read_text().splitlines() if line.startswith('MODEL3_RENDER ')]
            # Only known measurement/source artifacts; never child logs or session files.
            artifacts={p.name:base64.b64encode(p.read_bytes()).decode() for p in out.iterdir()
                       if p.name=='hover.json' or p.name.endswith(('.png','.render.json'))}
            summary['runs'].append({'variant':variant,'model':model,'timeframe':tf,'returncode':p.returncode,
                'stop':stop,'cleanup_errors':cleanup,'survivors':survivors,'report':report,
                'samples':samples,'render_events':events,'artifacts':artifacts})
            if p.returncode or stop or cleanup or survivors or report.get('status')!='captured':break
    summary['completed']=len(summary['runs'])==5 and all(x['returncode']==0 and not x['survivors'] and
        not x['cleanup_errors'] and x['report'].get('status')=='captured' for x in summary['runs'])
    encoded=json.dumps(summary,separators=(',',':'),allow_nan=False).encode()
    packed=base64.b64encode(gzip.compress(encoded)).decode();parts=[packed[i:i+7000] for i in range(0,len(packed),7000)]
    for i,part in enumerate(parts):print('CG3EARLY '+json.dumps({'index':i,'count':len(parts),'data':part}),flush=True)
    print('CG3EARLY '+json.dumps({'event':'complete','sha256':hashlib.sha256(encoded).hexdigest(),'bytes':len(encoded)}),flush=True)
    return int(not summary['completed'])

if __name__=='__main__':raise SystemExit(main())
