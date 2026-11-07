"""Disposable native PostgreSQL, actual worker and browser. No live credentials."""
import hashlib,json,os,signal,subprocess,sys,threading,time,urllib.request
from pathlib import Path
from types import SimpleNamespace
import psycopg,psutil
from psycopg.rows import dict_row
LAB=Path(__file__).resolve().parent;ROOT=LAB.parents[2]
OUT=Path(os.environ['QUEUE_LAB_EVIDENCE']).resolve();OUT.mkdir(parents=True,exist_ok=True)
COLLECTOR=Path(os.environ['QUEUE_LAB_COLLECTOR']).resolve()
BASE='postgresql://postgres:postgres@127.0.0.1:5432/'
with psycopg.connect(BASE+'postgres',autocommit=True) as db:
    db.execute('create database queue_lab');db.execute('create database legacy_transaction_test')
with psycopg.connect(BASE+'queue_lab',autocommit=True) as db:
    db.execute("alter database queue_lab set lab.now = '2026-10-31T10:06:00Z'")
    db.execute("create or replace function pg_catalog.now() returns timestamptz language sql stable as $$select current_setting('lab.now')::timestamptz$$")
control=OUT/'control.json';control.write_text(json.dumps({'now':'2026-10-31T10:06:00Z','failedMap':None}))
env={k:os.environ[k] for k in ('PATH','HOME','LANG','PLAYWRIGHT_BROWSERS_PATH') if k in os.environ}
env.update(PYTHONPATH=str(LAB),QUEUE_LAB_COLLECTOR=str(COLLECTOR),QUEUE_LAB_CONTROL=str(control),QUEUE_LAB_AUDIT=str(OUT/'children'),COLLECTION_BRIDGE_ENABLED='true',DATABASE_URL=BASE+'queue_lab',COLLECTION_BRIDGE_HOURLY_LIMIT='20',OPENAI_API_KEY='offline-fixture-no-live-key',COINGLASS_COOKIE_HEADER='lab=fixture',COINGLASS_COLLECTOR_TOKEN='z'*64)
processes={};logs={};stop=threading.Event();samples=[];transitions=[];monitor_errors=[];cpu_seen={};code=None
started=time.monotonic()
containers=subprocess.check_output(['docker','ps','--filter','ancestor=postgres:16.10','--format','{{.ID}}'],text=True).split()
assert len(containers)==1,'Expected exactly one disposable PostgreSQL service'
database_pid=int(subprocess.check_output(['docker','inspect','--format','{{.State.Pid}}',containers[0]],text=True))
resource_groups=('collector','application','browser','database')
def launch(name,args,environment):
    logs[name]=(OUT/(name+'.log')).open('w')
    processes[name]=subprocess.Popen(args,cwd=ROOT,env=environment,stdout=logs[name],stderr=subprocess.STDOUT,start_new_session=True)
    return processes[name]
def ready(url,process,timeout=120):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        try:
            with urllib.request.urlopen(url,timeout=2) as r:return json.load(r)
        except Exception:
            if process.poll() is not None:raise RuntimeError('Service exited during startup')
            time.sleep(.2)
    raise RuntimeError('Loopback startup timeout')
def monitor():
    try:
        while not stop.wait(.2):
            with psycopg.connect(BASE+'queue_lab',row_factory=dict_row) as db:
                rows=db.execute('select id::text,heatmap_model,timeframe,status from public.ai_collection_bridge_jobs order by created_at,id').fetchall()
            running=[r for r in rows if r['status']=='running'];assert len(running)<=1
            state={'rows':rows,'running':len(running)}
            if not transitions or state!=transitions[-1]['state']:transitions.append({'wall_seconds':time.monotonic()-started,'state':state})
            groups={}
            for name,proc in list(processes.items())+[('database',SimpleNamespace(pid=database_pid))]:
                rss=0;cpu=0;count=0
                try: descendants=[psutil.Process(proc.pid)]+psutil.Process(proc.pid).children(recursive=True)
                except psutil.Error:descendants=[]
                for child in descendants:
                    try:
                        mem=child.memory_info();t=child.cpu_times();v=t.user+t.system
                        rss+=mem.rss;cpu+=v;count+=1
                        cpu_seen[(name,child.pid,child.create_time())]=max(cpu_seen.get((name,child.pid,child.create_time()),0),v)
                    except psutil.Error:pass
                groups[name]={'rss_bytes_sum':rss,'live_cpu_seconds':cpu,'processes':count}
            samples.append({'wall_seconds':round(time.monotonic()-started,3),'job_id':running[0]['id'] if running else None,'groups':groups})
    except Exception as e:monitor_errors.append(repr(e))
thread=None
try:
    collector=launch('collector',[sys.executable,str(LAB/'serve.py')],env)
    health=ready('http://127.0.0.1:21879/health',collector)
    assert health['mode']=='collection';(OUT/'health.json').write_text(json.dumps(health,indent=2))
    thread=threading.Thread(target=monitor,daemon=True);thread.start()
    subprocess.run([sys.executable,str(LAB/'prepare.py')],check=True,cwd=ROOT)
    subprocess.run(['bun',str(LAB/'build-ui.mjs')],check=True,cwd=ROOT)
    appenv=dict(os.environ,QUEUE_LAB_APP_DSN=BASE+'legacy_transaction_test')
    backend=launch('application',['node',str(ROOT/'.github/labs/btc-full-flow.generated.mjs'),'b'],appenv)
    ready('http://127.0.0.1:21880/fixture',backend)
    browser=launch('browser',[sys.executable,str(LAB/'browser.py')],appenv)
    code=browser.wait(timeout=1500);logs['browser'].flush()
    assert code==0,(OUT/'browser.log').read_text()
    assert backend.wait(timeout=15)==0
    with psycopg.connect(BASE+'queue_lab',row_factory=dict_row) as db:
        rows=db.execute('select id::text,heatmap_model,timeframe,status,error_code,result,created_at::text,started_at::text,finished_at::text,image_png from public.ai_collection_bridge_jobs order by created_at,id').fetchall()
        assert len(rows)==19 and sum(r['status']=='ready' for r in rows)==18
        assert [(r['heatmap_model'],r['timeframe']) for r in rows if r['status']=='failed']==[(3,'24H')]
        assert db.execute('select count(*) n from public.ai_collection_bridge_requests').fetchone()['n']==20
        assert db.execute('select pg_try_advisory_lock(748230102) as ok').fetchone()['ok']
        db.execute('select pg_advisory_unlock(748230102)')
    children=[json.loads(p.read_text()) for p in (OUT/'children').glob('*.json')]
    assert len(children)==19 and len({r['pid'] for r in children})==19
    assert all(r['contexts']==r['screenshots']==r['ai_requests']==1 and r['browsers_closed'] for r in children)
    app=json.loads((OUT/'application-evidence.json').read_text())
    for row in rows:
        image=row.pop('image_png');child=next(c for c in children if c['run_id']==row['id'])
        row['scanner']=child
        per=[s for s in samples if s['job_id']==row['id']]
        row['collector_tree_peak_sampled_rss_mib']=round(max((s['groups']['collector']['rss_bytes_sum'] for s in per),default=0)/2**20,2)
        if image is not None:
            digest=hashlib.sha256(image).hexdigest();row['stored_image_sha256']=digest
            assert digest in child['ai_input_sha256']
            assert hashlib.sha256((OUT/'children'/row['id']/'capture.png').read_bytes()).hexdigest()==digest
            if row['status']=='ready':
                assert digest==row['result']['evidence']['sha256']
                save=next(r for r in app['runs'] if r['analysis']['collector_run_id']==row['id'])
                assert save['analysis']['evidence_sha256']==digest
                assert save['timeframe']==row['timeframe']
        if row['status']=='ready':
            assert row['result']['heatmap_model']==row['heatmap_model'] and row['result']['timeframe']==row['timeframe']
    assert not monitor_errors,monitor_errors
    assert any(t['state']['running']==1 for t in transitions)
    (OUT/'queue.json').write_text(json.dumps(rows,indent=2))
    (OUT/'summary.json').write_text(json.dumps({'status':'passed','jobs':19,'ready':18,'failed_deliberately':1,'peak_running_jobs':1,'production_writes':0,'external_source_calls':0,'external_ai_calls':0,'real_browser':True,'real_queue':True,'real_worker':True,'actual_scanner':True,'actual_application_core_sql_and_components':True,'hosted_auth_and_supabase_transport':False,'one_map_bad_identity_retry':True,'capture_ai_input_saved_hash_chain':True},indent=2))
finally:
    stop.set()
    if thread:thread.join(5)
    for name,p in reversed(list(processes.items())):
        if p.poll() is None:
            os.killpg(p.pid,signal.SIGTERM)
            try:p.wait(timeout=15)
            except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait(timeout=5)
        logs[name].close()
    (OUT/'resources.json').write_text(json.dumps({'sample_interval_seconds':.2,'metric':'sum of process RSS; shared pages may be counted more than once; sampled CPU is a lower bound for short lived processes','logical_cpus':os.cpu_count(),'cpu_affinity':len(os.sched_getaffinity(0)),'cgroup_cpu_max':Path('/sys/fs/cgroup/cpu.max').read_text().strip() if Path('/sys/fs/cgroup/cpu.max').exists() else None,'total_wall_seconds':time.monotonic()-started,'sampled_cpu_seconds_by_group':{name:round(sum(v for (n,_,_),v in cpu_seen.items() if n==name),3) for name in resource_groups},'peak_sampled_rss_mib_by_group':{name:round(max((s['groups'].get(name,{}).get('rss_bytes_sum',0) for s in samples),default=0)/2**20,2) for name in resource_groups},'samples':samples},indent=2))
    (OUT/'queue-transitions.json').write_text(json.dumps(transitions,indent=2))
    (OUT/'cleanup.json').write_text(json.dumps({'processes':{n:{'exit':p.returncode,'stopped':p.poll() is not None} for n,p in processes.items()},'monitor_errors':monitor_errors,'browser_exit':code},indent=2))
    for name in logs:
        content=(OUT/(name+'.log')).read_text();print(name+' LOG\n'+content[-16000:],flush=True)
    (ROOT/'.github/labs/btc-full-flow.generated.mjs').unlink(missing_ok=True)
