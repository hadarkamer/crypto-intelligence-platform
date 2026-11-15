#!/usr/bin/env python3
"""Exact source-only wrapper command + real private PG store and Chromium.

Only HTTP source responses are synthetic. No supervisor, lifecycle, persistence,
procfs, health, interpreter, lock, pipe or browser implementation is substituted.
The disabled actual app supplies the real worker environment and local health.
"""
from __future__ import annotations

import argparse
import ast
import atexit
import base64
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import sysconfig
import time
import urllib.request

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
RUNTIME = ROOT / 'standalone_collector/runtime'
CONFIG = HERE / 'active-wrapper.json'
PAGE_URL = 'https://www.coinglass.com/pro/futures/LiquidationHeatMap?coin=BTC&type=symbol'
ACCOUNT_URL = 'https://capi.coinglass.com/coin-community/api/userapi/info'
MAP_URL = 'https://capi.coinglass.com/api/index/aggregate/liqHeatMap?symbol=BTC&interval=5&limit=144'
COMMIT = '33bad18f8be322009108584a6b71f3d65fa9b351'
COOKIE = 'fixture_session'
KEY = bytes(range(32))  # Public dummy fixture only.


class CheckFailed(Exception):
    pass


def check(condition, code):
    if not condition:
        raise CheckFailed(code)


def write_json(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, sort_keys=True, allow_nan=False))
    temporary.chmod(0o600)
    temporary.replace(path)


def generation(number):
    return 'DUMMY_WRAPPER_STATE_' + str(number)


def state(number):
    return {'cookies':[{'name':COOKIE,'value':generation(number),'domain':'.coinglass.com',
        'path':'/','expires':-1,'httpOnly':True,'secure':True,'sameSite':'Lax'}],'origins':[]}


def process_snapshot():
    rows = {}
    for entry in Path('/proc').iterdir():
        if not entry.name.isdecimal(): continue
        try:
            status = dict(line.split(':',1) for line in (entry/'status').read_text().splitlines() if ':' in line)
            rows[int(entry.name)] = (int(status['PPid']), (entry/'cmdline').read_bytes().split(b'\0'))
        except (OSError, KeyError, ValueError): pass
    return rows


def descendants(pid, rows):
    owned = {pid}
    while True:
        expanded = owned | {child for child,(parent,_) in rows.items() if parent in owned}
        if expanded == owned: return owned - {pid}
        owned = expanded


def html_fixture():
    path = ROOT/'experiments/model3-preparation-fix-20261031/check_capture.py'
    nodes = [n for n in ast.parse(path.read_text()).body if isinstance(n,ast.Assign)
        and any(isinstance(t,ast.Name) and t.id=='HTML' for t in n.targets)]
    check(len(nodes)==1,'existing_html_missing')
    html = ast.literal_eval(nodes[0].value).replace('MODEL','1').replace(
        'STAMP',datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M'))
    # Requests originate from the fixture page. The existing reader still clicks
    # Symbol/Model1/12H. Rendering stays hidden until the map response completes.
    return html + '''<script>
const panel=document.querySelector('#panel');panel.style.visibility='hidden';
const originalSelect=window.select;
window.select=h=>{
 if(h!==12)throw new Error('fixture range');
 document.querySelector('canvas').style.visibility='hidden';
 fetch(__MAP_URL__,{credentials:'include'}).then(r=>r.json()).then(v=>{
  if(v.success!==true)throw new Error('fixture map');
  originalSelect(h);document.querySelector('canvas').style.visibility='visible';
 });
};
fetch(__ACCOUNT_URL__,{credentials:'include'}).then(r=>r.json()).then(v=>{
 if(v.success===true)panel.style.visibility='visible';
});
</script>'''.replace('__MAP_URL__',json.dumps(MAP_URL)).replace('__ACCOUNT_URL__',json.dumps(ACCOUNT_URL))


def install_process_fixture(config):
    """Called by the disposable .pth before an unchanged wrapper command."""
    package = Path(config['package']).resolve(strict=True)
    role = Path(sys.argv[0]).name
    report_path = Path(config['evidence'])/(role+'.json')
    check(report_path.parent.is_dir() and report_path.parent.stat().st_mode&0o077==0,'private_evidence_missing')
    base = {'role':role,'pid':os.getpid(),'python':sys.version.split()[0],
        'real_entry_command':True,'crypto_loaded':False,'crypto_path_private':False}
    def process_complete():
        try:
            module=sys.modules.get('Crypto')
            if module is not None:
                loaded=Path(module.__file__).resolve()
                base['crypto_loaded']=True
                base['crypto_path_private']=loaded.is_relative_to(package/'vendor')
                base['crypto_version']=getattr(module,'__version__','unavailable')
                base['crypto_path_relative']=str(loaded.relative_to(package)) if base['crypto_path_private'] else 'outside_private_bundle'
            write_json(report_path,base)
        except BaseException: os._exit(98)
    atexit.register(process_complete)
    if role!='collector_browser_fixture.py': return
    check(sys.argv[1:]==['--live-worker'],'unexpected_child_command')
    check(os.geteuid()!=0,'root_browser_forbidden')
    check(sys.version_info[:3]==(3,11,11),'python_version_mismatch')
    check(importlib.metadata.version('playwright')=='1.55.0','playwright_version_mismatch')
    check('DATABASE_URL' not in os.environ and 'COINGLASS_SESSION_KEY' not in os.environ,'parent_secret_in_child')
    check(os.environ.get('COINGLASS_COOKIE_HEADER')=='','legacy_header_in_child')
    from playwright.sync_api import Browser, BrowserType, BrowserContext, Page
    original_launch,original_context=BrowserType.launch,Browser.new_context
    original_close,original_browser_close=BrowserContext.close,Browser.close
    original_shot=Page.screenshot
    browsers,contexts=[],[]
    browser_pids,profiles=set(),set()
    number=config['generation'];failure=config['failure']
    check(number in (1,2,3) and type(failure) is bool,'fixture_config_invalid')
    base.update(generation=number,browser_launches=0,contexts=0,document_requests=0,
        account_requests=0,map_requests=0,other_requests_aborted=0,account_cookie_matches=False,
        map_cookie_matches=False,obe_absent=True,updated_cookie_before_close=False,
        context_closed=False,browser_closed=False,profiles_removed=False,
        browser_processes_gone=False,parent_secrets_absent=True,legacy_header_absent=True,
        source_requests=0,ai_calls=0,internal_sandbox_enabled=False,
        dummy_only_exception=True,internal_sandbox_disabled_observed=False,
        failure_injected=False)
    html=html_fixture()
    def browser_processes():
        rows=process_snapshot()
        for pid in descendants(os.getpid(),rows):
            argv=rows[pid][1]
            if not argv or Path(os.fsdecode(argv[0])).name not in ('chrome','chromium','headless_shell','chrome-headless-shell'):continue
            browser_pids.add(pid)
            check(not any(arg in argv for arg in (b'--disable-web-security',b'--disable-site-isolation-trials',b'--allow-file-access-from-files')),'web_security_disabled')
            base['internal_sandbox_disabled_observed'] |= b'--no-sandbox' in argv
            for arg in argv:
                if arg.startswith(b'--user-data-dir='):
                    path=Path(os.fsdecode(arg.partition(b'=')[2])).resolve()
                    check(path.is_relative_to(Path(os.environ['TMPDIR']).resolve()),'profile_outside_owned_path')
                    profiles.add(path)
    def launch(owner,*args,**kwargs):
        check(owner.name=='chromium' and kwargs.get('headless') is True and not browsers,'unexpected_launch')
        kwargs['args']=[*kwargs.get('args',[]),'--disable-background-networking','--disable-component-update',
            '--no-first-run','--host-resolver-rules=MAP * ~NOTFOUND']
        value=original_launch(owner,*args,**kwargs)
        browsers.append(value);base['browser_launches']+=1;base['chromium_version']=value.version
        check(value.version=='140.0.7339.16','chromium_version_mismatch')
        browser_processes()
        return value
    def context(owner,*args,**kwargs):
        check(not contexts and 'extra_http_headers' not in kwargs,'unexpected_context')
        value=original_context(owner,*args,**kwargs)
        contexts.append(value);base['contexts']+=1
        def route_request(route):
            req=route.request
            if req.url==PAGE_URL and req.method=='GET' and req.is_navigation_request():
                base['document_requests']+=1
                check(base['document_requests']==1,'navigation_repeated')
                route.fulfill(status=200,content_type='text/html',body=html)
            elif req.url in (ACCOUNT_URL,MAP_URL) and req.method=='GET':
                kind='account' if req.url==ACCOUNT_URL else 'map'
                base[kind+'_requests']+=1
                headers=req.all_headers()
                parts=dict(item.strip().split('=',1) for item in headers.get('cookie','').split(';') if '=' in item)
                expected=number if kind=='account' else number+1
                matches=parts.get(COOKIE)==generation(expected)
                base[kind+'_cookie_matches']=matches
                base['obe_absent'] &= 'obe' not in headers
                headers=parts=None
                check(matches and base['obe_absent'] and base[kind+'_requests']==1,'dummy_request_identity_failed')
                response={'Content-Type':'application/json','Cache-Control':'no-store',
                    'Access-Control-Allow-Origin':'https://www.coinglass.com','Access-Control-Allow-Credentials':'true'}
                if kind=='account':response['Set-Cookie']=COOKIE+'='+generation(number+1)+'; Domain=.coinglass.com; Path=/; Secure; HttpOnly; SameSite=Lax'
                route.fulfill(status=200,body='{"success":true}',headers=response)
            else:
                base['other_requests_aborted']+=1;route.abort()
        value.route('**/*',route_request)
        return value
    def close(value,*args,**kwargs):
        try:
            if value in contexts:
                cookies=value.cookies(urls=[ACCOUNT_URL])
                base['updated_cookie_before_close']=any(c.get('name')==COOKIE and c.get('value')==generation(number+1) for c in cookies)
                cookies=None
        finally:
            result=original_close(value,*args,**kwargs)
            if value in contexts:base['context_closed']=True
        return result
    def browser_close(value,*args,**kwargs):
        try:browser_processes()
        finally:
            result=original_browser_close(value,*args,**kwargs)
            if value in browsers:base['browser_closed']=not value.is_connected()
        return result
    def screenshot(value,*args,**kwargs):
        if failure:
            check(base['account_cookie_matches'] and base['map_cookie_matches'],'failure_before_session_path')
            base['failure_injected']=True
            raise RuntimeError('dummy_source_failure_after_account_and_map')
        return original_shot(value,*args,**kwargs)
    def browser_complete():
        try:
            end=time.monotonic()+2
            while time.monotonic()<end and any(Path('/proc',str(pid)).exists() for pid in browser_pids):time.sleep(.05)
            base['browser_processes_gone']=bool(browser_pids) and all(not Path('/proc',str(pid)).exists() for pid in browser_pids)
            base['profiles_removed']=bool(profiles) and all(not p.exists() for p in profiles)
            base['browser_pids']=sorted(browser_pids)
            base['private_inputs_removed']=all(k not in os.environ for k in ('DATABASE_URL','COINGLASS_SESSION_KEY','COINGLASS_STORAGE_STATE_JSON','COINGLASS_COOKIE_HEADER','COINGLASS_SESSION_RETURN_FD'))
        except BaseException:os._exit(98)
    atexit.register(browser_complete)
    BrowserType.launch=launch;Browser.new_context=context;BrowserContext.close=close
    Browser.close=browser_close;Page.screenshot=screenshot


def stop(process):
    if process is None:return True
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5)
            return False
    return process.returncode==0


def copy_package(source,destination):
    check(not destination.exists(),'package_copy_exists')
    shutil.copytree(source,destination,ignore=shutil.ignore_patterns('__pycache__','attempt-*.json','source_evidence-*'))
    destination.chmod(0o700)
    m=json.loads((destination/'manifest.json').read_text())
    for name,digest in m['files'].items():
        check(hashlib.sha256((destination/name).read_bytes()).hexdigest()==digest,'package_integrity_failed')
    return destination


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();started=time.monotonic()
    report={'status':'failed','phase':'preflight','runs':[],'source_requests':0,'ai_calls':0,
        'production_writes':0,'actual_entry_command':'./.venv/bin/python -B <package>/approved_entry.py --bootstrap|--reuse',
        'database_and_browser_real':True,'concurrency_suite_repeated':False}
    worker=entry=admin=db=None;hook=None;clean=True
    work=Path(os.environ['CG_TEST_WORK']).resolve();private=work/'private'
    private.mkdir(mode=0o700,exist_ok=True)
    try:
        check(os.geteuid()!=0 and sys.version_info[:3]==(3,11,11),'runtime_mismatch')
        check(Path(sys.prefix).resolve()==(ROOT/'.venv').resolve(),'expected_worker_virtualenv_missing')
        check(importlib.metadata.version('playwright')=='1.55.0','playwright_version_mismatch')
        # The tested dependency must be absent from the venv; loading it from
        # an already-installed system package would not prove the final bundle.
        try:importlib.metadata.version('pycryptodome')
        except importlib.metadata.PackageNotFoundError:pass
        else:raise CheckFailed('ambient_crypto_would_mask_bundle')
        package=copy_package(HERE/'wrapper',private/'wrapper-success')
        sys.path.insert(0,str(package))
        import observation_supervisor as supervisor
        supervisor.activate_bundled_crypto()
        from session_state import PgSessionStore,WORK_LOCK
        import psycopg
        from psycopg.rows import dict_row
        from check_postgres import prepare_database,writer_dsn
        admin=prepare_database('cg-wrapper-setup')
        admin.execute('CREATE TABLE public.ai_collection_bridge_jobs (status text NOT NULL)')
        admin.execute('GRANT SELECT ON public.ai_collection_bridge_jobs TO cg_session_writer')
        db=psycopg.connect(writer_dsn(),autocommit=True,row_factory=dict_row,
            options='-c statement_timeout=3000 -c lock_timeout=1000')
        report['postgresql_version']=admin.execute('SHOW server_version').fetchone()['server_version']
        # This .pth exists only inside the disposable venv and installs test
        # HTTP response fixtures even after the actual entry clears its env.
        hook=Path(sysconfig.get_paths()['purelib'])/'cg_wrapper_fixture.pth'
        check(hook.resolve().is_relative_to(ROOT/'.venv') and not hook.exists(),'test_hook_scope_invalid')
        hook.write_text(str(HERE)+'\nimport wrapper_bootstrap\n');hook.chmod(0o600)
        with socket.socket() as port_socket:
            port_socket.bind(('127.0.0.1',0));port=port_socket.getsockname()[1]
        env={k:os.environ[k] for k in ('PATH','HOME','LANG','LC_CTYPE','PLAYWRIGHT_BROWSERS_PATH') if k in os.environ}
        env.update(RENDER_GIT_COMMIT=COMMIT,PORT=str(port),COLLECTION_BRIDGE_ENABLED='false',
            DATABASE_URL=writer_dsn(),COINGLASS_SESSION_KEY=base64.b64encode(KEY).decode(),
            COINGLASS_STORAGE_STATE_JSON=json.dumps(state(1),separators=(',',':')),
            COINGLASS_COOKIE_HEADER=COOKIE+'=DUMMY_STALE_HEADER',
            COINGLASS_ACCOUNT_DIAGNOSTIC_ON_START='false',PYTHONDONTWRITEBYTECODE='1')
        worker=subprocess.Popen([sys.executable,'-B','standalone_collector/app.py'],cwd=ROOT,env=env,
            stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
        env.clear()
        deadline=time.monotonic()+10
        while True:
            check(worker.poll() is None,'disabled_worker_exited')
            try:
                with urllib.request.urlopen('http://127.0.0.1:'+str(port)+'/health',timeout=1) as response:
                    if json.loads(response.read(16384)).get('mode')=='not_configured':break
            except (OSError,ValueError):pass
            check(time.monotonic()<deadline,'disabled_worker_not_ready');time.sleep(.05)
        successful_pids=[]
        for number,mode,failure in ((1,'bootstrap',False),(2,'reuse',False),(3,'bootstrap',True)):
            report['phase']='failure_cleanup' if failure else mode
            if failure:
                # New owned fixture data and same exact final package, not a
                # retry of a source attempt or mutation of its once markers.
                admin.execute('DELETE FROM collector_private.coinglass_session_state')
                package=copy_package(HERE/'wrapper',private/'wrapper-failure')
                # Failure bootstrap still starts with worker's generation 1.
                expected=1
            else:expected=number
            evidence=private/('cycle-'+str(number));evidence.mkdir(mode=0o700)
            write_json(CONFIG,{'package':str(package),'evidence':str(evidence),'generation':expected,'failure':failure})
            output=evidence/'entry-output.txt'
            with output.open('wb') as stream:
                entry=subprocess.Popen([sys.executable,'-B',str(package/'approved_entry.py'),'--'+mode],
                    cwd=ROOT,env={k:os.environ[k] for k in ('PATH','HOME','LANG','LC_CTYPE','PLAYWRIGHT_BROWSERS_PATH') if k in os.environ},
                    stdout=stream,stderr=subprocess.DEVNULL,start_new_session=True)
            cycle={'mode':mode,'failure_fixture':failure,'entry_pid':entry.pid,'entry_returned':False}
            report['runs'].append(cycle)
            deadline=time.monotonic()+63;lock_seen=False;owner_backend=None
            while entry.poll() is None:
                check(time.monotonic()<deadline,'entry_command_timeout')
                row=db.execute('SELECT revision,status,run_id FROM collector_private.coinglass_session_state').fetchone()
                if row and row['status']=='in_use':
                    acquired=db.execute('SELECT pg_try_advisory_lock(%s) AS held',(WORK_LOCK,)).fetchone()['held']
                    if acquired:db.execute('SELECT pg_advisory_unlock(%s)',(WORK_LOCK,))
                    else:
                        lock_seen=True
                        owner_backend=db.execute("SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted AND classid=0 AND objid=%s AND objsubid=1",(WORK_LOCK,)).fetchone()['pid']
                time.sleep(.05)
            cycle['entry_returned']=entry.returncode==0
            raw=output.read_text();lines=[line.split(' ',1)[1] for line in raw.splitlines() if line.startswith('COINGLASS_REUSE_12H_RESULT ')]
            check(len(lines)==1,'entry_receipt_missing')
            receipt=json.loads(lines[0]);raw=lines=None
            cycle['receipt']=receipt;cycle['lock_observed']=lock_seen;cycle['lock_owner_backend']=owner_backend
            check(entry.returncode==0,'entry_command_failed');entry=None
            check(lock_seen,'real_lock_not_observed')
            check(all(receipt.get(k) is True for k in ('own_descendants_gone','lock_release_confirmed','within_60_seconds','temporary_files_removed','no_future_source_launch_confirmed')),'wrapper_cleanup_failed')
            check(receipt.get('keeper_retained_for_safety') is False,'keeper_retained')
            check(db.execute('SELECT pg_try_advisory_lock(%s) AS held',(WORK_LOCK,)).fetchone()['held'] is True,'lock_leaked')
            try:
                current=PgSessionStore(db,KEY).read()
                check(any(c['name']==COOKIE and c['value']==generation(expected+1) for c in current['cookies']),'updated_state_not_saved')
                current.clear()
                row=db.execute('SELECT revision,status,encrypted_payload FROM collector_private.coinglass_session_state').fetchone()
                check(row['status']=='ready' and generation(expected+1) not in row['encrypted_payload'],'state_not_encrypted')
                cycle['saved_revision']=row['revision'];cycle['encrypted_payload_verified']=True
            finally:db.execute('SELECT pg_advisory_unlock(%s)',(WORK_LOCK,))
            process_evidence={}
            for role in ('approved_entry.py','observation_supervisor.py','collector_browser_fixture.py'):
                value=json.loads((evidence/(role+'.json')).read_text())
                process_evidence[role]=value
                if role!='collector_browser_fixture.py':
                    check(value['crypto_loaded'] and value['crypto_path_private'] and value['crypto_version']=='3.21.0','private_crypto_not_used')
            browser=process_evidence['collector_browser_fixture.py'];cycle['process_evidence']=process_evidence
            required=('account_cookie_matches','map_cookie_matches','obe_absent','updated_cookie_before_close',
                'context_closed','browser_closed','profiles_removed','browser_processes_gone','parent_secrets_absent',
                'legacy_header_absent','private_inputs_removed','internal_sandbox_disabled_observed')
            check(all(browser.get(k) is True for k in required),'browser_evidence_failed')
            check(all(browser.get(k)==1 for k in ('browser_launches','contexts','document_requests','account_requests','map_requests')),'browser_count_failed')
            check(receipt.get('session_persistence',{}).get('saved') is True,'session_persistence_failed')
            observation=receipt.get('observation_report') or {}
            if failure:
                check(browser['failure_injected'] and receipt['status']=='failed' and observation.get('native_hover_validated') is False,'failure_not_exercised')
            else:
                check(receipt['status']=='completed' and observation.get('access_verified') is True and observation.get('native_hover_validated') is True
                    and observation.get('png_validated') is True,'source_wrapper_dummy_capture_failed')
                successful_pids.append(browser['browser_pids'])
                check(cycle['saved_revision']==(3 if mode=='bootstrap' else 5),'revision_sequence_wrong')
            output.unlink()
        check(set(successful_pids[0]).isdisjoint(successful_pids[1]),'browser_not_restarted')
        report.update(status='passed',phase='complete',fresh_browser_reuse=True,
            old_bootstrap_ignored=True,failure_cleanup_verified=True,ownership_preserved=True)
    except CheckFailed as error:report['reason']=str(error)
    except BaseException as error:
        report['reason']='test_execution_failed'
        report['exception_kind']=type(error).__name__ if type(error).__name__ in ('OSError','OperationalError','ProgrammingError','IntegrityError','KeyError','ValueError','ImportError','ModuleNotFoundError','AssertionError','FileNotFoundError','PermissionError') else 'other'
    finally:
        clean &= stop(entry);clean &= stop(worker)
        locks_remaining = None
        if admin is not None:
            try:
                locks_remaining = admin.execute("SELECT count(*) AS n FROM pg_locks WHERE locktype='advisory' AND granted AND classid=0 AND objid=748230102 AND objsubid=1").fetchone()['n']
                clean &= locks_remaining == 0
            except BaseException:clean=False
        remaining = []
        for pid,(_,argv) in process_snapshot().items():
            if pid == os.getpid() or not argv:continue
            if Path(os.fsdecode(argv[0])).name in ('chrome','chromium','headless_shell','chrome-headless-shell') or any(
                    os.fsdecode(arg).startswith(str(private)+'/') for arg in argv if arg):remaining.append(pid)
        clean &= not remaining
        report['owned_processes_remaining'] = len(remaining)
        report['advisory_locks_remaining'] = locks_remaining
        if db is not None:
            try:db.close()
            except Exception:clean=False
        if admin is not None:
            try:admin.close()
            except Exception:clean=False
        if hook is not None:hook.unlink(missing_ok=True)
        CONFIG.unlink(missing_ok=True)
        # Supervisor's own PID census and external namespace cleanup remain in
        # force. This check removes only its private dummy data/evidence package.
        try:shutil.rmtree(private)
        except FileNotFoundError:pass
        except OSError:clean=False
        report['cleanup']={'entry_and_worker_stopped':clean,'private_test_files_removed':not private.exists(),
            'test_hook_removed':hook is None or not hook.exists()}
        report['elapsed_seconds']=round(time.monotonic()-started,3)
        if not all(report['cleanup'].values()):report['status']='failed'
        write_json(args.output,report)
        print('SESSION_WRAPPER_RESULT '+json.dumps(report,sort_keys=True,allow_nan=False),flush=True)
    return 0 if report['status']=='passed' else 1


if __name__=='__main__':raise SystemExit(main())
