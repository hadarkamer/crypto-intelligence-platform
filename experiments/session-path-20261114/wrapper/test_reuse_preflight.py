"""Exact-entry orchestration tests, without browser, network or PostgreSQL.

The real entry, worker procfs checks, supervisor, keeper subprocess, candidate
ManagedSession/SessionTransport and CaptureSession pipe execute. Health and
package metadata are substituted; the database/store and browser are explicit
test boundaries. These tests do not repeat or claim real SQL/browser proof.
"""
import ast
import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import struct
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

HERE = Path(__file__).resolve().parent
BASELINE = HERE.parent / 'coinglass-account-baseline/standalone_collector'
CANDIDATE = HERE.parent / 'coinglass-session-state/standalone_collector'
COMMIT = '33bad18f8be322009108584a6b71f3d65fa9b351'
KEY = base64.b64encode(bytes(range(32))).decode()
DEPENDENCIES = HERE.parent/'session-test-deps'
DUMMY = {'cookies': [{'name': 'fixture_session', 'value': 'DUMMY_INITIAL_NOT_SECRET',
    'domain': '.coinglass.com', 'path': '/', 'expires': -1, 'httpOnly': True,
    'secure': True, 'sameSite': 'Lax'}], 'origins': []}
FILES = ('approved_entry.py', 'collector_browser_fixture.py', 'observation_supervisor.py',
         'session_restore_state.py', 'session_state.py', 'session_runtime.py', 'session_capture.py')


def pins(path, name):
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in node.targets):
            return ast.literal_eval(node.value)
    raise AssertionError(name)


# This source boundary uses the candidate parser/observer/snapshot/pipe against
# a fake page. It neither opens Chromium nor calls an account endpoint.
CHILD = r'''
import copy, json, os, sys
from pathlib import Path
package = Path(sys.argv[1]); scenario = sys.argv[2]
sys.path[:0] = [str(package), os.environ['COLLECTOR_SOURCE_RUNTIME']]
def no_network(event, args):
    if event in ('socket.connect', 'socket.getaddrinfo', 'socket.bind'):
        raise RuntimeError('network_forbidden')
sys.addaudithook(no_network)
assert 'DATABASE_URL' not in os.environ
assert 'COINGLASS_SESSION_KEY' not in os.environ
assert 'COINGLASS_SESSION_STORE_ENABLED' not in os.environ
assert os.environ['COINGLASS_COOKIE_HEADER'] == ''
from session_capture import CaptureSession
from types import SimpleNamespace
session = CaptureSession.from_environment()
assert session is not None
assert all(name not in os.environ for name in ('COINGLASS_STORAGE_STATE_JSON', 'COINGLASS_COOKIE_HEADER',
    'COINGLASS_SESSION_RETURN_FD', 'COINGLASS_SESSION_RUN_ID', 'COINGLASS_SESSION_REVISION'))
assert not os.get_inheritable(session.descriptor)
if scenario == 'cancel_capture':
    import time
    time.sleep(4)
state = copy.deepcopy(session.state)
expected = 'DUMMY_REUSED_NOT_SECRET' if scenario == 'reuse' else 'DUMMY_INITIAL_NOT_SECRET'
assert state['cookies'][0]['value'] == expected
state['cookies'][0]['value'] = 'DUMMY_UPDATED_NOT_SECRET'
class Context:
    def new_cdp_session(self, page): return self
    def send(self, method, params):
        assert method == 'Network.getCookies' and params['urls']
        return {'cookies': copy.deepcopy(state['cookies'])}
    def detach(self): pass
context = Context()
class Page:
    def __init__(self):
        self.main_frame = SimpleNamespace(url='https://www.coinglass.com/pro/futures/LiquidationHeatMap')
        self.listeners = {}
    def on(self, event, callback): self.listeners.setdefault(event, []).append(callback)
    def remove_listener(self, event, callback): self.listeners[event].remove(callback)
page = Page()
class Request:
    url = 'https://capi.coinglass.com/coin-community/api/userapi/info'
    frame = page.main_frame
    method = 'GET'
    resource_type = 'fetch'
    redirected_from = redirected_to = None
    def response(self):
        return SimpleNamespace(status=200, body=lambda: b'{"success":false}' if scenario == 'blocked' else b'{"success":true}')
session.attach(context, page)
request = Request()
for callback in list(page.listeners['request']): callback(request)
for callback in list(page.listeners['requestfinished']): callback(request)
if scenario == 'missing_frame':
    os.close(session.descriptor)
else:
    session.finish(context)
    assert not any(page.listeners.values())
    assert session.descriptor is None and session.state == {}
from collector_browser_fixture import empty_live_report, sanitize_live_report
report = empty_live_report()
report.update(status='stopped', reason='native_capture_rejected', native_code='source_capture_failed',
    native_reason='hover_candidates_unresolved', phase='hover_collection', private_output_deleted=True,
    artifact_status='unavailable', artifact_files=['receipt.json'])
report['session_restore'].update(exclusive_input_validated=True, legacy_header_absent=True,
    context_state_applied=True, cookies_snapshot_observed=True, cookies_changed=True, private_state_released=True)
output = Path(os.environ['COLLECTOR_SOURCE_OUTPUT'])
output.mkdir(mode=0o700, parents=True, exist_ok=False)
(output/'receipt.json').write_text(json.dumps(report, separators=(',', ':')))
assert sanitize_live_report(report) is not None
print(json.dumps(report, separators=(',', ':')), flush=True)
'''


# Loaded only in the keeper test process. Existing candidate lifecycle and pipe
# remain real; this replaces the service and persistence boundaries explicitly.
KEEPER_SETUP = r'''
import asyncio, copy, json, os, sys, types
from pathlib import Path
package = Path(sys.argv[1]); scenario = sys.argv[2]
sys.path.insert(0, str(package))
import observation_supervisor as supervisor
from session_state import Lease, SessionStateError, validate_state
import session_state, session_runtime
STATE = json.loads(sys.argv[3])
CHILD = sys.argv[4]
class Cursor:
    def __init__(self, row): self.row = row
    def fetchone(self): return self.row
class Connection:
    def __init__(self): self.locked = False; self.closed = False; self.store = None
    def execute(self, sql, parameters=()):
        assert not self.closed
        if 'pg_try_advisory_lock' in sql:
            assert parameters == (748230102,)
            self.locked = True
            return Cursor({'held':True})
        if 'ai_collection_bridge_jobs' in sql: return Cursor(None)
        if 'pg_backend_pid()' in sql and 'EXISTS' in sql: return Cursor({'owner':1729, 'held':self.locked})
        if 'pg_backend_pid()' in sql: return Cursor({'owner':1729})
        if 'SELECT revision,status FROM collector_private.coinglass_session_state' in sql:
            assert self.store is not None and self.locked
            return Cursor({'revision':self.store.revision,'status':self.store.status})
        raise AssertionError('unexpected_sql_boundary')
    def close(self): self.closed = True; self.locked = False
connection = Connection()
def connect(dsn, **kwargs):
    assert dsn == 'postgresql://dummy.invalid/not-used'
    assert kwargs.get('autocommit') is True
    return connection
psycopg = types.ModuleType('psycopg'); psycopg.connect = connect
rows = types.ModuleType('psycopg.rows'); rows.dict_row = object()
psycopg.rows = rows
sys.modules['psycopg'] = psycopg; sys.modules['psycopg.rows'] = rows
class StoreBoundary:
    def __init__(self, held, key):
        assert held is connection and held.locked and not held.closed
        assert key == bytes(range(32))
        held.store = self
        self.status = 'missing' if os.environ['COINGLASS_REUSE_MODE'] == 'bootstrap' else 'ready'
        self.revision = 1 if self.status == 'missing' else 3
        self.state = copy.deepcopy(STATE)
        if self.status == 'ready': self.state['cookies'][0]['value'] = 'DUMMY_REUSED_NOT_SECRET'
    def _require_lock(self):
        assert connection.locked and not connection.closed
    def bootstrap(self, state):
        assert connection.locked and self.status == 'missing'
        assert validate_state(state) == STATE
        self.status = 'ready'
        return 1
    def begin(self, run_id):
        assert connection.locked and self.status == 'ready'
        if scenario == 'cancel_begin':
            import time
            print('{"test_boundary":"begin_entered"}',flush=True)
            time.sleep(0.5)
        self.status = 'in_use'
        self.revision += 1
        return Lease(self.revision, run_id, copy.deepcopy(self.state))
    def complete(self, lease, outcome, state=None):
        assert connection.locked and not connection.closed
        assert self.status == 'in_use' and lease.revision == self.revision
        if scenario == 'save_failure': raise SessionStateError('state_store_failed')
        expected = ('blocked' if scenario == 'blocked' else 'unconfirmed'
                    if scenario in ('missing_frame','cancel_begin','cancel_capture') else 'ready')
        assert outcome == expected
        if outcome == 'ready':
            assert validate_state(state)['cookies'][0]['value'] == 'DUMMY_UPDATED_NOT_SECRET'
        else: assert state is None
        self.status = outcome
        self.revision += 1
        return self.revision
session_state.PgSessionStore = session_runtime.PgSessionStore = StoreBoundary
real_spawn = supervisor.spawn
def spawn(command, env=None, *, pass_fds=()):
    assert command == [sys.executable, '-B', str(package/'collector_browser_fixture.py'), '--live-worker']
    assert len(pass_fds) == 1
    return real_spawn([sys.executable, '-B', '-c', CHILD, str(package), scenario], env, pass_fds=pass_fds)
supervisor.spawn = spawn
def no_network(event, args):
    if event in ('socket.connect', 'socket.getaddrinfo', 'socket.bind'):
        raise RuntimeError('network_forbidden')
sys.addaudithook(no_network)
raise SystemExit(supervisor.lock_holder())
'''


SITE_CUSTOMIZE = r'''
import atexit, importlib.metadata, io, json, os, signal, sys, urllib.request
from pathlib import Path
package = Path(os.environ['TEST_ENTRY_PACKAGE'])
scenario = os.environ['TEST_SCENARIO']
payload = json.loads(Path(os.environ['TEST_BOUNDARIES']).read_text())
sys.path.insert(0, str(package))
import observation_supervisor as supervisor
facts = {'keeper_launches':0, 'direct_source_launches':0, 'worker_header_preserved':False,
    'keeper_environment_filtered':False, 'calls':[], 'kernel_child_census_mocked':True,
    'network_calls':0, 'real_database_connections':0, 'browser_launches':0,
    'store_boundary_substituted':True, 'test_sigterm_sent':False}
def profile(frame, event, unused):
    if event == 'call' and frame.f_code.co_filename == supervisor.__file__:
        name = frame.f_code.co_name
        if name in {'run_source_probe','execute_once','verify_runtime','confirm_worker_environment'} and name not in facts['calls']:
            facts['calls'].append(name)
sys.setprofile(profile)
def no_network(event, args):
    if event in ('socket.connect','socket.getaddrinfo','socket.bind'):
        facts['network_calls'] += 1
        raise RuntimeError('network_forbidden')
sys.addaudithook(no_network)
real_version = importlib.metadata.version
def version(name):
    if name == 'playwright':return '1.55.0'
    if name == 'pycryptodome' and scenario == 'dependency_missing':
        raise importlib.metadata.PackageNotFoundError(name)
    return real_version(name)
importlib.metadata.version = version
class Health(io.BytesIO):
    def __enter__(self): return self
    def __exit__(self, *unused): self.close()
def health(url, *, timeout):
    assert url == 'http://127.0.0.1:10000/health' and timeout == 3
    return Health(b'{"mode":"not_configured"}')
urllib.request.urlopen = health
real_spawn = supervisor.spawn
def spawn(command, env=None, *, pass_fds=()):
    if command[-1] == '--lock-holder':
        facts['keeper_launches'] += 1
        facts['worker_header_preserved'] = os.environ.get('COINGLASS_COOKIE_HEADER') == 'obe=DUMMY_OLD_NOT_SECRET'
        facts['keeper_environment_filtered'] = all(k not in env for k in ('UNRELATED_SECRET','OPENAI_API_KEY','PYTHONPATH','TEST_BOUNDARIES','TEST_ENTRY_PACKAGE'))
        assert facts['worker_header_preserved'] and facts['keeper_environment_filtered']
        return real_spawn([sys.executable, '-B', '-c', payload['keeper'], str(package), scenario,
            json.dumps(payload['state'],separators=(',',':')), payload['child']], env, pass_fds=pass_fds)
    facts['direct_source_launches'] += 1
    raise AssertionError('source_not_keeper_owned')
supervisor.spawn = spawn
supervisor.kernel_children = lambda: {p.pid for p in supervisor.OWN_CHILDREN if p.poll() is None}
real_messages = supervisor.OutputReader.messages
def messages(reader, label):
    values = real_messages(reader,label)
    should_cancel = (scenario == 'cancel_begin' and any(row.get('test_boundary') == 'begin_entered' for row in values)
        or scenario == 'cancel_capture' and any(row.get('observer') == 'started' for row in values))
    if label == 'holder' and should_cancel and not facts['test_sigterm_sent']:
        facts['test_sigterm_sent'] = True
        os.kill(os.getpid(),signal.SIGTERM)
    return values
supervisor.OutputReader.messages = messages
@atexit.register
def report():
    sys.setprofile(None)
    print('REUSE_TEST_FACTS '+json.dumps(facts,separators=(',',':')),flush=True)
'''


class ConverterCompatibilityTests(unittest.TestCase):
    """Actual converter script + validators; DOM/clipboard are simulated here.

    The existing real-browser integration separately exercises its click handler
    and carries that output through the exact entry, storage and new browser.
    """
    def test_actual_converter_output_enters_both_real_validators_unchanged(self):
        import importlib.util
        path=HERE.parent/'check_wrapper.py'
        spec=importlib.util.spec_from_file_location('cookie_chain_fixture',path)
        fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)
        expected=fixture.state(1)
        raw={'cookies':[{**cookie,'session':True,'priority':'Medium','size':42,
            'sourceScheme':'Secure','sourcePort':443} for cookie in expected['cookies']]}
        script=r"""
const fs=require('node:fs'),vm=require('node:vm');
const elements=new Map();
const document={getElementById(id){if(!elements.has(id))elements.set(id,{value:'',style:{},
  addEventListener(event,fn){this[event]=fn},focus(){},select(){}});return elements.get(id)}};
const html=fs.readFileSync(process.argv[1],'utf8');
const source=html.match(/<script>([\s\S]*?)<\/script>/)[1];
const navigator={clipboard:{async writeText(){throw new Error('dummy clipboard unavailable')}}};
vm.runInNewContext(source,{document,navigator});
document.getElementById('input').value=fs.readFileSync(0,'utf8');
(async()=>{await document.getElementById('convert').click();
 const value=document.getElementById('output').value;
 if(!value||document.getElementById('input').value)throw new Error('converter_failed');
 process.stdout.write(value);document.getElementById('clear').click();
 if(document.getElementById('output').value)throw new Error('clear_failed');
})().catch(()=>{process.stderr.write('converter_failed');process.exitCode=1});
"""
        result=subprocess.run(['node','-e',script,str(HERE.parent/'coinglass_cookie_converter_offline.html')],
            input=json.dumps(raw),capture_output=True,text=True,timeout=5)
        self.assertEqual(result.returncode,0);self.assertEqual(result.stderr,'')
        actual=json.loads(result.stdout);self.assertEqual(actual,expected)
        sys.path.insert(0,str(HERE))
        from session_restore_state import Rejected,validate_storage_state
        from session_state import validate_state,SessionStateError
        self.assertEqual(json.loads(validate_storage_state(result.stdout)),expected)
        self.assertEqual(validate_state(actual),expected)
        self.assertNotIn('sameSite',actual['cookies'][0])
        bad=copy.deepcopy(actual);bad['cookies'][0]['sameSite']=None
        with self.assertRaises(Rejected):validate_storage_state(json.dumps(bad))
        with self.assertRaises(SessionStateError):validate_state(bad)


class ReuseEntryTests(unittest.TestCase):
    def run_entry(self, *, mode='bootstrap', scenario='ready', commit=COMMIT, state=DUMMY, key=KEY,
                  corrupt_runtime=False, repeat=False, prior_access=True):
        with tempfile.TemporaryDirectory(prefix='cg-reuse-entry-') as directory:
            root=Path(directory); service=root/'service'; package=root/'package'; hooks=root/'hooks'
            for path in (service,package,hooks): path.mkdir(mode=0o700)
            for name in FILES: shutil.copyfile(HERE/name, package/name)
            if mode == 'reuse' and prior_access is not None:
                prior={'observation_report':{'access_verified':prior_access},
                    'own_descendants_gone':True,'lock_release_confirmed':True,'within_60_seconds':True,
                    'temporary_files_removed':True,'filtered_evidence_retained':True,
                    'session_persistence':{'saved':True,'outcome':'ready','revision':3}}
                (package/'attempt-bootstrap.json').write_text(json.dumps(prior))
                (package/'attempt-bootstrap.json').chmod(0o600)
            runtime=service/'standalone_collector/runtime'; runtime.mkdir(parents=True)
            required=pins(package/'collector_browser_fixture.py','HASHES') | pins(package/'observation_supervisor.py','RUNTIME_HASHES')
            for name in required:
                target=runtime/name; target.parent.mkdir(parents=True,exist_ok=True)
                shutil.copyfile(BASELINE/'runtime'/name,target)
            if corrupt_runtime: (runtime/'model1_execution.py').write_text('# changed test runtime\n')
            (service/'.venv').symlink_to(Path(sys.prefix).resolve(),target_is_directory=True)
            (runtime.parent/'app.py').write_text("import time\nprint('READY',flush=True)\ntime.sleep(20)\n")
            (hooks/'sitecustomize.py').write_text(SITE_CUSTOMIZE)
            boundaries=root/'boundaries.json'
            boundaries.write_text(json.dumps({'keeper':KEEPER_SETUP,'child':CHILD,'state':DUMMY}))
            boundaries.chmod(0o600)
            worker_env={'PATH':os.environ['PATH'],'HOME':str(root),'LANG':'C.UTF-8','LC_CTYPE':'C.UTF-8',
                'PLAYWRIGHT_BROWSERS_PATH':str(root/'unused-browser-path'),'COINGLASS_COOKIE_HEADER':'obe=DUMMY_OLD_NOT_SECRET',
                'DATABASE_URL':'postgresql://dummy.invalid/not-used','COLLECTION_BRIDGE_ENABLED':'false',
                'PORT':'10000','COINGLASS_ACCOUNT_DIAGNOSTIC_ON_START':'false',
                'UNRELATED_SECRET':'DUMMY_DO_NOT_PROPAGATE','OPENAI_API_KEY':'DUMMY_DO_NOT_PROPAGATE'}
            if state is not None:worker_env['COINGLASS_STORAGE_STATE_JSON']=json.dumps(state,separators=(',',':'))
            if commit is not None:worker_env['RENDER_GIT_COMMIT']=commit
            if key is not None:worker_env['COINGLASS_SESSION_KEY']=key
            worker=subprocess.Popen([sys.executable,'-B','standalone_collector/app.py'],cwd=service,env=worker_env,
                stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            try:
                self.assertEqual(worker.stdout.readline().strip(),'READY')
                env={k:worker_env[k] for k in ('PATH','HOME','LANG','LC_CTYPE')}
                env.update(PYTHONPATH=os.pathsep.join((str(hooks),str(DEPENDENCIES))),TEST_ENTRY_PACKAGE=str(package),TEST_SCENARIO=scenario,
                    TEST_BOUNDARIES=str(boundaries),PYTHONDONTWRITEBYTECODE='1')
                command=[sys.executable,'-B',str(package/'approved_entry.py'),'--'+mode]
                if repeat:(package/('attempt-'+mode+'.json')).write_text('{"previous_attempt":true}')
                result=subprocess.run(command,cwd=service,env=env,capture_output=True,text=True,timeout=12)
                self.assertEqual(result.returncode,0);self.assertEqual(result.stderr,'')
                for secret in ('DUMMY_INITIAL_NOT_SECRET','DUMMY_REUSED_NOT_SECRET','DUMMY_UPDATED_NOT_SECRET',
                               'DUMMY_DO_NOT_PROPAGATE','DUMMY_OLD_NOT_SECRET',KEY):
                    self.assertNotIn(secret,result.stdout)
                lines=result.stdout.splitlines()
                receipt=[json.loads(s.removeprefix('COINGLASS_REUSE_12H_RESULT ')) for s in lines if s.startswith('COINGLASS_REUSE_12H_RESULT ')]
                facts=[json.loads(s.removeprefix('REUSE_TEST_FACTS ')) for s in lines if s.startswith('REUSE_TEST_FACTS ')]
                self.assertEqual(len(receipt),1);self.assertEqual(len(facts),1)
                self.assertEqual(facts[0]['network_calls'],0);self.assertEqual(facts[0]['direct_source_launches'],0)
                self.assertEqual(facts[0]['browser_launches'],0);self.assertEqual(facts[0]['real_database_connections'],0)
                attempt=package/('attempt-'+mode+'.json')
                if not repeat and attempt.exists():self.assertEqual(json.loads(attempt.read_text()),receipt[0])
                if repeat:self.assertEqual(json.loads(attempt.read_text()),{'previous_attempt':True})
                return receipt[0],facts[0]
            finally:
                worker.terminate()
                try:worker.wait(timeout=2)
                except subprocess.TimeoutExpired:worker.kill();worker.wait(timeout=2)
                worker.stdout.close();worker.stderr.close()

    def test_bad_commit_key_and_missing_bootstrap_state_stop_before_keeper(self):
        for kwargs in ({'commit':None},{'commit':'0'*40},{'key':None},{'key':'invalid'}, {'state':None}):
            with self.subTest(fields=list(kwargs)):
                receipt,facts=self.run_entry(**kwargs)
                self.assertEqual(facts['keeper_launches'],0)
                self.assertEqual(receipt['source_observer_launches'],0)
                self.assertEqual(receipt['status'],'blocked')

    def test_runtime_corruption_and_once_marker_fail_closed(self):
        for kwargs in ({'corrupt_runtime':True},{'repeat':True}):
            receipt,facts=self.run_entry(**kwargs)
            self.assertEqual(facts['keeper_launches'],0)
            self.assertEqual(receipt['source_observer_launches'],0)

    def test_missing_crypto_dependency_stops_before_reserving_attempt(self):
        receipt,facts=self.run_entry(scenario='dependency_missing')
        self.assertEqual(receipt['reason'],'session_crypto_unavailable')
        self.assertEqual(facts['keeper_launches'],0)

    def assert_completed_wiring(self, receipt, facts):
        self.assertEqual(facts['keeper_launches'],1)
        self.assertTrue(facts['worker_header_preserved'] and facts['keeper_environment_filtered'])
        self.assertEqual(set(facts['calls']),{'run_source_probe','execute_once','verify_runtime','confirm_worker_environment'})
        self.assertEqual(receipt['source_observer_launches'],1)
        self.assertTrue(receipt['own_descendants_gone'])
        self.assertTrue(receipt['lock_release_confirmed'])
        self.assertTrue(receipt['temporary_files_removed'])
        self.assertTrue(receipt['within_60_seconds'])
        self.assertFalse(receipt['keeper_retained_for_safety'])
        self.assertFalse(receipt['operator_intervention_required'])
        self.assertEqual(receipt['ai_calls'],0);self.assertEqual(receipt['job_writes'],0)
        # The dummy source deliberately reports a Hover failure. Saving valid
        # auth state must not be misreported as a successful map capture.
        self.assertEqual(receipt['status'],'failed')

    def test_bootstrap_exact_command_keeps_lock_until_pipe_state_is_saved(self):
        receipt,facts=self.run_entry()
        self.assert_completed_wiring(receipt,facts)
        self.assertEqual(receipt['reason'],'observer_completed')
        self.assertEqual(receipt['session_mode'],'bootstrap')
        self.assertEqual(receipt['session_persistence'],
            {'saved':True,'outcome':'ready','revision':3,'reason':None})
        self.assertTrue(receipt['filtered_evidence_retained'])

    def test_reuse_exact_command_takes_store_state_instead_of_stale_environment(self):
        receipt,facts=self.run_entry(mode='reuse',scenario='reuse')
        self.assert_completed_wiring(receipt,facts)
        self.assertEqual(receipt['session_mode'],'reuse')
        self.assertTrue(receipt['session_persistence']['saved'])

    def test_reuse_does_not_require_environment_snapshot(self):
        receipt,facts=self.run_entry(mode='reuse',scenario='reuse',state=None)
        self.assert_completed_wiring(receipt,facts)
        self.assertTrue(receipt['session_persistence']['saved'])

    def test_reuse_requires_prior_verified_access_before_keeper(self):
        for prior_access in (None,False):
            receipt,facts=self.run_entry(mode='reuse',scenario='reuse',prior_access=prior_access)
            self.assertEqual(facts['keeper_launches'],0)
            self.assertEqual(receipt['reason'],'reuse_requires_verified_bootstrap')

    def test_missing_return_and_save_failure_clean_without_success(self):
        for scenario,reason in (('missing_frame','session_transport_failed'),('save_failure','state_store_failed'),
                                ('blocked','session_access_requires_review')):
            with self.subTest(scenario=scenario):
                receipt,facts=self.run_entry(scenario=scenario)
                self.assert_completed_wiring(receipt,facts)
                self.assertFalse(receipt['session_persistence']['saved'])
                self.assertEqual(receipt['session_persistence']['reason'],reason)

    def test_actual_sigterm_during_begin_prevents_late_launch_and_releases_after_cleanup(self):
        receipt,facts=self.run_entry(scenario='cancel_begin')
        self.assertTrue(facts['test_sigterm_sent'])
        self.assertEqual(facts['keeper_launches'],1)
        self.assertEqual(receipt['source_observer_launches'],0)
        self.assertTrue(receipt['own_descendants_gone'])
        self.assertTrue(receipt['lock_release_confirmed'])
        self.assertTrue(receipt['temporary_files_removed'])
        self.assertTrue(receipt['within_60_seconds'])
        self.assertFalse(receipt['keeper_retained_for_safety'])
        self.assertFalse(receipt['operator_intervention_required'])
        self.assertFalse(receipt['session_persistence']['saved'])

    def test_actual_sigterm_during_source_cleans_child_before_releasing_keeper(self):
        receipt,facts=self.run_entry(scenario='cancel_capture')
        self.assert_completed_wiring(receipt,facts)
        self.assertTrue(facts['test_sigterm_sent'])
        self.assertEqual(receipt['reason'],'cancelled')
        self.assertFalse(receipt['session_persistence']['saved'])


class CandidateHookTests(unittest.TestCase):
    def test_candidate_modules_are_byte_identical(self):
        for name in ('session_state.py','session_runtime.py','session_capture.py'):
            self.assertEqual(hashlib.sha256((HERE/name).read_bytes()).digest(),
                             hashlib.sha256((CANDIDATE/name).read_bytes()).digest())

    def test_lifecycle_finishes_candidate_before_context_close_and_releases_state(self):
        sys.path.insert(0,str(BASELINE/'runtime'))
        sys.path.insert(0,str(HERE))
        from session_capture import CaptureSession
        import collector_browser_fixture as fixture
        read_fd,write_fd=os.pipe()
        session=None
        try:
            values={'COINGLASS_STORAGE_STATE_JSON':json.dumps(DUMMY),'COINGLASS_COOKIE_HEADER':'',
                'COINGLASS_SESSION_RETURN_FD':str(write_fd),'COINGLASS_SESSION_RUN_ID':str(uuid4()),
                'COINGLASS_SESSION_REVISION':'2'}
            with patch.dict(os.environ,values):session=CaptureSession.from_environment()
            sequence=[]
            class Context:
                def new_cdp_session(self,page):return self
                def send(self,method,params):
                    assert method=='Network.getCookies'
                    return {'cookies':self.cookies(urls=params['urls'])}
                def detach(self):pass
                def cookies(self, *, urls):
                    sequence.append('snapshot')
                    cookies=copy.deepcopy(DUMMY['cookies'])
                    if len(sequence)>1:cookies[0]['value']='DUMMY_UPDATED_NOT_SECRET'
                    return cookies
            context=Context()
            class Page:
                def __init__(self):
                    self.context=context;self.listeners={}
                    self.main_frame=SimpleNamespace(url='https://www.coinglass.com/pro/futures/LiquidationHeatMap')
                def on(self,event,callback):self.listeners.setdefault(event,[]).append(callback)
                def remove_listener(self,event,callback):self.listeners[event].remove(callback)
            page=Page();report=fixture.empty_live_report()
            lifecycle=fixture.SessionRestoreLifecycle(session.state,report,session)
            lifecycle.establish_context(context);lifecycle.attach(page)
            request=SimpleNamespace(url='https://capi.coinglass.com/coin-community/api/userapi/info',
                method='GET',resource_type='fetch',frame=page.main_frame,redirected_from=None,redirected_to=None,
                response=lambda:SimpleNamespace(status=200,body=lambda:b'{"success":true}'))
            for callback in list(page.listeners['request']):callback(request)
            for callback in list(page.listeners['requestfinished']):callback(request)
            lifecycle.before_close(lambda resource:sequence.append('native_close'))(context)
            lifecycle.finish()
            size=struct.unpack('!I',os.read(read_fd,4))[0]
            receipt=json.loads(os.read(read_fd,size))
            self.assertEqual(os.read(read_fd,1),b'')
            self.assertEqual(receipt['outcome'],'ready')
            self.assertEqual(receipt['state']['cookies'][0]['value'],'DUMMY_UPDATED_NOT_SECRET')
            self.assertEqual(sequence,['snapshot','snapshot','snapshot','native_close'])
            self.assertFalse(any(page.listeners.values()))
            self.assertIsNone(lifecycle.managed);self.assertIsNone(lifecycle.initial_state)
            self.assertIsNone(session.descriptor);self.assertEqual(session.state,{})
            self.assertTrue(report['session_restore']['private_state_released'])
            self.assertIsNone(report['account_auth'])
            self.assertFalse(lifecycle.cleanup_failed)
            self.assertNotIn('DUMMY_',json.dumps(report))
        finally:
            os.close(read_fd)
            if session is None or session.descriptor is not None:
                try:os.close(write_fd)
                except OSError:pass


if __name__ == '__main__':unittest.main(verbosity=2)
