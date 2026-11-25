"""One-shot native pilot for protected BTC/Model 1/Coin/12H, without AI or worksheet writes.

The normal app job proves publication separately. Run preflight in the candidate environment before
any source phase. Run first, then reuse in a separate invocation, then fresh after the immediate-gate
guard. Fresh starts with an empty child-only context. It uses the same account lock, incident budget
and CAS, and can replace the stored state only through a new protected-map proof.
"""
import argparse
import asyncio
import base64
import hashlib
import json
import os
import re
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
from uuid import uuid4

HERE = Path(__file__).resolve().parent
RUNTIME = HERE / 'runtime'
sys.path.insert(0, str(RUNTIME))
MODULES = ('collection_bridge', 'collection_model1_task', 'modern_map_reader', 'modern_map_task',
           'dedicated_map_flow', 'dedicated_login_form', 'dedicated_auth_state', 'session_channel',
           'session_capture', 'session_runtime', 'session_state', 'model1_execution')
COPIED_MODULES = ('dedicated_map_flow','dedicated_login_form','dedicated_auth_state','session_channel',
                  'session_capture','session_runtime','session_state','modern_map_reader','modern_map_task')
PHASES = ('first', 'reuse', 'fresh')
PUBLIC_CODES = {
    'auth_challenge', 'auth_code_required', 'auth_invalid_credentials', 'auth_form_unavailable',
    'auth_configuration_invalid', 'auth_rate_limited', 'auth_retry_exhausted', 'auth_unconfirmed',
    'auth_in_progress','auth_retry_pending','auth_gate_after_verification',
    'source_login_required', 'source_map_unavailable','source_not_readable','source_capture_failed',
    'source_navigation_changed', 'source_rate_limited', 'source_timeout','session_input_invalid','session_snapshot_failed',
    'session_observation_failed','session_transport_failed','state_revision_conflict',
}
BROWSER_ENV = ('PATH','HOME','LANG','TMPDIR','PLAYWRIGHT_BROWSERS_PATH','LD_LIBRARY_PATH','LC_ALL',
               'PYTHONPATH','SSL_CERT_FILE','XDG_RUNTIME_DIR','XDG_CACHE_HOME')


def ready_configuration(env):
    try:
        key_ok = len(base64.b64decode(env.get('COINGLASS_SESSION_KEY',''),validate=True)) == 32
    except (TypeError, ValueError):
        key_ok = False
    return {'modern_map': env.get('COINGLASS_MODERN_UI_ENABLED','').lower() == 'true',
            'managed_store': env.get('COINGLASS_SESSION_STORE_ENABLED','').lower() == 'true',
            'encryption_key': key_ok, 'database_configured': bool(env.get('DATABASE_URL')),
            'account_configured': bool(env.get('COINGLASS_LOGIN_EMAIL','').strip()),
            'form_secret_configured': bool(env.get('COINGLASS_LOGIN_PASSWORD'))}


def browser_probe():
    # Even when an operator invokes this internal subcommand directly, the inert browser
    # must receive only process settings. Its sole document is about:blank.
    environment = {name:os.environ[name] for name in BROWSER_ENV if name in os.environ}
    os.environ.clear()
    os.environ.update(environment)
    from playwright.sync_api import sync_playwright
    with sync_playwright() as pw:
        browser = pw.chromium.launch(chromium_sandbox=True, headless=True, timeout=10000,
                                     args=['--disable-dev-shm-usage'])
        try:
            context = browser.new_context()
            context.route('**/*', lambda route: route.abort())
            return context.new_page().url == 'about:blank'
        finally:
            browser.close()


def runtime_matches():
    try:
        for name in COPIED_MODULES:
            source = (HERE / (name+'.py')).read_bytes()
            generated = (RUNTIME / (name+'.py')).read_bytes()
            if hashlib.sha256(source).digest() != hashlib.sha256(generated).digest():
                return False
        return all((RUNTIME / (name+'.py')).is_file() for name in MODULES)
    except OSError:
        return False


def native_sandbox_present(env):
    try:
        with subprocess.Popen([sys.executable,str(Path(__file__).resolve()),'_browser'],env=env,
                              stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,
                              start_new_session=True) as probe:
            try:
                return probe.wait(timeout=18) == 0
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(probe.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                probe.wait()
    except OSError:
        pass
    return False


def preflight():
    report = ready_configuration(os.environ)
    report['candidate_runtime'] = runtime_matches()
    report['migration_file'] = (HERE/'dedicated_auth_state.sql').is_file()
    report['native_sandbox'] = report['private_schema_ready'] = False
    env = {k:os.environ[k] for k in BROWSER_ENV if k in os.environ}
    report['native_sandbox'] = native_sandbox_present(env)
    if report['database_configured']:
        try:
            import psycopg
            with psycopg.connect(os.environ['DATABASE_URL'], connect_timeout=5,
                                 options='-c statement_timeout=4000 -c lock_timeout=3000') as db:
                # Exercise only read-only metadata before a pilot can acquire ownership.
                checks = db.execute("""SELECT to_regclass('public.ai_collection_bridge_jobs') IS NOT NULL
                        AND EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='collector_private'
                            AND table_name='coinglass_dedicated_sessions' AND column_name='map_retry_after')
                        AND bool_and(has_table_privilege(current_user,tablename,'SELECT')
                                 AND has_table_privilege(current_user,tablename,'INSERT')
                                 AND has_table_privilege(current_user,tablename,'UPDATE'))
                    FROM unnest(ARRAY['collector_private.coinglass_dedicated_sessions',
                                      'collector_private.coinglass_dedicated_auth',
                                      'collector_private.coinglass_dedicated_auth_jobs']) AS t(tablename)""").fetchone()
                report['private_schema_ready'] = bool(checks and checks[0])
        except Exception:
            pass
    return report


def read_child(mode, folder):
    from collection_metrics import task_metrics
    from modern_map_task import capture
    from session_capture import CaptureSession
    session = CaptureSession.from_environment()
    if session is None or not session.dedicated:
        return {'ok':False, 'reason':'session_input_invalid'}
    if mode == 'fresh':
        # Only this child's decoded copy changes. The parent owns the verified encrypted state.
        session.state = {'cookies':[], 'origins':[]}
    with task_metrics(folder):
        result = capture('BTC', 1, '12H', folder/'map.png', allow_login=mode != 'reuse', session=session)
    correct = (result.get('schema') == 'coinglass.right-edge-measure.v1' and result.get('symbol') == 'BTC'
               and result.get('timeframe') == '12H' and result.get('heatmap_model') == 1
               and result.get('mode') == 'Coin' and bool(result.get('groups'))
               and (folder/'map.png').is_file())
    return {'ok':correct, 'groups':len(result['groups']) if correct else 0}


async def execute_pilot(mode, conn):
    from dedicated_auth_state import IMMEDIATE_GATE_SECONDS
    from session_runtime import ManagedSession
    from session_state import WORK_LOCK
    from collection_metrics import clean_numbers

    jid = str(uuid4())
    managed = ManagedSession.from_environment(conn, jid)
    with conn.transaction():
        if not conn.execute('SELECT pg_try_advisory_lock(%s) AS ok',(WORK_LOCK,)).fetchone()['ok']:
            return {'ok':False, 'reason':'worker_busy'}
        # Fail before opening a source if the requested stage does not establish the intended claim.
        state = conn.execute("SELECT status FROM collector_private.coinglass_dedicated_sessions WHERE slot=%s",
                             (managed.store.slot,)).fetchone()
        if mode == 'first' and state and state['status'] == 'ready':
            return {'ok':False,'reason':'saved_session_already_exists'}
        if mode != 'first' and (not state or state['status'] != 'ready'):
            return {'ok':False,'reason':'previous_verified_state_required'}
        if mode == 'fresh':
            recent = conn.execute("""SELECT 1 FROM collector_private.coinglass_dedicated_auth WHERE slot=%s
                AND phase='verified' AND verified_at>clock_timestamp()-%s*interval '1 second' LIMIT 1""",
                (managed.store.slot,IMMEDIATE_GATE_SECONDS)).fetchone()
            if recent:
                return {'ok':False,'reason':'immediate_gate_guard_active'}
    start = time.monotonic()
    transport = process = None
    await managed.begin()
    try:
        with tempfile.TemporaryDirectory(prefix='cg-protected-') as directory:
            folder = Path(directory)
            transport = await managed.open_transport(os.environ)
            env = dict(transport.env)
            env.pop('OPENAI_API_KEY',None)
            process = await asyncio.create_subprocess_exec(
                sys.executable,str(Path(__file__).resolve()),'_read',mode,directory,
                env=env,pass_fds=transport.pass_fds, stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.DEVNULL,start_new_session=True)
            transport.spawned()
            env.clear()
            try:
                await asyncio.wait_for(process.wait(),timeout=300)
            finally:
                if process.returncode is None:
                    try:
                        os.killpg(process.pid,signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    await process.wait()
            result = {'ok':False,'reason':'pilot_read_failed'}
            output = folder/'receipt.json'
            if output.exists() and output.stat().st_size <= 1024:
                data = json.loads(output.read_text())
                if data.get('ok') is True and type(data.get('groups')) is int and data['groups'] > 0:
                    result = {'ok':True,'groups':data['groups']}
                elif data.get('reason') in PUBLIC_CODES:
                    result['reason'] = data['reason']
            metrics = folder/'collection-metrics.json'
            if metrics.exists() and metrics.stat().st_size <= 8192:
                result['resources'] = clean_numbers(json.loads(metrics.read_text()))
    finally:
        await managed.finish(transport)
    if result['ok']:
        with conn.transaction():
            saved = conn.execute("""SELECT status='ready' AND encrypted_payload IS NOT NULL AS ready
                FROM collector_private.coinglass_dedicated_sessions WHERE slot=%s""",
                (managed.store.slot,)).fetchone()
            verified = conn.execute("""SELECT id FROM collector_private.coinglass_dedicated_auth
                WHERE slot=%s AND phase='verified' AND verified_job=%s""",
                (managed.store.slot,jid)).fetchone()
        result['encrypted_state_ready'] = bool(saved and saved['ready'])
        result['form_event_verified'] = bool(verified)
        if not result['encrypted_state_ready'] or result['form_event_verified'] != (mode != 'reuse'):
            result.update(ok=False,reason='pilot_receipt_mismatch')
    result.update(elapsed_ms=round((time.monotonic()-start)*1000),job_id=jid)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode',choices=('preflight',*PHASES,'_browser','_read'))
    parser.add_argument('internal',nargs='*')
    args=parser.parse_args()
    if args.mode == '_browser':
        return 0 if browser_probe() else 1
    if args.mode == '_read':
        if len(args.internal) != 2 or args.internal[0] not in PHASES:
            return 2
        folder=Path(args.internal[1])
        try:
            result=read_child(args.internal[0],folder)
        except Exception as error:
            reason=getattr(error,'code',None)
            result={'ok':False,'reason':reason if reason in PUBLIC_CODES else 'pilot_read_failed'}
        (folder/'receipt.json').write_text(json.dumps(result))
        return 0 if result['ok'] else 1
    checks=preflight()
    if args.mode == 'preflight' or not all(checks.values()):
        result = {'ok':all(checks.values()),'checks':checks,'source_visits':0}
    else:
        try:
            import psycopg
            from psycopg.rows import dict_row
            with psycopg.connect(os.environ['DATABASE_URL'], connect_timeout=5,row_factory=dict_row,
                    options='-c statement_timeout=8000 -c lock_timeout=3000') as db:
                result=asyncio.run(execute_pilot(args.mode,db))
        except Exception as error:
            reason=getattr(error,'code',None)
            result={'ok':False,'reason':reason if reason in PUBLIC_CODES else 'pilot_operation_failed'}
    revision = os.environ.get('RENDER_GIT_COMMIT','')
    result['host_commit'] = revision if re.fullmatch('[a-f0-9]{40}', revision) else None
    result['target_service'] = os.environ.get('RENDER_SERVICE_NAME') == 'decision-hub-model1-collector'
    print(json.dumps({'stage':args.mode, **result},sort_keys=True))
    return 0 if result['ok'] else 1


if __name__ == '__main__':
    os.umask(0o077)
    raise SystemExit(main())
