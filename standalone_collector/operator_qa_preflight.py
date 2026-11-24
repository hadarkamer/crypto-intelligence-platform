"""One neutral runtime check of each design per QA boot. Read-only HTTP status."""
from __future__ import annotations

import ctypes
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import pwd
import signal
import subprocess
import sys
import threading

HERE = Path(__file__).resolve().parent
SELF = str(HERE/'operator_qa_preflight.py')
ENV = {'PATH':'/usr/local/bin:/usr/bin:/bin','LANG':'C.UTF-8','PYTHONDONTWRITEBYTECODE':'1'}
POLICY = Path('/etc/chromium/policies/managed/operator.json')
DUAL_STATES = frozenset(('verified','collector_boundary_failed','neutral_browser_navigation_failed',
    'ordinary_chromium_sandbox_failed','mandatory_browser_url_policy_unconfirmed','restricted_proxy_unconfirmed',
    'chromium_namespace_sandbox_unavailable','chromium_suid_helper_unavailable','fixture_incomplete',
    'fixture_image_or_supervisor_user_invalid','qa_network_guard_missing','check_timed_out','check_failed'))
VIEWER_STATES = frozenset(('ready','service_user_must_be_nonroot','image_binaries_missing','image_assets_missing',
    'browser_policy_missing_or_invalid','host_namespace_denied','host_mount_denied','host_network_namespace_denied',
    'host_sandbox_denied','sandbox_image_setup_failed','sandbox_child_failed','private_rfb_handshake_failed',
    'runtime_probe_timeout','runtime_transport_unavailable','isolated_environment_failed','private_display_failed',
    'window_manager_failed','native_browser_failed','native_browser_close_failed','reader_import_failed',
    'sandboxed_verifier_failed','local_verification_failed','check_timed_out','check_failed'))
BOOL_FIELDS = ('two_uid_confidential_boundary','ordinary_chromium_sandbox',
               'source_requested_local_proxy','forbidden_origin_reached_proxy','proxy_accepted_connection')
NAVIGATION_RESULTS = frozenset(('completed','timeout','other_error','not_attempted','name_not_resolved',
    'proxy_connection_failed','tunnel_connection_failed','connection_refused','blocked_by_policy'))


def prepare_offline_policy():
    # The comparison policy alongside the neutral private-viewer probe receives
    # the identical, stricter QA policy; its normal policy-integrity check stays active.
    candidate = HERE/'operator_chromium_policy.json'
    policy = json.loads(candidate.read_text())
    policy.update(URLBlocklist=['*'],URLAllowlist=['about:blank','https://qa-source.invalid'],
                  ProxyMode='fixed_servers',ProxyServer='http://127.0.0.1:31497',
                  ProxyBypassList='<-loopback>',QuicAllowed=False,DnsOverHttpsMode='off',
                  WebRtcIPHandlingPolicy='disable_non_proxied_udp')
    encoded = json.dumps(policy,sort_keys=True)+'\n'
    POLICY.parent.mkdir(parents=True,exist_ok=True)
    POLICY.write_text(encoded)
    candidate.write_text(encoded)


def checked_result(value,kind):
    if not isinstance(value,dict):
        value = {}
    if kind=='dual_uid':
        result = {'status':value.get('status') if value.get('status') in DUAL_STATES else 'check_failed'}
        for key in BOOL_FIELDS:
            if type(value.get(key)) is bool:
                result[key] = value[key]
        for key in ('allowed_navigation','forbidden_navigation'):
            outcome = value.get(key)
            if isinstance(outcome,str) and outcome in NAVIGATION_RESULTS:
                result[key] = outcome
    else:
        result = {'reason':value.get('reason') if value.get('reason') in VIEWER_STATES else 'check_failed',
                  'isolated_operator_browser':value.get('isolated_operator_browser') is True}
    if value.get('provider_requests')==0:
        result['provider_requests'] = 0
    return result


def run_one(command,kind,timeout,identity=None):
    options = {'user':identity,'group':identity,'extra_groups':()} if identity else {}
    proc = subprocess.Popen([sys.executable,'-B',*command],cwd=HERE,stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,close_fds=True,start_new_session=True,
        env=dict(ENV,HOME='/tmp'),**options)
    try:
        output,_ = proc.communicate(timeout=timeout)
        if len(output)>4096:
            return checked_result({},kind)
        return checked_result(json.loads(output),kind)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid,signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait()
        return checked_result({'status':'check_timed_out','reason':'check_timed_out'},kind)
    except (OSError,ValueError):
        return checked_result({},kind)


def run_checks():
    # Children get only ENV. The privileged fixture process creates two real
    # UIDs; the complete private Viewer runs independently as its collector UID.
    results = {}
    for kind,command,seconds,uid in (
        ('dual_uid',[str(HERE/'test_operator_container_capabilities.py'),'--networked-qa'],45,None),
        ('private_viewer',[str(HERE/'operator_desktop.py'),'--check-runtime'],70,10001)):
        try:
            results[kind] = run_one(command,kind,seconds,uid)
        except Exception:
            results[kind] = checked_result({},kind)
    print(json.dumps(results,separators=(',',':')),flush=True)


def clean_port():
    value = int(os.environ.get('PORT','10000'))
    if not 1024<=value<=65535:
        raise ValueError('invalid_qa_port')
    return value


def initial():
    if os.geteuid()!=0:
        raise SystemExit('qa_supervisor_uid_required')
    port = clean_port()
    uid = pwd.getpwnam('cg-qa-status').pw_uid
    if uid!=10003:
        raise SystemExit('qa_status_uid_invalid')
    child = subprocess.Popen([sys.executable,'-I','-B',SELF,'--run-checks'],cwd=HERE,stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,close_fds=True,env=ENV)
    results = child.stdout.fileno()
    os.set_inheritable(results,True)
    os.setgroups([])
    os.setgid(uid)
    os.setuid(uid)
    if ctypes.CDLL(None).prctl(38,1,0,0,0):
        raise SystemExit('qa_status_privilege_drop_failed')
    # Re-exec discards the original service process memory and environment.
    os.execve(sys.executable,[sys.executable,'-I','-B',SELF,'--serve',str(port),str(results),str(child.pid)],ENV)


def serve(port,descriptor,child_pid):
    if os.geteuid()!=10003:
        raise SystemExit('qa_status_uid_invalid')
    state = {'phase':'running'}
    def collect():
        nonlocal state
        try:
            with os.fdopen(descriptor,'rb') as pipe:
                raw = pipe.read(4097)
            if len(raw)>4096:
                raise ValueError('invalid_result')
            value = json.loads(raw)
            result = {k:checked_result(value.get(k),k) for k in ('dual_uid','private_viewer')}
            state = {'phase':'complete',**result}
        except Exception:
            state = {'phase':'check_failed'}
        finally:
            try:
                os.waitpid(child_pid,0)
            except ChildProcessError:
                pass
        print(json.dumps(state,separators=(',',':')),flush=True)

    class Status(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path not in ('/','/health','/status'):
                self.send_error(404)
                return
            data = json.dumps(state,separators=(',',':')).encode('ascii')
            self.send_response_only(200)
            self.send_header('Content-Type','application/json')
            self.send_header('Cache-Control','no-store')
            self.send_header('X-Content-Type-Options','nosniff')
            self.send_header('Content-Length',str(len(data)))
            self.end_headers()
            try:
                self.wfile.write(data)
            except (BrokenPipeError,ConnectionResetError):
                pass
        def send_error(self,code,message=None,explain=None):
            self.send_response_only(code)
            self.send_header('Content-Length','0')
            self.end_headers()
        def log_message(self,*args):
            pass

    threading.Thread(target=collect,daemon=True).start()
    ThreadingHTTPServer(('0.0.0.0',port),Status).serve_forever()


if __name__=='__main__':
    args = sys.argv[1:]
    if not args:
        initial()
    elif args==['--prepare-offline-policy']:
        prepare_offline_policy()
    elif args==['--run-checks']:
        run_checks()
    elif len(args)==4 and args[0]=='--serve':
        serve(*(int(item) for item in args[1:]))
    else:
        raise SystemExit(2)
