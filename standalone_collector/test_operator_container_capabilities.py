"""Offline integration fixture for a standard container with two real service UIDs.

Run only in the candidate image on an isolated network. No production code,
credentials, persistent account, browser weakening or provider access is used.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import pwd
import socket
import subprocess
import sys
import tempfile
from contextlib import suppress
from threading import Thread

COLLECTOR = 10001
DESKTOP = 10002
CHROMIUM = '/usr/bin/chromium'
HERE = Path(__file__).resolve()


def _confidential_collector(directory):
    # Simulate the actual sensitive boundaries: owner-only material, a process
    # bearing a synthetic marker, a private FD and even an owner-only named socket.
    path = directory/'private-marker'
    path.write_text('synthetic-collector-marker')
    path.chmod(0o600)
    with path.open('rb') as marker, socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as endpoint:
        endpoint.bind(str(directory/'control'))
        os.chmod(directory/'control',0o600)
        endpoint.listen(1)
        print(json.dumps({'pid':os.getpid(),'fd':marker.fileno()}),flush=True)
        sys.stdin.buffer.read(1)


def _expect_denied(operation):
    try:
        operation()
    except PermissionError:
        return True
    return False


def _private_access_denied(collector, pid, fd):
    def connection():
        with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as endpoint:
            endpoint.connect(str(collector/'control'))
    return (os.geteuid()==DESKTOP and os.getegid()==DESKTOP and not os.getgroups()
            and 'FIXTURE_COLLECTOR_ONLY' not in os.environ
            and _expect_denied(lambda:(collector/'private-marker').read_bytes())
            and _expect_denied(connection)
            and _expect_denied(lambda:Path(f'/proc/{pid}/environ').read_bytes())
            and _expect_denied(lambda:Path(f'/proc/{pid}/fd/{fd}').read_bytes()))


class _OfflineProxy:
    """Never resolves or forwards anything. Record only the two synthetic outcomes."""
    def __init__(self,port=0,source_host='www.coinglass.com'):
        self.source = (source_host + ':443').encode('ascii')
        self.source_seen = self.forbidden_seen = False
        self.listener = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        self.listener.bind(('127.0.0.1',port))
        self.listener.listen(12)
        self.listener.settimeout(.2)
        self.port = self.listener.getsockname()[1]
        self.thread = Thread(target=self._serve,daemon=True)

    def _serve(self):
        while True:
            try:
                client,_ = self.listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            with client:
                client.settimeout(2)
                try:
                    line = client.recv(16384).split(b'\r\n',1)[0].split()
                    if len(line)>=2 and line[0]==b'CONNECT':
                        self.source_seen |= line[1]==self.source
                        self.forbidden_seen |= line[1]==b'denied.invalid:443'
                    client.sendall(b'HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
                except OSError:
                    pass

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self,*args):
        self.listener.close()
        self.thread.join(3)


def _browser_fixture(collector,pid,fd,home,proxy_port,source_host):
    if not _private_access_denied(collector,pid,fd):
        return 'collector_boundary_failed'
    print('collector_boundary_verified',flush=True)
    from playwright.sync_api import Error, sync_playwright
    failing_phase = 'ordinary_chromium_sandbox_failed'
    try:
        with sync_playwright() as runtime:
            browser = runtime.chromium.launch(executable_path=CHROMIUM,
                chromium_sandbox=True, headless=True,
                args=[f'--proxy-server=http://127.0.0.1:{proxy_port}',
                      '--disable-background-networking','--disable-quic','--disable-sync',
                      '--host-resolver-rules=MAP * ~NOTFOUND'])
            try:
                failing_phase = 'neutral_browser_navigation_failed'
                page = browser.new_page()
                page.goto('about:blank')
                if page.url!='about:blank':
                    return 'neutral_browser_navigation_failed'
                print('browser_sandbox_verified',flush=True)
                failing_phase = 'mandatory_browser_url_policy_unconfirmed'
                # The test container has --network none and the local fixture
                # returns 403. No actual DNS or provider connection can occur.
                with suppress(Error):
                    page.goto('https://'+source_host+'/offline-operator-fixture',timeout=5000)
                with suppress(Error):
                    page.goto('https://denied.invalid/offline-operator-fixture',timeout=5000)
                body = page.locator('body').inner_text(timeout=3000)
                if 'ERR_BLOCKED_BY_ADMINISTRATOR' not in body:
                    return 'mandatory_browser_url_policy_unconfirmed'
            finally:
                browser.close()
    except Exception as exc:
        diagnostic = str(exc).lower()
        # The input is an offline fixture; even its exception text stays private.
        if failing_phase == 'ordinary_chromium_sandbox_failed':
            if 'failed to move to new namespace' in diagnostic or 'no usable sandbox' in diagnostic:
                return 'chromium_namespace_sandbox_unavailable'
            if 'suid sandbox helper' in diagnostic:
                return 'chromium_suid_helper_unavailable'
        return failing_phase
    return 'verified'


def _result(networked_qa=False):
    if os.getuid()!=0 or pwd.getpwuid(COLLECTOR).pw_name!='collector' or pwd.getpwuid(DESKTOP).pw_name!='cg-window':
        return {'status':'fixture_image_or_supervisor_user_invalid'}
    source_host = 'qa-source.invalid' if networked_qa else 'www.coinglass.com'
    qa_port = 31497
    if networked_qa:
        # Only the dedicated QA image declares the fixed loopback sink and
        # disallows every real source/identity origin. Fail before Chromium
        # if a networked run points at an ordinary collector installation.
        try:
            policy = json.loads(Path('/etc/chromium/policies/managed/operator.json').read_text())
        except (OSError,ValueError):
            return {'status':'qa_network_guard_missing'}
        if (policy.get('ProxyMode')!='fixed_servers' or policy.get('ProxyServer')!=f'http://127.0.0.1:{qa_port}'
                or policy.get('URLBlocklist')!=['*'] or policy.get('URLAllowlist')!=['about:blank','https://qa-source.invalid']
                or policy.get('QuicAllowed') is not False or policy.get('DnsOverHttpsMode')!='off'):
            return {'status':'qa_network_guard_missing'}
    elif {name for _,name in socket.if_nameindex()}!={'lo'}:
        return {'status':'fixture_requires_no_external_network'}
    env = {'PATH':'/usr/local/bin:/usr/bin:/bin','LANG':'C.UTF-8','PYTHONDONTWRITEBYTECODE':'1'}
    with tempfile.TemporaryDirectory(dir='/dev/shm',prefix='dual-uid-proof-') as temp:
        root = Path(temp)
        root.chmod(0o711)
        collector,desktop = root/'collector',root/'desktop'
        for path,uid in ((collector,COLLECTOR),(desktop,DESKTOP)):
            path.mkdir(mode=0o700)
            os.chown(path,uid,uid)
        writer = subprocess.Popen([sys.executable,'-B',str(HERE),'--collector',str(collector)],
            user=COLLECTOR,group=COLLECTOR,extra_groups=(),stdin=subprocess.PIPE,stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,env={**env,'HOME':str(collector),'FIXTURE_COLLECTOR_ONLY':'fixture'},close_fds=True)
        try:
            marker = json.loads(writer.stdout.readline())
            with _OfflineProxy(qa_port if networked_qa else 0,source_host) as proxy:
                runner = subprocess.run([sys.executable,'-B',str(HERE),'--desktop',str(collector),
                    str(marker['pid']),str(marker['fd']),str(desktop),str(proxy.port),source_host],
                    user=DESKTOP,group=DESKTOP,extra_groups=(),stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=35,
                    env={**env,'HOME':str(desktop),'TMPDIR':str(desktop),'XDG_RUNTIME_DIR':str(desktop)},
                    close_fds=True)
                # Do not publish subprocess output; expose only fixed result fields.
                lines = set(runner.stdout.decode('ascii',errors='ignore').splitlines())
                states = ('collector_boundary_failed','neutral_browser_navigation_failed',
                          'mandatory_browser_url_policy_unconfirmed','ordinary_chromium_sandbox_failed',
                          'chromium_namespace_sandbox_unavailable','chromium_suid_helper_unavailable','verified')
                outcome = next((value for value in states if value in lines),'fixture_incomplete')
                if outcome=='verified' and (not proxy.source_seen or proxy.forbidden_seen):
                    outcome = 'restricted_proxy_unconfirmed'
                return {'status':outcome,'two_uid_confidential_boundary':'collector_boundary_verified' in lines,
                        'ordinary_chromium_sandbox':'browser_sandbox_verified' in lines,
                        'source_requested_local_proxy':proxy.source_seen,'forbidden_origin_reached_proxy':proxy.forbidden_seen,
                        'provider_requests':0}
        finally:
            writer.stdin.close()
            writer.wait(timeout=5)


if __name__=='__main__':
    if len(sys.argv)==3 and sys.argv[1]=='--collector':
        _confidential_collector(Path(sys.argv[2]))
    elif len(sys.argv)==8 and sys.argv[1]=='--desktop' and sys.argv[7] in ('qa-source.invalid','www.coinglass.com'):
        stage = _browser_fixture(Path(sys.argv[2]),int(sys.argv[3]),int(sys.argv[4]),Path(sys.argv[5]),int(sys.argv[6]),sys.argv[7])
        print(stage,flush=True)
        raise SystemExit(0 if stage=='verified' else 1)
    elif sys.argv[1:] in (['--check'],['--networked-qa']):
        result = _result(networked_qa=sys.argv[1]=='--networked-qa')
        print(json.dumps(result,sort_keys=True))
        raise SystemExit(0 if result['status']=='verified' else 1)
    else:
        raise SystemExit(2)
