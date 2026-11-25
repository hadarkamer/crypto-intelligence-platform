"""Passive service and isolated build for a manually provisioned native BTC pilot."""
import ast
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

SERVICE = 'decision-hub-coinglass-pilot-20261125'
BRANCH = 'qa/coinglass-native-pilot-service-20261125'
PRODUCT = 'f8412b67417f8636f9bebb11a028096bbc0ed1eb'
RUNTIME_DIGEST = 'a0fdcfbbf446a426ae0f2adcbd62c46718a3ad816e8ff1926bf178cfeed71070'
CLEAN = frozenset({
    'PATH','HOME','USER','TMPDIR','LANG','LANGUAGE','LC_ALL','LC_CTYPE','TZ',
    'LD_LIBRARY_PATH','VIRTUAL_ENV','PLAYWRIGHT_BROWSERS_PATH','XDG_CACHE_HOME',
    'XDG_RUNTIME_DIR','SSL_CERT_FILE','SSL_CERT_DIR','FONTCONFIG_PATH','PORT',
    'IS_PULL_REQUEST','RENDER','RENDER_SERVICE_NAME','RENDER_GIT_BRANCH','RENDER_GIT_COMMIT',
    'PYTHONDONTWRITEBYTECODE','COINGLASS_QA_CLEAN_STAGE',
})


def require_service(env):
    if (env.get('RENDER') != 'true' or env.get('IS_PULL_REQUEST') != 'false'
            or env.get('RENDER_SERVICE_NAME') != SERVICE
            or env.get('RENDER_GIT_BRANCH') != BRANCH
            or not re.fullmatch('[0-9a-f]{40}', env.get('RENDER_GIT_COMMIT',''))):
        raise SystemExit('native_pilot_scope_mismatch')


def isolate(stage):
    require_service(os.environ)
    # Re-exec before any test, dependency installer, service or probe is imported.
    # The initial process environment is not accessible to candidate code after exec.
    clean = {name:value for name, value in os.environ.items() if name in CLEAN}
    clean.update(COINGLASS_QA_CLEAN_STAGE=stage, PYTHONDONTWRITEBYTECODE='1')
    if clean != dict(os.environ):
        os.execve(sys.executable, [sys.executable,*sys.orig_argv[1:]], clean)


def checked_runtime(root):
    pilot = root/'dedicated_pilot.py'
    copied = next(ast.literal_eval(n.value) for n in ast.parse(pilot.read_text()).body
                  if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='COPIED_MODULES' for t in n.targets))
    paths = {root/(n+'.py') for n in copied} | {pilot,root/'dedicated_auth_state.sql'}
    paths.update(p for p in (root/'runtime').rglob('*') if p.is_file() and '__pycache__' not in p.parts)
    if any(p.is_symlink() for p in paths):
        return False
    digest = {str(p.relative_to(root.parent)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}
    content = json.dumps(digest,sort_keys=True,separators=(',',':')).encode()
    return hashlib.sha256(content).hexdigest() == RUNTIME_DIGEST


def require_runtime(root):
    if not checked_runtime(root):
        raise SystemExit('native_pilot_runtime_mismatch')


def build_contract_already_run_in_ci(command, **_options):
    # The candidate passed these contracts in its pinned CI revision. This QA host
    # packages the same runtime, which must match the digest before serving /health.
    if command[:3] != [sys.executable,'-m','unittest']:
        raise RuntimeError('unexpected_pilot_build_operation')


def service_info(env):
    require_service(env)
    return {'mode':'native_manual_pilot','candidate':PRODUCT,'qa_commit':env['RENDER_GIT_COMMIT']}


def manual_pilot(root, commit, stage):
    info = service_info(os.environ)
    if info['qa_commit'] != commit or stage not in ('preflight','first','reuse','fresh'):
        raise SystemExit('qa_commit_or_stage_mismatch')
    require_runtime(root)
    keys = CLEAN | {'DATABASE_URL','COINGLASS_SESSION_KEY','COINGLASS_LOGIN_EMAIL',
                    'COINGLASS_LOGIN_PASSWORD','COINGLASS_SESSION_STORE_ENABLED','COINGLASS_MODERN_UI_ENABLED'}
    env = {k:os.environ[k] for k in keys if k in os.environ}
    print(json.dumps(info,sort_keys=True),flush=True)
    os.execve(sys.executable, [sys.executable,'-B',str(root/'dedicated_pilot.py'),stage], env)


def make_server(port, info):
    payload = json.dumps(info, sort_keys=True, separators=(',',':')).encode()
    class Health(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass
        def do_GET(self):
            if self.path != '/health':
                self.send_response(404)
                self.send_header('Content-Length','0')
                self.end_headers()
                return
            self.send_response(200)
            self.send_header('Content-Type','application/json')
            self.send_header('Cache-Control','no-store')
            self.send_header('Content-Length',str(len(payload)))
            self.end_headers()
            if self.command == 'GET':
                self.wfile.write(payload)
        do_HEAD = do_GET
    class Server(HTTPServer):
        def get_request(self):
            sock,peer = super().get_request()
            sock.settimeout(5)
            return sock,peer
    return Server(('0.0.0.0',port),Health)


def entry(args, root):
    isolate('service')
    info = service_info(os.environ)
    if args not in ([],['--probe'],['--check-config']):
        raise SystemExit('native_pilot_command_unavailable')
    require_runtime(root)
    if args:
        print(json.dumps(info,sort_keys=True),flush=True)
        return
    with make_server(int(os.environ.get('PORT','10000')),info) as server:
        server.serve_forever()

def build(root):
    isolate('build')
    # Only transport and DB driver are needed ahead of the candidate's pinned
    # Playwright, Chromium, Pillow, requests and encryption dependency install.
    options = {'cwd': root, 'stdin':subprocess.DEVNULL, 'check':True}
    subprocess.run([sys.executable, '-m', 'pip', 'install', '--disable-pip-version-check', '--no-input',
                    'aiohttp==3.11.11', 'psycopg[binary]==3.2.3'], timeout=240, **options)
    subprocess.run([sys.executable, '-B', 'prepare.py'], timeout=600, **options)
    subprocess.run([sys.executable, '-B', '-m', 'unittest', 'test_readiness', '-q'], timeout=90, **options)
    require_runtime(root)
    print('NATIVE_QA_RUNTIME_VERIFIED', flush=True)


if __name__ == '__main__':
    root = Path(__file__).resolve().parent
    if sys.argv[1:] == ['build']:
        build(root)
    elif sys.argv[1:] == ['serve']:
        entry([], root)
    elif len(sys.argv) == 4 and sys.argv[1] == 'pilot':
        manual_pilot(root, sys.argv[2], sys.argv[3])
    else:
        raise SystemExit('native_pilot_command_unavailable')
