"""One approved invocation of the existing supervisor; never product code."""
import time
STARTED = time.monotonic()
import base64
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import resource
import sys
import urllib.request

ROOT = Path(__file__).resolve().parent
COMMIT = '33bad18f8be322009108584a6b71f3d65fa9b351'
PINS = {'collector_browser_fixture.py': 'b0189e753e5219feb180b20aa5388386d9743932f69ca29b6c1a205170c124ed', 'session_restore_state.py': 'f4fefa446c64a064e5354454d186cd7e269639b36e71a837ba72ee3e0e0cc3a0', 'observation_supervisor.py': 'dcc206ae70b91f5c39e47473dd102e1872216d1055427dfd6d0af9d3caf6c4a7', 'session_state.py': 'dd64ffcfe432805f71863b389f68ec8d3f10f1a84524cf9f7153df97732c0760', 'session_runtime.py': '327b324d6e48655ae77d62b63a7704603b7ee6242db706cd1948c0c69d2b1aae', 'session_capture.py': '3544546386fc701e89656260d3d106e0a39557a31b6c96a7338e5d746f0d66ab'}
result = {'status': 'blocked', 'source_observer_launches': 0, 'ai_calls': 0, 'job_writes': 0}
reason = 'preflight'
reserved = False
attempt_path = None
crypto_evidence = None
try:
    if len(sys.argv) != 2 or sys.argv[1] not in ('--bootstrap', '--reuse'):
        raise ValueError()
    mode = sys.argv[1][2:]
    attempt_path = ROOT / ('attempt-' + mode + '.json')
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    if os.geteuid() == 0:
        raise ValueError()
    if ROOT.is_symlink() or ROOT.stat().st_uid != os.geteuid() or ROOT.stat().st_mode & 0o077:
        raise ValueError()
    for name, digest in PINS.items():
        path = ROOT / name
        if path.is_symlink() or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise ValueError()
    reason = 'session_crypto_unavailable'
    from observation_supervisor import activate_bundled_crypto
    crypto_evidence = activate_bundled_crypto()
    expected_revision = None
    if mode == 'reuse':
        reason = 'reuse_requires_verified_bootstrap'
        prior = ROOT / 'attempt-bootstrap.json'
        if (prior.is_symlink() or prior.stat().st_uid != os.geteuid()
                or prior.stat().st_mode & 0o077 or prior.stat().st_size > 65536):
            raise ValueError()
        receipt = json.loads(prior.read_text())
        persisted = receipt.get('session_persistence', {})
        if (receipt.get('observation_report', {}).get('access_verified') is not True
                or not all(receipt.get(name) is True for name in (
                    'own_descendants_gone','lock_release_confirmed','within_60_seconds',
                    'temporary_files_removed','filtered_evidence_retained'))
                or persisted.get('saved') is not True or persisted.get('outcome') != 'ready'
                or type(persisted.get('revision')) is not int or not 0 < persisted['revision'] < 2**63):
            raise ValueError()
        expected_revision = persisted['revision']
    reason = 'already_attempted_do_not_repeat'
    fd = os.open(attempt_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    os.write(fd, b'{"reserved":true}')
    os.close(fd)
    reserved = True
    reason = 'worker_preflight_failed'
    rows = []
    for proc in Path('/proc').iterdir():
        if not proc.name.isdecimal():
            continue
        try:
            argv = (proc / 'cmdline').read_bytes().split(b'\0')
        except FileNotFoundError:
            continue
        if b'standalone_collector/app.py' in argv:
            rows.append(proc)
        if int(proc.name) != os.getpid() and any(Path(os.fsdecode(v)).name in {
            'chrome', 'chromium', 'headless_shell', 'chrome-headless-shell',
            'hover_source_check.py', 'collector_browser_fixture.py', 'observation_supervisor.py'
        } for v in argv if v):
            raise ValueError()
    if len(rows) != 1:
        raise ValueError()
    proc = rows[0]
    stamp = (proc / 'stat').read_text().rsplit(')', 1)[1].split()[19]
    with (proc / 'environ').open('rb') as stream:
        raw = stream.read(1048577)
    if len(raw) > 1048576:
        raise ValueError()
    worker = {os.fsdecode(k): os.fsdecode(v) for item in raw.split(b'\0') if b'=' in item
              for k, v in [item.split(b'=', 1)]}
    del raw
    if (worker.get('RENDER_GIT_COMMIT') != COMMIT
            or worker.get('COLLECTION_BRIDGE_ENABLED', '').lower() != 'false'
            or worker.get('COINGLASS_ACCOUNT_DIAGNOSTIC_ON_START', '').lower() == 'true'
            or not os.path.samefile('/proc/self/exe', proc / 'exe')
            or Path(sys.prefix).resolve() != (Path.cwd() / '.venv').resolve()
            or importlib.metadata.version('playwright') != '1.55.0'):
        raise ValueError()
    with urllib.request.urlopen('http://127.0.0.1:' + worker.get('PORT', '10000') + '/health', timeout=3) as response:
        health = json.loads(response.read(16384))
    if health.get('mode') != 'not_configured':
        raise ValueError()
    selected = {k: worker[k] for k in ('PATH', 'HOME', 'LANG', 'LC_CTYPE', 'PLAYWRIGHT_BROWSERS_PATH',
        'COINGLASS_COOKIE_HEADER', 'COINGLASS_STORAGE_STATE_JSON', 'COINGLASS_SESSION_KEY',
        'DATABASE_URL', 'COLLECTION_BRIDGE_ENABLED', 'RENDER_GIT_COMMIT') if k in worker}
    worker.clear()
    reason = 'restoration_input_invalid'
    sys.path.insert(0, str(ROOT))
    from session_restore_state import validate_storage_state
    key = base64.b64decode(selected.get('COINGLASS_SESSION_KEY', ''), validate=True)
    if len(key) != 32:
        raise ValueError()
    key = None
    if mode == 'bootstrap':
        validate_storage_state(selected.get('COINGLASS_STORAGE_STATE_JSON'))
    if stamp != (proc / 'stat').read_text().rsplit(')', 1)[1].split()[19]:
        raise ValueError()
    os.environ.clear()
    os.environ.update(selected)
    os.environ['COINGLASS_REUSE_MODE'] = mode
    if expected_revision is not None:
        os.environ['COINGLASS_REUSE_EXPECTED_REVISION'] = str(expected_revision)
    selected.clear()
    sys.path.insert(0, str(ROOT))
    import observation_supervisor as supervisor
    supervisor.LIVE_REVIEW_ACCEPTED = True
    result = supervisor.run_source_probe(STARTED)
except BaseException:
    result['reason'] = reason
finally:
    for key in ('COINGLASS_COOKIE_HEADER', 'COINGLASS_STORAGE_STATE_JSON', 'COINGLASS_SESSION_KEY', 'DATABASE_URL'):
        os.environ.pop(key, None)
    result['entry_elapsed_seconds'] = round(time.monotonic() - STARTED, 3)
    if crypto_evidence is not None:
        result['crypto_dependency'] = crypto_evidence
    if reserved:
        with attempt_path.open('w') as stream:
            json.dump(result, stream, allow_nan=False, separators=(',', ':'))
    print('COINGLASS_REUSE_12H_RESULT ' + json.dumps(result, allow_nan=False, separators=(',', ':')), flush=True)
