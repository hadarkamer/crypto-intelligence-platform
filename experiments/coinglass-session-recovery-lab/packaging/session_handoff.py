#!/usr/bin/env python3
"""Bounded no-echo candidate contract; accepts PUBLIC SYNTHETIC fixtures only.

No live mode exists. Neither a flag nor an environment variable enables real
credentials. This module imports no browser/app and makes no network request.
"""
from __future__ import annotations
import ast
import json
import hashlib
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import termios
import time

DEFAULT_RUNTIME = Path('/lab/collector/standalone_collector/runtime')
CAPTURE_SHA = '3a8778e0c6de4b4c1e24dc8dea3292e2e6c5ce2e2163bad251c3edc12a2f129a'
LOADER_SHA = 'dad80585a13e74de5a3d3d6f936a4b75b2277c1d11aae49edfb09e5864efc3f9'
MAX_TTY_BYTES = 65536  # Includes editing bytes and the terminating newline.
MAX_TTY_SECONDS = 20.0
PROMPT = b'Public synthetic Cookie header: '
CANCEL_SIGNALS = {signal.SIGTERM, signal.SIGHUP, signal.SIGINT}
FIXTURES = {
    'single_obe': 'obe=PUBLIC_SYNTHETIC_SESSION_NOT_A_CREDENTIAL',
    'full_header': 'obe=PUBLIC_SYNTHETIC_SESSION_NOT_A_CREDENTIAL; fixture_language=en; fixture_marker=PUBLIC_ONLY; fixture_equals=a=b',
}


class Rejected(BaseException):
    def __init__(self, code): self.code = code


def validate_fixture(raw):
    if not isinstance(raw, str) or not raw: raise Rejected('input_missing')
    if len(raw.encode('utf8')) >= MAX_TTY_BYTES: raise Rejected('input_too_large')
    if any(ord(char) < 32 or ord(char) > 126 for char in raw): raise Rejected('input_malformed')
    if raw.lstrip().lower().startswith(('cookie:', 'set-cookie:')): raise Rejected('field_prefix_rejected')
    names = set()
    for part in raw.split(';'):
        name, separator, value = part.strip().partition('=')
        name = name.strip()
        if not separator or not name or not value.strip(): raise Rejected('input_malformed')
        if name in names: raise Rejected('duplicate_cookie_name')
        if any(not (c.isalnum() or c in "!#$%&'*+-.^_`|~") for c in name):
            raise Rejected('input_malformed')
        names.add(name)
    if 'obe' not in names: raise Rejected('session_cookie_missing')
    for fixture_id, value in FIXTURES.items():
        if raw == value: return fixture_id
    raise Rejected('synthetic_fixture_required')


def read_tty(*, max_bytes=MAX_TTY_BYTES, timeout=MAX_TTY_SECONDS):
    """Read at most max_bytes total bytes, with a monotonic deadline; no fallback.

    Noncanonical mode avoids the terminal's own canonical line-size truncation.
    Backspace is supported but still consumes the total byte budget.
    """
    if not 1 <= max_bytes <= MAX_TTY_BYTES or not 0 < timeout <= MAX_TTY_SECONDS:
        raise Rejected('input_limit_invalid')
    fd = None
    prior = None
    handlers = {}
    buffer = bytearray()
    consumed = 0
    try:
        fd = os.open('/dev/tty', os.O_RDWR | os.O_NOCTTY)
        if not os.isatty(fd): raise Rejected('secure_tty_required')
        prior = termios.tcgetattr(fd)
        settings = [*prior[:6], list(prior[6])]
        settings[3] &= ~(termios.ECHO | termios.ECHONL | termios.ICANON)
        settings[6][termios.VMIN] = 0
        settings[6][termios.VTIME] = 0
        termios.tcsetattr(fd, termios.TCSAFLUSH, settings)
        def interrupted(_signal, _frame): raise Rejected('input_interrupted')
        handlers = {sig: signal.signal(sig, interrupted) for sig in CANCEL_SIGNALS}
        os.write(fd, PROMPT)
        deadline = time.monotonic() + timeout
        while consumed < max_bytes:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not select.select([fd], [], [], remaining)[0]:
                raise Rejected('input_timeout')
            byte = os.read(fd, 1)
            if not byte: raise Rejected('input_missing')
            consumed += 1
            if byte in (b'\n', b'\r'):
                value = buffer.decode('ascii', errors='strict')
                validate_fixture(value)
                return value
            if byte in (b'\x7f', b'\x08'):
                if buffer: buffer.pop()
                continue
            if byte == b'\x04': raise Rejected('input_missing')
            if byte[0] < 32 or byte[0] > 126: raise Rejected('input_malformed')
            buffer.extend(byte)
        raise Rejected('input_too_large')
    except Rejected: raise
    except KeyboardInterrupt: raise Rejected('input_interrupted') from None
    except BaseException: raise Rejected('secure_tty_required') from None
    finally:
        buffer.clear()
        failed = False
        # Catchable cancellation cannot interrupt terminal restoration.
        mask = signal.pthread_sigmask(signal.SIG_BLOCK, CANCEL_SIGNALS)
        try:
            if fd is not None:
                if prior is not None:
                    try: termios.tcsetattr(fd, termios.TCSAFLUSH, prior)
                    except BaseException: failed = True
                os.close(fd)
            for sig, handler in handlers.items(): signal.signal(sig, handler)
        finally: signal.pthread_sigmask(signal.SIG_SETMASK, mask)
        if failed: raise Rejected('tty_restore_failed')


def fixture_environment(fixture_id, model=1):
    if fixture_id not in FIXTURES or type(model) is not int or model not in (1, 2, 3):
        raise Rejected('fixture_selection_invalid')
    return {'LANG': 'C.UTF-8', 'COLLECTION_HEATMAP_MODEL': str(model),
            'COINGLASS_COOKIE_HEADER': FIXTURES[fixture_id]}


def conversion_from_installed(runtime):
    path = Path(runtime).resolve() / 'market_vision/coinglass_heatmap_capture.py'
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != CAPTURE_SHA: raise Rejected('runtime_changed')
    source = raw.decode()
    tree = ast.parse(source)
    matches = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_load_cookie_header']
    if len(matches) != 1: raise Rejected('runtime_changed')
    node = matches[0]
    if hashlib.sha256(ast.get_source_segment(source, node).encode()).hexdigest() != LOADER_SHA:
        raise Rejected('runtime_changed')
    scope = {'os': os}
    exec(compile(ast.Module(body=[node], type_ignores=[]), '<installed-cookie-conversion>', 'exec'), scope)
    return scope['_load_cookie_header']()


def child_result(runtime):
    fixture_id = validate_fixture(os.environ.get('COINGLASS_COOKIE_HEADER', ''))
    if set(os.environ) - {'LANG', 'LC_CTYPE', 'COLLECTION_HEATMAP_MODEL', 'COINGLASS_COOKIE_HEADER'}:
        raise Rejected('child_environment_invalid')
    if os.environ.get('COLLECTION_HEATMAP_MODEL') != '1': raise Rejected('child_model_invalid')
    if any(value in arg for value in FIXTURES.values() for arg in sys.argv):
        raise Rejected('child_arguments_invalid')
    cookies = conversion_from_installed(runtime)
    parts = [part.strip().split('=', 1) for part in FIXTURES[fixture_id].split(';')]
    expected = [{'name': name, 'value': value, 'domain': '.coinglass.com', 'path': '/',
                 'secure': True, 'sameSite': 'Lax'} for name, value in parts]
    if cookies != expected: raise Rejected('conversion_mismatch')
    return {'fixture_id': fixture_id, 'cookie_count': len(cookies), 'conversion_verified': True,
            'environment_isolated': True, 'argv_safe': True, 'source_requests': 0,
            'browser_launches': 0, 'ai_calls': 0, 'live_enabled': False}


def run_handoff(raw, runtime):
    fixture_id = validate_fixture(raw)
    child = None
    report = None
    original_mask = signal.pthread_sigmask(signal.SIG_BLOCK, CANCEL_SIGNALS)
    try:
        command = [sys.executable, '-I', '-B', str(Path(__file__).resolve()), '--child', '--runtime', str(Path(runtime).resolve())]
        child = subprocess.Popen(command, env=fixture_environment(fixture_id), stdin=subprocess.DEVNULL,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True, start_new_session=True)
        output, errors = child.communicate(timeout=2)
        if child.returncode or errors or len(output) > 1024: raise Rejected('child_failed')
        report = json.loads(output)
        expected = {'status':'passed', 'fixture_id':fixture_id,
                    'cookie_count':len(FIXTURES[fixture_id].split(';')), 'conversion_verified':True,
                    'environment_isolated':True,'argv_safe':True,
                    'source_requests':0,'browser_launches':0,'ai_calls':0,'live_enabled':False}
        if report != expected: raise Rejected('child_output_invalid')
    except subprocess.TimeoutExpired: raise Rejected('child_timeout') from None
    finally:
        raw = None
        if child is not None:
            if child.poll() is None:
                try: os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError: pass
            child.wait(timeout=2)
            for pipe in (child.stdout, child.stderr): pipe.close()
        pending = signal.sigpending() & CANCEL_SIGNALS
        for _ in range(len(CANCEL_SIGNALS)):
            if signal.sigtimedwait(CANCEL_SIGNALS, 0) is None: break
        signal.pthread_sigmask(signal.SIG_SETMASK, original_mask)
        if pending: raise Rejected('handoff_interrupted')
    report['child_reaped'] = True
    return report


def arguments(argv):
    runtime, choice, child = DEFAULT_RUNTIME, None, False
    i = 0
    while i < len(argv):
        key = argv[i]
        if key == '--child' and not child: child = True; i += 1; continue
        if key == '--tty' and choice is None: choice = 'tty'; i += 1; continue
        if key == '--runtime' and i+1 < len(argv): runtime = Path(argv[i+1]); i += 2; continue
        if key == '--fixture-id' and choice is None and i+1 < len(argv) and argv[i+1] in FIXTURES:
            choice = argv[i+1]; i += 2; continue
        raise Rejected('invalid_arguments')
    if child and choice is not None: raise Rejected('invalid_arguments')
    if not child and choice is None: raise Rejected('invalid_arguments')
    return runtime, choice, child


def main(argv):
    def no_network(event, _args):
        if event.startswith('socket.'): raise Rejected('network_forbidden')
    sys.addaudithook(no_network)
    try:
        runtime, choice, child = arguments(argv)
        if child:
            signal.pthread_sigmask(signal.SIG_UNBLOCK, CANCEL_SIGNALS)
            report = child_result(runtime)
        else:
            raw = read_tty() if choice == 'tty' else FIXTURES[choice]
            report = run_handoff(raw, runtime)
        print(json.dumps({'status':'passed', **report}, separators=(',', ':')), flush=True)
        return 0
    except Rejected as exc: code = exc.code
    except BaseException: code = 'handoff_failed'
    print(json.dumps({'status':'blocked','reason':code,'live_enabled':False}, separators=(',', ':')), flush=True)
    return 2


if __name__ == '__main__': raise SystemExit(main(sys.argv[1:]))
