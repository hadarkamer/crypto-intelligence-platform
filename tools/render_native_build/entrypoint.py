"""Build-only native correctness check; publish a small synthetic result only."""
from __future__ import annotations

import base64
from datetime import datetime, timezone
import hashlib
import html
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT/'tools'))
import run_ordered_native_postgres as native

VERSION = 'ordered-native-build-check-v1'
BUILDER_SHA256 = '08e665705098479fcceb4366bfb4d7c882538c3ddcecb917a6312a82ff479320'
CONTRACT = ROOT/'tools/ordered_native_runtime_contract_20261012.json'
PUBLIC = ROOT/'native-build-public'
RECEIPTS = ('preflight.json', 'native_lifecycle.json', 'runner_cleanup.json',
            'host/connection.json', 'correctness/correctness_receipt.json',
            'correctness/fixture_manifest.json')
MAX_RECEIPT_BYTES = 262144
MAX_RECEIPTS_BYTES = 1048576
PUBLIC_KEYS = {'version', 'result', 'scope', 'build_commit', 'checked_at_utc',
               'synthetic_rows', 'positive_input_cases', 'memory_passes',
               'runtime', 'receipt_sha256', 'limitations'}
LIMITATIONS = ['No memory measurement or production deployment.',
              'Declared runtime contract only; not full production equivalence.',
              'Cleanup covers owned groups and recorded postmaster descendants.']


def receipt_bytes(root, name):
    if name not in RECEIPTS:
        raise ValueError('Receipt path is not allowed')
    path = root/name
    if any(part.is_symlink() for part in (path, *path.parents)):
        raise ValueError('Receipt symlink is forbidden')
    if path.stat().st_size > MAX_RECEIPT_BYTES:
        raise ValueError('Receipt is too large')
    return path.read_bytes()


def preserve_receipts(root):
    """Exact allowlisted bytes to private build logs, never to the static tree."""
    present = [(name, receipt_bytes(root, name)) for name in RECEIPTS if (root/name).exists()]
    if sum(len(raw) for _, raw in present) > MAX_RECEIPTS_BYTES:
        raise ValueError('Receipt bundle is too large')
    manifest = []
    for name, raw in present:
        encoded = base64.b64encode(raw).decode('ascii')
        chunks = [encoded[i:i+2048] for i in range(0, len(encoded), 2048)] or ['']
        entry = {'path':name, 'bytes':len(raw), 'sha256':hashlib.sha256(raw).hexdigest(),
                 'chunks':len(chunks)}
        for index, chunk in enumerate(chunks):
            print('[NATIVE_BUILD_RECEIPT] '+json.dumps(dict(entry, kind='chunk',
                  index=index, data=chunk), sort_keys=True), flush=True)
        print('[NATIVE_BUILD_RECEIPT] '+json.dumps(dict(entry, kind='file_complete'),
              sort_keys=True), flush=True)
        manifest.append(entry)
    print('[NATIVE_BUILD_RECEIPT] '+json.dumps({'kind':'complete', 'version':VERSION,
          'files':manifest}, sort_keys=True), flush=True)


def preserve_failure_logs(root, prefix=''):
    for name in ('host/initdb.log', 'host/postgres.log', 'correctness.stdout.log', 'correctness.stderr.log',
                 'memory_prepare.stdout.log', 'memory_prepare.stderr.log',
                 'memory_measure.stdout.log', 'memory_measure.stderr.log',
                 'concurrent_prepare.stdout.log', 'concurrent_prepare.stderr.log',
                 'concurrent_measure.stdout.log', 'concurrent_measure.stderr.log'):
        path = root/name
        if not path.is_file() or any(part.is_symlink() for part in (path, *path.parents)):
            continue
        size = path.stat().st_size
        with path.open('rb') as stream:
            stream.seek(max(0, size-8192))
            tail = stream.read(8192)
        encoded = base64.b64encode(tail).decode('ascii')
        chunks = [encoded[i:i+2048] for i in range(0, len(encoded), 2048)] or ['']
        for index, chunk in enumerate(chunks):
            print('[NATIVE_BUILD_DIAGNOSTIC] '+json.dumps({'path':prefix+name, 'original_bytes':size,
                'tail_bytes':len(tail), 'truncated':size>len(tail), 'tail_sha256':hashlib.sha256(tail).hexdigest(),
                'index':index, 'chunks':len(chunks), 'data':chunk}, sort_keys=True), flush=True)


def validate_run(root, bindings):
    raw = {name:receipt_bytes(root, name) for name in RECEIPTS}
    data = {name:json.loads(value) for name,value in raw.items()}
    pre, life, cleanup, host, correct, _manifest = (data[name] for name in RECEIPTS)
    pre_sha = hashlib.sha256(raw['preflight.json']).hexdigest()
    if (pre.get('result') != 'READY' or pre.get('source_bindings') != bindings
            or pre.get('runner_sha256') != bindings['runner']
            or pre.get('contract_sha256') != bindings['contract']
            or pre.get('contract') != json.loads(CONTRACT.read_text())
            or native.compare_runtime(pre['contract'], pre['observed'])[1]
            or life.get('result') != 'PASS' or life.get('native_lifecycle_tested') is not True
            or life.get('sources_unchanged') is not True or life.get('measured_passes') != 0
            or life.get('source_bindings') != bindings or life.get('preflight_sha256') != pre_sha
            or cleanup.get('exit_code') != 0 or host.get('runtime_preflight_sha256') != pre_sha
            or host.get('host') != '127.0.0.1' or host.get('engine') != 'native PostgreSQL'
            or host.get('dbname') != 'postgres' or host.get('user') != 'postgres'
            or host.get('run_token') != life.get('run_token')):
        raise ValueError('Native receipt gate failed')
    for item in [*life.get('cleanup', []), *cleanup.get('cleanup', [])]:
        if item.get('group_gone') is not True or item.get('forced') is not False:
            raise ValueError('Process cleanup is incomplete')
    descendants = life.get('descendant_cleanup', {})
    if descendants.get('forced') is not False or descendants.get('tracked_live_remaining') != 0:
        raise ValueError('Recorded descendant cleanup is incomplete')
    proof_sha = native.verify_correctness(root, life['run_token'], root/'host/connection.json',
                                          bindings, pre['observed']['driver_binding'])
    expected_cases = {'empty_no_query', 'duplicate_and_missing', 'existing_int_normalization',
                      'signed_int64_boundaries', 'string_int64_boundaries'}
    if (life.get('correctness_receipt_sha256') != proof_sha
            or {case['case'] for case in correct['source_semantics_audit']['cases']} != expected_cases
            or len(correct['source_semantics_audit']['cases']) != 5
            or correct.get('connection_count') != 3):
        raise ValueError('Correctness receipt gate failed')
    runtime = {key:pre['observed'][key] for key in native.FIELDS if key in pre['observed']}
    return {'synthetic_rows':32, 'positive_input_cases':5, 'memory_passes':0,
            'runtime':runtime, 'receipt_sha256':{name:hashlib.sha256(value).hexdigest()
                                                for name,value in raw.items()}}


def publish_summary(directory, summary):
    if (set(summary) != PUBLIC_KEYS or summary.get('result') != 'PASS'
            or summary.get('version') != VERSION or summary.get('scope') != 'synthetic_native_correctness_only'
            or summary.get('limitations') != LIMITATIONS
            or summary.get('synthetic_rows') != 32 or summary.get('positive_input_cases') != 5
            or summary.get('memory_passes') != 0
            or set(summary.get('receipt_sha256', {})) != set(RECEIPTS)
            or any(not re.fullmatch('[0-9a-f]{64}', item) for item in summary['receipt_sha256'].values())
            or not set(summary.get('runtime', {})) <= set(native.FIELDS)):
        raise ValueError('Public summary schema is not allowed')
    if not re.fullmatch('[0-9a-f]{40}', summary.get('build_commit', '')):
        raise ValueError('Public summary lacks an exact build commit')
    if directory.exists() or any(item.is_symlink() for item in (directory, *directory.parents)):
        raise ValueError('Publish directory must be new and contain no symlinks')
    raw = json.dumps(summary, sort_keys=True, indent=2)+'\n'
    if len(raw.encode()) > 8192:
        raise ValueError('Public summary is too large')
    pending = directory.with_name(directory.name+'.pending')
    pending.mkdir(mode=0o700)
    (pending/'summary.json').write_text(raw)
    (pending/'index.html').write_text('<!doctype html><meta charset="utf-8">'
        '<title>Isolated native correctness check</title><h1>Synthetic correctness: PASS</h1>'
        '<p>This is a build-only test. It makes no production memory or OOM-resolution claim.</p>'
        '<pre>'+html.escape(raw)+'</pre>')
    pending.rename(directory)


def run_step(command, timeout):
    existing = set(native.OWNED_PROCESSES)
    try:
        proc = native.spawn_owned(command)
        if proc.wait(timeout=timeout):
            raise RuntimeError('Build step failed')
    finally:
        previous = {sig:signal.signal(sig, signal.SIG_IGN) for sig in (signal.SIGINT, signal.SIGTERM)}
        try:
            # spawn_owned registers before delivering a deferred signal. That
            # interruption must clean the child before a caller removes PGDATA.
            failed = False
            for pid, child in list(native.OWNED_PROCESSES.items()):
                if pid in existing: continue
                cleanup = native.stop_owned(child, timeout=75)
                failed |= not cleanup['group_gone'] or cleanup['forced']
            if failed: raise RuntimeError('Build step cleanup failed')
        finally:
            for sig, handler in previous.items(): signal.signal(sig, handler)


def main():
    stage, commit, run_dir = 'setup', None, None
    def interrupted(_signum, _frame): raise native.RunnerInterrupted('Build interrupted')
    previous = {sig:signal.signal(sig, interrupted) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        if PUBLIC.exists() or any(key.startswith('PG') for key in os.environ):
            raise ValueError('Existing public output or ambient PostgreSQL configuration')
        if sys.version_info[:3] != (3, 11, 9) or os.geteuid() == 0:
            raise ValueError('Expected non-root CPython3.11.9 build environment')
        commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT,
                                         text=True, timeout=10).strip()
        if not re.fullmatch('[0-9a-f]{40}', commit): raise ValueError('Missing build identity')
        builder = Path(__file__).with_name('build_postgres.py')
        if native.sha_file(builder) != BUILDER_SHA256: raise ValueError('Builder provenance mismatch')
        stage = 'dependencies'
        run_step([sys.executable, '-m', 'pip', 'install', '--disable-pip-version-check',
                  'psycopg[binary]==3.2.3', 'requests==2.32.3'], 180)
        stage = 'postgres_build'
        run_step([sys.executable, str(builder)], 2400)
        with tempfile.TemporaryDirectory(prefix='onpg-', dir='/tmp') as temporary:
            run_dir = Path(temporary)/'run'
            args = type('Arguments', (), {'python':sys.executable,
                'pg_bin':str(ROOT/'.render_preflight_pg/bin'), 'contract':str(CONTRACT)})()
            bindings = native.source_bindings(args)
            stage = 'native_correctness'
            accepted = False
            try:
                run_step([sys.executable, '-I', str(ROOT/'tools/run_ordered_native_postgres.py'),
                    '--python', args.python, '--pg-bin', args.pg_bin, '--contract', args.contract,
                    '--output', str(run_dir)], 360)
                details = validate_run(run_dir, bindings)
                accepted = True
            finally:
                preserve_receipts(run_dir)
                if not accepted: preserve_failure_logs(run_dir)
            stage = 'publication'
            summary = dict(details, version=VERSION, result='PASS', scope='synthetic_native_correctness_only',
                build_commit=commit, checked_at_utc=datetime.now(timezone.utc).isoformat(),
                limitations=LIMITATIONS)
            publish_summary(PUBLIC, summary)
            print('[NATIVE_BUILD_SUMMARY] '+json.dumps(summary, sort_keys=True), flush=True)
        return 0
    except (Exception, native.RunnerInterrupted) as error:
        print('[NATIVE_BUILD_SUMMARY] '+json.dumps({'version':VERSION, 'result':'FAIL',
              'stage':stage, 'build_commit':commit, 'error_type':type(error).__name__}, sort_keys=True), flush=True)
        return 1
    finally:
        for sig in previous: signal.signal(sig, signal.SIG_IGN)
        for proc in list(native.OWNED_PROCESSES.values()): native.stop_owned(proc, timeout=75)
        for sig, handler in previous.items(): signal.signal(sig, handler)


if __name__ == '__main__':
    from readiness_entrypoint import main as readiness_main
    raise SystemExit(readiness_main())
