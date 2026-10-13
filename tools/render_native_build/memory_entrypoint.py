"""Bounded native ABBA experiment in the existing isolated build service."""
from __future__ import annotations

import base64
from datetime import datetime, timezone
import hashlib
import html
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile

import entrypoint as build
from ordered_postgres_memory_replay import source_hashes
from verify_ordered_native_memory import validate_comparison

VERSION = 'ordered-native-memory-build-v1'
ORDER = ('A1', 'B1', 'B2', 'A2')
MODULE = 'research_common_window_metrics_store.py'
BASE_SHA = 'e1e74fc7cac10b5b454614c242621857ab1ae5d23cdbffe6e20dc961051f5341'
FIX_SHA = 'ac2e05e3157928f9daaa8735c0aee533dc3ce2886251a1242e9344a93ba752fd'
ARM_FILES = ('preflight.json', 'native_lifecycle.json', 'runner_cleanup.json',
             'host/connection.json', 'memory/evidence.zip', 'memory/sealed.json')
RECEIPTS = tuple('correctness_gate/'+name for name in build.RECEIPTS) + tuple(
    'arms/'+arm+'/'+name for arm in ORDER for name in ARM_FILES) + ('comparison.json', 'staging.json')
LIMITATIONS = [
    'Synthetic engineering comparison: 2000 rows, one scope, one client thread per run.',
    'Four fresh processes and clusters in ABBA order; within-process passes are correlated.',
    'Sampled client RSS is not a continuous peak; lifetime high-water RSS is reported separately.',
    'Normal GC; no forced collection or allocator trimming. Observer work remains in both arms.',
    'Production loaded driver/libpq equivalence and concurrent-worker behavior remain unverified.',
    'No production deployment or claim that production OOM is resolved.',
]
PUBLIC_KEYS = {'version','result','scope','build_commit','checked_at_utc','synthetic_rows',
               'memory_passes','positive_input_cases','runtime','comparison','receipt_sha256','limitations'}
ARM_SUMMARY_KEYS = {'label','source','endpoint_rss_bytes','warm_median_rss_bytes',
    'warm_min_rss_bytes','warm_max_rss_bytes','warm_slope_bytes_per_pass','sampled_max_rss_bytes',
    'lifetime_maxrss_bytes','total_elapsed_ns','total_cpu_ns','total_observer_ns'}


def validate_public_comparison(value):
    if (type(value) is not dict or set(value) != {'order','rows','passes_per_arm','warm_passes','arms'}
            or value['order'] != list(ORDER) or value['rows'] != 2000
            or value['passes_per_arm'] != 12 or value['warm_passes'] != [4,12]
            or type(value['arms']) is not list or len(value['arms']) != 4):
        raise ValueError('Invalid public comparison shape')
    for label, arm in zip(ORDER,value['arms']):
        if (type(arm) is not dict or set(arm) != ARM_SUMMARY_KEYS or arm['label'] != label
                or arm['source'] != ('baseline' if label.startswith('A') else 'fixed')
                or type(arm['endpoint_rss_bytes']) is not list or len(arm['endpoint_rss_bytes']) != 12
                or any(type(n) is not int or n <= 0 for n in arm['endpoint_rss_bytes'])):
            raise ValueError('Invalid public arm shape')
        for key in ARM_SUMMARY_KEYS-{'label','source','endpoint_rss_bytes'}:
            n = arm[key]
            if type(n) not in (int,float) or not math.isfinite(n) or (n < 0 and key != 'warm_slope_bytes_per_pass'):
                raise ValueError('Invalid public numeric metric')


def stage_sources(destination, run_root):
    """Copy tracked source bytes only; baseline differs by one pinned app blob."""
    root = build.ROOT
    names = subprocess.check_output(['git', 'ls-files', '-z'], cwd=root, timeout=10).decode().split('\0')
    names = sorted(name for name in names if (name.endswith(('.py', '.sql'))
        or name == 'tools/ordered_native_runtime_contract_20261012.json')
        and not name.startswith('tools/render_native_build/baseline/'))
    baseline = root/'tools/render_native_build/baseline'/MODULE
    if build.native.sha_file(baseline) != BASE_SHA or build.native.sha_file(root/MODULE) != FIX_SHA:
        raise ValueError('Actual application source pins differ')
    expected = source_hashes()
    if not set(expected) <= set(names): raise ValueError('Missing measured sources')
    fixed_hashes = {}
    for name in names:
        path = root/name
        if path.is_symlink() or not path.is_file(): raise ValueError('Invalid tracked source')
        raw = path.read_bytes()
        fixed_hashes[name] = hashlib.sha256(raw).hexdigest()
        for arm in ('baseline', 'fixed'):
            target = destination/arm/name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(baseline.read_bytes() if arm == 'baseline' and name == MODULE else raw)
            target.chmod(0o400)
    for arm in ('baseline', 'fixed'):
        for name, sha in fixed_hashes.items():
            target_sha = BASE_SHA if arm == 'baseline' and name == MODULE else sha
            if build.native.sha_file(destination/arm/name) != target_sha:
                raise ValueError('Staged source drift')
    build.native.publish(run_root/'staging.json', {
        'version':VERSION, 'actual_source_comparison':True, 'only_changed_module':MODULE,
        'baseline_commit':'3c5bdb9ae5a450d1b3fb0cd4c71bab599d252ad3',
        'baseline_sha256':BASE_SHA, 'fixed_sha256':FIX_SHA,
        'fixed_file_sha256':fixed_hashes, 'order':list(ORDER),
        'rows_per_arm':2000, 'passes_per_arm':12, 'scope_parity':'postonly',
        'cursor_policy':'observed', 'gc':'enabled_unchanged', 'warm_passes':[4,12]})
    return expected


def preserve_receipts(root):
    present = []
    for name in RECEIPTS:
        path = root/name
        if not path.exists(): continue
        if any(p.is_symlink() for p in (path, *path.parents)):
            raise ValueError('Receipt symlink is forbidden')
        if path.stat().st_size > build.MAX_RECEIPT_BYTES: raise ValueError('Oversized receipt')
        present.append((name,path.read_bytes()))
    if sum(len(raw) for _,raw in present) > build.MAX_RECEIPTS_BYTES:
        raise ValueError('Oversized private evidence')
    manifest = []
    for name, raw in present:
        encoded = base64.b64encode(raw).decode('ascii')
        chunks = [encoded[i:i+2048] for i in range(0,len(encoded),2048)] or ['']
        entry = dict(path=name,bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest(),chunks=len(chunks))
        for index,chunk in enumerate(chunks):
            print('[NATIVE_BUILD_RECEIPT] '+json.dumps(dict(entry,kind='chunk',index=index,data=chunk),sort_keys=True),flush=True)
        print('[NATIVE_BUILD_RECEIPT] '+json.dumps(dict(entry,kind='file_complete'),sort_keys=True),flush=True)
        manifest.append(entry)
    print('[NATIVE_BUILD_RECEIPT] '+json.dumps(dict(kind='complete',version=VERSION,files=manifest),sort_keys=True),flush=True)
    return {entry['path']:entry['sha256'] for entry in manifest}


def publish_summary(summary):
    public = build.PUBLIC
    if public.exists() or any(p.is_symlink() for p in (public,*public.parents)):
        raise ValueError('Public directory must be new')
    if (type(summary) is not dict or set(summary) != PUBLIC_KEYS
            or summary['version'] != VERSION or summary['result'] != 'PASS'
            or summary['scope'] != 'synthetic_native_memory_abba'
            or not re.fullmatch('[0-9a-f]{40}',summary['build_commit'])
            or summary['memory_passes'] != 48 or summary['synthetic_rows'] != 2000
            or summary['positive_input_cases'] != 5
            or summary['limitations'] != LIMITATIONS
            or set(summary['receipt_sha256']) != set(RECEIPTS)
            or any(type(sha) is not str or not re.fullmatch('[0-9a-f]{64}',sha)
                   for sha in summary['receipt_sha256'].values())
            or set(summary['runtime']) != set(build.native.FIELDS)
            or any(type(summary['runtime'][key]) is not kind for key,kind in build.native.FIELDS.items())):
        raise ValueError('Invalid bounded public summary')
    # Restrict strings to known contract/runtime values and an ISO timestamp.
    contract = json.loads(build.CONTRACT.read_text())
    for key,value in contract['expected'].items():
        if summary['runtime'].get(key) != value: raise ValueError('Unexpected public runtime')
    if (summary['runtime']['libpq_version'] != 170000 or summary['runtime']['pq_implementation'] != 'binary'
            or summary['runtime']['machine'] != 'x86_64'
            or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?\+00:00',summary['checked_at_utc'])):
        raise ValueError('Unexpected public metadata')
    validate_public_comparison(summary['comparison'])
    raw = json.dumps(summary,sort_keys=True,indent=2,allow_nan=False)+'\n'
    if len(raw.encode()) > 32768: raise ValueError('Public summary is too large')
    pending = public.with_name(public.name+'.pending')
    pending.mkdir(mode=0o700)
    (pending/'summary.json').write_text(raw)
    (pending/'index.html').write_text('<!doctype html><meta charset="utf-8">'
        '<title>Isolated native memory comparison</title><h1>Synthetic ABBA comparison</h1>'
        '<p>Engineering evidence only. No production OOM resolution claim.</p><pre>'+html.escape(raw)+'</pre>')
    pending.rename(public)


def main():
    stage,commit = 'setup',None
    def interrupted(_signum,_frame): raise build.native.RunnerInterrupted('Build interrupted')
    previous = {sig:signal.signal(sig,interrupted) for sig in (signal.SIGINT,signal.SIGTERM)}
    try:
        if build.PUBLIC.exists() or any(key.startswith('PG') for key in os.environ):
            raise ValueError('Existing public output or ambient PG configuration')
        if sys.version_info[:3] != (3,11,9) or os.geteuid() == 0:
            raise ValueError('Expected non-root CPython3.11.9')
        commit = subprocess.check_output(['git','rev-parse','HEAD'],cwd=build.ROOT,text=True,timeout=10).strip()
        if not re.fullmatch('[0-9a-f]{40}',commit): raise ValueError('Missing build identity')
        builder = Path(__file__).with_name('build_postgres.py')
        if build.native.sha_file(builder) != build.BUILDER_SHA256: raise ValueError('Builder provenance mismatch')
        stage = 'dependencies'
        build.run_step([sys.executable,'-m','pip','install','--disable-pip-version-check',
            'psycopg[binary]==3.2.3','requests==2.32.3'],180)
        stage = 'postgres_build'
        build.run_step([sys.executable,str(builder)],2400)
        with tempfile.TemporaryDirectory(prefix='nm-',dir='/tmp') as temporary:
            top = Path(temporary)
            run = top/'run'
            run.mkdir(mode=0o700)
            args = type('Arguments',(),dict(python=sys.executable,
                pg_bin=str(build.ROOT/'.render_preflight_pg/bin'),contract=str(build.CONTRACT)))()
            accepted = False
            try:
                stage = 'correctness_gate'
                bindings = build.native.source_bindings(args)
                command = [sys.executable,'-I',str(build.ROOT/'tools/run_ordered_native_postgres.py'),
                    '--python',args.python,'--pg-bin',args.pg_bin,'--contract',args.contract]
                build.run_step(command+['--output',str(run/'correctness_gate')],360)
                correctness = build.validate_run(run/'correctness_gate',bindings)
                expected = stage_sources(top/'src',run)
                (run/'arms').mkdir()
                for label in ORDER:
                    arm = 'baseline' if label.startswith('A') else 'fixed'
                    stage = 'memory_'+label
                    print('[NATIVE_MEMORY_PROGRESS] '+json.dumps(dict(stage=stage,rows=2000,passes=12)),flush=True)
                    build.run_step([sys.executable,'-I',str(top/'src'/arm/'tools/run_ordered_native_postgres.py'),
                        '--python',args.python,'--pg-bin',args.pg_bin,'--contract',args.contract,
                        '--output',str(run/'arms'/label),'--memory-arm',arm],1500)
                stage = 'comparison'
                comparison = validate_comparison([run/'arms'/label for label in ORDER],expected_sources=expected)
                build.native.publish(run/'comparison.json',comparison)
                accepted = True
            finally:
                receipt_hashes = preserve_receipts(run)
                if not accepted:
                    build.preserve_failure_logs(run/'correctness_gate','correctness_gate/')
                    for label in ORDER: build.preserve_failure_logs(run/'arms'/label,'arms/'+label+'/')
            stage = 'publication'
            # The validator's public summary is a closed synthetic numeric schema.
            summary = dict(version=VERSION,result='PASS',scope='synthetic_native_memory_abba',
                build_commit=commit,checked_at_utc=datetime.now(timezone.utc).isoformat(),
                synthetic_rows=2000,memory_passes=48,positive_input_cases=5,
                runtime=correctness['runtime'],comparison=comparison['summary'],
                receipt_sha256=receipt_hashes,limitations=LIMITATIONS)
            publish_summary(summary)
            print('[NATIVE_BUILD_SUMMARY] '+json.dumps(summary,sort_keys=True),flush=True)
        return 0
    except (Exception,build.native.RunnerInterrupted) as error:
        print('[NATIVE_BUILD_SUMMARY] '+json.dumps(dict(version=VERSION,result='FAIL',stage=stage,
            build_commit=commit,error_type=type(error).__name__,error=str(error)[:250]),sort_keys=True),flush=True)
        return 1
    finally:
        for sig in previous: signal.signal(sig,signal.SIG_IGN)
        for proc in list(build.native.OWNED_PROCESSES.values()): build.native.stop_owned(proc,timeout=75)
        for sig,handler in previous.items(): signal.signal(sig,handler)


if __name__ == '__main__':
    raise SystemExit(main())
