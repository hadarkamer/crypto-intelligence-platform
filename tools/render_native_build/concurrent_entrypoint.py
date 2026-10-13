"""Bounded ordered-scope plus real BTC-source overlap in the isolated build."""
from __future__ import annotations

from datetime import datetime, timezone
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
import memory_entrypoint as memory
from ordered_postgres_concurrent_replay import source_hashes
from verify_ordered_native_concurrent import validate_comparison

VERSION = 'ordered-native-concurrent-build-v1'
ORDER = memory.ORDER
ARM_FILES = ('preflight.json', 'native_lifecycle.json', 'runner_cleanup.json',
             'host/connection.json', 'concurrent/evidence.zip', 'concurrent/sealed.json')
RECEIPTS = tuple('correctness_gate/'+name for name in build.RECEIPTS) + tuple(
    'arms/'+arm+'/'+name for arm in ORDER for name in ARM_FILES) + ('comparison.json','staging.json')
LIMITATIONS = [
    'Synthetic overlap of one ordered scope and one real BTC source-loading/digest path.',
    'Two persistent client threads and separate connections in one process; 2000 events per lane.',
    'Four fresh processes and clusters in ABBA order; repeated rounds within each process are correlated.',
    'Both-loaded RSS is process-wide at a coordinated overlap, not a continuous peak or per-worker allocation.',
    'Normal GC without forced collection or allocator trimming; observer and gate costs remain in both arms.',
    'Excludes full worker scheduling, pricing, ACK/outbox, transaction execution and production deployment.',
    'Production loaded driver/libpq equivalence and resolution of production OOM remain unverified.',
]
PUBLIC_KEYS = {'version','result','scope','build_commit','checked_at_utc','rows_per_lane',
               'rounds','task_completions','positive_input_cases','runtime','comparison',
               'receipt_sha256','limitations'}
ARM_KEYS = {'label','source','both_loaded_rss_bytes','round_end_rss_bytes',
    'warm_both_loaded_median_rss_bytes','warm_both_loaded_min_rss_bytes','warm_both_loaded_max_rss_bytes',
    'warm_round_end_median_rss_bytes','warm_round_end_min_rss_bytes','warm_round_end_max_rss_bytes',
    'warm_round_end_slope_bytes_per_round','sampled_max_rss_bytes','lifetime_maxrss_bytes',
    'total_elapsed_ns','total_cpu_ns','total_observer_ns'}


def validate_public_comparison(value):
    if (type(value) is not dict or set(value) != {'order','rows_per_lane','rounds_per_arm','lanes','warm_rounds','arms'}
            or value['order'] != list(ORDER) or value['rows_per_lane'] != 2000
            or value['rounds_per_arm'] != 12 or value['lanes'] != 2 or value['warm_rounds'] != [4,12]
            or type(value['arms']) is not list or len(value['arms']) != 4):
        raise ValueError('Invalid public concurrent comparison')
    for label, arm in zip(ORDER,value['arms']):
        if (type(arm) is not dict or set(arm) != ARM_KEYS or arm['label'] != label
                or arm['source'] != ('baseline' if label.startswith('A') else 'fixed')):
            raise ValueError('Invalid public arm identity')
        for key in ('both_loaded_rss_bytes','round_end_rss_bytes'):
            if (type(arm[key]) is not list or len(arm[key]) != 12
                    or any(type(n) is not int or n <= 0 for n in arm[key])):
                raise ValueError('Invalid public RSS series')
        for key in ARM_KEYS-{'label','source','both_loaded_rss_bytes','round_end_rss_bytes'}:
            n = arm[key]
            if type(n) not in (int,float) or not math.isfinite(n) or (n < 0 and key != 'warm_round_end_slope_bytes_per_round'):
                raise ValueError('Invalid public numeric metric')


def publish_summary(summary):
    if build.PUBLIC.exists() or any(p.is_symlink() for p in (build.PUBLIC,*build.PUBLIC.parents)):
        raise ValueError('Public directory must be new')
    if (type(summary) is not dict or set(summary) != PUBLIC_KEYS
            or summary['version'] != VERSION or summary['result'] != 'PASS'
            or summary['scope'] != 'synthetic_native_concurrent_abba'
            or not re.fullmatch('[0-9a-f]{40}',summary['build_commit'])
            or summary['rows_per_lane'] != 2000 or summary['rounds'] != 48
            or summary['task_completions'] != 96 or summary['positive_input_cases'] != 5
            or summary['limitations'] != LIMITATIONS
            or set(summary['receipt_sha256']) != set(RECEIPTS)
            or any(type(sha) is not str or not re.fullmatch('[0-9a-f]{64}',sha) for sha in summary['receipt_sha256'].values())
            or set(summary['runtime']) != set(build.native.FIELDS)
            or any(type(summary['runtime'][key]) is not kind for key,kind in build.native.FIELDS.items())):
        raise ValueError('Invalid bounded public summary')
    contract = json.loads(build.CONTRACT.read_text())
    if (any(summary['runtime'].get(key) != value for key,value in contract['expected'].items())
            or summary['runtime']['libpq_version'] != 170000
            or summary['runtime']['pq_implementation'] != 'binary' or summary['runtime']['machine'] != 'x86_64'
            or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?\+00:00',summary['checked_at_utc'])):
        raise ValueError('Unexpected public runtime/metadata')
    validate_public_comparison(summary['comparison'])
    raw = json.dumps(summary,sort_keys=True,indent=2,allow_nan=False)+'\n'
    if len(raw.encode()) > 32768: raise ValueError('Public summary too large')
    pending = build.PUBLIC.with_name(build.PUBLIC.name+'.pending')
    pending.mkdir(mode=0o700)
    (pending/'summary.json').write_text(raw)
    (pending/'index.html').write_text('<!doctype html><meta charset="utf-8">'
        '<title>Isolated concurrent memory comparison</title><h1>Synthetic concurrent ABBA comparison</h1>'
        '<p>Engineering evidence only. No production deployment or OOM resolution claim.</p><pre>'+html.escape(raw)+'</pre>')
    pending.rename(build.PUBLIC)


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
        with tempfile.TemporaryDirectory(prefix='nc-',dir='/tmp') as temporary:
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
                stage = 'concurrency_failure_controls'
                build.run_step([sys.executable,'-B',str(build.ROOT/'tools/ordered_postgres_concurrent_replay.py'),
                    '--selftest-gates'],30)
                expected = source_hashes()
                memory.stage_sources(top/'src',run,expected_sources=expected,protocol={
                    'version':VERSION,'rows_per_lane':2000,'rounds_per_arm':12,'lanes':2,
                    'warm_rounds':[4,12],'topology':'ordered_scope_and_btc_source',
                    'primary_metric':'warm_both_loaded_median_rss_bytes'})
                (run/'arms').mkdir()
                for label in ORDER:
                    arm = 'baseline' if label.startswith('A') else 'fixed'
                    stage = 'concurrent_'+label
                    print('[NATIVE_CONCURRENT_PROGRESS] '+json.dumps(dict(stage=stage,rows_per_lane=2000,rounds=12)),flush=True)
                    build.run_step([sys.executable,'-I',str(top/'src'/arm/'tools/run_ordered_native_postgres.py'),
                        '--python',args.python,'--pg-bin',args.pg_bin,'--contract',args.contract,
                        '--output',str(run/'arms'/label),'--concurrent-arm',arm],1500)
                stage = 'comparison'
                comparison = validate_comparison([run/'arms'/label for label in ORDER],expected_sources=expected)
                build.native.publish(run/'comparison.json',comparison)
                accepted = True
            finally:
                receipt_hashes = memory.preserve_receipts(run,receipts=RECEIPTS,version=VERSION)
                if not accepted:
                    build.preserve_failure_logs(run/'correctness_gate','correctness_gate/')
                    for label in ORDER: build.preserve_failure_logs(run/'arms'/label,'arms/'+label+'/')
            stage = 'publication'
            summary = dict(version=VERSION,result='PASS',scope='synthetic_native_concurrent_abba',
                build_commit=commit,checked_at_utc=datetime.now(timezone.utc).isoformat(),
                rows_per_lane=2000,rounds=48,task_completions=96,positive_input_cases=5,
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
