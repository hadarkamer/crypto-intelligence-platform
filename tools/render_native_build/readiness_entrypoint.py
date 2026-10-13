"""Two actual source versions; native correctness/recovery only, no memory run."""
from __future__ import annotations

import copy
from datetime import datetime, timezone
import html
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile

import entrypoint as build
import memory_entrypoint as transport
from ordered_postgres_memory_replay import source_hashes

VERSION = 'ordered-native-release-readiness-v1'
ARMS = ('baseline', 'fixed')
RECEIPTS = tuple('arms/'+arm+'/'+name for arm in ARMS for name in build.RECEIPTS) + (
    'staging.json', 'comparison.json')
LIMITATIONS = ['Synthetic correctness and recovery only; no production deployment.',
    'No new memory benchmark or long-duration production efficacy claim.',
    'Production loaded driver/libpq equivalence remains unverified.']
PUBLIC_KEYS = {'version','result','scope','build_commit','checked_at_utc','arms',
    'synthetic_rows_per_arm','positive_cases_per_arm','overflow_cases_per_arm',
    'memory_passes','full_results_equal','recovery_verified','runtime','receipt_sha256','limitations'}


def compare(run, bindings):
    details, proofs = {}, {}
    for arm in ARMS:
        expected = copy.deepcopy(bindings)
        expected['actual_sources'][transport.MODULE] = (
            transport.BASE_SHA if arm == 'baseline' else transport.FIX_SHA)
        folder = run/'arms'/arm
        details[arm] = build.validate_run(folder, expected)
        proofs[arm] = json.loads((folder/'correctness/correctness_receipt.json').read_text())
        if proofs[arm].get('source_arm') != arm:
            raise ValueError('Unexpected actual source variant')
    a, b = (proofs[arm] for arm in ARMS)
    if (a['full_scope_result'] != b['full_scope_result']
            or a['source_semantics_audit'] != b['source_semantics_audit']
            or a['range_audit']['reference_sha256'] != b['range_audit']['reference_sha256']
            or details['baseline']['runtime'] != details['fixed']['runtime']):
        raise ValueError('Original and changed source results differ')
    # Every arm is verified independently above; compare error semantics while
    # excluding process identities, which must differ between owned clusters.
    fields = ('case','input_decimal','sqlstate','error_class','error_transaction_state',
              'after_rollback_state','final_transaction_state','full_result_equal')
    for left, right in zip(a['range_audit']['cases'], b['range_audit']['cases']):
        if {key:left[key] for key in fields} != {key:right[key] for key in fields}:
            raise ValueError('Original and changed source error semantics differ')
    return dict(version=VERSION, result='PASS', arms=list(ARMS),
        full_results_equal=True, recovery_verified=True,
        source_sha256={arm:proofs[arm]['source_hashes'][transport.MODULE] for arm in ARMS},
        runtime=details['fixed']['runtime'])


def publish_summary(summary):
    if (type(summary) is not dict or set(summary) != PUBLIC_KEYS
            or summary['version'] != VERSION or summary['result'] != 'PASS'
            or summary['scope'] != 'synthetic_native_correctness_and_recovery'
            or summary['arms'] != list(ARMS) or summary['synthetic_rows_per_arm'] != 32
            or summary['positive_cases_per_arm'] != 5 or summary['overflow_cases_per_arm'] != 2
            or summary['memory_passes'] != 0 or summary['full_results_equal'] is not True
            or summary['recovery_verified'] is not True or summary['limitations'] != LIMITATIONS
            or not re.fullmatch('[0-9a-f]{40}', summary['build_commit'])
            or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?\+00:00', summary['checked_at_utc'])
            or set(summary['receipt_sha256']) != set(RECEIPTS)
            or any(not re.fullmatch('[0-9a-f]{64}', sha) for sha in summary['receipt_sha256'].values())
            or set(summary['runtime']) != set(build.native.FIELDS)
            or any(type(summary['runtime'][key]) is not kind for key,kind in build.native.FIELDS.items())):
        raise ValueError('Invalid closed public summary')
    expected = dict(json.loads(build.CONTRACT.read_text())['expected'], libpq_version=170000,
                    pq_implementation='binary', machine='x86_64')
    if any(summary['runtime'].get(key) != value for key,value in expected.items()):
        raise ValueError('Unexpected public runtime')
    public = build.PUBLIC
    if public.exists() or any(p.is_symlink() for p in (public,*public.parents)):
        raise ValueError('Public directory must be new')
    raw = json.dumps(summary,sort_keys=True,indent=2,allow_nan=False)+'\n'
    if len(raw.encode()) > 16384: raise ValueError('Oversized public summary')
    pending = public.with_name(public.name+'.pending')
    pending.mkdir(mode=0o700)
    (pending/'summary.json').write_text(raw)
    (pending/'index.html').write_text('<!doctype html><meta charset="utf-8">'
        '<title>Native correctness and recovery</title><h1>Synthetic checks: PASS</h1>'
        '<p>No production deployment or OOM-resolution claim.</p><pre>'+html.escape(raw)+'</pre>')
    pending.rename(public)


def main():
    stage, commit = 'setup', None
    def interrupted(_signum,_frame): raise build.native.RunnerInterrupted('Build interrupted')
    previous = {sig:signal.signal(sig,interrupted) for sig in (signal.SIGINT,signal.SIGTERM)}
    try:
        if build.PUBLIC.exists() or any(key.startswith('PG') for key in os.environ):
            raise ValueError('Existing public output or ambient PostgreSQL configuration')
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
        with tempfile.TemporaryDirectory(prefix='nr-',dir='/tmp') as temporary:
            top = Path(temporary)
            run = top/'run'
            run.mkdir(mode=0o700)
            args = type('Arguments',(),dict(python=sys.executable,
                pg_bin=str(build.ROOT/'.render_preflight_pg/bin'),contract=str(build.CONTRACT)))()
            accepted = False
            try:
                stage = 'staging'
                bindings = build.native.source_bindings(args)
                transport.stage_sources(top/'src',run,expected_sources=source_hashes(),protocol={
                    'version':VERSION,'order':list(ARMS),'rows_per_arm':32,'memory_passes':0,
                    'positive_cases_per_arm':5,'overflow_cases_per_arm':2})
                (run/'arms').mkdir()
                for arm in ARMS:
                    stage = 'correctness_'+arm
                    print('[NATIVE_READINESS_PROGRESS] '+json.dumps(dict(stage=stage)),flush=True)
                    build.run_step([sys.executable,'-I',str(top/'src'/arm/'tools/run_ordered_native_postgres.py'),
                        '--python',args.python,'--pg-bin',args.pg_bin,'--contract',args.contract,
                        '--output',str(run/'arms'/arm)],360)
                stage = 'comparison'
                comparison = compare(run,bindings)
                build.native.publish(run/'comparison.json',comparison)
                accepted = True
            finally:
                hashes = transport.preserve_receipts(run,receipts=RECEIPTS,version=VERSION)
                if not accepted:
                    for arm in ARMS: build.preserve_failure_logs(run/'arms'/arm,'arms/'+arm+'/')
            stage = 'publication'
            summary = dict(version=VERSION,result='PASS',scope='synthetic_native_correctness_and_recovery',
                build_commit=commit,checked_at_utc=datetime.now(timezone.utc).isoformat(),arms=list(ARMS),
                synthetic_rows_per_arm=32,positive_cases_per_arm=5,overflow_cases_per_arm=2,
                memory_passes=0,full_results_equal=True,recovery_verified=True,
                runtime=comparison['runtime'],receipt_sha256=hashes,limitations=LIMITATIONS)
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
