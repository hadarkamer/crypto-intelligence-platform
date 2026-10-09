"""Pinned isolated Render build verification; no runtime or deployment hooks.

The build obtains a verified PostgreSQL distribution separately. This wrapper
checks an allowlisted two-checkout manifest, runs isolated test children with a
clean environment and disposable loopback databases, and requires a completely
passing, zero-skip result plus verified PostgreSQL shutdown. It then compares
4,000 g65 paths in a separate guarded, database-free child.

Only a generic status page is written to public/. Source, private test logs,
reports, accounts, endpoints and infrastructure identifiers are never published.
The sole printed object contains fixed statuses, integer counts and SHA256s.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile

from . import experimental_isolated_verify as isolated
from .render_preflight import core

SCHEMA = 'experimental_render_verification_v1'
BUNDLE_SCHEMA = 'experimental_render_bundle_v1'
PIN_ENV = 'EXPERIMENTAL_BUNDLE_MANIFEST_SHA256'
BUILD_ONLY_ENV = 'EXPERIMENTAL_RENDER_BUILD_ONLY'
BUILD_ONLY_EXIT = 42
MANIFEST_NAME = 'bundle-manifest.json'
MAX_MANIFEST_BYTES = 2*1024*1024
MAX_PARITY_BYTES = 16384
MAX_SUMMARY_BYTES = 16384
HEX = re.compile(r'[0-9a-f]{64}\Z')
COUNTS = ('tests_run','passed','skipped','failures','errors','unexpected_successes','network_attempts_blocked')
PUBLIC = {
    status: '<!doctype html><html lang="en"><meta charset="utf-8"><title>Isolated verification</title><body><p>' + text + '</p></body></html>\n'
    for status,text in (
        ('RUNNING','Isolated verification is running.'),
        ('PASSED','Isolated verification completed.'),
        ('FAILED','Isolated verification did not complete successfully.'))
}
FAILURES = frozenset((
    'BUNDLE_ROOT_REQUIRED','BUNDLE_MANIFEST_PIN_REQUIRED','BUNDLE_MANIFEST_INVALID',
    'BUNDLE_MANIFEST_HASH_MISMATCH','BUNDLE_MANIFEST_SCHEMA_MISMATCH','BUNDLE_FILES_CHANGED',
    'BUNDLE_SYMLINK_REFUSED','BUNDLE_SOURCE_LAYOUT_INVALID','VERIFICATION_DID_NOT_PASS',
    'VERIFICATION_SUITES_REQUIRED','VERIFICATION_COUNTS_INVALID','VERIFICATION_SKIPS_OR_FAILURES',
    'VERIFICATION_POSTGRES_NOT_PROVEN','VERIFICATION_CANDIDATE_CHANGED','VERIFICATION_HASHES_CHANGED',
    'PARITY_DID_NOT_PASS','PARITY_REPORT_INVALID','PUBLIC_DIRECTORY_NOT_ISOLATED',
    'PUBLIC_STATUS_WRITE_FAILED','RENDER_VERIFICATION_FAILED','SUMMARY_TOO_LARGE'))


class RenderVerificationError(ValueError):
    """Fixed diagnostic codes; never raw infrastructure details."""


def _fail(code):
    raise RenderVerificationError(code)


def _read_json(path, maximum, code):
    if path.is_symlink() or not path.is_file() or not 0<path.stat().st_size<=maximum:
        _fail(code)
    raw=path.read_bytes()
    if len(raw)>maximum:
        _fail(code)
    def pairs(rows):
        result={}
        for key,value in rows:
            if key in result:_fail(code)
            result[key]=value
        return result
    try:
        value=json.loads(raw,object_pairs_hook=pairs,parse_constant=lambda _: _fail(code))
    except (ValueError,TypeError,UnicodeError,RecursionError):
        _fail(code)
    if not isinstance(value,dict):_fail(code)
    return value,raw


def _safe_file_map(value):
    if not isinstance(value,dict) or not 1<=len(value)<=20000:
        _fail('BUNDLE_MANIFEST_INVALID')
    for name,digest in value.items():
        if (not isinstance(name,str) or len(name)>500 or '\\' in name
                or Path(name).is_absolute() or not name or any(p in ('','.', '..') or p.startswith('.') for p in name.split('/'))
                or not isinstance(digest,str) or not HEX.fullmatch(digest)):
            _fail('BUNDLE_MANIFEST_INVALID')
    return value


def validate_bundle(bundle_root, expected_hash):
    """Pin exact source/requirements/fixtures before any candidate test import."""
    root=Path(bundle_root)
    if root.is_symlink() or not root.is_dir():_fail('BUNDLE_ROOT_REQUIRED')
    root=root.resolve()
    if not isinstance(expected_hash,str) or not HEX.fullmatch(expected_hash):
        _fail('BUNDLE_MANIFEST_PIN_REQUIRED')
    value,raw=_read_json(root/MANIFEST_NAME,MAX_MANIFEST_BYTES,'BUNDLE_MANIFEST_INVALID')
    if hashlib.sha256(raw).hexdigest()!=expected_hash:
        _fail('BUNDLE_MANIFEST_HASH_MISMATCH')
    if value.get('schema')!=BUNDLE_SCHEMA:_fail('BUNDLE_MANIFEST_SCHEMA_MISMATCH')
    candidates=value.get('candidate_files')
    if not isinstance(candidates,dict) or set(candidates)!={'receiver','producer'}:
        _fail('BUNDLE_SOURCE_LAYOUT_INVALID')
    fingerprints={}
    for name in ('receiver','producer'):
        scope=root/name
        if scope.is_symlink() or not scope.is_dir():_fail('BUNDLE_SOURCE_LAYOUT_INVALID')
        expected=_safe_file_map(candidates[name])
        try:actual=isolated.candidate_files(scope)
        except core.PreflightError as error:
            _fail('BUNDLE_SYMLINK_REFUSED' if str(error)=='MANIFEST_SYMLINK_REFUSED' else 'BUNDLE_FILES_CHANGED')
        if actual!=expected:_fail('BUNDLE_FILES_CHANGED')
        fingerprints[name]=isolated.file_digest(actual)
    return dict(manifest_sha256=expected_hash,candidate_files=candidates,candidate_sha256=fingerprints)


def _integer(value):
    if type(value) is not int or not 0<=value<=10000000:
        _fail('VERIFICATION_COUNTS_INVALID')
    return value


def safe_diagnostics(report):
    """Bounded test identifiers/counts only; never messages, tracebacks or paths."""
    if not isinstance(report,dict):return {}
    statuses={
        'postgresql':{'FRESH_LOOPBACK_POSTGRES18','NOT_REQUESTED','NOT_STARTED',
            'NOT_RUN_POSTGRES_REQUIRES_NONROOT','NOT_RUN_VERIFIED_POSTGRES186_UNAVAILABLE'},
        'producer_postgresql':{'FRESH_LOOPBACK_PRODUCER_DATABASE','NOT_CONFIGURED_BY_THIS_RUNNER'},
        'postgresql_cleanup':{'STOPPED','NOT_STARTED','FAILED','NOT_REQUESTED'}}
    result={key:report.get(key) if report.get(key) in allowed else 'UNVERIFIED' for key,allowed in statuses.items()}
    prefixes=('hl_testnet_runtime.','xrp_r2732_','hype_row71205_','sol_g65_','sol_proximity_',
        'maxpain_','alert_cards_','experimental_execution_','hyperliquid_testnet_executor_selftest.','unittest.loader.')
    exception_types={'AssertionError','RuntimeError','ValueError','TypeError','KeyError','ImportError',
        'ModuleNotFoundError','AttributeError','OSError','TimeoutError','OperationalError','DatabaseError',
        'InterfaceError','ProgrammingError','IntegrityError','UniqueViolation','InvalidTextRepresentation',
        'UndefinedTable','UndefinedColumn','InFailedSqlTransaction','SerializationFailure','DeadlockDetected',
        'LockNotAvailable','JournalError','DispatchError','PlanStoreError','ContractError','BoundaryError','StateError'}
    details_left=10
    stages=report.get('stages',[])
    if not isinstance(stages,list):return result
    result['stages']={}
    for row in stages[:2]:
        if not isinstance(row,dict) or row.get('suite') not in ('runtime','producer'):continue
        value={key:row[key] for key in COUNTS if type(row.get(key)) is int and 0<=row[key]<=10000000}
        value['exit_code']=row.get('exit_code') if type(row.get('exit_code')) is int and -255<=row['exit_code']<=255 else None
        value['postgresql_configured']=row.get('postgresql_configured') is True
        details=row.get('failure_details',[])
        if isinstance(details,list):
            value['failure_details']=[]
            for detail in details[:details_left]:
                if not isinstance(detail,dict):continue
                name=detail.get('test_id')
                valid=(isinstance(name,str) and len(name)<=300 and re.fullmatch(r'[A-Za-z0-9_.]+',name) and name.startswith(prefixes))
                value['failure_details'].append(dict(test_id=name if valid else 'unidentified_test',
                    exception_type=detail.get('exception_type') if detail.get('exception_type') in exception_types else 'OtherError'))
                details_left-=1
        result['stages'][row['suite']]=value
    return result


def checked_verification(report, manifest):
    """Do not turn skipped/partial or failed PostgreSQL cleanup into a pass."""
    if not isinstance(report,dict) or report.get('status')!='PASSED':
        _fail('VERIFICATION_DID_NOT_PASS')
    if (report.get('postgresql')!='FRESH_LOOPBACK_POSTGRES18'
            or report.get('producer_postgresql')!='FRESH_LOOPBACK_PRODUCER_DATABASE'
            or report.get('postgresql_cleanup')!='STOPPED'):
        _fail('VERIFICATION_POSTGRES_NOT_PROVEN')
    if (report.get('snapshot_unchanged_after_tests') is not True
            or report.get('workspace_unchanged_since_snapshot') is not True):
        _fail('VERIFICATION_CANDIDATE_CHANGED')
    for name in ('receiver','producer'):
        candidate=report.get('candidates',{}).get(name,{})
        if (candidate.get('files')!=manifest['candidate_files'][name]
                or candidate.get('sha256')!=manifest['candidate_sha256'][name]):
            _fail('VERIFICATION_HASHES_CHANGED')
    stages=report.get('stages')
    if (not isinstance(stages,list) or len(stages)!=2
            or {row.get('suite') for row in stages if isinstance(row,dict)}!={'runtime','producer'}):
        _fail('VERIFICATION_SUITES_REQUIRED')
    result={}
    for row in stages:
        counts={key:_integer(row.get(key)) for key in COUNTS}
        if (row.get('exit_code')!=0 or row.get('successful') is not True
                or row.get('postgresql_configured') is not True
                or counts['tests_run']==0 or counts['passed']!=counts['tests_run']
                or any(counts[key] for key in COUNTS if key not in ('tests_run','passed'))):
            _fail('VERIFICATION_SKIPS_OR_FAILURES')
        result[row['suite']]=counts
    return result


def run_parity(receiver,producer,output):
    """Fixed guarded child command; no Render environment or database URL."""
    log=Path(output)/'g65-parity-private.log'
    result=core.run_process([sys.executable,'-m','hl_testnet_runtime.experimental_g65_source_parity',
        '--child','--producer-root',str(producer),'--seed','65','--cases','4000'],
        cwd=receiver,env=core.clean_environment(receiver),log_path=log,timeout=240)
    if result.get('exit_code')!=0:_fail('PARITY_DID_NOT_PASS')
    report,_raw=_read_json(log,MAX_PARITY_BYTES,'PARITY_REPORT_INVALID')
    if (report.get('schema')!='g65_producer_differential_v1' or report.get('status')!='PASSED'
            or report.get('comparisons')!=4000 or report.get('bars_per_comparison')!=4
            or report.get('network_attempts_blocked')!=0 or report.get('live_exchange_requests_sent')!=0
            or report.get('decision_function_executed_unchanged') is not True
            or report.get('source_files_modified') is not False):
        _fail('PARITY_DID_NOT_PASS')
    hashes={key:report.get(key) for key in ('producer_file_sha256','producer_decision_functions_sha256','candidate_sha256')}
    if any(not isinstance(value,str) or not HEX.fullmatch(value) for value in hashes.values()):
        _fail('PARITY_REPORT_INVALID')
    # Bind comparison evidence to the actual pinned sources, not only a self-report.
    for key,path in (('producer_file_sha256',Path(producer)/'sol_g65_experimental_store.py'),
                     ('candidate_sha256',Path(receiver)/'hl_testnet_runtime/sol_g65_conditional_stop.py')):
        if hashes[key]!=hashlib.sha256(path.read_bytes()).hexdigest():_fail('PARITY_REPORT_INVALID')
    return dict(comparisons=4000,bars_per_comparison=4,network_attempts_blocked=0,**hashes)


def public_status(root,status):
    """Publish only our fixed page; refuse existing files rather than deleting."""
    directory=Path(root)/'public'
    try:
        if directory.is_symlink():_fail('PUBLIC_DIRECTORY_NOT_ISOLATED')
        if directory.exists():
            if not directory.is_dir():_fail('PUBLIC_DIRECTORY_NOT_ISOLATED')
            children=list(directory.iterdir())
            if len(children)>1 or any(p.name!='index.html' or p.is_symlink() or not p.is_file() for p in children):
                _fail('PUBLIC_DIRECTORY_NOT_ISOLATED')
            if children and children[0].read_text() not in PUBLIC.values():
                _fail('PUBLIC_DIRECTORY_NOT_ISOLATED')
        else:
            directory.mkdir(mode=0o755)
        (directory/'index.html').write_text(PUBLIC[status])
    except RenderVerificationError:raise
    except (OSError,UnicodeError):_fail('PUBLIC_STATUS_WRITE_FAILED')


def run_bundle(bundle_root,expected_hash):
    """Return only a bounded, non-sensitive build result; private files expire."""
    result=dict(schema=SCHEMA,status='FAILED',manifest_sha256=None,real_exchange_requests_sent=0)
    root=Path(bundle_root)
    try:
        manifest=validate_bundle(root,expected_hash)
        result.update(manifest_sha256=manifest['manifest_sha256'],candidate_sha256=manifest['candidate_sha256'])
        public_status(root,'RUNNING')
        with tempfile.TemporaryDirectory(prefix='experimental-render-private-') as temporary:
            private=Path(temporary)
            report=isolated.verify(root/'receiver',root/'producer',private/'verification',postgres=True)
            result['verification']=safe_diagnostics(report)
            counts=checked_verification(report,manifest)
            parity=run_parity(root/'receiver',root/'producer',private)
            final=validate_bundle(root,expected_hash)
            if final!=manifest:_fail('VERIFICATION_CANDIDATE_CHANGED')
            result.update(status='PASSED',stages=counts,parity=parity,postgresql_cleanup='STOPPED')
        public_status(root,'PASSED')
    except Exception as error:
        code=str(error) if isinstance(error,RenderVerificationError) and str(error) in FAILURES else 'RENDER_VERIFICATION_FAILED'
        result.update(status='FAILED',failure_code=code)
        if root.is_dir() and not root.is_symlink():
            try:public_status(root,'FAILED')
            except RenderVerificationError:pass
    return result


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle-root',type=Path,required=True)
    args=parser.parse_args(argv)
    report=run_bundle(args.bundle_root,os.environ.get(PIN_ENV))
    build_only=os.environ.get(BUILD_ONLY_ENV)=='1'
    if build_only:
        # A successful Render build publishes its output. This opt-in reports
        # verification success but deliberately fails the build so even the
        # generic public status page is never published.
        report.update(build_only=True,publication_blocked=True)
    raw=json.dumps(report,sort_keys=True,separators=(',',':'))
    if len(raw.encode())>MAX_SUMMARY_BYTES:
        fallback=dict(schema=SCHEMA,status='FAILED',failure_code='SUMMARY_TOO_LARGE')
        if build_only:
            fallback.update(build_only=True,publication_blocked=True)
        raw=json.dumps(fallback)
        report['status']='FAILED'
    print(raw)
    if build_only and report['status']=='PASSED':
        return BUILD_ONLY_EXIT
    return 0 if report['status']=='PASSED' else 1


if __name__=='__main__':
    raise SystemExit(main())
