"""Safe diagnostics for the app-owned collector. No raw errors, secrets or profiles.

Each paid job is attempted once. This module never retries or contacts a source.
The child writes a small fixed-schema error record which its parent can retain
as the job's failure code before deleting the temporary execution directory.
"""
from __future__ import annotations
import json
from pathlib import Path
import time

CODES = frozenset({
    'source_capture_failed','source_timeout','source_not_readable',
    'screenshot_identity_mismatch','image_evidence_invalid','analysis_timeout',
    'analysis_incomplete','analysis_response_invalid','analysis_unauthorized',
    'analysis_rate_limited','analysis_quota_exceeded','analysis_http_error',
    'price_uncertain','zones_invalid','worker_crashed',
})
STAGES = frozenset({'startup','capture','image_check','analysis','validation','saving'})
_current_stage = 'startup'
_usage = None
_analysis_facts = {}

class StageFailure(RuntimeError):
    def __init__(self, code):
        self.code = code if code in CODES else 'worker_crashed'
        super().__init__(self.code)

def stage(value):
    global _current_stage
    _current_stage = value if value in STAGES else 'startup'

def numeric_usage(raw):
    if not isinstance(raw, dict):
        return None
    return {key: value for key, value in raw.items()
            if key in {'input_tokens','output_tokens','total_tokens'}
            and type(value) is int and 0 <= value <= 2_000_000}

def accept_model_response(response):
    """Check the API envelope before parsing model text. Never expose its body."""
    global _usage
    if not response.ok:
        status = response.status_code
        code = {401:'analysis_unauthorized',403:'analysis_unauthorized',
                429:'analysis_rate_limited'}.get(status,'analysis_http_error')
        if status == 429:
            try:
                if response.json().get('error',{}).get('code') == 'insufficient_quota':
                    code = 'analysis_quota_exceeded'
            except Exception:
                pass
        raise StageFailure(code)
    try:
        payload = response.json()
    except Exception:
        raise StageFailure('analysis_response_invalid') from None
    if not isinstance(payload, dict):
        raise StageFailure('analysis_response_invalid')
    _usage = numeric_usage(payload.get('usage'))
    if payload.get('status') == 'incomplete':
        raise StageFailure('analysis_incomplete')
    if payload.get('status') != 'completed':
        raise StageFailure('analysis_response_invalid')
    return payload

def remember_analysis(raw):
    """Retain only enumerations, counts and booleans for a failed validation."""
    global _usage, _analysis_facts
    if not isinstance(raw, dict):
        return
    _usage = numeric_usage(raw.get('usage')) or _usage
    scans = raw.get('scans')
    if not isinstance(scans,list) or len(scans)!=1 or not isinstance(scans[0],dict):
        return
    scan=scans[0]
    facts={}
    for key, allowed in {
        'observed_timeframe':{'12h','24h','48h','unknown'},
        'current_price_confidence':{'high','medium','low'},
        'blocking_condition':{'none','loading','login','blur','challenge','unknown'},
    }.items():
        value=scan.get(key)
        if isinstance(value,str) and value in allowed:
            facts[key]=value
    if type(scan.get('readable')) is bool:
        facts['readable']=scan['readable']
    _analysis_facts=facts

def classify(exc):
    if isinstance(exc,StageFailure):
        return exc.code
    timeout = type(exc).__name__ in {'TimeoutError','Timeout','ReadTimeout','ConnectTimeout'}
    if _current_stage=='capture':
        return 'source_timeout' if timeout else 'source_capture_failed'
    if _current_stage=='analysis':
        return 'analysis_timeout' if timeout else 'analysis_response_invalid'
    if _current_stage=='image_check':
        return 'image_evidence_invalid'
    if _current_stage=='validation':
        # Match our own fixed validator messages only, never echo arbitrary text.
        if str(exc)=='Current price is not readable with sufficient confidence':
            return 'price_uncertain'
        return 'zones_invalid'
    return 'worker_crashed'

def run_task(main, timeframe, job_id, output):
    global _current_stage, _usage, _analysis_facts
    _current_stage='startup'; _usage=None; _analysis_facts={}
    started=time.monotonic()
    root=Path(output)
    try:
        main(timeframe,job_id,output)
        return 0
    except Exception as exc:
        report={'code':classify(exc),'stage':_current_stage,
                'elapsed_seconds':round(time.monotonic()-started,2),
                'usage':_usage, **_analysis_facts}
        root.mkdir(parents=True,exist_ok=True)
        path=root/'error.json'
        path.write_text(json.dumps(report),encoding='utf-8')
        path.chmod(0o600)
        return 1

def read_failure(path, job_id, returncode):
    """Parent-side strict allowlist. GET/poll never starts a new paid attempt."""
    report={'code':'worker_crashed','stage':'startup'}
    try:
        path=Path(path)
        if path.is_file() and not path.is_symlink() and path.stat().st_size<=4096:
            raw=json.loads(path.read_text())
            if isinstance(raw,dict):
                if raw.get('code') in CODES: report['code']=raw['code']
                if raw.get('stage') in STAGES: report['stage']=raw['stage']
                if type(raw.get('elapsed_seconds')) in (int,float) and 0<=raw['elapsed_seconds']<=360:
                    report['elapsed_seconds']=raw['elapsed_seconds']
                report['usage']=numeric_usage(raw.get('usage'))
                for key,allowed in {'observed_timeframe':{'12h','24h','48h','unknown'},
                    'current_price_confidence':{'high','medium','low'},
                    'blocking_condition':{'none','loading','login','blur','challenge','unknown'}}.items():
                    val=raw.get(key)
                    if isinstance(val,str) and val in allowed: report[key]=val
                if type(raw.get('readable')) is bool: report['readable']=raw['readable']
    except Exception:
        pass
    print('MODEL1_JOB_FAILURE '+json.dumps({'job_id':job_id,'exit_code':returncode,**report}),flush=True)
    return report['code']
