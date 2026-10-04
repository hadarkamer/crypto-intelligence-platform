"""Safe execution diagnostics and small-worker resource control.

Never logs raw browser/API errors, response bodies, cookies or profiles.
Only unrelated advertising/analytics hosts are excluded; CoinGlass, its assets,
authentication and challenge providers are not intercepted or bypassed.
"""
from __future__ import annotations
from contextlib import contextmanager
import json
import math
from pathlib import Path
import re
import time

CODES=frozenset({'source_capture_failed','source_timeout','source_not_readable',
    'screenshot_identity_mismatch','image_evidence_invalid','analysis_timeout',
    'analysis_incomplete','analysis_response_invalid','analysis_unauthorized',
    'analysis_rate_limited','analysis_quota_exceeded','analysis_http_error',
    'price_uncertain','zones_invalid','worker_crashed'})
STAGES=frozenset({'startup','capture','image_check','analysis','validation','saving'})
PHASES=frozenset({'capture','select_12h','select_24h','select_48h','model_selection',
    'symbol_selection','wait_for_chart','screenshot','navigation','browser_launch',
    'browser_context','source_session_setup','page_creation','overlay_dismissal',
    'timeframe_selection','legend_preparation','chart_geometry','render_settle',
    'context_close','browser_close','render_readiness'})
KINDS=frozenset({'TimeoutError','TargetClosedError','Error','RuntimeError','ValueError',
    'MemoryError','OSError','FileNotFoundError','ReadTimeout','ConnectTimeout'})
AD_HOSTS=re.compile(r'^https?://(?:[a-zA-Z0-9-]+\.)*(?:doubleclick\.net|googlesyndication\.com|google-analytics\.com|googletagmanager\.com)/')
_current_stage='startup'
_usage=None
_analysis_facts={}
_capture_events=[]

@contextmanager
def capture_operation(phase):
    """Observe one existing operation; never change its call, timeout or pixels."""
    if phase not in PHASES:raise ValueError('Unknown capture phase')
    started=time.monotonic();status='completed'
    try:
        yield
    except Exception as exc:
        status='failed'
        if not hasattr(exc,'_model1_capture_phase'):exc._model1_capture_phase=phase
        raise
    finally:
        _capture_events.append({'phase':phase,'status':status,
            'duration_seconds':round(time.monotonic()-started,2)})
        del _capture_events[:-24]

class StageFailure(RuntimeError):
    def __init__(self,code):
        self.code=code if code in CODES else 'worker_crashed'
        super().__init__(self.code)

def prepare_context(context):
    # A narrow host filter, never a wildcard block of source or authentication.
    context.route(AD_HOSTS,lambda route: route.abort())

def control(function,page,*args):
    phase={'_select_model_one':'model_selection','_select_symbol_mode':'symbol_selection',
           '_wait_for_heatmap':'wait_for_chart'}.get(function.__name__,'capture')
    if function.__name__=='_select_timeframe' and args and args[0] in ('12h','24h','48h'):
        phase='select_'+args[0]
    try:
        return function(page,*args)
    except Exception as exc:
        exc._model1_capture_phase=phase
        raise

def stage(value):
    global _current_stage
    _current_stage=value if value in STAGES else 'startup'

def numeric_usage(raw):
    if not isinstance(raw,dict):return None
    return {k:v for k,v in raw.items() if k in {'input_tokens','output_tokens','total_tokens'}
            and type(v) is int and 0<=v<=2_000_000}

def accept_model_response(response):
    global _usage
    if not response.ok:
        status=response.status_code
        code={401:'analysis_unauthorized',403:'analysis_unauthorized',429:'analysis_rate_limited'}.get(status,'analysis_http_error')
        if status==429:
            try:
                if response.json().get('error',{}).get('code')=='insufficient_quota':code='analysis_quota_exceeded'
            except Exception:pass
        raise StageFailure(code)
    try:payload=response.json()
    except Exception:raise StageFailure('analysis_response_invalid') from None
    if not isinstance(payload,dict):raise StageFailure('analysis_response_invalid')
    _usage=numeric_usage(payload.get('usage'))
    if payload.get('status')=='incomplete':raise StageFailure('analysis_incomplete')
    if payload.get('status')!='completed':raise StageFailure('analysis_response_invalid')
    return payload

def remember_analysis(raw):
    global _usage,_analysis_facts
    if not isinstance(raw,dict):return
    _usage=numeric_usage(raw.get('usage')) or _usage
    scans=raw.get('scans')
    _analysis_facts={}
    if isinstance(scans,list):_analysis_facts['scan_count']=min(len(scans),100)
    if not isinstance(scans,list) or len(scans)!=1 or not isinstance(scans[0],dict):return
    scan=scans[0];facts=dict(_analysis_facts)
    facts['prominent_levels_present']='prominent_levels' in scan
    for key,allowed in {'observed_timeframe':{'12h','24h','48h','unknown'},
        'current_price_confidence':{'high','medium','low'},
        'blocking_condition':{'none','loading','login','blur','challenge','unknown'}}.items():
        value=scan.get(key)
        if isinstance(value,str) and value in allowed:facts[key]=value
    if type(scan.get('readable')) is bool:facts['readable']=scan['readable']
    _analysis_facts=facts

def failure_detail(exc):
    inner=exc.__cause__ if exc.__cause__ is not None else exc
    kind=type(inner).__name__
    phase=getattr(exc,'_model1_capture_phase','capture')
    detail={'exception_kind':kind if kind in KINDS else 'other',
            'capture_phase':phase if phase in PHASES else 'capture'}
    readiness=getattr(exc,'_model1_source_readiness',None)
    if readiness is not None:
        from capture_readiness import safe_readiness,safe_network
        detail['source_readiness']=safe_readiness(readiness)
        detail['source_network']=safe_network(getattr(exc,'_model1_source_network',None))
        detail['readable']=False
        detail['blocking_condition']={'loading-indicator':'loading','blur':'blur'}.get(
            detail['source_readiness']['reason'],'unknown')
    # Only recognize fixed strings from our own selector; don't retain any text.
    text=str(exc)
    if text.startswith('Could not find the visible CoinGlass timeframe selector'):
        detail['selector_issue']='trigger_missing'
    elif 'Visible option ' in text and 'did not appear after opening selector' in text:
        detail['selector_issue']='option_missing'
    elif 'Timeframe selection did not settle on ' in text:
        detail['selector_issue']='selection_not_settled'
    elif kind=='TimeoutError':detail['selector_issue']='action_timeout'
    return detail

def classify(exc):
    if isinstance(exc,StageFailure):return exc.code
    inner=exc.__cause__ if exc.__cause__ is not None else exc
    timeout=type(inner).__name__ in {'TimeoutError','Timeout','ReadTimeout','ConnectTimeout'}
    if _current_stage=='capture':return 'source_timeout' if timeout else 'source_capture_failed'
    if _current_stage=='analysis':return 'analysis_timeout' if timeout else 'analysis_response_invalid'
    if _current_stage=='image_check':return 'image_evidence_invalid'
    if _current_stage=='validation':
        if str(exc)=='Current price is not readable with sufficient confidence':return 'price_uncertain'
        return 'zones_invalid'
    return 'worker_crashed'

def run_task(main,timeframe,job_id,output):
    global _current_stage,_usage,_analysis_facts,_capture_events
    _current_stage='startup';_usage=None;_analysis_facts={};_capture_events=[]
    started=time.monotonic();root=Path(output)
    try:
        main(timeframe,job_id,output);return 0
    except Exception as exc:
        report={'code':classify(exc),'stage':_current_stage,
            'elapsed_seconds':round(time.monotonic()-started,2),'usage':_usage,
            **failure_detail(exc),**_analysis_facts,'capture_operations':list(_capture_events)}
        root.mkdir(parents=True,exist_ok=True);path=root/'error.json'
        path.write_text(json.dumps(report),encoding='utf-8');path.chmod(0o600)
        return 1

def read_failure(path,job_id,returncode):
    report={'code':'worker_crashed','stage':'startup'}
    try:
        path=Path(path)
        if path.is_file() and not path.is_symlink() and path.stat().st_size<=4096:
            raw=json.loads(path.read_text())
            if isinstance(raw,dict):
                for key,allowed in {'code':CODES,'stage':STAGES,'capture_phase':PHASES,
                    'exception_kind':KINDS|{'other'},
                    'selector_issue':{'trigger_missing','option_missing','selection_not_settled','action_timeout'},
                    'observed_timeframe':{'12h','24h','48h','unknown'},
                    'current_price_confidence':{'high','medium','low'},
                    'blocking_condition':{'none','loading','login','blur','challenge','unknown'}}.items():
                    val=raw.get(key)
                    if isinstance(val,str) and val in allowed:report[key]=val
                if type(raw.get('elapsed_seconds')) in (int,float) and 0<=raw['elapsed_seconds']<=360:
                    report['elapsed_seconds']=raw['elapsed_seconds']
                report['usage']=numeric_usage(raw.get('usage'))
                if type(raw.get('readable')) is bool:report['readable']=raw['readable']
                if type(raw.get('scan_count')) is int and 0<=raw['scan_count']<=100:
                    report['scan_count']=raw['scan_count']
                if type(raw.get('prominent_levels_present')) is bool:
                    report['prominent_levels_present']=raw['prominent_levels_present']
                if isinstance(raw.get('source_readiness'),dict):
                    from capture_readiness import safe_readiness,safe_network
                    report['source_readiness']=safe_readiness(raw['source_readiness'])
                    report['source_network']=safe_network(raw.get('source_network'))
                events=raw.get('capture_operations')
                if isinstance(events,list):
                    report['capture_operations']=[]
                    for event in events[-24:]:
                        if not isinstance(event,dict):continue
                        duration=event.get('duration_seconds')
                        if (isinstance(event.get('phase'),str) and event.get('phase') in PHASES
                            and event.get('status') in ('completed','failed')
                            and type(duration) in (int,float) and math.isfinite(duration) and 0<=duration<=360):
                            report['capture_operations'].append({'phase':event['phase'],
                                'status':event['status'],'duration_seconds':duration})
    except Exception:pass
    print('MODEL1_JOB_FAILURE '+json.dumps({'job_id':job_id,'exit_code':returncode,**report}),flush=True)
    return report['code']
