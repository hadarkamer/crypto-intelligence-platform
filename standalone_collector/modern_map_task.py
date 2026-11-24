"""One managed-session map, one PNG and one bounded visual verification."""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path

from collection_metrics import phase, task_metrics
from modern_map_evidence import number, validate_groups
from model1_execution import StageFailure, numeric_usage, prepare_context, remember_analysis, stage

INSTRUCTIONS = """Read the actual visible coin, selected model, timeframe and Coin tab from the attached chart.
When any identity is unreadable, use unknown. Reject a login gate, obscured edge, wrong view or unreadable chart.
The supplied candidate IDs and price bounds index locations measured on this exact image. For every ID check whether
the coloured band reaches the latest plotted column. Return one verification per ID and a relative strength score
between 0 and 1. Use the exact printed strings, such as Model 1 and 12 hour. For a clear chart use none for
blocking_condition. Never create extra candidates or supply numerical measurements. Keep the summary observational."""


def object_schema(properties):
    return {'type':'object','properties':properties,'required':list(properties),'additionalProperties':False}


def scan_schema():
    text={'type':'string'}
    candidate=object_schema(dict(candidate_id=text,right_edge_visible={'type':'boolean'},
                                 strength_score={'type':'number','minimum':0,'maximum':1}))
    def enum(*values):
        return {'type':'string','enum':list(values)}
    return object_schema(dict(observed_symbol=text,observed_model=enum('Model 1','Model 2','Model 3','unknown'),
                              observed_timeframe=enum('12 hour','24 hour','48 hour','unknown'),
                              observed_mode=enum('Coin','Pair','unknown'),readable={'type':'boolean'},
                              blocking_condition=enum('none','loading','login','blur','challenge','unknown'),
                              short_summary=text,prominent_levels={'type':'array','items':candidate}))


def normalized(raw, snapshot, run_id, image):
    g=snapshot['groups']
    validate_groups(g,number(snapshot['raw_last_price'])[0],snapshot['raw_price_step'],
                    snapshot['raw_above_total'],snapshot['raw_below_total'])
    symbol, model, tf=snapshot['symbol'],snapshot['heatmap_model'],snapshot['timeframe']
    if not isinstance(raw,dict) or not isinstance(raw.get('model'),str) or not 1<=len(raw['model'])<=100:
        raise StageFailure('analysis_response_invalid')
    if (raw.get('observed_symbol'),raw.get('observed_model'),raw.get('observed_timeframe'),raw.get('observed_mode'))!=(
            symbol,f'Model {model}',tf.lower().replace('h',' hour'),'Coin'):
        raise StageFailure('screenshot_identity_mismatch')
    if raw.get('readable') is not True or raw.get('blocking_condition') != 'none':
        raise StageFailure('source_not_readable')
    verdicts=raw.get('prominent_levels')
    if not isinstance(verdicts,list) or any(not isinstance(v,dict) for v in verdicts):
        raise StageFailure('analysis_response_invalid')
    indexed={v.get('candidate_id'):v for v in verdicts if isinstance(v.get('candidate_id'),str)}
    if len(verdicts)!=len(g) or set(indexed)!={f'c{i}' for i in range(len(g))}:
        raise StageFailure('zones_invalid','hover_candidate_disagreement')
    zones=[]
    for i,group in enumerate(g):
        verdict=indexed[f'c{i}']
        if verdict.get('right_edge_visible') is not True:
            raise StageFailure('zones_invalid','right_edge_missing')
        score=verdict.get('strength_score')
        if type(score) not in (int,float) or not math.isfinite(score) or not 0<=score<=1:
            raise StageFailure('zones_invalid','strength_score')
        prices=[m['raw_price'] for m in group['members']]
        lo,hi=(float(number(x)[0]) for x in (prices[0],prices[-1]))
        colour=group['colour']
        zone=dict(side=group['side'],price_low=lo,price_high=hi,
                  intensity={'yellow':'many','green':'normal','blue':'few'}[colour],
                  shape='point' if lo==hi else 'range',price_precision='verified_label',
                  prices=[lo] if lo==hi else [],evidence_basis='numeric_row_label',
                  price_evidence='Right edge · '+(prices[0] if lo==hi else f'{prices[0]} → {prices[-1]}'),
                  uncertainty_usd=0,strength_score=score)
        if lo!=hi:
            zone.update(row_colour=colour,continuous=True,weak_gap_present=False)
        zones.append(zone)
    current=float(number(snapshot['raw_last_price'])[0])
    zones.sort(key=lambda z:(z['side']!='above',{'many':0,'normal':1,'few':2}[z['intensity']],
                             -z['strength_score'],min(abs(z['price_low']-current),abs(z['price_high']-current))))
    digest=hashlib.sha256(image).hexdigest()
    return dict(schema_version=f'coinglass-model{model}.v1',level_contract_version='prominent-rows.v1',
                selection_policy='prominent_right_edge.v1',run_id=run_id,source_url=snapshot['source_url'],
                symbol=symbol,heatmap_model=model,timeframe=tf,captured_at=snapshot['captured_at'],
                source_updated_at=snapshot['source_time'],provider='OpenAI',model=raw['model'],usage=numeric_usage(raw.get('usage')),
                observed_price=current,price_reference_type='last_price_label',observed_price_range=None,
                omitted_ambiguous_zones=0,zones=zones,evidence=dict(artifact_id=run_id,sha256=digest,content_type='image/png'),
                measure_snapshot=dict(snapshot,artifact_id=run_id,sha256=digest),summary=str(raw.get('short_summary',''))[:1000],
                quality='visual_estimate',quantity_status='unavailable',quantity_reason='source_usd_unverified')


def capture(symbol,model,timeframe,image_path):
    from playwright.sync_api import sync_playwright
    from modern_map_reader import read_map
    from session_capture import CaptureSession
    session=CaptureSession.from_environment()
    if session is None:
        raise StageFailure('session_input_invalid')
    context=None
    with sync_playwright() as pw:
        browser=None
        try:
            with phase('browser_context'):
                browser=pw.chromium.launch(headless=True,args=['--disable-dev-shm-usage'])
                context=browser.new_context(storage_state=session.state,locale='en-US',timezone_id='UTC',
                                            color_scheme='dark',device_scale_factor=1,viewport={'width':1440,'height':1280})
                prepare_context(context)
                page=context.new_page()
                session.attach(context,page)
            return read_map(page,symbol,model,timeframe,image_path)
        except Exception as error:
            session.note_source_error(error)
            raise
        finally:
            try:
                with phase('browser_checkpoint'):
                    session.finish(context)
            finally:
                if context is not None:context.close()
                if browser is not None:browser.close()


def run_modern_task(timeframe,run_id,output):
    root=Path(output)
    root.mkdir(parents=True,exist_ok=True)
    with task_metrics(root):
        return _run(timeframe,run_id,root)


def _run(timeframe,run_id,root):
    from market_vision.openai_heatmap_scanner import analyze_heatmap_images
    from modern_map_reader import map_url
    symbol=os.environ['COLLECTION_HEATMAP_SYMBOL']
    map_url(symbol)
    raw_model=os.environ['COLLECTION_HEATMAP_MODEL']
    if raw_model not in ('1','2','3') or timeframe not in ('12H','24H','48H'):
        raise ValueError('Invalid capture identity')
    path=root/'image.png'
    stage('capture')
    snapshot=capture(symbol,int(raw_model),timeframe,path)
    stage('image_check')
    image=path.read_bytes()
    if len(image)>4*1024*1024 or not image.startswith(b'\x89PNG\r\n\x1a\n'):
        raise StageFailure('image_evidence_invalid')
    candidates=[dict(candidate_id=f'c{i}',colour=g['colour'],side=g['side'],
                     low=g['members'][0]['raw_price'],high=g['members'][-1]['raw_price']) for i,g in enumerate(snapshot['groups'])]
    stage('analysis')
    with phase('analysis'):
        raw=analyze_heatmap_images([dict(image=str(path),timeframe=timeframe,measure_candidates=candidates)],
                                symbol=symbol,timeout_seconds=120,evidence_schema=scan_schema(),evidence_instructions=INSTRUCTIONS)
    remember_analysis(raw)
    stage('validation')
    result=normalized(raw,snapshot,run_id,image)
    stage('saving')
    with phase('result_write'):
        (root/'result.json').write_text(json.dumps(result,allow_nan=False),encoding='utf-8')
