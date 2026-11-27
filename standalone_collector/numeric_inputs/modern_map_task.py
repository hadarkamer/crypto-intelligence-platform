"""Collect one managed-session map and publish the validated result for its reader."""
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
    scores=[]
    for i,group in enumerate(g):
        verdict=indexed[f'c{i}']
        if verdict.get('right_edge_visible') is not True:
            raise StageFailure('zones_invalid','right_edge_missing')
        score=verdict.get('strength_score')
        if type(score) not in (int,float) or not math.isfinite(score) or not 0<=score<=1:
            raise StageFailure('zones_invalid','strength_score')
        scores.append(score)
    return _normalized_result(raw, snapshot, run_id, image, scores)


def _normalized_result(raw, snapshot, run_id, image, scores):
    symbol, model, tf = snapshot['symbol'], snapshot['heatmap_model'], snapshot['timeframe']
    zones = []
    for group, score in zip(snapshot['groups'], scores, strict=True):
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


def check_ocr_runtime(observe=None):
    from modern_map_axis_ocr import BINARY
    # The build verifies the binary's version and the language model's
    # digest. Catch an incomplete runtime before any new map navigation.
    for asset,mode,cause in (
            (BINARY,os.X_OK,'ocr_launcher_unavailable'),
            (BINARY.parent/'bin/tesseract',os.X_OK,'ocr_binary_unavailable'),
            (BINARY.parent/'tessdata/eng.traineddata',os.R_OK,'ocr_english_unavailable'),
            (Path('/usr/bin/prlimit'),os.X_OK,'ocr_process_limiter_unavailable')):
        if not asset.is_file() or not os.access(asset,mode):
            if observe:
                observe('stage',dict(name='basic_ocr_readiness',status='failed',condition=cause))
            error=StageFailure('source_capture_failed')
            error.basic_reason=cause
            raise error


def capture(symbol, model, timeframe, image_path, *, allow_login=True, session=None, observe=None, base_only=False):
    from contextlib import ExitStack
    from dedicated_map_flow import collect_with_dedicated_session, MapTarget
    from model1_execution import capture_operation
    from playwright.sync_api import sync_playwright
    from session_capture import CaptureSession

    if session is None:
        session = CaptureSession.from_environment()
    if session is None or not session.dedicated:
        raise StageFailure('session_input_invalid')
    if base_only:
        check_ocr_runtime(observe)
    context = None
    # The private session IPC removes credentials before this point. The AI
    # client runs later in this process. Its key must not reach the driver
    # or browser. Each capture already runs in a dedicated worker process.
    ai_key = os.environ.pop('OPENAI_API_KEY', None)
    try:
        with ExitStack() as resources:
            try:
                with phase('browser_context'):
                    pw = resources.enter_context(sync_playwright())
                    if base_only or (symbol,model,timeframe)==('BTC',1,'12H'):
                        browser=pw.firefox.launch(headless=True,timeout=20000,env={
                            k:os.environ[k] for k in ('HOME','LANG','PATH','LD_LIBRARY_PATH') if k in os.environ})
                    else:
                        browser=pw.chromium.launch(headless=True,chromium_sandbox=True,
                                                   args=['--disable-dev-shm-usage'])
                    resources.callback(browser.close)
                    context = browser.new_context(storage_state=session.state, locale='en-US',
                        timezone_id='UTC', color_scheme='dark', device_scale_factor=1,
                        viewport={'width': 1440, 'height': 1280})
                    resources.callback(context.close)
                    prepare_context(context)
                    page = context.new_page()
                    session.attach(context, page)
                with capture_operation('capture'):
                    return collect_with_dedicated_session(session, context, page,
                        MapTarget(symbol, model, timeframe), image_path, allow_login=allow_login, observe=observe,
                        base_only=base_only)
            except Exception as error:
                session.note_source_error(error)
                raise
            finally:
                with phase('browser_checkpoint'):
                    session.finish(context)
    finally:
        if ai_key is not None:
            os.environ['OPENAI_API_KEY'] = ai_key


def run_modern_task(timeframe,run_id,output):
    root=Path(output)
    root.mkdir(parents=True,exist_ok=True)
    with task_metrics(root):
        return _run(timeframe,run_id,root)


def _run(timeframe,run_id,root):
    from modern_map_reader import map_url
    symbol=os.environ['COLLECTION_HEATMAP_SYMBOL']
    map_url(symbol)
    raw_model=os.environ['COLLECTION_HEATMAP_MODEL']
    from modern_map_basic_numbers import basic_target
    if raw_model not in ('1','2','3') or not basic_target(symbol,int(raw_model),timeframe):
        raise ValueError('Invalid capture identity')
    return _run_basic_btc(root,run_id,base_only=True,target=(symbol,int(raw_model),timeframe))


def _write_once(path, value):
    """Publish only a fully written JSON file in the private work directory."""
    import tempfile
    encoded = json.dumps(value,ensure_ascii=False,allow_nan=False,sort_keys=True,separators=(',',':')).encode()
    fd, temp = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd,'wb') as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temp,path)
    finally:
        os.unlink(temp)


def numeric_result(snapshot, job_id, png_bytes, *, restored=False):
    """Only a live, end-verified BASIC observation can become a ready job."""
    from uuid import UUID
    from modern_map_basic_numbers import validate_basic_snapshot
    try:
        valid_id = str(UUID(job_id)) == job_id
    except (TypeError, ValueError, AttributeError):
        valid_id = False
    if not valid_id or len(png_bytes) > 4*1024*1024 or not png_bytes.startswith(b'\x89PNG\r\n\x1a\n'):
        raise StageFailure('image_evidence_invalid')
    digest = hashlib.sha256(png_bytes).hexdigest()
    snapshot = validate_basic_snapshot(snapshot,restored=restored)
    if snapshot.get('source_sha256') != digest:
        raise StageFailure('image_evidence_invalid')
    if (snapshot.get('provenance') != 'live' or snapshot.get('end_column_verified') is not True
            or snapshot.get('end_reference_verified') is not True):
        raise StageFailure('source_not_readable','live_basic_verification_missing')
    return dict(schema_version=f"coinglass-model{snapshot['heatmap_model']}.v1", result_type='verified_basic', run_id=job_id,
                symbol=snapshot['symbol'],heatmap_model=snapshot['heatmap_model'],
                timeframe=snapshot['timeframe'],source_url=snapshot['source_url'],
                captured_at=snapshot['captured_at'], source_updated_at=snapshot['source_time'],provider='CoinGlass',
                evidence=dict(artifact_id=job_id,sha256=digest,content_type='image/png'),basic_snapshot=snapshot)


def _run_basic_btc(root, run_id, *, base_only=True,target=('BTC',1,'12H')):
    from modern_map_checkpoint import acknowledgements
    with acknowledgements() as acknowledge:
        return _process_basic_btc(root, run_id, acknowledge, base_only=base_only,target=target)


def _same_basic_observation(basic, later):
    """Compare two validated snapshots using their original market values and chart evidence."""
    from datetime import datetime
    replaceable = {'captured_at','collection_tier','enrichment','coverage','levels','chart_regions',
                   'source_evidence','strongest_observed_prices'}
    def identity(snapshot):
        direct = {k:v for k,v in snapshot.items() if k not in replaceable}
        evidence = {k:v for k,v in snapshot['source_evidence'].items() if k != 'enrichment'}
        geometry = [(r['side'],r['colour'],r['prices']) for r in snapshot['chart_regions']]
        return direct, evidence, geometry
    start = datetime.fromisoformat(basic['captured_at'].replace('Z','+00:00'))
    end = datetime.fromisoformat(later['captured_at'].replace('Z','+00:00'))
    return end >= start and identity(basic)==identity(later)


def same_basic_with_enrichment(basic, enriched):
    return (basic['collection_tier']=='basic' and enriched['collection_tier']=='enriched'
            and _same_basic_observation(basic,enriched))


def _process_basic_btc(root, run_id, acknowledge, *, base_only,target=('BTC',1,'12H'),capture_fn=None):
    from modern_map_checkpoint import SOURCE, UPGRADE, partial_record
    image_path = root / 'image.png'
    source_path, enriched_path = root / SOURCE, root / UPGRADE

    def check_target(snapshot):
        if (snapshot.get('symbol'),snapshot.get('heatmap_model'),snapshot.get('timeframe'))!=target:
            raise StageFailure('screenshot_identity_mismatch')

    def record_result(record, image):
        check_target(record['snapshot'])
        result = numeric_result(record['snapshot'],run_id,image,restored=True)
        if record.get('run_id') != run_id or record.get('image_sha256') != result['evidence']['sha256']:
            raise StageFailure('image_evidence_invalid')
        return result

    def persist(snapshot,path,action, *, basic=None):
        check_target(snapshot)
        result = numeric_result(snapshot,run_id,image_path.read_bytes())
        if basic is not None and not same_basic_with_enrichment(basic,snapshot):
            raise StageFailure('source_not_readable','basic_observation_changed')
        if base_only and (snapshot['collection_tier']!='basic' or snapshot['enrichment']['observations']!=0):
            raise StageFailure('source_not_readable','unexpected_enrichment_in_base_only')
        record = dict(run_id=run_id,snapshot=snapshot,image_sha256=result['evidence']['sha256'])
        if path.exists():
            if json.loads(path.read_text()) != record:
                raise StageFailure('image_evidence_invalid')
        else:
            _write_once(path,record)
        acknowledge(action)
        return result

    if source_path.exists():
        # The parent restores the strongest committed version to SOURCE, even after the
        # old sandbox was destroyed. A local UPGRADE may be waiting for acknowledgement.
        path,action = (enriched_path,b'U') if enriched_path.exists() else (source_path,b'S')
        result = record_result(json.loads(path.read_text()),image_path.read_bytes())
        acknowledge(action)
    else:
        overview, hovers, stages = {}, [], []
        result = None
        def observe(kind, data):
            nonlocal result
            if kind == 'overview':
                overview.update(data)
            elif kind == 'stage':
                stages.append(data)
            elif kind == 'hover':
                hovers.append(data)
            elif kind == 'verified_basic':
                if data.get('collection_tier')!='basic':
                    raise StageFailure('source_not_readable','invalid_first_basic_receipt')
                from copy import deepcopy
                result = persist(deepcopy(data),source_path,b'S')
        stage('capture')
        try:
            snapshot = (capture_fn or capture)(*target,image_path,observe=observe,base_only=base_only)
        except Exception as error:
            if result is None:
                if not source_path.exists() and (overview or hovers or stages):
                    partial = partial_record(run_id,overview,hovers,stages,error,image_path,identity=target)
                    _write_once(root/'measured-partial.json',partial)
                    acknowledge(b'P')
                raise
            # Only a read failure after the independently closed first observation can
            # fall back immediately. Auth/storage/transport errors keep their own status.
            if not isinstance(error,StageFailure) or error.code not in ('source_not_readable','source_timeout'):
                raise
        else:
            stage('validation')
            if result is None:
                result = persist(snapshot,source_path,b'S')
            elif snapshot!=result['basic_snapshot']:
                if base_only:
                    raise StageFailure('source_not_readable','basic_observation_changed')
                basic = result['basic_snapshot']
                if snapshot.get('collection_tier')=='basic':
                    # A repeated, previously known row is not a stronger observation.
                    numeric_result(snapshot,run_id,image_path.read_bytes())
                    if not _same_basic_observation(basic,snapshot):
                        raise StageFailure('source_not_readable','basic_observation_changed')
                else:
                    result = persist(snapshot,enriched_path,b'U',basic=basic)
    stage('saving')
    out = root/'result.json'
    if out.exists():
        if json.loads(out.read_text()) != result:
            raise StageFailure('image_evidence_invalid')
        return
    with phase('result_write'):
        _write_once(out,result)
