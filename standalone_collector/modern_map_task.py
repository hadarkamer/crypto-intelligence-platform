"""One managed-session map, one PNG and one bounded visual verification."""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
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


MEASURED_INSTRUCTIONS = """Write a concise qualitative Hebrew summary of this single verified heatmap.
Explain the relative distribution and prominent clusters, and the limits of coverage. Open levels differ
from completed liquidations. Only describe supplied observations. Do not transcribe, estimate or calculate
numbers, forecast, give advice or invent levels. All displayed figures and cluster membership come from code."""


def summary_schema():
    return object_schema({'short_summary': {'type': 'string'}})


def interpretation_inputs(snapshot, overview=None):
    """Project only market labels with verified types from this map update into the request."""
    from modern_map_cells import map_metrics, metrics_from_overview
    overview = overview or {}
    metrics = metrics_from_overview(snapshot, overview) if overview else map_metrics(snapshot)
    facts = {k: snapshot[k] for k in ('symbol','heatmap_model','timeframe','mode','source_time',
                                      'column_time','raw_last_price','raw_price_step','raw_above_total','raw_below_total')}
    for field in ('raw_total','raw_realized_total','raw_realized_longs','raw_realized_shorts',
                  'raw_largest_price','raw_largest_value'):
        raw = overview.get(field)
        if raw is not None:
            number(raw, field != 'raw_largest_price')
        facts[field] = (None if field.startswith('raw_realized_') and
                        metrics.get('realized_24h_status') == 'component_total_conflict' else raw)
    if overview:
        facts['realized_24h_status'] = metrics['realized_24h_status']
        facts['sidebar_price_amount_distance'] = []
        for row in overview.get('sidebar_levels', []):
            price, amount, distance = (row.get(k) for k in ('raw_price','raw_value','raw_distance_pct'))
            number(price)
            number(amount, True)
            if distance is not None and not re.fullmatch(r'[+−-]?\d+(?:\.\d+)?%', distance):
                raise StageFailure('source_not_readable', 'sidebar_distance_unverified')
            facts['sidebar_price_amount_distance'].append([price, amount, distance])
        period = overview.get('raw_column_period')
        if period is not None and not re.fullmatch(r'\d+(?:s|m|h|min|minutes?)', period):
            raise StageFailure('source_not_readable', 'column_period_unverified')
        facts['column_period'] = period
        facts['sidebar_coverage'] = 'Displayed highlights; chart candidates assessed separately'
    facts['side_percent'] = metrics['side_percent']
    facts['denominator_basis'] = metrics['denominator_basis']
    facts['cluster_columns'] = ['side','colour','low','high','raw_total','distance_pct','liquidity_share_pct']
    rows = [[row[k] for k in facts['cluster_columns']] for row in metrics['clusters']]
    return MEASURED_INSTRUCTIONS + ' Verified market data: ' + json.dumps(facts, separators=(',', ':'), default=str), rows


def normalized_summary(raw, snapshot, run_id, image, overview=None):
    """Identity, coverage, prices and membership were verified by the reader before capture."""
    if (not isinstance(raw, dict) or set(raw) - {'short_summary','model','usage'} or
            not isinstance(raw.get('model'), str) or not 1 <= len(raw['model']) <= 100):
        raise StageFailure('analysis_response_invalid')
    summary = raw.get('short_summary')
    if not isinstance(summary, str) or not 1 <= len(summary.strip()) <= 1000 or re.search(r'[\d$%€]', summary):
        raise StageFailure('analysis_response_invalid', 'qualitative_summary_expected')
    if ((snapshot['symbol'],snapshot['heatmap_model'],snapshot['timeframe'],snapshot['mode']) != ('BTC',1,'12H','Coin')
            or snapshot['source_time'] != snapshot['source_time_after']
            or snapshot['column_time'] != snapshot['column_time_after']):
        raise StageFailure('source_not_readable', 'analysis_snapshot_identity_or_epoch')
    groups = snapshot['groups']
    validate_groups(groups, number(snapshot['raw_last_price'])[0], snapshot['raw_price_step'],
                    snapshot['raw_above_total'], snapshot['raw_below_total'])
    amounts = [number(group['raw_total'], True)[0] for group in groups]
    largest = max(amounts)
    scores = [float(amount / largest) for amount in amounts]
    result = _normalized_result(raw, snapshot, run_id, image, scores)
    if overview:
        from modern_map_cells import display_facts_summary, metrics_from_overview
        metrics = metrics_from_overview(snapshot, overview)
        prose = 'פרשנות: ' + summary.strip()
        source_lines = display_facts_summary(snapshot, overview, metrics, 4499-len(prose))
        result['summary'] = source_lines + '\n' + prose
    return result


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


def capture(symbol, model, timeframe, image_path, *, allow_login=True, session=None, observe=None):
    from contextlib import ExitStack
    from dedicated_map_flow import collect_with_dedicated_session, MapTarget
    from model1_execution import capture_operation
    from playwright.sync_api import sync_playwright
    from session_capture import CaptureSession

    if session is None:
        session = CaptureSession.from_environment()
    if session is None or not session.dedicated:
        raise StageFailure('session_input_invalid')
    context = None
    # The private session IPC removes credentials before this point. The AI
    # client runs later in this process. Its key must not reach the driver
    # or Chromium. Each capture already runs in a dedicated worker process.
    ai_key = os.environ.pop('OPENAI_API_KEY', None)
    try:
        with ExitStack() as resources:
            try:
                with phase('browser_context'):
                    pw = resources.enter_context(sync_playwright())
                    browser = pw.chromium.launch(headless=True, chromium_sandbox=True,
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
                        MapTarget(symbol, model, timeframe), image_path, allow_login=allow_login, observe=observe)
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
    from market_vision.openai_heatmap_scanner import analyze_heatmap_images
    from modern_map_reader import map_url
    symbol=os.environ['COLLECTION_HEATMAP_SYMBOL']
    map_url(symbol)
    raw_model=os.environ['COLLECTION_HEATMAP_MODEL']
    if raw_model not in ('1','2','3') or timeframe not in ('12H','24H','48H'):
        raise ValueError('Invalid capture identity')
    if (symbol,raw_model,timeframe) == ('BTC','1','12H'):
        return _run_measured_btc(root,run_id,analyze_heatmap_images)
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


def _run_measured_btc(root, run_id, analyze):
    image_path = root / 'image.png'
    source_path = root / 'measured-source.json'
    answer_path = root / 'measured-interpretation.json'
    out = root / 'result.json'
    if source_path.exists():
        capture_record = json.loads(source_path.read_text())
    else:
        overview = {}
        def observe(kind, data):
            if kind == 'overview':
                overview.update(data)
        stage('capture')
        snapshot = capture('BTC',1,'12H',image_path,observe=observe)
        image = image_path.read_bytes()
        if len(image) > 4*1024*1024 or not image.startswith(b'\x89PNG\r\n\x1a\n'):
            raise StageFailure('image_evidence_invalid')
        capture_record = dict(run_id=run_id,snapshot=snapshot,overview=overview,
                              image_sha256=hashlib.sha256(image).hexdigest())
        _write_once(source_path,capture_record)
    image = image_path.read_bytes()
    digest = hashlib.sha256(image).hexdigest()
    if capture_record.get('run_id') != run_id or capture_record.get('image_sha256') != digest:
        raise StageFailure('image_evidence_invalid')
    snapshot, overview = capture_record['snapshot'], capture_record['overview']
    if out.exists():
        saved = json.loads(out.read_text())
        if (saved['run_id'],saved['evidence']['sha256']) != (run_id,digest):
            raise StageFailure('image_evidence_invalid')
        return
    if answer_path.exists():
        stored = json.loads(answer_path.read_text())
        if (stored['run_id'],stored['image_sha256']) != (run_id,digest):
            raise StageFailure('analysis_response_invalid')
        raw = stored['answer']
    else:
        instructions, rows = interpretation_inputs(snapshot,overview)
        # Only a known response can be replayed. An indeterminate POST cannot
        # consume another request for the same retained capture.
        try:
            _write_once(root/'measured-analysis-started.json', {'run_id':run_id,'image_sha256':digest})
        except FileExistsError:
            raise StageFailure('analysis_response_invalid') from None
        stage('analysis')
        with phase('analysis'):
            raw = analyze([dict(image=str(image_path),timeframe='12H',measure_candidates=rows)], symbol='BTC',
                          timeout_seconds=120,evidence_schema=summary_schema(),evidence_instructions=instructions)
        _write_once(answer_path,dict(run_id=run_id,image_sha256=digest,answer=raw))
    remember_analysis(raw)
    stage('validation')
    result = normalized_summary(raw,snapshot,run_id,image,overview)
    stage('saving')
    with phase('result_write'):
        _write_once(out,result)
