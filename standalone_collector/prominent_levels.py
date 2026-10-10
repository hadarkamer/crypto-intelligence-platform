"""Source-specific prominent heatmap rows, validated without inferred precision.

This module is installed only in the standalone app collector. It never scans
the source itself. All price evidence refers to the one model/horizon PNG.
"""
from __future__ import annotations
import hashlib
import math
import re

from heatmap_models import HEATMAP_MODEL, MODEL_LABEL, SOURCE_URL, SCHEMA_VERSION
from model1_execution import StageFailure
from model1_price_range import validate_price_range

CONTRACT='prominent-rows.v1'
SELECTION_POLICY='prominent_right_edge.v1'
MAX_LEVELS=40
NUMBER_TOKEN=re.compile(r'(?<![\d.])(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?(?![\d.])')

def num(value):
    if type(value) not in (int,float) or not math.isfinite(value) or value<=0:
        raise StageFailure('zones_invalid')
    return float(value)

def evidence(value, prices=()):
    if not isinstance(value,str) or not value.strip() or len(value)>500:
        raise StageFailure('zones_invalid')
    found={float(token.replace(',','')) for token in NUMBER_TOKEN.findall(value)}
    if any(price not in found for price in prices):
        raise StageFailure('zones_invalid')
    return value.strip()

def identity(raw,timeframe):
    scans=raw.get('scans') if isinstance(raw,dict) else None
    if not isinstance(scans,list) or len(scans)!=1 or not isinstance(scans[0],dict):
        raise StageFailure('analysis_response_invalid')
    scan=scans[0]
    if (timeframe not in ('12H','24H','48H') or raw.get('symbol')!='BTC'
        or raw.get('analysis_mode')!='visual_screenshot'
        or str(scan.get('timeframe','')).upper()!=timeframe
        or scan.get('observed_symbol')!='BTC' or scan.get('observed_mode')!='Symbol'
        or scan.get('observed_model')!=MODEL_LABEL
        or scan.get('observed_timeframe')!=timeframe.lower()):
        raise StageFailure('screenshot_identity_mismatch')
    if scan.get('readable') is not True or scan.get('blocking_condition')!='none':
        raise StageFailure('source_not_readable')
    return scan

def normalize_prominent(raw,*,timeframe,run_id,captured_at,image):
    scan=identity(raw,timeframe)
    current=validate_price_range(scan.get('current_price_range'))
    axis=scan.get('price_axis_range')
    if not isinstance(axis,dict) or set(axis)!={'low','high','price_evidence'}:
        raise StageFailure('zones_invalid')
    axis_low,axis_high=num(axis['low']),num(axis['high'])
    if not axis_low<=current['low']<current['high']<=axis_high:
        raise StageFailure('zones_invalid')
    axis_text=evidence(axis['price_evidence'],(axis_low,axis_high))
    candidates=scan.get('prominent_levels')
    if not isinstance(candidates,list) or len(candidates)>MAX_LEVELS:
        raise StageFailure('zones_invalid')
    model=raw.get('model')
    if not isinstance(model,str) or not model.strip() or len(model)>100:
        raise StageFailure('analysis_response_invalid')
    zones=[];seen=set();omitted=0
    for item in candidates:
        if not isinstance(item,dict):raise StageFailure('zones_invalid')
        if item.get('confidence')=='low':continue
        if item.get('confidence') not in ('high','medium'):raise StageFailure('zones_invalid')
        if item.get('right_edge_visible') is not True:raise StageFailure('zones_invalid')
        colour=item.get('row_colour');shape=item.get('shape');intensity=item.get('intensity')
        if colour not in ('yellow','green','blue') or shape not in ('point','range'):
            raise StageFailure('zones_invalid')
        if colour=='yellow' and (intensity!='many' or (shape=='range' and
            (item.get('continuous') is not True or item.get('weak_gap_present') is not False))):
            raise StageFailure('zones_invalid')
        if colour=='green' and (shape!='range' or intensity!='normal'
            or item.get('continuous') is not True or item.get('weak_gap_present') is not False):
            raise StageFailure('zones_invalid')
        if colour=='blue' and intensity!='few':raise StageFailure('zones_invalid')
        low,high=num(item.get('price_low')),num(item.get('price_high'))
        if not axis_low<=low<=high<=axis_high or (shape=='range' and not low<high):
            raise StageFailure('zones_invalid')
        raw_prices=item.get('prices')
        if not isinstance(raw_prices,list) or len(raw_prices)!=(1 if shape=='point' else 0):
            raise StageFailure('zones_invalid')
        prices=[num(p) for p in raw_prices]
        if any(p<low or p>high for p in prices):raise StageFailure('zones_invalid')
        precision=item.get('price_precision');basis=item.get('evidence_basis')
        uncertainty=item.get('uncertainty_usd')
        if type(uncertainty) not in (int,float) or not math.isfinite(uncertainty) or uncertainty<0:
            raise StageFailure('zones_invalid')
        text=evidence(item.get('price_evidence'))
        if precision=='verified_label':
            if (basis not in ('numeric_row_label','tooltip') or uncertainty!=0
                or item.get('confidence')!='high'):
                raise StageFailure('zones_invalid')
            evidence(text,prices or (low,high))
            if shape=='point' and not low==high==prices[0]:raise StageFailure('zones_invalid')
        elif precision=='axis_estimate':
            if basis!='axis_interpolation' or uncertainty<=0 or not low<high:
                raise StageFailure('zones_invalid')
            if prices and uncertainty+1e-9<max(prices[0]-low,high-prices[0]):
                raise StageFailure('zones_invalid')
        else:raise StageFailure('zones_invalid')
        score=item.get('strength_score')
        if type(score) not in (int,float) or not math.isfinite(score) or not 0<=score<=1:
            raise StageFailure('zones_invalid')
        side=item.get('side')
        if side not in ('above','below'):raise StageFailure('zones_invalid')
        if low<=current['high'] and high>=current['low']:
            omitted+=1;continue
        if (side=='above' and low<=current['high']) or (side=='below' and high>=current['low']):
            raise StageFailure('zones_invalid')
        key=(side,shape,low,high,tuple(prices))
        if key in seen:continue
        seen.add(key)
        zones.append({'side':side,'price_low':low,'price_high':high,'intensity':intensity,
            'shape':shape,'price_precision':precision,'prices':prices,
            'evidence_basis':basis,'price_evidence':text,'uncertainty_usd':float(uncertainty),
            'strength_score':float(score)})
        if colour=='yellow' and shape=='range':
            zones[-1].update(row_colour='yellow',continuous=True,weak_gap_present=False)
    # Faint blue rows never pad a list containing independently prominent rows.
    if any(z['intensity'] in ('many','normal') for z in zones):
        zones=[z for z in zones if z['intensity']!='few']
    zones.sort(key=lambda z:(0 if z['side']=='above' else 1,
        {'many':0,'normal':1,'few':2}[z['intensity']],-z['strength_score'],
        min(abs(z['price_low']-current['high']),abs(z['price_high']-current['low']))))
    if not zones:raise StageFailure('no_unambiguous_zones')
    return {'schema_version':SCHEMA_VERSION,'level_contract_version':CONTRACT,
        'selection_policy':SELECTION_POLICY,
        'run_id':run_id,'source_url':SOURCE_URL,'symbol':'BTC','heatmap_model':HEATMAP_MODEL,
        'timeframe':timeframe,'captured_at':captured_at,'source_updated_at':None,
        'provider':'OpenAI','model':model,'usage':raw.get('usage'),'observed_price':None,
        'price_reference_type':'visual_range','observed_price_range':current,
        'price_axis_range':{'low':axis_low,'high':axis_high,'price_evidence':axis_text},
        'omitted_ambiguous_zones':omitted,'zones':zones,
        'evidence':{'sha256':hashlib.sha256(image).hexdigest(),'artifact_id':run_id,'content_type':'image/png'},
        'summary':str(scan.get('short_summary',''))[:1000],'quality':'visual_estimate',
        'quantity_status':'unavailable','quantity_reason':'source_usd_unverified'}

LEVEL_SCHEMA={'type':'object','additionalProperties':False,'properties':{
    'side':{'type':'string','enum':['above','below']},
    'shape':{'type':'string','enum':['point','range']},
    'price_low':{'type':'number'},'price_high':{'type':'number'},
    'prices':{'type':'array','maxItems':1,'items':{'type':'number'}},
    'intensity':{'type':'string','enum':['many','normal','few']},
    'row_colour':{'type':'string','enum':['yellow','green','blue']},
    'confidence':{'type':'string','enum':['high','medium','low']},
    'price_precision':{'type':'string','enum':['verified_label','axis_estimate']},
    'evidence_basis':{'type':'string','enum':['numeric_row_label','tooltip','axis_interpolation']},
    'price_evidence':{'type':'string','maxLength':500},
    'uncertainty_usd':{'type':'number','minimum':0},
    'strength_score':{'type':'number','minimum':0,'maximum':1},
    'continuous':{'type':'boolean'},'weak_gap_present':{'type':'boolean'},
    'right_edge_visible':{'type':'boolean'},
}}
LEVEL_SCHEMA['required']=list(LEVEL_SCHEMA['properties'])
AXIS_SCHEMA={'type':'object','additionalProperties':False,'properties':{
    'low':{'type':'number'},'high':{'type':'number'},
    'price_evidence':{'type':'string','maxLength':500}},'required':['low','high','price_evidence']}

INSTRUCTIONS='''
This request contains exactly ONE independent screenshot for ONE requested
model/timeframe. An enlarged detail crop, if supplied, contains saved pixels
from that same screenshot. Return exactly ONE object in scans for the pair,
including if it is unreadable; never create another scan for the detail crop.
Use the prominent_levels list as the authoritative extraction, with no target
number of levels. Treat every requested model and timeframe independently.
First transcribe the visible minimum/maximum PRICE-axis ticks into
price_axis_range, including their exact displayed text as price_evidence.
Work only at the CURRENT RIGHT EDGE where active heatmap rows reach the latest
time. Historical bright regions that have ended are excluded.
Extract the clearest active YELLOW price bands first, as concise price ranges
when the visible yellow band has continuous vertical width. Use shape=range,
intensity=many, prices=[], continuous=true and weak_gap_present=false for that
band. A weak/empty gap splits bands; never combine separate yellow rows across
a gap. An independently prominent thin yellow ROW remains a point with
intensity=many. Its uncertainty interval is not a continuous yellow band.
A prominent uninterrupted green CLUSTER is a range with intensity=normal;
its area or sum must NEVER upgrade it to many. Empty or very weak/blue rows
BREAK that cluster: split it, never bridge the gap. continuous=true and
weak_gap_present=false are necessary for a green range.
Omit faint blue/few levels whenever prominent yellow or green levels exist.
Do not fill quotas or create missing levels. Within each side, prioritize many
over normal, then the strongest independently visible row/cluster first.
strength_score is relative visual prominence in 0..1, not dollars/probability.
Prices are exact ONLY when a numeric label of THAT specific row or a tooltip
is visibly readable. Transcribe its verbatim text as price_evidence, use
verified_label and numeric_row_label/tooltip, uncertainty_usd=0. A mere axis
tick, interpolation, enlargement or model confidence NEVER verifies a row's
exact price. Never fabricate a tooltip or row label absent from the PNG.
Otherwise use axis_estimate and axis_interpolation with a positive explicit
uncertainty_usd and explain the visible bracketing ticks. For an estimated
yellow point, prices contains the approximate row center; price_low/high
bound its uncertainty, not a continuous liquidity cluster. uncertainty_usd
must cover both bounds. Do not narrow estimates to pass validation.
For an exact point, price_low=price_high=prices[0]. A green range has prices=[]
and ordered price_low<price_high; preserve its actual continuous bounds.
Above/below classification must respect the FULL current_price_range, never
its midpoint. Uncertainty intervals crossing that current range are omitted.
Do not treat colour legend quantities as prices or side totals. Preserve
original symbol/model/timeframe and source-readability safeguards.
'''

def install_schema(schema):
    scans=schema['properties']['scans']
    scans.update(minItems=1,maxItems=1)
    scan=scans['items']
    for key in ('above_price','below_price'):
        scan['properties'].pop(key,None)
        scan['required'].remove(key)
    scan['properties']['prominent_levels']={'type':'array','maxItems':MAX_LEVELS,'items':LEVEL_SCHEMA}
    scan['properties']['price_axis_range']=AXIS_SCHEMA
    scan['required'].extend(['prominent_levels','price_axis_range'])
