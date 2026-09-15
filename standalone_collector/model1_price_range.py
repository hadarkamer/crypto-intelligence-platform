"""Range-aware extraction for the app; legacy point validation remains available.

An interval is an estimate, not an exact quote. The initial 1% width cap is an
engineering policy, not calibrated accuracy or a trading threshold. Zones that
intersect the uncertainty interval are omitted, not assigned using a midpoint.
"""
from __future__ import annotations
import hashlib
import math
from typing import Any
from model1_execution import StageFailure

SCHEMA='coinglass-model1.v1'
SOURCE='https://www.coinglass.com/pro/futures/LiquidationHeatMap?coin=BTC&type=symbol'
MAX_RANGE_FRACTION=0.01
RANGE_KEYS={'low','high','confidence','basis','axis_low','axis_high'}
STRENGTH={'very_strong':'many','strong':'many','medium':'normal','weak':'few'}
RANGE_SCHEMA={
    'type':'object','additionalProperties':False,
    'properties':{
        'low':{'type':['number','null']},'high':{'type':['number','null']},
        'confidence':{'type':'string','enum':['high','medium','low']},
        'basis':{'type':'string','enum':['last_candle_axis_bracket','unreadable']},
        'axis_low':{'type':['number','null']},'axis_high':{'type':['number','null']},
    },'required':['low','high','confidence','basis','axis_low','axis_high'],
}
RANGE_INSTRUCTIONS='''
The user needs NARROW PRICE RANGES, not an exact current quote absent from the
chart. Return current_price_range for the LAST RIGHTMOST candle close, using the
adjacent numeric PRICE axis (not time or the liquidity legend). low/high enclose
that close at the visible resolution; axis_low/axis_high transcribe TWO distinct
visible axis ticks that bracket that range. Confidence describes the INTERVAL,
not an exact point. A clear interval may have medium/high confidence even when
current_price_estimate is null/low confidence because no exact label is printed.
Do not invent an exact quote or assign the midpoint to current_price_estimate.
Only an actually legible printed last-price label supports an exact quote.
The operational range limit is 1% of its midpoint; NEVER narrow an uncertain
range artificially to satisfy it. If unable to bound the last close, use null
bounds, basis=unreadable and confidence=low. Preserve observed timeframe.
Above zones must lie ENTIRELY above the range.high; below zones ENTIRELY below
range.low. Ranges touching/overlapping the price interval are ambiguous, not
extra evidence. Preserve visible zone widths instead of invented exact prices.
'''


def finite_positive(value: Any, code='price_range_uncertain') -> float:
    if type(value) not in (int,float) or not math.isfinite(value) or value<=0:
        raise StageFailure(code)
    return float(value)


def validate_price_range(raw: Any) -> dict:
    if (not isinstance(raw,dict) or set(raw)!=RANGE_KEYS
        or raw.get('confidence') not in ('high','medium')
        or raw.get('basis')!='last_candle_axis_bracket'):
        raise StageFailure('price_range_uncertain')
    low,high,axis_low,axis_high=(finite_positive(raw.get(k)) for k in ('low','high','axis_low','axis_high'))
    if not axis_low<=low<high<=axis_high or not axis_low<axis_high:
        raise StageFailure('price_range_uncertain')
    midpoint=low/2+high/2
    if (high-low)/midpoint>MAX_RANGE_FRACTION+1e-12:
        raise StageFailure('price_range_too_wide')
    return {'low':low,'high':high,'confidence':raw['confidence'],
            'basis':'last_candle_axis_bracket','axis_low':axis_low,'axis_high':axis_high}


def normalize_with_range(raw,*,timeframe,run_id,captured_at,image,point_validator):
    if not isinstance(raw,dict):raise StageFailure('analysis_response_invalid')
    scans=raw.get('scans')
    if not isinstance(scans,list) or len(scans)!=1 or not isinstance(scans[0],dict):
        raise StageFailure('analysis_response_invalid')
    scan=scans[0]
    proposed=scan.get('current_price_range')
    # Old outputs remain valid ONLY via the old high-confidence point validator.
    # An explicitly non-empty but invalid range never falls back to a midpoint.
    empty_range=(isinstance(proposed,dict) and proposed.get('low') is None
                 and proposed.get('high') is None and proposed.get('basis')=='unreadable'
                 and proposed.get('confidence')=='low')
    if proposed is None or empty_range:
        return point_validator(raw,timeframe=timeframe,run_id=run_id,captured_at=captured_at,image=image)
    if (timeframe not in ('12H','24H','48H') or raw.get('symbol')!='BTC'
        or raw.get('analysis_mode')!='visual_screenshot'
        or str(scan.get('timeframe','')).upper()!=timeframe):
        raise StageFailure('screenshot_identity_mismatch')
    if scan.get('readable') is not True or scan.get('blocking_condition')!='none':
        raise StageFailure('source_not_readable')
    if (scan.get('observed_symbol')!='BTC' or scan.get('observed_mode')!='Symbol'
        or scan.get('observed_model')!='Model 1'
        or scan.get('observed_timeframe')!=timeframe.lower()):
        raise StageFailure('screenshot_identity_mismatch')
    price=validate_price_range(proposed)
    model=raw.get('model')
    if not isinstance(model,str) or not model.strip() or len(model)>100:
        raise StageFailure('analysis_response_invalid')
    zones=[];seen=set();omitted=0
    for side,key in (('above','above_price'),('below','below_price')):
        group=scan.get(key)
        if not isinstance(group,dict) or not isinstance(group.get('secondary_zones'),list) or len(group['secondary_zones'])>4:
            raise StageFailure('zones_invalid')
        for zone in [group.get('main_zone'),*group['secondary_zones']]:
            if not isinstance(zone,dict):raise StageFailure('zones_invalid')
            if zone.get('low_price') is None or zone.get('high_price') is None or zone.get('confidence')=='low':
                continue
            if zone.get('confidence') not in ('high','medium'):
                raise StageFailure('zones_invalid')
            low=finite_positive(zone['low_price'],'zones_invalid')
            high=finite_positive(zone['high_price'],'zones_invalid')
            intensity=STRENGTH.get(zone.get('relative_strength'))
            if high<low or intensity is None:raise StageFailure('zones_invalid')
            identity=(low,high)
            if identity in seen:continue
            seen.add(identity)
            if low<=price['high'] and high>=price['low']:
                omitted+=1
                continue
            if (side=='above' and low<=price['high']) or (side=='below' and high>=price['low']):
                raise StageFailure('zones_invalid')
            zones.append({'side':side,'price_low':low,'price_high':high,'intensity':intensity})
    if not zones:raise StageFailure('no_unambiguous_zones')
    return {'schema_version':SCHEMA,'run_id':run_id,'source_url':SOURCE,'symbol':'BTC',
        'heatmap_model':1,'timeframe':timeframe,'captured_at':captured_at,'source_updated_at':None,
        'provider':'OpenAI','model':model,'usage':raw.get('usage'),
        'observed_price':None,'price_reference_type':'visual_range','observed_price_range':price,
        'omitted_ambiguous_zones':omitted,'zones':zones,
        'evidence':{'sha256':hashlib.sha256(image).hexdigest(),'artifact_id':run_id,'content_type':'image/png'},
        'summary':str(scan.get('short_summary',''))[:1000],'quality':'visual_estimate'}
