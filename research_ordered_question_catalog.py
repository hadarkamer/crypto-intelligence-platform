"""Outcome-blind, finite research map and captured feature contracts.

Every cutoff below comes from the user research map (averages 55..80,
liquidity 40/50/60/70/80) or an existing signal rule (total 65). They are
discovery predicates, never acceptance thresholds. No future path is read.
"""
from __future__ import annotations
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping

VERSION = 'captured-question-search-v1'
ROOT = Path(__file__).resolve().parent
DIRECTIONS = ('LONG', 'SHORT')


def number(value):
    if value is None or isinstance(value, bool): return None
    try: value = float(value)
    except (TypeError, ValueError, OverflowError): return None
    return value if math.isfinite(value) else None


def mapping(value): return value if isinstance(value, Mapping) else {}


def extended_features(event: Mapping[str, Any]) -> dict[str, Any]:
    """Captured values retain original names and no-signal is never inferred."""
    s = mapping(event.get('engine_snapshot'))
    f: dict[str, Any] = {}
    kind = str(event.get('event_type') or '')
    direction = event.get('direction')
    side = s.get('alert_side') or event.get('source_side')
    inverse_family = 'MAX_PAIN' in kind or kind == 'COMBINED_CONFIRMATION'
    expected = {'SHORT':'LONG', 'LONG':'SHORT'}.get(side) if inverse_family else (
        {'UPPER':'LONG','LOWER':'SHORT'}.get(mapping(s.get('magnet')).get('side')) if 'MAGNET' in kind else direction)
    current, target = number(event.get('current_price')), number(event.get('target_price'))
    target_direction = ('LONG' if target > current else 'SHORT' if target < current else 'NEUTRAL') if current and target else None
    verified = direction in DIRECTIONS and expected == direction and (not inverse_family or target_direction in (None,direction)) and not s.get('calculation_validation_errors')
    f['event.direction_mapping_valid'] = verified
    f['event.analysis_direction'] = direction
    if side: f['event.displayed_side'] = side
    if event.get('code_version'): f['event.code_version'] = str(event['code_version'])
    if event.get('strategy_version'): f['event.strategy_version'] = str(event['strategy_version'])
    if kind: f['event.event_type'] = kind
    try:
        from zoneinfo import ZoneInfo
        t = event['alert_time_utc']
        t = t if isinstance(t,datetime) else datetime.fromisoformat(str(t).replace('Z','+00:00'))
        if t.tzinfo is not None:
            local=t.astimezone(ZoneInfo('Asia/Jerusalem'))
            f.update({'time.hour_israel':local.hour,'time.weekday_israel':local.weekday(),'time.weekend':local.weekday()>=5})
    except (KeyError,ValueError,TypeError): pass
    def put(name,value):
        value=number(value)
        if value is not None: f[name]=value
    if inverse_family and verified:
        prefix='max_pain.'
        put(prefix+'selected_score',event.get('score'))
        for field in ('opposite_score','average_score_all_timeframes','opposite_average_score_all_timeframes','distance_pct','near_amount','far_amount','near_share_pct'):
            put(prefix+field,s.get(field))
        if kind=='COMBINED_CONFIRMATION':
            put(prefix+'top_item_average_score_all_timeframes',s.get('top_item_average_score_all_timeframes'))
        components=mapping(s.get('score_components') or s.get('top_item_components'))
        for key in ('directional_alignment','target_proximity','cluster_confidence','relative_gap'):
            put(prefix+'components.'+key,components.get(key))
        a,b=f.get(prefix+'selected_score'),f.get(prefix+'opposite_score')
        if a is not None and b is not None:
            put(prefix+'selected_opposite_difference',a-b)
            if b!=0: put(prefix+'selected_opposite_ratio',a/b)
        for numerator,denominator in (('consensus_hits','consensus_total'),('gap_consensus_supporting','gap_consensus_total')):
            n,d=number(s.get(numerator)),number(s.get(denominator))
            if n is not None and d is not None and n.is_integer() and d.is_integer() and 0<=n<=d and d>0:
                f[prefix+numerator],f[prefix+denominator]=int(n),int(d)
                f[prefix+numerator+'_ratio']=n/d
                f[prefix+numerator+'_full']=n==d
        for display_side,values in mapping(s.get('directional_scores_all_timeframes')).items():
            if display_side not in DIRECTIONS: continue
            for tf,score in mapping(values).items():
                if tf in ('15m','30m','1h','4h','12h','24h','1d','1w','1m'):
                    put(prefix+'tf.'+display_side+'.'+tf,score)
        selected_tf=mapping(mapping(s.get('directional_scores_all_timeframes')).get(side))
        opposite_tf=mapping(mapping(s.get('directional_scores_all_timeframes')).get({'LONG':'SHORT','SHORT':'LONG'}.get(side)))
        for tf in ('15m','30m','1h','4h','12h','24h'):
            a,b=number(selected_tf.get(tf)),number(opposite_tf.get(tf))
            if a is not None and b is not None:
                f[prefix+'tf.'+tf+'.selected_dominates']=a>b
        conf=mapping(s.get('maxpain_confirmation') or s.get('top_item_confirmation'))
        if conf.get('status'): f['captured.maxpain_confirmation_status']=str(conf['status'])
        # near_amount is _liquidity_balance(selected.near_amount,far_amount),
        # and native source-side inversion above binds selected to target price.
        # Combined list entries lack the same amount contract; keep separate.
        share=number(s.get('near_share_pct'))
        near,far=number(s.get('near_amount')),number(s.get('far_amount'))
        valid_amounts=near is not None and far is not None and near>=0 and far>=0 and near+far>0
        if share is not None and 0<=share<=100 and valid_amounts and math.isclose(share,100*near/(near+far),abs_tol=0.05):
            f.update({'liquidity.selected_share_pct':share,'liquidity.selected_amount':near,'liquidity.opposite_amount':far,
                'liquidity.source_contract':'MAX_PAIN_CAPTURED_AMOUNTS_V1','liquidity.alignment':'SUPPORTS' if share>=60 else 'OPPOSES' if share<=40 else 'BALANCED',
                'liquidity.capture_status':'VALID'})
        else:
            f['liquidity.capture_status']='MISSING' if share is None and near is None and far is None else 'UNVERIFIED'
    magnet=mapping(s.get('magnet'))
    if verified and magnet:
        for key in ('magnet_quality','liquidity_edge_pct','count','spread_pct','average_target'):
            put('captured.magnet.'+key,magnet.get(key))
        mside=magnet.get('side')
        if mside in ('UPPER','LOWER'): f['captured.magnet.analysis_direction']={'UPPER':'LONG','LOWER':'SHORT'}[mside]
        status=mapping(s.get('magnet_confirmation')).get('status')
        if status: f['captured.magnet.confirmation_status']=str(status)
    if kind=='COMBINED_CONFIRMATION' and verified:
        put('captured.combined.signal_count',s.get('signal_count'))
        for key in ('normal_confirmations','strong_confirmations','high_scores','anomaly_setups','liquidity_imbalances'):
            if isinstance(s.get(key),list): f['captured.combined.'+key+'_count']=len(s[key])
    if verified and current and target:
        put('captured.target_distance_pct',100*abs(target/current-1))
        f['captured.target_direction']=target_direction
    return f


def condition(feature,operator,value): return {'feature':feature,'operator':operator,'value':value}


def candidates() -> list[dict[str,Any]]:
    out=[]
    def add(key,qs,conditions,group,justification):
        out.append({'formula_id':VERSION+':'+key,'conditions':conditions,'repeat_count':1,
            'catalog_version':VERSION,'question_ids':qs,'overlap_group':group,'justification':justification,
            'research_orientation':'NORMAL','discovery_only':True})
    valid=condition('event.direction_mapping_valid','==',True)
    for state in ('SUPPORTS','OPPOSES','BALANCED'):
        add('LIQUIDITY_'+state,['Q01','Q05'],[valid,condition('liquidity.alignment','==',state)],'LIQUIDITY','Compare valid captured selected-side shares under one source contract')
    # Missing is a coverage diagnostic only, never a tradable no-signal formula.
    for lo,hi in ((0,40),(40,50),(50,60),(60,70),(70,80),(80,101)):
        add(f'LIQUIDITY_{lo}_{hi}',['Q02'],[valid,condition('liquidity.selected_share_pct','>=',lo),condition('liquidity.selected_share_pct','<',hi)],'LIQUIDITY','User-requested disjoint ranges; not presumed monotonic')
    averages=('average_score_all_timeframes','opposite_average_score_all_timeframes','top_item_average_score_all_timeframes')
    for name in averages:
        for cutoff in (55,60,65,70,75,80):
            add(f'{name}_GE{cutoff}',['Q07','Q10','Q31'],[valid,condition('max_pain.'+name,'>=',cutoff)],'MAX_PAIN_AVERAGE:'+name,'User-requested exact average field, never selected TF fallback')
        for lo,hi in zip((55,60,65,70,75),(60,65,70,75,80)):
            add(f'{name}_{lo}_{hi}',['Q07'],[valid,condition('max_pain.'+name,'>=',lo),condition('max_pain.'+name,'<',hi)],'MAX_PAIN_AVERAGE:'+name,'Intermediate bands test nonmonotonic response')
    for field in ('selected_opposite_difference','components.directional_alignment','components.target_proximity','components.cluster_confidence','components.relative_gap'):
        add(field+'_POS',['Q09','Q26','Q31'],[valid,condition('max_pain.'+field,'>',0)],'MAX_PAIN_COMPONENT:'+field,'Positive component versus zero; component retains its original name')
    for full in (True,False):
        add('CONSENSUS_'+str(full),['Q25'],[valid,condition('max_pain.consensus_hits_full','==',full)],'MAX_PAIN_CONSENSUS','Complete versus partial valid hits/total; denominator retained')
    for tf in ('15m','1h','4h'):
        add('TF_'+tf+'_SELECTED_DOMINATES',['Q23'],[valid,condition('max_pain.tf.'+tf+'.selected_dominates','==',True)],'MAX_PAIN_TF','Compare actual selected/opposite TF scores separately from family total')
    add('TF_15m_1h_4h_AGREE',['Q23'],[valid]+[condition('max_pain.tf.'+tf+'.selected_dominates','==',True) for tf in ('15m','1h','4h')],'MAX_PAIN_TF','User-requested short/mid/long TF conjunction')
    for family in ('maxpain','magnet'):
        key='captured.maxpain_confirmation_status' if family=='maxpain' else 'captured.magnet.confirmation_status'
        for status in ('CONFIRMED','STRONG_CONFIRMED','NOT_CONFIRMED','OBSERVATION'):
            add(f'{family}_{status}',['Q11','Q20'],[valid,condition(key,'==',status)],'CONFIRMATION:'+family,'Captured status labels only; missing is not NOT_CONFIRMED')
    add('COMBINED',['Q03','Q20'],[valid,condition('event.event_type','==','COMBINED_CONFIRMATION')],'COMBINED','Baseline for paired liquidity test')
    for family in ('price_oi','futures_cvd','spot_cvd'):
        for state in ('SUPPORTS','OPPOSES'):
            add(f'{family}_65_LIQUIDITY_{state}',['Q04','Q57'],[condition(family+'.aligned_score','>=',65),valid,condition('liquidity.alignment','==',state)],'TOTAL_LIQUIDITY:'+family,'Prespecified total65 baseline plus valid directional liquidity')
    add('SPOT65_FUTURES_OPPOSES',['Q34'],[condition('spot_cvd.aligned_score','>=',65),condition('futures_cvd.aligned_score','<=',-65)],'CVD_CONTRADICTION','Separate captured Spot/Futures total direction')
    add('COMBINED_LIQUIDITY_SUPPORTS',['Q03','Q57'],[valid,condition('event.event_type','==','COMBINED_CONFIRMATION'),condition('liquidity.alignment','==','SUPPORTS')],'COMBINED','Exact combined baseline plus valid selected-side liquidity')
    add('GAP_PROXIMITY_CONSENSUS',['Q27','Q57'],[valid,condition('max_pain.components.relative_gap','>',0),condition('max_pain.components.target_proximity','>',0),condition('max_pain.consensus_hits_full','==',True)],'MAX_PAIN_GAP_PROXIMITY_CONSENSUS','User-requested conjunction; original component scores, not score differences')
    add('TRIPLE65_LIQUIDITY',['Q04','Q57'],[condition(x+'.aligned_score','>=',65) for x in ('price_oi','futures_cvd','spot_cvd')]+[condition('liquidity.alignment','==','SUPPORTS')],'TOTAL_TRIPLE_LIQUIDITY','User-requested liquidity increment over frozen triple65')
    # Simple time splits are fixed before outcomes, with all other metrics separate.
    for weekend in (True,False):
        add('WEEKEND_'+str(weekend),['Q16'],[condition('time.weekend','==',weekend)],'TIME_WEEKEND','Descriptive weekday/weekend population; no acceptance relaxation')
    for window in ('15m','30m','1h','4h','12h','24h'):
        for state in ('UP','DOWN','FLAT'):
            add('PRIOR_'+window+'_'+state,['Q46'],[condition('historical.closed_1m.'+window+'.direction','==',state)],'PRIOR_RETURN:'+window,'Exact sign of closed prior Spot return; FLAT means exactly zero, not a fabricated range regime')
    for state in ('UP','DOWN','FLAT'):
        add('BTC_PRIOR_1h_'+state,['Q52'],[condition('historical.closed_1m.1h.btc_direction','==',state)],'BTC_PRIOR_RETURN','BTC closed prior1h sign measured at the same entry cutoff')
    for operator,state in (('>','STRONGER'),('<','WEAKER')):
        add('RELATIVE_BTC_1h_'+state,['Q53'],[condition('historical.closed_1m.1h.relative_strength_pct',operator,0)],'RELATIVE_BTC','Difference of same-window closed coin and BTC returns')
    for alignment in ('SUPPORTS','OPPOSES','FLAT'):
        add('SPOT65_PRIOR_1h_'+alignment,['Q49','Q57'],[condition('spot_cvd.aligned_score','>=',65),condition('historical.closed_1m.1h.alignment','==',alignment)],'SPOT_PRIOR_RETURN','Frozen Spot65 condition with or against completed prior1h move')
    return sorted(out,key=lambda c:(len(c['conditions']),c['question_ids'][0],c['formula_id']))


def inverse_candidates(base):
    return [{**c,'formula_id':VERSION+':INVERSE:'+c['formula_id'],
        'question_ids':sorted(set(candidate_question_ids(c))|{'Q43','Q59'}),
        'base_candidate_key':c['formula_id'],'research_orientation':'INVERSE',
        'overlap_group':c.get('overlap_group',c['formula_id']),
        'catalog_version':VERSION} for c in base]


def candidate_question_ids(candidate):
    # Metadata lives outside the old seven immutable definitions.
    if candidate.get('question_ids'): return candidate['question_ids']
    if candidate.get('catalog_version')=='ordered-v7-total-score-simple-pairs-strict65-v1':
        return ['Q08','Q32','Q57'] if len(candidate['conditions'])>1 else ['Q32','Q57']
    return []


def component_families(candidate):
    """Overlapping versions share components even when their keys differ."""
    return sorted({item['feature'].split('.',1)[0] for item in candidate['conditions']
        if not item['feature'].startswith('event.')})


def question_map():
    return json.loads((ROOT/'docs'/'ordered_research_question_map.json').read_text(encoding='utf-8'))


def coverage_observation(question,features,catalog):
    relevant=[c for c in catalog if question['question_id'] in candidate_question_ids(c)]
    normal=[c for c in relevant if c.get('research_orientation')!='INVERSE']
    required={x['feature'] for c in normal for x in c['conditions']}
    present=sorted(required & features.keys())
    missing=sorted(required-features.keys())
    covered=[c['formula_id'] for c in normal if all(x['feature'] in features for x in c['conditions'])]
    return {'question_id':question['question_id'],'status':'PARTIAL_FEATURE_SCREEN' if covered else 'BLOCKED_FEATURE_COVERAGE' if relevant else 'BLOCKED_PIPELINE_STEP',
        'required_features':sorted(required),'present_features':present,'missing_features':missing,'covered_candidates':covered,
        'missing_step':('GOAT average and separately defined engine average are not present in captured schema' if question['question_id']=='Q07' else None if relevant else question['missing_step']),'statistical_test_performed':False,
        'inverse_status':'OUTCOMES_IN_SEPARATE_CANONICAL_QUEUE' if any(c.get('research_orientation')=='INVERSE' for c in relevant) else 'NOT_APPLICABLE',
        'regime_status':'RANGE_REGIME_NOT_DEFINED','prior_movement_status':'PARTIAL_CLOSED_PRIOR_RETURNS' if any(key.startswith('historical.closed_1m.') and not key.endswith('.method_version') for key in features) else 'BLOCKED_PRIOR_PRICE_HISTORY'}
