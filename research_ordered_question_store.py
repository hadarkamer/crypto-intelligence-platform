"""Incremental captured-data coverage, inverse provenance and input fingerprints."""
from __future__ import annotations
from collections import Counter
from datetime import timedelta
import hashlib
import json
import research_ordered_question_catalog as catalog


def canonical(value): return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,default=str,allow_nan=False)
def digest(value): return hashlib.sha256(canonical(value).encode()).hexdigest()


def register_questions(conn):
    with conn.cursor() as cur:
        cur.executemany('''INSERT INTO research_ordered_question_map(question_id,definition,map_version)
          VALUES(%s,%s::jsonb,%s) ON CONFLICT(question_id) DO UPDATE SET
          definition=EXCLUDED.definition,map_version=EXCLUDED.map_version''',
          [(q['question_id'],canonical(q),catalog.VERSION) for q in catalog.question_map()])


def new_feature_events(conn,events):
    if not events: return {}
    known={r['event_id']:r['input_sha256'] for r in conn.execute('''SELECT event_id,input_sha256
       FROM research_ordered_feature_screens WHERE event_id=ANY(%s) AND feature_version=%s''',
       (list(events),catalog.VERSION)).fetchall()}
    return {key:event for key,event in events.items() if known.get(key)!=digest(event)}


def record_screens(conn,events,features_by_id,candidates,*,now,inverse_requested):
    if not events: return {'question_screens':0,'feature_events_changed':0}
    with conn.cursor() as cur:
        cur.executemany('''INSERT INTO research_ordered_feature_screens(event_id,feature_version,input_sha256,features,checked_at_utc)
          VALUES(%s,%s,%s,%s::jsonb,%s) ON CONFLICT(event_id,feature_version) DO UPDATE SET
          input_sha256=EXCLUDED.input_sha256,features=EXCLUDED.features,checked_at_utc=EXCLUDED.checked_at_utc''',
          [(eid,catalog.VERSION,digest(event),canonical(features_by_id[eid]),now) for eid,event in events.items()])
    fingerprint=digest({'version':catalog.VERSION,'events':events,'inverse_requested':sorted(inverse_requested)})
    tests=[]
    import research_formula_ordered_v7 as evaluator
    for q in catalog.question_map():
        observations=[catalog.coverage_observation(q,features_by_id[eid],candidates) for eid in events]
        covered=Counter(key for o in observations for key in o['covered_candidates'])
        relevant=[c for c in candidates if c['formula_id'] in covered]
        matched=Counter(c['formula_id'] for c in relevant for eid,e in events.items()
            if evaluator.matches(c,features_by_id[eid],e['direction']))
        result={'question_id':q['question_id'],'feature_version':catalog.VERSION,
            'status':'PARTIAL_FEATURE_SCREEN' if covered else observations[0]['status'],
            'checked_event_ids':sorted(events),'source_scope':'LIVE_DELIVERED_ALERTS','coverage_scope':'THIS_CHANGED_INPUT_BATCH',
            'symbols_directions':dict(Counter(e['symbol']+':'+e['direction'] for e in events.values())),
            'covered_candidate_counts':dict(covered),'matched_candidate_counts':dict(matched),
            'failed_candidate_counts':{key:n-matched[key] for key,n in covered.items()},
            'missing_feature_counts':dict(Counter(key for o in observations for key in o['missing_features'])),
            'missing_step':observations[0]['missing_step'],'statistical_test_performed':False,
            'inverse_requested_source_event_ids':sorted(inverse_requested),
            'prior_history_and_regime':{'valid_prior_feature_events':sum(any(key.startswith('historical.closed_1m.') and not key.endswith('.method_version') for key in features_by_id[eid]) for eid in events),
                'range_regime_status':'NOT_DEFINED; exact return sign is reported separately'},
            'candidate_keys':sorted(covered),'overlap_groups':sorted({c.get('overlap_group',c['formula_id']) for c in relevant}),
            'candidate_component_families':{c['formula_id']:catalog.component_families(c) for c in relevant},
            'next_step':'Versioned wave-level outcomes in formula scopes; no feature screen promotes formulas'}
        tests.append((q['question_id'],fingerprint,canonical(result),now))
    with conn.cursor() as cur:
        cur.executemany('''INSERT INTO research_ordered_question_runs(question_id,input_sha256,result,checked_at_utc)
           VALUES(%s,%s,%s::jsonb,%s) ON CONFLICT(question_id,input_sha256) DO NOTHING''',tests)
    return {'question_screens':len(tests),'feature_events_changed':len(events)}


def exact_opposite_events(conn,events):
    """Only an unambiguous canonical counterpart; no clock-near heuristic."""
    if not events: return {}
    rows=conn.execute('''SELECT source.event_id AS source_event_id, target.event_id,
        target.symbol,target.direction,target.alert_time_utc,target.current_price,
        target.event_fingerprint,target.engine_snapshot->>'sheet_snapshot_id' AS sheet_snapshot_id
      FROM research_events source JOIN research_events target
        ON target.symbol=source.symbol AND target.alert_time_utc=source.alert_time_utc
        AND target.current_price=source.current_price AND target.direction<>source.direction
        AND target.direction IN ('LONG','SHORT') AND target.event_kind='ALERT'
        AND target.delivery_status='DELIVERED'
        AND target.strategy_version IS NOT DISTINCT FROM source.strategy_version
        AND target.code_version IS NOT DISTINCT FROM source.code_version
        AND (NULLIF(source.engine_snapshot->>'sheet_snapshot_id','')=NULLIF(target.engine_snapshot->>'sheet_snapshot_id','')
          OR NULLIF(source.engine_snapshot->>'watch_scan_id','')=NULLIF(target.engine_snapshot->>'watch_scan_id',''))
      WHERE source.event_id=ANY(%s)''',(list(events),)).fetchall()
    grouped={}
    for row in rows: grouped.setdefault(row['source_event_id'],[]).append(row)
    return {key:values[0] for key,values in grouped.items() if len(values)==1}


def evaluation_input(scope,rows,*,now,population_complete,membership_complete):
    # Only membership in FRESH can change without new data. The wall clock
    # itself is excluded, so unchanged results do not create duplicate trials.
    fresh_start=now-timedelta(days=14)
    from research_formula_ordered_v7 import _utc
    fresh_ids=[r['event_id'] for r in rows if _utc(r.get('parent_start_time_utc') or r['alert_time_utc'])>=fresh_start]
    ready_as_of=[]
    for row in rows:
        record=row.get('common_window_record') or {}
        try:
            if record.get('status')=='READY' and _utc(record['window_end_utc'])<=now and _utc(record['observed_at_utc'])<=now:
                ready_as_of.append(row['event_id'])
        except (KeyError,ValueError,TypeError): pass
    return digest({'scope_key':scope['scope_key'],'feature_version':catalog.VERSION,
        'rows':rows,'fresh_event_ids':fresh_ids,'common_window_ready_as_of':ready_as_of,
        'source_complete':population_complete,'membership_complete':membership_complete})


def potential_match_with_missing_history(features,candidate):
    """Unknown is distinct from a definitively failed known condition."""
    import research_formula_ordered_v7 as evaluator
    features=features or {}
    conditions=evaluator._conditions(candidate)
    def known(c):
        value=features.get(c['feature']);expected=c['value']
        if isinstance(expected,bool): return type(value) is bool
        if isinstance(expected,(int,float)): return type(value) in (int,float) and evaluator._number(value) is not None
        return isinstance(value,str)
    historical=[c for c in conditions if c['feature'].startswith('historical.')]
    if not historical or all(known(c) for c in historical):
        return False
    return evaluator._matches(features,[c for c in conditions if known(c)])


def historical_coverage_predicate(candidate):
    """Parameterized PostgreSQL three-valued feature predicate, no label inputs."""
    import research_formula_ordered_v7 as evaluator
    conditions=evaluator._conditions(candidate)
    history=[c for c in conditions if c['feature'].startswith('historical.')]
    if not history: return None,()
    params=[]
    def expected_type(value):
        return 'boolean' if isinstance(value,bool) else 'number' if isinstance(value,(int,float)) else 'string'
    def unknown(c):
        params.extend((c['feature'],c['feature'],expected_type(c['value'])))
        # A SQL NULL joined row, absent key, JSON null or wrong type all remain
        # unknown, never a failed condition or a no-signal observation.
        return "(f.features->%s IS NULL OR jsonb_typeof(f.features->%s) IS DISTINCT FROM %s)"
    missing='('+' OR '.join(unknown(c) for c in history)+')'
    possible=[]
    operators={'==':'=','>':'>','<':'<','>=':'>=','<=':'<='}
    for c in conditions:
        absent=unknown(c)
        params.extend((c['feature'],canonical(c['value'])))
        possible.append('('+absent+f" OR f.features->%s {operators[c['operator']]} %s::jsonb)")
    return missing+' AND '+' AND '.join(possible),tuple(params)


def record_scope_trial(conn,candidate,scope,result,*,input_sha,now):
    """Question coverage distinguishes the executed descriptive cell from acceptance."""
    qids=catalog.candidate_question_ids(candidate)
    if not qids: return
    complete=all(result.get(key,True) for key in ('source_population_complete','membership_population_complete','source_coverage_complete')) and not result.get('truncated')
    run={'status':'PARTIAL_ORDERED_V7_DESCRIPTIVE' if complete and result.get('decision_waves') else 'PARTIAL_BLOCKED_EVIDENCE',
        'stage':'ORDERED_V7_SCOPE_EVALUATION','scope_key':scope['scope_key'],'candidate_key':scope['candidate_key'],
        'symbol':scope['symbol'],'direction':scope['direction'],'threshold_bps':scope['threshold_bps'],'window_minutes':scope['window_minutes'],
        'period_key':scope['period_key'],'source_scope':'LIVE','feature_version':catalog.VERSION,
        'research_orientation':candidate.get('research_orientation','NORMAL'),'overlap_group':candidate.get('overlap_group',candidate['formula_id']),
        'component_families':catalog.component_families(candidate),
        'statistical_test_performed':False,'descriptive_outcome_calculation_performed':True,
        'prospective_outcome_evaluation_performed':bool(result.get('prospective_validation')),
        'acceptance_contract_status':(result.get('prospective_validation') or {}).get('standard',{}),
        'independent_waves':result.get('independent_waves'),'fresh_independent_waves':result.get('fresh_independent_waves'),
        'status_counts':result.get('status_counts'),'hit_rate_pct':result.get('hit_rate_pct'),
        'common_window_metrics':result.get('common_window_metrics'),
        'validation_status':result.get('validation_status'),'research_ready':result.get('research_ready',False),
        'exclusion_reasons':result.get('exclusion_reasons'),'prior_history_and_regime':result.get('past_price_coverage',{'status':'NOT_AVAILABLE'})}
    with conn.cursor() as cur:
        cur.executemany('''INSERT INTO research_ordered_question_runs(question_id,input_sha256,result,checked_at_utc)
          VALUES(%s,%s,%s::jsonb,%s) ON CONFLICT(question_id,input_sha256) DO NOTHING''',
          [(qid,digest([scope['scope_key'],input_sha]),canonical({**run,'question_id':qid}),now) for qid in qids])


def apply_validation_cohorts(result,validated):
    """Display probability and fixed-window metrics for the same whole-wave route."""
    whole=validated['all_period_metrics']
    fresh=whole.get('fresh') or {}
    total=whole['resolved_waves']; recent=fresh.get('resolved_waves',0)
    complete=bool(whole['source_coverage_complete'])
    route='STANDARD' if total>=5 else 'FRESH' if recent>=3 else 'INSUFFICIENT'
    active=fresh if route=='FRESH' else whole
    result.update({'independent_waves':total,'fresh_independent_waves':recent,
        'fresh_policy_version':'WHOLE_PARENT_START_IN_ROLLING_14D',
        'count_route':route if complete else 'INCOMPLETE_DECISION_POPULATION',
        'count_eligible':complete and route!='INSUFFICIENT','sample_size':active['resolved_waves'],
        'successes':active['successes'],'failures':active['failures'],
        'hit_rate_pct':active['hit_rate_pct'] if complete else None,
        'wilson_95_lower_pct':active['wilson_95_lower_pct'] if complete else None,
        'status_counts':active['status_counts'],'all_wave_status_counts':whole['status_counts'],
        'fresh_wave_status_counts':fresh.get('status_counts'),
        'status_count_scope':'FRESH_14D_WHOLE_PARENT_WAVES' if route=='FRESH' else 'ALL_SELECTED_WAVES',
        'open_waves':active['status_counts']['OPEN'],'ambiguous_waves':active['status_counts']['AMBIGUOUS'],
        'no_touch_waves':active['status_counts']['NO_TOUCH'],'data_missing_waves':active['status_counts']['DATA_MISSING'],
        'common_window_metrics':active})
