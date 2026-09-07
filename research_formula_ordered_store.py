"""Incremental, bounded persistence for frozen-score ordered-v7 research."""
from __future__ import annotations
import hashlib
import json
from datetime import datetime, timezone, timedelta
from typing import Any, Mapping
import research_formula_ordered_v7 as evaluator
import research_sheet_outbox
import research_ordered_question_catalog as questions
import research_ordered_question_store as question_store

PARENT_POLICY = 'btc-parent-close-reversal-200bps-v1'
WORKER_KEY = 'ordered-v7-captured-questions-periods-v3'
FORMULA_VERSION = evaluator.POLICY_VERSION + ':' + evaluator.CATALOG_VERSION + ':' + PARENT_POLICY
PERIOD_VERSION = 'live-israel-cutoffs-v1'
PERIODS = {
    'ALL_COMPATIBLE_SINCE_20260816': datetime(2026,8,15,21,tzinfo=timezone.utc),
    'SINCE_20260904': datetime(2026,9,3,21,tzinfo=timezone.utc),
}
SOURCE_START_UTC = min(PERIODS.values())
REQUIRED_TABLES = ('research_ordered_formula_event_checks','research_ordered_formula_candidates','research_ordered_formula_matches','research_ordered_formula_scopes','research_ordered_formula_episodes','research_ordered_formula_trials','research_ordered_formula_worker_state','research_sheet_upsert_outbox','research_event_btc_movements','research_btc_parent_movements','research_ordered_first_touch_outcomes','research_ordered_question_map','research_ordered_feature_screens','research_ordered_question_runs','research_event_scan_cursors','research_ordered_scope_schedule_state')


def canonical(value: Any) -> str:
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,default=str,allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def schema_status(conn: Any) -> dict[str, Any]:
    missing = [name for name in REQUIRED_TABLES if conn.execute('SELECT to_regclass(%s) AS relation',(name,)).fetchone()['relation'] is None]
    if not missing:
        columns = {row['column_name'] for row in conn.execute("SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='research_ordered_formula_scopes'").fetchall()}
        missing.extend('research_ordered_formula_scopes.'+name for name in ('period_key','period_start_utc') if name not in columns)
    return {'schema_present':not missing,'missing_tables':missing}


def period_contract(scope: Mapping[str,Any]) -> dict[str,Any]:
    key = scope.get('period_key')
    if key not in PERIODS:
        raise ValueError('An explicit supported LIVE research period is required')
    cutoff = PERIODS[key]
    if scope.get('period_start_utc') is not None and evaluator._utc(scope['period_start_utc']) != cutoff:
        raise ValueError('Persisted period cutoff does not match its versioned definition')
    return {'period_key':key,'period_start_utc':cutoff.isoformat(),
            'period_version':PERIOD_VERSION,'source_scope':'LIVE',
            'boundary_policy':'Exclude parent waves starting before cutoff; retain all alert outcomes separately',
            'overlap_policy':'Periods overlap and are never additional independent evidence'}


def scope_formula_version(scope: Mapping[str,Any]) -> str:
    prefix = FORMULA_VERSION if not str(scope['candidate_key']).startswith(questions.VERSION+':') else evaluator.POLICY_VERSION+':'+questions.VERSION+':'+PARENT_POLICY
    return prefix + ':' + PERIOD_VERSION + ':' + period_contract(scope)['period_key']


def register_catalog(conn: Any) -> list[dict[str, Any]]:
    catalog = evaluator.candidate_catalog(include_extended=True)
    question_store.register_questions(conn)
    records = []
    for candidate in catalog:
        key = candidate['formula_id']
        version = FORMULA_VERSION if candidate.get('catalog_version')==evaluator.CATALOG_VERSION else evaluator.POLICY_VERSION+':'+questions.VERSION+':'+PARENT_POLICY
        definition = {'candidate':candidate,'formula_version':version,'parent_policy_version':PARENT_POLICY}
        sha = digest(definition)
        records.append((key,version,sha,canonical(definition)))
    if records:
        with conn.cursor() as cur:
            cur.executemany('''INSERT INTO research_ordered_formula_candidates(candidate_key,formula_version,definition_sha256,definition)
                VALUES(%s,%s,%s,%s::jsonb) ON CONFLICT(candidate_key) DO NOTHING''',records)
        found = {row['candidate_key']:row['definition_sha256'] for row in conn.execute(
            'SELECT candidate_key,definition_sha256 FROM research_ordered_formula_candidates WHERE candidate_key=ANY(%s)',
            ([row[0] for row in records],)).fetchall()}
    else:
        found = {}
    for key, _version, sha, _definition in records:
        if found.get(key) != sha:
            raise ValueError('Frozen candidate definition changed; use a new versioned key')
    return catalog


def register_scopes(conn: Any,catalog: list[dict[str,Any]],symbols: set[str],*,directions=None,include_all:bool=True,period_keys=None) -> int:
    records = []
    for candidate in catalog:
        for symbol in sorted(symbols | ({'ALL'} if include_all else set())):
            for direction in (directions or evaluator.DIRECTIONS):
                for horizon in evaluator.HORIZONS_MINUTES:
                    for bps in evaluator.THRESHOLDS_BPS:
                        for period_key in (period_keys or PERIODS):
                            contract = period_contract({'period_key':period_key})
                            version=scope_formula_version({'candidate_key':candidate['formula_id'],'period_key':period_key})
                            # Keep original seven scope keys byte-for-byte stable.
                            identity = (candidate['formula_id'],symbol,direction,horizon,bps,FORMULA_VERSION if not candidate['formula_id'].startswith(questions.VERSION+':') else version,PERIOD_VERSION,period_key,'LIVE')
                            records.append((digest(identity),*identity[:5],period_key,contract['period_start_utc']))
    if records:
        with conn.cursor() as cur:
            cur.executemany('''INSERT INTO research_ordered_formula_scopes(scope_key,candidate_key,symbol,direction,window_minutes,threshold_bps,period_key,period_start_utc)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING''',records)
    return len(records)


# Fetch compact module totals only after limiting source IDs. No price-path or
# full decision-bundle JSON is materialized in the Python worker.
_EVENT_PROJECT = '''
SELECT e.event_id,e.symbol,e.direction,e.alert_time_utc,e.event_fingerprint,
       e.current_price,e.event_type,e.event_kind,e.delivery_status,e.score,e.source_side,
       e.target_price,e.timeframe,e.strategy_version,e.code_version,
       jsonb_build_object('sheet_snapshot_id',projected.fields->'sheet_snapshot_id',
           'market_evidence',jsonb_build_object('modules',jsonb_build_object(
             'positioning',jsonb_build_object('score',projected.fields#>'{market_evidence,modules,positioning,score}','direction',projected.fields#>'{market_evidence,modules,positioning,direction}'),
             'futures_flow',jsonb_build_object('score',projected.fields#>'{market_evidence,modules,futures_flow,score}','direction',projected.fields#>'{market_evidence,modules,futures_flow,direction}'),
             'spot_flow',jsonb_build_object('score',projected.fields#>'{market_evidence,modules,spot_flow,score}','direction',projected.fields#>'{market_evidence,modules,spot_flow,direction}'))))
       || jsonb_strip_nulls(jsonb_build_object(
           'watch_scan_id',projected.fields->'watch_scan_id','alert_side',projected.fields->'alert_side',
           'score_components',projected.fields->'score_components','opposite_score',projected.fields->'opposite_score',
           'calculation_validation_errors',projected.fields->'calculation_validation_errors',
           'average_score_all_timeframes',projected.fields->'average_score_all_timeframes',
           'opposite_average_score_all_timeframes',projected.fields->'opposite_average_score_all_timeframes',
           'directional_scores_all_timeframes',projected.fields->'directional_scores_all_timeframes',
           'top_item_average_score_all_timeframes',projected.fields->'top_item_average_score_all_timeframes',
           'top_item_components',projected.fields->'top_item_components','top_item_confirmation',projected.fields->'top_item_confirmation',
           'consensus_hits',projected.fields->'consensus_hits','consensus_total',projected.fields->'consensus_total',
           'gap_consensus_supporting',projected.fields->'gap_consensus_supporting','gap_consensus_total',projected.fields->'gap_consensus_total',
           'distance_pct',projected.fields->'distance_pct','near_amount',projected.fields->'near_amount','far_amount',projected.fields->'far_amount',
           'near_share_pct',projected.fields->'near_share_pct','magnet',projected.fields->'magnet',
           'magnet_confirmation',projected.fields->'magnet_confirmation','maxpain_confirmation',projected.fields->'maxpain_confirmation',
           'signal_count',projected.fields->'signal_count','normal_confirmations',projected.fields->'normal_confirmations',
           'strong_confirmations',projected.fields->'strong_confirmations','high_scores',projected.fields->'high_scores',
           'anomaly_setups',projected.fields->'anomaly_setups','liquidity_imbalances',projected.fields->'liquidity_imbalances')) AS engine_snapshot
FROM picked JOIN research_events e USING(event_id)
CROSS JOIN LATERAL (
    -- Extract archived fields together, retaining only the small fields
    -- used by formula extraction. Repeated paths below use this compact value.
    SELECT COALESCE(jsonb_object_agg(field.key,
        CASE WHEN field.key='market_evidence' THEN
            jsonb_build_object('modules',jsonb_build_object('positioning',jsonb_build_object('score',field.value#>'{modules,positioning,score}','direction',field.value#>'{modules,positioning,direction}'),'futures_flow',jsonb_build_object('score',field.value#>'{modules,futures_flow,score}','direction',field.value#>'{modules,futures_flow,direction}'),'spot_flow',jsonb_build_object('score',field.value#>'{modules,spot_flow,score}','direction',field.value#>'{modules,spot_flow,direction}')))
        ELSE field.value END),'{}'::jsonb) AS fields
    FROM jsonb_each(CASE WHEN jsonb_typeof(e.engine_snapshot)='object'
        THEN e.engine_snapshot ELSE '{}'::jsonb END) AS field(key,value)
    WHERE field.key IN ('sheet_snapshot_id','watch_scan_id','alert_side','score_components','opposite_score','calculation_validation_errors','average_score_all_timeframes','opposite_average_score_all_timeframes','directional_scores_all_timeframes','top_item_average_score_all_timeframes','top_item_components','top_item_confirmation','consensus_hits','consensus_total','gap_consensus_supporting','gap_consensus_total','distance_pct','near_amount','far_amount','near_share_pct','magnet','magnet_confirmation','maxpain_confirmation','signal_count','normal_confirmations','strong_confirmations','high_scores','anomaly_setups','liquidity_imbalances','market_evidence')
) projected
ORDER BY e.event_id
'''


def load_sequence_history(conn:Any,changed:Mapping[int,Mapping[str,Any]])->tuple[list[dict[str,Any]],dict[str,int]]:
    """Keep the original source bound; project only causally relevant history."""
    stats={'sequence_source_rows':0,'sequence_projected_rows':0,'sequence_projection_batches':0}
    if not changed: return [],stats
    lower=min(event['alert_time_utc'] for event in changed.values())-timedelta(hours=4)
    upper=max(event['alert_time_utc'] for event in changed.values())
    # Select the same ordered source set as before, without detoasting JSON.
    # Check the original global cap before removing irrelevant source rows.
    source=conn.execute('''SELECT event_id,symbol,direction,alert_time_utc
        FROM research_events WHERE alert_time_utc>=%s AND alert_time_utc<=%s
          AND event_kind='ALERT' AND delivery_status='DELIVERED' AND direction IN ('LONG','SHORT')
        ORDER BY event_id LIMIT 5001''',(lower,upper)).fetchall()
    stats['sequence_source_rows']=len(source)
    if len(source)>5000:
        raise RuntimeError('bounded causal sequence source exceeded; paginate before evaluating')
    windows={}
    for event in changed.values():
        # These are the stored direction/symbol returned by _EVENT_PROJECT;
        # inverse candidate direction is assigned only after sequence capture.
        windows.setdefault((event['symbol'],event['direction']),[]).append(event['alert_time_utc'])
    ids=[old['event_id'] for old in source if any(
        when-timedelta(hours=4)<=old['alert_time_utc']<when
        for when in windows.get((old['symbol'],old['direction']),()))]
    source_by_id={old['event_id']:old for old in source}
    history=[]
    for start in range(0,len(ids),64):
        batch=ids[start:start+64]
        projected=conn.execute('''WITH picked AS MATERIALIZED (
            SELECT unnest(%s::bigint[]) AS event_id) '''+_EVENT_PROJECT,(batch,)).fetchall()
        if [event['event_id'] for event in projected]!=batch or any(
            any(event[key]!=source_by_id[event['event_id']][key]
                for key in ('symbol','direction','alert_time_utc')) for event in projected):
            raise RuntimeError('bounded causal sequence projection lost or changed source rows')
        history.extend(projected)
        stats['sequence_projection_batches']+=1
    stats['sequence_projected_rows']=len(history)
    return history,stats


def recent_unscreened_event_ids(conn,*,now:datetime,limit:int=32)->list[int]:
    """Inspect a finite live tail before filtering already screened siblings.

    A late Magnet burst must not repeatedly occupy all 32 live slots while
    earlier Max Pain/CVD alerts from the same scan wait on historical replay.
    The ascending history cursor still recovers everything outside this tail.
    """
    rows=conn.execute('''WITH recent_source AS MATERIALIZED (
        SELECT event_id FROM research_events
        WHERE alert_time_utc>=%s AND alert_time_utc<=%s
          AND event_kind='ALERT' AND delivery_status='DELIVERED'
          AND direction IN ('LONG','SHORT')
        ORDER BY event_id DESC LIMIT 256
    ) SELECT recent.event_id FROM recent_source recent
      WHERE NOT EXISTS (
          SELECT 1 FROM research_ordered_feature_screens checked
          WHERE checked.event_id=recent.event_id AND checked.feature_version=%s
          LIMIT 1 OFFSET 0
      ) ORDER BY recent.event_id DESC LIMIT %s''',
      (SOURCE_START_UTC,now,questions.VERSION,max(1,min(32,int(limit))))).fetchall()
    return [int(row['event_id']) for row in rows]


def _missing_scope_cells(conn,cells):
    """Probe each expected cell's 32nd unique scope, including partial repair."""
    missing=set()
    cells=sorted(cells)
    for start in range(0,len(cells),128):
        batch=cells[start:start+128]
        placeholders=','.join(['(%s,%s,%s,%s)']*len(batch))
        expected=len(evaluator.HORIZONS_MINUTES)*len(evaluator.THRESHOLDS_BPS)
        rows=conn.execute('''WITH requested(candidate_key,symbol,direction,period_key)
            AS (VALUES '''+placeholders+''')
            SELECT * FROM requested r WHERE NOT EXISTS (
                SELECT 1 FROM research_ordered_formula_scopes s
                WHERE s.candidate_key=r.candidate_key AND s.symbol=r.symbol
                  AND s.direction=r.direction AND s.period_key=r.period_key
                  AND s.window_minutes=ANY(%s) AND s.threshold_bps=ANY(%s)
                LIMIT 1 OFFSET '''+str(expected-1)+')',
                (*[value for cell in batch for value in cell],
                 list(evaluator.HORIZONS_MINUTES),list(evaluator.THRESHOLDS_BPS))).fetchall()
        missing.update((r['candidate_key'],r['symbol'],r['direction'],r['period_key']) for r in rows)
    return missing


def discover_scope_cells(conn,catalog,observed_cells=(),*,page_limit=128,cell_limit=8):
    """Drain finite historical pages alongside newly matched formula cells.

    Creating a cell inserts all horizon/threshold scopes in this transaction.
    A page stays pending until every missing historical cell has been created;
    rollback also replays its cursor. Later laps recover matches inserted for
    older source IDs, including prior catalog upgrades and late delivery.
    """
    from research_event_scan import claim_event_page,retain_unprocessed_tail
    candidates={candidate['formula_id']:candidate for candidate in catalog}
    queue=questions.VERSION+':scope-cell-discovery-v1'
    ids=claim_event_page(conn,queue,limit=min(128,max(1,int(page_limit))),predicate='TRUE')
    rows=(conn.execute('''SELECT DISTINCT candidate_key,symbol,direction
        FROM research_ordered_formula_matches
        WHERE event_id=ANY(%s) AND candidate_key=ANY(%s) AND alert_time_utc>=%s''',
        (ids,list(candidates),SOURCE_START_UTC)).fetchall() if ids and candidates else [])
    historical={(row['candidate_key'],row['symbol'],row['direction']) for row in rows}
    def periods(cells):
        return {(key,scope_symbol,direction,period) for key,symbol,direction in cells
            if key in candidates for scope_symbol in (symbol,'ALL') for period in PERIODS}
    historical=periods(historical)
    fresh=periods(observed_cells)
    pending=_missing_scope_cells(conn,historical|fresh)
    rank=lambda cell:(len(candidates[cell[0]]['conditions']),cell)
    old=sorted(pending&historical,key=rank)
    new=sorted(pending-historical,key=rank)
    # A continuous stream of newly matched cells cannot starve the retained
    # historical page. Its lane gets the first slot even with a one-cell cap.
    selected=_interleave(old,new,min(8,max(1,int(cell_limit))))
    for candidate_key,symbol,direction,period_key in selected:
        register_scopes(conn,[candidates[candidate_key]],{symbol},directions=[direction],
            include_all=False,period_keys=[period_key])
    retained=bool(set(old)-set(selected))
    if retained:
        retain_unprocessed_tail(conn,queue,ids[0]-1)
    return {'scope_cells_created':len(selected),'scope_cells_waiting':len(pending)-len(selected),
        'scope_cells_waiting_scope':'CURRENT_DISCOVERY_PAGE_AND_OBSERVED_BATCH',
        'scope_discovery_source_ids':len(ids),'scope_discovery_page_retained':retained,
        'scope_discovery_cursor':(ids[0]-1 if retained else ids[-1]) if ids else 0}


def ingest_matches(conn: Any,catalog: list[dict[str,Any]],*,now: datetime,event_limit: int=128) -> dict[str,Any]:
    conn.execute('INSERT INTO research_ordered_formula_worker_state(worker_key) VALUES(%s) ON CONFLICT DO NOTHING',(WORKER_KEY,))
    cursor = conn.execute('SELECT last_event_id FROM research_ordered_formula_worker_state WHERE worker_key=%s FOR UPDATE',(WORKER_KEY,)).fetchone()['last_event_id']
    since = SOURCE_START_UTC
    events = conn.execute('''WITH picked AS MATERIALIZED (
        SELECT event_id FROM research_events WHERE event_id>%s AND alert_time_utc>=%s
          AND alert_time_utc<=%s AND event_kind='ALERT' AND delivery_status='DELIVERED'
          AND direction IN ('LONG','SHORT') ORDER BY event_id LIMIT %s
        ) ''' + _EVENT_PROJECT,(cursor,since,now,event_limit)).fetchall()
    # Project only unscreened current-version events from the bounded live
    # tail. Immutable older inputs and delayed transitions retain the existing
    # ascending/wrap-around replay; past-feature changes have their own queue.
    recent_ids=recent_unscreened_event_ids(conn,now=now,limit=event_limit)
    recent=(conn.execute('''WITH picked AS MATERIALIZED (
        SELECT unnest(%s::bigint[]) AS event_id
        ) ''' + _EVENT_PROJECT,(recent_ids,)).fetchall() if recent_ids else [])
    conn.execute('UPDATE research_ordered_formula_worker_state SET last_event_id=%s,initial_scan_complete=initial_scan_complete OR %s,updated_at_utc=NOW() WHERE worker_key=%s',
                 (max(row['event_id'] for row in events) if events else 0,not events,WORKER_KEY))
    records, symbols, missing_features = [], set(), 0
    unique = {event['event_id']:event for event in [*events,*recent]}
    import research_past_price_features_store as past_store
    import research_past_price_features as past_features
    past_refresh=[]
    if past_store.available(conn):
        past_refresh=past_store.pending_refresh_ids(conn,limit=32)
        if past_refresh:
            refreshed=conn.execute('''WITH picked AS MATERIALIZED (
                SELECT event_id FROM research_events WHERE event_id=ANY(%s::bigint[])
                  AND alert_time_utc>=%s AND alert_time_utc<=%s AND event_kind='ALERT'
                  AND delivery_status='DELIVERED' AND direction IN ('LONG','SHORT')
                ) '''+_EVENT_PROJECT,([row['event_id'] for row in past_refresh],since,now)).fetchall()
            unique.update({event['event_id']:event for event in refreshed})
        past=past_store.load_by_event_ids(conn,list(unique))
        for event_id,event in unique.items():
            if event_id in past:
                # Computed later is allowed only because this adapter validates
                # closed source candles at/before the original decision cutoff.
                event['causal_past_features']=past_features.flatten_event_features(past[event_id],event['direction'])
                event['causal_past_feature_sha256']=past[event_id].get('feature_sha256')
    changed = question_store.new_feature_events(conn,unique)
    sequence_source={}
    history,sequence_stats=load_sequence_history(conn,changed)
    for prior in history:
        base=evaluator.extract_event_features(prior)
        base.update(questions.extended_features(prior))
        sequence_source[prior['event_id']]=(prior,base)
    features_by_id, inverse_requests = {}, set()
    for event in changed.values():
        symbols.add(event['symbol'])
        features = evaluator.extract_event_features(event)
        features.update(questions.extended_features(event))
        features.update(event.get('causal_past_features') or {})
        features.update(questions.sequence_features(event,features,[value for key,value in sequence_source.items() if key!=event['event_id']]))
        features_by_id[event['event_id']]=features
        if not any(name.endswith('.aligned_score') and value is not None for name,value in features.items()):
            missing_features += 1
        for candidate in catalog:
            if evaluator.matches(candidate,features,event['direction']):
                snapshot = event.get('engine_snapshot') or {}
                inverse=candidate.get('research_orientation')=='INVERSE'
                if inverse: inverse_requests.add(event['event_id'])
                direction=({'LONG':'SHORT','SHORT':'LONG'}[event['direction']] if inverse else event['direction'])
                records.append((candidate['formula_id'],event['event_id'],event['symbol'],direction,event['alert_time_utc'],
                                snapshot.get('sheet_snapshot_id') or event['event_fingerprint'],event['current_price'],canonical(features)))
    screens=question_store.record_screens(conn,changed,features_by_id,catalog,now=now,inverse_requested=inverse_requests)
    for refresh in past_refresh:
        # ACK is conditional on the captured version and commits atomically
        # with screens/matches. Ignored non-LIVE sources do not jam this queue.
        past_store.ack_refresh(conn,refresh['event_id'],refresh['feature_sha256'])
    if unique:
        with conn.cursor() as cur:
            cur.executemany('INSERT INTO research_ordered_formula_event_checks(event_id) VALUES(%s) ON CONFLICT DO NOTHING',[(event_id,) for event_id in unique])
    if records:
        with conn.cursor() as cur:
            cur.executemany('''INSERT INTO research_ordered_formula_matches(candidate_key,event_id,symbol,direction,alert_time_utc,snapshot_id,entry_price,decision_features)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s::jsonb) ON CONFLICT(candidate_key,event_id) DO NOTHING''',records)
    scope_discovery=discover_scope_cells(conn,catalog,
        {(record[0],record[2],record[3]) for record in records})
    return {'events_checked':len(changed),'fresh_unscreened_events_selected':len(recent_ids),'screen_feature_version':questions.VERSION,'events_unchanged_skipped':len(unique)-len(changed),'missing_total_score_features':missing_features,'matches_observed':len(records),'symbols':sorted(symbols),'cursor':max((e['event_id'] for e in events),default=0),'source_start_utc':since.isoformat(),'inverse_source_event_ids':sorted(inverse_requests),**screens,**sequence_stats,**scope_discovery}


class ScopeScheduleBatch(list):
    """Keep scheduling metadata outside immutable scope evidence dictionaries."""
    def __init__(self,rows,ticket):
        super().__init__(rows)
        self.schedule_ticket=ticket


def _interleave(first,second,limit):
    selected=[]
    for index in range(max(len(first),len(second))):
        for lane in (first,second):
            if index<len(lane):
                selected.append(lane[index])
                if len(selected)>=limit:
                    return selected
    return selected


def due_scopes(conn:Any,limit:int=64,*,candidate_keys=None)->list[dict[str,Any]]:
    keys=list(candidate_keys or ())
    if not keys or limit<1:
        return ScopeScheduleBatch([],0)
    cap=max(1,min(512,int(limit)))
    scheduler=WORKER_KEY+':'+questions.VERSION+':fair-scopes-v1'
    conn.execute('''INSERT INTO research_ordered_scope_schedule_state(scheduler_key)
        VALUES(%s) ON CONFLICT DO NOTHING''',(scheduler,))
    ticket=int(conn.execute('''SELECT next_ticket FROM research_ordered_scope_schedule_state
        WHERE scheduler_key=%s FOR UPDATE''',(scheduler,)).fetchone()['next_ticket'])
    conn.execute('''UPDATE research_ordered_scope_schedule_state
        SET next_ticket=next_ticket+1,updated_at_utc=NOW() WHERE scheduler_key=%s''',(scheduler,))
    # Limit lightweight IDs in both lanes; only the selected combined batch
    # loads result JSON. Never-evaluated catalog expansion cannot starve old
    # published results, and refresh cannot block new candidate coverage.
    initial=conn.execute('''SELECT scope_key FROM research_ordered_formula_scopes
        WHERE period_key=ANY(%s) AND candidate_key=ANY(%s)
          AND last_evaluated_at_utc IS NULL
        ORDER BY scope_key LIMIT %s''',(list(PERIODS),keys,cap)).fetchall()
    refresh=conn.execute('''SELECT scope_key FROM research_ordered_formula_scopes
        WHERE period_key=ANY(%s) AND candidate_key=ANY(%s)
          AND last_evaluated_at_utc IS NOT NULL
        ORDER BY last_evaluated_at_utc,scope_key LIMIT %s''',(list(PERIODS),keys,cap)).fetchall()
    lanes=(refresh,initial) if ticket%2==0 else (initial,refresh)
    ids=[row['scope_key'] for row in _interleave(*lanes,cap)]
    if not ids:
        return ScopeScheduleBatch([],ticket)
    rows=conn.execute('''SELECT * FROM research_ordered_formula_scopes
        WHERE scope_key=ANY(%s)''',(ids,)).fetchall()
    found={row['scope_key']:row for row in rows}
    if set(found)!=set(ids):
        raise RuntimeError('Scheduled formula scopes disappeared during selection')
    return ScopeScheduleBatch([found[key] for key in ids],ticket)


def interleave_experimental_refresh(scopes,priority,*,limit):
    """Reserve ordinary work even while qualified formulas need fresh checks."""
    ticket=getattr(scopes,'schedule_ticket',0)
    priority_by_id={row['scope_key']:row for row in priority}
    ordinary=[row for row in scopes if row['scope_key'] not in priority_by_id]
    special=list(priority_by_id.values())
    # With even a one-scope time budget, both ordinary lanes and experimental
    # refresh get a first position across a six-pass cycle. No wall clock or
    # process restart can pin one lane ahead of the others.
    lanes=(special,ordinary) if ticket%3==2 else (ordinary,special)
    return _interleave(*lanes,max(1,int(limit)))


def load_scope_rows(conn:Any,scope:Mapping[str,Any],*,row_limit:int=5000,now:datetime|None=None)->tuple[list[dict[str,Any]],bool]:
    # First matching time is chosen BEFORE any outcome join. All simultaneous
    # matching cards/coins are retained, so missing labels cannot select winners.
    cutoff=period_contract(scope)['period_start_utc']
    now=now or datetime.now(timezone.utc)
    candidate_count=conn.execute("SELECT COUNT(*) AS n FROM (SELECT event_id FROM research_ordered_formula_matches WHERE candidate_key=%s AND direction=%s AND (%s='ALL' OR symbol=%s) AND alert_time_utc>=%s AND alert_time_utc<=%s LIMIT 10001) bounded",(scope['candidate_key'],scope['direction'],scope['symbol'],scope['symbol'],cutoff,now)).fetchone()['n']
    rows=conn.execute('''
        WITH source_matches AS MATERIALIZED (
            SELECT * FROM research_ordered_formula_matches WHERE candidate_key=%s AND direction=%s AND (%s='ALL' OR symbol=%s)
              AND alert_time_utc>=%s AND alert_time_utc<=%s
            ORDER BY alert_time_utc,event_id LIMIT 10000
        ), matched AS MATERIALIZED (
            SELECT m.*,membership.btc_parent_movement_id,membership.membership_status,
                   parent.evidence_eligible AS parent_evidence_eligible,
                   parent.start_time_utc AS parent_start_time_utc,
                   MIN(m.alert_time_utc) OVER(PARTITION BY membership.btc_parent_movement_id) AS first_match_time
            FROM source_matches m
            JOIN research_event_btc_movements membership ON membership.event_id=m.event_id
              AND membership.episode_policy_version=%s AND membership.membership_status='LIVE'
            JOIN research_btc_parent_movements parent ON parent.btc_parent_movement_id=membership.btc_parent_movement_id
              AND parent.episode_policy_version=%s AND parent.evidence_eligible IS TRUE
              AND parent.start_time_utc>=%s
            WHERE m.candidate_key=%s AND m.direction=%s AND (%s='ALL' OR m.symbol=%s)
        ), representatives AS MATERIALIZED (
            SELECT * FROM matched WHERE alert_time_utc=first_match_time
            ORDER BY alert_time_utc,btc_parent_movement_id,event_id LIMIT %s
        )
        SELECT m.*,m.alert_time_utc AS features_observed_at_utc,
               'btc-parent-close-reversal-200bps-v1' AS episode_policy_version,
               %s::integer AS window_minutes,%s::integer AS threshold_bps,
               CASE WHEN o.event_id IS NULL THEN jsonb_build_object('window_minutes',%s::integer,'threshold_bps',%s::integer)
                    ELSE to_jsonb(o)-'calculation_audit'-'threshold_policy' END AS ordered_outcome
        FROM representatives m LEFT JOIN research_ordered_first_touch_outcomes o
          ON o.event_id=m.event_id AND o.window_minutes=%s AND o.threshold_bps=%s
             AND o.method_version='ordered-first-touch-v7'
        ORDER BY m.alert_time_utc,m.event_id
    ''',(scope['candidate_key'],scope['direction'],scope['symbol'],scope['symbol'],cutoff,now,PARENT_POLICY,PARENT_POLICY,cutoff,scope['candidate_key'],scope['direction'],scope['symbol'],scope['symbol'],row_limit+1,
         scope['window_minutes'],scope['threshold_bps'],scope['window_minutes'],scope['threshold_bps'],scope['window_minutes'],scope['threshold_bps'])).fetchall()
    truncated=len(rows)>row_limit or candidate_count>10000
    if len(rows)>row_limit:
        # Never describe a partial simultaneous cohort at the row-budget edge.
        last_parent=rows[row_limit]['btc_parent_movement_id']
        rows=[row for row in rows[:row_limit] if row['btc_parent_movement_id']!=last_parent]
    if str(scope['candidate_key']).startswith(questions.VERSION+':INVERSE:'):
        import research_ordered_inverse_store as inverse_store
        present=inverse_store.available(conn)
        labels=inverse_store.load_inverse_outcomes(conn,[row['event_id'] for row in rows],scope['window_minutes'],scope['threshold_bps']) if present else {}
        ids=inverse_store.outcome_event_ids(conn,[row['event_id'] for row in rows]) if present else {}
        rows=[{**row,'source_analysis_direction':{'LONG':'SHORT','SHORT':'LONG'}[scope['direction']],
            'research_orientation':'INVERSE','outcome_event_id':ids.get(row['event_id']),
            'ordered_outcome':labels.get(row['event_id'],{'window_minutes':scope['window_minutes'],'threshold_bps':scope['threshold_bps']})} for row in rows]
    return rows,truncated


def inverse_reconciliation_ids(conn,*,limit=32):
    """Reconcile one finite source page; never sort the full match archive."""
    import research_ordered_inverse_store as inverse_store
    from research_event_scan import claim_event_page
    ids=claim_event_page(conn,questions.VERSION+':inverse-request-reconciliation',
        limit=max(1,min(128,limit)),
        predicate="e.event_kind='ALERT' AND e.delivery_status='DELIVERED' AND e.direction IN ('LONG','SHORT')")
    if not ids:
        return []
    rows=conn.execute('''SELECT DISTINCT m.event_id FROM research_ordered_formula_matches m
        LEFT JOIN research_ordered_inverse_requests r ON r.linked_source_event_id=m.event_id AND r.inverse_version=%s
        WHERE m.event_id=ANY(%s::bigint[]) AND m.candidate_key LIKE %s
          AND r.linked_source_event_id IS NULL
        ORDER BY m.event_id''',(inverse_store.VERSION,ids,questions.VERSION+':INVERSE:%')).fetchall()
    return [row['event_id'] for row in rows]


def common_window_rows(conn,scope,rows):
    import research_common_window_metrics_store as metrics
    if not metrics.available(conn): return {}
    ids={row['event_id']:row.get('outcome_event_id',row['event_id']) for row in rows}
    loaded=metrics.load_by_event_ids(conn,[key for key in ids.values() if key is not None],scope['window_minutes'])
    return {original:loaded[derived] for original,derived in ids.items() if derived in loaded}


def persist_scope(conn:Any,scope:Mapping[str,Any],rows:list[dict[str,Any]],result:dict[str,Any],*,now:datetime)->dict[str,int]:
    period=period_contract(scope)
    formula_version=scope_formula_version(scope)
    episodes=result.get('episodes',[])
    summary={key:value for key,value in result.items() if key!='episodes'}
    summary['decision_evidence_sha256']=digest(episodes)
    summary.update({'formula_version':formula_version,'parent_policy_version':PARENT_POLICY,
                    'feature_source':'IMMUTABLE_DELIVERED_ALERT_CAPTURED_FIELDS','universe':'LIVE_DELIVERED_ALERTS',
                    'feature_version':questions.VERSION,
                    'validation_limitation':'Read separate fixed-window and prospective contracts; acceptance requires documented compatible policy.'})
    summary.update(period)
    evidence_sha=digest({'scope':dict(scope)|{'last_evaluated_at_utc':None,'result':None},'episodes':episodes,'summary':summary})
    conn.execute('UPDATE research_ordered_formula_scopes SET result=%s::jsonb,last_evaluated_at_utc=%s WHERE scope_key=%s',(canonical(summary),now,scope['scope_key']))
    if canonical(summary)==canonical(scope.get('result') or {}):
        return {'episodes':0,'upserts':0}
    conn.execute('''INSERT INTO research_ordered_formula_trials(trial_id,scope_key,evidence_sha256,result,evaluated_at_utc)
        VALUES(%s,%s,%s,%s::jsonb,%s) ON CONFLICT(scope_key,evidence_sha256) DO NOTHING''',
        (digest([scope['scope_key'],evidence_sha]),scope['scope_key'],evidence_sha,canonical(summary),now))
    by_id={row['event_id']:row for row in rows}
    upserts=[]
    for episode in episodes:
        episode_id=digest([scope['scope_key'],episode['btc_parent_movement_id']])
        conn.execute('''INSERT INTO research_ordered_formula_episodes(episode_id,scope_key,btc_parent_movement_id,representative_event_ids,evidence)
            VALUES(%s,%s,%s,%s::jsonb,%s::jsonb) ON CONFLICT(scope_key,btc_parent_movement_id) DO UPDATE SET
              representative_event_ids=EXCLUDED.representative_event_ids,evidence=EXCLUDED.evidence,updated_at_utc=NOW()''',
            (episode_id,scope['scope_key'],episode['btc_parent_movement_id'],canonical(episode['event_ids']),canonical(episode)))
        first=by_id.get(episode['event_ids'][0],{})
        labels=[by_id[event_id].get('ordered_outcome') or {} for event_id in episode['event_ids'] if event_id in by_id]
        decisive=[label for label in labels if label.get('status')==episode['status']]
        audit={**period,'window_minutes':scope['window_minutes'],'threshold_bps':scope['threshold_bps'],'representative_event_ids':episode['event_ids'],
               'exclusion_reasons':episode['exclusion_reasons'],'metric_scope':'STOP_AT_FIRST_TOUCH','parent_policy_version':PARENT_POLICY,
               'representative_outcome_statuses':episode.get('representative_outcome_statuses',[]),
               'status_reporting_version':evaluator.STATUS_REPORTING_VERSION,
               'same_wave_repeats':'Not additional independent evidence','live_effect':'NONE'}
        row={'episode_id':episode_id,'candidate_key':scope['candidate_key'],'symbol':scope['symbol'],'direction':scope['direction'],
             'threshold_pct':scope['threshold_bps']/100,'first_snapshot_id':first.get('snapshot_id'),
             'opened_at_utc':str(episode['forecast_start_utc']),'entry_price':first.get('entry_price'),
             'state':'LIVE','result':episode['status'],'closed_at_utc':max((str(label.get('decision_time_utc') or '') for label in decisive),default=''),
             'close_price':(decisive[0].get('favorable_touch_price') if episode['success'] else decisive[0].get('adverse_touch_price')) if decisive else '',
             'swallowed_snapshot_count':'','rearmed_snapshot_id':'','rearm_reason':'NO_TIME_RESET_RULE',
             'btc_parent_movement_id':episode['btc_parent_movement_id'],'policy_version':formula_version,'audit_note':canonical(audit)}
        upserts.append({'sheet':'Episodes','key':'episode_id','row':row})
    complete=bool(summary.get('source_coverage_complete',True) and summary.get('source_population_complete',True) and summary.get('membership_population_complete',True) and not summary.get('truncated'))
    detail=f"{summary['independent_waves']} {'independent' if complete else 'provisional'} BTC waves; route {summary['count_route']}; research only."
    detail+=' Period='+canonical(period)+'. Coverage='+canonical(summary.get('period_coverage',{}))+'.'
    detail+=f" All grouped waves={summary['independent_waves']}; recent14d={summary['fresh_independent_waves']}; displayed denominator={summary['sample_size'] if complete else 0}."
    detail+=' Wave status audit='+canonical({key:summary.get(key) for key in (
        'status_reporting_version','status_count_scope','status_counts','all_wave_status_counts',
        'fresh_wave_status_counts','cohort_status_policy',
    )})+'. Only SUCCESS/FAILURE enter the hit-rate denominator; excluded evidence is not OPEN.'
    if summary['exclusion_reasons']:
        detail+=' Excluded evidence reasons='+canonical(summary['exclusion_reasons'])+'.'
    if not complete:
        detail+=' Decision population incomplete; grouped-wave counts are provisional and rates withheld.'
    candidate=next(candidate for candidate in evaluator.candidate_catalog(include_extended=True) if candidate['formula_id']==scope['candidate_key'])
    detail+=' Question search='+canonical({key:candidate.get(key) for key in ('question_ids','overlap_group','research_orientation','justification')})+'.'
    detail+=' Shared component families='+canonical(questions.component_families(candidate))+'. Overlapping components and shared BTC parents are not independent confirmations.'
    common=summary.get('common_window_metrics') or {}
    common_complete=complete and common.get('common_window_complete') is True
    # Existing Sheet headers say median: never substitute means or stopped-FT
    # values. The historical stopped metrics remain explicitly named in JSON.
    detail+=' Fixed-window metric contract='+canonical({key:common.get(key) for key in (
        'common_window_method_version','asymmetry_method','common_window_complete','common_window_waves',
        'common_window_asymmetry_state','selected_waves')})+'.'
    detail+=' Prospective validation='+canonical({key:(summary.get('prospective_validation') or {}).get(key) for key in (
        'validation_status','research_ready','registered_attempts','discovery','prospective')})+'.'
    formula_row={'candidate_key':scope['candidate_key'],'candidate_name':scope['candidate_key'],'exact_conditions':canonical(candidate['conditions']),
        'coin_scope':scope['symbol'],'direction':scope['direction'],'threshold_pct':scope['threshold_bps']/100,'horizon':scope['window_minutes'],
        'independent_episodes':summary['sample_size'] if complete else 0,'successes':summary['successes'] if complete else 0,'failures':summary['failures'] if complete else 0,
        'open_episodes':summary['open_waves'],'hit_rate':summary['hit_rate_pct'] if complete else '',
        'median_mfe_pct':common.get('common_window_median_mfe_pct') if common_complete else '',
        'median_mae_pct':common.get('common_window_median_mae_pct') if common_complete else '',
        'asymmetry_ratio':common.get('common_window_asymmetry_ratio') if common_complete else '',
        'opposite_indicator_test':'INVERSE_CANDIDATE' if candidate.get('research_orientation')=='INVERSE' else 'SEPARATE_VERSIONED_CANDIDATE','current_period_hit_rate':'','prior_period_hit_rate':'','change_pp':'',
        'strongest_failure_pattern':'NOT_TESTED','status':'INCOMPLETE_DECISION_POPULATION' if not complete else summary['validation_status'] if summary['count_eligible'] else 'INSUFFICIENT_INDEPENDENT_EVIDENCE',
        'meets_min_5':complete and summary['independent_waves']>=5,'last_evaluated_at':now.isoformat(),'chat_summary':detail,'formula_version':formula_version}
    if canonical(summary) != canonical(scope.get('result') or {}):
        upserts.append({'sheet':'Formula_Results','key':'candidate_key,coin_scope,direction,threshold_pct,horizon,formula_version','row':formula_row})
    research_sheet_outbox.stage_upserts(conn,upserts)
    return {'episodes':len(episodes),'upserts':len(upserts)}


def source_population_complete(conn:Any,*,now:datetime,period_key:str)->bool:
    # A processed ledger covers both matching and nonmatching source alerts.
    # No v7/outcome condition is used in this source population check.
    return conn.execute("""SELECT NOT EXISTS(
        SELECT 1 FROM research_events e LEFT JOIN research_ordered_feature_screens checked
          ON checked.event_id=e.event_id AND checked.feature_version=%s
        WHERE e.event_kind='ALERT' AND e.delivery_status='DELIVERED' AND e.direction IN ('LONG','SHORT')
          AND e.alert_time_utc>=%s AND e.alert_time_utc<=%s AND checked.event_id IS NULL LIMIT 1
    ) AS complete""",(questions.VERSION,period_contract({'period_key':period_key})['period_start_utc'],now)).fetchone()['complete']


def candidate_feature_coverage_complete(conn,scope,candidate,*,now):
    """Earlier unknown historical features cannot select a later winner.

    The compact versioned source screen includes failed conditions. Only
    source alerts not ruled out by their known frozen conditions matter here.
    Initial excluded boundary parents stay outside the operational universe;
    missing parent membership stays potentially relevant and therefore blocks.
    """
    predicate,feature_params=question_store.historical_coverage_predicate(candidate)
    if predicate is None: return {'complete':True,'historical_features_required':False}
    direction=({'LONG':'SHORT','SHORT':'LONG'}[scope['direction']] if candidate.get('research_orientation')=='INVERSE' else scope['direction'])
    cutoff=period_contract(scope)['period_start_utc']
    query='''SELECT e.event_id,m.btc_parent_movement_id FROM research_events e
        LEFT JOIN research_ordered_feature_screens f ON f.event_id=e.event_id AND f.feature_version=%s
        LEFT JOIN research_event_btc_movements m ON m.event_id=e.event_id AND m.episode_policy_version=%s
        LEFT JOIN research_btc_parent_movements p ON p.btc_parent_movement_id=m.btc_parent_movement_id AND p.episode_policy_version=%s
        WHERE e.event_kind='ALERT' AND e.delivery_status='DELIVERED' AND e.direction=%s
          AND e.alert_time_utc>=%s AND e.alert_time_utc<=%s AND (%s='ALL' OR e.symbol=%s)
          AND (m.event_id IS NULL OR m.membership_status='BTC_DATA_MISSING'
               OR (m.membership_status='LIVE' AND (p.btc_parent_movement_id IS NULL
                   OR (p.evidence_eligible IS TRUE AND p.start_time_utc>=%s))))
          AND '''+predicate+' ORDER BY e.alert_time_utc,e.event_id LIMIT 1'
    row=conn.execute(query,(questions.VERSION,PARENT_POLICY,PARENT_POLICY,direction,cutoff,now,scope['symbol'],scope['symbol'],cutoff,*feature_params)).fetchone()
    return {'complete':row is None,'historical_features_required':True,
        'potentially_matching_unknown_event_id':row['event_id'] if row else None,
        'btc_parent_movement_id':row['btc_parent_movement_id'] if row else None,
        'source_analysis_direction':direction,'reason':'REQUIRED_PAST_FEATURES_UNKNOWN_FOR_POSSIBLE_EARLIER_MATCH' if row else None}


def membership_complete(conn:Any,scope:Mapping[str,Any],*,now:datetime|None=None)->bool:
    return conn.execute("""SELECT NOT EXISTS(
        SELECT 1 FROM research_ordered_formula_matches m
        LEFT JOIN research_event_btc_movements membership ON membership.event_id=m.event_id AND membership.episode_policy_version=%s
        WHERE m.candidate_key=%s AND m.direction=%s AND (%s='ALL' OR m.symbol=%s)
          AND m.alert_time_utc>=%s AND m.alert_time_utc<=%s
          AND (membership.event_id IS NULL OR membership.membership_status='BTC_DATA_MISSING') LIMIT 1
    ) AS complete""",(PARENT_POLICY,scope['candidate_key'],scope['direction'],scope['symbol'],scope['symbol'],period_contract(scope)['period_start_utc'],now or datetime.now(timezone.utc))).fetchone()['complete']


def period_coverage(conn:Any,scope:Mapping[str,Any],*,now:datetime)->dict[str,Any]:
    cutoff=period_contract(scope)['period_start_utc']
    return dict(conn.execute('''SELECT COUNT(*) AS matching_alert_count,
        COUNT(DISTINCT m.snapshot_id) AS matching_snapshot_count,
        COUNT(*) FILTER(WHERE parent.start_time_utc<%s) AS boundary_excluded_alerts,
        COUNT(DISTINCT parent.btc_parent_movement_id) FILTER(WHERE parent.start_time_utc<%s) AS boundary_excluded_waves,
        COUNT(*) FILTER(WHERE membership.event_id IS NULL OR membership.membership_status='BTC_DATA_MISSING') AS missing_membership_alerts,
        COUNT(*) FILTER(WHERE membership.membership_status='BOUNDARY_UNVERIFIED') AS unverified_boundary_alerts
        FROM research_ordered_formula_matches m
        LEFT JOIN research_event_btc_movements membership ON membership.event_id=m.event_id AND membership.episode_policy_version=%s
        LEFT JOIN research_btc_parent_movements parent ON parent.btc_parent_movement_id=membership.btc_parent_movement_id AND parent.episode_policy_version=%s
        WHERE m.candidate_key=%s AND m.direction=%s AND (%s='ALL' OR m.symbol=%s)
          AND m.alert_time_utc>=%s AND m.alert_time_utc<=%s
    ''',(cutoff,cutoff,PARENT_POLICY,PARENT_POLICY,scope['candidate_key'],scope['direction'],scope['symbol'],scope['symbol'],cutoff,now)).fetchone())
