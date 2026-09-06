"""Incremental, bounded persistence for frozen-score ordered-v7 research."""
from __future__ import annotations
import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
import research_formula_ordered_v7 as evaluator
import research_sheet_outbox

PARENT_POLICY = 'btc-parent-close-reversal-200bps-v1'
WORKER_KEY = 'ordered-v7-frozen-total-scores-v1'
FORMULA_VERSION = evaluator.POLICY_VERSION + ':' + evaluator.CATALOG_VERSION + ':' + PARENT_POLICY
REQUIRED_TABLES = ('research_ordered_formula_event_checks','research_ordered_formula_candidates','research_ordered_formula_matches','research_ordered_formula_scopes','research_ordered_formula_episodes','research_ordered_formula_trials','research_ordered_formula_worker_state','research_sheet_upsert_outbox','research_event_btc_movements','research_btc_parent_movements','research_ordered_first_touch_outcomes')


def canonical(value: Any) -> str:
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,default=str,allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def schema_status(conn: Any) -> dict[str, Any]:
    missing = [name for name in REQUIRED_TABLES if conn.execute('SELECT to_regclass(%s) AS relation',(name,)).fetchone()['relation'] is None]
    return {'schema_present':not missing,'missing_tables':missing}


def register_catalog(conn: Any) -> list[dict[str, Any]]:
    catalog = evaluator.candidate_catalog()
    for candidate in catalog:
        key = candidate['formula_id']
        definition = {'candidate':candidate,'formula_version':FORMULA_VERSION,'parent_policy_version':PARENT_POLICY}
        sha = digest(definition)
        conn.execute('''INSERT INTO research_ordered_formula_candidates(candidate_key,formula_version,definition_sha256,definition)
            VALUES(%s,%s,%s,%s::jsonb) ON CONFLICT(candidate_key) DO NOTHING''',(key,FORMULA_VERSION,sha,canonical(definition)))
        found = conn.execute('SELECT definition_sha256 FROM research_ordered_formula_candidates WHERE candidate_key=%s',(key,)).fetchone()
        if found['definition_sha256'] != sha:
            raise ValueError('Frozen candidate definition changed; use a new versioned key')
    return catalog


def register_scopes(conn: Any,catalog: list[dict[str,Any]],symbols: set[str],*,directions=None,include_all:bool=True) -> int:
    records = []
    for candidate in catalog:
        for symbol in sorted(symbols | ({'ALL'} if include_all else set())):
            for direction in (directions or evaluator.DIRECTIONS):
                for horizon in evaluator.HORIZONS_MINUTES:
                    for bps in evaluator.THRESHOLDS_BPS:
                        identity = (candidate['formula_id'],symbol,direction,horizon,bps,FORMULA_VERSION)
                        records.append((digest(identity),*identity[:5]))
    if records:
        with conn.cursor() as cur:
            cur.executemany('''INSERT INTO research_ordered_formula_scopes(scope_key,candidate_key,symbol,direction,window_minutes,threshold_bps)
                VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING''',records)
    return len(records)


# Fetch compact module totals only after limiting source IDs. No price-path or
# full decision-bundle JSON is materialized in the Python worker.
_EVENT_PROJECT = '''
SELECT e.event_id,e.symbol,e.direction,e.alert_time_utc,e.event_fingerprint,
       e.current_price,e.event_type,e.event_kind,e.delivery_status,
       jsonb_build_object('sheet_snapshot_id',e.engine_snapshot->'sheet_snapshot_id',
           'market_evidence',jsonb_build_object('modules',jsonb_build_object(
             'positioning',jsonb_build_object('score',e.engine_snapshot#>'{market_evidence,modules,positioning,score}','direction',e.engine_snapshot#>'{market_evidence,modules,positioning,direction}'),
             'futures_flow',jsonb_build_object('score',e.engine_snapshot#>'{market_evidence,modules,futures_flow,score}','direction',e.engine_snapshot#>'{market_evidence,modules,futures_flow,direction}'),
             'spot_flow',jsonb_build_object('score',e.engine_snapshot#>'{market_evidence,modules,spot_flow,score}','direction',e.engine_snapshot#>'{market_evidence,modules,spot_flow,direction}')))) AS engine_snapshot
FROM picked JOIN research_events e USING(event_id) ORDER BY e.event_id
'''


def ingest_matches(conn: Any,catalog: list[dict[str,Any]],*,now: datetime,event_limit: int=128,lookback_days: int=14) -> dict[str,Any]:
    conn.execute('INSERT INTO research_ordered_formula_worker_state(worker_key) VALUES(%s) ON CONFLICT DO NOTHING',(WORKER_KEY,))
    cursor = conn.execute('SELECT last_event_id FROM research_ordered_formula_worker_state WHERE worker_key=%s FOR UPDATE',(WORKER_KEY,)).fetchone()['last_event_id']
    since = now-timedelta(days=lookback_days)
    events = conn.execute('''WITH picked AS MATERIALIZED (
        SELECT event_id FROM research_events WHERE event_id>%s AND alert_time_utc>=%s
          AND alert_time_utc<=%s AND event_kind='ALERT' AND delivery_status='DELIVERED'
          AND direction IN ('LONG','SHORT') ORDER BY event_id LIMIT %s
        ) ''' + _EVENT_PROJECT,(cursor,since,now,event_limit)).fetchall()
    # The recent tail advances live work while the ascending cursor backfills;
    # wrap-around also recovers delayed delivery-state transitions below it.
    recent = conn.execute('''WITH picked AS MATERIALIZED (
        SELECT event_id FROM research_events WHERE alert_time_utc>=%s AND alert_time_utc<=%s
          AND event_kind='ALERT' AND delivery_status='DELIVERED'
          AND direction IN ('LONG','SHORT') ORDER BY event_id DESC LIMIT %s
        ) ''' + _EVENT_PROJECT,(since,now,min(32,event_limit))).fetchall()
    conn.execute('UPDATE research_ordered_formula_worker_state SET last_event_id=%s,initial_scan_complete=initial_scan_complete OR %s,updated_at_utc=NOW() WHERE worker_key=%s',
                 (max(row['event_id'] for row in events) if events else 0,not events,WORKER_KEY))
    records, symbols, missing_features = [], set(), 0
    unique = {event['event_id']:event for event in [*events,*recent]}
    for event in unique.values():
        symbols.add(event['symbol'])
        features = evaluator.extract_event_features(event)
        if not any(name.endswith('.aligned_score') and value is not None for name,value in features.items()):
            missing_features += 1
        for candidate in catalog:
            if evaluator.matches(candidate,features,event['direction']):
                snapshot = event.get('engine_snapshot') or {}
                records.append((candidate['formula_id'],event['event_id'],event['symbol'],event['direction'],event['alert_time_utc'],
                                snapshot.get('sheet_snapshot_id') or event['event_fingerprint'],event['current_price'],canonical(features)))
    if unique:
        with conn.cursor() as cur:
            cur.executemany('INSERT INTO research_ordered_formula_event_checks(event_id) VALUES(%s) ON CONFLICT DO NOTHING',[(event_id,) for event_id in unique])
    if records:
        with conn.cursor() as cur:
            cur.executemany('''INSERT INTO research_ordered_formula_matches(candidate_key,event_id,symbol,direction,alert_time_utc,snapshot_id,entry_price,decision_features)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s::jsonb) ON CONFLICT(candidate_key,event_id) DO NOTHING''',records)
    known = {(row['candidate_key'],row['symbol'],row['direction']) for row in conn.execute('SELECT DISTINCT candidate_key,symbol,direction FROM research_ordered_formula_scopes').fetchall()}
    if not known:
        register_scopes(conn,catalog,set())
    candidate_by_id = {candidate['formula_id']:candidate for candidate in catalog}
    for candidate_key,symbol,direction in sorted({(record[0],record[2],record[3]) for record in records}-known):
        register_scopes(conn,[candidate_by_id[candidate_key]],{symbol},directions=[direction],include_all=False)
    return {'events_checked':len(unique),'missing_total_score_features':missing_features,'matches_observed':len(records),'symbols':sorted(symbols),'cursor':max((e['event_id'] for e in events),default=0)}


def due_scopes(conn:Any,limit:int=64)->list[dict[str,Any]]:
    return conn.execute('SELECT * FROM research_ordered_formula_scopes ORDER BY last_evaluated_at_utc ASC NULLS FIRST,scope_key LIMIT %s',(limit,)).fetchall()


def load_scope_rows(conn:Any,scope:Mapping[str,Any],*,row_limit:int=5000)->tuple[list[dict[str,Any]],bool]:
    # First matching time is chosen BEFORE any outcome join. All simultaneous
    # matching cards/coins are retained, so missing labels cannot select winners.
    candidate_count=conn.execute("SELECT COUNT(*) AS n FROM (SELECT event_id FROM research_ordered_formula_matches WHERE candidate_key=%s AND direction=%s AND (%s='ALL' OR symbol=%s) LIMIT 10001) bounded",(scope['candidate_key'],scope['direction'],scope['symbol'],scope['symbol'])).fetchone()['n']
    rows=conn.execute('''
        WITH source_matches AS MATERIALIZED (
            SELECT * FROM research_ordered_formula_matches WHERE candidate_key=%s AND direction=%s AND (%s='ALL' OR symbol=%s)
            ORDER BY alert_time_utc,event_id LIMIT 10000
        ), matched AS MATERIALIZED (
            SELECT m.*,membership.btc_parent_movement_id,membership.membership_status,
                   parent.evidence_eligible AS parent_evidence_eligible,
                   MIN(m.alert_time_utc) OVER(PARTITION BY membership.btc_parent_movement_id) AS first_match_time
            FROM source_matches m
            JOIN research_event_btc_movements membership ON membership.event_id=m.event_id
              AND membership.episode_policy_version=%s AND membership.membership_status='LIVE'
            JOIN research_btc_parent_movements parent ON parent.btc_parent_movement_id=membership.btc_parent_movement_id
              AND parent.episode_policy_version=%s AND parent.evidence_eligible IS TRUE
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
    ''',(scope['candidate_key'],scope['direction'],scope['symbol'],scope['symbol'],PARENT_POLICY,PARENT_POLICY,scope['candidate_key'],scope['direction'],scope['symbol'],scope['symbol'],row_limit+1,
         scope['window_minutes'],scope['threshold_bps'],scope['window_minutes'],scope['threshold_bps'],scope['window_minutes'],scope['threshold_bps'])).fetchall()
    truncated=len(rows)>row_limit or candidate_count>10000
    if len(rows)>row_limit:
        # Never describe a partial simultaneous cohort at the row-budget edge.
        last_parent=rows[row_limit]['btc_parent_movement_id']
        rows=[row for row in rows[:row_limit] if row['btc_parent_movement_id']!=last_parent]
    return rows,truncated


def persist_scope(conn:Any,scope:Mapping[str,Any],rows:list[dict[str,Any]],result:dict[str,Any],*,now:datetime)->dict[str,int]:
    episodes=result.get('episodes',[])
    summary={key:value for key,value in result.items() if key!='episodes'}
    summary['decision_evidence_sha256']=digest(episodes)
    summary.update({'formula_version':FORMULA_VERSION,'parent_policy_version':PARENT_POLICY,
                    'feature_source':'IMMUTABLE_DELIVERED_ALERT_TOTAL_SCORES','universe':'ALERTS_ONLY_MODEL_TOTAL_SCORES',
                    'validation_limitation':'Independent prospective/full-horizon validation is not yet implemented in this path.'})
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
        audit={'window_minutes':scope['window_minutes'],'threshold_bps':scope['threshold_bps'],'representative_event_ids':episode['event_ids'],
               'exclusion_reasons':episode['exclusion_reasons'],'metric_scope':'STOP_AT_FIRST_TOUCH','parent_policy_version':PARENT_POLICY,
               'same_wave_repeats':'Not additional independent evidence','live_effect':'NONE'}
        row={'episode_id':episode_id,'candidate_key':scope['candidate_key'],'symbol':scope['symbol'],'direction':scope['direction'],
             'threshold_pct':scope['threshold_bps']/100,'first_snapshot_id':first.get('snapshot_id'),
             'opened_at_utc':str(episode['forecast_start_utc']),'entry_price':first.get('entry_price'),
             'state':'LIVE','result':episode['status'],'closed_at_utc':max((str(label.get('decision_time_utc') or '') for label in decisive),default=''),
             'close_price':(decisive[0].get('favorable_touch_price') if episode['success'] else decisive[0].get('adverse_touch_price')) if decisive else '',
             'swallowed_snapshot_count':'','rearmed_snapshot_id':'','rearm_reason':'NO_TIME_RESET_RULE',
             'btc_parent_movement_id':episode['btc_parent_movement_id'],'policy_version':FORMULA_VERSION,'audit_note':canonical(audit)}
        upserts.append({'sheet':'Episodes','key':'episode_id','row':row})
    complete=bool(summary.get('source_population_complete',True) and summary.get('membership_population_complete',True) and not summary.get('truncated'))
    detail=f"{summary['independent_waves']} {'independent' if complete else 'provisional'} BTC waves; route {summary['count_route']}; research only; stop-at-first-touch metrics; validation pending."
    detail+=f" All grouped waves={summary['independent_waves']}; recent14d={summary['fresh_independent_waves']}; displayed denominator={summary['sample_size'] if complete else 0}."
    if summary['exclusion_reasons']:
        detail+=' Excluded evidence reasons='+canonical(summary['exclusion_reasons'])+'.'
    if not complete:
        detail+=' Decision population incomplete; grouped-wave counts are provisional and rates withheld.'
    formula_row={'candidate_key':scope['candidate_key'],'candidate_name':scope['candidate_key'],'exact_conditions':canonical(next(candidate['conditions'] for candidate in evaluator.candidate_catalog() if candidate['formula_id']==scope['candidate_key'])),
        'coin_scope':scope['symbol'],'direction':scope['direction'],'threshold_pct':scope['threshold_bps']/100,'horizon':scope['window_minutes'],
        'independent_episodes':summary['sample_size'] if complete else 0,'successes':summary['successes'] if complete else 0,'failures':summary['failures'] if complete else 0,
        'open_episodes':summary['excluded_waves'],'hit_rate':summary['hit_rate_pct'] if complete else '','median_mfe_pct':summary['median_mfe_pct'],'median_mae_pct':summary['median_mae_pct'],
        'asymmetry_ratio':summary['median_mfe_mae_ratio'] if complete else '','opposite_indicator_test':'NOT_TESTED','current_period_hit_rate':'','prior_period_hit_rate':'','change_pp':'',
        'strongest_failure_pattern':'NOT_TESTED','status':'INCOMPLETE_DECISION_POPULATION' if not complete else summary['validation_status'] if summary['count_eligible'] else 'INSUFFICIENT_INDEPENDENT_EVIDENCE',
        'meets_min_5':complete and summary['independent_waves']>=5,'last_evaluated_at':now.isoformat(),'chat_summary':detail,'formula_version':FORMULA_VERSION}
    if canonical(summary) != canonical(scope.get('result') or {}):
        upserts.append({'sheet':'Formula_Results','key':'candidate_key,coin_scope,direction,threshold_pct,horizon,formula_version','row':formula_row})
    research_sheet_outbox.stage_upserts(conn,upserts)
    return {'episodes':len(episodes),'upserts':len(upserts)}


def source_population_complete(conn:Any,*,now:datetime,lookback_days:int=14)->bool:
    # A processed ledger covers both matching and nonmatching source alerts.
    # No v7/outcome condition is used in this source population check.
    return conn.execute("""SELECT NOT EXISTS(
        SELECT 1 FROM research_events e LEFT JOIN research_ordered_formula_event_checks checked USING(event_id)
        WHERE e.event_kind='ALERT' AND e.delivery_status='DELIVERED' AND e.direction IN ('LONG','SHORT')
          AND e.alert_time_utc>=%s AND e.alert_time_utc<=%s AND checked.event_id IS NULL LIMIT 1
    ) AS complete""",(now-timedelta(days=lookback_days),now)).fetchone()['complete']


def membership_complete(conn:Any,scope:Mapping[str,Any])->bool:
    return conn.execute("""SELECT NOT EXISTS(
        SELECT 1 FROM research_ordered_formula_matches m
        LEFT JOIN research_event_btc_movements membership ON membership.event_id=m.event_id AND membership.episode_policy_version=%s
        WHERE m.candidate_key=%s AND m.direction=%s AND (%s='ALL' OR m.symbol=%s) AND membership.event_id IS NULL LIMIT 1
    ) AS complete""",(PARENT_POLICY,scope['candidate_key'],scope['direction'],scope['symbol'],scope['symbol'])).fetchone()['complete']
