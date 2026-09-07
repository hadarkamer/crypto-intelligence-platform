"""Recurring descriptive Formula evidence over ordered v7 and causal BTC parents."""
from __future__ import annotations
import asyncio
from datetime import datetime,timezone
import os
import time
from typing import Any
import research_formula_ordered_store as store
import research_formula_ordered_v7 as evaluator
import research_ordered_question_catalog as questions
import research_ordered_question_store as question_store
import research_ordered_validation_store as validation_store
import research_ordered_inverse_store as inverse_store
try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:
    psycopg=None
    dict_row=None

_TRUE={'1','true','yes','on'}
_ENABLED=os.getenv('RESEARCH_ORDERED_FORMULA_ENABLED',os.getenv('RESEARCH_OUTCOME_ENRICHMENT_ENABLED','')).strip().lower() in _TRUE
_POLL=max(30,int(os.getenv('RESEARCH_ORDERED_FORMULA_POLL_SECONDS','60')))
_EVENT_LIMIT=max(8,min(512,int(os.getenv('RESEARCH_ORDERED_FORMULA_EVENT_LIMIT','128'))))
_SCOPE_LIMIT=max(1,min(512,int(os.getenv('RESEARCH_ORDERED_FORMULA_SCOPE_LIMIT','128'))))
_ROW_LIMIT=max(100,min(5000,int(os.getenv('RESEARCH_ORDERED_FORMULA_ROW_LIMIT','2000'))))
_PASS_SECONDS=max(10,min(50,float(os.getenv('RESEARCH_ORDERED_FORMULA_PASS_SECONDS','40'))))
_STATEMENT_TIMEOUT_MS=max(15000,min(60000,int(os.getenv('RESEARCH_ORDERED_FORMULA_STATEMENT_TIMEOUT_MS','30000'))))
_LOCK_ID=588794583090203387


def _database_url()->str:
    dedicated=os.getenv('RESEARCH_DATABASE_URL','').strip()
    return dedicated or (os.getenv('DATABASE_URL','').strip() if os.getenv('RESEARCH_USE_PRIMARY_DATABASE','').strip().lower() in _TRUE else '')


def _connect(url:str):
    return psycopg.connect(url,row_factory=dict_row,connect_timeout=5,options=f'-c statement_timeout={_STATEMENT_TIMEOUT_MS} -c lock_timeout=1000')


class ResearchFormulaOrderedWorker:
    def __init__(self)->None:
        self._task=None
        self._stopping=False
        self._schema_ready=False
        self.metrics={'runs':0,'failures':0,'last_run_utc':None,'last_error':None,'last_stage':None,'last_summary':None}

    def status(self)->dict[str,Any]:
        return {'enabled':_ENABLED,'configured':bool(_database_url()),'running':bool(self._task and not self._task.done()),
            'schema_ready':self._schema_ready,'poll_seconds':_POLL,'formula_version':store.FORMULA_VERSION,
            'parent_policy_version':store.PARENT_POLICY,'outcome_method_version':evaluator.METHOD_VERSION,
            'research_periods':[store.period_contract({'period_key':key}) for key in store.PERIODS],
            'candidate_count':len(evaluator.candidate_catalog(include_extended=True)),'scope_limit_per_pass':_SCOPE_LIMIT,'event_limit_per_pass':_EVENT_LIMIT,
            'evaluation_budget_seconds':_PASS_SECONDS,
            'thresholds_bps':list(evaluator.THRESHOLDS_BPS),'horizons_minutes':list(evaluator.HORIZONS_MINUTES),
            'evidence_policy':'One causally verified BTC parent across all coins/time; earliest frozen matching cohort',
            'research_scope':'Versioned Q01-Q72 captured-feature queue; normal/inverse; finite singles/pairs/justified triples/quads; original delivered-alert wave cohorts',
            'question_map_count':len(questions.question_map()),'feature_version':questions.VERSION,
            'live_effect':'NONE','remaining_validation':'Sequence/regime features and exact acceptance are active for new v2 freezes; genuine later waves, incomplete captured fields and explicit trade approval remain',
            'metrics':dict(self.metrics)}

    async def start(self)->bool:
        if not _ENABLED:
            return False
        if psycopg is None or not _database_url():
            raise RuntimeError('Ordered Formula research database is not configured')
        def check():
            with _connect(_database_url()) as conn:
                return store.schema_status(conn)
        schema=await asyncio.to_thread(check)
        if not schema['schema_present']:
            raise RuntimeError('Ordered Formula schema missing: '+','.join(schema['missing_tables']))
        self._schema_ready=True
        if self._task and not self._task.done():
            return True
        self._stopping=False
        self._task=asyncio.create_task(self._run(),name='research-formula-ordered-v7')
        return True

    async def stop(self)->None:
        self._stopping=True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task=None

    async def _run(self)->None:
        await asyncio.sleep(15)
        while not self._stopping:
            try:
                await asyncio.to_thread(self.run_once)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.metrics['failures']+=1
                self.metrics['last_error']=f'{type(exc).__name__}: {exc}'
                print(f"[ordered-formula] run failed at {self.metrics.get('last_stage')}: {exc!r}",flush=True)
            await asyncio.sleep(_POLL)

    def run_once(self,*,now:datetime|None=None)->dict[str,Any]:
        now=now or datetime.now(timezone.utc)
        started=time.monotonic()
        summary={'scopes_evaluated':0,'unchanged_scopes_skipped':0,'episodes':0,'upserts':0,'truncated_scopes':0,'locked':False}
        url=_database_url()
        if not url or psycopg is None:
            raise RuntimeError('Ordered Formula research database is unavailable')
        with _connect(url) as conn:
            acquired=conn.execute('SELECT pg_try_advisory_lock(%s) AS acquired',(_LOCK_ID,)).fetchone()['acquired']
            conn.commit()
            if not acquired:
                summary['locked']=True
                return summary
            try:
                self.metrics['last_stage']='REGISTER_CATALOG'
                catalog=store.register_catalog(conn)
                self.metrics['last_stage']='INGEST_MATCHES'
                summary.update(store.ingest_matches(conn,catalog,now=now,event_limit=_EVENT_LIMIT))
                self.metrics['last_stage']='INVERSE_RECONCILIATION'
                inverse_available=inverse_store.available(conn)
                if inverse_available:
                    for event_id in sorted(set(summary.pop('inverse_source_event_ids',[]))|set(store.inverse_reconciliation_ids(conn,limit=32))):
                        inverse_store.request_inverse(conn,{'event_id':event_id},now=now)
                else:
                    summary['inverse_status']='BLOCKED_MISSING_MIGRATION_033'
                conn.commit()
                self.metrics['last_stage']='PREPARE_SCOPE_QUEUE'
                validation_available=validation_store.schema_status(conn)['schema_present']
                candidates={candidate['formula_id']:candidate for candidate in catalog}
                attempts=conn.execute("SELECT COUNT(*) AS n FROM research_ordered_formula_scopes WHERE period_key<>'LEGACY_UNSCOPED' AND candidate_key=ANY(%s)",(list(candidates),)).fetchone()['n']
                period_population={key:store.source_population_complete(conn,now=now,period_key=key) for key in store.PERIODS}
                summary['source_population_complete_by_period']=period_population
                summary['source_population_complete']=all(period_population.values())
                scopes=store.due_scopes(conn,_SCOPE_LIMIT,candidate_keys=candidates)
                feature_coverage_cache={}
                conn.commit()
                # Intake and reconciliation have separate row/query bounds.
                # Their latency must not consume the scope-evaluation budget.
                evaluation_started=time.monotonic()
                summary['preparation_seconds']=round(evaluation_started-started,3)
                summary['evaluation_budget_seconds']=_PASS_SECONDS
                summary['evaluation_budget_exhausted']=False
                summary['scopes_fetched']=len(scopes)
                for scope in scopes:
                    # Finish and commit an in-flight scope; stop starting new
                    # scopes once this stage has spent its time allowance.
                    if time.monotonic()-evaluation_started>=_PASS_SECONDS:
                        summary['evaluation_budget_exhausted']=True
                        break
                    population_complete=period_population[scope['period_key']]
                    self.metrics['last_stage']='EVALUATE_SCOPE:'+scope['scope_key']
                    feature_key=(scope['candidate_key'],scope['symbol'],scope['direction'],scope['period_key'])
                    if feature_key not in feature_coverage_cache:
                        feature_coverage_cache[feature_key]=store.candidate_feature_coverage_complete(conn,scope,candidates[scope['candidate_key']],now=now)
                    feature_coverage=feature_coverage_cache[feature_key]
                    candidate_population_complete=bool(population_complete and feature_coverage['complete'])
                    rows,truncated=store.load_scope_rows(conn,scope,row_limit=_ROW_LIMIT,now=now)
                    mappings_complete=store.membership_complete(conn,scope,now=now)
                    common_rows=store.common_window_rows(conn,scope,rows)
                    input_sha=question_store.evaluation_input(scope,[{**row,'common_window_record':common_rows.get(row['event_id'])} for row in rows],now=now,population_complete=candidate_population_complete,membership_complete=mappings_complete)
                    input_sha=store.digest({'input':input_sha,'validation_available':validation_available,'registered_attempts':attempts})
                    if scope.get('evaluation_input_sha256')==input_sha:
                        conn.execute('UPDATE research_ordered_formula_scopes SET last_evaluated_at_utc=%s WHERE scope_key=%s',(now,scope['scope_key']))
                        conn.commit()
                        summary['unchanged_scopes_skipped']+=1
                        continue
                    result=evaluator.summarize_scope(rows,analysis_as_of_utc=now,truncated=truncated,source_coverage_complete=bool(candidate_population_complete and mappings_complete))
                    result['candidate_feature_coverage']=feature_coverage
                    result['past_price_coverage']={'representative_events':len(rows),
                        'with_closed_prior_features':sum(any(key.startswith('historical.closed_1m.') and not key.endswith('.method_version') for key in (row.get('decision_features') or {})) for row in rows),
                        'range_regime_status':'NOT_DEFINED; exact closed-return sign is separate'}
                    result['period_coverage']=store.period_coverage(conn,scope,now=now)
                    result['source_population_complete']=candidate_population_complete
                    result['source_alert_screen_complete']=population_complete
                    result['membership_population_complete']=mappings_complete
                    if not candidate_population_complete or not mappings_complete:
                        result['count_eligible']=False
                        result['count_route']='INCOMPLETE_DECISION_POPULATION'
                        result['exclusion_reasons']['INCOMPLETE_DECISION_POPULATION']=1
                    if not feature_coverage['complete']:
                        result['exclusion_reasons']['REQUIRED_PAST_FEATURES_UNKNOWN_FOR_POSSIBLE_EARLIER_MATCH']=1
                    if validation_available:
                        contract={**scope,**store.period_contract(scope),'parent_policy_version':store.PARENT_POLICY}
                        candidate=candidates[scope['candidate_key']]
                        validation_store.register_supported_acceptance(conn,contract,candidate)
                        validated=validation_store.evaluate_scope(conn,contract,rows,now=now,
                            candidate_definition={**candidate,'direction_mode':candidate.get('research_orientation','NORMAL')},
                            source_coverage_complete=bool(candidate_population_complete and mappings_complete),truncated=truncated,
                            common_window_rows=common_rows,registered_attempts=attempts)
                        result['prospective_validation']=validated
                        result['research_ready']=validated['research_ready']
                        result['validation_status']=validated['validation_status']
                        if 'all_period_metrics' in validated:
                            question_store.apply_validation_cohorts(result,validated)
                    counts=store.persist_scope(conn,scope,rows,result,now=now)
                    question_store.record_scope_trial(conn,candidates[scope['candidate_key']],scope,result,input_sha=input_sha,now=now)
                    conn.execute('UPDATE research_ordered_formula_scopes SET evaluation_input_sha256=%s WHERE scope_key=%s',(input_sha,scope['scope_key']))
                    conn.commit()
                    summary['scopes_evaluated']+=1
                    summary['truncated_scopes']+=int(truncated)
                    for name in ('episodes','upserts'):
                        summary[name]+=counts[name]
                summary['evaluation_seconds']=round(time.monotonic()-evaluation_started,3)
            finally:
                conn.rollback()
                conn.execute('SELECT pg_advisory_unlock(%s)',(_LOCK_ID,))
                conn.commit()
        summary['elapsed_seconds']=round(time.monotonic()-started,3)
        self.metrics['runs']+=1
        self.metrics['last_run_utc']=now.isoformat()
        self.metrics['last_error']=None
        self.metrics['last_stage']='COMPLETE'
        self.metrics['last_summary']=dict(summary)
        return summary


WORKER=ResearchFormulaOrderedWorker()
