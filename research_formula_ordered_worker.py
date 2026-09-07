"""Recurring descriptive Formula evidence over ordered v7 and causal BTC parents."""
from __future__ import annotations
import asyncio
from datetime import datetime,timezone
import os
import time
from typing import Any
import research_formula_ordered_store as store
import research_formula_ordered_v7 as evaluator
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
_LOCK_ID=588794583090203387


def _database_url()->str:
    dedicated=os.getenv('RESEARCH_DATABASE_URL','').strip()
    return dedicated or (os.getenv('DATABASE_URL','').strip() if os.getenv('RESEARCH_USE_PRIMARY_DATABASE','').strip().lower() in _TRUE else '')


def _connect(url:str):
    return psycopg.connect(url,row_factory=dict_row,connect_timeout=5,options='-c statement_timeout=15000 -c lock_timeout=1000')


class ResearchFormulaOrderedWorker:
    def __init__(self)->None:
        self._task=None
        self._stopping=False
        self._schema_ready=False
        self.metrics={'runs':0,'failures':0,'last_run_utc':None,'last_error':None,'last_summary':None}

    def status(self)->dict[str,Any]:
        return {'enabled':_ENABLED,'configured':bool(_database_url()),'running':bool(self._task and not self._task.done()),
            'schema_ready':self._schema_ready,'poll_seconds':_POLL,'formula_version':store.FORMULA_VERSION,
            'parent_policy_version':store.PARENT_POLICY,'outcome_method_version':evaluator.METHOD_VERSION,
            'research_periods':[store.period_contract({'period_key':key}) for key in store.PERIODS],
            'candidate_count':len(evaluator.candidate_catalog()),'scope_limit_per_pass':_SCOPE_LIMIT,'event_limit_per_pass':_EVENT_LIMIT,
            'thresholds_bps':list(evaluator.THRESHOLDS_BPS),'horizons_minutes':list(evaluator.HORIZONS_MINUTES),
            'evidence_policy':'One causally verified BTC parent across all coins/time; earliest frozen matching cohort',
            'research_scope':'Descriptive singles/pairs/triple total-score65 screen; delivered alerts only; metrics stop at first touch',
            'live_effect':'NONE','remaining_validation':'Prospective/full-horizon probability/asymmetry validation not yet implemented',
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
                print(f'[ordered-formula] run failed: {exc!r}',flush=True)
            await asyncio.sleep(_POLL)

    def run_once(self,*,now:datetime|None=None)->dict[str,Any]:
        now=now or datetime.now(timezone.utc)
        started=time.monotonic()
        summary={'scopes_evaluated':0,'episodes':0,'upserts':0,'truncated_scopes':0,'locked':False}
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
                catalog=store.register_catalog(conn)
                summary.update(store.ingest_matches(conn,catalog,now=now,event_limit=_EVENT_LIMIT))
                conn.commit()
                period_population={key:store.source_population_complete(conn,now=now,period_key=key) for key in store.PERIODS}
                summary['source_population_complete_by_period']=period_population
                summary['source_population_complete']=all(period_population.values())
                scopes=store.due_scopes(conn,_SCOPE_LIMIT)
                conn.commit()
                for scope in scopes:
                    if time.monotonic()-started>=_PASS_SECONDS:
                        break
                    population_complete=period_population[scope['period_key']]
                    rows,truncated=store.load_scope_rows(conn,scope,row_limit=_ROW_LIMIT,now=now)
                    mappings_complete=store.membership_complete(conn,scope,now=now)
                    result=evaluator.summarize_scope(rows,analysis_as_of_utc=now,truncated=truncated,source_coverage_complete=bool(population_complete and mappings_complete))
                    result['period_coverage']=store.period_coverage(conn,scope,now=now)
                    result['source_population_complete']=population_complete
                    result['membership_population_complete']=mappings_complete
                    if not population_complete or not mappings_complete:
                        result['count_eligible']=False
                        result['count_route']='INCOMPLETE_DECISION_POPULATION'
                        result['exclusion_reasons']['INCOMPLETE_DECISION_POPULATION']=1
                    counts=store.persist_scope(conn,scope,rows,result,now=now)
                    conn.commit()
                    summary['scopes_evaluated']+=1
                    summary['truncated_scopes']+=int(truncated)
                    for name in ('episodes','upserts'):
                        summary[name]+=counts[name]
            finally:
                conn.rollback()
                conn.execute('SELECT pg_advisory_unlock(%s)',(_LOCK_ID,))
                conn.commit()
        self.metrics['runs']+=1
        self.metrics['last_run_utc']=now.isoformat()
        self.metrics['last_error']=None
        self.metrics['last_summary']=dict(summary)
        return summary


WORKER=ResearchFormulaOrderedWorker()
