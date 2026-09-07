"""Slow intake must leave bounded, committed formula evaluation progress."""
from contextlib import ExitStack
import json
from unittest.mock import patch

import research_formula_ordered_worker as worker
from research_formula_ordered_v7_selftest import AS_OF, row


class Connection:
    def __init__(self):
        self.calls=[]
        self.committed=set()
        self.pending=set()

    def __enter__(self): return self
    def __exit__(self,*args): return False
    def cursor(self): return self
    def execute(self,sql,params=()):
        self.calls.append((sql,params))
        if 'SET evaluation_input_sha256=' in sql:
            self.pending.add(params[1])
        return self
    def executemany(self,sql,records):
        for params in records: self.execute(sql,params)
    def fetchone(self): return {'acquired':True,'n':5}
    def commit(self):
        self.committed.update(self.pending)
        self.pending.clear()
    def rollback(self): self.pending.clear()


def exercise(*,source_complete=True,scope_seconds=20,priority_refresh=False):
    conn=Connection()
    clock=[0.0]
    candidate=worker.evaluator.candidate_catalog()[0]
    period='SINCE_20260904'
    scopes=[{'scope_key':str(bps),'candidate_key':candidate['formula_id'],
        'symbol':'ALL','direction':'LONG','window_minutes':60,'threshold_bps':bps,
        'period_key':period,'period_start_utc':worker.store.PERIODS[period],
        'result':{}} for bps in (25,50,75,100,125)]
    loaded=[]
    published=[]
    if priority_refresh:
        # A previously qualified grant can outlive a newer OPEN-wave result.
        # The input hash is unchanged, but its qualification must still expire.
        scopes[0]['result']={'research_ready':False}
        scopes[0]['evaluation_input_sha256']=worker.store.digest({
            'input':'fixed-input','validation_available':True,'registered_attempts':5})

    def ingest(conn,catalog,*,now,event_limit):
        assert event_limit==32 and now==AS_OF
        clock[0]+=85  # Longer than the entire former shared 40-second budget.
        return {'events_checked':32,'inverse_source_event_ids':[]}

    def due(conn,limit,*,candidate_keys):
        assert limit==128 and list(candidate_keys)==[candidate['formula_id']]
        return worker.store.ScopeScheduleBatch(
            [scope for scope in scopes if scope['scope_key'] not in conn.committed][:limit],2)

    def load(conn,scope,*,row_limit,now):
        assert row_limit==2000 and now==AS_OF
        loaded.append(scope['scope_key'])
        clock[0]+=scope_seconds
        # Use real calculator labels, real formula summaries and real outbox
        # serialization. Only database reads and elapsed time are simulated.
        return [row(i+1,wave=f'wave-{i}',threshold=scope['threshold_bps'])
                for i in range(5)],False

    service=worker.ResearchFormulaOrderedWorker()
    with ExitStack() as stack:
        overrides=[(worker,'psycopg',object()),
            (worker,'_database_url',lambda:'postgresql://selftest'),
            (worker,'_connect',lambda url:conn),
            (worker,'_EVENT_LIMIT',32),(worker,'_SCOPE_LIMIT',128),
            (worker,'_ROW_LIMIT',2000),(worker,'_PASS_SECONDS',40),
            (worker.time,'monotonic',lambda:clock[0]),
            (worker.store,'register_catalog',lambda conn:[candidate]),
            (worker.store,'ingest_matches',ingest),
            (worker.inverse_store,'available',lambda conn:False),
            (worker.validation_store,'schema_status',lambda conn:{'schema_present':False}),
            (worker.store,'source_population_complete',lambda *a,**kw:source_complete),
            (worker.store,'due_scopes',due),
            (worker.store,'candidate_feature_coverage_complete',lambda *a,**kw:{'complete':True}),
            (worker.store,'load_scope_rows',load),
            (worker.store,'membership_complete',lambda *a,**kw:True),
            (worker.store,'common_window_rows',lambda *a:{}),
            (worker.store,'period_coverage',lambda *a,**kw:{})]
        for target,name,value in overrides: stack.enter_context(patch.object(target,name,value))
        if priority_refresh:
            def prioritize(conn,ordinary,*,now,limit,candidate_keys):
                assert ordinary==[] and limit==8 and now==AS_OF
                return [] if '25' in conn.committed else [scopes[0]]
            additional=[(worker.experimental_worker,'enabled',lambda:True),
                (worker.experimental_store,'available',lambda conn:True),
                (worker.experimental_store,'prioritize',prioritize),
                (worker.validation_store,'schema_status',lambda conn:{'schema_present':True}),
                (worker.validation_store,'register_supported_acceptance',lambda *a,**kw:None),
                (worker.validation_store,'evaluate_scope',lambda *a,**kw:{
                    'research_ready':False,'validation_status':'VALIDATING_PROSPECTIVE'}),
                (worker.experimental_store,'publish_evaluation',lambda conn,scope,*a,**kw:published.append(scope['scope_key'])),
                (worker.question_store,'evaluation_input',lambda *a,**kw:'fixed-input')]
            for target,name,value in additional: stack.enter_context(patch.object(target,name,value))
        first=service.run_once(now=AS_OF)
        first_committed=set(conn.committed)
        first_loaded=list(loaded)
        second=service.run_once(now=AS_OF)
    if priority_refresh:
        assert first_loaded[0]=='25' and published.count('25')==1
        assert first['scopes_evaluated']==2 and first['unchanged_scopes_skipped']==0
    payloads=[json.loads(params[2]) for sql,params in conn.calls
              if 'INSERT INTO research_sheet_upsert_outbox' in sql]
    formulas=[payload['row'] for payload in payloads if payload['sheet']=='Formula_Results']
    return first,second,first_committed,first_loaded,conn.committed,formulas,service


def run():
    first,second,committed,loaded,all_committed,formulas,service=exercise()
    assert first['scopes_evaluated']==2 and first['upserts']>0
    assert committed==set(loaded)=={'25','50'}  # Third scope was never started.
    assert first['preparation_seconds']==85 and first['evaluation_seconds']==40
    assert first['elapsed_seconds']==125 and first['evaluation_budget_exhausted']
    assert first['scopes_fetched']==5 and first['evaluation_budget_seconds']==40
    assert second['scopes_evaluated']==2 and all_committed=={'25','50','75','100'}
    assert all(item['independent_episodes']==5 and item['hit_rate']==100 for item in formulas)
    assert service.metrics['last_summary']==second and service.metrics['last_stage']=='COMPLETE'

    # A scope that crosses the time allowance is committed atomically. The
    # next scope remains pending; the worker does not abort partial evidence.
    over,_,committed,loaded,_,_,_=exercise(scope_seconds=45)
    assert over['scopes_evaluated']==1 and committed==set(loaded)=={'25'}
    assert over['evaluation_seconds']==45 and over['evaluation_budget_exhausted']

    # Fair scheduling cannot promote an incompletely scanned population.
    blocked,_,_,_,_,formulas,_=exercise(source_complete=False)
    assert blocked['scopes_evaluated']==2 and not blocked['source_population_complete']
    assert all(item['status']=='INCOMPLETE_DECISION_POPULATION'
        and item['independent_episodes']==0 and item['hit_rate']==''
        and not item['meets_min_5'] for item in formulas)
    exercise(priority_refresh=True)
    print('ordered formula worker: slow intake, bounded committed progress, resume and incomplete coverage PASS')


if __name__=='__main__': run()
