"""Slow intake must leave bounded, committed formula evaluation progress."""
from contextlib import ExitStack
import gc
import json
from unittest.mock import patch
import weakref

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


class TrackedRows(list):
    """Weak references observe ownership without retaining the loaded rows."""


class TrackedMapping(dict):
    pass


def exercise_scope_lifetimes(*,validation_available,check_released=True):
    """Changed, unchanged, changed scopes keep only the current scope's roots.

    Real input hashes, first-touch summaries, persistence and outbox payloads
    remain in use. Plain function seams and weak references avoid mock-call
    histories retaining the objects whose lifetimes are under test.
    """
    assert gc.isenabled()  # No collection, GC disabling or allocator tuning.
    conn=Connection()
    commit_points=[]
    original_commit=conn.commit
    def commit():
        original_commit()
        commit_points.append((len(conn.calls),sorted(conn.committed)))
    conn.commit=commit
    candidate=worker.evaluator.candidate_catalog()[0]
    period='SINCE_20260904'
    scopes=[{'scope_key':str(bps),'candidate_key':candidate['formula_id'],
        'symbol':'ALL','direction':'LONG','window_minutes':60,'threshold_bps':bps,
        'period_key':period,'period_start_utc':worker.store.PERIODS[period],
        'result':{}} for bps in (25,50,75)]
    roots=[]
    load_checks=[]
    loaded=[]
    summarized=[]
    validated_scopes=[]
    registered=[]
    seeded=[]

    def source_rows(scope):
        return [row(int(scope['scope_key'])*100+i,wave=f"{scope['scope_key']}-wave-{i}",
                    threshold=scope['threshold_bps']) for i in range(5)]

    def window_rows(rows):
        return {item['event_id']:{'status':'DATA_MISSING'} for item in rows}

    def input_hash(scope):
        rows=source_rows(scope)
        common=window_rows(rows)
        value=worker.question_store.evaluation_input(scope,
            [{**item,'common_window_record':common[item['event_id']]} for item in rows],
            now=AS_OF,population_complete=True,membership_complete=True)
        return worker.store.digest({'input':value,'validation_available':validation_available,
                                    'registered_attempts':5})
    scopes[1]['evaluation_input_sha256']=input_hash(scopes[1])

    def track(scope_key,name,value):
        roots.append((scope_key,name,weakref.ref(value)))
        return value

    def load(conn,scope,*,row_limit,now):
        assert row_limit==2000 and now==AS_OF
        alive=[(scope_key,name) for scope_key,name,ref in roots if ref() is not None]
        load_checks.append((scope['scope_key'],alive))
        if check_released:
            assert not alive, f'Previous scope roots retained before load: {alive}'
        loaded.append(scope['scope_key'])
        return track(scope['scope_key'],'rows',TrackedRows(source_rows(scope))),False

    def common(conn,scope,rows):
        return track(scope['scope_key'],'common_rows',TrackedMapping(window_rows(rows)))

    real_summary=worker.evaluator.summarize_scope
    def summarize(rows,**kwargs):
        scope_key=loaded[-1]
        summarized.append(scope_key)
        return track(scope_key,'result',TrackedMapping(real_summary(rows,**kwargs)))

    def register(conn,contract,candidate_definition):
        registered.append(contract['scope_key'])

    def validate(conn,contract,rows,**kwargs):
        assert kwargs['registered_attempts']==5 and kwargs['now']==AS_OF
        assert kwargs['source_coverage_complete'] and not kwargs['truncated']
        assert set(kwargs['common_window_rows'])=={item['event_id'] for item in rows}
        validated_scopes.append(contract['scope_key'])
        # The DB-backed validation boundary is synthetic; the returned graph
        # is still attached to the real result and serialized by real stores.
        return track(contract['scope_key'],'validated',TrackedMapping({
            'research_ready':False,'validation_status':'VALIDATING_PROSPECTIVE',
            'registered_attempts':5,'discovery':{'event_ids':[item['event_id'] for item in rows]}}))

    def seed(conn,scope,version):
        assert version==worker.store.scope_formula_version(scope)
        seeded.append(scope['scope_key'])
        return 0

    service=worker.ResearchFormulaOrderedWorker()
    with ExitStack() as stack:
        overrides=[(worker,'psycopg',object()),
            (worker,'_database_url',lambda:'postgresql://selftest'),
            (worker,'_connect',lambda url:conn),
            (worker,'_ROW_LIMIT',2000),(worker,'_PASS_SECONDS',40),
            (worker.time,'monotonic',lambda:0.0),
            (worker.store,'register_catalog',lambda conn:[candidate]),
            (worker.store,'ingest_matches',lambda *a,**kw:{'events_checked':0,'inverse_source_event_ids':[]}),
            (worker.inverse_store,'available',lambda conn:False),
            (worker.experimental_worker,'enabled',lambda:False),
            (worker.validation_store,'schema_status',lambda conn:{'schema_present':validation_available}),
            (worker.validation_store,'register_supported_acceptance',register),
            (worker.validation_store,'evaluate_scope',validate),
            (worker.store,'source_population_complete',lambda *a,**kw:True),
            (worker.store,'due_scopes',lambda *a,**kw:worker.store.ScopeScheduleBatch(scopes,2)),
            (worker.store,'candidate_feature_coverage_complete',lambda *a,**kw:{'complete':True}),
            (worker.store,'load_scope_rows',load),
            (worker.store,'membership_complete',lambda *a,**kw:True),
            (worker.store,'common_window_rows',common),
            (worker.store,'period_coverage',lambda *a,**kw:{}),
            (worker.evaluator,'summarize_scope',summarize),
            (worker.store.publication,'seed_missing_scope',seed)]
        for target,name,value in overrides: stack.enter_context(patch.object(target,name,value))
        summary=service.run_once(now=AS_OF)

    assert loaded==['25','50','75'] and summarized==['25','75'] and seeded==['50']
    assert validated_scopes==registered==(['25','75'] if validation_available else [])
    assert [(key,name) for key,name,_ in roots]==[
        (key,name) for key in ('25','50','75')
        for name in (('rows','common_rows') if key=='50' else
                     ('rows','common_rows','result','validated') if validation_available else
                     ('rows','common_rows','result'))]
    assert all(ref() is None for _,_,ref in roots), 'Completed pass retains scope roots'
    assert summary['scopes_evaluated']==2 and summary['unchanged_scopes_skipped']==1
    assert summary['episodes']==10 and summary['upserts']==2
    assert len(commit_points)==7 and conn.committed=={'25','75'}
    assert not summary['evaluation_budget_exhausted'] and summary['scopes_fetched']==3
    payloads=[json.loads(params[2]) for sql,params in conn.calls
              if 'INSERT INTO research_sheet_upsert_outbox' in sql]
    assert len(payloads)==2 and all(item['row']['independent_episodes']==5
        and item['row']['hit_rate']==100 for item in payloads)
    assert [params[1] for sql,params in conn.calls
            if 'SET last_evaluated_at_utc=' in sql]==['50']
    # Scalar bytes provide a baseline-comparison seam without retaining rows,
    # result graphs or nondeterministic phase timing receipts.
    trace=worker.store.canonical({'summary':summary,'sql':conn.calls,'commits':commit_points,
        'loaded':loaded,'summarized':summarized,'validated':validated_scopes,
        'registered':registered,'seeded':seeded})
    return trace,load_checks


def exercise(*,source_complete=True,scope_seconds=20,priority_refresh=False,idle_seed=False):
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
    if idle_seed:
        scopes[0]['evaluation_input_sha256']=worker.store.digest({
            'input':'fixed-input','validation_available':False,'registered_attempts':5})

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
        if idle_seed:
            def seed(conn,scope,version):
                assert scope['scope_key']=='25' and version==worker.store.scope_formula_version(scope)
                published.append(scope['scope_key'])
                conn.pending.add(scope['scope_key'])
                return 1
            stack.enter_context(patch.object(worker.question_store,'evaluation_input',lambda *a,**kw:'fixed-input'))
            stack.enter_context(patch.object(worker.store.publication,'seed_missing_scope',seed))
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
    if idle_seed:
        assert published==['25'] and first['unchanged_scopes_skipped']==1
        assert first['scopes_evaluated']==1 and first['upserts']==2
    payloads=[json.loads(params[2]) for sql,params in conn.calls
              if 'INSERT INTO research_sheet_upsert_outbox' in sql]
    formulas=[payload['row'] for payload in payloads if payload['sheet']=='Formula_Current']
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
    exercise(idle_seed=True)
    for validation_available in (False,True):
        _,checks=exercise_scope_lifetimes(validation_available=validation_available)
        assert checks==[('25',[]),('50',[]),('75',[])]
    print('ordered formula worker: slow intake, bounded committed progress, resume, incomplete coverage and scope lifetimes PASS')


if __name__=='__main__': run()
