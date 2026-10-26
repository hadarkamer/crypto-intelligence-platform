"""Offline differential and transaction tests for observation-only DOGE hooks."""
from __future__ import annotations

import ast
import asyncio
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from types import ModuleType, SimpleNamespace
import sys
import time
import unittest
from unittest.mock import patch
from uuid import UUID

ROOT = Path(__file__).resolve().parent
NOW = datetime(2026, 10, 26, 10, 5, tzinfo=timezone.utc)
# Exact pre-observation source from the pinned 2c2a94e production revision.
BASELINE_STORE_FUNCTIONS = 'def load_sequence_history(conn:Any,changed:Mapping[int,Mapping[str,Any]])->tuple[list[dict[str,Any]],dict[str,int]]:\n    """Read the complete causal union in bounded pages, before any screen ACK.\n\n    Historical replay and live intake can be days apart. The gap between\n    their four-hour windows is not sequence evidence and must not exhaust a\n    shared source cap. A server cursor holds one metadata SELECT snapshot;\n    fetching pages neither changes that source set nor commits partial work.\n    """\n    stats={\'sequence_source_rows\':0,\'sequence_source_pages\':0,\n        \'sequence_projected_rows\':0,\'sequence_projection_batches\':0}\n    if not changed: return [],stats\n    windows={}\n    for event in changed.values():\n        # These are the stored direction/symbol returned by _EVENT_PROJECT;\n        # inverse candidate direction is assigned only after sequence capture.\n        when=event[\'alert_time_utc\']\n        windows.setdefault((event[\'symbol\'],event[\'direction\']),[]).append((when-timedelta(hours=4),when))\n    merged=[]\n    for (symbol,direction),intervals in sorted(windows.items()):\n        union=[]\n        for lower,upper in sorted(intervals):\n            if union and lower<=union[-1][1]:\n                union[-1]=(union[-1][0],max(union[-1][1],upper))\n            else:\n                union.append((lower,upper))\n        merged.extend((symbol,direction,lower,upper) for lower,upper in union)\n    placeholders=\',\'.join([\'(%s,%s,%s,%s)\']*len(merged))\n    query=\'\'\'WITH wanted(symbol,direction,lower_utc,upper_utc) AS (VALUES \'\'\'+placeholders+\'\'\')\n        SELECT e.event_id,e.symbol,e.direction,e.alert_time_utc\n        FROM wanted w JOIN research_events e\n          ON e.symbol=w.symbol AND e.direction=w.direction\n          AND e.alert_time_utc>=w.lower_utc AND e.alert_time_utc<w.upper_utc\n        WHERE e.event_kind=\'ALERT\' AND e.delivery_status=\'DELIVERED\'\n          AND e.direction IN (\'LONG\',\'SHORT\')\n        ORDER BY e.event_id\'\'\'\n    history=[]\n    with conn.cursor(name=\'ordered_sequence_\'+uuid4().hex) as source_cursor:\n        source_cursor.execute(query,tuple(value for interval in merged for value in interval))\n        while source:=source_cursor.fetchmany(512):\n            stats[\'sequence_source_pages\']+=1\n            stats[\'sequence_source_rows\']+=len(source)\n            source_by_id={old[\'event_id\']:old for old in source}\n            ids=[old[\'event_id\'] for old in source]\n            if len(source_by_id)!=len(ids) or ids!=sorted(ids) or (history and ids[0]<=history[-1][\'event_id\']):\n                raise RuntimeError(\'bounded causal sequence source order changed\')\n            for start in range(0,len(ids),64):\n                batch=ids[start:start+64]\n                projected=conn.execute(\'\'\'WITH picked AS MATERIALIZED (\n                    SELECT unnest(%s::bigint[]) AS event_id) \'\'\'+_EVENT_PROJECT,(batch,)).fetchall()\n                if [event[\'event_id\'] for event in projected]!=batch or any(\n                    any(event[key]!=source_by_id[event[\'event_id\']][key]\n                        for key in (\'symbol\',\'direction\',\'alert_time_utc\'))\n                    or event[\'event_kind\']!=\'ALERT\' or event[\'delivery_status\']!=\'DELIVERED\'\n                    for event in projected):\n                    raise RuntimeError(\'bounded causal sequence projection lost or changed source rows\')\n                history.extend(projected)\n                stats[\'sequence_projection_batches\']+=1\n    stats[\'sequence_projected_rows\']=len(history)\n    return history,stats\n\ndef recent_unscreened_event_ids(conn,*,now:datetime,limit:int=32)->list[int]:\n    """Inspect a finite live tail before filtering already screened siblings.\n\n    A late Magnet burst must not repeatedly occupy all 32 live slots while\n    earlier Max Pain/CVD alerts from the same scan wait on historical replay.\n    The ascending history cursor still recovers everything outside this tail.\n    """\n    rows=conn.execute(\'\'\'WITH recent_source AS MATERIALIZED (\n        SELECT event_id FROM research_events\n        WHERE alert_time_utc>=%s AND alert_time_utc<=%s\n          AND event_kind=\'ALERT\' AND delivery_status=\'DELIVERED\'\n          AND direction IN (\'LONG\',\'SHORT\')\n        ORDER BY event_id DESC LIMIT 256\n    ) SELECT recent.event_id FROM recent_source recent\n      WHERE NOT EXISTS (\n          SELECT 1 FROM research_ordered_feature_screens checked\n          WHERE checked.event_id=recent.event_id AND checked.feature_version=%s\n          LIMIT 1 OFFSET 0\n      ) ORDER BY recent.event_id DESC LIMIT %s\'\'\',\n      (SOURCE_START_UTC,now,questions.VERSION,max(1,min(32,int(limit))))).fetchall()\n    return [int(row[\'event_id\']) for row in rows]\n\ndef ingest_matches(conn: Any,catalog: list[dict[str,Any]],*,now: datetime,event_limit: int=128) -> dict[str,Any]:\n    conn.execute(\'INSERT INTO research_ordered_formula_worker_state(worker_key) VALUES(%s) ON CONFLICT DO NOTHING\',(WORKER_KEY,))\n    cursor = conn.execute(\'SELECT last_event_id FROM research_ordered_formula_worker_state WHERE worker_key=%s FOR UPDATE\',(WORKER_KEY,)).fetchone()[\'last_event_id\']\n    since = SOURCE_START_UTC\n    events = conn.execute(\'\'\'WITH picked AS MATERIALIZED (\n        SELECT event_id FROM research_events WHERE event_id>%s AND alert_time_utc>=%s\n          AND alert_time_utc<=%s AND event_kind=\'ALERT\' AND delivery_status=\'DELIVERED\'\n          AND direction IN (\'LONG\',\'SHORT\') ORDER BY event_id LIMIT %s\n        ) \'\'\' + _EVENT_PROJECT,(cursor,since,now,event_limit)).fetchall()\n    # Project only unscreened current-version events from the bounded live\n    # tail. Immutable older inputs and delayed transitions retain the existing\n    # ascending/wrap-around replay; past-feature changes have their own queue.\n    recent_ids=recent_unscreened_event_ids(conn,now=now,limit=event_limit)\n    recent=(conn.execute(\'\'\'WITH picked AS MATERIALIZED (\n        SELECT unnest(%s::bigint[]) AS event_id\n        ) \'\'\' + _EVENT_PROJECT,(recent_ids,)).fetchall() if recent_ids else [])\n    conn.execute(\'UPDATE research_ordered_formula_worker_state SET last_event_id=%s,initial_scan_complete=initial_scan_complete OR %s,updated_at_utc=NOW() WHERE worker_key=%s\',\n                 (max(row[\'event_id\'] for row in events) if events else 0,not events,WORKER_KEY))\n    records, symbols, missing_features = [], set(), 0\n    unique = {event[\'event_id\']:event for event in [*events,*recent]}\n    import research_past_price_features_store as past_store\n    import research_past_price_features as past_features\n    past_refresh=[]\n    if past_store.available(conn):\n        past_refresh=past_store.pending_refresh_ids(conn,limit=32)\n        if past_refresh:\n            refreshed=conn.execute(\'\'\'WITH picked AS MATERIALIZED (\n                SELECT event_id FROM research_events WHERE event_id=ANY(%s::bigint[])\n                  AND alert_time_utc>=%s AND alert_time_utc<=%s AND event_kind=\'ALERT\'\n                  AND delivery_status=\'DELIVERED\' AND direction IN (\'LONG\',\'SHORT\')\n                ) \'\'\'+_EVENT_PROJECT,([row[\'event_id\'] for row in past_refresh],since,now)).fetchall()\n            unique.update({event[\'event_id\']:event for event in refreshed})\n        past=past_store.load_by_event_ids(conn,list(unique))\n        for event_id,event in unique.items():\n            if event_id in past:\n                # Computed later is allowed only because this adapter validates\n                # closed source candles at/before the original decision cutoff.\n                event[\'causal_past_features\']=past_features.flatten_event_features(past[event_id],event[\'direction\'])\n                event[\'causal_past_feature_sha256\']=past[event_id].get(\'feature_sha256\')\n    changed = question_store.new_feature_events(conn,unique)\n    sequence_source={}\n    history,sequence_stats=load_sequence_history(conn,changed)\n    for prior in history:\n        base=evaluator.extract_event_features(prior)\n        base.update(questions.extended_features(prior))\n        sequence_source[prior[\'event_id\']]=(prior,base)\n    features_by_id, inverse_requests = {}, set()\n    for event in changed.values():\n        symbols.add(event[\'symbol\'])\n        features = evaluator.extract_event_features(event)\n        features.update(questions.extended_features(event))\n        features.update(event.get(\'causal_past_features\') or {})\n        features.update(questions.sequence_features(event,features,[value for key,value in sequence_source.items() if key!=event[\'event_id\']]))\n        features_by_id[event[\'event_id\']]=features\n        if not any(name.endswith(\'.aligned_score\') and value is not None for name,value in features.items()):\n            missing_features += 1\n        features_json = None\n        for candidate in catalog:\n            if evaluator.matches(candidate,features,event[\'direction\']):\n                snapshot = event.get(\'engine_snapshot\') or {}\n                inverse=candidate.get(\'research_orientation\')==\'INVERSE\'\n                if inverse: inverse_requests.add(event[\'event_id\'])\n                direction=({\'LONG\':\'SHORT\',\'SHORT\':\'LONG\'}[event[\'direction\']] if inverse else event[\'direction\'])\n                if features_json is None:\n                    features_json = canonical(features)\n                records.append((candidate[\'formula_id\'],event[\'event_id\'],event[\'symbol\'],direction,event[\'alert_time_utc\'],\n                                snapshot.get(\'sheet_snapshot_id\') or event[\'event_fingerprint\'],event[\'current_price\'],features_json))\n    screens=question_store.record_screens(conn,changed,features_by_id,catalog,now=now,inverse_requested=inverse_requests)\n    for refresh in past_refresh:\n        # ACK is conditional on the captured version and commits atomically\n        # with screens/matches. Ignored non-LIVE sources do not jam this queue.\n        past_store.ack_refresh(conn,refresh[\'event_id\'],refresh[\'feature_sha256\'])\n    if unique:\n        with conn.cursor() as cur:\n            cur.executemany(\'INSERT INTO research_ordered_formula_event_checks(event_id) VALUES(%s) ON CONFLICT DO NOTHING\',[(event_id,) for event_id in unique])\n    if records:\n        with conn.cursor() as cur:\n            cur.executemany(\'\'\'INSERT INTO research_ordered_formula_matches(candidate_key,event_id,symbol,direction,alert_time_utc,snapshot_id,entry_price,decision_features)\n                VALUES(%s,%s,%s,%s,%s,%s,%s,%s::jsonb) ON CONFLICT(candidate_key,event_id) DO NOTHING\'\'\',records)\n    scope_discovery=discover_scope_cells(conn,catalog,\n        {(record[0],record[2],record[3]) for record in records})\n    return {\'events_checked\':len(changed),\'fresh_unscreened_events_selected\':len(recent_ids),\'screen_feature_version\':questions.VERSION,\'events_unchanged_skipped\':len(unique)-len(changed),\'missing_total_score_features\':missing_features,\'matches_observed\':len(records),\'symbols\':sorted(symbols),\'cursor\':max((e[\'event_id\'] for e in events),default=0),\'source_start_utc\':since.isoformat(),\'inverse_source_event_ids\':sorted(inverse_requests),**screens,**sequence_stats,**scope_discovery}'


def _event(event_id, symbol='DOGE', minute=0):
    return dict(event_id=event_id, symbol=symbol, direction='SHORT',
        alert_time_utc=NOW.replace(minute=0)-timedelta(minutes=minute),
        event_fingerprint=f'fingerprint-{event_id}', current_price=1.0,
        event_type='MAX_PAIN', event_kind='ALERT', delivery_status='DELIVERED',
        engine_snapshot={'sheet_snapshot_id':f'snapshot-{event_id}',
                         'watch_scan_id':'current-scan' if event_id in (10,30,31,90) else 'other'})


class RecordingCapture:
    def __init__(self, trace=None):
        self.trace = trace if trace is not None else []
        self.records = []
        self.submitted = []
        self.discarded = []
        self.fail_record = self.fail_enter = self.fail_exit = self.fail_submit = False
        self.doge_event_count = 0

    def record(self, kind, payload):
        if self.fail_record:
            raise RuntimeError('capture record failed')
        if kind == 'current_events':
            self.doge_event_count = len(payload['events'])
        self.records.append((kind, deepcopy(payload)))

    def current(self):
        return self

    @contextmanager
    def observe(self):
        if self.fail_enter:
            raise RuntimeError('capture enter failed')
        observation = SimpleNamespace(closed=False)
        self.trace.append('capture_enter')
        try:
            yield observation
        finally:
            observation.closed = True
            self.trace.append('capture_exit')
            if self.fail_exit:
                raise RuntimeError('capture exit failed')

    def discard(self, observation):
        self.discarded.append(observation)
        self.trace.append('capture_discard')

    def submit_after_commit(self, observation):
        if self.fail_submit:
            raise RuntimeError('capture submit failed')
        if not observation.closed:
            raise AssertionError('observation was not frozen before submit')
        self.submitted.append(observation)
        self.trace.append('capture_submit')

    def status(self):
        return {'state':'QUEUED', 'queued_items':1, 'live_effect':'NONE'}


class Result:
    def __init__(self, rows):
        self.rows = rows
    def fetchall(self):
        return deepcopy(self.rows)
    def fetchone(self):
        return deepcopy(self.rows[0])


class IntakeCursor:
    def __init__(self, conn, name):
        self.conn, self.name, self.fetched = conn, name, False
    def __enter__(self):
        return self
    def __exit__(self, *_):
        return False
    def execute(self, sql, parameters):
        self.conn.trace.append(('source_execute', sql, deepcopy(parameters)))
    def fetchmany(self, size):
        self.conn.trace.append(('source_fetch', size))
        if self.fetched:
            return []
        self.fetched = True
        return [{key:event[key] for key in ('event_id','symbol','direction','alert_time_utc')}
                for event in self.conn.history]
    def executemany(self, sql, records):
        records = list(records)
        self.conn.trace.append(('executemany', sql, deepcopy(records)))
        if 'INSERT INTO research_ordered_formula_matches' in sql:
            for record in records:
                self.conn.matches.setdefault((record[0],record[1]),deepcopy(record))


class IntakeConnection:
    isolation_level = None
    def __init__(self):
        self.events = [_event(30), _event(40,'ETH')]
        self.recent = [_event(31,minute=-1)]
        # An older-time source can have a larger event_id. Keep it, as well
        # as another symbol and a current event present for a later window.
        self.history = [_event(10,minute=2),_event(20,'ETH',minute=1),
                        _event(30),_event(90,minute=1)]
        self.by_id = {event['event_id']:event for event in [*self.events,*self.recent,*self.history]}
        self.trace, self.matches = [], {('normal',30):('OLD_FIRST_MATCH',)}
    def execute(self, sql, parameters=()):
        self.trace.append(('execute',sql,deepcopy(parameters)))
        if 'SELECT last_event_id' in sql:
            return Result([{'last_event_id':0}])
        if 'event_id>%s' in sql:
            return Result(self.events)
        if 'WITH recent_source' in sql:
            return Result([{'event_id':event['event_id']} for event in self.recent])
        if 'SELECT unnest(%s::bigint[])' in sql:
            return Result([self.by_id[event_id] for event_id in parameters[0]])
        return Result([])
    def cursor(self, name=None):
        self.trace.append(('cursor','named' if name else 'plain'))
        return IntakeCursor(self,name)


def _load_store(capture, *, baseline=False, changed_ids=None, mutate_screens=False):
    trace = []
    evaluator = SimpleNamespace(
        extract_event_features=lambda event:{'probe.aligned_score':event['event_id']},
        matches=lambda candidate,features,direction:
            (trace.append(('match',candidate['formula_id'],features['probe.aligned_score'],direction))
             or candidate['hit']))
    def sequence(event, features, prior):
        trace.append(('sequence',event['event_id'],[old['event_id'] for old,_ in prior]))
        return {'sequence.fixture_count':len(prior)}
    questions = SimpleNamespace(VERSION='frozen-feature-version',
        extended_features=lambda event:{'extension':event['symbol']}, sequence_features=sequence)
    def screens(conn, changed, features, catalog, **kwargs):
        trace.append(('screens',list(changed),deepcopy(features),kwargs))
        if mutate_screens:
            for event in changed.values():
                event['engine_snapshot']['late_mutation']='after the sequence call'
            for feature in features.values():
                feature['late_mutation']='after the sequence call'
        return {'screens_written':len(changed)}
    question_store = SimpleNamespace(
        new_feature_events=lambda conn,unique:
            {key:value for key,value in unique.items() if changed_ids is None or key in changed_ids},
        record_screens=screens)
    namespace = dict(Any=object,Mapping=dict,datetime=datetime,timezone=timezone,
        timedelta=timedelta,time=time,json=json,hashlib=hashlib,
        uuid4=lambda:UUID(int=1),evaluator=evaluator,questions=questions,
        question_store=question_store,doge_sequence_capture=capture,
        WORKER_KEY='frozen-worker',SOURCE_START_UTC=datetime(2026,8,15,21,tzinfo=timezone.utc),
        discover_scope_cells=lambda conn,catalog,cells:{'scope_cells_created':len(cells)})
    tree = ast.parse((ROOT/'research_formula_ordered_store.py').read_text(encoding='utf-8'))
    keep = {'canonical','digest','load_sequence_history','recent_unscreened_event_ids','ingest_matches'}
    nodes = [node for node in tree.body if isinstance(node,ast.FunctionDef)
             and (node.name in keep or node.name.startswith('_sequence_capture_'))]
    event_project = next(node for node in tree.body if isinstance(node,ast.Assign)
        and any(isinstance(name,ast.Name) and name.id=='_EVENT_PROJECT' for name in node.targets))
    exec(compile(ast.Module(body=[event_project,*nodes],type_ignores=[]),'store-hooks','exec'),namespace)
    if baseline:
        exec(compile(BASELINE_STORE_FUNCTIONS,'frozen-2c2-store','exec'),namespace)
    return namespace,trace


@contextmanager
def _past_dependencies():
    past = ModuleType('research_past_price_features_store')
    past.available = lambda conn:True
    past.pending_refresh_ids = lambda conn,limit:[]
    past.load_by_event_ids = lambda conn,ids:{30:{'feature_sha256':'closed-prior-hash'}}
    past.ack_refresh = lambda *_:None
    features = ModuleType('research_past_price_features')
    features.flatten_event_features = lambda row,direction:{'past.closed_input':'retained-causal'}
    with patch.dict(sys.modules,{past.__name__:past,features.__name__:features}):
        yield


CATALOG = [dict(formula_id='normal',hit=True),
           dict(formula_id='inverse',hit=True,research_orientation='INVERSE'),
           dict(formula_id='miss',hit=False)]


def _intake(capture, **options):
    baseline = options.pop('baseline',False)
    namespace,trace = _load_store(capture,baseline=baseline,**options)
    conn = IntakeConnection()
    with _past_dependencies():
        result = namespace['ingest_matches'](conn,deepcopy(CATALOG),now=NOW,event_limit=128)
    return result,trace,conn


class WorkerConnection:
    def __init__(self, trace, fail_commit=None, error=None, locked=False):
        self.trace, self.commits = trace, 0
        self.fail_commit, self.error, self.locked = fail_commit, error, locked
    def __enter__(self):
        return self
    def __exit__(self,*_):
        self.trace.append('connection_exit')
        return False
    def execute(self, sql, parameters=()):
        if 'pg_try_advisory_lock' in sql:
            return Result([{'acquired':not self.locked}])
        if 'SELECT COUNT(*)' in sql:
            return Result([{'n':0}])
        return Result([])
    def commit(self):
        self.commits += 1
        self.trace.append(f'commit_{self.commits}')
        if self.commits == self.fail_commit:
            raise self.error
    def rollback(self):
        self.trace.append('rollback')


def _worker(capture, trace, *, failure=None, error=None, locked=False):
    def fail_at(stage):
        if failure == stage:
            raise error
    def intake(*args,**kwargs):
        trace.append('intake')
        fail_at('intake')
        return {'inverse_source_event_ids':[30],'events_checked':1}
    def inverse(*args,**kwargs):
        trace.append('inverse')
        fail_at('inverse')
    def population(*args,**kwargs):
        trace.append('queue')
        fail_at('queue')
        return True
    store = SimpleNamespace(register_catalog=lambda conn:deepcopy(CATALOG),ingest_matches=intake,
        inverse_reconciliation_ids=lambda conn,limit:[],PERIODS={'ALL':NOW},
        source_population_complete=population,due_scopes=lambda *a,**k:[],
        publication=SimpleNamespace(status=lambda:{}))
    conn = WorkerConnection(trace,fail_commit=2 if failure=='commit' else None,error=error,locked=locked)
    namespace = dict(contextmanager=contextmanager,Any=object,datetime=datetime,timezone=timezone,
        time=time,store=store,evaluator=SimpleNamespace(),questions=SimpleNamespace(),
        question_store=SimpleNamespace(),
        inverse_store=SimpleNamespace(available=lambda conn:True,request_inverse=inverse),
        validation_store=SimpleNamespace(schema_status=lambda conn:{'schema_present':False}),
        experimental_worker=SimpleNamespace(enabled=lambda:False),experimental_store=SimpleNamespace(),
        doge_sequence_capture=capture,psycopg=object(),_database_url=lambda:'configured',
        _connect=lambda url:conn,_timing_phase=lambda *a:None,
        _LOCK_ID=1,_EVENT_LIMIT=128,_SCOPE_LIMIT=128,_ROW_LIMIT=2000,_PASS_SECONDS=40)
    tree = ast.parse((ROOT/'research_formula_ordered_worker.py').read_text(encoding='utf-8'))
    nodes = [node for node in tree.body if (
        isinstance(node,ast.FunctionDef) and node.name.startswith('_sequence_capture_')) or
        (isinstance(node,ast.ClassDef) and node.name=='ResearchFormulaOrderedWorker')]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'worker-hooks','exec'),namespace)
    return namespace['ResearchFormulaOrderedWorker'](),namespace,conn


class IntakeIntegrationTests(unittest.TestCase):
    def test_actual_core_closes_and_recovers_the_integrated_arrays(self):
        import research_doge_sequence_capture as capture
        with capture.observe() as observation:
            _intake(capture,mutate_screens=True)
        # A deterministic connection fixture has no server; the production
        # worker's successful-COMMIT gate is tested separately below.
        observation.committed_at_utc = capture.utc_now()
        prepared = capture.prepare(observation)
        self.assertEqual(prepared['manifest']['summary']['status'],'CAPTURED')
        self.assertEqual(prepared['manifest']['summary']['gaps'],[])
        self.assertEqual(prepared['manifest']['summary']['doge_event_count'],2)
        package = capture.package_from_observation(prepared)
        self.assertEqual(capture.summarize(package),prepared['manifest']['summary'])
        self.assertFalse(prepared['manifest']['original_first_match_observed'])

    def test_original_outputs_sql_calls_and_candidate_order_are_identical(self):
        expected,expected_trace,expected_conn = _intake(None,baseline=True)
        actual,actual_trace,actual_conn = _intake(RecordingCapture())
        self.assertEqual(actual,expected)
        self.assertEqual(actual_trace,expected_trace)
        self.assertEqual(actual_conn.trace,expected_conn.trace)
        self.assertEqual(actual_conn.matches,expected_conn.matches)
        self.assertEqual(actual['events_checked'],3)
        self.assertEqual(len([item for item in actual_trace if item[0]=='match']),9)
        self.assertEqual([item[1] for item in actual_trace if item[0]=='sequence'],[30,40,31])

    def test_capture_failure_preserves_all_original_outputs_and_queries(self):
        capture = RecordingCapture()
        capture.fail_record = True
        actual,trace,conn = _intake(capture)
        expected,old_trace,old_conn = _intake(None,baseline=True)
        self.assertEqual((actual,trace,conn.trace,conn.matches),(expected,old_trace,old_conn.trace,old_conn.matches))

    def test_sequence_exception_is_the_original_object_despite_failed_recorder(self):
        capture = RecordingCapture()
        capture.fail_record = True
        namespace,_ = _load_store(capture)
        error = RuntimeError('original sequence failure')
        def broken_sequence(*args):
            raise error
        namespace['questions'].sequence_features = broken_sequence
        with _past_dependencies(), self.assertRaises(RuntimeError) as caught:
            namespace['ingest_matches'](IntakeConnection(),deepcopy(CATALOG),now=NOW)
        self.assertIs(caught.exception,error)

    def test_actual_prior_list_and_causal_base_are_frozen_before_later_mutation(self):
        capture = RecordingCapture()
        _intake(capture,mutate_screens=True)
        inputs = [payload for kind,payload in capture.records if kind=='sequence_call_input']
        outputs = [payload for kind,payload in capture.records if kind=='sequence_call_output']
        self.assertEqual([item['event_id'] for item in inputs],[30,31])
        self.assertEqual([[event['event_id'] for event,_ in item['prior']] for item in inputs],
                         [[10,20,90],[10,20,30,90]])
        self.assertEqual(inputs[0]['base_features']['past.closed_input'],'retained-causal')
        self.assertTrue(any(event['symbol']=='ETH' for event,_ in inputs[0]['prior']))
        self.assertTrue(any(event['event_id']==90 for event,_ in inputs[0]['prior']))
        self.assertNotIn('late_mutation',inputs[0]['event']['engine_snapshot'])
        self.assertNotIn('late_mutation',outputs[0]['final_features'])
        self.assertEqual([item['candidate_key'] for item in outputs[0]['candidate_outcomes']],
                         ['normal','inverse','miss'])
        self.assertEqual([item['matched'] for item in outputs[0]['candidate_outcomes']],[True,True,False])
        self.assertEqual(outputs[0]['candidate_outcomes'][1]['routed_direction'],'LONG')
        self.assertLessEqual(outputs[0]['started_monotonic_ns'],outputs[0]['finished_monotonic_ns'])

    def test_named_cursor_exhaustion_and_each_actual_projection_have_brackets(self):
        capture = RecordingCapture()
        _intake(capture)
        reads = [payload for kind,payload in capture.records if kind=='history_read']
        self.assertEqual([item['phase'] for item in reads],
                         ['metadata_execute','metadata_fetch','projection','metadata_fetch'])
        self.assertEqual(reads[-1]['rows'],[])
        self.assertTrue(reads[-1]['exhausted'])
        self.assertEqual(reads[2]['requested_event_ids'],[10,20,30,90])
        self.assertEqual([row['event_id'] for row in reads[2]['rows']],[10,20,30,90])
        for item in reads:
            self.assertLessEqual(item['started_monotonic_ns'],item['finished_monotonic_ns'])
            self.assertIsNone(item['configured_isolation_level'])
            self.assertIn('unmeasured',item['database_snapshot_claim'])

    def test_unchanged_recent_events_do_not_force_historical_rescreen(self):
        capture = RecordingCapture()
        actual,trace,conn = _intake(capture,changed_ids={40})
        expected,old_trace,old_conn = _intake(None,baseline=True,changed_ids={40})
        self.assertEqual((actual,trace,conn.trace),(expected,old_trace,old_conn.trace))
        self.assertEqual(actual['events_checked'],1)
        self.assertEqual(actual['events_unchanged_skipped'],2)
        self.assertEqual([payload for kind,payload in capture.records if kind=='sequence_call_input'],[])
        self.assertEqual([payload for kind,payload in capture.records if kind=='current_events'],[{'events':[]}])
        self.assertEqual([kind for kind,_ in capture.records],['current_events'])

    def test_existing_first_match_is_never_relabelled_as_current_insert(self):
        capture = RecordingCapture()
        _,_,conn = _intake(capture)
        self.assertEqual(conn.matches[('normal',30)],('OLD_FIRST_MATCH',))
        routing = [payload for kind,payload in capture.records if kind=='routing'][0]
        self.assertEqual(routing['insert_outcome'],'UNOBSERVED_INSERT_OR_EXISTING')
        self.assertEqual(len(routing['candidate_hits']),4)
        self.assertEqual(routing['rule'],'ON CONFLICT(candidate_key,event_id) DO NOTHING')

    def test_no_changed_events_produces_no_history_queries_or_sequence_calls(self):
        capture = RecordingCapture()
        actual,trace,conn = _intake(capture,changed_ids=set())
        expected,old_trace,old_conn = _intake(None,baseline=True,changed_ids=set())
        self.assertEqual((actual,trace,conn.trace),(expected,old_trace,old_conn.trace))
        self.assertFalse(any(item[0]=='source_execute' for item in conn.trace))


class TransactionIntegrationTests(unittest.TestCase):
    def test_submit_occurs_after_intake_commit_and_context_exit(self):
        trace = []
        capture = RecordingCapture(trace)
        worker,_,_ = _worker(capture,trace)
        result = worker._run_once(now=NOW)
        self.assertEqual(len(capture.submitted),1)
        self.assertEqual(capture.discarded,[])
        self.assertLess(trace.index('commit_2'),trace.index('capture_exit'))
        self.assertLess(trace.index('capture_exit'),trace.index('capture_submit'))
        self.assertLess(trace.index('capture_submit'),trace.index('queue'))
        self.assertNotIn('observation',result)
        self.assertNotIn('observation',worker.metrics)

    def test_ingest_inverse_and_commit_errors_keep_original_exception_and_discard(self):
        for stage in ('intake','inverse','commit'):
            with self.subTest(stage=stage):
                trace = []
                capture = RecordingCapture(trace)
                error = RuntimeError(f'original {stage}')
                worker,_,_ = _worker(capture,trace,failure=stage,error=error)
                with self.assertRaises(RuntimeError) as caught:
                    worker._run_once(now=NOW)
                self.assertIs(caught.exception,error)
                self.assertEqual(len(capture.discarded),1)
                self.assertEqual(capture.submitted,[])
                self.assertIn('rollback',trace)

    def test_cancelled_intake_discards_and_never_enqueues(self):
        trace = []
        capture = RecordingCapture(trace)
        error = asyncio.CancelledError('cancel intake')
        worker,_,_ = _worker(capture,trace,failure='intake',error=error)
        with self.assertRaises(asyncio.CancelledError) as caught:
            worker._run_once(now=NOW)
        self.assertIs(caught.exception,error)
        self.assertEqual(len(capture.discarded),1)
        self.assertEqual(capture.submitted,[])

    def test_later_scope_preparation_error_keeps_committed_observation(self):
        trace = []
        capture = RecordingCapture(trace)
        error = RuntimeError('later queue failure')
        worker,_,_ = _worker(capture,trace,failure='queue',error=error)
        with self.assertRaises(RuntimeError) as caught:
            worker._run_once(now=NOW)
        self.assertIs(caught.exception,error)
        self.assertEqual(len(capture.submitted),1)
        self.assertEqual(capture.discarded,[])

    def test_capture_enter_exit_and_submit_failures_do_not_fail_intake(self):
        for failure in ('fail_enter','fail_exit','fail_submit'):
            with self.subTest(failure=failure):
                trace = []
                capture = RecordingCapture(trace)
                setattr(capture,failure,True)
                worker,_,_ = _worker(capture,trace)
                result = worker._run_once(now=NOW)
                self.assertFalse(result['locked'])
                self.assertEqual(result['events_checked'],1)

    def test_capture_exit_failure_does_not_replace_original_worker_error(self):
        trace = []
        capture = RecordingCapture(trace)
        capture.fail_exit = True
        error = RuntimeError('original intake failure')
        worker,_,_ = _worker(capture,trace,failure='intake',error=error)
        with self.assertRaises(RuntimeError) as caught:
            worker._run_once(now=NOW)
        self.assertIs(caught.exception,error)
        self.assertEqual(capture.submitted,[])
        self.assertEqual(len(capture.discarded),1)

    def test_locked_worker_does_not_open_an_observation(self):
        trace = []
        capture = RecordingCapture(trace)
        worker,_,_ = _worker(capture,trace,locked=True)
        self.assertTrue(worker._run_once(now=NOW)['locked'])
        self.assertNotIn('capture_enter',trace)
        self.assertEqual(capture.submitted,[])

    def test_status_uses_scalar_capture_receipt_and_handles_failure(self):
        trace = []
        capture = RecordingCapture(trace)
        _,namespace,_ = _worker(capture,trace)
        self.assertEqual(namespace['_sequence_capture_status'](),capture.status())
        namespace['doge_sequence_capture'] = SimpleNamespace(status=lambda:1/0)
        self.assertEqual(namespace['_sequence_capture_status']()['state'],'UNAVAILABLE')


if __name__ == '__main__':
    unittest.main(verbosity=2)
