#!/usr/bin/env python3
"""Offline repeated ordered-worker memory replay, with synthetic IO only.

Example: python tools/ordered_memory_replay.py --rows 32 --scopes 3 --passes 2
  --output /absolute/new/output_directory
Use --trace for a SEPARATE fresh run; never compare traced CPU to plain CPU.
No database/provider calls, GC collection/disable, allocator trim, or runtime edits.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, redirect_stdout
from datetime import datetime, timedelta, timezone
import gc
import hashlib
import json
import os
from pathlib import Path
import resource
import socket
import subprocess
import sys
import threading
import time
from unittest.mock import patch

sys.dont_write_bytecode = True
REPO = Path(__file__).resolve().parents[1]
START = datetime(2026, 9, 8, tzinfo=timezone.utc)
FREEZE = START + timedelta(hours=1)
AS_OF = datetime(2026, 10, 1, tzinfo=timezone.utc)
VERSION = 'ordered-memory-replay-v2'
LIMITATIONS = [
    'Engineering row scales and synthetic outcomes are not measured production cardinalities or object equivalence.',
    'Single persistent executor thread, sequential passes; no concurrent production workers or Chromium.',
    'SQL is emulated: no PostgreSQL planner, server, JSONB/libpq buffers, network, ingestion, inverse reconciliation or delivery.',
    'IO helpers supply complete source/feature/membership coverage and a deterministic scope queue.',
    'Frozen registrations, representative metadata and latest scope-result JSON are held in bounded adapter stores; warm-up is reported.',
    'Actual common-window aggregation is used over synthetic READY records; candle-level common-window calculation is not replayed.',
    'One existing OPEN outcome resolves per pass; entry identities and relevant frozen predicate values remain unchanged.',
    'Phase samples include synchronous observer overhead; sampled RSS is not continuous peak RSS.',
    'Trace-only snapshots perturb allocation/RSS; snapshot capture and post-run attribution are not plain memory/CPU benchmarks.',
    'Native allocator counters are not exclusive RSS partitions or reclaimable-byte estimates.',
    'Local Python/libc may differ from deployed Python 3.11/glibc 2.36; no production attribution follows.',
]


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      default=str, allow_nan=False)


def file_sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def imports():
    sys.path.insert(0, str(REPO))
    import research_formula_ordered_worker as worker
    import runtime_allocation_diagnostics as allocation
    return worker, allocation


def prepare(args):
    worker, _ = imports()
    from research_formula_ordered_v7_selftest import row as make_row
    from research_ordered_validation_selftest import common
    import research_ordered_first_touch as ordered
    folder = Path(args.fixture)
    folder.mkdir(exist_ok=False)
    candidate = next(item for item in worker.evaluator.candidate_catalog(include_extended=True)
                     if item['formula_id'].endswith(':CORE_PRICE_OI_TOTAL_65')
                     and item.get('research_orientation') == 'NORMAL')
    scopes, files = [], {}
    pairs = [(h, b) for h in worker.evaluator.HORIZONS_MINUTES
             for b in worker.evaluator.THRESHOLDS_BPS][:args.scopes]
    for index, (horizon, threshold) in enumerate(pairs):
        scope = {'scope_key':f'synthetic-memory-{index:02d}', 'candidate_key':candidate['formula_id'],
                 'symbol':'ALL', 'direction':'LONG', 'window_minutes':horizon,
                 'threshold_bps':threshold, 'period_key':'SINCE_20260904',
                 'period_start_utc':worker.store.PERIODS['SINCE_20260904'].isoformat()}
        rows, windows, pending = [], {}, {}
        for ordinal in range(1, args.rows + 1):
            start = START + timedelta(minutes=10 * ordinal)
            row = make_row(ordinal, wave=f'synthetic-wave-{ordinal:05d}', start=start,
                           success=ordinal % 4 != 0, symbol='BTC', threshold=threshold, horizon=horizon)
            row.update(parent_start_time_utc=start-timedelta(minutes=1),
                       episode_policy_version=worker.store.PARENT_POLICY,
                       event_kind='ALERT', delivery_status='DELIVERED',
                       event_fingerprint=f'synthetic-event-{ordinal:05d}',
                       snapshot_id=f'synthetic-snapshot-{ordinal:05d}')
            row['decision_features'].update({'time.weekend':False,
                'historical.closed_1m.return_15m_pct':0.25, 'historical.closed_1m.return_60m_pct':0.5})
            record = common(row, mfe=3.0, mae=1.0)
            end = start + timedelta(minutes=horizon)
            record.update(window_minutes=horizon, window_end_utc=end, observed_at_utc=end,
                          path_samples=horizon, observed_through_utc=end-timedelta(milliseconds=1))
            windows[str(ordinal)] = record
            if ordinal <= args.passes:
                candle = {'open_time_utc':start,
                          'close_time_utc':start+timedelta(seconds=59,milliseconds=999),
                          'open':100.0,'high':100.01,'low':99.99,'close':100.0}
                pending[str(ordinal)] = ordered.calculate_ordered_first_touch_outcome(
                    reference_price=100, direction='LONG', event_time=start, candles=[candle],
                    threshold_pct=threshold/100, observation_closed=False, path_complete=True)
            rows.append(row)
        for suffix, payload in (('rows',{'rows':rows,'pending_labels':pending}), ('windows',windows)):
            path = folder / (scope['scope_key'] + '.' + suffix + '.json')
            with path.open('x', encoding='utf-8') as stream:
                stream.write(canonical(payload))
            files[path.name] = {'bytes':path.stat().st_size,'sha256':file_sha(path)}
        scopes.append(scope)
    manifest = {'version':VERSION,'rows':args.rows,'scopes':scopes,'passes':args.passes,
                'candidate':candidate,'files':files,'synthetic_only':True}
    with (folder/'manifest.json').open('x',encoding='utf-8') as stream:
        stream.write(canonical(manifest)+'\n')


class Cursor:
    def __init__(self, rows=()): self.rows = rows
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def fetchone(self):
        rows, self.rows = self.rows, ()
        return next(iter(rows), None)
    def fetchall(self):
        rows, self.rows = self.rows, ()
        return list(rows)
    rowcount = 1


class Adapter:
    """Bounded emulator. SQL parameters are hashed then discarded, never logged."""
    def __init__(self, fixture, manifest):
        self.fixture, self.manifest = fixture, manifest
        self.registrations, self.policies, self.waves = {}, {}, {}
        self.results, self.inputs, self.outbox = {}, {}, set()
        self.pass_index = 0
        self.scope_index = 0
        self.phase = None
        self.begin_pass(0)

    def begin_pass(self, index):
        self.pass_index = index
        self.scope_index = 0
        self.sql_hash = hashlib.sha256()
        self.counts = Counter()
        self.commits = self.rollbacks = 0
        self.validation_episodes = self.validation_evaluations = 0
        self.formula_episodes = self.question_runs = 0
        self.sql_observer_ns = 0

    def __enter__(self): return self
    def __exit__(self, *args): return False
    def cursor(self): return self
    def commit(self): self.commits += 1
    def rollback(self): self.rollbacks += 1
    def executemany(self, sql, records):
        for params in records: self.execute(sql, params)

    def _observe(self, sql, params):
        # Chunk strings so observing a large SQL payload does not create a
        # second complete UTF-8 buffer. The digest has fixed-size retained state.
        started = time.perf_counter_ns()
        self.sql_hash.update(sql.encode())
        for item in params:
            text = item if isinstance(item, str) else canonical(item)
            self.sql_hash.update((type(item).__name__+':'+str(len(text))+':').encode())
            for start in range(0, len(text), 65536):
                self.sql_hash.update(text[start:start+65536].encode())
        self.sql_observer_ns += time.perf_counter_ns()-started

    def execute(self, sql, params=()):
        self._observe(sql, params)
        key = None
        rows = ()
        if 'pg_try_advisory_lock' in sql:
            key, rows = 'lock', [{'acquired':True}]
        elif 'pg_advisory_unlock' in sql: key = 'unlock'
        elif 'COUNT(*) AS n FROM research_ordered_formula_scopes' in sql:
            key, rows = 'scope_count', [{'n':len(self.manifest['scopes'])}]
        elif sql.startswith('SELECT name,to_regclass'):
            key, rows = 'validation_schema', [{'name':name,'present':True} for name in params[0]]
        elif sql.startswith('SELECT registration'):
            key = 'freeze_read'
            if params[0] in self.registrations:
                rows = [{'registration':json.loads(self.registrations[params[0]])}]
        elif sql.startswith('SELECT policy'):
            key = 'policy_read'
            if params[0] in self.policies: rows = [{'policy':json.loads(self.policies[params[0]])}]
        elif 'SELECT clock_timestamp()' in sql:
            key, rows = 'freeze_clock', [{'now':FREEZE}]
        elif 'INSERT INTO research_ordered_validation_acceptance' in sql:
            key = 'policy_write'
            prior = self.policies.setdefault(params[1], params[2])
            if prior != params[2]: raise AssertionError('Acceptance mutated')
        elif 'INSERT INTO research_ordered_validation_freezes' in sql:
            key = 'freeze_write'
            prior = self.registrations.setdefault(params[1], params[4])
            if prior != params[4]: raise AssertionError('Registration mutated')
        elif 'SELECT e.event_id,e.event_kind,e.delivery_status' in sql:
            key = 'event_metadata'
            rows = [{'event_id':i,'event_kind':'ALERT','delivery_status':'DELIVERED',
                'parent_start_time_utc':(START+timedelta(minutes=10*i-1)).isoformat(),
                'btc_parent_movement_id':f'synthetic-wave-{i:05d}',
                'episode_policy_version':params[0],'membership_status':'LIVE',
                'parent_evidence_eligible':True} for i in params[1]]
        elif sql.startswith('SELECT btc_parent_movement_id,representative_event_ids'):
            key = 'ledger_read'
            rows = [json.loads(value) for value in self.waves.get(params[0], {}).values()]
        elif 'INSERT INTO research_ordered_validation_waves' in sql:
            key = 'ledger_insert'
            ledger = self.waves.setdefault(params[0], {})
            value = canonical({'btc_parent_movement_id':params[1],
                               'representative_event_ids':json.loads(params[4]),
                               'representative_sha256':params[5],'representative_conflict':False})
            if ledger.setdefault(params[1], value) != value: raise AssertionError('Ledger mutated')
        elif 'SET representative_conflict=TRUE' in sql:
            raise AssertionError('Synthetic representative identity changed')
        elif 'UPDATE research_ordered_validation_waves SET evidence=' in sql:
            key = 'validation_episode_write'
            self.validation_episodes += 1
        elif 'INSERT INTO research_ordered_validation_evaluations' in sql:
            key = 'validation_evaluation_write'
            self.validation_evaluations += 1
        elif sql.startswith('UPDATE research_ordered_formula_scopes SET result='):
            key = 'scope_result_write'
            self.results[params[2]] = params[0]
        elif sql.startswith('UPDATE research_ordered_formula_scopes SET evaluation_input_sha256='):
            key = 'input_hash_write'
            self.inputs[params[1]] = params[0]
        elif 'INSERT INTO research_ordered_formula_trials' in sql: key = 'formula_trial_write'
        elif 'INSERT INTO research_ordered_formula_episodes' in sql:
            key = 'formula_episode_write'
            self.formula_episodes += 1
        elif 'INSERT INTO research_ordered_question_runs' in sql:
            key = 'question_trial_write'
            self.question_runs += 1
        elif 'INSERT INTO research_sheet_upsert_outbox' in sql:
            key = 'outbox_write'
            self.outbox.add((params[0],params[1]))
        elif 'UPDATE research_sheet_upsert_outbox' in sql: key = 'outbox_time_write'
        elif 'SELECT 1 AS present FROM research_sheet_upsert_outbox' in sql:
            key = 'outbox_present'
            if tuple(params) in self.outbox: rows = [{'present':1}]
        else:
            raise AssertionError('Unmodeled SQL boundary: ' + ' '.join(sql.split())[:150])
        self.counts[key] += 1
        return Cursor(rows)

    def due_scopes(self, conn, limit, **kwargs):
        import research_formula_ordered_store as store
        if limit < len(self.manifest['scopes']): raise AssertionError('Scope budget smaller than fixture')
        return store.ScopeScheduleBatch([{**scope,
            'result':json.loads(self.results.get(scope['scope_key'],'{}')),
            'evaluation_input_sha256':self.inputs.get(scope['scope_key'])}
            for scope in self.manifest['scopes']], self.pass_index)

    def load_rows(self, conn, scope, *, row_limit, now):
        self.scope_index += 1
        payload = json.loads((self.fixture/(scope['scope_key']+'.rows.json')).read_text())
        rows = payload['rows']
        for row in rows:
            pending = payload['pending_labels'].get(str(row['event_id']))
            if pending is not None and row['event_id'] > self.pass_index:
                row.update(pending)
        if len(rows) > row_limit: raise AssertionError('Unexpected fixture truncation')
        return rows, False

    def common_rows(self, conn, scope, rows):
        # Independent decode models another DB result; no row graphs are cached.
        payload = json.loads((self.fixture/(scope['scope_key']+'.windows.json')).read_text())
        return {int(key):value for key,value in payload.items()}


class Observer:
    def __init__(self, output, allocation, trace):
        self.pending_path = output/'samples.inprogress.jsonl'
        self.stream = self.pending_path.open('x',encoding='utf-8')
        self.provider, self.glibc_version, self.native_reason = allocation._initialize_native()
        self.native_fields = allocation.NATIVE_FIELDS
        self.trace = trace
        self.total_ns = 0
        self.count = 0
        self.gc_threshold = gc.get_threshold()
        self.pid = os.getpid()
        self.thread_ids = set()
        self.snapshots = []
        self.output = output

    def snapshot(self, pass_index, label):
        import tracemalloc
        started = time.perf_counter_ns()
        path = self.output / f'snapshot_pass_{pass_index:03d}_{label}.trace'
        if path.exists(): raise AssertionError('Snapshot output already exists')
        snapshot = tracemalloc.take_snapshot()
        snapshot.dump(str(path))
        del snapshot
        metadata = {'pass_index':pass_index,'label':label,'file':path.name,
                    'duration_ns':time.perf_counter_ns()-started,'bytes':path.stat().st_size}
        self.snapshots.append(metadata)
        self.stream.write(canonical({'version':VERSION,'event':'trace_snapshot',**metadata})+'\n')
        self.stream.flush()

    def sample(self, adapter, phase, *, event='sample', extra=None):
        before = time.perf_counter_ns()
        if not gc.isenabled() or gc.get_threshold() != self.gc_threshold:
            raise AssertionError('Normal GC configuration changed')
        thread_id = threading.get_native_id()
        self.thread_ids.add(thread_id)
        rss = int(Path('/proc/self/statm').read_text().split()[1]) * os.sysconf('SC_PAGE_SIZE')
        native = {'status':self.native_reason or 'ok','glibc_version':self.glibc_version,
                  'stats':None,'duration_us':None}
        if self.provider is not None:
            tick = time.perf_counter_ns()
            result = self.provider()
            native['stats'] = {name:int(getattr(result,name)) for name in self.native_fields}
            native['duration_us'] = (time.perf_counter_ns()-tick)//1000
        trace = None
        if self.trace:
            import tracemalloc
            current, peak = tracemalloc.get_traced_memory()
            trace = {'current_bytes':current,'peak_bytes':peak}
        record = {'version':VERSION,'event':event,'pass_index':adapter.pass_index,
            'scope_index':adapter.scope_index,'phase':phase,'pid':self.pid,'thread_id':thread_id,
            'rss_bytes':rss,'ru_maxrss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
            'python_allocated_blocks':sys.getallocatedblocks(), 'gc_count':gc.get_count(),
            'gc_stats':gc.get_stats(),'gc_enabled':True,'native':native,'trace':trace}
        if extra: record.update(extra)
        self.stream.write(canonical(record)+'\n')
        self.stream.flush()
        self.count += 1
        self.total_ns += time.perf_counter_ns()-before
        if self.trace:
            import tracemalloc
            tracemalloc.reset_peak()


def child(args):
    # Limits apply to this local child only; never inherited by a service.
    resource.setrlimit(resource.RLIMIT_AS, (1536*1024*1024,1536*1024*1024))
    worker, allocation = imports()
    fixture, output = Path(args.fixture), Path(args.output)
    manifest = json.loads((fixture/'manifest.json').read_text())
    for name, metadata in manifest['files'].items():
        if file_sha(fixture/name) != metadata['sha256']: raise AssertionError('Fixture changed')
    adapter = Adapter(fixture, manifest)
    observer = Observer(output, allocation, args.trace)
    service = worker.ResearchFormulaOrderedWorker()
    catalog = [manifest['candidate']]
    original_phase = worker._timing_phase
    def phase(timing, name):
        if adapter.phase is not None:
            observer.sample(adapter, adapter.phase)
        original_phase(timing, name)
        adapter.phase = name
    def run_pass(index):
        adapter.begin_pass(index)
        adapter.phase = None
        if index == 1:
            if args.trace:
                import tracemalloc
                tracemalloc.start(8)
            observer.sample(adapter,'observer_baseline')
        observer_before = observer.total_ns
        started, cpu = time.perf_counter_ns(), time.process_time_ns()
        summary = service.run_once(now=AS_OF+timedelta(minutes=index))
        elapsed, cpu = time.perf_counter_ns()-started, time.process_time_ns()-cpu
        if summary['scopes_evaluated'] != args.scopes or summary['unchanged_scopes_skipped'] != 0:
            raise AssertionError('Changed-scope workload was not completed')
        if adapter.validation_evaluations != args.scopes or adapter.validation_episodes != args.scopes*args.rows:
            raise AssertionError('Real validation did not emit every synthetic episode')
        if len(adapter.registrations) != args.scopes or sum(map(len,adapter.waves.values())) != args.scopes*args.rows:
            raise AssertionError('Frozen registration/ledger cardinality changed')
        if adapter.question_runs <= 0: raise AssertionError('Real question trial path did not execute')
        digest_started = time.perf_counter_ns()
        result_hash = hashlib.sha256()
        for key, value in sorted(adapter.results.items()):
            result_hash.update(key.encode())
            for start in range(0,len(value),65536): result_hash.update(value[start:start+65536].encode())
        result_observer_ns = time.perf_counter_ns()-digest_started
        observer.sample(adapter,'pass_return',event='pass_end',extra={
            'summary':summary,'sql_sha256':adapter.sql_hash.hexdigest(),
            'result_sha256':result_hash.hexdigest(),'sql_counts':dict(adapter.counts),
            'validation_evaluations':adapter.validation_evaluations,'validation_episodes':adapter.validation_episodes,
            'formula_episodes':adapter.formula_episodes,'question_runs':adapter.question_runs,
            'frozen_registrations':len(adapter.registrations),'ledger_entries':sum(map(len,adapter.waves.values())),
            'retained_scope_results':len(adapter.results),'retained_scope_result_characters':sum(map(len,adapter.results.values())),
            'commits':adapter.commits,'rollbacks':adapter.rollbacks,
            'elapsed_ns':elapsed,'cpu_ns':cpu,
            'observer_ns':observer.total_ns-observer_before+adapter.sql_observer_ns,
            'probe_observer_ns':observer.total_ns-observer_before,
            'sql_observer_ns':adapter.sql_observer_ns,'postpass_digest_observer_ns':result_observer_ns})
        if args.trace and index in {min(2,args.passes),args.passes}:
            observer.snapshot(index,'pass_return')
        return adapter.sql_hash.hexdigest(), result_hash.hexdigest()
    def no_network(*args, **kwargs):
        raise AssertionError('Network/database access is forbidden in the local replay')
    overrides = [(socket.socket,'connect',no_network),(socket.socket,'connect_ex',no_network),
        (socket,'create_connection',no_network),
        (worker,'_database_url',lambda:'synthetic://no-network'),
        (worker,'psycopg',object()),(worker,'_connect',lambda url:adapter),
        (worker,'_SCOPE_LIMIT',args.scopes),(worker,'_ROW_LIMIT',max(100,args.rows)),
        (worker,'_PASS_SECONDS',50),(worker,'_timing_phase',phase),
        (worker.store,'register_catalog',lambda conn:catalog),
        (worker.store,'ingest_matches',lambda *a,**k:{'events_checked':0,'inverse_source_event_ids':[]}),
        (worker.inverse_store,'available',lambda conn:False),
        (worker.experimental_worker,'enabled',lambda:False),
        (worker.store,'source_population_complete',lambda *a,**k:True),
        (worker.store,'due_scopes',adapter.due_scopes),
        (worker.store,'candidate_feature_coverage_complete',lambda *a,**k:{'complete':True}),
        (worker.store,'load_scope_rows',adapter.load_rows),
        (worker.store,'membership_complete',lambda *a,**k:True),
        (worker.store,'common_window_rows',adapter.common_rows),
        (worker.store,'period_coverage',lambda *a,**k:{'matching_alert_count':args.rows})]
    if worker.psycopg is not None:
        overrides.insert(0,(worker.psycopg,'connect',no_network))
    combined = hashlib.sha256()
    with (output/'worker_timing.jsonl').open('x',encoding='utf-8') as timing_log, ExitStack() as stack:
        for module, name, replacement in overrides:
            stack.enter_context(patch.object(module,name,replacement))
        stack.enter_context(redirect_stdout(timing_log))
        with ThreadPoolExecutor(max_workers=1,thread_name_prefix='ordered-memory') as executor:
            for index in range(1,args.passes+1):
                pair = executor.submit(run_pass,index).result()
                combined.update(canonical(pair).encode())
                timing_log.flush()
    observer.stream.flush()
    os.fsync(observer.stream.fileno())
    observer.stream.close()
    # Publish a completed journal once. Consumers must never interpret a
    # growing journal as final evidence merely because a receipt exists.
    sample_path = output/'samples.jsonl'
    observer.pending_path.replace(sample_path)
    sample_bytes = sample_path.read_bytes()
    sample_records = [json.loads(line) for line in sample_bytes.splitlines()]
    if (sum(row['event'] in ('sample','pass_end') for row in sample_records) != observer.count
            or [row['pass_index'] for row in sample_records if row['event']=='pass_end'] != list(range(1,args.passes+1))):
        raise AssertionError('Completed evidence journal is inconsistent')
    samples_artifact = {'file':sample_path.name,'bytes':len(sample_bytes),
        'sha256':hashlib.sha256(sample_bytes).hexdigest(),'record_count':len(sample_records)}
    del sample_bytes, sample_records
    if len(observer.thread_ids) != 1: raise AssertionError('Executor thread changed')
    if args.trace:
        import tracemalloc
        tracemalloc.stop()
        # Attribution runs only after all measured passes and after tracing has
        # stopped. Never retain Snapshot objects during the measured workload.
        first_path = output/observer.snapshots[0]['file']
        last_path = output/observer.snapshots[-1]['file']
        first, last = tracemalloc.Snapshot.load(str(first_path)), tracemalloc.Snapshot.load(str(last_path))
        differences = last.compare_to(first,'traceback')
        top = [{'size_diff':item.size_diff,'count_diff':item.count_diff,
                'size':item.size,'count':item.count,
                'traceback':[{'file':frame.filename,'line':frame.lineno} for frame in item.traceback]}
               for item in differences[:40]]
        attribution = {'version':VERSION,'trace_only':True,'snapshots':observer.snapshots,
                       'compared_passes':[observer.snapshots[0]['pass_index'],observer.snapshots[-1]['pass_index']],
                       'total_size_diff':sum(item.size_diff for item in differences),
                       'total_count_diff':sum(item.count_diff for item in differences),
                       'top_40_absolute_differences':top,
                       'limitations':'Python traced allocation sites, not native allocation ownership; trace RSS is perturbed.'}
        with (output/'trace_comparison.json').open('x') as stream:
            stream.write(canonical(attribution)+'\n')
        del first, last, differences, attribution, top
    # Acceptance decoding occurs only after the final measured endpoint. It
    # must not create a second retained result graph during the experiment.
    audited = []
    for scope_key, encoded in sorted(adapter.results.items()):
        result = json.loads(encoded)
        validated = result['prospective_validation']
        metrics = validated['all_period_metrics']
        if validated['excluded_rows'] or validated['representative_conflicts']:
            raise AssertionError('Synthetic validation contained rejected/conflicting evidence')
        expected_successes, expected_failures = args.rows-args.rows//4, args.rows//4
        if (metrics['successes'],metrics['failures'],metrics['status_counts']['OPEN']) != (expected_successes,expected_failures,0):
            raise AssertionError('Synthetic outcomes were not usable resolved evidence')
        if metrics['common_window_complete'] is not True or metrics['common_window_waves'] != args.rows:
            raise AssertionError('Synthetic common-window evidence was not complete')
        if not validated['discovery']['selected_waves'] or not validated['prospective']['selected_waves']:
            raise AssertionError('Both sides of the frozen partition must execute')
        audited.append({'scope_key':scope_key,'successes':metrics['successes'],
            'failures':metrics['failures'],'open':0,'common_window_waves':metrics['common_window_waves'],
            'common_window_complete':True,'excluded_rows':{},'representative_conflicts':[],
            'discovery_waves':validated['discovery']['selected_waves'],
            'prospective_waves':validated['prospective']['selected_waves']})
        del result, validated, metrics
    source_names = ['research_formula_ordered_worker.py','research_formula_ordered_store.py',
                    'research_ordered_question_store.py','research_ordered_validation_store.py',
                    'research_ordered_validation.py','research_formula_ordered_v7.py']
    head = subprocess.run(['git','rev-parse','HEAD'],cwd=REPO,check=True,capture_output=True,text=True).stdout.strip()
    receipt = {'version':VERSION,'status':'PASS','python':sys.version,'glibc':observer.glibc_version,'source_head':head,
        'rows':args.rows,'scopes':args.scopes,'passes':args.passes,'trace':args.trace,
        'pid':os.getpid(),'executor_thread_ids':sorted(observer.thread_ids),
        'fixture_manifest_sha256':file_sha(fixture/'manifest.json'),
        'combined_sql_result_sha256':combined.hexdigest(),'source_sha256':{n:file_sha(REPO/n) for n in source_names},
        'harness_sha256':file_sha(__file__),'gc_enabled':gc.isenabled(),'gc_threshold':gc.get_threshold(),
        'sample_count':observer.count,'observer_total_ns':observer.total_ns,
        'observer_total_ns_scope':'RSS/native/GC/JSONL probes only; per-pass sql_observer_ns is recorded separately',
        'forced_gc_calls':0,'allocator_trim_calls':0,'database_connections':0,'provider_requests':0,
        'network_guard':True,'real_calculation_functions':['worker.run_once','question_store.evaluation_input',
            'evaluator.summarize_scope','validation_store.register_supported_acceptance',
            'validation_store.evaluate_scope','store.persist_scope','question_store.record_scope_trial'],
        'synthetic_io_seams':['worker._connect','store.register_catalog','store.ingest_matches',
            'inverse_store.available','experimental_worker.enabled','store.source_population_complete',
            'store.due_scopes','store.candidate_feature_coverage_complete','store.load_scope_rows',
            'store.membership_complete','store.common_window_rows','store.period_coverage',
            'SQL adapter for validation registrations/metadata/ledger and persistence writes'],
        'full_sql_or_result_history_retained':False,'limitations':LIMITATIONS}
    receipt['final_postmeasurement_fixture_audit'] = audited
    receipt['pass_observer_scope'] = ('observer_ns includes phase probes plus SQL digest, both inside elapsed_ns; '
        'pass_end sample and postpass_digest_observer_ns are outside elapsed_ns; snapshots are separate trace-only events.')
    receipt['samples_artifact'] = samples_artifact
    receipt_pending = output/'receipt.pending.json'
    with receipt_pending.open('x') as stream:
        stream.write(canonical(receipt)+'\n')
        stream.flush()
        os.fsync(stream.fileno())
    receipt_pending.replace(output/'receipt.json')
    print(canonical(receipt))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows',type=int,choices=(32,256,2000),default=32)
    parser.add_argument('--scopes',type=int,default=29)
    parser.add_argument('--passes',type=int,default=10)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--trace',action='store_true')
    parser.add_argument('--prepare',action='store_true',help=argparse.SUPPRESS)
    parser.add_argument('--child',action='store_true',help=argparse.SUPPRESS)
    parser.add_argument('--fixture',type=Path,help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not 1 <= args.scopes <= 32 or not 1 <= args.passes <= min(32,args.rows):
        parser.error('scopes must be1..32; passes1..min(32,rows)')
    if args.prepare: prepare(args); return
    if args.child: child(args); return
    args.output.mkdir(parents=True,exist_ok=False)
    fixture = args.output/'fixtures'
    command = [sys.executable,str(Path(__file__).resolve()),'--rows',str(args.rows),
               '--scopes',str(args.scopes),'--passes',str(args.passes),'--output',str(args.output),
               '--fixture',str(fixture)]
    # The launcher imports no application or fixture builders. Heavy fixture
    # preparation exits before the fresh measured process is started.
    subprocess.run([*command,'--prepare'],check=True,timeout=120,cwd=REPO)
    subprocess.run([*command,'--child',*(['--trace'] if args.trace else [])],
                   check=True,timeout=1200,cwd=REPO)


if __name__ == '__main__': main()
