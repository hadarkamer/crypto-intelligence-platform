#!/usr/bin/env python3
"""Bounded disposable PostgreSQL client replay; no service/runtime modifications.

Only a parent-created loopback connection receipt and matching database marker
are accepted. Preparation runs in a separate process. Measured passes execute
sequentially on one persistent thread and roll back to a fixed checkpoint.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
import gc
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import resource
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4
import zipfile

sys.dont_write_bytecode = True
REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
VERSION = 'ordered-postgres-memory-replay-v1'
SCOPE_DIAGNOSTIC_HASHES = frozenset({'decoded_rows_sha256', 'decoded_common_windows_sha256',
                                   'summary_sha256', 'common_metrics_sha256'})
NOW = datetime(2026, 10, 1, 12, tzinfo=timezone.utc)
MIGRATIONS = ['001_research_archive_v1.sql', '022_ordered_formula_research.sql',
              '027_ordered_formula_research_periods.sql', '030_ordered_question_search.sql',
              '038_runtime_event_scan_bounds.sql']
TABLES = ['research_events', 'research_ordered_formula_candidates',
          'research_ordered_feature_screens', 'research_ordered_formula_matches',
          'research_ordered_formula_scopes', 'research_ordered_formula_worker_state',
          'research_ordered_formula_event_checks', 'research_ordered_question_runs',
          'research_event_scan_cursors']
LIMITATIONS = [
    'Synthetic engineering fixtures; no production data or cardinality equivalence.',
    'Real psycopg/libpq SQL and JSONB decoding; one persistent executor thread, new connection per pass.',
    'Rollback replays a fixed checkpoint; server caches/MVCC and operating-system caches may warm.',
    'No concurrent services, collector, Chromium, inverse provider or publication.',
    'Past-feature migration 034 is intentionally absent in history/ingest fixtures.',
    'Normal GC only; no forced collection, allocator trimming or production instrumentation.',
    'Phase samples and output fingerprints add observer work; RSS samples are not continuous peaks.',
    'Allocator counters are neither exclusive RSS partitions nor reclaimable-byte estimates.',
    'Local Python/glibc may differ from deployed Python 3.11/glibc 2.36.',
]


def engine_limitations(receipt):
    if receipt.get('engine') == 'native PostgreSQL':
        return ['Owned disposable native PostgreSQL; declared runtime contract only, not a production workload replica.']
    return ['PostgreSQL is supplied by a disposable PGlite WASM socket, not a native production server.']


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      default=str, allow_nan=False)


def sha_file(path):
    result = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def write_json(path, value):
    with Path(path).open('x', encoding='utf-8') as stream:
        stream.write(canonical(value) + '\n')
        stream.flush()
        os.fsync(stream.fileno())


def digest_rows(rows):
    result = hashlib.sha256()
    for row in rows:
        result.update(canonical(row).encode('utf-8'))
        result.update(b'\n')
    return result.hexdigest()


def connection_receipt(path):
    # libpq consults PG* variables even with an explicit host. Reject all routing,
    # service/options/auth defaults rather than silently inheriting environment.
    if any(key.startswith('PG') for key in os.environ):
        raise ValueError('PG environment variables are forbidden for this replay')
    receipt = json.loads(Path(path).read_text())
    if (receipt.get('version') != 'isolated-ordered-pg-host-v1'
            or receipt.get('host') != '127.0.0.1'
            or type(receipt.get('port')) is not int
            or not 1024 <= receipt['port'] <= 65535
            or not re.fullmatch(r'[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}', receipt.get('run_token', ''))
            or receipt.get('dbname') != 'postgres'
            or receipt.get('user') != 'postgres'
            or receipt.get('marker_table') != 'public.local_memory_replay_identity'
            or receipt.get('purpose') != 'isolated-ordered-memory-replay'):
        raise ValueError('Invalid disposable loopback connection receipt')
    return receipt


def connect(receipt, schema=None):
    import psycopg
    from psycopg import sql
    from psycopg.rows import dict_row
    conn = psycopg.connect(host='127.0.0.1', hostaddr='127.0.0.1', port=receipt['port'],
        dbname='postgres', user='postgres', password='', sslmode='disable',
        connect_timeout=5, row_factory=dict_row, application_name=VERSION,
        options='-c statement_timeout=20000 -c lock_timeout=1000 -c timezone=UTC')
    try:
        # Fixed SQL and qualified marker name; no receipt-provided SQL or DSN.
        row = conn.execute('SELECT run_token,purpose FROM public.local_memory_replay_identity').fetchall()
        if row != [{'run_token':receipt['run_token'],'purpose':receipt['purpose']}]:
            raise ValueError('Disposable database marker mismatch')
        conn.rollback()
        if schema:
            if not re.fullmatch(r'ordered_memory_replay_[0-9a-f]{24}', schema):
                raise ValueError('Invalid disposable schema')
            conn.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
            conn.commit()
        return conn
    except BaseException:
        conn.close()
        raise


def project(conn, ids):
    import research_formula_ordered_store as store
    return conn.execute('WITH picked AS MATERIALIZED (SELECT unnest(%s::bigint[]) AS event_id) '
                        + store._EVENT_PROJECT, (ids,)).fetchall()


def checkpoint(conn, *, semantic=False):
    """Full row fingerprints only before/after measurement, fetched in small pages."""
    from psycopg import sql
    result = {}
    for table in TABLES:
        digest, count = hashlib.sha256(), 0
        # Server canonical text permits exact stable-state comparison, including
        # timestamps. Per-row hashes bound client materialization outside passes.
        with conn.cursor(name='verify_' + uuid4().hex) as cursor:
            expression = ("(to_jsonb(t)-'created_at'-'created_at_utc'-'updated_at_utc'-'checked_at_utc')::text"
                          if semantic else 'row_to_json(t)::text')
            cursor.execute(sql.SQL('SELECT md5('+expression+') AS h FROM {} t ORDER BY 1')
                           .format(sql.Identifier(table)))
            while rows := cursor.fetchmany(256):
                for row in rows:
                    digest.update(row['h'].encode('ascii'))
                    count += 1
        result[table] = {'rows':count, 'sha256':digest.hexdigest()}
    absent = conn.execute("SELECT to_regclass('research_past_price_features') IS NULL AS absent").fetchone()['absent']
    if not absent:
        raise AssertionError('Unexpected past-feature schema in ingestion fixture')
    return result


def add_events(conn, first, last, when):
    conn.execute('''INSERT INTO research_events(event_id,schema_version,event_kind,event_type,
        alert_time_utc,symbol,direction,setup_key,event_fingerprint,strategy_version,code_version,
        runtime_session_id,delivery_status,current_price,target_price,score,source_side,engine_snapshot)
        SELECT id,'test','ALERT','MAGNET_ALERT',%s,'BTC','LONG',repeat('s',64),
          lpad(id::text,64,'0'),'test','test','test','DELIVERED',100,101,70,'LONG',
          jsonb_build_object('watch_scan_id','scan-'||id,'magnet',jsonb_build_object('side','UPPER'),
            'market_evidence',jsonb_build_object('modules',jsonb_build_object(
              'futures_flow',jsonb_build_object('score',65,'direction','LONG'),
              'spot_flow',jsonb_build_object('score',66,'direction','LONG'))),
            'unused_large_payload',repeat('not-projected',100))
        FROM generate_series(%s::bigint,%s::bigint) id''', (when, first, last))


def audit_current_features(conn, fixture):
    """Independent fixed-value oracle, outside every measured interval."""
    import research_formula_ordered_store as store
    rows = conn.execute('''SELECT event_id,features FROM research_ordered_feature_screens
        WHERE event_id=ANY(%s::bigint[]) AND feature_version=%s ORDER BY event_id''',
        (fixture['current_ids'], store.questions.VERSION)).fetchall()
    if [row['event_id'] for row in rows] != fixture['current_ids']:
        raise AssertionError('Current feature screen identities differ from fixture')
    expected = {'sequence.240m.prior_distinct_scans':fixture['historical_rows'],
                'sequence.240m.previous_primary_family':'SPOT_CVD',
                'sequence.240m.futures_cvd.score_change':-13}
    for row in rows:
        if any(row['features'].get(key) != value for key, value in expected.items()):
            raise AssertionError('Real sequence features differ from independent fixed-value oracle')
    matches = conn.execute('''SELECT count(*) AS count,count(DISTINCT event_id) AS events
        FROM research_ordered_formula_matches WHERE event_id=ANY(%s::bigint[])''',
        (fixture['current_ids'],)).fetchone()
    if matches['count'] <= 0 or matches['events'] != 8:
        raise AssertionError('Real matching did not persist all eight current events')
    return {'screen_event_ids':fixture['current_ids'], 'expected_features':expected,
            'matched_events':matches['events'],'matches':matches['count'],
            'screen_features_sha256':digest_rows(rows)}


def prepare(args):
    from psycopg import sql
    import research_formula_ordered_store as store
    receipt = connection_receipt(args.connection_receipt)
    schema = 'ordered_memory_replay_' + uuid4().hex[:24]
    with connect(receipt) as conn:
        conn.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
        conn.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
        if args.mode == 'scope':
            import ordered_postgres_scope_fixture as scope_fixture
            fixture = scope_fixture.prepare(conn, args)
            conn.commit()
            expected = run_operation(conn, args.mode, fixture, [], cursor_policy=args.cursor_policy)
            conn.rollback()
            baseline = scope_fixture.verify(conn, fixture)
            expected_written = None
        else:
            for name in MIGRATIONS:
                conn.execute((REPO/'migrations'/name).read_text())
            catalog = store.register_catalog(conn)
            add_events(conn, 1, args.rows, NOW-timedelta(hours=2))
            conn.execute("UPDATE research_events SET alert_time_utc=%s,event_type='SPOT_CVD_HIGH',"
                         "engine_snapshot=jsonb_set(engine_snapshot,'{market_evidence,modules,futures_flow,score}','88') WHERE event_id=%s",
                         (NOW-timedelta(minutes=1), args.rows))
            ids = list(range(args.rows+1, args.rows+9))
            add_events(conn, ids[0], ids[-1], NOW)
            conn.execute("UPDATE research_events SET engine_snapshot=jsonb_set(engine_snapshot,"
                         "'{market_evidence,modules,futures_flow,score}','75') WHERE event_id>%s", (args.rows,))
            # Historical screens are immutable input fingerprints, not synthetic
            # feature-computation results. Sequence extraction still reads source.
            for first in range(1, args.rows+1, 64):
                projected = project(conn, list(range(first, min(first+64, args.rows+1))))
                with conn.cursor() as cursor:
                    cursor.executemany('''INSERT INTO research_ordered_feature_screens
                        (event_id,feature_version,input_sha256,features,checked_at_utc)
                        VALUES(%s,%s,%s,'{}'::jsonb,%s)''',
                        [(row['event_id'], store.questions.VERSION, store.question_store.digest(row), NOW)
                         for row in projected])
            conn.execute('''INSERT INTO research_ordered_formula_worker_state(worker_key,last_event_id,
                initial_scan_complete) VALUES(%s,%s,FALSE)''', (store.WORKER_KEY, args.rows))
            conn.commit()
            if args.mode == 'ingest_unchanged':
                first_result = store.ingest_matches(conn, catalog, now=NOW, event_limit=128)
                if first_result['events_checked'] != 8:
                    raise AssertionError('Preparation failed to screen eight current events')
                conn.commit()
                # Revisit eight known inputs rather than measuring an empty idle
                # pass. Rollback keeps this ascending-cursor checkpoint fixed.
                conn.execute('UPDATE research_ordered_formula_worker_state SET last_event_id=%s WHERE worker_key=%s',
                             (args.rows, store.WORKER_KEY))
                conn.commit()
            fixture = {'current_ids':ids, 'catalog_sha256':store.digest(catalog),
                       'catalog_count':len(catalog), 'historical_rows':args.rows}
            expected = run_operation(conn, args.mode, fixture, catalog)
            expected_written = checkpoint(conn, semantic=True)
            conn.rollback()
            baseline = checkpoint(conn)
        conn.rollback()
        server = conn.execute('SELECT version() AS version').fetchone()['version']
    from ordered_cursor_lifetime_probe import driver_binding, driver_cycle_probe
    driver = driver_binding()
    cycle_probe = (driver_cycle_probe(lambda:connect(receipt, schema))
                   if args.mode == 'scope' else None)
    expected_measured = expected
    if args.mode == 'scope' and args.scope_parity == 'postonly':
        # Bind the observed subset explicitly; omitted hashes are never copied
        # into a measured result. The production input hash remains required.
        expected_measured = {key:value for key,value in expected.items()
                             if key not in SCOPE_DIAGNOSTIC_HASHES}
        expected_measured['full_parity_verified'] = False
    manifest = {'version':VERSION, 'schema':schema, 'mode':args.mode, 'rows':args.rows,
                'passes':args.passes, 'scope_parity':args.scope_parity,
                'cursor_policy':args.cursor_policy, 'driver_binding':driver,
                'driver_cycle_probe':cycle_probe,
                'fixture':fixture, 'expected':expected, 'expected_measured':expected_measured,
                'baseline':baseline, 'expected_written':expected_written, 'server':server,
                'connection_receipt_sha256':sha_file(args.connection_receipt),
                'sources':source_hashes(), 'preparation_pid':os.getpid(),
                'engine':receipt.get('engine', 'PGlite WASM socket')}
    if args.mode == 'scope':
        import research_common_window_metrics_store as common_source
        actual_path = Path(common_source.__file__).resolve()
        if actual_path != REPO/'research_common_window_metrics_store.py':
            raise AssertionError('Application loader imported outside the selected source root')
        manifest['loaded_metrics_module'] = {'path':str(actual_path), 'sha256':sha_file(actual_path)}
    write_json(Path(args.output)/'fixture_manifest.json', manifest)


def run_operation(conn, mode, fixture, catalog, phase=None, *, full_parity=True,
                  cursor_policy='baseline', cursor_contract=None, cursor_counts=None):
    import research_formula_ordered_store as store
    if mode == 'scope':
        import ordered_postgres_scope_fixture as scope_fixture
        from ordered_cursor_lifetime_probe import CursorPolicyConnection, query_contract
        guarded = CursorPolicyConnection(conn, cursor_policy, cursor_contract or query_contract(),
                                         validate_binding=full_parity)
        result = scope_fixture.run_pass(guarded, fixture, phase=phase, full_parity=full_parity)
        counts = guarded.finish()
        if cursor_counts is not None:
            cursor_counts.update(counts)
        return result
    if mode == 'history':
        if phase: phase('current_projection')
        current = {row['event_id']:row for row in project(conn, fixture['current_ids'])}
        if phase: phase('load_sequence_history')
        history, stats = store.load_sequence_history(conn, current)
        if phase: phase('history_materialized')
        # Streaming per-row digest has bounded temporary encoding. Explicitly
        # separated from the loader phase and accounted as observer fingerprinting.
        if phase: phase('fingerprint')
        result = {**stats, 'history_sha256':digest_rows(history),
                  'current_sha256':digest_rows(current.values())}
        if stats['sequence_projected_rows'] != fixture['historical_rows']:
            raise AssertionError('Historical population mismatch')
        if any(row['event_id'] != i for i, row in enumerate(history, 1)):
            raise AssertionError('Historical identities are incomplete or out of order')
        return result
    if phase: phase('ingest_matches')
    result = store.ingest_matches(conn, catalog, now=NOW, event_limit=128)
    if result['events_checked'] != (8 if mode == 'ingest_changed' else 0):
        raise AssertionError('Changed-event workload mismatch')
    if mode == 'ingest_changed' and result['sequence_projected_rows'] != fixture['historical_rows']:
        raise AssertionError('Ingestion did not load complete history')
    if mode == 'ingest_unchanged' and result['events_unchanged_skipped'] != 8:
        raise AssertionError('Unchanged mode did not revisit eight known inputs')
    if mode == 'ingest_changed' and (result['question_screens'] <= 0 or result['matches_observed'] <= 0):
        raise AssertionError('Changed mode did not exercise real screening and matching')
    return result


def source_hashes():
    names = ['tools/ordered_postgres_memory_replay.py', 'tools/ordered_memory_replay.py',
        'tools/ordered_cursor_lifetime_probe.py',
        'runtime_allocation_diagnostics.py', 'research_formula_ordered_store.py',
        'research_formula_ordered_v7.py', 'research_ordered_question_catalog.py',
        'research_ordered_question_store.py', 'research_event_scan.py',
        'research_past_price_features_store.py', 'research_past_price_features.py',
        'research_ordered_first_touch.py', 'research_common_window_metrics.py',
        'research_common_window_metrics_store.py', 'research_formula_sequence_history_selftest.py',
        'docs/ordered_research_question_map.json',
        'migrations/020_ordered_first_touch_v7.sql', 'migrations/021_btc_parent_movements_v1.sql',
        'migrations/029_common_window_metrics.sql'] + ['migrations/'+name for name in MIGRATIONS]
    optional = REPO/'tools/ordered_postgres_scope_fixture.py'
    if optional.exists(): names.append('tools/ordered_postgres_scope_fixture.py')
    return {name:sha_file(REPO/name) for name in names}


def measured(args):
    import psycopg
    import research_formula_ordered_store as store
    import runtime_allocation_diagnostics as allocation
    from ordered_memory_replay import Observer
    output = Path(args.output)
    manifest = json.loads((output/'fixture_manifest.json').read_text())
    if manifest['sources'] != source_hashes():
        raise AssertionError('Source changed between preparation and measurement')
    if args.mode == 'scope':
        import research_common_window_metrics_store as common_source
        actual_path = Path(common_source.__file__).resolve()
        if (actual_path != REPO/'research_common_window_metrics_store.py'
                or manifest['loaded_metrics_module'] != {'path':str(actual_path), 'sha256':sha_file(actual_path)}):
            raise AssertionError('Measured application loader differs from preparation')
    if manifest['connection_receipt_sha256'] != sha_file(args.connection_receipt):
        raise AssertionError('Connection receipt changed')
    if manifest['scope_parity'] != args.scope_parity:
        raise AssertionError('Requested parity mode differs from preparation')
    if manifest['cursor_policy'] != args.cursor_policy:
        raise AssertionError('Requested cursor policy differs from preparation')
    from ordered_cursor_lifetime_probe import driver_binding, query_contract
    if driver_binding() != manifest['driver_binding']:
        raise AssertionError('Installed driver changed since preparation')
    cursor_contract = query_contract() if args.mode == 'scope' else None
    receipt = connection_receipt(args.connection_receipt)
    catalog = store.evaluator.candidate_catalog(include_extended=True)
    if args.mode != 'scope' and store.digest(catalog) != manifest['fixture']['catalog_sha256']:
        raise AssertionError('Full catalog changed')
    observer = Observer(output, allocation, False)
    expected_measured = manifest['expected_measured']
    state = SimpleNamespace(pass_index=0, scope_index=0)
    active = None
    def phase(name):
        nonlocal active
        if active is not None: observer.sample(state, active)
        active = name
    # Wrappers keep production calls/arguments/results intact and retain no rows.
    original_history = store.load_sequence_history
    def history(*a, **kw):
        if args.mode.startswith('ingest'): phase('load_sequence_history')
        result = original_history(*a, **kw)
        if args.mode.startswith('ingest'): phase('extract_sequence_match_and_persist')
        return result
    results = []
    def one_pass(index):
        nonlocal active
        state.pass_index, active = index, None
        if index == 1: observer.sample(state, 'observer_baseline')
        before = observer.total_ns
        started, cpu = time.perf_counter_ns(), time.process_time_ns()
        phase('connect_and_marker')
        cursor_counts = {}
        with connect(receipt, manifest['schema']) as conn:
            result = run_operation(conn, args.mode, manifest['fixture'], catalog, phase,
                full_parity=args.scope_parity == 'full', cursor_policy=args.cursor_policy,
                cursor_contract=cursor_contract, cursor_counts=cursor_counts)
            phase('rollback')
            conn.rollback()
            phase('connection_close')
        phase(None)
        elapsed, cpu = time.perf_counter_ns()-started, time.process_time_ns()-cpu
        if result != expected_measured:
            raise AssertionError('Repeated operation changed its result')
        result_sha = hashlib.sha256(canonical(result).encode()).hexdigest()
        observer.sample(state, 'pass_return', event='pass_end', extra={
            'mode':args.mode, 'scope_parity':args.scope_parity,
            'cursor_policy':args.cursor_policy, 'cursor_counts':cursor_counts,
            'elapsed_ns':elapsed, 'cpu_ns':cpu,
            'observer_ns':observer.total_ns-before, 'result':result, 'result_sha256':result_sha,
            'commits_in_workload':0, 'rollbacks':1})
        return {'pass_index':index,'result_sha256':result_sha,
                'thread_id':threading.get_native_id()}
    with patch.object(store, 'load_sequence_history', history):
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix='postgres-memory') as executor:
            for index in range(1, args.passes+1):
                results.append(executor.submit(one_pass, index).result())
    observer.stream.flush()
    os.fsync(observer.stream.fileno())
    observer.stream.close()
    if len(observer.thread_ids) != 1:
        raise AssertionError('Persistent executor thread changed')
    # Verification hydration is deliberately after the last measured endpoint.
    with connect(receipt, manifest['schema']) as conn:
        if args.mode == 'scope':
            import ordered_postgres_scope_fixture as scope_fixture
            after = scope_fixture.verify(conn, manifest['fixture'])
        else:
            after = checkpoint(conn)
        conn.rollback()
    if after != manifest['baseline']:
        raise AssertionError('Repeated rollback failed to preserve checkpoint')
    scope_audit = None
    source_audit = None
    if args.mode != 'scope':
        # Separate postmeasurement replay verifies actual stored features,
        # matches, question results and scope identities, not only row counts.
        # Server-generated wall-clock metadata is excluded from this parity
        # check; the exact baseline check above includes it.
        with connect(receipt, manifest['schema']) as conn:
            verified_result = run_operation(conn, args.mode, manifest['fixture'], catalog)
            written = checkpoint(conn, semantic=True)
            feature_audit = (audit_current_features(conn, manifest['fixture'])
                             if args.mode.startswith('ingest') else None)
            conn.rollback()
        if verified_result != manifest['expected'] or written != manifest['expected_written']:
            raise AssertionError('Postmeasurement write-content parity failed')
    else:
        feature_audit = None
        # Full independent-reference parity is always checked after measurement,
        # including the postonly case. No audit allocation enters pass endpoints.
        with connect(receipt, manifest['schema']) as conn:
            scope_audit = run_operation(conn, args.mode, manifest['fixture'], catalog,
                full_parity=True, cursor_policy=args.cursor_policy, cursor_contract=cursor_contract)
            conn.rollback()
        if args.cursor_policy in ('source', 'observed'):
            from ordered_cursor_lifetime_probe import source_semantics_audit
            source_audit = source_semantics_audit(lambda:connect(receipt, manifest['schema']),
                                                manifest['fixture']['scope']['window_minutes'])
        if scope_audit != manifest['expected']:
            raise AssertionError('Postmeasurement full scope parity failed')
    sample_path = output/'samples.jsonl'
    if sample_path.exists(): raise FileExistsError(sample_path)
    os.link(observer.pending_path, sample_path)
    observer.pending_path.unlink()
    count, pass_ids = 0, []
    with sample_path.open() as stream:
        for line in stream:
            record = json.loads(line)
            count += 1
            if record['event'] == 'pass_end': pass_ids.append(record['pass_index'])
    if count != observer.count or pass_ids != list(range(1,args.passes+1)):
        raise AssertionError('Final journal count mismatch')
    completed = {'version':VERSION,'result':'PASS','mode':args.mode,'rows':args.rows,
        'passes':args.passes,'scope_parity':args.scope_parity,
        'cursor_policy':args.cursor_policy, 'driver_binding':manifest['driver_binding'],
        'driver_cycle_probe':manifest['driver_cycle_probe'],
        'source_hashes':manifest['sources'],
        'fixture_manifest_sha256':sha_file(output/'fixture_manifest.json'),
        'samples':{'file':sample_path.name,'bytes':sample_path.stat().st_size,
                   'sha256':sha_file(sample_path),'records':count,'pass_ids':pass_ids},
        'results':results,'checkpoint_unchanged':True,'postmeasurement_content_parity':True,
        'postmeasurement_feature_audit':feature_audit,
        'postmeasurement_full_scope_audit':scope_audit,
        'postmeasurement_source_semantics_audit':source_audit,
        'preparation_pid':manifest['preparation_pid'], 'measurement_pid':os.getpid(),
        'loaded_metrics_module':manifest.get('loaded_metrics_module'),
        'engine':receipt.get('engine', 'PGlite WASM socket'),
        'server':manifest['server'],
        'python':sys.version,'libc':platform.libc_ver(), 'psycopg':psycopg.__version__,
        'libpq':psycopg.pq.version(),'gc_thresholds':gc.get_threshold(), 'gc_enabled':gc.isenabled(),
        'forced_gc_calls':0, 'allocator_trim_calls':0,
        'probe_observer_ns':observer.total_ns,'observer_scope':
          'Synchronous samples only; fingerprint phase and post-pass scalar digest add separate work.',
        'limitations':LIMITATIONS+engine_limitations(receipt)+([
            'All scope arms use the same four-query cursor proxy; policy '+args.cursor_policy
            +' changes only explicit cursor close or the bound common-window ID-array representation; '
            'source policy validates the application binding without conversion or replacement. '
            'observed policy records binding type only during measured passes; full representation checks run in prep/post audits. '
            'Preparation-only weakref probe is separate from measured passes; it does not assign native bytes to a driver object.',
            'Scope parity mode '+args.scope_parity+': full diagnostic row/window/summary hashes '
            +('run during every measured pass.' if args.scope_parity == 'full' else
              'are omitted from measured passes; full reference parity is checked in preparation and after measurement. ')
            +' Production evaluation_input hashing remains enabled.'
        ] if args.mode == 'scope' else [])}
    write_json(output/'receipt.json', completed)
    seal(output)


def seal(output):
    files = ['fixture_manifest.json','samples.jsonl','receipt.json']
    inventory = {name:{'sha256':sha_file(output/name),'bytes':(output/name).stat().st_size}
                 for name in files}
    write_json(output/'bundle_manifest.json', inventory)
    files.append('bundle_manifest.json')
    pending, destination = output/'evidence.inprogress.zip', output/'evidence.zip'
    with zipfile.ZipFile(pending, 'x', zipfile.ZIP_DEFLATED) as archive:
        for name in files: archive.write(output/name, name)
    with pending.open('rb') as stream: os.fsync(stream.fileno())
    with zipfile.ZipFile(pending) as archive:
        if archive.namelist() != files or archive.testzip() is not None:
            raise AssertionError('ZIP readback failed')
        for name in inventory:
            if hashlib.sha256(archive.read(name)).hexdigest() != inventory[name]['sha256']:
                raise AssertionError('ZIP content checksum mismatch')
    os.link(pending, destination)
    pending.unlink()
    write_json(output/'sealed.json', {'version':VERSION,'result':'PASS',
        'file':destination.name,'bytes':destination.stat().st_size,'sha256':sha_file(destination),
        'receipt_sha256':sha_file(output/'receipt.json'),'readback_verified':True})


def negative_boundary(args):
    """One expected SQL error, last operation on a separately owned host."""
    import psycopg
    from psycopg import sql
    import ordered_postgres_scope_fixture as scope_fixture
    from ordered_cursor_lifetime_probe import driver_binding
    import research_common_window_metrics_store as common
    output = Path(args.output)
    receipt = connection_receipt(args.connection_receipt)
    sources = source_hashes()
    driver = driver_binding()
    schema = 'ordered_memory_replay_' + uuid4().hex[:24]
    conn = connect(receipt)
    try:
        conn.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
        conn.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
        fixture = scope_fixture.prepare(conn, args)
        conn.commit()
        conn.autocommit = True
        manifest = {'version':VERSION, 'kind':'negative_boundary', 'boundary':args.negative_boundary,
                    'schema':schema, 'fixture':fixture, 'sources':sources, 'driver_binding':driver,
                    'connection_receipt_sha256':sha_file(args.connection_receipt),
                    'run_token':receipt['run_token']}
        write_json(output/'fixture_manifest.json', manifest)
        value = 2**63 if args.negative_boundary == 'above' else -(2**63)-1
        try:
            common.load_by_event_ids(conn, [value], fixture['scope']['window_minutes'])
        except psycopg.Error as error:
            if error.sqlstate != '22003':
                raise AssertionError('Bigint overflow no longer has SQLSTATE 22003') from error
            observed = {'case':'overflow_'+args.negative_boundary+'_int64',
                        'sqlstate':error.sqlstate, 'error_class':type(error).__name__}
        else:
            raise AssertionError('Out-of-range ID was silently accepted')
        # Intentionally no SQL/rollback after the expected statement error.
    finally:
        conn.close()
    if sources != source_hashes() or driver != driver_binding():
        raise AssertionError('Source or driver changed during negative boundary probe')
    result = {'version':VERSION, 'result':'PASS', 'kind':'negative_boundary',
              'actual_loader':True, 'proxy_used':False, 'observed':observed,
              'autocommit':True, 'connection_closed':conn.closed, 'sql_after_error':0,
              'fixture_manifest_sha256':sha_file(output/'fixture_manifest.json'),
              'connection_receipt_sha256':sha_file(args.connection_receipt),
              'source_hashes':sources, 'driver_binding':driver, 'run_token':receipt['run_token']}
    pending = output/'negative_receipt.json.pending'
    write_json(pending, result)
    os.link(pending, output/'negative_receipt.json')
    pending.unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--connection-receipt', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--mode', choices=['history','ingest_changed','ingest_unchanged','scope'], required=True)
    parser.add_argument('--rows', type=int, choices=[32,256,2000,6001], default=256)
    parser.add_argument('--passes', type=int, default=12)
    parser.add_argument('--scope-parity', choices=['full','postonly'], default='full')
    parser.add_argument('--cursor-policy', choices=['baseline','close','scalar-array','source','observed'])
    parser.add_argument('--stage', choices=['prepare','measure','negative'])
    parser.add_argument('--negative-boundary', choices=['above','below'])
    args = parser.parse_args()
    if args.cursor_policy is None:
        args.cursor_policy = 'source' if args.mode == 'scope' else 'baseline'
    if not 1 <= args.passes <= 32: parser.error('passes must be 1..32')
    if args.scope_parity != 'full' and args.mode != 'scope':
        parser.error('--scope-parity postonly requires --mode scope')
    if args.cursor_policy in ('close','scalar-array') and (args.mode != 'scope' or args.scope_parity != 'postonly'):
        parser.error('Cursor treatment policies require scope mode with postonly parity')
    if args.cursor_policy == 'source' and args.mode != 'scope':
        parser.error('Source cursor policy requires scope mode')
    if args.cursor_policy == 'observed' and (args.mode != 'scope' or args.scope_parity != 'postonly'):
        parser.error('Observed cursor policy requires scope mode with postonly parity')
    if args.negative_boundary and (args.mode != 'scope' or args.cursor_policy != 'source'
                                  or args.rows != 32 or args.stage in ('prepare','measure')):
        parser.error('Negative boundary requires source scope with 32 rows and negative stage')
    if args.stage == 'negative' and not args.negative_boundary:
        parser.error('Negative stage requires --negative-boundary')
    connection_receipt(args.connection_receipt)
    if args.stage == 'prepare': return prepare(args)
    if args.stage == 'measure': return measured(args)
    if args.stage == 'negative': return negative_boundary(args)
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    command = [sys.executable, str(Path(__file__).resolve()),
        '--connection-receipt', str(Path(args.connection_receipt).resolve()),
        '--output',str(output),'--mode',args.mode,'--rows',str(args.rows),'--passes',str(args.passes),
        '--scope-parity',args.scope_parity,'--cursor-policy',args.cursor_policy]
    if args.negative_boundary:
        subprocess.run(command+['--negative-boundary',args.negative_boundary,'--stage','negative'],
                       check=True, timeout=240)
        print(canonical({'result':'PASS','negative_receipt':str(output/'negative_receipt.json')}))
        return
    # The light launcher never decodes fixture data or imports the worker, so
    # measured ru_maxrss is not inherited from heavy preparation.
    subprocess.run(command+['--stage','prepare'], check=True, timeout=240)
    subprocess.run(command+['--stage','measure'], check=True, timeout=900)
    print(canonical({'result':'PASS','receipt':str(output/'receipt.json'),
                     'sealed':str(output/'sealed.json')}))


if __name__ == '__main__':
    main()
