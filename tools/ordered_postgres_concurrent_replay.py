#!/usr/bin/env python3
"""Two bounded synthetic research workloads in one native PostgreSQL client.

Preparation and measurement are separate processes. Only the coordinator reads
process RSS/CPU and writes samples. Workers retain payloads at an explicit ready
gate until the coordinator samples their combined resident footprint.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, wait
import gc
import hashlib
import importlib
import json
import os
from pathlib import Path
import platform
import sys
import threading
import time
from types import SimpleNamespace
from uuid import uuid4

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import ordered_postgres_memory_replay as replay

VERSION = 'ordered-postgres-concurrent-replay-v1'
KIND = 'ordered_scope_and_btc_report'
LANES = ('ordered', 'report')
PROTOCOL = {'rows_per_lane': 2000, 'report_parents': 8, 'events_per_parent': 250,
    'rounds': 12, 'lanes': 2, 'gate_timeout_seconds': 30,
    'round_timeout_seconds': 60, 'abort_join_seconds': 5,
    'samples': 25, 'scope_parity': 'postonly', 'cursor_policy': 'observed',
    'transaction_read_only': True, 'transaction_isolation': 'repeatable read'}
LIMITATIONS = [
    'Synthetic fixed populations; no production cardinality or OOM-resolution claim.',
    'One ordered scope and one BTC report source/digest workload overlap in one process.',
    'Report calculation, provider reads, publication, collector and Chromium are absent.',
    'Ready/release gates deliberately hold both payload graphs; this is a controlled overlap.',
    'RSS, allocator statistics, GC counters and CPU time are process-wide, not per-thread attribution.',
    'Three boundary kinds are sampled; sampled RSS is not a continuous peak.',
    'Normal GC only; no forced collection or allocator trimming in measured rounds.',
    'Full fixture parity/checkpoints run before preparation ends and after measured endpoints.',
]


def require(value, message):
    if not value:
        raise AssertionError(message)


def digest(value):
    return hashlib.sha256(replay.canonical(value).encode()).hexdigest()


class RoundGate:
    """Two-stage gate retaining metadata only; abort wakes every waiting lane."""
    def __init__(self, round_index, gate_timeout=30.0, round_timeout=60.0):
        if gate_timeout <= 0 or round_timeout <= 0:
            raise ValueError('Positive gate deadlines required')
        self.round_index = round_index
        self.gate_timeout = gate_timeout
        self.deadline = time.monotonic() + round_timeout
        self.condition = threading.Condition()
        self.ready = {}
        self.released = {}
        self.both_loaded_ns = None
        self.release_ns = None
        self.error = None

    def _abort_locked(self, error):
        if self.error is None:
            self.error = str(error)[:300]
        self.condition.notify_all()

    def abort(self, error):
        with self.condition:
            self._abort_locked(error)

    def _wait(self, predicate, deadline):
        while not predicate():
            if self.error is not None:
                raise RuntimeError('Round aborted: ' + self.error)
            remaining = min(deadline, self.deadline) - time.monotonic()
            if remaining <= 0:
                self._abort_locked('Gate or round deadline exceeded')
                raise TimeoutError('Gate or round deadline exceeded')
            self.condition.wait(remaining)
        if self.error is not None:
            raise RuntimeError('Round aborted: ' + self.error)

    def worker_ready(self, lane, identity):
        with self.condition:
            require(lane in LANES and lane not in self.ready, 'Duplicate/unknown ready lane')
            require(set(identity) == {'pid', 'thread_id', 'backend_pid'}
                    and all(type(v) is int and v > 0 for v in identity.values()),
                    'Invalid lane identity')
            self.ready[lane] = {**identity, 'ready_ns': time.monotonic_ns()}
            self.condition.notify_all()
            self._wait(lambda: self.release_ns is not None, time.monotonic() + self.gate_timeout)
            self.released[lane] = time.monotonic_ns()

    def wait_loaded(self):
        with self.condition:
            self._wait(lambda: set(self.ready) == set(LANES), time.monotonic() + self.gate_timeout)
            require(self.both_loaded_ns is None, 'Loaded boundary sampled twice')
            self.both_loaded_ns = time.monotonic_ns()
            return self._snapshot_locked()

    def release(self):
        with self.condition:
            require(self.error is None and self.both_loaded_ns is not None
                    and self.release_ns is None, 'Release without successful loaded boundary')
            self.release_ns = time.monotonic_ns()
            self.condition.notify_all()

    def _snapshot_locked(self):
        return {'ready': {lane: dict(meta) for lane, meta in self.ready.items()},
                'both_loaded_ns': self.both_loaded_ns, 'release_ns': self.release_ns,
                'released_ns': dict(self.released)}

    def snapshot(self):
        with self.condition:
            return self._snapshot_locked()


def report_fixture():
    return importlib.import_module('ordered_concurrent_report_fixture')


def source_hashes():
    fixture = report_fixture()
    names = set(getattr(fixture, 'SOURCE_FILES', ())) | {
        'tools/ordered_postgres_concurrent_replay.py',
        'tools/ordered_postgres_concurrent_replay_selftest.py',
        'tools/ordered_concurrent_report_fixture.py',
        'tools/verify_ordered_native_concurrent.py',
        'tools/verify_ordered_native_memory.py',
        'market_session_baseline.py', 'research_current_publication.py',
        'research_event_capture.py', 'research_formula_acceptance.py',
        'research_formula_engine.py', 'research_formula_families.py',
        'research_historical_replay.py', 'research_market_episode.py',
        'research_max_pain_archive.py', 'research_maxpain_sheet_rows.py',
        'research_mfe_mae_efficiency.py', 'research_prospective_anchors.py',
        'research_prospective_feature_freeze.py', 'research_sheet_publication.py',
        'tools/render_native_build/concurrent_entrypoint.py',
        'research_btc_wave_report_worker.py', 'research_btc_wave_endpoint_report.py',
        'research_outcome_worker.py', 'research_btc_wave_endpoint_report_selftest.py'}
    values = replay.source_hashes()
    for name in sorted(names):
        require(not Path(name).is_absolute() and '..' not in Path(name).parts, 'Unsafe source inventory path')
        values[name] = replay.sha_file(ROOT/name)
    return values


def loaded_modules():
    result = {}
    for name in ('research_common_window_metrics_store', 'research_btc_wave_report_worker',
                 'research_btc_wave_endpoint_report'):
        module = importlib.import_module(name)
        path = Path(module.__file__).resolve()
        require(path == ROOT/(name + '.py'), 'Application module loaded outside selected source root')
        result[name] = {'path': str(path), 'sha256': replay.sha_file(path)}
    return result


def host_receipt(path):
    receipt = replay.connection_receipt(path)
    require(receipt.get('engine') == 'native PostgreSQL', 'Native PostgreSQL receipt required')
    return receipt


def scope_operation(conn, fixture, *, full, phase=None):
    counts = {}
    result = replay.run_operation(conn, 'scope', fixture, [], phase,
        full_parity=full, cursor_policy='observed', cursor_counts=counts)
    return result, counts


def prepare(args):
    from psycopg import sql
    import ordered_postgres_scope_fixture as scope
    from ordered_cursor_lifetime_probe import driver_binding, driver_cycle_probe
    report = report_fixture()
    host = host_receipt(args.connection_receipt)
    schema = 'ordered_memory_replay_' + uuid4().hex[:24]
    with replay.connect(host) as conn:
        conn.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
        conn.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
        fixture = scope.prepare_scope(conn, 2000)
        conn.commit()
        expected, counts = scope_operation(conn, fixture, full=True)
        checkpoint = scope.verify(conn, fixture)
        conn.rollback()
        server = conn.execute('SELECT version() AS version').fetchone()['version']
    measured = {key: value for key, value in expected.items() if key not in replay.SCOPE_DIAGNOSTIC_HASHES}
    measured['full_parity_verified'] = False
    with replay.connect(host) as conn:
        report_meta = report.prepare(conn)
        conn.commit()
        report_expected = report.load(conn, report_meta)
        report_full = report.full_oracle(conn, report_meta)
        report_checkpoint = report.checkpoint(conn, report_meta)
        conn.rollback()
    manifest = {'version': VERSION, 'kind': KIND, 'protocol': PROTOCOL,
        'preparation_pid': os.getpid(), 'connection_receipt_sha256': replay.sha_file(args.connection_receipt),
        'sources': source_hashes(), 'driver_binding': driver_binding(),
        'driver_cycle_probe': driver_cycle_probe(lambda: replay.connect(host, schema)),
        'server': server, 'engine': host['engine'], 'loaded_modules': loaded_modules(),
        'ordered': {'schema': schema, 'fixture': fixture, 'expected': expected,
                    'expected_measured': measured, 'checkpoint': checkpoint, 'cursor_counts': counts},
        'report': {'fixture': report_meta, 'expected': report_expected,
                   'full_oracle': report_full, 'checkpoint': report_checkpoint},
        'limitations': LIMITATIONS}
    replay.write_json(Path(args.output)/'fixture_manifest.json', manifest)


def lane_operation(lane, index, gate, host, manifest):
    """No graph, cursor or connection escapes into the future's result."""
    try:
        schema = manifest['ordered']['schema'] if lane == 'ordered' else None
        with replay.connect(host, schema) as conn:
            conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
            transaction = conn.execute("SELECT current_setting('transaction_read_only') AS read_only, "
                "current_setting('transaction_isolation') AS isolation").fetchone()
            require(transaction == {'read_only': 'on', 'isolation': 'repeatable read'},
                    'Lane transaction is not read-only/repeatable-read')
            identity = {'pid': os.getpid(), 'thread_id': threading.get_native_id(),
                        'backend_pid': int(conn.info.backend_pid)}
            target = 'input_hash' if lane == 'ordered' else 'report_loaded'
            reached = False
            def phase(name):
                nonlocal reached
                if name == target:
                    require(not reached, 'Lane reached loaded boundary twice')
                    reached = True
                    gate.worker_ready(lane, identity)
            if lane == 'ordered':
                result, counts = scope_operation(conn, manifest['ordered']['fixture'], full=False, phase=phase)
                expected = manifest['ordered']['expected_measured']
                require(counts == manifest['ordered']['cursor_counts'], 'Observed source binding changed')
            else:
                result = report_fixture().load(conn, manifest['report']['fixture'], phase=phase)
                counts, expected = None, manifest['report']['expected']
            require(reached and result == expected, 'Repeated lane workload/oracle mismatch')
            conn.rollback()
        require(conn.closed, 'Lane connection remained open after its workload')
        return {**identity, 'round_index': index, 'result': result, 'result_sha256': digest(result),
                'cursor_counts': counts, 'commits_in_workload': 0, 'rollbacks': 1,
                'connection_closed': True, 'transaction': transaction}
    except BaseException as error:
        gate.abort(type(error).__name__ + ': ' + str(error)[:200])
        raise


def _abort_workers(gate, futures, executors, output, error):
    gate.abort(type(error).__name__ + ': ' + str(error)[:200])
    for future in futures.values():
        future.cancel()
    _, pending = wait(tuple(futures.values()), timeout=PROTOCOL['abort_join_seconds'])
    for executor in executors.values():
        executor.shutdown(wait=False, cancel_futures=True)
    if pending:
        # Python cannot kill a running executor thread. Terminate only this
        # disposable measured child; its parent still owns PostgreSQL cleanup.
        replay.write_json(output/'failure.json', {'version': VERSION, 'result': 'FAIL',
            'round_index': gate.round_index, 'gate': gate.snapshot(),
            'error': type(error).__name__ + ': ' + str(error)[:200],
            'workers_quiesced': False, 'forced_measured_child_exit': True})
        os._exit(2)


def measured(args):
    import psycopg
    import runtime_allocation_diagnostics as allocation
    import ordered_postgres_scope_fixture as scope
    from ordered_memory_replay import Observer
    from ordered_cursor_lifetime_probe import driver_binding, source_semantics_audit
    output, report = Path(args.output), report_fixture()
    manifest = json.loads((output/'fixture_manifest.json').read_text())
    require(manifest['version'] == VERSION and manifest['kind'] == KIND
            and manifest['protocol'] == PROTOCOL, 'Wrong prepared concurrent protocol')
    require(manifest['sources'] == source_hashes() and manifest['loaded_modules'] == loaded_modules(),
            'Source/module changed after preparation')
    require(manifest['driver_binding'] == driver_binding(), 'Driver changed after preparation')
    require(manifest['connection_receipt_sha256'] == replay.sha_file(args.connection_receipt),
            'Connection receipt changed')
    require(manifest['preparation_pid'] != os.getpid(), 'Preparation/measurement must be distinct processes')
    host = host_receipt(args.connection_receipt)
    # Load application definitions before the baseline; no full fixture graph.
    scope._imports()
    coordinator = threading.get_native_id()
    observer = Observer(output, allocation, False)
    state = SimpleNamespace(pass_index=0, scope_index=0)
    executors = {lane: ThreadPoolExecutor(max_workers=1, thread_name_prefix='concurrent-' + lane)
                 for lane in LANES}
    threads = {lane: pool.submit(threading.get_native_id).result(timeout=5) for lane, pool in executors.items()}
    require(len(set(threads.values())) == 2 and coordinator not in threads.values(), 'Two distinct worker threads required')
    results = []
    try:
        observer.sample(state, 'observer_baseline', extra={'round_index': 0, 'lane_thread_ids': threads})
        for index in range(1, 13):
            state.pass_index = index
            started_ns = time.monotonic_ns()
            gate = RoundGate(index)
            before, started, cpu = observer.total_ns, time.perf_counter_ns(), time.process_time_ns()
            futures = {lane: executors[lane].submit(lane_operation, lane, index, gate, host, manifest) for lane in LANES}
            try:
                loaded = gate.wait_loaded()
                require({v['pid'] for v in loaded['ready'].values()} == {os.getpid()}
                        and len({v['backend_pid'] for v in loaded['ready'].values()}) == 2
                        and all(loaded['ready'][lane]['thread_id'] == threads[lane] for lane in LANES),
                        'Overlap identities differ from process/threads/connections')
                observer.sample(state, 'both_loaded', extra={'round_index': index, 'gate': loaded})
                gate.release()
                lane_results = {lane: future.result(timeout=max(0.001, gate.deadline - time.monotonic()))
                                for lane, future in futures.items()}
                completed_ns = time.monotonic_ns()
                require(time.monotonic() <= gate.deadline, 'Round exceeded 60 seconds')
                elapsed, cpu_used = time.perf_counter_ns()-started, time.process_time_ns()-cpu
                final_gate = gate.snapshot()
                require(set(final_gate['released_ns']) == set(LANES), 'A lane did not observe release')
                record = {'round_index': index, 'lanes': lane_results, 'gate': final_gate,
                          'started_ns': started_ns, 'completed_ns': completed_ns,
                          'elapsed_ns': elapsed, 'cpu_ns': cpu_used, 'observer_ns': observer.total_ns-before}
                observer.sample(state, 'round_end', event='round_end', extra=record)
                results.append(record)
                del futures, lane_results
            except BaseException as error:
                _abort_workers(gate, futures, executors, output, error)
                raise
    finally:
        for executor in executors.values():
            executor.shutdown(wait=True, cancel_futures=True)
        observer.stream.flush()
        os.fsync(observer.stream.fileno())
        observer.stream.close()
    require(observer.thread_ids == {coordinator} and observer.count == 25, 'Coordinator journal shape changed')
    with replay.connect(host, manifest['ordered']['schema']) as conn:
        ordered_checkpoint = scope.verify(conn, manifest['ordered']['fixture'])
        ordered_full, full_counts = scope_operation(conn, manifest['ordered']['fixture'], full=True)
        conn.rollback()
    with replay.connect(host) as conn:
        report_checkpoint = report.checkpoint(conn, manifest['report']['fixture'])
        report_full = report.full_oracle(conn, manifest['report']['fixture'])
        conn.rollback()
    require(ordered_checkpoint == manifest['ordered']['checkpoint']
            and report_checkpoint == manifest['report']['checkpoint'], 'Fixture checkpoint changed')
    require(ordered_full == manifest['ordered']['expected']
            and full_counts == manifest['ordered']['cursor_counts']
            and report_full == manifest['report']['full_oracle'], 'Full postmeasurement oracle changed')
    semantics = source_semantics_audit(lambda: replay.connect(host, manifest['ordered']['schema']),
        manifest['ordered']['fixture']['scope']['window_minutes'])
    require(source_hashes() == manifest['sources'] and driver_binding() == manifest['driver_binding'],
            'Source/driver changed during measurement')
    samples = output/'samples.jsonl'
    os.link(observer.pending_path, samples)
    observer.pending_path.unlink()
    count, round_ids = 0, []
    with samples.open() as stream:
        for line in stream:
            row = json.loads(line)
            count += 1
            if row['event'] == 'round_end': round_ids.append(row['round_index'])
    require(count == 25 and round_ids == list(range(1, 13)), 'Incomplete concurrent journal')
    completed = {'version': VERSION, 'kind': KIND, 'result': 'PASS', 'protocol': PROTOCOL,
        'fixture_manifest_sha256': replay.sha_file(output/'fixture_manifest.json'),
        'source_hashes': manifest['sources'], 'driver_binding': manifest['driver_binding'],
        'driver_cycle_probe': manifest['driver_cycle_probe'], 'loaded_modules': manifest['loaded_modules'],
        'preparation_pid': manifest['preparation_pid'], 'measurement_pid': os.getpid(),
        'coordinator_thread_id': coordinator, 'lane_thread_ids': threads,
        'connection_receipt_sha256': manifest['connection_receipt_sha256'],
        'engine': host['engine'], 'server': manifest['server'], 'results': results,
        'samples': {'file': samples.name, 'bytes': samples.stat().st_size,
                    'sha256': replay.sha_file(samples), 'records': count, 'round_ids': round_ids},
        'checkpoint_unchanged': True, 'postmeasurement_content_parity': True,
        'postmeasurement': {'ordered_checkpoint': ordered_checkpoint, 'report_checkpoint': report_checkpoint,
            'ordered_full': ordered_full, 'report_full': report_full, 'ordered_source_semantics': semantics},
        'python': sys.version, 'libc': platform.libc_ver(), 'psycopg': psycopg.__version__,
        'libpq': psycopg.pq.version(), 'gc_thresholds': gc.get_threshold(), 'gc_enabled': gc.isenabled(),
        'forced_gc_calls': 0, 'allocator_trim_calls': 0,
        'control_count_scope': 'Measured rounds only; preparation-only driver probe is outside measurement.',
        'probe_observer_ns': observer.total_ns,
        'observer_scope': 'Coordinator-only process measurements; round timings include gates and both-loaded probe; round_end probe is outside round timing.',
        'limitations': LIMITATIONS}
    replay.write_json(output/'receipt.json', completed)
    replay.seal(output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--selftest-gates', action='store_true')
    parser.add_argument('--stage', choices=('prepare', 'measure'))
    parser.add_argument('--connection-receipt', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if args.selftest_gates:
        require(args.stage is args.connection_receipt is args.output is None,
                'Gate selftests do not accept database/stage/output arguments')
        import unittest
        from ordered_postgres_concurrent_replay_selftest import RoundGateFailureTests
        outcome = unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromTestCase(RoundGateFailureTests))
        require(outcome.wasSuccessful() and outcome.testsRun == 2, 'Gate failure selftests did not pass')
        print(replay.canonical({'result': 'PASS', 'kind': 'offline_gate_failures', 'tests': 2}))
        return
    require(args.stage is not None and args.connection_receipt is not None and args.output is not None,
            'stage, connection-receipt and output are required')
    require(args.output.is_dir() and not any(p.is_symlink() for p in (args.output, *args.output.parents)),
            'An owned nonsymlink output directory is required')
    if args.stage == 'prepare':
        require(not any(args.output.iterdir()), 'Preparation output must be empty')
        prepare(args)
    else:
        require(sorted(p.name for p in args.output.iterdir()) == ['fixture_manifest.json'],
                'Measurement output must contain only the prepared manifest')
        measured(args)


if __name__ == '__main__':
    main()
