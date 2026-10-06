"""Local caller-cost benchmark for passive diagnostics; never runs the bot.

Run from the repository root with ``python -m
hl_testnet_runtime.benchmark_passive_recorder --output /tmp/recorder.json``.
Only this process's recorder is activated. No environment settings, trade state,
database, or exchange are used. All network attempts are rejected explicitly.
Results describe this machine and these payloads, not live trading latency.
"""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import gc
import json
import math
import os
from pathlib import Path
import platform
import random
import socket
import statistics
import sys
import threading
import time
from unittest.mock import patch

if __package__:
    from . import passive_timing as timing
else:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from hl_testnet_runtime import passive_timing as timing


COUNTERS = ('accepted', 'exported', 'dropped_full', 'dropped_busy',
            'dropped_invalid', 'record_errors', 'sink_errors', 'deduplicated',
            'archive_evicted')
SCENARIOS = (
    ('disabled', 1), ('enabled', 1), ('full', 1), ('contended', 1), ('hung', 1),
    ('disabled', 2), ('enabled', 2),
)
PAYLOADS = {
    'notification': ('notification_received', dict(
        account_role='long_account', symbol='SOL', order_id='12345',
        fill_id='54321', exchange_at_ms=1791277200000, generation=1,
        revision=3, notification_type='userFills', batch_count=1,
        captured_count=1)),
    'trade_projection': ('trade_observed', dict(
        account_role='long_account', symbol='SOL', bucket='b' * 64,
        card_id='c' * 64, revision=8, state='PROTECTED', entry_quantity='100',
        exit_quantity='0', remaining_quantity='100', stop_quantity='100',
        take_profit_quantity='100', first_entry_at_ms=1791277200000,
        last_entry_at_ms=1791277200100, last_exit_at_ms=None,
        evidence_at_ms=1791277200200, protection_verified=True,
        closure_verified=False, verification_status='VERIFIED',
        stop_public_status_at_ms=1791277200150,
        stop_observed_at_ms=1791277200200, active_order_ids=('12346', '12347'),
        entry_order_ids=('12345',), issues=())),
}


def summary(values):
    """Nearest-rank percentiles; values remain in nanoseconds."""
    ordered = sorted(values)
    if not ordered:
        return dict(count=0)
    return dict(count=len(ordered), mean=statistics.fmean(ordered),
                median=statistics.median(ordered),
                p95=ordered[max(0, math.ceil(len(ordered) * 0.95) - 1)],
                maximum=ordered[-1], minimum=ordered[0])


@contextmanager
def deny_network():
    attempts = []

    def denied(*args, **kwargs):
        attempts.append('blocked')
        raise RuntimeError('LOCAL_BENCHMARK_NETWORK_DISABLED')

    with patch.object(socket, 'create_connection', denied), \
            patch.object(socket, 'getaddrinfo', denied), \
            patch.object(socket.socket, 'connect', denied), \
            patch.object(socket.socket, 'connect_ex', denied), \
            patch.object(socket.socket, 'sendto', denied):
        yield attempts


def measure_batch(mode, producers, payload_name, calls):
    """Worker lifecycle, locking, synchronization and cleanup are not timed."""
    recorder = None
    release_sink = threading.Event()
    sink_entered = threading.Event()
    lock_held = False
    threads = []
    output = [None] * producers
    failures = []
    gate = threading.Barrier(producers + 1) if producers > 1 else None
    kind, prototype = PAYLOADS[payload_name]
    fields = []
    for index in range(producers):
        role = 'long_account' if index == 0 else 'short_account'
        fields.append({**prototype, 'account_role': role})

    def produce(index):
        durations = []
        accepted = 0
        try:
            if gate is not None:
                gate.wait(timeout=5)
            batch_start = time.perf_counter_ns()
            for _ in range(calls):
                before = time.perf_counter_ns()
                result = timing.record(kind, **fields[index])
                after = time.perf_counter_ns()
                durations.append(after - before)
                accepted += bool(result)
            elapsed = time.perf_counter_ns() - batch_start
            output[index] = dict(durations=durations, accepted=accepted,
                                 batch_elapsed_ns=elapsed)
        except BaseException as exc:
            failures.append(repr(exc))

    try:
        if not timing.set_recorder(None):
            raise RuntimeError('EXISTING_RECORDER_STILL_RUNNING')
        if mode != 'disabled':
            if mode == 'hung':
                def stalled_sink(_line):
                    sink_entered.set()
                    release_sink.wait()
                recorder = timing.Recorder(sink=stalled_sink)
            else:
                recorder = timing.Recorder()
            if not timing.set_recorder(recorder):
                raise RuntimeError('RECORDER_INSTALL_FAILED')
            if mode in ('enabled', 'hung'):
                if mode == 'hung':
                    assert recorder.record('benchmark_seed')
                assert recorder.start()
                if mode == 'hung' and not sink_entered.wait(5):
                    raise RuntimeError('STALLED_WRITER_NOT_READY')
            if mode in ('full', 'hung'):
                # Intentional saturation, excluded from measured calls/counters.
                for _ in range(recorder.capacity):
                    assert recorder.record('benchmark_seed')
            elif mode == 'contended':
                recorder._lock.acquire()
                lock_held = True
        before_health = recorder.health(include_recent=False) if recorder else {}
        if producers == 1:
            produce(0)
        else:
            threads = [threading.Thread(target=produce, args=(index,))
                       for index in range(producers)]
            for thread in threads:
                thread.start()
            gate.wait(timeout=5)
            for thread in threads:
                thread.join(timeout=5)
                if thread.is_alive():
                    raise RuntimeError('PRODUCER_DID_NOT_FINISH')
        if failures:
            raise RuntimeError(str(failures))
        # Drain the normal archive after the measured calls, without adding this
        # wait to caller timings. Forced saturation keeps its original state.
        if mode == 'enabled' and not recorder.stop(timeout=0.25):
            raise RuntimeError('DEFAULT_WRITER_DID_NOT_DRAIN')
        after_health = recorder.health(include_recent=False) if recorder else {}
        delta = {key: after_health.get(key, 0) - before_health.get(key, 0)
                 for key in COUNTERS}
        if any(delta[key] for key in ('dropped_invalid', 'record_errors', 'sink_errors')):
            raise RuntimeError('UNEXPECTED_DIAGNOSTIC_ERROR: ' + str(delta))
        expected = calls * producers
        accepted = sum(row['accepted'] for row in output)
        if mode == 'disabled' and accepted:
            raise RuntimeError('DISABLED_RECORDER_ACCEPTED')
        if mode in ('full', 'hung') and delta['dropped_full'] != expected:
            raise RuntimeError('FULL_QUEUE_SETUP_FAILED')
        if mode == 'contended' and delta['dropped_busy'] != expected:
            raise RuntimeError('LOCK_CONTENTION_SETUP_FAILED')
        return dict(durations=[value for row in output for value in row['durations']],
                    producer_batch_elapsed_ns=[row['batch_elapsed_ns'] for row in output],
                    accepted_return_values=accepted, counters=delta)
    finally:
        if lock_held:
            recorder._lock.release()
        release_sink.set()
        if recorder is not None:
            if not recorder.stop(timeout=0.25):
                raise RuntimeError('BENCHMARK_WRITER_LEAK')
        if not timing.set_recorder(None):
            raise RuntimeError('BENCHMARK_RECORDER_CLEANUP_FAILED')


def execute(rounds, warmups, calls, seed):
    if rounds < 2 or rounds % 2 or warmups < 0 or not 1 <= calls <= 128:
        raise ValueError('Use positive even rounds, nonnegative warmups, 1..128 calls')
    conditions = [(payload, mode, producers) for payload in PAYLOADS
                  for mode, producers in SCENARIOS]
    random.Random(seed).shuffle(conditions)
    samples = {condition: [] for condition in conditions}
    with deny_network() as network_attempts:
        timer_baseline = []
        for _ in range(10000):
            before = time.perf_counter_ns()
            timer_baseline.append(time.perf_counter_ns() - before)
        for iteration in range(warmups + rounds):
            # Every adjacent pair reverses exact order, balancing early/late
            # positions. Each timed batch gets a fresh recorder outside timing.
            order = conditions if iteration % 2 == 0 else list(reversed(conditions))
            for condition in order:
                payload, mode, producers = condition
                row = measure_batch(mode, producers, payload, calls)
                if iteration >= warmups:
                    row['round'] = iteration - warmups
                    samples[condition].append(row)
    rows = []
    for condition in sorted(samples):
        payload, mode, producers = condition
        batches = samples[condition]
        per_call = [n for row in batches for n in row['durations']]
        counters = {key: sum(row['counters'][key] for row in batches) for key in COUNTERS}
        rows.append(dict(payload=payload, fields=len(PAYLOADS[payload][1]), mode=mode,
                         producers=producers, intentional_saturation=mode in ('full', 'contended', 'hung'),
                         caller_duration_ns=summary(per_call),
                         per_round_mean_ns=summary([statistics.fmean(row['durations']) for row in batches]),
                         producer_batch_elapsed_ns=summary([n for row in batches for n in row['producer_batch_elapsed_ns']]),
                         accepted_return_values=sum(row['accepted_return_values'] for row in batches),
                         counters=counters,
                         rounds=[dict(round=row['round'], caller_duration_ns=summary(row['durations']),
                                      counters=row['counters']) for row in batches]))
    comparisons = []
    for payload in PAYLOADS:
        for producers in (1, 2):
            disabled = {row['round']: statistics.fmean(row['durations'])
                        for row in samples[(payload, 'disabled', producers)]}
            enabled = {row['round']: statistics.fmean(row['durations'])
                       for row in samples[(payload, 'enabled', producers)]}
            comparisons.append(dict(payload=payload, producers=producers,
                paired_round_mean_difference_ns=summary([enabled[i] - disabled[i] for i in range(rounds)]),
                definition='Active-recorder mean caller duration minus disabled mean in the same round.'))
    return dict(schema='passive_recorder_local_benchmark_v1',
                completed_at_utc=datetime.now(timezone.utc).isoformat(),
                environment=dict(python=sys.version, platform=platform.platform(),
                                 cpu_count=os.cpu_count(), load_average=os.getloadavg(),
                                 gc_enabled=gc.isenabled(),
                                 perf_counter_resolution_seconds=time.get_clock_info('perf_counter').resolution),
                settings=dict(rounds=rounds, warmup_rounds=warmups, calls_per_producer_per_batch=calls,
                              seed=seed, default_capacity=256, default_batch_size=32),
                network_attempts_blocked=len(network_attempts),
                timer_pair_baseline_ns=summary(timer_baseline), results=rows, comparisons=comparisons,
                caveats=[
                    'No real exchange, database, bot controller or Render scheduling is measured.',
                    'Caller durations include Python keyword expansion and timer overhead; no timer subtraction is applied.',
                    'Thread start, barrier, recorder setup, stop/drain and health inspection are outside timed calls.',
                    'Batch elapsed times additionally include loop/sample bookkeeping and are not per-call timings.',
                    'Full, contended and hung-writer cases intentionally discard all measured diagnostic events.',
                    'The production-default enabled case uses the real background archive worker and bounded buffer.',
                    'Two producers start together but Python scheduling/GIL may serialize work; this is not CPU parallelism proof.',
                    'Identical trade projections are deliberately deduplicated by the normal archive worker.',
                    'Maximum and mean times are sensitive to local CPU scheduling; no production overhead guarantee follows.',
                    'Measurements describe the timing.record caller only, not an entire trading hook or fill-to-protection latency.',
                ])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--rounds', type=int, default=24)
    parser.add_argument('--warmups', type=int, default=2)
    parser.add_argument('--calls', type=int, default=64)
    parser.add_argument('--seed', type=int, default=20261006)
    args = parser.parse_args()
    report = execute(args.rounds, args.warmups, args.calls, args.seed)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    print(json.dumps(dict(output=str(args.output.resolve()),
                          conditions=len(report['results']),
                          measured_calls=sum(row['caller_duration_ns']['count'] for row in report['results']),
                          network_attempts_blocked=report['network_attempts_blocked']), sort_keys=True))


if __name__ == '__main__':
    main()
