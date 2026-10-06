"""Offline real-clock comparison; never starts the bot or an exchange adapter.

Requires the disposable loopback hl_journal_ci database used by existing tests.
Run serially: the existing fixture clears its own CI schemas on every capture.
All exchange prices, fills and exchange timestamps are synthetic. Only the
independent perf_counter_ns durations reported here are measured wall time.
"""
import argparse
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import statistics
import time
from unittest.mock import patch

from . import test_passive_timing_durable as driver
from . import test_long_stream_runtime as fixtures
from . import long_stream_runtime as stream
from . import passive_timing as timing
from .postgres_journal import PostgresJournal


CLOCK = time.perf_counter_ns
MODES = ('disabled', 'enabled', 'full', 'contended', 'hung')


def stats(values):
    ordered = sorted(values)
    if not ordered:
        return {'n': 0}
    def percentile(q):
        position = (len(ordered) - 1) * q
        lo = int(position)
        hi = min(lo + 1, len(ordered) - 1)
        return ordered[lo] + (ordered[hi] - ordered[lo]) * (position - lo)
    return dict(n=len(ordered), mean=statistics.mean(ordered),
                median=statistics.median(ordered), p95=percentile(.95),
                min=ordered[0], max=ordered[-1])


def capture(mode, side, lost_reply):
    durations = defaultdict(list)
    pending = []
    original_fixture = fixtures.DurableLongStreamTests
    original_tick, original_observe = stream.tick, stream.observed_trades

    class ClockedFixture(original_fixture):
        def setUp(self):
            super().setUp()
            original_fill = self.v.fill
            quantity = Decimal(0)
            def fill(oid, amount):
                nonlocal quantity
                is_entry = self.v.orders[oid]['order']['orderType'] == 'Limit'
                result = original_fill(oid, amount)
                if is_entry:
                    quantity += Decimal(amount)
                    pending.append((CLOCK(), quantity))
                return result
            self.v.fill = fill

    def tick(*args, **kwargs):
        started = CLOCK()
        try:
            return original_tick(*args, **kwargs)
        finally:
            durations['tick_ms'].append((CLOCK() - started) / 1e6)

    def observe(*args, **kwargs):
        started = CLOCK()
        result = original_observe(*args, **kwargs)
        finished = CLOCK()
        durations['projection_ms'].append((finished - started) / 1e6)
        for row in result:
            if pending and row.get('protection_verified'):
                entered = Decimal(row['entry_quantity'])
                while pending and entered >= pending[0][1]:
                    at, quantity = pending.pop(0)
                    assert Decimal(row['stop_quantity']) >= quantity
                    assert Decimal(row['take_profit_quantity']) >= quantity
                    durations['simulated_fill_to_verified_projection_ms'].append(
                        (finished - at) / 1e6)
        return result

    with patch.object(fixtures, 'DurableLongStreamTests', ClockedFixture), \
            patch.object(stream, 'tick', tick), \
            patch.object(stream, 'observed_trades', observe):
        trace, health = driver.capture_scenario(mode, side=side, lost_reply=lost_reply)
    assert not pending, 'FILL_NOT_PROTECTED'
    assert len(durations['simulated_fill_to_verified_projection_ms']) == 3
    assert trace['sent'] == 8 and trace['reads'] == 174
    assert trace['trades'][-1][0]['closure_verified']
    assert timing._recorder is None, 'RECORDER_NOT_CLEANED_UP'
    if mode != 'disabled':
        assert health['dropped_invalid'] == health['record_errors'] == health['sink_errors'] == 0
        if mode in ('full', 'contended'):
            assert health['dropped_full' if mode == 'full' else 'dropped_busy'] > 0
        if mode == 'enabled':
            assert health['accepted'] > 0
            assert health['dropped_full'] == health['dropped_busy'] == 0
    durations['runtime_subtotal_ms'] = [sum(durations['tick_ms']) + sum(durations['projection_ms'])]
    return trace, dict(durations), health


def bootstrap_mean_interval(values):
    rng = random.Random(60261006)
    means = sorted(statistics.mean(rng.choices(values, k=len(values))) for _ in range(3000))
    return [means[74], means[2924]]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rounds', type=int, default=12)
    parser.add_argument('--warmups', type=int, default=1)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.rounds < 2 or args.warmups < 1:
        parser.error('Use at least 2 rounds and 1 warmup')
    # Existing CI validator rejects every non-loopback/database-name DSN.
    PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL'])
    rows, checks, hashes = [], 0, {}
    started = datetime.now(timezone.utc).isoformat()
    scenarios = [(side, lost) for side in ('LONG', 'SHORT') for lost in (False, True)]
    for round_index in range(-args.warmups, args.rounds):
        for scenario_index, (side, lost) in enumerate(scenarios):
            order = list(MODES)
            shift = (round_index + scenario_index) % len(order)
            order = order[shift:] + order[:shift]
            if round_index % 2:
                order.reverse()
            reference = None
            for mode in order:
                trace, durations, health = capture(mode, side, lost)
                if reference is None:
                    reference = trace
                else:
                    assert trace == reference, f'TRACE_CHANGED: {side}/{lost}/{mode}'
                    checks += 1
                canonical = json.dumps(trace, sort_keys=True, separators=(',', ':'))
                digest = hashlib.sha256(canonical.encode()).hexdigest()
                key = f'{side}_lost_reply_{lost}'
                if key in hashes:
                    assert hashes[key] == digest, 'NONDETERMINISTIC_TRACE'
                hashes[key] = digest
                if round_index >= 0:
                    rows.append(dict(round=round_index, side=side, lost_reply=lost,
                                     mode=mode, durations=durations, health=health))
        print(json.dumps({'completed_round': round_index + 1,
                          'measured_scenarios': len(rows)}), flush=True)

    summaries, comparisons = {}, {}
    by_mode = defaultdict(lambda: defaultdict(list))
    for row in rows:
        for metric, values in row['durations'].items():
            by_mode[row['mode']][metric].extend(values)
    for mode, metrics in by_mode.items():
        summaries[mode] = {metric: stats(values) for metric, values in metrics.items()}
    baselines = {(r['round'], r['side'], r['lost_reply']): r for r in rows if r['mode'] == 'disabled'}
    for mode in MODES[1:]:
        deltas, ratios = [], []
        by_round = defaultdict(list)
        for row in rows:
            if row['mode'] != mode:
                continue
            baseline = baselines[(row['round'], row['side'], row['lost_reply'])]
            before = baseline['durations']['runtime_subtotal_ms'][0]
            after = row['durations']['runtime_subtotal_ms'][0]
            deltas.append(after - before)
            by_round[row['round']].append(after - before)
            ratios.append(100 * (after - before) / before)
        comparisons[mode] = dict(paired_delta_ms=stats(deltas),
            paired_delta_pct=stats(ratios),
            bootstrap_unit='round_mean_across_four_scenarios',
            bootstrap_mean_delta_95pct_ms=bootstrap_mean_interval(
                [statistics.mean(values) for values in by_round.values()]))
    report = dict(schema='local_passive_runtime_benchmark_v1', started_at=started,
        finished_at=datetime.now(timezone.utc).isoformat(),
        python=platform.python_version(), platform=platform.platform(),
        rounds=args.rounds, warmups=args.warmups, modes=MODES,
        trace_equality_checks_including_warmups=checks, trace_hashes=hashes,
        simulated_order_requests_per_scenario=8, simulated_reader_calls_per_scenario=174,
        methodology='Rotating/reversed mode order, real perf_counter_ns; setup, '
                    'registration, final trace inspection and cleanup excluded. '
                    'Subtotal sums only disjoint tick and projection calls. '
                    'All exchange responses synthetic; real loopback PostgreSQL. '
                    'Bootstrap is descriptive local sampling uncertainty, not a production guarantee.',
        limitations=['No exchange latency, network scheduling, signing, Render, or real account access.',
                     'Fill-to-projection includes existing local reporting and simulated restart when applicable.',
                     'Shared executor and tmpfs database; p95/max are observed samples only.',
                     'Recorder global requires serial scenarios; concurrency covered separately.',
                     'Negative paired differences can reflect scheduler noise, not speedups.'],
        summary=summaries, comparisons=comparisons, raw_samples=rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'output': str(args.output), 'summary': summaries,
                      'comparisons': comparisons}, indent=2))


if __name__ == '__main__':
    main()
