"""Offline elapsed-cost comparison of the actual notification receive hook.

The decision/readiness clock and synthetic exchange messages come from existing
test fixtures; measured durations and Recorder timestamps use real clocks. No
feed socket threads, exchange requests, signing, databases or bot loop run.
"""
import argparse
from copy import deepcopy
from datetime import datetime, timezone
import gc
import json
import os
from pathlib import Path
import platform
import random
import sys
import time
from unittest.mock import patch

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hl_testnet_runtime import fill_wakeups as wake, passive_timing as timing
from hl_testnet_runtime.benchmark_passive_recorder import COUNTERS, deny_network, summary
from hl_testnet_runtime.test_fill_wakeups import A, B, fill, fills
from hl_testnet_runtime.test_passive_timing_hooks import PassiveTimingHookTests


def state(feed):
    return dict(states=deepcopy(feed._states), health=feed.health(),
                dirty={role: feed.dirty_symbols(account)
                       for role, account in (('long_account', A), ('short_account', B))},
                entries={role: feed.entry_allowed(account)
                         for role, account in (('long_account', A), ('short_account', B))},
                pending=feed.pending_accounts(), wake=feed.wake_event.is_set())


def prepare(frame_size, account, duplicate):
    """All message construction and feed bootstrap stay outside timing."""
    assert timing.set_recorder(None)
    fixture = PassiveTimingHookTests()
    feed = fixture.ready_feed()
    rows = [fill('DOGE' if i == frame_size - 1 else ('SOL' if i < 64 else 'ETH'),
                 tid=i + 1, oid=i + 100, at=1000 + i)
            for i in range(frame_size)]
    raw = json.dumps(fills(account, rows=rows))
    assert len(raw) <= wake.MAX_FRAME_BYTES
    generation = feed._states[account]['generation']
    if duplicate:
        assert frame_size <= wake.MAX_SEEN
        assert feed._receive(account, generation, raw)
        assert feed.finish_reconciliation(feed.begin_reconciliation(account), complete=True)
        feed.wake_event.clear()
    assert not feed._threads and not feed._sockets
    return feed, raw, generation


def measure(feed, raw, generation, account, enabled, frame_size, duplicate):
    recorder = None
    expected_receipts = 0 if duplicate else min(frame_size, 64)
    before_state = state(feed)
    try:
        assert timing.set_recorder(None)
        if enabled:
            recorder = timing.Recorder()
            assert timing.set_recorder(recorder)
            assert recorder.start()
        receive = feed._receive
        start = time.perf_counter_ns()
        accepted = receive(account, generation, raw)
        elapsed = time.perf_counter_ns() - start
        assert accepted is True
        assert not feed._threads and not feed._sockets
        if recorder is not None:
            assert recorder.stop(timeout=0.25), 'RECORDER_DID_NOT_DRAIN'
            health = recorder.health(include_recent=True)
            events = health['recent_events']
            assert health['dropped_invalid'] == 0
            assert health['record_errors'] == 0
            assert health['sink_errors'] == 0
            assert health['archive_evicted'] == 0
            assert health['deduplicated'] == 0
            assert health['accepted'] <= expected_receipts <= 64
            assert health['accepted'] + health['dropped_busy'] + health['dropped_full'] == expected_receipts
            assert len(events) == health['accepted'] == health['exported']
            expected_role = 'long_account' if account == A else 'short_account'
            for event in events:
                assert event['kind'] == 'notification_received'
                assert event['account_role'] == expected_role
                assert event['captured_count'] == expected_receipts
                assert event['batch_count'] == frame_size
                # Record timestamps must be real clock captures, independent of
                # the deliberately synthetic exchange and decision times.
                assert event['at_ms'] > 1_000_000_000_000
                assert event['mono_ns'] > 0
        else:
            health = {}
        after_state = state(feed)
        if duplicate:
            assert after_state == before_state
            assert not after_state['wake']
            assert after_state['pending'] == ()
        else:
            opposite = B if account == A else A
            assert after_state['wake']
            assert not feed.entry_allowed(account)
            assert feed.entry_allowed(opposite)
            assert after_state['pending'] == (account,)
            assert len(feed._states[account]['seen']) == min(frame_size, wake.MAX_SEEN)
            expected_symbols = {'DOGE'}
            if frame_size > 1:
                expected_symbols.add('SOL')
            if frame_size > 65:
                expected_symbols.add('ETH')
            assert set(feed.dirty_symbols(account)) == expected_symbols
        return dict(elapsed_ns=elapsed, diagnostic_counters={key: health.get(key, 0) for key in COUNTERS}), after_state
    finally:
        if recorder is not None:
            assert recorder.stop(timeout=0.25), 'RECORDER_WORKER_LEAK'
        assert timing.set_recorder(None)


def execute(pairs, warmups, seed):
    if pairs < 2 or pairs % 2 or warmups < 0:
        raise ValueError('Use positive even pairs and nonnegative warmups')
    scenarios = [(size, role, False) for size in (1, 64, wake.MAX_BATCH_ROWS)
                 for role in ('long_account', 'short_account')]
    # A 10,000-row replay exceeds the existing bounded 256-identity seen cache;
    # it cannot serve as a duplicate-suppression benchmark.
    scenarios += [(size, role, True) for size in (1, 64)
                  for role in ('long_account', 'short_account')]
    random.Random(seed).shuffle(scenarios)
    records = {scenario: [] for scenario in scenarios}
    parity_pairs = 0
    with deny_network() as network_attempts, \
            patch.object(wake.FillWakeups, 'start', side_effect=AssertionError('FEED_NETWORK_START_FORBIDDEN')), \
            patch.object(wake, '_connector', side_effect=AssertionError('FEED_CONNECTION_FORBIDDEN')):
        for iteration in range(warmups + pairs):
            order = scenarios if iteration % 2 == 0 else list(reversed(scenarios))
            for scenario in order:
                frame_size, role, duplicate = scenario
                account = A if role == 'long_account' else B
                prepared = {mode: prepare(frame_size, account, duplicate) for mode in (False, True)}
                modes = (False, True) if iteration % 2 == 0 else (True, False)
                results = {}
                states = {}
                for enabled in modes:
                    feed, raw, generation = prepared[enabled]
                    results[enabled], states[enabled] = measure(
                        feed, raw, generation, account, enabled, frame_size, duplicate)
                assert states[False] == states[True], 'EXACT_FEED_STATE_PARITY_FAILED'
                if iteration >= warmups:
                    parity_pairs += 1
                    records[scenario].append(dict(pair=iteration - warmups,
                        disabled=results[False], enabled=results[True],
                        added_ns=results[True]['elapsed_ns'] - results[False]['elapsed_ns']))
    output = []
    for scenario in sorted(records):
        frame_size, role, duplicate = scenario
        rows = records[scenario]
        output.append(dict(frame_size=frame_size, account_role=role, duplicate=duplicate,
            disabled_receive_duration_ns=summary([row['disabled']['elapsed_ns'] for row in rows]),
            enabled_receive_duration_ns=summary([row['enabled']['elapsed_ns'] for row in rows]),
            paired_added_duration_ns=summary([row['added_ns'] for row in rows]),
            enabled_diagnostic_counters={key: sum(row['enabled']['diagnostic_counters'][key] for row in rows) for key in COUNTERS},
            exact_feed_state_parity=True, pairs=rows))
    return dict(schema='passive_feed_local_benchmark_v1',
                completed_at_utc=datetime.now(timezone.utc).isoformat(),
                environment=dict(python=sys.version, platform=platform.platform(), cpu_count=os.cpu_count(),
                                 load_average=os.getloadavg(), gc_enabled=gc.isenabled()),
                settings=dict(pairs=pairs, warmup_pairs=warmups, seed=seed,
                              recorder_capacity=256, recorder_batch_size=32),
                exact_feed_state_parity_pairs=parity_pairs,
                network_attempts_blocked=len(network_attempts), results=output,
                caveats=[
                    'Actual FillWakeups._receive elapsed time includes JSON parsing, validation, normal state updates and telemetry hooks.',
                    'Message generation, feed setup, recorder worker startup/drain and assertions are outside timed receives.',
                    'Elapsed timings and Recorder clocks are real; exchange event and decision/readiness timestamps are fixture values.',
                    'Synthetic decision clocks enable exact equality of all resulting feed states and health values.',
                    'The feed is never started; no socket threads, exchange calls, database operations or real trades occur.',
                    'Each condition has only the reported pair count; extreme percentiles/maxima are local observations, not production bounds.',
                    'Active/disabled order alternates and scenario order reverses to reduce ordering bias; CPU scheduling noise remains.',
                    'Negative paired differences reflect noise and do not demonstrate that telemetry accelerates processing.',
                    '10,000-row frames are valid maximum-size stress cases, not a claim about usual exchange traffic.',
                    'At most 64 notification identities are recorded per fresh frame; all validated identities still affect the feed.',
                    'This does not measure downstream trading, exchange event-to-receipt latency, or fill-to-protection time.',
                ])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--pairs', type=int, default=12)
    parser.add_argument('--warmups', type=int, default=2)
    parser.add_argument('--seed', type=int, default=20261006)
    args = parser.parse_args()
    report = execute(args.pairs, args.warmups, args.seed)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    print(json.dumps(dict(output=str(args.output.resolve()), scenarios=len(report['results']),
                         exact_feed_state_parity_pairs=report['exact_feed_state_parity_pairs'],
                         network_attempts_blocked=report['network_attempts_blocked']), sort_keys=True))


if __name__ == '__main__':
    main()
