"""Deterministic lifetime/selection/safety controls; no bot startup or live I/O."""
from datetime import datetime, timezone
import json
from pathlib import Path
import unittest
from unittest.mock import patch

import research_history_memory_followup as m


class Clock:
    def __init__(self): self.value = 0.
    def __call__(self): return self.value


def watch(cycle=None, index=None, active=0, revision=1, complete=True, browser=0):
    return dict(active_cycle_count=active, journal=dict(process_session_id='test-session'),
        post_cleanup_followups=dict(pending_origin_cycle_id=cycle if index != 5 else None,
            activity_revision=revision, last_sample=dict(origin_cycle_id=cycle,
                observation_index=index, sample_completed_at_utc='2026-10-31T22:00:00Z',
                memory=dict(status='COMPLETE' if complete else 'PARTIAL',
                    descendants=dict(browser_named=dict(process_count=browser),
                                     other=dict(process_count=0))))))


class Tests(unittest.TestCase):
    def make(self, current, rss=600_000_000, cache=None):
        clock = Clock(); events = []; calls = []
        def measure_cache():
            calls.append(True)
            return cache() if cache else dict(status='COMPLETE', union_bytes=123)
        obj = m.Investigation(lambda: current[0], duration=3600,
            measure=lambda: dict(rss_bytes=rss, python_counters=dict(allocated_blocks_count=123)),
            cache_measure=measure_cache, clock=clock, sink=events.append,
            allocator_emit=lambda identity: dict(status='TEST_FAKE_NO_EMISSION'),
            admission_measure=lambda: dict(status='COMPLETE',
                self=dict(rss_bytes=rss),
                descendants=dict(browser_named=dict(process_count=0), other=dict(process_count=0)),
                cgroup=dict(current_bytes=800_000_000,max_bytes=2_147_483_648)))
        return obj, clock, events, calls

    def test_first_selection_not_replaced_and_exact_two_probes(self):
        current = [watch('a', 2)]; obj, clock, events, calls = self.make(current)
        self.assertTrue(obj.tick()); self.assertEqual(obj.selected['origin_cycle_id'], 'a')
        current[0] = watch('a', 3); clock.value = 120; obj.tick(); obj.tick()
        self.assertEqual(len(calls), 1)
        current[0] = watch('a', 5); clock.value = 600; obj.tick()
        self.assertEqual(len(calls), 2)
        clock.value = 659; self.assertTrue(obj.tick())
        clock.value = 660; self.assertFalse(obj.tick()); self.assertEqual(obj.state, 'COMPLETED')
        current[0] = watch('b', 3); self.assertFalse(obj.tick()); self.assertEqual(len(calls), 2)
        self.assertEqual([p['payload']['observation_index'] for p in obj.probes], [3, 5])

    def test_missing_index_kept_no_later_replacement(self):
        current = [watch('a', 2)]; obj, clock, events, calls = self.make(current); obj.tick()
        current[0] = watch('a', 4); clock.value = 300; obj.tick()
        self.assertEqual(obj.probes[0]['payload']['status'], 'MISSED_SCHEDULED_TAIL')
        current[0] = watch('b', 2); clock.value = 700; obj.tick()
        self.assertEqual(obj.probes[1]['payload']['status'], 'PREEMPTED_BY_LATER_CYCLE')
        self.assertEqual(calls, [])

    def test_guards_never_walk(self):
        for kwargs, rss, reason in [({'active': 1}, 1, 'SKIPPED_WATCH_ACTIVE'),
                ({}, m.MAX_SELF_RSS_BYTES + 1, 'SKIPPED_SELF_RSS_GUARD'),
                ({'complete': False}, 1, 'SKIPPED_DESCENDANT_COVERAGE_GUARD'),
                ({'browser': 1}, 1, 'SKIPPED_DESCENDANT_COVERAGE_GUARD')]:
            with self.subTest(reason=reason):
                current = [watch('a', 2)]; obj, clock, events, calls = self.make(current, rss=rss); obj.tick()
                current[0] = watch('a', 3, **kwargs); clock.value = 120; obj.tick()
                self.assertEqual(obj.probes[0]['payload']['status'], reason); self.assertEqual(calls, [])

    def test_watch_overlap_rejects_ownership(self):
        current = [watch('a', 2)]
        def mutate(): current[0] = watch('a', 3, revision=2); return dict(status='COMPLETE')
        obj, clock, events, calls = self.make(current, cache=mutate); obj.tick()
        current[0] = watch('a', 3); clock.value = 120; obj.tick()
        self.assertEqual(obj.probes[0]['payload']['status'], 'WATCH_ACTIVITY_CHANGED')

    def test_fresh_cgroup_guard(self):
        current = [watch('a', 2)]; obj, clock, events, calls = self.make(current); obj.tick()
        obj.admission_measure = lambda: dict(status='PARTIAL')
        current[0] = watch('a', 3); clock.value = 120; obj.tick()
        self.assertEqual(obj.probes[0]['payload']['status'], 'SKIPPED_FRESH_PROCESS_OR_CGROUP_GUARD')
        self.assertEqual(calls, [])

    def test_actual_process_api_reaches_cache_with_fake_procfs(self):
        current = [watch('a', 2)]; obj, clock, events, calls = self.make(current); obj.tick()
        smaps = b'Rss: 600000 kB\nPss: 590000 kB\nShared_Clean: 10000 kB\nShared_Dirty: 0 kB\nPrivate_Clean: 0 kB\nPrivate_Dirty: 590000 kB\nAnonymous: 590000 kB\n'
        def fake_read(path, cap): return b'VmRSS: 600000 kB\n' if path.endswith('/status') else smaps
        obj.measure = lambda: m.research_process_memory.sample(platform_name='linux',
            read_file=fake_read, getallocatedblocks=lambda: 123, getgccounts=lambda: (1, 2, 3))
        current[0] = watch('a', 3); clock.value = 120; obj.tick()
        self.assertEqual(obj.probes[0]['payload']['status'], 'MEASURED')
        self.assertEqual(obj.probes[0]['payload']['process_before']['rss_bytes'], 600000 * 1024)
        self.assertEqual(len(calls), 1)

    def test_watch_start_during_admission_prevents_walk(self):
        current = [watch('a', 2)]; obj, clock, events, calls = self.make(current); obj.tick()
        def admission():
            current[0] = watch('a', 3, active=1, revision=2)
            return dict(status='COMPLETE', self=dict(rss_bytes=1),
                descendants=dict(browser_named=dict(process_count=0), other=dict(process_count=0)),
                cgroup=dict(current_bytes=1, max_bytes=2_147_483_648))
        obj.admission_measure = admission
        current[0] = watch('a', 3); clock.value = 120; obj.tick()
        self.assertEqual(obj.probes[0]['payload']['status'], 'SKIPPED_FRESH_PROCESS_OR_CGROUP_GUARD')
        self.assertEqual(calls, [])

    def test_fresh_high_rss_prevents_walk(self):
        current = [watch('a', 2)]; obj, clock, events, calls = self.make(current); obj.tick()
        old = obj.admission_measure
        def admission():
            sample = old(); sample['self']['rss_bytes'] = m.MAX_SELF_RSS_BYTES + 1; return sample
        obj.admission_measure = admission
        current[0] = watch('a', 3); clock.value = 120; obj.tick()
        self.assertEqual(obj.probes[0]['payload']['status'], 'SKIPPED_FRESH_PROCESS_OR_CGROUP_GUARD')
        self.assertEqual(calls, [])

    def test_failed_cache_probe_not_repeated(self):
        current = [watch('a', 2)]
        def fail(): raise RuntimeError('sensitive exception never recorded')
        obj, clock, events, calls = self.make(current, cache=fail); obj.tick()
        current[0] = watch('a', 3); clock.value = 120; obj.tick(); obj.tick()
        self.assertEqual(len(calls), 1)
        self.assertNotIn('sensitive', json.dumps(events))
        self.assertEqual(obj.probes[0]['payload']['cache']['reason'], 'CACHE_PROBE_ERROR')

    def test_expiry_no_rearm(self):
        current = [watch('a', 3)]; obj, clock, events, calls = self.make(current)
        clock.value = 3600; self.assertFalse(obj.tick()); self.assertEqual(calls, [])
        self.assertEqual(m.remaining_seconds(datetime(2026, 11, 1, 10, 30, tzinfo=timezone.utc)), 0)
        self.assertEqual(m.remaining_seconds(datetime(2026, 11, 2, tzinfo=timezone.utc)), 0)

    def test_health_cached_only_and_detached(self):
        current = [watch('a', 2)]; obj, clock, events, calls = self.make(current); obj.tick()
        with patch.object(obj, 'measure', side_effect=AssertionError('health probe')):
            snap = obj.status(); snap['pulses'].clear()
            self.assertEqual(obj.status()['pulse_count'], 1)
        self.assertEqual(calls, [])

    def test_no_probe_refs_or_values_in_health(self):
        current = [watch('a', 2)]; obj, clock, events, calls = self.make(current); obj.tick()
        self.assertNotIn('watch_status', obj.status()); self.assertNotIn('cache_measure', obj.status())

    def test_pulse_limit_stops(self):
        current = [watch()]; obj, clock, events, calls = self.make(current)
        obj.deadline = 100000
        for i in range(m.MAX_PULSES): clock.value = i * 15; self.assertTrue(obj.tick())
        clock.value += 15; self.assertFalse(obj.tick()); self.assertEqual(obj.state, 'PULSE_LIMIT')

    def test_main_additive_and_old_expiry_preserved(self):
        root = Path(__file__).resolve().parent
        text = (root / 'main.py').read_text(encoding='utf-8')
        import ast
        tree = ast.parse(text)
        calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
                 and isinstance(node.func, ast.Attribute)
                 and isinstance(node.func.value, ast.Name)
                 and node.func.value.id == 'research_history_memory_followup'
                 and node.func.attr == 'install']
        self.assertEqual(calls, [])
        self.assertIn('"named_cache_memory_followup": research_history_memory_followup.status()', text)
        self.assertIn("EXPIRES_AT_UTC = '2026-10-31T17:15:00+00:00'",
                      (root / 'research_background_memory.py').read_text(encoding='utf-8'))
        self.assertIn("EXPIRES_AT_UTC = '2026-10-31T22:45:00+00:00'",
                      (root / 'research_memory_investigation.py').read_text(encoding='utf-8'))

    def test_pymalloc_guard_never_calls_arbitrary_function(self):
        calls = []
        result = m.emit_pymalloc_stats({}, implementation='cpython', version=(3, 11, 9),
            debug_build=False, platform_name='linux', function=lambda: calls.append(True), write=lambda *args: calls.append(True))
        self.assertEqual(result['status'], 'UNAVAILABLE'); self.assertEqual(calls, [])

    def test_pymalloc_two_same_stderr_markers_without_live_call(self):
        import sys
        writes = []
        # An injected builtin has the same safe dispatch shape; getallocatedblocks
        # emits nothing and does not call the live debug allocator API.
        def fake_write(fd, raw): writes.append((fd, raw)); return len(raw)
        result = m.emit_pymalloc_stats(dict(session='fake'), implementation='cpython',
            version=(3, 11, 9), debug_build=False, platform_name='linux', function=sys.getallocatedblocks, write=fake_write)
        self.assertEqual(result['status'], 'EMITTED_NOT_YET_INGESTED')
        self.assertEqual([part[0] for part in writes], [2, 2])
        self.assertIn(b'"phase":"BEGIN"', writes[0][1]); self.assertIn(b'"phase":"END"', writes[1][1])

    def test_pymalloc_precedes_cache_accounting(self):
        current = [watch('a', 2)]; obj, clock, events, calls = self.make(current); obj.tick()
        order = []
        obj.allocator_emit = lambda identity: order.append('allocator') or dict(status='TEST_FAKE')
        obj.cache_measure = lambda: order.append('cache') or dict(status='COMPLETE')
        current[0] = watch('a', 3); clock.value = 120; obj.tick()
        self.assertEqual(order, ['allocator', 'cache'])


if __name__ == '__main__': unittest.main()
