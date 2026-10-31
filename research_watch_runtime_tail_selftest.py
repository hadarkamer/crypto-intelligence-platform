"""Adversarial cleanup-tail controls using fake clocks, memory and persistence."""
import asyncio
from copy import deepcopy
import importlib.util
import itertools
import json
from pathlib import Path
import sys
from threading import Event, current_thread
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import research_runtime_memory as memory
import research_runtime_memory_observations as observations

CYCLE = 'shared-watch:2026-10-31T14:32:15+00:00'
NEXT = 'shared-watch:2026-10-31T15:02:15+00:00'
STAMP = '2026-10-31T14:38:00+00:00'
SESSION, COMMIT = 'a' * 32, 'b' * 40
COUNTER = itertools.count()


class Journal:
    def __init__(self): self.events = []
    def begin(self, cycle_id, **kwargs):
        h = SimpleNamespace(process_session_id=SESSION, _runtime_commit=COMMIT, closed=False)
        self.events.append(('START', cycle_id)); return h
    def stage(self, name, *, handle, **kwargs):
        if handle.closed: raise AssertionError('CLOSED_WATCH_JOURNAL')
        self.events.append((name, kwargs)); return {'status': 'QUEUED', 'durable': False}
    def finish(self, handle, **kwargs):
        handle.closed = True; self.events.append(('FINISH', kwargs))
        return {'status': 'QUEUED', 'durable': False}
    def reset(self, handle): pass
    def status(self): return {'status': 'FAKE'}


class Followups:
    def __init__(self): self.markers, self.samples = [], []; self.marker_error = self.sample_error = None
    def record_marker(self, **kwargs):
        if self.marker_error: raise self.marker_error
        self.markers.append(deepcopy(kwargs)); return {'status': 'QUEUED', 'durable': False}
    def record_sample(self, **kwargs):
        if self.sample_error: raise self.sample_error
        self.samples.append(deepcopy(kwargs)); return {'status': 'QUEUED', 'durable': False}
    def status(self): return {'status': 'FAKE', 'durable': False}


class TailTests(unittest.TestCase):
    def setUp(self):
        self.name = '_tail_observer_fixture_' + str(next(COUNTER))
        spec = importlib.util.spec_from_file_location(self.name, ROOT / 'research_watch_runtime.py')
        self.m = importlib.util.module_from_spec(spec); sys.modules[self.name] = self.m
        spec.loader.exec_module(self.m)
        self.clock = 0.; self.journal, self.followups = Journal(), Followups()
        self.real_monitor_start, self.real_now = self.m._ensure_monitor_locked, self.m._now
        self.pins = {name: 'c' * 64 for name in ('main.py', 'research_runtime_memory.py', 'research_runtime_journal.py', 'research_watch_runtime.py', 'research_runtime_memory_observations.py')}
        self.patches = [patch.object(self.m, 'journal', self.journal), patch.object(self.m, 'followups', self.followups),
                        patch.object(self.m, '_ensure_monitor_locked', lambda: None),
                        patch.object(self.m, '_now', lambda: self.clock), patch.object(self.m, '_utc', lambda: STAMP),
                        patch.object(self.m, '_code_pins', lambda: self.pins)]
        for p in self.patches: p.start()

    def tearDown(self):
        for handle in list(self.m._ACTIVE.values()): self.m.finish(handle)
        self.m._shutdown_for_tests()
        for p in reversed(self.patches): p.stop()
        sys.modules.pop(self.name, None)

    def tail(self, cycle=CYCLE):
        h = self.m.begin(cycle); self.assertIsNotNone(h)
        self.m.finish(h, cycle_status='completed'); self.m.cleanup_tail(h)
        return h

    def due(self, at):
        self.clock = at
        with self.m._CONDITION: return self.m._take_tail_due_locked(at)

    def probe(self, at=30., sample=None):
        item = self.due(at); self.assertIsNotNone(item)
        with patch.object(self.m.memory, 'sample_memory', return_value=deepcopy(sample if sample is not None else memory._empty())):
            return self.m._probe_tail(item)

    def test_tail_requires_closed_handle_and_is_idempotent(self):
        self.assertIsNone(self.m.cleanup_tail(None))
        h = self.m.begin(CYCLE); self.assertIsNone(self.m.cleanup_tail(h))
        self.assertEqual(self.followups.markers, [])
        self.m.finish(h, cycle_status='completed'); self.m.cleanup_tail(h); self.m.cleanup_tail(h)
        self.assertEqual(len(self.followups.markers), 1)
        self.assertEqual(sum(kind == 'FINISH' for kind, _ in self.journal.events), 1)
        self.assertFalse(any(kind == 'PERIODIC_MEMORY' for kind, _ in self.journal.events))

    def test_four_fixed_due_offsets_and_metadata_only_plan(self):
        h = self.tail(); plan = self.m._TAIL_PLAN
        self.assertEqual(set(vars(plan)), {'identity', 'anchor', 'activity_revision', 'next_index'})
        self.assertEqual(set(plan.identity), {'process_session_id', 'origin_cycle_id', 'cleanup_tail_at_utc', 'runtime_git_commit', 'source_code_sha256'})
        self.assertNotIn(h, plan.identity.values()); self.assertNotIn(h.journal_handle, plan.identity.values())
        self.pins['main.py'] = 'd' * 64
        self.assertEqual(plan.identity['source_code_sha256']['main.py'], 'c' * 64)
        for index, delay in ((2, 30), (3, 120), (4, 300), (5, 600)):
            self.assertIsNone(self.due(delay - .001)); self.probe(float(delay))
            row = self.followups.samples[-1]
            self.assertEqual((row['observation_index'], row['scheduled_delay_seconds'], row['observed_delay_ms']), (index, delay, delay * 1000.))
        self.assertIsNone(self.m._TAIL_PLAN); self.assertIsNone(self.due(10000.))
        self.assertEqual(len(self.followups.samples), 4)

    def test_native_tail_only_enqueues_without_probing(self):
        with patch.object(self.m.memory, 'sample_memory', side_effect=AssertionError('CALLER_PROCFS')):
            h = self.tail()
        self.assertTrue(h.cleanup_tail_seen); self.assertEqual(len(self.followups.markers), 1)
        self.assertEqual(self.followups.samples, []); self.assertIsNone(self.m._MONITOR)

    def test_invalid_begin_preempts_tail_even_without_handle(self):
        self.tail(); revision = self.m._ACTIVITY_REVISION
        self.assertIsNone(self.m.begin(None)); self.assertIsNone(self.m._TAIL_PLAN)
        self.assertGreater(self.m._ACTIVITY_REVISION, revision)

    def test_refused_journal_begin_still_preempts_tail(self):
        self.tail()
        with patch.object(self.journal, 'begin', return_value=None): self.assertIsNone(self.m.begin(NEXT))
        self.assertIsNone(self.m._TAIL_PLAN); self.assertEqual(self.m._METRICS['tail_preemptions'], 1)

    def test_older_tail_cannot_replace_newer_cycle_plan(self):
        old = self.m.begin(CYCLE); self.m.finish(old)
        self.tail(NEXT); newer = self.m._TAIL_PLAN
        self.m.cleanup_tail(old)
        self.assertIs(self.m._TAIL_PLAN, newer)
        self.assertEqual(newer.identity['origin_cycle_id'], NEXT)
        self.assertEqual(len(self.followups.markers), 2)
        self.assertEqual(self.m._METRICS['tail_plan_refusals'], 1)

    def test_inflight_begin_finish_changes_revision_even_counts_return_zero(self):
        self.tail(); item = self.due(30.)
        def during_probe():
            h = self.m.begin(NEXT); self.m.finish(h)
            return memory._empty()
        with patch.object(self.m.memory, 'sample_memory', side_effect=during_probe): self.m._probe_tail(item)
        row = self.followups.samples[-1]
        self.assertEqual((row['watch_active_before'], row['watch_active_after']), (0, 0))
        self.assertGreater(row['watch_activity_revision_after'], row['watch_activity_revision_before'])
        self.assertEqual(observations._activity(row['watch_active_before'], row['watch_active_after'], row['watch_activity_revision_before'], row['watch_activity_revision_after']), 'WATCH_ACTIVITY_OBSERVED_OR_CHANGED')

    def test_revision_backup_preempts_stale_pending_plan(self):
        self.tail(); self.m._ACTIVITY_REVISION += 1
        self.assertIsNone(self.due(30.)); self.assertIsNone(self.m._TAIL_PLAN)

    def preserve_original_error(self, error):
        h = self.m.begin(CYCLE); self.followups.marker_error = SystemExit('private marker fault')
        async def native():
            try: raise error
            finally:
                self.m.finish(h, cycle_status='failed'); self.m.cleanup_tail(h)
        with self.assertRaises(type(error)) as caught: asyncio.run(native())
        self.assertIs(caught.exception, error)

    def test_marker_failure_preserves_original_runtimeerror(self):
        self.preserve_original_error(RuntimeError('native fixture'))

    def test_marker_failure_preserves_original_cancellation(self):
        self.preserve_original_error(asyncio.CancelledError('native cancellation'))

    def test_tail_utc_clock_failure_is_failopen_explicit_gap(self):
        h = self.m.begin(CYCLE); self.m.finish(h)
        with patch.object(self.m, '_utc', side_effect=SystemExit('private clock')):
            self.assertIsNone(self.m.cleanup_tail(h))
        self.assertEqual(self.followups.markers, []); self.assertIsNone(self.m._TAIL_PLAN)
        self.assertNotIn('private clock', json.dumps(self.m.status()))

    def test_tail_nonfinite_monotonic_clock_is_failopen(self):
        h = self.m.begin(CYCLE); self.m.finish(h)
        with patch.object(self.m, '_now', self.real_now), patch.object(self.m.time, 'monotonic', return_value=float('nan')):
            self.assertIsNone(self.m.cleanup_tail(h))
        self.assertEqual(self.followups.markers, []); self.assertIsNone(self.m._TAIL_PLAN)

    def test_invalid_sampler_becomes_valid_unavailable_record(self):
        self.tail(); self.probe(sample={'invalid': True})
        sample = self.followups.samples[-1]['memory']
        self.assertTrue(memory.validate_sample(sample)); self.assertEqual(sample['status'], 'UNAVAILABLE')
        self.assertEqual(sample['errors'], {'SAMPLER_ERROR': 1})

    def test_sampler_baseexception_becomes_valid_unavailable_record(self):
        self.tail(); item = self.due(30.)
        with patch.object(self.m.memory, 'sample_memory', side_effect=KeyboardInterrupt('private sampler')): self.m._probe_tail(item)
        sample = self.followups.samples[-1]['memory']
        self.assertTrue(memory.validate_sample(sample)); self.assertEqual(sample['status'], 'UNAVAILABLE')
        self.assertNotIn('private sampler', json.dumps(self.m.status()))

    def test_persistence_boundary_fault_stays_diagnostic(self):
        self.tail(); self.followups.sample_error = SystemExit('private persistence')
        self.assertIsNone(self.probe()); self.assertEqual(self.followups.samples, [])
        self.assertNotIn('private persistence', json.dumps(self.m.status()))

    def test_status_is_cached_and_defensively_copied(self):
        self.tail(); self.probe()
        with patch.object(self.m, '_code_pins', side_effect=AssertionError('SOURCE_READ')), patch.object(self.m.memory, 'sample_memory', side_effect=AssertionError('PROC_READ')):
            state = self.m.status()
        state['post_cleanup_followups']['last_sample']['memory']['errors']['MUTATED'] = 1
        state['post_cleanup_followups']['remaining_indices'].clear()
        again = self.m.status()['post_cleanup_followups']
        self.assertNotIn('MUTATED', again['last_sample']['memory']['errors'])
        self.assertEqual(again['remaining_indices'], [3, 4, 5])

    def test_same_singleton_monitor_services_watch_and_tail(self):
        sampled = Event(); threads = []
        def fake_sample():
            threads.append(current_thread()); sampled.set(); return memory._empty()
        with patch.object(self.m, '_ensure_monitor_locked', self.real_monitor_start), patch.object(self.m.memory, 'sample_memory', side_effect=fake_sample):
            h = self.m.begin(CYCLE); self.assertTrue(sampled.wait(3)); monitor = self.m._MONITOR
            self.m.finish(h); self.m.cleanup_tail(h); sampled.clear(); self.clock = 30.
            with self.m._CONDITION: self.m._CONDITION.notify_all()
            self.assertTrue(sampled.wait(3)); self.assertIs(self.m._MONITOR, monitor)
            self.assertGreaterEqual(len(threads), 2); self.assertTrue(all(t is monitor for t in threads))
            self.assertIsNot(monitor, current_thread())


if __name__ == '__main__': unittest.main()
