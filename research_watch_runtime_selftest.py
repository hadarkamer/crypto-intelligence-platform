"""Finite passive-observer tests; no market, network or database access."""
from contextvars import ContextVar
import asyncio
import importlib.util
import json
from pathlib import Path
import sys
from threading import Event, Thread, current_thread
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import research_runtime_memory as memory
import research_watch_runtime as observer

CYCLE = 'shared-watch:2026-10-31T12:00:00+00:00'


class Journal:
    def __init__(self):
        self.events = []
        self.current = ContextVar('fake_runtime_journal', default=None)
        self.begin_error = self.stage_error = self.finish_error = None

    def begin(self, cycle_id, **kwargs):
        if self.begin_error: raise self.begin_error
        handle = SimpleNamespace(cycle_id=cycle_id, closed=False, token=None, previous=self.current.get())
        handle.token = self.current.set(handle)
        self.events.append(dict(kind='START', handle=handle, kwargs=kwargs))
        return handle

    def stage(self, name, *, handle, **kwargs):
        if self.stage_error: raise self.stage_error
        if handle.closed: raise AssertionError('LATE_JOURNAL_STAGE')
        self.events.append(dict(kind=name, handle=handle, kwargs=kwargs))
        return {'status':'QUEUED','durable':False}

    def reset(self, handle):
        if self.current.get() is not handle:return
        if handle.token is not None:
            self.current.reset(handle.token);handle.token=None
        else:self.current.set(handle.previous)
        for _ in range(64):
            active=self.current.get()
            if active is None or not active.closed:return
            if active.token is not None:
                self.current.reset(active.token);active.token=None
            else:self.current.set(active.previous)

    def finish(self, handle, **kwargs):
        if self.finish_error: raise self.finish_error
        handle.closed = True
        self.events.append(dict(kind='FINISH', handle=handle, kwargs=kwargs))
        self.reset(handle)
        return {'status':'QUEUED','durable':False}

    def status(self): return {'status':'FAKE','durable':False}


class RuntimeObserverTests(unittest.TestCase):
    def setUp(self):
        observer._shutdown_for_tests()
        with observer._CONDITION:
            observer._ACTIVE.clear()
            observer._MONITOR = None
            observer._LAST_SAMPLE = None
            observer._STOP.clear()
            for name in observer._METRICS:
                observer._METRICS[name] = None if name == 'last_error_type' else 0
        observer._CURRENT.set(None)
        self.journal = Journal()
        self.patches = [patch.object(observer,'journal',self.journal),
            patch.object(observer,'_ensure_monitor_locked',lambda:None),
            patch.object(observer,'_code_pins',lambda:{'main.py':'a'*64})]
        for item in self.patches: item.start()
        self.sample = memory._empty()

    def tearDown(self):
        for handle in list(observer._ACTIVE.values()): observer.finish(handle)
        observer._shutdown_for_tests()
        for item in reversed(self.patches): item.stop()

    def due(self, at=0.):
        with observer._CONDITION: return observer._take_due_locked(at)

    def test_start_is_requested_before_any_memory_probe(self):
        with patch.object(observer.memory,'sample_memory',side_effect=AssertionError('ON_NATIVE_THREAD')):
            handle = observer.begin(CYCLE)
            observer.stage(handle,'INPUTS_READY')
        self.assertEqual([event['kind'] for event in self.journal.events],['START','COLLECTING_INPUTS','INPUTS_READY'])
        self.assertEqual(self.journal.events[0]['kwargs']['source_code_sha256'],{'main.py':'a'*64})
        self.assertIs(observer._CURRENT.get(),handle)

    def test_coalesced_probe_keeps_latest_marker_and_own_clocks(self):
        handle=observer.begin(CYCLE)
        observer.stage(handle,'SCORING_READY'); observer.stage(handle,'INPUTS_READY')
        with patch.object(observer.memory,'sample_memory',return_value=self.sample): observer._probe(self.due())
        probes=[event for event in self.journal.events if event['kind']=='PERIODIC_MEMORY']
        self.assertEqual(len(probes),1)
        data=probes[0]['kwargs']
        self.assertEqual((data['phase'],data['phase_revision']),('INPUTS_READY',3))
        self.assertLessEqual(data['phase_observed_at_utc'],data['sample_started_at_utc'])
        self.assertLessEqual(data['sample_started_at_utc'],data['sample_completed_at_utc'])
        self.assertEqual(data['memory'],self.sample)
        self.assertIsNone(self.due(29.99))
        self.assertIsNotNone(self.due(30.))

    def test_total_probe_bound_includes_stage_wakes(self):
        handle=observer.begin(CYCLE)
        for number in range(observer.MAX_SAMPLES_PER_CYCLE):
            observer.stage(handle,'INPUTS_READY')
            self.assertIsNotNone(self.due(float(number)))
        observer.stage(handle,'BEFORE_DELIVERY')
        self.assertIsNone(self.due(100000.))
        self.assertEqual(handle.probes,24)
        observer.finish(handle,cycle_status='completed')
        self.assertEqual(self.journal.events[-1]['kind'],'FINISH')

    def test_registry_capacity_and_duplicate_begin_leave_current_context(self):
        first=observer.begin(CYCLE)
        self.assertIsNone(observer.begin(CYCLE))
        second=observer.begin(CYCLE.replace('12:00','12:01'))
        self.assertIsNone(observer.begin(CYCLE.replace('12:00','12:02')))
        self.assertIs(observer._CURRENT.get(),second)
        self.assertEqual(len(observer._ACTIVE),2)
        observer.finish(second)
        self.assertIs(observer._CURRENT.get(),first)
        observer.finish(first)
        self.assertIsNone(observer._CURRENT.get())
        self.assertIsNone(self.journal.current.get())

    def test_finish_is_idempotent_and_blocks_later_markers(self):
        handle=observer.begin(CYCLE)
        observer.finish(handle,cycle_status='cancelled')
        observer.stage(handle,'INPUTS_READY'); observer.finish(handle)
        self.assertEqual(sum(event['kind']=='FINISH' for event in self.journal.events),1)
        self.assertEqual(self.journal.events[-1]['kwargs']['status'],'CANCELLED')
        self.assertEqual(len(observer._ACTIVE),0)

    def test_out_of_order_finish_preserves_inner_and_unwinds_closed_outer(self):
        outer=observer.begin(CYCLE)
        inner=observer.begin(CYCLE.replace('12:00','12:01'))
        observer.finish(outer,cycle_status='failed')
        self.assertIs(observer._CURRENT.get(),inner)
        self.assertIs(self.journal.current.get(),inner.journal_handle)
        observer.stage(inner,'INPUTS_READY')
        observer.finish(inner,cycle_status='completed')
        self.assertIsNone(observer._CURRENT.get())
        self.assertIsNone(self.journal.current.get())
        self.assertEqual(observer._ACTIVE,{})

    def test_exact_known_completion_variants_are_preserved(self):
        for native in ('completed_with_delivery_errors','completed_with_transition_gaps'):
            handle=observer.begin(CYCLE)
            observer.finish(handle,cycle_status=native)
            self.assertEqual(self.journal.events[-1]['kwargs']['status'],native.upper())

    def test_late_probe_after_finish_is_discarded(self):
        handle=observer.begin(CYCLE); observation=self.due()
        entered,released=Event(),Event()
        def sample():
            entered.set(); self.assertTrue(released.wait(3)); return self.sample
        with patch.object(observer.memory,'sample_memory',side_effect=sample):
            worker=Thread(target=observer._probe,args=(observation,)); worker.start()
            self.assertTrue(entered.wait(3))
            observer.finish(handle,cycle_status='completed')
            released.set(); worker.join(3); self.assertFalse(worker.is_alive())
        self.assertFalse(any(event['kind']=='PERIODIC_MEMORY' for event in self.journal.events))
        self.assertEqual(observer._METRICS['late_probe_drops'],1)

    def test_phase_change_during_probe_retains_observed_revision(self):
        handle=observer.begin(CYCLE); observation=self.due()
        def sample():
            observer.stage(handle,'INPUTS_READY'); return self.sample
        with patch.object(observer.memory,'sample_memory',side_effect=sample): observer._probe(observation)
        probe=self.journal.events[-1]
        self.assertEqual(probe['kind'],'PERIODIC_MEMORY')
        self.assertEqual((probe['kwargs']['phase'],probe['kwargs']['phase_revision']),('COLLECTING_INPUTS',1))
        self.assertEqual((handle.phase,handle.phase_revision),('INPUTS_READY',2))

    def test_sampler_baseexception_does_not_escape_or_store_error_text(self):
        observer.begin(CYCLE)
        with patch.object(observer.memory,'sample_memory',side_effect=SystemExit('secret fixture text')):
            observer._probe(self.due())
        probe=self.journal.events[-1]
        self.assertEqual(probe['kwargs']['reason_class'],'SystemExit')
        self.assertIsNone(probe['kwargs']['memory'])
        self.assertNotIn('secret',json.dumps(observer.status()))

    def test_invalid_sample_never_enters_health_cache(self):
        observer.begin(CYCLE)
        with patch.object(observer.memory,'sample_memory',return_value={'cmdline':'secret'}): observer._probe(self.due())
        self.assertIsNone(observer.status()['last_sample']['memory'])
        self.assertEqual(observer._METRICS['probe_errors'],1)
        self.assertNotIn('cmdline',json.dumps(observer.status()))

    def test_journal_begin_baseexception_resets_no_native_context(self):
        self.journal.begin_error=KeyboardInterrupt('private')
        self.assertIsNone(observer.begin(CYCLE))
        self.assertEqual(observer._ACTIVE,{})
        self.assertIsNone(observer._CURRENT.get())

    def test_context_initialization_mutation_then_failure_restores_previous(self):
        real=observer._CURRENT
        failed=[False]
        def setter(value):
            token=real.set(value)
            if isinstance(value,observer._Handle) and not failed[0]:
                failed[0]=True
                raise KeyboardInterrupt('diagnostic after mutation')
            return token
        with patch.object(observer,'_CURRENT',SimpleNamespace(get=real.get,set=setter,reset=real.reset)):
            self.assertIsNone(observer.begin(CYCLE))
        self.assertIsNone(real.get())
        self.assertIsNone(self.journal.current.get())
        self.assertEqual(observer._ACTIVE,{})

    def test_context_reset_fault_cannot_escape_finally(self):
        handle=observer.begin(CYCLE)
        def broken(*args):raise SystemExit('diagnostic token failure')
        fake=SimpleNamespace(get=lambda:handle,set=broken,reset=broken)
        with patch.object(observer,'_CURRENT',fake):
            observer.finish(handle,cycle_status='failed')
        observer._safe_reset(handle)
        self.assertIsNone(observer._CURRENT.get())
        self.assertGreater(observer._METRICS['context_reset_errors'],0)

    def test_context_get_fault_preserves_native_exception_identity(self):
        for native in (RuntimeError('native operation'),asyncio.CancelledError('native cancellation')):
            handle=observer.begin(CYCLE)
            def broken(*args):raise KeyboardInterrupt('diagnostic get')
            fake=SimpleNamespace(get=broken,set=broken,reset=broken)
            caught=None
            try:
                try:raise native
                finally:
                    with patch.object(observer,'_CURRENT',fake):
                        observer.finish(handle,cycle_status='cancelled')
            except BaseException as error:caught=error
            observer._safe_reset(handle)
            self.assertIs(caught,native)
            self.assertIsNone(observer._CURRENT.get())

    def test_initial_marker_failure_preserves_start_handle_for_finally(self):
        self.journal.stage_error=SystemExit('diagnostic')
        handle=observer.begin(CYCLE)
        self.assertIsNotNone(handle)
        observer.finish(handle,cycle_status='failed')
        self.assertEqual([event['kind'] for event in self.journal.events],['START','FINISH'])
        self.assertIsNone(observer._CURRENT.get())

    def test_stage_and_finish_failures_keep_finally_context_cleanup(self):
        handle=observer.begin(CYCLE)
        self.journal.stage_error=KeyboardInterrupt('diagnostic')
        self.assertIsNone(observer.stage(handle,'INPUTS_READY'))
        self.journal.finish_error=SystemExit('diagnostic')
        self.assertIsNone(observer.finish(handle,cycle_status='failed'))
        self.assertIsNone(observer._CURRENT.get())
        self.assertIsNone(self.journal.current.get())
        self.assertEqual(observer._ACTIVE,{})

    def test_marker_health_and_finish_do_not_probe_procfs(self):
        with patch.object(observer.memory,'sample_memory',side_effect=AssertionError('PROCFS_IN_CALLER')):
            handle=observer.begin(CYCLE); observer.stage(handle,'INPUTS_READY')
            result=observer.status(); observer.finish(handle)
        self.assertFalse(result['memory_probes_on_native_thread'])
        self.assertIn('NOT_DURABLE_ROWS',result['counter_scope'])
        self.assertIn('NOT_COROUTINE_RETURN',result['finish_scope'])

    def test_invalid_clock_is_rejected_without_probe_reservation(self):
        observer.begin(CYCLE)
        with patch.object(observer.time,'monotonic',return_value=float('nan')):
            with self.assertRaisesRegex(ValueError,'INVALID_DIAGNOSTIC_CLOCK'): observer._now()
        self.assertEqual(observer._METRICS['probe_attempts'],0)

    def test_single_real_monitor_thread_samples_off_caller_thread(self):
        # Enable only the actual bounded monitor; journal remains a pure fake.
        self.patches[1].stop()
        self.patches[1]=patch.object(observer,'INTERVAL_SECONDS',30.)
        self.patches[1].start()
        observed=Event(); worker_names=[]
        def sample():
            worker_names.append(current_thread().name); observed.set(); return self.sample
        with patch.object(observer.memory,'sample_memory',side_effect=sample):
            handle=observer.begin(CYCLE)
            self.assertTrue(observed.wait(3))
            first=observer._MONITOR
            observer.stage(handle,'INPUTS_READY')
            self.assertIs(observer._MONITOR,first)
            observer.finish(handle,cycle_status='completed')
        observer._shutdown_for_tests()
        self.assertTrue(worker_names)
        self.assertEqual(set(worker_names),{'watch-runtime-memory'})
        self.assertFalse(first.is_alive())


if __name__=='__main__':unittest.main(verbosity=2)
