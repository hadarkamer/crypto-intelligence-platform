"""Deterministic lifetime/selection/safety controls; no bot startup or live I/O."""
from datetime import datetime, timezone
import json
from pathlib import Path
import unittest
from unittest.mock import patch

import research_current_history_memory_investigation as m


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


@patch.object(m, 'PROBE_INDICES', (3, 5))
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
                ({'browser': 1}, 1, 'SKIPPED_FRESH_PROCESS_OR_CGROUP_GUARD')]:
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
        self.assertEqual(m.remaining_seconds(datetime(2026, 11, 1, 17, 0, tzinfo=timezone.utc)), 0)
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

    def test_source_scope_and_old_expiry_preserved(self):
        root = Path(__file__).resolve().parent
        if not (root / 'research_background_memory.py').is_file():
            root = root.parent / 'source_repo'
        self.assertEqual(m.SOURCE_FILES, ('main.py', 'research_current_history_memory_investigation.py', 'research_current_history_memory.py', 'research_process_memory.py'))
        self.assertIn("EXPIRES_AT_UTC = '2026-10-31T17:15:00+00:00'", (root / 'research_background_memory.py').read_text(encoding='utf-8'))
        self.assertIn("EXPIRES_AT_UTC = '2026-10-31T22:45:00+00:00'", (root / 'research_memory_investigation.py').read_text(encoding='utf-8'))

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


    def attempt(self, samples, *, cached_complete=False, mutate=None):
        current=[watch('a',2)];obj,clock,events,calls=self.make(current);obj.tick()
        sample_calls=[]
        def admission():
            index=len(sample_calls);sample_calls.append(index)
            if mutate:mutate(index,current,clock)
            value=samples[min(index,len(samples)-1)]
            if isinstance(value,Exception):raise value
            return value
        obj.admission_measure=admission
        current[0]=watch('a',3,complete=cached_complete);clock.value=120;obj.tick()
        return obj,clock,current,calls,sample_calls

    @staticmethod
    def safe_sample():
        return dict(status='COMPLETE',self=dict(rss_bytes=600000000),
          descendants=dict(browser_named=dict(process_count=0),other=dict(process_count=0)),
          cgroup=dict(current_bytes=800000000,max_bytes=2147483648),errors={})

    @classmethod
    def partial_sample(cls):
        value=cls.safe_sample();value['status']='PARTIAL';value['errors']={'TIME_BUDGET':1,'ANCESTOR_UNVERIFIED':1};return value

    def test_partial_cached_context_fresh_complete_admits(self):
        obj,_,_,calls,samples=self.attempt([self.safe_sample()])
        p=obj.probes[0]['payload'];self.assertEqual(p['status'],'MEASURED');self.assertEqual(len(calls),1)
        self.assertEqual(p['admission_result']['cached_tail_memory']['status'],'PARTIAL')
        self.assertEqual(p['admission_result']['selected_sample_index'],1);self.assertEqual(len(samples),1)

    def test_transient_then_complete_preserves_both(self):
        obj,_,_,calls,samples=self.attempt([self.partial_sample(),self.safe_sample()])
        r=obj.probes[0]['payload']['admission_result'];self.assertEqual(r['status'],'ADMITTED');self.assertEqual(len(calls),1)
        self.assertEqual([x['decision'] for x in r['samples']],['RETRY_TRANSIENT_COVERAGE','ADMIT'])
        self.assertEqual(r['selected_sample_index'],2);self.assertEqual(len(samples),2)

    def test_three_transient_samples_exhaust_without_graph(self):
        obj,_,_,calls,samples=self.attempt([self.partial_sample()])
        r=obj.probes[0]['payload']['admission_result'];self.assertEqual(r['reason'],'SAMPLE_LIMIT')
        self.assertEqual(len(samples),3);self.assertEqual(calls,[]);self.assertIsNone(r['selected_sample_index'])

    def test_known_unsafe_partial_is_terminal(self):
        for kind in ('child','rss','group'):
            with self.subTest(kind=kind):
                bad=self.partial_sample()
                if kind=='child':bad['descendants']['other']['process_count']=1
                if kind=='rss':bad['self']['rss_bytes']=m.MAX_SELF_RSS_BYTES+1
                if kind=='group':bad['cgroup']['current_bytes']=bad['cgroup']['max_bytes']
                obj,_,_,calls,samples=self.attempt([bad,self.safe_sample()]);self.assertEqual(len(samples),1);self.assertEqual(calls,[])
                self.assertNotEqual(obj.probes[0]['payload']['admission_result']['reason'],'SAMPLE_LIMIT')

    def test_nontransient_coverage_never_retries(self):
        bad=self.partial_sample();bad['errors']={'READ_ERROR':1}
        obj,_,_,calls,samples=self.attempt([bad,self.safe_sample()])
        self.assertEqual(obj.probes[0]['payload']['admission_result']['reason'],'NONTRANSIENT_COVERAGE')
        self.assertEqual(len(samples),1);self.assertEqual(calls,[])

    def test_context_changed_between_samples_is_terminal(self):
        def mutate(index,current,clock):
            if index==1:current[0]=watch('a',3,revision=2)
        obj,_,_,calls,samples=self.attempt([self.partial_sample(),self.safe_sample()],mutate=mutate)
        self.assertEqual(obj.probes[0]['payload']['admission_result']['reason'],'WATCH_CONTEXT_CHANGED')
        self.assertEqual(len(samples),2);self.assertEqual(calls,[])

    def test_session_and_target_changes_are_terminal(self):
        for field in ('session','target'):
            def mutate(index,current,clock):
                if field=='session':current[0]['journal']['process_session_id']='other'
                else:current[0]['post_cleanup_followups']['last_sample']['observation_index']=4
            obj,_,_,calls,samples=self.attempt([self.safe_sample()],mutate=mutate)
            self.assertEqual(obj.probes[0]['payload']['admission_result']['reason'],'WATCH_CONTEXT_CHANGED')
            self.assertEqual(calls,[])

    def test_admission_time_bound_stops_without_graph(self):
        def mutate(index,current,clock):clock.value+=m.MAX_ADMISSION_SECONDS
        obj,_,_,calls,samples=self.attempt([self.safe_sample()],mutate=mutate)
        self.assertEqual(obj.probes[0]['payload']['admission_result']['reason'],'ADMISSION_TIME_BOUND')
        self.assertEqual(len(samples),1);self.assertEqual(calls,[])

    def test_admission_exception_consumes_target_without_repeat(self):
        obj,_,_,calls,samples=self.attempt([RuntimeError('private secret')]);obj.tick()
        r=obj.probes[0]['payload']['admission_result'];self.assertEqual(r['reason'],'ADMISSION_EXCEPTION')
        self.assertEqual(len(samples),1);self.assertEqual(calls,[]);self.assertNotIn('private',json.dumps(obj.status()))

    def test_watch_change_during_allocator_emission_prevents_walk(self):
        current=[watch('a',2)];obj,clock,events,calls=self.make(current);obj.tick()
        def emit(identity):current[0]=watch('a',3,revision=2);return dict(status='TEST_FAKE')
        obj.allocator_emit=emit;current[0]=watch('a',3);clock.value=120;obj.tick()
        self.assertEqual(calls,[]);self.assertEqual(obj.probes[0]['payload']['status'],'WATCH_ACTIVITY_CHANGED')
        self.assertEqual(obj.probes[0]['payload']['cache']['reason'],'PREWALK_CONTEXT_CHANGED')

class SingleProductionTargetTests(unittest.TestCase):
    make = Tests.make

    def test_default_is_only_original600_target(self):
        self.assertEqual(m.PROBE_INDICES, (5,))
        self.assertEqual(m.EXPIRES_AT_UTC, '2026-11-01T17:00:00+00:00')
        self.assertEqual(m.MAX_SELF_RSS_BYTES, 1073741824)
        self.assertEqual(m.MAX_ADMISSION_SAMPLES, 3)
        self.assertEqual(m.MAX_ADMISSION_SECONDS, 2.5)

    def test_earlier_tails_never_walk_then_exactly_one600(self):
        current=[watch('a',1)];obj,clock,events,calls=self.make(current);obj.tick()
        for index,seconds in ((2,30),(3,120),(4,300)):
            current[0]=watch('a',index);clock.value=seconds;obj.tick()
            self.assertEqual(calls,[]);self.assertEqual(obj.probes,[])
        current[0]=watch('a',5);clock.value=600;obj.tick();obj.tick()
        self.assertEqual(len(calls),1)
        self.assertEqual([p['payload']['observation_index'] for p in obj.probes],[5])
        clock.value=660;self.assertFalse(obj.tick());self.assertEqual(obj.state,'COMPLETED')
        current[0]=watch('b',5);self.assertFalse(obj.tick());self.assertEqual(len(calls),1)

    def test_original600_unsafe_is_terminal_not_replaced(self):
        current=[watch('a',1)];obj,clock,events,calls=self.make(current);obj.tick()
        current[0]=watch('a',5,browser=1);clock.value=600;obj.tick()
        self.assertEqual(calls,[]);self.assertEqual(len(obj.probes),1)
        self.assertEqual(obj.probes[0]['payload']['admission_result']['reason'],'CACHED_POSITIVE_DESCENDANT')
        current[0]=watch('a',5);clock.value=610;obj.tick();self.assertEqual(calls,[])

    def test_original600_partial_fresh_retry_is_preserved(self):
        current=[watch('a',1)];obj,clock,events,calls=self.make(current);obj.tick()
        samples=[Tests.partial_sample(),Tests.safe_sample()];used=[]
        def admission():
            used.append(True);return samples[len(used)-1]
        obj.admission_measure=admission
        current[0]=watch('a',5,complete=False);clock.value=600;obj.tick()
        self.assertEqual(len(calls),1);self.assertEqual(len(used),2)
        result=obj.probes[0]['payload']['admission_result']
        self.assertEqual(result['selected_sample_index'],2)
        self.assertEqual([x['decision'] for x in result['samples']],['RETRY_TRANSIENT_COVERAGE','ADMIT'])

    def test_later_cycle_preemption_does_not_replace_original(self):
        current=[watch('a',1)];obj,clock,events,calls=self.make(current);obj.tick()
        current[0]=watch('b',1);clock.value=601;obj.tick()
        self.assertEqual(calls,[]);self.assertEqual(len(obj.probes),1)
        self.assertEqual(obj.probes[0]['payload']['status'],'PREEMPTED_BY_LATER_CYCLE')
        self.assertEqual(obj.probes[0]['payload']['observation_index'],5)

    def test_600_walk_rechecks_context_after_allocator(self):
        current=[watch('a',1)];obj,clock,events,calls=self.make(current);obj.tick()
        def emit(identity):
            current[0]=watch('a',5,revision=2);return {'status':'TEST_FAKE'}
        obj.allocator_emit=emit;current[0]=watch('a',5);clock.value=600;obj.tick()
        self.assertEqual(calls,[])
        self.assertEqual(obj.probes[0]['payload']['status'],'WATCH_ACTIVITY_CHANGED')


if __name__ == '__main__': unittest.main()
