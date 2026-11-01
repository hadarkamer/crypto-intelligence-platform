"""Finite controller controls with fake dependencies; no live/native probes."""
from copy import deepcopy
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sys
from types import ModuleType
import unittest
from unittest.mock import patch

# The exact controller is loaded; all data-acquisition dependencies are fakes.
dependencies = ('research_rebound_memory','research_current_history_memory',
    'research_retained_memory_accounting_v2','research_process_memory','research_runtime_memory')
saved = {name: sys.modules.get(name) for name in dependencies}
for name in dependencies:
    fake = ModuleType(name)
    def forbidden(*args, **kwargs): raise AssertionError('REAL_DEPENDENCY_NOT_ALLOWED')
    for function in ('capture_shells_and_operations','capture_live_roots_and_measure',
            'install_operation_observer','stop_operation_observer','operation_status','sample','sample_memory'):
        setattr(fake,function,forbidden)
    sys.modules[name] = fake
path = Path(__file__).with_name('research_rebound_memory_investigation.py')
spec = importlib.util.spec_from_file_location('rebound_controller_under_test',path)
m = importlib.util.module_from_spec(spec)
try: spec.loader.exec_module(m)
finally:
    for name, value in saved.items():
        if value is None:sys.modules.pop(name,None)
        else:sys.modules[name]=value

CYCLE = 'shared-watch:2026-11-01T17:32:15.000001+00:00'
LATER = 'shared-watch:2026-11-01T18:02:15.000001+00:00'
EARLIER = 'shared-watch:2026-11-01T17:02:15.000001+00:00'
SESSION = '1'*32


class Clock:
    def __init__(self):self.value=0.
    def __call__(self):return self.value


def memory(**changes):
    result=dict(status='COMPLETE',self=dict(rss_bytes=600_000_000),
        descendants=dict(browser_named=dict(process_count=0),other=dict(process_count=0)),
        cgroup=dict(current_bytes=800_000_000,max_bytes=2_147_483_648),errors={})
    result.update(changes)
    return result


def watch(*,cycle=CYCLE,index=2,revision=1,active=0,anchor=True,durable=True,session=SESSION):
    slot='2026-11-01T18:02:15Z' if cycle==LATER else '2026-11-01T17:02:15Z' if cycle==EARLIER else m.FIXED_NOMINAL_SLOT_UTC
    return dict(nominal_watch_slot_utc=slot,active_cycle_count=active,journal=dict(process_session_id=session),
        cleanup_anchor=dict(status='CAPTURED',origin_cycle_id=cycle,watch_session=session,
            cleanup_tail_at_utc='2026-11-01T17:38:00Z',cleanup_monotonic=0.,activity_revision=revision) if anchor else dict(status='UNAVAILABLE'),
        post_cleanup_followups=dict(pending_origin_cycle_id=cycle if index!=5 else None,activity_revision=revision,
            last_sample=dict(origin_cycle_id=cycle,observation_index=index,memory=memory()),
            persistence=dict(last_durable=dict(durable=durable,persisted=durable,origin_cycle_id=cycle,
                observation_index=index,process_session_id=session,observation_kind='POST_CLEANUP_MEMORY'))))


class Tests(unittest.TestCase):
    def make(self,current=None):
        current=current or [watch()]
        clock=Clock();events=[];calls=[]
        def shell(**kwargs):calls.append(('shell',kwargs));return dict(status='COMPLETE',roots={})
        def history(**kwargs):calls.append(('h4',kwargs));return dict(status='COMPLETE',complete=True,measurement_valid=True)
        def root32(**kwargs):calls.append(('root32',kwargs));return dict(status='COMPLETE',complete=True,measurement_valid=True)
        def start(**kwargs):calls.append(('start',kwargs));return dict(status='INSTALLED')
        def stop():calls.append(('stop',{}));return dict(status='STOPPED')
        def emit(identity,**kwargs):calls.append(('emit',dict(identity=identity,**kwargs)));return dict(status='FAKE_NO_NATIVE_CALL')
        obj=m.Investigation(lambda:current[0],duration=3600,clock=clock,sink=events.append,
            measure=lambda:dict(status='COMPLETE',rss_bytes=600_000_000,python_counters=dict(allocated_blocks_count=100)),
            shell_measure=shell,history_measure=history,root32_measure=root32,admission_measure=memory,
            operation_install=start,operation_stop=stop,operation_status=lambda:dict(status='FAKE'),allocator_emit=emit)
        return obj,clock,current,calls,events

    def selected(self):
        obj,clock,current,calls,events=self.make();obj.tick()
        return obj,clock,current,calls,events

    def to(self,clock,current,obj,age):
        clock.value=float(age)
        if age>=600:current[0]=watch(index=5,anchor=False)
        return obj.tick()

    def test_original_fixed_schedule_and_caps(self):
        obj,clock,current,calls,_=self.selected()
        for age in (540,600,660,720,780,840,900,1020,1080):self.to(clock,current,obj,age)
        self.assertEqual(obj.state,'COMPLETED')
        self.assertEqual([e['payload']['measurement_id'] for e in obj.snapshots],['SHELL_600','SHELL_660','SHELL_720','SHELL_780'])
        self.assertEqual([e['payload']['measurement_id'] for e in obj.probes],['H4_900','ROOT32_1020'])
        self.assertTrue(all(e['payload']['original_native_tail_index']==5 for e in obj.snapshots+obj.probes))
        self.assertEqual([c[0] for c in calls].count('h4'),1);self.assertEqual([c[0] for c in calls].count('root32'),1)
        self.assertEqual(next(c[1] for c in calls if c[0]=='h4'),dict(max_nodes=4_000_000,max_workspace_bytes=268435456,max_elapsed_seconds=30.))
        self.assertEqual(next(c[1] for c in calls if c[0]=='root32'),dict(max_nodes=400_000,max_workspace_bytes=67108864,max_elapsed_seconds=8.))

    def test_no_deep_walk_inside_rebound_window(self):
        obj,clock,current,calls,_=self.selected()
        for age in (540,600,615,660,720,780,840):self.to(clock,current,obj,age)
        self.assertFalse(any(c[0] in ('h4','root32') for c in calls))
        self.assertTrue(all(e['payload']['eligible_for_rebound_comparison'] for e in obj.snapshots))

    def test_graph32_immutable_disabled_branch(self):
        obj,clock,current,calls,_=self.selected()
        with patch.object(m,'ENABLE_ROOT32_GRAPH',False):self.to(clock,current,obj,1020)
        self.assertFalse(any(c[0]=='root32' for c in calls))
        self.assertEqual(obj.probes[-1]['payload']['status'],'DISABLED_NOT_FEASIBILITY_APPROVED')

    def test_partial_h4_does_not_cancel_independent_root32(self):
        obj,clock,current,calls,_=self.selected()
        obj.history_measure=lambda **kwargs:dict(status='PARTIAL',complete=False)
        self.to(clock,current,obj,900);self.to(clock,current,obj,1020)
        self.assertTrue(any(c[0]=='root32' for c in calls))

    def test_graph_exception_is_reserved_once(self):
        obj,clock,current,calls,_=self.selected();attempts=[]
        def fail(**kwargs):attempts.append(1);raise BaseException('PRIVATE_ERROR')
        obj.history_measure=fail
        self.to(clock,current,obj,900);obj.tick()
        self.assertEqual(attempts,[1]);self.assertNotIn('PRIVATE_ERROR',json.dumps(obj.status()))
        self.assertEqual(obj.probes[0]['payload']['cache']['reason_codes'],['POST_REBOUND_GRAPH_ERROR'])

    def test_graph_requires_durable_original_tail(self):
        obj,clock,current,calls,_=self.selected();clock.value=900;current[0]=watch(index=5,anchor=False,durable=False);obj.tick()
        self.assertFalse(any(c[0]=='h4' for c in calls));self.assertEqual(obj.probes[0]['payload']['status'],'SKIPPED_WATCH_CONTEXT_CHANGED')

    def test_wrong_durable_session_rejected(self):
        obj,clock,current,calls,_=self.selected();clock.value=900;current[0]=watch(index=5,anchor=False)
        current[0]['post_cleanup_followups']['persistence']['last_durable']['process_session_id']='2'*32
        obj.tick();self.assertFalse(any(c[0]=='h4' for c in calls))

    def test_cached_positive_children_veto_without_fresh_call(self):
        obj,clock,current,calls,_=self.selected();clock.value=900;current[0]=watch(index=5,anchor=False)
        current[0]['post_cleanup_followups']['last_sample']['memory']['descendants']['browser_named']['process_count']=1
        obj.admission_measure=lambda: (_ for _ in ()).throw(AssertionError('SHOULD_NOT_RUN'))
        obj.tick();self.assertEqual(obj.probes[0]['payload']['admission_result']['reason'],'CACHED_POSITIVE_DESCENDANT')

    def test_transient_retry_retains_both_samples(self):
        obj,clock,current,_,_=self.selected();current[0]=watch(index=5,anchor=False)
        values=iter([memory(status='PARTIAL',errors={'TIME_BUDGET':1}),memory()]);obj.admission_measure=lambda:next(values)
        result=obj._fresh_admission(memory());self.assertEqual(result['status'],'ADMITTED')
        self.assertEqual([v['decision'] for v in result['samples']],['RETRY_TRANSIENT_COVERAGE','ADMIT'])

    def test_nontransient_never_retries(self):
        obj,_,current,_,_=self.selected();current[0]=watch(index=5,anchor=False);calls=[]
        obj.admission_measure=lambda:calls.append(1) or memory(status='PARTIAL',errors={'PERMISSION':1})
        self.assertEqual(obj._fresh_admission(memory())['reason'],'NONTRANSIENT_COVERAGE');self.assertEqual(calls,[1])

    def test_admission_baseexception_retained(self):
        obj,_,current,_,_=self.selected();current[0]=watch(index=5,anchor=False)
        def fail():raise BaseException('PRIVATE')
        obj.admission_measure=fail;result=obj._fresh_admission(memory())
        self.assertEqual(result['samples'][0]['decision'],'ADMISSION_EXCEPTION');self.assertNotIn('PRIVATE',json.dumps(result))

    def test_admission_bad_type_retained(self):
        obj,_,current,_,_=self.selected();current[0]=watch(index=5,anchor=False);obj.admission_measure=lambda:[]
        self.assertEqual(obj._fresh_admission(memory())['reason'],'ADMISSION_EXCEPTION')

    def test_fresh_context_change_blocks_graph(self):
        obj,clock,current,calls,_=self.selected()
        def change():current[0]=watch(index=5,anchor=False,revision=2);return memory()
        obj.admission_measure=change;self.to(clock,current,obj,900)
        self.assertFalse(any(c[0]=='h4' for c in calls))

    def test_fresh_time_limit_blocks_graph(self):
        obj,clock,current,calls,_=self.selected()
        def slow():clock.value+=2.5;return memory()
        obj.admission_measure=slow;self.to(clock,current,obj,900)
        self.assertEqual(obj.probes[0]['payload']['admission_result']['reason'],'ADMISSION_TIME_BOUND')
        self.assertFalse(any(c[0]=='h4' for c in calls))

    def test_graph_cgroup_headroom_blocks(self):
        obj,clock,current,calls,_=self.selected()
        obj.admission_measure=lambda:memory(cgroup=dict(current_bytes=2_000_000_000,max_bytes=2_147_483_648))
        self.to(clock,current,obj,900);self.assertFalse(any(c[0]=='h4' for c in calls))

    def test_process_rss_guard_blocks_graph(self):
        obj,clock,current,calls,_=self.selected();obj.measure=lambda:dict(rss_bytes=m.MAX_SELF_RSS_BYTES+1)
        self.to(clock,current,obj,900);self.assertEqual(obj.probes[0]['payload']['status'],'SKIPPED_SELF_RSS_GUARD')

    def test_revision_change_preempts_without_zero_count_idle_claim(self):
        obj,clock,current,calls,_=self.selected();self.to(clock,current,obj,540)
        clock.value=601;current[0]=watch(index=5,revision=2);obj.tick()
        self.assertEqual(len(obj.reserved),6);self.assertEqual([c[0]for c in calls].count('stop'),1)
        self.assertTrue(all(e['payload']['status']=='PREEMPTED_BY_WATCH_CONTEXT_CHANGE' for e in obj.snapshots+obj.probes))

    def test_new_session_preempts_same_cycle(self):
        obj,clock,current,calls,_=self.selected();clock.value=600;current[0]=watch(index=5,session='2'*32);obj.tick()
        self.assertFalse(any(c[0]=='shell' for c in calls))

    def test_earlier_cycle_is_not_selected(self):
        obj,clock,current,_,_=self.make([watch(cycle=EARLIER)]);obj.tick();self.assertIsNone(obj.selected)
        current[0]=watch();obj.tick();self.assertEqual(obj.selected['origin_cycle_id'],CYCLE)

    def test_later_slot_never_replaces(self):
        obj,_,_,calls,_=self.make([watch(cycle=LATER)]);self.assertFalse(obj.tick());self.assertEqual(obj.state,'MISSED_FIXED_SLOT');self.assertEqual(calls,[])

    def test_missing_original_anchor_is_explicit(self):
        obj,_,_,_,_=self.make([watch(index=5,anchor=False)]);self.assertFalse(obj.tick());self.assertEqual(obj.state,'MISSING_ORIGINAL_CLEANUP_ANCHOR')

    def test_future_anchor_rejected(self):
        current=[watch()];current[0]['cleanup_anchor']['cleanup_monotonic']=1.
        obj,_,_,_,_=self.make(current);self.assertFalse(obj.tick());self.assertIsNone(obj.selected)

    def test_naive_cleanup_anchor_rejected(self):
        current=[watch()];current[0]['cleanup_anchor']['cleanup_tail_at_utc']='2026-11-01T17:38:00'
        obj,_,_,_,_=self.make(current);self.assertFalse(obj.tick())

    def test_missed_shells_are_not_compressed_backfill(self):
        obj,clock,current,calls,_=self.selected();self.to(clock,current,obj,780)
        self.assertEqual([c[0]for c in calls].count('shell'),1)
        self.assertEqual([e['payload']['status']for e in obj.snapshots[:3]],['MISSED_FIXED_MEASUREMENT_WINDOW']*3)

    def test_late_nominal_endpoint_is_not_inside_actual_window(self):
        obj,clock,current,_,_=self.selected();self.to(clock,current,obj,800)
        result=obj.snapshots[-1]['payload'];self.assertEqual(result['measurement_id'],'SHELL_780')
        self.assertIs(result['nominal_window_endpoint'],True);self.assertIs(result['within_actual_rebound_window'],False)
        self.assertIs(result['eligible_for_rebound_comparison'],False);self.assertEqual(result['capture_started_delay_ms'],800000.)

    def test_pulse_crossing_window_end_not_eligible(self):
        obj,clock,current,_,_=self.selected()
        def slow():clock.value+=.2;return dict(status='COMPLETE',rss_bytes=600_000_000)
        obj.measure=slow;self.to(clock,current,obj,779.9)
        result=obj.pulses[-1]['payload']
        self.assertAlmostEqual(result['capture_started_delay_ms'],779900.)
        self.assertAlmostEqual(result['capture_completed_delay_ms'],780100.)
        self.assertIs(result['within_actual_rebound_window'],False)
        self.assertIs(result['eligible_for_rebound_comparison'],False)

    def test_pending_other_cycle_rejected_even_same_revision(self):
        obj,clock,current,calls,_=self.selected();clock.value=600;current[0]=watch(index=5,anchor=False)
        current[0]['post_cleanup_followups']['pending_origin_cycle_id']=LATER
        obj.tick();self.assertFalse(any(c[0]=='shell'for c in calls))

    def test_original_tail5_inflight_gap_allows_cheap_only(self):
        obj,clock,current,calls,_=self.selected();clock.value=600;current[0]=watch(index=4,anchor=False)
        current[0]['post_cleanup_followups']['pending_origin_cycle_id']=None
        obj.tick();self.assertTrue(any(c[0]=='shell'for c in calls));self.assertTrue(obj._context_matches())
        self.assertFalse(obj._context_matches(require_tail=True))
        clock.value=900;obj.tick();self.assertFalse(any(c[0]=='h4'for c in calls))

    def test_tail5_inflight_wrong_origin_rejected(self):
        obj,clock,current,calls,_=self.selected();clock.value=600;current[0]=watch(index=4,anchor=False)
        tail=current[0]['post_cleanup_followups'];tail['pending_origin_cycle_id']=None;tail['last_sample']['origin_cycle_id']=EARLIER
        obj.tick();self.assertFalse(any(c[0]=='shell'for c in calls))

    def test_tail5_inflight_new_revision_rejected(self):
        obj,clock,current,calls,_=self.selected();clock.value=600;current[0]=watch(index=4,anchor=False,revision=2)
        current[0]['post_cleanup_followups']['pending_origin_cycle_id']=None
        obj.tick();self.assertFalse(any(c[0]=='shell'for c in calls))

    def test_missed_h4_not_replaced_at_root32(self):
        obj,clock,current,calls,_=self.selected();self.to(clock,current,obj,1020)
        self.assertFalse(any(c[0]=='h4'for c in calls));self.assertTrue(any(c[0]=='root32'for c in calls))

    def test_shell_exception_is_retained_once(self):
        obj,clock,current,calls,_=self.selected();count=[]
        def fail(**kwargs):count.append(1);raise BaseException('PRIVATE')
        obj.shell_measure=fail;self.to(clock,current,obj,600);obj.tick()
        self.assertEqual(count,[1]);self.assertEqual(obj.snapshots[0]['payload']['shell']['reason'],'CHEAP_SNAPSHOT_ERROR')

    def test_process_failure_does_not_lose_shell(self):
        obj,clock,current,_,_=self.selected()
        def fail():raise BaseException('PRIVATE')
        obj.measure=fail;self.to(clock,current,obj,600)
        result=obj.snapshots[0]['payload'];self.assertEqual(result['shell']['status'],'COMPLETE');self.assertEqual(result['process_after']['reason'],'PROCESS_SNAPSHOT_ERROR')

    def test_only_two_window_emissions(self):
        obj,clock,current,calls,_=self.selected()
        for age in (600,660,720,780,900,1020):self.to(clock,current,obj,age)
        emissions=[c[1]for c in calls if c[0]=='emit']
        self.assertEqual([c['sample_label']for c in emissions],['WINDOW_BEGIN_600','WINDOW_END_780'])
        self.assertEqual([c['identity']['measurement_id']for c in emissions],['SHELL_600','SHELL_780'])

    def test_window_emission_order(self):
        obj,clock,current,calls,_=self.selected();self.to(clock,current,obj,600);self.to(clock,current,obj,780)
        names=[c[0]for c in calls];self.assertLess(names.index('emit'),names.index('shell'));self.assertEqual(names[-1],'emit')

    def test_emission_failure_not_retried(self):
        obj,clock,current,calls,_=self.selected();count=[]
        def fail(*args,**kwargs):count.append(1);raise BaseException('PRIVATE')
        obj.allocator_emit=fail;self.to(clock,current,obj,600);obj.tick()
        self.assertEqual(count,[1]);self.assertEqual(obj.snapshots[0]['payload']['allocator_emission']['reason'],'AGGREGATE_EMISSION_FAILED')

    def test_all_postgraph_pulses_marked_perturbed(self):
        obj,clock,current,_,_=self.selected();self.to(clock,current,obj,900);self.to(clock,current,obj,915)
        pulse=obj.pulses[-1]['payload'];self.assertIs(pulse['observer_perturbed'],True);self.assertIs(pulse['eligible_for_rebound_comparison'],False)

    def test_graph_mutation_invalidates_comparison(self):
        obj,clock,current,_,_=self.selected()
        def change(**kwargs):current[0]=watch(index=5,revision=2,anchor=False);return dict(status='COMPLETE')
        obj.history_measure=change;self.to(clock,current,obj,900)
        self.assertEqual(obj.probes[0]['payload']['status'],'WATCH_ACTIVITY_CHANGED')

    def test_status_does_not_measure(self):
        obj,_,_,calls,_=self.make();obj.status();obj.status();self.assertEqual(calls,[])

    def test_deadline_stops_operation_once(self):
        obj,clock,current,calls,_=self.selected();self.to(clock,current,obj,540);clock.value=3600
        self.assertFalse(obj.tick());self.assertEqual([c[0]for c in calls].count('stop'),1);self.assertEqual(obj.state,'EXPIRED')

    def test_late_operation_window_not_rearmed(self):
        obj,clock,current,calls,_=self.selected();self.to(clock,current,obj,600);self.to(clock,current,obj,615)
        self.assertFalse(any(c[0]=='start'for c in calls));self.assertEqual(obj.operation_events[0]['payload']['observer']['status'],'MISSED_OPERATION_WINDOW_PREFIX')

    def test_operation_window_fixed_stop(self):
        obj,clock,current,calls,_=self.selected()
        for age in (540,600,840,845):self.to(clock,current,obj,age)
        self.assertEqual([c[0]for c in calls].count('start'),1);self.assertEqual([c[0]for c in calls].count('stop'),1)
        self.assertEqual(next(c[1]for c in calls if c[0]=='start')['deadline_monotonic'],840.)

    def test_final_operation_status_sees_poststop_completed_edge(self):
        obj,clock,current,_,_=self.selected();reads=[]
        obj.operation_status=lambda:reads.append(1) or dict(status='STOPPED',completed_after_window=1,active_tracked_calls=0,coverage_complete=False)
        for age in (540,840,900,1020,1080):self.to(clock,current,obj,age)
        self.assertEqual(reads,[1]);self.assertEqual(obj.operation_final_status['observer']['completed_after_window'],1)
        self.assertIn('NOT_ALL_WORK_FINISHED',obj.operation_final_status['scope'])

    def test_final_operation_status_cached_not_recaptured(self):
        obj,clock,current,_,_=self.selected();reads=[]
        obj.operation_status=lambda:reads.append(1) or dict(status='STOPPED',coverage_complete=False)
        for age in (540,840,900,1020,1080):self.to(clock,current,obj,age)
        obj.status();obj.status();obj._final_operations('MONITOR_EXIT');self.assertEqual(reads,[1])

    def test_final_operation_status_failure_closed_no_retry(self):
        obj,clock,current,_,_=self.selected();reads=[]
        def fail():reads.append(1);raise BaseException('PRIVATE')
        obj.operation_status=fail
        for age in (540,840,900,1020,1080):self.to(clock,current,obj,age)
        obj._final_operations('MONITOR_EXIT');self.assertEqual(reads,[1])
        self.assertEqual(obj.operation_final_status['observer']['reason'],'FINAL_OPERATION_STATUS_ERROR')
        self.assertNotIn('PRIVATE',json.dumps(obj.status()))

    def test_allocator_guard_and_closed_markers(self):
        for label in ('WINDOW_BEGIN_600','WINDOW_END_780'):
            writes=[]
            value=m.emit_allocator_stats({},sample_label=label,implementation='cpython',version=(3,11,9),debug_build=False,platform_name='linux',function=locals,write=lambda fd,raw:writes.append((fd,raw)) or len(raw))
            self.assertEqual(value['status'],'EMITTED_NOT_YET_INGESTED');self.assertEqual([a for a,_ in writes],[2,2])
            self.assertTrue(all(b'[REBOUND_ALLOCATOR_V1]'in raw for _,raw in writes))

    def test_allocator_unsupported_abi_never_calls(self):
        calls=[]
        value=m.emit_allocator_stats({},sample_label='WINDOW_BEGIN_600',version=(3,11,8),function=locals,write=lambda *args:calls.append(1))
        self.assertEqual(value['status'],'UNAVAILABLE');self.assertEqual(calls,[])

    def test_allocator_invalid_label_never_calls(self):
        calls=[];value=m.emit_allocator_stats({},sample_label=True,function=locals,write=lambda *args:calls.append(1))
        self.assertEqual(value['reason'],'INVALID_SAMPLE_LABEL');self.assertEqual(calls,[])

    def test_allocator_short_marker_write_retained(self):
        value=m.emit_allocator_stats({},sample_label='WINDOW_BEGIN_600',implementation='cpython',version=(3,11,9),debug_build=False,platform_name='linux',function=locals,write=lambda *args:0)
        self.assertEqual(value['reason'],'AGGREGATE_EMISSION_FAILED')

    def test_absolute_expiry_and_duration(self):
        self.assertEqual(m.remaining_seconds(datetime(2026,11,1,23,tzinfo=timezone.utc)),0.)
        self.assertEqual(m.remaining_seconds(datetime(2026,11,1,16,tzinfo=timezone.utc)),5400)

    def test_late_install_cached_no_thread(self):
        with patch.object(m,'_INSTANCE',None),patch.object(m,'_INSTALL_STATE',None),patch.object(m,'remaining_seconds',return_value=100),patch.object(m,'utc_now',return_value=m.FIXED_NOMINAL_SLOT_UTC),patch.object(m.threading,'Thread',side_effect=AssertionError('NO_THREAD')):
            m.install();m.install();self.assertEqual(m.status()['state'],'MISSED_FIXED_SLOT')

    def test_import_source_has_no_trim_gc_or_cache_clear(self):
        import ast
        tree=ast.parse(path.read_text(encoding='utf-8'))
        calls=[node for node in ast.walk(tree)if isinstance(node,ast.Call)]
        names=[node.func.attr if isinstance(node.func,ast.Attribute)else node.func.id if isinstance(node.func,ast.Name)else ''for node in calls]
        self.assertFalse(set(names)&{'malloc_trim','collect','clear_cache','clear_flow_cache','parse_smaps'})

    def adapter(self, *, changed=False, acquired=True, naive=False, wrong_type=False):
        module=ModuleType('research_watch_runtime');released=[];timeouts=[]
        class Condition:
            def acquire(self,**kwargs):timeouts.append(kwargs['timeout']);return acquired
            def release(self):released.append(1)
        class Plan:pass
        plan=Plan();plan.identity=dict(origin_cycle_id=CYCLE,process_session_id=SESSION,cleanup_tail_at_utc='2026-11-01T17:38:00' if naive else '2026-11-01T17:38:00Z',private_unused='NOT_COPIED');plan.anchor=123.;plan.activity_revision=1
        module._TailPlan=Plan;module._TAIL_PLAN=object()if wrong_type else plan;module._CONDITION=Condition();module._ACTIVE={};module._ACTIVITY_REVISION=1
        count=[]
        def status():
            count.append(1);return watch(revision=2 if changed and len(count)>1 else 1)
        module.status=status
        with patch.dict(sys.modules,{'research_watch_runtime':module}),patch.object(m.threading,'Condition',Condition),patch.object(m,'_native_slot',return_value=m.FIXED_NOMINAL_SLOT_UTC):result=m._observed_watch_status()
        return result,released,timeouts

    def test_anchor_adapter_bounded_scalar_copy(self):
        result,released,timeouts=self.adapter()
        self.assertEqual(result['cleanup_anchor']['status'],'CAPTURED');self.assertEqual(released,[1]);self.assertEqual(timeouts,[.005]);self.assertNotIn('private_unused',json.dumps(result['cleanup_anchor']))

    def test_anchor_adapter_lock_unavailable(self):
        result,released,_=self.adapter(acquired=False);self.assertEqual(result['cleanup_anchor']['status'],'LOCK_UNAVAILABLE');self.assertEqual(released,[])

    def test_anchor_adapter_reread_changed(self):
        result,released,_=self.adapter(changed=True);self.assertEqual(result['cleanup_anchor']['status'],'PUBLIC_CONTEXT_CHANGED');self.assertEqual(released,[1])

    def test_anchor_adapter_naive_clock(self):
        result,released,_=self.adapter(naive=True);self.assertEqual(result['cleanup_anchor']['status'],'UNAVAILABLE');self.assertEqual(released,[1])

    def test_anchor_adapter_wrong_registered_type(self):
        result,released,_=self.adapter(wrong_type=True);self.assertEqual(result['cleanup_anchor']['status'],'ORIGINAL_PLAN_NOT_AVAILABLE');self.assertEqual(released,[1])


if __name__=='__main__':unittest.main()
