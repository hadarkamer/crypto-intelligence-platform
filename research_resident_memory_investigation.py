"""Finite fixed-slot resident-memory trial; cached health only.

New finite protocol. Existing observers and their deadlines are unchanged.
Only one predeclared native slot and its original +600 tail can admit the
separately bounded paired core. No GC, cache clear, heap walk or task wrapper.
"""
from __future__ import annotations

from collections import deque
from datetime import datetime, timezone, timedelta
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import threading
import time
from types import BuiltinFunctionType, ModuleType
from uuid import uuid4

import research_resident_memory
import research_process_memory
import research_runtime_memory

VERSION = 'resident-memory-investigation-v1'
EXPIRES_AT_UTC = '2026-11-01T20:00:00+00:00'
FIXED_NOMINAL_SLOT_UTC = '2026-11-01T16:02:15+00:00'
MAX_DURATION_SECONDS = 5400
PULSE_SECONDS = 15
POLL_SECONDS = 5
MAX_PULSES = 240
MAX_RECORD_BYTES = 262144
MAX_SELF_RSS_BYTES = 1024 * 1024 * 1024
PROBE_INDICES = (5,)
MAX_ADMISSION_SAMPLES = 3
MAX_ADMISSION_SECONDS = 2.5
TRANSIENT_COVERAGE_ERRORS = frozenset({'TIME_BUDGET', 'ANCESTOR_UNVERIFIED'})
SOURCE_FILES = ('main.py', 'research_resident_memory_investigation.py',
                'research_resident_memory.py', 'research_resident_smaps.py', 'research_process_memory.py')
_INSTANCE = None
_INSTALL_STATE = None
_INSTALL_LOCK = threading.Lock()


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def remaining_seconds(now=None):
    now = now or datetime.now(timezone.utc)
    return max(0., min(MAX_DURATION_SECONDS,
        (datetime.fromisoformat(EXPIRES_AT_UTC) - now).total_seconds()))


def _json_copy(value, limit=MAX_RECORD_BYTES):
    raw = json.dumps(value, separators=(',', ':'), allow_nan=False)
    if len(raw.encode('utf-8')) > limit:
        raise ValueError('RECORD_BOUND')
    return json.loads(raw)



def _trial_witness(value, *, invoked, reason):
    """Tiny exact-built-in metadata; never turn missing return data into no-call."""
    trim=dict.get(value,'trim') if type(value) is dict else None
    known=(type(trim) is dict and type(dict.get(trim,'attempted')) is bool and
        type(dict.get(trim,'call_count')) is int and dict.get(trim,'call_count') in (0,1) and
        dict.get(trim,'call_count')==(1 if dict.get(trim,'attempted') else 0))
    unknown=bool(invoked) and not known
    attempted=dict.get(trim,'attempted') if known else None if unknown else False
    count=dict.get(trim,'call_count') if known else None if unknown else 0
    returned=dict.get(trim,'return_value') if known else None
    returned=returned if type(returned) is int and returned in (0,1) else None
    outcome=dict.get(trim,'outcome') if known else None
    if type(outcome) is not str or outcome not in ('NOT_ATTEMPTED','CALL_IN_FLIGHT','RETURNED','INVALID_RETURN_NOT_RETRIED','EXCEPTION_NOT_RETRIED'):
        outcome='UNKNOWN_POSSIBLY_ATTEMPTED' if unknown else 'RETURN_METADATA_INCOMPLETE' if known else 'NOT_ATTEMPTED'
    def clock(name):
        stamp=dict.get(trim,name) if known else None
        if type(stamp) is not str or len(stamp)>80:return None
        try:
            parsed=datetime.fromisoformat(stamp.replace('Z','+00:00'))
            return stamp if parsed.utcoffset() is not None else None
        except (ValueError,TypeError,OverflowError):return None
    return dict(status='UNAVAILABLE',reason=reason,components_omitted=True,
        trim=dict(attempt_knowledge='UNKNOWN_POSSIBLY_ATTEMPTED' if unknown else 'KNOWN_RETURNED_TRIAL' if known else 'CORE_NOT_INVOKED',
            attempted=attempted,call_count=count,pad_bytes=0 if known or not invoked else None,
            return_value=returned,valid_return=(dict.get(trim,'valid_return') is True and returned is not None) if known else None if unknown else False,
            outcome=outcome,started_at_utc=clock('started_at_utc'),completed_at_utc=clock('completed_at_utc'),
            return_semantics='ZERO_OR_ONE_NOT_BYTES_FREED'),
        scope='RETAINED_ATTEMPT_METADATA_ONLY_COMPONENTS_OMITTED_NOT_COMPLETED_PROTOCOL')


def _pins():
    result = {}
    for name in SOURCE_FILES:
        raw = (Path(__file__).resolve().parent / name).read_bytes()
        if len(raw) > 2 * 1024 * 1024:
            raise ValueError('SOURCE_BOUND')
        result[name] = hashlib.sha256(raw).hexdigest()
    return result


def emit_pymalloc_stats(identity, *, sample_label='BEFORE_SHAM', implementation=None, version=None,
                       debug_build=None, function=None, write=None, platform_name=None):
    """At most one release-build aggregate dump per admitted cache attempt.

    Both markers and CPython output use stderr, with no redirection or capture.
    Retrieval must reject incomplete/interleaved/unexpected tables. This reads
    allocator structures before the accounting memo exists, never object values.
    """
    if type(sample_label) is not str or sample_label not in ('BEFORE_SHAM','AFTER_SETTLED'):
        return dict(status='UNAVAILABLE', reason='INVALID_SAMPLE_LABEL')
    implementation = sys.implementation.name if implementation is None else implementation
    version = tuple(sys.version_info[:3]) if version is None else version
    debug_build = hasattr(sys, 'gettotalrefcount') if debug_build is None else debug_build
    function = getattr(sys, '_debugmallocstats', None) if function is None else function
    platform_name = sys.platform if platform_name is None else platform_name
    if platform_name != 'linux' or implementation != 'cpython' or version != (3, 11, 9) or debug_build or type(function) is not BuiltinFunctionType:
        return dict(status='UNAVAILABLE', reason='EXPECTED_RELEASE_CPYTHON3119_API_UNAVAILABLE')
    write = os.write if write is None else write
    started = utc_now()
    def marker(phase):
        raw = ('[RESIDENT_ALLOCATOR_V1] ' + json.dumps(dict(phase=phase,
            identity=identity, sample_label=sample_label, at_utc=utc_now()), separators=(',', ':'), allow_nan=False) + '\n').encode('utf-8')
        if len(raw) > 4096 or write(2, raw) != len(raw):
            raise ValueError('MARKER_WRITE_BOUND')
    try:
        marker('BEGIN')
        function()
        marker('END')
        return dict(status='EMITTED_NOT_YET_INGESTED', started_at_utc=started,
            completed_at_utc=utc_now(), format='CPYTHON_3_11_9_RELEASE_DEBUGMALLOCSTATS',
            sample_label=sample_label, phase='PAIRED_TRIAL_SOURCE_ANNOTATION',
            scope='ALLOCATOR_CAPACITY_AND_BLOCKS_NOT_CACHE_OWNERSHIP_OR_RESIDENT_PAGE_PARTITION')
    except BaseException:
        return dict(status='UNAVAILABLE', reason='AGGREGATE_EMISSION_FAILED')



def _slot_relation(value):
    if type(value) is not str or len(value)>80:return None
    try:
        stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
        if stamp.utcoffset() is None:return None
        fixed=datetime.fromisoformat(FIXED_NOMINAL_SLOT_UTC)
        return -1 if stamp<fixed else 1 if stamp>fixed else 0
    except (ValueError,TypeError,OverflowError):return None


def _cycle_in_fixed_slot(cycle):
    if type(cycle) is not str or not cycle.startswith('shared-watch:') or len(cycle)>160:return False
    try:
        stamp=datetime.fromisoformat(cycle.removeprefix('shared-watch:').replace('Z','+00:00'))
        fixed=datetime.fromisoformat(FIXED_NOMINAL_SLOT_UTC)
        return stamp.utcoffset() is not None and fixed<=stamp<fixed+timedelta(minutes=30)
    except (ValueError,TypeError,OverflowError):return False


def _observed_watch_status():
    """Read one known scalar, never frames, arbitrary objects or globals graphs."""
    try:
        import research_watch_runtime
        value=research_watch_runtime.status()
        if type(value) is not dict:return {'nominal_watch_slot_utc':None}
        slot=None
        for name in ('__main__','main'):
            module=sys.modules.get(name)
            if type(module) is not ModuleType:continue
            namespace=module.__dict__;runtime=dict.get(namespace,'WATCH_RUNTIME')
            if type(runtime) is not dict:continue
            candidate=dict.get(runtime,'last_claimed_slot_utc')
            slot=candidate if _slot_relation(candidate) is not None else None
            break
        return {**value,'nominal_watch_slot_utc':slot}
    except BaseException:return {'nominal_watch_slot_utc':None}


class Investigation:
    def __init__(self, watch_status, *, duration, measure=None, trial_measure=None,
                 clock=time.monotonic, sink=None, pins=None, admission_measure=None,
                 allocator_emit=None):
        self.watch_status = watch_status
        self.measure = measure or (lambda: research_process_memory.sample(include_mallinfo2=True))
        self.trial_measure = trial_measure or research_resident_memory.run_resident_probe
        self.admission_measure = admission_measure or research_runtime_memory.sample_memory
        self.allocator_emit = allocator_emit or emit_pymalloc_stats
        self.clock = clock
        self.deadline = clock() + max(0., min(duration, MAX_DURATION_SECONDS))
        self.sink = sink or (lambda value: print('[RESIDENT_MEMORY_V1] ' +
                    json.dumps(value, separators=(',', ':'), allow_nan=False), flush=True))
        self.lock = threading.Lock()
        self.session = uuid4().hex
        commit = os.getenv('RENDER_GIT_COMMIT', '')
        self.commit = commit if re.fullmatch('[0-9a-f]{40}', commit) else None
        self.pins = pins or {}
        self.pulses = deque(maxlen=MAX_PULSES)
        self.probes = []
        self.selected = None
        self.completed_indices = set()
        self.last_pulse = float('-inf')
        self.last_probe_finished = None
        self.sequence = 0
        self.errors = 0
        self.state = 'WAITING_FIXED_NATURAL_CLEANUP'
        self.thread = None

    def _record(self, kind, payload):
        with self.lock:
            self.sequence += 1
            event = _json_copy(dict(version=VERSION, session=self.session,
                runtime_commit=self.commit, sequence=self.sequence, at_utc=utc_now(),
                kind=kind, payload=payload))
            if kind == 'PULSE':
                self.pulses.append(event)
            elif kind == 'RESIDENT_PROBE':
                self.probes.append(event)
        try:
            self.sink(event)
        except BaseException:
            with self.lock:
                self.errors += 1

    def _process_snapshot(self):
        try:
            value=_json_copy(self.measure(),16384)
            if type(value) is dict:return value
        except BaseException:pass
        return dict(status='UNAVAILABLE',rss_bytes=None,reason='PROCESS_SNAPSHOT_ERROR')

    def _fresh_admission(self, cycle, target, tail, cached_memory):
        """At most three coverage-only retries inside one reserved target."""
        started=self.clock()
        result=dict(version='fresh-retained-root-admission-v1', status='SKIPPED',
            reason='NOT_ADMITTED', samples=[], selected_sample_index=None,
            cached_tail_memory=_json_copy(cached_memory,8192),
            max_samples=MAX_ADMISSION_SAMPLES, budget_seconds=MAX_ADMISSION_SECONDS,
            scope='FRESH_COMPLETE_SAFETY_ONLY_CACHED_PARTIAL_CONTEXT_NOT_RELABELLED')
        def done(reason):
            result['reason']=reason
            result['elapsed_ms']=round(max(0.,self.clock()-started)*1000,3)
            return result
        def stable():
            return self._context_matches(cycle,target,tail.get('activity_revision'))
        def unsafe(sample):
            children=sample.get('descendants') or {}
            for group in ('browser_named','other'):
                count=(children.get(group) or {}).get('process_count')
                if type(count) is int and count>0:return 'POSITIVE_DESCENDANT'
            rss=(sample.get('self') or {}).get('rss_bytes')
            if type(rss) is int and (rss<0 or rss>MAX_SELF_RSS_BYTES):return 'SELF_RSS_GUARD'
            group=sample.get('cgroup') or {};current=group.get('current_bytes');maximum=group.get('max_bytes')
            if type(current) is int and type(maximum) is int and (current<0 or maximum<=0 or maximum-current<512*1024*1024):return 'CGROUP_HEADROOM_GUARD'
            return None
        cached_veto=unsafe(cached_memory)
        if cached_veto:return done('CACHED_'+cached_veto)
        for index in range(1,MAX_ADMISSION_SAMPLES+1):
            if self.clock()>=self.deadline:return done('DEADLINE')
            if self.clock()-started>=MAX_ADMISSION_SECONDS:return done('ADMISSION_TIME_BOUND')
            if not stable():return done('WATCH_CONTEXT_CHANGED')
            before=self.clock();at=utc_now()
            try:sample=_json_copy(self.admission_measure(),8192)
            except BaseException:
                result['samples'].append(dict(index=index,started_at_utc=at,completed_at_utc=utc_now(),
                    elapsed_ms=round(max(0.,self.clock()-before)*1000,3),sample=None,decision='ADMISSION_EXCEPTION'))
                return done('ADMISSION_EXCEPTION')
            entry=dict(index=index,started_at_utc=at,completed_at_utc=utc_now(),
                elapsed_ms=round(max(0.,self.clock()-before)*1000,3),sample=sample,decision='VETO')
            result['samples'].append(entry)
            veto=unsafe(sample)
            if veto:entry['decision']=veto;return done(veto)
            if not stable():entry['decision']='WATCH_CONTEXT_CHANGED';return done('WATCH_CONTEXT_CHANGED')
            if self.clock()>=self.deadline or self.clock()-started>=MAX_ADMISSION_SECONDS:
                entry['decision']='ADMISSION_TIME_BOUND';return done('ADMISSION_TIME_BOUND')
            children=sample.get('descendants') or {};group=sample.get('cgroup') or {}
            rss=(sample.get('self') or {}).get('rss_bytes');current=group.get('current_bytes');maximum=group.get('max_bytes')
            safe=(sample.get('status')=='COMPLETE' and type(rss) is int and 0<=rss<=MAX_SELF_RSS_BYTES and
                all(type((children.get(g) or {}).get('process_count')) is int and (children.get(g) or {}).get('process_count')==0 for g in ('browser_named','other')) and
                type(current) is int and type(maximum) is int and current>=0 and maximum-current>=512*1024*1024)
            if safe:
                entry['decision']='ADMIT';result['status']='ADMITTED';result['selected_sample_index']=index
                return done('FRESH_COMPLETE_SAFE')
            errors=sample.get('errors') or {}
            transient=(sample.get('status') in ('PARTIAL','UNAVAILABLE') and type(errors) is dict and
                bool(errors) and set(errors)<=TRANSIENT_COVERAGE_ERRORS)
            if not transient:entry['decision']='NONTRANSIENT_COVERAGE';return done('NONTRANSIENT_COVERAGE')
            if index==MAX_ADMISSION_SAMPLES:entry['decision']='SAMPLE_LIMIT';return done('SAMPLE_LIMIT')
            entry['decision']='RETRY_TRANSIENT_COVERAGE'
        return done('SAMPLE_LIMIT')

    def _context_matches(self,cycle,target,revision):
        try:
            current=self.watch_status();tail=current.get('post_cleanup_followups') or {};last=tail.get('last_sample') or {}
            return (type(current.get('active_cycle_count')) is int and current['active_cycle_count']==0 and
                type(revision) is int and type(tail.get('activity_revision')) is int and tail['activity_revision']==revision and
                type(last.get('observation_index')) is int and last['observation_index']==target and
                last.get('origin_cycle_id')==cycle and _cycle_in_fixed_slot(cycle) and
                type(current.get('journal',{}).get('process_session_id')) is str and
                current['journal']['process_session_id']==self.selected.get('watch_session') and
                _slot_relation(current.get('nominal_watch_slot_utc'))==0 and self.clock()<self.deadline)
        except BaseException:return False

    def tick(self):
        now=self.clock()
        if self.state in ('COMPLETED','EXPIRED','PULSE_LIMIT','MISSED_FIXED_SLOT'):return False
        if now>=self.deadline:
            with self.lock:self.state='EXPIRED'
            self._record('EXPIRED',{'missing_probe_indices':[i for i in PROBE_INDICES if i not in self.completed_indices]})
            return False
        watch=self.watch_status();tail=watch.get('post_cleanup_followups') or {};cycle=tail.get('pending_origin_cycle_id')
        relation=_slot_relation(watch.get('nominal_watch_slot_utc'))
        if self.selected is None and relation==1:
            with self.lock:self.state='MISSED_FIXED_SLOT'
            self._record('MISSED_FIXED_SLOT',{'fixed_nominal_slot_utc':FIXED_NOMINAL_SLOT_UTC,'observed_nominal_slot_utc':watch['nominal_watch_slot_utc']})
            return False
        if self.selected is None and relation==0 and _cycle_in_fixed_slot(cycle):
            selected=dict(origin_cycle_id=cycle,watch_session=watch.get('journal',{}).get('process_session_id'),
                activity_revision=tail.get('activity_revision'),fixed_nominal_slot_utc=FIXED_NOMINAL_SLOT_UTC,
                selected_at_utc=utc_now(),rule='FIXED_PREDECLARED_NATIVE_SLOT_ORIGINAL_CLEANUP_WITHOUT_OUTCOME_FILTER')
            with self.lock:self.selected=selected;self.state='OBSERVING_FIXED_SELECTED_CYCLE'
            self._record('SELECTED',selected)
        if now-self.last_pulse>=PULSE_SECONDS:
            if len(self.pulses)>=MAX_PULSES:
                with self.lock:self.state='PULSE_LIMIT'
                self._record('STOPPED',{'reason':'PULSE_LIMIT'});return False
            self._record('PULSE',dict(process=self._process_snapshot(),watch_active_count=watch.get('active_cycle_count'),
                watch_activity_revision=tail.get('activity_revision'),
                nominal_watch_slot_utc=watch.get('nominal_watch_slot_utc') if relation is not None else None))
            self.last_pulse=self.clock()
        last=tail.get('last_sample') or {}
        if self.selected is not None:
            selected_cycle=self.selected['origin_cycle_id'];index=last.get('observation_index')
            if last.get('origin_cycle_id')==selected_cycle and type(index) is int:
                for target in PROBE_INDICES:
                    if target in self.completed_indices or index<target:continue
                    # Consume the original attempt before any native helper/factory.
                    self.completed_indices.add(target);self.last_probe_finished=self.clock();trial_invoked=False
                    if index>target:result=dict(status='MISSED_SCHEDULED_TAIL',trial=None)
                    elif type(watch.get('active_cycle_count')) is not int or watch['active_cycle_count']!=0:
                        result=dict(status='SKIPPED_WATCH_ACTIVE',trial=None)
                    else:
                        before=self._process_snapshot();rss=before.get('rss_bytes');memory=last.get('memory') or {}
                        if type(rss) is not int or not 0<=rss<=MAX_SELF_RSS_BYTES:
                            result=dict(status='SKIPPED_SELF_RSS_GUARD',trial=None,process_before=before)
                        else:
                            admission_result=self._fresh_admission(selected_cycle,target,tail,memory)
                            if admission_result['status']!='ADMITTED':
                                result=dict(status='SKIPPED_FRESH_PROCESS_OR_CGROUP_GUARD',trial=None,process_before=before,admission_result=admission_result)
                            else:
                                identity=dict(session=self.session,runtime_commit=self.commit,origin_cycle_id=selected_cycle,observation_index=target)
                                try:emitted=self.allocator_emit(identity)
                                except BaseException:emitted=dict(status='UNAVAILABLE',reason='AGGREGATE_EMISSION_FAILED')
                                revision=tail.get('activity_revision')
                                context=lambda:self._context_matches(selected_cycle,target,revision)
                                def admit_again():return self._fresh_admission(selected_cycle,target,tail,memory)
                                def core_emit(label,supplied):
                                    if type(label) is not str or label not in ('BEFORE_SHAM','AFTER_SETTLED') or type(supplied) is not dict or supplied!=identity:
                                        raise ValueError('EMITTER_CONTEXT_MISMATCH')
                                    if label=='BEFORE_SHAM':return emitted
                                    return self.allocator_emit(identity,sample_label=label)
                                ready=context()
                                try:
                                    if ready:
                                        trial_invoked=True
                                        trial=self.trial_measure(process_snapshot=self.measure,monotonic=self.clock,utc_now=utc_now,
                                            context_check=context,admission=admit_again,emit=core_emit,identity=identity)
                                    else:trial=_trial_witness(None,invoked=False,reason='PRETRIAL_CONTEXT_CHANGED')
                                except BaseException:trial=_trial_witness(None,invoked=True,reason='RESIDENT_PROBE_ERROR')
                                after=self._process_snapshot();stable=ready and context()
                                result=dict(status='MEASURED' if stable else 'WATCH_ACTIVITY_CHANGED',trial=trial,
                                    process_before=before,process_after=after,pymalloc_before=emitted,admission_result=admission_result,
                                    admission_sample=admission_result['samples'][-1]['sample'],watch_activity_revision_after=revision if stable else None)
                    result.update(observation_index=target,origin_cycle_id=selected_cycle,
                        fixed_nominal_slot_utc=FIXED_NOMINAL_SLOT_UTC,tail_sample_completed_at_utc=last.get('sample_completed_at_utc'),
                        scheduled_delay_seconds={3:120,5:600}[target],watch_activity_revision_before=tail.get('activity_revision'),
                        scope='ONE_FIXED_RESIDENT_TRIM_RESPONSE_NOT_EXACT_FREE_PAGE_CENSUS_OR_CAUSAL_RSS_SAVING')
                    try:self._record('RESIDENT_PROBE',result)
                    except BaseException:self._record('RESIDENT_PROBE',dict(status='PROBE_RECORD_UNAVAILABLE',trial=_trial_witness(dict.get(result,'trial'),invoked=trial_invoked,reason='PROBE_RECORD_BOUND_OR_SERIALIZATION'),observation_index=target,origin_cycle_id=selected_cycle))
                    self.last_probe_finished=self.clock()
            if cycle is not None and cycle!=selected_cycle:
                for target in PROBE_INDICES:
                    if target not in self.completed_indices:
                        self.completed_indices.add(target)
                        self._record('RESIDENT_PROBE',dict(status='PREEMPTED_BY_LATER_CYCLE',trial=None,observation_index=target,origin_cycle_id=selected_cycle))
                        self.last_probe_finished=self.clock()
        if self.completed_indices==set(PROBE_INDICES) and self.last_probe_finished is not None and self.clock()-self.last_probe_finished>=60:
            with self.lock:self.state='COMPLETED'
            self._record('COMPLETED',{'probe_indices':list(PROBE_INDICES)});return False
        return True

    def _run(self):
        while True:
            try:
                if not self.tick():
                    return
            except BaseException:
                with self.lock:
                    self.errors += 1
                # Bounded error recovery never repeats an expensive cache walk.
                if self.errors >= 3:
                    with self.lock:
                        self.state = 'EXPIRED'
                    return
            time.sleep(POLL_SECONDS)

    def status(self):
        with self.lock:
            return _json_copy(dict(version=VERSION, state=self.state, session=self.session,
                runtime_commit=self.commit, expires_at_utc=EXPIRES_AT_UTC,
                source_code_sha256=self.pins, selected_cycle=self.selected,
                probes=self.probes, pulse_count=len(self.pulses),
                # All pulses are small aggregates; the health handler does no probes.
                pulses=list(self.pulses), errors=self.errors,
                limits=dict(max_self_rss_bytes=MAX_SELF_RSS_BYTES,
                    max_pulses=MAX_PULSES, max_duration_seconds=MAX_DURATION_SECONDS,
                    pulse_seconds=PULSE_SECONDS, probe_indices=list(PROBE_INDICES)),
                fixed_nominal_slot_utc=FIXED_NOMINAL_SLOT_UTC,
                scope='ONE_FIXED_TRIM_RESPONSE_NOT_EXACT_FREE_RESIDENT_BYTES_OR_CAUSAL_RSS_PARTITION'), limit=2 * 1024 * 1024)


def install():
    global _INSTANCE,_INSTALL_STATE
    try:
        with _INSTALL_LOCK:
            if _INSTANCE is not None or _INSTALL_STATE is not None:return
            if remaining_seconds()<=0:_INSTALL_STATE='EXPIRED';return
            relation=_slot_relation(utc_now())
            if relation is None:_INSTALL_STATE='INSTALL_CONTEXT_UNAVAILABLE';return
            if relation>=0:_INSTALL_STATE='MISSED_FIXED_SLOT';return
            candidate=Investigation(_observed_watch_status,duration=remaining_seconds(),pins=_pins())
            candidate.thread=threading.Thread(target=candidate._run,name='resident-memory-investigation',daemon=True)
            _INSTANCE=candidate;candidate.thread.start()
    except BaseException:
        _INSTALL_STATE='INSTALL_UNAVAILABLE'


def status():
    if _INSTANCE is None:
        return dict(version=VERSION, state=_INSTALL_STATE or 'INACTIVE', expires_at_utc=EXPIRES_AT_UTC, fixed_nominal_slot_utc=FIXED_NOMINAL_SLOT_UTC)
    try:
        return _INSTANCE.status()
    except BaseException:
        return dict(version=VERSION, state='STATUS_UNAVAILABLE', expires_at_utc=EXPIRES_AT_UTC)
