"""One fixed natural-cycle rebound window, followed by separate bounded graphs.

Health reads return cached primitives. Imports do no I/O or thread startup.
No trim, GC, cache clear, native schedule change or replacement cycle.
"""
from __future__ import annotations

from collections import deque
from datetime import datetime, timezone, timedelta
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
import threading
import time
from types import BuiltinFunctionType, ModuleType
from uuid import uuid4

import research_rebound_memory
import research_current_history_memory
import research_retained_memory_accounting_v2
import research_process_memory
import research_runtime_memory

VERSION = 'rebound-memory-investigation-v1'
FIXED_NOMINAL_SLOT_UTC = '2026-11-01T17:32:15+00:00'
EXPIRES_AT_UTC = '2026-11-01T22:00:00+00:00'
MAX_DURATION_SECONDS = 5400
MAX_PULSES = 240
PULSE_SECONDS = 15
POLL_SECONDS = 5
MAX_RECORD_BYTES = 262144
MAX_SELF_RSS_BYTES = 1024 * 1024 * 1024
MAX_ADMISSION_SAMPLES = 3
MAX_ADMISSION_SECONDS = 2.5
OPERATION_START_SECONDS = 540
OPERATION_STOP_SECONDS = 840
SHELL_SCHEDULE = (('SHELL_600', 600), ('SHELL_660', 660), ('SHELL_720', 720), ('SHELL_780', 780))
GRAPH_SCHEDULE = (('H4_900', 900), ('ROOT32_1020', 1020))
ENABLE_ROOT32_GRAPH = True  # Immutable prepublication 64MiB/400k/8s feasibility PASS.
TRANSIENT_COVERAGE_ERRORS = frozenset({'TIME_BUDGET', 'ANCESTOR_UNVERIFIED'})
SOURCE_FILES = ('main.py', 'research_rebound_memory_investigation.py', 'research_rebound_memory.py',
    'research_current_history_memory.py', 'research_retained_memory_accounting_v2.py',
    'research_process_memory.py', 'research_runtime_memory.py', 'research_watch_runtime.py')
_INSTANCE = None
_INSTALL_STATE = None
_INSTALL_LOCK = threading.Lock()


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def remaining_seconds(now=None):
    now = now or datetime.now(timezone.utc)
    return max(0., min(MAX_DURATION_SECONDS, (datetime.fromisoformat(EXPIRES_AT_UTC) - now).total_seconds()))


def _json_copy(value, limit=MAX_RECORD_BYTES):
    raw = json.dumps(value, separators=(',', ':'), allow_nan=False)
    if len(raw.encode('utf-8')) > limit:
        raise ValueError('RECORD_BOUND')
    return json.loads(raw)


def _clock(value):
    if type(value) is not str or len(value) > 80:
        raise ValueError('CLOCK')
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.utcoffset() is None:
        raise ValueError('AWARE_CLOCK')
    return parsed


def _slot_relation(value):
    try:
        stamp, fixed = _clock(value), _clock(FIXED_NOMINAL_SLOT_UTC)
        return -1 if stamp < fixed else 1 if stamp > fixed else 0
    except (ValueError, TypeError, OverflowError):
        return None


def _cycle_in_fixed_slot(cycle):
    if type(cycle) is not str or not cycle.startswith('shared-watch:') or len(cycle) > 160:
        return False
    try:
        fixed = _clock(FIXED_NOMINAL_SLOT_UTC)
        return fixed <= _clock(cycle.removeprefix('shared-watch:')) < fixed + timedelta(minutes=30)
    except (ValueError, TypeError, OverflowError):
        return False


def _native_slot():
    for name in ('__main__', 'main'):
        module = sys.modules.get(name)
        if type(module) is not ModuleType:
            continue
        runtime = dict.get(module.__dict__, 'WATCH_RUNTIME')
        if type(runtime) is dict:
            value = dict.get(runtime, 'last_claimed_slot_utc')
            return value if _slot_relation(value) is not None else None
    return None


def _observed_watch_status():
    """Copy one original TailPlan's scalar anchor under its existing lock.

    Never infer cleanup UTC by subtracting monotonic delays; the two source
    clocks are retained separately. No live references escape this adapter.
    """
    module = sys.modules.get('research_watch_runtime')
    if type(module) is not ModuleType:
        return dict(nominal_watch_slot_utc=_native_slot(), cleanup_anchor=dict(status='NOT_LOADED'))
    namespace = module.__dict__
    status_function = dict.get(namespace, 'status')
    anchor = dict(status='UNAVAILABLE')
    condition = dict.get(namespace, '_CONDITION')
    acquired = False
    try:
        before = status_function()
        if type(before) is not dict:
            raise ValueError('PUBLIC_STATUS')
        if type(condition) is not threading.Condition or not condition.acquire(timeout=.005):
            anchor = dict(status='LOCK_UNAVAILABLE')
        else:
            acquired = True
            registered = dict.get(namespace, '_TailPlan')
            plan = dict.get(namespace, '_TAIL_PLAN')
            if type(registered) is not type or type(plan) is not registered:
                anchor = dict(status='ORIGINAL_PLAN_NOT_AVAILABLE')
            else:
                values = object.__getattribute__(plan, '__dict__')
                identity = dict.get(values, 'identity')
                mono, revision = dict.get(values, 'anchor'), dict.get(values, 'activity_revision')
                if type(identity) is not dict or type(mono) not in (float, int) or not math.isfinite(mono) or mono < 0 or type(revision) is not int or revision < 0:
                    raise ValueError('ANCHOR_TYPES')
                cycle, session, cleanup = (dict.get(identity, key) for key in ('origin_cycle_id', 'process_session_id', 'cleanup_tail_at_utc'))
                if not _cycle_in_fixed_slot(cycle) or type(session) is not str or not re.fullmatch('[0-9a-f]{32}', session):
                    raise ValueError('ANCHOR_IDENTITY')
                _clock(cleanup)
                if type(dict.get(namespace, '_ACTIVE')) is not dict or len(dict.get(namespace, '_ACTIVE')) or dict.get(namespace, '_ACTIVITY_REVISION') != revision:
                    raise ValueError('ANCHOR_ACTIVITY')
                anchor = dict(status='CAPTURED', origin_cycle_id=cycle, watch_session=session,
                    cleanup_tail_at_utc=cleanup, cleanup_monotonic= float(mono), activity_revision=revision,
                    scope='SOURCE_CLEANUP_TAIL_NOT_COROUTINE_RETURN_OR_GLOBAL_IDLE')
    except BaseException:
        before = {}
        anchor = dict(status='UNAVAILABLE')
    finally:
        plan = values = identity = registered = None
        if acquired:
            condition.release()
    try:
        after = status_function()
        if type(after) is not dict:
            raise ValueError('PUBLIC_STATUS')
        if anchor['status'] == 'CAPTURED':
            for current in (before, after):
                tail = current.get('post_cleanup_followups') or {}
                if (current.get('active_cycle_count') != 0 or type(current.get('active_cycle_count')) is not int or
                        tail.get('pending_origin_cycle_id') != anchor['origin_cycle_id'] or
                        tail.get('activity_revision') != anchor['activity_revision'] or
                        (current.get('journal') or {}).get('process_session_id') != anchor['watch_session']):
                    anchor = dict(status='PUBLIC_CONTEXT_CHANGED')
                    break
        return {**after, 'nominal_watch_slot_utc': _native_slot(), 'cleanup_anchor': anchor}
    except BaseException:
        return dict(nominal_watch_slot_utc=_native_slot(), cleanup_anchor=dict(status='PUBLIC_STATUS_UNAVAILABLE'))


def _pins():
    result = {}
    for name in SOURCE_FILES:
        raw = (Path(__file__).resolve().parent / name).read_bytes()
        if len(raw) > 2 * 1024 * 1024:
            raise ValueError('SOURCE_BOUND')
        result[name] = hashlib.sha256(raw).hexdigest()
    return result


def emit_allocator_stats(identity, *, sample_label, implementation=None, version=None,
        debug_build=None, function=None, write=None, platform_name=None):
    """Two source-guarded aggregate readings, never an allocator intervention."""
    if sample_label not in ('WINDOW_BEGIN_600','WINDOW_END_780'):
        return dict(status='UNAVAILABLE',reason='INVALID_SAMPLE_LABEL')
    implementation = sys.implementation.name if implementation is None else implementation
    version = tuple(sys.version_info[:3]) if version is None else version
    debug_build = hasattr(sys,'gettotalrefcount') if debug_build is None else debug_build
    function = getattr(sys,'_debugmallocstats',None) if function is None else function
    platform_name = sys.platform if platform_name is None else platform_name
    if platform_name!='linux' or implementation!='cpython' or version!=(3,11,9) or debug_build or type(function) is not BuiltinFunctionType:
        return dict(status='UNAVAILABLE',reason='EXPECTED_RELEASE_CPYTHON3119_API_UNAVAILABLE')
    write = os.write if write is None else write
    began = utc_now()
    def marker(phase):
        raw = ('[REBOUND_ALLOCATOR_V1] '+json.dumps(dict(phase=phase,identity=identity,sample_label=sample_label,at_utc=utc_now()),separators=(',',':'),allow_nan=False)+'\n').encode()
        if len(raw)>4096 or write(2,raw)!=len(raw):raise ValueError('MARKER_WRITE_BOUND')
    try:
        marker('BEGIN');function();marker('END')
        return dict(status='EMITTED_NOT_YET_INGESTED',sample_label=sample_label,started_at_utc=began,completed_at_utc=utc_now(),
            format='CPYTHON_3_11_9_RELEASE_DEBUGMALLOCSTATS',scope='ALLOCATOR_CAPACITIES_NOT_RSS_OWNERSHIP_PHYSICAL_FREE_OR_CAUSAL_EFFECT')
    except BaseException:
        return dict(status='UNAVAILABLE',reason='AGGREGATE_EMISSION_FAILED',sample_label=sample_label)


class Investigation:
    def __init__(self, watch_status, *, duration, measure=None, shell_measure=None,
            history_measure=None, root32_measure=None, admission_measure=None,
            operation_install=None, operation_stop=None, operation_status=None, allocator_emit=None,
            clock=time.monotonic, sink=None, pins=None):
        self.watch_status = watch_status
        self.measure = measure or (lambda: research_process_memory.sample(include_mallinfo2=True))
        self.shell_measure = shell_measure or research_rebound_memory.capture_shells_and_operations
        self.history_measure = history_measure or research_current_history_memory.capture_live_roots_and_measure
        self.root32_measure = root32_measure or research_retained_memory_accounting_v2.capture_live_roots_and_measure
        self.admission_measure = admission_measure or research_runtime_memory.sample_memory
        self.operation_install = operation_install or research_rebound_memory.install_operation_observer
        self.operation_stop = operation_stop or research_rebound_memory.stop_operation_observer
        self.operation_status = operation_status or research_rebound_memory.operation_status
        self.allocator_emit = allocator_emit or emit_allocator_stats
        self.clock = clock
        self.deadline = clock() + max(0., min(duration, MAX_DURATION_SECONDS))
        self.sink = sink or (lambda value: print('[REBOUND_MEMORY_V1] ' + json.dumps(value, separators=(',', ':'), allow_nan=False), flush=True))
        self.lock = threading.Lock()
        self.session = uuid4().hex
        commit = os.getenv('RENDER_GIT_COMMIT', '')
        self.commit = commit if re.fullmatch('[0-9a-f]{40}', commit) else None
        self.pins = pins or {}
        self.pulses = deque(maxlen=MAX_PULSES)
        self.snapshots, self.probes, self.operation_events = [], [], []
        self.selected = None
        self.reserved = set()
        self.operation_install_attempted = False
        self.operation_stop_attempted = False
        self.operation_final_capture_attempted = False
        self.operation_final_status = None
        self.allocator_emission_attempts = set()
        self.heavy_observer_started_at_utc = None
        self.last_pulse = float('-inf')
        self.last_probe_finished = None
        self.sequence, self.errors = 0, 0
        self.state = 'WAITING_FIXED_NATURAL_CLEANUP'
        self.thread = None

    def _record(self, kind, payload):
        with self.lock:
            self.sequence += 1
            event = _json_copy(dict(version=VERSION, session=self.session, runtime_commit=self.commit,
                sequence=self.sequence, at_utc=utc_now(), kind=kind, payload=payload))
            if kind == 'PULSE': self.pulses.append(event)
            elif kind == 'SHELL': self.snapshots.append(event)
            elif kind == 'POST_REBOUND_GRAPH': self.probes.append(event)
            elif kind.startswith('OPERATION_'): self.operation_events.append(event)
        try:
            self.sink(event)
        except BaseException:
            with self.lock: self.errors += 1

    def _process_snapshot(self):
        try:
            value = _json_copy(self.measure(), 16384)
            if type(value) is dict: return value
        except BaseException:
            pass
        return dict(status='UNAVAILABLE', rss_bytes=None, reason='PROCESS_SNAPSHOT_ERROR')

    def _context_matches(self, *, require_tail=False):
        try:
            if self.selected is None or self.clock() >= self.deadline: return False
            current = self.watch_status()
            tail = current.get('post_cleanup_followups') or {}
            last = tail.get('last_sample') or {}
            pending = tail.get('pending_origin_cycle_id')
            same_cycle = (pending == self.selected['origin_cycle_id'] or pending is None and
                last.get('origin_cycle_id') == self.selected['origin_cycle_id'] and
                type(last.get('observation_index')) is int and 2 <= last['observation_index'] <= 5)
            same = (type(current.get('active_cycle_count')) is int and current['active_cycle_count'] == 0 and
                same_cycle and
                type(tail.get('activity_revision')) is int and tail['activity_revision'] == self.selected['activity_revision'] and
                (current.get('journal') or {}).get('process_session_id') == self.selected['watch_session'] and
                _slot_relation(current.get('nominal_watch_slot_utc')) == 0)
            if require_tail:
                last = tail.get('last_sample') or {}
                durable = (tail.get('persistence') or {}).get('last_durable') or {}
                same = (same and type(last.get('observation_index')) is int and last['observation_index'] == 5 and last.get('origin_cycle_id') == self.selected['origin_cycle_id'] and
                    durable.get('durable') is True and durable.get('persisted') is True and
                    type(durable.get('observation_index')) is int and durable['observation_index']==5 and
                    durable.get('origin_cycle_id')==self.selected['origin_cycle_id'] and
                    durable.get('process_session_id')==self.selected['watch_session'] and
                    durable.get('observation_kind')=='POST_CLEANUP_MEMORY')
            return same
        except BaseException:
            return False

    def _fresh_admission(self, cached):
        began = self.clock()
        result = dict(version='fresh-retained-root-admission-v1', status='SKIPPED', reason='NOT_ADMITTED',
            samples=[], selected_sample_index=None, cached_tail_memory=_json_copy(cached, 8192),
            max_samples=MAX_ADMISSION_SAMPLES, budget_seconds=MAX_ADMISSION_SECONDS,
            scope='FRESH_COMPLETE_SAFETY_ONLY_CACHED_PARTIAL_NOT_RELABELLED')
        def done(reason):
            result.update(reason=reason, elapsed_ms=round(max(0., self.clock()-began)*1000, 3))
            return result
        def unsafe(sample):
            children = sample.get('descendants') or {}
            for group in ('browser_named', 'other'):
                count = (children.get(group) or {}).get('process_count')
                if type(count) is int and count > 0: return 'POSITIVE_DESCENDANT'
            rss = (sample.get('self') or {}).get('rss_bytes')
            if type(rss) is int and (rss < 0 or rss > MAX_SELF_RSS_BYTES): return 'SELF_RSS_GUARD'
            cg = sample.get('cgroup') or {}; current, maximum = cg.get('current_bytes'), cg.get('max_bytes')
            if type(current) is int and type(maximum) is int and (current < 0 or maximum <= 0 or maximum-current < 512*1024*1024): return 'CGROUP_HEADROOM_GUARD'
            return None
        veto = unsafe(cached)
        if veto: return done('CACHED_'+veto)
        for index in range(1, MAX_ADMISSION_SAMPLES+1):
            if not self._context_matches(require_tail=True): return done('WATCH_CONTEXT_CHANGED')
            if self.clock()-began >= MAX_ADMISSION_SECONDS: return done('ADMISSION_TIME_BOUND')
            before, stamp = self.clock(), utc_now()
            try:
                sample = _json_copy(self.admission_measure(), 8192)
                if type(sample) is not dict:raise ValueError('SAMPLE_TYPE')
            except BaseException:
                result['samples'].append(dict(index=index, started_at_utc=stamp, completed_at_utc=utc_now(), elapsed_ms=round(max(0.,self.clock()-before)*1000,3), sample=None, decision='ADMISSION_EXCEPTION'))
                return done('ADMISSION_EXCEPTION')
            entry = dict(index=index, started_at_utc=stamp, completed_at_utc=utc_now(), elapsed_ms=round(max(0.,self.clock()-before)*1000,3), sample=sample, decision='VETO')
            result['samples'].append(entry)
            veto = unsafe(sample)
            if veto: entry['decision']=veto; return done(veto)
            if not self._context_matches(require_tail=True): entry['decision']='WATCH_CONTEXT_CHANGED'; return done('WATCH_CONTEXT_CHANGED')
            if self.clock()-began >= MAX_ADMISSION_SECONDS: entry['decision']='ADMISSION_TIME_BOUND'; return done('ADMISSION_TIME_BOUND')
            children = sample.get('descendants') or {}; cg = sample.get('cgroup') or {}
            rss, current, maximum = (sample.get('self') or {}).get('rss_bytes'), cg.get('current_bytes'), cg.get('max_bytes')
            safe = (sample.get('status') == 'COMPLETE' and type(rss) is int and 0 <= rss <= MAX_SELF_RSS_BYTES and
                all(type((children.get(group) or {}).get('process_count')) is int and (children[group]['process_count']) == 0 for group in ('browser_named','other')) and
                type(current) is int and type(maximum) is int and current >= 0 and maximum-current >= 512*1024*1024)
            if safe:
                entry['decision']='ADMIT'; result.update(status='ADMITTED', selected_sample_index=index)
                return done('FRESH_COMPLETE_SAFE')
            errors = sample.get('errors') or {}
            transient = sample.get('status') in ('PARTIAL','UNAVAILABLE') and type(errors) is dict and bool(errors) and set(errors) <= TRANSIENT_COVERAGE_ERRORS
            if not transient: entry['decision']='NONTRANSIENT_COVERAGE'; return done('NONTRANSIENT_COVERAGE')
            if index == MAX_ADMISSION_SAMPLES: entry['decision']='SAMPLE_LIMIT'; return done('SAMPLE_LIMIT')
            entry['decision']='RETRY_TRANSIENT_COVERAGE'
        return done('SAMPLE_LIMIT')

    def _stop_operations(self, reason):
        if not self.operation_install_attempted or self.operation_stop_attempted: return
        self.operation_stop_attempted = True
        try: result = _json_copy(self.operation_stop(), 32768)
        except BaseException: result = dict(status='UNAVAILABLE', reason='OPERATION_STOP_ERROR')
        self._record('OPERATION_STOPPED', dict(reason=reason, observer=result))

    def _final_operations(self, reason):
        if not self.operation_install_attempted or self.operation_final_capture_attempted:return
        self.operation_final_capture_attempted=True
        try:result=_json_copy(self.operation_status(),16384)
        except BaseException:result=dict(status='UNAVAILABLE',reason='FINAL_OPERATION_STATUS_ERROR')
        self.operation_final_status=dict(captured_at_utc=utc_now(),reason=reason,observer=result,
            scope='CACHED_COUNTERS_AS_OF_READ_INFLIGHT_ENDS_MAY_STILL_BE_UNKNOWN_NOT_ALL_WORK_FINISHED_OR_COMPLETE_CAUSAL_COVERAGE')
        self._record('OPERATION_FINAL_STATUS',self.operation_final_status)

    def _shell(self, measurement_id, delay, age):
        self.reserved.add(measurement_id)
        capture_started = self.clock()-self.selected['cleanup_monotonic']
        base = dict(measurement_id=measurement_id, scheduled_delay_seconds=delay, observed_delay_ms=age*1000,
            original_native_tail_index=5, origin_cycle_id=self.selected['origin_cycle_id'],
            observer_perturbed=False, eligible_for_rebound_comparison=False,
            nominal_window_endpoint=measurement_id=='SHELL_780',capture_started_delay_ms=capture_started*1000)
        if not self._context_matches():
            base.update(status='SKIPPED_WATCH_CONTEXT_CHANGED', shell=None)
        else:
            before = self._process_snapshot()
            emitted = None
            if measurement_id=='SHELL_600':emitted=self._emit_window('WINDOW_BEGIN_600',measurement_id,before)
            try: shell = _json_copy(self.shell_measure(monotonic=self.clock, utc_now=utc_now), 32768)
            except BaseException: shell = dict(status='UNAVAILABLE', reason='CHEAP_SNAPSHOT_ERROR')
            after = self._process_snapshot()
            stable = self._context_matches()
            capture_completed=self.clock()-self.selected['cleanup_monotonic']
            inside_actual_window=600<=capture_started<=capture_completed<=780
            if measurement_id=='SHELL_780':emitted=self._emit_window('WINDOW_END_780',measurement_id,after)
            base.update(status='MEASURED' if stable else 'WATCH_ACTIVITY_CHANGED', shell=shell,
                process_before=before, process_after=after, eligible_for_rebound_comparison=stable and inside_actual_window,
                within_actual_rebound_window=inside_actual_window,capture_completed_delay_ms=capture_completed*1000,
                allocator_emission=emitted,
                scope='SHALLOW_HEADERS_AND_PROCESS_GAUGES_NOT_DEEP_UNION_OR_ALLOCATION_OWNERSHIP')
        try:self._record('SHELL', base)
        except BaseException:
            self._record('SHELL',dict(measurement_id=measurement_id,original_native_tail_index=5,
                origin_cycle_id=self.selected['origin_cycle_id'],status='RECORD_UNAVAILABLE_ATTEMPT_NOT_REPEATED',
                shell=None,eligible_for_rebound_comparison=False))

    def _emit_window(self,label,measurement_id,process):
        if label in self.allocator_emission_attempts:return dict(status='UNAVAILABLE',reason='EMISSION_ALREADY_RESERVED')
        self.allocator_emission_attempts.add(label)
        rss=process.get('rss_bytes')
        if not self._context_matches() or type(rss) is not int or not 0<=rss<=MAX_SELF_RSS_BYTES:
            return dict(status='UNAVAILABLE',reason='WINDOW_CONTEXT_OR_RSS_GUARD')
        identity=dict(session=self.session,runtime_commit=self.commit,origin_cycle_id=self.selected['origin_cycle_id'],
            measurement_id=measurement_id,original_native_tail_index=5)
        try:return _json_copy(self.allocator_emit(identity,sample_label=label),8192)
        except BaseException:return dict(status='UNAVAILABLE',reason='AGGREGATE_EMISSION_FAILED')

    def _graph(self, measurement_id, delay, age, watch):
        self.reserved.add(measurement_id)
        self.last_probe_finished = self.clock()
        tail = watch.get('post_cleanup_followups') or {}
        last = tail.get('last_sample') or {}
        durable = (tail.get('persistence') or {}).get('last_durable') or {}
        base = dict(measurement_id=measurement_id, scheduled_delay_seconds=delay, observed_delay_ms=age*1000,
            original_native_tail_index=5, origin_cycle_id=self.selected['origin_cycle_id'],
            observer_perturbed=True, eligible_for_rebound_comparison=False,
            original_tail_context={key:last.get(key) for key in ('origin_cycle_id','observation_index','scheduled_delay_seconds',
                'observed_delay_ms','sample_started_at_utc','sample_completed_at_utc','watch_active_before','watch_active_after',
                'watch_activity_revision_before','watch_activity_revision_after')},
            original_tail_durable={key:durable.get(key) for key in ('origin_cycle_id','process_session_id','observation_index',
                'observation_kind','payload_sha256','durable','persisted','created_at_utc','at_utc')})
        if measurement_id == 'ROOT32_1020' and not ENABLE_ROOT32_GRAPH:
            base.update(status='DISABLED_NOT_FEASIBILITY_APPROVED', cache=None)
        elif not self._context_matches(require_tail=True):
            base.update(status='SKIPPED_WATCH_CONTEXT_CHANGED', cache=None)
        else:
            before = self._process_snapshot(); rss = before.get('rss_bytes')
            if type(rss) is not int or not 0 <= rss <= MAX_SELF_RSS_BYTES:
                base.update(status='SKIPPED_SELF_RSS_GUARD', cache=None, process_before=before)
            else:
                admitted = self._fresh_admission(last.get('memory') or {})
                base.update(process_before=before, admission_result=admitted)
                if admitted['status'] != 'ADMITTED':
                    base.update(status='SKIPPED_FRESH_PROCESS_OR_CGROUP_GUARD', cache=None)
                elif not self._context_matches(require_tail=True):
                    base.update(status='WATCH_ACTIVITY_CHANGED', cache=None)
                else:
                    if self.heavy_observer_started_at_utc is None: self.heavy_observer_started_at_utc = utc_now()
                    options = dict(max_nodes=4_000_000, max_workspace_bytes=256*1024*1024, max_elapsed_seconds=30.) if measurement_id == 'H4_900' else dict(max_nodes=400_000, max_workspace_bytes=64*1024*1024, max_elapsed_seconds=8.)
                    core = self.history_measure if measurement_id == 'H4_900' else self.root32_measure
                    try:
                        cache = _json_copy(core(**options))
                        if type(cache) is not dict:raise ValueError('GRAPH_TYPE')
                    except BaseException: cache = dict(status='UNAVAILABLE', reason_codes=['POST_REBOUND_GRAPH_ERROR'])
                    after = self._process_snapshot(); stable = self._context_matches(require_tail=True)
                    base.update(status='MEASURED' if stable else 'WATCH_ACTIVITY_CHANGED', cache=cache,
                        process_after=after, effective_options=options,
                        scope='POST_REBOUND_OBSERVER_PERTURBED_SUPPORTED_GRAPH_NOT_SIMULTANEOUS36_UNION_OR_RSS_OWNERSHIP')
        try: self._record('POST_REBOUND_GRAPH', base)
        except BaseException:
            self._record('POST_REBOUND_GRAPH', dict(measurement_id=measurement_id, original_native_tail_index=5,
                origin_cycle_id=self.selected['origin_cycle_id'], status='RECORD_UNAVAILABLE_ATTEMPT_NOT_REPEATED', cache=None,
                graph_attempt_reserved=True, observer_perturbed=True, eligible_for_rebound_comparison=False))
        self.last_probe_finished = self.clock()

    def tick(self):
        now = self.clock()
        if self.state in ('COMPLETED','EXPIRED','PULSE_LIMIT','MISSED_FIXED_SLOT','MISSING_ORIGINAL_CLEANUP_ANCHOR'): return False
        if now >= self.deadline:
            self._stop_operations('DEADLINE'); self._final_operations('DEADLINE'); self.state='EXPIRED'; return False
        watch = self.watch_status(); tail = watch.get('post_cleanup_followups') or {}
        relation = _slot_relation(watch.get('nominal_watch_slot_utc'))
        if self.selected is None and relation == 1:
            self.state='MISSED_FIXED_SLOT'; self._record('MISSED_FIXED_SLOT',dict(fixed_nominal_slot_utc=FIXED_NOMINAL_SLOT_UTC)); return False
        if self.selected is None and relation == 0:
            anchor = watch.get('cleanup_anchor') or {}
            if anchor.get('status') == 'CAPTURED' and _cycle_in_fixed_slot(anchor.get('origin_cycle_id')):
                try:cleanup_clock_valid=_clock(anchor.get('cleanup_tail_at_utc')) is not None
                except (ValueError,TypeError,OverflowError):cleanup_clock_valid=False
                if (type(anchor.get('cleanup_monotonic')) not in (int,float) or not math.isfinite(anchor['cleanup_monotonic']) or
                        anchor['cleanup_monotonic']<0 or anchor['cleanup_monotonic'] > now or not cleanup_clock_valid or
                        type(anchor.get('watch_session')) is not str or not re.fullmatch('[0-9a-f]{32}',anchor['watch_session']) or
                        type(anchor.get('activity_revision')) is not int or anchor['activity_revision']<0 or
                        anchor['activity_revision']!=tail.get('activity_revision') or
                        anchor['watch_session']!=(watch.get('journal') or {}).get('process_session_id')):
                    self.state='MISSING_ORIGINAL_CLEANUP_ANCHOR'; return False
                self.selected = {key: anchor[key] for key in ('origin_cycle_id','watch_session','cleanup_tail_at_utc','cleanup_monotonic','activity_revision')}
                self.selected.update(selected_at_utc=utc_now(), fixed_nominal_slot_utc=FIXED_NOMINAL_SLOT_UTC,
                    rule='FIXED_PREDECLARED_SLOT_ORIGINAL_SCALAR_CLEANUP_ANCHOR_NO_OUTCOME_FILTER')
                self.state='OBSERVING_FIXED_SELECTED_CYCLE'; self._record('SELECTED',self.selected)
            elif (tail.get('last_sample') or {}).get('observation_index') == 5 and _cycle_in_fixed_slot((tail.get('last_sample') or {}).get('origin_cycle_id')):
                self.state='MISSING_ORIGINAL_CLEANUP_ANCHOR'; self._record('STOPPED',dict(reason=self.state)); return False
        age = now-self.selected['cleanup_monotonic'] if self.selected is not None else None
        if now-self.last_pulse >= PULSE_SECONDS:
            if len(self.pulses) >= MAX_PULSES:
                self._stop_operations('PULSE_LIMIT'); self._final_operations('PULSE_LIMIT'); self.state='PULSE_LIMIT'; return False
            pulse_started=self.clock()-self.selected['cleanup_monotonic'] if self.selected is not None else None
            pulse_process=self._process_snapshot()
            pulse_completed=self.clock()-self.selected['cleanup_monotonic'] if self.selected is not None else None
            inside_actual_window=pulse_started is not None and 600<=pulse_started<=pulse_completed<=780
            self._record('PULSE',dict(process=pulse_process, watch_active_count=watch.get('active_cycle_count'),
                watch_activity_revision=tail.get('activity_revision'), nominal_watch_slot_utc=watch.get('nominal_watch_slot_utc'),
                cleanup_relative_seconds=age, observer_perturbed=self.heavy_observer_started_at_utc is not None,
                capture_started_delay_ms=pulse_started*1000 if pulse_started is not None else None,
                capture_completed_delay_ms=pulse_completed*1000 if pulse_completed is not None else None,
                within_actual_rebound_window=inside_actual_window,
                observer_flag_scope='GRAPH_WALK_ONLY_CHEAP_OBSERVERS_AND_AGGREGATE_READINGS_REMAIN_PRESENT',
                eligible_for_rebound_comparison=inside_actual_window and self.heavy_observer_started_at_utc is None and self._context_matches()))
            self.last_pulse=self.clock()
        if self.selected is None: return True
        if not self._context_matches():
            self._stop_operations('WATCH_CONTEXT_CHANGED')
            for measurement_id, delay in SHELL_SCHEDULE + GRAPH_SCHEDULE:
                if measurement_id not in self.reserved:
                    self.reserved.add(measurement_id)
                    self._record('SHELL' if measurement_id.startswith('SHELL') else 'POST_REBOUND_GRAPH', dict(measurement_id=measurement_id, scheduled_delay_seconds=delay, original_native_tail_index=5, origin_cycle_id=self.selected['origin_cycle_id'],status='PREEMPTED_BY_WATCH_CONTEXT_CHANGE'))
            self.last_probe_finished=self.clock() if self.last_probe_finished is None else self.last_probe_finished
        else:
            if age >= OPERATION_START_SECONDS and not self.operation_install_attempted:
                self.operation_install_attempted=True
                if age >= 600: result=dict(status='MISSED_OPERATION_WINDOW_PREFIX')
                else:
                    try: result=_json_copy(self.operation_install(deadline_monotonic=min(self.deadline,self.selected['cleanup_monotonic']+OPERATION_STOP_SECONDS),monotonic=self.clock,utc_now=utc_now),32768)
                    except BaseException: result=dict(status='UNAVAILABLE',reason='OPERATION_INSTALL_ERROR')
                self._record('OPERATION_STARTED',dict(observed_delay_ms=age*1000,observer=result))
            if age >= OPERATION_STOP_SECONDS: self._stop_operations('FIXED_WINDOW_END')
            schedule = SHELL_SCHEDULE + GRAPH_SCHEDULE
            for pos, (measurement_id, delay) in enumerate(schedule):
                if measurement_id in self.reserved or age < delay: continue
                next_delay = SHELL_SCHEDULE[pos+1][1] if pos < len(SHELL_SCHEDULE)-1 else 840 if measurement_id=='SHELL_780' else 1020 if measurement_id=='H4_900' else None
                if next_delay is not None and age >= next_delay:
                    self.reserved.add(measurement_id)
                    self._record('SHELL' if measurement_id.startswith('SHELL') else 'POST_REBOUND_GRAPH',dict(measurement_id=measurement_id,scheduled_delay_seconds=delay,observed_delay_ms=age*1000,original_native_tail_index=5,origin_cycle_id=self.selected['origin_cycle_id'],status='MISSED_FIXED_MEASUREMENT_WINDOW'))
                    if not measurement_id.startswith('SHELL'):self.last_probe_finished=self.clock()
                elif measurement_id.startswith('SHELL'): self._shell(measurement_id,delay,age)
                else: self._graph(measurement_id,delay,age,watch)
        if len(self.reserved)==len(SHELL_SCHEDULE)+len(GRAPH_SCHEDULE) and self.last_probe_finished is not None and self.clock()-self.last_probe_finished>=60:
            self._stop_operations('COMPLETED'); self._final_operations('COMPLETED'); self.state='COMPLETED'
            self._record('COMPLETED',dict(measurement_ids=sorted(self.reserved))); return False
        return True

    def _run(self):
        try:
            while True:
                try:
                    if not self.tick(): return
                except BaseException:
                    with self.lock: self.errors += 1
                    if self.errors >= 3: self.state='EXPIRED'; return
                time.sleep(POLL_SECONDS)
        finally:
            try:
                self._stop_operations('MONITOR_EXIT')
                self._final_operations('MONITOR_EXIT')
            except BaseException: pass

    def status(self):
        with self.lock:
            return _json_copy(dict(version=VERSION,state=self.state,session=self.session,runtime_commit=self.commit,
                expires_at_utc=EXPIRES_AT_UTC,fixed_nominal_slot_utc=FIXED_NOMINAL_SLOT_UTC,source_code_sha256=self.pins,
                selected_cycle=self.selected,pulse_count=len(self.pulses),pulses=list(self.pulses),snapshots=self.snapshots,
                probes=self.probes,operation_events=self.operation_events,errors=self.errors,
                operation_final_status=self.operation_final_status,
                heavy_observer_started_at_utc=self.heavy_observer_started_at_utc,
                limits=dict(max_self_rss_bytes=MAX_SELF_RSS_BYTES,max_pulses=MAX_PULSES,max_duration_seconds=MAX_DURATION_SECONDS,
                    pulse_seconds=PULSE_SECONDS,shell_delays=[600,660,720,780],operation_window_seconds=[540,840],
                    graph_delays=[900,1020],root32_enabled=ENABLE_ROOT32_GRAPH),
                scope='REBOUND600_TO780_DEEP_WALK_FREE_POST900_GRAPHS_SEPARATE_NOT_ATOMIC36_UNION_OR_RSS_OWNERSHIP'),limit=2*1024*1024)


def install():
    global _INSTANCE, _INSTALL_STATE
    try:
        with _INSTALL_LOCK:
            if _INSTANCE is not None or _INSTALL_STATE is not None: return
            if remaining_seconds() <= 0: _INSTALL_STATE='EXPIRED'; return
            relation=_slot_relation(utc_now())
            if relation is None: _INSTALL_STATE='INSTALL_CONTEXT_UNAVAILABLE'; return
            if relation >= 0: _INSTALL_STATE='MISSED_FIXED_SLOT'; return
            instance=Investigation(_observed_watch_status,duration=remaining_seconds(),pins=_pins())
            instance.thread=threading.Thread(target=instance._run,name='rebound-memory-investigation',daemon=True)
            _INSTANCE=instance; instance.thread.start()
    except BaseException:
        _INSTALL_STATE='INSTALL_UNAVAILABLE'


def status():
    if _INSTANCE is None:
        return dict(version=VERSION,state=_INSTALL_STATE or 'INACTIVE',expires_at_utc=EXPIRES_AT_UTC,fixed_nominal_slot_utc=FIXED_NOMINAL_SLOT_UTC)
    try: return _INSTANCE.status()
    except BaseException: return dict(version=VERSION,state='STATUS_UNAVAILABLE',expires_at_utc=EXPIRES_AT_UTC)
