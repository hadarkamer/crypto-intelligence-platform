"""Finite additional retained-root investigation; health reads use cached aggregates.

This is a separate corrected investigation. Earlier expired controllers remain
unchanged. It is not a rearm of the expired
background observer. No task wrappers, scheduling/formula changes, GC, trimming,
history reads, value logging, or generic heap scans. All probes run on one daemon.
"""
from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import threading
import time
from types import BuiltinFunctionType
from uuid import uuid4

import research_retained_memory_accounting as research_history_memory
import research_process_memory
import research_runtime_memory

VERSION = 'retained-root-memory-investigation-v1'
EXPIRES_AT_UTC = '2026-11-01T13:00:00+00:00'
MAX_DURATION_SECONDS = 5400
PULSE_SECONDS = 15
POLL_SECONDS = 5
MAX_PULSES = 240
MAX_RECORD_BYTES = 262144
MAX_SELF_RSS_BYTES = 1024 * 1024 * 1024
PROBE_INDICES = (3, 5)
SOURCE_FILES = ('main.py', 'research_retained_memory_investigation.py',
                'research_retained_memory_accounting.py', 'research_process_memory.py')
_INSTANCE = None
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


def _pins():
    result = {}
    for name in SOURCE_FILES:
        raw = (Path(__file__).resolve().parent / name).read_bytes()
        if len(raw) > 2 * 1024 * 1024:
            raise ValueError('SOURCE_BOUND')
        result[name] = hashlib.sha256(raw).hexdigest()
    return result


def emit_pymalloc_stats(identity, *, implementation=None, version=None,
                       debug_build=None, function=None, write=None, platform_name=None):
    """At most one release-build aggregate dump per admitted cache attempt.

    Both markers and CPython output use stderr, with no redirection or capture.
    Retrieval must reject incomplete/interleaved/unexpected tables. This reads
    allocator structures before the accounting memo exists, never object values.
    """
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
        raw = ('[RETAINED_ROOT_ALLOCATOR] ' + json.dumps(dict(phase=phase,
            identity=identity, at_utc=utc_now()), separators=(',', ':'), allow_nan=False) + '\n').encode('utf-8')
        if len(raw) > 4096 or write(2, raw) != len(raw):
            raise ValueError('MARKER_WRITE_BOUND')
    try:
        marker('BEGIN')
        function()
        marker('END')
        return dict(status='EMITTED_NOT_YET_INGESTED', started_at_utc=started,
            completed_at_utc=utc_now(), format='CPYTHON_3_11_9_RELEASE_DEBUGMALLOCSTATS',
            phase='BEFORE_THIS_CACHE_WALK_MEMO_AND_TEMPORARY_PINS',
            scope='ALLOCATOR_CAPACITY_AND_BLOCKS_NOT_CACHE_OWNERSHIP_OR_RESIDENT_PAGE_PARTITION')
    except BaseException:
        return dict(status='UNAVAILABLE', reason='AGGREGATE_EMISSION_FAILED')


class Investigation:
    def __init__(self, watch_status, *, duration, measure=None, cache_measure=None,
                 clock=time.monotonic, sink=None, pins=None, admission_measure=None,
                 allocator_emit=None):
        self.watch_status = watch_status
        self.measure = measure or (lambda: research_process_memory.sample(include_mallinfo2=True))
        self.cache_measure = cache_measure or (lambda: research_history_memory.capture_live_roots_and_measure(
            max_elapsed_seconds=min(research_history_memory.MAX_SECONDS,
                max(.001, self.deadline - self.clock()))))
        self.admission_measure = admission_measure or research_runtime_memory.sample_memory
        self.allocator_emit = allocator_emit or emit_pymalloc_stats
        self.clock = clock
        self.deadline = clock() + max(0., min(duration, MAX_DURATION_SECONDS))
        self.sink = sink or (lambda value: print('[RETAINED_ROOT_MEMORY] ' +
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
        self.state = 'WAITING_FIRST_NATURAL_CLEANUP'
        self.thread = None

    def _record(self, kind, payload):
        with self.lock:
            self.sequence += 1
            event = _json_copy(dict(version=VERSION, session=self.session,
                runtime_commit=self.commit, sequence=self.sequence, at_utc=utc_now(),
                kind=kind, payload=payload))
            if kind == 'PULSE':
                self.pulses.append(event)
            elif kind == 'CACHE_PROBE':
                self.probes.append(event)
        try:
            self.sink(event)
        except BaseException:
            with self.lock:
                self.errors += 1

    def tick(self):
        now = self.clock()
        if self.state in ('COMPLETED', 'EXPIRED', 'PULSE_LIMIT'):
            return False
        if now >= self.deadline:
            with self.lock:
                self.state = 'EXPIRED'
            self._record('EXPIRED', {'missing_probe_indices':
                [i for i in PROBE_INDICES if i not in self.completed_indices]})
            return False
        watch = self.watch_status()
        tail = watch.get('post_cleanup_followups', {})
        cycle = tail.get('pending_origin_cycle_id')
        if self.selected is None and type(cycle) is str and len(cycle) <= 160:
            selected = dict(origin_cycle_id=cycle,
                watch_session=watch.get('journal', {}).get('process_session_id'),
                activity_revision=tail.get('activity_revision'),
                selected_at_utc=utc_now(),
                rule='FIRST_CLEANUP_TAIL_SEEN_IN_THIS_PROCESS_WITHOUT_OUTCOME_OR_MEMORY_FILTER')
            with self.lock:
                self.selected = selected
                self.state = 'OBSERVING_SELECTED_CYCLE'
            self._record('SELECTED', selected)
        if now - self.last_pulse >= PULSE_SECONDS:
            with self.lock:
                pulse_count = len(self.pulses)
            if pulse_count >= MAX_PULSES:
                with self.lock:
                    self.state = 'PULSE_LIMIT'
                self._record('STOPPED', {'reason': 'PULSE_LIMIT'})
                return False
            self._record('PULSE', dict(process=self.measure(),
                watch_active_count=watch.get('active_cycle_count'),
                watch_activity_revision=tail.get('activity_revision')))
            self.last_pulse = self.clock()
        last = tail.get('last_sample') or {}
        if self.selected is not None:
            selected_cycle = self.selected['origin_cycle_id']
            index = last.get('observation_index')
            same_cycle = last.get('origin_cycle_id') == selected_cycle
            if same_cycle and type(index) is int:
                for target in PROBE_INDICES:
                    if target in self.completed_indices or index < target:
                        continue
                    # Mark the attempt before any observer work: a broken or
                    # oversized result must never repeat an expensive walk.
                    self.completed_indices.add(target)
                    self.last_probe_finished = self.clock()
                    if index > target:
                        result = dict(status='MISSED_SCHEDULED_TAIL', cache=None)
                    elif watch.get('active_cycle_count') != 0:
                        result = dict(status='SKIPPED_WATCH_ACTIVE', cache=None)
                    else:
                        before = self.measure()
                        rss = before.get('rss_bytes')
                        memory = last.get('memory') or {}
                        descendants = memory.get('descendants') or {}
                        browser = descendants.get('browser_named') or {}
                        other = descendants.get('other') or {}
                        if type(rss) is not int or rss > MAX_SELF_RSS_BYTES:
                            result = dict(status='SKIPPED_SELF_RSS_GUARD', cache=None,
                                          process_before=before)
                        elif memory.get('status') != 'COMPLETE' or browser.get('process_count') != 0 or other.get('process_count') != 0:
                            result = dict(status='SKIPPED_DESCENDANT_COVERAGE_GUARD', cache=None,
                                          process_before=before)
                        else:
                            admission = self.admission_measure()
                            children = admission.get('descendants') or {}
                            group = admission.get('cgroup') or {}
                            fresh_rss = (admission.get('self') or {}).get('rss_bytes')
                            current, maximum = group.get('current_bytes'), group.get('max_bytes')
                            gate_watch = self.watch_status()
                            gate_tail = gate_watch.get('post_cleanup_followups') or {}
                            gate_last = gate_tail.get('last_sample') or {}
                            admitted = (admission.get('status') == 'COMPLETE' and
                                type(fresh_rss) is int and fresh_rss <= MAX_SELF_RSS_BYTES and
                                (children.get('browser_named') or {}).get('process_count') == 0 and
                                (children.get('other') or {}).get('process_count') == 0 and
                                type(current) is int and type(maximum) is int and
                                maximum - current >= 512 * 1024 * 1024 and
                                gate_watch.get('active_cycle_count') == 0 and
                                gate_tail.get('activity_revision') == tail.get('activity_revision') and
                                gate_last.get('origin_cycle_id') == selected_cycle and
                                self.clock() < self.deadline)
                            if not admitted:
                                result = dict(status='SKIPPED_FRESH_PROCESS_OR_CGROUP_GUARD',
                                    cache=None, process_before=before, admission_sample=admission)
                            else:
                                emitted = self.allocator_emit(dict(session=self.session,
                                    runtime_commit=self.commit, origin_cycle_id=selected_cycle,
                                    observation_index=target))
                                try:
                                    cache = self.cache_measure()
                                except BaseException:
                                    cache = dict(status='UNAVAILABLE', reason='CACHE_PROBE_ERROR')
                                after = self.measure()
                                after_watch = self.watch_status()
                                after_tail = after_watch.get('post_cleanup_followups') or {}
                                stable = after_watch.get('active_cycle_count') == 0 and after_tail.get('activity_revision') == tail.get('activity_revision')
                                result = dict(status='MEASURED' if stable else 'WATCH_ACTIVITY_CHANGED',
                                    cache=cache, process_before=before, process_after=after,
                                    pymalloc_emission=emitted,
                                    admission_sample=admission,
                                    watch_activity_revision_after=after_tail.get('activity_revision'))
                    result.update(observation_index=target, origin_cycle_id=selected_cycle,
                        tail_sample_completed_at_utc=last.get('sample_completed_at_utc'),
                        scheduled_delay_seconds={3:120, 5:600}[target],
                        watch_activity_revision_before=tail.get('activity_revision'),
                        scope='ADDITIONAL_NAMED_REACHABLE_SHALLOW_BYTES_NOT_EXCLUSIVE_RSS_OR_MEMORY_FREED_ON_EVICTION')
                    try:
                        self._record('CACHE_PROBE', result)
                    except BaseException:
                        self._record('CACHE_PROBE', dict(status='PROBE_RECORD_UNAVAILABLE',
                            cache=None, observation_index=target, origin_cycle_id=selected_cycle))
                    self.last_probe_finished = self.clock()
            if cycle is not None and cycle != selected_cycle:
                for target in PROBE_INDICES:
                    if target not in self.completed_indices:
                        self._record('CACHE_PROBE', dict(status='PREEMPTED_BY_LATER_CYCLE',
                            cache=None, observation_index=target, origin_cycle_id=selected_cycle))
                        self.completed_indices.add(target)
                        self.last_probe_finished = self.clock()
        if self.completed_indices == set(PROBE_INDICES) and self.last_probe_finished is not None and self.clock() - self.last_probe_finished >= 60:
            with self.lock:
                self.state = 'COMPLETED'
            self._record('COMPLETED', {'probe_indices': list(PROBE_INDICES)})
            return False
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
                scope='READ_ONLY_NAMED_ROOT_MEASUREMENT_NOT_A_HEAP_CENSUS_OR_CAUSAL_RSS_PARTITION'), limit=2 * 1024 * 1024)


def install():
    global _INSTANCE
    try:
        with _INSTALL_LOCK:
            if _INSTANCE is not None or remaining_seconds() <= 0:
                return
            import research_watch_runtime
            candidate = Investigation(research_watch_runtime.status,
                duration=remaining_seconds(), pins=_pins())
            candidate.thread = threading.Thread(target=candidate._run,
                name='retained-root-memory-one-off', daemon=True)
            _INSTANCE = candidate
            candidate.thread.start()
    except BaseException:
        pass


def status():
    if _INSTANCE is None:
        return dict(version=VERSION, state='INACTIVE', expires_at_utc=EXPIRES_AT_UTC)
    try:
        return _INSTANCE.status()
    except BaseException:
        return dict(version=VERSION, state='STATUS_UNAVAILABLE', expires_at_utc=EXPIRES_AT_UTC)
