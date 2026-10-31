"""Passive Watch markers and one bounded background memory sampler.

All procfs reads happen in the monitor thread. Markers are non-exclusive:
DOM and derivative collection overlap. FINISH_REQUESTED witnesses entering
Watch finally, before the existing capsule sealing and asynchronous cleanup;
it does not prove that the coroutine returned or that any message was sent.
"""
from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
import hashlib
import json
import math
from pathlib import Path
from threading import Condition, Event, Thread
import time

import research_runtime_journal as journal
import research_runtime_memory as memory

VERSION = 'watch-runtime-observer-v1'
INTERVAL_SECONDS = 30.
MAX_ACTIVE_CYCLES = 2
MAX_SAMPLES_PER_CYCLE = 24
STAGES = frozenset({'COLLECTING_INPUTS', 'DERIVATIVES_READY', 'SCORING_READY', 'INPUTS_READY', 'BEFORE_DELIVERY'})
MARKER_SCOPE = 'NON_EXCLUSIVE_WATCH_MARKERS_NOT_MEMORY_ALLOCATION_OR_DELIVERY_CAUSES'
FINISH_SCOPE = 'WATCH_FINALLY_ENTERED_BEFORE_CAPSULE_SEAL_AND_ASYNC_CLEANUP_NOT_COROUTINE_RETURN'
_CURRENT = ContextVar('watch_runtime_observer', default=None)
_CONDITION = Condition()
_STOP = Event()
_MONITOR = None
_ACTIVE = {}
_LAST_SAMPLE = None
_METRICS = dict(begun=0, finished=0, markers=0, probe_attempts=0, probe_event_calls=0,
    probe_errors=0, late_probe_drops=0, capacity_drops=0, duplicate_begins=0,
    observer_errors=0, context_reset_errors=0, monitor_start_errors=0, last_error_type=None)


@dataclass
class _Handle:
    journal_handle: object
    cycle_id: str
    phase: str = 'COLLECTING_INPUTS'
    phase_revision: int = 0
    probes: int = 0
    next_due: float = 0.
    dirty: bool = True
    closed: bool = False
    token: object = None
    previous: object = None


def _utc():
    return datetime.now(timezone.utc).isoformat()


def _now():
    value = time.monotonic()
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError('INVALID_DIAGNOSTIC_CLOCK')
    return value


def _error(error, counter='observer_errors'):
    _METRICS[counter] += 1
    _METRICS['last_error_type'] = type(error).__name__[:64]


@lru_cache(maxsize=1)
def _code_pins():
    """Bounded once-per-process source reads; proc probing stays off-thread."""
    result = {}
    try:
        root = Path(__file__).resolve().parent
        for name in ('main.py', 'research_watch_runtime.py', 'research_runtime_journal.py', 'research_runtime_memory.py'):
            with (root / name).open('rb') as stream:
                raw = stream.read(1024 * 1024 + 1)
            if len(raw) > 1024 * 1024: raise ValueError('SOURCE_BYTE_BOUND')
            result[name] = hashlib.sha256(raw).hexdigest()
        return result
    except BaseException as error:
        with _CONDITION: _error(error)
        return {}


def _reset_context(handle):
    try:
        journal.reset(handle.journal_handle)
    except BaseException as error:
        with _CONDITION: _error(error)
    # A closed outer scope must not overwrite a newer active inner scope.
    if _CURRENT.get() is not handle:
        return
    for _ in range(64):
        current = _CURRENT.get()
        if not isinstance(current, _Handle) or not current.closed:
            return
        try:
            if current.token is None:
                _CURRENT.set(current.previous)
            else:
                _CURRENT.reset(current.token)
                current.token = None
        except BaseException as error:
            with _CONDITION: _error(error, 'context_reset_errors')
            _CURRENT.set(current.previous)
    with _CONDITION:
        _METRICS['context_reset_errors'] += 1
        _METRICS['last_error_type'] = 'ContextUnwindBound'


def _safe_reset(handle):
    try:
        _reset_context(handle)
    except BaseException as error:
        try:
            with _CONDITION: _error(error, 'context_reset_errors')
        except BaseException:
            pass


def _ensure_monitor_locked():
    global _MONITOR
    if _MONITOR is not None and _MONITOR.is_alive():
        return
    _STOP.clear()
    candidate = Thread(target=_monitor_loop, name='watch-runtime-memory', daemon=True)
    try:
        candidate.start()
        _MONITOR = candidate
    except BaseException as error:
        _error(error, 'monitor_start_errors')


def begin(cycle_id):
    """Enqueue START first; marker and proc sampling never await durability."""
    handle = None
    journal_handle = None
    try:
        if type(cycle_id) is not str or not cycle_id or len(cycle_id.encode('utf-8')) > 160:
            raise ValueError('INVALID_DIAGNOSTIC_CYCLE')
        with _CONDITION:
            if len(_ACTIVE) >= MAX_ACTIVE_CYCLES:
                _METRICS['capacity_drops'] += 1
                return None
            if any(item.cycle_id == cycle_id for item in _ACTIVE.values()):
                _METRICS['duplicate_begins'] += 1
                return None
            journal_handle = journal.begin(cycle_id, source_code_sha256=_code_pins())
            if journal_handle is None:
                raise ValueError('JOURNAL_HANDLE_UNAVAILABLE')
            handle = _Handle(journal_handle=journal_handle, cycle_id=cycle_id)
            handle.previous = _CURRENT.get()
            handle.token = _CURRENT.set(handle)
            _ACTIVE[id(handle)] = handle
            _METRICS['begun'] += 1
            handle.phase_revision = 1
            try:
                journal.stage('COLLECTING_INPUTS', handle=journal_handle,
                    phase='COLLECTING_INPUTS', phase_revision=handle.phase_revision,
                    phase_observed_at_utc=_utc())
            except BaseException as error:
                _error(error)
            _ensure_monitor_locked()
            _CONDITION.notify_all()
        return handle
    except BaseException as error:
        with _CONDITION:
            _error(error)
            if handle is not None:
                handle.closed = True
                _ACTIVE.pop(id(handle), None)
        if handle is not None:
            _safe_reset(handle)
        elif journal_handle is not None:
            try: journal.reset(journal_handle)
            except BaseException: pass
        return None


def stage(handle, name):
    """Record a logical marker and coalesce a background sample request."""
    try:
        if not isinstance(handle, _Handle):
            return None
        if name not in STAGES:
            raise ValueError('INVALID_DIAGNOSTIC_STAGE')
        with _CONDITION:
            if handle.closed or _ACTIVE.get(id(handle)) is not handle:
                return None
            handle.phase = name
            handle.phase_revision += 1
            handle.dirty = True
            _ensure_monitor_locked()
            _CONDITION.notify_all()
            receipt = journal.stage(name, handle=handle.journal_handle, phase=name,
                phase_revision=handle.phase_revision, phase_observed_at_utc=_utc())
            _METRICS['markers'] += 1
            return receipt
    except BaseException as error:
        with _CONDITION: _error(error)
        return None


def _take_due_locked(now):
    for handle in tuple(_ACTIVE.values()):
        if (not handle.closed and handle.probes < MAX_SAMPLES_PER_CYCLE
                and (handle.dirty or now >= handle.next_due)):
            handle.probes += 1
            handle.dirty = False
            handle.next_due = now + INTERVAL_SECONDS
            _METRICS['probe_attempts'] += 1
            return handle, handle.phase, handle.phase_revision, _utc()
    return None


def _probe(observation):
    global _LAST_SAMPLE
    handle, phase, revision, observed_at = observation
    sample, reason = None, None
    started = _utc()
    try:
        candidate = memory.sample_memory()
        if memory.validate_sample(candidate):
            sample = candidate
        else:
            raise ValueError('INVALID_MEMORY_SAMPLE')
    except BaseException as error:
        reason = type(error).__name__[:64]
        with _CONDITION: _error(error, 'probe_errors')
    completed = _utc()
    try:
        with _CONDITION:
            if handle.closed or _ACTIVE.get(id(handle)) is not handle:
                _METRICS['late_probe_drops'] += 1
                return None
            receipt = journal.stage('PERIODIC_MEMORY', handle=handle.journal_handle,
                memory=sample, phase=phase, phase_revision=revision,
                phase_observed_at_utc=observed_at, sample_started_at_utc=started,
                sample_completed_at_utc=completed, reason_class=reason)
            _METRICS['probe_event_calls'] += 1
            _LAST_SAMPLE = dict(cycle_id=handle.cycle_id, phase=phase,
                phase_revision=revision, phase_observed_at_utc=observed_at,
                sample_started_at_utc=started, sample_completed_at_utc=completed,
                memory=sample, reason_class=reason)
            return receipt
    except BaseException as error:
        with _CONDITION: _error(error)
        return None


def _monitor_loop():
    while not _STOP.is_set():
        try:
            with _CONDITION:
                if _STOP.is_set(): return
                now = _now()
                observation = _take_due_locked(now)
                if observation is None:
                    due = [max(0.01, h.next_due - now) for h in _ACTIVE.values()
                           if not h.closed and h.probes < MAX_SAMPLES_PER_CYCLE]
                    _CONDITION.wait(timeout=min([INTERVAL_SECONDS, *due]))
                    continue
            _probe(observation)
        except BaseException as error:
            with _CONDITION: _error(error)
            _STOP.wait(1.)


def finish(handle, *, cycle_status=None):
    """Close monitoring at Watch finally entry, before existing async cleanup."""
    if not isinstance(handle, _Handle): return None
    try:
        with _CONDITION:
            if handle.closed:
                return None
            handle.closed = True
            _ACTIVE.pop(id(handle), None)
            _METRICS['finished'] += 1
            _CONDITION.notify_all()
        known = {'completed', 'completed_with_delivery_errors', 'completed_with_transition_gaps', 'failed', 'cancelled'}
        status = cycle_status.upper() if type(cycle_status) is str and cycle_status in known else 'UNKNOWN'
        return journal.finish(handle.journal_handle, status=status)
    except BaseException as error:
        with _CONDITION: _error(error)
        return None
    finally:
        _safe_reset(handle)


def status():
    """Cached observation metadata only; no filesystem, market or DB reads."""
    try:
        with _CONDITION:
            result = {**_METRICS, 'version': VERSION,
                'monitor_running': bool(_MONITOR is not None and _MONITOR.is_alive()),
                'active_cycle_count': len(_ACTIVE),
                'active_cycles': [dict(cycle_id=h.cycle_id, phase=h.phase,
                    phase_revision=h.phase_revision, probes=h.probes) for h in _ACTIVE.values()],
                'last_sample': json.loads(json.dumps(_LAST_SAMPLE, allow_nan=False)),
                'limits': dict(interval_seconds=INTERVAL_SECONDS,
                    max_active_cycles=MAX_ACTIVE_CYCLES, max_samples_per_cycle=MAX_SAMPLES_PER_CYCLE),
                'marker_scope': MARKER_SCOPE, 'finish_scope': FINISH_SCOPE,
                'counter_scope': 'BRIDGE_CALLS_NOT_DURABLE_ROWS_SEE_JOURNAL_PERSISTENCE',
                'memory_probes_on_native_thread': False,
                'scope': 'PASSIVE_RUNTIME_OBSERVABILITY_NOT_TRADING_OR_TRANSPORT'}
        result['journal'] = journal.status()
        return result
    except BaseException as error:
        return dict(version=VERSION, status='UNAVAILABLE', error_type=type(error).__name__[:64])


def _shutdown_for_tests():
    """Test-only thread cleanup; never called by native Watch paths."""
    _STOP.set()
    with _CONDITION: _CONDITION.notify_all()
    if _MONITOR is not None:
        _MONITOR.join(timeout=2.)
