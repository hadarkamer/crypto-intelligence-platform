"""Temporary bounded process observations; never retain task inputs/results.

No import-time I/O or threads. Explicit startup installation wraps existing calls,
not scheduling, storage, formulas or delivery. RSS deltas are process-wide, not
exclusive ownership. FINISH precedes caller release; PULSE observes later state.
No forced GC, allocator trimming, heap walks or tracemalloc in production.
"""
from __future__ import annotations

import asyncio
from collections import deque
from contextvars import ContextVar
from datetime import datetime, timezone
from functools import wraps
import gc
import inspect
import json
import os
import queue
import re
import sys
import threading
import time
from uuid import uuid4

import research_runtime_memory

PREFIX = '[BACKGROUND_MEMORY] '
MAX_ACTIVE = 64
MAX_QUEUE = 64
MAX_LINE = 16384
DURATION_SECONDS = 7200
# Absolute end of this authorized one-off investigation. Restarts cannot rearm
# a fresh two-hour run, and later deployments of this source remain inactive.
EXPIRES_AT_UTC = '2026-10-31T17:15:00+00:00'
INTERVAL_SECONDS = 15
_CURRENT = ContextVar('background_memory_spans', default=())
_OBSERVER = None


def _remaining_seconds():
    remaining = (datetime.fromisoformat(EXPIRES_AT_UTC) - datetime.now(timezone.utc)).total_seconds()
    return max(0, min(DURATION_SECONDS, remaining))


def counters():
    """One bounded procfs read; no process enumeration on the task's thread."""
    rss = None
    try:
        with open('/proc/self/status', 'rb', buffering=0) as handle:
            raw = handle.read(16384)
        for line in raw.splitlines():
            if line.startswith(b'VmRSS:'):
                fields = line.split()
                if len(fields) == 3 and fields[2] == b'kB':
                    rss = int(fields[1]) * 1024
                break
    except BaseException:
        pass
    return {'rss_bytes': rss, 'python_allocated_blocks': sys.getallocatedblocks(),
            'gc_counts': list(gc.get_count())}


class Observer:
    def __init__(self, *, duration=DURATION_SECONDS, interval=INTERVAL_SECONDS,
                 clock=time.monotonic, measure=counters, sink=None):
        self.clock, self.measure = clock, measure
        self.deadline = clock() + duration
        self.interval = interval
        self.sink = sink or (lambda line: print(PREFIX + line, flush=True))
        self.session = uuid4().hex[:12]
        commit = os.getenv('RENDER_GIT_COMMIT', '')
        self.commit = commit if re.fullmatch(r'[0-9a-f]{40}', commit) else None
        self.lock = threading.Lock()
        self.queue = queue.Queue(maxsize=MAX_QUEUE)
        self.active = {}
        self.recent = deque(maxlen=MAX_ACTIVE)
        self.sequence = 0
        self.dropped = 0
        self.errors = 0
        self.closed = False
        self.thread = None

    def _put(self, event):
        # All producers hold lock; bounded primitives only, never native objects.
        try:
            event.update(version='background-memory-v1', session=self.session,
                         commit=self.commit, at_utc=datetime.now(timezone.utc).isoformat(),
                         dropped=self.dropped, observer_errors=self.errors)
            line = json.dumps(event, separators=(',', ':'), allow_nan=False)
            if len(line.encode()) > MAX_LINE:
                self.dropped += 1
                return
            self.queue.put_nowait(line)
        except queue.Full:
            self.dropped += 1
        except BaseException:
            self.errors += 1

    def begin(self, label):
        try:
            with self.lock:
                if self.closed or self.clock() >= self.deadline:
                    return None
                if len(self.active) >= MAX_ACTIVE:
                    self.dropped += 1
                    return None
                before = self.measure()
                self.sequence += 1
                key = self.sequence
                overlaps = {s['task'] for s in self.active.values()}
                for span in self.active.values():
                    span['overlap'].add(label)
                self.active[key] = {'task': label, 'start': self.clock(),
                                    'before': before, 'overlap': overlaps}
                self._put({'event': 'START', 'id': key, 'task': label,
                           'memory': before, 'active': self._active()})
                return key
        except BaseException:
            self.errors += 1
            return None

    def _active(self):
        return [{'id': k, 'task': v['task']} for k, v in self.active.items()]

    def end(self, key, outcome):
        if key is None:
            return
        try:
            with self.lock:
                span = self.active.pop(key, None)
                if span is None or self.closed:
                    return
                after = self.measure()
                before_rss = span['before']['rss_bytes']
                after_rss = after['rss_bytes']
                self.recent.append({'id': key, 'task': span['task']})
                self._put({'event': 'FINISH', 'id': key, 'task': span['task'],
                           'outcome': outcome, 'seconds': round(self.clock() - span['start'], 4),
                           'before': span['before'], 'memory': after,
                           'rss_delta_bytes': after_rss - before_rss
                           if after_rss is not None and before_rss is not None else None,
                           'overlap': sorted(span['overlap']), 'active': self._active(),
                           'scope': 'PROCESS_DELTA_NOT_EXCLUSIVE_TASK_OWNERSHIP'})
        except BaseException:
            self.errors += 1

    def wrap(self, label, fn):
        if not re.fullmatch(r'[a-z0-9_.-]{1,80}', label):
            raise ValueError('Invalid static observer label')
        if getattr(fn, '_background_memory_wrapped', False):
            return fn

        def enter():
            # Skip recursive self-calls, but track independent concurrent calls.
            current = _CURRENT.get()
            if label in current or self.closed:
                return None, None
            key = self.begin(label)
            return key, _CURRENT.set(current + (label,)) if key is not None else None

        def leave(key, token, outcome):
            try:
                self.end(key, outcome)
            except BaseException:
                pass
            finally:
                if token is not None:
                    try:
                        _CURRENT.reset(token)
                    except BaseException:
                        pass

        if inspect.iscoroutinefunction(fn):
            @wraps(fn)
            async def observed(*args, **kwargs):
                key, token = None, None
                try:
                    key, token = enter()
                except BaseException:
                    pass
                outcome = 'RAISED'
                try:
                    result = await fn(*args, **kwargs)
                    outcome = 'RETURNED'
                    return result
                except asyncio.CancelledError:
                    outcome = 'CANCELLED'
                    raise
                finally:
                    leave(key, token, outcome)
        else:
            @wraps(fn)
            def observed(*args, **kwargs):
                key, token = None, None
                try:
                    key, token = enter()
                except BaseException:
                    pass
                outcome = 'RAISED'
                try:
                    result = fn(*args, **kwargs)
                    outcome = 'RETURNED'
                    return result
                finally:
                    leave(key, token, outcome)
        observed._background_memory_wrapped = True
        return observed

    def pulse(self):
        # Descendant sampling only on our daemon, never a native worker thread.
        sample = research_runtime_memory.sample_memory(budget_ms=10, max_processes=128)
        with self.lock:
            self._put({'event': 'PULSE', 'memory': self.measure(), 'active': self._active(),
                       'completed_since_pulse': list(self.recent), 'process_sample': sample})
            self.recent.clear()

    def _run(self):
        next_pulse = self.clock()
        while True:
            try:
                if self.clock() >= self.deadline and not self.closed:
                    with self.lock:
                        self.closed = True
                        self._put({'event': 'EXPIRED', 'active': self._active()})
                        self.active.clear()
                        self.recent.clear()
                if self.closed and self.queue.empty():
                    return
                if not self.closed and self.clock() >= next_pulse:
                    self.pulse()
                    next_pulse = self.clock() + self.interval
                try:
                    line = self.queue.get(timeout=.5)
                except queue.Empty:
                    continue
                try:
                    self.sink(line)
                finally:
                    self.queue.task_done()
            except BaseException:
                self.errors += 1
                # A broken observer must not spin or disrupt native work.
                time.sleep(.5)

    def start(self):
        self.thread = threading.Thread(target=self._run, daemon=True, name='background-memory-observer')
        self.thread.start()


# Only explicit known entry points. No generic profiling or import hooks.
# Workers with module-level run_once intentionally wrap the module, not WORKER.
TARGETS = (
    ('research_event_store', 'WRITER._write_batch', 'event-store'),
    ('research_outcome_worker', 'WORKER.run_once', 'outcomes'),
    ('research_formula_worker', 'WORKER.run_discovery_horizon_once', 'formula-discovery'),
    ('research_formula_worker', 'WORKER.run_shadow_once', 'formula-shadow'),
    ('research_watch_scan_intake', 'run_once', 'watch-intake'),
    ('research_watch_scan_measurement_worker', 'run_once', 'watch-measurement'),
    ('research_watch_scan_formula_worker', 'run_once', 'watch-formulas'),
    ('research_watch_scan_formula_timeframe_worker', 'run_once', 'watch-timeframes'),
    ('research_price_archive_worker', 'WORKER.run_once', 'price-archive'),
    ('research_btc_episode_worker', 'WORKER.run_once', 'btc-episodes'),
    ('research_btc_wave_report_worker', 'WORKER.run_once', 'btc-wave-report'),
    ('research_formula_ordered_worker', 'WORKER.run_once', 'formula-ordered'),
    ('research_ordered_experimental_worker', 'WORKER.run_once', 'ordered-experimental'),
    ('research_snapshot_sync_worker', 'WORKER.run_once', 'snapshot-sync'),
    ('research_forward_paper_worker', 'WORKER._cycle_owned', 'forward-paper'),
    ('research_forward_paper_worker', 'WORKER._open_owned', 'forward-paper-restore'),
    ('research_forward_evaluation_worker', 'WORKER._cycle_owned', 'forward-evaluation'),
    ('research_liquidity_forward_worker', 'WORKER._cycle_owned', 'liquidity-forward'),
    ('research_liquidity_forward_worker', 'WORKER._open_owned', 'liquidity-forward-restore'),
    ('research_prospective_anchor_worker', 'WORKER.run_once', 'prospective-anchor'),
    ('u21_experimental_worker', 'WORKER.tick', 'u21'),
    ('sol_proximity_experimental_worker', 'WORKER.tick', 'sol-proximity'),
    ('sol_g65_experimental_worker', 'WORKER.tick', 'sol-g65'),
    ('doge_partial_experimental_worker', 'WORKER.tick', 'doge-partial'),
    ('hype_row71205_experimental_worker', 'WORKER.tick', 'hype-row71205'),
    ('xrp_r2732_experimental_worker', 'WORKER.tick', 'xrp-r2732'),
    ('coinglass_oi_regime_service', '_history', 'oi-history-read'),
)
MAIN_TARGETS = (
    ('run_watch_cycle', 'watch'),
    ('run_specific_watch_cycle', 'specific-watch'),
    ('collect_live_rows_for_watch', 'collect-live'),
    ('collect_once', 'collect-once'),
    ('_run_history_backfill_once', 'history-backfill'),
    ('_first_touch_backfill_once', 'first-touch-backfill'),
    ('_collect_oi_regime_once_locked', 'oi-collection'),
    ('_collect_flow_once_locked', 'flow-collection'),
)


def install(namespace):
    """Called once at application startup; failures leave all native calls usable."""
    global _OBSERVER
    try:
        if _OBSERVER is not None:
            return
        duration = _remaining_seconds()
        if duration <= 0:
            return
        observer = Observer(duration=duration)
        installed, missing = [], []
        for module_name, path, label in TARGETS:
            try:
                owner = namespace[module_name]
                parts = path.split('.')
                for part in parts[:-1]:
                    owner = getattr(owner, part)
                fn = getattr(owner, parts[-1])
                if not callable(fn):
                    raise TypeError('Not callable')
                setattr(owner, parts[-1], observer.wrap(label, fn))
                installed.append(label)
            except BaseException:
                missing.append(label)
        for name, label in MAIN_TARGETS:
            try:
                namespace[name] = observer.wrap(label, namespace[name])
                installed.append(label)
            except BaseException:
                missing.append(label)
        for symbol, worker in namespace['sol_proximity_experimental_worker'].ADDITIONAL_WORKERS.items():
            label = 'proximity-' + symbol.lower()
            try:
                worker.tick = observer.wrap(label, worker.tick)
                installed.append(label)
            except BaseException:
                missing.append(label)
        with observer.lock:
            observer._put({'event': 'INSTALLED', 'tasks': installed, 'missing': missing,
                           'duration_seconds': duration, 'expires_at_utc': EXPIRES_AT_UTC,
                           'interval_seconds': INTERVAL_SECONDS,
                           'scope': 'BOUNDED_PROCESS_OBSERVATIONS_NOT_FULL_HEAP_ATTRIBUTION'})
        _OBSERVER = observer
        observer.start()
    except BaseException:
        # Installation cannot prevent startup, even if thread creation fails.
        if _OBSERVER is not None:
            _OBSERVER.closed = True
