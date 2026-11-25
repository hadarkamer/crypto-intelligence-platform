"""Numeric diagnostics for one generated standalone map task, never a browser profile."""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
import json
import os
from pathlib import Path
import resource
import threading
import time
from uuid import UUID

CURRENT = ContextVar('collection_metric_sink', default=None)
PHASES = ('browser_context', 'map_access', 'map_read', 'browser_checkpoint', 'analysis', 'result_write')
COUNTERS = ('provider_requests', 'cpu_ms', 'sampled_peak_rss_kib', 'sampled_peak_processes')
LIMITS = {**{phase + '_ms': 86400000 for phase in PHASES},
          'provider_requests': 100, 'cpu_ms': 86400000, 'sampled_peak_rss_kib': 2 ** 32,
          'sampled_peak_processes': 65536, 'queue_wait_ms': 2 ** 53 - 1, 'collector_db_save_ms': 86400000,
          'session_load_ms': 86400000, 'session_store_ms': 86400000}


def clean_numbers(values):
    return {k: v for k, v in values.items() if k in LIMITS and type(v) is int and 0 <= v <= LIMITS[k]}


def emit(run_id, values):
    try:
        safe = clean_numbers(values)
        if safe:
            print('COINGLASS_METRICS ' + json.dumps({'job_id': str(UUID(str(run_id))), **safe},
                  separators=(',', ':')), flush=True)
    except (ValueError, TypeError, OSError):
        pass


def elapsed_ms(started):
    return round((time.monotonic() - started) * 1000)


def resource_sample(pid):
    """Only process relationships and RSS are read, never argv, env, URLs or file contents."""
    rss = 0
    todo, seen = [pid], set()
    while todo and len(seen) < 1024:
        child = todo.pop()
        if child in seen:
            continue
        seen.add(child)
        try:
            # Fields after process name begin at state, PPID, ... RSS (field 24).
            info = Path(f'/proc/{child}/stat').read_text().rsplit(')', 1)[1].split()
            rss += int(info[21]) * os.sysconf('SC_PAGE_SIZE') // 1024
            for task in Path(f'/proc/{child}/task').iterdir():
                todo.extend(int(p) for p in (task / 'children').read_text().split())
        except (OSError, ValueError, IndexError):
            continue
    return rss, len(seen)


def cpu_ms():
    return round(1000 * sum(sum((u.ru_utime, u.ru_stime)) for u in
           (resource.getrusage(resource.RUSAGE_SELF), resource.getrusage(resource.RUSAGE_CHILDREN))))


class TaskMetrics:
    def __init__(self, root):
        self.path = Path(root) / 'collection-metrics.json'
        self.values = {'provider_requests': 0}
        self.started = {}
        self.cpu_start = cpu_ms()
        self.lock = threading.Lock()
        self.stop = threading.Event()

    def update(self):
        # A private tempfile and atomic replacement permit a bounded last sample
        # even when the outer scanner deadline kills the browser child.
        try:
            rss, count = resource_sample(os.getpid())
            with self.lock:
                self.values['sampled_peak_rss_kib'] = max(self.values.get('sampled_peak_rss_kib', 0), rss)
                self.values['sampled_peak_processes'] = max(self.values.get('sampled_peak_processes', 0), count)
                self.values['cpu_ms'] = cpu_ms() - self.cpu_start
                out = dict(self.values)
                for name, start in self.started.items():
                    out[name] = out.get(name, 0) + elapsed_ms(start)
                temp = self.path.with_suffix('.tmp')
                temp.write_text(json.dumps(clean_numbers(out)), encoding='ascii')
                temp.replace(self.path)
        except (OSError, ValueError):
            pass

    def sample(self):
        while not self.stop.wait(0.5):
            self.update()


@contextmanager
def task_metrics(root):
    data = TaskMetrics(root)
    token = CURRENT.set(data)
    sampler = threading.Thread(target=data.sample, daemon=True)
    sampler.start()
    try:
        yield
    finally:
        data.stop.set()
        sampler.join(timeout=1)
        data.update()
        CURRENT.reset(token)


@contextmanager
def phase(name):
    data = CURRENT.get()
    if data is None:
        yield
        return
    key = name + '_ms'
    if name not in PHASES:
        raise ValueError('invalid metric phase')
    start = time.monotonic()
    with data.lock:
        data.started[key] = start
    try:
        yield
    finally:
        with data.lock:
            data.values[key] = data.values.get(key, 0) + elapsed_ms(start)
            data.started.pop(key, None)
        data.update()


def provider_request():
    data = CURRENT.get()
    if data:
        with data.lock:
            data.values['provider_requests'] += 1
        data.update()


def emit_child(run_id, root):
    try:
        path = Path(root) / 'collection-metrics.json'
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 2048:
            return
        val = json.loads(path.read_text())
        if isinstance(val, dict):
            emit(run_id, val)
    except (OSError, ValueError):
        pass


@contextmanager
def logged_phase(run_id, key):
    """Parent-only session/queue persistence timing even when an operation raises."""
    if key not in ('collector_db_save_ms', 'session_load_ms', 'session_store_ms'):
        raise ValueError('invalid metric phase')
    start = time.monotonic()
    try:
        yield
    finally:
        emit(run_id, {key: elapsed_ms(start)})


def queue_wait(run_id, row):
    created, started = row.get('created_at'), row.get('started_at')
    if hasattr(created, 'utcoffset') and hasattr(started, 'utcoffset'):
        try:
            emit(run_id, {'queue_wait_ms': round((started - created).total_seconds() * 1000)})
        except (TypeError, ValueError, OverflowError):
            pass
