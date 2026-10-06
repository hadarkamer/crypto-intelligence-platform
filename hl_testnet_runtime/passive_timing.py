"""Optional, lossy Testnet timing diagnostics, never execution authority.

Producers only validate bounded immutable data and try a lock once. They never
write, serialize, wait for a sink, signal a condition, or access trade storage.
One daemon writer retains a bounded in-memory archive, with no default I/O. A
custom test sink may lose diagnostics but cannot grow the buffer or create
replacement writer threads. Local duration
clocks are comparable only within ``session_id``. Wall/exchange times are separate.
"""
from collections import deque, OrderedDict
import json
import math
import re
import threading
import time
import uuid


SERVICE = 'srv-dakptbh594qs7395460g'
MODE = 'long_stream_testnet_v1'
OPT_IN = 'passive_v1'
MAX_CAPACITY = 2048
MAX_BATCH = 64
MAX_FIELDS = 32
MAX_STRING = 192
MAX_TEXT_TOTAL = 2048
MAX_TUPLE = 16
MAX_RECENT = 64
MAX_DEDUPE = 64
_TOKEN = re.compile(r'[A-Za-z0-9_./:+@-]*\Z')
_KIND = re.compile(r'[a-z][a-z0-9_]{0,47}\Z')
FIELDS = frozenset({
    'account_role', 'symbol', 'bucket', 'card_id', 'event_id', 'order_id',
    'fill_id', 'request_id', 'action', 'status', 'reason', 'quantity',
    'observed_at_ms', 'exchange_at_ms', 'duration_ns', 'generation', 'revision',
    'notification_type', 'order_requests_sent', 'failure_code', 'evidence_at_ms',
    'state', 'entry_quantity', 'exit_quantity', 'remaining_quantity',
    'first_entry_at_ms', 'last_entry_at_ms', 'last_exit_at_ms',
    'protection_verified', 'closure_verified', 'verification_status',
    'stop_public_status_at_ms', 'stop_observed_at_ms', 'stop_quantity',
    'take_profit_quantity', 'active_order_ids', 'entry_order_ids', 'issues',
    'batch_count', 'captured_count', 'retry_after_ms', 'budget_stage',
})


def _scalar(value):
    """Exact builtin types only: no user conversion, nested data, or NaN."""
    if value is None or type(value) is bool:
        return True
    if type(value) is int:
        return -(2**63) < value < 2**63
    if type(value) is float:
        return math.isfinite(value)
    if type(value) is str:
        return len(value) <= MAX_STRING and bool(_TOKEN.fullmatch(value))
    return False


def _payload(kind, fields):
    if type(kind) is not str or not _KIND.fullmatch(kind):
        return None
    if len(fields) > MAX_FIELDS:
        return None
    items = []
    text_size = len(kind)
    for key, value in fields.items():
        if type(key) is not str or key not in FIELDS:
            return None
        values = value if type(value) is tuple else (value,)
        if len(values) > MAX_TUPLE or not all(_scalar(v) for v in values):
            return None
        text_size += sum(len(v) for v in values if type(v) is str)
        if text_size > MAX_TEXT_TOTAL:
            return None
        items.append((key, value))
    return tuple(items)


def _clock():
    return time.time_ns() // 1_000_000, time.monotonic_ns()


class Recorder:
    """Bounded in-memory recorder; no stdout, file, network, or database export.

    Optional ``sink`` is for local tests and consumes JSON lines on the writer
    thread only. Production start() does not accept or configure a custom sink.

    Instantiate/start before execution workers. Without start(), records remain
    in the bounded buffer (useful for deterministic tests). Dropped/error counts
    are diagnostics, not exact accounting under arbitrary interpreter races.
    Memory is bounded by capacity plus one batch, the recent archive and a fixed
    size deduplication cache. A copy of the archive is available from health().
    """
    def __init__(self, *, capacity=256, batch_size=32, sink=None):
        if type(capacity) is not int or not 1 <= capacity <= MAX_CAPACITY:
            raise ValueError('INVALID_TIMING_CAPACITY')
        if type(batch_size) is not int or not 1 <= batch_size <= MAX_BATCH:
            raise ValueError('INVALID_TIMING_BATCH')
        if sink is not None and not callable(sink):
            raise ValueError('INVALID_TIMING_SINK')
        self.capacity = capacity
        self.batch_size = min(batch_size, capacity)
        self.session_id = uuid.uuid4().hex
        self._sink = sink
        self._queue = deque()
        self._lock = threading.Lock()
        self._archive = deque(maxlen=MAX_RECENT)
        self._archive_lock = threading.Lock()
        self._dedupe = OrderedDict()
        self._control = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._accepting = True
        self._sequence = 0
        self._exported = 0
        self._in_flight = 0
        self._dropped_full = 0
        self._dropped_busy = 0
        self._dropped_invalid = 0
        self._sink_errors = 0
        self._record_errors = 0
        self._deduplicated = 0
        self._archive_evicted = 0

    def record(self, kind, *, at_ms=None, mono_ns=None, **fields):
        """Return immediately; ordinary failures never escape to trading."""
        try:
            if not self._accepting:
                return False
            payload = _payload(kind, fields)
            if payload is None:
                self._dropped_invalid += 1
                return False
            if at_ms is None and mono_ns is None:
                at_ms, mono_ns = _clock()
            if (type(at_ms) is not int or not 0 <= at_ms < 2**63
                    or type(mono_ns) is not int or not 0 <= mono_ns < 2**63):
                self._dropped_invalid += 1
                return False
            if not self._lock.acquire(False):
                self._dropped_busy += 1
                return False
            try:
                if not self._accepting:
                    return False
                if len(self._queue) >= self.capacity:
                    self._dropped_full += 1
                    return False
                self._sequence += 1
                self._queue.append((self._sequence, kind, at_ms, mono_ns, payload))
                return True
            finally:
                self._lock.release()
        except Exception:
            self._record_errors += 1
            return False

    def start(self):
        """One writer per recorder, including when a previous sink is stuck."""
        if not self._control.acquire(False):
            return False
        try:
            if self._thread is not None:
                return self._thread.is_alive() and self._accepting
            if not self._accepting:
                return False
            self._thread = threading.Thread(target=self._run, daemon=True,
                                           name='testnet-passive-timing')
            self._thread.start()
            return True
        finally:
            self._control.release()

    def stop(self, timeout=0.05):
        """Stop accepting; bounded best-effort flush, never wait for a hung sink."""
        self._accepting = False
        self._stop.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(max(0, min(float(timeout), 0.25)))
        return thread is None or not thread.is_alive()

    def worker_alive(self):
        return self._thread is not None and self._thread.is_alive()

    def _run(self):
        while True:
            with self._lock:
                batch = [self._queue.popleft()
                         for _ in range(min(self.batch_size, len(self._queue)))]
            self._in_flight = len(batch)
            for sequence, kind, at_ms, mono_ns, payload in batch:
                try:
                    event = dict(payload)
                    if kind == 'trade_observed' and event.get('card_id'):
                        key = (event.get('account_role'), event['card_id'])
                        signature = tuple(sorted(payload))
                        if self._dedupe.get(key) == signature:
                            self._deduplicated += 1
                            self._dedupe.move_to_end(key)
                            continue
                        self._dedupe[key] = signature
                        self._dedupe.move_to_end(key)
                        if len(self._dedupe) > MAX_DEDUPE:
                            self._dedupe.popitem(last=False)
                    event.update(schema='passive_v1', session_id=self.session_id,
                                 sequence=sequence, kind=kind,
                                 at_ms=at_ms, mono_ns=mono_ns)
                    with self._archive_lock:
                        if len(self._archive) == MAX_RECENT:
                            self._archive_evicted += 1
                        self._archive.append(event)
                    if self._sink is not None:
                        self._sink(json.dumps({'testnet_timing': event},
                                              separators=(',', ':'), allow_nan=False))
                    self._exported += 1
                except Exception:
                    self._sink_errors += 1
                finally:
                    self._in_flight -= 1
            if not batch:
                if self._stop.is_set():
                    return
                self._stop.wait(0.025)

    def health(self, *, include_recent=True):
        """Approximate snapshot. Public callers must omit the private archive."""
        acquired = self._lock.acquire(False)
        try:
            result = dict(enabled=self._accepting, session_id=self.session_id,
                        writer_alive=self.worker_alive(), busy=not acquired,
                        buffered=len(self._queue) if acquired else None,
                        capacity=self.capacity, in_flight=self._in_flight,
                        accepted=self._sequence, exported=self._exported,
                        dropped_full=self._dropped_full, dropped_busy=self._dropped_busy,
                        dropped_invalid=self._dropped_invalid,
                        sink_errors=self._sink_errors, record_errors=self._record_errors,
                        deduplicated=self._deduplicated, archive_evicted=self._archive_evicted)
        finally:
            if acquired:
                self._lock.release()
        if not include_recent:
            return result
        archive_acquired = self._archive_lock.acquire(False)
        try:
            # Scalar/tuple payloads are immutable; copying each event dictionary
            # prevents diagnostics callers from changing archived observations.
            result.update(recent_events=[dict(event) for event in self._archive]
                          if archive_acquired else [],
                          recent_busy=not archive_acquired, recent_capacity=MAX_RECENT)
            return result
        finally:
            if archive_acquired:
                self._archive_lock.release()


_recorder = None
_lifecycle_lock = threading.Lock()


def record(kind, **fields):
    try:
        current = _recorder
        return current.record(kind, **fields) if current is not None else False
    except Exception:
        return False


def stamp():
    """Clock capture for local durations; disabled mode does no clock work."""
    try:
        current = _recorder
        return _clock() if current is not None and current._accepting else None
    except Exception:
        return None


def health(*, include_recent=False):
    """Public-safe counters by default; event archive requires private routing."""
    try:
        current = _recorder
        return current.health(include_recent=include_recent) if current is not None else {'enabled': False}
    except Exception:
        return {'enabled': False, 'health_error': True}


def set_recorder(recorder):
    """Local test injection, never exposed by an HTTP or trading interface.

    Refuse replacement while an old writer is alive, even after stop timed out.
    This also prevents repeated activation from accumulating hung threads.
    """
    global _recorder
    if recorder is not None and not isinstance(recorder, Recorder):
        return False
    if not _lifecycle_lock.acquire(False):
        return False
    try:
        if _recorder is recorder:
            return True
        if _recorder is not None and _recorder.worker_alive():
            return False
        if _recorder is not None:
            _recorder._accepting = False
        _recorder = recorder
        return True
    except Exception:
        return False
    finally:
        _lifecycle_lock.release()


def start(env):
    """Explicit Testnet opt-in only; no environment lookup or side-effect on import."""
    global _recorder
    try:
        allowed = (env.get('HL_TESTNET_TIMING_TELEMETRY') == OPT_IN
                   and env.get('RENDER_SERVICE_ID') == SERVICE
                   and env.get('HL_TESTNET_RUNTIME_MODE') == MODE)
        if not allowed:
            stop()
            return False
        if not _lifecycle_lock.acquire(False):
            return False
        try:
            if _recorder is not None and _recorder.worker_alive():
                return _recorder._accepting
            if _recorder is None or not _recorder._accepting:
                _recorder = Recorder()
            return _recorder.start()
        finally:
            _lifecycle_lock.release()
    except Exception:
        return False


def stop():
    try:
        current = _recorder
        return current.stop() if current is not None else True
    except Exception:
        return False
