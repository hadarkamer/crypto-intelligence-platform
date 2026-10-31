"""Bounded passive Watch runtime journal, independent of decision/event stores.

Import performs no file, database, schema or thread work. begin() immediately
requests an asynchronous START write; queue acceptance is never durable ACK.
FINISH records finally entry before native cleanup, not coroutine return.
All diagnostic boundaries catch BaseException without changing native work.
"""
from __future__ import annotations

from contextvars import ContextVar
from datetime import datetime, timezone
from functools import lru_cache
import hashlib
import json
import math
import os
from pathlib import Path
import queue
import re
import threading
from uuid import uuid4

VERSION = "watch-runtime-journal-v1"
TABLE = "research_runtime_journal_events"
MAX_EVENT_BYTES = 32 * 1024
MAX_MEMORY_BYTES = 8192
MAX_QUEUE_ITEMS = 16
MAX_QUEUE_BYTES = 512 * 1024
MAX_EVENTS_PER_CYCLE = 32
CONNECT_TIMEOUT_SECONDS = 3
STATEMENT_TIMEOUT_MS = 3000
LOCK_TIMEOUT_MS = 1000
STAGES = frozenset({"COLLECTING_INPUTS", "DOM_READY", "DERIVATIVES_READY", "INPUTS_READY", "SCORING_READY",
                    "BEFORE_DECISIONS", "AFTER_DECISIONS", "BEFORE_DELIVERY", "PERIODIC_MEMORY"})
PHASES = (STAGES - {"PERIODIC_MEMORY"}) | {"STARTING", "DELIVERING", "FINALLY_ENTERED", "UNKNOWN"}
OUTCOMES = frozenset({"COMPLETED", "COMPLETED_WITH_DELIVERY_ERRORS", "COMPLETED_WITH_TRANSITION_GAPS", "FAILED", "CANCELLED", "UNKNOWN"})
CLOCK_SCOPE = "PROCESS_CLOCK_ANNOTATIONS_NOT_EARLIEST_DATABASE_OR_PROVIDER_AVAILABILITY"
FINISH_SCOPE = "WATCH_FINALLY_ENTRY_BEFORE_EXISTING_CLEANUP_NOT_COROUTINE_RETURN"
PHASE_SCOPE = "CALLER_REPORTED_NONEXCLUSIVE_PHASE_MARKER_OVERLAPPING_TASKS_POSSIBLE"
_HEX = re.compile(r"[0-9a-f]{64}\Z")
_CLASS = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,79}\Z")
_FORBIDDEN = frozenset({"chat_id", "recipient", "recipient_id", "message", "message_text", "text", "token", "secret",
    "password", "database_url", "api_key", "authorization", "credentials", "cmdline", "environ", "command", "pid"})
_CURRENT = ContextVar("passive_watch_runtime_journal", default=None)
_QUEUE = queue.Queue(maxsize=MAX_QUEUE_ITEMS)
_LOCK = threading.Lock()
_WORKER_LOCK = threading.Lock()
_SESSION_LOCK = threading.Lock()
_WORKER = None
_STORE = None
_SESSION = None
_METRICS = dict(begin_calls=0, started_cycles=0, stage_calls=0, finish_calls=0, finished_cycles=0,
    enqueue_attempts=0, enqueued=0, persisted=0, failed=0, invalid=0, dropped=0, unavailable=0,
    idempotent_existing=0, queued_items=0, queued_bytes=0, observer_failures=0,
    start_enqueued=0, start_persisted=0, finish_enqueued=0, finish_persisted=0,
    last=None, last_durable=None, last_durable_start=None, last_durable_finish=None)


class JournalValidationError(ValueError):
    def __init__(self, code): self.code = code; super().__init__(code)


def _require(value, code):
    if not value: raise JournalValidationError(code)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def digest(value): return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def _utc_now(): return datetime.now(timezone.utc).isoformat()


def _clock():
    try: return _utc_now()
    except BaseException: return None


def _valid_clock(value):
    if type(value) is not str or len(value) > 64: return False
    try: return datetime.fromisoformat(value.replace("Z", "+00:00")).tzinfo is not None
    except (TypeError, ValueError): return False


def _copy_json(value, *, max_bytes=MAX_EVENT_BYTES, max_nodes=2048, max_depth=10):
    nodes, active = 0, set()
    def visit(item, depth):
        nonlocal nodes
        nodes += 1
        _require(nodes <= max_nodes and depth <= max_depth, "JSON_STRUCTURE_BOUND")
        kind = type(item)
        if item is None or kind is bool: return item
        if kind is str:
            _require(len(item) <= max_bytes and len(item.encode("utf-8")) <= max_bytes, "JSON_STRING_BOUND")
            return item
        if kind is int:
            _require(item.bit_length() <= 128, "JSON_INTEGER_BOUND"); return item
        if kind is float:
            _require(math.isfinite(item), "JSON_NONFINITE_NUMBER"); return item
        _require(kind in (dict, list) and id(item) not in active and len(item) <= 512, "JSON_TYPE_OR_COLLECTION_BOUND")
        active.add(id(item))
        try:
            if kind is list: return [visit(part, depth + 1) for part in item]
            result = {}
            for key, part in item.items():
                _require(type(key) is str and len(key) <= 128 and key.lower() not in _FORBIDDEN, "JSON_FORBIDDEN_OR_INVALID_KEY")
                result[key] = visit(part, depth + 1)
            return result
        finally: active.remove(id(item))
    copied = visit(value, 0)
    _require(len(canonical(copied).encode("utf-8")) <= max_bytes, "JSON_BYTE_BOUND")
    return copied


@lru_cache(maxsize=1)
def _writer_code_sha256(): return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _error_code(error):
    if isinstance(error, JournalValidationError): return error.code
    name = type(error).__name__
    return name if _CLASS.fullmatch(name) else "Exception"


def _summary(prepared):
    return {key:prepared[key] for key in ("process_session_id", "cycle_id", "sequence", "event_type", "stage", "outcome", "payload_sha256")}


def _failure(code, *, status="FAILED", prepared=None):
    result = {"version":VERSION, "status":status, "reason_code":code, "durable":False}
    if prepared is not None: result.update(_summary(prepared))
    try:
        with _LOCK:
            key = {"FAILED":"failed", "INVALID":"invalid", "DROPPED":"dropped", "UNAVAILABLE":"unavailable", "OBSERVER_ERROR":"observer_failures"}.get(status, "observer_failures")
            _METRICS[key] += 1
            _METRICS["last"] = {**result, "at_utc":_clock()}
    except BaseException: pass
    return result


def _session():
    global _SESSION
    with _SESSION_LOCK:
        if _SESSION is None: _SESSION = (uuid4().hex, _clock())
        return _SESSION


def _source_pins(value):
    flags, result = [], {}
    try:
        if value is not None:
            _require(type(value) is dict and len(value) <= 16, "SOURCE_PINS_INVALID")
            for name, pin in value.items():
                _require(type(name) is str and re.fullmatch(r"[A-Za-z0-9_]{1,100}\.py", name) and type(pin) is str and _HEX.fullmatch(pin), "SOURCE_PINS_INVALID")
                result[name] = pin
        if "research_runtime_journal.py" not in result:
            result["research_runtime_journal.py"] = _writer_code_sha256()
            flags.append("SOURCE_PINS_INCOMPLETE")
    except BaseException:
        result, flags = {}, ["SOURCE_PINS_UNAVAILABLE"]
    return result, flags


def _memory_sample_valid(value):
    import research_runtime_memory
    return research_runtime_memory.validate_sample(value)


def _memory(value):
    if value is None: return {"status":"NOT_REQUESTED"}, []
    try:
        copied = _copy_json(value, max_bytes=MAX_MEMORY_BYTES, max_nodes=512, max_depth=6)
        _require(_memory_sample_valid(copied) is True, "MEMORY_SNAPSHOT_INVALID")
        flags = [] if copied["status"] == "COMPLETE" else ["MEMORY_SAMPLE_" + copied["status"]]
        return {"status":"CAPTURED", "sample":copied, "sample_sha256":digest(copied)}, flags
    except BaseException:
        return {"status":"UNAVAILABLE", "reason_code":"MEMORY_SNAPSHOT_INVALID"}, ["MEMORY_SNAPSHOT_INVALID"]


class _Handle:
    def __init__(self, cycle_id, session, pins, flags, runtime_commit):
        self.cycle_id = cycle_id
        self.process_session_id, self.process_session_started_at_utc = session
        self._pins, self._flags, self._runtime_commit = pins, flags, runtime_commit
        self._sequence, self._closed, self._token, self._previous = 0, False, None, None
        self._lock = threading.Lock()
        self.start_receipt = None


def _event(handle, event_type, stage, *, phase=None, outcome=None, reason_class=None, memory=None,
           phase_revision=None, phase_observed_at_utc=None, sample_started_at_utc=None, sample_completed_at_utc=None):
    flags = list(handle._flags)
    now = _clock()
    if now is None: flags.append("EVENT_CLOCK_UNAVAILABLE")
    if handle.process_session_started_at_utc is None: flags.append("SESSION_CLOCK_UNAVAILABLE")
    if handle._runtime_commit is None: flags.append("RUNTIME_COMMIT_UNAVAILABLE")
    if reason_class is not None and (type(reason_class) is not str or not _CLASS.fullmatch(reason_class)):
        reason_class = "UNKNOWN"; flags.append("REASON_CLASS_INVALID")
    if phase is None: phase = stage if stage in PHASES else "UNKNOWN"
    if type(phase) is not str or phase not in PHASES: phase = "UNKNOWN"; flags.append("PHASE_UNAVAILABLE")
    if phase_revision is not None and (type(phase_revision) is not int or not 0 <= phase_revision <= 1000000):
        phase_revision = None; flags.append("PHASE_REVISION_INVALID")
    clocks = {"phase_observed_at_utc":phase_observed_at_utc, "sample_started_at_utc":sample_started_at_utc, "sample_completed_at_utc":sample_completed_at_utc}
    for name, value in clocks.items():
        if value is not None and not _valid_clock(value): clocks[name] = None; flags.append(name.upper() + "_INVALID")
    if (clocks["sample_started_at_utc"] is None) != (clocks["sample_completed_at_utc"] is None):
        flags.append("SAMPLE_CLOCK_PAIR_INCOMPLETE")
    if clocks["sample_started_at_utc"] is not None and clocks["sample_completed_at_utc"] is not None:
        if datetime.fromisoformat(clocks["sample_started_at_utc"].replace("Z", "+00:00")) > datetime.fromisoformat(clocks["sample_completed_at_utc"].replace("Z", "+00:00")):
            flags.append("SAMPLE_CLOCK_ORDER_INVALID")
    captured_memory, memory_flags = _memory(memory)
    flags.extend(memory_flags)
    if outcome == "UNKNOWN": flags.append("OUTCOME_UNAVAILABLE")
    return {"version":VERSION, "process_session_id":handle.process_session_id,
        "process_session_started_at_utc":handle.process_session_started_at_utc, "cycle_id":handle.cycle_id,
        "sequence":handle._sequence, "event_type":event_type, "stage":stage, "phase":phase,
        "phase_revision":phase_revision, **clocks, "outcome":outcome, "reason_class":reason_class,
        "event_time_utc":now, "runtime_git_commit":handle._runtime_commit, "source_code_sha256":dict(handle._pins),
        "memory":captured_memory, "capture_flags":sorted(set(flags)), "clock_scope":CLOCK_SCOPE,
        "phase_scope":PHASE_SCOPE, "finish_scope":FINISH_SCOPE if event_type == "FINISH_REQUESTED" else None}


def prepare(event):
    value = _copy_json(event)
    expected = {"version", "process_session_id", "process_session_started_at_utc", "cycle_id", "sequence", "event_type", "stage", "phase",
        "phase_revision", "phase_observed_at_utc", "sample_started_at_utc", "sample_completed_at_utc", "outcome", "reason_class",
        "event_time_utc", "runtime_git_commit", "source_code_sha256", "memory", "capture_flags", "clock_scope", "phase_scope", "finish_scope"}
    _require(type(value) is dict and set(value) == expected and value["version"] == VERSION, "EVENT_ENVELOPE")
    _require(type(value["process_session_id"]) is str and re.fullmatch(r"[0-9a-f]{32}", value["process_session_id"]), "SESSION_ID")
    cycle = value["cycle_id"]
    _require(type(cycle) is str and cycle.startswith("shared-watch:") and len(cycle) <= 200 and _valid_clock(cycle[len("shared-watch:"):]), "CYCLE_ID")
    _require(type(value["sequence"]) is int and 1 <= value["sequence"] <= MAX_EVENTS_PER_CYCLE, "EVENT_SEQUENCE")
    kind = value["event_type"]
    _require(kind in {"START_REQUESTED", "STAGE_REQUESTED", "FINISH_REQUESTED"}, "EVENT_TYPE")
    _require((kind == "START_REQUESTED" and value["stage"] == "STARTING" and value["sequence"] == 1 and value["outcome"] is None) or
             (kind == "STAGE_REQUESTED" and value["stage"] in STAGES and value["sequence"] < MAX_EVENTS_PER_CYCLE and value["outcome"] is None) or
             (kind == "FINISH_REQUESTED" and value["stage"] == "FINALLY_ENTERED" and value["outcome"] in OUTCOMES), "EVENT_STAGE_OR_OUTCOME")
    _require(value["phase"] in PHASES and value["phase_scope"] == PHASE_SCOPE and value["clock_scope"] == CLOCK_SCOPE, "EVENT_SCOPE")
    _require(value["finish_scope"] == (FINISH_SCOPE if kind == "FINISH_REQUESTED" else None), "FINISH_SCOPE")
    _require(value["phase_revision"] is None or type(value["phase_revision"]) is int and 0 <= value["phase_revision"] <= 1000000, "PHASE_REVISION")
    for key in ("event_time_utc", "process_session_started_at_utc", "phase_observed_at_utc", "sample_started_at_utc", "sample_completed_at_utc"):
        _require(value[key] is None or _valid_clock(value[key]), "EVENT_CLOCK")
    _require(value["runtime_git_commit"] is None or type(value["runtime_git_commit"]) is str and re.fullmatch(r"[0-9a-f]{40,64}", value["runtime_git_commit"]), "RUNTIME_COMMIT")
    _require(value["reason_class"] is None or type(value["reason_class"]) is str and _CLASS.fullmatch(value["reason_class"]), "REASON_CLASS")
    pins = value["source_code_sha256"]
    _require(type(pins) is dict and len(pins) <= 16, "SOURCE_PINS")
    for name, pin in pins.items():
        _require(re.fullmatch(r"[A-Za-z0-9_]{1,100}\.py", name) and type(pin) is str and _HEX.fullmatch(pin), "SOURCE_PIN")
    flags = value["capture_flags"]
    _require(type(flags) is list and len(flags) <= 16 and flags == sorted(set(flags)) and
             all(type(flag) is str and re.fullmatch(r"[A-Z_]{1,80}", flag) for flag in flags), "CAPTURE_FLAGS")
    memory = value["memory"]
    _require(type(memory) is dict and memory.get("status") in {"NOT_REQUESTED", "CAPTURED", "UNAVAILABLE"}, "MEMORY_WRAPPER")
    if memory["status"] == "CAPTURED":
        _require(set(memory) == {"status", "sample", "sample_sha256"} and _memory_sample_valid(memory["sample"]) is True and
                 digest(memory["sample"]) == memory["sample_sha256"] and len(canonical(memory["sample"]).encode("utf-8")) <= MAX_MEMORY_BYTES, "MEMORY_SAMPLE")
    elif memory["status"] == "NOT_REQUESTED": _require(set(memory) == {"status"}, "MEMORY_NOT_REQUESTED")
    else: _require(memory == {"status":"UNAVAILABLE", "reason_code":"MEMORY_SNAPSHOT_INVALID"}, "MEMORY_UNAVAILABLE")
    text = canonical(value)
    return {**{key:value[key] for key in ("process_session_id", "cycle_id", "sequence", "event_type", "stage", "outcome")},
            "payload_sha256":hashlib.sha256(text.encode("utf-8")).hexdigest(), "payload_json":text,
            "payload_bytes":len(text.encode("utf-8"))}


def _jsonb_equal(left, right):
    if type(left) in (int, float) and type(right) in (int, float): return left == right
    if type(left) is not type(right): return False
    if type(left) is dict: return set(left) == set(right) and all(_jsonb_equal(left[key], right[key]) for key in left)
    if type(left) is list: return len(left) == len(right) and all(_jsonb_equal(a, b) for a,b in zip(left,right))
    return left == right


def _verify_existing(row, prepared):
    fields = ("process_session_id", "cycle_id", "sequence", "event_type", "stage", "outcome", "payload_sha256", "payload_json")
    _require(row is not None and all(row[key] == prepared[key] for key in fields), "EVENT_IDENTITY_COLLISION")
    _require(row.get("writer_version") == VERSION and row.get("writer_code_sha256") == _writer_code_sha256(), "EVENT_WRITER_IDENTITY_MISMATCH")
    _require(_jsonb_equal(row["payload"], json.loads(prepared["payload_json"])), "EVENT_JSONB_MISMATCH")


class PostgresStore:
    """Independent immutable metadata table; constructor has no IO."""
    def __init__(self, database_url): self.database_url, self.initialized = database_url, False
    def _connect(self):
        import psycopg
        from psycopg.rows import dict_row
        return psycopg.connect(self.database_url, connect_timeout=CONNECT_TIMEOUT_SECONDS, row_factory=dict_row)
    def persist(self, event, enqueued_at_utc=None): return self.persist_prepared(prepare(event), enqueued_at_utc or _clock())
    def persist_prepared(self, prepared, enqueued_at_utc):
        value = json.loads(prepared["payload_json"])
        _require(prepare(value) == prepared, "PREPARED_EVENT_CHANGED")
        from psycopg.types.json import Jsonb
        with self._connect() as conn:
            conn.execute("SET LOCAL statement_timeout = 3000")
            conn.execute("SET LOCAL lock_timeout = 1000")
            if not self.initialized:
                conn.execute("SELECT pg_advisory_xact_lock(%s)", (73182461390741541,))
                conn.execute('''CREATE TABLE IF NOT EXISTS research_runtime_journal_events (
                    process_session_id TEXT NOT NULL,cycle_id TEXT NOT NULL,sequence INTEGER NOT NULL CHECK(sequence BETWEEN 1 AND 32),
                    event_type TEXT NOT NULL,stage TEXT NOT NULL,outcome TEXT,
                    payload_sha256 TEXT NOT NULL,payload_json TEXT NOT NULL CHECK(octet_length(payload_json)<=32768),payload JSONB NOT NULL,
                    enqueued_at_utc TIMESTAMPTZ,writer_version TEXT NOT NULL,writer_code_sha256 TEXT NOT NULL,
                    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                    PRIMARY KEY(process_session_id,cycle_id,sequence))''')
            inserted = conn.execute('''INSERT INTO research_runtime_journal_events
                (process_session_id,cycle_id,sequence,event_type,stage,outcome,payload_sha256,payload_json,payload,enqueued_at_utc,writer_version,writer_code_sha256)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT(process_session_id,cycle_id,sequence) DO NOTHING RETURNING sequence''',
                tuple(prepared[key] for key in ("process_session_id", "cycle_id", "sequence", "event_type", "stage", "outcome", "payload_sha256", "payload_json")) +
                (Jsonb(value), enqueued_at_utc, VERSION, _writer_code_sha256())).fetchone()
            row = conn.execute('''SELECT process_session_id,cycle_id,sequence,event_type,stage,outcome,payload_sha256,payload_json,payload,writer_version,writer_code_sha256,created_at_utc
                FROM research_runtime_journal_events WHERE process_session_id=%s AND cycle_id=%s AND sequence=%s''',
                (prepared["process_session_id"], prepared["cycle_id"], prepared["sequence"])).fetchone()
            _verify_existing(row, prepared)
            created = row["created_at_utc"]
            if isinstance(created, datetime): created = created.astimezone(timezone.utc).isoformat()
            result = {**_summary(prepared), "persisted":True, "idempotent_existing":inserted is None, "created_at_utc":created}
        self.initialized = True
        return result


def _persist_one(prepared, enqueued_at):
    try:
        ack = _STORE.persist_prepared(prepared, enqueued_at)
        with _LOCK:
            _METRICS["persisted"] += 1
            _METRICS["idempotent_existing"] += int(ack["idempotent_existing"])
            result = {**ack, "status":"PERSISTED", "durable":True, "at_utc":_clock()}
            _METRICS["last"] = _METRICS["last_durable"] = result
            if prepared["event_type"] == "START_REQUESTED":
                _METRICS["start_persisted"] += 1; _METRICS["last_durable_start"] = result
            if prepared["event_type"] == "FINISH_REQUESTED":
                _METRICS["finish_persisted"] += 1; _METRICS["last_durable_finish"] = result
    except BaseException as error: _failure(_error_code(error), prepared=prepared)
    finally:
        with _LOCK:
            _METRICS["queued_items"] -= 1; _METRICS["queued_bytes"] -= prepared["payload_bytes"]


def _run():
    while True:
        prepared, enqueued_at = _QUEUE.get()
        try: _persist_one(prepared, enqueued_at)
        finally: _QUEUE.task_done()


def _ensure_worker():
    global _WORKER, _STORE
    with _WORKER_LOCK:
        if _WORKER is not None and _WORKER.is_alive(): return True
        if _STORE is None:
            url = os.getenv("RESEARCH_DATABASE_URL", "").strip() or os.getenv("DATABASE_URL", "").strip()
            if not url: return False
            _STORE = PostgresStore(url)
        _WORKER = threading.Thread(target=_run, name="research-runtime-journal", daemon=True)
        _WORKER.start()
        return True


def submit(event):
    """Freeze/enqueue without waiting for a database operation; never raise."""
    prepared = None
    try:
        with _LOCK: _METRICS["enqueue_attempts"] += 1
        try: prepared = prepare(event)
        except BaseException as error: return _failure(_error_code(error), status="INVALID")
        if not _ensure_worker(): return _failure("DATABASE_NOT_CONFIGURED", status="UNAVAILABLE", prepared=prepared)
        enqueued_at = _clock()
        with _LOCK:
            if _METRICS["queued_items"] >= MAX_QUEUE_ITEMS or _METRICS["queued_bytes"] + prepared["payload_bytes"] > MAX_QUEUE_BYTES:
                _METRICS["dropped"] += 1
                result = {"version":VERSION, "status":"DROPPED", "reason_code":"QUEUE_BOUND", "durable":False, **_summary(prepared)}
                _METRICS["last"] = {**result, "at_utc":enqueued_at}; return result
            _METRICS["queued_items"] += 1; _METRICS["queued_bytes"] += prepared["payload_bytes"]
            try: _QUEUE.put_nowait((prepared, enqueued_at))
            except BaseException:
                _METRICS["queued_items"] -= 1; _METRICS["queued_bytes"] -= prepared["payload_bytes"]
                raise
            _METRICS["enqueued"] += 1
            if prepared["event_type"] == "START_REQUESTED": _METRICS["start_enqueued"] += 1
            if prepared["event_type"] == "FINISH_REQUESTED": _METRICS["finish_enqueued"] += 1
            result = {"version":VERSION, "status":"QUEUED", "durable":False, **_summary(prepared)}
            _METRICS["last"] = {**result, "at_utc":enqueued_at}; return result
    except BaseException as error: return _failure(_error_code(error), status="UNAVAILABLE", prepared=prepared)


def begin(cycle_id, *, runtime_commit=None, source_code_sha256=None, memory=None):
    """Enqueue START before optional probes; return an opaque per-cycle handle."""
    handle = None
    try:
        with _LOCK: _METRICS["begin_calls"] += 1
        _require(type(cycle_id) is str and cycle_id.startswith("shared-watch:") and len(cycle_id) <= 200 and _valid_clock(cycle_id[len("shared-watch:"):]), "CYCLE_ID")
        if runtime_commit is None: runtime_commit = os.getenv("RENDER_GIT_COMMIT")
        if type(runtime_commit) is not str or re.fullmatch(r"[0-9a-f]{40,64}", runtime_commit) is None: runtime_commit = None
        pins, flags = _source_pins(source_code_sha256)
        handle = _Handle(cycle_id, _session(), pins, flags, runtime_commit)
        handle._previous = _CURRENT.get()
        handle._token = _CURRENT.set(handle)
        with handle._lock:
            handle._sequence = 1
            handle.start_receipt = submit(_event(handle, "START_REQUESTED", "STARTING", memory=memory))
        with _LOCK: _METRICS["started_cycles"] += 1
        return handle
    except BaseException as error:
        if handle is not None: reset(handle)
        _failure(_error_code(error), status="OBSERVER_ERROR")
        return None


def current():
    try:
        handle = _CURRENT.get()
        return handle if isinstance(handle, _Handle) and not handle._closed else None
    except BaseException: return None


def stage(name, *, handle=None, memory=None, phase=None, reason_class=None, phase_revision=None,
          phase_observed_at_utc=None, sample_started_at_utc=None, sample_completed_at_utc=None):
    try:
        with _LOCK: _METRICS["stage_calls"] += 1
        handle = current() if handle is None else handle
        if not isinstance(handle, _Handle): return {"version":VERSION, "status":"IGNORED", "reason_code":"NO_ACTIVE_HANDLE", "durable":False}
        if type(name) is not str or name not in STAGES: return _failure("STAGE_UNSUPPORTED", status="DROPPED")
        with handle._lock:
            if handle._closed: return _failure("HANDLE_CLOSED", status="DROPPED")
            if handle._sequence >= MAX_EVENTS_PER_CYCLE - 1: return _failure("CYCLE_EVENT_BOUND_FINISH_RESERVED", status="DROPPED")
            handle._sequence += 1
            return submit(_event(handle, "STAGE_REQUESTED", name, phase=phase, reason_class=reason_class, memory=memory,
                phase_revision=phase_revision, phase_observed_at_utc=phase_observed_at_utc,
                sample_started_at_utc=sample_started_at_utc, sample_completed_at_utc=sample_completed_at_utc))
    except BaseException as error: return _failure(_error_code(error), status="OBSERVER_ERROR")


def reset(handle):
    """Restore this scope and unwind closed outers without clobbering live ones."""
    try:
        if not isinstance(handle, _Handle) or _CURRENT.get() is not handle: return
        seen = set()
        for _ in range(MAX_EVENTS_PER_CYCLE):
            if id(handle) in seen:
                _CURRENT.set(None)
                _failure("CONTEXT_RESET_CYCLE", status="OBSERVER_ERROR")
                return
            seen.add(id(handle))
            try:
                if handle._token is None: _CURRENT.set(handle._previous)
                else: _CURRENT.reset(handle._token)
            except BaseException: _CURRENT.set(handle._previous)
            previous = _CURRENT.get()
            if not isinstance(previous, _Handle) or not previous._closed: return
            handle = previous
        _CURRENT.set(None)
        _failure("CONTEXT_RESET_BOUND", status="OBSERVER_ERROR")
    except BaseException: pass


def finish(handle, *, status="COMPLETED", reason_class=None, memory=None):
    """Close against late/racing probes and request a finally-entry write."""
    try:
        with _LOCK: _METRICS["finish_calls"] += 1
        if not isinstance(handle, _Handle): return {"version":VERSION, "status":"IGNORED", "reason_code":"NO_ACTIVE_HANDLE", "durable":False}
        with handle._lock:
            if handle._closed: return {"version":VERSION, "status":"IGNORED", "reason_code":"ALREADY_FINISHED", "durable":False}
            handle._closed = True
            handle._sequence += 1
            outcome = status.upper() if type(status) is str and status.upper() in OUTCOMES else "UNKNOWN"
            result = submit(_event(handle, "FINISH_REQUESTED", "FINALLY_ENTERED", outcome=outcome,
                                   reason_class=reason_class, memory=memory))
        with _LOCK: _METRICS["finished_cycles"] += 1
        return result
    except BaseException as error: return _failure(_error_code(error), status="OBSERVER_ERROR")
    finally: reset(handle)


def status():
    try:
        with _LOCK: metrics = json.loads(json.dumps(_METRICS))
        return {"version":VERSION, "scope":"PASSIVE_RUNTIME_METADATA_ONLY_NO_NATIVE_STATE_OR_TRANSPORT",
            "table":TABLE, "worker_alive":bool(_WORKER and _WORKER.is_alive()), "queue_is_durable":False,
            "shared_research_event_queue_used":False, "process_session_id":_SESSION[0] if _SESSION else None,
            "configured":bool(_STORE is not None or os.getenv("RESEARCH_DATABASE_URL") or os.getenv("DATABASE_URL")),
            "process_session_started_at_utc":_SESSION[1] if _SESSION else None,
            "session_clock_semantics":"Lazy observer-session first use, not operating-system process birth or proof of earliest activation.",
            "start_semantics":"START_REQUESTED queue acceptance is not durable start acknowledgement; fast interruption may prevent persistence.",
            "finish_semantics":FINISH_SCOPE, "phase_semantics":PHASE_SCOPE,
            "persistence_retry_policy":"No automatic retry; failed, unavailable and dropped events are explicit observation gaps.",
            "finish_reservation_scope":"A per-cycle sequence slot only; no queue-capacity, commit or durability reservation is guaranteed.",
            "limits":{"event_bytes":MAX_EVENT_BYTES, "memory_sample_bytes":MAX_MEMORY_BYTES, "events_per_cycle":MAX_EVENTS_PER_CYCLE,
                "queue_items":MAX_QUEUE_ITEMS, "queue_canonical_utf8_bytes":MAX_QUEUE_BYTES,
                "connect_timeout_seconds":CONNECT_TIMEOUT_SECONDS, "statement_timeout_ms":STATEMENT_TIMEOUT_MS, "lock_timeout_ms":LOCK_TIMEOUT_MS},
            **metrics}
    except BaseException: return {"version":VERSION, "status":"UNAVAILABLE", "reason_code":"STATUS_UNAVAILABLE"}
