"""Independent bounded persistence for passive Watch decision capsules.

No schema, connection, thread or write occurs on import. submit() freezes a
strict bounded JSON copy, then queues it without waiting for database work.
Queue acceptance is not durable acknowledgment. Existing market archives,
research-event queues, native state, and message delivery are never touched.
"""
from __future__ import annotations

from datetime import datetime, timezone
from contextlib import closing, contextmanager
from functools import lru_cache
import hashlib
import json
import math
import os
from pathlib import Path
import queue
import re
import sqlite3
import threading

VERSION = 'alert-decision-store-v1'
CAPTURE_VERSION = 'alert-decision-capture-v1'
MAX_CAPSULE_BYTES = 256 * 1024
MAX_NODES = 16384
MAX_DEPTH = 12
MAX_STRING_CHARS = MAX_CAPSULE_BYTES
MAX_QUEUE_ITEMS = 8
MAX_QUEUE_BYTES = MAX_QUEUE_ITEMS * MAX_CAPSULE_BYTES
CONNECT_TIMEOUT_SECONDS = 3
STATEMENT_TIMEOUT_MS = 3000
LOCK_TIMEOUT_MS = 1000
TABLE = 'research_alert_decision_cycles'
_HEX = re.compile(r'^[0-9a-f]{64}$')
_FORBIDDEN_KEYS = frozenset(('chat_id','recipient','recipient_id','message','message_text',
    'text','token','attempt_token','secret','password','database_url','api_key',
    'authorization','credentials'))
_QUEUE = queue.Queue(maxsize=MAX_QUEUE_ITEMS)
_WORKER = None
_STORE = None
_WORKER_LOCK = threading.Lock()
_LOCK = threading.Lock()
_METRICS = dict(enqueue_attempts=0,enqueued=0,persisted=0,failed=0,dropped=0,
    unavailable=0,idempotent_existing=0,queued_items=0,queued_bytes=0,last=None,last_durable=None)


class CapsuleValidationError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def _metric_clock():
    try:
        return _utc_now()
    except BaseException:
        # Unknown is explicit; a diagnostic clock must not mask persistence.
        return None


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()


def _require(value, code):
    if not value:
        raise CapsuleValidationError(code)


def _bounded_copy(value):
    remaining, nodes, active = MAX_CAPSULE_BYTES, 0, set()
    def spend(size):
        nonlocal remaining
        remaining -= size
        _require(remaining >= 0,'CAPSULE_BYTE_BOUND')
    def text_size(value):
        _require(len(value) <= MAX_STRING_CHARS,'CAPSULE_STRING_BOUND')
        return len(json.dumps(value,ensure_ascii=False).encode('utf-8'))
    def visit(item, depth):
        nonlocal nodes
        nodes += 1
        _require(nodes <= MAX_NODES and depth <= MAX_DEPTH,'CAPSULE_STRUCTURE_BOUND')
        kind = type(item)
        if item is None or kind is bool:
            spend(4 if item is None or item is True else 5)
            return item
        if kind is str:
            spend(text_size(item))
            return item
        if kind is int:
            _require(item.bit_length() <= 1024,'CAPSULE_INTEGER_BOUND')
            spend(len(str(item)))
            return item
        if kind is float:
            _require(math.isfinite(item),'CAPSULE_NONFINITE_NUMBER')
            spend(len(json.dumps(item,allow_nan=False)))
            return item
        _require(kind in (list,dict),'CAPSULE_NOT_JSON')
        _require(len(item) <= MAX_NODES and id(item) not in active,'CAPSULE_STRUCTURE_BOUND')
        active.add(id(item))
        try:
            spend(2+max(0,len(item)-1))
            if kind is list:
                return [visit(child,depth+1) for child in item]
            output = {}
            for key, child in item.items():
                _require(type(key) is str,'CAPSULE_NONSTRING_KEY')
                _require(key.lower() not in _FORBIDDEN_KEYS,'CAPSULE_FORBIDDEN_FIELD')
                spend(text_size(key)+1)
                output[key] = visit(child,depth+1)
            return output
        finally:
            active.remove(id(item))
    return visit(value,0)


def prepare(capsule):
    """Freeze the exact strict-JSON hash preimage before retaining any reference."""
    copied = _bounded_copy(capsule)
    _require(type(copied) is dict and set(copied) == {'version','cycle_id','status','payload','payload_sha256'},'CAPSULE_ENVELOPE')
    _require(copied['version'] == CAPTURE_VERSION,'CAPSULE_VERSION')
    cycle = copied['cycle_id']
    _require(type(cycle) is str and cycle.startswith('shared-watch:') and 14 < len(cycle) <= 200,'CAPSULE_CYCLE_ID')
    try:
        cycle_time = datetime.fromisoformat(cycle[len('shared-watch:'):].replace('Z','+00:00'))
    except (TypeError,ValueError):
        raise CapsuleValidationError('CAPSULE_CYCLE_ID') from None
    _require(cycle_time.tzinfo is not None,'CAPSULE_CYCLE_ID')
    _require(copied['status'] in ('COMPLETE','PARTIAL','UNAVAILABLE'),'CAPSULE_STATUS')
    _require(type(copied['payload']) is dict,'CAPSULE_PAYLOAD')
    pins = copied['payload'].get('source_code_sha256')
    _require(type(pins) is dict and len(pins) <= 32,'CAPSULE_SOURCE_PINS')
    for name, pin in pins.items():
        _require(bool(re.fullmatch(r'[A-Za-z0-9_]{1,100}\.py',name)) and type(pin) is str and bool(_HEX.fullmatch(pin)),'CAPSULE_SOURCE_PIN')
    pin = copied['payload_sha256']
    _require(type(pin) is str and bool(_HEX.fullmatch(pin)),'CAPSULE_HASH_SHAPE')
    _require(digest({k:v for k,v in copied.items() if k != 'payload_sha256'}) == pin,'CAPSULE_HASH_MISMATCH')
    payload_json = canonical(copied)
    size = len(payload_json.encode('utf-8'))
    _require(size <= MAX_CAPSULE_BYTES,'CAPSULE_BYTE_BOUND')
    return dict(cycle_id=cycle,capture_version=copied['version'],capture_status=copied['status'],
                payload_sha256=pin,payload_json=payload_json,payload_bytes=size)


@lru_cache(maxsize=1)
def _writer_code_sha256():
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _row_summary(row, *, idempotent):
    created = row['created_at_utc']
    if isinstance(created,datetime):
        created = created.astimezone(timezone.utc).isoformat()
    return dict(persisted=True,idempotent_existing=idempotent,cycle_id=row['cycle_id'],
        capture_version=row['capture_version'],capture_status=row['capture_status'],
        payload_sha256=row['payload_sha256'],created_at_utc=created)


def _verify_existing(row, prepared):
    if not row or any(row[k] != prepared[k] for k in ('cycle_id','capture_version','capture_status','payload_sha256','payload_json')):
        raise CapsuleValidationError('CYCLE_IDENTITY_COLLISION')


@contextmanager
def _sqlite_connection(path):
    with closing(sqlite3.connect(path,timeout=1.)) as conn:
        with conn:
            yield conn


class SQLiteStore:
    """Explicit local test backend; never selected automatically at runtime."""
    def __init__(self, path):
        self.path = str(path)
    def persist(self, capsule, enqueued_at_utc=None):
        prepared = prepare(capsule)
        return self.persist_prepared(prepared,enqueued_at_utc or _utc_now())
    def persist_prepared(self, prepared, enqueued_at_utc):
        # Revalidate the frozen preimage at the persistence boundary too.
        _require(prepare(json.loads(prepared['payload_json'])) == prepared,'PREPARED_CAPSULE_CHANGED')
        with _sqlite_connection(self.path) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute('PRAGMA busy_timeout=1000')
            conn.execute('''CREATE TABLE IF NOT EXISTS research_alert_decision_cycles (
                cycle_id TEXT NOT NULL,capture_version TEXT NOT NULL,capture_status TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,payload_json TEXT NOT NULL,enqueued_at_utc TEXT NOT NULL,
                writer_version TEXT NOT NULL,writer_code_sha256 TEXT NOT NULL,
                created_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                PRIMARY KEY(cycle_id,capture_version))''')
            cur = conn.execute('''INSERT INTO research_alert_decision_cycles
                (cycle_id,capture_version,capture_status,payload_sha256,payload_json,enqueued_at_utc,writer_version,writer_code_sha256)
                VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(cycle_id,capture_version) DO NOTHING''',
                tuple(prepared[k] for k in ('cycle_id','capture_version','capture_status','payload_sha256','payload_json'))+
                (enqueued_at_utc,VERSION,_writer_code_sha256()))
            row = conn.execute('SELECT * FROM research_alert_decision_cycles WHERE cycle_id=? AND capture_version=?',
                (prepared['cycle_id'],prepared['capture_version'])).fetchone()
            _verify_existing(row,prepared)
            return _row_summary(row,idempotent=cur.rowcount == 0)


class PostgresStore:
    def __init__(self, database_url):
        self.database_url = database_url
        self.initialized = False
    def _connect(self):
        import psycopg
        from psycopg.rows import dict_row
        # Respect DSN search_path/options used by isolated CI schemas.
        return psycopg.connect(self.database_url,connect_timeout=CONNECT_TIMEOUT_SECONDS,row_factory=dict_row)
    def persist(self, capsule, enqueued_at_utc=None):
        return self.persist_prepared(prepare(capsule),enqueued_at_utc or _utc_now())
    def persist_prepared(self, prepared, enqueued_at_utc):
        from psycopg.types.json import Jsonb
        capsule = json.loads(prepared['payload_json'])
        _require(prepare(capsule) == prepared,'PREPARED_CAPSULE_CHANGED')
        with self._connect() as conn:
            conn.execute('SET LOCAL statement_timeout = 3000')
            conn.execute('SET LOCAL lock_timeout = 1000')
            if not self.initialized:
                conn.execute('SELECT pg_advisory_xact_lock(%s)',(73182461390741539,))
                conn.execute('''CREATE TABLE IF NOT EXISTS research_alert_decision_cycles (
                    cycle_id TEXT NOT NULL,capture_version TEXT NOT NULL,capture_status TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,payload_json TEXT NOT NULL,payload JSONB NOT NULL,
                    enqueued_at_utc TIMESTAMPTZ NOT NULL,writer_version TEXT NOT NULL,
                    writer_code_sha256 TEXT NOT NULL,created_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                    PRIMARY KEY(cycle_id,capture_version))''')
            inserted = conn.execute('''INSERT INTO research_alert_decision_cycles
                (cycle_id,capture_version,capture_status,payload_sha256,payload_json,payload,enqueued_at_utc,writer_version,writer_code_sha256)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT(cycle_id,capture_version) DO NOTHING RETURNING cycle_id''',
                tuple(prepared[k] for k in ('cycle_id','capture_version','capture_status','payload_sha256','payload_json'))+
                (Jsonb(capsule),enqueued_at_utc,VERSION,_writer_code_sha256())).fetchone()
            row = conn.execute('''SELECT cycle_id,capture_version,capture_status,payload_sha256,payload_json,created_at_utc
                FROM research_alert_decision_cycles WHERE cycle_id=%s AND capture_version=%s''',
                (prepared['cycle_id'],prepared['capture_version'])).fetchone()
            _verify_existing(row,prepared)
            result = _row_summary(row,idempotent=inserted is None)
        # Context-manager exit committed successfully before reporting durable.
        self.initialized = True
        return result


def _error_code(exc):
    if isinstance(exc,CapsuleValidationError):
        return exc.code
    name = type(exc).__name__
    return name if re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,79}',name) else 'Exception'


def _failure(code, *, cycle_id=None, status='FAILED'):
    with _LOCK:
        _METRICS['dropped' if status == 'DROPPED' else 'failed'] += 1
        _METRICS['unavailable'] += int(status == 'UNAVAILABLE')
        _METRICS['last'] = dict(status=status,reason_code=code,cycle_id=cycle_id,at_utc=_metric_clock())
    return dict(version=VERSION,status=status,reason_code=code,cycle_id=cycle_id)


def _persist_one(prepared, enqueued_at_utc):
    try:
        result = _STORE.persist_prepared(prepared,enqueued_at_utc)
        with _LOCK:
            _METRICS['persisted'] += 1
            _METRICS['idempotent_existing'] += int(result['idempotent_existing'])
            _METRICS['last_durable'] = {**result,'status':'PERSISTED','at_utc':_metric_clock()}
            _METRICS['last'] = dict(_METRICS['last_durable'])
    except BaseException as exc:
        _failure(_error_code(exc),cycle_id=prepared['cycle_id'])
    finally:
        with _LOCK:
            _METRICS['queued_items'] -= 1
            _METRICS['queued_bytes'] -= prepared['payload_bytes']


def _run():
    while True:
        prepared, enqueued_at = _QUEUE.get()
        try:
            _persist_one(prepared,enqueued_at)
        finally:
            _QUEUE.task_done()


def _ensure_worker():
    global _WORKER,_STORE
    with _WORKER_LOCK:
        if _WORKER is not None and _WORKER.is_alive():
            return True
        if _STORE is None:
            url = os.getenv('RESEARCH_DATABASE_URL','').strip() or os.getenv('DATABASE_URL','').strip()
            if not url:
                return False
            _STORE = PostgresStore(url)
        _WORKER = threading.Thread(target=_run,name='research-alert-decisions',daemon=True)
        _WORKER.start()
        return True


def submit(capsule):
    """Never raise to Watch; QUEUED is explicitly different from PERSISTED."""
    try:
        with _LOCK:
            _METRICS['enqueue_attempts'] += 1
        prepared = prepare(capsule)
        if not _ensure_worker():
            return _failure('DATABASE_NOT_CONFIGURED',cycle_id=prepared['cycle_id'],status='UNAVAILABLE')
        enqueued_at = _utc_now()
        with _LOCK:
            if _METRICS['queued_items'] >= MAX_QUEUE_ITEMS or _METRICS['queued_bytes']+prepared['payload_bytes'] > MAX_QUEUE_BYTES:
                _METRICS['dropped'] += 1
                result = dict(version=VERSION,status='DROPPED',reason_code='QUEUE_BOUND',cycle_id=prepared['cycle_id'])
                _METRICS['last'] = {**result,'at_utc':enqueued_at}
                return result
            _METRICS['queued_items'] += 1
            _METRICS['queued_bytes'] += prepared['payload_bytes']
            try:
                _QUEUE.put_nowait((prepared,enqueued_at))
            except BaseException:
                _METRICS['queued_items'] -= 1
                _METRICS['queued_bytes'] -= prepared['payload_bytes']
                raise
            _METRICS['enqueued'] += 1
            result = dict(version=VERSION,status='QUEUED',cycle_id=prepared['cycle_id'],
                          payload_sha256=prepared['payload_sha256'],durable=False)
            _METRICS['last'] = {**result,'at_utc':enqueued_at}
            return result
    except BaseException as exc:
        return _failure(_error_code(exc))


def status():
    with _LOCK:
        snapshot = json.loads(json.dumps(_METRICS))
    return dict(version=VERSION,scope='passive-watch-decision-capsules-only',
        worker_alive=bool(_WORKER and _WORKER.is_alive()),table=TABLE,
        queue_is_durable=False,shared_research_event_queue_used=False,
        persistence_retry_policy='No automatic retry; a failed capsule is an explicit observation gap.',
        limits=dict(capsule_bytes=MAX_CAPSULE_BYTES,queue_items=MAX_QUEUE_ITEMS,
                    queue_bytes=MAX_QUEUE_BYTES,connect_timeout_seconds=CONNECT_TIMEOUT_SECONDS,
                    statement_timeout_ms=STATEMENT_TIMEOUT_MS,lock_timeout_ms=LOCK_TIMEOUT_MS),**snapshot)
