"""Authoritative PostgreSQL checkpoints for the isolated paper-study journal.

No account, quote transport, order, notification or production-table access.
The connection holding the session advisory lock is also the only writer.
After a database error the instance is poisoned: discard its SQLite engine,
acquire a new leader session and restore the authoritative PostgreSQL state.
"""
from __future__ import annotations

import copy
import hashlib
import json
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path


class CheckpointError(RuntimeError):
    """Safe operational error whose message never includes connection details."""


class ConfigurationDrift(CheckpointError):
    pass


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def _hash(value):
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _object(value):
    if value is None:
        return None
    return json.loads(value) if isinstance(value, str) else copy.deepcopy(value)


def _default_connect(dsn):
    options = {"connect_timeout": 5, "application_name": "forward_paper_checkpoint",
               "options": "-c statement_timeout=5000 -c lock_timeout=2000 -c timezone=UTC",
               "keepalives": 1, "keepalives_idle": 5, "keepalives_interval": 2,
               "keepalives_count": 2, "tcp_user_timeout": 10000}
    try:
        import psycopg2
    except ImportError:
        import psycopg
        return psycopg.connect(dsn, **options)
    return psycopg2.connect(dsn, **options)


DDL = (
    """CREATE TABLE IF NOT EXISTS research_forward_paper_studies (
        study_id TEXT PRIMARY KEY,
        config_sha256 TEXT NOT NULL,
        policy_sha256 TEXT NOT NULL,
        policy_json JSONB NOT NULL,
        state_json JSONB,
        journal_seq BIGINT NOT NULL DEFAULT 0,
        last_status JSONB,
        activated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
    """CREATE TABLE IF NOT EXISTS research_forward_paper_journal (
        study_id TEXT NOT NULL,
        seq BIGINT NOT NULL,
        kind TEXT NOT NULL,
        utc TIMESTAMPTZ,
        input_key TEXT,
        payload_json JSONB NOT NULL,
        payload_sha256 TEXT NOT NULL,
        recorded_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY(study_id, seq),
        UNIQUE(study_id, input_key)
    )""",
)


class PostgresCheckpoint:
    """Session leader plus transactional state/journal persistence.

    ``config`` is an immutable policy template without study_start_utc.
    It may additionally pin transport/query versions. A study's actual start
    is generated once, before its first bootstrap, and is never regenerated.

    Historical input identities are hydrated as metadata-only SQLite rows
    for cross-restart idempotency; they are excluded from new checkpoints.
    Their payload hashes refer to authoritative PostgreSQL payloads, not to
    the small local placeholder payloads. SQLite is a disposable temporary
    file with a 2MiB page cache; PostgreSQL is its sole recovery authority.
    """

    def __init__(self, dsn, study_id, config: dict, connect=None):
        if not isinstance(study_id, str) or not study_id or len(study_id) > 200:
            raise ValueError("A bounded nonempty study identifier is required")
        if not isinstance(config, dict) or "study_start_utc" in config:
            raise ValueError("Configuration must be a template without study_start_utc")
        self._dsn = dsn
        self.study_id = study_id
        self.config = copy.deepcopy(config)
        self.config_sha256 = _hash(self.config)
        self.lock_key = int.from_bytes(hashlib.sha256(study_id.encode()).digest()[:8], "big") & ((1 << 63) - 1)
        self._connect = connect or _default_connect
        self._conn = None
        self._leader = False
        self._backend_pid = None
        self._loaded = None
        self._engine = None
        self._local_cursor = 0
        self._pg_seq = 0
        self._temporary = None
        self._cache_engine = None

    @property
    def journal_sequence(self):
        return self._pg_seq

    def acquire(self) -> bool:
        """Acquire leadership or return False without creating any tables."""
        if self._leader:
            self._assert_leader()
            return True
        self.close()
        try:
            conn = self._connect(self._dsn)
            conn.autocommit = True
            self._conn = conn
            with conn.cursor() as cursor:
                cursor.execute("SELECT pg_try_advisory_lock(%s), pg_backend_pid()", (self.lock_key,))
                acquired, pid = cursor.fetchone()
            if not acquired:
                self.close()
                return False
            self._leader = True
            self._backend_pid = pid
            with conn.cursor() as cursor:
                cursor.execute("SET statement_timeout = '5s'")
                cursor.execute("SET lock_timeout = '2s'")
                cursor.execute("SET TIME ZONE 'UTC'")
            return True
        except Exception:
            self._poison()
            raise CheckpointError("Paper-study leader connection could not be acquired") from None

    def _assert_leader(self):
        if not self._leader or self._conn is None or bool(getattr(self._conn, "closed", False)):
            self._poison()
            raise CheckpointError("Paper-study writer has no live leader session")
        try:
            with self._conn.cursor() as cursor:
                cursor.execute("SELECT pg_backend_pid()")
                if cursor.fetchone()[0] != self._backend_pid:
                    raise ValueError("Session changed")
        except Exception:
            self._poison()
            raise CheckpointError("Paper-study leader session was lost; restore is required") from None

    @contextmanager
    def _transaction(self):
        self._assert_leader()
        cursor = self._conn.cursor()
        try:
            cursor.execute("BEGIN")
            yield cursor
            cursor.execute("COMMIT")
        except Exception:
            try:
                cursor.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            try:
                cursor.close()
            except Exception:
                pass

    def initialize_or_load(self):
        """Atomically initialize one activation clock or load its frozen state."""
        try:
            with self._transaction() as cursor:
                for sql in DDL:
                    cursor.execute(sql)
                cursor.execute(
                    "SELECT config_sha256,policy_sha256,policy_json,state_json,journal_seq,last_status "
                    "FROM research_forward_paper_studies WHERE study_id=%s FOR UPDATE",
                    (self.study_id,))
                row = cursor.fetchone()
                if row is None:
                    policy = copy.deepcopy(self.config)
                    policy["study_start_utc"] = datetime.now(timezone.utc).isoformat()
                    policy_sha = _hash(policy)
                    cursor.execute(
                        "INSERT INTO research_forward_paper_studies "
                        "(study_id,config_sha256,policy_sha256,policy_json,state_json,journal_seq,last_status) "
                        "VALUES(%s,%s,%s,%s::jsonb,NULL,0,%s::jsonb)",
                        (self.study_id, self.config_sha256, policy_sha,
                         _canonical(policy), _canonical({"state": "ACTIVATED_AWAITING_BOOTSTRAP"})))
                    loaded = {"policy": policy, "policy_sha256": policy_sha,
                              "state": None, "journal_seq": 0,
                              "last_status": {"state": "ACTIVATED_AWAITING_BOOTSTRAP"}}
                else:
                    cfg_sha, policy_sha, raw_policy, raw_state, sequence, status = row
                    if cfg_sha != self.config_sha256:
                        raise ConfigurationDrift("Frozen paper-study configuration changed; a new study identifier is required")
                    policy = _object(raw_policy)
                    if _hash(policy) != policy_sha:
                        raise ConfigurationDrift("Stored paper-study policy integrity check failed")
                    loaded = {"policy": policy, "policy_sha256": policy_sha,
                              "state": _object(raw_state), "journal_seq": int(sequence),
                              "last_status": _object(status)}
            self._loaded = loaded
            self._pg_seq = loaded["journal_seq"]
            return copy.deepcopy(loaded)
        except ConfigurationDrift:
            self._poison()
            raise
        except Exception:
            self._poison()
            raise CheckpointError("Paper-study initialization failed; no local state may be used") from None

    def restore_engine(self, engine_class):
        """Restore a disposable file core; PostgreSQL remains the authority."""
        loaded = self.initialize_or_load() if self._loaded is None else self._loaded
        self._assert_leader()
        engine = None
        try:
            self._temporary = tempfile.TemporaryDirectory(prefix="forward-paper-")
            engine = engine_class(str(Path(self._temporary.name) / "working.sqlite"),
                                  copy.deepcopy(loaded["policy"]), resume=False)
            self._cache_engine = engine
            engine.db.execute("PRAGMA cache_size=-2048")
            if engine.policy_sha256 != loaded["policy_sha256"]:
                raise ValueError("Core policy digest differs")
            if loaded["state"] is not None:
                engine.db.execute("BEGIN IMMEDIATE")
                try:
                    engine._save(copy.deepcopy(loaded["state"]))
                    with self._conn.cursor() as cursor:
                        cursor.execute(
                            "SELECT input_key,payload_sha256 FROM research_forward_paper_journal "
                            "WHERE study_id=%s AND input_key IS NOT NULL ORDER BY seq", (self.study_id,))
                        while True:
                            records = cursor.fetchmany(1000)
                            if not records:
                                break
                            for input_key, payload_sha in records:
                                engine.db.execute(
                                    "INSERT INTO journal(kind,utc,input_key,payload_json,payload_sha256) VALUES(?,?,?,?,?)",
                                    ("RESTORED_INPUT_IDENTITY_METADATA", None, input_key,
                                     _canonical({"metadata_only": True, "authoritative_payload": "PostgreSQL", "do_not_archive": True}),
                                     payload_sha))
                    engine.db.execute("COMMIT")
                except Exception:
                    engine.db.execute("ROLLBACK")
                    raise
                # Skip the core's temporary constructor event and all identity
                # placeholders. Only actual events after restore are appended.
                self._local_cursor = engine.db.execute("SELECT COALESCE(MAX(event_id),0) FROM journal").fetchone()[0]
                if not hasattr(engine, "resume_observer"):
                    raise ValueError("Core must expose resume_observer for a real process resume")
                engine.resume_observer()
            else:
                self._local_cursor = 0
            self._engine = engine
            return engine
        except Exception:
            if engine is not None:
                try:
                    engine.close()
                except Exception:
                    pass
            self._poison()
            self._cleanup_cache()
            raise CheckpointError("Paper-study state restoration failed; observations must stop") from None

    def checkpoint(self, engine, status):
        """Commit journal append, state and health in one PG transaction.

        The in-memory engine may already have consumed an observation. On any
        failure it MUST be discarded, even if COMMIT acknowledgement was lost.
        A subsequent leader restores whichever complete transaction persisted.
        """
        if engine is not self._engine or self._loaded is None:
            raise CheckpointError("Checkpoint requires the engine restored by this writer")
        try:
            if engine.policy_sha256 != self._loaded["policy_sha256"]:
                raise ConfigurationDrift("Local paper-study policy changed")
            rows = engine.db.execute(
                "SELECT event_id,kind,utc,input_key,payload_json,payload_sha256 FROM journal "
                "WHERE event_id>? ORDER BY event_id", (self._local_cursor,)).fetchall()
            state_json = _canonical(engine._load())
            next_sequence = self._pg_seq + len(rows)
            safe_status = copy.deepcopy(status)
            safe_status["durable_journal_seq"] = next_sequence
            safe_status["journal_events"] = next_sequence
            with self._transaction() as cursor:
                cursor.execute(
                    "SELECT journal_seq,config_sha256,policy_sha256 FROM research_forward_paper_studies "
                    "WHERE study_id=%s FOR UPDATE", (self.study_id,))
                row = cursor.fetchone()
                if row is None or int(row[0]) != self._pg_seq:
                    raise ValueError("Checkpoint sequence changed")
                if row[1] != self.config_sha256 or row[2] != self._loaded["policy_sha256"]:
                    raise ConfigurationDrift("Durable paper-study identity changed")
                for offset, (_, kind, utc, input_key, payload_json, payload_sha) in enumerate(rows, 1):
                    if kind == "RESTORED_INPUT_IDENTITY_METADATA":
                        raise ValueError("Identity metadata must not enter the authoritative journal")
                    if hashlib.sha256(payload_json.encode()).hexdigest() != payload_sha:
                        raise ValueError("Local journal payload integrity mismatch")
                    cursor.execute(
                        "INSERT INTO research_forward_paper_journal "
                        "(study_id,seq,kind,utc,input_key,payload_json,payload_sha256) "
                        "VALUES(%s,%s,%s,%s,%s,%s::jsonb,%s)",
                        (self.study_id, self._pg_seq + offset, kind, utc, input_key, payload_json, payload_sha))
                cursor.execute(
                    "UPDATE research_forward_paper_studies SET state_json=%s::jsonb,journal_seq=%s,"
                    "last_status=%s::jsonb,updated_at_utc=now() WHERE study_id=%s AND journal_seq=%s",
                    (state_json, next_sequence, _canonical(safe_status), self.study_id, self._pg_seq))
                if cursor.rowcount != 1:
                    raise ValueError("Checkpoint update did not match authoritative state")
            self._pg_seq = next_sequence
            if rows:
                self._local_cursor = rows[-1][0]
            self._loaded["state"] = _object(state_json)
            self._loaded["journal_seq"] = next_sequence
            self._loaded["last_status"] = safe_status
        except ConfigurationDrift:
            self._poison()
            raise
        except Exception:
            self._poison()
            raise CheckpointError("Paper-study checkpoint failed; discard local engine and restore") from None

    def update_health(self, status):
        """Update only health; never advance state or journal after an error."""
        if self._loaded is None:
            raise CheckpointError("Initialize the durable study before recording health")
        try:
            safe_status = copy.deepcopy(status)
            safe_status["durable_journal_seq"] = self._pg_seq
            safe_status["journal_events"] = self._pg_seq
            with self._transaction() as cursor:
                cursor.execute(
                    "UPDATE research_forward_paper_studies SET last_status=%s::jsonb,updated_at_utc=now() "
                    "WHERE study_id=%s AND journal_seq=%s AND config_sha256=%s",
                    (_canonical(safe_status), self.study_id, self._pg_seq, self.config_sha256))
                if cursor.rowcount != 1:
                    raise ValueError("Health does not match authoritative state")
            self._loaded["last_status"] = safe_status
        except Exception:
            self._poison()
            raise CheckpointError("Paper-study health update failed; writer must stop") from None

    def _poison(self):
        self._leader = False
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
        self._conn = None
        self._backend_pid = None
        self._loaded = None
        self._engine = None

    def close(self):
        connection = self._conn
        if connection is not None and self._leader:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT pg_advisory_unlock(%s)", (self.lock_key,))
            except Exception:
                pass
        self._poison()
        self._cleanup_cache()

    def _cleanup_cache(self):
        if self._cache_engine is not None:
            try:
                self._cache_engine.close()
            except Exception:
                pass
            self._cache_engine = None
        if self._temporary is not None:
            try:
                self._temporary.cleanup()
            except OSError:
                # A caller violating the discard requirement may retain an
                # open handle on Windows. A later close can retry cleanup.
                return
            self._temporary = None
