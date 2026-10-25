"""Isolated descriptive evaluator; source studies and journals remain read-only.

One owned executor thread performs all database and audit work. A separate
session lock protects only the additive evaluation table. PASS results are
immutable; a failed result can only be upgraded to PASS for the same input.
"""
from __future__ import annotations

import asyncio
import copy
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import hashlib
import importlib
import json
from pathlib import Path
import re
import time
from uuid import uuid4

STUDY_ID = "doge-eth-first-observed-paper-render-v1"
PACKAGE = Path(__file__).resolve().parent / "forward_evaluation_runtime"
LOCK_KEY = int.from_bytes(hashlib.sha256(b"forward-paper-descriptive-evaluation-v1").digest()[:8], "big") & ((1 << 63) - 1)
MAX_QUOTE_PACKETS = 50000
MAX_CASE_WINDOWS = 256
MAX_EVIDENCE_SECONDS = 20
BASELINE_KEYS = ("study_id", "policy_sha256", "config_sha256", "study_start_utc")


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value):
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _utc():
    return datetime.now(timezone.utc).isoformat()


def _time(value):
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ValueError("UTC timestamp required")
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise ValueError("Explicit UTC required")
    return result.astimezone(timezone.utc)


def _object(value):
    return json.loads(value) if isinstance(value, str) else copy.deepcopy(value)


def _connect(dsn):
    import psycopg
    return psycopg.connect(
        dsn, connect_timeout=5, application_name="forward_paper_evaluator",
        options="-c statement_timeout=5000 -c lock_timeout=2000 -c timezone=UTC",
        keepalives=1, keepalives_idle=5, keepalives_interval=2,
        keepalives_count=2, tcp_user_timeout=10000,
    )


class EvaluationStorageError(RuntimeError):
    """Sanitized error; no driver exception text is exposed."""


class EvidenceLimitExceeded(RuntimeError):
    pass


DDL = (
    """CREATE TABLE IF NOT EXISTS research_forward_paper_evaluations (
        study_id TEXT NOT NULL,
        evaluator_version TEXT NOT NULL,
        dataset_sha256 TEXT NOT NULL,
        source_fingerprint TEXT NOT NULL,
        evaluator_source_sha256 TEXT NOT NULL,
        baseline_sha256 TEXT NOT NULL,
        source_journal_seq BIGINT NOT NULL,
        policy_sha256 TEXT NOT NULL,
        audit_status TEXT NOT NULL CHECK(audit_status IN ('PASS','FAIL')),
        assessment_json JSONB NOT NULL,
        assessment_sha256 TEXT NOT NULL,
        writer_runtime_commit TEXT,
        evidence_sha256 TEXT NOT NULL,
        created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
        completed_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY(study_id,evaluator_version,dataset_sha256)
    )""",
    """CREATE INDEX IF NOT EXISTS idx_forward_paper_evaluations_latest
        ON research_forward_paper_evaluations(study_id,completed_at_utc DESC)""",
)


def _case_windows(snapshot):
    """Malformed cases stay available for the pure core to reject explicitly."""
    windows = set()
    books = (snapshot.get("state_json") or {}).get("books") or {}
    for book in books.values():
        if not isinstance(book, dict):
            continue
        for case in book.get("closed") or []:
            if not isinstance(case, dict):
                continue
            try:
                start = _time(case["selected_at_utc"]) - timedelta(seconds=30)
                end = _time(case["exit_received_utc"]) + timedelta(seconds=30)
                if start > end:
                    continue
                windows.add((start.isoformat(), end.isoformat()))
            except (KeyError, ValueError, TypeError):
                continue
    if len(windows) > MAX_CASE_WINDOWS:
        raise EvidenceLimitExceeded("Too many case windows for bounded evaluation")
    return sorted(windows)


class SourceRead:
    def __init__(self, connection, snapshot, server_time, *, max_packets=MAX_QUOTE_PACKETS):
        self.connection = connection
        self.snapshot = snapshot
        self.server_time = server_time
        self.max_packets = max_packets

    def quotes(self):
        windows = _case_windows(self.snapshot)
        if not windows:
            return []
        values = ",".join("(%s::timestamptz,%s::timestamptz)" for _ in windows)
        parameters = [v for pair in windows for v in pair]
        parameters += [STUDY_ID, int(self.snapshot["journal_seq"]), self.max_packets + 1]
        sql = """WITH windows(started,finished) AS (VALUES """ + values + """)
            SELECT json_build_object('seq',j.seq,'kind',j.kind,'utc',j.utc,
                'input_key',j.input_key,'payload_json',j.payload_json,
                'payload_sha256',j.payload_sha256,'recorded_at_utc',j.recorded_at_utc)
            FROM research_forward_paper_journal j
            WHERE j.study_id=%s AND j.seq<=%s AND j.kind='INPUT_QUOTES'
              AND EXISTS(SELECT 1 FROM windows w WHERE
                (j.payload_json->>'response_received_utc')::timestamptz
                BETWEEN w.started AND w.finished)
            ORDER BY j.seq LIMIT %s"""
        records = []
        began = time.monotonic()
        with self.connection.cursor(name="forward_evaluation_" + uuid4().hex) as cursor:
            cursor.execute(sql, parameters)
            while True:
                if time.monotonic() - began > MAX_EVIDENCE_SECONDS:
                    raise EvidenceLimitExceeded("Evidence read exceeded bounded duration")
                batch = cursor.fetchmany(500)
                if not batch:
                    break
                records.extend(_object(row[0]) for row in batch)
                if len(records) > self.max_packets:
                    raise EvidenceLimitExceeded("Evidence packet cap reached; nothing was truncated")
        return records


class EvaluationStore:
    def __init__(self, dsn, *, connect=None, max_packets=MAX_QUOTE_PACKETS):
        self._dsn = dsn
        self._connect = connect or _connect
        self.max_packets = max_packets
        self.connection = None
        self.pid = None
        self.leader = False

    def acquire(self):
        self.close()
        try:
            self.connection = self._connect(self._dsn)
            self.connection.autocommit = True
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT pg_try_advisory_lock(%s),pg_backend_pid()", (LOCK_KEY,))
                acquired, self.pid = cursor.fetchone()
            if not acquired:
                self.close()
                return False
            self.leader = True
            with self._transaction() as cursor:
                for sql in DDL:
                    cursor.execute(sql)
            return True
        except Exception:
            self.close()
            raise EvaluationStorageError("Evaluation storage initialization failed") from None

    def _assert_leader(self):
        if not self.leader or self.connection is None or bool(getattr(self.connection, "closed", False)):
            raise EvaluationStorageError("Evaluation writer has no live leader session")
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT pg_backend_pid()")
            if cursor.fetchone()[0] != self.pid:
                raise EvaluationStorageError("Evaluation leader session changed")

    @contextmanager
    def _transaction(self):
        self._assert_leader()
        cursor = self.connection.cursor()
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
            cursor.close()

    @contextmanager
    def source(self):
        connection = None
        try:
            connection = self._connect(self._dsn)
            connection.autocommit = False
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                cursor.execute("SET LOCAL statement_timeout = '5s'")
                cursor.execute("SET LOCAL lock_timeout = '2s'")
                cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                cursor.execute(
                    "SELECT row_to_json(s),statement_timestamp() FROM research_forward_paper_studies s WHERE study_id=%s",
                    (STUDY_ID,),
                )
                row = cursor.fetchone()
            if row is None:
                raise ValueError("Study is not activated")
            snapshot = _object(row[0])
            if not isinstance(snapshot, dict):
                raise ValueError("Invalid study projection")
            yield SourceRead(connection, snapshot, _time(row[1]), max_packets=self.max_packets)
        finally:
            if connection is not None:
                try:
                    connection.rollback()
                except Exception:
                    pass
                connection.close()

    def find_pass(self, version, dataset_sha):
        self._assert_leader()
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT assessment_json,completed_at_utc,source_journal_seq,assessment_sha256 FROM research_forward_paper_evaluations "
                "WHERE study_id=%s AND evaluator_version=%s AND dataset_sha256=%s AND audit_status='PASS'",
                (STUDY_ID, version, dataset_sha),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        assessment = _object(row[0])
        if _digest(assessment) != row[3] or assessment.get("status") != "PASS":
            raise EvaluationStorageError("Cached evaluation integrity check failed")
        return {"assessment": assessment, "completed_at_utc": _time(row[1]).isoformat(),
                "source_journal_seq": int(row[2])}

    def latest(self, version, code_sha):
        self._assert_leader()
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT assessment_json,assessment_sha256 FROM research_forward_paper_evaluations "
                "WHERE study_id=%s AND evaluator_version=%s AND evaluator_source_sha256=%s "
                "ORDER BY completed_at_utc DESC LIMIT 1", (STUDY_ID, version, code_sha),
            )
            row = cursor.fetchone()
        if not row:
            return None
        assessment = _object(row[0])
        if _digest(assessment) != row[1]:
            raise EvaluationStorageError("Previous evaluation integrity check failed")
        return assessment

    def persist(self, result, metadata):
        status = result.get("status")
        if status not in {"PASS", "FAIL"}:
            raise ValueError("Only complete pass/fail assessments may be persisted")
        if result.get("dataset_fingerprint") != metadata["source_fingerprint"] or result.get("evaluator_version") != metadata["version"]:
            raise ValueError("Assessment identity differs from the requested evaluator input")
        payload = _json(result)
        sql = """INSERT INTO research_forward_paper_evaluations
            (study_id,evaluator_version,dataset_sha256,source_fingerprint,
             evaluator_source_sha256,baseline_sha256,source_journal_seq,policy_sha256,audit_status,
             assessment_json,assessment_sha256,writer_runtime_commit,evidence_sha256)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s)
            ON CONFLICT(study_id,evaluator_version,dataset_sha256) DO UPDATE SET
                source_journal_seq=EXCLUDED.source_journal_seq,
                audit_status=EXCLUDED.audit_status,assessment_json=EXCLUDED.assessment_json,
                assessment_sha256=EXCLUDED.assessment_sha256,
                writer_runtime_commit=EXCLUDED.writer_runtime_commit,
                evidence_sha256=EXCLUDED.evidence_sha256,completed_at_utc=now()
            WHERE research_forward_paper_evaluations.audit_status<>'PASS'
              AND EXCLUDED.audit_status='PASS'
            RETURNING completed_at_utc"""
        parameters = (STUDY_ID, metadata["version"], metadata["dataset_sha256"], metadata["source_fingerprint"],
                      metadata["code_sha256"], metadata["baseline_sha256"], metadata["source_journal_seq"], metadata["policy_sha256"], status,
                      payload, hashlib.sha256(payload.encode()).hexdigest(), metadata.get("runtime_commit"),
                      metadata["evidence_sha256"])
        try:
            with self._transaction() as cursor:
                cursor.execute(sql, parameters)
                row = cursor.fetchone()
            return _time(row[0]).isoformat() if row else None
        except Exception:
            self.close()
            raise EvaluationStorageError("Evaluation persistence failed; leadership must be reacquired") from None

    def close(self):
        connection = self.connection
        self.connection = None
        self.leader = False
        self.pid = None
        if connection is not None:
            try:
                connection.close()  # Session closure also releases the advisory lock.
            except Exception:
                pass


class ForwardEvaluationWorker:
    def __init__(self, *, package=None, store_factory=None, core_module=None,
                 first_delay=5, poll_seconds=60, retry_seconds=30):
        self.package = Path(package) if package is not None else PACKAGE
        self._store_factory = store_factory or EvaluationStore
        self._core = core_module
        self.first_delay = first_delay
        self.poll_seconds = poll_seconds
        self.retry_seconds = retry_seconds
        self._executor = self._task = self._stop = None
        self._dsn = self._store = self._baseline = None
        self._code_sha = self._version = self._baseline_sha = None
        self._last_log = 0.0
        self._last_state = None
        self._health = {"study_id": STUDY_ID, "enabled": False, "configured": False,
                        "state": "DISABLED", "leader": False, "runtime_commit": None,
                        "poll_seconds": poll_seconds, "polls": 0, "audits_attempted": 0,
                        "assessments_written": 0, "unchanged_skips": 0, "failures": 0,
                        "audit_failures": 0, "pending_attempts": 0, "last_error_type": None,
                        "source_journal_seq": None, "source_updated_at_utc": None,
                        "source_age_seconds": None, "last_checked_utc": None,
                        "last_assessment_utc": None, "assessment": None,
                        "assessment_source_journal_seq": None, "assessment_persisted": False}

    def status(self):
        result = copy.deepcopy(self._health)
        result["running"] = bool(self._task and not self._task.done())
        result["trade_execution"] = False
        result["notifications"] = False
        result["strategy_approved"] = False
        return result

    def _publish(self, **values):
        self._health = {**self._health, **values}

    def _log(self, *, force=False):
        tick = time.monotonic()
        if force or self._health["state"] != self._last_state or tick - self._last_log >= 60:
            print("[FORWARD_EVALUATION] " + _json(self.status()), flush=True)
            self._last_log = tick
            self._last_state = self._health["state"]

    async def start(self, *, database_url=None, enabled=False, runtime_commit=None):
        if self._task and not self._task.done():
            return True
        commit = str(runtime_commit or "")
        commit = commit if re.fullmatch(r"[0-9a-fA-F]{7,64}", commit) else None
        self._publish(enabled=bool(enabled), configured=bool(database_url), runtime_commit=commit)
        if not enabled or not database_url:
            self._publish(state="DISABLED")
            return False
        self._dsn = database_url
        self._stop = asyncio.Event()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="forward-evaluation")
        self._publish(state="STARTING")
        self._task = asyncio.create_task(self._run(), name="forward-paper-evaluation")
        return True

    async def _wait(self, seconds):
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=max(.001, seconds))
            return True
        except asyncio.TimeoutError:
            return False

    async def _run(self):
        if await self._wait(self.first_delay):
            return
        while not self._stop.is_set():
            began = time.monotonic()
            delay = self.poll_seconds
            try:
                await asyncio.get_running_loop().run_in_executor(self._executor, self._cycle_owned)
                delay = max(.05, self.poll_seconds - (time.monotonic() - began))
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._publish(state="RETRYING", leader=False, failures=self._health["failures"] + 1,
                              last_error_type=type(error).__name__)
                self._log(force=True)
                delay = self.retry_seconds
            if await self._wait(delay):
                break

    def _open_owned(self):
        if self._core is None:
            self._core = importlib.import_module("research_forward_evaluation")
        self._version = self._core.EVALUATOR_VERSION
        source_path = getattr(self._core, "__file__", None)
        source = Path(source_path).read_text(encoding="utf8").replace("\r\n", "\n") if source_path else self._version
        self._code_sha = hashlib.sha256(source.encode()).hexdigest()
        self._baseline = json.loads((self.package / "baseline.json").read_text(encoding="utf8"))
        self._baseline_sha = _digest({key: self._baseline.get(key) for key in BASELINE_KEYS})
        self._store = self._store_factory(self._dsn)
        if not self._store.acquire():
            self._discard_owned()
            self._publish(state="STANDBY", leader=False)
            self._log()
            return False
        self._publish(leader=True, evaluator_version=self._version, evaluator_source_sha256=self._code_sha,
                      baseline_sha256=self._baseline_sha)
        return True

    def _cycle_owned(self):
        try:
            if self._store is None and not self._open_owned():
                return
            with self._store.source() as source:
                snapshot = source.snapshot
                age = (source.server_time - _time(snapshot["updated_at_utc"])).total_seconds()
                source_status = snapshot.get("last_status") or {}
                receipt_ages = {}
                for key in ("last_source_received_utc", "last_quote_received_utc"):
                    value = source_status.get(key)
                    if value is not None:
                        receipt_ages[key] = (source.server_time - _time(value)).total_seconds()
                threshold = max(60, float((snapshot.get("policy_json") or {}).get("poll_seconds", 10)) * 6)
                self._publish(polls=self._health["polls"] + 1, source_journal_seq=int(snapshot["journal_seq"]),
                              source_updated_at_utc=snapshot["updated_at_utc"], source_age_seconds=age,
                              last_checked_utc=source.server_time.isoformat(), leader=True,
                              last_source_receipt_age_seconds=receipt_ages.get("last_source_received_utc"),
                              last_quote_receipt_age_seconds=receipt_ages.get("last_quote_received_utc"))
                if any(v < -5 or v > threshold for v in [age, *receipt_ages.values()]):
                    self._publish(state="SOURCE_STALE", last_error_type="SourceClockOrFreshness")
                    self._log()
                    return
                if source_status.get("state") != "OBSERVING" or not source_status.get("leader"):
                    self._publish(state="SOURCE_NOT_OBSERVING", last_error_type="SourceNotObserving")
                    self._log()
                    return
                if len(receipt_ages) != 2:
                    self._publish(state="SOURCE_NOT_OBSERVING", last_error_type="SourceReceiptClockMissing")
                    self._log()
                    return
                fingerprint = self._core.stable_fingerprint(snapshot)
                if not isinstance(fingerprint, str) or not re.fullmatch(r"[0-9a-f]{64}", fingerprint):
                    raise ValueError("Invalid evaluator fingerprint")
                dataset_sha = _digest({"source_fingerprint": fingerprint, "evaluator_source_sha256": self._code_sha,
                                       "baseline_sha256": self._baseline_sha})
                cached = self._store.find_pass(self._version, dataset_sha)
                if cached is not None:
                    if cached["assessment"].get("dataset_fingerprint") != fingerprint or cached["assessment"].get("evaluator_version") != self._version:
                        raise EvaluationStorageError("Cached evaluation identity differs from its input")
                    self._publish(state="WAITING_FOR_NEW_DATA", last_error_type=None,
                                  unchanged_skips=self._health["unchanged_skips"] + 1,
                                  dataset_sha256=dataset_sha, last_assessment_utc=cached["completed_at_utc"],
                                  assessment_source_journal_seq=cached["source_journal_seq"], assessment_persisted=True,
                                  assessment=self._core.safe_summary(cached["assessment"]))
                    self._log()
                    return
                previous = self._store.latest(self._version, self._code_sha)
                packets = source.quotes()
            # Close the repeatable read before CPU work or assessment writes.
            result = self._core.assess(snapshot, packets, expected_baseline=self._baseline,
                                       previous_assessment=previous)
            _json(result)  # Fail closed on nonfinite or unserializable audit output.
            status = result.get("status")
            if status not in {"PASS", "FAIL", "PENDING_PRICE_DATA"}:
                raise ValueError("Unknown assessment status")
            if result.get("dataset_fingerprint") != fingerprint or result.get("evaluator_version") != self._version:
                raise ValueError("Assessment identity does not match its input")
            now = _utc()
            self._publish(audits_attempted=self._health["audits_attempted"] + 1,
                          dataset_sha256=dataset_sha, last_assessment_utc=now,
                          assessment_source_journal_seq=int(snapshot["journal_seq"]), assessment_persisted=False,
                          assessment=self._core.safe_summary(result), last_error_type=None)
            if status == "PENDING_PRICE_DATA":
                self._publish(state="PENDING_PRICE_DATA", pending_attempts=self._health["pending_attempts"] + 1)
                self._log()
                return
            metadata = {"version": self._version, "dataset_sha256": dataset_sha,
                        "source_fingerprint": fingerprint, "code_sha256": self._code_sha,
                        "baseline_sha256": self._baseline_sha,
                        "source_journal_seq": int(snapshot["journal_seq"]), "policy_sha256": snapshot["policy_sha256"],
                        "runtime_commit": self._health["runtime_commit"],
                        "evidence_sha256": _digest([[p.get("seq"), p.get("payload_sha256")] for p in packets])}
            completed = self._store.persist(result, metadata)
            self._publish(state="AUDIT_PASSED" if status == "PASS" else "AUDIT_FAILED",
                          assessment_persisted=completed is not None,
                          assessments_written=self._health["assessments_written"] + int(completed is not None),
                          audit_failures=self._health["audit_failures"] + int(status == "FAIL"))
            self._log()
        except EvidenceLimitExceeded:
            self._publish(state="EVIDENCE_LIMIT", last_error_type="EvidenceLimitExceeded")
            self._log(force=True)
        except Exception:
            self._discard_owned()
            raise

    def _discard_owned(self):
        store, self._store = self._store, None
        if store is not None:
            store.close()

    async def stop(self):
        if self._task is None:
            return
        self._stop.set()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            await asyncio.get_running_loop().run_in_executor(self._executor, self._discard_owned)
            self._executor.shutdown(wait=True)
            self._executor = self._task = self._dsn = None
            self._publish(state="STOPPED", leader=False)


WORKER = ForwardEvaluationWorker()


async def start_worker(*, database_url=None, enabled=False, runtime_commit=None):
    return await WORKER.start(database_url=database_url, enabled=enabled, runtime_commit=runtime_commit)


async def stop_worker():
    await WORKER.stop()


def status():
    return WORKER.status()
