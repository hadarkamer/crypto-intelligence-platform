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
MAX_SOURCE_PACKETS = 5000
MAX_SOURCE_ROWS = 20000
MAX_SOURCE_DECISIONS = 10000
MAX_PREDICATE_ROWS = 2000
MAX_PREDICATE_BYTES = 20_000_000
SOURCE_DECISION_KINDS = ("PARENT_CHOICE_FROZEN", "BOOTSTRAP_EXCLUDED", "INPUT_REJECTED", "POLICY_SOURCE_DRIFT")
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


def _implementation_identity(core):
    """Bind persistent cache identity to every executable audit component."""
    normalized = lambda path: hashlib.sha256(Path(path).read_text(encoding="utf8").replace("\r\n", "\n").encode()).hexdigest()
    components = {"worker": normalized(__file__)}
    core_path = getattr(core, "__file__", None)
    components["core"] = normalized(core_path) if core_path else hashlib.sha256(core.EVALUATOR_VERSION.encode()).hexdigest()
    required = {"forward-evaluation-v2": {"source_audit"},
                "forward-evaluation-v3": {"source_audit", "predicate_audit"}}.get(core.EVALUATOR_VERSION, set())
    for name, filename in (("source_audit", "research_forward_source_audit.py"),
                           ("predicate_audit", "research_forward_predicate_audit.py")):
        path = Path(core_path).with_name(filename) if core_path else None
        if path is not None and path.exists():
            components[name] = normalized(path)
        elif name in required:
            raise ValueError("A required audit implementation is missing")
    return _digest(components), components


def _source_selection_marker(snapshot, counts):
    """Quiet empty polls remain quiet; new meaningful source evidence does not."""
    cutoff = int(snapshot["journal_seq"])
    state = snapshot.get("state_json") or {}
    inputs = int(counts["total_input_packets"])
    rejected = int(counts["rejected_input_packets"])
    candidate_total = int(counts["candidate_packets_total"])
    marker = {key: counts.get(key) for key in (
        "candidate_selected_packets_total", "candidate_nonempty_packets_total",
        "candidate_bootstrap_packets_total", "candidate_packet_rows",
        "max_candidate_selected_seq", "source_decision_rows_total",
        "max_source_decision_seq", "rejected_input_packets", "candidate_invalid_rows_packets_total")}
    marker["journal_prefix_contiguous"] = (int(counts["total_journal_rows"]) == cutoff and
        int(counts.get("min_journal_seq") or 0) == 1 and int(counts.get("max_journal_seq") or 0) == cutoff)
    marker["input_totals_consistent"] = (inputs == candidate_total + int(counts["quote_packets_total"]) and
        inputs - rejected == int(state.get("observation_sequence", -1)))
    marker["candidate_totals_consistent"] = candidate_total == (
        int(counts["candidate_empty_packets_total"]) + int(counts["candidate_nonempty_packets_total"]))
    return _digest(marker)


def _cache_preserves_latest_pins(cached, previous, snapshot):
    """An older exact cache cannot erase newer successful prospective pins."""
    try:
        if previous is None or previous["status"] != cached["status"] or cached["status"] != "PASS":
            return False
        if previous["evaluator_version"] != cached["evaluator_version"]:
            return False
        if type(previous["source_journal_seq"]) is not int or int(snapshot["journal_seq"]) < previous["source_journal_seq"]:
            return False
        if any(previous[field] != snapshot[field] or cached[field] != snapshot[field]
               for field in ("study_id", "policy_sha256", "config_sha256")):
            return False
        old, current = previous["predicate_audit"], cached["predicate_audit"]
        if old["status"] != "PASS" or current["status"] != "PASS":
            return False
        if any(not old.get(field) or old[field] != current.get(field)
               for field in ("version", "retained_record_digest_version")):
            return False
        def pins(audit):
            result = {}
            for case in audit["cases"]:
                key, digest = case["event_key"], case["retained_record_sha256"]
                if not isinstance(key, str) or key in result or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
                    raise ValueError("Invalid retained record pin")
                result[key] = digest
            if len(result) != audit["summary"]["retained_matches_recomputed"]:
                raise ValueError("Incomplete retained record pins")
            return result
        prior_pins, cached_pins = pins(old), pins(current)
        return all(cached_pins.get(key) == digest for key, digest in prior_pins.items())
    except (KeyError, TypeError, ValueError):
        return False


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
        self._evidence_started = time.monotonic()
        self._counts = None

    def _predicate_requested(self):
        scopes = {}
        for scope in (self.snapshot.get("policy_json") or {}).get("scopes") or []:
            if not isinstance(scope, dict) or not isinstance(scope.get("scope_key"), str) or not isinstance(scope.get("candidate_key"), str):
                raise EvaluationStorageError("Malformed predicate scope metadata")
            if scope["scope_key"] in scopes:
                raise EvaluationStorageError("Duplicate predicate scope metadata")
            scopes[scope["scope_key"]] = scope["candidate_key"]
        requested = []
        first_seen = (self.snapshot.get("state_json") or {}).get("first_seen") or {}
        if not isinstance(first_seen, dict):
            raise EvaluationStorageError("Malformed predicate event identities")
        for key in sorted(first_seen):
            if not isinstance(key, str) or ":" not in key:
                raise EvaluationStorageError("Malformed predicate event identity")
            scope_key, raw_id = key.rsplit(":", 1)
            if scope_key not in scopes or not re.fullmatch(r"(?:0|[1-9][0-9]*)", raw_id):
                raise EvaluationStorageError("Unknown predicate event identity")
            event_id = int(raw_id)
            if event_id > (1 << 63) - 1:
                raise EvaluationStorageError("Predicate event identity is out of range")
            requested.append((scope_key, scopes[scope_key], event_id))
        candidates = sorted(set(scopes.values()))
        if len(requested) + len(candidates) > MAX_PREDICATE_ROWS:
            raise EvidenceLimitExceeded("Predicate evidence row cap reached")
        return requested, candidates

    def _predicate_records(self, query, parameters, cap, remaining_bytes):
        if time.monotonic() - self._evidence_started > MAX_EVIDENCE_SECONDS:
            raise EvidenceLimitExceeded("Predicate read exceeded bounded duration")
        preflight = "SELECT count(*),COALESCE(sum(octet_length(row_to_json(p)::text)),0) FROM (" + query + ") p"
        with self.connection.cursor() as cursor:
            cursor.execute(preflight, parameters)
            count, byte_count = cursor.fetchone()
        count, byte_count = int(count), int(byte_count)
        # Account for enclosing list brackets and commas in the byte budget.
        byte_count += 2 * count + 2
        if count > cap or byte_count > remaining_bytes:
            raise EvidenceLimitExceeded("Predicate evidence row or byte cap reached")
        records = self._records("SELECT row_to_json(p) FROM (" + query + ") p LIMIT %s",
                                [*parameters, cap + 1], cap, "Predicate")
        if len(records) != count:
            raise EvaluationStorageError("Predicate evidence changed within its source snapshot")
        return records, byte_count

    def predicate_evidence(self):
        """Read current retained positives/definitions before consulting PASS cache."""
        requested, candidates = self._predicate_requested()
        keys = sorted(scope + ":" + str(event_id) for scope, _, event_id in requested)
        used_bytes = len(_json({"expected_keys": keys}).encode()) + 1024
        if used_bytes > MAX_PREDICATE_BYTES:
            raise EvidenceLimitExceeded("Predicate evidence byte cap reached")
        definition_sql = """/* PREDICATE_DEFINITIONS */
            WITH requested AS(SELECT unnest(%s::text[]) AS candidate_key)
            SELECT q.candidate_key,(c.candidate_key IS NOT NULL) AS definition_present,
                c.formula_version,c.definition_sha256,c.definition
            FROM requested q LEFT JOIN research_ordered_formula_candidates c ON c.candidate_key=q.candidate_key
            ORDER BY q.candidate_key"""
        definitions, size = self._predicate_records(definition_sql, [candidates], len(candidates), MAX_PREDICATE_BYTES - used_bytes)
        used_bytes += size
        matches = []
        if requested:
            values = ",".join("(%s::text,%s::text,%s::bigint)" for _ in requested)
            parameters = [value for triple in requested for value in triple]
            match_sql = """/* PREDICATE_MATCHES */
                WITH requested(scope_key,candidate_key,event_id) AS(VALUES """ + values + """)
                SELECT q.scope_key,q.candidate_key,q.event_id,(m.event_id IS NOT NULL) AS match_present,
                    (e.event_id IS NOT NULL) AS source_present,m.symbol,m.direction,m.alert_time_utc,
                    m.snapshot_id,m.entry_price,m.created_at_utc,m.decision_features,
                    e.direction AS source_direction,e.symbol AS source_symbol,
                    e.alert_time_utc AS source_alert_time_utc
                FROM requested q LEFT JOIN research_ordered_formula_matches m
                    ON m.candidate_key=q.candidate_key AND m.event_id=q.event_id
                LEFT JOIN research_events e ON e.event_id=q.event_id
                ORDER BY q.scope_key,q.event_id"""
            matches, size = self._predicate_records(match_sql, parameters, len(requested), MAX_PREDICATE_BYTES - used_bytes)
            used_bytes += size
        definitions.sort(key=lambda row: str(row.get("candidate_key")))
        matches.sort(key=lambda row: (str(row.get("scope_key")), int(row.get("event_id", -1))))
        evidence = {"matches": matches, "definitions": definitions, "expected_keys": keys,
                    "counts": {"expected_positive_keys": len(keys),
                               "matched_rows": sum(row.get("match_present") is True for row in matches),
                               "definition_rows": sum(row.get("definition_present") is True for row in definitions)},
                    "source_journal_seq": int(self.snapshot["journal_seq"]),
                    "read_utc": self.server_time.isoformat(), "extraction_complete": True}
        if len(_json(evidence).encode()) > MAX_PREDICATE_BYTES:
            raise EvidenceLimitExceeded("Predicate evidence byte cap reached")
        return evidence

    def counts(self):
        if self._counts is not None:
            return copy.deepcopy(self._counts)
        sql = """WITH base AS MATERIALIZED (
            SELECT seq,kind,
                CASE WHEN kind='INPUT_CANDIDATES' AND jsonb_typeof(payload_json->'rows')='array'
                     THEN jsonb_array_length(payload_json->'rows') ELSE NULL END AS row_count,
                CASE WHEN kind='INPUT_CANDIDATES' THEN COALESCE((payload_json->>'bootstrap')::boolean,false)
                     ELSE false END AS bootstrap
            FROM research_forward_paper_journal WHERE study_id=%s AND seq<=%s)
            SELECT json_build_object(
              'candidate_packets_total',count(*) FILTER(WHERE kind='INPUT_CANDIDATES'),
              'candidate_nonempty_packets_total',count(*) FILTER(WHERE kind='INPUT_CANDIDATES' AND row_count>0),
              'candidate_empty_packets_total',count(*) FILTER(WHERE kind='INPUT_CANDIDATES' AND row_count=0),
              'candidate_bootstrap_packets_total',count(*) FILTER(WHERE kind='INPUT_CANDIDATES' AND bootstrap),
              'candidate_selected_packets_total',count(*) FILTER(WHERE kind='INPUT_CANDIDATES' AND (bootstrap OR row_count>0)),
              'candidate_packet_rows',COALESCE(sum(row_count) FILTER(WHERE kind='INPUT_CANDIDATES'),0),
              'candidate_invalid_rows_packets_total',count(*) FILTER(WHERE kind='INPUT_CANDIDATES' AND row_count IS NULL),
              'quote_packets_total',count(*) FILTER(WHERE kind='INPUT_QUOTES'),
              'total_input_packets',count(*) FILTER(WHERE kind IN('INPUT_CANDIDATES','INPUT_QUOTES')),
              'rejected_input_packets',count(*) FILTER(WHERE kind='INPUT_REJECTED'),
              'source_decision_rows_total',count(*) FILTER(WHERE kind IN('PARENT_CHOICE_FROZEN','BOOTSTRAP_EXCLUDED','INPUT_REJECTED','POLICY_SOURCE_DRIFT')),
              'total_journal_rows',count(*),'min_journal_seq',min(seq),'max_journal_seq',max(seq),
              'max_candidate_selected_seq',max(seq) FILTER(WHERE kind='INPUT_CANDIDATES' AND (bootstrap OR row_count>0)),
              'max_source_decision_seq',max(seq) FILTER(WHERE kind IN('PARENT_CHOICE_FROZEN','BOOTSTRAP_EXCLUDED','INPUT_REJECTED','POLICY_SOURCE_DRIFT')))
            FROM base"""
        with self.connection.cursor() as cursor:
            cursor.execute(sql, (STUDY_ID, int(self.snapshot["journal_seq"])))
            row = cursor.fetchone()
        counts = _object(row[0]) if row else None
        if not isinstance(counts, dict) or counts.get("candidate_invalid_rows_packets_total", 0):
            raise EvaluationStorageError("Malformed source candidate coverage")
        self._counts = counts
        return copy.deepcopy(counts)

    def _records(self, sql, parameters, cap, label):
        records = []
        with self.connection.cursor(name="forward_evaluation_" + uuid4().hex) as cursor:
            cursor.execute(sql, parameters)
            while True:
                if time.monotonic() - self._evidence_started > MAX_EVIDENCE_SECONDS:
                    raise EvidenceLimitExceeded("Evidence read exceeded bounded duration")
                batch = cursor.fetchmany(500)
                if time.monotonic() - self._evidence_started > MAX_EVIDENCE_SECONDS:
                    raise EvidenceLimitExceeded("Evidence read exceeded bounded duration")
                if not batch:
                    break
                records.extend(_object(row[0]) for row in batch)
                if len(records) > cap:
                    raise EvidenceLimitExceeded(label + " evidence cap reached; nothing was truncated")
        return records

    def source_evidence(self, counts=None):
        counts = counts or self.counts()
        if (int(counts["candidate_selected_packets_total"]) > MAX_SOURCE_PACKETS or
                int(counts["candidate_packet_rows"]) > MAX_SOURCE_ROWS or
                int(counts["source_decision_rows_total"]) > MAX_SOURCE_DECISIONS):
            raise EvidenceLimitExceeded("Source evidence cap reached; nothing was truncated")
        packets_sql = """WITH ranked AS MATERIALIZED (
            SELECT seq,kind,
              COALESCE(sum(CASE WHEN kind='INPUT_REJECTED' THEN 1 ELSE 0 END)
                OVER(ORDER BY seq ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS rejected_input_count_before
            FROM research_forward_paper_journal WHERE study_id=%s AND seq<=%s),
            inputs AS MATERIALIZED (
              SELECT seq,row_number() OVER(ORDER BY seq) AS input_ordinal,
                lead(seq) OVER(ORDER BY seq) AS next_input_seq
              FROM ranked WHERE kind IN('INPUT_CANDIDATES','INPUT_QUOTES'))
            SELECT json_build_object('seq',j.seq,'kind',j.kind,'utc',j.utc,'input_key',j.input_key,
                'payload_json',j.payload_json,'payload_sha256',j.payload_sha256,'recorded_at_utc',j.recorded_at_utc,
                'input_ordinal',i.input_ordinal,'rejected_input_count_before',r.rejected_input_count_before,
                'observation_sequence',i.input_ordinal-r.rejected_input_count_before,'next_input_seq',i.next_input_seq)
            FROM ranked r JOIN inputs i ON i.seq=r.seq
              JOIN research_forward_paper_journal j ON j.study_id=%s AND j.seq=r.seq
            WHERE r.kind='INPUT_CANDIDATES' AND
              (COALESCE((j.payload_json->>'bootstrap')::boolean,false) OR jsonb_array_length(j.payload_json->'rows')>0)
            ORDER BY j.seq LIMIT %s"""
        decisions_sql = """SELECT json_build_object('seq',seq,'kind',kind,'utc',utc,'input_key',input_key,
            'payload_json',payload_json,'payload_sha256',payload_sha256,'recorded_at_utc',recorded_at_utc)
            FROM research_forward_paper_journal WHERE study_id=%s AND seq<=%s
              AND kind IN('PARENT_CHOICE_FROZEN','BOOTSTRAP_EXCLUDED','INPUT_REJECTED','POLICY_SOURCE_DRIFT')
            ORDER BY seq LIMIT %s"""
        cutoff = int(self.snapshot["journal_seq"])
        packets = self._records(packets_sql, (STUDY_ID, cutoff, STUDY_ID, MAX_SOURCE_PACKETS + 1), MAX_SOURCE_PACKETS, "Source packet")
        decisions = self._records(decisions_sql, (STUDY_ID, cutoff, MAX_SOURCE_DECISIONS + 1), MAX_SOURCE_DECISIONS, "Source decision")
        if len(packets) != int(counts["candidate_selected_packets_total"]) or len(decisions) != int(counts["source_decision_rows_total"]):
            raise EvaluationStorageError("Source evidence coverage differs from bounded database counts")
        return {"packets": packets, "decisions": decisions, "counts": counts,
                "source_journal_seq": cutoff, "selection_complete": True}

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
        return self._records(sql, parameters, self.max_packets, "Quote packet")


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

    def latest_pass(self, version, policy_sha):
        """Keep prospective retained-evidence pins across v3 implementation fixes."""
        self._assert_leader()
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT assessment_json,assessment_sha256 FROM research_forward_paper_evaluations "
                "WHERE study_id=%s AND evaluator_version=%s AND policy_sha256=%s AND audit_status='PASS' "
                "ORDER BY completed_at_utc DESC LIMIT 1", (STUDY_ID, version, policy_sha),
            )
            row = cursor.fetchone()
        if not row:
            return None
        assessment = _object(row[0])
        if (_digest(assessment) != row[1] or assessment.get("status") != "PASS" or
                assessment.get("evaluator_version") != version or assessment.get("policy_sha256") != policy_sha):
            raise EvaluationStorageError("Previous successful evaluation integrity check failed")
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
        self._code_components = None
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
        self._code_sha, self._code_components = _implementation_identity(self._core)
        self._baseline = json.loads((self.package / "baseline.json").read_text(encoding="utf8"))
        self._baseline_sha = _digest({key: self._baseline.get(key) for key in BASELINE_KEYS})
        self._store = self._store_factory(self._dsn)
        if not self._store.acquire():
            self._discard_owned()
            self._publish(state="STANDBY", leader=False)
            self._log()
            return False
        self._publish(leader=True, evaluator_version=self._version, evaluator_source_sha256=self._code_sha,
                      evaluator_source_components=self._code_components, baseline_sha256=self._baseline_sha)
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
                source_counts = source.counts()
                source_marker = _source_selection_marker(snapshot, source_counts)
                predicate_evidence = source.predicate_evidence()
                predicate_marker = _digest({key: predicate_evidence[key] for key in (
                    "matches", "definitions", "expected_keys", "counts", "extraction_complete")})
                self._publish(source_candidate_packets_total=int(source_counts["candidate_packets_total"]),
                              source_selected_packets_total=int(source_counts["candidate_selected_packets_total"]),
                              source_selection_marker=source_marker,
                              retained_predicate_rows=predicate_evidence["counts"]["expected_positive_keys"],
                              retained_predicate_marker=predicate_marker)
                dataset_sha = _digest({"source_fingerprint": fingerprint, "evaluator_source_sha256": self._code_sha,
                                       "baseline_sha256": self._baseline_sha, "source_selection_marker": source_marker,
                                       "retained_predicate_marker": predicate_marker})
                input_dataset_sha = dataset_sha
                previous = (self._store.latest_pass(self._version, snapshot["policy_sha256"])
                            if self._version == "forward-evaluation-v3" else None)
                pin_baseline_sha = _digest(previous) if previous is not None else None
                cached = self._store.find_pass(self._version, dataset_sha)
                if (cached is not None and self._version == "forward-evaluation-v3" and
                        not _cache_preserves_latest_pins(cached["assessment"], previous, snapshot)):
                    # The old PASS remains immutable; a rollback failure has a
                    # separate, stable identity bound to its newer pin baseline.
                    dataset_sha = _digest({"input_dataset_sha256": dataset_sha,
                                           "successful_pin_baseline_sha256": pin_baseline_sha})
                    cached = None
                self._publish(input_dataset_sha256=input_dataset_sha,
                              prospective_pin_baseline_sha256=pin_baseline_sha,
                              pin_context_identity_applied=dataset_sha != input_dataset_sha)
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
                if self._version != "forward-evaluation-v3":
                    previous = self._store.latest(self._version, self._code_sha)
                source_evidence = source.source_evidence(source_counts)
                packets = source.quotes()
            # Close the repeatable read before CPU work or assessment writes.
            result = self._core.assess(snapshot, packets, expected_baseline=self._baseline,
                                       previous_assessment=previous, source_evidence=source_evidence,
                                       predicate_evidence=predicate_evidence)
            result["worker_evaluation_context"] = {
                "input_dataset_sha256": input_dataset_sha, "evaluation_dataset_sha256": dataset_sha,
                "prospective_pin_baseline_sha256": pin_baseline_sha,
                "prospective_pin_baseline_source_journal_seq": (previous or {}).get("source_journal_seq"),
                "pin_context_identity_applied": dataset_sha != input_dataset_sha}
            _json(result)  # Fail closed on nonfinite or unserializable audit output.
            status = result.get("status")
            if status not in {"PASS", "FAIL", "PENDING_PRICE_DATA", "PENDING_SOURCE_EVIDENCE", "PENDING_PREDICATE_EVIDENCE"}:
                raise ValueError("Unknown assessment status")
            if result.get("dataset_fingerprint") != fingerprint or result.get("evaluator_version") != self._version:
                raise ValueError("Assessment identity does not match its input")
            now = _utc()
            self._publish(audits_attempted=self._health["audits_attempted"] + 1,
                          dataset_sha256=dataset_sha, last_assessment_utc=now,
                          assessment_source_journal_seq=int(snapshot["journal_seq"]), assessment_persisted=False,
                          assessment=self._core.safe_summary(result), last_error_type=None)
            if status in {"PENDING_PRICE_DATA", "PENDING_SOURCE_EVIDENCE", "PENDING_PREDICATE_EVIDENCE"}:
                self._publish(state=status, pending_attempts=self._health["pending_attempts"] + 1)
                self._log()
                return
            metadata = {"version": self._version, "dataset_sha256": dataset_sha,
                        "source_fingerprint": fingerprint, "code_sha256": self._code_sha,
                        "baseline_sha256": self._baseline_sha,
                        "source_journal_seq": int(snapshot["journal_seq"]), "policy_sha256": snapshot["policy_sha256"],
                        "runtime_commit": self._health["runtime_commit"],
                        "evidence_sha256": _digest({"quotes": [[p.get("seq"), p.get("payload_sha256")] for p in packets],
                            "source_packets": [[p.get("seq"), p.get("payload_sha256"), p.get("input_ordinal"),
                                p.get("rejected_input_count_before"), p.get("observation_sequence"), p.get("next_input_seq")]
                                for p in source_evidence["packets"]],
                            "source_decisions": [[p.get("seq"), p.get("payload_sha256")] for p in source_evidence["decisions"]],
                            "source_counts": source_evidence["counts"],
                            "source_journal_seq": source_evidence["source_journal_seq"],
                            "predicate_evidence": predicate_evidence})}
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
