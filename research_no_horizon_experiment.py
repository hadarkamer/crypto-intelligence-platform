"""Frozen, local, retrospective multi-scope experiments; never a discovery gate.

Every declared cell is reported separately, including unknown inputs and failed
preparation. Plans do not pool waves, rank candidates, correct multiple testing,
prove prospective performance, or authorize any runtime action. Input blocking
is terminal for this immutable plan; corrected input requires a new plan.
"""
from __future__ import annotations

import ast
from dataclasses import dataclass
from datetime import timedelta
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Mapping

import research_no_horizon_contract as contracts
import research_no_horizon_gate as gate
import research_no_horizon_source as source
import research_no_horizon_store as child
from research_no_horizon_source import formulas

VERSION = "no-horizon-local-frozen-experiment-v1"
MAX_SCOPES = 64
MAX_EXPANDED_BYTES = 256 * 1024 * 1024
_AUTHORITY = {"runtime_authorized": False, "telegram_authorized": False, "trading_authorized": False}
_TERMINAL = {"COMPLETE", "BLOCKED", "INPUT_BLOCKED"}
REQUIRE_DECIDABLE_SOURCE = "REQUIRE_DECIDABLE_SOURCE_V1"
DIAGNOSTIC_ALLOW_INCOMPLETE_SOURCE = "DIAGNOSTIC_ALLOW_INCOMPLETE_SOURCE_V1"


class SourcePreflightBlocked(ValueError):
    """All declared source decisions, preserved before any job is created."""
    def __init__(self, receipt):
        self.receipt = receipt
        super().__init__("SOURCE_FEATURE_PREFLIGHT_BLOCKED")


class ParentCoverageBlocked(ValueError):
    """Explicit all-scope feasibility requirement failed before any job write."""
    def __init__(self, receipt):
        self.receipt = receipt
        self.source_receipt = receipt["source_preflight"]
        super().__init__("MATCHED_PARENT_COVERAGE_PREFLIGHT_BLOCKED")


@dataclass(frozen=True)
class _PreparedSubmission:
    export: dict
    identity: dict
    plan_id: str

    @property
    def receipt(self):
        return self.identity["source_admission"]["preflight_receipt"]

    @property
    def parent_coverage_receipt(self):
        admission = self.identity.get("parent_coverage_admission")
        return admission["preflight_receipt"] if admission else None


def _implementation() -> dict:
    """Conservatively bind repository-local imports, including conditional ones.

    This includes helpers imported by capture validation even when their network
    functions are unused. The experiment itself never calls those functions.
    Third-party package versions are not a claim of reproducible environment.
    """
    root = Path(__file__).resolve().parent
    pending = [Path(module.__file__).stem for module in (source, child, formulas, gate)]
    pending.append(Path(__file__).stem)
    files = {}
    while pending:
        name = pending.pop()
        path = root / (name + ".py")
        if path.name in files or not path.is_file():
            continue
        data = path.read_bytes()
        files[path.name] = hashlib.sha256(data).hexdigest()
        for node in ast.walk(ast.parse(data)):
            if isinstance(node, ast.Import):
                pending.extend(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                pending.append(node.module.split(".")[0])
    catalog_map = root / "docs" / "ordered_research_question_map.json"
    if catalog_map.is_file():
        files["docs/ordered_research_question_map.json"] = hashlib.sha256(catalog_map.read_bytes()).hexdigest()
    return {"experiment_version": VERSION, "source_version": source.VERSION,
        "source_export_version": source.EXPORT_VERSION, "formula_version": formulas.VERSION,
        "feature_version": formulas.FEATURE_VERSION, "catalog_sha256": formulas.CATALOG_SHA256,
        "default_gate_policy": gate.make_policy(), "child_versions": child._versions(),
        "implementation_files": dict(sorted(files.items()))}


def _admit_export(export: Mapping[str, Any]) -> tuple[dict, int]:
    encoded = formulas.canonical(export)
    size = len(encoded.encode("utf-8"))
    if size > source.MAX_BYTES:
        raise ValueError("SOURCE_EXPORT_BYTE_LIMIT_EXCEEDED")
    frozen = json.loads(encoded)
    if not isinstance(frozen, dict) or frozen.get("export_version") != source.EXPORT_VERSION:
        raise ValueError("SOURCE_EXPORT_IDENTITY_MISMATCH")
    source.route_for_symbol(frozen.get("symbol"))
    start, end, cutoff = (contracts.utc(frozen[key]) for key in
        ("source_start_utc", "source_end_utc", "cutoff_utc"))
    if not start < end <= cutoff or cutoff-start > timedelta(days=source.MAX_DAYS):
        raise ValueError("SOURCE_TIME_OR_DAY_BOUND_INVALID")
    rows, bars = frozen.get("source_rows"), frozen.get("candles")
    if (not isinstance(rows, list) or len(rows) > source.MAX_ROWS
            or not isinstance(bars, list) or len(bars) > source.MAX_DAYS*1440
            or not isinstance(frozen.get("source_receipt"), dict)):
        raise ValueError("SOURCE_STRUCTURE_OR_CARDINALITY_INVALID")
    return frozen, size


def _scopes(scopes) -> list[dict]:
    if not isinstance(scopes, (list, tuple)) or not 1 <= len(scopes) <= MAX_SCOPES:
        raise ValueError("EXPERIMENT_SCOPE_LIMIT_EXCEEDED")
    catalog = {row["candidate_key"]: row for row in formulas.catalog_records()}
    normalized, seen = [], set()
    for item in scopes:
        if not isinstance(item, Mapping) or set(item) != {"candidate_key", "base_direction", "threshold_pct"}:
            raise ValueError("EXACT_SCOPE_FIELDS_REQUIRED")
        name, direction = item["candidate_key"], item["base_direction"]
        record = catalog.get(name) if isinstance(name, str) else None
        if record is None or not record["supported"]:
            raise ValueError("UNSUPPORTED_EXISTING_CANDIDATE")
        if direction not in formulas.DIRECTIONS:
            raise ValueError("INVALID_BASE_DIRECTION")
        threshold = contracts.number(item["threshold_pct"], "threshold_pct")
        if not 0 < threshold < 100:
            raise ValueError("INVALID_SYMMETRIC_THRESHOLD")
        scope = {"candidate_key": name, "base_direction": direction, "threshold_pct": threshold}
        scope_id = contracts.digest(scope)
        if scope_id in seen:
            raise ValueError("DUPLICATE_NORMALIZED_SCOPE")
        seen.add(scope_id)
        normalized.append({**scope, "scope_id": scope_id, "candidate": record,
            "analysis_direction": ("SHORT" if direction == "LONG" else "LONG")
                if record["orientation"] == "INVERSE" else direction})
    return sorted(normalized, key=lambda item: (item["candidate_key"], item["base_direction"], item["threshold_pct"]))


def prepare_submission(export, scopes, *, allow_incomplete_source=False, require_parent_coverage=False):
    """One whole-plan feature preflight, before SQLite or outcome processing.

    Diagnostic admission is explicit and frozen; it never changes source
    decisions or grants research qualification. Optional all-scope parent
    coverage is a necessary feasibility check, not a qualification gate.
    The child runner still validates parents, entries and outcome paths.
    """
    from research_no_horizon_preflight import preflight_source_features
    from research_no_horizon_parent_coverage import _coverage_from_preflight, REQUIRE_ALL_SCOPES

    if type(allow_incomplete_source) is not bool:
        raise ValueError("EXPLICIT_BOOLEAN_DIAGNOSTIC_POLICY_REQUIRED")
    if type(require_parent_coverage) is not bool:
        raise ValueError("EXPLICIT_BOOLEAN_PARENT_COVERAGE_POLICY_REQUIRED")
    frozen, size = _admit_export(export)
    normalized = _scopes(scopes)
    if size * len(normalized) > MAX_EXPANDED_BYTES:
        raise ValueError("EXPERIMENT_EXPANDED_BYTE_LIMIT_EXCEEDED")
    receipt = preflight_source_features(frozen, scopes)
    coverage = None
    if require_parent_coverage:
        coverage = _coverage_from_preflight(frozen, receipt, gate_policy=gate.make_policy())
        if not coverage["all_scopes_potentially_sufficient"]:
            raise ParentCoverageBlocked(coverage)
    if not receipt["ready_for_outcome_research"] and not allow_incomplete_source:
        raise SourcePreflightBlocked(receipt)
    policy = DIAGNOSTIC_ALLOW_INCOMPLETE_SOURCE if allow_incomplete_source else REQUIRE_DECIDABLE_SOURCE
    identity = {"source_export_sha256": contracts.digest(frozen), "source_canonical_bytes": size,
        "symbol": frozen["symbol"], "scopes": normalized, "versions": _implementation(),
        "source_admission": {"policy": policy, "preflight_receipt": receipt}}
    if coverage is not None:
        identity["parent_coverage_admission"] = {"policy": REQUIRE_ALL_SCOPES, "preflight_receipt": coverage}
    return _PreparedSubmission(frozen, identity, contracts.digest(identity))


class LocalExperimentStore:
    """Composition over the fenced child store; only explicit linked jobs run.

    scope_budget bounds attempted cells and heavy source preparation per call.
    candle_budget is shared across all those cells. entry_budget and batch_size
    retain the child store's per-claim meaning. A persisted round-robin cursor
    keeps long open paths from monopolizing successive bounded calls.
    """
    def __init__(self, path):
        # Reject future coordinator schemas before the child initializer writes.
        if str(path) != ":memory:" and Path(path).is_file():
            probe = sqlite3.connect(Path(path).resolve().as_uri()+"?mode=ro", uri=True)
            try:
                if probe.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='experiment_version'").fetchone():
                    version = probe.execute("SELECT version FROM experiment_version WHERE singleton=1").fetchone()
                    if version is None or version[0] != VERSION:
                        raise ValueError("unsupported experiment store version")
            finally:
                probe.close()
        self.children = child.LocalResearchStore(path)
        self.connection = self.children.connection
        existing = self.connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='experiment_version'").fetchone()
        if existing:
            version = self.connection.execute("SELECT version FROM experiment_version WHERE singleton=1").fetchone()
            if version is None or version[0] != VERSION:
                self.close()
                raise ValueError("unsupported experiment store version")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS experiment_version(singleton INTEGER PRIMARY KEY CHECK(singleton=1),version TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS experiment_sources(source_sha256 TEXT PRIMARY KEY,payload_json TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS experiment_plans(plan_id TEXT PRIMARY KEY,plan_key TEXT UNIQUE,
                source_sha256 TEXT NOT NULL REFERENCES experiment_sources,identity_json TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS experiment_scopes(plan_id TEXT NOT NULL REFERENCES experiment_plans,
                ordinal INTEGER NOT NULL,scope_json TEXT NOT NULL,PRIMARY KEY(plan_id,ordinal));
            CREATE TABLE IF NOT EXISTS experiment_scope_state(plan_id TEXT NOT NULL,ordinal INTEGER NOT NULL,
                job_id TEXT REFERENCES local_jobs,error TEXT,
                PRIMARY KEY(plan_id,ordinal),FOREIGN KEY(plan_id,ordinal) REFERENCES experiment_scopes,
                CHECK(job_id IS NULL OR error IS NULL));
            CREATE TABLE IF NOT EXISTS experiment_schedule(plan_id TEXT PRIMARY KEY REFERENCES experiment_plans,next_ordinal INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS experiment_work_commits(job_id TEXT NOT NULL,fencing_token INTEGER NOT NULL,
                starting_evaluations INTEGER NOT NULL,ending_evaluations INTEGER NOT NULL,PRIMARY KEY(job_id,fencing_token));
            CREATE TRIGGER IF NOT EXISTS experiment_count_work AFTER UPDATE OF candle_evaluations ON local_jobs
                WHEN EXISTS(SELECT 1 FROM experiment_scope_state WHERE job_id=NEW.job_id)
                BEGIN INSERT INTO experiment_work_commits VALUES(NEW.job_id,NEW.fencing_token,
                    OLD.candle_evaluations,NEW.candle_evaluations); END;
            CREATE TRIGGER IF NOT EXISTS experiment_immutable_link BEFORE UPDATE ON experiment_scope_state
                WHEN NEW.plan_id IS NOT OLD.plan_id OR NEW.ordinal IS NOT OLD.ordinal
                    OR (OLD.job_id IS NOT NULL AND NEW.job_id IS NOT OLD.job_id)
                    OR (OLD.error IS NOT NULL AND NEW.error IS NOT OLD.error)
                BEGIN SELECT RAISE(ABORT,'immutable experiment scope result'); END;
        """)
        self.connection.execute("INSERT OR IGNORE INTO experiment_version VALUES(1,?)", (VERSION,))
        for table in ("experiment_version", "experiment_sources", "experiment_plans", "experiment_scopes", "experiment_work_commits"):
            for operation in ("UPDATE", "DELETE"):
                self.connection.execute(f"""CREATE TRIGGER IF NOT EXISTS immutable_{table}_{operation}
                    BEFORE {operation} ON {table} BEGIN SELECT RAISE(ABORT,'immutable experiment record'); END""")

    def close(self):
        self.children.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def submit_plan(self, export, scopes, *, plan_key=None, allow_incomplete_source=False, require_parent_coverage=False) -> str:
        prepared = prepare_submission(export, scopes, allow_incomplete_source=allow_incomplete_source,
            require_parent_coverage=require_parent_coverage)
        return self._submit_prepared(prepared, plan_key=plan_key)

    def _submit_prepared(self, prepared, *, plan_key=None) -> str:
        """Persist the internally prepared inputs without repeating row checks.

        The CLI prepares before opening SQLite; the public submit_plan method
        performs the same check. This is not an API for imported audit reports.
        """
        if plan_key is not None:
            child._name(plan_key, "plan_key")
        if not isinstance(prepared, _PreparedSubmission):
            raise ValueError("INTERNAL_PREPARED_SUBMISSION_REQUIRED")
        frozen, identity, plan_id = prepared.export, prepared.identity, prepared.plan_id
        source_sha = contracts.digest(frozen)
        if (contracts.digest(identity) != plan_id or identity["source_export_sha256"] != source_sha
                or identity["versions"] != _implementation()):
            raise ValueError("prepared submission integrity/implementation mismatch")
        normalized = identity["scopes"]
        with self.children._write():
            if plan_key is not None:
                named = self.connection.execute("SELECT plan_id FROM experiment_plans WHERE plan_key=?", (plan_key,)).fetchone()
                if named is not None and named[0] != plan_id:
                    raise ValueError("plan_key already names a different immutable plan")
            existing = self.connection.execute("SELECT plan_key FROM experiment_plans WHERE plan_id=?", (plan_id,)).fetchone()
            if existing is not None:
                if plan_key is not None and existing[0] != plan_key:
                    raise ValueError("immutable plan already exists with another plan_key")
                return plan_id
            self.connection.execute("INSERT OR IGNORE INTO experiment_sources VALUES(?,?)", (source_sha, contracts.canonical(frozen)))
            self.connection.execute("INSERT INTO experiment_plans VALUES(?,?,?,?)", (plan_id, plan_key, source_sha, contracts.canonical(identity)))
            for ordinal, scope in enumerate(normalized):
                self.connection.execute("INSERT INTO experiment_scopes VALUES(?,?,?)", (plan_id, ordinal, contracts.canonical(scope)))
                self.connection.execute("INSERT INTO experiment_scope_state VALUES(?,?,NULL,NULL)", (plan_id, ordinal))
            self.connection.execute("INSERT INTO experiment_schedule VALUES(?,0)", (plan_id,))
        return plan_id

    def _load(self, plan_id):
        row = self.connection.execute("SELECT p.*,s.payload_json FROM experiment_plans p JOIN experiment_sources s USING(source_sha256) WHERE plan_id=?", (plan_id,)).fetchone()
        if row is None:
            raise KeyError("unknown experiment plan")
        identity, export = json.loads(row["identity_json"]), json.loads(row["payload_json"])
        if (contracts.digest(identity) != plan_id or contracts.digest(export) != row["source_sha256"]
                or identity["source_export_sha256"] != row["source_sha256"]):
            raise ValueError("experiment plan/source integrity mismatch")
        scopes = self.connection.execute("""SELECT s.ordinal,s.scope_json,t.job_id,t.error FROM experiment_scopes s
            JOIN experiment_scope_state t USING(plan_id,ordinal) WHERE plan_id=? ORDER BY ordinal""", (plan_id,)).fetchall()
        if ([row["ordinal"] for row in scopes] != list(range(len(identity["scopes"])))
                or [json.loads(row["scope_json"]) for row in scopes] != identity["scopes"]):
            raise ValueError("experiment scope manifest integrity mismatch")
        return identity, export, scopes

    def _reserve(self, plan_id, limit):
        with self.children._write():
            cursor = self.connection.execute("SELECT next_ordinal FROM experiment_schedule WHERE plan_id=?", (plan_id,)).fetchone()[0]
            rows = self.connection.execute("""SELECT s.* FROM experiment_scope_state s LEFT JOIN local_jobs j USING(job_id)
                WHERE s.plan_id=? AND s.error IS NULL AND (s.job_id IS NULL OR j.status IN ('PENDING','RUNNING'))
                ORDER BY CASE WHEN s.ordinal>=? THEN 0 ELSE 1 END,s.ordinal LIMIT ?""", (plan_id, cursor, limit)).fetchall()
            if rows:
                self.connection.execute("UPDATE experiment_schedule SET next_ordinal=? WHERE plan_id=?", (rows[-1]["ordinal"]+1, plan_id))
            return [dict(row) for row in rows]

    def _prepare(self, plan_id, ordinal, export, scope):
        try:
            snapshot = source.build_snapshot(export, scope["candidate_key"], scope["base_direction"], export["symbol"], scope["threshold_pct"])
            # No named key: a crash before linking is repaired by exact-input reuse.
            job_id = self.children.submit_snapshot(snapshot)
        except (ValueError, TypeError, KeyError, OverflowError, AttributeError, IndexError) as exc:
            with self.children._write():
                self.connection.execute("""UPDATE experiment_scope_state SET error=?
                    WHERE plan_id=? AND ordinal=? AND job_id IS NULL AND error IS NULL""",
                    ("INPUT_BLOCKED:"+type(exc).__name__+":"+str(exc)[:300], plan_id, ordinal))
            return None
        self._link(plan_id, ordinal, job_id)
        return job_id

    def _link(self, plan_id, ordinal, job_id):
        with self.children._write():
            row = self.connection.execute("SELECT job_id,error FROM experiment_scope_state WHERE plan_id=? AND ordinal=?", (plan_id, ordinal)).fetchone()
            if row["error"] is not None or row["job_id"] not in (None, job_id):
                raise ValueError("conflicting immutable scope preparation")
            self.connection.execute("UPDATE experiment_scope_state SET job_id=? WHERE plan_id=? AND ordinal=?", (job_id, plan_id, ordinal))

    def run_plan(self, plan_id, worker_id, *, scope_budget=1, candle_budget=1024, entry_budget=128, batch_size=128):
        child._name(worker_id, "worker_id")
        child._bound(scope_budget, "scope_budget", MAX_SCOPES)
        child._bound(candle_budget, "candle_budget", 100000000)
        child._bound(entry_budget, "entry_budget", child.replay.MAX_OPPORTUNITIES)
        child._bound(batch_size, "batch_size", 100000)
        identity, export, _ = self._load(plan_id)
        if identity["versions"] != _implementation() or identity["versions"]["child_versions"] != self.children.versions:
            raise ValueError("experiment implementation/policy mismatch; submit a new plan")
        attempts, consumed = [], 0
        for row in self._reserve(plan_id, scope_budget):
            ordinal = row["ordinal"]
            attempts.append(ordinal)
            job_id = row["job_id"] or self._prepare(plan_id, ordinal, export, identity["scopes"][ordinal])
            if job_id is None or consumed == candle_budget:
                continue
            claim = self.children.claim_job(worker_id, job_id=job_id)
            if claim is None:
                continue
            starting = self.children._check_claim(claim)["candle_evaluations"]
            self.children.process_claim(claim, candle_budget=candle_budget-consumed,
                entry_budget=entry_budget, batch_size=batch_size)
            committed = self.connection.execute("""SELECT starting_evaluations,ending_evaluations FROM experiment_work_commits
                WHERE job_id=? AND fencing_token=?""", (job_id, claim["fencing_token"])).fetchone()
            if committed is None or committed[0] != starting or not 0 <= committed[1]-starting <= candle_budget-consumed:
                raise ValueError("experiment fenced work accounting mismatch")
            consumed += committed[1]-starting
        return {**self.report(plan_id), "attempted_scopes": len(attempts), "attempted_ordinals": attempts, "candle_evaluations_this_run": consumed}

    def report(self, plan_id):
        """One SQLite read snapshot; historical receipts need no current-code match."""
        self.connection.execute("BEGIN")
        try:
            identity, _, states = self._load(plan_id)
            rows = []
            for state in states:
                scope = identity["scopes"][state["ordinal"]]
                row = {key: scope[key] for key in ("scope_id", "candidate_key", "base_direction", "analysis_direction", "threshold_pct")}
                row.update(ordinal=state["ordinal"], job_id=state["job_id"], error=state["error"],
                    status="INPUT_BLOCKED" if state["error"] else "NOT_SUBMITTED", source_counts=None,
                    source_coverage_complete=False if state["error"] else None,
                    computation_complete=False if state["error"] else None, status_counts=None, gate=None, candle_evaluations=None, source_blockers=None,
                    emitted_opportunities=None, snapshot_sha256=None, receipt_sha256=None)
                if state["job_id"]:
                    job = self.children.get_job(state["job_id"])
                    metadata = json.loads(self.connection.execute("SELECT metadata_json FROM local_snapshots WHERE snapshot_sha256=?", (job["snapshot_sha256"],)).fetchone()[0])
                    source_receipt = metadata["source_receipt"]
                    selection = source_receipt["selection"]
                    if (contracts.digest(job["identity"]) != job["job_id"]
                            or source_receipt["source_export_sha256"] != identity["source_export_sha256"]
                            or job["identity"]["effective_gate_policy"] != identity["versions"]["default_gate_policy"]
                            or any(job["identity"].get(k) != v for k, v in identity["versions"]["child_versions"].items())
                            or any(selection.get(k) != scope[k] for k in ("candidate_key", "base_direction", "threshold_pct"))
                            or selection["candidate_definition_sha256"] != scope["candidate"]["definition_sha256"]):
                        raise ValueError("experiment linked child identity mismatch")
                    row.update(status=job["status"], source_counts=source_receipt["counts"],
                        source_coverage_complete=metadata["source_coverage_complete"], candle_evaluations=job["candle_evaluations"],
                        source_blockers=source_receipt["source_blockers"], emitted_opportunities=source_receipt["emitted_opportunities"],
                        snapshot_sha256=job["snapshot_sha256"])
                    receipt = self.children.get_receipt(state["job_id"])
                    if receipt is not None:
                        if (receipt["job_id"] != job["job_id"] or receipt["job_identity"] != job["identity"]
                                or receipt["snapshot_sha256"] != job["snapshot_sha256"]):
                            raise ValueError("experiment receipt/child binding mismatch")
                        row["receipt_sha256"] = receipt["receipt_sha256"]
                        row.update(computation_complete=receipt["computation_complete"],
                            status_counts=receipt["status_counts"], gate=receipt["gate"])
                    elif job["status"] in _TERMINAL:
                        raise ValueError("terminal child is missing its immutable receipt")
                rows.append({**row, **_AUTHORITY})
            return {"experiment_version": identity["versions"]["experiment_version"], "plan_id": plan_id,
                "plan_identity": identity, "declared_scopes": len(rows), "scopes": rows,
                "all_scopes_processed": all(row["status"] in _TERMINAL for row in rows),
                "source_coverage_complete": all(row["source_coverage_complete"] is True for row in rows),
                "computation_complete": all(row["computation_complete"] is True for row in rows),
                "research_classification": "RETROSPECTIVE_EXPLORATORY_DESCRIPTIVE",
                "multiple_testing_adjusted": False, "validated_discovery": False,
                "scope_pooling": False, "scope_ranking": False, **_AUTHORITY}
        finally:
            self.connection.execute("ROLLBACK")
