"""Durable local execution of global cohort scopes, never per-part experiments.

Complete source admission and outcome-blind representative selection precede
any job. The unchanged child store provides fenced, bounded first-touch work.
Only exact frozen inputs and implementations resume; historical reports remain
readable under a later implementation. No network or production integration.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sqlite3

import research_no_horizon_contract as contracts
import research_no_horizon_cohort_outcomes as preparation
import research_no_horizon_store as child

VERSION = "no-horizon-local-global-cohort-store-v1"
_AUTHORITY = {"runtime_authorized": False, "telegram_authorized": False,
              "trading_authorized": False}
_TERMINAL = {"COMPLETE", "BLOCKED", "INPUT_BLOCKED"}


class LocalCohortStore:
    """One connection per process/thread; use a local SQLite file."""

    def __init__(self, path):
        # Reject foreign/future coordinator schemas before child bootstrap,
        # journal-mode changes, or any other writes to the existing database.
        if str(path) != ":memory:" and Path(path).is_file():
            probe = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
            try:
                if probe.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='cohort_store_version'").fetchone():
                    row = probe.execute("SELECT version FROM cohort_store_version WHERE singleton=1").fetchone()
                    if row is None or row[0] != VERSION:
                        raise ValueError("unsupported global cohort store version")
            finally:
                probe.close()
        self.children = child.LocalResearchStore(path)
        self.connection = self.children.connection
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS cohort_store_version(
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),version TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS cohort_payloads(
                payload_sha256 TEXT PRIMARY KEY,payload_json TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS cohort_plans(
                plan_id TEXT PRIMARY KEY,cohort_key TEXT UNIQUE,
                payload_sha256 TEXT NOT NULL REFERENCES cohort_payloads,identity_json TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS cohort_scopes(
                plan_id TEXT NOT NULL REFERENCES cohort_plans,ordinal INTEGER NOT NULL,
                scope_plan_json TEXT NOT NULL,PRIMARY KEY(plan_id,ordinal));
            CREATE TABLE IF NOT EXISTS cohort_scope_state(
                plan_id TEXT NOT NULL,ordinal INTEGER NOT NULL,
                job_id TEXT REFERENCES local_jobs,error TEXT,
                PRIMARY KEY(plan_id,ordinal),
                FOREIGN KEY(plan_id,ordinal) REFERENCES cohort_scopes,
                CHECK(job_id IS NULL OR error IS NULL));
            CREATE TABLE IF NOT EXISTS cohort_schedule(
                plan_id TEXT PRIMARY KEY REFERENCES cohort_plans,next_ordinal INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS cohort_work_commits(
                job_id TEXT NOT NULL,fencing_token INTEGER NOT NULL,
                starting_evaluations INTEGER NOT NULL,ending_evaluations INTEGER NOT NULL,
                PRIMARY KEY(job_id,fencing_token));
            CREATE TRIGGER IF NOT EXISTS cohort_count_work
                AFTER UPDATE OF candle_evaluations ON local_jobs
                WHEN EXISTS(SELECT 1 FROM cohort_scope_state WHERE job_id=NEW.job_id)
                BEGIN INSERT INTO cohort_work_commits VALUES(NEW.job_id,NEW.fencing_token,
                    OLD.candle_evaluations,NEW.candle_evaluations); END;
            CREATE TRIGGER IF NOT EXISTS cohort_immutable_link
                BEFORE UPDATE ON cohort_scope_state
                WHEN NEW.plan_id IS NOT OLD.plan_id OR NEW.ordinal IS NOT OLD.ordinal
                    OR (OLD.job_id IS NOT NULL AND NEW.job_id IS NOT OLD.job_id)
                    OR (OLD.error IS NOT NULL AND NEW.error IS NOT OLD.error)
                BEGIN SELECT RAISE(ABORT,'immutable global cohort scope result'); END;
            CREATE TRIGGER IF NOT EXISTS cohort_scope_state_delete_immutable
                BEFORE DELETE ON cohort_scope_state
                BEGIN SELECT RAISE(ABORT,'immutable global cohort scope result'); END;
        """)
        self.connection.execute("INSERT OR IGNORE INTO cohort_store_version VALUES(1,?)", (VERSION,))
        for table in ("cohort_store_version", "cohort_payloads", "cohort_plans", "cohort_scopes", "cohort_work_commits"):
            for action in ("UPDATE", "DELETE"):
                self.connection.execute(f"""CREATE TRIGGER IF NOT EXISTS {table}_{action.lower()}_immutable
                    BEFORE {action} ON {table}
                    BEGIN SELECT RAISE(ABORT,'immutable global cohort input'); END""")

    def close(self):
        self.children.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    @contextmanager
    def _read(self):
        owns = not self.connection.in_transaction
        if owns:
            self.connection.execute("BEGIN")
        try:
            yield
        finally:
            if owns:
                self.connection.execute("ROLLBACK")

    def submit_cohort(self, declaration, anchor, load_part, *, cohort_key=None):
        prepared = preparation.prepare_cohort_submission(declaration, anchor, load_part)
        return self._submit_prepared(prepared, cohort_key=cohort_key)

    def _submit_prepared(self, prepared, *, cohort_key=None):
        preparation.validate_prepared(prepared)
        if cohort_key is not None:
            child._name(cohort_key, "cohort_key")
        identity, payload, plan_id = prepared.identity, prepared.payload, prepared.plan_id
        encoded = contracts.canonical(payload)
        with self.children._write():
            if cohort_key is not None:
                named = self.connection.execute("SELECT plan_id FROM cohort_plans WHERE cohort_key=?", (cohort_key,)).fetchone()
                if named is not None and named[0] != plan_id:
                    raise ValueError("cohort_key already names a different immutable plan")
            existing = self.connection.execute("SELECT cohort_key,identity_json FROM cohort_plans WHERE plan_id=?", (plan_id,)).fetchone()
            if existing is not None:
                if existing[1] != contracts.canonical(identity):
                    raise ValueError("global cohort plan identity collision")
                if cohort_key is not None and existing[0] != cohort_key:
                    raise ValueError("immutable cohort already exists with another cohort_key")
                self._load(plan_id)
                return plan_id
            prior = self.connection.execute("SELECT payload_json FROM cohort_payloads WHERE payload_sha256=?", (identity["payload_sha256"],)).fetchone()
            if prior is not None and prior[0] != encoded:
                raise ValueError("global cohort payload identity collision")
            self.connection.execute("INSERT OR IGNORE INTO cohort_payloads VALUES(?,?)", (identity["payload_sha256"], encoded))
            self.connection.execute("INSERT INTO cohort_plans VALUES(?,?,?,?)",
                (plan_id, cohort_key, identity["payload_sha256"], contracts.canonical(identity)))
            for item in payload["scope_plans"]:
                self.connection.execute("INSERT INTO cohort_scopes VALUES(?,?,?)",
                    (plan_id, item["ordinal"], contracts.canonical(item)))
                self.connection.execute("INSERT INTO cohort_scope_state VALUES(?,?,NULL,?)",
                    (plan_id, item["ordinal"], item["input_error"]))
            self.connection.execute("INSERT INTO cohort_schedule VALUES(?,0)", (plan_id,))
        return plan_id

    def _load(self, plan_id):
        row = self.connection.execute("""SELECT p.*,d.payload_json FROM cohort_plans p
            JOIN cohort_payloads d USING(payload_sha256) WHERE plan_id=?""", (plan_id,)).fetchone()
        if row is None:
            if self.connection.execute("SELECT 1 FROM cohort_plans WHERE plan_id=?", (plan_id,)).fetchone():
                raise ValueError("missing global cohort payload")
            raise KeyError("unknown global cohort plan")
        identity, payload = json.loads(row["identity_json"]), json.loads(row["payload_json"])
        preparation.validate_prepared(preparation.PreparedCohort(identity, payload, plan_id),
            check_implementation=False)
        if (contracts.digest(identity) != plan_id or contracts.digest(payload) != row["payload_sha256"]
                or identity["payload_sha256"] != row["payload_sha256"]
                or contracts.digest(payload["candles"]) != identity["candles_sha256"]
                or payload["coverage_receipt"]["receipt_sha256"] != identity["coverage_receipt_sha256"]
                or contracts.digest({k: v for k, v in payload["coverage_receipt"].items()
                    if k != "receipt_sha256"}) != identity["coverage_receipt_sha256"]):
            raise ValueError("global cohort identity/payload integrity mismatch")
        rows = self.connection.execute("""SELECT s.ordinal,s.scope_plan_json,t.job_id,t.error
            FROM cohort_scopes s JOIN cohort_scope_state t USING(plan_id,ordinal)
            WHERE s.plan_id=? ORDER BY s.ordinal""", (plan_id,)).fetchall()
        plans = payload["scope_plans"]
        if ([r["ordinal"] for r in rows] != list(range(len(plans)))
                or [json.loads(r["scope_plan_json"]) for r in rows] != plans
                or any(r["error"] != p["input_error"] for r, p in zip(rows, plans))):
            raise ValueError("global cohort scope manifest integrity mismatch")
        schedule = self.connection.execute("SELECT next_ordinal FROM cohort_schedule WHERE plan_id=?", (plan_id,)).fetchone()
        if schedule is None or type(schedule[0]) is not int or not 0 <= schedule[0] <= len(plans):
            raise ValueError("global cohort schedule integrity mismatch")
        return identity, payload, rows

    @staticmethod
    def _snapshot(payload, item):
        if item["input_error"] is not None or item["snapshot_metadata"] is None:
            raise ValueError("input-blocked global scope cannot create a child")
        snapshot = {**item["snapshot_metadata"], "candles": payload["candles"]}
        if contracts.digest(snapshot) != item["snapshot_sha256"]:
            raise ValueError("global cohort frozen snapshot integrity mismatch")
        return snapshot

    def _verify_child(self, identity, payload, item, job_id):
        with self._read():
            return self._verify_child_snapshot(identity, payload, item, job_id)

    def _verify_child_snapshot(self, identity, payload, item, job_id):
        snapshot = self._snapshot(payload, item)
        job = self.children.get_job(job_id)
        if (contracts.digest(job["identity"]) != job_id
                or job["snapshot_sha256"] != item["snapshot_sha256"]
                or job["identity"]["snapshot_sha256"] != item["snapshot_sha256"]
                or job["identity"]["effective_gate_policy"] != identity["gate_policy"]
                or any(job["identity"].get(k) != v for k, v in identity["versions"]["child_versions"].items())):
            raise ValueError("global cohort linked child identity mismatch")
        stored = self.connection.execute("SELECT * FROM local_snapshots WHERE snapshot_sha256=?", (job["snapshot_sha256"],)).fetchone()
        expected_metadata = {k: v for k, v in snapshot.items() if k not in ("candles", "opportunities")}
        if (stored is None or hashlib.sha256(stored["payload_json"].encode("utf-8")).hexdigest() != item["snapshot_sha256"]
                or json.loads(stored["metadata_json"]) != expected_metadata
                or stored["source_candles"] != len(payload["candles"])):
            raise ValueError("global cohort child snapshot metadata integrity mismatch")
        entries = self.connection.execute("SELECT ordinal,entry_id,metadata_json FROM local_entries WHERE job_id=? ORDER BY ordinal", (job_id,)).fetchall()
        expected = item["normalized_entries"]
        if ([r["ordinal"] for r in entries] != list(range(len(expected)))
                or [r["entry_id"] for r in entries] != [r["contract"]["entry_id"] for r in expected]
                or [json.loads(r["metadata_json"]) for r in entries] != expected
                or job["total_entries"] != len(expected)):
            raise ValueError("global cohort child representative manifest mismatch")
        selected = sorted(row["entry_id"] for row in item["representatives"])
        if sorted(r["entry_id"] for r in entries) != selected:
            raise ValueError("global cohort selected representative binding mismatch")
        prices = self.connection.execute("SELECT open_time_utc,candle_json FROM local_candles WHERE snapshot_sha256=? ORDER BY open_time_utc", (job["snapshot_sha256"],)).fetchall()
        if (len(prices) != len(payload["candles"])
                or any(r["open_time_utc"] != contracts.utc(bar["open_time_utc"]).isoformat(timespec="microseconds")
                    or json.loads(r["candle_json"]) != bar for r, bar in zip(prices, payload["candles"]))):
            raise ValueError("global cohort child materialized price integrity mismatch")
        progress = self.connection.execute("SELECT ordinal,checkpoint_json,processed FROM local_progress WHERE job_id=? ORDER BY ordinal", (job_id,)).fetchall()
        if ([r["ordinal"] for r in progress] != list(range(len(expected)))
                or type(job["next_ordinal"]) is not int or not 0 <= job["next_ordinal"] <= len(expected)):
            raise ValueError("global cohort child checkpoint manifest mismatch")
        evaluations = 0
        for index, (state_row, entry) in enumerate(zip(progress, expected)):
            state = json.loads(state_row["checkpoint_json"])
            if (state["checkpoint_sha256"] != contracts.digest({k: v for k, v in state.items() if k != "checkpoint_sha256"})
                    or state["contract"] != entry["contract"] or state["cutoff_utc"] != payload["cutoff_utc"]
                    or type(state["processed_candles"]) is not int or state["processed_candles"] < 0
                    or state_row["processed"] != int(index < job["next_ordinal"])):
                raise ValueError("global cohort child checkpoint integrity mismatch")
            evaluations += state["processed_candles"]
        if evaluations != job["candle_evaluations"]:
            raise ValueError("global cohort child work counter integrity mismatch")
        return job

    def _reserve(self, plan_id, limit):
        with self.children._write():
            cursor = self.connection.execute("SELECT next_ordinal FROM cohort_schedule WHERE plan_id=?", (plan_id,)).fetchone()[0]
            rows = self.connection.execute("""SELECT s.* FROM cohort_scope_state s
                LEFT JOIN local_jobs j USING(job_id) WHERE s.plan_id=? AND s.error IS NULL
                AND (s.job_id IS NULL OR j.status IN ('PENDING','RUNNING'))
                ORDER BY CASE WHEN s.ordinal>=? THEN 0 ELSE 1 END,s.ordinal LIMIT ?""", (plan_id, cursor, limit)).fetchall()
            if rows:
                self.connection.execute("UPDATE cohort_schedule SET next_ordinal=? WHERE plan_id=?", (rows[-1]["ordinal"]+1, plan_id))
            return [dict(r) for r in rows]

    def _link(self, plan_id, ordinal, job_id):
        with self.children._write():
            row = self.connection.execute("SELECT job_id,error FROM cohort_scope_state WHERE plan_id=? AND ordinal=?", (plan_id, ordinal)).fetchone()
            if row is None or row["error"] is not None or row["job_id"] not in (None, job_id):
                raise ValueError("conflicting immutable global scope preparation")
            if row["job_id"] is None:
                job = self.connection.execute("SELECT fencing_token,candle_evaluations,status FROM local_jobs WHERE job_id=?", (job_id,)).fetchone()
                if job is None or tuple(job) != (0, 0, "PENDING"):
                    raise ValueError("unlinked global child already claimed or processed")
            self.connection.execute("UPDATE cohort_scope_state SET job_id=? WHERE plan_id=? AND ordinal=?", (job_id, plan_id, ordinal))

    def run_cohort(self, plan_id, worker_id, *, scope_budget=1, candle_budget=1024,
                   entry_budget=128, batch_size=128):
        child._name(worker_id, "worker_id")
        child._bound(scope_budget, "scope_budget", 64)
        child._bound(candle_budget, "candle_budget", 100000000)
        child._bound(entry_budget, "entry_budget", child.replay.MAX_OPPORTUNITIES)
        child._bound(batch_size, "batch_size", 100000)
        with self._read():
            identity, payload, states = self._load(plan_id)
            if (identity["versions"] != preparation.implementation()
                    or identity["versions"]["child_versions"] != self.children.versions):
                raise ValueError("global cohort implementation/policy mismatch; submit a new plan")
            for state in states:
                if state["job_id"]:
                    self._verify_child(identity, payload, payload["scope_plans"][state["ordinal"]], state["job_id"])
        attempts, consumed = [], 0
        for row in self._reserve(plan_id, scope_budget):
            if consumed == candle_budget:
                break
            ordinal = row["ordinal"]
            item = payload["scope_plans"][ordinal]
            attempts.append(ordinal)
            job_id = row["job_id"]
            if job_id is None:
                # Crash before linking is repaired by exact child-input reuse.
                job_id = self.children.submit_snapshot(self._snapshot(payload, item))
                self._verify_child(identity, payload, item, job_id)
                self._link(plan_id, ordinal, job_id)
            claim = self.children.claim_job(worker_id, job_id=job_id)
            if claim is None:
                continue
            starting = self.children._check_claim(claim)["candle_evaluations"]
            self.children.process_claim(claim, candle_budget=candle_budget-consumed,
                entry_budget=entry_budget, batch_size=batch_size)
            committed = self.connection.execute("""SELECT starting_evaluations,ending_evaluations
                FROM cohort_work_commits WHERE job_id=? AND fencing_token=?""", (job_id, claim["fencing_token"])).fetchone()
            if (committed is None or committed[0] != starting
                    or not 0 <= committed[1]-starting <= candle_budget-consumed):
                raise ValueError("global cohort fenced work accounting mismatch")
            consumed += committed[1]-starting
        result = {**self.report(plan_id), "attempted_scopes": len(attempts),
            "attempted_ordinals": attempts, "candle_evaluations_this_run": consumed}
        result["report_sha256"] = contracts.digest({k: v for k, v in result.items() if k != "report_sha256"})
        return result

    def report(self, plan_id):
        """One coherent read snapshot; do not reinterpret historical evidence."""
        self.connection.execute("BEGIN")
        try:
            identity, payload, states = self._load(plan_id)
            results = []
            for state in states:
                item = payload["scope_plans"][state["ordinal"]]
                scope = item["scope"]
                result = {k: scope[k] for k in ("scope_id", "candidate_key", "base_direction", "analysis_direction", "threshold_pct")}
                result.update(ordinal=state["ordinal"], job_id=state["job_id"], error=state["error"],
                    status="INPUT_BLOCKED" if state["error"] is not None else "NOT_SUBMITTED",
                    representatives=item["representatives"], missing_entry_ids=item["missing_entry_ids"],
                    missing_entry_locators=item["missing_entry_locators"],
                    snapshot_sha256=item["snapshot_sha256"], receipt_sha256=None, gate=None,
                    computation_complete=False if state["error"] is not None else None,
                    candle_evaluations=0, outcome_trial_executed=False)
                if state["job_id"]:
                    job = self._verify_child(identity, payload, item, state["job_id"])
                    result.update(status=job["status"], candle_evaluations=job["candle_evaluations"])
                    result["outcome_trial_executed"] = self.connection.execute(
                        "SELECT 1 FROM cohort_work_commits WHERE job_id=? LIMIT 1", (job["job_id"],)).fetchone() is not None
                    receipt = self.children.get_receipt(job["job_id"])
                    if receipt is not None:
                        selected = sorted(row["entry_id"] for row in item["representatives"])
                        checkpoint_outcomes = [{"entry_id": json.loads(r["checkpoint_json"])["contract"]["entry_id"],
                            "outcome": json.loads(r["checkpoint_json"])} for r in self.connection.execute(
                                "SELECT checkpoint_json FROM local_progress WHERE job_id=? ORDER BY ordinal", (job["job_id"],)).fetchall()]
                        if (receipt["job_id"] != job["job_id"] or receipt["job_identity"] != job["identity"]
                                or receipt["snapshot_sha256"] != item["snapshot_sha256"]
                                or receipt["dataset_id"] != payload["dataset_id"]
                                or receipt["cohort_id"] != payload["cohort_id"]
                                or receipt["cutoff_utc"] != payload["cutoff_utc"]
                                or receipt["source_receipt"] != item["snapshot_metadata"]["source_receipt"]
                                or sorted(r["entry_id"] for r in receipt["outcomes"]) != selected
                                or sorted(r["entry_id"] for r in receipt["gate"]["representatives"]) != selected
                                or receipt["outcomes"] != checkpoint_outcomes
                                or receipt["gate"]["policy"] != identity["gate_policy"]
                                or receipt["candle_evaluations"] != job["candle_evaluations"]
                                or job["status"] != ("COMPLETE" if receipt["computation_complete"] else "BLOCKED")
                                or job["next_ordinal"] != job["total_entries"]):
                            raise ValueError("global cohort receipt/representative binding mismatch")
                        result.update(receipt_sha256=receipt["receipt_sha256"], gate=receipt["gate"],
                            computation_complete=receipt["computation_complete"], status_counts=receipt["status_counts"],
                            outcomes=receipt["outcomes"])
                    elif job["status"] in _TERMINAL:
                        raise ValueError("terminal global scope is missing its immutable receipt")
                results.append({**result, **_AUTHORITY})
            value = {"cohort_store_version": VERSION, "plan_id": plan_id, "plan_identity": identity,
                "coverage_receipt": payload["coverage_receipt"], "declared_scopes": len(results),
                "outcome_trials_executed": sum(r["outcome_trial_executed"] for r in results),
                "scopes": results, "all_scopes_processed": all(r["status"] in _TERMINAL for r in results),
                "computation_complete": all(r["computation_complete"] is True for r in results),
                "source_coverage_complete": payload["coverage_receipt"]["source_decisions_complete"],
                "scope_pooling": False, "scope_ranking": False, "parts_are_trials": False,
                "multiple_testing_adjusted": False, "validated_discovery": False,
                "research_classification": "RETROSPECTIVE_EXPLORATORY_GLOBAL_COHORT_DESCRIPTIVE",
                "is_prospective_formula_evidence": False, **_AUTHORITY}
            return {**value, "report_sha256": contracts.digest(value)}
        finally:
            self.connection.execute("ROLLBACK")
