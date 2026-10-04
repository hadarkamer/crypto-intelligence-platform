"""PostgreSQL execution of freshly admitted, frozen global research cohorts.

Source preparation and representative selection stay in the existing pure
adapter. This module owns only durable execution, not discovery, scheduling new
cohorts, source collection, publication or trading. Public methods own their
transactions and require an idle dict-row connection. No runtime DDL.
"""
from __future__ import annotations

from collections import Counter
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

import research_no_horizon_cohort_outcomes as preparation
import research_no_horizon_contract as contracts
import research_no_horizon_first_touch as first_touch
import research_no_horizon_gate as gate
import research_no_horizon_store as child

VERSION = "no-horizon-postgres-cohort-store-v1"
RECEIPT_VERSION = "no-horizon-postgres-immutable-receipt-v1"
LeaseLost = child.LeaseLost
_AUTHORITY = {"runtime_authorized": False, "telegram_authorized": False,
              "trading_authorized": False}
_TERMINAL = {"COMPLETE", "BLOCKED", "INPUT_BLOCKED"}
TABLES = tuple("research_no_horizon_" + suffix for suffix in
    ("schema", "plans", "candles", "scopes", "entries", "progress", "receipts", "work_commits"))


def implementation() -> dict[str, Any]:
    """Keep runtime implementations distinct from the original frozen engine."""
    root = Path(__file__).resolve().parent
    original = preparation.implementation()
    files = dict(original["implementation_files"])
    for path in (Path(__file__), root / "migrations/056_no_horizon_runtime.sql"):
        files[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
    return {"store_version": VERSION, "receipt_version": RECEIPT_VERSION,
            "preparation": original, "implementation_files": dict(sorted(files.items()))}


def _idle(conn):
    if int(conn.info.transaction_status) != 0:
        raise ValueError("fresh idle connection required; caller transaction is not owned here")


def schema_status(conn) -> dict[str, Any]:
    _idle(conn)
    with conn.transaction():
        missing = [name for name in TABLES if conn.execute(
            "SELECT to_regclass(%s) AS relation", (name,)).fetchone()["relation"] is None]
        version = None if "research_no_horizon_schema" in missing else conn.execute(
            "SELECT version FROM research_no_horizon_schema WHERE singleton=TRUE").fetchone()
        compatible = bool(version and version["version"] == VERSION)
        return {"schema_present": not missing and compatible, "missing_tables": missing,
                "schema_version": version["version"] if version else None,
                "expected_version": VERSION, "compatible": compatible}


class PostgresCohortStore:
    def __init__(self, conn):
        _idle(conn)
        self.connection = conn
        self.versions = implementation()
        self.implementation_sha256 = contracts.digest(self.versions)

    def submit_cohort(self, declaration, anchor, load_part, *, cohort_key=None):
        """Raw anchored parts are validated before the first database write.

        PreparedCohort is deliberately not a public serialized receipt import
        contract. The internal helper below accepts only the freshly validated
        output of the existing source/parent/representative preparation.
        """
        _idle(self.connection)
        prepared = preparation.prepare_cohort_submission(declaration, anchor, load_part)
        return self._submit_prepared(prepared, cohort_key=cohort_key)

    def _submit_prepared(self, prepared, *, cohort_key=None):
        _idle(self.connection)
        preparation.validate_prepared(prepared)
        if cohort_key is not None:
            child._name(cohort_key, "cohort_key")
        backend = {"store_version": VERSION, "prepared_plan_id": prepared.plan_id,
                   "implementation_sha256": self.implementation_sha256,
                   "implementation": self.versions}
        plan_id = contracts.digest(backend)
        identity = contracts.canonical(prepared.identity)
        payload = contracts.canonical(prepared.payload)
        backend_json = contracts.canonical(backend)
        p = prepared.payload
        with self.connection.transaction():
            # The unique named key remains immutable even across implementations.
            inserted = self.connection.execute("""INSERT INTO research_no_horizon_plans
                (plan_id,prepared_plan_id,cohort_key,implementation_sha256,
                 backend_identity_json,identity_json,payload_json,cutoff_utc,source_candles,scope_count)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING RETURNING plan_id""",
                (plan_id, prepared.plan_id, cohort_key, self.implementation_sha256,
                 backend_json, identity, payload, contracts.utc(p["cutoff_utc"]),
                 len(p["candles"]), len(p["scope_plans"]))).fetchone()
            if inserted is None:
                row = self.connection.execute("SELECT * FROM research_no_horizon_plans WHERE plan_id=%s",
                                              (plan_id,)).fetchone()
                if (row is None or cohort_key is not None and row["cohort_key"] != cohort_key
                        or row["admission_sealed"] is not True
                        or (row["identity_json"], row["payload_json"], row["backend_identity_json"])
                        != (identity, payload, backend_json)):
                    raise ValueError("cohort key or runtime plan already binds a different immutable input")
                # A concurrent checkpoint commit must not split this exact-input
                # verification between old counters and newer progress rows.
                self.connection.execute("""SELECT scope_ordinal FROM research_no_horizon_scopes
                    WHERE plan_id=%s ORDER BY scope_ordinal FOR SHARE""", (plan_id,)).fetchall()
                self._verify_materialized(row, prepared)
                return plan_id
            with self.connection.cursor() as cursor:
                cursor.executemany("INSERT INTO research_no_horizon_candles VALUES(%s,%s,%s)",
                    [(plan_id, contracts.utc(bar["open_time_utc"]), contracts.canonical(bar))
                     for bar in p["candles"]])
                for item in p["scope_plans"]:
                    ordinal = item["ordinal"]
                    entries = item["normalized_entries"]
                    cursor.execute("""INSERT INTO research_no_horizon_scopes
                        (plan_id,scope_ordinal,scope_plan_json,total_entries,input_error,status)
                        VALUES(%s,%s,%s,%s,%s,%s)""", (plan_id, ordinal, contracts.canonical(item),
                            len(entries), item["input_error"],
                            "INPUT_BLOCKED" if item["input_error"] is not None else "PENDING"))
                    cursor.executemany("INSERT INTO research_no_horizon_entries VALUES(%s,%s,%s,%s,%s)",
                        [(plan_id, ordinal, index, entry["contract"]["entry_id"], contracts.canonical(entry))
                         for index, entry in enumerate(entries)])
                    cursor.executemany("""INSERT INTO research_no_horizon_progress
                        (plan_id,scope_ordinal,entry_ordinal,checkpoint_json) VALUES(%s,%s,%s,%s)""",
                        [(plan_id, ordinal, index, contracts.canonical(first_touch.initialize(
                            entry["contract"], cutoff_utc=p["cutoff_utc"])))
                         for index, entry in enumerate(entries)])
            self.connection.execute("UPDATE research_no_horizon_plans SET admission_sealed=TRUE WHERE plan_id=%s",
                                    (plan_id,))
        return plan_id

    def _plan_header(self, plan_id, *, compatible=False):
        row = self.connection.execute("""SELECT plan_id,prepared_plan_id,cohort_key,
            implementation_sha256,backend_identity_json,identity_json,cutoff_utc,source_candles,scope_count,admission_sealed
            FROM research_no_horizon_plans WHERE plan_id=%s""", (plan_id,)).fetchone()
        if row is None:
            raise KeyError("unknown PostgreSQL cohort plan")
        backend = json.loads(row["backend_identity_json"])
        identity = json.loads(row["identity_json"])
        if (row["admission_sealed"] is not True or contracts.digest(backend) != row["plan_id"]
                or contracts.digest(identity) != row["prepared_plan_id"]
                or backend["prepared_plan_id"] != row["prepared_plan_id"]
                or backend["implementation_sha256"] != row["implementation_sha256"]
                or contracts.digest(backend["implementation"]) != row["implementation_sha256"]):
            raise ValueError("runtime plan identity integrity mismatch")
        if compatible and (row["implementation_sha256"] != self.implementation_sha256
                or backend["implementation"] != self.versions):
            raise ValueError("runtime plan requires its frozen implementation; submit a new revision")
        return row

    def claim_job(self, worker_id, *, lease_seconds=60, plan_id=None):
        _idle(self.connection)
        child._name(worker_id, "worker_id")
        child._bound(lease_seconds, "lease_seconds", 3600)
        with self.connection.transaction():
            if plan_id is not None:
                self._plan_header(plan_id, compatible=True)
            params = [self.implementation_sha256]
            filter_sql = ""
            if plan_id is not None:
                filter_sql = " AND s.plan_id=%s"
                params.append(plan_id)
            row = self.connection.execute("""SELECT s.plan_id,s.scope_ordinal
                FROM research_no_horizon_scopes s JOIN research_no_horizon_plans p USING(plan_id)
                WHERE p.implementation_sha256=%s AND p.admission_sealed=TRUE AND
                 (s.status='PENDING' OR (s.status='RUNNING' AND s.lease_until<=clock_timestamp()))"""
                 + filter_sql + """ ORDER BY s.last_claimed_at_utc NULLS FIRST,p.created_at_utc,
                    s.plan_id,s.scope_ordinal LIMIT 1 FOR UPDATE OF s SKIP LOCKED""", params).fetchone()
            if row is None:
                return None
            self._plan_header(row["plan_id"], compatible=True)
            return dict(self.connection.execute("""UPDATE research_no_horizon_scopes
                SET status='RUNNING',worker_id=%s,fencing_token=fencing_token+1,
                    lease_until=clock_timestamp()+%s*INTERVAL '1 second',
                    last_claimed_at_utc=clock_timestamp()
                WHERE plan_id=%s AND scope_ordinal=%s
                RETURNING plan_id,scope_ordinal,worker_id,fencing_token,lease_until""",
                (worker_id, lease_seconds, row["plan_id"], row["scope_ordinal"])).fetchone())

    def _check_claim(self, claim, *, lock=False):
        if (not isinstance(claim, Mapping) or type(claim.get("fencing_token")) is not int
                or type(claim.get("scope_ordinal")) is not int):
            raise LeaseLost("invalid PostgreSQL fencing claim")
        row = self.connection.execute("""SELECT * FROM research_no_horizon_scopes
            WHERE plan_id=%s AND scope_ordinal=%s AND status='RUNNING'
            AND worker_id=%s AND fencing_token=%s AND lease_until>clock_timestamp()"""
            + (" FOR UPDATE" if lock else ""),
            (claim.get("plan_id"), claim["scope_ordinal"], claim.get("worker_id"),
             claim["fencing_token"])).fetchone()
        if row is None:
            raise LeaseLost("claim is expired, released or fenced by a new owner")
        return row

    @staticmethod
    def _entry_state(row, expected, *, cutoff, processed):
        metadata = json.loads(row["metadata_json"])
        state = first_touch.validate_state(json.loads(row["checkpoint_json"]))
        if (metadata != expected or row["entry_id"] != expected["contract"]["entry_id"]
                or state["contract"] != expected["contract"]
                or contracts.utc(state["cutoff_utc"]) != contracts.utc(cutoff)
                or row["processed"] is not processed):
            raise ValueError("runtime entry/checkpoint identity or progress mismatch")
        return metadata, state

    def process_claim(self, claim, *, candle_budget=1024, entry_budget=128, batch_size=128):
        _idle(self.connection)
        child._bound(candle_budget, "candle_budget", 100000000)
        child._bound(entry_budget, "entry_budget", child.replay.MAX_OPPORTUNITIES)
        child._bound(batch_size, "batch_size", 100000)
        updates = []
        with self.connection.transaction():
            # Computation holds a coherent read snapshot, never a write lock.
            self.connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            job = self._check_claim(claim)
            plan = self._plan_header(job["plan_id"], compatible=True)
            item = json.loads(job["scope_plan_json"])
            expected = item["normalized_entries"]
            if (item["ordinal"] != job["scope_ordinal"] or item["input_error"] is not None
                    or len(expected) != job["total_entries"]):
                raise ValueError("runtime scope identity mismatch")
            rows = self.connection.execute("""SELECT e.*,p.checkpoint_json,p.processed
                FROM research_no_horizon_entries e JOIN research_no_horizon_progress p
                USING(plan_id,scope_ordinal,entry_ordinal)
                WHERE e.plan_id=%s AND e.scope_ordinal=%s AND e.entry_ordinal>=%s
                ORDER BY e.entry_ordinal LIMIT %s""",
                (job["plan_id"], job["scope_ordinal"], job["next_ordinal"], entry_budget)).fetchall()
            if [row["entry_ordinal"] for row in rows] != list(range(job["next_ordinal"],
                    min(job["total_entries"], job["next_ordinal"] + entry_budget))):
                raise ValueError("runtime entry/checkpoint manifest incomplete")
            remaining, next_ordinal = candle_budget, job["next_ordinal"]
            for row in rows:
                if not remaining:
                    break
                index = row["entry_ordinal"]
                _, state = self._entry_state(row, expected[index], cutoff=plan["cutoff_utc"], processed=False)
                finished = False
                while remaining:
                    limit = min(batch_size, remaining)
                    bars = self.connection.execute("""SELECT open_time_utc,candle_json
                        FROM research_no_horizon_candles WHERE plan_id=%s AND open_time_utc>=%s
                        ORDER BY open_time_utc LIMIT %s""",
                        (job["plan_id"], contracts.utc(state["next_open_utc"]), limit)).fetchall()
                    candles = [json.loads(bar["candle_json"]) for bar in bars]
                    if any(contracts.utc(candle["open_time_utc"]) != bar["open_time_utc"]
                           for candle, bar in zip(candles, bars)):
                        raise ValueError("runtime candle index identity mismatch")
                    before = state["processed_candles"]
                    state = first_touch.advance(state, candles,
                        cutoff_utc=plan["cutoff_utc"], max_candles=limit)
                    consumed = state["processed_candles"] - before
                    remaining -= consumed
                    if state["progress"] != "MORE_DATA_REQUIRED" or not consumed:
                        finished = True
                        break
                updates.append((contracts.canonical(state), finished, job["plan_id"], job["scope_ordinal"], index))
                if not finished:
                    break
                next_ordinal = index + 1
        consumed = candle_budget - remaining
        evaluations = job["candle_evaluations"] + consumed
        with self.connection.transaction():
            current = self._check_claim(claim, lock=True)
            self._plan_header(job["plan_id"], compatible=True)
            if (current["next_ordinal"], current["candle_evaluations"]) != (
                    job["next_ordinal"], job["candle_evaluations"]):
                raise LeaseLost("claimed checkpoint changed during computation")
            with self.connection.cursor() as cursor:
                cursor.executemany("""UPDATE research_no_horizon_progress
                    SET checkpoint_json=%s,processed=%s
                    WHERE plan_id=%s AND scope_ordinal=%s AND entry_ordinal=%s""", updates)
            status = "PENDING"
            if next_ordinal == job["total_entries"]:
                receipt = self._make_receipt(plan, job, item, evaluations)
                status = "COMPLETE" if receipt["computation_complete"] else "BLOCKED"
                self.connection.execute("INSERT INTO research_no_horizon_receipts VALUES(%s,%s,%s,%s)",
                    (job["plan_id"], job["scope_ordinal"], receipt["receipt_sha256"], contracts.canonical(receipt)))
            self.connection.execute("""INSERT INTO research_no_horizon_work_commits
                VALUES(%s,%s,%s,%s,%s,%s,%s)""", (job["plan_id"], job["scope_ordinal"], claim["fencing_token"],
                    job["candle_evaluations"], evaluations, job["next_ordinal"], next_ordinal))
            # Database-clock check at the final mutation also covers receipt work.
            updated = self.connection.execute("""UPDATE research_no_horizon_scopes
                SET status=%s,next_ordinal=%s,candle_evaluations=%s,worker_id=NULL,lease_until=NULL
                WHERE plan_id=%s AND scope_ordinal=%s AND status='RUNNING' AND worker_id=%s
                    AND fencing_token=%s AND lease_until>clock_timestamp()
                    AND next_ordinal=%s AND candle_evaluations=%s RETURNING plan_id""",
                (status, next_ordinal, evaluations, job["plan_id"], job["scope_ordinal"],
                 claim["worker_id"], claim["fencing_token"], job["next_ordinal"],
                 job["candle_evaluations"])).fetchone()
            if updated is None:
                raise LeaseLost("claim expired before checkpoint commit")
        return {"plan_id": job["plan_id"], "scope_ordinal": job["scope_ordinal"],
                "status": status, "next_ordinal": next_ordinal,
                "candle_evaluations": evaluations, "candle_evaluations_this_run": consumed,
                **_AUTHORITY}

    def _all_entries(self, plan_id, ordinal):
        return self.connection.execute("""SELECT e.*,p.checkpoint_json,p.processed
            FROM research_no_horizon_entries e JOIN research_no_horizon_progress p
            USING(plan_id,scope_ordinal,entry_ordinal)
            WHERE e.plan_id=%s AND e.scope_ordinal=%s ORDER BY e.entry_ordinal""",
            (plan_id, ordinal)).fetchall()

    def _make_receipt(self, plan, job, item, evaluations):
        rows = self._all_entries(job["plan_id"], job["scope_ordinal"])
        expected = item["normalized_entries"]
        if [row["entry_ordinal"] for row in rows] != list(range(len(expected))):
            raise ValueError("complete representative population required before gate")
        normalized, outcomes, incomplete = [], [], []
        for row, entry in zip(rows, expected):
            metadata, state = self._entry_state(row, entry, cutoff=plan["cutoff_utc"], processed=True)
            normalized.append({**metadata, "outcome": state})
            entry_id = entry["contract"]["entry_id"]
            outcomes.append({"entry_id": entry_id, "outcome": state})
            if not (state["status"] in first_touch.TERMINAL or
                    state["status"] == "OPEN" and state["coverage_complete_through_cutoff"] is True):
                incomplete.append(entry_id)
        if sum(row["outcome"]["processed_candles"] for row in outcomes) != evaluations:
            raise ValueError("runtime checkpoint work counter mismatch")
        identity = json.loads(plan["identity_json"])
        metadata = item["snapshot_metadata"]
        aggregate = gate.evaluate_gate(normalized, as_of_utc=metadata["cutoff_utc"],
            source_coverage_complete=metadata["source_coverage_complete"] and not incomplete,
            policy=identity["gate_policy"])
        counts = Counter(row["outcome"]["status"] for row in outcomes)
        value = {"receipt_version": RECEIPT_VERSION, "plan_id": plan["plan_id"],
            "prepared_plan_id": plan["prepared_plan_id"], "scope_ordinal": job["scope_ordinal"],
            "runtime_implementation_sha256": plan["implementation_sha256"],
            "snapshot_sha256": item["snapshot_sha256"], "dataset_id": metadata["dataset_id"],
            "cohort_id": metadata["cohort_id"], "source_route": metadata["source_route"],
            "source_receipt": metadata["source_receipt"], "source_provenance_verified_by_this_tool": False,
            "cutoff_utc": metadata["cutoff_utc"], "opportunities": len(normalized),
            "source_candles": plan["source_candles"], "candle_evaluations": evaluations,
            "cohort_processing_complete": True, "incomplete_entry_ids": incomplete,
            "computation_complete": not incomplete,
            "status_counts": {key: counts[key] for key in first_touch.STATUSES},
            "outcomes": outcomes, "gate": aggregate, **_AUTHORITY}
        return {**value, "receipt_sha256": contracts.digest(value)}

    def run_once(self, worker_id, *, plan_id=None, lease_seconds=60,
                 candle_budget=1024, entry_budget=128, batch_size=128):
        # Bad budgets must not take a lease and delay otherwise runnable work.
        child._bound(candle_budget, "candle_budget", 100000000)
        child._bound(entry_budget, "entry_budget", child.replay.MAX_OPPORTUNITIES)
        child._bound(batch_size, "batch_size", 100000)
        claim = self.claim_job(worker_id, lease_seconds=lease_seconds, plan_id=plan_id)
        return None if claim is None else self.process_claim(claim,
            candle_budget=candle_budget, entry_budget=entry_budget, batch_size=batch_size)

    def _verify_materialized(self, row, prepared):
        """Full evidence verification for admission reuse and explicit reports.

        Work passes read only bounded suffixes from immutable indexed candles;
        reports verify their complete materialization against the frozen payload.
        """
        payload = prepared.payload
        bars = self.connection.execute("SELECT * FROM research_no_horizon_candles WHERE plan_id=%s ORDER BY open_time_utc",
                                       (row["plan_id"],)).fetchall()
        scopes = self.connection.execute("SELECT * FROM research_no_horizon_scopes WHERE plan_id=%s ORDER BY scope_ordinal",
                                         (row["plan_id"],)).fetchall()
        if (row["prepared_plan_id"] != prepared.plan_id
                or row["source_candles"] != len(payload["candles"])
                or contracts.utc(payload["cutoff_utc"]) != row["cutoff_utc"]
                or row["scope_count"] != len(payload["scope_plans"])
                or len(bars) != len(payload["candles"])
                or any(bar["open_time_utc"] != contracts.utc(expected["open_time_utc"])
                    or json.loads(bar["candle_json"]) != expected
                    for bar, expected in zip(bars, payload["candles"]))
                or [scope["scope_ordinal"] for scope in scopes] != list(range(len(payload["scope_plans"])))):
            raise ValueError("runtime materialized cohort/candle manifest mismatch")
        for scope, item in zip(scopes, payload["scope_plans"]):
            if (json.loads(scope["scope_plan_json"]) != item or scope["input_error"] != item["input_error"]
                    or scope["total_entries"] != len(item["normalized_entries"])):
                raise ValueError("runtime materialized scope manifest mismatch")
            entries = self._all_entries(row["plan_id"], scope["scope_ordinal"])
            expected = item["normalized_entries"]
            if [entry["entry_ordinal"] for entry in entries] != list(range(len(expected))):
                raise ValueError("runtime materialized entry manifest mismatch")
            total = 0
            for index, (entry, metadata) in enumerate(zip(entries, expected)):
                _, state = self._entry_state(entry, metadata, cutoff=payload["cutoff_utc"],
                                            processed=index < scope["next_ordinal"])
                total += state["processed_candles"]
            if total != scope["candle_evaluations"]:
                raise ValueError("runtime materialized work counter mismatch")
        return scopes

    def report(self, plan_id):
        _idle(self.connection)
        with self.connection.transaction():
            self.connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            header = self._plan_header(plan_id)
            row = self.connection.execute("SELECT * FROM research_no_horizon_plans WHERE plan_id=%s", (plan_id,)).fetchone()
            prepared = preparation.PreparedCohort(json.loads(row["identity_json"]),
                json.loads(row["payload_json"]), row["prepared_plan_id"])
            preparation.validate_prepared(prepared, check_implementation=False)
            states = self._verify_materialized(row, prepared)
            payload = prepared.payload
            results = []
            for state, item in zip(states, payload["scope_plans"]):
                scope = item["scope"]
                ordinal = state["scope_ordinal"]
                commits = self.connection.execute("""SELECT * FROM research_no_horizon_work_commits
                    WHERE plan_id=%s AND scope_ordinal=%s ORDER BY fencing_token""", (plan_id, ordinal)).fetchall()
                previous_evaluations, previous_ordinal = 0, 0
                for commit in commits:
                    if ((commit["starting_evaluations"], commit["starting_ordinal"]) !=
                            (previous_evaluations, previous_ordinal)
                            or commit["fencing_token"] > state["fencing_token"]):
                        raise ValueError("runtime committed-work chain mismatch")
                    previous_evaluations, previous_ordinal = commit["ending_evaluations"], commit["ending_ordinal"]
                if (previous_evaluations, previous_ordinal) != (state["candle_evaluations"], state["next_ordinal"]):
                    raise ValueError("runtime committed-work totals mismatch")
                result = {key: scope[key] for key in
                          ("scope_id", "candidate_key", "base_direction", "analysis_direction", "threshold_pct")}
                result.update(ordinal=ordinal, job_id=None if state["input_error"] else
                    contracts.digest({"plan_id": plan_id, "scope_ordinal": ordinal}),
                    error=state["input_error"], status=state["status"],
                    representatives=item["representatives"], missing_entry_ids=item["missing_entry_ids"],
                    missing_entry_locators=item["missing_entry_locators"], snapshot_sha256=item["snapshot_sha256"],
                    receipt_sha256=None, gate=None, computation_complete=False if state["input_error"] else None,
                    candle_evaluations=state["candle_evaluations"], outcome_trial_executed=bool(commits))
                stored = self.connection.execute("SELECT * FROM research_no_horizon_receipts WHERE plan_id=%s AND scope_ordinal=%s",
                                                 (plan_id, ordinal)).fetchone()
                if state["status"] in ("COMPLETE", "BLOCKED"):
                    if stored is None:
                        raise ValueError("terminal runtime scope is missing its immutable receipt")
                    receipt = json.loads(stored["payload_json"])
                    if (receipt["receipt_version"] != RECEIPT_VERSION
                            or receipt["receipt_sha256"] != stored["receipt_sha256"]
                            or receipt["receipt_sha256"] != contracts.digest({key: value for key, value in receipt.items()
                                                                           if key != "receipt_sha256"})
                            or receipt["plan_id"] != plan_id or receipt["scope_ordinal"] != ordinal
                            or receipt["prepared_plan_id"] != row["prepared_plan_id"]
                            or receipt["runtime_implementation_sha256"] != row["implementation_sha256"]
                            or receipt["snapshot_sha256"] != item["snapshot_sha256"]
                            or any(receipt[key] != payload[key] for key in ("dataset_id", "cohort_id", "source_route"))
                            or receipt["source_candles"] != row["source_candles"]
                            or receipt["opportunities"] != len(item["normalized_entries"])
                            or receipt["cohort_processing_complete"] is not True
                            or receipt["source_receipt"] != item["snapshot_metadata"]["source_receipt"]
                            or receipt["cutoff_utc"] != payload["cutoff_utc"]
                            or receipt["gate"]["policy"] != prepared.identity["gate_policy"]
                            or receipt["candle_evaluations"] != state["candle_evaluations"]
                            or state["status"] != ("COMPLETE" if receipt["computation_complete"] else "BLOCKED")
                            or not commits or state["next_ordinal"] != state["total_entries"]
                            or any(receipt[key] is not False for key in _AUTHORITY)):
                        raise ValueError("runtime terminal receipt binding mismatch")
                    entries = self._all_entries(plan_id, ordinal)
                    outcomes = [{"entry_id": entry["entry_id"], "outcome": json.loads(entry["checkpoint_json"])}
                                for entry in entries]
                    selected = sorted(rep["entry_id"] for rep in item["representatives"])
                    if (receipt["outcomes"] != outcomes
                            or sorted(outcome["entry_id"] for outcome in receipt["outcomes"]) != selected
                            or sorted(rep["entry_id"] for rep in receipt["gate"]["representatives"]) != selected):
                        raise ValueError("runtime receipt representative/outcome mismatch")
                    by_id = {entry["contract"]["entry_id"]: entry for entry in item["normalized_entries"]}
                    outcome_by_id = {entry["entry_id"]: entry["outcome"] for entry in outcomes}
                    for representative in receipt["gate"]["representatives"]:
                        entry = by_id[representative["entry_id"]]
                        outcome = outcome_by_id[representative["entry_id"]]
                        if (representative["btc_parent_movement_id"] != entry["btc_parent_movement_id"]
                                or contracts.utc(representative["decision_time_utc"]) !=
                                    contracts.utc(entry["contract"]["decision_time_utc"])
                                or representative["status"] != ("DATA_MISSING" if outcome["status"] == "OPEN"
                                    and not outcome["coverage_complete_through_cutoff"] else outcome["status"])):
                            raise ValueError("runtime gate representative evidence mismatch")
                    result.update(receipt_sha256=receipt["receipt_sha256"], gate=receipt["gate"],
                        computation_complete=receipt["computation_complete"],
                        status_counts=receipt["status_counts"], outcomes=receipt["outcomes"])
                elif stored is not None:
                    raise ValueError("unfinished runtime scope has a terminal receipt")
                if state["status"] == "INPUT_BLOCKED" and (commits or state["candle_evaluations"] or state["fencing_token"]):
                    raise ValueError("input-blocked runtime scope fabricated outcome work")
                results.append({**result, **_AUTHORITY})
            value = {"cohort_store_version": VERSION, "plan_id": plan_id,
                "prepared_plan_id": row["prepared_plan_id"], "plan_identity": prepared.identity,
                "runtime_identity": json.loads(header["backend_identity_json"]),
                "coverage_receipt": payload["coverage_receipt"], "declared_scopes": len(results),
                "outcome_trials_executed": sum(result["outcome_trial_executed"] for result in results),
                "scopes": results, "all_scopes_processed": all(result["status"] in _TERMINAL for result in results),
                "computation_complete": all(result["computation_complete"] is True for result in results),
                "source_coverage_complete": payload["coverage_receipt"]["source_decisions_complete"],
                "scope_pooling": False, "scope_ranking": False, "parts_are_trials": False,
                "multiple_testing_adjusted": False, "validated_discovery": False,
                "research_classification": "RETROSPECTIVE_EXPLORATORY_GLOBAL_COHORT_DESCRIPTIVE",
                "is_prospective_formula_evidence": False, **_AUTHORITY}
            return {**value, "report_sha256": contracts.digest(value)}
