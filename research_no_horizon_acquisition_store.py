"""Durable acquisition of explicitly registered frozen research cohorts.

One root anchor is retained forever. Bounded later reads prove exact anchored
bytes; retries never refresh the source population. Destination and source
connections are distinct. This adapter creates no declarations automatically,
changes no source rows, and grants no publication or trading authority.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import research_no_horizon_acquisition_source as source
import research_no_horizon_cohort as cohort
import research_no_horizon_contract as contracts
import research_no_horizon_manifest_sql as sql_builder
import research_no_horizon_postgres_store as executor
import research_no_horizon_store as child

VERSION = "no-horizon-cohort-acquisition-store-v1"
MAX_RETAINED_PROOF_BYTES = 512 * 1024 * 1024
MAX_LEAF_BUDGET = 32
_AUTHORITY = {"runtime_authorized": False, "telegram_authorized": False, "trading_authorized": False}
_TERMINAL = {"ADMITTED", "BLOCKED"}
LeaseLost = child.LeaseLost
TABLES = tuple("research_no_horizon_acquisition_" + name for name in
               ("schema", "requests", "anchors", "leaves", "receipts"))


def implementation():
    root = Path(__file__).resolve().parent
    runtime = executor.implementation()
    files = dict(runtime["implementation_files"])
    for path in (Path(__file__), Path(source.__file__), root / "migrations/057_no_horizon_acquisition.sql"):
        files[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
    return {"version": VERSION, "executor": runtime,
            "max_retained_proof_bytes": MAX_RETAINED_PROOF_BYTES,
            "max_queries_per_pass": MAX_LEAF_BUDGET,
            "implementation_files": dict(sorted(files.items()))}


def schema_status(conn):
    executor._idle(conn)
    base = executor.schema_status(conn)
    with conn.transaction():
        missing = [table for table in TABLES if conn.execute(
            "SELECT to_regclass(%s) AS relation", (table,)).fetchone()["relation"] is None]
        version = None if TABLES[0] in missing else conn.execute(
            "SELECT version FROM research_no_horizon_acquisition_schema WHERE singleton=TRUE").fetchone()
        compatible = bool(version and version["version"] == VERSION)
        return {"schema_present": base["schema_present"] and not missing and compatible,
                "missing_tables": base["missing_tables"] + missing,
                "schema_version": version["version"] if version else None,
                "expected_version": VERSION, "compatible": compatible and base["compatible"]}


def _encoded(value):
    result = contracts.canonical(value)
    return result, len(result.encode("utf-8"))


class AcquisitionStore:
    def __init__(self, conn):
        executor._idle(conn)
        self.connection = conn
        self.versions = implementation()
        self.implementation_sha256 = contracts.digest(self.versions)

    def register_request(self, declaration, *, request_key=None, require_before_start=False):
        """Register one population; optionally reject newly late registration atomically.

        Existing identical requests retain their actual registration timestamp.
        The timestamp records database row creation, not unseen-outcome proof.
        """
        executor._idle(self.connection)
        frozen = cohort.normalize_declaration(declaration)
        if type(require_before_start) is not bool:
            raise ValueError("explicit boolean require_before_start required")
        start = contracts.utc(frozen["source_start_utc"])
        if request_key is not None:
            child._name(request_key, "request_key")
        identity = {"version": VERSION, "declaration_sha256": contracts.digest(frozen),
                    "implementation_sha256": self.implementation_sha256,
                    "implementation": self.versions}
        request_id = contracts.digest(identity)
        declaration_json = contracts.canonical(frozen)
        identity_json = contracts.canonical(identity)
        due = max(contracts.utc(frozen["declared_at_utc"]), contracts.utc(frozen["cutoff_utc"]))
        with self.connection.transaction():
            inserted = self.connection.execute("""INSERT INTO research_no_horizon_acquisition_requests
                (request_id,request_key,identity_json,declaration_json,implementation_sha256,not_before_utc)
                VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING request_id,created_at_utc""",
                (request_id, request_key, identity_json, declaration_json, self.implementation_sha256, due)).fetchone()
            if inserted is None:
                row = self.connection.execute("""SELECT request_key,identity_json,declaration_json,created_at_utc
                    FROM research_no_horizon_acquisition_requests WHERE request_id=%s""", (request_id,)).fetchone()
                if (row is None or request_key is not None and row["request_key"] != request_key
                        or row["identity_json"] != identity_json or row["declaration_json"] != declaration_json):
                    raise ValueError("request key already binds another immutable cohort or implementation")
                if require_before_start and row["created_at_utc"] > start:
                    raise ValueError("REGISTRATION_AFTER_SOURCE_START")
            elif require_before_start:
                # Raising inside this transaction rolls the new row back, even
                # when insertion waited on a conflicting transaction.
                checked = self.connection.execute("SELECT clock_timestamp() AS now").fetchone()["now"]
                if inserted["created_at_utc"] > start or checked > start:
                    raise ValueError("REGISTRATION_AFTER_SOURCE_START")
        return request_id

    def registration_info(self, request_id):
        """Read immutable registration identity without loading transport proofs."""
        executor._idle(self.connection)
        with self.connection.transaction():
            row = self._request(request_id)
            declaration = json.loads(row["declaration_json"])
            start = contracts.utc(declaration["source_start_utc"])
            return {"request_id": row["request_id"],
                "declaration_sha256": json.loads(row["identity_json"])["declaration_sha256"],
                "implementation_sha256": row["implementation_sha256"],
                "created_at_utc": row["created_at_utc"].isoformat(),
                "source_start_utc": start.isoformat(),
                "registered_before_source_start": row["created_at_utc"] <= start}


    def _request(self, request_id, *, compatible=False, lock=False):
        row = self.connection.execute("SELECT * FROM research_no_horizon_acquisition_requests WHERE request_id=%s"
                                      + (" FOR UPDATE" if lock else ""), (request_id,)).fetchone()
        if row is None:
            raise KeyError("unknown cohort acquisition request")
        identity, declaration = json.loads(row["identity_json"]), json.loads(row["declaration_json"])
        if (contracts.digest(identity) != row["request_id"]
                or contracts.digest(declaration) != identity["declaration_sha256"]
                or identity["implementation_sha256"] != row["implementation_sha256"]
                or contracts.digest(identity["implementation"]) != row["implementation_sha256"]
                or row["not_before_utc"] != max(contracts.utc(declaration["declared_at_utc"]),
                                                contracts.utc(declaration["cutoff_utc"]))):
            raise ValueError("acquisition request identity integrity mismatch")
        if compatible and (row["implementation_sha256"] != self.implementation_sha256
                           or identity["implementation"] != self.versions):
            raise ValueError("acquisition requires its frozen implementation; register a new request")
        return row

    def claim_request(self, worker_id, *, lease_seconds=120, request_id=None):
        executor._idle(self.connection)
        child._name(worker_id, "worker_id")
        child._bound(lease_seconds, "lease_seconds", 3600)
        with self.connection.transaction():
            if request_id is not None:
                self._request(request_id, compatible=True)
            params = [self.implementation_sha256]
            selected = ""
            if request_id is not None:
                selected = " AND request_id=%s"
                params.append(request_id)
            row = self.connection.execute("""SELECT request_id
                FROM research_no_horizon_acquisition_requests
                WHERE implementation_sha256=%s AND status NOT IN ('ADMITTED','BLOCKED')
                AND not_before_utc<=clock_timestamp()
                AND (worker_id IS NULL OR lease_until<=clock_timestamp())""" + selected + """
                ORDER BY last_claimed_at_utc,request_id LIMIT 1 FOR UPDATE SKIP LOCKED""", params).fetchone()
            if row is None:
                return None
            self._request(row["request_id"], compatible=True)
            return dict(self.connection.execute("""UPDATE research_no_horizon_acquisition_requests
                SET worker_id=%s,fencing_token=fencing_token+1,
                    lease_until=clock_timestamp()+%s*INTERVAL '1 second',last_claimed_at_utc=clock_timestamp()
                WHERE request_id=%s RETURNING request_id,worker_id,fencing_token,lease_until""",
                (worker_id, lease_seconds, row["request_id"])).fetchone())

    def _check_claim(self, claim, *, lock=False):
        if not isinstance(claim, dict) or type(claim.get("fencing_token")) is not int:
            raise LeaseLost("invalid acquisition fencing claim")
        row = self._request(claim.get("request_id"), compatible=True, lock=lock)
        valid = self.connection.execute("""SELECT 1 AS valid FROM research_no_horizon_acquisition_requests
            WHERE request_id=%s AND worker_id=%s AND fencing_token=%s AND lease_until>clock_timestamp()
            AND not_before_utc<=clock_timestamp() AND status NOT IN ('ADMITTED','BLOCKED')""",
            (claim["request_id"], claim.get("worker_id"), claim["fencing_token"])).fetchone()
        if valid is None:
            raise LeaseLost("acquisition lease expired, released or fenced by another owner")
        return row

    def _state(self, claim):
        with self.connection.transaction():
            self.connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            request = self._check_claim(claim)
            anchor = self.connection.execute("SELECT * FROM research_no_horizon_acquisition_anchors WHERE request_id=%s",
                                             (claim["request_id"],)).fetchone()
            leaves = self.connection.execute("""SELECT leaf_ordinal,task_json FROM research_no_horizon_acquisition_leaves
                WHERE request_id=%s ORDER BY leaf_ordinal""", (claim["request_id"],)).fetchall()
        declaration = json.loads(request["declaration_json"])
        sealed = None
        if anchor is not None:
            sealed = source.validate_anchor_proof(declaration, json.loads(anchor["proof_json"]))
            if anchor["validation_error"] is not None or json.loads(anchor["anchor_json"]) != sealed:
                raise ValueError("acquisition persisted anchor binding mismatch")
        tasks = source.leaf_tasks(declaration, sealed) if sealed is not None else []
        if (anchor is None and request["status"] != "WAITING"
                or anchor is not None and request["status"] == "WAITING"
                or [leaf["leaf_ordinal"] for leaf in leaves] != list(range(len(leaves)))
                or len(leaves) > len(tasks)
                or any(json.loads(leaf["task_json"]) != tasks[index] for index, leaf in enumerate(leaves))
                or request["status"] == "READY" and len(leaves) != len(tasks)):
            raise ValueError("acquisition phase or immutable leaf manifest mismatch")
        return request, declaration, sealed, tasks, len(leaves)

    @staticmethod
    def _fetch_reservation(declaration, anchor, task):
        """Reserve transport escaping/SQL overhead before another source query.

        Raw manifest payload bytes may be escaped into the SQL JSON response,
        then again in the retained proof. Eightfold payload allowance plus exact
        SQL and envelope overhead is deliberately conservative, never truncates.
        """
        if task is None:
            return 8 * cohort.MAX_ANCHOR_BYTES + 2 * len(cohort.anchor_sql(declaration).encode()) + 65536
        manifest = anchor["parts"][task["part_ordinal"]]["manifest"]
        if task["kind"] == "source":
            query = sql_builder.source_chunk_sql(manifest, task["ordinals"])
            entries = [manifest["source_entries"][ordinal] for ordinal in task["ordinals"]]
        else:
            query = sql_builder.candle_chunk_sql(manifest, task["ordinals"][0])
            entries = [manifest["candle_pages"][task["ordinals"][0]]]
        return 8 * sum(entry["byte_length"] for entry in entries) + 2 * len(query.encode()) + 65536

    def _save_anchor(self, claim, declaration, proof):
        anchor = source.validate_anchor_proof(declaration, proof)
        tasks = source.leaf_tasks(declaration, anchor)
        proof_json, proof_size = _encoded(proof)
        anchor_json, anchor_size = _encoded(anchor)
        with self.connection.transaction():
            row = self._check_claim(claim, lock=True)
            if row["status"] != "WAITING":
                raise LeaseLost("anchor already persisted by an earlier pass")
            size = row["retained_proof_bytes"] + proof_size + anchor_size
            if size + 65536 > MAX_RETAINED_PROOF_BYTES:
                raise ValueError("ACQUISITION_RETAINED_PROOF_BYTE_LIMIT_EXCEEDED")
            self.connection.execute("INSERT INTO research_no_horizon_acquisition_anchors VALUES(%s,%s,%s,NULL)",
                                    (claim["request_id"], proof_json, anchor_json))
            self._check_claim(claim)
            self.connection.execute("""UPDATE research_no_horizon_acquisition_requests
                SET status=%s,retained_proof_bytes=%s,last_error=NULL WHERE request_id=%s""",
                ("FETCHING" if tasks else "READY", size, claim["request_id"]))

    def _save_leaf(self, claim, declaration, anchor, ordinal, task, proof, total):
        source.validate_chunk_proof(declaration, anchor, proof)
        if any(proof[key] != task[key] for key in ("part_ordinal", "kind", "ordinals")):
            raise ValueError("ACQUISITION_PROOF_TASK_BINDING_MISMATCH")
        proof_json, proof_size = _encoded(proof)
        task_json, task_size = _encoded(task)
        with self.connection.transaction():
            row = self._check_claim(claim, lock=True)
            count = self.connection.execute("SELECT count(*) AS n FROM research_no_horizon_acquisition_leaves WHERE request_id=%s",
                                            (claim["request_id"],)).fetchone()["n"]
            if row["status"] != "FETCHING" or count != ordinal:
                raise LeaseLost("anchored leaf cursor changed during source fetch")
            size = row["retained_proof_bytes"] + proof_size + task_size
            if size + 65536 > MAX_RETAINED_PROOF_BYTES:
                raise ValueError("ACQUISITION_RETAINED_PROOF_BYTE_LIMIT_EXCEEDED")
            self.connection.execute("INSERT INTO research_no_horizon_acquisition_leaves VALUES(%s,%s,%s,%s)",
                                    (claim["request_id"], ordinal, task_json, proof_json))
            self._check_claim(claim)
            self.connection.execute("""UPDATE research_no_horizon_acquisition_requests
                SET status=%s,retained_proof_bytes=%s,last_error=NULL WHERE request_id=%s""",
                ("READY" if ordinal + 1 == total else "FETCHING", size, claim["request_id"]))

    def _release(self, claim, *, error=None):
        with self.connection.transaction():
            self._check_claim(claim, lock=True)
            self.connection.execute("""UPDATE research_no_horizon_acquisition_requests
                SET worker_id=NULL,lease_until=NULL,last_error=%s WHERE request_id=%s""",
                (error, claim["request_id"]))

    def _block(self, claim, error, *, proof=None, diagnostic=None, task=None, coverage_receipt=None):
        """Keep rejected evidence; exceptional oversize retains an explicit digest diagnostic."""
        with self.connection.transaction():
            row = self._check_claim(claim, lock=True)
            saved = self.connection.execute("SELECT anchor_json FROM research_no_horizon_acquisition_anchors WHERE request_id=%s",
                                            (claim["request_id"],)).fetchone()
            anchor = json.loads(saved["anchor_json"]) if saved and saved["anchor_json"] else None
            receipt = {"version": VERSION, "request_id": claim["request_id"], "status": "BLOCKED",
                "reason": error, "task": task, "rejected_proof": proof,
                "diagnostic": diagnostic, "raw_response_retained": proof is not None,
                "coverage_receipt": coverage_receipt, "coverage_receipt_retained": coverage_receipt is not None,
                "declaration_sha256": json.loads(row["identity_json"])["declaration_sha256"],
                "anchor_sha256": anchor["anchor_sha256"] if anchor else None,
                "executor_plan_id": None, **_AUTHORITY}
            # Ordinary validation failures retain full proof. An unexpectedly
            # oversized/corrupt response cannot defeat the hard storage bound.
            receipt_json, size = _encoded(receipt)
            if row["retained_proof_bytes"] + size + 128 > MAX_RETAINED_PROOF_BYTES:
                receipt["rejected_proof"] = None
                receipt["raw_response_retained"] = False
                receipt["coverage_receipt"] = None
                receipt["coverage_receipt_retained"] = False
                receipt["diagnostic"] = {"reason": "ACQUISITION_RETAINED_PROOF_BYTE_LIMIT_EXCEEDED",
                    "proof_sha256": contracts.digest(proof) if proof is not None else None,
                    "proof_bytes": len(contracts.canonical(proof).encode()) if proof is not None else 0,
                    "raw_response_sha256": proof.get("raw_response_sha256") if proof else None,
                    "raw_response_bytes": proof.get("raw_response_bytes") if proof else None,
                    "coverage_receipt_sha256": contracts.digest(coverage_receipt) if coverage_receipt is not None else None,
                    "coverage_receipt_bytes": len(contracts.canonical(coverage_receipt).encode()) if coverage_receipt is not None else 0,
                    "truncated": True, "raw_response_retained": False}
            receipt["receipt_sha256"] = contracts.digest(receipt)
            receipt_json, size = _encoded(receipt)
            if row["retained_proof_bytes"] + size > MAX_RETAINED_PROOF_BYTES:
                raise ValueError("terminal acquisition diagnostic exceeds reserved evidence budget")
            self.connection.execute("INSERT INTO research_no_horizon_acquisition_receipts VALUES(%s,%s)",
                                    (claim["request_id"], receipt_json))
            self._check_claim(claim)
            self.connection.execute("""UPDATE research_no_horizon_acquisition_requests SET status='BLOCKED',
                worker_id=NULL,lease_until=NULL,last_error=%s,retained_proof_bytes=%s WHERE request_id=%s""",
                (error, row["retained_proof_bytes"] + size, claim["request_id"]))

    def _ready_parts(self, claim):
        request, declaration, anchor, tasks, count = self._state(claim)
        if request["status"] != "READY" or count != len(tasks):
            raise ValueError("COMPLETE_ANCHORED_PROOF_SET_REQUIRED")
        with self.connection.transaction():
            self._check_claim(claim)
            rows = self.connection.execute("""SELECT leaf_ordinal,task_json,proof_json
                FROM research_no_horizon_acquisition_leaves WHERE request_id=%s ORDER BY leaf_ordinal""",
                (claim["request_id"],)).fetchall()
        if len(rows) != count:
            raise ValueError("COMPLETE_ANCHORED_PROOF_SET_REQUIRED")
        by_part = {part["ordinal"]: {"source": [], "candles": []} for part in anchor["parts"]}
        for ordinal, row in enumerate(rows):
            if row["leaf_ordinal"] != ordinal or json.loads(row["task_json"]) != tasks[ordinal]:
                raise ValueError("ACQUISITION_LEAF_ORDER_OR_TASK_MISMATCH")
            proof = json.loads(row["proof_json"])
            task = tasks[ordinal]
            if any(proof[key] != task[key] for key in ("part_ordinal", "kind", "ordinals")):
                raise ValueError("ACQUISITION_PROOF_TASK_BINDING_MISMATCH")
            by_part[task["part_ordinal"]][task["kind"]].append(proof)
        parts = [source.assemble_part(declaration, anchor, part["ordinal"],
                    by_part[part["ordinal"]]["source"], by_part[part["ordinal"]]["candles"])
                 for part in anchor["parts"]]
        return request, declaration, anchor, parts

    def _mark_admitted(self, claim, plan_id):
        with self.connection.transaction():
            row = self._check_claim(claim, lock=True)
            plan = self.connection.execute("""SELECT plan_id,identity_json FROM research_no_horizon_plans
                WHERE plan_id=%s AND cohort_key=%s AND admission_sealed=TRUE""",
                (plan_id, "acquisition:" + claim["request_id"])).fetchone()
            if row["status"] != "READY" or plan is None:
                raise ValueError("ACQUISITION_EXECUTOR_HANDOFF_BINDING_MISMATCH")
            anchor_row = self.connection.execute("SELECT anchor_json FROM research_no_horizon_acquisition_anchors WHERE request_id=%s",
                                                 (claim["request_id"],)).fetchone()
            anchor = json.loads(anchor_row["anchor_json"])
            identity = json.loads(plan["identity_json"])
            if (identity["anchor_sha256"] != anchor["anchor_sha256"]
                    or identity["declaration_sha256"] != anchor["declaration_sha256"]):
                raise ValueError("ACQUISITION_EXECUTOR_SOURCE_BINDING_MISMATCH")
            receipt = {"version": VERSION, "request_id": claim["request_id"], "status": "ADMITTED",
                "anchor_sha256": anchor["anchor_sha256"], "declaration_sha256": anchor["declaration_sha256"],
                "executor_plan_id": plan_id, **_AUTHORITY}
            receipt["receipt_sha256"] = contracts.digest(receipt)
            encoded, size = _encoded(receipt)
            if row["retained_proof_bytes"] + size > MAX_RETAINED_PROOF_BYTES:
                raise ValueError("ACQUISITION_RETAINED_PROOF_BYTE_LIMIT_EXCEEDED")
            self.connection.execute("INSERT INTO research_no_horizon_acquisition_receipts VALUES(%s,%s)",
                                    (claim["request_id"], encoded))
            self._check_claim(claim)
            self.connection.execute("""UPDATE research_no_horizon_acquisition_requests
                SET status='ADMITTED',executor_plan_id=%s,worker_id=NULL,lease_until=NULL,last_error=NULL,
                    retained_proof_bytes=%s WHERE request_id=%s""",
                (plan_id, row["retained_proof_bytes"] + size, claim["request_id"]))

    def _admit(self, claim):
        request, declaration, anchor, parts = self._ready_parts(claim)
        def guard(conn):
            if conn is not self.connection:
                raise ValueError("acquisition guard requires the destination connection")
            current = self._check_claim(claim, lock=True)
            if (current["status"] != "READY"
                    or current["retained_proof_bytes"] != request["retained_proof_bytes"]):
                raise LeaseLost("frozen acquisition changed before executor admission")
        store = executor.PostgresCohortStore(self.connection)
        if store.versions != self.versions["executor"]:
            raise RuntimeError("EXECUTOR_IMPLEMENTATION_CHANGED_DURING_ACQUISITION")
        plan_id = store.submit_cohort(declaration, anchor, lambda ordinal: parts[ordinal],
            cohort_key="acquisition:" + claim["request_id"], _transaction_guard=guard)
        # Crash here leaves a valid atomic executor admission. Retry uses exactly
        # this immutable key and rechecks the same proofs; no duplicate plan.
        self._mark_admitted(claim, plan_id)

    def process_claim(self, claim, *, source_connection_factory, leaf_budget=4):
        executor._idle(self.connection)
        child._bound(leaf_budget, "leaf_budget", MAX_LEAF_BUDGET)
        if not callable(source_connection_factory):
            raise ValueError("explicit source connection factory required")
        conn = None
        queries, proof, task = 0, None, None
        try:
            while True:
                # A later preflight error is not a rejection of the preceding
                # successfully committed leaf. Only the current failed fetch
                # can supply rejected raw evidence.
                proof, task = None, None
                request, declaration, anchor, tasks, completed = self._state(claim)
                if request["status"] == "READY":
                    self._admit(claim)
                    break
                if queries == leaf_budget:
                    self._release(claim)
                    break
                task = None if anchor is None else tasks[completed]
                if request["retained_proof_bytes"] + self._fetch_reservation(declaration, anchor, task) > MAX_RETAINED_PROOF_BYTES:
                    self._block(claim, "ACQUISITION_PROOF_BUDGET_BEFORE_FETCH", task=task)
                    break
                if conn is None:
                    try:
                        conn = source_connection_factory()
                        if conn is self.connection:
                            conn = None
                            raise RuntimeError("source and destination connections must be distinct")
                        executor._idle(conn)
                    except ValueError as exc:
                        # Connection setup is retryable configuration, never a
                        # permanent scientific rejection or a DSN log message.
                        raise RuntimeError("SOURCE_CONNECTION_SETUP_FAILED") from exc
                proof = None
                queries += 1
                if anchor is None:
                    proof = source.read_anchor(conn, declaration)
                    self._save_anchor(claim, declaration, proof)
                elif task["kind"] == "source":
                    proof = source.read_source_chunk(conn, declaration, anchor, task["part_ordinal"], task["ordinals"])
                    self._save_leaf(claim, declaration, anchor, completed, task, proof, len(tasks))
                else:
                    proof = source.read_candle_chunk(conn, declaration, anchor, task["part_ordinal"], task["ordinals"][0])
                    self._save_leaf(claim, declaration, anchor, completed, task, proof, len(tasks))
        except LeaseLost:
            raise
        except source.SourceNotDue:
            self._release(claim, error="SOURCE_CLOCK_BEFORE_FROZEN_CUTOFF")
        except ValueError as exc:
            self._block(claim, str(exc), proof=getattr(exc, "proof", None) or proof,
                        diagnostic=getattr(exc, "diagnostic", None), task=task,
                        coverage_receipt=getattr(exc, "receipt", None))
        except Exception as exc:
            try:
                self._release(claim, error=type(exc).__name__)
            except LeaseLost:
                pass
            raise
        finally:
            if conn is not None:
                conn.close()
        # A runtime pass must not reread/decompress every earlier raw proof just
        # to emit a small progress metric. Full accounting is an explicit report.
        with self.connection.transaction():
            self.connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            summary = self.connection.execute("""SELECT r.status,r.executor_plan_id,
                (SELECT count(*) FROM research_no_horizon_acquisition_leaves l
                    WHERE l.request_id=r.request_id)
                +(SELECT count(*) FROM research_no_horizon_acquisition_anchors a
                    WHERE a.request_id=r.request_id) AS proof_count
                FROM research_no_horizon_acquisition_requests r WHERE r.request_id=%s""",
                (claim["request_id"],)).fetchone()
        return {"request_id": claim["request_id"], "status": summary["status"],
                "plan_id": summary["executor_plan_id"], "proof_count": summary["proof_count"],
                "queries_this_run": queries, **_AUTHORITY}

    def run_once(self, worker_id, *, source_connection_factory, lease_seconds=120, leaf_budget=4, request_id=None):
        child._bound(leaf_budget, "leaf_budget", MAX_LEAF_BUDGET)
        if not callable(source_connection_factory):
            raise ValueError("explicit source connection factory required")
        claim = self.claim_request(worker_id, lease_seconds=lease_seconds, request_id=request_id)
        return None if claim is None else self.process_claim(claim,
            source_connection_factory=source_connection_factory, leaf_budget=leaf_budget)

    def report(self, request_id, *, include_proofs=False):
        executor._idle(self.connection)
        if type(include_proofs) is not bool:
            raise ValueError("explicit boolean include_proofs required")
        with self.connection.transaction():
            self.connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            row = self._request(request_id)
            anchor = self.connection.execute("SELECT * FROM research_no_horizon_acquisition_anchors WHERE request_id=%s",
                                             (request_id,)).fetchone()
            leaves = self.connection.execute("""SELECT leaf_ordinal,task_json,
                octet_length(convert_to(proof_json,'UTF8')) AS proof_bytes"""
                + (",proof_json" if include_proofs else "") + """
                FROM research_no_horizon_acquisition_leaves WHERE request_id=%s ORDER BY leaf_ordinal""",
                (request_id,)).fetchall()
            receipt = self.connection.execute("SELECT payload_json FROM research_no_horizon_acquisition_receipts WHERE request_id=%s",
                                              (request_id,)).fetchone()
            sealed = json.loads(anchor["anchor_json"]) if anchor and anchor["anchor_json"] else None
            actual_bytes = sum(leaf["proof_bytes"] + len(leaf["task_json"].encode()) for leaf in leaves)
            if anchor:
                actual_bytes += len(anchor["proof_json"].encode()) + len((anchor["anchor_json"] or "").encode())
            if receipt:
                actual_bytes += len(receipt["payload_json"].encode())
            if (actual_bytes != row["retained_proof_bytes"]
                    or [leaf["leaf_ordinal"] for leaf in leaves] != list(range(len(leaves)))
                    or (row["status"] in _TERMINAL) != (receipt is not None)):
                raise ValueError("acquisition persisted proof accounting mismatch")
            if sealed and sealed["anchor_sha256"] != contracts.digest({key: value for key, value in sealed.items()
                                                                       if key != "anchor_sha256"}):
                raise ValueError("acquisition stored anchor integrity mismatch")
            terminal = json.loads(receipt["payload_json"]) if receipt else None
            if terminal is not None:
                if (terminal.get("version") != VERSION or terminal.get("request_id") != request_id
                        or terminal.get("status") != row["status"]
                        or terminal.get("executor_plan_id") != row["executor_plan_id"]
                        or terminal.get("anchor_sha256") != (sealed["anchor_sha256"] if sealed else None)
                        or terminal.get("declaration_sha256") != json.loads(row["identity_json"])["declaration_sha256"]
                        or terminal.get("receipt_sha256") != contracts.digest({key: value for key, value in terminal.items()
                                                                             if key != "receipt_sha256"})
                        or any(terminal.get(key) is not False for key in _AUTHORITY)):
                    raise ValueError("acquisition terminal receipt binding mismatch")
                if not include_proofs and terminal.get("rejected_proof") is not None:
                    rejected = terminal.pop("rejected_proof")
                    terminal["rejected_proof_summary"] = {
                        "proof_sha256": contracts.digest(rejected), "proof_bytes": len(contracts.canonical(rejected).encode()),
                        "raw_response_sha256": rejected.get("raw_response_sha256"),
                        "raw_response_bytes": rejected.get("raw_response_bytes"),
                        "full_proof_omitted_from_report": True}
                    # The retained receipt hash binds the full stored
                    # object; this compact projection has its own report hash.
                    terminal["receipt_projection"] = "RAW_PROOF_OMITTED"
                if not include_proofs and terminal.get("coverage_receipt") is not None:
                    coverage = terminal.pop("coverage_receipt")
                    terminal["coverage_receipt_summary"] = {
                        "receipt_sha256": coverage.get("receipt_sha256"),
                        "accepted_source_rows": coverage.get("accepted_source_rows"),
                        "declared_scope_count": coverage.get("declared_scope_count"),
                        "coverage_status_counts": coverage.get("coverage_status_counts"),
                        "source_decisions_complete": coverage.get("source_decisions_complete"),
                        "scopes": [{key: scope.get(key) for key in ("candidate_key", "base_direction",
                            "threshold_pct", "coverage_status", "source_counts", "blockers")}
                            for scope in coverage.get("scopes", [])],
                        "full_receipt_omitted_from_report": True}
                    terminal["receipt_projection"] = "LARGE_EVIDENCE_OMITTED"
            total = sum((len(part["manifest"]["source_entries"]) + sql_builder.SOURCE_PAGE_SIZE - 1)
                        // sql_builder.SOURCE_PAGE_SIZE + len(part["manifest"]["candle_pages"])
                        for part in sealed["parts"]) if sealed else None
            result = {key: row[key] for key in ("request_id", "request_key", "status", "fencing_token",
                      "retained_proof_bytes", "executor_plan_id", "last_error")}
            result.update(version=VERSION, identity=json.loads(row["identity_json"]),
                declaration=json.loads(row["declaration_json"]), not_before_utc=row["not_before_utc"].isoformat(),
                lease_until=row["lease_until"].isoformat() if row["lease_until"] else None,
                anchor_sha256=sealed["anchor_sha256"] if sealed else None,
                leaves_completed=len(leaves), total_leaves=total, proof_count=len(leaves) + int(anchor is not None),
                terminal_receipt=terminal, **_AUTHORITY)
            if include_proofs:
                result.update(anchor_proof=json.loads(anchor["proof_json"]) if anchor else None, anchor=sealed,
                    leaves=[{"leaf_ordinal": leaf["leaf_ordinal"], "task": json.loads(leaf["task_json"]),
                             "proof": json.loads(leaf["proof_json"])} for leaf in leaves])
            return {**result, "report_sha256": contracts.digest(result)}
