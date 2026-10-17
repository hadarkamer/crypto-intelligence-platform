"""Local SQLite research jobs with immutable inputs, leases and checkpoints.

No production database, worker, notification or trading integration. A new
cutoff/input creates a new job and receipt; only the same frozen job resumes.
SQLite ownership is local process coordination, not external source attestation.
"""
from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any, Mapping

import research_no_horizon_contract as contracts
import research_no_horizon_first_touch as first_touch
import research_no_horizon_gate as gate
import research_no_horizon_replay as replay

VERSION = "no-horizon-local-sqlite-store-v1"
RECEIPT_VERSION = "no-horizon-local-immutable-receipt-v1"
_CLOCK = "((julianday('now') - 2440587.5) * 86400.0)"
_AUTHORITY = {"runtime_authorized": False, "telegram_authorized": False,
              "trading_authorized": False}


class LeaseLost(ValueError):
    """The claim expired or a new owner fenced this worker out."""


def _bound(value: Any, name: str, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValueError(f"{name} must be an integer from 1 to {maximum}")
    return value


def _name(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(name + " must be an explicit nonempty string")
    return value


def _frozen_json(value: Any) -> str:
    """Reject oversized/non-JSON input before starting a database transaction."""
    pieces, size = [], 0
    encoder = json.JSONEncoder(sort_keys=True, separators=(",", ":"),
                               ensure_ascii=False, allow_nan=False)
    for piece in encoder.iterencode(value):
        size += len(piece.encode("utf-8"))
        if size > replay.MAX_INPUT_BYTES:
            raise ValueError("snapshot exceeds the bounded input size")
        pieces.append(piece)
    return "".join(pieces)


def _versions() -> dict[str, Any]:
    modules = (contracts, first_touch, gate, replay)
    sources = [Path(module.__file__) for module in modules] + [Path(__file__)]
    return {"store_version": VERSION, "contract_version": contracts.VERSION,
        "checkpoint_version": first_touch.VERSION, "gate_version": gate.VERSION,
        "replay_receipt_version": replay.RECEIPT_VERSION,
        "implementation_sha256": contracts.digest({
            path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in sources})}


def _policy(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    p = snapshot.get("gate_policy")
    if p is None:
        return gate.make_policy()
    try:
        expected = gate.make_policy(**{key: p[key] for key in (
            "policy_version", "minimum_waves", "hit_rate_min_pct", "wilson_lower_min_pct")})
        if contracts.canonical(p) != contracts.canonical(expected):
            raise ValueError("unsupported or altered gate policy")
        return expected
    except (KeyError, TypeError) as exc:
        raise ValueError("incomplete gate policy") from exc


class LocalResearchStore:
    """One connection per thread/process; use a local file, not a network share."""

    def __init__(self, path: str | Path):
        self.connection = sqlite3.connect(str(path), timeout=15, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        # Inspect an existing version before journal changes, DDL or bootstrap
        # writes. A newer/incompatible local database belongs to its own reader.
        version_table = self.connection.execute("""SELECT 1 FROM sqlite_master
            WHERE type='table' AND name='local_store_version'""").fetchone()
        if version_table:
            existing_version = self.connection.execute(
                "SELECT version FROM local_store_version WHERE singleton=1").fetchone()
            if existing_version is not None and existing_version[0] != VERSION:
                self.close()
                raise ValueError("unsupported local store schema version")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.versions = _versions()
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS local_store_version (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1), version TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS local_snapshots (
                snapshot_sha256 TEXT PRIMARY KEY, payload_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL, source_candles INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS local_candles (
                snapshot_sha256 TEXT NOT NULL REFERENCES local_snapshots,
                open_time_utc TEXT NOT NULL, candle_json TEXT NOT NULL,
                PRIMARY KEY(snapshot_sha256, open_time_utc));
            CREATE TABLE IF NOT EXISTS local_jobs (
                job_id TEXT PRIMARY KEY,
                snapshot_sha256 TEXT NOT NULL REFERENCES local_snapshots,
                job_key TEXT UNIQUE, identity_json TEXT NOT NULL,
                total_entries INTEGER NOT NULL, next_ordinal INTEGER NOT NULL DEFAULT 0,
                candle_evaluations INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'PENDING'
                    CHECK(status IN ('PENDING','RUNNING','COMPLETE','BLOCKED')),
                worker_id TEXT, fencing_token INTEGER NOT NULL DEFAULT 0,
                lease_until REAL);
            CREATE TABLE IF NOT EXISTS local_entries (
                job_id TEXT NOT NULL REFERENCES local_jobs, ordinal INTEGER NOT NULL,
                entry_id TEXT NOT NULL, metadata_json TEXT NOT NULL,
                PRIMARY KEY(job_id,ordinal), UNIQUE(job_id,entry_id));
            CREATE TABLE IF NOT EXISTS local_progress (
                job_id TEXT NOT NULL, ordinal INTEGER NOT NULL, checkpoint_json TEXT NOT NULL,
                processed INTEGER NOT NULL DEFAULT 0 CHECK(processed IN (0,1)),
                PRIMARY KEY(job_id,ordinal),
                FOREIGN KEY(job_id,ordinal) REFERENCES local_entries);
            CREATE TABLE IF NOT EXISTS local_receipts (
                job_id TEXT PRIMARY KEY REFERENCES local_jobs,
                receipt_sha256 TEXT NOT NULL UNIQUE, payload_json TEXT NOT NULL);
            CREATE TRIGGER IF NOT EXISTS local_jobs_identity_immutable
            BEFORE UPDATE OF job_id,snapshot_sha256,job_key,identity_json,total_entries ON local_jobs
            BEGIN SELECT RAISE(ABORT,'immutable job identity'); END;
        """)
        for table in ("local_snapshots", "local_candles", "local_entries", "local_receipts"):
            for action in ("UPDATE", "DELETE"):
                self.connection.execute(f"""CREATE TRIGGER IF NOT EXISTS {table}_{action.lower()}_immutable
                    BEFORE {action} ON {table}
                    BEGIN SELECT RAISE(ABORT,'immutable research input or receipt'); END""")
        self.connection.execute("INSERT OR IGNORE INTO local_store_version VALUES(1,?)", (VERSION,))
        if self.connection.execute("SELECT version FROM local_store_version WHERE singleton=1").fetchone()[0] != VERSION:
            self.close()
            raise ValueError("unsupported local store schema version")

    def close(self) -> None:
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    @contextmanager
    def _write(self):
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.connection.execute("COMMIT")
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise

    def submit_snapshot(self, snapshot: Mapping[str, Any], *, job_key: str | None = None) -> str:
        """Idempotent exact-input submit; an existing named key cannot be repointed."""
        if job_key is not None:
            _name(job_key, "job_key")
        if not isinstance(snapshot, Mapping):
            raise ValueError("snapshot must be an object")
        # Cardinality checks precede serializing potentially very large arrays.
        for name, maximum in (("candles", replay.MAX_SOURCE_CANDLES),
                              ("opportunities", replay.MAX_OPPORTUNITIES)):
            if not isinstance(snapshot.get(name), list) or len(snapshot[name]) > maximum:
                raise ValueError("bounded " + name + " array required")
        payload = _frozen_json(dict(snapshot))
        frozen = json.loads(payload)
        cutoff, bars, times, normalized = replay.prepare_snapshot(frozen)
        # Use the engine's exact validator before persistence. It deliberately
        # does not inspect OHLC whose full-minute availability is after cutoff.
        for bar in bars:
            first_touch._bar(bar, cutoff=cutoff)
        policy = _policy(frozen)
        route = frozen["source_route"]
        if (not isinstance(route, dict) or set(route) != set(contracts.SOURCE_FIELDS)
                or type(route["interval_seconds"]) is not int or route["interval_seconds"] != 60):
            raise ValueError("complete explicit one-minute source route required")
        for key in contracts.SOURCE_FIELDS[:-1]:
            _name(route[key], "source_route." + key)
        snapshot_sha = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        identity = {**self.versions, "snapshot_sha256": snapshot_sha,
                    "effective_gate_policy": policy}
        job_id = contracts.digest(identity)
        metadata = {key: value for key, value in frozen.items() if key not in ("candles", "opportunities")}
        with self._write():
            if job_key is not None:
                named = self.connection.execute("SELECT job_id FROM local_jobs WHERE job_key=?", (job_key,)).fetchone()
                if named and named[0] != job_id:
                    raise ValueError("job_key already binds a different immutable input or policy")
            existing = self.connection.execute("SELECT job_key,identity_json FROM local_jobs WHERE job_id=?", (job_id,)).fetchone()
            if existing:
                if existing[1] != contracts.canonical(identity):
                    raise ValueError("job identity collision")
                if job_key is not None and existing[0] != job_key:
                    raise ValueError("identical job already exists with a different job_key")
                return job_id
            existing_snapshot = self.connection.execute(
                "SELECT payload_json FROM local_snapshots WHERE snapshot_sha256=?", (snapshot_sha,)).fetchone()
            if existing_snapshot and existing_snapshot[0] != payload:
                raise ValueError("snapshot identity collision")
            if not existing_snapshot:
                self.connection.execute("INSERT INTO local_snapshots VALUES(?,?,?,?)",
                    (snapshot_sha, payload, contracts.canonical(metadata), len(bars)))
                self.connection.executemany("INSERT INTO local_candles VALUES(?,?,?)", (
                    (snapshot_sha, when.isoformat(timespec="microseconds"), contracts.canonical(bar))
                    for when, bar in zip(times, bars)))
            self.connection.execute("""INSERT INTO local_jobs
                (job_id,snapshot_sha256,job_key,identity_json,total_entries) VALUES(?,?,?,?,?)""",
                (job_id, snapshot_sha, job_key, contracts.canonical(identity), len(normalized)))
            self.connection.executemany("INSERT INTO local_entries VALUES(?,?,?,?)", (
                (job_id, ordinal, row["contract"]["entry_id"], contracts.canonical(row))
                for ordinal, row in enumerate(normalized)))
            self.connection.executemany("INSERT INTO local_progress VALUES(?,?,?,0)", (
                (job_id, ordinal, contracts.canonical(first_touch.initialize(row["contract"], cutoff_utc=cutoff)))
                for ordinal, row in enumerate(normalized)))
        return job_id

    def get_job(self, job_id: str) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM local_jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            raise KeyError("unknown local job")
        result = dict(row)
        result["identity"] = json.loads(result.pop("identity_json"))
        return {**result, **_AUTHORITY}

    def claim_job(self, worker_id: str, *, lease_seconds: int = 60,
                  job_id: str | None = None) -> dict[str, Any] | None:
        """Atomically claim pending work or reclaim an expired lease with a fence."""
        _name(worker_id, "worker_id")
        _bound(lease_seconds, "lease_seconds", 3600)
        with self._write():
            if job_id is not None:
                explicit = self.connection.execute(
                    "SELECT identity_json FROM local_jobs WHERE job_id=?", (job_id,)).fetchone()
                if explicit and not self._compatible(json.loads(explicit[0])):
                    raise ValueError("job requires its frozen engine implementation; submit a new revision")
            query = f"""SELECT job_id FROM local_jobs WHERE
                (status='PENDING' OR (status='RUNNING' AND lease_until<={_CLOCK}))"""
            args = []
            # Skip retained jobs from another frozen implementation rather than
            # repeatedly leasing the oldest incompatible job and starving new work.
            for key, value in self.versions.items():
                query += " AND json_extract(identity_json,?)=?"
                args.extend(("$." + key, value))
            if job_id is not None:
                query += " AND job_id=?"
                args.append(job_id)
            row = self.connection.execute(query + " ORDER BY rowid LIMIT 1", args).fetchone()
            if row is None:
                return None
            self.connection.execute(f"""UPDATE local_jobs SET status='RUNNING',worker_id=?,
                fencing_token=fencing_token+1,lease_until={_CLOCK}+? WHERE job_id=?""",
                (worker_id, lease_seconds, row[0]))
            result = self.get_job(row[0])
        return {key: result[key] for key in ("job_id", "worker_id", "fencing_token", "lease_until")}

    def _compatible(self, identity: Mapping[str, Any]) -> bool:
        return all(identity.get(key) == value for key, value in self.versions.items())

    def _check_claim(self, claim: Mapping[str, Any]) -> dict[str, Any]:
        if type(claim.get("fencing_token")) is not int:
            raise LeaseLost("invalid fencing token")
        row = self.connection.execute(f"""SELECT * FROM local_jobs WHERE job_id=?
            AND status='RUNNING' AND worker_id=? AND fencing_token=? AND lease_until>{_CLOCK}""",
            (claim.get("job_id"), claim.get("worker_id"), claim["fencing_token"])).fetchone()
        if row is None:
            raise LeaseLost("claim is expired, released or fenced by a new owner")
        job = dict(row)
        identity = json.loads(job["identity_json"])
        if not self._compatible(identity):
            raise ValueError("job requires its frozen engine implementation; submit a new revision")
        return job

    def process_claim(self, claim: Mapping[str, Any], *, candle_budget: int = 1024,
                      entry_budget: int = 128, batch_size: int = 128) -> dict[str, Any]:
        """Compute a bounded suffix, then atomically checkpoint if the lease holds.

        Work is outside the write transaction. A crash loses only this batch;
        token+cursor comparisons prevent stale or duplicate batch application.
        Source exhaustion is final for this immutable input and blocks its gate.
        """
        _bound(candle_budget, "candle_budget", 100000000)
        _bound(entry_budget, "entry_budget", replay.MAX_OPPORTUNITIES)
        _bound(batch_size, "batch_size", 100000)
        job = self._check_claim(claim)
        snapshot = self.connection.execute(
            "SELECT metadata_json,source_candles FROM local_snapshots WHERE snapshot_sha256=?",
            (job["snapshot_sha256"],)).fetchone()
        metadata = json.loads(snapshot[0])
        cutoff = contracts.utc(metadata["cutoff_utc"])
        entries = self.connection.execute("""SELECT e.ordinal,e.metadata_json,p.checkpoint_json
            FROM local_entries e JOIN local_progress p USING(job_id,ordinal)
            WHERE e.job_id=? AND e.ordinal>=? ORDER BY e.ordinal LIMIT ?""",
            (job["job_id"], job["next_ordinal"], entry_budget)).fetchall()
        remaining, cursor, updates = candle_budget, job["next_ordinal"], []
        for entry in entries:
            if not remaining:
                break
            state = first_touch.validate_state(json.loads(entry["checkpoint_json"]))
            finished = False
            while remaining:
                limit = min(batch_size, remaining)
                rows = self.connection.execute("""SELECT candle_json FROM local_candles
                    WHERE snapshot_sha256=? AND open_time_utc>=?
                    ORDER BY open_time_utc LIMIT ?""", (job["snapshot_sha256"],
                        contracts.utc(state["next_open_utc"]).isoformat(timespec="microseconds"), limit)).fetchall()
                before = state["processed_candles"]
                state = first_touch.advance(state, (json.loads(row[0]) for row in rows),
                    cutoff_utc=cutoff, max_candles=limit)
                consumed = state["processed_candles"] - before
                remaining -= consumed
                if state["progress"] != "MORE_DATA_REQUIRED" or not consumed:
                    finished = True
                    break
            updates.append((contracts.canonical(state), int(finished), job["job_id"], entry["ordinal"]))
            if not finished:
                break
            cursor = entry["ordinal"] + 1
        with self._write():
            current = self._check_claim(claim)
            if (current["next_ordinal"], current["candle_evaluations"]) != (
                    job["next_ordinal"], job["candle_evaluations"]):
                raise LeaseLost("the claimed checkpoint changed during computation")
            self.connection.executemany("""UPDATE local_progress SET checkpoint_json=?,processed=?
                WHERE job_id=? AND ordinal=?""", updates)
            evaluations = job["candle_evaluations"] + candle_budget - remaining
            status = "PENDING"
            if cursor == job["total_entries"]:
                receipt = self._make_receipt(job, metadata, snapshot[1], evaluations)
                status = "COMPLETE" if receipt["computation_complete"] else "BLOCKED"
                self.connection.execute("INSERT INTO local_receipts VALUES(?,?,?)",
                    (job["job_id"], receipt["receipt_sha256"], contracts.canonical(receipt)))
            # Final ownership/expiry check also covers bounded receipt assembly.
            self._check_claim(claim)
            self.connection.execute("""UPDATE local_jobs SET next_ordinal=?,candle_evaluations=?,
                status=?,worker_id=NULL,lease_until=NULL WHERE job_id=?""",
                (cursor, evaluations, status, job["job_id"]))
        return self.get_job(job["job_id"])

    def _make_receipt(self, job: Mapping[str, Any], metadata: dict[str, Any],
                      source_candles: int, evaluations: int) -> dict[str, Any]:
        rows = self.connection.execute("""SELECT e.metadata_json,p.checkpoint_json,p.processed
            FROM local_entries e JOIN local_progress p USING(job_id,ordinal)
            WHERE e.job_id=? ORDER BY e.ordinal""", (job["job_id"],)).fetchall()
        if len(rows) != job["total_entries"] or any(row["processed"] != 1 for row in rows):
            raise ValueError("cohort must be completely processed before evaluating the gate")
        normalized, outcomes, incomplete = [], [], []
        for row in rows:
            item = json.loads(row["metadata_json"])
            state = first_touch.validate_state(json.loads(row["checkpoint_json"]))
            item["outcome"] = state
            normalized.append(item)
            entry_id = item["contract"]["entry_id"]
            outcomes.append({"entry_id": entry_id, "outcome": state})
            if not (state["status"] in first_touch.TERMINAL or
                    state["status"] == "OPEN" and state["coverage_complete_through_cutoff"] is True):
                incomplete.append(entry_id)
        identity = json.loads(job["identity_json"])
        aggregate = gate.evaluate_gate(normalized, as_of_utc=metadata["cutoff_utc"],
            source_coverage_complete=metadata["source_coverage_complete"] and not incomplete,
            policy=identity["effective_gate_policy"])
        counts = Counter(item["outcome"]["status"] for item in outcomes)
        result = {"receipt_version": RECEIPT_VERSION, "job_id": job["job_id"],
            "job_identity": identity, "snapshot_sha256": job["snapshot_sha256"],
            "dataset_id": metadata["dataset_id"], "cohort_id": metadata["cohort_id"],
            "source_route": metadata["source_route"], "source_receipt": metadata.get("source_receipt"),
            "source_provenance_verified_by_this_tool": False,
            "cutoff_utc": contracts.utc(metadata["cutoff_utc"]).isoformat(),
            "opportunities": len(normalized), "source_candles": source_candles,
            "candle_evaluations": evaluations, "cohort_processing_complete": True,
            "incomplete_entry_ids": incomplete, "computation_complete": not incomplete,
            "status_counts": {key: counts[key] for key in first_touch.STATUSES},
            "outcomes": outcomes, "gate": aggregate, **_AUTHORITY}
        return {**result, "receipt_sha256": contracts.digest(result)}

    def get_receipt(self, job_id: str) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT payload_json FROM local_receipts WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            self.get_job(job_id)  # Distinguish not-yet-processed from unknown job.
            return None
        result = json.loads(row[0])
        expected = contracts.digest({key: value for key, value in result.items() if key != "receipt_sha256"})
        if result["receipt_sha256"] != expected:
            raise ValueError("stored receipt integrity mismatch")
        return result

    def run_once(self, worker_id: str, *, job_id: str | None = None,
                 lease_seconds: int = 60, candle_budget: int = 1024,
                 entry_budget: int = 128, batch_size: int = 128) -> dict[str, Any] | None:
        _bound(candle_budget, "candle_budget", 100000000)
        _bound(entry_budget, "entry_budget", replay.MAX_OPPORTUNITIES)
        _bound(batch_size, "batch_size", 100000)
        claim = self.claim_job(worker_id, lease_seconds=lease_seconds, job_id=job_id)
        return None if claim is None else self.process_claim(claim, candle_budget=candle_budget,
            entry_budget=entry_budget, batch_size=batch_size)
