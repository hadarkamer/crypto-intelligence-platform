"""Import a reviewed runtime-local archive SQLite into isolated archive tables.

This module provides no endpoint, upload channel, credential discovery or LIVE
promotion. Delivery of the artifact to an authenticated runtime is a separate
explicit operation. Each event and its outcomes/metrics commit atomically;
rerunning verifies existing rows and inserts only missing identical rows.
"""
from __future__ import annotations

import argparse
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any, Mapping

from research_telegram_archive_backfill import BACKFILL_VERSION, WINDOWS, canonical, parent_policy
from research_telegram_archive_features import ENTRY_VERSION, FEATURE_VERSION, DIRECTION_VERSION, TIME_VERSION
from research_telegram_html_archive import _hash
from research_ordered_first_touch import METHOD_VERSION
from research_common_window_metrics import METHOD_VERSION as METRIC_VERSION

IMPORT_VERSION = "archive-runtime-local-reviewed-sqlite-v1"
MAX_BATCH_JSON_BYTES = 4_000_000
RUN_CONTRACT_ORDER = ("backfill_version", "prepared_stage_digest", "entry_policy_version", "feature_version",
    "direction_version", "time_version", "parent_policy_version", "source_scope", "threshold_bps",
    "window_minutes", "variants", "cache_sha256", "live_union_eligible", "phase", "formula_relevance")
TABLES = {
    "research_archive_reconstruction_runs": (("run_key", "text"), ("contract", "jsonb"), ("source_scope", "text")),
    "research_archive_reconstructed_events": (("run_key", "text"), ("event_key", "text"), ("source_time_utc", "timestamptz"),
        ("symbol", "text"), ("reconstruction_status", "text"), ("calculation_status", "text"), ("event_payload", "jsonb")),
    "research_archive_delayed_entry_outcomes": (("run_key", "text"), ("event_key", "text"), ("signal_variant", "text"),
        ("window_minutes", "integer"), ("threshold_bps", "integer"), ("outcome_id", "text"), ("status", "text"), ("outcome_payload", "jsonb")),
    "research_archive_common_window_metrics": (("run_key", "text"), ("event_key", "text"), ("signal_variant", "text"),
        ("window_minutes", "integer"), ("status", "text"), ("metrics_payload", "jsonb")),
    "research_archive_btc_parents": (("run_key", "text"), ("btc_parent_movement_id", "text"), ("parent_payload", "jsonb")),
}
KEYS = {
    "research_archive_reconstruction_runs": ("run_key",),
    "research_archive_reconstructed_events": ("run_key", "event_key"),
    "research_archive_delayed_entry_outcomes": ("run_key", "event_key", "signal_variant", "window_minutes", "threshold_bps"),
    "research_archive_common_window_metrics": ("run_key", "event_key", "signal_variant", "window_minutes"),
    "research_archive_btc_parents": ("run_key", "btc_parent_movement_id"),
}


def file_sha256(path: Path) -> str:
    hashed = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            hashed.update(block)
    return hashed.hexdigest()


def open_reviewed_artifact(path: Path, expected_sha256: str) -> sqlite3.Connection:
    path = path.expanduser().resolve(strict=True)
    if not path.is_file() or len(expected_sha256) != 64 or file_sha256(path) != expected_sha256:
        raise ValueError("Reviewed archive artifact SHA256 mismatch")
    for suffix in ("-wal", "-journal"):
        sidecar = Path(str(path) + suffix)
        if sidecar.exists() and sidecar.stat().st_size:
            raise ValueError("Archive writer must finish and checkpoint before runtime import")
    conn = sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        conn.close()
        raise ValueError("Archive SQLite integrity check failed")
    return conn


def validate_contract(contract: Mapping[str, Any], run_key: str) -> None:
    expected = {"backfill_version": BACKFILL_VERSION, "entry_policy_version": ENTRY_VERSION,
        "feature_version": FEATURE_VERSION, "direction_version": DIRECTION_VERSION, "time_version": TIME_VERSION,
        "parent_policy_version": parent_policy.POLICY_VERSION, "source_scope": "ARCHIVE_ONLY",
        "threshold_bps": list(range(25, 201, 25)), "window_minutes": list(WINDOWS),
        "variants": ["NORMAL", "INVERSE"], "live_union_eligible": False, "phase": "DISCOVERY",
        "formula_relevance": "NOT_EVALUATED"}
    # The retained v1 producer hashes insertion order but serializes canonical
    # sorted JSON. Reconstruct that exact versioned field order, never rehash the
    # parsed sorted dictionary and silently invent a replacement run identity.
    reconstructed = {key: contract.get(key) for key in RUN_CONTRACT_ORDER}
    if set(contract) != set(RUN_CONTRACT_ORDER) or _hash(reconstructed) != run_key or any(contract.get(key) != value for key, value in expected.items()):
        raise ValueError("Unknown, mutated or non-isolated archive reconstruction contract")
    if any(not isinstance(contract.get(key), str) or len(contract[key]) != 64 for key in ("prepared_stage_digest", "cache_sha256")):
        raise ValueError("Archive source/cache provenance digest is missing")


def _json_row(row: Mapping[str, Any], field: str) -> dict[str, Any]:
    value = json.loads(row[field])
    if not isinstance(value, dict):
        raise ValueError("Archive JSON payload must be an object")
    canonical(value)  # Reject NaN/Infinity before binding JSON to PostgreSQL.
    return value


def _one_value(row: Any, key: str) -> Any:
    return row[key] if isinstance(row, Mapping) else row[0]


def _event_record(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = _json_row(row, "event_json")
    if (payload.get("archive_event_key") != row["event_key"] or payload.get("symbol") != row["symbol"]
            or payload.get("source_message_time_utc") != row["source_time_utc"]
            or payload.get("reconstruction_status") != row["reconstruction_status"]
            or payload.get("source_scope") != "ARCHIVE_ONLY" or payload.get("record_mode") != "ARCHIVE"
            or payload.get("live_union_eligible") is not False or payload.get("entry_policy_version") != ENTRY_VERSION
            or payload.get("feature_version") != FEATURE_VERSION or payload.get("direction_mapping_version") != DIRECTION_VERSION
            or payload.get("time_policy_version") != TIME_VERSION
            or payload.get("statistical_phase") != "DISCOVERY"):
        raise ValueError("Archive event relational identity or isolation disagrees with payload")
    return {**{key: row[key] for key in ("run_key", "event_key", "source_time_utc", "symbol", "reconstruction_status", "calculation_status")},
            "event_payload": payload}


def _outcome_record(row: Mapping[str, Any], event: Mapping[str, Any]) -> dict[str, Any]:
    payload = _json_row(row, "outcome_json")
    variant, window, threshold = row["signal_variant"], row["window_minutes"], row["threshold_bps"]
    event_id = _hash(event["event_key"], variant)
    direction = event["event_payload"].get("analysis_direction")
    if variant == "INVERSE":
        direction = {"LONG": "SHORT", "SHORT": "LONG"}.get(direction)
    if (variant not in ("NORMAL", "INVERSE") or window not in WINDOWS or threshold not in range(25, 201, 25)
            or payload.get("event_id") != event_id or row["outcome_id"] != _hash(event_id, METHOD_VERSION, window, threshold)
            or payload.get("outcome_id") != row["outcome_id"] or payload.get("status") != row["status"]
            or row["status"] not in ("SUCCESS", "FAILURE", "OPEN", "UNRESOLVED", "DATA_MISSING")
            or payload.get("window_minutes") != window or payload.get("threshold_bps") != threshold
            or payload.get("method_version") != METHOD_VERSION or payload.get("outcome_method_version") != METHOD_VERSION
            or payload.get("source_scope") != "ARCHIVE_ONLY" or payload.get("record_mode") != "ARCHIVE"
            or payload.get("entry_policy_version") != ENTRY_VERSION or payload.get("signal_variant") != variant
            or payload.get("direction") != direction or payload.get("source_price_exchange") != "binance"
            or payload.get("source_price_market") != "spot"):
        raise ValueError("Archive v7 outcome identity/version/source mismatch")
    return {**{key: row[key] for key in ("run_key", "event_key", "signal_variant", "window_minutes", "threshold_bps", "outcome_id", "status")},
            "outcome_payload": payload}


def _metric_record(row: Mapping[str, Any], event: Mapping[str, Any]) -> dict[str, Any]:
    payload = _json_row(row, "metrics_json")
    variant, window = row["signal_variant"], row["window_minutes"]
    if (variant not in ("NORMAL", "INVERSE") or window not in WINDOWS
            or payload.get("event_id") != _hash(event["event_key"], variant)
            or payload.get("method_version") != METRIC_VERSION or payload.get("window_minutes") != window
            or payload.get("status") != row["status"] or payload.get("signal_variant") != variant
            or row["status"] not in ("READY", "OPEN", "DATA_MISSING")
            or payload.get("source_scope") != "ARCHIVE_ONLY" or payload.get("entry_policy_version") != ENTRY_VERSION):
        raise ValueError("Archive common-window identity/version/source mismatch")
    return {**{key: row[key] for key in ("run_key", "event_key", "signal_variant", "window_minutes", "status")},
            "metrics_payload": payload}


def _write_verified_batch(conn: Any, table: str, records: list[dict[str, Any]]) -> int:
    if table not in TABLES or not records:
        if table not in TABLES:
            raise ValueError("Only isolated archive destination tables are supported")
        return 0
    encoded = canonical(records)
    if len(encoded.encode()) > MAX_BATCH_JSON_BYTES:
        raise ValueError("Archive batch exceeds byte budget; use a smaller batch size")
    fields = TABLES[table]
    columns = ",".join(name for name, _ in fields)
    typed = ",".join(name + " " + kind for name, kind in fields)
    source = "jsonb_to_recordset(%s::jsonb) AS incoming(" + typed + ")"
    count = conn.execute(f"INSERT INTO {table}({columns}) SELECT {columns} FROM {source} ON CONFLICT DO NOTHING", (encoded,)).rowcount
    join = " AND ".join(f"existing.{name}=incoming.{name}" for name in KEYS[table])
    mismatch = " OR ".join(f"existing.{name} IS DISTINCT FROM incoming.{name}" for name, _ in fields)
    conflict = conn.execute(f"SELECT EXISTS(SELECT 1 FROM {source} LEFT JOIN {table} existing ON {join} WHERE existing.{KEYS[table][0]} IS NULL OR {mismatch}) AS conflict", (encoded,)).fetchone()
    if bool(conflict["conflict"] if isinstance(conflict, Mapping) else conflict[0]):
        raise ValueError("Existing archive row conflicts with reviewed artifact; prior source preserved")
    return count


def import_artifact(conn: Any, sqlite_path: Path, *, expected_sha256: str,
                    expected_run_key: str, batch_size: int = 8) -> dict[str, Any]:
    """Explicit connection supplied by authenticated runtime; archive tables only."""
    if type(batch_size) is not int or not 1 <= batch_size <= 50:
        raise ValueError("Archive event batch size must be 1..50")
    source = open_reviewed_artifact(sqlite_path, expected_sha256)
    inserted = {table: 0 for table in TABLES}
    reviewed_events = 0
    try:
        row = source.execute("SELECT contract_json FROM archive_reconstruction_runs WHERE run_key=?", (expected_run_key,)).fetchone()
        if row is None:
            raise ValueError("Reviewed run key not present in artifact")
        contract = json.loads(row["contract_json"])
        validate_contract(contract, expected_run_key)
        for table in ("archive_delayed_entry_outcomes", "archive_common_window_metrics"):
            orphan = source.execute(f"SELECT EXISTS(SELECT 1 FROM {table} c LEFT JOIN archive_reconstructed_events e ON e.run_key=c.run_key AND e.event_key=c.event_key WHERE c.run_key=? AND e.event_key IS NULL)", (expected_run_key,)).fetchone()[0]
            if orphan:
                raise ValueError("Archive contains orphan result rows without an original reconstructed event")
        missing = [name for name in TABLES if not _one_value(conn.execute("SELECT to_regclass(%s) IS NOT NULL AS present", (name,)).fetchone(), "present")]
        if missing:
            raise ValueError("Archive reconstruction migration 032 is not installed")
        inserted["research_archive_reconstruction_runs"] += _write_verified_batch(conn, "research_archive_reconstruction_runs",
            [{"run_key": expected_run_key, "contract": contract, "source_scope": "ARCHIVE_ONLY"}])
        conn.commit()
        parents = source.execute("SELECT * FROM archive_btc_parents WHERE run_key=? ORDER BY btc_parent_movement_id", (expected_run_key,))
        while batch := parents.fetchmany(250):
            records = []
            for parent in batch:
                payload = _json_row(parent, "parent_json")
                if payload.get("btc_parent_movement_id") != parent["btc_parent_movement_id"] or payload.get("episode_policy_version") != parent_policy.POLICY_VERSION:
                    raise ValueError("Archive parent identity or method mismatch")
                records.append({"run_key": expected_run_key, "btc_parent_movement_id": parent["btc_parent_movement_id"], "parent_payload": payload})
            inserted["research_archive_btc_parents"] += _write_verified_batch(conn, "research_archive_btc_parents", records)
            conn.commit()
        cursor = source.execute("SELECT * FROM archive_reconstructed_events WHERE run_key=? ORDER BY event_key", (expected_run_key,))
        while batch := cursor.fetchmany(batch_size):
            events = [_event_record(row) for row in batch]
            outcomes, metrics = [], []
            for event in events:
                key = (expected_run_key, event["event_key"])
                children = [_outcome_record(row, event) for row in source.execute("SELECT * FROM archive_delayed_entry_outcomes WHERE run_key=? AND event_key=?", key)]
                measured = [_metric_record(row, event) for row in source.execute("SELECT * FROM archive_common_window_metrics WHERE run_key=? AND event_key=?", key)]
                if event["calculation_status"] == "COMPLETE_64_LABELS" and (len(children) != 64 or len(measured) != 8):
                    raise ValueError("Archive event claims complete without all exact outcome/metric cells")
                outcomes.extend(children)
                metrics.extend(measured)
            # Same transaction: no event marked complete before its own labels.
            inserted["research_archive_reconstructed_events"] += _write_verified_batch(conn, "research_archive_reconstructed_events", events)
            inserted["research_archive_delayed_entry_outcomes"] += _write_verified_batch(conn, "research_archive_delayed_entry_outcomes", outcomes)
            inserted["research_archive_common_window_metrics"] += _write_verified_batch(conn, "research_archive_common_window_metrics", metrics)
            conn.commit()
            reviewed_events += len(events)
        return {"import_version": IMPORT_VERSION, "artifact_sha256": expected_sha256, "run_key": expected_run_key,
                "verified_events": reviewed_events, "inserted_by_table": inserted, "source_scope": "ARCHIVE_ONLY",
                "phase": "DISCOVERY", "live_events_inserted": 0, "live_formula_statuses_changed": 0,
                "live_union_eligible": False}
    except Exception:
        conn.rollback()
        raise
    finally:
        source.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sqlite-artifact", type=Path, required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--expected-run-key", required=True)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    with closing(open_reviewed_artifact(args.sqlite_artifact, args.expected_sha256)) as source:
        row = source.execute("SELECT contract_json FROM archive_reconstruction_runs WHERE run_key=?", (args.expected_run_key,)).fetchone()
        if row is None:
            raise ValueError("Reviewed archive run key missing")
        validate_contract(json.loads(row[0]), args.expected_run_key)
    if not args.apply:
        print(json.dumps({"validated_artifact": True, "production_rows_inserted": 0, "run_key": args.expected_run_key}))
        return
    from research_telegram_export_import import _database_url
    import psycopg
    database_url = _database_url()
    if not database_url:
        raise RuntimeError("Explicit runtime research database configuration is missing")
    with psycopg.connect(database_url, connect_timeout=5, options="-c statement_timeout=15000 -c lock_timeout=1000") as conn:
        report = import_artifact(conn, args.sqlite_artifact, expected_sha256=args.expected_sha256,
                                 expected_run_key=args.expected_run_key, batch_size=args.batch_size)
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
