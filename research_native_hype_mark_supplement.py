"""Bounded, explicit MARK remeasurement of delivered native HYPE Max Pain alerts.

Original event, decision price and BTC membership are retained verbatim. The
derived entry is the next full-minute Binance Futures MARK OPEN, under its own
version. Results live in separate tables and cannot qualify native Spot formulas.
No source event, original outcome, or BTC membership is modified.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os

import binance_futures_mark_price_path as provider
import research_telegram_archive_hype_supplement as archive
from research_telegram_archive_backfill import canonical

ADAPTER_VERSION = "native-hype-mark-derived-outcomes-v1"
ENTRY_VERSION = "native-hype-next-full-minute-mark-open-v1"
METRIC_VERSION = "native-hype-common-window-mark-1m-v1"
SOURCE_SCOPE = "DERIVED_NATIVE_HYPE_MARK"
PARENT_POLICY = "btc-parent-close-reversal-200bps-v1"
MAX_EVENTS = 128
TERMINAL_FIELDS = ("status", "first_touch_side", "terminal_reason", "success", "decision_time_utc",
    "time_to_decision_seconds", "reference_price", "favorable_barrier_price", "adverse_barrier_price",
    "favorable_touch_price", "adverse_touch_price", "max_favorable_price", "max_adverse_price",
    "mfe_pct", "mae_pct", "observed_through_utc", "path_samples")
SOURCE_FIELDS = ("event_id", "event_kind", "event_type", "alert_time_utc", "symbol",
    "direction", "source_side", "timeframe", "score", "current_price", "target_price",
    "initial_target_distance_pct", "event_fingerprint", "strategy_version",
    "code_version", "delivery_status", "engine_snapshot")


def _utc(value):
    return provider._utc(value)


def source_record(event):
    return {key: event.get(key) for key in SOURCE_FIELDS}


def source_digest(event):
    return hashlib.sha256(canonical(source_record(event)).encode()).hexdigest()


def _validate_event(event):
    if (type(event.get("event_id")) is not int or event["event_id"] <= 0
        or event.get("symbol") != "HYPE" or event.get("event_kind") != "ALERT"
        or event.get("event_type") != "MAX_PAIN_ALERT"
        or event.get("delivery_status") != "DELIVERED"
        or event.get("direction") not in ("LONG", "SHORT")):
        raise ValueError("Expected a delivered native HYPE MAX_PAIN_ALERT")
    for key in ("score", "current_price"):
        value = event.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError("Original score and reference price must be finite")
    if event["score"] < 65 or event["current_price"] <= 0:
        raise ValueError("This repair supports score >=65 and positive original reference only")
    if not isinstance(event.get("engine_snapshot"), dict):
        raise ValueError("Original archived source snapshot is required")
    fingerprint = str(event.get("event_fingerprint") or "").strip()
    if len(fingerprint) != 64 or any(ch not in "0123456789abcdef" for ch in fingerprint):
        raise ValueError("Original native event fingerprint is required")
    return _utc(event["alert_time_utc"]).replace(second=0, microsecond=0) + timedelta(minutes=1)


def derive_entry(event, membership, path_result):
    """Return the same isolated entry for a full-wave path of any length.

    Only the entry candle is selected here; the wave calculator must validate
    the rest of its own window, continuity and cutoff separately.
    """
    entry = _validate_event(event)
    entry_bars = [raw for raw in path_result.get("candles", [])
        if _utc(raw["open_time_utc"]) == entry]
    return derive_measurement(event, membership, {**path_result, "candles": entry_bars},
        observed_at=entry + timedelta(minutes=1))["event"]


def derive_measurement(event, membership, path_result, *, observed_at):
    """Pure isolated derivation; input rows and native source metadata stay intact."""
    entry = _validate_event(event)
    now = _utc(observed_at)
    if membership is not None:
        if (membership.get("event_id") != event["event_id"]
            or membership.get("episode_policy_version") != PARENT_POLICY
            or _utc(membership["decision_time_utc"]) != _utc(event["alert_time_utc"])):
            raise ValueError("BTC membership must belong to the original decision")
    if any(path_result.get(key) != value for key, value in archive.SOURCE.items()):
        raise ValueError("Derived HYPE path must retain exact official Binance Futures MARK source")
    cutoff = min(entry + timedelta(days=1), now.replace(second=0, microsecond=0))
    bars = {}
    for raw in path_result.get("candles", []):
        bar = archive._validated_bar(raw)
        opened = bar["open_time_utc"]
        if not entry <= opened < cutoff or bar["close_time_utc"] >= cutoff:
            raise ValueError("Derived path contains an unclosed or out-of-window candle")
        if opened in bars and bars[opened] != bar:
            raise ValueError("Conflicting duplicate MARK candle")
        bars[opened] = bar
    digest = source_digest(event)
    derived_id = hashlib.sha256(f"{ADAPTER_VERSION}|{event['event_id']}|{digest}".encode()).hexdigest()
    # Reuse the archive's pure candle/entry/v7 math. Its archive-only identity is
    # deliberately internal: every persisted record below has this native-derived contract.
    internal = {"symbol": "HYPE", "entry_policy_version": archive.ENTRY_VERSION,
        "price_source_contract": dict(archive.SOURCE), "entry_time_utc": entry,
        "analysis_direction": event["direction"], "archive_event_key": derived_id}
    calculated, labels, metrics = archive.calculate_event(internal, bars, observed_at=now)
    shared = {"adapter_version": ADAPTER_VERSION, "entry_policy_version": ENTRY_VERSION,
        "source_scope": SOURCE_SCOPE, "record_mode": "DERIVED",
        "source_event_id": event["event_id"], "event_id": event["event_id"],
        "derived_measurement_id": derived_id, "source_sha256": digest,
        "live_union_eligible": False, "formula_relevance": "NOT_EVALUATED",
        "btc_parent_movement_id": (membership or {}).get("btc_parent_movement_id"),
        "btc_membership_status": (membership or {}).get("membership_status", "BTC_DATA_MISSING")}
    output = {**shared, "original_event": source_record(event), "original_btc_membership": membership,
        "entry_time_utc": entry, "entry_price": calculated.get("entry_price"),
        "original_reference_price": event["current_price"], "observed_at_utc": now,
        "price_source_contract": dict(archive.SOURCE),
        "calculation_status": calculated["calculation_status"].replace("64_LABELS", "32_LABELS"),
        "entry_delay_seconds": (entry - _utc(event["alert_time_utc"])).total_seconds()}
    normal_labels = [{**label, **shared} for label in labels if label["signal_variant"] == "NORMAL"]
    normal_metrics = [{**metric, **shared, "method_version": METRIC_VERSION}
        for metric in metrics if metric["signal_variant"] == "NORMAL"]
    output["window_coverage"] = {str(metric["window_minutes"]): {
        "path_samples": metric["path_samples"],
        "observed_prefix_complete": metric["observed_prefix_complete"], "status": metric["status"]}
        for metric in normal_metrics}
    return {"event": output, "outcomes": normal_labels, "metrics": normal_metrics}


def _load(conn, event_ids):
    columns = ",".join("e." + field for field in SOURCE_FIELDS)
    return conn.execute(f"""SELECT {columns}, to_jsonb(m) AS btc_membership
        FROM research_events e LEFT JOIN research_event_btc_movements m
          ON m.event_id=e.event_id AND m.episode_policy_version=%s
        WHERE e.event_id=ANY(%s) ORDER BY e.event_id""", (PARENT_POLICY, event_ids)).fetchall()


def write_measurement(conn, measurement):
    """Atomically persist an idempotent refresh without source changes or regression."""
    event = measurement["event"]
    event_id = event["event_id"]
    with conn.transaction():
        # Serializes two invocations for this original event, including first insert.
        conn.execute("SELECT pg_advisory_xact_lock(1942808,%s)", (event_id,))
        current = _load(conn, [event_id])
        if not current or source_digest(current[0]) != event["source_sha256"]:
            raise ValueError("Native source changed while its MARK path was fetched")
        if canonical(current[0].get("btc_membership")) != canonical(event["original_btc_membership"]):
            raise ValueError("BTC membership changed while its MARK path was fetched; retry")
        old = conn.execute("""SELECT source_sha256, observed_at_utc, measurement_payload
            FROM research_native_hype_mark_measurements WHERE event_id=%s AND adapter_version=%s""",
            (event_id, ADAPTER_VERSION)).fetchone()
        if old:
            if old["source_sha256"] != event["source_sha256"]:
                raise ValueError("Derived source identity is immutable")
            if _utc(old["observed_at_utc"]) > event["observed_at_utc"]:
                return False
            previous_entry = old["measurement_payload"].get("entry_price")
            if previous_entry is not None and previous_entry != event["entry_price"]:
                raise ValueError("A refresh cannot change or erase its frozen MARK entry")
            old_parent = old["measurement_payload"].get("btc_parent_movement_id")
            if old_parent is not None and old_parent != event["btc_parent_movement_id"]:
                raise ValueError("A refresh cannot reassign its original BTC wave")
            for window, previous in old["measurement_payload"].get("window_coverage", {}).items():
                incoming = event["window_coverage"].get(window, {})
                if previous.get("observed_prefix_complete") and (
                    not incoming.get("observed_prefix_complete")
                    or incoming.get("path_samples", 0) < previous["path_samples"]):
                    return False
            incoming_labels = {(row["window_minutes"], row["threshold_bps"]): row
                for row in measurement["outcomes"]}
            for previous in conn.execute("""SELECT window_minutes,threshold_bps,outcome_payload
                FROM research_native_hype_mark_outcomes WHERE event_id=%s AND adapter_version=%s
                AND status IN ('SUCCESS','FAILURE','UNRESOLVED')""", (event_id, ADAPTER_VERSION)).fetchall():
                candidate = incoming_labels.get((previous["window_minutes"], previous["threshold_bps"]), {})
                before = {key: previous["outcome_payload"].get(key) for key in TERMINAL_FIELDS}
                after = {key: candidate.get(key) for key in TERMINAL_FIELDS}
                if canonical(before) != canonical(after):
                    raise ValueError("Historical MARK candles conflict with frozen terminal evidence")
            incoming_metrics = {row["window_minutes"]: row for row in measurement["metrics"]}
            for previous in conn.execute("""SELECT window_minutes,metrics_payload
                FROM research_native_hype_mark_metrics WHERE event_id=%s AND adapter_version=%s
                AND status='READY'""", (event_id, ADAPTER_VERSION)).fetchall():
                candidate = incoming_metrics.get(previous["window_minutes"], {})
                if previous["metrics_payload"].get("path_sha256") != candidate.get("path_sha256"):
                    raise ValueError("Historical MARK candles conflict with frozen complete-window evidence")
        conn.execute("""INSERT INTO research_native_hype_mark_measurements
            (event_id,adapter_version,source_sha256,source_event,btc_membership,observed_at_utc,measurement_payload)
            VALUES (%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s::jsonb)
            ON CONFLICT(event_id,adapter_version) DO UPDATE SET
                btc_membership=EXCLUDED.btc_membership, observed_at_utc=EXCLUDED.observed_at_utc,
                measurement_payload=EXCLUDED.measurement_payload, updated_at_utc=NOW()""",
            (event_id, ADAPTER_VERSION, event["source_sha256"], canonical(event["original_event"]),
             canonical(event["original_btc_membership"]), event["observed_at_utc"], canonical(event)))
        for label in measurement["outcomes"]:
            conn.execute("""INSERT INTO research_native_hype_mark_outcomes
                (event_id,adapter_version,window_minutes,threshold_bps,status,outcome_payload)
                VALUES (%s,%s,%s,%s,%s,%s::jsonb) ON CONFLICT(event_id,adapter_version,window_minutes,threshold_bps)
                DO UPDATE SET status=EXCLUDED.status,outcome_payload=EXCLUDED.outcome_payload""",
                (event_id, ADAPTER_VERSION, label["window_minutes"], label["threshold_bps"], label["status"], canonical(label)))
        for metric in measurement["metrics"]:
            conn.execute("""INSERT INTO research_native_hype_mark_metrics
                (event_id,adapter_version,window_minutes,status,metrics_payload)
                VALUES (%s,%s,%s,%s,%s::jsonb) ON CONFLICT(event_id,adapter_version,window_minutes)
                DO UPDATE SET status=EXCLUDED.status,metrics_payload=EXCLUDED.metrics_payload""",
                (event_id, ADAPTER_VERSION, metric["window_minutes"], metric["status"], canonical(metric)))
    return True


def run(*, database_url, event_ids, observed_at=None, fetch_candles=provider.fetch_closed_candles):
    """Fetch at most 128 exact IDs, one <=24h public MARK request per eligible event."""
    import psycopg
    from psycopg.rows import dict_row
    if (not isinstance(event_ids, (list, tuple)) or not 1 <= len(event_ids) <= MAX_EVENTS
        or any(type(value) is not int or value <= 0 for value in event_ids)
        or len(set(event_ids)) != len(event_ids)):
        raise ValueError("Supply 1..128 distinct positive native event IDs")
    now = _utc(observed_at or datetime.now(timezone.utc))
    if now > datetime.now(timezone.utc):
        raise ValueError("Observation cutoff cannot be in the future")
    result = {"adapter_version": ADAPTER_VERSION, "observed_at_utc": now.isoformat(),
        "requested": len(event_ids), "written": 0, "labels_written": 0, "metrics_written": 0,
        "unchanged": 0, "errors": [], "measurements": [],
        "source_events_modified": 0, "native_outcomes_modified": 0}
    with psycopg.connect(database_url, autocommit=True, row_factory=dict_row,
            connect_timeout=5, options="-c statement_timeout=15000 -c lock_timeout=1000") as conn:
        events = _load(conn, list(event_ids))
        by_id = {row["event_id"]: row for row in events}
        for event_id in event_ids:
            try:
                if event_id not in by_id:
                    raise ValueError("Native event ID not found")
                row = by_id[event_id]
                start = _validate_event(row)
                cutoff = min(start + timedelta(days=1), now.replace(second=0, microsecond=0))
                path = fetch_candles("HYPE", start, cutoff) if cutoff > start else {**archive.SOURCE, "candles": []}
                measurement = derive_measurement(row, row.get("btc_membership"), path, observed_at=now)
                if write_measurement(conn, measurement):
                    result["written"] += 1
                    result["labels_written"] += len(measurement["outcomes"])
                    result["metrics_written"] += len(measurement["metrics"])
                else:
                    result["unchanged"] += 1
                # Read committed persisted state: an older or incomplete retry
                # may intentionally retain a newer, more complete measurement.
                saved = conn.execute("""SELECT measurement_payload FROM research_native_hype_mark_measurements
                    WHERE event_id=%s AND adapter_version=%s""", (event_id, ADAPTER_VERSION)).fetchone()
                payload = saved["measurement_payload"]
                ready = [int(window) for window, item in payload.get("window_coverage", {}).items()
                    if item.get("status") == "READY"]
                coverage = payload.get("window_coverage", {})
                prefix_complete = (set(coverage) == {str(window) for window in archive.WINDOWS}
                    and all(item.get("observed_prefix_complete") is True
                        and item.get("status") in ("READY", "OPEN") for item in coverage.values()))
                result["measurements"].append({"event_id": event_id,
                    "calculation_status": payload["calculation_status"],
                    "observed_at_utc": payload["observed_at_utc"], "ready_windows": sorted(ready),
                    "complete": sorted(ready) == list(archive.WINDOWS),
                    "current_prefix_complete": prefix_complete,
                    "source_scope": SOURCE_SCOPE, "live_union_eligible": False})
            except Exception as exc:
                result["errors"].append({"event_id": event_id, "error": str(exc)[:300]})
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-ids", required=True, help="Comma-separated original native IDs, at most 128")
    parser.add_argument("--observed-at", help="Explicit timezone-aware observation cutoff")
    args = parser.parse_args()
    url = os.environ.get("RESEARCH_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        raise SystemExit("RESEARCH_DATABASE_URL or DATABASE_URL is required")
    print(json.dumps(run(database_url=url, event_ids=[int(value) for value in args.event_ids.split(",")],
        observed_at=args.observed_at), ensure_ascii=False))
