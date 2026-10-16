"""Local, read-only explanations of existing BTC-wave report records.

This module deliberately imports no research engine, database or price client.
It does not validate source provenance or recalculate research outcomes.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sys
from uuid import UUID

DIAGNOSTIC_VERSION = "btc-wave-missing-diagnostics-v1"
METHOD_VERSION = "btc-wave-endpoint-closed-1m-v1"
SPOT = "CANONICAL_SPOT_IMMUTABLE_ALERT"
MARK = "DERIVED_NATIVE_HYPE_MARK"
PERP = "DERIVED_NATIVE_HYPE_PERP"
MIXED = {"SPOT_WITH_DERIVED_HYPE_MARK_V1": MARK,
         "SPOT_WITH_DERIVED_HYPE_PERP_V1": PERP}
MINUTE = timedelta(minutes=1)
MILLISECOND = timedelta(milliseconds=1)
LIMITATION = (
    "Diagnostic of supplied frozen records only; source completeness, authentic report "
    "hashes and current prices are not verified. A positive time window does not prove "
    "a provider gap. Original selection, statuses, metrics and comparison rows remain "
    "unchanged. Grouping is for explanation only, never a combined research cohort."
)


def _stamp(value, field):
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a timezone-aware ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be a timezone-aware ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} requires an explicit timezone")
    return parsed.astimezone(timezone.utc)


def _text(row, field):
    value = row.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a nonempty string")
    return value


def _count(first, cutoff):
    return max(0, int((cutoff + MILLISECOND - first) // MINUTE))


def _inspect(row, index):
    if not isinstance(row, dict):
        raise ValueError("record must be an object")
    if row.get("method_version") != METHOD_VERSION:
        raise ValueError("unsupported or missing method_version")
    event_id = row.get("source_event_id")
    if type(event_id) is not int or event_id <= 0:
        raise ValueError("source_event_id must be a positive JSON integer")
    parent = _text(row, "btc_parent_movement_id")
    source, coin, symbol = (_text(row, key) for key in ("source_scope", "coin_scope", "symbol"))
    direction = row.get("direction")
    if direction not in {"LONG", "SHORT"}:
        raise ValueError("direction must be LONG or SHORT")
    if row.get("status") not in {"READY", "OPEN", "DATA_MISSING"}:
        raise ValueError("unsupported or missing status")
    reasons = row.get("missing_reasons")
    if not isinstance(reasons, list) or any(not isinstance(reason, str) for reason in reasons):
        raise ValueError("missing_reasons must be an array of strings")
    if source in MIXED:
        component = row.get("component_source_scope")
        expected_component = MIXED[source] if symbol == "HYPE" else SPOT
        if component != expected_component:
            raise ValueError("mixed record requires its exact component_source_scope")
    elif source in {SPOT, MARK, PERP}:
        component = source
        if row.get("component_source_scope", component) != component:
            raise ValueError("component_source_scope contradicts source_scope")
    else:
        raise ValueError("unsupported source_scope")
    if component in {MARK, PERP} and symbol != "HYPE":
        raise ValueError("derived component requires HYPE")
    alert = _stamp(row.get("original_alert_time_utc"), "original_alert_time_utc")
    observed = _stamp(row.get("observed_at_utc"), "observed_at_utc")
    if observed < alert:
        raise ValueError("observed_at_utc precedes the alert")
    if "wave_end_utc" not in row:
        raise ValueError("wave_end_utc is required (null for an active parent)")
    end = _stamp(row["wave_end_utc"], "wave_end_utc") if row["wave_end_utc"] is not None else None
    if end is not None and end <= alert:
        raise ValueError("wave_end_utc must follow the alert")
    closed = row.get("observation_closed")
    if type(closed) is not bool or closed != bool(end is not None and end <= observed):
        raise ValueError("observation_closed contradicts the frozen timestamps")
    floor = alert.replace(second=0, microsecond=0)
    measurement = alert if component == SPOT else floor + MINUTE
    entry_version = {SPOT: "IMMUTABLE_ALERT_REFERENCE",
                     MARK: "native-hype-next-full-minute-mark-open-v1",
                     PERP: "native-hype-next-full-minute-perp-open-v1"}[component]
    if row.get("entry_version", entry_version) != entry_version:
        raise ValueError("entry_version contradicts the source entry contract")
    if row.get("measurement_start_utc") is not None:
        if _stamp(row["measurement_start_utc"], "measurement_start_utc") != measurement:
            raise ValueError("measurement_start_utc contradicts the source entry contract")
    first = measurement.replace(second=0, microsecond=0)
    if measurement != first:
        first += MINUTE
    first_close = first + MINUTE - MILLISECOND
    latest = observed.replace(second=0, microsecond=0) - MILLISECOND
    cutoff = min(end, latest) if end is not None else None
    if end is None and row.get("btc_parent_observed_through_utc") is not None:
        cutoff = min(_stamp(row["btc_parent_observed_through_utc"],
                            "btc_parent_observed_through_utc"), latest)
    if row.get("observation_cutoff_utc") is not None:
        supplied = _stamp(row["observation_cutoff_utc"], "observation_cutoff_utc")
        if supplied > latest or (cutoff is not None and cutoff != supplied):
            raise ValueError("observation_cutoff_utc contradicts the frozen boundary")
        cutoff = supplied
    expected = _count(first, cutoff) if cutoff is not None else None
    if "expected_candles" in row:
        stored = row["expected_candles"]
        if type(stored) is not int or stored < 0:
            raise ValueError("expected_candles must be a nonnegative JSON integer")
        if expected is not None and expected != stored:
            raise ValueError("expected_candles contradicts the frozen boundary")
    final_expected = _count(first, end) if end is not None else None
    upper_bound = _count(first, latest)
    if closed and final_expected == 0:
        diagnosis = "CLOSED_WAVE_HAS_NO_FULL_POST_ENTRY_MINUTE"
        explanation = "The first admissible full candle closes after the frozen wave endpoint."
    elif not closed and (expected == 0 or (expected is None and upper_bound == 0)):
        diagnosis = "AWAITING_FIRST_COMPLETE_POST_ENTRY_MINUTE"
        explanation = "The frozen observable boundary admits no complete post-entry minute yet."
    elif cutoff is None:
        diagnosis = "INSUFFICIENT_BOUNDARY_EVIDENCE"
        explanation = "The active record lacks its verified BTC observation boundary."
    elif expected is not None and expected > 0 and component in {MARK, PERP} and any(
        reason.removeprefix("ValueError: ") == (
            "derived PERP TRADE entry must be a finite number" if component == PERP
            else "derived MARK entry must be a finite number") for reason in reasons
    ):
        diagnosis = "DERIVED_ENTRY_UNAVAILABLE_IN_REPORT"
        explanation = ("This report record could not use its derived entry. This does not "
                       "establish why, or whether another stored measurement has a valid entry.")
    else:
        diagnosis = "SOURCE_PATH_REQUIRES_REVIEW"
        explanation = "Timing alone cannot establish why the stored measurement is unavailable."
    return (event_id, parent, component), {
        "record_index": index, "source_scope": source, "coin_scope": coin,
        "symbol": symbol, "direction": direction, "original_status": row["status"],
        "original_missing_reasons": list(reasons), "diagnosis": diagnosis,
        "explanation": explanation, "first_admissible_open_utc": first.isoformat(),
        "first_admissible_close_utc": first_close.isoformat(),
        "wave_end_utc": end.isoformat() if end is not None else None,
        "observation_cutoff_utc": cutoff.isoformat() if cutoff is not None else None,
        "expected_complete_candles": expected,
        "expected_complete_candles_at_wave_end": final_expected,
        "wall_clock_upper_bound_candles": upper_bound,
    }


def diagnose(payload):
    """Return separate diagnostics; never mutate or replace supplied records."""
    if not isinstance(payload, dict):
        raise ValueError("input must be an object with generation_id and records")
    try:
        json.dumps(payload, allow_nan=False)
    except (ValueError, TypeError) as exc:
        raise ValueError("input must contain only finite JSON values") from exc
    generation = payload.get("generation_id")
    if not isinstance(generation, str):
        raise ValueError("generation_id must be a UUID string")
    try:
        UUID(generation)
    except ValueError as exc:
        raise ValueError("generation_id must be a UUID string") from exc
    records = payload.get("records")
    if not isinstance(records, list):
        raise ValueError("records must be an array of existing report records")
    groups, missing = {}, 0
    for index, row in enumerate(records):
        try:
            key, diagnostic = _inspect(row, index)
        except (ValueError, TypeError, OverflowError) as exc:
            raise ValueError(f"records[{index}]: {exc}") from exc
        if row["status"] != "DATA_MISSING":
            continue
        missing += 1
        if key not in groups:
            groups[key] = {"source_event_id": key[0], "btc_parent_movement_id": key[1],
                           "component_source_scope": key[2], "scope_records": []}
        groups[key]["scope_records"].append(diagnostic)
    return {"diagnostic_version": DIAGNOSTIC_VERSION, "generation_id": generation,
            "input_record_count": len(records), "missing_record_count": missing,
            "diagnostic_group_count": len(groups),
            "groups": [groups[key] for key in sorted(groups)], "limitation": LIMITATION}


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_constant(value):
    raise ValueError(f"Non-finite JSON constant: {value}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="Existing records JSON export")
    parser.add_argument("--output", type=Path, help="New diagnostic JSON file; defaults to stdout")
    args = parser.parse_args(argv)
    try:
        raw = args.input.read_bytes()
        payload = json.loads(raw, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
        result = diagnose(payload)
        result["input_file_sha256"] = hashlib.sha256(raw).hexdigest()
        encoded = json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
        if args.output:
            # Exclusive creation prevents overwriting either the evidence input or old diagnostics.
            with args.output.open("x", encoding="utf-8") as handle:
                handle.write(encoded)
        else:
            sys.stdout.write(encoded)
    except (OSError, UnicodeError, ValueError) as exc:
        print(f"Diagnostic input/output error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
