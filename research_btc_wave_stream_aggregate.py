"""Bounded full-history aggregation over immutable, verified parent reports.

The research mathematics remains research_btc_wave_endpoint_report's contract.
Only storage/provenance changes: exact rows and IDs live in generation parts.
Part payloads use PostgreSQL JSON, preserving the original numeric literals.
JSONB would turn 1e+16 into an integer-looking token and erase signed zero,
breaking both canonical evidence digests and native floating summation types.
Call aggregate_generation inside the caller's repeatable-read transaction after
source verification. This module neither commits nor writes database rows.
"""
from __future__ import annotations

from collections import Counter
import hashlib
import json
from typing import Callable, Iterable, Mapping
from uuid import uuid4

import research_btc_wave_endpoint_report as native

STORAGE_VERSION = "btc-wave-parts-v1"
_SCOPES = {native.SPOT_SCOPE, native.PERP_SCOPE, native.PERP_MIXED_SCOPE}
_LABEL_STATUSES = {"SUCCESS", "FAILURE", "UNRESOLVED", "OPEN", "DATA_MISSING"}
_RECORD_STATUSES = {"READY", "OPEN", "DATA_MISSING"}
# Existing Sheet delivery admits at most 256 cells, eight per comparison.
# At most three explicit source contracts share that bounded comparison set.
MAX_MIXED_SUMMARY_ROWS = 256
MAX_GROUPS = 3 * (MAX_MIXED_SUMMARY_ROWS // 8)
CURSOR_ROWS = 128
_SCALAR_FIELDS = {"mfe_pct", "mae_pct", "provisional_observed_mfe_pct", "provisional_observed_mae_pct"}


def _canonical(value):
    return json.dumps(value, sort_keys=True, default=native._json_default,
                      separators=(",", ":"), allow_nan=False, ensure_ascii=True)


def _digest(value):
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _reference(generation_id, kind, group=None, **extra):
    result = {"storage_version": STORAGE_VERSION, "generation_id": str(generation_id), "kind": kind}
    if group is not None:
        result.update(zip(("source_scope", "coin_scope", "direction"), group))
    result.update(extra)
    return result


def _groups_checked(groups):
    result = list(groups)
    if len(result) > MAX_GROUPS or len(result) != len(set(result)):
        raise ValueError("Full-wave aggregation exceeds its bounded comparison set")
    if any(len(group) != 3 or group[0] not in _SCOPES for group in result):
        raise ValueError("Unexpected full-wave source comparison")
    if sum(group[0] == native.PERP_MIXED_SCOPE for group in result) * 8 > MAX_MIXED_SUMMARY_ROWS:
        raise ValueError("Full-wave Sheet summary exceeds its existing 256-row bound")
    return sorted(result)


def aggregate_factories(groups: Iterable[tuple], record_factory: Callable,
                        scalar_factory: Callable, *, generation_id) -> dict:
    """Aggregate bounded comparison state; factories must share one snapshot.

    record_factory(group) yields records in strictly increasing parent-ID order.
    scalar_factory(group, field, closed_ready) yields that same ordered cohort's
    scalar values. Builtin sum consumes each iterator directly: no per-parent
    averages, SQL floating aggregation or runtime-dependent partial sums.
    """
    summaries, status_counts = [], Counter()
    record_count = mixed_missing = 0
    for group in _groups_checked(groups):
        count = closed_count = ready_count = missing_count = 0
        observed_complete, previous_parent = True, None
        closed_status = {round(t * 100): Counter() for t in native.SUPPORTED_THRESHOLDS_PCT}
        all_status = {bps: Counter() for bps in closed_status}
        for row in record_factory(group):
            if tuple(row[key] for key in ("source_scope", "coin_scope", "direction")) != group:
                raise ValueError("Record does not belong to the requested comparison")
            parent = row["btc_parent_movement_id"]
            if not isinstance(parent, str) or (previous_parent is not None and parent <= previous_parent):
                raise ValueError("A parent cannot occur twice or out of order in one comparison stratum")
            previous_parent = parent
            if row["status"] not in _RECORD_STATUSES or not isinstance(row["observation_closed"], bool):
                raise ValueError("Invalid full-wave record status")
            prefix_complete = bool(row.get("observed_prefix_complete"))
            thresholds = row.get("thresholds", [])
            if prefix_complete:
                bps = [label.get("threshold_bps") for label in thresholds]
                if len(bps) != 8 or set(bps) != set(range(25, 201, 25)) or row.get("method_version") != native.METHOD_VERSION:
                    raise ValueError("Complete full-wave record requires the exact eight versioned thresholds")
            closed = row["observation_closed"]
            ready = bool(closed and row["status"] == "READY" and row["path_complete"])
            count += 1
            closed_count += closed
            ready_count += ready
            missing_count += row["status"] == "DATA_MISSING"
            observed_complete = observed_complete and prefix_complete
            status_counts[row["status"]] += 1
            for bps in all_status:
                label = next((label for label in thresholds if label["threshold_bps"] == bps), {"status": "DATA_MISSING"})
                status = label["status"]
                if status not in _LABEL_STATUSES:
                    raise ValueError("Unexpected ordered first-touch status")
                all_status[bps][status] += 1
                if ready:
                    closed_status[bps][status] += 1
        if not count:
            raise ValueError("Empty comparison returned by the frozen group manifest")
        complete = bool(closed_count and closed_count == ready_count)
        # Match the native function's sum order and Python runtime exactly.
        total_mfe = sum(scalar_factory(group, "mfe_pct", True))
        total_mae = sum(scalar_factory(group, "mae_pct", True))
        observed_mfe = sum(scalar_factory(group, "provisional_observed_mfe_pct", False)) if observed_complete else None
        observed_mae = sum(scalar_factory(group, "provisional_observed_mae_pct", False)) if observed_complete else None
        source, scope, direction = group
        record_count += count
        if source == native.PERP_MIXED_SCOPE:
            mixed_missing += missing_count
        for threshold in native.SUPPORTED_THRESHOLDS_PCT:
            bps = round(threshold * 100)
            counts, all_counts = closed_status[bps], all_status[bps]
            decided = counts["SUCCESS"] + counts["FAILURE"]
            observed_decided = all_counts["SUCCESS"] + all_counts["FAILURE"]
            summaries.append({
                "method_version": native.METHOD_VERSION, "candidate": native.CANDIDATE, "source_scope": source,
                "coin_scope": scope, "direction": direction, "threshold_pct": threshold, "threshold_bps": bps,
                "representative_policy": native.REPRESENTATIVE_POLICY, "expected_representatives": count,
                "independent_parent_count": count, "closed_representatives": closed_count,
                "ready_closed_representatives": ready_count, "active_representatives": count - closed_count,
                "missing_representatives": missing_count, "closed_cohort_coverage_complete": complete,
                "closed_cohort_status_counts": dict(counts), "all_observed_status_counts": dict(all_counts),
                "success_probability": counts["SUCCESS"] / decided if complete and decided else None,
                "probability_definition": "FAVORABLE_FIRST / (FAVORABLE_FIRST + ADVERSE_FIRST); unresolved shown separately",
                "probability_denominator": decided if complete else None,
                "mfe_pct": total_mfe / ready_count if complete else None,
                "mae_pct": total_mae / ready_count if complete else None,
                "asymmetry_ratio": total_mfe / total_mae if complete and total_mae > 0 else None,
                "asymmetry_status": "INCOMPLETE_CLOSED_COHORT" if not complete else "UNDEFINED_ZERO_MAE" if not total_mae else "DEFINED",
                "asymmetry_method": native.ASYMMETRY_METHOD,
                "observed_cohort_coverage_complete": observed_complete,
                "provisional_observed_probability_denominator": observed_decided if observed_complete else None,
                "provisional_observed_success_probability": all_counts["SUCCESS"] / observed_decided if observed_complete and observed_decided else None,
                "provisional_observed_mfe_pct": observed_mfe / count if observed_complete else None,
                "provisional_observed_mae_pct": observed_mae / count if observed_complete else None,
                "provisional_observed_asymmetry_ratio": observed_mfe / observed_mae if observed_complete and observed_mae > 0 else None,
                "source_event_ids": _reference(generation_id, "source_event_ids", group),
                "btc_parent_movement_ids": _reference(generation_id, "btc_parent_movement_ids", group),
            })
    return {"summary_rows": summaries, "record_count": record_count,
            "record_status_counts": dict(status_counts), "missing_representatives": mixed_missing}


def _stream(conn, sql, params, *, batch_rows=CURSOR_ROWS):
    # A normal cursor buffers the entire result in libpq even if Python iterates.
    # A named cursor makes the transport batch bound real as well.
    with conn.cursor(name="btc_wave_aggregate_" + uuid4().hex) as cursor:
        cursor.itersize = batch_rows
        cursor.execute(sql, params)
        while True:
            batch = cursor.fetchmany(batch_rows)
            if not batch:
                break
            for item in batch:
                yield item
                del item
            batch.clear()
            del batch


_RECORDS_FROM = """FROM research_btc_wave_report_parts p
    CROSS JOIN LATERAL json_array_elements(p.report->'records') AS r(record)
    WHERE p.generation_id=%s AND r.record->>'source_scope'=%s
      AND r.record->>'coin_scope'=%s AND r.record->>'direction'=%s"""
_RECORD_ORDER = " ORDER BY (r.record->>'btc_parent_movement_id') COLLATE \"C\", (r.record->>'source_event_id')::bigint"


def aggregate_generation(conn, generation_id, observed_at, manifest):
    """Return the compact published root, reading no all-history Python graph."""
    if manifest.get("storage_version") != STORAGE_VERSION or str(manifest.get("generation_id")) != str(generation_id):
        raise ValueError("Wrong frozen generation manifest")
    observed_at = native.utc(observed_at)
    header = conn.execute("""SELECT observed_at_utc,parent_count,status FROM research_btc_wave_report_generations
        WHERE generation_id=%s""", (generation_id,)).fetchone()
    if (not header or native.utc(header["observed_at_utc"]) != observed_at
            or int(header["parent_count"]) != int(manifest["parent_count"])
            or header["status"] not in {"CAPTURED", "PUBLISHED"}):
        raise ValueError("Generation header does not match its frozen manifest")
    manifest_digest = manifest.get("source_manifest_digest")
    if not isinstance(manifest_digest, str) or len(manifest_digest) != 64 or any(c not in "0123456789abcdef" for c in manifest_digest):
        raise ValueError("Invalid source manifest digest")
    parts_hash = hashlib.sha256(b"btc-wave-parts-v1:reports\0")
    part_count, previous_ordinal, observed_btc = 0, None, None
    for part in _stream(conn, """SELECT ordinal,parent_id,report,report_digest FROM research_btc_wave_report_parts
            WHERE generation_id=%s ORDER BY ordinal""", (generation_id,), batch_rows=1):
        completed = part["report"]
        if (not completed or _digest(completed) != part["report_digest"]
                or completed.get("method_version") != native.METHOD_VERSION
                or native.utc(completed["observed_at_utc"]) != observed_at):
            raise ValueError("Incomplete or changed parent report")
        if previous_ordinal is not None and part["ordinal"] <= previous_ordinal:
            raise ValueError("Duplicate or unordered parent part")
        previous_ordinal = part["ordinal"]
        waves = completed["waves"]
        if len(waves) != 1 or waves[0]["btc_parent_movement_id"] != part["parent_id"]:
            raise ValueError("A stored part must describe exactly its own parent")
        if any(row["btc_parent_movement_id"] != part["parent_id"] for row in completed["records"]):
            raise ValueError("Stored record belongs to another parent")
        parent = waves[0]
        if not parent.get("end_time_utc") and parent.get("observed_through_utc"):
            cutoff = native.utc(parent["observed_through_utc"])
            observed_btc = cutoff if observed_btc is None else max(observed_btc, cutoff)
        parts_hash.update(_canonical([part["ordinal"], part["parent_id"], part["report_digest"]]).encode())
        parts_hash.update(b"\n")
        part_count += 1
        del parent, waves, completed, part
    if part_count != header["parent_count"] or not part_count:
        raise ValueError("Generation is missing a parent part")
    groups = conn.execute("""SELECT DISTINCT r.record->>'source_scope' AS source_scope,
        r.record->>'coin_scope' AS coin_scope,r.record->>'direction' AS direction
        FROM research_btc_wave_report_parts p
        CROSS JOIN LATERAL json_array_elements(p.report->'records') AS r(record)
        WHERE p.generation_id=%s LIMIT %s""", (generation_id, MAX_GROUPS + 1)).fetchall()
    groups = [tuple(row[key] for key in ("source_scope", "coin_scope", "direction")) for row in groups]
    def records(group):
        for row in _stream(conn, "SELECT r.record AS record " + _RECORDS_FROM + _RECORD_ORDER,
                           (generation_id, *group)):
            yield row["record"]
    def scalars(group, field, closed_ready):
        if field not in _SCALAR_FIELDS:
            raise ValueError("Unsupported full-wave scalar")
        predicate = (" AND r.record->>'observation_closed'='true' AND r.record->>'status'='READY'"
                     " AND r.record->>'path_complete'='true'") if closed_ready else ""
        for row in _stream(conn, "SELECT r.record->%s AS value " + _RECORDS_FROM + predicate + _RECORD_ORDER,
                           (field, generation_id, *group)):
            yield row["value"]
    aggregated = aggregate_factories(groups, records, scalars, generation_id=generation_id)
    records_digest = parts_hash.hexdigest()
    reference = {"source_manifest_digest": manifest_digest, "report_manifest_digest": records_digest}
    return {
        "method_version": native.METHOD_VERSION, "candidate": native.CANDIDATE,
        "storage_version": STORAGE_VERSION, "observed_at_utc": observed_at,
        "latest_closed_cutoff_utc": native.latest_closed_cutoff(observed_at),
        "representative_policy": native.REPRESENTATIVE_POLICY,
        "source_scope_policy": "CANONICAL_SPOT_AND_PERP_SEPARATE; EXPLICIT_ALTERNATIVE_MIXED_CONTRACT_VERSIONED",
        "mixed_source_contract": {"source_scope": native.PERP_MIXED_SCOPE,
            "non_hype_entry": "IMMUTABLE_ALERT_REFERENCE_AND_CANONICAL_SPOT_PATH",
            "hype_entry": "NEXT_FULL_MINUTE_HYPERLIQUID_PERPETUAL_TRADE_OPEN_AND_TRADE_PATH",
            "production_formula_eligible": False, "original_events_modified": False},
        "boundary_policy": "FIRST_FULL_POST_ENTRY_MINUTE_THROUGH_INCLUSIVE_BTC_REVERSAL_CLOSE_OR_LATEST_CLOSED_MINUTE",
        "fixed_horizon_outcomes_modified": False,
        "generation_id": str(generation_id), "parent_count": part_count,
        "source_digest": manifest_digest, "source_manifest": dict(manifest),
        "report_manifest_digest": records_digest,
        "waves": _reference(generation_id, "waves", **reference),
        "records": _reference(generation_id, "records", **reference),
        "waves_without_qualifying_native_alert": _reference(generation_id, "waves_without_qualifying_native_alert", **reference),
        "btc_observed_through_utc": observed_btc,
        "limitations": ["Native DELIVERED alerts only; archive WATCH_CANDIDATE is a separate cohort.",
            "Wave boundaries are an operational independence policy, not statistical proof.",
            "LONG and SHORT strata and ALL versus individual coins must not be summed.",
            "Active waves expose provisional observed results and remain OPEN.",
            "Exact source and result rows are retained in the referenced immutable generation parts."],
        **aggregated,
    }
