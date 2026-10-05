"""Bounded read-only Stage-8 source-coverage receipt.

This command never searches for a database URL, mutates a database, evaluates
a candidate, or reports formula qualification. It requires explicit target,
role and password fields and only aggregates structural statuses returned by
``research_operational_score_source_audit``. A missing explicit configuration
is an ``UNKNOWN_NOT_QUERIED`` result, never zero data.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import math
import os
import re
import time
from typing import Any, Callable, Mapping, Sequence

import research_operational_score_source_audit as source_audit
import research_stage8_contract as contract


VERSION = "stage8-bounded-coverage-receipt-v1"
ATTEMPT_COHORT_HANDOFF_VERSION = "stage8-bounded-attempt-cohort-handoff-v1"
OUTCOME_FREE_POPULATION_RECEIPT_VERSION = (
    "stage8-outcome-free-attempt-population-receipt-v1"
)
DATABASE_URL_ENV = "RESEARCH_STAGE8_AUDIT_DATABASE_URL"
START_UTC_ENV = "RESEARCH_STAGE8_AUDIT_START_UTC"
END_UTC_ENV = "RESEARCH_STAGE8_AUDIT_END_UTC"
MAX_PAGE_SIZE = 25
MAX_PAGES = 40
MAX_WALL_SECONDS = 30.0
DEFAULT_PAGE_SIZE = MAX_PAGE_SIZE
DEFAULT_MAX_PAGES = MAX_PAGES
DEFAULT_MAX_WALL_SECONDS = MAX_WALL_SECONDS
IN_FLIGHT_STATEMENT_TIMEOUT_MILLISECONDS = 1000
_STARTUP_MANIFEST = contract.frozen_manifest()
contract.validate_manifest(_STARTUP_MANIFEST)
_MANIFEST_SHA256 = contract.digest(_STARTUP_MANIFEST)
_AUDIT_VERSION = _STARTUP_MANIFEST["source"]["audit_version"]
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_SYMBOLS = tuple(dict.fromkeys(
    symbol
    for scope in _STARTUP_MANIFEST["candidates"]["scopes"]
    for symbol in scope["symbols"]
))
_SCOPE_KEYS = frozenset({
    "sampler_version", "symbols", "start_utc", "end_utc", "version",
    "max_capture_age_seconds", "windows", "thresholds_bps", "page_size",
    "capture_version", "outcome_version", "parent_policy",
})
_REQUIRED_CONNECTION_FIELDS = frozenset({
    "host", "port", "dbname", "user", "password", "sslmode",
})
_ALLOWED_CONNECTION_FIELDS = _REQUIRED_CONNECTION_FIELDS | frozenset({
    "hostaddr", "sslrootcert", "channel_binding", "target_session_attrs",
})
_FORBIDDEN_CONNECTION_FIELDS = frozenset({"service", "servicefile", "passfile"})
_SAFE_STATUSES = {
    "attempt": frozenset({"EVALUABLE", "UNEVALUABLE", "COVERAGE_EXCLUDED", "UNKNOWN"}),
    "authority": frozenset({"VALID", "UNKNOWN", "NOT_APPLICABLE"}),
    "outcome_source": frozenset({"OPEN", "SUCCESS", "FAILURE", "UNRESOLVED",
                                  "DATA_MISSING", "MISSING"}),
    "outcome_reported": frozenset({"SUCCESS", "FAILURE", "OPEN", "AMBIGUOUS",
                                    "NO_TOUCH", "DATA_MISSING", "UNKNOWN"}),
}
_SAFE_REASONS = {
    "anchor": frozenset("""
        ANCHOR_ATTEMPT_ID_INVALID ANCHOR_ATTEMPT_SLOT_MISMATCH
        ANCHOR_AUTHORITY_INVALID ANCHOR_COVERAGE_INVALID
        ANCHOR_COVERAGE_POLICY_VERSION_MISMATCH ANCHOR_DECISION_INTERVAL_INVALID
        ANCHOR_EVALUATION_REASON_MISSING ANCHOR_EVALUATION_STATUS_UNKNOWN
        ANCHOR_EVENT_ANCHOR_KEY_MISMATCH ANCHOR_EVENT_AUTHORITY_MISMATCH
        ANCHOR_EVENT_FINGERPRINT_MISMATCH ANCHOR_EVENT_MISSING
        ANCHOR_EVENT_REFERENCE_MISMATCH ANCHOR_EVENT_TIME_MISMATCH
        ANCHOR_EXACT_EVENT_PAIR_MISMATCH ANCHOR_FEATURE_BUNDLE_INVALID
        ANCHOR_MODEL_SCORE_STATUS_NOT_ABSENT ANCHOR_REFERENCE_PRICE_MISMATCH
        ANCHOR_SAMPLER_VERSION_MISMATCH ANCHOR_SILENT_NEUTRAL_CONTRACT_MISMATCH
        ANCHOR_SLOT_ID_INVALID ANCHOR_SLOT_MISSING ANCHOR_SOURCE_INVALID
        NON_EVALUABLE_ATTEMPT_HAS_DECISION_TIME
    """.split()),
    "capture": frozenset("""
        ATTEMPT_NOT_EVALUABLE_NO_DECISION_TIME
        NO_DURABLY_PRIOR_WATCH_CAPTURE_WITHIN_MAX_AGE
        CAPTURE_ADDITIVE_COMPONENT_CONTRACT_MISMATCH
        CAPTURE_AVAILABLE_MEMBER_REFERENCE_MISSING
        CAPTURE_AVAILABLE_MODEL_WINDOW_EVIDENCE_MISSING
        CAPTURE_AVAILABLE_WINDOW_TIME_MISSING CAPTURE_CODE_HASH_IDENTITIES_INVALID
        CAPTURE_COIN_MISSING_OR_INVALID CAPTURE_COIN_PARTIAL_OR_ABSENT
        CAPTURE_COIN_UNIVERSE_MISMATCH CAPTURE_COMPUTED_TIME_ORDER_INVALID
        CAPTURE_CONTRACT_MISMATCH CAPTURE_CYCLE_ID_MISMATCH
        CAPTURE_DERIVATIVES_REFERENCE_HASH_INVALID
        CAPTURE_HYPE_OPERATIONAL_PAIR_MISMATCH CAPTURE_HYPE_SPOT_IDENTITY_INCOMPLETE
        CAPTURE_INNER_HASH_MISMATCH CAPTURE_INPUT_ROW_COUNT_INVALID
        CAPTURE_INPUT_UNIVERSE_HASH_INVALID CAPTURE_INVALID
        CAPTURE_MAXPAIN_ADDITIVE_COMPONENT_INVALID CAPTURE_MAXPAIN_ADDITIVE_SUM_MISMATCH
        CAPTURE_MAXPAIN_SCORE_STATE_INVALID CAPTURE_MAXPAIN_SLOT_GRID_MISMATCH
        CAPTURE_MODEL_AVAILABILITY_MISMATCH CAPTURE_MODEL_AVAILABILITY_MISSING
        CAPTURE_MODEL_SCORE_INVALID CAPTURE_NOT_COMPLETE
        CAPTURE_NOT_DURABLY_PRIOR_WITHIN_MAX_AGE
        CAPTURE_OPERATIONAL_PRICE_IDENTITY_INCOMPATIBLE
        CAPTURE_OPERATIONAL_PRICE_IDENTITY_MISSING CAPTURE_OUTER_REFERENCE_INVALID
        CAPTURE_SIZE_LIMIT_EXCEEDED CAPTURE_SNAPSHOT_ID_INVALID
        CAPTURE_SOURCE_NOT_WATCH_SHARED CAPTURE_SOURCE_ROWS_INCOMPLETE
        CAPTURE_SOURCE_SIDE_SEMANTICS_MISMATCH CAPTURE_SOURCE_TIME_AUDIT_MISSING
        CAPTURE_SOURCE_TIME_ERROR CAPTURE_SOURCE_TIME_FUTURE CAPTURE_SOURCE_TIME_UNKNOWN
        CAPTURE_SYMBOL_UNIVERSE_MISMATCH CAPTURE_TIME_FAMILY_MEMBERS_MISSING
        CAPTURE_UNAVAILABLE_TARGET_HAS_SCORE
    """.split()),
    "outcome": frozenset("""
        OUTCOME_CREATED_BEFORE_EVENT OUTCOME_EVENT_AUTHORITY_MISSING
        OUTCOME_EVIDENCE_AFTER_REVISION OUTCOME_INVALID OUTCOME_MARKET_PAIR_MISMATCH
        OUTCOME_PRICE_SOURCE_GRAMMAR_INVALID OUTCOME_PRICE_SOURCE_ROUTE_MISSING
        OUTCOME_PRICE_SOURCE_SEGMENTS_NOT_UNIQUE OUTCOME_REFERENCE_SOURCE_IDENTITY_MISMATCH
        OUTCOME_REQUESTED_CELL_MISMATCH OUTCOME_REVISION_TIME_MISSING_OR_AFTER_AUDIT
        OUTCOME_REVISION_TIME_ORDER_INVALID OUTCOME_ROUTE_QUALITY_MISMATCH
        OUTCOME_ROW_MISSING BARRIER_PRICE_MISMATCH CANDLE_INTERVAL_MISMATCH
        DECISION_AFTER_HORIZON DECISION_DURATION_MISMATCH DIRECTION_MISMATCH
        FIRST_TOUCH_SIDE_MISMATCH INCOMPLETE_PATH INITIAL_GAP_AUDIT_MISMATCH
        INVALID_EVENT_IDENTITY INVALID_NONDECISIVE_OUTCOME_STATE
        MEASUREMENT_DOES_NOT_START_AT_ALERT MISSING_BARRIER_PRICES
        MISSING_DECISION_FEATURE_TIMESTAMP MISSING_EXCURSION_METRICS
        MISSING_ORIGINAL_ENTRY_PRICE MISSING_OR_INVALID_DECISION_TIME
        MISSING_OUTCOME_CELL MISSING_PATH_SAMPLES MISSING_TOUCH_PRICE
        NONDECISIVE_OUTCOME_HAS_TOUCH OUTCOME_EVENT_ID_MISMATCH OUTCOME_METHOD_MISMATCH
        OUTCOME_TIME_ORDER_OR_FUTURE_DATA REFERENCE_PRICE_DIFFERS_FROM_ORIGINAL_ENTRY
        SUCCESS_FLAG_MISMATCH TERMINAL_REASON_MISMATCH
        TOUCH_PRICE_DOES_NOT_MATCH_BARRIER UNRESOLVED_OR_OPEN_OUTCOME
        UNSUPPORTED_HORIZON UNSUPPORTED_THRESHOLD UNVERIFIED_DATA_QUALITY
    """.split()),
    "parent": frozenset("""
        PARENT_EVENT_AUTHORITY_MISSING PARENT_MEMBERSHIP_DECISION_TIME_MISMATCH
        PARENT_MEMBERSHIP_EVENT_ID_MISMATCH PARENT_MEMBERSHIP_MISSING
        PARENT_MEMBERSHIP_POLICY_MISMATCH PARENT_MEMBERSHIP_STATUS_UNKNOWN
        BTC_CANONICAL_MEMBERSHIP_MISMATCH BTC_DATA_MISSING
        BTC_MEMBERSHIP_BAR_TIME_MISMATCH BTC_PARENT_BOUNDARY_INVALID
        BTC_PARENT_BOUNDARY_UNVERIFIED BTC_PARENT_CANONICAL_ID_MISMATCH
        BTC_PARENT_ID_OR_POLICY_MISMATCH BTC_PARENT_MEMBERSHIP_INVALID
        BTC_PARENT_OR_BAR_SOURCE_MISMATCH BTC_PARENT_ROW_MISSING
        BTC_PARENT_STATE_INVALID BTC_SOURCE_BAR_MISSING
    """.split()),
}


def _utc(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
        str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("audit bounds require an explicit UTC offset")
    return parsed.astimezone(timezone.utc)


def _counter(counter: Counter) -> dict[str, int]:
    return {str(key): counter[key] for key in sorted(counter)}


def _reason_counts(items: Sequence[Mapping[str, Any]], *, group: str) -> dict[str, int]:
    counts = Counter()
    allowed = _SAFE_REASONS[group]
    for item in items:
        reasons = item.get("reasons")
        if isinstance(reasons, list):
            for reason in reasons:
                base = reason.split(":", 1)[0] if isinstance(reason, str) else ""
                counts[base if base in allowed else group.upper() + "_UNRECOGNIZED_REASON"] += 1
    return _counter(counts)


def _status(value: Any, *, group: str) -> str:
    return value if isinstance(value, str) and value in _SAFE_STATUSES[group] else "UNKNOWN"


def _assert_runtime_contract() -> None:
    manifest = contract.frozen_manifest()
    contract.validate_manifest(manifest)
    if (manifest["source"]["audit_version"] != _AUDIT_VERSION
            or source_audit.VERSION != _AUDIT_VERSION):
        raise ValueError("source audit module version differs from the frozen manifest")


def _normalized_scope(scope: Mapping[str, Any]) -> dict:
    """Validate every manifest-controlled adapter axis and return strict JSON."""
    manifest = contract.frozen_manifest()
    contract.validate_manifest(manifest)
    _assert_runtime_contract()
    if not isinstance(scope, Mapping) or set(scope) != _SCOPE_KEYS:
        raise ValueError("coverage page scope shape mismatch")
    symbols = scope.get("symbols")
    windows = scope.get("windows")
    thresholds = scope.get("thresholds_bps")
    page_size = scope.get("page_size")
    age = scope.get("max_capture_age_seconds")
    if (not isinstance(symbols, list) or not symbols
            or any(type(symbol) is not str or symbol not in _SYMBOLS for symbol in symbols)
            or symbols != sorted(set(symbols))):
        raise ValueError("coverage page symbols are not one explicit frozen subset")
    if (type(page_size) is not int or not 1 <= page_size <= MAX_PAGE_SIZE
            or isinstance(age, bool) or type(age) not in (int, float)
            or not math.isfinite(float(age))):
        raise ValueError("coverage page bound is invalid")
    start, end = _utc(scope.get("start_utc")), _utc(scope.get("end_utc"))
    normalized = {
        "adapter_version": scope.get("version"),
        "sampler_version": scope.get("sampler_version"),
        "symbols": symbols,
        "start_utc": start.isoformat(),
        "end_utc": end.isoformat(),
        "max_capture_age_seconds": float(age),
        "windows": windows,
        "thresholds_bps": thresholds,
        "page_size": page_size,
        "capture_version": scope.get("capture_version"),
        "outcome_version": scope.get("outcome_version"),
        "parent_policy": scope.get("parent_policy"),
    }
    expected_axes = {
        "adapter_version": _AUDIT_VERSION,
        "sampler_version": manifest["source"]["sampler_version"],
        "max_capture_age_seconds": float(manifest["source"]["max_capture_age_seconds"]),
        "windows": [manifest["labels"]["window_minutes"]],
        "thresholds_bps": manifest["labels"]["thresholds_bps"],
        "capture_version": manifest["source"]["watch_version"],
        "outcome_version": manifest["labels"]["method_version"],
        "parent_policy": manifest["independence"]["parent_policy_version"],
    }
    if start >= end or any(normalized[key] != value for key, value in expected_axes.items()):
        raise ValueError("coverage page scope differs from frozen manifest axes")
    return normalized


def _expected_scope(*, symbols: Sequence[str], start: datetime, end: datetime,
                    page_size: int) -> dict:
    manifest = contract.frozen_manifest()
    contract.validate_manifest(manifest)
    return {
        "adapter_version": _AUDIT_VERSION,
        "sampler_version": manifest["source"]["sampler_version"],
        "symbols": sorted(symbols), "start_utc": start.isoformat(),
        "end_utc": end.isoformat(),
        "max_capture_age_seconds": float(manifest["source"]["max_capture_age_seconds"]),
        "windows": [manifest["labels"]["window_minutes"]],
        "thresholds_bps": manifest["labels"]["thresholds_bps"],
        "page_size": page_size, "capture_version": manifest["source"]["watch_version"],
        "outcome_version": manifest["labels"]["method_version"],
        "parent_policy": manifest["independence"]["parent_policy_version"],
    }


def _unknown(reason: str, *, missing_configuration: Sequence[str] = ()) -> dict:
    return {
        "version": VERSION,
        "manifest_sha256": _MANIFEST_SHA256,
        "status": "UNKNOWN_NOT_QUERIED",
        "reason": reason,
        "missing_configuration": sorted(set(missing_configuration)),
        "database_connection_attempted": False,
        "counts": None,
        "interpretation": (
            "No database observation was made; absent counts are unknown, not zero."
        ),
    }


def _explicit_connection_fields(raw: str, conninfo_to_dict: Callable[[str], Mapping]) -> dict:
    """Return an explicitly declared target, role and password field set."""
    parsed = conninfo_to_dict(raw)
    if (not isinstance(parsed, Mapping)
            or _FORBIDDEN_CONNECTION_FIELDS.intersection(parsed)
            or set(parsed).difference(_ALLOWED_CONNECTION_FIELDS)
            or any(not isinstance(parsed.get(key), str) or not parsed[key]
                   for key in _REQUIRED_CONNECTION_FIELDS)):
        raise ValueError("explicit database target, role and password fields are required")
    host, port = parsed["host"], parsed["port"]
    if ("," in host or "," in port or not port.isdecimal()
            or not 1 <= int(port) <= 65535):
        raise ValueError("exactly one explicit database host and port are required")
    if parsed["sslmode"] not in {"disable", "require", "verify-ca", "verify-full"}:
        raise ValueError("sslmode must be an explicit non-fallback mode")
    if parsed["sslmode"] in {"verify-ca", "verify-full"} and not parsed.get("sslrootcert"):
        raise ValueError("verified TLS requires an explicit root certificate path")
    result = {key: parsed[key] for key in sorted(_ALLOWED_CONNECTION_FIELDS)
              if key in parsed}
    result.setdefault("channel_binding", "prefer")
    result.setdefault("target_session_attrs", "any")
    return result


def outcome_free_population_payload(value: Mapping[str, Any]) -> dict:
    """Project only source-population fields; outcome state is excluded."""
    raw_counts = value.get("counts")
    counts = raw_counts if isinstance(raw_counts, Mapping) else {}
    return {
        "version": OUTCOME_FREE_POPULATION_RECEIPT_VERSION,
        "coverage_receipt_version": value.get("version"),
        "manifest_sha256": value.get("manifest_sha256"),
        "manifest_axes_verified": value.get("manifest_axes_verified"),
        "status": value.get("status"),
        "database_connection_attempted": value.get("database_connection_attempted"),
        "query_scope": value.get("query_scope"),
        "attempts_examined": value.get("attempts_examined"),
        "attempt_status_counts": counts.get("attempt_status"),
        "high_water_attempt_id": value.get("high_water_attempt_id"),
        "attempt_population_version": value.get("attempt_population_version"),
        "attempt_population_sha256": value.get("attempt_population_sha256"),
        "transaction_identity_sha256": value.get("transaction_identity_sha256"),
        "stop_reason": value.get("stop_reason"),
    }


def _aggregate_pages(pages: Sequence[Mapping[str, Any]], *, truncated: bool,
                     stop_reason: str | None = None,
                     expected_query_scope: Mapping[str, Any] | None = None,
                     _attempt_ids_out: list[int] | None = None) -> dict:
    """Internal reducer for pages read by one runner-owned transaction."""
    if not pages and not truncated:
        raise ValueError("a complete receipt requires at least one audited page")
    attempts = Counter()
    anchors = Counter()
    captures = Counter()
    outcomes = Counter()
    reported_outcomes = Counter()
    parents = Counter()
    anchor_items: list[Mapping[str, Any]] = []
    capture_items: list[Mapping[str, Any]] = []
    outcome_items: list[Mapping[str, Any]] = []
    parent_items: list[Mapping[str, Any]] = []
    parent_ids: set[str] = set()
    valid_intersections = 0
    examined = 0
    high_water = None
    first_read = None
    last_read = None
    expected_scope = None
    first_scope = None
    atomic_snapshot = True
    transaction_identity = None
    last_attempt_id = 0
    attempt_ids: list[int] = []

    for index, page in enumerate(pages):
        if not isinstance(page, Mapping):
            raise ValueError("coverage page must be a mapping")
        _assert_runtime_contract()
        if page.get("version") != _AUDIT_VERSION:
            raise ValueError("coverage page source version mismatch")
        scope = page.get("scope")
        normalized_scope = _normalized_scope(scope)
        if (expected_query_scope is not None
                and contract.canonical(normalized_scope)
                != contract.canonical(expected_query_scope)):
            raise ValueError("coverage page differs from the requested bounded cohort")
        scope_identity = contract.digest(normalized_scope)
        if expected_scope is None:
            expected_scope = scope_identity
            first_scope = normalized_scope
            high_water = page.get("high_water_attempt_id")
        elif (scope_identity != expected_scope
              or page.get("high_water_attempt_id") != high_water):
            raise ValueError("coverage pages do not share one bounded cohort")
        rows = page.get("rows")
        has_more = page.get("has_more")
        complete = page.get("population_page_complete")
        examined_page, emitted_page = page.get("examined"), page.get("emitted")
        if (not isinstance(rows, list) or any(not isinstance(row, Mapping) for row in rows)
                or type(examined_page) is not int or type(emitted_page) is not int
                or examined_page != len(rows) or emitted_page != len(rows)
                or len(rows) > normalized_scope["page_size"]
                or type(high_water) is not int or not 0 <= high_water <= 9223372036854775807
                or type(has_more) is not bool or type(complete) is not bool
                or complete is has_more
                or (page.get("next_cursor") is not None) is not has_more
                or (has_more and not rows)
                or (index < len(pages) - 1 and not has_more)
                or (index == len(pages) - 1 and not truncated and has_more)):
            raise ValueError("coverage page protocol is malformed")
        page_ids = []
        for row in rows:
            attempt = row.get("attempt")
            attempt_id = attempt.get("attempt_id") if isinstance(attempt, Mapping) else None
            if (type(attempt_id) is not int or not last_attempt_id < attempt_id <= high_water):
                raise ValueError("coverage attempt keyset is not strictly increasing")
            page_ids.append(attempt_id)
            attempt_ids.append(attempt_id)
            last_attempt_id = attempt_id
        if page_ids != sorted(set(page_ids)):
            raise ValueError("coverage attempt page contains duplicate or unordered IDs")
        if page.get("snapshot_consistency") != "CALLER_TRANSACTION_SNAPSHOT":
            atomic_snapshot = False
        page_transaction_identity = page.get("transaction_identity_sha256")
        if (not isinstance(page_transaction_identity, str)
                or _HASH.fullmatch(page_transaction_identity) is None):
            raise ValueError("coverage page transaction identity is invalid")
        if transaction_identity is None:
            transaction_identity = page_transaction_identity
        elif transaction_identity != page_transaction_identity:
            raise ValueError("coverage pages do not share one database transaction")
        examined += examined_page
        observed = _utc(page["read_started_at_utc"])
        first_read = observed if first_read is None else min(first_read, observed)
        last_read = observed if last_read is None else max(last_read, observed)
        for row in rows:
            attempt = row.get("attempt") or {}
            attempts[_status(attempt.get("evaluation_status"), group="attempt")] += 1
            anchor = row.get("anchor_authority") or {}
            captured = row.get("capture") or {}
            if not isinstance(anchor, Mapping) or not isinstance(captured, Mapping):
                raise ValueError("coverage authority envelope is malformed")
            anchor_items.append(anchor)
            capture_items.append(captured)
            anchors[_status(anchor.get("status"), group="authority")] += 1
            captures[_status(captured.get("status"), group="authority")] += 1
            cells_by_direction: dict[str, list[Mapping[str, Any]]] = {}
            cells = row.get("outcome_cells")
            parent_map = row.get("parent_memberships")
            if (not isinstance(cells, list)
                    or any(not isinstance(cell, Mapping) for cell in cells)
                    or not isinstance(parent_map, Mapping)
                    or set(parent_map) != {"LONG", "SHORT"}):
                raise ValueError("coverage outcome or parent envelope is malformed")
            expected_cells = {
                (direction, window, threshold)
                for direction in ("LONG", "SHORT")
                for window in normalized_scope["windows"]
                for threshold in normalized_scope["thresholds_bps"]
            }
            actual_cells = [
                (cell.get("direction"), cell.get("window_minutes"),
                 cell.get("threshold_bps"))
                for cell in cells
            ]
            if len(actual_cells) != len(expected_cells) or set(actual_cells) != expected_cells:
                raise ValueError("coverage outcome cell matrix is incomplete or duplicated")
            for cell in cells:
                outcome_items.append(cell)
                direction = str(cell.get("direction") or "UNKNOWN")
                cells_by_direction.setdefault(direction, []).append(cell)
                outcomes[_status(cell.get("source_status") or "MISSING",
                                 group="outcome_source")] += 1
                reported_outcomes[_status(cell.get("reported_status"),
                                          group="outcome_reported")] += 1
            for direction, parent in parent_map.items():
                parent = parent or {}
                if not isinstance(parent, Mapping):
                    raise ValueError("coverage parent envelope is malformed")
                parent_items.append(parent)
                parents[_status(parent.get("status"), group="authority")] += 1
                if parent.get("status") == "VALID":
                    membership = parent.get("membership") or {}
                    identity = membership.get("btc_parent_movement_id")
                    if isinstance(identity, str) and identity:
                        parent_ids.add(identity)
                if (anchor.get("status") == "VALID"
                        and captured.get("status") == "VALID"
                        and parent.get("status") == "VALID"):
                    valid_intersections += sum(
                        cell.get("status") == "VALID"
                        for cell in cells_by_direction.get(str(direction), [])
                    )

    if not atomic_snapshot:
        truncated = True
        stop_reason = stop_reason or "NON_ATOMIC_DATABASE_SNAPSHOT"
    if not truncated and not (
            (high_water == 0 and examined == 0)
            or (examined > 0 and last_attempt_id == high_water)):
        raise ValueError("complete coverage page did not reach its high-water mark")
    status = "BOUNDED_PARTIAL" if truncated else "COMPLETE_BOUNDED_COHORT"
    query_scope = ({**(first_scope or expected_query_scope),
                    "query_sha256": expected_scope or contract.digest(expected_query_scope)}
                   if first_scope or expected_query_scope else None)
    attempt_population = {
        "version": "stage8-bounded-attempt-id-population-v1",
        "manifest_sha256": _MANIFEST_SHA256,
        "query_sha256": query_scope.get("query_sha256") if query_scope else None,
        "transaction_identity_sha256": transaction_identity,
        "high_water_attempt_id": high_water,
        "attempt_ids": attempt_ids,
    }
    result = {
        "version": VERSION,
        "manifest_sha256": _MANIFEST_SHA256,
        "manifest_axes_verified": bool(pages),
        "status": status,
        "database_connection_attempted": True,
        "pages_read": len(pages),
        "query_scope": query_scope,
        "attempts_examined": examined,
        "high_water_attempt_id": high_water,
        "attempt_population_version": attempt_population["version"],
        "attempt_population_sha256": contract.digest(attempt_population),
        "read_started_at_utc": first_read.isoformat() if first_read else None,
        "last_page_read_started_at_utc": last_read.isoformat() if last_read else None,
        "transaction_identity_sha256": transaction_identity,
        "stop_reason": stop_reason,
        "counts": {
            "attempt_status": _counter(attempts),
            "anchor_authority_status": _counter(anchors),
            "capture_status": _counter(captures),
            "outcome_source_status": _counter(outcomes),
            "outcome_reported_status": _counter(reported_outcomes),
            "parent_membership_status": _counter(parents),
            "distinct_valid_btc_parent_movement_ids": len(parent_ids),
            "valid_anchor_capture_parent_outcome_cell_intersections": valid_intersections,
        },
        "reason_counts": {
            "anchor": _reason_counts(anchor_items, group="anchor"),
            "capture": _reason_counts(capture_items, group="capture"),
            "outcome": _reason_counts(outcome_items, group="outcome"),
            "parent": _reason_counts(parent_items, group="parent"),
        },
        "formula_qualification_evaluated": False,
        "candidate_score_values_returned": False,
        "interpretation": (
            "Structural source coverage only. Counts do not establish candidate "
            "matches, statistical independence, probability, asymmetry, delivery, "
            "or formula qualification."
        ),
    }
    result["outcome_free_population_receipt_sha256"] = contract.digest(
        outcome_free_population_payload(result)
    )
    if _attempt_ids_out is not None:
        _attempt_ids_out.extend(attempt_ids)
    return {**result, "receipt_sha256": contract.digest(result)}


def _run_bounded_audit_with_attempt_ids(
    conn: Any, *, start_utc: Any, end_utc: Any,
    symbols: Sequence[str] = _SYMBOLS,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages: int = DEFAULT_MAX_PAGES,
    max_wall_seconds: float = DEFAULT_MAX_WALL_SECONDS,
    page_reader: Callable[..., Mapping[str, Any]] =
        source_audit.audit_anchor_attempt_page_from_connection,
    monotonic: Callable[[], float] = time.monotonic,
) -> tuple[dict, list[int]]:
    """One traversal; retain validated IDs separately from count-only output."""
    start, end = _utc(start_utc), _utc(end_utc)
    _assert_runtime_contract()
    if start >= end:
        raise ValueError("start_utc must precede end_utc")
    if (type(max_pages) is not int or not 1 <= max_pages <= MAX_PAGES
            or type(page_size) is not int
            or not 1 <= page_size <= MAX_PAGE_SIZE):
        raise ValueError("invalid audit page bound")
    if (isinstance(max_wall_seconds, bool)
            or not isinstance(max_wall_seconds, (int, float))
            or not math.isfinite(float(max_wall_seconds))
            or not 0 < float(max_wall_seconds) <= MAX_WALL_SECONDS):
        raise ValueError("max_wall_seconds exceeds the fixed positive bound")
    if (isinstance(symbols, (str, bytes)) or not isinstance(symbols, Sequence)
            or not symbols or any(type(symbol) is not str or symbol not in _SYMBOLS
                                  for symbol in symbols)
            or len(set(symbols)) != len(symbols)):
        raise ValueError("symbols must be one explicit unique frozen subset")
    started = monotonic()
    if (isinstance(started, bool) or not isinstance(started, (int, float))
            or not math.isfinite(float(started))):
        raise ValueError("monotonic clock returned an invalid value")
    started = float(started)
    absolute_deadline = started + float(max_wall_seconds)
    pages = []
    cursor = None
    stop_reason = None
    elapsed = 0.0
    expected_query_scope = _expected_scope(
        symbols=symbols, start=start, end=end, page_size=page_size)
    for _ in range(max_pages):
        before = monotonic()
        if (isinstance(before, bool) or not isinstance(before, (int, float))
                or not math.isfinite(float(before)) or float(before) < started):
            raise ValueError("monotonic clock returned an invalid value")
        elapsed = float(before) - started
        if elapsed >= float(max_wall_seconds):
            stop_reason = "MAX_WALL_SECONDS"
            break
        manifest = contract.frozen_manifest()
        contract.validate_manifest(manifest)
        try:
            page = page_reader(
                conn, symbols=tuple(symbols), start_utc=start, end_utc=end,
                max_capture_age_seconds=manifest["source"]["max_capture_age_seconds"],
                windows=(manifest["labels"]["window_minutes"],),
                thresholds_bps=tuple(manifest["labels"]["thresholds_bps"]),
                page_size=page_size, cursor=cursor,
                absolute_deadline_monotonic=absolute_deadline,
                monotonic=monotonic,
            )
        except source_audit.AuditDeadlineExceeded:
            elapsed = max(0.0, float(monotonic()) - started)
            stop_reason = "MAX_WALL_SECONDS"
            break
        except Exception:
            failed_at = monotonic()
            if (isinstance(failed_at, bool) or not isinstance(failed_at, (int, float))
                    or not math.isfinite(float(failed_at)) or float(failed_at) < started):
                raise ValueError("monotonic clock returned an invalid value")
            if float(failed_at) >= absolute_deadline:
                elapsed = float(failed_at) - started
                stop_reason = "MAX_WALL_SECONDS"
                break
            raise
        if not isinstance(page, Mapping):
            raise ValueError("coverage page must be a mapping")
        pages.append(page)
        cursor = page.get("next_cursor")
        after = monotonic()
        if (isinstance(after, bool) or not isinstance(after, (int, float))
                or not math.isfinite(float(after)) or float(after) < float(before)):
            raise ValueError("monotonic clock returned an invalid value")
        elapsed = float(after) - started
        if elapsed >= float(max_wall_seconds):
            stop_reason = "MAX_WALL_SECONDS"
            break
        if cursor is None:
            break
    if cursor is not None and stop_reason is None:
        stop_reason = "MAX_PAGES"
    truncated = cursor is not None or stop_reason is not None
    attempt_ids: list[int] = []
    result = _aggregate_pages(
        pages, truncated=truncated, stop_reason=stop_reason,
        expected_query_scope=expected_query_scope,
        _attempt_ids_out=attempt_ids)
    result["execution_bounds"] = {
        "page_size": page_size,
        "max_pages": max_pages,
        "max_wall_seconds": float(max_wall_seconds),
        "in_flight_statement_timeout_milliseconds": IN_FLIGHT_STATEMENT_TIMEOUT_MILLISECONDS,
        "wall_budget_semantics": "CHECKED_BETWEEN_AND_WITHIN_SOURCE_QUERIES",
        "elapsed_seconds": elapsed,
    }
    result.pop("receipt_sha256", None)
    result["receipt_sha256"] = contract.digest(result)
    return result, attempt_ids


def run_bounded_audit(
    conn: Any, *, start_utc: Any, end_utc: Any,
    symbols: Sequence[str] = _SYMBOLS,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages: int = DEFAULT_MAX_PAGES,
    max_wall_seconds: float = DEFAULT_MAX_WALL_SECONDS,
    page_reader: Callable[..., Mapping[str, Any]] =
        source_audit.audit_anchor_attempt_page_from_connection,
    monotonic: Callable[[], float] = time.monotonic,
) -> dict:
    """Traverse one bounded caller-owned transaction and return count-only data."""
    result, _ = _run_bounded_audit_with_attempt_ids(
        conn, start_utc=start_utc, end_utc=end_utc, symbols=symbols,
        page_size=page_size, max_pages=max_pages, max_wall_seconds=max_wall_seconds,
        page_reader=page_reader, monotonic=monotonic)
    return result


def _attempt_cohort_handoff_payload(value: Mapping[str, Any]) -> dict:
    """Identity excludes full audit hash and every outcome/candidate value."""
    return {
        "version": ATTEMPT_COHORT_HANDOFF_VERSION,
        "outcome_free_population_receipt_sha256":
            value.get("outcome_free_population_receipt_sha256"),
        "attempt_ids": value.get("attempt_ids"),
    }


def validate_attempt_cohort_handoff(
    value: Mapping[str, Any], *, expected_handoff_sha256: str | None = None,
) -> None:
    """Verify an exact complete same-transaction population; partial is invalid.

    Hashes detect mismatches, not a malicious producer that fabricates every
    input. Callers must retain the read-only runner's provenance and transaction
    boundary; a trusted retained expected hash additionally pins the handoff.
    Full receipt integrity is checked separately, never in selection identity.
    """
    _assert_runtime_contract()
    keys = {"version", "status", "coverage_receipt", "attempt_ids",
            "outcome_free_population_receipt_sha256", "handoff_sha256"}
    if (not isinstance(value, Mapping) or set(value) != keys
            or value.get("version") != ATTEMPT_COHORT_HANDOFF_VERSION
            or value.get("status") != "COMPLETE_BOUNDED_COHORT"):
        raise ValueError("attempt cohort handoff is incomplete or malformed")
    coverage = value.get("coverage_receipt")
    ids = value.get("attempt_ids")
    if (not isinstance(coverage, Mapping) or not isinstance(ids, list)
            or len(ids) > MAX_PAGE_SIZE * MAX_PAGES
            or any(type(item) is not int or not 0 < item <= 9223372036854775807 for item in ids)
            or ids != sorted(set(ids))):
        raise ValueError("attempt cohort handoff IDs are invalid")
    unsigned_receipt = dict(coverage)
    full_hash = unsigned_receipt.pop("receipt_sha256", None)
    if (not isinstance(full_hash, str) or _HASH.fullmatch(full_hash) is None
            or contract.digest(unsigned_receipt) != full_hash):
        raise ValueError("attempt cohort full audit receipt hash mismatch")
    safe_payload = outcome_free_population_payload(coverage)
    population_hash = value.get("outcome_free_population_receipt_sha256")
    if (not isinstance(population_hash, str) or _HASH.fullmatch(population_hash) is None
            or coverage.get("outcome_free_population_receipt_sha256") != population_hash
            or contract.digest(safe_payload) != population_hash
            or safe_payload.get("coverage_receipt_version") != VERSION
            or safe_payload.get("manifest_sha256") != _MANIFEST_SHA256
            or safe_payload.get("manifest_axes_verified") is not True
            or safe_payload.get("status") != "COMPLETE_BOUNDED_COHORT"
            or safe_payload.get("database_connection_attempted") is not True
            or safe_payload.get("stop_reason") is not None):
        raise ValueError("attempt cohort outcome-free receipt is incomplete or mismatched")
    query = safe_payload.get("query_scope")
    if not isinstance(query, Mapping):
        raise ValueError("attempt cohort query is invalid")
    unsigned_query = dict(query)
    query_hash = unsigned_query.pop("query_sha256", None)
    adapter_scope = dict(unsigned_query)
    adapter_scope["version"] = adapter_scope.pop("adapter_version", None)
    if (not isinstance(query_hash, str) or _HASH.fullmatch(query_hash) is None
            or contract.digest(unsigned_query) != query_hash
            or contract.canonical(_normalized_scope(adapter_scope))
            != contract.canonical(unsigned_query)):
        raise ValueError("attempt cohort query hash or axes mismatch")
    high_water = safe_payload.get("high_water_attempt_id")
    transaction_hash = safe_payload.get("transaction_identity_sha256")
    count = safe_payload.get("attempts_examined")
    counts = safe_payload.get("attempt_status_counts")
    if (type(count) is not int or count != len(ids)
            or type(high_water) is not int or high_water != (ids[-1] if ids else 0)
            or not isinstance(transaction_hash, str) or _HASH.fullmatch(transaction_hash) is None
            or not isinstance(counts, Mapping)
            or not set(counts).issubset(_SAFE_STATUSES["attempt"])
            or any(type(number) is not int or number < 0 for number in counts.values())
            or sum(counts.values()) != count):
        raise ValueError("attempt cohort population coverage is incomplete")
    population = {
        "version": "stage8-bounded-attempt-id-population-v1",
        "manifest_sha256": _MANIFEST_SHA256,
        "query_sha256": query_hash,
        "transaction_identity_sha256": transaction_hash,
        "high_water_attempt_id": high_water,
        "attempt_ids": ids,
    }
    if (safe_payload.get("attempt_population_version") != population["version"]
            or safe_payload.get("attempt_population_sha256") != contract.digest(population)):
        raise ValueError("attempt cohort exact attempt population hash mismatch")
    supplied_hash = value.get("handoff_sha256")
    if (not isinstance(supplied_hash, str) or _HASH.fullmatch(supplied_hash) is None
            or contract.digest(_attempt_cohort_handoff_payload(value)) != supplied_hash
            or (expected_handoff_sha256 is not None
                and (not isinstance(expected_handoff_sha256, str)
                     or _HASH.fullmatch(expected_handoff_sha256) is None
                     or supplied_hash != expected_handoff_sha256))):
        raise ValueError("attempt cohort handoff hash mismatch")


def read_bounded_attempt_cohort_from_connection(
    conn: Any, *, start_utc: Any, end_utc: Any,
    symbols: Sequence[str] = _SYMBOLS,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages: int = DEFAULT_MAX_PAGES,
    max_wall_seconds: float = DEFAULT_MAX_WALL_SECONDS,
    page_reader: Callable[..., Mapping[str, Any]] =
        source_audit.audit_anchor_attempt_page_from_connection,
    monotonic: Callable[[], float] = time.monotonic,
) -> dict:
    """Read once and return complete IDs beside the unchanged count-only receipt.

    A partial result carries no usable IDs or handoff identity. Source-level
    UNKNOWN rows remain in complete populations; downstream authority checks
    still decide whether those rows can support any representative.
    """
    coverage, attempt_ids = _run_bounded_audit_with_attempt_ids(
        conn, start_utc=start_utc, end_utc=end_utc, symbols=symbols,
        page_size=page_size, max_pages=max_pages, max_wall_seconds=max_wall_seconds,
        page_reader=page_reader, monotonic=monotonic)
    complete = coverage.get("status") == "COMPLETE_BOUNDED_COHORT"
    result = {
        "version": ATTEMPT_COHORT_HANDOFF_VERSION,
        "status": coverage.get("status"),
        "coverage_receipt": coverage,
        "attempt_ids": attempt_ids if complete else None,
        "outcome_free_population_receipt_sha256":
            coverage.get("outcome_free_population_receipt_sha256"),
        "handoff_sha256": None,
    }
    if complete:
        result["handoff_sha256"] = contract.digest(_attempt_cohort_handoff_payload(result))
        validate_attempt_cohort_handoff(result)
    return result


def run_from_environment(environ: Mapping[str, str] | None = None) -> dict:
    """Opt in with an explicit DSN and bounds; never inspect other URL names."""
    values = os.environ if environ is None else environ
    required = (DATABASE_URL_ENV, START_UTC_ENV, END_UTC_ENV)
    missing = [name for name in required if not str(values.get(name) or "").strip()]
    if missing:
        return _unknown("EXPLICIT_AUDIT_CONFIGURATION_MISSING",
                        missing_configuration=missing)
    try:
        start, end = _utc(values[START_UTC_ENV]), _utc(values[END_UTC_ENV])
    except (TypeError, ValueError, OverflowError):
        return _unknown("AUDIT_TIME_BOUND_INVALID")
    if start >= end:
        return _unknown("AUDIT_TIME_RANGE_INVALID")

    try:
        _assert_runtime_contract()
    except Exception as exc:
        return {
            **_unknown("RUNTIME_CONTRACT_MISMATCH"),
            "status": "QUERY_FAILED",
            "error_type": type(exc).__name__,
        }

    # Lazy import keeps ordinary tests and runtime imports disconnected from
    # PostgreSQL.  The DSN is passed directly and is never copied to output.
    try:
        import psycopg
        from psycopg.conninfo import conninfo_to_dict
        from psycopg.rows import dict_row
    except Exception as exc:
        return {
            **_unknown("DATABASE_DRIVER_UNAVAILABLE"),
            "status": "QUERY_FAILED",
            "error_type": type(exc).__name__,
        }
    try:
        connection_fields = _explicit_connection_fields(
            values[DATABASE_URL_ENV], conninfo_to_dict)
    except Exception as exc:
        return {
            **_unknown("EXPLICIT_DATABASE_CONNECTION_FIELDS_INVALID"),
            "error_type": type(exc).__name__,
        }
    try:
        with psycopg.connect(
            **connection_fields, autocommit=False, row_factory=dict_row,
            connect_timeout=5,
            options=("-c default_transaction_read_only=on "
                     f"-c statement_timeout={IN_FLIGHT_STATEMENT_TIMEOUT_MILLISECONDS} "
                     "-c lock_timeout=1000 "
                     "-c idle_in_transaction_session_timeout=30000"),
        ) as conn:
            conn.read_only = True
            conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
            result = run_bounded_audit(conn, start_utc=start, end_utc=end)
            conn.rollback()
            return result
    except Exception as exc:  # Deliberately omit message/DSN from the receipt.
        return {
            **_unknown("DATABASE_AUDIT_FAILED"),
            "status": "QUERY_FAILED",
            "database_connection_attempted": True,
            "error_type": type(exc).__name__,
        }


def main() -> int:
    receipt = run_from_environment()
    print(json.dumps(receipt, sort_keys=True, separators=(",", ":")))
    return {
        "COMPLETE_BOUNDED_COHORT": 0,
        "QUERY_FAILED": 2,
        "UNKNOWN_NOT_QUERIED": 3,
        "BOUNDED_PARTIAL": 4,
    }.get(receipt.get("status"), 2)


if __name__ == "__main__":
    raise SystemExit(main())
