"""Outcome-blind catalog grids bound to finite, pre-registered cohort calendars.

A search owns one explicit finite scope denominator per calendar window. It does
not split oversized grids, pool windows, invent supported features or authorize
delivery. Existing catalog aliases remain separate, visibly overlapping trials.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
from pathlib import Path

import research_no_horizon_calendar as calendar
import research_no_horizon_cohort as cohort
import research_no_horizon_contract as contracts
import research_no_horizon_experiment as experiment
import research_no_horizon_ranking as ranking
import research_watch_scan_formula as formulas

VERSION = "no-horizon-frozen-catalog-discovery-plan-v1"
_AUTHORITY = {"runtime_authorized": False, "telegram_authorized": False,
              "trading_authorized": False}
_LIMITATIONS = {
    "is_prospective_formula_evidence": False,
    "validated_discovery": False,
    "multiple_testing_adjusted": False,
    "scope_pooling": False,
    "cross_window_outcome_pooling": False,
    "cross_window_parent_independence_proven": False,
    "catalog_entries_are_independent_strategies": False,
}


def implementation():
    return {"planner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "ranking": ranking.implementation(), "calendar": calendar.implementation()}


def _directions(values):
    if not isinstance(values, (list, tuple)) or not 1 <= len(values) <= len(formulas.DIRECTIONS):
        raise ValueError("DISCOVERY_EXPLICIT_BASE_DIRECTIONS_REQUIRED")
    if any(not isinstance(value, str) or value not in formulas.DIRECTIONS for value in values):
        raise ValueError("DISCOVERY_INVALID_BASE_DIRECTION")
    if len(set(values)) != len(values):
        raise ValueError("DISCOVERY_DUPLICATE_BASE_DIRECTION")
    return sorted(values)


def _thresholds(values):
    if not isinstance(values, (list, tuple)) or not 1 <= len(values) <= experiment.MAX_SCOPES:
        raise ValueError("DISCOVERY_EXPLICIT_THRESHOLDS_REQUIRED")
    normalized = [contracts.number(value, "threshold_pct") for value in values]
    if any(not 0 < value < 100 for value in normalized):
        raise ValueError("DISCOVERY_THRESHOLD_OUT_OF_BOUNDS")
    if len(set(normalized)) != len(normalized):
        raise ValueError("DISCOVERY_DUPLICATE_NORMALIZED_THRESHOLD")
    return sorted(normalized)


def _catalog(candidate_keys):
    records = formulas.catalog_records()
    by_key = {record["candidate_key"]: record for record in records}
    supported = {key for key, record in by_key.items() if record["supported"]}
    if candidate_keys is None:
        selected = sorted(supported)
        mode = "ALL_SUPPORTED"
    else:
        if not isinstance(candidate_keys, (list, tuple)) or not candidate_keys:
            raise ValueError("DISCOVERY_EXPLICIT_NONEMPTY_CANDIDATE_SELECTION_REQUIRED")
        if any(not isinstance(key, str) or key not in supported for key in candidate_keys):
            raise ValueError("DISCOVERY_UNKNOWN_OR_UNSUPPORTED_CANDIDATE")
        if len(set(candidate_keys)) != len(candidate_keys):
            raise ValueError("DISCOVERY_DUPLICATE_CANDIDATE")
        selected = sorted(candidate_keys)
        mode = "EXPLICIT_SUBSET"
    if not selected:
        raise ValueError("DISCOVERY_EMPTY_SUPPORTED_CATALOG")
    chosen = set(selected)
    manifest = [{**{field: deepcopy(record[field]) for field in (
                    "candidate_key", "definition_sha256", "orientation",
                    "supported", "unsupported_features")},
                 "selected": record["candidate_key"] in chosen}
                for record in sorted(records, key=lambda row: row["candidate_key"])]
    return selected, mode, manifest


def build_plan(first_declaration, *, base_directions, thresholds_pct,
               candidate_keys=None, window_count=1):
    """Freeze catalog coverage and the exact grid before any source/price read.

    The supplied template is validated intact, then copied with a new scope grid
    and new identity. Neither the template nor an existing frozen experiment is
    changed. The existing calendar performs all time and window-count checks.
    """
    template = cohort.normalize_declaration(first_declaration)
    directions = _directions(base_directions)
    thresholds = _thresholds(thresholds_pct)
    selected, mode, manifest = _catalog(candidate_keys)
    scope_count = len(selected) * len(directions) * len(thresholds)
    if scope_count > experiment.MAX_SCOPES:
        raise ValueError("DISCOVERY_GRID_EXCEEDS_SINGLE_COHORT_SCOPE_LIMIT")
    grid = [{"candidate_key": key, "base_direction": direction, "threshold_pct": threshold}
            for key in selected for direction in directions for threshold in thresholds]
    versions = implementation()
    selection = {"template_declaration": template, "base_directions": directions,
                 "thresholds_pct": thresholds, "candidate_keys": selected,
                 "selection_mode": mode, "catalog_manifest": manifest}
    grid_declaration = deepcopy(template)
    grid_declaration["scopes"] = grid
    grid_declaration["cohort_key"] = "discovery:" + contracts.digest({
        "version": VERSION, "selection": selection, "implementation": versions})
    calendar_plan = calendar.build_plan(grid_declaration, window_count=window_count)
    # Detect a changed dependency between the two captures before emitting a plan.
    if calendar_plan["implementation"] != versions["calendar"]:
        raise ValueError("DISCOVERY_CALENDAR_IMPLEMENTATION_CHANGED")
    total_supported = sum(record["supported"] for record in manifest)
    value = {"version": VERSION, **selection, "calendar_plan": calendar_plan,
             "implementation": versions,
             "summary": {
                 "catalog_candidates": len(manifest),
                 "supported_candidates": total_supported,
                 "selected_candidates": len(selected),
                 "omitted_supported_candidates": total_supported - len(selected),
                 "unsupported_candidates": len(manifest) - total_supported,
                 "scopes_per_window": scope_count,
                 "window_count": calendar_plan["window_count"],
                 "total_declared_scopes": scope_count * calendar_plan["window_count"],
                 "max_scopes_per_window": experiment.MAX_SCOPES,
                 "max_source_scope_decisions": cohort.MAX_DECISIONS,
                 "max_source_rows_from_decision_budget": cohort.MAX_DECISIONS // scope_count,
                 "max_actual_source_rows_per_window": min(
                     cohort.MAX_SOURCE_ROWS, cohort.MAX_DECISIONS // scope_count,
                     sum(part["source_row_limit"] for part in template["parts"])),
             },
             "representative_selection":
                 "GLOBAL_EARLIEST_DECISION_THEN_ENTRY_ID_WITHOUT_OUTCOMES_V1",
             "catalog_scope": "EXISTING_ACCEPTED_WATCH_SUPPORTED_PREDICATES_ONLY",
             "source_reads_performed": 0, "outcome_reads_performed": 0,
             **_LIMITATIONS, **_AUTHORITY}
    return {**value, "plan_sha256": contracts.digest(value)}


def validate_plan(value):
    """Require the complete canonical plan under the exact current versions."""
    required = {"template_declaration", "base_directions", "thresholds_pct",
                "candidate_keys", "selection_mode", "calendar_plan"}
    if not isinstance(value, dict) or not required <= set(value):
        raise ValueError("COMPLETE_DISCOVERY_PLAN_REQUIRED")
    mode = value["selection_mode"]
    if mode not in ("ALL_SUPPORTED", "EXPLICIT_SUBSET"):
        raise ValueError("DISCOVERY_INVALID_SELECTION_MODE")
    if not isinstance(value["calendar_plan"], dict) or "window_count" not in value["calendar_plan"]:
        raise ValueError("COMPLETE_DISCOVERY_CALENDAR_REQUIRED")
    rebuilt = build_plan(value["template_declaration"],
                         base_directions=value["base_directions"],
                         thresholds_pct=value["thresholds_pct"],
                         candidate_keys=None if mode == "ALL_SUPPORTED" else value["candidate_keys"],
                         window_count=value["calendar_plan"]["window_count"])
    if contracts.canonical(value) != contracts.canonical(rebuilt):
        raise ValueError("DISCOVERY_PLAN_OR_IMPLEMENTATION_MISMATCH")
    return rebuilt


def register_plan(backend, value):
    """Validate the whole search before existing calendar registration writes."""
    plan = validate_plan(value)
    registered = calendar.register_plan(backend, plan["calendar_plan"])
    if (registered["plan_sha256"] != plan["calendar_plan"]["plan_sha256"]
            or registered["registration_complete"] is not True):
        raise ValueError("DISCOVERY_CALENDAR_REGISTRATION_BINDING_MISMATCH")
    receipt = {"version": VERSION, "plan_sha256": plan["plan_sha256"],
               "calendar_plan_sha256": plan["calendar_plan"]["plan_sha256"],
               "calendar_registration": registered, "registration_complete": True,
               **_LIMITATIONS, **_AUTHORITY}
    return {**receipt, "receipt_sha256": contracts.digest(receipt)}
