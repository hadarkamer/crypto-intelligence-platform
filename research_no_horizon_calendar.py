"""Finite UTC cohort calendars; registration is separate from outcome validation.

All windows are fixed together before any registration. Adjacent source windows
remain separate experiments: BTC parents may cross their boundaries. The planner
does not pool evidence, select winners, reschedule windows, or start a service.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import timedelta
import hashlib
from pathlib import Path

import research_no_horizon_acquisition_store as acquisition
import research_no_horizon_cohort as cohort
import research_no_horizon_contract as contracts

VERSION = "no-horizon-finite-cohort-calendar-v1"
MAX_WINDOWS = 32
MAX_CALENDAR_DAYS = 366
_AUTHORITY = {"runtime_authorized": False, "telegram_authorized": False,
              "trading_authorized": False}
_LIMITATIONS = {
    "is_prospective_formula_evidence": False,
    "validated_discovery": False,
    "multiple_testing_adjusted": False,
    "scope_pooling": False,
    "cross_window_outcome_pooling": False,
    "cross_window_parent_independence_proven": False,
}


def implementation():
    return {"planner_version": VERSION,
            "planner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "acquisition": acquisition.implementation()}


def build_plan(first_declaration, *, window_count):
    """Freeze adjacent equal-duration windows without reading source or outcomes."""
    if type(window_count) is not int or not 1 <= window_count <= MAX_WINDOWS:
        raise ValueError("CALENDAR_WINDOW_COUNT_OUT_OF_BOUNDS")
    first = cohort.normalize_declaration(first_declaration)
    start = contracts.utc(first["source_start_utc"])
    duration = contracts.utc(first["source_end_utc"]) - start
    if contracts.utc(first["declared_at_utc"]) > start:
        raise ValueError("CALENDAR_DECLARATION_AFTER_FIRST_SOURCE_START")
    final_cutoff = contracts.utc(first["cutoff_utc"]) + duration * (window_count - 1)
    if final_cutoff - start > timedelta(days=MAX_CALENDAR_DAYS):
        raise ValueError("CALENDAR_TOTAL_SPAN_EXCEEDED")
    seed = contracts.digest(first)
    windows = []
    for ordinal in range(window_count):
        declaration = deepcopy(first)
        delta = duration * ordinal
        # The seed fixes the complete original declaration. Ordinal zero also
        # gets a new identity, so a calendar never overwrites its template.
        declaration["cohort_key"] = f"calendar:{seed}:{ordinal:02d}"
        for key in ("source_start_utc", "source_end_utc", "cutoff_utc"):
            declaration[key] = (contracts.utc(first[key]) + delta).isoformat()
        for part, original in zip(declaration["parts"], first["parts"]):
            for key in ("source_start_utc", "source_end_utc"):
                part[key] = (contracts.utc(original[key]) + delta).isoformat()
        declaration = cohort.normalize_declaration(declaration)
        windows.append({"ordinal": ordinal, "declaration": declaration,
                        "declaration_sha256": contracts.digest(declaration)})
    result = {"version": VERSION, "first_declaration": first,
              "window_count": window_count, "windows": windows,
              "implementation": implementation(),
              "cadence": "ADJACENT_EQUAL_UTC_SOURCE_INTERVALS",
              "registration_policy": "DATABASE_CREATED_AT_NOT_AFTER_SOURCE_START",
              "max_windows": MAX_WINDOWS, "max_calendar_days": MAX_CALENDAR_DAYS,
              **_LIMITATIONS, **_AUTHORITY}
    return {**result, "plan_sha256": contracts.digest(result)}


def validate_plan(value):
    """Require the entire canonical plan and its current exact implementations."""
    if not isinstance(value, dict) or not {"first_declaration", "window_count"} <= set(value):
        raise ValueError("COMPLETE_CALENDAR_PLAN_REQUIRED")
    rebuilt = build_plan(value["first_declaration"], window_count=value["window_count"])
    if contracts.canonical(value) != contracts.canonical(rebuilt):
        raise ValueError("CALENDAR_PLAN_OR_IMPLEMENTATION_MISMATCH")
    return rebuilt


def register_plan(backend, value):
    """Register every window in order; exact earlier writes survive a retry.

    Each existing acquisition registration owns its transaction. Failure stops
    this call without skipping a window; retry the same complete plan. A source
    window that was not registered by its start cannot be backdated on retry.
    """
    plan = validate_plan(value)
    if contracts.canonical(backend.versions) != contracts.canonical(plan["implementation"]["acquisition"]):
        raise ValueError("CALENDAR_BACKEND_IMPLEMENTATION_MISMATCH")
    registrations = []
    for window in plan["windows"]:
        declaration = window["declaration"]
        request_id = backend.register_request(declaration, require_before_start=True)
        info = backend.registration_info(request_id)
        # Check the compact immutable registration evidence, never an outcome.
        if (info.get("request_id") != request_id
                or info.get("declaration_sha256") != window["declaration_sha256"]
                or info.get("registered_before_source_start") is not True
                or contracts.utc(info["source_start_utc"]) != contracts.utc(declaration["source_start_utc"])
                or contracts.utc(info["created_at_utc"]) > contracts.utc(declaration["source_start_utc"])):
            raise ValueError("CALENDAR_REGISTRATION_BINDING_MISMATCH")
        registrations.append({"ordinal": window["ordinal"], **info})
    receipt = {"version": VERSION, "plan_sha256": plan["plan_sha256"],
               "window_count": plan["window_count"], "registrations": registrations,
               "all_windows_registered_before_source_start": True,
               "registration_complete": True, **_LIMITATIONS, **_AUTHORITY}
    return {**receipt, "receipt_sha256": contracts.digest(receipt)}
