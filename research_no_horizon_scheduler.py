"""Bounded direct-PostgreSQL execution of the original eight research requests.

This orchestration layer does not register populations, install schemas, alter
frozen modules, open source connections itself, publish signals or trade.
The caller owns process locking, durable storage, source authentication, the
native-transport adoption seal, clean shutdown and the next scheduled tick.
"""
from __future__ import annotations

from copy import deepcopy
import json

import research_no_horizon_acquisition_store as acquisition
import research_no_horizon_contract as contracts
import research_no_horizon_postgres_store as executor
import research_no_horizon_selection as selection
from research_no_horizon_store import _bound, _name

VERSION = "no-horizon-original-eight-native-scheduler-v1"
TRANSPORT = "DIRECT_POSTGRES_NATIVE"
_AUTHORITY = {"runtime_authorized": False, "telegram_authorized": False,
              "trading_authorized": False}
_REQUEST_COUNT = 8
_GROUP_COUNT = 4
MAX_SELECTION_REPORT_BYTES = 128 * 1024 * 1024


def _same(left, right):
    return contracts.canonical(left) == contracts.canonical(right)


def verify_original_registrations(store, registrations):
    """Verify the whole original registry before any mutable unit of work.

    This deliberately uses the same native compatibility checks as the frozen
    evidence driver. A supplied verifier may instead call that original driver.
    The trusted checkpoint, not this function, establishes preregistration truth.
    """
    if len(registrations) != _REQUEST_COUNT:
        raise ValueError("SCHEDULER_REQUIRES_ORIGINAL_EIGHT_REGISTRATIONS")
    if acquisition.schema_status(store.connection)["compatible"] is not True:
        raise ValueError("SCHEDULER_NATIVE_SCHEMA_INCOMPATIBLE")
    with store.connection.transaction():
        rows = store.connection.execute(
            "SELECT request_id FROM research_no_horizon_acquisition_requests ORDER BY request_id"
        ).fetchall()
    if {row["request_id"] for row in rows} != set(registrations):
        raise ValueError("SCHEDULER_UNRELATED_OR_MISSING_REGISTRATION")
    for request_id, item in registrations.items():
        if not _same(store.registration_info(request_id), item["registration"]):
            raise ValueError("SCHEDULER_ORIGINAL_REGISTRATION_CHANGED")
        with store.connection.transaction():
            native = store._request(request_id, compatible=True)
        if not _same(json.loads(native["declaration_json"]), item["declaration"]):
            raise ValueError("SCHEDULER_ORIGINAL_DECLARATION_CHANGED")


def _context(plans, registrations):
    """Bind all four validated plans to their exact ordered original requests."""
    if (not isinstance(plans, dict) or len(plans) != _GROUP_COUNT
            or not isinstance(registrations, dict) or len(registrations) != _REQUEST_COUNT):
        raise ValueError("SCHEDULER_COMPLETE_ORIGINAL_CONTEXT_REQUIRED")
    covered = []
    for group, definition in plans.items():
        plan = selection.validate_plan(definition["plan"])
        ids = definition["requests"]
        windows = plan["discovery_plan"]["calendar_plan"]["windows"]
        if not isinstance(ids, list) or len(ids) != 2 or len(windows) != 2:
            raise ValueError("SCHEDULER_EXACT_TWO_WINDOWS_REQUIRED")
        if definition["receipt"].get("plan_sha256") != plan["plan_sha256"]:
            raise ValueError("SCHEDULER_ORIGINAL_PLAN_RECEIPT_MISMATCH")
        for ordinal, (request_id, window) in enumerate(zip(ids, windows, strict=True)):
            item = registrations[request_id]
            registration = item["registration"]
            if (item["group"] != group or item["window_ordinal"] != ordinal
                    or registration["request_id"] != request_id
                    or registration["registered_before_source_start"] is not True
                    or registration["declaration_sha256"] != contracts.digest(item["declaration"])
                    or not _same(item["declaration"], window["declaration"])):
                raise ValueError("SCHEDULER_ORIGINAL_WINDOW_BINDING_MISMATCH")
            covered.append(request_id)
    if len(set(covered)) != _REQUEST_COUNT or set(covered) != set(registrations):
        raise ValueError("SCHEDULER_ORIGINAL_REQUEST_DENOMINATOR_MISMATCH")
    # Stable source-window-first order prevents caller mapping order affecting
    # progress. The persisted cursor fairly rotates even when budgets are small.
    return sorted(registrations, key=lambda request_id: (
        registrations[request_id]["window_ordinal"],
        registrations[request_id]["group"], request_id))


def _compact_request(report):
    return {key: report.get(key) for key in (
        "request_id", "status", "executor_plan_id", "proof_count",
        "leaves_completed", "total_leaves", "not_before_utc", "report_sha256")}


def run_tick(connection, plans, registrations, *, source_connection_factory,
             worker_id, provenance_verifier, verifier=None, cursor=0,
             request_budget=8, acquisition_leaf_budget=2, execution_passes=1,
             lease_seconds=120, candle_budget=1024, entry_budget=128,
             batch_size=128, cancelled=None):
    """Advance a bounded fair subset and compute each available native selector.

    ``plans`` and ``registrations`` are the trusted ``checkpoint_context`` values.
    ``provenance_verifier(store, registrations)`` must return exactly True after
    checking the caller's durable DIRECT_POSTGRES_NATIVE adoption seal. That seal
    may initially adopt only the zero-proof original registry. Existing external
    proofs must have their original transport linkage independently verified;
    silently relabeling them as native transport is forbidden. Native proof rows
    themselves have no transport-origin field, so this cannot be inferred here.

    Source authentication/read-only connection settings belong to the explicit
    factory. Native acquisition calls it only after a database-clock due claim.
    Operational failures yield sanitized error types and remain retryable under
    native semantics; native scientific rejections remain terminal BLOCKED.
    Identity/provenance verification failures abort before any work. Cancellation
    is checked between bounded units; in-flight native transactions finish.
    """
    for value, name, maximum in (
        (request_budget, "request_budget", _REQUEST_COUNT),
        (acquisition_leaf_budget, "acquisition_leaf_budget", 32),
        (execution_passes, "execution_passes", 32),
        (lease_seconds, "lease_seconds", 3600),
        (candle_budget, "candle_budget", 65536),
        (entry_budget, "entry_budget", 1024),
        (batch_size, "batch_size", 1024),
    ):
        _bound(value, name, maximum)
    _name(worker_id, "worker_id")
    if type(cursor) is not int or not 0 <= cursor < _REQUEST_COUNT:
        raise ValueError("SCHEDULER_INVALID_FAIR_CURSOR")
    if not callable(source_connection_factory) or not callable(provenance_verifier):
        raise ValueError("SCHEDULER_EXPLICIT_SOURCE_AND_PROVENANCE_VERIFIER_REQUIRED")
    if verifier is not None and not callable(verifier):
        raise ValueError("SCHEDULER_INVALID_REGISTRATION_VERIFIER")
    if cancelled is not None and not callable(cancelled):
        raise ValueError("SCHEDULER_INVALID_CANCELLATION_CALLBACK")
    stop = cancelled or (lambda: False)
    # Snapshot caller context before callbacks run. This is not a substitute for
    # filesystem locking, which is owned by the durable process wrapper.
    plans, registrations = deepcopy(plans), deepcopy(registrations)
    ordered = _context(plans, registrations)
    intake = acquisition.AcquisitionStore(connection)
    (verifier or verify_original_registrations)(intake, registrations)
    if provenance_verifier(intake, registrations) is not True:
        raise ValueError("SCHEDULER_NATIVE_TRANSPORT_PROVENANCE_NOT_VERIFIED")
    backend = executor.PostgresCohortStore(connection)
    result = {"version": VERSION, "source_transport": TRANSPORT,
        "original_registration_times_preserved": True,
        "request_denominator": _REQUEST_COUNT, "group_denominator": _GROUP_COUNT,
        "cursor": cursor, "next_cursor": cursor, "visited_request_ids": [],
        "requests": [], "groups": [], "errors": [], "cancelled": False,
        "group_or_window_pooling": False, "automatic_schedule_created": False,
        "source_origin_authenticated_by_hashes": False, **_AUTHORITY}
    for offset in range(request_budget):
        if stop():
            result["cancelled"] = True
            break
        index = (cursor + offset) % _REQUEST_COUNT
        request_id = ordered[index]
        result["next_cursor"] = (index + 1) % _REQUEST_COUNT
        result["visited_request_ids"].append(request_id)
        report = intake.report(request_id)
        step = {"request_id": request_id, "acquisition_claimed": False,
                "execution_steps": []}
        if report["status"] not in ("ADMITTED", "BLOCKED"):
            try:
                acquired = intake.run_once(worker_id,
                    source_connection_factory=source_connection_factory,
                    request_id=request_id, lease_seconds=lease_seconds,
                    leaf_budget=acquisition_leaf_budget)
                step["acquisition_claimed"] = acquired is not None
                if acquired is not None:
                    step["acquisition"] = {key: acquired.get(key) for key in (
                        "request_id", "status", "plan_id", "proof_count", "queries_this_run")}
            except Exception as exc:
                # Native acquisition records/release semantics determine whether
                # this request is retryable. Never log SQL, payloads or a DSN.
                result["errors"].append({"request_id": request_id,
                    "stage": "ACQUISITION", "error_type": type(exc).__name__})
            report = intake.report(request_id)
        if report["status"] == "ADMITTED":
            for _ in range(execution_passes):
                if stop():
                    result["cancelled"] = True
                    break
                try:
                    executed = backend.run_once(worker_id,
                        plan_id=report["executor_plan_id"], lease_seconds=lease_seconds,
                        candle_budget=candle_budget, entry_budget=entry_budget,
                        batch_size=batch_size)
                except Exception as exc:
                    result["errors"].append({"request_id": request_id,
                        "stage": "EXECUTION", "error_type": type(exc).__name__})
                    break
                if executed is None:
                    break
                step["execution_steps"].append({key: executed.get(key) for key in (
                    "plan_id", "scope_ordinal", "status", "candle_evaluations_this_run")})
        step["request"] = _compact_request(report)
        result["requests"].append(step)
        if result["cancelled"]:
            break
    for group in sorted(plans):
        if result["cancelled"] or stop():
            result["cancelled"] = True
            break
        definition = plans[group]
        rows = [intake.report(request_id) for request_id in definition["requests"]]
        item = {"group": group, "requests": [_compact_request(row) for row in rows]}
        if any(row["status"] == "BLOCKED" for row in rows):
            item["status"] = "BLOCKED_ACQUISITION"
        elif not all(row["status"] == "ADMITTED" for row in rows):
            item["status"] = "WAITING_FOR_BOTH_WINDOW_REPORTS"
        else:
            # Native selection revalidates all reports and suppresses selections
            # until both whole-window calculations are complete. Never substitute
            # externally supplied rankings, aggregate percentages or a shortlist.
            reports, report_bytes = [], 0
            for row in rows:
                report = backend.report(row["executor_plan_id"])
                report_bytes += len(contracts.canonical(report).encode("utf-8"))
                if report_bytes > MAX_SELECTION_REPORT_BYTES:
                    raise ValueError("SCHEDULER_SELECTION_REPORT_BYTE_LIMIT_EXCEEDED")
                reports.append(report)
            selected = selection.select_reports(definition["plan"], reports)
            item.update(status="SELECTED" if selected["selection_complete"] else "INCOMPLETE",
                native_selection=selected,
                ordered_executor_report_sha256=[report["report_sha256"] for report in reports],
                separate_original_registration_receipt=definition["receipt"],
                registration_linkage=[{"request_id": row["request_id"],
                    "executor_plan_id": row["executor_plan_id"],
                    "registration": registrations[row["request_id"]]["registration"]} for row in rows])
        result["groups"].append(item)
    result["selection_complete"] = (len(result["groups"]) == _GROUP_COUNT
        and all(group["status"] == "SELECTED" for group in result["groups"]))
    return {**result, "tick_sha256": contracts.digest(result)}
