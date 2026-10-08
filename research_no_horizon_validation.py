"""One registered future cohort for exact selected no-horizon research scopes.

Training selection and original validation outcomes are reverified before an
outcome-blind whole-parent freshness filter. Database registration time and the
acquisition-to-executor link are mandatory. No result authorizes delivery or
trading; hashes bind content, not the truth of external market observations.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
from pathlib import Path

import research_no_horizon_acquisition_store as acquisition
import research_no_horizon_cohort as cohort
import research_no_horizon_contract as contracts
from research_no_horizon_experiment import _scopes
import research_no_horizon_first_touch as first_touch
import research_no_horizon_gate as gate
import research_no_horizon_postgres_store as executor
import research_no_horizon_ranking as ranking
import research_no_horizon_selection as selection

VERSION = "no-horizon-registered-fresh-parent-validation-v1"
POLICY_VERSION = "no-horizon-whole-parent-prospective-gate-v1"
_AUTHORITY = {"runtime_authorized": False, "telegram_authorized": False, "trading_authorized": False}
_LIMITATIONS = {"validated_discovery": False, "multiple_testing_adjusted": False,
    "scope_pooling": False, "window_pooling": False, "statistical_independence_proven": False,
    "profitability_validated": False, "source_provenance_verified_by_this_tool": False}
_require, _same = ranking._require, ranking._same


def implementation():
    return {"validator_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "selection": selection.implementation(), "ranking": ranking.implementation(),
        "acquisition": acquisition.implementation()}


def policy():
    return {"policy_version": POLICY_VERSION,
        "freshness": "PARENT_START_AT_OR_AFTER_VALIDATION_START_AND_ID_ABSENT_FROM_ALL_TRAINING_MATCHES",
        "selection": "KEEP_ORIGINAL_EARLIEST_REPRESENTATIVE_EXCLUDE_WHOLE_PARENTS_WITHOUT_OUTCOMES",
        "gate": "UNCHANGED_DECLARED_ATOMIC_GATE_ON_COMPLETE_FRESH_SURVIVORS",
        "old_parent_missing_outcome": "EXCLUDABLE_ONLY_AFTER_COMPLETE_ORIGINAL_REPORT_VERIFICATION",
        "input_blocked": "NEVER_SALVAGE_ABSENT_SELECTED_ENTRY_OUTCOMES",
        "completion": "EVERY_DECLARED_SCOPE_TERMINAL_BEFORE_ANY_FINAL_QUALIFICATION",
        "registration": "DATABASE_CREATED_AT_BETWEEN_TRAINING_MAX_CUTOFF_AND_VALIDATION_START",
        "statistical_claim": "DESCRIPTIVE_PROSPECTIVE_RESEARCH_GATE"}


def _seal(value, field):
    return {**value, field: contracts.digest(value)}


def build_plan(first_declaration, *, selection_plan, selection_reports):
    chosen = selection.select_reports(selection_plan, selection_reports)
    _require(chosen["selection_complete"] is True and bool(chosen["selected_scopes"]),
             "VALIDATION_COMPLETE_NONEMPTY_SELECTION_REQUIRED")
    template = cohort.normalize_declaration(first_declaration)
    training = selection_plan["template_declaration"]
    _require(template["prior_outcomes_observed"] is False, "VALIDATION_FUTURE_OUTCOMES_MUST_BE_UNOBSERVED")
    _require(all(_same(template[key], training[key]) for key in ("symbol", "price_route", "gate_policy")),
             "VALIDATION_TRAINING_ROUTE_SYMBOL_OR_GATE_CHANGED")
    windows = [{"ordinal": ordinal, "source_report_sha256": report["report_sha256"],
        "source_plan_id": report["plan_id"], "cutoff_utc": report["coverage_receipt"]["cutoff_utc"]}
        for ordinal, report in enumerate(selection_reports)]
    maximum = max(contracts.utc(window["cutoff_utc"]) for window in windows)
    start = contracts.utc(template["source_start_utc"])
    _require(maximum < start and contracts.utc(template["declared_at_utc"]) <= start,
             "VALIDATION_MUST_START_AFTER_ALL_TRAINING_CUTOFFS")
    versions, frozen_policy = implementation(), policy()
    declaration = deepcopy(template)
    declaration["scopes"] = [{key: scope[key] for key in
        ("candidate_key", "base_direction", "threshold_pct")} for scope in chosen["selected_scopes"]]
    declaration["cohort_key"] = "validation:" + contracts.digest({"version": VERSION,
        "template_declaration": template, "selection_report_sha256": chosen["report_sha256"],
        "policy": frozen_policy, "implementation": versions})
    declaration = cohort.normalize_declaration(declaration)
    identity = {"version": acquisition.VERSION, "declaration_sha256": contracts.digest(declaration),
        "implementation_sha256": contracts.digest(versions["acquisition"]),
        "implementation": versions["acquisition"]}
    result = {"version": VERSION, "template_declaration": template, "declaration": declaration,
        "declaration_sha256": contracts.digest(declaration), "policy": frozen_policy,
        "implementation": versions, "selection_plan_sha256": selection_plan["plan_sha256"],
        "selection_report_sha256": chosen["report_sha256"], "selected_scopes": _scopes(declaration["scopes"]),
        "known_matched_training_parent_ids": chosen["known_matched_parent_ids"],
        "training_windows": windows, "training_max_cutoff_utc": maximum.isoformat(),
        "training_prior_outcomes_observed": chosen["prior_outcomes_observed"],
        "prior_outcomes_observed": False, "request_identity": identity,
        "request_id": contracts.digest(identity), **_LIMITATIONS, **_AUTHORITY}
    return _seal(result, "plan_sha256")


def validate_plan(value, *, selection_plan, selection_reports):
    try:
        rebuilt = build_plan(value["template_declaration"], selection_plan=selection_plan,
                             selection_reports=selection_reports)
        _require(_same(value, rebuilt), "VALIDATION_PLAN_OR_IMPLEMENTATION_MISMATCH")
        return rebuilt
    except (KeyError, TypeError, IndexError, OverflowError) as exc:
        raise ValueError("VALIDATION_MALFORMED_PLAN") from exc


def _registration(plan, info):
    start = contracts.utc(plan["declaration"]["source_start_utc"])
    created = contracts.utc(info["created_at_utc"])
    _require(info["request_id"] == plan["request_id"] and
        info["declaration_sha256"] == plan["declaration_sha256"] and
        info["implementation_sha256"] == plan["request_identity"]["implementation_sha256"] and
        info["registered_before_source_start"] is True and contracts.utc(info["source_start_utc"]) == start and
        contracts.utc(plan["training_max_cutoff_utc"]) <= created <= start,
        "VALIDATION_REGISTRATION_IDENTITY_OR_TIMING_MISMATCH")
    return {"registration_info": info, "training_max_cutoff_utc": plan["training_max_cutoff_utc"],
        "registration_not_before_training_cutoff": True, "registration_not_after_source_start": True}


def _acquisition(plan, report):
    ranking._sealed(report, "report_sha256")
    ranking._false(report, _AUTHORITY)
    declared = plan["declaration"]
    _require(report["version"] == acquisition.VERSION and report["request_id"] == plan["request_id"] and
        _same(report["identity"], plan["request_identity"]) and _same(report["declaration"], declared) and
        contracts.utc(report["not_before_utc"]) == max(contracts.utc(declared["declared_at_utc"]),
                                                     contracts.utc(declared["cutoff_utc"])),
        "VALIDATION_ACQUISITION_IDENTITY_MISMATCH")
    status, terminal = report["status"], report["terminal_receipt"]
    _require(status in ("WAITING", "FETCHING", "READY", "ADMITTED", "BLOCKED"),
             "VALIDATION_UNSUPPORTED_ACQUISITION_STATUS")
    if status in ("ADMITTED", "BLOCKED"):
        _require(isinstance(terminal, dict) and terminal["version"] == acquisition.VERSION and
            terminal["request_id"] == plan["request_id"] and terminal["status"] == status and
            terminal["declaration_sha256"] == plan["declaration_sha256"] and
            terminal["anchor_sha256"] == report["anchor_sha256"] and
            terminal["executor_plan_id"] == report["executor_plan_id"], "VALIDATION_TERMINAL_LINK_MISMATCH")
        ranking._false(terminal, _AUTHORITY)
        # BLOCKED reports can contain the store's compact projection of rejected
        # proofs. They never qualify and their stored full receipt hash is retained.
        if status == "ADMITTED" or "receipt_projection" not in terminal:
            ranking._sealed(terminal, "receipt_sha256")
        else:
            ranking._hash(terminal["receipt_sha256"])
    else:
        _require(terminal is None, "VALIDATION_UNFINISHED_ACQUISITION_HAS_RECEIPT")
    if status == "ADMITTED":
        ranking._hash(report["anchor_sha256"])
        ranking._hash(report["executor_plan_id"])
        _require("receipt_projection" not in terminal, "VALIDATION_ADMISSION_PROJECTION_UNSUPPORTED")
    else:
        _require(report["executor_plan_id"] is None, "VALIDATION_UNADMITTED_EXECUTOR_LINK")
    return report


def _backend(backend, expected):
    _require(_same(backend.versions, expected), "VALIDATION_BACKEND_IMPLEMENTATION_MISMATCH")


def register_plan(backend, value, *, selection_plan, selection_reports):
    plan = validate_plan(value, selection_plan=selection_plan, selection_reports=selection_reports)
    _backend(backend, plan["implementation"]["acquisition"])
    executor._idle(backend.connection)
    with backend.connection.transaction():
        now = backend.connection.execute("SELECT clock_timestamp() AS now").fetchone()["now"]
        _require(contracts.utc(now) >= contracts.utc(plan["training_max_cutoff_utc"]),
                 "VALIDATION_TRAINING_CUTOFF_NOT_REACHED")
    request_id = backend.register_request(plan["declaration"], require_before_start=True)
    _require(request_id == plan["request_id"], "VALIDATION_REGISTERED_REQUEST_MISMATCH")
    info = backend.registration_info(request_id)
    timing = _registration(plan, info)
    report = _acquisition(plan, backend.report(request_id))
    result = {"version": VERSION, "plan_sha256": plan["plan_sha256"], "request_id": request_id,
        "registration_info": info, "timing_evidence": timing, "registration_complete": True,
        "acquisition_report_sha256": report["report_sha256"], **_LIMITATIONS, **_AUTHORITY}
    return deepcopy(_seal(result, "receipt_sha256"))


def _verify_executor(report, declared):
    """Use the pinned ranker's checks for exact scopes without inventing a grid."""
    receipt, dataset_id, cohort_id = ranking._identity(report, declared)
    scopes = _scopes(declared["scopes"])
    _require(type(report["declared_scopes"]) is int and report["declared_scopes"] == len(scopes) and
        receipt["declared_scope_count"] == len(scopes) and len(report["scopes"]) == len(scopes) and
        len(receipt["scopes"]) == len(scopes), "VALIDATION_SCOPE_DENOMINATOR_MISMATCH")
    _require(receipt["part_count"] == len(declared["parts"]) == len(receipt["parts"]) and
        [part["ordinal"] for part in receipt["parts"]] == list(range(len(declared["parts"]))) and
        sum(part["accepted_source_rows"] for part in receipt["parts"]) == receipt["accepted_source_rows"],
        "VALIDATION_PART_DENOMINATOR_MISMATCH")
    rows, records, population = [], [], None
    for ordinal, (item, full, scope) in enumerate(zip(report["scopes"], receipt["scopes"], scopes)):
        _require(type(item["ordinal"]) is int and item["ordinal"] == ordinal, "VALIDATION_SCOPE_ORDER_MISMATCH")
        records.append(ranking._coverage_scope(full, scope, declared, receipt))
        current = [{key: row[key] for key in ranking._SOURCE_KEYS} for row in full["decision_ledger"]]
        if population is None:
            population = current
        _require(_same(current, population), "VALIDATION_CROSS_SCOPE_SOURCE_MISMATCH")
        rows.append(ranking._scope_result(item, full, scope, declared, dataset_id, cohort_id, records[-1]))
    complete = all(row["status"] in ranking._TERMINAL for row in rows)
    _require(report["all_scopes_processed"] is complete and
        report["computation_complete"] is all(item["computation_complete"] is True for item in report["scopes"]) and
        report["outcome_trials_executed"] == sum(item["outcome_trial_executed"] for item in report["scopes"]),
        "VALIDATION_EXECUTOR_COMPLETION_MISMATCH")
    return {"report_sha256": report["report_sha256"], "plan_id": report["plan_id"],
        "coverage_receipt_sha256": receipt["receipt_sha256"], "all_scopes_processed": complete,
        "computation_complete": report["computation_complete"], "declared_scopes": len(scopes),
        "rows": rows}, records


def _fresh_row(plan, original, full, raw, records, *, complete):
    start = contracts.utc(plan["declaration"]["source_start_utc"])
    training = set(plan["known_matched_training_parent_ids"])
    survivors, excluded = [], []
    # Membership and start times were verified over every matched ledger row;
    # one original earliest representative retains each entire parent's fate.
    for representative in full["representatives"]:
        record = records[representative["entry_id"]]
        representative = {**representative, "parent_start_time_utc": record["parent_start_time_utc"]}
        reasons = []
        if representative["btc_parent_movement_id"] in training:
            reasons.append("PARENT_PRESENT_IN_TRAINING_MATCHED_POPULATION")
        if contracts.utc(record["parent_start_time_utc"]) < start:
            reasons.append("PARENT_STARTED_BEFORE_VALIDATION_SOURCE_START")
        if reasons:
            excluded.append({**representative, "reasons": reasons})
        else:
            survivors.append(representative)
    fresh_gate, exclusions = None, []
    if original["status"] == "INPUT_BLOCKED":
        exclusions.append("ORIGINAL_INPUT_BLOCKED")
    elif original["status"] in ranking._PENDING:
        exclusions.append("ORIGINAL_SCOPE_PENDING")
    else:
        outcomes = {row["entry_id"]: row["outcome"] for row in raw["outcomes"]}
        normalized = []
        for representative in survivors:
            record, state = records[representative["entry_id"]], outcomes[representative["entry_id"]]
            normalized.append({"contract": state["contract"], "outcome": state,
                **{key: record[key] for key in ("btc_parent_movement_id", "membership_status",
                    "parent_evidence_eligible", "parent_start_time_utc", "parent_confirmed_at_utc", "parent_policy_version")},
                "features_observed_at_utc": record["source_observed_at_utc"]})
        usable = all(row["outcome"]["status"] in first_touch.TERMINAL or
            row["outcome"]["status"] == "OPEN" and row["outcome"]["coverage_complete_through_cutoff"] is True
            for row in normalized)
        fresh_gate = gate.evaluate_gate(normalized, as_of_utc=plan["declaration"]["cutoff_utc"],
            source_coverage_complete=usable, policy=plan["declaration"]["gate_policy"])
        exclusions.extend(fresh_gate["blockers"])
    if not complete:
        exclusions.append("GLOBAL_VALIDATION_INCOMPLETE")
    return {**{key: original[key] for key in ("ordinal", *ranking._SCOPE_KEYS)},
        "candidate_version": full["candidate"]["definition_sha256"],
        "original_status": original["status"], "fresh_gate": fresh_gate,
        "prospective_gate_passed": complete and bool(fresh_gate and fresh_gate["experimental_eligible"]),
        "fresh_representatives": survivors, "excluded_parents": excluded,
        "fresh_parent_count": len(survivors), "excluded_parent_count": len(excluded),
        "exclusion_reasons": sorted(set(exclusions)), **_AUTHORITY}


def _evaluate(plan, info, acquired, report):
    timing = _registration(plan, info)
    acquired = _acquisition(plan, acquired)
    verified, rows, complete = None, [], False
    if acquired["status"] == "ADMITTED":
        _require(report is not None and report["plan_id"] == acquired["executor_plan_id"] and
            report["plan_identity"]["anchor_sha256"] == acquired["anchor_sha256"] and
            report["plan_identity"]["declaration_sha256"] == plan["declaration_sha256"],
            "VALIDATION_ADMITTED_EXECUTOR_BINDING_MISMATCH")
        verified, records = _verify_executor(report, plan["declaration"])
        complete = verified["all_scopes_processed"]
        rows = [_fresh_row(plan, original, full, raw, ledger, complete=complete)
            for original, full, raw, ledger in zip(verified["rows"], report["coverage_receipt"]["scopes"],
                                                 report["scopes"], records)]
    else:
        _require(report is None, "VALIDATION_UNADMITTED_OUTCOME_REPORT")
        rows = [{**{key: scope[key] for key in ranking._SCOPE_KEYS}, "ordinal": ordinal,
            "candidate_version": scope["candidate"]["definition_sha256"],
            "original_status": None, "fresh_gate": None, "prospective_gate_passed": False,
            "fresh_representatives": [], "excluded_parents": [], "fresh_parent_count": None,
            "excluded_parent_count": None, "exclusion_reasons": ["ACQUISITION_" + acquired["status"]],
            **_AUTHORITY} for ordinal, scope in enumerate(plan["selected_scopes"])]
    result = {"validation_version": VERSION, "plan_sha256": plan["plan_sha256"],
        "selected_scopes": plan["selected_scopes"], "implementation": plan["implementation"],
        "selection_report_sha256": plan["selection_report_sha256"], "request_id": plan["request_id"],
        "declaration_sha256": plan["declaration_sha256"], "policy": plan["policy"],
        "validation_complete": complete, "qualified_scope_ids": [row["scope_id"] for row in rows
                                                                   if row["prospective_gate_passed"]],
        "rows": rows, "original_verification": verified, "timing_evidence": timing,
        "acquisition_evidence": {key: acquired[key] for key in ("report_sha256", "status", "request_id",
                                  "anchor_sha256", "executor_plan_id", "terminal_receipt")},
        "denominator": {"declared_scopes": len(rows), "processed_scopes": sum(
            row["original_status"] in ranking._TERMINAL for row in rows),
            "qualified_scopes": sum(row["prospective_gate_passed"] for row in rows)},
        "registration_timing_verified": True, "database_origin_authenticated_by_this_tool": False,
        "prior_outcomes_observed": False,
        "limitations": ["TRUSTED_DATABASE_REPORTS_REQUIRED_HASHES_DO_NOT_AUTHENTICATE_ORIGIN",
            "PROSPECTIVE_GATE_PASS_IS_NOT_A_PROFITABILITY_OR_STATISTICAL_INDEPENDENCE_PROOF",
            "ALL_TRAINING_MATCHED_PARENTS_EXCLUDED_NOT_ALL_MARKET_PARENTS_KNOWN",
            "ORIGINAL_BLOCKED_EVIDENCE_RETAINED_EVEN_WHEN_OLD_PARENTS_ARE_EXCLUDED",
            "NO_MULTIPLE_TESTING_ADJUSTMENT_OR_COMPATIBLE_NO_HORIZON_ASYMMETRY"],
        **_LIMITATIONS, **_AUTHORITY}
    return deepcopy(_seal(result, "report_sha256"))


def evaluate_reports(value, *, selection_plan, selection_reports, registration_info,
                     acquisition_report, executor_report=None):
    """Pure structural verification; supplied timestamp hashes cannot prove origin."""
    try:
        plan = validate_plan(value, selection_plan=selection_plan, selection_reports=selection_reports)
        return _evaluate(plan, registration_info, acquisition_report, executor_report)
    except (KeyError, TypeError, IndexError, OverflowError) as exc:
        raise ValueError("VALIDATION_MALFORMED_EVIDENCE") from exc


def evaluate_plan(acquisition_backend, executor_backend, value, *, selection_plan, selection_reports):
    plan = validate_plan(value, selection_plan=selection_plan, selection_reports=selection_reports)
    _backend(acquisition_backend, plan["implementation"]["acquisition"])
    _backend(executor_backend, plan["implementation"]["acquisition"]["executor"])
    info = acquisition_backend.registration_info(plan["request_id"])
    _registration(plan, info)
    acquired = _acquisition(plan, acquisition_backend.report(plan["request_id"]))
    report = executor_backend.report(acquired["executor_plan_id"]) if acquired["status"] == "ADMITTED" else None
    if report is not None:
        _require(report["cohort_store_version"] == executor.VERSION, "VALIDATION_POSTGRES_EXECUTOR_REQUIRED")
    return _evaluate(plan, info, acquired, report)
