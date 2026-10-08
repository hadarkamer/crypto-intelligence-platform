"""Descriptive ranking of every scope in one predeclared discovery window.

This consumes compatible executor reports, not arbitrary metrics. All selected
outcomes and causal representatives are checked and the existing atomic gate is
recomputed. Hashes bind serialized evidence; database origin, feature capture,
and actual price provenance still require external attestation.
"""
from __future__ import annotations

from collections import Counter
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

import research_no_horizon_cohort as cohort
import research_no_horizon_cohort_coverage as coverage
import research_no_horizon_cohort_outcomes as preparation
import research_no_horizon_cohort_store as local_store
import research_no_horizon_contract as contracts
from research_no_horizon_experiment import _scopes
import research_no_horizon_first_touch as first_touch
import research_no_horizon_gate as gate
import research_no_horizon_postgres_store as postgres_store
import research_no_horizon_source as source

VERSION = "no-horizon-frozen-window-descriptive-ranking-v1"
POLICY_VERSION = "no-horizon-wilson-resolved-hit-scope-order-v1"
_AUTHORITY = {"runtime_authorized": False, "telegram_authorized": False, "trading_authorized": False}
_TERMINAL = {"COMPLETE", "BLOCKED", "INPUT_BLOCKED"}
_PENDING = {"NOT_SUBMITTED", "PENDING", "RUNNING"}
_SCOPE_KEYS = ("scope_id", "candidate_key", "base_direction", "analysis_direction", "threshold_pct")
_REP_KEYS = ("btc_parent_movement_id", "snapshot_set_id", "part_ordinal", "ordinal",
             "global_ordinal", "entry_id", "decision_time_utc")
_ELIGIBILITY_ONLY = {"FEWER_THAN_REQUIRED_RESOLVED_PARENTS", "NO_COMPATIBLE_METRIC_ROUTE_PASSES"}
_SOURCE_KEYS = ("part_ordinal", "ordinal", "global_ordinal", "snapshot_set_id", "feature_sha256",
                "usable_from_utc", "source_observed_at_utc", "bundle_sha256", "parent_payload_sha256")


def policy() -> dict[str, Any]:
    return {"policy_version": POLICY_VERSION,
        "ordering": ["wilson_95_lower_pct_desc", "resolved_parents_desc",
                     "hit_rate_pct_desc", "scope_id_asc"],
        "minimum_resolved_for_descriptive_rank": 1,
        "require_all_declared_scopes_processed": True,
        "experimental_gate_policy": "UNCHANGED_DECLARED_COHORT_GATE_POLICY",
        "asymmetry_ranking_available": False,
        "statistical_claim": "DESCRIPTIVE_RESEARCH_HEURISTIC"}


def implementation() -> dict[str, Any]:
    return {"version": VERSION, "policy": policy(),
            "implementation_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _same(left, right):
    return contracts.canonical(left) == contracts.canonical(right)


def _sealed(value, field):
    _require(isinstance(value, Mapping), "RANKING_OBJECT_REQUIRED")
    _require(value[field] == contracts.digest({k: v for k, v in value.items() if k != field}),
             "RANKING_EVIDENCE_HASH_MISMATCH")


def _false(value, keys):
    _require(all(value.get(key) is False for key in keys), "RANKING_UNSUPPORTED_AUTHORITY_OR_CLAIM")


def _hash(value):
    _require(isinstance(value, str) and len(value) == 64 and
             all(char in "0123456789abcdef" for char in value), "RANKING_HASH_REQUIRED")


def _identity(report, declared):
    _sealed(report, "report_sha256")
    identity, receipt = report["plan_identity"], report["coverage_receipt"]
    _sealed(receipt, "receipt_sha256")
    prepared_id = contracts.digest(identity)
    _require(identity["preparation_version"] == preparation.VERSION and
             _same(identity["versions"], preparation.implementation()), "RANKING_STALE_PREPARATION")
    _require(_same(receipt["cohort_declaration"], declared) and
             receipt["coverage_version"] == coverage.VERSION and
             receipt["declaration_sha256"] == contracts.digest(declared) and
             receipt["validated_source_rows"] == receipt["accepted_source_rows"] and
             receipt["invalid_source_rows"] == 0, "RANKING_FOREIGN_DECLARATION")
    _require(all(_same(identity[key], receipt[key]) for key in
                 ("declaration_sha256", "anchor_sha256", "gate_policy")) and
             identity["coverage_receipt_sha256"] == receipt["receipt_sha256"] and
             _same(identity["gate_policy"], declared["gate_policy"]), "RANKING_PREPARED_BINDING_MISMATCH")
    for key in ("anchor_sha256", "candles_sha256", "payload_sha256"):
        _hash(identity[key])
    if report["cohort_store_version"] == postgres_store.VERSION:
        runtime = report["runtime_identity"]
        expected_implementation = postgres_store.implementation()
        _require(report["prepared_plan_id"] == prepared_id and
                 _same(runtime, {"store_version": postgres_store.VERSION,
                     "prepared_plan_id": prepared_id,
                     "implementation_sha256": contracts.digest(expected_implementation),
                     "implementation": expected_implementation}) and
                 report["plan_id"] == contracts.digest(runtime), "RANKING_RUNTIME_BINDING_MISMATCH")
    else:
        _require(report["cohort_store_version"] == local_store.VERSION and
                 report["plan_id"] == prepared_id and "runtime_identity" not in report and
                 "prepared_plan_id" not in report, "RANKING_UNSUPPORTED_EXECUTOR")
    for item in (report, receipt):
        _false(item, _AUTHORITY)
        _false(item, ("scope_pooling", "is_prospective_formula_evidence"))
    _false(report, ("parts_are_trials", "scope_ranking", "multiple_testing_adjusted", "validated_discovery"))
    _require(receipt["source_decisions_complete"] is True and report["source_coverage_complete"] is True and
             all(receipt[key] == declared[key] for key in
                 ("symbol", "source_start_utc", "source_end_utc", "cutoff_utc")),
             "RANKING_INCOMPLETE_OR_FOREIGN_SOURCE")
    dataset_id = "watch-global-dataset:" + contracts.digest({
        "declaration_sha256": receipt["declaration_sha256"],
        "anchor_sha256": receipt["anchor_sha256"], "candles_sha256": identity["candles_sha256"]})
    return receipt, dataset_id, "watch-global-cohort:" + receipt["declaration_sha256"]


def _coverage_scope(full, expected, declared, receipt):
    _require(all(_same(full[key], expected[key]) for key in (*_SCOPE_KEYS, "candidate")),
             "RANKING_SCOPE_IDENTITY_MISMATCH")
    ledger = full["decision_ledger"]
    count = receipt["accepted_source_rows"]
    _require(type(count) is int and 0 <= count <= cohort.MAX_SOURCE_ROWS and
             isinstance(ledger, list) and len(ledger) == count and
             count * receipt["declared_scope_count"] <= cohort.MAX_SOURCE_SCOPE_DECISIONS,
             "RANKING_LEDGER_DENOMINATOR_MISMATCH")
    _require([row["global_ordinal"] for row in ledger] == list(range(count)) and
             len({row["snapshot_set_id"] for row in ledger}) == count, "RANKING_DUPLICATE_OR_MISSING_DECISION")
    groups = {}
    for row in ledger:
        part_ordinal = row["part_ordinal"]
        _require(type(part_ordinal) is int and 0 <= part_ordinal < len(receipt["parts"]),
                 "RANKING_FOREIGN_PART")
        part = receipt["parts"][part_ordinal]
        interval = declared["parts"][part_ordinal]
        usable = contracts.utc(row["usable_from_utc"])
        _require(contracts.utc(interval["source_start_utc"]) <= usable < contracts.utc(interval["source_end_utc"]) and
                 contracts.utc(row["source_observed_at_utc"]) <= usable and
                 type(row["snapshot_set_id"]) is int and row["snapshot_set_id"] > 0,
                 "RANKING_DECISION_TIME_OR_SOURCE_ID_MISMATCH")
        _require(type(row["ordinal"]) is int and 0 <= row["ordinal"] < part["accepted_source_rows"] and
                 row["global_ordinal"] == part["global_source_ordinal_start"] + row["ordinal"],
                 "RANKING_DECISION_PART_MISMATCH")
        _require(row["match_status"] in ("MATCH", "NO_MATCH") and not row["parent_blockers"],
                 "RANKING_UNVERIFIED_DECISION")
        if row["match_status"] == "NO_MATCH":
            _require(row["btc_parent_movement_id"] is None, "RANKING_NONMATCH_PARENT")
            continue
        decision = contracts.utc(row["decision_time_utc"])
        parent = row["btc_parent_movement_id"]
        _require(isinstance(parent, str) and parent.strip() == parent and bool(parent) and
                 row["entry_id"] == f"watch:{row['snapshot_set_id']}:{declared['symbol']}:{expected['base_direction']}" and
                 row["membership_status"] == "LIVE" and row["parent_evidence_eligible"] is True and
                 row["parent_policy_version"] == declared["version_bindings"]["parent_policy_version"] and
                 contracts.utc(row["source_observed_at_utc"]) <= decision <= contracts.utc(declared["cutoff_utc"]) and
                 contracts.utc(row["usable_from_utc"]) == decision and
                 contracts.utc(row["parent_start_time_utc"]) <= contracts.utc(row["parent_confirmed_at_utc"]) <= decision,
                 "RANKING_CAUSAL_METADATA_MISMATCH")
        groups.setdefault(parent, []).append(row)
    # Reuse the outcome-blind reducer over the full ledger, never over winners.
    rebuilt = coverage._finish_scope(expected, json.loads(contracts.canonical(ledger)),
        [{"source_decisions_complete": True, "blockers": []}], declared["gate_policy"])
    _require(_same(rebuilt, full) and rebuilt["coverage_status"] != "BLOCKED",
             "RANKING_COVERAGE_OR_EARLIEST_REPRESENTATIVE_MISMATCH")
    return {row["entry_id"]: row for row in ledger if row["match_status"] == "MATCH"}


def _scope_result(item, full, expected, declared, dataset_id, cohort_id, records):
    _require(all(_same(item[key], expected[key]) for key in _SCOPE_KEYS) and
             _same(item["representatives"], full["representatives"]), "RANKING_REPORT_SCOPE_MISMATCH")
    _false(item, _AUTHORITY)
    status = item["status"]
    _require(status in _TERMINAL | _PENDING and type(item["outcome_trial_executed"]) is bool and
             type(item["candle_evaluations"]) is int and item["candle_evaluations"] >= 0,
             "RANKING_UNSUPPORTED_PROGRESS")
    result = {**{key: item[key] for key in ("ordinal", *_SCOPE_KEYS)}, "status": status,
        "rank": None, "rankable": False, "exclusion_reasons": [], "experimental_eligible": False,
        "gate": None, "representatives": full["representatives"], "outcome_status_counts": None}
    if status == "INPUT_BLOCKED":
        missing = item["missing_entry_ids"]
        locators = item["missing_entry_locators"]
        reps = {rep["entry_id"]: rep for rep in full["representatives"]}
        _require(item["error"] == "MISSING_SELECTED_ENTRY_OPEN" and isinstance(missing, list) and bool(missing) and
                 len(missing) == len(set(missing)) and set(missing) <= set(reps) and
                 isinstance(locators, list) and [row["entry_id"] for row in locators] == missing,
                 "RANKING_INPUT_BLOCKED_BINDING_MISMATCH")
        for row in locators:
            _require(_same({key: row[key] for key in _REP_KEYS}, reps[row["entry_id"]]) and
                     row["entry_time_utc"] == source._entry_time(
                         contracts.utc(reps[row["entry_id"]]["decision_time_utc"])).isoformat(),
                     "RANKING_MISSING_ENTRY_LOCATOR_MISMATCH")
        _require(item["job_id"] is None and item["snapshot_sha256"] is None and item["gate"] is None and
                 item["receipt_sha256"] is None and item["computation_complete"] is False and
                 item["candle_evaluations"] == 0 and item["outcome_trial_executed"] is False and
                 "outcomes" not in item, "RANKING_BLOCKED_SCOPE_FABRICATED_WORK")
        result["exclusion_reasons"] = ["INPUT_BLOCKED"]
        return result
    _require(item["error"] is None and item["missing_entry_ids"] == [] and
             item["missing_entry_locators"] == [], "RANKING_SCOPE_INPUT_MISMATCH")
    _hash(item["snapshot_sha256"])
    if status in _PENDING:
        _require(item["gate"] is None and item["receipt_sha256"] is None and
                 item["computation_complete"] is None and "outcomes" not in item,
                 "RANKING_UNFINISHED_SCOPE_HAS_RESULTS")
        result["exclusion_reasons"] = ["PENDING_SCOPE"]
        return result
    _hash(item["receipt_sha256"])
    _require(item["outcome_trial_executed"] is True, "RANKING_TERMINAL_SCOPE_HAS_NO_WORK")
    outcomes = item["outcomes"]
    selected = sorted(full["representatives"], key=lambda row:
                      (contracts.utc(row["decision_time_utc"]), row["entry_id"]))
    _require(isinstance(outcomes, list) and [row["entry_id"] for row in outcomes] ==
             [row["entry_id"] for row in selected], "RANKING_OUTCOME_POPULATION_MISMATCH")
    normalized, states = [], []
    for raw, representative in zip(outcomes, selected):
        state = first_touch.validate_state(raw["outcome"])
        contract = state["contract"]
        record = records[representative["entry_id"]]
        wanted = {"candidate_id": expected["candidate_key"],
            "candidate_version": expected["candidate"]["definition_sha256"],
            "cohort_id": cohort_id, "dataset_id": dataset_id, "entry_id": representative["entry_id"],
            "symbol": declared["symbol"], "direction": expected["analysis_direction"],
            "decision_time_utc": record["decision_time_utc"], "threshold_pct": expected["threshold_pct"],
            "source_route": source.route_for_symbol(declared["symbol"]),
            "parent_policy_version": record["parent_policy_version"]}
        _require(all(_same(contract[key], value) for key, value in wanted.items()) and
                 state["cutoff_utc"] == declared["cutoff_utc"], "RANKING_OUTCOME_CONTRACT_MISMATCH")
        normalized.append({"contract": contract, "outcome": state,
            **{key: record[key] for key in ("btc_parent_movement_id", "membership_status",
                "parent_evidence_eligible", "parent_start_time_utc", "parent_confirmed_at_utc",
                "parent_policy_version")}, "features_observed_at_utc": record["source_observed_at_utc"]})
        states.append(state)
    complete = all(state["status"] in first_touch.TERMINAL or
                   state["status"] == "OPEN" and state["coverage_complete_through_cutoff"] is True for state in states)
    fresh = gate.evaluate_gate(normalized, as_of_utc=declared["cutoff_utc"],
                              source_coverage_complete=complete, policy=declared["gate_policy"])
    counts = Counter(state["status"] for state in states)
    _require(_same(fresh, item["gate"]) and item["computation_complete"] is complete and
             status == ("COMPLETE" if complete else "BLOCKED") and
             _same(item["status_counts"], {key: counts[key] for key in first_touch.STATUSES}) and
             item["candle_evaluations"] == sum(state["processed_candles"] for state in states),
             "RANKING_GATE_OR_PROGRESS_MISMATCH")
    evidence_blockers = sorted(set(fresh["blockers"]) - _ELIGIBILITY_ONLY)
    reasons = ["EVIDENCE_BLOCKED"] if evidence_blockers or not complete else []
    if fresh["resolved_parents"] == 0:
        reasons.append("NO_RESOLVED_PARENTS")
    result.update(gate=fresh, outcome_status_counts=item["status_counts"],
                  exclusion_reasons=reasons, experimental_eligible=fresh["experimental_eligible"])
    return result


def rank_report(search_plan, report, *, window_ordinal=0) -> dict[str, Any]:
    """Rank one exact calendar window; pending trials suppress every rank.

    The input must come from the trusted compatible executor reporting path.
    This verifies internal evidence and recomputes metrics, not market data truth.
    Row eligibility retains its exact atomic gate meaning even if another scope
    is pending; only a complete global population can receive descriptive ranks.
    """
    import research_no_horizon_discovery as discovery

    try:
        discovery.validate_plan(search_plan)
        windows = search_plan["calendar_plan"]["windows"]
        _require(type(window_ordinal) is int and 0 <= window_ordinal < len(windows),
                 "RANKING_INVALID_WINDOW_ORDINAL")
        declared = windows[window_ordinal]["declaration"]
        receipt, dataset_id, cohort_id = _identity(report, declared)
        scopes = _scopes(declared["scopes"])
        _require(type(report["declared_scopes"]) is int and report["declared_scopes"] == len(scopes) and
                 receipt["declared_scope_count"] == len(scopes) and len(report["scopes"]) == len(scopes) and
                 len(receipt["scopes"]) == len(scopes), "RANKING_SCOPE_DENOMINATOR_MISMATCH")
        _require(receipt["part_count"] == len(declared["parts"]) == len(receipt["parts"]) and
                 [part["ordinal"] for part in receipt["parts"]] == list(range(len(declared["parts"]))) and
                 sum(part["accepted_source_rows"] for part in receipt["parts"]) == receipt["accepted_source_rows"],
                 "RANKING_PART_DENOMINATOR_MISMATCH")
        rows, source_population = [], None
        for ordinal, (item, full, expected) in enumerate(zip(report["scopes"], receipt["scopes"], scopes)):
            _require(type(item["ordinal"]) is int and item["ordinal"] == ordinal, "RANKING_SCOPE_ORDER_MISMATCH")
            records = _coverage_scope(full, expected, declared, receipt)
            population = [{key: row[key] for key in _SOURCE_KEYS} for row in full["decision_ledger"]]
            if source_population is None:
                source_population = population
            _require(_same(population, source_population), "RANKING_CROSS_SCOPE_SOURCE_MISMATCH")
            rows.append(_scope_result(item, full, expected, declared, dataset_id, cohort_id, records))
        complete = all(row["status"] in _TERMINAL for row in rows)
        _require(report["all_scopes_processed"] is complete and
                 report["computation_complete"] is all(item["computation_complete"] is True for item in report["scopes"]) and
                 report["outcome_trials_executed"] == sum(item["outcome_trial_executed"] for item in report["scopes"]),
                 "RANKING_REPORT_COMPLETION_MISMATCH")
        for row in rows:
            if not complete:
                row["exclusion_reasons"].append("GLOBAL_SEARCH_INCOMPLETE")
            row["rankable"] = complete and not row["exclusion_reasons"]
        ranked = sorted((row for row in rows if row["rankable"]), key=lambda row: (
            -row["gate"]["probability"]["wilson_95_lower_pct"], -row["gate"]["resolved_parents"],
            -row["gate"]["probability"]["hit_rate_pct"], row["scope_id"]))
        for rank, row in enumerate(ranked, 1):
            row["rank"] = rank
        denominator = {"declared_scopes": len(rows), "processed_scopes": sum(row["status"] in _TERMINAL for row in rows),
            "ranked_scopes": len(ranked), "unranked_scopes": len(rows) - len(ranked),
            "pending_scopes": sum(row["status"] in _PENDING for row in rows),
            "input_blocked_scopes": sum(row["status"] == "INPUT_BLOCKED" for row in rows),
            "evidence_blocked_scopes": sum("EVIDENCE_BLOCKED" in row["exclusion_reasons"] for row in rows),
            "zero_resolved_scopes": sum("NO_RESOLVED_PARENTS" in row["exclusion_reasons"] for row in rows)}
        value = {"ranking_version": VERSION, "policy": policy(), "implementation": implementation(),
            "experimental_gate_policy": declared["gate_policy"],
            "search_plan_sha256": search_plan["plan_sha256"], "window_ordinal": window_ordinal,
            "declaration_sha256": receipt["declaration_sha256"], "source_report_sha256": report["report_sha256"],
            "source_plan_id": report["plan_id"], "global_ranking_complete": complete,
            "declared_scope_count": len(rows), "denominator": denominator,
            "ranked_scope_ids": [row["scope_id"] for row in ranked], "rows": rows,
            "scope_pooling": False, "window_pooling": False, "parts_are_trials": False,
            "multiple_testing_adjusted": False, "validated_discovery": False,
            "is_prospective_formula_evidence": False, "source_provenance_verified_by_this_tool": False,
            "limitations": ["DESCRIPTIVE_RANKING_IS_NOT_VALIDATED_SELECTION_OR_PROFITABILITY",
                "FULL_DECLARED_DENOMINATOR_RETAINED_WITHOUT_MULTIPLE_TESTING_ADJUSTMENT",
                "NO_COMPARISON_OR_AGGREGATION_ACROSS_CALENDAR_WINDOWS",
                "BTC_PARENT_GROUPING_IS_NOT_A_STATISTICAL_INDEPENDENCE_PROOF",
                "SOURCE_FEATURE_AND_PRICE_PROVENANCE_REQUIRE_EXTERNAL_ATTESTATION",
                "NO_COMPATIBLE_NO_HORIZON_ASYMMETRY_RANKING"], **_AUTHORITY}
        # Return an isolated JSON value: caller mutation cannot alter its inputs.
        return json.loads(contracts.canonical({**value, "report_sha256": contracts.digest(value)}))
    except (KeyError, TypeError, IndexError, OverflowError) as exc:
        raise ValueError("RANKING_MALFORMED_REPORT_OR_PLAN") from exc
