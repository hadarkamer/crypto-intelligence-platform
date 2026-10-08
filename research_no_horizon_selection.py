"""Frozen descriptive selection across exact no-horizon discovery windows.

Each raw executor report is independently revalidated and reranked. Selection
compares per-window metrics; it never pools outcomes or treats repeated BTC
parents as independent evidence. A selected scope is only a research shortlist,
not prospective validation, publication authority or a trading instruction.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
from pathlib import Path

import research_no_horizon_cohort as cohort
import research_no_horizon_contract as contracts
import research_no_horizon_discovery as discovery
from research_no_horizon_experiment import _scopes
import research_no_horizon_ranking as ranking

VERSION = "no-horizon-frozen-multi-window-selection-v1"
POLICY_VERSION = "no-horizon-minimum-window-metrics-selection-v1"
_AUTHORITY = {"runtime_authorized": False, "telegram_authorized": False, "trading_authorized": False}
_LIMITATIONS = {"validated_discovery": False, "is_prospective_formula_evidence": False,
    "multiple_testing_adjusted": False, "scope_pooling": False, "window_pooling": False,
    "cross_window_outcome_pooling": False, "cross_window_parent_independence_proven": False,
    "catalog_entries_are_independent_strategies": False}
_SCOPE_KEYS = ("scope_id", "candidate_key", "base_direction", "analysis_direction", "threshold_pct")


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _same(left, right):
    return contracts.canonical(left) == contracts.canonical(right)


def _seal(value, field):
    return {**value, field: contracts.digest(value)}


def implementation():
    return {"selector_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "discovery": discovery.implementation()}


def policy(*, top_k, required_eligible_windows):
    """Contextual upper bounds are checked against the complete built grid."""
    for name, value in (("top_k", top_k), ("required_eligible_windows", required_eligible_windows)):
        _require(type(value) is int and value >= 1, "SELECTION_POSITIVE_INTEGER_REQUIRED:" + name)
    return {"policy_version": POLICY_VERSION, "top_k": top_k,
        "required_eligible_windows": required_eligible_windows,
        "rankability": "EVERY_DECLARED_WINDOW_RANKABLE",
        "eligibility": "UNCHANGED_ATOMIC_GATE_PASS_IN_REQUIRED_WINDOWS",
        "ordering": ["minimum_wilson_95_lower_pct_desc", "minimum_resolved_parents_desc",
                     "minimum_hit_rate_pct_desc", "scope_id_asc"],
        "metric_aggregation": "MINIMUM_PER_WINDOW_VALUES_WITHOUT_OUTCOME_POOLING",
        "window_counts_are_independent_samples": False,
        "statistical_claim": "DESCRIPTIVE_RESEARCH_HEURISTIC"}


def build_plan(first_declaration, *, base_directions, thresholds_pct,
               candidate_keys=None, window_count=1, top_k, required_eligible_windows):
    """Freeze selection policy before any source or outcome reads.

    Policy, total window count and implementation enter every child population's
    identity. Changing a policy cannot reuse a prefix of an earlier registration.
    The original normalized template, including prior outcome knowledge, remains
    intact in the outer plan.
    """
    template = cohort.normalize_declaration(first_declaration)
    frozen_policy = policy(top_k=top_k, required_eligible_windows=required_eligible_windows)
    _require(type(window_count) is int and window_count >= 1, "SELECTION_INVALID_WINDOW_COUNT")
    _require(required_eligible_windows <= window_count, "SELECTION_ELIGIBLE_WINDOW_LIMIT_EXCEEDED")
    versions = implementation()
    seeded = deepcopy(template)
    seeded["cohort_key"] = "selection:" + contracts.digest({
        "version": VERSION, "template_declaration": template, "policy": frozen_policy,
        "window_count": window_count, "implementation": versions})
    search = discovery.build_plan(seeded, base_directions=base_directions,
        thresholds_pct=thresholds_pct, candidate_keys=candidate_keys, window_count=window_count)
    _require(_same(search["implementation"], versions["discovery"]), "SELECTION_IMPLEMENTATION_CHANGED")
    scope_count = search["summary"]["scopes_per_window"]
    _require(top_k <= scope_count, "SELECTION_TOP_K_EXCEEDS_DECLARED_SCOPES")
    value = {"version": VERSION, "template_declaration": template, "policy": frozen_policy,
        "discovery_plan": search, "implementation": versions,
        "summary": {**search["summary"], "top_k": top_k,
                    "required_eligible_windows": required_eligible_windows},
        "prior_outcomes_observed": template["prior_outcomes_observed"],
        "source_reads_performed": 0, "outcome_reads_performed": 0,
        **_LIMITATIONS, **_AUTHORITY}
    return _seal(value, "plan_sha256")


def validate_plan(value):
    """Regenerate every field, including the policy-bound child identities."""
    try:
        _require(isinstance(value, dict), "COMPLETE_SELECTION_PLAN_REQUIRED")
        search, frozen_policy = value["discovery_plan"], value["policy"]
        mode = search["selection_mode"]
        _require(mode in ("ALL_SUPPORTED", "EXPLICIT_SUBSET"), "SELECTION_INVALID_CATALOG_MODE")
        rebuilt = build_plan(value["template_declaration"],
            base_directions=search["base_directions"], thresholds_pct=search["thresholds_pct"],
            candidate_keys=None if mode == "ALL_SUPPORTED" else search["candidate_keys"],
            window_count=search["calendar_plan"]["window_count"],
            top_k=frozen_policy["top_k"],
            required_eligible_windows=frozen_policy["required_eligible_windows"])
        _require(_same(value, rebuilt), "SELECTION_PLAN_OR_IMPLEMENTATION_MISMATCH")
        return rebuilt
    except (KeyError, TypeError, IndexError, OverflowError) as exc:
        raise ValueError("SELECTION_MALFORMED_PLAN") from exc


def register_plan(backend, value):
    """Use the existing strict database-clock registration path after validation."""
    plan = validate_plan(value)
    registered = discovery.register_plan(backend, plan["discovery_plan"])
    _require(registered["plan_sha256"] == plan["discovery_plan"]["plan_sha256"] and
             registered["registration_complete"] is True, "SELECTION_REGISTRATION_BINDING_MISMATCH")
    receipt = {"version": VERSION, "plan_sha256": plan["plan_sha256"],
        "discovery_plan_sha256": plan["discovery_plan"]["plan_sha256"],
        "discovery_registration": registered, "registration_complete": True,
        **_LIMITATIONS, **_AUTHORITY}
    return _seal(receipt, "receipt_sha256")


def _overlap(parent_windows):
    parents = [{"btc_parent_movement_id": parent, "window_ordinals": sorted(ordinals)}
               for parent, ordinals in sorted(parent_windows.items())]
    repeated = [item for item in parents if len(item["window_ordinals"]) > 1]
    return {"basis": "ALL_MATCHED_DECISIONS_ONLY", "parents": parents,
        "overlapping_parents": repeated, "distinct_matched_parent_count": len(parents),
        "repeated_parent_count": len(repeated), "all_market_parents_known": False,
        "cross_window_parent_independence_proven": False}


def _parent_population(reports, scope_count):
    """Include all MATCH rows even if their outcomes are pending or blocked."""
    global_parents, per_scope = {}, [{} for _ in range(scope_count)]
    for window_ordinal, report in enumerate(reports):
        for scope_ordinal, scope in enumerate(report["coverage_receipt"]["scopes"]):
            for record in scope["decision_ledger"]:
                if record["match_status"] != "MATCH":
                    continue
                parent = record["btc_parent_movement_id"]
                # The ranker has already verified assignable causal membership.
                _require(isinstance(parent, str) and bool(parent), "SELECTION_MATCH_PARENT_REQUIRED")
                global_parents.setdefault(parent, set()).add(window_ordinal)
                per_scope[scope_ordinal].setdefault(parent, set()).add(window_ordinal)
    return _overlap(global_parents), [_overlap(parents) for parents in per_scope]


def _candidate_row(scope, ordinal, windows, overlap, *, complete, required):
    evidence = [window["rows"][ordinal] for window in windows]
    _require(all(all(_same(item[key], scope[key]) for key in _SCOPE_KEYS) for item in evidence),
             "SELECTION_CROSS_WINDOW_SCOPE_MISMATCH")
    eligible = sum(item["experimental_eligible"] is True for item in evidence)
    rankable_in_every_window = all(item["rankable"] is True for item in evidence)
    rankable = complete and rankable_in_every_window
    selectable = rankable and eligible >= required
    reasons = []
    if not complete:
        reasons.append("GLOBAL_SELECTION_INCOMPLETE")
    if not rankable_in_every_window:
        reasons.append("NOT_RANKABLE_IN_EVERY_WINDOW")
    if eligible < required:
        reasons.append("FEWER_THAN_REQUIRED_ELIGIBLE_WINDOWS")
    metrics = {"minimum_wilson_95_lower_pct": None,
               "minimum_resolved_parents": None, "minimum_hit_rate_pct": None}
    if rankable:
        metrics = {"minimum_wilson_95_lower_pct": min(
                       item["gate"]["probability"]["wilson_95_lower_pct"] for item in evidence),
            "minimum_resolved_parents": min(item["gate"]["resolved_parents"] for item in evidence),
            "minimum_hit_rate_pct": min(item["gate"]["probability"]["hit_rate_pct"] for item in evidence)}
    return {**{key: scope[key] for key in _SCOPE_KEYS}, "ordinal": ordinal,
        "candidate_version": scope["candidate"]["definition_sha256"],
        "rank": None, "rankable": rankable, "selectable": selectable, "selected": False,
        "eligible_window_count": eligible, "required_eligible_windows": required,
        "exclusion_reasons": reasons, "metrics": metrics,
        "window_statuses": [{"window_ordinal": index, **{key: item[key] for key in
            ("status", "rankable", "experimental_eligible", "exclusion_reasons")}}
            for index, item in enumerate(evidence)],
        "known_matched_parent_ids": [item["btc_parent_movement_id"] for item in overlap["parents"]],
        "matched_parent_overlap": overlap}


def select_reports(value, reports):
    """Revalidate raw reports in exact calendar order and retain every trial.

    Completion permits descriptive selection, not a prospective claim. Raw
    executor reports can originate outside the registration path, so this API
    explicitly does not attest when the selection policy was registered.
    """
    try:
        plan = validate_plan(value)
        search = plan["discovery_plan"]
        count = search["calendar_plan"]["window_count"]
        _require(isinstance(reports, list) and len(reports) == count,
                 "SELECTION_EXACT_ORDERED_WINDOW_REPORTS_REQUIRED")
        # Never consume caller-created ranking receipts or cached probabilities.
        windows = [ranking.rank_report(search, report, window_ordinal=ordinal)
                   for ordinal, report in enumerate(reports)]
        complete = all(window["global_ranking_complete"] for window in windows)
        scopes = _scopes(search["calendar_plan"]["windows"][0]["declaration"]["scopes"])
        global_overlap, per_scope = _parent_population(reports, len(scopes))
        frozen_policy = plan["policy"]
        rows = [_candidate_row(scope, ordinal, windows, per_scope[ordinal],
                    complete=complete, required=frozen_policy["required_eligible_windows"])
                for ordinal, scope in enumerate(scopes)]
        ranked = sorted((row for row in rows if row["rankable"]), key=lambda row: (
            -row["metrics"]["minimum_wilson_95_lower_pct"],
            -row["metrics"]["minimum_resolved_parents"],
            -row["metrics"]["minimum_hit_rate_pct"], row["scope_id"]))
        for rank, row in enumerate(ranked, 1):
            row["rank"] = rank
        chosen = [row for row in ranked if row["selectable"]][:frozen_policy["top_k"]]
        selected_ids = {row["scope_id"] for row in chosen}
        for row in rows:
            row["selected"] = row["scope_id"] in selected_ids
            if row["selectable"] and not row["selected"]:
                row["exclusion_reasons"].append("TOP_K_LIMIT")
        by_id = {scope["scope_id"]: scope for scope in scopes}
        denominator = {"declared_windows": count,
            "complete_windows": sum(window["global_ranking_complete"] for window in windows),
            "declared_scopes": len(scopes), "declared_scope_windows": len(scopes) * count,
            "ranked_scopes": len(ranked), "unranked_scopes": len(scopes) - len(ranked),
            "selectable_scopes": sum(row["selectable"] for row in rows),
            "selected_scopes": len(chosen), "unselected_scopes": len(scopes) - len(chosen)}
        result = {"selection_version": VERSION, "plan_sha256": plan["plan_sha256"],
            "discovery_plan_sha256": search["plan_sha256"], "implementation": plan["implementation"],
            "policy": frozen_policy, "selection_complete": complete,
            "ranked_scope_ids": [row["scope_id"] for row in ranked],
            "selected_scope_ids": [row["scope_id"] for row in chosen],
            "selected_scopes": [by_id[row["scope_id"]] for row in chosen],
            "rows": rows, "windows": windows, "denominator": denominator,
            "known_matched_parent_ids": [item["btc_parent_movement_id"] for item in global_overlap["parents"]],
            "matched_parent_overlap": global_overlap,
            "prior_outcomes_observed": plan["prior_outcomes_observed"],
            "policy_registration_verified": False, "source_provenance_verified_by_this_tool": False,
            "limitations": ["SELECTION_IS_A_RESEARCH_SHORTLIST_NOT_PROSPECTIVE_VALIDATION",
                "MINIMUM_WINDOW_METRICS_ARE_NOT_POOLED_PROBABILITY_OR_NEW_CONFIDENCE_INTERVALS",
                "ELIGIBLE_WINDOW_COUNTS_ARE_NOT_INDEPENDENT_EVIDENCE_COUNTS",
                "MATCHED_PARENT_UNION_DOES_NOT_IDENTIFY_ALL_MARKET_PARENTS",
                "REPEATED_PARENT_IDS_AND_CATALOG_ALIASES_REMAIN_DEPENDENT",
                "ACTUAL_REGISTRATION_TIME_AND_MARKET_PROVENANCE_REQUIRE_SEPARATE_EVIDENCE",
                "NO_MULTIPLE_TESTING_ADJUSTMENT_OR_COMPATIBLE_NO_HORIZON_ASYMMETRY"],
            **_LIMITATIONS, **_AUTHORITY}
        return deepcopy(_seal(result, "report_sha256"))
    except (KeyError, TypeError, IndexError, OverflowError) as exc:
        raise ValueError("SELECTION_MALFORMED_PLAN_OR_REPORTS") from exc
