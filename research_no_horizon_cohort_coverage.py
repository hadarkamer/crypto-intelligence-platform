"""One outcome-free parent-coverage receipt over a single anchored cohort.

Parts are bounded transport units, not experiments. Every source decision and
matched parent is revalidated before global grouping and representative choice.
Only one full part is held at a time; compact ledgers have a separate explicit
global budget. This does not extend the local outcome runner's input limits.
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Mapping
from typing import Any

import research_no_horizon_cohort as cohort
import research_no_horizon_contract as contracts
import research_no_horizon_manifest as transport
import research_no_horizon_parent_coverage as coverage
import research_no_horizon_preflight as features
from research_no_horizon_experiment import _admit_export, _scopes

VERSION = "no-horizon-single-anchor-cohort-parent-coverage-v1"
_STATUSES = ("MATCH", "NO_MATCH", "UNKNOWN", "UNKNOWN_SOURCE")
_AUTHORITY = {"runtime_authorized": False, "telegram_authorized": False, "trading_authorized": False}


def _finish_scope(scope, rows, part_scopes, policy):
    groups = {}
    for row in rows:
        if row["btc_parent_movement_id"] is not None:
            groups.setdefault(row["btc_parent_movement_id"], []).append(row)
    conflicts = coverage._conflicting_groups(groups)
    for row in rows:
        if row["btc_parent_movement_id"] in conflicts:
            row["parent_blockers"] = sorted(set(row["parent_blockers"] +
                ["CONFLICTING_MATCHED_BTC_PARENT_EVIDENCE"]))
    parent_blockers = sorted({error for row in rows for error in row["parent_blockers"]})
    # Per-part insufficiency is expected and is not a global source failure.
    source_blockers = {error for part in part_scopes for error in part["blockers"]
        if error != "FEWER_THAN_REQUIRED_MATCHED_PARENTS"}
    source_complete = all(part["source_decisions_complete"] for part in part_scopes)
    parent_complete = not parent_blockers
    complete = source_complete and parent_complete
    bound = len(groups) if complete else None
    status = "BLOCKED" if not complete else "POSSIBLE" if bound >= policy["minimum_waves"] else "INSUFFICIENT"
    representatives = []
    if complete:
        for _, members in sorted(groups.items()):
            first = min(members, key=lambda row: (contracts.utc(row["decision_time_utc"]), row["entry_id"]))
            representatives.append({key: first[key] for key in (
                "btc_parent_movement_id", "snapshot_set_id", "part_ordinal", "ordinal",
                "global_ordinal", "entry_id", "decision_time_utc")})
    blockers = sorted(source_blockers | set(parent_blockers))
    if status == "INSUFFICIENT":
        blockers.append("FEWER_THAN_REQUIRED_MATCHED_PARENTS")
    counts = Counter(row["match_status"] for row in rows)
    return {**scope, "source_counts": {key: counts[key] for key in _STATUSES},
        "source_decisions_complete": source_complete, "parent_membership_complete": parent_complete,
        "matched_parent_count": len(groups), "verified_matched_rows": sum(map(len, groups.values())),
        "unverified_matched_rows": sum(row["match_status"] == "MATCH" and
            row["btc_parent_movement_id"] is None for row in rows),
        "conflicting_parent_ids": conflicts, "matched_parent_upper_bound": bound,
        "minimum_required_parents": policy["minimum_waves"], "coverage_status": status,
        "potentially_sufficient": status == "POSSIBLE", "blockers": blockers,
        "decision_ledger": rows, "representatives": representatives,
        "representative_selection": "GLOBAL_EARLIEST_DECISION_THEN_ENTRY_ID_WITHOUT_OUTCOMES_V1"
            if complete else "NOT_SELECTED_INCOMPLETE_POPULATION",
        "outcome_evaluation": "NOT_EVALUATED", "gate_evaluation": "NOT_EVALUATED", "gate": None,
        **_AUTHORITY}


def preflight_cohort(declaration: Mapping[str, Any], anchor: Mapping[str, Any],
                     load_part: Callable[[int], Mapping[str, Any]]) -> dict[str, Any]:
    """Revalidate each exact anchored export once, then reduce all source ledgers.

    The loader receives a part ordinal and returns its assembled export. Missing,
    foreign or malformed parts raise; no partial successful receipt is returned.
    Invalid individual captures, unknown predicates and unverified/conflicting
    matched parents remain visible and block the affected complete global scope.
    Caller-supplied outcome or coverage receipts are never consumed.
    """
    declared = cohort.normalize_declaration(declaration)
    frozen_anchor = cohort.validate_anchor(declared, anchor)
    if not callable(load_part):
        raise ValueError("COHORT_PART_LOADER_REQUIRED")
    scopes = _scopes(declared["scopes"])
    ledgers = {scope["scope_id"]: [] for scope in scopes}
    partials = {scope["scope_id"]: [] for scope in scopes}
    parts, total_bytes, total_rows, validated_rows = [], 0, 0, 0
    seen = set()
    for item in frozen_anchor["parts"]:
        ordinal, manifest = item["ordinal"], item["manifest"]
        try:
            loaded = load_part(ordinal)
        except (KeyError, IndexError, FileNotFoundError) as exc:
            raise ValueError("MISSING_COHORT_PART:" + str(ordinal)) from exc
        frozen, size = _admit_export(loaded)
        del loaded
        if size > declared["parts"][ordinal]["source_byte_limit"]:
            raise ValueError("COHORT_PART_DECLARED_CANONICAL_BYTE_LIMIT_EXCEEDED")
        total_bytes += size
        if total_bytes > cohort.MAX_TOTAL_BYTES:
            raise ValueError("COHORT_CANONICAL_BYTE_LIMIT_EXCEEDED")
        transport.validate_export_binding(frozen)
        if contracts.canonical(frozen["source_receipt"]["anchor_manifest"]) != contracts.canonical(manifest):
            raise ValueError("FOREIGN_COHORT_PART_ANCHOR:" + str(ordinal))
        row_count = len(frozen["source_rows"])
        if total_rows + row_count > cohort.MAX_TOTAL_ROWS or (total_rows + row_count) * len(scopes) > cohort.MAX_DECISIONS:
            raise ValueError("COHORT_GLOBAL_ROW_OR_DECISION_LIMIT_EXCEEDED")
        for row, entry in zip(frozen["source_rows"], manifest["source_entries"]):
            identity = row["intake"]["snapshot_set_id"]
            if identity in seen:
                raise ValueError("DUPLICATE_COHORT_SOURCE_ID")
            seen.add(identity)
            if contracts.canonical(row.get("btc_parent")) != contracts.canonical(
                    transport._strict_json(entry["btc_parent_payload_text"])):
                raise ValueError("COHORT_PART_PARENT_PIN_MISMATCH")
        feature_receipt = features.preflight_source_features(frozen, declared["scopes"])
        part_receipt = coverage._coverage_from_preflight(frozen, feature_receipt,
            gate_policy=declared["gate_policy"])
        # Retain every decision, including bad early matches and contradictory
        # later checkpoints. Reducing local representatives would lose evidence.
        for part_scope in part_receipt["scopes"]:
            scope_id = part_scope["scope_id"]
            for row in part_scope["decision_ledger"]:
                ledgers[scope_id].append({**row, "part_ordinal": ordinal,
                    "global_ordinal": total_rows + row["ordinal"]})
            partials[scope_id].append({key: part_scope[key] for key in (
                "source_decisions_complete", "blockers")})
        parts.append({"ordinal": ordinal, "anchor_manifest_sha256": contracts.digest(manifest),
            "source_export_sha256": part_receipt["source_export_sha256"], "source_canonical_bytes": size,
            "accepted_source_rows": row_count, "global_source_ordinal_start": total_rows,
            "feature_preflight_receipt_sha256": feature_receipt["receipt_sha256"],
            "parent_coverage_receipt_sha256": part_receipt["receipt_sha256"],
            "coverage_status_counts": part_receipt["coverage_status_counts"]})
        total_rows += row_count
        validated_rows += feature_receipt["validated_source_rows"]
        # The next loader call does not retain this part's large capture payload.
        del frozen, feature_receipt, part_receipt
    results = [_finish_scope(scope, ledgers[scope["scope_id"]], partials[scope["scope_id"]],
        declared["gate_policy"]) for scope in scopes]
    counts = Counter(scope["coverage_status"] for scope in results)
    all_possible = all(scope["potentially_sufficient"] for scope in results)
    result = {"coverage_version": VERSION, "cohort_declaration": declared,
        "declaration_sha256": contracts.digest(declared), "anchor_sha256": frozen_anchor["anchor_sha256"],
        "anchor_query_sha256": frozen_anchor["extraction_receipt"]["query_sha256"],
        "symbol": declared["symbol"], "source_start_utc": declared["source_start_utc"],
        "source_end_utc": declared["source_end_utc"], "cutoff_utc": declared["cutoff_utc"],
        "part_count": len(parts), "parts": parts, "accepted_source_rows": total_rows,
        "validated_source_rows": validated_rows, "invalid_source_rows": total_rows - validated_rows,
        "source_canonical_bytes": total_bytes, "scopes": results, "gate_policy": declared["gate_policy"],
        "coverage_status_counts": {key: counts[key] for key in ("BLOCKED", "INSUFFICIENT", "POSSIBLE")},
        "all_scopes_potentially_sufficient": all_possible,
        "any_scope_potentially_sufficient": any(scope["potentially_sufficient"] for scope in results),
        "ready_for_outcome_research": all_possible,
        "source_decisions_complete": all(scope["source_decisions_complete"] for scope in results),
        "entry_evaluation": "NOT_EVALUATED", "parent_evaluation": "CAUSAL_MEMBERSHIP_ONLY",
        "outcome_evaluation": "NOT_EVALUATED", "gate_evaluation": "NOT_EVALUATED",
        "outcomes_evaluated": False, "gate": None, "coverage_only": True,
        "outcome_runner_available": False, "local_experiment_store_compatible": False,
        "scope_pooling": False, "prior_outcome_receipts_consumed": 0,
        "declared_scope_count": len(scopes), "outcome_trials_executed": 0,
        "research_classification": "RETROSPECTIVE_SINGLE_COHORT_MATCHED_PARENT_COVERAGE",
        "is_prospective_formula_evidence": False, "db_origin_authenticated_by_this_tool": False,
        "limitations": ["MATCHED_PARENT_BOUND_IS_NOT_RESOLVED_OUTCOME_COUNT",
            "BTC_PARENT_GROUPING_IS_NOT_A_STATISTICAL_INDEPENDENCE_PROOF",
            "POSSIBLE_COVERAGE_DOES_NOT_EVALUATE_PROBABILITY_ASYMMETRY_OR_GATE",
            "ENTRY_PRICE_AND_OUTCOME_PATH_COVERAGE_NOT_EVALUATED",
            "COMMON_ANCHOR_PROVENANCE_IS_EXTERNAL_ATTESTATION_NOT_DATABASE_AUTHENTICATION",
            "DECLARATION_TIME_IS_NOT_PROOF_OF_UNSEEN_HISTORICAL_OUTCOMES",
            "GLOBAL_OUTCOME_RUNNER_REQUIRES_SEPARATE_IMPLEMENTATION"], **_AUTHORITY}
    return {**result, "receipt_sha256": contracts.digest(result)}
