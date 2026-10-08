"""Outcome-free source/feature preflight for every declared research scope.

Accepted intake, available features, and decidable predicates are different
properties. This report never builds entries, assigns causal parents, evaluates
outcomes, or runs an evidence gate. Unknown rows remain in every scope ledger.
"""
from __future__ import annotations

from collections import Counter
from typing import Any, Mapping

import research_no_horizon_contract as contracts
import research_no_horizon_source as source
import research_watch_scan_formula as formulas

VERSION = "no-horizon-source-feature-preflight-v2-directional"
_STATUSES = ("MATCH", "NO_MATCH", "UNKNOWN", "UNKNOWN_SOURCE")
_AUTHORITY = {"runtime_authorized": False, "telegram_authorized": False, "trading_authorized": False}


def preflight_source_features(export: Mapping[str, Any], scopes) -> dict[str, Any]:
    """Validate one frozen population once, then report all unchanged predicates.

    Global structural errors raise before classification. Invalid individual
    rows become UNKNOWN_SOURCE with their original ordinal and claimed ID.
    Known false conjunctions remain NO_MATCH even with another missing feature.
    Readiness means source/predicate completeness, never enough parent evidence.
    """
    # Local import permits the coordinator to call preflight without a cycle.
    from research_no_horizon_experiment import _admit_export, _scopes
    frozen, canonical_bytes = _admit_export(export)
    normalized = _scopes(scopes)
    symbol = frozen["symbol"]
    start, end, cutoff = (contracts.utc(frozen[key]) for key in
        ("source_start_utc", "source_end_utc", "cutoff_utc"))
    rows = frozen["source_rows"]
    extraction_blockers = source._extraction_blockers(frozen, rows, frozen["source_receipt"])
    required = {scope["scope_id"]: sorted({condition["feature"] for condition in
        formulas.existing._conditions(scope["candidate"]["definition"])}) for scope in normalized}
    requested = {}
    for scope in normalized:
        for name in required[scope["scope_id"]]:
            requested.setdefault(name, set()).add(scope["base_direction"])
    # Availability is directional: an inactive source side can leave the other
    # side's own average known. Multiple scopes never multiply these row counts.
    availability = {name: {base: {"available_rows": 0, "unavailable_rows": 0,
        "unknown_source_rows": 0, "reason_counts": Counter(),
        "unavailable_source_ordinals": []} for base in sorted(bases)}
        for name, bases in sorted(requested.items())}
    scope_rows = [{**scope, "required_features": required[scope["scope_id"]], "decision_ledger": []}
                  for scope in normalized]
    seen, validated, errors = set(), 0, Counter()
    for ordinal, raw in enumerate(rows):
        intake = raw.get("intake") if isinstance(raw, Mapping) else None
        source_id = intake.get("snapshot_set_id") if isinstance(intake, Mapping) else None
        base = {"ordinal": ordinal, "snapshot_set_id": source_id}
        result, source_error = None, None
        try:
            if type(source_id) is not int or source_id <= 0 or source_id in seen:
                raise ValueError("INVALID_OR_DUPLICATE_SOURCE_ID")
            seen.add(source_id)
            observation, result = source._validate_row(raw, start=start, end=end, cutoff=cutoff, symbol=symbol)
            indexed = {(item["candidate_key"], item["base_direction"]): item for item in result["evaluations"]}
            # Missing selected evaluations are source failures, never omitted cells.
            for scope in normalized:
                decision = indexed[(scope["candidate_key"], scope["base_direction"])]
                if decision["match_status"] not in _STATUSES[:3]:
                    raise ValueError("INVALID_EXISTING_PREDICATE_DECISION")
            validated += 1
            base.update(feature_sha256=result["feature_sha256"],
                usable_from_utc=result["usable_from_utc"], source_observed_at_utc=result["source_observed_at_utc"],
                bundle_sha256=observation["bundle_sha256"], parent_payload_sha256=observation["parent_payload_sha256"])
        except (ValueError, TypeError, KeyError, OverflowError, AttributeError, IndexError) as exc:
            result, source_error = None, str(exc)
            errors[source_error] += 1
        for name, directions in availability.items():
            for direction, counts in directions.items():
                if result is None:
                    counts["unknown_source_rows"] += 1
                    continue
                features = result["features_by_direction"][direction]
                reasons = features["unavailable_features"].get(name)
                if reasons is None and name in features["features"]:
                    counts["available_rows"] += 1
                else:
                    counts["unavailable_rows"] += 1
                    counts["unavailable_source_ordinals"].append(ordinal)
                    counts["reason_counts"].update(set(reasons or ["MISSING_EXTRACTED_FEATURE"]))
        for scope in scope_rows:
            if result is None:
                record = {**base, "match_status": "UNKNOWN_SOURCE", "missing_features": [],
                    "missing_feature_reasons": {}, "source_error": source_error}
            else:
                decision = indexed[(scope["candidate_key"], scope["base_direction"])]
                missing = decision["missing_features"]
                unavailable = result["features_by_direction"][scope["base_direction"]]["unavailable_features"]
                record = {**base, "match_status": decision["match_status"], "missing_features": list(missing),
                    "missing_feature_reasons": {name: list(unavailable.get(name, ["MISSING_EXTRACTED_FEATURE"])) for name in missing}}
            scope["decision_ledger"].append(record)
    for directions in availability.values():
        for counts in directions.values():
            counts["reason_counts"] = dict(sorted(counts["reason_counts"].items()))
    for scope in scope_rows:
        counts = Counter(row["match_status"] for row in scope["decision_ledger"])
        blockers = list(extraction_blockers)
        if counts["UNKNOWN_SOURCE"]:
            blockers.append("INVALID_OR_MISSING_FROZEN_SOURCE")
        if counts["UNKNOWN"]:
            blockers.append("UNKNOWN_POTENTIALLY_MATCHING_FEATURES")
        scope.update(counts={status: counts[status] for status in _STATUSES},
            source_blockers=sorted(set(blockers)), ready_for_outcome_research=not blockers,
            predicate_decisions_complete=not (counts["UNKNOWN"] or counts["UNKNOWN_SOURCE"]),
            required_features_complete=not counts["UNKNOWN_SOURCE"] and not any(
                row["missing_features"] for row in scope["decision_ledger"]))
    result = {"preflight_version": VERSION, "source_export_sha256": contracts.digest(frozen),
        "source_canonical_bytes": canonical_bytes, "symbol": symbol,
        "source_start_utc": start.isoformat(), "source_end_utc": end.isoformat(), "cutoff_utc": cutoff.isoformat(),
        "accepted_source_rows": len(rows), "validated_source_rows": validated, "invalid_source_rows": len(rows)-validated,
        "intake_validation": {"claimed_accepted_rows": len(rows), "revalidated_rows": validated,
            "invalid_rows": len(rows)-validated, "reason_counts": dict(sorted(errors.items()))},
        "extraction_blockers": sorted(set(extraction_blockers)), "feature_availability": availability,
        "scopes": scope_rows, "ready_for_outcome_research": all(scope["ready_for_outcome_research"] for scope in scope_rows),
        "entry_evaluation": "NOT_EVALUATED", "parent_evaluation": "NOT_EVALUATED",
        "outcome_evaluation": "NOT_EVALUATED", "gate_evaluation": "NOT_EVALUATED",
        "outcomes_evaluated": False, "gate": None,
        "research_classification": "RETROSPECTIVE_SOURCE_FEATURE_AVAILABILITY",
        "is_prospective_formula_evidence": False, "db_origin_authenticated_by_this_tool": False, **_AUTHORITY}
    return {**result, "receipt_sha256": contracts.digest(result)}
