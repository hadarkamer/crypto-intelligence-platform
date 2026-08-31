"""Pure same-anchor Replay/Shadow comparison for the Formula Lab.

The comparison is deliberately read-only and outcome-blind.  Both the current
V7.1 cohort and retained V6.2 Shadow cohort are evaluated against the exact
same verified sampler-v4 decision rows.  Raw anchor matches remain auditable,
while market episodes and evidence families prevent correlated observations
from being presented as independent proof.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from typing import Any, Dict, Iterable, Mapping, Sequence

import research_evidence_contract
import research_evidence_telegram_renderer
import research_formula_engine
import research_formula_families
import research_market_episode


COMPARISON_VERSION = "formula-lab-same-anchor-comparison-v1"
MAX_PAIR_DETAILS = 10


def _utc_iso(value: Any) -> str:
    parsed = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    )
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _runtime_contract(formula: Mapping[str, Any]) -> tuple[str, str]:
    return (
        str(formula.get("formula_schema_version") or ""),
        str(formula.get("engine_version") or ""),
    )


def _expected_contract(cohort: str) -> tuple[str, str]:
    if cohort == "CURRENT_V7_1":
        return (
            research_formula_engine.FORMULA_SCHEMA_VERSION,
            research_formula_engine.ENGINE_VERSION,
        )
    if cohort == "LEGACY_V6_2":
        return (
            research_formula_engine.LEGACY_V6_FORMULA_SCHEMA_VERSION,
            research_formula_engine.LEGACY_V6_ENGINE_VERSION,
        )
    raise ValueError(f"unsupported comparison cohort: {cohort}")


def _anchor_id(row: Mapping[str, Any]) -> int:
    event = row.get("event") if isinstance(row.get("event"), Mapping) else {}
    value = row.get("prospective_anchor_slot_id") or event.get(
        "prospective_anchor_slot_id"
    )
    if value is None:
        value = event.get("event_id") or row.get("event_id")
    return int(value)


def _event_id(row: Mapping[str, Any]) -> int:
    event = row.get("event") if isinstance(row.get("event"), Mapping) else {}
    return int(event.get("event_id") or row.get("event_id") or 0)


def _episode_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    event = dict(row.get("event") or {})
    event["forecast_start_time_utc"] = (
        event.get("forecast_start_time_utc")
        or event.get("alert_time_utc")
        or row.get("prospective_decision_time_utc")
    )
    return {"event": event, "prospective_anchor_slot_id": _anchor_id(row)}


def _formula_summary(
    formula: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    direction = str(formula.get("direction") or "").upper()
    horizon = int(formula.get("horizon_minutes") or 0)
    conditions = list(formula.get("conditions") or [])
    status_counts: Counter[str] = Counter()
    missing_features: Counter[str] = Counter()
    matched_rows: list[Dict[str, Any]] = []
    matched_anchor_ids: list[int] = []
    for row in rows:
        event = row.get("event") if isinstance(row.get("event"), Mapping) else {}
        result = research_formula_engine.evaluate_frozen_feature_values(
            row.get("frozen_decision_features"),
            direction=direction,
            event_direction=event.get("direction"),
            conditions=conditions,
        )
        status_counts[result["status"]] += 1
        for condition in result.get("condition_results") or []:
            if condition.get("available") is not True:
                missing_features[str(condition.get("feature") or "UNKNOWN")] += 1
        if result["status"] == "MATCHED":
            matched_anchor_ids.append(_anchor_id(row))
            matched_rows.append(_episode_row(row))

    episodes = research_market_episode.group_rows(
        matched_rows,
        horizon_minutes=horizon,
    )
    episode_keys = [str(episode["episode_key"]) for episode in episodes]
    family_policy = research_formula_families.condition_family_policy(conditions)
    evaluated = status_counts["MATCHED"] + status_counts["UNMATCHED"]
    unique_matched_anchor_ids = sorted(set(matched_anchor_ids))
    match_fingerprint = hashlib.sha256(
        json.dumps(unique_matched_anchor_ids, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        "formula_id": int(formula.get("formula_id") or 0),
        "formula_key": str(formula.get("formula_key") or ""),
        "formula_version": int(formula.get("formula_version") or 0),
        "current_stage": str(formula.get("current_stage") or ""),
        "direction": direction,
        "horizon_minutes": horizon,
        "conditions": conditions,
        "condition_family_policy": family_policy,
        "status_counts": dict(sorted(status_counts.items())),
        "evaluable_anchor_count": evaluated,
        "raw_match_count": len(matched_anchor_ids),
        "matched_anchor_ids_sample": unique_matched_anchor_ids[:5],
        "matched_anchor_ids_truncated": max(
            0, len(unique_matched_anchor_ids) - 5
        ),
        "matched_anchor_fingerprint": match_fingerprint,
        "_matched_anchor_ids": unique_matched_anchor_ids,
        "independent_market_episode_count": len(episode_keys),
        "matched_market_episode_ids": sorted(episode_keys),
        "sample_inflation_prevented": len(episode_keys) <= len(matched_anchor_ids),
        "missing_feature_counts": dict(sorted(missing_features.items())),
        "ranking_score": formula.get("ranking_score"),
    }


def _cohort_summary(
    cohort: str,
    formulas: Sequence[Mapping[str, Any]],
    rows: Sequence[Mapping[str, Any]],
    *,
    direction: str,
    horizon_minutes: int,
) -> Dict[str, Any]:
    expected = _expected_contract(cohort)
    mismatched = [
        str(formula.get("formula_key") or formula.get("formula_id") or "UNKNOWN")
        for formula in formulas
        if _runtime_contract(formula) != expected
    ]
    if mismatched:
        raise ValueError(
            f"{cohort} contains formulas from another runtime: {', '.join(mismatched)}"
        )
    filter_mismatches = [
        str(formula.get("formula_key") or formula.get("formula_id") or "UNKNOWN")
        for formula in formulas
        if (
            str(formula.get("direction") or "").upper() != direction
            or int(formula.get("horizon_minutes") or 0) != horizon_minutes
        )
    ]
    if filter_mismatches:
        raise ValueError(
            f"{cohort} formula direction/horizon does not match comparison filters: "
            + ", ".join(filter_mismatches)
        )
    summaries = [_formula_summary(formula, rows) for formula in formulas]
    family_inputs = [
        {
            **summary,
            "recommended_stage": summary["current_stage"],
            "_evidence_keys": summary["matched_market_episode_ids"],
        }
        for summary in summaries
        if summary["matched_market_episode_ids"]
    ]
    families = research_formula_families.group_formula_evidence(family_inputs)
    evaluable_formulas = sum(
        summary["evaluable_anchor_count"] > 0 for summary in summaries
    )
    invalid_condition_families = [
        summary["formula_key"]
        for summary in summaries
        if summary["condition_family_policy"]["valid"] is not True
    ]
    return {
        "cohort": cohort,
        "runtime": {
            "formula_schema_version": expected[0],
            "engine_version": expected[1],
        },
        "formula_count": len(summaries),
        "evaluable_formula_count": evaluable_formulas,
        "operational": bool(summaries and rows and evaluable_formulas),
        "invalid_condition_family_formula_keys": invalid_condition_families,
        "evidence_families": {
            "policy_version": families["policy_version"],
            "input_formulas": families["input_formulas"],
            "exact_duplicates_collapsed": families[
                "exact_duplicates_collapsed"
            ],
            "overlap_families": families["overlap_families"],
            "families": [
                {
                    "family_id": family["family_id"],
                    "champion_formula_key": family[
                        "champion_formula_key"
                    ],
                    "member_count": family["member_count"],
                }
                for family in families["families"]
            ],
        },
        "formulas": summaries,
    }


def _overlap_summary(
    current: Mapping[str, Any], legacy: Mapping[str, Any]
) -> Dict[str, Any]:
    pairs = []
    positive = 0
    exact = 0
    for current_formula in current.get("formulas") or []:
        current_ids = set(current_formula.get("_matched_anchor_ids") or [])
        for legacy_formula in legacy.get("formulas") or []:
            legacy_ids = set(legacy_formula.get("_matched_anchor_ids") or [])
            union = current_ids | legacy_ids
            overlap = len(current_ids & legacy_ids) / len(union) if union else 1.0
            if current_ids & legacy_ids:
                positive += 1
            if current_ids == legacy_ids:
                exact += 1
            pairs.append(
                {
                    "current_formula_key": current_formula["formula_key"],
                    "legacy_formula_key": legacy_formula["formula_key"],
                    "current_raw_matches": len(current_ids),
                    "legacy_raw_matches": len(legacy_ids),
                    "shared_raw_matches": len(current_ids & legacy_ids),
                    "jaccard": round(overlap, 6),
                }
            )
    pairs.sort(
        key=lambda item: (
            item["shared_raw_matches"],
            item["jaccard"],
            item["current_formula_key"],
            item["legacy_formula_key"],
        ),
        reverse=True,
    )
    return {
        "pair_count": len(pairs),
        "positive_overlap_pairs": positive,
        "exact_match_set_pairs": exact,
        "top_pairs": pairs[:MAX_PAIR_DETAILS],
        "details_truncated": max(0, len(pairs) - MAX_PAIR_DETAILS),
    }


def _dry_run_summary(
    values: Iterable[Mapping[str, Any]],
    *,
    direction: str,
    horizon_minutes: int,
) -> Dict[str, Any]:
    snapshots = [
        research_evidence_contract.EvidenceSnapshot.from_dict(value)
        for value in values
    ]
    accepted_paths = Counter()
    by_compatibility: Dict[str, Counter[str]] = {}
    for snapshot in snapshots:
        formula = snapshot.to_dict()["formula"]
        if (
            str(formula.get("direction") or "").upper() != direction
            or int(formula.get("horizon_minutes") or 0) != horizon_minutes
        ):
            raise ValueError(
                "EvidenceSnapshot direction/horizon does not match comparison filters"
            )
        compatibility = snapshot.compatibility
        paths = snapshot.assessment.accepted_paths
        by_compatibility.setdefault(compatibility, Counter()).update(paths)
        accepted_paths.update(paths)
    dry_run = research_evidence_telegram_renderer.dry_run_evidence_snapshots(
        snapshots
    )
    return {
        "accepted_path_counts": dict(sorted(accepted_paths.items())),
        "accepted_path_counts_by_compatibility": {
            key: dict(sorted(value.items()))
            for key, value in sorted(by_compatibility.items())
        },
        "both_acceptance_paths_exercised": all(
            accepted_paths[path] > 0 for path in ("PROBABILITY", "ASYMMETRY")
        ),
        "both_runtime_compatibilities_rendered": all(
            compatibility in by_compatibility
            for compatibility in (
                research_evidence_contract.CURRENT_V7,
                research_evidence_contract.LEGACY_SHADOW_READ_ONLY,
            )
        ),
        "renderer": dry_run,
    }


def compare_same_anchors(
    *,
    current_formulas: Sequence[Mapping[str, Any]],
    legacy_formulas: Sequence[Mapping[str, Any]],
    anchor_rows: Sequence[Mapping[str, Any]],
    evidence_snapshots: Sequence[Mapping[str, Any]] = (),
    hype_status: Mapping[str, Any] | None = None,
    direction: str,
    horizon_minutes: int,
) -> Dict[str, Any]:
    """Compare exact V7.1 and V6.2 cohorts without outcomes or writes."""

    normalized_direction = str(direction or "").upper()
    if normalized_direction not in {"LONG", "SHORT"}:
        raise ValueError("direction must be LONG or SHORT")
    horizon = int(horizon_minutes)
    if horizon not in {60, 240, 720, 1440}:
        raise ValueError("invalid horizon_minutes")

    rows = []
    anchor_ids = []
    event_ids = []
    for source in anchor_rows:
        row = dict(source)
        event = row.get("event") if isinstance(row.get("event"), Mapping) else {}
        if str(event.get("direction") or "").upper() != normalized_direction:
            raise ValueError("anchor row direction does not match comparison direction")
        if row.get("authoritative_verified") is not True:
            raise ValueError("comparison accepts only authoritative verified anchor rows")
        rows.append(row)
        anchor_ids.append(_anchor_id(row))
        event_ids.append(_event_id(row))
    duplicate_anchor_ids = sorted(
        identifier for identifier, count in Counter(anchor_ids).items() if count > 1
    )
    duplicate_event_ids = sorted(
        identifier for identifier, count in Counter(event_ids).items() if count > 1
    )
    if duplicate_anchor_ids or duplicate_event_ids:
        raise ValueError("same-anchor comparison input contains duplicate anchors or events")
    rows.sort(
        key=lambda row: (
            _utc_iso((row.get("event") or {}).get("alert_time_utc")),
            _event_id(row),
        )
    )
    provenance_rows = sum(
        isinstance(row.get("prospective_evidence"), Mapping)
        and bool(row["prospective_evidence"].get("source_provenance"))
        for row in rows
    )
    max_pain_status_counts: Counter[str] = Counter()
    for row in rows:
        max_pain = row.get("max_pain_features")
        status = (
            str(max_pain.get("evaluation_status") or "UNKNOWN").upper()
            if isinstance(max_pain, Mapping)
            else "UNKNOWN"
        )
        max_pain_status_counts[status] += 1

    current = _cohort_summary(
        "CURRENT_V7_1",
        current_formulas,
        rows,
        direction=normalized_direction,
        horizon_minutes=horizon,
    )
    legacy = _cohort_summary(
        "LEGACY_V6_2",
        legacy_formulas,
        rows,
        direction=normalized_direction,
        horizon_minutes=horizon,
    )
    dry_run = _dry_run_summary(
        evidence_snapshots,
        direction=normalized_direction,
        horizon_minutes=horizon,
    )
    blockers = []
    if not current["formula_count"]:
        blockers.append("exact current V7.1 formula cohort is unavailable")
    elif not current["operational"]:
        blockers.append("current V7.1 cohort has no evaluable same-anchor rows")
    if not legacy["formula_count"]:
        blockers.append("exact legacy V6.2 Shadow cohort is unavailable")
    elif not legacy["operational"]:
        blockers.append("legacy V6.2 cohort has no evaluable same-anchor rows")
    if not rows:
        blockers.append("no authoritative same-anchor rows are available")
    if rows and provenance_rows != len(rows):
        blockers.append(
            "decision-time source provenance is unavailable for one or more anchors"
        )
    if max_pain_status_counts.get("UNKNOWN", 0):
        blockers.append(
            "decision-time Max Pain availability/provenance status is unknown"
        )
    if not dry_run["both_acceptance_paths_exercised"]:
        blockers.append(
            "verified EvidenceSnapshots do not exercise both PROBABILITY and ASYMMETRY paths"
        )
    if not dry_run["both_runtime_compatibilities_rendered"]:
        blockers.append(
            "Telegram Dry Run does not contain both current V7.1 and legacy V6.2 snapshots"
        )
    if current["invalid_condition_family_formula_keys"]:
        blockers.append("current cohort contains an invalid correlated condition family")
    if legacy["invalid_condition_family_formula_keys"]:
        blockers.append("legacy cohort contains an invalid correlated condition family")
    all_formula_summaries = current["formulas"] + legacy["formulas"]
    if any(
        summary["sample_inflation_prevented"] is not True
        for summary in all_formula_summaries
    ):
        blockers.append("market-episode sample inflation invariant failed")

    overlap = _overlap_summary(current, legacy)
    for summary in current["formulas"] + legacy["formulas"]:
        summary.pop("_matched_anchor_ids", None)

    return {
        "comparison_version": COMPARISON_VERSION,
        "mode": "LAB_REPLAY_SHADOW_READ_ONLY",
        "status": "READY" if not blockers else "WAITING_DATA",
        "blockers": blockers,
        "filters": {
            "direction": normalized_direction,
            "horizon_minutes": horizon,
        },
        "same_anchor_contract": {
            "authoritative_verified": bool(rows),
            "anchor_count": len(rows),
            "event_count": len(event_ids),
            "anchor_ids": sorted(anchor_ids),
            "input_shared_by_both_cohorts": True,
            "duplicate_anchor_ids": duplicate_anchor_ids,
            "duplicate_event_ids": duplicate_event_ids,
            "decision_time_provenance": {
                "authoritative_verified_rows": len(rows),
                "source_provenance_rows": provenance_rows,
                "max_pain_status_counts": dict(
                    sorted(max_pain_status_counts.items())
                ),
                "later_snapshot_lookup": False,
                "runtime_evidence_mixed": False,
            },
        },
        "current_v7_1": current,
        "legacy_v6_2": legacy,
        "cross_cohort_overlap": overlap,
        "telegram_dry_run": dry_run,
        "hype_isolation": dict(hype_status or {}),
        "safety": {
            "reads_outcomes": False,
            "database_writes": False,
            "delivery_attempts": dry_run["renderer"]["delivery_attempts"],
            "delivery_channel": dry_run["renderer"]["delivery_channel"],
            "live_effect": "NONE",
            "market_episode_policy_version": research_market_episode.POLICY_VERSION,
            "evidence_family_policy_version": (
                research_formula_families.EVIDENCE_FAMILY_VERSION
            ),
        },
    }
