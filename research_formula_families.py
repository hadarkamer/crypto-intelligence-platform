"""Deterministic feature- and evidence-family policy for formula research.

This module is deliberately independent from the discovery engine and storage
layer.  It provides two bounded pieces of policy:

* correlated decision features cannot be stacked in a hierarchical formula
  unless the search configuration carries a non-empty written exception; and
* formulas supported by the same prospective observations are collapsed into
  deterministic evidence families with one champion.

Neither policy requires more than one symbol.  A repeatable effect observed on
one coin remains valid evidence; family grouping only prevents duplicated or
heavily overlapping formula claims from being counted as independent findings.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Mapping, Sequence


EVIDENCE_FAMILY_VERSION = "formula-evidence-family-v2-interval-duration-overlap"

_MAX_PAIN_COMPONENT_MARKERS = (
    "distance_pct",
    "target_distance",
    "near_share",
    "near_amount",
    "far_amount",
    "near_far",
    "consensus_hits",
    "consensus_total",
    "gap_consensus",
    "gap.advantage",
    "gap.near_distance",
    "gap.far_distance",
    "balance.near_share",
    "magnet.",
)


def _normalized(feature: Any) -> str:
    return str(feature or "").strip().lower().replace("-", "_")


def is_max_pain_composite(feature: Any) -> bool:
    """Whether a feature is an already-composed Max Pain score/verdict."""
    name = _normalized(feature)
    explicit_context = "maxpain" in name or "max_pain" in name
    composite_marker = any(
        marker in name
        for marker in (
            "score",
            "points",
            "confirmation",
            "confirmed",
            "strength",
            "status",
        )
    )
    category_verdict = name.startswith("category.") and any(
        marker in name for marker in ("max_pain", "maxpain")
    )
    return bool(category_verdict or (explicit_context and composite_marker))


def is_max_pain_component(feature: Any) -> bool:
    """Whether a feature is a raw component used to construct Max Pain state."""
    name = _normalized(feature)
    if is_max_pain_composite(name):
        return False
    # Migration-007 exposes a versioned namespace made only from the coherent
    # seven-timeframe archive.  Every non-composite value in that namespace is
    # one Max-Pain evidence family, including liquidity, cluster, consensus,
    # distance and prior-snapshot delta fields.  Treating unknown descendants
    # as unrelated fallback families would let a 4/5-condition formula count
    # the same Max-Pain snapshot several times as independent information.
    if name.startswith("max_pain."):
        return True
    if any(marker in name for marker in _MAX_PAIN_COMPONENT_MARKERS):
        return True
    return name.startswith("category.near_max_pain")


def feature_correlation_family(feature: Any) -> str:
    """Map a flattened feature to a conservative economic evidence family."""
    name = _normalized(feature)
    if is_max_pain_composite(name):
        return "max_pain_composite"
    if is_max_pain_component(name):
        return "max_pain_components"
    if "price_oi_state" in name:
        return "price_oi_composite"
    if "futures" in name and ("cvd" in name or "taker" in name or "flow" in name):
        return "futures_cvd"
    if "spot" in name and ("cvd" in name or "taker" in name or "flow" in name):
        return "spot_cvd"
    if "oi_change" in name or "open_interest" in name:
        return "open_interest"
    if "price_change" in name or name.startswith("aligned_log") and "price" in name:
        return "price"
    if "session" in name or name.startswith("time.market_"):
        return "market_session"
    if name.startswith("sequence.") or name.startswith("aligned_sequence."):
        return "alert_sequence"
    if name in {"model.alert_score", "model.snapshot.score", "model.snapshot.total_score"}:
        return "bot_composite_score"
    if "alignment" in name:
        return "cross_market_alignment"
    # Unknown model fields are not assumed correlated merely because they came
    # from the same immutable snapshot.  The exact path is a stable fallback.
    return f"feature:{name}"


def _justified_family_exceptions(values: Iterable[Any]) -> set[str]:
    """Parse ``family: written justification`` entries from frozen config."""
    allowed: set[str] = set()
    for raw in values:
        family, separator, reason = str(raw or "").partition(":")
        if separator and family.strip() and len(reason.strip()) >= 8:
            allowed.add(family.strip())
    return allowed


def condition_family_policy(
    conditions: Sequence[Mapping[str, Any]],
    *,
    justified_exceptions: Sequence[str] = (),
    enforce_correlated_families: bool = True,
) -> Dict[str, Any]:
    """Validate one formula without reading outcomes.

    Composite Max Pain outputs and their raw components are never allowed in
    the same formula, even when a general correlated-family exception exists.
    """
    features = [str(condition.get("feature") or "") for condition in conditions]
    families = [feature_correlation_family(feature) for feature in features]
    reasons: list[str] = []
    if any(is_max_pain_composite(feature) for feature in features) and any(
        is_max_pain_component(feature) for feature in features
    ):
        reasons.append("composite Max Pain evidence cannot be combined with its components")
    if enforce_correlated_families:
        exceptions = _justified_family_exceptions(justified_exceptions)
        counts: Dict[str, int] = {}
        for family in families:
            counts[family] = counts.get(family, 0) + 1
        reasons.extend(
            f"correlated family repeated without written exception: {family}"
            for family, count in sorted(counts.items())
            if count > 1 and family not in exceptions
        )
    return {
        "valid": not reasons,
        "families": families,
        "reasons": reasons,
    }


def evidence_fingerprint(evidence_keys: Iterable[Any]) -> str:
    normalized = sorted({str(value) for value in evidence_keys})
    payload = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def evidence_overlap(left: Iterable[Any], right: Iterable[Any]) -> float:
    left_set = {str(value) for value in left}
    right_set = {str(value) for value in right}
    union = left_set | right_set
    return len(left_set & right_set) / len(union) if union else 1.0


def _interval_timestamp(value: Any) -> float:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).timestamp()


def _merged_evidence_intervals(
    values: Iterable[Any],
) -> Dict[str, list[tuple[float, float]]]:
    grouped: Dict[str, list[tuple[float, float]]] = {}
    for value in values:
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
            continue
        if len(value) != 3:
            continue
        partition = str(value[0])
        try:
            start = _interval_timestamp(value[1])
            end = _interval_timestamp(value[2])
        except (TypeError, ValueError, OverflowError):
            continue
        if end <= start:
            continue
        grouped.setdefault(partition, []).append((start, end))
    merged: Dict[str, list[tuple[float, float]]] = {}
    for partition, intervals in grouped.items():
        combined: list[tuple[float, float]] = []
        for start, end in sorted(intervals):
            if combined and start <= combined[-1][1]:
                combined[-1] = (combined[-1][0], max(combined[-1][1], end))
            else:
                combined.append((start, end))
        merged[partition] = combined
    return merged


def evidence_interval_overlap(left: Iterable[Any], right: Iterable[Any]) -> float:
    """Duration Jaccard across compact, partitioned evidence intervals."""

    left_by_partition = _merged_evidence_intervals(left)
    right_by_partition = _merged_evidence_intervals(right)
    left_total = sum(
        end - start
        for intervals in left_by_partition.values()
        for start, end in intervals
    )
    right_total = sum(
        end - start
        for intervals in right_by_partition.values()
        for start, end in intervals
    )
    intersection = 0.0
    for partition in set(left_by_partition) & set(right_by_partition):
        left_intervals = left_by_partition[partition]
        right_intervals = right_by_partition[partition]
        left_index = right_index = 0
        while left_index < len(left_intervals) and right_index < len(
            right_intervals
        ):
            left_start, left_end = left_intervals[left_index]
            right_start, right_end = right_intervals[right_index]
            intersection += max(
                0.0, min(left_end, right_end) - max(left_start, right_start)
            )
            if left_end <= right_end:
                left_index += 1
            else:
                right_index += 1
    union = left_total + right_total - intersection
    return intersection / union if union > 0.0 else 1.0


def _formula_priority(formula: Mapping[str, Any]) -> tuple[Any, ...]:
    stage_order = {
        "SHADOW": 4,
        "HOLDOUT_PASSED": 3,
        "BACKTESTED": 2,
        "DISCOVERED": 1,
    }
    holdout = formula.get("holdout_metrics")
    holdout_n = int(holdout.get("sample_size") or 0) if isinstance(holdout, Mapping) else 0
    return (
        stage_order.get(str(formula.get("recommended_stage") or ""), 0),
        float(formula.get("ranking_score") or 0.0),
        holdout_n,
        -int(formula.get("condition_count") or len(formula.get("conditions") or [])),
        str(formula.get("formula_key") or ""),
    )


def _family_id(formulas: Sequence[Mapping[str, Any]]) -> str:
    members = sorted(
        str(formula.get("_evidence_fingerprint") or "") for formula in formulas
    )
    first = formulas[0] if formulas else {}
    payload = {
        "version": EVIDENCE_FAMILY_VERSION,
        "direction": str(first.get("direction") or ""),
        "horizon_minutes": int(first.get("horizon_minutes") or 0),
        "member_fingerprints": members,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def group_formula_evidence(
    formulas: Sequence[Mapping[str, Any]],
    *,
    overlap_threshold: float = 0.75,
) -> Dict[str, Any]:
    """Collapse exact evidence duplicates and select one champion per family."""
    threshold = min(1.0, max(0.0, float(overlap_threshold)))
    prepared: list[Dict[str, Any]] = []
    for source in formulas:
        formula = dict(source)
        evidence_keys = tuple(sorted({str(value) for value in formula.get("_evidence_keys") or ()}))
        evidence_intervals = tuple(formula.get("_evidence_intervals") or ())
        fingerprint = evidence_fingerprint(evidence_keys)
        formula["_evidence_keys"] = evidence_keys
        formula["_evidence_intervals"] = evidence_intervals
        formula["_evidence_fingerprint"] = fingerprint
        prepared.append(formula)
    prepared.sort(key=_formula_priority, reverse=True)

    exact_groups: Dict[tuple[str, int, str], list[Dict[str, Any]]] = {}
    for formula in prepared:
        exact_key = (
            str(formula.get("direction") or ""),
            int(formula.get("horizon_minutes") or 0),
            str(formula["_evidence_fingerprint"]),
        )
        exact_groups.setdefault(exact_key, []).append(formula)

    unique: list[Dict[str, Any]] = []
    exact_collapsed = 0
    for group in exact_groups.values():
        group.sort(key=_formula_priority, reverse=True)
        champion = group[0]
        duplicate_keys = sorted(
            str(item.get("formula_key") or "") for item in group[1:]
        )
        champion["_exact_duplicate_formula_keys"] = duplicate_keys
        exact_collapsed += len(duplicate_keys)
        unique.append(champion)
    unique.sort(key=_formula_priority, reverse=True)

    families: list[list[Dict[str, Any]]] = []
    for formula in unique:
        best_index = None
        best_overlap = -1.0
        for index, family in enumerate(families):
            leader = family[0]
            if (
                formula.get("direction") != leader.get("direction")
                or int(formula.get("horizon_minutes") or 0)
                != int(leader.get("horizon_minutes") or 0)
            ):
                continue
            formula_intervals = formula.get("_evidence_intervals") or ()
            leader_intervals = leader.get("_evidence_intervals") or ()
            overlap = (
                evidence_interval_overlap(formula_intervals, leader_intervals)
                if formula_intervals and leader_intervals
                else evidence_overlap(
                    formula.get("_evidence_keys") or (),
                    leader.get("_evidence_keys") or (),
                )
            )
            if overlap >= threshold and overlap > best_overlap:
                best_index = index
                best_overlap = overlap
        if best_index is None:
            families.append([formula])
        else:
            families[best_index].append(formula)

    champions: list[Dict[str, Any]] = []
    summaries: list[Dict[str, Any]] = []
    for family in families:
        family.sort(key=_formula_priority, reverse=True)
        champion = family[0]
        identifier = _family_id(family)
        member_keys = sorted(str(item.get("formula_key") or "") for item in family)
        metadata = {
            "version": EVIDENCE_FAMILY_VERSION,
            "family_id": identifier,
            "evidence_fingerprint": champion["_evidence_fingerprint"],
            "overlap_threshold": threshold,
            "family_member_count": len(family),
            "family_member_formula_keys": member_keys,
            "exact_duplicate_count": len(
                champion.get("_exact_duplicate_formula_keys") or []
            ),
            "exact_duplicate_formula_keys": list(
                champion.get("_exact_duplicate_formula_keys") or []
            ),
            "champion": True,
            "symbol_breadth_required": False,
        }
        multiple_testing = dict(champion.get("multiple_testing") or {})
        multiple_testing["evidence_family"] = metadata
        champion["multiple_testing"] = multiple_testing
        champion.pop("_evidence_keys", None)
        champion.pop("_evidence_intervals", None)
        champion.pop("_evidence_fingerprint", None)
        champion.pop("_exact_duplicate_formula_keys", None)
        champions.append(champion)
        summaries.append(
            {
                "family_id": identifier,
                "champion_formula_key": str(champion.get("formula_key") or ""),
                "member_count": len(family),
                "member_formula_keys": member_keys,
                "symbol_breadth_required": False,
            }
        )
    champions.sort(key=_formula_priority, reverse=True)
    summaries.sort(key=lambda item: item["family_id"])
    return {
        "champions": champions,
        "families": summaries,
        "input_formulas": len(formulas),
        "exact_duplicates_collapsed": exact_collapsed,
        "overlap_families": len(families),
        "policy_version": EVIDENCE_FAMILY_VERSION,
    }
