"""Pure bounded candidate search for authoritative Stage-4 observations.

This module deliberately owns no database, Formula registry, Shadow, Telegram,
LIVE, or trading boundary.  It searches only the decision-time feature map of
locally validated :class:`ExplorationObservation` objects.  Future path fields
are read only after a candidate's BTC-parent-wave occurrence membership has
been frozen.

An experimental candidate passes one atomic gate: the same pattern has at
least five completed, independent BTC parent market-movement occurrences and
those occurrences already pass the probability and/or movement-asymmetry
route.  Exact-binomial and multiple-testing values are disclosed at this early
stage, but do not silently turn that user-facing experimental gate into a
later, stricter research-acceptance gate.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import itertools
import json
import math
from statistics import median
from typing import Any, Dict, Iterator, Mapping, Optional, Sequence

import research_formula_acceptance
import research_mfe_mae_efficiency
import research_no_dwell_outcome
import research_signal_formula_exploration as exploration


ENGINE_VERSION = "stage4-experimental-candidate-search-v1"
CANDIDATE_SCHEMA_VERSION = "stage4-experimental-candidate-v1"
LABEL_POLICY_VERSION = "stage4-static-no-dwell-favorable-movement-label-v1"
INDEPENDENCE_POLICY_VERSION = "stage4-btc-parent-first-opportunity-v1"
MULTIPLE_TESTING_POLICY_VERSION = "stage4-experimental-bh-disclosure-v1"

# These are the historical route floors already enforced by
# research_formula_acceptance.  The Stage-4 route intentionally omits that
# policy's control-improvement, holdout, and current-relevance claims.  Those
# claims are outside this early within-pattern contract even when no-signal
# outcome carriers are available.
PROBABILITY_HIT_RATE_FLOOR_PCT = 60.0
PROBABILITY_WILSON_LOWER_FLOOR_PCT = 45.0
PROBABILITY_MIN_MFE_MAE_RATIO = 1.10
ASYMMETRY_DIRECTIONAL_HIT_RATE_FLOOR_PCT = 45.0
ASYMMETRY_DOMINANCE_RATE_FLOOR_PCT = 70.0
ASYMMETRY_WILSON_LOWER_FLOOR_PCT = 40.0
ASYMMETRY_MIN_MFE_MAE_RATIO = 2.0

_SUPPORTED_HORIZONS = frozenset(
    int(value)
    for value in research_no_dwell_outcome.BASE_FAVORABLE_WIDTH_PCT_BY_HORIZON
)
_BOOLEAN_FEATURES = tuple(
    feature
    for feature in exploration.ALLOWED_FEATURES
    if feature != exploration.FEATURE_COMBINED_VOTE_COUNT
)
_UTC = timezone.utc


@dataclass(frozen=True)
class Stage4SearchConfig:
    """Hard bounds and the single experimental evidence floor."""

    minimum_independent_occurrences: int = (
        exploration.EXPLORATION_MIN_BTC_PARENT_MOVEMENTS
    )
    max_observations: int = 32768
    max_conditions: int = 3
    max_candidates_evaluated: int = 256
    max_candidates_returned: int = 40


def _validated_config(value: Optional[Stage4SearchConfig]) -> Stage4SearchConfig:
    config = value or Stage4SearchConfig()
    if type(config.minimum_independent_occurrences) is not int or (
        config.minimum_independent_occurrences < 5
    ):
        raise ValueError("minimum_independent_occurrences cannot be below five")
    if type(config.max_observations) is not int or not (
        1 <= config.max_observations <= 32768
    ):
        raise ValueError("max_observations must be between 1 and 32768")
    if type(config.max_conditions) is not int or not (1 <= config.max_conditions <= 3):
        raise ValueError("max_conditions must be between 1 and 3")
    if type(config.max_candidates_evaluated) is not int or not (
        1 <= config.max_candidates_evaluated <= 4096
    ):
        raise ValueError("max_candidates_evaluated must be between 1 and 4096")
    if type(config.max_candidates_returned) is not int or not (
        1 <= config.max_candidates_returned <= config.max_candidates_evaluated
    ):
        raise ValueError(
            "max_candidates_returned must be positive and within the search budget"
        )
    return config


def _utc(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field} is not an ISO timestamp") from exc
    else:
        raise ValueError(f"{field} is required")
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must be timezone-aware")
    return parsed.astimezone(_UTC)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def _fingerprint(kind: str, value: Any) -> str:
    return hashlib.sha256(f"{kind}:{_canonical_json(value)}".encode("utf-8")).hexdigest()


def _finite(value: Any, *, field: str) -> float:
    if value is None or isinstance(value, bool):
        raise ValueError(f"{field} must be finite")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field} must be finite") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field} must be finite")
    return number


def _round(value: Optional[float], digits: int = 6) -> Optional[float]:
    return None if value is None else round(float(value), digits)


def _wilson_lower_pct(successes: int, total: int, z: float = 1.96) -> Optional[float]:
    if total <= 0 or successes < 0 or successes > total:
        return None
    proportion = successes / total
    denominator = 1.0 + z * z / total
    centre = proportion + z * z / (2.0 * total)
    margin = z * math.sqrt(
        (proportion * (1.0 - proportion) + z * z / (4.0 * total)) / total
    )
    return max(0.0, (centre - margin) / denominator) * 100.0


def _one_sided_exact_binomial_p(successes: int, total: int) -> Optional[float]:
    """Return P[X >= successes] for X~Binomial(total, 0.5)."""

    if total <= 0 or successes < 0 or successes > total:
        return None
    numerator = sum(math.comb(total, value) for value in range(successes, total + 1))
    return min(1.0, numerator / (2**total))


def _bh_q_values(values: Sequence[Optional[float]]) -> list[Optional[float]]:
    indexed = [
        (index, float(value))
        for index, value in enumerate(values)
        if value is not None and math.isfinite(float(value)) and 0.0 <= float(value) <= 1.0
    ]
    if not indexed:
        return [None] * len(values)
    indexed.sort(key=lambda item: item[1])
    hypothesis_count = len(values)
    adjusted: Dict[int, float] = {}
    running = 1.0
    for reverse_index in range(len(indexed) - 1, -1, -1):
        original_index, p_value = indexed[reverse_index]
        rank = reverse_index + 1
        running = min(running, p_value * hypothesis_count / rank)
        adjusted[original_index] = min(1.0, running)
    return [adjusted.get(index) for index in range(len(values))]


def _coerce_observations(
    values: Sequence[exploration.ExplorationObservation | Mapping[str, Any]],
    *,
    horizon_minutes: int,
    analysis_as_of_utc: datetime,
    max_observations: int,
) -> list[Dict[str, Any]]:
    if not isinstance(values, (list, tuple)):
        raise TypeError("observations must be a bounded list or tuple")
    if len(values) > max_observations:
        raise ValueError("Stage-4 candidate input exceeds max_observations")
    rows: list[Dict[str, Any]] = []
    seen: set[str] = set()
    for value in values:
        observation = (
            value
            if isinstance(value, exploration.ExplorationObservation)
            else exploration.ExplorationObservation.from_dict(value)
        )
        row = observation.to_dict()
        observation_id = str(row["observation_id"])
        if observation_id in seen:
            raise ValueError("duplicate Stage-4 observation_id")
        seen.add(observation_id)
        decision = _utc(
            row["projection_decision_time_utc"],
            field="projection_decision_time_utc",
        )
        if decision > analysis_as_of_utc:
            raise ValueError("Stage-4 observation is after analysis_as_of_utc")
        outcome = row["outcome"]
        outcome_horizon = outcome.get("horizon_minutes")
        if outcome.get("status") != "UNBOUND" and outcome_horizon != horizon_minutes:
            raise ValueError("Stage-4 observation outcome horizon mismatch")
        rows.append(row)
    rows.sort(
        key=lambda row: (
            row["projection_decision_time_utc"],
            row["projection_event_id"],
            row["symbol"],
            row["direction"],
            row["observation_id"],
        )
    )
    return rows


def _predicate_catalog(rows: Sequence[Mapping[str, Any]]) -> list[Dict[str, Any]]:
    predicates: list[Dict[str, Any]] = []
    for feature in _BOOLEAN_FEATURES:
        if any(row["features"].get(feature) is True for row in rows):
            predicates.append(
                {"feature": feature, "operator": "==", "value": True}
            )
    vote_feature = exploration.FEATURE_COMBINED_VOTE_COUNT
    vote_values = {
        row["features"].get(vote_feature)
        for row in rows
        if type(row["features"].get(vote_feature)) is int
    }
    for threshold in (2, 3):
        if any(value >= threshold for value in vote_values):
            predicates.append(
                {"feature": vote_feature, "operator": ">=", "value": threshold}
            )
    return sorted(
        predicates,
        key=lambda item: (
            item["feature"],
            item["operator"],
            _canonical_json(item["value"]),
        ),
    )


def _candidate_specifications(
    predicates: Sequence[Mapping[str, Any]],
    *,
    max_conditions: int,
) -> Iterator[
    Optional[tuple[list[Dict[str, Any]], Mapping[str, Any], str]]
]:
    """Yield each valid condition/direction once; ``None`` is one rejection.

    Flattening depth, condition, and direction traversal gives the caller one
    explicit budget stop.  Family validation remains condition-set scoped, so
    a rejected set is not accidentally counted once per direction.
    """

    for depth in range(1, min(max_conditions, len(predicates)) + 1):
        for raw_conditions in itertools.combinations(predicates, depth):
            conditions = sorted(
                (dict(condition) for condition in raw_conditions),
                key=lambda item: (
                    item["feature"],
                    item["operator"],
                    _canonical_json(item["value"]),
                ),
            )
            try:
                family_policy = exploration.validate_candidate_feature_set(
                    [condition["feature"] for condition in conditions]
                )
            except ValueError:
                yield None
                continue
            for direction in ("LONG", "SHORT"):
                yield conditions, family_policy, direction


def _condition_matches(
    features: Mapping[str, Any], condition: Mapping[str, Any]
) -> bool:
    feature = str(condition["feature"])
    if feature not in features:
        return False
    actual = features[feature]
    operator = condition["operator"]
    expected = condition["value"]
    if operator == "==":
        return type(actual) is type(expected) and actual == expected
    if operator == ">=":
        return (
            not isinstance(actual, bool)
            and isinstance(actual, (int, float))
            and math.isfinite(float(actual))
            and float(actual) >= float(expected)
        )
    raise ValueError("unsupported Stage-4 predicate operator")


def _conditions_match(
    row: Mapping[str, Any], conditions: Sequence[Mapping[str, Any]]
) -> bool:
    features = row["features"]
    return all(_condition_matches(features, condition) for condition in conditions)


def _freeze_occurrence_membership(
    matches: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Freeze first opportunity cohorts without reading any outcome field."""

    grouped: Dict[str, list[Mapping[str, Any]]] = {}
    unbound: list[str] = []
    for row in matches:
        binding = row["wave_binding"]
        if binding.get("status") != "BOUND" or not binding.get(
            "btc_parent_movement_id"
        ):
            unbound.append(str(row["observation_id"]))
            continue
        grouped.setdefault(str(binding["btc_parent_movement_id"]), []).append(row)

    occurrences: list[Dict[str, Any]] = []
    for parent_id, members in sorted(grouped.items()):
        earliest = min(
            _utc(
                row["projection_decision_time_utc"],
                field="projection_decision_time_utc",
            )
            for row in members
        )
        earliest_rows = [
            row
            for row in members
            if _utc(
                row["projection_decision_time_utc"],
                field="projection_decision_time_utc",
            )
            == earliest
        ]
        # A duplicate projection for the same symbol must not give that symbol
        # extra weight.  The stable observation id is selected before outcomes.
        by_symbol: Dict[str, Mapping[str, Any]] = {}
        for row in sorted(earliest_rows, key=lambda item: item["observation_id"]):
            by_symbol.setdefault(str(row["symbol"]), row)
        evidence_rows = [by_symbol[symbol] for symbol in sorted(by_symbol)]
        occurrences.append(
            {
                "btc_parent_movement_id": parent_id,
                "first_match_time_utc": earliest.isoformat(),
                "evidence_observation_ids": [
                    str(row["observation_id"]) for row in evidence_rows
                ],
                "evidence_symbols": [str(row["symbol"]) for row in evidence_rows],
                "observed_match_observation_ids": sorted(
                    str(row["observation_id"]) for row in members
                ),
                # Kept private until membership for every occurrence is frozen.
                "_evidence_rows": evidence_rows,
            }
        )
    return {
        "occurrences": occurrences,
        "unbound_observation_ids": sorted(unbound),
    }


def _label_frozen_occurrences(
    frozen: Mapping[str, Any],
    *,
    horizon_minutes: int,
    analysis_as_of_utc: datetime,
) -> Dict[str, Any]:
    threshold = research_no_dwell_outcome.base_favorable_width_pct(
        horizon_minutes
    )
    completed: list[Dict[str, Any]] = []
    pending: list[Dict[str, Any]] = []
    unavailable: list[Dict[str, Any]] = []
    for frozen_occurrence in frozen["occurrences"]:
        occurrence = {
            key: value
            for key, value in frozen_occurrence.items()
            if not str(key).startswith("_")
        }
        start = _utc(
            occurrence["first_match_time_utc"], field="first_match_time_utc"
        )
        if start + timedelta(minutes=horizon_minutes) > analysis_as_of_utc:
            pending.append({**occurrence, "status": "PENDING_HORIZON"})
            continue
        rows = list(frozen_occurrence["_evidence_rows"])
        if any(row["outcome"].get("status") != "AVAILABLE" for row in rows):
            reasons = sorted(
                {
                    str(row["outcome"].get("reason") or "OUTCOME_UNAVAILABLE")
                    for row in rows
                    if row["outcome"].get("status") != "AVAILABLE"
                }
            )
            unavailable.append(
                {
                    **occurrence,
                    "status": "OUTCOME_UNAVAILABLE",
                    "reasons": reasons,
                }
            )
            continue

        paths = [row["outcome"]["path"] for row in rows]
        mfe = [_finite(path.get("mfe_pct"), field="mfe_pct") for path in paths]
        mae = [_finite(path.get("mae_pct"), field="mae_pct") for path in paths]
        directional = [
            _finite(path.get("directional_return_pct"), field="directional_return_pct")
            for path in paths
        ]
        hit_flags = [value >= threshold for value in mfe]
        paired_edges = [favorable - adverse for favorable, adverse in zip(mfe, mae)]
        dominance_flags = [value > 0.0 for value in paired_edges]
        favorable_move_hit = sum(hit_flags) > len(hit_flags) / 2.0
        favorable_dominance = sum(dominance_flags) > len(dominance_flags) / 2.0
        completed.append(
            {
                **occurrence,
                "status": "COMPLETED",
                "label_policy_version": LABEL_POLICY_VERSION,
                "qualifying_favorable_move_pct": threshold,
                "favorable_move_hit": favorable_move_hit,
                "favorable_move_member_hits": sum(hit_flags),
                "favorable_move_member_count": len(hit_flags),
                "favorable_dominance": favorable_dominance,
                "favorable_dominance_member_hits": sum(dominance_flags),
                "median_directional_return_pct": _round(median(directional)),
                "median_mfe_pct": _round(median(mfe)),
                "median_mae_pct": _round(median(mae)),
                "adverse_tail_mae_pct": _round(max(mae)),
                "median_paired_favorable_minus_adverse_pct": _round(
                    median(paired_edges)
                ),
                "survival_or_dwell_required": False,
            }
        )
    return {
        "completed": completed,
        "pending": pending,
        "unavailable": unavailable,
        "unbound_observation_ids": list(frozen["unbound_observation_ids"]),
        "qualifying_favorable_move_pct": threshold,
    }


def _route_metrics(
    labeled: Mapping[str, Any], *, minimum_occurrences: int
) -> Dict[str, Any]:
    completed = list(labeled["completed"])
    sample_size = len(completed)
    successes = sum(item["favorable_move_hit"] is True for item in completed)
    dominance_successes = sum(
        item["favorable_dominance"] is True for item in completed
    )
    hit_rate = successes / sample_size * 100.0 if sample_size else None
    dominance_rate = (
        dominance_successes / sample_size * 100.0 if sample_size else None
    )
    hit_wilson = _wilson_lower_pct(successes, sample_size)
    dominance_wilson = _wilson_lower_pct(dominance_successes, sample_size)
    median_mfe = (
        median(float(item["median_mfe_pct"]) for item in completed)
        if completed
        else None
    )
    median_mae = (
        median(float(item["median_mae_pct"]) for item in completed)
        if completed
        else None
    )
    paired_edge = (
        median(
            float(item["median_paired_favorable_minus_adverse_pct"])
            for item in completed
        )
        if completed
        else None
    )
    adverse_tail = (
        max(float(item["adverse_tail_mae_pct"]) for item in completed)
        if completed
        else None
    )
    efficiency = research_mfe_mae_efficiency.classify(median_mfe, median_mae)
    coverage_complete = not labeled["unavailable"] and not labeled[
        "unbound_observation_ids"
    ]
    common = {
        "minimum independent BTC parent occurrences": sample_size
        >= minimum_occurrences,
        "mature matched occurrence coverage complete": coverage_complete,
        "wide favorable movement floor": bool(
            median_mfe is not None
            and median_mfe >= labeled["qualifying_favorable_move_pct"]
        ),
    }
    probability = {
        "hit rate": bool(
            hit_rate is not None and hit_rate >= PROBABILITY_HIT_RATE_FLOOR_PCT
        ),
        "Wilson lower bound": bool(
            hit_wilson is not None
            and hit_wilson >= PROBABILITY_WILSON_LOWER_FLOOR_PCT
        ),
        "minimum favorable/adverse efficiency": efficiency.meets_threshold(
            PROBABILITY_MIN_MFE_MAE_RATIO
        ),
    }
    asymmetry = {
        "minimum directional hit rate": bool(
            hit_rate is not None
            and hit_rate >= ASYMMETRY_DIRECTIONAL_HIT_RATE_FLOOR_PCT
        ),
        "favorable dominance rate": bool(
            dominance_rate is not None
            and dominance_rate >= ASYMMETRY_DOMINANCE_RATE_FLOOR_PCT
        ),
        "favorable dominance Wilson lower bound": bool(
            dominance_wilson is not None
            and dominance_wilson >= ASYMMETRY_WILSON_LOWER_FLOOR_PCT
        ),
        "strong favorable/adverse efficiency": efficiency.meets_threshold(
            ASYMMETRY_MIN_MFE_MAE_RATIO
        ),
        "positive paired favorable/adverse edge": bool(
            paired_edge is not None and paired_edge > 0.0
        ),
    }
    common_passed = all(common.values())
    probability_passed = common_passed and all(probability.values())
    asymmetry_passed = common_passed and all(asymmetry.values())
    accepted_paths = [
        name
        for name, passed in (
            ("PROBABILITY", probability_passed),
            ("ASYMMETRY", asymmetry_passed),
        )
        if passed
    ]
    return {
        "sample_size": sample_size,
        "successes": successes,
        "hit_rate_pct": _round(hit_rate, 4),
        "wilson_95_lower_pct": _round(hit_wilson, 4),
        "favorable_dominance_successes": dominance_successes,
        "favorable_dominance_rate_pct": _round(dominance_rate, 4),
        "favorable_dominance_wilson_95_lower_pct": _round(
            dominance_wilson, 4
        ),
        "median_mfe_pct": _round(median_mfe),
        "median_mae_pct": _round(median_mae),
        "adverse_tail_mae_pct": _round(adverse_tail),
        "median_paired_favorable_minus_adverse_pct": _round(paired_edge),
        "median_mfe_mae_ratio": _round(efficiency.ratio),
        "median_mfe_mae_ratio_state": efficiency.state,
        "probability_exact_binomial_p_value": _round(
            _one_sided_exact_binomial_p(successes, sample_size), 8
        ),
        "asymmetry_exact_binomial_p_value": _round(
            _one_sided_exact_binomial_p(dominance_successes, sample_size), 8
        ),
        "common_gates": common,
        "routes": {
            "PROBABILITY": {"passed": probability_passed, "gates": probability},
            "ASYMMETRY": {"passed": asymmetry_passed, "gates": asymmetry},
        },
        "accepted_paths": accepted_paths,
        "experimental_formula_eligible": bool(accepted_paths),
        "missing_by_route": {
            "COMMON": [name for name, passed in common.items() if not passed],
            "PROBABILITY": [
                name for name, passed in probability.items() if not passed
            ],
            "ASYMMETRY": [
                name for name, passed in asymmetry.items() if not passed
            ],
        },
    }


def _candidate_key(
    *, direction: str, horizon_minutes: int, conditions: Sequence[Mapping[str, Any]]
) -> str:
    return _fingerprint(
        "stage4-experimental-candidate",
        {
            "engine_version": ENGINE_VERSION,
            "candidate_schema_version": CANDIDATE_SCHEMA_VERSION,
            "feature_schema_version": exploration.FEATURE_SCHEMA_VERSION,
            "label_policy_version": LABEL_POLICY_VERSION,
            "independence_policy_version": INDEPENDENCE_POLICY_VERSION,
            "direction": direction,
            "horizon_minutes": horizon_minutes,
            "conditions": list(conditions),
        },
    )


def _candidate_formula_text(
    direction: str, conditions: Sequence[Mapping[str, Any]]
) -> str:
    return f"{direction} WHEN " + " AND ".join(
        f"{condition['feature']} {condition['operator']} "
        f"{json.dumps(condition['value'], ensure_ascii=False)}"
        for condition in conditions
    )


def _candidate_sort_key(candidate: Mapping[str, Any]) -> tuple[Any, ...]:
    metrics = candidate["metrics"]
    return (
        int(candidate["experimental_formula_eligible"]),
        len(metrics["accepted_paths"]),
        float(metrics.get("wilson_95_lower_pct") or 0.0),
        float(metrics.get("favorable_dominance_wilson_95_lower_pct") or 0.0),
        int(metrics.get("sample_size") or 0),
        -len(candidate["conditions"]),
        str(candidate["candidate_key"]),
    )


def search_experimental_candidates(
    observations: Sequence[
        exploration.ExplorationObservation | Mapping[str, Any]
    ],
    *,
    horizon_minutes: int,
    analysis_as_of_utc: Any,
    config: Optional[Stage4SearchConfig] = None,
) -> Dict[str, Any]:
    """Search a bounded Stage-4 page without producing downstream authority."""

    if type(horizon_minutes) is not int or horizon_minutes not in _SUPPORTED_HORIZONS:
        raise ValueError("unsupported Stage-4 candidate horizon")
    active = _validated_config(config)
    as_of = _utc(analysis_as_of_utc, field="analysis_as_of_utc")
    rows = _coerce_observations(
        observations,
        horizon_minutes=horizon_minutes,
        analysis_as_of_utc=as_of,
        max_observations=active.max_observations,
    )
    predicates = _predicate_catalog(rows)
    evaluated: list[Dict[str, Any]] = []
    family_policy_rejections = 0
    empty_match_rejections = 0
    search_budget_exhausted = False

    for specification in _candidate_specifications(
        predicates,
        max_conditions=active.max_conditions,
    ):
        if len(evaluated) >= active.max_candidates_evaluated:
            search_budget_exhausted = True
            break
        if specification is None:
            family_policy_rejections += 1
            continue
        conditions, family_policy, direction = specification
        matches = [
            row
            for row in rows
            if row["direction"] == direction
            and _conditions_match(row, conditions)
        ]
        if not matches:
            empty_match_rejections += 1
            continue
        # This boundary is intentional: occurrence membership is complete
        # before _label_frozen_occurrences can inspect future path outcomes.
        frozen = _freeze_occurrence_membership(matches)
        labeled = _label_frozen_occurrences(
            frozen,
            horizon_minutes=horizon_minutes,
            analysis_as_of_utc=as_of,
        )
        metrics = _route_metrics(
            labeled,
            minimum_occurrences=active.minimum_independent_occurrences,
        )
        raw_match_ids = sorted(str(row["observation_id"]) for row in matches)
        match_set_sha = _fingerprint(
            "stage4-candidate-match-set",
            {"direction": direction, "observation_ids": raw_match_ids},
        )
        candidate_key = _candidate_key(
            direction=direction,
            horizon_minutes=horizon_minutes,
            conditions=conditions,
        )
        evaluated.append(
            {
                "candidate_key": candidate_key,
                "candidate_schema_version": CANDIDATE_SCHEMA_VERSION,
                "engine_version": ENGINE_VERSION,
                "feature_schema_version": exploration.FEATURE_SCHEMA_VERSION,
                "label_policy_version": LABEL_POLICY_VERSION,
                "independence_policy_version": INDEPENDENCE_POLICY_VERSION,
                "direction": direction,
                "horizon_minutes": horizon_minutes,
                "conditions": conditions,
                "formula_text": _candidate_formula_text(direction, conditions),
                "condition_source_closure": family_policy["source_closure"],
                "condition_evidence_sources": family_policy[
                    "deduplicated_sources"
                ],
                "raw_match_count": len(matches),
                "match_set_sha256": match_set_sha,
                "occurrence_counts": {
                    "independent_parent_movements_seen": len(
                        frozen["occurrences"]
                    ),
                    "completed": len(labeled["completed"]),
                    "pending_horizon": len(labeled["pending"]),
                    "mature_outcome_unavailable": len(labeled["unavailable"]),
                    "wave_unbound_matches": len(
                        labeled["unbound_observation_ids"]
                    ),
                },
                "completed_occurrences": labeled["completed"],
                "pending_occurrences": labeled["pending"],
                "unavailable_occurrences": labeled["unavailable"],
                "metrics": metrics,
                "accepted_paths": list(metrics["accepted_paths"]),
                "experimental_formula_eligible": metrics[
                    "experimental_formula_eligible"
                ],
                "eligibility_gate": {
                    "atomic": True,
                    "minimum_independent_occurrences": (
                        active.minimum_independent_occurrences
                    ),
                    "independence_unit": "DISTINCT_BTC_PARENT_MARKET_MOVEMENT",
                    "passed": metrics["experimental_formula_eligible"],
                    "separate_later_probability_gate": False,
                },
                "controls_evaluated": False,
                "holdout_evaluated": False,
                "experimental_caveats": [
                    "NO_CONTROL_RELATIVE_CLAIM",
                    "NO_HOLDOUT_CLAIM",
                    "MULTIPLE_TESTING_DISCLOSURE_NOT_CONFIRMATORY_GATE",
                ],
                "formula_registry_effect": "NONE",
                "authority_effect": "NONE",
                "delivery_channel": "NONE",
                "live_eligible": False,
                "telegram_delivery_allowed": False,
                "trade_execution_allowed": False,
            }
        )

    probability_p = [
        candidate["metrics"].get("probability_exact_binomial_p_value")
        for candidate in evaluated
    ]
    asymmetry_p = [
        candidate["metrics"].get("asymmetry_exact_binomial_p_value")
        for candidate in evaluated
    ]
    q_values = _bh_q_values([*probability_p, *asymmetry_p])
    split = len(evaluated)
    for index, candidate in enumerate(evaluated):
        candidate["multiple_testing"] = {
            "policy_version": MULTIPLE_TESTING_POLICY_VERSION,
            "method": "BENJAMINI_HOCHBERG_JOINT_PROBABILITY_ASYMMETRY_DIRECTIONS",
            "hypotheses_in_family": len(q_values),
            "probability_q_value": _round(q_values[index], 8),
            "asymmetry_q_value": _round(q_values[split + index], 8),
            "decision_effect": "DISCLOSURE_ONLY_EXPERIMENTAL",
            "eligibility_changed": False,
        }

    # Equivalent frozen observation sets are one display result.  Every
    # searched condition set nevertheless remains in the BH disclosure family.
    by_match_set: Dict[str, list[Dict[str, Any]]] = {}
    for candidate in evaluated:
        by_match_set.setdefault(candidate["match_set_sha256"], []).append(candidate)
    displayed: list[Dict[str, Any]] = []
    duplicate_candidates_collapsed = 0
    for group in by_match_set.values():
        group.sort(
            key=lambda item: (
                len(item["conditions"]),
                _canonical_json(item["conditions"]),
                item["candidate_key"],
            )
        )
        champion = group[0]
        duplicate_candidates_collapsed += len(group) - 1
        champion["display_equivalent_candidates"] = len(group)
        champion["display_equivalent_candidate_keys"] = sorted(
            item["candidate_key"] for item in group
        )
        displayed.append(champion)
    displayed.sort(key=_candidate_sort_key, reverse=True)
    displayed = displayed[: active.max_candidates_returned]
    eligible = [
        candidate
        for candidate in displayed
        if candidate["experimental_formula_eligible"]
    ]
    result_status = (
        "EMPTY_CORPUS"
        if not rows
        else "ELIGIBLE_EXPERIMENTAL_CANDIDATES_FOUND"
        if eligible
        else "NO_ELIGIBLE_EXPERIMENTAL_CANDIDATES"
    )
    output = {
        "available": bool(rows),
        "status": result_status,
        "engine_version": ENGINE_VERSION,
        "candidate_schema_version": CANDIDATE_SCHEMA_VERSION,
        "feature_schema_version": exploration.FEATURE_SCHEMA_VERSION,
        "label_policy_version": LABEL_POLICY_VERSION,
        "independence_policy_version": INDEPENDENCE_POLICY_VERSION,
        "historical_threshold_source_policy_version": (
            research_formula_acceptance.POLICY_VERSION
        ),
        "analysis_as_of_utc": as_of.isoformat(),
        "horizon_minutes": horizon_minutes,
        "config": asdict(active),
        "qualifying_favorable_move_pct": (
            research_no_dwell_outcome.base_favorable_width_pct(horizon_minutes)
        ),
        "counts": {
            "observations": len(rows),
            "predicates": len(predicates),
            "candidates_evaluated": len(evaluated),
            "display_candidates": len(displayed),
            "display_equivalent_candidates_collapsed": (
                duplicate_candidates_collapsed
            ),
            "eligible_experimental_candidates": len(eligible),
            "family_policy_rejections": family_policy_rejections,
            "empty_direction_match_rejections": empty_match_rejections,
            "hypotheses_disclosed": len(q_values),
        },
        "search_budget_exhausted": search_budget_exhausted,
        "candidates": displayed,
        "eligible_candidates": eligible,
        "atomic_eligibility": {
            "minimum_independent_occurrences": active.minimum_independent_occurrences,
            "independence_unit": "DISTINCT_BTC_PARENT_MARKET_MOVEMENT",
            "requirement": (
                "the same pattern has the minimum completed independent "
                "occurrences and those outcomes already pass probability "
                "and/or favorable movement asymmetry"
            ),
            "separate_later_probability_gate": False,
        },
        "statistical_scope": {
            "controls_evaluated": False,
            "holdout_evaluated": False,
            "multiple_testing_effect": "DISCLOSURE_ONLY_EXPERIMENTAL",
            "claim": "WITHIN_PATTERN_EXPERIMENTAL_EVIDENCE_ONLY",
        },
        "ready_for_candidate_search": True,
        "formula_registry_effect": "NONE",
        "authority_effect": "NONE",
        "delivery_channel": "NONE",
        "live_eligible": False,
        "telegram_delivery_allowed": False,
        "trade_execution_allowed": False,
    }
    output["search_receipt_sha256"] = _fingerprint(
        "stage4-candidate-search-receipt", output
    )
    return output


def descriptor() -> Dict[str, Any]:
    return {
        "engine_version": ENGINE_VERSION,
        "candidate_schema_version": CANDIDATE_SCHEMA_VERSION,
        "feature_schema_version": exploration.FEATURE_SCHEMA_VERSION,
        "label_policy_version": LABEL_POLICY_VERSION,
        "independence_policy_version": INDEPENDENCE_POLICY_VERSION,
        "multiple_testing_policy_version": MULTIPLE_TESTING_POLICY_VERSION,
        "minimum_independent_occurrences": (
            exploration.EXPLORATION_MIN_BTC_PARENT_MOVEMENTS
        ),
        "independence_unit": "DISTINCT_BTC_PARENT_MARKET_MOVEMENT",
        "fixed_time_spacing_rule": None,
        "label": (
            "MFE reaches the versioned static horizon width; no dwell or "
            "survival requirement"
        ),
        "probability_floors": {
            "hit_rate_pct": PROBABILITY_HIT_RATE_FLOOR_PCT,
            "wilson_95_lower_pct": PROBABILITY_WILSON_LOWER_FLOOR_PCT,
            "mfe_mae_ratio": PROBABILITY_MIN_MFE_MAE_RATIO,
        },
        "asymmetry_floors": {
            "directional_hit_rate_pct": (
                ASYMMETRY_DIRECTIONAL_HIT_RATE_FLOOR_PCT
            ),
            "dominance_rate_pct": ASYMMETRY_DOMINANCE_RATE_FLOOR_PCT,
            "dominance_wilson_95_lower_pct": (
                ASYMMETRY_WILSON_LOWER_FLOOR_PCT
            ),
            "mfe_mae_ratio": ASYMMETRY_MIN_MFE_MAE_RATIO,
            "paired_edge": "POSITIVE",
        },
        "outcome_fields_allowed_as_predicates": False,
        "max_conditions": 3,
        "max_candidates_evaluated": 256,
        "multiple_testing_effect": "DISCLOSURE_ONLY_EXPERIMENTAL",
        "control_relative_claim": False,
        "holdout_claim": False,
        "formula_registry_effect": "NONE",
        "authority_effect": "NONE",
        "delivery_channel": "NONE",
        "live_eligible": False,
        "telegram_delivery_allowed": False,
        "trade_execution_allowed": False,
    }
