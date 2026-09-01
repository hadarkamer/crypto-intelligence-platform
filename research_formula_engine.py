"""Deterministic automatic formula discovery for archived bot alerts.

The engine searches decision-time features only. Canonical spot path outcomes
are labels and are never exposed to a condition.  Thresholds are learned from
the earlier chronological discovery partition, frozen, and then evaluated on
the later holdout partition.

Candidate structures are learned from the initial Fit partition. Numeric
thresholds are re-fitted from expanding prior-only prefixes across versioned
Walk-forward Selection folds before the final formula is frozen for the
untouched outer Test.

This module is pure computation: it does not write PostgreSQL, send Telegram
messages, or alter any production score/threshold.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from statistics import mean, median
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

import market_session_baseline
import research_formula_acceptance
import research_formula_families
import research_market_episode
import research_mfe_mae_efficiency
import research_no_dwell_outcome

ENGINE_VERSION = (
    "formula-discovery-v7.2-walk-forward-watermarked-condition-family-fail-closed"
)
FORMULA_SCHEMA_VERSION = "research-formula-v7-adaptive-evidence"
LEGACY_V6_ENGINE_VERSION = (
    "formula-discovery-v6.2-first-touch-maxpain-hierarchical-holdout-isolated"
)
LEGACY_V6_FORMULA_SCHEMA_VERSION = "research-formula-v6-first-touch-maxpain"
ALLOWED_OPERATORS = {">=", "<=", "=="}
WALK_FORWARD_POLICY_VERSION = "formula-walk-forward-v1-expanding-refit"
PURGE_POLICY_VERSION = "market-episode-boundary-purge-v1"
EMBARGO_POLICY_VERSION = "full-outcome-horizon-embargo-v1"

# These values describe archive/runtime mechanics rather than market state.
# They remain visible for diagnostics but may never become formula predicates.
_FORBIDDEN_CANDIDATE_FEATURES = {
    "event.strategy_version",
    "event.code_version",
    "time.utc_hour",
    "time.utc_weekday",
    "time.utc_weekday_name",
    "time.is_calendar_weekend_utc",
    "time.fixed_utc_session_bucket",
    "time.market_utc_offset_minutes",
}
_FORBIDDEN_CANDIDATE_SUFFIXES = (
    "_history_samples",
    "_session_matched_samples",
    "_session_matched_effective_samples",
    ".prior_points",
    ".sufficient_history",
)
_UNNORMALIZED_ABSOLUTE_FLOW_SUFFIXES = (
    "futures_continuous_cvd_change_usd",
    "spot_continuous_cvd_change_usd",
    "futures_api_cvd_change_usd",
    "spot_api_cvd_change_usd",
)


def candidate_feature_allowed(feature: str) -> bool:
    """Whether a flattened decision-time field represents market evidence."""
    name = str(feature or "")
    if not name or name in _FORBIDDEN_CANDIDATE_FEATURES:
        return False
    if name.endswith(_FORBIDDEN_CANDIDATE_SUFFIXES):
        return False
    if name.endswith(".age_minutes") or name.endswith(".available"):
        return False
    if name.endswith(".complete"):
        return False
    return True


def discovery_candidate_feature_allowed(feature: str) -> bool:
    """Whether v7 may learn a new predicate from this decision feature."""

    name = str(feature or "")
    if not candidate_feature_allowed(name):
        return False
    # Absolute dollar CVD thresholds are not comparable across symbols.  The
    # feature matrix already exposes prior-only, same-symbol, session-matched
    # percentile forms under ``historical.*``; v7 discovery must use those
    # normalized measures instead of rediscovering formula-3019-style dollar
    # cutoffs.  Raw values stay frozen for audit and legacy formula evaluation.
    if any(token in name for token in _UNNORMALIZED_ABSOLUTE_FLOW_SUFFIXES):
        return "percentile_session_matched" in name
    return True


MIN_MEDIAN_MFE_BY_HORIZON = (
    research_no_dwell_outcome.BASE_FAVORABLE_WIDTH_PCT_BY_HORIZON
)
# Market Episodes replace raw-alert counts. Ten effective controls matches the
# strict historical holdout floor and keeps the 120d/70:30 contract reachable;
# the former raw-row floor of 30 would geometrically require more independent
# holdout days than the default 36-day partition contains.
SESSION_BASELINE_MIN_EFFECTIVE_SAMPLES = 10.0


@dataclass(frozen=True)
class DiscoveryConfig:
    discovery_fraction: float = 0.70
    fit_fraction_within_development: float = 0.70
    min_discovery_samples: int = 12
    min_holdout_samples: int = 6
    strict_discovery_samples: int = 20
    strict_holdout_samples: int = 10
    numeric_quantiles: tuple[float, ...] = (0.20, 0.35, 0.50, 0.65, 0.80)
    max_single_predicates: int = 90
    max_pair_candidates: int = 3200
    max_triple_candidates: int = 2200
    max_candidates_evaluated: int = 8000
    max_formulas_returned: int = 80
    # Four/five-condition discovery is intentionally opt-in. It extends only a
    # bounded beam of stable chronological triple parents; default v5-depth
    # single/pair/triple search remains unchanged when this flag is false.
    hierarchical_search_enabled: bool = False
    hierarchical_max_conditions: int = 5
    hierarchical_beam_width: int = 24
    hierarchical_extension_predicates: int = 30
    max_quad_candidates: int = 400
    max_quint_candidates: int = 160
    hierarchical_min_discovery_samples: int = 24
    hierarchical_min_holdout_samples: int = 12
    hierarchical_discovery_sample_increment: int = 4
    hierarchical_holdout_sample_increment: int = 2
    hierarchical_min_parent_gain: float = 1.0
    hierarchical_max_parent_hit_rate_gap: float = 20.0
    hierarchical_max_parent_score_drop: float = 12.0
    # Entries use ``family: written justification``. Max Pain composite/component
    # conflicts remain forbidden even when a family exception is supplied.
    condition_family_exceptions: tuple[str, ...] = ()
    evidence_family_overlap_threshold: float = 0.75
    recent_window_days: int = 21
    recency_half_life_days: float = 14.0
    maximum_last_match_age_days: int = 21
    # Candidate structures are learned only from Fit. Numeric thresholds are
    # then re-fitted on an expanding, prior-only training prefix before each
    # chronological Selection fold. The untouched outer Test is still opened
    # only after formula identity, family and rank have been frozen.
    walk_forward_folds: int = 3
    walk_forward_min_completed_folds: int = 2


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _number(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _strict_json_number(value: Any) -> Optional[float]:
    """Accept only an actual finite JSON number, never bool or text."""
    if type(value) not in (int, float):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _round(value: Any, digits: int = 6) -> Optional[float]:
    number = _number(value)
    return round(number, digits) if number is not None else None


def _json_safe(value: Any) -> Any:
    return json.loads(
        json.dumps(
            value,
            default=str,
            ensure_ascii=False,
            allow_nan=False,
        )
    )


def _quantile(values: Sequence[float], fraction: float) -> Optional[float]:
    ordered = sorted(float(value) for value in values if math.isfinite(float(value)))
    if not ordered:
        return None
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * min(1.0, max(0.0, float(fraction)))
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _wilson_lower(successes: int, total: int, z: float = 1.96) -> Optional[float]:
    if total <= 0:
        return None
    proportion = successes / total
    denominator = 1.0 + z * z / total
    centre = proportion + z * z / (2.0 * total)
    margin = z * math.sqrt(
        (proportion * (1.0 - proportion) + z * z / (4.0 * total)) / total
    )
    return max(0.0, (centre - margin) / denominator)


def _one_sided_two_proportion_p(
    candidate_successes: float,
    candidate_total: float,
    control_successes: float,
    control_total: float,
) -> Optional[float]:
    if candidate_total <= 0 or control_total <= 0:
        return None
    candidate_rate = candidate_successes / candidate_total
    control_rate = control_successes / control_total
    integer_counts = all(
        abs(value - round(value)) <= 1e-9
        for value in (
            candidate_successes,
            candidate_total,
            control_successes,
            control_total,
        )
    )
    if integer_counts:
        a = int(round(candidate_successes))
        n1 = int(round(candidate_total))
        c = int(round(control_successes))
        n2 = int(round(control_total))
        if not (0 <= a <= n1 and 0 <= c <= n2):
            return None
        # Fisher's exact one-sided tail, conditioned on the observed number
        # of successes.  This avoids the anti-conservative small-sample
        # normal approximation used by the previous implementation.
        successes = a + c
        lower = max(0, successes - n2)
        upper = min(n1, successes)
        denominator = math.comb(n1 + n2, successes)
        if denominator <= 0:
            return None
        return min(
            1.0,
            sum(
                math.comb(n1, value) * math.comb(n2, successes - value)
                for value in range(max(a, lower), upper + 1)
            )
            / denominator,
        )
    pooled = (candidate_successes + control_successes) / (
        candidate_total + control_total
    )
    variance = pooled * (1.0 - pooled) * (
        1.0 / candidate_total + 1.0 / control_total
    )
    if variance <= 0:
        return 1.0 if candidate_rate <= control_rate else 0.0
    z_score = (candidate_rate - control_rate) / math.sqrt(variance)
    return min(1.0, max(0.0, 0.5 * math.erfc(z_score / math.sqrt(2.0))))


def _bh_q_values(p_values: Sequence[Optional[float]]) -> list[Optional[float]]:
    indexed = [(index, value) for index, value in enumerate(p_values) if value is not None]
    if not indexed:
        return [None] * len(p_values)
    indexed.sort(key=lambda item: float(item[1]))
    # The correction family is every distinct hypothesis whose discovery
    # metrics were calculated. A hypothesis with an unavailable p-value still
    # occupies one conservative slot instead of silently shrinking ``m``.
    total = len(p_values)
    adjusted: Dict[int, float] = {}
    running = 1.0
    for reverse_rank, (index, value) in enumerate(reversed(indexed), start=1):
        rank = len(indexed) - reverse_rank + 1
        candidate = min(1.0, float(value) * total / rank)
        running = min(running, candidate)
        adjusted[index] = running
    return [adjusted.get(index) for index in range(len(p_values))]


def _put_feature(output: Dict[str, Any], name: str, value: Any) -> None:
    if value is None:
        return
    if isinstance(value, bool):
        output[name] = value
        return
    number = _number(value)
    if number is not None:
        output[name] = number
        return
    if isinstance(value, str) and value and len(value) <= 120:
        output[name] = value


def extract_decision_features(row: Mapping[str, Any]) -> Dict[str, Any]:
    """Flatten only information available at the alert decision timestamp."""
    output: Dict[str, Any] = {}
    event = row.get("event") if isinstance(row.get("event"), Mapping) else {}
    direction = str(event.get("direction") or "").upper()
    direction_sign = 1.0 if direction == "LONG" else -1.0 if direction == "SHORT" else 0.0

    for key in (
        "symbol",
        "event_type",
        "source_side",
        "timeframe",
        "strategy_version",
        "code_version",
    ):
        _put_feature(output, f"event.{key}", event.get(key))

    time_features = row.get("time_features")
    if isinstance(time_features, Mapping):
        for key, value in time_features.items():
            _put_feature(output, f"time.{key}", value)

    historical = row.get("historical_context")
    if isinstance(historical, Mapping):
        _put_feature(
            output,
            "historical.event_market_session",
            historical.get("event_market_session"),
        )
        historical_windows = historical.get("windows")
        if isinstance(historical_windows, Mapping):
            for window, values in historical_windows.items():
                if not isinstance(values, Mapping):
                    continue
                for key, value in values.items():
                    _put_feature(output, f"historical.{window}.{key}", value)

    max_pain = row.get("max_pain_features")
    if isinstance(max_pain, Mapping) and str(
        max_pain.get("evaluation_status") or ""
    ).upper() == "EVALUABLE":
        features = max_pain.get("features")
        if isinstance(features, Mapping):
            for key, value in features.items():
                name = str(key)
                if name.startswith("max_pain."):
                    _put_feature(output, name, value)

    raw = row.get("raw_features") if isinstance(row.get("raw_features"), Mapping) else {}
    captured = raw.get("captured_event_inputs")
    if isinstance(captured, Mapping):
        for key in ("event_initial_target_distance_pct",):
            _put_feature(output, f"captured.{key}", captured.get(key))
        snapshot_inputs = captured.get("snapshot_inputs")
        if isinstance(snapshot_inputs, Mapping):
            for key, value in snapshot_inputs.items():
                _put_feature(output, f"captured.snapshot.{key}", value)

    latest = raw.get("latest_at_or_before_alert")
    if isinstance(latest, Mapping):
        for family in ("price_oi", "futures_cvd", "spot_cvd"):
            values = latest.get(family)
            if not isinstance(values, Mapping):
                continue
            for key in ("available", "age_minutes", "buy_sell_ratio"):
                _put_feature(output, f"latest.{family}.{key}", values.get(key))

    windows = raw.get("windows")
    if isinstance(windows, Mapping):
        for window, values in windows.items():
            if not isinstance(values, Mapping):
                continue
            for key in (
                "session_active_ratio",
                "session_weekend_ratio",
                "session_composition",
                "price_change_pct",
                "oi_change_pct",
                "futures_continuous_cvd_change_usd",
                "spot_continuous_cvd_change_usd",
                "futures_api_cvd_change_usd",
                "spot_api_cvd_change_usd",
                "spot_to_futures_abs_cvd_ratio",
                "price_oi_state",
                "spot_futures_alignment",
                "price_spot_alignment",
                "price_futures_alignment",
                "complete",
            ):
                value = values.get(key)
                path = f"raw.{window}.{key}"
                _put_feature(output, path, value)
                number = _number(value)
                if direction_sign and number is not None and key in {
                    "price_change_pct",
                    "futures_continuous_cvd_change_usd",
                    "spot_continuous_cvd_change_usd",
                    "futures_api_cvd_change_usd",
                    "spot_api_cvd_change_usd",
                }:
                    aligned = direction_sign * number
                    _put_feature(output, f"aligned.{window}.{key}", aligned)
                    _put_feature(
                        output,
                        f"aligned_log.{window}.{key}",
                        math.copysign(math.log10(1.0 + abs(aligned)), aligned),
                    )

    model = row.get("model_features")
    if isinstance(model, Mapping):
        _put_feature(output, "model.alert_score", model.get("alert_score"))
        _put_feature(
            output,
            "model.initial_target_distance_pct",
            model.get("initial_target_distance_pct"),
        )
        for category in model.get("categories") or []:
            label = str(category or "").strip().upper()
            if label and len(label) <= 80:
                output[f"category.{label}"] = True
        snapshot = model.get("snapshot_features")
        if isinstance(snapshot, Mapping):
            for key, value in snapshot.items():
                _put_feature(output, f"model.{key}", value)

    sequence = row.get("sequence_features")
    if isinstance(sequence, Mapping):
        for window, values in sequence.items():
            if not isinstance(values, Mapping):
                continue
            for key, value in values.items():
                _put_feature(output, f"sequence.{window}.{key}", value)
                if key == "market_direction_balance_pct" and direction_sign:
                    number = _number(value)
                    if number is not None:
                        _put_feature(
                            output,
                            f"aligned_sequence.{window}.{key}",
                            direction_sign * number,
                        )
    return output


def condition_matches(features: Mapping[str, Any], condition: Mapping[str, Any]) -> bool:
    feature = str(condition.get("feature") or "")
    operator = str(condition.get("operator") or "")
    expected = condition.get("value")
    if not feature or operator not in ALLOWED_OPERATORS or feature not in features:
        return False
    actual = features[feature]
    if operator == "==":
        # ``bool`` is a subclass of ``int`` in Python.  Formula evidence must
        # never let a forged ``true`` satisfy a numeric ``1`` predicate (or
        # vice versa), while ordinary int/float numeric equality remains
        # useful for thresholds materialized by PostgreSQL JSON.
        if isinstance(actual, bool) or isinstance(expected, bool):
            return (
                isinstance(actual, bool)
                and isinstance(expected, bool)
                and actual is expected
            )
        actual_number = _strict_json_number(actual)
        expected_number = _strict_json_number(expected)
        if actual_number is not None or expected_number is not None:
            return (
                actual_number is not None
                and expected_number is not None
                and actual_number == expected_number
            )
        return type(actual) is type(expected) and actual == expected
    actual_number = _strict_json_number(actual)
    expected_number = _strict_json_number(expected)
    if actual_number is None or expected_number is None:
        return False
    return actual_number >= expected_number if operator == ">=" else actual_number <= expected_number


def formula_matches(
    row: Mapping[str, Any], *, direction: str, conditions: Sequence[Mapping[str, Any]]
) -> bool:
    event = row.get("event") if isinstance(row.get("event"), Mapping) else {}
    if str(event.get("direction") or "").upper() != str(direction or "").upper():
        return False
    features = extract_decision_features(row)
    return bool(conditions) and all(
        condition_matches(features, condition) for condition in conditions
    )


def condition_is_evaluable(
    features: Mapping[str, Any], condition: Mapping[str, Any]
) -> bool:
    """Return whether a frozen predicate can be decided without censoring."""
    feature = str(condition.get("feature") or "")
    operator = str(condition.get("operator") or "")
    if not feature or operator not in ALLOWED_OPERATORS or feature not in features:
        return False
    actual = features.get(feature)
    expected = condition.get("value")
    if operator in {">=", "<="}:
        return (
            _strict_json_number(actual) is not None
            and _strict_json_number(expected) is not None
        )
    return (
        isinstance(actual, (bool, int, float, str))
        and isinstance(expected, (bool, int, float, str))
        and not (isinstance(actual, float) and not math.isfinite(actual))
        and not (isinstance(expected, float) and not math.isfinite(expected))
    )


def evaluate_formula(
    row: Optional[Mapping[str, Any]],
    *,
    direction: str,
    conditions: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Return an auditable, decision-time-only Shadow evaluation.

    A missing feature is ``UNEVALUABLE`` rather than a negative control.  This
    distinction prevents archive gaps from improving a formula's apparent
    performance.  No outcome field is read by this function.
    """
    if not isinstance(row, Mapping):
        return {
            "status": "UNEVALUABLE",
            "matched": False,
            "reason": "decision-time feature row unavailable",
            "features": {},
            "condition_results": [],
        }
    event = row.get("event") if isinstance(row.get("event"), Mapping) else {}
    return evaluate_frozen_feature_values(
        extract_decision_features(row),
        direction=direction,
        event_direction=event.get("direction"),
        conditions=conditions,
    )


def evaluate_frozen_feature_values(
    frozen_features: Optional[Mapping[str, Any]],
    *,
    direction: str,
    event_direction: Any,
    conditions: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Evaluate operators directly against one immutable flat feature map.

    This is the prospective Shadow execution boundary.  It deliberately does
    not accept a research row and cannot call ``extract_decision_features``;
    callers must provide the exact decision-time values already frozen in the
    authoritative sampler-v4 feature bundle.  Missing or malformed values are
    UNEVALUABLE, never negative controls.
    """
    condition_results: list[Dict[str, Any]] = []
    if not isinstance(frozen_features, Mapping):
        return {
            "status": "UNEVALUABLE",
            "matched": False,
            "reason": "frozen decision feature map unavailable",
            "features": {},
            "condition_results": [],
        }
    features = dict(frozen_features)
    normalized_direction = str(direction or "").upper()
    frozen_event_direction = str(event_direction or "").upper()
    unavailable = False
    for condition in conditions:
        feature = str(condition.get("feature") or "")
        operator = str(condition.get("operator") or "")
        expected = condition.get("value")
        actual = features.get(feature)
        available = bool(
            feature and operator in ALLOWED_OPERATORS and feature in features
        )
        if available:
            if operator in {">=", "<="}:
                available = (
                    _strict_json_number(actual) is not None
                    and _strict_json_number(expected) is not None
                )
            else:
                available = (
                    isinstance(actual, (bool, int, float, str))
                    and isinstance(expected, (bool, int, float, str))
                    and not (
                        isinstance(actual, float) and not math.isfinite(actual)
                    )
                    and not (
                        isinstance(expected, float)
                        and not math.isfinite(expected)
                    )
                )
        passed = available and condition_matches(features, condition)
        if not available:
            unavailable = True
        condition_results.append(
            {
                "feature": feature,
                "operator": operator,
                "expected": expected,
                "actual": actual,
                "available": available,
                "passed": bool(passed),
            }
        )
    if not conditions:
        status = "UNEVALUABLE"
        reason = "formula has no conditions"
    elif frozen_event_direction != normalized_direction:
        status = "UNEVALUABLE"
        reason = "event direction does not match formula direction"
    elif any(
        item["available"] and not item["passed"]
        for item in condition_results
    ):
        # Three-valued conjunction: one known-false condition proves that the
        # whole formula did not match even when another input is unknown.  The
        # row is therefore a legitimate control, not a censor.  Unknown is
        # retained only when no condition is already definitively false.
        status = "UNMATCHED"
        reason = "one or more available formula conditions failed"
    elif unavailable:
        status = "UNEVALUABLE"
        reason = "one or more required decision-time features are unavailable"
    elif all(item["passed"] for item in condition_results):
        status = "MATCHED"
        reason = "all formula conditions passed"
    else:
        status = "UNMATCHED"
        reason = "one or more formula conditions failed"
    return {
        "status": status,
        "matched": status == "MATCHED",
        "reason": reason,
        "features": features,
        "condition_results": condition_results,
    }


def _canonical_conditions(conditions: Sequence[Mapping[str, Any]]) -> list[Dict[str, Any]]:
    normalized = [
        {
            "feature": str(condition["feature"]),
            "operator": str(condition["operator"]),
            "value": condition.get("value"),
        }
        for condition in conditions
    ]
    return sorted(
        normalized,
        key=lambda item: (
            item["feature"],
            item["operator"],
            json.dumps(item["value"], sort_keys=True, ensure_ascii=False),
        ),
    )


def formula_key(
    *,
    direction: str,
    horizon_minutes: int,
    feature_schema_version: str,
    conditions: Sequence[Mapping[str, Any]],
    condition_family_exceptions: Sequence[str] = (),
) -> str:
    canonical = {
        "formula_schema_version": FORMULA_SCHEMA_VERSION,
        "engine_version": ENGINE_VERSION,
        "condition_family_policy_version": (
            research_formula_families.CONDITION_FAMILY_POLICY_VERSION
        ),
        "condition_family_exceptions": sorted(
            str(value).strip() for value in condition_family_exceptions
        ),
        "direction": str(direction).upper(),
        "horizon_minutes": int(horizon_minutes),
        "feature_schema_version": str(feature_schema_version),
        "conditions": _canonical_conditions(conditions),
    }
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _formula_text(direction: str, conditions: Sequence[Mapping[str, Any]]) -> str:
    parts = []
    for condition in _canonical_conditions(conditions):
        value = condition["value"]
        rendered = f"{value:.6g}" if isinstance(value, float) else json.dumps(value, ensure_ascii=False)
        parts.append(f"{condition['feature']} {condition['operator']} {rendered}")
    return f"{str(direction).upper()} WHEN " + " AND ".join(parts)


def _outcome_values(rows: Sequence[Mapping[str, Any]], key: str) -> list[float]:
    values = []
    for row in rows:
        label = row.get("outcome_label")
        value = label.get(key) if isinstance(label, Mapping) else None
        number = _number(value)
        if number is not None:
            values.append(number)
    return values


def _final_path_success(row: Mapping[str, Any]) -> Optional[bool]:
    label = row.get("outcome_label")
    if not isinstance(label, Mapping):
        return None
    value = label.get("path_success")
    status = str(label.get("first_touch_status") or "").upper()
    if status == "HIT" and value is True:
        return True
    if status == "MISS" and value is False:
        return False
    return None


def _outcome_success_flags(rows: Sequence[Mapping[str, Any]]) -> list[bool]:
    """Return only explicit final first-touch labels.

    Directional endpoint return is deliberately not a compatibility fallback:
    a later reversal must not erase an earlier qualifying touch, and a positive
    close must not fabricate a touch whose frozen width was never reached.
    """
    flags: list[bool] = []
    for row in rows:
        value = _final_path_success(row)
        if value is not None:
            flags.append(value)
    return flags


def _paired_favorable_adverse(
    row: Mapping[str, Any],
) -> tuple[bool, float] | None:
    """Return the same-row favorable/adverse comparison.

    Aggregated Market Episodes carry an explicit paired result from their
    outcome-blind evidence cohort. It must not be reconstructed by comparing
    independently aggregated MFE and MAE medians.
    """

    label = row.get("outcome_label")
    if not isinstance(label, Mapping):
        return None
    explicit_edge = _number(label.get("paired_favorable_minus_adverse_pct"))
    explicit_dominance = label.get("favorable_dominance")
    if explicit_edge is not None and isinstance(explicit_dominance, bool):
        return explicit_dominance, explicit_edge
    mfe = _number(label.get("mfe_pct"))
    mae = _number(label.get("mae_pct"))
    if mfe is None or mae is None:
        return None
    edge = mfe - mae
    return edge > 0.0, edge


def _empirical_percentile(value: Optional[float], population: Sequence[float]) -> Optional[float]:
    if value is None or not population:
        return None
    ordered = [float(item) for item in population if math.isfinite(float(item))]
    if not ordered:
        return None
    return sum(1 for item in ordered if item <= value) / len(ordered) * 100.0


def _row_outcome_active_ratio(row: Mapping[str, Any]) -> float:
    label = row.get("outcome_label")
    if isinstance(label, Mapping):
        explicit = _number(label.get("session_active_ratio"))
        if explicit is not None:
            return min(1.0, max(0.0, explicit))
        horizon = int(_number(label.get("horizon_minutes")) or 0)
    else:
        horizon = 0
    event = row.get("event") if isinstance(row.get("event"), Mapping) else {}
    alert_time = event.get("alert_time_utc")
    if alert_time is None or horizon <= 0:
        return 1.0
    start = _utc(alert_time)
    active_ratio, _, _ = market_session_baseline.session_ratios(
        start, start + timedelta(minutes=horizon)
    )
    return active_ratio


def _session_composition_label(active_ratio: float) -> str:
    if active_ratio >= 1.0 - 1e-9:
        return "ACTIVE_ONLY"
    if active_ratio <= 1e-9:
        return "WEEKEND_ONLY"
    return "MIXED"


def _composition_profile_weights(
    rows: Sequence[Mapping[str, Any]],
    selected_active_ratios: Sequence[float],
) -> list[tuple[Mapping[str, Any], float]]:
    """Calculate exact mean triangular similarity in O(n log n).

    This is algebraically identical to averaging ``composition_weight`` for
    every selected/control pair, but avoids quadratic CPU work inside the
    thousands of formula candidates evaluated on the production bot.
    """
    if not selected_active_ratios:
        return []
    selected = sorted(float(value) for value in selected_active_ratios)
    prefix = [0.0]
    for value in selected:
        prefix.append(prefix[-1] + value)
    tolerance = market_session_baseline.DEFAULT_COMPOSITION_TOLERANCE
    profile: list[tuple[Mapping[str, Any], float]] = []
    for row in rows:
        historical = _row_outcome_active_ratio(row)
        left = bisect_left(selected, historical - tolerance)
        middle = bisect_right(selected, historical)
        right = bisect_right(selected, historical + tolerance)
        left_count = middle - left
        right_count = right - middle
        left_sum = prefix[middle] - prefix[left]
        right_sum = prefix[right] - prefix[middle]
        similarity_sum = (
            left_count * (1.0 - historical / tolerance)
            + left_sum / tolerance
            + right_count * (1.0 + historical / tolerance)
            - right_sum / tolerance
        )
        weight = max(0.0, similarity_sum / len(selected))
        if weight > 0.0:
            profile.append((row, weight))
    return profile


def _weighted_outcomes(
    profile: Sequence[tuple[Mapping[str, Any], float]], outcome_key: str
) -> list[tuple[float, float]]:
    result: list[tuple[float, float]] = []
    for row, weight in profile:
        label = row.get("outcome_label")
        value = _number(label.get(outcome_key)) if isinstance(label, Mapping) else None
        if value is not None:
            result.append((value, weight))
    return result


def _two_direction_union_q(value: Any) -> Optional[float]:
    """Control the LONG-or-SHORT union after per-direction joint BH."""

    number = _number(value)
    if number is None:
        return None
    return min(1.0, max(0.0, number) * 2.0)


def _weighted_successes(
    profile: Sequence[tuple[Mapping[str, Any], float]],
) -> list[tuple[bool, float]]:
    result: list[tuple[bool, float]] = []
    for row, weight in profile:
        value = _final_path_success(row)
        if value is not None:
            result.append((value, weight))
    return result


def _weighted_percentile_rank(
    value: Optional[float], population: Sequence[tuple[float, float]]
) -> Optional[float]:
    if value is None or not population:
        return None
    total = sum(weight for _, weight in population)
    if total <= 0.0:
        return None
    below = sum(weight for item, weight in population if item < value)
    equal = sum(weight for item, weight in population if item == value)
    return (below + 0.5 * equal) / total * 100.0


def _width_floor_scale(selected: Sequence[Mapping[str, Any]]) -> tuple[float, int]:
    factors: list[float] = []
    for row in selected:
        label = row.get("outcome_label")
        reference = (
            label.get("movement_width_reference")
            if isinstance(label, Mapping)
            else None
        )
        factor = (
            _number(reference.get("floor_scale_factor"))
            if isinstance(reference, Mapping)
            else None
        )
        if factor is not None:
            factors.append(min(1.0, max(0.50, factor)))
    if not selected or len(factors) != len(selected):
        return 1.0, len(factors)
    return float(median(factors)), len(factors)


def _metrics(
    selected: Sequence[Mapping[str, Any]],
    universe: Sequence[Mapping[str, Any]],
    *,
    recent_window_days: int = 21,
    recency_half_life_days: float = 14.0,
    evidence_as_of_utc: Any = None,
    already_independent_episodes: bool = False,
    discard_boundary_episode: bool = False,
    include_private_evidence_keys: bool = False,
) -> Dict[str, Any]:
    # Keep every denominator and every descriptive risk/movement metric on the
    # same terminal first-touch cohort.  PENDING, malformed and legacy-only
    # labels are unavailable evidence rather than implicit failures/successes.
    explicit_evidence_as_of = (
        _utc(evidence_as_of_utc) if evidence_as_of_utc is not None else None
    )

    def row_time(row: Mapping[str, Any]) -> datetime:
        return research_market_episode.row_time_utc(row)

    def available_by_as_of(row: Mapping[str, Any]) -> bool:
        return (
            explicit_evidence_as_of is None
            or row_time(row) <= explicit_evidence_as_of
        )

    future_selected_exclusions = sum(
        1
        for row in selected
        if explicit_evidence_as_of is not None
        and row_time(row) > explicit_evidence_as_of
    )
    future_universe_exclusions = sum(
        1
        for row in universe
        if explicit_evidence_as_of is not None
        and row_time(row) > explicit_evidence_as_of
    )
    # Episode membership must be frozen from decision rows before any outcome
    # is inspected.  A PENDING member at the earliest forecast cohort blocks
    # that whole episode instead of disappearing and making a simultaneous HIT
    # look like independent 100% evidence.
    selected = [row for row in selected if available_by_as_of(row)]
    universe = [row for row in universe if available_by_as_of(row)]
    raw_selected_size = len(selected)
    raw_universe_size = len(universe)
    raw_evidence_times = sorted(
        row_time(row) for row in universe
    )
    raw_evidence_as_of = raw_evidence_times[-1] if raw_evidence_times else None
    evidence_as_of = explicit_evidence_as_of or raw_evidence_as_of
    selected_ids = {
        int(row.get("event", {}).get("event_id"))
        for row in selected
        if row.get("event", {}).get("event_id") is not None
    }
    selected_symbols = {
        str(symbol).upper()
        for row in selected
        for symbol in (
            row.get("event", {}).get("market_episode_member_symbols")
            or [row.get("event", {}).get("symbol")]
        )
        if symbol
    }
    raw_controls = [
        row
        for row in universe
        if int(row.get("event", {}).get("event_id") or -1) not in selected_ids
        and (
            not selected_symbols
            or str(row.get("event", {}).get("symbol") or "").upper()
            in selected_symbols
        )
    ]
    horizon = int(
        _number(
            ((selected or universe)[0].get("outcome_label") or {}).get(
                "horizon_minutes"
            )
        )
        or 1
    ) if (selected or universe) else 1
    episode_span = timedelta(
        minutes=research_market_episode.episode_minutes(horizon)
    )
    def terminal_row(row: Mapping[str, Any]) -> bool:
        label = row.get("outcome_label")
        return bool(
            _final_path_success(row) is not None
            and isinstance(label, Mapping)
            and _number(label.get("mfe_pct")) is not None
            and _number(label.get("mae_pct")) is not None
        )

    def episode_complete(episode: Mapping[str, Any]) -> bool:
        evidence_rows = research_market_episode.episode_evidence_rows(episode)
        return bool(evidence_rows) and all(terminal_row(row) for row in evidence_rows)

    boundary_excluded_matches = 0
    boundary_excluded_controls = 0
    if already_independent_episodes:
        selected = [row for row in selected if terminal_row(row)]
        controls = [row for row in raw_controls if terminal_row(row)]
        independent = {
            "match_episodes": [],
            "control_episodes": [],
            "excluded_match_event_ids": [],
            "excluded_control_event_ids": [],
        }
        open_match_episodes: list[Dict[str, Any]] = []
        open_control_episodes: list[Dict[str, Any]] = []
    else:
        independent = research_market_episode.select_independent(
            selected,
            raw_controls,
            horizon_minutes=horizon,
            presorted=False,
        )
        match_episodes = list(independent["match_episodes"])
        control_episodes = list(independent["control_episodes"])
        if discard_boundary_episode and match_episodes:
            boundary_excluded_matches = 1
            match_episodes = match_episodes[1:]
        if discard_boundary_episode and control_episodes:
            boundary_excluded_controls = 1
            control_episodes = control_episodes[1:]
        finalized_match_episodes, open_match_episodes = (
            research_market_episode.partition_finalized(
                match_episodes,
                horizon_minutes=horizon,
                as_of_utc=evidence_as_of,
            )
            if evidence_as_of is not None
            else ([], list(match_episodes))
        )
        finalized_control_episodes, open_control_episodes = (
            research_market_episode.partition_finalized(
                control_episodes,
                horizon_minutes=horizon,
                as_of_utc=evidence_as_of,
            )
            if evidence_as_of is not None
            else ([], list(control_episodes))
        )
        complete_match_episodes = [
            episode for episode in finalized_match_episodes if episode_complete(episode)
        ]
        complete_control_episodes = [
            episode for episode in finalized_control_episodes if episode_complete(episode)
        ]
        open_match_episodes = list(open_match_episodes) + [
            episode for episode in finalized_match_episodes if not episode_complete(episode)
        ]
        open_control_episodes = list(open_control_episodes) + [
            episode for episode in finalized_control_episodes if not episode_complete(episode)
        ]
        selected = [
            research_market_episode.aggregate_metric_episode(
                episode["rows"],
                episode_key=episode["episode_key"],
                episode_start_utc=episode["start_time_utc"],
                episode_end_utc=episode["end_time_utc"],
            )
            for episode in complete_match_episodes
        ]
        controls = [
            research_market_episode.aggregate_metric_episode(
                episode["rows"],
                episode_key=episode["episode_key"],
                episode_start_utc=episode["start_time_utc"],
                episode_end_utc=episode["end_time_utc"],
            )
            for episode in complete_control_episodes
        ]
    universe = selected + controls
    selected_active_ratios = [_row_outcome_active_ratio(row) for row in selected]
    session_composition_counts = {
        label: sum(
            1
            for ratio in selected_active_ratios
            if _session_composition_label(ratio) == label
        )
        for label in ("ACTIVE_ONLY", "MIXED", "WEEKEND_ONLY")
    }
    directional = _outcome_values(selected, "directional_return_pct")
    success_flags = _outcome_success_flags(selected)
    control_success_flags = _outcome_success_flags(controls)
    mfe = _outcome_values(selected, "mfe_pct")
    mae = _outcome_values(selected, "mae_pct")
    def adverse_tail_mae(row: Mapping[str, Any]) -> Optional[float]:
        label = row.get("outcome_label")
        label = label if isinstance(label, Mapping) else {}
        value = _number(label.get("adverse_tail_mae_pct"))
        return value if value is not None else _number(label.get("mae_pct"))

    risk_mae = [
        value
        for row in selected
        if (value := adverse_tail_mae(row)) is not None
    ]
    universe_mfe = _outcome_values(universe, "mfe_pct")
    first_progress = _outcome_values(selected, "time_to_first_progress_seconds")
    time_to_mfe = _outcome_values(selected, "time_to_mfe_seconds")
    target_progress = _outcome_values(selected, "target_progress_ratio")
    target_flags = [
        bool(row.get("outcome_label", {}).get("target_reached"))
        for row in selected
        if row.get("outcome_label", {}).get("target_reached") is not None
    ]
    ambiguity_flags = [
        bool(row.get("outcome_label", {}).get("qualifying_candle_order_ambiguous"))
        for row in selected
        if isinstance(row.get("outcome_label"), Mapping)
        and isinstance(
            row.get("outcome_label", {}).get("qualifying_candle_order_ambiguous"),
            bool,
        )
    ]
    event_times = sorted(
        research_market_episode.row_time_utc(row)
        for row in selected
    )
    distinct_dates = {timestamp.date().isoformat() for timestamp in event_times}
    time_span_hours = (
        (event_times[-1] - event_times[0]).total_seconds() / 3600.0
        if len(event_times) >= 2
        else 0.0
    )
    recent_cutoff = (
        evidence_as_of - timedelta(days=max(1, int(recent_window_days)))
        if evidence_as_of is not None
        else None
    )
    recent_selected = [
        row
        for row in selected
        if recent_cutoff is not None
        and research_market_episode.row_time_utc(row) >= recent_cutoff
    ]
    recent_controls = [
        row
        for row in controls
        if recent_cutoff is not None
        and research_market_episode.row_time_utc(row) >= recent_cutoff
    ]

    half_life_seconds = max(1.0, float(recency_half_life_days) * 86400.0)

    def recency_weight(row: Mapping[str, Any]) -> float:
        if evidence_as_of is None:
            return 0.0
        age = max(
            0.0,
            (evidence_as_of - research_market_episode.row_time_utc(row)).total_seconds(),
        )
        return 0.5 ** (age / half_life_seconds)

    def recency_pairs(
        rows: Sequence[Mapping[str, Any]], key: str
    ) -> list[tuple[float, float]]:
        pairs = []
        for row in rows:
            value = _number((row.get("outcome_label") or {}).get(key))
            weight = recency_weight(row)
            if value is not None and weight > 0.0:
                pairs.append((value, weight))
        return pairs

    selected_weights = [recency_weight(row) for row in recent_selected]
    selected_weight_sum = sum(selected_weights)
    selected_weight_sq_sum = sum(weight * weight for weight in selected_weights)
    recency_effective_n = (
        selected_weight_sum * selected_weight_sum / selected_weight_sq_sum
        if selected_weight_sq_sum > 0.0
        else 0.0
    )
    recent_weight_sum = selected_weight_sum
    recent_weight_sq_sum = selected_weight_sq_sum
    recent_effective_n = (
        recent_weight_sum * recent_weight_sum / recent_weight_sq_sum
        if recent_weight_sq_sum > 0.0
        else 0.0
    )
    weighted_successes = sum(
        weight
        for row, weight in zip(recent_selected, selected_weights)
        if _final_path_success(row) is True
    )
    recency_hit_rate = (
        weighted_successes / selected_weight_sum * 100.0
        if selected_weight_sum > 0.0
        else None
    )
    recency_wilson = (
        _wilson_lower(
            recency_effective_n * recency_hit_rate / 100.0,
            recency_effective_n,
        )
        * 100.0
        if recency_hit_rate is not None and recency_effective_n > 0.0
        else None
    )
    recent_control_profile = _composition_profile_weights(
        recent_controls,
        [_row_outcome_active_ratio(row) for row in recent_selected],
    )
    control_composition_weights = {
        id(row): weight for row, weight in recent_control_profile
    }
    control_weights = [
        recency_weight(row) * control_composition_weights.get(id(row), 0.0)
        for row in recent_controls
    ]
    control_weight_sum = sum(control_weights)
    control_weight_sq_sum = sum(weight * weight for weight in control_weights)
    recent_control_effective_n = (
        control_weight_sum * control_weight_sum / control_weight_sq_sum
        if control_weight_sq_sum > 0.0
        else 0.0
    )
    recency_control_hit = (
        sum(
            weight
            for row, weight in zip(recent_controls, control_weights)
            if _final_path_success(row) is True
        )
        / control_weight_sum
        * 100.0
        if control_weight_sum > 0.0
        else None
    )
    recency_mfe = recency_pairs(recent_selected, "mfe_pct")
    recency_mae = recency_pairs(recent_selected, "mae_pct")
    recency_risk_mae = [
        (value, weight)
        for row in recent_selected
        for value, weight in [(adverse_tail_mae(row), recency_weight(row))]
        if value is not None and weight > 0.0
    ]
    selected_paired = [
        paired
        for row in selected
        if (paired := _paired_favorable_adverse(row)) is not None
    ]
    control_paired = [
        paired
        for row in controls
        if (paired := _paired_favorable_adverse(row)) is not None
    ]
    control_session_profile = _composition_profile_weights(
        controls, selected_active_ratios
    )
    control_paired_by_identity = {
        id(row): paired
        for row in controls
        if (paired := _paired_favorable_adverse(row)) is not None
    }
    session_control_dominance = [
        (control_paired_by_identity[id(row)][0], weight)
        for row, weight in control_session_profile
        if id(row) in control_paired_by_identity and weight > 0.0
    ]
    session_control_dominance_total = sum(
        weight for _, weight in session_control_dominance
    )
    session_control_dominance_weight_sq = sum(
        weight * weight for _, weight in session_control_dominance
    )
    session_control_dominance_effective = (
        session_control_dominance_total * session_control_dominance_total
        / session_control_dominance_weight_sq
        if session_control_dominance_weight_sq > 0.0
        else 0.0
    )
    session_control_dominance_rate = (
        sum(
            weight
            for favorable, weight in session_control_dominance
            if favorable
        )
        / session_control_dominance_total
        * 100.0
        if session_control_dominance_total > 0.0
        else None
    )
    dominance_flags = [favorable for favorable, _ in selected_paired]
    control_dominance_flags = [favorable for favorable, _ in control_paired]
    favorable_dominance_rate = (
        sum(dominance_flags) / len(dominance_flags) * 100.0
        if dominance_flags
        else None
    )
    unadjusted_control_dominance_rate = (
        sum(control_dominance_flags) / len(control_dominance_flags) * 100.0
        if control_dominance_flags
        else None
    )
    unadjusted_dominance_p_value = _one_sided_two_proportion_p(
        sum(dominance_flags),
        len(dominance_flags),
        sum(control_dominance_flags),
        len(control_dominance_flags),
    )
    dominance_p_value = _one_sided_two_proportion_p(
        sum(dominance_flags),
        len(dominance_flags),
        (
            session_control_dominance_effective
            * session_control_dominance_rate
            / 100.0
            if session_control_dominance_rate is not None
            else 0.0
        ),
        session_control_dominance_effective,
    )
    paired_edges = [edge for _, edge in selected_paired]
    recency_dominance_entries = [
        (paired[0], paired[1], weight)
        for row, weight in zip(recent_selected, selected_weights)
        for paired in [_paired_favorable_adverse(row)]
        if paired is not None and weight > 0.0
    ]
    recency_dominance_total_weight = sum(
        weight for _, _, weight in recency_dominance_entries
    )
    recency_dominance_weight_sq = sum(
        weight * weight for _, _, weight in recency_dominance_entries
    )
    recency_dominance_effective_n = (
        recency_dominance_total_weight * recency_dominance_total_weight
        / recency_dominance_weight_sq
        if recency_dominance_weight_sq > 0.0
        else 0.0
    )
    recency_dominance_weight = sum(
        weight
        for favorable, _, weight in recency_dominance_entries
        if favorable
    )
    recency_dominance_rate = (
        recency_dominance_weight / recency_dominance_total_weight * 100.0
        if recency_dominance_total_weight > 0.0
        else None
    )
    recency_dominance_wilson = (
        _wilson_lower(
            recency_dominance_effective_n * recency_dominance_rate / 100.0,
            recency_dominance_effective_n,
        )
        * 100.0
        if recency_dominance_rate is not None
        and recency_dominance_effective_n > 0.0
        else None
    )
    recency_control_dominance_entries = [
        (paired[0], weight)
        for row, weight in zip(recent_controls, control_weights)
        for paired in [_paired_favorable_adverse(row)]
        if paired is not None and weight > 0.0
    ]
    recency_control_dominance_total_weight = sum(
        weight for _, weight in recency_control_dominance_entries
    )
    recency_control_dominance_weight_sq = sum(
        weight * weight for _, weight in recency_control_dominance_entries
    )
    recency_control_dominance_effective_n = (
        recency_control_dominance_total_weight
        * recency_control_dominance_total_weight
        / recency_control_dominance_weight_sq
        if recency_control_dominance_weight_sq > 0.0
        else 0.0
    )
    recency_control_dominance_weight = sum(
        weight
        for favorable, weight in recency_control_dominance_entries
        if favorable
    )
    recency_control_dominance_rate = (
        recency_control_dominance_weight
        / recency_control_dominance_total_weight
        * 100.0
        if recency_control_dominance_total_weight > 0.0
        else None
    )
    recency_paired_edges = [
        (paired_edge, weight)
        for _, paired_edge, weight in recency_dominance_entries
    ]
    successes = sum(success_flags)
    control_successes = sum(control_success_flags)
    hit_rate = successes / len(success_flags) * 100.0 if success_flags else None
    control_hit_rate = (
        control_successes / len(control_success_flags) * 100.0
        if control_success_flags
        else None
    )
    session_control_successes = _weighted_successes(control_session_profile)
    session_control_mfe = _weighted_outcomes(control_session_profile, "mfe_pct")
    session_control_mae = _weighted_outcomes(control_session_profile, "mae_pct")
    session_control_effective = sum(weight for _, weight in session_control_successes)
    session_mfe_effective = sum(weight for _, weight in session_control_mfe)
    session_mae_effective = sum(weight for _, weight in session_control_mae)
    weighted_successes = sum(
        weight for value, weight in session_control_successes if value
    )
    session_hit_baseline = (
        weighted_successes / session_control_effective * 100.0
        if session_control_effective > 0.0
        else None
    )
    session_mfe_median = market_session_baseline.weighted_percentile(
        session_control_mfe, 0.50
    )
    session_mfe_p90 = market_session_baseline.weighted_percentile(
        session_control_mfe, 0.90
    )
    session_mae_median = market_session_baseline.weighted_percentile(
        session_control_mae, 0.50
    )
    session_mae_p90 = market_session_baseline.weighted_percentile(
        session_control_mae, 0.90
    )
    active_control_mfe = market_session_baseline.composition_weighted_values(
        [
            (value, _row_outcome_active_ratio(control))
            for control in controls
            for value in [
                _number(
                    (control.get("outcome_label") or {}).get("mfe_pct")
                    if isinstance(control.get("outcome_label"), Mapping)
                    else None
                )
            ]
            if value is not None
        ],
        1.0,
        market_session_baseline.DEFAULT_COMPOSITION_TOLERANCE,
    )
    active_control_mfe_effective = sum(weight for _, weight in active_control_mfe)
    active_control_mfe_p90 = market_session_baseline.weighted_percentile(
        active_control_mfe, 0.90
    )
    median_mae = median(mae) if mae else None
    median_mfe = median(mfe) if mfe else None
    universe_median_mfe = median(universe_mfe) if universe_mfe else None
    universe_p90_mfe = _quantile(universe_mfe, 0.90)
    efficiency = research_mfe_mae_efficiency.classify(
        median_mfe, median_mae
    )
    sample_share = len(selected) / len(universe) * 100.0 if universe else 0.0
    rarity_class = "RARE" if sample_share <= 5.0 else "UNCOMMON" if sample_share <= 15.0 else "COMMON"
    median_mfe_percentile = _empirical_percentile(median_mfe, universe_mfe)
    mfe_uplift = (
        (median_mfe / universe_median_mfe - 1.0) * 100.0
        if median_mfe is not None and universe_median_mfe not in (None, 0.0)
        else None
    )
    session_mfe_percentile = _weighted_percentile_rank(
        median_mfe, session_control_mfe
    )
    session_mae_percentile = _weighted_percentile_rank(
        median_mae, session_control_mae
    )
    session_mfe_uplift = (
        (median_mfe / session_mfe_median - 1.0) * 100.0
        if median_mfe is not None
        and session_mfe_median not in (None, 0.0)
        else None
    )
    floor_scale, floor_reference_samples = _width_floor_scale(selected)
    floor_source = "prior raw-price session calibration"
    if floor_reference_samples != len(selected):
        floor_scale = 1.0
        floor_source = "no relaxation: insufficient prior-only calibration"
    base_floor = minimum_wide_move_pct(0 if not selected else int(
        _number((selected[0].get("outcome_label") or {}).get("horizon_minutes"))
        or 0
    ))
    effective_floor = base_floor * floor_scale
    mae_p90 = _quantile(risk_mae, 0.90)
    favorable_minus_p90_adverse = (
        median_mfe - mae_p90
        if median_mfe is not None and mae_p90 is not None
        else None
    )
    expected_favorable = (
        median_mfe * hit_rate / 100.0
        if median_mfe is not None and hit_rate is not None
        else None
    )
    result = {
        "sample_size": len(selected),
        "universe_size": len(universe),
        "raw_sample_size": raw_selected_size,
        "raw_universe_size": raw_universe_size,
        "future_selected_rows_excluded": future_selected_exclusions,
        "future_universe_rows_excluded": future_universe_exclusions,
        "market_episode_policy_version": research_market_episode.POLICY_VERSION,
        "market_episode_minutes": max(
            research_market_episode.MINIMUM_EPISODE_MINUTES, horizon
        ),
        "market_episode_excluded_match_rows": len(
            independent["excluded_match_event_ids"]
        ),
        "market_episode_excluded_control_rows": len(
            independent["excluded_control_event_ids"]
        ),
        "market_episode_open_matches": len(open_match_episodes),
        "market_episode_open_controls": len(open_control_episodes),
        "market_episode_boundary_excluded_matches": boundary_excluded_matches,
        "market_episode_boundary_excluded_controls": boundary_excluded_controls,
        "market_episode_finalization_lag_minutes_after_membership": 0,
        "market_episode_evidence_anchor_horizon_minutes": horizon,
        "sample_share_pct": round(sample_share, 4),
        "rarity_class": rarity_class,
        "first_sample_time_utc": event_times[0] if event_times else None,
        "last_sample_time_utc": event_times[-1] if event_times else None,
        "time_span_hours": round(time_span_hours, 4),
        "distinct_utc_dates": len(distinct_dates),
        "evidence_as_of_utc": evidence_as_of,
        "recent_window_days": max(1, int(recent_window_days)),
        "recency_half_life_days": float(recency_half_life_days),
        "recent_sample_size": len(recent_selected),
        "recent_control_sample_size": sum(
            1 for weight in control_weights if weight > 0.0
        ),
        "last_sample_age_hours": _round(
            (
                (evidence_as_of - event_times[-1]).total_seconds() / 3600.0
                if evidence_as_of is not None and event_times
                else None
            ),
            4,
        ),
        "recency_effective_sample_size": _round(recent_effective_n, 4),
        "recency_total_effective_sample_size": _round(recency_effective_n, 4),
        "recency_control_effective_sample_size": _round(
            recent_control_effective_n, 4
        ),
        "recency_control_weighting_policy": (
            "21-day recency decay multiplied by exact outcome-session "
            "composition similarity to recent matches"
        ),
        "recency_weighted_hit_rate_pct": _round(recency_hit_rate, 4),
        "recency_weighted_wilson_95_lower_approx_pct": _round(
            recency_wilson, 4
        ),
        "recency_weighted_control_hit_rate_pct": _round(
            recency_control_hit, 4
        ),
        "recency_weighted_hit_rate_improvement_pct_points": _round(
            (
                recency_hit_rate - recency_control_hit
                if recency_hit_rate is not None
                and recency_control_hit is not None
                else None
            ),
            4,
        ),
        "recency_weighted_median_mfe_pct": _round(
            market_session_baseline.weighted_percentile(recency_mfe, 0.50), 6
        ),
        "recency_weighted_median_mae_pct": _round(
            market_session_baseline.weighted_percentile(recency_mae, 0.50), 6
        ),
        "recency_weighted_mae_p90_pct": _round(
            market_session_baseline.weighted_percentile(
                recency_risk_mae, 0.90
            ),
            6,
        ),
        "recency_weighted_mae_p95_pct": _round(
            market_session_baseline.weighted_percentile(
                recency_risk_mae, 0.95
            ),
            6,
        ),
        "favorable_dominance_rate_pct": _round(
            favorable_dominance_rate, 4
        ),
        "favorable_dominance_wilson_95_lower_pct": _round(
            (
                _wilson_lower(sum(dominance_flags), len(dominance_flags))
                * 100.0
                if dominance_flags
                else None
            ),
            4,
        ),
        "control_favorable_dominance_rate_pct": _round(
            session_control_dominance_rate, 4
        ),
        "unadjusted_control_favorable_dominance_rate_pct": _round(
            unadjusted_control_dominance_rate, 4
        ),
        "session_matched_control_favorable_dominance_effective_samples": _round(
            session_control_dominance_effective, 4
        ),
        "favorable_dominance_improvement_pct_points": _round(
            (
                favorable_dominance_rate - session_control_dominance_rate
                if favorable_dominance_rate is not None
                and session_control_dominance_rate is not None
                else None
            ),
            4,
        ),
        "median_paired_favorable_minus_adverse_pct": _round(
            median(paired_edges) if paired_edges else None, 6
        ),
        "asymmetry_one_sided_p_value": _round(dominance_p_value, 8),
        "unadjusted_asymmetry_one_sided_p_value": _round(
            unadjusted_dominance_p_value, 8
        ),
        "asymmetry_control_adjustment": (
            "Kish-effective outcome-session-composition matched controls"
        ),
        "recency_weighted_favorable_dominance_rate_pct": _round(
            recency_dominance_rate, 4
        ),
        "recency_favorable_dominance_effective_sample_size": _round(
            recency_dominance_effective_n, 4
        ),
        "recency_weighted_favorable_dominance_wilson_95_lower_approx_pct": _round(
            recency_dominance_wilson, 4
        ),
        "recency_weighted_control_favorable_dominance_rate_pct": _round(
            recency_control_dominance_rate, 4
        ),
        "recency_control_favorable_dominance_effective_sample_size": _round(
            recency_control_dominance_effective_n, 4
        ),
        "recency_weighted_favorable_dominance_improvement_pct_points": _round(
            (
                recency_dominance_rate - recency_control_dominance_rate
                if recency_dominance_rate is not None
                and recency_control_dominance_rate is not None
                else None
            ),
            4,
        ),
        "recency_weighted_median_paired_favorable_minus_adverse_pct": _round(
            market_session_baseline.weighted_percentile(
                recency_paired_edges, 0.50
            ),
            6,
        ),
        "outcome_session_composition_counts": session_composition_counts,
        "outcome_mean_active_ratio": _round(
            mean(selected_active_ratios) if selected_active_ratios else None, 6
        ),
        "outcome_mean_weekend_ratio": _round(
            1.0 - mean(selected_active_ratios)
            if selected_active_ratios
            else None,
            6,
        ),
        "outcome_session_timezone": "America/New_York",
        "outcome_session_definition": "SUN_18_ET__FRI_20_ET_ACTIVE",
        "distinct_symbols": len(
            {
                symbol
                for row in selected
                for symbol in (
                    row.get("event", {}).get("market_episode_member_symbols")
                    or [row.get("event", {}).get("symbol")]
                )
                if symbol
            }
        ),
        "distinct_event_types": len(
            {
                event_type
                for row in selected
                for event_type in (
                    row.get("event", {}).get("market_episode_member_event_types")
                    or [row.get("event", {}).get("event_type")]
                )
                if event_type
            }
        ),
        "successes": successes,
        "hit_rate_pct": _round(hit_rate, 4),
        "wilson_95_lower_pct": _round(
            (_wilson_lower(successes, len(success_flags)) or 0.0) * 100.0, 4
        ) if success_flags else None,
        "avg_directional_return_pct": _round(mean(directional), 6) if directional else None,
        "median_directional_return_pct": _round(median(directional), 6) if directional else None,
        "median_mfe_pct": _round(median_mfe, 6),
        "universe_median_mfe_pct": _round(universe_median_mfe, 6),
        "universe_p90_mfe_pct": _round(universe_p90_mfe, 6),
        "median_mfe_percentile_pct": _round(median_mfe_percentile, 4),
        "median_mfe_uplift_vs_universe_pct": _round(mfe_uplift, 4),
        "session_adjusted_mfe_percentile_pct": _round(
            session_mfe_percentile, 4
        ),
        "session_matched_control_median_mfe_pct": _round(
            session_mfe_median, 6
        ),
        "session_matched_control_p90_mfe_pct": _round(
            session_mfe_p90, 6
        ),
        "session_adjusted_mfe_uplift_pct": _round(session_mfe_uplift, 4),
        "session_adjusted_mae_percentile_pct": _round(
            session_mae_percentile, 4
        ),
        "session_matched_control_median_mae_pct": _round(
            session_mae_median, 6
        ),
        "session_matched_control_p90_mae_pct": _round(
            session_mae_p90, 6
        ),
        "movement_width_floor_base_pct": _round(base_floor, 6),
        "movement_width_floor_scale_factor": _round(floor_scale, 6),
        "movement_width_floor_effective_pct": _round(effective_floor, 6),
        "movement_width_floor_reference_samples": floor_reference_samples,
        "movement_width_floor_source": floor_source,
        "active_reference_mfe_effective_samples": _round(
            active_control_mfe_effective, 4
        ),
        "active_reference_mfe_p90_pct": _round(active_control_mfe_p90, 6),
        "movement_width_floor_policy": (
            "absolute floor only; prior raw-price session calibration; "
            "probability/asymmetry route gates unchanged; p90/p95 adverse "
            "excursion remains mandatory disclosure, not a standalone rejection"
        ),
        "expected_favorable_excursion_pct": _round(expected_favorable, 6),
        "favorable_minus_p90_adverse_pct": _round(
            favorable_minus_p90_adverse, 6
        ),
        "median_mae_pct": _round(median_mae, 6),
        "mae_p75_pct": _round(_quantile(risk_mae, 0.75), 6),
        "mae_p90_pct": _round(mae_p90, 6),
        "mae_p95_pct": _round(_quantile(risk_mae, 0.95), 6),
        "mae_tail_policy": (
            "per-episode maximum member adverse excursion; each Market "
            "Episode still has total statistical weight one"
        ),
        "median_mfe_mae_ratio": _round(efficiency.ratio, 6),
        "median_mfe_mae_ratio_state": efficiency.state,
        "median_mfe_mae_ratio_policy_version": (
            research_mfe_mae_efficiency.POLICY_VERSION
        ),
        "median_time_to_first_progress_seconds": _round(median(first_progress), 2) if first_progress else None,
        "median_time_to_mfe_seconds": _round(median(time_to_mfe), 2) if time_to_mfe else None,
        "avg_target_progress_ratio": _round(mean(target_progress), 6) if target_progress else None,
        "target_reached_rate_pct": _round(sum(target_flags) / len(target_flags) * 100.0, 4) if target_flags else None,
        "qualifying_candle_ambiguity_rate_pct": _round(
            sum(ambiguity_flags) / len(ambiguity_flags) * 100.0, 4
        ) if ambiguity_flags else None,
        "control_sample_size": len(control_success_flags),
        "control_hit_rate_pct": _round(control_hit_rate, 4),
        "hit_rate_improvement_pct_points": _round(
            hit_rate - control_hit_rate,
            4,
        ) if hit_rate is not None and control_hit_rate is not None else None,
        "session_matched_control_sample_size": len(session_control_successes),
        "session_matched_control_effective_samples": _round(
            session_control_effective, 4
        ),
        "session_matched_mfe_effective_samples": _round(
            session_mfe_effective, 4
        ),
        "session_matched_mae_effective_samples": _round(
            session_mae_effective, 4
        ),
        "session_matched_hit_rate_baseline_pct": _round(
            session_hit_baseline, 4
        ),
        "session_hit_rate_improvement_pct_points": _round(
            hit_rate - session_hit_baseline,
            4,
        )
        if hit_rate is not None and session_hit_baseline is not None
        else None,
        "session_baseline_complete": (
            bool(selected)
            and len(selected_active_ratios) == len(selected)
            and session_control_effective >= SESSION_BASELINE_MIN_EFFECTIVE_SAMPLES
            and session_mfe_effective >= SESSION_BASELINE_MIN_EFFECTIVE_SAMPLES
            and session_mae_effective >= SESSION_BASELINE_MIN_EFFECTIVE_SAMPLES
        ),
        "baseline_policy": (
            "same outcome-horizon ACTIVE/WEEKEND composition; "
            "America/New_York session; triangular similarity weighting"
        ),
        "unadjusted_one_sided_p_value": _round(
            _one_sided_two_proportion_p(
                successes,
                len(success_flags),
                control_successes,
                len(control_success_flags),
            ),
            8,
        ),
        "one_sided_p_value": _round(
            _one_sided_two_proportion_p(
                successes,
                len(success_flags),
                weighted_successes,
                session_control_effective,
            ),
            8,
        ),
        "raw_sample_event_ids": sorted(selected_ids)[:20],
        "independent_episode_representative_event_ids": sorted(
            int(row.get("event", {}).get("event_id") or 0)
            for row in selected
        )[:20],
        "sample_event_ids": sorted(
            int(row.get("event", {}).get("event_id") or 0)
            for row in selected
        )[:20],
    }
    if include_private_evidence_keys:
        span = timedelta(
            minutes=research_market_episode.episode_minutes(horizon)
        )
        result["_market_episode_evidence_intervals"] = sorted(
            (
                _utc(row.get("event", {}).get("alert_time_utc")).isoformat(),
                (
                    _utc(row.get("event", {}).get("alert_time_utc")) + span
                ).isoformat(),
            )
            for row in selected
        )
    return result


def summarize_outcomes(
    selected: Sequence[Mapping[str, Any]],
    universe: Sequence[Mapping[str, Any]],
    *,
    recent_window_days: int = 21,
    recency_half_life_days: float = 14.0,
    evidence_as_of_utc: Any = None,
) -> Dict[str, Any]:
    """Public deterministic metric surface for raw decision/outcome rows."""
    return _metrics(
        selected,
        universe,
        recent_window_days=recent_window_days,
        recency_half_life_days=recency_half_life_days,
        evidence_as_of_utc=evidence_as_of_utc,
    )


def summarize_preaggregated_market_episodes(
    selected: Sequence[Mapping[str, Any]],
    universe: Sequence[Mapping[str, Any]],
    *,
    recent_window_days: int = 21,
    recency_half_life_days: float = 14.0,
    evidence_as_of_utc: Any,
) -> Dict[str, Any]:
    """Summarize trusted, finalized episode aggregates with fail-closed checks."""

    as_of = _utc(evidence_as_of_utc)
    episode_keys: list[str] = []
    directions: set[str] = set()
    for row in universe:
        event = row.get("event") if isinstance(row.get("event"), Mapping) else {}
        key = str(event.get("market_episode_key") or "")
        finalized_at = event.get("market_episode_finalization_time_utc")
        direction = str(event.get("direction") or "").upper()
        if not key or finalized_at is None:
            raise ValueError(
                "preaggregated market episodes require identity and finalization metadata"
            )
        if _utc(finalized_at) > as_of:
            raise ValueError("preaggregated market episode is not finalized as of evaluation")
        episode_keys.append(key)
        if direction:
            directions.add(direction)
    if len(episode_keys) != len(set(episode_keys)):
        raise ValueError("preaggregated market episode keys must be unique")
    if len(directions) > 1:
        raise ValueError("preaggregated market episodes must share one direction")
    return _metrics(
        selected,
        universe,
        recent_window_days=recent_window_days,
        recency_half_life_days=recency_half_life_days,
        evidence_as_of_utc=as_of,
        already_independent_episodes=True,
    )


def rank_prospective_metrics(
    metrics: Mapping[str, Any], *, horizon_minutes: int
) -> Dict[str, Any]:
    """Transparent prospective priority score; never an activation gate.

    The score orders Shadow candidates by the eight dimensions requested by
    the research policy.  It cannot promote a formula and it never weakens a
    probability or risk requirement on weekends.  A prior-only weekend scale
    affects only the absolute favorable-width reference.
    """
    horizon = max(1, int(horizon_minutes))
    hit = max(0.0, min(100.0, _number(metrics.get("hit_rate_pct")) or 0.0))
    wilson = max(
        0.0,
        min(100.0, _number(metrics.get("wilson_95_lower_pct")) or 0.0),
    )
    probability_improvement = _number(
        metrics.get("session_hit_rate_improvement_pct_points")
    )
    if probability_improvement is None:
        probability_improvement = _number(
            metrics.get("hit_rate_improvement_pct_points")
        )
    dominance = max(
        0.0,
        min(
            100.0,
            _number(metrics.get("favorable_dominance_rate_pct")) or 0.0,
        ),
    )
    dominance_wilson = max(
        0.0,
        min(
            100.0,
            _number(
                metrics.get("favorable_dominance_wilson_95_lower_pct")
            )
            or 0.0,
        ),
    )
    dominance_improvement = _number(
        metrics.get("favorable_dominance_improvement_pct_points")
    )
    paired_edge = _number(
        metrics.get("median_paired_favorable_minus_adverse_pct")
    )
    mfe = max(0.0, _number(metrics.get("median_mfe_pct")) or 0.0)
    mae_p90 = max(0.0, _number(metrics.get("mae_p90_pct")) or 0.0)
    efficiency = research_mfe_mae_efficiency.from_metrics(metrics)
    width_percentile = _number(
        metrics.get("session_adjusted_mfe_percentile_pct")
    )
    if width_percentile is None:
        width_percentile = _number(metrics.get("median_mfe_percentile_pct"))
    width_percentile = max(0.0, min(100.0, width_percentile or 0.0))
    effective_floor = max(
        0.01, minimum_wide_move_pct(horizon, metrics)
    )
    def improvement_quality(value: Optional[float]) -> float:
        bounded = value if value is not None else -5.0
        return max(0.0, min(1.0, (bounded + 5.0) / 25.0))

    probability_route_quality = (
        0.40 * hit / 100.0
        + 0.40 * wilson / 100.0
        + 0.20 * improvement_quality(probability_improvement)
    )
    asymmetry_route_quality = (
        0.30 * dominance / 100.0
        + 0.30 * dominance_wilson / 100.0
        + 0.20 * improvement_quality(dominance_improvement)
        + 0.10 * max(
            0.0, min(1.0, (paired_edge or 0.0) / effective_floor)
        )
        + 0.10 * hit / 100.0
    )
    eligible_routes = _eligible_ranking_routes(
        metrics,
        phase="PROSPECTIVE",
        require_multiple_testing=False,
    )
    route_qualities = {
        "PROBABILITY": probability_route_quality,
        "ASYMMETRY": asymmetry_route_quality,
    }
    route_pool = eligible_routes or tuple(route_qualities)
    selected_historical_route = max(
        route_pool, key=lambda route: route_qualities[route]
    )
    speed_seconds = _number(metrics.get("median_time_to_first_progress_seconds"))
    horizon_seconds = float(horizon * 60)
    speed_quality = (
        max(0.0, 1.0 - min(horizon_seconds, max(0.0, speed_seconds)) / horizon_seconds)
        if speed_seconds is not None
        else 0.0
    )
    sample_size = max(0, int(metrics.get("sample_size") or 0))
    rarity_quality = {
        "RARE": 1.0,
        "UNCOMMON": 0.65,
        "COMMON": 0.25,
    }.get(str(metrics.get("rarity_class") or "").upper(), 0.0)
    components = {
        "probability_or_asymmetry": 0.28
        * route_qualities[selected_historical_route],
        "favorable_movement": 0.12 * min(1.0, mfe / effective_floor),
        "movement_width": 0.16 * (width_percentile / 100.0),
        "low_adverse_movement": 0.15 * max(
            0.0, min(1.0, 1.0 - mae_p90 / max(mfe, 0.01))
        ),
        "mfe_mae_ratio": 0.10 * efficiency.capped_quality(5.0),
        "speed": 0.06 * speed_quality,
        "rarity": 0.03 * rarity_quality,
        "sample_size": 0.10 * min(
            1.0, math.log1p(sample_size) / math.log(51.0)
        ),
    }
    historical_score = round(100.0 * sum(components.values()), 4)
    current_score = _current_relevance_score(
        metrics,
        horizon_minutes=horizon_minutes,
        allowed_routes=eligible_routes,
    )
    blended_components = {
        key: round(70.0 * value, 4) for key, value in components.items()
    }
    blended_components["current_relevance"] = round(0.30 * current_score, 4)
    return {
        "policy_version": "prospective-shadow-priority-v3-rolling-70-30",
        "mfe_mae_efficiency_policy_version": (
            research_mfe_mae_efficiency.POLICY_VERSION
        ),
        "score": round(0.70 * historical_score + 0.30 * current_score, 4),
        "historical_score": historical_score,
        "current_relevance_score": current_score,
        "weights": {"historical_pct": 70, "current_relevance_pct": 30},
        "selected_historical_route": selected_historical_route,
        "eligible_routes": list(eligible_routes),
        "route_selection_fallback": not bool(eligible_routes),
        "components": blended_components,
        "weekend_adjustment_scope": "absolute favorable width only",
        "activation_effect": "none; ranking is descriptive",
    }


def minimum_wide_move_pct(
    horizon_minutes: int, metrics: Optional[Mapping[str, Any]] = None
) -> float:
    base = float(MIN_MEDIAN_MFE_BY_HORIZON.get(int(horizon_minutes), 0.0))
    if not isinstance(metrics, Mapping):
        return base
    factor = _number(metrics.get("movement_width_floor_scale_factor"))
    if factor is None:
        return base
    return base * min(1.0, max(0.50, factor))


def _predicate_catalog(
    rows: Sequence[Mapping[str, Any]],
    feature_rows: Sequence[Mapping[str, Any]],
    config: DiscoveryConfig,
) -> list[Dict[str, Any]]:
    values_by_feature: Dict[str, list[Any]] = {}
    for features in feature_rows:
        for name, value in features.items():
            values_by_feature.setdefault(name, []).append(value)

    predicates: Dict[str, Dict[str, Any]] = {}
    for feature, values in sorted(values_by_feature.items()):
        if not discovery_candidate_feature_allowed(feature):
            continue
        # Weekday numbers are labels, not an ordered magnitude. Formulae may
        # use the explicit weekday name, exact market session, or continuous
        # per-window session ratios instead of a misleading weekday threshold.
        numeric = (
            []
            if feature in {"time.utc_weekday", "time.market_local_weekday"}
            else [
                number
                for value in values
                if (number := _number(value)) is not None
            ]
        )
        non_missing_min = min(config.min_discovery_samples, max(3, len(rows) // 5))
        if len(numeric) >= non_missing_min and len(set(round(value, 10) for value in numeric)) > 1:
            for fraction in config.numeric_quantiles:
                threshold = _quantile(numeric, fraction)
                if threshold is None:
                    continue
                threshold = round(threshold, 8)
                for operator in (">=", "<="):
                    condition = {
                        "feature": feature,
                        "operator": operator,
                        "value": threshold,
                        "_source": "NUMERIC_QUANTILE",
                        "_quantile_fraction": float(fraction),
                    }
                    key = json.dumps(
                        _canonical_conditions([condition])[0],
                        sort_keys=True,
                        ensure_ascii=False,
                    )
                    predicates[key] = condition
            continue

        counts: Dict[Any, int] = {}
        for value in values:
            if isinstance(value, (str, bool)):
                counts[value] = counts.get(value, 0) + 1
        for value, count in sorted(counts.items(), key=lambda item: (-item[1], str(item[0])))[:16]:
            if count < non_missing_min or count >= len(rows):
                continue
            condition = {
                "feature": feature,
                "operator": "==",
                "value": value,
                "_source": "CATEGORICAL_EXACT",
            }
            key = json.dumps(
                _canonical_conditions([condition])[0],
                sort_keys=True,
                ensure_ascii=False,
            )
            predicates[key] = condition
    return list(predicates.values())


def _refit_predicate_blueprints(
    blueprints: Sequence[Mapping[str, Any]],
    training_rows: Sequence[Mapping[str, Any]],
) -> Optional[list[Dict[str, Any]]]:
    """Materialize one frozen structure from prior-only training rows.

    Numeric values retain only their quantile position and are recalculated on
    the expanding training prefix. Categorical values were selected in the
    original Fit partition and remain exact. Outcome fields are never read.
    """

    feature_rows = [extract_decision_features(row) for row in training_rows]
    conditions: list[Dict[str, Any]] = []
    for blueprint in blueprints:
        feature = str(blueprint.get("feature") or "")
        operator = str(blueprint.get("operator") or "")
        source = str(blueprint.get("_source") or "")
        if not feature or operator not in ALLOWED_OPERATORS:
            return None
        if source == "NUMERIC_QUANTILE":
            fraction = _number(blueprint.get("_quantile_fraction"))
            values = [
                number
                for features in feature_rows
                if (number := _strict_json_number(features.get(feature)))
                is not None
            ]
            if fraction is None or len(values) < 3:
                return None
            threshold = _quantile(values, fraction)
            if threshold is None:
                return None
            value: Any = round(threshold, 8)
        elif source == "CATEGORICAL_EXACT":
            value = blueprint.get("value")
            if not any(
                feature in features and type(features.get(feature)) is type(value)
                for features in feature_rows
            ):
                return None
        else:
            return None
        conditions.append(
            {"feature": feature, "operator": operator, "value": value}
        )
    return _canonical_conditions(conditions) if conditions else None


def _walk_forward_selection_folds(
    rows: Sequence[Mapping[str, Any]], *, fold_count: int
) -> list[tuple[int, ...]]:
    """Split chronological Selection rows without splitting a timestamp."""

    ordered_times = sorted(
        {research_market_episode.row_time_utc(row) for row in rows}
    )
    if not ordered_times:
        return []
    folds = max(1, min(int(fold_count), len(ordered_times)))
    time_groups: list[list[datetime]] = [[] for _ in range(folds)]
    for index, timestamp in enumerate(ordered_times):
        bucket = min(folds - 1, (index * folds) // len(ordered_times))
        time_groups[bucket].append(timestamp)
    index_by_time: Dict[datetime, list[int]] = {}
    for index, row in enumerate(rows):
        index_by_time.setdefault(
            research_market_episode.row_time_utc(row), []
        ).append(index)
    return [
        tuple(
            index
            for timestamp in group
            for index in index_by_time[timestamp]
        )
        for group in time_groups
        if group
    ]


def _preliminary_score(
    metrics: Mapping[str, Any], complexity: int, *, route: str | None = None
) -> float:
    hit = _number(metrics.get("hit_rate_pct")) or 0.0
    lower = _number(metrics.get("wilson_95_lower_pct")) or 0.0
    improvement = _number(metrics.get("session_hit_rate_improvement_pct_points"))
    if improvement is None:
        improvement = _number(metrics.get("hit_rate_improvement_pct_points")) or 0.0
    movement_percentile = _number(
        metrics.get("session_adjusted_mfe_percentile_pct")
    )
    if movement_percentile is None:
        movement_percentile = (
            _number(metrics.get("median_mfe_percentile_pct")) or 0.0
        )
    favorable_edge = (
        _number(metrics.get("favorable_minus_p90_adverse_pct")) or 0.0
    )
    dominance = _number(metrics.get("favorable_dominance_rate_pct")) or 0.0
    dominance_lower = (
        _number(metrics.get("favorable_dominance_wilson_95_lower_pct")) or 0.0
    )
    dominance_improvement = (
        _number(metrics.get("favorable_dominance_improvement_pct_points"))
        or 0.0
    )
    probability_signal = (
        0.22 * lower
        + 0.10 * hit
        + 0.12 * max(-20.0, improvement)
    )
    asymmetry_signal = (
        0.22 * dominance_lower
        + 0.10 * dominance
        + 0.12 * max(-20.0, dominance_improvement)
    )
    efficiency = research_mfe_mae_efficiency.from_metrics(metrics)
    sample = int(metrics.get("sample_size") or 0)
    normalized_route = str(route or "").upper()
    route_signal = (
        probability_signal
        if normalized_route == "PROBABILITY"
        else asymmetry_signal
        if normalized_route == "ASYMMETRY"
        else max(probability_signal, asymmetry_signal)
    )
    return (
        route_signal
        + 0.32 * movement_percentile
        + 32.0 * efficiency.capped_quality(4.0)
        + 5.0 * max(-1.0, min(3.0, favorable_edge))
        + 3.0 * math.log1p(sample)
        - 2.0 * max(0, complexity - 1)
        - 3.0 * max(0, complexity - 3) ** 2
    )


def _stable_route_names(
    fit: Mapping[str, Any],
    selection: Mapping[str, Any],
    *,
    maximum_rate_gap: float,
) -> tuple[str, ...]:
    """Return evidence routes that remain stable across a nested split."""

    def probability_improvement(metrics: Mapping[str, Any]) -> Optional[float]:
        value = _number(metrics.get("session_hit_rate_improvement_pct_points"))
        if value is None:
            value = _number(metrics.get("hit_rate_improvement_pct_points"))
        return value

    routes: list[str] = []
    fit_hit = _number(fit.get("hit_rate_pct"))
    selection_hit = _number(selection.get("hit_rate_pct"))
    fit_probability_improvement = probability_improvement(fit)
    selection_probability_improvement = probability_improvement(selection)
    if (
        fit_hit is not None
        and selection_hit is not None
        and abs(fit_hit - selection_hit) <= float(maximum_rate_gap)
        and fit_probability_improvement is not None
        and fit_probability_improvement >= 0.0
        and selection_probability_improvement is not None
        and selection_probability_improvement >= 0.0
    ):
        routes.append("PROBABILITY")

    fit_dominance = _number(fit.get("favorable_dominance_rate_pct"))
    selection_dominance = _number(
        selection.get("favorable_dominance_rate_pct")
    )
    fit_asymmetry_improvement = _number(
        fit.get("favorable_dominance_improvement_pct_points")
    )
    selection_asymmetry_improvement = _number(
        selection.get("favorable_dominance_improvement_pct_points")
    )
    fit_paired_edge = _number(
        fit.get("median_paired_favorable_minus_adverse_pct")
    )
    selection_paired_edge = _number(
        selection.get("median_paired_favorable_minus_adverse_pct")
    )
    if (
        fit_dominance is not None
        and selection_dominance is not None
        and abs(fit_dominance - selection_dominance)
        <= float(maximum_rate_gap)
        and fit_asymmetry_improvement is not None
        and fit_asymmetry_improvement >= 0.0
        and selection_asymmetry_improvement is not None
        and selection_asymmetry_improvement >= 0.0
        and fit_paired_edge is not None
        and fit_paired_edge > 0.0
        and selection_paired_edge is not None
        and selection_paired_edge > 0.0
    ):
        routes.append("ASYMMETRY")
    return tuple(routes)


def _current_relevance_score(
    metrics: Mapping[str, Any], *, horizon_minutes: int,
    allowed_routes: Sequence[str] | None = None,
) -> float:
    """Bounded 0..100 quality score for the frozen recent evidence slice."""
    hit = max(
        0.0,
        min(100.0, _number(metrics.get("recency_weighted_hit_rate_pct")) or 0.0),
    )
    wilson = max(
        0.0,
        min(
            100.0,
            _number(
                metrics.get("recency_weighted_wilson_95_lower_approx_pct")
            )
            or 0.0,
        ),
    )
    probability_quality = 0.5 * hit / 100.0 + 0.5 * wilson / 100.0
    dominance = max(
        0.0,
        min(
            100.0,
            _number(
                metrics.get("recency_weighted_favorable_dominance_rate_pct")
            )
            or 0.0,
        ),
    )
    dominance_wilson = max(
        0.0,
        min(
            100.0,
            _number(
                metrics.get(
                    "recency_weighted_favorable_dominance_wilson_95_lower_approx_pct"
                )
            )
            or 0.0,
        ),
    )
    asymmetry_quality = (
        0.5 * dominance / 100.0 + 0.5 * dominance_wilson / 100.0
    )
    probability_improvement = _number(
        metrics.get("recency_weighted_hit_rate_improvement_pct_points")
    )
    asymmetry_improvement = _number(
        metrics.get(
            "recency_weighted_favorable_dominance_improvement_pct_points"
        )
    )
    def improvement_quality(value: Optional[float]) -> float:
        bounded = value if value is not None else -5.0
        return max(0.0, min(1.0, (bounded + 5.0) / 25.0))

    probability_route_quality = (
        0.70 * probability_quality
        + 0.30 * improvement_quality(probability_improvement)
    )
    asymmetry_route_quality = (
        0.70 * asymmetry_quality
        + 0.30 * improvement_quality(asymmetry_improvement)
    )
    mfe = max(
        0.0, _number(metrics.get("recency_weighted_median_mfe_pct")) or 0.0
    )
    mae = max(
        0.0, _number(metrics.get("recency_weighted_median_mae_pct")) or 0.0
    )
    efficiency = research_mfe_mae_efficiency.classify(mfe, mae)
    floor = max(0.01, minimum_wide_move_pct(horizon_minutes, metrics))
    effective_n = max(
        0.0, _number(metrics.get("recency_effective_sample_size")) or 0.0
    )
    raw_age_hours = _number(metrics.get("last_sample_age_hours"))
    age_hours = max(0.0, raw_age_hours if raw_age_hours is not None else 10**9)
    freshness = max(0.0, 1.0 - min(21.0 * 24.0, age_hours) / (21.0 * 24.0))
    normalized_routes = {
        str(route).upper() for route in (allowed_routes or ())
    }
    route_quality = (
        max(
            quality
            for route, quality in (
                ("PROBABILITY", probability_route_quality),
                ("ASYMMETRY", asymmetry_route_quality),
            )
            if route in normalized_routes
        )
        if normalized_routes
        else max(probability_route_quality, asymmetry_route_quality)
    )
    quality = (
        0.50 * route_quality
        + 0.20 * efficiency.capped_quality(3.0)
        + 0.15 * min(1.0, mfe / floor)
        + 0.10 * min(1.0, effective_n / 12.0)
        + 0.05 * freshness
    )
    return round(100.0 * quality, 4)


def _eligible_ranking_routes(
    metrics: Mapping[str, Any],
    *,
    phase: str,
    probability_q_value: Any = None,
    asymmetry_q_value: Any = None,
    require_multiple_testing: bool,
) -> tuple[str, ...]:
    """Return only routes that satisfy their immutable statistical contract."""

    result = research_formula_acceptance.evaluate(
        metrics,
        phase=phase,
        minimum_matches=0,
        minimum_controls=0,
        minimum_recent_matches=0,
        minimum_recent_effective_samples=0.0,
        minimum_recent_control_effective_samples=0.0,
        maximum_last_match_age_hours=21.0 * 24.0,
        probability_q_value=probability_q_value,
        asymmetry_q_value=asymmetry_q_value,
        require_multiple_testing=require_multiple_testing,
    )
    return tuple(result["accepted_paths"])


def _final_score(
    discovery: Mapping[str, Any],
    holdout: Mapping[str, Any],
    *,
    horizon_minutes: int,
    q_value: Optional[float],
    asymmetry_q_value: Optional[float] = None,
    complexity: int,
) -> float:
    evidence = holdout if int(holdout.get("sample_size") or 0) else discovery
    hit = (_number(evidence.get("hit_rate_pct")) or 0.0) / 100.0
    lower = (_number(evidence.get("wilson_95_lower_pct")) or 0.0) / 100.0
    improvement = _number(evidence.get("session_hit_rate_improvement_pct_points"))
    if improvement is None:
        improvement = _number(evidence.get("hit_rate_improvement_pct_points")) or 0.0
    improvement_quality = min(1.0, max(0.0, (improvement + 5.0) / 25.0))
    dominance = max(
        0.0,
        min(100.0, _number(evidence.get("favorable_dominance_rate_pct")) or 0.0),
    ) / 100.0
    dominance_lower = max(
        0.0,
        min(
            100.0,
            _number(evidence.get("favorable_dominance_wilson_95_lower_pct"))
            or 0.0,
        ),
    ) / 100.0
    dominance_improvement = _number(
        evidence.get("favorable_dominance_improvement_pct_points")
    )
    dominance_improvement_quality = min(
        1.0,
        max(
            0.0,
            ((dominance_improvement if dominance_improvement is not None else -5.0) + 5.0)
            / 25.0,
        ),
    )
    paired_edge = _number(
        evidence.get("median_paired_favorable_minus_adverse_pct")
    )
    paired_edge_quality = max(
        0.0,
        min(1.0, (paired_edge or 0.0) / max(0.01, minimum_wide_move_pct(horizon_minutes, evidence))),
    )
    probability_route_quality = (
        0.50 * lower + 0.235 * hit + 0.265 * improvement_quality
    )
    asymmetry_route_quality = (
        0.35 * dominance_lower
        + 0.20 * dominance
        + 0.20 * dominance_improvement_quality
        + 0.10 * hit
        + 0.15 * paired_edge_quality
    )
    eligible_routes = _eligible_ranking_routes(
        evidence,
        phase="HISTORICAL",
        probability_q_value=q_value,
        asymmetry_q_value=asymmetry_q_value,
        require_multiple_testing=True,
    )
    route_qualities = {
        "PROBABILITY": probability_route_quality,
        "ASYMMETRY": asymmetry_route_quality,
    }
    route_pool = eligible_routes or tuple(route_qualities)
    selected_route = max(route_pool, key=lambda route: route_qualities[route])
    mae = max(0.0, _number(evidence.get("median_mae_pct")) or 0.0)
    mae_quality = 1.0 / (1.0 + mae)
    efficiency = research_mfe_mae_efficiency.from_metrics(evidence)
    efficiency_quality = efficiency.capped_quality(3.0)
    movement_percentile_value = _number(
        evidence.get("session_adjusted_mfe_percentile_pct")
    )
    if movement_percentile_value is None:
        movement_percentile_value = (
            _number(evidence.get("median_mfe_percentile_pct")) or 0.0
        )
    movement_percentile = max(0.0, min(100.0, movement_percentile_value))
    movement_quality = movement_percentile / 100.0
    median_mfe = max(0.0, _number(evidence.get("median_mfe_pct")) or 0.0)
    session_p90_mfe = _number(
        evidence.get("session_matched_control_p90_mfe_pct")
    )
    universe_p90_mfe = max(
        0.01,
        session_p90_mfe
        if session_p90_mfe is not None
        else (_number(evidence.get("universe_p90_mfe_pct")) or 0.01),
    )
    absolute_movement_quality = min(1.0, median_mfe / universe_p90_mfe)
    progress_seconds = _number(evidence.get("median_time_to_first_progress_seconds"))
    horizon_seconds = max(60.0, float(horizon_minutes) * 60.0)
    speed_quality = (
        max(0.0, 1.0 - min(horizon_seconds, progress_seconds) / horizon_seconds)
        if progress_seconds is not None
        else 0.0
    )
    sample = int(evidence.get("sample_size") or 0)
    sample_quality = min(1.0, math.log1p(sample) / math.log(31.0))
    rarity_share = min(100.0, max(0.0, _number(evidence.get("sample_share_pct")) or 0.0))
    rarity_quality = 1.0 - rarity_share / 100.0
    asymmetry_selected = selected_route == "ASYMMETRY"
    if asymmetry_selected:
        discovery_rate = _number(
            discovery.get("favorable_dominance_rate_pct")
        )
        holdout_rate = _number(
            holdout.get("favorable_dominance_rate_pct")
        )
    else:
        discovery_rate = _number(discovery.get("hit_rate_pct"))
        holdout_rate = _number(holdout.get("hit_rate_pct"))
    stability = (
        max(0.0, 1.0 - abs(discovery_rate - holdout_rate) / 40.0)
        if discovery_rate is not None and holdout_rate is not None
        else 0.0
    )
    probability_q = _number(q_value)
    asymmetry_q = _number(asymmetry_q_value)
    if asymmetry_selected:
        route_quality = route_qualities["ASYMMETRY"]
        selected_route_q = asymmetry_q
    else:
        route_quality = route_qualities["PROBABILITY"]
        selected_route_q = probability_q
    significance = (
        0.0
        if selected_route_q is None
        else max(0.0, 1.0 - selected_route_q)
    )
    target_progress = _number(evidence.get("avg_target_progress_ratio"))
    target_quality = (
        max(0.0, min(1.0, target_progress)) if target_progress is not None else 0.5
    )
    score = 100.0 * (
        0.34 * route_quality
        + 0.23 * movement_quality
        + 0.09 * absolute_movement_quality
        + 0.11 * mae_quality
        + 0.08 * efficiency_quality
        + 0.05 * speed_quality
        + 0.04 * sample_quality
        + 0.025 * stability
        + 0.01 * significance
        + 0.005 * rarity_quality
        + 0.02 * target_quality
    )
    score -= 1.5 * max(0, complexity - 1)
    score -= 2.5 * max(0, complexity - 3) ** 2
    historical_score = max(0.0, min(100.0, score))
    current_score = _current_relevance_score(
        evidence,
        horizon_minutes=horizon_minutes,
        allowed_routes=eligible_routes,
    )
    return round(0.70 * historical_score + 0.30 * current_score, 4)


def _recommended_stage(
    discovery: Mapping[str, Any],
    holdout: Mapping[str, Any],
    *,
    horizon_minutes: int,
    q_value: Optional[float],
    config: DiscoveryConfig,
    asymmetry_q_value: Optional[float] = None,
    complexity: int = 1,
    walk_forward: Optional[Mapping[str, Any]] = None,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    discovery_n = int(discovery.get("sample_size") or 0)
    holdout_n = int(holdout.get("sample_size") or 0)
    if holdout_n < config.min_holdout_samples:
        return "DISCOVERED", ["insufficient chronological holdout sample"]
    stage = "BACKTESTED"
    movement_percentile = _number(
        holdout.get("session_adjusted_mfe_percentile_pct")
    )
    if movement_percentile is None:
        movement_percentile = _number(
            holdout.get("median_mfe_percentile_pct")
        ) or 0.0
    strict_checks = {
        "discovery independent market-episode sample": discovery_n
        >= config.strict_discovery_samples,
        "holdout independent market-episode sample": holdout_n
        >= config.strict_holdout_samples,
        "discovery session-composition baseline coverage": bool(
            discovery.get("session_baseline_complete")
        ),
        "holdout session-composition baseline coverage": bool(
            holdout.get("session_baseline_complete")
        ),
        "wide movement percentile": movement_percentile >= 65.0,
        "versioned walk-forward validation complete": bool(
            isinstance(walk_forward, Mapping)
            and walk_forward.get("policy_version")
            == WALK_FORWARD_POLICY_VERSION
            and walk_forward.get("purge_policy_version")
            == PURGE_POLICY_VERSION
            and walk_forward.get("embargo_policy_version")
            == EMBARGO_POLICY_VERSION
            and walk_forward.get("complete") is True
            and walk_forward.get("outer_test_used") is False
        ),
    }
    if int(complexity) >= 4:
        depth = int(complexity)
        strict_checks["hierarchical discovery sample"] = discovery_n >= max(
            config.strict_discovery_samples,
            config.hierarchical_min_discovery_samples
            + (depth - 4) * config.hierarchical_discovery_sample_increment,
        )
        strict_checks["hierarchical holdout sample"] = holdout_n >= max(
            config.strict_holdout_samples,
            config.hierarchical_min_holdout_samples
            + (depth - 4) * config.hierarchical_holdout_sample_increment,
        )
    acceptance = research_formula_acceptance.evaluate(
        holdout,
        phase="HISTORICAL",
        minimum_matches=config.strict_holdout_samples,
        minimum_controls=config.strict_holdout_samples,
        minimum_recent_matches=3,
        minimum_recent_effective_samples=min(
            6.0, float(config.strict_holdout_samples)
        ),
        maximum_last_match_age_hours=float(
            max(1, int(config.maximum_last_match_age_days)) * 24
        ),
        probability_q_value=q_value,
        asymmetry_q_value=asymmetry_q_value,
        require_multiple_testing=True,
        mandatory_checks=strict_checks,
        probability_checks={
            "probability edge did not materially deteriorate": (
                _number(discovery.get("hit_rate_pct")) is not None
                and _number(holdout.get("hit_rate_pct")) is not None
                and float(holdout["hit_rate_pct"])
                >= float(discovery["hit_rate_pct"]) - 20.0
            )
        },
        asymmetry_checks={
            "asymmetry edge did not materially deteriorate": (
                _number(discovery.get("favorable_dominance_rate_pct"))
                is not None
                and _number(holdout.get("favorable_dominance_rate_pct"))
                is not None
                and float(holdout["favorable_dominance_rate_pct"])
                >= float(discovery["favorable_dominance_rate_pct"]) - 20.0
            )
        },
    )
    reasons.extend(acceptance["missing_by_path"]["COMMON"])
    if not acceptance["research_ready"]:
        reasons.append(
            "neither acceptance path passed: probability=["
            + ", ".join(acceptance["missing_by_path"]["PROBABILITY"])
            + "]; asymmetry=["
            + ", ".join(acceptance["missing_by_path"]["ASYMMETRY"])
            + "]"
        )
    if acceptance["research_ready"]:
        # Discovery itself stops at SHADOW. A separate future-observation
        # validator may later promote under the owner-approved live policy.
        return "SHADOW", [
            "research acceptance passed via "
            + "/".join(acceptance["accepted_paths"])
        ]
    return stage, reasons


def _search_direction(
    discovery_rows: Sequence[Mapping[str, Any]],
    selection_rows: Sequence[Mapping[str, Any]],
    holdout_rows: Sequence[Mapping[str, Any]],
    *,
    direction: str,
    horizon_minutes: int,
    feature_schema_version: str,
    config: DiscoveryConfig,
    discovery_as_of_utc: Any = None,
    selection_as_of_utc: Any = None,
    holdout_as_of_utc: Any = None,
) -> Dict[str, Any]:
    discovery_features = [extract_decision_features(row) for row in discovery_rows]
    holdout_features: Optional[list[Dict[str, Any]]] = None
    purge_minutes = research_market_episode.episode_minutes(horizon_minutes)
    embargo_minutes = int(horizon_minutes)
    boundary_gap = timedelta(minutes=purge_minutes + embargo_minutes)
    # Formula construction sees Fit; identity/ranking and hierarchy see the
    # separate Selection partition; the final Test partition stays untouched
    # until champions and their order have been frozen.
    hierarchical_fit_rows = list(discovery_rows)
    hierarchical_selection_rows = list(selection_rows)
    predicates = _predicate_catalog(discovery_rows, discovery_features, config)
    predicate_matches: list[set[int]] = []
    usable_predicates: list[Dict[str, Any]] = []
    for predicate in predicates:
        matched = {
            index
            for index, features in enumerate(discovery_features)
            if condition_matches(features, predicate)
        }
        if config.min_discovery_samples <= len(matched) < len(discovery_rows):
            usable_predicates.append(predicate)
            predicate_matches.append(matched)

    evaluated = 0
    tested_condition_sets: set[tuple[int, ...]] = set()
    dedup_observations: Dict[tuple[int, ...], tuple[int, ...]] = {}
    candidates: list[Dict[str, Any]] = []
    family_policy_rejections = 0
    insufficient_sample_rejections = 0
    correlated_sample_rejections = 0

    def compact_evidence_intervals(
        rows: Sequence[Mapping[str, Any]],
        universe_rows: Sequence[Mapping[str, Any]],
        *,
        partition: str,
    ) -> tuple[tuple[str, str, str], ...]:
        if not rows or not universe_rows:
            return ()
        as_of = max(
            research_market_episode.row_time_utc(row)
            for row in universe_rows
        )
        if partition == "discovery" and discovery_as_of_utc is not None:
            as_of = _utc(discovery_as_of_utc)
        if partition == "selection" and selection_as_of_utc is not None:
            as_of = _utc(selection_as_of_utc)
        if partition == "holdout" and holdout_as_of_utc is not None:
            as_of = _utc(holdout_as_of_utc)
        grouped = research_market_episode.group_rows(
            rows, horizon_minutes=horizon_minutes
        )
        # The left edge is censored by lookback or a purge gap.  Keep the
        # entire first visible episode out of both metrics and family identity.
        grouped = grouped[1:] if grouped else []
        finalized, _ = research_market_episode.partition_finalized(
            grouped,
            horizon_minutes=horizon_minutes,
            as_of_utc=as_of,
        )
        complete = [
            episode
            for episode in finalized
            if research_market_episode.episode_evidence_rows(episode)
            and all(
                _final_path_success(row) is not None
                and _number((row.get("outcome_label") or {}).get("mfe_pct"))
                is not None
                and _number((row.get("outcome_label") or {}).get("mae_pct"))
                is not None
                for row in research_market_episode.episode_evidence_rows(episode)
            )
        ]
        return tuple(
            (
                partition,
                episode["start_time_utc"].isoformat(),
                episode["end_time_utc"].isoformat(),
            )
            for episode in complete
        )

    def condition_policy(condition_indexes: Sequence[int]) -> Dict[str, Any]:
        conditions = [usable_predicates[index] for index in condition_indexes]
        return research_formula_families.condition_family_policy(
            conditions,
            justified_exceptions=config.condition_family_exceptions,
            enforce_correlated_families=True,
        )

    def reject_family_policy_before_budget(
        condition_indexes: Sequence[int],
    ) -> bool:
        """Keep policy-invalid combinations from exhausting search budgets."""

        nonlocal family_policy_rejections
        normalized_indexes = tuple(
            sorted({int(index) for index in condition_indexes})
        )
        if normalized_indexes in tested_condition_sets:
            return True
        if condition_policy(normalized_indexes)["valid"]:
            return False
        tested_condition_sets.add(normalized_indexes)
        family_policy_rejections += 1
        return True

    def add_candidate(
        condition_indexes: Sequence[int],
        *,
        minimum_discovery_samples: Optional[int] = None,
        hierarchical: bool = False,
    ) -> Optional[Dict[str, Any]]:
        nonlocal evaluated, family_policy_rejections
        nonlocal insufficient_sample_rejections, correlated_sample_rejections
        normalized_indexes = tuple(sorted({int(index) for index in condition_indexes}))
        if len(normalized_indexes) != len(condition_indexes):
            return None
        if normalized_indexes in tested_condition_sets:
            return None
        tested_condition_sets.add(normalized_indexes)
        policy = condition_policy(normalized_indexes)
        if not policy["valid"]:
            family_policy_rejections += 1
            return None
        if evaluated >= config.max_candidates_evaluated:
            return None
        evaluated += 1
        matched = set(range(len(discovery_rows)))
        for index in normalized_indexes:
            matched &= predicate_matches[index]
        required_sample = max(
            config.min_discovery_samples,
            int(minimum_discovery_samples or config.min_discovery_samples),
        )
        if len(matched) < required_sample:
            insufficient_sample_rejections += 1
            return None
        observation_key = tuple(sorted(matched))
        if observation_key in dedup_observations:
            return None
        dedup_observations[observation_key] = normalized_indexes
        conditions = _canonical_conditions(
            [usable_predicates[index] for index in normalized_indexes]
        )
        selected = [discovery_rows[index] for index in observation_key]
        evaluable_universe = [
            row
            for row in discovery_rows
            if all(
                condition_is_evaluable(extract_decision_features(row), condition)
                for condition in conditions
            )
        ]
        metrics = _metrics(
            selected,
            evaluable_universe,
            recent_window_days=config.recent_window_days,
            recency_half_life_days=config.recency_half_life_days,
            evidence_as_of_utc=discovery_as_of_utc,
            discard_boundary_episode=True,
        )
        if int(metrics.get("sample_size") or 0) < required_sample:
            correlated_sample_rejections += 1
            return None
        candidate = {
            "condition_indexes": normalized_indexes,
            "conditions": conditions,
            "condition_blueprints": [
                dict(usable_predicates[index]) for index in normalized_indexes
            ],
            "condition_families": list(policy["families"]),
            "discovery_indices": observation_key,
            "discovery_metrics": metrics,
            "preliminary_score": _preliminary_score(metrics, len(conditions)),
            "eligible_for_output": not hierarchical,
            "hierarchical": hierarchical,
        }
        candidates.append(candidate)
        return candidate

    def attach_discovery_evidence(candidate: Dict[str, Any]) -> None:
        if "discovery_evidence_intervals" in candidate:
            return
        selected = [
            discovery_rows[index]
            for index in candidate["discovery_indices"]
        ]
        intervals = compact_evidence_intervals(
            selected,
            discovery_rows,
            partition="discovery",
        )
        candidate["discovery_evidence_intervals"] = intervals
        candidate["discovery_evidence_keys"] = tuple(
            f"{partition}:episode:{start}:{end}"
            for partition, start, end in intervals
        )

    def attach_selection(candidate: Dict[str, Any]) -> None:
        if "selection_metrics" in candidate:
            return
        selection_feature_rows = [
            extract_decision_features(row) for row in selection_rows
        ]
        folds = _walk_forward_selection_folds(
            selection_rows, fold_count=config.walk_forward_folds
        )
        matched_index_set: set[int] = set()
        evaluable_index_set: set[int] = set()
        fold_reports: list[Dict[str, Any]] = []
        completed_folds = 0
        for fold_number, validation_indexes in enumerate(folds, start=1):
            validation_start = min(
                research_market_episode.row_time_utc(selection_rows[index])
                for index in validation_indexes
            )
            validation_end = max(
                research_market_episode.row_time_utc(selection_rows[index])
                for index in validation_indexes
            )
            training_cutoff = validation_start - boundary_gap
            expanding_training = list(discovery_rows) + [
                row
                for row in selection_rows
                if research_market_episode.row_time_utc(row) < training_cutoff
            ]
            fold_conditions = _refit_predicate_blueprints(
                candidate["condition_blueprints"], expanding_training
            )
            if not fold_conditions:
                fold_reports.append(
                    {
                        "fold": fold_number,
                        "status": "UNEVALUABLE",
                        "reason": "predicate refit lacked prior-only training data",
                        "training_rows": len(expanding_training),
                        "training_cutoff_utc": training_cutoff,
                        "validation_start_utc": validation_start,
                        "validation_end_utc": validation_end,
                        "validation_rows": len(validation_indexes),
                    }
                )
                continue
            fold_matched = tuple(
                index
                for index in validation_indexes
                if all(
                    condition_matches(
                        selection_feature_rows[index], condition
                    )
                    for condition in fold_conditions
                )
            )
            fold_evaluable = tuple(
                index
                for index in validation_indexes
                if all(
                    condition_is_evaluable(
                        selection_feature_rows[index], condition
                    )
                    for condition in fold_conditions
                )
            )
            fold_as_of = validation_end + timedelta(
                minutes=research_market_episode.episode_minutes(horizon_minutes)
            )
            if selection_as_of_utc is not None:
                fold_as_of = min(fold_as_of, _utc(selection_as_of_utc))
            fold_metrics = _metrics(
                [selection_rows[index] for index in fold_matched],
                [selection_rows[index] for index in fold_evaluable],
                recent_window_days=config.recent_window_days,
                recency_half_life_days=config.recency_half_life_days,
                evidence_as_of_utc=fold_as_of,
                discard_boundary_episode=True,
            )
            matched_index_set.update(fold_matched)
            evaluable_index_set.update(fold_evaluable)
            completed_folds += 1
            fold_reports.append(
                {
                    "fold": fold_number,
                    "status": "COMPLETED",
                    "training_rows": len(expanding_training),
                    "training_cutoff_utc": training_cutoff,
                    "validation_start_utc": validation_start,
                    "validation_end_utc": validation_end,
                    "validation_rows": len(validation_indexes),
                    "conditions": fold_conditions,
                    "metrics": {
                        key: value
                        for key, value in fold_metrics.items()
                        if not str(key).startswith("_")
                    },
                }
            )

        final_conditions = _refit_predicate_blueprints(
            candidate["condition_blueprints"],
            [*discovery_rows, *selection_rows],
        )
        if final_conditions:
            candidate["conditions"] = final_conditions
        matched_indexes = tuple(sorted(matched_index_set))
        selected = [selection_rows[index] for index in matched_indexes]
        evaluable_universe = [
            selection_rows[index] for index in sorted(evaluable_index_set)
        ]
        metrics = _metrics(
            selected,
            evaluable_universe,
            recent_window_days=config.recent_window_days,
            recency_half_life_days=config.recency_half_life_days,
            evidence_as_of_utc=selection_as_of_utc,
            discard_boundary_episode=True,
        )
        candidate["selection_indices"] = matched_indexes
        selection_evidence_intervals = compact_evidence_intervals(
            selected,
            selection_rows,
            partition="selection",
        )
        candidate["selection_evidence_intervals"] = selection_evidence_intervals
        candidate["selection_evidence_keys"] = tuple(
            f"{partition}:episode:{start}:{end}"
            for partition, start, end in selection_evidence_intervals
        )
        candidate["selection_metrics"] = metrics
        candidate["selection_preliminary_score"] = _preliminary_score(
            metrics, len(candidate["conditions"])
        )
        candidate["walk_forward_validation"] = {
            "policy_version": WALK_FORWARD_POLICY_VERSION,
            "purge_policy_version": PURGE_POLICY_VERSION,
            "embargo_policy_version": EMBARGO_POLICY_VERSION,
            "requested_folds": int(config.walk_forward_folds),
            "completed_folds": completed_folds,
            "minimum_completed_folds": int(
                config.walk_forward_min_completed_folds
            ),
            "complete": completed_folds
            >= int(config.walk_forward_min_completed_folds),
            "purge_minutes": purge_minutes,
            "embargo_minutes": embargo_minutes,
            "boundary_gap_minutes": purge_minutes + embargo_minutes,
            "threshold_refit": "expanding prior-only training prefix",
            "outer_test_used": False,
            "folds": fold_reports,
        }

    def attach_holdout(candidate: Dict[str, Any]) -> None:
        nonlocal holdout_features
        if "holdout_metrics" in candidate:
            return
        if holdout_features is None:
            holdout_features = [
                extract_decision_features(row) for row in holdout_rows
            ]
        matched_indexes = tuple(
            index
            for index, features in enumerate(holdout_features)
            if all(
                condition_matches(features, condition)
                for condition in candidate["conditions"]
            )
        )
        selected = [holdout_rows[index] for index in matched_indexes]
        evaluable_universe = [
            holdout_rows[index]
            for index, features in enumerate(holdout_features)
            if all(
                condition_is_evaluable(features, condition)
                for condition in candidate["conditions"]
            )
        ]
        candidate["holdout_indices"] = matched_indexes
        candidate["holdout_metrics"] = _metrics(
            selected,
            evaluable_universe,
            recent_window_days=config.recent_window_days,
            recency_half_life_days=config.recency_half_life_days,
            evidence_as_of_utc=holdout_as_of_utc,
            discard_boundary_episode=True,
        )

    def attach_hierarchical_screen(candidate: Dict[str, Any]) -> None:
        if "hierarchical_selection_metrics" in candidate:
            return
        attach_selection(candidate)
        fit_metrics = candidate["discovery_metrics"]
        selection_metrics = candidate["selection_metrics"]
        candidate["hierarchical_fit_metrics"] = fit_metrics
        candidate["hierarchical_selection_metrics"] = selection_metrics
        candidate["hierarchical_fit_preliminary_score"] = _preliminary_score(
            fit_metrics, len(candidate["conditions"])
        )
        candidate["hierarchical_selection_preliminary_score"] = _preliminary_score(
            selection_metrics, len(candidate["conditions"])
        )

    def stable_parent(candidate: Dict[str, Any]) -> bool:
        attach_hierarchical_screen(candidate)
        depth = len(candidate["conditions"])
        if depth <= 3:
            required_fit = config.strict_discovery_samples
            required_selection = config.strict_holdout_samples
        else:
            required_fit = max(
                config.strict_discovery_samples,
                config.hierarchical_min_discovery_samples
                + (depth - 4) * config.hierarchical_discovery_sample_increment,
            )
            required_selection = max(
                config.strict_holdout_samples,
                config.hierarchical_min_holdout_samples
                + (depth - 4) * config.hierarchical_holdout_sample_increment,
            )
        fit_metrics = candidate["hierarchical_fit_metrics"]
        selection_metrics = candidate["hierarchical_selection_metrics"]
        stable_routes = _stable_route_names(
            fit_metrics,
            selection_metrics,
            maximum_rate_gap=float(
                config.hierarchical_max_parent_hit_rate_gap
            ),
        )
        score_stable_routes = tuple(
            route
            for route in stable_routes
            if _preliminary_score(
                selection_metrics, len(candidate["conditions"]), route=route
            )
            >= _preliminary_score(
                fit_metrics, len(candidate["conditions"]), route=route
            )
            - float(config.hierarchical_max_parent_score_drop)
        )
        stable = bool(
            int(fit_metrics.get("sample_size") or 0) >= required_fit
            and int(selection_metrics.get("sample_size") or 0)
            >= required_selection
            and stable_routes
            and score_stable_routes
        )
        candidate["hierarchical_parent_stable"] = stable
        candidate["hierarchical_parent_stable_routes"] = list(
            score_stable_routes
        )
        return stable

    def hierarchical_samples(depth: int) -> tuple[int, int]:
        return (
            max(
                config.min_discovery_samples,
                config.hierarchical_min_discovery_samples
                + (depth - 4) * config.hierarchical_discovery_sample_increment,
            ),
            max(
                config.min_holdout_samples,
                config.hierarchical_min_holdout_samples
                + (depth - 4) * config.hierarchical_holdout_sample_increment,
            ),
        )

    def finish_hierarchical_child(
        child: Dict[str, Any], parent: Dict[str, Any]
    ) -> bool:
        attach_hierarchical_screen(parent)
        attach_hierarchical_screen(child)
        depth = len(child["conditions"])
        required_fit, required_selection = hierarchical_samples(depth)
        stable_routes = set(
            _stable_route_names(
                child["hierarchical_fit_metrics"],
                child["hierarchical_selection_metrics"],
                maximum_rate_gap=float(
                    config.hierarchical_max_parent_hit_rate_gap
                ),
            )
        ) & set(parent.get("hierarchical_parent_stable_routes") or ())
        route_gains = {
            route: {
                "fit": _preliminary_score(
                    child["hierarchical_fit_metrics"], depth, route=route
                )
                - _preliminary_score(
                    parent["hierarchical_fit_metrics"],
                    len(parent["conditions"]),
                    route=route,
                ),
                "selection": _preliminary_score(
                    child["hierarchical_selection_metrics"], depth, route=route
                )
                - _preliminary_score(
                    parent["hierarchical_selection_metrics"],
                    len(parent["conditions"]),
                    route=route,
                ),
            }
            for route in stable_routes
        }
        passed_routes = [
            route
            for route, gains in route_gains.items()
            if gains["fit"] >= float(config.hierarchical_min_parent_gain)
            and gains["selection"] >= float(config.hierarchical_min_parent_gain)
        ]
        best_route = (
            max(
                passed_routes or list(route_gains),
                key=lambda route: min(
                    route_gains[route]["fit"],
                    route_gains[route]["selection"],
                ),
            )
            if route_gains
            else None
        )
        passed = bool(
            int(child["hierarchical_fit_metrics"].get("sample_size") or 0)
            >= required_fit
            and int(
                child["hierarchical_selection_metrics"].get("sample_size") or 0
            )
            >= required_selection
            and passed_routes
        )
        child["eligible_for_output"] = passed
        child["hierarchical_validation"] = {
            "parent_condition_count": len(parent["conditions"]),
            "parent_conditions": list(parent["conditions"]),
            "route_incremental_score_gains": {
                route: {
                    name: round(value, 6) for name, value in gains.items()
                }
                for route, gains in sorted(route_gains.items())
            },
            "passed_routes": sorted(passed_routes),
            "fit_incremental_score_gain": round(
                route_gains[best_route]["fit"], 6
            )
            if best_route
            else None,
            "selection_incremental_score_gain": round(
                route_gains[best_route]["selection"], 6
            )
            if best_route
            else None,
            "minimum_incremental_gain": float(config.hierarchical_min_parent_gain),
            "required_fit_samples": required_fit,
            "required_selection_samples": required_selection,
            "passed": passed,
            "final_test_used_for_selection": False,
            "outer_holdout_used_for_selection": False,
            "selection_note": (
                "expanding-refit Walk-forward Selection chooses the hierarchy "
                "under versioned purge/embargo; the final Test remains untouched"
            ),
        }
        return passed

    singles: list[Dict[str, Any]] = []
    for index in range(len(usable_predicates)):
        candidate = add_candidate((index,))
        if candidate is not None:
            singles.append(candidate)
    singles.sort(key=lambda item: item["preliminary_score"], reverse=True)
    top_singles = singles[: config.max_single_predicates]

    pair_count = 0
    for left_offset, left in enumerate(top_singles):
        left_index = int(left["condition_indexes"][0])
        for right in top_singles[left_offset + 1 :]:
            if pair_count >= config.max_pair_candidates or evaluated >= config.max_candidates_evaluated:
                break
            right_index = int(right["condition_indexes"][0])
            if usable_predicates[left_index]["feature"] == usable_predicates[right_index]["feature"]:
                continue
            if reject_family_policy_before_budget((left_index, right_index)):
                continue
            pair_count += 1
            add_candidate((left_index, right_index))
        if pair_count >= config.max_pair_candidates or evaluated >= config.max_candidates_evaluated:
            break

    pairs = [candidate for candidate in candidates if len(candidate["conditions"]) == 2]
    pairs.sort(key=lambda item: item["preliminary_score"], reverse=True)
    triple_count = 0
    for pair in pairs[:80]:
        used = set(pair["condition_indexes"])
        used_features = {usable_predicates[index]["feature"] for index in used}
        for single in top_singles[:35]:
            if triple_count >= config.max_triple_candidates or evaluated >= config.max_candidates_evaluated:
                break
            index = int(single["condition_indexes"][0])
            if index in used or usable_predicates[index]["feature"] in used_features:
                continue
            condition_indexes = (*pair["condition_indexes"], index)
            if reject_family_policy_before_budget(condition_indexes):
                continue
            triple_count += 1
            add_candidate(condition_indexes)
        if triple_count >= config.max_triple_candidates or evaluated >= config.max_candidates_evaluated:
            break

    hierarchical_diagnostics: Dict[str, Any] = {
        "enabled": bool(config.hierarchical_search_enabled),
        "selection_policy": WALK_FORWARD_POLICY_VERSION,
        "fit_rows": len(hierarchical_fit_rows),
        "selection_rows": len(hierarchical_selection_rows),
        "selection_start_time_utc": (
            hierarchical_selection_rows[0]["event"]["alert_time_utc"]
            if hierarchical_selection_rows
            else None
        ),
        "final_test_used_for_hierarchical_selection": False,
        "outer_holdout_used_for_hierarchical_selection": False,
        "stable_triple_parents": 0,
        "quad_candidates_attempted": 0,
        "quad_candidates_tested": 0,
        "quad_candidates_passed_gain": 0,
        "stable_quad_parents": 0,
        "quint_candidates_attempted": 0,
        "quint_candidates_tested": 0,
        "quint_candidates_passed_gain": 0,
        "beam_width": int(config.hierarchical_beam_width),
        "family_exceptions": list(config.condition_family_exceptions),
        "purge_policy_version": PURGE_POLICY_VERSION,
        "embargo_policy_version": EMBARGO_POLICY_VERSION,
        "purge_minutes": purge_minutes,
        "embargo_minutes": embargo_minutes,
    }
    if config.hierarchical_search_enabled and int(config.hierarchical_max_conditions) >= 4:
        beam_width = max(1, int(config.hierarchical_beam_width))
        parent_pool_limit = max(beam_width, beam_width * 4)
        triples = sorted(
            (
                candidate
                for candidate in candidates
                if len(candidate["conditions"]) == 3
                and research_formula_families.condition_family_policy(
                    candidate["conditions"],
                    justified_exceptions=config.condition_family_exceptions,
                    enforce_correlated_families=True,
                )["valid"]
            ),
            key=lambda item: item["preliminary_score"],
            reverse=True,
        )
        stable_triples = [
            candidate
            for candidate in triples[:parent_pool_limit]
            if stable_parent(candidate)
        ]
        stable_triples.sort(
            key=lambda item: min(
                item["hierarchical_fit_preliminary_score"],
                item["hierarchical_selection_preliminary_score"],
            ),
            reverse=True,
        )
        stable_triples = stable_triples[:beam_width]
        hierarchical_diagnostics["stable_triple_parents"] = len(stable_triples)
        extension_singles = top_singles[
            : max(1, int(config.hierarchical_extension_predicates))
        ]
        quads: list[Dict[str, Any]] = []
        quad_attempts = 0
        for parent in stable_triples:
            used = set(parent["condition_indexes"])
            used_features = {
                usable_predicates[index]["feature"] for index in used
            }
            for single in extension_singles:
                if (
                    quad_attempts >= max(0, int(config.max_quad_candidates))
                    or evaluated >= config.max_candidates_evaluated
                ):
                    break
                index = int(single["condition_indexes"][0])
                if index in used or usable_predicates[index]["feature"] in used_features:
                    continue
                condition_indexes = (*parent["condition_indexes"], index)
                if reject_family_policy_before_budget(condition_indexes):
                    continue
                quad_attempts += 1
                required_discovery, _ = hierarchical_samples(4)
                child = add_candidate(
                    condition_indexes,
                    minimum_discovery_samples=required_discovery,
                    hierarchical=True,
                )
                if child is None:
                    continue
                hierarchical_diagnostics["quad_candidates_tested"] += 1
                if finish_hierarchical_child(child, parent):
                    quads.append(child)
                    hierarchical_diagnostics["quad_candidates_passed_gain"] += 1
            if (
                quad_attempts >= max(0, int(config.max_quad_candidates))
                or evaluated >= config.max_candidates_evaluated
            ):
                break
        hierarchical_diagnostics["quad_candidates_attempted"] = quad_attempts

        if int(config.hierarchical_max_conditions) >= 5 and quads:
            stable_quads = [candidate for candidate in quads if stable_parent(candidate)]
            stable_quads.sort(
                key=lambda item: min(
                    item["hierarchical_fit_preliminary_score"],
                    item["hierarchical_selection_preliminary_score"],
                ),
                reverse=True,
            )
            stable_quads = stable_quads[: max(1, beam_width // 2)]
            hierarchical_diagnostics["stable_quad_parents"] = len(stable_quads)
            quint_attempts = 0
            for parent in stable_quads:
                used = set(parent["condition_indexes"])
                used_features = {
                    usable_predicates[index]["feature"] for index in used
                }
                for single in extension_singles:
                    if (
                        quint_attempts >= max(0, int(config.max_quint_candidates))
                        or evaluated >= config.max_candidates_evaluated
                    ):
                        break
                    index = int(single["condition_indexes"][0])
                    if index in used or usable_predicates[index]["feature"] in used_features:
                        continue
                    condition_indexes = (*parent["condition_indexes"], index)
                    if reject_family_policy_before_budget(condition_indexes):
                        continue
                    quint_attempts += 1
                    required_discovery, _ = hierarchical_samples(5)
                    child = add_candidate(
                        condition_indexes,
                        minimum_discovery_samples=required_discovery,
                        hierarchical=True,
                    )
                    if child is None:
                        continue
                    hierarchical_diagnostics["quint_candidates_tested"] += 1
                    if finish_hierarchical_child(child, parent):
                        hierarchical_diagnostics["quint_candidates_passed_gain"] += 1
                if (
                    quint_attempts >= max(0, int(config.max_quint_candidates))
                    or evaluated >= config.max_candidates_evaluated
                ):
                    break
            hierarchical_diagnostics["quint_candidates_attempted"] = quint_attempts

    # Correct across every unique candidate inspected, not only the shortlist
    # later persisted in the registry. The complete hypothesis family is now
    # frozen using discovery-only evidence before the outer holdout is touched.
    probability_p_values = [
        candidate["discovery_metrics"].get("one_sided_p_value")
        for candidate in candidates
    ]
    asymmetry_p_values = [
        candidate["discovery_metrics"].get("asymmetry_one_sided_p_value")
        for candidate in candidates
    ]
    # The acceptance decision is the union of two routes.  Correct both route
    # hypotheses in one family so an OR across separately corrected families
    # cannot inflate the configured false-discovery rate.
    joint_q_values = _bh_q_values(probability_p_values + asymmetry_p_values)
    probability_q_values = joint_q_values[: len(candidates)]
    asymmetry_q_values = joint_q_values[len(candidates) :]
    for index, candidate in enumerate(candidates):
        candidate["discovery_q_value"] = probability_q_values[index]
        candidate["discovery_asymmetry_q_value"] = asymmetry_q_values[index]
    hypothesis_payload = [
        {
            "conditions": candidate["conditions"],
            "discovery_q_value": candidate.get("discovery_q_value"),
            "discovery_asymmetry_q_value": candidate.get(
                "discovery_asymmetry_q_value"
            ),
        }
        for candidate in candidates
    ]
    hierarchical_diagnostics["hypothesis_family_fingerprint"] = hashlib.sha256(
        json.dumps(
            hypothesis_payload,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    eligible_candidates = sorted(
        (candidate for candidate in candidates if candidate["eligible_for_output"]),
        key=lambda item: item["preliminary_score"],
        reverse=True,
    )
    finalists = eligible_candidates[: max(config.max_formulas_returned * 4, 120)]
    for candidate in finalists:
        attach_discovery_evidence(candidate)
        attach_selection(candidate)

    # Freeze formula identity, rank and evidence family using Fit+Selection
    # only.  The final Test partition is still unread at this point.
    development_results: list[Dict[str, Any]] = []
    for candidate in finalists:
        q_value = _two_direction_union_q(candidate.get("discovery_q_value"))
        asymmetry_q_value = _two_direction_union_q(
            candidate.get("discovery_asymmetry_q_value")
        )
        conditions = candidate["conditions"]
        discovery = candidate["discovery_metrics"]
        selection = candidate["selection_metrics"]
        score = _final_score(
            discovery,
            selection,
            horizon_minutes=horizon_minutes,
            q_value=q_value,
            asymmetry_q_value=asymmetry_q_value,
            complexity=len(conditions),
        )
        key = formula_key(
            direction=direction,
            horizon_minutes=horizon_minutes,
            feature_schema_version=feature_schema_version,
            conditions=conditions,
            condition_family_exceptions=config.condition_family_exceptions,
        )
        discovery_output = dict(discovery)
        discovery_output["selection_metrics"] = selection
        discovery_output["walk_forward_validation"] = candidate.get(
            "walk_forward_validation"
        )
        development_results.append(
            {
                "formula_key": key,
                "formula_version": 1,
                "formula_schema_version": FORMULA_SCHEMA_VERSION,
                "engine_version": ENGINE_VERSION,
                "feature_schema_version": feature_schema_version,
                "direction": direction,
                "horizon_minutes": horizon_minutes,
                "conditions": conditions,
                "condition_count": len(conditions),
                "formula_text": _formula_text(direction, conditions),
                "discovery_metrics": discovery_output,
                "selection_metrics": selection,
                # Formula-family priority accepts this established field.  It
                # is replaced with untouched Test metrics only after grouping.
                "holdout_metrics": selection,
                "multiple_testing": {
                    "method": (
                        "per-direction joint probability+asymmetry BH, then "
                        "x2 LONG/SHORT union correction; Fit for development "
                        "and frozen Test champions for acceptance"
                    ),
                    "discovery_one_sided_p_value": discovery.get("one_sided_p_value"),
                    "development_q_value": _round(q_value, 8),
                    "development_asymmetry_q_value": _round(
                        asymmetry_q_value, 8
                    ),
                    "hypotheses_tested_per_route": len(candidates),
                    "total_route_hypotheses_tested": len(candidates) * 2,
                    "hypothesis_routes": ["PROBABILITY", "ASYMMETRY"],
                    "candidate_combinations_evaluated": evaluated,
                    "condition_families": list(candidate["condition_families"]),
                    "condition_family_policy": {
                        "policy_version": (
                            research_formula_families.CONDITION_FAMILY_POLICY_VERSION
                        ),
                        "enforcement": "ALL_CONDITION_DEPTHS",
                        "families": list(candidate["condition_families"]),
                        "justified_exceptions": list(
                            config.condition_family_exceptions
                        ),
                    },
                    "hierarchical_validation": candidate.get(
                        "hierarchical_validation"
                    ),
                },
                "ranking_score": score,
                "ranking_basis": (
                    "Fit plus expanding-refit Walk-forward Selection only; "
                    "final Test excluded"
                ),
                "recommended_stage": "BACKTESTED",
                "gate_notes": ["identity and rank frozen before final Test"],
                "live_alert_approved": False,
                "hierarchical_validation": candidate.get("hierarchical_validation"),
                "_candidate_ref": candidate,
                "_evidence_keys": tuple(
                    sorted(
                        set(candidate.get("discovery_evidence_keys") or ())
                        | set(candidate.get("selection_evidence_keys") or ())
                    )
                ),
                "_evidence_intervals": tuple(
                    sorted(
                        set(candidate.get("discovery_evidence_intervals") or ())
                        | set(candidate.get("selection_evidence_intervals") or ())
                    )
                ),
            }
        )
    development_results.sort(
        key=lambda item: (
            item["ranking_score"],
            item["selection_metrics"].get("sample_size") or 0,
        ),
        reverse=True,
    )
    candidates_by_key: Dict[str, Dict[str, Any]] = {}
    unique_development_results: list[Dict[str, Any]] = []
    for item in development_results:
        key = str(item["formula_key"])
        if key in candidates_by_key:
            continue
        candidate = item.pop("_candidate_ref")
        candidates_by_key[key] = candidate
        unique_development_results.append(item)
    development_results = unique_development_results
    grouped = research_formula_families.group_formula_evidence(
        development_results,
        overlap_threshold=config.evidence_family_overlap_threshold,
    )
    results = grouped["champions"][: config.max_formulas_returned]
    for rank, result in enumerate(results, start=1):
        result["rank"] = rank
    for result in results:
        attach_holdout(candidates_by_key[result["formula_key"]])

    test_probability_p_values = [
        candidates_by_key[result["formula_key"]]["holdout_metrics"].get(
            "one_sided_p_value"
        )
        for result in results
    ]
    test_asymmetry_p_values = [
        candidates_by_key[result["formula_key"]]["holdout_metrics"].get(
            "asymmetry_one_sided_p_value"
        )
        for result in results
    ]
    test_joint_q = _bh_q_values(
        test_probability_p_values + test_asymmetry_p_values
    )
    test_probability_q = [
        _two_direction_union_q(value) for value in test_joint_q[: len(results)]
    ]
    test_asymmetry_q = [
        _two_direction_union_q(value) for value in test_joint_q[len(results) :]
    ]
    for index, result in enumerate(results):
        candidate = candidates_by_key[result["formula_key"]]
        discovery = candidate["discovery_metrics"]
        holdout = candidate["holdout_metrics"]
        stage, gate_reasons = _recommended_stage(
            discovery,
            holdout,
            horizon_minutes=horizon_minutes,
            q_value=test_probability_q[index],
            config=config,
            asymmetry_q_value=test_asymmetry_q[index],
            complexity=len(candidate["conditions"]),
            walk_forward=candidate.get("walk_forward_validation"),
        )
        result["holdout_metrics"] = holdout
        result["recommended_stage"] = stage
        result["gate_notes"] = gate_reasons
        multiple_testing = dict(result.get("multiple_testing") or {})
        multiple_testing.update(
            {
                "test_q_value": _round(test_probability_q[index], 8),
                "test_asymmetry_q_value": _round(
                    test_asymmetry_q[index], 8
                ),
                # Compatibility names now unambiguously refer to the untouched
                # final Test used for historical research acceptance.
                "q_value": _round(test_probability_q[index], 8),
                "asymmetry_q_value": _round(test_asymmetry_q[index], 8),
                "test_champions_per_route": len(results),
                "test_total_route_hypotheses": len(results) * 2,
                "test_influenced_identity_or_rank": False,
            }
        )
        result["multiple_testing"] = multiple_testing
    return {
        "direction": direction,
        "discovery_rows": len(discovery_rows),
        "selection_rows": len(selection_rows),
        "holdout_rows": len(holdout_rows),
        "predicate_count": len(usable_predicates),
        "candidates_evaluated": evaluated,
        "statistical_hypotheses_tested": len(candidates),
        "unique_candidate_observation_sets": len(dedup_observations),
        "family_policy_rejections": family_policy_rejections,
        "condition_family_policy_version": (
            research_formula_families.CONDITION_FAMILY_POLICY_VERSION
        ),
        "insufficient_sample_rejections": insufficient_sample_rejections,
        "correlated_sample_rejections": correlated_sample_rejections,
        "hierarchical_search": hierarchical_diagnostics,
        "evidence_family_grouping": {
            key: value
            for key, value in grouped.items()
            if key not in {"champions"}
        },
        "formulas": results,
    }


def discover_formulas(
    rows: Sequence[Mapping[str, Any]],
    *,
    horizon_minutes: int,
    feature_schema_version: str,
    config: Optional[DiscoveryConfig] = None,
    analysis_as_of_utc: Any = None,
) -> Dict[str, Any]:
    """Search formulas using an earlier discovery and later holdout period."""
    active_config = config or DiscoveryConfig()
    analysis_as_of = _utc(analysis_as_of_utc) if analysis_as_of_utc is not None else None
    ordered = sorted(
        [
            row
            for row in rows
            if str(row.get("event", {}).get("direction") or "").upper()
            in {"LONG", "SHORT"}
            and (
                analysis_as_of is None
                or research_market_episode.row_time_utc(row) <= analysis_as_of
            )
        ],
        key=lambda row: (
            research_market_episode.row_time_utc(row),
            int(row["event"].get("event_id") or 0),
        ),
    )
    if len(ordered) < 2:
        return {
            "available": False,
            "reason": "at least two chronological decision rows are required",
            "sample_size": len(ordered),
            "formulas": [],
        }
    distinct_times = sorted(
        {research_market_episode.row_time_utc(row) for row in ordered}
    )
    if len(distinct_times) < 2:
        return {
            "available": False,
            "reason": "at least two distinct chronological observation times are required",
            "sample_size": len(ordered),
            "formulas": [],
        }
    test_index = int(math.floor(len(ordered) * active_config.discovery_fraction))
    test_index = max(1, min(len(ordered) - 1, test_index))
    holdout_start = research_market_episode.row_time_utc(ordered[test_index])
    if holdout_start == distinct_times[0]:
        holdout_start = distinct_times[1]
    purge_minutes = research_market_episode.episode_minutes(horizon_minutes)
    embargo_minutes = int(horizon_minutes)
    boundary_gap = timedelta(minutes=purge_minutes + embargo_minutes)
    development_period = [
        row
        for row in ordered
        if research_market_episode.row_time_utc(row) < holdout_start - boundary_gap
    ]
    holdout_period = [
        row
        for row in ordered
        if research_market_episode.row_time_utc(row) >= holdout_start
    ]
    if len(development_period) < 2:
        return {
            "available": False,
            "reason": "insufficient purged/embargoed Fit+Selection development rows",
            "sample_size": len(ordered),
            "formulas": [],
        }
    fit_fraction = min(
        0.95, max(0.05, float(active_config.fit_fraction_within_development))
    )
    selection_index = int(math.floor(len(development_period) * fit_fraction))
    selection_index = max(1, min(len(development_period) - 1, selection_index))
    development_times = sorted(
        {research_market_episode.row_time_utc(row) for row in development_period}
    )
    selection_start = research_market_episode.row_time_utc(
        development_period[selection_index]
    )
    if selection_start == development_times[0] and len(development_times) >= 2:
        selection_start = development_times[1]
    discovery_period = [
        row
        for row in development_period
        if research_market_episode.row_time_utc(row)
        < selection_start - boundary_gap
    ]
    selection_period = [
        row
        for row in development_period
        if research_market_episode.row_time_utc(row) >= selection_start
    ]
    fit_boundary_end = selection_start - boundary_gap
    selection_boundary_end = holdout_start - boundary_gap
    discovery_as_of = (
        fit_boundary_end
        if discovery_period
        else None
    )
    holdout_as_of = (
        analysis_as_of
        or max(research_market_episode.row_time_utc(row) for row in holdout_period)
        if holdout_period
        else None
    )
    selection_as_of = (
        selection_boundary_end
        if selection_period
        else None
    )
    direction_results = []
    formulas = []
    for direction in ("LONG", "SHORT"):
        earlier = [
            row
            for row in discovery_period
            if str(row["event"].get("direction") or "").upper() == direction
        ]
        later = [
            row
            for row in selection_period
            if str(row["event"].get("direction") or "").upper() == direction
        ]
        test = [
            row
            for row in holdout_period
            if str(row["event"].get("direction") or "").upper() == direction
        ]
        if len(earlier) < active_config.min_discovery_samples:
            direction_results.append(
                {
                    "direction": direction,
                    "discovery_rows": len(earlier),
                    "selection_rows": len(later),
                    "holdout_rows": len(test),
                    "candidates_evaluated": 0,
                    "reason": "insufficient discovery sample",
                    "formulas": [],
                }
            )
            continue
        result = _search_direction(
            earlier,
            later,
            test,
            direction=direction,
            horizon_minutes=int(horizon_minutes),
            feature_schema_version=feature_schema_version,
            config=active_config,
            discovery_as_of_utc=discovery_as_of,
            selection_as_of_utc=selection_as_of,
            holdout_as_of_utc=holdout_as_of,
        )
        direction_results.append(result)
        formulas.extend(result["formulas"])

    formulas.sort(key=lambda item: item["ranking_score"], reverse=True)
    return _json_safe(
        {
            "available": True,
            "engine_version": ENGINE_VERSION,
            "formula_schema_version": FORMULA_SCHEMA_VERSION,
            "feature_schema_version": feature_schema_version,
            "horizon_minutes": int(horizon_minutes),
            "config": asdict(active_config),
            "analysis_as_of_utc": analysis_as_of,
            "sample_size": len(ordered),
            "discovery_sample_size": len(discovery_period),
            "selection_sample_size": len(selection_period),
            "holdout_sample_size": len(holdout_period),
            "first_alert_time_utc": ordered[0]["event"]["alert_time_utc"],
            "selection_start_time_utc": (
                selection_period[0]["event"]["alert_time_utc"]
                if selection_period
                else None
            ),
            "holdout_start_time_utc": (
                holdout_period[0]["event"]["alert_time_utc"]
                if holdout_period
                else None
            ),
            "discovery_evidence_as_of_utc": discovery_as_of,
            "selection_evidence_as_of_utc": selection_as_of,
            "holdout_evidence_as_of_utc": holdout_as_of,
            "walk_forward_policy_version": WALK_FORWARD_POLICY_VERSION,
            "condition_family_policy_version": (
                research_formula_families.CONDITION_FAMILY_POLICY_VERSION
            ),
            "purge_policy_version": PURGE_POLICY_VERSION,
            "embargo_policy_version": EMBARGO_POLICY_VERSION,
            "purge_minutes": purge_minutes,
            "embargo_minutes": embargo_minutes,
            "boundary_gap_minutes": purge_minutes + embargo_minutes,
            # Compatibility field retained for older read-only consumers.
            "split_purge_minutes": purge_minutes + embargo_minutes,
            "last_alert_time_utc": ordered[-1]["event"]["alert_time_utc"],
            "split_policy": (
                "approximately 49% initial Fit, 21% expanding-refit "
                "Walk-forward Selection and 30% untouched Test; a versioned "
                "Market-Episode purge plus full outcome-horizon embargo "
                "separates boundaries; identical timestamps never split; Test "
                "can change acceptance only, never formula identity or rank"
            ),
            "directions": direction_results,
            "candidates_evaluated": sum(
                int(result.get("candidates_evaluated") or 0) for result in direction_results
            ),
            "formulas": formulas,
            "automatic_stage_ceiling": "SHADOW",
            "live_activation": (
                "only after separate deterministic future-Shadow validation "
                "under the owner-approved policy"
            ),
        }
    )
