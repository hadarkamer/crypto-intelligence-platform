"""Deterministic automatic formula discovery for archived bot alerts.

The engine searches decision-time features only.  Binance Spot path outcomes
are labels and are never exposed to a condition.  Thresholds are learned from
the earlier chronological discovery partition, frozen, and then evaluated on
the later holdout partition.

This module is pure computation: it does not write PostgreSQL, send Telegram
messages, or alter any production score/threshold.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
from statistics import mean, median
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence


ENGINE_VERSION = "formula-discovery-v1"
FORMULA_SCHEMA_VERSION = "research-formula-v1"
ALLOWED_OPERATORS = {">=", "<=", "=="}


@dataclass(frozen=True)
class DiscoveryConfig:
    discovery_fraction: float = 0.70
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


def _round(value: Any, digits: int = 6) -> Optional[float]:
    number = _number(value)
    return round(number, digits) if number is not None else None


def _json_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str, ensure_ascii=False))


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
    candidate_successes: int,
    candidate_total: int,
    control_successes: int,
    control_total: int,
) -> Optional[float]:
    if candidate_total <= 0 or control_total <= 0:
        return None
    candidate_rate = candidate_successes / candidate_total
    control_rate = control_successes / control_total
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
    total = len(indexed)
    adjusted: Dict[int, float] = {}
    running = 1.0
    for reverse_rank, (index, value) in enumerate(reversed(indexed), start=1):
        rank = total - reverse_rank + 1
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
        return actual == expected
    actual_number = _number(actual)
    expected_number = _number(expected)
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
    *, direction: str, horizon_minutes: int, feature_schema_version: str, conditions: Sequence[Mapping[str, Any]]
) -> str:
    canonical = {
        "formula_schema_version": FORMULA_SCHEMA_VERSION,
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


def _metrics(
    selected: Sequence[Mapping[str, Any]],
    universe: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    selected_ids = {
        int(row.get("event", {}).get("event_id"))
        for row in selected
        if row.get("event", {}).get("event_id") is not None
    }
    controls = [
        row
        for row in universe
        if int(row.get("event", {}).get("event_id") or -1) not in selected_ids
    ]
    directional = _outcome_values(selected, "directional_return_pct")
    control_directional = _outcome_values(controls, "directional_return_pct")
    mfe = _outcome_values(selected, "mfe_pct")
    mae = _outcome_values(selected, "mae_pct")
    first_progress = _outcome_values(selected, "time_to_first_progress_seconds")
    time_to_mfe = _outcome_values(selected, "time_to_mfe_seconds")
    target_progress = _outcome_values(selected, "target_progress_ratio")
    target_flags = [
        bool(row.get("outcome_label", {}).get("target_reached"))
        for row in selected
        if row.get("outcome_label", {}).get("target_reached") is not None
    ]
    successes = sum(1 for value in directional if value > 0)
    control_successes = sum(1 for value in control_directional if value > 0)
    hit_rate = successes / len(directional) * 100.0 if directional else None
    control_hit_rate = (
        control_successes / len(control_directional) * 100.0
        if control_directional
        else None
    )
    median_mae = median(mae) if mae else None
    median_mfe = median(mfe) if mfe else None
    efficiency = (
        median_mfe / median_mae
        if median_mfe is not None and median_mae not in (None, 0.0)
        else None
    )
    sample_share = len(selected) / len(universe) * 100.0 if universe else 0.0
    rarity_class = "RARE" if sample_share <= 5.0 else "UNCOMMON" if sample_share <= 15.0 else "COMMON"
    return {
        "sample_size": len(selected),
        "universe_size": len(universe),
        "sample_share_pct": round(sample_share, 4),
        "rarity_class": rarity_class,
        "successes": successes,
        "hit_rate_pct": _round(hit_rate, 4),
        "wilson_95_lower_pct": _round(
            (_wilson_lower(successes, len(directional)) or 0.0) * 100.0, 4
        ) if directional else None,
        "avg_directional_return_pct": _round(mean(directional), 6) if directional else None,
        "median_directional_return_pct": _round(median(directional), 6) if directional else None,
        "median_mfe_pct": _round(median_mfe, 6),
        "median_mae_pct": _round(median_mae, 6),
        "mae_p75_pct": _round(_quantile(mae, 0.75), 6),
        "mae_p90_pct": _round(_quantile(mae, 0.90), 6),
        "mae_p95_pct": _round(_quantile(mae, 0.95), 6),
        "median_mfe_mae_ratio": _round(efficiency, 6),
        "median_time_to_first_progress_seconds": _round(median(first_progress), 2) if first_progress else None,
        "median_time_to_mfe_seconds": _round(median(time_to_mfe), 2) if time_to_mfe else None,
        "avg_target_progress_ratio": _round(mean(target_progress), 6) if target_progress else None,
        "target_reached_rate_pct": _round(sum(target_flags) / len(target_flags) * 100.0, 4) if target_flags else None,
        "control_sample_size": len(controls),
        "control_hit_rate_pct": _round(control_hit_rate, 4),
        "hit_rate_improvement_pct_points": _round(
            hit_rate - control_hit_rate,
            4,
        ) if hit_rate is not None and control_hit_rate is not None else None,
        "one_sided_p_value": _round(
            _one_sided_two_proportion_p(
                successes,
                len(directional),
                control_successes,
                len(control_directional),
            ),
            8,
        ),
        "sample_event_ids": sorted(selected_ids)[:20],
    }


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
        numeric = [number for value in values if (number := _number(value)) is not None]
        non_missing_min = min(config.min_discovery_samples, max(3, len(rows) // 5))
        if len(numeric) >= non_missing_min and len(set(round(value, 10) for value in numeric)) > 1:
            for fraction in config.numeric_quantiles:
                threshold = _quantile(numeric, fraction)
                if threshold is None:
                    continue
                threshold = round(threshold, 8)
                for operator in (">=", "<="):
                    condition = {"feature": feature, "operator": operator, "value": threshold}
                    key = json.dumps(condition, sort_keys=True, ensure_ascii=False)
                    predicates[key] = condition
            continue

        counts: Dict[Any, int] = {}
        for value in values:
            if isinstance(value, (str, bool)):
                counts[value] = counts.get(value, 0) + 1
        for value, count in sorted(counts.items(), key=lambda item: (-item[1], str(item[0])))[:16]:
            if count < non_missing_min or count >= len(rows):
                continue
            condition = {"feature": feature, "operator": "==", "value": value}
            key = json.dumps(condition, sort_keys=True, ensure_ascii=False)
            predicates[key] = condition
    return list(predicates.values())


def _preliminary_score(metrics: Mapping[str, Any], complexity: int) -> float:
    hit = _number(metrics.get("hit_rate_pct")) or 0.0
    lower = _number(metrics.get("wilson_95_lower_pct")) or 0.0
    improvement = _number(metrics.get("hit_rate_improvement_pct_points")) or 0.0
    mfe = _number(metrics.get("median_mfe_pct")) or 0.0
    mae = _number(metrics.get("median_mae_pct")) or 0.0
    efficiency = mfe / max(0.05, mae)
    sample = int(metrics.get("sample_size") or 0)
    return (
        0.30 * lower
        + 0.20 * hit
        + 0.20 * max(-20.0, improvement)
        + 8.0 * min(4.0, efficiency)
        + 4.0 * math.log1p(sample)
        - 2.0 * max(0, complexity - 1)
    )


def _final_score(
    discovery: Mapping[str, Any],
    holdout: Mapping[str, Any],
    *,
    horizon_minutes: int,
    q_value: Optional[float],
    complexity: int,
) -> float:
    evidence = holdout if int(holdout.get("sample_size") or 0) else discovery
    hit = (_number(evidence.get("hit_rate_pct")) or 0.0) / 100.0
    lower = (_number(evidence.get("wilson_95_lower_pct")) or 0.0) / 100.0
    improvement = (_number(evidence.get("hit_rate_improvement_pct_points")) or 0.0)
    improvement_quality = min(1.0, max(0.0, (improvement + 5.0) / 25.0))
    mae = max(0.0, _number(evidence.get("median_mae_pct")) or 0.0)
    mae_quality = 1.0 / (1.0 + mae)
    efficiency = max(0.0, _number(evidence.get("median_mfe_mae_ratio")) or 0.0)
    efficiency_quality = min(1.0, efficiency / 3.0)
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
    discovery_hit = _number(discovery.get("hit_rate_pct")) or 0.0
    holdout_hit = _number(holdout.get("hit_rate_pct"))
    stability = (
        max(0.0, 1.0 - abs(discovery_hit - holdout_hit) / 40.0)
        if holdout_hit is not None
        else 0.0
    )
    significance = 0.0 if q_value is None else max(0.0, 1.0 - q_value)
    score = 100.0 * (
        0.24 * lower
        + 0.13 * hit
        + 0.12 * improvement_quality
        + 0.14 * mae_quality
        + 0.12 * efficiency_quality
        + 0.08 * speed_quality
        + 0.08 * sample_quality
        + 0.05 * stability
        + 0.02 * significance
        + 0.02 * rarity_quality
    )
    score -= 1.5 * max(0, complexity - 1)
    return round(max(0.0, min(100.0, score)), 4)


def _recommended_stage(
    discovery: Mapping[str, Any],
    holdout: Mapping[str, Any],
    *,
    q_value: Optional[float],
    config: DiscoveryConfig,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    discovery_n = int(discovery.get("sample_size") or 0)
    holdout_n = int(holdout.get("sample_size") or 0)
    if holdout_n < config.min_holdout_samples:
        return "DISCOVERED", ["insufficient chronological holdout sample"]
    stage = "BACKTESTED"
    holdout_hit = _number(holdout.get("hit_rate_pct")) or 0.0
    holdout_lower = _number(holdout.get("wilson_95_lower_pct")) or 0.0
    improvement = _number(holdout.get("hit_rate_improvement_pct_points")) or -100.0
    efficiency = _number(holdout.get("median_mfe_mae_ratio")) or 0.0
    discovery_hit = _number(discovery.get("hit_rate_pct")) or 0.0
    strict_checks = {
        "discovery sample": discovery_n >= config.strict_discovery_samples,
        "holdout sample": holdout_n >= config.strict_holdout_samples,
        "holdout hit rate": holdout_hit >= 60.0,
        "holdout Wilson lower bound": holdout_lower >= 45.0,
        "holdout improvement": improvement >= 5.0,
        "MFE/MAE efficiency": efficiency >= 1.25,
        "discovery/holdout stability": abs(discovery_hit - holdout_hit) <= 20.0,
        "multiple-testing q-value": q_value is not None and q_value <= 0.20,
    }
    reasons.extend(name for name, passed in strict_checks.items() if not passed)
    if not reasons:
        # SHADOW is observational only. APPROVED and LIVE are never automatic.
        return "SHADOW", ["strict chronological holdout gates passed"]
    return stage, reasons


def _search_direction(
    discovery_rows: Sequence[Mapping[str, Any]],
    holdout_rows: Sequence[Mapping[str, Any]],
    *,
    direction: str,
    horizon_minutes: int,
    feature_schema_version: str,
    config: DiscoveryConfig,
) -> Dict[str, Any]:
    discovery_features = [extract_decision_features(row) for row in discovery_rows]
    holdout_features = [extract_decision_features(row) for row in holdout_rows]
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
    dedup_observations: set[tuple[int, ...]] = set()
    candidates: list[Dict[str, Any]] = []

    def add_candidate(condition_indexes: Sequence[int]) -> Optional[Dict[str, Any]]:
        nonlocal evaluated
        if evaluated >= config.max_candidates_evaluated:
            return None
        evaluated += 1
        matched = set(range(len(discovery_rows)))
        for index in condition_indexes:
            matched &= predicate_matches[index]
        if len(matched) < config.min_discovery_samples:
            return None
        observation_key = tuple(sorted(matched))
        if observation_key in dedup_observations:
            return None
        dedup_observations.add(observation_key)
        conditions = _canonical_conditions([usable_predicates[index] for index in condition_indexes])
        selected = [discovery_rows[index] for index in sorted(matched)]
        metrics = _metrics(selected, discovery_rows)
        candidate = {
            "condition_indexes": tuple(condition_indexes),
            "conditions": conditions,
            "discovery_metrics": metrics,
            "preliminary_score": _preliminary_score(metrics, len(conditions)),
        }
        candidates.append(candidate)
        return candidate

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
            triple_count += 1
            add_candidate((*pair["condition_indexes"], index))
        if triple_count >= config.max_triple_candidates or evaluated >= config.max_candidates_evaluated:
            break

    candidates.sort(key=lambda item: item["preliminary_score"], reverse=True)
    # Correct across every unique candidate inspected, not only the shortlist
    # later persisted in the registry.
    all_q_values = _bh_q_values(
        [candidate["discovery_metrics"].get("one_sided_p_value") for candidate in candidates]
    )
    for candidate, q_value in zip(candidates, all_q_values):
        candidate["discovery_q_value"] = q_value
    finalists = candidates[: max(config.max_formulas_returned * 4, 120)]
    for candidate in finalists:
        selected_holdout = [
            row
            for row, features in zip(holdout_rows, holdout_features)
            if all(condition_matches(features, condition) for condition in candidate["conditions"])
        ]
        candidate["holdout_metrics"] = _metrics(selected_holdout, holdout_rows)

    results: list[Dict[str, Any]] = []
    for candidate in finalists:
        q_value = candidate.get("discovery_q_value")
        conditions = candidate["conditions"]
        discovery = candidate["discovery_metrics"]
        holdout = candidate["holdout_metrics"]
        stage, gate_reasons = _recommended_stage(
            discovery,
            holdout,
            q_value=q_value,
            config=config,
        )
        score = _final_score(
            discovery,
            holdout,
            horizon_minutes=horizon_minutes,
            q_value=q_value,
            complexity=len(conditions),
        )
        key = formula_key(
            direction=direction,
            horizon_minutes=horizon_minutes,
            feature_schema_version=feature_schema_version,
            conditions=conditions,
        )
        results.append(
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
                "discovery_metrics": discovery,
                "holdout_metrics": holdout,
                "multiple_testing": {
                    "method": "Benjamini-Hochberg",
                    "discovery_one_sided_p_value": discovery.get("one_sided_p_value"),
                    "q_value": _round(q_value, 8),
                },
                "ranking_score": score,
                "recommended_stage": stage,
                "gate_notes": gate_reasons,
                "live_alert_approved": False,
            }
        )
    results.sort(
        key=lambda item: (
            item["recommended_stage"] == "SHADOW",
            item["ranking_score"],
            item["holdout_metrics"].get("sample_size") or 0,
        ),
        reverse=True,
    )
    results = results[: config.max_formulas_returned]
    for rank, result in enumerate(results, start=1):
        result["rank"] = rank
    return {
        "direction": direction,
        "discovery_rows": len(discovery_rows),
        "holdout_rows": len(holdout_rows),
        "predicate_count": len(usable_predicates),
        "candidates_evaluated": evaluated,
        "unique_candidate_observation_sets": len(dedup_observations),
        "formulas": results,
    }


def discover_formulas(
    rows: Sequence[Mapping[str, Any]],
    *,
    horizon_minutes: int,
    feature_schema_version: str,
    config: Optional[DiscoveryConfig] = None,
) -> Dict[str, Any]:
    """Search formulas using an earlier discovery and later holdout period."""
    active_config = config or DiscoveryConfig()
    ordered = sorted(
        [
            row
            for row in rows
            if str(row.get("event", {}).get("direction") or "").upper()
            in {"LONG", "SHORT"}
            and _number(row.get("outcome_label", {}).get("directional_return_pct"))
            is not None
        ],
        key=lambda row: (
            _utc(row["event"]["alert_time_utc"]),
            int(row["event"].get("event_id") or 0),
        ),
    )
    if len(ordered) < 2:
        return {
            "available": False,
            "reason": "at least two verified chronological rows are required",
            "sample_size": len(ordered),
            "formulas": [],
        }
    split_index = int(math.floor(len(ordered) * active_config.discovery_fraction))
    split_index = max(1, min(len(ordered) - 1, split_index))
    discovery_period = ordered[:split_index]
    holdout_period = ordered[split_index:]
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
            for row in holdout_period
            if str(row["event"].get("direction") or "").upper() == direction
        ]
        if len(earlier) < active_config.min_discovery_samples:
            direction_results.append(
                {
                    "direction": direction,
                    "discovery_rows": len(earlier),
                    "holdout_rows": len(later),
                    "candidates_evaluated": 0,
                    "reason": "insufficient discovery sample",
                    "formulas": [],
                }
            )
            continue
        result = _search_direction(
            earlier,
            later,
            direction=direction,
            horizon_minutes=int(horizon_minutes),
            feature_schema_version=feature_schema_version,
            config=active_config,
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
            "sample_size": len(ordered),
            "discovery_sample_size": len(discovery_period),
            "holdout_sample_size": len(holdout_period),
            "first_alert_time_utc": ordered[0]["event"]["alert_time_utc"],
            "holdout_start_time_utc": holdout_period[0]["event"]["alert_time_utc"],
            "last_alert_time_utc": ordered[-1]["event"]["alert_time_utc"],
            "split_policy": "earliest 70% discovery; latest 30% frozen chronological holdout",
            "directions": direction_results,
            "candidates_evaluated": sum(
                int(result.get("candidates_evaluated") or 0) for result in direction_results
            ),
            "formulas": formulas,
            "automatic_stage_ceiling": "SHADOW",
            "live_activation": "never automatic",
        }
    )
