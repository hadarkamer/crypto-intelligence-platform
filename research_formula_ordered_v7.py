"""Auditable, read-only formula evidence from ordered First Touch v7.

This is deliberately separate from the retained one-sided native discovery
pipeline.  Formula predicates are frozen before labels are inspected; their
evidence unit is an independently assigned BTC parent movement, shared across
coins, alert times, directions, thresholds and horizons.  Count eligibility is
not statistical acceptance and never authorizes a Telegram delivery.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from statistics import median
from typing import Any, Mapping, Sequence

import research_ordered_first_touch


METHOD_VERSION = research_ordered_first_touch.METHOD_VERSION
POLICY_VERSION = "formula-evidence-v1-ordered-v7-btc-parent-5-or-fresh3"
THRESHOLDS_BPS = tuple(range(25, 201, 25))
HORIZONS_MINUTES = (60, 240, 720, 1440)
DIRECTIONS = ("LONG", "SHORT")
FRESH_DAYS = 14
CATALOG_VERSION = "ordered-v7-total-score-simple-pairs-strict65-v1"
METRIC_SCOPE = "STOP_AT_FIRST_TOUCH_V7"
STATUS_REPORTING_VERSION = "ordered-v7-wave-status-reporting-v2"
WAVE_STATUSES = ("SUCCESS", "FAILURE", "OPEN", "AMBIGUOUS", "NO_TOUCH", "DATA_MISSING")
VERIFIED_DATA_QUALITY = (
    "VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES",
    "VERIFIED_HYPERLIQUID_SPOT_1M_CLOSED_CANDLES",
)


def _utc(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )
    if parsed.tzinfo is None:
        raise ValueError("ordered v7 evidence timestamps must include a timezone")
    return parsed.astimezone(timezone.utc)


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _outcome(row: Mapping[str, Any]) -> Mapping[str, Any]:
    return _mapping(row.get("ordered_outcome")) or row


def _event(row: Mapping[str, Any]) -> Mapping[str, Any]:
    return _mapping(row.get("event")) or row


def _field(row: Mapping[str, Any], name: str) -> Any:
    return row.get(name, _event(row).get(name))


def _decision_time(row: Mapping[str, Any]) -> datetime:
    # This is the immutable alert/forecast timestamp, not the later price
    # barrier decision_time_utc field in the ordered outcome.
    event = _event(row)
    return _utc(
        event.get("forecast_start_time_utc")
        or event.get("alert_time_utc")
        or row.get("alert_time_utc")
        or _outcome(row).get("measurement_start_utc")
    )


def ordered_outcome_evidence(
    row: Mapping[str, Any], *, analysis_as_of_utc: Any
) -> dict[str, Any]:
    """Validate one label without guessing missing audit fields or versions."""

    label = _outcome(row)
    reasons: list[str] = []
    as_of = _utc(analysis_as_of_utc)
    if label.get("method_version") != METHOD_VERSION:
        reasons.append("OUTCOME_METHOD_MISMATCH")
    expected_event_id=row.get('outcome_event_id',_field(row,'event_id'))
    if type(expected_event_id) is not int or expected_event_id<=0 or label.get('event_id')!=expected_event_id:
        reasons.append('OUTCOME_EVENT_ID_MISMATCH')
    direction = label.get("direction")
    if direction not in DIRECTIONS or _field(row, "direction") != direction:
        reasons.append("DIRECTION_MISMATCH")
    horizon = label.get("window_minutes", label.get("horizon_minutes"))
    threshold = label.get("threshold_bps")
    if type(horizon) is not int or horizon not in HORIZONS_MINUTES:
        reasons.append("UNSUPPORTED_HORIZON")
    if type(threshold) is not int or threshold not in THRESHOLDS_BPS:
        reasons.append("UNSUPPORTED_THRESHOLD")

    status = label.get("status")
    reported_status = status
    expected = {
        "SUCCESS": (True, "FAVORABLE", "FAVORABLE_FIRST", "favorable_touch_price"),
        "FAILURE": (False, "ADVERSE", "ADVERSE_FIRST", "adverse_touch_price"),
    }.get(status)
    if expected is None:
        if status == "OPEN":
            valid_state = (
                label.get("first_touch_side") == "NONE"
                and label.get("terminal_reason") is None
                and label.get("observation_closed") is False
            )
        elif status == "UNRESOLVED" and label.get("terminal_reason") == "SAME_CANDLE_BOTH":
            reported_status = "AMBIGUOUS"
            valid_state = label.get("first_touch_side") == "AMBIGUOUS"
        elif status == "UNRESOLVED" and label.get("terminal_reason") == "OBSERVATION_WINDOW_CLOSED_NO_TOUCH":
            reported_status = "NO_TOUCH"
            valid_state = (
                label.get("first_touch_side") == "NONE"
                and label.get("observation_closed") is True
            )
        elif status == "DATA_MISSING":
            valid_state = (
                label.get("first_touch_side") == "NONE"
                and label.get("terminal_reason") == "INCOMPLETE_PATH"
            )
        else:
            valid_state = False
        if not valid_state or label.get("success") is not None:
            reasons.append("INVALID_NONDECISIVE_OUTCOME_STATE")
        if reported_status in ("OPEN", "NO_TOUCH", "DATA_MISSING") and any(
            label.get(key) is not None for key in (
                "decision_time_utc", "time_to_decision_seconds",
                "favorable_touch_price", "adverse_touch_price",
            )
        ):
            reasons.append("NONDECISIVE_OUTCOME_HAS_TOUCH")
    else:
        success, side, terminal, touch_key = expected
        if label.get("success") is not success:
            reasons.append("SUCCESS_FLAG_MISMATCH")
        if label.get("first_touch_side") != side:
            reasons.append("FIRST_TOUCH_SIDE_MISMATCH")
        if label.get("terminal_reason") != terminal:
            reasons.append("TERMINAL_REASON_MISMATCH")
        touch_price = _number(label.get(touch_key))
        if touch_price is None or touch_price <= 0.0:
            reasons.append("MISSING_TOUCH_PRICE")

    for key in ("mfe_pct", "mae_pct"):
        value = _number(label.get(key))
        if value is None or value < 0.0:
            reasons.append("MISSING_EXCURSION_METRICS")
            break
    if label.get("path_complete") is not True:
        reasons.append("INCOMPLETE_PATH")
    if label.get("data_quality_status") not in VERIFIED_DATA_QUALITY:
        reasons.append("UNVERIFIED_DATA_QUALITY")
    if label.get("candle_interval_seconds") != 60:
        reasons.append("CANDLE_INTERVAL_MISMATCH")
    samples = label.get("path_samples")
    if type(samples) is not int or samples < (1 if expected is not None or reported_status == "AMBIGUOUS" else 0):
        reasons.append("MISSING_PATH_SAMPLES")
    try:
        measurement = _utc(label.get("measurement_start_utc"))
        observed = _utc(label.get("observed_through_utc"))
        if not measurement <= observed <= as_of:
            reasons.append("OUTCOME_TIME_ORDER_OR_FUTURE_DATA")
        if measurement != _decision_time(row):
            reasons.append("MEASUREMENT_DOES_NOT_START_AT_ALERT")
        if expected is not None or reported_status == "AMBIGUOUS":
            decided = _utc(label.get("decision_time_utc"))
            if not measurement <= decided <= observed:
                reasons.append("OUTCOME_TIME_ORDER_OR_FUTURE_DATA")
            if type(horizon) is int and decided > measurement + timedelta(minutes=horizon):
                reasons.append("DECISION_AFTER_HORIZON")
            elapsed = label.get("time_to_decision_seconds")
            if type(elapsed) is not int or elapsed != int((decided - measurement).total_seconds()):
                reasons.append("DECISION_DURATION_MISMATCH")
    except (TypeError, ValueError, OverflowError):
        reasons.append("MISSING_OR_INVALID_DECISION_TIME")

    # Initial sub-minute unobserved data is an explicit v7 contract limitation,
    # never silently represented as a complete entry-minute price path.
    gap = label.get("initial_gap_seconds")
    unobserved = label.get("initial_gap_unobserved")
    if (
        type(gap) is not int
        or not 0 <= gap < 60
        or type(unobserved) is not bool
        or unobserved != (gap > 0)
    ):
        reasons.append("INITIAL_GAP_AUDIT_MISMATCH")

    reference = _number(label.get("reference_price"))
    original_entry = _number(_field(row, "entry_price"))
    if original_entry is None:
        original_entry = _number(_event(row).get("current_price"))
    if original_entry is None or original_entry <= 0:
        reasons.append("MISSING_ORIGINAL_ENTRY_PRICE")
    elif reference is not None and not math.isclose(reference, original_entry, rel_tol=1e-12):
        reasons.append("REFERENCE_PRICE_DIFFERS_FROM_ORIGINAL_ENTRY")
    favorable = _number(label.get("favorable_barrier_price"))
    adverse = _number(label.get("adverse_barrier_price"))
    if (
        reference is None or reference <= 0
        or favorable is None or favorable <= 0
        or adverse is None or adverse <= 0
    ):
        reasons.append("MISSING_BARRIER_PRICES")
    elif direction in DIRECTIONS and type(threshold) is int:
        sign = 1 if direction == "LONG" else -1
        if not (
            math.isclose(favorable / reference, 1 + sign * threshold / 10000, abs_tol=1e-10)
            and math.isclose(adverse / reference, 1 - sign * threshold / 10000, abs_tol=1e-10)
        ):
            reasons.append("BARRIER_PRICE_MISMATCH")
        if expected is not None:
            touch = _number(label.get(expected[3]))
            barrier = favorable if expected[0] else adverse
            if touch is not None and not math.isclose(touch, barrier, rel_tol=1e-9):
                reasons.append("TOUCH_PRICE_DOES_NOT_MATCH_BARRIER")
        elif reported_status == "AMBIGUOUS":
            for key, barrier in (("favorable_touch_price", favorable), ("adverse_touch_price", adverse)):
                touch = _number(label.get(key))
                if touch is None or not math.isclose(touch, barrier, rel_tol=1e-9):
                    reasons.append("TOUCH_PRICE_DOES_NOT_MATCH_BARRIER")
    # Nondecisive v7 states are observable statuses, not usable probability
    # evidence. A broken state/path stays DATA_MISSING rather than OPEN.
    reported_status = "DATA_MISSING" if reasons else reported_status
    if expected is None:
        reasons.append("UNRESOLVED_OR_OPEN_OUTCOME")
    return {
        "eligible": not reasons,
        "success": expected[0] if expected is not None and not reasons else None,
        "reported_status": reported_status,
        "source_status": status,
        "exclusion_reasons": sorted(set(reasons)),
    }


_FEATURE_PREFIXES = (
    "event.", "time.", "historical.", "max_pain.", "captured.", "latest.",
    "raw.", "aligned.", "aligned_log.", "model.", "category.", "sequence.",
    "aligned_sequence.", "price_oi.", "futures_cvd.", "spot_cvd.", "liquidity.",
)
_LABEL_TOKENS = ("outcome", "first_touch", "mfe", "mae", "future", "btc_parent", "label")


def _conditions(formula: Mapping[str, Any]) -> list[dict[str, Any]]:
    values = formula.get("conditions")
    if not isinstance(values, (list, tuple)) or not values:
        raise ValueError("ordered v7 formulas need frozen decision-time conditions")
    result = []
    for item in values:
        if not isinstance(item, Mapping):
            raise ValueError("formula conditions must be objects")
        feature = str(item.get("feature") or "")
        if (
            not feature.startswith(_FEATURE_PREFIXES)
            or any(token in feature.lower().replace("futures", "") for token in _LABEL_TOKENS)
            or item.get("operator") not in {">=", "<=", "==", ">", "<"}
            or isinstance(item.get("value"), (dict, list, tuple))
            or item.get("value") is None
        ):
            raise ValueError("invalid or non-decision-time formula condition")
        result.append(dict(item))
    return result


def _matches(features: Mapping[str, Any], conditions: Sequence[Mapping[str, Any]]) -> bool:
    for condition in conditions:
        feature = condition["feature"]
        if feature not in features:
            return False
        actual, expected = features[feature], condition["value"]
        if condition["operator"] == "==":
            if isinstance(actual, bool) or isinstance(expected, bool):
                passed = type(actual) is bool and type(expected) is bool and actual is expected
            else:
                passed = type(actual) is type(expected) and actual == expected
                if type(actual) in (int, float) and type(expected) in (int, float):
                    passed = actual == expected and _number(actual) is not None
        else:
            left, right = _number(actual), _number(expected)
            passed = left is not None and right is not None and {
                ">=": lambda: left >= right, "<=": lambda: left <= right,
                ">": lambda: left > right, "<": lambda: left < right,
            }[condition["operator"]]()
        if not passed:
            return False
    return True


def _identity(row: Mapping[str, Any]) -> tuple[Any, str]:
    return _field(row, "event_id"), str(_field(row, "direction") or "")


def extract_event_features(event: Mapping[str, Any]) -> dict[str, Any]:
    """Read captured market modules; never query a later/latest market sample."""
    snapshot = _mapping(event.get("engine_snapshot"))
    modules = _mapping(_mapping(snapshot.get("market_evidence")).get("modules"))
    direction = str(event.get("direction") or "").upper()
    features: dict[str, Any] = {}
    for field in ("symbol", "event_type", "timeframe"):
        if event.get(field):
            features[f"event.{field}"] = event[field]
    for name, source in (
        ("price_oi", "positioning"),
        ("futures_cvd", "futures_flow"),
        ("spot_cvd", "spot_flow"),
    ):
        module = _mapping(modules.get(source))
        score = _number(module.get("score"))
        if score is None:
            continue
        raw_direction = str(module.get("direction") or "").upper()
        source_direction = {
            "BUY": "LONG", "BULLISH": "LONG", "UP": "LONG",
            "SELL": "SHORT", "BEARISH": "SHORT", "DOWN": "SHORT",
        }.get(raw_direction, raw_direction)
        if source_direction not in DIRECTIONS:
            source_direction = "LONG" if score > 0 else "SHORT" if score < 0 else "NEUTRAL"
        features[f"{name}.total_score"] = abs(score)
        features[f"{name}.direction"] = source_direction
        features[f"{name}.aligned_score"] = abs(score) if source_direction == direction else -abs(score)
    return features


def candidate_catalog(*, include_extended: bool = False) -> list[dict[str, Any]]:
    """Small prespecified screen, never label-fitted or advertised as exhaustive."""
    families = ("price_oi", "futures_cvd", "spot_cvd")
    candidates = []
    def condition(name: str) -> dict[str, Any]:
        return {"feature": f"{name}.aligned_score", "operator": ">=", "value": 65.0}
    for family in families:
        candidates.append({
            "formula_id": f"{family.upper()}_TOTAL_65",
            "conditions": [condition(family)], "repeat_count": 1,
            "catalog_version": CATALOG_VERSION,
        })
    for index, left in enumerate(families):
        for right in families[index + 1:]:
            candidates.append({
                "formula_id": f"{left.upper()}_{right.upper()}_TOTAL_65",
                "conditions": [condition(left), condition(right)], "repeat_count": 1,
                "catalog_version": CATALOG_VERSION,
            })
    candidates.append({
        "formula_id": "STRICT_TRIPLE_TOTAL_65",
        "conditions": [condition(family) for family in families], "repeat_count": 1,
        "catalog_version": CATALOG_VERSION,
    })
    if include_extended:
        import research_ordered_question_catalog as questions
        extended = questions.candidates()
        return candidates + extended + questions.inverse_candidates(candidates + extended)
    return candidates


def formula_contract(
    formula: Mapping[str, Any], *, episode_policy_versions: Sequence[str] = ()
) -> dict[str, Any]:
    """Freeze every identity dimension; new policies never inherit old evidence."""
    return {
        "conditions": _conditions(formula),
        "repeat_count": formula.get("repeat_count", 1),
        "direction": formula.get("direction"),
        "symbol": formula.get("symbol"),
        "frozen_at_utc": (
            _utc(formula["frozen_at_utc"]).isoformat()
            if formula.get("frozen_at_utc") is not None else None
        ),
        "catalog_version": formula.get("catalog_version"),
        "candidate_version": formula.get("candidate_version"),
        "episode_policy_versions": sorted(set(episode_policy_versions)),
        "method_version": METHOD_VERSION,
        "policy_version": POLICY_VERSION,
        "metric_scope": METRIC_SCOPE,
    }


def matches(
    candidate: Mapping[str, Any], features: Mapping[str, Any], direction: str
) -> bool:
    """Outcome-blind candidate match for incremental stores."""
    if direction not in DIRECTIONS or candidate.get("direction") not in (None, direction):
        return False
    if candidate.get("symbol") and features.get("event.symbol") != candidate["symbol"]:
        return False
    return _matches(features, _conditions(candidate))


def _formula_decisions(
    rows: Sequence[Mapping[str, Any]], formula: Mapping[str, Any], *, as_of: datetime
) -> tuple[list[Mapping[str, Any]], Counter]:
    """Choose one repeat-qualified alert per symbol/wave without reading labels."""
    conditions = _conditions(formula)
    repeat = formula.get("repeat_count", 1)
    if type(repeat) is not int or repeat < 1:
        raise ValueError("repeat_count must be a positive integer")
    direction = formula.get("direction")
    if direction not in (None, *DIRECTIONS):
        raise ValueError("formula direction must be LONG, SHORT or absent")
    exclusions: Counter = Counter()
    unique: dict[tuple[Any, str], Mapping[str, Any]] = {}
    conflicting: set[tuple[Any, str]] = set()
    for row in rows:
        try:
            timestamp = _decision_time(row)
            observed = _utc(_field(row, "features_observed_at_utc"))
        except (TypeError, ValueError, OverflowError):
            exclusions["MISSING_DECISION_FEATURE_TIMESTAMP"] += 1
            continue
        if timestamp > as_of or observed > timestamp:
            exclusions["FUTURE_DECISION_OR_FEATURES"] += 1
            continue
        if direction and _field(row, "direction") != direction:
            continue
        if formula.get("symbol") and _field(row, "symbol") != formula["symbol"]:
            continue
        if formula.get("frozen_at_utc") is not None and timestamp < _utc(formula["frozen_at_utc"]):
            exclusions["BEFORE_FORMULA_FREEZE"] += 1
            continue
        if (
            not _field(row, "btc_parent_movement_id")
            or _field(row, "membership_status") != "LIVE"
            or _field(row, "parent_evidence_eligible") is not True
        ):
            exclusions["UNVERIFIED_BTC_PARENT_MOVEMENT"] += 1
            if (
                _field(row, "membership_status") in (None, "", "BTC_DATA_MISSING")
                and _matches(_mapping(row.get("decision_features")), conditions)
            ):
                # Missing membership could hide an earlier matching alert in
                # a subsequently observed wave. Do not let dropping it select
                # a better-known, later outcome. The explicit initial warm-up
                # BOUNDARY_UNVERIFIED period is outside this policy's evidence
                # universe and is tracked separately without censoring it.
                exclusions["MISSING_BTC_MEMBERSHIP_COVERAGE"] += 1
            continue
        identity = _identity(row)
        if type(identity[0]) is not int or identity[0] <= 0 or identity[1] not in DIRECTIONS:
            exclusions["INVALID_EVENT_IDENTITY"] += 1
            continue
        previous = unique.get(identity)
        if previous is not None:
            signature = lambda value: (
                _decision_time(value), _field(value, "symbol"),
                _field(value, "btc_parent_movement_id"),
                _mapping(value.get("decision_features")),
            )
            if signature(previous) != signature(row):
                conflicting.add(identity)
            continue
        unique[identity] = row
    exclusions["CONFLICTING_DECISION_SNAPSHOTS"] += len(conflicting)
    groups: dict[tuple[str, str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for identity, row in unique.items():
        if identity not in conflicting and _matches(_mapping(row.get("decision_features")), conditions):
            key = (
                str(_field(row, "btc_parent_movement_id")),
                str(_field(row, "symbol") or ""),
                str(_field(row, "direction")),
            )
            groups[key].append(row)
    selected = []
    for group in groups.values():
        ordered = sorted(group, key=lambda row: (_decision_time(row), _identity(row)))
        # Different categories emitted at one sampling timestamp do not create
        # a spurious second/third repetition of the same observation.
        timestamps: dict[datetime, list[Mapping[str, Any]]] = defaultdict(list)
        for row in ordered:
            timestamps[_decision_time(row)].append(row)
        if len(timestamps) >= repeat:
            selected.extend(timestamps[sorted(timestamps)[repeat - 1]])
        else:
            exclusions["REPEAT_CONDITION_NOT_YET_MET"] += 1
    return selected, +exclusions


def _wilson(successes: int, total: int) -> float | None:
    if not total:
        return None
    z = 1.959963984540054
    proportion = successes / total
    return 100 * max(0.0, (
        proportion + z * z / (2 * total)
        - z * math.sqrt((proportion * (1 - proportion) + z * z / (4 * total)) / total)
    ) / (1 + z * z / total))


def _summary(evidence: Sequence[Mapping[str, Any]], *, as_of: datetime) -> dict[str, Any]:
    fresh = [row for row in evidence if row["forecast_start_utc"] >= as_of - timedelta(days=FRESH_DAYS)]
    route = "STANDARD" if len(evidence) >= 5 else "FRESH" if len(fresh) >= 3 else "INSUFFICIENT"
    active = fresh if route == "FRESH" else list(evidence)
    successes = sum(row["success"] is True for row in active)
    mfe = [row["mfe_pct"] for row in active]
    mae = [row["mae_pct"] for row in active]
    median_mfe, median_mae = (median(mfe), median(mae)) if active else (None, None)
    return {
        "independent_waves": len(evidence),
        "fresh_independent_waves": len(fresh),
        "fresh_window_days": FRESH_DAYS,
        "count_route": route,
        "count_eligible": route != "INSUFFICIENT",
        "sample_size": len(active),
        "successes": successes,
        "failures": len(active) - successes,
        "hit_rate_pct": 100 * successes / len(active) if active else None,
        "diagnostic_hit_rate_pct": 100 * successes / len(active) if active else None,
        "usable_sample_size": len(active),
        "evidence_coverage_status": "COMPLETE_SELECTED_COHORT",
        "wilson_95_lower_pct": _wilson(successes, len(active)),
        "median_mfe_pct": median_mfe,
        "median_mae_pct": median_mae,
        "median_mfe_mae_ratio": median_mfe / median_mae if median_mae else None,
        "median_mfe_mae_ratio_state": (
            "MISSING" if not active else "FINITE" if median_mae else
            "UNBOUNDED_ZERO_MAE" if median_mfe else "UNDEFINED_ZERO_ZERO"
        ),
        "favorable_dominance_rate_pct": (
            100 * sum(row["mfe_pct"] > row["mae_pct"] for row in active) / len(active)
            if active else None
        ),
        "median_paired_favorable_minus_adverse_pct": median([
            row["mfe_pct"] - row["mae_pct"] for row in active
        ]) if active else None,
        "btc_parent_movement_ids": sorted(row["btc_parent_movement_id"] for row in active),
        "evidence_event_ids": sorted({event_id for row in active for event_id in row["event_ids"]}),
    }


def _block_incomplete_evidence(result: dict[str, Any], reason: str) -> None:
    """Retain visible diagnostics without publishing a usable censored rate."""
    result["count_eligible"] = False
    result["count_route"] = reason
    result["usable_sample_size"] = 0
    result["hit_rate_pct"] = None
    result["wilson_95_lower_pct"] = None
    result["evidence_coverage_status"] = reason


def _cohort_status(statuses: Sequence[str], *, conflicting: bool = False) -> str:
    """Report one cohort without treating any nondecisive member as a vote."""
    if conflicting or not statuses:
        return "DATA_MISSING"
    # Preserve the existing all-members-required evidence policy. These are
    # reporting priorities only; individual member statuses remain in audit.
    for status in ("DATA_MISSING", "OPEN", "AMBIGUOUS", "NO_TOUCH", "FAILURE"):
        if status in statuses:
            return status
    return "SUCCESS"


def _status_counts(
    episodes: Sequence[Mapping[str, Any]], *, as_of: datetime, count_route: str
) -> dict[str, Any]:
    def counts(items: Sequence[Mapping[str, Any]]) -> dict[str, int]:
        counter = Counter(item["status"] for item in items)
        return {status: counter[status] for status in WAVE_STATUSES}

    fresh = [item for item in episodes if item["forecast_start_utc"] >= as_of - timedelta(days=FRESH_DAYS)]
    active = fresh if count_route == "FRESH" else episodes
    active_counts = counts(active)
    return {
        "status_reporting_version": STATUS_REPORTING_VERSION,
        "status_count_scope": "FRESH_14D_SELECTED_WAVES" if count_route == "FRESH" else "ALL_SELECTED_WAVES",
        "status_counts": active_counts,
        "all_wave_status_counts": counts(episodes),
        "fresh_wave_status_counts": counts(fresh),
        "open_waves": active_counts["OPEN"],
        "ambiguous_waves": active_counts["AMBIGUOUS"],
        "no_touch_waves": active_counts["NO_TOUCH"],
        "data_missing_waves": active_counts["DATA_MISSING"],
        "cohort_status_policy": "DATA_MISSING > OPEN > AMBIGUOUS > NO_TOUCH > FAILURE > SUCCESS; all representative labels required for a decisive vote",
    }


def summarize_scope(
    rows: Sequence[Mapping[str, Any]],
    *,
    analysis_as_of_utc: Any,
    truncated: bool = False,
    source_coverage_complete: bool = True,
) -> dict[str, Any]:
    """Audit an already outcome-blind selected candidate/symbol/cell scope.

    The incremental store must select candidate matches before its LEFT JOIN
    to outcomes.  Every simultaneous earliest representative must be retained,
    including missing/unresolved labels.  This function reselects the earliest
    timestamp from the supplied decisions and validates one shared cell.
    """
    as_of = _utc(analysis_as_of_utc)
    if type(source_coverage_complete) is not bool:
        raise ValueError("source_coverage_complete must be an explicit boolean")
    waves: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    diagnostics: Counter = Counter()
    cells = set()
    episode_policies = {
        str(_field(row, "episode_policy_version"))
        for row in rows if _field(row, "episode_policy_version")
    }
    if len(episode_policies) > 1:
        raise ValueError("summarize_scope cannot pool different BTC movement policies")
    for row in rows:
        label = _outcome(row)
        cells.add((
            _field(row, "direction"),
            label.get("window_minutes", label.get("horizon_minutes")),
            label.get("threshold_bps"),
        ))
        try:
            timestamp = _decision_time(row)
            feature_time = _utc(_field(row, "features_observed_at_utc"))
        except (TypeError, ValueError, OverflowError):
            diagnostics["MISSING_DECISION_FEATURE_TIMESTAMP"] += 1
            continue
        if timestamp > as_of or feature_time > timestamp:
            diagnostics["FUTURE_DECISION_OR_FEATURES"] += 1
            continue
        if (
            not _field(row, "btc_parent_movement_id")
            or _field(row, "membership_status") != "LIVE"
            or _field(row, "parent_evidence_eligible") is not True
        ):
            diagnostics["UNVERIFIED_BTC_PARENT_MOVEMENT"] += 1
            if _field(row, "membership_status") in (None, "", "BTC_DATA_MISSING"):
                source_coverage_complete = False
                diagnostics["MISSING_BTC_MEMBERSHIP_COVERAGE"] += 1
            continue
        if type(_identity(row)[0]) is not int or _identity(row)[0] <= 0:
            diagnostics["INVALID_EVENT_IDENTITY"] += 1
            continue
        waves[str(_field(row, "btc_parent_movement_id"))].append(row)
    if len(cells) > 1:
        raise ValueError("summarize_scope requires one direction/horizon/threshold cell")
    episodes, evidence = [], []
    for wave, members in sorted(waves.items()):
        earliest = min(_decision_time(row) for row in members)
        selected = [row for row in members if _decision_time(row) == earliest]
        unique: dict[Any, Mapping[str, Any]] = {}
        excluded: Counter = Counter()
        for row in selected:
            identity = _identity(row)
            if identity in unique and dict(_outcome(unique[identity])) != dict(_outcome(row)):
                excluded["CONFLICTING_OUTCOME_ROWS"] += 1
            unique[identity] = row
        labels, member_statuses = [], []
        for row in unique.values():
            audit = ordered_outcome_evidence(row, analysis_as_of_utc=as_of)
            excluded.update(audit["exclusion_reasons"])
            member_statuses.append({
                "event_id": _identity(row)[0],
                "reported_status": audit["reported_status"],
                "source_status": audit["source_status"],
                "first_touch_side": _outcome(row).get("first_touch_side"),
                "terminal_reason": _outcome(row).get("terminal_reason"),
            })
            if audit["eligible"]:
                labels.append(_outcome(row))
        eligible = bool(labels) and not excluded
        success = all(label["success"] is True for label in labels) if eligible else None
        item = {
            "btc_parent_movement_id": wave,
            "forecast_start_utc": earliest,
            "event_ids": sorted({_identity(row)[0] for row in selected}),
            "eligible": eligible,
            "success": success,
            "status": _cohort_status(
                [item["reported_status"] for item in member_statuses],
                conflicting=bool(excluded.get("CONFLICTING_OUTCOME_ROWS")),
            ),
            "representative_outcome_statuses": member_statuses,
            "first_touch_side": "FAVORABLE" if success is True else "ADVERSE" if success is False else None,
            "mfe_pct": min(float(label["mfe_pct"]) for label in labels) if eligible else None,
            "mae_pct": max(float(label["mae_pct"]) for label in labels) if eligible else None,
            "exclusion_reasons": dict(sorted(excluded.items())),
        }
        episodes.append(item)
        diagnostics.update(excluded)
        if eligible:
            evidence.append(item)
    result = _summary(evidence, as_of=as_of)
    result.update(_status_counts(episodes, as_of=as_of, count_route=result["count_route"]))
    if not source_coverage_complete:
        _block_incomplete_evidence(result, "INCOMPLETE_MEMBERSHIP_COVERAGE")
        diagnostics["INCOMPLETE_MEMBERSHIP_COVERAGE"] += 1
    if truncated:
        _block_incomplete_evidence(result, "TRUNCATED_EVIDENCE")
        diagnostics["TRUNCATED_EVIDENCE"] += 1
    result.update({
        "policy_version": POLICY_VERSION,
        "outcome_method_version": METHOD_VERSION,
        "metric_scope": METRIC_SCOPE,
        "episode_policy_versions": sorted(episode_policies),
        "decision_waves": len(waves), "excluded_waves": len(waves) - len(evidence),
        "exclusion_reasons": dict(sorted(diagnostics.items())),
        "episodes": episodes, "truncated": truncated,
        "source_coverage_complete": source_coverage_complete,
        "research_ready": False,
        "validation_status": "ORDERED_EVIDENCE_ONLY_AWAITING_INDEPENDENT_VALIDATION",
        "validation_limitations": [
            "BTC parent membership is an explicit operational grouping policy, not a statistical independence test",
            "MFE/MAE stop at first touch; full-horizon potential and asymmetry acceptance require separate evidence",
            "count routes do not replace frozen prospective or holdout probability/asymmetry validation",
        ],
        "live_effect": "NONE",
    })
    return result


def evaluate_formulas(
    rows: Sequence[Mapping[str, Any]],
    formulas: Sequence[Mapping[str, Any]],
    *,
    analysis_as_of_utc: Any,
    include_per_symbol: bool = True,
) -> dict[str, Any]:
    """Evaluate all 8 thresholds × 4 horizons for each frozen formula.

    Rows are complete decision/outcome joins, including unresolved decisions.
    Omitting unresolved matches would leak outcomes into sample selection.
    Simultaneous earliest eligible coin matches share one conservative vote;
    one unresolved member excludes that cohort, and any loss makes it a loss.
    Repeats start at the nth matching alert; earlier price paths are never used.
    """
    as_of = _utc(analysis_as_of_utc)
    outcomes: dict[tuple[Any, str, Any, Any], Mapping[str, Any]] = {}
    conflicts: set[tuple[Any, str, Any, Any]] = set()
    for row in rows:
        label = _outcome(row)
        key = (*_identity(row), label.get("window_minutes", label.get("horizon_minutes")), label.get("threshold_bps"))
        previous = outcomes.get(key)
        if previous is not None and dict(_outcome(previous)) != dict(label):
            conflicts.add(key)
        else:
            outcomes[key] = row
    results = []
    for formula in formulas:
        selected, decision_exclusions = _formula_decisions(rows, formula, as_of=as_of)
        formula_payload = formula_contract(formula, episode_policy_versions=[
            str(_field(row, "episode_policy_version")) for row in rows
            if _field(row, "episode_policy_version")
        ])
        formula_key = hashlib.sha256(json.dumps(formula_payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        scopes = [None]
        if include_per_symbol:
            scopes.extend(sorted({str(_field(row, "symbol")) for row in selected if _field(row, "symbol")}))
        cells = []
        for scope in scopes:
            for direction in DIRECTIONS:
                if formula.get("direction") and formula["direction"] != direction:
                    continue
                candidates = [row for row in selected if _field(row, "direction") == direction and (scope is None or _field(row, "symbol") == scope)]
                waves: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
                for row in candidates:
                    waves[str(_field(row, "btc_parent_movement_id"))].append(row)
                # Freeze the wave's earliest matching/repeat-qualified cohort
                # once, before looking at any threshold or horizon outcome.
                cohorts = {
                    wave: [row for row in members if _decision_time(row) == min(_decision_time(item) for item in members)]
                    for wave, members in waves.items()
                }
                for horizon in HORIZONS_MINUTES:
                    for threshold in THRESHOLDS_BPS:
                        evidence = []
                        episode_statuses = []
                        excluded: Counter = Counter()
                        for wave, members in cohorts.items():
                            labels = []
                            statuses = []
                            eligible = True
                            for member in members:
                                key = (*_identity(member), horizon, threshold)
                                row = outcomes.get(key)
                                if key in conflicts:
                                    audit = {"eligible": False, "reported_status": "DATA_MISSING", "exclusion_reasons": ["CONFLICTING_OUTCOME_ROWS"]}
                                elif row is None:
                                    audit = {"eligible": False, "reported_status": "DATA_MISSING", "exclusion_reasons": ["MISSING_OUTCOME_CELL"]}
                                else:
                                    audit = ordered_outcome_evidence(row, analysis_as_of_utc=as_of)
                                statuses.append(audit["reported_status"])
                                if not audit["eligible"]:
                                    eligible = False
                                    excluded.update(audit["exclusion_reasons"])
                                else:
                                    labels.append(_outcome(row))
                            episode_statuses.append({
                                "forecast_start_utc": _decision_time(members[0]),
                                "status": _cohort_status(statuses),
                            })
                            if eligible:
                                evidence.append({
                                    "btc_parent_movement_id": wave,
                                    "forecast_start_utc": _decision_time(members[0]),
                                    "success": all(label["success"] is True for label in labels),
                                    "mfe_pct": min(float(label["mfe_pct"]) for label in labels),
                                    "mae_pct": max(float(label["mae_pct"]) for label in labels),
                                    "event_ids": sorted({_identity(row)[0] for row in members}),
                                })
                        summary = _summary(evidence, as_of=as_of)
                        summary.update(_status_counts(episode_statuses, as_of=as_of, count_route=summary["count_route"]))
                        if decision_exclusions.get("MISSING_BTC_MEMBERSHIP_COVERAGE"):
                            _block_incomplete_evidence(summary, "INCOMPLETE_MEMBERSHIP_COVERAGE")
                        if len(formula_payload["episode_policy_versions"]) > 1:
                            _block_incomplete_evidence(summary, "MIXED_BTC_MEMBERSHIP_POLICIES")
                        cells.append({
                            "symbol": scope or "ALL", "direction": direction,
                            "window_minutes": horizon, "threshold_bps": threshold,
                            "decision_waves": len(cohorts), "excluded_waves": len(cohorts) - len(evidence),
                            "exclusion_reasons": dict(sorted(excluded.items())),
                            **summary,
                            "research_ready": False,
                            "metric_scope": METRIC_SCOPE,
                            "validation_status": "ORDERED_EVIDENCE_ONLY_AWAITING_INDEPENDENT_VALIDATION",
                        })
        results.append({
            "formula_id": formula.get("formula_id", formula.get("formula_key", formula_key)),
            "formula_key": formula_key, "formula_contract": formula_payload,
            "decision_exclusion_reasons": dict(sorted(decision_exclusions.items())),
            "cells": cells,
        })
    return {
        "policy_version": POLICY_VERSION, "outcome_method_version": METHOD_VERSION,
        "analysis_as_of_utc": as_of.isoformat(), "formulas": results,
        "evidence_scope": "one BTC parent movement across all coins and times; counts never added across cells",
        "acceptance_scope": "count route is not acceptance; frozen discovery/holdout and probability/asymmetry validation remain required",
        "live_effect": "NONE",
    }
