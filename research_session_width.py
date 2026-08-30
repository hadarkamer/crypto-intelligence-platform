"""Shared prior-only market-session movement-width calibration.

The Formula feature matrix and the historical first-touch replay must freeze
the same qualifying width at one decision time.  This module owns both the
price-only historical index and the calibration calculation so those two
paths cannot silently drift.

Only raw price observations available at or before their recorded timestamp
enter the index.  A decision uses the prefix strictly before its calibration
``as_of`` time.  The resulting factor may reduce only the favorable movement
width, and only for a horizon containing WEEKEND time; it never changes any
probability, direction, MAE, efficiency or statistical gate.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import math
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

import market_session_baseline


CALIBRATION_VERSION = "prior-only-session-width-v2"
LOOKBACK_DAYS = 180
MIN_EFFECTIVE_SAMPLES = 30
MAX_POINT_AGE_MINUTES = 45
COMPOSITION_TOLERANCE = market_session_baseline.DEFAULT_COMPOSITION_TOLERANCE
SOURCE_KIND = "PRIOR_ONLY_SESSION_CALIBRATION"


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _finite_number(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _strict_json_number(value: Any) -> Optional[float]:
    """Accept a finite JSON number without coercing strings or booleans."""
    if type(value) not in (int, float):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _rounded(value: Any, digits: int = 6) -> Optional[float]:
    number = _finite_number(value)
    return round(number, digits) if number is not None else None


def session_composition_label(active_ratio: float) -> str:
    if active_ratio >= 1.0 - 1e-9:
        return "ACTIVE_ONLY"
    if active_ratio <= 1e-9:
        return "WEEKEND_ONLY"
    return "MIXED"


def validate_movement_width_reference(
    reference: Mapping[str, Any],
    *,
    expected_symbol: Any,
    event_time: Any,
    horizon_minutes: int,
) -> tuple[bool, str]:
    """Validate a same-symbol prior-only calibration without trusting summaries."""
    try:
        decision_time = _utc(event_time)
        horizon = int(horizon_minutes)
        stored_horizon = reference.get("horizon_minutes")
        lookback_days = reference.get("lookback_days")
        minimum_samples = reference.get("minimum_effective_samples")
        stored_segments_value = reference.get("session_segments")
        if any(
            isinstance(value, bool) or not isinstance(value, int)
            for value in (
                stored_horizon,
                lookback_days,
                minimum_samples,
                stored_segments_value,
            )
        ):
            return False, "movement-width integer fields are malformed"
        symbol = str(expected_symbol or "").strip().upper()
        if not symbol or str(reference.get("symbol") or "").strip().upper() != symbol:
            return False, "movement-width symbol differs from the decision symbol"
        if reference.get("calibration_version") != CALIBRATION_VERSION:
            return False, "movement-width calibration version is incompatible"
        if reference.get("policy") != (
            "prior raw price width; same-symbol session-composition matched; "
            "weekend width only"
        ):
            return False, "movement-width policy is incompatible"
        if str(reference.get("source_kind") or "").upper() != SOURCE_KIND:
            return False, "movement-width source is not prior-only"
        if stored_horizon != horizon:
            return False, "movement-width horizon differs from formula horizon"
        composition_tolerance = _strict_json_number(
            reference.get("composition_tolerance")
        )
        if (
            lookback_days != LOOKBACK_DAYS
            or minimum_samples != MIN_EFFECTIVE_SAMPLES
            or composition_tolerance is None
            or not math.isclose(
                composition_tolerance,
                COMPOSITION_TOLERANCE,
                rel_tol=0.0,
                abs_tol=1e-12,
            )
        ):
            return False, "movement-width calibration parameters are incompatible"
        as_of_utc = _utc(reference.get("as_of_utc"))
        if as_of_utc > decision_time:
            return False, "movement-width calibration is newer than decision time"

        floor_scale = _strict_json_number(reference.get("floor_scale_factor"))
        threshold_scale = _strict_json_number(
            reference.get("threshold_scale_factor")
        )
        if not (
            floor_scale is not None
            and threshold_scale is not None
            and 0.50 <= threshold_scale <= 1.00
            and math.isclose(
                floor_scale,
                threshold_scale,
                rel_tol=0.0,
                abs_tol=1e-12,
            )
        ):
            return False, "movement-width scale fields are invalid or inconsistent"
        applied = reference.get("applied")
        if not isinstance(applied, bool) or applied != (
            threshold_scale < 1.0 - 1e-9
        ):
            return False, "movement-width applied flag differs from scale"

        active_ratio, weekend_ratio, segments = (
            market_session_baseline.session_ratios(
                decision_time,
                decision_time + timedelta(minutes=horizon),
            )
        )
        stored_active = _strict_json_number(reference.get("session_active_ratio"))
        stored_weekend = _strict_json_number(
            reference.get("session_weekend_ratio")
        )
        stored_segments = stored_segments_value
        stored_composition = str(reference.get("session_composition") or "")
        if not (
            stored_active is not None
            and stored_weekend is not None
            and math.isclose(
                stored_active, active_ratio, rel_tol=0.0, abs_tol=1e-6
            )
            and math.isclose(
                stored_weekend, weekend_ratio, rel_tol=0.0, abs_tol=1e-6
            )
            and stored_segments == segments
            and stored_composition == session_composition_label(active_ratio)
        ):
            return False, "movement-width session context differs from New York calendar"
        if weekend_ratio <= 1e-9 and threshold_scale < 1.0 - 1e-9:
            return False, "ACTIVE-only horizon cannot relax movement width"

        reason = str(reference.get("reason") or "")
        evidence_names = (
            "prior_points",
            "session_matched_samples",
            "session_matched_effective_samples",
            "active_reference_samples",
            "active_reference_effective_samples",
            "session_matched_abs_return_p90_pct",
            "active_reference_abs_return_p90_pct",
        )
        present = {name: name in reference for name in evidence_names}
        if reason == "historical horizon unavailable":
            if any(present.values()) or not math.isclose(
                threshold_scale, 1.0, rel_tol=0.0, abs_tol=1e-12
            ):
                return False, "unavailable movement-width history has forged evidence"
            return True, "movement-width reference context is coherent"
        if not all(present.values()):
            return False, "movement-width evidence summary is incomplete"

        def strict_count(name: str) -> Optional[int]:
            value = reference.get(name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                return None
            return int(value)

        prior_points = strict_count("prior_points")
        matched_samples = strict_count("session_matched_samples")
        active_samples = strict_count("active_reference_samples")
        matched_effective = _strict_json_number(
            reference.get("session_matched_effective_samples")
        )
        active_effective = _strict_json_number(
            reference.get("active_reference_effective_samples")
        )
        raw_matched_p90 = reference.get(
            "session_matched_abs_return_p90_pct"
        )
        raw_active_p90 = reference.get(
            "active_reference_abs_return_p90_pct"
        )
        matched_p90 = (
            None
            if raw_matched_p90 is None
            else _strict_json_number(raw_matched_p90)
        )
        active_p90 = (
            None
            if raw_active_p90 is None
            else _strict_json_number(raw_active_p90)
        )
        if (
            prior_points is None
            or matched_samples is None
            or active_samples is None
            or matched_effective is None
            or active_effective is None
            or (raw_matched_p90 is not None and matched_p90 is None)
            or (raw_active_p90 is not None and active_p90 is None)
            or matched_samples > prior_points
            or active_samples > prior_points
            or matched_effective < 0.0
            or active_effective < 0.0
            or matched_effective > matched_samples + 1e-6
            or active_effective > active_samples + 1e-6
            or (matched_samples == 0) != (matched_p90 is None)
            or (active_samples == 0) != (active_p90 is None)
            or (matched_p90 is not None and matched_p90 < 0.0)
            or (active_p90 is not None and active_p90 < 0.0)
        ):
            return False, "movement-width sample evidence is malformed"

        sufficient = bool(
            matched_effective >= MIN_EFFECTIVE_SAMPLES
            and active_effective >= MIN_EFFECTIVE_SAMPLES
            and matched_p90 is not None
            and matched_p90 >= 0.0
            and active_p90 is not None
            and active_p90 > 0.0
        )
        if not sufficient:
            if reason != "insufficient prior-only width calibration evidence":
                return False, "insufficient movement-width evidence has an invalid reason"
            if not math.isclose(
                threshold_scale, 1.0, rel_tol=0.0, abs_tol=1e-12
            ):
                return False, "insufficient movement-width evidence cannot relax width"
            return True, "movement-width reference context is coherent"

        if weekend_ratio <= 1e-9:
            if (
                reason != "ACTIVE-only horizon keeps the static movement width"
                or not math.isclose(
                    threshold_scale, 1.0, rel_tol=0.0, abs_tol=1e-12
                )
            ):
                return False, "ACTIVE-only movement-width decision is inconsistent"
            return True, "movement-width reference context is coherent"

        expected_scale = round(
            min(1.0, max(0.50, float(matched_p90) / float(active_p90))),
            6,
        )
        expected_applied = expected_scale < 1.0 - 1e-9
        expected_reason = (
            "weekend/mixed width floor calibrated from prior raw price history"
            if expected_applied
            else "session width was not below the ACTIVE reference"
        )
        if (
            not math.isclose(
                threshold_scale, expected_scale, rel_tol=0.0, abs_tol=1e-12
            )
            or applied is not expected_applied
            or reason != expected_reason
        ):
            return False, "movement-width scale does not match frozen evidence"
        return True, "movement-width reference context is coherent"
    except (TypeError, ValueError, OverflowError):
        return False, "movement-width reference context is malformed"


@dataclass(frozen=True)
class PriceWidthSeries:
    """One horizon's prior raw absolute-return observations."""

    times: tuple[datetime, ...]
    abs_return_pcts: tuple[float, ...]
    active_ratios: tuple[float, ...]


def build_price_width_index(
    *,
    price_points: Mapping[str, Iterable[tuple[Any, Any]]],
    horizons_minutes: Sequence[int],
    max_point_age_minutes: int = MAX_POINT_AGE_MINUTES,
) -> Dict[tuple[str, int], PriceWidthSeries]:
    """Build same-symbol backward price changes without future outcomes.

    ``price_points`` contains availability timestamps and raw prices.  A point
    at time ``t`` uses the newest price at or before ``t - horizon``; if that
    reference is stale it is omitted.  Duplicate timestamps are resolved by
    the final row supplied for that timestamp, matching the feature matrix's
    rightmost prior-point semantics.
    """
    horizons = tuple(sorted({int(value) for value in horizons_minutes if int(value) > 0}))
    maximum_age = max(0, int(max_point_age_minutes))
    output: Dict[tuple[str, int], PriceWidthSeries] = {}

    for raw_symbol, raw_points in price_points.items():
        symbol = str(raw_symbol or "").strip().upper()
        if not symbol:
            continue
        by_time: Dict[datetime, float] = {}
        for raw_time, raw_price in raw_points:
            price = _finite_number(raw_price)
            if price is None or price <= 0.0:
                continue
            try:
                point_time = _utc(raw_time)
            except (TypeError, ValueError, OverflowError):
                continue
            by_time[point_time] = price
        times = tuple(sorted(by_time))
        prices = tuple(by_time[value] for value in times)
        if not times:
            continue

        for horizon in horizons:
            samples: list[tuple[datetime, float, float]] = []
            delta = timedelta(minutes=horizon)
            for index, point_time in enumerate(times):
                reference_time = point_time - delta
                prior_index = bisect_right(times, reference_time) - 1
                if prior_index < 0:
                    continue
                prior_time = times[prior_index]
                age_minutes = (reference_time - prior_time).total_seconds() / 60.0
                if age_minutes < 0.0 or age_minutes > maximum_age:
                    continue
                previous = prices[prior_index]
                current = prices[index]
                if previous <= 0.0:
                    continue
                change = abs((current - previous) / previous * 100.0)
                if not math.isfinite(change):
                    continue
                active_ratio, _weekend_ratio, _segments = (
                    market_session_baseline.session_ratios(
                        reference_time, point_time
                    )
                )
                samples.append((point_time, change, float(active_ratio)))
            if samples:
                output[(symbol, horizon)] = PriceWidthSeries(
                    times=tuple(item[0] for item in samples),
                    abs_return_pcts=tuple(item[1] for item in samples),
                    active_ratios=tuple(item[2] for item in samples),
                )
    return output


def movement_width_reference(
    *,
    symbol: str,
    event_time: Any,
    horizon_minutes: int,
    as_of_utc: Any,
    historical_index: Mapping[tuple[str, int], PriceWidthSeries],
    lookback_days: int = LOOKBACK_DAYS,
    minimum_effective_samples: int = MIN_EFFECTIVE_SAMPLES,
    composition_tolerance: float = COMPOSITION_TOLERANCE,
) -> Dict[str, Any]:
    """Freeze a prior-only session-matched width reference for one horizon."""
    normalized_symbol = str(symbol or "").strip().upper()
    decision_time = _utc(event_time)
    cutoff = _utc(as_of_utc)
    if cutoff > decision_time:
        raise ValueError("movement-width calibration cannot be newer than decision time")
    horizon = int(horizon_minutes)
    outcome_end = decision_time + timedelta(minutes=max(0, horizon))
    active_ratio, weekend_ratio, segments = market_session_baseline.session_ratios(
        decision_time, outcome_end
    )
    result: Dict[str, Any] = {
        "symbol": normalized_symbol,
        "calibration_version": CALIBRATION_VERSION,
        "policy": (
            "prior raw price width; same-symbol session-composition matched; "
            "weekend width only"
        ),
        "source_kind": SOURCE_KIND,
        "as_of_utc": cutoff,
        "horizon_minutes": horizon,
        "session_active_ratio": round(active_ratio, 6),
        "session_weekend_ratio": round(weekend_ratio, 6),
        "session_segments": segments,
        "session_composition": session_composition_label(active_ratio),
        "lookback_days": int(lookback_days),
        "composition_tolerance": float(composition_tolerance),
        "minimum_effective_samples": int(minimum_effective_samples),
        "floor_scale_factor": 1.0,
        "threshold_scale_factor": 1.0,
        "applied": False,
    }
    historical = historical_index.get((normalized_symbol, horizon))
    if horizon <= 0 or historical is None:
        result["reason"] = "historical horizon unavailable"
        return result

    start = cutoff - timedelta(days=max(1, int(lookback_days)))
    left = bisect_left(historical.times, start)
    right = bisect_left(historical.times, cutoff)
    samples = list(
        zip(
            historical.abs_return_pcts[left:right],
            historical.active_ratios[left:right],
        )
    )
    matched = market_session_baseline.composition_weighted_values(
        samples,
        active_ratio,
        float(composition_tolerance),
    )
    active_reference = market_session_baseline.composition_weighted_values(
        samples,
        1.0,
        float(composition_tolerance),
    )
    matched_effective = sum(weight for _, weight in matched)
    active_effective = sum(weight for _, weight in active_reference)
    stored_matched_effective = round(matched_effective, 4)
    stored_active_effective = round(active_effective, 4)
    matched_p90 = market_session_baseline.weighted_percentile(matched, 0.90)
    active_p90 = market_session_baseline.weighted_percentile(
        active_reference, 0.90
    )
    stored_matched_p90 = _rounded(matched_p90)
    stored_active_p90 = _rounded(active_p90)
    result.update(
        {
            "prior_points": len(samples),
            "session_matched_samples": len(matched),
            "session_matched_effective_samples": stored_matched_effective,
            "active_reference_samples": len(active_reference),
            "active_reference_effective_samples": stored_active_effective,
            "session_matched_abs_return_p90_pct": stored_matched_p90,
            "active_reference_abs_return_p90_pct": stored_active_p90,
        }
    )
    minimum = max(1, int(minimum_effective_samples))
    if (
        stored_matched_effective < minimum
        or stored_active_effective < minimum
        or stored_matched_p90 is None
        or stored_matched_p90 < 0.0
        or stored_active_p90 is None
        or stored_active_p90 <= 0.0
    ):
        result["reason"] = "insufficient prior-only width calibration evidence"
        return result
    if weekend_ratio <= 1e-9:
        result["reason"] = "ACTIVE-only horizon keeps the static movement width"
        return result

    scale = round(
        min(
            1.0,
            max(0.50, float(stored_matched_p90) / float(stored_active_p90)),
        ),
        6,
    )
    result["floor_scale_factor"] = scale
    result["threshold_scale_factor"] = scale
    result["applied"] = scale < 1.0 - 1e-9
    result["reason"] = (
        "weekend/mixed width floor calibrated from prior raw price history"
        if result["applied"]
        else "session width was not below the ACTIVE reference"
    )
    return result
