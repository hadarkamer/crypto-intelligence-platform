"""Active-vs-Weekend baseline utilities.

Session definition is evaluated in ``America/New_York`` so DST changes are
handled by the timezone database:
- ACTIVE: Sunday 18:00 ET through Friday 20:00 ET
- WEEKEND: Friday 20:00 ET through Sunday 18:00 ET

Historical baselines are selected by similarity to the current window's exact
ACTIVE/WEEKEND composition.  Values are never split fractionally between two
session distributions and percentiles are never blended linearly.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo
import math

NEW_YORK = ZoneInfo("America/New_York")
DEFAULT_COMPOSITION_TOLERANCE = 0.25
COINGLASS_CANDLE_INTERVAL_MINUTES = 30
COINGLASS_CANDLE_GRACE_MINUTES = 2


def as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def is_active_market(moment: datetime) -> bool:
    local = as_utc(moment).astimezone(NEW_YORK)
    weekday = local.weekday()  # Monday=0 ... Sunday=6
    minutes = local.hour * 60 + local.minute
    if weekday <= 3:
        return True
    if weekday == 4:
        return minutes < 20 * 60
    if weekday == 5:
        return False
    return minutes >= 18 * 60


def market_time_features(moment: datetime) -> dict:
    """Return DST-safe New-York-local time features for research.

    Raw UTC-hour thresholds drift by one hour when New York changes between
    standard and daylight-saving time.  Formula discovery therefore receives
    local labels and stable buckets, while UTC remains available only as audit
    metadata in the feature matrix.
    """
    local = as_utc(moment).astimezone(NEW_YORK)
    minutes = local.hour * 60 + local.minute
    if minutes < 6 * 60:
        bucket = "ET_00_05_OVERNIGHT"
    elif minutes < 9 * 60 + 30:
        bucket = "ET_06_09_PRE_US"
    elif minutes < 16 * 60:
        bucket = "ET_09_15_US_CASH"
    elif minutes < 20 * 60:
        bucket = "ET_16_19_AFTER_US"
    else:
        bucket = "ET_20_23_LATE"
    return {
        "market_local_hour": local.hour,
        "market_local_minute": local.minute,
        "market_local_weekday": local.weekday(),
        "market_local_weekday_name": local.strftime("%A").upper(),
        "market_time_bucket": bucket,
        "market_utc_offset_minutes": int(
            (local.utcoffset() or timedelta(0)).total_seconds() / 60
        ),
    }


def closed_candle_available_at(
    candle_time: datetime,
    *,
    interval_minutes: int = COINGLASS_CANDLE_INTERVAL_MINUTES,
    grace_minutes: int = COINGLASS_CANDLE_GRACE_MINUTES,
) -> datetime:
    """Return the earliest safe decision time for an interval-open candle."""
    return as_utc(candle_time) + timedelta(
        minutes=max(1, int(interval_minutes)) + max(0, int(grace_minutes))
    )


def _session_boundaries(start: datetime, end: datetime) -> List[datetime]:
    """Return all exact Friday-20:00 / Sunday-18:00 boundaries in UTC."""
    start_utc = as_utc(start)
    end_utc = as_utc(end)
    local_start = start_utc.astimezone(NEW_YORK).date() - timedelta(days=8)
    local_end = end_utc.astimezone(NEW_YORK).date() + timedelta(days=8)
    result: List[datetime] = []
    current = local_start
    while current <= local_end:
        weekday = current.weekday()
        if weekday == 4:
            local_dt = datetime(current.year, current.month, current.day, 20, 0, tzinfo=NEW_YORK)
            utc_dt = local_dt.astimezone(timezone.utc)
            if start_utc < utc_dt < end_utc:
                result.append(utc_dt)
        elif weekday == 6:
            local_dt = datetime(current.year, current.month, current.day, 18, 0, tzinfo=NEW_YORK)
            utc_dt = local_dt.astimezone(timezone.utc)
            if start_utc < utc_dt < end_utc:
                result.append(utc_dt)
        current += timedelta(days=1)
    return sorted(set(result))


def session_ratios(start: datetime, end: datetime, step_minutes: int = 30) -> Tuple[float, float, int]:
    """Return exact ACTIVE/WEEKEND ratios for ``[start, end)``.

    The interval is split at real session boundaries rather than sampled by a
    midpoint.  ``step_minutes`` remains accepted for API compatibility but is
    intentionally not used to approximate the result.
    """
    del step_minutes
    start_utc = as_utc(start)
    end_utc = as_utc(end)
    if end_utc <= start_utc:
        return 1.0, 0.0, 0

    points = [start_utc, *_session_boundaries(start_utc, end_utc), end_utc]
    active_seconds = 0.0
    weekend_seconds = 0.0
    for left, right in zip(points, points[1:]):
        seconds = (right - left).total_seconds()
        if seconds <= 0:
            continue
        midpoint = left + (right - left) / 2
        if is_active_market(midpoint):
            active_seconds += seconds
        else:
            weekend_seconds += seconds

    total = active_seconds + weekend_seconds
    if total <= 0:
        return 1.0, 0.0, max(0, len(points) - 1)
    diagnostic_segments = max(1, int(math.ceil(total / (30 * 60))))
    return active_seconds / total, weekend_seconds / total, diagnostic_segments


def weighted_percentile(values_and_weights: Sequence[Tuple[float, float]], q: float) -> Optional[float]:
    """Linearly interpolated weighted percentile.

    For equal weights this is exactly the ordinary ``(n-1)*q`` linear
    percentile used elsewhere in the project.  Unequal weights use monotonic
    weighted plotting positions and linear interpolation.
    """
    cleaned = sorted(
        (float(value), float(weight))
        for value, weight in values_and_weights
        if math.isfinite(float(value)) and math.isfinite(float(weight)) and float(weight) > 0
    )
    if not cleaned:
        return None
    if len(cleaned) == 1:
        return cleaned[0][0]

    q = min(max(float(q), 0.0), 1.0)
    weights = [weight for _, weight in cleaned]
    if max(weights) - min(weights) <= 1e-12:
        position = (len(cleaned) - 1) * q
        lower = int(math.floor(position))
        upper = int(math.ceil(position))
        if lower == upper:
            return cleaned[lower][0]
        fraction = position - lower
        return cleaned[lower][0] * (1.0 - fraction) + cleaned[upper][0] * fraction

    total = sum(weights)
    # Centers of each weighted observation, rescaled so first=0 and last=1.
    centers: List[float] = []
    cumulative = 0.0
    for _, weight in cleaned:
        centers.append(cumulative + weight / 2.0)
        cumulative += weight
    low = centers[0]
    high = centers[-1]
    if high <= low:
        return cleaned[0][0]
    positions = [(center - low) / (high - low) for center in centers]
    if q <= positions[0]:
        return cleaned[0][0]
    if q >= positions[-1]:
        return cleaned[-1][0]
    for idx in range(1, len(cleaned)):
        if q <= positions[idx]:
            left_p, right_p = positions[idx - 1], positions[idx]
            left_v, right_v = cleaned[idx - 1][0], cleaned[idx][0]
            if right_p <= left_p:
                return right_v
            fraction = (q - left_p) / (right_p - left_p)
            return left_v * (1.0 - fraction) + right_v * fraction
    return cleaned[-1][0]


def composition_weight(current_active_ratio: float, historical_active_ratio: float, tolerance: float = DEFAULT_COMPOSITION_TOLERANCE) -> float:
    """Triangular similarity weight for two window compositions."""
    tolerance = max(float(tolerance), 1e-9)
    difference = abs(float(current_active_ratio) - float(historical_active_ratio))
    return max(0.0, 1.0 - difference / tolerance)


def composition_weighted_values(
    samples: Iterable[Tuple[float, float]],
    current_active_ratio: float,
    tolerance: float = DEFAULT_COMPOSITION_TOLERANCE,
) -> List[Tuple[float, float]]:
    result: List[Tuple[float, float]] = []
    for value, historical_active_ratio in samples:
        weight = composition_weight(current_active_ratio, historical_active_ratio, tolerance)
        if weight > 0 and math.isfinite(float(value)):
            result.append((float(value), weight))
    return result


def is_closed_candle(
    candle_time: datetime,
    now: Optional[datetime] = None,
    interval_minutes: int = 30,
    grace_minutes: int = 2,
) -> bool:
    """Return True only after a candle that starts at ``candle_time`` closed.

    CoinGlass history timestamps are treated as interval-open timestamps.  A
    small grace period protects against provider finalization latency.
    """
    current = as_utc(now or datetime.now(timezone.utc))
    return closed_candle_available_at(
        candle_time,
        interval_minutes=interval_minutes,
        grace_minutes=grace_minutes,
    ) <= current


def blend_values(active_value: Optional[float], weekend_value: Optional[float], active_ratio: float, weekend_ratio: float, fallback: Optional[float] = None) -> Optional[float]:
    """Backward-compatible diagnostic helper.

    Production baselines no longer blend percentiles with this function; it is
    retained only for old diagnostics/tests and external callers.
    """
    if active_value is None:
        active_value = fallback
    if weekend_value is None:
        weekend_value = fallback
    if active_value is None and weekend_value is None:
        return None
    if active_value is None:
        return float(weekend_value)
    if weekend_value is None:
        return float(active_value)
    total = max(0.0, active_ratio) + max(0.0, weekend_ratio)
    if total <= 0:
        return fallback if fallback is not None else float(active_value)
    return (float(active_value) * max(0.0, active_ratio) + float(weekend_value) * max(0.0, weekend_ratio)) / total
