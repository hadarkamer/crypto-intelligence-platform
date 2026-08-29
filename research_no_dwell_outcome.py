"""Versioned no-dwell first-touch outcome labels for canonical 1m paths.

The label answers one narrow question: did price touch a predeclared favorable
width before the horizon ended?  A touch is sufficient; there is no candle-close
or persistence requirement.  Closed one-minute OHLC cannot reveal the order of
the high and low inside one candle, so adverse movement from the qualifying
candle is conservatively included and the ambiguity is recorded explicitly.

Thresholds are frozen independently from the future path.  The default is the
static horizon floor shared with formula ranking.  An optional calibration is
accepted only when it declares prior-only provenance and an as-of timestamp no
later than the decision time.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import math
from types import MappingProxyType
from typing import Any, Dict, Iterable, Mapping, Optional


METHOD_VERSION = "no-dwell-first-touch-v6"
BASE_FAVORABLE_WIDTH_PCT_BY_HORIZON = MappingProxyType(
    {
        60: 0.50,
        240: 1.00,
        720: 1.50,
        1440: 2.00,
    }
)
MIN_THRESHOLD_SCALE_FACTOR = 0.50
MAX_THRESHOLD_SCALE_FACTOR = 1.00
_PRIOR_ONLY_SOURCE_KIND = "PRIOR_ONLY_SESSION_CALIBRATION"
_STATIC_SOURCE_KIND = "STATIC_HORIZON_FLOOR"


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _number(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _seconds_since(start: datetime, value: datetime) -> int:
    return max(0, int((_utc(value) - _utc(start)).total_seconds()))


def _first_full_minute_open(start: datetime) -> datetime:
    floor = start.replace(second=0, microsecond=0)
    return floor if start == floor else floor + timedelta(minutes=1)


def base_favorable_width_pct(horizon_minutes: int) -> float:
    """Return the single authoritative static width for a supported horizon."""
    horizon = int(horizon_minutes)
    try:
        return float(BASE_FAVORABLE_WIDTH_PCT_BY_HORIZON[horizon])
    except KeyError as exc:
        raise ValueError(f"unsupported first-touch horizon: {horizon}") from exc


def freeze_threshold_policy(
    *,
    horizon_minutes: int,
    decision_time: Any,
    prior_only_reference: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Freeze a static or demonstrably prior-only qualifying-width threshold.

    ``prior_only_reference`` must include:

    - ``source_kind='PRIOR_ONLY_SESSION_CALIBRATION'``;
    - ``as_of_utc`` no later than ``decision_time``;
    - ``threshold_scale_factor`` (or the compatible ``floor_scale_factor``)
      between 0.50 and 1.00.

    Refusing incomplete provenance is intentional: a future-derived scale must
    never be smuggled into an outcome label.
    """
    horizon = int(horizon_minutes)
    event_time = _utc(decision_time)
    base = base_favorable_width_pct(horizon)
    scale = 1.0
    source_kind = _STATIC_SOURCE_KIND
    source = "versioned static horizon floor"
    as_of_utc: Optional[datetime] = None

    if prior_only_reference is not None:
        if not isinstance(prior_only_reference, Mapping):
            raise ValueError("prior_only_reference must be a mapping")
        source_kind = str(prior_only_reference.get("source_kind") or "").upper()
        if source_kind != _PRIOR_ONLY_SOURCE_KIND:
            raise ValueError(
                "threshold calibration must declare PRIOR_ONLY_SESSION_CALIBRATION"
            )
        if prior_only_reference.get("as_of_utc") is None:
            raise ValueError("prior-only threshold calibration requires as_of_utc")
        as_of_utc = _utc(prior_only_reference["as_of_utc"])
        if as_of_utc > event_time:
            raise ValueError("threshold calibration cannot be newer than decision time")
        scale_value = prior_only_reference.get("threshold_scale_factor")
        if scale_value is None:
            scale_value = prior_only_reference.get("floor_scale_factor")
        parsed_scale = _number(scale_value)
        if parsed_scale is None or not (
            MIN_THRESHOLD_SCALE_FACTOR
            <= parsed_scale
            <= MAX_THRESHOLD_SCALE_FACTOR
        ):
            raise ValueError("threshold_scale_factor must be between 0.50 and 1.00")
        scale = float(parsed_scale)
        weekend_ratio = _number(prior_only_reference.get("session_weekend_ratio"))
        if weekend_ratio is not None and not 0.0 <= weekend_ratio <= 1.0:
            raise ValueError("session_weekend_ratio must be between 0 and 1")
        if scale < 1.0 and (weekend_ratio is None or weekend_ratio <= 0.0):
            raise ValueError(
                "threshold relaxation is allowed only for a weekend/mixed horizon"
            )
        source = str(
            prior_only_reference.get("source")
            or prior_only_reference.get("policy")
            or "prior-only session calibration"
        )

    return {
        "method_version": METHOD_VERSION,
        "horizon_minutes": horizon,
        "base_threshold_pct": round(base, 8),
        "threshold_scale_factor": round(scale, 8),
        "qualifying_move_threshold_pct": round(base * scale, 8),
        "threshold_source_kind": source_kind,
        "threshold_source": source,
        "threshold_as_of_utc": as_of_utc,
        "session_weekend_ratio": (
            _number(prior_only_reference.get("session_weekend_ratio"))
            if prior_only_reference is not None
            else 0.0
        ),
        "frozen_at_decision_time_utc": event_time,
        "lookahead_policy": "static or prior-only as-of decision time",
        "dwell_required_seconds": 0,
    }


def _validated_policy(
    policy: Mapping[str, Any], *, horizon_minutes: int, decision_time: datetime
) -> Dict[str, Any]:
    if str(policy.get("method_version") or "") != METHOD_VERSION:
        raise ValueError("first-touch threshold policy method version mismatch")
    if int(policy.get("horizon_minutes") or 0) != int(horizon_minutes):
        raise ValueError("first-touch threshold policy horizon mismatch")
    source_kind = str(policy.get("threshold_source_kind") or "").upper()
    if source_kind not in {_STATIC_SOURCE_KIND, _PRIOR_ONLY_SOURCE_KIND}:
        raise ValueError("first-touch threshold source is not lookahead-safe")
    threshold = _number(policy.get("qualifying_move_threshold_pct"))
    scale = _number(policy.get("threshold_scale_factor"))
    if threshold is None or threshold <= 0.0:
        raise ValueError("qualifying_move_threshold_pct must be positive")
    if scale is None or not (
        MIN_THRESHOLD_SCALE_FACTOR <= scale <= MAX_THRESHOLD_SCALE_FACTOR
    ):
        raise ValueError("threshold_scale_factor must be between 0.50 and 1.00")
    if source_kind == _STATIC_SOURCE_KIND and not math.isclose(
        scale, 1.0, rel_tol=0.0, abs_tol=1e-12
    ):
        raise ValueError("static first-touch threshold cannot apply a scale factor")
    expected = base_favorable_width_pct(horizon_minutes) * scale
    if not math.isclose(threshold, expected, rel_tol=0.0, abs_tol=1e-8):
        raise ValueError("first-touch threshold does not match frozen base and scale")
    if source_kind == _PRIOR_ONLY_SOURCE_KIND:
        if policy.get("threshold_as_of_utc") is None:
            raise ValueError("prior-only first-touch policy requires threshold_as_of_utc")
        if _utc(policy["threshold_as_of_utc"]) > decision_time:
            raise ValueError("first-touch threshold policy uses future calibration")
        weekend_ratio = _number(policy.get("session_weekend_ratio"))
        if weekend_ratio is not None and not 0.0 <= weekend_ratio <= 1.0:
            raise ValueError("session_weekend_ratio must be between 0 and 1")
        if scale < 1.0 and (weekend_ratio is None or weekend_ratio <= 0.0):
            raise ValueError(
                "threshold relaxation is allowed only for a weekend/mixed horizon"
            )
    return dict(policy)


def calculate_first_touch_outcome(
    *,
    reference_price: float,
    direction: str,
    event_time: Any,
    candles: Iterable[Any],
    horizon_minutes: int,
    horizon_closed: bool,
    threshold_policy: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Calculate the no-dwell first-touch result from canonical closed candles.

    ``HIT`` is final as soon as one candle reaches the frozen favorable width.
    A later reversal cannot cancel it.  Without a hit, the result is ``PENDING``
    until ``horizon_closed`` is true, then ``MISS``.  Full-horizon endpoint,
    MFE and MAE remain separate legacy diagnostics and are not read here.
    """
    reference = float(reference_price)
    if reference <= 0.0:
        raise ValueError("reference_price must be positive")
    normalized_direction = str(direction or "").strip().upper()
    if normalized_direction not in {"LONG", "SHORT"}:
        raise ValueError("first-touch direction must be LONG or SHORT")
    horizon = int(horizon_minutes)
    start = _utc(event_time)
    policy = (
        freeze_threshold_policy(
            horizon_minutes=horizon,
            decision_time=start,
        )
        if threshold_policy is None
        else _validated_policy(
            threshold_policy,
            horizon_minutes=horizon,
            decision_time=start,
        )
    )
    threshold_pct = float(policy["qualifying_move_threshold_pct"])
    favorable_price = (
        reference * (1.0 + threshold_pct / 100.0)
        if normalized_direction == "LONG"
        else reference * (1.0 - threshold_pct / 100.0)
    )
    first_open = _first_full_minute_open(start)
    horizon_end = start + timedelta(minutes=horizon)
    path = sorted(
        (
            candle
            for candle in candles
            if _utc(candle.open_time_utc) >= first_open
            and _utc(candle.close_time_utc) <= horizon_end
        ),
        key=lambda candle: _utc(candle.open_time_utc),
    )
    if not path:
        raise ValueError("at least one closed candle is required")
    adverse_price = reference

    for candle in path:
        close_time = _utc(candle.close_time_utc)
        if close_time > horizon_end:
            continue
        high = float(candle.high)
        low = float(candle.low)
        if normalized_direction == "LONG":
            adverse_price = min(adverse_price, low)
            qualified = high >= favorable_price
            candle_adverse_pct = max(0.0, (reference - low) / reference * 100.0)
        else:
            adverse_price = max(adverse_price, high)
            qualified = low <= favorable_price
            candle_adverse_pct = max(0.0, (high - reference) / reference * 100.0)
        if not qualified:
            continue
        pre_qualifying_mae_pct = (
            max(0.0, (reference - adverse_price) / reference * 100.0)
            if normalized_direction == "LONG"
            else max(0.0, (adverse_price - reference) / reference * 100.0)
        )
        return {
            "method_version": METHOD_VERSION,
            "status": "HIT",
            "success": True,
            "failure_final": False,
            "direction": normalized_direction,
            "horizon_minutes": horizon,
            "observed_through_utc": close_time,
            "first_qualifying_move_time_utc": close_time,
            "time_to_first_qualifying_move_seconds": _seconds_since(start, close_time),
            "qualifying_move_price": favorable_price,
            "qualifying_move_threshold_pct": threshold_pct,
            "threshold_scale_factor": float(policy["threshold_scale_factor"]),
            "threshold_source_kind": policy["threshold_source_kind"],
            "threshold_source": policy["threshold_source"],
            "threshold_policy": policy,
            "pre_qualifying_mae_pct": pre_qualifying_mae_pct,
            "qualifying_candle_adverse_excursion_pct": candle_adverse_pct,
            "qualifying_candle_order_ambiguous": candle_adverse_pct > 0.0,
            "dwell_required_seconds": 0,
            "post_hit_reversal_policy": "ignored_for_success",
        }

    observed_through = _utc(path[-1].close_time_utc)
    pre_qualifying_mae_pct = (
        max(0.0, (reference - adverse_price) / reference * 100.0)
        if normalized_direction == "LONG"
        else max(0.0, (adverse_price - reference) / reference * 100.0)
    )
    status = "MISS" if horizon_closed else "PENDING"
    return {
        "method_version": METHOD_VERSION,
        "status": status,
        "success": False if horizon_closed else None,
        "failure_final": bool(horizon_closed),
        "direction": normalized_direction,
        "horizon_minutes": horizon,
        "observed_through_utc": observed_through,
        "first_qualifying_move_time_utc": None,
        "time_to_first_qualifying_move_seconds": None,
        "qualifying_move_price": favorable_price,
        "qualifying_move_threshold_pct": threshold_pct,
        "threshold_scale_factor": float(policy["threshold_scale_factor"]),
        "threshold_source_kind": policy["threshold_source_kind"],
        "threshold_source": policy["threshold_source"],
        "threshold_policy": policy,
        "pre_qualifying_mae_pct": pre_qualifying_mae_pct,
        "qualifying_candle_adverse_excursion_pct": None,
        "qualifying_candle_order_ambiguous": False,
        "dwell_required_seconds": 0,
        "post_hit_reversal_policy": "ignored_for_success",
    }
