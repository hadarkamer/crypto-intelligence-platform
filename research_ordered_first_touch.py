"""Pure ordered two-barrier First Touch labels for Research v7.

Unlike the retained v6 label, this contract compares a favorable threshold
with the symmetric adverse threshold and records which one was observed first.
Closed one-minute OHLC cannot order two barriers crossed inside the same candle,
so that case is explicitly unresolved instead of being guessed.  Likewise, a
missing candle fails closed as ``DATA_MISSING``.  The intentionally unobserved
partial minute immediately after a non-aligned decision is disclosed, while
labels apply to the fully observed post-gap 1m path.

This module has no database, network, or production-alert side effects.  The v6
calculator remains available separately for historical audit compatibility.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import math
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence


METHOD_VERSION = "ordered-first-touch-v7"
CANDLE_INTERVAL_SECONDS = 60
SUPPORTED_THRESHOLDS_PCT = (
    0.25,
    0.50,
    0.75,
    1.00,
    1.25,
    1.50,
    1.75,
    2.00,
)

_STATUS_OPEN = "OPEN"
_STATUS_SUCCESS = "SUCCESS"
_STATUS_FAILURE = "FAILURE"
_STATUS_UNRESOLVED = "UNRESOLVED"
_STATUS_DATA_MISSING = "DATA_MISSING"

_SIDE_NONE = "NONE"
_SIDE_FAVORABLE = "FAVORABLE"
_SIDE_ADVERSE = "ADVERSE"
_SIDE_AMBIGUOUS = "AMBIGUOUS"


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _field(candle: Any, name: str) -> Any:
    if isinstance(candle, Mapping):
        return candle[name]
    return getattr(candle, name)


def _finite_number(value: Any, *, name: str) -> float:
    if value is None or isinstance(value, bool):
        raise ValueError(f"{name} must be a finite number")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a finite number") from exc
    if not math.isfinite(number):
        raise ValueError(f"{name} must be a finite number")
    return number


def _threshold(value: Any) -> tuple[float, int]:
    threshold = _finite_number(value, name="threshold_pct")
    for supported in SUPPORTED_THRESHOLDS_PCT:
        if math.isclose(threshold, supported, rel_tol=0.0, abs_tol=1e-12):
            return float(supported), int(round(supported * 100.0))
    values = ", ".join(f"{item:.2f}" for item in SUPPORTED_THRESHOLDS_PCT)
    raise ValueError(f"threshold_pct must be one of: {values}")


def _first_full_minute_open(start: datetime) -> datetime:
    floor = start.replace(second=0, microsecond=0)
    return floor if start == floor else floor + timedelta(minutes=1)


def _seconds_since(start: datetime, value: datetime) -> int:
    return max(0, int((_utc(value) - _utc(start)).total_seconds()))


def _initial_gap(start: datetime, first_open: datetime) -> tuple[int, bool]:
    raw_seconds = max(0.0, (_utc(first_open) - _utc(start)).total_seconds())
    if raw_seconds <= 0.0:
        return 0, False
    # Storage uses whole seconds and reserves zero for a truly aligned start.
    # Truncation is intentional, but a positive sub-second gap remains visible.
    return max(1, int(raw_seconds)), True


def _prepared_candles(
    candles: Iterable[Any], *, event_time: datetime
) -> tuple[list[Dict[str, Any]], datetime, bool]:
    """Normalize eligible closed candles and detect any missing 1m prefix."""
    expected_first_open = _first_full_minute_open(event_time)
    normalized: list[Dict[str, Any]] = []
    for candle in candles:
        open_time = _utc(_field(candle, "open_time_utc"))
        close_time = _utc(_field(candle, "close_time_utc"))
        if open_time < expected_first_open:
            # The overlapping decision candle contains pre-decision movement
            # and is never eligible for ordered First Touch.
            continue
        open_price = _finite_number(_field(candle, "open"), name="candle.open")
        high = _finite_number(_field(candle, "high"), name="candle.high")
        low = _finite_number(_field(candle, "low"), name="candle.low")
        close_price = _finite_number(_field(candle, "close"), name="candle.close")
        if min(open_price, high, low, close_price) <= 0.0:
            raise ValueError("candle prices must be positive")
        if high < max(open_price, low, close_price) or low > min(
            open_price, high, close_price
        ):
            raise ValueError("candle OHLC values are inconsistent")
        if close_time <= open_time:
            raise ValueError("candle close time must be after its open time")
        normalized.append(
            {
                "open_time_utc": open_time,
                "close_time_utc": close_time,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close_price,
            }
        )

    normalized.sort(key=lambda item: item["open_time_utc"])
    contiguous = True
    if normalized and normalized[0]["open_time_utc"] != expected_first_open:
        contiguous = False
    for previous, current in zip(normalized, normalized[1:]):
        if current["open_time_utc"] - previous["open_time_utc"] != timedelta(
            seconds=CANDLE_INTERVAL_SECONDS
        ):
            contiguous = False
            break
    return normalized, expected_first_open, contiguous


def _barriers(
    *, reference_price: float, direction: str, threshold_pct: float
) -> tuple[float, float]:
    width = threshold_pct / 100.0
    if direction == "LONG":
        return reference_price * (1.0 + width), reference_price * (1.0 - width)
    return reference_price * (1.0 - width), reference_price * (1.0 + width)


def _crossed(
    candle: Mapping[str, Any],
    *,
    direction: str,
    favorable_barrier: float,
    adverse_barrier: float,
) -> tuple[bool, bool]:
    high = float(candle["high"])
    low = float(candle["low"])
    if direction == "LONG":
        return high >= favorable_barrier, low <= adverse_barrier
    return low <= favorable_barrier, high >= adverse_barrier


def _excursion_metrics(
    *,
    reference_price: float,
    direction: str,
    maximum: float,
    minimum: float,
) -> Dict[str, float]:
    if direction == "LONG":
        max_favorable_price = maximum
        max_adverse_price = minimum
        mfe_pct = max(0.0, (maximum - reference_price) / reference_price * 100.0)
        mae_pct = max(0.0, (reference_price - minimum) / reference_price * 100.0)
    else:
        max_favorable_price = minimum
        max_adverse_price = maximum
        mfe_pct = max(0.0, (reference_price - minimum) / reference_price * 100.0)
        mae_pct = max(0.0, (maximum - reference_price) / reference_price * 100.0)
    return {
        "max_favorable_price": max_favorable_price,
        "max_adverse_price": max_adverse_price,
        "mfe_pct": mfe_pct,
        "mae_pct": mae_pct,
    }


def calculate_ordered_first_touch_outcome(
    *,
    reference_price: float,
    direction: str,
    event_time: Any,
    candles: Iterable[Any],
    threshold_pct: float,
    observation_closed: bool,
    path_complete: bool = True,
) -> Dict[str, Any]:
    """Return one ordered symmetric-barrier label for a fixed threshold.

    ``path_complete`` means that every eligible closed 1m candle through the
    caller's current observation cutoff is present.  It is distinct from
    ``observation_closed``: a complete prefix can still belong to an open
    observation window.

    A non-minute-aligned decision leaves an intentionally unobserved initial
    partial minute.  This is disclosed in the result, while the ordered label
    applies to the fully observed closed-1m path beginning at the next minute.
    """
    reference = _finite_number(reference_price, name="reference_price")
    if reference <= 0.0:
        raise ValueError("reference_price must be positive")
    normalized_direction = str(direction or "").strip().upper()
    if normalized_direction not in {"LONG", "SHORT"}:
        raise ValueError("direction must be LONG or SHORT")
    if not isinstance(observation_closed, bool):
        raise ValueError("observation_closed must be a boolean")
    if not isinstance(path_complete, bool):
        raise ValueError("path_complete must be a boolean")
    threshold, threshold_bps = _threshold(threshold_pct)
    start = _utc(event_time)
    path, expected_first_open, internally_contiguous = _prepared_candles(
        candles, event_time=start
    )
    initial_gap_seconds, initial_gap_unobserved = _initial_gap(
        start, expected_first_open
    )
    effective_path_complete = bool(path_complete and internally_contiguous)
    favorable_barrier, adverse_barrier = _barriers(
        reference_price=reference,
        direction=normalized_direction,
        threshold_pct=threshold,
    )

    maximum = reference
    minimum = reference
    decision: Optional[Dict[str, Any]] = None
    processed = 0
    for candle in path:
        processed += 1
        maximum = max(maximum, float(candle["high"]))
        minimum = min(minimum, float(candle["low"]))
        favorable_crossed, adverse_crossed = _crossed(
            candle,
            direction=normalized_direction,
            favorable_barrier=favorable_barrier,
            adverse_barrier=adverse_barrier,
        )
        if not favorable_crossed and not adverse_crossed:
            continue
        if favorable_crossed and adverse_crossed:
            decision = {
                "status": _STATUS_UNRESOLVED,
                "first_touch_side": _SIDE_AMBIGUOUS,
                "terminal_reason": "SAME_CANDLE_BOTH",
                "success": None,
                "favorable_touch_price": favorable_barrier,
                "adverse_touch_price": adverse_barrier,
                "decision_time_utc": candle["close_time_utc"],
            }
        elif favorable_crossed:
            decision = {
                "status": _STATUS_SUCCESS,
                "first_touch_side": _SIDE_FAVORABLE,
                "terminal_reason": "FAVORABLE_FIRST",
                "success": True,
                "favorable_touch_price": favorable_barrier,
                "adverse_touch_price": None,
                "decision_time_utc": candle["close_time_utc"],
            }
        else:
            decision = {
                "status": _STATUS_FAILURE,
                "first_touch_side": _SIDE_ADVERSE,
                "terminal_reason": "ADVERSE_FIRST",
                "success": False,
                "favorable_touch_price": None,
                "adverse_touch_price": adverse_barrier,
                "decision_time_utc": candle["close_time_utc"],
            }
        if effective_path_complete:
            break
        # A result from an incomplete path is never terminal evidence. Keep
        # scanning solely so the diagnostic extrema describe every available
        # candle in the current path.
        decision = None

    last_observed = path[processed - 1]["close_time_utc"] if processed else start
    metrics = _excursion_metrics(
        reference_price=reference,
        direction=normalized_direction,
        maximum=maximum,
        minimum=minimum,
    )

    if not effective_path_complete:
        status = _STATUS_DATA_MISSING
        side = _SIDE_NONE
        terminal_reason = "INCOMPLETE_PATH"
        success: Optional[bool] = None
        decision_time = None
        favorable_touch_price = None
        adverse_touch_price = None
    elif decision is not None:
        status = str(decision["status"])
        side = str(decision["first_touch_side"])
        terminal_reason = str(decision["terminal_reason"])
        success = decision["success"]
        decision_time = _utc(decision["decision_time_utc"])
        favorable_touch_price = decision["favorable_touch_price"]
        adverse_touch_price = decision["adverse_touch_price"]
    elif observation_closed:
        status = _STATUS_UNRESOLVED
        side = _SIDE_NONE
        terminal_reason = "OBSERVATION_WINDOW_CLOSED_NO_TOUCH"
        success = None
        decision_time = None
        favorable_touch_price = None
        adverse_touch_price = None
    else:
        status = _STATUS_OPEN
        side = _SIDE_NONE
        terminal_reason = None
        success = None
        decision_time = None
        favorable_touch_price = None
        adverse_touch_price = None

    observed_from = path[0]["open_time_utc"] if path else None
    data_quality_note = (
        "INCOMPLETE_CLOSED_1M_PATH"
        if not effective_path_complete
        else "COMPLETE_POST_GAP_CLOSED_1M_PATH"
        if initial_gap_unobserved
        else "COMPLETE_CLOSED_1M_PATH"
    )
    return {
        "method_version": METHOD_VERSION,
        "threshold_pct": threshold,
        "threshold_bps": threshold_bps,
        "reference_price": reference,
        "direction": normalized_direction,
        "status": status,
        "first_touch_side": side,
        "terminal_reason": terminal_reason,
        "success": success,
        "decision_time_utc": decision_time,
        "outcome_decided_at_utc": decision_time,
        "time_to_decision_seconds": (
            _seconds_since(start, decision_time) if decision_time is not None else None
        ),
        "favorable_barrier_price": favorable_barrier,
        "adverse_barrier_price": adverse_barrier,
        "favorable_touch_price": favorable_touch_price,
        "adverse_touch_price": adverse_touch_price,
        "measurement_start_utc": start,
        "first_observed_open_utc": observed_from,
        "observed_from_utc": observed_from,
        "observed_through_utc": last_observed,
        "initial_gap_seconds": initial_gap_seconds,
        "initial_gap_unobserved": initial_gap_unobserved,
        "data_quality_note": data_quality_note,
        **metrics,
        "path_samples": processed,
        "path_complete": effective_path_complete,
        "input_path_complete": path_complete,
        "observation_closed": observation_closed,
        "candle_interval_seconds": CANDLE_INTERVAL_SECONDS,
    }


def calculate_all_ordered_first_touch_outcomes(
    *,
    reference_price: float,
    direction: str,
    event_time: Any,
    candles: Iterable[Any],
    observation_closed: bool,
    path_complete: bool = True,
    thresholds_pct: Sequence[float] = SUPPORTED_THRESHOLDS_PCT,
) -> list[Dict[str, Any]]:
    """Return ordered labels for all requested fixed thresholds."""
    frozen_path = list(candles)
    return [
        calculate_ordered_first_touch_outcome(
            reference_price=reference_price,
            direction=direction,
            event_time=event_time,
            candles=frozen_path,
            threshold_pct=threshold,
            observation_closed=observation_closed,
            path_complete=path_complete,
        )
        for threshold in thresholds_pct
    ]
