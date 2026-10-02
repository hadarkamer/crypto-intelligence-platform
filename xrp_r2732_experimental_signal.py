"""Frozen XRP R2732 profit-lock signal, restricted to New York weekdays.

This is the distance-only R2732 rule, not U21. There is no BTC condition,
session-location condition, or minimum-width filter. The shared NYSE calendar
is the same audited 2025/2026 calendar used by the historical replay. Only
completed XRP Binance Spot minutes enter the predicate; D+1 OPEN is observed
separately by the worker after the decision has been made.
"""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from u21_experimental_signal import (
    MINUTE_MS, DECISION_STEP_MS, SUPPORTED_CALENDAR_YEARS,
    _ms, _positive, _normalize, _complete,
    last_closed_decision_ms, previous_ny_regular_session, nonoverlap_allows,
)

FORMULA_ID = "RG_XRP_SHORT_g11_R2732__lock05_at2__ny_weekdays"
FORMULA_VERSION = "xrp-r2732-ny-weekdays-lock05-at2-cap1-v1"
DISTANCE_LOWER = -0.017415876858352997
DISTANCE_UPPER = -0.004158176397388663
_NY = ZoneInfo("America/New_York")


def _decision(value):
    d = _ms(value)
    if d % DECISION_STEP_MS:
        raise ValueError("Decision is not on the UTC 15-minute grid")
    local = datetime.fromtimestamp(d / 1000, timezone.utc).astimezone(_NY)
    if local.year not in SUPPORTED_CALENDAR_YEARS:
        raise ValueError("Unsupported NYSE calendar year")
    return d, local


def required_history_start_ms(decision_ms):
    """Only XRP is needed, beginning at the most recent completed NY session."""
    d, _ = _decision(decision_ms)
    start, _ = previous_ny_regular_session(d)
    return {"XRP": start}


def predicate_values(last_closed_price, session_high):
    """Preserve the exact double-precision expression and frozen inequalities."""
    close, high = map(_positive, (last_closed_price, session_high))
    distance = close / high - 1.0
    return {"distance": distance,
            "signal": DISTANCE_LOWER < distance <= DISTANCE_UPPER}


def evaluate_signal(xrp_bars, decision_ms):
    """Evaluate the causal weekday predicate, without inheriting U21's gates.

    Required evidence is the complete prior session plus minute D-1. Bars
    between those windows do not define this formula and need not be present.
    A NY holiday is still a weekday: it may qualify using the preceding session.
    Weekends block new decisions; they do not end already-active observations.
    """
    result = {"formula_id": FORMULA_ID, "version": FORMULA_VERSION,
              "valid": False, "signal": False}
    try:
        d, local = _decision(decision_ms)
        result.update(decision_ms=d, entry_ms=d + MINUTE_MS,
                      ny_weekday=local.weekday(), ny_decision_date=local.date().isoformat())
        if local.weekday() >= 5:
            result.update(valid=True, reason="ny_weekend")
            return result
        start, end = previous_ny_regular_session(d)
        rows = _normalize(xrp_bars, start, d)
        session = [row for row in rows if row[0] < end]
        if not _complete(session, start, end):
            result["reason"] = "incomplete_prior_ny_session"
            return result
        if not rows or rows[-1][0] != d - MINUTE_MS:
            result["reason"] = "missing_last_closed_minute"
            return result
        high = max(row[2] for row in session)
        values = predicate_values(rows[-1][4], high)
        result.update(values, valid=True, reason="match" if values["signal"] else "no_match",
                      last_closed_price=rows[-1][4], last_closed_minute_ms=d - MINUTE_MS,
                      prior_ny_start_ms=start, prior_ny_end_ms=end,
                      prior_ny_high=high, reference_age_h=(d-end)/3600000)
        return result
    except (ValueError, TypeError, IndexError, KeyError, OverflowError) as exc:
        result["reason"] = "invalid_input: " + str(exc)
        return result


def build_levels(entry_price):
    """Price geometry matches DynamicPath's original-risk arithmetic exactly.

    The monitor first checks the old stop/TP, then promotes only when the
    completed candle's (entry-low)/original_risk >= 2. The new stop is effective
    from the following minute; it is never applied to its trigger candle.
    """
    entry = _positive(entry_price)
    stop = entry * 1.005
    risk = abs(entry - stop)
    if risk <= 0:
        raise ValueError("Entry has no representable positive risk distance")
    return {"direction": "SHORT", "entry_price": entry, "stop_loss": stop,
            "take_profit": entry * .92, "risk_fraction": .005,
            "take_fraction": .08, "reward_risk": 16.0,
            "original_risk_distance": risk,
            "lock_trigger_price": entry - 2 * risk,
            "locked_stop_loss": entry - .5 * risk,
            "lock_trigger_R": 2.0, "lock_profit_R": .5}
