"""Causal, globally shared BTC parent movements for research evidence.

This is a new engineering policy, not a claim that a 2% reversal is a user
specified or statistically optimized constant. Two percent is deliberately the
largest researched target width: smaller zigzags cannot manufacture repeated
independent proof. All assets, formulas, directions and horizons share these
parents. The boundary is the close that confirms a reversal, never a past pivot.
There is no fixed duration, dwell, survival or return-to-entry requirement.

The incomplete left edge and every data discontinuity start an ineligible
segment. A subsequent fully observed 2% reversal supplies the first eligible
boundary. This is an operational independence policy, not a proof that separate
market movements are probabilistically independent.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import math
from typing import Any, Mapping, Sequence


POLICY_VERSION = "btc-parent-close-reversal-200bps-v1"
REVERSAL_BPS = 200
SOURCE = "BINANCE_SPOT_BTCUSDT_1M"
MINUTE = timedelta(minutes=1)
MILLISECOND = timedelta(milliseconds=1)


def utc(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def validate_candle(candle: Any) -> dict:
    value = candle.to_dict() if hasattr(candle, "to_dict") else dict(candle)
    opened, closed = utc(value["open_time_utc"]), utc(value["close_time_utc"])
    if opened.second or opened.microsecond or closed != opened + MINUTE - MILLISECOND:
        raise ValueError("BTC parent source must be a complete UTC one-minute bar")
    result = {"open_time_utc": opened, "close_time_utc": closed}
    for name in ("open", "high", "low", "close"):
        if isinstance(value.get(name), bool):
            raise ValueError("BTC source prices must be finite positive numbers")
        number = float(value[name])
        if not math.isfinite(number) or number <= 0:
            raise ValueError("BTC source prices must be finite positive numbers")
        result[name] = number
    if result["high"] < max(result["open"], result["close"], result["low"]):
        raise ValueError("BTC source high is inconsistent")
    if result["low"] > min(result["open"], result["close"], result["high"]):
        raise ValueError("BTC source low is inconsistent")
    return result


def _identity(start: datetime) -> str:
    return hashlib.sha256(f"{POLICY_VERSION}|{SOURCE}|{start.isoformat()}".encode()).hexdigest()


def _new_parent(bar: Mapping, *, direction: str, eligible: bool, reason: str) -> dict:
    close = float(bar["close"])
    return {
        "btc_parent_movement_id": _identity(bar["close_time_utc"]),
        "episode_policy_version": POLICY_VERSION,
        "start_time_utc": bar["close_time_utc"],
        "end_time_utc": None,
        "confirmed_at_utc": bar["close_time_utc"] if eligible else None,
        "direction": direction,
        "evidence_eligible": eligible,
        "boundary_reason": reason,
        "observed_through_utc": bar["close_time_utc"],
        "price_source": SOURCE,
        "state_json": {
            "seed_close": close,
            "extreme_close": close,
            "last_open_time_utc": bar["open_time_utc"].isoformat(),
            "last_close": close,
            "reversal_bps": REVERSAL_BPS,
        },
    }


def advance_parents(candles: Sequence[Any], *, previous: Mapping | None = None,
                    as_of_utc: Any) -> list[dict]:
    """Return changed parents, including the final resumable checkpoint.

    Input must be chronological and non-overlapping. No future candles are
    accepted. A gap is an explicit unknown interval, not a new eligible wave.
    Resume from the last returned parent; splitting a stream never changes IDs.
    """
    cutoff = utc(as_of_utc)
    current = dict(previous) if previous else None
    if current:
        if current.get("episode_policy_version") != POLICY_VERSION or current.get("end_time_utc"):
            raise ValueError("checkpoint must be the open parent for this exact policy")
        current["state_json"] = dict(current["state_json"])
        current["observed_through_utc"] = utc(current["observed_through_utc"])
    changed: dict[str, dict] = {}
    for raw in candles:
        bar = validate_candle(raw)
        if bar["close_time_utc"] > cutoff:
            raise ValueError("future BTC candle cannot establish a decision boundary")
        if current:
            previous_open = utc(current["state_json"]["last_open_time_utc"])
            if bar["open_time_utc"] <= previous_open:
                raise ValueError("BTC candles must advance strictly without duplicates")
            if bar["open_time_utc"] != previous_open + MINUTE:
                current["end_time_utc"] = current["observed_through_utc"] + MINUTE
                changed[current["btc_parent_movement_id"]] = current
                current = _new_parent(bar, direction="UNKNOWN", eligible=False,
                                      reason="BTC_DATA_GAP")
                changed[current["btc_parent_movement_id"]] = current
                continue
        else:
            current = _new_parent(bar, direction="UNKNOWN", eligible=False,
                                  reason="LEFT_BOUNDARY_UNVERIFIED")
            changed[current["btc_parent_movement_id"]] = current
            continue
        state = current["state_json"]
        price, extreme = float(bar["close"]), float(state["extreme_close"])
        reversal = REVERSAL_BPS / 10000.0
        direction = str(current["direction"])
        next_direction = None
        if direction == "UNKNOWN":
            seed = float(state["seed_close"])
            if price >= seed * (1 + reversal):
                current["direction"] = "UP"
                state["extreme_close"] = price
            elif price <= seed * (1 - reversal):
                current["direction"] = "DOWN"
                state["extreme_close"] = price
        elif direction == "UP":
            if price <= extreme * (1 - reversal):
                next_direction = "DOWN"
            else:
                state["extreme_close"] = max(price, extreme)
        elif direction == "DOWN":
            if price >= extreme * (1 + reversal):
                next_direction = "UP"
            else:
                state["extreme_close"] = min(price, extreme)
        else:
            raise ValueError("invalid BTC parent checkpoint direction")
        if next_direction:
            current["end_time_utc"] = bar["close_time_utc"]
            changed[current["btc_parent_movement_id"]] = current
            current = _new_parent(bar, direction=next_direction, eligible=True,
                                  reason="CAUSAL_CLOSE_REVERSAL")
        else:
            current["observed_through_utc"] = bar["close_time_utc"]
            state["last_open_time_utc"] = bar["open_time_utc"].isoformat()
            state["last_close"] = price
        changed[current["btc_parent_movement_id"]] = current
    return list(changed.values())


def membership(event: Mapping, *, parent: Mapping | None,
               btc_bar: Mapping | None) -> dict:
    """Assign only information observable at the event's own decision time."""
    decision = utc(event["alert_time_utc"])
    result = {
        "event_id": int(event["event_id"]),
        "episode_policy_version": POLICY_VERSION,
        "decision_time_utc": decision,
        "btc_parent_movement_id": None,
        "btc_observed_close_utc": None,
        "membership_status": "BTC_DATA_MISSING",
    }
    if not parent or not btc_bar:
        return result
    closed = utc(btc_bar["close_time_utc"])
    if not timedelta(0) <= decision - closed < MINUTE:
        return result
    if parent.get("episode_policy_version") != POLICY_VERSION:
        return result
    start = utc(parent["start_time_utc"])
    end = utc(parent["end_time_utc"]) if parent.get("end_time_utc") else None
    if decision < start or (end and decision >= end) or closed < start:
        return result
    result["btc_parent_movement_id"] = str(parent["btc_parent_movement_id"])
    result["btc_observed_close_utc"] = closed
    confirmed = parent.get("confirmed_at_utc")
    eligible = parent.get("evidence_eligible") is True and confirmed is not None
    if eligible and utc(confirmed) > decision:
        return result | {"btc_parent_movement_id": None}
    result["membership_status"] = "LIVE" if eligible else "BOUNDARY_UNVERIFIED"
    return result
