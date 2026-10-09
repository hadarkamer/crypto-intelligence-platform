"""Bounded resumable first touch over an explicit next-minute-open contract.

No database/network/runtime imports. A cutoff limits observable data; it never
closes a no-touch trade. A checkpoint hash detects accidental alteration, not
malicious fabrication. Callers must independently attest route/data provenance.
"""
from __future__ import annotations

from datetime import timedelta
import json
import math
from typing import Any, Iterable, Mapping

import research_no_horizon_contract as contracts

VERSION = "no-horizon-first-touch-checkpoint-v1"
TERMINAL = ("SUCCESS", "FAILURE", "AMBIGUOUS")
STATUSES = ("OPEN", "SUCCESS", "FAILURE", "AMBIGUOUS", "DATA_MISSING", "BLOCKED_ENTRY")
_MINUTE = timedelta(minutes=1)


def _seal(value: Mapping[str, Any]) -> dict[str, Any]:
    result = {key: item for key, item in value.items() if key != "checkpoint_sha256"}
    return {**result, "checkpoint_sha256": contracts.digest(result)}


def initialize(contract: Mapping[str, Any], *, cutoff_utc: Any) -> dict[str, Any]:
    c = contracts.validate_contract(contract)
    cutoff = contracts.utc(cutoff_utc)
    entry = contracts.utc(c["entry_time_utc"])
    if cutoff < entry:
        raise ValueError("cutoff precedes the declared entry")
    return _seal({"state_version": VERSION, "contract": c, "cutoff_utc": cutoff.isoformat(),
        "status": "OPEN", "success": None, "terminal_reason": None,
        "first_touch_side": "NONE", "decision_time_utc": None, "decision_precision": None,
        "processed_candles": 0, "next_open_utc": entry.isoformat(),
        "observed_through_utc": entry.isoformat(), "entry_verified": False,
        "path_chain_sha256": "0" * 64, "prefix_complete": True,
        "coverage_complete_through_cutoff": entry + _MINUTE > cutoff,
        "progress": "AWAITING_CLOSED_ENTRY_CANDLE", "missing_open_utc": None,
        "runtime_authorized": False, "telegram_authorized": False, "trading_authorized": False})


def validate_state(state: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the serialized checkpoint before consuming any new prices."""
    if not isinstance(state, Mapping):
        raise ValueError("checkpoint must be an object")
    try:
        if _seal(state)["checkpoint_sha256"] != state["checkpoint_sha256"]:
            raise ValueError("checkpoint integrity mismatch")
        value = json.loads(contracts.canonical(state))
        c = contracts.validate_contract(value["contract"])
        n = value["processed_candles"]
        expected = contracts.utc(c["entry_time_utc"]) + n * _MINUTE if type(n) is int and n >= 0 else None
        if (value["state_version"] != VERSION or value["status"] not in STATUSES
                or expected is None or contracts.utc(value["next_open_utc"]) != expected
                or contracts.utc(value["observed_through_utc"]) != expected
                or expected > contracts.utc(value["cutoff_utc"])
                or type(value["entry_verified"]) is not bool
                or value["entry_verified"] != (n > 0)
                or any(value[key] is not False for key in ("runtime_authorized", "telegram_authorized", "trading_authorized"))):
            raise ValueError("inconsistent checkpoint identity or progress")
        if value["status"] in TERMINAL:
            if (not value["entry_verified"] or value["prefix_complete"] is not True
                    or value["success"] is not {"SUCCESS": True, "FAILURE": False, "AMBIGUOUS": None}[value["status"]]
                    or not contracts.utc(c["entry_time_utc"]) <= contracts.utc(value["decision_time_utc"]) <= expected):
                raise ValueError("invalid terminal evidence")
        elif value["success"] is not None or value["decision_time_utc"] is not None:
            raise ValueError("nonterminal state cannot carry a result")
    except (KeyError, TypeError, OverflowError) as exc:
        raise ValueError("invalid checkpoint") from exc
    return value


def _bar(raw: Mapping[str, Any], *, cutoff: Any) -> dict[str, Any] | None:
    if not isinstance(raw, Mapping):
        raise ValueError("candle must be an object")
    try:
        start = contracts.utc(raw["open_time_utc"])
        end = contracts.utc(raw["close_time_utc"])
        available = start + _MINUTE
        if start.second or start.microsecond or end not in (available, available-timedelta(milliseconds=1)):
            raise ValueError("candle must cover one complete aligned minute")
        # Future prices are never inspected, even if the caller supplied them.
        if available > cutoff:
            return None
        prices = {key: contracts.number(raw[key], "candle."+key) for key in ("open", "high", "low", "close")}
        if (min(prices.values()) <= 0 or prices["high"] < max(prices.values())
                or prices["low"] > min(prices.values())):
            raise ValueError("invalid OHLC candle")
        return {"open_time_utc": start.isoformat(), "close_time_utc": available.isoformat(), **prices}
    except KeyError as exc:
        raise ValueError("candle is missing a required field") from exc


def _touch(c: Mapping[str, Any], bar: Mapping[str, Any]) -> tuple[str, str] | None:
    favorable, adverse = c["favorable_barrier_price"], c["adverse_barrier_price"]
    opened = bar["open"]
    long = c["direction"] == "LONG"
    # The candle open is known to precede both extrema; a gap can establish
    # order even when the complete OHLC range later contains both barriers.
    if (opened >= favorable) if long else (opened <= favorable):
        return "SUCCESS", "OPEN"
    if (opened <= adverse) if long else (opened >= adverse):
        return "FAILURE", "OPEN"
    f = bar["high"] >= favorable if long else bar["low"] <= favorable
    a = bar["low"] <= adverse if long else bar["high"] >= adverse
    if f and a:
        return "AMBIGUOUS", "WITHIN_MINUTE"
    if f or a:
        return ("SUCCESS" if f else "FAILURE"), "WITHIN_MINUTE"
    return None


def advance(state: Mapping[str, Any], candles: Iterable[Mapping[str, Any]], *,
            cutoff_utc: Any, max_candles: int = 10000) -> dict[str, Any]:
    """Consume at most max_candles, beginning exactly at next_open_utc.

    Resume by supplying only the unconsumed suffix. Cutoffs may advance but
    never retreat. Exhausting a supplied chunk is MORE_DATA_REQUIRED while
    still OPEN; an actual interior gap is DATA_MISSING and can be repaired by
    supplying its missing prefix. A terminal prefix ignores subsequent gaps.
    """
    value = validate_state(state)
    cutoff = contracts.utc(cutoff_utc)
    if cutoff < contracts.utc(value["cutoff_utc"]):
        raise ValueError("cutoff cannot move backward; replay from a new checkpoint")
    if type(max_candles) is not int or not 1 <= max_candles <= 100000:
        raise ValueError("max_candles must be an integer from 1 to 100000")
    value["cutoff_utc"] = cutoff.isoformat()
    if value["status"] in TERMINAL or value["status"] == "BLOCKED_ENTRY":
        value["coverage_complete_through_cutoff"] = contracts.utc(value["next_open_utc"]) + _MINUTE > cutoff
        return _seal(value)
    c = value["contract"]
    value.update(status="OPEN", terminal_reason=None, missing_open_utc=None, prefix_complete=True)
    consumed = 0
    iterator = iter(candles)
    while consumed < max_candles:
        try:
            raw = next(iterator)
        except StopIteration:
            break
        bar = _bar(raw, cutoff=cutoff)
        if bar is None:
            break
        expected = contracts.utc(value["next_open_utc"])
        start = contracts.utc(bar["open_time_utc"])
        if start < expected:
            raise ValueError("candles must be strictly chronological and start at checkpoint cursor")
        if start > expected:
            value.update(status="DATA_MISSING", terminal_reason="MISSING_PRE_TOUCH_CANDLE",
                missing_open_utc=expected.isoformat(), prefix_complete=False, progress="REPAIR_MISSING_PREFIX")
            break
        if not value["entry_verified"] and not math.isclose(bar["open"], c["reference_price"], rel_tol=1e-12, abs_tol=0):
            value.update(status="BLOCKED_ENTRY", terminal_reason="REFERENCE_DOES_NOT_MATCH_ACTUAL_ENTRY_OPEN",
                prefix_complete=False, progress="NEW_ENTRY_CONTRACT_REQUIRED")
            break
        value["entry_verified"] = True
        value["processed_candles"] += 1
        consumed += 1
        value["next_open_utc"] = bar["close_time_utc"]
        value["observed_through_utc"] = bar["close_time_utc"]
        value["path_chain_sha256"] = contracts.digest([value["path_chain_sha256"], bar])
        touched = _touch(c, bar)
        if touched:
            status, precision = touched
            value.update(status=status, success={"SUCCESS": True, "FAILURE": False, "AMBIGUOUS": None}[status],
                terminal_reason={"SUCCESS": "FAVORABLE_FIRST", "FAILURE": "ADVERSE_FIRST", "AMBIGUOUS": "SAME_CANDLE_BOTH"}[status],
                first_touch_side={"SUCCESS": "FAVORABLE", "FAILURE": "ADVERSE", "AMBIGUOUS": "AMBIGUOUS"}[status],
                decision_time_utc=bar["open_time_utc"] if precision == "OPEN" else bar["close_time_utc"],
                decision_precision=precision, progress="TERMINAL_PREFIX_PROVED")
            break
    complete = contracts.utc(value["next_open_utc"]) + _MINUTE > cutoff
    value["coverage_complete_through_cutoff"] = complete
    if value["status"] == "OPEN":
        value["progress"] = "CAUGHT_UP_OPEN" if complete else "MORE_DATA_REQUIRED"
    return _seal(value)


def evaluate(contract: Mapping[str, Any], candles: Iterable[Mapping[str, Any]], *,
             cutoff_utc: Any, max_candles: int = 100000) -> dict[str, Any]:
    """One bounded pass; continue with advance if progress is MORE_DATA_REQUIRED."""
    return advance(initialize(contract, cutoff_utc=cutoff_utc), candles,
                   cutoff_utc=cutoff_utc, max_candles=max_candles)
