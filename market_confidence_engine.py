"""Stage 90: weighted time families and Max-Pain confirmation.

Two core derivatives families determine confirmation; Spot is secondary context:
1. Price + OI positioning
2. Futures CVD flow
3. Spot CVD flow (display/support/divergence only; no vote or veto)

Each data family first aggregates the same four weighted time families:
NOW 30m=35%, SHORT 1h+4h=30%, MEDIUM 12h+24h=20%,
LONG 48h+72h+7d=15%. Internal agreement and evidence quality reduce the
family contribution. The result never changes the existing Max-Pain score.
"""
from __future__ import annotations

from copy import deepcopy
import time
from typing import Any, Dict, List, Optional, Tuple

import coinglass_flow_engine
import coinglass_oi_regime_service
import time_family_engine

_FLOW_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_FLOW_CACHE_TTL_SECONDS = 300


def clear_flow_cache(symbol: Optional[str] = None) -> None:
    """Invalidate cached Flow immediately after a completed CVD write.

    The collector calls this only when new rows were stored.  A no-change poll
    keeps the existing cache, while Watch can never remain five minutes behind
    a newly closed candle.
    """
    if symbol is None:
        _FLOW_CACHE.clear()
    else:
        _FLOW_CACHE.pop(str(symbol or "").upper(), None)


def _cached_flow(symbol: str) -> Dict[str, Any]:
    symbol = str(symbol or "").upper()
    now = time.monotonic()
    cached = _FLOW_CACHE.get(symbol)
    if cached and now - cached[0] <= _FLOW_CACHE_TTL_SECONDS:
        return deepcopy(cached[1])
    result = coinglass_flow_engine.analyze_symbol(symbol)
    _FLOW_CACHE[symbol] = (now, deepcopy(result))
    return result


def _price_direction_from_side(side: Any) -> str:
    """Displayed Max-Pain side is the side expected to be hurt."""
    side = str(side or "").upper()
    return "BEARISH" if side == "LONG" else "BULLISH" if side == "SHORT" else "NEUTRAL"


def _direction_from_score(score: float) -> str:
    return "BULLISH" if score >= 12.0 else "BEARISH" if score <= -12.0 else "NEUTRAL"


def _relation(direction: str, expected: str) -> str:
    if direction not in {"BULLISH", "BEARISH"} or expected not in {"BULLISH", "BEARISH"}:
        return "NEUTRAL"
    return "SUPPORT" if direction == expected else "OPPOSE"


def _positioning_module(regime: Dict[str, Any], expected: str) -> Dict[str, Any]:
    windows = regime.get("windows") or {}
    weighted = time_family_engine.aggregate(windows, time_family_engine.oi_window_evaluator)
    available = bool(regime.get("available")) and str(regime.get("data_quality_status") or "PASS").upper() != "INVALID"
    overall = regime.get("overall") or {}
    score = float(weighted.get("score") or 0.0) if available else 0.0
    direction = _direction_from_score(score)
    if available and not windows:
        state = str(overall.get("state") or "").upper()
        fallback = {
            "BULLISH_BUILDUP": ("BULLISH", 100.0), "SHORT_COVERING": ("BULLISH", 100.0),
            "BEARISH_BUILDUP": ("BEARISH", -100.0), "LONG_UNWINDING": ("BEARISH", -100.0),
        }.get(state)
        if fallback:
            direction, score = fallback
    early_shift = None
    if bool(regime.get("early_transition")):
        # Price+OI early transition is confirmed by the two shortest windows
        # (30m and 1h) agreeing against the established broader state.
        short_states = [
            str((windows.get(label) or {}).get("state") or "").upper()
            for label in ("30m", "1h")
            if (windows.get(label) or {}).get("available")
        ]
        if len(short_states) == 2 and short_states[0] == short_states[1]:
            state_direction = {
                "BULLISH_BUILDUP": "BULLISH",
                "SHORT_COVERING": "BULLISH",
                "BEARISH_BUILDUP": "BEARISH",
                "LONG_UNWINDING": "BEARISH",
            }.get(short_states[0])
            if state_direction:
                early_shift = {
                    "new_direction": state_direction,
                    "new_state": short_states[0],
                    "source": "Price+OI",
                }

    return {
        "family": "Price+OI",
        "available": available,
        "direction": direction,
        "relation": _relation(direction, expected),
        "score": round(score, 4),
        "quality": round(abs(score) / 100.0, 4),
        "state": str(overall.get("state") or "UNAVAILABLE").upper(),
        "label": str(overall.get("label") or overall.get("state") or "No data").replace("_", " "),
        "time_families": weighted.get("families") or {},
        "early_shift": early_shift,
    }


def _flow_module(data: Dict[str, Any], family: str, expected: str) -> Dict[str, Any]:
    windows = data.get("windows") or {}
    weighted = time_family_engine.aggregate(windows, time_family_engine.flow_window_evaluator)
    quality_data = data.get("quality") or {}
    quality_status = str(quality_data.get("status") or "NO_DATA").upper()
    usable_for_confirmation = bool(quality_data.get("usable_for_confirmation", quality_status in {"PASS", "WARNING"}))
    available = bool(data.get("available")) and usable_for_confirmation
    score = float(weighted.get("score") or 0.0) if available else 0.0
    # A warning reduces confidence but preserves direction.
    if quality_status == "WARNING":
        score *= 0.75
    direction = _direction_from_score(score)
    overall = data.get("overall") or {}
    if available and not windows:
        raw = str(overall.get("direction") or "NEUTRAL").upper()
        if raw in {"BULLISH", "BEARISH"}:
            direction = raw
            score = 100.0 if raw == "BULLISH" else -100.0
    return {
        "family": family,
        "available": available,
        "direction": direction,
        "relation": _relation(direction, expected),
        "score": round(score, 4),
        "quality": round(abs(score) / 100.0, 4),
        "state": str(overall.get("state") or "NO_DATA").upper(),
        "label": str(overall.get("state") or "No data").replace("_", " ").title(),
        "quality_status": quality_status,
        "freshness_status": str(quality_data.get("freshness_status") or "UNKNOWN").upper(),
        "age_minutes": quality_data.get("age_minutes"),
        "quality_reasons": list(quality_data.get("reasons") or []),
        "time_families": weighted.get("families") or {},
        "early_shift": data.get("early_shift"),
    }


def _spot_context(module: Dict[str, Any]) -> Dict[str, Any]:
    """Return Spot as secondary context only; it never confirms or vetoes."""
    relation = str(module.get("relation") or "NEUTRAL").upper()
    score = float(module.get("score") or 0.0)
    if relation == "SUPPORT":
        status = "SUPPORTS"
        label = "Spot תומך"
    elif relation == "OPPOSE":
        status = "DIVERGING"
        label = "Spot סותר / Divergence"
    else:
        status = "NEUTRAL"
        label = "Spot ניטרלי"
    return {"status": status, "label": label, "relation": relation, "score": round(score, 4)}


def _conclusion(modules: Dict[str, Dict[str, Any]], expected: str) -> Dict[str, Any]:
    """Summarize evidence while keeping Spot outside the confirmation vote."""
    counts = {"BULLISH": 0, "BEARISH": 0, "NEUTRAL": 0, "MIXED": 0}
    for module in modules.values():
        direction = str(module.get("direction") or "NEUTRAL").upper()
        counts[direction if direction in counts else "NEUTRAL"] += 1

    core_keys = ("positioning", "futures_flow")
    core_support = sum(int(str((modules.get(k) or {}).get("relation") or "NEUTRAL") == "SUPPORT") for k in core_keys)
    core_opposition = sum(int(str((modules.get(k) or {}).get("relation") or "NEUTRAL") == "OPPOSE") for k in core_keys)
    spot = _spot_context(modules.get("spot_flow") or {})

    if core_opposition:
        classification = "CORE_CONFLICT"
        label = "קונפליקט — Price+OI או Futures Flow מתנגדים"
        relation_to_alert = "CONFLICT"
    elif core_support == 2:
        classification = "CORE_CONFIRMATION"
        label = "אישור נגזרים מלא — Price+OI + Futures Flow"
        relation_to_alert = "SUPPORT"
    elif core_support == 1:
        classification = "WEAK_EVIDENCE"
        label = "עדות חלקית — מנוע נגזרים אחד בלבד"
        relation_to_alert = "NO_CONFIRMATION"
    else:
        classification = "NO_DIRECTIONAL_EVIDENCE"
        label = "אין כרגע אישור נגזרים כיווני"
        relation_to_alert = "NO_CONFIRMATION"

    return {
        "counts": counts,
        "supporting_families": core_support,
        "opposing_families": core_opposition,
        "core_supporting_families": core_support,
        "core_opposing_families": core_opposition,
        "spot_context": spot,
        "classification": classification,
        "classification_label": label,
        "relation_to_alert": relation_to_alert,
    }


def _early_shift_opposes(modules: Dict[str, Dict[str, Any]], expected: str) -> bool:
    for key in ("positioning", "futures_flow"):
        early = (modules.get(key) or {}).get("early_shift") or {}
        if early and str(early.get("new_direction") or "").upper() in {"BULLISH", "BEARISH"}:
            if str(early.get("new_direction")).upper() != expected:
                return True
    return False


def _confirmation(maxpain_score: float, expected: str, modules: Dict[str, Dict[str, Any]], conclusion: Dict[str, Any]) -> Dict[str, Any]:
    score = float(maxpain_score or 0.0)
    score_ok = score >= 65.0
    strong_score_ok = score >= 75.0
    early_against = _early_shift_opposes(modules, expected)
    oi_relation = str((modules.get("positioning") or {}).get("relation") or "NEUTRAL")
    oi_opposes = oi_relation == "OPPOSE"
    support = int(conclusion.get("core_supporting_families") or conclusion.get("supporting_families") or 0)
    opposition = int(conclusion.get("core_opposing_families") or conclusion.get("opposing_families") or 0)
    positioning_score = abs(float((modules.get("positioning") or {}).get("score") or 0.0))
    futures_score = abs(float((modules.get("futures_flow") or {}).get("score") or 0.0))
    strong_core = support == 2 and positioning_score >= 25.0 and futures_score >= 25.0

    if not score_ok:
        status, label = "BELOW_SCORE", "ללא Confirmation — ציון Max Pain מתחת ל-65"
    elif early_against:
        position_early = (modules.get("positioning") or {}).get("early_shift") or {}
        futures_early = (modules.get("futures_flow") or {}).get("early_shift") or {}
        position_against = str(position_early.get("new_direction") or "").upper() in {"BULLISH", "BEARISH"} and str(position_early.get("new_direction") or "").upper() != expected
        futures_against = str(futures_early.get("new_direction") or "").upper() in {"BULLISH", "BEARISH"} and str(futures_early.get("new_direction") or "").upper() != expected
        if position_against and futures_against:
            label = "Max Pain Conflict — Price+OI ו-Futures Early Shift נגד העסקה"
        elif position_against:
            label = "Max Pain Conflict — Price+OI Early Shift נגד העסקה"
        else:
            label = "Max Pain Conflict — Futures Early Shift נגד העסקה"
        status = "CONFLICT"
    elif oi_opposes:
        status, label = "CONFLICT", "Max Pain Conflict — Price+OI סותר"
    elif opposition:
        status, label = "CONFLICT", "Max Pain Conflict — מנוע נגזרים מתנגד"
    elif support == 2 and strong_score_ok:
        status, label = "STRONG_CONFIRMED", "Max Pain Strong Confirmation — ציון 75+ עם Price+OI + Futures"
    elif support == 2:
        status, label = "CONFIRMED", "Max Pain Confirmed — ציון 65–74.99 עם Price+OI + Futures"
    else:
        status, label = "UNCONFIRMED", "Max Pain לא מאומת כרגע"

    return {
        "status": status,
        "label": label,
        "score_threshold": 65.0,
        "strong_score_threshold": 75.0,
        "score_ok": score_ok,
        "strong_score_ok": strong_score_ok,
        "early_shift_opposes": early_against,
        "oi_opposes": oi_opposes,
        "supporting_families": support,
        "opposing_families": opposition,
        "strong_core": strong_core,
        "strong_evidence_threshold": 25.0,
    }


def combine(
    symbol: str,
    expected_price_direction: Optional[str],
    regime: Optional[Dict[str, Any]] = None,
    flow: Optional[Dict[str, Any]] = None,
    maxpain_score: float = 0.0,
) -> Dict[str, Any]:
    symbol = str(symbol or "").upper()
    expected = str(expected_price_direction or "NEUTRAL").upper()
    if expected in {"LONG", "SHORT"}:
        expected = "BULLISH" if expected == "LONG" else "BEARISH"
    regime = regime if regime is not None else coinglass_oi_regime_service.latest(symbol)
    flow = flow if flow is not None else _cached_flow(symbol)
    modules = {
        "positioning": _positioning_module(regime, expected),
        "futures_flow": _flow_module(flow.get("futures") or {}, "Futures Flow", expected),
        "spot_flow": _flow_module(flow.get("spot") or {}, "Spot Flow", expected),
    }
    conclusion = _conclusion(modules, expected)
    confirmation = _confirmation(maxpain_score, expected, modules, conclusion)
    return {
        "symbol": symbol,
        "expected_price_direction": expected,
        "maxpain_score": float(maxpain_score or 0.0),
        "modules": modules,
        **conclusion,
        "confirmation": confirmation,
        "note": "Confirmation is read-only; existing Max-Pain score and ranking are unchanged.",
    }


def max_pain_side_to_price_direction(max_pain_side: Any) -> str:
    side = str(max_pain_side or "").upper()
    return "LONG" if side == "SHORT" else "SHORT" if side == "LONG" else "NEUTRAL"


def attach_to_opportunities(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    flow_cache: Dict[str, Dict[str, Any]] = {}
    regime_cache: Dict[str, Dict[str, Any]] = {}
    for item in items:
        symbol = str(item.get("symbol") or "").upper()
        if symbol not in flow_cache:
            try:
                flow_cache[symbol] = _cached_flow(symbol)
            except Exception as exc:
                flow_cache[symbol] = {
                    "futures": {"available": False, "quality": {"status": "NO_DATA", "reasons": [repr(exc)]}},
                    "spot": {"available": False, "quality": {"status": "NO_DATA", "reasons": [repr(exc)]}},
                }
        if symbol not in regime_cache:
            regime_cache[symbol] = item.get("market_regime") or coinglass_oi_regime_service.latest(symbol)
        expected = _price_direction_from_side(item.get("side"))
        maxpain_score = float(item.get("score", item.get("priority", 0)) or 0.0)
        item["flow_context"] = flow_cache[symbol]
        item["market_evidence"] = combine(symbol, expected, regime_cache[symbol], flow_cache[symbol], maxpain_score)
        item["maxpain_confirmation"] = item["market_evidence"].get("confirmation") or {}
    return items
