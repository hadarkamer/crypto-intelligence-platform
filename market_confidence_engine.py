"""Stage 90: weighted time families and Max-Pain confirmation.

Three independent data families are evaluated:
1. Price + OI positioning
2. Futures CVD flow
3. Spot CVD flow

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
            "BULLISH_BUILDUP": ("BULLISH", 100.0), "SHORT_COVERING": ("BULLISH", 65.0),
            "BEARISH_BUILDUP": ("BEARISH", -100.0), "LONG_UNWINDING": ("BEARISH", -65.0),
        }.get(state)
        if fallback:
            direction, score = fallback
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
        "early_shift": bool(regime.get("early_transition")),
    }


def _flow_module(data: Dict[str, Any], family: str, expected: str) -> Dict[str, Any]:
    windows = data.get("windows") or {}
    weighted = time_family_engine.aggregate(windows, time_family_engine.flow_window_evaluator)
    quality_data = data.get("quality") or {}
    quality_status = str(quality_data.get("status") or "NO_DATA").upper()
    available = bool(data.get("available")) and quality_status in {"PASS", "WARNING"}
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
        "quality_reasons": list(quality_data.get("reasons") or []),
        "time_families": weighted.get("families") or {},
        "early_shift": data.get("early_shift"),
    }


def _conclusion(modules: Dict[str, Dict[str, Any]], expected: str) -> Dict[str, Any]:
    counts = {"BULLISH": 0, "BEARISH": 0, "NEUTRAL": 0, "MIXED": 0}
    support = opposition = 0
    for module in modules.values():
        direction = str(module.get("direction") or "NEUTRAL").upper()
        counts[direction if direction in counts else "NEUTRAL"] += 1
        relation = module.get("relation")
        support += int(relation == "SUPPORT")
        opposition += int(relation == "OPPOSE")

    bull = counts.get("BULLISH", 0)
    bear = counts.get("BEARISH", 0)
    if bull == 3 or bear == 3:
        classification = "FULL_CONFIRMATION"
        label = f"אישור {'שורי' if bull == 3 else 'דובי'} מלא — 3/3"
        relation_to_alert = "SUPPORT" if support == 3 else "CONFLICT"
    elif support == 2 and opposition == 0:
        classification = "CLEAR_CONFIRMATION"
        label = "אישור ברור — 2/3 ללא סתירה"
        relation_to_alert = "SUPPORT"
    elif opposition:
        classification = "CONFLICT"
        label = "קונפליקט — לפחות משפחת מידע אחת מתנגדת"
        relation_to_alert = "CONFLICT"
    elif support == 1:
        classification = "WEAK_EVIDENCE"
        label = "עדות חלשה — משפחה אחת בלבד"
        relation_to_alert = "NO_CONFIRMATION"
    else:
        classification = "NO_DIRECTIONAL_EVIDENCE"
        label = "אין כרגע אישור כיווני"
        relation_to_alert = "NO_CONFIRMATION"

    return {
        "counts": counts,
        "supporting_families": support,
        "opposing_families": opposition,
        "classification": classification,
        "classification_label": label,
        "relation_to_alert": relation_to_alert,
    }


def _early_shift_opposes(modules: Dict[str, Dict[str, Any]], expected: str) -> bool:
    for key in ("futures_flow", "spot_flow"):
        early = (modules.get(key) or {}).get("early_shift") or {}
        if early and str(early.get("new_direction") or "").upper() in {"BULLISH", "BEARISH"}:
            if str(early.get("new_direction")).upper() != expected:
                return True
    return False


def _confirmation(maxpain_score: float, expected: str, modules: Dict[str, Dict[str, Any]], conclusion: Dict[str, Any]) -> Dict[str, Any]:
    score_ok = float(maxpain_score or 0.0) >= 70.0
    early_against = _early_shift_opposes(modules, expected)
    oi_relation = str((modules.get("positioning") or {}).get("relation") or "NEUTRAL")
    oi_opposes = oi_relation == "OPPOSE"
    support = int(conclusion.get("supporting_families") or 0)
    opposition = int(conclusion.get("opposing_families") or 0)

    if not score_ok:
        status, label = "BELOW_SCORE", "ללא Confirmation — ציון Max Pain מתחת ל-70"
    elif early_against:
        status, label = "CONFLICT", "Max Pain Conflict — Early Shift נגד העסקה"
    elif oi_opposes:
        status, label = "CONFLICT", "Max Pain Conflict — Price+OI סותר"
    elif opposition:
        status, label = "CONFLICT", "Max Pain Conflict — משפחת מידע מתנגדת"
    elif support == 3:
        status, label = "STRONG_CONFIRMED", "Max Pain Strong Confirmation — 3/3"
    elif support == 2:
        status, label = "CONFIRMED", "Max Pain Confirmed — 2/3"
    else:
        status, label = "UNCONFIRMED", "Max Pain לא מאומת כרגע"

    return {
        "status": status,
        "label": label,
        "score_threshold": 70.0,
        "score_ok": score_ok,
        "early_shift_opposes": early_against,
        "oi_opposes": oi_opposes,
        "supporting_families": support,
        "opposing_families": opposition,
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
