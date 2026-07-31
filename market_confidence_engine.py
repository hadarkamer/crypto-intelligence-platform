"""Stage 89: deterministic Market Evidence integration.

Combines three independent data families without modifying the existing
Max-Pain score or LONG/SHORT selection:

1. Positioning: Price + Open Interest regime.
2. Futures Flow: Futures CVD/Buy-Sell family.
3. Spot Flow: Spot CVD/Buy-Sell family.

The result is an alignment score in the range -100..+100 relative to an
expected PRICE direction. It is not a probability and is never fed back into
alert ranking, score, Watch activation or position sizing.
"""
from __future__ import annotations

from copy import deepcopy
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import coinglass_flow_engine
import coinglass_oi_regime_service

# Transparent weights. Each family gets one vote only; Buy/Sell and CVD are
# already combined inside the Flow family and are never counted separately.
WEIGHTS = {
    "positioning": 40.0,
    "futures_flow": 35.0,
    "spot_flow": 25.0,
}

_FLOW_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_FLOW_CACHE_TTL_SECONDS = 300


def _normalize_direction(value: Any) -> str:
    value = str(value or "").upper()
    return value if value in {"LONG", "SHORT"} else "NEUTRAL"


def _flow_direction(value: Any) -> str:
    value = str(value or "").upper()
    if value == "BULLISH":
        return "LONG"
    if value == "BEARISH":
        return "SHORT"
    return "NEUTRAL"


def _cached_flow(symbol: str) -> Dict[str, Any]:
    symbol = str(symbol or "").upper()
    now = time.monotonic()
    cached = _FLOW_CACHE.get(symbol)
    if cached and now - cached[0] <= _FLOW_CACHE_TTL_SECONDS:
        return deepcopy(cached[1])
    result = coinglass_flow_engine.analyze_symbol(symbol)
    _FLOW_CACHE[symbol] = (now, deepcopy(result))
    return result


def _positioning_module(regime: Dict[str, Any]) -> Dict[str, Any]:
    overall = regime.get("overall") or {}
    state = str(overall.get("state") or "").upper()
    label = str(overall.get("label") or state or "Unavailable")
    agreement = int(overall.get("agreement") or 0)
    valid_windows = int(overall.get("valid_windows") or 0)
    quality = str(regime.get("data_quality_status") or "PASS").upper()

    mapping = {
        "BULLISH_BUILDUP": ("LONG", 1.00, "new OI supports the rise"),
        "BEARISH_BUILDUP": ("SHORT", 1.00, "new OI supports the decline"),
        "SHORT_COVERING": ("LONG", 0.65, "price rises while OI falls"),
        "LONG_UNWINDING": ("SHORT", 0.65, "price falls while OI falls"),
    }
    direction, state_factor, note = mapping.get(
        state, ("NEUTRAL", 0.0, "no directional positioning conclusion")
    )
    agreement_factor = min(1.0, agreement / 5.0) if agreement else 0.0
    quality_factor = 0.0 if quality == "INVALID" else 0.75 if quality == "WARNING" else 1.0
    strength = state_factor * agreement_factor * quality_factor
    available = bool(regime.get("available")) and valid_windows > 0 and quality != "INVALID"
    if not available:
        strength = 0.0
        direction = "NEUTRAL"

    return {
        "family": "Positioning",
        "available": available,
        "quality": quality,
        "direction": direction,
        "strength": round(strength, 4),
        "state": state or "UNAVAILABLE",
        "label": label,
        "agreement": agreement,
        "valid_windows": valid_windows,
        "note": note,
    }


def _flow_module(flow_market: Dict[str, Any], family: str) -> Dict[str, Any]:
    overall = flow_market.get("overall") or {}
    state = str(overall.get("state") or "NO_DATA").upper()
    direction = _flow_direction(overall.get("direction"))
    quality = (flow_market.get("quality") or {})
    quality_status = str(quality.get("status") or "NO_DATA").upper()

    if state.endswith("_CONFIRMED"):
        state_factor = 1.00
    elif state.endswith("_EVIDENCE"):
        state_factor = 0.70
    elif state.endswith("_EARLY"):
        state_factor = 0.40
    else:
        state_factor = 0.0
        direction = "NEUTRAL"

    quality_factor = 1.0 if quality_status == "PASS" else 0.60 if quality_status == "WARNING" else 0.0
    available = bool(flow_market.get("available")) and quality_status in {"PASS", "WARNING"}
    strength = state_factor * quality_factor if available else 0.0

    return {
        "family": family,
        "available": available,
        "quality": quality_status,
        "quality_reasons": list(quality.get("reasons") or []),
        "direction": direction,
        "strength": round(strength, 4),
        "state": state,
        "label": state.replace("_", " ").title(),
        "early_shift": flow_market.get("early_shift"),
    }


def _classification(score: float) -> Tuple[str, str]:
    if score >= 70:
        return "STRONG_SUPPORT", "Strong support"
    if score >= 35:
        return "SUPPORT", "Support"
    if score > 0:
        return "WEAK_SUPPORT", "Weak support"
    if score <= -70:
        return "STRONG_CONFLICT", "Strong conflict"
    if score <= -35:
        return "CONFLICT", "Conflict"
    if score < 0:
        return "WEAK_CONFLICT", "Weak conflict"
    return "NEUTRAL", "Neutral / mixed"


def combine(
    symbol: str,
    expected_price_direction: Optional[str],
    regime: Optional[Dict[str, Any]] = None,
    flow: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Combine the three families relative to an expected PRICE direction.

    The score is evidence alignment, not probability:
      +100 = all available families strongly support the expected direction.
      -100 = all strongly contradict it.
         0 = neutral, mixed or no directional evidence.
    """
    symbol = str(symbol or "").upper()
    expected = _normalize_direction(expected_price_direction)
    regime = regime if regime is not None else coinglass_oi_regime_service.latest(symbol)
    flow = flow if flow is not None else _cached_flow(symbol)

    modules = {
        "positioning": _positioning_module(regime),
        "futures_flow": _flow_module(flow.get("futures") or {}, "Futures Flow"),
        "spot_flow": _flow_module(flow.get("spot") or {}, "Spot Flow"),
    }

    score = 0.0
    coverage = 0.0
    for key, module in modules.items():
        weight = WEIGHTS[key]
        if module.get("available"):
            coverage += weight
        direction = _normalize_direction(module.get("direction"))
        strength = float(module.get("strength") or 0.0)
        relation = 0
        if expected in {"LONG", "SHORT"} and direction in {"LONG", "SHORT"}:
            relation = 1 if direction == expected else -1
        contribution = weight * strength * relation
        module["weight"] = weight
        module["relation"] = "SUPPORT" if relation > 0 else "CONFLICT" if relation < 0 else "NEUTRAL"
        module["contribution"] = round(contribution, 2)
        score += contribution

    score = max(-100.0, min(100.0, score))
    code, label = _classification(score)
    if coverage < 50:
        evidence_quality = "LOW"
    elif coverage < 100:
        evidence_quality = "PARTIAL"
    else:
        evidence_quality = "COMPLETE"

    return {
        "symbol": symbol,
        "expected_price_direction": expected,
        "alignment_score": round(score, 2),
        "classification": code,
        "classification_label": label,
        "coverage_pct": round(coverage, 2),
        "evidence_quality": evidence_quality,
        "modules": modules,
        "score_is_probability": False,
        "note": "Read-only evidence alignment; Max-Pain score and ranking are unchanged.",
    }


def max_pain_side_to_price_direction(max_pain_side: Any) -> str:
    side = str(max_pain_side or "").upper()
    # Max-Pain LONG means longs are expected to be hurt -> implied price down.
    if side == "LONG":
        return "SHORT"
    if side == "SHORT":
        return "LONG"
    return "NEUTRAL"


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
                    "symbol": symbol,
                    "futures": {"available": False, "quality": {"status": "NO_DATA", "reasons": [repr(exc)]}},
                    "spot": {"available": False, "quality": {"status": "NO_DATA", "reasons": [repr(exc)]}},
                }
        if symbol not in regime_cache:
            regime_cache[symbol] = item.get("market_regime") or coinglass_oi_regime_service.latest(symbol)
        expected = max_pain_side_to_price_direction(item.get("side"))
        item["flow_context"] = flow_cache[symbol]
        item["market_evidence"] = combine(symbol, expected, regime_cache[symbol], flow_cache[symbol])
    return items
