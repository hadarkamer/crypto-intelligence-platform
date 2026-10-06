"""Shared weighted time-family aggregation for Stage 90.

The same four time families are used by normal scans, Price+OI, Futures Flow,
Spot Flow and Max-Pain confirmation:
- NOW: 30m (35%)
- SHORT: 1h + 4h (30%)
- MEDIUM: 12h + 24h (20%)
- LONG: 48h + 72h + 7d (15%)

Each family combines its strategic weight with the quality and internal
agreement of its member windows. Outputs are deterministic, descriptive and
are not probabilities.
"""
from __future__ import annotations

import math
from typing import Any, Callable, Dict, Mapping, Optional, Tuple

TIME_FAMILIES: Dict[str, Dict[str, Any]] = {
    "now": {"label": "עכשיו", "windows": ("30m",), "weight": 35.0},
    "short": {"label": "קצר", "windows": ("1h", "4h"), "weight": 30.0},
    "medium": {"label": "בינוני", "windows": ("12h", "24h"), "weight": 20.0},
    "long": {"label": "ארוך", "windows": ("48h", "72h", "7d"), "weight": 15.0},
}

DIRECTION_SIGN = {"BULLISH": 1.0, "BEARISH": -1.0, "NEUTRAL": 0.0, "MIXED": 0.0}


def continuous_percentile_strength(
    value_abs: Any,
    distribution: Mapping[str, Any],
) -> Optional[float]:
    """Return continuous 0..1 strength between historical P25 and P90.

    P25 remains the hard noise floor and P90 remains full strength. Inside
    that valid range, the value is mapped continuously through the
    P25/P50/P75/P90 anchors. ``None`` means that the reference is incomplete
    or invalid, allowing callers to retain their legacy compatibility path.
    """
    try:
        value = abs(float(value_abs))
        p25 = float(distribution.get("p25"))
        p50_raw = distribution.get("p50")
        if p50_raw is None:
            p50_raw = distribution.get("median")
        p50 = float(p50_raw)
        p75 = float(distribution.get("p75"))
        p90 = float(distribution.get("p90"))
    except (TypeError, ValueError):
        return None

    if not all(math.isfinite(item) for item in (value, p25, p50, p75, p90)):
        return None
    if not (p25 <= p50 <= p75 <= p90):
        return None
    if value <= p25:
        return 0.0
    if value >= p90:
        return 1.0
    if p90 <= p25:
        return 0.0

    # Flat samples can produce duplicate percentile values. Keep the first
    # (lowest) percentile at a duplicated value so the curve stays conservative
    # instead of manufacturing a jump.
    distinct = []
    for anchor_value, percentile in (
        (p25, 0.25),
        (p50, 0.50),
        (p75, 0.75),
        (p90, 0.90),
    ):
        if distinct and math.isclose(
            anchor_value,
            distinct[-1][0],
            rel_tol=0.0,
            abs_tol=max(1e-12, abs(anchor_value) * 1e-12),
        ):
            continue
        distinct.append((anchor_value, percentile))

    percentile_position = 0.25
    for (lower_value, lower_pct), (upper_value, upper_pct) in zip(
        distinct,
        distinct[1:],
    ):
        if value <= upper_value:
            ratio = (value - lower_value) / (upper_value - lower_value)
            percentile_position = lower_pct + ratio * (upper_pct - lower_pct)
            break
    else:
        percentile_position = 0.90

    strength = (percentile_position - 0.25) / (0.90 - 0.25)
    return round(max(0.0, min(1.0, strength)), 6)


def aggregate(
    windows: Mapping[str, Mapping[str, Any]],
    evaluator: Callable[[Mapping[str, Any]], Tuple[str, float]],
) -> Dict[str, Any]:
    """Aggregate window evidence into fixed weighted time families.

    evaluator returns (direction, strength) where strength is 0..1. A family
    contribution equals family weight multiplied by its signed internal net.
    The internal net naturally incorporates both evidence quality and conflict:
    opposing member windows cancel each other instead of receiving full votes.
    """
    families: Dict[str, Dict[str, Any]] = {}
    total_signed = 0.0
    available_weight = 0.0

    for key, cfg in TIME_FAMILIES.items():
        members = []
        signed_sum = 0.0
        available_count = 0
        directional_strength_sum = 0.0
        for label in cfg["windows"]:
            window = windows.get(label) or {}
            if not window.get("available"):
                members.append({"window": label, "available": False, "direction": "NEUTRAL", "strength": 0.0})
                continue
            direction, strength = evaluator(window)
            direction = str(direction or "NEUTRAL").upper()
            strength = max(0.0, min(1.0, float(strength or 0.0)))
            available_count += 1
            signed = DIRECTION_SIGN.get(direction, 0.0) * strength
            signed_sum += signed
            directional_strength_sum += abs(signed)
            members.append({
                "window": label,
                "available": True,
                "direction": direction,
                "strength": round(strength, 4),
                "signed": round(signed, 4),
            })

        configured_count = len(cfg["windows"])
        coverage = available_count / configured_count if configured_count else 0.0
        # Missing members reduce confidence. Neutral members also reduce the net.
        net = signed_sum / configured_count if configured_count else 0.0
        quality = abs(net)
        internal_agreement = (
            abs(signed_sum) / directional_strength_sum
            if directional_strength_sum > 0 else 0.0
        )
        direction = "BULLISH" if net > 0.05 else "BEARISH" if net < -0.05 else "NEUTRAL"
        weight = float(cfg["weight"])
        contribution = weight * net
        if available_count:
            available_weight += weight * coverage
        total_signed += contribution
        families[key] = {
            "key": key,
            "label": cfg["label"],
            "windows": list(cfg["windows"]),
            "weight": weight,
            "available_windows": available_count,
            "configured_windows": configured_count,
            "coverage": round(coverage, 4),
            "direction": direction,
            "quality": round(quality, 4),
            "agreement": round(internal_agreement, 4),
            "net": round(net, 4),
            "contribution": round(contribution, 4),
            "members": members,
        }

    score = max(-100.0, min(100.0, total_signed))
    direction = "BULLISH" if score >= 12.0 else "BEARISH" if score <= -12.0 else "NEUTRAL"
    return {
        "families": families,
        "score": round(score, 4),
        "direction": direction,
        "quality": round(abs(score) / 100.0, 4),
        "available_weight": round(available_weight, 4),
        "weights": {key: cfg["weight"] for key, cfg in TIME_FAMILIES.items()},
    }


def oi_window_evaluator(window: Mapping[str, Any]) -> Tuple[str, float]:
    state = str(window.get("state") or "NEUTRAL_INCONCLUSIVE").upper()
    mapping = {
        "BULLISH_BUILDUP": ("BULLISH", 1.0),
        "SHORT_COVERING": ("BULLISH", 1.0),
        "BEARISH_BUILDUP": ("BEARISH", 1.0),
        "LONG_UNWINDING": ("BEARISH", 1.0),
    }
    direction, base = mapping.get(state, ("NEUTRAL", 0.0))
    # Price and OI are both required. Their weaker continuous historical leg
    # determines quality. Old payloads retain the rank fallback below.
    continuous = []
    continuous_seen = False
    for name in ("price_strength", "oi_strength"):
        strength_payload = window.get(name) or {}
        if "continuous_strength" in strength_payload:
            continuous_seen = True
            value = strength_payload.get("continuous_strength")
            try:
                continuous.append(max(0.0, min(1.0, float(value))))
            except (TypeError, ValueError):
                pass
    if len(continuous) == 2:
        return direction, base * min(continuous)
    if continuous_seen:
        return direction, 0.0

    # Legacy fallback when historical continuous values are unavailable.
    ranks = []
    for name in ("price_strength", "oi_strength"):
        rank = (window.get(name) or {}).get("rank")
        if rank is not None:
            try:
                ranks.append(int(rank))
            except (TypeError, ValueError):
                pass
    if ranks:
        rank_factor = {0: 0.0, 1: 0.45, 2: 0.75, 3: 0.90, 4: 1.0}.get(min(ranks), 0.0)
        base *= rank_factor
    return direction, base


def flow_window_evaluator(window: Mapping[str, Any]) -> Tuple[str, float]:
    direction = str(window.get("direction") or "NEUTRAL").upper()
    if "continuous_strength" in window:
        continuous = window.get("continuous_strength")
        try:
            factor = max(0.0, min(1.0, float(continuous)))
        except (TypeError, ValueError):
            factor = 0.0
        return direction if direction in {"BULLISH", "BEARISH"} else "NEUTRAL", factor

    # Compatibility fallback only for pre-Stage-106 payloads, where the field
    # does not exist at all. A present-but-invalid continuous value must not
    # silently regain strength through the legacy step model.
    try:
        level = int(window.get("evidence_level") or 0)
    except (TypeError, ValueError):
        level = 0
    # Normal directional flow is useful but deliberately weaker than historical
    # meaningful/strong evidence.
    factor = {0: 0.0, 1: 0.35, 2: 0.75, 3: 1.0}.get(level, 0.0)
    return direction if direction in {"BULLISH", "BEARISH"} else "NEUTRAL", factor
