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

from typing import Any, Callable, Dict, Mapping, Tuple

TIME_FAMILIES: Dict[str, Dict[str, Any]] = {
    "now": {"label": "עכשיו", "windows": ("30m",), "weight": 35.0},
    "short": {"label": "קצר", "windows": ("1h", "4h"), "weight": 30.0},
    "medium": {"label": "בינוני", "windows": ("12h", "24h"), "weight": 20.0},
    "long": {"label": "ארוך", "windows": ("48h", "72h", "7d"), "weight": 15.0},
}

DIRECTION_SIGN = {"BULLISH": 1.0, "BEARISH": -1.0, "NEUTRAL": 0.0, "MIXED": 0.0}


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
        "SHORT_COVERING": ("BULLISH", 0.65),
        "BEARISH_BUILDUP": ("BEARISH", 1.0),
        "LONG_UNWINDING": ("BEARISH", 0.65),
    }
    direction, base = mapping.get(state, ("NEUTRAL", 0.0))
    # When historical ranks exist, require their weaker leg to determine quality.
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
    try:
        level = int(window.get("evidence_level") or 0)
    except (TypeError, ValueError):
        level = 0
    # Normal directional flow is useful but deliberately weaker than historical
    # meaningful/strong evidence.
    factor = {0: 0.0, 1: 0.35, 2: 0.75, 3: 1.0}.get(level, 0.0)
    return direction if direction in {"BULLISH", "BEARISH"} else "NEUTRAL", factor
