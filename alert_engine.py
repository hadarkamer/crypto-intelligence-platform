from __future__ import annotations
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple
import analysis
import decision_engine

TIMEFRAMES = ["12h", "24h", "48h", "3d", "1w", "2w", "1m"]

def _get(row: Any, key: str, default=None):
    try:
        return row[key]
    except Exception:
        return default

def _closest_side(row):
    return analysis.side_from_distances(
        _get(row, "distance_short_pct"),
        _get(row, "distance_long_pct"),
    )

def _closest_distance(row):
    side = _closest_side(row)
    if side == "SHORT":
        v = _get(row, "distance_short_pct")
    elif side == "LONG":
        v = _get(row, "distance_long_pct")
    else:
        return None
    return abs(v) if v is not None else None

def _amount(row, side):
    return _get(row, "short_liquidation_amount") if side == "SHORT" else _get(row, "long_liquidation_amount")

def _other_amount(row, side):
    return _get(row, "long_liquidation_amount") if side == "SHORT" else _get(row, "short_liquidation_amount")

def _gap_pct(row):
    price = _get(row, "current_price")
    s = _get(row, "short_max_pain")
    l = _get(row, "long_max_pain")
    if not price or s is None or l is None:
        return None
    return abs(s - l) / price * 100

def _percentile(value: float, values: List[float]) -> float:
    if not values:
        return 0.0
    return sum(1 for x in values if x <= value) / len(values)

def _tf_liquidity(rows):
    out = defaultdict(list)
    for r in rows:
        tf = str(_get(r, "timeframe", ""))
        side = _closest_side(r)
        amount = _amount(r, side) if side else None
        if tf and amount is not None:
            out[tf].append(float(amount))
    return dict(out)

def _distance_points(d):
    if d is None:
        return 0.0
    if d <= 0.25:
        return 35.0
    if d >= 1.5:
        return 0.0
    return round((1.5 - d) / 1.25 * 35.0, 2)

def _balance_points(near: float, far: float) -> Tuple[float, Optional[float], str]:
    if far <= 0:
        return (10.0, None, "near side has liquidity; opposite side is zero") if near > 0 else (0.0, None, "no balance data")
    ratio = near / far
    if ratio >= 2.0:
        return 10.0, ratio, "near side at least 2x larger"
    if ratio >= 1.5:
        return 7.0, ratio, "near side clearly larger"
    if ratio >= 1.0:
        return 3.0, ratio, "near side slightly larger or equal"
    if ratio >= 0.67:
        return 0.0, ratio, "near side mildly smaller; no penalty"
    if ratio >= 0.5:
        return -5.0, ratio, "near side significantly smaller"
    return -10.0, ratio, "near side less than half the opposite side"

def build_opportunities(rows, limit=30):
    tf_values = _tf_liquidity(rows)
    consensus_map = {x["symbol"]: x for x in analysis.calculate_consensus(rows, min_hits=1, limit=500)}
    setup_map = {x["symbol"]: x for x in decision_engine.calculate_scores(rows, limit=500)}
    out = []

    for r in rows:
        symbol = str(_get(r, "symbol", "")).upper()
        tf = str(_get(r, "timeframe", ""))
        side = _closest_side(r)
        dist = _closest_distance(r)
        if not symbol or not tf or not side or dist is None:
            continue

        near = float(_amount(r, side) or 0.0)
        far = float(_other_amount(r, side) or 0.0)
        gap = _gap_pct(r)
        liq_pct = _percentile(near, tf_values.get(tf, []))
        bal_points, ratio, bal_reason = _balance_points(near, far)

        cons = consensus_map.get(symbol, {})
        hits = int(cons.get("hits", 0) or 0)
        total = int(cons.get("total", 0) or 0)
        setup = setup_map.get(symbol, {}).get("setup_strength")

        types = []
        if dist <= 0.75:
            types.append("NEAR_MAX_PAIN")
        if ratio is not None and ratio >= 2:
            types.append("LIQUIDITY_IMBALANCE_NEAR_SIDE")
        if gap is not None and gap >= 20:
            types.append("EXTREME_GAP")
        if dist <= 1.0 and liq_pct >= 0.80:
            types.append("HIGH_LIQUIDITY_CLOSE_DISTANCE")
        if setup is not None and setup >= 75:
            types.append("HIGH_SETUP_STRENGTH")
        if not types:
            continue

        components = {
            "distance": _distance_points(dist),
            "liquidity_rank": round(liq_pct * 25, 2),
            "consensus": round((hits / total) * 20, 2) if total else 0.0,
            "setup_strength": round((min(max(setup or 0, 0), 100) / 100) * 15, 2),
            "liquidity_balance": bal_points,
        }
        priority = max(0.0, min(100.0, round(sum(components.values()), 2)))

        out.append({
            "symbol": symbol,
            "timeframe": tf,
            "side": side,
            "types": types,
            "priority": priority,
            "distance_pct": dist,
            "near_far_ratio": ratio,
            "consensus_hits": hits,
            "consensus_total": total,
            "setup_strength": setup,
            "liquidity_percentile": liq_pct,
            "components": components,
            "balance_reason": bal_reason,
        })

    out.sort(key=lambda x: (-x["priority"], x["distance_pct"], x["symbol"]))
    return out[:limit]
