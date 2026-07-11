"""Alert priority engine — corrected formula.

Important:
- Setup Strength is NOT used in alert priority.
- Liquidity is normalized inside each coin/timeframe, not ranked against larger coins.
- Liquidity balance is explicitly calculated and exposed in the output.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional

import analysis


def _get(row: Any, key: str, default=None):
    try:
        return row[key]
    except Exception:
        return default


def _closest_side(row: Any) -> Optional[str]:
    return analysis.side_from_distances(
        _get(row, "distance_short_pct"),
        _get(row, "distance_long_pct"),
    )


def _closest_distance(row: Any) -> Optional[float]:
    side = _closest_side(row)
    if side == "SHORT":
        value = _get(row, "distance_short_pct")
    elif side == "LONG":
        value = _get(row, "distance_long_pct")
    else:
        return None
    return abs(value) if value is not None else None


def _amount_for_side(row: Any, side: str) -> float:
    if side == "SHORT":
        return float(_get(row, "short_liquidation_amount") or 0.0)
    if side == "LONG":
        return float(_get(row, "long_liquidation_amount") or 0.0)
    return 0.0


def _opposite_amount(row: Any, side: str) -> float:
    if side == "SHORT":
        return float(_get(row, "long_liquidation_amount") or 0.0)
    if side == "LONG":
        return float(_get(row, "short_liquidation_amount") or 0.0)
    return 0.0


def _gap_pct(row: Any) -> Optional[float]:
    price = _get(row, "current_price")
    short_mp = _get(row, "short_max_pain")
    long_mp = _get(row, "long_max_pain")
    if not price or short_mp is None or long_mp is None:
        return None
    return abs(short_mp - long_mp) / price * 100


def _distance_points(distance_pct: Optional[float]) -> float:
    """0..35 points. Full score at <=0.25%; zero at >=1.50%."""
    if distance_pct is None:
        return 0.0
    if distance_pct <= 0.25:
        return 35.0
    if distance_pct >= 1.50:
        return 0.0
    return round((1.50 - distance_pct) / 1.25 * 35.0, 2)


def _consensus_points(hits: int, total: int) -> float:
    """0..20 points."""
    if not total:
        return 0.0
    return round((hits / total) * 20.0, 2)


def _liquidity_metrics(near_amount: float, far_amount: float) -> Dict[str, Any]:
    """Normalize liquidity inside the same coin/timeframe.

    near_share_pct = near / (near + far) * 100

    This avoids comparing BTC's absolute dollars with smaller assets.
    The liquidity score is 0..25 and moves continuously:
    - Near side larger -> more points
    - Nearly equal -> neutral-middle score
    - Near side significantly smaller -> fewer points
    """
    total = near_amount + far_amount
    if total <= 0:
        return {
            "near_share_pct": None,
            "near_far_ratio": None,
            "points": 0.0,
            "meaning": "no liquidity data",
        }

    share = near_amount / total
    ratio = (near_amount / far_amount) if far_amount > 0 else None
    points = round(share * 25.0, 2)

    if share >= 0.67:
        meaning = "near side clearly dominant"
    elif share >= 0.50:
        meaning = "near side equal or moderately larger"
    elif share >= 0.40:
        meaning = "near side mildly smaller"
    else:
        meaning = "near side significantly smaller"

    return {
        "near_share_pct": share * 100.0,
        "near_far_ratio": ratio,
        "points": points,
        "meaning": meaning,
    }


def _consensus_map(rows: List[Any]) -> Dict[str, Dict[str, Any]]:
    results = analysis.calculate_consensus(rows, min_hits=1, limit=500)
    return {item["symbol"]: item for item in results}



def _btc_similarity_map(rows: List[Any]) -> Dict[str, Dict[str, Any]]:
    return {x["symbol"]: x for x in analysis.calculate_btc_similarity(rows, min_hits=1, limit=500)}


def _btc_like_points(symbol: str, btc_map: Dict[str, Dict[str, Any]]) -> float:
    if symbol == "BTC":
        return 15.0
    item = btc_map.get(symbol, {})
    total = int(item.get("total", 0) or 0)
    hits = int(item.get("hits", 0) or 0)
    return round((hits / total) * 15.0, 2) if total else 0.0


def _cluster_metrics(symbol_rows: List[Any], side: str, target: Optional[float]) -> Dict[str, Any]:
    targets = []
    for row in symbol_rows:
        row_side = _closest_side(row)
        if row_side != side:
            continue
        value = _get(row, "short_max_pain") if side == "SHORT" else _get(row, "long_max_pain")
        if value is not None:
            targets.append(float(value))
    if target is None or len(targets) < 2:
        return {"hits": len(targets), "spread_pct": None, "points": 0.0}
    center = sum(targets) / len(targets)
    if not center:
        return {"hits": len(targets), "spread_pct": None, "points": 0.0}
    spread = (max(targets) - min(targets)) / abs(center) * 100.0
    density = min(len(targets) / 7.0, 1.0)
    tightness = max(0.0, 1.0 - min(spread, 3.0) / 3.0)
    return {"hits": len(targets), "spread_pct": spread, "points": round(10.0 * density * tightness, 2)}


def _data_quality(row: Any, symbol_rows: List[Any]) -> List[str]:
    issues = []
    present = {str(_get(x, "timeframe", "")) for x in symbol_rows}
    missing = [tf for tf in analysis.TIMEFRAMES if tf not in present]
    if missing:
        issues.append("חסרים טווחי זמן: " + ", ".join(missing))
    if _get(row, "current_price") is None:
        issues.append("מחיר Binance חסר")
    if _get(row, "short_max_pain") is None or _get(row, "long_max_pain") is None:
        issues.append("Max Pain חסר או חלקי")
    if _get(row, "short_liquidation_amount") is None or _get(row, "long_liquidation_amount") is None:
        issues.append("נתוני נזילות חלקיים")
    validation = _get(row, "validation_errors")
    if validation:
        issues.append(str(validation))
    return issues

def build_opportunities(rows: List[Any], limit: int = 30) -> List[Dict[str, Any]]:
    """Build one alert opportunity per coin/timeframe."""
    consensus = _consensus_map(rows)
    btc_map = _btc_similarity_map(rows)
    grouped = analysis.group_by_symbol(rows)
    out: List[Dict[str, Any]] = []

    for row in rows:
        symbol = str(_get(row, "symbol", "")).upper()
        timeframe = str(_get(row, "timeframe", ""))
        side = _closest_side(row)
        distance = _closest_distance(row)

        if not symbol or not timeframe or not side or distance is None:
            continue

        near_amount = _amount_for_side(row, side)
        far_amount = _opposite_amount(row, side)
        liq = _liquidity_metrics(near_amount, far_amount)
        gap = _gap_pct(row)
        symbol_rows = grouped.get(symbol, [])
        target = _get(row, "short_max_pain") if side == "SHORT" else _get(row, "long_max_pain")
        cluster = _cluster_metrics(symbol_rows, side, target)

        cons = consensus.get(symbol, {})
        hits = int(cons.get("hits", 0) or 0)
        total = int(cons.get("total", 0) or 0)

        types: List[str] = []

        if distance <= 0.75:
            types.append("NEAR_MAX_PAIN")

        ratio = liq["near_far_ratio"]
        if ratio is not None and ratio >= 2.0:
            types.append("LIQUIDITY_IMBALANCE_NEAR_SIDE")

        if distance <= 1.0 and liq["near_share_pct"] is not None and liq["near_share_pct"] >= 50.0:
            types.append("HIGH_LIQUIDITY_CLOSE_DISTANCE")

        if gap is not None and gap >= 20.0:
            types.append("EXTREME_GAP")

        if not types:
            continue

        balance_points = round((1.0 - abs((liq["near_share_pct"] or 50.0) - 50.0) / 50.0) * 10.0, 2) if liq["near_share_pct"] is not None else 0.0
        concentration_points = round((liq["near_share_pct"] or 0.0) / 100.0 * 10.0, 2)
        components = {
            "distance": _distance_points(distance),
            "consensus": _consensus_points(hits, total),
            "btc_like": _btc_like_points(symbol, btc_map),
            "liquidity_balance": balance_points,
            "liquidity_concentration": concentration_points,
            "cluster": cluster["points"],
        }

        priority = round(sum(components.values()), 2)
        priority = max(0.0, min(100.0, priority))

        out.append({
            "symbol": symbol,
            "timeframe": timeframe,
            "side": side,
            "types": types,
            "priority": priority,
            "distance_pct": distance,
            "near_amount": near_amount,
            "far_amount": far_amount,
            "near_share_pct": liq["near_share_pct"],
            "near_far_ratio": liq["near_far_ratio"],
            "liquidity_meaning": liq["meaning"],
            "consensus_hits": hits,
            "consensus_total": total,
            "gap_pct": gap,
            "cluster_hits": cluster["hits"],
            "cluster_spread_pct": cluster["spread_pct"],
            "data_quality_issues": _data_quality(row, symbol_rows),
            "components": components,
        })

    out.sort(
        key=lambda x: (
            -x["priority"],
            x["distance_pct"],
            x["symbol"],
            x["timeframe"],
        )
    )
    return out[:limit]
