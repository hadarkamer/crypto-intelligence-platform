from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional

import analysis


def _get(row: Any, key: str, default=None):
    try:
        return row[key]
    except Exception:
        return default


def _group_by_symbol(rows: Iterable[Any]) -> Dict[str, List[Any]]:
    grouped: Dict[str, List[Any]] = defaultdict(list)
    for row in rows:
        symbol = _get(row, "symbol")
        if symbol:
            grouped[str(symbol).upper()].append(row)
    return dict(grouped)


def _dominant_side(items: List[Any]) -> Dict[str, Any]:
    sides = []
    for row in items:
        ds = _get(row, "distance_short_pct")
        dl = _get(row, "distance_long_pct")
        side = analysis.side_from_distances(ds, dl)
        if side:
            sides.append(side)

    if not sides:
        return {"side": "NEUTRAL", "hits": 0, "total": 0, "avg_distance": None}

    short_hits = sides.count("SHORT")
    long_hits = sides.count("LONG")

    if short_hits > long_hits:
        side = "SHORT"
        hits = short_hits
    elif long_hits > short_hits:
        side = "LONG"
        hits = long_hits
    else:
        side = "NEUTRAL"
        hits = short_hits

    matching_distances = []
    for row in items:
        ds = _get(row, "distance_short_pct")
        dl = _get(row, "distance_long_pct")
        row_side = analysis.side_from_distances(ds, dl)
        if row_side == side:
            matching_distances.append(abs(ds) if side == "SHORT" else abs(dl))

    return {"side": side, "hits": hits, "total": len(sides), "avg_distance": analysis.safe_avg(matching_distances)}


def _liquidity_for_symbol(items: List[Any]) -> Dict[str, Any]:
    short_total = sum((_get(row, "short_liquidation_amount") or 0.0) for row in items)
    long_total = sum((_get(row, "long_liquidation_amount") or 0.0) for row in items)
    total = short_total + long_total

    if total <= 0:
        return {"dominant": "NEUTRAL", "ratio": None, "short_total": short_total, "long_total": long_total, "total": total}

    if long_total > short_total:
        dominant = "LONG"
        ratio = long_total / short_total if short_total else None
    elif short_total > long_total:
        dominant = "SHORT"
        ratio = short_total / long_total if long_total else None
    else:
        dominant = "NEUTRAL"
        ratio = 1.0

    return {"dominant": dominant, "ratio": ratio, "short_total": short_total, "long_total": long_total, "total": total}


def _gap_for_symbol(items: List[Any]) -> Optional[float]:
    gaps = []
    for row in items:
        price = _get(row, "current_price")
        short_mp = _get(row, "short_max_pain")
        long_mp = _get(row, "long_max_pain")
        if price and short_mp is not None and long_mp is not None:
            gaps.append(abs(short_mp - long_mp) / price * 100)
    return analysis.safe_avg(gaps)


def _market_bias(rows: List[Any]) -> str:
    overall = analysis.calculate_market_bias(rows).get("overall", {})
    return overall.get("bias", "NEUTRAL")


def _btc_similarity(rows: List[Any], symbol: str) -> Dict[str, Any]:
    if symbol == "BTC":
        return {"hits": 7, "total": 7, "reason": "BTC reference coin"}

    sims = analysis.calculate_btc_similarity(rows, min_hits=1, limit=500)
    for s in sims:
        if s["symbol"] == symbol:
            return {"hits": s["hits"], "total": s["total"], "reason": f"{s['hits']}/{s['total']} timeframes match BTC"}
    return {"hits": 0, "total": 7, "reason": "No BTC similarity"}


def calculate_score_for_symbol(rows: List[Any], symbol: str) -> Dict[str, Any]:
    symbol = symbol.upper()
    grouped = _group_by_symbol(rows)
    items = grouped.get(symbol, [])
    if not items:
        return {"symbol": symbol, "ok": False, "error": "symbol not found"}

    dominant = _dominant_side(items)
    side = dominant["side"]
    hits = dominant["hits"]
    total = dominant["total"]
    avg_distance = dominant["avg_distance"]

    components = []

    consensus_score = round((hits / total) * 35, 2) if total else 0
    components.append({
        "name": "CONSENSUS",
        "score": consensus_score,
        "max": 35,
        "direction": side,
        "reason": f"{hits}/{total} timeframes point {side}",
    })

    near_score = 0
    if avg_distance is not None:
        if avg_distance <= 0.5:
            near_score = 25
        elif avg_distance >= 3:
            near_score = 0
        else:
            near_score = round((3 - avg_distance) / 2.5 * 25, 2)
    components.append({
        "name": "NEAR_MAX_PAIN",
        "score": near_score,
        "max": 25,
        "direction": side,
        "reason": f"Avg distance {avg_distance:.2f}%" if avg_distance is not None else "No distance",
    })

    liq = _liquidity_for_symbol(items)
    liq_score = 0
    if liq["dominant"] == side and liq["ratio"]:
        if liq["ratio"] >= 3:
            liq_score = 20
        elif liq["ratio"] >= 2:
            liq_score = 15
        elif liq["ratio"] >= 1.5:
            liq_score = 10
        elif liq["ratio"] >= 1.2:
            liq_score = 5
    liq_reason = f"{liq['dominant']} ratio {liq['ratio']:.2f}x" if liq["ratio"] else "No liquidity ratio"
    components.append({"name": "LIQUIDITY_BALANCE", "score": liq_score, "max": 20, "direction": liq["dominant"], "reason": liq_reason})

    market = _market_bias(rows)
    market_score = 10 if market == side else 4 if market == "NEUTRAL" or side == "NEUTRAL" else 0
    components.append({"name": "MARKET_BIAS", "score": market_score, "max": 10, "direction": market, "reason": f"Overall market bias is {market}"})

    btc = _btc_similarity(rows, symbol)
    btc_score = round((btc["hits"] / btc["total"]) * 10, 2) if btc["total"] else 0
    components.append({"name": "BTC_LIKE", "score": btc_score, "max": 10, "direction": "MATCH", "reason": btc["reason"]})

    total_score = round(sum(c["score"] for c in components), 2)
    if total_score >= 75:
        confidence = "HIGH"
    elif total_score >= 55:
        confidence = "MEDIUM"
    elif total_score >= 35:
        confidence = "LOW"
    else:
        confidence = "WEAK"

    return {
        "symbol": symbol,
        "ok": True,
        "direction": side,
        "setup_strength": total_score,
        "confidence": confidence,
        "consensus_hits": hits,
        "consensus_total": total,
        "avg_distance": avg_distance,
        "gap_avg_pct": _gap_for_symbol(items),
        "liquidity": liq,
        "components": components,
    }


def calculate_scores(rows: List[Any], limit: int = 20) -> List[Dict[str, Any]]:
    results = []
    for symbol in _group_by_symbol(rows).keys():
        result = calculate_score_for_symbol(rows, symbol)
        if result.get("ok"):
            results.append(result)
    results.sort(key=lambda x: (-x["setup_strength"], x["symbol"]))
    return results[:limit]
