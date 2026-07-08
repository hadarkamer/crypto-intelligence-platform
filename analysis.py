"""Central analysis engine for the crypto Max Pain bot.

This module is intentionally independent of Telegram and database code.
It receives rows from the latest DB snapshot and returns plain Python
structures that command handlers can display.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional


TIMEFRAMES = ["12h", "24h", "48h", "3d", "1w", "2w", "1m"]


def _get(row: Any, key: str, default=None):
    """Support sqlite Row, psycopg dict-like rows, and normal dicts."""
    try:
        return row[key]
    except Exception:
        return default


def safe_avg(values: Iterable[Optional[float]]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    return sum(vals) / len(vals) if vals else None


def side_from_distances(distance_short_pct, distance_long_pct) -> Optional[str]:
    """Return the closer Max Pain side for one row."""
    if distance_short_pct is None or distance_long_pct is None:
        return None
    return "SHORT" if abs(distance_short_pct) <= abs(distance_long_pct) else "LONG"


def group_by_symbol(rows: Iterable[Any]) -> Dict[str, List[Any]]:
    grouped: Dict[str, List[Any]] = defaultdict(list)
    for row in rows:
        symbol = _get(row, "symbol")
        if symbol:
            grouped[str(symbol).upper()].append(row)
    return dict(grouped)


def calculate_consensus(rows: Iterable[Any], min_hits: int = 7, limit: int = 20) -> List[Dict[str, Any]]:
    """Coins whose closest Max Pain side is consistent across most/all timeframes."""
    results: List[Dict[str, Any]] = []

    for symbol, items in group_by_symbol(rows).items():
        sides = []
        short_dists = []
        long_dists = []
        active_tfs = []

        for row in items:
            ds = _get(row, "distance_short_pct")
            dl = _get(row, "distance_long_pct")
            side = side_from_distances(ds, dl)
            if not side:
                continue
            sides.append(side)
            short_dists.append(abs(ds))
            long_dists.append(abs(dl))
            active_tfs.append(_get(row, "timeframe"))

        if not sides:
            continue

        short_count = sides.count("SHORT")
        long_count = sides.count("LONG")
        if short_count >= long_count:
            dominant_side = "SHORT"
            hits = short_count
            avg_dist = safe_avg(short_dists)
        else:
            dominant_side = "LONG"
            hits = long_count
            avg_dist = safe_avg(long_dists)

        total = len(sides)
        if hits < min_hits:
            continue

        results.append({
            "symbol": symbol,
            "side": dominant_side,
            "hits": hits,
            "total": total,
            "avg_dist": avg_dist,
            "tfs": ",".join([str(x) for x in active_tfs if x]),
        })

    results.sort(key=lambda x: (-x["hits"], -(x["avg_dist"] if x["avg_dist"] is not None else -1), x["symbol"]))
    return results[:limit]


def calculate_gap(rows: Iterable[Any], limit: int = 20) -> List[Dict[str, Any]]:
    """Average % gap between Short Max Pain and Long Max Pain.

    Formula approved by Yoni:
        gap_pct = abs(short_max_pain - long_max_pain) / current_price * 100

    Notes:
    - This can be very high for low-priced/high-volatility assets.
    - The function also returns avg_gap_abs for sanity-checking.
    """
    results: List[Dict[str, Any]] = []

    for symbol, items in group_by_symbol(rows).items():
        gaps_pct = []
        gaps_abs = []
        max_gap = None
        max_gap_tf = None
        min_gap = None
        min_gap_tf = None

        for row in items:
            price = _get(row, "current_price")
            short_mp = _get(row, "short_max_pain")
            long_mp = _get(row, "long_max_pain")
            if not price or price == 0 or short_mp is None or long_mp is None:
                continue

            gap_abs = abs(short_mp - long_mp)
            gap_pct = gap_abs / price * 100
            gaps_abs.append(gap_abs)
            gaps_pct.append(gap_pct)

            tf = _get(row, "timeframe")
            if max_gap is None or gap_pct > max_gap:
                max_gap = gap_pct
                max_gap_tf = tf
            if min_gap is None or gap_pct < min_gap:
                min_gap = gap_pct
                min_gap_tf = tf

        if not gaps_pct:
            continue

        results.append({
            "symbol": symbol,
            "count": len(gaps_pct),
            "avg_gap": sum(gaps_pct) / len(gaps_pct),
            "avg_gap_abs": sum(gaps_abs) / len(gaps_abs),
            "max_gap": max_gap,
            "max_gap_tf": max_gap_tf,
            "min_gap": min_gap,
            "min_gap_tf": min_gap_tf,
        })

    results.sort(key=lambda x: (-x["avg_gap"], x["symbol"]))
    return results[:limit]


def calculate_liquidity_balance(rows: Iterable[Any]) -> Dict[str, Any]:
    """Sum short/long liquidation amounts by timeframe + total."""
    by_tf: Dict[str, Dict[str, float]] = defaultdict(lambda: {"short_total": 0.0, "long_total": 0.0})

    for row in rows:
        tf = _get(row, "timeframe")
        if not tf:
            continue
        short_amount = _get(row, "short_liquidation_amount") or 0.0
        long_amount = _get(row, "long_liquidation_amount") or 0.0
        by_tf[str(tf)]["short_total"] += short_amount
        by_tf[str(tf)]["long_total"] += long_amount

    timeframe_rows = []
    total_short = 0.0
    total_long = 0.0

    for tf in sorted(by_tf.keys(), key=lambda x: TIMEFRAMES.index(x) if x in TIMEFRAMES else 99):
        short_total = by_tf[tf]["short_total"]
        long_total = by_tf[tf]["long_total"]
        total_short += short_total
        total_long += long_total
        timeframe_rows.append(_liquidity_row(tf, short_total, long_total))

    return {
        "timeframes": timeframe_rows,
        "total": _liquidity_row("TOTAL", total_short, total_long),
    }


def _liquidity_row(tf: str, short_total: float, long_total: float) -> Dict[str, Any]:
    diff = long_total - short_total

    if long_total > short_total:
        dominant = "LONG"
        ratio = long_total / short_total if short_total else None
    elif short_total > long_total:
        dominant = "SHORT"
        ratio = short_total / long_total if long_total else None
    else:
        dominant = "BALANCED"
        ratio = 1.0

    return {
        "timeframe": tf,
        "short_total": short_total,
        "long_total": long_total,
        "dominant": dominant,
        "diff": diff,
        "ratio": ratio,
    }



def calculate_liquidity_by_coin(rows: Iterable[Any], symbol_filter: Optional[str] = None, limit: int = 20) -> List[Dict[str, Any]]:
    """Sum liquidity per coin across all timeframes, optionally only one symbol."""
    results: List[Dict[str, Any]] = []

    for symbol, items in group_by_symbol(rows).items():
        if symbol_filter and symbol != symbol_filter.upper():
            continue

        short_total = sum((_get(row, "short_liquidation_amount") or 0.0) for row in items)
        long_total = sum((_get(row, "long_liquidation_amount") or 0.0) for row in items)
        total = short_total + long_total
        diff = long_total - short_total

        if total <= 0:
            continue

        if long_total > short_total:
            dominant = "LONG"
            ratio = long_total / short_total if short_total else None
        elif short_total > long_total:
            dominant = "SHORT"
            ratio = short_total / long_total if long_total else None
        else:
            dominant = "BALANCED"
            ratio = 1.0

        results.append({
            "symbol": symbol,
            "short_total": short_total,
            "long_total": long_total,
            "total": total,
            "dominant": dominant,
            "diff": diff,
            "ratio": ratio,
            "count": len(items),
        })

    results.sort(key=lambda x: (-x["total"], x["symbol"]))
    return results[:limit]


def calculate_liquidity_for_symbol_by_timeframe(rows: Iterable[Any], symbol: str) -> Dict[str, Any]:
    """Liquidity balance for one coin, by timeframe + total."""
    symbol = symbol.upper()
    filtered = []
    for row in rows:
        if str(_get(row, "symbol", "")).upper() == symbol:
            filtered.append(row)
    return calculate_liquidity_balance(filtered)


def calculate_btc_similarity(rows: Iterable[Any], min_hits: int = 5, limit: int = 20) -> List[Dict[str, Any]]:
    """Find coins whose closer Max Pain side matches BTC across timeframes."""
    grouped = group_by_symbol(rows)
    btc_rows = grouped.get("BTC", [])
    btc_by_tf: Dict[str, str] = {}

    for row in btc_rows:
        tf = _get(row, "timeframe")
        side = side_from_distances(_get(row, "distance_short_pct"), _get(row, "distance_long_pct"))
        if tf and side:
            btc_by_tf[str(tf)] = side

    if not btc_by_tf:
        return []

    results: List[Dict[str, Any]] = []
    for symbol, items in grouped.items():
        if symbol == "BTC":
            continue

        same = []
        different = []
        compared = 0

        for row in items:
            tf = _get(row, "timeframe")
            if not tf or str(tf) not in btc_by_tf:
                continue
            side = side_from_distances(_get(row, "distance_short_pct"), _get(row, "distance_long_pct"))
            if not side:
                continue

            compared += 1
            if side == btc_by_tf[str(tf)]:
                same.append(str(tf))
            else:
                different.append(str(tf))

        hits = len(same)
        if hits < min_hits:
            continue

        results.append({
            "symbol": symbol,
            "hits": hits,
            "total": compared,
            "same_tfs": ",".join(same),
            "different_tfs": ",".join(different) if different else "-",
        })

    results.sort(key=lambda x: (-x["hits"], x["symbol"]))
    return results[:limit]


def calculate_market_bias(rows: Iterable[Any]) -> Dict[str, Any]:
    """Reserved for Stage 4. Counts LONG/SHORT closer side by timeframe."""
    counts: Dict[str, Dict[str, int]] = defaultdict(lambda: {"LONG": 0, "SHORT": 0})
    for row in rows:
        tf = _get(row, "timeframe")
        side = side_from_distances(_get(row, "distance_short_pct"), _get(row, "distance_long_pct"))
        if tf and side:
            counts[str(tf)][side] += 1

    results = []
    total_long = 0
    total_short = 0
    for tf in sorted(counts.keys(), key=lambda x: TIMEFRAMES.index(x) if x in TIMEFRAMES else 99):
        long_count = counts[tf]["LONG"]
        short_count = counts[tf]["SHORT"]
        total = long_count + short_count
        total_long += long_count
        total_short += short_count
        results.append({
            "timeframe": tf,
            "long_count": long_count,
            "short_count": short_count,
            "long_pct": (long_count / total * 100) if total else None,
            "short_pct": (short_count / total * 100) if total else None,
            "bias": "LONG" if long_count > short_count else "SHORT" if short_count > long_count else "NEUTRAL",
        })

    total = total_long + total_short
    return {
        "timeframes": results,
        "overall": {
            "long_count": total_long,
            "short_count": total_short,
            "long_pct": (total_long / total * 100) if total else None,
            "short_pct": (total_short / total * 100) if total else None,
            "bias": "LONG" if total_long > total_short else "SHORT" if total_short > total_long else "NEUTRAL",
        },
    }
