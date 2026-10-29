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
    """Return the closest still-active Max Pain side.

    Distances are signed and recalculated from the live Binance price:
    - SHORT Max Pain is active only while it is above price (positive distance).
    - LONG Max Pain is active only while it is below price (negative distance).

    A target that has already been crossed is excluded rather than being treated
    as a nearby opportunity on the opposite side of the market.
    """
    valid = []
    if distance_short_pct is not None and float(distance_short_pct) > 0:
        valid.append(("SHORT", abs(float(distance_short_pct))))
    if distance_long_pct is not None and float(distance_long_pct) < 0:
        valid.append(("LONG", abs(float(distance_long_pct))))
    if not valid:
        return None
    return min(valid, key=lambda item: item[1])[0]


def group_by_symbol(rows: Iterable[Any]) -> Dict[str, List[Any]]:
    grouped: Dict[str, List[Any]] = defaultdict(list)
    for row in rows:
        symbol = _get(row, "symbol")
        if symbol:
            grouped[str(symbol).upper()].append(row)
    return dict(grouped)


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
