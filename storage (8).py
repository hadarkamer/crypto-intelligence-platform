from typing import Dict, Any, List, Optional
from .storage import query

def pct_change(new: Optional[float], old: Optional[float]) -> Optional[float]:
    if new is None or old is None or old == 0:
        return None
    return ((new - old) / old) * 100

def abs_diff(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None:
        return None
    return a - b

def distance_abs(price: Optional[float], target: Optional[float]) -> Optional[float]:
    if price is None or target is None:
        return None
    return abs(target - price)

def distance_pct(price: Optional[float], target: Optional[float]) -> Optional[float]:
    if price is None or target is None or price == 0:
        return None
    return abs((target - price) / price) * 100

def alert_level(delta_short_pct: Optional[float], delta_long_pct: Optional[float]) -> str:
    values = [abs(v) for v in [delta_short_pct, delta_long_pct] if v is not None]
    if not values:
        return "none"
    max_delta = max(values)
    if max_delta >= 7:
        return "high"
    if max_delta >= 3:
        return "medium"
    if max_delta >= 1:
        return "low"
    return "none"

def previous_row(symbol: str, timeframe: str, before_collected_at: str):
    rows = query(
        """
        SELECT * FROM max_pain_snapshots
        WHERE symbol = ? AND timeframe = ? AND collected_at < ?
        ORDER BY collected_at DESC
        LIMIT 1
        """,
        (symbol, timeframe, before_collected_at)
    )
    return rows[0] if rows else None

def enrich_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    enriched = []
    for row in rows:
        price = row.get("current_price")
        short_mp = row.get("short_max_pain")
        long_mp = row.get("long_max_pain")

        row["distance_short_abs"] = distance_abs(price, short_mp)
        row["distance_short_pct"] = distance_pct(price, short_mp)
        row["distance_long_abs"] = distance_abs(price, long_mp)
        row["distance_long_pct"] = distance_pct(price, long_mp)

        prev = previous_row(row["symbol"], row["timeframe"], row["collected_at"])
        if prev:
            row["delta_short_abs"] = abs_diff(short_mp, prev["short_max_pain"])
            row["delta_short_pct"] = pct_change(short_mp, prev["short_max_pain"])
            row["delta_long_abs"] = abs_diff(long_mp, prev["long_max_pain"])
            row["delta_long_pct"] = pct_change(long_mp, prev["long_max_pain"])
        else:
            row["delta_short_abs"] = None
            row["delta_short_pct"] = None
            row["delta_long_abs"] = None
            row["delta_long_pct"] = None

        row["alert_level"] = alert_level(row["delta_short_pct"], row["delta_long_pct"])
        enriched.append(row)
    return enriched
