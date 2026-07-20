"""Independent counter-direction scoring for displayed alerts only.

The primary opportunity score remains untouched in alert_engine.py.  This
module evaluates the opposite direction only when an alert is actually being
formatted for /alerts or Watch.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

import alert_engine


def _get(row: Any, key: str, default=None):
    try:
        return row[key]
    except Exception:
        return default


def _opposite_side(side: str) -> Optional[str]:
    side = str(side or "").upper()
    if side == "LONG":
        return "SHORT"
    if side == "SHORT":
        return "LONG"
    return None


def _find_row(rows: Iterable[Any], symbol: str, timeframe: str) -> Optional[Any]:
    for row in rows:
        if (
            str(_get(row, "symbol", "") or "").upper() == symbol
            and str(_get(row, "timeframe", "") or "") == timeframe
        ):
            return row
    return None


def _active_distance(row: Any, side: str) -> Optional[float]:
    price = _get(row, "current_price")
    target_key = "short_max_pain" if side == "SHORT" else "long_max_pain"
    target = _get(row, target_key)
    if price is None or target is None or float(price) <= 0:
        return None

    signed = (float(target) - float(price)) / float(price) * 100.0
    if side == "SHORT" and signed <= 0:
        return None
    if side == "LONG" and signed >= 0:
        return None
    return abs(signed)


def _counter_gap_points(row: Any, counter_side: str) -> Dict[str, Optional[float]]:
    """0..15 advantage for the counter target versus the other active target."""
    counter_distance = _active_distance(row, counter_side)
    other_side = _opposite_side(counter_side)
    other_distance = _active_distance(row, other_side) if other_side else None
    if counter_distance is None or other_distance is None or other_distance <= 0:
        return {"advantage": None, "points": 0.0}

    advantage = max(
        0.0,
        min(1.0, (other_distance - counter_distance) / other_distance),
    )
    return {"advantage": advantage, "points": round(advantage * 15.0, 2)}


def calculate_counter_score(
    primary_item: Dict[str, Any],
    rows: Iterable[Any],
    all_items: Optional[Iterable[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Calculate the opposite-direction score for one displayed alert.

    Uses the same score components and ranges as the primary engine, but does
    not modify or replace the primary score.  It is intentionally called only
    for alerts selected for Telegram output.
    """
    symbol = str(primary_item.get("symbol") or "").upper()
    timeframe = str(primary_item.get("timeframe") or "")
    primary_side = str(primary_item.get("side") or "").upper()
    counter_side = _opposite_side(primary_side)

    if not symbol or not timeframe or counter_side is None:
        return {"available": False, "reason": "invalid_primary_alert"}

    deduped_rows, _ = alert_engine._dedupe_rows(list(rows))
    row = _find_row(deduped_rows, symbol, timeframe)
    if row is None:
        return {
            "available": False,
            "side": counter_side,
            "reason": "row_not_found",
        }

    distance = _active_distance(row, counter_side)
    if distance is None:
        return {
            "available": False,
            "side": counter_side,
            "reason": "counter_target_inactive_or_crossed",
        }

    consensus = alert_engine._consensus_map(deduped_rows).get(symbol, {})
    consensus_hits = int(consensus.get(counter_side, 0) or 0)
    consensus_total = int(consensus.get("total", 0) or 0)

    btc_reference = None
    if symbol != "BTC":
        available_items = list(all_items or [])
        if not available_items:
            available_items = alert_engine.build_opportunities(
                deduped_rows, limit=500
            )
        btc_item = next(
            (
                item for item in available_items
                if item.get("symbol") == "BTC"
                and item.get("timeframe") == timeframe
            ),
            None,
        )
        if btc_item:
            btc_reference = {
                "side": btc_item.get("side"),
                "score": btc_item.get("score", 0.0),
            }

    directional = alert_engine._directional_alignment(
        symbol,
        consensus_hits,
        consensus_total,
        counter_side,
        btc_reference,
    )
    allowed_distance = alert_engine._allowed_distance_pct(
        symbol, _get(row, "rank")
    )
    target_proximity = alert_engine._target_proximity_points(
        distance, allowed_distance
    )
    cluster = alert_engine._cluster_map(deduped_rows).get(symbol, {}).get(
        counter_side,
        {"points": 0.0, "count": 0, "members": []},
    )
    gap = _counter_gap_points(row, counter_side)

    components = {
        "directional_alignment": float(directional.get("total", 0.0) or 0.0),
        "target_proximity": float(target_proximity or 0.0),
        "cluster_confidence": float(cluster.get("points", 0.0) or 0.0),
        "relative_gap": float(gap.get("points", 0.0) or 0.0),
    }
    score = round(max(0.0, min(100.0, sum(components.values()))), 2)

    return {
        "available": True,
        "side": counter_side,
        "score": score,
        "distance_pct": round(distance, 4),
        "allowed_distance_pct": allowed_distance,
        "consensus_hits": consensus_hits,
        "consensus_total": consensus_total,
        "components": components,
        "cluster_count": int(cluster.get("count", 0) or 0),
        "cluster_members": list(cluster.get("members", []) or []),
        "btc_reference_side": directional.get("btc_reference_side"),
        "btc_reference_score": directional.get("btc_reference_score"),
    }
