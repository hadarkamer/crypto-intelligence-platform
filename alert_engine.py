"""Manual alert engine for the crypto Max Pain bot.

Stage 9:
- Manual alert scan only: /alert_check
- Uses latest saved Max Pain snapshot.
- Does not run automatically yet.
- Does not write alert history yet.
- No Hyperliquid dependency.

Alert types:
1. NEAR_MAX_PAIN
2. LIQUIDITY_IMBALANCE_NEAR_SIDE
3. EXTREME_GAP
4. HIGH_LIQUIDITY_CLOSE_DISTANCE
5. HIGH_SETUP_STRENGTH
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional

import analysis
import decision_engine


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


def _closest_side(row: Any) -> Optional[str]:
    return analysis.side_from_distances(
        _get(row, "distance_short_pct"),
        _get(row, "distance_long_pct"),
    )


def _closest_distance(row: Any) -> Optional[float]:
    side = _closest_side(row)
    if side == "SHORT":
        val = _get(row, "distance_short_pct")
    elif side == "LONG":
        val = _get(row, "distance_long_pct")
    else:
        return None
    return abs(val) if val is not None else None


def _amount_for_side(row: Any, side: str) -> Optional[float]:
    if side == "SHORT":
        return _get(row, "short_liquidation_amount")
    if side == "LONG":
        return _get(row, "long_liquidation_amount")
    return None


def _other_amount(row: Any, side: str) -> Optional[float]:
    if side == "SHORT":
        return _get(row, "long_liquidation_amount")
    if side == "LONG":
        return _get(row, "short_liquidation_amount")
    return None


def _gap_pct(row: Any) -> Optional[float]:
    price = _get(row, "current_price")
    short_mp = _get(row, "short_max_pain")
    long_mp = _get(row, "long_max_pain")
    if not price or short_mp is None or long_mp is None:
        return None
    return abs(short_mp - long_mp) / price * 100


def _money_rank_threshold(rows: List[Any], percentile: float = 0.80) -> float:
    """Return approximate high-liquidity threshold based on closest-side amount."""
    values = []
    for row in rows:
        side = _closest_side(row)
        if not side:
            continue
        amount = _amount_for_side(row, side)
        if amount is not None:
            values.append(amount)
    if not values:
        return 0.0
    values.sort()
    idx = int((len(values) - 1) * percentile)
    return values[idx]


def find_alerts(
    rows: List[Any],
    *,
    near_threshold_pct: float = 0.75,
    imbalance_ratio_threshold: float = 2.0,
    gap_threshold_pct: float = 20.0,
    high_setup_threshold: float = 75.0,
    high_liq_distance_pct: float = 1.0,
    limit: int = 30,
) -> List[Dict[str, Any]]:
    alerts: List[Dict[str, Any]] = []
    high_liq_threshold = _money_rank_threshold(rows, percentile=0.80)

    # Row-level alerts by coin/timeframe.
    for row in rows:
        symbol = str(_get(row, "symbol", "")).upper()
        timeframe = _get(row, "timeframe")
        side = _closest_side(row)
        distance = _closest_distance(row)
        if not symbol or not timeframe or not side or distance is None:
            continue

        near_amount = _amount_for_side(row, side) or 0.0
        far_amount = _other_amount(row, side) or 0.0
        ratio = (near_amount / far_amount) if far_amount else None
        gap = _gap_pct(row)

        # 1. Very close to Max Pain.
        if distance <= near_threshold_pct:
            alerts.append({
                "type": "NEAR_MAX_PAIN",
                "symbol": symbol,
                "timeframe": timeframe,
                "side": side,
                "severity": _severity_from_distance(distance),
                "distance_pct": distance,
                "gap_pct": gap,
                "amount": near_amount,
                "ratio": ratio,
                "reason": f"{symbol}/{timeframe} is {distance:.2f}% from {side} Max Pain",
            })

        # 2. Liquidity imbalance only if the bigger side is also the closer Max Pain side.
        if ratio is not None and ratio >= imbalance_ratio_threshold:
            alerts.append({
                "type": "LIQUIDITY_IMBALANCE_NEAR_SIDE",
                "symbol": symbol,
                "timeframe": timeframe,
                "side": side,
                "severity": _severity_from_ratio(ratio),
                "distance_pct": distance,
                "gap_pct": gap,
                "amount": near_amount,
                "ratio": ratio,
                "reason": f"{side} side is {ratio:.2f}x larger and is the closer Max Pain side",
            })

        # 3. Extreme gap between two Max Pain edges.
        if gap is not None and gap >= gap_threshold_pct:
            alerts.append({
                "type": "EXTREME_GAP",
                "symbol": symbol,
                "timeframe": timeframe,
                "side": side,
                "severity": _severity_from_gap(gap),
                "distance_pct": distance,
                "gap_pct": gap,
                "amount": near_amount,
                "ratio": ratio,
                "reason": f"Gap between Short/Long Max Pain is {gap:.2f}%",
            })

        # 4. Close + high liquidity on the close side.
        if distance <= high_liq_distance_pct and near_amount >= high_liq_threshold and high_liq_threshold > 0:
            alerts.append({
                "type": "HIGH_LIQUIDITY_CLOSE_DISTANCE",
                "symbol": symbol,
                "timeframe": timeframe,
                "side": side,
                "severity": "HIGH",
                "distance_pct": distance,
                "gap_pct": gap,
                "amount": near_amount,
                "ratio": ratio,
                "reason": f"Close to {side} Max Pain and liquidity is in top 20% of current snapshot",
            })

    # Symbol-level high setup strength alerts.
    scores = decision_engine.calculate_scores(rows, limit=500)
    for score in scores:
        if score.get("setup_strength", 0) >= high_setup_threshold:
            alerts.append({
                "type": "HIGH_SETUP_STRENGTH",
                "symbol": score["symbol"],
                "timeframe": "ALL",
                "side": score["direction"],
                "severity": "HIGH",
                "distance_pct": score.get("avg_distance"),
                "gap_pct": score.get("gap_avg_pct"),
                "amount": score.get("liquidity", {}).get("total"),
                "ratio": score.get("liquidity", {}).get("ratio"),
                "reason": f"Setup Strength {score['setup_strength']} ({score['confidence']})",
            })

    # Deduplicate similar alerts and sort strongest first.
    deduped = {}
    for a in alerts:
        key = (a["type"], a["symbol"], a["timeframe"], a["side"])
        existing = deduped.get(key)
        if not existing or _alert_sort_key(a) < _alert_sort_key(existing):
            deduped[key] = a

    result = list(deduped.values())
    result.sort(key=_alert_sort_key)
    return result[:limit]


def _severity_from_distance(distance: float) -> str:
    if distance <= 0.35:
        return "HIGH"
    if distance <= 0.75:
        return "MEDIUM"
    return "LOW"


def _severity_from_ratio(ratio: float) -> str:
    if ratio >= 3:
        return "HIGH"
    if ratio >= 2:
        return "MEDIUM"
    return "LOW"


def _severity_from_gap(gap: float) -> str:
    if gap >= 40:
        return "HIGH"
    if gap >= 20:
        return "MEDIUM"
    return "LOW"


def _severity_weight(severity: str) -> int:
    return {"HIGH": 0, "MEDIUM": 1, "LOW": 2}.get(severity, 9)


def _alert_sort_key(alert: Dict[str, Any]):
    distance = alert.get("distance_pct")
    distance_sort = distance if distance is not None else 999
    return (
        _severity_weight(alert.get("severity")),
        distance_sort,
        -float(alert.get("ratio") or 0),
        alert.get("symbol", ""),
        alert.get("timeframe", ""),
    )
