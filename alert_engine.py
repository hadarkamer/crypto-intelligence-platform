"""Alert Score v2.

Final agreed principles:
- Setup Strength is not part of Alert Priority.
- Data quality is not part of the score; it is displayed as a warning only.
- Multiple alerts for one coin do not add score.
- Consensus and BTC similarity share one Directional Alignment component.
- Target clustering is scored.
- Liquidity Density is intentionally excluded because it depends on repeated historical samples and is not reliable enough yet.
- HIGH_LIQUIDITY_CLOSE_DISTANCE is both an alert type and a 0..10 score component.
- Its liquidity ratio is adjusted by sqrt(timeframe hours) before comparison, so long timeframes do not dominate automatically.
- Liquidity Balance is a bonus/penalty from -10 to +10.
- Historical direction persistence is not scored.
"""

from __future__ import annotations

from collections import defaultdict
import math
from typing import Any, Dict, Iterable, List, Optional

import analysis


TIMEFRAMES = ["12h", "24h", "48h", "3d", "1w", "2w", "1m"]
TIMEFRAME_HOURS = {
    "12h": 12.0,
    "24h": 24.0,
    "48h": 48.0,
    "3d": 72.0,
    "1w": 168.0,
    "2w": 336.0,
    "1m": 720.0,
}
RAW_MAX_SCORE = 100.0


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
    return abs(float(value)) if value is not None else None


def _target_for_side(row: Any, side: str) -> Optional[float]:
    key = "short_max_pain" if side == "SHORT" else "long_max_pain"
    value = _get(row, key)
    return float(value) if value is not None else None


def _amount_for_side(row: Any, side: str) -> float:
    key = "short_liquidation_amount" if side == "SHORT" else "long_liquidation_amount"
    return float(_get(row, key) or 0.0)


def _opposite_amount(row: Any, side: str) -> float:
    key = "long_liquidation_amount" if side == "SHORT" else "short_liquidation_amount"
    return float(_get(row, key) or 0.0)


def _relative_gap_advantage(row: Any) -> Dict[str, Optional[float]]:
    """Relative advantage of the closer target over the farther target.

    Symmetric targets receive zero advantage.
    """
    price = _get(row, "current_price")
    short_mp = _get(row, "short_max_pain")
    long_mp = _get(row, "long_max_pain")
    if not price or short_mp is None or long_mp is None:
        return {"near_distance": None, "far_distance": None, "advantage": None, "points": 0.0}

    short_distance = abs(float(short_mp) - float(price)) / float(price) * 100.0
    long_distance = abs(float(long_mp) - float(price)) / float(price) * 100.0
    near_distance = min(short_distance, long_distance)
    far_distance = max(short_distance, long_distance)

    if far_distance <= 0:
        advantage = 0.0
    else:
        advantage = max(0.0, min(1.0, (far_distance - near_distance) / far_distance))

    return {
        "near_distance": near_distance,
        "far_distance": far_distance,
        "advantage": advantage,
        "points": round(advantage * 5.0, 2),
    }


def _allowed_distance_pct(symbol: str, rank: Optional[int]) -> float:
    """Dynamic Max Pain distance threshold."""
    symbol = symbol.upper()
    if symbol == "BTC":
        return 2.5
    if symbol == "ETH":
        return 2.7
    rank_value = int(rank or 999)
    if rank_value <= 10:
        return 3.0
    if rank_value <= 20:
        return 3.5
    return 4.0


def _proximity_base_points(
    distance_pct: Optional[float],
    allowed_distance_pct: float,
) -> float:
    """Continuous 0..35 base score."""
    if distance_pct is None or allowed_distance_pct <= 0:
        return 0.0
    normalized = 1.0 - float(distance_pct) / allowed_distance_pct
    return round(max(0.0, min(1.0, normalized)) * 35.0, 2)


def _proximity_points(distance_pct: Optional[float]) -> float:
    """0..30. Full at <=0.25%, zero at >=2.00%."""
    if distance_pct is None:
        return 0.0
    if distance_pct <= 0.25:
        return 30.0
    if distance_pct >= 2.0:
        return 0.0
    return round((2.0 - distance_pct) / 1.75 * 30.0, 2)


def _consensus_map(rows: List[Any]) -> Dict[str, Dict[str, Any]]:
    results = analysis.calculate_consensus(rows, min_hits=1, limit=1000)
    return {item["symbol"]: item for item in results}


def _btc_similarity_map(rows: List[Any]) -> Dict[str, Dict[str, Any]]:
    results = analysis.calculate_btc_similarity(rows, min_hits=0, limit=1000)
    mapping = {item["symbol"]: item for item in results}

    btc_rows = [
        row for row in rows
        if str(_get(row, "symbol", "")).upper() == "BTC"
        and _closest_side(row)
    ]
    if btc_rows:
        mapping["BTC"] = {
            "symbol": "BTC",
            "hits": len(btc_rows),
            "total": len(btc_rows),
            "same_tfs": ",".join(str(_get(row, "timeframe")) for row in btc_rows),
            "different_tfs": "-",
        }
    return mapping


def _directional_alignment(
    symbol: str,
    consensus_hits: int,
    consensus_total: int,
    btc_hits: int,
    btc_total: int,
    market_support_pct: Optional[float],
) -> Dict[str, float]:
    """Continuous Directional Alignment, 0..35.

    Regular coins:
    - Consensus: 0..20
    - BTC Like: 0..8
    - Market: 0..7

    BTC:
    - Consensus: 0..27
    - Market: 0..8
    - BTC Like is excluded.
    """
    is_btc = symbol.upper() == "BTC"
    consensus_max = 27.0 if is_btc else 20.0
    btc_like_max = 0.0 if is_btc else 8.0
    market_max = 8.0 if is_btc else 7.0

    consensus_points = (
        round(consensus_hits / consensus_total * consensus_max, 2)
        if consensus_total else 0.0
    )
    btc_points = (
        round(btc_hits / btc_total * btc_like_max, 2)
        if (not is_btc and btc_total) else 0.0
    )

    # Continuous market score:
    # <=50% support = 0; 100% support = full score.
    market_points = 0.0
    if market_support_pct is not None:
        market_points = round(
            max(
                0.0,
                min(
                    market_max,
                    (float(market_support_pct) - 50.0) / 50.0 * market_max,
                ),
            ),
            2,
        )

    return {
        "consensus_points": consensus_points,
        "btc_like_points": btc_points,
        "market_points": market_points,
        "consensus_max": consensus_max,
        "btc_like_max": btc_like_max,
        "market_max": market_max,
        "total": round(consensus_points + btc_points + market_points, 2),
    }


def _market_bias_map(rows: List[Any]) -> Dict[str, Any]:
    """Aggregate market schema from all valid asset/timeframe rows."""
    market = analysis.calculate_market_bias(rows)
    overall = market.get("overall", {})

    return {
        "long_count": int(overall.get("long_count", 0) or 0),
        "short_count": int(overall.get("short_count", 0) or 0),
        "total": int(
            (overall.get("long_count", 0) or 0)
            + (overall.get("short_count", 0) or 0)
        ),
        "long_pct": overall.get("long_pct"),
        "short_pct": overall.get("short_pct"),
        "bias": overall.get("bias", "NEUTRAL"),
    }


def _cluster_map(
    rows: List[Any],
    consensus: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Target Clustering, 0..25.

    Components:
    - target spread quality: 0..10
    - timeframe coverage: 0..7
    - real liquidity growth: 0..8

    Liquidity growth prevents repeated cumulative liquidity from receiving
    full extra credit merely because it appears in several timeframes.
    """
    grouped: Dict[str, List[Any]] = defaultdict(list)
    for row in rows:
        symbol = str(_get(row, "symbol", "")).upper()
        if symbol:
            grouped[symbol].append(row)

    results: Dict[str, Dict[str, Any]] = {}
    for symbol, symbol_rows in grouped.items():
        dominant = consensus.get(symbol, {}).get("side")
        entries = []

        if dominant in {"LONG", "SHORT"}:
            for row in symbol_rows:
                if _closest_side(row) != dominant:
                    continue
                target = _target_for_side(row, dominant)
                if target is None or target <= 0:
                    continue
                timeframe = str(_get(row, "timeframe", ""))
                entries.append({
                    "timeframe": timeframe,
                    "hours": TIMEFRAME_HOURS.get(timeframe, 10**9),
                    "target": float(target),
                    "amount": _amount_for_side(row, dominant),
                })

        entries.sort(key=lambda item: item["hours"])
        count = len(entries)

        if count < 3:
            results[symbol] = {
                "side": dominant,
                "count": count,
                "spread_pct": None,
                "spread_points": 0.0,
                "coverage_points": round(count / 7.0 * 7.0, 2),
                "growth_points": 0.0,
                "total_growth_pct": None,
                "positive_step_ratio": None,
                "points": 0.0,
            }
            continue

        targets = [item["target"] for item in entries]
        avg_target = sum(targets) / len(targets)
        spread_pct = (
            (max(targets) - min(targets)) / avg_target * 100.0
            if avg_target else None
        )

        # Continuous: 0% spread = 10; 3% or more = 0.
        spread_points = (
            round(max(0.0, 1.0 - float(spread_pct) / 3.0) * 10.0, 2)
            if spread_pct is not None else 0.0
        )
        coverage_points = round(min(1.0, count / 7.0) * 7.0, 2)

        amounts = [max(0.0, float(item["amount"])) for item in entries]
        first_amount = amounts[0] if amounts else 0.0
        last_amount = amounts[-1] if amounts else 0.0
        total_growth_ratio = (
            max(0.0, last_amount - first_amount) / first_amount
            if first_amount > 0 else 0.0
        )

        positive_steps = 0
        total_steps = max(0, len(amounts) - 1)
        for previous, current in zip(amounts, amounts[1:]):
            # Require at least 2% growth to count as a real positive step.
            if previous > 0 and current >= previous * 1.02:
                positive_steps += 1

        positive_step_ratio = (
            positive_steps / total_steps if total_steps else 0.0
        )
        growth_points = round(
            min(1.0, total_growth_ratio) * 4.0
            + positive_step_ratio * 4.0,
            2,
        )

        results[symbol] = {
            "side": dominant,
            "count": count,
            "spread_pct": spread_pct,
            "spread_points": spread_points,
            "coverage_points": coverage_points,
            "growth_points": growth_points,
            "total_growth_pct": total_growth_ratio * 100.0,
            "positive_step_ratio": positive_step_ratio,
            "points": round(spread_points + coverage_points + growth_points, 2),
        }

    return results


def _liquidity_balance(near_amount: float, far_amount: float) -> Dict[str, Any]:
    total = near_amount + far_amount
    if total <= 0:
        return {"near_share_pct": None, "near_far_ratio": None, "balance": None, "points": 0.0}
    balance = (near_amount - far_amount) / total
    near_share = near_amount / total * 100.0
    ratio = near_amount / far_amount if far_amount > 0 else None
    points = max(-10.0, min(20.0, balance * 30.0))
    return {
        "near_share_pct": near_share,
        "near_far_ratio": ratio,
        "balance": balance,
        "points": round(points, 2),
    }


def _incremental_adjusted_liquidity_map(
    rows: List[Any],
) -> Dict[str, Dict[str, Any]]:
    """Incremental, timeframe-adjusted liquidity by symbol and side.

    Long timeframes contain liquidity already visible in shorter timeframes.
    Therefore we score only the positive *new increment* between consecutive
    timeframes and normalize it by sqrt(delta hours).

    This intentionally removes the automatic advantage of long timeframes.
    """
    grouped: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)

    for row in rows:
        symbol = str(_get(row, "symbol", "")).upper()
        timeframe = str(_get(row, "timeframe", ""))
        hours = TIMEFRAME_HOURS.get(timeframe)
        if not symbol or not hours:
            continue

        for side in ("LONG", "SHORT"):
            grouped[(symbol, side)].append({
                "timeframe": timeframe,
                "hours": hours,
                "amount": _amount_for_side(row, side),
            })

    result: Dict[str, Dict[str, Any]] = defaultdict(dict)

    for (symbol, side), entries in grouped.items():
        entries.sort(key=lambda item: item["hours"])
        previous_amount = 0.0
        previous_hours = 0.0
        adjusted_items = []

        for entry in entries:
            amount = max(0.0, float(entry["amount"]))
            increment = max(0.0, amount - previous_amount)
            delta_hours = max(1.0, float(entry["hours"]) - previous_hours)
            adjusted_increment = increment / math.sqrt(delta_hours)

            adjusted_items.append({
                **entry,
                "incremental_liquidity": increment,
                "adjusted_incremental_liquidity": adjusted_increment,
            })
            previous_amount = amount
            previous_hours = float(entry["hours"])

        positive_values = [
            item["adjusted_incremental_liquidity"]
            for item in adjusted_items
            if item["adjusted_incremental_liquidity"] > 0
        ]
        baseline = (
            sum(positive_values) / len(positive_values)
            if positive_values else None
        )

        result[symbol][side] = {
            "baseline": baseline,
            "items": {
                item["timeframe"]: item for item in adjusted_items
            },
        }

    return dict(result)


def _adjusted_multiplier(ratio: Optional[float]) -> float:
    """Moderate 0.90..1.10 multiplier, not a separate score."""
    if ratio is None:
        return 1.0
    ratio = max(0.0, float(ratio))
    if ratio <= 1.0:
        return round(0.90 + min(1.0, ratio) * 0.10, 4)
    return round(min(1.10, 1.0 + min(1.0, ratio - 1.0) * 0.10), 4)


def _balance_multiplier(near_share_pct: Optional[float]) -> float:
    """Small continuous 0.95..1.05 modifier."""
    if near_share_pct is None:
        return 1.0
    share = float(near_share_pct)
    if share <= 40.0:
        return 0.95
    if share >= 60.0:
        return 1.05
    return round(0.95 + (share - 40.0) / 20.0 * 0.10, 4)


def _high_liquidity_close_points(
    distance_pct: Optional[float],
    adjusted_ratio: Optional[float],
) -> float:
    """0..30 points, only when distance <= 1%."""
    if distance_pct is None or distance_pct > 1.0 or adjusted_ratio is None:
        return 0.0
    if adjusted_ratio >= 2.50:
        return 30.0
    if adjusted_ratio >= 2.00:
        return 24.0
    if adjusted_ratio >= 1.60:
        return 18.0
    if adjusted_ratio >= 1.30:
        return 12.0
    if adjusted_ratio >= 1.10:
        return 6.0
    return 0.0


def build_opportunities(rows: List[Any], limit: int = 30) -> List[Dict[str, Any]]:
    """Score every valid coin/timeframe; alerts are chosen by the caller."""
    consensus = _consensus_map(rows)
    btc_like = _btc_similarity_map(rows)
    market = _market_bias_map(rows)
    clusters = _cluster_map(rows, consensus)
    incremental_adjusted = _incremental_adjusted_liquidity_map(rows)
    out: List[Dict[str, Any]] = []

    for row in rows:
        symbol = str(_get(row, "symbol", "")).upper()
        timeframe = str(_get(row, "timeframe", ""))
        side = _closest_side(row)
        distance = _closest_distance(row)
        rank = _get(row, "rank")

        if not symbol or not timeframe or not side or distance is None:
            continue

        near_amount = _amount_for_side(row, side)
        far_amount = _opposite_amount(row, side)
        balance = _liquidity_balance(near_amount, far_amount)

        allowed_distance = _allowed_distance_pct(symbol, rank)
        proximity_base = _proximity_base_points(distance, allowed_distance)

        side_adjusted = incremental_adjusted.get(symbol, {}).get(side, {})
        adjusted_item = side_adjusted.get("items", {}).get(timeframe, {})
        adjusted_value = adjusted_item.get("adjusted_incremental_liquidity")
        incremental_value = adjusted_item.get("incremental_liquidity")
        baseline = side_adjusted.get("baseline")
        adjusted_ratio = (
            adjusted_value / baseline
            if adjusted_value is not None and baseline and baseline > 0
            else None
        )
        adjusted_multiplier = _adjusted_multiplier(adjusted_ratio)
        balance_multiplier = _balance_multiplier(balance["near_share_pct"])

        target_attraction = round(
            min(
                35.0,
                proximity_base * adjusted_multiplier * balance_multiplier,
            ),
            2,
        )

        cons = consensus.get(symbol, {})
        consensus_hits = int(cons.get("hits", 0) or 0)
        consensus_total = int(cons.get("total", 0) or 0)

        btc = btc_like.get(symbol, {})
        btc_hits = int(btc.get("hits", 0) or 0)
        btc_total = int(btc.get("total", 0) or 0)

        market_support_pct = (
            market.get("short_pct") if side == "SHORT" else market.get("long_pct")
        )
        market_support_count = (
            market.get("short_count") if side == "SHORT" else market.get("long_count")
        )

        directional = _directional_alignment(
            symbol,
            consensus_hits,
            consensus_total,
            btc_hits,
            btc_total,
            market_support_pct,
        )

        cluster = clusters.get(symbol, {
            "count": 0,
            "spread_pct": None,
            "points": 0.0,
            "side": None,
        })
        cluster_points = (
            float(cluster.get("points", 0.0))
            if cluster.get("side") == side else 0.0
        )

        gap = _relative_gap_advantage(row)
        gap_points = float(gap.get("points", 0.0))

        components = {
            "directional_alignment": directional["total"],
            "consensus": directional["consensus_points"],
            "btc_like": directional["btc_like_points"],
            "market": directional["market_points"],
            "consensus_max": directional["consensus_max"],
            "btc_like_max": directional["btc_like_max"],
            "market_max": directional["market_max"],
            "target_attraction": target_attraction,
            "proximity_base": proximity_base,
            "target_clustering": cluster_points,
            "relative_gap": gap_points,
        }

        score = round(
            components["directional_alignment"]
            + components["target_attraction"]
            + components["target_clustering"]
            + components["relative_gap"],
            2,
        )
        score = round(max(0.0, min(100.0, score)), 2)

        types: List[str] = []
        if distance <= allowed_distance:
            types.append("NEAR_MAX_PAIN")
        if adjusted_ratio is not None and adjusted_ratio >= 1.30 and distance <= allowed_distance:
            types.append("HIGH_LIQUIDITY_CLOSE_DISTANCE")
        if cluster_points >= 15.0:
            types.append("TARGET_CLUSTER")
        if gap.get("advantage") is not None and float(gap["advantage"]) >= 0.40:
            types.append("RELATIVE_GAP_ADVANTAGE")
        if balance["near_share_pct"] is not None and float(balance["near_share_pct"]) >= 60.0:
            types.append("LIQUIDITY_BALANCE_SUPPORT")

        current_price = _get(row, "current_price")
        target_price = _target_for_side(row, side)
        target_direction = None
        if current_price is not None and target_price is not None:
            target_direction = (
                "UP" if float(target_price) > float(current_price) else "DOWN"
            )

        out.append({
            "symbol": symbol,
            "timeframe": timeframe,
            "rank": rank,
            "side": side,
            "current_price": current_price,
            "target_price": target_price,
            "target_direction": target_direction,
            "types": types,
            "priority": score,
            "score": score,
            "raw_score": score,
            "raw_max_score": 100.0,
            "distance_pct": distance,
            "allowed_distance_pct": allowed_distance,
            "near_amount": near_amount,
            "far_amount": far_amount,
            "near_share_pct": balance["near_share_pct"],
            "near_far_ratio": balance["near_far_ratio"],
            "liquidity_balance": balance["balance"],
            "incremental_liquidity": incremental_value,
            "adjusted_incremental_liquidity": adjusted_value,
            "adjusted_near_liquidity_ratio": adjusted_ratio,
            "adjusted_multiplier": adjusted_multiplier,
            "balance_multiplier": balance_multiplier,
            "consensus_hits": consensus_hits,
            "consensus_total": consensus_total,
            "btc_like_hits": btc_hits,
            "btc_like_total": btc_total,
            "market_support_pct": market_support_pct,
            "market_support_count": market_support_count,
            "market_total_count": market.get("total", 0),
            "cluster_count": cluster.get("count", 0),
            "cluster_spread_pct": cluster.get("spread_pct"),
            "cluster_growth_pct": cluster.get("total_growth_pct"),
            "cluster_positive_step_ratio": cluster.get("positive_step_ratio"),
            "cluster_side": cluster.get("side"),
            "relative_gap_advantage": gap.get("advantage"),
            "near_distance_pct": gap.get("near_distance"),
            "far_distance_pct": gap.get("far_distance"),
            "components": components,
        })

    # Average Score across all available timeframes is a secondary
    # prioritization signal only. The current timeframe Score remains primary.
    scores_by_symbol: Dict[str, List[float]] = defaultdict(list)
    for item in out:
        scores_by_symbol[item["symbol"]].append(float(item["score"]))

    averages = {
        symbol: sum(values) / len(values)
        for symbol, values in scores_by_symbol.items()
        if values
    }
    for item in out:
        item["average_score_all_timeframes"] = round(
            averages.get(item["symbol"], float(item["score"])),
            2,
        )

    out.sort(
        key=lambda x: (
            -float(x["score"]),
            -float(x.get("average_score_all_timeframes", 0)),
            float(x["distance_pct"]),
            x["symbol"],
            TIMEFRAMES.index(x["timeframe"])
            if x["timeframe"] in TIMEFRAMES
            else 99,
        )
    )
    return out[:limit]
