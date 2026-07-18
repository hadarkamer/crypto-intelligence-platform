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

    short_signed = (float(short_mp) - float(price)) / float(price) * 100.0
    long_signed = (float(long_mp) - float(price)) / float(price) * 100.0
    active_distances = []
    if short_signed > 0:
        active_distances.append(abs(short_signed))
    if long_signed < 0:
        active_distances.append(abs(long_signed))

    # Relative-gap advantage requires two still-active opposing targets.
    # A target already crossed by the live Binance price is excluded.
    if len(active_distances) < 2:
        return {"near_distance": None, "far_distance": None, "advantage": None, "points": 0.0}

    near_distance = min(active_distances)
    far_distance = max(active_distances)

    if far_distance <= 0:
        advantage = 0.0
    else:
        advantage = max(0.0, min(1.0, (far_distance - near_distance) / far_distance))

    return {
        "near_distance": near_distance,
        "far_distance": far_distance,
        "advantage": advantage,
        "points": round(advantage * 15.0, 2),
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


def _target_proximity_points(
    distance_pct: Optional[float],
    allowed_distance_pct: float,
) -> float:
    """Tradable target-distance score, 0..25.

    The dynamic threshold remains available for display and eligibility, while
    the score follows the agreed simple tradability bands.
    """
    if distance_pct is None or allowed_distance_pct <= 0:
        return 0.0

    distance = float(distance_pct)
    if distance < 0.5 or distance > float(allowed_distance_pct):
        return 0.0
    if distance < 0.7:
        return 17.0
    if distance <= 1.3:
        return 25.0
    if distance <= 2.0:
        return 20.0
    return 15.0


def _proximity_points(distance_pct: Optional[float]) -> float:
    """0..30. Full at <=0.25%, zero at >=2.00%."""
    if distance_pct is None:
        return 0.0
    if distance_pct <= 0.25:
        return 30.0
    if distance_pct >= 2.0:
        return 0.0
    return round((2.0 - distance_pct) / 1.75 * 30.0, 2)


def _dedupe_rows(rows: List[Any]) -> tuple[List[Any], Dict[str, int]]:
    """Keep one row per symbol/timeframe and count removed duplicates."""
    unique: Dict[tuple[str, str], Any] = {}
    duplicate_counts: Dict[str, int] = defaultdict(int)
    for row in rows:
        symbol = str(_get(row, "symbol", "") or "").upper()
        timeframe = str(_get(row, "timeframe", "") or "")
        if not symbol or timeframe not in TIMEFRAMES:
            continue
        key = (symbol, timeframe)
        if key in unique:
            duplicate_counts[symbol] += 1
        unique[key] = row
    ordered = sorted(
        unique.values(),
        key=lambda row: (
            str(_get(row, "symbol", "") or "").upper(),
            TIMEFRAMES.index(str(_get(row, "timeframe", ""))),
        ),
    )
    return ordered, dict(duplicate_counts)


def _consensus_map(rows: List[Any]) -> Dict[str, Dict[str, Any]]:
    """Return side-specific consensus counts for every symbol.

    Each alert must be scored using the number of timeframes supporting that
    alert's own direction, not the dominant direction's hit count.
    """
    grouped: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"LONG": 0, "SHORT": 0, "total": 0, "by_timeframe": {}}
    )
    for row in rows:
        symbol = str(_get(row, "symbol", "") or "").upper()
        timeframe = str(_get(row, "timeframe", "") or "")
        side = _closest_side(row)
        if not symbol or timeframe not in TIMEFRAMES or side not in {"LONG", "SHORT"}:
            continue
        grouped[symbol][side] += 1
        grouped[symbol]["total"] += 1
        grouped[symbol]["by_timeframe"][timeframe] = side

    result: Dict[str, Dict[str, Any]] = {}
    for symbol, data in grouped.items():
        long_count = int(data["LONG"])
        short_count = int(data["SHORT"])
        dominant = "SHORT" if short_count >= long_count else "LONG"
        result[symbol] = {
            "side": dominant,
            "hits": max(long_count, short_count),
            "total": int(data["total"]),
            "LONG": long_count,
            "SHORT": short_count,
            "by_timeframe": dict(data["by_timeframe"]),
        }
    return result


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
    btc_reference: Optional[Dict[str, Any]],
    alert_side: str,
) -> Dict[str, float]:
    """Directional Alignment agreed with Yoni, continuous and capped 0..30.

    BTC: own consensus only, scaled to 30.
    Altcoins: own consensus 0..15, plus continuous BTC confirmation 0..15
    when BTC points the same way, or a continuous penalty up to 10 when BTC
    points the opposite way. Market breadth is informational only.
    """
    is_btc = symbol.upper() == "BTC"
    consensus_max = 30.0 if is_btc else 15.0
    consensus_points = (
        round(consensus_hits / consensus_total * consensus_max, 2)
        if consensus_total else 0.0
    )
    btc_score = float((btc_reference or {}).get("score", 0.0) or 0.0)
    btc_side = (btc_reference or {}).get("side")
    btc_confirmation = 0.0
    btc_penalty = 0.0
    if not is_btc and btc_side in {"LONG", "SHORT"}:
        if btc_side == alert_side:
            btc_confirmation = round(min(15.0, max(0.0, btc_score * 0.15)), 2)
        else:
            btc_penalty = round(min(10.0, max(0.0, btc_score * 0.10)), 2)
    total = max(0.0, min(30.0, consensus_points + btc_confirmation - btc_penalty))
    return {
        "consensus_points": consensus_points,
        "btc_confirmation_points": btc_confirmation,
        "btc_conflict_penalty": btc_penalty,
        "btc_reference_score": round(btc_score, 2),
        "consensus_max": consensus_max,
        "btc_confirmation_max": 15.0 if not is_btc else 0.0,
        "btc_penalty_max": 10.0 if not is_btc else 0.0,
        "total": round(total, 2),
    }


def _btc_reference_map(
    rows: List[Any],
    consensus: Dict[str, Dict[str, Any]],
    clusters: Dict[str, Dict[str, Dict[str, Any]]],
) -> Dict[str, Dict[str, Any]]:
    """Build BTC's non-circular score for each timeframe."""
    result: Dict[str, Dict[str, Any]] = {}
    cons = consensus.get("BTC", {})
    total = int(cons.get("total", 0) or 0)
    for row in rows:
        if str(_get(row, "symbol", "")).upper() != "BTC":
            continue
        tf = str(_get(row, "timeframe", ""))
        side = _closest_side(row)
        distance = _closest_distance(row)
        if tf not in TIMEFRAMES or side not in {"LONG", "SHORT"} or distance is None:
            continue
        hits = int(cons.get(side, 0) or 0)
        directional = round(hits / total * 30.0, 2) if total else 0.0
        proximity = _target_proximity_points(distance, _allowed_distance_pct("BTC", _get(row, "rank")))
        cluster = clusters.get("BTC", {}).get(side, {})
        gap = _relative_gap_advantage(row)
        score = round(min(100.0, max(0.0, directional + proximity + float(cluster.get("points", 0) or 0) + float(gap.get("points", 0) or 0))), 2)
        result[tf] = {"side": side, "score": score}
    return result

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


CLUSTER_MEMBER_MAX_DISTANCE_PCT = 1.5

LIQUIDITY_GROWTH_THRESHOLDS = {
    ("12h", "24h"): 0.15,
    ("24h", "48h"): 0.20,
    ("48h", "3d"): 0.15,
    ("3d", "1w"): 0.25,
    ("1w", "2w"): 0.25,
    ("2w", "1m"): 0.30,
}

CLUSTER_COVERAGE_POINTS = {
    3: 2.0,
    4: 4.0,
    5: 6.0,
    6: 7.0,
    7: 8.0,
}


def _median(values: List[float]) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def _transition_growth_score(
    previous_amount: float,
    current_amount: float,
    threshold: float,
) -> float:
    """Score one liquidity transition from 0 to 1.

    Below threshold: 0.
    At threshold: 0.5.
    At double threshold or more: 1.
    Between threshold and double threshold: continuous.
    """
    if previous_amount <= 0 or threshold <= 0:
        return 0.0

    growth = (current_amount - previous_amount) / previous_amount
    if growth < threshold:
        return 0.0
    if growth >= threshold * 2.0:
        return 1.0

    return round(
        0.5 + (growth - threshold) / threshold * 0.5,
        4,
    )


def _cluster_for_side(symbol_rows: List[Any], side: str) -> Dict[str, Any]:
    """Calculate one independent cluster for one direction."""
    directional_entries: List[Dict[str, Any]] = []
    for row in symbol_rows:
        if _closest_side(row) != side:
            continue
        target = _target_for_side(row, side)
        timeframe = str(_get(row, "timeframe", ""))
        if target is None or target <= 0 or timeframe not in TIMEFRAME_HOURS:
            continue
        directional_entries.append({
            "timeframe": timeframe,
            "hours": TIMEFRAME_HOURS[timeframe],
            "target": float(target),
            "amount": max(0.0, _amount_for_side(row, side)),
        })

    directional_entries = list({
        item["timeframe"]: item for item in directional_entries
    }.values())
    directional_entries.sort(key=lambda item: item["hours"])
    same_direction_count = len(directional_entries)
    empty = {
        "side": side,
        "same_direction_count": same_direction_count,
        "count": 0,
        "members": [],
        "median_target": None,
        "mean_deviation_pct": None,
        "spread_pct": None,
        "density_points": 0.0,
        "coverage_points": 0.0,
        "growth_points": 0.0,
        "liquidity_multiplier": 0.0,
        "growth_transition_scores": {},
        "points": 0.0,
    }
    if same_direction_count < 3:
        return empty

    median_target = _median([item["target"] for item in directional_entries])
    if median_target is None or median_target <= 0:
        return empty

    cluster_entries = []
    for item in directional_entries:
        deviation = abs(item["target"] - median_target) / median_target * 100.0
        if deviation <= CLUSTER_MEMBER_MAX_DISTANCE_PCT:
            cluster_entries.append({**item, "distance_from_median_pct": deviation})
    cluster_count = len(cluster_entries)
    if cluster_count < 3:
        return {**empty, "median_target": median_target, "count": cluster_count,
                "members": [x["timeframe"] for x in cluster_entries]}

    deviations = [x["distance_from_median_pct"] for x in cluster_entries]
    mean_deviation_pct = sum(deviations) / len(deviations)
    targets = [x["target"] for x in cluster_entries]
    average_target = sum(targets) / len(targets)
    spread_pct = ((max(targets)-min(targets))/average_target*100.0) if average_target else None
    density_points = round(max(0.0, 1.0 - mean_deviation_pct / CLUSTER_MEMBER_MAX_DISTANCE_PCT) * 12.0, 2)
    coverage_points = CLUSTER_COVERAGE_POINTS.get(min(cluster_count, 7), 0.0)

    entries_by_tf = {x["timeframe"]: x for x in cluster_entries}
    transition_scores: Dict[str, float] = {}
    for (previous_tf, current_tf), threshold in LIQUIDITY_GROWTH_THRESHOLDS.items():
        previous = entries_by_tf.get(previous_tf)
        current = entries_by_tf.get(current_tf)
        if previous is None or current is None:
            continue
        transition_scores[f"{previous_tf}->{current_tf}"] = _transition_growth_score(
            previous["amount"], current["amount"], threshold
        )
    growth_score = round(sum(transition_scores.values()) / len(transition_scores) * 10.0, 2) if transition_scores else 0.0
    liquidity_multiplier = round(growth_score / 10.0 * 1.5, 4)
    cluster_points = round(min(30.0, (density_points + coverage_points) * liquidity_multiplier), 2)
    return {
        "side": side,
        "same_direction_count": same_direction_count,
        "count": cluster_count,
        "members": [x["timeframe"] for x in cluster_entries],
        "median_target": median_target,
        "mean_deviation_pct": mean_deviation_pct,
        "spread_pct": spread_pct,
        "density_points": density_points,
        "coverage_points": coverage_points,
        "growth_points": growth_score,
        "liquidity_multiplier": liquidity_multiplier,
        "growth_transition_scores": transition_scores,
        "points": cluster_points,
    }


def _cluster_map(rows: List[Any]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Calculate separate LONG and SHORT clusters for each symbol."""
    grouped: Dict[str, List[Any]] = defaultdict(list)
    for row in rows:
        symbol = str(_get(row, "symbol", "") or "").upper()
        if symbol:
            grouped[symbol].append(row)
    return {
        symbol: {
            "LONG": _cluster_for_side(symbol_rows, "LONG"),
            "SHORT": _cluster_for_side(symbol_rows, "SHORT"),
        }
        for symbol, symbol_rows in grouped.items()
    }


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


def build_opportunities(
    rows: List[Any],
    limit: int = 30,
) -> List[Dict[str, Any]]:
    """Score every valid coin/timeframe with the agreed 100-point model."""
    rows, duplicate_counts = _dedupe_rows(rows)
    consensus = _consensus_map(rows)
    market = _market_bias_map(rows)  # informational only
    clusters = _cluster_map(rows)
    btc_references = _btc_reference_map(rows, consensus, clusters)
    out: List[Dict[str, Any]] = []

    for row in rows:
        symbol = str(_get(row, "symbol", "")).upper()
        timeframe = str(_get(row, "timeframe", ""))
        side = _closest_side(row)
        distance = _closest_distance(row)
        rank = _get(row, "rank")

        if (
            not symbol
            or not timeframe
            or not side
            or distance is None
        ):
            continue

        near_amount = _amount_for_side(row, side)
        far_amount = _opposite_amount(row, side)
        balance = _liquidity_balance(near_amount, far_amount)

        allowed_distance = _allowed_distance_pct(symbol, rank)
        target_proximity = _target_proximity_points(
            distance,
            allowed_distance,
        )

        cons = consensus.get(symbol, {})
        consensus_hits = int(cons.get(side, 0) or 0)
        consensus_total = int(cons.get("total", 0) or 0)

        btc_reference = btc_references.get(timeframe)

        market_support_pct = (
            market.get("short_pct")
            if side == "SHORT"
            else market.get("long_pct")
        )
        market_support_count = (
            market.get("short_count")
            if side == "SHORT"
            else market.get("long_count")
        )

        directional = _directional_alignment(
            symbol,
            consensus_hits,
            consensus_total,
            btc_reference,
            side,
        )

        cluster = clusters.get(symbol, {}).get(side, {
            "count": 0, "same_direction_count": 0, "members": [],
            "spread_pct": None, "mean_deviation_pct": None,
            "median_target": None, "density_points": 0.0,
            "coverage_points": 0.0, "growth_points": 0.0,
            "liquidity_multiplier": 0.0,
            "growth_transition_scores": {}, "points": 0.0, "side": side,
        })
        cluster_points = float(cluster.get("points", 0.0) or 0.0)

        gap = _relative_gap_advantage(row)
        gap_points = float(gap.get("points", 0.0))

        components = {
            "directional_alignment": directional["total"],
            "consensus": directional["consensus_points"],
            "btc_confirmation": directional["btc_confirmation_points"],
            "btc_conflict_penalty": directional["btc_conflict_penalty"],
            "btc_reference_score": directional["btc_reference_score"],
            "consensus_max": directional["consensus_max"],
            "btc_confirmation_max": directional["btc_confirmation_max"],
            "btc_penalty_max": directional["btc_penalty_max"],
            "target_proximity": target_proximity,
            "cluster_confidence": cluster_points,
            # Compatibility key for older formatting code.
            "target_clustering": cluster_points,
            "cluster_density": float(
                cluster.get("density_points", 0.0)
            ),
            "cluster_coverage": float(
                cluster.get("coverage_points", 0.0)
            ),
            "cluster_liquidity_growth": float(
                cluster.get("growth_points", 0.0)
            ),
            "cluster_liquidity_multiplier": float(
                cluster.get("liquidity_multiplier", 0.0)
            ),
            "relative_gap": gap_points,
        }

        score = round(
            components["directional_alignment"]
            + components["target_proximity"]
            + components["cluster_confidence"]
            + components["relative_gap"],
            2,
        )
        score = round(max(0.0, min(100.0, score)), 2)

        types: List[str] = []
        if distance <= allowed_distance:
            types.append("NEAR_MAX_PAIN")
        if cluster_points >= 18.0:
            types.append("TARGET_CLUSTER")
        if (
            gap.get("advantage") is not None
            and float(gap["advantage"]) >= 0.40
        ):
            types.append("RELATIVE_GAP_ADVANTAGE")
        if (
            balance["near_share_pct"] is not None
            and float(balance["near_share_pct"]) >= 60.0
        ):
            types.append("LIQUIDITY_BALANCE_SUPPORT")

        current_price = _get(row, "current_price")
        target_price = _target_for_side(row, side)
        target_direction = None
        if current_price is not None and target_price is not None:
            target_direction = (
                "UP"
                if float(target_price) > float(current_price)
                else "DOWN"
            )

        validation_errors: List[str] = []
        component_sum = round(
            components["directional_alignment"]
            + components["target_proximity"]
            + components["cluster_confidence"]
            + components["relative_gap"], 2
        )
        if abs(component_sum - score) > 0.01:
            validation_errors.append(
                f"Score mismatch: components={component_sum:.2f}, score={score:.2f}"
            )
        if consensus_hits > consensus_total:
            validation_errors.append(
                f"Consensus invalid: {consensus_hits}/{consensus_total}"
            )
        if int(cluster.get("count", 0) or 0) > consensus_hits:
            validation_errors.append(
                "Cluster count exceeds timeframes supporting alert direction"
            )
        duplicate_rows_removed = int(duplicate_counts.get(symbol, 0) or 0)

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
            "distance_trade_band": (
                "BORDERLINE" if distance < 0.7
                else "PREFERRED" if distance <= 1.3
                else "FARTHER"
            ),
            "allowed_distance_pct": allowed_distance,
            "near_amount": near_amount,
            "far_amount": far_amount,
            "near_share_pct": balance["near_share_pct"],
            "near_far_ratio": balance["near_far_ratio"],
            "liquidity_balance": balance["balance"],
            "consensus_hits": consensus_hits,
            "consensus_total": consensus_total,
            "btc_reference_side": (btc_reference or {}).get("side"),
            "btc_reference_score": (btc_reference or {}).get("score"),
            "market_support_pct": market_support_pct,
            "market_support_count": market_support_count,
            "market_total_count": market.get("total", 0),
            "cluster_count": cluster.get("count", 0),
            "cluster_same_direction_count":
                cluster.get("same_direction_count", 0),
            "cluster_median_target":
                cluster.get("median_target"),
            "cluster_mean_deviation_pct":
                cluster.get("mean_deviation_pct"),
            "cluster_spread_pct":
                cluster.get("spread_pct"),
            "cluster_density_points":
                cluster.get("density_points", 0.0),
            "cluster_coverage_points":
                cluster.get("coverage_points", 0.0),
            "cluster_growth_points":
                cluster.get("growth_points", 0.0),
            "cluster_liquidity_multiplier":
                cluster.get("liquidity_multiplier", 0.0),
            "cluster_growth_transition_scores":
                cluster.get("growth_transition_scores", {}),
            "cluster_side": cluster.get("side"),
            "cluster_members": cluster.get("members", []),
            "duplicate_rows_removed": duplicate_rows_removed,
            "calculation_validation_errors": validation_errors,
            "component_sum_check": component_sum,
            "relative_gap_advantage": gap.get("advantage"),
            "near_distance_pct": gap.get("near_distance"),
            "far_distance_pct": gap.get("far_distance"),
            "components": components,
        })

    # Current timeframe Score is primary. All-timeframe average is secondary.
    scores_by_symbol: Dict[str, List[float]] = defaultdict(list)
    for item in out:
        scores_by_symbol[item["symbol"]].append(
            float(item["score"])
        )

    averages = {
        symbol: sum(values) / len(values)
        for symbol, values in scores_by_symbol.items()
        if values
    }

    for item in out:
        item["average_score_all_timeframes"] = round(
            averages.get(
                item["symbol"],
                float(item["score"]),
            ),
            2,
        )

    out.sort(
        key=lambda item: (
            -float(item["score"]),
            -float(
                item.get(
                    "average_score_all_timeframes",
                    0,
                )
            ),
            float(item["distance_pct"]),
            item["symbol"],
            (
                TIMEFRAMES.index(item["timeframe"])
                if item["timeframe"] in TIMEFRAMES
                else 99
            ),
        )
    )

    return out[:limit]


def debug_symbol(rows: List[Any], symbol: str) -> Dict[str, Any]:
    """Return transparent calculations and integrity checks for one symbol."""
    symbol = str(symbol or "").upper()
    deduped, duplicate_counts = _dedupe_rows(rows)
    items = build_opportunities(deduped, limit=1000)
    selected = [x for x in items if x.get("symbol") == symbol]
    selected.sort(key=lambda x: TIMEFRAMES.index(x["timeframe"]) if x.get("timeframe") in TIMEFRAMES else 99)
    consensus = _consensus_map(deduped).get(symbol, {})
    errors: List[str] = []
    if int(consensus.get("LONG", 0)) + int(consensus.get("SHORT", 0)) != int(consensus.get("total", 0)):
        errors.append("LONG + SHORT does not equal consensus total")
    if len({x.get("timeframe") for x in selected}) != len(selected):
        errors.append("Duplicate timeframe remained after deduplication")
    for item in selected:
        errors.extend(item.get("calculation_validation_errors", []))
    return {
        "symbol": symbol,
        "LONG": int(consensus.get("LONG", 0) or 0),
        "SHORT": int(consensus.get("SHORT", 0) or 0),
        "total": int(consensus.get("total", 0) or 0),
        "duplicates_removed": int(duplicate_counts.get(symbol, 0) or 0),
        "items": selected,
        "errors": errors,
    }
