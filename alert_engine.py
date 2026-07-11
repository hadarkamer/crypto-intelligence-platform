"""Alert Score v2.

Final agreed principles:
- Setup Strength is not part of Alert Priority.
- Data quality is not part of the score; it is displayed as a warning only.
- Multiple alerts for one coin do not add score.
- Consensus and BTC similarity share one Directional Alignment component.
- Target clustering is scored.
- Liquidity is split into:
  1. Liquidity Density — positive 0..20
  2. Liquidity Balance — bonus/penalty -10..+10
- Historical direction persistence is not scored.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional

import analysis


TIMEFRAMES = ["12h", "24h", "48h", "3d", "1w", "2w", "1m"]
RAW_MAX_SCORE = 85.0


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


def _baseline_for_side(row: Any, side: str) -> Optional[float]:
    key = "baseline_short_liquidity" if side == "SHORT" else "baseline_long_liquidity"
    value = _get(row, key)
    if value is None:
        return None
    value = float(value)
    return value if value > 0 else None


def _gap_pct(row: Any) -> Optional[float]:
    price = _get(row, "current_price")
    short_mp = _get(row, "short_max_pain")
    long_mp = _get(row, "long_max_pain")
    if not price or short_mp is None or long_mp is None:
        return None
    return abs(float(short_mp) - float(long_mp)) / float(price) * 100.0


def _proximity_points(distance_pct: Optional[float]) -> float:
    """0..20. Full at <=0.25%, zero at >=2.00%."""
    if distance_pct is None:
        return 0.0
    if distance_pct <= 0.25:
        return 20.0
    if distance_pct >= 2.0:
        return 0.0
    return round((2.0 - distance_pct) / 1.75 * 20.0, 2)


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
    consensus_hits: int,
    consensus_total: int,
    btc_hits: int,
    btc_total: int,
) -> Dict[str, float]:
    """Total 0..20: Consensus 0..15 + BTC Like 0..5.

    BTC Like can reinforce a coherent coin, but cannot rescue a split signal.
    It receives points only when consensus is at least 5 timeframes.
    """
    consensus_points = (
        round(consensus_hits / consensus_total * 15.0, 2)
        if consensus_total else 0.0
    )

    btc_points = 0.0
    if consensus_hits >= 5 and btc_total:
        btc_points = round(btc_hits / btc_total * 5.0, 2)

    return {
        "consensus_points": consensus_points,
        "btc_like_points": btc_points,
        "total": round(consensus_points + btc_points, 2),
    }


def _cluster_map(rows: List[Any], consensus: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Target clustering for the dominant side of each coin.

    spread_pct = (max_target - min_target) / average_target * 100

    Minimum: 3 timeframes on the dominant side.
    Score:
      <=0.50%: 15
      <=1.00%: 12
      <=2.00%: 8
      <=3.00%: 4
      >3.00%: 0
    """
    grouped: Dict[str, List[Any]] = defaultdict(list)
    for row in rows:
        symbol = str(_get(row, "symbol", "")).upper()
        if symbol:
            grouped[symbol].append(row)

    result: Dict[str, Dict[str, Any]] = {}
    for symbol, items in grouped.items():
        dominant = consensus.get(symbol, {}).get("side")
        if dominant not in {"LONG", "SHORT"}:
            result[symbol] = {
                "side": dominant,
                "count": 0,
                "spread_pct": None,
                "points": 0.0,
                "targets": [],
            }
            continue

        targets = []
        for row in items:
            if _closest_side(row) != dominant:
                continue
            target = _target_for_side(row, dominant)
            if target is not None and target > 0:
                targets.append(target)

        if len(targets) < 3:
            result[symbol] = {
                "side": dominant,
                "count": len(targets),
                "spread_pct": None,
                "points": 0.0,
                "targets": targets,
            }
            continue

        avg_target = sum(targets) / len(targets)
        spread = ((max(targets) - min(targets)) / avg_target * 100.0) if avg_target else None

        if spread is None:
            points = 0.0
        elif spread <= 0.50:
            points = 15.0
        elif spread <= 1.00:
            points = 12.0
        elif spread <= 2.00:
            points = 8.0
        elif spread <= 3.00:
            points = 4.0
        else:
            points = 0.0

        result[symbol] = {
            "side": dominant,
            "count": len(targets),
            "spread_pct": spread,
            "points": points,
            "targets": targets,
        }

    return result


def _liquidity_density(
    near_amount: float,
    baseline_amount: Optional[float],
    distance_pct: Optional[float],
) -> Dict[str, Any]:
    """Liquidity Density 0..20.

    relative_liquidity = current near-side liquidity /
                         historical median for same coin, timeframe and side

    density = relative_liquidity / max(distance_pct, 0.10)

    This avoids ranking BTC above smaller coins merely because BTC has more dollars.
    """
    if near_amount <= 0 or not baseline_amount or distance_pct is None:
        return {
            "relative_liquidity": None,
            "density": None,
            "points": 0.0,
            "baseline": baseline_amount,
        }

    relative = near_amount / baseline_amount
    density = relative / max(distance_pct, 0.10)

    if density >= 4.0:
        points = 20.0
    elif density >= 2.5:
        points = 16.0
    elif density >= 1.5:
        points = 12.0
    elif density >= 1.0:
        points = 8.0
    elif density >= 0.5:
        points = 4.0
    else:
        points = 0.0

    return {
        "relative_liquidity": relative,
        "density": density,
        "points": points,
        "baseline": baseline_amount,
    }


def _liquidity_balance(near_amount: float, far_amount: float) -> Dict[str, Any]:
    """Liquidity Balance bonus/penalty -10..+10.

    balance = (near - far) / (near + far)
    points = balance * 10

    Equal sides give 0. A weaker near side subtracts points.
    """
    total = near_amount + far_amount
    if total <= 0:
        return {
            "near_share_pct": None,
            "near_far_ratio": None,
            "balance": None,
            "points": 0.0,
        }

    balance = (near_amount - far_amount) / total
    near_share = near_amount / total * 100.0
    ratio = near_amount / far_amount if far_amount > 0 else None

    return {
        "near_share_pct": near_share,
        "near_far_ratio": ratio,
        "balance": balance,
        "points": round(balance * 10.0, 2),
    }


def build_opportunities(rows: List[Any], limit: int = 30) -> List[Dict[str, Any]]:
    """Build one independent alert per coin/timeframe."""
    consensus = _consensus_map(rows)
    btc_like = _btc_similarity_map(rows)
    clusters = _cluster_map(rows, consensus)
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
        baseline = _baseline_for_side(row, side)

        cons = consensus.get(symbol, {})
        consensus_hits = int(cons.get("hits", 0) or 0)
        consensus_total = int(cons.get("total", 0) or 0)

        btc = btc_like.get(symbol, {})
        btc_hits = int(btc.get("hits", 0) or 0)
        btc_total = int(btc.get("total", 0) or 0)

        directional = _directional_alignment(
            consensus_hits, consensus_total, btc_hits, btc_total
        )
        cluster = clusters.get(symbol, {
            "count": 0, "spread_pct": None, "points": 0.0, "side": None
        })
        density = _liquidity_density(near_amount, baseline, distance)
        balance = _liquidity_balance(near_amount, far_amount)
        gap = _gap_pct(row)

        types: List[str] = []
        if distance <= 0.75:
            types.append("NEAR_MAX_PAIN")
        if balance["near_far_ratio"] is not None and balance["near_far_ratio"] >= 2.0:
            types.append("LIQUIDITY_IMBALANCE_NEAR_SIDE")
        if distance <= 1.0 and density["points"] >= 12.0:
            types.append("HIGH_LIQUIDITY_CLOSE_DISTANCE")
        if cluster["points"] >= 8.0 and cluster.get("side") == side:
            types.append("TARGET_CLUSTER")
        if gap is not None and gap >= 20.0:
            types.append("EXTREME_GAP")

        if not types:
            continue

        components = {
            "proximity": _proximity_points(distance),
            "directional_alignment": directional["total"],
            "consensus": directional["consensus_points"],
            "btc_like": directional["btc_like_points"],
            "target_clustering": float(cluster["points"]),
            "liquidity_density": float(density["points"]),
            "liquidity_balance": float(balance["points"]),
        }

        raw_score = round(
            components["proximity"]
            + components["directional_alignment"]
            + components["target_clustering"]
            + components["liquidity_density"]
            + components["liquidity_balance"],
            2,
        )
        priority = round(max(0.0, min(100.0, raw_score / RAW_MAX_SCORE * 100.0)), 2)

        out.append({
            "symbol": symbol,
            "timeframe": timeframe,
            "side": side,
            "types": types,
            "priority": priority,
            "raw_score": raw_score,
            "raw_max_score": RAW_MAX_SCORE,
            "distance_pct": distance,
            "near_amount": near_amount,
            "far_amount": far_amount,
            "near_share_pct": balance["near_share_pct"],
            "near_far_ratio": balance["near_far_ratio"],
            "liquidity_balance": balance["balance"],
            "baseline_liquidity": baseline,
            "relative_liquidity": density["relative_liquidity"],
            "liquidity_density": density["density"],
            "consensus_hits": consensus_hits,
            "consensus_total": consensus_total,
            "btc_like_hits": btc_hits,
            "btc_like_total": btc_total,
            "cluster_count": cluster["count"],
            "cluster_spread_pct": cluster["spread_pct"],
            "cluster_side": cluster.get("side"),
            "gap_pct": gap,
            "components": components,
        })

    out.sort(
        key=lambda x: (
            -x["priority"],
            x["distance_pct"],
            x["symbol"],
            TIMEFRAMES.index(x["timeframe"]) if x["timeframe"] in TIMEFRAMES else 99,
        )
    )
    return out[:limit]
