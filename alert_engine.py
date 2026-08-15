"""Alert Score v2.

Final agreed principles:
- Setup Strength is not part of Alert Priority.
- Data quality is not part of the score; it is displayed as a warning only.
- Multiple alerts for one coin do not add score.
- Directional Alignment is based only on the coin's own Gap consensus.
- Every coin uses the same 0..30 consensus scale, including BTC.
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




def _active_distance_for_side(row: Any, side: str) -> Optional[float]:
    """Return the still-active distance for one explicit direction."""
    price = _get(row, "current_price")
    target = _target_for_side(row, side)
    if price is None or target is None or float(price) <= 0:
        return None
    signed = (float(target) - float(price)) / float(price) * 100.0
    if side == "SHORT" and signed <= 0:
        return None
    if side == "LONG" and signed >= 0:
        return None
    return abs(signed)


def _relative_gap_points_for_side(row: Any, side: str) -> float:
    """Relative-gap points for an explicit side, using the existing formula."""
    distance = _active_distance_for_side(row, side)
    opposite = "SHORT" if side == "LONG" else "LONG"
    opposite_distance = _active_distance_for_side(row, opposite)
    if distance is None or opposite_distance is None or opposite_distance <= 0:
        return 0.0
    advantage = max(0.0, min(1.0, (opposite_distance - distance) / opposite_distance))
    return round(advantage * 15.0, 2)




def _gap_consensus_details(
    rows: List[Any],
    symbol: str,
    side: str,
    excluded_timeframe: str,
    max_points: float,
) -> Dict[str, Any]:
    """Consensus from the Gap quality of the other available timeframes.

    The alert timeframe is excluded, leaving six fixed comparison slots. Each
    slot contributes its directional relative-Gap quality (0..15). Stronger
    Gap evidence receives more influence, but a zero-quality timeframe keeps a
    base weight of 1 so it cannot disappear from the consensus average.

    weight = 1 + quality / 15
    weighted_quality = sum(quality * weight) / sum(weight)
    """
    wanted_symbol = str(symbol or "").upper()
    wanted_side = str(side or "").upper()
    excluded = str(excluded_timeframe or "")
    rows_by_timeframe = {
        str(_get(other_row, "timeframe", "") or ""): other_row
        for other_row in rows
        if str(_get(other_row, "symbol", "") or "").upper() == wanted_symbol
        and str(_get(other_row, "timeframe", "") or "") in TIMEFRAMES
    }

    qualities: List[float] = []
    for timeframe in TIMEFRAMES:
        if timeframe == excluded:
            continue
        other_row = rows_by_timeframe.get(timeframe)
        quality = (
            float(_relative_gap_points_for_side(other_row, wanted_side) or 0.0)
            if other_row is not None else 0.0
        )
        qualities.append(max(0.0, min(15.0, quality)))

    if not qualities:
        return {"points": 0.0, "supporting": 0, "total": 0, "qualities": []}

    weights = [1.0 + value / 15.0 for value in qualities]
    total_weight = sum(weights)
    weighted_quality = (
        sum(value * weight for value, weight in zip(qualities, weights)) / total_weight
        if total_weight > 0.0 else 0.0
    )
    points = round(
        max(0.0, min(float(max_points), weighted_quality / 15.0 * float(max_points))),
        2,
    )
    return {
        "points": points,
        "supporting": sum(1 for value in qualities if value > 0.0),
        "total": len(qualities),
        "qualities": qualities,
    }


def _gap_consensus_points(
    rows: List[Any],
    symbol: str,
    side: str,
    excluded_timeframe: str,
    max_points: float,
) -> float:
    """Compatibility wrapper returning only the new Gap-consensus score."""
    return float(_gap_consensus_details(
        rows, symbol, side, excluded_timeframe, max_points
    )["points"])


def _score_explicit_side(
    row: Any,
    side: str,
    consensus: Dict[str, Dict[str, Any]],
    clusters: Dict[str, Dict[str, Dict[str, Any]]],
    all_rows: Optional[List[Any]] = None,
) -> Optional[float]:
    """Calculate one direction's score without selecting the leading side."""
    symbol = str(_get(row, "symbol", "") or "").upper()
    timeframe = str(_get(row, "timeframe", "") or "")
    distance = _active_distance_for_side(row, side)
    if not symbol or not timeframe or distance is None:
        return None

    cons = consensus.get(symbol, {})
    consensus_max = 30.0
    gap_consensus_points = _gap_consensus_points(
        list(all_rows or []), symbol, side, timeframe, consensus_max
    )
    directional = _directional_alignment(
        symbol,
        int(cons.get(side, 0) or 0),
        int(cons.get("total", 0) or 0),
        side,
        consensus_points_override=gap_consensus_points,
    )
    allowed = _allowed_distance_pct(symbol, _get(row, "rank"))
    target_points = _target_proximity_points(distance, allowed)
    cluster_points = float(
        clusters.get(symbol, {}).get(side, {}).get("points", 0.0) or 0.0
    )
    gap_points = _relative_gap_points_for_side(row, side)
    score = (
        float(directional.get("total", 0.0) or 0.0)
        + float(target_points or 0.0)
        + cluster_points
        + gap_points
    )
    return round(max(0.0, min(100.0, score)), 2)

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


def _preferred_distance_ceiling(allowed_distance_pct: float) -> float:
    """Upper edge of the 25-point Max Pain proximity band.

    The lower edge stays fixed at 0.8%. The upper edge expands only upward
    with the coin's dynamic Max Pain eligibility threshold.
    """
    allowed = float(allowed_distance_pct)
    if allowed <= 2.5:
        return 1.3
    if allowed <= 2.7:
        return 1.4
    if allowed <= 3.0:
        return 1.5
    if allowed <= 3.5:
        return 1.7
    return 2.0


def _target_proximity_points(
    distance_pct: Optional[float],
    allowed_distance_pct: float,
) -> float:
    """Tradable target-distance score, 0..25, with continuous decay.

    The existing ideal band remains worth 25 points. Beyond its upper edge,
    the score declines linearly and continuously to the existing 15-point
    minimum at the symbol-specific allowed distance. Distances below 0.8% or
    above the allowed distance remain worth 0 points.
    """
    if distance_pct is None or allowed_distance_pct <= 0:
        return 0.0

    distance = float(distance_pct)
    allowed = float(allowed_distance_pct)
    preferred_ceiling = _preferred_distance_ceiling(allowed)

    if distance < 0.8 or distance > allowed:
        return 0.0
    if distance <= preferred_ceiling:
        return 25.0
    if allowed <= preferred_ceiling:
        return 15.0

    progress = (distance - preferred_ceiling) / (allowed - preferred_ceiling)
    return round(25.0 - max(0.0, min(1.0, progress)) * 10.0, 2)


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
    side: str,
    consensus_points_override: Optional[float] = None,
) -> Dict[str, Any]:
    """Directional Alignment, 0..30, from the coin's own Gap consensus.

    BTC and every altcoin use the exact same calculation. BTC is not used as
    an approval, bonus, penalty, blocker, or reference for another coin.

    Market breadth is intentionally excluded from scoring and remains
    display-only information.
    """
    consensus_max = 30.0
    consensus_points = (
        round(max(0.0, min(consensus_max, float(consensus_points_override))), 2)
        if consensus_points_override is not None
        else (
            round(consensus_hits / consensus_total * consensus_max, 2)
            if consensus_total else 0.0
        )
    )

    return {
        "consensus_points": consensus_points,
        "consensus_max": consensus_max,
        "total": consensus_points,
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


CLUSTER_MAX_SPREAD_PCT = 1.0

LIQUIDITY_GROWTH_THRESHOLDS = {
    ("12h", "24h"): 0.15,
    ("24h", "48h"): 0.20,
    ("48h", "3d"): 0.15,
    ("3d", "1w"): 0.25,
    ("1w", "2w"): 0.25,
    ("2w", "1m"): 0.30,
}

def _coverage_points(cluster_count: int) -> float:
    """Continuous 0..10 coverage score for 2..7 cluster timeframes.

    Fewer than two timeframes are not a cluster. From two timeframes onward,
    coverage increases linearly until all seven Max Pain timeframes are
    represented.
    """
    count = int(cluster_count or 0)
    if count < 2:
        return 0.0
    capped = min(count, len(TIMEFRAMES))
    return round(10.0 * (capped - 1) / (len(TIMEFRAMES) - 1), 4)



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
    """Calculate the strongest independent cluster for one direction.

    Cluster membership no longer depends on a median. A valid cluster is the
    best contiguous group of at least two directional targets whose full
    low-to-high spread is no more than 1% of their average target.
    """
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
        "candidate_cluster_count": 0,
        "candidate_clusters": [],
        "points": 0.0,
    }
    if same_direction_count < 2:
        return empty

    # Sort by price only for cluster discovery. Every contiguous price window is
    # evaluated, and the strongest valid window is selected deterministically:
    # most members, then narrowest spread, then greatest represented liquidity.
    by_target = sorted(directional_entries, key=lambda item: item["target"])
    candidates: List[Dict[str, Any]] = []
    for left in range(len(by_target)):
        for right in range(left + 1, len(by_target)):
            group = by_target[left:right + 1]
            targets = [item["target"] for item in group]
            average_target = sum(targets) / len(targets)
            if average_target <= 0:
                continue
            spread_pct = (max(targets) - min(targets)) / average_target * 100.0
            if spread_pct <= CLUSTER_MAX_SPREAD_PCT + 1e-12:
                candidates.append({
                    "entries": group,
                    "spread_pct": spread_pct,
                    "amount": sum(item["amount"] for item in group),
                    "left": left,
                    "right": right,
                })

    if not candidates:
        return empty

    # Evaluate every valid price window with the complete cluster formula. This
    # avoids selecting a very tight but non-growing group when another valid
    # group has a stronger final cluster score.
    evaluated_candidates: List[Dict[str, Any]] = []
    for candidate in candidates:
        entries = sorted(candidate["entries"], key=lambda item: item["hours"])
        count = len(entries)
        spread = float(candidate["spread_pct"] or 0.0)
        density = round(
            max(0.0, 1.0 - spread / CLUSTER_MAX_SPREAD_PCT) * 10.0,
            2,
        )
        coverage = round(_coverage_points(count), 2)
        entries_by_tf = {item["timeframe"]: item for item in entries}
        transitions: Dict[str, float] = {}
        for (previous_tf, current_tf), threshold in LIQUIDITY_GROWTH_THRESHOLDS.items():
            previous = entries_by_tf.get(previous_tf)
            current = entries_by_tf.get(current_tf)
            if previous is None or current is None:
                continue
            transitions[f"{previous_tf}->{current_tf}"] = _transition_growth_score(
                previous["amount"], current["amount"], threshold
            )
        growth = (
            round(sum(transitions.values()) / len(transitions) * 10.0, 2)
            if transitions else 0.0
        )
        multiplier = round(growth / 10.0 * 1.5, 4)
        points = round(min(30.0, (density + coverage) * multiplier), 2)
        evaluated_candidates.append({
            **candidate,
            "entries": entries,
            "density_points": density,
            "coverage_points": coverage,
            "growth_transition_scores": transitions,
            "growth_points": growth,
            "liquidity_multiplier": multiplier,
            "points": points,
        })

    # Count only maximal candidate windows so nested sub-windows are not
    # reported as separate clusters. Overlapping maximal windows remain visible
    # because they represent genuinely different valid groupings under the 1%
    # full-width rule.
    maximal_candidates: List[Dict[str, Any]] = []
    for candidate in evaluated_candidates:
        left = int(candidate.get("left", 0))
        right = int(candidate.get("right", 0))
        contained = any(
            int(other.get("left", 0)) <= left
            and int(other.get("right", 0)) >= right
            and (
                int(other.get("left", 0)) < left
                or int(other.get("right", 0)) > right
            )
            for other in evaluated_candidates
        )
        if not contained:
            maximal_candidates.append(candidate)

    candidate_summaries = []
    for candidate in sorted(
        maximal_candidates,
        key=lambda item: (-float(item.get("points", 0.0)), float(item.get("spread_pct", 0.0))),
    ):
        candidate_entries = list(candidate.get("entries") or [])
        candidate_targets = [float(item["target"]) for item in candidate_entries]
        candidate_summaries.append({
            "count": len(candidate_entries),
            "members": [item["timeframe"] for item in candidate_entries],
            "min_target": min(candidate_targets) if candidate_targets else None,
            "max_target": max(candidate_targets) if candidate_targets else None,
            "spread_pct": float(candidate.get("spread_pct", 0.0) or 0.0),
            "points": float(candidate.get("points", 0.0) or 0.0),
        })

    selected = max(
        evaluated_candidates,
        key=lambda candidate: (
            candidate["points"],
            len(candidate["entries"]),
            -candidate["spread_pct"],
            candidate["amount"],
        ),
    )
    cluster_entries = selected["entries"]
    cluster_count = len(cluster_entries)
    targets = [item["target"] for item in cluster_entries]
    median_target = _median(targets)  # informational/display compatibility only
    average_target = sum(targets) / len(targets)
    spread_pct = float(selected["spread_pct"] or 0.0)
    mean_deviation_pct = (
        sum(abs(target - average_target) / average_target * 100.0 for target in targets)
        / len(targets)
        if average_target else None
    )
    density_points = float(selected["density_points"] or 0.0)
    coverage_points = float(selected["coverage_points"] or 0.0)
    transition_scores = dict(selected["growth_transition_scores"] or {})

    # Growth receives no separate component points. It affects the cluster only
    # through the existing continuous 0..1.5 multiplier. The legacy
    # growth_points field remains as a transparent 0..10 display value.
    growth_score = float(selected["growth_points"] or 0.0)
    liquidity_multiplier = float(selected["liquidity_multiplier"] or 0.0)
    cluster_points = float(selected["points"] or 0.0)
    return {
        "side": side,
        "same_direction_count": same_direction_count,
        "count": cluster_count,
        "members": [item["timeframe"] for item in cluster_entries],
        "median_target": median_target,
        "mean_deviation_pct": mean_deviation_pct,
        "spread_pct": spread_pct,
        "density_points": density_points,
        "coverage_points": coverage_points,
        "growth_points": growth_score,
        "liquidity_multiplier": liquidity_multiplier,
        "growth_transition_scores": transition_scores,
        "candidate_cluster_count": len(candidate_summaries),
        "candidate_clusters": candidate_summaries,
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


def _score_details_for_side(
    row: Any,
    side: str,
    consensus: Dict[str, Dict[str, Any]],
    market: Dict[str, Any],
    clusters: Dict[str, Dict[str, Dict[str, Any]]],
    all_rows: List[Any],
) -> Optional[Dict[str, Any]]:
    """Build the complete score details for one explicit direction."""
    symbol = str(_get(row, "symbol", "") or "").upper()
    timeframe = str(_get(row, "timeframe", "") or "")
    rank = _get(row, "rank")
    distance = _active_distance_for_side(row, side)
    if not symbol or not timeframe or distance is None:
        return None

    cons = consensus.get(symbol, {})
    consensus_hits = int(cons.get(side, 0) or 0)
    consensus_total = int(cons.get("total", 0) or 0)
    consensus_max = 30.0
    gap_consensus = _gap_consensus_details(
        all_rows, symbol, side, timeframe, consensus_max
    )
    directional = _directional_alignment(
        symbol,
        consensus_hits,
        consensus_total,
        side,
        consensus_points_override=float(gap_consensus["points"]),
    )
    allowed_distance = _allowed_distance_pct(symbol, rank)
    target_proximity = _target_proximity_points(distance, allowed_distance)
    cluster = clusters.get(symbol, {}).get(side, {
        "count": 0, "same_direction_count": 0, "members": [],
        "spread_pct": None, "mean_deviation_pct": None,
        "median_target": None, "density_points": 0.0,
        "coverage_points": 0.0, "growth_points": 0.0,
        "liquidity_multiplier": 0.0,
        "growth_transition_scores": {}, "candidate_cluster_count": 0,
        "candidate_clusters": [], "points": 0.0, "side": side,
    })
    cluster_points = float(cluster.get("points", 0.0) or 0.0)

    opposite = "SHORT" if side == "LONG" else "LONG"
    opposite_distance = _active_distance_for_side(row, opposite)
    if opposite_distance is None or opposite_distance <= 0:
        gap = {"near_distance": None, "far_distance": None, "advantage": None, "points": 0.0}
    else:
        advantage = max(0.0, min(1.0, (opposite_distance - distance) / opposite_distance))
        gap = {
            "near_distance": distance,
            "far_distance": opposite_distance,
            "advantage": advantage,
            "points": round(advantage * 15.0, 2),
        }

    near_amount = _amount_for_side(row, side)
    far_amount = _opposite_amount(row, side)
    balance = _liquidity_balance(near_amount, far_amount)
    components = {
        "directional_alignment": directional["total"],
        "consensus": directional["consensus_points"],
        "consensus_max": directional["consensus_max"],
        "target_proximity": target_proximity,
        "cluster_confidence": cluster_points,
        "target_clustering": cluster_points,
        "cluster_density": float(cluster.get("density_points", 0.0)),
        "cluster_coverage": float(cluster.get("coverage_points", 0.0)),
        "cluster_liquidity_growth": float(cluster.get("growth_points", 0.0)),
        "cluster_liquidity_multiplier": float(cluster.get("liquidity_multiplier", 0.0)),
        "relative_gap": float(gap.get("points", 0.0) or 0.0),
    }
    score = round(max(0.0, min(100.0,
        float(components["directional_alignment"])
        + float(components["target_proximity"])
        + float(components["cluster_confidence"])
        + float(components["relative_gap"])
    )), 2)
    return {
        "side": side,
        "score": score,
        "distance": distance,
        "allowed_distance": allowed_distance,
        "consensus_hits": consensus_hits,
        "consensus_total": consensus_total,
        "gap_consensus_supporting": int(gap_consensus["supporting"]),
        "gap_consensus_total": int(gap_consensus["total"]),
        "directional": directional,
        "cluster": cluster,
        "gap": gap,
        "near_amount": near_amount,
        "far_amount": far_amount,
        "balance": balance,
        "components": components,
        "market_support_pct": market.get("short_pct") if side == "SHORT" else market.get("long_pct"),
        "market_support_count": market.get("short_count") if side == "SHORT" else market.get("long_count"),
    }


def _choose_scored_side(
    row: Any,
    candidates: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Choose the higher full score; use Max Pain distance only as tie-breaker."""
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda candidate: (
            float(candidate.get("score", 0.0) or 0.0),
            -float(candidate.get("distance", float("inf")) or float("inf")),
            1 if candidate.get("side") == "LONG" else 0,
        ),
    )


def build_opportunities(
    rows: List[Any],
    limit: int = 30,
    forced_symbol: Optional[str] = None,
    forced_side: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Score LONG and SHORT fully, then select the stronger direction.

    When ``forced_symbol`` and ``forced_side`` are supplied, only that symbol
    is displayed from the requested side.  The score itself is still produced
    by the exact same directional calculation used by ordinary alerts; all
    other symbols keep automatic selection.
    """
    forced_symbol = str(forced_symbol or "").upper() or None
    forced_side = str(forced_side or "").upper() or None
    if forced_side not in (None, "LONG", "SHORT"):
        raise ValueError("forced_side must be LONG or SHORT")
    rows, duplicate_counts = _dedupe_rows(rows)
    consensus = _consensus_map(rows)
    market = _market_bias_map(rows)
    clusters = _cluster_map(rows)

    out: List[Dict[str, Any]] = []
    directional_scores: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(
        lambda: {"LONG": {}, "SHORT": {}}
    )

    for row in rows:
        symbol = str(_get(row, "symbol", "") or "").upper()
        timeframe = str(_get(row, "timeframe", "") or "")
        rank = _get(row, "rank")
        if not symbol or not timeframe:
            continue

        candidates = []
        for side in ("LONG", "SHORT"):
            details = _score_details_for_side(
                row, side, consensus, market, clusters, rows,
            )
            if details is not None:
                candidates.append(details)
                directional_scores[symbol][side][timeframe] = float(details["score"])

        if forced_symbol == symbol and forced_side:
            selected = next((x for x in candidates if x.get("side") == forced_side), None)
        else:
            selected = _choose_scored_side(row, candidates)
        if selected is None:
            continue

        # Stage 86: distance no longer controls whether an active Max Pain target
        # is displayed. Targets below 0.8% or above the symbol-specific allowed
        # distance remain valid opportunities, but receive 0 proximity points.
        selected_distance = float(selected["distance"])
        selected_allowed = float(selected["allowed_distance"])

        side = selected["side"]
        score = float(selected["score"])
        distance = float(selected["distance"])
        allowed_distance = float(selected["allowed_distance"])
        cluster = selected["cluster"]
        gap = selected["gap"]
        balance = selected["balance"]
        components = selected["components"]
        consensus_hits = int(selected["consensus_hits"])
        consensus_total = int(selected["consensus_total"])
        cluster_points = float(cluster.get("points", 0.0) or 0.0)

        types: List[str] = []
        if distance <= allowed_distance:
            types.append("NEAR_MAX_PAIN")
        if cluster_points >= 18.0:
            types.append("TARGET_CLUSTER")
        if gap.get("advantage") is not None and float(gap["advantage"]) >= 0.40:
            types.append("RELATIVE_GAP_ADVANTAGE")
        if balance["near_share_pct"] is not None and float(balance["near_share_pct"]) >= 60.0:
            types.append("LIQUIDITY_BALANCE_SUPPORT")

        current_price = _get(row, "current_price")
        target_price = _target_for_side(row, side)
        target_direction = None
        if current_price is not None and target_price is not None:
            target_direction = "UP" if float(target_price) > float(current_price) else "DOWN"

        component_sum = round(
            float(components["directional_alignment"])
            + float(components["target_proximity"])
            + float(components["cluster_confidence"])
            + float(components["relative_gap"]), 2
        )
        validation_errors: List[str] = []
        if abs(component_sum - score) > 0.01:
            validation_errors.append(f"Score mismatch: components={component_sum:.2f}, score={score:.2f}")
        if consensus_hits > consensus_total:
            validation_errors.append(f"Consensus invalid: {consensus_hits}/{consensus_total}")
        if int(cluster.get("count", 0) or 0) > consensus_hits:
            validation_errors.append("Cluster count exceeds timeframes supporting alert direction")

        opposite_side = "SHORT" if side == "LONG" else "LONG"
        opposite_candidate = next((x for x in candidates if x["side"] == opposite_side), None)
        opposite_score = opposite_candidate.get("score") if opposite_candidate else None

        out.append({
            "symbol": symbol,
            "timeframe": timeframe,
            "rank": rank,
            "side": side,
            "current_price": current_price,
            "price_source": _get(row, "price_source"),
            "price_pair": _get(row, "price_pair"),
            "target_price": target_price,
            "target_direction": target_direction,
            "types": types,
            "priority": score,
            "score": score,
            "raw_score": score,
            "raw_max_score": 100.0,
            "distance_pct": distance,
            "distance_trade_band": (
                "BORDERLINE" if distance < 0.8 else "PREFERRED"
                if distance <= _preferred_distance_ceiling(allowed_distance) else "FARTHER"
            ),
            "allowed_distance_pct": allowed_distance,
            "near_amount": selected["near_amount"],
            "far_amount": selected["far_amount"],
            "near_share_pct": balance["near_share_pct"],
            "near_far_ratio": balance["near_far_ratio"],
            "liquidity_balance": balance["balance"],
            "consensus_hits": consensus_hits,
            "consensus_total": consensus_total,
            "gap_consensus_supporting": int(selected.get("gap_consensus_supporting", 0) or 0),
            "gap_consensus_total": int(selected.get("gap_consensus_total", 0) or 0),
            "market_support_pct": selected["market_support_pct"],
            "market_support_count": selected["market_support_count"],
            "market_total_count": market.get("total", 0),
            "cluster_count": cluster.get("count", 0),
            "cluster_same_direction_count": cluster.get("same_direction_count", 0),
            "cluster_median_target": cluster.get("median_target"),
            "cluster_mean_deviation_pct": cluster.get("mean_deviation_pct"),
            "cluster_spread_pct": cluster.get("spread_pct"),
            "cluster_density_points": cluster.get("density_points", 0.0),
            "cluster_coverage_points": cluster.get("coverage_points", 0.0),
            "cluster_growth_points": cluster.get("growth_points", 0.0),
            "cluster_liquidity_multiplier": cluster.get("liquidity_multiplier", 0.0),
            "cluster_growth_transition_scores": cluster.get("growth_transition_scores", {}),
            "cluster_candidate_count": cluster.get("candidate_cluster_count", 0),
            "cluster_candidates": cluster.get("candidate_clusters", []),
            "cluster_side": cluster.get("side"),
            "cluster_members": cluster.get("members", []),
            "duplicate_rows_removed": int(duplicate_counts.get(symbol, 0) or 0),
            "calculation_validation_errors": validation_errors,
            "component_sum_check": component_sum,
            "relative_gap_advantage": gap.get("advantage"),
            "near_distance_pct": gap.get("near_distance"),
            "far_distance_pct": gap.get("far_distance"),
            "components": components,
            "opposite_side": opposite_side,
            "opposite_score": opposite_score,
            "directional_edge": round(score - float(opposite_score), 2) if opposite_score is not None else None,
        })

    directional_averages: Dict[str, Dict[str, float]] = defaultdict(dict)
    for symbol, sides in directional_scores.items():
        for direction, tf_values in sides.items():
            values = list(tf_values.values())
            if values:
                directional_averages[symbol][direction] = round(sum(values) / len(values), 2)

    for item in out:
        symbol = item["symbol"]
        side = item["side"]
        opposite = "SHORT" if side == "LONG" else "LONG"
        item["average_score_long"] = directional_averages.get(symbol, {}).get("LONG")
        item["average_score_short"] = directional_averages.get(symbol, {}).get("SHORT")
        item["directional_scores_all_timeframes"] = {
            "LONG": dict(directional_scores.get(symbol, {}).get("LONG", {})),
            "SHORT": dict(directional_scores.get(symbol, {}).get("SHORT", {})),
        }
        item["average_score_all_timeframes"] = round(
            directional_averages.get(symbol, {}).get(side, float(item["score"])), 2
        )
        item["opposite_average_score_all_timeframes"] = directional_averages.get(symbol, {}).get(opposite)

    out.sort(key=lambda item: (
        -float(item["score"]),
        -float(item.get("average_score_all_timeframes", 0) or 0),
        float(item["distance_pct"]),
        item["symbol"],
        TIMEFRAMES.index(item["timeframe"]) if item["timeframe"] in TIMEFRAMES else 99,
    ))
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
