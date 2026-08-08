"""Read-only Magnet Engine V1 diagnostics.

This module intentionally does not participate in the legacy alert score.
It evaluates Max Pain cluster geometry, relative liquidity edge, and liquidity
consistency as separate diagnostics so the V1 model can be validated alongside
the existing production scoring system.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional


TIMEFRAMES = ("12h", "24h", "48h", "3d", "1w", "2w", "1m")
TIMEFRAME_ORDER = {timeframe: index for index, timeframe in enumerate(TIMEFRAMES)}
MAX_CLUSTER_SPREAD_PCT = 1.0


def _get(row: Any, key: str, default=None):
    try:
        return row[key]
    except Exception:
        return default


def _as_positive_float(value: Any) -> float:
    try:
        return max(0.0, float(value or 0.0))
    except (TypeError, ValueError):
        return 0.0


def _as_optional_float(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0.0 else None


def _side_fields(side: str) -> Dict[str, str]:
    if side == "UPPER":
        return {
            "target": "short_max_pain",
            "candidate_liquidity": "short_liquidation_amount",
            "opposite_liquidity": "long_liquidation_amount",
        }
    if side == "LOWER":
        return {
            "target": "long_max_pain",
            "candidate_liquidity": "long_liquidation_amount",
            "opposite_liquidity": "short_liquidation_amount",
        }
    raise ValueError("side must be UPPER or LOWER")


def _is_active_target(side: str, target: float, current_price: float) -> bool:
    if side == "UPPER":
        return target > current_price
    return target < current_price


def _concentration_quality(spread_pct: float) -> float:
    """Continuous 50..100 quality for a valid 0..1% cluster.

    The 50-point floor at the valid 1% boundary is deliberate: cluster
    validity already penalizes anything wider, so a still-valid boundary
    cluster must not receive a second zero-value penalty.
    """
    bounded = max(0.0, min(MAX_CLUSTER_SPREAD_PCT, float(spread_pct)))
    return round(100.0 - 50.0 * (bounded / MAX_CLUSTER_SPREAD_PCT), 2)


def _candidate_entries(rows_by_timeframe: Dict[str, Any], side: str) -> List[Dict[str, Any]]:
    fields = _side_fields(side)
    entries: List[Dict[str, Any]] = []
    for timeframe in TIMEFRAMES:
        row = rows_by_timeframe.get(timeframe)
        if row is None:
            continue
        target = _as_optional_float(_get(row, fields["target"]))
        current_price = _as_optional_float(_get(row, "current_price"))
        if target is None or current_price is None:
            continue
        if not _is_active_target(side, target, current_price):
            continue
        entries.append({
            "timeframe": timeframe,
            "target": target,
            "current_price": current_price,
            "candidate_liquidity": _as_positive_float(
                _get(row, fields["candidate_liquidity"])
            ),
            "opposite_liquidity": _as_positive_float(
                _get(row, fields["opposite_liquidity"])
            ),
        })
    return entries


def _maximal_price_clusters(entries: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    """Return independent maximal contiguous price windows up to 1% wide."""
    ordered = sorted(entries, key=lambda item: item["target"])
    candidates: List[Dict[str, Any]] = []
    for left in range(len(ordered)):
        for right in range(left + 1, len(ordered)):
            group = ordered[left:right + 1]
            targets = [float(item["target"]) for item in group]
            average_target = sum(targets) / len(targets)
            if average_target <= 0.0:
                continue
            spread_pct = (max(targets) - min(targets)) / average_target * 100.0
            if spread_pct <= MAX_CLUSTER_SPREAD_PCT + 1e-12:
                candidates.append({
                    "left": left,
                    "right": right,
                    "entries": group,
                })

    maximal: List[List[Dict[str, Any]]] = []
    for candidate in candidates:
        left = int(candidate["left"])
        right = int(candidate["right"])
        contained = any(
            int(other["left"]) <= left
            and int(other["right"]) >= right
            and (int(other["left"]) < left or int(other["right"]) > right)
            for other in candidates
        )
        if not contained:
            maximal.append(list(candidate["entries"]))
    return maximal


def _liquidity_diagnostics(entries: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    details: List[Dict[str, Any]] = []
    signed_edges: List[float] = []
    for entry in sorted(
        list(entries), key=lambda item: TIMEFRAME_ORDER.get(item["timeframe"], 99)
    ):
        candidate = float(entry.get("candidate_liquidity", 0.0) or 0.0)
        opposite = float(entry.get("opposite_liquidity", 0.0) or 0.0)
        total = candidate + opposite
        edge = (candidate - opposite) / total if total > 0.0 else None
        if edge is not None:
            signed_edges.append(edge)
        details.append({
            "timeframe": entry["timeframe"],
            "candidate_liquidity": candidate,
            "opposite_liquidity": opposite,
            "edge_pct": round(edge * 100.0, 2) if edge is not None else None,
        })

    liquidity_edge_pct = (
        round(sum(signed_edges) / len(signed_edges) * 100.0, 2)
        if signed_edges else None
    )
    absolute_sum = sum(abs(value) for value in signed_edges)
    consistency_pct = (
        round(abs(sum(signed_edges)) / absolute_sum * 100.0, 2)
        if len(signed_edges) >= 2 and absolute_sum > 1e-12
        else None
    )
    return {
        "liquidity_edge_pct": liquidity_edge_pct,
        "consistency_pct": consistency_pct,
        "liquidity_samples": len(signed_edges),
        "liquidity_details": details,
    }


def _build_candidate(symbol: str, side: str, entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    ordered = sorted(entries, key=lambda item: TIMEFRAME_ORDER.get(item["timeframe"], 99))
    targets = [float(item["target"]) for item in ordered]
    average_target = sum(targets) / len(targets)
    spread_pct = (max(targets) - min(targets)) / average_target * 100.0
    liquidity = _liquidity_diagnostics(ordered)
    return {
        "symbol": symbol,
        "side": side,
        "count": len(ordered),
        "members": [item["timeframe"] for item in ordered],
        "min_target": min(targets),
        "max_target": max(targets),
        "average_target": average_target,
        "spread_pct": round(spread_pct, 4),
        "magnet_quality": _concentration_quality(spread_pct),
        **liquidity,
    }


def build_magnets(rows: Iterable[Any], symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    """Build independent UPPER/LOWER Magnet V1 candidates.

    Every participating timeframe gets one equal vote in Liquidity Edge. Raw
    dollar liquidity is never summed across timeframes. Consistency is returned
    only as a diagnostic and does not alter Magnet Quality or the legacy score.
    """
    requested_symbol = str(symbol or "").strip().upper() or None
    grouped: Dict[str, Dict[str, Any]] = defaultdict(dict)
    for row in rows:
        row_symbol = str(_get(row, "symbol", "") or "").strip().upper()
        timeframe = str(_get(row, "timeframe", "") or "")
        if not row_symbol or timeframe not in TIMEFRAME_ORDER:
            continue
        if requested_symbol is not None and row_symbol != requested_symbol:
            continue
        grouped[row_symbol][timeframe] = row

    magnets: List[Dict[str, Any]] = []
    for row_symbol, rows_by_timeframe in grouped.items():
        for side in ("UPPER", "LOWER"):
            entries = _candidate_entries(rows_by_timeframe, side)
            for cluster_entries in _maximal_price_clusters(entries):
                magnets.append(_build_candidate(row_symbol, side, cluster_entries))

    magnets.sort(key=lambda item: (
        item["symbol"],
        0 if item["side"] == "UPPER" else 1,
        -float(item["magnet_quality"]),
        -int(item["count"]),
        float(item["average_target"]),
    ))
    return magnets

