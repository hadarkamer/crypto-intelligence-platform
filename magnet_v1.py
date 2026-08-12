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
TIMEFRAME_HOURS = {
    "12h": 12.0,
    "24h": 24.0,
    "48h": 48.0,
    "3d": 72.0,
    "1w": 168.0,
    "2w": 336.0,
    "1m": 720.0,
}
MAX_CLUSTER_SPREAD_PCT = 1.0
CONFIRMATION_MIN_QUALITY = 60.0
STRONG_CONFIRMATION_MIN_QUALITY = 75.0
LIQUIDITY_OPPOSITION_PCT = -10.0
LIQUIDITY_SUPPORT_PCT = 10.0
STRONG_LIQUIDITY_SUPPORT_PCT = 20.0


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


def expected_price_direction(side: str) -> str:
    """Map a Magnet side to the price direction used by Confirmation."""
    normalized = str(side or "").upper()
    if normalized == "UPPER":
        return "BULLISH"
    if normalized == "LOWER":
        return "BEARISH"
    return "NEUTRAL"


def _derivatives_state(market_evidence: Dict[str, Any]) -> Dict[str, Any]:
    """Read the existing Confirmation evidence without changing its engine.

    Price+OI and Futures Flow remain the two voting families. Spot stays
    secondary. Existing early-shift / opposition flags remain vetoes. The raw
    state is intentionally independent of Magnet Quality so weak valid magnets
    can still collect useful validation evidence.
    """
    confirmation = market_evidence.get("confirmation") or {}
    support = int(
        market_evidence.get("core_supporting_families")
        or market_evidence.get("supporting_families")
        or confirmation.get("supporting_families")
        or 0
    )
    opposition = int(
        market_evidence.get("core_opposing_families")
        or market_evidence.get("opposing_families")
        or confirmation.get("opposing_families")
        or 0
    )
    early_against = bool(confirmation.get("early_shift_opposes"))
    oi_opposes = bool(confirmation.get("oi_opposes"))
    strong_core = bool(confirmation.get("strong_core"))

    if early_against or oi_opposes or opposition:
        status = "CONFLICT"
        label = "נגזרים סותרים את כיוון המגנט"
    elif support == 2:
        status = "STRONG_CONFIRMED" if strong_core else "CONFIRMED"
        label = (
            "Price+OI + Futures CVD מאשרים בעוצמה"
            if strong_core
            else "Price+OI + Futures CVD מאשרים"
        )
    else:
        status = "UNCONFIRMED"
        label = "אין עדיין אישור מלא מ-Price+OI + Futures CVD"

    return {
        "status": status,
        "label": label,
        "supporting_families": support,
        "opposing_families": opposition,
        "early_shift_opposes": early_against,
        "oi_opposes": oi_opposes,
        "strong_core": strong_core,
    }


def evaluate_confirmation(
    magnet: Dict[str, Any], market_evidence: Dict[str, Any]
) -> Dict[str, Any]:
    """Gate existing derivatives evidence with Magnet V1 MQ and Liquidity Edge.

    MQ, Liquidity Edge and derivatives evidence are never averaged. This is a
    decision matrix layered on top of the read-only V1 diagnostics.
    """
    quality = float(magnet.get("magnet_quality") or 0.0)
    raw_edge = magnet.get("liquidity_edge_pct")
    edge = float(raw_edge) if raw_edge is not None else None
    derivatives = _derivatives_state(market_evidence)

    if edge is None:
        liquidity_status = "UNAVAILABLE"
        liquidity_label = "אין נתון Liquidity Edge"
    elif edge <= LIQUIDITY_OPPOSITION_PCT:
        liquidity_status = "OPPOSE"
        liquidity_label = "נזילות מתנגדת למגנט"
    elif edge >= LIQUIDITY_SUPPORT_PCT:
        liquidity_status = "SUPPORT"
        liquidity_label = "נזילות תומכת במגנט"
    else:
        liquidity_status = "NEUTRAL"
        liquidity_label = "נזילות מאוזנת / ניטרלית"

    derivatives_status = derivatives["status"]
    derivatives_confirm = derivatives_status in {"CONFIRMED", "STRONG_CONFIRMED"}

    if quality < CONFIRMATION_MIN_QUALITY:
        status = "OBSERVATION"
        label = "👁 Observation — MQ מתחת ל-60"
    elif derivatives_status == "CONFLICT":
        status = "NOT_CONFIRMED"
        label = "❌ Magnet לא מאומת — נגזרים סותרים"
    elif not derivatives_confirm:
        status = "NOT_CONFIRMED"
        label = "❌ Magnet עדיין לא מאומת"
    elif liquidity_status == "UNAVAILABLE":
        status = "LIQUIDITY_UNAVAILABLE"
        label = "⚪ אין הכרעת Magnet — חסר Liquidity Edge"
    elif liquidity_status == "OPPOSE":
        status = "LIQUIDITY_CONFLICT"
        label = "⚠️ Liquidity Conflict — ה-Confirmation הגולמי נשמר"
    elif (
        quality >= STRONG_CONFIRMATION_MIN_QUALITY
        and edge is not None
        and edge >= STRONG_LIQUIDITY_SUPPORT_PCT
        and derivatives_status == "STRONG_CONFIRMED"
    ):
        status = "STRONG_CONFIRMED"
        label = "🔥 Strong Magnet Confirmation"
    else:
        status = "CONFIRMED"
        label = "✅ Magnet Confirmed"

    return {
        "status": status,
        "label": label,
        "magnet_quality": quality,
        "liquidity_edge_pct": edge,
        "liquidity_status": liquidity_status,
        "liquidity_label": liquidity_label,
        "derivatives": derivatives,
    }


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
    """Build gross Liquidity Edge and non-overlapping Consistency diagnostics.

    CoinGlass timeframe totals are cumulative. The Liquidity Edge therefore
    uses the widest available cumulative timeframe directly: it answers which
    side has more actual gross liquidity in the current cumulative snapshot.

    The first available timeframe
    is therefore a named baseline (normally ``12h``), not a subtraction from a
    nonexistent earlier range. Every later valid layer contains only the
    positive amount added since the previous valid timeframe. A non-monotonic
    window is reported and skipped without becoming the next comparison anchor.
    These layers are used only for Consistency. Distance/reachability and time
    reduction are deliberately excluded from this transitional version.
    """
    details: List[Dict[str, Any]] = []
    signed_layer_imbalances: List[float] = []
    non_monotonic_layers: List[str] = []
    # Keep the last *valid* cumulative window as the comparison anchor.  A
    # non-monotonic window must not become the baseline for the following one;
    # otherwise one bad 24h sample can manufacture a false 24h→48h increment.
    anchor_candidate: Optional[float] = None
    anchor_opposite: Optional[float] = None
    anchor_hours = 0.0
    anchor_timeframe: Optional[str] = None

    ordered = sorted(
        list(entries), key=lambda item: TIMEFRAME_ORDER.get(item["timeframe"], 99)
    )
    gross_candidate_total: Optional[float] = None
    gross_opposite_total: Optional[float] = None
    gross_timeframe: Optional[str] = None
    for entry in ordered:
        timeframe = str(entry["timeframe"])
        timeframe_hours = float(TIMEFRAME_HOURS[timeframe])
        cumulative_candidate = float(entry.get("candidate_liquidity", 0.0) or 0.0)
        cumulative_opposite = float(entry.get("opposite_liquidity", 0.0) or 0.0)
        gross_candidate_total = cumulative_candidate
        gross_opposite_total = cumulative_opposite
        gross_timeframe = timeframe

        is_baseline = anchor_candidate is None
        additional_hours = (
            timeframe_hours if is_baseline else timeframe_hours - anchor_hours
        )
        layer_candidate = (
            cumulative_candidate
            if is_baseline
            else cumulative_candidate - float(anchor_candidate)
        )
        layer_opposite = (
            cumulative_opposite
            if is_baseline
            else cumulative_opposite - float(anchor_opposite)
        )

        tolerance = max(
            1e-6,
            cumulative_candidate * 1e-9,
            cumulative_opposite * 1e-9,
        )
        valid = (
            additional_hours > 0.0
            and layer_candidate >= -tolerance
            and layer_opposite >= -tolerance
        )
        issue = None
        if not valid:
            issue = "NON_MONOTONIC_CUMULATIVE_LIQUIDITY"
            non_monotonic_layers.append(timeframe)

        # Tiny floating-point negatives at an otherwise monotonic boundary are
        # zero, but a materially negative layer is flagged and excluded.
        layer_candidate = max(0.0, layer_candidate)
        layer_opposite = max(0.0, layer_opposite)
        weighted_candidate = layer_candidate if valid else 0.0
        weighted_opposite = layer_opposite if valid else 0.0
        weighted_total = weighted_candidate + weighted_opposite
        edge = (
            (weighted_candidate - weighted_opposite) / weighted_total
            if valid and weighted_total > 0.0
            else None
        )
        if edge is not None:
            signed_layer_imbalances.append(weighted_candidate - weighted_opposite)

        details.append({
            "timeframe": timeframe,
            "layer_type": "BASE" if is_baseline else "INCREMENT",
            "previous_timeframe": anchor_timeframe,
            "additional_hours": additional_hours,
            "candidate_liquidity": layer_candidate,
            "opposite_liquidity": layer_opposite,
            "cumulative_candidate_liquidity": cumulative_candidate,
            "cumulative_opposite_liquidity": cumulative_opposite,
            "previous_cumulative_candidate_liquidity": anchor_candidate,
            "previous_cumulative_opposite_liquidity": anchor_opposite,
            "time_weighted_candidate": weighted_candidate,
            "time_weighted_opposite": weighted_opposite,
            "edge_pct": round(edge * 100.0, 2) if edge is not None else None,
            "valid": valid,
            "validation_issue": issue,
            "distance_weight_applied": False,
        })

        if valid:
            anchor_candidate = cumulative_candidate
            anchor_opposite = cumulative_opposite
            anchor_hours = timeframe_hours
            anchor_timeframe = timeframe

    weighted_candidate_total = float(gross_candidate_total or 0.0)
    weighted_opposite_total = float(gross_opposite_total or 0.0)
    weighted_total = weighted_candidate_total + weighted_opposite_total
    liquidity_edge_pct = (
        round(
            (weighted_candidate_total - weighted_opposite_total)
            / weighted_total
            * 100.0,
            2,
        )
        if weighted_total > 0.0
        else None
    )
    absolute_sum = sum(abs(value) for value in signed_layer_imbalances)
    consistency_pct = (
        round(abs(sum(signed_layer_imbalances)) / absolute_sum * 100.0, 2)
        if len(signed_layer_imbalances) >= 2 and absolute_sum > 1e-12
        else None
    )
    return {
        "liquidity_edge_pct": liquidity_edge_pct,
        "consistency_pct": consistency_pct,
        "liquidity_samples": len(signed_layer_imbalances),
        "liquidity_details": details,
        "gross_liquidity_timeframe": gross_timeframe,
        "gross_candidate_liquidity": weighted_candidate_total,
        "gross_opposite_liquidity": weighted_opposite_total,
        "non_monotonic_layers": non_monotonic_layers,
        "distance_weighting_enabled": False,
        "liquidity_calculation_version": "V2_GROSS_EDGE_INCREMENTAL_CONSISTENCY_NO_DISTANCE",
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

    Liquidity V2 uses gross cumulative liquidity for Edge and incremental
    layers for Consistency. Distance is intentionally absent. Consistency
    remains a diagnostic and neither it nor Liquidity Edge alters Magnet
    Quality or the legacy score.
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
