"""Magnet V1 geometry, quality, liquidity edge and confirmation."""

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
STRONG_LIQUIDITY_SUPPORT_PCT = 10.0


def _get(row: Any, key: str, default=None):
    try:
        return row[key]
    except Exception:
        return default


def _as_liquidity_float(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0.0 else None


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
    modules = market_evidence.get("modules") or {}
    positioning_score = abs(float((modules.get("positioning") or {}).get("score") or 0.0))
    futures_score = abs(float((modules.get("futures_flow") or {}).get("score") or 0.0))
    core_confirmed = (
        support == 2
        and positioning_score >= 25.0
        and futures_score >= 25.0
    )

    if early_against or oi_opposes or opposition:
        status = "CONFLICT"
        label = "נגזרים סותרים את כיוון המגנט"
    elif core_confirmed:
        status = "CONFIRMED"
        label = "Price+OI + Futures CVD מאשרים"
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
        "strong_core": core_confirmed,
        "positioning_score": round(positioning_score, 4),
        "futures_score": round(futures_score, 4),
        "minimum_engine_score": 25.0,
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
        and derivatives_confirm
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
            "candidate_liquidity": _as_liquidity_float(
                _get(row, fields["candidate_liquidity"])
            ),
            "opposite_liquidity": _as_liquidity_float(
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
    """Compare both sides only in the widest valid member of the cluster.

    Every CoinGlass timeframe is treated as its own snapshot.  We do not
    subtract one timeframe from another and we do not infer missing data as
    zero.  This keeps Liquidity Edge independent and avoids false cumulative
    data warnings.
    """
    ordered = sorted(
        list(entries), key=lambda item: TIMEFRAME_ORDER.get(item["timeframe"], 99)
    )
    valid = [
        item for item in ordered
        if item.get("candidate_liquidity") is not None
        and item.get("opposite_liquidity") is not None
    ]
    widest = valid[-1] if valid else None
    candidate = float(widest["candidate_liquidity"]) if widest else None
    opposite = float(widest["opposite_liquidity"]) if widest else None
    total = (candidate + opposite) if candidate is not None and opposite is not None else 0.0
    edge = (
        round((candidate - opposite) / total * 100.0, 2)
        if candidate is not None and opposite is not None and total > 0.0
        else None
    )
    return {
        "liquidity_edge_pct": edge,
        "gross_liquidity_timeframe": widest.get("timeframe") if widest else None,
        "gross_candidate_liquidity": candidate,
        "gross_opposite_liquidity": opposite,
        "distance_weighting_enabled": False,
        "liquidity_calculation_version": "V3_WIDEST_CLUSTER_MEMBER_NO_DISTANCE",
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

    Liquidity Edge compares both sides in the widest valid cluster member.
    Distance is intentionally absent, and Liquidity Edge does not alter Magnet
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
