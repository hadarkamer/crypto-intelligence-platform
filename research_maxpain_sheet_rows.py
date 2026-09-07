"""Event-level Max Pain rows from frozen observations, never scan averages."""
from __future__ import annotations
import json
import math
from typing import Any, Mapping

VERSION = "maxpain-event-timeframes-v1"
TIMEFRAMES = ("15m", "30m", "1h", "4h", "12h", "24h", "48h", "3d", "1w", "2w", "1m")


def mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def number(value: Any) -> float | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (ValueError, TypeError):
        return None


def classification(event_type: Any, raw_text: Any = None) -> str:
    """Only the proven historic card has an alias; original type stays intact."""
    import re
    source = str(event_type or "")
    if source == "WATCH_CANDIDATE" and re.search(r"\bMax\s*Pain\b", str(raw_text or ""), re.I):
        return "MAX_PAIN_ALERT"
    return source


def build_rows(event: Mapping[str, Any]) -> list[dict[str, Any]]:
    kind = str(event.get("event_type") or "")
    if not ("MAX_PAIN" in kind or kind == "COMBINED_CONFIRMATION"):
        return []
    snapshot = mapping(event.get("engine_snapshot"))
    fingerprint = str(event.get("event_fingerprint") or "")
    selected_side = str(snapshot.get("alert_side") or event.get("source_side") or "").upper()
    selected_tf = str(event.get("timeframe") or "")
    if not fingerprint or selected_side not in {"LONG", "SHORT"}:
        return []
    opposite_side = "SHORT" if selected_side == "LONG" else "LONG"
    observations: dict[tuple[str, str], dict[str, Any]] = {}
    # Older snapshots contain totals for both liquidation sides. Keep every
    # observed timeframe; missing components/targets remain genuinely blank.
    for side in ("LONG", "SHORT"):
        for tf, score in mapping(mapping(snapshot.get("directional_scores_all_timeframes")).get(side)).items():
            if tf in TIMEFRAMES and number(score) is not None:
                observations[(tf, side)] = {"score": number(score)}
    # Current source carries richer values from the same scoring calculation.
    rich_keys = set()
    raw_observations = snapshot.get("maxpain_timeframes")
    if isinstance(raw_observations, list) and len(raw_observations) <= 14:
        seen = set()
        for raw in raw_observations:
            raw = mapping(raw)
            key = (str(raw.get("timeframe") or ""), str(raw.get("source_side") or ""))
            if key[0] not in TIMEFRAMES or key[1] not in {"LONG", "SHORT"}:
                continue
            if key in seen:
                raise ValueError("duplicate frozen Max Pain timeframe and side")
            seen.add(key)
            rich_keys.add(key)
            observations.setdefault(key, {}).update(raw)
    if selected_tf in TIMEFRAMES:
        selected = observations.setdefault((selected_tf, selected_side), {})
        frozen = {
            "score": event.get("score"), "target_price": event.get("target_price"),
            "distance_pct": snapshot.get("distance_pct", event.get("initial_target_distance_pct")),
            "components": snapshot.get("score_components") or snapshot.get("top_item_components"),
            "near_amount": snapshot.get("near_amount"), "far_amount": snapshot.get("far_amount"),
            "near_share_pct": snapshot.get("near_share_pct"),
            "consensus_hits": snapshot.get("consensus_hits"), "consensus_total": snapshot.get("consensus_total"),
        }
        for name, value in frozen.items():
            # New capture explicitly distinguishes missing source liquidity
            # from the legacy scorer's zero defaults in top-level fields.
            if (selected_tf, selected_side) in rich_keys and name in {"near_amount", "far_amount", "near_share_pct"}:
                continue
            if value is not None and value != {}:
                selected[name] = value
        if number(snapshot.get("opposite_score")) is not None:
            observations.setdefault((selected_tf, opposite_side), {})["score"] = number(snapshot["opposite_score"])
    result = []
    for tf in TIMEFRAMES:
        selected = observations.get((tf, selected_side), {})
        opposite = observations.get((tf, opposite_side), {})
        if not selected and not opposite:
            continue
        # long_* / short_* are PRICE directions, like direction/selected_side.
        # Raw liquidation side is explicit in source_side for auditability.
        bullish = observations.get((tf, "SHORT"), {})
        bearish = observations.get((tf, "LONG"), {})
        score, other = number(selected.get("score")), number(opposite.get("score"))
        components = {str(k): number(v) for k, v in mapping(selected.get("components")).items()
                      if number(v) is not None}
        row = {
            "event_id": fingerprint,
            "snapshot_id": str(snapshot.get("sheet_snapshot_id") or fingerprint),
            "symbol": event.get("symbol"), "direction": event.get("direction"),
            "timeframe": tf, "timestamp_utc": str(event.get("alert_time_utc") or ""),
            "current_price": number(event.get("current_price")),
            "long_maxpain_price": number(bullish.get("target_price")),
            "short_maxpain_price": number(bearish.get("target_price")),
            "long_score": number(bullish.get("score")), "short_score": number(bearish.get("score")),
            "selected_side": "LONG" if selected_side == "SHORT" else "SHORT",
            "source_side": selected_side, "score_direction_basis": "PRICE_DIRECTION",
            "selected_score": score, "opposite_score": other,
            "score_edge": score - other if score is not None and other is not None else None,
            "score_ratio": score / other if score is not None and other not in (None, 0) else None,
            "long_liquidity_usd": number(bullish.get("near_amount")),
            "short_liquidity_usd": number(bearish.get("near_amount")),
            "selected_liquidity_usd": number(selected.get("near_amount")),
            "opposite_liquidity_usd": number(selected.get("far_amount")),
            "gap_score": components.get("relative_gap"),
            "proximity_score": components.get("target_proximity"),
            "cluster_score": components.get("cluster_confidence"),
            "consensus_score": components.get("consensus"),
            "components_json": json.dumps(components, sort_keys=True, separators=(",", ":")) if components else None,
            "is_alert_timeframe": tf == selected_tf,
            "target_distance_pct": number(selected.get("distance_pct")),
            "selected_liquidity_share_pct": number(selected.get("near_share_pct")),
            "consensus_hits": number(selected.get("consensus_hits")),
            "consensus_total": number(selected.get("consensus_total")),
            "source_record_type": kind,
            "quality_status": "FROZEN_COMPONENTS_PRESENT" if components else "FROZEN_TOTALS_ONLY",
            "calculation_version": VERSION,
        }
        result.append({"sheet": "MaxPain_TF", "key": "event_id,timeframe", "row": row})
    return result
