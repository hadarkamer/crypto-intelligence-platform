"""Pure, in-memory Research Event capture for the AI candidate.

This module is deliberately isolated from PostgreSQL, Telegram and production
Watch scheduling. It does not import psycopg/sqlite and has no persistence
function. Its job is to normalize the different alert/signal shapes into one
compact, timestamp-first research event that can be validated before any
production database integration is considered.

Design rules:
- exact UTC event timestamp is preserved;
- ``setup_key`` groups repeated occurrences of the same logical setup;
- ``event_fingerprint`` identifies one exact occurrence, so repetitions are not
  accidentally deduplicated away;
- only non-reconstructable / decision-time state is retained in the compact
  engine snapshot; raw time series are excluded;
- expected price direction is stored separately from Max-Pain liquidation side;
- the candidate sink is memory-only and bounded.
"""
from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from typing import Any, Deque, Dict, Iterable, List, Mapping, Optional

SCHEMA_VERSION = "research-event-v1"
MAX_ENGINE_SNAPSHOT_BYTES = 32_000
DEFAULT_DRY_RUN_EVENTS = 500

_ALLOWED_DIRECTIONS = {"LONG", "SHORT", "NEUTRAL"}
_ALLOWED_KINDS = {"ALERT", "SIGNAL_STATE_CHANGE"}


def _utc(value: Any = None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip().replace("Z", "+00:00")
        if not text:
            raise ValueError("event timestamp is required")
        parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso_utc(value: Any = None) -> str:
    # Keep microseconds. Event order inside one Watch cycle can matter later.
    return _utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _symbol(value: Any) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol or len(symbol) > 20 or not symbol.replace("-", "").isalnum():
        raise ValueError("invalid research-event symbol")
    return symbol


def _direction(value: Any) -> str:
    raw = str(value or "NEUTRAL").strip().upper()
    aliases = {
        "UP": "LONG", "BULL": "LONG", "BULLISH": "LONG", "BUY": "LONG",
        "DOWN": "SHORT", "BEAR": "SHORT", "BEARISH": "SHORT", "SELL": "SHORT",
        "NONE": "NEUTRAL", "FLAT": "NEUTRAL", "": "NEUTRAL",
    }
    raw = aliases.get(raw, raw)
    if raw not in _ALLOWED_DIRECTIONS:
        raise ValueError(f"invalid research-event direction: {value!r}")
    return raw


def _float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _json_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str, ensure_ascii=False))


def _canonical(value: Any) -> str:
    return json.dumps(_json_safe(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _compact_module(module: Any) -> Dict[str, Any]:
    """Keep the decision-time module state, not reconstructable raw windows."""
    if not isinstance(module, Mapping):
        return {}
    keys = (
        "family", "available", "direction", "relation", "score", "quality",
        "state", "label", "quality_status", "freshness_status", "age_minutes",
        "early_shift",
    )
    return {key: _json_safe(module.get(key)) for key in keys if key in module}


def _compact_confirmation(value: Any) -> Dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    keys = (
        "status", "label", "score_threshold", "strong_score_threshold",
        "score_ok", "score_confirmation", "strong_score_ok",
        "early_shift_opposes", "oi_opposes", "supporting_families",
        "opposing_families", "strong_core", "strong_evidence_threshold",
        "magnet_quality", "liquidity_edge_pct", "liquidity_status",
        "liquidity_label", "minimum_engine_score",
    )
    out = {key: _json_safe(value.get(key)) for key in keys if key in value}
    derivatives = value.get("derivatives")
    if isinstance(derivatives, Mapping):
        out["derivatives"] = {
            key: _json_safe(derivatives.get(key))
            for key in (
                "status", "supporting_families", "opposing_families",
                "early_shift_opposes", "oi_opposes", "strong_core",
                "positioning_score", "futures_score", "minimum_engine_score",
            )
            if key in derivatives
        }
    return out


def _compact_market_evidence(value: Any) -> Dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    modules = value.get("modules") or {}
    out: Dict[str, Any] = {
        key: _json_safe(value.get(key))
        for key in (
            "expected_price_direction", "maxpain_score", "classification",
            "classification_label", "relation_to_alert", "supporting_families",
            "opposing_families", "core_supporting_families",
            "core_qualified_supporting_families", "core_opposing_families",
            "spot_context",
        )
        if key in value
    }
    if isinstance(modules, Mapping):
        out["modules"] = {
            name: _compact_module(modules.get(name))
            for name in ("positioning", "futures_flow", "spot_flow")
            if isinstance(modules.get(name), Mapping)
        }
    confirmation = value.get("confirmation")
    if isinstance(confirmation, Mapping):
        out["confirmation"] = _compact_confirmation(confirmation)
    return out


def _compact_cluster(value: Any) -> Dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    return {
        key: _json_safe(value.get(key))
        for key in (
            "points", "count", "spread_pct", "average_target", "members",
            "density_points", "coverage_points", "growth_points",
            "liquidity_multiplier",
        )
        if key in value
    }


def _compact_gap(value: Any) -> Dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    return {
        key: _json_safe(value.get(key))
        for key in ("points", "advantage", "near_distance", "far_distance")
        if key in value
    }


def _bounded_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    safe = _json_safe(snapshot)
    raw = _canonical(safe).encode("utf-8")
    if len(raw) <= MAX_ENGINE_SNAPSHOT_BYTES:
        return safe
    # Fail closed rather than silently sending a giant object to storage/model.
    raise ValueError(
        f"research engine snapshot is too large: {len(raw)} bytes > {MAX_ENGINE_SNAPSHOT_BYTES}"
    )


def _expected_direction_from_maxpain_item(item: Mapping[str, Any]) -> str:
    explicit = item.get("target_direction")
    if explicit:
        return _direction(explicit)
    current = _float(item.get("current_price"))
    target = _float(item.get("target_price"))
    if current is not None and target is not None:
        if target > current:
            return "LONG"
        if target < current:
            return "SHORT"
    # In this codebase the displayed Max-Pain side is the liquidation side:
    # SHORT target means price is expected UP; LONG target means price DOWN.
    alert_side = str(item.get("side") or "").upper()
    if alert_side == "SHORT":
        return "LONG"
    if alert_side == "LONG":
        return "SHORT"
    return "NEUTRAL"


def _initial_target_distance_pct(current_price: Any, target_price: Any) -> Optional[float]:
    current = _float(current_price)
    target = _float(target_price)
    if current is None or target is None or current <= 0:
        return None
    return round(abs(target - current) / current * 100.0, 8)


def _versions(strategy_version: Optional[str], code_version: Optional[str]) -> tuple[str, str]:
    strategy = str(strategy_version or os.getenv("STRATEGY_VERSION") or "candidate-unspecified")
    code = str(
        code_version
        or os.getenv("RENDER_GIT_COMMIT")
        or os.getenv("GITHUB_SHA")
        or "candidate-unknown"
    )
    return strategy, code


def _setup_key(
    *, symbol: str, direction: str, timeframe: Optional[str], event_family: str,
    setup_identity: Optional[Mapping[str, Any]] = None,
) -> str:
    return _sha256({
        "symbol": symbol,
        "direction": direction,
        "timeframe": timeframe or "",
        "event_family": str(event_family).upper(),
        "setup_identity": _json_safe(dict(setup_identity or {})),
    })


def _event_fingerprint(
    *, setup_key: str, event_kind: str, event_type: str, alert_time_utc: str,
    occurrence_state: Optional[Mapping[str, Any]] = None,
) -> str:
    return _sha256({
        "setup_key": setup_key,
        "event_kind": event_kind,
        "event_type": str(event_type).upper(),
        "alert_time_utc": alert_time_utc,
        "occurrence_state": _json_safe(dict(occurrence_state or {})),
    })


@dataclass(frozen=True)
class ResearchEvent:
    schema_version: str
    event_kind: str
    event_type: str
    alert_time_utc: str
    symbol: str
    direction: str
    timeframe: Optional[str]
    score: Optional[float]
    current_price: Optional[float]
    target_price: Optional[float]
    initial_target_distance_pct: Optional[float]
    categories: List[str]
    setup_key: str
    event_fingerprint: str
    strategy_version: str
    code_version: str
    engine_snapshot: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def build_maxpain_event(
    item: Mapping[str, Any], *, event_type: str = "MAX_PAIN_ALERT",
    event_time: Any = None, strategy_version: Optional[str] = None,
    code_version: Optional[str] = None,
) -> ResearchEvent:
    """Normalize one Max-Pain opportunity/confirmation occurrence."""
    symbol = _symbol(item.get("symbol"))
    timeframe = str(item.get("timeframe") or "").strip() or None
    direction = _expected_direction_from_maxpain_item(item)
    timestamp = _iso_utc(event_time)
    score = _float(item.get("score", item.get("priority")))
    current_price = _float(item.get("current_price"))
    target_price = _float(item.get("target_price"))
    categories = sorted({str(x) for x in (item.get("types") or []) if str(x).strip()})
    alert_side = str(item.get("side") or "").upper() or None

    snapshot = _bounded_snapshot({
        "alert_side": alert_side,
        "score_components": _json_safe(item.get("components") or {}),
        "opposite_score": _float(item.get("opposite_score")),
        "directional_edge": _float(item.get("directional_edge")),
        "consensus_hits": item.get("consensus_hits"),
        "consensus_total": item.get("consensus_total"),
        "component_sum_check": item.get("component_sum_check"),
        "calculation_validation_errors": _json_safe(item.get("calculation_validation_errors") or []),
        "balance": _json_safe(item.get("balance") or {}),
        "cluster": _compact_cluster(item.get("cluster")),
        "gap": _compact_gap(item.get("gap")),
        "maxpain_confirmation": _compact_confirmation(item.get("maxpain_confirmation")),
        "market_evidence": _compact_market_evidence(item.get("market_evidence")),
        "price_source": item.get("price_source"),
        "price_pair": item.get("price_pair"),
    })

    setup = _setup_key(
        symbol=symbol,
        direction=direction,
        timeframe=timeframe,
        event_family="MAX_PAIN",
        setup_identity={
            "alert_side": alert_side,
            # Do not include exact target/score: repeated scans of the same
            # logical setup must remain groupable as they evolve.
        },
    )
    fingerprint = _event_fingerprint(
        setup_key=setup,
        event_kind="ALERT",
        event_type=event_type,
        alert_time_utc=timestamp,
        occurrence_state={
            "score": score,
            "target_price": target_price,
            "categories": categories,
            "confirmation_status": (snapshot.get("maxpain_confirmation") or {}).get("status"),
        },
    )
    strategy, code = _versions(strategy_version, code_version)
    return ResearchEvent(
        schema_version=SCHEMA_VERSION,
        event_kind="ALERT",
        event_type=str(event_type).upper(),
        alert_time_utc=timestamp,
        symbol=symbol,
        direction=direction,
        timeframe=timeframe,
        score=score,
        current_price=current_price,
        target_price=target_price,
        initial_target_distance_pct=_initial_target_distance_pct(current_price, target_price),
        categories=categories,
        setup_key=setup,
        event_fingerprint=fingerprint,
        strategy_version=strategy,
        code_version=code,
        engine_snapshot=snapshot,
    )


def build_magnet_event(
    magnet: Mapping[str, Any], *, confirmation: Optional[Mapping[str, Any]] = None,
    market_evidence: Optional[Mapping[str, Any]] = None, current_price: Any = None,
    event_type: Optional[str] = None, event_time: Any = None,
    strategy_version: Optional[str] = None, code_version: Optional[str] = None,
) -> ResearchEvent:
    """Normalize Magnet / Magnet Confirmation / Strong Magnet Confirmation."""
    symbol = _symbol(magnet.get("symbol"))
    side = str(magnet.get("side") or "").upper()
    direction = "LONG" if side == "UPPER" else "SHORT" if side == "LOWER" else "NEUTRAL"
    conf = dict(confirmation or {})
    status = str(conf.get("status") or "").upper()
    inferred_type = {
        "STRONG_CONFIRMED": "STRONG_MAGNET_CONFIRMATION",
        "CONFIRMED": "MAGNET_CONFIRMATION",
    }.get(status, "MAGNET_ALERT")
    event_type = str(event_type or inferred_type).upper()
    timestamp = _iso_utc(event_time)
    target = _float(magnet.get("average_target"))
    current = _float(current_price)
    quality = _float(magnet.get("magnet_quality"))
    members = [str(x) for x in (magnet.get("members") or [])]

    snapshot = _bounded_snapshot({
        "magnet": {
            key: _json_safe(magnet.get(key))
            for key in (
                "side", "count", "members", "min_target", "max_target",
                "average_target", "spread_pct", "magnet_quality",
                "liquidity_edge_pct", "gross_liquidity_timeframe",
                "gross_candidate_liquidity", "gross_opposite_liquidity",
                "liquidity_calculation_version",
            )
            if key in magnet
        },
        "magnet_confirmation": _compact_confirmation(conf),
        "market_evidence": _compact_market_evidence(market_evidence or {}),
    })
    setup = _setup_key(
        symbol=symbol,
        direction=direction,
        timeframe=None,
        event_family="MAGNET",
        setup_identity={"side": side, "members": members},
    )
    fingerprint = _event_fingerprint(
        setup_key=setup,
        event_kind="ALERT",
        event_type=event_type,
        alert_time_utc=timestamp,
        occurrence_state={
            "quality": quality,
            "target": target,
            "confirmation_status": status,
            "liquidity_edge_pct": _float(magnet.get("liquidity_edge_pct")),
        },
    )
    strategy, code = _versions(strategy_version, code_version)
    return ResearchEvent(
        schema_version=SCHEMA_VERSION,
        event_kind="ALERT",
        event_type=event_type,
        alert_time_utc=timestamp,
        symbol=symbol,
        direction=direction,
        timeframe=None,
        score=quality,
        current_price=current,
        target_price=target,
        initial_target_distance_pct=_initial_target_distance_pct(current, target),
        categories=["MAGNET", status] if status else ["MAGNET"],
        setup_key=setup,
        event_fingerprint=fingerprint,
        strategy_version=strategy,
        code_version=code,
        engine_snapshot=snapshot,
    )


def build_generic_alert_event(
    *, symbol: str, event_type: str, direction: str, event_time: Any = None,
    timeframe: Optional[str] = None, score: Any = None, current_price: Any = None,
    target_price: Any = None, categories: Optional[Iterable[str]] = None,
    engine_snapshot: Optional[Mapping[str, Any]] = None,
    setup_identity: Optional[Mapping[str, Any]] = None,
    strategy_version: Optional[str] = None, code_version: Optional[str] = None,
) -> ResearchEvent:
    """Adapter for standalone OI/CVD/Combined and future alert families."""
    normalized_symbol = _symbol(symbol)
    normalized_direction = _direction(direction)
    timestamp = _iso_utc(event_time)
    normalized_timeframe = str(timeframe or "").strip() or None
    normalized_score = _float(score)
    current = _float(current_price)
    target = _float(target_price)
    category_list = sorted({str(x) for x in (categories or []) if str(x).strip()})
    compact = _bounded_snapshot(dict(engine_snapshot or {}))
    family = str(event_type or "GENERIC_ALERT").upper()
    setup = _setup_key(
        symbol=normalized_symbol,
        direction=normalized_direction,
        timeframe=normalized_timeframe,
        event_family=family,
        setup_identity=setup_identity,
    )
    fingerprint = _event_fingerprint(
        setup_key=setup,
        event_kind="ALERT",
        event_type=family,
        alert_time_utc=timestamp,
        occurrence_state={
            "score": normalized_score,
            "categories": category_list,
            "snapshot": compact,
        },
    )
    strategy, code = _versions(strategy_version, code_version)
    return ResearchEvent(
        schema_version=SCHEMA_VERSION,
        event_kind="ALERT",
        event_type=family,
        alert_time_utc=timestamp,
        symbol=normalized_symbol,
        direction=normalized_direction,
        timeframe=normalized_timeframe,
        score=normalized_score,
        current_price=current,
        target_price=target,
        initial_target_distance_pct=_initial_target_distance_pct(current, target),
        categories=category_list,
        setup_key=setup,
        event_fingerprint=fingerprint,
        strategy_version=strategy,
        code_version=code,
        engine_snapshot=compact,
    )


def build_signal_state_change(
    *, symbol: str, signal_name: str, old_state: Any, new_state: Any,
    event_time: Any = None, direction: str = "NEUTRAL",
    timeframe: Optional[str] = None, score: Any = None,
    current_price: Any = None, evidence: Optional[Mapping[str, Any]] = None,
    strategy_version: Optional[str] = None, code_version: Optional[str] = None,
) -> ResearchEvent:
    """Capture a meaningful state transition without minute-by-minute snapshots."""
    normalized_symbol = _symbol(symbol)
    normalized_direction = _direction(direction)
    name = str(signal_name or "").strip().upper()
    if not name:
        raise ValueError("signal_name is required")
    old = _json_safe(old_state)
    new = _json_safe(new_state)
    if _canonical(old) == _canonical(new):
        raise ValueError("state-change event requires old_state != new_state")
    timestamp = _iso_utc(event_time)
    normalized_timeframe = str(timeframe or "").strip() or None
    snapshot = _bounded_snapshot({
        "signal_name": name,
        "old_state": old,
        "new_state": new,
        "evidence": _json_safe(dict(evidence or {})),
    })
    setup = _setup_key(
        symbol=normalized_symbol,
        direction=normalized_direction,
        timeframe=normalized_timeframe,
        event_family="SIGNAL_STATE_CHANGE",
        setup_identity={"signal_name": name},
    )
    fingerprint = _event_fingerprint(
        setup_key=setup,
        event_kind="SIGNAL_STATE_CHANGE",
        event_type=name,
        alert_time_utc=timestamp,
        occurrence_state={"old_state": old, "new_state": new},
    )
    strategy, code = _versions(strategy_version, code_version)
    return ResearchEvent(
        schema_version=SCHEMA_VERSION,
        event_kind="SIGNAL_STATE_CHANGE",
        event_type=name,
        alert_time_utc=timestamp,
        symbol=normalized_symbol,
        direction=normalized_direction,
        timeframe=normalized_timeframe,
        score=_float(score),
        current_price=_float(current_price),
        target_price=None,
        initial_target_distance_pct=None,
        categories=["SIGNAL_STATE_CHANGE", name],
        setup_key=setup,
        event_fingerprint=fingerprint,
        strategy_version=strategy,
        code_version=code,
        engine_snapshot=snapshot,
    )


def validate_event(event: ResearchEvent) -> None:
    if event.schema_version != SCHEMA_VERSION:
        raise ValueError("unexpected Research Event schema version")
    if event.event_kind not in _ALLOWED_KINDS:
        raise ValueError("invalid Research Event kind")
    _utc(event.alert_time_utc)
    _symbol(event.symbol)
    _direction(event.direction)
    if len(event.setup_key) != 64 or len(event.event_fingerprint) != 64:
        raise ValueError("invalid Research Event hash")
    _bounded_snapshot(event.engine_snapshot)


class DryRunResearchCapture:
    """Bounded memory-only sink used before production persistence exists."""

    def __init__(self, max_events: int = DEFAULT_DRY_RUN_EVENTS):
        size = int(max_events)
        if size < 1 or size > 10_000:
            raise ValueError("max_events must be between 1 and 10000")
        self._events: Deque[ResearchEvent] = deque(maxlen=size)
        self._fingerprints: set[str] = set()

    def emit(self, event: ResearchEvent) -> bool:
        validate_event(event)
        # Exact replay is ignored; a new timestamp is a new occurrence even if
        # setup_key is identical. This preserves repetition density research.
        if event.event_fingerprint in self._fingerprints:
            return False
        if len(self._events) == self._events.maxlen and self._events:
            dropped = self._events[0]
            self._fingerprints.discard(dropped.event_fingerprint)
        self._events.append(event)
        self._fingerprints.add(event.event_fingerprint)
        return True

    def events(self) -> List[Dict[str, Any]]:
        return [event.to_dict() for event in self._events]

    def clear(self) -> None:
        self._events.clear()
        self._fingerprints.clear()

    def status(self) -> Dict[str, Any]:
        return {
            "mode": "DRY_RUN_MEMORY_ONLY",
            "schema_version": SCHEMA_VERSION,
            "events": len(self._events),
            "capacity": self._events.maxlen,
            "database_writes": False,
        }
