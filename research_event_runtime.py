"""Dry-run mapping from live Watch/alert shapes into Research Events.

This module is intentionally persistence-free.  It never opens PostgreSQL or
SQLite and cannot mutate strategy/runtime state outside its own in-memory
research tracker.  Production-like alert paths may call it as a sidecar: if
research capture fails, the caller should continue normal alert delivery.

The mapping mirrors the current main.py transition semantics:
- Max-Pain score confirmation enters at 65 and resets below 60;
- special Max-Pain 83+ alert fires on entry;
- Max-Pain Confirmation / Strong Confirmation fire on status transition;
- Price+OI and Futures CVD high alerts use +/-65 and reset below abs(60);
- Spot CVD family high alerts use quality 65+ and reset below 60;
- Combined Confirmation is captured only when main.py has already decided a
  candidate is newly active or has gained genuinely new evidence;
- Magnet Watch events are rebuilt from the exact shared Watch rows and frozen
  derivatives snapshot, without additional collection.

Research-only state-change events are also emitted on weakening/reset because
Yoni/Hadar want to study delayed entries, inverse signals and post-strength
behaviour.  Those state-change events do not create Telegram alerts.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional

import magnet_v1
import market_confidence_engine
import research_event_capture

SCORE_CONFIRMATION_THRESHOLD = 65.0
SCORE_CONFIRMATION_RESET_THRESHOLD = 60.0
SPECIAL_HIGH_SCORE_THRESHOLD = 83.0
DERIVATIVES_HIGH_THRESHOLD = 65.0
DERIVATIVES_HIGH_RESET_THRESHOLD = 60.0

SINK = research_event_capture.DryRunResearchCapture(max_events=4000)

_CONFIRMATION_STATE: Dict[str, str] = {}
_SCORE_CONFIRMATION_STATE: Dict[str, bool] = {}
_HIGH_SCORE_83_STATE: Dict[str, bool] = {}
_DERIVATIVES_HIGH_STATE: Dict[str, str] = {}
_SPOT_FAMILY_HIGH_STATE: Dict[str, str] = {}
_MAGNET_STATE: Dict[str, str] = {}


def _now(value: Any = None) -> Any:
    return value if value is not None else datetime.now(timezone.utc)


def _state_key(item: Mapping[str, Any]) -> str:
    return "|".join([
        str(item.get("symbol") or "").upper(),
        str(item.get("timeframe") or ""),
        str(item.get("side") or "").upper(),
    ])


def _price_direction_from_alert_side(side: Any) -> str:
    # In current Max-Pain cards, side is the liquidation side: SHORT Max Pain
    # lies above price and implies upward price travel; LONG lies below price.
    normalized = str(side or "").upper()
    if normalized == "SHORT":
        return "LONG"
    if normalized == "LONG":
        return "SHORT"
    return "NEUTRAL"


def _direction_from_bull_bear(value: Any) -> str:
    normalized = str(value or "NEUTRAL").upper()
    if normalized == "BULLISH":
        return "LONG"
    if normalized == "BEARISH":
        return "SHORT"
    return "NEUTRAL"


def _safe_float(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _row_get(row: Any, key: str, default=None):
    try:
        return row[key]
    except Exception:
        try:
            return row.get(key, default)
        except Exception:
            return default


def _compact_module(module: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        key: module.get(key)
        for key in (
            "family", "available", "direction", "relation", "score", "quality",
            "state", "label", "quality_status", "freshness_status", "age_minutes",
            "early_shift",
        )
        if key in module
    }


def _emit(event: research_event_capture.ResearchEvent) -> bool:
    return SINK.emit(event)


def _emit_state_change(
    *, symbol: str, signal_name: str, old_state: Any, new_state: Any,
    direction: str = "NEUTRAL", score: Any = None, current_price: Any = None,
    timeframe: Optional[str] = None, event_time: Any = None,
    evidence: Optional[Mapping[str, Any]] = None,
) -> bool:
    if old_state == new_state:
        return False
    event = research_event_capture.build_signal_state_change(
        symbol=symbol,
        signal_name=signal_name,
        old_state=old_state,
        new_state=new_state,
        direction=direction,
        score=score,
        current_price=current_price,
        timeframe=timeframe,
        event_time=_now(event_time),
        evidence=dict(evidence or {}),
    )
    return _emit(event)


def capture_sent_maxpain(item: Mapping[str, Any], *, event_time: Any = None) -> bool:
    """Capture one regular Max-Pain card at the point it is actually sent."""
    try:
        event = research_event_capture.build_maxpain_event(
            item,
            event_type="MAX_PAIN_ALERT",
            event_time=_now(event_time),
        )
        return _emit(event)
    except Exception as exc:
        print(f"[research-dry-run] maxpain capture failed: {exc!r}", flush=True)
        return False


def capture_special_transitions(
    items: Iterable[Mapping[str, Any]], *, event_time: Any = None,
) -> int:
    """Mirror current independent alert transitions and add reset-state research events."""
    timestamp = _now(event_time)
    emitted = 0
    item_list = list(items)

    for item in item_list:
        symbol = str(item.get("symbol") or "").upper()
        if not symbol:
            continue
        timeframe = str(item.get("timeframe") or "") or None
        key = _state_key(item)
        score = float(item.get("score", item.get("priority", 0)) or 0.0)
        current_price = item.get("current_price")
        direction = _price_direction_from_alert_side(item.get("side"))

        # Independent Max-Pain score confirmation: enter 65+, reset only <60.
        was_score_active = bool(_SCORE_CONFIRMATION_STATE.get(key, False))
        score_active = was_score_active
        if score >= SCORE_CONFIRMATION_THRESHOLD:
            score_active = True
        elif score < SCORE_CONFIRMATION_RESET_THRESHOLD:
            score_active = False
        _SCORE_CONFIRMATION_STATE[key] = score_active
        if score_active and not was_score_active:
            event = research_event_capture.build_maxpain_event(
                item,
                event_type="MAX_PAIN_SCORE_65",
                event_time=timestamp,
            )
            emitted += int(_emit(event))
        elif was_score_active and not score_active:
            emitted += int(_emit_state_change(
                symbol=symbol,
                signal_name="MAX_PAIN_SCORE_65",
                old_state={"active": True, "threshold": SCORE_CONFIRMATION_THRESHOLD},
                new_state={"active": False, "score": score},
                direction=direction,
                score=score,
                current_price=current_price,
                timeframe=timeframe,
                event_time=timestamp,
                evidence={"reset_below": SCORE_CONFIRMATION_RESET_THRESHOLD},
            ))

        # Full Max-Pain derivatives Confirmation.
        confirmation = (
            item.get("maxpain_confirmation")
            or (item.get("market_evidence") or {}).get("confirmation")
            or {}
        )
        status = str(confirmation.get("status") or "UNCONFIRMED").upper()
        previous_status = _CONFIRMATION_STATE.get(key)
        _CONFIRMATION_STATE[key] = status
        if status in {"CONFIRMED", "STRONG_CONFIRMED"} and previous_status != status:
            event_type = (
                "STRONG_MAX_PAIN_CONFIRMATION"
                if status == "STRONG_CONFIRMED"
                else "MAX_PAIN_CONFIRMATION"
            )
            event = research_event_capture.build_maxpain_event(
                item,
                event_type=event_type,
                event_time=timestamp,
            )
            emitted += int(_emit(event))
        if previous_status is not None and previous_status != status:
            emitted += int(_emit_state_change(
                symbol=symbol,
                signal_name="MAX_PAIN_CONFIRMATION",
                old_state=previous_status,
                new_state=status,
                direction=direction,
                score=score,
                current_price=current_price,
                timeframe=timeframe,
                event_time=timestamp,
                evidence={"confirmation": dict(confirmation)},
            ))

        # Separate Max-Pain 83+ transition: no hysteresis in current main.py.
        was_high = bool(_HIGH_SCORE_83_STATE.get(key, False))
        is_high = score >= SPECIAL_HIGH_SCORE_THRESHOLD
        _HIGH_SCORE_83_STATE[key] = is_high
        if is_high and not was_high:
            event = research_event_capture.build_maxpain_event(
                item,
                event_type="MAX_PAIN_SCORE_83",
                event_time=timestamp,
            )
            emitted += int(_emit(event))
        elif was_high and not is_high:
            emitted += int(_emit_state_change(
                symbol=symbol,
                signal_name="MAX_PAIN_SCORE_83",
                old_state={"active": True, "score_floor": SPECIAL_HIGH_SCORE_THRESHOLD},
                new_state={"active": False, "score": score},
                direction=direction,
                score=score,
                current_price=current_price,
                timeframe=timeframe,
                event_time=timestamp,
            ))

    # Current main.py evaluates these once per symbol using the first item.
    source_by_symbol: Dict[str, Mapping[str, Any]] = {}
    for item in item_list:
        symbol = str(item.get("symbol") or "").upper()
        if symbol and symbol not in source_by_symbol:
            source_by_symbol[symbol] = item

    for symbol, item in source_by_symbol.items():
        evidence = item.get("market_evidence") or {}
        modules = evidence.get("modules") or {}
        current_price = item.get("current_price")

        for module_key, event_type in (
            ("positioning", "OI_PRICE_HIGH"),
            ("futures_flow", "FUTURES_CVD_HIGH"),
        ):
            module = modules.get(module_key) or {}
            available = module.get("available") is not False
            module_score = float(module.get("score") or 0.0) if available else 0.0
            direction_bb = (
                "BULLISH" if module_score >= DERIVATIVES_HIGH_THRESHOLD
                else "BEARISH" if module_score <= -DERIVATIVES_HIGH_THRESHOLD
                else "NEUTRAL"
            )
            state_key = f"{symbol}|{module_key}"
            previous = _DERIVATIVES_HIGH_STATE.get(state_key, "NEUTRAL")
            if abs(module_score) < DERIVATIVES_HIGH_RESET_THRESHOLD or not available:
                new_state = "NEUTRAL"
            elif direction_bb != "NEUTRAL":
                new_state = direction_bb
            else:
                new_state = previous
            _DERIVATIVES_HIGH_STATE[state_key] = new_state

            if new_state in {"BULLISH", "BEARISH"} and previous != new_state:
                event = research_event_capture.build_generic_alert_event(
                    symbol=symbol,
                    event_type=event_type,
                    direction=_direction_from_bull_bear(new_state),
                    event_time=timestamp,
                    score=module_score,
                    current_price=current_price,
                    categories=[module_key, "DERIVATIVES_HIGH_65"],
                    engine_snapshot={
                        "module": _compact_module(module),
                        "threshold": DERIVATIVES_HIGH_THRESHOLD,
                        "reset_threshold": DERIVATIVES_HIGH_RESET_THRESHOLD,
                    },
                    setup_identity={"module": module_key},
                )
                emitted += int(_emit(event))
            if previous != new_state and previous != "NEUTRAL":
                emitted += int(_emit_state_change(
                    symbol=symbol,
                    signal_name=event_type,
                    old_state=previous,
                    new_state=new_state,
                    direction=_direction_from_bull_bear(new_state),
                    score=module_score,
                    current_price=current_price,
                    event_time=timestamp,
                    evidence={"module": _compact_module(module)},
                ))

        spot = modules.get("spot_flow") or {}
        families = spot.get("time_families") or {}
        for family_key in ("now", "short", "medium", "long"):
            family = families.get(family_key) or {}
            quality = float(family.get("quality") or 0.0) * 100.0
            direction_bb = str(family.get("direction") or "NEUTRAL").upper()
            state_key = f"{symbol}|{family_key}"
            previous = _SPOT_FAMILY_HIGH_STATE.get(state_key, "NEUTRAL")
            if quality < DERIVATIVES_HIGH_RESET_THRESHOLD or direction_bb not in {"BULLISH", "BEARISH"}:
                new_state = "NEUTRAL"
            elif quality >= DERIVATIVES_HIGH_THRESHOLD:
                new_state = direction_bb
            else:
                new_state = previous
            _SPOT_FAMILY_HIGH_STATE[state_key] = new_state

            if new_state in {"BULLISH", "BEARISH"} and previous != new_state:
                event = research_event_capture.build_generic_alert_event(
                    symbol=symbol,
                    event_type="SPOT_CVD_HIGH",
                    direction=_direction_from_bull_bear(new_state),
                    event_time=timestamp,
                    score=quality,
                    current_price=current_price,
                    categories=["SPOT_CVD", family_key, "QUALITY_65"],
                    engine_snapshot={
                        "family_key": family_key,
                        "family": {
                            key: family.get(key)
                            for key in ("label", "direction", "quality", "agreement", "weight", "windows")
                            if key in family
                        },
                        "threshold": DERIVATIVES_HIGH_THRESHOLD,
                        "reset_threshold": DERIVATIVES_HIGH_RESET_THRESHOLD,
                    },
                    setup_identity={"spot_family": family_key},
                )
                emitted += int(_emit(event))
            if previous != new_state and previous != "NEUTRAL":
                emitted += int(_emit_state_change(
                    symbol=symbol,
                    signal_name=f"SPOT_CVD_HIGH:{family_key}",
                    old_state=previous,
                    new_state=new_state,
                    direction=_direction_from_bull_bear(new_state),
                    score=quality,
                    current_price=current_price,
                    event_time=timestamp,
                    evidence={"family_key": family_key, "family": dict(family)},
                ))

    return emitted


def capture_combined_confirmation(candidate: Mapping[str, Any], *, event_time: Any = None) -> bool:
    """Capture a Combined Confirmation only after main.py approved its transition."""
    try:
        top_item = candidate.get("top_item") or {}
        side = str(candidate.get("side") or "").upper()
        signal_keys = sorted(str(x) for x in (candidate.get("signal_keys") or set()))
        event = research_event_capture.build_generic_alert_event(
            symbol=str(candidate.get("symbol") or "").upper(),
            event_type="COMBINED_CONFIRMATION",
            direction=_price_direction_from_alert_side(side),
            event_time=_now(event_time),
            timeframe=str(top_item.get("timeframe") or "") or None,
            score=top_item.get("score", top_item.get("priority")),
            current_price=top_item.get("current_price"),
            target_price=top_item.get("target_price"),
            categories=signal_keys,
            engine_snapshot={
                "alert_side": side,
                "signal_count": candidate.get("signal_count"),
                "signal_keys": signal_keys,
                "normal_confirmations": candidate.get("normal_confirmations") or [],
                "strong_confirmations": candidate.get("strong_confirmations") or [],
                "high_scores": candidate.get("high_scores") or [],
                "anomaly_setups": candidate.get("anomaly_setups") or [],
                "liquidity_imbalances": candidate.get("liquidity_imbalances") or [],
                "derivatives_high": candidate.get("derivatives_high") or [],
                "magnet": candidate.get("magnet") or {},
                "top_item_components": top_item.get("components") or {},
                "top_item_confirmation": top_item.get("maxpain_confirmation") or {},
            },
            setup_identity={
                "alert_side": side,
                "signal_families": sorted({key.split(":", 1)[0] for key in signal_keys}),
            },
        )
        return _emit(event)
    except Exception as exc:
        print(f"[research-dry-run] combined capture failed: {exc!r}", flush=True)
        return False


def capture_magnet_watch_symbol(
    symbol: str,
    rows: Iterable[Any],
    derivatives_snapshot: Mapping[str, Mapping[str, Any]],
    *, event_time: Any = None,
) -> int:
    """Capture the same Magnet states shown by Magnet Watch, without new I/O."""
    timestamp = _now(event_time)
    symbol = str(symbol or "").upper()
    if not symbol:
        return 0
    row_list = list(rows)
    try:
        magnets = magnet_v1.build_magnets(row_list, symbol=symbol)
    except Exception as exc:
        print(f"[research-dry-run] magnet build failed {symbol}: {exc!r}", flush=True)
        return 0
    if not magnets:
        # Record disappearance for any active setup of this symbol.
        emitted = 0
        for key, old_status in list(_MAGNET_STATE.items()):
            if not key.startswith(symbol + "|"):
                continue
            emitted += int(_emit_state_change(
                symbol=symbol,
                signal_name="MAGNET",
                old_state=old_status,
                new_state="ABSENT",
                event_time=timestamp,
                evidence={"setup": key},
            ))
            _MAGNET_STATE.pop(key, None)
        return emitted

    captured = (derivatives_snapshot or {}).get(symbol) or {}
    evidence_by_direction: Dict[str, Dict[str, Any]] = {}
    for direction in {
        magnet_v1.expected_price_direction(magnet.get("side"))
        for magnet in magnets
    }:
        if direction not in {"BULLISH", "BEARISH"}:
            continue
        evidence_by_direction[direction] = market_confidence_engine.combine(
            symbol,
            direction,
            captured.get("regime") or {},
            captured.get("flow") or {},
        )

    current_price = next((
        _safe_float(_row_get(row, "current_price"))
        for row in row_list
        if str(_row_get(row, "symbol", "") or "").upper() == symbol
        and _safe_float(_row_get(row, "current_price")) is not None
    ), None)

    emitted = 0
    active_keys = set()
    for magnet in magnets:
        side = str(magnet.get("side") or "").upper()
        members = tuple(str(x) for x in (magnet.get("members") or []))
        # Stable grouping for repeated geometry; target itself may drift.
        state_key = f"{symbol}|{side}|{','.join(members)}"
        active_keys.add(state_key)
        direction = magnet_v1.expected_price_direction(side)
        evidence = evidence_by_direction.get(direction) or {}
        confirmation = magnet_v1.evaluate_confirmation(magnet, evidence)
        status = str(confirmation.get("status") or "OBSERVATION").upper()
        previous = _MAGNET_STATE.get(state_key)
        _MAGNET_STATE[state_key] = status

        event = research_event_capture.build_magnet_event(
            magnet,
            confirmation=confirmation,
            market_evidence=evidence,
            current_price=current_price,
            event_time=timestamp,
        )
        emitted += int(_emit(event))

        if previous is not None and previous != status:
            emitted += int(_emit_state_change(
                symbol=symbol,
                signal_name="MAGNET",
                old_state=previous,
                new_state=status,
                direction=_direction_from_bull_bear(direction),
                score=magnet.get("magnet_quality"),
                current_price=current_price,
                event_time=timestamp,
                evidence={
                    "side": side,
                    "members": list(members),
                    "liquidity_edge_pct": magnet.get("liquidity_edge_pct"),
                    "confirmation": dict(confirmation),
                },
            ))

    for key, old_status in list(_MAGNET_STATE.items()):
        if key.startswith(symbol + "|") and key not in active_keys:
            emitted += int(_emit_state_change(
                symbol=symbol,
                signal_name="MAGNET",
                old_state=old_status,
                new_state="ABSENT",
                event_time=timestamp,
                evidence={"setup": key},
            ))
            _MAGNET_STATE.pop(key, None)
    return emitted


def status() -> Dict[str, Any]:
    base = dict(SINK.status())
    base.update({
        "runtime_mapper": "live-shapes-dry-run-v1",
        "tracked_confirmation_states": len(_CONFIRMATION_STATE),
        "tracked_score65_states": len(_SCORE_CONFIRMATION_STATE),
        "tracked_score83_states": len(_HIGH_SCORE_83_STATE),
        "tracked_derivatives_high_states": len(_DERIVATIVES_HIGH_STATE),
        "tracked_spot_family_states": len(_SPOT_FAMILY_HIGH_STATE),
        "tracked_magnet_states": len(_MAGNET_STATE),
    })
    return base


def events(limit: int = 50) -> List[Dict[str, Any]]:
    data = SINK.events()
    return [event.to_dict() if hasattr(event, "to_dict") else dict(event) for event in data[-max(1, int(limit)):]]


def reset() -> None:
    global SINK
    SINK = research_event_capture.DryRunResearchCapture(max_events=4000)
    _CONFIRMATION_STATE.clear()
    _SCORE_CONFIRMATION_STATE.clear()
    _HIGH_SCORE_83_STATE.clear()
    _DERIVATIVES_HIGH_STATE.clear()
    _SPOT_FAMILY_HIGH_STATE.clear()
    _MAGNET_STATE.clear()
