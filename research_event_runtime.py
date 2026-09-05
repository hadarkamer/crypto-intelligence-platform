"""Map live Watch/alert shapes into Research Events through a fail-open sidecar.

Every event is retained in a bounded in-memory tracker. Explicit production
delivery hooks may additionally enqueue it to the asynchronous Research writer;
shadow replay and self-tests keep persistence disabled. The mapper never alters
strategy, Watch or scoring state, and capture failure must not block Telegram.

Research-only state-change events are emitted on weakening/reset because the
research brief explicitly requires delayed entries, inverse signals, repeated
signals and post-strength behaviour. Those state-change events do not create
Telegram alerts or alter scoring.
"""
from __future__ import annotations

from contextvars import ContextVar, Token
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
from typing import Any, Dict, Iterable, List, Mapping, Optional

import magnet_v1
import market_confidence_engine
import google_sheets_sync
import research_event_capture
import research_event_store

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
_COMBINED_STATE: Dict[str, set[str]] = {}
_WATCH_CONTEXT: ContextVar[Dict[str, Any]] = ContextVar(
    "research_watch_context", default={}
)


def set_watch_context(**values: Any) -> Token:
    """Attach one immutable Watch-cycle identity to every emitted child event."""
    return _WATCH_CONTEXT.set({
        str(key): value for key, value in values.items() if value not in (None, "")
    })


def reset_watch_context(token: Token) -> None:
    _WATCH_CONTEXT.reset(token)


def _with_watch_context(
    event: research_event_capture.ResearchEvent,
) -> research_event_capture.ResearchEvent:
    context = dict(_WATCH_CONTEXT.get() or {})
    if not context:
        return event
    snapshot = dict(event.engine_snapshot or {})
    snapshot.update(context)
    watch_scan_id = str(context.get("watch_scan_id") or "")
    if watch_scan_id:
        snapshot["sheet_snapshot_id"] = hashlib.sha256(
            f"{watch_scan_id}|{event.symbol}|{event.direction}".encode("utf-8")
        ).hexdigest()
    return replace(event, engine_snapshot=snapshot)


def _now(value: Any = None) -> Any:
    return value if value is not None else datetime.now(timezone.utc)


def _state_key(item: Mapping[str, Any]) -> str:
    return "|".join([
        str(item.get("symbol") or "").upper(),
        str(item.get("timeframe") or ""),
        str(item.get("side") or "").upper(),
    ])


def _price_direction_from_alert_side(side: Any) -> str:
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


def _price_provenance(item: Mapping[str, Any]) -> Dict[str, Any]:
    """Preserve the decision-price source needed for reproducible outcomes."""
    return {
        "price_source": item.get("price_source"),
        "price_pair": item.get("price_pair"),
    }


def _compact_time_families(value: Any) -> Dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    out: Dict[str, Any] = {}
    for family_key, family in value.items():
        if not isinstance(family, Mapping):
            continue
        out[str(family_key)] = {
            key: family.get(key)
            for key in ("label", "direction", "score", "quality", "agreement", "weight", "state", "windows")
            if key in family
        }
    return out


def _compact_module(module: Mapping[str, Any]) -> Dict[str, Any]:
    out = {
        key: module.get(key)
        for key in (
            "family", "available", "direction", "relation", "score", "quality",
            "state", "label", "quality_status", "freshness_status", "age_minutes",
            "early_shift", "quality_reasons",
        )
        if key in module
    }
    families = _compact_time_families(module.get("time_families"))
    if families:
        out["time_families"] = families
    return out


def _emit(
    event: research_event_capture.ResearchEvent,
    *,
    persist: bool = False,
    capture_stage: str = "OBSERVED",
    delivery_status: str = "",
    delivery_attempted_at_utc: Any = None,
    delivered_at_utc: Any = None,
) -> bool:
    """Always retain a bounded memory copy; persist only on an explicit live hook.

    Shadow Replay and deterministic self-tests use the default ``persist=False``
    and therefore can never leak reconstructed history into the production
    Research Archive.
    """
    event = _with_watch_context(event)
    remembered = SINK.emit(event)
    queued = False
    if persist:
        queued = research_event_store.WRITER.enqueue(
            event,
            capture_stage=capture_stage,
            delivery_status=delivery_status,
            delivery_attempted_at_utc=delivery_attempted_at_utc,
            delivered_at_utc=delivered_at_utc,
        )
        # Sheets is an independent fail-open copy. Only Telegram events that
        # were actually delivered are exposed as live alerts in the workbook.
        if str(delivery_status or "").upper() == "DELIVERED":
            try:
                google_sheets_sync.enqueue_delivered_event(
                    event,
                    delivered_at_utc=delivered_at_utc,
                )
            except Exception as exc:
                print(f"[google-sheets] alert enqueue failed open: {exc!r}", flush=True)
    return bool(remembered or queued)


def _emit_state_change(
    *, symbol: str, signal_name: str, old_state: Any, new_state: Any,
    direction: str = "NEUTRAL", score: Any = None, current_price: Any = None,
    source_side: Optional[str] = None, timeframe: Optional[str] = None,
    event_time: Any = None, evidence: Optional[Mapping[str, Any]] = None,
    persist: bool = False,
) -> bool:
    if old_state == new_state:
        return False
    event = research_event_capture.build_signal_state_change(
        symbol=symbol,
        signal_name=signal_name,
        old_state=old_state,
        new_state=new_state,
        direction=direction,
        source_side=source_side,
        score=score,
        current_price=current_price,
        timeframe=timeframe,
        event_time=_now(event_time),
        evidence=dict(evidence or {}),
    )
    return _emit(
        event,
        persist=persist,
        capture_stage="STATE_CHANGE",
        delivery_status="NOT_APPLICABLE",
    )


def capture_sent_maxpain(
    item: Mapping[str, Any],
    *,
    event_time: Any = None,
    persist: bool = False,
    delivery_status: str = "DELIVERED",
    delivery_attempted_at_utc: Any = None,
    delivered_at_utc: Any = None,
) -> bool:
    """Capture one delivered Max-Pain card using an explicitly supplied decision time when available."""
    try:
        event = research_event_capture.build_maxpain_event(
            item,
            event_type="MAX_PAIN_ALERT",
            event_time=_now(event_time),
        )
        return _emit(
            event,
            persist=persist,
            capture_stage="TELEGRAM_ALERT",
            delivery_status=delivery_status,
            delivery_attempted_at_utc=delivery_attempted_at_utc,
            delivered_at_utc=delivered_at_utc,
        )
    except Exception as exc:
        print(f"[research-dry-run] maxpain capture failed: {exc!r}", flush=True)
        return False


def capture_manual_maxpain_sample(
    item: Mapping[str, Any], *, event_time: Any = None, persist: bool = False,
) -> bool:
    """Store a user-requested scan as a Decision Sample, never as an alert.

    This prevents `/alert`, `/alerts` and forced-direction experiments from
    contaminating performance statistics for automatic delivered alerts.
    """
    try:
        base = research_event_capture.build_maxpain_event(
            item,
            event_type="MAX_PAIN_ALERT",
            event_time=_now(event_time),
        )
        sample = research_event_capture.build_decision_sample(
            symbol=base.symbol,
            sample_type="MANUAL_MAX_PAIN_SCAN",
            direction=base.direction,
            source_side=base.source_side,
            timeframe=base.timeframe,
            score=base.score,
            current_price=base.current_price,
            target_price=base.target_price,
            categories=base.categories,
            engine_snapshot=base.engine_snapshot,
            setup_identity={"source_side": base.source_side, "origin": "telegram_manual_scan"},
            event_time=base.alert_time_utc,
        )
        return _emit(
            sample,
            persist=persist,
            capture_stage="TELEGRAM_MANUAL_SCAN",
            delivery_status="NOT_APPLICABLE",
        )
    except Exception as exc:
        print(f"[research] manual sample capture failed: {exc!r}", flush=True)
        return False


def capture_special_transitions(
    items: Iterable[Mapping[str, Any]], *, event_time: Any = None,
    persist: bool = False, delivery_status: str = "DELIVERED",
    delivery_attempted_at_utc: Any = None, delivered_at_utc: Any = None,
) -> int:
    """Mirror independent alert transitions and preserve reset/weakening states."""
    timestamp = _now(event_time)
    emitted = 0
    item_list = list(items)

    for item in item_list:
        symbol = str(item.get("symbol") or "").upper()
        if not symbol:
            continue
        timeframe = str(item.get("timeframe") or "") or None
        source_side = str(item.get("side") or "").upper() or None
        key = _state_key(item)
        score = float(item.get("score", item.get("priority", 0)) or 0.0)
        current_price = item.get("current_price")
        direction = _price_direction_from_alert_side(source_side)

        was_score_active = bool(_SCORE_CONFIRMATION_STATE.get(key, False))
        score_active = was_score_active
        if score >= SCORE_CONFIRMATION_THRESHOLD:
            score_active = True
        elif score < SCORE_CONFIRMATION_RESET_THRESHOLD:
            score_active = False
        _SCORE_CONFIRMATION_STATE[key] = score_active
        if score_active and not was_score_active:
            event = research_event_capture.build_maxpain_event(
                item, event_type="MAX_PAIN_SCORE_65", event_time=timestamp
            )
            emitted += int(_emit(
                event, persist=persist, capture_stage="TELEGRAM_SPECIAL_ALERT",
                delivery_status=delivery_status,
                delivery_attempted_at_utc=delivery_attempted_at_utc,
                delivered_at_utc=delivered_at_utc,
            ))
        elif was_score_active and not score_active:
            emitted += int(_emit_state_change(
                symbol=symbol,
                signal_name="MAX_PAIN_SCORE_65",
                old_state={"active": True, "threshold": SCORE_CONFIRMATION_THRESHOLD},
                new_state={"active": False, "score": score},
                direction=direction,
                source_side=source_side,
                score=score,
                current_price=current_price,
                timeframe=timeframe,
                event_time=timestamp,
                evidence={"reset_below": SCORE_CONFIRMATION_RESET_THRESHOLD},
                persist=persist,
            ))

        confirmation = (
            item.get("maxpain_confirmation")
            or (item.get("market_evidence") or {}).get("confirmation")
            or {}
        )
        status = str(confirmation.get("status") or "UNCONFIRMED").upper()
        previous_status = _CONFIRMATION_STATE.get(key)
        _CONFIRMATION_STATE[key] = status
        if status in {"CONFIRMED", "STRONG_CONFIRMED"} and previous_status != status:
            event_type = "STRONG_MAX_PAIN_CONFIRMATION" if status == "STRONG_CONFIRMED" else "MAX_PAIN_CONFIRMATION"
            event = research_event_capture.build_maxpain_event(item, event_type=event_type, event_time=timestamp)
            emitted += int(_emit(
                event, persist=persist, capture_stage="TELEGRAM_SPECIAL_ALERT",
                delivery_status=delivery_status,
                delivery_attempted_at_utc=delivery_attempted_at_utc,
                delivered_at_utc=delivered_at_utc,
            ))
        if previous_status is not None and previous_status != status:
            emitted += int(_emit_state_change(
                symbol=symbol,
                signal_name="MAX_PAIN_CONFIRMATION",
                old_state=previous_status,
                new_state=status,
                direction=direction,
                source_side=source_side,
                score=score,
                current_price=current_price,
                timeframe=timeframe,
                event_time=timestamp,
                evidence={"confirmation": dict(confirmation)},
                persist=persist,
            ))

        was_high = bool(_HIGH_SCORE_83_STATE.get(key, False))
        is_high = score >= SPECIAL_HIGH_SCORE_THRESHOLD
        _HIGH_SCORE_83_STATE[key] = is_high
        if is_high and not was_high:
            event = research_event_capture.build_maxpain_event(item, event_type="MAX_PAIN_SCORE_83", event_time=timestamp)
            emitted += int(_emit(
                event, persist=persist, capture_stage="TELEGRAM_SPECIAL_ALERT",
                delivery_status=delivery_status,
                delivery_attempted_at_utc=delivery_attempted_at_utc,
                delivered_at_utc=delivered_at_utc,
            ))
        elif was_high and not is_high:
            emitted += int(_emit_state_change(
                symbol=symbol,
                signal_name="MAX_PAIN_SCORE_83",
                old_state={"active": True, "score_floor": SPECIAL_HIGH_SCORE_THRESHOLD},
                new_state={"active": False, "score": score},
                direction=direction,
                source_side=source_side,
                score=score,
                current_price=current_price,
                timeframe=timeframe,
                event_time=timestamp,
                persist=persist,
            ))

    source_by_symbol: Dict[str, Mapping[str, Any]] = {}
    for item in item_list:
        symbol = str(item.get("symbol") or "").upper()
        if symbol and symbol not in source_by_symbol:
            source_by_symbol[symbol] = item

    for symbol, item in source_by_symbol.items():
        evidence = item.get("market_evidence") or {}
        modules = evidence.get("modules") or {}
        current_price = item.get("current_price")

        for module_key, event_type in (("positioning", "OI_PRICE_HIGH"), ("futures_flow", "FUTURES_CVD_HIGH")):
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
                    source_side=new_state,
                    event_time=timestamp,
                    score=module_score,
                    current_price=current_price,
                    categories=[module_key, "DERIVATIVES_HIGH_65"],
                    engine_snapshot={
                        "module": _compact_module(module),
                        "market_evidence": {
                            "modules": {
                                str(name): _compact_module(value)
                                for name, value in modules.items()
                                if isinstance(value, Mapping)
                            }
                        },
                        "threshold": DERIVATIVES_HIGH_THRESHOLD,
                        "reset_threshold": DERIVATIVES_HIGH_RESET_THRESHOLD,
                        **_price_provenance(item),
                    },
                    setup_identity={"module": module_key},
                )
                emitted += int(_emit(
                    event, persist=persist, capture_stage="TELEGRAM_SPECIAL_ALERT",
                    delivery_status=delivery_status,
                    delivery_attempted_at_utc=delivery_attempted_at_utc,
                    delivered_at_utc=delivered_at_utc,
                ))
            if previous != new_state and previous != "NEUTRAL":
                emitted += int(_emit_state_change(
                    symbol=symbol,
                    signal_name=event_type,
                    old_state=previous,
                    new_state=new_state,
                    direction=_direction_from_bull_bear(new_state),
                    source_side=new_state,
                    score=module_score,
                    current_price=current_price,
                    event_time=timestamp,
                    evidence={"module": _compact_module(module)},
                    persist=persist,
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
                    source_side=new_state,
                    event_time=timestamp,
                    score=quality,
                    current_price=current_price,
                    categories=["SPOT_CVD", family_key, "QUALITY_65"],
                    engine_snapshot={
                        "family_key": family_key,
                        "family": {key: family.get(key) for key in ("label", "direction", "score", "quality", "agreement", "weight", "windows") if key in family},
                        "market_evidence": {
                            "modules": {
                                str(name): _compact_module(value)
                                for name, value in modules.items()
                                if isinstance(value, Mapping)
                            }
                        },
                        "threshold": DERIVATIVES_HIGH_THRESHOLD,
                        "reset_threshold": DERIVATIVES_HIGH_RESET_THRESHOLD,
                        **_price_provenance(item),
                    },
                    setup_identity={"spot_family": family_key},
                )
                emitted += int(_emit(
                    event, persist=persist, capture_stage="TELEGRAM_SPECIAL_ALERT",
                    delivery_status=delivery_status,
                    delivery_attempted_at_utc=delivery_attempted_at_utc,
                    delivered_at_utc=delivered_at_utc,
                ))
            if previous != new_state and previous != "NEUTRAL":
                emitted += int(_emit_state_change(
                    symbol=symbol,
                    signal_name=f"SPOT_CVD_HIGH:{family_key}",
                    old_state=previous,
                    new_state=new_state,
                    direction=_direction_from_bull_bear(new_state),
                    source_side=new_state,
                    score=quality,
                    current_price=current_price,
                    event_time=timestamp,
                    evidence={"family_key": family_key, "family": dict(family)},
                    persist=persist,
                ))

    return emitted


def capture_combined_state_changes(
    candidates: Iterable[Mapping[str, Any]], *, event_time: Any = None,
    persist: bool = False,
) -> int:
    """Research-only Combined composition tracker, including weakening and disappearance.

    main.py remains authoritative for whether Telegram emits a Combined alert.
    This tracker only records changes in the already-built candidate signal sets.
    """
    timestamp = _now(event_time)
    candidate_list = list(candidates)
    current_by_key: Dict[str, Mapping[str, Any]] = {
        str(candidate.get("key") or ""): candidate
        for candidate in candidate_list
        if str(candidate.get("key") or "")
    }
    emitted = 0

    for key, previous_signals in list(_COMBINED_STATE.items()):
        if key in current_by_key:
            continue
        symbol, source_side = key.split("|", 1) if "|" in key else (key, "")
        emitted += int(_emit_state_change(
            symbol=symbol,
            signal_name="COMBINED_CONFIRMATION_STATE",
            old_state={"active": True, "signal_keys": sorted(previous_signals)},
            new_state={"active": False, "signal_keys": []},
            direction=_price_direction_from_alert_side(source_side),
            source_side=source_side,
            event_time=timestamp,
            evidence={"reason": "combined_candidate_no_longer_meets_minimum"},
            persist=persist,
        ))
        _COMBINED_STATE.pop(key, None)

    for key, candidate in current_by_key.items():
        current_signals = set(str(x) for x in (candidate.get("signal_keys") or set()))
        previous_signals = _COMBINED_STATE.get(key)
        _COMBINED_STATE[key] = current_signals
        if previous_signals is None or previous_signals == current_signals:
            continue
        symbol = str(candidate.get("symbol") or "").upper()
        source_side = str(candidate.get("side") or "").upper()
        top_item = candidate.get("top_item") or {}
        emitted += int(_emit_state_change(
            symbol=symbol,
            signal_name="COMBINED_CONFIRMATION_STATE",
            old_state={"active": True, "signal_keys": sorted(previous_signals)},
            new_state={"active": True, "signal_keys": sorted(current_signals)},
            direction=_price_direction_from_alert_side(source_side),
            source_side=source_side,
            score=top_item.get("score", top_item.get("priority")),
            current_price=top_item.get("current_price"),
            timeframe=str(top_item.get("timeframe") or "") or None,
            event_time=timestamp,
            evidence={
                "added": sorted(current_signals - previous_signals),
                "removed": sorted(previous_signals - current_signals),
                "signal_count": len(current_signals),
            },
            persist=persist,
        ))
    return emitted


def capture_combined_confirmation(
    candidate: Mapping[str, Any], *, event_time: Any = None,
    persist: bool = False, delivery_status: str = "DELIVERED",
    delivery_attempted_at_utc: Any = None, delivered_at_utc: Any = None,
) -> bool:
    """Capture a Combined alert occurrence only after main.py approved its Telegram transition."""
    try:
        top_item = candidate.get("top_item") or {}
        source_side = str(candidate.get("side") or "").upper()
        signal_keys = sorted(str(x) for x in (candidate.get("signal_keys") or set()))
        event = research_event_capture.build_generic_alert_event(
            symbol=str(candidate.get("symbol") or "").upper(),
            event_type="COMBINED_CONFIRMATION",
            direction=_price_direction_from_alert_side(source_side),
            source_side=source_side,
            event_time=_now(event_time),
            timeframe=str(top_item.get("timeframe") or "") or None,
            score=top_item.get("score", top_item.get("priority")),
            current_price=top_item.get("current_price"),
            target_price=top_item.get("target_price"),
            categories=signal_keys,
            engine_snapshot={
                "alert_side": source_side,
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
                "market_evidence": top_item.get("market_evidence") or {},
                "top_item_confirmation": top_item.get("maxpain_confirmation") or {},
                "top_item_average_score_all_timeframes": top_item.get("average_score_all_timeframes"),
                "top_item_price_source": top_item.get("price_source"),
                "top_item_price_pair": top_item.get("price_pair"),
            },
            setup_identity={
                "alert_side": source_side,
                "signal_families": sorted({key.split(":", 1)[0] for key in signal_keys}),
            },
        )
        return _emit(
            event,
            persist=persist,
            capture_stage="TELEGRAM_COMBINED_ALERT",
            delivery_status=delivery_status,
            delivery_attempted_at_utc=delivery_attempted_at_utc,
            delivered_at_utc=delivered_at_utc,
        )
    except Exception as exc:
        print(f"[research-dry-run] combined capture failed: {exc!r}", flush=True)
        return False


def capture_magnet_watch_symbol(
    symbol: str,
    rows: Iterable[Any],
    derivatives_snapshot: Mapping[str, Mapping[str, Any]],
    *, event_time: Any = None, persist: bool = False,
    delivery_status: str = "DELIVERED",
    delivery_attempted_at_utc: Any = None, delivered_at_utc: Any = None,
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
                persist=persist,
            ))
            _MAGNET_STATE.pop(key, None)
        return emitted

    captured = (derivatives_snapshot or {}).get(symbol) or {}
    evidence_by_direction: Dict[str, Dict[str, Any]] = {}
    for direction in {magnet_v1.expected_price_direction(magnet.get("side")) for magnet in magnets}:
        if direction not in {"BULLISH", "BEARISH"}:
            continue
        evidence_by_direction[direction] = market_confidence_engine.combine(
            symbol,
            direction,
            captured.get("regime") or {},
            captured.get("flow") or {},
        )

    price_row = next((
        row
        for row in row_list
        if str(_row_get(row, "symbol", "") or "").upper() == symbol
        and _safe_float(_row_get(row, "current_price")) is not None
    ), None)
    current_price = (
        _safe_float(_row_get(price_row, "current_price"))
        if price_row is not None
        else None
    )
    price_source = _row_get(price_row, "price_source") if price_row is not None else None
    price_pair = _row_get(price_row, "price_pair") if price_row is not None else None

    emitted = 0
    active_keys = set()
    for magnet in magnets:
        source_side = str(magnet.get("side") or "").upper()
        members = tuple(str(x) for x in (magnet.get("members") or []))
        state_key = f"{symbol}|{source_side}|{','.join(members)}"
        active_keys.add(state_key)
        direction = magnet_v1.expected_price_direction(source_side)
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
            price_source=price_source,
            price_pair=price_pair,
            event_time=timestamp,
        )
        emitted += int(_emit(
            event, persist=persist, capture_stage="TELEGRAM_MAGNET_ALERT",
            delivery_status=delivery_status,
            delivery_attempted_at_utc=delivery_attempted_at_utc,
            delivered_at_utc=delivered_at_utc,
        ))

        if previous is not None and previous != status:
            emitted += int(_emit_state_change(
                symbol=symbol,
                signal_name="MAGNET",
                old_state=previous,
                new_state=status,
                direction=_direction_from_bull_bear(direction),
                source_side=source_side,
                score=magnet.get("magnet_quality"),
                current_price=current_price,
                event_time=timestamp,
                evidence={
                    "side": source_side,
                    "members": list(members),
                    "liquidity_edge_pct": magnet.get("liquidity_edge_pct"),
                    "confirmation": dict(confirmation),
                },
                persist=persist,
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
                persist=persist,
            ))
            _MAGNET_STATE.pop(key, None)
    return emitted


def status() -> Dict[str, Any]:
    base = dict(SINK.status())
    persistence = research_event_store.WRITER.status()
    persistence_running = bool(
        persistence.get("enabled")
        and persistence.get("configured")
        and persistence.get("running")
    )
    base.update({
        "mode": "LIVE_ASYNC_PERSISTENCE" if persistence_running else "DRY_RUN_MEMORY_ONLY",
        "database_writes": persistence_running,
        "runtime_mapper": "live-alert-sidecar-v3",
        "tracked_confirmation_states": len(_CONFIRMATION_STATE),
        "tracked_score65_states": len(_SCORE_CONFIRMATION_STATE),
        "tracked_score83_states": len(_HIGH_SCORE_83_STATE),
        "tracked_derivatives_high_states": len(_DERIVATIVES_HIGH_STATE),
        "tracked_spot_family_states": len(_SPOT_FAMILY_HIGH_STATE),
        "tracked_magnet_states": len(_MAGNET_STATE),
        "tracked_combined_states": len(_COMBINED_STATE),
        "persistence": persistence,
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
    _COMBINED_STATE.clear()
