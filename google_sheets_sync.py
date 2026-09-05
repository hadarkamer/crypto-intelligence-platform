"""Fail-open Google Sheets sidecar for delivered alerts and 1m outcomes.

The production Telegram path must never wait for Google Sheets.  Callers only
enqueue compact JSON payloads; a daemon thread posts them to a bound Apps Script
web app.  The web app performs idempotent upserts into the approved workbook.
"""
from __future__ import annotations

from collections import OrderedDict
from datetime import datetime
import json
import os
from queue import Empty, Full, Queue
import threading
import time
from typing import Any, Dict, Mapping, Optional
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

_TRUE = {"1", "true", "yes", "on"}
_ENABLED = os.getenv("GOOGLE_SHEETS_SYNC_ENABLED", "").strip().lower() in _TRUE
_WEBHOOK_URL = os.getenv("GOOGLE_SHEETS_WEBHOOK_URL", "").strip()
_WEBHOOK_SECRET = os.getenv("GOOGLE_SHEETS_WEBHOOK_SECRET", "").strip()
_SPREADSHEET_ID = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
_QUEUE: Queue[Dict[str, Any]] = Queue(maxsize=2000)
_THREAD: Optional[threading.Thread] = None
_LOCK = threading.Lock()
_SNAPSHOT_LOCK = threading.Lock()
_SNAPSHOT_CACHE: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
_STOP = threading.Event()
_METRICS = {
    "enqueued": 0,
    "delivered": 0,
    "queue_full_drops": 0,
    "delivery_failures": 0,
    "retries": 0,
}


def enabled() -> bool:
    return bool(_ENABLED and _WEBHOOK_URL and _WEBHOOK_SECRET and _SPREADSHEET_ID)


def status() -> Dict[str, Any]:
    return {
        "enabled": _ENABLED,
        "configured": bool(_WEBHOOK_URL and _WEBHOOK_SECRET and _SPREADSHEET_ID),
        "spreadsheet_id": _SPREADSHEET_ID or None,
        "running": bool(_THREAD and _THREAD.is_alive()),
        "queue_size": _QUEUE.qsize(),
        "fail_open": True,
        "metrics": dict(_METRICS),
    }


def _ensure_thread() -> None:
    global _THREAD
    if not enabled() or (_THREAD and _THREAD.is_alive()):
        return
    with _LOCK:
        if _THREAD and _THREAD.is_alive():
            return
        _STOP.clear()
        _THREAD = threading.Thread(target=_run, name="google-sheets-sync", daemon=True)
        _THREAD.start()


def enqueue(payload: Mapping[str, Any]) -> bool:
    if not enabled():
        return False
    _ensure_thread()
    envelope = {
        "secret": _WEBHOOK_SECRET,
        "spreadsheet_id": _SPREADSHEET_ID,
        "payload": dict(payload),
    }
    try:
        _QUEUE.put_nowait(envelope)
        _METRICS["enqueued"] += 1
        return True
    except Full:
        _METRICS["queue_full_drops"] += 1
        print("[google-sheets] queue full; sheet copy dropped", flush=True)
        return False


def _run() -> None:
    while not _STOP.is_set():
        try:
            item = _QUEUE.get(timeout=1.0)
        except Empty:
            continue
        delivered = False
        for attempt in range(1, 6):
            try:
                request = Request(
                    _WEBHOOK_URL,
                    data=json.dumps(item, ensure_ascii=False, default=str).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urlopen(request, timeout=8) as response:
                    body = json.loads(response.read().decode("utf-8"))
                if body.get("ok") is not True:
                    raise RuntimeError(f"Sheets webhook rejected payload: {body!r}")
                delivered = True
                _METRICS["delivered"] += 1
                break
            except Exception as exc:
                _METRICS["delivery_failures"] += 1
                if attempt < 5:
                    _METRICS["retries"] += 1
                    time.sleep(min(30.0, 0.5 * (2 ** (attempt - 1))))
                else:
                    print(f"[google-sheets] delivery abandoned after retries: {exc!r}", flush=True)
        _QUEUE.task_done()
        if not delivered:
            continue


def _float(value: Any) -> Optional[float]:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _direction(value: Any) -> str:
    value = str(value or "").upper()
    return {"BULLISH": "LONG", "BEARISH": "SHORT"}.get(value, value)


def _module(snapshot: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    market = snapshot.get("market_evidence") or {}
    return (market.get("modules") or {}).get(name) or {}


def _module_total(snapshot: Mapping[str, Any], name: str) -> tuple[str, Optional[float]]:
    module = _module(snapshot, name)
    score = _float(module.get("score"))
    direction = _direction(module.get("direction"))
    if direction not in {"LONG", "SHORT"} and score is not None:
        direction = "LONG" if score > 0 else "SHORT" if score < 0 else "NEUTRAL"
    return direction, abs(score) if score is not None else None


def _is_aligned(direction: str, values: list[tuple[str, Optional[float]]]) -> bool:
    return bool(direction in {"LONG", "SHORT"} and all(d == direction for d, score in values if score is not None) and all(score is not None for _, score in values))


def _merged_snapshot(row: Mapping[str, Any]) -> Dict[str, Any]:
    """Merge Telegram child alerts into one research row per Watch/symbol/side."""
    key = str(row.get("snapshot_id") or "")
    if not key:
        return dict(row)
    with _SNAPSHOT_LOCK:
        existing = dict(_SNAPSHOT_CACHE.get(key) or {})
        merged = dict(existing)
        for name, value in row.items():
            if value not in (None, ""):
                merged[name] = value
            elif name not in merged:
                merged[name] = value
        old_types = {
            value.strip()
            for value in str(existing.get("alert_types") or "").split(",")
            if value.strip()
        }
        new_types = {
            value.strip()
            for value in str(row.get("alert_types") or "").split(",")
            if value.strip()
        }
        merged["alert_types"] = ", ".join(sorted(old_types | new_types))
        merged["telegram_event_count"] = int(
            existing.get("telegram_event_count") or 0
        ) + 1
        priorities = {
            "COMBINED_CONFIRMATION": 5,
            "STRONG_MAX_PAIN_CONFIRMATION": 4,
            "MAX_PAIN_CONFIRMATION": 3,
            "MAX_PAIN_ALERT": 2,
            "MAGNET_ALERT": 2,
        }
        old_primary = str(existing.get("primary_alert_type") or "")
        new_primary = str(row.get("primary_alert_type") or "")
        merged["primary_alert_type"] = (
            new_primary
            if priorities.get(new_primary, 1) >= priorities.get(old_primary, 0)
            else old_primary
        )
        _SNAPSHOT_CACHE[key] = dict(merged)
        _SNAPSHOT_CACHE.move_to_end(key)
        while len(_SNAPSHOT_CACHE) > 5000:
            _SNAPSHOT_CACHE.popitem(last=False)
        return merged


def enqueue_delivered_event(event: Any, *, delivered_at_utc: Any = None) -> bool:
    """Copy one successfully delivered Telegram event to all live Sheet views."""
    if not enabled():
        return False
    data = event.to_dict() if hasattr(event, "to_dict") else dict(event)
    snapshot = data.get("engine_snapshot") or {}
    if not isinstance(snapshot, Mapping):
        snapshot = {}
    fingerprint = str(data.get("event_fingerprint") or "")
    sheet_snapshot_id = str(snapshot.get("sheet_snapshot_id") or fingerprint)
    analysis_direction = _direction(
        snapshot.get("analysis_direction") or data.get("direction")
    )
    displayed_direction = _direction(
        snapshot.get("displayed_direction") or analysis_direction
    )
    direction = analysis_direction  # Backward-compatible outcome direction.
    modules = [
        _module_total(snapshot, "positioning"),
        _module_total(snapshot, "futures_flow"),
        _module_total(snapshot, "spot_flow"),
    ]
    aligned = _is_aligned(direction, modules)
    triple = bool(aligned and all((score or 0) >= 65 for _, score in modules))
    timestamp = str(data.get("alert_time_utc") or "")
    try:
        israel_time = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).astimezone(ZoneInfo("Asia/Jerusalem")).strftime("%d/%m/%Y %H:%M")
    except Exception:
        israel_time = timestamp
    max_selected = _float(data.get("score"))
    max_opposite = _float(snapshot.get("opposite_score"))
    selected_avg = _float(snapshot.get("average_score_all_timeframes"))
    opposite_avg = _float(snapshot.get("opposite_average_score_all_timeframes"))
    score_edge = None if max_selected is None or max_opposite is None else max_selected - max_opposite
    average_edge = None if selected_avg is None or opposite_avg is None else selected_avg - opposite_avg
    categories = data.get("categories") or []
    event_type = str(data.get("event_type") or "")
    snapshot_row = {
        "snapshot_id": sheet_snapshot_id,
        "timestamp_utc": timestamp,
        "timestamp_israel": israel_time,
        "watch_scan_id": snapshot.get("watch_scan_id"),
        "parent_event_id": fingerprint,
        "btc_parent_movement_id": snapshot.get("btc_parent_movement_id"),
        "symbol": data.get("symbol"),
        "direction": direction,
        "displayed_direction": displayed_direction,
        "analysis_direction": analysis_direction,
        "no_alert_snapshot": False,
        "reference_price": data.get("current_price"),
        "data_quality_status": "LIVE",
        "alert_sent": True,
        "alert_types": ", ".join(str(x) for x in categories),
        "primary_alert_type": event_type,
        "telegram_event_count": 1,
        "price_oi_total_direction": modules[0][0],
        "price_oi_total_score": modules[0][1],
        "futures_cvd_total_direction": modules[1][0],
        "futures_cvd_total_score": modules[1][1],
        "spot_cvd_total_direction": modules[2][0],
        "spot_cvd_total_score": modules[2][1],
        "all_three_aligned": aligned,
        "strict_triple_65_match": triple,
        "maxpain_selected_timeframe": data.get("timeframe"),
        "maxpain_selected_score": max_selected,
        "maxpain_opposite_score": max_opposite,
        "maxpain_score_edge": score_edge,
        "maxpain_score_ratio": (max_selected / max_opposite) if max_selected is not None and max_opposite not in (None, 0) else None,
        "maxpain_direction_average": selected_avg,
        "maxpain_opposite_average": opposite_avg,
        "maxpain_average_edge": average_edge,
        "maxpain_average_ratio": (selected_avg / opposite_avg) if selected_avg is not None and opposite_avg not in (None, 0) else None,
        "consensus_hits": snapshot.get("consensus_hits"),
        "consensus_total": snapshot.get("consensus_total"),
        "target_price": data.get("target_price"),
        "target_distance_pct": data.get("initial_target_distance_pct"),
        "liquidity_balance_pct": snapshot.get("near_share_pct"),
        "strategy_version": data.get("strategy_version"),
        "code_version": data.get("code_version"),
        "snapshot_written_at": delivered_at_utc or timestamp,
    }
    snapshot_row = _merged_snapshot(snapshot_row)
    live_row = {
        "זמן סריקה": israel_time,
        "מטבע": data.get("symbol"),
        "כיוון נבדק": direction,
        "כיוון מוצג": displayed_direction,
        "כיוון ניתוח": analysis_direction,
        "מחיר ייחוס": data.get("current_price"),
        "נשלחה התראה": "כן",
        "סוג התראה": snapshot_row.get("primary_alert_type"),
        "Price/OI כולל": snapshot_row.get("price_oi_total_score"),
        "כיוון Price/OI": snapshot_row.get("price_oi_total_direction"),
        "Futures CVD כולל": snapshot_row.get("futures_cvd_total_score"),
        "כיוון Futures": snapshot_row.get("futures_cvd_total_direction"),
        "Spot CVD כולל": snapshot_row.get("spot_cvd_total_score"),
        "כיוון Spot": snapshot_row.get("spot_cvd_total_direction"),
        "שלישייה 65+": "כן" if snapshot_row.get("strict_triple_65_match") else "לא",
        "MaxPain נבחר": snapshot_row.get("maxpain_selected_score"),
        "MaxPain נגדי": snapshot_row.get("maxpain_opposite_score"),
        "פער MaxPain": snapshot_row.get("maxpain_score_edge"),
        "ממוצע לכיוון": snapshot_row.get("maxpain_direction_average"),
        "ממוצע נגדי": snapshot_row.get("maxpain_opposite_average"),
        "יעד": snapshot_row.get("target_price"),
        "מרחק ליעד": snapshot_row.get("target_distance_pct"),
        "מאזן נזילות": snapshot_row.get("liquidity_balance_pct"),
        "סטטוס נתונים": "LIVE",
        "snapshot_id": sheet_snapshot_id,
    }
    telegram_row = {
        "event_id": fingerprint,
        "snapshot_id": sheet_snapshot_id,
        "telegram_message_id": None,
        "timestamp_utc": timestamp,
        "symbol": data.get("symbol"),
        "direction": direction,
        "displayed_direction": displayed_direction,
        "analysis_direction": analysis_direction,
        "record_type": event_type,
        "timeframe": data.get("timeframe"),
        "verification_status": "DELIVERED",
        "raw_text": None,
    }
    return enqueue({"kind": "alert", "upserts": [
        {"sheet": "תצוגת לייב", "key": "snapshot_id", "row": live_row},
        {"sheet": "Snapshots", "key": "snapshot_id", "row": snapshot_row},
        {"sheet": "Telegram_Events", "key": "event_id", "row": telegram_row},
    ]})


def enqueue_first_touch_outcome(*, event: Mapping[str, Any], horizon: int, reference_price: float, reference_source: str, path_result: Mapping[str, Any], first_touch: Mapping[str, Any], quality: str) -> bool:
    if not enabled():
        return False
    event_id = str(event.get("event_id") or "")
    threshold = _float(first_touch.get("qualifying_move_threshold_pct"))
    row = {
        "snapshot_id": (
            (_mapping(event.get("engine_snapshot"))).get("sheet_snapshot_id")
            or event.get("event_fingerprint")
            or event_id
        ),
        "symbol": event.get("symbol"),
        "direction": _direction(first_touch.get("direction") or event.get("direction")),
        "threshold_pct": threshold,
        "measurement_start_utc": event.get("alert_time_utc"),
        "status": {"HIT": "SUCCESS", "MISS": "FAILURE", "PENDING": "OPEN"}.get(str(first_touch.get("status") or "").upper(), first_touch.get("status")),
        "first_touch_side": "FAVORABLE" if first_touch.get("success") else "ADVERSE" if first_touch.get("failure_final") else "NONE",
        "decision_time_utc": first_touch.get("first_qualifying_move_time_utc"),
        "minutes_to_decision": (_float(first_touch.get("time_to_first_qualifying_move_seconds")) or 0) / 60 if first_touch.get("time_to_first_qualifying_move_seconds") is not None else None,
        "mae_pct": first_touch.get("pre_qualifying_mae_pct"),
        "favorable_touch_price": first_touch.get("qualifying_move_price"),
        "market_source": reference_source,
        "market_pair": path_result.get("pair"),
        "candle_interval": "1m",
        "candle_count": len(path_result.get("candles") or []),
        "data_quality_status": quality,
        "outcome_method_version": "first-touch-no-dwell-v6",
    }
    return enqueue({"kind": "outcome", "upserts": [{
        "sheet": "Outcomes",
        "key": "snapshot_id,threshold_pct",
        "row": row,
    }]})
