"""Fail-open Google Sheets sidecar for delivered alerts and 1m outcomes.

The production Telegram path must never wait for Google Sheets.  Callers only
enqueue compact JSON payloads; a daemon thread posts them to a bound Apps Script
web app.  The web app performs idempotent upserts into the approved workbook.
"""
from __future__ import annotations

from collections import OrderedDict, deque
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import math
import os
from queue import Empty, Full, Queue
import threading
import time
from typing import Any, Callable, Dict, Mapping, Optional
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

_TRUE = {"1", "true", "yes", "on"}
_ENABLED = os.getenv("GOOGLE_SHEETS_SYNC_ENABLED", "").strip().lower() in _TRUE
_WEBHOOK_URL = os.getenv("GOOGLE_SHEETS_WEBHOOK_URL", "").strip()
_WEBHOOK_SECRET = os.getenv("GOOGLE_SHEETS_WEBHOOK_SECRET", "").strip()
_SPREADSHEET_ID = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
_HTTP_TIMEOUT_SECONDS = max(
    5.0, min(60.0, float(os.getenv("GOOGLE_SHEETS_HTTP_TIMEOUT_SECONDS", "45")))
)
_QUEUE: Queue[Dict[str, Any]] = Queue(maxsize=2000)
_THREAD: Optional[threading.Thread] = None
_LOCK = threading.Lock()
_SNAPSHOT_LOCK = threading.Lock()
_SNAPSHOT_CACHE: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
_STOP = threading.Event()
_RECEIVER_VERSION: Optional[str] = None
_LAST_HTTP_SECONDS: Optional[float] = None
_ORDERED_BATCH_FALLBACK = False
_DURABLE_SNAPSHOT_MODE = False
_DELIVERY_LOCK = threading.RLock()
_DELIVERY_TURN = threading.Condition()
_DELIVERY_WAITERS: "deque[object]" = deque()
_DELIVERY_SLOT_LOCAL = threading.local()
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
        "http_timeout_seconds": _HTTP_TIMEOUT_SECONDS,
        "receiver_version": _RECEIVER_VERSION,
        "last_http_seconds": _LAST_HTTP_SECONDS,
        "ordered_outcome_batch_limit": ordered_outcome_batch_limit(),
        "durable_snapshot_mode": _DURABLE_SNAPSHOT_MODE,
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


def _deliver_envelope(envelope: Mapping[str, Any], *, attempts: int = 5) -> bool:
    """Post one already-built envelope and report confirmed delivery.

    The asynchronous alert path continues to use the in-memory queue.  Outcome
    workers may call :func:`deliver_now` after their database transaction has
    committed, which prevents a Sheet row from preceding its durable source.
    """
    global _RECEIVER_VERSION, _LAST_HTTP_SECONDS
    for attempt in range(1, max(1, int(attempts)) + 1):
        started_at = time.monotonic()
        try:
            request = Request(
                _WEBHOOK_URL,
                data=json.dumps(
                    dict(envelope), ensure_ascii=False, default=str
                ).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with _DELIVERY_LOCK:
                if _DURABLE_SNAPSHOT_MODE and _mapping(envelope.get("payload")).get("kind") in {
                    "alert", "neutral_snapshot"
                }:
                    return False
                with urlopen(request, timeout=_HTTP_TIMEOUT_SECONDS) as response:
                    body = json.loads(response.read().decode("utf-8"))
            _LAST_HTTP_SECONDS = round(time.monotonic() - started_at, 3)
            if body.get("ok") is not True:
                raise RuntimeError(
                    f"Sheets webhook rejected payload: {body!r}"
                )
            receiver_version = str(body.get("version") or "unversioned")
            if receiver_version != _RECEIVER_VERSION:
                print(
                    f"[google-sheets] receiver version={receiver_version}",
                    flush=True,
                )
            _RECEIVER_VERSION = receiver_version
            _METRICS["delivered"] += 1
            return True
        except Exception as exc:
            _LAST_HTTP_SECONDS = round(time.monotonic() - started_at, 3)
            _METRICS["delivery_failures"] += 1
            if attempt < max(1, int(attempts)):
                _METRICS["retries"] += 1
                time.sleep(min(30.0, 0.5 * (2 ** (attempt - 1))))
            else:
                print(
                    "[google-sheets] delivery abandoned after retries: "
                    f"{exc!r}",
                    flush=True,
                )
    return False


def ordered_outcome_batch_limit() -> int:
    """Keep later claims small after an unconfirmed legacy receiver request.

    The old Apps Script scans the complete worksheet for every input row. A
    multirow timeout can therefore write rows without confirming any of them.
    Retry one idempotent row per lease until a successful reply proves the
    prepared batch receiver has actually been deployed. This state affects
    request sizing only; the durable outbox remains the delivery authority.
    """
    return 1 if _ORDERED_BATCH_FALLBACK else 8


@contextmanager
def delivery_slot(*, wait_seconds: float = 120.0):
    """Reserve the shared receiver in FIFO order BEFORE leasing any rows.

    A nonblocking attempt lost its turn whenever another sender was active.
    The frequent snapshot sender could therefore repeatedly exclude outcomes.
    Remember each waiting sender's turn, with a bounded wait and no DB lease.
    The original reentrant lock still serializes all HTTP, including legacy
    callers. A timed-out waiter defers without claiming or acknowledging rows.
    """
    # A sender may invoke a helper that reserves another slot in this thread.
    # It already owns the turn; waiting behind itself would deadlock.
    if getattr(_DELIVERY_SLOT_LOCAL, "active", False):
        with _DELIVERY_LOCK:
            yield True
        return

    timeout = max(0.0, min(120.0, float(wait_seconds)))
    deadline = time.monotonic() + timeout
    ticket = object()
    acquired = False
    with _DELIVERY_TURN:
        _DELIVERY_WAITERS.append(ticket)
        _DELIVERY_TURN.notify_all()
    try:
        with _DELIVERY_TURN:
            while _DELIVERY_WAITERS[0] is not ticket:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                _DELIVERY_TURN.wait(remaining)
            has_turn = _DELIVERY_WAITERS[0] is ticket
        if has_turn:
            remaining = max(0.0, deadline - time.monotonic())
            acquired = _DELIVERY_LOCK.acquire(timeout=remaining)
        if acquired:
            _DELIVERY_SLOT_LOCAL.active = True
        yield acquired
    finally:
        if acquired:
            _DELIVERY_SLOT_LOCAL.active = False
            _DELIVERY_LOCK.release()
        with _DELIVERY_TURN:
            _DELIVERY_WAITERS.remove(ticket)
            _DELIVERY_TURN.notify_all()


def deliver_now(payload: Mapping[str, Any], *, attempts: int = 1) -> bool:
    """Confirm one committed payload; its durable outbox owns later retries.

    One bounded HTTP attempt stays within the outbox lease even when a growing
    Sheet needs more than the old eight-second timeout to complete its batch.
    """
    global _ORDERED_BATCH_FALLBACK
    if not enabled():
        return False
    envelope = {
        "secret": _WEBHOOK_SECRET,
        "spreadsheet_id": _SPREADSHEET_ID,
        "payload": dict(payload),
    }
    delivered = _deliver_envelope(envelope, attempts=attempts)
    if payload.get("kind") in {"ordered_first_touch_outcomes", "research_sheet_upserts"}:
        if not delivered:
            _ORDERED_BATCH_FALLBACK = True
        elif _RECEIVER_VERSION == "sheets-batch-v2":
            _ORDERED_BATCH_FALLBACK = False
    return delivered


def _run() -> None:
    while not _STOP.is_set():
        try:
            item = _QUEUE.get(timeout=1.0)
        except Empty:
            continue
        if _DURABLE_SNAPSHOT_MODE and _mapping(item.get("payload")).get("kind") in {
            "alert", "neutral_snapshot"
        }:
            # A committed-source replay owns aggregate snapshot generations.
            # Do not let an old in-memory child overwrite its complete row.
            _QUEUE.task_done()
            continue
        delivered = _deliver_envelope(item, attempts=5)
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


def _utc(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    try:
        parsed = (
            value
            if isinstance(value, datetime)
            else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        )
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _minutes_between(start: Any, end: Any) -> Optional[float]:
    start_utc = _utc(start)
    end_utc = _utc(end)
    if start_utc is None or end_utc is None:
        return None
    return max(0.0, (end_utc - start_utc).total_seconds() / 60.0)


def _positive_event_id(event: Mapping[str, Any]) -> str:
    raw_event_id = event.get("event_id")
    if isinstance(raw_event_id, bool):
        raise ValueError("First Touch requires a positive event_id")
    try:
        numeric_event_id = int(raw_event_id)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("First Touch requires a positive event_id") from exc
    if numeric_event_id <= 0:
        raise ValueError("First Touch requires a positive event_id")
    return str(numeric_event_id)


def _outcome_id(
    *, event_id: str, window_minutes: int, threshold_bps: int, method_version: str
) -> str:
    window = int(window_minutes)
    threshold = int(threshold_bps)
    method = str(method_version or "").strip()
    if window <= 0:
        raise ValueError("First Touch requires a positive window_minutes")
    if threshold <= 0:
        raise ValueError("First Touch requires a positive threshold_bps")
    if not method:
        raise ValueError("First Touch requires a method_version")
    # This identity mirrors the durable database key. Direction is event-level
    # content, so correcting it must update this row rather than create a stale
    # sibling in Sheets.
    return "|".join((event_id, str(window), str(threshold), method))


def _path_extrema(
    *, reference_price: float, direction: str, path_result: Mapping[str, Any]
) -> Dict[str, Optional[float]]:
    candles = list(path_result.get("candles") or [])
    reference = _float(reference_price)
    normalized = _direction(direction)
    if reference is None or reference <= 0 or not candles:
        return {
            "mfe_pct": None,
            "mae_pct": None,
            "max_favorable_price": None,
            "max_adverse_price": None,
        }
    highs = [
        _float(
            candle.get("high")
            if isinstance(candle, Mapping)
            else getattr(candle, "high", None)
        )
        for candle in candles
    ]
    lows = [
        _float(
            candle.get("low")
            if isinstance(candle, Mapping)
            else getattr(candle, "low", None)
        )
        for candle in candles
    ]
    highs = [value for value in highs if value is not None]
    lows = [value for value in lows if value is not None]
    if not highs or not lows or normalized not in {"LONG", "SHORT"}:
        return {
            "mfe_pct": None,
            "mae_pct": None,
            "max_favorable_price": None,
            "max_adverse_price": None,
        }
    if normalized == "LONG":
        favorable = max(reference, max(highs))
        adverse = min(reference, min(lows))
        mfe = (favorable - reference) / reference * 100.0
        mae = (reference - adverse) / reference * 100.0
    else:
        favorable = min(reference, min(lows))
        adverse = max(reference, max(highs))
        mfe = (reference - favorable) / reference * 100.0
        mae = (adverse - reference) / reference * 100.0
    return {
        "mfe_pct": max(0.0, mfe),
        "mae_pct": max(0.0, mae),
        "max_favorable_price": favorable,
        "max_adverse_price": adverse,
    }


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


def _liquidity_fields(
    data: Mapping[str, Any], snapshot: Mapping[str, Any]
) -> Dict[str, Any]:
    """Recover captured Max-Pain balances without mixing unrelated metrics.

    Max-Pain's source side names the positions being liquidated: SHORT
    liquidity is above price (LONG price direction), and LONG liquidity is
    below it. Combined events retain balances in a per-timeframe list rather
    than the top-level field used by a Max-Pain card. Missing observations stay
    missing; Magnet's liquidity edge is a different measure.
    """
    def percentage(value: Any) -> Optional[float]:
        parsed = _float(value)
        return (
            parsed
            if parsed is not None and math.isfinite(parsed) and 0 <= parsed <= 100
            else None
        )

    event_type = str(data.get("event_type") or "").upper()
    source_side = str(snapshot.get("alert_side") or data.get("source_side") or "").upper()
    timeframe = str(data.get("timeframe") or "")
    near = _float(snapshot.get("near_amount"))
    far = _float(snapshot.get("far_amount"))
    near = near if near is not None and math.isfinite(near) and near >= 0 else None
    far = far if far is not None and math.isfinite(far) and far >= 0 else None
    share = percentage(snapshot.get("near_share_pct"))
    source = "MAX_PAIN_NEAR_SHARE" if share is not None else None
    if share is None:
        share = percentage(_mapping(snapshot.get("balance")).get("near_share_pct"))
        source = "MAX_PAIN_BALANCE" if share is not None else None
    if share is None:
        if (
            near is not None and far is not None
            and near + far > 0
        ):
            share = near / (near + far) * 100.0
            source = "MAX_PAIN_CAPTURED_AMOUNTS"
    balances = []
    for item in snapshot.get("liquidity_imbalances") or []:
        if not isinstance(item, Mapping):
            continue
        item_share = percentage(item.get("share_pct"))
        if item_share is not None:
            balances.append({
                "timeframe": str(item.get("timeframe") or ""),
                "share_pct": item_share,
            })
    if share is None and event_type == "COMBINED_CONFIRMATION":
        selected = [item for item in balances if item["timeframe"] == timeframe]
        if not selected and len(balances) == 1:
            selected = balances
        # A disagreement for the same timeframe is not a valid single balance.
        if selected and len({item["share_pct"] for item in selected}) == 1:
            share = selected[0]["share_pct"]
            timeframe = selected[0]["timeframe"]
            source = "COMBINED_CAPTURED_TIMEFRAME"
    inverse_family = "MAX_PAIN" in event_type or event_type == "COMBINED_CONFIRMATION"
    long_share = short_share = None
    if share is not None and inverse_family and source_side in {"LONG", "SHORT"}:
        long_share = share if source_side == "SHORT" else 100.0 - share
        short_share = 100.0 - long_share
    return {
        "liquidity_balance_pct": share,
        "selected_liquidity_usd": near,
        "opposite_liquidity_usd": far,
        "liquidity_long_pct": long_share,
        "liquidity_short_pct": short_share,
        "liquidity_timeframe": timeframe if share is not None else None,
        "liquidity_data_source": source,
        "liquidity_by_timeframe_json": (
            json.dumps(balances, ensure_ascii=False, sort_keys=True)
            if balances else None
        ),
    }


def _is_aligned(direction: str, values: list[tuple[str, Optional[float]]]) -> bool:
    return bool(direction in {"LONG", "SHORT"} and all(d == direction for d, score in values if score is not None) and all(score is not None for _, score in values))


def merge_snapshot_rows(
    existing: Mapping[str, Any], row: Mapping[str, Any]
) -> Dict[str, Any]:
    """Pure deterministic merge; callers own complete ordered child sets."""
    priorities = {
        "COMBINED_CONFIRMATION": 5,
        "STRONG_MAX_PAIN_CONFIRMATION": 4,
        "MAX_PAIN_CONFIRMATION": 3,
        "MAX_PAIN_ALERT": 2,
        "MAGNET_ALERT": 2,
    }
    old_primary = str(existing.get("primary_alert_type") or "")
    new_primary = str(row.get("primary_alert_type") or "")
    new_is_primary = (
        not existing
        or priorities.get(new_primary, 1) >= priorities.get(old_primary, 0)
    )
    merged = dict(existing)
    primary_fields = {
        "displayed_direction", "parent_event_id", "reference_price",
        "btc_parent_movement_id",
        "timestamp_utc", "timestamp_israel", "target_price",
        "target_distance_pct", "consensus_hits", "consensus_total",
        "maxpain_selected_timeframe", "maxpain_selected_score",
        "maxpain_opposite_score", "maxpain_score_edge", "maxpain_score_ratio",
        "maxpain_direction_average", "maxpain_opposite_average",
        "maxpain_average_edge", "maxpain_average_ratio",
        "liquidity_balance_pct", "selected_liquidity_usd", "opposite_liquidity_usd",
        "liquidity_long_pct", "liquidity_short_pct", "liquidity_timeframe",
        "liquidity_data_source", "liquidity_by_timeframe_json",
    }
    for name, value in row.items():
        # Keep timeframe, prices and liquidity bound to the chosen primary
        # event. Even missing values from a new primary replace the old source;
        # they must not inherit another event's measurements.
        if name in primary_fields:
            if new_is_primary:
                merged[name] = value
            continue
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
    ) + int(row.get("telegram_event_count") or 0)
    merged["primary_alert_type"] = new_primary if new_is_primary else old_primary
    return merged


def _merged_snapshot(row: Mapping[str, Any]) -> Dict[str, Any]:
    """Merge live child alerts in the legacy in-memory delivery path."""
    key = str(row.get("snapshot_id") or "")
    if not key:
        return dict(row)
    with _SNAPSHOT_LOCK:
        merged = merge_snapshot_rows(_SNAPSHOT_CACHE.get(key) or {}, row)
        _SNAPSHOT_CACHE[key] = dict(merged)
        _SNAPSHOT_CACHE.move_to_end(key)
        while len(_SNAPSHOT_CACHE) > 5000:
            _SNAPSHOT_CACHE.popitem(last=False)
        return merged


def build_neutral_snapshot_payload(
    event: Any,
    *,
    decision_feature_bundle: Mapping[str, Any],
    anchor_slot_id: Any = None,
    feature_bundle_policy_version: Any = None,
    feature_bundle_sha256: Any = None,
) -> Dict[str, Any]:
    """Mirror one persisted silent anchor to Sheets without fabricating an alert.

    The canonical formula-visible feature bundle remains in PostgreSQL.  This
    Sheet row is a compact audit/index view; alert-only scores intentionally
    remain empty when the prospective model-score wrapper is absent.
    """
    data = event.to_dict() if hasattr(event, "to_dict") else dict(event)
    bundle = _mapping(decision_feature_bundle)
    timestamp = str(data.get("alert_time_utc") or "")
    try:
        israel_time = datetime.fromisoformat(
            timestamp.replace("Z", "+00:00")
        ).astimezone(ZoneInfo("Asia/Jerusalem")).strftime("%d/%m/%Y %H:%M")
    except Exception:
        israel_time = timestamp
    direction = _direction(data.get("direction"))
    fingerprint = str(data.get("event_fingerprint") or "")
    sampler_version = str(bundle.get("sampler_version") or data.get("strategy_version") or "")
    per_direction = _mapping(_mapping(bundle.get("features_by_direction")).get(direction))
    market_session = per_direction.get("time.market_session")
    is_weekend = per_direction.get("time.is_market_weekend")
    model_score_status = str(bundle.get("model_score_status") or "ABSENT").upper()
    provenance = ":".join(
        value for value in (
            str(feature_bundle_policy_version or bundle.get("feature_bundle_policy_version") or ""),
            str(feature_bundle_sha256 or bundle.get("feature_bundle_sha256") or "")[:16],
        ) if value
    )
    snapshot_row = {
        "snapshot_id": fingerprint,
        "timestamp_utc": timestamp,
        "timestamp_israel": israel_time,
        "watch_scan_id": f"prospective-anchor:{anchor_slot_id or timestamp}",
        "parent_event_id": fingerprint,
        "btc_parent_movement_id": data.get("btc_parent_movement_id"),
        "symbol": data.get("symbol"),
        "direction": direction,
        "displayed_direction": direction,
        "analysis_direction": direction,
        "no_alert_snapshot": True,
        "reference_price": data.get("current_price"),
        "data_quality_status": "PROSPECTIVE_EVALUABLE",
        "alert_sent": False,
        "alert_types": "",
        "primary_alert_type": "PROSPECTIVE_NEUTRAL_30M",
        "telegram_event_count": 0,
        "market_session": market_session,
        "is_weekend": is_weekend,
        "strategy_version": sampler_version,
        "code_version": provenance or data.get("code_version"),
        "snapshot_written_at": timestamp,
    }
    # Explicitly document why alert-derived aggregate-score columns are blank.
    if model_score_status != "ABSENT":
        snapshot_row["data_quality_status"] = (
            "PROSPECTIVE_EVALUABLE_MODEL_SCORES_PRESENT"
        )
    live_row = {
        "זמן סריקה": snapshot_row.get("timestamp_israel"),
        "מטבע": data.get("symbol"),
        "כיוון נבדק": direction,
        "כיוון מוצג": direction,
        "כיוון ניתוח": direction,
        "מחיר ייחוס": snapshot_row.get("reference_price"),
        "נשלחה התראה": "לא",
        "סוג התראה": "PROSPECTIVE_NEUTRAL_30M",
        "שלישייה 65+": "לא נמדד",
        "סטטוס נתונים": snapshot_row["data_quality_status"],
        "snapshot_id": fingerprint,
    }
    return {"kind": "neutral_snapshot", "upserts": [
        {"sheet": "תצוגת לייב", "key": "snapshot_id", "row": live_row},
        {"sheet": "Snapshots", "key": "snapshot_id", "row": snapshot_row},
    ]}


def enqueue_neutral_snapshot(event: Any, **kwargs: Any) -> bool:
    if not enabled() or _DURABLE_SNAPSHOT_MODE:
        return False
    return enqueue(build_neutral_snapshot_payload(event, **kwargs))


def build_delivered_event_payload(
    event: Any,
    *,
    delivered_at_utc: Any = None,
    snapshot_merger: Optional[Callable[[Mapping[str, Any]], Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Build a Sheet payload from frozen source fields without network/cache I/O."""
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
    carries_maxpain = "MAX_PAIN" in event_type or event_type == "COMBINED_CONFIRMATION"
    if not carries_maxpain:
        max_selected = max_opposite = selected_avg = opposite_avg = None
        score_edge = average_edge = None
    snapshot_row = {
        "snapshot_id": sheet_snapshot_id,
        "timestamp_utc": timestamp,
        "timestamp_israel": israel_time,
        "watch_scan_id": snapshot.get("watch_scan_id"),
        "parent_event_id": fingerprint,
        "btc_parent_movement_id": data.get("btc_parent_movement_id", snapshot.get("btc_parent_movement_id")),
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
        "maxpain_selected_timeframe": data.get("timeframe") if carries_maxpain else None,
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
        **_liquidity_fields(data, snapshot),
        "strategy_version": data.get("strategy_version"),
        "code_version": data.get("code_version"),
        "snapshot_written_at": delivered_at_utc or timestamp,
    }
    if snapshot_merger is not None:
        snapshot_row = snapshot_merger(snapshot_row)
    live_row = {
        "זמן סריקה": snapshot_row.get("timestamp_israel"),
        "מטבע": data.get("symbol"),
        "כיוון נבדק": direction,
        "כיוון מוצג": snapshot_row.get("displayed_direction"),
        "כיוון ניתוח": snapshot_row.get("analysis_direction"),
        "מחיר ייחוס": snapshot_row.get("reference_price"),
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
    return {"kind": "alert", "upserts": [
        {"sheet": "תצוגת לייב", "key": "snapshot_id", "row": live_row},
        {"sheet": "Snapshots", "key": "snapshot_id", "row": snapshot_row},
        {"sheet": "Telegram_Events", "key": "event_id", "row": telegram_row},
    ]}


def enqueue_delivered_event(event: Any, *, delivered_at_utc: Any = None) -> bool:
    if not enabled() or _DURABLE_SNAPSHOT_MODE:
        return False
    return enqueue(build_delivered_event_payload(
        event, delivered_at_utc=delivered_at_utc, snapshot_merger=_merged_snapshot
    ))


def use_durable_snapshots() -> None:
    """Select committed-source replay instead of partial in-memory snapshots."""
    global _DURABLE_SNAPSHOT_MODE
    with _DELIVERY_LOCK:
        _DURABLE_SNAPSHOT_MODE = True


def enqueue_first_touch_outcome(*, event: Mapping[str, Any], horizon: int, reference_price: float, reference_source: str, path_result: Mapping[str, Any], first_touch: Mapping[str, Any], quality: str) -> bool:
    """Mirror the legacy one-sided v6 label without inventing adverse touches.

    A v6 ``MISS`` means only that the favorable width was not reached before
    expiry.  It is not evidence that an equal adverse barrier was touched.
    The ordered two-barrier workbook contract is emitted separately by
    :func:`deliver_ordered_first_touch_outcomes`.
    """
    if not enabled():
        return False
    event_id = _positive_event_id(event)
    threshold = _float(first_touch.get("qualifying_move_threshold_pct"))
    if threshold is None or threshold <= 0:
        raise ValueError("First Touch requires a positive threshold")
    threshold_bps = int(round(threshold * 100.0))
    method_version = str(first_touch.get("method_version") or "").strip()
    outcome_id = _outcome_id(
        event_id=event_id,
        window_minutes=int(horizon),
        threshold_bps=threshold_bps,
        method_version=method_version,
    )
    raw_status = str(first_touch.get("status") or "").upper()
    decision_time = (
        first_touch.get("first_qualifying_move_time_utc")
        if raw_status == "HIT"
        else first_touch.get("observed_through_utc")
        if raw_status == "MISS"
        else None
    )
    extrema = _path_extrema(
        reference_price=reference_price,
        direction=first_touch.get("direction") or event.get("direction"),
        path_result=path_result,
    )
    row = {
        "event_id": event_id,
        "snapshot_id": (
            (_mapping(event.get("engine_snapshot"))).get("sheet_snapshot_id")
            or event.get("event_fingerprint")
            or event_id
        ),
        "symbol": event.get("symbol"),
        "direction": _direction(first_touch.get("direction") or event.get("direction")),
        "threshold_pct": threshold,
        "measurement_start_utc": event.get("alert_time_utc"),
        "status": {
            "HIT": "SUCCESS",
            "MISS": "UNRESOLVED",
            "PENDING": "OPEN",
        }.get(raw_status, first_touch.get("status")),
        "first_touch_side": "FAVORABLE" if raw_status == "HIT" else "NONE",
        "decision_time_utc": decision_time,
        "minutes_to_decision": _minutes_between(
            event.get("alert_time_utc"), decision_time
        ),
        "mfe_pct": extrema["mfe_pct"],
        "mae_pct": extrema["mae_pct"],
        "favorable_touch_price": (
            first_touch.get("qualifying_move_price")
            if raw_status == "HIT"
            else None
        ),
        "adverse_touch_price": None,
        "max_favorable_price": extrema["max_favorable_price"],
        "max_adverse_price": extrema["max_adverse_price"],
        "market_source": reference_source,
        "market_pair": path_result.get("pair"),
        "candle_interval": "1m",
        "candle_count": len(path_result.get("candles") or []),
        "data_quality_status": quality,
        "outcome_method_version": method_version,
        "outcome_id": outcome_id,
        "window_minutes": int(horizon),
        "threshold_bps": threshold_bps,
    }
    return enqueue({"kind": "outcome", "upserts": [{
        "sheet": "Outcomes",
        "key": "outcome_id",
        "row": row,
    }]})


def ordered_outcome_row(
    *,
    event: Mapping[str, Any],
    reference_source: str,
    path_result: Mapping[str, Any],
    outcome: Mapping[str, Any],
    quality: str,
) -> Dict[str, Any]:
    """Return one complete row for the workbook's ordered barrier contract."""
    event_id = _positive_event_id(event)
    snapshot_id = str(
        (_mapping(event.get("engine_snapshot"))).get("sheet_snapshot_id")
        or event.get("event_fingerprint")
        or event_id
    )
    direction = _direction(outcome.get("direction") or event.get("direction"))
    threshold_bps = int(outcome["threshold_bps"])
    window_minutes = int(outcome["window_minutes"])
    method_version = str(outcome.get("method_version") or "")
    normalized_quality = str(quality or "").strip()
    if not normalized_quality:
        raise ValueError("ordered First Touch requires data quality")
    outcome_id = _outcome_id(
        event_id=event_id,
        window_minutes=window_minutes,
        threshold_bps=threshold_bps,
        method_version=method_version,
    )
    decision_time = (
        outcome.get("decision_time_utc")
        if outcome.get("decision_time_utc") not in (None, "")
        else outcome.get("outcome_decided_at_utc")
    )
    observed_from = (
        outcome.get("observed_from_utc")
        if outcome.get("observed_from_utc") not in (None, "")
        else outcome.get("first_observed_open_utc")
    )
    return {
        "event_id": event_id,
        "snapshot_id": snapshot_id,
        "symbol": event.get("symbol"),
        "direction": direction,
        "threshold_pct": (
            outcome.get("threshold_pct")
            if outcome.get("threshold_pct") is not None
            else threshold_bps / 100.0
        ),
        "measurement_start_utc": outcome.get("measurement_start_utc"),
        "status": outcome.get("status"),
        "first_touch_side": outcome.get("first_touch_side"),
        "decision_time_utc": decision_time,
        "minutes_to_decision": (
            _float(outcome.get("time_to_decision_seconds")) / 60.0
            if outcome.get("time_to_decision_seconds") is not None
            else None
        ),
        "mfe_pct": outcome.get("mfe_pct"),
        "mae_pct": outcome.get("mae_pct"),
        "favorable_touch_price": outcome.get("favorable_touch_price"),
        "adverse_touch_price": outcome.get("adverse_touch_price"),
        "max_favorable_price": outcome.get("max_favorable_price"),
        "max_adverse_price": outcome.get("max_adverse_price"),
        "market_source": reference_source,
        "market_pair": path_result.get("pair"),
        "candle_interval": "1m",
        "candle_count": outcome.get("path_samples"),
        "data_quality_status": normalized_quality,
        "outcome_method_version": method_version,
        "outcome_id": outcome_id,
        "window_minutes": window_minutes,
        "threshold_bps": threshold_bps,
        "observed_from_utc": observed_from,
        "observed_through_utc": outcome.get("observed_through_utc"),
        "terminal_reason": outcome.get("terminal_reason"),
        "initial_gap_seconds": outcome.get("initial_gap_seconds"),
        "favorable_barrier_price": outcome.get("favorable_barrier_price"),
        "adverse_barrier_price": outcome.get("adverse_barrier_price"),
        "initial_gap_unobserved": outcome.get("initial_gap_unobserved"),
        "data_quality_note": outcome.get("data_quality_note"),
        "path_complete": outcome.get("path_complete"),
    }


def deliver_ordered_first_touch_outcomes(
    *,
    event: Mapping[str, Any],
    reference_source: str,
    path_result: Mapping[str, Any],
    outcomes: list[Mapping[str, Any]],
    quality: str,
) -> bool:
    """Deliver a committed v7 outcome set with collision-safe identities."""
    if not enabled() or not outcomes:
        return False
    rows = [
        ordered_outcome_row(
            event=event,
            reference_source=reference_source,
            path_result=path_result,
            outcome=outcome,
            quality=quality,
        )
        for outcome in outcomes
    ]
    return deliver_now(
        {
            "kind": "ordered_first_touch_outcomes",
            "upserts": [
                {"sheet": "Outcomes", "key": "outcome_id", "row": row}
                for row in rows
            ],
        }
    )
