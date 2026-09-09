"""Read-only source audit, refreshed by a background worker and cached for health.

The daily 30m Price/OI backfill and the live Price/OI collector have different
freshness contracts. Neither is a continuous one-minute OHLC archive. This
module schedules no work and fetches no prices. ``status()`` performs no I/O.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import os
from threading import Lock
from typing import Any, Mapping, Sequence

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # Allows status and deterministic checks without Postgres.
    psycopg = None
    dict_row = None

SYMBOLS = ("BTC", "ETH", "SOL", "HYPE", "DOGE", "ZEC", "BNB", "XRP")
VERSION = "source-freshness-separate-cadences-v1"
CACHE_MAX_AGE_SECONDS = 300
SOURCE_SQL = """
SELECT CURRENT_TIMESTAMP AS checked_at, target.symbol,
       live.collected_at, live.price_fetched_at, live.oi_fetched_at,
       live.price_source, live.oi_source, live.data_quality_status,
       archive.candle_time, archive.imported_at
FROM unnest(%s::text[]) AS target(symbol)
LEFT JOIN LATERAL (
  SELECT collected_at, price_fetched_at, oi_fetched_at, price_source,
         oi_source, data_quality_status
  FROM oi_regime_snapshots WHERE symbol=target.symbol
  ORDER BY collected_at DESC LIMIT 1
) live ON TRUE
LEFT JOIN LATERAL (
  SELECT candle_time, imported_at FROM oi_price_history
  WHERE symbol=target.symbol ORDER BY candle_time DESC LIMIT 1
) archive ON TRUE
ORDER BY target.symbol
"""
RUNS_SQL = """
SELECT completed_at, source, ok_count, total_count FROM oi_backfill_runs
ORDER BY completed_at DESC LIMIT 32
"""
_cache_lock = Lock()
_cache: dict[str, Any] = {"version": VERSION, "audit_status": "NOT_RUN", "checked_at": None}


def _utc(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return result.replace(tzinfo=timezone.utc) if result.tzinfo is None else result.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def _stamp(value: Any) -> str | None:
    parsed = _utc(value)
    return parsed.isoformat() if parsed is not None else None


def _age_minutes(now: datetime, value: Any) -> float | None:
    parsed = _utc(value)
    return round((now - parsed).total_seconds() / 60, 2) if parsed is not None else None


def _freshness(now: datetime, value: Any, max_age_minutes: int) -> str:
    parsed = _utc(value)
    if parsed is None:
        return "SOURCE_TIME_UNKNOWN"
    age = (now - parsed).total_seconds()
    if age < -60:
        return "SOURCE_TIME_IN_FUTURE"
    return "FRESH" if age <= max_age_minutes * 60 else "STALE"


def build_report(*, now: datetime, rows: Sequence[Mapping], runs: Sequence[Mapping],
                 history_interval_hours: int = 24, history_check_minutes: int = 60,
                 live_interval_minutes: int = 30, live_grace_minutes: int = 10) -> dict:
    """Classify persisted evidence without equating daily candles with live data."""
    now = _utc(now)
    if now is None:
        raise ValueError("A valid observation time is required")
    history_interval_hours = max(1, int(history_interval_hours))
    history_check_minutes = max(5, int(history_check_minutes))
    max_age = max(1, int(live_interval_minutes)) + max(0, int(live_grace_minutes))
    sources = {str(row["symbol"]): row for row in rows}
    live, historical = {}, {}
    for symbol in SYMBOLS:
        row = sources.get(symbol, {})
        price_state = _freshness(now, row.get("price_fetched_at"), max_age)
        oi_state = _freshness(now, row.get("oi_fetched_at"), max_age)
        quality = str(row.get("data_quality_status") or "UNKNOWN").upper()
        states = {price_state, oi_state}
        if not row.get("collected_at"):
            state = "MISSING"
        elif "SOURCE_TIME_IN_FUTURE" in states:
            state = "SOURCE_TIME_IN_FUTURE"
        elif "STALE" in states:
            state = "STALE"
        elif "SOURCE_TIME_UNKNOWN" in states:
            state = "SOURCE_TIME_UNKNOWN"
        elif quality != "PASS":
            state = "QUALITY_NOT_PASS"
        else:
            state = "FRESH"
        live[symbol] = {
            "status": state, "price_freshness": price_state, "oi_freshness": oi_state,
            "collected_at": _stamp(row.get("collected_at")),
            "price_fetched_at": _stamp(row.get("price_fetched_at")),
            "oi_fetched_at": _stamp(row.get("oi_fetched_at")),
            "price_age_minutes": _age_minutes(now, row.get("price_fetched_at")),
            "oi_age_minutes": _age_minutes(now, row.get("oi_fetched_at")),
            "data_quality_status": quality,
            "price_source": row.get("price_source"), "oi_source": row.get("oi_source"),
        }
        historical[symbol] = {
            "latest_candle_time": _stamp(row.get("candle_time")),
            "latest_candle_imported_at": _stamp(row.get("imported_at")),
            "candle_age_minutes": _age_minutes(now, row.get("candle_time")),
        }
    complete = [row for row in runs[:32]
                if int(row.get("total_count") or 0) == len(SYMBOLS)
                and int(row.get("ok_count") or 0) == len(SYMBOLS)
                and _utc(row.get("completed_at")) is not None]
    last = max(complete, key=lambda row: _utc(row["completed_at"]), default=None)
    last_at = _utc(last["completed_at"]) if last else None
    due_at = last_at + timedelta(hours=history_interval_hours) if last_at else None
    if last_at is None:
        schedule_state = "NO_SUCCESS_IN_RECENT_32_RUNS"
    elif last_at > now + timedelta(minutes=1):
        schedule_state = "COMPLETION_TIME_IN_FUTURE"
    elif now < due_at:
        schedule_state = "NOT_DUE"
    elif now <= due_at + timedelta(minutes=history_check_minutes):
        schedule_state = "DUE_WITHIN_CHECK_INTERVAL"
    else:
        schedule_state = "OVERDUE"
    fresh_count = sum(row["status"] == "FRESH" for row in live.values())
    return {
        "version": VERSION, "audit_status": "OK", "checked_at": now.isoformat(),
        "target_count": len(SYMBOLS),
        "live_price_oi": {
            "table": "oi_regime_snapshots", "status": "FRESH" if fresh_count == len(SYMBOLS) else "ATTENTION",
            "fresh_count": fresh_count, "intended_interval_minutes": live_interval_minutes,
            "freshness_grace_minutes": live_grace_minutes, "symbols": live,
        },
        "historical_30m_backfill": {
            "table": "oi_price_history", "candle_interval_minutes": 30,
            "intended_refresh_hours": history_interval_hours,
            "check_interval_minutes": history_check_minutes,
            "schedule_status": schedule_state,
            "last_successful_completion_at": _stamp(last_at),
            "last_successful_source": last.get("source") if last else None,
            "next_due_at": _stamp(due_at), "symbols": historical,
        },
        "scope_note": "Endpoint freshness only; no one-minute archive continuity claim. Daily backfill candle age is not live Price/OI lag.",
    }


def run(*, database_url: str | None = None) -> dict:
    """Two bounded read-only queries; errors expose their type, never connection data."""
    checked_at = datetime.now(timezone.utc).isoformat()
    dsn = database_url if database_url is not None else os.getenv("DATABASE_URL", "")
    if not dsn or psycopg is None:
        return {"version": VERSION, "audit_status": "UNAVAILABLE", "checked_at": checked_at,
                "error_code": "DATABASE_NOT_CONFIGURED" if not dsn else "POSTGRES_DRIVER_UNAVAILABLE"}
    try:
        with psycopg.connect(dsn, row_factory=dict_row, connect_timeout=3,
                options="-c default_transaction_read_only=on -c statement_timeout=3000 -c lock_timeout=1000") as conn:
            rows = conn.execute(SOURCE_SQL, (list(SYMBOLS),)).fetchall()
            runs = conn.execute(RUNS_SQL).fetchall()
        return build_report(
            now=rows[0]["checked_at"], rows=rows, runs=runs,
            history_interval_hours=int(os.getenv("HISTORY_BACKFILL_INTERVAL_HOURS", "24")),
            history_check_minutes=int(os.getenv("HISTORY_BACKFILL_CHECK_INTERVAL_MINUTES", "60")),
        )
    except Exception as exc:
        return {"version": VERSION, "audit_status": "ERROR", "checked_at": checked_at,
                "error_code": type(exc).__name__}


def run_refresh(*, database_url: str | None = None) -> dict:
    """Call from an existing background pass, never from an HTTP health request."""
    global _cache
    report = run(database_url=database_url)
    with _cache_lock:
        last_success = _cache.get("last_successful_check_at")
        _cache = deepcopy(report)
        _cache["last_successful_check_at"] = report["checked_at"] if report["audit_status"] == "OK" else last_success
    return report


def status(*, now: datetime | None = None) -> dict:
    """Return a defensive copy of the cached audit with its observation age."""
    with _cache_lock:
        result = deepcopy(_cache)
    current = _utc(now) if now is not None else datetime.now(timezone.utc)
    checked = _utc(result.get("checked_at"))
    age = (current - checked).total_seconds() if current and checked else None
    result["cache_age_seconds"] = round(age, 1) if age is not None else None
    result["cache_stale"] = age is None or age > CACHE_MAX_AGE_SECONDS or age < -60
    result["cache_max_age_seconds"] = CACHE_MAX_AGE_SECONDS
    return result
