"""Historical Price + Open Interest backfill for Stage 77.

Purpose
-------
Collect a clean 30-minute historical series for Price and aggregated OI, then
measure the *normal historical movement* of each symbol separately for the
same analytical windows used by the live regime layer:

    30m / 1h / 4h / 12h / 24h

This module is intentionally isolated from alert_engine.py and from the live
Price+OI regime table. It does NOT change Max-Pain scores, alert selection,
Watch, or live OI conclusions. Its output is reference statistics only.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from bisect import bisect_left
from datetime import datetime, timedelta, timezone
import math
import hashlib
import pickle
import os
from pathlib import Path
import sqlite3
import time
import weakref
from collections import OrderedDict
from contextlib import contextmanager
from functools import wraps
from threading import Lock
from uuid import uuid4
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests

import market_session_baseline as session_baseline
import time_family_engine
import research_consumed_inputs as consumed_inputs

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None

API_BASE_URL = "https://open-api-v4.coinglass.com"
OI_HISTORY_ENDPOINT = "/api/futures/open-interest/aggregated-history"
PRICE_HISTORY_ENDPOINT = "/api/futures/price/history"
API_TIMEOUT_SECONDS = 20
BACKFILL_DAYS = 180
DAILY_REFRESH_DAYS = 3
MAX_BACKFILL_DAYS = 365
HISTORY_INTERVAL = "30m"
# 30 days = about 1,440 candles. Two 15-day chunks stay safely under the
# documented maximum of 1,000 records per request.
CHUNK_DAYS = 15
REQUEST_LIMIT = 1000
REQUEST_PAUSE_SECONDS = max(
    0.25, float(os.getenv("OI_BACKFILL_REQUEST_PAUSE_SECONDS", "0.5"))
)
MAX_REQUEST_ATTEMPTS = max(
    1, int(os.getenv("OI_BACKFILL_MAX_REQUEST_ATTEMPTS", "4"))
)
RETRY_BACKOFF_SECONDS: Tuple[float, ...] = (2.0, 5.0, 10.0)

TARGET_SYMBOLS: Tuple[str, ...] = (
    "BTC", "ETH", "SOL", "HYPE", "DOGE", "ZEC", "BNB", "XRP"
)

WINDOWS: Dict[str, int] = {
    "30m": 1,
    "1h": 2,
    "4h": 8,
    "12h": 24,
    "24h": 48,
    "48h": 96,
    "72h": 144,
    "7d": 336,
}

class _ReferenceRanges(dict):
    """Allow receipts to bind a generation without retaining its result."""
    __slots__ = ("__weakref__",)


_REFERENCE_CACHE: Dict[str, Dict[str, Any]] = {}
# Receipts bind the exact returned cache object, including concurrent builds.
# An evicted/missing receipt is a declared capture gap, never a fresh SQL read.
_REFERENCE_INPUT_RECEIPTS = OrderedDict()
_REFERENCE_INPUT_LOCK = Lock()
_REFERENCE_INPUT_MAX_BYTES = 16 * 1024 * 1024
_REFERENCE_INPUT_MAX_ENTRIES = 4


def _local_input_guard(result):
    # Process-local mutation guard only. Pickle is never loaded or persisted;
    # portable evidence identities continue to use canonical JSON SHA-256.
    return hashlib.sha256(pickle.dumps(result, protocol=5)).digest()


def _observe_inputs(kind, argument_names):
    def decorate(function):
        @wraps(function)
        def observed(*args, **kwargs):
            started = datetime.now(timezone.utc).isoformat()
            result = function(*args, **kwargs)
            arguments = {name: value for name, value in zip(argument_names, args)}
            arguments.update({name: kwargs[name] for name in argument_names if name in kwargs})
            if str(arguments.get("symbol") or "").upper() in {"ETH", "DOGE"}:
                try:
                    consumed_inputs.record(kind, {"arguments": arguments, "result": result,
                        "call_started_at_utc": started,
                        "call_finished_at_utc": datetime.now(timezone.utc).isoformat()})
                except BaseException:
                    pass
            return result
        return observed
    return decorate


_NEAREST_CAPTURE_VERSION = "oi-nearest-sql-candidates-v1"
_NEAREST_CAPTURE_MAX_BYTES = 8192
_NEAREST_CAPTURE_MAX_TEXT_BYTES = 256


def _nearest_operation_error_code(exc):
    if isinstance(exc, sqlite3.Error):
        return "SQLITE_ERROR"
    if psycopg is not None and isinstance(exc, psycopg.Error):
        return "POSTGRES_ERROR"
    if isinstance(exc, ValueError):
        return "VALUE_ERROR"
    if isinstance(exc, TypeError):
        return "TYPE_ERROR"
    return "OTHER_OPERATION_ERROR"


def _nearest_capture_gap():
    # Fixed codes only: never retain database exception messages or SQL text.
    try:
        consumed_inputs.record("capture_gap", {"source": "oi_historical_nearest_candidates",
                                               "reason": "NEAREST_CANDIDATE_CAPTURE_UNAVAILABLE"})
    except BaseException:
        pass
    try:
        collector = consumed_inputs.current()
        if collector is not None:
            collector.gap("NEAREST_CANDIDATE_CAPTURE_UNAVAILABLE")
    except BaseException:
        pass


def _nearest_capture_update(trace, action):
    if trace is None:
        return
    try:
        action()
    except BaseException as exc:
        bounded = type(exc) is ValueError and len(exc.args) == 1 and type(exc.args[0]) is str and exc.args[0] == "CAPTURE_BOUND"
        trace["capture_status"] = "TRUNCATED" if bounded else "UNAVAILABLE"
        trace["capture_reason_codes"] = ["CAPTURE_BOUND" if trace["capture_status"] == "TRUNCATED" else "CAPTURE_PROJECTION_UNAVAILABLE"]
        _nearest_capture_gap()


def _nearest_capture_values(row, keys):
    values, types = {}, {}
    for key in keys:
        value = row[key]
        types[key] = type(value).__name__
        if isinstance(value, datetime):
            value = value.isoformat() if value.tzinfo is None else value.astimezone(timezone.utc).isoformat()
        elif value is not None and not isinstance(value, (str, bool, int, float)):
            raise TypeError("UNSUPPORTED_CAPTURE_VALUE")
        if isinstance(value, str) and len(value.encode("utf-8")) > _NEAREST_CAPTURE_MAX_TEXT_BYTES:
            raise ValueError("CAPTURE_BOUND")
        if isinstance(value, int) and not -(2 ** 63) <= value < 2 ** 63:
            raise ValueError("CAPTURE_BOUND")
        if isinstance(value, float) and not math.isfinite(value):
            raise TypeError("NONFINITE_CAPTURE_VALUE")
        values[key] = value
    return values, types


@contextmanager
def _nearest_candidate_observation(symbol, target_time):
    trace = None
    try:
        if (isinstance(symbol, str) and symbol.strip().upper() in {"DOGE", "ETH"}
                and consumed_inputs.current() is not None):
            arguments, _ = _nearest_capture_values({"symbol": symbol.strip().upper(), "target_time": target_time}, ("symbol", "target_time"))
            trace = {"version": _NEAREST_CAPTURE_VERSION,
                     "query_contract": "oi-price-history-before-after-limit1-v1",
                     "arguments": arguments, "backend": "UNAVAILABLE",
                     "call_started_at_utc": datetime.now(timezone.utc).isoformat(),
                     "queries": {side: {"status": "NOT_EXECUTED"} for side in ("before", "after")},
                     "capture_status": "COMPLETE", "capture_reason_codes": [],
                     "selection": {"status": "NOT_COMPLETED"},
                     "scope": "SAME_CALL_TWO_STATEMENT_RETURNS_NOT_FULL_TABLE_OR_SINGLE_SNAPSHOT"}
    except BaseException:
        _nearest_capture_gap()
    try:
        yield trace
    except BaseException as exc:
        def failed():
            trace["operation_status"] = "ERROR"
            trace["operation_error_code"] = _nearest_operation_error_code(exc)
        _nearest_capture_update(trace, failed)
        raise
    finally:
        if trace is not None:
            try:
                trace["call_finished_at_utc"] = datetime.now(timezone.utc).isoformat()
                operation_error = trace.get("operation_status") == "ERROR"
                completed = sum(q.get("status") == "SUCCESS" for q in trace["queries"].values())
                trace["status"] = ("PARTIAL" if completed else "UNAVAILABLE") if operation_error else trace["capture_status"]
                if operation_error and trace["capture_status"] != "COMPLETE":
                    trace["status"] = trace["capture_status"]
                if trace["status"] != "COMPLETE":
                    _nearest_capture_gap()
                if len(consumed_inputs.canonical(trace).encode("utf-8")) > _NEAREST_CAPTURE_MAX_BYTES:
                    trace = {"version": _NEAREST_CAPTURE_VERSION, "status": "TRUNCATED",
                             "capture_reason_codes": ["CAPTURE_BYTE_BOUND"],
                             "arguments": trace["arguments"],
                             "call_started_at_utc": trace["call_started_at_utc"],
                             "call_finished_at_utc": trace["call_finished_at_utc"]}
                    _nearest_capture_gap()
                if len(consumed_inputs.canonical(trace).encode("utf-8")) > _NEAREST_CAPTURE_MAX_BYTES:
                    _nearest_capture_gap()
                elif consumed_inputs.record("oi_historical_nearest_candidates", trace) is None:
                    _nearest_capture_gap()
            except BaseException:
                _nearest_capture_gap()


def _nearest_candidate_query(trace, side, read):
    def started():
        trace["queries"][side] = {"status": "STARTED", "call_started_at_utc": datetime.now(timezone.utc).isoformat()}
    _nearest_capture_update(trace, started)
    try:
        row = read()
    except BaseException as exc:
        def failed():
            trace["queries"][side].update(status="ERROR", error_code=_nearest_operation_error_code(exc),
                                         call_finished_at_utc=datetime.now(timezone.utc).isoformat())
        _nearest_capture_update(trace, failed)
        raise
    def finished():
        query = trace["queries"][side]
        query.update(status="SUCCESS", call_finished_at_utc=datetime.now(timezone.utc).isoformat(),
                     row_found=row is not None)
        query["row"], query["sql_value_types"] = _nearest_capture_values(row, ("candle_time", "price_close", "oi_close_usd")) if row is not None else (None, None)
    _nearest_capture_update(trace, finished)
    return row


def _nearest_candidate_decision(trace, selected, result):
    def capture():
        row, _ = _nearest_capture_values(selected, ("candle_time", "price_close", "oi_close_usd")) if selected is not None else (None, None)
        side = next((name for name in ("before", "after") if row is not None and trace["queries"][name].get("row") == row), None)
        returned, types = _nearest_capture_values(result, ("collected_at", "price", "open_interest_usd", "source")) if result is not None else (None, None)
        trace["operation_status"] = "SUCCESS"
        trace["selection"] = {"status": "NO_CANDIDATE" if selected is None else "SELECTED",
                              "selected_side": side, "tie_rule": "BEFORE_FIRST_STABLE_MIN",
                              "returned_result": returned, "sql_value_types": types}
        if selected is not None and side is None:
            raise TypeError("SELECTED_SIDE_UNAVAILABLE")
    _nearest_capture_update(trace, capture)


@contextmanager
def _safe_input_collection():
    context = None
    try:
        context = consumed_inputs.collect()
        collector = context.__enter__()
    except Exception:
        context, collector = None, None
    try:
        yield collector
    finally:
        if context is not None:
            try:
                context.__exit__(None, None, None)
            except Exception:
                pass


def _remember_reference_inputs(result, package, metadata):
    try:
        size = sum(len(obj.get("compressed_b64", "")) for obj in package.get("objects", {}).values())
        if size > _REFERENCE_INPUT_MAX_BYTES:
            return
        with _REFERENCE_INPUT_LOCK:
            for key, receipt in list(_REFERENCE_INPUT_RECEIPTS.items()):
                if receipt[0]() is None:
                    del _REFERENCE_INPUT_RECEIPTS[key]
            _REFERENCE_INPUT_RECEIPTS[id(result)] = (weakref.ref(result), package,
                {**metadata, "result_sha256": consumed_inputs.digest(result)}, size,
                _local_input_guard(result))
            while (len(_REFERENCE_INPUT_RECEIPTS) > _REFERENCE_INPUT_MAX_ENTRIES or
                   sum(item[3] for item in _REFERENCE_INPUT_RECEIPTS.values()) > _REFERENCE_INPUT_MAX_BYTES):
                _REFERENCE_INPUT_RECEIPTS.popitem(last=False)
    except Exception:
        pass


def _use_reference_inputs(symbol, result, cache_hit):
    if consumed_inputs.current() is None:
        return
    try:
        with _REFERENCE_INPUT_LOCK:
            receipt = _REFERENCE_INPUT_RECEIPTS.get(id(result))
            if receipt is not None and receipt[0]() is result:
                _REFERENCE_INPUT_RECEIPTS.move_to_end(id(result))
            else:
                receipt = None
        if receipt is not None and _local_input_guard(result) != receipt[4]:
            receipt = None
        if receipt is None:
            consumed_inputs.record("capture_gap", {"symbol": symbol,
                "source": "oi_baseline_cache", "reason": "MISSING_EXACT_GENERATION_RECEIPT"})
        else:
            consumed_inputs.merge(receipt[1], {**receipt[2], "cache_hit": cache_hit,
                "used_at_utc": datetime.now(timezone.utc).isoformat()})
    except Exception:
        pass


# Price history is exchange/pair based. For most assets Binance is the first
# choice. HYPE starts with Hyperliquid. Fallbacks exist only for historical
# collection and never touch the live price provider.
PRICE_MARKET_CANDIDATES: Dict[str, Sequence[Tuple[str, str]]] = {
    "HYPE": (
        ("Hyperliquid", "HYPEUSDT"),
        ("Hyperliquid", "HYPEUSD"),
        ("Binance", "HYPEUSDT"),
        ("Bybit", "HYPEUSDT"),
        ("Gate", "HYPEUSDT"),
    ),
}
DEFAULT_EXCHANGES: Tuple[str, ...] = ("Binance", "Bybit", "OKX", "Gate")

DATABASE_URL = os.getenv("DATABASE_URL", "")
_SCHEMA_INITIALIZED_FOR = None
_SCHEMA_ADVISORY_LOCK_ID = 94837211
DB_PATH = os.getenv("DB_PATH", "coinglass.db")

SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS oi_price_history (
    symbol TEXT NOT NULL,
    candle_time TEXT NOT NULL,
    price_close REAL NOT NULL,
    oi_close_usd REAL NOT NULL,
    price_exchange TEXT NOT NULL,
    price_pair TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'coinglass_backfill',
    imported_at TEXT NOT NULL,
    PRIMARY KEY (symbol, candle_time)
);
CREATE INDEX IF NOT EXISTS idx_oi_price_history_symbol_time
ON oi_price_history(symbol, candle_time);
CREATE TABLE IF NOT EXISTS oi_backfill_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    completed_at TEXT NOT NULL,
    source TEXT NOT NULL,
    ok_count INTEGER NOT NULL,
    total_count INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_oi_backfill_runs_completed
ON oi_backfill_runs(completed_at);
"""

POSTGRES_SCHEMA = """
CREATE TABLE IF NOT EXISTS oi_price_history (
    symbol TEXT NOT NULL,
    candle_time TIMESTAMPTZ NOT NULL,
    price_close DOUBLE PRECISION NOT NULL,
    oi_close_usd DOUBLE PRECISION NOT NULL,
    price_exchange TEXT NOT NULL,
    price_pair TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'coinglass_backfill',
    imported_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (symbol, candle_time)
);
CREATE INDEX IF NOT EXISTS idx_oi_price_history_symbol_time
ON oi_price_history(symbol, candle_time);
CREATE TABLE IF NOT EXISTS oi_backfill_runs (
    id BIGSERIAL PRIMARY KEY,
    completed_at TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    ok_count INTEGER NOT NULL,
    total_count INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_oi_backfill_runs_completed
ON oi_backfill_runs(completed_at);
"""


@dataclass(frozen=True)
class BackfillResult:
    symbol: str
    price_rows: int
    oi_rows: int
    matched_rows: int
    inserted_rows: int
    price_exchange: Optional[str]
    price_pair: Optional[str]
    start_time: Optional[str]
    end_time: Optional[str]
    ok: bool
    message: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _use_postgres() -> bool:
    return bool(DATABASE_URL and psycopg)


def _api_key() -> str:
    return os.getenv("COINGLASS_API_KEY", "").strip()


def init_db() -> None:
    global _SCHEMA_INITIALIZED_FOR
    schema_key = ("postgres", DATABASE_URL) if _use_postgres() else ("sqlite", DB_PATH)
    if _SCHEMA_INITIALIZED_FOR == schema_key:
        return
    if _use_postgres():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            conn.execute("SELECT pg_advisory_xact_lock(%s)", (_SCHEMA_ADVISORY_LOCK_ID,))
            rows = conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='public' AND table_name = ANY(%s)",
                (["oi_price_history", "oi_backfill_runs"],),
            ).fetchall()
            existing_tables = {str(row["table_name"]) for row in rows}
            if existing_tables != {"oi_price_history", "oi_backfill_runs"}:
                conn.execute(POSTGRES_SCHEMA)
            conn.commit()
    else:
        Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(DB_PATH) as conn:
            conn.executescript(SQLITE_SCHEMA)
            conn.commit()
    _SCHEMA_INITIALIZED_FOR = schema_key


def _request(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    key = _api_key()
    if not key:
        raise RuntimeError("COINGLASS_API_KEY is not configured")
    last_error: Optional[Exception] = None
    for attempt in range(1, MAX_REQUEST_ATTEMPTS + 1):
        response = None
        permanent_error = False
        try:
            response = requests.get(
                API_BASE_URL + path,
                params=params,
                headers={"CG-API-KEY": key, "accept": "application/json"},
                timeout=API_TIMEOUT_SECONDS,
            )
            permanent_error = response.status_code in {401, 403}
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or str(payload.get("code")) not in {"0", "200"}:
                msg = payload.get("msg") if isinstance(payload, dict) else "invalid response"
                permanent_error = isinstance(payload, dict) and (
                    str(payload.get("code")) in {"401", "403"}
                    or msg == "The requested pair does not exist on the exchange."
                )
                raise RuntimeError(f"CoinGlass API error: {msg!r}")
            return payload
        except Exception as exc:
            last_error = exc
            if permanent_error or attempt >= MAX_REQUEST_ATTEMPTS:
                break
            retry_after = 0.0
            try:
                retry_after = float(response.headers.get("Retry-After") or 0.0)
            except Exception:
                retry_after = 0.0
            fallback = RETRY_BACKOFF_SECONDS[
                min(attempt - 1, len(RETRY_BACKOFF_SECONDS) - 1)
            ]
            delay = max(retry_after, fallback)
            print(
                f"[oi-backfill] request retry {attempt}/{MAX_REQUEST_ATTEMPTS} "
                f"in {delay:.1f}s: {type(exc).__name__}: {exc}",
                flush=True,
            )
            time.sleep(delay)
    raise RuntimeError(
        f"CoinGlass request failed after {attempt} attempts: {last_error!r}"
    )


def _chunks(start: datetime, end: datetime) -> Iterable[Tuple[datetime, datetime]]:
    cursor = start
    while cursor < end:
        chunk_end = min(end, cursor + timedelta(days=CHUNK_DAYS))
        yield cursor, chunk_end
        cursor = chunk_end


def _normalize_candles(payload: Dict[str, Any]) -> Dict[int, float]:
    out: Dict[int, float] = {}
    for row in payload.get("data") or []:
        if not isinstance(row, dict):
            continue
        try:
            timestamp = int(row.get("time"))
            close = float(row.get("close"))
        except (TypeError, ValueError):
            continue
        if timestamp > 0 and math.isfinite(close) and close > 0:
            out[timestamp] = close
    return out


def fetch_oi_history(symbol: str, start: datetime, end: datetime) -> Dict[int, float]:
    symbol = str(symbol or "").upper()
    rows: Dict[int, float] = {}
    for chunk_start, chunk_end in _chunks(start, end):
        payload = _request(
            OI_HISTORY_ENDPOINT,
            {
                "symbol": symbol,
                "interval": HISTORY_INTERVAL,
                "unit": "usd",
                "limit": REQUEST_LIMIT,
                "start_time": int(chunk_start.timestamp() * 1000),
                "end_time": int(chunk_end.timestamp() * 1000),
            },
        )
        rows.update(_normalize_candles(payload))
        time.sleep(REQUEST_PAUSE_SECONDS)
    return rows


def _price_candidates(symbol: str) -> Sequence[Tuple[str, str]]:
    symbol = str(symbol or "").upper()
    if symbol in PRICE_MARKET_CANDIDATES:
        return PRICE_MARKET_CANDIDATES[symbol]
    pair = f"{symbol}USDT"
    return tuple((exchange, pair) for exchange in DEFAULT_EXCHANGES)


def _fetch_price_for_market(
    exchange: str,
    pair: str,
    start: datetime,
    end: datetime,
) -> Dict[int, float]:
    rows: Dict[int, float] = {}
    for chunk_start, chunk_end in _chunks(start, end):
        payload = _request(
            PRICE_HISTORY_ENDPOINT,
            {
                "exchange": exchange,
                "symbol": pair,
                "interval": HISTORY_INTERVAL,
                "limit": REQUEST_LIMIT,
                "start_time": int(chunk_start.timestamp() * 1000),
                "end_time": int(chunk_end.timestamp() * 1000),
            },
        )
        rows.update(_normalize_candles(payload))
        time.sleep(REQUEST_PAUSE_SECONDS)
    return rows


def fetch_price_history(
    symbol: str,
    start: datetime,
    end: datetime,
) -> Tuple[Dict[int, float], str, str]:
    errors: List[str] = []
    for exchange, pair in _price_candidates(symbol):
        try:
            rows = _fetch_price_for_market(exchange, pair, start, end)
            # A 30-day sample should be large. Requiring 100 candles prevents
            # accidentally selecting an unsupported/empty market response.
            if len(rows) >= 100:
                return rows, exchange, pair
            errors.append(f"{exchange}:{pair} returned {len(rows)} rows")
        except Exception as exc:
            errors.append(f"{exchange}:{pair}: {exc!r}")
    raise RuntimeError("No usable CoinGlass price history market. " + " | ".join(errors))


def _iso_from_ms(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc).isoformat()


def _store_rows(
    symbol: str,
    matched: Sequence[Tuple[int, float, float]],
    exchange: str,
    pair: str,
) -> int:
    init_db()
    imported_at_dt = datetime.now(timezone.utc)
    imported_at = imported_at_dt if _use_postgres() else imported_at_dt.isoformat()
    values = [
        (
            str(symbol).upper(),
            datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc) if _use_postgres() else _iso_from_ms(ts),
            float(price),
            float(oi),
            exchange,
            pair,
            "coinglass_backfill",
            imported_at,
        )
        for ts, price, oi in matched
    ]
    if not values:
        return 0

    if _use_postgres():
        sql = """
        INSERT INTO oi_price_history
        (symbol,candle_time,price_close,oi_close_usd,price_exchange,price_pair,source,imported_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (symbol,candle_time) DO UPDATE SET
          price_close=EXCLUDED.price_close,
          oi_close_usd=EXCLUDED.oi_close_usd,
          price_exchange=EXCLUDED.price_exchange,
          price_pair=EXCLUDED.price_pair,
          source=EXCLUDED.source,
          imported_at=EXCLUDED.imported_at
        """
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, values)
            conn.commit()
    else:
        sql = """
        INSERT INTO oi_price_history
        (symbol,candle_time,price_close,oi_close_usd,price_exchange,price_pair,source,imported_at)
        VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol,candle_time) DO UPDATE SET
          price_close=excluded.price_close,
          oi_close_usd=excluded.oi_close_usd,
          price_exchange=excluded.price_exchange,
          price_pair=excluded.price_pair,
          source=excluded.source,
          imported_at=excluded.imported_at
        """
        with sqlite3.connect(DB_PATH) as conn:
            conn.executemany(sql, values)
            conn.commit()
    _REFERENCE_CACHE.pop(str(symbol).upper(), None)
    return len(values)


def record_backfill_run(source: str, ok_count: int, total_count: int, completed_at: Optional[datetime] = None) -> None:
    """Persist the completion time so restarts do not trigger redundant downloads."""
    init_db()
    completed_at = completed_at or datetime.now(timezone.utc)
    if _use_postgres():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            conn.execute(
                "INSERT INTO oi_backfill_runs (completed_at,source,ok_count,total_count) VALUES (%s,%s,%s,%s)",
                (completed_at, str(source), int(ok_count), int(total_count)),
            )
            conn.commit()
    else:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT INTO oi_backfill_runs (completed_at,source,ok_count,total_count) VALUES (?,?,?,?)",
                (completed_at.isoformat(), str(source), int(ok_count), int(total_count)),
            )
            conn.commit()


def last_backfill_run() -> Optional[Dict[str, Any]]:
    """Return the latest completed automatic or manual backfill, if one exists."""
    init_db()
    if _use_postgres():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            row = conn.execute(
                "SELECT completed_at,source,ok_count,total_count FROM oi_backfill_runs ORDER BY completed_at DESC LIMIT 1"
            ).fetchone()
    else:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT completed_at,source,ok_count,total_count FROM oi_backfill_runs ORDER BY completed_at DESC LIMIT 1"
            ).fetchone()
    return dict(row) if row else None


def backfill_symbol(symbol: str, days: int = BACKFILL_DAYS) -> Dict[str, Any]:
    symbol = str(symbol or "").strip().upper()
    if symbol not in TARGET_SYMBOLS:
        return BackfillResult(
            symbol, 0, 0, 0, 0, None, None, None, None, False,
            f"Symbol is not in Stage 77 historical set: {', '.join(TARGET_SYMBOLS)}",
        ).to_dict()

    days = max(1, min(int(days), MAX_BACKFILL_DAYS))
    end = datetime.now(timezone.utc)
    # Round down to the latest completed 30-minute boundary, avoiding a
    # partially formed current candle.
    minute = 30 if end.minute >= 30 else 0
    end = end.replace(minute=minute, second=0, microsecond=0)
    start = end - timedelta(days=max(1, int(days)))

    try:
        oi_rows = fetch_oi_history(symbol, start, end)
        price_rows, exchange, pair = fetch_price_history(symbol, start, end)
        common = sorted(set(oi_rows).intersection(price_rows))
        matched = [(ts, price_rows[ts], oi_rows[ts]) for ts in common]
        inserted = _store_rows(symbol, matched, exchange, pair)
        return BackfillResult(
            symbol=symbol,
            price_rows=len(price_rows),
            oi_rows=len(oi_rows),
            matched_rows=len(matched),
            inserted_rows=inserted,
            price_exchange=exchange,
            price_pair=pair,
            start_time=start.isoformat(),
            end_time=end.isoformat(),
            ok=len(matched) >= 100,
            message="OK" if len(matched) >= 100 else "Too few matched 30m candles",
        ).to_dict()
    except Exception as exc:
        return BackfillResult(
            symbol, 0, 0, 0, 0, None, None, start.isoformat(), end.isoformat(), False, repr(exc)
        ).to_dict()


def backfill_all(days: int = BACKFILL_DAYS) -> Dict[str, Dict[str, Any]]:
    return {symbol: backfill_symbol(symbol, days=days) for symbol in TARGET_SYMBOLS}


@_observe_inputs("oi_baseline_rows", ("symbol",))
def _history_rows(symbol: str) -> List[Dict[str, Any]]:
    init_db()
    symbol = str(symbol or "").upper()
    if _use_postgres():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            rows = conn.execute(
                "SELECT candle_time,price_close,oi_close_usd FROM oi_price_history "
                "WHERE symbol=%s ORDER BY candle_time ASC",
                (symbol,),
            ).fetchall()
    else:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT candle_time,price_close,oi_close_usd FROM oi_price_history "
                "WHERE symbol=? ORDER BY candle_time ASC",
                (symbol,),
            ).fetchall()
    return [dict(row) for row in rows]


def _pct_change(new: float, old: float) -> Optional[float]:
    if old is None or float(old) == 0.0:
        return None
    return (float(new) - float(old)) / float(old) * 100.0


def _percentile(values: Sequence[float], percentile: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(float(v) for v in values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * float(percentile)
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _distribution(values: Sequence[float]) -> Dict[str, Optional[float]]:
    values = [abs(float(v)) for v in values if v is not None and math.isfinite(float(v))]
    return {
        "count": len(values),
        "p25": _percentile(values, 0.25),
        "median": _percentile(values, 0.50),
        "p75": _percentile(values, 0.75),
        "p90": _percentile(values, 0.90),
        "p95": _percentile(values, 0.95),
    }


def _weighted_distribution(values_and_weights: Sequence[Tuple[float, float]]) -> Dict[str, Optional[float]]:
    cleaned = [
        (abs(float(value)), float(weight))
        for value, weight in values_and_weights
        if value is not None and math.isfinite(float(value)) and math.isfinite(float(weight)) and float(weight) > 0
    ]
    effective_samples = sum(weight for _, weight in cleaned)
    return {
        "count": len(cleaned),
        "effective_samples": effective_samples,
        "p25": session_baseline.weighted_percentile(cleaned, 0.25),
        "median": session_baseline.weighted_percentile(cleaned, 0.50),
        "p75": session_baseline.weighted_percentile(cleaned, 0.75),
        "p90": session_baseline.weighted_percentile(cleaned, 0.90),
        "p95": session_baseline.weighted_percentile(cleaned, 0.95),
    }


def _composition_distribution(
    samples: Sequence[Tuple[float, float]],
    current_active_ratio: float,
    global_dist: Dict[str, Any],
    min_effective_samples: float = 30.0,
) -> Dict[str, Any]:
    weighted = session_baseline.composition_weighted_values(samples, current_active_ratio)
    matched = _weighted_distribution(weighted)
    if float(matched.get("effective_samples") or 0.0) < min_effective_samples:
        out = dict(global_dist or {})
        out.update({
            "baseline_mode": "GLOBAL_FALLBACK",
            "active_ratio": current_active_ratio,
            "weekend_ratio": 1.0 - current_active_ratio,
            "matched_effective_samples": float(matched.get("effective_samples") or 0.0),
        })
        return out
    matched.update({
        "baseline_mode": "SESSION_COMPOSITION_MATCHED",
        "active_ratio": current_active_ratio,
        "weekend_ratio": 1.0 - current_active_ratio,
        "matched_effective_samples": float(matched.get("effective_samples") or 0.0),
    })
    return matched


@_observe_inputs("oi_historical_before", ("symbol", "target_time"))
def historical_point_at_or_before(symbol: str, target_time: datetime) -> Optional[Dict[str, Any]]:
    """Return the newest backfilled 30m candle at or before ``target_time``.

    This is a read-only bridge for the live regime layer. It lets 4h/12h/24h
    windows use the already downloaded CoinGlass history immediately after a
    deploy, instead of waiting many hours for live snapshots to accumulate.
    The historical table remains separate and is never written by this call.
    """
    init_db()
    symbol = str(symbol or "").strip().upper()
    if target_time.tzinfo is None:
        target_time = target_time.replace(tzinfo=timezone.utc)
    else:
        target_time = target_time.astimezone(timezone.utc)

    if _use_postgres():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            row = conn.execute(
                "SELECT candle_time,price_close,oi_close_usd FROM oi_price_history "
                "WHERE symbol=%s AND candle_time<=%s ORDER BY candle_time DESC LIMIT 1",
                (symbol, target_time),
            ).fetchone()
    else:
        target_iso = target_time.isoformat()
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT candle_time,price_close,oi_close_usd FROM oi_price_history "
                "WHERE symbol=? AND candle_time<=? ORDER BY candle_time DESC LIMIT 1",
                (symbol, target_iso),
            ).fetchone()

    if not row:
        return None
    data = dict(row)
    return {
        "collected_at": data["candle_time"],
        "price": float(data["price_close"]),
        "open_interest_usd": float(data["oi_close_usd"]),
        "source": "historical_backfill",
    }


@_observe_inputs("oi_historical_nearest", ("symbol", "target_time"))
def historical_point_nearest(symbol: str, target_time: datetime) -> Optional[Dict[str, Any]]:
    """Return the backfilled 30m candle nearest to ``target_time``.

    The caller is responsible for enforcing its tolerance. Looking on both
    sides of the requested time avoids systematically selecting the previous
    candle when the next closed candle is actually closer.
    """
    with _nearest_candidate_observation(symbol, target_time) as capture:
        init_db()
        symbol = str(symbol or "").strip().upper()
        if target_time.tzinfo is None:
            target_time = target_time.replace(tzinfo=timezone.utc)
        else:
            target_time = target_time.astimezone(timezone.utc)
        _nearest_capture_update(capture, lambda: capture.update(arguments=
            _nearest_capture_values({"symbol": symbol, "target_time": target_time}, ("symbol", "target_time"))[0]))

        before = None
        after = None
        if _use_postgres():
            _nearest_capture_update(capture, lambda: capture.update(backend="postgresql"))
            with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                before = _nearest_candidate_query(capture, "before", lambda: conn.execute(
                    "SELECT candle_time,price_close,oi_close_usd FROM oi_price_history "
                    "WHERE symbol=%s AND candle_time<=%s ORDER BY candle_time DESC LIMIT 1",
                    (symbol, target_time),
                ).fetchone())
                after = _nearest_candidate_query(capture, "after", lambda: conn.execute(
                    "SELECT candle_time,price_close,oi_close_usd FROM oi_price_history "
                    "WHERE symbol=%s AND candle_time>=%s ORDER BY candle_time ASC LIMIT 1",
                    (symbol, target_time),
                ).fetchone())
        else:
            _nearest_capture_update(capture, lambda: capture.update(backend="sqlite"))
            target_iso = target_time.isoformat()
            with sqlite3.connect(DB_PATH) as conn:
                conn.row_factory = sqlite3.Row
                before = _nearest_candidate_query(capture, "before", lambda: conn.execute(
                    "SELECT candle_time,price_close,oi_close_usd FROM oi_price_history "
                    "WHERE symbol=? AND candle_time<=? ORDER BY candle_time DESC LIMIT 1",
                    (symbol, target_iso),
                ).fetchone())
                after = _nearest_candidate_query(capture, "after", lambda: conn.execute(
                    "SELECT candle_time,price_close,oi_close_usd FROM oi_price_history "
                    "WHERE symbol=? AND candle_time>=? ORDER BY candle_time ASC LIMIT 1",
                    (symbol, target_iso),
                ).fetchone())

        candidates = [dict(row) for row in (before, after) if row]
        if not candidates:
            _nearest_candidate_decision(capture, None, None)
            return None

        def as_utc(value):
            if isinstance(value, datetime):
                return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)

        data = min(candidates, key=lambda row: abs((as_utc(row["candle_time"]) - target_time).total_seconds()))
        result = {
            "collected_at": data["candle_time"],
            "price": float(data["price_close"]),
            "open_interest_usd": float(data["oi_close_usd"]),
            "source": "historical_backfill",
        }
        _nearest_candidate_decision(capture, data, result)
        return result

@_observe_inputs("oi_baseline_calculation", ("symbol",))
def calculate_reference_ranges(symbol: str) -> Dict[str, Any]:
    """Build composition-aware Price/OI historical reference samples.

    Each historical window is kept intact with its exact ACTIVE ratio. Live
    windows are later compared mainly with historical windows having a similar
    composition. No historical change is split between session buckets.
    """
    symbol = str(symbol or "").strip().upper()
    rows = _history_rows(symbol)
    if len(rows) < 2:
        return {"symbol": symbol, "available": False, "reason": "No backfill data", "windows": {}}

    normalized_rows = []
    for row in rows:
        item = dict(row)
        value = item.get("candle_time")
        if isinstance(value, datetime):
            ts = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
        else:
            ts = datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
        item["_time"] = ts
        normalized_rows.append(item)

    windows: Dict[str, Any] = {}
    for label, step in WINDOWS.items():
        price_changes: List[float] = []
        oi_changes: List[float] = []
        price_composition_samples: List[Tuple[float, float]] = []
        oi_composition_samples: List[Tuple[float, float]] = []
        times = [item["_time"] for item in normalized_rows]
        target_delta = timedelta(minutes=step * 30)
        for i in range(1, len(normalized_rows)):
            target_time = times[i] - target_delta
            pos = bisect_left(times, target_time, 0, i)
            candidates = []
            if pos < i:
                candidates.append(pos)
            if pos > 0:
                candidates.append(pos - 1)
            if not candidates:
                continue
            ref_idx = min(candidates, key=lambda idx: abs((times[idx] - target_time).total_seconds()))
            if abs((times[ref_idx] - target_time).total_seconds()) > 20 * 60:
                continue
            p = _pct_change(normalized_rows[i]["price_close"], normalized_rows[ref_idx]["price_close"])
            o = _pct_change(normalized_rows[i]["oi_close_usd"], normalized_rows[ref_idx]["oi_close_usd"])
            start_time = normalized_rows[ref_idx]["_time"]
            end_time = normalized_rows[i]["_time"]
            active_ratio, _, _ = session_baseline.session_ratios(start_time, end_time)
            if p is not None:
                magnitude = abs(p)
                price_changes.append(magnitude)
                price_composition_samples.append((magnitude, active_ratio))
            if o is not None:
                magnitude = abs(o)
                oi_changes.append(magnitude)
                oi_composition_samples.append((magnitude, active_ratio))
        price_global = _distribution(price_changes)
        oi_global = _distribution(oi_changes)
        windows[label] = {
            "samples": min(len(price_changes), len(oi_changes)),
            "price_abs_change_pct": {**price_global, "global": price_global},
            "oi_abs_change_pct": {**oi_global, "global": oi_global},
            "price_composition_samples": price_composition_samples,
            "oi_composition_samples": oi_composition_samples,
        }

    return {
        "symbol": symbol,
        "available": True,
        "rows": len(normalized_rows),
        "windows": windows,
        "note": "Session-composition matched historical reference for live Price+OI significance only; Max-Pain remains independent.",
    }

def get_reference_ranges(symbol: str, refresh: bool = False) -> Dict[str, Any]:
    """Cached historical reference for one symbol.

    Backfill invalidates the cache automatically, so repeated Alerts/Watch calls
    do not reread ~1,440 history rows for every opportunity.
    """
    symbol = str(symbol or "").strip().upper()
    if not refresh and symbol in _REFERENCE_CACHE:
        result = _REFERENCE_CACHE[symbol]
        if symbol in {"ETH", "DOGE"}:
            _use_reference_inputs(symbol, result, True)
        return result
    if symbol in {"ETH", "DOGE"}:
        started = datetime.now(timezone.utc).isoformat()
        with _safe_input_collection() as collector:
            result = _ReferenceRanges(calculate_reference_ranges(symbol))
        try:
            if collector is not None:
                package = collector.package()
                metadata = {"source": "oi_baseline_cache", "symbol": symbol,
                    "generation_id": str(uuid4()), "computed_started_at_utc": started,
                    "computed_finished_at_utc": datetime.now(timezone.utc).isoformat()}
                _remember_reference_inputs(result, package, metadata)
        except Exception:
            pass
    else:
        result = calculate_reference_ranges(symbol)
    _REFERENCE_CACHE[symbol] = result
    if symbol in {"ETH", "DOGE"}:
        _use_reference_inputs(symbol, result, False)
    return result


def strength_from_distribution(change_pct: Optional[float], distribution: Dict[str, Any]) -> Dict[str, Any]:
    """Map an absolute move to labels plus active continuous strength."""
    if change_pct is None or not distribution:
        return {"available": False, "label": "Unknown", "rank": None}
    value = abs(float(change_pct))
    p25 = distribution.get("p25")
    median = distribution.get("median")
    p75 = distribution.get("p75")
    p90 = distribution.get("p90")
    p95 = distribution.get("p95")
    if any(x is None for x in (p25, median, p75, p90, p95)):
        return {"available": False, "label": "Unknown", "rank": None}
    if value < float(p25):
        label, rank = "Weak / Noise", 0
    elif value < float(p75):
        label, rank = "Normal", 1
    elif value < float(p90):
        label, rank = "Elevated", 2
    elif value < float(p95):
        label, rank = "Strong", 3
    else:
        label, rank = "Extreme", 4
    continuous_strength = time_family_engine.continuous_percentile_strength(
        value,
        distribution,
    )
    return {
        "available": True,
        "label": label,
        "rank": rank,
        "continuous_strength": continuous_strength,
        "absolute_change_pct": value,
        "p25": float(p25),
        "median": float(median),
        "p75": float(p75),
        "p90": float(p90),
        "p95": float(p95),
    }


@_observe_inputs("oi_window_reference", ("symbol", "label", "window_start", "window_end"))
def reference_for_window(
    symbol: str,
    label: str,
    window_start: Optional[datetime] = None,
    window_end: Optional[datetime] = None,
) -> Dict[str, Any]:
    stats = get_reference_ranges(symbol)
    if not stats.get("available"):
        return {}
    raw = dict((stats.get("windows") or {}).get(label) or {})
    if not raw:
        return {}
    price_sets = raw.get("price_abs_change_pct") or {}
    oi_sets = raw.get("oi_abs_change_pct") or {}
    price_global = dict(price_sets.get("global") or price_sets)
    oi_global = dict(oi_sets.get("global") or oi_sets)
    if window_start is None or window_end is None:
        return {
            "samples": raw.get("samples"),
            "price_abs_change_pct": price_global,
            "oi_abs_change_pct": oi_global,
            "baseline_mode": "GLOBAL",
        }
    active_ratio, weekend_ratio, segments = session_baseline.session_ratios(window_start, window_end)
    price_distribution = _composition_distribution(
        raw.get("price_composition_samples") or [], active_ratio, price_global
    )
    oi_distribution = _composition_distribution(
        raw.get("oi_composition_samples") or [], active_ratio, oi_global
    )
    mode = "SESSION_COMPOSITION_MATCHED"
    if price_distribution.get("baseline_mode") == "GLOBAL_FALLBACK" or oi_distribution.get("baseline_mode") == "GLOBAL_FALLBACK":
        mode = "PARTIAL_GLOBAL_FALLBACK"
    return {
        "samples": raw.get("samples"),
        "price_abs_change_pct": price_distribution,
        "oi_abs_change_pct": oi_distribution,
        "baseline_mode": mode,
        "active_ratio": active_ratio,
        "weekend_ratio": weekend_ratio,
        "segments": segments,
    }

def calculate_all_reference_ranges() -> Dict[str, Dict[str, Any]]:
    return {symbol: get_reference_ranges(symbol, refresh=True) for symbol in TARGET_SYMBOLS}

