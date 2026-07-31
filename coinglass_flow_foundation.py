"""Stage 87 foundation: official CoinGlass aggregated CVD history.

This module stores CoinGlass 30-minute aggregated Futures and Spot order-flow
history for the configured symbols. Each saved candle includes:

- official aggregated taker buy volume from CoinGlass
- official aggregated taker sell volume from CoinGlass
- the official chunk-relative cumulative volume delta returned by CoinGlass
- a continuous CVD series stitched across API pagination chunks

It deliberately does NOT turn those values into Flow conclusions, confidence,
scores, alerts, or trade decisions. Later stages may analyse the saved history
without changing the existing trading engine.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import math
import os
from pathlib import Path
import sqlite3
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None

API_BASE_URL = "https://open-api-v4.coinglass.com"
FUTURES_ENDPOINT = "/api/futures/aggregated-cvd/history"
SPOT_ENDPOINT = "/api/spot/aggregated-cvd/history"
INTERVAL = "30m"
DEFAULT_BACKFILL_DAYS = 180
MAX_BACKFILL_DAYS = 365
# 90 days × 48 half-hour candles = 4,320, below CoinGlass max limit 4,500.
CHUNK_DAYS = 90
REQUEST_LIMIT = 4500
REQUEST_PAUSE_SECONDS = 0.15
API_TIMEOUT_SECONDS = 20
EXCHANGE_LIST = "Binance,OKX,Bybit"
TARGET_SYMBOLS: Tuple[str, ...] = ("BTC", "ETH", "SOL", "HYPE", "DOGE", "ZEC", "BNB", "XRP")

DATABASE_URL = os.getenv("DATABASE_URL", "")
DB_PATH = os.getenv("DB_PATH", "data/coinglass.db")

SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS futures_taker_history (
    symbol TEXT NOT NULL,
    candle_time TEXT NOT NULL,
    buy_volume_usd REAL NOT NULL,
    sell_volume_usd REAL NOT NULL,
    api_cum_vol_delta_usd REAL NOT NULL DEFAULT 0,
    continuous_cum_vol_delta_usd REAL NOT NULL DEFAULT 0,
    exchange_list TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'coinglass_futures_aggregated_cvd',
    imported_at TEXT NOT NULL,
    PRIMARY KEY (symbol, candle_time)
);
CREATE INDEX IF NOT EXISTS idx_futures_taker_symbol_time
ON futures_taker_history(symbol, candle_time);
CREATE TABLE IF NOT EXISTS spot_taker_history (
    symbol TEXT NOT NULL,
    candle_time TEXT NOT NULL,
    buy_volume_usd REAL NOT NULL,
    sell_volume_usd REAL NOT NULL,
    api_cum_vol_delta_usd REAL NOT NULL DEFAULT 0,
    continuous_cum_vol_delta_usd REAL NOT NULL DEFAULT 0,
    exchange_list TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'coinglass_spot_aggregated_cvd',
    imported_at TEXT NOT NULL,
    PRIMARY KEY (symbol, candle_time)
);
CREATE INDEX IF NOT EXISTS idx_spot_taker_symbol_time
ON spot_taker_history(symbol, candle_time);
"""

POSTGRES_SCHEMA = """
CREATE TABLE IF NOT EXISTS futures_taker_history (
    symbol TEXT NOT NULL,
    candle_time TIMESTAMPTZ NOT NULL,
    buy_volume_usd DOUBLE PRECISION NOT NULL,
    sell_volume_usd DOUBLE PRECISION NOT NULL,
    api_cum_vol_delta_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    continuous_cum_vol_delta_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    exchange_list TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'coinglass_futures_aggregated_cvd',
    imported_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (symbol, candle_time)
);
CREATE INDEX IF NOT EXISTS idx_futures_taker_symbol_time
ON futures_taker_history(symbol, candle_time);
CREATE TABLE IF NOT EXISTS spot_taker_history (
    symbol TEXT NOT NULL,
    candle_time TIMESTAMPTZ NOT NULL,
    buy_volume_usd DOUBLE PRECISION NOT NULL,
    sell_volume_usd DOUBLE PRECISION NOT NULL,
    api_cum_vol_delta_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    continuous_cum_vol_delta_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    exchange_list TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'coinglass_spot_aggregated_cvd',
    imported_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (symbol, candle_time)
);
CREATE INDEX IF NOT EXISTS idx_spot_taker_symbol_time
ON spot_taker_history(symbol, candle_time);
"""


@dataclass(frozen=True)
class FlowBackfillResult:
    symbol: str
    market: str
    received_rows: int
    stored_rows: int
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


def _sqlite_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _migrate_sqlite(conn: sqlite3.Connection) -> None:
    for table in ("futures_taker_history", "spot_taker_history"):
        columns = _sqlite_columns(conn, table)
        if "api_cum_vol_delta_usd" not in columns:
            conn.execute(
                f"ALTER TABLE {table} ADD COLUMN api_cum_vol_delta_usd REAL NOT NULL DEFAULT 0"
            )
        if "continuous_cum_vol_delta_usd" not in columns:
            conn.execute(
                f"ALTER TABLE {table} ADD COLUMN continuous_cum_vol_delta_usd REAL NOT NULL DEFAULT 0"
            )


def _migrate_postgres(conn) -> None:
    for table in ("futures_taker_history", "spot_taker_history"):
        conn.execute(
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
            "api_cum_vol_delta_usd DOUBLE PRECISION NOT NULL DEFAULT 0"
        )
        conn.execute(
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
            "continuous_cum_vol_delta_usd DOUBLE PRECISION NOT NULL DEFAULT 0"
        )


def init_db() -> None:
    if _use_postgres():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            conn.execute(POSTGRES_SCHEMA)
            _migrate_postgres(conn)
            conn.commit()
    else:
        Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(DB_PATH) as conn:
            conn.executescript(SQLITE_SCHEMA)
            _migrate_sqlite(conn)
            conn.commit()


def _request(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    key = _api_key()
    if not key:
        raise RuntimeError("COINGLASS_API_KEY is not configured")
    response = requests.get(
        API_BASE_URL + path,
        params=params,
        headers={"CG-API-KEY": key, "accept": "application/json"},
        timeout=API_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or str(payload.get("code")) not in {"0", "200"}:
        msg = payload.get("msg") if isinstance(payload, dict) else "invalid response"
        raise RuntimeError(f"CoinGlass API error: {msg!r}")
    return payload


def _chunks(start: datetime, end: datetime) -> Iterable[Tuple[datetime, datetime]]:
    cursor = start
    while cursor < end:
        chunk_end = min(end, cursor + timedelta(days=CHUNK_DAYS))
        yield cursor, chunk_end
        cursor = chunk_end


def _normalise_timestamp(raw: Any) -> int:
    ts = int(raw)
    # Some CoinGlass examples return seconds while most return milliseconds.
    return ts * 1000 if ts < 10_000_000_000 else ts


def _normalize(payload: Dict[str, Any]) -> Dict[int, Tuple[float, float, float]]:
    rows: Dict[int, Tuple[float, float, float]] = {}
    for row in payload.get("data") or []:
        if not isinstance(row, dict):
            continue
        try:
            ts = _normalise_timestamp(row.get("time"))
            buy = float(row.get("agg_taker_buy_vol"))
            sell = float(row.get("agg_taker_sell_vol"))
            api_cvd = float(row.get("cum_vol_delta"))
        except (TypeError, ValueError):
            continue
        if ts > 0 and all(math.isfinite(v) for v in (buy, sell, api_cvd)) and buy >= 0 and sell >= 0:
            rows[ts] = (buy, sell, api_cvd)
    return rows


def _stitch_chunks(
    chunks: List[Dict[int, Tuple[float, float, float]]]
) -> Dict[int, Tuple[float, float, float, float]]:
    """Keep CoinGlass CVD and also create one continuous series across chunks.

    CoinGlass calculates cum_vol_delta from each request's start_time. Because a
    long backfill needs multiple requests, the API value can restart at each
    chunk. The raw API value is saved untouched, while the continuous field is
    offset so the series does not reset at pagination boundaries.
    """
    output: Dict[int, Tuple[float, float, float, float]] = {}
    previous_continuous_close: Optional[float] = None
    for chunk in chunks:
        if not chunk:
            continue
        ordered = sorted(chunk.items())
        first_api = ordered[0][1][2]
        # Preserve CoinGlass values in the first chunk. At later pagination
        # boundaries, offset the new request so its first value continues from
        # the previous continuous close instead of resetting.
        chunk_offset = (
            0.0
            if previous_continuous_close is None
            else previous_continuous_close - first_api
        )
        for ts, (buy, sell, api_cvd) in ordered:
            continuous = api_cvd + chunk_offset
            output[ts] = (buy, sell, api_cvd, continuous)
        previous_continuous_close = output[ordered[-1][0]][3]
    return output


def fetch_history(
    symbol: str,
    market: str,
    start: datetime,
    end: datetime,
) -> Dict[int, Tuple[float, float, float, float]]:
    market = market.lower()
    if market not in {"futures", "spot"}:
        raise ValueError("market must be futures or spot")
    endpoint = FUTURES_ENDPOINT if market == "futures" else SPOT_ENDPOINT
    fetched_chunks: List[Dict[int, Tuple[float, float, float]]] = []
    for chunk_start, chunk_end in _chunks(start, end):
        payload = _request(endpoint, {
            "exchange_list": EXCHANGE_LIST,
            "symbol": str(symbol).upper(),
            "interval": INTERVAL,
            "limit": REQUEST_LIMIT,
            "start_time": int(chunk_start.timestamp() * 1000),
            "end_time": int(chunk_end.timestamp() * 1000),
            "unit": "usd",
        })
        fetched_chunks.append(_normalize(payload))
        time.sleep(REQUEST_PAUSE_SECONDS)
    return _stitch_chunks(fetched_chunks)


def _store(
    symbol: str,
    market: str,
    rows: Dict[int, Tuple[float, float, float, float]],
) -> int:
    init_db()
    table = "futures_taker_history" if market == "futures" else "spot_taker_history"
    source = f"coinglass_{market}_aggregated_cvd"
    now = datetime.now(timezone.utc)
    values = []
    for ts, (buy, sell, api_cvd, continuous_cvd) in sorted(rows.items()):
        candle = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
        values.append((
            str(symbol).upper(),
            candle if _use_postgres() else candle.isoformat(),
            float(buy),
            float(sell),
            float(api_cvd),
            float(continuous_cvd),
            EXCHANGE_LIST,
            source,
            now if _use_postgres() else now.isoformat(),
        ))
    if not values:
        return 0

    if _use_postgres():
        sql = f"""
        INSERT INTO {table}
        (symbol,candle_time,buy_volume_usd,sell_volume_usd,
         api_cum_vol_delta_usd,continuous_cum_vol_delta_usd,
         exchange_list,source,imported_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (symbol,candle_time) DO UPDATE SET
          buy_volume_usd=EXCLUDED.buy_volume_usd,
          sell_volume_usd=EXCLUDED.sell_volume_usd,
          api_cum_vol_delta_usd=EXCLUDED.api_cum_vol_delta_usd,
          continuous_cum_vol_delta_usd=EXCLUDED.continuous_cum_vol_delta_usd,
          exchange_list=EXCLUDED.exchange_list,
          source=EXCLUDED.source,
          imported_at=EXCLUDED.imported_at
        """
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, values)
            conn.commit()
    else:
        sql = f"""
        INSERT INTO {table}
        (symbol,candle_time,buy_volume_usd,sell_volume_usd,
         api_cum_vol_delta_usd,continuous_cum_vol_delta_usd,
         exchange_list,source,imported_at)
        VALUES (?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol,candle_time) DO UPDATE SET
          buy_volume_usd=excluded.buy_volume_usd,
          sell_volume_usd=excluded.sell_volume_usd,
          api_cum_vol_delta_usd=excluded.api_cum_vol_delta_usd,
          continuous_cum_vol_delta_usd=excluded.continuous_cum_vol_delta_usd,
          exchange_list=excluded.exchange_list,
          source=excluded.source,
          imported_at=excluded.imported_at
        """
        with sqlite3.connect(DB_PATH) as conn:
            conn.executemany(sql, values)
            conn.commit()
    return len(values)


def backfill_symbol(symbol: str, market: str, days: int = DEFAULT_BACKFILL_DAYS) -> Dict[str, Any]:
    symbol = str(symbol or "").upper()
    days = max(1, min(int(days), MAX_BACKFILL_DAYS))
    end = datetime.now(timezone.utc)
    minute = 30 if end.minute >= 30 else 0
    end = end.replace(minute=minute, second=0, microsecond=0)
    start = end - timedelta(days=days)
    try:
        rows = fetch_history(symbol, market, start, end)
        stored = _store(symbol, market, rows)
        return FlowBackfillResult(
            symbol,
            market,
            len(rows),
            stored,
            start.isoformat(),
            end.isoformat(),
            len(rows) >= 100,
            "OK" if len(rows) >= 100 else "Too few 30m rows",
        ).to_dict()
    except Exception as exc:
        return FlowBackfillResult(
            symbol, market, 0, 0, start.isoformat(), end.isoformat(), False, repr(exc)
        ).to_dict()


def backfill_all(days: int = DEFAULT_BACKFILL_DAYS) -> Dict[str, Dict[str, Dict[str, Any]]]:
    result: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for symbol in TARGET_SYMBOLS:
        result[symbol] = {
            "futures": backfill_symbol(symbol, "futures", days),
            "spot": backfill_symbol(symbol, "spot", days),
        }
    return result


def table_count(symbol: str, market: str) -> int:
    init_db()
    table = "futures_taker_history" if market == "futures" else "spot_taker_history"
    sql = f"SELECT COUNT(*) AS n FROM {table} WHERE symbol=?"
    if _use_postgres():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            row = conn.execute(sql.replace("?", "%s"), (str(symbol).upper(),)).fetchone()
    else:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(sql, (str(symbol).upper(),)).fetchone()
    return int(row["n"] if row else 0)
