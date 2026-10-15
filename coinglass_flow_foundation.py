"""Stage 87.2 foundation: stable CoinGlass CVD backfill downloader.

The module stores official CoinGlass 30-minute aggregated Futures and Spot
Buy/Sell volume plus the official API CVD. It is intentionally isolated from
all alert, Watch, Max-Pain and trade-decision logic.

Stage 87.2 adds:
- one-request-at-a-time queue
- safe spacing under the Startup-plan rate limit
- retry with exponential backoff for HTTP 429 / transient failures
- resume/skip behaviour so already-current markets are not downloaded again
- an explicit reverse-ordered frontfill path for extending history before the
  earliest stored candle without changing the live tail refresh semantics
- chunk-by-chunk commits, so successful work survives a later failure
- continuous CVD rebuilt deterministically from saved Buy-Sell deltas
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import math
import os
from pathlib import Path
import sqlite3
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

import market_session_baseline as session_baseline

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
CHUNK_DAYS = 90
REQUEST_LIMIT = 4500
# Startup allows up to 80 requests/minute. One second spacing keeps this
# downloader below that ceiling even before retries and leaves room for the
# separate live OI collector.
REQUEST_PAUSE_SECONDS = 1.0
API_TIMEOUT_SECONDS = 20
MAX_REQUEST_ATTEMPTS = 5
RETRY_BACKOFF_SECONDS: Tuple[int, ...] = (5, 10, 20, 40)
# A flow snapshot older than 30 minutes from the interpreted candle close is
# never eligible for confirmation. This is intentionally a hard ceiling.
MAX_CVD_AGE_MINUTES = 30
FRESHNESS_TOLERANCE_MINUTES = MAX_CVD_AGE_MINUTES
CANDLE_INTERVAL_MINUTES = 30
CANDLE_GRACE_MINUTES = 2
CVD_TIMESTAMP_MODE = os.getenv("COINGLASS_CVD_TIMESTAMP_MODE", "open").strip().lower()
if CVD_TIMESTAMP_MODE not in {"open", "close"}:
    CVD_TIMESTAMP_MODE = "open"
FLOW_COLLECTION_INTERVAL_MINUTES = max(5, int(os.getenv("FLOW_COLLECTION_INTERVAL_MINUTES", "5")))
EXCHANGE_LIST = "Binance,OKX,Bybit"
TARGET_SYMBOLS: Tuple[str, ...] = ("BTC", "ETH", "SOL", "HYPE", "DOGE", "ZEC", "BNB", "XRP")

DATABASE_URL = os.getenv("DATABASE_URL", "")
_SCHEMA_INITIALIZED_FOR = None
_SCHEMA_ADVISORY_LOCK_ID = 94837211
DB_PATH = os.getenv("DB_PATH", "coinglass.db")

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
    total_rows: int
    start_time: Optional[str]
    end_time: Optional[str]
    ok: bool
    skipped: bool
    attempts: int
    message: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _use_postgres() -> bool:
    return bool(DATABASE_URL and psycopg)


def _api_key() -> str:
    return os.getenv("COINGLASS_API_KEY", "").strip()


def _table_for_market(market: str) -> str:
    if market == "futures":
        return "futures_taker_history"
    if market == "spot":
        return "spot_taker_history"
    raise ValueError("market must be futures or spot")


def _series_advisory_lock_id(table: str, symbol: str) -> int:
    """Return a stable positive PostgreSQL bigint lock key per stored series."""
    identity = f"coinglass-flow:{table}:{str(symbol).upper()}".encode("utf-8")
    return int.from_bytes(hashlib.sha256(identity).digest()[:8], "big") & 0x7FFF_FFFF_FFFF_FFFF


def _acquire_series_write_lock(conn, table: str, symbol: str) -> None:
    """Serialize insert plus cumulative repair for one market/symbol series."""
    if _use_postgres():
        conn.execute(
            "SELECT pg_advisory_xact_lock(%s)",
            (_series_advisory_lock_id(table, symbol),),
        )
    else:
        # SQLite permits one writer. Acquiring it before the insert prevents a
        # concurrent tail refresh from interleaving with cumulative repair.
        conn.execute("BEGIN IMMEDIATE")


def _sqlite_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _migrate_sqlite(conn: sqlite3.Connection) -> None:
    for table in ("futures_taker_history", "spot_taker_history"):
        columns = _sqlite_columns(conn, table)
        if "api_cum_vol_delta_usd" not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN api_cum_vol_delta_usd REAL NOT NULL DEFAULT 0")
        if "continuous_cum_vol_delta_usd" not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN continuous_cum_vol_delta_usd REAL NOT NULL DEFAULT 0")


def _migrate_postgres(conn) -> None:
    for table in ("futures_taker_history", "spot_taker_history"):
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=%s",
            (table,),
        ).fetchall()
        columns = {str(row["column_name"]) for row in rows}
        if "api_cum_vol_delta_usd" not in columns:
            conn.execute(
                f"ALTER TABLE {table} ADD COLUMN "
                "api_cum_vol_delta_usd DOUBLE PRECISION NOT NULL DEFAULT 0"
            )
        if "continuous_cum_vol_delta_usd" not in columns:
            conn.execute(
                f"ALTER TABLE {table} ADD COLUMN "
                "continuous_cum_vol_delta_usd DOUBLE PRECISION NOT NULL DEFAULT 0"
            )


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
                (["futures_taker_history", "spot_taker_history"],),
            ).fetchall()
            existing_tables = {str(row["table_name"]) for row in rows}
            if existing_tables != {"futures_taker_history", "spot_taker_history"}:
                conn.execute(POSTGRES_SCHEMA)
            _migrate_postgres(conn)
            conn.commit()
    else:
        Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(DB_PATH) as conn:
            conn.executescript(SQLITE_SCHEMA)
            _migrate_sqlite(conn)
            conn.commit()
    _SCHEMA_INITIALIZED_FOR = schema_key


def _is_rate_limit_message(message: Any) -> bool:
    text = str(message or "").lower()
    return "too many requests" in text or "rate limit" in text or "429" in text


def _request(path: str, params: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
    """Perform one API call with bounded exponential backoff.

    Returns ``(payload, attempts_used)``. Only 429/rate-limit and transient 5xx
    failures are retried. Permanent 4xx failures are raised immediately.
    """
    key = _api_key()
    if not key:
        raise RuntimeError("COINGLASS_API_KEY is not configured")

    last_error: Optional[BaseException] = None
    for attempt in range(1, MAX_REQUEST_ATTEMPTS + 1):
        try:
            response = requests.get(
                API_BASE_URL + path,
                params=params,
                headers={"CG-API-KEY": key, "accept": "application/json"},
                timeout=API_TIMEOUT_SECONDS,
            )
            status = int(response.status_code)
            if status == 429 or 500 <= status <= 599:
                raise requests.HTTPError(f"HTTP {status}", response=response)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise RuntimeError("CoinGlass API returned an invalid response")
            if str(payload.get("code")) not in {"0", "200"}:
                msg = payload.get("msg")
                if _is_rate_limit_message(msg):
                    raise RuntimeError(f"CoinGlass API rate limit: {msg!r}")
                raise RuntimeError(f"CoinGlass API error: {msg!r}")
            return payload, attempt
        except Exception as exc:
            last_error = exc
            response = getattr(exc, "response", None)
            status = int(getattr(response, "status_code", 0) or 0)
            retryable = status == 429 or 500 <= status <= 599 or _is_rate_limit_message(exc)
            if not retryable or attempt >= MAX_REQUEST_ATTEMPTS:
                raise
            delay = RETRY_BACKOFF_SECONDS[min(attempt - 1, len(RETRY_BACKOFF_SECONDS) - 1)]
            print(
                f"[flow-backfill] retry {attempt}/{MAX_REQUEST_ATTEMPTS} "
                f"after {delay}s: {type(exc).__name__}: {exc}",
                flush=True,
            )
            time.sleep(delay)
    raise RuntimeError(f"CoinGlass request failed: {last_error!r}")


def _chunks(start: datetime, end: datetime) -> Iterable[Tuple[datetime, datetime]]:
    cursor = start
    while cursor < end:
        chunk_end = min(end, cursor + timedelta(days=CHUNK_DAYS))
        yield cursor, chunk_end
        cursor = chunk_end


def _reverse_chunks(start: datetime, end: datetime) -> Iterable[Tuple[datetime, datetime]]:
    """Yield contiguous chunks newest-first for resumable prefix extension.

    A frontfill advances its proven contiguous boundary backwards. Fetching
    the adjacent chunk first means a failed run can resume from the newly
    verified boundary without leaving an invisible hole in the requested
    prefix.
    """
    cursor = end
    while cursor > start:
        chunk_start = max(start, cursor - timedelta(days=CHUNK_DAYS))
        yield chunk_start, cursor
        cursor = chunk_start


def _half_open_rows(
    rows: Dict[int, Tuple[float, float, float, float]],
    start: datetime,
    end: datetime,
) -> Dict[int, Tuple[float, float, float, float]]:
    """Keep only provider candles in ``[start, end)``.

    CoinGlass endpoint boundary semantics are not relied upon here. In
    particular, a frontfill ending at the current ``MIN(candle_time)`` must
    not rewrite that existing boundary candle or any newer live-tail row.
    """
    start_ms = int(_as_utc(start).timestamp() * 1000)
    end_ms = int(_as_utc(end).timestamp() * 1000)
    return {ts: value for ts, value in rows.items() if start_ms <= int(ts) < end_ms}


class IncompleteHistoricalGridError(RuntimeError):
    """A provider response cannot safely advance the historical resume point."""


def _complete_half_open_rows(
    rows: Dict[int, Tuple[float, float, float, float]],
    start: datetime,
    end: datetime,
) -> Dict[int, Tuple[float, float, float, float]]:
    """Validate and return an exact 30-minute grid for ``[start, end)``.

    A whole chunk is rejected before any write when its first, last, or an
    interior candle is absent. Consequently the durable resume boundary moves
    only across a prefix whose continuity has already been proven.
    """
    start_time = _as_utc(start)
    end_time = _as_utc(end)
    if start_time is None or end_time is None or start_time >= end_time:
        raise ValueError("historical chunk must have a non-empty UTC range")
    step_ms = CANDLE_INTERVAL_MINUTES * 60 * 1000
    start_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)
    span_ms = end_ms - start_ms
    if span_ms % step_ms:
        raise ValueError("historical chunk boundaries must align to the 30-minute grid")

    accepted = {
        int(ts): value
        for ts, value in _half_open_rows(rows, start_time, end_time).items()
    }
    expected = set(range(start_ms, end_ms, step_ms))
    actual = set(accepted)
    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)
    if missing or unexpected:
        first_missing = (
            datetime.fromtimestamp(missing[0] / 1000.0, tz=timezone.utc).isoformat()
            if missing
            else None
        )
        first_unexpected = (
            datetime.fromtimestamp(unexpected[0] / 1000.0, tz=timezone.utc).isoformat()
            if unexpected
            else None
        )
        raise IncompleteHistoricalGridError(
            "incomplete 30m provider grid: "
            f"range=[{start_time.isoformat()},{end_time.isoformat()}) "
            f"expected={len(expected)} received={len(actual)} "
            f"missing={len(missing)} unexpected={len(unexpected)} "
            f"first_missing={first_missing} first_unexpected={first_unexpected}"
        )
    return accepted


def _normalise_timestamp(raw: Any) -> int:
    ts = int(raw)
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
    """Backward-compatible helper retained for tests and diagnostics."""
    output: Dict[int, Tuple[float, float, float, float]] = {}
    previous_continuous_close: Optional[float] = None
    for chunk in chunks:
        if not chunk:
            continue
        ordered = sorted(chunk.items())
        first_api = ordered[0][1][2]
        chunk_offset = 0.0 if previous_continuous_close is None else previous_continuous_close - first_api
        for ts, (buy, sell, api_cvd) in ordered:
            continuous = api_cvd + chunk_offset
            output[ts] = (buy, sell, api_cvd, continuous)
        previous_continuous_close = output[ordered[-1][0]][3]
    return output


def _fetch_chunk(
    symbol: str,
    market: str,
    start: datetime,
    end: datetime,
) -> Tuple[Dict[int, Tuple[float, float, float, float]], int]:
    endpoint = FUTURES_ENDPOINT if market == "futures" else SPOT_ENDPOINT
    payload, attempts = _request(endpoint, {
        "exchange_list": EXCHANGE_LIST,
        "symbol": str(symbol).upper(),
        "interval": INTERVAL,
        "limit": REQUEST_LIMIT,
        "start_time": int(start.timestamp() * 1000),
        "end_time": int(end.timestamp() * 1000),
        "unit": "usd",
    })
    normalized = _normalize(payload)
    # Continuous values are rebuilt from saved Buy-Sell deltas in the same
    # transaction as each chunk. The placeholder avoids trusting chunk-relative
    # API CVD as a cross-request continuous series.
    rows = {ts: (buy, sell, api_cvd, 0.0) for ts, (buy, sell, api_cvd) in normalized.items()}
    time.sleep(REQUEST_PAUSE_SECONDS)
    return rows, attempts


def fetch_history(
    symbol: str,
    market: str,
    start: datetime,
    end: datetime,
) -> Dict[int, Tuple[float, float, float, float]]:
    """Compatibility API: fetch all requested chunks without writing them."""
    market = market.lower()
    _table_for_market(market)
    all_rows: Dict[int, Tuple[float, float, float, float]] = {}
    for chunk_start, chunk_end in _chunks(start, end):
        rows, _ = _fetch_chunk(symbol, market, chunk_start, chunk_end)
        all_rows.update(rows)
    return all_rows


def _store(
    symbol: str,
    market: str,
    rows: Dict[int, Tuple[float, float, float, float]],
    *,
    preserve_existing: bool = False,
) -> int:
    init_db()
    table = _table_for_market(market)
    source = f"coinglass_{market}_aggregated_cvd"
    now = datetime.now(timezone.utc)
    values = []
    for ts, (buy, sell, api_cvd, continuous_cvd) in sorted(rows.items()):
        candle = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
        if not is_candle_closed(candle, now):
            continue
        values.append((
            str(symbol).upper(),
            candle if _use_postgres() else candle.isoformat(),
            float(buy), float(sell), float(api_cvd), float(continuous_cvd),
            EXCHANGE_LIST, source,
            now if _use_postgres() else now.isoformat(),
        ))
    if not values:
        return 0
    stored_count = len(values)

    if _use_postgres():
        conflict_action = (
            "DO NOTHING"
            if preserve_existing
            else """DO UPDATE SET
          buy_volume_usd=EXCLUDED.buy_volume_usd,
          sell_volume_usd=EXCLUDED.sell_volume_usd,
          api_cum_vol_delta_usd=EXCLUDED.api_cum_vol_delta_usd,
          continuous_cum_vol_delta_usd=EXCLUDED.continuous_cum_vol_delta_usd,
          exchange_list=EXCLUDED.exchange_list,
          source=EXCLUDED.source,
          imported_at=EXCLUDED.imported_at"""
        )
        sql = f"""
        INSERT INTO {table}
        (symbol,candle_time,buy_volume_usd,sell_volume_usd,
         api_cum_vol_delta_usd,continuous_cum_vol_delta_usd,
         exchange_list,source,imported_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (symbol,candle_time) {conflict_action}
        """
        for attempt in range(3):
            try:
                with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                    _acquire_series_write_lock(conn, table, str(symbol).upper())
                    with conn.cursor() as cur:
                        cur.executemany(sql, values)
                        if preserve_existing and cur.rowcount >= 0:
                            stored_count = int(cur.rowcount)
                    _repair_continuous_in_transaction(conn, table, str(symbol).upper())
                    conn.commit()
                break
            except psycopg.errors.DeadlockDetected:
                if attempt >= 2:
                    raise
                time.sleep(0.4 * (attempt + 1))
    else:
        conflict_action = (
            "DO NOTHING"
            if preserve_existing
            else """DO UPDATE SET
          buy_volume_usd=excluded.buy_volume_usd,
          sell_volume_usd=excluded.sell_volume_usd,
          api_cum_vol_delta_usd=excluded.api_cum_vol_delta_usd,
          continuous_cum_vol_delta_usd=excluded.continuous_cum_vol_delta_usd,
          exchange_list=excluded.exchange_list,
          source=excluded.source,
          imported_at=excluded.imported_at"""
        )
        sql = f"""
        INSERT INTO {table}
        (symbol,candle_time,buy_volume_usd,sell_volume_usd,
         api_cum_vol_delta_usd,continuous_cum_vol_delta_usd,
         exchange_list,source,imported_at)
        VALUES (?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol,candle_time) {conflict_action}
        """
        with sqlite3.connect(DB_PATH) as conn:
            _acquire_series_write_lock(conn, table, str(symbol).upper())
            cursor = conn.executemany(sql, values)
            if preserve_existing and cursor.rowcount >= 0:
                stored_count = int(cursor.rowcount)
            _repair_continuous_in_transaction(conn, table, str(symbol).upper())
            conn.commit()
    return stored_count


def _store_prefix(
    symbol: str,
    market: str,
    rows: Dict[int, Tuple[float, float, float, float]],
) -> int:
    """Insert historical prefix rows without replacing concurrent/live data."""
    return _store(symbol, market, rows, preserve_existing=True)


def _as_utc(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)


def candle_close_time(candle_time: datetime) -> datetime:
    """Return the market-data coverage end for one CoinGlass timestamp.

    CoinGlass has historically exposed the 30m row timestamp as the candle
    opening time. ``COINGLASS_CVD_TIMESTAMP_MODE=close`` is available only if a
    live probe proves that the endpoint returns closing timestamps instead.
    """
    candle = _as_utc(candle_time)
    if candle is None:
        raise ValueError("invalid candle time")
    if CVD_TIMESTAMP_MODE == "close":
        return candle
    return candle + timedelta(minutes=CANDLE_INTERVAL_MINUTES)


def candle_age_minutes(candle_time: Optional[datetime], now: Optional[datetime] = None) -> Optional[float]:
    if candle_time is None:
        return None
    current = _as_utc(now) if now is not None else datetime.now(timezone.utc)
    close_time = candle_close_time(candle_time)
    return max(0.0, (current - close_time).total_seconds() / 60.0)


def is_candle_closed(candle_time: datetime, now: Optional[datetime] = None) -> bool:
    current = _as_utc(now) if now is not None else datetime.now(timezone.utc)
    return candle_close_time(candle_time) + timedelta(minutes=CANDLE_GRACE_MINUTES) <= current


def freshness(symbol: str, market: str, now: Optional[datetime] = None) -> Dict[str, Any]:
    data = coverage(symbol, market)
    latest = data.get("max_time")
    current = _as_utc(now) if now is not None else datetime.now(timezone.utc)
    age = candle_age_minutes(latest, current)
    return {
        **data,
        "candle_close": candle_close_time(latest) if latest is not None else None,
        "age_minutes": age,
        "fresh": age is not None and age <= MAX_CVD_AGE_MINUTES,
        "max_age_minutes": MAX_CVD_AGE_MINUTES,
        "timestamp_mode": CVD_TIMESTAMP_MODE,
    }


def coverage(symbol: str, market: str) -> Dict[str, Any]:
    init_db()
    table = _table_for_market(market)
    symbol = str(symbol).upper()
    # Ignore any current/open candle when deciding freshness. The database
    # timestamp meaning is governed by CVD_TIMESTAMP_MODE.
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=CANDLE_GRACE_MINUTES)
    if CVD_TIMESTAMP_MODE == "open":
        cutoff -= timedelta(minutes=CANDLE_INTERVAL_MINUTES)
    if _use_postgres():
        sql = f"SELECT COUNT(*) AS n, MIN(candle_time) AS min_time, MAX(candle_time) AS max_time FROM {table} WHERE symbol=%s AND candle_time<=%s"
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            row = conn.execute(sql, (symbol, cutoff)).fetchone()
    else:
        sql = f"SELECT COUNT(*) AS n, MIN(candle_time) AS min_time, MAX(candle_time) AS max_time FROM {table} WHERE symbol=? AND candle_time<=?"
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(sql, (symbol, cutoff.isoformat())).fetchone()
    data = dict(row) if row else {"n": 0, "min_time": None, "max_time": None}
    return {
        "count": int(data.get("n") or 0),
        "min_time": _as_utc(data.get("min_time")),
        "max_time": _as_utc(data.get("max_time")),
    }


def _contiguous_stored_start(
    symbol: str,
    market: str,
    requested_start: datetime,
    *,
    existing: Optional[Dict[str, Any]] = None,
) -> Optional[datetime]:
    """Return the start of the exact 30m suffix ending at stored MAX.

    Unlike ``MIN(candle_time)``, this cursor cannot jump backwards over sparse
    rows left by an interrupted or older importer. Only a candle-by-candle
    contiguous suffix is trusted as the durable frontfill boundary.
    """
    init_db()
    table = _table_for_market(market)
    symbol = str(symbol).upper()
    start_time = _as_utc(requested_start)
    data = existing if existing is not None else coverage(symbol, market)
    latest = _as_utc(data.get("max_time"))
    if start_time is None or latest is None:
        return None
    if latest < start_time:
        return None

    if _use_postgres():
        sql = (
            f"SELECT candle_time FROM {table} "
            "WHERE symbol=%s AND candle_time>=%s AND candle_time<=%s"
        )
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            rows = conn.execute(sql, (symbol, start_time, latest)).fetchall()
        stored = {_as_utc(row["candle_time"]) for row in rows}
    else:
        sql = (
            f"SELECT candle_time FROM {table} "
            "WHERE symbol=? AND candle_time>=? AND candle_time<=?"
        )
        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute(
                sql, (symbol, start_time.isoformat(), latest.isoformat())
            ).fetchall()
        stored = {_as_utc(row[0]) for row in rows}

    cursor = latest
    step = timedelta(minutes=CANDLE_INTERVAL_MINUTES)
    while cursor >= start_time and cursor in stored:
        cursor -= step
    return cursor + step

def latest_eligible_candle_time(now: Optional[datetime] = None) -> datetime:
    """Return the newest CoinGlass 30m timestamp that is safe to store.

    The endpoint timestamps rows by candle *open* by default. A row becomes
    eligible only after its close plus the configured grace period. This
    helper deliberately derives eligibility from real wall-clock time rather
    than from a rounded request boundary; otherwise a database that is one
    full candle behind can be mistaken for current.
    """
    current = _as_utc(now) if now is not None else datetime.now(timezone.utc)
    safe_time = current - timedelta(minutes=CANDLE_GRACE_MINUTES)
    close_boundary = safe_time.replace(
        minute=30 if safe_time.minute >= 30 else 0,
        second=0,
        microsecond=0,
    )
    if CVD_TIMESTAMP_MODE == "close":
        return close_boundary
    return close_boundary - timedelta(minutes=CANDLE_INTERVAL_MINUTES)


def _is_current(existing: Dict[str, Any], now: Optional[datetime] = None) -> bool:
    """Whether the DB already contains the newest closed, grace-cleared row."""
    latest = _as_utc(existing.get("max_time"))
    if int(existing.get("count") or 0) < 100 or latest is None:
        return False
    return latest >= latest_eligible_candle_time(now)


def _repair_continuous_in_transaction(conn, table: str, symbol: str) -> int:
    """Publish raw candles and their cumulative series in the same transaction.

    Update only changed values in one database statement; readers cannot see
    placeholders and a failed repair rolls back the corresponding raw insert.
    """
    placeholder = "%s" if _use_postgres() else "?"
    conn.execute(f"""
        WITH computed AS (
            SELECT candle_time,
                   SUM(buy_volume_usd - sell_volume_usd) OVER (
                       ORDER BY candle_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) AS cvd
            FROM {table} WHERE symbol={placeholder}
        )
        UPDATE {table} AS target
        SET continuous_cum_vol_delta_usd=computed.cvd
        FROM computed
        WHERE target.symbol={placeholder}
          AND target.candle_time=computed.candle_time
          AND target.continuous_cum_vol_delta_usd <> computed.cvd
    """, (symbol, symbol))
    row = conn.execute(
        f"SELECT COUNT(*) AS count FROM {table} WHERE symbol={placeholder}",
        (symbol,),
    ).fetchone()
    return int(row["count"] if _use_postgres() else row[0])


def _rebuild_continuous_cvd(symbol: str, market: str) -> int:
    """Repair interrupted legacy writes, without rewriting unchanged history."""
    init_db()
    table = _table_for_market(market)
    symbol = str(symbol).upper()
    if _use_postgres():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            _acquire_series_write_lock(conn, table, symbol)
            total = _repair_continuous_in_transaction(conn, table, symbol)
            conn.commit()
            return total
    with sqlite3.connect(DB_PATH) as conn:
        _acquire_series_write_lock(conn, table, symbol)
        total = _repair_continuous_in_transaction(conn, table, symbol)
        conn.commit()
        return total


def backfill_symbol(
    symbol: str,
    market: str,
    days: int = DEFAULT_BACKFILL_DAYS,
    force: bool = False,
) -> Dict[str, Any]:
    symbol = str(symbol or "").upper()
    market = market.lower()
    _table_for_market(market)
    days = max(1, min(int(days), MAX_BACKFILL_DAYS))
    now = datetime.now(timezone.utc)
    end = now.replace(
        minute=30 if now.minute >= 30 else 0,
        second=0,
        microsecond=0,
    )
    requested_start = end - timedelta(days=days)
    existing = coverage(symbol, market)

    if not force and _is_current(existing, now):
        # Repair any legacy interrupted rebuild even when source candles are
        # already current. The set-based repair writes only inconsistent rows.
        total_rows = _rebuild_continuous_cvd(symbol, market)
        return FlowBackfillResult(
            symbol, market, 0, 0, total_rows,
            existing["min_time"].isoformat() if existing.get("min_time") else requested_start.isoformat(),
            existing["max_time"].isoformat() if existing.get("max_time") else end.isoformat(),
            True, True, 0, "Already current — verified cumulative series; unchanged rows not rewritten",
        ).to_dict()

    # Resume from the next 30m candle after the latest successful stored row.
    # A one-candle overlap is harmless because the table uses UPSERT and helps
    # refresh the boundary candle.
    latest = existing.get("max_time")
    start = requested_start
    if latest is not None and latest > requested_start:
        start = max(requested_start, latest)

    total_received = 0
    total_stored = 0
    attempts_used = 0
    try:
        chunk_list = list(_chunks(start, end))
        for index, (chunk_start, chunk_end) in enumerate(chunk_list, start=1):
            print(
                f"[flow-backfill] {symbol} {market} chunk {index}/{len(chunk_list)} "
                f"{chunk_start.isoformat()} -> {chunk_end.isoformat()}",
                flush=True,
            )
            rows, attempts = _fetch_chunk(symbol, market, chunk_start, chunk_end)
            attempts_used += attempts
            total_received += len(rows)
            # Commit every chunk immediately. A later 429 cannot erase work that
            # already succeeded in this run.
            total_stored += _store(symbol, market, rows)

        total_rows = _rebuild_continuous_cvd(symbol, market)
        current = coverage(symbol, market)
        ok = total_rows >= 100 or _is_current(current, now)
        message = "OK" if ok else "Too few 30m rows"
        print(
            f"[flow-backfill] {symbol} {market} done: "
            f"received={total_received} total={total_rows}",
            flush=True,
        )
        return FlowBackfillResult(
            symbol, market, total_received, total_stored, total_rows,
            requested_start.isoformat(), end.isoformat(), ok, False,
            attempts_used, message,
        ).to_dict()
    except Exception as exc:
        # Report partial progress from the DB; the next run resumes from it.
        current = coverage(symbol, market)
        return FlowBackfillResult(
            symbol, market, total_received, total_stored, int(current["count"]),
            requested_start.isoformat(), end.isoformat(), False, False,
            attempts_used, repr(exc),
        ).to_dict()


def frontfill_symbol(
    symbol: str,
    market: str,
    days: int = DEFAULT_BACKFILL_DAYS,
    *,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Extend one stored series backwards without touching its live tail.

    ``backfill_symbol`` intentionally retains its established tail-refresh
    contract: it resumes at ``MAX(candle_time)``. Historical research instead
    needs a separate operation which requests the missing half-open prefix
    ``[requested_start, contiguous_suffix_start)``. Chunks are processed
    newest-first so a partial failure remains resumable from the newly proven
    contiguous boundary.

    Every fetched chunk must contain its exact 30-minute grid before any row in
    it is written. Existing timestamps are protected by a half-open boundary,
    insert-only conflict handling and the shared per-series writer lock. Raw
    tail candles are never deleted or updated; their derived continuous CVD is
    rebuilt because adding older deltas changes the correct cumulative value
    of every later candle.
    """
    symbol = str(symbol or "").upper()
    market = market.lower()
    _table_for_market(market)
    days = max(1, min(int(days), MAX_BACKFILL_DAYS))
    current_time = _as_utc(now) if now is not None else datetime.now(timezone.utc)
    request_end = current_time.replace(
        minute=30 if current_time.minute >= 30 else 0,
        second=0,
        microsecond=0,
    )
    requested_start = request_end - timedelta(days=days)
    existing = coverage(symbol, market)
    contiguous_start = _contiguous_stored_start(
        symbol, market, requested_start, existing=existing
    )

    if contiguous_start is not None and contiguous_start <= requested_start:
        total_rows = _rebuild_continuous_cvd(symbol, market)
        return FlowBackfillResult(
            symbol, market, 0, 0, total_rows,
            requested_start.isoformat(), contiguous_start.isoformat(),
            True, True, 0,
            "Historical prefix has a complete 30m grid; live tail left unchanged",
        ).to_dict()

    # With no history, stop after the newest closed, grace-cleared candle.
    # Otherwise stop exactly at the proven contiguous suffix and filter that
    # timestamp out before every write.
    eligible_end = latest_eligible_candle_time(current_time) + timedelta(
        minutes=CANDLE_INTERVAL_MINUTES
    )
    prefix_end = (
        min(contiguous_start, eligible_end)
        if contiguous_start is not None
        else eligible_end
    )
    if prefix_end <= requested_start:
        total_rows = _rebuild_continuous_cvd(symbol, market)
        return FlowBackfillResult(
            symbol, market, 0, 0, total_rows,
            requested_start.isoformat(), prefix_end.isoformat(),
            True, True, 0,
            "No historical prefix is missing; live tail left unchanged",
        ).to_dict()

    total_received = 0
    total_stored = 0
    attempts_used = 0
    try:
        chunk_list = list(_reverse_chunks(requested_start, prefix_end))
        for index, (chunk_start, chunk_end) in enumerate(chunk_list, start=1):
            print(
                f"[flow-frontfill] {symbol} {market} chunk {index}/{len(chunk_list)} "
                f"{chunk_start.isoformat()} -> {chunk_end.isoformat()}",
                flush=True,
            )
            provider_rows, attempts = _fetch_chunk(
                symbol, market, chunk_start, chunk_end
            )
            attempts_used += attempts
            total_received += len(provider_rows)
            prefix_rows = _complete_half_open_rows(
                provider_rows, chunk_start, chunk_end
            )
            # Do not advance MIN(candle_time) over a sparse provider result.
            # Validation above covers the complete chunk before its first write.
            # Each prefix chunk and the corresponding cumulative repair are
            # committed atomically by _store. A later failure keeps this chunk.
            total_stored += _store_prefix(symbol, market, prefix_rows)

        total_rows = _rebuild_continuous_cvd(symbol, market)
        current = coverage(symbol, market)
        verified_start = _contiguous_stored_start(
            symbol, market, requested_start, existing=current
        )
        covered = verified_start is not None and verified_start <= requested_start
        message = (
            "OK — historical prefix filled; live tail left unchanged"
            if covered
            else "Historical prefix incomplete — earliest requested candle was not stored"
        )
        print(
            f"[flow-frontfill] {symbol} {market} done: "
            f"received={total_received} total={total_rows}",
            flush=True,
        )
        return FlowBackfillResult(
            symbol, market, total_received, total_stored, total_rows,
            requested_start.isoformat(), prefix_end.isoformat(), covered, False,
            attempts_used, message,
        ).to_dict()
    except Exception as exc:
        # Newest-first complete chunks make the contiguous suffix a durable
        # resume cursor; a sparse failed chunk has not written any rows.
        current = coverage(symbol, market)
        return FlowBackfillResult(
            symbol, market, total_received, total_stored, int(current["count"]),
            requested_start.isoformat(), prefix_end.isoformat(), False, False,
            attempts_used, repr(exc),
        ).to_dict()


def backfill_all(
    days: int = DEFAULT_BACKFILL_DAYS,
    force: bool = False,
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Sequential queue: one symbol/market is processed at a time."""
    result: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for symbol in TARGET_SYMBOLS:
        result[symbol] = {}
        for market in ("futures", "spot"):
            result[symbol][market] = backfill_symbol(symbol, market, days, force=force)
    return result


def table_count(symbol: str, market: str) -> int:
    return int(coverage(symbol, market)["count"])



def probe_latest_api(symbol: str, market: str) -> Dict[str, Any]:
    """Fetch recent API rows without storing them, for timestamp diagnostics."""
    now = datetime.now(timezone.utc)
    payload, attempts = _request(
        FUTURES_ENDPOINT if market.lower() == "futures" else SPOT_ENDPOINT,
        {
            "exchange_list": EXCHANGE_LIST,
            "symbol": str(symbol).upper(),
            "interval": INTERVAL,
            "limit": 12,
            "start_time": int((now - timedelta(hours=6)).timestamp() * 1000),
            "end_time": int(now.timestamp() * 1000),
            "unit": "usd",
        },
    )
    normalized = _normalize(payload)
    rows = []
    for ts, (buy, sell, api_cvd) in sorted(normalized.items())[-6:]:
        raw = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
        rows.append({
            "raw_timestamp": raw.isoformat(),
            "age_if_open_minutes": round(max(0.0, (now - (raw + timedelta(minutes=30))).total_seconds() / 60.0), 2),
            "age_if_close_minutes": round(max(0.0, (now - raw).total_seconds() / 60.0), 2),
            "buy_volume_usd": buy,
            "sell_volume_usd": sell,
            "api_cvd": api_cvd,
        })
    return {
        "requested_at": now.isoformat(),
        "symbol": str(symbol).upper(),
        "market": market.lower(),
        "attempts": attempts,
        "configured_timestamp_mode": CVD_TIMESTAMP_MODE,
        "rows": rows,
    }


def _cli() -> int:
    import json
    import sys
    action = sys.argv[1].lower() if len(sys.argv) > 1 else ""
    if action == "probe" and len(sys.argv) >= 4:
        print(json.dumps(probe_latest_api(sys.argv[2], sys.argv[3]), indent=2, ensure_ascii=False))
        return 0
    if action == "freshness" and len(sys.argv) >= 4:
        data = freshness(sys.argv[2], sys.argv[3])
        serializable = {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in data.items()}
        print(json.dumps(serializable, indent=2, ensure_ascii=False))
        return 0
    if action == "refresh" and len(sys.argv) >= 3:
        symbol = sys.argv[2]
        markets = (sys.argv[3].lower(),) if len(sys.argv) >= 4 else ("futures", "spot")
        output = {market: backfill_symbol(symbol, market, days=2, force=True) for market in markets}
        print(json.dumps(output, indent=2, ensure_ascii=False, default=str))
        return 0
    if action == "frontfill" and len(sys.argv) >= 3:
        symbol = sys.argv[2]
        markets = (sys.argv[3].lower(),) if len(sys.argv) >= 4 else ("futures", "spot")
        days = int(sys.argv[4]) if len(sys.argv) >= 5 else DEFAULT_BACKFILL_DAYS
        output = {market: frontfill_symbol(symbol, market, days=days) for market in markets}
        print(json.dumps(output, indent=2, ensure_ascii=False, default=str))
        return 0
    print(
        "Usage: python coinglass_flow_foundation.py probe BTC spot | "
        "freshness BTC spot | refresh BTC [spot|futures] | "
        "frontfill BTC [spot|futures] [days]"
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(_cli())
