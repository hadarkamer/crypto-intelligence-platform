"""Stage 87.2 foundation: stable CoinGlass CVD backfill downloader.

The module stores official CoinGlass 30-minute aggregated Futures and Spot
Buy/Sell volume plus the official API CVD. It is intentionally isolated from
all alert, Watch, Max-Pain and trade-decision logic.

Stage 87.2 adds:
- one-request-at-a-time queue
- safe spacing under the Startup-plan rate limit
- retry with exponential backoff for HTTP 429 / transient failures
- resume/skip behaviour so already-current markets are not downloaded again
- chunk-by-chunk commits, so successful work survives a later failure
- continuous CVD rebuilt deterministically from saved Buy-Sell deltas
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
    # Continuous values are rebuilt from all stored Buy-Sell deltas after each
    # symbol/market finishes. The placeholder avoids trusting chunk-relative
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
        for attempt in range(3):
            try:
                with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                    with conn.cursor() as cur:
                        cur.executemany(sql, values)
                    conn.commit()
                break
            except psycopg.errors.DeadlockDetected:
                if attempt >= 2:
                    raise
                time.sleep(0.4 * (attempt + 1))
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


def _rebuild_continuous_cvd(symbol: str, market: str) -> int:
    """Recompute one deterministic continuous series from saved Buy-Sell deltas."""
    init_db()
    table = _table_for_market(market)
    symbol = str(symbol).upper()
    if _use_postgres():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            rows = conn.execute(
                f"SELECT candle_time,buy_volume_usd,sell_volume_usd FROM {table} "
                "WHERE symbol=%s ORDER BY candle_time",
                (symbol,),
            ).fetchall()
            cumulative = 0.0
            values = []
            for row in rows:
                cumulative += float(row["buy_volume_usd"]) - float(row["sell_volume_usd"])
                values.append((cumulative, row["candle_time"], symbol))
            with conn.cursor() as cur:
                cur.executemany(
                    f"UPDATE {table} SET continuous_cum_vol_delta_usd=%s WHERE candle_time=%s AND symbol=%s",
                    values,
                )
            conn.commit()
            return len(values)
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"SELECT candle_time,buy_volume_usd,sell_volume_usd FROM {table} "
            "WHERE symbol=? ORDER BY candle_time",
            (symbol,),
        ).fetchall()
        cumulative = 0.0
        values = []
        for row in rows:
            cumulative += float(row["buy_volume_usd"]) - float(row["sell_volume_usd"])
            values.append((cumulative, row["candle_time"], symbol))
        conn.executemany(
            f"UPDATE {table} SET continuous_cum_vol_delta_usd=? WHERE candle_time=? AND symbol=?",
            values,
        )
        conn.commit()
        return len(values)


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
        # A five-minute poll frequently finds that the newest *closed* 30m
        # candle is already stored.  Rebuilding the entire continuous series in
        # that no-change path creates needless writes and can contend with OI
        # and Watch reads.  A real insert still rebuilds deterministically below.
        total_rows = int(existing.get("count") or 0)
        return FlowBackfillResult(
            symbol, market, 0, 0, total_rows,
            existing["min_time"].isoformat() if existing.get("min_time") else requested_start.isoformat(),
            existing["max_time"].isoformat() if existing.get("max_time") else end.isoformat(),
            True, True, 0, "Already current — skipped without database writes",
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
    print("Usage: python coinglass_flow_foundation.py probe BTC spot | freshness BTC spot | refresh BTC [spot|futures]")
    return 2


if __name__ == "__main__":
    raise SystemExit(_cli())
