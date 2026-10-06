"""Immutable, source-separated closed one-minute price archive/read-through.

Missing minutes remain missing. A successful API request or a row count alone
is never accepted as continuity. Disabled or not-yet-migrated installations use
their original reader. Invalid source evidence and frozen revisions fail closed.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import math
import os
from typing import Mapping

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # dependency remains optional for standalone provider tests
    psycopg = None
    dict_row = None

BINANCE_SPOT = 'BINANCE_SPOT_TRADE_1M'
HYPERLIQUID_PERP = 'HYPERLIQUID_HYPE_PERP_TRADE_1M'
BINANCE_MARK = 'BINANCE_HYPE_FUTURES_MARK_1M'
HYPERLIQUID_SPOT = 'HYPERLIQUID_HYPE_SPOT_TRADE_1M'
SPOT_SYMBOLS = ('BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'DOGE', 'ZEC')
ACTIVE_ROUTES = tuple((BINANCE_SPOT, s) for s in SPOT_SYMBOLS) + ((HYPERLIQUID_PERP, 'HYPE'),)
MINUTE = timedelta(minutes=1)
MILLISECOND = timedelta(milliseconds=1)
PAGE_MINUTES = 1000
TRUE = {'1', 'true', 'yes', 'on'}


def enabled():
    return os.getenv('RESEARCH_PRICE_ARCHIVE_ENABLED', '').strip().lower() in TRUE


def database_url():
    dedicated = os.getenv('RESEARCH_DATABASE_URL', '').strip()
    if dedicated:
        return dedicated
    if os.getenv('RESEARCH_USE_PRIMARY_DATABASE', '').strip().lower() in TRUE:
        return os.getenv('DATABASE_URL', '').strip()
    return ''


def utc(value):
    value = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError('Archive boundaries require an explicit timezone')
    return value.astimezone(timezone.utc)


def bounds(start, end):
    start, end = utc(start), utc(end)
    if end <= start:
        raise ValueError('Archive end must be after start')
    first = start.replace(second=0, microsecond=0)
    if first < start:
        first += MINUTE
    # Inclusive close-time cutoff; an exact minute boundary does not admit the
    # newly opened minute. Also handles cutoffs with microsecond precision.
    last = (end + MILLISECOND).replace(second=0, microsecond=0) - MINUTE
    return first, last, max(0, int((last-first)/MINUTE)+1)


def source_metadata(route, symbol):
    if route == BINANCE_SPOT and symbol in SPOT_SYMBOLS:
        return dict(symbol=symbol, pair=symbol+'USDT', exchange='binance', market='spot',
                    interval='1m', interval_seconds=60, multiplier=1.0)
    if symbol != 'HYPE':
        raise ValueError('Unsupported archive route/symbol')
    if route == HYPERLIQUID_PERP:
        from hyperliquid_perp_price_path import SOURCE
        return dict(SOURCE)
    if route == BINANCE_MARK:
        from binance_futures_mark_price_path import SOURCE_URL, METHOD_VERSION, PROVENANCE
        return dict(symbol='HYPE', pair='HYPEUSDT', exchange='binance', market='futures',
                    price_kind='MARK', interval='1m', interval_seconds=60,
                    source_url=SOURCE_URL, method_version=METHOD_VERSION, provenance=PROVENANCE)
    if route == HYPERLIQUID_SPOT:
        return dict(symbol='HYPE', pair='HYPE/USDT', api_coin='@107', exchange='hyperliquid',
                    market='spot', interval='1m', interval_seconds=60,
                    provenance='EXCHANGE_API_HISTORICAL_CANDLES_IMPORTED')
    raise ValueError('Unsupported archive route/symbol')


def schema_ready(conn):
    row = conn.execute("""SELECT to_regclass('research_price_archive_bars') IS NOT NULL
        AND to_regclass('research_price_archive_cursors') IS NOT NULL
        AND to_regclass('research_price_archive_gaps') IS NOT NULL AS ready""").fetchone()
    return bool(row and (row['ready'] if isinstance(row, Mapping) else row[0]))


def normalize_bar(candle, *, now=None):
    get = candle.get if isinstance(candle, Mapping) else lambda key, default=None: getattr(candle, key, default)
    opened, closed = utc(get('open_time_utc')), utc(get('close_time_utc'))
    if opened != opened.replace(second=0, microsecond=0) or closed != opened+MINUTE-MILLISECOND:
        raise ValueError('Archive bar must be an aligned closed one-minute candle')
    if closed > utc(now or datetime.now(timezone.utc)):
        raise ValueError('Archive bar is not yet closed')
    values = {key: float(get(key)) for key in ('open', 'high', 'low', 'close')}
    if not all(math.isfinite(v) and v > 0 for v in values.values()):
        raise ValueError('Archive prices must be finite and positive')
    if values['low'] > min(values.values()) or values['high'] < max(values.values()):
        raise ValueError('Archive OHLC is inconsistent')
    volume = get('volume')
    if volume is not None:
        volume = float(volume)
        if not math.isfinite(volume) or volume < 0:
            raise ValueError('Archive volume must be finite and nonnegative')
    return dict(open_time_utc=opened, close_time_utc=closed, **values, volume=volume)


def validate_path(route, symbol, path, start, end, *, now=None):
    expected_meta = source_metadata(route, symbol)
    if any(path.get(k) != v for k, v in expected_meta.items()):
        raise ValueError('Archive price source metadata does not match its route')
    first, last, _ = bounds(start, end)
    bars = {}
    for candle in path.get('candles') or []:
        bar = normalize_bar(candle, now=now)
        opened = bar['open_time_utc']
        if opened < first or opened > last:
            raise ValueError('Archive source candle is outside the requested interval')
        if (route in (BINANCE_SPOT, HYPERLIQUID_SPOT)) != (bar['volume'] is not None):
            raise ValueError('Archive volume does not match source candle contract')
        if opened in bars and bars[opened] != bar:
            raise ValueError('Conflicting duplicate archive candle')
        bars[opened] = bar
    return [bars[t] for t in sorted(bars)]


def missing_ranges(open_times, first, last, *, page_minutes=PAGE_MINUTES):
    """Exact sorted contiguous absent intervals, bounded in provider page units."""
    present = set(open_times)
    result, cursor = [], first
    while cursor <= last:
        if cursor in present:
            cursor += MINUTE
            continue
        start = cursor
        while cursor <= last and cursor not in present and cursor-start < page_minutes*MINUTE:
            cursor += MINUTE
        result.append((start, cursor-MILLISECOND))
    return result


def write_bars(conn, route, symbol, bars):
    """Atomic insert; the DB trigger closes concurrent ON CONFLICT revision races."""
    source_metadata(route, symbol)
    if not bars:
        return 0
    payload = json.dumps(bars, default=str, allow_nan=False)
    with conn.transaction():
        cursor = conn.execute("""INSERT INTO research_price_archive_bars
            (route,symbol,open_time_utc,close_time_utc,open,high,low,close,volume)
            SELECT %s,%s,x.* FROM jsonb_to_recordset(%s::jsonb) AS x(
                open_time_utc timestamptz,close_time_utc timestamptz,
                open double precision,high double precision,low double precision,
                close double precision,volume double precision)
            ON CONFLICT(route,symbol,open_time_utc) DO UPDATE SET
                close_time_utc=EXCLUDED.close_time_utc,open=EXCLUDED.open,
                high=EXCLUDED.high,low=EXCLUDED.low,close=EXCLUDED.close,volume=EXCLUDED.volume""",
            (route, symbol, payload))
    return max(0, cursor.rowcount)


def read_bars(conn, route, symbol, first, last):
    return conn.execute("""SELECT open_time_utc,close_time_utc,open,high,low,close,volume
        FROM research_price_archive_bars WHERE route=%s AND symbol=%s
        AND open_time_utc>=%s AND open_time_utc<=%s ORDER BY open_time_utc""",
        (route, symbol, first, last)).fetchall()


def _result(route, symbol, bars, first, last, expected, requests, cached):
    meta = source_metadata(route, symbol)
    missing = missing_ranges((r['open_time_utc'] for r in bars), first, last)
    if route in (BINANCE_SPOT, HYPERLIQUID_SPOT):
        from binance_spot_price_path import SpotCandle
        candles = [SpotCandle(**dict(bar)) for bar in bars]
    else:
        candles = [{k:v for k,v in bar.items() if k!='volume'} for bar in bars]
    result = dict(meta, candles=candles, expected_candles=expected, complete=not missing,
                  archive_route=route, archive_cached_candles=cached,
                  archive_missing_ranges=[{'start_time_utc':a.isoformat(), 'end_time_utc':b.isoformat()} for a,b in missing])
    if route in (HYPERLIQUID_PERP, BINANCE_MARK):
        result.update(missing_candles=expected-len(bars), duplicate_candles=0, request_count=requests)
    if route == HYPERLIQUID_PERP:
        result['retention_candles'] = 5000
    return result


def get_path(route, symbol, start, end, fetcher, *, database_url=None, connection=None, now=None):
    """Read exact stored path, fetch/persist only absent pages via *raw* fetcher.

    The optional connection is for bounded worker/test use and always takes a
    dictionary row factory. No network/DB fallback can hide invalid evidence.
    """
    # The shared Spot reader also serves additional bot symbols and scaled
    # aliases. Only the eight explicitly configured instruments are archived.
    # Preserve the original reader for every other supported Spot instrument.
    symbol = str(symbol).strip().upper()
    if route == BINANCE_SPOT and symbol not in SPOT_SYMBOLS:
        return fetcher(symbol,start,end)
    dsn = database_url if database_url is not None else globals()['database_url']()
    if connection is None:
        if not enabled() or not dsn or psycopg is None:
            return fetcher(symbol, start, end)
        with psycopg.connect(dsn, autocommit=True, row_factory=dict_row, connect_timeout=5,
                            options='-c statement_timeout=15000 -c lock_timeout=2000') as conn:
            if not schema_ready(conn):
                return fetcher(symbol, start, end)
            return get_path(route, symbol, start, end, fetcher, connection=conn, now=now)
    conn = connection
    source_metadata(route, symbol)
    first, last, expected = bounds(start, end)
    if expected > 100000:
        raise ValueError('Archive read exceeds bounded path budget')
    bars = [dict(b) for b in read_bars(conn, route, symbol, first, last)]
    cached, requests = len(bars), 0
    for page_start, page_end in missing_ranges((r['open_time_utc'] for r in bars), first, last):
        path = fetcher(symbol, page_start, page_end)
        incoming = validate_path(route, symbol, path, page_start, page_end, now=now)
        write_bars(conn, route, symbol, incoming)
        requests += 1
    bars = [dict(b) for b in read_bars(conn, route, symbol, first, last)]
    return _result(route, symbol, bars, first, last, expected, requests, cached)


def read_path(route, symbol, start, end, *, database_url=None, connection=None):
    """Read-only exact path; no provider calls, fallback or database writes.

    Explicit connection/DSN is sufficient for read-only analysis even when the
    scheduled collector is disabled. Missing schema is reported, not fabricated.
    """
    if connection is None:
        dsn = database_url if database_url is not None else globals()['database_url']()
        if not dsn or psycopg is None:
            raise RuntimeError('Price archive database is not configured')
        with psycopg.connect(dsn, autocommit=True, row_factory=dict_row, connect_timeout=5,
                            options='-c statement_timeout=15000 -c lock_timeout=2000') as conn:
            if not schema_ready(conn):
                raise RuntimeError('Price archive schema is not available')
            return read_path(route, symbol, start, end, connection=conn)
    source_metadata(route, symbol)
    first, last, expected = bounds(start, end)
    if expected > 31*24*60:
        raise ValueError('Read-only archive path exceeds 31 days')
    bars = [dict(b) for b in read_bars(connection, route, symbol, first, last)]
    return _result(route, symbol, bars, first, last, expected, 0, len(bars))
