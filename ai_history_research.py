"""Read-only historical research helpers for the candidate AI agent.

This module never creates or mutates schema. It opens PostgreSQL in default
read-only mode and returns bounded summaries so the language model can research
historical OI/CVD/price context without loading raw tables into model context.

Exact timestamps are preserved because future alert research will join compact
alert events to market/exchange/news context by event time instead of duplicating
large datasets inside every alert record.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
from typing import Any, Dict, Iterable, Optional

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
MAX_LOOKBACK_HOURS = 24 * 90
MAX_CONTEXT_WINDOW_MINUTES = 24 * 60


def _require_db() -> None:
    if not DATABASE_URL or psycopg is None:
        raise RuntimeError("DATABASE_URL/PostgreSQL is not available to the AI candidate")


def _symbol(value: Any) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol or len(symbol) > 16 or not symbol.replace("-", "").isalnum():
        raise ValueError("Invalid crypto symbol")
    return symbol


def _as_utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip().replace("Z", "+00:00")
        if not text:
            raise ValueError("timestamp is required")
        parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _row(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    return dict(row) if row else None


def _rows(rows: Iterable[Dict[str, Any]]) -> list[Dict[str, Any]]:
    return [dict(row) for row in rows]


def _connect():
    _require_db()
    # default_transaction_read_only prevents accidental writes even if a future
    # query in this module is changed incorrectly.
    conn = psycopg.connect(
        DATABASE_URL,
        row_factory=dict_row,
        options="-c default_transaction_read_only=on -c statement_timeout=8000",
    )
    return conn


def _first_last(conn, table: str, time_col: str, symbol: str, start: datetime, end: datetime, columns: str):
    first = conn.execute(
        f"SELECT {columns} FROM {table} WHERE symbol=%s AND {time_col}>=%s AND {time_col}<=%s "
        f"ORDER BY {time_col} ASC LIMIT 1",
        (symbol, start, end),
    ).fetchone()
    last = conn.execute(
        f"SELECT {columns} FROM {table} WHERE symbol=%s AND {time_col}>=%s AND {time_col}<=%s "
        f"ORDER BY {time_col} DESC LIMIT 1",
        (symbol, start, end),
    ).fetchone()
    return _row(first), _row(last)


def _pct_change(new: Any, old: Any) -> Optional[float]:
    try:
        n = float(new)
        o = float(old)
    except (TypeError, ValueError):
        return None
    if o == 0:
        return None
    return round((n - o) / o * 100.0, 6)


def historical_summary(symbol: str, lookback_hours: int) -> Dict[str, Any]:
    """Summarize existing historical market data for one symbol.

    The query returns aggregates and boundary samples only. It deliberately does
    not return thousands of raw candles to the model.
    """
    symbol = _symbol(symbol)
    hours = int(lookback_hours)
    if hours < 1 or hours > MAX_LOOKBACK_HOURS:
        raise ValueError(f"lookback_hours must be between 1 and {MAX_LOOKBACK_HOURS}")

    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=hours)

    with _connect() as conn:
        price_first, price_last = _first_last(
            conn,
            "oi_price_history",
            "candle_time",
            symbol,
            start,
            end,
            "candle_time, price_close, oi_close_usd, price_exchange, price_pair, source",
        )
        price_stats = _row(
            conn.execute(
                "SELECT COUNT(*) AS samples, MIN(price_close) AS min_price, MAX(price_close) AS max_price, "
                "AVG(oi_close_usd) AS avg_oi_usd FROM oi_price_history "
                "WHERE symbol=%s AND candle_time>=%s AND candle_time<=%s",
                (symbol, start, end),
            ).fetchone()
        ) or {}

        markets: Dict[str, Any] = {}
        for market, table in (("futures", "futures_taker_history"), ("spot", "spot_taker_history")):
            first, last = _first_last(
                conn,
                table,
                "candle_time",
                symbol,
                start,
                end,
                "candle_time, continuous_cum_vol_delta_usd, api_cum_vol_delta_usd, exchange_list, source",
            )
            totals = _row(
                conn.execute(
                    f"SELECT COUNT(*) AS samples, SUM(buy_volume_usd) AS buy_volume_usd, "
                    f"SUM(sell_volume_usd) AS sell_volume_usd, "
                    f"SUM(buy_volume_usd-sell_volume_usd) AS net_taker_flow_usd "
                    f"FROM {table} WHERE symbol=%s AND candle_time>=%s AND candle_time<=%s",
                    (symbol, start, end),
                ).fetchone()
            ) or {}
            cvd_delta = None
            if first and last:
                cvd_delta = float(last["continuous_cum_vol_delta_usd"]) - float(first["continuous_cum_vol_delta_usd"])
            markets[market] = {
                "first": first,
                "last": last,
                "continuous_cvd_change_usd": cvd_delta,
                "totals": totals,
            }

        regimes = _rows(
            conn.execute(
                "SELECT state, direction, COUNT(*) AS samples FROM oi_regime_snapshots "
                "WHERE symbol=%s AND collected_at>=%s AND collected_at<=%s "
                "GROUP BY state, direction ORDER BY samples DESC",
                (symbol, start, end),
            ).fetchall()
        )

        technical = _rows(
            conn.execute(
                "SELECT timeframe, direction, COUNT(*) AS samples, AVG(technical_score) AS avg_score "
                "FROM technical_signals WHERE symbol=%s AND signal_timestamp>=%s AND signal_timestamp<=%s "
                "GROUP BY timeframe, direction ORDER BY timeframe, direction",
                (symbol, start, end),
            ).fetchall()
        )

    return {
        "symbol": symbol,
        "requested_lookback_hours": hours,
        "period_start_utc": start,
        "period_end_utc": end,
        "price_oi": {
            "first": price_first,
            "last": price_last,
            "price_change_pct": _pct_change(
                price_last.get("price_close") if price_last else None,
                price_first.get("price_close") if price_first else None,
            ),
            "oi_change_pct": _pct_change(
                price_last.get("oi_close_usd") if price_last else None,
                price_first.get("oi_close_usd") if price_first else None,
            ),
            "stats": price_stats,
        },
        "cvd": markets,
        "oi_regime_distribution": regimes,
        "technical_signal_distribution": technical,
        "research_note": (
            "This is historical market evidence, not alert-performance history. "
            "Alert outcome analysis requires timestamped Research Events that are not yet being written."
        ),
    }


def context_at_time(symbol: str, timestamp: Any, window_minutes: int) -> Dict[str, Any]:
    """Return the existing market evidence nearest to an exact UTC event time."""
    symbol = _symbol(symbol)
    event_time = _as_utc(timestamp)
    window = int(window_minutes)
    if window < 1 or window > MAX_CONTEXT_WINDOW_MINUTES:
        raise ValueError(f"window_minutes must be between 1 and {MAX_CONTEXT_WINDOW_MINUTES}")
    start = event_time - timedelta(minutes=window)
    end = event_time + timedelta(minutes=window)

    with _connect() as conn:
        price_oi = _row(
            conn.execute(
                "SELECT candle_time, price_close, oi_close_usd, price_exchange, price_pair, source, "
                "ABS(EXTRACT(EPOCH FROM (candle_time-%s))) AS distance_seconds "
                "FROM oi_price_history WHERE symbol=%s AND candle_time BETWEEN %s AND %s "
                "ORDER BY distance_seconds ASC LIMIT 1",
                (event_time, symbol, start, end),
            ).fetchone()
        )

        flow: Dict[str, Any] = {}
        for market, table in (("futures", "futures_taker_history"), ("spot", "spot_taker_history")):
            flow[market] = _row(
                conn.execute(
                    f"SELECT candle_time, buy_volume_usd, sell_volume_usd, "
                    f"api_cum_vol_delta_usd, continuous_cum_vol_delta_usd, exchange_list, source, "
                    f"ABS(EXTRACT(EPOCH FROM (candle_time-%s))) AS distance_seconds "
                    f"FROM {table} WHERE symbol=%s AND candle_time BETWEEN %s AND %s "
                    f"ORDER BY distance_seconds ASC LIMIT 1",
                    (event_time, symbol, start, end),
                ).fetchone()
            )

        regime = _row(
            conn.execute(
                "SELECT collected_at, price, open_interest_usd, price_change_pct, oi_change_pct, state, direction, "
                "reason, data_quality_status, price_source, oi_source, "
                "ABS(EXTRACT(EPOCH FROM (collected_at-%s))) AS distance_seconds "
                "FROM oi_regime_snapshots WHERE symbol=%s AND collected_at BETWEEN %s AND %s "
                "ORDER BY distance_seconds ASC LIMIT 1",
                (event_time, symbol, start, end),
            ).fetchone()
        )

        technical = _rows(
            conn.execute(
                "SELECT signal_timestamp, timeframe, direction, technical_score, is_confirmed, indicator_version, source "
                "FROM technical_signals WHERE symbol=%s AND signal_timestamp BETWEEN %s AND %s "
                "ORDER BY ABS(EXTRACT(EPOCH FROM (signal_timestamp-%s))) ASC LIMIT 20",
                (symbol, start, end, event_time),
            ).fetchall()
        )

        nearest_mp_time = _row(
            conn.execute(
                "SELECT collected_at, ABS(EXTRACT(EPOCH FROM (collected_at-%s))) AS distance_seconds "
                "FROM max_pain_snapshots WHERE symbol=%s AND collected_at BETWEEN %s AND %s "
                "ORDER BY distance_seconds ASC LIMIT 1",
                (event_time, symbol, start, end),
            ).fetchone()
        )
        max_pain = []
        if nearest_mp_time:
            max_pain = _rows(
                conn.execute(
                    "SELECT collected_at, timeframe, current_price, short_max_pain, long_max_pain, "
                    "short_liquidation_amount, long_liquidation_amount, distance_short_pct, distance_long_pct, alert_level "
                    "FROM max_pain_snapshots WHERE symbol=%s AND collected_at=%s ORDER BY timeframe",
                    (symbol, nearest_mp_time["collected_at"]),
                ).fetchall()
            )

    return {
        "symbol": symbol,
        "event_time_utc": event_time,
        "window_minutes_each_side": window,
        "window_start_utc": start,
        "window_end_utc": end,
        "nearest_price_oi": price_oi,
        "nearest_cvd": flow,
        "nearest_oi_regime": regime,
        "nearby_technical_signals": technical,
        "nearest_max_pain_snapshot_time": nearest_mp_time,
        "nearest_max_pain_rows": max_pain,
        "research_note": (
            "Exact event time is preserved. Future exchange/news/global-context datasets should be stored separately "
            "with their own timestamps and joined to this event time, avoiding duplication per alert."
        ),
    }
