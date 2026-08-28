"""Read-only historical quality replay for Research Event capture.

This module is a QA tool only. It reads a bounded slice of already-stored market
history from PostgreSQL in transaction read-only mode and feeds timestamped
state changes into an isolated in-memory Research Event sink.

It does NOT reconstruct historical production alerts and does NOT claim that
shadow events were real Telegram alerts. Its purpose is to verify, with real
historical timestamps/data, that the Research Event layer preserves chronology,
repetitions, UTC times and compact state while performing zero database writes.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import os
from typing import Any, Dict, List, Optional

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None

import research_event_capture

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
MAX_HOURS = 24 * 30
MAX_ROWS_PER_STREAM = 500


def _symbol(value: Any) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol or len(symbol) > 16 or not symbol.replace("-", "").isalnum():
        raise ValueError("invalid crypto symbol")
    return symbol


def _connect():
    if not DATABASE_URL or psycopg is None:
        raise RuntimeError("DATABASE_URL/PostgreSQL is not available")
    return psycopg.connect(
        DATABASE_URL,
        row_factory=dict_row,
        options="-c default_transaction_read_only=on -c statement_timeout=8000",
    )


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _f(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _direction(delta: Optional[float], epsilon: float = 0.0) -> str:
    if delta is None or abs(delta) <= epsilon:
        return "NEUTRAL"
    return "LONG" if delta > 0 else "SHORT"


def _price_oi_state(price_delta: Optional[float], oi_delta: Optional[float]) -> str:
    p = _direction(price_delta)
    o = _direction(oi_delta)
    return f"PRICE_{p}_OI_{o}"


def _fetch_rows(symbol: str, start: datetime, end: datetime, limit: int) -> Dict[str, List[Dict[str, Any]]]:
    with _connect() as conn:
        price_oi = [dict(row) for row in conn.execute(
            "SELECT candle_time, price_close, oi_close_usd, price_exchange, price_pair, source "
            "FROM oi_price_history WHERE symbol=%s AND candle_time>=%s AND candle_time<=%s "
            "ORDER BY candle_time ASC LIMIT %s",
            (symbol, start, end, limit),
        ).fetchall()]
        futures = [dict(row) for row in conn.execute(
            "SELECT candle_time, buy_volume_usd, sell_volume_usd, continuous_cum_vol_delta_usd, "
            "exchange_list, source FROM futures_taker_history "
            "WHERE symbol=%s AND candle_time>=%s AND candle_time<=%s "
            "ORDER BY candle_time ASC LIMIT %s",
            (symbol, start, end, limit),
        ).fetchall()]
        spot = [dict(row) for row in conn.execute(
            "SELECT candle_time, buy_volume_usd, sell_volume_usd, continuous_cum_vol_delta_usd, "
            "exchange_list, source FROM spot_taker_history "
            "WHERE symbol=%s AND candle_time>=%s AND candle_time<=%s "
            "ORDER BY candle_time ASC LIMIT %s",
            (symbol, start, end, limit),
        ).fetchall()]
    return {"price_oi": price_oi, "futures": futures, "spot": spot}


def _emit_state(
    sink: research_event_capture.DryRunResearchCapture,
    *, symbol: str, signal_name: str, old_state: str, new_state: str,
    event_time: Any, direction: str, score: Any = None, current_price: Any = None,
    evidence: Optional[Dict[str, Any]] = None,
) -> bool:
    if old_state == new_state:
        return False
    event = research_event_capture.build_signal_state_change(
        symbol=symbol,
        signal_name=signal_name,
        old_state=old_state,
        new_state=new_state,
        event_time=event_time,
        direction=direction,
        score=score,
        current_price=current_price,
        evidence={"quality_test_only": True, **dict(evidence or {})},
        strategy_version="shadow-replay-qa",
    )
    return sink.emit(event)


def _replay_price_oi(symbol: str, rows: List[Dict[str, Any]], sink: research_event_capture.DryRunResearchCapture) -> int:
    emitted = 0
    previous_row: Optional[Dict[str, Any]] = None
    previous_state = "UNSEEN"
    for row in rows:
        if previous_row is None:
            previous_row = row
            continue
        price = _f(row.get("price_close"))
        old_price = _f(previous_row.get("price_close"))
        oi = _f(row.get("oi_close_usd"))
        old_oi = _f(previous_row.get("oi_close_usd"))
        price_delta = None if price is None or old_price is None else price - old_price
        oi_delta = None if oi is None or old_oi is None else oi - old_oi
        state = _price_oi_state(price_delta, oi_delta)
        if previous_state == "UNSEEN":
            previous_state = state
        elif state != previous_state:
            emitted += int(_emit_state(
                sink,
                symbol=symbol,
                signal_name="SHADOW_OI_PRICE_RELATION",
                old_state=previous_state,
                new_state=state,
                event_time=row.get("candle_time"),
                direction=_direction(price_delta),
                current_price=price,
                evidence={
                    "price_delta": price_delta,
                    "oi_delta": oi_delta,
                    "price_source": row.get("price_exchange"),
                    "pair": row.get("price_pair"),
                },
            ))
            previous_state = state
        previous_row = row
    return emitted


def _replay_cvd(
    symbol: str, rows: List[Dict[str, Any]], sink: research_event_capture.DryRunResearchCapture,
    signal_name: str,
) -> int:
    emitted = 0
    previous_row: Optional[Dict[str, Any]] = None
    previous_state = "UNSEEN"
    for row in rows:
        if previous_row is None:
            previous_row = row
            continue
        current = _f(row.get("continuous_cum_vol_delta_usd"))
        old = _f(previous_row.get("continuous_cum_vol_delta_usd"))
        delta = None if current is None or old is None else current - old
        state = _direction(delta)
        if previous_state == "UNSEEN":
            previous_state = state
        elif state != previous_state:
            emitted += int(_emit_state(
                sink,
                symbol=symbol,
                signal_name=signal_name,
                old_state=previous_state,
                new_state=state,
                event_time=row.get("candle_time"),
                direction=state,
                score=delta,
                evidence={
                    "cvd_delta_usd": delta,
                    "exchange_list": row.get("exchange_list"),
                    "source": row.get("source"),
                },
            ))
            previous_state = state
        previous_row = row
    return emitted


def _sort_sink_chronologically(sink: research_event_capture.DryRunResearchCapture) -> research_event_capture.DryRunResearchCapture:
    """Merge separately replayed streams into the same order a live timeline has."""
    ordered = sorted(sink.events(), key=lambda event: _utc(event["alert_time_utc"]))
    chronological = research_event_capture.DryRunResearchCapture(max_events=2000)
    for event_dict in ordered:
        chronological.emit(research_event_capture.ResearchEvent(**event_dict))
    return chronological


def run_shadow_replay(symbol: str = "BTC", hours: int = 24, max_rows: int = 250) -> Dict[str, Any]:
    """Run a bounded real-history QA replay with zero persistence."""
    symbol = _symbol(symbol)
    hours = int(hours)
    limit = max(10, min(int(max_rows), MAX_ROWS_PER_STREAM))
    if hours < 1 or hours > MAX_HOURS:
        raise ValueError(f"hours must be between 1 and {MAX_HOURS}")

    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=hours)
    streams = _fetch_rows(symbol, start, end, limit)
    stream_sink = research_event_capture.DryRunResearchCapture(max_events=2000)

    emitted_by_stream = {
        "price_oi": _replay_price_oi(symbol, streams["price_oi"], stream_sink),
        "futures_cvd": _replay_cvd(symbol, streams["futures"], stream_sink, "SHADOW_FUTURES_CVD_DIRECTION"),
        "spot_cvd": _replay_cvd(symbol, streams["spot"], stream_sink, "SHADOW_SPOT_CVD_DIRECTION"),
    }

    # Historical streams are queried separately. Merge by the original event
    # timestamp before validation so the replay mirrors one real market timeline.
    sink = _sort_sink_chronologically(stream_sink)
    events = sink.events()
    timestamps = [_utc(event["alert_time_utc"]) for event in events]
    ordered = timestamps == sorted(timestamps)
    all_utc = all(ts.utcoffset() == timedelta(0) for ts in timestamps)
    unique_fingerprints = len({event["event_fingerprint"] for event in events}) == len(events)

    # Exact replay must deduplicate, while a different timestamp remains a new occurrence.
    exact_replay_dedup_ok = True
    if events:
        first = events[0]
        duplicate = research_event_capture.build_signal_state_change(
            symbol=first["symbol"],
            signal_name=first["event_type"],
            old_state=first["engine_snapshot"]["old_state"],
            new_state=first["engine_snapshot"]["new_state"],
            event_time=first["alert_time_utc"],
            direction=first["direction"],
            score=first.get("score"),
            current_price=first.get("current_price"),
            evidence=first["engine_snapshot"].get("evidence") or {},
            strategy_version="shadow-replay-qa",
        )
        exact_replay_dedup_ok = sink.emit(duplicate) is False

    types = Counter(event["event_type"] for event in events)
    sample = events[:3]
    status = sink.status()
    pass_checks = {
        "database_writes_false": status.get("database_writes") is False,
        "timestamps_ordered": ordered,
        "timestamps_utc": all_utc,
        "fingerprints_unique": unique_fingerprints,
        "exact_replay_deduplicated": exact_replay_dedup_ok,
        "bounded_memory": status.get("capacity", 0) <= 2000,
    }

    return {
        "ok": all(pass_checks.values()),
        "symbol": symbol,
        "requested_hours": hours,
        "period_start_utc": start.isoformat(),
        "period_end_utc": end.isoformat(),
        "raw_rows": {name: len(rows) for name, rows in streams.items()},
        "events_created": len(events),
        "events_by_stream": emitted_by_stream,
        "event_types": dict(types),
        "first_event_utc": events[0]["alert_time_utc"] if events else None,
        "last_event_utc": events[-1]["alert_time_utc"] if events else None,
        "checks": pass_checks,
        "sink": status,
        "sample_events": sample,
        "scope_note": (
            "Quality test only: real stored Price/OI/CVD history is replayed into Research Events. "
            "These shadow events are not reconstructed historical Telegram alerts. Exact alert-performance "
            "research starts only after timestamped production Research Events are persisted."
        ),
    }
