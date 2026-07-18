"""TradingView technical-signal ingestion for Shadow Mode.

This module intentionally does not change liquidity or alert scores. It only:
- validates and normalizes incoming TradingView webhook payloads;
- stores both normalized fields and the raw payload;
- prevents duplicate inserts through a deterministic fingerprint;
- exposes read helpers for status/debugging.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


ALLOWED_DIRECTIONS = {"LONG", "SHORT", "NEUTRAL"}
TIMEFRAME_RE = re.compile(r"^(?:[1-9]\d*)(?:[mhdwM])$|^[1-9]\d*$")


@dataclass(frozen=True)
class NormalizedTechnicalSignal:
    symbol: str
    exchange: Optional[str]
    timeframe: str
    direction: str
    technical_score: float
    signal_timestamp: str
    bar_close_timestamp: Optional[str]
    is_confirmed: bool
    indicator_version: Optional[str]
    settings_profile: Optional[str]
    source: str
    fingerprint: str
    raw_payload: str


def _pick(payload: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in payload and payload[key] not in (None, ""):
            return payload[key]
    return default


def normalize_symbol(value: Any) -> Tuple[str, Optional[str]]:
    raw = str(value or "").strip().upper()
    if not raw:
        raise ValueError("missing symbol")

    exchange = None
    if ":" in raw:
        exchange, raw = raw.split(":", 1)

    raw = raw.replace("/", "").replace("-", "").replace("_", "")
    for quote in ("USDT.P", "USDTPERP", "USDT", "USDC", "BUSD", "USD", "PERP"):
        if raw.endswith(quote) and len(raw) > len(quote):
            raw = raw[: -len(quote)]
            break

    if not re.fullmatch(r"[A-Z0-9]{2,20}", raw):
        raise ValueError(f"invalid symbol: {value!r}")
    return raw, exchange


def normalize_timeframe(value: Any) -> str:
    raw = str(value or "").strip()
    aliases = {
        "60": "1h", "120": "2h", "240": "4h", "360": "6h",
        "D": "1d", "1D": "1d", "W": "1w", "1W": "1w",
        "M": "1M", "1M": "1M",
    }
    raw = aliases.get(raw, raw)
    if raw.isdigit():
        raw = f"{raw}m"
    raw = raw.lower() if not raw.endswith("M") else raw
    if not TIMEFRAME_RE.fullmatch(raw):
        raise ValueError(f"invalid timeframe: {value!r}")
    return raw


def normalize_direction(value: Any, score: Optional[float] = None) -> str:
    raw = str(value or "").strip().upper()
    aliases = {
        "BUY": "LONG", "BULL": "LONG", "BULLISH": "LONG", "1": "LONG",
        "SELL": "SHORT", "BEAR": "SHORT", "BEARISH": "SHORT", "-1": "SHORT",
        "FLAT": "NEUTRAL", "NONE": "NEUTRAL", "0": "NEUTRAL",
    }
    raw = aliases.get(raw, raw)
    if not raw and score is not None:
        raw = "LONG" if score > 50 else "SHORT" if score < 50 else "NEUTRAL"
    if raw not in ALLOWED_DIRECTIONS:
        raise ValueError(f"invalid direction: {value!r}")
    return raw


def normalize_score(value: Any) -> float:
    try:
        score = float(value)
    except (TypeError, ValueError):
        raise ValueError("missing or invalid technical score")
    if not 0 <= score <= 100:
        raise ValueError("technical score must be between 0 and 100")
    return round(score, 4)


def normalize_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "confirmed", "closed"}


def normalize_timestamp(value: Any, *, default_now: bool = False) -> Optional[str]:
    if value in (None, ""):
        if not default_now:
            return None
        return datetime.now(timezone.utc).isoformat()

    if isinstance(value, (int, float)) or str(value).strip().isdigit():
        numeric = float(value)
        if numeric > 10_000_000_000:
            numeric /= 1000.0
        parsed = datetime.fromtimestamp(numeric, tz=timezone.utc)
    else:
        text = str(value).strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"invalid timestamp: {value!r}") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        parsed = parsed.astimezone(timezone.utc)
    return parsed.isoformat()


def normalize_payload(payload: Dict[str, Any]) -> NormalizedTechnicalSignal:
    if not isinstance(payload, dict):
        raise ValueError("payload must be a JSON object")

    symbol, parsed_exchange = normalize_symbol(
        _pick(payload, "symbol", "ticker", "tickerid", "syminfo_ticker")
    )
    exchange = _pick(payload, "exchange", default=parsed_exchange)
    exchange = str(exchange).upper().strip() if exchange else parsed_exchange

    timeframe = normalize_timeframe(_pick(payload, "timeframe", "interval", "tf"))
    score = normalize_score(_pick(payload, "technical_score", "score", "technicalScore"))
    direction = normalize_direction(_pick(payload, "direction", "side", "signal"), score)
    signal_timestamp = normalize_timestamp(
        _pick(payload, "signal_timestamp", "timestamp", "time", "timenow"),
        default_now=True,
    )
    bar_close_timestamp = normalize_timestamp(
        _pick(payload, "bar_close_timestamp", "bar_close_time", "barTime", "bar_time")
    )
    is_confirmed = normalize_bool(
        _pick(payload, "is_confirmed", "confirmed", "bar_closed"),
        default=False,
    )
    indicator_version = _pick(payload, "indicator_version", "version")
    settings_profile = _pick(payload, "settings_profile", "profile")
    source = str(_pick(payload, "source", default="tradingview")).strip() or "tradingview"

    canonical = {
        "symbol": symbol,
        "exchange": exchange,
        "timeframe": timeframe,
        "direction": direction,
        "technical_score": score,
        "signal_timestamp": signal_timestamp,
        "bar_close_timestamp": bar_close_timestamp,
        "is_confirmed": is_confirmed,
        "indicator_version": indicator_version,
        "settings_profile": settings_profile,
        "source": source,
    }
    fingerprint = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()

    return NormalizedTechnicalSignal(
        **canonical,
        fingerprint=fingerprint,
        raw_payload=json.dumps(payload, ensure_ascii=False, sort_keys=True),
    )



def _clean_embed_text(value: Any) -> str:
    text = str(value or "").strip()
    text = text.replace("**", "").replace("`", "")
    return text.strip()


def _extract_number(value: Any, *, field_name: str) -> float:
    text = _clean_embed_text(value).replace(",", "")
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not match:
        raise ValueError(f"missing or invalid {field_name}")
    return float(match.group(0))


def normalize_tradingview_webhook(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Convert the G.O.A.T Discord-embed webhook into the internal contract.

    TradingView sends the alert message exactly as configured. The shared
    indicator templates use Discord embed JSON, so this adapter extracts the
    symbol, exchange, event type, direction and plotted values without
    requiring the user to rewrite the alert messages.
    """
    if not isinstance(payload, dict):
        raise ValueError("payload must be a JSON object")

    # Already-normalized payloads remain supported.
    if not isinstance(payload.get("embeds"), list):
        return dict(payload)

    embeds = payload.get("embeds") or []
    if not embeds or not isinstance(embeds[0], dict):
        raise ValueError("missing TradingView embed")

    embed = embeds[0]
    title = _clean_embed_text(embed.get("title"))
    match = re.match(r"^([^:|]+):([^|]+)\|\s*(.+)$", title)
    if not match:
        raise ValueError("invalid TradingView embed title")

    ticker = match.group(1).strip().upper()
    exchange = match.group(2).strip().upper()
    label = match.group(3).strip().upper()

    if "BULLISH" in label:
        event_type = "bullish_signal"
        direction = "LONG"
    elif "BEARISH" in label:
        event_type = "bearish_signal"
        direction = "SHORT"
    elif "STRONG ZONE" in label:
        event_type = "strong_zone"
        direction = "NEUTRAL"
    else:
        raise ValueError(f"unsupported TradingView alert type: {label}")

    fields = {}
    for item in embed.get("fields") or []:
        if not isinstance(item, dict):
            continue
        name = _clean_embed_text(item.get("name")).lower()
        value = _clean_embed_text(item.get("value"))
        if "score" in name and "avg" not in name:
            fields["score"] = value
        elif "avg" in name and "score" in name:
            fields["avg_score"] = value
        elif "price" in name:
            fields["price"] = value
        elif name.endswith("tf") or " tf" in name or "timeframe" in name:
            fields["timeframe"] = value
        elif "exit" in name:
            fields["exit_pressure"] = value
        elif "atr" in name and "stop" in name:
            fields["atr_stop"] = value

    score = _extract_number(fields.get("score"), field_name="Score")
    timeframe = _clean_embed_text(fields.get("timeframe"))
    if not timeframe:
        raise ValueError("missing timeframe")

    now = datetime.now(timezone.utc).replace(second=0, microsecond=0).isoformat()
    normalized = {
        "source": "tradingview_goat",
        "symbol": ticker,
        "exchange": exchange,
        "timeframe": timeframe,
        "direction": direction,
        "technical_score": score,
        "signal_timestamp": now,
        "is_confirmed": True,
        "indicator_version": "goat-toolkit",
        "settings_profile": event_type,
        "event_type": event_type,
        "title": title,
        "raw_tradingview_payload": payload,
    }

    for key in ("avg_score", "price", "exit_pressure", "atr_stop"):
        if key in fields:
            try:
                normalized[key] = _extract_number(fields[key], field_name=key)
            except ValueError:
                normalized[key] = None

    return normalized


def sqlite_schema() -> str:
    return """
    CREATE TABLE IF NOT EXISTS technical_signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        received_at TEXT NOT NULL,
        source TEXT NOT NULL,
        symbol TEXT NOT NULL,
        exchange TEXT,
        timeframe TEXT NOT NULL,
        direction TEXT NOT NULL,
        technical_score REAL NOT NULL,
        signal_timestamp TEXT NOT NULL,
        bar_close_timestamp TEXT,
        is_confirmed INTEGER NOT NULL DEFAULT 0,
        indicator_version TEXT,
        settings_profile TEXT,
        fingerprint TEXT NOT NULL UNIQUE,
        raw_payload TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_technical_symbol_tf_time
        ON technical_signals(symbol, timeframe, signal_timestamp);
    CREATE INDEX IF NOT EXISTS idx_technical_received_at
        ON technical_signals(received_at);
    """


def postgres_schema() -> str:
    return """
    CREATE TABLE IF NOT EXISTS technical_signals (
        id BIGSERIAL PRIMARY KEY,
        received_at TIMESTAMPTZ NOT NULL,
        source TEXT NOT NULL,
        symbol TEXT NOT NULL,
        exchange TEXT,
        timeframe TEXT NOT NULL,
        direction TEXT NOT NULL,
        technical_score DOUBLE PRECISION NOT NULL,
        signal_timestamp TIMESTAMPTZ NOT NULL,
        bar_close_timestamp TIMESTAMPTZ,
        is_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
        indicator_version TEXT,
        settings_profile TEXT,
        fingerprint TEXT NOT NULL UNIQUE,
        raw_payload JSONB NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_technical_symbol_tf_time
        ON technical_signals(symbol, timeframe, signal_timestamp);
    CREATE INDEX IF NOT EXISTS idx_technical_received_at
        ON technical_signals(received_at);
    """
