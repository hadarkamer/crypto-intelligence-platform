"""Stage 77: independent Price + Open Interest market-regime layer.

This module deliberately does NOT modify the existing alert/Max-Pain score.
It records aggregated futures Open Interest from CoinGlass together with the
bot's live market price every 30 minutes and classifies the sign relationship
between the two changes.

There are no hand-tuned market thresholds and no intensity grades in Stage 77.
Magnitude is stored as raw percentage change so that thresholds can be learned
later from the bot's own history instead of being invented up front.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import math
import os
from pathlib import Path
import sqlite3
from typing import Any, Dict, List, Optional

import requests

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - optional locally
    psycopg = None
    dict_row = None


API_BASE_URL = "https://open-api-v4.coinglass.com"
OI_ENDPOINT = "/api/futures/open-interest/exchange-list"
API_TIMEOUT_SECONDS = 15
COLLECTION_INTERVAL_MINUTES = 30
# Keep enough history to calibrate coin-specific magnitude bands later.
HISTORY_RETENTION_DAYS = 60
DATABASE_URL = os.getenv("DATABASE_URL", "")
DB_PATH = os.getenv("DB_PATH", "data/coinglass.db")

SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS oi_regime_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    collected_at TEXT NOT NULL,
    symbol TEXT NOT NULL,
    price REAL NOT NULL,
    open_interest_usd REAL NOT NULL,
    price_change_pct REAL,
    oi_change_pct REAL,
    state TEXT NOT NULL,
    direction TEXT NOT NULL,
    reason TEXT NOT NULL,
    UNIQUE(collected_at, symbol)
);
CREATE INDEX IF NOT EXISTS idx_oi_regime_symbol_time
ON oi_regime_snapshots(symbol, collected_at);
"""

POSTGRES_SCHEMA = """
CREATE TABLE IF NOT EXISTS oi_regime_snapshots (
    id BIGSERIAL PRIMARY KEY,
    collected_at TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    open_interest_usd DOUBLE PRECISION NOT NULL,
    price_change_pct DOUBLE PRECISION,
    oi_change_pct DOUBLE PRECISION,
    state TEXT NOT NULL,
    direction TEXT NOT NULL,
    reason TEXT NOT NULL,
    UNIQUE(collected_at, symbol)
);
CREATE INDEX IF NOT EXISTS idx_oi_regime_symbol_time
ON oi_regime_snapshots(symbol, collected_at);
"""


@dataclass(frozen=True)
class RegimeResult:
    symbol: str
    state: str
    label: str
    direction: str
    price_change_pct: Optional[float]
    oi_change_pct: Optional[float]
    reason: str
    available: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _api_key() -> str:
    """Read at call time so Render env and local dotenv both work."""
    return os.getenv("COINGLASS_API_KEY", "").strip()


def _use_postgres() -> bool:
    return bool(DATABASE_URL and psycopg)


def init_db() -> None:
    if _use_postgres():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            conn.execute(POSTGRES_SCHEMA)
            conn.commit()
    else:
        Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(DB_PATH) as conn:
            conn.executescript(SQLITE_SCHEMA)
            conn.commit()


def _pct_change(new: float, old: float) -> Optional[float]:
    if old is None or float(old) == 0.0:
        return None
    return (float(new) - float(old)) / float(old) * 100.0


def _is_zero(value: float) -> bool:
    """Numerical zero only; this is not a market-significance threshold."""
    return math.isclose(float(value), 0.0, rel_tol=0.0, abs_tol=1e-12)


def classify(
    symbol: str,
    price_change_pct: Optional[float],
    oi_change_pct: Optional[float],
) -> RegimeResult:
    """Classify one of the five agreed Price+OI states.

    Stage 77 uses direction/sign only. It intentionally does not claim that a
    particular percentage is 'small', 'large' or 'extreme'.
    """
    symbol = str(symbol or "").upper()
    if price_change_pct is None or oi_change_pct is None:
        return RegimeResult(
            symbol=symbol,
            state="UNAVAILABLE",
            label="אין מספיק היסטוריה",
            direction="NEUTRAL",
            price_change_pct=price_change_pct,
            oi_change_pct=oi_change_pct,
            reason="נדרשות לפחות שתי דגימות מחיר ו-OI להשוואה.",
            available=False,
        )

    p = float(price_change_pct)
    o = float(oi_change_pct)

    # Fifth state: one or both series were effectively flat between snapshots.
    # No arbitrary 'meaningful change' cutoff is used.
    if _is_zero(p) or _is_zero(o):
        flat_parts = []
        if _is_zero(p):
            flat_parts.append("המחיר ללא שינוי")
        if _is_zero(o):
            flat_parts.append("ה-OI ללא שינוי")
        return RegimeResult(
            symbol=symbol,
            state="NEUTRAL_INCONCLUSIVE",
            label="Neutral / Inconclusive",
            direction="NEUTRAL",
            price_change_pct=round(p, 6),
            oi_change_pct=round(o, 6),
            reason="; ".join(flat_parts) + "; אין מסקנת Price+OI כיוונית.",
        )

    if p > 0 and o > 0:
        state = "BULLISH_BUILDUP"
        label = "Bullish Build-up"
        direction = "LONG"
        reason = "המחיר וה-OI עלו יחד: התנועה מלווה בגידול בפוזיציות פתוחות."
    elif p < 0 and o > 0:
        state = "BEARISH_BUILDUP"
        label = "Bearish Build-up"
        direction = "SHORT"
        reason = "המחיר ירד וה-OI עלה: התנועה מלווה בגידול בפוזיציות פתוחות."
    elif p > 0 and o < 0:
        state = "SHORT_COVERING"
        label = "Short Covering"
        direction = "LONG"
        reason = "המחיר עלה בזמן שה-OI ירד: התנועה מתאימה לסגירת פוזיציות, ולא לבניית OI חדש."
    else:
        state = "LONG_UNWINDING"
        label = "Long Unwinding"
        direction = "SHORT"
        reason = "המחיר ירד בזמן שה-OI ירד: התנועה מתאימה לסגירת פוזיציות, ולא לבניית OI חדש."

    return RegimeResult(
        symbol=symbol,
        state=state,
        label=label,
        direction=direction,
        price_change_pct=round(p, 6),
        oi_change_pct=round(o, 6),
        reason=reason,
    )


def fetch_aggregated_oi(symbol: str) -> float:
    """Fetch aggregate futures OI (USD) across exchanges from CoinGlass V4."""
    api_key = _api_key()
    if not api_key:
        raise RuntimeError("COINGLASS_API_KEY is not configured")

    response = requests.get(
        API_BASE_URL + OI_ENDPOINT,
        params={"symbol": str(symbol or "").upper()},
        headers={"CG-API-KEY": api_key, "accept": "application/json"},
        timeout=API_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or str(payload.get("code")) not in {"0", "200"}:
        message = payload.get("msg") if isinstance(payload, dict) else "invalid response"
        raise RuntimeError(f"CoinGlass API error: {message!r}")

    rows = payload.get("data") or []
    if not isinstance(rows, list):
        raise ValueError(f"CoinGlass returned invalid OI data for {symbol}")

    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("exchange", "")).strip().lower() != "all":
            continue
        try:
            value = float(row.get("open_interest_usd"))
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value

    raise ValueError(f"CoinGlass returned no aggregated OI for {symbol}")


def _previous_snapshot(symbol: str) -> Optional[Dict[str, float]]:
    init_db()
    symbol = str(symbol or "").upper()
    if _use_postgres():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            row = conn.execute(
                "SELECT price, open_interest_usd FROM oi_regime_snapshots "
                "WHERE symbol=%s ORDER BY collected_at DESC LIMIT 1",
                (symbol,),
            ).fetchone()
    else:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT price, open_interest_usd FROM oi_regime_snapshots "
                "WHERE symbol=? ORDER BY collected_at DESC LIMIT 1",
                (symbol,),
            ).fetchone()
    return dict(row) if row else None


def _insert_snapshot(
    symbol: str,
    price: float,
    oi: float,
    result: RegimeResult,
) -> None:
    init_db()
    now = datetime.now(timezone.utc)
    collected_at = now if _use_postgres() else now.isoformat()
    params = (
        collected_at,
        str(symbol).upper(),
        float(price),
        float(oi),
        result.price_change_pct,
        result.oi_change_pct,
        result.state,
        result.direction,
        result.reason,
    )
    sql = (
        "INSERT INTO oi_regime_snapshots "
        "(collected_at,symbol,price,open_interest_usd,price_change_pct,oi_change_pct,state,direction,reason) "
        "VALUES (?,?,?,?,?,?,?,?,?)"
    )

    if _use_postgres():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            conn.execute(sql.replace("?", "%s"), params)
            conn.execute(
                "DELETE FROM oi_regime_snapshots WHERE collected_at < %s",
                (now - timedelta(days=HISTORY_RETENTION_DAYS),),
            )
            conn.commit()
    else:
        cutoff = (now - timedelta(days=HISTORY_RETENTION_DAYS)).isoformat()
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(sql, params)
            conn.execute(
                "DELETE FROM oi_regime_snapshots WHERE collected_at < ?",
                (cutoff,),
            )
            conn.commit()


def collect_symbol(symbol: str, price: float) -> Dict[str, Any]:
    symbol = str(symbol or "").upper()
    oi = fetch_aggregated_oi(symbol)
    previous = _previous_snapshot(symbol)
    price_delta = _pct_change(float(price), previous["price"]) if previous else None
    oi_delta = _pct_change(float(oi), previous["open_interest_usd"]) if previous else None
    result = classify(symbol, price_delta, oi_delta)
    _insert_snapshot(symbol, float(price), float(oi), result)

    payload = result.to_dict()
    payload.update(
        {
            "price": float(price),
            "open_interest_usd": float(oi),
            "window_minutes": COLLECTION_INTERVAL_MINUTES,
        }
    )
    return payload


def collect_many(symbol_prices: Dict[str, float]) -> Dict[str, Dict[str, Any]]:
    """Collect each symbol independently; one API failure never stops alerts."""
    results: Dict[str, Dict[str, Any]] = {}
    for symbol, price in sorted(symbol_prices.items()):
        try:
            results[symbol] = collect_symbol(symbol, price)
        except Exception as exc:
            results[symbol] = {
                "symbol": symbol,
                "state": "UNAVAILABLE",
                "label": "נתוני Price + OI לא זמינים",
                "direction": "NEUTRAL",
                "price_change_pct": None,
                "oi_change_pct": None,
                "reason": str(exc),
                "available": False,
                "window_minutes": COLLECTION_INTERVAL_MINUTES,
            }
    return results


def latest(symbol: str) -> Dict[str, Any]:
    init_db()
    symbol = str(symbol or "").upper()
    sql = "SELECT * FROM oi_regime_snapshots WHERE symbol=? ORDER BY collected_at DESC LIMIT 1"
    if _use_postgres():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            row = conn.execute(sql.replace("?", "%s"), (symbol,)).fetchone()
    else:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(sql, (symbol,)).fetchone()

    if not row:
        return {
            "symbol": symbol,
            "state": "UNAVAILABLE",
            "label": "אין נתוני Price + OI",
            "direction": "NEUTRAL",
            "available": False,
            "reason": "טרם נאספו שתי דגימות מחיר ו-OI.",
            "window_minutes": COLLECTION_INTERVAL_MINUTES,
        }

    result = dict(row)
    result["label"] = _state_label(str(result.get("state") or ""))
    result["available"] = result.get("state") != "UNAVAILABLE"
    result["window_minutes"] = COLLECTION_INTERVAL_MINUTES
    return result


def _state_label(state: str) -> str:
    return {
        "BULLISH_BUILDUP": "Bullish Build-up",
        "BEARISH_BUILDUP": "Bearish Build-up",
        "SHORT_COVERING": "Short Covering",
        "LONG_UNWINDING": "Long Unwinding",
        "NEUTRAL_INCONCLUSIVE": "Neutral / Inconclusive",
        "UNAVAILABLE": "אין מספיק נתונים",
    }.get(state, state or "לא ידוע")


def composite_conclusion(regime: Dict[str, Any], alert_side: str) -> str:
    """Third layer: textual relationship only; no numeric score is altered."""
    side = str(alert_side or "").upper()
    state = str(regime.get("state") or "")

    if not regime.get("available"):
        return "אין כרגע נתוני Price+OI להשוואה; הציון הקיים נשאר עצמאי."
    if state == "NEUTRAL_INCONCLUSIVE":
        return "Price+OI ניטרלי; אין אישור או התנגדות לכיוון שנבחר."

    if state == "BULLISH_BUILDUP":
        return (
            "Price+OI תומך בכיוון LONG: מחיר ו-OI עולים יחד."
            if side == "LONG"
            else "Price+OI מנוגד לכיוון SHORT: מחיר ו-OI עולים יחד."
        )
    if state == "BEARISH_BUILDUP":
        return (
            "Price+OI תומך בכיוון SHORT: המחיר יורד בזמן שה-OI עולה."
            if side == "SHORT"
            else "Price+OI מנוגד לכיוון LONG: המחיר יורד בזמן שה-OI עולה."
        )
    if state == "SHORT_COVERING":
        if side == "LONG":
            return "המחיר עולה, אך ה-OI יורד: התנועה בכיוון LONG ללא אישור של בניית OI חדש."
        return "המחיר עולה בזמן שה-OI יורד; מצב Price+OI מנוגד כרגע לכיוון SHORT."
    if state == "LONG_UNWINDING":
        if side == "SHORT":
            return "המחיר יורד, אך ה-OI יורד: התנועה בכיוון SHORT ללא אישור של בניית OI חדש."
        return "המחיר יורד בזמן שה-OI יורד; מצב Price+OI מנוגד כרגע לכיוון LONG."

    return "מצב Price+OI זמין אך ללא מסקנה משולבת."


def attach_to_opportunities(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Attach independent regime data without changing any Stage 76 score."""
    cache: Dict[str, Dict[str, Any]] = {}
    for item in items:
        symbol = str(item.get("symbol", "")).upper()
        if symbol not in cache:
            cache[symbol] = latest(symbol)
        regime = cache[symbol]
        item["market_regime"] = regime
        item["composite_conclusion"] = composite_conclusion(
            regime,
            str(item.get("side", "")),
        )
    return items
