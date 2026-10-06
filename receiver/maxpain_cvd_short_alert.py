"""Pure selector for the dedicated MP65 + total CVD + short-Futures alert.

The caller supplies *only* actual MAX_PAIN_SCORE_65 transition opportunities
from one Watch, in their original delivery order.  This module neither fetches
data nor sends messages.  It selects the first eligible MP65/both-total-CVD
opportunity per symbol, THEN applies the short-family filter.  A later card
cannot replace that first card when the short-family filter fails.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from html import escape
import math
from typing import Any, Iterable, Mapping, Optional
from zoneinfo import ZoneInfo


FORMULA_ID = "FORMULA_MP65_CVD_SHORT"
FORMULA_VERSION = "mp65-cvd-short-v1"
_ISRAEL = ZoneInfo("Asia/Jerusalem")
_PRICE_DIRECTION = {"SHORT": "LONG", "LONG": "SHORT"}
_FLOW_DIRECTION = {"LONG": "BULLISH", "SHORT": "BEARISH"}
_TARGET_DIRECTIONS = {
    "LONG": "LONG", "UP": "LONG", "BULL": "LONG", "BULLISH": "LONG", "BUY": "LONG",
    "SHORT": "SHORT", "DOWN": "SHORT", "BEAR": "SHORT", "BEARISH": "SHORT", "SELL": "SHORT",
}


@dataclass(frozen=True)
class FormulaMatch:
    item: Mapping[str, Any]
    symbol: str
    direction: str  # Expected PRICE direction, already inverted from pain side.
    source_side: str
    timeframe: str
    maxpain_score: float
    futures_score: float  # Signed TOTAL family score, not a window score.
    spot_score: float
    short_quality_pct: float
    short_direction: str
    reference_price: float


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: Any, lower: float, upper: float) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number) or not lower <= number <= upper:
        return None
    return number


def _usable_total(module: Mapping[str, Any], direction: str) -> Optional[float]:
    # These fields are emitted by market_confidence_engine._flow_module.
    # WARNING is usable there (its score has already been reduced); do not
    # apply a second penalty. Missing/stale/invalid evidence must not fire.
    if module.get("available") is not True:
        return None
    if str(module.get("quality_status") or "").upper() not in {"PASS", "WARNING"}:
        return None
    if str(module.get("freshness_status") or "").upper() != "FRESH":
        return None
    if str(module.get("direction") or "").upper() != _FLOW_DIRECTION[direction]:
        return None
    score = _number(module.get("score"), -100.0, 100.0)
    sign = 1.0 if direction == "LONG" else -1.0
    return score if score is not None and score * sign > 65.0 else None


def select_matches(score65_transition_items: Iterable[Mapping[str, Any]]) -> list[FormulaMatch]:
    """Return at most one matching original opportunity per symbol per Watch.

    ``side`` is the Max Pain side and is inverted exactly once here. Any
    existing ``direction`` field is deliberately not used for that inversion.
    Total CVD thresholds are strictly >65; MP and short quality include 65.
    The calling integration is responsible for the SCORE_65 event lineage.
    """
    selected_symbols: set[str] = set()
    result: list[FormulaMatch] = []
    for raw_item in score65_transition_items:
        item = _mapping(raw_item)
        symbol = str(item.get("symbol") or "").strip().upper()
        side = str(item.get("side") or "").upper()
        direction = _PRICE_DIRECTION.get(side)
        score = _number(item.get("score"), 0.0, 100.0)
        if not symbol or symbol in selected_symbols or direction is None or score is None or score < 65.0:
            continue
        modules = _mapping(_mapping(item.get("market_evidence")).get("modules"))
        futures = _mapping(modules.get("futures_flow"))
        spot = _mapping(modules.get("spot_flow"))
        futures_score = _usable_total(futures, direction)
        spot_score = _usable_total(spot, direction)
        if futures_score is None or spot_score is None:
            continue

        # Freeze the first both-CVD eligible item BEFORE inspecting short
        # quality or reference-price validity. No substitution by later cards.
        selected_symbols.add(symbol)
        short = _mapping(_mapping(futures.get("time_families")).get("short"))
        short_quality = _number(short.get("quality"), 0.0, 1.0)
        short_direction = str(short.get("direction") or "").upper()
        reference_price = _number(item.get("current_price"), 0.0, float("inf"))
        if short_quality is None or short_quality < 0.65:
            continue
        if short_direction != _FLOW_DIRECTION[direction]:
            continue
        if reference_price is None or reference_price <= 0.0:
            continue
        # Event capture prefers explicit target_direction / target_price over
        # pain-side inversion. Reject contradictory evidence so the displayed
        # direction and the captured research event cannot disagree.
        explicit_direction = str(item.get("target_direction") or "").strip().upper()
        if explicit_direction and _TARGET_DIRECTIONS.get(explicit_direction) != direction:
            continue
        if item.get("target_price") not in (None, ""):
            target_price = _number(item.get("target_price"), 0.0, float("inf"))
            if target_price is None or target_price <= 0.0:
                continue
            if (target_price - reference_price) * (1 if direction == "LONG" else -1) < 0:
                continue
        result.append(FormulaMatch(
            item=item,
            symbol=symbol,
            direction=direction,
            source_side=side,
            timeframe=str(item.get("timeframe") or ""),
            maxpain_score=score,
            futures_score=futures_score,
            spot_score=spot_score,
            short_quality_pct=short_quality * 100.0,
            short_direction=short_direction,
            reference_price=reference_price,
        ))
    return result


def _score_text(value: float, signed: bool = False) -> str:
    text = f"{value:+.4f}" if signed else f"{value:.4f}"
    return text.rstrip("0").rstrip(".")


def render_message(match: FormulaMatch, decision_time: datetime) -> str:
    """Render Telegram HTML. The caller must supply an aware decision time."""
    if not isinstance(decision_time, datetime) or decision_time.utcoffset() is None:
        raise ValueError("decision_time must be a timezone-aware datetime")
    direction_he = "לונג — עלייה" if match.direction == "LONG" else "שורט — ירידה"
    short_he = "שורי" if match.short_direction == "BULLISH" else "דובי"
    price = f"{match.reference_price:.10f}".rstrip("0").rstrip(".")
    source = str(match.item.get("price_source") or "").strip()
    source_line = f"\nמקור מחיר: {escape(source)}" if source else ""
    return (
        "🧪 <b>ניסיוני — התאמה לנוסחת מחקר</b>\n"
        f"<b>{escape(match.symbol)} | {direction_he}</b>\n"
        "Max Pain 65+ · שני CVD כוללים מעל 65 · Futures קצר 65+\n\n"
        f"ציון Max Pain: <b>{_score_text(match.maxpain_score)}</b>"
        f" | טווח: {escape(match.timeframe) or 'לא צוין'}\n"
        f"Futures CVD — ציון כולל: <b>{_score_text(match.futures_score, signed=True)}</b>\n"
        f"Spot CVD — ציון כולל: <b>{_score_text(match.spot_score, signed=True)}</b>\n"
        f"Futures קצר (1h + 4h): <b>{_score_text(match.short_quality_pct)}</b> — {short_he}\n"
        f"מחיר ייחוס: <b>{price}</b>{source_line}\n"
        f"זמן הבדיקה בישראל: {decision_time.astimezone(_ISRAEL):%d.%m.%Y %H:%M:%S}\n\n"
        "הכיוון לעיל הוא כיוון המחיר הצפוי לאחר היפוך צד ה־Max Pain.\n"
        "זו התאמה לתנאי הנוסחה; הסתברות ואסימטריה אינן נקבעות מהופעה אחת."
    )
