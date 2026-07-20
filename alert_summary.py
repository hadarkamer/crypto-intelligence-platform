"""Telegram summaries for a displayed batch of alerts."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Iterable, List


def format_alert_count_summary(items: Iterable[Dict[str, Any]]) -> str:
    """Summarize displayed alerts by coin, e.g. BTC: 2 LONG, 1 SHORT."""
    counts: Dict[str, Dict[str, int]] = defaultdict(
        lambda: {"LONG": 0, "SHORT": 0}
    )
    order: List[str] = []

    for item in items:
        symbol = str(item.get("symbol") or "").upper()
        side = str(item.get("side") or "").upper()
        if not symbol or side not in {"LONG", "SHORT"}:
            continue
        if symbol not in order:
            order.append(symbol)
        counts[symbol][side] += 1

    if not order:
        return "📊 סיכום התראות לפי מטבע: אין התראות להצגה."

    lines = ["📊 סיכום התראות לפי מטבע"]
    for symbol in order:
        parts = []
        if counts[symbol]["LONG"]:
            parts.append(f"{counts[symbol]['LONG']} LONG")
        if counts[symbol]["SHORT"]:
            parts.append(f"{counts[symbol]['SHORT']} SHORT")
        lines.append(f"{symbol}: {', '.join(parts)}")
    return "\n".join(lines)
