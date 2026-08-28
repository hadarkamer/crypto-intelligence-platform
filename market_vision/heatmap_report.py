"""Human-readable report formatter for CoinGlass heatmap scan results.

The formatter is deterministic and does not call an LLM. It turns the structured
vision result into a concise Hebrew market-scanner report while preserving the
observational-only nature of the POC.
"""

from __future__ import annotations

from typing import Any, Mapping


_STRENGTH_HE = {
    "very_strong": "חזקה מאוד",
    "strong": "חזקה",
    "medium": "בינונית",
    "weak": "חלשה",
}

_CONFIDENCE_HE = {
    "high": "גבוהה",
    "medium": "בינונית",
    "low": "נמוכה",
}

_DOMINANCE_HE = {
    "above": "יותר נזילות מעל המחיר",
    "below": "יותר נזילות מתחת למחיר",
    "balanced": "מאוזן יחסית",
    "unclear": "לא חד-משמעי",
}


def _money(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "לא זמין"
    if value >= 1000:
        return f"${value:,.0f}"
    return f"${value:,.2f}"


def _zone_text(zone: Mapping[str, Any] | None) -> str:
    if not zone:
        return "לא זוהה בביטחון מספיק"
    low = zone.get("low_price")
    high = zone.get("high_price")
    strength = _STRENGTH_HE.get(str(zone.get("relative_strength")), str(zone.get("relative_strength") or ""))
    confidence = _CONFIDENCE_HE.get(str(zone.get("confidence")), str(zone.get("confidence") or ""))

    if isinstance(low, (int, float)) and isinstance(high, (int, float)):
        if abs(high - low) < 1e-9:
            price = _money(low)
        else:
            price = f"{_money(low)}–{_money(high)}"
    elif isinstance(low, (int, float)):
        price = f"סביב {_money(low)}"
    elif isinstance(high, (int, float)):
        price = f"סביב {_money(high)}"
    else:
        price = "טווח מחיר לא קריא"

    suffix = f" | עוצמה {strength}" if strength else ""
    if confidence:
        suffix += f" | אמינות {confidence}"
    return f"{price}{suffix}"


def _secondary_text(side: Mapping[str, Any]) -> str:
    zones = side.get("secondary_zones") or []
    if not isinstance(zones, list) or not zones:
        return "אין מוקד משני ברור"
    parts = [_zone_text(zone) for zone in zones[:3] if isinstance(zone, Mapping)]
    return "; ".join(parts) if parts else "אין מוקד משני ברור"


def _scan_block(scan: Mapping[str, Any]) -> list[str]:
    timeframe = str(scan.get("timeframe") or "?")
    price = _money(scan.get("current_price_estimate"))
    price_conf = _CONFIDENCE_HE.get(
        str(scan.get("current_price_confidence")),
        str(scan.get("current_price_confidence") or "לא ידועה"),
    )
    above = scan.get("above_price") if isinstance(scan.get("above_price"), Mapping) else {}
    below = scan.get("below_price") if isinstance(scan.get("below_price"), Mapping) else {}
    dominance = _DOMINANCE_HE.get(
        str(scan.get("dominant_side")),
        str(scan.get("dominant_side") or "לא ידוע"),
    )
    dominance_conf = _CONFIDENCE_HE.get(
        str(scan.get("dominance_confidence")),
        str(scan.get("dominance_confidence") or "לא ידועה"),
    )

    lines = [
        f"⏱ {timeframe} | מחיר משוער: {price} | אמינות מחיר: {price_conf}",
        f"⬆️ מוקד עיקרי מעל: {_zone_text(above.get('main_zone') if isinstance(above, Mapping) else None)}",
        f"⬇️ מוקד עיקרי מתחת: {_zone_text(below.get('main_zone') if isinstance(below, Mapping) else None)}",
    ]

    above_score = above.get("visual_intensity_score") if isinstance(above, Mapping) else None
    below_score = below.get("visual_intensity_score") if isinstance(below, Mapping) else None
    if isinstance(above_score, (int, float)) and isinstance(below_score, (int, float)):
        lines.append(
            f"⚖️ מאזן חזותי: {dominance} | עוצמה יחסית מעל {above_score:.1f}/10, מתחת {below_score:.1f}/10 | אמינות {dominance_conf}"
        )
    else:
        lines.append(f"⚖️ מאזן חזותי: {dominance} | אמינות {dominance_conf}")

    lines.append(f"↗️ מוקדים משניים מעל: {_secondary_text(above)}")
    lines.append(f"↘️ מוקדים משניים מתחת: {_secondary_text(below)}")

    short_summary = str(scan.get("short_summary") or "").strip()
    if short_summary:
        lines.append(f"📝 סיכום AI: {short_summary}")
    return lines


def build_heatmap_report(result: Mapping[str, Any]) -> str:
    """Create a concise Hebrew report from the structured heatmap result."""
    symbol = str(result.get("symbol") or "BTC")
    generated_at = str(result.get("scan_generated_at_utc") or "").strip()
    scans = result.get("scans") or []

    lines = [f"📊 {symbol} — CoinGlass Liquidation Heatmap"]
    if generated_at:
        lines.append(f"🕒 סריקה: {generated_at}")
    lines.append("")

    if isinstance(scans, list):
        for index, scan in enumerate(scans):
            if not isinstance(scan, Mapping):
                continue
            if index:
                lines.append("")
            lines.extend(_scan_block(scan))

    cross = result.get("cross_timeframe")
    if isinstance(cross, Mapping) and cross.get("available"):
        lines.append("")
        lines.append("🔗 חפיפה בין 12h ל־24h")
        shared = cross.get("shared_zones") or []
        if isinstance(shared, list) and shared:
            lines.append("• אזורים משותפים: " + " | ".join(str(item) for item in shared[:5]))
        changes = cross.get("main_changes") or []
        if isinstance(changes, list) and changes:
            lines.append("• הבדלים עיקריים: " + " | ".join(str(item) for item in changes[:5]))
        summary = str(cross.get("summary") or "").strip()
        if summary:
            lines.append(f"• תמונה כוללת: {summary}")

    exact_totals_available = any(
        isinstance(scan, Mapping) and bool(scan.get("exact_dollar_totals_available"))
        for scan in scans
    ) if isinstance(scans, list) else False

    lines.append("")
    if exact_totals_available:
        lines.append("💵 סכומי נזילות מדויקים: זמינים רק היכן שהם מופיעים מפורשות במקור.")
    else:
        lines.append("⚠️ סכומי $ מדויקים לכל צד אינם ניתנים להסקה מהצבעים בלבד; העוצמות בדוח הן יחסיות וחזותיות.")
    lines.append("ℹ️ הדוח מתאר את המפה הנצפית ואינו תחזית מחיר או המלצת מסחר.")

    return "\n".join(lines).rstrip() + "\n"

