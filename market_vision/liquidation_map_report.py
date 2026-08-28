"""Deterministic Hebrew formatter for CoinGlass liquidation-map scan results."""

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
    "above": "יותר ריכוזי ליקווידציה מעל המחיר",
    "below": "יותר ריכוזי ליקווידציה מתחת למחיר",
    "balanced": "מאוזן יחסית",
    "unclear": "לא חד-משמעי",
}

_CONSENSUS_HE = {
    "above": "רוב המפות מציגות ריכוז חזק יותר מעל המחיר",
    "below": "רוב המפות מציגות ריכוז חזק יותר מתחת למחיר",
    "mixed": "המפות אינן מסכימות על צד דומיננטי",
    "unclear": "אין קונצנזוס חזותי ברור",
}

_MAP_LABELS = {
    "binance_btc_usdt": "Binance BTC/USDT",
    "bitcoin_exchange_aggregate": "Bitcoin Exchange Aggregate",
    "hyperliquid_btc": "Hyperliquid BTC",
}


def _money(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "לא זמין"
    return f"${value:,.0f}" if value >= 1000 else f"${value:,.2f}"


def _level_text(level: Mapping[str, Any] | None) -> str:
    if not level:
        return "לא זוהה בביטחון מספיק"
    low = level.get("low_price")
    high = level.get("high_price")
    if isinstance(low, (int, float)) and isinstance(high, (int, float)):
        price = _money(low) if abs(high - low) < 1e-9 else f"{_money(low)}–{_money(high)}"
    elif isinstance(low, (int, float)):
        price = f"סביב {_money(low)}"
    elif isinstance(high, (int, float)):
        price = f"סביב {_money(high)}"
    else:
        price = "טווח מחיר לא קריא"

    strength = _STRENGTH_HE.get(str(level.get("relative_strength")), "")
    confidence = _CONFIDENCE_HE.get(str(level.get("confidence")), "")
    note = str(level.get("note") or "").strip()
    details = []
    if strength:
        details.append(f"עוצמה {strength}")
    if confidence:
        details.append(f"אמינות {confidence}")
    if note:
        details.append(note)
    return price + (" | " + " | ".join(details) if details else "")


def _map_block(map_key: str, scan: Mapping[str, Any]) -> list[str]:
    label = _MAP_LABELS.get(map_key, str(scan.get("title") or map_key))
    timeframe = str(scan.get("timeframe") or "").strip()
    price = _money(scan.get("current_price_estimate"))
    price_conf = _CONFIDENCE_HE.get(str(scan.get("current_price_confidence")), "לא ידועה")
    above = scan.get("above_price") if isinstance(scan.get("above_price"), Mapping) else {}
    below = scan.get("below_price") if isinstance(scan.get("below_price"), Mapping) else {}
    dominance = _DOMINANCE_HE.get(str(scan.get("dominant_side")), "לא ידוע")
    dominance_conf = _CONFIDENCE_HE.get(str(scan.get("dominance_confidence")), "לא ידועה")

    heading = f"📍 {label}"
    if timeframe:
        heading += f" | {timeframe}"
    lines = [
        heading,
        f"💰 מחיר משוער: {price} | אמינות {price_conf}",
        f"⬆️ הריכוז החזק מעל: {_level_text(above.get('strongest_level'))}",
        f"⬇️ הריכוז החזק מתחת: {_level_text(below.get('strongest_level'))}",
    ]

    above_score = above.get("visual_intensity_score")
    below_score = below.get("visual_intensity_score")
    if isinstance(above_score, (int, float)) and isinstance(below_score, (int, float)):
        lines.append(
            f"⚖️ מאזן חזותי: {dominance} | מעל {above_score:.1f}/10, מתחת {below_score:.1f}/10 | אמינות {dominance_conf}"
        )
    else:
        lines.append(f"⚖️ מאזן חזותי: {dominance} | אמינות {dominance_conf}")

    points = scan.get("points_of_interest") or []
    if isinstance(points, list) and points:
        lines.append("🎯 נקודות עניין: " + " | ".join(str(item) for item in points[:4]))

    summary = str(scan.get("short_summary") or "").strip()
    if summary:
        lines.append(f"📝 {summary}")
    return lines


def build_liquidation_map_report(result: Mapping[str, Any]) -> str:
    symbol = str(result.get("symbol") or "BTC")
    generated_at = str(result.get("scan_generated_at_utc") or "").strip()
    maps = result.get("maps") if isinstance(result.get("maps"), Mapping) else {}

    lines = [f"🗺️ {symbol} — CoinGlass Liquidation Maps"]
    if generated_at:
        lines.append(f"🕒 סריקה: {generated_at}")

    for map_key in ("binance_btc_usdt", "bitcoin_exchange_aggregate", "hyperliquid_btc"):
        scan = maps.get(map_key) if isinstance(maps, Mapping) else None
        if not isinstance(scan, Mapping):
            continue
        lines.append("")
        lines.extend(_map_block(map_key, scan))

    cross = result.get("cross_map")
    if isinstance(cross, Mapping):
        lines.append("")
        lines.append("🔗 השוואה בין המפות")
        consensus = _CONSENSUS_HE.get(str(cross.get("consensus")), "לא ידוע")
        confidence = _CONFIDENCE_HE.get(str(cross.get("confidence")), "לא ידועה")
        lines.append(f"• קונצנזוס חזותי: {consensus} | אמינות {confidence}")
        shared = cross.get("shared_observations") or []
        if isinstance(shared, list) and shared:
            lines.append("• משותף: " + " | ".join(str(item) for item in shared[:5]))
        disagreements = cross.get("disagreements") or []
        if isinstance(disagreements, list) and disagreements:
            lines.append("• הבדלים: " + " | ".join(str(item) for item in disagreements[:5]))
        summary = str(cross.get("summary") or "").strip()
        if summary:
            lines.append(f"• תמונה כוללת: {summary}")

    exact_values_available = False
    if isinstance(maps, Mapping):
        exact_values_available = any(
            isinstance(scan, Mapping) and bool(scan.get("exact_dollar_values_available"))
            for scan in maps.values()
        )

    lines.append("")
    if exact_values_available:
        lines.append("💵 סכומי $ נכללים רק כאשר הם מופיעים מפורשות במפה הרלוונטית.")
    else:
        lines.append("⚠️ אין להסיק סכומי $ מדויקים מעוצמת הגרפים בלבד; העוצמות בדוח יחסיות וחזותיות.")
    lines.append("ℹ️ הדוח תצפיתי בלבד ואינו תחזית או המלצת מסחר.")
    return "\n".join(lines).rstrip() + "\n"

