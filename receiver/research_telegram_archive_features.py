"""Causal, source-line annotated extraction from one Telegram message.

No neighbouring message, scan maximum, later confirmation or label is consulted.
The historical display contract is explicit: MaxPain/Combined display the hurt
side, while OI/CVD display expected price direction. Missing totals stay missing.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import math
import re
from typing import Any, Mapping

from research_telegram_html_archive import _hash, normalize_time

FEATURE_VERSION = "telegram-per-message-causal-features-v1"
DIRECTION_VERSION = "telegram-post-20260815-display-direction-v1"
ENTRY_VERSION = "archive-next-full-minute-binance-spot-open-v1"
TIME_VERSION = "user-israel-wall-clock-dated-cvd-corroboration-v1"
SUPPORTED_SYMBOLS = {"BTC", "BNB", "DOGE", "ETH", "SOL", "XRP", "ZEC", "HYPE"}
INVERSE_DISPLAY_FAMILIES = {"WATCH_CANDIDATE", "COMBINED_CONFIRMATION", "MAX_PAIN_SCORE_CONFIRMATION", "MAX_PAIN_VERIFIED", "MAX_PAIN_STRONG", "MAX_PAIN_SCORE_83_PLUS"}
IGNORED_FAMILIES = {"SCAN_CONTEXT", "OTHER_SOURCE_MESSAGE", "CONTINUATION_UNLINKED"}
NUMBER = r"[+-]?\d[\d,]*(?:\.\d+)?"


def utc(value: Any) -> datetime:
    result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Archive source timestamp must have an explicit offset")
    return result.astimezone(timezone.utc)


def _number(value: str) -> float:
    result = float(value.replace(",", ""))
    if not math.isfinite(result):
        raise ValueError("Non-finite printed value")
    return result


def extract_message(row: Mapping[str, Any], manifest: Mapping[str, Any]) -> dict[str, Any]:
    raw = str(row["message_text"])
    text = re.sub(r"[\u200e\u200f\u202a-\u202e]", "", raw)
    family = row["message_family"]
    features: dict[str, Any] = {}
    evidence: dict[str, str] = {}
    missing: list[str] = []
    conflicts: list[str] = []
    result = {
        "source_identity_key": row["source_identity_key"], "source_revision_sha256": row["source_revision_sha256"],
        "source_message_id": row["source_message_id"], "source_chat_key": row["source_chat_key"],
        "source_scope": "ARCHIVE_ONLY", "record_mode": "ARCHIVE", "message_family": family,
        "feature_version": FEATURE_VERSION, "direction_mapping_version": DIRECTION_VERSION,
        "entry_policy_version": ENTRY_VERSION, "time_policy_version": TIME_VERSION,
        "period_scope_ids": row["period_scope_ids"], "existing_source_links": row["existing_source_links"],
        "raw_time_title": row["raw_time_title"], "header_message_time_utc": row.get("header_message_time_utc"),
        "features": features, "field_source_lines": evidence, "missing_evidence": missing, "conflicts": conflicts,
        "statistical_phase": "DISCOVERY", "live_union_eligible": False,
    }

    def numeric(key: str, pattern: str) -> float | None:
        matches = list(re.finditer(pattern, text, re.MULTILINE))
        values = [_number(match[1]) for match in matches]
        if not values:
            return None
        if len(set(values)) > 1:
            conflicts.append("CONFLICTING_PRINTED_FIELD:" + key)
            return None
        features[key], evidence[key] = values[0], matches[0][0]
        return values[0]

    matches = re.findall(r"(?:נכס\s+|#\d+\s+)([A-Z0-9]+)(?:\s|/|$)", text)
    symbols = set(matches) & SUPPORTED_SYMBOLS
    result["symbol"] = next(iter(symbols)) if len(symbols) == 1 else None
    features["event.event_type"] = family
    if result["symbol"]:
        features["event.symbol"] = result["symbol"]
    if not result["symbol"]:
        missing.append("UNAMBIGUOUS_SYMBOL_IN_SAME_MESSAGE")
    display = re.search(r"(?:נכס\s+[A-Z0-9]+|#\d+\s+[A-Z0-9]+\s*/[^\n|]+)\s*\|\s*[🟢🔴]?\s*(LONG|SHORT)\b", text)
    displayed = display[1] if display else None
    direct_score = None
    if family in {"PRICE_OI_STRENGTH", "FUTURES_CVD_STRENGTH"}:
        direct_score = numeric(("price_oi" if family == "PRICE_OI_STRENGTH" else "futures_cvd") + ".signed_total_score", rf"^[🟢🔴]?\s*ציון\s*({NUMBER})/100\s*$")
        if direct_score is not None and direct_score != 0:
            displayed = "LONG" if direct_score > 0 else "SHORT"
    elif family == "SPOT_CVD_STRONG":
        sides = set(re.findall(r"^([🟢🔴])\s*[^\n]+\|\s*עוצמה", text, re.MULTILINE))
        displayed = ("LONG" if next(iter(sides)) == "🟢" else "SHORT") if len(sides) == 1 else None
        numeric("spot_cvd.group_score", rf"\|\s*עוצמה\s*({NUMBER})/100")
        group = re.search(r"^[🟢🔴]\s*([^|\n]+)\|\s*עוצמה", text, re.MULTILINE)
        if group:
            features["spot_cvd.group_name"] = group[1].strip()
    result["displayed_direction"] = displayed
    result["analysis_direction"] = ("SHORT" if displayed == "LONG" else "LONG") if displayed and family in INVERSE_DISPLAY_FAMILIES else displayed
    result["display_direction_inverted"] = family in INVERSE_DISPLAY_FAMILIES
    if not displayed:
        missing.append("EXPLICIT_DIRECTION_IN_SAME_MESSAGE")

    for name, pattern in (
        ("price_oi", rf"מחיר\+OI:[^\n]*?ציון\s*({NUMBER})/100"),
        ("futures_cvd", rf"סיכום Futures:\s*ציון\s*({NUMBER})/100"),
        ("spot_cvd", rf"סיכום Spot:\s*ציון\s*({NUMBER})/100"),
    ):
        score = numeric(name + ".signed_total_score", pattern)
        if score is None:
            score = features.get(name + ".signed_total_score")
        if score is not None:
            side = "LONG" if score > 0 else "SHORT" if score < 0 else "NEUTRAL"
            features[name + ".total_score"] = abs(score)
            features[name + ".direction"] = side
            if result["analysis_direction"]:
                features[name + ".aligned_score"] = abs(score) if side == result["analysis_direction"] else -abs(score)

    if family in INVERSE_DISPLAY_FAMILIES:
        numeric("maxpain.selected_score", rf"^#\d+\s+[A-Z0-9]+\s*/[^\n|]+\|\s*[🟢🔴]\s*(?:LONG|SHORT)\s*\|\s*({NUMBER})\s*$")
        if family.startswith("MAX_PAIN"):
            numeric("maxpain.selected_score", rf"^ציון:?\s*({NUMBER})(?:/100)?\s*$")
        numeric("maxpain.selected_average", rf"^ממוצע (?:LONG|SHORT) בכל הטווחים:\s*({NUMBER})")
        numeric("maxpain.opposite_score", rf"ניקוד לכיוון הנגדי[^:]*:\s*({NUMBER})")
        numeric("maxpain.opposite_average", rf"ניקוד לכיוון הנגדי[^\n]*ממוצע (?:LONG|SHORT) בכל הטווחים:\s*({NUMBER})")
        numeric("maxpain.target_price", rf"(?:🎯 Max Pain:|יעד Max Pain:)\s*\$({NUMBER})")
        numeric("maxpain.target_distance_pct", rf"(?:🎯 Max Pain:|יעד Max Pain:)\s*\${NUMBER}\s*\(({NUMBER})%\)")
        numeric("maxpain.gap_score", rf"^Gap\s*\n\s*({NUMBER})\s*/\s*15")
        numeric("maxpain.consensus_hits", r"קונצנזוס Gap:\s*(\d+)/\d+")
        numeric("maxpain.consensus_total", r"קונצנזוס Gap:\s*\d+/(\d+)")
        numeric("liquidity.selected_share_pct", rf"מאזן נזילות:\s*({NUMBER})% לצד הנבחר")
        numeric("liquidity.selected_amount", rf"נזילות בכיוון הנבחר:\s*\$({NUMBER})")
        numeric("liquidity.opposite_amount", rf"נזילות בכיוון ההפוך:\s*\$({NUMBER})")
        selected, opposite = features.get("maxpain.selected_score"), features.get("maxpain.opposite_score")
        if selected is not None and opposite is not None:
            features["maxpain.selected_opposite_score_difference"] = selected - opposite
            if opposite != 0:
                features["maxpain.selected_opposite_score_ratio"] = selected / opposite
    quoted = re.search(rf"מחיר נוכחי:\s*\$({NUMBER})", text)
    result["original_printed_price"] = _number(quoted[1]) if quoted else None
    result["original_price_source"] = "TELEGRAM_PRINTED_QUOTE_EXCHANGE_UNSPECIFIED" if quoted else None
    timing = normalize_time(row["raw_time_title"], "israel-wall-clock-v1")
    audit = manifest.get("embedded_utc_age_audit") or {}
    corroborated = (
        manifest.get("time_policy") == "israel-wall-clock-v1"
        and audit.get("source_messages_with_dated_utc_and_age", 0) > 0
        and audit.get("normalized_residual_minutes_min", -1) >= 0
        and audit.get("header_residual_minutes_min", 0) >= 60
    )
    decision = utc(timing["normalized_message_time_utc"]) if timing.get("normalized_message_time_utc") else None
    if not decision or row.get("import_action", "").startswith("QUARANTINE") or row.get("identity_status") != "EXACT_SOURCE_ID":
        missing.append("RESOLVED_SOURCE_TIME_AND_REVISION")
    elif not corroborated:
        missing.append("CORROBORATED_ISRAEL_TIME_POLICY")
    if decision:
        for observed, age in re.findall(r"CVD עד:\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2}) UTC\s*\|\s*גיל בפועל\s*(\d+)", text):
            observed_time = utc(observed.replace(" ", "T") + ":00Z")
            if observed_time + timedelta(minutes=int(age)) > decision:
                conflicts.append("EMBEDDED_CVD_TIME_AFTER_SOURCE_MESSAGE")
        result["source_message_time_utc"] = decision.isoformat()
        result["entry_time_utc"] = (decision.replace(second=0, microsecond=0) + timedelta(minutes=1)).isoformat()
    result["time_corroboration"] = {"dataset_message_count": audit.get("source_messages_with_dated_utc_and_age", 0), "individual_dated_utc_present": bool(re.search(r"CVD עד:\s*\d{4}-\d{2}-\d{2}", text)), "raw_offset_preserved": True}
    result["reconstruction_status"] = "IGNORED_CONTEXT" if family in IGNORED_FAMILIES else "BLOCKED_SOURCE_EVIDENCE" if missing or conflicts else "READY_FOR_SPOT_ENTRY_PATH"
    result["archive_event_key"] = _hash("archive-causal-event-v1", row["source_identity_key"], row["source_revision_sha256"], FEATURE_VERSION, DIRECTION_VERSION, ENTRY_VERSION, TIME_VERSION)
    return result
