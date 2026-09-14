"""Owner-selected research notifications; no orders or statistical gates.

Predicates are frozen to the audited definitions. Features must be captured at
the original native alert, aligned to that alert's *research* direction. That
direction is inverted exactly once for these notifications. Sequence history
is supplied by the caller; this module never reads later observations.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from html import escape
import hashlib
import json
import math
from typing import Any, Mapping
from zoneinfo import ZoneInfo

VERSION = "manual-formula-experimental-alerts-v2"
PREVIOUS_VERSION = "manual-formula-experimental-alerts-v1"
PREVIOUS_RULESET_SHA256 = "7f7be576af92fdbb283398b14f8f5f81527d1353d46732c358cf66e669eb5560"
SYMBOLS = ("BTC", "ETH", "SOL", "HYPE", "DOGE", "ZEC", "BNB", "XRP")
TRIGGER_TTL = timedelta(minutes=10)
_ISRAEL = ZoneInfo("Asia/Jerusalem")
_INVERSE = {"LONG": "SHORT", "SHORT": "LONG"}
_MAGNET_TYPES = frozenset(("MAGNET_ALERT", "MAGNET_CONFIRMATION", "STRONG_MAGNET_CONFIRMATION"))
_ARCHIVE_KEYS = frozenset(("archive_reconstruction", "archive_only", "telegram_archive", "archive_run_key",
                           "archive_import", "telegram_archive_import"))

RULES = {
    "C1274": {
        "name": "C1274 — Futures ארוך נגד המגנט",
        "threshold_bps": 100,
        "symbols": ("BTC", "BNB", "DOGE", "HYPE", "SOL"),
        "notes": {coin: ("כמות הופעות / אסימטריה טעונות שיפור",) for coin in ("BTC", "SOL")},
        "conditions_text": "משפחת Futures בטווח הארוך באיכות 65% ומעלה נגד המגנט, וגם הציון הכולל נגדו בעוצמה 25 ומעלה.",
    },
    "PRICE_OI_ENTRY2": {
        "name": "כניסת Price/OI השנייה, הפוך",
        "threshold_bps": 100,
        "symbols": ("BTC", "BNB", "DOGE", "ETH", "SOL", "XRP"),
        "notes": {"BTC": ("כמות הופעות קטנה",), "ETH": ("כמות הופעות קטנה",),
                  "SOL": ("כמות הופעות קטנה",), "DOGE": ("אסימטריה נמוכה",)},
        "conditions_text": "Price/OI בציון 65 ומעלה באותו כיוון, עם מונה כניסה 2 בחלון של 30 דקות לפי הגדרת המחקר.",
    },
    "PRICE_OI_SPOT65": {
        "name": "Price/OI ו־Spot CVD ≥65, הפוך",
        "threshold_bps": 200,
        "symbols": ("BTC", "BNB", "DOGE", "HYPE", "SOL", "XRP", "ZEC"),
        "notes": {coin: ("כמות הופעות קטנה",) + (("כמות ההופעות הגדולה ביותר",) if coin == "ZEC" else ())
                  for coin in SYMBOLS if coin != "ETH"},
        "conditions_text": "Price/OI ו־Spot CVD בציון 65 ומעלה, שניהם תומכים בכיוון אירוע המקור.",
    },
    "CONSENSUS_FULL": {
        "name": "Max Pain עם הסכמה מלאה, הפוך",
        "threshold_bps": 200,
        "symbols": ("BTC", "DOGE", "ETH", "HYPE", "SOL", "XRP"),
        "notes": {"SOL": ("אסימטריה טעונת שיפור",),
                  **{coin: ("הסתברות / אסימטריה גבוהות אבל מעט הופעות יחסית",)
                     for coin in ("XRP", "ETH", "BTC")}},
        "conditions_text": "הסכמה מלאה בכל אופקי Max Pain התקפים, עם מיפוי כיוון מקור מאומת.",
    },
    "C0964": {
        "name": "C0964 — Magnet והסכמת Spot, בחיזוי הפוך",
        "threshold_bps": 200,
        "symbols": ("BTC",),
        "notes": {"BTC": ("מבוסס בעיקר על אוגוסט ועל עליות",)},
        "conditions_text": "מגנט עם יתרון נזילות LE של 30% ומעלה, ו־Spot CVD בציון כולל 25 ומעלה בכיוון המגנט.",
    },
}
RULE_IDS = tuple(RULES)
RULESET_SHA256 = hashlib.sha256(json.dumps(RULES, ensure_ascii=False, sort_keys=True,
                                         separators=(',', ':')).encode()).hexdigest()


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        value = float(value)
    except (ValueError, TypeError, OverflowError):
        return None
    return value if math.isfinite(value) else None


def utc(value: Any) -> datetime:
    value = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("A timezone-qualified timestamp is required")
    return value.astimezone(timezone.utc)


def source_is_eligible(event: Mapping[str, Any], features: Mapping[str, Any], now: Any, *, planned=False) -> bool:
    """Fail closed on derived/imported/demo/stale sources; do not gate HYPE routes.

    This screen observes captured conditions only. It neither evaluates nor
    substitutes a future price path, so no spot-only outcome gate belongs here.
    """
    if not isinstance(event, Mapping) or not isinstance(features, Mapping):
        return False
    snapshot = event.get("engine_snapshot")
    direction = event.get("direction")
    fingerprint = event.get("event_fingerprint")
    if planned:
        valid_source = (event.get("delivery_status") == "NOT_ATTEMPTED"
                        and event.get("capture_stage") == "WATCH_PLANNED_ALERT"
                        and event.get("event_id") == "watch:" + str(fingerprint)
                        and isinstance(snapshot, Mapping)
                        and isinstance(snapshot.get("watch_scan_id"), str)
                        and bool(snapshot["watch_scan_id"].strip()))
    else:
        valid_source = (event.get("delivery_status") == "DELIVERED"
                        and type(event.get("event_id")) is int and event["event_id"] > 0)
    if (event.get("event_kind") != "ALERT" or not valid_source
            or not isinstance(fingerprint, str) or len(fingerprint) != 64
            or any(char not in "0123456789abcdefABCDEF" for char in fingerprint)
            or event.get("symbol") not in SYMBOLS or direction not in _INVERSE
            or not isinstance(snapshot, Mapping)
            or not isinstance(event.get("event_type"), str) or not event["event_type"]
            or event["event_type"].startswith("ORDERED_")
            or any(marker in str(event.get('capture_stage') or '').upper()
                   for marker in ('ARCHIVE', 'INVERSE', 'DEMO', 'REPLAY'))
            or "inverse_analysis" in snapshot or _ARCHIVE_KEYS.intersection(snapshot)
            or _ARCHIVE_KEYS.intersection(event)
            or event.get("source_scope", "LIVE") != "LIVE"
            or features.get("event.direction_mapping_valid") is not True
            or features.get("event.analysis_direction") != direction
            or event.get("source_direction", direction) != direction):
        return False
    for block in (event, snapshot):
        if any(str(block.get(key, "")).upper() in ("DEMO", "ARCHIVE", "ARCHIVE_ONLY", "IMPORTED")
               for key in ("data_mode", "state", "mode", "record_mode", "source_scope")):
            return False
    price = _number(event.get("current_price"))
    if price is None or price <= 0:
        return False
    try:
        age = utc(now) - utc(event.get("alert_time_utc"))
    except (ValueError, TypeError, OverflowError):
        return False
    return timedelta(0) <= age <= TRIGGER_TTL


def _c1274(event: Mapping[str, Any]) -> bool:
    if event.get("event_type") not in _MAGNET_TYPES:
        return False
    snapshot = _mapping(event.get("engine_snapshot"))
    modules = _mapping(_mapping(snapshot.get("market_evidence")).get("modules"))
    futures = _mapping(modules.get("futures_flow"))
    long_family = _mapping(_mapping(futures.get("time_families")).get("long"))
    quality, score = _number(long_family.get("quality")), _number(futures.get("score"))
    expected_family_direction = "BEARISH" if event["direction"] == "LONG" else "BULLISH"
    sign = 1 if event["direction"] == "LONG" else -1
    return (futures.get("available") is True and quality is not None and 0.65 <= quality <= 1
            and long_family.get("direction") == expected_family_direction
            and score is not None and -100 <= score <= 100 and sign * score <= -25)


def _score65(features: Mapping[str, Any], name: str) -> bool:
    score = _number(features.get(name + ".aligned_score"))
    return score is not None and 65 <= score <= 100


def _matches(rule_id: str, event: Mapping[str, Any], features: Mapping[str, Any]) -> bool:
    if rule_id == "C0964":
        magnet = _mapping(_mapping(event.get("engine_snapshot")).get("magnet"))
        edge = _number(magnet.get("liquidity_edge_pct"))
        spot = _number(features.get("spot_cvd.aligned_score"))
        return (event.get("event_type") in _MAGNET_TYPES
                and edge is not None and edge >= 30
                and spot is not None and 25 <= spot <= 100)
    if rule_id == "C1274":
        return _c1274(event)
    if rule_id == "PRICE_OI_ENTRY2":
        ordinal = features.get("sequence.30m.price_oi.entry_ordinal")
        return (type(ordinal) in (int, float) and ordinal == 2
                and _score65(features, "price_oi")
                and features.get("sequence.capture_status") == "READY")
    if rule_id == "PRICE_OI_SPOT65":
        return _score65(features, "price_oi") and _score65(features, "spot_cvd")
    if rule_id == "CONSENSUS_FULL":
        return features.get("max_pain.consensus_hits_full") is True
    return False


def render_message(payload: Mapping[str, Any]) -> str:
    """Render HTML for an exact rule/symbol/direction, without performance claims."""
    rule = RULES.get(payload.get("rule_id"))
    if (not rule or payload.get("predicate_version") != VERSION
            or payload.get("threshold_bps") != rule["threshold_bps"]
            or payload.get("symbol") not in rule["symbols"]
            or payload.get("source_direction") not in _INVERSE
            or payload.get("direction") != _INVERSE[payload["source_direction"]]):
        raise ValueError("Invalid frozen experimental notification")
    stamp = utc(payload["event_time"]).astimezone(_ISRAEL)
    direction = "עלייה — LONG" if payload["direction"] == "LONG" else "ירידה — SHORT"
    lines = [f'🧪 <b>סף {rule["threshold_bps"] / 100:g}% — ניסיוני, לא למסחר</b>',
             f'<b>{escape(rule["name"])}</b>',
             f'<b>{escape(payload["symbol"])} | {direction}</b>',
             escape(rule["conditions_text"]),
             "החיזוי הפוך לכיוון המחקר של אירוע המקור.",
             *["<b>הערה</b>: " + escape(note) for note in rule["notes"].get(payload["symbol"], ())],
             f"זמן ההתראה בישראל: {stamp:%d.%m.%Y %H:%M:%S}",
             *([] if str(payload["event_id"]).startswith("watch:") else [f'אירוע מקור: {payload["event_id"]}']),
             "הסף הוא סף התנועה שנבדק במחקר, לא יעד רווח מובטח. לא בוצעה עסקה."]
    return "\n".join(lines)


def evaluate_event(event: Mapping[str, Any], features: Mapping[str, Any], now: Any, *, planned=False) -> list[dict[str, Any]]:
    """Return zero or more exact inverse experimental notifications.

    The caller provides canonical event/sequence features and owns activation,
    durable idempotency and delivery. Invalid or absent data never manufacture
    a match, and an event is never inverted twice.
    """
    if not source_is_eligible(event, features, now, planned=planned):
        return []
    result = []
    for rule_id, rule in RULES.items():
        if event["symbol"] not in rule["symbols"] or not _matches(rule_id, event, features):
            continue
        payload = {"rule_id": rule_id, "threshold_bps": rule["threshold_bps"],
                   "symbol": event["symbol"], "direction": _INVERSE[event["direction"]],
                   "event_id": event["event_id"], "event_time": utc(event["alert_time_utc"]).isoformat(),
                   "source_direction": event["direction"], "predicate_version": VERSION}
        payload["text"] = render_message(payload)
        result.append(payload)
    return result
