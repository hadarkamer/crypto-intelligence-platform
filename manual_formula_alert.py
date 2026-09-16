"""Owner-selected research notifications; no orders or statistical gates.

Event-based predicates use features captured at the original native alert.
C1274 instead consumes the one frozen operational-score bundle produced for
each Watch scan, because its Futures-only definition must not depend on a
Magnet or any other alert existing in that scan.  This module never reads later
observations.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from html import escape
import hashlib
import json
import math
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from experimental_reference_price import VERSION as REFERENCE_VERSION
from experimental_reference_price import render_reference_levels, select_reference

VERSION = "manual-formula-experimental-alerts-v3"
PREVIOUS_VERSION = "manual-formula-experimental-alerts-v2"
PREVIOUS_RULESET_SHA256 = "9d28a33d38faae8bafce6f03b3ce400a1f928aca16f36edecb46b9450dd234c8"
SYMBOLS = ("BTC", "ETH", "SOL", "HYPE", "DOGE", "ZEC", "BNB", "XRP")
TRIGGER_TTL = timedelta(minutes=10)
_ISRAEL = ZoneInfo("Asia/Jerusalem")
_INVERSE = {"LONG": "SHORT", "SHORT": "LONG"}
_MAGNET_TYPES = frozenset(("MAGNET_ALERT", "MAGNET_CONFIRMATION", "STRONG_MAGNET_CONFIRMATION"))
_ARCHIVE_KEYS = frozenset(("archive_reconstruction", "archive_only", "telegram_archive", "archive_run_key",
                           "archive_import", "telegram_archive_import"))
_WATCH_SCORE_VERSION = "watch-operational-scores-v2"
_WATCH_SCORE_POPULATION = "all-top8-watch-scans-before-display-v1"
_WATCH_SCORE_HASH_VERSION = "json-integer-float-zero-normalized-v1"

RULES = {
    "C1274": {
        "name": "C1274 — Futures ארוך",
        "threshold_bps": 150,
        "symbols": ("SOL",),
        "notes": {"SOL": ("כמות הופעות / אסימטריה טעונות שיפור",)},
        "conditions_text": "משפחת Futures בטווח הארוך באיכות 65% ומעלה, והציון הכולל באותו כיוון בעוצמה 25 ומעלה.",
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

# Display provenance is separate from the frozen matching rules and dedup IDs.
# A combination begins at its earliest required component anchor. The shared
# selector validates captured references; detection never fetches a newer quote.
_REFERENCE_COMPONENTS = {
    "C1274": ("FUTURES_CVD",),
    "PRICE_OI_ENTRY2": ("PRICE_OI",),
    "PRICE_OI_SPOT65": ("PRICE_OI", "SPOT_CVD"),
    "CONSENSUS_FULL": ("MAX_PAIN",),
    "C0964": ("MAX_PAIN", "SPOT_CVD"),
}


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


def _watch_numeric_normalized(value: Any) -> Any:
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, Mapping):
        return {key: _watch_numeric_normalized(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_watch_numeric_normalized(item) for item in value]
    return value


def _watch_digest(value: Any) -> str:
    encoded = json.dumps(_watch_numeric_normalized(value), ensure_ascii=False,
                         sort_keys=True, separators=(",", ":"), default=str,
                         allow_nan=False).encode()
    return hashlib.sha256(encoded).hexdigest()


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


def _c1274_futures(futures: Mapping[str, Any]) -> str | None:
    """Return the direct price direction for the audited Futures-only rule."""
    long_family = _mapping(_mapping(futures.get("time_families")).get("long"))
    quality, score = _number(long_family.get("quality")), _number(futures.get("score"))
    family_direction = long_family.get("direction")
    sign = {"BULLISH": 1, "BEARISH": -1}.get(family_direction)
    if (futures.get("available") is not True or quality is None or not 0.65 <= quality <= 1
            or sign is None or score is None or not -100 <= score <= 100
            or sign * score < 25):
        return None
    return "LONG" if family_direction == "BULLISH" else "SHORT"


def evaluate_c1274_scan(bundle: Mapping[str, Any], references: Mapping[str, Any], now: Any) -> dict[str, Any]:
    """Validate one frozen Watch bundle and optionally create one C1274 payload.

    A valid non-match is returned with ``payload=None`` so the outbox can retain
    a scan receipt.  Invalid, stale or hash-mismatched bundles raise and are
    isolated by the caller from the event-based notification rules.
    """
    if not isinstance(bundle, Mapping):
        raise ValueError("C1274 scan bundle is missing")
    body = {key: value for key, value in bundle.items() if key != "payload_sha256"}
    coins = bundle.get("coins")
    if (bundle.get("version") != _WATCH_SCORE_VERSION
            or bundle.get("population") != _WATCH_SCORE_POPULATION
            or bundle.get("hash_version") != _WATCH_SCORE_HASH_VERSION
            or bundle.get("status") not in ("COMPLETE", "PARTIAL")
            or not isinstance(coins, Mapping) or set(coins) != set(SYMBOLS)
            or not isinstance(bundle.get("symbols_expected"), list)
            or sorted(bundle.get("symbols_expected") or ()) != sorted(SYMBOLS)
            or bundle.get("payload_sha256") != _watch_digest(body)):
        raise ValueError("C1274 scan bundle identity mismatch")
    scan = bundle.get("cycle_id")
    if not isinstance(scan, str) or not scan.strip() or len(scan) > 200:
        raise ValueError("C1274 scan identity is invalid")
    computed = utc(bundle.get("computed_at_utc"))
    age = utc(now) - computed
    if not timedelta(0) <= age <= TRIGGER_TTL:
        raise ValueError("C1274 scan bundle is stale or future")
    coin = _mapping(coins.get("SOL"))
    errors = coin.get("source_time_errors")
    if (coin.get("status") not in ("CAPTURED", "PARTIAL")
            or not isinstance(errors, list)
            or not all(isinstance(error, str) for error in errors)
            or any(error.startswith(("futures/", "derivatives/")) for error in errors)):
        raise ValueError("C1274 SOL source contract is invalid")
    futures = _mapping(_mapping(coin.get("models")).get("futures_flow"))
    expected_capture = "AVAILABLE" if futures.get("available") is True else "UNAVAILABLE"
    if futures.get("capture_status") != expected_capture:
        raise ValueError("C1274 Futures capture status is invalid")
    if (futures.get("available") is True
            and (str(futures.get("quality_status") or "").upper() not in ("PASS", "WARNING")
                 or str(futures.get("freshness_status") or "").upper() != "FRESH")):
        raise ValueError("C1274 Futures quality is invalid")
    sources = _mapping(coin.get("sources"))
    futures_source = _mapping(sources.get("futures"))
    candle_close = utc(_mapping(futures_source.get("quality")).get("candle_close"))
    if candle_close > computed or computed - candle_close > timedelta(minutes=30):
        raise ValueError("C1274 Futures candle clock is invalid")
    direction = _c1274_futures(futures)
    result = {"watch_scan_id": scan, "bundle_sha256": bundle["payload_sha256"],
              "event_time": computed.isoformat(), "source_candle_close_utc": candle_close.isoformat(),
              "payload": None}
    if direction is None:
        return result
    rule = RULES["C1274"]
    reference_map = _mapping(references).get("SOL")
    reference = select_reference(reference_map, _REFERENCE_COMPONENTS["C1274"],
                                 symbol="SOL", as_of=computed)
    if (reference.get("status") == "READY"
            and utc(reference.get("anchor_time_utc")) != candle_close):
        # A valid quote from another source boundary must never silently become
        # this Futures candle's entry price.
        reference = {"status": "UNAVAILABLE", "version": REFERENCE_VERSION,
                     "reason": "REFERENCE_SOURCE_CLOCK_MISMATCH"}
    event_id = "watch:" + hashlib.sha256(f"C1274|{scan}|SOL".encode()).hexdigest()
    payload = {"rule_id": "C1274", "threshold_bps": rule["threshold_bps"],
               "symbol": "SOL", "direction": direction, "source_direction": direction,
               "prediction_mode": "DIRECT", "event_id": event_id,
               "event_time": computed.isoformat(), "watch_scan_id": scan,
               "source_candle_close_utc": candle_close.isoformat(),
               "source_bundle_sha256": bundle["payload_sha256"],
               "predicate_version": VERSION, "price_reference": deepcopy(reference)}
    payload["text"] = render_message(payload)
    result["payload"] = payload
    return result


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
        # C1274 is evaluated once per frozen Watch score bundle, never through
        # a Magnet/native-alert carrier.
        return False
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


def _valid_c1274_payload(payload: Mapping[str, Any]) -> bool:
    try:
        scan = payload.get("watch_scan_id")
        digest = payload.get("source_bundle_sha256")
        event_time = utc(payload.get("event_time"))
        candle_close = utc(payload.get("source_candle_close_utc"))
        expected_event_id = "watch:" + hashlib.sha256(f"C1274|{scan}|SOL".encode()).hexdigest()
        reference = _mapping(payload.get("price_reference"))
        return (isinstance(scan, str) and bool(scan.strip()) and len(scan) <= 200
                and isinstance(digest, str) and len(digest) == 64
                and all(character in "0123456789abcdef" for character in digest)
                and payload.get("event_id") == expected_event_id
                and timedelta(0) <= event_time - candle_close <= timedelta(minutes=30)
                and (reference.get("status") != "READY"
                     or utc(reference.get("anchor_time_utc")) == candle_close))
    except (TypeError, ValueError, OverflowError):
        return False


def render_message(payload: Mapping[str, Any]) -> str:
    """Render HTML for an exact rule/symbol/direction, without performance claims."""
    rule = RULES.get(payload.get("rule_id"))
    direct = payload.get("rule_id") == "C1274"
    expected_direction = (payload.get("source_direction") if direct
                          else _INVERSE.get(payload.get("source_direction")))
    if (not rule or payload.get("predicate_version") != VERSION
            or payload.get("threshold_bps") != rule["threshold_bps"]
            or payload.get("symbol") not in rule["symbols"]
            or payload.get("source_direction") not in _INVERSE
            or payload.get("direction") != expected_direction
            or (direct and (payload.get("prediction_mode") != "DIRECT"
                            or not _valid_c1274_payload(payload)))):
        raise ValueError("Invalid frozen experimental notification")
    stamp = utc(payload["event_time"]).astimezone(_ISRAEL)
    direction = "עלייה — LONG" if payload["direction"] == "LONG" else "ירידה — SHORT"
    lines = [f'🧪 <b>סף {rule["threshold_bps"] / 100:g}% — ניסיוני, לא למסחר</b>',
             f'<b>{escape(rule["name"])}</b>',
             f'<b>{escape(payload["symbol"])} | {direction}</b>',
             render_reference_levels(payload.get("price_reference"),
                                     rule["threshold_bps"], payload["direction"], html=True),
             escape(rule["conditions_text"]),
             *([] if direct else ["החיזוי הפוך לכיוון המחקר של אירוע המקור."]),
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
        if rule_id == "C1274":
            continue
        if event["symbol"] not in rule["symbols"] or not _matches(rule_id, event, features):
            continue
        payload = {"rule_id": rule_id, "threshold_bps": rule["threshold_bps"],
                   "symbol": event["symbol"], "direction": _INVERSE[event["direction"]],
                   "event_id": event["event_id"], "event_time": utc(event["alert_time_utc"]).isoformat(),
                   "source_direction": event["direction"], "predicate_version": VERSION}
        references = _mapping(event.get("engine_snapshot")).get("experimental_price_references")
        reference = select_reference(references, _REFERENCE_COMPONENTS[rule_id],
                                     symbol=event["symbol"], as_of=event["alert_time_utc"])
        payload["price_reference"] = deepcopy(reference)
        payload["text"] = render_message(payload)
        result.append(payload)
    return result
