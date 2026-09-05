"""Pure Stage-4 contract for silent Max-Pain, Magnet and Combined snapshots.

The module receives already-collected inputs and recomputes their engine outputs.
It does no database, network, Telegram, Formula or trading work. Every emitted object is a
``DECISION_SAMPLE`` whose stable fingerprint is bound to one immutable passive
Max-Pain snapshot and one logical signal locator.  Confirmation tier is evidence,
not identity, so a Strong upgrade never creates an extra Combined vote.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import math
import struct
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

import alert_engine
import magnet_v1
import market_confidence_engine
import research_event_capture


CONTRACT_VERSION = "research-signal-snapshot-v1"
STRATEGY_VERSION = "signal-snapshot-v1"
CAPTURE_STAGE = "SILENT_SIGNAL_SNAPSHOT"
MAX_PAIN_EVENT_TYPE = "MAX_PAIN_CONFIRMATION_STATE"
MAGNET_EVENT_TYPE = "MAGNET_CONFIRMATION_STATE"
COMBINED_EVENT_TYPE = "SILENT_COMBINED_CONFIRMATION_SNAPSHOT"
PROJECTION_EVENT_TYPE = "SIGNAL_SNAPSHOT_PROJECTION"
PROJECTION_SYMBOL = "RESEARCH"
QUALIFYING_TIERS = frozenset({"CONFIRMED", "STRONG_CONFIRMED"})
INDICATION_FAMILIES = ("MAX_PAIN", "MAGNET", "PRICE_OI", "FUTURES_CVD")
VOTING_SOURCE_FAMILIES = (
    "COINGLASS_MAX_PAIN",
    "PRICE_OI",
    "FUTURES_CVD",
)
DERIVATIVES_HIGH_THRESHOLD = 65.0
MAX_PAIN_HIGH_SCORE_THRESHOLD = 80.0
MIN_COMBINED_VOTES = 2
MAX_DECISION_LAG_MINUTES = 15
MAX_PRICE_OI_SOURCE_AGE_MINUTES = 45
MAX_PRICE_OI_TIME_GAP_SECONDS = 30
MAX_FUTURES_CVD_SOURCE_AGE_MINUTES = 30
REQUIRED_TIMEFRAMES = ("12h", "24h", "48h", "3d", "1w", "2w", "1m")
_SIGNAL_EVENT_SET_COMMITMENT_VERSION = "research-signal-event-set-v1"
_SIGNAL_EVENT_FLOAT_FIELDS = (
    "score",
    "current_price",
    "target_price",
    "initial_target_distance_pct",
)

_MAGNET_EVENT_FIELDS = (
    "side",
    "count",
    "members",
    "min_target",
    "max_target",
    "average_target",
    "spread_pct",
    "magnet_quality",
    "liquidity_edge_pct",
    "gross_liquidity_timeframe",
    "gross_candidate_liquidity",
    "gross_opposite_liquidity",
    "liquidity_calculation_version",
)
_MAGNET_CONFIRMATION_FIELDS = (
    "status",
    "label",
    "score_threshold",
    "strong_score_threshold",
    "score_ok",
    "score_confirmation",
    "strong_score_ok",
    "early_shift_opposes",
    "oi_opposes",
    "supporting_families",
    "opposing_families",
    "strong_core",
    "strong_evidence_threshold",
    "magnet_quality",
    "liquidity_edge_pct",
    "liquidity_status",
    "liquidity_label",
    "minimum_engine_score",
)
_MAGNET_CONFIRMATION_DERIVATIVE_FIELDS = (
    "status",
    "label",
    "supporting_families",
    "opposing_families",
    "early_shift_opposes",
    "oi_opposes",
    "strong_core",
    "positioning_score",
    "futures_score",
    "minimum_engine_score",
)

_FAMILY_DEPENDENCIES = {
    "MAX_PAIN": {
        "logical_engine": "MAX_PAIN_SCORE_AND_CONFIRMATION",
        "raw_sources": ["COINGLASS_MAX_PAIN"],
        "qualification_dependencies": ["PRICE_OI", "FUTURES_CVD"],
    },
    "MAGNET": {
        "logical_engine": "MAGNET_V1",
        "raw_sources": ["COINGLASS_MAX_PAIN"],
        "qualification_dependencies": ["PRICE_OI", "FUTURES_CVD"],
    },
    "PRICE_OI": {
        "logical_engine": "PRICE_OI_POSITIONING",
        "raw_sources": ["SPOT_PRICE", "OPEN_INTEREST"],
        "qualification_dependencies": [],
    },
    "FUTURES_CVD": {
        "logical_engine": "FUTURES_CVD",
        "raw_sources": ["FUTURES_TAKER_BUY_SELL"],
        "qualification_dependencies": [],
    },
}

_SOURCE_DEPENDENCIES = {
    "COINGLASS_MAX_PAIN": {
        "logical_engines": ["MAX_PAIN_SCORE_AND_CONFIRMATION", "MAGNET_V1"],
        "raw_sources": ["COINGLASS_MAX_PAIN"],
        "qualification_dependencies": ["PRICE_OI", "FUTURES_CVD"],
        "deduplication_rule": "MAX_PAIN_AND_MAGNET_SHARE_ONE_SOURCE_VOTE",
    },
    "PRICE_OI": _FAMILY_DEPENDENCIES["PRICE_OI"],
    "FUTURES_CVD": _FAMILY_DEPENDENCIES["FUTURES_CVD"],
}


@dataclass(frozen=True)
class SignalSnapshotBatch:
    events: tuple[research_event_capture.ResearchEvent, ...]
    snapshot_key: str
    snapshot_set_id: int
    decision_time_utc: str
    eligible_symbols: tuple[str, ...]
    evaluated_symbols: tuple[str, ...]
    unevaluable_symbols: tuple[str, ...]
    evaluation_status: str
    counts: Dict[str, int]


def _utc(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
    else:
        raise ValueError(f"{field} must be a timezone-aware timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _iso(value: Any, *, field: str) -> str:
    return _utc(value, field=field).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _json_safe(value: Any) -> Any:
    try:
        return json.loads(
            json.dumps(
                value,
                default=str,
                ensure_ascii=False,
                sort_keys=True,
                allow_nan=False,
            )
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("signal snapshot contains non-finite or invalid JSON") from exc


def _canonical(value: Any) -> str:
    return json.dumps(
        _json_safe(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _compact_magnet_component(
    *, tier: str, magnet: Mapping[str, Any], confirmation: Mapping[str, Any]
) -> Dict[str, Any]:
    """Mirror the public Research Event projection used by the Magnet sibling."""
    normalized_magnet = dict(magnet)
    if "members" in normalized_magnet:
        normalized_magnet["members"] = sorted(
            {str(item) for item in normalized_magnet.get("members") or []}
        )
    compact_confirmation = {
        key: _json_safe(confirmation.get(key))
        for key in _MAGNET_CONFIRMATION_FIELDS
        if key in confirmation
    }
    derivatives = confirmation.get("derivatives")
    if isinstance(derivatives, Mapping):
        compact_confirmation["derivatives"] = {
            key: _json_safe(derivatives.get(key))
            for key in _MAGNET_CONFIRMATION_DERIVATIVE_FIELDS
            if key in derivatives
        }
    return {
        "tier": tier,
        "magnet": {
            key: _json_safe(normalized_magnet.get(key))
            for key in _MAGNET_EVENT_FIELDS
            if key in normalized_magnet
        },
        "confirmation": compact_confirmation,
    }


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _commitment_number(value: Any) -> str:
    text = format(Decimal(str(value)), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in {"", "-0"} else text


def _commitment_canonical_safe(value: Any) -> str:
    if value is None:
        return "n"
    if type(value) is bool:
        return "b1" if value else "b0"
    if isinstance(value, (int, float)):
        text = _commitment_number(value)
        return f"d{len(text.encode('utf-8'))}:{text}"
    if isinstance(value, str):
        return f"s{len(value.encode('utf-8'))}:{value}"
    if isinstance(value, list):
        return f"a{len(value)}:" + "".join(
            _commitment_canonical_safe(item) for item in value
        )
    if isinstance(value, dict):
        keys = sorted(value, key=lambda key: key.encode("utf-8"))
        return f"o{len(keys)}:" + "".join(
            f"k{len(key.encode('utf-8'))}:{key}"
            + _commitment_canonical_safe(value[key])
            for key in keys
        )
    raise TypeError(f"unsupported commitment value: {type(value).__name__}")


def _commitment_canonical(value: Any) -> str:
    return _commitment_canonical_safe(_json_safe(value))


def _commitment_sha256(value: Any) -> str:
    return hashlib.sha256(
        _commitment_canonical(value).encode("utf-8")
    ).hexdigest()


def _signal_event_commitment_payload(
    event: research_event_capture.ResearchEvent,
) -> Dict[str, Any]:
    payload = event.to_dict()
    for field in _SIGNAL_EVENT_FLOAT_FIELDS:
        raw = payload[field]
        payload[field] = (
            None if raw is None else struct.pack("!d", float(raw)).hex()
        )
    return payload


def _signal_events_payload_sha256(
    signal_events: Sequence[research_event_capture.ResearchEvent],
) -> str:
    ordered = sorted(signal_events, key=lambda event: event.event_fingerprint)
    row_hashes = [
        _commitment_sha256(_signal_event_commitment_payload(event))
        for event in ordered
    ]
    material = (
        f"{_SIGNAL_EVENT_SET_COMMITMENT_VERSION}:{len(row_hashes)}:"
        + "".join(row_hashes)
    )
    return hashlib.sha256(material.encode("ascii")).hexdigest()


def _finite(value: Any, *, field: str, optional: bool = False) -> Optional[float]:
    if value in (None, "") and optional:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a finite number")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field} must be a finite number") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field} must be a finite number")
    return number


def _positive_int(value: Any, *, field: str) -> int:
    if type(value) is not int or value <= 0 or value > 2**63 - 1:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _direction_from_source_side(value: Any) -> str:
    side = str(value or "").strip().upper()
    if side in {"SHORT", "UPPER"}:
        return "LONG"
    if side in {"LONG", "LOWER"}:
        return "SHORT"
    raise ValueError(f"unsupported signal source side: {value!r}")


def _normalized_direction(value: Any, *, field: str) -> str:
    raw = str(value or "").strip().upper()
    aliases = {
        "LONG": "LONG",
        "BULL": "LONG",
        "BULLISH": "LONG",
        "UP": "LONG",
        "SHORT": "SHORT",
        "BEAR": "SHORT",
        "BEARISH": "SHORT",
        "DOWN": "SHORT",
    }
    normalized = aliases.get(raw)
    if normalized is None:
        raise ValueError(f"{field} must be LONG/BULLISH or SHORT/BEARISH")
    return normalized


def _validate_market_evidence(
    evidence: Mapping[str, Any],
    *,
    symbol: str,
    direction: str,
    field: str,
) -> Dict[str, Any]:
    data = dict(evidence or {})
    if not data:
        raise ValueError(f"{field} is missing")
    evidence_symbol = str(data.get("symbol") or "").strip().upper()
    if evidence_symbol != symbol:
        raise ValueError(f"{field} symbol does not match {symbol}")
    expected = _normalized_direction(
        data.get("expected_price_direction"), field=f"{field} expected direction"
    )
    if expected != direction:
        raise ValueError(f"{field} expected direction does not match {direction}")
    modules = data.get("modules") or {}
    if not isinstance(modules, Mapping):
        raise ValueError(f"{field} modules must be a mapping")
    for module_key in ("positioning", "futures_flow"):
        module = modules.get(module_key) or {}
        if not isinstance(module, Mapping):
            raise ValueError(f"{field} {module_key} module must be a mapping")
        if str(module.get("relation") or "").strip().upper() != "SUPPORT":
            continue
        module_direction = _normalized_direction(
            module.get("direction"), field=f"{field} {module_key} direction"
        )
        if module_direction != direction:
            raise ValueError(
                f"{field} {module_key} SUPPORT direction does not match {direction}"
            )
        score = _finite(module.get("score"), field=f"{field} {module_key} score")
        if (module_direction == "LONG" and score < 0.0) or (
            module_direction == "SHORT" and score > 0.0
        ):
            raise ValueError(f"{field} {module_key} score sign contradicts direction")
    return data


def _validate_maxpain_confirmation(
    item: Mapping[str, Any], *, symbol: str, direction: str
) -> None:
    evidence = dict(item.get("market_evidence") or {})
    modules = dict(evidence.get("modules") or {})
    expected_label = "BULLISH" if direction == "LONG" else "BEARISH"
    expected_conclusion = market_confidence_engine._conclusion(
        modules, expected_label
    )
    for key, expected_value in expected_conclusion.items():
        if _canonical(evidence.get(key)) != _canonical(expected_value):
            raise ValueError(
                f"Max-Pain market evidence conclusion mismatch at {key}"
            )
    score = _finite(
        item.get("score", item.get("priority")), field="Max-Pain score"
    )
    if _finite(evidence.get("maxpain_score"), field="evidence Max-Pain score") != score:
        raise ValueError("Max-Pain evidence score does not match opportunity")
    expected_confirmation = market_confidence_engine._confirmation(
        score, expected_label, modules, expected_conclusion
    )
    if _canonical(evidence.get("confirmation") or {}) != _canonical(
        expected_confirmation
    ):
        raise ValueError("Max-Pain evidence confirmation was not engine-derived")
    if _canonical(item.get("maxpain_confirmation") or {}) != _canonical(
        expected_confirmation
    ):
        raise ValueError("Max-Pain confirmation was not engine-derived")


def _validate_opportunity_direction(
    item: Mapping[str, Any], *, field: str = "Max-Pain opportunity"
) -> tuple[str, str, float, float]:
    symbol = str(item.get("symbol") or "").strip().upper()
    if not symbol:
        raise ValueError(f"{field} symbol is missing")
    source_side = str(item.get("side") or "").strip().upper()
    direction = _direction_from_source_side(source_side)
    current = _finite(item.get("current_price"), field=f"{field} current_price")
    target = _finite(item.get("target_price"), field=f"{field} target_price")
    price_direction = "LONG" if target > current else "SHORT" if target < current else ""
    if not price_direction or price_direction != direction:
        raise ValueError(f"{field} source side and target direction disagree")
    if item.get("target_direction") not in (None, ""):
        explicit = _normalized_direction(
            item.get("target_direction"), field=f"{field} target_direction"
        )
        if explicit != direction:
            raise ValueError(f"{field} explicit target direction disagrees")
    _validate_market_evidence(
        item.get("market_evidence") or {},
        symbol=symbol,
        direction=direction,
        field=f"{field} market evidence",
    )
    _validate_maxpain_confirmation(item, symbol=symbol, direction=direction)
    return symbol, direction, current, target


def _validate_magnet_direction(
    observation: Mapping[str, Any], *, field: str = "Magnet observation"
) -> tuple[str, str, float, float]:
    magnet = dict(observation.get("magnet") or {})
    symbol = str(magnet.get("symbol") or "").strip().upper()
    if not symbol:
        raise ValueError(f"{field} symbol is missing")
    source_side = str(magnet.get("side") or "").strip().upper()
    direction = _direction_from_source_side(source_side)
    current = _finite(observation.get("current_price"), field=f"{field} current_price")
    target = _finite(magnet.get("average_target"), field=f"{field} average_target")
    price_direction = "LONG" if target > current else "SHORT" if target < current else ""
    if not price_direction or price_direction != direction:
        raise ValueError(f"{field} side and target direction disagree")
    _validate_market_evidence(
        observation.get("market_evidence") or {},
        symbol=symbol,
        direction=direction,
        field=f"{field} market evidence",
    )
    return symbol, direction, current, target


def _verify_payload_hash(record: Mapping[str, Any], *, label: str) -> None:
    expected = str(record.get("payload_sha256") or "").strip()
    if len(expected) != 64:
        raise ValueError(f"{label} payload hash is missing or invalid")
    body = dict(record)
    body.pop("payload_sha256", None)
    if _sha256(body) != expected:
        raise ValueError(f"{label} payload hash mismatch")


def _validated_archive(
    payload: Mapping[str, Any], persistence: Mapping[str, Any]
) -> tuple[Dict[str, Any], Dict[str, Dict[str, Any]], Dict[str, list[Dict[str, Any]]]]:
    set_record = dict(payload.get("set") or {})
    manifests = [dict(item) for item in payload.get("symbols") or []]
    rows = [dict(item) for item in payload.get("rows") or []]
    if str(set_record.get("source") or "").upper() != "RESEARCH_PASSIVE":
        raise ValueError("signal snapshots require a RESEARCH_PASSIVE archive source")
    if set_record.get("research_eligible") is not True:
        raise ValueError("Max-Pain snapshot set is not research eligible")
    if persistence.get("persisted") is not True:
        raise ValueError("Max-Pain snapshot was not durably persisted")
    set_id = _positive_int(
        persistence.get("snapshot_set_id"), field="snapshot_set_id"
    )
    snapshot_key = str(set_record.get("snapshot_key") or "").strip()
    if len(snapshot_key) != 64:
        raise ValueError("Max-Pain snapshot key is missing or invalid")

    for manifest in manifests:
        _verify_payload_hash(manifest, label="symbol manifest")
    for row in rows:
        _verify_payload_hash(row, label="Max-Pain row")
    expected_set_hash = str(set_record.get("payload_sha256") or "").strip()
    set_without_hash = dict(set_record)
    set_without_hash.pop("payload_sha256", None)
    if _sha256(
        {"set": set_without_hash, "symbols": manifests, "rows": rows}
    ) != expected_set_hash:
        raise ValueError("Max-Pain snapshot-set payload hash mismatch")

    manifest_symbols = [
        str(item.get("symbol") or "").strip().upper() for item in manifests
    ]
    if any(not symbol for symbol in manifest_symbols):
        raise ValueError("Max-Pain snapshot contains an empty symbol manifest")
    if len(manifest_symbols) != len(set(manifest_symbols)):
        raise ValueError("Max-Pain snapshot contains duplicate symbol manifests")
    eligible = {
        str(item.get("symbol") or "").strip().upper(): item
        for item in manifests
        if item.get("research_eligible") is True
    }
    if not eligible:
        raise ValueError("Max-Pain snapshot has no eligible symbols")
    rows_by_symbol: Dict[str, list[Dict[str, Any]]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        if symbol not in eligible:
            continue
        if row.get("row_valid") is not True:
            raise ValueError(f"eligible symbol {symbol} contains an invalid row")
        rows_by_symbol.setdefault(symbol, []).append(row)
    for symbol in eligible:
        symbol_rows = rows_by_symbol.get(symbol) or []
        timeframes = [str(row.get("timeframe") or "") for row in symbol_rows]
        if len(symbol_rows) != len(REQUIRED_TIMEFRAMES) or set(timeframes) != set(
            REQUIRED_TIMEFRAMES
        ):
            raise ValueError(f"eligible symbol {symbol} does not have exact 7/7 rows")
        if len(timeframes) != len(set(timeframes)):
            raise ValueError(f"eligible symbol {symbol} has duplicate timeframes")
    set_record["snapshot_set_id"] = set_id
    return set_record, eligible, rows_by_symbol


def _assert_not_future(value: Any, cutoff: datetime, *, field: str) -> None:
    if value in (None, ""):
        return
    if _utc(value, field=field) > cutoff:
        raise ValueError(f"{field} is after derivatives read completion")


def _derivative_reference(
    *,
    symbol: str,
    snapshot: Mapping[str, Any],
    read_started_at: datetime,
    read_completed_at: datetime,
    decision_time: datetime,
) -> Dict[str, Any]:
    if not isinstance(snapshot, Mapping):
        raise ValueError("derivatives snapshot must be a mapping")
    data = _json_safe(dict(snapshot or {}))
    if str(data.get("symbol") or symbol).upper() != symbol:
        raise ValueError(f"derivatives snapshot symbol mismatch for {symbol}")
    if not isinstance(data.get("regime"), Mapping) or not isinstance(
        data.get("flow"), Mapping
    ):
        raise ValueError("derivatives regime and flow must be mappings")
    regime = dict(data["regime"])
    flow = dict(data["flow"])
    for key in ("windows", "overall"):
        if not isinstance(regime.get(key), Mapping):
            raise ValueError(f"Price+OI {key} must be a mapping")
    if any(
        not isinstance(value, Mapping)
        for value in dict(regime["windows"]).values()
    ):
        raise ValueError("Price+OI window values must be mappings")
    for label, source in (("Price+OI", regime), ("CVD", flow)):
        source_symbol = str(source.get("symbol") or symbol).upper()
        if source_symbol != symbol:
            raise ValueError(f"{label} snapshot symbol mismatch for {symbol}")
    if regime.get("available") is not True:
        raise ValueError(f"Price+OI evidence is unavailable for {symbol}")
    if regime.get("available"):
        _positive_int(regime.get("source_snapshot_id"), field="Price+OI snapshot id")
        if (
            type(regime.get("data_quality_status")) is not str
            or regime.get("data_quality_status") != "PASS"
            or type(regime.get("collected_at")) is not str
            or type(regime.get("price_fetched_at")) is not str
            or type(regime.get("oi_fetched_at")) is not str
            or type(regime.get("price_source")) is not str
            or not regime.get("price_source").strip()
            or type(regime.get("oi_source")) is not str
            or not regime.get("oi_source").strip()
        ):
            raise ValueError("available Price+OI evidence has invalid provenance")
        gap_seconds = _finite(
            regime.get("time_gap_seconds"), field="Price+OI time gap"
        )
        price_fetched_at = _utc(
            regime.get("price_fetched_at"), field="Price+OI price_fetched_at"
        )
        oi_fetched_at = _utc(
            regime.get("oi_fetched_at"), field="Price+OI oi_fetched_at"
        )
        observed_gap = abs((oi_fetched_at - price_fetched_at).total_seconds())
        if (
            gap_seconds < 0.0
            or gap_seconds > MAX_PRICE_OI_TIME_GAP_SECONDS
            or abs(gap_seconds - observed_gap) > 0.000001
        ):
            raise ValueError("available Price+OI evidence has invalid provenance")
        collected_at = _utc(
            regime.get("collected_at"), field="Price+OI collected_at"
        )
        age_minutes = (read_completed_at - collected_at).total_seconds() / 60.0
        if age_minutes > MAX_PRICE_OI_SOURCE_AGE_MINUTES:
            raise ValueError(
                "available Price+OI evidence is stale: "
                f"{age_minutes:.2f} minutes old"
            )
    else:
        age_minutes = None
    for key in ("collected_at", "price_fetched_at", "oi_fetched_at"):
        _assert_not_future(
            regime.get(key), read_completed_at, field=f"Price+OI {key}"
        )

    flow_refs: Dict[str, Any] = {}
    for market in ("futures", "spot"):
        if not isinstance(flow.get(market), Mapping):
            raise ValueError(f"{market} CVD evidence must be a mapping")
        market_data = dict(flow[market])
        if (
            type(market_data.get("symbol")) is not str
            or market_data.get("symbol").strip().upper() != symbol
            or type(market_data.get("market")) is not str
            or market_data.get("market") != market
        ):
            raise ValueError(f"{market} CVD identity is invalid for {symbol}")
        for key in ("quality", "windows", "overall"):
            if not isinstance(market_data.get(key), Mapping):
                raise ValueError(f"{market} CVD {key} must be a mapping")
        if any(
            not isinstance(value, Mapping)
            for value in dict(market_data["windows"]).values()
        ):
            raise ValueError(f"{market} CVD window values must be mappings")
        quality = dict(market_data["quality"])
        if market == "futures" and (
            market_data.get("available") is not True
            or quality.get("usable_for_confirmation") is not True
            or type(quality.get("status")) is not str
            or quality.get("status") not in {"PASS", "WARNING"}
            or type(quality.get("freshness_status")) is not str
            or quality.get("freshness_status") != "FRESH"
            or (
                quality.get("continuous_cvd_check") is not True
                and quality.get("continuous_cvd_check") != "PASS"
            )
            or type(quality.get("rows")) is not int
            or quality.get("rows") <= 0
            or quality.get("rows") > 2**63 - 1
        ):
            raise ValueError(f"Futures CVD evidence is unavailable for {symbol}")
        if market_data.get("available") is True:
            if (
                type(quality.get("latest_time")) is not str
                or type(quality.get("candle_close")) is not str
            ):
                raise ValueError(
                    f"available {market} CVD evidence is missing candle provenance"
                )
        elif market_data.get("available") is not False:
            raise ValueError(f"{market} CVD availability must be boolean")
        if market == "spot":
            if market_data.get("available") is True and (
                type(quality.get("status")) is not str
                or quality.get("status") not in {"PASS", "WARNING"}
                or type(quality.get("freshness_status")) is not str
                or quality.get("freshness_status") not in {"FRESH", "STALE"}
                or type(quality.get("usable_for_confirmation")) is not bool
                or type(quality.get("rows")) is not int
                or quality.get("rows") <= 0
                or quality.get("rows") > 2**63 - 1
                or (
                    type(quality.get("continuous_cvd_check")) is not bool
                    and quality.get("continuous_cvd_check") != "PASS"
                )
            ):
                raise ValueError(f"available Spot CVD context is invalid for {symbol}")
            if market_data.get("available") is False and (
                quality.get("status") != "NO_DATA"
                or type(quality.get("rows")) is not int
                or quality.get("rows") != 0
                or any(
                    quality.get(key) is not None
                    for key in (
                        "latest_time",
                        "candle_close",
                        "freshness_status",
                        "usable_for_confirmation",
                        "continuous_cvd_check",
                    )
                )
            ):
                raise ValueError(f"unavailable Spot CVD context is invalid for {symbol}")
        for key in ("latest_time", "candle_close"):
            _assert_not_future(
                quality.get(key), read_completed_at, field=f"{market} CVD {key}"
            )
        if market == "futures" and (
            read_completed_at
            - _utc(quality.get("candle_close"), field="futures CVD candle_close")
            > timedelta(minutes=MAX_FUTURES_CVD_SOURCE_AGE_MINUTES)
        ):
            raise ValueError(f"Futures CVD evidence is unavailable for {symbol}")
        for label, window in (market_data.get("windows") or {}).items():
            if not isinstance(window, Mapping):
                continue
            for key in ("latest_time", "reference_time", "target_time"):
                _assert_not_future(
                    window.get(key),
                    read_completed_at,
                    field=f"{market} CVD {label} {key}",
                )
        flow_refs[market] = {
            "source_table": (
                "futures_taker_history"
                if market == "futures"
                else "spot_taker_history"
            ),
            "latest_candle_time_utc": (
                _iso(quality.get("latest_time"), field=f"{market} CVD latest_time")
                if quality.get("latest_time") is not None
                else None
            ),
            "latest_candle_close_utc": (
                _iso(quality.get("candle_close"), field=f"{market} CVD candle_close")
                if quality.get("candle_close") is not None
                else None
            ),
            "quality_status": quality.get("status"),
            "freshness_status": quality.get("freshness_status"),
            "usable_for_confirmation": quality.get("usable_for_confirmation"),
            "row_count": quality.get("rows"),
            "continuous_cvd_check": quality.get("continuous_cvd_check"),
        }

    if read_started_at > read_completed_at or read_completed_at > decision_time:
        raise ValueError("derivatives read timestamps are not causal")
    return {
        "read_started_at_utc": _iso(
            read_started_at, field="derivatives_read_started_at_utc"
        ),
        "read_completed_at_utc": _iso(
            read_completed_at, field="derivatives_read_completed_at_utc"
        ),
        "payload_sha256": _sha256(data),
        "price_oi": {
            "source_table": "oi_regime_snapshots",
            "source_snapshot_id": regime.get("source_snapshot_id"),
            "collected_at_utc": _iso(
                regime.get("collected_at"), field="Price+OI collected_at"
            ),
            "price_fetched_at_utc": _iso(
                regime.get("price_fetched_at"), field="Price+OI price_fetched_at"
            ),
            "oi_fetched_at_utc": _iso(
                regime.get("oi_fetched_at"), field="Price+OI oi_fetched_at"
            ),
            "time_gap_seconds": gap_seconds,
            "quality_status": regime.get("data_quality_status"),
            "price_source": regime.get("price_source").strip(),
            "oi_source": regime.get("oi_source").strip(),
            "age_minutes_at_read": (
                round(age_minutes, 6) if age_minutes is not None else None
            ),
            "maximum_age_minutes": MAX_PRICE_OI_SOURCE_AGE_MINUTES,
        },
        "futures_cvd": flow_refs["futures"],
        "spot_cvd_context": flow_refs["spot"],
    }


def _derivative_rejection_code(exc: ValueError) -> str:
    """Collapse provider details into a bounded, non-secret reason code."""

    message = str(exc)
    if "Price+OI evidence is unavailable" in message:
        return "PRICE_OI_UNAVAILABLE"
    if "Price+OI evidence is stale" in message:
        return "PRICE_OI_STALE"
    if "Futures CVD evidence is unavailable" in message:
        return "FUTURES_CVD_UNAVAILABLE"
    return "DERIVATIVES_SNAPSHOT_INVALID"


def _archive_reference(
    *,
    set_record: Mapping[str, Any],
    manifest: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    ordered_rows = sorted(
        rows, key=lambda row: REQUIRED_TIMEFRAMES.index(str(row["timeframe"]))
    )
    price_row = ordered_rows[0]
    raw_price = dict(price_row.get("raw_provenance") or {})
    return {
        "snapshot_set_id": int(set_record["snapshot_set_id"]),
        "snapshot_key": set_record.get("snapshot_key"),
        "set_payload_sha256": set_record.get("payload_sha256"),
        "symbol_manifest_payload_sha256": manifest.get("payload_sha256"),
        "row_payload_sha256": [
            {
                "timeframe": row.get("timeframe"),
                "payload_sha256": row.get("payload_sha256"),
            }
            for row in ordered_rows
        ],
        "max_pain_targets": [
            {
                "timeframe": row.get("timeframe"),
                "short_max_pain": row.get("short_max_pain"),
                "long_max_pain": row.get("long_max_pain"),
                "short_liquidation_amount": row.get(
                    "short_liquidation_amount"
                ),
                "long_liquidation_amount": row.get(
                    "long_liquidation_amount"
                ),
            }
            for row in ordered_rows
        ],
        "archive_schema_version": set_record.get("archive_schema_version"),
        "method_version": set_record.get("method_version"),
        "cycle_id": set_record.get("cycle_id"),
        "cycle_time_utc": set_record.get("cycle_time_utc"),
        "available_at_utc": set_record.get("available_at_utc"),
        "source": set_record.get("source"),
        "collector_version": set_record.get("collector_version"),
        "official_price": {
            "price": price_row.get("current_price"),
            "source": price_row.get("price_source"),
            "exchange": price_row.get("price_exchange"),
            "market": price_row.get("price_market"),
            "pair": price_row.get("price_pair"),
            "instrument": price_row.get("price_instrument"),
            "interval": raw_price.get("price_interval"),
            "fetched_at_utc": price_row.get("price_fetched_at_utc"),
            "observed_at_utc": raw_price.get("price_observed_at_utc"),
            "candle_open_time_utc": raw_price.get("price_candle_open_time_utc"),
            "candle_close_time_utc": raw_price.get("price_candle_close_time_utc"),
            "policy_status": price_row.get("price_source_policy_status"),
        },
    }


def _frozen_target(
    archive_reference: Mapping[str, Any], *, timeframe: Any, field: str
) -> float:
    normalized_timeframe = str(timeframe or "").strip()
    matches = [
        row
        for row in archive_reference.get("max_pain_targets") or []
        if str((row or {}).get("timeframe") or "") == normalized_timeframe
    ]
    if len(matches) != 1:
        raise ValueError(f"frozen archive target missing for {normalized_timeframe}")
    return _finite(matches[0].get(field), field=f"archived {field}")


def _validate_frozen_opportunity_target(
    item: Mapping[str, Any], archive_reference: Mapping[str, Any]
) -> None:
    source_side = str(item.get("side") or "").strip().upper()
    target_field = {
        "SHORT": "short_max_pain",
        "LONG": "long_max_pain",
    }.get(source_side)
    if target_field is None:
        raise ValueError("unsupported Max-Pain source side")
    frozen = _frozen_target(
        archive_reference,
        timeframe=item.get("timeframe"),
        field=target_field,
    )
    target = _finite(item.get("target_price"), field="Max-Pain target_price")
    if target != frozen:
        raise ValueError("Max-Pain target does not match the frozen archive row")


def _validate_frozen_magnet_targets(
    observation: Mapping[str, Any], archive_reference: Mapping[str, Any]
) -> None:
    magnet = dict(observation.get("magnet") or {})
    side = str(magnet.get("side") or "").strip().upper()
    target_field = {
        "UPPER": "short_max_pain",
        "LOWER": "long_max_pain",
    }.get(side)
    if target_field is None:
        raise ValueError("unsupported Magnet side")
    members = [str(item) for item in magnet.get("members") or []]
    if not members or len(members) != len(set(members)):
        raise ValueError("Magnet members must be non-empty and unique")
    if int(magnet.get("count") or 0) != len(members):
        raise ValueError("Magnet count does not match its member set")
    official_price = _finite(
        (archive_reference.get("official_price") or {}).get("price"),
        field="archived official price",
    )
    frozen_rows = [
        {
            "symbol": str(magnet.get("symbol") or "").strip().upper(),
            "timeframe": row.get("timeframe"),
            "current_price": official_price,
            "short_max_pain": row.get("short_max_pain"),
            "long_max_pain": row.get("long_max_pain"),
            "short_liquidation_amount": row.get("short_liquidation_amount"),
            "long_liquidation_amount": row.get("long_liquidation_amount"),
        }
        for row in archive_reference.get("max_pain_targets") or []
    ]
    expected_candidates = [
        candidate
        for candidate in magnet_v1.build_magnets(frozen_rows)
        if str(candidate.get("side") or "").upper() == side
        and sorted(str(value) for value in candidate.get("members") or [])
        == sorted(members)
    ]
    if len(expected_candidates) != 1:
        raise ValueError("Magnet geometry does not identify one frozen candidate")
    if _canonical(magnet) != _canonical(expected_candidates[0]):
        raise ValueError("Magnet metrics were not derived from frozen archive rows")


def _stable_event(
    event: research_event_capture.ResearchEvent,
    *,
    snapshot_key: str,
    signal_family: str,
    locator: Mapping[str, Any],
) -> research_event_capture.ResearchEvent:
    fingerprint = _stable_fingerprint(
        snapshot_key=snapshot_key,
        event_type=event.event_type,
        symbol=event.symbol,
        direction=event.direction,
        signal_family=signal_family,
        locator=locator,
    )
    return replace(event, event_fingerprint=fingerprint)


def _stable_fingerprint(
    *,
    snapshot_key: str,
    event_type: str,
    symbol: str,
    direction: str,
    signal_family: str,
    locator: Mapping[str, Any],
) -> str:
    return _sha256(
        {
            "contract_version": CONTRACT_VERSION,
            "snapshot_key": snapshot_key,
            "event_type": event_type,
            "symbol": symbol,
            "direction": direction,
            "signal_family": signal_family,
            "locator": _json_safe(dict(locator)),
        }
    )


def projection_event_fingerprint(snapshot_key: str) -> str:
    normalized = str(snapshot_key or "").strip()
    if len(normalized) != 64:
        raise ValueError("Max-Pain snapshot key is missing or invalid")
    return _stable_fingerprint(
        snapshot_key=normalized,
        event_type=PROJECTION_EVENT_TYPE,
        symbol=PROJECTION_SYMBOL,
        direction="NEUTRAL",
        signal_family="PROJECTION",
        locator={"scope": "SNAPSHOT_SET"},
    )


def projection_setup_key() -> str:
    """Return the one canonical setup identity shared by projection receipts."""

    probe = research_event_capture.build_decision_sample(
        symbol=PROJECTION_SYMBOL,
        sample_type=PROJECTION_EVENT_TYPE,
        direction="NEUTRAL",
        categories=["DECISION_SAMPLE", "SILENT"],
        engine_snapshot={},
        setup_identity={
            "contract_version": CONTRACT_VERSION,
            "signal_family": "PROJECTION",
            "scope": "SNAPSHOT_SET",
        },
        event_time=datetime(2000, 1, 1, tzinfo=timezone.utc),
        strategy_version=STRATEGY_VERSION,
        code_version="projection-setup-key",
    )
    return probe.setup_key


def _signal_metadata(
    *,
    family: str,
    tier: str,
    archive_reference: Mapping[str, Any],
    derivatives_reference: Mapping[str, Any],
    decision_time: datetime,
) -> Dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "signal_family": family,
        "tier": tier,
        "decision_time_utc": _iso(decision_time, field="decision_time_utc"),
        "archive_reference": _json_safe(dict(archive_reference)),
        "derivatives_reference": _json_safe(dict(derivatives_reference)),
        "dependency_lineage": _json_safe(_FAMILY_DEPENDENCIES.get(family) or {}),
        "formula_authorized": False,
        "outcome_authorized": False,
        "telegram_delivery_allowed": False,
        "trade_execution_allowed": False,
    }


def _stage4_maxpain_payload(item: Mapping[str, Any]) -> Dict[str, Any]:
    """Add Stage-4 audit detail without changing ordinary alert snapshots."""

    enriched = dict(item)
    enriched["balance"] = _json_safe(
        item.get("balance")
        or {
            "near_share_pct": item.get("near_share_pct"),
            "near_far_ratio": item.get("near_far_ratio"),
            "balance": item.get("liquidity_balance"),
        }
    )
    enriched["cluster"] = _json_safe(
        item.get("cluster")
        or {
            "points": (item.get("components") or {}).get("cluster_confidence"),
            "count": item.get("cluster_count"),
            "spread_pct": item.get("cluster_spread_pct"),
            "median_target": item.get("cluster_median_target"),
            "members": item.get("cluster_members") or [],
            "density_points": item.get("cluster_density_points"),
            "coverage_points": item.get("cluster_coverage_points"),
            "growth_points": item.get("cluster_growth_points"),
            "liquidity_multiplier": item.get("cluster_liquidity_multiplier"),
        }
    )
    enriched["gap"] = _json_safe(
        item.get("gap")
        or {
            "points": (item.get("components") or {}).get("relative_gap"),
            "advantage": item.get("relative_gap_advantage"),
            "near_distance": item.get("near_distance_pct"),
            "far_distance": item.get("far_distance_pct"),
        }
    )
    return enriched


def _maxpain_event(
    item: Mapping[str, Any],
    *,
    archive_reference: Mapping[str, Any],
    derivatives_reference: Mapping[str, Any],
    decision_time: datetime,
    code_version: Optional[str],
) -> research_event_capture.ResearchEvent:
    errors = list(item.get("calculation_validation_errors") or [])
    if errors:
        raise ValueError("Max-Pain calculation validation failed: " + "; ".join(errors))
    tier = str((item.get("maxpain_confirmation") or {}).get("status") or "").upper()
    if tier not in QUALIFYING_TIERS:
        raise ValueError("Max-Pain event is not a qualifying confirmation")
    symbol, direction, current_price, target_price = _validate_opportunity_direction(
        item
    )
    _finite(item.get("score", item.get("priority")), field="Max-Pain score")
    official_price = _finite(
        (archive_reference.get("official_price") or {}).get("price"),
        field="archived official price",
    )
    if current_price != official_price:
        raise ValueError("Max-Pain current price does not match the frozen archive")
    _validate_frozen_opportunity_target(item, archive_reference)
    base = research_event_capture.build_maxpain_event(
        _stage4_maxpain_payload(item),
        event_type=MAX_PAIN_EVENT_TYPE,
        event_time=decision_time,
        strategy_version=STRATEGY_VERSION,
        code_version=code_version,
    )
    if (
        base.symbol != symbol
        or base.direction != direction
        or base.current_price != current_price
        or base.target_price != target_price
    ):
        raise ValueError("Max-Pain event normalization changed directional identity")
    metadata = _signal_metadata(
        family="MAX_PAIN",
        tier=tier,
        archive_reference=archive_reference,
        derivatives_reference=derivatives_reference,
        decision_time=decision_time,
    )
    event = research_event_capture.build_decision_sample(
        symbol=base.symbol,
        sample_type=MAX_PAIN_EVENT_TYPE,
        direction=base.direction,
        source_side=base.source_side,
        timeframe=base.timeframe,
        score=base.score,
        current_price=base.current_price,
        target_price=base.target_price,
        categories=[*base.categories, "DECISION_SAMPLE", "SILENT", tier],
        engine_snapshot={**base.engine_snapshot, "signal_snapshot": metadata},
        setup_identity={
            "contract_version": CONTRACT_VERSION,
            "signal_family": "MAX_PAIN",
            "source_side": base.source_side,
        },
        event_time=decision_time,
        strategy_version=STRATEGY_VERSION,
        code_version=code_version,
    )
    return _stable_event(
        event,
        snapshot_key=str(archive_reference["snapshot_key"]),
        signal_family="MAX_PAIN",
        locator={"timeframe": base.timeframe, "source_side": base.source_side},
    )


def _magnet_event(
    observation: Mapping[str, Any],
    *,
    archive_reference: Mapping[str, Any],
    derivatives_reference: Mapping[str, Any],
    decision_time: datetime,
    code_version: Optional[str],
) -> research_event_capture.ResearchEvent:
    magnet = dict(observation.get("magnet") or {})
    confirmation = dict(observation.get("confirmation") or {})
    tier = str(confirmation.get("status") or "").upper()
    if tier not in QUALIFYING_TIERS:
        raise ValueError("Magnet event is not a qualifying confirmation")
    symbol, direction, current_price, target_price = _validate_magnet_direction(
        observation
    )
    _finite(magnet.get("magnet_quality"), field="magnet_quality")
    _finite(magnet.get("liquidity_edge_pct"), field="liquidity_edge_pct")
    official_price = _finite(
        (archive_reference.get("official_price") or {}).get("price"),
        field="archived official price",
    )
    if current_price != official_price:
        raise ValueError("Magnet current price does not match the frozen archive")
    _validate_frozen_magnet_targets(observation, archive_reference)
    expected_confirmation = magnet_v1.evaluate_confirmation(
        magnet, dict(observation.get("market_evidence") or {})
    )
    if _canonical(confirmation) != _canonical(expected_confirmation):
        raise ValueError("Magnet confirmation was not engine-derived")
    members = sorted({str(item) for item in magnet.get("members") or []})
    magnet["members"] = members
    base = research_event_capture.build_magnet_event(
        magnet,
        confirmation=confirmation,
        market_evidence=observation.get("market_evidence") or {},
        current_price=observation.get("current_price"),
        price_source=observation.get("price_source"),
        price_pair=observation.get("price_pair"),
        event_type=MAGNET_EVENT_TYPE,
        event_time=decision_time,
        strategy_version=STRATEGY_VERSION,
        code_version=code_version,
    )
    if (
        base.symbol != symbol
        or base.direction != direction
        or base.current_price != current_price
        or base.target_price != target_price
    ):
        raise ValueError("Magnet event normalization changed directional identity")
    metadata = _signal_metadata(
        family="MAGNET",
        tier=tier,
        archive_reference=archive_reference,
        derivatives_reference=derivatives_reference,
        decision_time=decision_time,
    )
    event = research_event_capture.build_decision_sample(
        symbol=base.symbol,
        sample_type=MAGNET_EVENT_TYPE,
        direction=base.direction,
        source_side=base.source_side,
        score=base.score,
        current_price=base.current_price,
        target_price=base.target_price,
        categories=["MAGNET", "DECISION_SAMPLE", "SILENT", tier],
        engine_snapshot={**base.engine_snapshot, "signal_snapshot": metadata},
        setup_identity={
            "contract_version": CONTRACT_VERSION,
            "signal_family": "MAGNET",
            "magnet_side": base.source_side,
            "members": members,
        },
        event_time=decision_time,
        strategy_version=STRATEGY_VERSION,
        code_version=code_version,
    )
    return _stable_event(
        event,
        snapshot_key=str(archive_reference["snapshot_key"]),
        signal_family="MAGNET",
        locator={"magnet_side": base.source_side, "members": members},
    )


def _scoring_rows_from_archive(
    rows_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]]
) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    for symbol in sorted(rows_by_symbol):
        for original in rows_by_symbol[symbol]:
            row = dict(original)
            row["distance_short_pct"] = row.get(
                "short_target_signed_distance_pct"
            )
            row["distance_long_pct"] = row.get(
                "long_target_signed_distance_pct"
            )
            rows.append(row)
    return rows


def _canonical_opportunities(
    rows: Sequence[Mapping[str, Any]],
    derivatives_snapshot: Mapping[str, Mapping[str, Any]],
) -> list[Dict[str, Any]]:
    values: list[Dict[str, Any]] = []
    scoring_rows = [dict(row) for row in rows]
    if not scoring_rows:
        return []
    forced_symbol = str(scoring_rows[0].get("symbol") or "").strip().upper()
    if not forced_symbol:
        raise ValueError("canonical Max-Pain rows are missing a symbol")
    for source_side in ("LONG", "SHORT"):
        values.extend(
            alert_engine.build_opportunities(
                scoring_rows,
                limit=max(1, len(scoring_rows)),
                forced_symbol=forced_symbol,
                forced_side=source_side,
            )
        )
    return market_confidence_engine.attach_to_opportunities(
        values,
        snapshot_by_symbol={
            symbol: dict(snapshot)
            for symbol, snapshot in derivatives_snapshot.items()
        },
    )


def _canonical_magnet_observations(
    rows: Sequence[Mapping[str, Any]],
    derivatives_snapshot: Mapping[str, Mapping[str, Any]],
) -> list[Dict[str, Any]]:
    rows_by_symbol: Dict[str, list[Dict[str, Any]]] = {}
    for original in rows:
        row = dict(original)
        symbol = str(row.get("symbol") or "").strip().upper()
        if symbol:
            rows_by_symbol.setdefault(symbol, []).append(row)
    observations: list[Dict[str, Any]] = []
    for magnet in magnet_v1.build_magnets(list(rows)):
        symbol = str(magnet.get("symbol") or "").strip().upper()
        captured = dict(derivatives_snapshot.get(symbol) or {})
        direction = magnet_v1.expected_price_direction(magnet.get("side"))
        evidence = market_confidence_engine.combine(
            symbol,
            direction,
            captured.get("regime") or {},
            captured.get("flow") or {},
            maxpain_score=0.0,
        )
        price_row = next(
            (
                row
                for row in rows_by_symbol.get(symbol, [])
                if row.get("current_price") is not None
            ),
            {},
        )
        observations.append(
            {
                "magnet": dict(magnet),
                "confirmation": magnet_v1.evaluate_confirmation(
                    magnet, evidence
                ),
                "market_evidence": evidence,
                "current_price": price_row.get("current_price"),
                "price_source": price_row.get("price_source"),
                "price_pair": price_row.get("price_pair"),
            }
        )
    return observations


def _assert_canonical_collection(
    supplied: Sequence[Mapping[str, Any]],
    expected: Sequence[Mapping[str, Any]],
    *,
    identity,
    field: str,
) -> None:
    if not supplied:
        return

    def indexed(values: Sequence[Mapping[str, Any]]) -> Dict[Any, Dict[str, Any]]:
        result: Dict[Any, Dict[str, Any]] = {}
        for value in values:
            item = dict(value)
            key = identity(item)
            if key in result:
                raise ValueError(f"{field} contains duplicate identity {key!r}")
            result[key] = item
        return result

    supplied_by_key = indexed(supplied)
    expected_by_key = indexed(expected)
    if set(supplied_by_key) != set(expected_by_key):
        raise ValueError(f"{field} does not contain the canonical engine set")
    for key, canonical_item in expected_by_key.items():
        if _canonical(supplied_by_key[key]) != _canonical(canonical_item):
            raise ValueError(
                f"{field} does not match frozen engine output for {key!r}"
            )


def _canonical_directional_evidence(
    derivatives_snapshot: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    result: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for symbol in sorted(derivatives_snapshot):
        captured = dict(derivatives_snapshot[symbol] or {})
        result[symbol] = {
            direction: market_confidence_engine.combine(
                symbol,
                "BULLISH" if direction == "LONG" else "BEARISH",
                captured.get("regime") or {},
                captured.get("flow") or {},
                maxpain_score=0.0,
            )
            for direction in ("LONG", "SHORT")
        }
    return result


def _combined_candidates(
    opportunities: Sequence[Mapping[str, Any]],
    magnet_observations: Sequence[Mapping[str, Any]],
    directional_market_evidence: Mapping[str, Mapping[str, Mapping[str, Any]]],
) -> list[Dict[str, Any]]:
    groups: Dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for item in opportunities:
        symbol, direction, _current, _target = _validate_opportunity_direction(
            item, field="Combined Max-Pain opportunity"
        )
        groups.setdefault((symbol, direction), []).append(item)

    for raw_symbol, by_direction in directional_market_evidence.items():
        symbol = str(raw_symbol or "").upper()
        if not isinstance(by_direction, Mapping):
            continue
        for raw_direction in by_direction:
            direction = str(raw_direction or "").upper()
            if symbol and direction in {"LONG", "SHORT"}:
                groups.setdefault((symbol, direction), [])

    magnets: Dict[tuple[str, str], Dict[str, Any]] = {}
    for observation in magnet_observations:
        confirmation = dict(observation.get("confirmation") or {})
        tier = str(confirmation.get("status") or "").upper()
        if tier not in QUALIFYING_TIERS:
            continue
        magnet = dict(observation.get("magnet") or {})
        symbol, direction, _current, _target = _validate_magnet_direction(
            observation, field="Combined Magnet observation"
        )
        candidate = _compact_magnet_component(
            tier=tier,
            magnet=magnet,
            confirmation=confirmation,
        )
        previous = magnets.get((symbol, direction))
        rank = (
            int(tier == "STRONG_CONFIRMED"),
            _finite(magnet.get("magnet_quality"), field="magnet_quality"),
            _finite(
                magnet.get("liquidity_edge_pct"),
                field="liquidity_edge_pct",
                optional=True,
            )
            or 0.0,
        )
        previous_magnet = dict((previous or {}).get("magnet") or {})
        previous_rank = (
            int(str((previous or {}).get("tier") or "") == "STRONG_CONFIRMED"),
            float(previous_magnet.get("magnet_quality") or 0.0),
            float(previous_magnet.get("liquidity_edge_pct") or 0.0),
        )
        if previous is None or rank > previous_rank:
            magnets[(symbol, direction)] = candidate
        groups.setdefault((symbol, direction), [])

    combined: list[Dict[str, Any]] = []
    for (symbol, direction), group in groups.items():
        ordered = sorted(
            group,
            key=lambda item: (
                -(
                    _finite(
                        item.get("score", item.get("priority", 0.0)),
                        field="Combined Max-Pain score",
                    )
                    or 0.0
                ),
                str(item.get("timeframe") or ""),
            ),
        )
        valid = [
            item
            for item in ordered
            if not list(item.get("calculation_validation_errors") or [])
        ]
        indication_families: set[str] = set()
        vote_sources: set[str] = set()
        maxpain_components: list[Dict[str, Any]] = []
        maxpain_component_items: list[Mapping[str, Any]] = []
        for item in valid:
            confirmation = dict(item.get("maxpain_confirmation") or {})
            tier = str(confirmation.get("status") or "").upper()
            score = _finite(
                item.get("score", item.get("priority", 0.0)), field="Max-Pain score"
            ) or 0.0
            types = sorted({str(value) for value in item.get("types") or []})
            near_share = _finite(
                item.get("near_share_pct"), field="near_share_pct", optional=True
            )
            reasons = []
            # Confirmation is the archive eligibility gate. The native
            # Max-Pain score is the family vote; tier remains quality metadata.
            if tier in QUALIFYING_TIERS:
                if score >= DERIVATIVES_HIGH_THRESHOLD:
                    reasons.append("MAX_PAIN_SCORE_65_NATIVE")
                if score > MAX_PAIN_HIGH_SCORE_THRESHOLD:
                    reasons.append("SCORE_OVER_80")
                if len(types) >= 3:
                    reasons.append("THREE_ANOMALIES")
                if near_share is not None and near_share >= 60.0:
                    reasons.append("LIQUIDITY_60")
            if reasons:
                maxpain_component_items.append(item)
                maxpain_components.append(
                    {
                        "timeframe": item.get("timeframe"),
                        "tier": tier or None,
                        "score": score,
                        "types": types,
                        "near_share_pct": near_share,
                        "reasons": reasons,
                    }
                )
        if maxpain_components:
            indication_families.add("MAX_PAIN")
            vote_sources.add("COINGLASS_MAX_PAIN")

        # Bind a CoinGlass Max-Pain vote to an event that is actually emitted.
        # A higher-ranked but non-qualifying observation must not become an
        # unsourced Combined top item.
        top = (
            maxpain_component_items[0]
            if maxpain_component_items
            else (valid[0] if valid else {})
        )
        evidence = dict(
            (directional_market_evidence.get(symbol) or {}).get(direction) or {}
        )
        if not evidence and top:
            evidence = dict(top.get("market_evidence") or {})
        if evidence:
            evidence = _validate_market_evidence(
                evidence,
                symbol=symbol,
                direction=direction,
                field="Combined directional evidence",
            )
        modules = dict(evidence.get("modules") or {})
        derivative_components: Dict[str, Any] = {}
        for module_key, family in (
            ("positioning", "PRICE_OI"),
            ("futures_flow", "FUTURES_CVD"),
        ):
            module = dict(modules.get(module_key) or {})
            score = _finite(
                module.get("score", 0.0), field=f"{family} score"
            ) or 0.0
            supports = (
                module.get("available") is True
                and str(module.get("relation") or "").upper() == "SUPPORT"
                and abs(score) >= DERIVATIVES_HIGH_THRESHOLD
            )
            derivative_components[family] = {
                "supports": supports,
                "score": score,
                "relation": module.get("relation"),
                "available": module.get("available"),
            }
            if supports:
                indication_families.add(family)
                vote_sources.add(family)

        magnet = magnets.get((symbol, direction))
        if magnet:
            indication_families.add("MAGNET")
            vote_sources.add("COINGLASS_MAX_PAIN")
        if len(vote_sources) < MIN_COMBINED_VOTES:
            continue
        source_side = "SHORT" if direction == "LONG" else "LONG"
        combined.append(
            {
                "symbol": symbol,
                "source_side": source_side,
                "direction": direction,
                "source_families": sorted(vote_sources),
                "indication_families": sorted(indication_families),
                "vote_count": len(vote_sources),
                "maxpain_components": maxpain_components,
                "derivative_components": derivative_components,
                "magnet_component": magnet,
                "spot_context": _json_safe(
                    evidence.get("spot_context") or {}
                ),
                "dependency_lineage": {
                    source: _json_safe(_SOURCE_DEPENDENCIES[source])
                    for source in sorted(vote_sources)
                },
                "top_item": top,
            }
        )
    return sorted(combined, key=lambda item: (item["symbol"], item["direction"]))


def _combined_event(
    candidate: Mapping[str, Any],
    *,
    archive_reference: Mapping[str, Any],
    derivatives_reference: Mapping[str, Any],
    decision_time: datetime,
    code_version: Optional[str],
) -> research_event_capture.ResearchEvent:
    top = dict(candidate.get("top_item") or {})
    symbol = str(candidate.get("symbol") or "").strip().upper()
    direction = _normalized_direction(
        candidate.get("direction"), field="Combined direction"
    )
    source_side = str(candidate.get("source_side") or "").strip().upper()
    if _direction_from_source_side(source_side) != direction:
        raise ValueError("Combined source side and direction disagree")
    base = (
        research_event_capture.build_maxpain_event(
            _stage4_maxpain_payload(top),
            event_type=COMBINED_EVENT_TYPE,
            event_time=decision_time,
            strategy_version=STRATEGY_VERSION,
            code_version=code_version,
        )
        if top
        else None
    )
    if top:
        top_symbol, top_direction, top_current, top_target = (
            _validate_opportunity_direction(
                top, field="Combined selected Max-Pain opportunity"
            )
        )
        if (
            top_symbol != symbol
            or top_direction != direction
            or base is None
            or base.symbol != symbol
            or base.direction != direction
            or base.current_price != top_current
            or base.target_price != top_target
        ):
            raise ValueError("Combined Max-Pain context changed directional identity")
        official_price = _finite(
            (archive_reference.get("official_price") or {}).get("price"),
            field="Combined archived official price",
        )
        if top_current != official_price:
            raise ValueError(
                "Combined current price does not match the frozen archive"
            )
        _validate_frozen_opportunity_target(top, archive_reference)
        current_price = top_current
        target_price = top_target
    else:
        current_price = _finite(
            (archive_reference.get("official_price") or {}).get("price"),
            field="Combined archived official price",
        )
        target_price = None
    metadata = _signal_metadata(
        family="COMBINED",
        tier="CONFIRMED",
        archive_reference=archive_reference,
        derivatives_reference=derivatives_reference,
        decision_time=decision_time,
    )
    families = sorted({str(item) for item in candidate.get("source_families") or []})
    if any(family not in VOTING_SOURCE_FAMILIES for family in families):
        raise ValueError("Combined contains an unsupported voting source family")
    if len(families) < MIN_COMBINED_VOTES:
        raise ValueError("Combined does not contain two independent source votes")
    vote_count = int(candidate.get("vote_count") or 0)
    if vote_count != len(families):
        raise ValueError("Combined vote count does not match deduplicated sources")
    indication_families = sorted(
        {str(item) for item in candidate.get("indication_families") or []}
    )
    if any(family not in INDICATION_FAMILIES for family in indication_families):
        raise ValueError("Combined contains an unsupported indication family")
    snapshot = {
        "signal_snapshot": metadata,
        "vote_count": vote_count,
        "source_families": families,
        "indication_families": indication_families,
        "source_vote_policy": "INDEPENDENT_RAW_SOURCE_FAMILIES_V1",
        "maxpain_components": _json_safe(candidate.get("maxpain_components") or []),
        "derivative_components": _json_safe(
            candidate.get("derivative_components") or {}
        ),
        "magnet_component": _json_safe(candidate.get("magnet_component") or {}),
        "spot_context": _json_safe(candidate.get("spot_context") or {}),
        "dependency_lineage": _json_safe(candidate.get("dependency_lineage") or {}),
        "top_item": base.engine_snapshot if base is not None else {},
    }
    event = research_event_capture.build_decision_sample(
        symbol=symbol,
        sample_type=COMBINED_EVENT_TYPE,
        direction=direction,
        source_side=source_side,
        # Combined is a symbol/direction setup.  The selected Max-Pain
        # timeframe is evidence, not setup identity.
        timeframe=None,
        score=base.score if base is not None else None,
        current_price=current_price,
        target_price=target_price,
        categories=[
            *families,
            *indication_families,
            "DECISION_SAMPLE",
            "SILENT",
        ],
        engine_snapshot=snapshot,
        setup_identity={
            "contract_version": CONTRACT_VERSION,
            "signal_family": "COMBINED",
        },
        event_time=decision_time,
        strategy_version=STRATEGY_VERSION,
        code_version=code_version,
    )
    return _stable_event(
        event,
        snapshot_key=str(archive_reference["snapshot_key"]),
        signal_family="COMBINED",
        locator={"source_side": candidate.get("source_side")},
    )


def _projection_event(
    *,
    set_record: Mapping[str, Any],
    eligible_symbols: Sequence[str],
    symbol_evaluations: Sequence[Mapping[str, Any]],
    decision_time: datetime,
    status: str,
    counts: Mapping[str, int],
    signal_events: Sequence[research_event_capture.ResearchEvent],
    derivatives_read_started_at: Optional[datetime] = None,
    derivatives_read_completed_at: Optional[datetime] = None,
    reason: Optional[str] = None,
    code_version: Optional[str] = None,
) -> research_event_capture.ResearchEvent:
    normalized_status = str(status or "").strip().upper()
    if normalized_status not in {"COMPLETED", "MISSED_CAUSAL_WINDOW"}:
        raise ValueError("invalid signal snapshot projection status")
    normalized_eligible = sorted({str(value).upper() for value in eligible_symbols})
    normalized_evaluations = sorted(
        (
            {
                "symbol": str(item.get("symbol") or "").strip().upper(),
                "status": str(item.get("status") or "").strip().upper(),
                "reason": (
                    str(item.get("reason") or "").strip() or None
                ),
            }
            for item in symbol_evaluations
        ),
        key=lambda item: item["symbol"],
    )
    evaluation_symbols = [item["symbol"] for item in normalized_evaluations]
    if (
        any(not symbol for symbol in evaluation_symbols)
        or len(evaluation_symbols) != len(set(evaluation_symbols))
        or evaluation_symbols != normalized_eligible
    ):
        raise ValueError(
            "symbol evaluations must partition every eligible archive symbol"
        )
    for item in normalized_evaluations:
        if item["status"] == "EVALUABLE":
            if item["reason"] is not None:
                raise ValueError("EVALUABLE symbol cannot carry a rejection reason")
        elif item["status"] == "UNEVALUABLE":
            if item["reason"] not in {
                "DERIVATIVES_SNAPSHOT_MISSING",
                "DERIVATIVES_SNAPSHOT_INVALID",
                "PRICE_OI_UNAVAILABLE",
                "PRICE_OI_STALE",
                "FUTURES_CVD_UNAVAILABLE",
                "MISSED_CAUSAL_WINDOW",
            }:
                raise ValueError("UNEVALUABLE symbol has an unsupported reason code")
        else:
            raise ValueError("invalid per-symbol evaluation status")
    evaluated_count = sum(
        int(item["status"] == "EVALUABLE") for item in normalized_evaluations
    )
    if normalized_status == "MISSED_CAUSAL_WINDOW":
        evaluation_status = "UNEVALUABLE"
        if evaluated_count or any(
            item["reason"] != "MISSED_CAUSAL_WINDOW"
            for item in normalized_evaluations
        ):
            raise ValueError("missed projection must mark every symbol unevaluable")
    elif evaluated_count == len(normalized_evaluations):
        evaluation_status = "EVALUABLE"
    elif evaluated_count:
        evaluation_status = "PARTIAL"
    else:
        evaluation_status = "UNEVALUABLE"
    projection = {
        "status": normalized_status,
        "evaluation_status": evaluation_status,
        "reason": str(reason or "").strip() or None,
        "snapshot_set_id": int(set_record["snapshot_set_id"]),
        "snapshot_key": set_record.get("snapshot_key"),
        "set_payload_sha256": set_record.get("payload_sha256"),
        "available_at_utc": set_record.get("available_at_utc"),
        "eligible_symbols": normalized_eligible,
        "symbol_evaluations": normalized_evaluations,
        "decision_time_utc": _iso(decision_time, field="decision_time_utc"),
        "derivatives_read_started_at_utc": (
            _iso(
                derivatives_read_started_at,
                field="derivatives_read_started_at_utc",
            )
            if derivatives_read_started_at is not None
            else None
        ),
        "derivatives_read_completed_at_utc": (
            _iso(
                derivatives_read_completed_at,
                field="derivatives_read_completed_at_utc",
            )
            if derivatives_read_completed_at is not None
            else None
        ),
        "counts": {key: int(value) for key, value in sorted(counts.items())},
        "signal_event_count": len(signal_events),
        "signal_events_payload_sha256": _signal_events_payload_sha256(
            signal_events
        ),
    }
    event = research_event_capture.build_decision_sample(
        symbol=PROJECTION_SYMBOL,
        sample_type=PROJECTION_EVENT_TYPE,
        direction="NEUTRAL",
        categories=["DECISION_SAMPLE", "SILENT", normalized_status],
        engine_snapshot={
            "signal_snapshot": {
                "contract_version": CONTRACT_VERSION,
                "signal_family": "PROJECTION",
                "tier": normalized_status,
                "formula_authorized": False,
                "outcome_authorized": False,
                "telegram_delivery_allowed": False,
                "trade_execution_allowed": False,
            },
            "projection": projection,
        },
        setup_identity={
            "contract_version": CONTRACT_VERSION,
            "signal_family": "PROJECTION",
            "scope": "SNAPSHOT_SET",
        },
        event_time=decision_time,
        strategy_version=STRATEGY_VERSION,
        code_version=code_version,
    )
    fingerprint = projection_event_fingerprint(str(set_record["snapshot_key"]))
    return replace(event, event_fingerprint=fingerprint)


def build_missed_projection_event(
    *,
    archive_payload: Mapping[str, Any],
    archive_persistence: Mapping[str, Any],
    observed_at_utc: Any,
    reason: str = "projection was not completed inside the causal window",
    code_version: Optional[str] = None,
) -> research_event_capture.ResearchEvent:
    """Create a terminal receipt without inventing historical derivatives."""

    set_record, manifests, _rows_by_symbol = _validated_archive(
        archive_payload, archive_persistence
    )
    observed_at = _utc(observed_at_utc, field="observed_at_utc")
    available_at = _utc(set_record.get("available_at_utc"), field="available_at_utc")
    if observed_at - available_at <= timedelta(minutes=MAX_DECISION_LAG_MINUTES):
        raise ValueError("causal projection window is still open")
    event = _projection_event(
        set_record=set_record,
        eligible_symbols=tuple(sorted(manifests)),
        symbol_evaluations=tuple(
            {
                "symbol": symbol,
                "status": "UNEVALUABLE",
                "reason": "MISSED_CAUSAL_WINDOW",
            }
            for symbol in sorted(manifests)
        ),
        decision_time=observed_at,
        status="MISSED_CAUSAL_WINDOW",
        counts={"max_pain": 0, "magnet": 0, "combined": 0},
        signal_events=(),
        reason=reason,
        code_version=code_version,
    )
    research_event_capture.validate_event(event)
    return event


def build_signal_snapshot_batch(
    *,
    archive_payload: Mapping[str, Any],
    archive_persistence: Mapping[str, Any],
    opportunities: Iterable[Mapping[str, Any]],
    magnet_observations: Iterable[Mapping[str, Any]],
    derivatives_snapshot: Mapping[str, Mapping[str, Any]],
    directional_market_evidence: Optional[
        Mapping[str, Mapping[str, Mapping[str, Any]]]
    ] = None,
    derivatives_read_started_at_utc: Any,
    derivatives_read_completed_at_utc: Any,
    decision_time_utc: Any,
    code_version: Optional[str] = None,
) -> SignalSnapshotBatch:
    """Build one non-deliverable batch with fail-closed per-symbol evidence."""

    set_record, manifests, rows_by_symbol = _validated_archive(
        archive_payload, archive_persistence
    )
    available_at = _utc(set_record.get("available_at_utc"), field="available_at_utc")
    read_started = _utc(
        derivatives_read_started_at_utc, field="derivatives_read_started_at_utc"
    )
    read_completed = _utc(
        derivatives_read_completed_at_utc, field="derivatives_read_completed_at_utc"
    )
    decision_time = _utc(decision_time_utc, field="decision_time_utc")
    if available_at > read_started:
        raise ValueError("derivatives were read before the Max-Pain archive was available")
    if read_started > read_completed:
        raise ValueError("derivatives read completion precedes its start")
    if decision_time < read_completed:
        raise ValueError("decision time precedes completion of derivative reads")
    if decision_time - available_at > timedelta(minutes=MAX_DECISION_LAG_MINUTES):
        raise ValueError("signal decision is too late for the frozen Max-Pain snapshot")

    derivative_refs: Dict[str, Dict[str, Any]] = {}
    archive_refs: Dict[str, Dict[str, Any]] = {}
    canonical_opportunities: list[Dict[str, Any]] = []
    canonical_magnets: list[Dict[str, Any]] = []
    canonical_directional: Dict[str, Dict[str, Dict[str, Any]]] = {}
    symbol_evaluations: list[Dict[str, Any]] = []
    for symbol in sorted(manifests):
        manifest = manifests[symbol]
        if symbol not in derivatives_snapshot:
            symbol_evaluations.append(
                {
                    "symbol": symbol,
                    "status": "UNEVALUABLE",
                    "reason": "DERIVATIVES_SNAPSHOT_MISSING",
                }
            )
            continue
        try:
            raw_derivatives = derivatives_snapshot[symbol]
            if not isinstance(raw_derivatives, Mapping):
                raise TypeError("derivatives snapshot must be a mapping")
            derivative_reference = _derivative_reference(
                symbol=symbol,
                snapshot=raw_derivatives,
                read_started_at=read_started,
                read_completed_at=read_completed,
                decision_time=decision_time,
            )
            # Treat provider payloads as a per-symbol trust boundary.  Run every
            # engine derivation while the symbol is still isolated so a malformed
            # nested shape cannot abort terminalization for otherwise valid peers.
            symbol_rows = _scoring_rows_from_archive(
                {symbol: rows_by_symbol[symbol]}
            )
            symbol_derivatives = {symbol: dict(raw_derivatives)}
            symbol_opportunities = _canonical_opportunities(
                symbol_rows, symbol_derivatives
            )
            symbol_magnets = _canonical_magnet_observations(
                symbol_rows, symbol_derivatives
            )
            symbol_directional = _canonical_directional_evidence(
                symbol_derivatives
            )
        except Exception as exc:
            symbol_evaluations.append(
                {
                    "symbol": symbol,
                    "status": "UNEVALUABLE",
                    "reason": (
                        _derivative_rejection_code(exc)
                        if isinstance(exc, ValueError)
                        else "DERIVATIVES_SNAPSHOT_INVALID"
                    ),
                }
            )
            continue
        archive_refs[symbol] = _archive_reference(
            set_record=set_record,
            manifest=manifest,
            rows=rows_by_symbol[symbol],
        )
        derivative_refs[symbol] = derivative_reference
        canonical_opportunities.extend(symbol_opportunities)
        canonical_magnets.extend(symbol_magnets)
        canonical_directional[symbol] = symbol_directional[symbol]
        symbol_evaluations.append(
            {"symbol": symbol, "status": "EVALUABLE", "reason": None}
        )

    evaluated_symbols = set(derivative_refs)

    supplied_opportunities = [
        dict(item)
        for item in opportunities
        if str(item.get("symbol") or "").upper() in evaluated_symbols
    ]
    supplied_magnets = [
        dict(item)
        for item in magnet_observations
        if str((item.get("magnet") or {}).get("symbol") or "").upper()
        in evaluated_symbols
    ]
    opportunity_list = canonical_opportunities
    magnet_list = canonical_magnets
    _assert_canonical_collection(
        supplied_opportunities,
        opportunity_list,
        identity=lambda item: (
            str(item.get("symbol") or "").upper(),
            str(item.get("timeframe") or ""),
            str(item.get("side") or "").upper(),
        ),
        field="Max-Pain opportunities",
    )
    _assert_canonical_collection(
        supplied_magnets,
        magnet_list,
        identity=lambda item: (
            str((item.get("magnet") or {}).get("symbol") or "").upper(),
            str((item.get("magnet") or {}).get("side") or "").upper(),
            tuple(sorted(str(value) for value in (
                (item.get("magnet") or {}).get("members") or []
            ))),
        ),
        field="Magnet observations",
    )
    events: list[research_event_capture.ResearchEvent] = []
    counts = {"max_pain": 0, "magnet": 0, "combined": 0}

    for item in opportunity_list:
        status = str((item.get("maxpain_confirmation") or {}).get("status") or "").upper()
        if status not in QUALIFYING_TIERS:
            continue
        symbol = str(item.get("symbol") or "").upper()
        events.append(
            _maxpain_event(
                item,
                archive_reference=archive_refs[symbol],
                derivatives_reference=derivative_refs[symbol],
                decision_time=decision_time,
                code_version=code_version,
            )
        )
        counts["max_pain"] += 1

    for observation in magnet_list:
        status = str((observation.get("confirmation") or {}).get("status") or "").upper()
        if status not in QUALIFYING_TIERS:
            continue
        symbol = str((observation.get("magnet") or {}).get("symbol") or "").upper()
        events.append(
            _magnet_event(
                observation,
                archive_reference=archive_refs[symbol],
                derivatives_reference=derivative_refs[symbol],
                decision_time=decision_time,
                code_version=code_version,
            )
        )
        counts["magnet"] += 1

    directional_evidence = canonical_directional
    for raw_symbol, by_direction in (directional_market_evidence or {}).items():
        symbol = str(raw_symbol).strip().upper()
        if symbol not in evaluated_symbols or not isinstance(by_direction, Mapping):
            continue
        for raw_direction, evidence in by_direction.items():
            direction = str(raw_direction).strip().upper()
            if direction not in {"LONG", "SHORT"}:
                raise ValueError(
                    f"unsupported directional evidence key: {raw_direction!r}"
                )
            if not isinstance(evidence, Mapping):
                raise ValueError(
                    f"directional evidence for {symbol} {direction} must be a mapping"
                )
            supplied = _validate_market_evidence(
                evidence,
                symbol=symbol,
                direction=direction,
                field=f"directional evidence for {symbol} {direction}",
            )
            if _canonical(supplied) != _canonical(
                directional_evidence[symbol][direction]
            ):
                raise ValueError(
                    f"directional evidence for {symbol} {direction} "
                    "does not match the frozen derivatives snapshot"
                )
    for candidate in _combined_candidates(
        opportunity_list, magnet_list, directional_evidence
    ):
        symbol = str(candidate["symbol"])
        events.append(
            _combined_event(
                candidate,
                archive_reference=archive_refs[symbol],
                derivatives_reference=derivative_refs[symbol],
                decision_time=decision_time,
                code_version=code_version,
            )
        )
        counts["combined"] += 1

    signal_events = tuple(events)
    events.append(
        _projection_event(
            set_record=set_record,
            eligible_symbols=tuple(sorted(manifests)),
            symbol_evaluations=tuple(symbol_evaluations),
            decision_time=decision_time,
            status="COMPLETED",
            counts=counts,
            signal_events=signal_events,
            derivatives_read_started_at=read_started,
            derivatives_read_completed_at=read_completed,
            code_version=code_version,
        )
    )

    fingerprints = [event.event_fingerprint for event in events]
    if len(fingerprints) != len(set(fingerprints)):
        raise ValueError("signal snapshot batch contains duplicate event identities")
    for event in events:
        research_event_capture.validate_event(event)
        if event.event_kind != "DECISION_SAMPLE":
            raise ValueError("signal snapshot attempted to create a deliverable event")

    events.sort(
        key=lambda event: (
            event.event_type == PROJECTION_EVENT_TYPE,
            event.symbol,
            event.direction,
            event.event_type,
            event.timeframe or "",
            event.source_side or "",
        )
    )
    return SignalSnapshotBatch(
        events=tuple(events),
        snapshot_key=str(set_record["snapshot_key"]),
        snapshot_set_id=int(set_record["snapshot_set_id"]),
        decision_time_utc=_iso(decision_time, field="decision_time_utc"),
        eligible_symbols=tuple(sorted(manifests)),
        evaluated_symbols=tuple(sorted(evaluated_symbols)),
        unevaluable_symbols=tuple(sorted(set(manifests) - evaluated_symbols)),
        evaluation_status=(
            "EVALUABLE"
            if len(evaluated_symbols) == len(manifests)
            else "PARTIAL"
            if evaluated_symbols
            else "UNEVALUABLE"
        ),
        counts=counts,
    )
