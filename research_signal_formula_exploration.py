"""Pure Stage-4 signal/Wave-v5 cohort contract for future Formula research.

This module deliberately stops before candidate search.  It turns one supplied
terminal Stage-4 projection into two immutable observations (LONG and SHORT)
for every evaluable symbol, including explicit no-signal observations.  It can
then bind only same-slot, already-causal Wave-v5 memberships and closed Stage-4
outcomes whose local contracts validate.

The resulting objects are research inputs, never Formula registry authority.
Wave identities are grouping metadata, not predicates, and a missing outcome
is an explicit coverage blocker rather than a failure or a zero return.
Authenticity of the supplied database rows is intentionally outside this pure
module; readiness therefore remains fail-closed unless its caller supplies the
migration-attested reader and implemented label/search capability flags.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import re
import struct
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence


POLICY_VERSION = "stage4-wave-v5-formula-exploration-cohort-v1"
FEATURE_SCHEMA_VERSION = "stage4-signal-presence-features-v1"
WAVE_BINDING_POLICY_VERSION = "stage4-cycle-plus-2m-wave-v5-binding-v1"
OUTCOME_BINDING_POLICY_VERSION = "stage4-exact-closed-outcome-binding-v2"

STAGE4_CONTRACT_VERSION = "research-signal-snapshot-v1"
STAGE4_STRATEGY_VERSION = "signal-snapshot-v1"
STAGE4_CAPTURE_STAGE = "SILENT_SIGNAL_SNAPSHOT"
STAGE4_OUTCOME_METHOD_VERSION = (
    "canonical-spot-1m-ohlc-path-v3+stage4-frozen-archive-input-v1"
)
STAGE4_OUTCOME_ADMISSION_POLICY_VERSION = (
    "stage4-signal-completed-projection-derived-admission-v1"
)
STAGE4_OUTCOME_REFERENCE_POLICY_VERSION = (
    "stage4-signal-frozen-archive-price-reference-v1"
)
STAGE4_NO_SIGNAL_OUTCOME_METHOD_VERSION = (
    "canonical-spot-1m-ohlc-path-v3+stage4-no-signal-frozen-archive-input-v1"
)
STAGE4_NO_SIGNAL_OUTCOME_ADMISSION_POLICY_VERSION = (
    "stage4-no-signal-completed-projection-evaluable-cell-admission-v1"
)
STAGE4_NO_SIGNAL_OUTCOME_REFERENCE_POLICY_VERSION = (
    "stage4-no-signal-frozen-archive-price-reference-v1"
)
STAGE4_OUTCOME_SEMANTICS = (
    "post_decision_path_metrics_relative_to_frozen_archive_input_price;"
    "not_trade_entry_return"
)
WAVE_CONTRACT_VERSION = "market-movement-v5-causal-neutral-price-wave"
NO_SIGNAL_ABSENCE_BASIS = (
    "COMPLETED_PROJECTION_EVALUABLE_SYMBOL_WITHOUT_SIGNAL"
)

MAX_PAIN_EVENT_TYPE = "MAX_PAIN_CONFIRMATION_STATE"
MAGNET_EVENT_TYPE = "MAGNET_CONFIRMATION_STATE"
COMBINED_EVENT_TYPE = "SILENT_COMBINED_CONFIRMATION_SNAPSHOT"
PROJECTION_EVENT_TYPE = "SIGNAL_SNAPSHOT_PROJECTION"
SIGNAL_EVENT_TYPES = (
    MAX_PAIN_EVENT_TYPE,
    MAGNET_EVENT_TYPE,
    COMBINED_EVENT_TYPE,
)
_EVENT_FAMILY = {
    MAX_PAIN_EVENT_TYPE: "MAX_PAIN",
    MAGNET_EVENT_TYPE: "MAGNET",
    COMBINED_EVENT_TYPE: "COMBINED",
}
_COUNT_KEY = {
    MAX_PAIN_EVENT_TYPE: "max_pain",
    MAGNET_EVENT_TYPE: "magnet",
    COMBINED_EVENT_TYPE: "combined",
}

EXPLORATION_MIN_BTC_PARENT_MOVEMENTS = 5
MATURITY_MIN_BTC_PARENT_MOVEMENTS = 20

AUTHORITY_EFFECT = "NONE"
DELIVERY_CHANNEL = "NONE"

FEATURE_MAX_PAIN_CONFIRMED = "stage4.max_pain.confirmed"
FEATURE_MAX_PAIN_STRONG = "stage4.max_pain.strong_confirmed"
FEATURE_MAGNET_CONFIRMED = "stage4.magnet.confirmed"
FEATURE_MAGNET_STRONG = "stage4.magnet.strong_confirmed"
FEATURE_COMBINED_CONFIRMED = "stage4.combined.confirmed"
FEATURE_COMBINED_COINGLASS = "stage4.combined.source.coinglass_max_pain"
FEATURE_COMBINED_PRICE_OI = "stage4.combined.source.price_oi"
FEATURE_COMBINED_FUTURES_CVD = "stage4.combined.source.futures_cvd"
FEATURE_COMBINED_VOTE_COUNT = "stage4.combined.vote_count"

ALLOWED_FEATURES = (
    FEATURE_MAX_PAIN_CONFIRMED,
    FEATURE_MAX_PAIN_STRONG,
    FEATURE_MAGNET_CONFIRMED,
    FEATURE_MAGNET_STRONG,
    FEATURE_COMBINED_CONFIRMED,
    FEATURE_COMBINED_COINGLASS,
    FEATURE_COMBINED_PRICE_OI,
    FEATURE_COMBINED_FUTURES_CVD,
    FEATURE_COMBINED_VOTE_COUNT,
)

_FEATURE_SOURCE_CLOSURE = {
    FEATURE_MAX_PAIN_CONFIRMED: frozenset(
        {"COINGLASS_MAX_PAIN", "PRICE_OI", "FUTURES_CVD"}
    ),
    FEATURE_MAX_PAIN_STRONG: frozenset(
        {"COINGLASS_MAX_PAIN", "PRICE_OI", "FUTURES_CVD"}
    ),
    FEATURE_MAGNET_CONFIRMED: frozenset(
        {"COINGLASS_MAX_PAIN", "PRICE_OI", "FUTURES_CVD"}
    ),
    FEATURE_MAGNET_STRONG: frozenset(
        {"COINGLASS_MAX_PAIN", "PRICE_OI", "FUTURES_CVD"}
    ),
    FEATURE_COMBINED_CONFIRMED: frozenset(
        {"COINGLASS_MAX_PAIN", "PRICE_OI", "FUTURES_CVD"}
    ),
    FEATURE_COMBINED_COINGLASS: frozenset({"COINGLASS_MAX_PAIN"}),
    FEATURE_COMBINED_PRICE_OI: frozenset({"PRICE_OI"}),
    FEATURE_COMBINED_FUTURES_CVD: frozenset({"FUTURES_CVD"}),
    FEATURE_COMBINED_VOTE_COUNT: frozenset(
        {"COINGLASS_MAX_PAIN", "PRICE_OI", "FUTURES_CVD"}
    ),
}

_PROJECTION_KEYS = frozenset(
    {
        "status",
        "evaluation_status",
        "reason",
        "snapshot_set_id",
        "snapshot_key",
        "set_payload_sha256",
        "available_at_utc",
        "eligible_symbols",
        "symbol_evaluations",
        "decision_time_utc",
        "derivatives_read_started_at_utc",
        "derivatives_read_completed_at_utc",
        "counts",
        "signal_event_count",
        "signal_events_payload_sha256",
    }
)
_PROJECTION_SIGNAL_METADATA_KEYS = frozenset(
    {
        "contract_version",
        "signal_family",
        "tier",
        "formula_authorized",
        "outcome_authorized",
        "telegram_delivery_allowed",
        "trade_execution_allowed",
    }
)
_SIGNAL_METADATA_KEYS = frozenset(
    {
        "contract_version",
        "signal_family",
        "tier",
        "decision_time_utc",
        "archive_reference",
        "derivatives_reference",
        "dependency_lineage",
        "formula_authorized",
        "outcome_authorized",
        "telegram_delivery_allowed",
        "trade_execution_allowed",
    }
)
_AUTHORITY_KEYS = (
    "formula_authorized",
    "outcome_authorized",
    "telegram_delivery_allowed",
    "trade_execution_allowed",
)
_SIGNAL_COMMITMENT_FIELDS = (
    "schema_version",
    "event_kind",
    "event_type",
    "alert_time_utc",
    "symbol",
    "direction",
    "source_side",
    "timeframe",
    "score",
    "current_price",
    "target_price",
    "initial_target_distance_pct",
    "categories",
    "setup_key",
    "event_fingerprint",
    "strategy_version",
    "code_version",
    "engine_snapshot",
)
_SIGNAL_FLOAT_FIELDS = frozenset(
    {"score", "current_price", "target_price", "initial_target_distance_pct"}
)
_SIGNAL_SET_COMMITMENT_VERSION = "research-signal-event-set-v1"
_COMPLETE_QUALITIES = frozenset(
    {
        "VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES",
        "VERIFIED_HYPERLIQUID_SPOT_1M_CLOSED_CANDLES",
    }
)
_BINANCE_PAIR_ALIASES = {"1000PEPE": "PEPEUSDT"}
_OBSERVATION_KEYS = frozenset(
    {
        "policy_version",
        "feature_schema_version",
        "projection_event_id",
        "projection_event_fingerprint",
        "snapshot_set_id",
        "snapshot_key",
        "projection_decision_time_utc",
        "archive_cycle_time_utc",
        "cohort_evaluable_symbols",
        "cohort_expected_observation_count",
        "projection_signal_event_count",
        "projection_signal_events_payload_sha256",
        "symbol",
        "direction",
        "symbol_evaluation_status",
        "symbol_evaluation_reason",
        "features",
        "source_families",
        "source_event_ids",
        "source_event_fingerprints",
        "explicit_no_signal",
        "absence_basis",
        "wave_binding",
        "outcome",
        "authority_effect",
        "formula_registry_effect",
        "delivery_channel",
        "live_eligible",
        "telegram_delivery_allowed",
        "trade_execution_allowed",
    }
)
_PATH_LABEL_FIELDS = (
    "reference_price",
    "price_at_horizon",
    "raw_return_pct",
    "directional_return_pct",
    "max_favorable_price",
    "max_adverse_price",
    "mfe_pct",
    "mae_pct",
    "time_to_first_progress_seconds",
    "time_to_mfe_seconds",
    "path_resolution_seconds",
    "path_samples",
    "outcome_method_version",
    "data_quality_status",
    "price_source",
)

_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL = re.compile(r"^[A-Z0-9-]{1,20}$")
_UTC = timezone.utc


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
    return parsed.astimezone(_UTC)


def _iso(value: Any, *, field: str) -> str:
    return _utc(value, field=field).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _positive_int(value: Any, *, field: str) -> int:
    if type(value) is not int or value <= 0 or value > 2**63 - 1:
        raise ValueError(f"{field} must be a positive signed-64-bit integer")
    return value


def _hash(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or _HEX_64.fullmatch(value.strip()) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
    return value.strip()


def _symbol(value: Any, *, field: str = "symbol") -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a symbol string")
    normalized = value.strip().upper()
    if _SYMBOL.fullmatch(normalized) is None:
        raise ValueError(f"invalid {field}: {value!r}")
    return normalized


def _mapping(value: Any, *, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be an object")
    return value


def _sequence(value: Any, *, field: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"{field} must be an array")
    return value


def _finite(value: Any, *, field: str, optional: bool = False) -> Optional[float]:
    if value is None and optional:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a finite number")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field} must be a finite number") from exc
    if not math.isfinite(result):
        raise ValueError(f"{field} must be a finite number")
    return 0.0 if result == 0.0 else result


def _finite_float_bits(value: Any, *, field: str) -> float:
    """Validate a float while preserving the sign bit of negative zero."""

    if isinstance(value, bool):
        raise ValueError(f"{field} must be a finite number")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field} must be a finite number") from exc
    if not math.isfinite(result):
        raise ValueError(f"{field} must be a finite number")
    return result


def _json_value(value: Any, *, path: str = "root") -> Any:
    if value is None or type(value) in (str, bool, int):
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError(f"{path} contains a non-finite number")
        if value != value.to_integral():
            raise ValueError(f"{path} contains an unsupported fractional Decimal")
        return int(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} contains a non-finite number")
        return 0.0 if value == 0.0 else value
    if isinstance(value, datetime):
        return _iso(value, field=path)
    if isinstance(value, Mapping):
        normalized: Dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise ValueError(f"{path} contains an invalid object key")
            normalized[key] = _json_value(item, path=f"{path}.{key}")
        return {key: normalized[key] for key in sorted(normalized)}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [
            _json_value(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    raise ValueError(f"{path} contains unsupported type {type(value).__name__}")


def canonical_json(value: Any) -> str:
    return json.dumps(
        _json_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _fingerprint(kind: str, payload: Mapping[str, Any]) -> str:
    envelope = {
        "policy_version": POLICY_VERSION,
        "kind": kind,
        "payload": dict(payload),
    }
    return hashlib.sha256(canonical_json(envelope).encode("utf-8")).hexdigest()


def _key_count(value: Any, target: str) -> int:
    if isinstance(value, Mapping):
        return int(target in value) + sum(
            _key_count(item, target) for item in value.values()
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return sum(_key_count(item, target) for item in value)
    return 0


def _authority_metadata(
    engine_snapshot: Mapping[str, Any], *, projection: bool
) -> Mapping[str, Any]:
    snapshot = _mapping(
        engine_snapshot.get("signal_snapshot"), field="signal_snapshot"
    )
    expected = (
        _PROJECTION_SIGNAL_METADATA_KEYS if projection else _SIGNAL_METADATA_KEYS
    )
    if frozenset(snapshot) != expected:
        raise ValueError("Stage-4 signal_snapshot has an unexpected shape")
    for key in _AUTHORITY_KEYS:
        if snapshot.get(key) is not False or _key_count(engine_snapshot, key) != 1:
            raise ValueError(f"Stage-4 {key} must occur exactly once and be false")
    if snapshot.get("contract_version") != STAGE4_CONTRACT_VERSION:
        raise ValueError("Stage-4 contract version mismatch")
    return snapshot


def _base_stage4_event(row: Mapping[str, Any], *, projection: bool) -> Dict[str, Any]:
    if row.get("schema_version") != "research-event-v1":
        raise ValueError("Stage-4 event schema version mismatch")
    if row.get("event_kind") != "DECISION_SAMPLE":
        raise ValueError("Stage-4 cohort accepts only DECISION_SAMPLE")
    if row.get("delivery_status") != "NOT_APPLICABLE":
        raise ValueError("Stage-4 cohort accepts only NOT_APPLICABLE delivery")
    if row.get("capture_stage") != STAGE4_CAPTURE_STAGE:
        raise ValueError("Stage-4 capture stage mismatch")
    if row.get("strategy_version") != STAGE4_STRATEGY_VERSION:
        raise ValueError("Stage-4 strategy version mismatch")
    if row.get("delivery_attempted_at_utc") is not None or row.get(
        "delivered_at_utc"
    ) is not None:
        raise ValueError("silent Stage-4 event cannot carry delivery timestamps")
    event_id = _positive_int(row.get("event_id"), field="event_id")
    event_fingerprint = _hash(
        row.get("event_fingerprint"), field="event_fingerprint"
    )
    _hash(row.get("setup_key"), field="setup_key")
    decision = _utc(row.get("alert_time_utc"), field="alert_time_utc")
    code_version = str(row.get("code_version") or "").strip()
    runtime_session_id = str(row.get("runtime_session_id") or "").strip()
    if not code_version or not runtime_session_id:
        raise ValueError("Stage-4 code/runtime identity is required")
    categories = [
        str(item)
        for item in _sequence(row.get("categories"), field="categories")
    ]
    if len(categories) != len(set(categories)) or categories != sorted(categories):
        raise ValueError("Stage-4 categories must be sorted and unique")
    if not {"DECISION_SAMPLE", "SILENT"}.issubset(categories):
        raise ValueError("Stage-4 event lacks silent decision categories")
    engine_snapshot = _mapping(row.get("engine_snapshot"), field="engine_snapshot")
    metadata = _authority_metadata(engine_snapshot, projection=projection)
    return {
        "event_id": event_id,
        "event_fingerprint": event_fingerprint,
        "decision": decision,
        "decision_iso": _iso(decision, field="alert_time_utc"),
        "code_version": code_version,
        "runtime_session_id": runtime_session_id,
        "categories": categories,
        "engine_snapshot": engine_snapshot,
        "metadata": metadata,
    }


def _commitment_number(value: Any) -> str:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("signal commitment contains an invalid number") from exc
    if not number.is_finite():
        raise ValueError("signal commitment contains a non-finite number")
    text = format(number, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in {"", "-0"} else text


def _commitment_canonical(value: Any) -> str:
    if value is None:
        return "n"
    if type(value) is bool:
        return "b1" if value else "b0"
    if isinstance(value, (int, float, Decimal)):
        text = _commitment_number(value)
        return f"d{len(text.encode('utf-8'))}:{text}"
    if isinstance(value, str):
        return f"s{len(value.encode('utf-8'))}:{value}"
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return f"a{len(value)}:" + "".join(
            _commitment_canonical(item) for item in value
        )
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise ValueError("signal commitment object keys must be strings")
        keys = sorted(value, key=lambda key: key.encode("utf-8"))
        return f"o{len(keys)}:" + "".join(
            f"k{len(key.encode('utf-8'))}:{key}"
            + _commitment_canonical(value[key])
            for key in keys
        )
    raise ValueError(
        f"signal commitment contains unsupported type {type(value).__name__}"
    )


def _signal_event_payload(row: Mapping[str, Any]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    for field in _SIGNAL_COMMITMENT_FIELDS:
        if field not in row:
            raise ValueError(f"signal event is missing commitment field {field}")
        value = row[field]
        if field == "alert_time_utc":
            value = _iso(value, field="signal alert_time_utc")
        elif field in {"setup_key", "event_fingerprint"}:
            value = _hash(value, field=f"signal {field}")
        if field in _SIGNAL_FLOAT_FIELDS:
            value = (
                None
                if value is None
                else struct.pack(">d", _finite_float_bits(value, field=field)).hex()
            )
        payload[field] = _json_value(value, path=f"signal.{field}")
    return payload


def signal_event_set_commitment(signal_events: Sequence[Mapping[str, Any]]) -> str:
    """Recompute the exact Stage-4 committed sibling-event set digest."""

    row_hashes: list[tuple[str, str]] = []
    for row in signal_events:
        fingerprint = _hash(
            row.get("event_fingerprint"), field="signal event_fingerprint"
        )
        payload = _signal_event_payload(row)
        row_hash = hashlib.sha256(
            _commitment_canonical(payload).encode("utf-8")
        ).hexdigest()
        row_hashes.append((fingerprint, row_hash))
    row_hashes.sort()
    if len({fingerprint for fingerprint, _ in row_hashes}) != len(row_hashes):
        raise ValueError("duplicate Stage-4 signal event fingerprint")
    material = (
        f"{_SIGNAL_SET_COMMITMENT_VERSION}:{len(row_hashes)}:"
        + "".join(row_hash for _, row_hash in row_hashes)
    )
    return hashlib.sha256(material.encode("ascii")).hexdigest()


def _validate_observation_body(body: Mapping[str, Any]) -> None:
    if frozenset(body) != _OBSERVATION_KEYS:
        raise ValueError("exploration observation has an unexpected shape")
    _positive_int(body.get("projection_event_id"), field="projection_event_id")
    _hash(
        body.get("projection_event_fingerprint"),
        field="projection_event_fingerprint",
    )
    _positive_int(body.get("snapshot_set_id"), field="snapshot_set_id")
    _hash(body.get("snapshot_key"), field="snapshot_key")
    evaluable_symbols = [
        _symbol(value, field="cohort evaluable symbol")
        for value in _sequence(
            body.get("cohort_evaluable_symbols"),
            field="cohort_evaluable_symbols",
        )
    ]
    if evaluable_symbols != sorted(set(evaluable_symbols)) or not evaluable_symbols:
        raise ValueError("cohort evaluable symbols must be sorted and unique")
    expected_count = body.get("cohort_expected_observation_count")
    if type(expected_count) is not int or expected_count != 2 * len(evaluable_symbols):
        raise ValueError("cohort expected observation count is inconsistent")
    signal_count = body.get("projection_signal_event_count")
    if type(signal_count) is not int or signal_count < 0:
        raise ValueError("projection signal event count is invalid")
    _hash(
        body.get("projection_signal_events_payload_sha256"),
        field="projection_signal_events_payload_sha256",
    )
    decision = _utc(
        body.get("projection_decision_time_utc"),
        field="projection_decision_time_utc",
    )
    cycle = _utc(body.get("archive_cycle_time_utc"), field="archive_cycle_time_utc")
    if cycle.minute not in {0, 30} or cycle.second or cycle.microsecond or decision < cycle:
        raise ValueError("exploration observation has invalid cycle/decision timing")
    if _symbol(body.get("symbol")) not in evaluable_symbols:
        raise ValueError("observation symbol is absent from its cohort manifest")
    if body.get("direction") not in {"LONG", "SHORT"}:
        raise ValueError("exploration observation direction is invalid")
    if body.get("symbol_evaluation_status") != "EVALUABLE" or body.get(
        "symbol_evaluation_reason"
    ) is not None:
        raise ValueError("exploration observation is not an evaluable cohort cell")

    features = _mapping(body.get("features"), field="features")
    if frozenset(features) != frozenset(ALLOWED_FEATURES):
        raise ValueError("exploration observation feature shape mismatch")
    for name in ALLOWED_FEATURES:
        if name == FEATURE_COMBINED_VOTE_COUNT:
            continue
        if type(features.get(name)) is not bool:
            raise ValueError(f"exploration feature {name} must be boolean")
    if features[FEATURE_MAX_PAIN_STRONG] and not features[
        FEATURE_MAX_PAIN_CONFIRMED
    ]:
        raise ValueError("strong Max-Pain feature requires confirmed presence")
    if features[FEATURE_MAGNET_STRONG] and not features[FEATURE_MAGNET_CONFIRMED]:
        raise ValueError("strong Magnet feature requires confirmed presence")
    combined_present = features[FEATURE_COMBINED_CONFIRMED]
    combined_sources = sum(
        int(features[name])
        for name in (
            FEATURE_COMBINED_COINGLASS,
            FEATURE_COMBINED_PRICE_OI,
            FEATURE_COMBINED_FUTURES_CVD,
        )
    )
    combined_votes = features[FEATURE_COMBINED_VOTE_COUNT]
    if combined_present:
        if type(combined_votes) is not int or combined_votes not in {2, 3}:
            raise ValueError("present Combined feature requires two or three votes")
        if combined_votes != combined_sources:
            raise ValueError("Combined feature vote count is not deduplicated")
    elif combined_votes is not None or combined_sources:
        raise ValueError("absent Combined feature cannot carry sources or vote count")

    source_ids = list(_sequence(body.get("source_event_ids"), field="source_event_ids"))
    if any(type(value) is not int or value <= 0 for value in source_ids):
        raise ValueError("source_event_ids must contain positive integers")
    source_fingerprints = list(
        _sequence(
            body.get("source_event_fingerprints"),
            field="source_event_fingerprints",
        )
    )
    for value in source_fingerprints:
        _hash(value, field="source_event_fingerprint")
    if (
        len(source_ids) != len(source_fingerprints)
        or len(source_ids) != len(set(source_ids))
        or source_fingerprints != sorted(set(source_fingerprints))
    ):
        raise ValueError("source event identities are not unique and deterministic")
    if type(body.get("explicit_no_signal")) is not bool:
        raise ValueError("explicit_no_signal must be boolean")
    any_signal = bool(
        features[FEATURE_MAX_PAIN_CONFIRMED]
        or features[FEATURE_MAGNET_CONFIRMED]
        or features[FEATURE_COMBINED_CONFIRMED]
    )
    if body.get("explicit_no_signal") != (not source_ids) or any_signal != bool(
        source_ids
    ):
        raise ValueError("signal identities, presence features and absence disagree")
    if body.get("absence_basis") != "COMPLETED_PROJECTION_EVALUABLE_SYMBOL":
        raise ValueError("signal absence basis mismatch")

    source_families = list(
        _sequence(body.get("source_families"), field="source_families")
    )
    expected_sources: set[str] = set()
    if features[FEATURE_MAX_PAIN_CONFIRMED] or features[FEATURE_MAGNET_CONFIRMED]:
        expected_sources.add("COINGLASS_MAX_PAIN")
    for name, source in (
        (FEATURE_COMBINED_COINGLASS, "COINGLASS_MAX_PAIN"),
        (FEATURE_COMBINED_PRICE_OI, "PRICE_OI"),
        (FEATURE_COMBINED_FUTURES_CVD, "FUTURES_CVD"),
    ):
        if features[name]:
            expected_sources.add(source)
    if source_families != sorted(expected_sources):
        raise ValueError("observation source families do not match its features")

    if (
        body.get("authority_effect") != AUTHORITY_EFFECT
        or body.get("formula_registry_effect") != "NONE"
        or body.get("delivery_channel") != DELIVERY_CHANNEL
        or body.get("live_eligible") is not False
        or body.get("telegram_delivery_allowed") is not False
        or body.get("trade_execution_allowed") is not False
    ):
        raise ValueError("exploration observation cannot carry downstream authority")

    binding = _mapping(body.get("wave_binding"), field="wave_binding")
    if binding.get("role") != "INDEPENDENCE_ONLY_NOT_FORMULA_PREDICATE":
        raise ValueError("Wave binding role mismatch")
    binding_status = binding.get("status")
    if binding_status == "UNBOUND":
        if frozenset(binding) != frozenset({"status", "reason", "role"}):
            raise ValueError("UNBOUND Wave shape mismatch")
    elif binding_status == "UNAVAILABLE":
        if frozenset(binding) != frozenset(
            {"status", "reason", "policy_version", "expected_eligible_at_utc", "role"}
        ) or binding.get("policy_version") != WAVE_BINDING_POLICY_VERSION:
            raise ValueError("UNAVAILABLE Wave shape mismatch")
        _utc(binding.get("expected_eligible_at_utc"), field="expected Wave slot")
    elif binding_status == "BOUND":
        expected_binding_keys = {
            "status",
            "reason",
            "policy_version",
            "expected_eligible_at_utc",
            "symbol_membership_receipt_sha256",
            "symbol_transition_receipt_sha256",
            "symbol_stream_id",
            "symbol_movement_id",
            "btc_parent_membership_receipt_sha256",
            "btc_parent_transition_receipt_sha256",
            "btc_parent_stream_id",
            "btc_parent_movement_id",
            "role",
        }
        if frozenset(binding) != frozenset(expected_binding_keys):
            raise ValueError("BOUND Wave shape mismatch")
        if binding.get("policy_version") != WAVE_BINDING_POLICY_VERSION or binding.get(
            "reason"
        ) is not None:
            raise ValueError("BOUND Wave policy/reason mismatch")
        expected_slot = cycle + timedelta(minutes=2)
        if _utc(
            binding.get("expected_eligible_at_utc"), field="expected Wave slot"
        ) != expected_slot:
            raise ValueError("BOUND Wave slot does not match archive cycle")
        for name in expected_binding_keys - {
            "status",
            "reason",
            "policy_version",
            "expected_eligible_at_utc",
            "role",
        }:
            _hash(binding.get(name), field=name)
    else:
        raise ValueError("Wave binding status is invalid")

    outcome = _mapping(body.get("outcome"), field="outcome")
    if outcome.get("label_fields_exposed_as_features") is not False:
        raise ValueError("outcome labels may not be exposed as features")
    outcome_status = outcome.get("status")
    if outcome_status == "UNBOUND":
        if frozenset(outcome) != frozenset(
            {"status", "reason", "label_fields_exposed_as_features"}
        ):
            raise ValueError("UNBOUND outcome shape mismatch")
    elif outcome_status == "OUTCOME_UNAVAILABLE":
        if frozenset(outcome) != frozenset(
            {
                "status",
                "reason",
                "policy_version",
                "horizon_minutes",
                "label_fields_exposed_as_features",
            }
        ) or outcome.get("policy_version") != OUTCOME_BINDING_POLICY_VERSION:
            raise ValueError("unavailable outcome shape mismatch")
        if type(outcome.get("horizon_minutes")) is not int or outcome.get(
            "horizon_minutes"
        ) not in {60, 240, 720, 1440}:
            raise ValueError("unavailable outcome horizon mismatch")
        if not isinstance(outcome.get("reason"), str) or not outcome.get(
            "reason"
        ):
            raise ValueError("unavailable outcome requires a reason")
    elif outcome_status == "AVAILABLE":
        if frozenset(outcome) != frozenset(
            {
                "status",
                "reason",
                "policy_version",
                "horizon_minutes",
                "carrier_type",
                "carrier_payload_sha256",
                "source_event_ids",
                "path",
                "measured_at_utc",
                "label_fields_exposed_as_features",
            }
        ) or outcome.get("policy_version") != OUTCOME_BINDING_POLICY_VERSION:
            raise ValueError("available outcome shape mismatch")
        if outcome.get("reason") is not None:
            raise ValueError("available outcome cannot carry a reason")
        horizon = outcome.get("horizon_minutes")
        if type(horizon) is not int or horizon not in {60, 240, 720, 1440}:
            raise ValueError("available outcome horizon mismatch")
        carrier_type = outcome.get("carrier_type")
        if carrier_type not in {
            "STAGE4_SIGNAL_EVENTS",
            "STAGE4_NO_SIGNAL_CELL",
        }:
            raise ValueError("available outcome carrier type is invalid")
        carrier_payload_sha256 = outcome.get("carrier_payload_sha256")
        no_signal_carrier = carrier_type == "STAGE4_NO_SIGNAL_CELL"
        if body.get("explicit_no_signal") is True and not no_signal_carrier:
            raise ValueError(
                "no-signal observation cannot carry a Stage-4 signal outcome"
            )
        if body.get("explicit_no_signal") is not True and no_signal_carrier:
            raise ValueError(
                "signal-bearing observation cannot carry a no-signal outcome"
            )
        if no_signal_carrier:
            _hash(
                carrier_payload_sha256,
                field="carrier_payload_sha256",
            )
        elif carrier_payload_sha256 is not None:
            raise ValueError("signal outcome cannot carry a cell payload hash")
        if list(outcome.get("source_event_ids") or []) != sorted(source_ids):
            raise ValueError("available outcome source identities mismatch")
        if frozenset(_mapping(outcome.get("path"), field="outcome path")) != frozenset(
            _PATH_LABEL_FIELDS
        ):
            raise ValueError("available outcome path shape mismatch")
        measured = _utc(
            outcome.get("measured_at_utc"), field="outcome measured_at_utc"
        )
        normalized = _normalized_outcome(
            {
                **_mapping(outcome.get("path"), field="outcome path"),
                "horizon_minutes": outcome.get("horizon_minutes"),
                "measured_at_utc": measured,
            },
            horizon_minutes=horizon,
            event_time=decision,
            analysis_as_of_utc=decision + timedelta(minutes=horizon),
            direction=str(body.get("direction")),
            symbol=_symbol(body.get("symbol")),
            snapshot_set_id=_positive_int(
                body.get("snapshot_set_id"), field="snapshot_set_id"
            ),
            snapshot_key=_hash(body.get("snapshot_key"), field="snapshot_key"),
            outcome_method_version=(
                STAGE4_NO_SIGNAL_OUTCOME_METHOD_VERSION
                if no_signal_carrier
                else STAGE4_OUTCOME_METHOD_VERSION
            ),
            reference_policy_version=(
                STAGE4_NO_SIGNAL_OUTCOME_REFERENCE_POLICY_VERSION
                if no_signal_carrier
                else STAGE4_OUTCOME_REFERENCE_POLICY_VERSION
            ),
            admission_policy_version=(
                STAGE4_NO_SIGNAL_OUTCOME_ADMISSION_POLICY_VERSION
                if no_signal_carrier
                else STAGE4_OUTCOME_ADMISSION_POLICY_VERSION
            ),
        )
        if normalized["measured_at_utc"] != _iso(
            measured, field="outcome measured_at_utc"
        ):
            raise ValueError("available outcome measured time mismatch")
    else:
        raise ValueError("outcome status is invalid")


def _projection_contract(
    projection_event: Mapping[str, Any],
    archive_set: Mapping[str, Any],
    *,
    analysis_as_of_utc: Any,
) -> Dict[str, Any]:
    base = _base_stage4_event(projection_event, projection=True)
    if projection_event.get("event_type") != PROJECTION_EVENT_TYPE:
        raise ValueError("expected one Stage-4 projection event")
    if projection_event.get("symbol") != "RESEARCH" or projection_event.get(
        "direction"
    ) != "NEUTRAL":
        raise ValueError("Stage-4 projection identity is invalid")
    if any(
        projection_event.get(field) is not None
        for field in (
            "source_side",
            "timeframe",
            "score",
            "current_price",
            "target_price",
            "initial_target_distance_pct",
        )
    ):
        raise ValueError("Stage-4 projection carries forbidden signal values")
    metadata = base["metadata"]
    if metadata.get("signal_family") != "PROJECTION" or metadata.get(
        "tier"
    ) != "COMPLETED":
        raise ValueError("Stage-4 projection metadata is not COMPLETED")
    if base["categories"] != ["COMPLETED", "DECISION_SAMPLE", "SILENT"]:
        raise ValueError("Stage-4 projection categories are not exact")
    engine_snapshot = base["engine_snapshot"]
    if frozenset(engine_snapshot) != frozenset({"signal_snapshot", "projection"}):
        raise ValueError("Stage-4 projection engine snapshot has unknown fields")
    projection = _mapping(engine_snapshot.get("projection"), field="projection")
    if frozenset(projection) != _PROJECTION_KEYS:
        raise ValueError("Stage-4 projection receipt has an unexpected shape")
    if projection.get("status") != "COMPLETED" or projection.get("reason") is not None:
        raise ValueError("Stage-4 projection receipt is not terminal COMPLETED")
    if projection.get("evaluation_status") not in {
        "EVALUABLE",
        "PARTIAL",
        "UNEVALUABLE",
    }:
        raise ValueError("Stage-4 projection evaluation status is invalid")
    if _utc(projection.get("decision_time_utc"), field="projection decision") != base[
        "decision"
    ]:
        raise ValueError("Stage-4 projection decision time mismatch")
    as_of = _utc(analysis_as_of_utc, field="analysis_as_of_utc")
    if base["decision"] > as_of:
        raise ValueError("Stage-4 projection is after analysis_as_of_utc")

    snapshot_set_id = _positive_int(
        projection.get("snapshot_set_id"), field="projection snapshot_set_id"
    )
    snapshot_key = _hash(projection.get("snapshot_key"), field="snapshot_key")
    set_payload_sha256 = _hash(
        projection.get("set_payload_sha256"), field="set_payload_sha256"
    )
    _hash(
        projection.get("signal_events_payload_sha256"),
        field="signal_events_payload_sha256",
    )
    available_at = _utc(
        projection.get("available_at_utc"), field="projection available_at_utc"
    )
    read_started = _utc(
        projection.get("derivatives_read_started_at_utc"),
        field="derivatives_read_started_at_utc",
    )
    read_completed = _utc(
        projection.get("derivatives_read_completed_at_utc"),
        field="derivatives_read_completed_at_utc",
    )
    if not available_at <= read_started <= read_completed <= base["decision"]:
        raise ValueError("Stage-4 projection read timestamps are not causal")

    archive_id = _positive_int(
        archive_set.get("snapshot_set_id"), field="archive snapshot_set_id"
    )
    if archive_id != snapshot_set_id:
        raise ValueError("projection/archive snapshot_set_id mismatch")
    if _hash(archive_set.get("snapshot_key"), field="archive snapshot_key") != snapshot_key:
        raise ValueError("projection/archive snapshot_key mismatch")
    if (
        _hash(archive_set.get("payload_sha256"), field="archive payload_sha256")
        != set_payload_sha256
    ):
        raise ValueError("projection/archive payload hash mismatch")
    if archive_set.get("source") != "RESEARCH_PASSIVE" or archive_set.get(
        "research_eligible"
    ) is not True:
        raise ValueError("Stage-4 archive is not research-eligible passive input")
    archive_available = _utc(
        archive_set.get("available_at_utc"), field="archive available_at_utc"
    )
    if archive_available != available_at:
        raise ValueError("projection/archive availability mismatch")
    cycle = _utc(archive_set.get("cycle_time_utc"), field="archive cycle_time_utc")
    if cycle.minute not in {0, 30} or cycle.second != 0 or cycle.microsecond != 0:
        raise ValueError("archive cycle must be on the exact :00/:30 lattice")
    if cycle > available_at or base["decision"] - available_at > timedelta(minutes=15):
        raise ValueError("Stage-4 archive/projection timing is outside its causal window")

    eligible = [
        _symbol(item, field="eligible symbol")
        for item in _sequence(
            projection.get("eligible_symbols"), field="eligible_symbols"
        )
    ]
    if eligible != sorted(set(eligible)) or not eligible:
        raise ValueError("eligible_symbols must be non-empty, sorted and unique")
    evaluation_rows = _sequence(
        projection.get("symbol_evaluations"), field="symbol_evaluations"
    )
    evaluations: Dict[str, Dict[str, Any]] = {}
    allowed_reasons = {
        "DERIVATIVES_SNAPSHOT_MISSING",
        "DERIVATIVES_SNAPSHOT_INVALID",
        "PRICE_OI_UNAVAILABLE",
        "PRICE_OI_STALE",
        "FUTURES_CVD_UNAVAILABLE",
    }
    for raw in evaluation_rows:
        item = _mapping(raw, field="symbol evaluation")
        if frozenset(item) != frozenset({"symbol", "status", "reason"}):
            raise ValueError("symbol evaluation has an unexpected shape")
        symbol = _symbol(item.get("symbol"), field="evaluation symbol")
        status = item.get("status")
        reason = item.get("reason")
        if symbol in evaluations:
            raise ValueError("duplicate symbol evaluation")
        if status == "EVALUABLE" and reason is not None:
            raise ValueError("EVALUABLE symbol cannot have a reason")
        if status == "UNEVALUABLE" and reason not in allowed_reasons:
            raise ValueError("UNEVALUABLE symbol has an invalid reason")
        if status not in {"EVALUABLE", "UNEVALUABLE"}:
            raise ValueError("symbol evaluation status is invalid")
        evaluations[symbol] = {"status": status, "reason": reason}
    if list(evaluations) != eligible:
        raise ValueError("symbol evaluations must exactly partition eligible symbols")
    evaluable_count = sum(
        int(item["status"] == "EVALUABLE") for item in evaluations.values()
    )
    expected_status = (
        "EVALUABLE"
        if evaluable_count == len(evaluations)
        else "PARTIAL" if evaluable_count else "UNEVALUABLE"
    )
    if projection.get("evaluation_status") != expected_status:
        raise ValueError("projection aggregate evaluation status is inconsistent")

    counts = _mapping(projection.get("counts"), field="projection counts")
    if frozenset(counts) != frozenset({"max_pain", "magnet", "combined"}):
        raise ValueError("projection counts have an unexpected shape")
    normalized_counts: Dict[str, int] = {}
    for key in ("max_pain", "magnet", "combined"):
        value = counts.get(key)
        if type(value) is not int or value < 0 or value > 2**63 - 1:
            raise ValueError(f"projection count {key} is invalid")
        normalized_counts[key] = value
    signal_count = projection.get("signal_event_count")
    if type(signal_count) is not int or signal_count < 0:
        raise ValueError("projection signal_event_count is invalid")
    if signal_count != sum(normalized_counts.values()):
        raise ValueError("projection signal count total is inconsistent")
    return {
        **base,
        "projection": projection,
        "snapshot_set_id": snapshot_set_id,
        "snapshot_key": snapshot_key,
        "cycle": cycle,
        "cycle_iso": _iso(cycle, field="archive cycle_time_utc"),
        "evaluations": evaluations,
        "counts": normalized_counts,
        "signal_count": signal_count,
    }


def _signal_contract(
    row: Mapping[str, Any], *, projection: Mapping[str, Any]
) -> Dict[str, Any]:
    base = _base_stage4_event(row, projection=False)
    event_type = str(row.get("event_type") or "")
    if event_type not in SIGNAL_EVENT_TYPES:
        raise ValueError(f"unsupported Stage-4 signal type: {event_type!r}")
    family = _EVENT_FAMILY[event_type]
    metadata = base["metadata"]
    if metadata.get("signal_family") != family:
        raise ValueError("Stage-4 signal type/family mismatch")
    tier = str(metadata.get("tier") or "")
    if event_type == COMBINED_EVENT_TYPE:
        if tier != "CONFIRMED":
            raise ValueError("Combined Stage-4 tier must be CONFIRMED")
    elif tier not in {"CONFIRMED", "STRONG_CONFIRMED"}:
        raise ValueError("Stage-4 signal tier is invalid")
    if event_type != COMBINED_EVENT_TYPE and tier not in base["categories"]:
        raise ValueError("Stage-4 signal categories omit its tier")
    if base["decision"] != projection["decision"]:
        raise ValueError("Stage-4 signal/projection decision mismatch")
    if base["code_version"] != projection["code_version"] or base[
        "runtime_session_id"
    ] != projection["runtime_session_id"]:
        raise ValueError("Stage-4 signal/projection runtime identity mismatch")
    symbol = _symbol(row.get("symbol"))
    direction = str(row.get("direction") or "").upper()
    if direction not in {"LONG", "SHORT"}:
        raise ValueError("Stage-4 signal direction is invalid")
    evaluation = projection["evaluations"].get(symbol)
    if not evaluation or evaluation["status"] != "EVALUABLE":
        raise ValueError("Stage-4 signal is not in the evaluable projection partition")
    archive = _mapping(metadata.get("archive_reference"), field="archive_reference")
    if _positive_int(
        archive.get("snapshot_set_id"), field="signal snapshot_set_id"
    ) != projection["snapshot_set_id"]:
        raise ValueError("Stage-4 signal/projection snapshot_set_id mismatch")
    if _hash(archive.get("snapshot_key"), field="signal snapshot_key") != projection[
        "snapshot_key"
    ]:
        raise ValueError("Stage-4 signal/projection snapshot_key mismatch")
    if _utc(metadata.get("decision_time_utc"), field="signal decision_time_utc") != base[
        "decision"
    ]:
        raise ValueError("Stage-4 signal metadata decision mismatch")

    source_families: tuple[str, ...] = ()
    vote_count: Optional[int] = None
    if event_type == COMBINED_EVENT_TYPE:
        engine = base["engine_snapshot"]
        if engine.get("source_vote_policy") != "INDEPENDENT_RAW_SOURCE_FAMILIES_V1":
            raise ValueError("Combined source-vote policy mismatch")
        raw_sources = _sequence(
            engine.get("source_families"), field="Combined source_families"
        )
        source_families = tuple(str(item) for item in raw_sources)
        allowed_sources = {
            "COINGLASS_MAX_PAIN",
            "PRICE_OI",
            "FUTURES_CVD",
        }
        if (
            list(source_families) != sorted(set(source_families))
            or len(source_families) < 2
            or any(item not in allowed_sources for item in source_families)
        ):
            raise ValueError("Combined source families are not canonical")
        vote_count = engine.get("vote_count")
        if type(vote_count) is not int or vote_count != len(source_families):
            raise ValueError("Combined vote count is not deduplicated")
    return {
        **base,
        "event_type": event_type,
        "family": family,
        "tier": tier,
        "symbol": symbol,
        "direction": direction,
        "source_families": source_families,
        "vote_count": vote_count,
    }


@dataclass(frozen=True)
class ExplorationObservation:
    """Immutable, content-addressed Stage-4 cohort observation."""

    observation_id: str
    _payload_json: str

    def __post_init__(self) -> None:
        try:
            body = json.loads(self._payload_json)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError("exploration observation payload is invalid JSON") from exc
        if canonical_json(body) != self._payload_json:
            raise ValueError("exploration observation payload is not canonical")
        if body.get("policy_version") != POLICY_VERSION:
            raise ValueError("exploration observation policy mismatch")
        if body.get("feature_schema_version") != FEATURE_SCHEMA_VERSION:
            raise ValueError("exploration feature schema mismatch")
        _validate_observation_body(body)
        expected = _fingerprint("exploration-observation", body)
        if self.observation_id != expected:
            raise ValueError("exploration observation fingerprint mismatch")

    @classmethod
    def _from_payload(cls, payload: Mapping[str, Any]) -> "ExplorationObservation":
        body = dict(_json_value(payload, path="observation"))
        body.pop("observation_id", None)
        if body.get("policy_version") != POLICY_VERSION:
            raise ValueError("exploration observation policy mismatch")
        if body.get("feature_schema_version") != FEATURE_SCHEMA_VERSION:
            raise ValueError("exploration feature schema mismatch")
        _validate_observation_body(body)
        identifier = _fingerprint("exploration-observation", body)
        return cls(identifier, canonical_json(body))

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ExplorationObservation":
        declared = _hash(value.get("observation_id"), field="observation_id")
        result = cls._from_payload(value)
        if result.observation_id != declared:
            raise ValueError("exploration observation fingerprint mismatch")
        return result

    def to_dict(self) -> Dict[str, Any]:
        return {"observation_id": self.observation_id, **json.loads(self._payload_json)}


def _feature_values(signals: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    max_pain = [item for item in signals if item["event_type"] == MAX_PAIN_EVENT_TYPE]
    magnet = [item for item in signals if item["event_type"] == MAGNET_EVENT_TYPE]
    combined = [item for item in signals if item["event_type"] == COMBINED_EVENT_TYPE]
    if len(combined) > 1:
        raise ValueError("one cohort cell cannot contain duplicate Combined signals")
    combined_signal = combined[0] if combined else None
    sources = set(combined_signal["source_families"] if combined_signal else ())
    return {
        FEATURE_MAX_PAIN_CONFIRMED: bool(max_pain),
        FEATURE_MAX_PAIN_STRONG: any(
            item["tier"] == "STRONG_CONFIRMED" for item in max_pain
        ),
        FEATURE_MAGNET_CONFIRMED: bool(magnet),
        FEATURE_MAGNET_STRONG: any(
            item["tier"] == "STRONG_CONFIRMED" for item in magnet
        ),
        FEATURE_COMBINED_CONFIRMED: combined_signal is not None,
        FEATURE_COMBINED_COINGLASS: "COINGLASS_MAX_PAIN" in sources,
        FEATURE_COMBINED_PRICE_OI: "PRICE_OI" in sources,
        FEATURE_COMBINED_FUTURES_CVD: "FUTURES_CVD" in sources,
        FEATURE_COMBINED_VOTE_COUNT: (
            combined_signal["vote_count"] if combined_signal is not None else None
        ),
    }


def build_stage4_frames(
    projection_event: Mapping[str, Any],
    archive_set: Mapping[str, Any],
    signal_events: Sequence[Mapping[str, Any]],
    *,
    analysis_as_of_utc: Any,
) -> tuple[ExplorationObservation, ...]:
    """Expand one supplied, locally validated projection into an unbiased grid."""

    projection = _projection_contract(
        projection_event, archive_set, analysis_as_of_utc=analysis_as_of_utc
    )
    raw_signals = list(_sequence(signal_events, field="signal_events"))
    if len(raw_signals) != projection["signal_count"]:
        raise ValueError("supplied Stage-4 siblings do not match committed event count")
    if (
        signal_event_set_commitment(raw_signals)
        != projection["projection"]["signal_events_payload_sha256"]
    ):
        raise ValueError("supplied Stage-4 siblings do not match committed event payload")
    signals = [
        _signal_contract(_mapping(row, field="signal event"), projection=projection)
        for row in raw_signals
    ]
    if len({item["event_id"] for item in signals}) != len(signals):
        raise ValueError("duplicate Stage-4 signal event_id")
    actual_counts = {"max_pain": 0, "magnet": 0, "combined": 0}
    for signal in signals:
        actual_counts[_COUNT_KEY[signal["event_type"]]] += 1
    if actual_counts != projection["counts"]:
        raise ValueError("supplied Stage-4 sibling family counts do not match projection")

    grouped: Dict[tuple[str, str], list[Dict[str, Any]]] = {}
    for signal in signals:
        grouped.setdefault((signal["symbol"], signal["direction"]), []).append(signal)

    observations: list[ExplorationObservation] = []
    evaluable_symbols = sorted(
        symbol
        for symbol, evaluation in projection["evaluations"].items()
        if evaluation["status"] == "EVALUABLE"
    )
    for symbol, evaluation in projection["evaluations"].items():
        if evaluation["status"] != "EVALUABLE":
            continue
        for direction in ("LONG", "SHORT"):
            cell_signals = sorted(
                grouped.get((symbol, direction), []),
                key=lambda item: (item["event_fingerprint"], item["event_id"]),
            )
            features = _feature_values(cell_signals)
            source_families: set[str] = set()
            if any(
                item["event_type"] in {MAX_PAIN_EVENT_TYPE, MAGNET_EVENT_TYPE}
                for item in cell_signals
            ):
                source_families.add("COINGLASS_MAX_PAIN")
            for signal in cell_signals:
                source_families.update(signal["source_families"])
            payload = {
                "policy_version": POLICY_VERSION,
                "feature_schema_version": FEATURE_SCHEMA_VERSION,
                "projection_event_id": projection["event_id"],
                "projection_event_fingerprint": projection["event_fingerprint"],
                "snapshot_set_id": projection["snapshot_set_id"],
                "snapshot_key": projection["snapshot_key"],
                "projection_decision_time_utc": projection["decision_iso"],
                "archive_cycle_time_utc": projection["cycle_iso"],
                "cohort_evaluable_symbols": evaluable_symbols,
                "cohort_expected_observation_count": 2 * len(evaluable_symbols),
                "projection_signal_event_count": projection["signal_count"],
                "projection_signal_events_payload_sha256": projection[
                    "projection"
                ]["signal_events_payload_sha256"],
                "symbol": symbol,
                "direction": direction,
                "symbol_evaluation_status": "EVALUABLE",
                "symbol_evaluation_reason": None,
                "features": features,
                "source_families": sorted(source_families),
                "source_event_ids": [item["event_id"] for item in cell_signals],
                "source_event_fingerprints": [
                    item["event_fingerprint"] for item in cell_signals
                ],
                "explicit_no_signal": not cell_signals,
                "absence_basis": "COMPLETED_PROJECTION_EVALUABLE_SYMBOL",
                "wave_binding": {
                    "status": "UNBOUND",
                    "reason": "WAVE_BINDING_NOT_ATTEMPTED",
                    "role": "INDEPENDENCE_ONLY_NOT_FORMULA_PREDICATE",
                },
                "outcome": {
                    "status": "UNBOUND",
                    "reason": "CLOSED_OUTCOME_BINDING_NOT_ATTEMPTED",
                    "label_fields_exposed_as_features": False,
                },
                "authority_effect": AUTHORITY_EFFECT,
                "formula_registry_effect": "NONE",
                "delivery_channel": DELIVERY_CHANNEL,
                "live_eligible": False,
                "telegram_delivery_allowed": False,
                "trade_execution_allowed": False,
            }
            observations.append(ExplorationObservation._from_payload(payload))
    observations.sort(
        key=lambda item: (
            item.to_dict()["projection_event_id"],
            item.to_dict()["snapshot_key"],
            item.to_dict()["symbol"],
            item.to_dict()["direction"],
        )
    )
    return tuple(observations)


def _validated_wave_rows(
    memberships: Sequence[Mapping[str, Any]],
    transitions: Sequence[Mapping[str, Any]],
    *,
    analysis_as_of_utc: datetime,
) -> list[Dict[str, Any]]:
    transition_by_receipt: Dict[str, Mapping[str, Any]] = {}
    for raw in transitions:
        row = _mapping(raw, field="Wave transition")
        if row.get("contract_version") != WAVE_CONTRACT_VERSION:
            raise ValueError("Wave transition contract mismatch")
        receipt = _hash(
            row.get("transition_receipt_sha256"),
            field="transition_receipt_sha256",
        )
        if receipt in transition_by_receipt:
            raise ValueError("duplicate Wave transition receipt")
        namespace = row.get("namespace")
        symbol = _symbol(row.get("symbol"), field="Wave transition symbol")
        if namespace not in {"SYMBOL", "BTC_PARENT"}:
            raise ValueError("Wave transition namespace is invalid")
        if namespace == "BTC_PARENT" and symbol != "BTC":
            raise ValueError("BTC_PARENT transition must use BTC")
        stream_id = _hash(row.get("stream_id"), field="Wave transition stream_id")
        movement_id = _hash(
            row.get("movement_id"), field="Wave transition movement_id"
        )
        trigger_anchor_id = _hash(
            row.get("trigger_anchor_id"), field="Wave trigger_anchor_id"
        )
        trigger_eligible = _utc(
            row.get("trigger_eligible_at_utc"), field="Wave trigger eligibility"
        )
        trigger_decision = _utc(
            row.get("trigger_decision_time_utc"), field="Wave trigger decision"
        )
        if (
            trigger_eligible.minute not in {2, 32}
            or trigger_eligible.second
            or trigger_eligible.microsecond
            or not trigger_eligible
            <= trigger_decision
            < trigger_eligible + timedelta(minutes=30)
        ):
            raise ValueError("Wave transition timing is outside the exact slot")
        if trigger_decision > analysis_as_of_utc:
            raise ValueError("Wave transition is after analysis_as_of_utc")
        transition_by_receipt[receipt] = {
            "stream_id": stream_id,
            "movement_id": movement_id,
            "namespace": namespace,
            "symbol": symbol,
            "trigger_anchor_id": trigger_anchor_id,
            "trigger_eligible_at_utc": trigger_eligible,
            "trigger_decision_time_utc": trigger_decision,
        }

    joined: list[Dict[str, Any]] = []
    seen_memberships: set[str] = set()
    for raw in memberships:
        row = _mapping(raw, field="Wave membership")
        if row.get("contract_version") != WAVE_CONTRACT_VERSION:
            raise ValueError("Wave membership contract mismatch")
        receipt = _hash(
            row.get("membership_receipt_sha256"),
            field="membership_receipt_sha256",
        )
        if receipt in seen_memberships:
            raise ValueError("duplicate Wave membership receipt")
        seen_memberships.add(receipt)
        transition_receipt = _hash(
            row.get("emitted_by_transition_receipt_sha256"),
            field="emitted_by_transition_receipt_sha256",
        )
        transition = transition_by_receipt.get(transition_receipt)
        if transition is None:
            raise ValueError("Wave membership lacks its emitting transition")
        stream_id = _hash(row.get("stream_id"), field="Wave membership stream_id")
        movement_id = _hash(
            row.get("movement_id"), field="Wave membership movement_id"
        )
        anchor_id = _hash(row.get("anchor_id"), field="Wave membership anchor_id")
        _hash(
            row.get("anchor_receipt_sha256"),
            field="Wave membership anchor_receipt_sha256",
        )
        eligible = _utc(row.get("eligible_at_utc"), field="Wave membership eligibility")
        decision = _utc(row.get("decision_time_utc"), field="Wave membership decision")
        if (
            eligible.minute not in {2, 32}
            or eligible.second
            or eligible.microsecond
            or not eligible <= decision < eligible + timedelta(minutes=30)
        ):
            raise ValueError("Wave membership timing is outside the exact slot")
        if decision > analysis_as_of_utc:
            raise ValueError("Wave membership is after analysis_as_of_utc")
        if (
            transition.get("stream_id") != stream_id
            or transition.get("movement_id") != movement_id
            or transition.get("trigger_anchor_id") != anchor_id
            or transition.get("trigger_eligible_at_utc") != eligible
            or transition.get("trigger_decision_time_utc") != decision
        ):
            raise ValueError("Wave membership/emitting transition mismatch")
        joined.append(
            {
                "membership_receipt_sha256": receipt,
                "transition_receipt_sha256": transition_receipt,
                "stream_id": stream_id,
                "movement_id": movement_id,
                "anchor_id": anchor_id,
                "eligible_at": eligible,
                "eligible_at_utc": _iso(eligible, field="Wave membership eligibility"),
                "decision": decision,
                "decision_time_utc": _iso(decision, field="Wave membership decision"),
                "namespace": transition.get("namespace"),
                "symbol": _symbol(
                    transition.get("symbol"), field="Wave transition symbol"
                ),
            }
        )
    return joined


def _coerce_observation(
    value: ExplorationObservation | Mapping[str, Any],
) -> ExplorationObservation:
    return ExplorationObservation.from_dict(
        value.to_dict() if isinstance(value, ExplorationObservation) else value
    )


def bind_wave_v5(
    observations: Sequence[ExplorationObservation | Mapping[str, Any]],
    memberships: Sequence[Mapping[str, Any]],
    transitions: Sequence[Mapping[str, Any]],
    *,
    analysis_as_of_utc: Any,
) -> tuple[ExplorationObservation, ...]:
    """Bind exact, already-known same-slot SYMBOL and BTC_PARENT identities."""

    as_of = _utc(analysis_as_of_utc, field="analysis_as_of_utc")
    joined = _validated_wave_rows(
        memberships, transitions, analysis_as_of_utc=as_of
    )
    output: list[ExplorationObservation] = []
    for original in observations:
        body = _coerce_observation(original).to_dict()
        body.pop("observation_id")
        decision = _utc(
            body.get("projection_decision_time_utc"), field="projection decision"
        )
        if decision > as_of:
            raise ValueError("projection decision is after analysis_as_of_utc")
        cycle = _utc(body.get("archive_cycle_time_utc"), field="archive cycle")
        expected_slot = cycle + timedelta(minutes=2)
        symbol = _symbol(body.get("symbol"))
        local = [
            item
            for item in joined
            if item["namespace"] == "SYMBOL"
            and item["symbol"] == symbol
            and item["eligible_at"] == expected_slot
            and item["decision"] <= decision
        ]
        parent = [
            item
            for item in joined
            if item["namespace"] == "BTC_PARENT"
            and item["symbol"] == "BTC"
            and item["eligible_at"] == expected_slot
            and item["decision"] <= decision
        ]
        if len(local) != 1:
            reason = (
                "SYMBOL_WAVE_MEMBERSHIP_MISSING"
                if not local
                else "SYMBOL_WAVE_MEMBERSHIP_AMBIGUOUS"
            )
            binding = {
                "status": "UNAVAILABLE",
                "reason": reason,
                "policy_version": WAVE_BINDING_POLICY_VERSION,
                "expected_eligible_at_utc": _iso(
                    expected_slot, field="expected Wave slot"
                ),
                "role": "INDEPENDENCE_ONLY_NOT_FORMULA_PREDICATE",
            }
        elif len(parent) != 1:
            reason = (
                "BTC_PARENT_WAVE_MEMBERSHIP_MISSING"
                if not parent
                else "BTC_PARENT_WAVE_MEMBERSHIP_AMBIGUOUS"
            )
            binding = {
                "status": "UNAVAILABLE",
                "reason": reason,
                "policy_version": WAVE_BINDING_POLICY_VERSION,
                "expected_eligible_at_utc": _iso(
                    expected_slot, field="expected Wave slot"
                ),
                "role": "INDEPENDENCE_ONLY_NOT_FORMULA_PREDICATE",
            }
        else:
            binding = {
                "status": "BOUND",
                "reason": None,
                "policy_version": WAVE_BINDING_POLICY_VERSION,
                "expected_eligible_at_utc": _iso(
                    expected_slot, field="expected Wave slot"
                ),
                "symbol_membership_receipt_sha256": local[0][
                    "membership_receipt_sha256"
                ],
                "symbol_transition_receipt_sha256": local[0][
                    "transition_receipt_sha256"
                ],
                "symbol_stream_id": local[0]["stream_id"],
                "symbol_movement_id": local[0]["movement_id"],
                "btc_parent_membership_receipt_sha256": parent[0][
                    "membership_receipt_sha256"
                ],
                "btc_parent_transition_receipt_sha256": parent[0][
                    "transition_receipt_sha256"
                ],
                "btc_parent_stream_id": parent[0]["stream_id"],
                "btc_parent_movement_id": parent[0]["movement_id"],
                "role": "INDEPENDENCE_ONLY_NOT_FORMULA_PREDICATE",
            }
        body["wave_binding"] = binding
        output.append(ExplorationObservation._from_payload(body))
    return tuple(
        sorted(
            output,
            key=lambda item: (
                item.to_dict()["projection_event_id"],
                item.to_dict()["snapshot_key"],
                item.to_dict()["symbol"],
                item.to_dict()["direction"],
            ),
        )
    )


def _numbers_close(actual: float, expected: float) -> bool:
    return math.isclose(actual, expected, rel_tol=1e-9, abs_tol=1e-9)


def _closed_path_lattice(
    event_time: datetime, horizon_minutes: int
) -> tuple[int, datetime]:
    """Mirror Stage 5's count and exact final closed-candle timestamp."""

    interval_ms = 60_000
    start_ms = int(event_time.timestamp() * 1000)
    end_ms = int(
        (event_time + timedelta(minutes=horizon_minutes)).timestamp() * 1000
    )
    first_open = ((start_ms + interval_ms - 1) // interval_ms) * interval_ms
    last_open = ((end_ms - (interval_ms - 1)) // interval_ms) * interval_ms
    if last_open < first_open:
        return 0, event_time
    expected = int((last_open - first_open) // interval_ms) + 1
    close_ms = last_open + interval_ms - 1
    close_time = datetime.fromtimestamp(close_ms // 1000, tz=_UTC) + timedelta(
        milliseconds=close_ms % 1000
    )
    return expected, close_time


def _expected_path_samples(event_time: datetime, horizon_minutes: int) -> int:
    """Expose the Stage 5 closed-candle count for deterministic verification."""

    return _closed_path_lattice(event_time, horizon_minutes)[0]


def _price_source_fields(value: str) -> Dict[str, str]:
    fields: Dict[str, str] = {}
    for token in value.split("|"):
        key, separator, item = token.partition("=")
        if not separator or not key or key in fields:
            raise ValueError("outcome price_source provenance is malformed")
        fields[key] = item
    return fields


def _validate_price_source(
    value: str,
    *,
    event_time: datetime,
    symbol: str,
    snapshot_set_id: int,
    snapshot_key: str,
    reference_policy_version: str = STAGE4_OUTCOME_REFERENCE_POLICY_VERSION,
    admission_policy_version: str = STAGE4_OUTCOME_ADMISSION_POLICY_VERSION,
) -> None:
    fields = _price_source_fields(value)
    required = {
        "reference",
        "admission_policy",
        "semantics",
        "source",
        "exchange",
        "market",
        "pair",
        "instrument",
        "observed_at_utc",
        "fetched_at_utc",
        "observed_age_seconds",
        "fetched_age_seconds",
        "snapshot_set_id",
        "snapshot_key",
        "path",
        "provenance",
    }
    if set(fields) != required:
        raise ValueError("outcome price_source provenance shape mismatch")
    if (
        fields["reference"]
        != "reference_policy=" + reference_policy_version
        or fields["admission_policy"]
        != admission_policy_version
        or fields["semantics"] != STAGE4_OUTCOME_SEMANTICS
        or fields["snapshot_set_id"] != str(snapshot_set_id)
        or fields["snapshot_key"] != snapshot_key
    ):
        raise ValueError("outcome price_source provenance mismatch")
    observed = _utc(fields["observed_at_utc"], field="outcome observed_at_utc")
    fetched = _utc(fields["fetched_at_utc"], field="outcome fetched_at_utc")
    if observed > fetched or fetched > event_time:
        raise ValueError("outcome price_source is post-decision")
    observed_age = _finite(
        fields["observed_age_seconds"], field="outcome observed_age_seconds"
    )
    fetched_age = _finite(
        fields["fetched_age_seconds"], field="outcome fetched_age_seconds"
    )
    if (
        observed_age is None
        or fetched_age is None
        or observed_age < 0
        or fetched_age < 0
        or observed_age > 3600
        or fetched_age > 3600
        or not _numbers_close(observed_age, (event_time - observed).total_seconds())
        or not _numbers_close(fetched_age, (event_time - fetched).total_seconds())
    ):
        raise ValueError("outcome price_source age provenance mismatch")
    if symbol == "HYPE":
        route_ok = (
            fields["source"].lower() == "hyperliquid"
            and fields["exchange"].lower() == "hyperliquid"
            and fields["market"].lower() == "spot"
            and fields["pair"].upper() == "HYPE/USDT"
            and fields["instrument"] == "@107"
            and fields["path"].lower() == "hyperliquid_spot:hype/usdt:1m"
        )
    else:
        reference_pair = f"{symbol}USDT"
        path_pair = _BINANCE_PAIR_ALIASES.get(symbol, reference_pair)
        route_ok = (
            fields["source"].lower() == "binance_spot"
            and fields["exchange"].lower() == "binance"
            and fields["market"].lower() == "spot"
            and fields["pair"].upper() == reference_pair
            and fields["path"].lower()
            == f"binance_spot:{path_pair.lower()}:1m"
        )
    if (
        not route_ok
        or fields["provenance"]
        != "EXCHANGE_API_HISTORICAL_CANDLES_IMPORTED"
    ):
        raise ValueError("outcome price_source route mismatch")


def _normalized_outcome(
    row: Mapping[str, Any],
    *,
    horizon_minutes: int,
    event_time: datetime,
    analysis_as_of_utc: datetime,
    direction: str,
    symbol: str,
    snapshot_set_id: int,
    snapshot_key: str,
    outcome_method_version: str = STAGE4_OUTCOME_METHOD_VERSION,
    reference_policy_version: str = STAGE4_OUTCOME_REFERENCE_POLICY_VERSION,
    admission_policy_version: str = STAGE4_OUTCOME_ADMISSION_POLICY_VERSION,
) -> Dict[str, Any]:
    if type(horizon_minutes) is not int or type(row.get("horizon_minutes")) is not int:
        raise ValueError("outcome horizon mismatch")
    if row.get("horizon_minutes") != horizon_minutes:
        raise ValueError("outcome horizon mismatch")
    if direction not in {"LONG", "SHORT"}:
        raise ValueError("outcome direction is invalid")
    if row.get("outcome_method_version") != outcome_method_version:
        raise ValueError("outcome method is not the Stage-4 closed-path contract")
    if row.get("data_quality_status") not in _COMPLETE_QUALITIES:
        raise ValueError("Stage-4 outcome path is not complete")
    expected_quality = (
        "VERIFIED_HYPERLIQUID_SPOT_1M_CLOSED_CANDLES"
        if symbol == "HYPE"
        else "VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES"
    )
    if row.get("data_quality_status") != expected_quality:
        raise ValueError("Stage-4 outcome quality/source route mismatch")
    measured = _utc(row.get("measured_at_utc"), field="outcome measured_at_utc")
    horizon_end = event_time + timedelta(minutes=horizon_minutes)
    if analysis_as_of_utc < horizon_end:
        raise ValueError("outcome horizon is not mature at analysis_as_of_utc")
    if measured > analysis_as_of_utc:
        raise ValueError("outcome is after analysis_as_of_utc")
    closing_gap = horizon_end - measured
    if closing_gap < timedelta(0) or closing_gap >= timedelta(seconds=60):
        raise ValueError("outcome measured time is outside the closed 1m lattice")

    normalized: Dict[str, Any] = {}
    for field in _PATH_LABEL_FIELDS:
        value = row.get(field)
        if field in {
            "reference_price",
            "price_at_horizon",
            "raw_return_pct",
            "directional_return_pct",
            "max_favorable_price",
            "max_adverse_price",
            "mfe_pct",
            "mae_pct",
        }:
            value = _finite(value, field=f"outcome {field}")
        elif field == "time_to_first_progress_seconds":
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError(
                    f"outcome {field} must be null or a non-negative integer"
                )
        elif field == "time_to_mfe_seconds":
            if type(value) is not int or value < 0:
                raise ValueError(
                    "outcome time_to_mfe_seconds must be a non-negative integer"
                )
        elif field in {"path_resolution_seconds", "path_samples"}:
            if type(value) is not int or value < 0:
                raise ValueError(f"outcome {field} must be a non-negative integer")
        elif field == "price_source":
            if not isinstance(value, str) or not value.strip():
                raise ValueError("outcome price_source provenance is required")
            value = value.strip()
        normalized[field] = value

    if (
        normalized["reference_price"] <= 0
        or normalized["price_at_horizon"] <= 0
        or normalized["max_favorable_price"] <= 0
        or normalized["max_adverse_price"] <= 0
        or normalized["path_resolution_seconds"] != 60
    ):
        raise ValueError("Stage-4 outcome reference/resolution is invalid")
    expected_samples, expected_measured = _closed_path_lattice(
        event_time, horizon_minutes
    )
    if normalized["path_samples"] != expected_samples or expected_samples <= 0:
        raise ValueError("Stage-4 outcome path sample count is invalid")
    if measured != expected_measured:
        raise ValueError("outcome measured time is not the final closed 1m candle")

    reference = normalized["reference_price"]
    raw_return = (normalized["price_at_horizon"] - reference) / reference * 100.0
    expected_directional = raw_return if direction == "LONG" else -raw_return
    if direction == "LONG":
        extrema_ok = (
            normalized["max_favorable_price"]
            >= max(reference, normalized["price_at_horizon"])
            and normalized["max_adverse_price"]
            <= min(reference, normalized["price_at_horizon"])
        )
        expected_mfe = max(
            0.0,
            (normalized["max_favorable_price"] - reference) / reference * 100.0,
        )
        expected_mae = max(
            0.0,
            (reference - normalized["max_adverse_price"]) / reference * 100.0,
        )
    else:
        extrema_ok = (
            normalized["max_favorable_price"]
            <= min(reference, normalized["price_at_horizon"])
            and normalized["max_adverse_price"]
            >= max(reference, normalized["price_at_horizon"])
        )
        expected_mfe = max(
            0.0,
            (reference - normalized["max_favorable_price"]) / reference * 100.0,
        )
        expected_mae = max(
            0.0,
            (normalized["max_adverse_price"] - reference) / reference * 100.0,
        )
    if not extrema_ok or any(
        not _numbers_close(actual, expected)
        for actual, expected in (
            (normalized["raw_return_pct"], raw_return),
            (normalized["directional_return_pct"], expected_directional),
            (normalized["mfe_pct"], expected_mfe),
            (normalized["mae_pct"], expected_mae),
        )
    ):
        raise ValueError("Stage-4 outcome path metrics are inconsistent")
    observed_seconds = max(0, int((measured - event_time).total_seconds()))
    for field in ("time_to_first_progress_seconds", "time_to_mfe_seconds"):
        if normalized[field] is not None and normalized[field] > observed_seconds:
            raise ValueError(f"outcome {field} exceeds its observed path")
    if normalized["mfe_pct"] == 0:
        if (
            normalized["time_to_mfe_seconds"] != 0
            or normalized["time_to_first_progress_seconds"] is not None
        ):
            raise ValueError("zero-MFE timing is inconsistent")
    elif (
        normalized["time_to_first_progress_seconds"] is None
        or normalized["time_to_first_progress_seconds"]
        > normalized["time_to_mfe_seconds"]
    ):
        raise ValueError("positive-MFE timing is inconsistent")
    _validate_price_source(
        normalized["price_source"],
        event_time=event_time,
        symbol=symbol,
        snapshot_set_id=snapshot_set_id,
        snapshot_key=snapshot_key,
        reference_policy_version=reference_policy_version,
        admission_policy_version=admission_policy_version,
    )
    normalized["measured_at_utc"] = _iso(measured, field="outcome measured_at_utc")
    return normalized


def attach_closed_outcomes(
    observations: Sequence[ExplorationObservation | Mapping[str, Any]],
    outcomes: Sequence[Mapping[str, Any]],
    *,
    no_signal_outcomes: Sequence[Mapping[str, Any]] = (),
    horizon_minutes: int,
    analysis_as_of_utc: Any,
) -> tuple[ExplorationObservation, ...]:
    """Attach exact signal-event or explicit no-signal cell paths."""

    if type(horizon_minutes) is not int or horizon_minutes not in {60, 240, 720, 1440}:
        raise ValueError("horizon_minutes must be 60, 240, 720 or 1440")
    as_of = _utc(analysis_as_of_utc, field="analysis_as_of_utc")
    by_event: Dict[int, list[Mapping[str, Any]]] = {}
    for raw in outcomes:
        row = _mapping(raw, field="outcome")
        event_id = _positive_int(row.get("event_id"), field="outcome event_id")
        if row.get("horizon_minutes") == horizon_minutes:
            by_event.setdefault(event_id, []).append(row)
    observation_bodies = [
        _coerce_observation(item).to_dict() for item in observations
    ]
    observation_by_cell: Dict[tuple[int, str, str], Dict[str, Any]] = {}
    for body in observation_bodies:
        key = (
            int(body["projection_event_id"]),
            _symbol(body["symbol"]),
            str(body["direction"]),
        )
        if key in observation_by_cell:
            raise ValueError("duplicate Stage-4 projection cohort cell")
        observation_by_cell[key] = body
    by_no_signal_cell: Dict[
        tuple[int, str, str], list[Mapping[str, Any]]
    ] = {}
    for raw in no_signal_outcomes:
        row = _mapping(raw, field="no-signal outcome")
        if row.get("horizon_minutes") != horizon_minutes:
            continue
        key = (
            _positive_int(
                row.get("projection_event_id"),
                field="no-signal projection_event_id",
            ),
            _symbol(row.get("symbol"), field="no-signal symbol"),
            str(row.get("direction") or "").strip().upper(),
        )
        if key[2] not in {"LONG", "SHORT"}:
            raise ValueError("no-signal outcome direction is invalid")
        target = observation_by_cell.get(key)
        if target is None:
            raise ValueError("no-signal outcome is outside the supplied cohort")
        if target.get("explicit_no_signal") is not True:
            raise ValueError("no-signal outcome targets a signal-bearing cell")
        by_no_signal_cell.setdefault(key, []).append(row)
    output: list[ExplorationObservation] = []
    for original_body in observation_bodies:
        body = dict(original_body)
        body.pop("observation_id")
        source_ids = list(body.get("source_event_ids") or [])
        if not source_ids:
            cell_key = (
                int(body["projection_event_id"]),
                _symbol(body["symbol"]),
                str(body["direction"]),
            )
            carrier_rows = by_no_signal_cell.get(cell_key, [])
            if len(carrier_rows) > 1:
                outcome = {
                    "status": "OUTCOME_UNAVAILABLE",
                    "reason": "DUPLICATE_STAGE4_NO_SIGNAL_OUTCOME",
                    "policy_version": OUTCOME_BINDING_POLICY_VERSION,
                    "horizon_minutes": horizon_minutes,
                    "label_fields_exposed_as_features": False,
                }
            elif not carrier_rows:
                outcome = {
                    "status": "OUTCOME_UNAVAILABLE",
                    "reason": "CANONICAL_NO_SIGNAL_OUTCOME_NOT_MATERIALIZED",
                    "policy_version": OUTCOME_BINDING_POLICY_VERSION,
                    "horizon_minutes": horizon_minutes,
                    "label_fields_exposed_as_features": False,
                }
            else:
                carrier = carrier_rows[0]
                try:
                    if (
                        _hash(
                            carrier.get("projection_event_fingerprint"),
                            field="carrier projection_event_fingerprint",
                        )
                        != body["projection_event_fingerprint"]
                        or _positive_int(
                            carrier.get("snapshot_set_id"),
                            field="carrier snapshot_set_id",
                        )
                        != body["snapshot_set_id"]
                        or _hash(
                            carrier.get("snapshot_key"),
                            field="carrier snapshot_key",
                        )
                        != body["snapshot_key"]
                        or _utc(
                            carrier.get("decision_time_utc"),
                            field="carrier decision_time_utc",
                        )
                        != _utc(
                            body["projection_decision_time_utc"],
                            field="projection_decision_time_utc",
                        )
                        or carrier.get("absence_basis")
                        != NO_SIGNAL_ABSENCE_BASIS
                    ):
                        raise ValueError("no-signal carrier identity mismatch")
                    normalized = _normalized_outcome(
                        carrier,
                        horizon_minutes=horizon_minutes,
                        event_time=_utc(
                            body["projection_decision_time_utc"],
                            field="projection decision",
                        ),
                        analysis_as_of_utc=as_of,
                        direction=str(body["direction"]),
                        symbol=_symbol(body["symbol"]),
                        snapshot_set_id=_positive_int(
                            body["snapshot_set_id"], field="snapshot_set_id"
                        ),
                        snapshot_key=_hash(
                            body["snapshot_key"], field="snapshot_key"
                        ),
                        outcome_method_version=(
                            STAGE4_NO_SIGNAL_OUTCOME_METHOD_VERSION
                        ),
                        reference_policy_version=(
                            STAGE4_NO_SIGNAL_OUTCOME_REFERENCE_POLICY_VERSION
                        ),
                        admission_policy_version=(
                            STAGE4_NO_SIGNAL_OUTCOME_ADMISSION_POLICY_VERSION
                        ),
                    )
                    path_identity = {
                        key: normalized[key] for key in _PATH_LABEL_FIELDS
                    }
                    outcome = {
                        "status": "AVAILABLE",
                        "reason": None,
                        "policy_version": OUTCOME_BINDING_POLICY_VERSION,
                        "horizon_minutes": horizon_minutes,
                        "carrier_type": "STAGE4_NO_SIGNAL_CELL",
                        "carrier_payload_sha256": _hash(
                            carrier.get("outcome_payload_sha256"),
                            field="outcome_payload_sha256",
                        ),
                        "source_event_ids": [],
                        "path": path_identity,
                        "measured_at_utc": normalized["measured_at_utc"],
                        "label_fields_exposed_as_features": False,
                    }
                except ValueError as exc:
                    outcome = {
                        "status": "OUTCOME_UNAVAILABLE",
                        "reason": f"INVALID_STAGE4_NO_SIGNAL_OUTCOME:{exc}",
                        "policy_version": OUTCOME_BINDING_POLICY_VERSION,
                        "horizon_minutes": horizon_minutes,
                        "label_fields_exposed_as_features": False,
                    }
        else:
            rows: list[Mapping[str, Any]] = []
            missing = False
            duplicate = False
            for event_id in source_ids:
                candidates = by_event.get(int(event_id), [])
                if not candidates:
                    missing = True
                elif len(candidates) != 1:
                    duplicate = True
                else:
                    rows.append(candidates[0])
            if duplicate:
                outcome = {
                    "status": "OUTCOME_UNAVAILABLE",
                    "reason": "DUPLICATE_STAGE4_SIGNAL_OUTCOME",
                    "policy_version": OUTCOME_BINDING_POLICY_VERSION,
                    "horizon_minutes": horizon_minutes,
                    "label_fields_exposed_as_features": False,
                }
            elif missing:
                outcome = {
                    "status": "OUTCOME_UNAVAILABLE",
                    "reason": "STAGE4_SIGNAL_OUTCOME_MISSING",
                    "policy_version": OUTCOME_BINDING_POLICY_VERSION,
                    "horizon_minutes": horizon_minutes,
                    "label_fields_exposed_as_features": False,
                }
            else:
                try:
                    normalized = [
                        _normalized_outcome(
                            row,
                            horizon_minutes=horizon_minutes,
                            event_time=_utc(
                                body["projection_decision_time_utc"],
                                field="projection decision",
                            ),
                            analysis_as_of_utc=as_of,
                            direction=str(body["direction"]),
                            symbol=_symbol(body["symbol"]),
                            snapshot_set_id=_positive_int(
                                body["snapshot_set_id"], field="snapshot_set_id"
                            ),
                            snapshot_key=_hash(
                                body["snapshot_key"], field="snapshot_key"
                            ),
                        )
                        for row in rows
                    ]
                    path_identity = {
                        key: normalized[0][key]
                        for key in _PATH_LABEL_FIELDS
                    }
                    if any(
                        {key: item[key] for key in _PATH_LABEL_FIELDS}
                        != path_identity
                        for item in normalized[1:]
                    ) or any(
                        item["measured_at_utc"]
                        != normalized[0]["measured_at_utc"]
                        for item in normalized[1:]
                    ):
                        raise ValueError("sibling Stage-4 outcomes disagree")
                    outcome = {
                        "status": "AVAILABLE",
                        "reason": None,
                        "policy_version": OUTCOME_BINDING_POLICY_VERSION,
                        "horizon_minutes": horizon_minutes,
                        "carrier_type": "STAGE4_SIGNAL_EVENTS",
                        "carrier_payload_sha256": None,
                        "source_event_ids": sorted(int(value) for value in source_ids),
                        "path": path_identity,
                        "measured_at_utc": normalized[0]["measured_at_utc"],
                        "label_fields_exposed_as_features": False,
                    }
                except ValueError as exc:
                    outcome = {
                        "status": "OUTCOME_UNAVAILABLE",
                        "reason": f"INVALID_STAGE4_SIGNAL_OUTCOME:{exc}",
                        "policy_version": OUTCOME_BINDING_POLICY_VERSION,
                        "horizon_minutes": horizon_minutes,
                        "label_fields_exposed_as_features": False,
                    }
        body["outcome"] = outcome
        output.append(ExplorationObservation._from_payload(body))
    return tuple(
        sorted(
            output,
            key=lambda item: (
                item.to_dict()["projection_event_id"],
                item.to_dict()["snapshot_key"],
                item.to_dict()["symbol"],
                item.to_dict()["direction"],
            ),
        )
    )


def validate_candidate_feature_set(features: Iterable[Any]) -> Dict[str, Any]:
    """Validate a future predicate set before it may consume any budget."""

    supplied = [str(value or "").strip() for value in features]
    if not supplied or any(name not in ALLOWED_FEATURES for name in supplied):
        unknown = sorted({name for name in supplied if name not in ALLOWED_FEATURES})
        raise ValueError(f"candidate contains forbidden Stage-4 features: {unknown}")
    if len(supplied) != len(set(supplied)):
        raise ValueError("candidate contains duplicate Stage-4 features")
    selected = set(supplied)
    names = [name for name in ALLOWED_FEATURES if name in selected]
    used: set[str] = set()
    closure: Dict[str, list[str]] = {}
    for name in names:
        sources = set(_FEATURE_SOURCE_CLOSURE[name])
        overlap = used & sources
        if overlap:
            raise ValueError(
                "candidate double-counts dependent Stage-4 sources: "
                + ", ".join(sorted(overlap))
            )
        used.update(sources)
        closure[name] = sorted(sources)
    return {
        "valid": True,
        "features": names,
        "source_closure": closure,
        "deduplicated_sources": sorted(used),
        "spot_cvd_role": "CONTEXT_ONLY_NOT_A_VOTE_OR_ALLOWED_FEATURE",
        "budget_effect": "NONE; validation only",
        "authority_effect": AUTHORITY_EFFECT,
    }


def dataset_readiness(
    observations: Sequence[ExplorationObservation | Mapping[str, Any]],
    *,
    source_authority_attested: bool = False,
    statistical_label_contract_implemented: bool = False,
    wave_identity_candidate_search_implemented: bool = False,
) -> Dict[str, Any]:
    """Report coverage and parent floors without claiming an edge or maturity."""

    for name, value in (
        ("source_authority_attested", source_authority_attested),
        (
            "statistical_label_contract_implemented",
            statistical_label_contract_implemented,
        ),
        (
            "wave_identity_candidate_search_implemented",
            wave_identity_candidate_search_implemented,
        ),
    ):
        if type(value) is not bool:
            raise ValueError(f"{name} must be boolean")
    rows = [_coerce_observation(item).to_dict() for item in observations]
    blockers: set[str] = set()
    if not source_authority_attested:
        blockers.add("AUTHORITATIVE_SOURCE_ATTESTATION_NOT_IMPLEMENTED")
    if not statistical_label_contract_implemented:
        blockers.add("VERSIONED_STATISTICAL_LABEL_CONTRACT_NOT_IMPLEMENTED")
    if not wave_identity_candidate_search_implemented:
        blockers.add("WAVE_IDENTITY_CANDIDATE_SEARCH_NOT_IMPLEMENTED")
    cohort_blockers: set[str] = set()
    if not rows:
        cohort_blockers.add("EMPTY_COHORT")
    groups: Dict[tuple[int, str], list[Dict[str, Any]]] = {}
    projection_keys: Dict[int, str] = {}
    for row in rows:
        projection_id = int(row["projection_event_id"])
        snapshot_key = str(row["snapshot_key"])
        if projection_id in projection_keys and projection_keys[projection_id] != snapshot_key:
            cohort_blockers.add("PROJECTION_ID_SNAPSHOT_KEY_FORK")
        projection_keys[projection_id] = snapshot_key
        groups.setdefault((projection_id, snapshot_key), []).append(row)
    for group in groups.values():
        first = group[0]
        manifest = tuple(first["cohort_evaluable_symbols"])
        expected_cells = {
            (symbol, direction)
            for symbol in manifest
            for direction in ("LONG", "SHORT")
        }
        actual_cells = [(row["symbol"], row["direction"]) for row in group]
        if len(actual_cells) != len(set(actual_cells)):
            cohort_blockers.add("DUPLICATE_PROJECTION_COHORT_CELL")
        if set(actual_cells) != expected_cells or len(group) != first[
            "cohort_expected_observation_count"
        ]:
            cohort_blockers.add("INCOMPLETE_PROJECTION_COHORT")
        manifest_fields = (
            "projection_event_fingerprint",
            "snapshot_set_id",
            "projection_decision_time_utc",
            "archive_cycle_time_utc",
            "cohort_expected_observation_count",
            "projection_signal_event_count",
            "projection_signal_events_payload_sha256",
        )
        if any(
            tuple(row[field] for field in manifest_fields)
            != tuple(first[field] for field in manifest_fields)
            or tuple(row["cohort_evaluable_symbols"]) != manifest
            for row in group[1:]
        ):
            cohort_blockers.add("INCONSISTENT_PROJECTION_COHORT_MANIFEST")
        source_ids = [
            int(event_id)
            for row in group
            for event_id in row["source_event_ids"]
        ]
        source_fingerprints = [
            str(fingerprint)
            for row in group
            for fingerprint in row["source_event_fingerprints"]
        ]
        expected_signal_count = first["projection_signal_event_count"]
        signal_partition_invalid = (
            len(source_ids) != len(set(source_ids))
            or len(source_ids) != expected_signal_count
            or len(source_fingerprints) != len(set(source_fingerprints))
            or len(source_fingerprints) != expected_signal_count
        )
        if signal_partition_invalid:
            cohort_blockers.add("PROJECTION_SIGNAL_PARTITION_INCOMPLETE")
    blockers.update(cohort_blockers)
    label_horizons = sorted(
        {
            int(row["outcome"]["horizon_minutes"])
            for row in rows
            if (row.get("outcome") or {}).get("status") != "UNBOUND"
            and type((row.get("outcome") or {}).get("horizon_minutes")) is int
        }
    )
    label_contract_blockers: set[str] = set()
    if len(label_horizons) > 1:
        label_contract_blockers.add("MIXED_OUTCOME_HORIZONS")
    blockers.update(label_contract_blockers)
    parent_ids: set[str] = set()
    effect_parent_ids: set[str] = set()
    for row in rows:
        binding = _mapping(row.get("wave_binding"), field="wave_binding")
        outcome = _mapping(row.get("outcome"), field="outcome")
        if binding.get("status") != "BOUND":
            blockers.add(str(binding.get("reason") or "WAVE_BINDING_UNAVAILABLE"))
            continue
        parent_id = _hash(
            binding.get("btc_parent_movement_id"), field="btc_parent_movement_id"
        )
        parent_ids.add(parent_id)
        if outcome.get("status") != "AVAILABLE":
            blockers.add(str(outcome.get("reason") or "OUTCOME_UNAVAILABLE"))
        else:
            effect_parent_ids.add(parent_id)
    structurally_complete = bool(rows) and not cohort_blockers
    wave_complete = structurally_complete and all(
        (row.get("wave_binding") or {}).get("status") == "BOUND" for row in rows
    )
    label_complete = (
        structurally_complete
        and not label_contract_blockers
        and all(
            (row.get("outcome") or {}).get("status") == "AVAILABLE" for row in rows
        )
    )
    descriptive_effect_count = len(effect_parent_ids)
    technically_ready = bool(
        structurally_complete
        and wave_complete
        and label_complete
        and source_authority_attested
        and statistical_label_contract_implemented
        and wave_identity_candidate_search_implemented
    )
    return {
        "policy_version": POLICY_VERSION,
        "observation_count": len(rows),
        "explicit_no_signal_count": sum(
            int(row.get("explicit_no_signal") is True) for row in rows
        ),
        "wave_bound_count": sum(
            int((row.get("wave_binding") or {}).get("status") == "BOUND")
            for row in rows
        ),
        "outcome_available_count": sum(
            int((row.get("outcome") or {}).get("status") == "AVAILABLE")
            for row in rows
        ),
        "outcome_horizons_minutes": label_horizons,
        "distinct_bound_btc_parent_movements": len(parent_ids),
        "descriptive_distinct_labeled_btc_parent_movements": (
            descriptive_effect_count
        ),
        "distinct_effect_btc_parent_movements": (
            descriptive_effect_count if technically_ready else 0
        ),
        "exploration_parent_floor": EXPLORATION_MIN_BTC_PARENT_MOVEMENTS,
        "maturity_parent_floor": MATURITY_MIN_BTC_PARENT_MOVEMENTS,
        "descriptive_reaches_exploration_parent_floor": bool(
            structurally_complete
            and wave_complete
            and label_complete
            and descriptive_effect_count >= EXPLORATION_MIN_BTC_PARENT_MOVEMENTS
        ),
        "descriptive_reaches_maturity_parent_floor": bool(
            structurally_complete
            and wave_complete
            and label_complete
            and descriptive_effect_count >= MATURITY_MIN_BTC_PARENT_MOVEMENTS
        ),
        "meets_exploration_parent_floor": bool(
            technically_ready
            and descriptive_effect_count >= EXPLORATION_MIN_BTC_PARENT_MOVEMENTS
        ),
        "meets_maturity_parent_floor": bool(
            technically_ready
            and descriptive_effect_count >= MATURITY_MIN_BTC_PARENT_MOVEMENTS
        ),
        "cohort_structurally_complete": structurally_complete,
        "wave_coverage_complete": wave_complete,
        "label_coverage_complete": label_complete,
        "source_authority_attested": source_authority_attested,
        "statistical_label_contract_implemented": (
            statistical_label_contract_implemented
        ),
        "wave_identity_candidate_search_implemented": (
            wave_identity_candidate_search_implemented
        ),
        "ready_for_formula_effect_research": technically_ready,
        "blockers": sorted(blockers),
        "edge_established": False,
        "maturity_established": False,
        "formula_registry_effect": "NONE",
        "authority_effect": AUTHORITY_EFFECT,
        "delivery_channel": DELIVERY_CHANNEL,
        "live_eligible": False,
        "telegram_delivery_allowed": False,
        "trade_execution_allowed": False,
    }


def descriptor() -> Dict[str, Any]:
    return {
        "policy_version": POLICY_VERSION,
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "wave_binding_policy_version": WAVE_BINDING_POLICY_VERSION,
        "outcome_binding_policy_version": OUTCOME_BINDING_POLICY_VERSION,
        "allowed_features": list(ALLOWED_FEATURES),
        "exploration_parent_floor": EXPLORATION_MIN_BTC_PARENT_MOVEMENTS,
        "maturity_parent_floor": MATURITY_MIN_BTC_PARENT_MOVEMENTS,
        "parent_floor_semantics": (
            "distinct BTC_PARENT movements are necessary coverage floors only; "
            "they do not establish edge, maturity, approval or delivery authority"
        ),
        "true_negative_semantics": (
            "signal absence is locally valid only under one COMPLETED projection "
            "with an EVALUABLE per-symbol receipt; source attestation remains "
            "external to this pure contract"
        ),
        "wave_slot_semantics": (
            "Stage-6 cohorts admit :00/:30 archive cycles and bind only the "
            "same cycle's +2 minute Wave slot"
        ),
        "wave_role": "INDEPENDENCE_ONLY_NOT_FORMULA_PREDICATE",
        "spot_cvd_role": "CONTEXT_ONLY_NOT_A_VOTE_OR_ALLOWED_FEATURE",
        "source_trust_boundary": (
            "UNATTESTED_PURE_INPUT; the authoritative reader verifies migrations "
            "022/023/024/025/026 and archived receipt identities before readiness"
        ),
        "formula_registry_effect": "NONE",
        "authority_effect": AUTHORITY_EFFECT,
        "delivery_channel": DELIVERY_CHANNEL,
        "live_eligible": False,
        "telegram_delivery_allowed": False,
        "trade_execution_allowed": False,
    }
