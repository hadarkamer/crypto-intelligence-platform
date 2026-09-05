"""Pure causal neutral-price Market Movement (Wave v5) contract.

This module deliberately has no database, network, runtime, Telegram, formula,
alert, or outcome dependencies.  It validates one exact closed Spot 1m price
anchor at each 30-minute eligibility point and advances an append-only wave
state machine using only those frozen prices.

The contract is intentionally distinct from ``research_market_episode`` v4.
Market movements are global price facts, not formula-local evidence windows:
they have no 24-hour boundary and never inspect a forecast or its result.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
from typing import Any, Mapping, Optional, Sequence


POLICY_VERSION = "market-movement-v5-causal-neutral-price-wave"
NEUTRAL_PRICE_SAMPLER_VERSION = "neutral-price-anchor-v5-exact-closed-1m"
LEGACY_V3_SAMPLER_VERSION = (
    "prospective-neutral-anchor-v3-max-pain-frozen"
)
LEGACY_V4_SAMPLER_VERSION = (
    "prospective-neutral-anchor-v4-decision-features-frozen"
)
AUTHORIZED_LEGACY_SAMPLERS: tuple[str, ...] = (
    LEGACY_V3_SAMPLER_VERSION,
    LEGACY_V4_SAMPLER_VERSION,
)
LEGACY_V4_FEATURE_BUNDLE_POLICY_VERSION = (
    "prospective-decision-feature-bundle-v1"
)
LEGACY_V4_FEATURE_BUNDLE_SCHEMA_VERSION = (
    "prospective-decision-feature-bundle-schema-v1"
)
HISTORICAL_IMPORT_NOT_BEFORE_UTC = datetime(
    2026, 8, 29, tzinfo=timezone.utc
)
HISTORICAL_PRICE_CANDLE_IDENTITY_BASIS = (
    "DERIVED_FROM_FROZEN_CLOSE_AND_AUTHORIZED_1M_SAMPLER_CONTRACT"
)
PROSPECTIVE_PRICE_CANDLE_IDENTITY_BASIS = (
    "FROZEN_EXACT_CLOSED_1M_CANDLE"
)

INTERVAL_MINUTES = 30
ELIGIBILITY_MINUTES: tuple[int, ...] = (2, 32)
CAPTURE_WINDOW_MINUTES = 30

EVALUABLE = "EVALUABLE"
UNEVALUABLE = "UNEVALUABLE"

SYMBOL_NAMESPACE = "SYMBOL"
BTC_PARENT_NAMESPACE = "BTC_PARENT"
PENDING_DIRECTION = "PENDING"
UP_DIRECTION = "UP"
DOWN_DIRECTION = "DOWN"
OPEN_STATUS = "OPEN"
CLOSED_STATUS = "CLOSED"

DATA_GAP_CENSORED = "DATA_GAP_CENSORED"
TWO_CONSECUTIVE_NON_EXTREMES = "TWO_CONSECUTIVE_NON_EXTREMES"

OPENED = "OPENED"
OPENED_AFTER_DATA_GAP = "OPENED_AFTER_DATA_GAP"
OPENED_AFTER_DIRECTION_END = "OPENED_AFTER_DIRECTION_END"
DIRECTION_ESTABLISHED = "DIRECTION_ESTABLISHED"
EXTREME_EXTENDED = "EXTREME_EXTENDED"
NON_EXTREME_OBSERVED = "NON_EXTREME_OBSERVED"
MOVEMENT_CLOSED = "MOVEMENT_CLOSED"

START_MEMBER = "START"
DIRECTIONAL_EXTREME_MEMBER = "DIRECTIONAL_EXTREME"
EXTREME_EXTENSION_MEMBER = "EXTREME_EXTENSION"
NON_EXTREME_MEMBER = "NON_EXTREME"

_UTC = timezone.utc
_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL = re.compile(r"^[A-Z0-9-]{1,20}$")


def _utc(value: Any, *, field: str) -> datetime:
    """Parse an explicitly timezone-aware timestamp and normalize it to UTC."""

    if isinstance(value, bool) or value in (None, ""):
        raise ValueError(f"{field} is required")
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError(f"{field} is required")
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
    else:
        raise ValueError(f"{field} must be a datetime or ISO-8601 string")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed.astimezone(_UTC)


def _iso(value: Any, *, field: str = "timestamp") -> str:
    return (
        _utc(value, field=field)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _optional_utc(value: Any, *, field: str) -> Optional[datetime]:
    return None if value is None else _utc(value, field=field)


def _optional_iso(value: Any, *, field: str) -> Optional[str]:
    return None if value is None else _iso(value, field=field)


def _symbol(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("symbol must be a string")
    trimmed = value.strip()
    if not trimmed.isascii():
        raise ValueError(f"invalid neutral-price symbol: {value!r}")
    normalized = trimmed.upper()
    if _SYMBOL.fullmatch(normalized) is None:
        raise ValueError(f"invalid neutral-price symbol: {value!r}")
    return normalized


def _strict_string(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


def _strict_int(value: Any, *, field: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"{field} must be an integer >= {minimum}")
    return value


def _decimal(value: Any, *, field: str = "price") -> Decimal:
    if isinstance(value, bool) or value in (None, ""):
        raise ValueError(f"{field} must be a finite positive number")
    if not isinstance(value, (int, float, str, Decimal)):
        raise ValueError(f"{field} must be a finite positive number")
    try:
        result = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a finite positive number") from exc
    if not result.is_finite() or result <= 0:
        raise ValueError(f"{field} must be a finite positive number")
    return result


def _decimal_text(value: Any, *, field: str = "price") -> str:
    number = _decimal(value, field=field)
    text = format(number, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("value is not canonical JSON") from exc


def _sha256(kind: str, payload: Mapping[str, Any]) -> str:
    envelope = {
        "contract_version": POLICY_VERSION,
        "kind": kind,
        "payload": dict(payload),
    }
    return hashlib.sha256(_canonical_json(envelope).encode("utf-8")).hexdigest()


def _require_hash(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or _HEX_64.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 hex digest")
    return value


def _only_fields(value: Mapping[str, Any], fields: set[str], *, kind: str) -> None:
    unknown = set(value) - fields
    missing = fields - set(value)
    if unknown:
        raise ValueError(f"unknown {kind} fields: {sorted(unknown)!r}")
    if missing:
        raise ValueError(f"missing {kind} fields: {sorted(missing)!r}")


def _normalized_pair(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return "".join(character for character in value.upper() if character.isalnum())


def _eligibility(value: Any) -> datetime:
    moment = _utc(value, field="eligible_at_utc")
    if (
        moment.minute not in ELIGIBILITY_MINUTES
        or moment.second != 0
        or moment.microsecond != 0
    ):
        raise ValueError("eligible_at_utc must be exactly on the :02/:32 lattice")
    return moment


def _route(
    symbol: str, source_provenance: Any
) -> tuple[str, str, str, Optional[str], str, str, bool, str, str, str]:
    if not isinstance(source_provenance, Mapping):
        raise ValueError("official price provenance is required")
    source = _strict_string(source_provenance.get("source"), field="source")
    quality = _strict_string(
        source_provenance.get("quality_status"), field="quality_status"
    ).upper()
    exchange = _strict_string(
        source_provenance.get("price_exchange"), field="price_exchange"
    )
    market = _strict_string(
        source_provenance.get("price_market"), field="price_market"
    )
    pair = _strict_string(source_provenance.get("price_pair"), field="price_pair")
    timeframe = _strict_string(
        source_provenance.get("price_timeframe"), field="price_timeframe"
    ).lower()
    fallback = source_provenance.get("fallback_used")
    fallback_policy = _strict_string(
        source_provenance.get("fallback_policy"), field="fallback_policy"
    )
    upstream_source = _strict_string(
        source_provenance.get("upstream_source"), field="upstream_source"
    )
    instrument_value = source_provenance.get("price_instrument_id")
    instrument = (
        None
        if instrument_value in (None, "")
        else _strict_string(instrument_value, field="price_instrument_id")
    )
    if quality != "PASS":
        raise ValueError("official price quality_status must be PASS")
    if timeframe != "1m":
        raise ValueError("official price timeframe must be 1m")
    if fallback is not False:
        raise ValueError("official price fallback_used must be exactly false")
    if fallback_policy.upper() != "PROVIDER_ATTESTED_NO_FALLBACK":
        raise ValueError("official price fallback policy is not authoritative")
    normalized_pair = _normalized_pair(pair)
    if symbol == "HYPE":
        if (
            source.lower() != "hyperliquid_spot_@107"
            or exchange.upper() != "HYPERLIQUID"
            or market.upper() != "SPOT"
            or normalized_pair != "HYPEUSDT"
            or (instrument or "").upper() != "@107"
            or upstream_source.lower() != "hyperliquid"
        ):
            raise ValueError("HYPE requires Hyperliquid Spot @107 provenance")
    elif (
        source.lower() != "binance_spot"
        or exchange.upper() != "BINANCE"
        or market.upper() != "SPOT"
        or normalized_pair != f"{symbol}USDT"
        or (instrument or "").upper() != f"{symbol}USDT"
        or upstream_source.lower() != "binance_spot"
    ):
        raise ValueError(f"{symbol} requires Binance Spot {symbol}USDT provenance")
    return (
        source,
        exchange,
        market,
        instrument,
        timeframe,
        quality,
        False,
        fallback_policy,
        pair,
        upstream_source,
    )


def _validate_causal_times(
    *,
    eligible_at_utc: Any,
    decision_time_utc: Any,
    source_price_candle_open_utc: Any,
    source_price_candle_close_utc: Any,
    observed_at_utc: Any,
    refresh_completed_at_utc: Any,
) -> tuple[datetime, datetime, datetime, datetime, datetime, datetime]:
    eligible = _eligibility(eligible_at_utc)
    decision = _utc(decision_time_utc, field="decision_time_utc")
    opened = _utc(
        source_price_candle_open_utc,
        field="source_price_candle_open_utc",
    )
    closed = _utc(
        source_price_candle_close_utc,
        field="source_price_candle_close_utc",
    )
    observed = _utc(observed_at_utc, field="observed_at_utc")
    refreshed = _utc(
        refresh_completed_at_utc, field="refresh_completed_at_utc"
    )
    if opened != eligible - timedelta(minutes=1):
        raise ValueError("price candle open must equal eligibility minus one minute")
    if not eligible - timedelta(seconds=1) <= closed < eligible:
        raise ValueError("price candle close must be in the final second before eligibility")
    if observed != closed:
        raise ValueError("observed_at_utc must equal the frozen candle close")
    if not eligible <= refreshed <= decision:
        raise ValueError("refresh must be causal and between eligibility and decision")
    if not eligible <= decision < eligible + timedelta(minutes=CAPTURE_WINDOW_MINUTES):
        raise ValueError("decision time is outside the 30-minute capture window")
    return eligible, decision, opened, closed, observed, refreshed


def compute_authorized_legacy_input_fingerprint(
    record: Mapping[str, Any]
) -> str:
    """Recompute the exact v3/v4 frozen-slot input fingerprint locally.

    This is a compatibility verifier, not an import from the v3/v4 runtime.
    It mirrors that sampler's canonical payload and binds every frozen family,
    while Wave v5 itself consumes only the neutral official price.
    """

    if not isinstance(record, Mapping):
        raise ValueError("legacy slot record must be a mapping")
    sampler = _strict_string(record.get("sampler_version"), field="sampler_version")
    coverage_policy = _strict_string(
        record.get("coverage_policy_version"), field="coverage_policy_version"
    )
    coverage = record.get("coverage_snapshot")
    timestamps = record.get("source_timestamps")
    provenance = record.get("source_provenance")
    frozen = record.get("frozen_inputs")
    for field, value in (
        ("coverage_snapshot", coverage),
        ("source_timestamps", timestamps),
        ("source_provenance", provenance),
        ("frozen_inputs", frozen),
    ):
        if not isinstance(value, Mapping):
            raise ValueError(f"legacy {field} must be an object")
    payload: dict[str, Any] = {
        "sampler_version": sampler,
        "coverage_policy_version": coverage_policy,
        "coverage_snapshot": dict(coverage),
        "symbol": _symbol(record.get("symbol")),
        "source_candle_open_utc": _iso(
            record.get("source_candle_open_utc"),
            field="source_candle_open_utc",
        ),
        "source_candle_close_utc": _iso(
            record.get("source_candle_close_utc"),
            field="source_candle_close_utc",
        ),
        "base_eligible_at_utc": _iso(
            record.get("base_eligible_at_utc"),
            field="base_eligible_at_utc",
        ),
        "expires_at_utc": _iso(
            record.get("expires_at_utc"), field="expires_at_utc"
        ),
        "evaluation_status": "EVALUABLE",
        "decision_time_utc": _iso(
            record.get("decision_time_utc"), field="decision_time_utc"
        ),
        "source_timestamps": dict(timestamps),
        "source_provenance": dict(provenance),
        "frozen_formula_visible_inputs": dict(frozen),
    }
    feature_policy = record.get("feature_bundle_policy_version")
    feature_sha = record.get("feature_bundle_sha256")
    if feature_policy not in (None, ""):
        payload["feature_bundle_policy_version"] = str(feature_policy)
    if feature_sha not in (None, ""):
        payload["feature_bundle_sha256"] = str(feature_sha)
    # v3/v4 used this exact JSON contract. Values inside persisted JSONB are
    # already JSON scalars; allow_nan=False adds a fail-closed guard without
    # changing valid bytes.
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _validate_authorized_legacy_version_shape(
    record: Mapping[str, Any],
    *,
    sampler: str,
    symbol: str,
    decision_time_utc: datetime,
) -> None:
    """Validate the frozen v3/v4 envelope boundary needed for import.

    The full decision-feature semantics stay owned by the prospective sampler.
    Wave v5 verifies the version-specific references, archive shape, and
    content-hash consistency of a row supplied by the trusted historical
    reader.  These digests are integrity checks, not origin signatures;
    authorization remains outside this pure price contract.
    """

    feature_policy = record.get("feature_bundle_policy_version")
    feature_sha = record.get("feature_bundle_sha256")
    decision_bundle = record.get("decision_feature_bundle")
    frozen_inputs = record.get("frozen_inputs")
    if not isinstance(frozen_inputs, Mapping):
        raise ValueError("legacy frozen_inputs must be an object")
    max_pain = frozen_inputs.get("max_pain")
    if (
        not isinstance(max_pain, Mapping)
        or not isinstance(max_pain.get("features"), Mapping)
        or max_pain.get("evaluation_status") not in {EVALUABLE, UNEVALUABLE}
    ):
        raise ValueError("legacy frozen max_pain wrapper is incompatible")
    if sampler == LEGACY_V3_SAMPLER_VERSION:
        if (
            feature_policy is not None
            or feature_sha is not None
            or decision_bundle is not None
        ):
            raise ValueError(
                "legacy v3 must not carry v4 decision-feature references"
            )
        return

    if sampler != LEGACY_V4_SAMPLER_VERSION:
        raise ValueError("legacy sampler is not authorized")
    if "decision_feature_bundle" in frozen_inputs:
        raise ValueError(
            "legacy v4 decision feature bundle must not be duplicated in frozen_inputs"
        )
    if feature_policy != LEGACY_V4_FEATURE_BUNDLE_POLICY_VERSION:
        raise ValueError("legacy v4 feature bundle policy is incompatible")
    normalized_sha = _require_hash(
        feature_sha, field="feature_bundle_sha256"
    )
    if not isinstance(decision_bundle, Mapping):
        raise ValueError("legacy v4 decision feature bundle is required")

    required_fields = {
        "bundle_schema_version",
        "feature_policy_version",
        "feature_schema_version",
        "decision_time_utc",
        "symbol",
        "source_series_manifest",
        "features_by_direction",
        "horizon_context",
        "model_score_status",
    }
    model_status = decision_bundle.get("model_score_status")
    allowed_fields = set(required_fields)
    if model_status == "FROZEN":
        allowed_fields.add("frozen_model_wrapper")
    if set(decision_bundle) != allowed_fields:
        raise ValueError("legacy v4 decision feature bundle fields are incompatible")
    if (
        decision_bundle.get("bundle_schema_version")
        != LEGACY_V4_FEATURE_BUNDLE_SCHEMA_VERSION
        or decision_bundle.get("feature_policy_version")
        != LEGACY_V4_FEATURE_BUNDLE_POLICY_VERSION
    ):
        raise ValueError("legacy v4 decision feature bundle version is incompatible")
    _strict_string(
        decision_bundle.get("feature_schema_version"),
        field="decision_feature_bundle.feature_schema_version",
    )
    if decision_bundle.get("symbol") != symbol:
        raise ValueError("legacy v4 decision feature bundle symbol mismatch")
    if decision_bundle.get("decision_time_utc") != _iso(decision_time_utc):
        raise ValueError("legacy v4 decision feature bundle timestamp mismatch")
    if not isinstance(decision_bundle.get("source_series_manifest"), Mapping):
        raise ValueError("legacy v4 decision feature source manifest is required")
    features = decision_bundle.get("features_by_direction")
    if (
        not isinstance(features, Mapping)
        or set(features) != {"LONG", "SHORT"}
        or any(not isinstance(features.get(side), Mapping) for side in features)
    ):
        raise ValueError("legacy v4 decision features require LONG and SHORT maps")
    horizon_context = decision_bundle.get("horizon_context")
    if not isinstance(horizon_context, Mapping) or set(horizon_context) != {
        "60",
        "240",
        "720",
        "1440",
    }:
        raise ValueError("legacy v4 decision feature horizon context is incomplete")
    if model_status == "FROZEN":
        if not isinstance(decision_bundle.get("frozen_model_wrapper"), Mapping):
            raise ValueError("legacy v4 frozen model wrapper is required")
    elif model_status != "ABSENT":
        raise ValueError("legacy v4 model score status is incompatible")

    actual_sha = hashlib.sha256(
        _canonical_json(dict(decision_bundle)).encode("utf-8")
    ).hexdigest()
    if actual_sha != normalized_sha:
        raise ValueError("legacy v4 decision feature bundle hash mismatch")


@dataclass(frozen=True)
class NeutralPriceAnchor:
    """One immutable, content-addressed neutral close-price anchor."""

    contract_version: str
    anchor_id: str
    anchor_receipt_sha256: str
    symbol: str
    origin: str
    sampler_version: str
    eligible_at_utc: datetime
    decision_time_utc: datetime
    source_price_candle_open_utc: datetime
    source_price_candle_close_utc: datetime
    observed_at_utc: datetime
    refresh_completed_at_utc: datetime
    price: Decimal
    source: str
    upstream_source: str
    price_exchange: str
    price_market: str
    price_pair: str
    price_instrument_id: Optional[str]
    price_timeframe: str
    quality_status: str
    fallback_used: bool
    fallback_policy: str
    price_candle_identity_basis: str
    source_input_fingerprint: Optional[str]
    source_record_created_at_utc: Optional[datetime]

    @classmethod
    def _build(
        cls,
        *,
        symbol: Any,
        origin: str,
        sampler_version: Any,
        eligible_at_utc: Any,
        decision_time_utc: Any,
        source_price_candle_open_utc: Any,
        source_price_candle_close_utc: Any,
        observed_at_utc: Any,
        refresh_completed_at_utc: Any,
        price: Any,
        source_provenance: Mapping[str, Any],
        price_candle_identity_basis: Any,
        source_input_fingerprint: Any = None,
        source_record_created_at_utc: Any = None,
    ) -> "NeutralPriceAnchor":
        normalized_symbol = _symbol(symbol)
        normalized_sampler = _strict_string(
            sampler_version, field="sampler_version"
        )
        if origin == "PROSPECTIVE_V5":
            if normalized_sampler != NEUTRAL_PRICE_SAMPLER_VERSION:
                raise ValueError("prospective anchor sampler version is incompatible")
            expected_basis = PROSPECTIVE_PRICE_CANDLE_IDENTITY_BASIS
        elif origin == "AUTHORIZED_LEGACY_V3_V4":
            if normalized_sampler not in AUTHORIZED_LEGACY_SAMPLERS:
                raise ValueError("legacy sampler is not authorized")
            expected_basis = HISTORICAL_PRICE_CANDLE_IDENTITY_BASIS
        else:
            raise ValueError("unsupported neutral-price anchor origin")
        basis = _strict_string(
            price_candle_identity_basis, field="price_candle_identity_basis"
        )
        if basis != expected_basis:
            raise ValueError("price_candle_identity_basis conflicts with contract")
        eligible, decision, opened, closed, observed, refreshed = (
            _validate_causal_times(
                eligible_at_utc=eligible_at_utc,
                decision_time_utc=decision_time_utc,
                source_price_candle_open_utc=source_price_candle_open_utc,
                source_price_candle_close_utc=source_price_candle_close_utc,
                observed_at_utc=observed_at_utc,
                refresh_completed_at_utc=refresh_completed_at_utc,
            )
        )
        normalized_price = _decimal(price)
        (
            source,
            exchange,
            market,
            instrument,
            timeframe,
            quality,
            fallback,
            fallback_policy,
            pair,
            upstream_source,
        ) = _route(normalized_symbol, source_provenance)
        fingerprint = None
        if source_input_fingerprint not in (None, ""):
            fingerprint = _require_hash(
                source_input_fingerprint, field="source_input_fingerprint"
            )
        source_created = _optional_utc(
            source_record_created_at_utc,
            field="source_record_created_at_utc",
        )
        if origin == "PROSPECTIVE_V5" and source_created is not None:
            raise ValueError(
                "prospective anchor cannot carry a legacy source created time"
            )
        if origin == "AUTHORIZED_LEGACY_V3_V4":
            if fingerprint is None:
                raise ValueError("authorized legacy slot requires its frozen input fingerprint")
            if source_created is None:
                raise ValueError("authorized legacy slot requires created_at_utc")
            if not (
                decision
                <= source_created
                <= decision + timedelta(minutes=5)
                and source_created
                < eligible + timedelta(minutes=CAPTURE_WINDOW_MINUTES)
            ):
                raise ValueError("legacy created_at_utc is outside its causal window")
        identity_payload = {
            "symbol": normalized_symbol,
            "eligible_at_utc": _iso(eligible),
            "source_price_candle_open_utc": _iso(opened),
        }
        anchor_id = _sha256("neutral-price-anchor-identity", identity_payload)
        receipt_payload = {
            **identity_payload,
            "origin": origin,
            "sampler_version": normalized_sampler,
            "decision_time_utc": _iso(decision),
            "source_price_candle_close_utc": _iso(closed),
            "observed_at_utc": _iso(observed),
            "refresh_completed_at_utc": _iso(refreshed),
            "price": _decimal_text(normalized_price),
            "source": source,
            "upstream_source": upstream_source,
            "price_exchange": exchange,
            "price_market": market,
            "price_pair": pair,
            "price_instrument_id": instrument,
            "price_timeframe": timeframe,
            "quality_status": quality,
            "fallback_used": fallback,
            "fallback_policy": fallback_policy,
            "price_candle_identity_basis": basis,
            "source_input_fingerprint": fingerprint,
            "source_record_created_at_utc": _optional_iso(
                source_created, field="source_record_created_at_utc"
            ),
        }
        return cls(
            contract_version=POLICY_VERSION,
            anchor_id=anchor_id,
            anchor_receipt_sha256=_sha256(
                "neutral-price-anchor-receipt", receipt_payload
            ),
            symbol=normalized_symbol,
            origin=origin,
            sampler_version=normalized_sampler,
            eligible_at_utc=eligible,
            decision_time_utc=decision,
            source_price_candle_open_utc=opened,
            source_price_candle_close_utc=closed,
            observed_at_utc=observed,
            refresh_completed_at_utc=refreshed,
            price=normalized_price,
            source=source,
            upstream_source=upstream_source,
            price_exchange=exchange,
            price_market=market,
            price_pair=pair,
            price_instrument_id=instrument,
            price_timeframe=timeframe,
            quality_status=quality,
            fallback_used=fallback,
            fallback_policy=fallback_policy,
            price_candle_identity_basis=basis,
            source_input_fingerprint=fingerprint,
            source_record_created_at_utc=source_created,
        )

    @classmethod
    def build_prospective(
        cls,
        *,
        symbol: Any,
        eligible_at_utc: Any,
        decision_time_utc: Any,
        source_price_candle_open_utc: Any,
        source_price_candle_close_utc: Any,
        observed_at_utc: Any,
        refresh_completed_at_utc: Any,
        price: Any,
        source_provenance: Mapping[str, Any],
        source_input_fingerprint: Any = None,
    ) -> "NeutralPriceAnchor":
        return cls._build(
            symbol=symbol,
            origin="PROSPECTIVE_V5",
            sampler_version=NEUTRAL_PRICE_SAMPLER_VERSION,
            eligible_at_utc=eligible_at_utc,
            decision_time_utc=decision_time_utc,
            source_price_candle_open_utc=source_price_candle_open_utc,
            source_price_candle_close_utc=source_price_candle_close_utc,
            observed_at_utc=observed_at_utc,
            refresh_completed_at_utc=refresh_completed_at_utc,
            price=price,
            source_provenance=source_provenance,
            price_candle_identity_basis=PROSPECTIVE_PRICE_CANDLE_IDENTITY_BASIS,
            source_input_fingerprint=source_input_fingerprint,
        )

    @classmethod
    def from_authorized_legacy_slot(
        cls, record: Mapping[str, Any]
    ) -> "NeutralPriceAnchor":
        """Build only from the already-frozen, authorized v3/v4 slot envelope.

        No provider value, import timestamp, or later archive field is used.
        Legacy records contain the provider close in
        ``source_timestamps.official_price.observed_at_utc`` but do not contain
        the 1m open.  The open is derived only after every sampler, route,
        timing, and no-fallback guard succeeds.
        """

        if not isinstance(record, Mapping):
            raise ValueError("legacy slot record must be a mapping")
        sampler = record.get("sampler_version")
        if sampler not in AUTHORIZED_LEGACY_SAMPLERS:
            raise ValueError("legacy sampler is not authorized")
        symbol = _symbol(record.get("symbol"))
        slot_open = _utc(
            record.get("source_candle_open_utc"),
            field="source_candle_open_utc",
        )
        slot_close = _utc(
            record.get("source_candle_close_utc"),
            field="source_candle_close_utc",
        )
        if (
            slot_open.second != 0
            or slot_open.microsecond != 0
            or slot_open.minute not in (0, 30)
            or slot_close != slot_open + timedelta(minutes=30)
        ):
            raise ValueError("legacy source slot is not an exact 30-minute candle")
        eligible = _eligibility(record.get("base_eligible_at_utc"))
        if eligible != slot_close + timedelta(minutes=2):
            raise ValueError("legacy eligibility does not equal close plus two minutes")
        if eligible < HISTORICAL_IMPORT_NOT_BEFORE_UTC:
            raise ValueError("legacy slot predates the authorized import boundary")
        expires = _utc(record.get("expires_at_utc"), field="expires_at_utc")
        if expires != eligible + timedelta(minutes=CAPTURE_WINDOW_MINUTES):
            raise ValueError("legacy capture window is incompatible")
        decision = _utc(
            record.get("decision_time_utc"), field="decision_time_utc"
        )
        source_timestamps = record.get("source_timestamps")
        source_provenance = record.get("source_provenance")
        frozen_inputs = record.get("frozen_inputs")
        if not isinstance(source_timestamps, Mapping):
            raise ValueError("legacy source_timestamps are required")
        if not isinstance(source_provenance, Mapping):
            raise ValueError("legacy source_provenance is required")
        if not isinstance(frozen_inputs, Mapping):
            raise ValueError("legacy frozen_inputs are required")
        _validate_authorized_legacy_version_shape(
            record,
            sampler=sampler,
            symbol=symbol,
            decision_time_utc=decision,
        )
        required_families = {"official_price", "price_oi", "futures_cvd", "spot_cvd"}
        for field, envelope in (
            ("source_timestamps", source_timestamps),
            ("source_provenance", source_provenance),
            ("frozen_inputs", frozen_inputs),
        ):
            if not required_families.issubset(envelope):
                raise ValueError(f"legacy {field} is missing a frozen source family")
            if any(not isinstance(envelope.get(family), Mapping) for family in required_families):
                raise ValueError(f"legacy {field} source families must be objects")
        timestamps = source_timestamps.get("official_price")
        provenance = source_provenance.get("official_price")
        frozen_price = frozen_inputs.get("official_price")
        if not isinstance(timestamps, Mapping):
            raise ValueError("legacy official-price timestamps are required")
        if not isinstance(provenance, Mapping):
            raise ValueError("legacy official-price provenance is required")
        if not isinstance(frozen_price, Mapping):
            raise ValueError("legacy frozen official price is required")
        observed = _utc(
            timestamps.get("observed_at_utc"), field="observed_at_utc"
        )
        refreshed = _utc(
            timestamps.get("refresh_completed_at_utc"),
            field="refresh_completed_at_utc",
        )
        derived_open = eligible - timedelta(minutes=1)

        for container in (record, timestamps, provenance, frozen_price):
            if (
                "price_candle_identity_basis" in container
                and container.get("price_candle_identity_basis")
                != HISTORICAL_PRICE_CANDLE_IDENTITY_BASIS
            ):
                raise ValueError("legacy price_candle_identity_basis conflicts with contract")

        # Some later exports may carry the derived fields.  They are evidence,
        # never an alternative derivation: if present they must match exactly.
        for container in (record, timestamps, provenance, frozen_price):
            for key in (
                "source_price_candle_open_utc",
                "price_candle_open_utc",
                "price_candle_open_time_utc",
                "candle_open_time_utc",
            ):
                if key in container:
                    if _utc(container.get(key), field=key) != derived_open:
                        raise ValueError("supplied legacy price-candle open conflicts")
            for key in (
                "source_price_candle_close_utc",
                "price_candle_close_utc",
                "price_candle_close_time_utc",
                "price_observed_at_utc",
                "candle_close_time_utc",
            ):
                if key in container:
                    if _utc(container.get(key), field=key) != observed:
                        raise ValueError("supplied legacy price-candle close conflicts")

        legacy_price = frozen_price.get("price")
        if isinstance(legacy_price, bool) or not isinstance(
            legacy_price, (int, float, Decimal)
        ):
            raise ValueError("legacy frozen official price must be a JSON number")
        recorded_fingerprint = _require_hash(
            record.get("input_fingerprint"), field="input_fingerprint"
        )
        if record.get("evaluation_status") not in (None, EVALUABLE):
            raise ValueError("legacy captured slot must be evaluable")
        expected_fingerprint = compute_authorized_legacy_input_fingerprint(record)
        if recorded_fingerprint != expected_fingerprint:
            raise ValueError("legacy input_fingerprint does not match frozen slot")
        created_at = _utc(
            record.get("created_at_utc"), field="created_at_utc"
        )

        return cls._build(
            symbol=symbol,
            origin="AUTHORIZED_LEGACY_V3_V4",
            sampler_version=sampler,
            eligible_at_utc=eligible,
            decision_time_utc=decision,
            source_price_candle_open_utc=derived_open,
            source_price_candle_close_utc=observed,
            observed_at_utc=observed,
            refresh_completed_at_utc=refreshed,
            price=legacy_price,
            source_provenance=provenance,
            price_candle_identity_basis=(
                HISTORICAL_PRICE_CANDLE_IDENTITY_BASIS
            ),
            source_input_fingerprint=recorded_fingerprint,
            source_record_created_at_utc=created_at,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "anchor_id": self.anchor_id,
            "anchor_receipt_sha256": self.anchor_receipt_sha256,
            "symbol": self.symbol,
            "origin": self.origin,
            "sampler_version": self.sampler_version,
            "eligible_at_utc": _iso(self.eligible_at_utc),
            "decision_time_utc": _iso(self.decision_time_utc),
            "source_price_candle_open_utc": _iso(
                self.source_price_candle_open_utc
            ),
            "source_price_candle_close_utc": _iso(
                self.source_price_candle_close_utc
            ),
            "observed_at_utc": _iso(self.observed_at_utc),
            "refresh_completed_at_utc": _iso(self.refresh_completed_at_utc),
            "price": _decimal_text(self.price),
            "source": self.source,
            "upstream_source": self.upstream_source,
            "price_exchange": self.price_exchange,
            "price_market": self.price_market,
            "price_pair": self.price_pair,
            "price_instrument_id": self.price_instrument_id,
            "price_timeframe": self.price_timeframe,
            "quality_status": self.quality_status,
            "fallback_used": self.fallback_used,
            "fallback_policy": self.fallback_policy,
            "price_candle_identity_basis": self.price_candle_identity_basis,
            "source_input_fingerprint": self.source_input_fingerprint,
            "source_record_created_at_utc": _optional_iso(
                self.source_record_created_at_utc,
                field="source_record_created_at_utc",
            ),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "NeutralPriceAnchor":
        if not isinstance(value, Mapping):
            raise ValueError("neutral price anchor must be a mapping")
        fields = {
            "contract_version", "anchor_id", "anchor_receipt_sha256", "symbol",
            "origin", "sampler_version", "eligible_at_utc", "decision_time_utc",
            "source_price_candle_open_utc", "source_price_candle_close_utc",
            "observed_at_utc", "refresh_completed_at_utc", "price", "source",
            "upstream_source",
            "price_exchange", "price_market", "price_pair", "price_instrument_id",
            "price_timeframe", "quality_status", "fallback_used", "fallback_policy",
            "price_candle_identity_basis", "source_input_fingerprint",
            "source_record_created_at_utc",
        }
        _only_fields(value, fields, kind="neutral-price anchor")
        if value.get("contract_version") != POLICY_VERSION:
            raise ValueError("neutral-price anchor contract version is incompatible")
        provenance = {
            "source": value.get("source"),
            "upstream_source": value.get("upstream_source"),
            "quality_status": value.get("quality_status"),
            "price_exchange": value.get("price_exchange"),
            "price_market": value.get("price_market"),
            "price_pair": value.get("price_pair"),
            "price_instrument_id": value.get("price_instrument_id"),
            "price_timeframe": value.get("price_timeframe"),
            "fallback_used": value.get("fallback_used"),
            "fallback_policy": value.get("fallback_policy"),
        }
        rebuilt = cls._build(
            symbol=value.get("symbol"),
            origin=value.get("origin"),
            sampler_version=value.get("sampler_version"),
            eligible_at_utc=value.get("eligible_at_utc"),
            decision_time_utc=value.get("decision_time_utc"),
            source_price_candle_open_utc=value.get(
                "source_price_candle_open_utc"
            ),
            source_price_candle_close_utc=value.get(
                "source_price_candle_close_utc"
            ),
            observed_at_utc=value.get("observed_at_utc"),
            refresh_completed_at_utc=value.get("refresh_completed_at_utc"),
            price=value.get("price"),
            source_provenance=provenance,
            price_candle_identity_basis=value.get(
                "price_candle_identity_basis"
            ),
            source_input_fingerprint=value.get("source_input_fingerprint"),
            source_record_created_at_utc=value.get(
                "source_record_created_at_utc"
            ),
        )
        if value.get("anchor_id") != rebuilt.anchor_id:
            raise ValueError("neutral-price anchor identity is forged")
        if value.get("anchor_receipt_sha256") != rebuilt.anchor_receipt_sha256:
            raise ValueError("neutral-price anchor receipt is forged")
        if _canonical_json(dict(value)) != _canonical_json(rebuilt.to_dict()):
            raise ValueError("neutral-price anchor is not canonical")
        return rebuilt


@dataclass(frozen=True)
class NeutralPriceAnchorAttempt:
    contract_version: str
    attempt_receipt_sha256: str
    symbol: str
    eligible_at_utc: datetime
    decision_time_utc: datetime
    evaluation_status: str
    evaluation_reason: str
    anchor_id: Optional[str]
    anchor_receipt_sha256: Optional[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "attempt_receipt_sha256": self.attempt_receipt_sha256,
            "symbol": self.symbol,
            "eligible_at_utc": _iso(self.eligible_at_utc),
            "decision_time_utc": _iso(self.decision_time_utc),
            "evaluation_status": self.evaluation_status,
            "evaluation_reason": self.evaluation_reason,
            "anchor_id": self.anchor_id,
            "anchor_receipt_sha256": self.anchor_receipt_sha256,
        }


@dataclass(frozen=True)
class NeutralPriceAnchorDecision:
    attempt: NeutralPriceAnchorAttempt
    anchor: Optional[NeutralPriceAnchor]


def evaluate_prospective_anchor(
    *,
    symbol: Any,
    eligible_at_utc: Any,
    decision_time_utc: Any,
    price_candle: Any,
    source_provenance: Any,
    source_input_fingerprint: Any = None,
) -> NeutralPriceAnchorDecision:
    """Return an auditable ``UNEVALUABLE`` attempt instead of a partial anchor.

    Scheduling identity fields are required and fail directly if malformed;
    source/candle failures are represented as ``UNEVALUABLE`` with no anchor.
    """

    normalized_symbol = _symbol(symbol)
    eligible = _eligibility(eligible_at_utc)
    decision = _utc(decision_time_utc, field="decision_time_utc")
    anchor: Optional[NeutralPriceAnchor] = None
    try:
        if not isinstance(price_candle, Mapping):
            raise ValueError("price candle is required")
        anchor = NeutralPriceAnchor.build_prospective(
            symbol=normalized_symbol,
            eligible_at_utc=eligible,
            decision_time_utc=decision,
            source_price_candle_open_utc=price_candle.get("open_time_utc"),
            source_price_candle_close_utc=price_candle.get("close_time_utc"),
            observed_at_utc=price_candle.get("observed_at_utc"),
            refresh_completed_at_utc=price_candle.get(
                "refresh_completed_at_utc"
            ),
            price=price_candle.get("price"),
            source_provenance=source_provenance,
            source_input_fingerprint=source_input_fingerprint,
        )
        status = EVALUABLE
        reason = "EXACT_CLOSED_1M_NEUTRAL_PRICE_ANCHOR"
    except (TypeError, ValueError) as exc:
        status = UNEVALUABLE
        reason = str(exc)
    payload = {
        "symbol": normalized_symbol,
        "eligible_at_utc": _iso(eligible),
        "decision_time_utc": _iso(decision),
        "evaluation_status": status,
        "evaluation_reason": reason,
        "anchor_id": anchor.anchor_id if anchor is not None else None,
        "anchor_receipt_sha256": (
            anchor.anchor_receipt_sha256 if anchor is not None else None
        ),
    }
    attempt = NeutralPriceAnchorAttempt(
        contract_version=POLICY_VERSION,
        attempt_receipt_sha256=_sha256("neutral-price-anchor-attempt", payload),
        symbol=normalized_symbol,
        eligible_at_utc=eligible,
        decision_time_utc=decision,
        evaluation_status=status,
        evaluation_reason=reason,
        anchor_id=payload["anchor_id"],
        anchor_receipt_sha256=payload["anchor_receipt_sha256"],
    )
    return NeutralPriceAnchorDecision(attempt=attempt, anchor=anchor)


@dataclass(frozen=True)
class MovementIdentity:
    contract_version: str
    namespace: str
    symbol: str
    stream_id: str

    @classmethod
    def for_symbol(cls, symbol: Any) -> "MovementIdentity":
        return cls._build(namespace=SYMBOL_NAMESPACE, symbol=symbol)

    @classmethod
    def btc_parent(cls) -> "MovementIdentity":
        return cls._build(namespace=BTC_PARENT_NAMESPACE, symbol="BTC")

    @classmethod
    def _build(cls, *, namespace: Any, symbol: Any) -> "MovementIdentity":
        if namespace not in (SYMBOL_NAMESPACE, BTC_PARENT_NAMESPACE):
            raise ValueError("movement namespace must be SYMBOL or BTC_PARENT")
        normalized_symbol = _symbol(symbol)
        if namespace == BTC_PARENT_NAMESPACE and normalized_symbol != "BTC":
            raise ValueError("BTC_PARENT accepts only BTC neutral-price anchors")
        stream_id = _sha256(
            "movement-stream-identity",
            {"namespace": namespace, "symbol": normalized_symbol},
        )
        return cls(POLICY_VERSION, namespace, normalized_symbol, stream_id)

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "namespace": self.namespace,
            "symbol": self.symbol,
            "stream_id": self.stream_id,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "MovementIdentity":
        fields = {"contract_version", "namespace", "symbol", "stream_id"}
        if not isinstance(value, Mapping):
            raise ValueError("movement identity must be a mapping")
        _only_fields(value, fields, kind="movement identity")
        if value.get("contract_version") != POLICY_VERSION:
            raise ValueError("movement identity contract version is incompatible")
        rebuilt = cls._build(
            namespace=value.get("namespace"), symbol=value.get("symbol")
        )
        if value.get("stream_id") != rebuilt.stream_id:
            raise ValueError("movement stream identity is forged")
        return rebuilt


@dataclass(frozen=True)
class MovementState:
    contract_version: str
    stream_id: str
    namespace: str
    symbol: str
    movement_id: str
    status: str
    direction: str
    started_anchor_id: str
    started_eligible_at_utc: datetime
    started_decision_time_utc: datetime
    start_price: Decimal
    extreme_anchor_id: str
    extreme_eligible_at_utc: datetime
    extreme_price: Decimal
    last_member_anchor_id: str
    last_member_eligible_at_utc: datetime
    last_member_decision_time_utc: datetime
    last_member_price: Decimal
    member_count: int
    consecutive_non_extremes: int
    closed_at_utc: Optional[datetime]
    close_boundary_eligible_at_utc: Optional[datetime]
    close_reason: Optional[str]
    state_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "stream_id": self.stream_id,
            "namespace": self.namespace,
            "symbol": self.symbol,
            "movement_id": self.movement_id,
            "status": self.status,
            "direction": self.direction,
            "started_anchor_id": self.started_anchor_id,
            "started_eligible_at_utc": _iso(self.started_eligible_at_utc),
            "started_decision_time_utc": _iso(self.started_decision_time_utc),
            "start_price": _decimal_text(self.start_price),
            "extreme_anchor_id": self.extreme_anchor_id,
            "extreme_eligible_at_utc": _iso(self.extreme_eligible_at_utc),
            "extreme_price": _decimal_text(self.extreme_price),
            "last_member_anchor_id": self.last_member_anchor_id,
            "last_member_eligible_at_utc": _iso(
                self.last_member_eligible_at_utc
            ),
            "last_member_decision_time_utc": _iso(
                self.last_member_decision_time_utc
            ),
            "last_member_price": _decimal_text(self.last_member_price),
            "member_count": self.member_count,
            "consecutive_non_extremes": self.consecutive_non_extremes,
            "closed_at_utc": _optional_iso(
                self.closed_at_utc, field="closed_at_utc"
            ),
            "close_boundary_eligible_at_utc": _optional_iso(
                self.close_boundary_eligible_at_utc,
                field="close_boundary_eligible_at_utc",
            ),
            "close_reason": self.close_reason,
            "state_sha256": self.state_sha256,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "MovementState":
        fields = {
            "contract_version", "stream_id", "namespace", "symbol",
            "movement_id", "status", "direction", "started_anchor_id",
            "started_eligible_at_utc", "started_decision_time_utc", "start_price",
            "extreme_anchor_id", "extreme_eligible_at_utc", "extreme_price",
            "last_member_anchor_id", "last_member_eligible_at_utc",
            "last_member_decision_time_utc", "last_member_price", "member_count",
            "consecutive_non_extremes", "closed_at_utc",
            "close_boundary_eligible_at_utc", "close_reason", "state_sha256",
        }
        if not isinstance(value, Mapping):
            raise ValueError("movement state must be a mapping")
        _only_fields(value, fields, kind="movement state")
        if value.get("contract_version") != POLICY_VERSION:
            raise ValueError("movement state contract version is incompatible")
        identity = MovementIdentity._build(
            namespace=value.get("namespace"), symbol=value.get("symbol")
        )
        if value.get("stream_id") != identity.stream_id:
            raise ValueError("movement state stream identity is forged")
        rebuilt = _make_state(
            identity=identity,
            movement_id=value.get("movement_id"),
            status=value.get("status"),
            direction=value.get("direction"),
            started_anchor_id=value.get("started_anchor_id"),
            started_eligible_at_utc=value.get("started_eligible_at_utc"),
            started_decision_time_utc=value.get("started_decision_time_utc"),
            start_price=value.get("start_price"),
            extreme_anchor_id=value.get("extreme_anchor_id"),
            extreme_eligible_at_utc=value.get("extreme_eligible_at_utc"),
            extreme_price=value.get("extreme_price"),
            last_member_anchor_id=value.get("last_member_anchor_id"),
            last_member_eligible_at_utc=value.get("last_member_eligible_at_utc"),
            last_member_decision_time_utc=value.get(
                "last_member_decision_time_utc"
            ),
            last_member_price=value.get("last_member_price"),
            member_count=value.get("member_count"),
            consecutive_non_extremes=value.get("consecutive_non_extremes"),
            closed_at_utc=value.get("closed_at_utc"),
            close_boundary_eligible_at_utc=value.get(
                "close_boundary_eligible_at_utc"
            ),
            close_reason=value.get("close_reason"),
        )
        if value.get("state_sha256") != rebuilt.state_sha256:
            raise ValueError("movement state receipt is forged")
        if _canonical_json(dict(value)) != _canonical_json(rebuilt.to_dict()):
            raise ValueError("movement state is not canonical")
        return rebuilt


def _make_state(
    *,
    identity: MovementIdentity,
    movement_id: Any,
    status: Any,
    direction: Any,
    started_anchor_id: Any,
    started_eligible_at_utc: Any,
    started_decision_time_utc: Any,
    start_price: Any,
    extreme_anchor_id: Any,
    extreme_eligible_at_utc: Any,
    extreme_price: Any,
    last_member_anchor_id: Any,
    last_member_eligible_at_utc: Any,
    last_member_decision_time_utc: Any,
    last_member_price: Any,
    member_count: Any,
    consecutive_non_extremes: Any,
    closed_at_utc: Any = None,
    close_boundary_eligible_at_utc: Any = None,
    close_reason: Any = None,
) -> MovementState:
    MovementIdentity.from_dict(identity.to_dict())
    normalized_movement_id = _require_hash(movement_id, field="movement_id")
    if status not in (OPEN_STATUS, CLOSED_STATUS):
        raise ValueError("movement status must be OPEN or CLOSED")
    if direction not in (PENDING_DIRECTION, UP_DIRECTION, DOWN_DIRECTION):
        raise ValueError("movement direction is incompatible")
    started_id = _require_hash(started_anchor_id, field="started_anchor_id")
    expected_movement_id = _sha256(
        "market-movement-identity",
        {"stream_id": identity.stream_id, "started_anchor_id": started_id},
    )
    if normalized_movement_id != expected_movement_id:
        raise ValueError("movement_id does not match stream and starting anchor")
    extreme_id = _require_hash(extreme_anchor_id, field="extreme_anchor_id")
    last_id = _require_hash(last_member_anchor_id, field="last_member_anchor_id")
    started_time = _eligibility(started_eligible_at_utc)
    started_decision = _utc(
        started_decision_time_utc, field="started_decision_time_utc"
    )
    extreme_time = _eligibility(extreme_eligible_at_utc)
    last_time = _eligibility(last_member_eligible_at_utc)
    last_decision = _utc(
        last_member_decision_time_utc, field="last_member_decision_time_utc"
    )
    start_value = _decimal(start_price, field="start_price")
    extreme_value = _decimal(extreme_price, field="extreme_price")
    last_value = _decimal(last_member_price, field="last_member_price")
    count = _strict_int(member_count, field="member_count", minimum=1)
    streak = _strict_int(
        consecutive_non_extremes,
        field="consecutive_non_extremes",
        minimum=0,
    )
    if streak > 1:
        raise ValueError("an open movement cannot retain two non-extremes")
    if not started_time <= extreme_time <= last_time:
        raise ValueError("movement anchor times are inconsistent")
    if started_decision < started_time or last_decision < last_time:
        raise ValueError("movement decision times precede eligibility")
    if (
        started_decision >= started_time + timedelta(minutes=CAPTURE_WINDOW_MINUTES)
        or last_decision >= last_time + timedelta(minutes=CAPTURE_WINDOW_MINUTES)
    ):
        raise ValueError("movement decision time is outside its capture window")
    if last_time != started_time + timedelta(
        minutes=INTERVAL_MINUTES * (count - 1)
    ):
        raise ValueError("movement member count does not match its continuous lattice")
    if count == 1 and (
        started_id != extreme_id
        or started_id != last_id
        or start_value != extreme_value
        or start_value != last_value
        or direction != PENDING_DIRECTION
        or streak != 0
    ):
        raise ValueError("one-member movement must be the pending start state")
    if direction == PENDING_DIRECTION and (
        extreme_id != started_id
        or extreme_time != started_time
        or extreme_value != start_value
    ):
        raise ValueError("pending movement cannot have a directional extreme")
    if direction == UP_DIRECTION and extreme_value <= start_value:
        raise ValueError("UP movement extreme must exceed its start")
    if direction == DOWN_DIRECTION and extreme_value >= start_value:
        raise ValueError("DOWN movement extreme must be below its start")
    if streak == 0 and (
        last_id != extreme_id
        or last_time != extreme_time
        or last_value != extreme_value
    ):
        raise ValueError("zero-streak movement must end at its directional extreme")
    closed = _optional_utc(closed_at_utc, field="closed_at_utc")
    boundary = _optional_utc(
        close_boundary_eligible_at_utc,
        field="close_boundary_eligible_at_utc",
    )
    if boundary is not None:
        boundary = _eligibility(boundary)
    if status == OPEN_STATUS:
        if closed is not None or boundary is not None or close_reason is not None:
            raise ValueError("open movement cannot carry closure fields")
        normalized_reason = None
    else:
        if closed is None or boundary is None:
            raise ValueError("closed movement requires closure timestamps")
        if close_reason not in (
            DATA_GAP_CENSORED,
            TWO_CONSECUTIVE_NON_EXTREMES,
        ):
            raise ValueError("closed movement reason is incompatible")
        if closed < boundary:
            raise ValueError("movement close decision precedes its boundary")
        normalized_reason = str(close_reason)
    payload = {
        "stream_id": identity.stream_id,
        "namespace": identity.namespace,
        "symbol": identity.symbol,
        "movement_id": normalized_movement_id,
        "status": status,
        "direction": direction,
        "started_anchor_id": started_id,
        "started_eligible_at_utc": _iso(started_time),
        "started_decision_time_utc": _iso(started_decision),
        "start_price": _decimal_text(start_value),
        "extreme_anchor_id": extreme_id,
        "extreme_eligible_at_utc": _iso(extreme_time),
        "extreme_price": _decimal_text(extreme_value),
        "last_member_anchor_id": last_id,
        "last_member_eligible_at_utc": _iso(last_time),
        "last_member_decision_time_utc": _iso(last_decision),
        "last_member_price": _decimal_text(last_value),
        "member_count": count,
        "consecutive_non_extremes": streak,
        "closed_at_utc": _optional_iso(closed, field="closed_at_utc"),
        "close_boundary_eligible_at_utc": _optional_iso(
            boundary, field="close_boundary_eligible_at_utc"
        ),
        "close_reason": normalized_reason,
    }
    return MovementState(
        contract_version=POLICY_VERSION,
        stream_id=identity.stream_id,
        namespace=identity.namespace,
        symbol=identity.symbol,
        movement_id=normalized_movement_id,
        status=status,
        direction=direction,
        started_anchor_id=started_id,
        started_eligible_at_utc=started_time,
        started_decision_time_utc=started_decision,
        start_price=start_value,
        extreme_anchor_id=extreme_id,
        extreme_eligible_at_utc=extreme_time,
        extreme_price=extreme_value,
        last_member_anchor_id=last_id,
        last_member_eligible_at_utc=last_time,
        last_member_decision_time_utc=last_decision,
        last_member_price=last_value,
        member_count=count,
        consecutive_non_extremes=streak,
        closed_at_utc=closed,
        close_boundary_eligible_at_utc=boundary,
        close_reason=normalized_reason,
        state_sha256=_sha256("movement-state", payload),
    )


@dataclass(frozen=True)
class MovementTransition:
    contract_version: str
    transition_receipt_sha256: str
    previous_transition_receipt_sha256: Optional[str]
    transition_type: str
    stream_id: str
    movement_id: str
    trigger_anchor_id: str
    trigger_eligible_at_utc: datetime
    trigger_decision_time_utc: datetime
    pre_state_sha256: Optional[str]
    post_state: MovementState

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "transition_receipt_sha256": self.transition_receipt_sha256,
            "previous_transition_receipt_sha256": (
                self.previous_transition_receipt_sha256
            ),
            "transition_type": self.transition_type,
            "stream_id": self.stream_id,
            "movement_id": self.movement_id,
            "trigger_anchor_id": self.trigger_anchor_id,
            "trigger_eligible_at_utc": _iso(self.trigger_eligible_at_utc),
            "trigger_decision_time_utc": _iso(self.trigger_decision_time_utc),
            "pre_state_sha256": self.pre_state_sha256,
            "post_state": self.post_state.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "MovementTransition":
        fields = {
            "contract_version", "transition_receipt_sha256",
            "previous_transition_receipt_sha256", "transition_type", "stream_id",
            "movement_id", "trigger_anchor_id", "trigger_eligible_at_utc",
            "trigger_decision_time_utc", "pre_state_sha256", "post_state",
        }
        if not isinstance(value, Mapping):
            raise ValueError("movement transition must be a mapping")
        _only_fields(value, fields, kind="movement transition")
        if value.get("contract_version") != POLICY_VERSION:
            raise ValueError("movement transition contract version is incompatible")
        post = MovementState.from_dict(value.get("post_state"))
        transition = _make_transition(
            previous_transition_receipt_sha256=value.get(
                "previous_transition_receipt_sha256"
            ),
            transition_type=value.get("transition_type"),
            trigger_anchor_id=value.get("trigger_anchor_id"),
            trigger_eligible_at_utc=value.get("trigger_eligible_at_utc"),
            trigger_decision_time_utc=value.get("trigger_decision_time_utc"),
            pre_state_sha256=value.get("pre_state_sha256"),
            post_state=post,
        )
        if value.get("stream_id") != transition.stream_id:
            raise ValueError("movement transition stream is forged")
        if value.get("movement_id") != transition.movement_id:
            raise ValueError("movement transition movement is forged")
        if value.get("transition_receipt_sha256") != transition.transition_receipt_sha256:
            raise ValueError("movement transition receipt is forged")
        if _canonical_json(dict(value)) != _canonical_json(transition.to_dict()):
            raise ValueError("movement transition is not canonical")
        return transition


def _make_transition(
    *,
    previous_transition_receipt_sha256: Any,
    transition_type: Any,
    trigger_anchor_id: Any,
    trigger_eligible_at_utc: Any,
    trigger_decision_time_utc: Any,
    pre_state_sha256: Any,
    post_state: MovementState,
) -> MovementTransition:
    MovementState.from_dict(post_state.to_dict())
    allowed_types = {
        OPENED,
        OPENED_AFTER_DATA_GAP,
        OPENED_AFTER_DIRECTION_END,
        DIRECTION_ESTABLISHED,
        EXTREME_EXTENDED,
        NON_EXTREME_OBSERVED,
        MOVEMENT_CLOSED,
    }
    if transition_type not in allowed_types:
        raise ValueError("movement transition type is incompatible")
    previous = None
    if previous_transition_receipt_sha256 is not None:
        previous = _require_hash(
            previous_transition_receipt_sha256,
            field="previous_transition_receipt_sha256",
        )
    pre_state = None
    if pre_state_sha256 is not None:
        pre_state = _require_hash(pre_state_sha256, field="pre_state_sha256")
    opening_types = {
        OPENED,
        OPENED_AFTER_DATA_GAP,
        OPENED_AFTER_DIRECTION_END,
    }
    if transition_type == OPENED:
        if previous is not None or pre_state is not None:
            raise ValueError("initial OPENED transition cannot have a predecessor")
    elif previous is None:
        raise ValueError("non-initial transition requires a predecessor receipt")
    if transition_type in opening_types and pre_state is not None:
        raise ValueError("movement opening transition cannot reuse a prior state")
    if transition_type not in opening_types and pre_state is None:
        raise ValueError("movement continuation transition requires a prior state")
    if transition_type == MOVEMENT_CLOSED:
        if post_state.status != CLOSED_STATUS:
            raise ValueError("MOVEMENT_CLOSED transition requires a closed state")
    elif post_state.status != OPEN_STATUS:
        raise ValueError("non-closing transition requires an open state")
    trigger_id = _require_hash(trigger_anchor_id, field="trigger_anchor_id")
    trigger_time = _eligibility(trigger_eligible_at_utc)
    trigger_decision = _utc(
        trigger_decision_time_utc, field="trigger_decision_time_utc"
    )
    if trigger_decision < trigger_time:
        raise ValueError("transition trigger decision precedes eligibility")
    payload = {
        "previous_transition_receipt_sha256": previous,
        "transition_type": transition_type,
        "stream_id": post_state.stream_id,
        "movement_id": post_state.movement_id,
        "trigger_anchor_id": trigger_id,
        "trigger_eligible_at_utc": _iso(trigger_time),
        "trigger_decision_time_utc": _iso(trigger_decision),
        "pre_state_sha256": pre_state,
        "post_state_sha256": post_state.state_sha256,
    }
    return MovementTransition(
        contract_version=POLICY_VERSION,
        transition_receipt_sha256=_sha256("movement-transition", payload),
        previous_transition_receipt_sha256=previous,
        transition_type=str(transition_type),
        stream_id=post_state.stream_id,
        movement_id=post_state.movement_id,
        trigger_anchor_id=trigger_id,
        trigger_eligible_at_utc=trigger_time,
        trigger_decision_time_utc=trigger_decision,
        pre_state_sha256=pre_state,
        post_state=post_state,
    )


@dataclass(frozen=True)
class MovementMembership:
    contract_version: str
    membership_receipt_sha256: str
    stream_id: str
    movement_id: str
    anchor_id: str
    anchor_receipt_sha256: str
    ordinal: int
    classification: str
    eligible_at_utc: datetime
    decision_time_utc: datetime
    price: Decimal

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "membership_receipt_sha256": self.membership_receipt_sha256,
            "stream_id": self.stream_id,
            "movement_id": self.movement_id,
            "anchor_id": self.anchor_id,
            "anchor_receipt_sha256": self.anchor_receipt_sha256,
            "ordinal": self.ordinal,
            "classification": self.classification,
            "eligible_at_utc": _iso(self.eligible_at_utc),
            "decision_time_utc": _iso(self.decision_time_utc),
            "price": _decimal_text(self.price),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "MovementMembership":
        fields = {
            "contract_version", "membership_receipt_sha256", "stream_id",
            "movement_id", "anchor_id", "anchor_receipt_sha256", "ordinal",
            "classification", "eligible_at_utc", "decision_time_utc", "price",
        }
        if not isinstance(value, Mapping):
            raise ValueError("movement membership must be a mapping")
        _only_fields(value, fields, kind="movement membership")
        if value.get("contract_version") != POLICY_VERSION:
            raise ValueError("movement membership contract version is incompatible")
        rebuilt = _make_membership(
            stream_id=value.get("stream_id"),
            movement_id=value.get("movement_id"),
            anchor_id=value.get("anchor_id"),
            anchor_receipt_sha256=value.get("anchor_receipt_sha256"),
            ordinal=value.get("ordinal"),
            classification=value.get("classification"),
            eligible_at_utc=value.get("eligible_at_utc"),
            decision_time_utc=value.get("decision_time_utc"),
            price=value.get("price"),
        )
        if value.get("membership_receipt_sha256") != rebuilt.membership_receipt_sha256:
            raise ValueError("movement membership receipt is forged")
        if _canonical_json(dict(value)) != _canonical_json(rebuilt.to_dict()):
            raise ValueError("movement membership is not canonical")
        return rebuilt


def _make_membership(
    *,
    stream_id: Any,
    movement_id: Any,
    anchor_id: Any,
    anchor_receipt_sha256: Any,
    ordinal: Any,
    classification: Any,
    eligible_at_utc: Any,
    decision_time_utc: Any,
    price: Any,
) -> MovementMembership:
    stream = _require_hash(stream_id, field="stream_id")
    movement = _require_hash(movement_id, field="movement_id")
    anchor = _require_hash(anchor_id, field="anchor_id")
    anchor_receipt = _require_hash(
        anchor_receipt_sha256, field="anchor_receipt_sha256"
    )
    sequence = _strict_int(ordinal, field="ordinal", minimum=1)
    allowed = {
        START_MEMBER,
        DIRECTIONAL_EXTREME_MEMBER,
        EXTREME_EXTENSION_MEMBER,
        NON_EXTREME_MEMBER,
    }
    if classification not in allowed:
        raise ValueError("movement membership classification is incompatible")
    eligible = _eligibility(eligible_at_utc)
    decision = _utc(decision_time_utc, field="decision_time_utc")
    if decision < eligible:
        raise ValueError("membership decision precedes eligibility")
    normalized_price = _decimal(price)
    payload = {
        "stream_id": stream,
        "movement_id": movement,
        "anchor_id": anchor,
        "anchor_receipt_sha256": anchor_receipt,
        "ordinal": sequence,
        "classification": classification,
        "eligible_at_utc": _iso(eligible),
        "decision_time_utc": _iso(decision),
        "price": _decimal_text(normalized_price),
    }
    return MovementMembership(
        contract_version=POLICY_VERSION,
        membership_receipt_sha256=_sha256("movement-membership", payload),
        stream_id=stream,
        movement_id=movement,
        anchor_id=anchor,
        anchor_receipt_sha256=anchor_receipt,
        ordinal=sequence,
        classification=str(classification),
        eligible_at_utc=eligible,
        decision_time_utc=decision,
        price=normalized_price,
    )


@dataclass(frozen=True)
class MovementCursor:
    identity: MovementIdentity
    state: MovementState
    last_transition_receipt_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "identity": self.identity.to_dict(),
            "state": self.state.to_dict(),
            "last_transition_receipt_sha256": (
                self.last_transition_receipt_sha256
            ),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "MovementCursor":
        fields = {"identity", "state", "last_transition_receipt_sha256"}
        if not isinstance(value, Mapping):
            raise ValueError("movement cursor must be a mapping")
        _only_fields(value, fields, kind="movement cursor")
        identity = MovementIdentity.from_dict(value.get("identity"))
        state = MovementState.from_dict(value.get("state"))
        receipt = _require_hash(
            value.get("last_transition_receipt_sha256"),
            field="last_transition_receipt_sha256",
        )
        if state.stream_id != identity.stream_id or state.status != OPEN_STATUS:
            raise ValueError("movement cursor state is incompatible")
        return cls(identity=identity, state=state, last_transition_receipt_sha256=receipt)


@dataclass(frozen=True)
class MovementAdvance:
    cursor: MovementCursor
    transitions: tuple[MovementTransition, ...]
    memberships: tuple[MovementMembership, ...]


@dataclass(frozen=True)
class MovementHistory:
    identity: MovementIdentity
    cursor: Optional[MovementCursor]
    transitions: tuple[MovementTransition, ...]
    memberships: tuple[MovementMembership, ...]

    def canonical_receipts(self) -> tuple[tuple[str, ...], tuple[str, ...]]:
        return (
            tuple(item.transition_receipt_sha256 for item in self.transitions),
            tuple(item.membership_receipt_sha256 for item in self.memberships),
        )


def _movement_id(identity: MovementIdentity, anchor: NeutralPriceAnchor) -> str:
    return _sha256(
        "market-movement-identity",
        {
            "stream_id": identity.stream_id,
            "started_anchor_id": anchor.anchor_id,
        },
    )


def _new_state(
    identity: MovementIdentity, anchor: NeutralPriceAnchor
) -> MovementState:
    movement_id = _movement_id(identity, anchor)
    return _make_state(
        identity=identity,
        movement_id=movement_id,
        status=OPEN_STATUS,
        direction=PENDING_DIRECTION,
        started_anchor_id=anchor.anchor_id,
        started_eligible_at_utc=anchor.eligible_at_utc,
        started_decision_time_utc=anchor.decision_time_utc,
        start_price=anchor.price,
        extreme_anchor_id=anchor.anchor_id,
        extreme_eligible_at_utc=anchor.eligible_at_utc,
        extreme_price=anchor.price,
        last_member_anchor_id=anchor.anchor_id,
        last_member_eligible_at_utc=anchor.eligible_at_utc,
        last_member_decision_time_utc=anchor.decision_time_utc,
        last_member_price=anchor.price,
        member_count=1,
        consecutive_non_extremes=0,
    )


def _validate_anchor_for_identity(
    identity: MovementIdentity, anchor: NeutralPriceAnchor
) -> NeutralPriceAnchor:
    validated_identity = MovementIdentity.from_dict(identity.to_dict())
    validated_anchor = NeutralPriceAnchor.from_dict(anchor.to_dict())
    if validated_anchor.symbol != validated_identity.symbol:
        raise ValueError("anchor symbol does not match movement stream")
    if (
        validated_identity.namespace == BTC_PARENT_NAMESPACE
        and validated_anchor.symbol != "BTC"
    ):
        raise ValueError("BTC_PARENT can advance only from a BTC anchor")
    return validated_anchor


def _open_from_anchor(
    *,
    identity: MovementIdentity,
    anchor: NeutralPriceAnchor,
    previous_transition_receipt_sha256: Optional[str],
    transition_type: str,
) -> MovementAdvance:
    state = _new_state(identity, anchor)
    transition = _make_transition(
        previous_transition_receipt_sha256=previous_transition_receipt_sha256,
        transition_type=transition_type,
        trigger_anchor_id=anchor.anchor_id,
        trigger_eligible_at_utc=anchor.eligible_at_utc,
        trigger_decision_time_utc=anchor.decision_time_utc,
        pre_state_sha256=None,
        post_state=state,
    )
    membership = _make_membership(
        stream_id=identity.stream_id,
        movement_id=state.movement_id,
        anchor_id=anchor.anchor_id,
        anchor_receipt_sha256=anchor.anchor_receipt_sha256,
        ordinal=1,
        classification=START_MEMBER,
        eligible_at_utc=anchor.eligible_at_utc,
        decision_time_utc=anchor.decision_time_utc,
        price=anchor.price,
    )
    cursor = MovementCursor(
        identity=identity,
        state=state,
        last_transition_receipt_sha256=transition.transition_receipt_sha256,
    )
    return MovementAdvance(cursor, (transition,), (membership,))


def _closed_state(
    state: MovementState,
    identity: MovementIdentity,
    *,
    anchor: NeutralPriceAnchor,
    boundary: datetime,
    reason: str,
) -> MovementState:
    return _make_state(
        identity=identity,
        movement_id=state.movement_id,
        status=CLOSED_STATUS,
        direction=state.direction,
        started_anchor_id=state.started_anchor_id,
        started_eligible_at_utc=state.started_eligible_at_utc,
        started_decision_time_utc=state.started_decision_time_utc,
        start_price=state.start_price,
        extreme_anchor_id=state.extreme_anchor_id,
        extreme_eligible_at_utc=state.extreme_eligible_at_utc,
        extreme_price=state.extreme_price,
        last_member_anchor_id=state.last_member_anchor_id,
        last_member_eligible_at_utc=state.last_member_eligible_at_utc,
        last_member_decision_time_utc=state.last_member_decision_time_utc,
        last_member_price=state.last_member_price,
        member_count=state.member_count,
        consecutive_non_extremes=state.consecutive_non_extremes,
        closed_at_utc=anchor.decision_time_utc,
        close_boundary_eligible_at_utc=boundary,
        close_reason=reason,
    )


def advance_market_movement(
    cursor: Optional[MovementCursor],
    anchor: NeutralPriceAnchor,
    *,
    identity: Optional[MovementIdentity] = None,
) -> MovementAdvance:
    """Advance exactly one stream using one later frozen neutral-price anchor.

    Incremental callers must supply anchors in strictly increasing lattice
    order.  Use :func:`replay_market_movements` when input iteration order is
    not authoritative.
    """

    if cursor is None:
        selected_identity = identity or MovementIdentity.for_symbol(anchor.symbol)
        validated_anchor = _validate_anchor_for_identity(selected_identity, anchor)
        return _open_from_anchor(
            identity=selected_identity,
            anchor=validated_anchor,
            previous_transition_receipt_sha256=None,
            transition_type=OPENED,
        )
    restored = MovementCursor.from_dict(cursor.to_dict())
    selected_identity = identity or restored.identity
    if selected_identity != restored.identity:
        raise ValueError("supplied movement identity differs from cursor")
    validated_anchor = _validate_anchor_for_identity(selected_identity, anchor)
    state = restored.state
    if validated_anchor.eligible_at_utc <= state.last_member_eligible_at_utc:
        raise ValueError("incremental anchors must be strictly increasing")
    expected = state.last_member_eligible_at_utc + timedelta(
        minutes=INTERVAL_MINUTES
    )
    if validated_anchor.eligible_at_utc > expected:
        terminal = _closed_state(
            state,
            selected_identity,
            anchor=validated_anchor,
            boundary=expected,
            reason=DATA_GAP_CENSORED,
        )
        closing = _make_transition(
            previous_transition_receipt_sha256=(
                restored.last_transition_receipt_sha256
            ),
            transition_type=MOVEMENT_CLOSED,
            trigger_anchor_id=validated_anchor.anchor_id,
            trigger_eligible_at_utc=validated_anchor.eligible_at_utc,
            trigger_decision_time_utc=validated_anchor.decision_time_utc,
            pre_state_sha256=state.state_sha256,
            post_state=terminal,
        )
        opened = _open_from_anchor(
            identity=selected_identity,
            anchor=validated_anchor,
            previous_transition_receipt_sha256=(
                closing.transition_receipt_sha256
            ),
            transition_type=OPENED_AFTER_DATA_GAP,
        )
        return MovementAdvance(
            opened.cursor,
            (closing, *opened.transitions),
            opened.memberships,
        )
    if validated_anchor.eligible_at_utc != expected:
        raise ValueError("anchor is not on the next 30-minute lattice point")

    direction = state.direction
    is_new_extreme = False
    if direction == PENDING_DIRECTION:
        if validated_anchor.price > state.start_price:
            new_direction = UP_DIRECTION
            is_new_extreme = True
        elif validated_anchor.price < state.start_price:
            new_direction = DOWN_DIRECTION
            is_new_extreme = True
        else:
            new_direction = PENDING_DIRECTION
    elif direction == UP_DIRECTION:
        new_direction = UP_DIRECTION
        is_new_extreme = validated_anchor.price > state.extreme_price
    else:
        new_direction = DOWN_DIRECTION
        is_new_extreme = validated_anchor.price < state.extreme_price

    if not is_new_extreme and state.consecutive_non_extremes == 1:
        terminal = _closed_state(
            state,
            selected_identity,
            anchor=validated_anchor,
            boundary=validated_anchor.eligible_at_utc,
            reason=TWO_CONSECUTIVE_NON_EXTREMES,
        )
        closing = _make_transition(
            previous_transition_receipt_sha256=(
                restored.last_transition_receipt_sha256
            ),
            transition_type=MOVEMENT_CLOSED,
            trigger_anchor_id=validated_anchor.anchor_id,
            trigger_eligible_at_utc=validated_anchor.eligible_at_utc,
            trigger_decision_time_utc=validated_anchor.decision_time_utc,
            pre_state_sha256=state.state_sha256,
            post_state=terminal,
        )
        opened = _open_from_anchor(
            identity=selected_identity,
            anchor=validated_anchor,
            previous_transition_receipt_sha256=(
                closing.transition_receipt_sha256
            ),
            transition_type=OPENED_AFTER_DIRECTION_END,
        )
        return MovementAdvance(
            opened.cursor,
            (closing, *opened.transitions),
            opened.memberships,
        )

    if is_new_extreme:
        next_state = _make_state(
            identity=selected_identity,
            movement_id=state.movement_id,
            status=OPEN_STATUS,
            direction=new_direction,
            started_anchor_id=state.started_anchor_id,
            started_eligible_at_utc=state.started_eligible_at_utc,
            started_decision_time_utc=state.started_decision_time_utc,
            start_price=state.start_price,
            extreme_anchor_id=validated_anchor.anchor_id,
            extreme_eligible_at_utc=validated_anchor.eligible_at_utc,
            extreme_price=validated_anchor.price,
            last_member_anchor_id=validated_anchor.anchor_id,
            last_member_eligible_at_utc=validated_anchor.eligible_at_utc,
            last_member_decision_time_utc=validated_anchor.decision_time_utc,
            last_member_price=validated_anchor.price,
            member_count=state.member_count + 1,
            consecutive_non_extremes=0,
        )
        transition_type = (
            DIRECTION_ESTABLISHED
            if direction == PENDING_DIRECTION
            else EXTREME_EXTENDED
        )
        classification = (
            DIRECTIONAL_EXTREME_MEMBER
            if direction == PENDING_DIRECTION
            else EXTREME_EXTENSION_MEMBER
        )
    else:
        next_state = _make_state(
            identity=selected_identity,
            movement_id=state.movement_id,
            status=OPEN_STATUS,
            direction=state.direction,
            started_anchor_id=state.started_anchor_id,
            started_eligible_at_utc=state.started_eligible_at_utc,
            started_decision_time_utc=state.started_decision_time_utc,
            start_price=state.start_price,
            extreme_anchor_id=state.extreme_anchor_id,
            extreme_eligible_at_utc=state.extreme_eligible_at_utc,
            extreme_price=state.extreme_price,
            last_member_anchor_id=validated_anchor.anchor_id,
            last_member_eligible_at_utc=validated_anchor.eligible_at_utc,
            last_member_decision_time_utc=validated_anchor.decision_time_utc,
            last_member_price=validated_anchor.price,
            member_count=state.member_count + 1,
            consecutive_non_extremes=1,
        )
        transition_type = NON_EXTREME_OBSERVED
        classification = NON_EXTREME_MEMBER
    transition = _make_transition(
        previous_transition_receipt_sha256=(
            restored.last_transition_receipt_sha256
        ),
        transition_type=transition_type,
        trigger_anchor_id=validated_anchor.anchor_id,
        trigger_eligible_at_utc=validated_anchor.eligible_at_utc,
        trigger_decision_time_utc=validated_anchor.decision_time_utc,
        pre_state_sha256=state.state_sha256,
        post_state=next_state,
    )
    membership = _make_membership(
        stream_id=selected_identity.stream_id,
        movement_id=state.movement_id,
        anchor_id=validated_anchor.anchor_id,
        anchor_receipt_sha256=validated_anchor.anchor_receipt_sha256,
        ordinal=next_state.member_count,
        classification=classification,
        eligible_at_utc=validated_anchor.eligible_at_utc,
        decision_time_utc=validated_anchor.decision_time_utc,
        price=validated_anchor.price,
    )
    return MovementAdvance(
        cursor=MovementCursor(
            identity=selected_identity,
            state=next_state,
            last_transition_receipt_sha256=(
                transition.transition_receipt_sha256
            ),
        ),
        transitions=(transition,),
        memberships=(membership,),
    )


def replay_market_movements(
    anchors: Sequence[NeutralPriceAnchor],
    *,
    identity: Optional[MovementIdentity] = None,
) -> MovementHistory:
    """Canonical-sort, conflict-check, and replay one neutral-price stream.

    Exact duplicate receipts are idempotently collapsed.  Two different
    receipts for the same stream/eligibility slot fail closed.  Therefore the
    result is independent of caller iteration order without accepting a fork.
    """

    supplied = list(anchors)
    if not supplied:
        if identity is None:
            raise ValueError("identity is required when replay has no anchors")
        selected_identity = MovementIdentity.from_dict(identity.to_dict())
        return MovementHistory(selected_identity, None, (), ())
    selected_identity = identity or MovementIdentity.for_symbol(supplied[0].symbol)
    selected_identity = MovementIdentity.from_dict(selected_identity.to_dict())
    validated = [
        _validate_anchor_for_identity(selected_identity, anchor)
        for anchor in supplied
    ]
    ordered = sorted(
        validated,
        key=lambda item: (
            item.eligible_at_utc,
            item.anchor_receipt_sha256,
        ),
    )
    unique: list[NeutralPriceAnchor] = []
    by_slot: dict[datetime, NeutralPriceAnchor] = {}
    for anchor in ordered:
        prior = by_slot.get(anchor.eligible_at_utc)
        if prior is None:
            by_slot[anchor.eligible_at_utc] = anchor
            unique.append(anchor)
        elif prior.anchor_receipt_sha256 != anchor.anchor_receipt_sha256:
            raise ValueError("conflicting neutral-price anchors for one eligibility slot")
    cursor: Optional[MovementCursor] = None
    transitions: list[MovementTransition] = []
    memberships: list[MovementMembership] = []
    for anchor in unique:
        advanced = advance_market_movement(
            cursor, anchor, identity=selected_identity
        )
        cursor = advanced.cursor
        transitions.extend(advanced.transitions)
        memberships.extend(advanced.memberships)
    return MovementHistory(
        identity=selected_identity,
        cursor=cursor,
        transitions=tuple(transitions),
        memberships=tuple(memberships),
    )


# Compact aliases for store/replay callers; the verbose names remain the
# canonical public API and make accidental use of Market Episode v4 unlikely.
advance = advance_market_movement
replay = replay_market_movements
