"""Pure decision-time feature freezing for prospective Formula samples.

The caller supplies feature-matrix rows that were prepared at the actual
decision timestamp.  This module performs no database or network reads.  It
keeps only Formula-visible decision evidence, removes alert-sequence/model
features unless a separate model wrapper was explicitly frozen, and stores
the prior-only session/width context without copying any future outcome.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import math
import re
from typing import Any, Dict, Mapping, Optional, Sequence

import research_formula_engine
import research_session_width


BUNDLE_SCHEMA_VERSION = "prospective-decision-feature-bundle-schema-v1"
FEATURE_POLICY_VERSION = "prospective-decision-feature-bundle-v1"
REQUIRED_DIRECTIONS: tuple[str, ...] = ("LONG", "SHORT")
REQUIRED_HORIZONS: tuple[int, ...] = (60, 240, 720, 1440)
ALLOWED_SOURCE_SAMPLER_VERSIONS = {
    "prospective-neutral-anchor-v3-max-pain-frozen",
    "prospective-neutral-anchor-v4-decision-features-frozen",
}

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FORBIDDEN_FEATURE_PREFIXES = (
    "sequence.",
    "aligned_sequence.",
)
_MODEL_FEATURE_PREFIXES = (
    "model.",
)
_WINDOW_NAMES = {"30m", "60m", "240m", "720m", "1440m"}
_EVENT_FEATURES = {
    "event.symbol",
    "event.event_type",
    "event.source_side",
    "event.timeframe",
}
_TIME_FEATURES = {
    "time.is_market_weekend",
    "time.market_session",
    "time.market_regime",
    "time.market_session_timezone",
    "time.market_session_definition",
    "time.market_local_hour",
    "time.market_local_minute",
    "time.market_local_weekday",
    "time.market_local_weekday_name",
    "time.market_time_bucket",
}
_RAW_WINDOW_FEATURES = {
    "session_active_ratio",
    "session_weekend_ratio",
    "session_composition",
    "price_change_pct",
    "oi_change_pct",
    "futures_continuous_cvd_change_usd",
    "spot_continuous_cvd_change_usd",
    "futures_api_cvd_change_usd",
    "spot_api_cvd_change_usd",
    "spot_to_futures_abs_cvd_ratio",
    "price_oi_state",
    "spot_futures_alignment",
    "price_spot_alignment",
    "price_futures_alignment",
}
_ALIGNED_WINDOW_FEATURES = {
    "price_change_pct",
    "futures_continuous_cvd_change_usd",
    "spot_continuous_cvd_change_usd",
    "futures_api_cvd_change_usd",
    "spot_api_cvd_change_usd",
}
_HISTORICAL_BASE_FEATURES = {
    "price_change_pct",
    "oi_change_pct",
    "futures_continuous_cvd_change_usd",
    "spot_continuous_cvd_change_usd",
}
_HISTORICAL_STAT_SUFFIXES = {
    "percentile_session_matched",
    "abs_percentile_session_matched",
    "median_session_matched",
    "abs_median_session_matched",
}
_MAX_PAIN_TIMEFRAMES = {"12h", "24h", "48h", "3d", "1w", "2w", "1m"}
_MAX_PAIN_TIMEFRAME_FIELDS = {
    "short_target_signed_distance_pct",
    "long_target_signed_distance_pct",
    "upside_active_distance_pct",
    "downside_active_distance_pct",
    "upside_liquidity_usd",
    "downside_liquidity_usd",
    "short_liquidity_usd",
    "long_liquidity_usd",
    "upside_downside_liquidity_ratio",
    "short_long_liquidity_ratio",
    "liquidity_imbalance_pct",
    "closer_active_direction",
}
_MAX_PAIN_AGGREGATE_FIELDS = {
    "upside_active_timeframe_count",
    "downside_active_timeframe_count",
    "closer_upside_count",
    "closer_downside_count",
    "consensus_direction",
    "consensus_count",
    "consensus_ratio",
    "upside_liquidity_usd",
    "downside_liquidity_usd",
    "short_liquidity_usd",
    "long_liquidity_usd",
    "upside_downside_liquidity_ratio",
    "short_long_liquidity_ratio",
    "liquidity_imbalance_pct",
    "median_upside_active_distance_pct",
    "median_downside_active_distance_pct",
    "upside_cluster_count_1pct",
    "upside_cluster_spread_pct",
    "upside_all_target_spread_pct",
    "downside_cluster_count_1pct",
    "downside_cluster_spread_pct",
    "downside_all_target_spread_pct",
}
_MAX_PAIN_DELTA_AGGREGATES = {
    "upside_liquidity_usd",
    "downside_liquidity_usd",
    "liquidity_imbalance_pct",
    "closer_upside_count",
    "closer_downside_count",
    "upside_cluster_count_1pct",
    "downside_cluster_count_1pct",
    "upside_cluster_spread_pct",
    "downside_cluster_spread_pct",
}
_MAX_PAIN_DELTA_TIMEFRAME_FIELDS = {
    "upside_liquidity_usd",
    "downside_liquidity_usd",
    "upside_active_distance_pct",
    "downside_active_distance_pct",
    "short_target_signed_distance_pct",
    "long_target_signed_distance_pct",
}
_FORBIDDEN_BUNDLE_KEYS = {
    "outcome_label",
    "mfe",
    "mfe_pct",
    "mae",
    "mae_pct",
    "full_horizon_mae_pct",
    "path_success",
    "first_touch_status",
    "price_at_horizon",
    "raw_return_pct",
    "directional_return_pct",
    "target_reached",
    "time_to_mfe_seconds",
    "time_to_target_seconds",
    "time_to_first_progress_seconds",
}


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip().replace("Z", "+00:00")
        if not text:
            raise ValueError("timestamp is required")
        parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: Any) -> str:
    return _utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _symbol(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    if (
        not normalized
        or len(normalized) > 20
        or not normalized.replace("-", "").isalnum()
    ):
        raise ValueError(f"invalid prospective feature symbol: {value!r}")
    return normalized


def _json_value(value: Any, *, path: str = "bundle") -> Any:
    """Return a deterministic JSON value or reject lossy coercion."""
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, datetime):
        return _iso(value)
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} contains a non-finite number")
        # PostgreSQL JSONB normalizes negative zero.  Canonicalize it before
        # hashing so the slot digest survives a database round trip exactly.
        return 0.0 if value == 0.0 else value
    if isinstance(value, Mapping):
        result: Dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise ValueError(f"{path} contains a non-string/empty key")
            result[key] = _json_value(item, path=f"{path}.{key}")
        return result
    if isinstance(value, (list, tuple)):
        return [
            _json_value(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    raise ValueError(f"{path} contains a non-JSON value")


def _canonical(value: Any) -> str:
    return json.dumps(
        _json_value(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def canonicalize_feature_bundle(bundle: Mapping[str, Any]) -> Dict[str, Any]:
    """Return the detached JSON representation used by hashing/storage."""
    if not isinstance(bundle, Mapping):
        raise ValueError("decision feature bundle must be an object")
    normalized = json.loads(_canonical(bundle))
    if not isinstance(normalized, dict):
        raise ValueError("decision feature bundle must be an object")
    return normalized


def compute_feature_bundle_sha256(bundle: Mapping[str, Any]) -> str:
    """Return the canonical SHA256 for one decision feature bundle."""
    if not isinstance(bundle, Mapping):
        raise ValueError("decision feature bundle must be an object")
    return hashlib.sha256(_canonical(bundle).encode("utf-8")).hexdigest()


def _flat_scalar_features(value: Any, *, path: str) -> Dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{path} must be an object")
    result: Dict[str, Any] = {}
    for raw_name, raw_value in value.items():
        name = str(raw_name or "").strip()
        if not name or name != raw_name:
            raise ValueError(f"{path} has an invalid feature name")
        name_tokens = {
            token
            for token in re.split(r"[^a-z0-9]+", name.lower())
            if token
        }
        if name_tokens.intersection({"mfe", "mae", "outcome", "future"}):
            raise ValueError(f"{path}.{name} is a forbidden outcome feature")
        value = _json_value(raw_value, path=f"{path}.{name}")
        if not isinstance(value, (bool, int, float, str)):
            raise ValueError(f"{path}.{name} must be a flat scalar")
        result[name] = value
    return result


def _max_pain_feature_allowed(name: str) -> bool:
    parts = name.split(".")
    if len(parts) == 3 and parts[:2] == ["max_pain", "aggregate"]:
        return parts[2] in _MAX_PAIN_AGGREGATE_FIELDS
    if parts == ["max_pain", "delta", "minutes_since_previous_snapshot"]:
        return True
    if len(parts) == 3 and parts[:2] == ["max_pain", "delta"]:
        field = parts[2]
        for ending in ("_change_pct", "_change", "_trend"):
            if field.endswith(ending):
                return field[: -len(ending)] in _MAX_PAIN_DELTA_AGGREGATES
        return False
    if len(parts) == 4 and parts[:2] == ["max_pain", "delta"]:
        field = parts[3]
        for ending in ("_change", "_trend"):
            if field.endswith(ending):
                return (
                    parts[2] in _MAX_PAIN_TIMEFRAMES
                    and field[: -len(ending)]
                    in _MAX_PAIN_DELTA_TIMEFRAME_FIELDS
                )
        return False
    if len(parts) == 3 and parts[0] == "max_pain":
        return (
            parts[1] in _MAX_PAIN_TIMEFRAMES
            and parts[2] in _MAX_PAIN_TIMEFRAME_FIELDS
        )
    return False


def _canonical_candidate_feature_allowed(name: str) -> bool:
    """Exact v1 namespace generated by Formula's decision extractor."""
    if name in _EVENT_FEATURES or name in _TIME_FEATURES:
        return True
    if name == "historical.event_market_session":
        return True
    if name.startswith("latest."):
        parts = name.split(".")
        return (
            len(parts) == 3
            and parts[1] in {"price_oi", "futures_cvd", "spot_cvd"}
            and parts[2] == "buy_sell_ratio"
        )
    if name.startswith("raw."):
        parts = name.split(".")
        return (
            len(parts) == 3
            and parts[1] in _WINDOW_NAMES
            and parts[2] in _RAW_WINDOW_FEATURES
        )
    if name.startswith(("aligned.", "aligned_log.")):
        parts = name.split(".")
        return (
            len(parts) == 3
            and parts[1] in _WINDOW_NAMES
            and parts[2] in _ALIGNED_WINDOW_FEATURES
        )
    if name.startswith("historical."):
        parts = name.split(".")
        if len(parts) != 3 or parts[1] not in _WINDOW_NAMES:
            return False
        if parts[2] in {
            "session_active_ratio",
            "session_weekend_ratio",
            "session_composition",
        }:
            return True
        return any(
            parts[2] == f"{base}_{suffix}"
            for base in _HISTORICAL_BASE_FEATURES
            for suffix in _HISTORICAL_STAT_SUFFIXES
        )
    if name.startswith("max_pain."):
        return _max_pain_feature_allowed(name)
    return False


def _model_wrapper(
    value: Any,
    *,
    symbol: str,
    decision_time: datetime,
) -> tuple[str, Optional[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    if value is None:
        return "ABSENT", None, {direction: {} for direction in REQUIRED_DIRECTIONS}
    # Sampler v4 currently has no provenance-bound persisted model-score
    # source.  A timestamp and source label supplied by a caller are not proof
    # that a score existed at the decision.  Keep the bundle explicitly ABSENT
    # until a later policy can bind record id, payload hash and archive lookup.
    raise ValueError(
        "frozen_model_wrapper is disabled without authoritative persisted-score provenance"
    )


def _horizon_context(row: Mapping[str, Any], *, horizon: int) -> Dict[str, Any]:
    label = row.get("outcome_label")
    if not isinstance(label, Mapping):
        raise ValueError(f"{horizon}m feature row lacks horizon context")
    if type(label.get("horizon_minutes")) is not int or int(
        label["horizon_minutes"]
    ) != horizon:
        raise ValueError(f"{horizon}m feature row horizon mismatch")
    active = label.get("session_active_ratio")
    weekend = label.get("session_weekend_ratio")
    if isinstance(active, bool) or not isinstance(active, (int, float)):
        raise ValueError(f"{horizon}m active session ratio is invalid")
    if isinstance(weekend, bool) or not isinstance(weekend, (int, float)):
        raise ValueError(f"{horizon}m weekend session ratio is invalid")
    active_number = float(active)
    weekend_number = float(weekend)
    if (
        not math.isfinite(active_number)
        or not math.isfinite(weekend_number)
        or not 0.0 <= active_number <= 1.0
        or not 0.0 <= weekend_number <= 1.0
        or not math.isclose(
            active_number + weekend_number,
            1.0,
            rel_tol=0.0,
            abs_tol=1e-6,
        )
    ):
        raise ValueError(f"{horizon}m session ratios are incoherent")
    composition = str(label.get("session_composition") or "").strip().upper()
    if composition not in {"ACTIVE_ONLY", "WEEKEND_ONLY", "MIXED"}:
        raise ValueError(f"{horizon}m session composition is invalid")
    expected_composition = (
        "ACTIVE_ONLY"
        if active_number >= 1.0 - 1e-9
        else "WEEKEND_ONLY"
        if active_number <= 1e-9
        else "MIXED"
    )
    if composition != expected_composition:
        raise ValueError(f"{horizon}m session composition is incoherent")
    segments = label.get("session_segments")
    if type(segments) is not int or segments < 0:
        raise ValueError(f"{horizon}m session segment count is invalid")
    width = label.get("movement_width_reference")
    if not isinstance(width, Mapping):
        raise ValueError(f"{horizon}m movement-width reference is missing")
    normalized_width = deepcopy(width)
    if type(normalized_width.get("horizon_minutes")) is not int or int(
        normalized_width["horizon_minutes"]
    ) != horizon:
        raise ValueError(f"{horizon}m movement-width horizon mismatch")
    normalized_width["symbol"] = _symbol(normalized_width.get("symbol"))
    normalized_width["as_of_utc"] = _iso(normalized_width.get("as_of_utc"))
    return _json_value(
        {
            "session": {
                "active_ratio": round(active_number, 6),
                "weekend_ratio": round(weekend_number, 6),
                "composition": composition,
                "segments": segments,
            },
            "movement_width_reference": normalized_width,
        },
        path=f"horizon_context.{horizon}",
    )


def _safe_decision_features(
    row: Mapping[str, Any],
    *,
    model_features: Mapping[str, Any],
) -> Dict[str, Any]:
    extracted = research_formula_engine.extract_decision_features(row)
    safe = {
        name: value
        for name, value in extracted.items()
        if not name.startswith(_FORBIDDEN_FEATURE_PREFIXES)
        and not name.startswith(_MODEL_FEATURE_PREFIXES)
        and research_formula_engine.candidate_feature_allowed(name)
        and _canonical_candidate_feature_allowed(name)
    }
    safe.update(model_features)
    return _flat_scalar_features(safe, path="features_by_direction")


def _source_series_manifest(
    value: Any,
    *,
    decision_time: datetime,
) -> Dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        "count",
        "first_decision_time_utc",
        "last_decision_time_utc",
        "sha256",
        "sampler_versions",
    }:
        raise ValueError("source_series_manifest has incompatible fields")
    count = value.get("count")
    if type(count) is not int or count < 0:
        raise ValueError("source_series_manifest count is invalid")
    digest = str(value.get("sha256") or "").strip().lower()
    if not _SHA256_RE.fullmatch(digest):
        raise ValueError("source_series_manifest sha256 is invalid")
    versions = value.get("sampler_versions")
    if not isinstance(versions, list) or any(
        not isinstance(item, str) for item in versions
    ):
        raise ValueError("source_series_manifest sampler_versions is invalid")
    normalized_versions = sorted(set(versions))
    if len(normalized_versions) != len(versions) or not set(
        normalized_versions
    ).issubset(ALLOWED_SOURCE_SAMPLER_VERSIONS):
        raise ValueError("source_series_manifest sampler_versions is incompatible")
    first_value = value.get("first_decision_time_utc")
    last_value = value.get("last_decision_time_utc")
    if count == 0:
        if first_value is not None or last_value is not None or normalized_versions:
            raise ValueError("empty source_series_manifest must have null bounds")
        first = last = None
    else:
        if first_value in (None, "") or last_value in (None, ""):
            raise ValueError("source_series_manifest bounds are required")
        first_time = _utc(first_value)
        last_time = _utc(last_value)
        if first_time > last_time or last_time > decision_time:
            raise ValueError("source_series_manifest bounds are incoherent")
        if not normalized_versions:
            raise ValueError("source_series_manifest sampler_versions are required")
        first = _iso(first_time)
        last = _iso(last_time)
    return {
        "count": count,
        "first_decision_time_utc": first,
        "last_decision_time_utc": last,
        "sha256": digest,
        "sampler_versions": normalized_versions,
    }


def build_feature_bundle(
    *,
    decision_time_utc: Any,
    symbol: Any,
    feature_rows: Sequence[Mapping[str, Any]],
    source_series_manifest: Mapping[str, Any],
    frozen_model_wrapper: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Build one canonical, outcome-free bundle from prepared feature rows.

    Exactly one row per direction/horizon is required.  Decision features for
    a direction must be identical across horizons; horizon-dependent session
    and width context is stored once in ``horizon_context``.
    """
    normalized_symbol = _symbol(symbol)
    decision_time = _utc(decision_time_utc)
    normalized_manifest = _source_series_manifest(
        deepcopy(source_series_manifest),
        decision_time=decision_time,
    )
    model_status, model_wrapper, model_by_direction = _model_wrapper(
        frozen_model_wrapper,
        symbol=normalized_symbol,
        decision_time=decision_time,
    )
    rows_by_key: Dict[tuple[str, int], Mapping[str, Any]] = {}
    feature_schema_versions: set[str] = set()
    for raw_row in feature_rows:
        if not isinstance(raw_row, Mapping):
            raise ValueError("feature_rows must contain objects")
        row = raw_row
        event = row.get("event")
        if not isinstance(event, Mapping):
            raise ValueError("feature row event is missing")
        if _symbol(event.get("symbol")) != normalized_symbol:
            raise ValueError("feature row symbol mismatch")
        if _utc(event.get("alert_time_utc")) != decision_time:
            raise ValueError("feature row decision timestamp mismatch")
        direction = str(event.get("direction") or "").strip().upper()
        if direction not in REQUIRED_DIRECTIONS:
            raise ValueError("feature row direction is invalid")
        label = row.get("outcome_label")
        if not isinstance(label, Mapping) or type(label.get("horizon_minutes")) is not int:
            raise ValueError("feature row horizon is missing")
        horizon = int(label["horizon_minutes"])
        if horizon not in REQUIRED_HORIZONS:
            raise ValueError("feature row horizon is unsupported")
        key = (direction, horizon)
        if key in rows_by_key:
            raise ValueError("duplicate direction/horizon feature row")
        rows_by_key[key] = row
        schema_version = str(row.get("feature_schema_version") or "").strip()
        if not schema_version:
            raise ValueError("feature_schema_version is required")
        feature_schema_versions.add(schema_version)
    expected_keys = {
        (direction, horizon)
        for direction in REQUIRED_DIRECTIONS
        for horizon in REQUIRED_HORIZONS
    }
    if set(rows_by_key) != expected_keys:
        raise ValueError("feature_rows require LONG/SHORT rows for all horizons")
    if len(feature_schema_versions) != 1:
        raise ValueError("feature rows use different schema versions")

    features_by_direction: Dict[str, Dict[str, Any]] = {}
    for direction in REQUIRED_DIRECTIONS:
        per_horizon = [
            _safe_decision_features(
                rows_by_key[(direction, horizon)],
                model_features=model_by_direction[direction],
            )
            for horizon in REQUIRED_HORIZONS
        ]
        canonical_first = _canonical(per_horizon[0])
        if any(_canonical(features) != canonical_first for features in per_horizon[1:]):
            raise ValueError(
                f"{direction} decision features differ between horizon rows"
            )
        features_by_direction[direction] = per_horizon[0]

    horizon_context: Dict[str, Dict[str, Any]] = {}
    for horizon in REQUIRED_HORIZONS:
        long_context = _horizon_context(
            rows_by_key[("LONG", horizon)], horizon=horizon
        )
        short_context = _horizon_context(
            rows_by_key[("SHORT", horizon)], horizon=horizon
        )
        if _canonical(long_context) != _canonical(short_context):
            raise ValueError(f"{horizon}m context differs by direction")
        horizon_context[str(horizon)] = long_context

    bundle: Dict[str, Any] = {
        "bundle_schema_version": BUNDLE_SCHEMA_VERSION,
        "feature_policy_version": FEATURE_POLICY_VERSION,
        "feature_schema_version": next(iter(feature_schema_versions)),
        "decision_time_utc": _iso(decision_time),
        "symbol": normalized_symbol,
        "source_series_manifest": normalized_manifest,
        "features_by_direction": features_by_direction,
        "horizon_context": horizon_context,
        "model_score_status": model_status,
    }
    if model_wrapper is not None:
        bundle["frozen_model_wrapper"] = model_wrapper
    valid, reason = validate_feature_bundle(
        bundle,
        expected_symbol=normalized_symbol,
        expected_decision_time_utc=decision_time,
    )
    if not valid:
        raise ValueError(reason)
    return canonicalize_feature_bundle(bundle)


def _contains_forbidden_key(value: Any) -> bool:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = str(key or "").strip().lower()
            if normalized in _FORBIDDEN_BUNDLE_KEYS:
                return True
            if _contains_forbidden_key(item):
                return True
    elif isinstance(value, (list, tuple)):
        return any(_contains_forbidden_key(item) for item in value)
    return False


def validate_feature_bundle(
    bundle: Any,
    *,
    expected_sha256: Optional[Any] = None,
    expected_symbol: Optional[Any] = None,
    expected_decision_time_utc: Optional[Any] = None,
) -> tuple[bool, str]:
    """Strictly validate shape, provenance boundary and canonical hash."""
    try:
        if not isinstance(bundle, Mapping):
            return False, "decision feature bundle must be an object"
        normalized = _json_value(bundle)
        required_keys = {
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
        status = normalized.get("model_score_status")
        allowed_keys = set(required_keys)
        if status == "FROZEN":
            allowed_keys.add("frozen_model_wrapper")
        if set(normalized) != allowed_keys or not required_keys.issubset(normalized):
            return False, "decision feature bundle fields are incompatible"
        if normalized["bundle_schema_version"] != BUNDLE_SCHEMA_VERSION:
            return False, "decision feature bundle schema is incompatible"
        if normalized["feature_policy_version"] != FEATURE_POLICY_VERSION:
            return False, "decision feature policy is incompatible"
        if not str(normalized["feature_schema_version"] or "").strip():
            return False, "feature_schema_version is required"
        symbol = _symbol(normalized["symbol"])
        decision_time = _utc(normalized["decision_time_utc"])
        if normalized["decision_time_utc"] != _iso(decision_time):
            return False, "decision feature timestamp is not canonical"
        if expected_symbol is not None and symbol != _symbol(expected_symbol):
            return False, "decision feature symbol mismatch"
        if (
            expected_decision_time_utc is not None
            and decision_time != _utc(expected_decision_time_utc)
        ):
            return False, "decision feature timestamp mismatch"
        if _contains_forbidden_key(normalized):
            return False, "future outcome field is forbidden in decision bundle"
        manifest = _source_series_manifest(
            normalized.get("source_series_manifest"),
            decision_time=decision_time,
        )
        if _canonical(manifest) != _canonical(
            normalized.get("source_series_manifest")
        ):
            return False, "source_series_manifest is not canonical"

        raw_features = normalized.get("features_by_direction")
        if not isinstance(raw_features, Mapping) or set(raw_features) != set(
            REQUIRED_DIRECTIONS
        ):
            return False, "decision features require LONG and SHORT maps"
        for direction in REQUIRED_DIRECTIONS:
            features = _flat_scalar_features(
                raw_features[direction],
                path=f"features_by_direction.{direction}",
            )
            if any(name.startswith(_FORBIDDEN_FEATURE_PREFIXES) for name in features):
                return False, "alert-sequence features are forbidden"
            if any(
                not name.startswith(_MODEL_FEATURE_PREFIXES)
                and (
                    not research_formula_engine.candidate_feature_allowed(name)
                    or not _canonical_candidate_feature_allowed(name)
                )
                for name in features
            ):
                return False, "non-candidate decision features are forbidden"
            has_model = any(name.startswith(_MODEL_FEATURE_PREFIXES) for name in features)
            if (status == "ABSENT" and has_model) or (status == "FROZEN" and not has_model):
                return False, "model feature status is inconsistent"

        raw_context = normalized.get("horizon_context")
        if not isinstance(raw_context, Mapping) or set(raw_context) != {
            str(horizon) for horizon in REQUIRED_HORIZONS
        }:
            return False, "horizon context is incomplete"
        for horizon in REQUIRED_HORIZONS:
            context = raw_context[str(horizon)]
            if not isinstance(context, Mapping) or set(context) != {
                "session",
                "movement_width_reference",
            }:
                return False, f"{horizon}m horizon context is incompatible"
            session = context.get("session")
            if not isinstance(session, Mapping) or set(session) != {
                "active_ratio",
                "weekend_ratio",
                "composition",
                "segments",
            }:
                return False, f"{horizon}m session context is incompatible"
            active = session.get("active_ratio")
            weekend = session.get("weekend_ratio")
            if (
                isinstance(active, bool)
                or not isinstance(active, (int, float))
                or isinstance(weekend, bool)
                or not isinstance(weekend, (int, float))
                or not math.isfinite(float(active))
                or not math.isfinite(float(weekend))
                or not math.isclose(
                    float(active) + float(weekend),
                    1.0,
                    rel_tol=0.0,
                    abs_tol=1e-6,
                )
            ):
                return False, f"{horizon}m session ratios are incoherent"
            if type(session.get("segments")) is not int or int(
                session["segments"]
            ) < 0:
                return False, f"{horizon}m session segment count is invalid"
            expected_composition = (
                "ACTIVE_ONLY"
                if float(active) >= 1.0 - 1e-9
                else "WEEKEND_ONLY"
                if float(active) <= 1e-9
                else "MIXED"
            )
            if session.get("composition") != expected_composition:
                return False, f"{horizon}m session composition is incoherent"
            width = context.get("movement_width_reference")
            if not isinstance(width, Mapping):
                return False, f"{horizon}m movement-width reference is missing"
            if type(width.get("horizon_minutes")) is not int or int(
                width["horizon_minutes"]
            ) != horizon:
                return False, f"{horizon}m movement-width horizon mismatch"
            if _symbol(width.get("symbol")) != symbol:
                return False, f"{horizon}m movement-width symbol mismatch"
            as_of = _utc(width.get("as_of_utc"))
            if as_of > decision_time:
                return False, f"{horizon}m movement-width evidence is future"
            if width.get("as_of_utc") != _iso(as_of):
                return False, f"{horizon}m movement-width timestamp is not canonical"
            width_valid, width_reason = (
                research_session_width.validate_movement_width_reference(
                    width,
                    expected_symbol=symbol,
                    event_time=decision_time,
                    horizon_minutes=horizon,
                )
            )
            if not width_valid:
                return False, f"{horizon}m {width_reason}"
            if (
                not math.isclose(
                    float(session["active_ratio"]),
                    float(width["session_active_ratio"]),
                    rel_tol=0.0,
                    abs_tol=1e-6,
                )
                or not math.isclose(
                    float(session["weekend_ratio"]),
                    float(width["session_weekend_ratio"]),
                    rel_tol=0.0,
                    abs_tol=1e-6,
                )
                or session["composition"] != width["session_composition"]
                or int(session["segments"]) != int(width["session_segments"])
            ):
                return False, f"{horizon}m session and width context differ"

        if status == "ABSENT":
            if "frozen_model_wrapper" in normalized:
                return False, "absent model status cannot carry a model wrapper"
        elif status == "FROZEN":
            _status, wrapper, model_by_direction = _model_wrapper(
                normalized.get("frozen_model_wrapper"),
                symbol=symbol,
                decision_time=decision_time,
            )
            if wrapper is None:
                return False, "frozen model wrapper is missing"
            for direction in REQUIRED_DIRECTIONS:
                actual = {
                    name: value
                    for name, value in raw_features[direction].items()
                    if name.startswith(_MODEL_FEATURE_PREFIXES)
                }
                if _canonical(actual) != _canonical(model_by_direction[direction]):
                    return False, "frozen model features do not match their wrapper"
        else:
            return False, "model_score_status is incompatible"

        if expected_sha256 is not None:
            expected_hash = str(expected_sha256 or "").strip().lower()
            if not _SHA256_RE.fullmatch(expected_hash):
                return False, "decision feature bundle hash is invalid"
            if compute_feature_bundle_sha256(normalized) != expected_hash:
                return False, "decision feature bundle hash mismatch"
        return True, "decision feature bundle is coherent"
    except (TypeError, ValueError, OverflowError):
        return False, "decision feature bundle is malformed"


# Concise aliases for callers that treat this module as a canonical codec.
feature_bundle_sha256 = compute_feature_bundle_sha256
