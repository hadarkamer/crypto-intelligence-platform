"""Append-only coherent Max-Pain Watch archive and prior-only features.

This module intentionally never reads ``max_pain_snapshots``.  That legacy
table contains observations produced by an older calculation method and is not
eligible for Formula Discovery.  Only migration-007 snapshot sets written by
this module can be loaded.

Database writes are explicit, additive and fail-open at the caller.  Feature
lookups select a snapshot only when it was fully available at or before the
decision timestamp.  Missing, stale or incomplete evidence is UNEVALUABLE.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import math
import os
from statistics import median
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg.types.json import Jsonb
except Exception:  # pragma: no cover - validated in production
    psycopg = None
    dict_row = None
    Jsonb = None


ARCHIVE_SCHEMA_VERSION = "research-max-pain-archive-v1"
METHOD_VERSION = "coherent-max-pain-seven-timeframe-v1"
CUTOVER_MARKER = "POST_LEGACY_METHOD_2026_08_29"
CUTOVER_TIME_UTC = datetime(2026, 8, 29, 0, 0, tzinfo=timezone.utc)
OFFICIAL_PRICE_POLICY_VERSION = (
    "binance-spot-usdt-1m__hype-hyperliquid-spot-at107-1m-v2"
)
REQUIRED_TIMEFRAMES: tuple[str, ...] = (
    "12h",
    "24h",
    "48h",
    "3d",
    "1w",
    "2w",
    "1m",
)
DEFAULT_MAX_CAPTURE_SOURCE_AGE_MINUTES = 45
DEFAULT_MAX_DECISION_AGE_MINUTES = 45
DEFAULT_MAX_PREVIOUS_GAP_MINUTES = 90
SHADOW_PROVENANCE_POLICY_VERSION = "max-pain-shadow-provenance-v1"
_TRUE = {"1", "true", "yes", "on"}


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


def _iso_or_none(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    try:
        return _iso(value)
    except (TypeError, ValueError):
        return None


def _float(value: Any) -> Optional[float]:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _int(value: Any) -> Optional[int]:
    number = _float(value)
    return int(number) if number is not None and number.is_integer() else None


def _positive_float(value: Any) -> Optional[float]:
    number = _float(value)
    return number if number is not None and number > 0 else None


def _nonnegative_float(value: Any) -> Optional[float]:
    number = _float(value)
    return number if number is not None and number >= 0 else None


def _positive_int(value: Any) -> Optional[int]:
    number = _int(value)
    return number if number is not None and number > 0 else None


def _strict_positive_json_int(value: Any) -> Optional[int]:
    """Accept only a positive JSON integer, never a coercible scalar.

    PostgreSQL integer columns arrive here as Python ``int`` values.  Persisted
    JSON provenance, however, is an untyped trust boundary: strings, floats and
    booleans must not be allowed to impersonate archive identifiers or integer
    policy counts merely because ``int()``/``float()`` can coerce them.
    """
    return value if type(value) is int and value > 0 else None


def _strict_positive_json_number(value: Any) -> Optional[float]:
    """Accept only a finite positive JSON number, never a coercible scalar."""
    if type(value) not in (int, float):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def _round(value: Any, digits: int = 8) -> Optional[float]:
    number = _float(value)
    return round(number, digits) if number is not None else None


def _round_positive(value: Any, digits: int = 8) -> Optional[float]:
    number = _positive_float(value)
    if number is None:
        return None
    rounded = round(number, digits)
    # Preserve sub-rounding positive values instead of converting a valid
    # scalar into 0.0, which the archive's typed price constraints reject.
    return rounded if rounded > 0 else number


def _json_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str, ensure_ascii=False))


def _audit_scalar(value: Any) -> Any:
    safe = _json_safe(value)
    if isinstance(safe, float) and not math.isfinite(safe):
        return str(safe)
    return safe


def _canonical(value: Any) -> str:
    return json.dumps(
        _json_safe(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def canonical_provenance_sha256(value: Any) -> str:
    """Return the stable hash used to bind Max-Pain evidence to approval.

    This intentionally hashes only compact archive identities and never turns
    an identifier or payload hash into a Formula candidate feature.
    """
    payload = json.dumps(
        _json_safe(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _compact_snapshot_provenance(
    set_record: Optional[Mapping[str, Any]],
    symbol_manifest: Optional[Mapping[str, Any]],
    rows: Sequence[Mapping[str, Any]],
    *,
    symbol: str,
) -> Optional[Dict[str, Any]]:
    if not isinstance(set_record, Mapping):
        return None
    manifest = dict(symbol_manifest or {})
    rows_by_timeframe = {
        str(row.get("timeframe") or ""): dict(row)
        for row in rows
        if str(row.get("symbol") or "").upper() == symbol
    }
    return {
        "snapshot_set_id": _positive_int(set_record.get("snapshot_set_id")),
        "snapshot_key": str(set_record.get("snapshot_key") or "").strip() or None,
        "set_payload_sha256": (
            str(set_record.get("payload_sha256") or "").strip() or None
        ),
        "symbol": symbol,
        "symbol_manifest_payload_sha256": (
            str(manifest.get("payload_sha256") or "").strip() or None
        ),
        "row_payload_sha256": [
            {
                "timeframe": timeframe,
                "payload_sha256": (
                    str(
                        rows_by_timeframe.get(timeframe, {}).get("payload_sha256")
                        or ""
                    ).strip()
                    or None
                ),
            }
            for timeframe in REQUIRED_TIMEFRAMES
        ],
        "archive_schema_version": set_record.get("archive_schema_version"),
        "method_version": set_record.get("method_version"),
        "cutover_marker": set_record.get("cutover_marker"),
        "cutover_time_utc": _iso_or_none(set_record.get("cutover_time_utc")),
        "available_at_utc": _iso_or_none(set_record.get("available_at_utc")),
        "created_at_utc": _iso_or_none(set_record.get("created_at_utc")),
        "cycle_id": set_record.get("cycle_id"),
        "cycle_time_utc": _iso_or_none(set_record.get("cycle_time_utc")),
        "source": set_record.get("source"),
        "collector_version": set_record.get("collector_version"),
    }


def _provenance_bundle(
    *,
    symbol: str,
    current_set: Optional[Mapping[str, Any]],
    current_symbol_manifest: Optional[Mapping[str, Any]],
    current_rows: Sequence[Mapping[str, Any]],
    previous_set: Optional[Mapping[str, Any]],
    previous_symbol_manifest: Optional[Mapping[str, Any]],
    previous_rows: Sequence[Mapping[str, Any]],
    used_for_delta: bool,
    max_previous_gap_minutes: int,
) -> Dict[str, Any]:
    previous_gap_minutes = None
    if (
        used_for_delta
        and isinstance(current_set, Mapping)
        and isinstance(previous_set, Mapping)
    ):
        try:
            previous_gap_minutes = round(
                (
                    _utc(current_set.get("available_at_utc"))
                    - _utc(previous_set.get("available_at_utc"))
                ).total_seconds()
                / 60.0,
                6,
            )
        except (TypeError, ValueError):
            previous_gap_minutes = None
    provenance = {
        "policy_version": SHADOW_PROVENANCE_POLICY_VERSION,
        "symbol": symbol,
        "current": _compact_snapshot_provenance(
            current_set,
            current_symbol_manifest,
            current_rows,
            symbol=symbol,
        ),
        "previous": (
            _compact_snapshot_provenance(
                previous_set,
                previous_symbol_manifest,
                previous_rows,
                symbol=symbol,
            )
            if used_for_delta
            else None
        ),
        "used_for_delta": bool(used_for_delta),
        "previous_gap_minutes": previous_gap_minutes,
        "previous_gap_policy_minutes": max(1, int(max_previous_gap_minutes)),
    }
    return {
        "provenance": provenance,
        "provenance_sha256": canonical_provenance_sha256(provenance),
    }


def _sha256_text(value: Any) -> bool:
    text = str(value or "").strip()
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text)


def _snapshot_provenance_errors(
    value: Any,
    *,
    expected_symbol: str,
) -> list[str]:
    record = dict(value) if isinstance(value, Mapping) else {}
    errors: list[str] = []
    if not record:
        return ["snapshot provenance record is missing"]
    if _strict_positive_json_int(record.get("snapshot_set_id")) is None:
        errors.append("snapshot_set_id is missing or invalid")
    for field in (
        "snapshot_key",
        "set_payload_sha256",
        "symbol_manifest_payload_sha256",
    ):
        if not _sha256_text(record.get(field)):
            errors.append(f"{field} is missing or invalid")
    if str(record.get("symbol") or "").upper() != expected_symbol:
        errors.append("snapshot provenance symbol does not match the decision")
    if record.get("archive_schema_version") != ARCHIVE_SCHEMA_VERSION:
        errors.append("archive schema version is incompatible")
    if record.get("method_version") != METHOD_VERSION:
        errors.append("method version is incompatible")
    if record.get("cutover_marker") != CUTOVER_MARKER:
        errors.append("cutover marker is incompatible")
    try:
        if _utc(record.get("cutover_time_utc")) != CUTOVER_TIME_UTC:
            errors.append("cutover timestamp is incompatible")
    except (TypeError, ValueError):
        errors.append("cutover timestamp is missing or invalid")
    if str(record.get("source") or "") not in {"WATCH_SHARED", "RESEARCH_PASSIVE"}:
        errors.append("snapshot provenance source is not research eligible")
    for field in ("cycle_id", "collector_version"):
        if not str(record.get(field) or "").strip():
            errors.append(f"{field} is missing")
    for field in ("available_at_utc", "created_at_utc", "cycle_time_utc"):
        try:
            timestamp = _utc(record.get(field))
        except (TypeError, ValueError):
            errors.append(f"{field} is missing or invalid")
            continue
        if timestamp < CUTOVER_TIME_UTC:
            errors.append(f"{field} predates the archive cutover")
    try:
        if _utc(record.get("cycle_time_utc")) > _utc(
            record.get("available_at_utc")
        ):
            errors.append("cycle timestamp is after snapshot availability")
    except (TypeError, ValueError):
        pass
    row_hashes = record.get("row_payload_sha256")
    if not isinstance(row_hashes, Sequence) or isinstance(row_hashes, (str, bytes)):
        errors.append("seven row payload hashes are missing")
    else:
        entries = [dict(item) for item in row_hashes if isinstance(item, Mapping)]
        if len(row_hashes) != len(REQUIRED_TIMEFRAMES) or len(entries) != len(
            REQUIRED_TIMEFRAMES
        ):
            errors.append("snapshot provenance does not contain seven row hashes")
        by_timeframe = {
            str(item.get("timeframe") or ""): item.get("payload_sha256")
            for item in entries
        }
        ordered_timeframes = [str(item.get("timeframe") or "") for item in entries]
        if ordered_timeframes != list(REQUIRED_TIMEFRAMES):
            errors.append("snapshot provenance row timeframes are incomplete")
        ordered_hashes = [by_timeframe.get(timeframe) for timeframe in REQUIRED_TIMEFRAMES]
        if any(not _sha256_text(value) for value in ordered_hashes):
            errors.append("one or more row payload hashes are invalid")
        elif len(set(ordered_hashes)) != len(REQUIRED_TIMEFRAMES):
            errors.append("seven row payload hashes are not unique")
    return list(dict.fromkeys(errors))


def _validate_shadow_provenance(
    provenance: Any,
    provenance_sha256: Any,
    *,
    decision_time_utc: Any,
    expected_symbol: Any,
    require_previous: bool,
) -> tuple[bool, str]:
    """Validate a compact decision-time chain without reading archive tables."""
    normalized_symbol = _symbol(expected_symbol)
    value = dict(provenance) if isinstance(provenance, Mapping) else {}
    errors: list[str] = []
    if normalized_symbol is None:
        errors.append("decision symbol is invalid")
    if value.get("policy_version") != SHADOW_PROVENANCE_POLICY_VERSION:
        errors.append("Max-Pain provenance policy version is incompatible")
    if normalized_symbol and str(value.get("symbol") or "").upper() != normalized_symbol:
        errors.append("Max-Pain provenance symbol does not match the decision")
    expected_hash = str(provenance_sha256 or "").strip()
    if not _sha256_text(expected_hash):
        errors.append("canonical Max-Pain provenance hash is missing or invalid")
    elif canonical_provenance_sha256(value) != expected_hash:
        errors.append("canonical Max-Pain provenance hash does not match its payload")
    current = value.get("current")
    if normalized_symbol:
        errors.extend(
            _snapshot_provenance_errors(current, expected_symbol=normalized_symbol)
        )
    used_for_delta = value.get("used_for_delta")
    if not isinstance(used_for_delta, bool):
        errors.append("used_for_delta is not a boolean")
    previous = value.get("previous")
    policy_gap = _strict_positive_json_int(
        value.get("previous_gap_policy_minutes")
    )
    if policy_gap != DEFAULT_MAX_PREVIOUS_GAP_MINUTES:
        errors.append("previous snapshot gap policy is missing or invalid")
    if not require_previous:
        if bool(used_for_delta):
            errors.append("current-only condition unexpectedly used a previous snapshot")
        if previous is not None:
            errors.append("current-only condition carries unexpected previous provenance")
        if value.get("previous_gap_minutes") is not None:
            errors.append("current-only condition carries an unexpected previous gap")
    if require_previous and not bool(used_for_delta):
        errors.append("delta/trend condition lacks a frozen previous snapshot")
    if require_previous or bool(used_for_delta):
        if normalized_symbol:
            errors.extend(
                _snapshot_provenance_errors(previous, expected_symbol=normalized_symbol)
            )
        recomputed_gap = None
        if isinstance(current, Mapping) and isinstance(previous, Mapping):
            current_set_id = _strict_positive_json_int(
                current.get("snapshot_set_id")
            )
            previous_set_id = _strict_positive_json_int(
                previous.get("snapshot_set_id")
            )
            if (
                current_set_id is not None
                and previous_set_id is not None
                and current_set_id == previous_set_id
            ):
                errors.append("current and previous snapshot_set_id are identical")
            if str(current.get("snapshot_key") or "") == str(
                previous.get("snapshot_key") or ""
            ):
                errors.append("current and previous snapshot_key are identical")
            try:
                current_available = _utc(current.get("available_at_utc"))
                previous_available = _utc(previous.get("available_at_utc"))
                recomputed_gap = (
                    current_available - previous_available
                ).total_seconds() / 60.0
                if previous_available >= current_available:
                    errors.append("previous snapshot is not strictly earlier than current")
            except (TypeError, ValueError):
                pass
        gap = _strict_positive_json_number(value.get("previous_gap_minutes"))
        if gap is None:
            errors.append("previous snapshot gap is missing or invalid")
        elif recomputed_gap is not None and not math.isclose(
            gap, recomputed_gap, rel_tol=0.0, abs_tol=1e-6
        ):
            errors.append("previous snapshot gap does not match availability timestamps")
        if policy_gap is not None and recomputed_gap is not None and recomputed_gap > policy_gap:
            errors.append("previous snapshot gap exceeds the frozen policy")
    try:
        decision_time = _utc(decision_time_utc)
        if isinstance(current, Mapping):
            current_available = _utc(current.get("available_at_utc"))
            current_created = _utc(current.get("created_at_utc"))
            current_age_minutes = (
                decision_time - current_available
            ).total_seconds() / 60.0
            if current_age_minutes < -1e-6:
                errors.append("current snapshot was not available at decision time")
            elif current_age_minutes > DEFAULT_MAX_DECISION_AGE_MINUTES:
                errors.append("current snapshot was stale at decision time")
            if current_created > decision_time:
                errors.append("current snapshot was inserted after decision time")
        if isinstance(previous, Mapping):
            if _utc(previous.get("created_at_utc")) > decision_time:
                errors.append("previous snapshot was inserted after decision time")
    except (TypeError, ValueError):
        errors.append("decision timestamp is missing or invalid")
    if errors:
        return False, "; ".join(list(dict.fromkeys(errors)))
    return True, "complete Max-Pain provenance chain is bound to the decision"


def validate_shadow_provenance(
    provenance: Any,
    provenance_sha256: Any,
    *,
    decision_time_utc: Any,
    expected_symbol: Any,
    require_previous: bool,
) -> tuple[bool, str]:
    """Fail closed for every malformed scalar in persisted JSON evidence."""
    try:
        return _validate_shadow_provenance(
            provenance,
            provenance_sha256,
            decision_time_utc=decision_time_utc,
            expected_symbol=expected_symbol,
            require_previous=require_previous,
        )
    except (TypeError, ValueError, OverflowError):
        # JSONB normally constrains this input, but defensive readiness checks
        # must not let an oversized or otherwise hostile scalar abort the
        # entire formula scan before the incompatible event can be excluded.
        return False, "Max-Pain provenance payload contains an invalid scalar"


def _mapping(value: Any) -> Dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "__dict__"):
        return dict(vars(value))
    return {}


def _symbol(value: Any) -> Optional[str]:
    text = str(value or "").strip().upper()
    if not text or len(text) > 20 or not text.replace("-", "").isalnum():
        return None
    return text


def _snapshot_source_rows(snapshot: Mapping[str, Any]) -> list[Dict[str, Any]]:
    rows = [_mapping(row) for row in snapshot.get("rows") or []]
    if rows:
        return [row for row in rows if row]
    by_timeframe = snapshot.get("by_timeframe")
    if not isinstance(by_timeframe, Mapping):
        return []
    recovered: list[Dict[str, Any]] = []
    for timeframe in REQUIRED_TIMEFRAMES:
        for source in by_timeframe.get(timeframe) or []:
            row = _mapping(source)
            if row:
                row.setdefault("timeframe", timeframe)
                recovered.append(row)
    return recovered


def _pair(row: Mapping[str, Any]) -> Optional[tuple[str, str]]:
    symbol = _symbol(row.get("symbol"))
    timeframe = str(row.get("timeframe") or "").strip()
    if symbol is None or timeframe not in REQUIRED_TIMEFRAMES:
        return None
    return symbol, timeframe


def _source_aliases(row: Mapping[str, Any]) -> Dict[str, Any]:
    source = dict(row)
    return {
        **source,
        "short_max_pain": source.get(
            "short_max_pain", source.get("max_short_price")
        ),
        "long_max_pain": source.get(
            "long_max_pain", source.get("max_long_price")
        ),
        "short_liquidation_amount": source.get(
            "short_liquidation_amount", source.get("short_amount_usd")
        ),
        "long_liquidation_amount": source.get(
            "long_liquidation_amount", source.get("long_amount_usd")
        ),
        "source_observed_at_utc": source.get(
            "source_observed_at_utc", source.get("collected_at_utc")
        ),
    }


def _price_policy_status(
    symbol: str,
    price_source: Any,
    price_exchange: Any,
    price_market: Any,
    price_pair: Any,
    price_instrument: Any,
    price_interval: Any,
) -> tuple[str, Optional[str]]:
    source = str(price_source or "").strip().lower()
    exchange = str(price_exchange or "").strip().lower()
    market = str(price_market or "").strip().lower()
    pair_text = str(price_pair or "").strip().upper()
    pair = pair_text.replace("/", "")
    instrument = str(price_instrument or "").strip()
    interval = str(price_interval or "").strip().lower()
    if not source:
        return "UNKNOWN", "price source is missing"
    if symbol == "HYPE":
        if (
            source == "hyperliquid"
            and exchange == "hyperliquid"
            and market == "spot"
            and pair_text == "HYPE/USDT"
            and instrument == "@107"
            and interval == "1m"
        ):
            return "PASS", None
        return (
            "FAIL",
            "HYPE research requires explicit Hyperliquid HYPE/USDT Spot @107 1m provenance",
        )
    expected_pairs = {f"{symbol}USDT"}
    if (
        source == "binance_spot"
        and exchange == "binance"
        and market == "spot"
        and pair in expected_pairs
        and interval == "1m"
    ):
        return "PASS", None
    return "FAIL", "research price requires Binance Spot USDT 1m"


def _freshness(
    observed_at: Any,
    completed_at: datetime,
    *,
    max_age_minutes: int,
) -> tuple[str, Optional[float], Optional[str]]:
    if observed_at in (None, ""):
        return "UNKNOWN", None, "source observation timestamp is missing"
    try:
        observed = _utc(observed_at)
    except (TypeError, ValueError) as exc:
        return "UNKNOWN", None, f"invalid source timestamp: {exc}"
    age = (completed_at - observed).total_seconds() / 60.0
    if age < -1e-6:
        return "STALE", round(age, 6), "source timestamp is after collection completion"
    if age > max(1, int(max_age_minutes)):
        return "STALE", round(age, 6), "source observation is stale"
    return "FRESH", round(max(0.0, age), 6), None


def _normalized_row(
    source: Mapping[str, Any],
    *,
    completed_at: datetime,
    duplicate: bool,
    max_source_age_minutes: int,
) -> Dict[str, Any]:
    row = _source_aliases(source)
    symbol = _symbol(row.get("symbol")) or "INVALID"
    timeframe = str(row.get("timeframe") or "")
    current_price = _positive_float(row.get("current_price"))
    coinglass_price = _positive_float(row.get("coinglass_price"))
    if coinglass_price is None and not row.get("price_source"):
        coinglass_price = current_price
    short_target = _positive_float(row.get("short_max_pain"))
    long_target = _positive_float(row.get("long_max_pain"))
    short_amount = _nonnegative_float(row.get("short_liquidation_amount"))
    long_amount = _nonnegative_float(row.get("long_liquidation_amount"))
    price_source = str(row.get("price_source") or "").strip().lower()
    price_exchange = str(row.get("price_exchange") or "").strip().lower()
    price_market = str(row.get("price_market") or "").strip().lower()
    price_pair = str(row.get("price_pair") or "").strip().upper()
    price_instrument = str(row.get("price_instrument") or "").strip()
    price_interval = str(row.get("price_interval") or "").strip().lower()
    # Binance Spot's operational source label is sufficient to make its
    # exchange/market provenance explicit.  Generic Hyperliquid ``allMids`` is
    # intentionally not upgraded to HYPE Spot @107 without explicit fields.
    if price_source == "binance_spot":
        price_exchange = price_exchange or "binance"
        price_market = price_market or "spot"
    price_policy, policy_error = _price_policy_status(
        symbol,
        price_source,
        price_exchange,
        price_market,
        price_pair,
        price_instrument,
        price_interval,
    )
    observed_at = row.get("source_observed_at_utc")
    price_fetched_at = row.get("price_fetched_at_utc")
    price_observed_at = row.get("price_observed_at_utc") or price_fetched_at
    source_freshness, source_age, source_freshness_error = _freshness(
        observed_at,
        completed_at,
        max_age_minutes=max_source_age_minutes,
    )
    price_freshness, price_age, price_freshness_error = _freshness(
        price_observed_at,
        completed_at,
        max_age_minutes=max_source_age_minutes,
    )
    statuses = {source_freshness, price_freshness}
    freshness = (
        "STALE" if "STALE" in statuses else "UNKNOWN" if "UNKNOWN" in statuses else "FRESH"
    )

    errors: list[str] = []
    if timeframe not in REQUIRED_TIMEFRAMES:
        errors.append("invalid timeframe")
    if current_price is None or current_price <= 0:
        errors.append("current price is missing or invalid")
    if short_target is None or short_target <= 0:
        errors.append("short Max-Pain target is missing or invalid")
    if long_target is None or long_target <= 0:
        errors.append("long Max-Pain target is missing or invalid")
    if short_amount is None or short_amount < 0:
        errors.append("short liquidation amount is missing or invalid")
    if long_amount is None or long_amount < 0:
        errors.append("long liquidation amount is missing or invalid")
    if policy_error:
        errors.append(policy_error)
    if source_freshness_error:
        errors.append(f"Max-Pain source: {source_freshness_error}")
    if price_freshness_error:
        errors.append(f"price source: {price_freshness_error}")
    if duplicate:
        errors.append("duplicate symbol/timeframe in source collection")

    short_signed = (
        (short_target - current_price) / current_price * 100.0
        if current_price and short_target is not None
        else None
    )
    long_signed = (
        (long_target - current_price) / current_price * 100.0
        if current_price and long_target is not None
        else None
    )
    normalized = {
        "symbol": symbol,
        "timeframe": timeframe,
        "rank": _positive_int(row.get("rank")),
        "source_observed_at_utc": _iso_or_none(observed_at),
        "current_price": _round_positive(current_price),
        "coinglass_current_price": _round_positive(coinglass_price),
        "price_source": price_source or None,
        "price_exchange": price_exchange or None,
        "price_market": price_market or None,
        "price_pair": price_pair or None,
        "price_instrument": price_instrument or None,
        "price_fetched_at_utc": _iso_or_none(price_fetched_at),
        "price_source_policy_status": price_policy,
        "short_max_pain": _round_positive(short_target),
        "long_max_pain": _round_positive(long_target),
        "short_liquidation_amount": _round(short_amount, 2),
        "long_liquidation_amount": _round(long_amount, 2),
        "short_target_signed_distance_pct": _round(short_signed),
        "long_target_signed_distance_pct": _round(long_signed),
        "short_target_abs_distance_pct": _round(
            abs(short_signed) if short_signed is not None else None
        ),
        "long_target_abs_distance_pct": _round(
            abs(long_signed) if long_signed is not None else None
        ),
        "row_valid": not errors,
        "freshness_status": freshness,
        "validation_errors": errors,
        "raw_provenance": {
            "official_price_policy_version": OFFICIAL_PRICE_POLICY_VERSION,
            "source_age_minutes_at_capture": source_age,
            "price_age_minutes_at_capture": price_age,
            "price_interval": price_interval or None,
            "price_observed_at_utc": _iso_or_none(price_observed_at),
            "price_candle_open_time_utc": _iso_or_none(
                row.get("price_candle_open_time_utc")
            ),
            "price_candle_close_time_utc": _iso_or_none(
                row.get("price_candle_close_time_utc")
            ),
            "raw_collected_at_utc": row.get("collected_at_utc"),
            "closest_side": row.get("closest_side"),
            # Typed audit columns obey the database's scalar constraints even
            # when a source row is invalid. Preserve the exact rejected
            # numerics here so normalization never erases source evidence.
            "raw_numeric_values": {
                "rank": _audit_scalar(row.get("rank")),
                "current_price": _audit_scalar(row.get("current_price")),
                "coinglass_current_price": _audit_scalar(
                    row.get("coinglass_price")
                ),
                "short_max_pain": _audit_scalar(row.get("short_max_pain")),
                "long_max_pain": _audit_scalar(row.get("long_max_pain")),
                "short_liquidation_amount": _audit_scalar(
                    row.get("short_liquidation_amount")
                ),
                "long_liquidation_amount": _audit_scalar(
                    row.get("long_liquidation_amount")
                ),
            },
        },
    }
    normalized["payload_sha256"] = _sha256(normalized)
    return normalized


def build_snapshot_payload(
    *,
    cycle_id: str,
    cycle_time_utc: Any,
    collection_started_at_utc: Any,
    collection_completed_at_utc: Any,
    source: str,
    collector_version: str,
    snapshot: Optional[Mapping[str, Any]] = None,
    enriched_rows: Optional[Iterable[Mapping[str, Any]]] = None,
    live_result: Optional[Mapping[str, Any]] = None,
    capture_metadata: Optional[Mapping[str, Any]] = None,
    failure_reason: Optional[str] = None,
    max_source_age_minutes: int = DEFAULT_MAX_CAPTURE_SOURCE_AGE_MINUTES,
) -> Dict[str, Any]:
    """Normalize one collection attempt into an immutable set plus rows."""
    cycle_time = _utc(cycle_time_utc)
    started_at = _utc(collection_started_at_utc)
    completed_at = _utc(collection_completed_at_utc)
    if cycle_time < CUTOVER_TIME_UTC:
        raise ValueError("new Max-Pain archive refuses pre-cutover cycles")
    if completed_at < started_at:
        raise ValueError("collection completion precedes collection start")
    cycle_id = str(cycle_id or "").strip()
    source = str(source or "").strip()
    collector_version = str(collector_version or "").strip()
    if not cycle_id or not source or not collector_version:
        raise ValueError("cycle_id, source and collector_version are required")

    snapshot_map = dict(snapshot or {})
    live_map = dict(live_result or {})
    source_rows = _snapshot_source_rows(snapshot_map)
    enriched = [_mapping(row) for row in enriched_rows or []]
    all_input = [row for row in source_rows + enriched if _pair(row) is not None]
    pair_counts: Dict[tuple[str, str], int] = {}
    for row in source_rows:
        key = _pair(row)
        if key is not None:
            pair_counts[key] = pair_counts.get(key, 0) + 1

    merged: Dict[tuple[str, str], Dict[str, Any]] = {}
    for row in source_rows:
        key = _pair(row)
        if key is not None and key not in merged:
            merged[key] = _source_aliases(row)
    for row in enriched:
        key = _pair(row)
        if key is None:
            continue
        merged[key] = {**merged.get(key, {}), **_source_aliases(row)}

    duplicate_pairs = {
        f"{symbol}/{timeframe}"
        for (symbol, timeframe), count in pair_counts.items()
        if count > 1
    }
    duplicate_pairs.update(
        str(value) for value in snapshot_map.get("duplicate_pairs") or []
    )
    rows = [
        _normalized_row(
            merged[key],
            completed_at=completed_at,
            duplicate=f"{key[0]}/{key[1]}" in duplicate_pairs,
            max_source_age_minutes=max_source_age_minutes,
        )
        for key in sorted(
            merged,
            key=lambda item: (item[0], REQUIRED_TIMEFRAMES.index(item[1])),
        )
    ]

    rows_by_symbol: Dict[str, list[Dict[str, Any]]] = {}
    timeframes_by_symbol: Dict[str, set[str]] = {}
    for row in rows:
        rows_by_symbol.setdefault(row["symbol"], []).append(row)
        timeframes_by_symbol.setdefault(row["symbol"], set()).add(row["timeframe"])
    observed_timeframes = {row["timeframe"] for row in rows}
    missing_timeframes = set(snapshot_map.get("missing_timeframes") or [])
    missing_timeframes.update(
        timeframe for timeframe in REQUIRED_TIMEFRAMES if timeframe not in observed_timeframes
    )
    missing_timeframes = {
        value for value in missing_timeframes if value in REQUIRED_TIMEFRAMES
    }
    invalid_rows = [row for row in rows if not row["row_valid"]]
    skipped_symbols = sorted(
        {str(value).upper() for value in live_map.get("skipped_symbols") or []}
    )
    snapshot_ok = snapshot_map.get("ok") is True
    source_pairs = {
        key for row in source_rows if (key := _pair(row)) is not None
    }
    # Eligibility is per symbol.  An audit-only HYPE group (for example from
    # generic Hyperliquid allMids) must not poison coherent Binance Spot groups
    # in the same shared Watch cycle.  A globally failed DOM collection does
    # invalidate every group because it is not an atomic seven-tab snapshot.
    globally_usable = bool(
        rows
        and snapshot_ok
        and not failure_reason
        and not missing_timeframes
        and not duplicate_pairs
        and observed_timeframes == set(REQUIRED_TIMEFRAMES)
    )
    research_cycle = globally_usable and source in {
        "WATCH_SHARED",
        "RESEARCH_PASSIVE",
    }
    symbols: list[Dict[str, Any]] = []
    for symbol, symbol_rows in sorted(rows_by_symbol.items()):
        symbol_timeframes = timeframes_by_symbol[symbol]
        missing_for_symbol = [
            timeframe
            for timeframe in REQUIRED_TIMEFRAMES
            if timeframe not in symbol_timeframes
        ]
        duplicate_for_symbol = [
            timeframe
            for timeframe in REQUIRED_TIMEFRAMES
            if f"{symbol}/{timeframe}" in duplicate_pairs
        ]
        unbacked_timeframes = [
            timeframe
            for timeframe in REQUIRED_TIMEFRAMES
            if (symbol, timeframe) not in source_pairs
            and timeframe in symbol_timeframes
        ]
        invalid_count = sum(not bool(row["row_valid"]) for row in symbol_rows)
        price_signatures = {
            (
                row.get("current_price"),
                row.get("price_source"),
                row.get("price_exchange"),
                row.get("price_market"),
                row.get("price_pair"),
                row.get("price_instrument"),
                row.get("price_fetched_at_utc"),
            )
            for row in symbol_rows
        }
        price_overlay_coherent = len(price_signatures) == 1
        symbol_freshness_values = {
            str(row.get("freshness_status") or "UNKNOWN") for row in symbol_rows
        }
        symbol_freshness = (
            "FRESH"
            if symbol_rows and symbol_freshness_values == {"FRESH"}
            else "STALE"
            if "STALE" in symbol_freshness_values
            else "UNKNOWN"
        )
        complete_7of7 = bool(
            len(symbol_rows) == len(REQUIRED_TIMEFRAMES)
            and symbol_timeframes == set(REQUIRED_TIMEFRAMES)
            and not duplicate_for_symbol
        )
        symbol_errors: list[str] = []
        if not globally_usable:
            symbol_errors.append("collection cycle did not pass global atomic validation")
        elif not research_cycle:
            symbol_errors.append(
                "only WATCH_SHARED or RESEARCH_PASSIVE cycles are research eligible"
            )
        if missing_for_symbol:
            symbol_errors.append("symbol is missing one or more required timeframes")
        if duplicate_for_symbol:
            symbol_errors.append("symbol has duplicate timeframe evidence")
        if unbacked_timeframes:
            symbol_errors.append("one or more rows lack raw DOM backing")
        if symbol in skipped_symbols:
            symbol_errors.append("live-price collection marked the symbol as skipped")
        if invalid_count:
            symbol_errors.append("one or more symbol rows failed validation")
        if not price_overlay_coherent:
            symbol_errors.append("seven-timeframe current-price overlay is incoherent")
        if symbol_freshness != "FRESH":
            symbol_errors.append("symbol source freshness could not be proven")
        symbol_eligible = bool(
            research_cycle
            and complete_7of7
            and not unbacked_timeframes
            and symbol not in skipped_symbols
            and invalid_count == 0
            and price_overlay_coherent
            and symbol_freshness == "FRESH"
        )
        manifest: Dict[str, Any] = {
            "symbol": symbol,
            "observed_timeframe_count": len(symbol_timeframes),
            "missing_timeframes": missing_for_symbol,
            "duplicate_timeframes": duplicate_for_symbol,
            "invalid_row_count": invalid_count,
            "complete_7of7": complete_7of7,
            "price_overlay_coherent": price_overlay_coherent,
            "validation_status": "PASS" if symbol_eligible else "FAIL",
            "freshness_status": symbol_freshness,
            "research_eligible": symbol_eligible,
            "validation_errors": symbol_errors,
        }
        manifest["payload_sha256"] = _sha256(manifest)
        symbols.append(manifest)

    complete_symbols = sorted(
        item["symbol"] for item in symbols if item["complete_7of7"]
    )
    incomplete_symbols = {
        item["symbol"]: item["missing_timeframes"]
        for item in symbols
        if not item["complete_7of7"]
    }
    price_incoherent_symbols = sorted(
        item["symbol"] for item in symbols if not item["price_overlay_coherent"]
    )
    eligible_symbols = sorted(
        item["symbol"] for item in symbols if item["research_eligible"]
    )
    ineligible_symbols = sorted(
        item["symbol"] for item in symbols if not item["research_eligible"]
    )
    symbol_freshness_values = {item["freshness_status"] for item in symbols}
    freshness_status = (
        "FRESH"
        if symbols and symbol_freshness_values == {"FRESH"}
        else "PARTIAL"
        if "FRESH" in symbol_freshness_values
        else "STALE"
        if "STALE" in symbol_freshness_values
        else "UNKNOWN"
    )
    set_complete = globally_usable
    collection_status = (
        "FAILED"
        if failure_reason
        else "COMPLETE"
        if set_complete
        else "INCOMPLETE"
    )
    validation_errors: list[str] = []
    if failure_reason:
        validation_errors.append(str(failure_reason))
    if not snapshot_ok:
        validation_errors.append("DOM snapshot did not pass atomic validation")
    if missing_timeframes:
        validation_errors.append("one or more required timeframes are missing")
    if incomplete_symbols:
        validation_errors.append("one or more symbol groups do not have 7/7 timeframes")
    if duplicate_pairs:
        validation_errors.append("duplicate symbol/timeframe rows were observed")
    if invalid_rows:
        validation_errors.append("one or more audit rows failed validation")
    if price_incoherent_symbols:
        validation_errors.append(
            "one or more symbols have an incoherent seven-timeframe price overlay"
        )
    if ineligible_symbols:
        validation_errors.append(
            f"{len(ineligible_symbols)} of {len(symbols)} symbol groups are ineligible"
        )
    validation_status = (
        "PASS"
        if set_complete and symbols and len(eligible_symbols) == len(symbols)
        else "PARTIAL"
        if set_complete and eligible_symbols
        else "FAIL"
    )
    research_eligible = bool(set_complete and eligible_symbols)

    price_result = live_map.get("price_result")
    price_result = dict(price_result) if isinstance(price_result, Mapping) else {}
    source_metadata = {
        "legacy_table_policy": "NEVER_READ_OR_IMPORTED",
        "snapshot_collected_at_utc": snapshot_map.get("collected_at_utc"),
        "snapshot_row_count": snapshot_map.get("row_count", len(source_rows)),
        "snapshot_timeframe_counts": snapshot_map.get("timeframe_counts") or {},
        "price_result": {
            key: price_result.get(key)
            for key in (
                "source",
                "price_policy",
                "window_start_utc",
                "window_end_utc",
                "fetched_at_utc",
                "requested_count",
                "found_count",
                "missing_count",
                "missing_symbols",
                "futures_error",
                "spot_error",
                "bybit_futures_error",
                "bybit_spot_error",
                "hyperliquid_error",
                "errors",
                "fallback_used",
            )
            if key in price_result
        },
        "capture_metadata": _json_safe(dict(capture_metadata or {})),
    }
    set_record: Dict[str, Any] = {
        "snapshot_key": _sha256(
            {
                "method_version": METHOD_VERSION,
                "cutover_marker": CUTOVER_MARKER,
                "cycle_id": cycle_id,
                "cycle_time_utc": _iso(cycle_time),
                "source": source,
                "collector_version": collector_version,
            }
        ),
        "archive_schema_version": ARCHIVE_SCHEMA_VERSION,
        "method_version": METHOD_VERSION,
        "cutover_marker": CUTOVER_MARKER,
        "cutover_time_utc": _iso(CUTOVER_TIME_UTC),
        "cycle_id": cycle_id,
        "cycle_time_utc": _iso(cycle_time),
        "collection_started_at_utc": _iso(started_at),
        "collection_completed_at_utc": _iso(completed_at),
        "available_at_utc": _iso(completed_at),
        "source": source,
        "collector_version": collector_version,
        "expected_timeframes": list(REQUIRED_TIMEFRAMES),
        "expected_timeframe_count": len(REQUIRED_TIMEFRAMES),
        "observed_timeframe_count": len(observed_timeframes),
        "observed_symbol_count": len(timeframes_by_symbol),
        "complete_symbol_count": len(complete_symbols),
        "incomplete_symbol_count": len(incomplete_symbols),
        "eligible_symbol_count": len(eligible_symbols),
        "ineligible_symbol_count": len(ineligible_symbols),
        "row_count": len(rows),
        "invalid_row_count": len(invalid_rows),
        "missing_timeframes": sorted(
            missing_timeframes, key=REQUIRED_TIMEFRAMES.index
        ),
        "duplicate_pairs": sorted(duplicate_pairs),
        "skipped_symbols": skipped_symbols,
        "collection_status": collection_status,
        "validation_status": validation_status,
        "freshness_status": freshness_status,
        "set_complete_7of7": set_complete,
        "research_eligible": research_eligible,
        "completeness_report": {
            "required_timeframes": list(REQUIRED_TIMEFRAMES),
            "complete_symbols": complete_symbols,
            "incomplete_symbols": incomplete_symbols,
            "eligible_symbols": eligible_symbols,
            "ineligible_symbols": ineligible_symbols,
            "price_incoherent_symbols": price_incoherent_symbols,
            "source_input_rows": len(all_input),
        },
        "validation_errors": validation_errors,
        "source_metadata": source_metadata,
    }
    set_record["payload_sha256"] = _sha256(
        {"set": set_record, "symbols": symbols, "rows": rows}
    )
    return {"set": set_record, "symbols": symbols, "rows": rows}


def archive_enabled() -> bool:
    # Explicit opt-in prevents a code deploy from writing before migration 007
    # and its operational cutover have been deliberately enabled.
    value = os.getenv("MAX_PAIN_ARCHIVE_ENABLED", "0").strip().lower()
    return value in _TRUE


def _database_url() -> str:
    return os.getenv("DATABASE_URL", "").strip()


def persistence_status() -> Dict[str, Any]:
    return {
        "enabled": archive_enabled(),
        "database_configured": bool(_database_url()),
        "driver_available": psycopg is not None,
        "schema_auto_create": False,
        "legacy_table_allowed": False,
        "method_version": METHOD_VERSION,
        "cutover_marker": CUTOVER_MARKER,
    }


def _jsonb(value: Any) -> Any:
    return Jsonb(_json_safe(value)) if Jsonb is not None else _json_safe(value)


_SET_COLUMNS = (
    "snapshot_key",
    "archive_schema_version",
    "method_version",
    "cutover_marker",
    "cutover_time_utc",
    "cycle_id",
    "cycle_time_utc",
    "collection_started_at_utc",
    "collection_completed_at_utc",
    "available_at_utc",
    "source",
    "collector_version",
    "expected_timeframes",
    "expected_timeframe_count",
    "observed_timeframe_count",
    "observed_symbol_count",
    "complete_symbol_count",
    "incomplete_symbol_count",
    "eligible_symbol_count",
    "ineligible_symbol_count",
    "row_count",
    "invalid_row_count",
    "missing_timeframes",
    "duplicate_pairs",
    "skipped_symbols",
    "collection_status",
    "validation_status",
    "freshness_status",
    "set_complete_7of7",
    "research_eligible",
    "completeness_report",
    "validation_errors",
    "source_metadata",
    "payload_sha256",
)

_SYMBOL_COLUMNS = (
    "symbol",
    "observed_timeframe_count",
    "missing_timeframes",
    "duplicate_timeframes",
    "invalid_row_count",
    "complete_7of7",
    "price_overlay_coherent",
    "validation_status",
    "freshness_status",
    "research_eligible",
    "validation_errors",
    "payload_sha256",
)

_ROW_COLUMNS = (
    "symbol",
    "timeframe",
    "rank",
    "source_observed_at_utc",
    "current_price",
    "coinglass_current_price",
    "price_source",
    "price_exchange",
    "price_market",
    "price_pair",
    "price_instrument",
    "price_fetched_at_utc",
    "price_source_policy_status",
    "short_max_pain",
    "long_max_pain",
    "short_liquidation_amount",
    "long_liquidation_amount",
    "short_target_signed_distance_pct",
    "long_target_signed_distance_pct",
    "short_target_abs_distance_pct",
    "long_target_abs_distance_pct",
    "row_valid",
    "freshness_status",
    "validation_errors",
    "raw_provenance",
    "payload_sha256",
)


def persist_snapshot_payload(
    payload: Mapping[str, Any], *, database_url: Optional[str] = None
) -> Dict[str, Any]:
    """Atomically append one set, symbol manifests and rows.

    The immutable snapshot key makes collection retries idempotent.  A retry
    with different contents for the same key is rejected as a collision.
    """
    if not archive_enabled():
        return {"persisted": False, "reason": "Max-Pain archive is disabled"}
    url = str(database_url or _database_url()).strip()
    if not url:
        return {"persisted": False, "reason": "DATABASE_URL is not configured"}
    if psycopg is None:
        return {"persisted": False, "reason": "psycopg is unavailable"}
    set_record = dict(payload.get("set") or {})
    symbols = [dict(item) for item in payload.get("symbols") or []]
    rows = [dict(row) for row in payload.get("rows") or []]
    if set(set_record) != set(_SET_COLUMNS):
        missing = sorted(set(_SET_COLUMNS) - set(set_record))
        extra = sorted(set(set_record) - set(_SET_COLUMNS))
        raise ValueError(f"invalid set payload fields; missing={missing}; extra={extra}")
    if int(set_record.get("row_count") or 0) != len(rows):
        raise ValueError("set row_count does not match row payload")
    if int(set_record.get("observed_symbol_count") or 0) != len(symbols):
        raise ValueError("set observed_symbol_count does not match symbol payload")
    if sum(bool(item.get("research_eligible")) for item in symbols) != int(
        set_record.get("eligible_symbol_count") or 0
    ):
        raise ValueError("set eligible_symbol_count does not match symbol payload")
    symbol_names = {str(item.get("symbol") or "") for item in symbols}
    if len(symbol_names) != len(symbols):
        raise ValueError("symbol manifest payload contains duplicates")
    if {str(row.get("symbol") or "") for row in rows} != symbol_names:
        raise ValueError("snapshot rows and symbol manifests do not cover the same symbols")

    set_values = []
    for column in _SET_COLUMNS:
        value = set_record[column]
        if column in {"completeness_report", "validation_errors", "source_metadata"}:
            value = _jsonb(value)
        set_values.append(value)
    set_sql = (
        f"INSERT INTO research_max_pain_snapshot_sets ({','.join(_SET_COLUMNS)}) "
        f"VALUES ({','.join(['%s'] * len(_SET_COLUMNS))}) "
        "ON CONFLICT (snapshot_key) DO NOTHING RETURNING snapshot_set_id"
    )
    with psycopg.connect(
        url,
        row_factory=dict_row,
        connect_timeout=5,
        options="-c statement_timeout=10000",
    ) as conn:
        relation = conn.execute(
            "SELECT to_regclass('public.research_max_pain_snapshot_sets') AS sets, "
            "to_regclass('public.research_max_pain_snapshot_symbols') AS symbols, "
            "to_regclass('public.research_max_pain_snapshot_rows') AS rows"
        ).fetchone()
        if (
            not relation
            or not relation.get("sets")
            or not relation.get("symbols")
            or not relation.get("rows")
        ):
            raise RuntimeError("migration 007 Max-Pain archive schema is not installed")
        inserted = conn.execute(set_sql, tuple(set_values)).fetchone()
        if not inserted:
            existing = conn.execute(
                "SELECT snapshot_set_id, payload_sha256 "
                "FROM research_max_pain_snapshot_sets WHERE snapshot_key=%s",
                (set_record["snapshot_key"],),
            ).fetchone()
            if not existing:
                raise RuntimeError("snapshot key conflict could not be resolved")
            if str(existing["payload_sha256"]).strip() != set_record["payload_sha256"]:
                raise RuntimeError("snapshot key collision has a different payload hash")
            conn.commit()
            return {
                "persisted": True,
                "idempotent_existing": True,
                "snapshot_set_id": int(existing["snapshot_set_id"]),
                "symbol_count": len(symbols),
                "row_count": len(rows),
                "research_eligible": bool(set_record["research_eligible"]),
            }
        snapshot_set_id = int(inserted["snapshot_set_id"])
        if symbols:
            symbol_sql = (
                "INSERT INTO research_max_pain_snapshot_symbols "
                f"(snapshot_set_id,{','.join(_SYMBOL_COLUMNS)}) VALUES "
                f"({','.join(['%s'] * (len(_SYMBOL_COLUMNS) + 1))})"
            )
            symbol_values = []
            for manifest in symbols:
                if set(manifest) != set(_SYMBOL_COLUMNS):
                    raise ValueError("invalid snapshot-symbol payload fields")
                item = []
                for column in _SYMBOL_COLUMNS:
                    value = manifest[column]
                    if column == "validation_errors":
                        value = _jsonb(value)
                    item.append(value)
                symbol_values.append((snapshot_set_id, *item))
            conn.cursor().executemany(symbol_sql, symbol_values)
        if rows:
            row_sql = (
                "INSERT INTO research_max_pain_snapshot_rows "
                f"(snapshot_set_id,{','.join(_ROW_COLUMNS)}) VALUES "
                f"({','.join(['%s'] * (len(_ROW_COLUMNS) + 1))})"
            )
            values = []
            for row in rows:
                if set(row) != set(_ROW_COLUMNS):
                    raise ValueError("invalid snapshot-row payload fields")
                item = []
                for column in _ROW_COLUMNS:
                    value = row[column]
                    if column in {"validation_errors", "raw_provenance"}:
                        value = _jsonb(value)
                    item.append(value)
                values.append((snapshot_set_id, *item))
            conn.cursor().executemany(row_sql, values)
        conn.commit()
    return {
        "persisted": True,
        "idempotent_existing": False,
        "snapshot_set_id": snapshot_set_id,
        "symbol_count": len(symbols),
        "row_count": len(rows),
        "research_eligible": bool(set_record["research_eligible"]),
    }


def _ratio(numerator: Any, denominator: Any) -> Optional[float]:
    left = _float(numerator)
    right = _float(denominator)
    if left is None or right in (None, 0.0):
        return None
    return round(left / right, 8)


def _pct_change(current: Any, previous: Any) -> Optional[float]:
    current_value = _float(current)
    previous_value = _float(previous)
    if current_value is None or previous_value in (None, 0.0):
        return None
    return round((current_value - previous_value) / abs(previous_value) * 100.0, 8)


def _trend(value: Any, epsilon: float = 1e-9) -> Optional[str]:
    number = _float(value)
    if number is None:
        return None
    if number > epsilon:
        return "STRENGTHENING"
    if number < -epsilon:
        return "WEAKENING"
    return "UNCHANGED"


def _cluster(targets: Sequence[float], max_spread_pct: float = 1.0) -> Dict[str, Any]:
    ordered = sorted(float(value) for value in targets if _float(value) not in (None, 0.0))
    if not ordered:
        return {"count": 0, "spread_pct": None, "all_target_spread_pct": None}
    all_mean = sum(ordered) / len(ordered)
    all_spread = (
        (ordered[-1] - ordered[0]) / all_mean * 100.0 if all_mean > 0 else None
    )
    best: tuple[int, float] = (1, 0.0)
    for left in range(len(ordered)):
        for right in range(left, len(ordered)):
            group = ordered[left : right + 1]
            average = sum(group) / len(group)
            spread = (group[-1] - group[0]) / average * 100.0 if average else 0.0
            if spread <= max_spread_pct + 1e-12:
                candidate = (len(group), spread)
                if candidate[0] > best[0] or (
                    candidate[0] == best[0] and candidate[1] < best[1]
                ):
                    best = candidate
    return {
        "count": best[0],
        "spread_pct": round(best[1], 8),
        "all_target_spread_pct": _round(all_spread),
    }


def _current_features(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    by_timeframe = {str(row["timeframe"]): dict(row) for row in rows}
    features: Dict[str, Any] = {}
    closer_upside = 0
    closer_downside = 0
    upside_active = 0
    downside_active = 0
    upside_targets: list[float] = []
    downside_targets: list[float] = []
    upside_distances: list[float] = []
    downside_distances: list[float] = []
    total_upside_liquidity = 0.0
    total_downside_liquidity = 0.0

    for timeframe in REQUIRED_TIMEFRAMES:
        row = by_timeframe[timeframe]
        prefix = f"max_pain.{timeframe}"
        short_signed = _float(row.get("short_target_signed_distance_pct"))
        long_signed = _float(row.get("long_target_signed_distance_pct"))
        short_amount = float(row.get("short_liquidation_amount") or 0.0)
        long_amount = float(row.get("long_liquidation_amount") or 0.0)
        upside_distance = short_signed if short_signed is not None and short_signed > 0 else None
        downside_distance = abs(long_signed) if long_signed is not None and long_signed < 0 else None
        if upside_distance is not None:
            upside_active += 1
            upside_distances.append(upside_distance)
            upside_targets.append(float(row["short_max_pain"]))
        if downside_distance is not None:
            downside_active += 1
            downside_distances.append(downside_distance)
            downside_targets.append(float(row["long_max_pain"]))
        if upside_distance is not None and (
            downside_distance is None or upside_distance < downside_distance
        ):
            closer = "UPSIDE"
            closer_upside += 1
        elif downside_distance is not None and (
            upside_distance is None or downside_distance < upside_distance
        ):
            closer = "DOWNSIDE"
            closer_downside += 1
        else:
            closer = "NEUTRAL_OR_MISSING"
        gross = short_amount + long_amount
        imbalance = (
            (short_amount - long_amount) / gross * 100.0 if gross > 0 else None
        )
        total_upside_liquidity += short_amount
        total_downside_liquidity += long_amount
        features.update(
            {
                f"{prefix}.short_target_signed_distance_pct": _round(short_signed),
                f"{prefix}.long_target_signed_distance_pct": _round(long_signed),
                f"{prefix}.upside_active_distance_pct": _round(upside_distance),
                f"{prefix}.downside_active_distance_pct": _round(downside_distance),
                f"{prefix}.upside_liquidity_usd": round(short_amount, 2),
                f"{prefix}.downside_liquidity_usd": round(long_amount, 2),
                f"{prefix}.short_liquidity_usd": round(short_amount, 2),
                f"{prefix}.long_liquidity_usd": round(long_amount, 2),
                f"{prefix}.upside_downside_liquidity_ratio": _ratio(
                    short_amount, long_amount
                ),
                f"{prefix}.short_long_liquidity_ratio": _ratio(
                    short_amount, long_amount
                ),
                f"{prefix}.liquidity_imbalance_pct": _round(imbalance),
                f"{prefix}.closer_active_direction": closer,
            }
        )

    gross_total = total_upside_liquidity + total_downside_liquidity
    aggregate_imbalance = (
        (total_upside_liquidity - total_downside_liquidity)
        / gross_total
        * 100.0
        if gross_total > 0
        else None
    )
    dominant_count = max(closer_upside, closer_downside)
    dominant_direction = (
        "UPSIDE"
        if closer_upside > closer_downside
        else "DOWNSIDE"
        if closer_downside > closer_upside
        else "TIED"
    )
    upside_cluster = _cluster(upside_targets)
    downside_cluster = _cluster(downside_targets)
    features.update(
        {
            "max_pain.aggregate.upside_active_timeframe_count": upside_active,
            "max_pain.aggregate.downside_active_timeframe_count": downside_active,
            "max_pain.aggregate.closer_upside_count": closer_upside,
            "max_pain.aggregate.closer_downside_count": closer_downside,
            "max_pain.aggregate.consensus_direction": dominant_direction,
            "max_pain.aggregate.consensus_count": dominant_count,
            "max_pain.aggregate.consensus_ratio": round(
                dominant_count / len(REQUIRED_TIMEFRAMES), 8
            ),
            "max_pain.aggregate.upside_liquidity_usd": round(
                total_upside_liquidity, 2
            ),
            "max_pain.aggregate.downside_liquidity_usd": round(
                total_downside_liquidity, 2
            ),
            "max_pain.aggregate.short_liquidity_usd": round(
                total_upside_liquidity, 2
            ),
            "max_pain.aggregate.long_liquidity_usd": round(
                total_downside_liquidity, 2
            ),
            "max_pain.aggregate.upside_downside_liquidity_ratio": _ratio(
                total_upside_liquidity, total_downside_liquidity
            ),
            "max_pain.aggregate.short_long_liquidity_ratio": _ratio(
                total_upside_liquidity, total_downside_liquidity
            ),
            "max_pain.aggregate.liquidity_imbalance_pct": _round(
                aggregate_imbalance
            ),
            "max_pain.aggregate.median_upside_active_distance_pct": _round(
                median(upside_distances) if upside_distances else None
            ),
            "max_pain.aggregate.median_downside_active_distance_pct": _round(
                median(downside_distances) if downside_distances else None
            ),
            "max_pain.aggregate.upside_cluster_count_1pct": upside_cluster["count"],
            "max_pain.aggregate.upside_cluster_spread_pct": upside_cluster[
                "spread_pct"
            ],
            "max_pain.aggregate.upside_all_target_spread_pct": upside_cluster[
                "all_target_spread_pct"
            ],
            "max_pain.aggregate.downside_cluster_count_1pct": downside_cluster[
                "count"
            ],
            "max_pain.aggregate.downside_cluster_spread_pct": downside_cluster[
                "spread_pct"
            ],
            "max_pain.aggregate.downside_all_target_spread_pct": downside_cluster[
                "all_target_spread_pct"
            ],
        }
    )
    return {key: value for key, value in features.items() if value is not None}


def _snapshot_validation_errors(
    set_record: Mapping[str, Any],
    symbol_manifest: Optional[Mapping[str, Any]],
    rows: Sequence[Mapping[str, Any]],
    *,
    symbol: str,
    decision_time: datetime,
    max_age_minutes: int,
) -> list[str]:
    errors: list[str] = []
    if set_record.get("method_version") != METHOD_VERSION:
        errors.append("method version is not eligible")
    if set_record.get("cutover_marker") != CUTOVER_MARKER:
        errors.append("cutover marker is not eligible")
    if not bool(set_record.get("research_eligible")):
        errors.append("snapshot set is not research eligible")
    if not bool(set_record.get("set_complete_7of7")):
        errors.append("snapshot set is not 7/7 complete")
    if set_record.get("collection_status") != "COMPLETE":
        errors.append("snapshot collection did not complete")
    if set_record.get("source") not in {"WATCH_SHARED", "RESEARCH_PASSIVE"}:
        errors.append("snapshot is not from an approved research collection cohort")
    if set_record.get("validation_status") not in {"PASS", "PARTIAL"}:
        errors.append("snapshot set validation did not pass")
    if set_record.get("freshness_status") not in {"FRESH", "PARTIAL"}:
        errors.append("snapshot set was not fresh when captured")
    manifest = dict(symbol_manifest or {})
    if not manifest:
        errors.append("symbol eligibility manifest is missing")
    else:
        if str(manifest.get("symbol") or "").upper() != symbol:
            errors.append("symbol eligibility manifest does not match the request")
        if not bool(manifest.get("research_eligible")):
            errors.append("symbol group is not research eligible")
        if not bool(manifest.get("complete_7of7")):
            errors.append("symbol group is not 7/7 complete")
        if not bool(manifest.get("price_overlay_coherent")):
            errors.append("symbol current-price overlay is incoherent")
        if manifest.get("validation_status") != "PASS":
            errors.append("symbol group validation did not pass")
        if manifest.get("freshness_status") != "FRESH":
            errors.append("symbol group was not fresh when captured")
        if int(manifest.get("invalid_row_count") or 0) != 0:
            errors.append("symbol manifest declares invalid rows")
        if manifest.get("missing_timeframes"):
            errors.append("symbol manifest declares missing timeframes")
        if manifest.get("duplicate_timeframes"):
            errors.append("symbol manifest declares duplicate timeframes")
    try:
        available = _utc(set_record.get("available_at_utc"))
    except (TypeError, ValueError):
        errors.append("snapshot availability timestamp is missing or invalid")
        available = decision_time
    try:
        created = _utc(set_record.get("created_at_utc"))
    except (TypeError, ValueError):
        errors.append("snapshot creation timestamp is missing or invalid")
        created = decision_time
    if available < CUTOVER_TIME_UTC:
        errors.append("snapshot availability predates the archive cutover")
    age_minutes = (decision_time - available).total_seconds() / 60.0
    if age_minutes < -1e-6:
        errors.append("snapshot was not available at decision time")
    elif age_minutes > max(1, int(max_age_minutes)):
        errors.append("snapshot is stale at decision time")
    if created > decision_time:
        errors.append("snapshot was inserted after decision time")
    if len(rows) != len(REQUIRED_TIMEFRAMES):
        errors.append("symbol does not have exactly seven snapshot rows")
    timeframes = {str(row.get("timeframe") or "") for row in rows}
    if timeframes != set(REQUIRED_TIMEFRAMES):
        errors.append("symbol timeframe set is incomplete")
    if any(not bool(row.get("row_valid")) for row in rows):
        errors.append("one or more symbol rows are invalid")
    if any(row.get("price_source_policy_status") != "PASS" for row in rows):
        errors.append("one or more symbol rows violate the official-price policy")
    for row in rows:
        raw_provenance = _mapping(row.get("raw_provenance"))
        policy_status, _ = _price_policy_status(
            symbol,
            row.get("price_source"),
            row.get("price_exchange"),
            row.get("price_market"),
            row.get("price_pair"),
            row.get("price_instrument"),
            raw_provenance.get("price_interval"),
        )
        if policy_status != "PASS":
            errors.append("row price provenance does not satisfy the official policy")
    if any(row.get("freshness_status") != "FRESH" for row in rows):
        errors.append("one or more symbol rows were not fresh at capture")
    price_signatures = {
        (
            row.get("current_price"),
            row.get("price_source"),
            row.get("price_exchange"),
            row.get("price_market"),
            row.get("price_pair"),
            row.get("price_instrument"),
            _iso_or_none(row.get("price_fetched_at_utc")),
        )
        for row in rows
    }
    if len(price_signatures) != 1:
        errors.append("symbol rows do not share one coherent current-price overlay")
    for row in rows:
        for field in ("source_observed_at_utc", "price_fetched_at_utc"):
            timestamp = _iso_or_none(row.get(field))
            if timestamp is None:
                errors.append(f"row {field} is missing or invalid")
            elif _utc(timestamp) > available:
                errors.append(f"row {field} is after snapshot availability")
    return list(dict.fromkeys(errors))


def derive_prior_only_features(
    *,
    symbol: str,
    decision_time_utc: Any,
    current_set: Optional[Mapping[str, Any]],
    current_rows: Sequence[Mapping[str, Any]],
    current_symbol_manifest: Optional[Mapping[str, Any]] = None,
    previous_set: Optional[Mapping[str, Any]] = None,
    previous_rows: Sequence[Mapping[str, Any]] = (),
    previous_symbol_manifest: Optional[Mapping[str, Any]] = None,
    max_age_minutes: int = DEFAULT_MAX_DECISION_AGE_MINUTES,
    max_previous_gap_minutes: int = DEFAULT_MAX_PREVIOUS_GAP_MINUTES,
) -> Dict[str, Any]:
    """Derive raw Max-Pain evidence without reading after decision_time_utc."""
    normalized_symbol = _symbol(symbol)
    decision_time = _utc(decision_time_utc)
    if normalized_symbol is None:
        raise ValueError("invalid symbol")
    provenance_inputs = {
        "symbol": normalized_symbol,
        "current_set": current_set,
        "current_symbol_manifest": current_symbol_manifest,
        "current_rows": current_rows,
        "previous_set": previous_set,
        "previous_symbol_manifest": previous_symbol_manifest,
        "previous_rows": previous_rows,
    }
    if not isinstance(current_set, Mapping):
        return {
            "evaluation_status": "UNEVALUABLE",
            "reason": "no prior coherent Max-Pain snapshot is available",
            "features": {},
            **_provenance_bundle(
                **provenance_inputs,
                used_for_delta=False,
                max_previous_gap_minutes=max_previous_gap_minutes,
            ),
        }
    selected_rows = [
        dict(row)
        for row in current_rows
        if str(row.get("symbol") or "").upper() == normalized_symbol
    ]
    errors = _snapshot_validation_errors(
        current_set,
        current_symbol_manifest,
        selected_rows,
        symbol=normalized_symbol,
        decision_time=decision_time,
        max_age_minutes=max_age_minutes,
    )
    if errors:
        return {
            "evaluation_status": "UNEVALUABLE",
            "reason": "; ".join(errors),
            "features": {},
            "snapshot_set_id": current_set.get("snapshot_set_id"),
            **_provenance_bundle(
                **provenance_inputs,
                used_for_delta=False,
                max_previous_gap_minutes=max_previous_gap_minutes,
            ),
        }

    features = _current_features(selected_rows)
    current_available = _utc(current_set["available_at_utc"])
    change_status = "UNEVALUABLE"
    change_reason = "no eligible earlier snapshot is available"
    previous_id = None
    if isinstance(previous_set, Mapping):
        earlier_rows = [
            dict(row)
            for row in previous_rows
            if str(row.get("symbol") or "").upper() == normalized_symbol
        ]
        previous_available = _utc(previous_set.get("available_at_utc"))
        gap_minutes = (current_available - previous_available).total_seconds() / 60.0
        previous_errors = _snapshot_validation_errors(
            previous_set,
            previous_symbol_manifest,
            earlier_rows,
            symbol=normalized_symbol,
            decision_time=current_available,
            max_age_minutes=max_previous_gap_minutes,
        )
        if gap_minutes <= 0:
            previous_errors.append("previous snapshot is not strictly earlier")
        if gap_minutes > max(1, int(max_previous_gap_minutes)):
            previous_errors.append("previous snapshot gap is too large")
        if not previous_errors:
            prior_features = _current_features(earlier_rows)
            change_status = "EVALUABLE"
            change_reason = "strictly prior coherent snapshot compared"
            previous_id = previous_set.get("snapshot_set_id")
            features["max_pain.delta.minutes_since_previous_snapshot"] = round(
                gap_minutes, 6
            )
            delta_keys = (
                "max_pain.aggregate.upside_liquidity_usd",
                "max_pain.aggregate.downside_liquidity_usd",
                "max_pain.aggregate.liquidity_imbalance_pct",
                "max_pain.aggregate.closer_upside_count",
                "max_pain.aggregate.closer_downside_count",
                "max_pain.aggregate.upside_cluster_count_1pct",
                "max_pain.aggregate.downside_cluster_count_1pct",
                "max_pain.aggregate.upside_cluster_spread_pct",
                "max_pain.aggregate.downside_cluster_spread_pct",
            )
            for key in delta_keys:
                current = _float(features.get(key))
                previous = _float(prior_features.get(key))
                if current is None or previous is None:
                    continue
                suffix = key.removeprefix("max_pain.aggregate.")
                delta = round(current - previous, 8)
                features[f"max_pain.delta.{suffix}_change"] = delta
                if suffix in {
                    "upside_liquidity_usd",
                    "downside_liquidity_usd",
                }:
                    percent = _pct_change(current, previous)
                    if percent is not None:
                        features[f"max_pain.delta.{suffix}_change_pct"] = percent
                        features[f"max_pain.delta.{suffix}_trend"] = _trend(percent)
                elif suffix in {
                    "closer_upside_count",
                    "closer_downside_count",
                    "upside_cluster_count_1pct",
                    "downside_cluster_count_1pct",
                }:
                    features[f"max_pain.delta.{suffix}_trend"] = _trend(delta)
                elif suffix in {
                    "upside_cluster_spread_pct",
                    "downside_cluster_spread_pct",
                }:
                    # A narrowing cluster is strengthening consensus.
                    features[f"max_pain.delta.{suffix}_trend"] = _trend(-delta)
            for timeframe in REQUIRED_TIMEFRAMES:
                for field in (
                    "upside_liquidity_usd",
                    "downside_liquidity_usd",
                    "upside_active_distance_pct",
                    "downside_active_distance_pct",
                    "short_target_signed_distance_pct",
                    "long_target_signed_distance_pct",
                ):
                    key = f"max_pain.{timeframe}.{field}"
                    current = _float(features.get(key))
                    previous = _float(prior_features.get(key))
                    if current is None or previous is None:
                        continue
                    delta = round(current - previous, 8)
                    features[f"max_pain.delta.{timeframe}.{field}_change"] = delta
                    direction_delta = (
                        -delta if field == "long_target_signed_distance_pct" else delta
                    )
                    features[f"max_pain.delta.{timeframe}.{field}_trend"] = _trend(
                        direction_delta
                    )
        else:
            change_reason = "; ".join(previous_errors)

    return {
        "evaluation_status": "EVALUABLE",
        "reason": "latest coherent snapshot was available before decision time",
        "change_evaluation_status": change_status,
        "change_reason": change_reason,
        "symbol": normalized_symbol,
        "decision_time_utc": _iso(decision_time),
        "snapshot_set_id": current_set.get("snapshot_set_id"),
        "previous_snapshot_set_id": previous_id,
        "snapshot_available_at_utc": _iso(current_available),
        "snapshot_age_minutes": round(
            (decision_time - current_available).total_seconds() / 60.0, 6
        ),
        "method_version": METHOD_VERSION,
        "cutover_marker": CUTOVER_MARKER,
        "features": features,
        **_provenance_bundle(
            **provenance_inputs,
            used_for_delta=change_status == "EVALUABLE",
            max_previous_gap_minutes=max_previous_gap_minutes,
        ),
        "lookahead_contract": (
            "current and previous snapshot sets were complete, fresh and available "
            "at or before the decision timestamp; the legacy table was never read"
        ),
    }


def load_prior_only_features_from_connection(
    conn: Any,
    symbol: str,
    decision_time_utc: Any,
    *,
    max_age_minutes: int = DEFAULT_MAX_DECISION_AGE_MINUTES,
    max_previous_gap_minutes: int = DEFAULT_MAX_PREVIOUS_GAP_MINUTES,
) -> Dict[str, Any]:
    """Read at most two already-visible migration-007 sets on ``conn``.

    This surface lets the prospective sampler freeze the selected feature and
    provenance bundle in the same read pass that fixes the decision time.  A
    later archive insert can therefore never rewrite an older decision.
    """
    normalized_symbol = _symbol(symbol)
    if normalized_symbol is None:
        raise ValueError("invalid symbol")
    decision_time = _utc(decision_time_utc)
    relation = conn.execute(
        "SELECT to_regclass('public.research_max_pain_snapshot_sets') AS sets, "
        "to_regclass('public.research_max_pain_snapshot_symbols') AS symbols, "
        "to_regclass('public.research_max_pain_snapshot_rows') AS rows",
        (),
    ).fetchone()
    if (
        not relation
        or not relation.get("sets")
        or not relation.get("symbols")
        or not relation.get("rows")
    ):
        return {
            "evaluation_status": "UNEVALUABLE",
            "reason": "migration 007 Max-Pain archive schema is unavailable",
            "features": {},
        }
    sets = [
        dict(row)
        for row in conn.execute(
            """
            SELECT s.*
            FROM research_max_pain_snapshot_sets s
            JOIN research_max_pain_snapshot_symbols m
              ON m.snapshot_set_id=s.snapshot_set_id
            WHERE s.research_eligible=TRUE
              AND m.research_eligible=TRUE
              AND m.symbol=%s
              AND s.method_version=%s
              AND s.cutover_marker=%s
              AND s.available_at_utc <= %s
              AND s.created_at_utc <= %s
            ORDER BY s.available_at_utc DESC, s.snapshot_set_id DESC
            LIMIT 2
            """,
            (
                normalized_symbol,
                METHOD_VERSION,
                CUTOVER_MARKER,
                decision_time,
                decision_time,
            ),
        ).fetchall()
    ]
    rows_by_set: Dict[int, list[Dict[str, Any]]] = {}
    manifests_by_set: Dict[int, Dict[str, Any]] = {}
    for set_record in sets:
        set_id = int(set_record["snapshot_set_id"])
        manifest = conn.execute(
            "SELECT * FROM research_max_pain_snapshot_symbols "
            "WHERE snapshot_set_id=%s AND symbol=%s",
            (set_id, normalized_symbol),
        ).fetchone()
        manifests_by_set[set_id] = dict(manifest or {})
        rows_by_set[set_id] = [
            dict(row)
            for row in conn.execute(
                """
                SELECT * FROM research_max_pain_snapshot_rows
                WHERE snapshot_set_id=%s AND symbol=%s
                ORDER BY CASE timeframe
                    WHEN '12h' THEN 1 WHEN '24h' THEN 2 WHEN '48h' THEN 3
                    WHEN '3d' THEN 4 WHEN '1w' THEN 5 WHEN '2w' THEN 6
                    WHEN '1m' THEN 7 ELSE 99 END
                """,
                (set_id, normalized_symbol),
            ).fetchall()
        ]
    if not sets:
        return {
            "evaluation_status": "UNEVALUABLE",
            "reason": "no prior coherent Max-Pain snapshot is available",
            "features": {},
        }
    current = sets[0]
    previous = sets[1] if len(sets) > 1 else None
    return derive_prior_only_features(
        symbol=normalized_symbol,
        decision_time_utc=decision_time,
        current_set=current,
        current_rows=rows_by_set[int(current["snapshot_set_id"])],
        current_symbol_manifest=manifests_by_set[int(current["snapshot_set_id"])],
        previous_set=previous,
        previous_rows=(
            rows_by_set[int(previous["snapshot_set_id"])] if previous else ()
        ),
        previous_symbol_manifest=(
            manifests_by_set[int(previous["snapshot_set_id"])] if previous else None
        ),
        max_age_minutes=max_age_minutes,
        max_previous_gap_minutes=max_previous_gap_minutes,
    )


def load_prior_only_features(
    symbol: str,
    decision_time_utc: Any,
    *,
    max_age_minutes: int = DEFAULT_MAX_DECISION_AGE_MINUTES,
    max_previous_gap_minutes: int = DEFAULT_MAX_PREVIOUS_GAP_MINUTES,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    """Read at most two eligible migration-007 sets; never query legacy data."""
    normalized_symbol = _symbol(symbol)
    if normalized_symbol is None:
        raise ValueError("invalid symbol")
    decision_time = _utc(decision_time_utc)
    url = str(database_url or _database_url()).strip()
    if not url or psycopg is None:
        return {
            "evaluation_status": "UNEVALUABLE",
            "reason": "Max-Pain archive database is unavailable",
            "features": {},
        }
    with psycopg.connect(
        url,
        row_factory=dict_row,
        connect_timeout=5,
        options="-c statement_timeout=8000 -c default_transaction_read_only=on",
    ) as conn:
        return load_prior_only_features_from_connection(
            conn,
            normalized_symbol,
            decision_time,
            max_age_minutes=max_age_minutes,
            max_previous_gap_minutes=max_previous_gap_minutes,
        )
