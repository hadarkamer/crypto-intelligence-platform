"""Immutable, research-only no-horizon contract; independent of legacy v7.

The entry is the first minute open at or after the signal decision. Its actual
open must equal the declared reference price before an outcome can be evidence.
This is a new entry policy, not an exact replay of a mid-minute signal price.
Source routes are caller-declared identities, not proof of exchange provenance.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import math
from typing import Any, Mapping

VERSION = "no-horizon-next-minute-open-contract-v1"
ENTRY_POLICY = "FIRST_FULL_MINUTE_ACTUAL_OPEN_V1"
REPRESENTATIVE_POLICY = "EARLIEST_DECISION_THEN_ENTRY_ID_WITHOUT_OUTCOMES_V1"
SOURCE_FIELDS = ("exchange", "market", "instrument", "price_type", "interval_seconds")


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def utc(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamps must include a timezone")
    return parsed.astimezone(timezone.utc)


def number(value: Any, name: str) -> float:
    if isinstance(value, bool) or value is None:
        raise ValueError(f"{name} must be finite")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be finite") from exc
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _identity(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{name} requires an explicit nonempty string")
    return value


def make_contract(*, candidate_id: str, candidate_version: str, cohort_id: str,
                  dataset_id: str, entry_id: str, symbol: str, direction: str,
                  decision_time_utc: Any, reference_price: Any, threshold_pct: Any,
                  source_route: Mapping[str, Any], parent_policy_version: str) -> dict[str, Any]:
    """Create an exact event contract. Dataset revisions require a new ID.

    ``source_route`` has exactly exchange, market, instrument, price_type and
    interval_seconds=60. No exchange, product, quote or mark/trade fallback is
    inferred. Candle closes accept a minute boundary or boundary minus 1ms;
    availability for both is the full minute boundary.
    """
    identities = {key: _identity(value, key) for key, value in {
        "candidate_id": candidate_id, "candidate_version": candidate_version,
        "cohort_id": cohort_id, "dataset_id": dataset_id, "entry_id": entry_id,
        "symbol": symbol, "parent_policy_version": parent_policy_version,
    }.items()}
    if direction not in ("LONG", "SHORT"):
        raise ValueError("direction must be LONG or SHORT")
    if not isinstance(source_route, Mapping) or set(source_route) != set(SOURCE_FIELDS):
        raise ValueError("complete explicit source_route identity required")
    route = {key: _identity(source_route[key], key) for key in SOURCE_FIELDS[:-1]}
    if type(source_route["interval_seconds"]) is not int or source_route["interval_seconds"] != 60:
        raise ValueError("source_route must identify one-minute candles")
    route["interval_seconds"] = 60
    reference = number(reference_price, "reference_price")
    threshold = number(threshold_pct, "threshold_pct")
    if reference <= 0 or not 0 < threshold < 100:
        raise ValueError("reference must be positive and 0 < threshold_pct < 100")
    decision = utc(decision_time_utc)
    entry = decision.replace(second=0, microsecond=0)
    if entry < decision:
        entry += timedelta(minutes=1)
    width = Decimal(str(threshold)) / 100
    sign = 1 if direction == "LONG" else -1
    favorable = float(Decimal(str(reference)) * (1 + sign * width))
    adverse = float(Decimal(str(reference)) * (1 - sign * width))
    if not all(math.isfinite(x) and x > 0 for x in (favorable, adverse)) or favorable == reference or adverse == reference:
        raise ValueError("barriers must be finite, positive and distinguishable")
    value = {**identities, "contract_version": VERSION, "direction": direction,
        "decision_time_utc": decision.isoformat(), "entry_time_utc": entry.isoformat(),
        "reference_price": reference, "threshold_pct": threshold,
        "favorable_barrier_price": favorable, "adverse_barrier_price": adverse,
        "source_route": route, "entry_policy": ENTRY_POLICY,
        "representative_policy": REPRESENTATIVE_POLICY,
        "outcome_horizon_minutes": None, "outcome_rule": "FIRST_SYMMETRIC_BARRIER_TOUCH",
        "initial_signal_to_entry_seconds": (entry-decision).total_seconds(),
        "observation_policy": "CONTIGUOUS_FULL_MINUTES_AVAILABLE_AT_END_BOUNDARY_V1",
        "same_candle_policy": "OPEN_PRICE_FIRST_ELSE_BOTH_AMBIGUOUS_V1"}
    return {**value, "contract_sha256": digest(value)}


def validate_contract(contract: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(contract, Mapping):
        raise ValueError("contract must be an object")
    try:
        rebuilt = make_contract(**{key: contract[key] for key in (
            "candidate_id", "candidate_version", "cohort_id", "dataset_id", "entry_id",
            "symbol", "direction", "decision_time_utc", "reference_price", "threshold_pct",
            "source_route", "parent_policy_version")})
        if canonical(contract) != canonical(rebuilt):
            raise ValueError("contract changed or has an unsupported version")
    except (KeyError, TypeError) as exc:
        raise ValueError("incomplete contract") from exc
    return rebuilt


def scope_identity(contract: Mapping[str, Any]) -> dict[str, Any]:
    """Shared candidate/cohort identity; excludes each event's time and price."""
    value = validate_contract(contract)
    return {key: value[key] for key in (
        "contract_version", "candidate_id", "candidate_version", "cohort_id", "dataset_id",
        "symbol", "direction", "threshold_pct", "source_route", "parent_policy_version",
        "entry_policy", "representative_policy", "outcome_horizon_minutes", "outcome_rule",
        "observation_policy", "same_candle_policy")}
