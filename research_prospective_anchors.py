"""Pure prospective neutral-anchor construction for Formula Shadow research.

This module has no database, scheduler, Watch, Telegram, or delivery side
effects.  A caller supplies the already-refreshed decision-time source rows and
the frozen replay-coverage decision.  The module emits two silent
``DECISION_SAMPLE`` Research Events (LONG and SHORT) only when all required
30-minute inputs are coherent and available after candle close plus provider
grace.

Missing or incoherent inputs remain explicit ``UNEVALUABLE`` attempts.  A
coverage-excluded symbol is reported without deleting or mutating its supplied
source data.  In particular, HYPE is excluded while its coverage record is not
eligible and becomes eligible only if that same gate later passes.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from typing import Any, Dict, Mapping, Optional

import market_session_baseline
import canonical_price_path
import research_event_capture
import research_historical_replay
import research_no_dwell_outcome
import research_session_width


SAMPLER_VERSION = "prospective-neutral-anchor-v3-max-pain-frozen"
COVERAGE_POLICY_VERSION = (
    "prospective-coverage-v3-completed-fully-validated-replay-run:"
    + research_no_dwell_outcome.METHOD_VERSION
    + ":"
    + research_historical_replay.REPLAY_VERSION
)
EVENT_TYPE = "PROSPECTIVE_NEUTRAL_30M"
TIMEFRAME = "30m"
INTERVAL_MINUTES = market_session_baseline.COINGLASS_CANDLE_INTERVAL_MINUTES
GRACE_MINUTES = market_session_baseline.COINGLASS_CANDLE_GRACE_MINUTES
CAPTURE_WINDOW_MINUTES = INTERVAL_MINUTES
REQUIRED_FAMILIES: tuple[str, ...] = (
    "official_price",
    "price_oi",
    "futures_cvd",
    "spot_cvd",
)
DIRECTIONS: tuple[str, ...] = ("LONG", "SHORT")
EVALUABLE = "EVALUABLE"
UNEVALUABLE = "UNEVALUABLE"
COVERAGE_EXCLUDED = "COVERAGE_EXCLUDED"
NOT_DUE = "NOT_DUE"

_FAMILY_REQUIRED_VALUES: Dict[str, tuple[str, ...]] = {
    "official_price": ("price",),
    "price_oi": ("price_close", "oi_close_usd"),
    "futures_cvd": ("continuous_cum_vol_delta_usd",),
    "spot_cvd": ("continuous_cum_vol_delta_usd",),
}
_FAMILY_FORMULA_VISIBLE_VALUES: Dict[str, tuple[str, ...]] = {
    "official_price": ("price",),
    "price_oi": (
        "price_close",
        "oi_close_usd",
        "price_change_pct",
        "oi_change_pct",
    ),
    "futures_cvd": (
        "buy_volume_usd",
        "sell_volume_usd",
        "api_cum_vol_delta_usd",
        "continuous_cum_vol_delta_usd",
    ),
    "spot_cvd": (
        "buy_volume_usd",
        "sell_volume_usd",
        "api_cum_vol_delta_usd",
        "continuous_cum_vol_delta_usd",
    ),
}
_REQUIRED_COVERAGE_HORIZONS = (60, 240, 720, 1440)
_MIN_COVERAGE_ANCHORS = 250
_MIN_COVERAGE_UTC_DATES = 14
_MIN_COVERAGE_SPAN_HOURS = 336.0
_EXPECTED_RAW_SOURCES = {
    "futures_cvd": "coinglass_futures_aggregated_cvd",
    "spot_cvd": "coinglass_spot_aggregated_cvd",
}
_EXPECTED_PRICE_OI_SOURCE_TABLE = "oi_regime_snapshots"
_EXPECTED_EXCHANGES = {"BINANCE", "OKX", "BYBIT"}
_MAX_OFFICIAL_PRICE_AGE_SECONDS = 120.0
_FAILED_QUALITY = {
    "ERROR",
    "FAIL",
    "FAILED",
    "INCOMPLETE",
    "MISSING",
    "PARTIAL",
    "STALE",
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


def _canonical(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def compute_input_fingerprint(
    *,
    sampler_version: Any,
    coverage_policy_version: Any,
    coverage_snapshot: Mapping[str, Any],
    symbol: Any,
    source_candle_open_utc: Any,
    source_candle_close_utc: Any,
    base_eligible_at_utc: Any,
    expires_at_utc: Any,
    evaluation_status: Any,
    decision_time_utc: Any,
    source_timestamps: Mapping[str, Any],
    source_provenance: Mapping[str, Any],
    frozen_inputs: Mapping[str, Any],
) -> str:
    """Canonical identity for one frozen prospective decision payload."""
    return _sha256(
        {
            "sampler_version": str(sampler_version),
            "coverage_policy_version": str(coverage_policy_version),
            "coverage_snapshot": dict(coverage_snapshot),
            "symbol": _symbol(symbol),
            "source_candle_open_utc": _iso(source_candle_open_utc),
            "source_candle_close_utc": _iso(source_candle_close_utc),
            "base_eligible_at_utc": _iso(base_eligible_at_utc),
            "expires_at_utc": _iso(expires_at_utc),
            "evaluation_status": str(evaluation_status),
            "decision_time_utc": (
                _iso(decision_time_utc)
                if decision_time_utc not in (None, "")
                else None
            ),
            "source_timestamps": dict(source_timestamps),
            "source_provenance": dict(source_provenance),
            "frozen_formula_visible_inputs": dict(frozen_inputs),
        }
    )


def _symbol(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    if (
        not normalized
        or len(normalized) > 20
        or not normalized.replace("-", "").isalnum()
    ):
        raise ValueError(f"invalid prospective-anchor symbol: {value!r}")
    return normalized


def _number(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _strict_nonnegative_json_int(value: Any) -> Optional[int]:
    """Accept a persisted JSON count without bool/string/float coercion."""
    if type(value) is not int or value < 0:
        return None
    return value


def _strict_nonnegative_json_number(value: Any) -> Optional[float]:
    """Accept a finite persisted JSON number without bool/string coercion."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    result = float(value)
    return result if math.isfinite(result) and result >= 0.0 else None


def _floor_interval(value: Any) -> datetime:
    moment = _utc(value)
    minute = (moment.minute // INTERVAL_MINUTES) * INTERVAL_MINUTES
    return moment.replace(minute=minute, second=0, microsecond=0)


def latest_due_slot_open(now: Any) -> datetime:
    """Return the newest interval-open candle whose close+grace has elapsed."""
    safe = _utc(now) - timedelta(minutes=INTERVAL_MINUTES + GRACE_MINUTES)
    return _floor_interval(safe)


def _slot_times(slot_open: Any) -> tuple[datetime, datetime, datetime]:
    opened = _floor_interval(slot_open)
    if _utc(slot_open) != opened:
        raise ValueError("source slot must be aligned to a 30-minute boundary")
    closed = opened + timedelta(minutes=INTERVAL_MINUTES)
    eligible = closed + timedelta(minutes=GRACE_MINUTES)
    return opened, closed, eligible


def _coverage_entries(value: Mapping[str, Any]) -> Mapping[str, Any]:
    by_symbol = value.get("by_symbol")
    if isinstance(by_symbol, Mapping):
        return by_symbol
    return value


def _coverage_status(
    value: Any,
    *,
    expected_symbol: Any,
    checked_at_utc: Any,
    coverage_policy_version: str,
) -> tuple[bool, list[str]]:
    """Require a frozen all-horizon replay-coverage decision per symbol."""
    if not isinstance(value, Mapping):
        return False, ["coverage_snapshot_missing"]
    failures: list[str] = []
    expected_versions = {
        "coverage_policy_version": COVERAGE_POLICY_VERSION,
        "method_version": research_no_dwell_outcome.METHOD_VERSION,
        "replay_version": research_historical_replay.REPLAY_VERSION,
        "coverage_scope_version": (
            research_historical_replay.COVERAGE_SCOPE_VERSION
        ),
        "movement_width_calibration_version": (
            research_session_width.CALIBRATION_VERSION
        ),
        "canonical_price_method_version": canonical_price_path.METHOD_VERSION,
        "canonical_price_provenance_version": (
            canonical_price_path.PRICE_PROVENANCE_VERSION
        ),
    }
    if str(coverage_policy_version or "") != COVERAGE_POLICY_VERSION:
        failures.append("coverage_policy_argument_incompatible")
    normalized_symbol = _symbol(expected_symbol)
    try:
        recorded_symbol = _symbol(value.get("symbol"))
    except (TypeError, ValueError):
        recorded_symbol = ""
    if recorded_symbol != normalized_symbol:
        failures.append("coverage_symbol_mismatch")
    for key, expected in expected_versions.items():
        if value.get(key) != expected:
            failures.append(f"{key}_incompatible")
    run_id = value.get("replay_run_id")
    if isinstance(run_id, bool) or not isinstance(run_id, int) or run_id <= 0:
        failures.append("replay_run_id_invalid")
    checked_at = snapshot_as_of = completed_at = None
    try:
        checked_at = _utc(checked_at_utc)
        snapshot_as_of = _utc(value.get("as_of_utc"))
        completed_at = _utc(value.get("replay_completed_at_utc"))
        if snapshot_as_of > checked_at:
            failures.append("coverage_as_of_is_future")
        if completed_at > checked_at:
            failures.append("replay_completed_at_is_future")
        if completed_at > snapshot_as_of:
            failures.append("replay_completed_after_coverage_as_of")
    except (TypeError, ValueError, OverflowError):
        failures.append("coverage_timestamps_invalid")
    horizons = value.get("horizons")
    if not isinstance(horizons, Mapping):
        return False, sorted(set([*failures, "coverage_horizons_missing"]))
    for horizon in _REQUIRED_COVERAGE_HORIZONS:
        item = horizons.get(str(horizon), horizons.get(horizon))
        if not isinstance(item, Mapping):
            failures.append(f"{horizon}m_missing")
            continue
        if item.get("eligible") is not True:
            failures.append(f"{horizon}m_ineligible")
        item_failed_gates = item.get("failed_gates")
        if not isinstance(item_failed_gates, list) or item_failed_gates:
            failures.append(f"{horizon}m_failed_gates_inconsistent")
        anchors = _strict_nonnegative_json_int(item.get("anchors"))
        utc_dates = _strict_nonnegative_json_int(item.get("utc_dates"))
        span_hours = _strict_nonnegative_json_number(item.get("span_hours"))
        if anchors is None or anchors < _MIN_COVERAGE_ANCHORS:
            failures.append(f"{horizon}m_anchors")
        if utc_dates is None or utc_dates < _MIN_COVERAGE_UTC_DATES:
            failures.append(f"{horizon}m_utc_dates")
        if span_hours is None or span_hours < _MIN_COVERAGE_SPAN_HOURS:
            failures.append(f"{horizon}m_span_hours")
        try:
            minimum_time = _utc(item.get("min_anchor_time_utc"))
            maximum_time = _utc(item.get("max_anchor_time_utc"))
            if minimum_time > maximum_time:
                failures.append(f"{horizon}m_anchor_time_order")
            if snapshot_as_of is not None and maximum_time > snapshot_as_of:
                failures.append(f"{horizon}m_anchor_time_is_future")
            if completed_at is not None:
                if maximum_time > completed_at:
                    failures.append(f"{horizon}m_anchor_time_is_future")
                if maximum_time + timedelta(minutes=horizon) > completed_at:
                    failures.append(f"{horizon}m_outcome_not_closed_at_replay_completion")
            actual_span = (
                maximum_time - minimum_time
            ).total_seconds() / 3600.0
            if span_hours is None or not math.isclose(
                span_hours, actual_span, rel_tol=0.0, abs_tol=1e-6
            ):
                failures.append(f"{horizon}m_span_mismatch")
        except (TypeError, ValueError, OverflowError):
            failures.append(f"{horizon}m_anchor_times_invalid")
    aggregate_failed_gates = value.get("failed_gates")
    if not isinstance(aggregate_failed_gates, list):
        failures.append("aggregate_failed_gates_invalid")
        aggregate_failed_gates = []
    if value.get("eligible") is True and aggregate_failed_gates:
        failures.append("aggregate_failed_gates_inconsistent")
    if value.get("eligible") is not True:
        failures.extend(str(item) for item in aggregate_failed_gates)
        if not failures:
            failures.append("aggregate_coverage_ineligible")
    return not failures, sorted(set(failures))


def _row_values(row: Mapping[str, Any]) -> Mapping[str, Any]:
    values = row.get("values")
    return values if isinstance(values, Mapping) else row


def _source_candle_time(row: Mapping[str, Any]) -> Optional[datetime]:
    value = row.get("source_candle_time_utc")
    return _utc(value) if value not in (None, "") else None


def _refresh_time(row: Mapping[str, Any]) -> Optional[datetime]:
    # Archive ``imported_at`` values can be overwritten by later backfills.
    # Prospective evidence requires the actual live refresh completion time.
    value = row.get("refresh_completed_at_utc")
    return _utc(value) if value not in (None, "") else None


def _official_price_time(row: Mapping[str, Any]) -> Optional[datetime]:
    value = row.get("observed_at_utc")
    return _utc(value) if value not in (None, "") else None


def _provenance(row: Mapping[str, Any]) -> Dict[str, Any]:
    values = _row_values(row)
    result = {
        "source": str(row.get("source") or values.get("source") or "").strip(),
        "quality_status": str(
            row.get("quality_status")
            or row.get("data_quality_status")
            or values.get("quality_status")
            or values.get("data_quality_status")
            or ""
        ).strip().upper(),
    }
    for key in (
        "price_exchange",
        "price_market",
        "price_pair",
        "price_instrument_id",
        "price_timeframe",
        "exchange_list",
        "upstream_source",
        "source_table",
        "source_record_id",
        "price_source",
        "oi_source",
        "fallback_used",
        "fallback_policy",
        "candle_timestamp_mode",
        "refresh_time_semantics",
        "quality_status_basis",
    ):
        item = row.get(key, values.get(key))
        if item not in (None, ""):
            result[key] = item
    return result


def _normalized_pair(value: Any) -> str:
    return "".join(
        character
        for character in str(value or "").strip().upper()
        if character.isalnum()
    )


def _official_price_problem(symbol: str, row: Mapping[str, Any]) -> Optional[str]:
    """Enforce the canonical live-price route without a silent fallback."""
    provenance = _provenance(row)
    exchange = str(provenance.get("price_exchange") or "").strip().upper()
    market = str(provenance.get("price_market") or "").strip().upper()
    pair = _normalized_pair(provenance.get("price_pair"))
    instrument = str(provenance.get("price_instrument_id") or "").strip().upper()
    timeframe = str(provenance.get("price_timeframe") or "").strip().lower()
    fallback_policy = str(
        provenance.get("fallback_policy") or ""
    ).strip().upper()
    if not exchange or not market or not pair or not timeframe:
        return "MISSING_OFFICIAL_CURRENT_PRICE_PROVENANCE:official_price"
    if timeframe != "1m":
        return "UNOFFICIAL_CURRENT_PRICE_TIMEFRAME:official_price"
    if (
        provenance.get("fallback_used") is not False
        or fallback_policy != "PROVIDER_ATTESTED_NO_FALLBACK"
    ):
        return "OFFICIAL_CURRENT_PRICE_FALLBACK_NOT_EXCLUDED:official_price"
    if symbol == "HYPE":
        if (
            str(provenance.get("source") or "").strip().lower()
            != "hyperliquid_spot_@107"
            or
            exchange != "HYPERLIQUID"
            or market != "SPOT"
            or pair != "HYPEUSDT"
            or instrument != "@107"
        ):
            return "UNOFFICIAL_CURRENT_PRICE_PROVENANCE:official_price"
        return None
    expected_pair = f"{symbol}USDT"
    if (
        str(provenance.get("source") or "").strip().lower() != "binance_spot"
        or exchange != "BINANCE"
        or market != "SPOT"
        or pair != expected_pair
    ):
        return "UNOFFICIAL_CURRENT_PRICE_PROVENANCE:official_price"
    return None


def _formula_visible_values(family: str, row: Any) -> Dict[str, Any]:
    """Freeze only raw decision-time values that Formula may consume.

    Provenance and timestamps are frozen separately.  Keeping the whitelist
    here prevents later storage metadata, quality labels, or outcomes from
    accidentally becoming formula inputs.
    """
    if not isinstance(row, Mapping):
        return {}
    values = _row_values(row)
    return {
        key: values.get(key)
        for key in _FAMILY_FORMULA_VISIBLE_VALUES[family]
        if key in values
    }


def _source_timestamp_evidence(
    family: str, row: Any
) -> Dict[str, Any]:
    """Freeze supplied source times even when coverage blocks evaluation."""
    if not isinstance(row, Mapping):
        return {}
    source_key = (
        "observed_at_utc"
        if family == "official_price"
        else "observation_time_utc"
        if family == "price_oi"
        else "source_candle_time_utc"
    )
    result: Dict[str, Any] = {}
    for key in (source_key, "refresh_completed_at_utc"):
        value = row.get(key)
        if value not in (None, ""):
            result[key] = _iso(value)
    if family == "price_oi":
        for key in ("price_fetched_at_utc", "oi_fetched_at_utc"):
            value = row.get(key)
            if value not in (None, ""):
                result[key] = _iso(value)
    return result


def _family_problem(
    family: str,
    row: Any,
    *,
    symbol: str,
    slot_open: datetime,
    slot_close: datetime,
    base_eligible_at: datetime,
    now: datetime,
) -> tuple[Optional[str], Optional[datetime], Optional[datetime]]:
    if not isinstance(row, Mapping):
        return f"MISSING_REQUIRED_SOURCE:{family}", None, None
    if row.get("available") is False or row.get("complete") is False:
        return f"INCOMPLETE_REQUIRED_SOURCE:{family}", None, None
    row_symbol = str(row.get("symbol") or "").strip().upper()
    if row_symbol != symbol:
        return f"SOURCE_SYMBOL_MISMATCH:{family}", None, None
    refresh_time = _refresh_time(row)
    if refresh_time is None:
        return f"MISSING_REFRESH_TIMESTAMP:{family}", None, None
    if refresh_time < base_eligible_at:
        return f"REFRESH_PRECEDES_CLOSE_PLUS_GRACE:{family}", None, refresh_time
    if refresh_time > now:
        return f"REFRESH_NOT_COMPLETE:{family}", None, refresh_time
    source_time: Optional[datetime] = None
    quality = str(_provenance(row).get("quality_status") or "").upper()
    if quality != "PASS" or quality in _FAILED_QUALITY:
        return f"SOURCE_QUALITY_REJECTED:{family}:{quality}", source_time, refresh_time
    if family == "official_price":
        source_time = _official_price_time(row)
        if source_time is None:
            return f"MISSING_OBSERVATION_TIMESTAMP:{family}", None, refresh_time
        age_seconds = (now - source_time).total_seconds()
        if age_seconds < 0.0 or age_seconds > _MAX_OFFICIAL_PRICE_AGE_SECONDS:
            return f"OFFICIAL_PRICE_NOT_CURRENT:{family}", source_time, refresh_time
        if refresh_time < source_time:
            return f"REFRESH_PRECEDES_OBSERVATION:{family}", source_time, refresh_time
    elif family == "price_oi":
        observation_value = row.get("observation_time_utc")
        source_time = (
            _utc(observation_value)
            if observation_value not in (None, "")
            else None
        )
        if source_time is None:
            return f"MISSING_OBSERVATION_TIMESTAMP:{family}", None, refresh_time
        if source_time < base_eligible_at or source_time > now:
            return f"OBSERVATION_OUTSIDE_DECISION_WINDOW:{family}", source_time, refresh_time
        provenance = _provenance(row)
        if (
            str(provenance.get("source_table") or "").strip().lower()
            != _EXPECTED_PRICE_OI_SOURCE_TABLE
        ):
            return f"UNEXPECTED_SOURCE_PROVENANCE:{family}", source_time, refresh_time
        if not str(provenance.get("price_source") or "").strip():
            return f"MISSING_PRICE_SOURCE_PROVENANCE:{family}", source_time, refresh_time
        if not str(provenance.get("oi_source") or "").strip():
            return f"MISSING_OI_SOURCE_PROVENANCE:{family}", source_time, refresh_time
        for timestamp_key in ("price_fetched_at_utc", "oi_fetched_at_utc"):
            timestamp_value = row.get(timestamp_key)
            if timestamp_value in (None, ""):
                return (
                    f"MISSING_UPSTREAM_TIMESTAMP:{family}:{timestamp_key}",
                    source_time,
                    refresh_time,
                )
            upstream_time = _utc(timestamp_value)
            if upstream_time < slot_close or upstream_time > source_time:
                return (
                    f"UPSTREAM_TIMESTAMP_OUTSIDE_SOURCE_WINDOW:{family}:{timestamp_key}",
                    source_time,
                    refresh_time,
                )
    else:
        source_time = _source_candle_time(row)
        if source_time is None:
            return f"MISSING_SOURCE_TIMESTAMP:{family}", None, refresh_time
        provenance = _provenance(row)
        timestamp_mode = str(
            provenance.get("candle_timestamp_mode") or ""
        ).strip().lower()
        if timestamp_mode not in {"open", "close"}:
            return f"MISSING_CANDLE_TIMESTAMP_MODE:{family}", source_time, refresh_time
        expected_source_time = slot_open if timestamp_mode == "open" else slot_close
        if source_time != expected_source_time:
            return f"SOURCE_SLOT_MISMATCH:{family}", source_time, refresh_time
        expected_source = _EXPECTED_RAW_SOURCES[family]
        if str(provenance.get("source") or "").strip().lower() != expected_source:
            return f"UNEXPECTED_SOURCE_PROVENANCE:{family}", source_time, refresh_time
        if family in {"futures_cvd", "spot_cvd"}:
            exchanges = {
                part.strip().upper()
                for part in str(provenance.get("exchange_list") or "").split(",")
                if part.strip()
            }
            if exchanges != _EXPECTED_EXCHANGES:
                return f"UNEXPECTED_EXCHANGE_SET:{family}", source_time, refresh_time
    values = _row_values(row)
    for key in _FAMILY_REQUIRED_VALUES[family]:
        number = _number(values.get(key))
        if number is None:
            return f"MISSING_REQUIRED_VALUE:{family}:{key}", source_time, refresh_time
        if family in {"official_price", "price_oi"} and number <= 0:
            return f"INVALID_REQUIRED_VALUE:{family}:{key}", source_time, refresh_time
    if family != "price_oi" and not str(_provenance(row).get("source") or "").strip():
        return f"MISSING_SOURCE_PROVENANCE:{family}", source_time, refresh_time
    if family == "official_price":
        price_problem = _official_price_problem(symbol, row)
        if price_problem:
            return price_problem, source_time, refresh_time
    return None, source_time, refresh_time


@dataclass(frozen=True)
class AnchorDecision:
    sampler_version: str
    coverage_policy_version: str
    coverage_snapshot: Mapping[str, Any]
    symbol: str
    source_candle_open_utc: datetime
    source_candle_close_utc: datetime
    base_eligible_at_utc: datetime
    expires_at_utc: datetime
    checked_at_utc: datetime
    evaluation_status: str
    evaluation_reason: str
    missing_sources: tuple[str, ...]
    decision_time_utc: Optional[datetime]
    source_timestamps: Mapping[str, Any]
    source_provenance: Mapping[str, Any]
    frozen_inputs: Mapping[str, Any]
    input_fingerprint: str
    attempt_fingerprint: str
    events: tuple[research_event_capture.ResearchEvent, ...] = ()

    def attempt_record(self) -> Optional[Dict[str, Any]]:
        """Return one audit-attempt row, or ``None`` before the slot is due."""
        if self.evaluation_status == NOT_DUE:
            return None
        return {
            "sampler_version": self.sampler_version,
            "coverage_policy_version": self.coverage_policy_version,
            "coverage_snapshot": dict(self.coverage_snapshot),
            "symbol": self.symbol,
            "interval_minutes": INTERVAL_MINUTES,
            "source_candle_open_utc": self.source_candle_open_utc,
            "source_candle_close_utc": self.source_candle_close_utc,
            "base_eligible_at_utc": self.base_eligible_at_utc,
            "expires_at_utc": self.expires_at_utc,
            "decision_time_utc": self.decision_time_utc,
            "checked_at_utc": self.checked_at_utc,
            "evaluation_status": self.evaluation_status,
            "evaluation_reason": self.evaluation_reason,
            "missing_sources": list(self.missing_sources),
            "source_timestamps": dict(self.source_timestamps),
            "source_provenance": dict(self.source_provenance),
            "frozen_inputs": dict(self.frozen_inputs),
            "input_fingerprint": self.input_fingerprint,
            "attempt_fingerprint": self.attempt_fingerprint,
        }

    def ledger_record(self) -> Optional[Dict[str, Any]]:
        """Compatibility alias for the migration-008 attempt record."""
        return self.attempt_record()

    def atomic_persistence_bundle(self) -> Optional[Dict[str, Any]]:
        """Describe one transaction without writing to storage.

        Evaluable decisions must persist the audit attempt, both event rows,
        and the captured-slot row in one synchronous database transaction.
        The existing asynchronous event writer must not receive these event
        envelopes independently because that could expose a one-sided pair.
        Non-evaluable decisions contain only their auditable attempt.
        """
        attempt = self.attempt_record()
        if attempt is None:
            return None
        event_by_direction = {event.direction: event for event in self.events}
        if self.evaluation_status == EVALUABLE:
            if set(event_by_direction) != set(DIRECTIONS):
                raise ValueError(
                    "evaluable anchor requires one LONG and one SHORT event"
                )
            slot = {
                "sampler_version": self.sampler_version,
                "coverage_policy_version": self.coverage_policy_version,
                "coverage_snapshot": dict(self.coverage_snapshot),
                "symbol": self.symbol,
                "interval_minutes": INTERVAL_MINUTES,
                "source_candle_open_utc": self.source_candle_open_utc,
                "source_candle_close_utc": self.source_candle_close_utc,
                "base_eligible_at_utc": self.base_eligible_at_utc,
                "expires_at_utc": self.expires_at_utc,
                "decision_time_utc": self.decision_time_utc,
                "input_fingerprint": self.input_fingerprint,
                "source_timestamps": dict(self.source_timestamps),
                "source_provenance": dict(self.source_provenance),
                "frozen_inputs": dict(self.frozen_inputs),
                # The transaction adapter resolves these stable fingerprints
                # to event IDs before inserting the captured-slot row.
                "long_event_fingerprint": event_by_direction[
                    "LONG"
                ].event_fingerprint,
                "short_event_fingerprint": event_by_direction[
                    "SHORT"
                ].event_fingerprint,
            }
            event_persistence = tuple(
                {
                    "event": event_by_direction[direction],
                    "capture_stage": "SILENT_NEUTRAL_ANCHOR",
                    "delivery_status": "NOT_APPLICABLE",
                }
                for direction in DIRECTIONS
            )
        else:
            slot = None
            event_persistence = ()
        return {
            "attempt": attempt,
            "event_persistence": event_persistence,
            "slot": slot,
            "atomic_transaction_required": True,
            "live_delivery_allowed": False,
        }


@dataclass(frozen=True)
class AnchorBatch:
    sampler_version: str
    slot_open_utc: datetime
    checked_at_utc: datetime
    decisions: tuple[AnchorDecision, ...]

    @property
    def events(self) -> tuple[research_event_capture.ResearchEvent, ...]:
        return tuple(event for decision in self.decisions for event in decision.events)

    def ledger_records(self) -> list[Dict[str, Any]]:
        return [
            record
            for decision in self.decisions
            if (record := decision.ledger_record()) is not None
        ]

    def atomic_persistence_bundles(self) -> list[Dict[str, Any]]:
        """Return transaction contracts, never individual async envelopes."""
        return [
            bundle
            for decision in self.decisions
            if (bundle := decision.atomic_persistence_bundle()) is not None
        ]

    def summary(self) -> Dict[str, Any]:
        statuses = {status: 0 for status in (EVALUABLE, UNEVALUABLE, COVERAGE_EXCLUDED, NOT_DUE)}
        for decision in self.decisions:
            statuses[decision.evaluation_status] = statuses.get(decision.evaluation_status, 0) + 1
        return {
            "sampler_version": self.sampler_version,
            "slot_open_utc": _iso(self.slot_open_utc),
            "checked_at_utc": _iso(self.checked_at_utc),
            "symbols": len(self.decisions),
            "directional_events": len(self.events),
            "statuses": statuses,
            "telegram_alerts": 0,
            "trade_execution": False,
        }


def _decision(
    *,
    symbol: str,
    coverage: Any,
    coverage_policy_version: str,
    family_rows: Any,
    slot_open: datetime,
    slot_close: datetime,
    base_eligible_at: datetime,
    expires_at: datetime,
    checked_at: datetime,
    strategy_version: Optional[str],
    code_version: Optional[str],
) -> AnchorDecision:
    normalized_symbol = _symbol(symbol)
    eligible, coverage_failures = _coverage_status(
        coverage,
        expected_symbol=normalized_symbol,
        checked_at_utc=checked_at,
        coverage_policy_version=coverage_policy_version,
    )
    coverage_snapshot = dict(coverage) if isinstance(coverage, Mapping) else {}
    source_rows = family_rows if isinstance(family_rows, Mapping) else {}
    timestamps: Dict[str, Any] = {}
    provenance: Dict[str, Any] = {}
    problems: list[str] = []
    missing: list[str] = []
    refresh_times: list[datetime] = []

    # Audit evidence is independent of formula eligibility.  A coverage gate
    # may suppress the neutral event pair, but must not erase which exact
    # decision-time sources and timestamps were supplied for the attempt.
    for family in REQUIRED_FAMILIES:
        row = source_rows.get(family)
        if isinstance(row, Mapping):
            provenance[family] = _provenance(row)
            evidence = _source_timestamp_evidence(family, row)
            if evidence:
                timestamps[family] = evidence

    if checked_at < base_eligible_at:
        status = NOT_DUE
        problems.append("SOURCE_CANDLE_CLOSE_PLUS_GRACE_NOT_REACHED")
    elif checked_at >= expires_at:
        status = UNEVALUABLE if eligible else COVERAGE_EXCLUDED
        problems.append("PROSPECTIVE_CAPTURE_WINDOW_EXPIRED")
        if not eligible:
            problems.append("COVERAGE_GATE_NOT_MET")
    elif not eligible:
        status = COVERAGE_EXCLUDED
        failures = (
            coverage_failures
        )
        suffix = ":" + ",".join(str(item) for item in failures) if failures else ""
        problems.append(f"COVERAGE_GATE_NOT_MET{suffix}")
    else:
        status = EVALUABLE
        for family in REQUIRED_FAMILIES:
            row = source_rows.get(family)
            problem, source_time, refresh_time = _family_problem(
                family,
                row,
                symbol=normalized_symbol,
                slot_open=slot_open,
                slot_close=slot_close,
                base_eligible_at=base_eligible_at,
                now=checked_at,
            )
            if problem:
                status = UNEVALUABLE
                problems.append(problem)
                missing.append(family)
            elif refresh_time is not None:
                refresh_times.append(refresh_time)

    frozen_inputs = {
        family: _formula_visible_values(family, source_rows.get(family))
        for family in REQUIRED_FAMILIES
        if isinstance(source_rows.get(family), Mapping)
    }
    if isinstance(source_rows.get("max_pain"), Mapping):
        # Preserve the complete derived feature/provenance wrapper exactly as
        # selected during the decision-time read. Max Pain remains optional:
        # formulas that do not use it are evaluable even when this wrapper says
        # UNEVALUABLE, while formulas that do use it fail closed later.
        frozen_inputs["max_pain"] = dict(source_rows["max_pain"])
    input_fingerprint = compute_input_fingerprint(
        sampler_version=SAMPLER_VERSION,
        coverage_policy_version=coverage_policy_version,
        coverage_snapshot=coverage_snapshot,
        symbol=normalized_symbol,
        source_candle_open_utc=slot_open,
        source_candle_close_utc=slot_close,
        base_eligible_at_utc=base_eligible_at,
        expires_at_utc=expires_at,
        evaluation_status=status,
        decision_time_utc=checked_at if status == EVALUABLE else None,
        source_timestamps=timestamps,
        source_provenance=provenance,
        frozen_inputs=frozen_inputs,
    )
    # Never backdate a prospective event to a source-refresh time. The actual
    # successful check/persistence attempt is the earliest knowable decision
    # time; source refreshes remain provenance only.
    decision_time = (
        checked_at
        if status == EVALUABLE and len(refresh_times) == len(REQUIRED_FAMILIES)
        else None
    )
    events: tuple[research_event_capture.ResearchEvent, ...] = ()
    if status == EVALUABLE and decision_time is not None:
        anchor_key = _sha256(
            {
                "sampler_version": SAMPLER_VERSION,
                "symbol": normalized_symbol,
                "source_candle_open_utc": _iso(slot_open),
            }
        )
        snapshot = {
            "prospective_anchor": {
                "sampler_version": SAMPLER_VERSION,
                "anchor_key": anchor_key,
                "input_fingerprint": input_fingerprint,
                "source_candle_open_utc": _iso(slot_open),
                "source_candle_close_utc": _iso(slot_close),
                "base_eligible_at_utc": _iso(base_eligible_at),
                "expires_at_utc": _iso(expires_at),
                "decision_time_utc": _iso(decision_time),
                "coverage_policy_version": coverage_policy_version,
                "coverage_snapshot": coverage_snapshot,
                "coverage_eligible": True,
                "source_timestamps": timestamps,
                "source_provenance": provenance,
                "frozen_inputs": frozen_inputs,
                "required_families": list(REQUIRED_FAMILIES),
                "sampling_frame": "NEUTRAL_30M_BOTH_DIRECTIONS",
                "delivery_status": "NOT_APPLICABLE",
                "telegram_delivery_allowed": False,
                "trade_execution_allowed": False,
            }
        }
        current_price = _number(
            _row_values(source_rows["official_price"]).get("price")
        )
        built = []
        for direction in DIRECTIONS:
            generic = research_event_capture.build_decision_sample(
                symbol=normalized_symbol,
                sample_type=EVENT_TYPE,
                direction=direction,
                event_time=decision_time,
                source_side="RAW_NEUTRAL",
                timeframe=TIMEFRAME,
                current_price=current_price,
                categories=(
                    "DECISION_SAMPLE",
                    "NEUTRAL_PROSPECTIVE",
                    "SILENT",
                ),
                engine_snapshot=snapshot,
                setup_identity={
                    "sampler_version": SAMPLER_VERSION,
                    "sampling_frame": "NEUTRAL_30M_BOTH_DIRECTIONS",
                },
                strategy_version=strategy_version
                or "formula-prospective-neutral-v3",
                code_version=code_version,
            )
            # The generic builder hashes the whole snapshot. Raw source rows
            # may later be revised, so slot idempotency instead depends only
            # on immutable sampler/symbol/direction/source-slot identity.
            stable_fingerprint = _sha256(
                {
                    "sampler_version": SAMPLER_VERSION,
                    "event_type": EVENT_TYPE,
                    "symbol": normalized_symbol,
                    "direction": direction,
                    "source_candle_open_utc": _iso(slot_open),
                }
            )
            built.append(replace(generic, event_fingerprint=stable_fingerprint))
        events = tuple(built)

    reason = ";".join(problems) if problems else "ALL_REQUIRED_SOURCES_COHERENT"
    attempt_payload = {
        "sampler_version": SAMPLER_VERSION,
        "coverage_policy_version": coverage_policy_version,
        "symbol": normalized_symbol,
        "slot_open_utc": _iso(slot_open),
        "status": status,
        "reason": reason,
        "input_fingerprint": input_fingerprint,
        "event_fingerprints": [event.event_fingerprint for event in events],
    }
    return AnchorDecision(
        sampler_version=SAMPLER_VERSION,
        coverage_policy_version=coverage_policy_version,
        coverage_snapshot=coverage_snapshot,
        symbol=normalized_symbol,
        source_candle_open_utc=slot_open,
        source_candle_close_utc=slot_close,
        base_eligible_at_utc=base_eligible_at,
        expires_at_utc=expires_at,
        checked_at_utc=checked_at,
        evaluation_status=status,
        evaluation_reason=reason,
        missing_sources=tuple(sorted(set(missing))),
        decision_time_utc=decision_time,
        source_timestamps=timestamps,
        source_provenance=provenance,
        frozen_inputs=frozen_inputs,
        input_fingerprint=input_fingerprint,
        attempt_fingerprint=_sha256(attempt_payload),
        events=events,
    )


def build_anchor_batch(
    *,
    now: Any,
    coverage_by_symbol: Mapping[str, Any],
    source_inputs_by_symbol: Mapping[str, Mapping[str, Any]],
    coverage_policy_version: str,
    slot_open_utc: Any = None,
    strategy_version: Optional[str] = None,
    code_version: Optional[str] = None,
) -> AnchorBatch:
    """Build one deterministic, side-effect-free prospective anchor batch.

    ``coverage_by_symbol`` may be the direct ``{symbol: {eligible: ...}}`` map
    or a coverage object containing ``by_symbol``.  Only symbols present in
    that frozen coverage decision are considered.  A single eligible symbol is
    valid; this layer deliberately imposes no cross-symbol formula rule.
    """
    if not str(coverage_policy_version or "").strip():
        raise ValueError("coverage_policy_version is required")
    checked_at = _utc(now)
    opened = (
        _utc(slot_open_utc)
        if slot_open_utc is not None
        else latest_due_slot_open(checked_at)
    )
    opened, closed, eligible_at = _slot_times(opened)
    expires_at = eligible_at + timedelta(minutes=CAPTURE_WINDOW_MINUTES)
    coverage = _coverage_entries(coverage_by_symbol)
    normalized_sources = {
        _symbol(symbol): values
        for symbol, values in source_inputs_by_symbol.items()
    }
    decisions = tuple(
        _decision(
            symbol=_symbol(symbol),
            coverage=value,
            coverage_policy_version=str(coverage_policy_version),
            family_rows=normalized_sources.get(_symbol(symbol), {}),
            slot_open=opened,
            slot_close=closed,
            base_eligible_at=eligible_at,
            expires_at=expires_at,
            checked_at=checked_at,
            strategy_version=strategy_version,
            code_version=code_version,
        )
        for symbol, value in sorted(
            coverage.items(), key=lambda item: _symbol(item[0])
        )
    )
    return AnchorBatch(
        sampler_version=SAMPLER_VERSION,
        slot_open_utc=opened,
        checked_at_utc=checked_at,
        decisions=decisions,
    )


def shadow_event_fingerprints(batch: AnchorBatch) -> tuple[str, ...]:
    """Stable integration surface for migration-008 Shadow-event view rows."""
    return tuple(event.event_fingerprint for event in batch.events)
