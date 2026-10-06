"""PostgreSQL adapter for silent prospective Formula decision anchors.

This module is deliberately scheduler-facing rather than scheduler-owning:
``ProspectiveAnchorService.run_once`` performs one bounded pass and the caller
decides when to invoke it.  It never imports Telegram, never queues delivery,
and never creates or migrates schema.

An evaluable source slot is committed as one transaction containing its audit
attempt, exactly two neutral ``DECISION_SAMPLE`` events (LONG and SHORT), and
the authoritative slot row.  Conflicts are read back and compared; a changed
source bundle for the same sampler/symbol/slot raises instead of being silently
accepted by ``ON CONFLICT``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Sequence

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - optional in unit-only environments
    psycopg = None
    dict_row = None

import research_event_store
import canonical_price_path
import research_feature_matrix
import research_historical_replay
import research_max_pain_archive
import research_prospective_feature_freeze
import research_session_width
import research_prospective_anchors as anchors


FIRST_TOUCH_METHOD_VERSION = "no-dwell-first-touch-v6"
COMPLETE_PRICE_PATH_QUALITIES: tuple[str, ...] = (
    "VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES",
    "VERIFIED_HYPERLIQUID_SPOT_1M_CLOSED_CANDLES",
)
COVERAGE_POLICY_VERSION = anchors.COVERAGE_POLICY_VERSION
DEFAULT_SYMBOLS: tuple[str, ...] = (
    "BTC",
    "ETH",
    "SOL",
    "BNB",
    "DOGE",
    "XRP",
    "ZEC",
    "HYPE",
)
_HORIZONS = (60, 240, 720, 1440)
_MIN_ANCHORS = 250
_MIN_UTC_DATES = 14
_MIN_SPAN_HOURS = 336.0
_PRIOR_FROZEN_SOURCE_SAMPLERS: tuple[str, ...] = (
    "prospective-neutral-anchor-v3-max-pain-frozen",
    anchors.SAMPLER_VERSION,
)

_COVERAGE_SQL = """
SELECT replay_run_id, replay_version, outcome_method_version,
       config, coverage, completed_at_utc
  FROM research_historical_replay_runs
 WHERE status='COMPLETED'
   AND replay_version=%(replay_version)s
   AND outcome_method_version=%(price_method_version)s
   AND config->>'first_touch_method_version'=%(method_version)s
   AND config->>'movement_width_calibration_version'=%(calibration_version)s
   AND config->>'canonical_price_provenance_version'=%(price_provenance_version)s
   AND config->>'coverage_scope_version'=%(coverage_scope_version)s
   AND jsonb_typeof(config)='object'
   AND jsonb_typeof(coverage)='object'
   AND completed_at_utc IS NOT NULL
   AND completed_at_utc <= %(as_of_utc)s
   AND (config->>'frozen_fully_closed_end_utc')::timestamptz <= %(as_of_utc)s
 ORDER BY completed_at_utc DESC, replay_run_id DESC
 LIMIT 1
"""

_EXISTING_SLOTS_SQL = """
SELECT symbol, input_fingerprint
  FROM research_prospective_anchor_slots
 WHERE sampler_version = %(sampler_version)s
   AND source_candle_open_utc = %(slot_open_utc)s
   AND symbol = ANY(%(symbols)s)
"""

_OI_INPUT_SQL = """
SELECT DISTINCT ON (symbol)
       id, symbol, collected_at, price, open_interest_usd,
       price_change_pct, oi_change_pct, price_fetched_at, oi_fetched_at,
       time_gap_seconds, data_quality_status, price_source, oi_source
  FROM oi_regime_snapshots
 WHERE symbol = ANY(%(symbols)s)
   AND collected_at >= %(eligible_at_utc)s
   AND collected_at <= %(checked_at_utc)s
 ORDER BY symbol, collected_at DESC, id DESC
"""

_FLOW_INPUT_SQL = """
SELECT symbol, candle_time, buy_volume_usd, sell_volume_usd,
       api_cum_vol_delta_usd, continuous_cum_vol_delta_usd,
       exchange_list, source, imported_at
  FROM {table}
 WHERE symbol = ANY(%(symbols)s)
   AND candle_time = %(source_candle_time_utc)s
   AND imported_at <= %(checked_at_utc)s
"""

_ATTEMPT_INSERT_SQL = """
INSERT INTO research_prospective_anchor_attempts (
    sampler_version, coverage_policy_version, coverage_snapshot, symbol,
    interval_minutes, source_candle_open_utc, source_candle_close_utc,
    base_eligible_at_utc, expires_at_utc, decision_time_utc, checked_at_utc,
    evaluation_status, evaluation_reason, missing_sources,
    source_timestamps, source_provenance, frozen_inputs,
    feature_bundle_policy_version, feature_bundle_sha256,
    input_fingerprint, attempt_fingerprint
) VALUES (
    %(sampler_version)s, %(coverage_policy_version)s,
    %(coverage_snapshot)s::jsonb, %(symbol)s, %(interval_minutes)s,
    %(source_candle_open_utc)s, %(source_candle_close_utc)s,
    %(base_eligible_at_utc)s, %(expires_at_utc)s, %(decision_time_utc)s,
    %(checked_at_utc)s, %(evaluation_status)s, %(evaluation_reason)s,
    %(missing_sources)s::jsonb, %(source_timestamps)s::jsonb,
    %(source_provenance)s::jsonb, %(frozen_inputs)s::jsonb,
    %(feature_bundle_policy_version)s, %(feature_bundle_sha256)s,
    %(input_fingerprint)s, %(attempt_fingerprint)s
)
ON CONFLICT (attempt_fingerprint) DO NOTHING
RETURNING attempt_id
"""

_EVENT_INSERT_SQL = """
INSERT INTO research_events (
    schema_version, event_kind, event_type, alert_time_utc, symbol, direction,
    source_side, timeframe, score, current_price, target_price,
    initial_target_distance_pct, categories, setup_key, event_fingerprint,
    strategy_version, code_version, runtime_session_id, capture_stage,
    delivery_status, delivery_attempted_at_utc, delivered_at_utc,
    engine_snapshot
) VALUES (
    %(schema_version)s, %(event_kind)s, %(event_type)s, %(alert_time_utc)s,
    %(symbol)s, %(direction)s, %(source_side)s, %(timeframe)s, %(score)s,
    %(current_price)s, %(target_price)s, %(initial_target_distance_pct)s,
    %(categories)s::jsonb, %(setup_key)s, %(event_fingerprint)s,
    %(strategy_version)s, %(code_version)s, %(runtime_session_id)s,
    %(capture_stage)s, %(delivery_status)s, %(delivery_attempted_at_utc)s,
    %(delivered_at_utc)s, %(engine_snapshot)s::jsonb
)
ON CONFLICT (event_fingerprint) DO NOTHING
RETURNING event_id
"""

_SLOT_INSERT_SQL = """
INSERT INTO research_prospective_anchor_slots (
    sampler_version, coverage_policy_version, coverage_snapshot, symbol,
    interval_minutes, source_candle_open_utc, source_candle_close_utc,
    base_eligible_at_utc, expires_at_utc, decision_time_utc,
    input_fingerprint, source_timestamps, source_provenance, frozen_inputs,
    feature_bundle_policy_version, feature_bundle_sha256,
    decision_feature_bundle,
    long_event_id, short_event_id
) VALUES (
    %(sampler_version)s, %(coverage_policy_version)s,
    %(coverage_snapshot)s::jsonb, %(symbol)s, %(interval_minutes)s,
    %(source_candle_open_utc)s, %(source_candle_close_utc)s,
    %(base_eligible_at_utc)s, %(expires_at_utc)s, %(decision_time_utc)s,
    %(input_fingerprint)s, %(source_timestamps)s::jsonb,
    %(source_provenance)s::jsonb, %(frozen_inputs)s::jsonb,
    %(feature_bundle_policy_version)s, %(feature_bundle_sha256)s,
    %(decision_feature_bundle)s::jsonb,
    %(long_event_id)s, %(short_event_id)s
)
ON CONFLICT (sampler_version, symbol, source_candle_open_utc) DO NOTHING
RETURNING anchor_slot_id
"""


class ProspectiveAnchorError(RuntimeError):
    """Base error for the prospective anchor adapter."""


class ProspectiveAnchorConflictError(ProspectiveAnchorError):
    """A stable slot/fingerprint already exists with different frozen data."""


@dataclass(frozen=True)
class PersistResult:
    symbol: str
    evaluation_status: str
    attempt_id: int
    anchor_slot_id: Optional[int]
    long_event_id: Optional[int]
    short_event_id: Optional[int]
    idempotent: bool


@dataclass(frozen=True)
class SamplingRun:
    sampler_version: str
    slot_open_utc: datetime
    checked_at_utc: datetime
    existing_symbols: tuple[str, ...]
    batch: anchors.AnchorBatch
    persisted: tuple[PersistResult, ...]
    conflicts: tuple[str, ...]

    def summary(self) -> Dict[str, Any]:
        return {
            "sampler_version": self.sampler_version,
            "slot_open_utc": _iso(self.slot_open_utc),
            "checked_at_utc": _iso(self.checked_at_utc),
            "already_captured_symbols": list(self.existing_symbols),
            "decisions": len(self.batch.decisions),
            "directional_events": len(self.batch.events),
            "persisted_attempts": len(self.persisted),
            "persisted_slots": sum(
                1 for item in self.persisted if item.anchor_slot_id is not None
            ),
            "conflicts": list(self.conflicts),
            "telegram_alerts": 0,
            "live_delivery_allowed": False,
            "lookahead_inputs": False,
        }


@dataclass(frozen=True)
class SourceInputBatch:
    inputs_by_symbol: Mapping[str, Mapping[str, Any]]
    cutoff_at_utc: datetime
    read_completed_at_utc: datetime


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
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
        raise ValueError(f"invalid prospective anchor symbol: {value!r}")
    return normalized


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )


def _json_value(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _mapping(value: Any) -> Mapping[str, Any]:
    parsed = _json_value(value)
    return parsed if isinstance(parsed, Mapping) else {}


def _same_json(left: Any, right: Any) -> bool:
    return _json(_json_value(left)) == _json(_json_value(right))


def _same_time(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is None and right is None
    return _utc(left) == _utc(right)


def _same_float(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is None and right is None
    try:
        return math.isclose(
            float(left), float(right), rel_tol=0.0, abs_tol=1e-12
        )
    except (TypeError, ValueError):
        return False


def _strict_nonnegative_int(value: Any) -> Optional[int]:
    """Accept a JSON integer count without coercing strings/bools/floats."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return int(value)


def _strict_nonnegative_finite_number(value: Any) -> Optional[float]:
    """Accept a JSON number for a derived span, rejecting bools and strings."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) and number >= 0.0 else None


def _row_dict(row: Any) -> Dict[str, Any]:
    return dict(row) if row is not None else {}


def _frozen_source_payload(
    source_rows: Mapping[str, Any],
) -> tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """Freeze the base source values and their exact decision-time evidence."""
    frozen_inputs = anchors.freeze_source_inputs(source_rows)
    timestamps: Dict[str, Any] = {}
    provenance: Dict[str, Any] = {}
    for family in anchors.REQUIRED_FAMILIES:
        row = source_rows.get(family)
        if not isinstance(row, Mapping):
            continue
        provenance[family] = anchors._provenance(row)
        evidence = anchors._source_timestamp_evidence(family, row)
        if evidence:
            timestamps[family] = evidence
    return frozen_inputs, timestamps, provenance


def _source_series_manifest(
    price_rows: Sequence[Mapping[str, Any]], *, symbol: str
) -> Dict[str, Any]:
    """Hash the exact immutable prior-slot identities used by one bundle."""
    entries: Dict[int, Dict[str, Any]] = {}
    for source in price_rows:
        if str(source.get("symbol") or "").strip().upper() != symbol:
            continue
        slot_id = source.get("prospective_anchor_slot_id")
        fingerprint = str(
            source.get("prospective_input_fingerprint") or ""
        ).strip().lower()
        sampler_version = str(
            source.get("prospective_sampler_version") or ""
        ).strip()
        decision_time = source.get("prospective_decision_time_utc")
        if (
            isinstance(slot_id, bool)
            or not isinstance(slot_id, int)
            or slot_id <= 0
            or len(fingerprint) != 64
            or any(character not in "0123456789abcdef" for character in fingerprint)
            or sampler_version not in _PRIOR_FROZEN_SOURCE_SAMPLERS
            or decision_time in (None, "")
        ):
            continue
        entries[slot_id] = {
            "anchor_slot_id": slot_id,
            "input_fingerprint": fingerprint,
            "sampler_version": sampler_version,
            "decision_time_utc": _iso(decision_time),
        }
    ordered = sorted(
        entries.values(),
        key=lambda item: (
            _utc(item["decision_time_utc"]),
            int(item["anchor_slot_id"]),
        ),
    )
    digest = hashlib.sha256(_json(ordered).encode("utf-8")).hexdigest()
    return {
        "count": len(ordered),
        "first_decision_time_utc": (
            ordered[0]["decision_time_utc"] if ordered else None
        ),
        "last_decision_time_utc": (
            ordered[-1]["decision_time_utc"] if ordered else None
        ),
        "sha256": digest,
        "sampler_versions": sorted(
            {item["sampler_version"] for item in ordered}
        ),
    }


def _fetchone(result: Any) -> Optional[Dict[str, Any]]:
    row = result.fetchone()
    return _row_dict(row) if row is not None else None


def _fetchall(result: Any) -> list[Dict[str, Any]]:
    return [_row_dict(row) for row in result.fetchall()]


def _attempt_params(attempt: Mapping[str, Any]) -> Dict[str, Any]:
    result = dict(attempt)
    for key in (
        "coverage_snapshot",
        "missing_sources",
        "source_timestamps",
        "source_provenance",
        "frozen_inputs",
    ):
        result[key] = _json(result.get(key) or ([] if key == "missing_sources" else {}))
    return result


def _slot_params(slot: Mapping[str, Any], event_ids: Mapping[str, int]) -> Dict[str, Any]:
    result = dict(slot)
    for key in (
        "coverage_snapshot",
        "source_timestamps",
        "source_provenance",
        "frozen_inputs",
        "decision_feature_bundle",
    ):
        result[key] = _json(result.get(key) or {})
    result["long_event_id"] = int(event_ids["LONG"])
    result["short_event_id"] = int(event_ids["SHORT"])
    return result


def _assert_equal(label: str, existing: Any, expected: Any) -> None:
    if existing != expected:
        raise ProspectiveAnchorConflictError(
            f"prospective anchor conflict at {label}: "
            f"existing={existing!r} expected={expected!r}"
        )


def _verify_attempt(existing: Mapping[str, Any], expected: Mapping[str, Any]) -> None:
    for key in (
        "sampler_version",
        "coverage_policy_version",
        "symbol",
        "interval_minutes",
        "evaluation_status",
        "evaluation_reason",
        "feature_bundle_policy_version",
        "feature_bundle_sha256",
        "input_fingerprint",
        "attempt_fingerprint",
    ):
        _assert_equal(key, str(existing.get(key)).strip(), str(expected.get(key)).strip())
    for key in (
        "source_candle_open_utc",
        "source_candle_close_utc",
        "base_eligible_at_utc",
        "expires_at_utc",
        "decision_time_utc",
    ):
        if not _same_time(existing.get(key), expected.get(key)):
            raise ProspectiveAnchorConflictError(f"attempt timestamp conflict: {key}")
    for key in (
        "coverage_snapshot",
        "missing_sources",
        "source_timestamps",
        "source_provenance",
        "frozen_inputs",
    ):
        if not _same_json(existing.get(key), expected.get(key)):
            raise ProspectiveAnchorConflictError(f"attempt JSON conflict: {key}")


def _verify_event(existing: Mapping[str, Any], expected: Mapping[str, Any]) -> None:
    for key in (
        "schema_version",
        "event_kind",
        "event_type",
        "symbol",
        "direction",
        "source_side",
        "timeframe",
        "setup_key",
        "event_fingerprint",
        "strategy_version",
        "code_version",
        "capture_stage",
        "delivery_status",
    ):
        _assert_equal(
            key,
            str(existing.get(key) or "").strip(),
            str(expected.get(key) or "").strip(),
        )
    if not _same_time(existing.get("alert_time_utc"), expected.get("alert_time_utc")):
        raise ProspectiveAnchorConflictError("event alert_time_utc conflict")
    for key in (
        "score",
        "current_price",
        "target_price",
        "initial_target_distance_pct",
    ):
        if not _same_float(existing.get(key), expected.get(key)):
            raise ProspectiveAnchorConflictError(f"event numeric conflict: {key}")
    for key in ("categories", "engine_snapshot"):
        if not _same_json(existing.get(key), expected.get(key)):
            raise ProspectiveAnchorConflictError(f"event JSON conflict: {key}")


def _verify_slot(
    existing: Mapping[str, Any],
    expected: Mapping[str, Any],
    *,
    event_ids: Mapping[str, int],
    event_fingerprints: Mapping[str, str],
) -> None:
    for key in (
        "sampler_version",
        "coverage_policy_version",
        "symbol",
        "interval_minutes",
        "feature_bundle_policy_version",
        "feature_bundle_sha256",
        "input_fingerprint",
    ):
        _assert_equal(key, str(existing.get(key)).strip(), str(expected.get(key)).strip())
    for key in (
        "source_candle_open_utc",
        "source_candle_close_utc",
        "base_eligible_at_utc",
        "expires_at_utc",
        "decision_time_utc",
    ):
        if not _same_time(existing.get(key), expected.get(key)):
            raise ProspectiveAnchorConflictError(f"slot timestamp conflict: {key}")
    for key in (
        "coverage_snapshot",
        "source_timestamps",
        "source_provenance",
        "frozen_inputs",
        "decision_feature_bundle",
    ):
        if not _same_json(existing.get(key), expected.get(key)):
            raise ProspectiveAnchorConflictError(f"slot JSON conflict: {key}")
    for direction, column in (("LONG", "long_event_id"), ("SHORT", "short_event_id")):
        _assert_equal(column, int(existing.get(column)), int(event_ids[direction]))
        fingerprint_column = f"{direction.lower()}_event_fingerprint"
        if fingerprint_column in existing:
            _assert_equal(
                fingerprint_column,
                str(existing.get(fingerprint_column) or "").strip(),
                event_fingerprints[direction],
            )


class ProspectiveAnchorStore:
    """Read source evidence and atomically persist neutral anchor pairs."""

    def __init__(
        self,
        *,
        database_url: Optional[str] = None,
        connection_factory: Optional[Callable[[], Any]] = None,
        flow_timestamp_mode: Optional[str] = None,
    ) -> None:
        self.database_url = str(
            database_url if database_url is not None else research_event_store.database_url()
        ).strip()
        self._connection_factory = connection_factory
        mode = str(
            flow_timestamp_mode
            or os.getenv("COINGLASS_CVD_TIMESTAMP_MODE", "open")
        ).strip().lower()
        if mode not in {"open", "close"}:
            raise ValueError("flow_timestamp_mode must be open or close")
        self.flow_timestamp_mode = mode

    def _connect(self) -> Any:
        if self._connection_factory is not None:
            return self._connection_factory()
        if not self.database_url:
            raise ProspectiveAnchorError(
                "prospective anchors require RESEARCH_DATABASE_URL or the "
                "explicit primary-database research opt-in"
            )
        if psycopg is None:
            raise ProspectiveAnchorError("psycopg is unavailable")
        return psycopg.connect(
            self.database_url,
            row_factory=dict_row,
            connect_timeout=5,
            options="-c statement_timeout=15000 -c lock_timeout=3000",
        )

    def status(self) -> Dict[str, Any]:
        return {
            "configured": bool(self.database_url or self._connection_factory),
            "sampler_version": anchors.SAMPLER_VERSION,
            "coverage_policy_version": COVERAGE_POLICY_VERSION,
            "first_touch_method_version": FIRST_TOUCH_METHOD_VERSION,
            "flow_timestamp_mode": self.flow_timestamp_mode,
            "schema_auto_create": False,
            "atomic_pair_transaction": True,
            "idempotent_conflict_verification": True,
            "telegram_alerts": 0,
            "live_delivery_allowed": False,
        }

    def load_coverage(
        self,
        *,
        symbols: Sequence[str],
        as_of_utc: Any,
    ) -> Dict[str, Dict[str, Any]]:
        normalized = tuple(dict.fromkeys(_symbol(item) for item in symbols))
        coverage: Dict[str, Dict[str, Any]] = {
            symbol: {
                "symbol": symbol,
                "eligible": False,
                "failed_gates": [],
                "coverage_policy_version": COVERAGE_POLICY_VERSION,
                "method_version": FIRST_TOUCH_METHOD_VERSION,
                "replay_version": research_historical_replay.REPLAY_VERSION,
                "coverage_scope_version": (
                    research_historical_replay.COVERAGE_SCOPE_VERSION
                ),
                "movement_width_calibration_version": (
                    research_session_width.CALIBRATION_VERSION
                ),
                "canonical_price_method_version": (
                    canonical_price_path.METHOD_VERSION
                ),
                "canonical_price_provenance_version": (
                    canonical_price_path.PRICE_PROVENANCE_VERSION
                ),
                "as_of_utc": _iso(as_of_utc),
                "horizons": {},
            }
            for symbol in normalized
        }
        with self._connect() as conn:
            run_row = conn.execute(
                _COVERAGE_SQL,
                {
                    "method_version": FIRST_TOUCH_METHOD_VERSION,
                    "replay_version": research_historical_replay.REPLAY_VERSION,
                    "calibration_version": research_session_width.CALIBRATION_VERSION,
                    "price_method_version": canonical_price_path.METHOD_VERSION,
                    "price_provenance_version": canonical_price_path.PRICE_PROVENANCE_VERSION,
                    "coverage_scope_version": (
                        research_historical_replay.COVERAGE_SCOPE_VERSION
                    ),
                    "as_of_utc": _utc(as_of_utc),
                },
            ).fetchone()
        run_coverage = {}
        if run_row:
            parsed = _json_value(run_row.get("coverage"))
            if isinstance(parsed, Mapping):
                run_coverage = parsed
            for snapshot in coverage.values():
                snapshot["replay_run_id"] = int(run_row["replay_run_id"])
                snapshot["replay_completed_at_utc"] = _iso(
                    run_row["completed_at_utc"]
                )
        for symbol, snapshot in coverage.items():
            failures: list[str] = []
            for horizon in _HORIZONS:
                row = run_coverage.get(f"{symbol}:{horizon}", {})
                if not isinstance(row, Mapping):
                    row = {}
                count = _strict_nonnegative_int(row.get("outcomes"))
                dates = _strict_nonnegative_int(row.get("utc_dates"))
                first = row.get("first_observation_utc")
                last = row.get("last_observation_utc")
                first_time = last_time = None
                try:
                    first_time = _utc(first)
                    last_time = _utc(last)
                    span = _strict_nonnegative_finite_number(max(
                        0.0,
                        (last_time - first_time).total_seconds() / 3600.0,
                    ))
                except (TypeError, ValueError, OverflowError):
                    span = None
                horizon_failures: list[str] = []
                if count is None:
                    horizon_failures.append("outcomes_type")
                elif count < _MIN_ANCHORS:
                    horizon_failures.append("minimum_anchors")
                if dates is None:
                    horizon_failures.append("utc_dates_type")
                elif dates < _MIN_UTC_DATES:
                    horizon_failures.append("minimum_utc_dates")
                if span is None:
                    horizon_failures.append("span_hours_type")
                elif span < _MIN_SPAN_HOURS:
                    horizon_failures.append("minimum_span_hours")
                snapshot["horizons"][str(horizon)] = {
                    "eligible": not horizon_failures,
                    "anchors": count if count is not None else 0,
                    "utc_dates": dates if dates is not None else 0,
                    "span_hours": span if span is not None else 0.0,
                    "min_anchor_time_utc": (
                        _iso(first_time)
                        if first_time is not None
                        else None
                    ),
                    "max_anchor_time_utc": (
                        _iso(last_time)
                        if last_time is not None
                        else None
                    ),
                    "failed_gates": horizon_failures,
                }
                failures.extend(
                    f"{horizon}m_{failure}" for failure in horizon_failures
                )
            snapshot["failed_gates"] = sorted(set(failures))
            snapshot["eligible"] = not failures
        return coverage

    def existing_captured_symbols(
        self, *, symbols: Sequence[str], slot_open_utc: Any
    ) -> Dict[str, str]:
        normalized = tuple(dict.fromkeys(_symbol(item) for item in symbols))
        if not normalized:
            return {}
        with self._connect() as conn:
            rows = _fetchall(
                conn.execute(
                    _EXISTING_SLOTS_SQL,
                    {
                        "sampler_version": anchors.SAMPLER_VERSION,
                        "slot_open_utc": _utc(slot_open_utc),
                        "symbols": list(normalized),
                    },
                )
            )
        return {
            _symbol(row["symbol"]): str(row["input_fingerprint"]).strip()
            for row in rows
        }

    def load_source_inputs(
        self,
        *,
        symbols: Sequence[str],
        slot_open_utc: Any,
        checked_at_utc: Any,
        official_prices_by_symbol: Mapping[str, Mapping[str, Any]],
    ) -> SourceInputBatch:
        normalized = tuple(dict.fromkeys(_symbol(item) for item in symbols))
        if not normalized:
            checked = _utc(checked_at_utc)
            return SourceInputBatch({}, checked, checked)
        opened = _utc(slot_open_utc)
        closed = opened + timedelta(minutes=anchors.INTERVAL_MINUTES)
        eligible_at = closed + timedelta(minutes=anchors.GRACE_MINUTES)
        checked = _utc(checked_at_utc)
        flow_stamp = opened if self.flow_timestamp_mode == "open" else closed
        params = {
            "symbols": list(normalized),
            "eligible_at_utc": eligible_at,
            "checked_at_utc": checked,
            "source_candle_time_utc": flow_stamp,
        }
        with self._connect() as conn:
            oi_rows = _fetchall(conn.execute(_OI_INPUT_SQL, params))
            futures_rows = _fetchall(
                conn.execute(
                    _FLOW_INPUT_SQL.format(table="futures_taker_history"),
                    params,
                )
            )
            spot_rows = _fetchall(
                conn.execute(
                    _FLOW_INPUT_SQL.format(table="spot_taker_history"),
                    params,
                )
            )
            max_pain_by_symbol = {
                symbol: research_max_pain_archive.load_prior_only_features_from_connection(
                    conn,
                    symbol,
                    checked,
                )
                for symbol in normalized
            }
            read_completed_row = _fetchone(
                conn.execute(
                    "SELECT clock_timestamp() AS read_completed_at_utc",
                    (),
                )
            )
            if not read_completed_row or read_completed_row.get(
                "read_completed_at_utc"
            ) is None:
                raise ProspectiveAnchorError(
                    "database did not return the prospective source-read completion time"
                )
            read_completed = _utc(
                read_completed_row["read_completed_at_utc"]
            )
            if read_completed < checked:
                raise ProspectiveAnchorError(
                    "prospective source-read completion predates its cutoff"
                )
        oi_by_symbol = {_symbol(row["symbol"]): row for row in oi_rows}
        futures_by_symbol = {_symbol(row["symbol"]): row for row in futures_rows}
        spot_by_symbol = {_symbol(row["symbol"]): row for row in spot_rows}
        official = {
            _symbol(symbol): dict(value)
            for symbol, value in official_prices_by_symbol.items()
            if isinstance(value, Mapping)
        }
        output: Dict[str, Dict[str, Any]] = {}
        for symbol in normalized:
            families: Dict[str, Any] = {}
            if symbol in official:
                families["official_price"] = official[symbol]
            if symbol in oi_by_symbol:
                families["price_oi"] = self._price_oi_family(
                    oi_by_symbol[symbol], read_completed
                )
            if symbol in futures_by_symbol:
                families["futures_cvd"] = self._flow_family(
                    futures_by_symbol[symbol], "futures"
                )
            if symbol in spot_by_symbol:
                families["spot_cvd"] = self._flow_family(
                    spot_by_symbol[symbol], "spot"
                )
            # Max Pain is optional for the neutral anchor itself.  Its exact
            # decision-time result (including an explicit UNEVALUABLE state)
            # is nevertheless frozen into the slot so a later archive insert
            # cannot rewrite formulas that do consume it.
            families["max_pain"] = dict(max_pain_by_symbol.get(symbol) or {})
            output[symbol] = families
        return SourceInputBatch(output, checked, read_completed)

    def build_decision_feature_bundles(
        self,
        *,
        source_inputs_by_symbol: Mapping[str, Mapping[str, Any]],
        decision_time_utc: Any,
        code_version: Optional[str] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """Materialize each outcome-blind feature bundle once at its cutoff.

        Only validated immutable v3/v4 slots may supply prior history.  The
        current source payload is appended in memory, the pure feature builder
        runs once, and the resulting flat values/session-width contexts are
        hashed before any DECISION_SAMPLE event is published.
        """
        decision_time = _utc(decision_time_utc)
        symbols = sorted(
            {
                _symbol(symbol)
                for symbol, values in source_inputs_by_symbol.items()
                if isinstance(values, Mapping)
            }
        )
        if not symbols:
            return {}
        history_start = decision_time - timedelta(
            days=research_feature_matrix.HISTORICAL_BASELINE_DAYS,
            minutes=(
                max(research_feature_matrix.CORE_WINDOWS_MINUTES)
                + research_feature_matrix.MAX_POINT_AGE_MINUTES
            ),
        )
        with self._connect() as conn:
            (
                price_rows,
                oi_rows,
                futures_rows,
                spot_rows,
                _prior_max_pain,
            ) = research_feature_matrix._load_prospective_frozen_rows(
                conn,
                symbols=symbols,
                start=history_start,
                end=decision_time,
                sampler_versions=_PRIOR_FROZEN_SOURCE_SAMPLERS,
                as_of_created_utc=decision_time,
            )

        prior_price_rows = list(price_rows)
        all_price_rows = list(price_rows)
        all_oi_rows = list(oi_rows)
        all_futures_rows = list(futures_rows)
        all_spot_rows = list(spot_rows)
        frozen_by_symbol: Dict[str, Dict[str, Any]] = {}
        for symbol in symbols:
            source_rows = source_inputs_by_symbol.get(symbol)
            if not isinstance(source_rows, Mapping):
                continue
            frozen, _timestamps, provenance = _frozen_source_payload(source_rows)
            try:
                price_row, oi_row, futures_row, spot_row = (
                    research_feature_matrix.prospective_feature_series_rows(
                        symbol=symbol,
                        decision_time_utc=decision_time,
                        frozen_inputs=frozen,
                        source_provenance=provenance,
                        sampler_version=anchors.SAMPLER_VERSION,
                    )
                )
            except (TypeError, ValueError, OverflowError):
                continue
            frozen_by_symbol[symbol] = frozen
            all_price_rows.append(price_row)
            all_oi_rows.append(oi_row)
            all_futures_rows.append(futures_row)
            all_spot_rows.append(spot_row)

        events: list[Dict[str, Any]] = []
        max_pain_by_event_id: Dict[int, Dict[str, Any]] = {}
        event_sequence = 0
        for symbol in symbols:
            frozen = frozen_by_symbol.get(symbol)
            if not frozen:
                continue
            current_price = _mapping(frozen.get("official_price")).get("price")
            max_pain = _mapping(frozen.get("max_pain"))
            for direction in anchors.DIRECTIONS:
                for horizon in _HORIZONS:
                    event_sequence += 1
                    event_id = -event_sequence
                    events.append(
                        {
                            "event_id": event_id,
                            "event_kind": "DECISION_SAMPLE",
                            "alert_time_utc": decision_time,
                            "symbol": symbol,
                            "direction": direction,
                            "source_side": "RAW_NEUTRAL",
                            "timeframe": anchors.TIMEFRAME,
                            "event_type": anchors.EVENT_TYPE,
                            "score": None,
                            "current_price": current_price,
                            "target_price": None,
                            "initial_target_distance_pct": None,
                            "categories": [],
                            "setup_key": None,
                            "strategy_version": "formula-prospective-neutral-v4",
                            "code_version": code_version,
                            "engine_snapshot": {},
                            "horizon_minutes": horizon,
                        }
                    )
                    max_pain_by_event_id[event_id] = dict(max_pain)

        if not events:
            return {}
        feature_rows = research_feature_matrix.build_feature_rows(
            events,
            price_oi_rows=all_price_rows,
            oi_rows=all_oi_rows,
            futures_rows=all_futures_rows,
            spot_rows=all_spot_rows,
            prior_events=[],
            windows_minutes=research_feature_matrix.CORE_WINDOWS_MINUTES,
            max_pain_by_event_id=max_pain_by_event_id,
        )
        rows_by_symbol: Dict[str, list[Dict[str, Any]]] = {
            symbol: [] for symbol in symbols
        }
        for row in feature_rows:
            event = row.get("event") if isinstance(row, Mapping) else None
            symbol = str(
                event.get("symbol") if isinstance(event, Mapping) else ""
            ).strip().upper()
            if symbol in rows_by_symbol:
                rows_by_symbol[symbol].append(dict(row))

        bundles: Dict[str, Dict[str, Any]] = {}
        for symbol, rows in rows_by_symbol.items():
            if not rows:
                continue
            try:
                bundle = research_prospective_feature_freeze.build_feature_bundle(
                    decision_time_utc=decision_time,
                    symbol=symbol,
                    feature_rows=rows,
                    source_series_manifest=_source_series_manifest(
                        prior_price_rows, symbol=symbol
                    ),
                )
                digest = (
                    research_prospective_feature_freeze.compute_feature_bundle_sha256(
                        bundle
                    )
                )
            except (TypeError, ValueError, OverflowError):
                continue
            bundles[symbol] = {
                "feature_bundle_policy_version": (
                    research_prospective_feature_freeze.FEATURE_POLICY_VERSION
                ),
                "decision_feature_bundle": bundle,
                "feature_bundle_sha256": digest,
            }
        return bundles

    @staticmethod
    def _price_oi_family(row: Mapping[str, Any], checked: datetime) -> Dict[str, Any]:
        return {
            "symbol": _symbol(row["symbol"]),
            "observation_time_utc": row["collected_at"],
            # The source table has no inserted_at column.  The only exact
            # availability fact is that this synchronous read succeeded now.
            "refresh_completed_at_utc": checked,
            "refresh_time_semantics": "DATABASE_READ_CONFIRMATION",
            "source_table": "oi_regime_snapshots",
            "source_record_id": row["id"],
            "price_source": row.get("price_source"),
            "oi_source": row.get("oi_source"),
            "quality_status": row.get("data_quality_status"),
            "price_close": row.get("price"),
            "oi_close_usd": row.get("open_interest_usd"),
            "price_change_pct": row.get("price_change_pct"),
            "oi_change_pct": row.get("oi_change_pct"),
            "price_fetched_at_utc": row.get("price_fetched_at"),
            "oi_fetched_at_utc": row.get("oi_fetched_at"),
            "time_gap_seconds": row.get("time_gap_seconds"),
        }

    def _flow_family(self, row: Mapping[str, Any], market: str) -> Dict[str, Any]:
        expected_source = f"coinglass_{market}_aggregated_cvd"
        structurally_valid = (
            str(row.get("source") or "").strip().lower() == expected_source
            and all(
                row.get(key) is not None
                for key in (
                    "buy_volume_usd",
                    "sell_volume_usd",
                    "api_cum_vol_delta_usd",
                    "continuous_cum_vol_delta_usd",
                    "imported_at",
                )
            )
        )
        return {
            "symbol": _symbol(row["symbol"]),
            "source_candle_time_utc": row["candle_time"],
            "refresh_completed_at_utc": row.get("imported_at"),
            "refresh_time_semantics": "SOURCE_ROW_IMPORTED_AT",
            "source": row.get("source"),
            "quality_status": "PASS" if structurally_valid else "FAILED",
            "quality_status_basis": "ADAPTER_STRUCTURAL_VALIDATION",
            "exchange_list": row.get("exchange_list"),
            "candle_timestamp_mode": self.flow_timestamp_mode,
            "buy_volume_usd": row.get("buy_volume_usd"),
            "sell_volume_usd": row.get("sell_volume_usd"),
            "api_cum_vol_delta_usd": row.get("api_cum_vol_delta_usd"),
            "continuous_cum_vol_delta_usd": row.get(
                "continuous_cum_vol_delta_usd"
            ),
        }

    def persist_bundle(self, bundle: Mapping[str, Any]) -> PersistResult:
        if not bundle.get("atomic_transaction_required"):
            raise ValueError("prospective persistence requires an atomic bundle")
        if bundle.get("live_delivery_allowed") is not False:
            raise ValueError("prospective anchor bundle must forbid live delivery")
        self._validate_v4_persistence_bundle(bundle)
        with self._connect() as conn:
            with conn.transaction():
                return self._persist_bundle_with_connection(conn, bundle)

    @staticmethod
    def _validate_v4_persistence_bundle(bundle: Mapping[str, Any]) -> None:
        """Reject a mutated v4 bundle before any audit/event row is written."""
        attempt = bundle.get("attempt")
        if not isinstance(attempt, Mapping):
            raise ValueError("prospective bundle is missing its audit attempt")
        if str(attempt.get("sampler_version") or "") != anchors.SAMPLER_VERSION:
            return
        if str(attempt.get("evaluation_status") or "") != anchors.EVALUABLE:
            return
        slot = bundle.get("slot")
        if not isinstance(slot, Mapping):
            raise ValueError("evaluable sampler-v4 attempt is missing its slot")
        policy = str(slot.get("feature_bundle_policy_version") or "")
        digest = str(slot.get("feature_bundle_sha256") or "").strip().lower()
        if policy != research_prospective_feature_freeze.FEATURE_POLICY_VERSION:
            raise ValueError("sampler-v4 feature bundle policy is incompatible")
        for field in (
            "feature_bundle_policy_version",
            "feature_bundle_sha256",
            "input_fingerprint",
            "symbol",
            "decision_time_utc",
        ):
            left = attempt.get(field)
            right = slot.get(field)
            if field == "decision_time_utc":
                equal = _same_time(left, right)
            else:
                equal = str(left or "").strip() == str(right or "").strip()
            if not equal:
                raise ValueError(f"sampler-v4 attempt/slot {field} mismatch")
        for field in (
            "coverage_snapshot",
            "source_timestamps",
            "source_provenance",
            "frozen_inputs",
        ):
            if not _same_json(attempt.get(field), slot.get(field)):
                raise ValueError(f"sampler-v4 attempt/slot {field} mismatch")
        for record, evaluation_status, label in (
            (attempt, anchors.EVALUABLE, "attempt"),
            (slot, anchors.EVALUABLE, "slot"),
        ):
            expected_input_fingerprint = anchors.compute_input_fingerprint(
                sampler_version=record.get("sampler_version"),
                coverage_policy_version=record.get("coverage_policy_version"),
                coverage_snapshot=record.get("coverage_snapshot"),
                symbol=record.get("symbol"),
                source_candle_open_utc=record.get("source_candle_open_utc"),
                source_candle_close_utc=record.get("source_candle_close_utc"),
                base_eligible_at_utc=record.get("base_eligible_at_utc"),
                expires_at_utc=record.get("expires_at_utc"),
                evaluation_status=evaluation_status,
                decision_time_utc=record.get("decision_time_utc"),
                source_timestamps=record.get("source_timestamps"),
                source_provenance=record.get("source_provenance"),
                frozen_inputs=record.get("frozen_inputs"),
                feature_bundle_policy_version=record.get(
                    "feature_bundle_policy_version"
                ),
                feature_bundle_sha256=record.get("feature_bundle_sha256"),
            )
            if expected_input_fingerprint != str(
                record.get("input_fingerprint") or ""
            ).strip():
                raise ValueError(
                    f"sampler-v4 {label} input fingerprint mismatch"
                )
        decision_bundle = slot.get("decision_feature_bundle")
        valid, reason = research_prospective_feature_freeze.validate_feature_bundle(
            decision_bundle,
            expected_sha256=digest,
            expected_symbol=slot.get("symbol"),
            expected_decision_time_utc=slot.get("decision_time_utc"),
        )
        if not valid:
            raise ValueError(f"sampler-v4 feature bundle rejected: {reason}")
        event_persistence = bundle.get("event_persistence") or ()
        if len(event_persistence) != 2:
            raise ValueError("sampler-v4 requires exactly two silent events")
        for item in event_persistence:
            event = item.get("event") if isinstance(item, Mapping) else None
            snapshot = getattr(event, "engine_snapshot", None)
            anchor = (
                snapshot.get("prospective_anchor")
                if isinstance(snapshot, Mapping)
                else None
            )
            if not isinstance(anchor, Mapping):
                raise ValueError("sampler-v4 event lacks its anchor reference")
            if "decision_feature_bundle" in anchor:
                raise ValueError("sampler-v4 event duplicates the slot bundle")
            for field, expected in (
                ("feature_bundle_policy_version", policy),
                ("feature_bundle_sha256", digest),
                ("input_fingerprint", str(slot.get("input_fingerprint") or "")),
            ):
                if str(anchor.get(field) or "").strip() != expected:
                    raise ValueError(f"sampler-v4 event {field} mismatch")

    def _persist_bundle_with_connection(
        self, conn: Any, bundle: Mapping[str, Any]
    ) -> PersistResult:
        attempt = bundle.get("attempt")
        if not isinstance(attempt, Mapping):
            raise ValueError("prospective bundle is missing its audit attempt")
        attempt_params = _attempt_params(attempt)
        inserted_attempt = _fetchone(conn.execute(_ATTEMPT_INSERT_SQL, attempt_params))
        idempotent = inserted_attempt is None
        if inserted_attempt is None:
            existing_attempt = _fetchone(
                conn.execute(
                    "SELECT * FROM research_prospective_anchor_attempts "
                    "WHERE attempt_fingerprint=%(attempt_fingerprint)s FOR UPDATE",
                    {"attempt_fingerprint": attempt["attempt_fingerprint"]},
                )
            )
            if existing_attempt is None:
                raise ProspectiveAnchorConflictError(
                    "attempt conflict returned no existing audit row"
                )
            _verify_attempt(existing_attempt, attempt)
            attempt_id = int(existing_attempt["attempt_id"])
        else:
            attempt_id = int(inserted_attempt["attempt_id"])

        event_persistence = bundle.get("event_persistence") or ()
        slot = bundle.get("slot")
        if not event_persistence and slot is None:
            return PersistResult(
                symbol=_symbol(attempt["symbol"]),
                evaluation_status=str(attempt["evaluation_status"]),
                attempt_id=attempt_id,
                anchor_slot_id=None,
                long_event_id=None,
                short_event_id=None,
                idempotent=idempotent,
            )
        if not isinstance(slot, Mapping) or len(event_persistence) != 2:
            raise ValueError("evaluable anchor requires two events and one slot")
        by_direction = {
            str(item["event"].direction).upper(): item
            for item in event_persistence
        }
        if set(by_direction) != set(anchors.DIRECTIONS):
            raise ValueError("evaluable anchor pair must contain LONG and SHORT")
        if any(
            str(item.get("capture_stage") or "").upper()
            != "SILENT_NEUTRAL_ANCHOR"
            or str(item.get("delivery_status") or "").upper()
            != "NOT_APPLICABLE"
            for item in by_direction.values()
        ):
            raise ValueError("prospective event pair is not silent/non-deliverable")

        event_ids: Dict[str, int] = {}
        event_fingerprints: Dict[str, str] = {}
        for direction in anchors.DIRECTIONS:
            item = by_direction[direction]
            event = item["event"]
            serialized = research_event_store.serialize_event(
                event,
                capture_stage=item["capture_stage"],
                delivery_status=item["delivery_status"],
            )
            event_fingerprints[direction] = str(
                serialized["event_fingerprint"]
            ).strip()
            inserted_event = _fetchone(
                conn.execute(_EVENT_INSERT_SQL, serialized)
            )
            if inserted_event is None:
                existing_event = _fetchone(
                    conn.execute(
                        "SELECT * FROM research_events "
                        "WHERE event_fingerprint=%(event_fingerprint)s FOR UPDATE",
                        {"event_fingerprint": serialized["event_fingerprint"]},
                    )
                )
                if existing_event is None:
                    raise ProspectiveAnchorConflictError(
                        f"{direction} event conflict returned no row"
                    )
                _verify_event(existing_event, serialized)
                event_ids[direction] = int(existing_event["event_id"])
                idempotent = True
            else:
                event_ids[direction] = int(inserted_event["event_id"])

        slot_params = _slot_params(slot, event_ids)
        inserted_slot = _fetchone(conn.execute(_SLOT_INSERT_SQL, slot_params))
        if inserted_slot is None:
            existing_slot = _fetchone(
                conn.execute(
                    """
                    SELECT slot.*,
                           long_event.event_fingerprint AS long_event_fingerprint,
                           short_event.event_fingerprint AS short_event_fingerprint
                      FROM research_prospective_anchor_slots slot
                      JOIN research_events long_event
                        ON long_event.event_id=slot.long_event_id
                      JOIN research_events short_event
                        ON short_event.event_id=slot.short_event_id
                     WHERE slot.sampler_version=%(sampler_version)s
                       AND slot.symbol=%(symbol)s
                       AND slot.source_candle_open_utc=%(source_candle_open_utc)s
                     FOR UPDATE OF slot
                    """,
                    {
                        "sampler_version": slot["sampler_version"],
                        "symbol": slot["symbol"],
                        "source_candle_open_utc": slot[
                            "source_candle_open_utc"
                        ],
                    },
                )
            )
            if existing_slot is None:
                raise ProspectiveAnchorConflictError(
                    "slot conflict returned no authoritative row"
                )
            _verify_slot(
                existing_slot,
                slot,
                event_ids=event_ids,
                event_fingerprints=event_fingerprints,
            )
            anchor_slot_id = int(existing_slot["anchor_slot_id"])
            idempotent = True
        else:
            anchor_slot_id = int(inserted_slot["anchor_slot_id"])
        return PersistResult(
            symbol=_symbol(attempt["symbol"]),
            evaluation_status=str(attempt["evaluation_status"]),
            attempt_id=attempt_id,
            anchor_slot_id=anchor_slot_id,
            long_event_id=event_ids["LONG"],
            short_event_id=event_ids["SHORT"],
            idempotent=idempotent,
        )


class ProspectiveAnchorService:
    """One-pass orchestration surface for an external minute scheduler."""

    def __init__(
        self,
        store: ProspectiveAnchorStore,
        *,
        symbols: Sequence[str] = DEFAULT_SYMBOLS,
        coverage_policy_version: str = COVERAGE_POLICY_VERSION,
        strategy_version: Optional[str] = None,
        code_version: Optional[str] = None,
    ) -> None:
        self.store = store
        self.symbols = tuple(dict.fromkeys(_symbol(item) for item in symbols))
        self.coverage_policy_version = str(coverage_policy_version).strip()
        if not self.coverage_policy_version:
            raise ValueError("coverage_policy_version is required")
        self.strategy_version = strategy_version
        self.code_version = code_version

    def run_once(
        self,
        *,
        now: Any,
        official_prices_by_symbol: Mapping[str, Mapping[str, Any]],
        slot_open_utc: Any = None,
        symbols: Optional[Sequence[str]] = None,
    ) -> SamplingRun:
        checked = _utc(now)
        active_symbols = (
            self.symbols
            if symbols is None
            else tuple(dict.fromkeys(_symbol(item) for item in symbols))
        )
        if not active_symbols:
            raise ValueError("prospective sampling symbols are required")
        unknown_symbols = sorted(set(active_symbols) - set(self.symbols))
        if unknown_symbols:
            raise ValueError(
                "prospective sampling symbols exceed the configured scope: "
                + ",".join(unknown_symbols)
            )
        opened = (
            _utc(slot_open_utc)
            if slot_open_utc is not None
            else anchors.latest_due_slot_open(checked)
        )
        coverage = self.store.load_coverage(
            symbols=active_symbols,
            as_of_utc=checked,
        )
        existing = self.store.existing_captured_symbols(
            symbols=active_symbols,
            slot_open_utc=opened,
        )
        pending_symbols = tuple(
            symbol for symbol in active_symbols if symbol not in existing
        )
        pending_coverage = {
            symbol: coverage[symbol] for symbol in pending_symbols
        }
        source_batch = self.store.load_source_inputs(
            symbols=pending_symbols,
            slot_open_utc=opened,
            checked_at_utc=checked,
            official_prices_by_symbol=official_prices_by_symbol,
        )
        decision_checked = max(checked, _utc(source_batch.read_completed_at_utc))
        feature_bundles = self.store.build_decision_feature_bundles(
            source_inputs_by_symbol=source_batch.inputs_by_symbol,
            decision_time_utc=decision_checked,
            code_version=self.code_version,
        )
        batch = anchors.build_anchor_batch(
            now=decision_checked,
            slot_open_utc=opened,
            coverage_by_symbol=pending_coverage,
            source_inputs_by_symbol=source_batch.inputs_by_symbol,
            feature_bundles_by_symbol=feature_bundles,
            coverage_policy_version=self.coverage_policy_version,
            strategy_version=self.strategy_version,
            code_version=self.code_version,
        )
        persisted: list[PersistResult] = []
        conflicts: list[str] = []
        for bundle in batch.atomic_persistence_bundles():
            try:
                persisted.append(self.store.persist_bundle(bundle))
            except ProspectiveAnchorConflictError as exc:
                conflicts.append(f"{bundle['attempt']['symbol']}:{exc}")
        return SamplingRun(
            sampler_version=anchors.SAMPLER_VERSION,
            slot_open_utc=opened,
            checked_at_utc=decision_checked,
            existing_symbols=tuple(sorted(existing)),
            batch=batch,
            persisted=tuple(persisted),
            conflicts=tuple(conflicts),
        )

    async def run_once_async(self, **kwargs: Any) -> SamplingRun:
        """Run the blocking PostgreSQL pass without blocking an async bot loop."""
        return await asyncio.to_thread(self.run_once, **kwargs)


def seconds_until_next_scheduler_check(
    now: Any, *, cadence_seconds: int = 60
) -> float:
    """Return a bounded wall-clock-aligned delay for an external loop."""
    cadence = max(5, min(int(cadence_seconds), 300))
    current = _utc(now).timestamp()
    return max(0.05, cadence - (current % cadence))
