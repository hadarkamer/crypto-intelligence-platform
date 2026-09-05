"""Canonical spot-path enrichment with fail-closed prospective evidence.

The worker is observational: it never changes alert logic. Once an alert has
aged into a configured horizon, one canonical spot one-minute path is
fetched and converted into fixed-horizon return, MFE, MAE, speed and optional
target-progress measurements.  A separate additive v6 label records the first
touch of a frozen favorable width with zero dwell and conservative pre-touch
MAE.  Eligible delivered Alerts and authorized prospective Decision Samples
that match a Shadow formula are polled while their relevant horizon is still
open, so a verified first touch can be frozen without waiting for the horizon
to close. Current prospective Shadow labels require the exact decision-time
per-horizon session and movement-width bundle; no static fallback or later
reconstruction is allowed. Exact Stage-4 signal snapshots with a matching
completed projection receipt are admitted only after a fixed horizon closes;
they never enter First-Touch processing. Their metrics are post-decision path
measurements relative to the frozen archive input price, not trade-entry
returns, and use a distinct outcome method. Existing legacy rows and method
versions remain available for audit only.

Binance Spot USDT is the default route. HYPE is explicitly routed to the
Hyperliquid HYPE/USDT spot market. Historical candles may be imported from
those exchange APIs as long as their provenance and quality remain attached.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import math
import os
import re
import time
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

try:
    import psycopg
    from psycopg.conninfo import conninfo_to_dict
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    conninfo_to_dict = None
    dict_row = None

import binance_spot_price_path
import canonical_price_path
import research_database_timeout
import research_feature_matrix
import research_no_dwell_outcome
import research_session_width


_TRUE = {"1", "true", "yes", "on"}
_ENABLED = os.getenv("RESEARCH_OUTCOME_ENRICHMENT_ENABLED", "").strip().lower() in _TRUE
_HORIZONS = (60, 240, 720, 1440)
_POLL_SECONDS = max(60, int(os.getenv("RESEARCH_OUTCOME_POLL_SECONDS", "60")))
_OPEN_FIRST_TOUCH_EVENT_LIMIT = max(
    1,
    min(200, int(os.getenv("RESEARCH_OPEN_FIRST_TOUCH_EVENT_LIMIT", "32"))),
)
_SLOT_THRESHOLD_AUTHORITY_BATCH_SIZE = 50
_STAGE4_DUE_CANDIDATE_PAGE_SIZE = 256
_STAGE4_DUE_SCAN_STATE_KEY = "STAGE4_SIGNAL_DUE_V1"
_STAGE4_DUE_SCAN_STATE_VERSION = "stage4-signal-due-scan-state-v1"
_STAGE4_DUE_SCAN_LOCK_ID = 94837244
_STAGE4_DUE_SCAN_MAX_EVENT_ID = 9223372036854775807
_STAGE4_DUE_RESERVED_QUEUE_DIVISOR = 4
_STAGE4_DUE_SCAN_MAX_PAGES = max(
    1,
    min(16, int(os.getenv("RESEARCH_STAGE4_DUE_SCAN_MAX_PAGES", "4"))),
)
_STAGE4_DUE_SCAN_BUDGET_MS = max(
    30_000,
    min(
        600_000,
        int(os.getenv("RESEARCH_STAGE4_DUE_SCAN_BUDGET_MS", "240000")),
    ),
)
_METHOD_VERSION = canonical_price_path.METHOD_VERSION
_FIRST_TOUCH_METHOD_VERSION = research_no_dwell_outcome.METHOD_VERSION
_STRICT_FROZEN_EVIDENCE_POLICY_VERSION = (
    "prospective-shadow-frozen-decision-features-v1"
)
_STRICT_FROZEN_SNAPSHOT_POLICY_VERSION = (
    "formula-shadow-input-snapshot-v5-frozen-decision-features"
)
_STRICT_SLOT_THRESHOLD_AUTHORITY_VERSION = (
    "prospective-anchor-slot-threshold-authority-v1"
)
_STRICT_FEATURE_BUNDLE_POLICY_VERSION = (
    "prospective-decision-feature-bundle-v1"
)
_STRICT_PROSPECTIVE_SAMPLER_VERSION = (
    "prospective-neutral-anchor-v4-decision-features-frozen"
)
_ALERT_REFERENCE_REJECTION_POLICY_VERSION = "alert-reference-provenance-v1"
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_STAGE4_SIGNAL_OUTCOME_EVENT_TYPES = (
    "MAX_PAIN_CONFIRMATION_STATE",
    "MAGNET_CONFIRMATION_STATE",
    "SILENT_COMBINED_CONFIRMATION_SNAPSHOT",
)
_STAGE4_SIGNAL_SNAPSHOT_CONTRACT_VERSION = "research-signal-snapshot-v1"
_STAGE4_SIGNAL_SNAPSHOT_CAPTURE_STAGE = "SILENT_SIGNAL_SNAPSHOT"
_STAGE4_SIGNAL_SNAPSHOT_STRATEGY_VERSION = "signal-snapshot-v1"
_STAGE4_DERIVED_ADMISSION_POLICY_VERSION = (
    "stage4-signal-completed-projection-derived-admission-v1"
)
_STAGE4_FROZEN_REFERENCE_POLICY_VERSION = (
    "stage4-signal-frozen-archive-price-reference-v1"
)
_STAGE4_OUTCOME_METHOD_VERSION = (
    f"{canonical_price_path.METHOD_VERSION}+stage4-frozen-archive-input-v1"
)
# Frozen source freshness (45m) plus the maximum projection lag (15m).
_STAGE4_MAX_REFERENCE_AGE_SECONDS = (45 + 15) * 60
_STAGE4_OUTCOME_SEMANTICS = (
    "post_decision_path_metrics_relative_to_frozen_archive_input_price;"
    "not_trade_entry_return"
)
_STAGE4_NO_SIGNAL_DATABASE_URL_ENV = (
    "RESEARCH_STAGE4_NO_SIGNAL_OUTCOME_DATABASE_URL"
)
_STAGE4_NO_SIGNAL_CARRIER_CONTRACT_VERSION = (
    "stage4-explicit-no-signal-outcome-carrier-v1"
)
_STAGE4_NO_SIGNAL_OUTCOME_METHOD_VERSION = (
    f"{canonical_price_path.METHOD_VERSION}"
    "+stage4-no-signal-frozen-archive-input-v1"
)
_STAGE4_NO_SIGNAL_ADMISSION_POLICY_VERSION = (
    "stage4-no-signal-completed-projection-evaluable-cell-admission-v1"
)
_STAGE4_NO_SIGNAL_REFERENCE_RECEIPT_VERSION = (
    "stage4-no-signal-frozen-archive-price-reference-v1"
)
_STAGE4_NO_SIGNAL_ABSENCE_BASIS = (
    "COMPLETED_PROJECTION_EVALUABLE_SYMBOL_WITHOUT_SIGNAL"
)
_STAGE4_NO_SIGNAL_PROJECTION_PAGE_SIZE = 64
_STAGE4_NO_SIGNAL_MAX_PROJECTION_PAGES = 4
_STAGE4_NO_SIGNAL_MAX_CELL_ROWS = 32768


class FrozenThresholdPolicyConflict(ValueError):
    """A frozen event/horizon has incompatible decision-time width evidence."""


def _database_url() -> str:
    dedicated = os.getenv("RESEARCH_DATABASE_URL", "").strip()
    use_primary = os.getenv("RESEARCH_USE_PRIMARY_DATABASE", "").strip().lower() in _TRUE
    if dedicated:
        return dedicated
    if use_primary:
        return os.getenv("DATABASE_URL", "").strip()
    return ""


def _stage4_no_signal_database_url() -> str:
    """Return only the dedicated append-only no-signal writer URL."""

    return os.getenv(_STAGE4_NO_SIGNAL_DATABASE_URL_ENV, "").strip()


def _database_target_identity(database_url: str) -> tuple[str, ...]:
    """Return a credential-free, fail-closed PostgreSQL target identity."""

    if conninfo_to_dict is None:
        raise RuntimeError("psycopg conninfo parser is unavailable")
    try:
        values = conninfo_to_dict(database_url)
    except Exception as exc:
        raise RuntimeError("database target configuration is invalid") from exc
    database = str(values.get("dbname") or "").strip()
    service = str(values.get("service") or "").strip()
    if service:
        if not database:
            raise RuntimeError("database target must name an explicit database")
        return (
            "service",
            service,
            str(values.get("servicefile") or "").strip(),
            database,
        )
    host = str(values.get("host") or "").strip()
    hostaddr = str(values.get("hostaddr") or "").strip()
    if not database or not (host or hostaddr):
        raise RuntimeError(
            "database target must name an explicit host and database"
        )
    return (
        "direct",
        host,
        hostaddr,
        str(values.get("port") or "5432").strip(),
        database,
    )


def _assert_stage4_no_signal_database_target(database_url: str) -> None:
    """Require the least-privilege writer URL to target the research DB."""

    source_url = _database_url()
    if not source_url:
        raise RuntimeError("research database target is not configured")
    if _database_target_identity(database_url) != _database_target_identity(
        source_url
    ):
        raise RuntimeError(
            "Stage-4 no-signal writer target differs from the research database"
        )


def _database_connection_options() -> str:
    return (
        "-c statement_timeout="
        f"{research_database_timeout.heavy_statement_timeout_ms()} "
        "-c lock_timeout=1000"
    )


def _stage4_no_signal_connection_options() -> str:
    return (
        f"{_database_connection_options()} "
        "-c idle_in_transaction_session_timeout=30000 "
        "-c search_path=pg_catalog,public -c timezone=UTC "
        "-c DateStyle=ISO,YMD -c IntervalStyle=postgres "
        "-c extra_float_digits=3 -c row_security=on"
    )


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _latest_closed_candle_cutoff(value: Any) -> datetime:
    """Return the inclusive cutoff for the latest fully closed UTC minute."""
    now = _utc(value)
    minute_open = now.replace(second=0, microsecond=0)
    return minute_open - timedelta(milliseconds=1)


def _first_touch_write_is_safe(
    first_touch: Optional[Dict[str, Any]], *, observed_prefix_complete: bool
) -> bool:
    """Forbid terminal labels from a gapped or otherwise partial 1m prefix."""
    if first_touch is None:
        return True
    status = str(first_touch.get("status") or "").upper()
    if status == "PENDING":
        return True
    if status in {"HIT", "MISS"}:
        return bool(observed_prefix_complete)
    return False


def _first_touch_enabled_for_event(event: Mapping[str, Any]) -> bool:
    """Keep Stage-4 signal snapshots on fixed, closed horizons only."""
    direction = str(event.get("direction") or "").strip().upper()
    event_type = str(event.get("event_type") or "").strip().upper()
    return (
        direction in {"LONG", "SHORT"}
        and event_type not in _STAGE4_SIGNAL_OUTCOME_EVENT_TYPES
    )


def _is_stage4_signal_outcome_event(event: Mapping[str, Any]) -> bool:
    event_type = str(event.get("event_type") or "").strip().upper()
    return event_type in _STAGE4_SIGNAL_OUTCOME_EVENT_TYPES


def _outcome_method_version_for_event(event: Mapping[str, Any]) -> str:
    if _is_stage4_signal_outcome_event(event):
        return _STAGE4_OUTCOME_METHOD_VERSION
    return _METHOD_VERSION


def _mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, Mapping) else {}
    return {}


def _finite_number(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _strict_json_number(value: Any) -> Optional[float]:
    """Accept only a finite JSON number, never a string or boolean."""
    if type(value) not in (int, float):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _strict_positive_int(value: Any) -> Optional[int]:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return int(value)


def _strict_sha256(value: Any) -> Optional[str]:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        return None
    return value


def _prospective_anchor_evidence(event: Mapping[str, Any]) -> Mapping[str, Any]:
    engine_snapshot = _mapping(event.get("engine_snapshot"))
    return _mapping(engine_snapshot.get("prospective_anchor"))


def _prospective_sampler_version(event: Mapping[str, Any]) -> str:
    anchor = _prospective_anchor_evidence(event)
    return str(anchor.get("sampler_version") or "").strip()


def _slot_threshold_record(row: Mapping[str, Any]) -> Dict[str, Any]:
    """Normalize one verified slot row into the threshold evidence contract.

    ``load_shadow_feature_rows_by_horizon`` has already validated the
    migration-013 authority view, the event/slot fingerprints and the hashed
    decision-time bundle.  This adapter deliberately carries no formula
    identity: the slot is the sampler-v4 threshold authority, while Formula
    Shadow checks remain formula-evaluation evidence only.
    """
    event = _mapping(row.get("event"))
    label = _mapping(row.get("outcome_label"))
    prospective = _mapping(row.get("prospective_evidence"))
    horizon = _strict_positive_int(label.get("horizon_minutes")) or 0
    event_id = _strict_positive_int(event.get("event_id")) or 0
    evidence_policy = str(
        row.get("decision_input_policy_version") or ""
    ).strip()
    snapshot = {
        "snapshot_policy_version": _STRICT_SLOT_THRESHOLD_AUTHORITY_VERSION,
        "evidence_policy_version": evidence_policy,
        "decision_input_policy_version": evidence_policy,
        "horizon_minutes": horizon,
        "event": dict(event),
        "prospective_evidence": dict(prospective),
        "outcome_window_session": {
            key: label.get(key)
            for key in (
                "session_active_ratio",
                "session_weekend_ratio",
                "session_segments",
                "session_composition",
            )
        },
        "movement_width_reference": (
            dict(label.get("movement_width_reference"))
            if isinstance(label.get("movement_width_reference"), Mapping)
            else {}
        ),
    }
    return {
        "event_id": event_id,
        "horizon_minutes": horizon,
        "threshold_authority_version": (
            _STRICT_SLOT_THRESHOLD_AUTHORITY_VERSION
        ),
        "input_snapshot": snapshot,
        "evidence_policy_version": evidence_policy,
        "prospective_anchor_slot_id": row.get(
            "prospective_anchor_slot_id"
        ),
        "prospective_input_fingerprint": row.get(
            "prospective_input_fingerprint"
        ),
        "feature_bundle_sha256": row.get("feature_bundle_sha256"),
        "authoritative_verified": row.get("authoritative_verified") is True,
    }


def _is_current_frozen_evidence_record(record: Mapping[str, Any]) -> bool:
    """Recognize every marker that makes a row subject to the v4 contract.

    A partially forged row must not evade strict validation by omitting one of
    the duplicate policy fields. If *any* current marker is present, the full
    exact contract is required below.
    """
    snapshot = _mapping(record.get("input_snapshot"))
    evidence = _mapping(snapshot.get("prospective_evidence"))
    return (
        bool(str(record.get("threshold_authority_version") or "").strip())
        or str(record.get("evidence_policy_version") or "").strip()
        == _STRICT_FROZEN_EVIDENCE_POLICY_VERSION
        or str(snapshot.get("evidence_policy_version") or "").strip()
        == _STRICT_FROZEN_EVIDENCE_POLICY_VERSION
        or str(evidence.get("sampler_version") or "").strip()
        == _STRICT_PROSPECTIVE_SAMPLER_VERSION
    )


def _strict_frozen_threshold_policy(
    *,
    event: Mapping[str, Any],
    horizon_minutes: int,
    record: Mapping[str, Any],
) -> tuple[tuple[Any, ...], Dict[str, Any]]:
    """Validate and consume one exact decision-time v4 width/session bundle."""
    horizon = int(horizon_minutes)
    decision_time = _utc(event["alert_time_utc"])
    snapshot = _mapping(record.get("input_snapshot"))
    evidence = _mapping(snapshot.get("prospective_evidence"))

    if str(record.get("evidence_policy_version") or "").strip() != (
        _STRICT_FROZEN_EVIDENCE_POLICY_VERSION
    ):
        raise FrozenThresholdPolicyConflict(
            "current frozen evidence policy is missing or incompatible"
        )
    if str(snapshot.get("evidence_policy_version") or "").strip() != (
        _STRICT_FROZEN_EVIDENCE_POLICY_VERSION
    ):
        raise FrozenThresholdPolicyConflict(
            "current frozen snapshot evidence policy is missing or incompatible"
        )
    authority_version = str(
        record.get("threshold_authority_version") or ""
    ).strip()
    if authority_version and authority_version != (
        _STRICT_SLOT_THRESHOLD_AUTHORITY_VERSION
    ):
        raise FrozenThresholdPolicyConflict(
            "current threshold authority is missing or incompatible"
        )
    expected_snapshot_policy = (
        _STRICT_SLOT_THRESHOLD_AUTHORITY_VERSION
        if authority_version == _STRICT_SLOT_THRESHOLD_AUTHORITY_VERSION
        else _STRICT_FROZEN_SNAPSHOT_POLICY_VERSION
    )
    if str(snapshot.get("snapshot_policy_version") or "").strip() != (
        expected_snapshot_policy
    ):
        raise FrozenThresholdPolicyConflict(
            "current frozen snapshot policy is missing or incompatible"
        )
    if str(snapshot.get("decision_input_policy_version") or "").strip() != (
        _STRICT_FROZEN_EVIDENCE_POLICY_VERSION
    ):
        raise FrozenThresholdPolicyConflict(
            "current frozen decision-input policy is missing or incompatible"
        )
    if record.get("authoritative_verified") is not True:
        raise FrozenThresholdPolicyConflict(
            "current frozen evidence was not authoritatively verified"
        )
    if str(evidence.get("sampler_version") or "").strip() != (
        _STRICT_PROSPECTIVE_SAMPLER_VERSION
    ):
        raise FrozenThresholdPolicyConflict(
            "current frozen evidence is not bound to the exact sampler v4"
        )
    event_anchor = _prospective_anchor_evidence(event)
    event_sampler = str(event_anchor.get("sampler_version") or "").strip()
    if event_sampler != _STRICT_PROSPECTIVE_SAMPLER_VERSION:
        raise FrozenThresholdPolicyConflict(
            "outcome event is not bound to the exact current sampler v4"
        )
    if str(evidence.get("feature_bundle_policy_version") or "").strip() != (
        _STRICT_FEATURE_BUNDLE_POLICY_VERSION
    ):
        raise FrozenThresholdPolicyConflict(
            "current decision feature-bundle policy is missing or incompatible"
        )

    slot_id = _strict_positive_int(evidence.get("anchor_slot_id"))
    stored_slot_id = _strict_positive_int(
        record.get("prospective_anchor_slot_id")
    )
    input_fingerprint = _strict_sha256(evidence.get("input_fingerprint"))
    stored_input_fingerprint = _strict_sha256(
        record.get("prospective_input_fingerprint")
    )
    bundle_sha256 = _strict_sha256(evidence.get("feature_bundle_sha256"))
    stored_bundle_sha256 = _strict_sha256(record.get("feature_bundle_sha256"))
    if slot_id is None or stored_slot_id != slot_id:
        raise FrozenThresholdPolicyConflict(
            "current frozen anchor-slot identity is missing or inconsistent"
        )
    if input_fingerprint is None or stored_input_fingerprint != input_fingerprint:
        raise FrozenThresholdPolicyConflict(
            "current frozen input fingerprint is missing or inconsistent"
        )
    if bundle_sha256 is None or stored_bundle_sha256 != bundle_sha256:
        raise FrozenThresholdPolicyConflict(
            "current frozen decision feature-bundle hash is missing or inconsistent"
        )
    if _strict_sha256(event_anchor.get("input_fingerprint")) != input_fingerprint:
        raise FrozenThresholdPolicyConflict(
            "outcome event input fingerprint differs from frozen evidence"
        )
    if str(
        event_anchor.get("feature_bundle_policy_version") or ""
    ).strip() != _STRICT_FEATURE_BUNDLE_POLICY_VERSION:
        raise FrozenThresholdPolicyConflict(
            "outcome event feature-bundle policy is missing or incompatible"
        )
    if _strict_sha256(
        event_anchor.get("feature_bundle_sha256")
    ) != bundle_sha256:
        raise FrozenThresholdPolicyConflict(
            "outcome event feature-bundle hash differs from frozen evidence"
        )
    if not isinstance(evidence.get("source_timestamps"), Mapping):
        raise FrozenThresholdPolicyConflict(
            "current frozen source timestamps are missing or malformed"
        )
    if not isinstance(evidence.get("source_provenance"), Mapping):
        raise FrozenThresholdPolicyConflict(
            "current frozen source provenance is missing or malformed"
        )

    record_horizon = _strict_positive_int(record.get("horizon_minutes"))
    snapshot_horizon = _strict_positive_int(snapshot.get("horizon_minutes"))
    if record_horizon != horizon or snapshot_horizon != horizon:
        raise FrozenThresholdPolicyConflict(
            "current frozen horizon differs from the requested outcome horizon"
        )

    frozen_event = _mapping(snapshot.get("event"))
    event_id = _strict_positive_int(event.get("event_id"))
    frozen_event_id = _strict_positive_int(frozen_event.get("event_id"))
    if event_id is not None and frozen_event_id != event_id:
        raise FrozenThresholdPolicyConflict(
            "current frozen event identity differs from the outcome event"
        )
    if str(frozen_event.get("symbol") or "").strip().upper() != str(
        event.get("symbol") or ""
    ).strip().upper():
        raise FrozenThresholdPolicyConflict(
            "current frozen symbol differs from the outcome event"
        )
    if str(frozen_event.get("direction") or "").strip().upper() != str(
        event.get("direction") or ""
    ).strip().upper():
        raise FrozenThresholdPolicyConflict(
            "current frozen direction differs from the outcome event"
        )
    try:
        if _utc(frozen_event.get("alert_time_utc")) != decision_time:
            raise FrozenThresholdPolicyConflict(
                "current frozen decision time differs from the outcome event"
            )
    except (TypeError, ValueError, OverflowError) as exc:
        raise FrozenThresholdPolicyConflict(
            "current frozen decision time is missing or malformed"
        ) from exc

    session = snapshot.get("outcome_window_session")
    if not isinstance(session, Mapping):
        raise FrozenThresholdPolicyConflict(
            "current frozen outcome-window session is missing or malformed"
        )
    active_ratio = _strict_json_number(session.get("session_active_ratio"))
    weekend_ratio = _strict_json_number(session.get("session_weekend_ratio"))
    segments = session.get("session_segments")
    composition = str(session.get("session_composition") or "")
    if (
        active_ratio is None
        or weekend_ratio is None
        or not 0.0 <= active_ratio <= 1.0
        or not 0.0 <= weekend_ratio <= 1.0
        or not math.isclose(
            active_ratio + weekend_ratio,
            1.0,
            rel_tol=0.0,
            abs_tol=1e-6,
        )
        or isinstance(segments, bool)
        or not isinstance(segments, int)
        or segments <= 0
        or composition not in {"ACTIVE_ONLY", "WEEKEND_ONLY", "MIXED"}
    ):
        raise FrozenThresholdPolicyConflict(
            "current frozen outcome-window session is incomplete or inconsistent"
        )
    expected_composition = research_session_width.session_composition_label(
        active_ratio
    )
    if composition != expected_composition:
        raise FrozenThresholdPolicyConflict(
            "current frozen outcome-window session composition is inconsistent"
        )

    reference = snapshot.get("movement_width_reference")
    if not isinstance(reference, Mapping) or not reference:
        raise FrozenThresholdPolicyConflict(
            "current frozen per-horizon movement-width reference is missing"
        )
    compatible, reason = research_session_width.validate_movement_width_reference(
        reference,
        expected_symbol=event.get("symbol"),
        event_time=decision_time,
        horizon_minutes=horizon,
    )
    if not compatible:
        raise FrozenThresholdPolicyConflict(reason)
    reference_active = _strict_json_number(
        reference.get("session_active_ratio")
    )
    reference_weekend = _strict_json_number(
        reference.get("session_weekend_ratio")
    )
    reference_segments = reference.get("session_segments")
    reference_composition = str(reference.get("session_composition") or "")
    if not (
        reference_active is not None
        and reference_weekend is not None
        and math.isclose(
            reference_active, active_ratio, rel_tol=0.0, abs_tol=1e-6
        )
        and math.isclose(
            reference_weekend, weekend_ratio, rel_tol=0.0, abs_tol=1e-6
        )
        and reference_segments == segments
        and reference_composition == composition
    ):
        raise FrozenThresholdPolicyConflict(
            "current frozen width and outcome-window session bundles disagree"
        )

    try:
        threshold_policy = research_no_dwell_outcome.freeze_threshold_policy(
            horizon_minutes=horizon,
            decision_time=decision_time,
            prior_only_reference=dict(reference),
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise FrozenThresholdPolicyConflict(str(exc)) from exc
    fingerprint = (
        slot_id,
        input_fingerprint,
        bundle_sha256,
        round(active_ratio, 8),
        round(weekend_ratio, 8),
        segments,
        composition,
        str(threshold_policy.get("threshold_reference_hash") or ""),
        round(float(threshold_policy["threshold_scale_factor"]), 8),
    )
    return fingerprint, threshold_policy


def _frozen_threshold_policy(
    *,
    event: Mapping[str, Any],
    horizon_minutes: int,
    snapshot_records: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Build one deterministic threshold from immutable decision evidence.

    A sampler-v4 slot is the direct authority for current events.  Legacy
    formulas can still carry earlier snapshot references.  The canonical
    first-touch table has one row for an event/horizon, so multiple applicable
    authorities must agree exactly.  Ambiguity is rejected instead of silently
    falling back to a static threshold and mislabelling a weekend outcome.
    """
    horizon = int(horizon_minutes)
    decision_time = _utc(event["alert_time_utc"])
    raw_relevant = [
        record
        for record in snapshot_records
        if int(record.get("horizon_minutes") or 0) == horizon
    ]
    current_event = (
        _prospective_sampler_version(event)
        == _STRICT_PROSPECTIVE_SAMPLER_VERSION
    )
    if not raw_relevant:
        if current_event:
            raise FrozenThresholdPolicyConflict(
                "sampler-v4 event has no current frozen threshold evidence"
            )
        return research_no_dwell_outcome.freeze_threshold_policy(
            horizon_minutes=horizon,
            decision_time=decision_time,
        )

    current_records = [
        record
        for record in raw_relevant
        if _is_current_frozen_evidence_record(record)
    ]
    if current_event and not current_records:
        raise FrozenThresholdPolicyConflict(
            "sampler-v4 event has no current frozen threshold evidence"
        )
    if current_records:
        normalized_current = [
            _strict_frozen_threshold_policy(
                event=event,
                horizon_minutes=horizon,
                record=record,
            )
            for record in current_records
        ]
        if len({item[0] for item in normalized_current}) != 1:
            raise FrozenThresholdPolicyConflict(
                "current threshold authorities disagree on the frozen "
                "event/horizon feature bundle"
            )
        return normalized_current[0][1]

    # Earlier formula/sampler snapshots remain readable for historical audit,
    # but they are never treated as current frozen threshold evidence. Their
    # compatibility bridge below cannot authorize readiness or replace a
    # missing v4 decision bundle.
    relevant = raw_relevant

    normalized_relaxed: list[tuple[tuple[Any, ...], Dict[str, Any]]] = []
    static_records = 0
    for record in relevant:
        snapshot = _mapping(record.get("input_snapshot"))
        reference = _mapping(snapshot.get("movement_width_reference"))
        if not reference:
            static_records += 1
            continue
        scale = _finite_number(
            reference.get("threshold_scale_factor")
            if reference.get("threshold_scale_factor") is not None
            else reference.get("floor_scale_factor")
        )
        if scale is None or not 0.50 <= scale <= 1.00:
            raise FrozenThresholdPolicyConflict(
                "frozen movement-width scale is missing or outside 0.50-1.00"
            )
        applied = reference.get("applied") is True
        if math.isclose(scale, 1.0, rel_tol=0.0, abs_tol=1e-9):
            if applied:
                raise FrozenThresholdPolicyConflict(
                    "frozen movement-width reference marks scale 1.0 as applied"
                )
            static_records += 1
            continue
        if not applied:
            raise FrozenThresholdPolicyConflict(
                "relaxed movement-width scale was not frozen as applied"
            )
        reference_horizon = _finite_number(reference.get("horizon_minutes"))
        if (
            reference_horizon is None
            or not float(reference_horizon).is_integer()
            or int(reference_horizon) != horizon
        ):
            raise FrozenThresholdPolicyConflict(
                "relaxed reference horizon is missing or differs from formula horizon"
            )

        source_kind = str(reference.get("source_kind") or "").upper()
        policy_source = str(reference.get("policy") or "").strip()
        if not source_kind:
            # Compatibility for already-frozen snapshots created before the
            # explicit provenance fields were added.  Only the exact known
            # prior-only policy is eligible for this bridge.
            if policy_source != (
                "prior raw price width; same-symbol session-composition matched"
            ):
                raise FrozenThresholdPolicyConflict(
                    "legacy relaxed reference lacks recognized prior-only provenance"
                )
            source_kind = "PRIOR_ONLY_SESSION_CALIBRATION"
        if source_kind != "PRIOR_ONLY_SESSION_CALIBRATION":
            raise FrozenThresholdPolicyConflict(
                "relaxed reference is not prior-only session calibration"
            )

        as_of = reference.get("as_of_utc")
        if as_of is None:
            source_inputs = _mapping(snapshot.get("source_inputs"))
            price_oi = _mapping(source_inputs.get("price_oi"))
            as_of = price_oi.get("timestamp_utc")
        if as_of is None:
            raise FrozenThresholdPolicyConflict(
                "relaxed reference has no frozen prior-only as-of timestamp"
            )
        try:
            as_of_utc = _utc(as_of)
        except (TypeError, ValueError) as exc:
            raise FrozenThresholdPolicyConflict(
                "relaxed reference has an invalid prior-only as-of timestamp"
            ) from exc

        weekend_ratio = _finite_number(reference.get("session_weekend_ratio"))
        if weekend_ratio is None:
            session = _mapping(snapshot.get("outcome_window_session"))
            weekend_ratio = _finite_number(session.get("session_weekend_ratio"))
        prior_reference = {
            "source_kind": source_kind,
            "as_of_utc": as_of_utc,
            "threshold_scale_factor": scale,
            "session_weekend_ratio": weekend_ratio,
            "source": policy_source or "prior-only session calibration",
        }
        try:
            threshold_policy = research_no_dwell_outcome.freeze_threshold_policy(
                horizon_minutes=horizon,
                decision_time=decision_time,
                prior_only_reference=prior_reference,
            )
        except (TypeError, ValueError) as exc:
            raise FrozenThresholdPolicyConflict(str(exc)) from exc
        fingerprint = (
            round(float(threshold_policy["threshold_scale_factor"]), 8),
            round(float(threshold_policy["session_weekend_ratio"]), 8),
            _utc(threshold_policy["threshold_as_of_utc"]).isoformat(),
            str(threshold_policy["threshold_source_kind"]),
            str(threshold_policy["threshold_source"]),
            str(threshold_policy.get("threshold_reference_hash") or ""),
        )
        normalized_relaxed.append((fingerprint, threshold_policy))

    if not normalized_relaxed:
        return research_no_dwell_outcome.freeze_threshold_policy(
            horizon_minutes=horizon,
            decision_time=decision_time,
        )
    if static_records or len({item[0] for item in normalized_relaxed}) != 1:
        raise FrozenThresholdPolicyConflict(
            "formula snapshots disagree on the event/horizon width policy"
        )
    return normalized_relaxed[0][1]


def calculate_returns(
    reference_price: float,
    horizon_price: float,
    direction: str,
) -> tuple[float, Optional[float]]:
    """Return raw and direction-adjusted percentages for deterministic tests."""
    reference = float(reference_price)
    horizon = float(horizon_price)
    if reference <= 0:
        raise ValueError("reference_price must be positive")
    raw = (horizon - reference) / reference * 100.0
    normalized = str(direction or "NEUTRAL").upper()
    directional = raw if normalized == "LONG" else -raw if normalized == "SHORT" else None
    return raw, directional


def _snapshot_price_source(value: Any) -> str:
    provenance = _snapshot_price_provenance(value)
    source = provenance["source"] or "research_event_current_price"
    pair = provenance["pair"]
    return ":".join(part for part in (source, pair) if part)


def _snapshot_price_provenance(value: Any) -> Dict[str, str]:
    """Return archived decision-price provenance without inventing defaults."""
    snapshot = _mapping(value)
    market_evidence = _mapping(snapshot.get("market_evidence"))
    signal_snapshot = _mapping(snapshot.get("signal_snapshot"))
    archive_reference = _mapping(signal_snapshot.get("archive_reference"))
    archived_official_price = _mapping(archive_reference.get("official_price"))

    def field(name: str) -> str:
        return str(
            snapshot.get(f"price_{name}")
            or snapshot.get(f"top_item_price_{name}")
            or market_evidence.get(f"price_{name}")
            or archived_official_price.get(name)
            or ""
        ).strip()

    return {
        "source": field("source"),
        "exchange": field("exchange"),
        "market": field("market"),
        "pair": field("pair"),
        "instrument": field("instrument"),
    }


def _stage4_frozen_price_reference(
    event: Mapping[str, Any],
) -> tuple[float, str]:
    """Validate and serialize the immutable Stage-4 archive price reference.

    These metrics describe the post-decision path relative to an input price
    already frozen in the passive archive. They are not trade-entry returns.
    """
    if not _is_stage4_signal_outcome_event(event):
        raise ValueError("event is not an authorized Stage-4 signal type")
    snapshot = _mapping(event.get("engine_snapshot"))
    signal_snapshot = _mapping(snapshot.get("signal_snapshot"))
    archive_reference = _mapping(signal_snapshot.get("archive_reference"))
    official_price = _mapping(archive_reference.get("official_price"))

    frozen_price = _strict_json_number(official_price.get("price"))
    event_price = _finite_number(event.get("current_price"))
    if (
        frozen_price is None
        or frozen_price <= 0
        or event_price is None
        or event_price <= 0
        or frozen_price != event_price
    ):
        raise ValueError("Stage-4 frozen official price does not match event price")

    snapshot_set_id = _strict_positive_int(
        archive_reference.get("snapshot_set_id")
    )
    snapshot_key = _strict_sha256(archive_reference.get("snapshot_key"))
    if snapshot_set_id is None or snapshot_key is None:
        raise ValueError("Stage-4 frozen snapshot identity is invalid")

    raw_event_time = event.get("alert_time_utc")
    raw_observed = official_price.get("observed_at_utc")
    raw_fetched = official_price.get("fetched_at_utc")
    for name, raw_value in (
        ("decision", raw_event_time),
        ("observed", raw_observed),
        ("fetched", raw_fetched),
    ):
        if isinstance(raw_value, datetime):
            if raw_value.tzinfo is None or raw_value.utcoffset() is None:
                raise ValueError(f"Stage-4 {name} time lacks timezone")
        elif not isinstance(raw_value, str) or re.search(
            r"(Z|[+-][0-9]{2}:[0-9]{2})$", raw_value.strip()
        ) is None:
            raise ValueError(f"Stage-4 {name} time lacks timezone")
    try:
        decision_time = _utc(raw_event_time)
        observed_at = _utc(raw_observed)
        fetched_at = _utc(raw_fetched)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("Stage-4 frozen price timestamp is invalid") from exc
    if observed_at > fetched_at or fetched_at > decision_time:
        raise ValueError("Stage-4 frozen price reference is post-decision")
    observed_age = (decision_time - observed_at).total_seconds()
    fetched_age = (decision_time - fetched_at).total_seconds()
    if (
        observed_age < 0
        or fetched_age < 0
        or observed_age > _STAGE4_MAX_REFERENCE_AGE_SECONDS
        or fetched_age > _STAGE4_MAX_REFERENCE_AGE_SECONDS
    ):
        raise ValueError("Stage-4 frozen price reference is stale")

    provenance = {
        key: str(official_price.get(key) or "").strip()
        for key in ("source", "exchange", "market", "pair", "instrument")
    }
    symbol = str(event.get("symbol") or "").strip().upper()
    source = provenance["source"].lower()
    exchange = provenance["exchange"].lower()
    market = provenance["market"].lower()
    pair = provenance["pair"].upper()
    instrument = provenance["instrument"]
    if symbol == "HYPE":
        canonical_route = (
            source == "hyperliquid"
            and exchange == "hyperliquid"
            and market == "spot"
            and pair == "HYPE/USDT"
            and instrument == "@107"
        )
    else:
        canonical_route = (
            bool(symbol)
            and source == "binance_spot"
            and exchange == "binance"
            and market == "spot"
            and pair == f"{symbol}USDT"
        )
    if not canonical_route:
        raise ValueError("Stage-4 frozen official price source is non-canonical")

    reference_source = "|".join(
        (
            f"reference_policy={_STAGE4_FROZEN_REFERENCE_POLICY_VERSION}",
            f"admission_policy={_STAGE4_DERIVED_ADMISSION_POLICY_VERSION}",
            f"semantics={_STAGE4_OUTCOME_SEMANTICS}",
            f"source={provenance['source']}",
            f"exchange={provenance['exchange']}",
            f"market={provenance['market']}",
            f"pair={provenance['pair']}",
            f"instrument={provenance['instrument']}",
            f"observed_at_utc={observed_at.isoformat()}",
            f"fetched_at_utc={fetched_at.isoformat()}",
            f"observed_age_seconds={observed_age:.6f}",
            f"fetched_age_seconds={fetched_age:.6f}",
            f"snapshot_set_id={snapshot_set_id}",
            f"snapshot_key={snapshot_key}",
        )
    )
    return frozen_price, reference_source


def _explicit_utc(value: Any, *, field: str) -> datetime:
    """Parse a provenance time without repairing a missing timezone."""

    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{field} lacks timezone")
        return value.astimezone(timezone.utc)
    if not isinstance(value, str) or re.search(
        r"(Z|[+-][0-9]{2}:[0-9]{2})$", value.strip()
    ) is None:
        raise ValueError(f"{field} lacks timezone")
    try:
        return datetime.fromisoformat(value.strip().replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field} is invalid") from exc


def _iso_utc(value: Any, *, field: str) -> str:
    return _explicit_utc(value, field=field).isoformat(
        timespec="microseconds"
    ).replace("+00:00", "Z")


def _optional_iso_utc(value: Any, *, field: str) -> Optional[str]:
    if value in (None, ""):
        return None
    return _iso_utc(value, field=field)


def _stage4_no_signal_frozen_price_reference(
    cell: Mapping[str, Any],
) -> tuple[float, str, Dict[str, Any]]:
    """Validate one no-signal cell's pre-decision archive price receipt."""

    symbol = str(cell.get("symbol") or "").strip().upper()
    direction = str(cell.get("direction") or "").strip().upper()
    projection_fingerprint = _strict_sha256(
        str(cell.get("projection_event_fingerprint") or "").strip()
    )
    snapshot_key = _strict_sha256(str(cell.get("snapshot_key") or "").strip())
    set_payload_sha256 = _strict_sha256(
        str(cell.get("set_payload_sha256") or "").strip()
    )
    manifest_payload_sha256 = _strict_sha256(
        str(cell.get("manifest_payload_sha256") or "").strip()
    )
    row_payload_sha256 = _strict_sha256(
        str(cell.get("reference_row_payload_sha256") or "").strip()
    )
    projection_event_id = _strict_positive_int(cell.get("projection_event_id"))
    snapshot_set_id = _strict_positive_int(cell.get("snapshot_set_id"))
    reference_row_id = _strict_positive_int(cell.get("reference_snapshot_row_id"))
    if (
        not re.fullmatch(r"[A-Z0-9-]{1,20}", symbol)
        or direction not in {"LONG", "SHORT"}
        or projection_fingerprint is None
        or snapshot_key is None
        or set_payload_sha256 is None
        or manifest_payload_sha256 is None
        or row_payload_sha256 is None
        or projection_event_id is None
        or snapshot_set_id is None
        or reference_row_id is None
    ):
        raise ValueError("Stage-4 no-signal cell identity is invalid")
    if int(cell.get("archive_row_count") or 0) != 7 or int(
        cell.get("archive_price_signature_count") or 0
    ) != 1:
        raise ValueError(
            "Stage-4 no-signal archive price is not coherent across 7 rows"
        )

    decision_time = _explicit_utc(
        cell.get("decision_time_utc"), field="no-signal decision time"
    )
    fetched_at = _explicit_utc(
        cell.get("price_fetched_at_utc"), field="no-signal fetched time"
    )
    raw_provenance = _mapping(cell.get("raw_provenance"))
    observed_at = _explicit_utc(
        raw_provenance.get("price_observed_at_utc"),
        field="no-signal observed time",
    )
    if observed_at > fetched_at or fetched_at > decision_time:
        raise ValueError("Stage-4 no-signal price reference is post-decision")
    observed_age = (decision_time - observed_at).total_seconds()
    fetched_age = (decision_time - fetched_at).total_seconds()
    if max(observed_age, fetched_age) > _STAGE4_MAX_REFERENCE_AGE_SECONDS:
        raise ValueError("Stage-4 no-signal price reference is stale")

    price = _finite_number(cell.get("reference_price"))
    source = str(cell.get("price_source") or "").strip()
    exchange = str(cell.get("price_exchange") or "").strip()
    market = str(cell.get("price_market") or "").strip()
    pair = str(cell.get("price_pair") or "").strip()
    instrument = str(cell.get("price_instrument") or "").strip()
    interval = str(raw_provenance.get("price_interval") or "").strip()
    policy_status = str(cell.get("price_source_policy_status") or "").strip()
    if price is None or price <= 0 or interval != "1m" or policy_status != "PASS":
        raise ValueError("Stage-4 no-signal official price is incomplete")
    if symbol == "HYPE":
        route_ok = (
            source.lower() == "hyperliquid"
            and exchange.lower() == "hyperliquid"
            and market.lower() == "spot"
            and pair.upper() == "HYPE/USDT"
            and instrument == "@107"
        )
    else:
        route_ok = (
            source.lower() == "binance_spot"
            and exchange.lower() == "binance"
            and market.lower() == "spot"
            and pair.replace("/", "").upper() == f"{symbol}USDT"
        )
    if not route_ok:
        raise ValueError("Stage-4 no-signal official price route is non-canonical")

    receipt = {
        "contract_version": _STAGE4_NO_SIGNAL_REFERENCE_RECEIPT_VERSION,
        "projection_event_id": projection_event_id,
        "projection_event_fingerprint": projection_fingerprint,
        "snapshot_set_id": snapshot_set_id,
        "snapshot_key": snapshot_key,
        "set_payload_sha256": set_payload_sha256,
        "symbol": symbol,
        "symbol_manifest_payload_sha256": manifest_payload_sha256,
        "source_timeframe": "12h",
        "snapshot_row_id": reference_row_id,
        "snapshot_row_payload_sha256": row_payload_sha256,
        "official_price": {
            "price": price,
            "source": source,
            "exchange": exchange,
            "market": market,
            "pair": pair,
            "instrument": instrument,
            "interval": interval,
            "fetched_at_utc": _iso_utc(fetched_at, field="no-signal fetched time"),
            "observed_at_utc": _iso_utc(observed_at, field="no-signal observed time"),
            "candle_open_time_utc": _optional_iso_utc(
                raw_provenance.get("price_candle_open_time_utc"),
                field="no-signal candle open time",
            ),
            "candle_close_time_utc": _optional_iso_utc(
                raw_provenance.get("price_candle_close_time_utc"),
                field="no-signal candle close time",
            ),
            "policy_status": policy_status,
        },
    }
    reference_source = "|".join(
        (
            f"reference_policy={_STAGE4_NO_SIGNAL_REFERENCE_RECEIPT_VERSION}",
            f"admission_policy={_STAGE4_NO_SIGNAL_ADMISSION_POLICY_VERSION}",
            f"semantics={_STAGE4_OUTCOME_SEMANTICS}",
            f"source={source}",
            f"exchange={exchange}",
            f"market={market}",
            f"pair={pair}",
            f"instrument={instrument}",
            f"observed_at_utc={observed_at.isoformat()}",
            f"fetched_at_utc={fetched_at.isoformat()}",
            f"observed_age_seconds={observed_age:.6f}",
            f"fetched_age_seconds={fetched_age:.6f}",
            f"snapshot_set_id={snapshot_set_id}",
            f"snapshot_key={snapshot_key}",
        )
    )
    return price, reference_source, receipt


def _alert_reference_provenance_error(event: Mapping[str, Any]) -> Optional[str]:
    """Reject delivered Alerts whose immutable reference route is not official.

    Decision Samples are admitted only through the separately guarded
    ``research_prospective_shadow_events`` view and intentionally do not use
    this Alert-specific check.  A missing or operational fallback provenance is
    never repaired from a later candle because that would be look-ahead.
    """
    if str(event.get("event_kind") or "").strip().upper() != "ALERT":
        return None
    if str(event.get("delivery_status") or "").strip().upper() != "DELIVERED":
        return "Alert is not archived as DELIVERED"

    symbol = str(event.get("symbol") or "").strip().upper()
    if not symbol:
        return "Alert symbol is missing"
    provenance = _snapshot_price_provenance(event.get("engine_snapshot"))
    source = provenance["source"].lower()
    exchange = provenance["exchange"].lower()
    market = provenance["market"].lower()
    pair = provenance["pair"].upper()

    if symbol != "HYPE":
        expected_pair = f"{symbol}USDT"
        if (
            source != "binance_spot"
            or pair != expected_pair
            or exchange not in {"", "binance"}
            or market not in {"", "spot"}
        ):
            return (
                "non-HYPE Alert requires exact binance_spot/"
                f"{expected_pair} reference provenance without a conflicting "
                "exchange or market"
            )
        return None

    if (
        source != "hyperliquid"
        or exchange != "hyperliquid"
        or market != "spot"
        or pair != "HYPE/USDT"
        or provenance["instrument"] != "@107"
    ):
        return (
            "HYPE Alert requires exact Hyperliquid Spot HYPE/USDT "
            "instrument @107 reference provenance"
        )
    return None


def _alert_reference_queue_priority_sql(event_alias: str = "e") -> str:
    """Place canonical Alerts ahead of rejected rows before a bounded LIMIT.

    Python remains the authoritative provenance gate.  This equivalent SQL
    priority prevents a permanently rejected archived Alert from starving a
    later canonical event without mutating or relabelling the audit archive.
    Missing optional Binance exchange/market fields remain acceptable, but
    contradictory values do not receive canonical priority.
    """
    alias = str(event_alias).strip()
    if alias != "e":
        raise ValueError("unsupported event SQL alias")

    def archived_field(name: str, *, case: str = "LOWER") -> str:
        raw = (
            f"COALESCE("
            f"NULLIF(BTRIM({alias}.engine_snapshot->>'price_{name}'), ''), "
            f"NULLIF(BTRIM({alias}.engine_snapshot->>'top_item_price_{name}'), ''), "
            f"NULLIF(BTRIM({alias}.engine_snapshot->'market_evidence'->>"
            f"'price_{name}'), ''), '')"
        )
        return f"{case}({raw})"

    source = archived_field("source")
    exchange = archived_field("exchange")
    market = archived_field("market")
    pair = archived_field("pair", case="UPPER")
    instrument = archived_field("instrument", case="BTRIM")
    symbol = f"UPPER(BTRIM({alias}.symbol))"
    return f"""
        CASE
            WHEN {alias}.event_kind<>'ALERT' THEN 0
            WHEN {symbol}<>'HYPE'
             AND {source}='binance_spot'
             AND {pair}=({symbol} || 'USDT')
             AND {exchange} IN ('', 'binance')
             AND {market} IN ('', 'spot')
            THEN 0
            WHEN {symbol}='HYPE'
             AND {source}='hyperliquid'
             AND {exchange}='hyperliquid'
             AND {market}='spot'
             AND {pair}='HYPE/USDT'
             AND {instrument}='@107'
            THEN 0
            ELSE 1
        END
    """.strip()


def _canonical_path_provenance_error(
    symbol: Any, path_result: Mapping[str, Any]
) -> Optional[str]:
    """Reject a fetched candle path unless its official route is explicit."""
    normalized = str(symbol or "").strip().upper()
    exchange = str(path_result.get("exchange") or "").strip().lower()
    market = str(path_result.get("market") or "").strip().lower()
    pair = str(path_result.get("pair") or "").strip().upper()
    interval = str(path_result.get("interval") or "").strip().lower()
    returned_symbol = str(path_result.get("symbol") or "").strip().upper()

    if normalized == "HYPE":
        api_coin = str(path_result.get("api_coin") or "").strip()
        if (
            returned_symbol != "HYPE"
            or exchange != "hyperliquid"
            or market != "spot"
            or pair != "HYPE/USDT"
            or api_coin != "@107"
            or interval != "1m"
        ):
            return "HYPE candle path is not exact Hyperliquid Spot @107 1m"
        return None

    try:
        expected_pair, _ = binance_spot_price_path.resolve_pair(normalized)
    except (TypeError, ValueError) as exc:
        return f"unsupported Binance Spot symbol: {exc}"
    if (
        returned_symbol != normalized
        or exchange != "binance"
        or market != "spot"
        or pair != expected_pair
        or interval != "1m"
    ):
        return f"{normalized} candle path is not exact Binance Spot {expected_pair} 1m"
    return None


def _path_source(reference_source: str, path_result: Dict[str, Any]) -> str:
    exchange = str(path_result.get("exchange") or "unknown").lower()
    market = str(path_result.get("market") or "spot").lower()
    return (
        f"reference={reference_source}|path={exchange}_{market}:"
        f"{path_result['pair']}:{path_result['interval']}|"
        f"provenance={path_result.get('provenance') or 'exchange_api'}"
    )


def _due_horizons(
    event_time: datetime,
    existing_versions: Dict[int, str],
    existing_first_touch_versions: Optional[Dict[int, str]] = None,
    *,
    now: datetime,
    outcome_method_version: str = _METHOD_VERSION,
    first_touch_enabled: bool = True,
    open_first_touch_horizons: Iterable[int] = (),
) -> list[int]:
    """Return horizons missing legacy or first-touch enrichment.

    Legacy outcomes and unmatched controls remain close-only.  The caller may
    explicitly authorize selected open horizons for a prospective event that
    matched an active Shadow formula.  Limiting open polling to those exact
    horizons bounds canonical API load without delaying a qualifying touch.
    """
    first_touch_versions = existing_first_touch_versions or {}
    open_horizons = {
        int(horizon)
        for horizon in open_first_touch_horizons
        if int(horizon) in _HORIZONS
    }
    due = []
    for horizon in _HORIZONS:
        horizon_end = event_time + timedelta(minutes=horizon)
        horizon_closed = horizon_end <= now
        legacy_due = (
            horizon_closed
            and existing_versions.get(horizon) != outcome_method_version
        )
        first_touch_due = (
            first_touch_enabled
            and first_touch_versions.get(horizon) != _FIRST_TOUCH_METHOD_VERSION
            and (horizon_closed or horizon in open_horizons)
        )
        if legacy_due or first_touch_due:
            due.append(horizon)
    return due


def _versions(value: Any) -> Dict[int, str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = {}
    if not isinstance(value, dict):
        return {}
    result: Dict[int, str] = {}
    for key, version in value.items():
        try:
            result[int(key)] = str(version or "")
        except (TypeError, ValueError):
            continue
    return result


def _candles_for_horizon(
    candles: Iterable[binance_spot_price_path.SpotCandle],
    horizon_time: datetime,
) -> list[binance_spot_price_path.SpotCandle]:
    cutoff = _utc(horizon_time)
    return [candle for candle in candles if candle.close_time_utc <= cutoff]


def _expected_candles(event_time: datetime, horizon_time: datetime) -> int:
    start_ms = int(_utc(event_time).timestamp() * 1000)
    end_ms = int(_utc(horizon_time).timestamp() * 1000)
    interval_ms = canonical_price_path.INTERVAL_MS
    first_open = ((start_ms + interval_ms - 1) // interval_ms) * interval_ms
    last_open = ((end_ms - (interval_ms - 1)) // interval_ms) * interval_ms
    if last_open < first_open:
        return 0
    return int((last_open - first_open) // interval_ms) + 1


@dataclass
class OutcomeMetrics:
    runs: int = 0
    events_checked: int = 0
    outcomes_inserted: int = 0
    outcomes_upgraded: int = 0
    missing_price_paths: int = 0
    partial_price_paths: int = 0
    first_touch_rows_written: int = 0
    first_touch_hits: int = 0
    first_touch_pending: int = 0
    first_touch_threshold_policy_conflicts: int = 0
    alert_reference_provenance_rejections: int = 0
    first_touch_terminal_rows_deferred_for_incomplete_prefix: int = 0
    stage4_no_signal_cells_checked: int = 0
    stage4_no_signal_outcomes_inserted: int = 0
    stage4_no_signal_missing_price_paths: int = 0
    stage4_no_signal_failures: int = 0
    stage4_no_signal_last_error: Optional[str] = None
    stage4_signal_scan_pages: int = 0
    stage4_signal_scan_candidates: int = 0
    stage4_signal_scan_budget_exhaustions: int = 0
    stage4_signal_scan_laps_completed: int = 0
    stage4_signal_scan_failures: int = 0
    stage4_signal_scan_last_error: Optional[str] = None
    legacy_due_load_failures: int = 0
    legacy_due_load_last_error: Optional[str] = None
    open_first_touch_load_failures: int = 0
    open_first_touch_load_last_error: Optional[str] = None
    failures: int = 0
    current_phase: str = "IDLE"
    current_phase_started_at_utc: Optional[str] = None
    last_phase: Optional[str] = None
    last_phase_duration_ms: Optional[int] = None
    last_error_phase: Optional[str] = None
    last_timeout_phase: Optional[str] = None
    last_run_utc: Optional[str] = None
    last_error: Optional[str] = None


class ResearchOutcomeWorker:
    def __init__(self) -> None:
        self.metrics = OutcomeMetrics()
        self._task: Optional[asyncio.Task] = None
        self._stopping = False
        self._stage4_no_signal_projection_cursor: Optional[tuple[datetime, int]] = None
        self._last_stage4_due_scan: Dict[str, Any] = {
            "state_version": _STAGE4_DUE_SCAN_STATE_VERSION,
            "stop_reason": "NOT_RUN",
        }

    @contextmanager
    def _phase(self, name: str):
        phase = str(name or "UNKNOWN").strip().upper()
        started = time.monotonic()
        self.metrics.current_phase = phase
        self.metrics.current_phase_started_at_utc = datetime.now(
            timezone.utc
        ).isoformat()
        try:
            yield
        except Exception as exc:
            self.metrics.last_error_phase = phase
            if "statement timeout" in str(exc).lower():
                self.metrics.last_timeout_phase = phase
            raise
        finally:
            self.metrics.last_phase = phase
            self.metrics.last_phase_duration_ms = max(
                0, int(round((time.monotonic() - started) * 1000))
            )
            self.metrics.current_phase = "IDLE"
            self.metrics.current_phase_started_at_utc = None

    @contextmanager
    def _outcome_read_transaction(self, database_url: str):
        """Publish scan telemetry only after the read transaction commits."""

        try:
            with psycopg.connect(
                database_url,
                row_factory=dict_row,
                connect_timeout=5,
                options=_database_connection_options(),
            ) as conn:
                yield conn
        except Exception:
            self._discard_stage4_due_scan_telemetry()
            raise
        else:
            self._commit_stage4_due_scan_telemetry()

    @property
    def enabled(self) -> bool:
        return _ENABLED

    def status(self) -> Dict[str, Any]:
        return {
            "enabled": _ENABLED,
            "configured": bool(_database_url()),
            "running": bool(self._task and not self._task.done()),
            "horizons_minutes": list(_HORIZONS),
            "poll_seconds": _POLL_SECONDS,
            "heavy_query_timeout": (
                research_database_timeout.heavy_timeout_status()
            ),
            "method": _METHOD_VERSION,
            "stage4_signal_method": _STAGE4_OUTCOME_METHOD_VERSION,
            "stage4_signal_admission_policy": (
                _STAGE4_DERIVED_ADMISSION_POLICY_VERSION
            ),
            "stage4_signal_reference_policy": (
                _STAGE4_FROZEN_REFERENCE_POLICY_VERSION
            ),
            "stage4_signal_semantics": _STAGE4_OUTCOME_SEMANTICS,
            "stage4_signal_due_scan": {
                "state_version": _STAGE4_DUE_SCAN_STATE_VERSION,
                "page_size": _STAGE4_DUE_CANDIDATE_PAGE_SIZE,
                "max_pages_per_cycle": _STAGE4_DUE_SCAN_MAX_PAGES,
                "max_heavy_statements_per_cycle": (
                    _STAGE4_DUE_SCAN_MAX_PAGES * 2
                ),
                "wall_time_budget_ms": _STAGE4_DUE_SCAN_BUDGET_MS,
                "per_statement_timeout_ceiling_ms": (
                    research_database_timeout.heavy_statement_timeout_ms()
                ),
                "durable_cursor": True,
                "frozen_lap_high_water": True,
                "minimum_reserved_queue_fraction": (
                    f"1/{_STAGE4_DUE_RESERVED_QUEUE_DIVISOR}"
                ),
                "bidirectional_reservation_minimum_limit": 2,
                "last_scan": dict(self._last_stage4_due_scan),
            },
            "stage4_no_signal": {
                "configured": bool(_stage4_no_signal_database_url()),
                "database_env": _STAGE4_NO_SIGNAL_DATABASE_URL_ENV,
                "carrier_contract_version": (
                    _STAGE4_NO_SIGNAL_CARRIER_CONTRACT_VERSION
                ),
                "outcome_method_version": (
                    _STAGE4_NO_SIGNAL_OUTCOME_METHOD_VERSION
                ),
                "admission_policy_version": (
                    _STAGE4_NO_SIGNAL_ADMISSION_POLICY_VERSION
                ),
                "reference_policy_version": (
                    _STAGE4_NO_SIGNAL_REFERENCE_RECEIPT_VERSION
                ),
                "absence_basis": _STAGE4_NO_SIGNAL_ABSENCE_BASIS,
                "formula_registry_effect": "NONE",
                "telegram_delivery_allowed": False,
                "live_eligible": False,
                "trade_execution_allowed": False,
            },
            "first_touch_method": _FIRST_TOUCH_METHOD_VERSION,
            "open_first_touch_event_limit": _OPEN_FIRST_TOUCH_EVENT_LIMIT,
            "first_touch_policy": {
                "success": "first favorable width touch; zero dwell",
                "failure": "pending until the full horizon closes",
                "post_hit_reversal": "does not cancel success",
                "observation_resolution": (
                    "official closed 1m OHLC; a wick touch qualifies and the "
                    "candle need not close beyond the threshold"
                ),
                "worker_evaluation": (
                    "every minute for the exact horizons of eligible delivered "
                    "Alerts and authorized prospective Shadow matches; otherwise "
                    "at horizon close"
                ),
                "current_evidence_policy": (
                    _STRICT_FROZEN_EVIDENCE_POLICY_VERSION
                ),
                "current_sampler": _STRICT_PROSPECTIVE_SAMPLER_VERSION,
                "current_threshold_input": (
                    "exact frozen per-horizon movement-width and session bundle; "
                    "missing or malformed evidence fails closed"
                ),
                "legacy_evidence": "audit_only",
            },
            "price_paths": {
                "default": "Binance Spot USDT",
                "HYPE": "Hyperliquid HYPE/USDT spot (@107)",
                "market": "spot",
                "interval": canonical_price_path.INTERVAL,
                "first_partial_minute": "excluded_to_prevent_pre_alert_leakage",
                "historical_imports": "allowed_with_source_and_quality_provenance",
                "alert_reference_policy": (
                    "fail closed: exact binance_spot SYMBOLUSDT; HYPE exact "
                    "Hyperliquid Spot HYPE/USDT instrument @107"
                ),
            },
            "complete_quality_statuses": list(canonical_price_path.COMPLETE_QUALITIES),
            "metrics": self.metrics.__dict__.copy(),
        }

    async def start(self) -> bool:
        if not _ENABLED:
            return False
        if not _database_url():
            raise RuntimeError("Research outcome worker database is not configured")
        if psycopg is None:
            raise RuntimeError("psycopg is unavailable")
        if self._task and not self._task.done():
            return True
        self._stopping = False
        self._task = asyncio.create_task(self._run(), name="research-outcome-worker")
        return True

    async def stop(self) -> None:
        if not self._task:
            return
        self._stopping = True
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

    async def _run(self) -> None:
        while not self._stopping:
            try:
                await asyncio.to_thread(self.run_once)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.metrics.failures += 1
                self.metrics.last_error = f"{type(exc).__name__}: {exc}"
                print(f"[research-outcomes] run failed: {exc!r}", flush=True)
            await asyncio.sleep(_POLL_SECONDS)

    @staticmethod
    def _load_frozen_threshold_references(
        conn, event_ids: Sequence[int]
    ) -> list[Dict[str, Any]]:
        """Load legacy formula-snapshot width evidence for non-v4 events."""
        normalized = sorted(
            {int(event_id) for event_id in event_ids if int(event_id) > 0}
        )
        if not normalized:
            return []
        rows = conn.execute(
            """
            SELECT c.event_id, f.horizon_minutes, c.formula_id,
                   f.formula_key, f.formula_version,
                   f.formula_schema_version, f.engine_version,
                   f.feature_schema_version, f.outcome_method_version,
                   f.direction, f.conditions, c.input_snapshot,
                   c.condition_results, c.decision_cohort_key,
                   c.decision_anchor_time_utc, c.evaluation_status, c.matched,
                   c.evidence_policy_version,
                   c.prospective_anchor_slot_id,
                   c.prospective_input_fingerprint,
                   c.feature_bundle_sha256,
                   c.authoritative_verified
            FROM research_formula_shadow_checks c
            JOIN research_formulas f ON f.formula_id=c.formula_id
            WHERE c.event_id=ANY(%s)
              AND c.evaluation_status IN ('MATCHED', 'UNMATCHED')
            ORDER BY c.event_id, f.horizon_minutes, c.formula_id
            """,
            (normalized,),
        ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _load_current_slot_threshold_references(
        events: Sequence[Mapping[str, Any]], *, now: datetime
    ) -> Dict[int, list[Dict[str, Any]]]:
        """Load canonical sampler-v4 threshold authority by event and horizon.

        Formula checks are intentionally not an input.  The migration-013
        slot owns the immutable decision feature bundle for every authorized
        sampler-v4 event.  The shared loader validates that authority before
        returning its exact per-horizon session and movement-width context.
        Requests are chunked well below the shared loader's 250-event hard
        limit because its verified rows briefly contain the full frozen bundle
        before this worker reduces them to compact threshold evidence.
        """
        requested_by_event: Dict[int, set[int]] = {}
        for event in events:
            if (
                _prospective_sampler_version(event)
                != _STRICT_PROSPECTIVE_SAMPLER_VERSION
            ):
                continue
            event_id = _strict_positive_int(event.get("event_id"))
            if event_id is None:
                continue
            first_touch_enabled = _first_touch_enabled_for_event(event)
            if not first_touch_enabled:
                continue
            try:
                event_time = _utc(event.get("alert_time_utc"))
            except (TypeError, ValueError, OverflowError):
                continue
            first_touch_versions = _versions(
                event.get("first_touch_versions")
            )
            due_horizons = _due_horizons(
                event_time,
                _versions(event.get("outcome_versions")),
                first_touch_versions,
                now=_utc(now),
                first_touch_enabled=True,
                open_first_touch_horizons=(
                    event.get("open_first_touch_horizons") or ()
                ),
            )
            requested = {
                horizon
                for horizon in due_horizons
                if first_touch_versions.get(horizon)
                != _FIRST_TOUCH_METHOD_VERSION
            }
            if requested:
                requested_by_event[event_id] = requested

        references_by_event: Dict[int, list[Dict[str, Any]]] = {}
        event_ids = sorted(requested_by_event)
        batch_size = _SLOT_THRESHOLD_AUTHORITY_BATCH_SIZE
        for offset in range(0, len(event_ids), batch_size):
            chunk = event_ids[offset : offset + batch_size]
            requested_by_horizon = {
                horizon: [
                    event_id
                    for event_id in chunk
                    if horizon in requested_by_event[event_id]
                ]
                for horizon in _HORIZONS
            }
            requested_by_horizon = {
                horizon: requested_ids
                for horizon, requested_ids in requested_by_horizon.items()
                if requested_ids
            }
            loaded = (
                research_feature_matrix.load_shadow_feature_rows_by_horizon(
                    requested_by_horizon
                )
            )
            expected = {
                (event_id, horizon)
                for event_id in chunk
                for horizon in requested_by_event[event_id]
            }
            for key, row in sorted(loaded.items()):
                event_id, horizon = int(key[0]), int(key[1])
                if (event_id, horizon) not in expected:
                    continue
                references_by_event.setdefault(event_id, []).append(
                    _slot_threshold_record(row)
                )
        return references_by_event

    @staticmethod
    def _load_due_stage4_no_signal_projection_page(
        conn,
        *,
        cursor: Optional[tuple[Any, int]] = None,
        page_size: int = _STAGE4_NO_SIGNAL_PROJECTION_PAGE_SIZE,
    ) -> list[Dict[str, Any]]:
        """Select one bounded projection keyset page before JSON expansion."""

        cursor_clause = ""
        params: list[Any] = [
            _STAGE4_SIGNAL_SNAPSHOT_CAPTURE_STAGE,
            _STAGE4_SIGNAL_SNAPSHOT_STRATEGY_VERSION,
            _STAGE4_SIGNAL_SNAPSHOT_CONTRACT_VERSION,
        ]
        if cursor is not None:
            cursor_clause = "AND (e.alert_time_utc, e.event_id) > (%s, %s)"
            params.extend((_utc(cursor[0]), int(cursor[1])))
        params.append(
            max(1, min(int(page_size), _STAGE4_NO_SIGNAL_PROJECTION_PAGE_SIZE))
        )
        query = f"""
            /* research_outcomes:due_stage4_no_signal_projection_page */
            SELECT e.event_id AS projection_event_id,
                   e.alert_time_utc AS decision_time_utc
              FROM public.research_events e
             WHERE e.event_kind='DECISION_SAMPLE'
               AND e.event_type='SIGNAL_SNAPSHOT_PROJECTION'
               AND e.capture_stage=%s
               AND e.strategy_version=%s
               AND e.delivery_status='NOT_APPLICABLE'
               AND e.symbol='RESEARCH'
               AND e.direction='NEUTRAL'
               AND e.delivery_attempted_at_utc IS NULL
               AND e.delivered_at_utc IS NULL
               AND e.categories @>
                   '["DECISION_SAMPLE","SILENT","COMPLETED"]'::jsonb
               AND e.engine_snapshot->'signal_snapshot'->>'contract_version'=%s
               AND e.engine_snapshot->'signal_snapshot'->>'signal_family'='PROJECTION'
               AND e.engine_snapshot->'signal_snapshot'->>'tier'='COMPLETED'
               AND e.engine_snapshot->'projection'->>'status'='COMPLETED'
               AND e.alert_time_utc <= NOW() - INTERVAL '60 minutes'
               {cursor_clause}
             ORDER BY e.alert_time_utc ASC, e.event_id ASC
             LIMIT %s
        """
        return [dict(row) for row in conn.execute(query, params).fetchall()]

    @staticmethod
    def _load_stage4_no_signal_cells_for_projection_page(
        conn, projection_event_ids: Sequence[int]
    ) -> list[Dict[str, Any]]:
        """Expand only a previously bounded page into explicit no-signal cells."""

        normalized = sorted(
            {
                int(event_id)
                for event_id in projection_event_ids
                if int(event_id) > 0
            }
        )
        if not normalized:
            return []
        if len(normalized) > _STAGE4_NO_SIGNAL_PROJECTION_PAGE_SIZE:
            raise ValueError("Stage-4 no-signal projection page exceeds its hard bound")
        rows = conn.execute(
            """
            /* research_outcomes:hydrate_stage4_no_signal_cells */
            WITH projection AS MATERIALIZED (
                SELECT e.*,
                       (e.engine_snapshot->'projection'->>'snapshot_set_id')::bigint
                           AS snapshot_set_id,
                       e.engine_snapshot->'projection'->>'snapshot_key'
                           AS snapshot_key,
                       e.engine_snapshot->'projection'->>'set_payload_sha256'
                           AS set_payload_sha256
                  FROM public.research_events e
                 WHERE e.event_id=ANY(%s)
                   AND e.event_kind='DECISION_SAMPLE'
                   AND e.event_type='SIGNAL_SNAPSHOT_PROJECTION'
                   AND e.capture_stage=%s
                   AND e.strategy_version=%s
                   AND e.delivery_status='NOT_APPLICABLE'
                   AND e.symbol='RESEARCH'
                   AND e.direction='NEUTRAL'
                   AND e.delivery_attempted_at_utc IS NULL
                   AND e.delivered_at_utc IS NULL
                   AND e.categories @>
                       '["DECISION_SAMPLE","SILENT","COMPLETED"]'::jsonb
                   AND e.engine_snapshot->'signal_snapshot'->>'contract_version'=%s
                   AND e.engine_snapshot->'signal_snapshot'->>'signal_family'='PROJECTION'
                   AND e.engine_snapshot->'signal_snapshot'->>'tier'='COMPLETED'
                   AND e.engine_snapshot->'projection'->>'status'='COMPLETED'
                   AND (e.engine_snapshot->'projection'->>'decision_time_utc')
                       ::timestamptz=e.alert_time_utc
            ), cell AS MATERIALIZED (
                SELECT p.*,
                       evaluation.item->>'symbol' AS cell_symbol,
                       direction.value AS cell_direction
                  FROM projection p
                  CROSS JOIN LATERAL jsonb_array_elements(
                      p.engine_snapshot->'projection'->'symbol_evaluations'
                  ) evaluation(item)
                  CROSS JOIN (VALUES ('LONG'), ('SHORT')) direction(value)
                 WHERE evaluation.item->>'status'='EVALUABLE'
                   AND evaluation.item->'reason'
                       IS NOT DISTINCT FROM 'null'::jsonb
                   AND NOT EXISTS (
                       SELECT 1
                         FROM public.research_events signal
                        WHERE signal.capture_stage=%s
                          AND signal.event_type=ANY(%s)
                          AND signal.symbol=evaluation.item->>'symbol'
                          AND signal.direction=direction.value
                          AND signal.engine_snapshot #>>
                              '{signal_snapshot,archive_reference,snapshot_key}'=
                              p.snapshot_key
                   )
            )
            SELECT
                cell.event_id AS projection_event_id,
                BTRIM(cell.event_fingerprint) AS projection_event_fingerprint,
                cell.alert_time_utc AS decision_time_utc,
                cell.snapshot_set_id,
                cell.snapshot_key,
                cell.set_payload_sha256,
                cell.cell_symbol AS symbol,
                cell.cell_direction AS direction,
                BTRIM(manifest.payload_sha256) AS manifest_payload_sha256,
                reference.snapshot_row_id AS reference_snapshot_row_id,
                BTRIM(reference.payload_sha256)
                    AS reference_row_payload_sha256,
                reference.current_price AS reference_price,
                reference.price_source,
                reference.price_exchange,
                reference.price_market,
                reference.price_pair,
                reference.price_instrument,
                reference.price_fetched_at_utc,
                reference.price_source_policy_status,
                reference.raw_provenance,
                archive_check.row_count AS archive_row_count,
                archive_check.price_signature_count
                    AS archive_price_signature_count,
                COALESCE((
                    SELECT jsonb_object_agg(
                               existing.horizon_minutes,
                               existing.outcome_method_version
                           )
                      FROM public.research_stage4_no_signal_outcomes_v1 existing
                     WHERE existing.projection_event_id=cell.event_id
                       AND existing.symbol=cell.cell_symbol
                       AND existing.direction=cell.cell_direction
                ), '{}'::jsonb) AS outcome_versions
              FROM cell
              JOIN public.research_max_pain_snapshot_sets set_row
                ON set_row.snapshot_set_id=cell.snapshot_set_id
               AND BTRIM(set_row.snapshot_key)=cell.snapshot_key
               AND BTRIM(set_row.payload_sha256)=cell.set_payload_sha256
               AND set_row.research_eligible=TRUE
               AND set_row.source='RESEARCH_PASSIVE'
              JOIN public.research_max_pain_snapshot_symbols manifest
                ON manifest.snapshot_set_id=cell.snapshot_set_id
               AND manifest.symbol=cell.cell_symbol
               AND manifest.research_eligible=TRUE
               AND manifest.complete_7of7=TRUE
               AND manifest.price_overlay_coherent=TRUE
              JOIN public.research_max_pain_snapshot_rows reference
                ON reference.snapshot_set_id=cell.snapshot_set_id
               AND reference.symbol=cell.cell_symbol
               AND reference.timeframe='12h'
               AND reference.row_valid=TRUE
               AND reference.freshness_status='FRESH'
              CROSS JOIN LATERAL (
                  SELECT COUNT(*)::integer AS row_count,
                         COUNT(DISTINCT ROW(
                             archive_row.current_price,
                             COALESCE(archive_row.price_source, ''),
                             COALESCE(archive_row.price_exchange, ''),
                             COALESCE(archive_row.price_market, ''),
                             COALESCE(archive_row.price_pair, ''),
                             COALESCE(archive_row.price_instrument, ''),
                             archive_row.price_fetched_at_utc
                         ))::integer AS price_signature_count
                    FROM public.research_max_pain_snapshot_rows archive_row
                   WHERE archive_row.snapshot_set_id=cell.snapshot_set_id
                     AND archive_row.symbol=cell.cell_symbol
                     AND archive_row.row_valid=TRUE
                     AND archive_row.freshness_status='FRESH'
              ) archive_check
             ORDER BY cell.alert_time_utc, cell.event_id,
                      cell.cell_symbol, cell.cell_direction
             LIMIT %s
            """,
            (
                normalized,
                _STAGE4_SIGNAL_SNAPSHOT_CAPTURE_STAGE,
                _STAGE4_SIGNAL_SNAPSHOT_STRATEGY_VERSION,
                _STAGE4_SIGNAL_SNAPSHOT_CONTRACT_VERSION,
                _STAGE4_SIGNAL_SNAPSHOT_CAPTURE_STAGE,
                list(_STAGE4_SIGNAL_OUTCOME_EVENT_TYPES),
                _STAGE4_NO_SIGNAL_MAX_CELL_ROWS + 1,
            ),
        ).fetchall()
        if len(rows) > _STAGE4_NO_SIGNAL_MAX_CELL_ROWS:
            raise ValueError("Stage-4 no-signal cell hydration exceeded its hard bound")
        return [dict(row) for row in rows]

    def _load_due_stage4_no_signal_cells(
        self, conn, *, limit: int, now: datetime
    ) -> list[Dict[str, Any]]:
        """Advance a bounded in-memory keyset scan without invalid-row starvation."""

        bounded_limit = max(1, min(int(limit), 1000))
        selected: list[Dict[str, Any]] = []
        cursor = self._stage4_no_signal_projection_cursor
        wrapped = False
        for _ in range(_STAGE4_NO_SIGNAL_MAX_PROJECTION_PAGES):
            page = self._load_due_stage4_no_signal_projection_page(
                conn, cursor=cursor
            )
            if not page:
                if cursor is not None and not wrapped:
                    cursor = None
                    wrapped = True
                    continue
                self._stage4_no_signal_projection_cursor = None
                break
            last = page[-1]
            cursor = (_utc(last["decision_time_utc"]), int(last["projection_event_id"]))
            self._stage4_no_signal_projection_cursor = cursor
            hydrated = self._load_stage4_no_signal_cells_for_projection_page(
                conn, [int(row["projection_event_id"]) for row in page]
            )
            page_groups: Dict[tuple[int, str], list[Dict[str, Any]]] = {}
            for cell in hydrated:
                versions = _versions(cell.get("outcome_versions"))
                event_time = _utc(cell["decision_time_utc"])
                due = [
                    horizon
                    for horizon in _HORIZONS
                    if event_time + timedelta(minutes=horizon) <= now
                    and versions.get(horizon)
                    != _STAGE4_NO_SIGNAL_OUTCOME_METHOD_VERSION
                ]
                if not due:
                    continue
                normalized_cell = dict(cell)
                normalized_cell["due_horizons"] = due
                page_groups.setdefault(
                    (
                        int(normalized_cell["projection_event_id"]),
                        str(normalized_cell["symbol"]).strip().upper(),
                    ),
                    [],
                ).append(normalized_cell)
            for group in page_groups.values():
                # Keep an exact projection+symbol pair atomic.  A caller limit
                # of one may therefore return two directions, but never split
                # them and refetch the same canonical path on the next scan.
                if selected and len(selected) + len(group) > bounded_limit:
                    return selected
                selected.extend(group)
                if len(selected) >= bounded_limit:
                    return selected
            if len(page) < _STAGE4_NO_SIGNAL_PROJECTION_PAGE_SIZE:
                self._stage4_no_signal_projection_cursor = None
                break
        return selected

    @staticmethod
    def _write_stage4_no_signal_outcome(
        conn,
        *,
        cell: Mapping[str, Any],
        horizon: int,
        reference_receipt: Mapping[str, Any],
        reference_price: float,
        reference_source: str,
        path_result: Mapping[str, Any],
        path_metrics: Mapping[str, Any],
    ) -> bool:
        """Insert one immutable carrier; fail if the identity already differs."""

        source = _path_source(reference_source, dict(path_result))
        quality = canonical_price_path.quality_status(path_result, complete=True)
        values = (
            int(cell["projection_event_id"]),
            str(cell["projection_event_fingerprint"]).strip(),
            int(cell["snapshot_set_id"]),
            str(cell["snapshot_key"]).strip(),
            str(cell["symbol"]).strip().upper(),
            str(cell["direction"]).strip().upper(),
            int(horizon),
            _utc(cell["decision_time_utc"]),
            _STAGE4_NO_SIGNAL_ABSENCE_BASIS,
            json.dumps(
                reference_receipt,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
            path_metrics["measured_at_utc"],
            reference_price,
            path_metrics["price_at_horizon"],
            path_metrics["raw_return_pct"],
            path_metrics["directional_return_pct"],
            path_metrics["max_favorable_price"],
            path_metrics["max_adverse_price"],
            path_metrics["mfe_pct"],
            path_metrics["mae_pct"],
            path_metrics["time_to_first_progress_seconds"],
            path_metrics["time_to_mfe_seconds"],
            canonical_price_path.INTERVAL_SECONDS,
            len(path_result["candles"]),
            _STAGE4_NO_SIGNAL_OUTCOME_METHOD_VERSION,
            source,
            quality,
        )
        inserted = conn.execute(
            """
            /* research_outcomes:write_stage4_no_signal_outcome */
            INSERT INTO public.research_stage4_no_signal_outcomes_v1 (
                projection_event_id, projection_event_fingerprint,
                snapshot_set_id, snapshot_key, symbol, direction,
                horizon_minutes, decision_time_utc, absence_basis,
                reference_receipt, measured_at_utc, reference_price,
                price_at_horizon, raw_return_pct, directional_return_pct,
                max_favorable_price, max_adverse_price, mfe_pct, mae_pct,
                time_to_first_progress_seconds, time_to_mfe_seconds,
                path_resolution_seconds, path_samples,
                outcome_method_version, price_source, data_quality_status
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            ON CONFLICT (
                projection_event_id, symbol, direction, horizon_minutes
            ) DO NOTHING
            RETURNING outcome_payload_sha256
            """,
            values,
        ).fetchone()
        if inserted:
            return True
        existing = conn.execute(
            """
            SELECT projection_event_fingerprint, snapshot_set_id, snapshot_key,
                   decision_time_utc, absence_basis, reference_receipt,
                   measured_at_utc, reference_price, price_at_horizon,
                   raw_return_pct, directional_return_pct,
                   max_favorable_price, max_adverse_price, mfe_pct, mae_pct,
                   time_to_first_progress_seconds, time_to_mfe_seconds,
                   path_resolution_seconds, path_samples,
                   outcome_method_version, price_source, data_quality_status
              FROM public.research_stage4_no_signal_outcomes_v1
             WHERE projection_event_id=%s AND symbol=%s
               AND direction=%s AND horizon_minutes=%s
            """,
            (values[0], values[4], values[5], values[6]),
        ).fetchone()
        if existing is None:
            raise RuntimeError("no-signal outcome conflict disappeared")
        expected = {
            "projection_event_fingerprint": values[1],
            "snapshot_set_id": values[2],
            "snapshot_key": values[3],
            "decision_time_utc": _utc(values[7]),
            "absence_basis": values[8],
            "reference_receipt": dict(reference_receipt),
            "measured_at_utc": _utc(values[10]),
            "reference_price": values[11],
            "price_at_horizon": values[12],
            "raw_return_pct": values[13],
            "directional_return_pct": values[14],
            "max_favorable_price": values[15],
            "max_adverse_price": values[16],
            "mfe_pct": values[17],
            "mae_pct": values[18],
            "time_to_first_progress_seconds": values[19],
            "time_to_mfe_seconds": values[20],
            "path_resolution_seconds": values[21],
            "path_samples": values[22],
            "outcome_method_version": values[23],
            "price_source": values[24],
            "data_quality_status": values[25],
        }
        actual = dict(existing)
        actual["projection_event_fingerprint"] = str(
            actual["projection_event_fingerprint"]
        ).strip()
        actual["snapshot_key"] = str(actual["snapshot_key"]).strip()
        actual["decision_time_utc"] = _utc(actual["decision_time_utc"])
        actual["measured_at_utc"] = _utc(actual["measured_at_utc"])
        actual["reference_receipt"] = dict(_mapping(actual["reference_receipt"]))
        if actual != expected:
            raise RuntimeError("conflicting immutable Stage-4 no-signal outcome")
        return False

    @staticmethod
    def _load_due_legacy_and_prospective_events(
        conn, limit: int
    ) -> list[Dict[str, Any]]:
        """Load due delivered Alerts and authorized neutral anchors.

        Stage-4 signal snapshots use a separate paged validator below so their
        projection JSON cannot make this bounded legacy query scan the whole
        archive before applying its limit.
        """
        clauses = []
        condition_params: list[Any] = []
        for horizon in _HORIZONS:
            clauses.append(
                """
                (
                    e.alert_time_utc <= NOW() - (%s * INTERVAL '1 minute')
                    AND (
                        NOT EXISTS (
                            SELECT 1 FROM research_alert_outcomes current_o
                            WHERE current_o.event_id=e.event_id
                              AND current_o.horizon_minutes=%s
                              AND current_o.outcome_method_version=(
                                  CASE
                                      WHEN e.event_type=ANY(%s) THEN %s
                                      ELSE %s
                                  END
                              )
                              AND current_o.data_quality_status=ANY(%s)
                        )
                        OR (
                            e.direction IN ('LONG', 'SHORT')
                            AND NOT (e.event_type=ANY(%s))
                            AND NOT EXISTS (
                                SELECT 1 FROM research_first_touch_outcomes current_ft
                                WHERE current_ft.event_id=e.event_id
                                  AND current_ft.horizon_minutes=%s
                                  AND current_ft.method_version=%s
                                  AND current_ft.status IN ('HIT', 'MISS')
                                  AND current_ft.data_quality_status=ANY(%s)
                            )
                        )
                    )
                )
                """
            )
            condition_params.extend(
                (
                    horizon,
                    horizon,
                    list(_STAGE4_SIGNAL_OUTCOME_EVENT_TYPES),
                    _STAGE4_OUTCOME_METHOD_VERSION,
                    _METHOD_VERSION,
                    list(canonical_price_path.COMPLETE_QUALITIES),
                    list(_STAGE4_SIGNAL_OUTCOME_EVENT_TYPES),
                    horizon,
                    _FIRST_TOUCH_METHOD_VERSION,
                    list(canonical_price_path.COMPLETE_QUALITIES),
                )
            )
        query = f"""
            /* research_outcomes:due_legacy_and_prospective */
            SELECT e.event_id, e.alert_time_utc, e.symbol, e.direction,
                   e.event_type, e.setup_key,
                   e.event_kind, e.delivery_status,
                   e.current_price, e.target_price, e.engine_snapshot,
                   COALESCE(
                       jsonb_object_agg(
                           o.horizon_minutes,
                           CASE
                               WHEN o.outcome_method_version=(
                                    CASE
                                        WHEN e.event_type=ANY(%s) THEN %s
                                        ELSE %s
                                    END
                               )
                                AND o.data_quality_status=ANY(%s)
                               THEN o.outcome_method_version
                               ELSE COALESCE(o.outcome_method_version, '') || ':incomplete'
                           END
                       )
                           FILTER (WHERE o.event_id IS NOT NULL),
                       '{{}}'::jsonb
                   ) AS outcome_versions,
                   COALESCE(
                       (
                           SELECT jsonb_object_agg(
                               ft.horizon_minutes,
                               CASE
                                   WHEN ft.status IN ('HIT', 'MISS')
                                    AND ft.data_quality_status=ANY(%s)
                                   THEN ft.method_version
                                   ELSE COALESCE(ft.method_version, '') || ':' || ft.status
                               END
                           )
                           FROM research_first_touch_outcomes ft
                           WHERE ft.event_id=e.event_id
                             AND ft.method_version=%s
                       ),
                       '{{}}'::jsonb
                   ) AS first_touch_versions,
                   ARRAY[]::integer[] AS open_first_touch_horizons,
                   NULL::timestamptz AS open_first_touch_observed_utc,
                   {_alert_reference_queue_priority_sql("e")}
                       AS due_queue_priority
            FROM research_events e
            LEFT JOIN research_alert_outcomes o ON o.event_id=e.event_id
            WHERE (
                (e.event_kind='ALERT' AND e.delivery_status='DELIVERED')
                OR (
                    e.event_kind='DECISION_SAMPLE'
                    AND e.delivery_status='NOT_APPLICABLE'
                    AND NOT (e.event_type=ANY(%s))
                    AND EXISTS (
                        SELECT 1
                        FROM research_prospective_shadow_events authorized
                        WHERE authorized.event_id=e.event_id
                    )
                )
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM research_outcome_event_rejections rejected
                  WHERE rejected.event_id=e.event_id
                    AND rejected.rejection_policy_version=%s
              )
              AND ({' OR '.join(clauses)})
            GROUP BY e.event_id
            ORDER BY due_queue_priority ASC, e.alert_time_utc ASC, e.event_id ASC
            LIMIT %s
        """
        params: list[Any] = [
            list(_STAGE4_SIGNAL_OUTCOME_EVENT_TYPES),
            _STAGE4_OUTCOME_METHOD_VERSION,
            _METHOD_VERSION,
            list(canonical_price_path.COMPLETE_QUALITIES),
            list(canonical_price_path.COMPLETE_QUALITIES),
            _FIRST_TOUCH_METHOD_VERSION,
            list(_STAGE4_SIGNAL_OUTCOME_EVENT_TYPES),
            _ALERT_REFERENCE_REJECTION_POLICY_VERSION,
            *condition_params,
            max(1, min(int(limit), 1000)),
        ]
        return conn.execute(query, params).fetchall()

    @staticmethod
    def _load_due_stage4_candidate_page(
        conn,
        *,
        cursor: Optional[tuple[Any, int]] = None,
        upper_cursor: Optional[tuple[Any, int]] = None,
        page_size: int = _STAGE4_DUE_CANDIDATE_PAGE_SIZE,
    ) -> list[Dict[str, Any]]:
        """Load one cheap keyset page before Stage-4 projection validation."""

        due_clauses: list[str] = []
        due_params: list[Any] = []
        for horizon in _HORIZONS:
            due_clauses.append(
                """
                (
                    e.alert_time_utc <= NOW() - (%s * INTERVAL '1 minute')
                    AND NOT EXISTS (
                        SELECT 1
                        FROM research_alert_outcomes current_o
                        WHERE current_o.event_id=e.event_id
                          AND current_o.horizon_minutes=%s
                          AND current_o.outcome_method_version=%s
                          AND current_o.data_quality_status=ANY(%s)
                    )
                )
                """
            )
            due_params.extend(
                (
                    horizon,
                    horizon,
                    _STAGE4_OUTCOME_METHOD_VERSION,
                    list(canonical_price_path.COMPLETE_QUALITIES),
                )
            )
        cursor_clause = ""
        cursor_params: list[Any] = []
        if cursor is not None:
            cursor_clause = (
                "AND (e.alert_time_utc, e.event_id) > (%s, %s)"
            )
            cursor_params.extend((_utc(cursor[0]), int(cursor[1])))
        upper_cursor_clause = ""
        upper_cursor_params: list[Any] = []
        if upper_cursor is not None:
            upper_cursor_clause = (
                "AND (e.alert_time_utc, e.event_id) <= (%s, %s)"
            )
            upper_cursor_params.extend(
                (_utc(upper_cursor[0]), int(upper_cursor[1]))
            )
        bounded_page = max(
            1, min(int(page_size), _STAGE4_DUE_CANDIDATE_PAGE_SIZE)
        )
        query = f"""
            /* research_outcomes:due_stage4_candidate_page */
            SELECT e.event_id, e.alert_time_utc
            FROM research_events e
            WHERE e.event_kind='DECISION_SAMPLE'
              AND e.delivery_status='NOT_APPLICABLE'
              AND e.capture_stage=%s
              AND e.strategy_version=%s
              AND e.event_type=ANY(%s)
              AND e.direction IN ('LONG', 'SHORT')
              AND e.delivery_attempted_at_utc IS NULL
              AND e.delivered_at_utc IS NULL
              AND BTRIM(e.code_version)<>''
              AND BTRIM(e.runtime_session_id)<>''
              AND jsonb_typeof(e.categories)='array'
              AND e.categories @> '["DECISION_SAMPLE","SILENT"]'::jsonb
              AND e.engine_snapshot->'signal_snapshot'->>'contract_version'=%s
              AND e.engine_snapshot->'signal_snapshot'->>'signal_family'=
                    CASE e.event_type
                        WHEN 'MAX_PAIN_CONFIRMATION_STATE' THEN 'MAX_PAIN'
                        WHEN 'MAGNET_CONFIRMATION_STATE' THEN 'MAGNET'
                        WHEN 'SILENT_COMBINED_CONFIRMATION_SNAPSHOT' THEN 'COMBINED'
                    END
              AND e.engine_snapshot->'signal_snapshot'->'formula_authorized'
                    IS NOT DISTINCT FROM 'false'::jsonb
              AND e.engine_snapshot->'signal_snapshot'->'outcome_authorized'
                    IS NOT DISTINCT FROM 'false'::jsonb
              AND e.engine_snapshot->'signal_snapshot'->'telegram_delivery_allowed'
                    IS NOT DISTINCT FROM 'false'::jsonb
              AND e.engine_snapshot->'signal_snapshot'->'trade_execution_allowed'
                    IS NOT DISTINCT FROM 'false'::jsonb
              AND NOT EXISTS (
                    SELECT 1
                    FROM research_outcome_event_rejections rejected
                    WHERE rejected.event_id=e.event_id
                      AND rejected.rejection_policy_version=%s
              )
              AND ({' OR '.join(due_clauses)})
              {cursor_clause}
              {upper_cursor_clause}
            ORDER BY e.alert_time_utc ASC, e.event_id ASC
            LIMIT %s
        """
        params: list[Any] = [
            _STAGE4_SIGNAL_SNAPSHOT_CAPTURE_STAGE,
            _STAGE4_SIGNAL_SNAPSHOT_STRATEGY_VERSION,
            list(_STAGE4_SIGNAL_OUTCOME_EVENT_TYPES),
            _STAGE4_SIGNAL_SNAPSHOT_CONTRACT_VERSION,
            _ALERT_REFERENCE_REJECTION_POLICY_VERSION,
            *due_params,
            *cursor_params,
            *upper_cursor_params,
            bounded_page,
        ]
        return [dict(row) for row in conn.execute(query, params).fetchall()]

    @staticmethod
    def _validate_and_hydrate_due_stage4_events(
        conn, event_ids: Sequence[int]
    ) -> list[Dict[str, Any]]:
        """Validate only one bounded Stage-4 candidate page and hydrate it."""

        normalized = sorted(
            {int(event_id) for event_id in event_ids if int(event_id) > 0}
        )
        if not normalized:
            return []
        if len(normalized) > _STAGE4_DUE_CANDIDATE_PAGE_SIZE:
            raise ValueError("Stage-4 due validation page exceeds its hard bound")
        due_clauses: list[str] = []
        due_params: list[Any] = []
        for horizon in _HORIZONS:
            due_clauses.append(
                """
                (
                    e.alert_time_utc <= NOW() - (%s * INTERVAL '1 minute')
                    AND NOT EXISTS (
                        SELECT 1
                        FROM research_alert_outcomes current_o
                        WHERE current_o.event_id=e.event_id
                          AND current_o.horizon_minutes=%s
                          AND current_o.outcome_method_version=%s
                          AND current_o.data_quality_status=ANY(%s)
                    )
                )
                """
            )
            due_params.extend(
                (
                    horizon,
                    horizon,
                    _STAGE4_OUTCOME_METHOD_VERSION,
                    list(canonical_price_path.COMPLETE_QUALITIES),
                )
            )
        query = """
            /* research_outcomes:validate_due_stage4_page */
            WITH candidate AS MATERIALIZED (
                SELECT *
                FROM research_events
                WHERE event_id=ANY(%s)
            )
            SELECT e.event_id, e.alert_time_utc, e.symbol, e.direction,
                   e.event_type, e.setup_key,
                   e.event_kind, e.delivery_status,
                   e.current_price, e.target_price, e.engine_snapshot,
                   COALESCE(
                       (
                           SELECT jsonb_object_agg(
                               o.horizon_minutes,
                               CASE
                                   WHEN o.outcome_method_version=%s
                                    AND o.data_quality_status=ANY(%s)
                                   THEN o.outcome_method_version
                                   ELSE COALESCE(o.outcome_method_version, '')
                                        || ':incomplete'
                               END
                           )
                           FROM research_alert_outcomes o
                           WHERE o.event_id=e.event_id
                       ),
                       '{}'::jsonb
                   ) AS outcome_versions,
                   '{}'::jsonb AS first_touch_versions,
                   ARRAY[]::integer[] AS open_first_touch_horizons,
                   NULL::timestamptz AS open_first_touch_observed_utc,
                   0::integer AS due_queue_priority
            FROM candidate e
            WHERE e.event_kind='DECISION_SAMPLE'
              AND e.delivery_status='NOT_APPLICABLE'
              AND e.capture_stage=%s
              AND e.strategy_version=%s
              AND e.event_type=ANY(%s)
              AND e.direction IN ('LONG', 'SHORT')
              AND e.delivery_attempted_at_utc IS NULL
              AND e.delivered_at_utc IS NULL
              AND BTRIM(e.code_version)<>''
              AND BTRIM(e.runtime_session_id)<>''
              AND jsonb_typeof(e.categories)='array'
              AND e.categories @> '["DECISION_SAMPLE","SILENT"]'::jsonb
              AND e.engine_snapshot->'signal_snapshot'->>'contract_version'=%s
              AND e.engine_snapshot->'signal_snapshot'->>'signal_family'=
                    CASE e.event_type
                        WHEN 'MAX_PAIN_CONFIRMATION_STATE' THEN 'MAX_PAIN'
                        WHEN 'MAGNET_CONFIRMATION_STATE' THEN 'MAGNET'
                        WHEN 'SILENT_COMBINED_CONFIRMATION_SNAPSHOT' THEN 'COMBINED'
                    END
              AND e.engine_snapshot->'signal_snapshot'->'formula_authorized'
                    IS NOT DISTINCT FROM 'false'::jsonb
              AND e.engine_snapshot->'signal_snapshot'->'outcome_authorized'
                    IS NOT DISTINCT FROM 'false'::jsonb
              AND e.engine_snapshot->'signal_snapshot'->'telegram_delivery_allowed'
                    IS NOT DISTINCT FROM 'false'::jsonb
              AND e.engine_snapshot->'signal_snapshot'->'trade_execution_allowed'
                    IS NOT DISTINCT FROM 'false'::jsonb
              AND NOT EXISTS (
                    SELECT 1
                    FROM research_outcome_event_rejections rejected
                    WHERE rejected.event_id=e.event_id
                      AND rejected.rejection_policy_version=%s
              )
              AND (__STAGE4_DUE_CLAUSES__)
              AND EXISTS (
                    SELECT 1
                    FROM research_events projection
                    CROSS JOIN LATERAL (
                        SELECT
                            COUNT(*) FILTER (
                                WHERE symbol_evaluation->>'symbol'=e.symbol
                            ) AS symbol_count,
                            COUNT(*) FILTER (
                                WHERE symbol_evaluation->>'symbol'=e.symbol
                                  AND symbol_evaluation->>'status'='EVALUABLE'
                                  AND symbol_evaluation->'reason'
                                        IS NOT DISTINCT FROM 'null'::jsonb
                            ) AS evaluable_symbol_count
                        FROM jsonb_array_elements(
                            CASE
                                WHEN jsonb_typeof(
                                    projection.engine_snapshot->'projection'->'symbol_evaluations'
                                )='array'
                                THEN projection.engine_snapshot->'projection'->'symbol_evaluations'
                                ELSE '[]'::jsonb
                            END
                        ) symbol_evaluation
                    ) evaluation_partition
                    WHERE projection.event_kind='DECISION_SAMPLE'
                      AND projection.delivery_status='NOT_APPLICABLE'
                      AND projection.capture_stage=%s
                      AND projection.strategy_version=%s
                      AND projection.event_type='SIGNAL_SNAPSHOT_PROJECTION'
                      AND projection.symbol='RESEARCH'
                      AND projection.direction='NEUTRAL'
                      AND projection.delivery_attempted_at_utc IS NULL
                      AND projection.delivered_at_utc IS NULL
                      AND jsonb_typeof(projection.categories)='array'
                      AND jsonb_array_length(projection.categories)=3
                      AND projection.categories @>
                            '["DECISION_SAMPLE","SILENT","COMPLETED"]'::jsonb
                      AND projection.alert_time_utc=e.alert_time_utc
                      AND projection.code_version=e.code_version
                      AND projection.runtime_session_id=e.runtime_session_id
                      AND projection.engine_snapshot->'signal_snapshot'->>'contract_version'=%s
                      AND projection.engine_snapshot->'signal_snapshot'->>'signal_family'='PROJECTION'
                      AND projection.engine_snapshot->'signal_snapshot'->>'tier'='COMPLETED'
                      AND projection.engine_snapshot->'signal_snapshot'->'formula_authorized'
                            IS NOT DISTINCT FROM 'false'::jsonb
                      AND projection.engine_snapshot->'signal_snapshot'->'outcome_authorized'
                            IS NOT DISTINCT FROM 'false'::jsonb
                      AND projection.engine_snapshot->'signal_snapshot'->'telegram_delivery_allowed'
                            IS NOT DISTINCT FROM 'false'::jsonb
                      AND projection.engine_snapshot->'signal_snapshot'->'trade_execution_allowed'
                            IS NOT DISTINCT FROM 'false'::jsonb
                      AND projection.engine_snapshot->'projection'->>'status'='COMPLETED'
                      AND projection.engine_snapshot->'projection'->>'decision_time_utc'=
                            e.engine_snapshot->'signal_snapshot'->>'decision_time_utc'
                      AND projection.engine_snapshot->'projection'->>'snapshot_key'
                            ~ '^[0-9a-f]{64}$'
                      AND projection.engine_snapshot->'projection'->>'snapshot_key'=
                            e.engine_snapshot->'signal_snapshot'->'archive_reference'->>'snapshot_key'
                      AND evaluation_partition.symbol_count=1
                      AND evaluation_partition.evaluable_symbol_count=1
              )
            ORDER BY e.alert_time_utc ASC, e.event_id ASC
        """
        query = query.replace(
            "__STAGE4_DUE_CLAUSES__", " OR ".join(due_clauses)
        )
        params: list[Any] = [
            normalized,
            _STAGE4_OUTCOME_METHOD_VERSION,
            list(canonical_price_path.COMPLETE_QUALITIES),
            _STAGE4_SIGNAL_SNAPSHOT_CAPTURE_STAGE,
            _STAGE4_SIGNAL_SNAPSHOT_STRATEGY_VERSION,
            list(_STAGE4_SIGNAL_OUTCOME_EVENT_TYPES),
            _STAGE4_SIGNAL_SNAPSHOT_CONTRACT_VERSION,
            _ALERT_REFERENCE_REJECTION_POLICY_VERSION,
            *due_params,
            _STAGE4_SIGNAL_SNAPSHOT_CAPTURE_STAGE,
            _STAGE4_SIGNAL_SNAPSHOT_STRATEGY_VERSION,
            _STAGE4_SIGNAL_SNAPSHOT_CONTRACT_VERSION,
        ]
        return [dict(row) for row in conn.execute(query, params).fetchall()]

    @staticmethod
    def _stage4_due_cursor_from_row(
        row: Mapping[str, Any], *, prefix: str
    ) -> Optional[tuple[datetime, int]]:
        time_value = row.get(f"{prefix}_alert_time_utc")
        event_id = row.get(f"{prefix}_event_id")
        if (time_value is None) != (event_id is None):
            raise RuntimeError("Stage-4 due scan cursor pair is inconsistent")
        if time_value is None:
            return None
        normalized_id = int(event_id)
        if normalized_id <= 0:
            raise RuntimeError("Stage-4 due scan cursor event_id is invalid")
        return (_utc(time_value), normalized_id)

    @classmethod
    def _acquire_stage4_due_scan_state(
        cls, conn
    ) -> Optional[Dict[str, Any]]:
        """Lock and load the singleton durable Stage-4 signal scan state."""

        lock_row = conn.execute(
            "SELECT pg_catalog.pg_try_advisory_xact_lock(%s) AS acquired",
            (_STAGE4_DUE_SCAN_LOCK_ID,),
        ).fetchone()
        if not lock_row or lock_row.get("acquired") is not True:
            return None
        conn.execute(
            """
            INSERT INTO public.research_stage4_signal_scan_state_v1 (
                scan_key, state_version
            ) VALUES (%s, %s)
            ON CONFLICT (scan_key) DO NOTHING
            """,
            (_STAGE4_DUE_SCAN_STATE_KEY, _STAGE4_DUE_SCAN_STATE_VERSION),
        )
        row = conn.execute(
            """
            SELECT scan_key, state_version,
                   cursor_alert_time_utc, cursor_event_id,
                   lap_upper_alert_time_utc, lap_upper_event_id,
                   completed_laps, pages_scanned, candidates_scanned,
                   updated_at_utc
              FROM public.research_stage4_signal_scan_state_v1
             WHERE scan_key=%s
             FOR UPDATE
            """,
            (_STAGE4_DUE_SCAN_STATE_KEY,),
        ).fetchone()
        if not row:
            raise RuntimeError("Stage-4 due scan state row is unavailable")
        state = dict(row)
        if state.get("state_version") != _STAGE4_DUE_SCAN_STATE_VERSION:
            raise RuntimeError("Stage-4 due scan state version is incompatible")
        cursor = cls._stage4_due_cursor_from_row(state, prefix="cursor")
        upper = cls._stage4_due_cursor_from_row(state, prefix="lap_upper")
        if cursor is not None and upper is None:
            raise RuntimeError("Stage-4 due scan cursor has no lap high-water")
        if cursor is not None and cursor > upper:
            raise RuntimeError("Stage-4 due scan cursor exceeds lap high-water")
        if upper is None:
            row = conn.execute(
                """
                UPDATE public.research_stage4_signal_scan_state_v1
                   SET lap_upper_alert_time_utc=(
                           pg_catalog.transaction_timestamp()
                           - (%s * INTERVAL '1 minute')
                       ),
                       lap_upper_event_id=%s,
                       updated_at_utc=pg_catalog.transaction_timestamp()
                 WHERE scan_key=%s AND state_version=%s
                 RETURNING scan_key, state_version,
                           cursor_alert_time_utc, cursor_event_id,
                           lap_upper_alert_time_utc, lap_upper_event_id,
                           completed_laps, pages_scanned,
                           candidates_scanned, updated_at_utc
                """,
                (
                    min(_HORIZONS),
                    _STAGE4_DUE_SCAN_MAX_EVENT_ID,
                    _STAGE4_DUE_SCAN_STATE_KEY,
                    _STAGE4_DUE_SCAN_STATE_VERSION,
                ),
            ).fetchone()
            if not row:
                raise RuntimeError("Stage-4 due scan high-water initialization failed")
            state = dict(row)
            upper = cls._stage4_due_cursor_from_row(
                state, prefix="lap_upper"
            )
        if upper is None:
            raise RuntimeError("Stage-4 due scan lap high-water is unavailable")
        state["cursor"] = cursor
        state["lap_upper"] = upper
        return state

    @staticmethod
    def _record_stage4_due_scan_state(
        conn,
        *,
        cursor: Optional[tuple[Any, int]],
        lap_complete: bool,
        pages_scanned: int,
        candidates_scanned: int,
    ) -> None:
        """Persist one acknowledged scan prefix inside the caller transaction."""

        cursor_time = None if cursor is None else _utc(cursor[0])
        cursor_event_id = None if cursor is None else int(cursor[1])
        if lap_complete:
            cursor_time = None
            cursor_event_id = None
        row = conn.execute(
            """
            UPDATE public.research_stage4_signal_scan_state_v1
               SET cursor_alert_time_utc=%s,
                   cursor_event_id=%s,
                   lap_upper_alert_time_utc=(
                       CASE WHEN %s THEN NULL ELSE lap_upper_alert_time_utc END
                   ),
                   lap_upper_event_id=(
                       CASE WHEN %s THEN NULL ELSE lap_upper_event_id END
                   ),
                   completed_laps=completed_laps+(
                       CASE WHEN %s THEN 1 ELSE 0 END
                   ),
                   pages_scanned=pages_scanned+%s,
                   candidates_scanned=candidates_scanned+%s,
                   updated_at_utc=pg_catalog.transaction_timestamp()
             WHERE scan_key=%s AND state_version=%s
             RETURNING scan_key
            """,
            (
                cursor_time,
                cursor_event_id,
                bool(lap_complete),
                bool(lap_complete),
                bool(lap_complete),
                max(0, int(pages_scanned)),
                max(0, int(candidates_scanned)),
                _STAGE4_DUE_SCAN_STATE_KEY,
                _STAGE4_DUE_SCAN_STATE_VERSION,
            ),
        ).fetchone()
        if not row:
            raise RuntimeError("Stage-4 due scan state acknowledgement failed")

    @staticmethod
    def _set_stage4_due_statement_timeout(conn, remaining_ms: int) -> int:
        """Bound the next heavy statement by both query and cycle budgets."""

        bounded = max(
            1,
            min(
                int(remaining_ms),
                research_database_timeout.heavy_statement_timeout_ms(),
            ),
        )
        conn.execute(
            "SELECT pg_catalog.set_config('statement_timeout', %s, true)",
            (f"{bounded}ms",),
        )
        return bounded

    @staticmethod
    def _stage4_due_scan_remaining_ms(deadline: float) -> int:
        return max(0, int(math.ceil((deadline - time.monotonic()) * 1000)))

    @staticmethod
    def _stage4_due_cursor_receipt(
        cursor: Optional[tuple[Any, int]],
    ) -> Optional[Dict[str, Any]]:
        if cursor is None:
            return None
        return {
            "alert_time_utc": _utc(cursor[0]).isoformat(),
            "event_id": int(cursor[1]),
        }

    def _load_due_stage4_events(
        self, conn, limit: int
    ) -> list[Dict[str, Any]]:
        """Run one restart-safe, finite lap segment over Stage-4 signals."""

        bounded_limit = max(1, min(int(limit), 1000))
        self._active_stage4_due_scan = {
            "lock_acquired": False,
            "pages_scanned": 0,
            "heavy_statements": 0,
            "candidates_scanned": 0,
        }
        state = self._acquire_stage4_due_scan_state(conn)
        if state is None:
            del self._active_stage4_due_scan
            self._last_stage4_due_scan = {
                "state_version": _STAGE4_DUE_SCAN_STATE_VERSION,
                "stop_reason": "LOCK_BUSY",
                "lock_acquired": False,
                "pages_scanned": 0,
                "heavy_statements": 0,
                "candidates_scanned": 0,
                "accepted": 0,
            }
            return []
        self._active_stage4_due_scan["lock_acquired"] = True

        initial_cursor = state["cursor"]
        cursor = initial_cursor
        upper = state["lap_upper"]
        accepted: list[Dict[str, Any]] = []
        accepted_keys: list[tuple[datetime, int]] = []
        zero_result_cursor = initial_cursor
        pages_scanned = 0
        heavy_statements = 0
        candidates_scanned = 0
        lap_complete = False
        stop_reason = "PAGE_BUDGET"
        started = time.monotonic()
        deadline = started + (_STAGE4_DUE_SCAN_BUDGET_MS / 1000.0)

        for _ in range(_STAGE4_DUE_SCAN_MAX_PAGES):
            remaining_ms = self._stage4_due_scan_remaining_ms(deadline)
            if remaining_ms <= 0:
                stop_reason = "WALL_TIME_BUDGET"
                break
            self._set_stage4_due_statement_timeout(conn, remaining_ms)
            page = self._load_due_stage4_candidate_page(
                conn,
                cursor=cursor,
                upper_cursor=upper,
                page_size=_STAGE4_DUE_CANDIDATE_PAGE_SIZE,
            )
            pages_scanned += 1
            heavy_statements += 1
            candidates_scanned += len(page)
            self._active_stage4_due_scan.update(
                pages_scanned=pages_scanned,
                heavy_statements=heavy_statements,
                candidates_scanned=candidates_scanned,
            )
            if not page:
                lap_complete = True
                stop_reason = "LAP_COMPLETE"
                break

            page_keys: Dict[int, tuple[datetime, int]] = {}
            for candidate in page:
                event_id = int(candidate["event_id"])
                if event_id in page_keys:
                    raise RuntimeError("Stage-4 candidate page contains duplicates")
                key = (_utc(candidate["alert_time_utc"]), event_id)
                if key > upper or (cursor is not None and key <= cursor):
                    raise RuntimeError("Stage-4 candidate page escaped its keyset")
                page_keys[event_id] = key

            remaining_ms = self._stage4_due_scan_remaining_ms(deadline)
            if remaining_ms <= 0:
                stop_reason = "WALL_TIME_BUDGET"
                break
            self._set_stage4_due_statement_timeout(conn, remaining_ms)
            hydrated = self._validate_and_hydrate_due_stage4_events(
                conn, list(page_keys)
            )
            heavy_statements += 1
            self._active_stage4_due_scan["heavy_statements"] = heavy_statements
            hydrated_by_id: Dict[int, Dict[str, Any]] = {}
            for raw in hydrated:
                event = dict(raw)
                event_id = int(event["event_id"])
                if event_id not in page_keys or event_id in hydrated_by_id:
                    raise RuntimeError(
                        "Stage-4 hydration returned an invalid candidate identity"
                    )
                hydrated_by_id[event_id] = event
            ordered_hydrated = [
                hydrated_by_id[event_id]
                for event_id, _key in sorted(
                    page_keys.items(), key=lambda item: item[1]
                )
                if event_id in hydrated_by_id
            ]
            capacity = bounded_limit - len(accepted)
            retained = ordered_hydrated[:capacity]
            for event in retained:
                event_id = int(event["event_id"])
                accepted.append(event)
                accepted_keys.append(page_keys[event_id])

            if len(ordered_hydrated) > len(retained):
                cursor = accepted_keys[-1]
                stop_reason = "RESULT_LIMIT"
                break

            cursor = (
                _utc(page[-1]["alert_time_utc"]),
                int(page[-1]["event_id"]),
            )
            if not accepted:
                zero_result_cursor = cursor
            if len(accepted) >= bounded_limit:
                stop_reason = "RESULT_LIMIT"
                break

        elapsed_ms = max(0, int(round((time.monotonic() - started) * 1000)))
        self._set_stage4_due_statement_timeout(
            conn, research_database_timeout.heavy_statement_timeout_ms()
        )
        self._pending_stage4_due_scan = {
            "initial_cursor": initial_cursor,
            "zero_result_cursor": zero_result_cursor,
            "final_cursor": cursor,
            "lap_upper": upper,
            "lap_complete": lap_complete,
            "accepted_ids": [int(row["event_id"]) for row in accepted],
            "accepted_keys": accepted_keys,
            "pages_scanned": pages_scanned,
            "heavy_statements": heavy_statements,
            "candidates_scanned": candidates_scanned,
            "elapsed_ms": elapsed_ms,
            "stop_reason": stop_reason,
        }
        del self._active_stage4_due_scan
        return accepted

    def _acknowledge_stage4_due_scan(
        self, conn, included_event_ids: Sequence[int]
    ) -> None:
        pending = getattr(self, "_pending_stage4_due_scan", None)
        if not isinstance(pending, dict):
            return
        accepted_ids = list(pending["accepted_ids"])
        included_id_set = {int(value) for value in included_event_ids}
        included = [
            event_id
            for event_id in accepted_ids
            if int(event_id) in included_id_set
        ]
        if included != accepted_ids[: len(included)]:
            raise RuntimeError("Stage-4 merge retained a non-prefix scan result")
        retained_count = len(included)
        all_retained = retained_count == len(accepted_ids)
        if all_retained:
            acknowledged_cursor = pending["final_cursor"]
            lap_complete = bool(pending["lap_complete"])
        elif retained_count:
            acknowledged_cursor = pending["accepted_keys"][retained_count - 1]
            lap_complete = False
        else:
            acknowledged_cursor = pending["zero_result_cursor"]
            lap_complete = False
        if not accepted_ids:
            lap_complete = bool(pending["lap_complete"])

        self._record_stage4_due_scan_state(
            conn,
            cursor=acknowledged_cursor,
            lap_complete=lap_complete,
            pages_scanned=int(pending["pages_scanned"]),
            candidates_scanned=int(pending["candidates_scanned"]),
        )
        budget_exhausted = pending["stop_reason"] in {
            "PAGE_BUDGET",
            "WALL_TIME_BUDGET",
        }
        self._pending_stage4_due_telemetry = {
            "state_version": _STAGE4_DUE_SCAN_STATE_VERSION,
            "stop_reason": pending["stop_reason"],
            "lock_acquired": True,
            "pages_scanned": int(pending["pages_scanned"]),
            "heavy_statements": int(pending["heavy_statements"]),
            "candidates_scanned": int(pending["candidates_scanned"]),
            "accepted": len(accepted_ids),
            "retained_by_merge": retained_count,
            "elapsed_ms": int(pending["elapsed_ms"]),
            "budget_exhausted": budget_exhausted,
            "lap_completed": lap_complete,
            "cursor": self._stage4_due_cursor_receipt(
                None if lap_complete else acknowledged_cursor
            ),
            "lap_upper": self._stage4_due_cursor_receipt(
                None if lap_complete else pending["lap_upper"]
            ),
        }
        del self._pending_stage4_due_scan

    def _commit_stage4_due_scan_telemetry(self) -> None:
        """Publish cursor telemetry only after its DB transaction commits."""

        pending = getattr(self, "_pending_stage4_due_telemetry", None)
        if not isinstance(pending, dict):
            return
        self.metrics.stage4_signal_scan_pages += int(pending["pages_scanned"])
        self.metrics.stage4_signal_scan_candidates += int(
            pending["candidates_scanned"]
        )
        self.metrics.stage4_signal_scan_budget_exhaustions += int(
            bool(pending["budget_exhausted"])
        )
        self.metrics.stage4_signal_scan_laps_completed += int(
            bool(pending["lap_completed"])
        )
        self.metrics.stage4_signal_scan_last_error = None
        self._last_stage4_due_scan = dict(pending)
        del self._pending_stage4_due_telemetry

    def _discard_stage4_due_scan_telemetry(self) -> None:
        if hasattr(self, "_pending_stage4_due_telemetry"):
            del self._pending_stage4_due_telemetry

    @staticmethod
    def _due_queue_order_key(row: Mapping[str, Any]) -> tuple[int, datetime, int]:
        return (
            int(row.get("due_queue_priority") or 0),
            _utc(row["alert_time_utc"]),
            int(row["event_id"]),
        )

    @classmethod
    def _merge_due_queue_rows(
        cls,
        legacy_rows: Sequence[Mapping[str, Any]],
        stage4_rows: Sequence[Mapping[str, Any]],
        *,
        limit: int,
    ) -> list[Dict[str, Any]]:
        """Keep global order while reserving progress for both due queues."""

        bounded_limit = max(1, min(int(limit), 1000))
        legacy = sorted(
            (dict(row) for row in legacy_rows), key=cls._due_queue_order_key
        )
        stage4 = sorted(
            (dict(row) for row in stage4_rows), key=cls._due_queue_order_key
        )
        legacy_ids = {int(row["event_id"]) for row in legacy}
        stage4_ids = {int(row["event_id"]) for row in stage4}
        if len(legacy_ids) != len(legacy) or len(stage4_ids) != len(stage4):
            raise RuntimeError("due queue contains duplicate event identities")
        if legacy_ids & stage4_ids:
            raise RuntimeError("legacy and Stage-4 due queues overlap")
        combined = sorted([*legacy, *stage4], key=cls._due_queue_order_key)
        if len(combined) <= bounded_limit or not legacy or not stage4:
            return combined[:bounded_limit]

        reserve = max(
            1, bounded_limit // _STAGE4_DUE_RESERVED_QUEUE_DIVISOR
        )
        stage4_quota = min(len(stage4), reserve, bounded_limit)
        legacy_quota = min(
            len(legacy), reserve, bounded_limit - stage4_quota
        )
        selected = [*stage4[:stage4_quota], *legacy[:legacy_quota]]
        selected_ids = {int(row["event_id"]) for row in selected}
        for row in combined:
            if len(selected) >= bounded_limit:
                break
            event_id = int(row["event_id"])
            if event_id in selected_ids:
                continue
            selected.append(row)
            selected_ids.add(event_id)
        return sorted(selected, key=cls._due_queue_order_key)

    def _load_open_first_touch_events_isolated(
        self, conn, limit: int
    ) -> list[Dict[str, Any]]:
        """Keep an open-horizon timeout from starving both closed queues."""

        savepoint = "research_open_first_touch_load"
        conn.execute(f"SAVEPOINT {savepoint}")
        try:
            rows = self._load_open_first_touch_events(conn, limit)
        except Exception as exc:
            conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            conn.execute(f"RELEASE SAVEPOINT {savepoint}")
            self.metrics.open_first_touch_load_failures += 1
            self.metrics.open_first_touch_load_last_error = (
                f"{type(exc).__name__}: {str(exc)[:500]}"
            )
            if "statement timeout" in str(exc).lower():
                self.metrics.last_timeout_phase = "LOAD_OPEN_FIRST_TOUCH"
            print(
                "[research-outcomes] isolated open First-Touch load "
                f"failure: {exc!r}",
                flush=True,
            )
            return []
        conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        self.metrics.open_first_touch_load_last_error = None
        return rows

    def _load_due_events(self, conn, limit: int) -> list[Dict[str, Any]]:
        """Merge bounded queues and isolate a fail-closed Stage-4 scan."""

        bounded_limit = max(1, min(int(limit), 1000))
        legacy_savepoint = "research_legacy_due_load"
        conn.execute(f"SAVEPOINT {legacy_savepoint}")
        try:
            legacy_rows = self._load_due_legacy_and_prospective_events(
                conn, bounded_limit
            )
        except Exception as exc:
            conn.execute(f"ROLLBACK TO SAVEPOINT {legacy_savepoint}")
            conn.execute(f"RELEASE SAVEPOINT {legacy_savepoint}")
            self.metrics.legacy_due_load_failures += 1
            self.metrics.legacy_due_load_last_error = (
                f"{type(exc).__name__}: {str(exc)[:500]}"
            )
            if "statement timeout" in str(exc).lower():
                self.metrics.last_timeout_phase = "LOAD_DUE_LEGACY"
            print(
                "[research-outcomes] isolated legacy/prospective due load "
                f"failure: {exc!r}",
                flush=True,
            )
            legacy_rows = []
        else:
            conn.execute(f"RELEASE SAVEPOINT {legacy_savepoint}")
            self.metrics.legacy_due_load_last_error = None
        savepoint = "research_stage4_signal_due_scan"
        conn.execute(f"SAVEPOINT {savepoint}")
        try:
            stage4_rows = self._load_due_stage4_events(conn, bounded_limit)
            ordered = self._merge_due_queue_rows(
                legacy_rows, stage4_rows, limit=bounded_limit
            )
            stage4_ids = {int(row["event_id"]) for row in stage4_rows}
            self._acknowledge_stage4_due_scan(
                conn,
                [
                    int(row["event_id"])
                    for row in ordered
                    if int(row["event_id"]) in stage4_ids
                ],
            )
        except Exception as exc:
            conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            conn.execute(f"RELEASE SAVEPOINT {savepoint}")
            if hasattr(self, "_pending_stage4_due_scan"):
                del self._pending_stage4_due_scan
            self._discard_stage4_due_scan_telemetry()
            active_scan = getattr(self, "_active_stage4_due_scan", {})
            if hasattr(self, "_active_stage4_due_scan"):
                del self._active_stage4_due_scan
            self.metrics.stage4_signal_scan_failures += 1
            self.metrics.stage4_signal_scan_last_error = (
                f"{type(exc).__name__}: {str(exc)[:500]}"
            )
            if "statement timeout" in str(exc).lower():
                self.metrics.last_timeout_phase = "LOAD_DUE_STAGE4_SIGNAL"
            self._last_stage4_due_scan = {
                "state_version": _STAGE4_DUE_SCAN_STATE_VERSION,
                "stop_reason": "FAILED_CLOSED",
                "lock_acquired": bool(active_scan.get("lock_acquired")),
                "pages_scanned": int(active_scan.get("pages_scanned") or 0),
                "heavy_statements": int(
                    active_scan.get("heavy_statements") or 0
                ),
                "candidates_scanned": int(
                    active_scan.get("candidates_scanned") or 0
                ),
                "accepted": 0,
                "error_type": type(exc).__name__,
            }
            print(
                "[research-outcomes] isolated Stage-4 signal due scan "
                f"failure: {exc!r}",
                flush=True,
            )
            ordered = sorted(
                (dict(row) for row in legacy_rows),
                key=self._due_queue_order_key,
            )[:bounded_limit]
        else:
            conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        for row in ordered:
            row.pop("due_queue_priority", None)
        return ordered

    @staticmethod
    def _load_open_first_touch_events(conn, limit: int) -> list[Dict[str, Any]]:
        """Load eligible matched Shadow events with a newly closed 1m candle.

        This query has its own reserved, bounded queue so a closed historical
        backlog cannot delay first-touch detection.  Formula horizons are
        deduplicated before the worker fetches one canonical path per event.
        Delivered Alerts and authorized silent Decision Samples are the only
        admitted event classes; this query never reads any delivery queue.
        """
        query = f"""
            SELECT e.event_id, e.alert_time_utc, e.symbol, e.direction,
                   e.event_type, e.setup_key,
                   e.event_kind, e.delivery_status,
                   e.current_price, e.target_price, e.engine_snapshot,
                   '{{}}'::jsonb AS outcome_versions,
                   COALESCE(
                       (
                           SELECT jsonb_object_agg(
                               ft.horizon_minutes,
                               CASE
                                   WHEN ft.status IN ('HIT', 'MISS')
                                    AND ft.data_quality_status=ANY(%s)
                                   THEN ft.method_version
                                   ELSE COALESCE(ft.method_version, '') || ':' || ft.status
                               END
                           )
                           FROM research_first_touch_outcomes ft
                           WHERE ft.event_id=e.event_id
                             AND ft.method_version=%s
                       ),
                       '{{}}'::jsonb
                   ) AS first_touch_versions,
                   ARRAY_AGG(
                       DISTINCT open_formula.horizon_minutes
                       ORDER BY open_formula.horizon_minutes
                   ) AS open_first_touch_horizons,
                   MIN(open_pending.observed_through_utc)
                       AS open_first_touch_observed_utc
            FROM research_events e
            JOIN research_formula_shadow_checks open_check
              ON open_check.event_id=e.event_id
             AND open_check.matched=TRUE
             AND open_check.evaluation_status='MATCHED'
             AND open_check.evidence_policy_version=%s
             AND open_check.authoritative_verified=TRUE
             AND open_check.prospective_anchor_slot_id IS NOT NULL
             AND BTRIM(open_check.prospective_input_fingerprint)
                    ~ '^[0-9a-f]{{64}}$'
             AND BTRIM(open_check.feature_bundle_sha256)
                    ~ '^[0-9a-f]{{64}}$'
            JOIN research_formulas open_formula
              ON open_formula.formula_id=open_check.formula_id
             AND open_formula.active=TRUE
             AND open_formula.current_stage='SHADOW'
            LEFT JOIN research_first_touch_outcomes open_pending
              ON open_pending.event_id=e.event_id
             AND open_pending.horizon_minutes=open_formula.horizon_minutes
             AND open_pending.method_version=%s
             AND open_pending.status='PENDING'
            WHERE e.direction IN ('LONG', 'SHORT')
              AND (
                    (
                        e.event_kind='ALERT'
                        AND e.delivery_status='DELIVERED'
                    )
                    OR (
                        e.event_kind='DECISION_SAMPLE'
                        AND e.delivery_status='NOT_APPLICABLE'
                        AND EXISTS (
                            SELECT 1
                            FROM research_prospective_shadow_events authorized
                            WHERE authorized.event_id=e.event_id
                              AND authorized.anchor_slot_id=
                                  open_check.prospective_anchor_slot_id
                              AND BTRIM(authorized.input_fingerprint)=
                                  BTRIM(open_check.prospective_input_fingerprint)
                              AND BTRIM(
                                  authorized.feature_bundle_sha256
                              )=BTRIM(open_check.feature_bundle_sha256)
                        )
                    )
                  )
              AND NOT EXISTS (
                  SELECT 1
                  FROM research_outcome_event_rejections rejected
                  WHERE rejected.event_id=e.event_id
                    AND rejected.rejection_policy_version=%s
              )
              AND e.alert_time_utc
                    + (open_formula.horizon_minutes * INTERVAL '1 minute')
                  > NOW()
              AND date_trunc('minute', e.alert_time_utc)
                    + INTERVAL '1 minute'
                    + CASE
                        WHEN e.alert_time_utc > date_trunc(
                            'minute', e.alert_time_utc
                        ) THEN INTERVAL '1 minute'
                        ELSE INTERVAL '0 minutes'
                      END
                  <= date_trunc('minute', NOW())
              AND NOT EXISTS (
                  SELECT 1
                  FROM research_first_touch_outcomes open_ft
                  WHERE open_ft.event_id=e.event_id
                    AND open_ft.horizon_minutes=open_formula.horizon_minutes
                    AND open_ft.method_version=%s
                    AND (
                        (
                            open_ft.status IN ('HIT', 'MISS')
                            AND open_ft.data_quality_status=ANY(%s)
                        )
                        OR (
                            open_ft.status='PENDING'
                            AND open_ft.data_quality_status=ANY(%s)
                            AND open_ft.observed_through_utc >=
                                date_trunc('minute', NOW())
                                - INTERVAL '1 millisecond'
                        )
                    )
              )
            GROUP BY e.event_id
            ORDER BY
                {_alert_reference_queue_priority_sql("e")} ASC,
                COALESCE(
                    MIN(open_pending.observed_through_utc), e.alert_time_utc
                ) ASC,
                e.event_id ASC
            LIMIT %s
        """
        params = [
            list(canonical_price_path.COMPLETE_QUALITIES),
            _FIRST_TOUCH_METHOD_VERSION,
            _STRICT_FROZEN_EVIDENCE_POLICY_VERSION,
            _FIRST_TOUCH_METHOD_VERSION,
            _ALERT_REFERENCE_REJECTION_POLICY_VERSION,
            _FIRST_TOUCH_METHOD_VERSION,
            list(canonical_price_path.COMPLETE_QUALITIES),
            list(canonical_price_path.COMPLETE_QUALITIES),
            max(1, min(int(limit), _OPEN_FIRST_TOUCH_EVENT_LIMIT)),
        ]
        return conn.execute(query, params).fetchall()

    @staticmethod
    def _write_alert_reference_rejections(
        conn, rejections: Sequence[Mapping[str, Any]]
    ) -> int:
        """Persist one immutable audit row per event and policy version."""
        payload = []
        for rejection in rejections:
            event = rejection["event"]
            symbol = str(event.get("symbol") or "").strip().upper()
            payload.append(
                {
                    "event_id": int(event["event_id"]),
                    "reason_code": (
                        "HYPE_REFERENCE_PROVENANCE"
                        if symbol == "HYPE"
                        else "BINANCE_REFERENCE_PROVENANCE"
                    ),
                    "reason_text": str(rejection["reason"]),
                    "event_snapshot": {
                        "event_kind": str(event.get("event_kind") or ""),
                        "delivery_status": str(
                            event.get("delivery_status") or ""
                        ),
                        "symbol": symbol,
                        "price_provenance": _snapshot_price_provenance(
                            event.get("engine_snapshot")
                        ),
                    },
                }
            )
        if not payload:
            return 0
        row = conn.execute(
            """
            WITH candidates AS (
                SELECT *
                FROM jsonb_to_recordset(%s::jsonb) AS item(
                    event_id BIGINT,
                    reason_code TEXT,
                    reason_text TEXT,
                    event_snapshot JSONB
                )
            ), inserted AS (
                INSERT INTO research_outcome_event_rejections (
                    event_id, rejection_policy_version, reason_code,
                    reason_text, event_snapshot
                )
                SELECT event_id, %s, reason_code, reason_text, event_snapshot
                FROM candidates
                ON CONFLICT (event_id, rejection_policy_version) DO NOTHING
                RETURNING event_id
            )
            SELECT COUNT(*) AS inserted FROM inserted
            """,
            (
                json.dumps(payload, ensure_ascii=False, default=str),
                _ALERT_REFERENCE_REJECTION_POLICY_VERSION,
            ),
        ).fetchone()
        return int(row["inserted"] or 0)

    @staticmethod
    def _write_outcome(
        conn,
        *,
        event: Dict[str, Any],
        horizon: int,
        reference_price: float,
        reference_source: str,
        path_result: Dict[str, Any],
        path_metrics: Dict[str, Any],
        complete: bool,
    ) -> bool:
        source = _path_source(reference_source, path_result)
        outcome_method_version = _outcome_method_version_for_event(event)
        quality = canonical_price_path.quality_status(path_result, complete=complete)
        row = conn.execute(
            """
            INSERT INTO research_alert_outcomes (
                event_id, horizon_minutes, measured_at_utc,
                reference_price, price_at_horizon, raw_return_pct,
                directional_return_pct, max_favorable_price,
                max_adverse_price, mfe_pct, mae_pct,
                time_to_first_progress_seconds, time_to_mfe_seconds,
                time_to_closest_target_seconds, time_to_target_seconds,
                closest_target_price, closest_target_distance_pct,
                target_progress_ratio, target_reached,
                path_resolution_seconds, path_samples,
                outcome_method_version, price_source, data_quality_status
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (event_id, horizon_minutes) DO UPDATE SET
                measured_at_utc=EXCLUDED.measured_at_utc,
                reference_price=EXCLUDED.reference_price,
                price_at_horizon=EXCLUDED.price_at_horizon,
                raw_return_pct=EXCLUDED.raw_return_pct,
                directional_return_pct=EXCLUDED.directional_return_pct,
                max_favorable_price=EXCLUDED.max_favorable_price,
                max_adverse_price=EXCLUDED.max_adverse_price,
                mfe_pct=EXCLUDED.mfe_pct,
                mae_pct=EXCLUDED.mae_pct,
                time_to_first_progress_seconds=EXCLUDED.time_to_first_progress_seconds,
                time_to_mfe_seconds=EXCLUDED.time_to_mfe_seconds,
                time_to_closest_target_seconds=EXCLUDED.time_to_closest_target_seconds,
                time_to_target_seconds=EXCLUDED.time_to_target_seconds,
                closest_target_price=EXCLUDED.closest_target_price,
                closest_target_distance_pct=EXCLUDED.closest_target_distance_pct,
                target_progress_ratio=EXCLUDED.target_progress_ratio,
                target_reached=EXCLUDED.target_reached,
                path_resolution_seconds=EXCLUDED.path_resolution_seconds,
                path_samples=EXCLUDED.path_samples,
                outcome_method_version=EXCLUDED.outcome_method_version,
                price_source=EXCLUDED.price_source,
                data_quality_status=EXCLUDED.data_quality_status,
                created_at=NOW()
            WHERE research_alert_outcomes.outcome_method_version
                  IS DISTINCT FROM EXCLUDED.outcome_method_version
               OR research_alert_outcomes.data_quality_status
                  IS DISTINCT FROM EXCLUDED.data_quality_status
               OR research_alert_outcomes.path_samples < EXCLUDED.path_samples
            RETURNING event_id
            """,
            (
                event["event_id"],
                horizon,
                path_metrics["measured_at_utc"],
                reference_price,
                path_metrics["price_at_horizon"],
                path_metrics["raw_return_pct"],
                path_metrics["directional_return_pct"],
                path_metrics["max_favorable_price"],
                path_metrics["max_adverse_price"],
                path_metrics["mfe_pct"],
                path_metrics["mae_pct"],
                path_metrics["time_to_first_progress_seconds"],
                path_metrics["time_to_mfe_seconds"],
                path_metrics["time_to_closest_target_seconds"],
                path_metrics["time_to_target_seconds"],
                path_metrics["closest_target_price"],
                path_metrics["closest_target_distance_pct"],
                path_metrics["target_progress_ratio"],
                path_metrics["target_reached"],
                canonical_price_path.INTERVAL_SECONDS,
                len(path_result["candles"]),
                outcome_method_version,
                source,
                quality,
            ),
        ).fetchone()
        return bool(row)

    @staticmethod
    def _write_first_touch_outcome(
        conn,
        *,
        event: Dict[str, Any],
        horizon: int,
        reference_price: float,
        reference_source: str,
        path_result: Dict[str, Any],
        first_touch: Dict[str, Any],
        complete: bool,
    ) -> bool:
        quality = canonical_price_path.quality_status(path_result, complete=complete)
        source = _path_source(reference_source, path_result)
        row = conn.execute(
            """
            INSERT INTO research_first_touch_outcomes (
                event_id, horizon_minutes, method_version, direction,
                status, success, failure_final, observed_through_utc,
                reference_price, qualifying_move_price,
                qualifying_move_threshold_pct, threshold_scale_factor,
                threshold_source_kind, threshold_source, threshold_policy,
                first_qualifying_move_time_utc,
                time_to_first_qualifying_move_seconds,
                pre_qualifying_mae_pct,
                qualifying_candle_adverse_excursion_pct,
                qualifying_candle_order_ambiguous, dwell_required_seconds,
                path_resolution_seconds, path_samples, price_source,
                data_quality_status
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s::jsonb,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (event_id, horizon_minutes, method_version) DO UPDATE SET
                direction=EXCLUDED.direction,
                status=EXCLUDED.status,
                success=EXCLUDED.success,
                failure_final=EXCLUDED.failure_final,
                observed_through_utc=EXCLUDED.observed_through_utc,
                reference_price=EXCLUDED.reference_price,
                qualifying_move_price=EXCLUDED.qualifying_move_price,
                qualifying_move_threshold_pct=
                    EXCLUDED.qualifying_move_threshold_pct,
                threshold_scale_factor=EXCLUDED.threshold_scale_factor,
                threshold_source_kind=EXCLUDED.threshold_source_kind,
                threshold_source=EXCLUDED.threshold_source,
                threshold_policy=EXCLUDED.threshold_policy,
                first_qualifying_move_time_utc=
                    EXCLUDED.first_qualifying_move_time_utc,
                time_to_first_qualifying_move_seconds=
                    EXCLUDED.time_to_first_qualifying_move_seconds,
                pre_qualifying_mae_pct=EXCLUDED.pre_qualifying_mae_pct,
                qualifying_candle_adverse_excursion_pct=
                    EXCLUDED.qualifying_candle_adverse_excursion_pct,
                qualifying_candle_order_ambiguous=
                    EXCLUDED.qualifying_candle_order_ambiguous,
                dwell_required_seconds=0,
                path_resolution_seconds=EXCLUDED.path_resolution_seconds,
                path_samples=EXCLUDED.path_samples,
                price_source=EXCLUDED.price_source,
                data_quality_status=EXCLUDED.data_quality_status,
                updated_at_utc=NOW()
            WHERE NOT (
                research_first_touch_outcomes.status IN ('HIT', 'MISS')
                AND research_first_touch_outcomes.data_quality_status=ANY(%s)
            )
              AND EXCLUDED.observed_through_utc >=
                  research_first_touch_outcomes.observed_through_utc
              AND (
                  research_first_touch_outcomes.status<>'PENDING'
                  OR NOT (
                      research_first_touch_outcomes.data_quality_status=ANY(%s)
                  )
                  OR EXCLUDED.data_quality_status=ANY(%s)
              )
            RETURNING event_id
            """,
            (
                event["event_id"],
                horizon,
                _FIRST_TOUCH_METHOD_VERSION,
                first_touch["direction"],
                first_touch["status"],
                first_touch["success"],
                first_touch["failure_final"],
                first_touch["observed_through_utc"],
                reference_price,
                first_touch["qualifying_move_price"],
                first_touch["qualifying_move_threshold_pct"],
                first_touch["threshold_scale_factor"],
                first_touch["threshold_source_kind"],
                first_touch["threshold_source"],
                json.dumps(
                    first_touch["threshold_policy"],
                    ensure_ascii=False,
                    default=str,
                    separators=(",", ":"),
                ),
                first_touch["first_qualifying_move_time_utc"],
                first_touch["time_to_first_qualifying_move_seconds"],
                first_touch["pre_qualifying_mae_pct"],
                first_touch["qualifying_candle_adverse_excursion_pct"],
                first_touch["qualifying_candle_order_ambiguous"],
                first_touch["dwell_required_seconds"],
                canonical_price_path.INTERVAL_SECONDS,
                len(path_result["candles"]),
                source,
                quality,
                list(canonical_price_path.COMPLETE_QUALITIES),
                list(canonical_price_path.COMPLETE_QUALITIES),
                list(canonical_price_path.COMPLETE_QUALITIES),
            ),
        ).fetchone()
        return bool(row)

    def _run_stage4_no_signal_once(
        self, *, limit: int, now: datetime
    ) -> Dict[str, Any]:
        """Close missing no-signal labels without sharing legacy authority."""

        database_url = _stage4_no_signal_database_url()
        if not database_url:
            return {
                "configured": False,
                "checked": 0,
                "inserted": 0,
                "missing_price_paths": 0,
            }
        if psycopg is None:
            raise RuntimeError("psycopg is unavailable for no-signal outcomes")
        _assert_stage4_no_signal_database_target(database_url)
        with psycopg.connect(
            database_url,
            row_factory=dict_row,
            connect_timeout=5,
            options=_stage4_no_signal_connection_options(),
        ) as conn:
            cells = self._load_due_stage4_no_signal_cells(
                conn, limit=limit, now=now
            )

        groups: Dict[tuple[int, str], list[Dict[str, Any]]] = {}
        for raw in cells:
            cell = dict(raw)
            groups.setdefault(
                (int(cell["projection_event_id"]), str(cell["symbol"]).upper()),
                [],
            ).append(cell)

        prepared: list[Dict[str, Any]] = []
        failures = 0
        for (projection_event_id, symbol), group in sorted(groups.items()):
            try:
                references = [
                    _stage4_no_signal_frozen_price_reference(cell)
                    for cell in group
                ]
                first_price, first_source, first_receipt = references[0]
                if any(
                    price != first_price
                    or source != first_source
                    or receipt != first_receipt
                    for price, source, receipt in references[1:]
                ):
                    raise ValueError("no-signal direction cells disagree on reference")
                event_time = _utc(group[0]["decision_time_utc"])
                horizons = sorted(
                    {
                        int(horizon)
                        for cell in group
                        for horizon in cell.get("due_horizons") or ()
                    }
                )
                if not horizons:
                    continue
                maximum_cutoff = event_time + timedelta(minutes=max(horizons))
                if maximum_cutoff > now:
                    raise ValueError("no-signal horizon is not fully closed")
                path_result = canonical_price_path.fetch_closed_candles(
                    symbol, event_time, maximum_cutoff
                )
                route = canonical_price_path.validated_route(
                    symbol, path_result, require_complete=True
                )
                if route.get("provider_provenance") != (
                    "EXCHANGE_API_HISTORICAL_CANDLES_IMPORTED"
                ):
                    raise ValueError("no-signal path provenance is not canonical")
                provenance_error = _canonical_path_provenance_error(
                    symbol, path_result
                )
                if provenance_error is not None:
                    raise ValueError(provenance_error)
                full_path = list(path_result.get("candles") or ())
                full_expected = _expected_candles(event_time, maximum_cutoff)
                if full_expected <= 0 or len(full_path) != full_expected:
                    raise ValueError("incomplete closed-candle path")
                for cell in group:
                    for horizon in cell.get("due_horizons") or ():
                        horizon_cutoff = event_time + timedelta(minutes=int(horizon))
                        candles = _candles_for_horizon(full_path, horizon_cutoff)
                        expected = _expected_candles(event_time, horizon_cutoff)
                        if expected <= 0 or len(candles) != expected:
                            failures += 1
                            continue
                        metrics = binance_spot_price_path.calculate_path_metrics(
                            reference_price=first_price,
                            direction=str(cell["direction"]),
                            event_time=event_time,
                            candles=candles,
                            target_price=None,
                        )
                        bounded_path = dict(path_result)
                        bounded_path["candles"] = candles
                        prepared.append(
                            {
                                "cell": cell,
                                "horizon": int(horizon),
                                "reference_price": first_price,
                                "reference_source": first_source,
                                "reference_receipt": first_receipt,
                                "path_result": bounded_path,
                                "path_metrics": metrics,
                            }
                        )
            except Exception as exc:
                failures += len(group)
                print(
                    "[research-outcomes] Stage-4 no-signal path unavailable "
                    f"projection={projection_event_id} symbol={symbol}: {exc!r}",
                    flush=True,
                )

        inserted = 0
        if prepared:
            with psycopg.connect(
                database_url,
                row_factory=dict_row,
                connect_timeout=5,
                options=_stage4_no_signal_connection_options(),
            ) as conn:
                for outcome in prepared:
                    if self._write_stage4_no_signal_outcome(conn, **outcome):
                        inserted += 1
        return {
            "configured": True,
            "checked": len(cells),
            "inserted": inserted,
            "missing_price_paths": failures,
        }

    def run_once(self, *, limit_per_horizon: int = 200) -> Dict[str, Any]:
        url = _database_url()
        if not _ENABLED:
            return {"enabled": False, "inserted": 0, "upgraded": 0}
        if not url or psycopg is None:
            raise RuntimeError("Research outcome worker database is not configured")

        inserted = 0
        upgraded = 0
        checked = 0
        path_failures = 0
        partial_paths = 0
        first_touch_written = 0
        first_touch_hits = 0
        first_touch_pending = 0
        first_touch_policy_conflicts = 0
        alert_reference_provenance_rejections = 0
        rejected_alerts: list[Dict[str, Any]] = []
        first_touch_terminal_deferred = 0
        now = datetime.now(timezone.utc)
        latest_closed_cutoff = _latest_closed_candle_cutoff(now)
        prepared: list[Dict[str, Any]] = []
        unavailable_symbols: Dict[str, str] = {}
        unavailable_event_counts: Dict[str, int] = {}
        self.metrics.last_error = None
        self.metrics.last_error_phase = None

        # Keep this bounded carrier ahead of the legacy queue: a timeout in a
        # legacy load must not starve explicit no-signal outcome progress.
        no_signal_result = {
            "configured": bool(_stage4_no_signal_database_url()),
            "checked": 0,
            "inserted": 0,
            "missing_price_paths": 0,
        }
        try:
            no_signal_result = self._run_stage4_no_signal_once(
                limit=limit_per_horizon,
                now=now,
            )
            self.metrics.stage4_no_signal_last_error = None
        except Exception as exc:
            self.metrics.stage4_no_signal_failures += 1
            self.metrics.stage4_no_signal_last_error = (
                f"{type(exc).__name__}: {exc}"
            )
            print(
                "[research-outcomes] isolated Stage-4 no-signal carrier "
                f"failure: {exc!r}",
                flush=True,
            )
        self.metrics.stage4_no_signal_cells_checked += int(
            no_signal_result["checked"]
        )
        self.metrics.stage4_no_signal_outcomes_inserted += int(
            no_signal_result["inserted"]
        )
        self.metrics.stage4_no_signal_missing_price_paths += int(
            no_signal_result["missing_price_paths"]
        )

        with self._outcome_read_transaction(url) as conn:
            with self._phase("LOAD_OPEN_FIRST_TOUCH"):
                open_events = self._load_open_first_touch_events_isolated(
                    conn, limit_per_horizon
                )
            with self._phase("LOAD_DUE_EVENTS"):
                closed_events = self._load_due_events(
                    conn, limit_per_horizon
                )
            # The reserved open queue is intentionally first.  Merge a rare
            # boundary duplicate so one event still causes one canonical path
            # fetch even if another horizon closed in the same minute.
            events_by_id: Dict[int, Dict[str, Any]] = {}
            event_order: list[int] = []
            for source_event in [*open_events, *closed_events]:
                event_id = int(source_event["event_id"])
                candidate = dict(source_event)
                if event_id not in events_by_id:
                    events_by_id[event_id] = candidate
                    event_order.append(event_id)
                    continue
                existing = events_by_id[event_id]
                existing["outcome_versions"] = {
                    **_versions(existing.get("outcome_versions")),
                    **_versions(candidate.get("outcome_versions")),
                }
                existing["first_touch_versions"] = {
                    **_versions(existing.get("first_touch_versions")),
                    **_versions(candidate.get("first_touch_versions")),
                }
                existing["open_first_touch_horizons"] = sorted(
                    {
                        *(
                            existing.get("open_first_touch_horizons")
                            or ()
                        ),
                        *(
                            candidate.get("open_first_touch_horizons")
                            or ()
                        ),
                    }
                )
            events = [events_by_id[event_id] for event_id in event_order]
            legacy_event_ids = [
                event_id
                for event_id in event_order
                if not _is_stage4_signal_outcome_event(
                    events_by_id[event_id]
                )
                and _prospective_sampler_version(events_by_id[event_id])
                != _STRICT_PROSPECTIVE_SAMPLER_VERSION
            ]
            with self._phase("LOAD_FROZEN_THRESHOLDS"):
                frozen_references = self._load_frozen_threshold_references(
                    conn, legacy_event_ids
                )
            references_by_event: Dict[int, list[Dict[str, Any]]] = {}
            for reference in frozen_references:
                references_by_event.setdefault(
                    int(reference["event_id"]), []
                ).append(reference)
        with self._phase("LOAD_CURRENT_SLOT_THRESHOLDS"):
            slot_references_by_event = (
                self._load_current_slot_threshold_references(events, now=now)
            )
        for event in events:
            event_id = int(event["event_id"])
            if (
                _prospective_sampler_version(event)
                == _STRICT_PROSPECTIVE_SAMPLER_VERSION
            ):
                event["frozen_threshold_references"] = (
                    slot_references_by_event.get(event_id, [])
                )
            else:
                event["frozen_threshold_references"] = (
                    references_by_event.get(event_id, [])
                )
        # Do not hold a PostgreSQL connection or transaction while waiting for
        # Binance. This keeps outcome research isolated from the live bot load.
        for event in events:
            checked += 1
            symbol = str(event["symbol"]).strip().upper()
            event_time = _utc(event["alert_time_utc"])
            versions = _versions(event.get("outcome_versions"))
            first_touch_versions = _versions(event.get("first_touch_versions"))
            outcome_method_version = _outcome_method_version_for_event(event)
            horizons = _due_horizons(
                event_time,
                versions,
                first_touch_versions,
                now=now,
                outcome_method_version=outcome_method_version,
                first_touch_enabled=_first_touch_enabled_for_event(event),
                open_first_touch_horizons=(
                    event.get("open_first_touch_horizons") or ()
                ),
            )
            if not horizons:
                continue

            provenance_error = _alert_reference_provenance_error(event)
            if provenance_error is not None:
                alert_reference_provenance_rejections += 1
                rejected_alerts.append(
                    {"event": event, "reason": provenance_error}
                )
                continue

            # A symbol whose canonical provider rejected it earlier in this run will be
            # retried on the next scheduled run, but never once per event in
            # the same batch.  Missing metrics still count every affected
            # event so health reporting remains honest.
            if symbol in unavailable_symbols:
                path_failures += 1
                unavailable_event_counts[symbol] += 1
                continue

            # Threshold authority is decision-time evidence, so validate it
            # before downloading any future price path.  A permanently
            # malformed/missing authority must not trigger an expensive API
            # fetch when first-touch is the event's only remaining work.
            legacy_horizons: set[int] = set()
            threshold_policies: Dict[int, Dict[str, Any]] = {}
            first_touch_enabled = _first_touch_enabled_for_event(event)
            for horizon in horizons:
                horizon_closed = now >= (
                    event_time + timedelta(minutes=horizon)
                )
                if (
                    horizon_closed
                    and versions.get(horizon) != outcome_method_version
                ):
                    legacy_horizons.add(horizon)
                if (
                    not first_touch_enabled
                    or first_touch_versions.get(horizon)
                    == _FIRST_TOUCH_METHOD_VERSION
                ):
                    continue
                try:
                    threshold_policies[horizon] = _frozen_threshold_policy(
                        event=event,
                        horizon_minutes=horizon,
                        snapshot_records=(
                            event.get("frozen_threshold_references") or ()
                        ),
                    )
                except FrozenThresholdPolicyConflict as exc:
                    first_touch_policy_conflicts += 1
                    print(
                        "[research-outcomes] frozen first-touch threshold "
                        f"conflict event={event['event_id']} horizon={horizon}: "
                        f"{exc}",
                        flush=True,
                    )

            path_horizons = [
                horizon
                for horizon in horizons
                if horizon in legacy_horizons
                or horizon in threshold_policies
            ]
            if not path_horizons:
                continue

            try:
                if _is_stage4_signal_outcome_event(event):
                    reference_price, reference_source = (
                        _stage4_frozen_price_reference(event)
                    )
                else:
                    reference_value = event.get("current_price")
                    reference_price = float(reference_value)
                    if reference_price <= 0:
                        raise ValueError
                    reference_source = _snapshot_price_source(
                        event.get("engine_snapshot")
                    )
            except (TypeError, ValueError, OverflowError) as exc:
                # A later path cannot repair missing, stale or mismatched
                # immutable decision-time evidence.
                path_failures += 1
                print(
                    "[research-outcomes] immutable decision price unavailable "
                    f"event={event['event_id']} symbol={symbol}; skipped: {exc}",
                    flush=True,
                )
                continue

            max_horizon = max(path_horizons)
            horizon_time = min(
                event_time + timedelta(minutes=max_horizon),
                latest_closed_cutoff,
            )
            try:
                path_result = canonical_price_path.fetch_closed_candles(
                    symbol, event_time, horizon_time
                )
            except Exception as exc:
                path_failures += 1
                unavailable_symbols[symbol] = repr(exc)
                unavailable_event_counts[symbol] = 1
                print(
                    f"[research-outcomes] canonical {canonical_price_path.provider_for_symbol(symbol)} "
                    f"spot path unavailable event={event['event_id']} "
                    f"symbol={symbol}: {exc!r}",
                    flush=True,
                )
                continue

            path_provenance_error = _canonical_path_provenance_error(
                symbol, path_result
            )
            if path_provenance_error is not None:
                path_failures += 1
                unavailable_symbols[symbol] = path_provenance_error
                unavailable_event_counts[symbol] = 1
                print(
                    "[research-outcomes] non-canonical fetched path "
                    f"event={event['event_id']} symbol={symbol}; skipped: "
                    f"{path_provenance_error}",
                    flush=True,
                )
                continue

            full_path = list(path_result.get("candles") or [])
            if not full_path:
                path_failures += 1
                unavailable_symbols[symbol] = "empty closed-candle path"
                unavailable_event_counts[symbol] = 1
                continue

            for horizon in path_horizons:
                horizon_cutoff = event_time + timedelta(minutes=horizon)
                observed_cutoff = min(horizon_cutoff, latest_closed_cutoff)
                candles = _candles_for_horizon(full_path, observed_cutoff)
                if not candles:
                    path_failures += 1
                    continue
                expected_observed = _expected_candles(event_time, observed_cutoff)
                observed_complete = len(candles) == expected_observed
                horizon_closed = now >= horizon_cutoff
                full_complete = (
                    horizon_closed
                    and len(candles) == _expected_candles(event_time, horizon_cutoff)
                )
                partial_paths += int(not observed_complete)
                legacy_needed = horizon in legacy_horizons
                metrics = (
                    binance_spot_price_path.calculate_path_metrics(
                        reference_price=reference_price,
                        direction=str(event.get("direction") or "NEUTRAL"),
                        event_time=event_time,
                        candles=candles,
                        target_price=event.get("target_price"),
                    )
                    if legacy_needed
                    else None
                )
                first_touch_needed = horizon in threshold_policies
                first_touch = None
                first_touch_write_safe = False
                if first_touch_needed:
                    first_touch = (
                        research_no_dwell_outcome.calculate_first_touch_outcome(
                            reference_price=reference_price,
                            direction=str(
                                event.get("direction") or "NEUTRAL"
                            ),
                            event_time=event_time,
                            candles=candles,
                            horizon_minutes=horizon,
                            horizon_closed=full_complete,
                            threshold_policy=threshold_policies[horizon],
                        )
                    )
                    # ``first_qualifying_move_time_utc`` preserves the exact
                    # earlier touch. ``observed_through_utc`` tracks the
                    # complete prefix used for this recalculation so a
                    # corrected PENDING -> HIT write remains monotonic.
                    first_touch["observed_through_utc"] = _utc(
                        candles[-1].close_time_utc
                    )
                    first_touch_write_safe = _first_touch_write_is_safe(
                        first_touch,
                        observed_prefix_complete=observed_complete,
                    )
                if (
                    first_touch is not None
                    and first_touch.get("status") in {"HIT", "MISS"}
                    and not first_touch_write_safe
                ):
                    first_touch_terminal_deferred += 1
                outcome_path = dict(path_result)
                outcome_path["candles"] = candles
                prepared.append(
                    {
                        "event": event,
                        "horizon": horizon,
                        "reference_price": reference_price,
                        "reference_source": reference_source,
                        "path_result": outcome_path,
                        "path_metrics": metrics,
                        "first_touch": first_touch,
                        "complete": observed_complete,
                        "full_complete": full_complete,
                        "legacy_needed": legacy_needed,
                        "first_touch_needed": first_touch_needed,
                        "first_touch_write_safe": first_touch_write_safe,
                        "upgrade": horizon in versions,
                    }
                )

        if rejected_alerts:
            with self._phase("WRITE_REJECTIONS"):
                with psycopg.connect(
                    url,
                    row_factory=dict_row,
                    connect_timeout=5,
                    options=_database_connection_options(),
                ) as conn:
                    newly_quarantined = self._write_alert_reference_rejections(
                        conn, rejected_alerts
                    )
            by_route: Dict[str, int] = {}
            for rejected in rejected_alerts:
                route = (
                    "HYPE"
                    if str(rejected["event"].get("symbol") or "").upper()
                    == "HYPE"
                    else "BINANCE"
                )
                by_route[route] = by_route.get(route, 0) + 1
            summary = ", ".join(
                f"{route}={by_route[route]}" for route in sorted(by_route)
            )
            print(
                "[research-outcomes] quarantined non-canonical Alert references "
                f"policy={_ALERT_REFERENCE_REJECTION_POLICY_VERSION} "
                f"new={newly_quarantined} checked={len(rejected_alerts)} "
                f"routes={summary}",
                flush=True,
            )

        if unavailable_symbols:
            summary = ", ".join(
                f"{symbol} events={unavailable_event_counts[symbol]}"
                for symbol in sorted(unavailable_symbols)
            )
            print(
                f"[research-outcomes] unavailable canonical spot symbols this run: {summary}",
                flush=True,
            )

        if prepared:
            with self._phase("WRITE_OUTCOMES"):
                with psycopg.connect(
                    url,
                    row_factory=dict_row,
                    connect_timeout=5,
                    options=_database_connection_options(),
                ) as conn:
                    for outcome in prepared:
                        if (
                            outcome["first_touch_needed"]
                            and outcome["first_touch_write_safe"]
                        ):
                            touch_written = self._write_first_touch_outcome(
                                conn,
                                event=outcome["event"],
                                horizon=outcome["horizon"],
                                reference_price=outcome["reference_price"],
                                reference_source=outcome["reference_source"],
                                path_result=outcome["path_result"],
                                first_touch=outcome["first_touch"],
                                complete=outcome["complete"],
                            )
                            if touch_written:
                                first_touch_written += 1
                                first_touch_hits += int(
                                    outcome["first_touch"]["status"] == "HIT"
                                )
                                first_touch_pending += int(
                                    outcome["first_touch"]["status"] == "PENDING"
                                )
                        if outcome["legacy_needed"]:
                            written = self._write_outcome(
                                conn,
                                event=outcome["event"],
                                horizon=outcome["horizon"],
                                reference_price=outcome["reference_price"],
                                reference_source=outcome["reference_source"],
                                path_result=outcome["path_result"],
                                path_metrics=outcome["path_metrics"],
                                complete=outcome["full_complete"],
                            )
                            if not written:
                                continue
                            if outcome["upgrade"]:
                                upgraded += 1
                            else:
                                inserted += 1

        self.metrics.runs += 1
        self.metrics.events_checked += checked
        self.metrics.outcomes_inserted += inserted
        self.metrics.outcomes_upgraded += upgraded
        self.metrics.missing_price_paths += path_failures
        self.metrics.partial_price_paths += partial_paths
        self.metrics.first_touch_rows_written += first_touch_written
        self.metrics.first_touch_hits += first_touch_hits
        self.metrics.first_touch_pending += first_touch_pending
        self.metrics.first_touch_threshold_policy_conflicts += (
            first_touch_policy_conflicts
        )
        self.metrics.alert_reference_provenance_rejections += (
            alert_reference_provenance_rejections
        )
        self.metrics.first_touch_terminal_rows_deferred_for_incomplete_prefix += (
            first_touch_terminal_deferred
        )
        self.metrics.last_run_utc = datetime.now(timezone.utc).isoformat()
        self.metrics.last_error = None
        self.metrics.last_error_phase = None
        return {
            "enabled": True,
            "checked": checked,
            "inserted": inserted,
            "upgraded": upgraded,
            "missing_price_paths": path_failures,
            "partial_price_paths": partial_paths,
            "first_touch_rows_written": first_touch_written,
            "first_touch_hits": first_touch_hits,
            "first_touch_pending": first_touch_pending,
            "first_touch_threshold_policy_conflicts": (
                first_touch_policy_conflicts
            ),
            "alert_reference_provenance_rejections": (
                alert_reference_provenance_rejections
            ),
            "first_touch_terminal_rows_deferred_for_incomplete_prefix": (
                first_touch_terminal_deferred
            ),
            "unavailable_symbols": {
                symbol: unavailable_event_counts[symbol]
                for symbol in sorted(unavailable_symbols)
            },
            "stage4_signal_due_scan": dict(self._last_stage4_due_scan),
            "stage4_no_signal": no_signal_result,
        }


WORKER = ResearchOutcomeWorker()
