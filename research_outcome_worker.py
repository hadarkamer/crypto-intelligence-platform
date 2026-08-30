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
reconstruction is allowed. Existing legacy rows and method versions remain
available for audit only.

Binance Spot USDT is the default route. HYPE is explicitly routed to the
Hyperliquid HYPE/USDT spot market. Historical candles may be imported from
those exchange APIs as long as their provenance and quality remain attached.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import math
import os
import re
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None

import binance_spot_price_path
import canonical_price_path
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
_METHOD_VERSION = canonical_price_path.METHOD_VERSION
_FIRST_TOUCH_METHOD_VERSION = research_no_dwell_outcome.METHOD_VERSION
_STRICT_FROZEN_EVIDENCE_POLICY_VERSION = (
    "prospective-shadow-frozen-decision-features-v1"
)
_STRICT_FROZEN_SNAPSHOT_POLICY_VERSION = (
    "formula-shadow-input-snapshot-v5-frozen-decision-features"
)
_STRICT_FEATURE_BUNDLE_POLICY_VERSION = (
    "prospective-decision-feature-bundle-v1"
)
_STRICT_PROSPECTIVE_SAMPLER_VERSION = (
    "prospective-neutral-anchor-v4-decision-features-frozen"
)
_ALERT_REFERENCE_REJECTION_POLICY_VERSION = "alert-reference-provenance-v1"
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


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


def _is_current_frozen_evidence_record(record: Mapping[str, Any]) -> bool:
    """Recognize every marker that makes a row subject to the v4 contract.

    A partially forged row must not evade strict validation by omitting one of
    the duplicate policy fields. If *any* current marker is present, the full
    exact contract is required below.
    """
    snapshot = _mapping(record.get("input_snapshot"))
    evidence = _mapping(snapshot.get("prospective_evidence"))
    return (
        str(record.get("evidence_policy_version") or "").strip()
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
            "current Shadow check evidence policy is missing or incompatible"
        )
    if str(snapshot.get("evidence_policy_version") or "").strip() != (
        _STRICT_FROZEN_EVIDENCE_POLICY_VERSION
    ):
        raise FrozenThresholdPolicyConflict(
            "current frozen snapshot evidence policy is missing or incompatible"
        )
    if str(snapshot.get("snapshot_policy_version") or "").strip() != (
        _STRICT_FROZEN_SNAPSHOT_POLICY_VERSION
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
            "current Shadow evidence was not authoritatively verified"
        )
    if str(evidence.get("sampler_version") or "").strip() != (
        _STRICT_PROSPECTIVE_SAMPLER_VERSION
    ):
        raise FrozenThresholdPolicyConflict(
            "current Shadow evidence is not bound to the exact sampler v4"
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
            "outcome event input fingerprint differs from frozen Shadow evidence"
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
            "outcome event feature-bundle hash differs from frozen Shadow evidence"
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
    """Build one deterministic threshold from immutable Shadow snapshots.

    Multiple formulas can evaluate the same event and horizon, while the
    canonical first-touch table has one row for that event/horizon.  Any
    relaxed references must therefore agree exactly.  Ambiguity is rejected
    instead of silently falling back to a static threshold and mislabelling a
    weekend outcome.
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
                "current formula snapshots disagree on the frozen "
                "event/horizon feature bundle"
            )
        return normalized_current[0][1]

    # Earlier formula/sampler snapshots remain readable for historical audit,
    # but they are never treated as current frozen Shadow evidence. Their
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

    def field(name: str) -> str:
        return str(
            snapshot.get(f"price_{name}")
            or snapshot.get(f"top_item_price_{name}")
            or market_evidence.get(f"price_{name}")
            or ""
        ).strip()

    return {
        "source": field("source"),
        "exchange": field("exchange"),
        "market": field("market"),
        "pair": field("pair"),
        "instrument": field("instrument"),
    }


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
            and existing_versions.get(horizon) != _METHOD_VERSION
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
    failures: int = 0
    last_run_utc: Optional[str] = None
    last_error: Optional[str] = None


class ResearchOutcomeWorker:
    def __init__(self) -> None:
        self.metrics = OutcomeMetrics()
        self._task: Optional[asyncio.Task] = None
        self._stopping = False

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
            "method": _METHOD_VERSION,
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
        """Load immutable width evidence for matches and prospective controls."""
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
    def _load_due_events(conn, limit: int) -> list[Dict[str, Any]]:
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
                              AND current_o.outcome_method_version=%s
                              AND current_o.data_quality_status=ANY(%s)
                        )
                        OR (
                            e.direction IN ('LONG', 'SHORT')
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
                    _METHOD_VERSION,
                    list(canonical_price_path.COMPLETE_QUALITIES),
                    horizon,
                    _FIRST_TOUCH_METHOD_VERSION,
                    list(canonical_price_path.COMPLETE_QUALITIES),
                )
            )
        query = f"""
            SELECT e.event_id, e.alert_time_utc, e.symbol, e.direction,
                   e.event_type, e.setup_key,
                   e.event_kind, e.delivery_status,
                   e.current_price, e.target_price, e.engine_snapshot,
                   COALESCE(
                       jsonb_object_agg(
                           o.horizon_minutes,
                           CASE
                               WHEN o.outcome_method_version=%s
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
                   NULL::timestamptz AS open_first_touch_observed_utc
            FROM research_events e
            LEFT JOIN research_alert_outcomes o ON o.event_id=e.event_id
            WHERE (
                (e.event_kind='ALERT' AND e.delivery_status='DELIVERED')
                OR (
                    e.event_kind='DECISION_SAMPLE'
                    AND e.delivery_status='NOT_APPLICABLE'
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
            ORDER BY
                {_alert_reference_queue_priority_sql("e")} ASC,
                e.alert_time_utc ASC,
                e.event_id ASC
            LIMIT %s
        """
        params: list[Any] = [
            _METHOD_VERSION,
            list(canonical_price_path.COMPLETE_QUALITIES),
            list(canonical_price_path.COMPLETE_QUALITIES),
            _FIRST_TOUCH_METHOD_VERSION,
            _ALERT_REFERENCE_REJECTION_POLICY_VERSION,
            *condition_params,
            max(1, min(int(limit), 1000)),
        ]
        return conn.execute(query, params).fetchall()

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
                _METHOD_VERSION,
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

        with psycopg.connect(
            url,
            row_factory=dict_row,
            connect_timeout=5,
            options="-c statement_timeout=15000 -c lock_timeout=1000",
        ) as conn:
            open_events = self._load_open_first_touch_events(
                conn, limit_per_horizon
            )
            closed_events = self._load_due_events(conn, limit_per_horizon)
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
            frozen_references = self._load_frozen_threshold_references(
                conn, event_order
            )
            references_by_event: Dict[int, list[Dict[str, Any]]] = {}
            for reference in frozen_references:
                references_by_event.setdefault(
                    int(reference["event_id"]), []
                ).append(reference)
            for event in events:
                event["frozen_threshold_references"] = references_by_event.get(
                    int(event["event_id"]), []
                )
        # Do not hold a PostgreSQL connection or transaction while waiting for
        # Binance. This keeps outcome research isolated from the live bot load.
        for event in events:
            checked += 1
            symbol = str(event["symbol"]).strip().upper()
            event_time = _utc(event["alert_time_utc"])
            versions = _versions(event.get("outcome_versions"))
            first_touch_versions = _versions(event.get("first_touch_versions"))
            horizons = _due_horizons(
                event_time,
                versions,
                first_touch_versions,
                now=now,
                first_touch_enabled=str(event.get("direction") or "").upper()
                in {"LONG", "SHORT"},
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

            max_horizon = max(horizons)
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

            reference_value = event.get("current_price")
            reference_source = _snapshot_price_source(event.get("engine_snapshot"))
            try:
                reference_price = float(reference_value)
                if reference_price <= 0:
                    raise ValueError
            except (TypeError, ValueError):
                # The first full candle opens after the decision and therefore
                # cannot replace a missing immutable decision-time price.
                # Skipping is the only look-ahead-safe behavior; a later retry
                # may succeed only if the archived event itself is complete.
                path_failures += 1
                print(
                    "[research-outcomes] immutable decision price unavailable "
                    f"event={event['event_id']} symbol={symbol}; skipped",
                    flush=True,
                )
                continue

            for horizon in horizons:
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
                legacy_needed = (
                    horizon_closed and versions.get(horizon) != _METHOD_VERSION
                )
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
                first_touch_needed = (
                    str(event.get("direction") or "").upper() in {"LONG", "SHORT"}
                    and first_touch_versions.get(horizon)
                    != _FIRST_TOUCH_METHOD_VERSION
                )
                first_touch = None
                first_touch_write_safe = False
                if first_touch_needed:
                    try:
                        threshold_policy = _frozen_threshold_policy(
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
                    else:
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
                                threshold_policy=threshold_policy,
                            )
                        )
                        # ``first_qualifying_move_time_utc`` preserves the
                        # exact earlier touch.  ``observed_through_utc`` tracks
                        # the complete prefix used for this recalculation so a
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
            with psycopg.connect(
                url,
                row_factory=dict_row,
                connect_timeout=5,
                options="-c statement_timeout=15000 -c lock_timeout=1000",
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
            with psycopg.connect(
                url,
                row_factory=dict_row,
                connect_timeout=5,
                options="-c statement_timeout=15000 -c lock_timeout=1000",
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
        }


WORKER = ResearchOutcomeWorker()
