"""Fail-open background discovery and live Shadow evaluation workers."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import heapq
import json
import os
import threading
import time
from typing import Any, Dict, Mapping, Optional

import research_feature_matrix
import research_evidence_contract
import research_formula_acceptance
import research_formula_engine
import research_formula_relevance
import research_formula_store
import research_market_episode
import research_max_pain_archive
import research_mfe_mae_efficiency
import research_signal_formula_exploration
import research_signal_formula_exploration_reader
import research_stage4_candidate_search


_TRUE = {"1", "true", "yes", "on"}
_DISCOVERY_ENABLED = os.getenv("FORMULA_DISCOVERY_ENABLED", "").strip().lower() in _TRUE
_SHADOW_ENABLED = os.getenv("FORMULA_SHADOW_ENABLED", "").strip().lower() in _TRUE
_LIVE_ALERTS_ENABLED = os.getenv("FORMULA_LIVE_ALERTS_ENABLED", "").strip().lower() in _TRUE
_DISCOVERY_STARTUP_DELAY_SECONDS = max(
    15, int(os.getenv("FORMULA_DISCOVERY_STARTUP_DELAY_SECONDS", "30"))
)
_DISCOVERY_SLOT_GRACE_SECONDS = max(
    60, int(os.getenv("FORMULA_DISCOVERY_SLOT_GRACE_SECONDS", "300"))
)
_DISCOVERY_IDLE_POLL_SECONDS = max(
    15, int(os.getenv("FORMULA_DISCOVERY_IDLE_POLL_SECONDS", "60"))
)
_SHADOW_POLL_SECONDS = max(30, int(os.getenv("FORMULA_SHADOW_POLL_SECONDS", "60")))
_LOOKBACK_DAYS = max(1, min(3650, int(os.getenv("FORMULA_DISCOVERY_LOOKBACK_DAYS", "120"))))
_DATASET_LIMIT = max(100, min(5000, int(os.getenv("FORMULA_DISCOVERY_DATASET_LIMIT", "2000"))))
_DATASET_MODE = os.getenv("FORMULA_DISCOVERY_DATASET_MODE", "auto").strip().lower()
if _DATASET_MODE not in {"auto", "alerts", "historical_replay"}:
    _DATASET_MODE = "auto"
_HIERARCHICAL_SEARCH_ENABLED = (
    os.getenv("FORMULA_DISCOVERY_HIERARCHICAL_ENABLED", "").strip().lower()
    in _TRUE
)
_DECISION_COHORT_POLICY_VERSION = (
    research_formula_store._DECISION_COHORT_POLICY_VERSION
)
_STAGE4_CORPUS_DATASET_KIND = "authoritative_stage4_wave_closed_path_v1"
_STAGE4_CORPUS_LOOKBACK_DAYS = min(
    research_signal_formula_exploration_reader.MAX_LOOKBACK_DAYS,
    _LOOKBACK_DAYS,
)
_STAGE4_CORPUS_PROJECTION_LIMIT = max(
    1,
    min(
        research_signal_formula_exploration_reader.MAX_PROJECTION_LIMIT,
        int(os.getenv("FORMULA_STAGE4_CORPUS_PROJECTION_LIMIT", "128")),
    ),
)
_STAGE4_MIN_INDEPENDENT_OCCURRENCES = (
    research_signal_formula_exploration.EXPLORATION_MIN_BTC_PARENT_MOVEMENTS
)
_STAGE4_CANDIDATE_ELIGIBILITY_GATE = {
    "policy_version": "experimental-formula-eligibility-v1",
    "status": "NOT_EVALUATED_IN_THIS_INGESTION_STAGE",
    "minimum_independent_occurrences": _STAGE4_MIN_INDEPENDENT_OCCURRENCES,
    "independence_unit": "DISTINCT_BTC_PARENT_MARKET_MOVEMENT",
    "requirement": (
        f"the same pattern's {_STAGE4_MIN_INDEPENDENT_OCCURRENCES}+ independent "
        "occurrences must already show "
        "favorable probability and/or clear directional asymmetry"
    ),
    "separate_later_probability_gate": False,
}


def _horizons() -> tuple[int, ...]:
    values = []
    for raw in os.getenv("FORMULA_DISCOVERY_HORIZONS", "60,240,720,1440").split(","):
        try:
            value = int(raw.strip())
        except ValueError:
            continue
        if value in {60, 240, 720, 1440} and value not in values:
            values.append(value)
    return tuple(values or (240,))


def _discovery_config() -> research_formula_engine.DiscoveryConfig:
    return research_formula_engine.DiscoveryConfig(
        hierarchical_search_enabled=_HIERARCHICAL_SEARCH_ENABLED
    )


def _discovery_schedule(
    horizon_minutes: int, *, now: datetime
) -> tuple[datetime, datetime]:
    """Return the newest fixed UTC horizon slot whose grace has elapsed."""

    horizon = int(horizon_minutes)
    if horizon not in {60, 240, 720, 1440}:
        raise ValueError("unsupported Formula Discovery horizon")
    current = _as_utc(now)
    shifted = current - timedelta(seconds=_DISCOVERY_SLOT_GRACE_SECONDS)
    cadence_seconds = horizon * 60
    epoch_seconds = int(shifted.timestamp())
    slot_seconds = (epoch_seconds // cadence_seconds) * cadence_seconds
    slot = datetime.fromtimestamp(slot_seconds, tz=timezone.utc)
    return slot, slot + timedelta(seconds=_DISCOVERY_SLOT_GRACE_SECONDS)


def _next_discovery_due_at(*, now: datetime) -> datetime:
    current = _as_utc(now)
    next_due = []
    for horizon in _horizons():
        slot, due_at = _discovery_schedule(horizon, now=current)
        if due_at <= current:
            slot += timedelta(minutes=horizon)
            due_at = slot + timedelta(seconds=_DISCOVERY_SLOT_GRACE_SECONDS)
        next_due.append(due_at)
    return min(next_due)


def _dataset_watermark(
    dataset: Mapping[str, Any], *, horizon_minutes: int
) -> tuple[Dict[str, Any], str]:
    """Hash the exact bounded dataset without exposing it as a predicate."""

    rows = list(dataset.get("rows") or [])
    rows_payload = json.dumps(
        rows,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
        allow_nan=False,
    )
    coverage = dataset.get("coverage")
    coverage_payload = dict(coverage) if isinstance(coverage, Mapping) else {}
    coverage_payload.pop("analysis_as_of_utc", None)
    coverage_sha = hashlib.sha256(
        json.dumps(
            coverage_payload,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    watermark = {
        "watermark_version": research_formula_store.DISCOVERY_WATERMARK_VERSION,
        "horizon_minutes": int(horizon_minutes),
        "dataset_kind": coverage_payload.get("dataset_kind"),
        "feature_schema_version": dataset.get("feature_schema_version"),
        "outcome_method_version": dataset.get("outcome_method_version"),
        "replay_version": dataset.get("replay_version"),
        "sample_size": int(dataset.get("sample_size") or 0),
        "first_alert_time_utc": dataset.get("first_alert_time_utc"),
        "last_alert_time_utc": dataset.get("last_alert_time_utc"),
        "rows_sha256": hashlib.sha256(rows_payload.encode("utf-8")).hexdigest(),
        "coverage_sha256": coverage_sha,
        "predicate_effect": "NONE_OPERATIONAL_ONLY",
    }
    canonical = json.dumps(
        watermark,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
        allow_nan=False,
    )
    return watermark, hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _stage4_corpus_observability_receipt(
    payload: Mapping[str, Any],
    *,
    horizon_minutes: int,
    schedule_slot_utc: datetime,
    due_at_utc: datetime,
    duration_ms: int,
) -> Dict[str, Any]:
    """Reduce an authoritative corpus page to non-predicate telemetry."""

    raw_rows = payload.get("observations")
    if not isinstance(raw_rows, (list, tuple)) or any(
        not isinstance(row, Mapping) for row in raw_rows
    ):
        raise ValueError("authoritative corpus observations are not mappings")
    rows = [dict(row) for row in raw_rows]
    source_counts = payload.get("counts")
    if not isinstance(source_counts, Mapping):
        raise ValueError("authoritative corpus counts are not a mapping")
    source_counts = dict(source_counts)
    source_attestation = payload.get("source_attestation")
    if not isinstance(source_attestation, Mapping):
        raise ValueError("authoritative source attestation is not a mapping")
    source_attestation = dict(source_attestation)
    cursor = payload.get("cursor")
    if not isinstance(cursor, Mapping):
        raise ValueError("authoritative corpus cursor is not a mapping")
    cursor = dict(cursor)
    raw_source_blockers = payload.get("blockers")
    if not isinstance(raw_source_blockers, (list, tuple)) or any(
        type(item) is not str or not item.strip()
        for item in raw_source_blockers
    ):
        raise ValueError("authoritative corpus blockers are not a string sequence")
    source_blockers = list(raw_source_blockers)
    reader_ready_for_candidate_search = payload.get("ready_for_candidate_search")
    if type(reader_ready_for_candidate_search) is not bool:
        raise ValueError(
            "authoritative corpus candidate-search readiness must be boolean"
        )
    if any(
        payload.get(key) is not False
        for key in (
            "live_eligible",
            "telegram_delivery_allowed",
            "trade_execution_allowed",
        )
    ):
        raise ValueError(
            "authoritative corpus payload exceeds the ingestion authority boundary"
        )
    expected_authority_boundary = {
        "formula_registry_effect": "NONE",
        "authority_effect": "NONE",
        "delivery_channel": "NONE",
    }
    if any(
        payload.get(key) != expected
        for key, expected in expected_authority_boundary.items()
    ):
        raise ValueError(
            "authoritative corpus payload exceeds the ingestion authority boundary"
        )

    bound_parent_ids: set[str] = set()
    labeled_parent_ids: set[str] = set()
    source_event_ids: set[int] = set()
    projection_event_ids: set[int] = set()
    projection_decision_times: list[str] = []
    wave_bound_observations = 0
    labeled_observations = 0
    for row in rows:
        binding = row.get("wave_binding")
        binding = dict(binding) if isinstance(binding, Mapping) else {}
        outcome = row.get("outcome")
        outcome = dict(outcome) if isinstance(outcome, Mapping) else {}
        parent_id = str(binding.get("btc_parent_movement_id") or "").strip()
        if binding.get("status") == "BOUND":
            wave_bound_observations += 1
            if parent_id:
                bound_parent_ids.add(parent_id)
        if outcome.get("status") == "AVAILABLE":
            labeled_observations += 1
            if binding.get("status") == "BOUND" and parent_id:
                labeled_parent_ids.add(parent_id)
        projection_event_id = row.get("projection_event_id")
        if type(projection_event_id) is int:
            projection_event_ids.add(projection_event_id)
        projection_time = str(row.get("projection_decision_time_utc") or "").strip()
        if projection_time:
            projection_decision_times.append(projection_time)
        for event_id in row.get("source_event_ids") or []:
            if type(event_id) is int:
                source_event_ids.add(event_id)

    reported_observations = int(source_counts.get("observations") or 0)
    reported_labels = int(source_counts.get("available_outcomes") or 0)
    if reported_observations != len(rows):
        raise ValueError("authoritative corpus observation count is inconsistent")
    if reported_labels != labeled_observations:
        raise ValueError("authoritative corpus label count is inconsistent")
    if (
        type(source_counts.get("distinct_btc_parent_movements")) is not int
        or source_counts["distinct_btc_parent_movements"]
        != len(bound_parent_ids)
    ):
        raise ValueError(
            "authoritative corpus distinct parent-movement count is inconsistent"
        )

    blockers = list(
        dict.fromkeys(
            [
                *source_blockers,
                "CANDIDATE_SEARCH_NOT_YET_RUN",
                "EXPERIMENTAL_ELIGIBILITY_GATE_NOT_EVALUATED",
            ]
        )
    )

    return {
        "status": "INGESTED",
        "dataset_kind": _STAGE4_CORPUS_DATASET_KIND,
        "horizon_minutes": int(horizon_minutes),
        "schedule_slot_utc": schedule_slot_utc.isoformat(),
        "due_at_utc": due_at_utc.isoformat(),
        "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
        "duration_ms": max(0, int(duration_ms)),
        "analysis_as_of_utc": payload.get("analysis_as_of_utc"),
        "database_snapshot_id": payload.get("database_snapshot_id"),
        "corpus_attestation_receipt_sha256": payload.get(
            "attestation_receipt_sha256"
        ),
        "reader_ready_for_candidate_search": reader_ready_for_candidate_search,
        "source_attestation": {
            key: source_attestation.get(key)
            for key in (
                "source_contract_version",
                "source_catalog_sha256",
                "outcomes_view_definition_sha256",
                "outcomes_stage4_source_catalog_sha256",
                "no_signal_outcomes_view_definition_sha256",
                "no_signal_outcomes_stage4_source_catalog_sha256",
                "no_signal_outcomes_raw_catalog_sha256",
                "no_signal_outcomes_trigger_catalog_sha256",
                "no_signal_reference_hash_contract_version",
                "no_signal_outcome_hash_contract_version",
            )
            if source_attestation.get(key) is not None
        },
        "counts": {
            "observations": len(rows),
            "labeled_observations": labeled_observations,
            "wave_rows": int(source_counts.get("wave_rows") or 0),
            "wave_bound_observations": wave_bound_observations,
            "distinct_btc_parent_movements": len(bound_parent_ids),
            "distinct_labeled_btc_parent_movements": len(labeled_parent_ids),
            "projections": int(source_counts.get("projections") or 0),
            "stage4_events": int(source_counts.get("stage4_events") or 0),
            "signal_events": int(source_counts.get("signal_events") or 0),
            "outcome_rows": int(source_counts.get("outcome_rows") or 0),
        },
        "cursor": cursor,
        "source_bounds": {
            "max_projection_event_id": (
                max(projection_event_ids) if projection_event_ids else None
            ),
            "max_projection_decision_time_utc": (
                max(projection_decision_times) if projection_decision_times else None
            ),
            "max_source_event_id": max(source_event_ids) if source_event_ids else None,
        },
        "blockers": blockers,
        "ready_for_candidate_search": False,
        "candidate_eligibility_gate": dict(_STAGE4_CANDIDATE_ELIGIBILITY_GATE),
        "formula_registry_effect": "NONE",
        "authority_effect": "NONE",
        "delivery_channel": "NONE",
        "live_eligible": False,
        "telegram_delivery_allowed": False,
        "trade_execution_allowed": False,
    }


def _bounded_stage4_candidate_search_receipt(
    result: Mapping[str, Any],
) -> Dict[str, Any]:
    """Validate and compact pure-search output before exposing it in health."""

    if not isinstance(result, Mapping):
        raise ValueError("Stage-4 candidate search result is not a mapping")
    if type(result.get("ready_for_candidate_search")) is not bool:
        raise ValueError("Stage-4 candidate search readiness must be boolean")
    expected_boundary = {
        "formula_registry_effect": "NONE",
        "authority_effect": "NONE",
        "delivery_channel": "NONE",
        "live_eligible": False,
        "telegram_delivery_allowed": False,
        "trade_execution_allowed": False,
    }
    if any(
        result.get(key) != expected
        for key, expected in expected_boundary.items()
    ):
        raise ValueError("Stage-4 candidate search exceeded its authority boundary")
    raw_candidates = result.get("candidates")
    if not isinstance(raw_candidates, (list, tuple)) or any(
        not isinstance(candidate, Mapping) for candidate in raw_candidates
    ):
        raise ValueError("Stage-4 candidate search candidates are malformed")
    max_candidates = (
        research_stage4_candidate_search.Stage4SearchConfig()
        .max_candidates_returned
    )
    if len(raw_candidates) > max_candidates:
        raise ValueError("Stage-4 candidate search exceeded its result bound")
    counts = result.get("counts")
    if not isinstance(counts, Mapping):
        raise ValueError("Stage-4 candidate search counts are malformed")

    compact_candidates: list[Dict[str, Any]] = []
    eligible_count = 0
    for raw_candidate in raw_candidates:
        candidate = dict(raw_candidate)
        if any(
            candidate.get(key) != expected
            for key, expected in expected_boundary.items()
        ):
            raise ValueError(
                "Stage-4 candidate exceeded its authority boundary"
            )
        conditions = candidate.get("conditions")
        if not isinstance(conditions, (list, tuple)) or not (
            1 <= len(conditions) <= 3
        ):
            raise ValueError("Stage-4 candidate conditions are malformed")
        metrics = candidate.get("metrics")
        gate = candidate.get("eligibility_gate")
        occurrence_counts = candidate.get("occurrence_counts")
        multiple_testing = candidate.get("multiple_testing")
        if not all(
            isinstance(value, Mapping)
            for value in (metrics, gate, occurrence_counts, multiple_testing)
        ):
            raise ValueError("Stage-4 candidate evidence is malformed")
        eligible = candidate.get("experimental_formula_eligible")
        if type(eligible) is not bool or gate.get("passed") is not eligible:
            raise ValueError("Stage-4 candidate eligibility is inconsistent")
        if (
            gate.get("atomic") is not True
            or gate.get("separate_later_probability_gate") is not False
            or int(gate.get("minimum_independent_occurrences") or 0)
            < _STAGE4_MIN_INDEPENDENT_OCCURRENCES
        ):
            raise ValueError("Stage-4 candidate atomic gate is malformed")
        accepted_paths = candidate.get("accepted_paths")
        if not isinstance(accepted_paths, (list, tuple)) or any(
            path not in {"PROBABILITY", "ASYMMETRY"}
            for path in accepted_paths
        ):
            raise ValueError("Stage-4 candidate accepted paths are malformed")
        if eligible:
            eligible_count += 1
            if (
                int(occurrence_counts.get("completed") or 0)
                < int(gate["minimum_independent_occurrences"])
                or not accepted_paths
            ):
                raise ValueError(
                    "Stage-4 candidate bypassed the atomic eligibility gate"
                )
        if multiple_testing.get("decision_effect") != (
            "DISCLOSURE_ONLY_EXPERIMENTAL"
        ) or multiple_testing.get("eligibility_changed") is not False:
            raise ValueError(
                "Stage-4 candidate multiple-testing boundary is malformed"
            )
        compact_candidates.append(
            {
                key: candidate.get(key)
                for key in (
                    "candidate_key",
                    "candidate_schema_version",
                    "engine_version",
                    "feature_schema_version",
                    "label_policy_version",
                    "independence_policy_version",
                    "direction",
                    "horizon_minutes",
                    "conditions",
                    "formula_text",
                    "condition_source_closure",
                    "condition_evidence_sources",
                    "raw_match_count",
                    "match_set_sha256",
                    "occurrence_counts",
                    "metrics",
                    "accepted_paths",
                    "experimental_formula_eligible",
                    "eligibility_gate",
                    "multiple_testing",
                    "experimental_caveats",
                    "display_equivalent_candidates",
                    "display_equivalent_candidate_keys",
                    "formula_registry_effect",
                    "authority_effect",
                    "delivery_channel",
                    "live_eligible",
                    "telegram_delivery_allowed",
                    "trade_execution_allowed",
                )
            }
        )
    reported_eligible = int(counts.get("eligible_experimental_candidates") or 0)
    if reported_eligible != eligible_count:
        raise ValueError("Stage-4 candidate eligible count is inconsistent")
    return {
        "status": str(result.get("status") or "UNKNOWN"),
        "ran": True,
        "ready_for_candidate_search": result["ready_for_candidate_search"],
        "search_receipt_sha256": result.get("search_receipt_sha256"),
        "engine_version": result.get("engine_version"),
        "candidate_schema_version": result.get("candidate_schema_version"),
        "feature_schema_version": result.get("feature_schema_version"),
        "label_policy_version": result.get("label_policy_version"),
        "independence_policy_version": result.get(
            "independence_policy_version"
        ),
        "analysis_as_of_utc": result.get("analysis_as_of_utc"),
        "horizon_minutes": result.get("horizon_minutes"),
        "qualifying_favorable_move_pct": result.get(
            "qualifying_favorable_move_pct"
        ),
        "counts": dict(counts),
        "search_budget_exhausted": bool(result.get("search_budget_exhausted")),
        "atomic_eligibility": dict(result.get("atomic_eligibility") or {}),
        "statistical_scope": dict(result.get("statistical_scope") or {}),
        "candidates": compact_candidates,
        "formula_registry_effect": "NONE",
        "authority_effect": "NONE",
        "delivery_channel": "NONE",
        "live_eligible": False,
        "telegram_delivery_allowed": False,
        "trade_execution_allowed": False,
    }


def _attach_stage4_candidate_search(
    receipt: Mapping[str, Any], result: Mapping[str, Any]
) -> Dict[str, Any]:
    candidate_search = _bounded_stage4_candidate_search_receipt(result)
    source_ready = receipt.get("reader_ready_for_candidate_search") is True
    search_ready = candidate_search["ready_for_candidate_search"] is True
    ready_for_candidate_search = source_ready and search_ready
    obsolete = {
        "CANDIDATE_SEARCH_ADAPTER_NOT_IMPLEMENTED",
        "CANDIDATE_SEARCH_NOT_YET_RUN",
        "EXPERIMENTAL_ELIGIBILITY_GATE_NOT_EVALUATED",
    }
    blockers = [
        blocker for blocker in receipt.get("blockers") or [] if blocker not in obsolete
    ]
    if not search_ready:
        blockers.append("CANDIDATE_SEARCH_RAN_NOT_READY")
    eligible_count = int(
        (candidate_search.get("counts") or {}).get(
            "eligible_experimental_candidates"
        )
        or 0
    )
    return {
        **dict(receipt),
        "status": "INGESTED_AND_SEARCHED",
        "blockers": list(dict.fromkeys(blockers)),
        "ready_for_candidate_search": ready_for_candidate_search,
        "candidate_search": candidate_search,
        "candidate_eligibility_gate": {
            **dict(_STAGE4_CANDIDATE_ELIGIBILITY_GATE),
            "status": "EVALUATED",
            "search_receipt_sha256": candidate_search.get(
                "search_receipt_sha256"
            ),
            "eligible_experimental_candidates": eligible_count,
        },
        "formula_registry_effect": "NONE",
        "authority_effect": "NONE",
        "delivery_channel": "NONE",
        "live_eligible": False,
        "telegram_delivery_allowed": False,
        "trade_execution_allowed": False,
    }


def _conditions(value: Any) -> list[Dict[str, Any]]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = []
    return [dict(item) for item in value or [] if isinstance(item, dict)]


def _select_shadow_work_prefixes(
    work: list[Mapping[str, Any]], *, max_formula_events: int = 250
) -> Dict[int, list[int]]:
    """Select only contiguous event-id prefixes for each formula cursor."""

    budget = max(1, int(max_formula_events))
    queues: Dict[int, list[int]] = {}
    for formula in work:
        formula_id = int(formula["formula_id"])
        event_ids = sorted(
            {int(event["event_id"]) for event in formula.get("events") or []}
        )
        if event_ids:
            queues[formula_id] = event_ids
    selected: Dict[int, list[int]] = {formula_id: [] for formula_id in queues}
    offsets = {formula_id: 0 for formula_id in queues}
    pending = [
        (event_ids[0], formula_id)
        for formula_id, event_ids in queues.items()
    ]
    heapq.heapify(pending)
    while budget > 0 and pending:
        event_id, formula_id = heapq.heappop(pending)
        selected[formula_id].append(event_id)
        budget -= 1
        offset = offsets[formula_id] + 1
        offsets[formula_id] = offset
        if offset < len(queues[formula_id]):
            heapq.heappush(
                pending, (queues[formula_id][offset], formula_id)
            )
    return {
        formula_id: event_ids
        for formula_id, event_ids in selected.items()
        if event_ids
    }


def _as_utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        timestamp = value
    else:
        timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def _max_pain_snapshot_evidence(
    *,
    formula: Mapping[str, Any],
    row: Optional[Mapping[str, Any]],
) -> Optional[Dict[str, Any]]:
    return research_formula_store._canonical_max_pain_snapshot_evidence(
        formula, row
    )


def _shadow_snapshot(
    *,
    formula: Mapping[str, Any],
    event: Mapping[str, Any],
    row: Optional[Mapping[str, Any]],
    evaluation: Mapping[str, Any],
) -> Dict[str, Any]:
    features = evaluation.get("features")
    if not isinstance(features, Mapping):
        features = {}
    conditions = _conditions(formula.get("conditions"))
    raw = row.get("raw_features") if isinstance(row, Mapping) else {}
    if not isinstance(raw, Mapping):
        raw = {}
    latest = raw.get("latest_at_or_before_alert")
    if not isinstance(latest, Mapping):
        latest = {}
    label = row.get("outcome_label") if isinstance(row, Mapping) else {}
    if not isinstance(label, Mapping):
        label = {}
    session = {
        key: label.get(key)
        for key in (
            "session_active_ratio",
            "session_weekend_ratio",
            "session_segments",
            "session_composition",
        )
    }
    prospective = row.get("prospective_evidence") if isinstance(row, Mapping) else {}
    if not isinstance(prospective, Mapping):
        prospective = {}
    snapshot = {
        "snapshot_policy_version": (
            research_formula_store._SHADOW_INPUT_SNAPSHOT_POLICY_VERSION
        ),
        "decision_cohort_policy_version": _DECISION_COHORT_POLICY_VERSION,
        "decision_input_policy_version": (
            row.get("decision_input_policy_version")
            if isinstance(row, Mapping)
            else None
        ),
        "evidence_policy_version": (
            research_formula_store._PROSPECTIVE_EVIDENCE_POLICY_VERSION
        ),
        "prospective_evidence": {
            "sampler_version": prospective.get("sampler_version"),
            "feature_bundle_policy_version": prospective.get(
                "feature_bundle_policy_version"
            ),
            "anchor_slot_id": prospective.get("anchor_slot_id"),
            "input_fingerprint": prospective.get("input_fingerprint"),
            "feature_bundle_sha256": prospective.get(
                "feature_bundle_sha256"
            ),
            "source_timestamps": (
                dict(prospective.get("source_timestamps"))
                if isinstance(prospective.get("source_timestamps"), Mapping)
                else {}
            ),
            "source_provenance": (
                dict(prospective.get("source_provenance"))
                if isinstance(prospective.get("source_provenance"), Mapping)
                else {}
            ),
        },
        "formula_id": int(formula.get("formula_id") or 0),
        "formula_key": formula.get("formula_key"),
        "formula_version": int(formula.get("formula_version") or 0),
        "formula_schema_version": formula.get("formula_schema_version"),
        "engine_version": formula.get("engine_version"),
        "outcome_method_version": formula.get("outcome_method_version"),
        "horizon_minutes": int(formula.get("horizon_minutes") or 0),
        "event": {
            "event_id": int(event["event_id"]),
            "alert_time_utc": event.get("alert_time_utc"),
            "symbol": event.get("symbol"),
            "direction": event.get("direction"),
            "event_type": event.get("event_type"),
            "setup_key": event.get("setup_key"),
            "source_side": event.get("source_side"),
            "timeframe": event.get("timeframe"),
            "strategy_version": event.get("strategy_version"),
            "code_version": event.get("code_version"),
        },
        "formula_key_features": {
            condition["feature"]: features.get(condition["feature"])
            for condition in conditions
            if condition.get("feature")
        },
        "conditions": conditions,
        "condition_results": list(evaluation.get("condition_results") or []),
        "evaluation_status": str(
            evaluation.get("status") or "UNEVALUABLE"
        ).upper(),
        "evaluation_reason": evaluation.get("reason"),
        "feature_schema_version": formula.get("feature_schema_version"),
        "source_inputs": {
            family: dict(values) if isinstance(values, Mapping) else {}
            for family, values in latest.items()
            if family in {"price_oi", "futures_cvd", "spot_cvd"}
        },
        "outcome_window_session": session,
        "movement_width_reference": (
            dict(label.get("movement_width_reference"))
            if isinstance(label.get("movement_width_reference"), Mapping)
            else {}
        ),
        "lookahead_contract": (
            "decision-time inputs and prior-only width calibration; no realized "
            "return, MFE or MAE"
        ),
    }
    if (
        formula.get("formula_schema_version")
        == research_formula_store._LEGACY_V5_FORMULA_SCHEMA_VERSION
    ):
        snapshot["legacy_v5_shadow_adapter_version"] = (
            research_formula_store._LEGACY_V5_SHADOW_ADAPTER_VERSION
        )
    max_pain_evidence = _max_pain_snapshot_evidence(formula=formula, row=row)
    if max_pain_evidence is not None:
        # Audit identities are stored only for formulas that actually consume
        # Max-Pain. Formula candidate extraction continues to see only the
        # condition values in ``formula_key_features``.
        snapshot["max_pain_provenance"] = max_pain_evidence
    return snapshot


def _decision_cohort(
    *,
    formula: Mapping[str, Any],
    event: Mapping[str, Any],
    snapshot: Mapping[str, Any],
) -> tuple[str, datetime]:
    return research_formula_store._decision_cohort_identity(
        formula=formula,
        event=event,
        snapshot=snapshot,
    )


@dataclass
class FormulaWorkerMetrics:
    discovery_cycles: int = 0
    discovery_runs: int = 0
    discovery_locked_skips: int = 0
    discovery_unchanged_skips: int = 0
    discovery_unavailable_skips: int = 0
    candidates_evaluated: int = 0
    formulas_persisted: int = 0
    shadow_cycles: int = 0
    shadow_checks: int = 0
    shadow_hits: int = 0
    live_candidates_queued: int = 0
    live_deliveries_sent: int = 0
    live_deliveries_failed: int = 0
    formulas_promoted_live: int = 0
    formulas_ready_for_review: int = 0
    research_ready_formulas: int = 0
    experimentally_relevant_formulas: int = 0
    suspended_relevance_formulas: int = 0
    recovering_relevance_formulas: int = 0
    legacy_live_review_ready_formulas: int = 0
    failures: int = 0
    discovery_phase: str = "IDLE"
    shadow_phase: str = "IDLE"
    last_discovery_phase: Optional[str] = None
    last_shadow_phase: Optional[str] = None
    last_discovery_phase_duration_ms: Optional[int] = None
    last_shadow_phase_duration_ms: Optional[int] = None
    last_discovery_error: Optional[str] = None
    last_shadow_error: Optional[str] = None
    last_discovery_error_phase: Optional[str] = None
    last_shadow_error_phase: Optional[str] = None
    last_discovery_timeout_phase: Optional[str] = None
    last_shadow_timeout_phase: Optional[str] = None
    last_discovery_timeout_at_utc: Optional[str] = None
    last_shadow_timeout_at_utc: Optional[str] = None
    last_timeout_phase: Optional[str] = None
    last_discovery_utc: Optional[str] = None
    last_discovery_horizon_minutes: Optional[int] = None
    last_discovery_slot_utc: Optional[str] = None
    last_shadow_utc: Optional[str] = None
    last_error: Optional[str] = None
    stage4_ingestion_attempts: int = 0
    stage4_ingestion_successes: int = 0
    stage4_ingestion_configuration_required: int = 0
    stage4_ingestion_failures: int = 0
    stage4_ingestion_same_slot_reuses: int = 0
    last_stage4_ingestion_utc: Optional[str] = None
    last_stage4_ingestion_error: Optional[str] = None
    stage4_candidate_search_attempts: int = 0
    stage4_candidate_search_successes: int = 0
    stage4_candidate_search_failures: int = 0
    last_stage4_candidate_search_utc: Optional[str] = None
    last_stage4_candidate_search_error: Optional[str] = None


class FormulaResearchWorker:
    def __init__(self) -> None:
        self.metrics = FormulaWorkerMetrics()
        self._discovery_task: Optional[asyncio.Task] = None
        self._shadow_task: Optional[asyncio.Task] = None
        self._stopping = False
        self._schema_ready = False
        self._telegram_bot: Any = None
        self._stage4_ingestion_lock = threading.Lock()
        self._stage4_ingestion_attempted_slots: set[tuple[int, str]] = set()
        self._stage4_ingestion_receipts: Dict[int, Dict[str, Any]] = {}

    def _begin_lane_attempt(self, lane: str) -> None:
        normalized_lane = str(lane or "").strip().lower()
        if normalized_lane not in {"discovery", "shadow"}:
            raise ValueError("formula worker phase lane is invalid")
        setattr(self.metrics, f"last_{normalized_lane}_error", None)
        setattr(self.metrics, f"last_{normalized_lane}_error_phase", None)

    def _record_lane_timeout(self, lane: str, phase: str) -> None:
        normalized_lane = str(lane or "").strip().lower()
        normalized_phase = str(phase or "UNKNOWN").strip().upper()
        qualified = f"{normalized_lane.upper()}:{normalized_phase}"
        setattr(
            self.metrics,
            f"last_{normalized_lane}_timeout_phase",
            normalized_phase,
        )
        setattr(
            self.metrics,
            f"last_{normalized_lane}_timeout_at_utc",
            datetime.now(timezone.utc).isoformat(),
        )
        self.metrics.last_timeout_phase = qualified

    @contextmanager
    def _phase(self, lane: str, name: str):
        normalized_lane = str(lane or "").strip().lower()
        if normalized_lane not in {"discovery", "shadow"}:
            raise ValueError("formula worker phase lane is invalid")
        phase = str(name or "UNKNOWN").strip().upper()
        started = time.monotonic()
        setattr(self.metrics, f"{normalized_lane}_phase", phase)
        try:
            yield
        except Exception as exc:
            setattr(
                self.metrics,
                f"last_{normalized_lane}_error",
                f"{type(exc).__name__}: {exc}",
            )
            setattr(
                self.metrics,
                f"last_{normalized_lane}_error_phase",
                phase,
            )
            if "statement timeout" in str(exc).lower():
                self._record_lane_timeout(normalized_lane, phase)
            raise
        finally:
            setattr(self.metrics, f"last_{normalized_lane}_phase", phase)
            setattr(
                self.metrics,
                f"last_{normalized_lane}_phase_duration_ms",
                max(0, int(round((time.monotonic() - started) * 1000))),
            )
            setattr(self.metrics, f"{normalized_lane}_phase", "IDLE")

    def bind_telegram(self, bot: Any) -> None:
        """Bind the initialized Telegram bot used for durable live delivery."""
        self._telegram_bot = bot

    @staticmethod
    def _stage4_receipt_boundary() -> Dict[str, Any]:
        return {
            "ready_for_candidate_search": False,
            "candidate_eligibility_gate": dict(
                _STAGE4_CANDIDATE_ELIGIBILITY_GATE
            ),
            "formula_registry_effect": "NONE",
            "authority_effect": "NONE",
            "delivery_channel": "NONE",
            "live_eligible": False,
            "telegram_delivery_allowed": False,
            "trade_execution_allowed": False,
        }

    def _store_stage4_ingestion_receipt(
        self, horizon: int, receipt: Mapping[str, Any]
    ) -> Dict[str, Any]:
        normalized = json.loads(
            json.dumps(
                dict(receipt),
                sort_keys=True,
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            )
        )
        with self._stage4_ingestion_lock:
            self._stage4_ingestion_receipts[int(horizon)] = normalized
        return json.loads(json.dumps(normalized))

    def _stage4_ingestion_status(self) -> Dict[str, Any]:
        configured = bool(
            os.getenv(
                research_signal_formula_exploration_reader.DATABASE_URL_ENV,
                "",
            ).strip()
        )
        with self._stage4_ingestion_lock:
            receipts = {
                str(horizon): json.loads(json.dumps(receipt))
                for horizon, receipt in self._stage4_ingestion_receipts.items()
            }
        receipt_statuses = {
            str(receipt.get("status") or "UNKNOWN") for receipt in receipts.values()
        }
        if not configured:
            status = "CONFIGURATION_REQUIRED"
        elif not receipts:
            status = "WAITING_FOR_DISCOVERY_SLOT"
        elif "LOADING" in receipt_statuses:
            status = "LOADING"
        elif receipt_statuses.intersection({"FAILED", "INGESTED_SEARCH_FAILED"}):
            status = "DEGRADED"
        elif "INGESTED_AND_SEARCHED" in receipt_statuses:
            status = "CANDIDATE_SEARCH_OBSERVED"
        elif "INGESTED" in receipt_statuses:
            status = "INGESTION_OBSERVED"
        else:
            status = sorted(receipt_statuses)[0]
        searched_horizons = sorted(
            int(horizon)
            for horizon, receipt in receipts.items()
            if (receipt.get("candidate_search") or {}).get("ran") is True
        )
        ready_horizons = sorted(
            int(horizon)
            for horizon, receipt in receipts.items()
            if receipt.get("ready_for_candidate_search") is True
        )
        boundary = self._stage4_receipt_boundary()
        boundary["ready_for_candidate_search"] = bool(ready_horizons)
        if searched_horizons:
            boundary["candidate_eligibility_gate"] = {
                **dict(_STAGE4_CANDIDATE_ELIGIBILITY_GATE),
                "status": "EVALUATED_BY_HORIZON",
                "searched_horizons_minutes": searched_horizons,
                "ready_horizons_minutes": ready_horizons,
            }
        return {
            "status": status,
            "configured": configured,
            "database_url_env": (
                research_signal_formula_exploration_reader.DATABASE_URL_ENV
            ),
            "dataset_kind": _STAGE4_CORPUS_DATASET_KIND,
            "bounded_request": {
                "lookback_days": _STAGE4_CORPUS_LOOKBACK_DAYS,
                "projection_limit_per_horizon_slot": (
                    _STAGE4_CORPUS_PROJECTION_LIMIT
                ),
                "pagination": "FIRST_PAGE_ONLY_PER_HORIZON_SLOT",
            },
            "receipts_by_horizon": receipts,
            **boundary,
        }

    def _ingest_authoritative_stage4_corpus_once(
        self,
        horizon: int,
        *,
        schedule_slot_utc: datetime,
        due_at_utc: datetime,
    ) -> Dict[str, Any]:
        """Read one bounded corpus page and expose only a non-authoritative receipt."""

        slot = _as_utc(schedule_slot_utc)
        due_at = _as_utc(due_at_utc)
        slot_key = (int(horizon), slot.isoformat())
        loading_receipt = {
            "status": "LOADING",
            "dataset_kind": _STAGE4_CORPUS_DATASET_KIND,
            "horizon_minutes": int(horizon),
            "schedule_slot_utc": slot.isoformat(),
            "due_at_utc": due_at.isoformat(),
            "started_at_utc": datetime.now(timezone.utc).isoformat(),
            "corpus_attestation_receipt_sha256": None,
            "source_attestation": {},
            "counts": {
                "observations": 0,
                "labeled_observations": 0,
                "wave_rows": 0,
                "wave_bound_observations": 0,
                "distinct_btc_parent_movements": 0,
                "distinct_labeled_btc_parent_movements": 0,
                "projections": 0,
                "stage4_events": 0,
                "signal_events": 0,
                "outcome_rows": 0,
            },
            "cursor": {},
            "source_bounds": {
                "max_projection_event_id": None,
                "max_projection_decision_time_utc": None,
                "max_source_event_id": None,
            },
            "blockers": ["AUTHORITATIVE_CORPUS_READ_IN_PROGRESS"],
            **self._stage4_receipt_boundary(),
        }
        with self._stage4_ingestion_lock:
            if slot_key in self._stage4_ingestion_attempted_slots:
                existing = self._stage4_ingestion_receipts.get(
                    int(horizon), loading_receipt
                )
                self.metrics.stage4_ingestion_same_slot_reuses += 1
                reused = json.loads(json.dumps(existing))
                reused["same_slot_deduplicated"] = True
                return reused
            self._stage4_ingestion_attempted_slots = {
                key
                for key in self._stage4_ingestion_attempted_slots
                if key[0] != int(horizon)
            }
            self._stage4_ingestion_attempted_slots.add(slot_key)
            self._stage4_ingestion_receipts[int(horizon)] = loading_receipt

        database_url = os.getenv(
            research_signal_formula_exploration_reader.DATABASE_URL_ENV,
            "",
        ).strip()
        if not database_url:
            self.metrics.stage4_ingestion_configuration_required += 1
            receipt = {
                **loading_receipt,
                "status": "CONFIGURATION_REQUIRED",
                "completed_at_utc": datetime.now(timezone.utc).isoformat(),
                "blockers": [
                    "DEDICATED_STAGE4_READER_DATABASE_URL_MISSING",
                    "CANDIDATE_SEARCH_NOT_RUN",
                    "EXPERIMENTAL_ELIGIBILITY_GATE_NOT_EVALUATED",
                ],
            }
            return self._store_stage4_ingestion_receipt(horizon, receipt)

        self.metrics.stage4_ingestion_attempts += 1
        started = time.monotonic()
        try:
            corpus = (
                research_signal_formula_exploration_reader
                .load_authoritative_stage4_corpus(
                    horizon_minutes=int(horizon),
                    lookback_days=_STAGE4_CORPUS_LOOKBACK_DAYS,
                    projection_limit=_STAGE4_CORPUS_PROJECTION_LIMIT,
                    before_cursor=None,
                    database_url=database_url,
                )
            )
            payload = corpus.to_dict()
            if not isinstance(payload, Mapping):
                raise TypeError("authoritative Stage-4 corpus payload is not a mapping")
            receipt = _stage4_corpus_observability_receipt(
                payload,
                horizon_minutes=int(horizon),
                schedule_slot_utc=slot,
                due_at_utc=due_at,
                duration_ms=max(
                    0, int(round((time.monotonic() - started) * 1000))
                ),
            )
        except Exception as exc:
            self.metrics.stage4_ingestion_failures += 1
            error = f"{type(exc).__name__}: {exc}"[:1000]
            self.metrics.last_stage4_ingestion_error = error
            receipt = {
                **loading_receipt,
                "status": "FAILED",
                "completed_at_utc": datetime.now(timezone.utc).isoformat(),
                "duration_ms": max(
                    0, int(round((time.monotonic() - started) * 1000))
                ),
                "error": error,
                "blockers": [
                    "AUTHORITATIVE_STAGE4_CORPUS_READ_FAILED",
                    "CANDIDATE_SEARCH_NOT_RUN",
                    "EXPERIMENTAL_ELIGIBILITY_GATE_NOT_EVALUATED",
                ],
            }
            return self._store_stage4_ingestion_receipt(horizon, receipt)

        self.metrics.stage4_ingestion_successes += 1
        self.metrics.last_stage4_ingestion_utc = receipt["ingested_at_utc"]
        self.metrics.last_stage4_ingestion_error = None
        self.metrics.stage4_candidate_search_attempts += 1
        search_started = time.monotonic()
        try:
            search_result = (
                research_stage4_candidate_search.search_experimental_candidates(
                    payload["observations"],
                    horizon_minutes=int(horizon),
                    analysis_as_of_utc=payload["analysis_as_of_utc"],
                )
            )
            receipt = _attach_stage4_candidate_search(receipt, search_result)
            candidate_search = dict(receipt["candidate_search"])
            candidate_search["duration_ms"] = max(
                0, int(round((time.monotonic() - search_started) * 1000))
            )
            receipt["candidate_search"] = candidate_search
        except Exception as exc:
            self.metrics.stage4_candidate_search_failures += 1
            error = f"{type(exc).__name__}: {exc}"[:1000]
            self.metrics.last_stage4_candidate_search_error = error
            receipt = {
                **receipt,
                "status": "INGESTED_SEARCH_FAILED",
                "completed_at_utc": datetime.now(timezone.utc).isoformat(),
                "blockers": list(
                    dict.fromkeys(
                        [
                            *(
                                blocker
                                for blocker in receipt.get("blockers") or []
                                if blocker != "CANDIDATE_SEARCH_NOT_YET_RUN"
                            ),
                            "EXPERIMENTAL_ELIGIBILITY_GATE_NOT_EVALUATED",
                            "CANDIDATE_SEARCH_FAILED",
                        ]
                    )
                ),
                "candidate_search": {
                    "status": "FAILED",
                    "ran": False,
                    "duration_ms": max(
                        0,
                        int(round((time.monotonic() - search_started) * 1000)),
                    ),
                    "error": error,
                    "ready_for_candidate_search": False,
                    "formula_registry_effect": "NONE",
                    "authority_effect": "NONE",
                    "delivery_channel": "NONE",
                    "live_eligible": False,
                    "telegram_delivery_allowed": False,
                    "trade_execution_allowed": False,
                },
                **self._stage4_receipt_boundary(),
            }
            return self._store_stage4_ingestion_receipt(horizon, receipt)

        self.metrics.stage4_candidate_search_successes += 1
        self.metrics.last_stage4_candidate_search_utc = datetime.now(
            timezone.utc
        ).isoformat()
        self.metrics.last_stage4_candidate_search_error = None
        return self._store_stage4_ingestion_receipt(horizon, receipt)

    def status(self) -> Dict[str, Any]:
        return {
            "discovery_enabled": _DISCOVERY_ENABLED,
            "shadow_enabled": _SHADOW_ENABLED,
            "live_alerts_enabled": _LIVE_ALERTS_ENABLED,
            "running": bool(
                (self._discovery_task and not self._discovery_task.done())
                or (self._shadow_task and not self._shadow_task.done())
            ),
            "schema_ready": self._schema_ready,
            "discovery_running": bool(
                self._discovery_task and not self._discovery_task.done()
            ),
            "shadow_running": bool(self._shadow_task and not self._shadow_task.done()),
            "horizons_minutes": list(_horizons()),
            "lookback_days": _LOOKBACK_DAYS,
            "dataset_limit": _DATASET_LIMIT,
            "dataset_mode": _DATASET_MODE,
            "heavy_query_timeout": research_feature_matrix.runtime_status()[
                "heavy_query_timeout"
            ],
            "database_timeout_policy": (
                research_formula_store.database_timeout_status()
            ),
            "hierarchical_search_enabled": _HIERARCHICAL_SEARCH_ENABLED,
            "research_acceptance_policy_version": (
                research_formula_acceptance.POLICY_VERSION
            ),
            "relevance_hysteresis": research_formula_relevance.descriptor(),
            "market_episode_policy_version": research_market_episode.POLICY_VERSION,
            "evidence_contract": research_evidence_contract.contract_descriptor(),
            "recent_window_days": _discovery_config().recent_window_days,
            "recency_half_life_days": _discovery_config().recency_half_life_days,
            "discovery_scheduler_version": (
                research_formula_store.DISCOVERY_SCHEDULER_VERSION
            ),
            "discovery_cadence_minutes_by_horizon": {
                str(horizon): horizon for horizon in _horizons()
            },
            "discovery_slot_grace_seconds": _DISCOVERY_SLOT_GRACE_SECONDS,
            "discovery_idle_poll_seconds": _DISCOVERY_IDLE_POLL_SECONDS,
            "walk_forward_policy_version": (
                research_formula_engine.WALK_FORWARD_POLICY_VERSION
            ),
            "purge_policy_version": research_formula_engine.PURGE_POLICY_VERSION,
            "embargo_policy_version": (
                research_formula_engine.EMBARGO_POLICY_VERSION
            ),
            "shadow_poll_seconds": _SHADOW_POLL_SECONDS,
            "shadow_evidence_policy_version": (
                research_formula_store._PROSPECTIVE_EVIDENCE_POLICY_VERSION
            ),
            "shadow_snapshot_policy_version": (
                research_formula_store._SHADOW_INPUT_SNAPSHOT_POLICY_VERSION
            ),
            "automatic_stage_ceiling": "SHADOW_PENDING_EXPLICIT_APPROVAL",
            "live_delivery_gate": {
                "environment_enabled": _LIVE_ALERTS_ENABLED,
                "formula_validation_required": True,
                "telegram_delivery_connected": self._telegram_bot is not None,
                "chat_subscription_required": True,
                "reason": (
                    "delivery requires a separate explicit owner approval record, LIVE "
                    "stage, runtime enablement and /ai_alerts_on in the destination chat"
                ),
            },
            "canonical_outcomes": (
                "Binance Spot USDT 1m; HYPE via Hyperliquid HYPE/USDT spot (@107) 1m"
            ),
            "stage4_authoritative_ingestion": self._stage4_ingestion_status(),
            "metrics": self.metrics.__dict__.copy(),
        }

    async def start(self) -> bool:
        if not (_DISCOVERY_ENABLED or _SHADOW_ENABLED):
            return False
        schema = await asyncio.to_thread(research_formula_store.schema_status)
        if not schema.get("schema_present"):
            self._schema_ready = False
            raise RuntimeError(
                f"Formula Research schema is not installed: {schema.get('missing_tables')}"
            )
        self._schema_ready = True
        self._stopping = False
        if _DISCOVERY_ENABLED and not (
            self._discovery_task and not self._discovery_task.done()
        ):
            self._discovery_task = asyncio.create_task(
                self._discovery_loop(), name="formula-discovery-worker"
            )
        if _SHADOW_ENABLED and not (
            self._shadow_task and not self._shadow_task.done()
        ):
            self._shadow_task = asyncio.create_task(
                self._shadow_loop(), name="formula-shadow-worker"
            )
        return True

    async def stop(self) -> None:
        self._stopping = True
        tasks = [task for task in (self._discovery_task, self._shadow_task) if task]
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._discovery_task = None
        self._shadow_task = None

    async def _discovery_loop(self) -> None:
        await asyncio.sleep(_DISCOVERY_STARTUP_DELAY_SECONDS)
        while not self._stopping:
            now = datetime.now(timezone.utc)
            self._begin_lane_attempt("discovery")
            try:
                due_work = []
                for horizon in _horizons():
                    slot, due_at = _discovery_schedule(horizon, now=now)
                    state = await asyncio.to_thread(
                        research_formula_store.load_discovery_schedule_state,
                        horizon,
                    )
                    last_slot = (
                        _as_utc(state["last_slot_utc"])
                        if state and state.get("last_slot_utc") is not None
                        else None
                    )
                    if last_slot is None or last_slot < slot:
                        due_work.append((slot, horizon, due_at))
                for slot, horizon, due_at in sorted(due_work):
                    await asyncio.to_thread(
                        self.run_discovery_horizon_once,
                        horizon,
                        schedule_slot_utc=slot,
                        due_at_utc=due_at,
                    )
            except Exception as exc:
                self.metrics.failures += 1
                self.metrics.last_error = f"{type(exc).__name__}: {exc}"
                self.metrics.last_discovery_error = self.metrics.last_error
                phase = self.metrics.last_discovery_error_phase or "SCHEDULER"
                self.metrics.last_discovery_error_phase = phase
                if "statement timeout" in str(exc).lower():
                    self._record_lane_timeout("discovery", phase)
                print(f"[formula-discovery] cycle failed open: {exc!r}", flush=True)
            next_due = _next_discovery_due_at(now=datetime.now(timezone.utc))
            remaining = max(
                1.0,
                (next_due - datetime.now(timezone.utc)).total_seconds(),
            )
            await asyncio.sleep(min(_DISCOVERY_IDLE_POLL_SECONDS, remaining))

    async def _shadow_loop(self) -> None:
        await asyncio.sleep(20)
        while not self._stopping:
            self._begin_lane_attempt("shadow")
            try:
                await asyncio.to_thread(self.run_shadow_once)
                await self._deliver_pending_live_alerts()
            except Exception as exc:
                self.metrics.failures += 1
                self.metrics.last_error = f"{type(exc).__name__}: {exc}"
                self.metrics.last_shadow_error = self.metrics.last_error
                phase = self.metrics.last_shadow_error_phase or "DELIVERY"
                self.metrics.last_shadow_error_phase = phase
                if "statement timeout" in str(exc).lower():
                    self._record_lane_timeout("shadow", phase)
                print(f"[formula-shadow] cycle failed open: {exc!r}", flush=True)
            await asyncio.sleep(_SHADOW_POLL_SECONDS)

    def run_discovery_horizon_once(
        self,
        horizon: int,
        *,
        schedule_slot_utc: Any,
        due_at_utc: Any,
    ) -> Dict[str, Any]:
        self._begin_lane_attempt("discovery")
        horizon = int(horizon)
        if horizon not in _horizons():
            raise ValueError("horizon is not enabled for Formula Discovery")
        slot = _as_utc(schedule_slot_utc)
        due_at = _as_utc(due_at_utc)
        if due_at != slot + timedelta(seconds=_DISCOVERY_SLOT_GRACE_SECONDS):
            raise ValueError("Discovery due time does not match the fixed slot grace")
        watermark: Optional[Dict[str, Any]] = None
        watermark_sha: Optional[str] = None
        with research_formula_store.discovery_horizon_lock(horizon) as acquired:
            if not acquired:
                self.metrics.discovery_locked_skips += 1
                return {
                    "horizon_minutes": horizon,
                    "schedule_slot_utc": slot.isoformat(),
                    "skipped": True,
                    "reason": "another worker holds the PostgreSQL horizon lock",
                    "status": "SKIPPED_LOCKED",
                }
            # This read and the bounded Stage-4 candidate search are an
            # observability-only bridge.  Neither result is passed to the
            # legacy candidate engine or either persistence/delivery path.
            # The existing cross-replica horizon lock only prevents concurrent
            # duplicate ingestion work.
            self._ingest_authoritative_stage4_corpus_once(
                horizon,
                schedule_slot_utc=slot,
                due_at_utc=due_at,
            )
            state = research_formula_store.load_discovery_schedule_state(horizon)
            if state and _as_utc(state["last_slot_utc"]) >= slot:
                return {
                    "horizon_minutes": horizon,
                    "schedule_slot_utc": slot.isoformat(),
                    "skipped": True,
                    "reason": "schedule slot already reached a terminal state",
                    "status": "SKIPPED_ALREADY_TERMINAL",
                }
            recovered = research_formula_store.load_scheduled_discovery_run(
                horizon, schedule_slot_utc=slot
            )
            if recovered:
                recovered_watermark = recovered.get("source_watermark")
                recovered_sha = recovered.get("source_watermark_sha256")
                research_formula_store.record_discovery_schedule_state(
                    horizon_minutes=horizon,
                    slot_utc=slot,
                    due_at_utc=due_at,
                    status="COMPLETED",
                    source_watermark=(
                        recovered_watermark
                        if isinstance(recovered_watermark, Mapping)
                        else None
                    ),
                    source_watermark_sha256=recovered_sha,
                    discovery_run_id=int(recovered["run_id"]),
                    reason="recovered committed run after scheduler-state gap",
                )
                return {
                    "horizon_minutes": horizon,
                    "schedule_slot_utc": slot.isoformat(),
                    "skipped": True,
                    "reason": "committed run recovered into scheduler state",
                    "run_id": int(recovered["run_id"]),
                    "status": "SKIPPED_RECOVERED_COMMITTED",
                }
            self.metrics.discovery_cycles += 1
            try:
                with self._phase("discovery", "LOAD_DATASET"):
                    dataset = research_feature_matrix.load_formula_dataset(
                        lookback_days=_LOOKBACK_DAYS,
                        horizon_minutes=horizon,
                        limit=_DATASET_LIMIT,
                        analysis_as_of_utc=due_at,
                    )
                watermark, watermark_sha = _dataset_watermark(
                    dataset, horizon_minutes=horizon
                )
                if (
                    not dataset.get("available")
                    or int(dataset.get("sample_size") or 0) < 2
                ):
                    reason = dataset.get("reason") or "insufficient verified outcomes"
                    research_formula_store.record_discovery_schedule_state(
                        horizon_minutes=horizon,
                        slot_utc=slot,
                        due_at_utc=due_at,
                        status="SKIPPED_UNAVAILABLE",
                        source_watermark=watermark,
                        source_watermark_sha256=watermark_sha,
                        reason=reason,
                    )
                    self.metrics.discovery_unavailable_skips += 1
                    return {
                        "horizon_minutes": horizon,
                        "schedule_slot_utc": slot.isoformat(),
                        "skipped": True,
                        "reason": reason,
                        "sample_size": int(dataset.get("sample_size") or 0),
                        "coverage": dataset.get("coverage") or {},
                        "source_watermark_sha256": watermark_sha,
                        "status": "SKIPPED_UNAVAILABLE",
                    }
                if (
                    state
                    and state.get("last_source_watermark_sha256")
                    == watermark_sha
                ):
                    research_formula_store.record_discovery_schedule_state(
                        horizon_minutes=horizon,
                        slot_utc=slot,
                        due_at_utc=due_at,
                        status="SKIPPED_UNCHANGED",
                        source_watermark=watermark,
                        source_watermark_sha256=watermark_sha,
                        reason="bounded dataset watermark did not advance",
                    )
                    self.metrics.discovery_unchanged_skips += 1
                    return {
                        "horizon_minutes": horizon,
                        "schedule_slot_utc": slot.isoformat(),
                        "skipped": True,
                        "reason": "bounded dataset watermark did not advance",
                        "source_watermark_sha256": watermark_sha,
                        "status": "SKIPPED_UNCHANGED",
                    }
                with self._phase("discovery", "DISCOVER_FORMULAS"):
                    discovery = research_formula_engine.discover_formulas(
                        dataset["rows"],
                        horizon_minutes=horizon,
                        feature_schema_version=dataset[
                            "feature_schema_version"
                        ],
                        config=_discovery_config(),
                        analysis_as_of_utc=due_at,
                    )
                if not discovery.get("available"):
                    reason = discovery.get("reason") or "Discovery unavailable"
                    research_formula_store.record_discovery_schedule_state(
                        horizon_minutes=horizon,
                        slot_utc=slot,
                        due_at_utc=due_at,
                        status="SKIPPED_UNAVAILABLE",
                        source_watermark=watermark,
                        source_watermark_sha256=watermark_sha,
                        reason=reason,
                    )
                    self.metrics.discovery_unavailable_skips += 1
                    return {
                        "horizon_minutes": horizon,
                        "schedule_slot_utc": slot.isoformat(),
                        "skipped": True,
                        "reason": reason,
                        "sample_size": discovery.get("sample_size"),
                        "source_watermark_sha256": watermark_sha,
                        "status": "SKIPPED_UNAVAILABLE",
                    }
                with self._phase("discovery", "PERSIST_DISCOVERY"):
                    persisted = research_formula_store.persist_discovery_run(
                        dataset=dataset,
                        discovery=discovery,
                        lookback_days=_LOOKBACK_DAYS,
                        scheduler_metadata={
                            "scheduler_version": (
                                research_formula_store.DISCOVERY_SCHEDULER_VERSION
                            ),
                            "schedule_slot_utc": slot,
                            "source_watermark": watermark,
                            "source_watermark_sha256": watermark_sha,
                            "walk_forward_policy_version": (
                                research_formula_engine.WALK_FORWARD_POLICY_VERSION
                            ),
                            "purge_policy_version": (
                                research_formula_engine.PURGE_POLICY_VERSION
                            ),
                            "embargo_policy_version": (
                                research_formula_engine.EMBARGO_POLICY_VERSION
                            ),
                        },
                    )
                research_formula_store.record_discovery_schedule_state(
                    horizon_minutes=horizon,
                    slot_utc=slot,
                    due_at_utc=due_at,
                    status="COMPLETED",
                    source_watermark=watermark,
                    source_watermark_sha256=watermark_sha,
                    discovery_run_id=int(persisted["run_id"]),
                    reason="new bounded dataset watermark evaluated",
                )
            except Exception as exc:
                try:
                    committed = research_formula_store.load_scheduled_discovery_run(
                        horizon, schedule_slot_utc=slot
                    )
                    if committed:
                        committed_watermark = committed.get("source_watermark")
                        research_formula_store.record_discovery_schedule_state(
                            horizon_minutes=horizon,
                            slot_utc=slot,
                            due_at_utc=due_at,
                            status="COMPLETED",
                            source_watermark=(
                                committed_watermark
                                if isinstance(committed_watermark, Mapping)
                                else watermark
                            ),
                            source_watermark_sha256=(
                                committed.get("source_watermark_sha256")
                                or watermark_sha
                            ),
                            discovery_run_id=int(committed["run_id"]),
                            reason="recovered committed run after post-commit error",
                        )
                        self._begin_lane_attempt("discovery")
                        return {
                            "horizon_minutes": horizon,
                            "schedule_slot_utc": slot.isoformat(),
                            "run_id": int(committed["run_id"]),
                            "status": "COMPLETED_RECOVERED_POST_COMMIT",
                        }
                    research_formula_store.record_discovery_schedule_state(
                        horizon_minutes=horizon,
                        slot_utc=slot,
                        due_at_utc=due_at,
                        status="FAILED",
                        source_watermark=watermark,
                        source_watermark_sha256=watermark_sha,
                        reason=f"{type(exc).__name__}: {exc}",
                    )
                except Exception:
                    pass
                raise

        self.metrics.discovery_runs += 1
        self.metrics.candidates_evaluated += int(
            discovery.get("candidates_evaluated") or 0
        )
        self.metrics.formulas_persisted += int(
            persisted.get("formulas_persisted") or 0
        )
        now = datetime.now(timezone.utc).isoformat()
        self.metrics.last_discovery_utc = now
        self.metrics.last_discovery_horizon_minutes = horizon
        self.metrics.last_discovery_slot_utc = slot.isoformat()
        self.metrics.last_error = None
        self.metrics.last_discovery_error = None
        self.metrics.last_discovery_error_phase = None
        result = {
            **persisted,
            "sample_size": discovery.get("sample_size"),
            "discovery_sample_size": discovery.get("discovery_sample_size"),
            "selection_sample_size": discovery.get("selection_sample_size"),
            "holdout_sample_size": discovery.get("holdout_sample_size"),
            "candidates_evaluated": discovery.get("candidates_evaluated"),
            "coverage": dataset.get("coverage") or {},
            "dataset_kind": (dataset.get("coverage") or {}).get("dataset_kind"),
            "schedule_slot_utc": slot.isoformat(),
            "analysis_as_of_utc": due_at.isoformat(),
            "source_watermark_sha256": watermark_sha,
            "status": "COMPLETED",
        }
        print(f"[formula-discovery] completed horizon: {result}", flush=True)
        return result

    def run_discovery_once(self) -> Dict[str, Any]:
        """Run the newest fixed slot for every enabled horizon, serially."""

        cycle_started = datetime.now(timezone.utc)
        results = []
        for horizon in _horizons():
            slot, due_at = _discovery_schedule(horizon, now=cycle_started)
            results.append(
                self.run_discovery_horizon_once(
                    horizon,
                    schedule_slot_utc=slot,
                    due_at_utc=due_at,
                )
            )
        now = datetime.now(timezone.utc).isoformat()
        print(f"[formula-discovery] scheduled sweep: {results}", flush=True)
        return {
            "completed_at_utc": now,
            "scheduler_version": research_formula_store.DISCOVERY_SCHEDULER_VERSION,
            "results": results,
        }

    def run_shadow_once(self) -> Dict[str, Any]:
        self._begin_lane_attempt("shadow")
        self.metrics.shadow_cycles += 1
        with self._phase("shadow", "LOAD_WORK"):
            work = research_formula_store.load_shadow_work()
        selected_by_formula = _select_shadow_work_prefixes(
            work, max_formula_events=250
        )
        event_ids = sorted(
            {
                event_id
                for selected in selected_by_formula.values()
                for event_id in selected
            }
        )
        event_ids_by_horizon: Dict[int, list[int]] = {}
        for formula in work:
            horizon = int(formula["horizon_minutes"])
            selected = selected_by_formula.get(int(formula["formula_id"]), [])
            if selected:
                event_ids_by_horizon.setdefault(horizon, []).extend(selected)
        with self._phase("shadow", "LOAD_FEATURES"):
            feature_rows = (
                research_feature_matrix.load_shadow_feature_rows_by_horizon(
                    event_ids_by_horizon
                )
            )
        checked = 0
        matched = 0
        queued = 0
        for formula in work:
            selected_event_ids = set(
                selected_by_formula.get(int(formula["formula_id"]), [])
            )
            conditions = _conditions(formula.get("conditions"))
            results = []
            for event in formula.get("events") or []:
                event_id = int(event["event_id"])
                if event_id not in selected_event_ids:
                    continue
                horizon = int(formula["horizon_minutes"])
                row = feature_rows.get((event_id, horizon))
                frozen_features = (
                    row.get("frozen_decision_features")
                    if isinstance(row, Mapping)
                    else None
                )
                evaluation = research_formula_engine.evaluate_frozen_feature_values(
                    frozen_features,
                    direction=formula["direction"],
                    event_direction=(
                        row.get("event", {}).get("direction")
                        if isinstance(row, Mapping)
                        and isinstance(row.get("event"), Mapping)
                        else event.get("direction")
                    ),
                    conditions=conditions,
                )
                snapshot = _shadow_snapshot(
                    formula=formula,
                    event=event,
                    row=row,
                    evaluation=evaluation,
                )
                provenance_compatible, provenance_reason = (
                    research_formula_store._max_pain_snapshot_contract(
                        formula,
                        snapshot,
                        decision_time_utc=event.get("alert_time_utc"),
                        symbol=event.get("symbol"),
                    )
                )
                if not provenance_compatible:
                    evaluation = {
                        **dict(evaluation),
                        "status": "UNEVALUABLE",
                        "matched": False,
                        "reason": (
                            "Max-Pain provenance rejected: "
                            + provenance_reason
                        )[:1000],
                    }
                    snapshot = _shadow_snapshot(
                        formula=formula,
                        event=event,
                        row=row,
                        evaluation=evaluation,
                    )
                cohort_key, cohort_anchor = _decision_cohort(
                    formula=formula,
                    event=event,
                    snapshot=snapshot,
                )
                results.append(
                    {
                        "event_id": event_id,
                        "alert_time_utc": event.get("alert_time_utc"),
                        "matched": bool(evaluation.get("matched")),
                        "evaluation_status": evaluation.get("status"),
                        "evaluation_reason": evaluation.get("reason"),
                        "condition_results": evaluation.get("condition_results") or [],
                        "input_snapshot": snapshot,
                        "decision_cohort_key": cohort_key,
                        "decision_anchor_time_utc": cohort_anchor,
                    }
                )
            with self._phase("shadow", "PERSIST_CHECKS"):
                persisted = research_formula_store.record_shadow_results(
                    formula=formula,
                    results=results,
                )
            checked += persisted["checked"]
            matched += persisted["matched"]
            queued += int(persisted.get("queued") or 0)
        with self._phase("shadow", "EVALUATE_READINESS"):
            validation = research_formula_store.evaluate_shadow_readiness()
        research_ready = len(validation.get("research_ready") or [])
        legacy_live_review_ready = len(
            validation.get("legacy_live_review_ready") or []
        )
        relevance_states = validation.get("relevance_state_counts") or {}
        self.metrics.shadow_checks += checked
        self.metrics.shadow_hits += matched
        self.metrics.live_candidates_queued += queued
        self.metrics.formulas_ready_for_review = research_ready
        self.metrics.research_ready_formulas = research_ready
        self.metrics.experimentally_relevant_formulas = len(
            validation.get("experimentally_relevant") or []
        )
        self.metrics.suspended_relevance_formulas = int(
            relevance_states.get(research_formula_relevance.SUSPENDED) or 0
        )
        self.metrics.recovering_relevance_formulas = int(
            relevance_states.get(research_formula_relevance.RECOVERING) or 0
        )
        self.metrics.legacy_live_review_ready_formulas = (
            legacy_live_review_ready
        )
        now = datetime.now(timezone.utc).isoformat()
        self.metrics.last_shadow_utc = now
        self.metrics.last_error = None
        self.metrics.last_shadow_error = None
        self.metrics.last_shadow_error_phase = None
        if checked or matched:
            print(
                f"[formula-shadow] checked={checked}; matched={matched}; "
                f"queued={queued}; research_ready={research_ready}; "
                "experimentally_relevant="
                f"{self.metrics.experimentally_relevant_formulas}; "
                f"legacy_live_review_ready={legacy_live_review_ready}; "
                "promoted_live=0",
                flush=True,
            )
        return {
            "completed_at_utc": now,
            "active_formulas": len(work),
            "events_loaded": len(event_ids),
            "formula_event_checks_loaded": sum(
                len(selected) for selected in selected_by_formula.values()
            ),
            "checked": checked,
            "matched": matched,
            "queued_live_deliveries": queued,
            "validation": validation,
            "automatic_promotions": 0,
            "delivery": (
                "ENABLED_FOR_SUBSCRIBED_CHATS"
                if _LIVE_ALERTS_ENABLED
                else "DISABLED_BY_ENVIRONMENT"
            ),
        }

    @staticmethod
    def _as_mapping(value: Any) -> Mapping[str, Any]:
        if isinstance(value, Mapping):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                return {}
            return parsed if isinstance(parsed, Mapping) else {}
        return {}

    @classmethod
    def _live_alert_text(cls, delivery: Mapping[str, Any]) -> str:
        validation = cls._as_mapping(delivery.get("shadow_validation_metrics"))
        metrics = cls._as_mapping(validation.get("metrics"))
        holdout = cls._as_mapping(delivery.get("holdout_metrics"))

        def number(source: Mapping[str, Any], key: str, digits: int = 2) -> str:
            try:
                return f"{float(source.get(key)):.{digits}f}"
            except (TypeError, ValueError):
                return "-"

        horizon = int(delivery.get("horizon_minutes") or 0)
        horizon_label = {60: "1h", 240: "4h", 720: "12h", 1440: "24h"}.get(
            horizon, f"{horizon}m"
        )
        direction = str(delivery.get("direction") or "-").upper()
        direction_icon = "🟢" if direction == "LONG" else "🔴"
        target = delivery.get("target_price")
        target_text = f"{float(target):,.6g}" if target not in (None, "") else "לא הוגדר"
        current = delivery.get("current_price")
        current_text = f"{float(current):,.6g}" if current not in (None, "") else "-"
        rarity = str(holdout.get("rarity_class") or "-")
        movement_percentile_key = (
            "session_adjusted_mfe_percentile_pct"
            if metrics.get("session_adjusted_mfe_percentile_pct") is not None
            else "median_mfe_percentile_pct"
        )
        efficiency = research_mfe_mae_efficiency.from_metrics(metrics)
        if efficiency.state == research_mfe_mae_efficiency.UNBOUNDED_ZERO_MAE:
            efficiency_text = "בלתי־חסום (MAE חציוני 0)"
        elif (
            efficiency.state == research_mfe_mae_efficiency.FINITE
            and efficiency.ratio is not None
        ):
            efficiency_text = f"{efficiency.ratio:.2f}"
        elif (
            efficiency.state
            == research_mfe_mae_efficiency.UNDEFINED_ZERO_ZERO
        ):
            efficiency_text = "לא מוגדר (MFE ו־MAE חציוניים 0)"
        else:
            efficiency_text = "-"
        return (
            "🧠 התראת טרייד AI — נוסחה מאומתת\n"
            f"{direction_icon} {delivery.get('symbol')} {direction} | אופק {horizon_label}\n"
            f"אירוע #{delivery.get('event_id')} | {delivery.get('event_type')}\n"
            f"מחיר בעת ההתראה: {current_text} | יעד הבוט: {target_text}\n\n"
            f"נוסחה #{delivery.get('formula_id')} v{delivery.get('formula_version')}\n"
            f"{delivery.get('formula_text')}\n\n"
            "אימות עתידי ב-Shadow:\n"
            f"דגימות: {int(metrics.get('sample_size') or 0)} | נדירות Holdout: {rarity}\n"
            f"שיעור כיוון נכון: {number(metrics, 'hit_rate_pct')}% "
            f"(Wilson תחתון {number(metrics, 'wilson_95_lower_pct')}%)\n"
            f"מהלך חיובי חציוני MFE: {number(metrics, 'median_mfe_pct', 3)}% | "
            f"תנועה נגדית p90 MAE: {number(metrics, 'mae_p90_pct', 3)}%\n"
            f"אחוזון רוחב מהלך מותאם Session: {number(metrics, movement_percentile_key)} | "
            f"MFE/MAE: {efficiency_text}\n\n"
            "התראה מחקרית אוטונומית בלבד — הבוט לא ביצע עסקה."
        )

    async def _deliver_pending_live_alerts(self) -> Dict[str, int]:
        if not _LIVE_ALERTS_ENABLED or self._telegram_bot is None:
            return {"sent": 0, "failed": 0}
        pending = await asyncio.to_thread(
            research_formula_store.load_pending_live_deliveries
        )
        sent = 0
        failed = 0
        for delivery in pending:
            try:
                await self._telegram_bot.send_message(
                    chat_id=int(delivery["chat_id"]),
                    text=self._live_alert_text(delivery),
                )
            except Exception as exc:
                failed += 1
                await asyncio.to_thread(
                    research_formula_store.mark_live_delivery,
                    int(delivery["delivery_id"]),
                    sent=False,
                    error=f"{type(exc).__name__}: {exc}",
                )
            else:
                sent += 1
                await asyncio.to_thread(
                    research_formula_store.mark_live_delivery,
                    int(delivery["delivery_id"]),
                    sent=True,
                )
        self.metrics.live_deliveries_sent += sent
        self.metrics.live_deliveries_failed += failed
        return {"sent": sent, "failed": failed}


WORKER = FormulaResearchWorker()
