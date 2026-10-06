"""Versioned, side-effect-free relevance hysteresis for Formula Research.

Research acceptance, current relevance and delivery authority are deliberately
separate axes.  This module consumes an already-frozen FormulaAssessment and
advances only the relevance state.  It cannot change a formula stage, approve
LIVE, or send a message.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import re
from typing import Any, Dict, Mapping

import research_evidence_contract


POLICY_VERSION = "formula-relevance-hysteresis-v2-runtime-bound"
WEAK_OBSERVATIONS_TO_SUSPEND = 2
STRONG_EVIDENCE_ADVANCES_TO_REACTIVATE = 2

OBSERVING = "OBSERVING"
RELEVANT = "RELEVANT"
WEAKENING = "WEAKENING"
SUSPENDED = "SUSPENDED"
RECOVERING = "RECOVERING"
LEGACY_READ_ONLY = "LEGACY_READ_ONLY"

STRONG = "STRONG"
EARLY = "EARLY"
INSUFFICIENT = "INSUFFICIENT"
WEAK = "WEAK"
STALE = "STALE"
LEGACY = "LEGACY"

_STATES = frozenset(
    {OBSERVING, RELEVANT, WEAKENING, SUSPENDED, RECOVERING, LEGACY_READ_ONLY}
)
_OBSERVATIONS = frozenset({STRONG, EARLY, INSUFFICIENT, WEAK, STALE, LEGACY})
_HEX_64 = re.compile(r"^[0-9a-f]{64}$")


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _hex(value: Any, *, name: str) -> str:
    normalized = str(value or "").strip().lower()
    if not _HEX_64.fullmatch(normalized):
        raise ValueError(f"{name} must be a lowercase 64-character SHA-256 id")
    return normalized


def classify_observation(
    assessment: research_evidence_contract.FormulaAssessment | Mapping[str, Any],
    *,
    compatibility: str,
) -> str:
    """Map one frozen acceptance interpretation to a relevance observation."""

    resolved = (
        assessment
        if isinstance(assessment, research_evidence_contract.FormulaAssessment)
        else research_evidence_contract.FormulaAssessment.from_dict(assessment)
    )
    normalized_compatibility = str(compatibility or "").strip().upper()
    if normalized_compatibility == research_evidence_contract.LEGACY_SHADOW_READ_ONLY:
        return LEGACY
    if normalized_compatibility != research_evidence_contract.CURRENT_V7:
        raise ValueError("unsupported relevance compatibility state")
    if resolved.research_ready:
        return STRONG
    return {
        "EARLY_CURRENT_EDGE": EARLY,
        "ACCUMULATING_EVIDENCE": INSUFFICIENT,
        "EVIDENCE_PRESENT_EDGE_NOT_ESTABLISHED": WEAK,
        "STALE_OR_NOT_RECENT": STALE,
    }[resolved.maturity]


def observation_fingerprint(
    *,
    formula_contract: Mapping[str, Any],
    assessment: research_evidence_contract.FormulaAssessment | Mapping[str, Any],
    evidence_fingerprint: Any,
    observed_at_utc: Any,
) -> str:
    """Return an idempotency key for one bounded rolling relevance look.

    A UTC-day bucket permits time-decay to create at most one new decision per
    day when evidence did not advance.  The evidence fingerprint and frozen
    assessment id still create a new observation immediately when an
    independent Market Episode changes the evidence.  This never changes N_eff.
    """

    resolved = (
        assessment
        if isinstance(assessment, research_evidence_contract.FormulaAssessment)
        else research_evidence_contract.FormulaAssessment.from_dict(assessment)
    )
    formula_key = _hex(formula_contract.get("formula_key"), name="formula_key")
    evidence_id = _hex(evidence_fingerprint, name="evidence_fingerprint")
    try:
        formula_version = int(formula_contract.get("formula_version"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("formula_version must be a positive integer") from exc
    if formula_version <= 0 or isinstance(formula_contract.get("formula_version"), bool):
        raise ValueError("formula_version must be a positive integer")
    payload = {
        "policy_version": POLICY_VERSION,
        "formula_key": formula_key,
        "formula_version": formula_version,
        "formula_schema_version": str(
            formula_contract.get("formula_schema_version") or ""
        ),
        "assessment_id": resolved.assessment_id,
        "evidence_fingerprint": evidence_id,
    }
    runtime_compatibility = (
        research_evidence_contract.runtime_compatibility_for_formula_contract(
            formula_contract
        )
    )
    payload["observation_epoch"] = (
        _utc(observed_at_utc).date().isoformat()
        if runtime_compatibility == research_evidence_contract.CURRENT_V7
        else "LEGACY_EVIDENCE_ONLY"
    )
    return hashlib.sha256(
        research_evidence_contract.canonical_json(payload).encode("utf-8")
    ).hexdigest()


def advance(
    *,
    previous: Mapping[str, Any] | None,
    formula_contract: Mapping[str, Any],
    compatibility: str,
    assessment: research_evidence_contract.FormulaAssessment | Mapping[str, Any],
    evidence_fingerprint: Any,
    observed_at_utc: Any,
    snapshot_id: Any,
) -> Dict[str, Any]:
    """Advance one formula's relevance state under the frozen v1 policy."""

    resolved = (
        assessment
        if isinstance(assessment, research_evidence_contract.FormulaAssessment)
        else research_evidence_contract.FormulaAssessment.from_dict(assessment)
    )
    evidence_id = _hex(evidence_fingerprint, name="evidence_fingerprint")
    resolved_snapshot_id = _hex(snapshot_id, name="snapshot_id")
    fingerprint = observation_fingerprint(
        formula_contract=formula_contract,
        assessment=resolved,
        evidence_fingerprint=evidence_id,
        observed_at_utc=observed_at_utc,
    )
    declared_compatibility = str(compatibility or "").strip().upper()
    expected_payload_compatibility = (
        research_evidence_contract.compatibility_for_formula_schema(
            formula_contract.get("formula_schema_version")
        )
    )
    if declared_compatibility != expected_payload_compatibility:
        raise ValueError("relevance compatibility does not match formula schema")
    runtime_compatibility = (
        research_evidence_contract.runtime_compatibility_for_formula_contract(
            formula_contract
        )
    )
    relevance_compatibility = (
        research_evidence_contract.CURRENT_V7
        if runtime_compatibility == research_evidence_contract.CURRENT_V7
        else research_evidence_contract.LEGACY_SHADOW_READ_ONLY
    )
    observation = classify_observation(
        resolved, compatibility=relevance_compatibility
    )
    formula_version = int(formula_contract["formula_version"])

    usable_previous: Mapping[str, Any] | None = previous
    if usable_previous is not None and (
        str(usable_previous.get("policy_version") or "") != POLICY_VERSION
        or int(usable_previous.get("formula_version") or 0) != formula_version
    ):
        usable_previous = None
    if usable_previous is not None:
        previous_state = str(usable_previous.get("state") or "").upper()
        if previous_state not in _STATES:
            raise ValueError("previous relevance state is invalid")
        previous_observation_fingerprint = _hex(
            usable_previous.get("observation_fingerprint"),
            name="previous observation_fingerprint",
        )
        if previous_observation_fingerprint == fingerprint:
            duplicate = dict(usable_previous)
            duplicate["duplicate_observation"] = True
            return duplicate
    else:
        previous_state = None

    evidence_advanced = bool(
        usable_previous is None
        or str(usable_previous.get("evidence_fingerprint") or "") != evidence_id
    )
    weak_streak = int((usable_previous or {}).get("weak_observation_streak") or 0)
    recovery_streak = int((usable_previous or {}).get("recovery_evidence_streak") or 0)
    transition = "NONE"

    if observation == LEGACY:
        state = LEGACY_READ_ONLY
        weak_streak = 0
        recovery_streak = 0
        transition = "INITIALIZED" if previous_state is None else "NONE"
    elif previous_state is None or previous_state == LEGACY_READ_ONLY:
        state = RELEVANT if observation == STRONG else OBSERVING
        weak_streak = 0
        recovery_streak = 0
        transition = "BECAME_RELEVANT" if state == RELEVANT else "INITIALIZED"
    elif previous_state == OBSERVING:
        state = RELEVANT if observation == STRONG else OBSERVING
        weak_streak = 0
        recovery_streak = 0
        transition = "BECAME_RELEVANT" if state == RELEVANT else "NONE"
    elif previous_state == RELEVANT:
        if observation == STRONG:
            state = RELEVANT
            weak_streak = 0
            recovery_streak = 0
        else:
            state = WEAKENING
            weak_streak = 1
            recovery_streak = 0
            transition = "WEAKENING_STARTED"
    elif previous_state == WEAKENING:
        if observation == STRONG:
            state = RELEVANT
            weak_streak = 0
            recovery_streak = 0
            transition = "WEAKNESS_CLEARED"
        else:
            weak_streak += 1
            recovery_streak = 0
            if weak_streak >= WEAK_OBSERVATIONS_TO_SUSPEND:
                state = SUSPENDED
                transition = "SUSPENDED"
            else:
                state = WEAKENING
    elif previous_state == SUSPENDED:
        weak_streak = max(weak_streak, WEAK_OBSERVATIONS_TO_SUSPEND)
        if observation == STRONG and evidence_advanced:
            state = RECOVERING
            recovery_streak = 1
            transition = "RECOVERY_STARTED"
        else:
            state = SUSPENDED
            recovery_streak = 0
    elif previous_state == RECOVERING:
        weak_streak = max(weak_streak, WEAK_OBSERVATIONS_TO_SUSPEND)
        if observation == STRONG:
            if evidence_advanced:
                recovery_streak += 1
            if recovery_streak >= STRONG_EVIDENCE_ADVANCES_TO_REACTIVATE:
                state = RELEVANT
                weak_streak = 0
                recovery_streak = 0
                transition = "REACTIVATED"
            else:
                state = RECOVERING
        else:
            state = SUSPENDED
            recovery_streak = 0
            transition = "RECOVERY_FAILED"
    else:  # pragma: no cover - guarded by the state validation above
        raise ValueError("unsupported previous relevance state")

    experimental_eligible = bool(
        relevance_compatibility == research_evidence_contract.CURRENT_V7
        and state in {RELEVANT, WEAKENING}
    )
    reason = {
        LEGACY_READ_ONLY: "retained legacy formula remains observable and cannot become current-v7 relevant",
        OBSERVING: "current evidence has not established relevance",
        RELEVANT: "current evidence is relevant under the frozen acceptance contract",
        WEAKENING: "one distinct weak rolling observation; suspension requires confirmation",
        SUSPENDED: "two distinct weak rolling observations; new experimental indications are blocked",
        RECOVERING: "one new strong evidence version; another new strong version is required",
    }[state]
    return {
        "policy_version": POLICY_VERSION,
        "formula_version": formula_version,
        "observation_fingerprint": fingerprint,
        "evidence_fingerprint": evidence_id,
        "snapshot_id": resolved_snapshot_id,
        "observed_at_utc": _utc(observed_at_utc).isoformat(),
        "observation_utc_date": _utc(observed_at_utc).date().isoformat(),
        "compatibility": relevance_compatibility,
        "runtime_compatibility": runtime_compatibility,
        "observation": observation,
        "research_maturity": resolved.maturity,
        "research_ready": resolved.research_ready,
        "accepted_paths": list(resolved.accepted_paths),
        "previous_state": previous_state,
        "state": state,
        "transition": transition,
        "weak_observation_streak": weak_streak,
        "recovery_evidence_streak": recovery_streak,
        "evidence_advanced": evidence_advanced,
        "experimental_relevance_eligible": experimental_eligible,
        "decision_reason": reason,
        "hysteresis": {
            "weak_observations_to_suspend": WEAK_OBSERVATIONS_TO_SUSPEND,
            "new_strong_evidence_versions_to_reactivate": (
                STRONG_EVIDENCE_ADVANCES_TO_REACTIVATE
            ),
            "identical_polling_advances_state": False,
            "same_market_episode_adds_evidence": False,
        },
        "live_effect": "NONE",
        "delivery_channel": "NONE",
        "duplicate_observation": False,
    }


def descriptor() -> Dict[str, Any]:
    return {
        "policy_version": POLICY_VERSION,
        "states": sorted(_STATES),
        "weak_observations_to_suspend": WEAK_OBSERVATIONS_TO_SUSPEND,
        "new_strong_evidence_versions_to_reactivate": (
            STRONG_EVIDENCE_ADVANCES_TO_REACTIVATE
        ),
        "identical_polling_advances_state": False,
        "same_market_episode_adds_evidence": False,
        "live_effect": "NONE",
        "delivery_channel": "NONE",
    }
