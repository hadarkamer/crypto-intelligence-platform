"""Versioned, deterministic evidence snapshots for formula research consumers.

This module is deliberately side-effect free.  It gives Discovery, Shadow and
future experimental Telegram rendering one canonical envelope without changing
which formulas are accepted.  Existing v5/v6.2 formulas remain readable through
an explicit legacy compatibility marker; only the current v7 contract can be
marked as current.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import re
from typing import Any, Dict, Mapping, Sequence


CONTRACT_VERSION = "formula-evidence-contract-v1"
FORMULA_ASSESSMENT_SCHEMA_VERSION = "formula-assessment-v1"
EVIDENCE_SNAPSHOT_SCHEMA_VERSION = "evidence-snapshot-v1"
LEGACY_ADAPTER_VERSION = "formula-evidence-legacy-read-adapter-v1"

CURRENT_FORMULA_SCHEMA_VERSION = "research-formula-v7-adaptive-evidence"
LEGACY_V5_FORMULA_SCHEMA_VERSION = "research-formula-v5-safe-replay"
LEGACY_V6_FORMULA_SCHEMA_VERSION = "research-formula-v6-first-touch-maxpain"
CURRENT_ENGINE_VERSION = "formula-discovery-v7-adaptive-evidence-market-episodes"
LEGACY_V6_ENGINE_VERSION = (
    "formula-discovery-v6.2-first-touch-maxpain-hierarchical-holdout-isolated"
)
LEGACY_V5_ENGINE_VERSION = "formula-discovery-v5-safe-replay"
CURRENT_FEATURE_SCHEMA_VERSION = (
    "research-feature-matrix-v8-prospective-max-pain-frozen"
)
LEGACY_V5_FEATURE_SCHEMA_VERSION = "research-feature-matrix-v4-safe-replay"
CURRENT_OUTCOME_METHOD_VERSION = "no-dwell-first-touch-v6"
LEGACY_V5_OUTCOME_METHOD_VERSION = "canonical-spot-1m-ohlc-path-v3"
SUPPORTED_FORMULA_SCHEMA_VERSIONS = frozenset(
    {
        LEGACY_V5_FORMULA_SCHEMA_VERSION,
        LEGACY_V6_FORMULA_SCHEMA_VERSION,
        CURRENT_FORMULA_SCHEMA_VERSION,
    }
)
SUPPORTED_RUNTIME_CONTRACTS = {
    CURRENT_FORMULA_SCHEMA_VERSION: (
        CURRENT_ENGINE_VERSION,
        CURRENT_FEATURE_SCHEMA_VERSION,
        CURRENT_OUTCOME_METHOD_VERSION,
    ),
    LEGACY_V6_FORMULA_SCHEMA_VERSION: (
        LEGACY_V6_ENGINE_VERSION,
        CURRENT_FEATURE_SCHEMA_VERSION,
        CURRENT_OUTCOME_METHOD_VERSION,
    ),
    LEGACY_V5_FORMULA_SCHEMA_VERSION: (
        LEGACY_V5_ENGINE_VERSION,
        LEGACY_V5_FEATURE_SCHEMA_VERSION,
        LEGACY_V5_OUTCOME_METHOD_VERSION,
    ),
}

CURRENT_V7 = "CURRENT_V7"
LEGACY_SHADOW_READ_ONLY = "LEGACY_SHADOW_READ_ONLY"
_COMPATIBILITY_STATES = frozenset({CURRENT_V7, LEGACY_SHADOW_READ_ONLY})
_PHASES = frozenset({"HISTORICAL", "PROSPECTIVE"})
_DIRECTIONS = frozenset({"LONG", "SHORT"})
_PATH_ORDER = ("PROBABILITY", "ASYMMETRY")
_MATURITY_STATES = frozenset(
    {
        "RESEARCH_READY",
        "EARLY_CURRENT_EDGE",
        "ACCUMULATING_EVIDENCE",
        "STALE_OR_NOT_RECENT",
        "EVIDENCE_PRESENT_EDGE_NOT_ESTABLISHED",
    }
)
_HEX_64 = re.compile(r"^[0-9a-f]{64}$")


def _utc_iso(value: Any) -> str:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    normalized = parsed.astimezone(timezone.utc)
    return normalized.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _canonical_value(value: Any, *, path: str = "root") -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} contains a non-finite number")
        return 0.0 if value == 0.0 else value
    if isinstance(value, datetime):
        return _utc_iso(value)
    if isinstance(value, Mapping):
        normalized: Dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise ValueError(f"{path} contains a non-string or empty key")
            normalized[key] = _canonical_value(item, path=f"{path}.{key}")
        return {key: normalized[key] for key in sorted(normalized)}
    if isinstance(value, (set, frozenset)):
        normalized = [_canonical_value(item, path=f"{path}[]") for item in value]
        return sorted(
            normalized,
            key=lambda item: json.dumps(
                item, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ),
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [
            _canonical_value(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    raise ValueError(f"{path} contains unsupported type {type(value).__name__}")


def canonical_json(value: Any) -> str:
    """Return the only JSON representation used for contract fingerprints."""

    return json.dumps(
        _canonical_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _fingerprint(kind: str, payload: Mapping[str, Any]) -> str:
    bound = {
        "contract_version": CONTRACT_VERSION,
        "kind": str(kind),
        "payload": payload,
    }
    return hashlib.sha256(canonical_json(bound).encode("utf-8")).hexdigest()


def _hex_identifier(value: Any, *, name: str) -> str:
    normalized = str(value or "").strip().lower()
    if not _HEX_64.fullmatch(normalized):
        raise ValueError(f"{name} must be a lowercase 64-character SHA-256 id")
    return normalized


def _non_negative_number(value: Any, *, name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a finite non-negative number")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be a finite non-negative number") from exc
    if not math.isfinite(number) or number < 0.0:
        raise ValueError(f"{name} must be a finite non-negative number")
    return number


def _non_negative_integer(value: Any, *, name: str) -> int:
    number = _non_negative_number(value, name=name)
    if not number.is_integer():
        raise ValueError(f"{name} must be an integer")
    return int(number)


def _ordered_paths(values: Any, *, name: str) -> list[str]:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        raise ValueError(f"{name} must be an array")
    normalized = {str(value or "").strip().upper() for value in values}
    unknown = normalized - set(_PATH_ORDER)
    if unknown:
        raise ValueError(f"{name} contains unsupported paths: {sorted(unknown)}")
    return [path for path in _PATH_ORDER if path in normalized]


def compatibility_for_formula_schema(formula_schema_version: Any) -> str:
    version = str(formula_schema_version or "").strip()
    if version not in SUPPORTED_FORMULA_SCHEMA_VERSIONS:
        raise ValueError(f"unsupported formula schema version: {version!r}")
    return CURRENT_V7 if version == CURRENT_FORMULA_SCHEMA_VERSION else LEGACY_SHADOW_READ_ONLY


def derived_legacy_formula_family_id(formula_contract: Mapping[str, Any]) -> str:
    """Give retained v5/v6.2 rows a stable namespace without rewriting them."""

    payload = {
        "adapter_version": LEGACY_ADAPTER_VERSION,
        "formula_key": _hex_identifier(
            formula_contract.get("formula_key"), name="formula_key"
        ),
        "formula_version": _non_negative_integer(
            formula_contract.get("formula_version"), name="formula_version"
        ),
        "formula_schema_version": str(
            formula_contract.get("formula_schema_version") or ""
        ),
        "engine_version": str(formula_contract.get("engine_version") or ""),
        "feature_schema_version": str(
            formula_contract.get("feature_schema_version") or ""
        ),
        "outcome_method_version": str(
            formula_contract.get("outcome_method_version") or ""
        ),
        "direction": str(formula_contract.get("direction") or "").upper(),
        "horizon_minutes": _non_negative_integer(
            formula_contract.get("horizon_minutes"), name="horizon_minutes"
        ),
    }
    if payload["formula_version"] <= 0 or payload["horizon_minutes"] <= 0:
        raise ValueError("legacy formula version and horizon must be positive")
    compatibility_for_formula_schema(payload["formula_schema_version"])
    return _fingerprint("legacy-formula-family", payload)


@dataclass(frozen=True)
class FormulaAssessment:
    """Immutable interpretation of one already-computed acceptance result."""

    assessment_id: str
    _payload_json: str

    @classmethod
    def from_acceptance(
        cls, value: Mapping[str, Any], *, phase: str
    ) -> "FormulaAssessment":
        if not isinstance(value, Mapping):
            raise ValueError("acceptance result must be an object")
        normalized_phase = str(phase or "").strip().upper()
        if normalized_phase not in _PHASES:
            raise ValueError("assessment phase must be HISTORICAL or PROSPECTIVE")
        payload = dict(_canonical_value(value, path="assessment"))
        policy_version = str(payload.get("policy_version") or "").strip()
        if not policy_version:
            raise ValueError("assessment policy_version is required")
        maturity = str(payload.get("maturity") or "").strip().upper()
        if maturity not in _MATURITY_STATES:
            raise ValueError(f"unsupported maturity state: {maturity!r}")
        if type(payload.get("research_ready")) is not bool:
            raise ValueError("assessment research_ready must be a boolean")
        accepted_paths = _ordered_paths(
            payload.get("accepted_paths") or [], name="accepted_paths"
        )
        early_paths = _ordered_paths(
            payload.get("early_current_paths") or [], name="early_current_paths"
        )
        if bool(payload["research_ready"]) != bool(accepted_paths):
            raise ValueError("research_ready must match whether an acceptance path passed")
        missing = payload.get("missing_by_path")
        if not isinstance(missing, Mapping):
            raise ValueError("assessment missing_by_path must be an object")
        live_effect = str(payload.get("live_effect") or "").strip()
        if not live_effect.startswith("NONE"):
            raise ValueError("FormulaAssessment may not authorize or imply LIVE")
        payload.update(
            {
                "assessment_schema_version": FORMULA_ASSESSMENT_SCHEMA_VERSION,
                "contract_version": CONTRACT_VERSION,
                "phase": normalized_phase,
                "maturity": maturity,
                "accepted_paths": accepted_paths,
                "early_current_paths": early_paths,
            }
        )
        payload_json = canonical_json(payload)
        identifier = _fingerprint("formula-assessment", json.loads(payload_json))
        return cls(assessment_id=identifier, _payload_json=payload_json)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "FormulaAssessment":
        if not isinstance(value, Mapping):
            raise ValueError("FormulaAssessment payload must be an object")
        declared = value.get("assessment_id")
        body = {key: item for key, item in value.items() if key != "assessment_id"}
        if body.get("assessment_schema_version") != FORMULA_ASSESSMENT_SCHEMA_VERSION:
            raise ValueError("unsupported FormulaAssessment schema version")
        if body.get("contract_version") != CONTRACT_VERSION:
            raise ValueError("FormulaAssessment contract version mismatch")
        assessment = cls.from_acceptance(body, phase=str(body.get("phase") or ""))
        if declared is not None and _hex_identifier(
            declared, name="assessment_id"
        ) != assessment.assessment_id:
            raise ValueError("FormulaAssessment fingerprint mismatch")
        return assessment

    def to_dict(self) -> Dict[str, Any]:
        body = json.loads(self._payload_json)
        return {"assessment_id": self.assessment_id, **body}

    @property
    def research_ready(self) -> bool:
        return bool(json.loads(self._payload_json)["research_ready"])

    @property
    def maturity(self) -> str:
        return str(json.loads(self._payload_json)["maturity"])

    @property
    def accepted_paths(self) -> tuple[str, ...]:
        return tuple(json.loads(self._payload_json)["accepted_paths"])


def _identifier_array(values: Any, *, name: str) -> list[str]:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        raise ValueError(f"{name} must be an array")
    return sorted({_hex_identifier(value, name=name) for value in values})


def _validate_formula_contract(value: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("formula_contract must be an object")
    formula = dict(_canonical_value(value, path="formula_contract"))
    required_text = (
        "formula_schema_version",
        "engine_version",
        "feature_schema_version",
        "outcome_method_version",
    )
    formula["formula_key"] = _hex_identifier(
        formula.get("formula_key"), name="formula_key"
    )
    formula["formula_version"] = _non_negative_integer(
        formula.get("formula_version"), name="formula_version"
    )
    formula["horizon_minutes"] = _non_negative_integer(
        formula.get("horizon_minutes"), name="horizon_minutes"
    )
    formula["direction"] = str(formula.get("direction") or "").strip().upper()
    for key in required_text:
        formula[key] = str(formula.get(key) or "").strip()
        if not formula[key]:
            raise ValueError(f"formula_contract.{key} is required")
    if formula["formula_version"] <= 0 or formula["horizon_minutes"] <= 0:
        raise ValueError("formula version and horizon must be positive")
    if formula["direction"] not in _DIRECTIONS:
        raise ValueError("formula direction must be LONG or SHORT")
    compatibility_for_formula_schema(formula["formula_schema_version"])
    runtime = (
        formula["engine_version"],
        formula["feature_schema_version"],
        formula["outcome_method_version"],
    )
    if runtime != SUPPORTED_RUNTIME_CONTRACTS[formula["formula_schema_version"]]:
        raise ValueError(
            "formula runtime versions do not match the declared formula schema"
        )
    return formula


@dataclass(frozen=True)
class EvidenceSnapshot:
    """Content-addressed evidence envelope consumed without recomputation."""

    snapshot_id: str
    _payload_json: str

    @classmethod
    def build(
        cls,
        *,
        formula_contract: Mapping[str, Any],
        assessment: FormulaAssessment,
        assessed_at_utc: Any,
        formula_family_id: Any = None,
        matched_market_episode_ids: Sequence[Any] = (),
        control_market_episode_ids: Sequence[Any] = (),
        matched_parent_market_episode_ids: Sequence[Any] = (),
        control_parent_market_episode_ids: Sequence[Any] = (),
        raw_match_count: int = 0,
        raw_control_count: int = 0,
        matched_n_eff: float = 0.0,
        control_n_eff: float = 0.0,
        metrics: Mapping[str, Any] | None = None,
        evidence: Mapping[str, Any] | None = None,
        provenance: Mapping[str, Any] | None = None,
    ) -> "EvidenceSnapshot":
        if not isinstance(assessment, FormulaAssessment):
            raise ValueError("assessment must be a FormulaAssessment")
        formula = _validate_formula_contract(formula_contract)
        compatibility = compatibility_for_formula_schema(
            formula["formula_schema_version"]
        )
        if compatibility == LEGACY_SHADOW_READ_ONLY and assessment.research_ready:
            raise ValueError(
                "retained v5/v6.2 snapshots must remain research-not-ready "
                "under the v7 evidence contract"
            )
        if formula_family_id is None:
            if compatibility == CURRENT_V7:
                raise ValueError("current v7 snapshots require formula_family_id")
            family_id = derived_legacy_formula_family_id(formula)
        else:
            family_id = _hex_identifier(
                formula_family_id, name="formula_family_id"
            )
        assessment_payload = assessment.to_dict()
        phase = str(assessment_payload["phase"])
        matched_ids = _identifier_array(
            matched_market_episode_ids, name="matched_market_episode_ids"
        )
        control_ids = _identifier_array(
            control_market_episode_ids, name="control_market_episode_ids"
        )
        matched_parent_ids = _identifier_array(
            matched_parent_market_episode_ids,
            name="matched_parent_market_episode_ids",
        )
        control_parent_ids = _identifier_array(
            control_parent_market_episode_ids,
            name="control_parent_market_episode_ids",
        )
        raw_matches = _non_negative_integer(raw_match_count, name="raw_match_count")
        raw_controls = _non_negative_integer(
            raw_control_count, name="raw_control_count"
        )
        matched_effective = _non_negative_number(
            matched_n_eff, name="matched_n_eff"
        )
        control_effective = _non_negative_number(
            control_n_eff, name="control_n_eff"
        )
        if raw_matches < len(matched_ids) or raw_controls < len(control_ids):
            raise ValueError("raw counts may not be smaller than independent id counts")
        if matched_effective > len(matched_parent_ids or matched_ids):
            raise ValueError("matched_n_eff exceeds independent parent episodes")
        if control_effective > len(control_parent_ids or control_ids):
            raise ValueError("control_n_eff exceeds independent parent episodes")
        payload = {
            "snapshot_schema_version": EVIDENCE_SNAPSHOT_SCHEMA_VERSION,
            "assessment_schema_version": FORMULA_ASSESSMENT_SCHEMA_VERSION,
            "contract_version": CONTRACT_VERSION,
            "compatibility": compatibility,
            "legacy_adapter_version": (
                LEGACY_ADAPTER_VERSION
                if compatibility == LEGACY_SHADOW_READ_ONLY
                else None
            ),
            "phase": phase,
            "assessed_at_utc": _utc_iso(assessed_at_utc),
            "formula": formula,
            "formula_family_id": family_id,
            "matched_market_episode_ids": matched_ids,
            "control_market_episode_ids": control_ids,
            "matched_parent_market_episode_ids": matched_parent_ids,
            "control_parent_market_episode_ids": control_parent_ids,
            "raw_match_count": raw_matches,
            "raw_control_count": raw_controls,
            "matched_n_eff": matched_effective,
            "control_n_eff": control_effective,
            "metrics": _canonical_value(metrics or {}, path="metrics"),
            "evidence": _canonical_value(evidence or {}, path="evidence"),
            "provenance": _canonical_value(provenance or {}, path="provenance"),
            "assessment": assessment_payload,
            "live_eligible": False,
            "delivery_channel": "NONE",
        }
        payload_json = canonical_json(payload)
        identifier = _fingerprint("evidence-snapshot", json.loads(payload_json))
        return cls(snapshot_id=identifier, _payload_json=payload_json)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "EvidenceSnapshot":
        if not isinstance(value, Mapping):
            raise ValueError("EvidenceSnapshot payload must be an object")
        if value.get("snapshot_schema_version") != EVIDENCE_SNAPSHOT_SCHEMA_VERSION:
            raise ValueError("unsupported EvidenceSnapshot schema version")
        if value.get("assessment_schema_version") != FORMULA_ASSESSMENT_SCHEMA_VERSION:
            raise ValueError("EvidenceSnapshot assessment schema mismatch")
        if value.get("contract_version") != CONTRACT_VERSION:
            raise ValueError("EvidenceSnapshot contract version mismatch")
        if value.get("live_eligible") is not False or value.get("delivery_channel") != "NONE":
            raise ValueError("EvidenceSnapshot infrastructure may not authorize delivery")
        formula = value.get("formula")
        compatibility = compatibility_for_formula_schema(
            (formula or {}).get("formula_schema_version")
            if isinstance(formula, Mapping)
            else None
        )
        if value.get("compatibility") not in _COMPATIBILITY_STATES:
            raise ValueError("unsupported EvidenceSnapshot compatibility state")
        if value.get("compatibility") != compatibility:
            raise ValueError("EvidenceSnapshot compatibility does not match formula schema")
        assessment = FormulaAssessment.from_dict(value.get("assessment") or {})
        rebuilt = cls.build(
            formula_contract=formula or {},
            assessment=assessment,
            assessed_at_utc=value.get("assessed_at_utc"),
            formula_family_id=value.get("formula_family_id"),
            matched_market_episode_ids=value.get("matched_market_episode_ids") or [],
            control_market_episode_ids=value.get("control_market_episode_ids") or [],
            matched_parent_market_episode_ids=(
                value.get("matched_parent_market_episode_ids") or []
            ),
            control_parent_market_episode_ids=(
                value.get("control_parent_market_episode_ids") or []
            ),
            raw_match_count=value.get("raw_match_count"),
            raw_control_count=value.get("raw_control_count"),
            matched_n_eff=value.get("matched_n_eff"),
            control_n_eff=value.get("control_n_eff"),
            metrics=value.get("metrics") or {},
            evidence=value.get("evidence") or {},
            provenance=value.get("provenance") or {},
        )
        declared = _hex_identifier(value.get("snapshot_id"), name="snapshot_id")
        if declared != rebuilt.snapshot_id:
            raise ValueError("EvidenceSnapshot fingerprint mismatch")
        if canonical_json(value) != canonical_json(rebuilt.to_dict()):
            raise ValueError("EvidenceSnapshot contains non-canonical or unknown fields")
        return rebuilt

    def to_dict(self) -> Dict[str, Any]:
        body = json.loads(self._payload_json)
        return {"snapshot_id": self.snapshot_id, **body}

    @property
    def assessment(self) -> FormulaAssessment:
        return FormulaAssessment.from_dict(json.loads(self._payload_json)["assessment"])

    @property
    def formula_family_id(self) -> str:
        return str(json.loads(self._payload_json)["formula_family_id"])

    @property
    def compatibility(self) -> str:
        return str(json.loads(self._payload_json)["compatibility"])


def interpret_snapshot(value: EvidenceSnapshot | Mapping[str, Any]) -> FormulaAssessment:
    """Return the frozen assessment without rerunning acceptance calculations."""

    snapshot = value if isinstance(value, EvidenceSnapshot) else EvidenceSnapshot.from_dict(value)
    return snapshot.assessment


def contract_descriptor() -> Dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "assessment_schema_version": FORMULA_ASSESSMENT_SCHEMA_VERSION,
        "snapshot_schema_version": EVIDENCE_SNAPSHOT_SCHEMA_VERSION,
        "legacy_adapter_version": LEGACY_ADAPTER_VERSION,
        "current_formula_schema_version": CURRENT_FORMULA_SCHEMA_VERSION,
        "current_runtime": {
            "engine_version": CURRENT_ENGINE_VERSION,
            "feature_schema_version": CURRENT_FEATURE_SCHEMA_VERSION,
            "outcome_method_version": CURRENT_OUTCOME_METHOD_VERSION,
        },
        "legacy_formula_schema_versions": sorted(
            SUPPORTED_FORMULA_SCHEMA_VERSIONS - {CURRENT_FORMULA_SCHEMA_VERSION}
        ),
        "live_effect": "NONE",
        "delivery_channel": "NONE",
    }
