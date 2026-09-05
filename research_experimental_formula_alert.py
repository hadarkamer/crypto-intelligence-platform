"""Pure current-match contract for Stage-4 experimental Telegram alerts.

The candidate search remains historical and side-effect free.  This module is
the equally pure boundary that combines one verified search result with fresh,
frozen compact Stage-4 observations.  Historical outcomes are present in the
search receipt, but they are deliberately never read while deciding whether a
formula is true now.

No object produced here authorizes LIVE status or trading.  The content-
addressed payload is suitable for a later durable experimental-only queue.
"""

from __future__ import annotations

from collections.abc import Mapping as MappingABC
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from typing import Any, Dict, Mapping, Sequence

import research_signal_formula_exploration as exploration
import research_stage4_candidate_search as candidate_search
import research_mfe_mae_efficiency
import research_no_dwell_outcome


ALERT_SCHEMA_VERSION = "stage4-experimental-formula-alert-v1"
ALERT_ID_VERSION = "stage4-experimental-formula-alert-id-v1"
COMPACT_ENVELOPE_SCHEMA_VERSION = (
    "stage4-experimental-eligible-search-envelope-v1"
)
COMPACT_ENVELOPE_ID_VERSION = (
    "stage4-experimental-eligible-search-envelope-id-v1"
)
CURRENT_SNAPSHOT_COMMITMENT_VERSION = (
    "stage4-experimental-current-snapshot-no-outcome-v1"
)
SELECTION_POLICY_VERSION = (
    "stage4-experimental-top-ranked-per-symbol-direction-horizon-wave-v1"
)
CURRENT_TRIGGER_POLICY_VERSION = "stage4-experimental-current-trigger-v1"
RENDERER_VERSION = "stage4-experimental-telegram-renderer-v1"
EXPERIMENTAL_LABEL = "ניסיוני, לא מאושר למסחר"
MAX_CURRENT_SNAPSHOT_AGE_MINUTES = 45
SEARCH_FRESHNESS_CADENCE_MULTIPLIER = 2
ALERT_EXPIRY_MINUTES = 35
MAX_TELEGRAM_TEXT_LENGTH = 3900
MIN_INDEPENDENT_MOVEMENTS = 5

_UTC = timezone.utc
_HEX = frozenset("0123456789abcdef")
_SUPPORTED_HORIZONS = frozenset({60, 240, 720, 1440})
_BOUNDARY = {
    "formula_registry_effect": "NONE",
    "authority_effect": "NONE",
    "delivery_channel": "NONE",
    "live_eligible": False,
    "telegram_delivery_allowed": False,
    "trade_execution_allowed": False,
}
_ALERT_AUTHORITY = {
    "delivery_channel": "TELEGRAM_EXPERIMENTAL_ONLY",
    "formula_registry_effect": "NONE",
    "human_formula_approval_required": False,
    "live_eligible": False,
    "trade_execution_allowed": False,
    "telegram_delivery_allowed": True,
}
_REASON_TEXT = {
    "NO_HOLDOUT_CLAIM": "אין עדיין holdout עצמאי.",
    "NO_CONTROL_RELATIVE_CLAIM": (
        "לא הוכח יתרון ביחס לקבוצת ביקורת מקבילה."
    ),
    "EARLY_EVIDENCE_NOT_TRADING_MATURITY": (
        "זהו סף ראיות ניסיוני מוקדם, לא בשלות לאישור מסחר."
    ),
    "MULTIPLE_TESTING_DISCLOSURE_NOT_CONFIRMATORY_GATE": (
        "תיקון ריבוי הבדיקות מוצג לגילוי בלבד ואינו אישור מאמת."
    ),
}
_PATH_LABELS = {
    "PROBABILITY": "הסתברות",
    "ASYMMETRY": "אי־סימטריה",
}
_DIRECTION_LABELS = {"LONG": "לונג", "SHORT": "שורט"}
_METRIC_FIELDS = (
    "sample_size",
    "successes",
    "hit_rate_pct",
    "wilson_95_lower_pct",
    "favorable_dominance_successes",
    "favorable_dominance_rate_pct",
    "favorable_dominance_wilson_95_lower_pct",
    "median_mfe_pct",
    "median_mae_pct",
    "adverse_tail_mae_pct",
    "median_paired_favorable_minus_adverse_pct",
    "median_mfe_mae_ratio",
    "probability_exact_binomial_p_value",
    "asymmetry_exact_binomial_p_value",
)
_METRIC_RECEIPT_FIELDS = frozenset(
    {
        *_METRIC_FIELDS,
        "median_mfe_mae_ratio_state",
        "common_gates",
        "routes",
        "accepted_paths",
        "experimental_formula_eligible",
        "missing_by_route",
    }
)
_OCCURRENCE_COUNT_FIELDS = frozenset(
    {
        "independent_parent_movements_seen",
        "completed",
        "pending_horizon",
        "mature_outcome_unavailable",
        "wave_unbound_matches",
    }
)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def _fingerprint(kind: str, value: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        f"{kind}:{_canonical_json(value)}".encode("utf-8")
    ).hexdigest()


def _is_sha256(value: Any) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in _HEX for character in value)
    )


def _utc(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    elif type(value) is str and value.strip():
        try:
            result = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field_name} is not an ISO timestamp") from exc
    else:
        raise ValueError(f"{field_name} is required")
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return result.astimezone(_UTC)


def _finite(value: Any, *, field_name: str, optional: bool = False) -> Any:
    if value is None and optional:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be finite")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field_name} must be finite") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field_name} must be finite")
    if type(value) is int:
        return int(value)
    return number


def _json_copy(value: Any) -> Any:
    try:
        return json.loads(_canonical_json(value))
    except (TypeError, ValueError) as exc:
        raise ValueError("experimental alert input is not canonical JSON") from exc


def _formula_text(direction: str, conditions: Sequence[Mapping[str, Any]]) -> str:
    return f"{direction} WHEN " + " AND ".join(
        f"{condition['feature']} {condition['operator']} "
        f"{json.dumps(condition['value'], ensure_ascii=False)}"
        for condition in conditions
    )


def _validated_conditions(value: Any) -> tuple[list[Dict[str, Any]], Dict[str, Any]]:
    if not isinstance(value, (list, tuple)) or not (1 <= len(value) <= 3):
        raise ValueError("experimental candidate conditions are malformed")
    conditions: list[Dict[str, Any]] = []
    for item in value:
        if not isinstance(item, Mapping) or set(item) != {
            "feature",
            "operator",
            "value",
        }:
            raise ValueError("experimental candidate condition shape is invalid")
        feature = item.get("feature")
        operator = item.get("operator")
        expected = item.get("value")
        if feature not in exploration.ALLOWED_FEATURES:
            raise ValueError("experimental candidate uses an unknown feature")
        if feature == exploration.FEATURE_COMBINED_VOTE_COUNT:
            if operator != ">=" or type(expected) is not int or expected not in {2, 3}:
                raise ValueError("experimental vote-count condition is invalid")
        elif operator != "==" or expected is not True:
            raise ValueError("experimental boolean condition is invalid")
        conditions.append(
            {"feature": feature, "operator": operator, "value": expected}
        )
    canonical = sorted(
        conditions,
        key=lambda item: (
            item["feature"],
            item["operator"],
            _canonical_json(item["value"]),
        ),
    )
    if conditions != canonical:
        raise ValueError("experimental candidate conditions are not canonical")
    family = exploration.validate_candidate_feature_set(
        condition["feature"] for condition in conditions
    )
    return conditions, family


def _normalized_metrics(value: Any, *, completed: int) -> Dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _METRIC_RECEIPT_FIELDS:
        raise ValueError("experimental candidate metrics are malformed")
    metrics: Dict[str, Any] = {}
    for name in _METRIC_FIELDS:
        raw = value.get(name)
        if name in {
            "sample_size",
            "successes",
            "favorable_dominance_successes",
        }:
            if type(raw) is not int or raw < 0:
                raise ValueError(f"experimental metric {name} is invalid")
            metrics[name] = raw
        else:
            metrics[name] = _finite(
                raw, field_name=f"experimental metric {name}", optional=True
            )
    if metrics["sample_size"] != completed:
        raise ValueError("experimental sample size differs from completed movements")
    if metrics["successes"] > completed or (
        metrics["favorable_dominance_successes"] > completed
    ):
        raise ValueError("experimental success count exceeds sample size")
    for name in (
        "hit_rate_pct",
        "wilson_95_lower_pct",
        "favorable_dominance_rate_pct",
        "favorable_dominance_wilson_95_lower_pct",
    ):
        if metrics[name] is None or not (0.0 <= metrics[name] <= 100.0):
            raise ValueError(f"experimental metric {name} is outside 0..100")
    for name in (
        "median_mfe_pct",
        "median_mae_pct",
        "adverse_tail_mae_pct",
    ):
        if metrics[name] is None or metrics[name] < 0.0:
            raise ValueError(f"experimental metric {name} is negative or absent")
    if (
        metrics["median_paired_favorable_minus_adverse_pct"] is None
        or metrics["probability_exact_binomial_p_value"] is None
        or metrics["asymmetry_exact_binomial_p_value"] is None
    ):
        raise ValueError("experimental statistical metrics are incomplete")
    for name in (
        "probability_exact_binomial_p_value",
        "asymmetry_exact_binomial_p_value",
    ):
        if not 0.0 <= metrics[name] <= 1.0:
            raise ValueError(f"experimental metric {name} is outside 0..1")

    expected_rate = round(metrics["successes"] / completed * 100.0, 4)
    expected_wilson = round(
        float(candidate_search._wilson_lower_pct(metrics["successes"], completed)),
        4,
    )
    expected_dominance_rate = round(
        metrics["favorable_dominance_successes"] / completed * 100.0,
        4,
    )
    expected_dominance_wilson = round(
        float(
            candidate_search._wilson_lower_pct(
                metrics["favorable_dominance_successes"], completed
            )
        ),
        4,
    )
    expected_probability_p = round(
        float(
            candidate_search._one_sided_exact_binomial_p(
                metrics["successes"], completed
            )
        ),
        8,
    )
    expected_asymmetry_p = round(
        float(
            candidate_search._one_sided_exact_binomial_p(
                metrics["favorable_dominance_successes"], completed
            )
        ),
        8,
    )
    expected_numbers = {
        "hit_rate_pct": expected_rate,
        "wilson_95_lower_pct": expected_wilson,
        "favorable_dominance_rate_pct": expected_dominance_rate,
        "favorable_dominance_wilson_95_lower_pct": expected_dominance_wilson,
        "probability_exact_binomial_p_value": expected_probability_p,
        "asymmetry_exact_binomial_p_value": expected_asymmetry_p,
    }
    if any(metrics[name] != expected for name, expected in expected_numbers.items()):
        raise ValueError("experimental statistical metrics failed recomputation")

    efficiency = research_mfe_mae_efficiency.classify(
        metrics["median_mfe_pct"], metrics["median_mae_pct"]
    )
    expected_ratio = (
        None if efficiency.ratio is None else round(float(efficiency.ratio), 6)
    )
    if (
        metrics["median_mfe_mae_ratio"] != expected_ratio
        or value.get("median_mfe_mae_ratio_state") != efficiency.state
    ):
        raise ValueError("experimental MFE/MAE efficiency failed recomputation")
    metrics["median_mfe_mae_ratio_state"] = efficiency.state
    return metrics


def _recomputed_route_receipt(
    metrics: Mapping[str, Any],
    *,
    occurrences: Mapping[str, Any],
    minimum: int,
    horizon_minutes: int,
) -> Dict[str, Any]:
    efficiency = research_mfe_mae_efficiency.classify(
        metrics["median_mfe_pct"], metrics["median_mae_pct"]
    )
    coverage_complete = (
        occurrences["mature_outcome_unavailable"] == 0
        and occurrences["wave_unbound_matches"] == 0
    )
    common = {
        "minimum independent BTC parent occurrences": (
            metrics["sample_size"] >= minimum
        ),
        "mature matched occurrence coverage complete": coverage_complete,
        "wide favorable movement floor": (
            metrics["median_mfe_pct"]
            >= research_no_dwell_outcome.base_favorable_width_pct(
                horizon_minutes
            )
        ),
    }
    probability = {
        "hit rate": (
            metrics["hit_rate_pct"]
            >= candidate_search.PROBABILITY_HIT_RATE_FLOOR_PCT
        ),
        "Wilson lower bound": (
            metrics["wilson_95_lower_pct"]
            >= candidate_search.PROBABILITY_WILSON_LOWER_FLOOR_PCT
        ),
        "minimum favorable/adverse efficiency": efficiency.meets_threshold(
            candidate_search.PROBABILITY_MIN_MFE_MAE_RATIO
        ),
    }
    asymmetry = {
        "minimum directional hit rate": (
            metrics["hit_rate_pct"]
            >= candidate_search.ASYMMETRY_DIRECTIONAL_HIT_RATE_FLOOR_PCT
        ),
        "favorable dominance rate": (
            metrics["favorable_dominance_rate_pct"]
            >= candidate_search.ASYMMETRY_DOMINANCE_RATE_FLOOR_PCT
        ),
        "favorable dominance Wilson lower bound": (
            metrics["favorable_dominance_wilson_95_lower_pct"]
            >= candidate_search.ASYMMETRY_WILSON_LOWER_FLOOR_PCT
        ),
        "strong favorable/adverse efficiency": efficiency.meets_threshold(
            candidate_search.ASYMMETRY_MIN_MFE_MAE_RATIO
        ),
        "positive paired favorable/adverse edge": (
            metrics["median_paired_favorable_minus_adverse_pct"] > 0.0
        ),
    }
    common_passed = all(common.values())
    probability_passed = common_passed and all(probability.values())
    asymmetry_passed = common_passed and all(asymmetry.values())
    accepted_paths = [
        name
        for name, passed in (
            ("PROBABILITY", probability_passed),
            ("ASYMMETRY", asymmetry_passed),
        )
        if passed
    ]
    return {
        "common_gates": common,
        "routes": {
            "PROBABILITY": {
                "passed": probability_passed,
                "gates": probability,
            },
            "ASYMMETRY": {
                "passed": asymmetry_passed,
                "gates": asymmetry,
            },
        },
        "accepted_paths": accepted_paths,
        "experimental_formula_eligible": bool(accepted_paths),
        "missing_by_route": {
            "COMMON": [name for name, passed in common.items() if not passed],
            "PROBABILITY": [
                name for name, passed in probability.items() if not passed
            ],
            "ASYMMETRY": [
                name for name, passed in asymmetry.items() if not passed
            ],
        },
    }


def _normalized_candidate(
    value: Mapping[str, Any],
    *,
    horizon_minutes: int,
    search_minimum: int,
) -> Dict[str, Any]:
    for key, expected in _BOUNDARY.items():
        if value.get(key) != expected:
            raise ValueError("experimental candidate exceeded source authority")
    direction = value.get("direction")
    if direction not in {"LONG", "SHORT"}:
        raise ValueError("experimental candidate direction is invalid")
    if value.get("horizon_minutes") != horizon_minutes:
        raise ValueError("experimental candidate horizon is inconsistent")
    expected_versions = {
        "candidate_schema_version": candidate_search.CANDIDATE_SCHEMA_VERSION,
        "engine_version": candidate_search.ENGINE_VERSION,
        "feature_schema_version": exploration.FEATURE_SCHEMA_VERSION,
        "label_policy_version": candidate_search.LABEL_POLICY_VERSION,
        "independence_policy_version": candidate_search.INDEPENDENCE_POLICY_VERSION,
    }
    if any(value.get(name) != expected for name, expected in expected_versions.items()):
        raise ValueError("experimental candidate version is inconsistent")
    conditions, family = _validated_conditions(value.get("conditions"))
    candidate_key = value.get("candidate_key")
    if candidate_key != candidate_search.candidate_key_sha256(
        direction=direction,
        horizon_minutes=horizon_minutes,
        conditions=conditions,
    ):
        raise ValueError("experimental candidate key is invalid")
    if value.get("formula_text") != _formula_text(direction, conditions):
        raise ValueError("experimental formula text is inconsistent")
    if value.get("condition_source_closure") != family["source_closure"] or (
        value.get("condition_evidence_sources") != family["deduplicated_sources"]
    ):
        raise ValueError("experimental candidate source closure is inconsistent")
    for digest_name in (
        "match_set_sha256",
        "occurrence_evidence_sha256",
    ):
        if not _is_sha256(value.get(digest_name)):
            raise ValueError(f"experimental candidate {digest_name} is invalid")

    gate = value.get("eligibility_gate")
    occurrences = value.get("occurrence_counts")
    if (
        not isinstance(gate, Mapping)
        or set(gate)
        != {
            "atomic",
            "minimum_independent_occurrences",
            "independence_unit",
            "passed",
            "separate_later_probability_gate",
        }
        or not isinstance(occurrences, Mapping)
        or set(occurrences) != _OCCURRENCE_COUNT_FIELDS
        or any(type(occurrences[name]) is not int or occurrences[name] < 0 for name in occurrences)
    ):
        raise ValueError("experimental candidate gate is malformed")
    minimum = gate.get("minimum_independent_occurrences")
    completed = occurrences.get("completed")
    independent_seen = occurrences.get("independent_parent_movements_seen")
    if (
        value.get("experimental_formula_eligible") is not True
        or gate.get("atomic") is not True
        or gate.get("passed") is not True
        or gate.get("separate_later_probability_gate") is not False
        or gate.get("independence_unit")
        != "DISTINCT_BTC_PARENT_MARKET_MOVEMENT"
        or type(minimum) is not int
        or minimum != search_minimum
        or minimum < MIN_INDEPENDENT_MOVEMENTS
        or type(completed) is not int
        or completed < minimum
        or type(independent_seen) is not int
        or independent_seen < completed
        or independent_seen
        != completed
        + occurrences["pending_horizon"]
        + occurrences["mature_outcome_unavailable"]
    ):
        raise ValueError("experimental candidate bypassed the atomic five-movement gate")
    metrics = _normalized_metrics(value.get("metrics"), completed=completed)
    recomputed = _recomputed_route_receipt(
        metrics,
        occurrences=occurrences,
        minimum=minimum,
        horizon_minutes=horizon_minutes,
    )
    accepted_paths = value.get("accepted_paths")
    if (
        not isinstance(accepted_paths, (list, tuple))
        or not accepted_paths
        or len(accepted_paths) != len(set(accepted_paths))
        or any(path not in _PATH_LABELS for path in accepted_paths)
        or list(accepted_paths) != metrics_value_paths(value.get("metrics"))
        or list(accepted_paths) != recomputed["accepted_paths"]
    ):
        raise ValueError("experimental candidate accepted paths are inconsistent")
    raw_metrics = value["metrics"]
    if any(
        raw_metrics.get(name) != recomputed[name]
        for name in (
            "common_gates",
            "routes",
            "accepted_paths",
            "experimental_formula_eligible",
            "missing_by_route",
        )
    ) or any(
        value.get(name) is not True
        for name in ("experimental_formula_eligible",)
    ):
        raise ValueError("experimental candidate evidence gate failed recomputation")

    multiple = value.get("multiple_testing")
    if (
        not isinstance(multiple, Mapping)
        or multiple.get("policy_version")
        != candidate_search.MULTIPLE_TESTING_POLICY_VERSION
        or multiple.get("decision_effect") != "DISCLOSURE_ONLY_EXPERIMENTAL"
        or multiple.get("eligibility_changed") is not False
    ):
        raise ValueError("experimental multiple-testing disclosure is malformed")
    caveats = value.get("experimental_caveats")
    if (
        not isinstance(caveats, (list, tuple))
        or any(type(reason) is not str for reason in caveats)
        or "NO_HOLDOUT_CLAIM" not in caveats
        or "NO_CONTROL_RELATIVE_CLAIM" not in caveats
    ):
        raise ValueError("experimental candidate maturity caveats are incomplete")
    raw_match_count = value.get("raw_match_count")
    if type(raw_match_count) is not int or raw_match_count < independent_seen:
        raise ValueError("experimental candidate raw match count is invalid")
    return {
        "candidate_key": candidate_key,
        **expected_versions,
        "direction": direction,
        "horizon_minutes": horizon_minutes,
        "conditions": conditions,
        "formula_text": value["formula_text"],
        "condition_source_closure": _json_copy(family["source_closure"]),
        "condition_evidence_sources": list(family["deduplicated_sources"]),
        "raw_match_count": raw_match_count,
        "match_set_sha256": value["match_set_sha256"],
        "occurrence_evidence_sha256": value["occurrence_evidence_sha256"],
        "independent_movement_count": completed,
        "independent_parent_movements_seen": independent_seen,
        "metrics": metrics,
        "accepted_paths": list(accepted_paths),
        "multiple_testing": {
            "policy_version": multiple["policy_version"],
            "decision_effect": multiple["decision_effect"],
            "eligibility_changed": False,
            "probability_q_value": _finite(
                multiple.get("probability_q_value"),
                field_name="probability q-value",
                optional=True,
            ),
            "asymmetry_q_value": _finite(
                multiple.get("asymmetry_q_value"),
                field_name="asymmetry q-value",
                optional=True,
            ),
        },
        "experimental_caveats": sorted(set(caveats)),
        "experimental_formula_eligible": True,
        "formula_registry_effect": "NONE",
        "delivery_channel": "NONE",
        "live_eligible": False,
        "telegram_delivery_allowed": False,
        "trade_execution_allowed": False,
    }


def metrics_value_paths(metrics: Any) -> list[str]:
    if not isinstance(metrics, Mapping):
        return []
    value = metrics.get("accepted_paths")
    if not isinstance(value, (list, tuple)):
        return []
    return list(value)


def _candidate_rank(candidate: Mapping[str, Any]) -> tuple[Any, ...]:
    metrics = candidate["metrics"]
    return (
        -len(candidate["accepted_paths"]),
        -float(metrics.get("wilson_95_lower_pct") or 0.0),
        -float(metrics.get("favorable_dominance_wilson_95_lower_pct") or 0.0),
        -int(metrics["sample_size"]),
        len(candidate["conditions"]),
        candidate["candidate_key"],
    )


@dataclass(frozen=True, slots=True)
class CompactEligibleSearchEnvelope:
    """Content-addressed subset of one fully verified candidate-search result."""

    envelope_sha256: str
    _payload_json: str = field(repr=False)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "envelope_sha256": self.envelope_sha256,
            **json.loads(self._payload_json),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "CompactEligibleSearchEnvelope":
        if not isinstance(value, Mapping):
            raise ValueError("compact eligible search envelope must be an object")
        payload = dict(value)
        envelope_sha = payload.pop("envelope_sha256", None)
        if not _is_sha256(envelope_sha):
            raise ValueError("compact eligible search envelope hash is invalid")
        if payload.get("envelope_schema_version") != COMPACT_ENVELOPE_SCHEMA_VERSION:
            raise ValueError("compact eligible search envelope schema is invalid")
        expected = _fingerprint(COMPACT_ENVELOPE_ID_VERSION, payload)
        if envelope_sha != expected:
            raise ValueError("compact eligible search envelope hash mismatch")
        candidates = payload.get("eligible_candidates")
        if not isinstance(candidates, list):
            raise ValueError("compact eligible search envelope candidates are invalid")
        return cls(envelope_sha, _canonical_json(payload))


def compact_eligible_search_envelope(
    result: Mapping[str, Any],
) -> CompactEligibleSearchEnvelope:
    """Verify a full pure-search result and retain only eligible bounded data."""

    if not isinstance(result, Mapping):
        raise ValueError("candidate search result must be an object")
    receipt_sha = result.get("search_receipt_sha256")
    if not _is_sha256(receipt_sha) or receipt_sha != (
        candidate_search.candidate_search_receipt_sha256(result)
    ):
        raise ValueError("candidate search receipt hash mismatch")
    for key, expected in _BOUNDARY.items():
        if result.get(key) != expected:
            raise ValueError("candidate search exceeded its authority boundary")
    expected_versions = {
        "engine_version": candidate_search.ENGINE_VERSION,
        "candidate_schema_version": candidate_search.CANDIDATE_SCHEMA_VERSION,
        "feature_schema_version": exploration.FEATURE_SCHEMA_VERSION,
        "label_policy_version": candidate_search.LABEL_POLICY_VERSION,
        "independence_policy_version": candidate_search.INDEPENDENCE_POLICY_VERSION,
        "multiple_testing_policy_version": (
            candidate_search.MULTIPLE_TESTING_POLICY_VERSION
        ),
        "compact_observation_schema_version": (
            candidate_search.COMPACT_OBSERVATION_SCHEMA_VERSION
        ),
    }
    if any(result.get(key) != expected for key, expected in expected_versions.items()):
        raise ValueError("candidate search version mismatch")
    if result.get("ready_for_candidate_search") is not True:
        raise ValueError("candidate search is not ready")
    horizon = result.get("horizon_minutes")
    if type(horizon) is not int or horizon not in _SUPPORTED_HORIZONS:
        raise ValueError("candidate search horizon is invalid")
    atomic = result.get("atomic_eligibility")
    config = result.get("config")
    if not isinstance(atomic, Mapping) or not isinstance(config, Mapping):
        raise ValueError("candidate search atomic gate is malformed")
    search_minimum = atomic.get("minimum_independent_occurrences")
    if (
        type(search_minimum) is not int
        or search_minimum < MIN_INDEPENDENT_MOVEMENTS
        or config.get("minimum_independent_occurrences") != search_minimum
        or atomic.get("independence_unit")
        != "DISTINCT_BTC_PARENT_MARKET_MOVEMENT"
        or atomic.get("separate_later_probability_gate") is not False
        or result.get("qualifying_favorable_move_pct")
        != research_no_dwell_outcome.base_favorable_width_pct(horizon)
    ):
        raise ValueError("candidate search atomic gate is inconsistent")
    analysis_as_of = _utc(
        result.get("analysis_as_of_utc"), field_name="analysis_as_of_utc"
    )
    if (
        result.get("input_observation_schema_version")
        != candidate_search.COMPACT_OBSERVATION_SCHEMA_VERSION
        or result.get("input_observation_hash_contract_version")
        != candidate_search.COMPACT_OBSERVATION_CHAIN_HASH_VERSION
        or type(result.get("input_observation_count")) is not int
        or not (0 <= result["input_observation_count"] <= candidate_search.MAX_OBSERVATIONS)
        or not _is_sha256(result.get("input_observation_chain_sha256"))
    ):
        raise ValueError("candidate search observation binding is invalid")
    counts = result.get("counts")
    raw_candidates = result.get("eligible_candidate_variants")
    if not isinstance(counts, Mapping) or not isinstance(raw_candidates, list):
        raise ValueError("candidate search candidates are malformed")
    if (
        counts.get("observations") != result["input_observation_count"]
        or type(counts.get("eligible_candidate_variants")) is not int
    ):
        raise ValueError("candidate search counts are inconsistent")
    if any(not isinstance(candidate, Mapping) for candidate in raw_candidates):
        raise ValueError("candidate search variants are malformed")
    normalized = [
        _normalized_candidate(
            candidate,
            horizon_minutes=horizon,
            search_minimum=search_minimum,
        )
        for candidate in raw_candidates
        if isinstance(candidate, Mapping)
        and candidate.get("experimental_formula_eligible") is True
    ]
    if len(normalized) != counts["eligible_candidate_variants"]:
        raise ValueError("candidate search eligible count is inconsistent")
    normalized.sort(key=_candidate_rank)
    for rank, candidate in enumerate(normalized, start=1):
        candidate["search_rank"] = rank
    payload = {
        "envelope_schema_version": COMPACT_ENVELOPE_SCHEMA_VERSION,
        "search_receipt_sha256": receipt_sha,
        "analysis_as_of_utc": analysis_as_of.isoformat(),
        "horizon_minutes": horizon,
        "input_observation_schema_version": result[
            "input_observation_schema_version"
        ],
        "input_observation_hash_contract_version": result[
            "input_observation_hash_contract_version"
        ],
        "input_observation_count": result["input_observation_count"],
        "input_observation_chain_sha256": result[
            "input_observation_chain_sha256"
        ],
        "search_budget_exhausted": result.get("search_budget_exhausted") is True,
        "eligible_candidates": normalized,
    }
    envelope_sha = _fingerprint(COMPACT_ENVELOPE_ID_VERSION, payload)
    return CompactEligibleSearchEnvelope(
        envelope_sha256=envelope_sha,
        _payload_json=_canonical_json(payload),
    )


def _validated_current_observation(
    value: candidate_search.CompactCurrentStage4Observation,
    *,
    current_time: datetime,
) -> Dict[str, Any] | None:
    """Validate only frozen decision fields; never inspect the outcome member."""

    if type(value) is not candidate_search.CompactCurrentStage4Observation:
        raise ValueError("current Stage-4 observation type is invalid")
    decision = _utc(
        value.projection_decision_time_utc,
        field_name="projection_decision_time_utc",
    )
    age = current_time - decision
    if age < timedelta(0) or age > timedelta(
        minutes=MAX_CURRENT_SNAPSHOT_AGE_MINUTES
    ):
        raise ValueError("current Stage-4 observation is not fresh")
    candidate_search.validate_compact_current_observation(
        value,
        analysis_as_of_utc=current_time,
    )
    symbol = value.symbol
    features = value.features
    if (
        type(features) is not candidate_search._CompactFeatureMapping
        or type(features.true_mask) is not int
        or not (0 <= features.true_mask < (1 << len(candidate_search._BOOLEAN_FEATURES)))
        or (
            features.combined_vote_count is not None
            and (
                type(features.combined_vote_count) is not int
                or features.combined_vote_count not in {2, 3}
            )
        )
    ):
        raise ValueError("current Stage-4 frozen features are invalid")
    expanded = {name: features[name] for name in exploration.ALLOWED_FEATURES}
    if expanded[exploration.FEATURE_MAX_PAIN_STRONG] and not expanded[
        exploration.FEATURE_MAX_PAIN_CONFIRMED
    ]:
        raise ValueError("current strong Max-Pain lacks confirmation")
    if expanded[exploration.FEATURE_MAGNET_STRONG] and not expanded[
        exploration.FEATURE_MAGNET_CONFIRMED
    ]:
        raise ValueError("current strong Magnet lacks confirmation")
    combined_sources = sum(
        int(expanded[name])
        for name in (
            exploration.FEATURE_COMBINED_COINGLASS,
            exploration.FEATURE_COMBINED_PRICE_OI,
            exploration.FEATURE_COMBINED_FUTURES_CVD,
        )
    )
    if expanded[exploration.FEATURE_COMBINED_CONFIRMED]:
        if expanded[exploration.FEATURE_COMBINED_VOTE_COUNT] != combined_sources:
            raise ValueError("current Combined feature is inconsistent")
    elif expanded[exploration.FEATURE_COMBINED_VOTE_COUNT] is not None or combined_sources:
        raise ValueError("current absent Combined carries evidence")
    binding = value.wave_binding
    # The upstream validator admits exactly two terminal states.  UNAVAILABLE
    # is a valid current observation, but it cannot define the independent BTC
    # parent wave required for one trigger identity, so it is skipped.  Any
    # malformed/non-terminal binding has already failed closed above.
    if binding.status == "UNAVAILABLE":
        return None
    if (
        type(binding) is not candidate_search._CompactWaveBinding
        or binding.status != "BOUND"
        or binding.reason is not None
        or not _is_sha256(binding.btc_parent_movement_id)
    ):
        raise ValueError("current Stage-4 snapshot has an invalid BOUND wave")
    commitment = {
        "contract_version": CURRENT_SNAPSHOT_COMMITMENT_VERSION,
        "compact_observation_schema_version": (
            candidate_search.CURRENT_OBSERVATION_SCHEMA_VERSION
        ),
        "observation_id": value.observation_id,
        "projection_event_id": value.projection_event_id,
        "projection_event_fingerprint": value.projection_event_fingerprint,
        "snapshot_set_id": value.snapshot_set_id,
        "snapshot_key": value.snapshot_key,
        "projection_decision_time_utc": decision.isoformat(),
        "archive_cycle_time_utc": _utc(
            value.archive_cycle_time_utc,
            field_name="archive_cycle_time_utc",
        ).isoformat(),
        "symbol": symbol,
        "direction": value.direction,
        "feature_true_mask": features.true_mask,
        "combined_vote_count": features.combined_vote_count,
        "source_event_ids": list(value.source_event_ids),
        "source_event_fingerprints": list(value.source_event_fingerprints),
        "wave_binding_status": binding.status,
        "btc_parent_movement_id": binding.btc_parent_movement_id,
    }
    return {
        **commitment,
        "current_snapshot_sha256": _fingerprint(
            CURRENT_SNAPSHOT_COMMITMENT_VERSION, commitment
        ),
        "features": expanded,
    }


def _condition_result(
    features: Mapping[str, Any], condition: Mapping[str, Any]
) -> Dict[str, Any]:
    actual = features[condition["feature"]]
    if condition["operator"] == "==":
        passed = type(actual) is type(condition["value"]) and (
            actual == condition["value"]
        )
    else:
        passed = (
            not isinstance(actual, bool)
            and isinstance(actual, (int, float))
            and math.isfinite(float(actual))
            and float(actual) >= float(condition["value"])
        )
    return {
        "feature": condition["feature"],
        "operator": condition["operator"],
        "expected": condition["value"],
        "actual": actual,
        "passed": passed,
    }


def _single_condition_family(
    conditions: Sequence[Mapping[str, Any]],
) -> str | None:
    families = set()
    for condition in conditions:
        feature = str(condition["feature"])
        if feature.startswith("stage4.max_pain."):
            families.add("Max Pain")
        elif feature.startswith("stage4.magnet."):
            families.add("Magnet")
        elif feature.startswith("stage4.combined."):
            families.add("Combined/Composite")
        else:
            return None
    return next(iter(families)) if len(families) == 1 else None


@dataclass(frozen=True, slots=True)
class ExperimentalFormulaAlert:
    """Verified, content-addressed experimental alert payload."""

    alert_id: str
    _payload_json: str = field(repr=False)

    def to_dict(self) -> Dict[str, Any]:
        return {"alert_id": self.alert_id, **json.loads(self._payload_json)}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ExperimentalFormulaAlert":
        if not isinstance(value, Mapping):
            raise ValueError("experimental alert payload must be an object")
        payload = dict(value)
        alert_id = payload.pop("alert_id", None)
        if not _is_sha256(alert_id):
            raise ValueError("experimental alert id is invalid")
        if payload.get("alert_schema_version") != ALERT_SCHEMA_VERSION:
            raise ValueError("experimental alert schema is invalid")
        if payload.get("experimental_label") != EXPERIMENTAL_LABEL:
            raise ValueError("experimental alert label is invalid")
        authority = payload.get("authority")
        if authority != _ALERT_AUTHORITY:
            raise ValueError("experimental alert authority is invalid")
        expected = _fingerprint(ALERT_ID_VERSION, payload)
        if alert_id != expected:
            raise ValueError("experimental alert payload hash mismatch")
        return cls(alert_id=alert_id, _payload_json=_canonical_json(payload))


def build_experimental_alerts(
    latest_observations: Sequence[
        candidate_search.CompactCurrentStage4Observation
    ],
    search_envelope: CompactEligibleSearchEnvelope,
    *,
    current_time_utc: Any,
) -> list[ExperimentalFormulaAlert]:
    """Choose one ranked current formula per symbol/direction/horizon/wave."""

    if not isinstance(latest_observations, (list, tuple)):
        raise ValueError("latest Stage-4 observations must be a bounded sequence")
    if len(latest_observations) > 1024:
        raise ValueError("latest Stage-4 observation set is unbounded")
    if type(search_envelope) is not CompactEligibleSearchEnvelope:
        raise ValueError("eligible search envelope type is invalid")
    envelope = CompactEligibleSearchEnvelope.from_dict(search_envelope.to_dict())
    source = envelope.to_dict()
    analysis_as_of = _utc(
        source["analysis_as_of_utc"], field_name="analysis_as_of_utc"
    )
    current_time = _utc(current_time_utc, field_name="current_time_utc")
    search_age = current_time - analysis_as_of
    max_search_age_minutes = (
        int(source["horizon_minutes"]) * SEARCH_FRESHNESS_CADENCE_MULTIPLIER
    )
    if search_age < timedelta(0) or search_age > timedelta(
        minutes=max_search_age_minutes
    ):
        raise ValueError("eligible candidate search is not current")

    current_rows: list[Dict[str, Any]] = []
    for item in latest_observations:
        row = _validated_current_observation(item, current_time=current_time)
        if row is not None:
            current_rows.append(row)
    latest_by_symbol_direction: Dict[tuple[str, str], datetime] = {}
    for row in current_rows:
        key = (row["symbol"], row["direction"])
        decision = _utc(
            row["projection_decision_time_utc"],
            field_name="projection_decision_time_utc",
        )
        latest_by_symbol_direction[key] = max(
            decision, latest_by_symbol_direction.get(key, decision)
        )
    for row in current_rows:
        key = (row["symbol"], row["direction"])
        if _utc(
            row["projection_decision_time_utc"],
            field_name="projection_decision_time_utc",
        ) != latest_by_symbol_direction[key]:
            raise ValueError("current Stage-4 input contains a non-latest observation")

    horizon = source["horizon_minutes"]
    candidates = source["eligible_candidates"]
    selected: Dict[tuple[str, str, int, str], tuple[Dict[str, Any], Dict[str, Any]]] = {}
    for row in current_rows:
        matching: list[tuple[Dict[str, Any], list[Dict[str, Any]]]] = []
        for candidate in candidates:
            if candidate["direction"] != row["direction"]:
                continue
            results = [
                _condition_result(row["features"], condition)
                for condition in candidate["conditions"]
            ]
            if all(result["passed"] for result in results):
                matching.append((candidate, results))
        if not matching:
            continue
        matching.sort(key=lambda item: int(item[0]["search_rank"]))
        candidate, condition_results = matching[0]
        group = (
            row["symbol"],
            row["direction"],
            horizon,
            row["btc_parent_movement_id"],
        )
        existing = selected.get(group)
        if existing is None or (
            row["projection_event_id"], row["observation_id"]
        ) > (
            existing[0]["projection_event_id"], existing[0]["observation_id"]
        ):
            selected[group] = (
                {**row, "condition_results": condition_results},
                candidate,
            )

    alerts: list[ExperimentalFormulaAlert] = []
    for group in sorted(selected):
        row, candidate = selected[group]
        expires_at = _utc(
            row["projection_decision_time_utc"],
            field_name="projection_decision_time_utc",
        ) + timedelta(minutes=ALERT_EXPIRY_MINUTES)
        if current_time >= expires_at:
            continue
        reason_by_code = {
            code: _REASON_TEXT[code]
            for code in {
                "NO_HOLDOUT_CLAIM",
                "NO_CONTROL_RELATIVE_CLAIM",
                "EARLY_EVIDENCE_NOT_TRADING_MATURITY",
                *(
                    reason
                    for reason in candidate["experimental_caveats"]
                    if reason in _REASON_TEXT
                ),
            }
        }
        independent_count = candidate["independent_movement_count"]
        if independent_count < exploration.MATURITY_MIN_BTC_PARENT_MOVEMENTS:
            reason_by_code["BELOW_TWENTY_INDEPENDENT_MOVEMENTS"] = (
                f"רק {independent_count} תנועות עצמאיות; טרם הושלם סף "
                f"הבשלות של {exploration.MATURITY_MIN_BTC_PARENT_MOVEMENTS}."
            )
        single_family = _single_condition_family(candidate["conditions"])
        if single_family is not None:
            reason_by_code["SINGLE_FAMILY_EVIDENCE"] = (
                f"כל תנאי הנוסחה שייכים למשפחת {single_family} אחת; "
                "אין לספור רכיבים מאותה משפחה כראיות עצמאיות נוספות."
            )
        reason_codes = sorted(reason_by_code)
        trigger_key = _fingerprint(
            CURRENT_TRIGGER_POLICY_VERSION,
            {
                "candidate_key": candidate["candidate_key"],
                "symbol": row["symbol"],
                "direction": row["direction"],
                "horizon_minutes": horizon,
                "btc_parent_movement_id": row[
                    "btc_parent_movement_id"
                ],
            },
        )
        trigger_receipt = _fingerprint(
            "stage4-experimental-current-trigger-receipt-v1",
            {
                "selection_policy_version": SELECTION_POLICY_VERSION,
                "trigger_key": trigger_key,
                "current_snapshot_sha256": row[
                    "current_snapshot_sha256"
                ],
                "condition_results": row["condition_results"],
                "expires_at_utc": expires_at.isoformat(),
            },
        )
        unsigned = {
            "alert_schema_version": ALERT_SCHEMA_VERSION,
            "selection_policy_version": SELECTION_POLICY_VERSION,
            "current_trigger_policy_version": CURRENT_TRIGGER_POLICY_VERSION,
            "renderer_version": RENDERER_VERSION,
            "experimental_label": EXPERIMENTAL_LABEL,
            "disclaimer": EXPERIMENTAL_LABEL,
            "symbol": row["symbol"],
            "direction": row["direction"],
            "horizon_minutes": horizon,
            "decision_time_utc": row["projection_decision_time_utc"],
            "analysis_as_of_utc": source["analysis_as_of_utc"],
            "expires_at_utc": expires_at.isoformat(),
            "btc_parent_movement_id": row["btc_parent_movement_id"],
            "trigger_key": trigger_key,
            "current_trigger_receipt_sha256": trigger_receipt,
            "candidate_snapshot": _json_copy(candidate),
            "formula": {
                "candidate_key": candidate["candidate_key"],
                "formula_text": candidate["formula_text"],
                "conditions": candidate["conditions"],
                "condition_source_closure": candidate[
                    "condition_source_closure"
                ],
                "condition_evidence_sources": candidate[
                    "condition_evidence_sources"
                ],
            },
            "current_snapshot": {
                "status": "FROZEN_BOUND_FRESH",
                "contract_version": CURRENT_SNAPSHOT_COMMITMENT_VERSION,
                "observation_id": row["observation_id"],
                "projection_event_id": row["projection_event_id"],
                "projection_event_fingerprint": row[
                    "projection_event_fingerprint"
                ],
                "snapshot_set_id": row["snapshot_set_id"],
                "snapshot_key": row["snapshot_key"],
                "projection_decision_time_utc": row[
                    "projection_decision_time_utc"
                ],
                "archive_cycle_time_utc": row[
                    "archive_cycle_time_utc"
                ],
                "symbol": row["symbol"],
                "direction": row["direction"],
                "btc_parent_movement_id": row[
                    "btc_parent_movement_id"
                ],
                "feature_true_mask": row["feature_true_mask"],
                "combined_vote_count": row["combined_vote_count"],
                "source_event_ids": row["source_event_ids"],
                "source_event_fingerprints": row[
                    "source_event_fingerprints"
                ],
                "current_snapshot_sha256": row["current_snapshot_sha256"],
                "trigger_snapshot_sha256": row[
                    "current_snapshot_sha256"
                ],
                "condition_results": row["condition_results"],
            },
            "evidence": {
                "independence_unit": "DISTINCT_BTC_PARENT_MARKET_MOVEMENT",
                "independent_movement_count": candidate[
                    "independent_movement_count"
                ],
                "independent_parent_movements_seen": candidate[
                    "independent_parent_movements_seen"
                ],
                "raw_match_count": candidate["raw_match_count"],
                "accepted_paths": candidate["accepted_paths"],
                "metrics": candidate["metrics"],
                "multiple_testing": candidate["multiple_testing"],
            },
            "experimental_reason_codes": reason_codes,
            "experimental_reasons": [
                reason_by_code[code] for code in reason_codes
            ],
            "selection": {
                "search_rank": candidate["search_rank"],
                "group": {
                    "symbol": group[0],
                    "direction": group[1],
                    "horizon_minutes": group[2],
                    "btc_parent_movement_id": group[3],
                },
            },
            "provenance": {
                "search_receipt_sha256": source["search_receipt_sha256"],
                "compact_search_envelope_sha256": source["envelope_sha256"],
                "input_observation_schema_version": source[
                    "input_observation_schema_version"
                ],
                "input_observation_hash_contract_version": source[
                    "input_observation_hash_contract_version"
                ],
                "input_observation_count": source["input_observation_count"],
                "input_observation_chain_sha256": source[
                    "input_observation_chain_sha256"
                ],
                "candidate_schema_version": candidate[
                    "candidate_schema_version"
                ],
                "candidate_engine_version": candidate["engine_version"],
                "feature_schema_version": candidate["feature_schema_version"],
                "label_policy_version": candidate["label_policy_version"],
                "independence_policy_version": candidate[
                    "independence_policy_version"
                ],
            },
            "authority": dict(_ALERT_AUTHORITY),
        }
        alert_id = _fingerprint(ALERT_ID_VERSION, unsigned)
        alerts.append(
            ExperimentalFormulaAlert.from_dict(
                {"alert_id": alert_id, **unsigned}
            )
        )
    return alerts


def _format_number(value: Any, *, suffix: str = "") -> str:
    number = _finite(value, field_name="rendered metric")
    rounded = round(float(number), 2)
    text = str(int(rounded)) if rounded.is_integer() else (
        f"{rounded:.2f}".rstrip("0").rstrip(".")
    )
    return f"{text}{suffix}"


def _format_horizon(minutes: int) -> str:
    if minutes % 1440 == 0:
        days = minutes // 1440
        return "יום אחד" if days == 1 else f"{days} ימים"
    hours = minutes // 60
    return "שעה אחת" if hours == 1 else f"{hours} שעות"


def _format_condition(condition: Mapping[str, Any]) -> str:
    expected = condition["value"]
    rendered = (
        "true" if expected is True else "false" if expected is False else str(expected)
    )
    return f"{condition['feature']} {condition['operator']} {rendered}"


def render_experimental_telegram_alert(
    value: ExperimentalFormulaAlert | Mapping[str, Any],
) -> str:
    """Verify and render one plain-text experimental Telegram message."""

    alert = (
        value
        if type(value) is ExperimentalFormulaAlert
        else ExperimentalFormulaAlert.from_dict(value)
    )
    payload = ExperimentalFormulaAlert.from_dict(alert.to_dict()).to_dict()
    evidence = payload["evidence"]
    metrics = evidence["metrics"]
    conditions = payload["formula"]["conditions"]
    paths = " + ".join(_PATH_LABELS[path] for path in evidence["accepted_paths"])
    lines = [
        f"🧪 {EXPERIMENTAL_LABEL}",
        f"מטבע: {payload['symbol']}",
        (
            f"כיוון: {_DIRECTION_LABELS[payload['direction']]} "
            f"({payload['direction']})"
        ),
        f"אופק זמן: {_format_horizon(payload['horizon_minutes'])}",
        "תנאי הנוסחה:",
        *(f"• {_format_condition(condition)}" for condition in conditions),
        (
            "מספר תנועות עצמאיות: "
            f"{evidence['independent_movement_count']} "
            "(גלי מחיר BTC נפרדים)"
        ),
        f"מסלול ראיות: {paths}",
    ]
    if "PROBABILITY" in evidence["accepted_paths"]:
        lines.append(
            "הסתברות: "
            f"{metrics['successes']}/{metrics['sample_size']} הצלחות | "
            f"{_format_number(metrics['hit_rate_pct'], suffix='%')} | "
            "Wilson 95% תחתון "
            f"{_format_number(metrics['wilson_95_lower_pct'], suffix='%')}"
        )
    if "ASYMMETRY" in evidence["accepted_paths"]:
        efficiency_text = (
            "בלתי חסום (MAE=0)"
            if metrics.get("median_mfe_mae_ratio_state")
            == research_mfe_mae_efficiency.UNBOUNDED_ZERO_MAE
            else _format_number(metrics["median_mfe_mae_ratio"])
        )
        lines.append(
            "אי־סימטריה: דומיננטיות חיובית "
            f"{_format_number(metrics['favorable_dominance_rate_pct'], suffix='%')} | "
            f"MFE חציוני {_format_number(metrics['median_mfe_pct'], suffix='%')} | "
            f"MAE חציוני {_format_number(metrics['median_mae_pct'], suffix='%')} | "
            f"יחס MFE/MAE {efficiency_text}"
        )
    lines.extend(
        [
            "למה ההתראה עדיין ניסיונית:",
            *(f"• {reason}" for reason in payload["experimental_reasons"]),
            f"זמן החלטה: {payload['decision_time_utc']}",
            f"מזהה ניסויי: {payload['alert_id'][:16]}",
            "אין הרשאת LIVE ואין ביצוע מסחר אוטומטי.",
            EXPERIMENTAL_LABEL,
        ]
    )
    text = "\n".join(lines)
    if len(text) > MAX_TELEGRAM_TEXT_LENGTH:
        raise ValueError("experimental Telegram text exceeds the safe bound")
    return text


def descriptor() -> Dict[str, Any]:
    return {
        "alert_schema_version": ALERT_SCHEMA_VERSION,
        "compact_envelope_schema_version": COMPACT_ENVELOPE_SCHEMA_VERSION,
        "current_snapshot_commitment_version": (
            CURRENT_SNAPSHOT_COMMITMENT_VERSION
        ),
        "selection_policy_version": SELECTION_POLICY_VERSION,
        "renderer_version": RENDERER_VERSION,
        "experimental_label": EXPERIMENTAL_LABEL,
        "minimum_independent_movements": MIN_INDEPENDENT_MOVEMENTS,
        "max_current_snapshot_age_minutes": MAX_CURRENT_SNAPSHOT_AGE_MINUTES,
        "search_freshness_cadence_multiplier": (
            SEARCH_FRESHNESS_CADENCE_MULTIPLIER
        ),
        "alert_expiry_minutes": ALERT_EXPIRY_MINUTES,
        "max_telegram_text_length": MAX_TELEGRAM_TEXT_LENGTH,
        "current_decision_reads_outcomes": False,
        "database_effect": "NONE",
        "telegram_effect": "NONE",
        "formula_registry_effect": "NONE",
        "live_eligible": False,
        "trade_execution_allowed": False,
    }
