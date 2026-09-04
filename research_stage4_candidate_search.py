"""Pure bounded candidate search for authoritative Stage-4 observations.

This module deliberately owns no database, Formula registry, Shadow, Telegram,
LIVE, or trading boundary.  It searches only the decision-time feature map of
locally validated :class:`ExplorationObservation` objects.  Future path fields
are read only after a candidate's BTC-parent-wave occurrence membership has
been frozen.

An experimental candidate passes one atomic gate: the same pattern has at
least five completed, independent BTC parent market-movement occurrences and
those occurrences already pass the probability and/or movement-asymmetry
route.  Exact-binomial and multiple-testing values are disclosed at this early
stage, but do not silently turn that user-facing experimental gate into a
later, stricter research-acceptance gate.
"""

from __future__ import annotations

from collections.abc import Mapping as MappingABC
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import itertools
import json
import math
from statistics import median
import sys
import time
from typing import Any, Dict, Iterator, Mapping, Optional, Sequence

import research_formula_acceptance
import research_mfe_mae_efficiency
import research_no_dwell_outcome
import research_signal_formula_exploration as exploration


ENGINE_VERSION = "stage4-experimental-candidate-search-v2"
CANDIDATE_SCHEMA_VERSION = "stage4-experimental-candidate-v2"
LABEL_POLICY_VERSION = "stage4-static-no-dwell-favorable-movement-label-v1"
INDEPENDENCE_POLICY_VERSION = "stage4-btc-parent-first-opportunity-v1"
MULTIPLE_TESTING_POLICY_VERSION = "stage4-experimental-bh-disclosure-v1"
COMPACT_OBSERVATION_SCHEMA_VERSION = (
    "stage4-candidate-observation-compact-v1"
)
COMPACT_OBSERVATION_CHAIN_HASH_VERSION = (
    "stage4-candidate-observation-ordered-chain-v1"
)
MAX_OBSERVATIONS = 131_072
DEFAULT_SEARCH_WALL_BUDGET_MS = 60_000
MIN_SEARCH_WALL_BUDGET_MS = 5_000
MAX_SEARCH_WALL_BUDGET_MS = 300_000
OCCURRENCE_EVIDENCE_HASH_VERSION = "stage4-occurrence-evidence-chain-v1"
OCCURRENCE_AUDIT_SAMPLE_PER_STATUS = 2
OCCURRENCE_AUDIT_MEMBER_LIMIT = 8

# These are the historical route floors already enforced by
# research_formula_acceptance.  The Stage-4 route intentionally omits that
# policy's control-improvement, holdout, and current-relevance claims.  Those
# claims are outside this early within-pattern contract even when no-signal
# outcome carriers are available.
PROBABILITY_HIT_RATE_FLOOR_PCT = 60.0
PROBABILITY_WILSON_LOWER_FLOOR_PCT = 45.0
PROBABILITY_MIN_MFE_MAE_RATIO = 1.10
ASYMMETRY_DIRECTIONAL_HIT_RATE_FLOOR_PCT = 45.0
ASYMMETRY_DOMINANCE_RATE_FLOOR_PCT = 70.0
ASYMMETRY_WILSON_LOWER_FLOOR_PCT = 40.0
ASYMMETRY_MIN_MFE_MAE_RATIO = 2.0

_SUPPORTED_HORIZONS = frozenset(
    int(value)
    for value in research_no_dwell_outcome.BASE_FAVORABLE_WIDTH_PCT_BY_HORIZON
)
_BOOLEAN_FEATURES = tuple(
    feature
    for feature in exploration.ALLOWED_FEATURES
    if feature != exploration.FEATURE_COMBINED_VOTE_COUNT
)
_UTC = timezone.utc


@dataclass(frozen=True)
class Stage4SearchConfig:
    """Hard bounds and the single experimental evidence floor."""

    minimum_independent_occurrences: int = (
        exploration.EXPLORATION_MIN_BTC_PARENT_MOVEMENTS
    )
    max_observations: int = 32768
    max_conditions: int = 3
    max_candidates_evaluated: int = 256
    max_candidates_returned: int = 40
    wall_budget_ms: int = DEFAULT_SEARCH_WALL_BUDGET_MS


def _validated_config(value: Optional[Stage4SearchConfig]) -> Stage4SearchConfig:
    config = value or Stage4SearchConfig()
    if type(config.minimum_independent_occurrences) is not int or (
        config.minimum_independent_occurrences < 5
    ):
        raise ValueError("minimum_independent_occurrences cannot be below five")
    if type(config.max_observations) is not int or not (
        1 <= config.max_observations <= MAX_OBSERVATIONS
    ):
        raise ValueError(
            f"max_observations must be between 1 and {MAX_OBSERVATIONS}"
        )
    if type(config.max_conditions) is not int or not (1 <= config.max_conditions <= 3):
        raise ValueError("max_conditions must be between 1 and 3")
    if type(config.max_candidates_evaluated) is not int or not (
        1 <= config.max_candidates_evaluated <= 4096
    ):
        raise ValueError("max_candidates_evaluated must be between 1 and 4096")
    if type(config.max_candidates_returned) is not int or not (
        1 <= config.max_candidates_returned <= config.max_candidates_evaluated
    ):
        raise ValueError(
            "max_candidates_returned must be positive and within the search budget"
        )
    if type(config.wall_budget_ms) is not int or not (
        MIN_SEARCH_WALL_BUDGET_MS
        <= config.wall_budget_ms
        <= MAX_SEARCH_WALL_BUDGET_MS
    ):
        raise ValueError(
            "wall_budget_ms must be between "
            f"{MIN_SEARCH_WALL_BUDGET_MS} and {MAX_SEARCH_WALL_BUDGET_MS}"
        )
    return config


def _check_search_deadline(deadline: float) -> None:
    if time.monotonic() >= deadline:
        raise TimeoutError("Stage-4 candidate search wall budget exhausted")


def _utc(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field} is not an ISO timestamp") from exc
    else:
        raise ValueError(f"{field} is required")
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must be timezone-aware")
    return parsed.astimezone(_UTC)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def _fingerprint(kind: str, value: Any) -> str:
    return hashlib.sha256(f"{kind}:{_canonical_json(value)}".encode("utf-8")).hexdigest()


def _finite(value: Any, *, field: str) -> float:
    if value is None or isinstance(value, bool):
        raise ValueError(f"{field} must be finite")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field} must be finite") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field} must be finite")
    return number


def _round(value: Optional[float], digits: int = 6) -> Optional[float]:
    return None if value is None else round(float(value), digits)


def _wilson_lower_pct(successes: int, total: int, z: float = 1.96) -> Optional[float]:
    if total <= 0 or successes < 0 or successes > total:
        return None
    proportion = successes / total
    denominator = 1.0 + z * z / total
    centre = proportion + z * z / (2.0 * total)
    margin = z * math.sqrt(
        (proportion * (1.0 - proportion) + z * z / (4.0 * total)) / total
    )
    return max(0.0, (centre - margin) / denominator) * 100.0


def _one_sided_exact_binomial_p(successes: int, total: int) -> Optional[float]:
    """Return P[X >= successes] for X~Binomial(total, 0.5)."""

    if total <= 0 or successes < 0 or successes > total:
        return None
    numerator = sum(math.comb(total, value) for value in range(successes, total + 1))
    return min(1.0, numerator / (2**total))


def _bh_q_values(values: Sequence[Optional[float]]) -> list[Optional[float]]:
    indexed = [
        (index, float(value))
        for index, value in enumerate(values)
        if value is not None and math.isfinite(float(value)) and 0.0 <= float(value) <= 1.0
    ]
    if not indexed:
        return [None] * len(values)
    indexed.sort(key=lambda item: item[1])
    hypothesis_count = len(values)
    adjusted: Dict[int, float] = {}
    running = 1.0
    for reverse_index in range(len(indexed) - 1, -1, -1):
        original_index, p_value = indexed[reverse_index]
        rank = reverse_index + 1
        running = min(running, p_value * hypothesis_count / rank)
        adjusted[original_index] = min(1.0, running)
    return [adjusted.get(index) for index in range(len(values))]


@dataclass(frozen=True, slots=True)
class _CompactFeatureMapping(MappingABC[str, Any]):
    true_mask: int
    combined_vote_count: Optional[int]

    def __getitem__(self, key: str) -> Any:
        if key == exploration.FEATURE_COMBINED_VOTE_COUNT:
            return self.combined_vote_count
        try:
            index = _BOOLEAN_FEATURES.index(key)
        except ValueError as exc:
            raise KeyError(key) from exc
        return bool(self.true_mask & (1 << index))

    def __iter__(self) -> Iterator[str]:
        return iter(exploration.ALLOWED_FEATURES)

    def __len__(self) -> int:
        return len(exploration.ALLOWED_FEATURES)


@dataclass(frozen=True, slots=True)
class _CompactWaveBinding(MappingABC[str, Any]):
    status: str
    reason: Optional[str]
    btc_parent_movement_id: Optional[str]

    def __getitem__(self, key: str) -> Any:
        if key == "status":
            return self.status
        if key == "reason":
            return self.reason
        if key == "btc_parent_movement_id":
            return self.btc_parent_movement_id
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        return iter(("status", "reason", "btc_parent_movement_id"))

    def __len__(self) -> int:
        return 3


@dataclass(frozen=True, slots=True)
class _CompactOutcomePath(MappingABC[str, float]):
    directional_return_pct: float
    mfe_pct: float
    mae_pct: float

    def __getitem__(self, key: str) -> float:
        if key == "directional_return_pct":
            return self.directional_return_pct
        if key == "mfe_pct":
            return self.mfe_pct
        if key == "mae_pct":
            return self.mae_pct
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        return iter(("directional_return_pct", "mfe_pct", "mae_pct"))

    def __len__(self) -> int:
        return 3


@dataclass(frozen=True, slots=True)
class _CompactOutcome(MappingABC[str, Any]):
    status: str
    reason: Optional[str]
    horizon_minutes: Optional[int]
    path: Optional[_CompactOutcomePath]

    def __getitem__(self, key: str) -> Any:
        if key == "status":
            return self.status
        if key == "reason":
            return self.reason
        if key == "horizon_minutes":
            return self.horizon_minutes
        if key == "path" and self.path is not None:
            return self.path
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        keys = ["status", "reason", "horizon_minutes"]
        if self.path is not None:
            keys.append("path")
        return iter(keys)

    def __len__(self) -> int:
        return 4 if self.path is not None else 3


@dataclass(frozen=True, slots=True)
class CompactStage4CandidateObservation(MappingABC[str, Any]):
    observation_id: str
    projection_event_id: int
    projection_decision_time_utc: str
    symbol: str
    direction: str
    features: _CompactFeatureMapping
    wave_binding: _CompactWaveBinding
    outcome: _CompactOutcome

    def __getitem__(self, key: str) -> Any:
        if key == "_schema":
            return COMPACT_OBSERVATION_SCHEMA_VERSION
        if key == "observation_id":
            return self.observation_id
        if key == "projection_event_id":
            return self.projection_event_id
        if key == "projection_decision_time_utc":
            return self.projection_decision_time_utc
        if key == "symbol":
            return self.symbol
        if key == "direction":
            return self.direction
        if key == "features":
            return self.features
        if key == "wave_binding":
            return self.wave_binding
        if key == "outcome":
            return self.outcome
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        return iter(
            (
                "_schema",
                "observation_id",
                "projection_event_id",
                "projection_decision_time_utc",
                "symbol",
                "direction",
                "features",
                "wave_binding",
                "outcome",
            )
        )

    def __len__(self) -> int:
        return 9

    def hash_fields(self) -> tuple[Any, ...]:
        path = self.outcome.path
        return (
            COMPACT_OBSERVATION_SCHEMA_VERSION,
            self.observation_id,
            self.projection_event_id,
            self.projection_decision_time_utc,
            self.symbol,
            self.direction,
            self.features.true_mask,
            self.features.combined_vote_count,
            self.wave_binding.status,
            self.wave_binding.reason,
            self.wave_binding.btc_parent_movement_id,
            self.outcome.status,
            self.outcome.reason,
            self.outcome.horizon_minutes,
            None if path is None else path.directional_return_pct,
            None if path is None else path.mfe_pct,
            None if path is None else path.mae_pct,
        )


def compact_authoritative_observation(
    value: exploration.ExplorationObservation | Mapping[str, Any],
) -> CompactStage4CandidateObservation:
    """Keep only validated fields consumed by candidate search."""

    observation = (
        value
        if isinstance(value, exploration.ExplorationObservation)
        else exploration.ExplorationObservation.from_dict(value)
    )
    row = observation.to_dict()
    feature_values = row["features"]
    true_mask = sum(
        1 << index
        for index, name in enumerate(_BOOLEAN_FEATURES)
        if feature_values[name] is True
    )
    features = _CompactFeatureMapping(
        true_mask=true_mask,
        combined_vote_count=feature_values[
            exploration.FEATURE_COMBINED_VOTE_COUNT
        ],
    )
    binding = row["wave_binding"]
    outcome = row["outcome"]
    compact_path = None
    if outcome["status"] == "AVAILABLE":
        compact_path = _CompactOutcomePath(
            directional_return_pct=float(
                outcome["path"]["directional_return_pct"]
            ),
            mfe_pct=float(outcome["path"]["mfe_pct"]),
            mae_pct=float(outcome["path"]["mae_pct"]),
        )
    return CompactStage4CandidateObservation(
        observation_id=row["observation_id"],
        projection_event_id=row["projection_event_id"],
        projection_decision_time_utc=sys.intern(
            row["projection_decision_time_utc"]
        ),
        symbol=sys.intern(exploration._symbol(row["symbol"])),
        direction=sys.intern(row["direction"]),
        features=features,
        wave_binding=_CompactWaveBinding(
            status=sys.intern(binding["status"]),
            reason=(
                None
                if binding.get("reason") is None
                else sys.intern(str(binding["reason"]))
            ),
            btc_parent_movement_id=(
                None
                if binding.get("btc_parent_movement_id") is None
                else sys.intern(binding["btc_parent_movement_id"])
            ),
        ),
        outcome=_CompactOutcome(
            status=sys.intern(outcome["status"]),
            reason=(
                sys.intern(
                    str(outcome.get("reason") or "OUTCOME_UNAVAILABLE")
                )
                if outcome["status"] == "UNBOUND"
                else None
                if outcome.get("reason") is None
                else sys.intern(str(outcome["reason"]))
            ),
            horizon_minutes=outcome.get("horizon_minutes"),
            path=compact_path,
        ),
    )


def _is_sha256(value: Any) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def compact_observation_chain_sha256(
    values: Sequence[CompactStage4CandidateObservation],
) -> str:
    """Hash an ordered compact tuple without materializing one giant JSON."""

    if not isinstance(values, (list, tuple)):
        raise TypeError("compact Stage-4 observations must be a list or tuple")
    digest = hashlib.sha256()
    digest.update(COMPACT_OBSERVATION_CHAIN_HASH_VERSION.encode("ascii"))
    digest.update(b"\x00")
    for value in values:
        _update_compact_observation_chain_digest(digest, value)
    return digest.hexdigest()


def _update_compact_observation_chain_digest(
    digest: "hashlib._Hash", value: CompactStage4CandidateObservation
) -> None:
    if (
        type(value) is not CompactStage4CandidateObservation
        or type(value.features) is not _CompactFeatureMapping
        or type(value.wave_binding) is not _CompactWaveBinding
        or type(value.outcome) is not _CompactOutcome
        or (
            value.outcome.path is not None
            and type(value.outcome.path) is not _CompactOutcomePath
        )
    ):
        raise TypeError("compact Stage-4 observation type is invalid")
    encoded = _canonical_json(value.hash_fields()).encode("utf-8")
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)


def _coerce_compact_observation(
    value: CompactStage4CandidateObservation,
    *,
    horizon_minutes: int,
    analysis_as_of_utc: datetime,
) -> Mapping[str, Any]:
    if (
        type(value) is not CompactStage4CandidateObservation
        or type(value.features) is not _CompactFeatureMapping
        or type(value.wave_binding) is not _CompactWaveBinding
        or type(value.outcome) is not _CompactOutcome
        or (
            value.outcome.path is not None
            and type(value.outcome.path) is not _CompactOutcomePath
        )
    ):
        raise ValueError("compact Stage-4 observation type mismatch")
    if set(value) != {
        "_schema",
        "observation_id",
        "projection_event_id",
        "projection_decision_time_utc",
        "symbol",
        "direction",
        "features",
        "wave_binding",
        "outcome",
    }:
        raise ValueError("compact Stage-4 observation shape mismatch")
    if value.get("_schema") != COMPACT_OBSERVATION_SCHEMA_VERSION:
        raise ValueError("compact Stage-4 observation schema mismatch")
    if not _is_sha256(value.get("observation_id")):
        raise ValueError("compact Stage-4 observation_id is invalid")
    projection_event_id = value.get("projection_event_id")
    if type(projection_event_id) is not int or projection_event_id <= 0:
        raise ValueError("compact Stage-4 projection_event_id is invalid")
    decision = _utc(
        value.get("projection_decision_time_utc"),
        field="projection_decision_time_utc",
    )
    if decision > analysis_as_of_utc:
        raise ValueError("Stage-4 observation is after analysis_as_of_utc")
    symbol = value.get("symbol")
    try:
        canonical_symbol = exploration._symbol(symbol)
    except ValueError as exc:
        raise ValueError("compact Stage-4 symbol is invalid") from exc
    if canonical_symbol != symbol:
        raise ValueError("compact Stage-4 symbol is invalid")
    if value.get("direction") not in {"LONG", "SHORT"}:
        raise ValueError("compact Stage-4 direction is invalid")

    features = value.get("features")
    if (
        type(features) is not _CompactFeatureMapping
        or type(features.true_mask) is not int
        or not (0 <= features.true_mask < (1 << len(_BOOLEAN_FEATURES)))
    ):
        raise ValueError("compact Stage-4 feature shape mismatch")
    expanded = {name: features[name] for name in exploration.ALLOWED_FEATURES}
    if features.combined_vote_count is not None and (
        type(features.combined_vote_count) is not int
        or features.combined_vote_count not in {2, 3}
    ):
        raise ValueError("compact Stage-4 vote count is invalid")
    if expanded[exploration.FEATURE_MAX_PAIN_STRONG] and not expanded[
        exploration.FEATURE_MAX_PAIN_CONFIRMED
    ]:
        raise ValueError("compact strong Max-Pain lacks confirmation")
    if expanded[exploration.FEATURE_MAGNET_STRONG] and not expanded[
        exploration.FEATURE_MAGNET_CONFIRMED
    ]:
        raise ValueError("compact strong Magnet lacks confirmation")
    combined_present = expanded[exploration.FEATURE_COMBINED_CONFIRMED]
    combined_sources = sum(
        int(expanded[name])
        for name in (
            exploration.FEATURE_COMBINED_COINGLASS,
            exploration.FEATURE_COMBINED_PRICE_OI,
            exploration.FEATURE_COMBINED_FUTURES_CVD,
        )
    )
    combined_votes = expanded[exploration.FEATURE_COMBINED_VOTE_COUNT]
    if combined_present:
        if combined_votes not in {2, 3} or combined_votes != combined_sources:
            raise ValueError("compact Combined feature is inconsistent")
    elif combined_votes is not None or combined_sources:
        raise ValueError("compact absent Combined carries evidence")

    binding = value.get("wave_binding")
    if not isinstance(binding, Mapping) or set(binding) != {
        "status",
        "reason",
        "btc_parent_movement_id",
    }:
        raise ValueError("compact Stage-4 wave binding is invalid")
    parent_id = binding.get("btc_parent_movement_id")
    if binding.get("status") == "BOUND":
        if (
            not _is_sha256(parent_id)
            or binding.get("reason") is not None
        ):
            raise ValueError("compact Stage-4 parent binding is invalid")
    elif (
        binding.get("status") not in {"UNBOUND", "UNAVAILABLE"}
        or parent_id is not None
        or (
            binding.get("reason") is not None
            and type(binding.get("reason")) is not str
        )
    ):
        raise ValueError("compact Stage-4 unavailable binding is invalid")

    outcome = value.get("outcome")
    if not isinstance(outcome, Mapping):
        raise ValueError("compact Stage-4 outcome is invalid")
    outcome_status = outcome.get("status")
    compact_keys = {"status", "reason", "horizon_minutes"}
    if outcome_status == "AVAILABLE":
        if set(outcome) != compact_keys | {"path"}:
            raise ValueError("compact available outcome shape mismatch")
        if outcome.get("reason") is not None or (
            outcome.get("horizon_minutes") != horizon_minutes
        ):
            raise ValueError("compact available outcome is inconsistent")
        path = outcome.get("path")
        if not isinstance(path, Mapping) or set(path) != {
            "directional_return_pct",
            "mfe_pct",
            "mae_pct",
        }:
            raise ValueError("compact available outcome path is invalid")
        _finite(
            path.get("directional_return_pct"),
            field="directional_return_pct",
        )
        if _finite(path.get("mfe_pct"), field="mfe_pct") < 0.0 or (
            _finite(path.get("mae_pct"), field="mae_pct") < 0.0
        ):
            raise ValueError("compact available outcome path is invalid")
    elif outcome_status == "OUTCOME_UNAVAILABLE":
        if set(outcome) != compact_keys or (
            outcome.get("horizon_minutes") != horizon_minutes
        ):
            raise ValueError("compact unavailable outcome shape mismatch")
        if type(outcome.get("reason")) is not str or not outcome.get("reason"):
            raise ValueError("compact unavailable outcome reason is invalid")
    elif outcome_status == "UNBOUND":
        if set(outcome) != compact_keys or (
            outcome.get("horizon_minutes") is not None
        ):
            raise ValueError("compact unbound outcome shape mismatch")
        if type(outcome.get("reason")) is not str or not outcome.get("reason"):
            raise ValueError("compact unbound outcome reason is invalid")
    else:
        raise ValueError("compact Stage-4 outcome status is invalid")
    return value


def _coerce_observations(
    values: Sequence[exploration.ExplorationObservation | Mapping[str, Any]],
    *,
    horizon_minutes: int,
    analysis_as_of_utc: datetime,
    max_observations: int,
    deadline: Optional[float] = None,
) -> tuple[list[Mapping[str, Any]], str]:
    if not isinstance(values, (list, tuple)):
        raise TypeError("observations must be a bounded list or tuple")
    if len(values) > max_observations:
        raise ValueError("Stage-4 candidate input exceeds max_observations")
    rows: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    input_chain = hashlib.sha256()
    input_chain.update(COMPACT_OBSERVATION_CHAIN_HASH_VERSION.encode("ascii"))
    input_chain.update(b"\x00")
    for index, value in enumerate(values):
        if deadline is not None and index % 1024 == 0:
            _check_search_deadline(deadline)
        if type(value) is CompactStage4CandidateObservation:
            row = _coerce_compact_observation(
                value,
                horizon_minutes=horizon_minutes,
                analysis_as_of_utc=analysis_as_of_utc,
            )
            compact = value
        else:
            observation = (
                value
                if isinstance(value, exploration.ExplorationObservation)
                else exploration.ExplorationObservation.from_dict(value)
            )
            compact = compact_authoritative_observation(observation)
            row = _coerce_compact_observation(
                compact,
                horizon_minutes=horizon_minutes,
                analysis_as_of_utc=analysis_as_of_utc,
            )
        _update_compact_observation_chain_digest(input_chain, compact)
        observation_id = str(row["observation_id"])
        if observation_id in seen:
            raise ValueError("duplicate Stage-4 observation_id")
        seen.add(observation_id)
        decision = _utc(
            row["projection_decision_time_utc"],
            field="projection_decision_time_utc",
        )
        if decision > analysis_as_of_utc:
            raise ValueError("Stage-4 observation is after analysis_as_of_utc")
        outcome = row["outcome"]
        outcome_horizon = outcome.get("horizon_minutes")
        if outcome.get("status") != "UNBOUND" and outcome_horizon != horizon_minutes:
            raise ValueError("Stage-4 observation outcome horizon mismatch")
        rows.append(row)
    if deadline is not None:
        _check_search_deadline(deadline)
    rows.sort(
        key=lambda row: (
            row["projection_decision_time_utc"],
            row["projection_event_id"],
            row["symbol"],
            row["direction"],
            row["observation_id"],
        )
    )
    if deadline is not None:
        _check_search_deadline(deadline)
    return rows, input_chain.hexdigest()


def _predicate_catalog(
    rows: Sequence[Mapping[str, Any]], *, deadline: Optional[float] = None
) -> list[Dict[str, Any]]:
    predicates: list[Dict[str, Any]] = []
    for feature in _BOOLEAN_FEATURES:
        if deadline is not None:
            _check_search_deadline(deadline)
        if any(row["features"].get(feature) is True for row in rows):
            predicates.append(
                {"feature": feature, "operator": "==", "value": True}
            )
    vote_feature = exploration.FEATURE_COMBINED_VOTE_COUNT
    vote_values = {
        row["features"].get(vote_feature)
        for row in rows
        if type(row["features"].get(vote_feature)) is int
    }
    for threshold in (2, 3):
        if deadline is not None:
            _check_search_deadline(deadline)
        if any(value >= threshold for value in vote_values):
            predicates.append(
                {"feature": vote_feature, "operator": ">=", "value": threshold}
            )
    return sorted(
        predicates,
        key=lambda item: (
            item["feature"],
            item["operator"],
            _canonical_json(item["value"]),
        ),
    )


def _candidate_specifications(
    predicates: Sequence[Mapping[str, Any]],
    *,
    max_conditions: int,
) -> Iterator[
    Optional[tuple[list[Dict[str, Any]], Mapping[str, Any], str]]
]:
    """Yield each valid condition/direction once; ``None`` is one rejection.

    Flattening depth, condition, and direction traversal gives the caller one
    explicit budget stop.  Family validation remains condition-set scoped, so
    a rejected set is not accidentally counted once per direction.
    """

    for depth in range(1, min(max_conditions, len(predicates)) + 1):
        for raw_conditions in itertools.combinations(predicates, depth):
            conditions = sorted(
                (dict(condition) for condition in raw_conditions),
                key=lambda item: (
                    item["feature"],
                    item["operator"],
                    _canonical_json(item["value"]),
                ),
            )
            try:
                family_policy = exploration.validate_candidate_feature_set(
                    [condition["feature"] for condition in conditions]
                )
            except ValueError:
                yield None
                continue
            for direction in ("LONG", "SHORT"):
                yield conditions, family_policy, direction


def _condition_matches(
    features: Mapping[str, Any], condition: Mapping[str, Any]
) -> bool:
    feature = str(condition["feature"])
    if feature not in features:
        return False
    actual = features[feature]
    operator = condition["operator"]
    expected = condition["value"]
    if operator == "==":
        return type(actual) is type(expected) and actual == expected
    if operator == ">=":
        return (
            not isinstance(actual, bool)
            and isinstance(actual, (int, float))
            and math.isfinite(float(actual))
            and float(actual) >= float(expected)
        )
    raise ValueError("unsupported Stage-4 predicate operator")


def _conditions_match(
    row: Mapping[str, Any], conditions: Sequence[Mapping[str, Any]]
) -> bool:
    features = row["features"]
    return all(_condition_matches(features, condition) for condition in conditions)


def _freeze_occurrence_membership(
    matches: Sequence[Mapping[str, Any]],
    *,
    deadline: Optional[float] = None,
) -> Dict[str, Any]:
    """Freeze first opportunity cohorts without reading any outcome field."""

    grouped: Dict[str, list[Mapping[str, Any]]] = {}
    unbound: list[str] = []
    for index, row in enumerate(matches):
        if deadline is not None and index % 1024 == 0:
            _check_search_deadline(deadline)
        binding = row["wave_binding"]
        if binding.get("status") != "BOUND" or not binding.get(
            "btc_parent_movement_id"
        ):
            unbound.append(str(row["observation_id"]))
            continue
        grouped.setdefault(str(binding["btc_parent_movement_id"]), []).append(row)

    occurrences: list[Dict[str, Any]] = []
    for parent_index, (parent_id, members) in enumerate(sorted(grouped.items())):
        if deadline is not None and parent_index % 256 == 0:
            _check_search_deadline(deadline)
        earliest: Optional[datetime] = None
        parsed_times: list[tuple[Mapping[str, Any], datetime]] = []
        for member_index, row in enumerate(members):
            if deadline is not None and member_index % 1024 == 0:
                _check_search_deadline(deadline)
            parsed = _utc(
                row["projection_decision_time_utc"],
                field="projection_decision_time_utc",
            )
            parsed_times.append((row, parsed))
            if earliest is None or parsed < earliest:
                earliest = parsed
        if earliest is None:
            raise ValueError("Stage-4 occurrence has no members")
        earliest_rows = [row for row, parsed in parsed_times if parsed == earliest]
        # A duplicate projection for the same symbol must not give that symbol
        # extra weight.  The stable observation id is selected before outcomes.
        by_symbol: Dict[str, Mapping[str, Any]] = {}
        for member_index, row in enumerate(
            sorted(earliest_rows, key=lambda item: item["observation_id"])
        ):
            if deadline is not None and member_index % 1024 == 0:
                _check_search_deadline(deadline)
            by_symbol.setdefault(str(row["symbol"]), row)
        evidence_rows = [by_symbol[symbol] for symbol in sorted(by_symbol)]
        occurrences.append(
            {
                "btc_parent_movement_id": parent_id,
                "first_match_time_utc": earliest.isoformat(),
                "evidence_observation_ids": [
                    str(row["observation_id"]) for row in evidence_rows
                ],
                "evidence_symbols": [str(row["symbol"]) for row in evidence_rows],
                "observed_match_observation_ids": sorted(
                    str(row["observation_id"]) for row in members
                ),
                # Kept private until membership for every occurrence is frozen.
                "_evidence_rows": evidence_rows,
            }
        )
    return {
        "occurrences": occurrences,
        "unbound_observation_ids": sorted(unbound),
    }


def _label_frozen_occurrences(
    frozen: Mapping[str, Any],
    *,
    horizon_minutes: int,
    analysis_as_of_utc: datetime,
    deadline: Optional[float] = None,
) -> Dict[str, Any]:
    threshold = research_no_dwell_outcome.base_favorable_width_pct(
        horizon_minutes
    )
    completed: list[Dict[str, Any]] = []
    pending: list[Dict[str, Any]] = []
    unavailable: list[Dict[str, Any]] = []
    for index, frozen_occurrence in enumerate(frozen["occurrences"]):
        if deadline is not None and index % 256 == 0:
            _check_search_deadline(deadline)
        occurrence = {
            key: value
            for key, value in frozen_occurrence.items()
            if not str(key).startswith("_")
        }
        start = _utc(
            occurrence["first_match_time_utc"], field="first_match_time_utc"
        )
        if start + timedelta(minutes=horizon_minutes) > analysis_as_of_utc:
            pending.append({**occurrence, "status": "PENDING_HORIZON"})
            continue
        rows = list(frozen_occurrence["_evidence_rows"])
        if any(row["outcome"].get("status") != "AVAILABLE" for row in rows):
            reasons = sorted(
                {
                    str(row["outcome"].get("reason") or "OUTCOME_UNAVAILABLE")
                    for row in rows
                    if row["outcome"].get("status") != "AVAILABLE"
                }
            )
            unavailable.append(
                {
                    **occurrence,
                    "status": "OUTCOME_UNAVAILABLE",
                    "reasons": reasons,
                }
            )
            continue

        paths = [row["outcome"]["path"] for row in rows]
        mfe = [_finite(path.get("mfe_pct"), field="mfe_pct") for path in paths]
        mae = [_finite(path.get("mae_pct"), field="mae_pct") for path in paths]
        directional = [
            _finite(path.get("directional_return_pct"), field="directional_return_pct")
            for path in paths
        ]
        hit_flags = [value >= threshold for value in mfe]
        paired_edges = [favorable - adverse for favorable, adverse in zip(mfe, mae)]
        dominance_flags = [value > 0.0 for value in paired_edges]
        favorable_move_hit = sum(hit_flags) > len(hit_flags) / 2.0
        favorable_dominance = sum(dominance_flags) > len(dominance_flags) / 2.0
        completed.append(
            {
                **occurrence,
                "status": "COMPLETED",
                "label_policy_version": LABEL_POLICY_VERSION,
                "qualifying_favorable_move_pct": threshold,
                "favorable_move_hit": favorable_move_hit,
                "favorable_move_member_hits": sum(hit_flags),
                "favorable_move_member_count": len(hit_flags),
                "favorable_dominance": favorable_dominance,
                "favorable_dominance_member_hits": sum(dominance_flags),
                "median_directional_return_pct": _round(median(directional)),
                "median_mfe_pct": _round(median(mfe)),
                "median_mae_pct": _round(median(mae)),
                "adverse_tail_mae_pct": _round(max(mae)),
                "median_paired_favorable_minus_adverse_pct": _round(
                    median(paired_edges)
                ),
                "survival_or_dwell_required": False,
            }
        )
    return {
        "completed": completed,
        "pending": pending,
        "unavailable": unavailable,
        "unbound_observation_ids": list(frozen["unbound_observation_ids"]),
        "qualifying_favorable_move_pct": threshold,
    }


def _update_structured_digest(
    digest: "hashlib._Hash",
    value: Any,
    *,
    deadline: Optional[float],
    visited: list[int],
) -> None:
    """Hash nested audit evidence without constructing one giant JSON value."""

    visited[0] += 1
    if deadline is not None and visited[0] % 1024 == 0:
        _check_search_deadline(deadline)
    if isinstance(value, Mapping):
        digest.update(b"M")
        keys = sorted(str(key) for key in value)
        digest.update(len(keys).to_bytes(8, "big"))
        for key in keys:
            _update_structured_digest(
                digest, key, deadline=deadline, visited=visited
            )
            _update_structured_digest(
                digest, value[key], deadline=deadline, visited=visited
            )
        return
    if isinstance(value, (list, tuple)):
        digest.update(b"L")
        digest.update(len(value).to_bytes(8, "big"))
        for item in value:
            _update_structured_digest(
                digest, item, deadline=deadline, visited=visited
            )
        return
    encoded = _canonical_json(value).encode("utf-8")
    digest.update(b"S")
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)


def _occurrence_evidence_sha256(
    labeled: Mapping[str, Any], *, deadline: Optional[float]
) -> str:
    digest = hashlib.sha256()
    digest.update(OCCURRENCE_EVIDENCE_HASH_VERSION.encode("ascii"))
    digest.update(b"\x00")
    evidence = {
        "completed": labeled["completed"],
        "pending": labeled["pending"],
        "unavailable": labeled["unavailable"],
        "unbound_observation_ids": labeled["unbound_observation_ids"],
        "qualifying_favorable_move_pct": labeled[
            "qualifying_favorable_move_pct"
        ],
    }
    _update_structured_digest(
        digest, evidence, deadline=deadline, visited=[0]
    )
    if deadline is not None:
        _check_search_deadline(deadline)
    return digest.hexdigest()


def _bounded_occurrence_audit_item(value: Mapping[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    truncated_fields: list[str] = []
    for key, item in value.items():
        if isinstance(item, (list, tuple)):
            result[key] = list(item[:OCCURRENCE_AUDIT_MEMBER_LIMIT])
            result[f"{key}_count"] = len(item)
            if len(item) > OCCURRENCE_AUDIT_MEMBER_LIMIT:
                truncated_fields.append(str(key))
        else:
            result[key] = item
    result["truncated_fields"] = sorted(truncated_fields)
    return result


def _bounded_occurrence_audit_sample(
    labeled: Mapping[str, Any]
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "sample_per_status": OCCURRENCE_AUDIT_SAMPLE_PER_STATUS,
        "member_limit_per_field": OCCURRENCE_AUDIT_MEMBER_LIMIT,
    }
    for source_name, output_name in (
        ("completed", "completed"),
        ("pending", "pending_horizon"),
        ("unavailable", "mature_outcome_unavailable"),
    ):
        items = labeled[source_name]
        result[output_name] = [
            _bounded_occurrence_audit_item(item)
            for item in items[:OCCURRENCE_AUDIT_SAMPLE_PER_STATUS]
        ]
        result[f"{output_name}_count"] = len(items)
    unbound = labeled["unbound_observation_ids"]
    result["wave_unbound_observation_ids"] = list(
        unbound[:OCCURRENCE_AUDIT_MEMBER_LIMIT]
    )
    result["wave_unbound_observation_ids_count"] = len(unbound)
    return result


def _route_metrics(
    labeled: Mapping[str, Any], *, minimum_occurrences: int,
    deadline: Optional[float] = None,
) -> Dict[str, Any]:
    if deadline is not None:
        _check_search_deadline(deadline)
    completed = list(labeled["completed"])
    sample_size = len(completed)
    successes = sum(item["favorable_move_hit"] is True for item in completed)
    dominance_successes = sum(
        item["favorable_dominance"] is True for item in completed
    )
    hit_rate = successes / sample_size * 100.0 if sample_size else None
    dominance_rate = (
        dominance_successes / sample_size * 100.0 if sample_size else None
    )
    hit_wilson = _wilson_lower_pct(successes, sample_size)
    dominance_wilson = _wilson_lower_pct(dominance_successes, sample_size)
    median_mfe = (
        median(float(item["median_mfe_pct"]) for item in completed)
        if completed
        else None
    )
    median_mae = (
        median(float(item["median_mae_pct"]) for item in completed)
        if completed
        else None
    )
    paired_edge = (
        median(
            float(item["median_paired_favorable_minus_adverse_pct"])
            for item in completed
        )
        if completed
        else None
    )
    adverse_tail = (
        max(float(item["adverse_tail_mae_pct"]) for item in completed)
        if completed
        else None
    )
    efficiency = research_mfe_mae_efficiency.classify(median_mfe, median_mae)
    coverage_complete = not labeled["unavailable"] and not labeled[
        "unbound_observation_ids"
    ]
    common = {
        "minimum independent BTC parent occurrences": sample_size
        >= minimum_occurrences,
        "mature matched occurrence coverage complete": coverage_complete,
        "wide favorable movement floor": bool(
            median_mfe is not None
            and median_mfe >= labeled["qualifying_favorable_move_pct"]
        ),
    }
    probability = {
        "hit rate": bool(
            hit_rate is not None and hit_rate >= PROBABILITY_HIT_RATE_FLOOR_PCT
        ),
        "Wilson lower bound": bool(
            hit_wilson is not None
            and hit_wilson >= PROBABILITY_WILSON_LOWER_FLOOR_PCT
        ),
        "minimum favorable/adverse efficiency": efficiency.meets_threshold(
            PROBABILITY_MIN_MFE_MAE_RATIO
        ),
    }
    asymmetry = {
        "minimum directional hit rate": bool(
            hit_rate is not None
            and hit_rate >= ASYMMETRY_DIRECTIONAL_HIT_RATE_FLOOR_PCT
        ),
        "favorable dominance rate": bool(
            dominance_rate is not None
            and dominance_rate >= ASYMMETRY_DOMINANCE_RATE_FLOOR_PCT
        ),
        "favorable dominance Wilson lower bound": bool(
            dominance_wilson is not None
            and dominance_wilson >= ASYMMETRY_WILSON_LOWER_FLOOR_PCT
        ),
        "strong favorable/adverse efficiency": efficiency.meets_threshold(
            ASYMMETRY_MIN_MFE_MAE_RATIO
        ),
        "positive paired favorable/adverse edge": bool(
            paired_edge is not None and paired_edge > 0.0
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
    result = {
        "sample_size": sample_size,
        "successes": successes,
        "hit_rate_pct": _round(hit_rate, 4),
        "wilson_95_lower_pct": _round(hit_wilson, 4),
        "favorable_dominance_successes": dominance_successes,
        "favorable_dominance_rate_pct": _round(dominance_rate, 4),
        "favorable_dominance_wilson_95_lower_pct": _round(
            dominance_wilson, 4
        ),
        "median_mfe_pct": _round(median_mfe),
        "median_mae_pct": _round(median_mae),
        "adverse_tail_mae_pct": _round(adverse_tail),
        "median_paired_favorable_minus_adverse_pct": _round(paired_edge),
        "median_mfe_mae_ratio": _round(efficiency.ratio),
        "median_mfe_mae_ratio_state": efficiency.state,
        "probability_exact_binomial_p_value": _round(
            _one_sided_exact_binomial_p(successes, sample_size), 8
        ),
        "asymmetry_exact_binomial_p_value": _round(
            _one_sided_exact_binomial_p(dominance_successes, sample_size), 8
        ),
        "common_gates": common,
        "routes": {
            "PROBABILITY": {"passed": probability_passed, "gates": probability},
            "ASYMMETRY": {"passed": asymmetry_passed, "gates": asymmetry},
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
    if deadline is not None:
        _check_search_deadline(deadline)
    return result


def _candidate_key(
    *, direction: str, horizon_minutes: int, conditions: Sequence[Mapping[str, Any]]
) -> str:
    return _fingerprint(
        "stage4-experimental-candidate",
        {
            "engine_version": ENGINE_VERSION,
            "candidate_schema_version": CANDIDATE_SCHEMA_VERSION,
            "feature_schema_version": exploration.FEATURE_SCHEMA_VERSION,
            "label_policy_version": LABEL_POLICY_VERSION,
            "independence_policy_version": INDEPENDENCE_POLICY_VERSION,
            "direction": direction,
            "horizon_minutes": horizon_minutes,
            "conditions": list(conditions),
        },
    )


def candidate_key_sha256(
    *,
    direction: str,
    horizon_minutes: int,
    conditions: Sequence[Mapping[str, Any]],
) -> str:
    """Return the version-bound identity of one candidate formula."""

    return _candidate_key(
        direction=direction,
        horizon_minutes=horizon_minutes,
        conditions=conditions,
    )


def candidate_search_receipt_sha256(value: Mapping[str, Any]) -> str:
    """Hash the complete unsigned candidate-search result."""

    if not isinstance(value, Mapping):
        raise TypeError("candidate search receipt must be a mapping")
    unsigned = dict(value)
    unsigned.pop("search_receipt_sha256", None)
    return _fingerprint("stage4-candidate-search-receipt", unsigned)


def _candidate_formula_text(
    direction: str, conditions: Sequence[Mapping[str, Any]]
) -> str:
    return f"{direction} WHEN " + " AND ".join(
        f"{condition['feature']} {condition['operator']} "
        f"{json.dumps(condition['value'], ensure_ascii=False)}"
        for condition in conditions
    )


def _candidate_sort_key(candidate: Mapping[str, Any]) -> tuple[Any, ...]:
    metrics = candidate["metrics"]
    return (
        int(candidate["experimental_formula_eligible"]),
        len(metrics["accepted_paths"]),
        float(metrics.get("wilson_95_lower_pct") or 0.0),
        float(metrics.get("favorable_dominance_wilson_95_lower_pct") or 0.0),
        int(metrics.get("sample_size") or 0),
        -len(candidate["conditions"]),
        str(candidate["candidate_key"]),
    )


def search_experimental_candidates(
    observations: Sequence[
        exploration.ExplorationObservation
        | CompactStage4CandidateObservation
        | Mapping[str, Any]
    ],
    *,
    horizon_minutes: int,
    analysis_as_of_utc: Any,
    config: Optional[Stage4SearchConfig] = None,
) -> Dict[str, Any]:
    """Search one bounded complete Stage-4 corpus without downstream authority."""

    if type(horizon_minutes) is not int or horizon_minutes not in _SUPPORTED_HORIZONS:
        raise ValueError("unsupported Stage-4 candidate horizon")
    active = _validated_config(config)
    deadline = time.monotonic() + active.wall_budget_ms / 1000.0
    as_of = _utc(analysis_as_of_utc, field="analysis_as_of_utc")
    rows, input_observation_chain_sha256 = _coerce_observations(
        observations,
        horizon_minutes=horizon_minutes,
        analysis_as_of_utc=as_of,
        max_observations=active.max_observations,
        deadline=deadline,
    )
    predicates = _predicate_catalog(rows, deadline=deadline)
    evaluated: list[Dict[str, Any]] = []
    family_policy_rejections = 0
    empty_match_rejections = 0
    search_budget_exhausted = False
    evidence_by_match_set: Dict[str, Dict[str, Any]] = {}

    for specification in _candidate_specifications(
        predicates,
        max_conditions=active.max_conditions,
    ):
        _check_search_deadline(deadline)
        if len(evaluated) >= active.max_candidates_evaluated:
            search_budget_exhausted = True
            break
        if specification is None:
            family_policy_rejections += 1
            continue
        conditions, family_policy, direction = specification
        matches: list[Mapping[str, Any]] = []
        for row_index, row in enumerate(rows):
            if row_index % 1024 == 0:
                _check_search_deadline(deadline)
            if row["direction"] == direction and _conditions_match(
                row, conditions
            ):
                matches.append(row)
        if not matches:
            empty_match_rejections += 1
            continue
        raw_match_ids = sorted(str(row["observation_id"]) for row in matches)
        match_set_sha = _fingerprint(
            "stage4-candidate-match-set",
            {"direction": direction, "observation_ids": raw_match_ids},
        )
        evidence = evidence_by_match_set.get(match_set_sha)
        if evidence is None:
            # This boundary is intentional: occurrence membership is complete
            # before _label_frozen_occurrences can inspect future path outcomes.
            frozen = _freeze_occurrence_membership(matches, deadline=deadline)
            labeled = _label_frozen_occurrences(
                frozen,
                horizon_minutes=horizon_minutes,
                analysis_as_of_utc=as_of,
                deadline=deadline,
            )
            metrics = _route_metrics(
                labeled,
                minimum_occurrences=active.minimum_independent_occurrences,
                deadline=deadline,
            )
            evidence = {
                "occurrence_evidence_sha256": _occurrence_evidence_sha256(
                    labeled, deadline=deadline
                ),
                "occurrence_counts": {
                    "independent_parent_movements_seen": len(
                        frozen["occurrences"]
                    ),
                    "completed": len(labeled["completed"]),
                    "pending_horizon": len(labeled["pending"]),
                    "mature_outcome_unavailable": len(
                        labeled["unavailable"]
                    ),
                    "wave_unbound_matches": len(
                        labeled["unbound_observation_ids"]
                    ),
                },
                "occurrence_audit_sample": (
                    _bounded_occurrence_audit_sample(labeled)
                ),
                "metrics": metrics,
            }
            evidence_by_match_set[match_set_sha] = evidence
        metrics = evidence["metrics"]
        _check_search_deadline(deadline)
        candidate_key = _candidate_key(
            direction=direction,
            horizon_minutes=horizon_minutes,
            conditions=conditions,
        )
        evaluated.append(
            {
                "candidate_key": candidate_key,
                "candidate_schema_version": CANDIDATE_SCHEMA_VERSION,
                "engine_version": ENGINE_VERSION,
                "feature_schema_version": exploration.FEATURE_SCHEMA_VERSION,
                "label_policy_version": LABEL_POLICY_VERSION,
                "independence_policy_version": INDEPENDENCE_POLICY_VERSION,
                "direction": direction,
                "horizon_minutes": horizon_minutes,
                "conditions": conditions,
                "formula_text": _candidate_formula_text(direction, conditions),
                "condition_source_closure": family_policy["source_closure"],
                "condition_evidence_sources": family_policy[
                    "deduplicated_sources"
                ],
                "raw_match_count": len(matches),
                "match_set_sha256": match_set_sha,
                "occurrence_evidence_sha256": evidence[
                    "occurrence_evidence_sha256"
                ],
                "occurrence_counts": evidence["occurrence_counts"],
                "occurrence_audit_sample": evidence[
                    "occurrence_audit_sample"
                ],
                "metrics": metrics,
                "accepted_paths": list(metrics["accepted_paths"]),
                "experimental_formula_eligible": metrics[
                    "experimental_formula_eligible"
                ],
                "eligibility_gate": {
                    "atomic": True,
                    "minimum_independent_occurrences": (
                        active.minimum_independent_occurrences
                    ),
                    "independence_unit": "DISTINCT_BTC_PARENT_MARKET_MOVEMENT",
                    "passed": metrics["experimental_formula_eligible"],
                    "separate_later_probability_gate": False,
                },
                "controls_evaluated": False,
                "holdout_evaluated": False,
                "experimental_caveats": [
                    "NO_CONTROL_RELATIVE_CLAIM",
                    "NO_HOLDOUT_CLAIM",
                    "MULTIPLE_TESTING_DISCLOSURE_NOT_CONFIRMATORY_GATE",
                ],
                "formula_registry_effect": "NONE",
                "authority_effect": "NONE",
                "delivery_channel": "NONE",
                "live_eligible": False,
                "telegram_delivery_allowed": False,
                "trade_execution_allowed": False,
            }
        )

    _check_search_deadline(deadline)
    probability_p = [
        candidate["metrics"].get("probability_exact_binomial_p_value")
        for candidate in evaluated
    ]
    asymmetry_p = [
        candidate["metrics"].get("asymmetry_exact_binomial_p_value")
        for candidate in evaluated
    ]
    q_values = _bh_q_values([*probability_p, *asymmetry_p])
    split = len(evaluated)
    for index, candidate in enumerate(evaluated):
        if index % 256 == 0:
            _check_search_deadline(deadline)
        candidate["multiple_testing"] = {
            "policy_version": MULTIPLE_TESTING_POLICY_VERSION,
            "method": "BENJAMINI_HOCHBERG_JOINT_PROBABILITY_ASYMMETRY_DIRECTIONS",
            "hypotheses_in_family": len(q_values),
            "probability_q_value": _round(q_values[index], 8),
            "asymmetry_q_value": _round(q_values[split + index], 8),
            "decision_effect": "DISCLOSURE_ONLY_EXPERIMENTAL",
            "eligibility_changed": False,
        }

    # Equivalent frozen observation sets are one display result.  Every
    # searched condition set nevertheless remains in the BH disclosure family.
    by_match_set: Dict[str, list[Dict[str, Any]]] = {}
    for index, candidate in enumerate(evaluated):
        if index % 256 == 0:
            _check_search_deadline(deadline)
        by_match_set.setdefault(candidate["match_set_sha256"], []).append(candidate)
    displayed: list[Dict[str, Any]] = []
    duplicate_candidates_collapsed = 0
    for index, group in enumerate(by_match_set.values()):
        if index % 256 == 0:
            _check_search_deadline(deadline)
        group.sort(
            key=lambda item: (
                len(item["conditions"]),
                _canonical_json(item["conditions"]),
                item["candidate_key"],
            )
        )
        champion = group[0]
        duplicate_candidates_collapsed += len(group) - 1
        champion["display_equivalent_candidates"] = len(group)
        champion["display_equivalent_candidate_keys"] = sorted(
            item["candidate_key"] for item in group
        )
        displayed.append(champion)
    displayed.sort(key=_candidate_sort_key, reverse=True)
    displayed = displayed[: active.max_candidates_returned]
    eligible = [
        candidate
        for candidate in displayed
        if candidate["experimental_formula_eligible"]
    ]
    result_status = (
        "EMPTY_CORPUS"
        if not rows
        else "ELIGIBLE_EXPERIMENTAL_CANDIDATES_FOUND"
        if eligible
        else "NO_ELIGIBLE_EXPERIMENTAL_CANDIDATES"
    )
    output = {
        "available": bool(rows),
        "status": result_status,
        "engine_version": ENGINE_VERSION,
        "candidate_schema_version": CANDIDATE_SCHEMA_VERSION,
        "feature_schema_version": exploration.FEATURE_SCHEMA_VERSION,
        "label_policy_version": LABEL_POLICY_VERSION,
        "independence_policy_version": INDEPENDENCE_POLICY_VERSION,
        "multiple_testing_policy_version": MULTIPLE_TESTING_POLICY_VERSION,
        "compact_observation_schema_version": (
            COMPACT_OBSERVATION_SCHEMA_VERSION
        ),
        "historical_threshold_source_policy_version": (
            research_formula_acceptance.POLICY_VERSION
        ),
        "analysis_as_of_utc": as_of.isoformat(),
        "horizon_minutes": horizon_minutes,
        "input_observation_schema_version": (
            COMPACT_OBSERVATION_SCHEMA_VERSION
        ),
        "input_observation_hash_contract_version": (
            COMPACT_OBSERVATION_CHAIN_HASH_VERSION
        ),
        "input_observation_count": len(rows),
        "input_observation_chain_sha256": (
            input_observation_chain_sha256
        ),
        "config": asdict(active),
        "qualifying_favorable_move_pct": (
            research_no_dwell_outcome.base_favorable_width_pct(horizon_minutes)
        ),
        "counts": {
            "observations": len(rows),
            "predicates": len(predicates),
            "candidates_evaluated": len(evaluated),
            "evidence_match_sets_evaluated": len(evidence_by_match_set),
            "display_candidates": len(displayed),
            "display_equivalent_candidates_collapsed": (
                duplicate_candidates_collapsed
            ),
            "eligible_experimental_candidates": len(eligible),
            "family_policy_rejections": family_policy_rejections,
            "empty_direction_match_rejections": empty_match_rejections,
            "hypotheses_disclosed": len(q_values),
        },
        "search_budget_exhausted": search_budget_exhausted,
        "candidates": displayed,
        "eligible_candidates": eligible,
        "atomic_eligibility": {
            "minimum_independent_occurrences": active.minimum_independent_occurrences,
            "independence_unit": "DISTINCT_BTC_PARENT_MARKET_MOVEMENT",
            "requirement": (
                "the same pattern has the minimum completed independent "
                "occurrences and those outcomes already pass probability "
                "and/or favorable movement asymmetry"
            ),
            "separate_later_probability_gate": False,
        },
        "statistical_scope": {
            "controls_evaluated": False,
            "holdout_evaluated": False,
            "multiple_testing_effect": "DISCLOSURE_ONLY_EXPERIMENTAL",
            "claim": "WITHIN_PATTERN_EXPERIMENTAL_EVIDENCE_ONLY",
        },
        "ready_for_candidate_search": True,
        "formula_registry_effect": "NONE",
        "authority_effect": "NONE",
        "delivery_channel": "NONE",
        "live_eligible": False,
        "telegram_delivery_allowed": False,
        "trade_execution_allowed": False,
    }
    output["search_receipt_sha256"] = candidate_search_receipt_sha256(output)
    _check_search_deadline(deadline)
    return output


def descriptor() -> Dict[str, Any]:
    return {
        "engine_version": ENGINE_VERSION,
        "candidate_schema_version": CANDIDATE_SCHEMA_VERSION,
        "compact_observation_schema_version": (
            COMPACT_OBSERVATION_SCHEMA_VERSION
        ),
        "compact_observation_chain_hash_version": (
            COMPACT_OBSERVATION_CHAIN_HASH_VERSION
        ),
        "occurrence_evidence_hash_version": OCCURRENCE_EVIDENCE_HASH_VERSION,
        "feature_schema_version": exploration.FEATURE_SCHEMA_VERSION,
        "label_policy_version": LABEL_POLICY_VERSION,
        "independence_policy_version": INDEPENDENCE_POLICY_VERSION,
        "multiple_testing_policy_version": MULTIPLE_TESTING_POLICY_VERSION,
        "minimum_independent_occurrences": (
            exploration.EXPLORATION_MIN_BTC_PARENT_MOVEMENTS
        ),
        "independence_unit": "DISTINCT_BTC_PARENT_MARKET_MOVEMENT",
        "fixed_time_spacing_rule": None,
        "label": (
            "MFE reaches the versioned static horizon width; no dwell or "
            "survival requirement"
        ),
        "probability_floors": {
            "hit_rate_pct": PROBABILITY_HIT_RATE_FLOOR_PCT,
            "wilson_95_lower_pct": PROBABILITY_WILSON_LOWER_FLOOR_PCT,
            "mfe_mae_ratio": PROBABILITY_MIN_MFE_MAE_RATIO,
        },
        "asymmetry_floors": {
            "directional_hit_rate_pct": (
                ASYMMETRY_DIRECTIONAL_HIT_RATE_FLOOR_PCT
            ),
            "dominance_rate_pct": ASYMMETRY_DOMINANCE_RATE_FLOOR_PCT,
            "dominance_wilson_95_lower_pct": (
                ASYMMETRY_WILSON_LOWER_FLOOR_PCT
            ),
            "mfe_mae_ratio": ASYMMETRY_MIN_MFE_MAE_RATIO,
            "paired_edge": "POSITIVE",
        },
        "outcome_fields_allowed_as_predicates": False,
        "default_max_observations": Stage4SearchConfig().max_observations,
        "max_observations_hard_limit": MAX_OBSERVATIONS,
        "default_wall_budget_ms": DEFAULT_SEARCH_WALL_BUDGET_MS,
        "minimum_wall_budget_ms": MIN_SEARCH_WALL_BUDGET_MS,
        "maximum_wall_budget_ms": MAX_SEARCH_WALL_BUDGET_MS,
        "occurrence_audit_sample_per_status": (
            OCCURRENCE_AUDIT_SAMPLE_PER_STATUS
        ),
        "occurrence_audit_member_limit": OCCURRENCE_AUDIT_MEMBER_LIMIT,
        "max_conditions": 3,
        "max_candidates_evaluated": 256,
        "multiple_testing_effect": "DISCLOSURE_ONLY_EXPERIMENTAL",
        "control_relative_claim": False,
        "holdout_claim": False,
        "formula_registry_effect": "NONE",
        "authority_effect": "NONE",
        "delivery_channel": "NONE",
        "live_eligible": False,
        "telegram_delivery_allowed": False,
        "trade_execution_allowed": False,
    }
