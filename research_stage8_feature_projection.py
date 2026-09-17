"""Pure Stage-8 projection of frozen Watch-v2 operational model scores.

The evidence-eligible path consumes raw anchor authority rows plus the exact
Watch-v2 payload selected by an authoritative database audit and its bound,
population-complete selection attestation.  A caller may alternatively supply
a bounded Watch candidate list to :func:`project_from_watch_snapshots`, but
that discovery-only path is deliberately UNKNOWN because a list cannot prove
that no newer archive row was omitted.  This module performs no database or
network reads, writes, score replay, outcome access, delivery inference, or
sampler-v4 mutation.

The sampler-v4 feature bundle deliberately says ``model_score_status=ABSENT``.
That is an anchor property, not a model observation.  Operational model scores
are accepted only from a valid, durably-prior Watch-v2 capture and remain a
separate sidecar fact.  Unavailable or malformed inputs yield UNKNOWN; their
fallback zero is never converted into a known negative candidate match.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
import math
import re
from typing import Any, Mapping, Sequence

import research_operational_score_source_audit as source_audit
import research_stage8_contract as contract
import research_watch_score_capture as watch_capture


VERSION = contract.PROJECTION_VERSION
KNOWN = "KNOWN"
UNKNOWN = "UNKNOWN"
APPLICABLE = "APPLICABLE"
NOT_APPLICABLE = "NOT_APPLICABLE"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_INT64_MAX = 9223372036854775807
MAX_WATCH_SNAPSHOT_CANDIDATES = 100
WATCH_SELECTION_ATTESTATION_VERSION = "stage8-watch-db-selection-attestation-v1"
WATCH_SELECTION_QUERY_VERSION = "stage8-watch-db-selection-query-v1"
WATCH_SELECTION_POLICY = "LATEST_DURABLY_PRIOR_WATCH_SHARED_THEN_VALIDATE"
_WATCH_CODE_FILES = {
    "alert_engine.py", "market_confidence_engine.py", "time_family_engine.py",
    "coinglass_flow_engine.py", "coinglass_oi_regime_service.py", "live_price_provider.py",
}
_MODEL_FAMILIES = {
    "positioning": "Price+OI",
    "futures_flow": "Futures Flow",
    "spot_flow": "Spot Flow",
}
_MODEL_SOURCE_PATHS = {
    "positioning": "positioning",
    "futures_flow": "futures",
    "spot_flow": "spot",
}
_SCORE_DIRECTIONS = {"BULLISH", "BEARISH", "NEUTRAL"}
_VALIDATED_SOURCE_TOKEN = object()


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _finite(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    if not math.isfinite(number):
        return None
    # Emit one numeric representation even if JSONB returned an integer.
    return 0.0 if number == 0.0 else number


def _utc(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp requires an explicit UTC offset")
    return parsed.astimezone(timezone.utc)


def _iso(value: Any) -> str:
    return _utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _valid_hash(value: Any) -> bool:
    return isinstance(value, str) and _SHA256.fullmatch(value) is not None


def _score_direction(score: float) -> str:
    # This is the captured producer's exact signed-score direction rule.
    return "BULLISH" if score >= 12.0 else "BEARISH" if score <= -12.0 else "NEUTRAL"


def _reason_list(value: Any) -> tuple[list[str], bool]:
    """Return exact stable reasons and whether their source shape is valid."""
    if (type(value) is not list
            or any(not isinstance(reason, str) or not reason for reason in value)):
        return [], False
    normalized = sorted(set(value))
    return normalized, value == normalized


def _watch_hash(value: Any) -> str | None:
    """Hash already-validated JSON with the Watch-v2 normalization policy."""
    def strict_json(item: Any) -> Any:
        if item is None or type(item) in (str, bool, int):
            return item
        if type(item) is float:
            if not math.isfinite(item):
                raise ValueError("non-finite Watch value")
            return item
        if isinstance(item, Mapping):
            if any(type(key) is not str for key in item):
                raise ValueError("non-string Watch key")
            return {key: strict_json(child) for key, child in item.items()}
        if type(item) is list:
            return [strict_json(child) for child in item]
        raise ValueError("non-JSON Watch value")
    try:
        return watch_capture.digest(strict_json(value))
    except (TypeError, ValueError, OverflowError):
        return None


def _watch_code_manifest(snapshot: Mapping[str, Any] | None) -> tuple[dict | None, str | None]:
    block = source_audit._capture_block(_mapping(snapshot))
    codes = block.get("code_sha256")
    if (not isinstance(codes, Mapping) or set(codes) != _WATCH_CODE_FILES
            or any(not _valid_hash(value) for value in codes.values())):
        return None, None
    detached = {key: codes[key] for key in sorted(codes)}
    return detached, _watch_hash(detached)


def validate_watch_selection_attestation(value: Mapping[str, Any], *,
                                          attempt: Mapping[str, Any],
                                          selected_snapshot: Mapping[str, Any] | None,
                                          selected_code_manifest: Mapping[str, Any] | None = None,
                                          expected_watch_code_manifest_sha256: str | None = None) -> None:
    """Validate a DB-audit-produced, population-complete selection receipt.

    The receipt is an authority boundary: this projector can bind and validate
    it, but cannot itself prove that a caller supplied every archive row.  The
    producer must emit it from the same read-only repeatable-read query that
    performed latest-row selection.
    """
    invalid = "STAGE8_WATCH_SELECTION_ATTESTATION_INVALID"
    try:
        if type(value) is not dict or set(value) != {
                "version", "source_audit_version", "query_scope", "query_binding_sha256",
                "transaction_snapshot", "population_complete", "selection_policy",
                "attempt_id", "attempt_fingerprint", "symbol", "decision_time_utc",
                "max_capture_age_seconds", "read_started_at_utc", "selection_status",
                "selected_snapshot_set_id", "selected_snapshot_key",
                "selected_payload_sha256", "selected_durably_available_at_utc",
                "watch_code_manifest_sha256",
                "attestation_sha256"}:
            raise ValueError(invalid)
        if (value["version"] != WATCH_SELECTION_ATTESTATION_VERSION
                or value["source_audit_version"] != source_audit.VERSION
                or value["transaction_snapshot"] != "CALLER_TRANSACTION_SNAPSHOT"
                or value["population_complete"] is not True
                or value["selection_policy"] != WATCH_SELECTION_POLICY
                or value["max_capture_age_seconds"]
                != contract.frozen_manifest()["source"]["max_capture_age_seconds"]):
            raise ValueError(invalid)
        query_scope = value["query_scope"]
        if type(query_scope) is not dict or set(query_scope) != {
                "version", "source_audit_version", "source", "selection_policy",
                "attempt_id", "attempt_fingerprint", "symbol", "decision_time_utc",
                "max_capture_age_seconds", "archive_snapshot_high_water_id",
                "database_snapshot_id"}:
            raise ValueError(invalid)
        expected_query = {
            "version": WATCH_SELECTION_QUERY_VERSION,
            "source_audit_version": source_audit.VERSION,
            "source": "WATCH_SHARED",
            "selection_policy": WATCH_SELECTION_POLICY,
            "attempt_id": attempt.get("attempt_id"),
            "attempt_fingerprint": attempt.get("attempt_fingerprint"),
            "symbol": attempt.get("symbol"),
            "decision_time_utc": _iso(attempt.get("decision_time_utc")),
            "max_capture_age_seconds": contract.frozen_manifest()["source"]["max_capture_age_seconds"],
        }
        if (any(query_scope.get(key) != expected for key, expected in expected_query.items())
                or type(query_scope.get("archive_snapshot_high_water_id")) is not int
                or query_scope["archive_snapshot_high_water_id"] < 0
                or query_scope["archive_snapshot_high_water_id"] > _INT64_MAX
                or not isinstance(query_scope.get("database_snapshot_id"), str)
                or not query_scope["database_snapshot_id"].strip()
                or contract.digest(query_scope) != value["query_binding_sha256"]):
            raise ValueError(invalid)
        if (type(value["attempt_id"]) is not int
                or value["attempt_id"] != attempt.get("attempt_id")
                or value["attempt_fingerprint"] != attempt.get("attempt_fingerprint")
                or value["symbol"] != attempt.get("symbol")
                or value["decision_time_utc"] != _iso(attempt.get("decision_time_utc"))
                or not _valid_hash(value["attempt_fingerprint"])):
            raise ValueError(invalid)
        if (_iso(value["read_started_at_utc"]) != value["read_started_at_utc"]
                or _utc(value["read_started_at_utc"])
                < _utc(value["decision_time_utc"])):
            raise ValueError(invalid)
        unsigned = dict(value)
        supplied = unsigned.pop("attestation_sha256")
        if not _valid_hash(supplied) or contract.digest(unsigned) != supplied:
            raise ValueError(invalid)

        if value["selection_status"] == "NO_MATCH":
            if selected_snapshot is not None or any(value[key] is not None for key in (
                    "selected_snapshot_set_id", "selected_snapshot_key",
                    "selected_payload_sha256", "selected_durably_available_at_utc",
                    "watch_code_manifest_sha256")):
                raise ValueError(invalid)
            return
        if value["selection_status"] != "SELECTED" or not isinstance(selected_snapshot, Mapping):
            raise ValueError(invalid)
        available = _utc(selected_snapshot.get("available_at_utc"))
        created = _utc(selected_snapshot.get("created_at_utc"))
        durable = max(available, created)
        computed_manifest, code_digest = _watch_code_manifest(selected_snapshot)
        if selected_code_manifest is not None:
            supplied_manifest = dict(selected_code_manifest)
            supplied_digest = _watch_hash(supplied_manifest)
            if (set(supplied_manifest) != _WATCH_CODE_FILES
                    or any(not _valid_hash(item) for item in supplied_manifest.values())
                    or supplied_digest is None
                    or (computed_manifest is not None
                        and supplied_manifest != computed_manifest)):
                raise ValueError(invalid)
            code_digest = supplied_digest
        # A raw Watch payload authenticates its own code map through the inner
        # payload validation performed by the caller.  A thin projected
        # snapshot reference cannot: it needs the out-of-band frozen generation
        # digest as an additional authority input.
        if computed_manifest is None:
            if (selected_code_manifest is None
                    or not _valid_hash(expected_watch_code_manifest_sha256)
                    or code_digest != expected_watch_code_manifest_sha256):
                raise ValueError(invalid)
        elif (expected_watch_code_manifest_sha256 is not None
              and (not _valid_hash(expected_watch_code_manifest_sha256)
                   or code_digest != expected_watch_code_manifest_sha256)):
            raise ValueError(invalid)
        if (value["selected_snapshot_set_id"] != selected_snapshot.get("snapshot_set_id")
                or value["selected_snapshot_key"] != selected_snapshot.get("snapshot_key")
                or value["selected_payload_sha256"] != selected_snapshot.get("payload_sha256")
                or value["selected_durably_available_at_utc"] != _iso(durable)
                or value["watch_code_manifest_sha256"] != code_digest
                or selected_snapshot.get("source") != "WATCH_SHARED"
                or type(value["selected_snapshot_set_id"]) is not int
                or not 0 < value["selected_snapshot_set_id"] <= _INT64_MAX
                or not _valid_hash(value["selected_snapshot_key"])
                or not _valid_hash(value["selected_payload_sha256"])):
            raise ValueError(invalid)
        if value["selected_snapshot_set_id"] > query_scope["archive_snapshot_high_water_id"]:
            raise ValueError(invalid)
        decision = _utc(value["decision_time_utc"])
        age = (decision - durable).total_seconds()
        if (not 0 <= age <= value["max_capture_age_seconds"]
                or durable > _utc(value["read_started_at_utc"])):
            raise ValueError(invalid)
    except (TypeError, ValueError, KeyError, OverflowError) as exc:
        raise ValueError(invalid) from exc


def _price_provenance(coin: Mapping[str, Any], *, symbol: str,
                      label_route: str) -> tuple[dict, list[str]]:
    """Keep operational quote identities distinct from the later label path."""
    reasons: list[str] = []
    source_rows = _mapping(coin.get("sources")).get("maxpain_operational_rows")
    identities: list[dict[str, Any]] = []
    if not isinstance(source_rows, list):
        reasons.append("OPERATIONAL_PRICE_PROVENANCE_MISSING")
    else:
        for row in source_rows:
            row = _mapping(row)
            identity = {
                key: row.get(key) if isinstance(row.get(key), str) else None
                for key in ("timeframe", "price_source", "price_pair",
                            "price_market", "price_instrument")
            }
            if not all(isinstance(identity[key], str) and identity[key].strip()
                       for key in ("timeframe", "price_source", "price_pair")):
                reasons.append("OPERATIONAL_PRICE_IDENTITY_INVALID")
            identities.append(identity)
    identities.sort(key=lambda item: (
        str(item.get("timeframe")), str(item.get("price_source")),
        str(item.get("price_pair")), str(item.get("price_market")),
        str(item.get("price_instrument")),
    ))
    identity_hash = _watch_hash(identities)
    if identity_hash is None:
        reasons.append("OPERATIONAL_PRICE_PROVENANCE_NOT_HASHABLE")
    hype = symbol == "HYPE"
    return {
        "declared_label_price_route": label_route,
        "label_route_status": "DECLARATION_ONLY_NOT_AUDITED",
        "operational_identity_role": "WATCH_INPUT_ONLY_NOT_OUTCOME_PRICE",
        "operational_identities": identities,
        "operational_identities_sha256": identity_hash,
        "hype_label_instrument": "@107" if hype else None,
        "hype_operational_route_relabelled_to_spot": False if hype else None,
        "operational_and_label_routes_are_separate": True,
    }, reasons


def _base_fact(binding: Mapping[str, Any], attempt: Mapping[str, Any],
               slot: Mapping[str, Any], events: Sequence[Mapping[str, Any]]) -> dict:
    inner = binding["binding"]
    candidate = inner["candidate"]
    symbol = attempt.get("symbol") if isinstance(attempt.get("symbol"), str) else None
    sampler_version = attempt.get("sampler_version")
    sampler_version = sampler_version if isinstance(sampler_version, str) else None
    detached_binding = json.loads(contract.canonical(binding))
    direction = candidate["direction"]
    event_id = slot.get(direction.lower() + "_event_id")
    event_id = event_id if type(event_id) is int else None
    event = next((_mapping(item) for item in events
                  if _mapping(item).get("event_id") == event_id), {})
    slot_id = slot.get("anchor_slot_id")
    slot_id = slot_id if type(slot_id) is int else None
    attempt_id = attempt.get("attempt_id")
    attempt_id = attempt_id if type(attempt_id) is int else None
    identity = {
        "attempt_id": attempt_id,
        "attempt_fingerprint": (attempt.get("attempt_fingerprint")
                                if isinstance(attempt.get("attempt_fingerprint"), str) else None),
        "anchor_slot_id": slot_id,
        "event_id": event_id,
        "event_fingerprint": (event.get("event_fingerprint")
                              if isinstance(event.get("event_fingerprint"), str) else None),
        "sampler_version": sampler_version,
        "symbol": symbol,
        "source_candle_open_utc": None,
        "decision_time_utc": None,
        "scope_id": inner["scope"]["scope_id"],
        "candidate_id": candidate["candidate_id"],
        "model": candidate["model"],
        "direction": candidate["direction"],
        "window_minutes": inner["window_minutes"],
        "threshold_bps": inner["threshold_bps"],
    }
    for key in ("source_candle_open_utc", "decision_time_utc"):
        try:
            identity[key] = _iso(attempt.get(key))
        except (TypeError, ValueError, OverflowError):
            identity[key] = None
    return {
        "version": VERSION,
        "manifest_sha256": contract.MANIFEST_SHA256,
        "binding": detached_binding,
        "identity": identity,
        "applicability_status": APPLICABLE,
        "knowledge_status": UNKNOWN,
        "candidate_match": None,
        "reasons": [],
        "source_reasons": {"anchor": [], "selection": [], "capture": []},
        "feature": {
            "name": candidate["feature"],
            "model": candidate["model"],
            "raw_score": None,
            "source_direction": None,
            "candidate_direction": candidate["direction"],
            "direction_multiplier": contract.frozen_manifest()["projection"]
            ["direction_multiplier"][candidate["direction"]],
            "aligned_score": None,
            "operator": candidate["operator"],
            "value": candidate["value"],
        },
        "source": {
            "anchor_model_score_status": None,
            "watch_version": None,
            "outer_archive_hash_verified": False,
            "watch_selection_status": None,
            "watch_selection_query_binding_sha256": None,
            "watch_selection_attestation_sha256": None,
            "watch_selection_attestation": None,
            "watch_snapshot": None,
            "watch_inner_payload_sha256": None,
            "watch_code_sha256": None,
            "watch_code_manifest_sha256": None,
            "score_generation_status": None,
            "model_observation_sha256": None,
            "model_source_sha256": None,
            "derivatives_snapshot_sha256": None,
            "price_provenance": None,
        },
    }


def _finish(fact: dict, reasons: Sequence[str], *, known: bool = False) -> dict:
    fact["reasons"] = sorted(set(reasons))
    if known and not fact["reasons"]:
        fact["knowledge_status"] = KNOWN
    unsigned = dict(fact)
    unsigned.pop("fact_sha256", None)
    fact["fact_sha256"] = contract.digest(unsigned)
    return fact


def validate_fact(value: Mapping[str, Any], *,
                  expected_fact_sha256: str | None = None,
                  expected_watch_selection_attestation_sha256: str | None = None,
                  expected_watch_code_manifest_sha256: str | None = None) -> None:
    """Validate a durably frozen fact and its scoring generation.

    This is a structural/self-consistency validator, not a signature verifier.
    ``expected_*`` values are authority inputs and MUST come from a trusted,
    durable freeze/registry rather than from ``value``.  The fact hash binds
    anchor, selection, observation and provenance identities; for an applicable
    KNOWN fact the two narrower expected hashes additionally bind the database
    selection receipt and Watch scoring generation.  Call
    :func:`project_binding_fact` with the raw authority rows and full selected
    Watch payload when no such durable expected hashes exist.
    """
    invalid = "STAGE8_PROJECTED_FACT_INVALID"
    try:
        if type(value) is not dict or set(value) != {
                "version", "manifest_sha256", "binding", "identity",
                "applicability_status", "knowledge_status", "candidate_match",
                "reasons", "source_reasons", "feature", "source", "fact_sha256"}:
            raise ValueError(invalid)
        if value["version"] != VERSION or value["manifest_sha256"] != contract.MANIFEST_SHA256:
            raise ValueError(invalid)
        binding = value["binding"]
        contract.validate_exact_binding(binding)
        inner = binding["binding"]
        candidate, scope = inner["candidate"], inner["scope"]

        identity = value["identity"]
        if type(identity) is not dict or set(identity) != {
                "attempt_id", "attempt_fingerprint", "anchor_slot_id", "event_id", "event_fingerprint",
                "sampler_version", "symbol", "source_candle_open_utc", "decision_time_utc",
                "scope_id", "candidate_id", "model", "direction", "window_minutes",
                "threshold_bps"}:
            raise ValueError(invalid)
        expected_identity = {
            "scope_id": scope["scope_id"], "candidate_id": candidate["candidate_id"],
            "model": candidate["model"], "direction": candidate["direction"],
            "window_minutes": inner["window_minutes"],
            "threshold_bps": inner["threshold_bps"],
        }
        if any(identity.get(key) != expected for key, expected in expected_identity.items()):
            raise ValueError(invalid)

        feature = value["feature"]
        if type(feature) is not dict or set(feature) != {
                "name", "model", "raw_score", "source_direction", "candidate_direction",
                "direction_multiplier", "aligned_score", "operator", "value"}:
            raise ValueError(invalid)
        multiplier = contract.frozen_manifest()["projection"]["direction_multiplier"][
            candidate["direction"]
        ]
        if any((
            feature["name"] != candidate["feature"],
            feature["model"] != candidate["model"],
            feature["candidate_direction"] != candidate["direction"],
            feature["direction_multiplier"] != multiplier,
            feature["operator"] != candidate["operator"],
            feature["value"] != candidate["value"],
        )):
            raise ValueError(invalid)

        reasons = value["reasons"]
        source_reasons = value["source_reasons"]
        if (_reason_list(reasons) != (reasons, True)
                or type(source_reasons) is not dict
                or set(source_reasons) != {"anchor", "selection", "capture"}
                or any(_reason_list(source_reasons[key]) != (source_reasons[key], True)
                       for key in ("anchor", "selection", "capture"))
                or any(reason not in reasons for values in source_reasons.values()
                       for reason in values)):
            raise ValueError(invalid)

        applicability = value["applicability_status"]
        knowledge = value["knowledge_status"]
        if applicability not in (APPLICABLE, NOT_APPLICABLE, UNKNOWN):
            raise ValueError(invalid)
        if knowledge not in (KNOWN, UNKNOWN):
            raise ValueError(invalid)
        if applicability == NOT_APPLICABLE:
            frozen_symbols = {
                symbol
                for frozen_scope in contract.frozen_manifest()["candidates"]["scopes"]
                for symbol in frozen_scope["symbols"]
            }
            if (knowledge != KNOWN or value["candidate_match"] is not None
                    or reasons != ["SYMBOL_OUTSIDE_FROZEN_SCOPE"]
                    or any(source_reasons.values())
                    or identity["symbol"] not in frozen_symbols
                    or identity["symbol"] in scope["symbols"]):
                raise ValueError(invalid)
        elif applicability == UNKNOWN:
            if knowledge != UNKNOWN or value["candidate_match"] is not None or not reasons:
                raise ValueError(invalid)
        elif knowledge == UNKNOWN:
            if value["candidate_match"] is not None or not reasons:
                raise ValueError(invalid)
        elif reasons or type(value["candidate_match"]) is not bool:
            raise ValueError(invalid)

        raw, aligned = _finite(feature["raw_score"]), _finite(feature["aligned_score"])
        if knowledge == KNOWN:
            for key in ("attempt_fingerprint", "event_fingerprint"):
                if not _valid_hash(identity[key]):
                    raise ValueError(invalid)
            for key in ("attempt_id", "anchor_slot_id", "event_id"):
                if type(identity[key]) is not int or not 0 < identity[key] <= _INT64_MAX:
                    raise ValueError(invalid)
            if identity["sampler_version"] != contract.frozen_manifest()["source"]["sampler_version"]:
                raise ValueError(invalid)
            for key in ("source_candle_open_utc", "decision_time_utc"):
                if _iso(identity[key]) != identity[key]:
                    raise ValueError(invalid)
            if (_utc(identity["source_candle_open_utc"])
                    >= _utc(identity["decision_time_utc"])):
                raise ValueError(invalid)
        if applicability == APPLICABLE and knowledge == KNOWN:
            if (type(feature["raw_score"]) is not float
                    or type(feature["aligned_score"]) is not float
                    or raw is None or aligned is None
                    or feature["source_direction"] not in _SCORE_DIRECTIONS
                    or not -100.0 <= raw <= 100.0
                    or feature["source_direction"] != _score_direction(raw)
                    or aligned != raw * multiplier
                    or value["candidate_match"] != (aligned >= float(candidate["value"]))):
                raise ValueError(invalid)
            if identity["symbol"] not in scope["symbols"]:
                raise ValueError(invalid)
        elif any(feature[key] is not None for key in (
                "raw_score", "source_direction", "aligned_score")):
            raise ValueError(invalid)

        source = value["source"]
        if type(source) is not dict or set(source) != {
                "anchor_model_score_status", "watch_version", "outer_archive_hash_verified",
                "watch_selection_status", "watch_selection_query_binding_sha256",
                "watch_selection_attestation_sha256",
                "watch_selection_attestation",
                "watch_snapshot", "watch_inner_payload_sha256", "model_observation_sha256",
                "watch_code_sha256", "watch_code_manifest_sha256", "score_generation_status",
                "model_source_sha256", "derivatives_snapshot_sha256", "price_provenance"}:
            raise ValueError(invalid)
        if source["outer_archive_hash_verified"] is not False:
            raise ValueError(invalid)
        if knowledge == KNOWN and source["anchor_model_score_status"] != "ABSENT":
            raise ValueError(invalid)
        if applicability == APPLICABLE and knowledge == KNOWN:
            if (source["anchor_model_score_status"] != "ABSENT"
                    or source["watch_version"] != contract.frozen_manifest()["source"]["watch_version"]
                    or source["watch_selection_status"] != "SELECTED"
                    or not _valid_hash(source["watch_selection_query_binding_sha256"])
                    or not _valid_hash(source["watch_selection_attestation_sha256"])
                    or not _valid_hash(expected_watch_selection_attestation_sha256)
                    or source["watch_selection_attestation_sha256"]
                    != expected_watch_selection_attestation_sha256
                    or source["score_generation_status"]
                    != "OBSERVED_REQUIRES_DURABLE_FREEZE_BINDING"
                    or not _valid_hash(expected_watch_code_manifest_sha256)
                    or source["watch_code_manifest_sha256"]
                    != expected_watch_code_manifest_sha256
                    or any(source_reasons.values())
                    or any(not _valid_hash(source[key]) for key in (
                        "watch_inner_payload_sha256", "model_observation_sha256",
                        "model_source_sha256", "derivatives_snapshot_sha256"))):
                raise ValueError(invalid)
            snapshot = source["watch_snapshot"]
            if type(snapshot) is not dict or set(snapshot) != {
                    "snapshot_set_id", "snapshot_key", "payload_sha256", "source", "cycle_id",
                    "outer_hash_validation", "available_at_utc", "created_at_utc",
                    "durably_available_at_utc"}:
                raise ValueError(invalid)
            if (type(snapshot["snapshot_set_id"]) is not int
                    or not 0 < snapshot["snapshot_set_id"] <= _INT64_MAX
                    or snapshot["source"] != "WATCH_SHARED"
                    or snapshot["outer_hash_validation"] != "REFERENCE_ONLY_NOT_RECOMPUTED"
                    or not _valid_hash(snapshot["snapshot_key"])
                    or not _valid_hash(snapshot["payload_sha256"])
                    or not isinstance(snapshot["cycle_id"], str)
                    or not snapshot["cycle_id"].strip()):
                raise ValueError(invalid)
            for key in ("available_at_utc", "created_at_utc", "durably_available_at_utc"):
                if _iso(snapshot[key]) != snapshot[key]:
                    raise ValueError(invalid)
            if _utc(snapshot["durably_available_at_utc"]) != max(
                    _utc(snapshot["available_at_utc"]), _utc(snapshot["created_at_utc"])):
                raise ValueError(invalid)
            age = (_utc(identity["decision_time_utc"])
                   - _utc(snapshot["durably_available_at_utc"])).total_seconds()
            if not 0 <= age <= contract.frozen_manifest()["source"]["max_capture_age_seconds"]:
                raise ValueError(invalid)

            attestation = source["watch_selection_attestation"]
            if (type(attestation) is not dict
                    or attestation.get("attestation_sha256")
                    != source["watch_selection_attestation_sha256"]
                    or attestation.get("query_binding_sha256")
                    != source["watch_selection_query_binding_sha256"]):
                raise ValueError(invalid)
            validate_watch_selection_attestation(
                attestation,
                attempt={
                    "attempt_id": identity["attempt_id"],
                    "attempt_fingerprint": identity["attempt_fingerprint"],
                    "symbol": identity["symbol"],
                    "decision_time_utc": identity["decision_time_utc"],
                },
                selected_snapshot=snapshot,
                selected_code_manifest=source["watch_code_sha256"],
                expected_watch_code_manifest_sha256=
                expected_watch_code_manifest_sha256,
            )
            code_manifest = source["watch_code_sha256"]
            if (type(code_manifest) is not dict or set(code_manifest) != _WATCH_CODE_FILES
                    or any(not _valid_hash(item) for item in code_manifest.values())
                    or _watch_hash(code_manifest) != source["watch_code_manifest_sha256"]
                    or attestation.get("watch_code_manifest_sha256")
                    != source["watch_code_manifest_sha256"]):
                raise ValueError(invalid)

            price = source["price_provenance"]
            if type(price) is not dict or set(price) != {
                    "declared_label_price_route", "label_route_status",
                    "operational_identity_role", "operational_identities",
                    "operational_identities_sha256", "hype_label_instrument",
                    "hype_operational_route_relabelled_to_spot",
                    "operational_and_label_routes_are_separate"}:
                raise ValueError(invalid)
            if (price["declared_label_price_route"] != scope["price_route"]
                    or price["label_route_status"] != "DECLARATION_ONLY_NOT_AUDITED"
                    or price["operational_identity_role"] != "WATCH_INPUT_ONLY_NOT_OUTCOME_PRICE"
                    or price["operational_and_label_routes_are_separate"] is not True
                    or _watch_hash(price["operational_identities"])
                    != price["operational_identities_sha256"]):
                raise ValueError(invalid)
            identities = price["operational_identities"]
            if (type(identities) is not list
                    or len(identities) != len(source_audit.alert_engine.TIMEFRAMES)
                    or {row.get("timeframe") for row in identities if type(row) is dict}
                    != set(source_audit.alert_engine.TIMEFRAMES)
                    or any(type(row) is not dict or set(row) != {
                        "timeframe", "price_source", "price_pair", "price_market",
                        "price_instrument"} for row in identities)
                    or any(not isinstance(row[key], str) or not row[key].strip()
                           for row in identities
                           for key in ("timeframe", "price_source", "price_pair"))):
                raise ValueError(invalid)
            hype = identity["symbol"] == "HYPE"
            expected_hype = ("@107", False) if hype else (None, None)
            if (price["hype_label_instrument"],
                    price["hype_operational_route_relabelled_to_spot"]) != expected_hype:
                raise ValueError(invalid)
        elif applicability == NOT_APPLICABLE:
            if any(source[key] is not None for key in (
                    "watch_version", "watch_selection_status",
                    "watch_selection_query_binding_sha256",
                    "watch_selection_attestation_sha256",
                    "watch_selection_attestation", "watch_snapshot",
                    "watch_inner_payload_sha256", "watch_code_sha256",
                    "watch_code_manifest_sha256", "score_generation_status",
                    "model_observation_sha256", "model_source_sha256",
                    "derivatives_snapshot_sha256", "price_provenance")):
                raise ValueError(invalid)

        supplied = value["fact_sha256"]
        unsigned = dict(value)
        unsigned.pop("fact_sha256")
        expected = contract.digest(unsigned)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(invalid) from exc
    if not _valid_hash(supplied) or supplied != expected:
        raise ValueError(invalid)
    if not _valid_hash(expected_fact_sha256) or supplied != expected_fact_sha256:
        raise ValueError(invalid)


def _project_validated_sources(audit_record: Mapping[str, Any], *,
                               binding: Mapping[str, Any]) -> dict:
    """Project sources validated in this module's uninterrupted call path."""
    contract.validate_exact_binding(binding)
    if (not isinstance(audit_record, Mapping)
            or audit_record.get("_validation_token") is not _VALIDATED_SOURCE_TOKEN):
        raise ValueError("STAGE8_VALIDATED_SOURCE_ENVELOPE_REQUIRED")

    attempt = _mapping(audit_record.get("attempt"))
    slot = _mapping(audit_record.get("anchor_slot"))
    events = audit_record.get("anchor_events")
    events = events if isinstance(events, (list, tuple)) else ()
    authority = _mapping(audit_record.get("anchor_authority"))
    captured = _mapping(audit_record.get("capture"))
    fact = _base_fact(binding, attempt, slot, events)
    reasons: list[str] = []

    inner = binding["binding"]
    scope = inner["scope"]
    candidate = inner["candidate"]
    symbol = attempt.get("symbol")
    frozen_symbols = {
        frozen_symbol
        for frozen_scope in contract.frozen_manifest()["candidates"]["scopes"]
        for frozen_symbol in frozen_scope["symbols"]
    }
    if (not attempt or not isinstance(symbol, str) or symbol not in frozen_symbols
            or not _valid_hash(attempt.get("attempt_fingerprint"))):
        fact["applicability_status"] = UNKNOWN
        return _finish(fact, ["AUDIT_ATTEMPT_IDENTITY_INVALID"])
    expected_source = contract.frozen_manifest()["source"]
    if attempt.get("sampler_version") != expected_source["sampler_version"]:
        reasons.append("ATTEMPT_SAMPLER_VERSION_MISMATCH")
    if attempt.get("evaluation_status") != "EVALUABLE":
        reasons.append("ATTEMPT_NOT_EVALUABLE")
    if (type(fact["identity"]["attempt_id"]) is not int
            or not 0 < fact["identity"]["attempt_id"] <= _INT64_MAX):
        reasons.append("ATTEMPT_ID_INVALID")
    for key in ("source_candle_open_utc", "decision_time_utc"):
        if fact["identity"][key] is None:
            reasons.append("ATTEMPT_" + key.upper() + "_INVALID")
    if (type(fact["identity"]["anchor_slot_id"]) is not int
            or not 0 < fact["identity"]["anchor_slot_id"] <= _INT64_MAX
            or type(fact["identity"]["event_id"]) is not int
            or not 0 < fact["identity"]["event_id"] <= _INT64_MAX
            or not _valid_hash(fact["identity"]["event_fingerprint"])):
        reasons.append("ANCHOR_SLOT_OR_DIRECTION_EVENT_IDENTITY_INVALID")

    anchor_reasons, anchor_reasons_valid = _reason_list(authority.get("reasons"))
    fact["source_reasons"]["anchor"] = anchor_reasons
    reasons.extend(anchor_reasons)
    if not anchor_reasons_valid:
        reasons.append("ANCHOR_AUDIT_REASONS_INVALID")
    if authority.get("status") != "VALID" or anchor_reasons:
        reasons.append("ANCHOR_AUTHORITY_NOT_VALID")
    anchor_model_status = authority.get("model_score_status")
    fact["source"]["anchor_model_score_status"] = (
        anchor_model_status if isinstance(anchor_model_status, str) else None
    )
    if anchor_model_status != expected_source["anchor_model_score_status"]:
        reasons.append("ANCHOR_MODEL_SCORE_STATUS_NOT_ABSENT")

    # Scope exclusion is known only after the raw anchor authority is proven;
    # a broken/mismatched slot or event set must not be hidden as out-of-scope.
    if symbol not in scope["symbols"]:
        if reasons:
            return _finish(fact, reasons)
        fact["applicability_status"] = NOT_APPLICABLE
        fact["reasons"] = ["SYMBOL_OUTSIDE_FROZEN_SCOPE"]
        fact["knowledge_status"] = KNOWN
        unsigned = dict(fact)
        unsigned.pop("fact_sha256", None)
        fact["fact_sha256"] = contract.digest(unsigned)
        return fact

    attestation = _mapping(audit_record.get("watch_selection_attestation"))
    selection_reasons, selection_reasons_valid = _reason_list(
        audit_record.get("watch_selection_reasons")
    )
    fact["source_reasons"]["selection"] = selection_reasons
    reasons.extend(selection_reasons)
    if not selection_reasons_valid:
        reasons.append("WATCH_SELECTION_REASONS_INVALID")
    selection_status = attestation.get("selection_status")
    fact["source"]["watch_selection_status"] = (
        selection_status if isinstance(selection_status, str) else None
    )
    query_hash = attestation.get("query_binding_sha256")
    attestation_hash = attestation.get("attestation_sha256")
    fact["source"]["watch_selection_query_binding_sha256"] = (
        query_hash if isinstance(query_hash, str) else None
    )
    fact["source"]["watch_selection_attestation_sha256"] = (
        attestation_hash if isinstance(attestation_hash, str) else None
    )
    if not selection_reasons and type(attestation) is dict:
        fact["source"]["watch_selection_attestation"] = json.loads(
            contract.canonical(attestation)
        )
    selected_snapshot = _mapping(audit_record.get("selected_watch_snapshot"))
    code_manifest, code_manifest_hash = _watch_code_manifest(selected_snapshot)
    if not selection_reasons and selection_status == "SELECTED":
        fact["source"]["watch_code_sha256"] = code_manifest
        fact["source"]["watch_code_manifest_sha256"] = code_manifest_hash
        fact["source"]["score_generation_status"] = (
            "OBSERVED_REQUIRES_DURABLE_FREEZE_BINDING"
        )
        if code_manifest is None or code_manifest_hash is None:
            reasons.append("WATCH_SCORE_CODE_GENERATION_INVALID")
    if selection_reasons:
        reasons.append("WATCH_SELECTION_NOT_AUTHORITATIVE")

    capture_reasons, capture_reasons_valid = _reason_list(captured.get("reasons"))
    fact["source_reasons"]["capture"] = capture_reasons
    reasons.extend(capture_reasons)
    if not capture_reasons_valid:
        reasons.append("CAPTURE_AUDIT_REASONS_INVALID")
    if captured.get("status") != "VALID" or capture_reasons:
        reasons.append("CAPTURE_AUDIT_NOT_VALID")

    reference = _mapping(captured.get("snapshot_reference"))
    fact["source"]["watch_snapshot"] = {
        "snapshot_set_id": (reference.get("snapshot_set_id")
                            if type(reference.get("snapshot_set_id")) is int else None),
        **{key: reference.get(key) if isinstance(reference.get(key), str) else None
           for key in ("snapshot_key", "payload_sha256", "source", "cycle_id",
                       "outer_hash_validation")},
    } if reference else None
    if reference:
        for key in ("available_at_utc", "created_at_utc", "durably_available_at_utc"):
            try:
                fact["source"]["watch_snapshot"][key] = _iso(reference.get(key))
            except (TypeError, ValueError, OverflowError):
                fact["source"]["watch_snapshot"][key] = None
                reasons.append("WATCH_SNAPSHOT_TIME_INVALID:" + key)
        if (type(reference.get("snapshot_set_id")) is not int
                or not 0 < reference["snapshot_set_id"] <= _INT64_MAX
                or reference.get("source") != "WATCH_SHARED"
                or not _valid_hash(reference.get("snapshot_key"))
                or not _valid_hash(reference.get("payload_sha256"))
                or reference.get("outer_hash_validation") != "REFERENCE_ONLY_NOT_RECOMPUTED"):
            reasons.append("WATCH_SNAPSHOT_REFERENCE_INVALID")
        try:
            decision = _utc(attempt.get("decision_time_utc"))
            durable = _utc(reference.get("durably_available_at_utc"))
            age = (decision - durable).total_seconds()
            if not 0 <= age <= float(expected_source["max_capture_age_seconds"]):
                reasons.append("WATCH_SNAPSHOT_NOT_DURABLY_PRIOR_WITHIN_FROZEN_MAX_AGE")
        except (TypeError, ValueError, OverflowError):
            reasons.append("WATCH_SNAPSHOT_CAUSAL_TIME_INVALID")
    else:
        reasons.append("WATCH_SNAPSHOT_REFERENCE_MISSING")

    inner_hash = captured.get("inner_payload_sha256")
    fact["source"]["watch_inner_payload_sha256"] = (
        inner_hash if isinstance(inner_hash, str) else None
    )
    if not _valid_hash(inner_hash):
        reasons.append("WATCH_INNER_PAYLOAD_HASH_REFERENCE_INVALID")

    coin = _mapping(captured.get("coin"))
    models = _mapping(coin.get("models"))
    model_name = candidate["model"]
    model = _mapping(models.get(model_name))
    audited_model = _mapping(_mapping(captured.get("model_observations")).get(model_name))
    if not model:
        reasons.append("MODEL_OBSERVATION_MISSING")
    if _watch_hash(model) != _watch_hash(audited_model):
        reasons.append("MODEL_OBSERVATION_AUDIT_MISMATCH")
    model_hash = _watch_hash(model) if model else None
    fact["source"]["model_observation_sha256"] = model_hash
    if model_hash is None:
        reasons.append("MODEL_OBSERVATION_NOT_HASHABLE")

    if model.get("available") is not True:
        reasons.append("MODEL_NOT_AVAILABLE")
    if model.get("capture_status") != "AVAILABLE":
        reasons.append("MODEL_CAPTURE_STATUS_NOT_AVAILABLE")
    if model.get("family") != _MODEL_FAMILIES[model_name]:
        reasons.append("MODEL_FAMILY_MISMATCH")
    score = _finite(model.get("score"))
    if score is None:
        reasons.append("MODEL_SCORE_INVALID")
    elif not -100.0 <= score <= 100.0:
        reasons.append("MODEL_SCORE_OUT_OF_RANGE")
    source_direction = str(model.get("direction") or "").upper()
    if source_direction not in _SCORE_DIRECTIONS:
        reasons.append("MODEL_SOURCE_DIRECTION_INVALID")
    elif score is not None and source_direction != _score_direction(score):
        reasons.append("MODEL_DIRECTION_SCORE_MISMATCH")

    sources = _mapping(coin.get("sources"))
    derivative_hash = sources.get("derivatives_snapshot_sha256")
    source_block = _mapping(sources.get(_MODEL_SOURCE_PATHS[model_name]))
    source_hash = _watch_hash(source_block) if source_block else None
    fact["source"]["model_source_sha256"] = source_hash
    fact["source"]["derivatives_snapshot_sha256"] = (
        derivative_hash if isinstance(derivative_hash, str) else None
    )
    if not _valid_hash(derivative_hash) or not source_block or source_hash is None:
        reasons.append("MODEL_SOURCE_PROVENANCE_INVALID")
    if model.get("available") is True:
        references = _mapping(source_block.get("window_references"))
        families = _mapping(model.get("time_families"))
        if not references or not families:
            reasons.append("MODEL_SOURCE_WINDOW_PROVENANCE_MISSING")

    price, price_reasons = _price_provenance(
        coin, symbol=str(symbol or ""), label_route=scope["price_route"]
    )
    fact["source"]["price_provenance"] = price
    reasons.extend(price_reasons)
    fact["source"]["watch_version"] = expected_source["watch_version"]

    # Never expose a numeric feature or boolean match from invalid/unknown
    # evidence.  In particular an unavailable producer fallback score of zero
    # remains UNKNOWN rather than a known failed predicate.
    if reasons:
        return _finish(fact, reasons)

    multiplier = fact["feature"]["direction_multiplier"]
    aligned = 0.0 if score * multiplier == 0.0 else score * multiplier
    fact["feature"].update({
        "raw_score": score,
        "source_direction": source_direction,
        "aligned_score": aligned,
    })
    if candidate["operator"] != ">=":
        # Exact bindings currently make this unreachable; retain fail-closed
        # behavior if a future manifest changes without a new implementation.
        return _finish(fact, ["CANDIDATE_OPERATOR_NOT_IMPLEMENTED"])
    fact["candidate_match"] = aligned >= float(candidate["value"])
    return _finish(fact, (), known=True)


def _validated_source_envelope(*, attempt: Mapping[str, Any],
                               anchor_slot: Mapping[str, Any] | None,
                               anchor_events: Sequence[Mapping[str, Any]],
                               selected_watch_snapshot: Mapping[str, Any] | None,
                               watch_selection_attestation: Mapping[str, Any]) -> dict:
    """Validate exact raw authority/capture rows without building new anchors."""
    manifest = contract.frozen_manifest()
    max_age = manifest["source"]["max_capture_age_seconds"]
    try:
        if (not isinstance(attempt, Mapping)
                or (anchor_slot is not None and not isinstance(anchor_slot, Mapping))
                or isinstance(anchor_events, (str, bytes))
                or not isinstance(anchor_events, Sequence)
                or len(anchor_events) > 2):
            raise ValueError("anchor authority input shape invalid")
        authority = source_audit.validate_anchor_authority(
            attempt, anchor_slot, tuple(anchor_events)
        )
    except (TypeError, ValueError, KeyError, AttributeError, OverflowError):
        authority = {
            "status": "UNKNOWN",
            "reasons": ["ANCHOR_AUTHORITY_VALIDATION_INVALID"],
            "source_status": None,
            "model_score_status": None,
        }
    selection_reasons: list[str] = []
    try:
        validate_watch_selection_attestation(
            watch_selection_attestation, attempt=attempt,
            selected_snapshot=selected_watch_snapshot,
        )
        validated = source_audit.validate_capture(
            selected_watch_snapshot, symbol=str(attempt.get("symbol") or ""),
            decision_time_utc=attempt.get("decision_time_utc"),
            max_capture_age_seconds=max_age,
        )
    except (TypeError, ValueError, KeyError, AttributeError, OverflowError):
        selection_reasons.append("WATCH_SELECTION_ATTESTATION_INVALID")
        validated = {
            "status": "UNKNOWN",
            "reasons": ["WATCH_SELECTION_ATTESTATION_INVALID"],
            "snapshot_reference": None,
            "coin": None,
            "model_observations": {},
        }
    return {
        "_validation_token": _VALIDATED_SOURCE_TOKEN,
        "attempt": attempt,
        "anchor_slot": anchor_slot,
        "anchor_events": tuple(anchor_events) if isinstance(anchor_events, Sequence) else (),
        "anchor_authority": authority,
        "watch_selection_attestation": watch_selection_attestation,
        "watch_selection_reasons": selection_reasons,
        "selected_watch_snapshot": selected_watch_snapshot,
        "capture": validated,
    }


def project_binding_fact(*, attempt: Mapping[str, Any],
                         anchor_slot: Mapping[str, Any] | None,
                         anchor_events: Sequence[Mapping[str, Any]],
                         selected_watch_snapshot: Mapping[str, Any] | None,
                         watch_selection_attestation: Mapping[str, Any],
                         binding: Mapping[str, Any]) -> dict:
    """Validate authoritative raw rows and project one exact binding.

    Validation reuses the sampler-v4 authority validator; it never builds,
    changes, or rescores an anchor.  The only score source is the causally
    selected Watch-v2 payload.  No outcome or BTC-parent input is accepted.
    """
    envelope = _validated_source_envelope(
        attempt=attempt, anchor_slot=anchor_slot, anchor_events=anchor_events,
        selected_watch_snapshot=selected_watch_snapshot,
        watch_selection_attestation=watch_selection_attestation,
    )
    return _project_validated_sources(envelope, binding=binding)


def project_from_watch_snapshots(*, attempt: Mapping[str, Any],
                                 anchor_slot: Mapping[str, Any] | None,
                                 anchor_events: Sequence[Mapping[str, Any]],
                                 watch_snapshots: Sequence[Mapping[str, Any]],
                                 binding: Mapping[str, Any]) -> dict:
    """Discovery-only path: caller lists cannot prove archive completeness.

    It deliberately returns UNKNOWN even if the locally latest supplied row is
    valid.  A DB audit must instead call :func:`project_binding_fact` with its
    selected row and population-complete selection attestation.
    """
    selected = None
    try:
        if (isinstance(watch_snapshots, (str, bytes))
                or not isinstance(watch_snapshots, Sequence)
                or len(watch_snapshots) > MAX_WATCH_SNAPSHOT_CANDIDATES):
            raise ValueError("Watch candidate input must be a bounded sequence")
        selected = source_audit.select_prior_capture(
            tuple(watch_snapshots), decision_time_utc=attempt.get("decision_time_utc"),
            max_capture_age_seconds=contract.frozen_manifest()["source"]["max_capture_age_seconds"],
        )
    except (TypeError, ValueError, KeyError, AttributeError, OverflowError):
        selected = None
    # Structurally invalid on purpose: the stable reason prevents this
    # caller-bounded discovery projection from becoming evidence-eligible.
    return project_binding_fact(
        attempt=attempt, anchor_slot=anchor_slot, anchor_events=anchor_events,
        selected_watch_snapshot=selected, watch_selection_attestation={},
        binding=binding,
    )


def project_first_tranche(*, attempt: Mapping[str, Any],
                          anchor_slot: Mapping[str, Any] | None,
                          anchor_events: Sequence[Mapping[str, Any]],
                          selected_watch_snapshot: Mapping[str, Any] | None,
                          watch_selection_attestation: Mapping[str, Any]) -> dict:
    """Project every applicable fixed first-tranche binding in stable order."""
    manifest = contract.frozen_manifest()
    contract.validate_manifest(manifest)
    envelope = _validated_source_envelope(
        attempt=attempt, anchor_slot=anchor_slot, anchor_events=anchor_events,
        selected_watch_snapshot=selected_watch_snapshot,
        watch_selection_attestation=watch_selection_attestation,
    )
    attempt = _mapping(envelope.get("attempt"))
    symbol = attempt.get("symbol") if isinstance(attempt.get("symbol"), str) else None
    scopes = [scope for scope in manifest["candidates"]["scopes"]
              if symbol in scope["symbols"]]
    frozen_symbols = {
        frozen_symbol
        for frozen_scope in manifest["candidates"]["scopes"]
        for frozen_symbol in frozen_scope["symbols"]
    }
    enumerable_identity_valid = (
        bool(attempt) and symbol in frozen_symbols
        and _valid_hash(attempt.get("attempt_fingerprint"))
    )
    authority = _mapping(envelope.get("anchor_authority"))
    anchor_reasons, anchor_reasons_valid = _reason_list(authority.get("reasons"))
    scope_reasons: list[str] = []
    if not enumerable_identity_valid:
        scope_reasons.append("AUDIT_ATTEMPT_IDENTITY_INVALID")
    if (type(attempt.get("attempt_id")) is not int
            or not 0 < attempt["attempt_id"] <= _INT64_MAX):
        scope_reasons.append("ATTEMPT_ID_INVALID")
    if attempt.get("sampler_version") != manifest["source"]["sampler_version"]:
        scope_reasons.append("ATTEMPT_SAMPLER_VERSION_MISMATCH")
    if attempt.get("evaluation_status") != "EVALUABLE":
        scope_reasons.append("ATTEMPT_NOT_EVALUABLE")
    if not anchor_reasons_valid:
        scope_reasons.append("ANCHOR_AUDIT_REASONS_INVALID")
    scope_reasons.extend(anchor_reasons)
    if authority.get("status") != "VALID" or anchor_reasons:
        scope_reasons.append("ANCHOR_AUTHORITY_NOT_VALID")
    if authority.get("model_score_status") != manifest["source"]["anchor_model_score_status"]:
        scope_reasons.append("ANCHOR_MODEL_SCORE_STATUS_NOT_ABSENT")
    scope_reasons = sorted(set(scope_reasons))
    scope_authority_valid = not scope_reasons
    facts = []
    if enumerable_identity_valid:
        for scope in scopes:
            for candidate in manifest["candidates"]["definitions"]:
                for threshold in manifest["labels"]["thresholds_bps"]:
                    binding = contract.exact_binding(
                        scope_id=scope["scope_id"], candidate_id=candidate["candidate_id"],
                        threshold_bps=threshold,
                    )
                    facts.append(_project_validated_sources(envelope, binding=binding))
    result = {
        "version": VERSION,
        "manifest_sha256": contract.MANIFEST_SHA256,
        "symbol": symbol,
        "attempt_id": (attempt.get("attempt_id")
                       if type(attempt.get("attempt_id")) is int else None),
        "attempt_fingerprint": (attempt.get("attempt_fingerprint")
                                if _valid_hash(attempt.get("attempt_fingerprint")) else None),
        "scope_resolution_status": KNOWN if scope_authority_valid else UNKNOWN,
        "reasons": scope_reasons,
        "fact_count": len(facts),
        "facts": facts,
    }
    result["facts_sha256"] = contract.digest(result)
    return result
