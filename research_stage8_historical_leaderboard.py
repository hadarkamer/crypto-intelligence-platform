"""Fail-closed historical presentation for the frozen 432 Stage-8 bindings.

Only bindings explicitly marked EVALUATED in a complete analysis receipt can
be ranked.  EVALUATED requires an evaluation even when n=0.  BLOCKED and
NOT_EVALUATED are visible, unavailable rows; they are never converted into
NO_EVIDENCE.  This pure module grants no prospective or runtime authority.
"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from datetime import datetime, timezone
from functools import lru_cache
import json
import math
import re
from typing import Any

import research_stage8_contract as contract


VERSION = "stage8-historical-descriptive-leaderboard-v1"
ANALYSIS_RECEIPT_VERSION = "stage8-historical-analysis-receipt-v1"
EVALUATION_INPUT_VERSION = "stage8-historical-evaluation-input-v1"
ROUTE_POPULATION_VERSION = "stage8-historical-route-population-v1"
NO_EVIDENCE = "NO_EVIDENCE"
PROVISIONAL_N_LT_5 = "PROVISIONAL_N_LT_5"
DESCRIPTIVE_N_GTE_5 = "DESCRIPTIVE_N_GTE_5"
EVIDENCE_BANDS = (NO_EVIDENCE, PROVISIONAL_N_LT_5, DESCRIPTIVE_N_GTE_5)
ROUTES = ("PROBABILITY", "ASYMMETRY")
EVALUATED = "EVALUATED"
BLOCKED = "BLOCKED"
NOT_EVALUATED = "NOT_EVALUATED"
EVALUATION_STATUSES = (EVALUATED, BLOCKED, NOT_EVALUATED)
MINIMUM_EVIDENCE_COUNT = 5
MAX_INT64 = 9223372036854775807

_UTC_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,6})?Z$"
)
_HEX_RE = re.compile(r"^[0-9a-f]{64}$")
_COMMIT_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_REASON_RE = re.compile(r"^[A-Z][A-Z0-9_]{0,127}$")
_HALF_OPEN_BOUNDARY = "START_INCLUSIVE_END_EXCLUSIVE"
_SNAPSHOT_SCOPE = "ONE_READ_ONLY_REPEATABLE_READ_TRANSACTION"

_AUTHORITY_FALSE = {
    "atomic_gate_passed": False,
    "structurally_eligible": False,
    "qualifies_as_prospective_formula_evidence": False,
    "prospective_atomic_gate_passed": False,
    "server_replay_verified": False,
    "research_qualified": False,
    "telegram_eligible": False,
    "live_eligible": False,
    "trade_execution_eligible": False,
    "deployment_eligible": False,
}
_SOURCE_VERSIONS = {
    "contract_version": contract.VERSION,
    "source_version": contract.SOURCE_VERSION,
    "projection_version": contract.PROJECTION_VERSION,
    "candidate_version": contract.CANDIDATE_VERSION,
    "label_version": contract.LABEL_VERSION,
    "independence_version": contract.INDEPENDENCE_VERSION,
    "acceptance_version": contract.ACCEPTANCE_VERSION,
}
_PROBABILITY_FIELDS = {
    "distinct_parent_count", "successes", "failures", "hit_rate_pct",
    "wilson_95_lower_pct", "included_btc_parent_movement_ids",
    "excluded_btc_parents", "route_population_sha256",
}
_ASYMMETRY_FIELDS = {
    "distinct_parent_count", "favorable_count",
    "sum_representative_mfe_pct", "sum_representative_mae_pct",
    "common_window_asymmetry_ratio",
    "common_window_favorable_dominance_pct",
    "common_window_median_paired_edge_pct",
    "included_btc_parent_movement_ids", "excluded_btc_parents",
    "route_population_sha256",
}


def _manifest() -> dict[str, Any]:
    manifest = contract.frozen_manifest()
    contract.validate_manifest(manifest)
    return manifest


@lru_cache(maxsize=1)
def _frozen_binding_universe_json() -> str:
    manifest = _manifest()
    rows: list[dict[str, Any]] = []
    for direction in contract.DIRECTIONS:
        for threshold_bps in manifest["labels"]["thresholds_bps"]:
            for candidate in manifest["candidates"]["definitions"]:
                if candidate["direction"] != direction:
                    continue
                for scope in manifest["candidates"]["scopes"]:
                    exact = contract.exact_binding(
                        scope_id=scope["scope_id"],
                        candidate_id=candidate["candidate_id"],
                        threshold_bps=threshold_bps,
                    )
                    rows.append({
                        "binding_ordinal": len(rows) + 1,
                        "exact_binding_sha256": exact["binding_sha256"],
                        "candidate_id": candidate["candidate_id"],
                        "model": candidate["model"],
                        "direction": direction,
                        "scope_id": scope["scope_id"],
                        "symbols": list(scope["symbols"]),
                        "price_route": scope["price_route"],
                        "window_minutes": exact["binding"]["window_minutes"],
                        "threshold_bps": threshold_bps,
                    })
    expected = manifest["candidates"]["family_capacity"]
    if len(rows) != expected or len({r["exact_binding_sha256"] for r in rows}) != expected:
        raise RuntimeError("STAGE8_FROZEN_BINDING_UNIVERSE_INVALID")
    return contract.canonical(rows)


def frozen_binding_universe() -> tuple[dict[str, Any], ...]:
    return tuple(json.loads(_frozen_binding_universe_json()))


def _strict_dict(value: Any, fields: set[str], name: str) -> dict[str, Any]:
    if type(value) is not dict or set(value) != fields:
        raise ValueError(name + " has an invalid field set")
    return value


def _text(value: Any, name: str, maximum: int = 256) -> str:
    if type(value) is not str or not value or len(value) > maximum:
        raise ValueError(name + " must be a nonempty bounded string")
    return value


def _utc(value: Any, name: str) -> tuple[str, datetime]:
    if type(value) is not str or _UTC_RE.fullmatch(value) is None:
        raise ValueError(name + " must be a canonical UTC timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError(name + " must be a valid UTC timestamp") from exc
    if parsed.tzinfo != timezone.utc:
        raise ValueError(name + " must be UTC")
    return value, parsed


def _count(value: Any, name: str, *, positive: bool = False) -> int:
    lower = 1 if positive else 0
    if type(value) is not int or not lower <= value <= MAX_INT64:
        raise ValueError(name + " must be a valid int64")
    return value


def _finite(value: Any, name: str, *, nullable: bool = False) -> float | None:
    if value is None and nullable:
        return None
    if isinstance(value, bool) or type(value) not in (int, float):
        raise ValueError(name + " must be a finite JSON number" + (" or null" if nullable else ""))
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(name + " must be finite")
    return 0.0 if number == 0.0 else number


def _pct(value: Any, name: str, *, nullable: bool = False) -> float | None:
    number = _finite(value, name, nullable=nullable)
    if number is not None and not 0.0 <= number <= 100.0:
        raise ValueError(name + " must be between 0 and 100")
    return number


def _close(left: float, right: float) -> bool:
    return math.isclose(left, right, rel_tol=1e-12, abs_tol=1e-12)


def _wilson(successes: int, total: int, z: float) -> float | None:
    if total == 0:
        return None
    p = successes / total
    return 100.0 * max(0.0, (
        p + z * z / (2 * total)
        - z * math.sqrt((p * (1.0 - p) + z * z / (4 * total)) / total)
    ) / (1.0 + z * z / total))


def _band(count: int) -> str:
    if count == 0:
        return NO_EVIDENCE
    return PROVISIONAL_N_LT_5 if count < MINIMUM_EVIDENCE_COUNT else DESCRIPTIVE_N_GTE_5


def _route_provenance(
    route: dict[str, Any], *, binding_sha: str, route_name: str, count: int,
) -> dict[str, Any]:
    included = route["included_btc_parent_movement_ids"]
    if (type(included) is not list
            or any(type(v) is not int or not 1 <= v <= MAX_INT64 for v in included)
            or included != sorted(set(included)) or len(included) != count):
        raise ValueError(route_name + " included parent IDs must be sorted, unique, and equal n")
    excluded_raw = route["excluded_btc_parents"]
    if type(excluded_raw) is not list:
        raise ValueError(route_name + " excluded parent provenance must be a list")
    excluded: list[dict[str, Any]] = []
    excluded_ids: set[int] = set()
    for raw in excluded_raw:
        item = _strict_dict(raw, {"btc_parent_movement_id", "reason_code"}, route_name + " exclusion")
        parent_id = _count(item["btc_parent_movement_id"], route_name + " excluded parent", positive=True)
        reason = item["reason_code"]
        if (parent_id in excluded_ids or parent_id in included
                or type(reason) is not str or _REASON_RE.fullmatch(reason) is None):
            raise ValueError(route_name + " exclusion provenance is invalid")
        excluded_ids.add(parent_id)
        excluded.append({"btc_parent_movement_id": parent_id, "reason_code": reason})
    if [v["btc_parent_movement_id"] for v in excluded] != sorted(excluded_ids):
        raise ValueError(route_name + " exclusions must be sorted by parent ID")
    population = {
        "version": ROUTE_POPULATION_VERSION,
        "exact_binding_sha256": binding_sha,
        "route": route_name,
        "included_btc_parent_movement_ids": list(included),
        "excluded_btc_parents": excluded,
    }
    supplied = route["route_population_sha256"]
    if supplied != contract.digest(population):
        raise ValueError(route_name + " route population digest mismatch")
    return {
        "included_btc_parent_movement_ids": list(included),
        "excluded_btc_parents": excluded,
        "source_parent_count": len(included) + len(excluded),
        "route_population_sha256": supplied,
    }


def _probability(route: Any, *, binding_sha: str, z: float,
                 gates: Mapping[str, Any]) -> dict[str, Any]:
    route = _strict_dict(route, _PROBABILITY_FIELDS, "PROBABILITY route")
    count = _count(route["distinct_parent_count"], "PROBABILITY n")
    successes = _count(route["successes"], "PROBABILITY successes")
    failures = _count(route["failures"], "PROBABILITY failures")
    if successes + failures != count:
        raise ValueError("PROBABILITY success and failure counts must equal n")
    provenance = _route_provenance(route, binding_sha=binding_sha,
                                   route_name="PROBABILITY", count=count)
    hit = _pct(route["hit_rate_pct"], "PROBABILITY.hit_rate_pct", nullable=count == 0)
    wilson = _pct(route["wilson_95_lower_pct"], "PROBABILITY.wilson_95_lower_pct", nullable=count == 0)
    if count == 0:
        if hit is not None or wilson is not None:
            raise ValueError("PROBABILITY zero-evidence metrics must be null")
    else:
        expected_hit = 100.0 * successes / count
        expected_wilson = _wilson(successes, count, z)
        assert hit is not None and wilson is not None and expected_wilson is not None
        if not _close(hit, expected_hit) or not _close(wilson, expected_wilson):
            raise ValueError("PROBABILITY derived metrics are inconsistent")
        hit, wilson = expected_hit, expected_wilson
    gate_count = sum((
        hit is not None and hit >= float(gates["hit_rate_pct_gte"]),
        wilson is not None and wilson >= float(gates["wilson_95_lower_pct_gte"]),
    ))
    return provenance | {
        "distinct_parent_count": count, "successes": successes, "failures": failures,
        "hit_rate_pct": hit, "wilson_95_lower_pct": wilson,
        "non_n_gate_pass_count": gate_count,
    }


def _asymmetry(route: Any, *, binding_sha: str,
               gates: Mapping[str, Any]) -> dict[str, Any]:
    route = _strict_dict(route, _ASYMMETRY_FIELDS, "ASYMMETRY route")
    count = _count(route["distinct_parent_count"], "ASYMMETRY n")
    favorable = _count(route["favorable_count"], "ASYMMETRY favorable_count")
    if favorable > count:
        raise ValueError("ASYMMETRY favorable_count must not exceed n")
    provenance = _route_provenance(route, binding_sha=binding_sha,
                                   route_name="ASYMMETRY", count=count)
    sum_mfe = _finite(route["sum_representative_mfe_pct"], "ASYMMETRY sum MFE")
    sum_mae = _finite(route["sum_representative_mae_pct"], "ASYMMETRY sum MAE")
    assert sum_mfe is not None and sum_mae is not None
    if sum_mfe < 0.0 or sum_mae < 0.0:
        raise ValueError("ASYMMETRY excursion sums must be nonnegative")
    supplied_ratio = _finite(route["common_window_asymmetry_ratio"],
                             "ASYMMETRY ratio", nullable=True)
    supplied_dominance = _pct(route["common_window_favorable_dominance_pct"],
                              "ASYMMETRY dominance", nullable=count == 0)
    edge = _finite(route["common_window_median_paired_edge_pct"],
                   "ASYMMETRY median paired edge", nullable=count == 0)
    if count == 0:
        if (favorable != 0 or sum_mfe != 0.0 or sum_mae != 0.0
                or supplied_ratio is not None or supplied_dominance is not None
                or edge is not None):
            raise ValueError("ASYMMETRY zero-evidence aggregates are inconsistent")
        ratio = dominance = None
    else:
        dominance = 100.0 * favorable / count
        if supplied_dominance is None or not _close(supplied_dominance, dominance):
            raise ValueError("ASYMMETRY dominance is inconsistent with favorable_count and n")
        if sum_mae == 0.0:
            if supplied_ratio is not None:
                raise ValueError("ASYMMETRY zero denominator requires a null ratio")
            ratio = None
        else:
            ratio = sum_mfe / sum_mae
            if not math.isfinite(ratio) or supplied_ratio is None or not _close(supplied_ratio, ratio):
                raise ValueError("ASYMMETRY ratio is inconsistent with excursion sums")
    gate_count = sum((
        ratio is not None and ratio >= float(gates["common_window_asymmetry_ratio_gte"]),
        dominance is not None and dominance >= float(gates["common_window_favorable_dominance_pct_gte"]),
        edge is not None and edge > float(gates["common_window_median_paired_edge_pct_gt"]),
    ))
    return provenance | {
        "distinct_parent_count": count, "favorable_count": favorable,
        "sum_representative_mfe_pct": sum_mfe,
        "sum_representative_mae_pct": sum_mae,
        "common_window_asymmetry_ratio": ratio,
        "common_window_favorable_dominance_pct": dominance,
        "common_window_median_paired_edge_pct": edge,
        "non_n_gate_pass_count": gate_count,
    }


def _normalize_receipt(receipt: Any) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    receipt = _strict_dict(receipt, {
        "version", "manifest_sha256", "cohort", "snapshot", "source_versions",
        "evaluator", "sources", "binding_statuses",
    }, "analysis_receipt")
    if receipt["version"] != ANALYSIS_RECEIPT_VERSION:
        raise ValueError("STAGE8_HISTORICAL_RECEIPT_VERSION_MISMATCH")
    if receipt["manifest_sha256"] != contract.MANIFEST_SHA256:
        raise ValueError("STAGE8_HISTORICAL_RECEIPT_MANIFEST_MISMATCH")
    cohort = _strict_dict(receipt["cohort"],
                          {"start_utc", "end_utc_exclusive", "boundary_contract"},
                          "analysis_receipt.cohort")
    start_text, start = _utc(cohort["start_utc"], "cohort.start_utc")
    end_text, end = _utc(cohort["end_utc_exclusive"], "cohort.end_utc_exclusive")
    if cohort["boundary_contract"] != _HALF_OPEN_BOUNDARY or not start < end:
        raise ValueError("STAGE8_HISTORICAL_COHORT_MUST_BE_NONEMPTY_HALF_OPEN")
    snapshot = _strict_dict(receipt["snapshot"], {
        "read_only", "transaction_isolation", "snapshot_scope",
        "database_snapshot_id", "captured_at_utc",
    }, "analysis_receipt.snapshot")
    if (snapshot["read_only"] is not True
            or snapshot["transaction_isolation"] != "repeatable read"
            or snapshot["snapshot_scope"] != _SNAPSHOT_SCOPE):
        raise ValueError("STAGE8_HISTORICAL_SNAPSHOT_NOT_READ_ONLY_REPEATABLE_READ")
    snapshot_id = _text(snapshot["database_snapshot_id"], "snapshot.database_snapshot_id")
    captured_text, captured = _utc(
        snapshot["captured_at_utc"], "snapshot.captured_at_utc"
    )
    if captured < end:
        raise ValueError("STAGE8_HISTORICAL_SNAPSHOT_PRECEDES_COHORT_END")
    source_versions = _strict_dict(receipt["source_versions"], set(_SOURCE_VERSIONS),
                                   "analysis_receipt.source_versions")
    if source_versions != _SOURCE_VERSIONS:
        raise ValueError("STAGE8_HISTORICAL_SOURCE_VERSION_MISMATCH")
    evaluator = _strict_dict(receipt["evaluator"], {
        "evaluation_input_version", "adapter_version", "code_commit", "query_sha256",
    }, "analysis_receipt.evaluator")
    if evaluator["evaluation_input_version"] != EVALUATION_INPUT_VERSION:
        raise ValueError("STAGE8_HISTORICAL_EVALUATION_INPUT_VERSION_MISMATCH")
    adapter_version = _text(evaluator["adapter_version"], "evaluator.adapter_version")
    if type(evaluator["code_commit"]) is not str or _COMMIT_RE.fullmatch(evaluator["code_commit"]) is None:
        raise ValueError("evaluator.code_commit must be a full hexadecimal commit")
    if type(evaluator["query_sha256"]) is not str or _HEX_RE.fullmatch(evaluator["query_sha256"]) is None:
        raise ValueError("evaluator.query_sha256 must be SHA-256")
    sources = receipt["sources"]
    if type(sources) is not list or not sources:
        raise ValueError("analysis_receipt.sources must be nonempty")
    normalized_sources: list[dict[str, Any]] = []
    source_ids: set[str] = set()
    for raw in sources:
        item = _strict_dict(raw, {"source_id", "source_version", "high_water_field", "high_water_value"},
                            "analysis_receipt source")
        source_id = _text(item["source_id"], "source.source_id", 128)
        if source_id in source_ids:
            raise ValueError("STAGE8_HISTORICAL_DUPLICATE_SOURCE")
        source_ids.add(source_id)
        normalized_sources.append({
            "source_id": source_id,
            "source_version": _text(item["source_version"], "source.source_version"),
            "high_water_field": _text(item["high_water_field"], "source.high_water_field", 128),
            "high_water_value": _count(item["high_water_value"], "source.high_water_value"),
        })
    normalized_sources.sort(key=lambda value: value["source_id"])
    statuses = receipt["binding_statuses"]
    if type(statuses) is not list:
        raise ValueError("analysis_receipt.binding_statuses must be a list")
    universe_rows = frozen_binding_universe()
    universe = {row["exact_binding_sha256"] for row in universe_rows}
    status_by_sha: dict[str, dict[str, Any]] = {}
    for raw in statuses:
        item = _strict_dict(raw, {"exact_binding_sha256", "status", "reason_code", "evaluation_sha256"},
                            "analysis_receipt binding status")
        binding_sha = item["exact_binding_sha256"]
        if type(binding_sha) is not str or binding_sha not in universe:
            raise ValueError("STAGE8_HISTORICAL_STATUS_UNKNOWN_BINDING")
        if binding_sha in status_by_sha:
            raise ValueError("STAGE8_HISTORICAL_STATUS_DUPLICATE_BINDING")
        status, reason, evaluation_sha = item["status"], item["reason_code"], item["evaluation_sha256"]
        if status not in EVALUATION_STATUSES:
            raise ValueError("STAGE8_HISTORICAL_UNKNOWN_EVALUATION_STATUS")
        if status == EVALUATED:
            if reason is not None or type(evaluation_sha) is not str or _HEX_RE.fullmatch(evaluation_sha) is None:
                raise ValueError("EVALUATED status requires null reason and evaluation SHA-256")
        elif (type(reason) is not str or _REASON_RE.fullmatch(reason) is None
              or evaluation_sha is not None):
            raise ValueError("non-evaluated status requires reason and null evaluation SHA-256")
        status_by_sha[binding_sha] = dict(item)
    if universe - set(status_by_sha):
        raise ValueError("STAGE8_HISTORICAL_STATUS_MISSING_BINDING")
    if len(status_by_sha) != len(universe):
        raise ValueError("STAGE8_HISTORICAL_STATUS_CARDINALITY_MISMATCH")
    normalized = {
        "version": ANALYSIS_RECEIPT_VERSION,
        "manifest_sha256": contract.MANIFEST_SHA256,
        "cohort": {"start_utc": start_text, "end_utc_exclusive": end_text,
                   "boundary_contract": _HALF_OPEN_BOUNDARY},
        "snapshot": {"read_only": True, "transaction_isolation": "repeatable read",
                     "snapshot_scope": _SNAPSHOT_SCOPE,
                     "database_snapshot_id": snapshot_id,
                     "captured_at_utc": captured_text},
        "source_versions": dict(_SOURCE_VERSIONS),
        "evaluator": {"evaluation_input_version": EVALUATION_INPUT_VERSION,
                      "adapter_version": adapter_version,
                      "code_commit": evaluator["code_commit"],
                      "query_sha256": evaluator["query_sha256"]},
        "sources": normalized_sources,
        "binding_statuses": [status_by_sha[row["exact_binding_sha256"]] for row in universe_rows],
    }
    return normalized, status_by_sha


def _normalize_evaluations(evaluations: Any, *, receipt: dict[str, Any],
                           statuses: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    if not isinstance(evaluations, Mapping):
        raise ValueError("historical evaluations must be a mapping")
    universe = set(statuses)
    manifest = _manifest()
    cohort_sha = contract.digest(receipt["cohort"])
    source_ledger_sha = contract.digest(receipt["sources"])
    snapshot_id = receipt["snapshot"]["database_snapshot_id"]
    evaluator_version = receipt["evaluator"]["adapter_version"]
    normalized: dict[str, dict[str, Any]] = {}
    seen: set[str] = set()
    expected_fields = {
        "evaluation_input_version", "manifest_sha256", "exact_binding_sha256",
        "evaluator_version", "analysis_snapshot_id", "cohort_sha256",
        "source_ledger_sha256", "routes", "authority",
    }
    for binding_sha, evaluation in evaluations.items():
        if type(binding_sha) is not str or binding_sha not in universe:
            raise ValueError("STAGE8_HISTORICAL_UNKNOWN_BINDING")
        if binding_sha in seen:
            raise ValueError("STAGE8_HISTORICAL_DUPLICATE_BINDING")
        seen.add(binding_sha)
        evaluation = _strict_dict(evaluation, expected_fields, "historical evaluation")
        if (evaluation["evaluation_input_version"] != EVALUATION_INPUT_VERSION
                or evaluation["manifest_sha256"] != contract.MANIFEST_SHA256
                or evaluation["exact_binding_sha256"] != binding_sha
                or evaluation["evaluator_version"] != evaluator_version
                or evaluation["analysis_snapshot_id"] != snapshot_id
                or evaluation["cohort_sha256"] != cohort_sha
                or evaluation["source_ledger_sha256"] != source_ledger_sha):
            raise ValueError("STAGE8_HISTORICAL_EVALUATION_PROVENANCE_MISMATCH")
        if evaluation["authority"] != _AUTHORITY_FALSE:
            raise ValueError("historical input cannot assert authority")
        if statuses[binding_sha]["status"] != EVALUATED:
            raise ValueError("STAGE8_HISTORICAL_NON_EVALUATED_BINDING_HAS_EVALUATION")
        if statuses[binding_sha]["evaluation_sha256"] != contract.digest(evaluation):
            raise ValueError("STAGE8_HISTORICAL_EVALUATION_DIGEST_MISMATCH")
        routes = evaluation["routes"]
        if type(routes) is not dict or set(routes) != set(ROUTES):
            raise ValueError("historical evaluation must contain exactly two known routes")
        normalized[binding_sha] = {
            "PROBABILITY": _probability(
                routes["PROBABILITY"], binding_sha=binding_sha,
                z=float(manifest["acceptance"]["metric_definitions"]["wilson_z"]),
                gates=manifest["acceptance"]["probability"],
            ),
            "ASYMMETRY": _asymmetry(
                routes["ASYMMETRY"], binding_sha=binding_sha,
                gates=manifest["acceptance"]["asymmetry"],
            ),
        }
    expected = {sha for sha, value in statuses.items() if value["status"] == EVALUATED}
    if expected - set(normalized):
        raise ValueError("STAGE8_HISTORICAL_EVALUATED_BINDING_MISSING_EVALUATION")
    return normalized


def _rank_metrics(route: str, metrics: Mapping[str, Any]) -> tuple[float, ...] | None:
    if not metrics["distinct_parent_count"]:
        return None
    if route == "PROBABILITY":
        return (float(metrics["non_n_gate_pass_count"]), metrics["wilson_95_lower_pct"],
                metrics["hit_rate_pct"], float(metrics["distinct_parent_count"]))
    ratio = metrics["common_window_asymmetry_ratio"]
    if ratio is None:
        return None
    return (float(metrics["non_n_gate_pass_count"]), float(metrics["distinct_parent_count"]),
            ratio, metrics["common_window_favorable_dominance_pct"],
            metrics["common_window_median_paired_edge_pct"])


def _apply_ranks(rows: list[dict[str, Any]], route: str) -> None:
    key = route.lower()
    partitions: dict[tuple[str, int, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row["evaluation_status"] == EVALUATED:
            partitions[(row["direction"], row["threshold_bps"],
                        row[key]["evidence_band"])].append(row)
    for values in partitions.values():
        def order(row: Mapping[str, Any]) -> tuple[Any, ...]:
            metrics = row[key]["_rank_metrics"]
            identity = (row["candidate_id"], row["scope_id"], row["exact_binding_sha256"])
            return (1, *identity) if metrics is None else (0, *(-v for v in metrics), *identity)
        previous = None
        dense_rank = 0
        for ordinal, row in enumerate(sorted(values, key=order), start=1):
            ranking = row[key]
            metrics = ranking.pop("_rank_metrics")
            ranking["display_ordinal"] = ordinal
            ranking["rank_eligible"] = metrics is not None
            if metrics is None:
                ranking["dense_rank"] = None
            else:
                if previous is None or metrics != previous:
                    dense_rank += 1
                    previous = metrics
                ranking["dense_rank"] = dense_rank


def _unavailable_route() -> dict[str, Any]:
    return {"evidence_band": None, "partition": None, "metrics": None,
            "display_ordinal": None, "dense_rank": None, "rank_eligible": False}


def build_leaderboard(evaluations: Mapping[str, Mapping[str, Any]], *,
                      analysis_receipt: Any) -> dict[str, Any]:
    receipt, statuses = _normalize_receipt(analysis_receipt)
    normalized = _normalize_evaluations(evaluations, receipt=receipt, statuses=statuses)
    rows: list[dict[str, Any]] = []
    for identity in frozen_binding_universe():
        binding_sha = identity["exact_binding_sha256"]
        status = statuses[binding_sha]
        row = dict(identity) | {
            "evaluation_status": status["status"],
            "status_reason_code": status["reason_code"],
            "descriptive_discovery_only": True,
            "result_scope": "HISTORICAL_DISCOVERY_ONLY",
            "activation_effect": "NONE",
            "authority": dict(_AUTHORITY_FALSE),
        }
        if status["status"] == EVALUATED:
            probability = normalized[binding_sha]["PROBABILITY"]
            asymmetry = normalized[binding_sha]["ASYMMETRY"]
            for route_name, metrics in (("probability", probability), ("asymmetry", asymmetry)):
                band = _band(metrics["distinct_parent_count"])
                row[route_name] = {
                    "evidence_band": band,
                    "partition": {"direction": identity["direction"],
                                  "threshold_bps": identity["threshold_bps"],
                                  "evidence_band": band},
                    "metrics": metrics,
                    "_rank_metrics": _rank_metrics(route_name.upper(), metrics),
                }
        else:
            row["probability"] = _unavailable_route()
            row["asymmetry"] = _unavailable_route()
        rows.append(row)
    _apply_ranks(rows, "PROBABILITY")
    _apply_ranks(rows, "ASYMMETRY")
    status_counts = {status: sum(row["evaluation_status"] == status for row in rows)
                     for status in EVALUATION_STATUSES}
    result = {
        "version": VERSION,
        "manifest_sha256": contract.MANIFEST_SHA256,
        "source_kind": "RECEIPTED_HISTORICAL_DISCOVERY_DIAGNOSTICS",
        "result_scope": "HISTORICAL_DISCOVERY_ONLY",
        "maximum_result": "DESCRIPTIVE_DISCOVERY_ONLY",
        "binding_count": len(rows),
        "complete_binding_status_count": len(statuses),
        "complete_traversal_attested": True,
        "input_evaluation_count": len(normalized),
        "evaluation_status_counts": status_counts,
        "analysis_receipt": receipt,
        "analysis_receipt_sha256": contract.digest(receipt),
        "ranking_contract": {
            "only_evaluated_bindings_are_rankable": True,
            "blocked_or_not_evaluated_is_not_no_evidence": True,
            "routes_ranked_separately": True,
            "partition_keys": ["direction", "threshold_bps", "evidence_band"],
            "evidence_bands": list(EVIDENCE_BANDS),
            "minimum_evidence_count": MINIMUM_EVIDENCE_COUNT,
            "probability_metric_order_desc": ["non_n_gate_pass_count", "wilson_95_lower_pct",
                                               "hit_rate_pct", "distinct_parent_count"],
            "asymmetry_metric_order_desc": ["non_n_gate_pass_count", "distinct_parent_count",
                                             "common_window_asymmetry_ratio",
                                             "common_window_favorable_dominance_pct",
                                             "common_window_median_paired_edge_pct"],
            "non_n_gate_pass_count_is_unweighted_categorical_tier": True,
            "dense_metric_ties": True,
            "deterministic_display_tiebreak_asc": ["candidate_id", "scope_id",
                                                    "exact_binding_sha256"],
            "weighted_composite": False,
            "composite_score": None,
        },
        "descriptive_discovery_only": True,
        "activation_effect": "NONE",
        "authority": dict(_AUTHORITY_FALSE),
        "rows": rows,
    }
    result["leaderboard_sha256"] = contract.digest(result)
    return result
