"""Outcome-blind, immutable definition for the first Stage-8 research slice.

This module declares and hashes a contract. It does not register a prospective
freeze, inspect observations, calculate metrics, qualify a formula or deliver
anything. It has only standard-library imports and no import-time I/O.
"""
from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any, Mapping


VERSION = "stage8-operational-model-contract-v1"
SOURCE_VERSION = "stage8-neutral-v4-watch-v2-source-v1"
PROJECTION_VERSION = "stage8-watch-signed-model-sidecar-v1"
CANDIDATE_VERSION = "stage8-first-tranche-single-model-aligned65-60m-v1"
LABEL_VERSION = "stage8-ordered-v7-60m-eight-thresholds-full-window-v1"
INDEPENDENCE_VERSION = "stage8-earliest-match-per-btc-parent-v1"
ACCEPTANCE_VERSION = "stage8-five-parent-probability-or-asymmetry-v1"
HASH_VERSION = "stage8-strict-json-sha256-v1"
BINANCE_SYMBOLS = ("BTC", "ETH", "SOL", "DOGE", "ZEC", "BNB", "XRP")
MODELS = ("positioning", "futures_flow", "spot_flow")
DIRECTIONS = ("LONG", "SHORT")
THRESHOLDS_BPS = (25, 50, 75, 100, 125, 150, 175, 200)


def canonical(value: Any) -> str:
    """Strict recursive JSON matching PostgreSQL JSONB's numeric spelling.

    Expand the finite float's shortest JSON decimal, not its binary expansion.
    JSONB removes a negative-zero sign and prints exponent numbers in plain
    decimal; an ordinary float such as 5.0 retains its decimal scale. Boolean
    tokens remain distinct from numbers. Strings and sorted object keys retain
    the previous UTF-8 JSON representation. SQL key sorting must use C collation.
    """
    active_containers: set[int] = set()

    def render(item: Any) -> str:
        kind = type(item)
        if item is None:
            return "null"
        if kind is bool:
            return "true" if item else "false"
        if kind is str:
            return json.dumps(item, ensure_ascii=False)
        if kind is int:
            return str(item)
        if kind is float:
            # allow_nan=False rejects NaN/Infinity before Decimal receives them.
            spelling = json.dumps(item, allow_nan=False)
            return "0.0" if item == 0.0 else format(Decimal(spelling), "f")
        if kind not in (list, dict) or (
                kind is dict and any(type(key) is not str for key in item)):
            raise ValueError("contract values must be strict JSON")
        identity = id(item)
        if identity in active_containers:
            raise ValueError("contract values must not contain circular references")
        active_containers.add(identity)
        try:
            if kind is list:
                return "[" + ",".join(render(child) for child in item) + "]"
            return "{" + ",".join(
                json.dumps(key, ensure_ascii=False) + ":" + render(item[key])
                for key in sorted(item)
            ) + "}"
        finally:
            active_containers.remove(identity)
    return render(value)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def _definition() -> dict:
    scopes = [{"scope_id": "BINANCE_" + symbol, "symbols": [symbol],
               "price_route": "BINANCE_SPOT_1M"} for symbol in BINANCE_SYMBOLS]
    scopes += [
        {"scope_id": "ALL_BINANCE7", "symbols": list(BINANCE_SYMBOLS),
         "price_route": "BINANCE_SPOT_1M"},
        {"scope_id": "HYPE_SPOT_107", "symbols": ["HYPE"],
         "price_route": "HYPERLIQUID_SPOT_@107_1M"},
    ]
    candidates = [
        {"candidate_id": model.upper() + "_ALIGNED65_" + direction,
         "model": model, "direction": direction,
         "feature": "watch.models." + model + ".aligned_score",
         "operator": ">=", "value": 65}
        for model in MODELS for direction in DIRECTIONS
    ]
    return {
        "version": VERSION,
        "hash_version": HASH_VERSION,
        "definition_state": "LOCAL_DEFINITION_ONLY",
        "source_base_commit": "96bfdd732490464923f2b07dd96af7dff1e3b908",
        "source": {
            "version": SOURCE_VERSION,
            "audit_version": "operational-score-source-audit-v2",
            "sampler_version": "prospective-neutral-anchor-v4-decision-features-frozen",
            "feature_policy_version": "prospective-decision-feature-bundle-v1",
            "anchor_model_score_status": "ABSENT",
            "watch_version": "watch-operational-scores-v2",
            "watch_population": "all-top8-watch-scans-before-display-v1",
            "watch_hash_version": "json-integer-float-zero-normalized-v1",
            "population": "ALL_V4_ATTEMPTS_IN_DECLARED_SYMBOL_AND_TIME_SCOPE",
            "retain_attempt_statuses": ["EVALUABLE", "UNEVALUABLE", "COVERAGE_EXCLUDED"],
            "capture_selection": "LATEST_DURABLY_PRIOR_WATCH_SHARED_THEN_VALIDATE",
            "capture_order_desc": ["max(available_at_utc,created_at_utc)", "snapshot_set_id"],
            "max_capture_age_seconds": 300,
            "capture_fallback_to_older_valid": False,
            "outer_archive_hash_verified": False,
            "maxpain": {
                "slot_key": ["timeframe", "source_side"],
                "statuses": ["SCORED", "MISSING_INPUT", "INACTIVE_TARGET"],
                "liquidated_side_to_candidate_direction": {"SHORT": "LONG", "LONG": "SHORT"},
                "selected_is_delivery_or_signal": False,
                "additive_components": ["directional_alignment", "target_proximity",
                                        "cluster_confidence", "relative_gap"],
                "missing_amount_is_zero": False,
                "first_tranche_predicates": False,
            },
            "model_unavailable_fallback_zero_is_observed": False,
            "complete_capture_guarantees_every_model_available": False,
        },
        "projection": {
            "version": PROJECTION_VERSION,
            "storage": "SEPARATE_SIDECAR_NO_ANCHOR_OR_EVENT_REWRITE",
            "raw_score_path": "coins.<symbol>.models.<model>.score",
            "raw_score_range": [-100, 100],
            "direction_multiplier": {"LONG": 1, "SHORT": -1},
            "aligned_score": "raw_score * direction_multiplier",
            "requires": ["VALID_ANCHOR_AUTHORITY", "VALID_CAPTURE", "MODEL_AVAILABLE_TRUE",
                         "CAPTURE_STATUS_AVAILABLE", "FINITE_SCORE", "VALID_MODEL_SOURCE_PROVENANCE"],
            "missing_value": "UNKNOWN_NOT_ZERO_OR_FALSE",
            "rescore_from_current_inputs": False,
            "infer_delivery_from_watch_selection": False,
        },
        "candidates": {
            "version": CANDIDATE_VERSION,
            "scope_of_work": "FIRST_FIXED_MODEL_ONLY_VERTICAL_TRANCHE_NOT_COMPLETE_STAGE8_CATALOG",
            "definitions": candidates,
            "scopes": scopes,
            "scope_membership": "EXACT_FROZEN_SYMBOLS_NO_DYNAMIC_DROPOUT",
            "unknown_hype": "KEEP_UNKNOWN_IN_HYPE_SCOPE_NEVER_RENAME_OR_SHRINK_A_POOL",
            "selection_basis": "PREDECLARED_EXISTING_ALIGNED_SCORE_65_RULE_NOT_OUTCOMES",
            "family_capacity": len(candidates) * len(scopes) * len(THRESHOLDS_BPS),
            "multiplicity": "REPORT_ALL_432_CELLS_AND_OVERLAP_NO_MULTIPLICITY_ADJUSTED_CLAIM",
            "scope_expansion_requires_new_version_and_freeze": True,
        },
        "labels": {
            "version": LABEL_VERSION,
            "method_version": "ordered-first-touch-v7",
            "window_minutes": 60,
            "thresholds_bps": list(THRESHOLDS_BPS),
            "first_slice_basis": "ENGINEERING_60M_FIRST_ALL_EXISTING_THRESHOLDS_NOT_OUTCOME_OPTIMIZED",
            "alternative_window_or_threshold_fallback": False,
            "probability_decisive_statuses": ["SUCCESS", "FAILURE"],
            "persisted_statuses": ["OPEN", "SUCCESS", "FAILURE", "UNRESOLVED", "DATA_MISSING"],
            "nondecisive_persisted_statuses": ["OPEN", "UNRESOLVED", "DATA_MISSING"],
            "reported_nondecisive_states": ["OPEN", "AMBIGUOUS", "NO_TOUCH", "DATA_MISSING", "UNKNOWN"],
            "unresolved_terminal_reasons": ["SAME_CANDLE_BOTH", "OBSERVATION_WINDOW_CLOSED_NO_TOUCH"],
            "preserve_raw_status_and_terminal_reason": True,
            "probability_denominator": "DECISIVE_LABELS_OF_FROZEN_PARENT_REPRESENTATIVES",
            "retain_all_representatives_and_nondecisive_counts": True,
            "asymmetry_source_version": "common-window-spot-1m-v1",
            "asymmetry_measurement": "FULL_FIXED_60M_HORIZON_SAME_FROZEN_REPRESENTATIVES",
            "first_touch_truncated_excursions_allowed_for_asymmetry": False,
            "hype_outcome_instrument": "@107",
            "hype_operational_route_relabel_to_spot": False,
            "outcome_revision_asof": "EXPLICIT_READ_CUTOFF_NO_HISTORICAL_REVISION_RECONSTRUCTION",
        },
        "independence": {
            "version": INDEPENDENCE_VERSION,
            "parent_policy_version": "btc-parent-close-reversal-200bps-v1",
            "unit": "btc_parent_movement_id",
            "key": ["exact_binding_sha256", "btc_parent_movement_id"],
            "representative": "EARLIEST_VALID_MATCH_BEFORE_INSPECTING_LABELS",
            "representative_order_asc": ["decision_time_utc", "symbol", "anchor_slot_id", "event_id"],
            "attempt_deduplication": "EXACT_ANCHOR_SLOT_AND_EVENT_ID",
            "pool_cross_symbol_votes_per_parent": 1,
            "replace_unknown_representative_with_later_decisive": False,
            "unknown_earlier_membership_or_match": "BLOCK_PARENT_REPRESENTATIVE_UNTIL_COVERAGE_PROVEN",
            "missing_membership": "UNKNOWN_NOT_A_NEW_PARENT_OR_NO_WAVE",
            "statistical_independence_claim": False,
        },
        "acceptance": {
            "version": ACCEPTANCE_VERSION,
            "atomic_expression": "N_ROUTE_DISTINCT_BTC_PARENTS >= 5 AND (PROBABILITY OR ASYMMETRY)",
            "minimum_distinct_parents_per_passing_route": 5,
            "combination": "PROBABILITY_OR_ASYMMETRY",
            "route_cohort": "ONE_OUTCOME_BLIND_FROZEN_REPRESENTATIVE_SET_PER_EXACT_BINDING",
            "route_eligible_subsets": "DISCLOSE_EXACT_PARENT_IDS_NO_CROSS_ROUTE_COUNT_BORROWING",
            "probability": {"hit_rate_pct_gte": 70, "wilson_95_lower_pct_gte": 40},
            "asymmetry": {"common_window_asymmetry_ratio_gte": 1.5,
                          "common_window_favorable_dominance_pct_gte": 60,
                          "common_window_median_paired_edge_pct_gt": 0},
            "numeric_threshold_source": "docs/ORDERED_V7_ACCEPTANCE_POLICY_V1.md",
            "numeric_threshold_inheritance": "NUMERIC_CUTOFFS_ONLY_NOT_OLD_AND_OR_FRESH3_ADMISSION",
            "numeric_threshold_claim": "PREDECLARED_RESEARCH_HEURISTIC_NOT_PROVEN_PROFITABILITY",
            "metric_definitions": {
                "weighting": "ONE_UNWEIGHTED_VOTE_PER_FROZEN_PARENT_REPRESENTATIVE",
                "hit_rate_pct": "100*successes/(successes+failures)",
                "wilson_method": "TWO_SIDED_95_PERCENT_WILSON_SCORE_LOWER_BOUND_NO_CONTINUITY_CORRECTION",
                "wilson_z": 1.959963984540054,
                "wilson_95_lower_pct": "100*max(0,(p+z*z/(2*n)-z*sqrt((p*(1-p)+z*z/(4*n))/n))/(1+z*z/n))",
                "wilson_p": "successes/n",
                "wilson_n": "successes+failures",
                "no_decisive_labels": "UNKNOWN_NOT_ZERO_PERCENT",
                "excursion_unit": "NONNEGATIVE_FINITE_PERCENT_OF_SAME_REPRESENTATIVE_ENTRY_PRICE",
                "within_parent_excursions": "SINGLE_FROZEN_EARLIEST_REPRESENTATIVE_NO_MULTI_COIN_MIN_MAX_COLLAPSE",
                "common_window_asymmetry_ratio": "sum(representative_mfe_pct)/sum(representative_mae_pct)",
                "positive_mfe_zero_mae": "ZERO_DENOMINATOR_RATIO_NULL_ASYMMETRY_ROUTE_UNAVAILABLE",
                "zero_mfe_zero_mae": "UNDEFINED_ZERO_ZERO_RATIO_NULL_DOES_NOT_PASS",
                "common_window_favorable_dominance_pct": "100*count(mfe_pct>mae_pct)/full_window_parent_count",
                "common_window_median_paired_edge_pct": "median(mfe_pct-mae_pct)",
                "even_sample_median": "ARITHMETIC_MEAN_OF_TWO_MIDDLE_SORTED_VALUES",
                "json_nonfinite_allowed": False,
            },
            "fresh_three_parent_route": False,
            "unavailable_other_route_blocks_passing_route": False,
            "shared_coverage_or_provenance_failure_blocks_all_routes": True,
            "maximum_result": "EXPERIMENTAL_RESEARCH_ONLY",
        },
        "prospective_registration": {
            "implemented": False,
            "local_definition_is_durable_registry_freeze": False,
            "require_exact_binding_persisted_before_prospective_evidence": True,
            "eligible_parent_rule": "PARENT_START_STRICTLY_AFTER_REAL_DURABLE_FREEZE",
            "existing_or_inspected_parent_evidence": "DISCOVERY_ONLY_NOT_RETROACTIVE_PROSPECTIVE",
            "new_candidate_or_scope_or_policy_requires_new_freeze": True,
        },
        "runtime": {"enabled": False, "database_access": False, "worker_wiring": False,
                    "formula_qualification": False, "telegram": False, "live": False,
                    "trade_execution": False},
    }


# Keep immutable serialized bytes internally; callers receive independent copies.
_FROZEN_JSON = canonical(_definition())
MANIFEST_SHA256 = "cb8f23b6cfbefa637fec18f47200c7432ed106bdaa7bc85374cab2b81b5ba262"


def frozen_manifest() -> dict:
    return json.loads(_FROZEN_JSON)


def validate_manifest(value: Mapping[str, Any]) -> None:
    """Reject any changed contract, even if a caller supplies a fresh hash."""
    if canonical(value) != _FROZEN_JSON or digest(value) != MANIFEST_SHA256:
        raise ValueError("STAGE8_MANIFEST_NOT_EXACT_FROZEN_DEFINITION")


def exact_binding(*, scope_id: str, candidate_id: str, threshold_bps: int) -> dict:
    """Outcome-blind identity only; this does not create a registry freeze."""
    manifest = frozen_manifest()
    validate_manifest(manifest)
    if type(scope_id) is not str or type(candidate_id) is not str:
        raise ValueError("STAGE8_SCOPE_OR_CANDIDATE_NOT_PREDECLARED")
    scopes = {scope["scope_id"]: scope for scope in manifest["candidates"]["scopes"]}
    candidates = {candidate["candidate_id"]: candidate
                  for candidate in manifest["candidates"]["definitions"]}
    if scope_id not in scopes or candidate_id not in candidates:
        raise ValueError("STAGE8_SCOPE_OR_CANDIDATE_NOT_PREDECLARED")
    if type(threshold_bps) is not int or threshold_bps not in manifest["labels"]["thresholds_bps"]:
        raise ValueError("STAGE8_THRESHOLD_NOT_PREDECLARED")
    binding = {"version": manifest["version"], "manifest_sha256": digest(manifest),
               "scope": scopes[scope_id], "candidate": candidates[candidate_id],
               "source_version": manifest["source"]["version"],
               "projection_version": manifest["projection"]["version"],
               "label_version": manifest["labels"]["version"],
               "independence_version": manifest["independence"]["version"],
               "acceptance_version": manifest["acceptance"]["version"],
               "window_minutes": manifest["labels"]["window_minutes"],
               "threshold_bps": threshold_bps}
    return {"binding": binding, "binding_sha256": digest(binding)}


def validate_exact_binding(value: Mapping[str, Any]) -> None:
    try:
        expected = exact_binding(scope_id=value["binding"]["scope"]["scope_id"],
                                 candidate_id=value["binding"]["candidate"]["candidate_id"],
                                 threshold_bps=value["binding"]["threshold_bps"])
    except (KeyError, TypeError) as exc:
        raise ValueError("STAGE8_EXACT_BINDING_INVALID") from exc
    if canonical(value) != canonical(expected):
        raise ValueError("STAGE8_EXACT_BINDING_INVALID")
