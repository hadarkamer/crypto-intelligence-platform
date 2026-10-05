"""Bounded read-only leaderboard for the existing ordered-v7 Watch corpus.

This is a descriptive inspection surface, not a Stage-8 evidence or promotion
surface.  It deliberately enumerates the active catalog before joining outcome
aggregates, so candidates with zero observations cannot disappear.  Rankings
are separate for the probability and full-window asymmetry routes and never
apply a minimum-evidence filter.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import argparse
import hashlib
import json
import math
import os
from typing import Any, Mapping, Sequence

import research_watch_scan_formula_score_change as coin_formula_catalog
import research_watch_scan_formula_timeframe as timeframe_formula_catalog

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # Pure aggregation and selftests remain usable without it.
    psycopg = None
    dict_row = None


VERSION = "watch-descriptive-ordered-v7-leaderboard-v1"
SOURCE_KIND = "WATCH_ALL_FORMULA_DESCRIPTIVE_COMPARATOR"
SOURCE_CONTRACT = "WATCH_DESCRIPTIVE_ALL_CAPTURE_PHASES"
DATABASE_URL_ENV = "RESEARCH_WATCH_FORMULA_LEADERBOARD_DATABASE_URL"
Z_95 = 1.959963984540054
DIMENSIONS = ("COIN", "SELECTED_TIMEFRAME")
COIN_TIMEFRAME = "NOT_APPLICABLE"
TIMEFRAMES = ("12h", "24h", "48h", "3d", "1w", "2w", "1m")
SYMBOL_SCOPES = ("ALL", "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ZEC", "HYPE")
DIRECTIONS = ("LONG", "SHORT")
WINDOWS = (60, 240, 720, 1440)
THRESHOLDS = (25, 50, 75, 100, 125, 150, 175, 200)
MAX_TOP_PER_ROUTE = 20
DEFAULT_TOP_PER_ROUTE = 5
PROBABILITY_HIT_RATE_FLOOR_PCT = 70.0
PROBABILITY_WILSON_FLOOR_PCT = 40.0
ASYMMETRY_RATIO_FLOOR = 1.5
ASYMMETRY_DOMINANCE_FLOOR_PCT = 60.0
ASYMMETRY_MEDIAN_EDGE_FLOOR_PCT = 0.0
REFERENCE_FLOOR_POLICY_VERSION = "ordered-v7-common-window-research-acceptance-v1-20260907"
REFERENCE_FLOOR_DOCUMENT = "docs/ORDERED_V7_ACCEPTANCE_POLICY_V1.md"
NO_EVIDENCE_BAND = "N_0_NO_EVIDENCE"
PROVISIONAL_BAND = "N_LT_5_PROVISIONAL"
DESCRIPTIVE_BAND = "N_GTE_5_DESCRIPTIVE"
RANKABLE_BANDS = (DESCRIPTIVE_BAND, PROVISIONAL_BAND)

_AUTHORITY_FALSE = {
    "statistical_test_performed": False,
    "qualifies_as_prospective_formula_evidence": False,
    "server_replay_verified": False,
    "research_qualified": False,
    "live_delivery_authorized": False,
    "telegram_delivery_authorized": False,
    "trade_execution_authorized": False,
    "deployment_authorized": False,
}


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Not JSON serializable: {type(value).__name__}")


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False, default=_json_default)


def _database_url() -> str:
    """Return only the purpose-specific read URL; never fall back to primary."""
    return os.getenv(DATABASE_URL_ENV, "").strip()


@contextmanager
def readonly_connection():
    """Open one verified repeatable-read, read-only snapshot."""
    url = _database_url()
    if not url:
        raise RuntimeError(f"{DATABASE_URL_ENV} is required")
    if psycopg is None:
        raise RuntimeError("psycopg is unavailable")
    with psycopg.connect(
        url,
        row_factory=dict_row,
        connect_timeout=5,
        options=("-c default_transaction_read_only=on "
                 "-c statement_timeout=15000 -c lock_timeout=1000 "
                 "-c idle_in_transaction_session_timeout=20000"),
    ) as conn:
        with conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            yield conn


def _request(*, dimension: str, symbol_scope: str, window_minutes: int,
             timeframe: str, analysis_direction: str = "ALL",
             threshold_bps: int | str = "ALL", top_per_route: int = DEFAULT_TOP_PER_ROUTE) -> dict:
    if dimension not in DIMENSIONS:
        raise ValueError("dimension must be exactly COIN or SELECTED_TIMEFRAME")
    if symbol_scope not in SYMBOL_SCOPES:
        raise ValueError("unsupported exact symbol_scope")
    if type(window_minutes) is not int or window_minutes not in WINDOWS:
        raise ValueError("unsupported exact window_minutes")
    if dimension == "COIN":
        if timeframe != COIN_TIMEFRAME:
            raise ValueError(f"COIN requires timeframe={COIN_TIMEFRAME}")
    elif timeframe not in TIMEFRAMES:
        raise ValueError("SELECTED_TIMEFRAME requires one exact captured timeframe")
    if analysis_direction == "ALL":
        directions = list(DIRECTIONS)
    elif analysis_direction in DIRECTIONS:
        directions = [analysis_direction]
    else:
        raise ValueError("analysis_direction must be LONG, SHORT or ALL")
    if threshold_bps == "ALL":
        thresholds = list(THRESHOLDS)
    elif type(threshold_bps) is int and threshold_bps in THRESHOLDS:
        thresholds = [threshold_bps]
    else:
        raise ValueError("threshold_bps must be one frozen threshold or ALL")
    if type(top_per_route) is not int or not 1 <= top_per_route <= MAX_TOP_PER_ROUTE:
        raise ValueError(f"top_per_route must be between 1 and {MAX_TOP_PER_ROUTE}")
    return {
        "dimension": dimension,
        "symbol_scope": symbol_scope,
        "window_minutes": window_minutes,
        "timeframe": timeframe,
        "requested_analysis_direction": analysis_direction,
        "requested_threshold_bps": threshold_bps,
        "analysis_directions": directions,
        "thresholds_bps": thresholds,
        "top_per_route": top_per_route,
    }


def wilson_95_lower_pct(successes: int, total: int) -> float | None:
    if type(successes) is not int or type(total) is not int or total < 0 or not 0 <= successes <= total:
        raise ValueError("invalid Wilson counts")
    if total == 0:
        return None
    p = successes / total
    z2 = Z_95 * Z_95
    numerator = p + z2 / (2 * total) - Z_95 * math.sqrt(
        (p * (1 - p) + z2 / (4 * total)) / total
    )
    return 100.0 * max(0.0, numerator / (1 + z2 / total))


def _count(value: Any, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"invalid nonnegative count: {name}")
    return value


def _finite(value: Any, name: str, *, nonnegative: bool = False) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"invalid numeric value: {name}")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"invalid numeric value: {name}") from exc
    if not math.isfinite(result) or (nonnegative and result < 0):
        raise ValueError(f"invalid finite value: {name}")
    return result


def _definition(value: Any) -> dict:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError("catalog definition is not JSON") from exc
    if not isinstance(value, Mapping):
        raise ValueError("catalog definition must be an object")
    return dict(value)


def evidence_band(n: int) -> str:
    _count(n, "evidence_n")
    if n == 0:
        return NO_EVIDENCE_BAND
    if n < 5:
        return PROVISIONAL_BAND
    return DESCRIPTIVE_BAND


def _analysis_direction(base_direction: str, orientation: str) -> str:
    if orientation not in ("NORMAL", "INVERSE"):
        raise ValueError("invalid catalog orientation")
    if base_direction not in DIRECTIONS:
        raise ValueError("invalid base direction")
    if orientation == "NORMAL":
        return base_direction
    return "SHORT" if base_direction == "LONG" else "LONG"


def _base_direction(analysis_direction: str, orientation: str) -> str:
    """Invert the orientation mapping without mixing unlike result directions."""
    return _analysis_direction(analysis_direction, orientation)


def _catalog_index(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict]:
    result = {}
    for raw in rows:
        key = raw.get("candidate_key")
        if not isinstance(key, str) or not key or key in result:
            raise ValueError("catalog candidate keys must be unique nonempty strings")
        sha = raw.get("definition_sha256")
        if (not isinstance(sha, str) or len(sha) != 64
                or any(character not in "0123456789abcdef" for character in sha)):
            raise ValueError("invalid catalog definition_sha256")
        evaluation_version = raw.get("evaluation_version")
        feature_version = raw.get("feature_version")
        if not isinstance(evaluation_version, str) or not evaluation_version:
            raise ValueError("invalid catalog evaluation_version")
        if not isinstance(feature_version, str) or not feature_version:
            raise ValueError("invalid catalog feature_version")
        result[key] = {
            "evaluation_version": evaluation_version,
            "candidate_key": key,
            "definition": _definition(raw.get("definition")),
            "definition_sha256": sha,
            "orientation": raw.get("orientation"),
            "feature_version": feature_version,
        }
        _analysis_direction("LONG", result[key]["orientation"])
    if not result:
        raise ValueError("active supported formula catalog is empty")
    if len({row["evaluation_version"] for row in result.values()}) != 1:
        raise ValueError("catalog contains more than one active evaluation version")
    return result


def _attest_active_catalog(rows: Sequence[Mapping[str, Any]], dimension: str) -> dict:
    """Bind a database read to the exact supported catalog shipped in code."""
    module = (coin_formula_catalog if dimension == "COIN"
              else timeframe_formula_catalog)
    expected = {
        row["candidate_key"]: row
        for row in module.catalog_records()
        if row["supported"]
    }
    actual = _catalog_index(rows)
    if set(actual) != set(expected):
        raise ValueError("active supported catalog coverage mismatch")
    for key, row in actual.items():
        reference = expected[key]
        if (row["evaluation_version"] != module.VERSION
                or row["feature_version"] != module.FEATURE_VERSION
                or row["definition_sha256"] != reference["definition_sha256"]
                or row["orientation"] != reference["orientation"]
                or canonical(row["definition"]) != canonical(reference["definition"])):
            raise ValueError("active supported catalog identity mismatch")
    identities = [{
        "candidate_key": key,
        "definition_sha256": actual[key]["definition_sha256"],
        "orientation": actual[key]["orientation"],
    } for key in sorted(actual)]
    return {
        "status": "VERIFIED_AGAINST_SHIPPED_ACTIVE_CATALOG",
        "active_evaluation_version": module.VERSION,
        "feature_version": module.FEATURE_VERSION,
        "supported_candidate_count": len(identities),
        "supported_catalog_sha256": hashlib.sha256(
            canonical(identities).encode("utf-8")
        ).hexdigest(),
    }


def _comparison_index(rows: Sequence[Mapping[str, Any]], catalog: Mapping[str, Mapping[str, Any]],
                      request: Mapping[str, Any]) -> dict[tuple[str, str, int], dict]:
    result = {}
    for raw in rows:
        key = (raw.get("candidate_key"), raw.get("analysis_direction"), raw.get("threshold_bps"))
        if (key[0] not in catalog or key[1] not in request["analysis_directions"]
                or key[2] not in request["thresholds_bps"] or key in result):
            raise ValueError("comparison row lies outside or duplicates the exact request")
        if raw.get("evaluation_version") != catalog[key[0]]["evaluation_version"]:
            raise ValueError("comparison active evaluation version mismatch")
        expected_base = _base_direction(key[1], catalog[key[0]]["orientation"])
        if raw.get("base_direction") != expected_base:
            raise ValueError("comparison base/analysis direction mismatch")
        matched_success = _count(raw.get("matched_success"), "matched_success")
        matched_failure = _count(raw.get("matched_failure"), "matched_failure")
        control_success = _count(raw.get("control_success"), "control_success")
        control_failure = _count(raw.get("control_failure"), "control_failure")
        matched_waves = _count(raw.get("matched_waves"), "matched_waves")
        control_waves = _count(raw.get("control_waves"), "control_waves")
        if matched_success + matched_failure > matched_waves:
            raise ValueError("decisive matched count exceeds matched waves")
        if control_success + control_failure > control_waves:
            raise ValueError("decisive control count exceeds control waves")
        result[key] = {
            "matched_waves": matched_waves,
            "control_waves": control_waves,
            "distinct_waves": _count(raw.get("distinct_waves"), "distinct_waves"),
            "shared_waves": _count(raw.get("shared_waves"), "shared_waves"),
            "blocked_arms": _count(raw.get("blocked_arms"), "blocked_arms"),
            "matched_success": matched_success,
            "matched_failure": matched_failure,
            "control_success": control_success,
            "control_failure": control_failure,
            "open_arms": _count(raw.get("open_arms"), "open_arms"),
            "missing_arms": _count(raw.get("missing_arms"), "missing_arms"),
            "ambiguous_arms": _count(raw.get("ambiguous_arms"), "ambiguous_arms"),
            "no_touch_arms": _count(raw.get("no_touch_arms"), "no_touch_arms"),
        }
    return result


def _outcome_index(rows: Sequence[Mapping[str, Any]], catalog: Mapping[str, Mapping[str, Any]],
                   request: Mapping[str, Any]) -> dict[tuple[str, str, int], dict]:
    result = {}
    for raw in rows:
        key = (raw.get("candidate_key"), raw.get("analysis_direction"), raw.get("threshold_bps"))
        if (key[0] not in catalog or key[1] not in request["analysis_directions"]
                or key[2] not in request["thresholds_bps"] or key in result):
            raise ValueError("outcome row lies outside or duplicates the exact request")
        if raw.get("evaluation_version") != catalog[key[0]]["evaluation_version"]:
            raise ValueError("outcome active evaluation version mismatch")
        expected_base = _base_direction(key[1], catalog[key[0]]["orientation"])
        if raw.get("base_direction") != expected_base:
            raise ValueError("outcome base/analysis direction mismatch")
        n = _count(raw.get("full_window_parent_count"), "full_window_parent_count")
        row_count = _count(raw.get("full_window_row_count"), "full_window_row_count")
        if row_count != n:
            raise ValueError("full-window outcome contains duplicate parent rows")
        favorable = _count(raw.get("favorable_parent_count"), "favorable_parent_count")
        if favorable > n:
            raise ValueError("favorable count exceeds full-window parent count")
        sum_mfe = _finite(raw.get("sum_mfe_pct"), "sum_mfe_pct", nonnegative=True)
        sum_mae = _finite(raw.get("sum_mae_pct"), "sum_mae_pct", nonnegative=True)
        median_edge = _finite(raw.get("median_paired_edge_pct"), "median_paired_edge_pct")
        if n == 0:
            if any(value is not None for value in (sum_mfe, sum_mae, median_edge)) or favorable:
                raise ValueError("zero-parent asymmetry row contains metrics")
        elif any(value is None for value in (sum_mfe, sum_mae, median_edge)):
            raise ValueError("full-window parent count lacks aggregate metrics")
        result[key] = {
            "full_window_parent_count": n,
            "full_window_row_count": row_count,
            "favorable_parent_count": favorable,
            "sum_mfe_pct": sum_mfe,
            "sum_mae_pct": sum_mae,
            "median_paired_edge_pct": median_edge,
        }
    return result


def _empty_comparison() -> dict:
    return {key: 0 for key in (
        "matched_waves", "control_waves", "distinct_waves", "shared_waves", "blocked_arms",
        "matched_success", "matched_failure", "control_success", "control_failure", "open_arms",
        "missing_arms", "ambiguous_arms", "no_touch_arms",
    )}


def _empty_outcome() -> dict:
    return {"full_window_parent_count": 0, "full_window_row_count": 0,
            "favorable_parent_count": 0,
            "sum_mfe_pct": None, "sum_mae_pct": None, "median_paired_edge_pct": None}


def candidate_cells(*, request: Mapping[str, Any], catalog_rows: Sequence[Mapping[str, Any]],
                    comparison_rows: Sequence[Mapping[str, Any]],
                    outcome_rows: Sequence[Mapping[str, Any]]) -> list[dict]:
    """Return the complete catalog x requested-partition matrix, including n=0."""
    catalog = _catalog_index(catalog_rows)
    comparisons = _comparison_index(comparison_rows, catalog, request)
    outcomes = _outcome_index(outcome_rows, catalog, request)
    cells = []
    for analysis_direction in request["analysis_directions"]:
        for threshold_bps in request["thresholds_bps"]:
            for candidate_key in sorted(catalog):
                item = catalog[candidate_key]
                base_direction = _base_direction(
                    analysis_direction, item["orientation"]
                )
                key = (candidate_key, analysis_direction, threshold_bps)
                comparison = comparisons.get(key, _empty_comparison())
                outcome = outcomes.get(key, _empty_outcome())
                probability_n = comparison["matched_success"] + comparison["matched_failure"]
                control_n = comparison["control_success"] + comparison["control_failure"]
                hit_rate = (100.0 * comparison["matched_success"] / probability_n
                            if probability_n else None)
                wilson = wilson_95_lower_pct(
                    comparison["matched_success"], probability_n
                )
                control_rate = (100.0 * comparison["control_success"] / control_n
                                if control_n else None)
                asymmetry_n = outcome["full_window_parent_count"]
                sum_mae = outcome["sum_mae_pct"]
                asymmetry_ratio = (outcome["sum_mfe_pct"] / sum_mae
                                   if asymmetry_n and sum_mae is not None and sum_mae > 0 else None)
                dominance = (100.0 * outcome["favorable_parent_count"] / asymmetry_n
                             if asymmetry_n else None)
                if asymmetry_n > comparison["matched_waves"]:
                    raise ValueError("full-window parent count exceeds matched waves")
                cells.append({
                    "candidate_key": candidate_key,
                    "definition": item["definition"],
                    "definition_sha256": item["definition_sha256"],
                    "orientation": item["orientation"],
                    "evaluation_version": item["evaluation_version"],
                    "feature_version": item["feature_version"],
                    "dimension": request["dimension"],
                    "symbol_scope": request["symbol_scope"],
                    "timeframe": request["timeframe"],
                    "base_direction": base_direction,
                    "analysis_direction": analysis_direction,
                    "window_minutes": request["window_minutes"],
                    "threshold_bps": threshold_bps,
                    **comparison,
                    "probability_decisive_parent_count": probability_n,
                    "probability_hit_rate_pct": hit_rate,
                    "probability_wilson_95_lower_pct": wilson,
                    "control_decisive_parent_count": control_n,
                    "control_hit_rate_pct": control_rate,
                    "control_lift_percentage_points": (hit_rate - control_rate
                                                       if hit_rate is not None and control_rate is not None else None),
                    **outcome,
                    "common_window_asymmetry_ratio": asymmetry_ratio,
                    "common_window_favorable_dominance_pct": dominance,
                    "probability_reference_floor_pass_count": sum((
                        hit_rate is not None and hit_rate >= PROBABILITY_HIT_RATE_FLOOR_PCT,
                        wilson is not None and wilson >= PROBABILITY_WILSON_FLOOR_PCT,
                    )),
                    "asymmetry_reference_floor_pass_count": sum((
                        asymmetry_ratio is not None and asymmetry_ratio >= ASYMMETRY_RATIO_FLOOR,
                        dominance is not None and dominance >= ASYMMETRY_DOMINANCE_FLOOR_PCT,
                        outcome["median_paired_edge_pct"] is not None
                        and outcome["median_paired_edge_pct"] > ASYMMETRY_MEDIAN_EDGE_FLOOR_PCT,
                    )),
                    "probability_evidence_band": evidence_band(probability_n),
                    "asymmetry_evidence_band": evidence_band(asymmetry_n),
                    "probability_dense_rank": None,
                    "probability_display_order": None,
                    "asymmetry_dense_rank": None,
                    "asymmetry_display_order": None,
                    "source_contract": SOURCE_CONTRACT,
                    "source_kind": SOURCE_KIND,
                    **_AUTHORITY_FALSE,
                })
    return cells


def _probability_key(row: Mapping[str, Any]) -> tuple | None:
    if row["probability_decisive_parent_count"] == 0:
        return None
    return (row["probability_reference_floor_pass_count"],
            row["probability_wilson_95_lower_pct"], row["probability_hit_rate_pct"],
            row["probability_decisive_parent_count"])


def _asymmetry_key(row: Mapping[str, Any]) -> tuple | None:
    if (row["full_window_parent_count"] == 0
            or row["common_window_asymmetry_ratio"] is None
            or row["common_window_favorable_dominance_pct"] is None
            or row["median_paired_edge_pct"] is None):
        return None
    return (row["asymmetry_reference_floor_pass_count"],
            row["full_window_parent_count"],
            row["common_window_asymmetry_ratio"],
            row["common_window_favorable_dominance_pct"],
            row["median_paired_edge_pct"])


def _assign_route_rank(rows: list[dict], route: str) -> dict[str, list[dict]]:
    key_fn = _probability_key if route == "probability" else _asymmetry_key
    ranked_by_band: dict[str, list[dict]] = {}
    for band in RANKABLE_BANDS:
        ranked = [
            row for row in rows
            if row[f"{route}_evidence_band"] == band and key_fn(row) is not None
        ]
        ranked.sort(key=lambda row: (tuple(-value for value in key_fn(row)),
                                     row["candidate_key"], row["definition_sha256"]))
        previous, dense_rank = None, 0
        for display_order, row in enumerate(ranked, 1):
            key = key_fn(row)
            if key != previous:
                dense_rank += 1
                previous = key
            row[f"{route}_dense_rank"] = dense_rank
            row[f"{route}_display_order"] = display_order
        ranked_by_band[band] = ranked
    return ranked_by_band


def build_leaderboard(*, dimension: str, symbol_scope: str, window_minutes: int,
                      timeframe: str, catalog_rows: Sequence[Mapping[str, Any]],
                      comparison_rows: Sequence[Mapping[str, Any]],
                      outcome_rows: Sequence[Mapping[str, Any]],
                      analysis_direction: str = "ALL", threshold_bps: int | str = "ALL",
                      top_per_route: int = DEFAULT_TOP_PER_ROUTE,
                      snapshot: Mapping[str, Any] | None = None,
                      catalog_attestation: Mapping[str, Any] | None = None) -> dict:
    request = _request(dimension=dimension, symbol_scope=symbol_scope,
        window_minutes=window_minutes, timeframe=timeframe, analysis_direction=analysis_direction,
        threshold_bps=threshold_bps, top_per_route=top_per_route)
    cells = candidate_cells(request=request, catalog_rows=catalog_rows,
        comparison_rows=comparison_rows, outcome_rows=outcome_rows)
    partitions = []
    global_probability_bands = {band: 0 for band in (
        NO_EVIDENCE_BAND, PROVISIONAL_BAND, DESCRIPTIVE_BAND)}
    global_asymmetry_bands = dict(global_probability_bands)
    for direction in request["analysis_directions"]:
        for threshold in request["thresholds_bps"]:
            rows = [row for row in cells if row["analysis_direction"] == direction
                    and row["threshold_bps"] == threshold]
            probability_by_band = _assign_route_rank(rows, "probability")
            asymmetry_by_band = _assign_route_rank(rows, "asymmetry")
            probability_ranked_count = sum(
                len(probability_by_band[band]) for band in RANKABLE_BANDS
            )
            asymmetry_ranked_count = sum(
                len(asymmetry_by_band[band]) for band in RANKABLE_BANDS
            )
            probability_bands = {band: 0 for band in global_probability_bands}
            asymmetry_bands = {band: 0 for band in global_asymmetry_bands}
            for row in rows:
                probability_bands[row["probability_evidence_band"]] += 1
                asymmetry_bands[row["asymmetry_evidence_band"]] += 1
                global_probability_bands[row["probability_evidence_band"]] += 1
                global_asymmetry_bands[row["asymmetry_evidence_band"]] += 1
            no_evidence = [row["candidate_key"] for row in rows
                           if row["probability_evidence_band"] == NO_EVIDENCE_BAND
                           and row["asymmetry_evidence_band"] == NO_EVIDENCE_BAND]
            partitions.append({
                "analysis_direction": direction,
                "threshold_bps": threshold,
                "candidate_count": len(rows),
                "probability_evidence_band_counts": probability_bands,
                "asymmetry_evidence_band_counts": asymmetry_bands,
                "probability_ranked_count": probability_ranked_count,
                "asymmetry_ranked_count": asymmetry_ranked_count,
                "both_routes_no_evidence_count": len(no_evidence),
                "both_routes_no_evidence_candidate_keys_sample": no_evidence[:top_per_route],
                "no_evidence_sample_truncated": len(no_evidence) > top_per_route,
                "probability_leaders_by_evidence_band": {
                    band: probability_by_band[band][:top_per_route]
                    for band in RANKABLE_BANDS
                },
                "asymmetry_leaders_by_evidence_band": {
                    band: asymmetry_by_band[band][:top_per_route]
                    for band in RANKABLE_BANDS
                },
            })
    result = {
        "version": VERSION,
        "source_kind": SOURCE_KIND,
        "source_contract": SOURCE_CONTRACT,
        "evidence_scope": "DESCRIPTIVE_ONLY_NOT_STAGE8_PROSPECTIVE_EVIDENCE",
        "request": request,
        "snapshot": dict(snapshot or {}),
        "catalog_attestation": dict(catalog_attestation or {
            "status": "CALLER_SUPPLIED_CATALOG_NOT_DATABASE_ATTESTED",
        }),
        "ranking_contract": {
            "minimum_evidence_filter": None,
            "evidence_bands_ranked_separately": list(RANKABLE_BANDS),
            "probability_dense_rank_desc": [
                "descriptive_reference_floor_pass_count", "wilson_95_lower_pct",
                "hit_rate_pct", "decisive_parent_count",
            ],
            "asymmetry_dense_rank_desc": ["descriptive_reference_floor_pass_count", "full_window_parent_count",
                                           "sum_mfe_over_sum_mae", "favorable_dominance_pct",
                                           "median_paired_edge_pct"],
            "descriptive_reference_floors": {
                "policy_version": REFERENCE_FLOOR_POLICY_VERSION,
                "documented_source": REFERENCE_FLOOR_DOCUMENT,
                "ranking_tier_only_not_acceptance": True,
                "probability": {
                    "hit_rate_pct_gte": PROBABILITY_HIT_RATE_FLOOR_PCT,
                    "wilson_95_lower_pct_gte": PROBABILITY_WILSON_FLOOR_PCT,
                },
                "asymmetry": {
                    "sum_mfe_over_sum_mae_gte": ASYMMETRY_RATIO_FLOOR,
                    "favorable_dominance_pct_gte": ASYMMETRY_DOMINANCE_FLOOR_PCT,
                    "median_paired_edge_pct_gt": ASYMMETRY_MEDIAN_EDGE_FLOOR_PCT,
                },
            },
            "stable_tie_order_asc": ["candidate_key", "definition_sha256"],
            "partition": ["dimension", "symbol_scope", "timeframe", "analysis_direction",
                          "window_minutes", "threshold_bps"],
        },
        "summary": {
            "partition_count": len(partitions),
            "catalog_candidate_count": len(_catalog_index(catalog_rows)),
            "enumerated_candidate_cell_count": len(cells),
            "returned_leader_row_upper_bound": len(partitions) * top_per_route * 4,
            "probability_evidence_band_counts": global_probability_bands,
            "asymmetry_evidence_band_counts": global_asymmetry_bands,
        },
        "partitions": partitions,
        **_AUTHORITY_FALSE,
    }
    result["leaderboard_sha256"] = hashlib.sha256(
        canonical(result).encode("utf-8")
    ).hexdigest()
    return result


def _sources(dimension: str) -> tuple[str, str, str, str]:
    if dimension == "COIN":
        return ("research_watch_scan_formula_catalog", "research_watch_scan_formula_runtime",
                "research_watch_scan_formula_comparisons", "research_watch_scan_formula_wave_outcomes")
    return ("research_watch_scan_tf_formula_catalog", "research_watch_scan_tf_formula_runtime",
            "research_watch_scan_tf_formula_comparisons", "research_watch_scan_tf_formula_wave_outcomes")


def _read_rows(conn, request: Mapping[str, Any]) -> tuple[list, list, list, dict, dict]:
    catalog, runtime, comparisons, outcomes = _sources(request["dimension"])
    snapshot = conn.execute("""SELECT transaction_timestamp() AS snapshot_at_utc,
        pg_current_snapshot()::text AS mvcc_snapshot,
        current_setting('transaction_isolation') AS transaction_isolation,
        current_setting('transaction_read_only') AS transaction_read_only""").fetchone()
    if (snapshot.get("transaction_isolation") != "repeatable read"
            or str(snapshot.get("transaction_read_only")).lower() not in ("on", "true")):
        raise RuntimeError("leaderboard requires a repeatable-read read-only transaction")
    catalog_rows = conn.execute(f"""SELECT c.evaluation_version,c.candidate_key,c.definition,
        c.definition_sha256,c.orientation,c.feature_version
        FROM {catalog} c JOIN {runtime} runtime
          ON runtime.singleton AND runtime.active_evaluation_version=c.evaluation_version
        WHERE c.supported IS TRUE ORDER BY c.candidate_key""").fetchall()
    catalog_attestation = _attest_active_catalog(catalog_rows, request["dimension"])
    extra = " AND timeframe=%s" if request["dimension"] == "SELECTED_TIMEFRAME" else ""
    params = [request["symbol_scope"], request["window_minutes"],
              request["analysis_directions"], request["thresholds_bps"]]
    if extra:
        params.append(request["timeframe"])
    comparison_rows = conn.execute(f"""SELECT candidate_key,evaluation_version,base_direction,
        analysis_direction,threshold_bps,distinct_waves,matched_waves,control_waves,shared_waves,
        blocked_arms,matched_success,matched_failure,control_success,control_failure,
        open_arms,missing_arms,ambiguous_arms,no_touch_arms
        FROM {comparisons}
        WHERE symbol_scope=%s AND window_minutes=%s
          AND analysis_direction=ANY(%s::text[]) AND threshold_bps=ANY(%s::integer[]){extra}
        ORDER BY analysis_direction,threshold_bps,candidate_key""", params).fetchall()
    outcome_rows = conn.execute(f"""WITH filtered AS (
        SELECT candidate_key,evaluation_version,base_direction,analysis_direction,threshold_bps,
            btc_parent_movement_id,full_window_mfe_pct,full_window_mae_pct,
            match_status='MATCH' AND anchor_eligible IS TRUE
              AND full_window_mfe_pct IS NOT NULL AND full_window_mae_pct IS NOT NULL
              AND full_window_mfe_pct>=0 AND full_window_mae_pct>=0
              AND full_window_mfe_pct<'Infinity'::double precision
              AND full_window_mae_pct<'Infinity'::double precision AS full_window_valid
        FROM {outcomes}
        WHERE symbol_scope=%s AND window_minutes=%s
          AND analysis_direction=ANY(%s::text[]) AND threshold_bps=ANY(%s::integer[]){extra}
    )
    SELECT candidate_key,evaluation_version,base_direction,analysis_direction,threshold_bps,
        COUNT(*) FILTER(WHERE full_window_valid) AS full_window_row_count,
        COUNT(DISTINCT btc_parent_movement_id) FILTER(WHERE full_window_valid) AS full_window_parent_count,
        COUNT(DISTINCT btc_parent_movement_id) FILTER(WHERE full_window_valid
            AND full_window_mfe_pct>full_window_mae_pct) AS favorable_parent_count,
        SUM(full_window_mfe_pct) FILTER(WHERE full_window_valid) AS sum_mfe_pct,
        SUM(full_window_mae_pct) FILTER(WHERE full_window_valid) AS sum_mae_pct,
        percentile_cont(0.5) WITHIN GROUP
            (ORDER BY full_window_mfe_pct-full_window_mae_pct)
            FILTER(WHERE full_window_valid) AS median_paired_edge_pct
    FROM filtered GROUP BY candidate_key,evaluation_version,base_direction,analysis_direction,threshold_bps
    ORDER BY analysis_direction,threshold_bps,candidate_key""", params).fetchall()
    return (catalog_rows, comparison_rows, outcome_rows, dict(snapshot),
            catalog_attestation)


def read_leaderboard(conn, **kwargs) -> dict:
    request = _request(**kwargs)
    (catalog_rows, comparison_rows, outcome_rows, snapshot,
     catalog_attestation) = _read_rows(conn, request)
    return build_leaderboard(catalog_rows=catalog_rows, comparison_rows=comparison_rows,
        outcome_rows=outcome_rows, snapshot=snapshot,
        catalog_attestation=catalog_attestation, **kwargs)


def load_leaderboard(**kwargs) -> dict:
    with readonly_connection() as conn:
        return read_leaderboard(conn, **kwargs)


def _threshold_arg(value: str) -> int | str:
    return "ALL" if value == "ALL" else int(value)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dimension", required=True, choices=DIMENSIONS)
    parser.add_argument("--symbol-scope", required=True, choices=SYMBOL_SCOPES)
    parser.add_argument("--window-minutes", required=True, type=int, choices=WINDOWS)
    parser.add_argument("--timeframe", required=True,
                        help=f"one of {','.join(TIMEFRAMES)} or {COIN_TIMEFRAME} for COIN")
    parser.add_argument("--analysis-direction", default="ALL", choices=("ALL", *DIRECTIONS))
    parser.add_argument("--threshold-bps", default="ALL", type=_threshold_arg)
    parser.add_argument("--top-per-route", default=DEFAULT_TOP_PER_ROUTE, type=int)
    args = vars(parser.parse_args())
    print(canonical(load_leaderboard(**args)))


if __name__ == "__main__":
    main()
