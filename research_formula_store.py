"""Auditable PostgreSQL registry for discovered and shadow-tested formulas."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from typing import Any, Dict, Mapping, Optional, Sequence

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None

import canonical_price_path
import research_evidence_contract
import research_feature_matrix
import research_formula_acceptance
import research_formula_engine
import research_formula_families
import research_formula_lab_comparison
import research_formula_relevance
import research_historical_replay
import research_market_episode
import research_max_pain_archive
import research_mfe_mae_efficiency
import research_no_dwell_outcome
import research_prospective_anchors
import research_prospective_feature_freeze
import research_session_width


_TRUE = {"1", "true", "yes", "on"}
_LIVE_ALERTS_ENABLED = (
    os.getenv("FORMULA_LIVE_ALERTS_ENABLED", "").strip().lower() in _TRUE
)
_STAGE_ORDER = {
    "DISCOVERED": 0,
    "BACKTESTED": 1,
    "HOLDOUT_PASSED": 2,
    "SHADOW": 3,
    "APPROVED": 4,
    "LIVE": 5,
    "RETIRED": 6,
}
_AUTOMATIC_STAGE_PATH = ("DISCOVERED", "BACKTESTED", "HOLDOUT_PASSED", "SHADOW")
_PROTECTED_FORMULA_STAGES = frozenset(
    {"SHADOW", "APPROVED", "LIVE", "RETIRED"}
)
_REPLACEMENT_DATASET_KIND = "historical_raw_opportunity_replay"
_REPLACEMENT_QUALIFYING_STAGES = frozenset(
    {"BACKTESTED", "HOLDOUT_PASSED", "SHADOW", "APPROVED", "LIVE"}
)
_SHADOW_COMPATIBLE_FORMULA_SCHEMAS = (
    "research-formula-v5-safe-replay",
    research_formula_engine.LEGACY_V6_FORMULA_SCHEMA_VERSION,
    research_formula_engine.FORMULA_SCHEMA_VERSION,
)
_LEGACY_V5_FORMULA_SCHEMA_VERSION = "research-formula-v5-safe-replay"
_LEGACY_V5_ENGINE_VERSION = "formula-discovery-v5-safe-replay"
_LEGACY_V5_FEATURE_SCHEMA_VERSION = (
    "research-feature-matrix-v4-safe-replay"
)
_LEGACY_V5_OUTCOME_METHOD_VERSION = "canonical-spot-1m-ohlc-path-v3"
_LEGACY_V5_SHADOW_ADAPTER_VERSION = (
    "legacy-v5-to-sampler-v4-first-touch-shadow-adapter-v1"
)
_SHADOW_INPUT_SNAPSHOT_POLICY_VERSION = (
    "formula-shadow-input-snapshot-v5-frozen-decision-features"
)
_DECISION_COHORT_POLICY_VERSION = (
    "formula-shadow-decision-cohort-v5-frozen-feature-bundle"
)
_PROSPECTIVE_EVIDENCE_POLICY_VERSION = (
    "prospective-shadow-frozen-decision-features-v1"
)
_REJECTED_PROSPECTIVE_EVIDENCE_POLICY_VERSION = (
    _PROSPECTIVE_EVIDENCE_POLICY_VERSION + "-rejected"
)
_FEATURE_BUNDLE_POLICY_VERSION = "prospective-decision-feature-bundle-v1"
_PROSPECTIVE_ANCHOR_SAMPLER_VERSION = (
    "prospective-neutral-anchor-v4-decision-features-frozen"
)
_LIVE_APPROVAL_OPERATION_VERSION = (
    "formula-owner-live-approval-v2-engine-bound"
)
_LIVE_APPROVAL_TRIGGER_CONTRACTS = {
    "trg_formula_live_approvals_append_only": {
        "table": "research_formula_live_approvals",
        "function": "prevent_formula_live_approval_mutation",
        "trigger_type": 27,
        "update_columns": (),
        "tokens": ("research_formula_live_approvals is append-only",),
    },
    "trg_validate_formula_owner_live_approval": {
        "table": "research_formula_live_approvals",
        "function": "validate_formula_owner_live_approval",
        "trigger_type": 7,
        "update_columns": (),
        "tokens": (
            _LIVE_APPROVAL_OPERATION_VERSION,
            "formula_row.engine_version",
            "new.engine_version",
        ),
    },
    "trg_require_formula_owner_live_approval": {
        "table": "research_formulas",
        "function": "require_formula_owner_live_approval",
        "trigger_type": 19,
        "update_columns": ("current_stage",),
        "tokens": (
            _LIVE_APPROVAL_OPERATION_VERSION,
            "approval.engine_version",
            "new.engine_version",
        ),
    },
    "trg_prevent_protected_formula_contract_mutation": {
        "table": "research_formulas",
        "function": "prevent_protected_formula_contract_mutation",
        "trigger_type": 19,
        "update_columns": (),
        "tokens": (
            "protected formula stage cannot be downgraded or reactivated",
            "protected formula active state is inconsistent with lifecycle stage",
            "protected formula runtime contract is immutable",
            "protected formula approval evidence is immutable",
            "new.engine_version",
        ),
    },
}
_LIVE_APPROVAL_UNIQUE_INDEX = "idx_formula_live_approvals_exact_runtime"
DISCOVERY_SCHEDULER_VERSION = "formula-discovery-horizon-scheduler-v1"
DISCOVERY_WATERMARK_VERSION = "formula-discovery-dataset-watermark-v1"
_DISCOVERY_LOCK_BASE_ID = 94837300


def _shadow_executable_formula_contract(formula: Mapping[str, Any]) -> bool:
    """Return whether a retained Shadow formula has an exact runtime tuple."""
    runtime_tuple = (
        str(formula.get("formula_schema_version") or ""),
        str(formula.get("engine_version") or ""),
        str(formula.get("feature_schema_version") or ""),
        str(formula.get("outcome_method_version") or ""),
    )
    return bool(
        runtime_tuple
        in {
            (
                _LEGACY_V5_FORMULA_SCHEMA_VERSION,
                _LEGACY_V5_ENGINE_VERSION,
                _LEGACY_V5_FEATURE_SCHEMA_VERSION,
                _LEGACY_V5_OUTCOME_METHOD_VERSION,
            ),
            (
                research_formula_engine.LEGACY_V6_FORMULA_SCHEMA_VERSION,
                research_formula_engine.LEGACY_V6_ENGINE_VERSION,
                research_feature_matrix.FEATURE_SCHEMA_VERSION,
                research_feature_matrix.VERIFIED_OUTCOME_METHOD,
            ),
            (
                research_formula_engine.FORMULA_SCHEMA_VERSION,
                research_formula_engine.ENGINE_VERSION,
                research_feature_matrix.FEATURE_SCHEMA_VERSION,
                research_feature_matrix.VERIFIED_OUTCOME_METHOD,
            ),
        }
    )


def _current_live_formula_contract(formula: Mapping[str, Any]) -> bool:
    """Only the exact current v7 runtime may ever authorize delivery."""
    return bool(
        formula.get("formula_schema_version")
        == research_formula_engine.FORMULA_SCHEMA_VERSION
        and formula.get("engine_version") == research_formula_engine.ENGINE_VERSION
        and formula.get("feature_schema_version")
        == research_feature_matrix.FEATURE_SCHEMA_VERSION
        and formula.get("outcome_method_version")
        == research_feature_matrix.VERIFIED_OUTCOME_METHOD
    )


def _bind_efficiency_policy(policy_version: str) -> str:
    base = str(policy_version or "").strip()
    if research_mfe_mae_efficiency.POLICY_VERSION in base:
        return base
    return f"{base}+{research_mfe_mae_efficiency.POLICY_VERSION}"


def _bind_max_pain_policy(policy_version: str) -> str:
    base = str(policy_version or "").strip()
    for binding in (
        research_max_pain_archive.SHADOW_PROVENANCE_POLICY_VERSION,
        _DECISION_COHORT_POLICY_VERSION,
    ):
        if binding not in base:
            base = f"{base}+{binding}"
    return base


def _bind_adaptive_evidence_policy(policy_version: str) -> str:
    base = str(policy_version or "").strip()
    for binding in (
        research_formula_acceptance.POLICY_VERSION,
        research_market_episode.POLICY_VERSION,
    ):
        if binding not in base:
            base = f"{base}+{binding}"
    return base


_SHADOW_MONITORING_POLICY_BASE = (
    os.getenv("FORMULA_SHADOW_MONITORING_POLICY_VERSION", "").strip()
    or "shadow-monitoring-v4-frozen-decision-features-2026-08-30"
)
_SHADOW_MONITORING_POLICY_VERSION = _bind_max_pain_policy(
    _bind_adaptive_evidence_policy(
        _bind_efficiency_policy(_SHADOW_MONITORING_POLICY_BASE)
    )
)
_SHADOW_MIN_MATCHES = max(
    12, int(os.getenv("FORMULA_SHADOW_MIN_VALIDATED_MATCHES", "12"))
)
_SHADOW_MIN_CONTROLS = max(
    12, int(os.getenv("FORMULA_SHADOW_MIN_VALIDATED_CONTROLS", "12"))
)
_SHADOW_MIN_SPAN_HOURS = max(
    72, int(os.getenv("FORMULA_SHADOW_MIN_SPAN_HOURS", "72"))
)
_SHADOW_MIN_DATES = max(3, int(os.getenv("FORMULA_SHADOW_MIN_UTC_DATES", "3")))
_DELIVERY_MAX_ATTEMPTS = max(
    1, int(os.getenv("FORMULA_LIVE_DELIVERY_MAX_ATTEMPTS", "5"))
)
_DELIVERY_MAX_AGE_MINUTES = max(
    1, int(os.getenv("FORMULA_LIVE_DELIVERY_MAX_AGE_MINUTES", "15"))
)


def _database_url() -> str:
    dedicated = os.getenv("RESEARCH_DATABASE_URL", "").strip()
    use_primary = os.getenv("RESEARCH_USE_PRIMARY_DATABASE", "").strip().lower() in _TRUE
    if dedicated:
        return dedicated
    if use_primary:
        return os.getenv("DATABASE_URL", "").strip()
    return ""


def _connect(*, read_only: bool = False):
    url = _database_url()
    if not url:
        raise RuntimeError("Formula registry database is not configured")
    if psycopg is None:
        raise RuntimeError("psycopg is unavailable")
    options = "-c statement_timeout=20000"
    if read_only:
        options += " -c default_transaction_read_only=on"
    return psycopg.connect(
        url,
        row_factory=dict_row,
        connect_timeout=5,
        options=options,
    )


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        default=str,
        separators=(",", ":"),
        allow_nan=False,
    )


def _canonical_json(value: Any) -> str:
    """Serialize JSON for exact comparisons, preserving scalar JSON types."""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        default=str,
        separators=(",", ":"),
        allow_nan=False,
    )


def _type_strict_json_equal(left: Any, right: Any) -> bool:
    """Compare canonical JSON without Python's bool/number equality coercion."""
    try:
        return _canonical_json(left) == _canonical_json(right)
    except (TypeError, ValueError, OverflowError):
        return False


def _json_safe(value: Any) -> Any:
    return json.loads(
        json.dumps(
            value,
            ensure_ascii=False,
            default=str,
            allow_nan=False,
        )
    )


def _strict_int(value: Any) -> Optional[int]:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return int(value)


def _strict_finite_number(value: Any) -> Optional[float]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        number = float(value)
    except (OverflowError, TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _verified_replacement_readiness(
    *,
    dataset: Mapping[str, Any],
    discovery: Mapping[str, Any],
    formulas: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Verify the complete v6 replacement contract without trusting one flag.

    Coverage is produced by the historical replay loader, but retirement is a
    durable lifecycle change.  Recheck its frozen evidence here and require an
    explicitly version-bound, non-empty same-horizon replacement cohort.
    Missing or malformed evidence fails closed.
    """
    coverage_source = dataset.get("coverage")
    coverage = dict(coverage_source) if isinstance(coverage_source, Mapping) else {}
    reasons: list[str] = []
    expected_horizon = _strict_int(discovery.get("horizon_minutes"))

    if dataset.get("available") is not True:
        reasons.append("dataset_available")
    if discovery.get("available") is not True:
        reasons.append("discovery_available")
    if coverage.get("dataset_kind") != _REPLACEMENT_DATASET_KIND:
        reasons.append("dataset_kind")
    if coverage.get("replacement_ready") is not True:
        reasons.append("declared_replacement_ready")

    replay_version = dataset.get("replay_version")
    first_touch_version = dataset.get("first_touch_method_version")
    if replay_version != research_historical_replay.REPLAY_VERSION:
        reasons.append("replay_version")
    if coverage.get("replay_version") != replay_version:
        reasons.append("coverage_replay_version_conflict")
    if first_touch_version != research_feature_matrix.VERIFIED_OUTCOME_METHOD:
        reasons.append("first_touch_method_version")
    if coverage.get("first_touch_method_version") != first_touch_version:
        reasons.append("coverage_first_touch_method_version_conflict")
    calibration_version = dataset.get(
        "movement_width_calibration_version"
    )
    if calibration_version != research_session_width.CALIBRATION_VERSION:
        reasons.append("movement_width_calibration_version")
    if coverage.get("movement_width_calibration_version") != (
        research_session_width.CALIBRATION_VERSION
    ):
        reasons.append("coverage_movement_width_calibration_version")
    provenance_version = dataset.get("canonical_price_provenance_version")
    if provenance_version != canonical_price_path.PRICE_PROVENANCE_VERSION:
        reasons.append("canonical_price_provenance_version")
    if coverage.get("canonical_price_provenance_version") != provenance_version:
        reasons.append("coverage_canonical_price_provenance_version_conflict")
    if (
        dataset.get("outcome_method_version")
        != research_feature_matrix.VERIFIED_OUTCOME_METHOD
    ):
        reasons.append("outcome_method_version")
    if (
        dataset.get("feature_schema_version")
        != research_feature_matrix.FEATURE_SCHEMA_VERSION
    ):
        reasons.append("dataset_feature_schema_version")
    if (
        discovery.get("feature_schema_version")
        != research_feature_matrix.FEATURE_SCHEMA_VERSION
    ):
        reasons.append("discovery_feature_schema_version")
    if (
        discovery.get("formula_schema_version")
        != research_formula_engine.FORMULA_SCHEMA_VERSION
    ):
        reasons.append("formula_schema_version")
    if discovery.get("engine_version") != research_formula_engine.ENGINE_VERSION:
        reasons.append("engine_version")

    dataset_horizon = _strict_int(dataset.get("horizon_minutes"))
    if expected_horizon not in {60, 240, 720, 1440}:
        reasons.append("discovery_horizon_minutes")
    if dataset_horizon != expected_horizon:
        reasons.append("dataset_horizon_minutes")

    readiness_policy = coverage.get("readiness_policy")
    policy = dict(readiness_policy) if isinstance(readiness_policy, Mapping) else {}
    expected_policy = {
        "minimum_anchors_per_symbol": research_feature_matrix.REPLAY_MIN_ANCHORS_PER_SYMBOL,
        "minimum_eligible_symbols": research_feature_matrix.REPLAY_MIN_ELIGIBLE_SYMBOLS,
        "minimum_utc_dates_per_symbol": research_feature_matrix.REPLAY_MIN_UTC_DATES_PER_SYMBOL,
        "minimum_span_hours_per_symbol": research_feature_matrix.REPLAY_MIN_SPAN_HOURS_PER_SYMBOL,
        "continuity_gap_is_diagnostic_only": True,
    }
    for key, expected in expected_policy.items():
        actual = policy.get(key)
        if isinstance(expected, bool):
            valid = actual is expected
        elif isinstance(expected, int):
            valid = _strict_int(actual) == expected
        else:
            valid = _strict_finite_number(actual) == float(expected)
        if not valid:
            reasons.append(f"readiness_policy.{key}")

    by_symbol_source = coverage.get("by_symbol")
    by_symbol = by_symbol_source if isinstance(by_symbol_source, Mapping) else {}
    eligible_symbols: list[str] = []
    for raw_symbol, raw_item in sorted(
        by_symbol.items(), key=lambda item: str(item[0])
    ):
        symbol = str(raw_symbol or "").strip().upper()
        if not symbol or not isinstance(raw_item, Mapping):
            reasons.append("by_symbol_shape")
            continue
        anchors = _strict_int(raw_item.get("anchors"))
        utc_dates = _strict_int(raw_item.get("utc_dates"))
        span_hours = _strict_finite_number(raw_item.get("span_hours"))
        failed_gates = raw_item.get("failed_gates")
        failed_gates_valid = (
            isinstance(failed_gates, (list, tuple)) and not failed_gates
        )
        eligible = bool(
            anchors is not None
            and anchors >= research_feature_matrix.REPLAY_MIN_ANCHORS_PER_SYMBOL
            and utc_dates is not None
            and utc_dates >= research_feature_matrix.REPLAY_MIN_UTC_DATES_PER_SYMBOL
            and span_hours is not None
            and span_hours >= research_feature_matrix.REPLAY_MIN_SPAN_HOURS_PER_SYMBOL
            and raw_item.get("eligible") is True
            and failed_gates_valid
        )
        if eligible:
            eligible_symbols.append(symbol)
        elif raw_item.get("eligible") is True:
            reasons.append(f"by_symbol.{symbol}.eligibility_mismatch")

    declared_symbols = coverage.get("eligible_symbols")
    normalized_declared_symbols = (
        sorted(
            {
                str(value or "").strip().upper()
                for value in declared_symbols
                if value
            }
        )
        if isinstance(declared_symbols, (list, tuple))
        else []
    )
    if normalized_declared_symbols != eligible_symbols:
        reasons.append("eligible_symbols")
    if _strict_int(coverage.get("symbols")) != len(eligible_symbols):
        reasons.append("eligible_symbol_count")
    if len(eligible_symbols) < research_feature_matrix.REPLAY_MIN_ELIGIBLE_SYMBOLS:
        reasons.append("minimum_eligible_symbols")
    distinct_dates = _strict_int(coverage.get("distinct_utc_dates"))
    if (
        distinct_dates is None
        or distinct_dates < research_feature_matrix.REPLAY_MIN_UTC_DATES_PER_SYMBOL
    ):
        reasons.append("distinct_utc_dates")
    total_span = _strict_finite_number(coverage.get("span_hours"))
    if (
        total_span is None
        or total_span < research_feature_matrix.REPLAY_MIN_SPAN_HOURS_PER_SYMBOL
    ):
        reasons.append("span_hours")

    same_horizon_formulas = []
    qualifying_formulas = []
    for formula in formulas:
        if not isinstance(formula, Mapping):
            reasons.append("formula_shape")
            continue
        formula_horizon = _strict_int(formula.get("horizon_minutes"))
        # Outcome provenance is a dataset-level immutable contract verified
        # above and persisted uniformly for every formula in this run.  Engine
        # formula payloads intentionally do not duplicate that field.
        version_match = bool(
            formula.get("formula_schema_version")
            == research_formula_engine.FORMULA_SCHEMA_VERSION
            and formula.get("engine_version") == research_formula_engine.ENGINE_VERSION
            and formula.get("feature_schema_version")
            == research_feature_matrix.FEATURE_SCHEMA_VERSION
            and _strict_int(formula.get("formula_version")) == 1
        )
        if formula_horizon == expected_horizon and version_match:
            same_horizon_formulas.append(formula)
            if (
                str(formula.get("recommended_stage") or "").upper()
                in _REPLACEMENT_QUALIFYING_STAGES
            ):
                qualifying_formulas.append(formula)
        else:
            reasons.append("formula_version_or_horizon")
    if not formulas or not same_horizon_formulas:
        reasons.append("nonempty_replacement_cohort")
    if not qualifying_formulas:
        reasons.append("qualifying_replacement_formula")

    unique_reasons = list(dict.fromkeys(reasons))
    return {
        "verified": not unique_reasons,
        "reasons": unique_reasons,
        "eligible_symbols": eligible_symbols,
        "same_horizon_formula_count": len(same_horizon_formulas),
        "qualifying_formula_count": len(qualifying_formulas),
        "expected": {
            "dataset_kind": _REPLACEMENT_DATASET_KIND,
            "replay_version": research_historical_replay.REPLAY_VERSION,
            "first_touch_method_version": research_feature_matrix.VERIFIED_OUTCOME_METHOD,
            "feature_schema_version": research_feature_matrix.FEATURE_SCHEMA_VERSION,
            "formula_schema_version": research_formula_engine.FORMULA_SCHEMA_VERSION,
            "engine_version": research_formula_engine.ENGINE_VERSION,
            "readiness_policy": expected_policy,
        },
    }


def _replacement_cohort_supports_retirement(
    verified_readiness: bool,
    replacement_rows: Sequence[Mapping[str, Any]],
    *,
    horizon_minutes: int,
    run_id: int,
) -> bool:
    """Require an active current-run replacement before predecessor retirement."""
    return bool(
        verified_readiness
        and any(
            row.get("active") is True
            and _strict_int(row.get("formula_version")) == 1
            and row.get("formula_schema_version")
            == research_formula_engine.FORMULA_SCHEMA_VERSION
            and row.get("engine_version")
            == research_formula_engine.ENGINE_VERSION
            and row.get("feature_schema_version")
            == research_feature_matrix.FEATURE_SCHEMA_VERSION
            and row.get("outcome_method_version")
            == research_feature_matrix.VERIFIED_OUTCOME_METHOD
            and _strict_int(row.get("horizon_minutes")) == int(horizon_minutes)
            and _strict_int(row.get("latest_evaluation_run_id")) == int(run_id)
            and str(row.get("current_stage") or "").upper()
            in _REPLACEMENT_QUALIFYING_STAGES
            for row in replacement_rows
            if isinstance(row, Mapping)
        )
    )


def _annotate_mfe_mae_metrics(value: Any) -> Any:
    """Add zero-safe ratio evidence to legacy registry metrics on read.

    Stored discovery/holdout JSON remains immutable.  This read-side adapter
    derives the current state only from the archived median MFE and MAE, never
    from a stale numeric ratio or state field.
    """
    metrics = value
    if isinstance(metrics, str):
        try:
            metrics = json.loads(metrics)
        except json.JSONDecodeError:
            return value
    if not isinstance(metrics, Mapping):
        return value
    annotated = dict(metrics)
    efficiency = research_mfe_mae_efficiency.from_metrics(annotated)
    annotated["median_mfe_mae_ratio"] = (
        round(efficiency.ratio, 6)
        if efficiency.ratio is not None
        else None
    )
    annotated["median_mfe_mae_ratio_state"] = efficiency.state
    annotated["median_mfe_mae_ratio_policy_version"] = (
        research_mfe_mae_efficiency.POLICY_VERSION
    )
    return annotated


def _formula_registry_row(value: Mapping[str, Any]) -> Dict[str, Any]:
    row = dict(value)
    for key in ("discovery_metrics", "holdout_metrics"):
        row[key] = _annotate_mfe_mae_metrics(row.get(key))
    validation = row.pop("shadow_validation_metrics", None)
    if isinstance(validation, str):
        try:
            validation = json.loads(validation)
        except json.JSONDecodeError:
            validation = None
    if isinstance(validation, Mapping):
        acceptance = validation.get("research_acceptance")
        acceptance = acceptance if isinstance(acceptance, Mapping) else {}
        metrics = validation.get("metrics")
        metrics = metrics if isinstance(metrics, Mapping) else {}
        evidence = validation.get("evidence")
        evidence = evidence if isinstance(evidence, Mapping) else {}
        row["research_maturity"] = {
            "evaluated_at_utc": validation.get("evaluated_at_utc"),
            "policy_version": acceptance.get("policy_version"),
            "research_ready": bool(acceptance.get("research_ready")),
            "maturity": acceptance.get("maturity"),
            "accepted_paths": list(acceptance.get("accepted_paths") or []),
            "early_current_paths": list(
                acceptance.get("early_current_paths") or []
            ),
            "missing_by_path": acceptance.get("missing_by_path") or {},
            "rolling_metrics": {
                key: metrics.get(key)
                for key in (
                    "sample_size",
                    "control_sample_size",
                    "recent_sample_size",
                    "recency_effective_sample_size",
                    "recency_control_effective_sample_size",
                    "last_sample_age_hours",
                    "recency_weighted_hit_rate_pct",
                    "recency_weighted_wilson_95_lower_approx_pct",
                    "recency_weighted_hit_rate_improvement_pct_points",
                    "recency_weighted_favorable_dominance_rate_pct",
                    "recency_weighted_favorable_dominance_wilson_95_lower_approx_pct",
                    "recency_weighted_favorable_dominance_improvement_pct_points",
                    "recency_weighted_median_mfe_pct",
                    "recency_weighted_median_mae_pct",
                    "recency_weighted_mae_p90_pct",
                    "recency_weighted_mae_p95_pct",
                )
            },
            "market_episodes": {
                key: evidence.get(key)
                for key in (
                    "independent_matches",
                    "independent_controls",
                    "finalized_independent_matches",
                    "finalized_independent_controls",
                    "complete_independent_matches",
                    "complete_independent_controls",
                    "correlated_match_exclusions",
                    "correlated_control_exclusions",
                )
            },
            "live_effect": "NONE; explicit owner approval remains separate",
        }
    return row


def _table_exists(conn, table: str) -> bool:
    row = conn.execute(
        "SELECT to_regclass(%s) AS relation", (f"public.{table}",)
    ).fetchone()
    return bool(row and row.get("relation"))


def _missing_columns(
    conn, required: Mapping[str, Sequence[str]]
) -> list[str]:
    missing: list[str] = []
    for table, columns in required.items():
        rows = conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s
              AND column_name=ANY(%s)
            """,
            (table, list(columns)),
        ).fetchall()
        present = {str(row["column_name"]) for row in rows}
        missing.extend(
            f"{table}.{column}" for column in columns if column not in present
        )
    return missing


def _live_approval_enforcement_status(conn) -> Dict[str, Any]:
    """Prove that migration 011 enforcement objects are installed and enabled."""
    try:
        rows = conn.execute(
            """
            SELECT trigger.tgname AS trigger_name,
                   relation.relname AS table_name,
                   trigger.tgenabled AS enabled_state,
                   trigger.tgtype::integer AS trigger_type,
                   ARRAY(
                       SELECT attribute.attname
                       FROM UNNEST(trigger.tgattr::smallint[])
                            WITH ORDINALITY AS update_column(
                                attnum, ordinal_position
                            )
                       JOIN pg_attribute attribute
                         ON attribute.attrelid=trigger.tgrelid
                        AND attribute.attnum=update_column.attnum
                       ORDER BY update_column.ordinal_position
                   ) AS update_columns,
                   function.proname AS function_name,
                   pg_get_functiondef(function.oid) AS function_definition
            FROM pg_trigger trigger
            JOIN pg_class relation ON relation.oid=trigger.tgrelid
            JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
            JOIN pg_proc function ON function.oid=trigger.tgfoid
            WHERE namespace.nspname='public'
              AND trigger.tgisinternal=FALSE
              AND trigger.tgname=ANY(%s)
            """,
            (sorted(_LIVE_APPROVAL_TRIGGER_CONTRACTS),),
        ).fetchall()
        index = conn.execute(
            """
            SELECT index_meta.indisunique, index_meta.indisvalid,
                   index_meta.indisready,
                   ARRAY(
                       SELECT attribute.attname
                       FROM UNNEST(index_meta.indkey::smallint[])
                            WITH ORDINALITY AS key_column(attnum, ordinal_position)
                       JOIN pg_attribute attribute
                         ON attribute.attrelid=index_meta.indrelid
                        AND attribute.attnum=key_column.attnum
                       ORDER BY key_column.ordinal_position
                   ) AS key_columns,
                   pg_get_expr(index_meta.indpred, index_meta.indrelid)
                       AS predicate_definition
            FROM pg_index index_meta
            JOIN pg_class index_relation
              ON index_relation.oid=index_meta.indexrelid
            JOIN pg_class table_relation
              ON table_relation.oid=index_meta.indrelid
            JOIN pg_namespace namespace
              ON namespace.oid=table_relation.relnamespace
            WHERE namespace.nspname='public'
              AND table_relation.relname='research_formula_live_approvals'
              AND index_relation.relname=%s
            """,
            (_LIVE_APPROVAL_UNIQUE_INDEX,),
        ).fetchone()
    except Exception as exc:
        return {
            "ready": False,
            "missing": ["migration_011_enforcement_inspection_failed"],
            "inspection_error": f"{type(exc).__name__}: {exc}"[:500],
        }

    by_name = {str(row.get("trigger_name") or ""): row for row in rows}
    missing: list[str] = []
    for trigger_name, contract in _LIVE_APPROVAL_TRIGGER_CONTRACTS.items():
        row = by_name.get(trigger_name)
        if not row:
            missing.append(f"trigger:{trigger_name}")
            continue
        definition = " ".join(
            str(row.get("function_definition") or "").lower().split()
        )
        if (
            str(row.get("table_name") or "") != contract["table"]
            or str(row.get("function_name") or "") != contract["function"]
            or str(row.get("enabled_state") or "") not in {"O", "A"}
            or type(row.get("trigger_type")) is not int
            or row.get("trigger_type") != contract["trigger_type"]
            or tuple(row.get("update_columns") or ())
            != contract["update_columns"]
            or any(str(token).lower() not in definition for token in contract["tokens"])
        ):
            missing.append(f"trigger_contract:{trigger_name}")

    index_columns = (
        "formula_id",
        "formula_version",
        "formula_schema_version",
        "engine_version",
        "feature_schema_version",
        "outcome_method_version",
    )
    if (
        not index
        or index.get("indisunique") is not True
        or index.get("indisvalid") is not True
        or index.get("indisready") is not True
        or tuple(index.get("key_columns") or ()) != index_columns
        or " ".join(
            str(index.get("predicate_definition") or "")
            .strip()
            .strip("()")
            .lower()
            .split()
        )
        != "engine_version is not null"
    ):
        missing.append(f"index:{_LIVE_APPROVAL_UNIQUE_INDEX}")
    return {"ready": not missing, "missing": sorted(set(missing))}


def schema_status() -> Dict[str, Any]:
    required = (
        "research_formula_runs",
        "research_formulas",
        "research_formula_evaluations",
        "research_formula_stage_history",
        "research_formula_shadow_checks",
        "research_formula_shadow_hits",
        "research_legacy_alert_messages",
        "research_formula_alert_subscriptions",
        "research_formula_live_deliveries",
        "research_formula_live_approvals",
        "research_first_touch_outcomes",
        "research_max_pain_snapshot_sets",
        "research_max_pain_snapshot_symbols",
        "research_max_pain_snapshot_rows",
        "research_prospective_anchor_attempts",
        "research_prospective_anchor_slots",
        "research_prospective_shadow_events",
        "research_outcome_event_rejections",
        "research_formula_evidence_snapshots",
        "research_formula_relevance_assessments",
        "research_formula_discovery_schedule_state",
    )
    required_columns = {
        "research_formula_runs": (
            "scheduler_version",
            "schedule_slot_utc",
            "source_watermark",
            "source_watermark_sha256",
            "walk_forward_policy_version",
            "purge_policy_version",
            "embargo_policy_version",
        ),
        "research_formula_shadow_checks": (
            "evaluation_status",
            "evaluation_reason",
            "input_snapshot",
            "condition_results",
            "decision_cohort_key",
            "decision_anchor_time_utc",
            "evidence_policy_version",
            "prospective_anchor_slot_id",
            "prospective_input_fingerprint",
            "feature_bundle_sha256",
            "authoritative_verified",
        ),
        "research_formula_live_approvals": (
            "formula_version",
            "horizon_minutes",
            "review_kind",
            "validation_policy_version",
            "validation_started_at_utc",
            "validation_cutoff_event_id",
            "validation_cutoff_time_utc",
            "validation_fingerprint",
            "validated_future_matches",
            "validated_future_controls",
            "validated_span_hours",
            "validated_utc_dates",
            "thresholds_met",
            "approved_by",
            "approval_reason",
            "validation_snapshot",
            "formula_schema_version",
            "engine_version",
            "feature_schema_version",
            "outcome_method_version",
            "approval_operation_version",
            "confirmation_method",
            "approval_request_fingerprint",
            "delivery_environment_enabled",
        ),
        "research_historical_opportunity_outcomes": (
            "long_first_touch_metrics",
            "short_first_touch_metrics",
            "first_touch_method_version",
            "first_touch_data_quality_status",
        ),
        "research_first_touch_outcomes": (
            "status",
            "failure_final",
            "observed_through_utc",
            "first_qualifying_move_time_utc",
            "pre_qualifying_mae_pct",
            "dwell_required_seconds",
            "path_resolution_seconds",
            "data_quality_status",
        ),
        "research_prospective_anchor_attempts": (
            "coverage_snapshot",
            "source_timestamps",
            "source_provenance",
            "frozen_inputs",
            "input_fingerprint",
            "attempt_fingerprint",
            "feature_bundle_policy_version",
            "feature_bundle_sha256",
        ),
        "research_prospective_anchor_slots": (
            "long_event_id",
            "short_event_id",
            "source_timestamps",
            "source_provenance",
            "frozen_inputs",
            "input_fingerprint",
            "feature_bundle_policy_version",
            "feature_bundle_sha256",
            "decision_feature_bundle",
        ),
        "research_prospective_shadow_events": (
            "anchor_slot_id",
            "sampler_version",
            "input_fingerprint",
            "source_side",
            "timeframe",
            "strategy_version",
            "code_version",
            "source_timestamps",
            "source_provenance",
            "feature_bundle_policy_version",
            "feature_bundle_sha256",
            "decision_feature_bundle",
        ),
        "research_outcome_event_rejections": (
            "rejection_policy_version",
            "reason_code",
            "reason_text",
            "event_snapshot",
            "rejected_at_utc",
        ),
        "research_formula_evidence_snapshots": (
            "snapshot_schema_version",
            "assessment_schema_version",
            "contract_version",
            "compatibility",
            "formula_id",
            "source_run_id",
            "formula_key",
            "formula_version",
            "formula_schema_version",
            "phase",
            "assessed_at_utc",
            "formula_family_id",
            "matched_market_episode_ids",
            "control_market_episode_ids",
            "matched_parent_market_episode_ids",
            "control_parent_market_episode_ids",
            "raw_match_count",
            "raw_control_count",
            "matched_n_eff",
            "control_n_eff",
            "snapshot_payload",
            "created_at_utc",
        ),
        "research_formula_relevance_assessments": (
            "formula_id",
            "formula_version",
            "relevance_policy_version",
            "observation_fingerprint",
            "evidence_fingerprint",
            "snapshot_id",
            "observed_at_utc",
            "observation_utc_date",
            "compatibility",
            "observation",
            "previous_state",
            "relevance_state",
            "transition",
            "weak_observation_streak",
            "recovery_evidence_streak",
            "experimental_relevance_eligible",
            "decision_payload",
            "created_at_utc",
        ),
        "research_formula_discovery_schedule_state": (
            "scheduler_version",
            "engine_version",
            "feature_schema_version",
            "outcome_method_version",
            "horizon_minutes",
            "last_slot_utc",
            "last_due_at_utc",
            "last_analysis_as_of_utc",
            "last_status",
            "last_source_watermark",
            "last_source_watermark_sha256",
            "last_discovery_run_id",
            "last_reason",
            "checked_at_utc",
            "updated_at_utc",
        ),
    }
    base = {
        "configured": bool(_database_url()),
        "schema_present": False,
        "missing_tables": list(required),
        "missing_columns": [],
        "missing_enforcement": [],
    }
    if not base["configured"] or psycopg is None:
        return base
    with _connect(read_only=True) as conn:
        missing = [table for table in required if not _table_exists(conn, table)]
        base["missing_tables"] = missing
        if missing:
            return base
        missing_columns = _missing_columns(conn, required_columns)
        base["missing_columns"] = missing_columns
        base["schema_present"] = not missing_columns
        if missing_columns:
            return base
        enforcement = _live_approval_enforcement_status(conn)
        base["live_approval_enforcement"] = enforcement
        base["missing_enforcement"] = list(enforcement.get("missing") or [])
        base["schema_present"] = bool(
            base["schema_present"] and enforcement.get("ready") is True
        )
        if not base["schema_present"]:
            return base
        counts = conn.execute(
            """
            SELECT
              (SELECT COUNT(*) FROM research_formula_runs) AS runs,
              (SELECT COUNT(*) FROM research_formulas) AS formulas,
              (SELECT COUNT(*) FROM research_formulas WHERE current_stage='SHADOW' AND active) AS shadow_formulas,
              (SELECT COUNT(*) FROM research_formulas WHERE current_stage='LIVE' AND active) AS live_formulas,
              (SELECT COUNT(*) FROM research_formula_shadow_hits) AS shadow_hits,
              (SELECT COUNT(*) FROM research_legacy_alert_messages) AS legacy_messages,
              (SELECT COUNT(*) FROM research_formula_alert_subscriptions WHERE active) AS active_subscriptions,
              (SELECT COUNT(*) FROM research_formula_live_deliveries WHERE status='PENDING') AS pending_live_deliveries,
              (SELECT COUNT(*) FROM research_outcome_event_rejections) AS rejected_outcome_events,
              (SELECT COUNT(*) FROM research_formula_evidence_snapshots) AS evidence_snapshots,
              (SELECT COUNT(*) FROM research_formula_relevance_assessments) AS relevance_assessments,
              (SELECT COUNT(*) FROM research_formula_discovery_schedule_state) AS discovery_schedule_states
            """
        ).fetchone()
        base.update({key: int(counts[key] or 0) for key in counts})
    return _json_safe(base)


def _persist_evidence_snapshot_conn(
    conn: Any,
    snapshot: research_evidence_contract.EvidenceSnapshot | Mapping[str, Any],
    *,
    formula_id: int,
    source_run_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Persist or verify one snapshot inside the caller's transaction."""

    resolved = (
        snapshot
        if isinstance(snapshot, research_evidence_contract.EvidenceSnapshot)
        else research_evidence_contract.EvidenceSnapshot.from_dict(snapshot)
    )
    payload = resolved.to_dict()
    formula = payload["formula"]
    resolved_formula_id = _strict_int(formula_id)
    resolved_run_id = None if source_run_id is None else _strict_int(source_run_id)
    if resolved_formula_id is None or resolved_formula_id <= 0:
        raise ValueError("formula_id must be a positive integer")
    if source_run_id is not None and (
        resolved_run_id is None or resolved_run_id <= 0
    ):
        raise ValueError("source_run_id must be a positive integer when supplied")

    formula_row = conn.execute(
        """
        SELECT formula_key, formula_version, formula_schema_version,
               engine_version, feature_schema_version,
               outcome_method_version, direction, horizon_minutes
        FROM research_formulas
        WHERE formula_id=%s
        FOR SHARE
        """,
        (resolved_formula_id,),
    ).fetchone()
    if not formula_row:
        raise ValueError("formula_id does not exist")
    identity_fields = (
        "formula_key",
        "formula_version",
        "formula_schema_version",
        "engine_version",
        "feature_schema_version",
        "outcome_method_version",
        "direction",
        "horizon_minutes",
    )
    mismatched = [
        key for key in identity_fields if formula_row.get(key) != formula.get(key)
    ]
    if mismatched:
        raise RuntimeError(
            "snapshot formula identity mismatch: " + ", ".join(mismatched)
        )
    conn.execute(
        """
        INSERT INTO research_formula_evidence_snapshots (
            snapshot_id, snapshot_schema_version,
            assessment_schema_version, contract_version, compatibility,
            formula_id, source_run_id, formula_key, formula_version,
            formula_schema_version, phase, assessed_at_utc,
            formula_family_id, matched_market_episode_ids,
            control_market_episode_ids, matched_parent_market_episode_ids,
            control_parent_market_episode_ids, raw_match_count,
            raw_control_count, matched_n_eff, control_n_eff,
            snapshot_payload
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb,
            %s, %s, %s, %s, %s::jsonb
        )
        ON CONFLICT (snapshot_id) DO NOTHING
        """,
        (
            resolved.snapshot_id,
            payload["snapshot_schema_version"],
            payload["assessment_schema_version"],
            payload["contract_version"],
            payload["compatibility"],
            resolved_formula_id,
            resolved_run_id,
            formula["formula_key"],
            int(formula["formula_version"]),
            formula["formula_schema_version"],
            payload["phase"],
            payload["assessed_at_utc"],
            payload["formula_family_id"],
            _json(payload["matched_market_episode_ids"]),
            _json(payload["control_market_episode_ids"]),
            _json(payload["matched_parent_market_episode_ids"]),
            _json(payload["control_parent_market_episode_ids"]),
            int(payload["raw_match_count"]),
            int(payload["raw_control_count"]),
            float(payload["matched_n_eff"]),
            float(payload["control_n_eff"]),
            _json(payload),
        ),
    )
    stored = conn.execute(
        """
        SELECT formula_id, source_run_id, snapshot_payload
        FROM research_formula_evidence_snapshots
        WHERE snapshot_id=%s
        """,
        (resolved.snapshot_id,),
    ).fetchone()
    if (
        not stored
        or int(stored["formula_id"]) != resolved_formula_id
        or stored.get("source_run_id") != resolved_run_id
        or not _type_strict_json_equal(stored.get("snapshot_payload"), payload)
    ):
        raise RuntimeError("stored evidence snapshot conflicts with its content id")
    return {
        "snapshot_id": resolved.snapshot_id,
        "formula_id": resolved_formula_id,
        "source_run_id": resolved_run_id,
        "persisted_or_verified": True,
        "live_effect": "NONE",
    }


def persist_evidence_snapshot(
    snapshot: research_evidence_contract.EvidenceSnapshot | Mapping[str, Any],
    *,
    formula_id: int,
    source_run_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Persist one content-addressed snapshot without changing formula state."""

    with _connect(read_only=False) as conn:
        result = _persist_evidence_snapshot_conn(
            conn,
            snapshot,
            formula_id=formula_id,
            source_run_id=source_run_id,
        )
        conn.commit()
    return result


def load_evidence_snapshot(
    snapshot_id: str,
) -> Optional[research_evidence_contract.EvidenceSnapshot]:
    """Load and fingerprint-verify one append-only EvidenceSnapshot."""

    identifier = str(snapshot_id or "").strip().lower()
    if len(identifier) != 64 or any(
        character not in "0123456789abcdef" for character in identifier
    ):
        raise ValueError("snapshot_id must be a lowercase 64-character SHA-256 id")
    with _connect(read_only=True) as conn:
        row = conn.execute(
            """
            SELECT snapshot_payload
            FROM research_formula_evidence_snapshots
            WHERE snapshot_id=%s
            """,
            (identifier,),
        ).fetchone()
    if not row:
        return None
    return research_evidence_contract.EvidenceSnapshot.from_dict(
        row["snapshot_payload"]
    )


def _discovery_horizon(value: Any) -> int:
    horizon = _strict_int(value)
    if horizon not in {60, 240, 720, 1440}:
        raise ValueError("horizon_minutes must be 60, 240, 720 or 1440")
    return int(horizon)


@contextmanager
def discovery_horizon_lock(horizon_minutes: int):
    """Hold one session advisory lock for an entire horizon computation."""

    horizon = _discovery_horizon(horizon_minutes)
    lock_id = _DISCOVERY_LOCK_BASE_ID + horizon
    conn = _connect(read_only=False)
    acquired = False
    try:
        row = conn.execute(
            "SELECT pg_try_advisory_lock(%s) AS acquired", (lock_id,)
        ).fetchone()
        acquired = bool(row and row.get("acquired") is True)
        yield acquired
    finally:
        if acquired:
            try:
                conn.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))
            except Exception:
                pass
        conn.close()


def load_discovery_schedule_state(
    horizon_minutes: int,
    *,
    scheduler_version: str = DISCOVERY_SCHEDULER_VERSION,
) -> Optional[Dict[str, Any]]:
    horizon = _discovery_horizon(horizon_minutes)
    version = str(scheduler_version or "").strip()
    if not version:
        raise ValueError("scheduler_version is required")
    with _connect(read_only=True) as conn:
        if not _table_exists(conn, "research_formula_discovery_schedule_state"):
            raise RuntimeError("Formula Discovery scheduler schema is not installed")
        row = conn.execute(
            """
            SELECT scheduler_version, engine_version, feature_schema_version,
                   outcome_method_version, horizon_minutes, last_slot_utc,
                   last_due_at_utc, last_analysis_as_of_utc, last_status,
                   last_source_watermark, last_source_watermark_sha256,
                   last_discovery_run_id, last_reason, checked_at_utc,
                   updated_at_utc
            FROM research_formula_discovery_schedule_state
            WHERE scheduler_version=%s
              AND engine_version=%s
              AND feature_schema_version=%s
              AND outcome_method_version=%s
              AND horizon_minutes=%s
            """,
            (
                version,
                research_formula_engine.ENGINE_VERSION,
                research_feature_matrix.FEATURE_SCHEMA_VERSION,
                research_feature_matrix.VERIFIED_OUTCOME_METHOD,
                horizon,
            ),
        ).fetchone()
    return dict(row) if row else None


def load_scheduled_discovery_run(
    horizon_minutes: int,
    *,
    schedule_slot_utc: Any,
    scheduler_version: str = DISCOVERY_SCHEDULER_VERSION,
) -> Optional[Dict[str, Any]]:
    """Recover a committed run if state persistence was interrupted."""

    horizon = _discovery_horizon(horizon_minutes)
    slot = _as_utc(schedule_slot_utc)
    version = str(scheduler_version or "").strip()
    with _connect(read_only=True) as conn:
        row = conn.execute(
            """
            SELECT run_id, horizon_minutes, status, schedule_slot_utc,
                   source_watermark, source_watermark_sha256,
                   completed_at_utc
            FROM research_formula_runs
            WHERE scheduler_version=%s
              AND engine_version=%s
              AND feature_schema_version=%s
              AND outcome_method_version=%s
              AND horizon_minutes=%s
              AND schedule_slot_utc=%s
              AND status='COMPLETED'
            ORDER BY run_id DESC
            LIMIT 1
            """,
            (
                version,
                research_formula_engine.ENGINE_VERSION,
                research_feature_matrix.FEATURE_SCHEMA_VERSION,
                research_feature_matrix.VERIFIED_OUTCOME_METHOD,
                horizon,
                slot,
            ),
        ).fetchone()
    return dict(row) if row else None


def record_discovery_schedule_state(
    *,
    horizon_minutes: int,
    slot_utc: Any,
    due_at_utc: Any,
    status: str,
    source_watermark: Optional[Mapping[str, Any]],
    source_watermark_sha256: Optional[str],
    discovery_run_id: Optional[int] = None,
    reason: Optional[str] = None,
    checked_at_utc: Any = None,
    scheduler_version: str = DISCOVERY_SCHEDULER_VERSION,
) -> Dict[str, Any]:
    """Advance one locked horizon state after a terminal scheduled attempt."""

    horizon = _discovery_horizon(horizon_minutes)
    version = str(scheduler_version or "").strip()
    normalized_status = str(status or "").strip().upper()
    allowed_statuses = {
        "COMPLETED",
        "SKIPPED_UNCHANGED",
        "SKIPPED_UNAVAILABLE",
        "FAILED",
    }
    if not version or normalized_status not in allowed_statuses:
        raise ValueError("invalid Discovery scheduler state")
    slot = _as_utc(slot_utc)
    due_at = _as_utc(due_at_utc)
    checked_at = _as_utc(checked_at_utc or datetime.now(timezone.utc))
    if due_at < slot:
        raise ValueError("due_at_utc cannot precede schedule slot")
    watermark = (
        dict(source_watermark)
        if isinstance(source_watermark, Mapping)
        else None
    )
    watermark_sha = str(source_watermark_sha256 or "").strip() or None
    if (watermark is None) != (watermark_sha is None):
        raise ValueError("watermark payload and SHA-256 must be supplied together")
    if watermark_sha is not None and (
        len(watermark_sha) != 64
        or any(character not in "0123456789abcdef" for character in watermark_sha)
    ):
        raise ValueError("source_watermark_sha256 must be lowercase SHA-256")
    run_id = _strict_int(discovery_run_id)
    if normalized_status == "COMPLETED" and (run_id is None or run_id <= 0):
        raise ValueError("COMPLETED scheduler state requires discovery_run_id")
    with _connect(read_only=False) as conn:
        row = conn.execute(
            """
            INSERT INTO research_formula_discovery_schedule_state (
                scheduler_version, engine_version, feature_schema_version,
                outcome_method_version, horizon_minutes, last_slot_utc,
                last_due_at_utc, last_analysis_as_of_utc, last_status,
                last_source_watermark, last_source_watermark_sha256,
                last_discovery_run_id, last_reason, checked_at_utc,
                updated_at_utc
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s::jsonb, %s, %s, %s, %s, NOW()
            )
            ON CONFLICT (
                scheduler_version, engine_version, feature_schema_version,
                outcome_method_version, horizon_minutes
            ) DO UPDATE
            SET last_slot_utc=EXCLUDED.last_slot_utc,
                last_due_at_utc=EXCLUDED.last_due_at_utc,
                last_analysis_as_of_utc=EXCLUDED.last_analysis_as_of_utc,
                last_status=EXCLUDED.last_status,
                last_source_watermark=EXCLUDED.last_source_watermark,
                last_source_watermark_sha256=EXCLUDED.last_source_watermark_sha256,
                last_discovery_run_id=EXCLUDED.last_discovery_run_id,
                last_reason=EXCLUDED.last_reason,
                checked_at_utc=EXCLUDED.checked_at_utc,
                updated_at_utc=NOW()
            WHERE research_formula_discovery_schedule_state.last_slot_utc
                  <= EXCLUDED.last_slot_utc
            RETURNING scheduler_version, engine_version, feature_schema_version,
                      outcome_method_version, horizon_minutes, last_slot_utc,
                      last_due_at_utc, last_analysis_as_of_utc, last_status,
                      last_source_watermark, last_source_watermark_sha256,
                      last_discovery_run_id, last_reason, checked_at_utc,
                      updated_at_utc
            """,
            (
                version,
                research_formula_engine.ENGINE_VERSION,
                research_feature_matrix.FEATURE_SCHEMA_VERSION,
                research_feature_matrix.VERIFIED_OUTCOME_METHOD,
                horizon,
                slot,
                due_at,
                due_at,
                normalized_status,
                _json(watermark) if watermark is not None else None,
                watermark_sha,
                run_id,
                str(reason or "")[:1000] or None,
                checked_at,
            ),
        ).fetchone()
        conn.commit()
    if not row:
        current = load_discovery_schedule_state(
            horizon, scheduler_version=version
        )
        if current is None:
            raise RuntimeError("Discovery scheduler state upsert failed")
        return current
    return dict(row)


def latest_completed_discovery_runs(
    horizons: Sequence[int],
    *,
    lookback_days: int,
    config: Mapping[str, Any],
) -> Dict[int, datetime]:
    """Return the latest compatible completed run for each requested horizon."""
    requested = sorted({int(horizon) for horizon in horizons})
    if not requested:
        return {}
    with _connect(read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT horizon_minutes, MAX(completed_at_utc) AS completed_at_utc
            FROM research_formula_runs
            WHERE status='COMPLETED'
              AND completed_at_utc IS NOT NULL
              AND engine_version=%s
              AND feature_schema_version=%s
              AND outcome_method_version=%s
              AND lookback_days=%s
              AND config=%s::jsonb
              AND horizon_minutes=ANY(%s)
            GROUP BY horizon_minutes
            """,
            (
                research_formula_engine.ENGINE_VERSION,
                research_feature_matrix.FEATURE_SCHEMA_VERSION,
                research_feature_matrix.VERIFIED_OUTCOME_METHOD,
                int(lookback_days),
                _json(config),
                requested,
            ),
        ).fetchall()
    return {
        int(row["horizon_minutes"]): row["completed_at_utc"]
        for row in rows
        if row.get("completed_at_utc") is not None
    }


def _target_stage(value: Any) -> str:
    stage = str(value or "DISCOVERED").upper()
    if stage not in _AUTOMATIC_STAGE_PATH:
        return "DISCOVERED"
    return stage


def _advance_stage(
    conn,
    *,
    formula_id: int,
    current_stage: str,
    target_stage: str,
    run_id: int,
    reason: str,
) -> str:
    current = str(current_stage or "DISCOVERED").upper()
    target = _target_stage(target_stage)
    if current in {"APPROVED", "LIVE", "RETIRED"}:
        return current
    if _STAGE_ORDER.get(target, 0) <= _STAGE_ORDER.get(current, 0):
        return current
    current_index = _AUTOMATIC_STAGE_PATH.index(current)
    target_index = _AUTOMATIC_STAGE_PATH.index(target)
    for next_stage in _AUTOMATIC_STAGE_PATH[current_index + 1 : target_index + 1]:
        conn.execute(
            """
            INSERT INTO research_formula_stage_history (
                formula_id, run_id, from_stage, to_stage, reason, actor
            ) VALUES (%s, %s, %s, %s, %s, 'automatic-research-engine')
            ON CONFLICT (formula_id, run_id, to_stage) DO NOTHING
            """,
            (formula_id, run_id, current, next_stage, reason),
        )
        current = next_stage
    if current == "SHADOW":
        latest_event = conn.execute(
            """
            SELECT COALESCE(MAX(event_id), 0)::bigint AS event_id
            FROM research_events
            WHERE event_kind='ALERT' AND delivery_status='DELIVERED'
            """
        ).fetchone()
        conn.execute(
            """
            UPDATE research_formulas
            SET current_stage=%s,
                shadow_started_at_utc=COALESCE(shadow_started_at_utc, NOW()),
                last_shadow_event_id=GREATEST(last_shadow_event_id, %s),
                updated_at_utc=NOW()
            WHERE formula_id=%s
            """,
            (current, int(latest_event["event_id"] or 0), formula_id),
        )
    else:
        conn.execute(
            """
            UPDATE research_formulas
            SET current_stage=%s, updated_at_utc=NOW()
            WHERE formula_id=%s
            """,
            (current, formula_id),
        )
    return current


def _requested_stage_for_dataset(
    formula: Mapping[str, Any], *, replacement_ready: bool
) -> tuple[str, list[Any]]:
    """Apply the dataset-readiness ceiling to one discovered formula."""
    requested_stage = str(formula["recommended_stage"])
    gate_notes = list(formula.get("gate_notes") or [])
    if (
        not replacement_ready
        and _STAGE_ORDER.get(requested_stage, 0) > _STAGE_ORDER["BACKTESTED"]
    ):
        requested_stage = "BACKTESTED"
        gate_notes.append(
            "automatic stage capped at BACKTESTED until replacement dataset coverage is ready"
        )
    return requested_stage, gate_notes


def _formula_identity_matches_discovery(
    stored: Mapping[str, Any],
    discovered: Mapping[str, Any],
    *,
    outcome_method_version: str,
) -> bool:
    """Verify that a formula-key lookup resolved the exact immutable contract."""
    stored_formula_version = _strict_int(stored.get("formula_version"))
    discovered_formula_version = _strict_int(
        discovered.get("formula_version", 1)
    )
    stored_horizon = _strict_int(stored.get("horizon_minutes"))
    discovered_horizon = _strict_int(discovered.get("horizon_minutes"))
    stored_count = _strict_int(stored.get("condition_count"))
    discovered_count = _strict_int(discovered.get("condition_count"))
    if any(
        value is None or value <= 0
        for value in (
            stored_formula_version,
            discovered_formula_version,
            stored_horizon,
            discovered_horizon,
            stored_count,
            discovered_count,
        )
    ):
        return False
    scalar_match = bool(
        stored_formula_version == discovered_formula_version
        and str(stored.get("formula_schema_version") or "")
        == str(discovered.get("formula_schema_version") or "")
        and str(stored.get("engine_version") or "")
        == str(discovered.get("engine_version") or "")
        and str(stored.get("feature_schema_version") or "")
        == str(discovered.get("feature_schema_version") or "")
        and str(stored.get("outcome_method_version") or "")
        == str(outcome_method_version or "")
        and str(stored.get("direction") or "").upper()
        == str(discovered.get("direction") or "").upper()
        and stored_horizon == discovered_horizon
        and stored_count == discovered_count
    )
    return scalar_match and _type_strict_json_equal(
        stored.get("conditions"), discovered.get("conditions")
    )


def _validate_discovery_condition_families(
    *,
    dataset: Mapping[str, Any],
    discovery: Mapping[str, Any],
    formulas: Sequence[Mapping[str, Any]],
) -> None:
    """Reject a corrupt current-engine cohort before opening PostgreSQL."""

    discovery_horizon = _strict_int(discovery.get("horizon_minutes"))
    dataset_horizon = _strict_int(dataset.get("horizon_minutes"))
    runtime_mismatches: list[str] = []
    if discovery.get("engine_version") != research_formula_engine.ENGINE_VERSION:
        runtime_mismatches.append("discovery.engine_version")
    if (
        discovery.get("formula_schema_version")
        != research_formula_engine.FORMULA_SCHEMA_VERSION
    ):
        runtime_mismatches.append("discovery.formula_schema_version")
    if (
        discovery.get("feature_schema_version")
        != research_feature_matrix.FEATURE_SCHEMA_VERSION
    ):
        runtime_mismatches.append("discovery.feature_schema_version")
    if discovery_horizon not in {60, 240, 720, 1440}:
        runtime_mismatches.append("discovery.horizon_minutes")
    if dataset.get("available") is not True:
        runtime_mismatches.append("dataset.available")
    if (
        dataset.get("feature_schema_version")
        != research_feature_matrix.FEATURE_SCHEMA_VERSION
    ):
        runtime_mismatches.append("dataset.feature_schema_version")
    if (
        dataset.get("outcome_method_version")
        != research_feature_matrix.VERIFIED_OUTCOME_METHOD
    ):
        runtime_mismatches.append("dataset.outcome_method_version")
    if dataset_horizon != discovery_horizon:
        runtime_mismatches.append("dataset.horizon_minutes")
    if runtime_mismatches:
        raise ValueError(
            "Discovery persistence runtime contract mismatch: "
            + ", ".join(runtime_mismatches)
        )

    declared_policy = str(
        discovery.get("condition_family_policy_version") or ""
    ).strip()
    expected_policy = (
        research_formula_families.CONDITION_FAMILY_POLICY_VERSION
    )
    if declared_policy != expected_policy:
        raise ValueError(
            "Discovery condition-family policy version mismatch: "
            f"expected {expected_policy!r}, got {declared_policy!r}"
        )
    config = discovery.get("config")
    if not isinstance(config, Mapping):
        raise ValueError("Discovery config must freeze condition-family exceptions")
    raw_exceptions = config.get("condition_family_exceptions", ())
    if not isinstance(raw_exceptions, (list, tuple)) or any(
        not isinstance(value, str) for value in raw_exceptions
    ):
        raise ValueError("condition_family_exceptions must be a string array")
    exceptions = tuple(str(value).strip() for value in raw_exceptions)
    for value in exceptions:
        family, separator, reason = value.partition(":")
        if not separator or not family.strip() or len(reason.strip()) < 8:
            raise ValueError(
                "condition-family exceptions require 'family: written justification'"
            )

    violations: list[str] = []
    for rank, formula in enumerate(formulas, start=1):
        if not isinstance(formula, Mapping):
            violations.append(f"rank {rank}: formula must be an object")
            continue
        raw_formula_key = formula.get("formula_key")
        formula_key = raw_formula_key if isinstance(raw_formula_key, str) else ""
        formula_label = formula_key.strip() or f"rank-{rank}"
        formula_horizon = _strict_int(formula.get("horizon_minutes"))
        raw_formula_direction = formula.get("direction")
        formula_direction = (
            raw_formula_direction
            if isinstance(raw_formula_direction, str)
            else ""
        )
        formula_runtime_mismatches: list[str] = []
        if _strict_int(formula.get("formula_version")) != 1:
            formula_runtime_mismatches.append("formula_version")
        if (
            formula.get("formula_schema_version")
            != research_formula_engine.FORMULA_SCHEMA_VERSION
        ):
            formula_runtime_mismatches.append("formula_schema_version")
        if formula.get("engine_version") != research_formula_engine.ENGINE_VERSION:
            formula_runtime_mismatches.append("engine_version")
        if (
            formula.get("feature_schema_version")
            != research_feature_matrix.FEATURE_SCHEMA_VERSION
        ):
            formula_runtime_mismatches.append("feature_schema_version")
        if formula_horizon != discovery_horizon:
            formula_runtime_mismatches.append("horizon_minutes")
        if formula_direction not in {"LONG", "SHORT"}:
            formula_runtime_mismatches.append("direction")
        if formula_runtime_mismatches:
            violations.append(
                f"{formula_label}: runtime contract mismatch "
                + ", ".join(formula_runtime_mismatches)
            )
            continue
        conditions = formula.get("conditions")
        condition_count = _strict_int(formula.get("condition_count"))
        if (
            not isinstance(conditions, (list, tuple))
            or not conditions
            or any(not isinstance(item, Mapping) for item in conditions)
            or condition_count != len(conditions)
            or condition_count not in {1, 2, 3, 4, 5}
        ):
            violations.append(
                f"{formula_label}: invalid conditions/condition_count contract"
            )
            continue
        try:
            expected_formula_key = research_formula_engine.formula_key(
                direction=formula_direction,
                horizon_minutes=int(formula_horizon),
                feature_schema_version=str(formula["feature_schema_version"]),
                conditions=conditions,
                condition_family_exceptions=exceptions,
            )
        except (KeyError, TypeError, ValueError) as exc:
            violations.append(
                f"{formula_label}: invalid formula-key inputs ({exc})"
            )
            continue
        if formula_key != expected_formula_key:
            violations.append(
                f"{formula_label}: formula_key does not bind the exact "
                "runtime, conditions and condition-family exceptions"
            )
            continue
        policy = research_formula_families.condition_family_policy(
            conditions,
            justified_exceptions=exceptions,
            enforce_correlated_families=True,
        )
        if policy["valid"] is not True:
            violations.append(
                f"{formula_label}: " + ", ".join(policy["reasons"])
            )
            continue
        multiple_testing = formula.get("multiple_testing")
        frozen_policy = (
            multiple_testing.get("condition_family_policy")
            if isinstance(multiple_testing, Mapping)
            else None
        )
        expected_frozen = {
            "policy_version": expected_policy,
            "enforcement": "ALL_CONDITION_DEPTHS",
            "families": list(policy["families"]),
            "justified_exceptions": list(exceptions),
        }
        if not isinstance(frozen_policy, Mapping) or not _type_strict_json_equal(
            frozen_policy, expected_frozen
        ):
            violations.append(
                f"{formula_label}: frozen condition-family metadata mismatch"
            )
    if violations:
        raise ValueError(
            "Discovery cohort violates the fail-closed condition-family policy: "
            + "; ".join(violations[:10])
        )


def persist_discovery_run(
    *,
    dataset: Mapping[str, Any],
    discovery: Mapping[str, Any],
    lookback_days: int,
    scheduler_metadata: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Atomically store one completed run, immutable formulas and evaluations."""
    if discovery.get("available") is not True:
        raise ValueError("cannot persist an unavailable discovery result")
    raw_formulas = discovery.get("formulas")
    if not isinstance(raw_formulas, (list, tuple)):
        raise ValueError("Discovery formulas must be an explicit array")
    formulas = list(raw_formulas)
    _validate_discovery_condition_families(
        dataset=dataset,
        discovery=discovery,
        formulas=formulas,
    )
    coverage_source = dataset.get("coverage")
    coverage = (
        dict(coverage_source) if isinstance(coverage_source, Mapping) else {}
    )
    replacement_verification = _verified_replacement_readiness(
        dataset=dataset,
        discovery=discovery,
        formulas=formulas,
    )
    replacement_ready = bool(replacement_verification["verified"])
    coverage_for_run = dict(coverage)
    coverage_for_run["declared_replacement_ready"] = coverage.get(
        "replacement_ready"
    )
    coverage_for_run["replacement_ready"] = replacement_ready
    coverage_for_run["store_replacement_verification"] = (
        replacement_verification
    )
    coverage_for_run["analysis_as_of_utc"] = discovery.get(
        "analysis_as_of_utc"
    )
    coverage_for_run["condition_family_policy_version"] = (
        research_formula_families.CONDITION_FAMILY_POLICY_VERSION
    )
    coverage_for_run["condition_family_enforcement"] = "ALL_CONDITION_DEPTHS"
    dataset_kind = str(coverage.get("dataset_kind") or "unknown")
    outcome_method_version = str(
        dataset.get("outcome_method_version")
        or research_feature_matrix.VERIFIED_OUTCOME_METHOD
    )
    schedule = (
        dict(scheduler_metadata)
        if isinstance(scheduler_metadata, Mapping)
        else {}
    )
    scheduler_version = str(schedule.get("scheduler_version") or "").strip() or None
    schedule_slot = (
        _as_utc(schedule["schedule_slot_utc"])
        if schedule.get("schedule_slot_utc") not in (None, "")
        else None
    )
    source_watermark = schedule.get("source_watermark")
    source_watermark_sha = str(
        schedule.get("source_watermark_sha256") or ""
    ).strip() or None
    if scheduler_version is not None:
        if (
            scheduler_version != DISCOVERY_SCHEDULER_VERSION
            or schedule_slot is None
            or not isinstance(source_watermark, Mapping)
            or source_watermark_sha is None
            or len(source_watermark_sha) != 64
            or any(
                character not in "0123456789abcdef"
                for character in source_watermark_sha
            )
            or schedule.get("walk_forward_policy_version")
            != research_formula_engine.WALK_FORWARD_POLICY_VERSION
            or schedule.get("purge_policy_version")
            != research_formula_engine.PURGE_POLICY_VERSION
            or schedule.get("embargo_policy_version")
            != research_formula_engine.EMBARGO_POLICY_VERSION
        ):
            raise ValueError("invalid versioned Discovery scheduler metadata")
    elif schedule:
        raise ValueError("partial Discovery scheduler metadata is forbidden")
    with _connect(read_only=False) as conn:
        if not _table_exists(conn, "research_formulas"):
            raise RuntimeError("Formula Research schema is not installed")
        run = conn.execute(
            """
            INSERT INTO research_formula_runs (
                engine_version, feature_schema_version, outcome_method_version,
                horizon_minutes, lookback_days, status,
                dataset_start_utc, dataset_end_utc, holdout_start_utc,
                sample_size, discovery_sample_size, holdout_sample_size,
                candidates_evaluated, config, coverage,
                scheduler_version, schedule_slot_utc, source_watermark,
                source_watermark_sha256, walk_forward_policy_version,
                purge_policy_version, embargo_policy_version
            ) VALUES (
                %s, %s, %s, %s, %s, 'RUNNING', %s, %s, %s,
                %s, %s, %s, %s, %s::jsonb, %s::jsonb,
                %s, %s, %s::jsonb, %s, %s, %s, %s
            ) RETURNING run_id
            """,
            (
                discovery["engine_version"],
                discovery["feature_schema_version"],
                outcome_method_version,
                int(discovery["horizon_minutes"]),
                int(lookback_days),
                discovery.get("first_alert_time_utc"),
                discovery.get("last_alert_time_utc"),
                discovery.get("holdout_start_time_utc"),
                int(discovery.get("sample_size") or 0),
                int(discovery.get("discovery_sample_size") or 0),
                int(discovery.get("holdout_sample_size") or 0),
                int(discovery.get("candidates_evaluated") or 0),
                _json(discovery.get("config") or {}),
                _json(coverage_for_run),
                scheduler_version,
                schedule_slot,
                _json(source_watermark)
                if isinstance(source_watermark, Mapping)
                else None,
                source_watermark_sha,
                schedule.get("walk_forward_policy_version"),
                schedule.get("purge_policy_version"),
                schedule.get("embargo_policy_version"),
            ),
        ).fetchone()
        run_id = int(run["run_id"])
        persisted = 0
        stage_counts: Dict[str, int] = {}
        for global_rank, formula in enumerate(formulas, start=1):
            requested_stage, gate_notes = _requested_stage_for_dataset(
                formula,
                replacement_ready=replacement_ready,
            )
            existing = conn.execute(
                """
                SELECT formula_id, formula_version, formula_schema_version,
                       engine_version, feature_schema_version,
                       outcome_method_version, direction, horizon_minutes,
                       conditions, condition_count, formula_text,
                       current_stage, active
                FROM research_formulas
                WHERE formula_key=%s
                FOR UPDATE
                """,
                (formula["formula_key"],),
            ).fetchone()
            if existing:
                formula_id = int(existing["formula_id"])
                current_stage = str(existing["current_stage"]).upper()
                if not _formula_identity_matches_discovery(
                    existing,
                    formula,
                    outcome_method_version=outcome_method_version,
                ):
                    raise RuntimeError(
                        "formula_key resolved a different immutable formula contract"
                    )
                if current_stage not in _PROTECTED_FORMULA_STAGES:
                    conn.execute(
                        """
                        UPDATE research_formulas
                        SET latest_evaluation_run_id=%s,
                            formula_text=%s, updated_at_utc=NOW()
                        WHERE formula_id=%s
                          AND current_stage NOT IN (
                              'SHADOW', 'APPROVED', 'LIVE', 'RETIRED'
                          )
                        """,
                        (run_id, formula["formula_text"], formula_id),
                    )
            else:
                inserted = conn.execute(
                    """
                    INSERT INTO research_formulas (
                        formula_key, formula_version, formula_schema_version,
                        engine_version, feature_schema_version,
                        outcome_method_version, direction, horizon_minutes,
                        conditions, condition_count, formula_text, current_stage,
                        first_seen_run_id, latest_evaluation_run_id
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s::jsonb, %s, %s, 'DISCOVERED', %s, %s
                    ) RETURNING formula_id
                    """,
                    (
                        formula["formula_key"],
                        int(formula.get("formula_version") or 1),
                        formula["formula_schema_version"],
                        formula["engine_version"],
                        formula["feature_schema_version"],
                        outcome_method_version,
                        formula["direction"],
                        int(formula["horizon_minutes"]),
                        _json(formula["conditions"]),
                        int(formula["condition_count"]),
                        formula["formula_text"],
                        run_id,
                        run_id,
                    ),
                ).fetchone()
                formula_id = int(inserted["formula_id"])
                current_stage = "DISCOVERED"
                conn.execute(
                    """
                    INSERT INTO research_formula_stage_history (
                        formula_id, run_id, from_stage, to_stage, reason, actor
                    ) VALUES (%s, %s, NULL, 'DISCOVERED', 'first discovery', 'automatic-research-engine')
                    ON CONFLICT (formula_id, run_id, to_stage) DO NOTHING
                    """,
                    (formula_id, run_id),
                )

            conn.execute(
                """
                INSERT INTO research_formula_evaluations (
                    run_id, formula_id, rank_in_run, ranking_score,
                    discovery_metrics, holdout_metrics, multiple_testing,
                    recommended_stage, gate_notes
                ) VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s::jsonb)
                ON CONFLICT (run_id, formula_id) DO NOTHING
                """,
                (
                    run_id,
                    formula_id,
                    global_rank,
                    float(formula["ranking_score"]),
                    _json(formula["discovery_metrics"]),
                    _json(formula["holdout_metrics"]),
                    _json(formula["multiple_testing"]),
                    requested_stage,
                    _json(gate_notes),
                ),
            )
            final_stage = _advance_stage(
                conn,
                formula_id=formula_id,
                current_stage=current_stage,
                target_stage=requested_stage,
                run_id=run_id,
                reason=(
                    "automatic chronological discovery/holdout evaluation; "
                    f"dataset={dataset_kind}; replacement_ready={replacement_ready}"
                ),
            )
            stage_counts[final_stage] = stage_counts.get(final_stage, 0) + 1
            persisted += 1

        # Durable retirement is intentionally last: the new same-horizon
        # cohort and its evaluations must already exist in this transaction,
        # and at least one active current-schema/current-run formula must remain
        # BACKTESTED or higher. Inactive/retired rows cannot authorize removal
        # of a predecessor. SHADOW/APPROVED/LIVE predecessors are never selected.
        active_replacements = conn.execute(
            """
            SELECT formula_id, formula_version, current_stage, active,
                   formula_schema_version, engine_version,
                   feature_schema_version, outcome_method_version,
                   horizon_minutes, latest_evaluation_run_id
            FROM research_formulas
            WHERE active=TRUE
              AND formula_version=1
              AND formula_schema_version=%s
              AND engine_version=%s
              AND feature_schema_version=%s
              AND outcome_method_version=%s
              AND horizon_minutes=%s
              AND latest_evaluation_run_id=%s
              AND current_stage IN (
                  'BACKTESTED', 'HOLDOUT_PASSED', 'SHADOW', 'APPROVED', 'LIVE'
              )
            LIMIT 1
            FOR UPDATE
            """,
            (
                research_formula_engine.FORMULA_SCHEMA_VERSION,
                research_formula_engine.ENGINE_VERSION,
                research_feature_matrix.FEATURE_SCHEMA_VERSION,
                research_feature_matrix.VERIFIED_OUTCOME_METHOD,
                int(discovery["horizon_minutes"]),
                run_id,
            ),
        ).fetchall() if replacement_ready else []
        retirement_ready = _replacement_cohort_supports_retirement(
            replacement_ready,
            active_replacements,
            horizon_minutes=int(discovery["horizon_minutes"]),
            run_id=run_id,
        )
        superseded = conn.execute(
            """
            SELECT formula_id, current_stage
            FROM research_formulas
            WHERE active=TRUE
              AND (
                  formula_schema_version IS DISTINCT FROM %s
                  OR engine_version IS DISTINCT FROM %s
                  OR feature_schema_version IS DISTINCT FROM %s
                  OR outcome_method_version IS DISTINCT FROM %s
              )
              AND horizon_minutes=%s
              AND current_stage NOT IN ('SHADOW', 'APPROVED', 'LIVE', 'RETIRED')
            FOR UPDATE
            """,
            (
                research_formula_engine.FORMULA_SCHEMA_VERSION,
                research_formula_engine.ENGINE_VERSION,
                research_feature_matrix.FEATURE_SCHEMA_VERSION,
                research_feature_matrix.VERIFIED_OUTCOME_METHOD,
                int(discovery["horizon_minutes"]),
            ),
        ).fetchall() if retirement_ready else []
        for old_formula in superseded:
            conn.execute(
                """
                INSERT INTO research_formula_stage_history (
                    formula_id, run_id, from_stage, to_stage, reason, actor
                ) VALUES (%s, %s, %s, 'RETIRED', %s, 'automatic-research-engine')
                ON CONFLICT (formula_id, run_id, to_stage) DO NOTHING
                """,
                (
                    int(old_formula["formula_id"]),
                    run_id,
                    str(old_formula["current_stage"]),
                    (
                        "superseded by current versioned Formula Discovery cohort "
                        f"from {dataset_kind}"
                    ),
                ),
            )
        if superseded:
            superseded_ids = sorted(
                {int(row["formula_id"]) for row in superseded}
            )
            retired_cursor = conn.execute(
                """
                UPDATE research_formulas
                SET current_stage='RETIRED', active=FALSE, updated_at_utc=NOW()
                WHERE formula_id=ANY(%s)
                  AND active=TRUE
                  AND (
                      formula_schema_version IS DISTINCT FROM %s
                      OR engine_version IS DISTINCT FROM %s
                      OR feature_schema_version IS DISTINCT FROM %s
                      OR outcome_method_version IS DISTINCT FROM %s
                  )
                  AND horizon_minutes=%s
                  AND current_stage NOT IN ('SHADOW', 'APPROVED', 'LIVE', 'RETIRED')
                """,
                (
                    superseded_ids,
                    research_formula_engine.FORMULA_SCHEMA_VERSION,
                    research_formula_engine.ENGINE_VERSION,
                    research_feature_matrix.FEATURE_SCHEMA_VERSION,
                    research_feature_matrix.VERIFIED_OUTCOME_METHOD,
                    int(discovery["horizon_minutes"]),
                ),
            )
            if retired_cursor.rowcount != len(superseded_ids):
                raise RuntimeError(
                    "superseded formula retirement changed after row locking"
                )

        # Keep the active discovery surface tied to the newest chronological
        # cohort for this horizon.  Earlier candidates remain in the audit
        # archive, but a stale BACKTESTED formula must not outrank a formula
        # evaluated against the newer/larger dataset.  Frozen SHADOW/LIVE
        # formulas are intentionally preserved for genuine future validation.
        stale_candidates = conn.execute(
            """
            SELECT formula_id, current_stage
            FROM research_formulas
            WHERE active=TRUE
              AND formula_schema_version=%s
              AND engine_version=%s
              AND feature_schema_version=%s
              AND outcome_method_version=%s
              AND horizon_minutes=%s
              AND current_stage IN ('DISCOVERED', 'BACKTESTED', 'HOLDOUT_PASSED')
              AND latest_evaluation_run_id IS DISTINCT FROM %s
            FOR UPDATE
            """,
            (
                research_formula_engine.FORMULA_SCHEMA_VERSION,
                research_formula_engine.ENGINE_VERSION,
                research_feature_matrix.FEATURE_SCHEMA_VERSION,
                research_feature_matrix.VERIFIED_OUTCOME_METHOD,
                int(discovery["horizon_minutes"]),
                run_id,
            ),
        ).fetchall() if retirement_ready else []
        for stale_formula in stale_candidates:
            conn.execute(
                """
                INSERT INTO research_formula_stage_history (
                    formula_id, run_id, from_stage, to_stage, reason, actor
                ) VALUES (%s, %s, %s, 'RETIRED', %s, 'automatic-research-engine')
                ON CONFLICT (formula_id, run_id, to_stage) DO NOTHING
                """,
                (
                    int(stale_formula["formula_id"]),
                    run_id,
                    str(stale_formula["current_stage"]),
                    "superseded by newer same-horizon discovery cohort",
                ),
            )
        if stale_candidates:
            stale_candidate_ids = sorted(
                {int(row["formula_id"]) for row in stale_candidates}
            )
            stale_cursor = conn.execute(
                """
                UPDATE research_formulas
                SET current_stage='RETIRED', active=FALSE, updated_at_utc=NOW()
                WHERE formula_id=ANY(%s)
                  AND active=TRUE
                  AND formula_schema_version=%s
                  AND engine_version=%s
                  AND feature_schema_version=%s
                  AND outcome_method_version=%s
                  AND horizon_minutes=%s
                  AND current_stage IN ('DISCOVERED', 'BACKTESTED', 'HOLDOUT_PASSED')
                  AND latest_evaluation_run_id IS DISTINCT FROM %s
                """,
                (
                    stale_candidate_ids,
                    research_formula_engine.FORMULA_SCHEMA_VERSION,
                    research_formula_engine.ENGINE_VERSION,
                    research_feature_matrix.FEATURE_SCHEMA_VERSION,
                    research_feature_matrix.VERIFIED_OUTCOME_METHOD,
                    int(discovery["horizon_minutes"]),
                    run_id,
                ),
            )
            if stale_cursor.rowcount != len(stale_candidate_ids):
                raise RuntimeError(
                    "stale formula retirement changed after row locking"
                )

        conn.execute(
            """
            UPDATE research_formula_runs
            SET status='COMPLETED', formulas_persisted=%s,
                completed_at_utc=NOW()
            WHERE run_id=%s
            """,
            (persisted, run_id),
        )
        conn.commit()
    return {
        "run_id": run_id,
        "horizon_minutes": int(discovery["horizon_minutes"]),
        "formulas_persisted": persisted,
        "stage_counts": stage_counts,
        "replacement_ready": replacement_ready,
        "replacement_readiness_reasons": list(
            replacement_verification["reasons"]
        ),
        "retirement_ready": retirement_ready,
        "scheduler_version": scheduler_version,
        "schedule_slot_utc": schedule_slot,
        "source_watermark_sha256": source_watermark_sha,
    }


def formula_registry(
    *,
    stage: Any = None,
    direction: Any = None,
    horizon_minutes: Any = None,
    limit: int = 20,
) -> Dict[str, Any]:
    normalized_stage = str(stage or "").strip().upper() or None
    if normalized_stage and normalized_stage not in _STAGE_ORDER:
        raise ValueError("invalid formula stage")
    normalized_direction = str(direction or "").strip().upper() or None
    if normalized_direction not in {None, "LONG", "SHORT"}:
        raise ValueError("direction must be LONG, SHORT or null")
    horizon = int(horizon_minutes) if horizon_minutes is not None else None
    if horizon not in {None, 60, 240, 720, 1440}:
        raise ValueError("invalid horizon_minutes")
    row_limit = max(1, min(int(limit), 100))
    status = schema_status()
    if not status.get("schema_present"):
        return {"available": False, "schema": status, "formulas": []}
    clauses = []
    params: list[Any] = []
    if normalized_stage:
        clauses.append("AND f.current_stage=%s")
        params.append(normalized_stage)
    if normalized_direction:
        clauses.append("AND f.direction=%s")
        params.append(normalized_direction)
    if horizon:
        clauses.append("AND f.horizon_minutes=%s")
        params.append(horizon)
    params.append(row_limit)
    with _connect(read_only=True) as conn:
        rows = conn.execute(
            f"""
            SELECT f.formula_id, f.formula_key, f.formula_version,
                   f.formula_schema_version, f.engine_version,
                   f.direction, f.horizon_minutes, f.conditions,
                   f.formula_text, f.current_stage, f.active,
                   f.live_alert_approved, f.shadow_started_at_utc,
                   f.shadow_validation_metrics,
                   f.created_at_utc, f.updated_at_utc,
                   e.ranking_score, e.discovery_metrics,
                   e.holdout_metrics, e.multiple_testing, e.gate_notes,
                   r.coverage AS dataset_coverage,
                   (SELECT COUNT(*) FROM research_formula_shadow_checks c
                    WHERE c.formula_id=f.formula_id) AS shadow_checks,
                   (SELECT COUNT(*) FROM research_formula_shadow_hits h
                    WHERE h.formula_id=f.formula_id) AS shadow_hits
            FROM research_formulas f
            LEFT JOIN research_formula_evaluations e
              ON e.run_id=f.latest_evaluation_run_id AND e.formula_id=f.formula_id
            LEFT JOIN research_formula_runs r
              ON r.run_id=f.latest_evaluation_run_id
            WHERE f.active=TRUE
              {' '.join(clauses)}
            ORDER BY
              CASE f.current_stage
                WHEN 'LIVE' THEN 6 WHEN 'APPROVED' THEN 5 WHEN 'SHADOW' THEN 4
                WHEN 'HOLDOUT_PASSED' THEN 3 WHEN 'BACKTESTED' THEN 2 ELSE 1 END DESC,
              e.ranking_score DESC NULLS LAST, f.updated_at_utc DESC
            LIMIT %s
            """,
            params,
        ).fetchall()
    return _json_safe(
        {
            "available": True,
            "automatic_stage_ceiling": "SHADOW_PENDING_EXPLICIT_APPROVAL",
            "live_alerts_require": [
                "a separate explicit owner approval record",
                "strict chronological holdout plus a frozen future Shadow review",
                "FORMULA_LIVE_ALERTS_ENABLED=1",
                "an explicit Telegram chat subscription",
            ],
            "filters": {
                "stage": normalized_stage,
                "direction": normalized_direction,
                "horizon_minutes": horizon,
            },
            "feature_semantics": {
                "aligned_log": (
                    "signed log10(1 + abs(raw aligned value)); inverse is "
                    "sign(value) * (10^abs(value) - 1)"
                ),
                "session": (
                    "ACTIVE Sunday 18:00 ET through Friday 20:00 ET; "
                    "WEEKEND otherwise; exact America/New_York composition"
                ),
                "weekend_width_calibration": (
                    "may reduce only the absolute movement-width floor; "
                    "probability and adverse-excursion gates are unchanged"
                ),
            },
            "formulas": [_formula_registry_row(row) for row in rows],
        }
    )


def shadow_status(limit: int = 20) -> Dict[str, Any]:
    status = schema_status()
    if not status.get("schema_present"):
        return {"available": False, "schema": status}
    row_limit = max(1, min(int(limit), 100))
    with _connect(read_only=True) as conn:
        stages = conn.execute(
            """
            SELECT current_stage, COUNT(*)::bigint AS count
            FROM research_formulas WHERE active=TRUE
            GROUP BY current_stage ORDER BY current_stage
            """
        ).fetchall()
        recent = conn.execute(
            """
            SELECT h.shadow_hit_id, h.formula_id, h.event_id,
                   h.matched_at_utc, h.delivery_status,
                   f.direction, f.horizon_minutes, f.formula_text,
                   e.symbol, e.event_type
            FROM research_formula_shadow_hits h
            JOIN research_formulas f ON f.formula_id=h.formula_id
            JOIN research_events e ON e.event_id=h.event_id
            ORDER BY h.matched_at_utc DESC, h.shadow_hit_id DESC
            LIMIT %s
            """,
            (row_limit,),
        ).fetchall()
        maturity_rows = conn.execute(
            """
            SELECT formula_id, formula_key, formula_version,
                   formula_schema_version, engine_version,
                   direction, horizon_minutes, current_stage,
                   shadow_validation_metrics
            FROM research_formulas
            WHERE active=TRUE AND current_stage='SHADOW'
            ORDER BY updated_at_utc DESC, formula_id DESC
            LIMIT %s
            """,
            (row_limit,),
        ).fetchall()
        delivery_counts = conn.execute(
            """
            SELECT status, COUNT(*)::bigint AS count
            FROM research_formula_live_deliveries
            GROUP BY status ORDER BY status
            """
        ).fetchall()
        subscriptions = conn.execute(
            """
            SELECT COUNT(*)::bigint AS count
            FROM research_formula_alert_subscriptions WHERE active=TRUE
            """
        ).fetchone()
    return _json_safe(
        {
            "available": True,
            "stages": {row["current_stage"]: int(row["count"] or 0) for row in stages},
            "recent_shadow_hits": [dict(row) for row in recent],
            "research_maturity": [
                _formula_registry_row(row) for row in maturity_rows
            ],
            "live_delivery": {
                "active_subscriptions": int(subscriptions["count"] or 0),
                "by_status": {
                    row["status"]: int(row["count"] or 0)
                    for row in delivery_counts
                },
            },
        }
    )


def formula_lab_comparison(
    *,
    direction: Any = "LONG",
    horizon_minutes: Any = 720,
    max_formulas_per_cohort: int = 20,
    max_anchors: int = 250,
    max_snapshots: int = 20,
) -> Dict[str, Any]:
    """Run a bounded, read-only V7.2/V6.2 comparison on identical anchors."""

    normalized_direction = str(direction or "").strip().upper()
    if normalized_direction not in {"LONG", "SHORT"}:
        raise ValueError("direction must be LONG or SHORT")
    horizon = int(horizon_minutes)
    if horizon not in {60, 240, 720, 1440}:
        raise ValueError("invalid horizon_minutes")
    formula_limit = max(1, min(int(max_formulas_per_cohort), 25))
    anchor_limit = max(1, min(int(max_anchors), 250))
    snapshot_limit = max(1, min(int(max_snapshots), 50))
    status = schema_status()
    if not status.get("schema_present"):
        return {
            "available": False,
            "status": "WAITING_DATA",
            "blockers": ["Formula Lab schema is unavailable"],
            "schema": status,
            "live_effect": "NONE",
        }

    with _connect(read_only=True) as conn:
        current_formulas = conn.execute(
            """
            SELECT f.formula_id, f.formula_key, f.formula_version,
                   f.formula_schema_version, f.engine_version,
                   f.feature_schema_version, f.outcome_method_version,
                   f.direction, f.horizon_minutes, f.conditions,
                   f.formula_text, f.current_stage,
                   e.ranking_score, e.holdout_metrics, e.multiple_testing
            FROM research_formulas f
            LEFT JOIN research_formula_evaluations e
              ON e.run_id=f.latest_evaluation_run_id
             AND e.formula_id=f.formula_id
            WHERE f.active=TRUE
              AND f.current_stage IN (
                    'DISCOVERED', 'BACKTESTED', 'HOLDOUT_PASSED',
                    'SHADOW', 'LIVE'
                  )
              AND f.formula_schema_version=%s
              AND f.engine_version=%s
              AND f.feature_schema_version=%s
              AND f.outcome_method_version=%s
              AND f.direction=%s
              AND f.horizon_minutes=%s
            ORDER BY
              CASE f.current_stage
                WHEN 'LIVE' THEN 5 WHEN 'SHADOW' THEN 4
                WHEN 'HOLDOUT_PASSED' THEN 3
                WHEN 'BACKTESTED' THEN 2 ELSE 1
              END DESC,
              e.ranking_score DESC NULLS LAST,
              f.formula_id
            LIMIT %s
            """,
            (
                research_formula_engine.FORMULA_SCHEMA_VERSION,
                research_formula_engine.ENGINE_VERSION,
                research_feature_matrix.FEATURE_SCHEMA_VERSION,
                research_feature_matrix.VERIFIED_OUTCOME_METHOD,
                normalized_direction,
                horizon,
                formula_limit,
            ),
        ).fetchall()
        legacy_formulas = conn.execute(
            """
            SELECT f.formula_id, f.formula_key, f.formula_version,
                   f.formula_schema_version, f.engine_version,
                   f.feature_schema_version, f.outcome_method_version,
                   f.direction, f.horizon_minutes, f.conditions,
                   f.formula_text, f.current_stage,
                   e.ranking_score, e.holdout_metrics, e.multiple_testing
            FROM research_formulas f
            LEFT JOIN research_formula_evaluations e
              ON e.run_id=f.latest_evaluation_run_id
             AND e.formula_id=f.formula_id
            WHERE f.active=TRUE
              AND f.current_stage='SHADOW'
              AND f.formula_schema_version=%s
              AND f.engine_version=%s
              AND f.feature_schema_version=%s
              AND f.outcome_method_version=%s
              AND f.direction=%s
              AND f.horizon_minutes=%s
            ORDER BY e.ranking_score DESC NULLS LAST, f.formula_id
            LIMIT %s
            """,
            (
                research_formula_engine.LEGACY_V6_FORMULA_SCHEMA_VERSION,
                research_formula_engine.LEGACY_V6_ENGINE_VERSION,
                research_feature_matrix.FEATURE_SCHEMA_VERSION,
                research_feature_matrix.VERIFIED_OUTCOME_METHOD,
                normalized_direction,
                horizon,
                formula_limit,
            ),
        ).fetchall()
        events = conn.execute(
            """
            SELECT * FROM (
                SELECT candidate.event_id, candidate.alert_time_utc,
                       candidate.symbol, candidate.direction,
                       candidate.event_type, candidate.setup_key,
                       candidate.source_side, candidate.timeframe,
                       candidate.strategy_version, candidate.code_version,
                       candidate.current_price,
                       candidate.anchor_slot_id,
                       candidate.input_fingerprint,
                       candidate.feature_bundle_policy_version,
                       candidate.feature_bundle_sha256,
                       candidate.source_timestamps,
                       candidate.source_provenance
                FROM research_prospective_shadow_events candidate
                WHERE candidate.direction=%s
                  AND candidate.sampler_version=%s
                  AND candidate.feature_bundle_policy_version=%s
                ORDER BY candidate.alert_time_utc DESC,
                         candidate.event_id DESC
                LIMIT %s
            ) recent
            ORDER BY recent.alert_time_utc, recent.event_id
            """,
            (
                normalized_direction,
                _PROSPECTIVE_ANCHOR_SAMPLER_VERSION,
                _FEATURE_BUNDLE_POLICY_VERSION,
                anchor_limit,
            ),
        ).fetchall()
        snapshots = conn.execute(
            """
            SELECT snapshot.snapshot_payload
            FROM research_formula_evidence_snapshots snapshot
            JOIN research_formulas formula
              ON formula.formula_id=snapshot.formula_id
            WHERE formula.direction=%s
              AND formula.horizon_minutes=%s
              AND (
                    (
                      formula.formula_schema_version=%s
                      AND formula.engine_version=%s
                    )
                    OR
                    (
                      formula.formula_schema_version=%s
                      AND formula.engine_version=%s
                    )
                  )
            ORDER BY snapshot.assessed_at_utc DESC, snapshot.snapshot_id
            LIMIT %s
            """,
            (
                normalized_direction,
                horizon,
                research_formula_engine.FORMULA_SCHEMA_VERSION,
                research_formula_engine.ENGINE_VERSION,
                research_formula_engine.LEGACY_V6_FORMULA_SCHEMA_VERSION,
                research_formula_engine.LEGACY_V6_ENGINE_VERSION,
                snapshot_limit,
            ),
        ).fetchall()
        hype_attempts = conn.execute(
            """
            SELECT evaluation_status, COUNT(*)::bigint AS count,
                   MAX(checked_at_utc) AS latest_checked_at_utc
            FROM research_prospective_anchor_attempts
            WHERE symbol='HYPE' AND sampler_version=%s
            GROUP BY evaluation_status
            ORDER BY evaluation_status
            """,
            (_PROSPECTIVE_ANCHOR_SAMPLER_VERSION,),
        ).fetchall()
        latest_hype_attempt = conn.execute(
            """
            SELECT evaluation_status, evaluation_reason, missing_sources,
                   checked_at_utc, source_provenance
            FROM research_prospective_anchor_attempts
            WHERE symbol='HYPE' AND sampler_version=%s
            ORDER BY checked_at_utc DESC, attempt_id DESC
            LIMIT 1
            """,
            (_PROSPECTIVE_ANCHOR_SAMPLER_VERSION,),
        ).fetchone()
        hype_slots = conn.execute(
            """
            SELECT COUNT(*)::bigint AS count
            FROM research_prospective_anchor_slots
            WHERE symbol='HYPE' AND sampler_version=%s
            """,
            (_PROSPECTIVE_ANCHOR_SAMPLER_VERSION,),
        ).fetchone()

    event_ids = [int(event["event_id"]) for event in events]
    verified_by_key = research_feature_matrix.load_shadow_feature_rows_by_horizon(
        {horizon: event_ids}
    )
    event_by_id = {int(event["event_id"]): dict(event) for event in events}
    anchor_rows = []
    for event_id in event_ids:
        verified = verified_by_key.get((event_id, horizon))
        if verified is None:
            continue
        row = dict(verified)
        event = dict(row.get("event") or {})
        source_event = event_by_id[event_id]
        event.update(
            {
                "current_price": source_event.get("current_price"),
                "prospective_anchor_slot_id": source_event.get("anchor_slot_id"),
            }
        )
        row["event"] = event
        anchor_rows.append(row)

    hype_status = {
        "symbol": "HYPE",
        "separate_from_other_symbols": True,
        "blocks_other_symbols": False,
        "anchor_slot_count": int((hype_slots or {}).get("count") or 0),
        "attempt_counts": {
            str(item["evaluation_status"]): int(item["count"] or 0)
            for item in hype_attempts
        },
        "latest_attempt": dict(latest_hype_attempt) if latest_hype_attempt else None,
    }
    result = research_formula_lab_comparison.compare_same_anchors(
        current_formulas=[dict(row) for row in current_formulas],
        legacy_formulas=[dict(row) for row in legacy_formulas],
        anchor_rows=anchor_rows,
        evidence_snapshots=[row["snapshot_payload"] for row in snapshots],
        hype_status=hype_status,
        direction=normalized_direction,
        horizon_minutes=horizon,
    )
    result["available"] = True
    result["data_watermark"] = {
        "requested_anchor_limit": anchor_limit,
        "loaded_event_count": len(events),
        "authoritative_verified_anchor_count": len(anchor_rows),
        "latest_anchor_time_utc": (
            max(event["alert_time_utc"] for event in events) if events else None
        ),
        "verified_snapshot_count": len(snapshots),
        "formula_limit_per_cohort": formula_limit,
    }
    return _json_safe(result)


def load_shadow_work(max_events_per_formula: int = 100) -> list[Dict[str, Any]]:
    """Load only authoritative sampler-v4 prospective decision samples.

    Delivered alerts and older sampler rows are intentionally absent.  The
    authorization view is the database boundary that binds each event to its
    immutable slot and hashed decision feature bundle.
    """
    limit = max(1, min(int(max_events_per_formula), 250))
    work: list[Dict[str, Any]] = []
    with _connect(read_only=True) as conn:
        formulas = conn.execute(
            """
            SELECT f.formula_id, f.formula_key, f.formula_version, f.formula_text,
                   f.formula_schema_version, f.engine_version,
                   f.direction, f.horizon_minutes,
                   f.conditions, f.feature_schema_version,
                   f.outcome_method_version,
                   f.last_shadow_event_id, f.shadow_started_at_utc,
                   f.current_stage, f.live_alert_approved,
                   e.ranking_score, e.holdout_metrics
            FROM research_formulas f
            LEFT JOIN research_formula_evaluations e
              ON e.run_id=f.latest_evaluation_run_id AND e.formula_id=f.formula_id
            WHERE f.active=TRUE
              AND f.current_stage IN ('SHADOW', 'LIVE')
              AND (
                (
                  f.current_stage='SHADOW'
                  AND (
                    (
                      f.formula_schema_version=%s
                      AND f.engine_version=%s
                      AND f.feature_schema_version=%s
                      AND f.outcome_method_version=%s
                    )
                    OR (
                      f.formula_schema_version=%s
                      AND f.engine_version=%s
                      AND f.feature_schema_version=%s
                      AND f.outcome_method_version=%s
                    )
                  )
                )
                OR (
                  f.current_stage='LIVE'
                  AND f.formula_schema_version=%s
                  AND f.engine_version=%s
                  AND f.feature_schema_version=%s
                  AND f.outcome_method_version=%s
                )
              )
            ORDER BY
              CASE WHEN f.current_stage='LIVE' THEN 1 ELSE 0 END DESC,
              e.ranking_score DESC NULLS LAST,
              f.formula_id
            """,
            (
                research_formula_engine.LEGACY_V6_FORMULA_SCHEMA_VERSION,
                research_formula_engine.LEGACY_V6_ENGINE_VERSION,
                research_feature_matrix.FEATURE_SCHEMA_VERSION,
                research_feature_matrix.VERIFIED_OUTCOME_METHOD,
                research_formula_engine.FORMULA_SCHEMA_VERSION,
                research_formula_engine.ENGINE_VERSION,
                research_feature_matrix.FEATURE_SCHEMA_VERSION,
                research_feature_matrix.VERIFIED_OUTCOME_METHOD,
                research_formula_engine.FORMULA_SCHEMA_VERSION,
                research_formula_engine.ENGINE_VERSION,
                research_feature_matrix.FEATURE_SCHEMA_VERSION,
                research_feature_matrix.VERIFIED_OUTCOME_METHOD,
            ),
        ).fetchall()
        for formula in formulas:
            events = conn.execute(
                """
                SELECT candidate.event_id, candidate.alert_time_utc,
                       candidate.symbol, candidate.direction,
                       candidate.event_type, candidate.setup_key,
                       candidate.source_side, candidate.timeframe,
                       candidate.strategy_version, candidate.code_version,
                       'DECISION_SAMPLE'::text AS event_kind,
                       'NOT_APPLICABLE'::text AS delivery_status,
                       candidate.anchor_slot_id AS prospective_anchor_slot_id,
                       candidate.input_fingerprint
                         AS prospective_input_fingerprint,
                       candidate.feature_bundle_policy_version,
                       candidate.feature_bundle_sha256
                FROM research_prospective_shadow_events candidate
                WHERE candidate.direction=%s
                  AND candidate.event_id>%s
                  AND candidate.alert_time_utc>=COALESCE(
                        %s, '-infinity'::timestamptz
                      )
                  AND candidate.sampler_version=%s
                  AND candidate.feature_bundle_policy_version=%s
                ORDER BY event_id ASC
                LIMIT %s
                """,
                (
                    formula["direction"],
                    int(formula["last_shadow_event_id"] or 0),
                    formula["shadow_started_at_utc"],
                    _PROSPECTIVE_ANCHOR_SAMPLER_VERSION,
                    _FEATURE_BUNDLE_POLICY_VERSION,
                    limit,
                ),
            ).fetchall()
            if events:
                work.append(
                    {
                        **dict(formula),
                        "events": [dict(event) for event in events],
                    }
                )
    return work


def record_shadow_results(
    *,
    formula: Mapping[str, Any],
    results: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    if not results:
        return {"checked": 0, "matched": 0, "queued": 0, "new_hit_event_ids": []}
    formula_id = int(formula["formula_id"])
    checked = 0
    matched = 0
    queued = 0
    new_hit_event_ids: list[int] = []
    max_event_id = 0
    authoritative_rows: Dict[tuple[int, int], Dict[str, Any]] = {}
    authoritative_error: Optional[str] = None
    requested_ids = sorted({int(result["event_id"]) for result in results})
    try:
        authoritative_rows = (
            research_feature_matrix.load_shadow_feature_rows_by_horizon(
                {int(formula.get("horizon_minutes") or 0): requested_ids}
            )
        )
    except Exception as exc:
        # A missing configuration or malformed frozen bundle is a fail-closed
        # UNEVALUABLE check, never permission to trust submitted values.
        authoritative_error = f"{type(exc).__name__}: {exc}"[:800]
    with _connect(read_only=False) as conn:
        current = conn.execute(
            """
            SELECT formula_id, formula_key, formula_version,
                   formula_schema_version, engine_version,
                   feature_schema_version, outcome_method_version,
                   direction, horizon_minutes, conditions,
                   current_stage, active, shadow_started_at_utc,
                   last_shadow_event_id, live_alert_approved,
                   live_alert_approved_by
            FROM research_formulas WHERE formula_id=%s FOR UPDATE
            """,
            (formula_id,),
        ).fetchone()
        if (
            not current
            or current["current_stage"] not in {"SHADOW", "LIVE"}
            or not current["active"]
        ):
            return {
                "checked": 0,
                "matched": 0,
                "queued": 0,
                "new_hit_event_ids": [],
            }
        requested_event_ids = sorted(
            {int(result["event_id"]) for result in results}
        )
        event_rows = conn.execute(
            """
            SELECT authorized.event_id, authorized.alert_time_utc,
                   authorized.symbol, authorized.direction,
                   authorized.event_type, authorized.setup_key,
                   authorized.source_side, authorized.timeframe,
                   authorized.strategy_version, authorized.code_version,
                   'DECISION_SAMPLE'::text AS event_kind,
                   'NOT_APPLICABLE'::text AS delivery_status,
                   TRUE AS shadow_eligible,
                   slot.anchor_slot_id, slot.sampler_version,
                   slot.coverage_policy_version, slot.coverage_snapshot,
                   slot.source_candle_open_utc, slot.source_candle_close_utc,
                   slot.base_eligible_at_utc, slot.expires_at_utc,
                   slot.decision_time_utc, slot.input_fingerprint,
                   slot.source_timestamps, slot.source_provenance,
                   slot.frozen_inputs, slot.feature_bundle_policy_version,
                   slot.feature_bundle_sha256, slot.decision_feature_bundle,
                   slot.created_at_utc
            FROM research_prospective_shadow_events authorized
            JOIN research_prospective_anchor_slots slot
              ON slot.anchor_slot_id=authorized.anchor_slot_id
            WHERE authorized.event_id=ANY(%s)
              AND authorized.sampler_version=%s
              AND authorized.feature_bundle_policy_version=%s
            """,
            (
                requested_event_ids,
                _PROSPECTIVE_ANCHOR_SAMPLER_VERSION,
                _FEATURE_BUNDLE_POLICY_VERSION,
            ),
        ).fetchall()
        events_by_id = {int(row["event_id"]): dict(row) for row in event_rows}
        # The authorization view is deliberately fail-closed.  If a row that
        # was handed to the worker stops satisfying that view before the
        # write, retain an immutable rejected audit check instead of silently
        # losing the attempted observation.  The fallback never authorizes an
        # ALERT (or any other event kind), and it carries no slot evidence;
        # therefore the strict verifier below can only make it UNEVALUABLE.
        missing_authorized_ids = sorted(
            set(requested_event_ids).difference(events_by_id)
        )
        if missing_authorized_ids:
            rejected_event_rows = conn.execute(
                """
                SELECT event_id, alert_time_utc, symbol, direction,
                       event_type, setup_key, source_side, timeframe,
                       strategy_version, code_version, event_kind,
                       delivery_status, FALSE AS shadow_eligible
                FROM research_events
                WHERE event_id=ANY(%s)
                  AND event_kind='DECISION_SAMPLE'
                """,
                (missing_authorized_ids,),
            ).fetchall()
            events_by_id.update(
                {
                    int(row["event_id"]): dict(row)
                    for row in rejected_event_rows
                }
            )
        authoritative_formula = dict(current)
        for result in results:
            event_id = int(result["event_id"])
            event = events_by_id.get(event_id)
            if not event:
                continue
            if str(event.get("direction") or "").upper() != str(
                authoritative_formula.get("direction") or ""
            ).upper():
                continue
            if event_id <= int(authoritative_formula.get("last_shadow_event_id") or 0):
                continue
            try:
                if _as_utc(event.get("alert_time_utc")) < _as_utc(
                    authoritative_formula.get("shadow_started_at_utc")
                ):
                    continue
            except (TypeError, ValueError, OverflowError):
                continue
            event_kind = str(event.get("event_kind") or "")
            event_delivery_status = str(event.get("delivery_status") or "")
            max_event_id = max(max_event_id, event_id)
            evaluation_status = str(
                result.get("evaluation_status")
                or ("MATCHED" if result.get("matched") else "UNMATCHED")
            ).upper()
            if evaluation_status not in {"MATCHED", "UNMATCHED", "UNEVALUABLE"}:
                evaluation_status = "UNEVALUABLE"
            evaluation_reason = str(result.get("evaluation_reason") or "")
            submitted_matched = result.get("matched")
            bound_row = {
                **event,
                "input_snapshot": result.get("input_snapshot"),
                "condition_results": result.get("condition_results"),
                "decision_cohort_key": result.get("decision_cohort_key"),
                "decision_anchor_time_utc": result.get(
                    "decision_anchor_time_utc"
                ),
                "evaluation_status": evaluation_status,
                "matched": submitted_matched,
            }
            compatible, reason = _max_pain_shadow_check_contract(
                authoritative_formula, bound_row
            )
            strict_flag = type(submitted_matched) is bool and submitted_matched == (
                evaluation_status == "MATCHED"
            )
            if not strict_flag:
                compatible = False
                reason = "submitted matched flag differs from evaluation status"
            if compatible:
                if authoritative_error:
                    compatible = False
                    reason = (
                        "authoritative frozen-bundle load failed: "
                        + authoritative_error
                    )
                else:
                    compatible, reason = _authoritative_frozen_row_contract(
                        authoritative_formula,
                        event,
                        result,
                        authoritative_rows.get(
                            (
                                event_id,
                                int(authoritative_formula["horizon_minutes"]),
                            )
                        ),
                        event,
                    )
            if not compatible:
                evaluation_status = "UNEVALUABLE"
                evaluation_reason = (
                    "frozen Shadow write contract rejected: " + reason
                )[:1000]
            evidence_policy_version = (
                _PROSPECTIVE_EVIDENCE_POLICY_VERSION
                if compatible
                else _REJECTED_PROSPECTIVE_EVIDENCE_POLICY_VERSION
            )
            evidence_slot_id = _strict_int(event.get("anchor_slot_id"))
            evidence_input_fingerprint = str(
                event.get("input_fingerprint") or ""
            ).strip()
            evidence_bundle_sha256 = str(
                event.get("feature_bundle_sha256") or ""
            ).strip()
            if evidence_slot_id is None or evidence_slot_id <= 0:
                evidence_slot_id = None
            if len(evidence_input_fingerprint) != 64 or any(
                character not in "0123456789abcdef"
                for character in evidence_input_fingerprint
            ):
                evidence_input_fingerprint = None
            if len(evidence_bundle_sha256) != 64 or any(
                character not in "0123456789abcdef"
                for character in evidence_bundle_sha256
            ):
                evidence_bundle_sha256 = None
            is_match = evaluation_status == "MATCHED"
            inserted = conn.execute(
                """
                INSERT INTO research_formula_shadow_checks (
                    formula_id, event_id, matched, feature_schema_version,
                    evaluation_status, evaluation_reason, input_snapshot,
                    condition_results, decision_cohort_key,
                    decision_anchor_time_utc, evidence_policy_version,
                    prospective_anchor_slot_id,
                    prospective_input_fingerprint, feature_bundle_sha256,
                    authoritative_verified
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s,
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT (formula_id, event_id) DO NOTHING
                RETURNING formula_id
                """,
                (
                    formula_id,
                    event_id,
                    is_match,
                    authoritative_formula["feature_schema_version"],
                    evaluation_status,
                    evaluation_reason[:1000] or None,
                    _json(result.get("input_snapshot") or {}),
                    _json(result.get("condition_results") or []),
                    result.get("decision_cohort_key"),
                    result.get("decision_anchor_time_utc"),
                    evidence_policy_version,
                    evidence_slot_id,
                    evidence_input_fingerprint,
                    evidence_bundle_sha256,
                    bool(compatible),
                ),
            ).fetchone()
            if not inserted:
                continue
            checked += 1
            if is_match:
                matched += 1
                new_hit_event_ids.append(event_id)
                conn.execute(
                    """
                    INSERT INTO research_formula_shadow_hits (
                        formula_id, event_id, matched_at_utc,
                        input_snapshot, delivery_status
                    ) VALUES (%s, %s, %s, %s::jsonb, 'NOT_SENT')
                    ON CONFLICT (formula_id, event_id) DO NOTHING
                    """,
                    (
                        formula_id,
                        event_id,
                        event.get("alert_time_utc"),
                        _json(result.get("input_snapshot") or {}),
                    ),
                )
                if (
                    _LIVE_ALERTS_ENABLED
                    and event_kind == "ALERT"
                    and event_delivery_status == "DELIVERED"
                    and current["current_stage"] == "LIVE"
                    and bool(current["live_alert_approved"])
                    and bool(str(current.get("live_alert_approved_by") or "").strip())
                    and _current_live_formula_contract(current)
                ):
                    inserted_deliveries = conn.execute(
                        """
                        INSERT INTO research_formula_live_deliveries (
                            formula_id, event_id, chat_id, status
                        )
                        SELECT %s, %s, s.chat_id, 'PENDING'
                        FROM research_formula_alert_subscriptions s
                        WHERE s.active=TRUE
                          AND EXISTS (
                              SELECT 1
                              FROM research_formula_live_approvals a
                              WHERE a.formula_id=%s
                                AND a.formula_version=%s
                                AND a.horizon_minutes=%s
                                AND a.formula_schema_version=%s
                                AND a.engine_version=%s
                                AND a.feature_schema_version=%s
                                AND a.outcome_method_version=%s
                                AND a.approval_operation_version=%s
                                AND a.delivery_environment_enabled=FALSE
                                AND a.review_kind='FROZEN_PROSPECTIVE'
                                AND a.thresholds_met=TRUE
                                AND a.validated_future_matches>=%s
                                AND a.validated_future_controls>=%s
                                AND a.validated_span_hours>=%s
                                AND a.validated_utc_dates>=%s
                          )
                        ON CONFLICT (event_id, chat_id) DO NOTHING
                        RETURNING delivery_id
                        """,
                        (
                            formula_id,
                            event_id,
                            formula_id,
                            int(current["formula_version"]),
                            int(authoritative_formula["horizon_minutes"]),
                            str(authoritative_formula["formula_schema_version"]),
                            str(authoritative_formula["engine_version"]),
                            str(authoritative_formula["feature_schema_version"]),
                            str(authoritative_formula["outcome_method_version"]),
                            _LIVE_APPROVAL_OPERATION_VERSION,
                            _SHADOW_MIN_MATCHES,
                            _SHADOW_MIN_CONTROLS,
                            float(_SHADOW_MIN_SPAN_HOURS),
                            _SHADOW_MIN_DATES,
                        ),
                    ).fetchall()
                    queued += len(inserted_deliveries)
        if max_event_id:
            conn.execute(
                """
                UPDATE research_formulas
                SET last_shadow_event_id=GREATEST(last_shadow_event_id, %s),
                    updated_at_utc=NOW()
                WHERE formula_id=%s
                """,
                (max_event_id, formula_id),
            )
        conn.commit()
    return {
        "checked": checked,
        "matched": matched,
        "queued": queued,
        "new_hit_event_ids": new_hit_event_ids,
    }


def _shadow_outcome_rows(conn, formula: Mapping[str, Any]) -> list[Dict[str, Any]]:
    horizon_minutes = int(formula["horizon_minutes"])
    rows = conn.execute(
        """
        SELECT c.matched, c.evaluation_status, c.evaluation_reason,
               c.evidence_policy_version, c.authoritative_verified,
               c.input_snapshot, c.condition_results,
               c.decision_cohort_key, c.decision_anchor_time_utc,
               e.event_id, e.alert_time_utc, e.symbol, e.event_type,
               e.direction, e.setup_key, e.current_price,
               (e.alert_time_utc + (%s * INTERVAL '1 minute') <= NOW()) AS outcome_due,
               (ft.event_id IS NOT NULL) AS first_touch_available,
               (ft.status='HIT') AS first_touch_hit,
               (o.event_id IS NOT NULL) AS full_horizon_outcome_available,
               (ft.event_id IS NOT NULL AND o.event_id IS NOT NULL)
                 AS outcome_available,
               o.directional_return_pct, ft.success AS path_success,
               ft.status AS first_touch_status,
               o.mfe_pct, ft.pre_qualifying_mae_pct AS mae_pct,
               o.mae_pct AS full_horizon_mae_pct,
               ft.time_to_first_qualifying_move_seconds
                 AS time_to_first_progress_seconds,
               ft.time_to_first_qualifying_move_seconds,
               ft.qualifying_move_threshold_pct,
               ft.threshold_scale_factor AS first_touch_threshold_scale_factor,
               ft.threshold_source_kind AS first_touch_threshold_source_kind,
               ft.threshold_policy AS first_touch_threshold_policy,
               ft.qualifying_candle_order_ambiguous,
               o.time_to_mfe_seconds,
               o.target_progress_ratio, o.target_reached
        FROM research_formula_shadow_checks c
        JOIN research_events e ON e.event_id=c.event_id
        JOIN research_prospective_shadow_events authorized
          ON authorized.event_id=c.event_id
         AND authorized.anchor_slot_id=c.prospective_anchor_slot_id
         AND authorized.input_fingerprint
               IS NOT DISTINCT FROM c.prospective_input_fingerprint
         AND authorized.feature_bundle_sha256
               IS NOT DISTINCT FROM c.feature_bundle_sha256
        LEFT JOIN research_first_touch_outcomes ft
          ON ft.event_id=e.event_id
         AND ft.horizon_minutes=%s
         AND ft.method_version=%s
         AND ft.status IN ('HIT', 'MISS')
         AND ft.data_quality_status=ANY(%s)
        LEFT JOIN research_alert_outcomes o
          ON o.event_id=e.event_id
         AND o.horizon_minutes=%s
         AND o.outcome_method_version=%s
         AND o.data_quality_status=ANY(%s)
        WHERE c.formula_id=%s
          AND authorized.sampler_version=%s
          AND authorized.feature_bundle_policy_version=%s
        ORDER BY e.alert_time_utc, e.event_id
        """,
        (
            horizon_minutes,
            horizon_minutes,
            research_feature_matrix.VERIFIED_OUTCOME_METHOD,
            list(research_feature_matrix.VERIFIED_OUTCOME_QUALITIES),
            horizon_minutes,
            canonical_price_path.METHOD_VERSION,
            list(research_feature_matrix.VERIFIED_OUTCOME_QUALITIES),
            int(formula["formula_id"]),
            _PROSPECTIVE_ANCHOR_SAMPLER_VERSION,
            _FEATURE_BUNDLE_POLICY_VERSION,
        ),
    ).fetchall()
    result = [dict(row) for row in rows]
    for row in result:
        authoritative_membership = bool(
            row.get("authoritative_verified") is True
            and row.get("evidence_policy_version")
            == _PROSPECTIVE_EVIDENCE_POLICY_VERSION
        )
        if not authoritative_membership:
            row["matched"] = False
            row["evaluation_status"] = "UNEVALUABLE"
            row["evaluation_reason"] = (
                "membership censor: Shadow check failed the frozen evidence contract"
            )
            row["first_touch_available"] = False
            row["first_touch_hit"] = False
            row["outcome_available"] = False
        compatible, reason = _terminal_threshold_matches_snapshot(
            row,
            horizon_minutes=horizon_minutes,
            # The evidence contract belongs to the sampler-v4 observation,
            # not to the historical formula schema.  Retained v5 formulas use
            # the same frozen threshold reference as v6 formulas.
            require_v6_contract=True,
        )
        row["first_touch_threshold_policy_compatible"] = compatible
        row["first_touch_threshold_policy_reason"] = reason
        if (
            authoritative_membership
            and bool(row.get("first_touch_available"))
            and not compatible
        ):
            # Preserve the terminal row in PostgreSQL for audit, but never let
            # a label computed with a different width enter prospective gates.
            row["first_touch_available"] = False
            row["first_touch_hit"] = False
            row["outcome_available"] = False
        provenance_compatible, provenance_reason = _max_pain_shadow_check_contract(
            formula, row
        )
        row["max_pain_provenance_compatible"] = provenance_compatible
        row["max_pain_provenance_reason"] = provenance_reason
        if not provenance_compatible:
            # Keep the immutable check and all outcome rows auditable, but make
            # the proof UNEVALUABLE before independence selection so a malformed
            # Max-Pain check cannot overlap-exclude a later valid observation.
            row["matched"] = False
            row["evaluation_status"] = "UNEVALUABLE"
            row["evaluation_reason"] = (
                "Max-Pain provenance rejected: " + provenance_reason
            )[:1000]
    return result


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


def _formula_conditions(formula: Mapping[str, Any]) -> list[Dict[str, Any]]:
    value = formula.get("conditions")
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = []
    return [dict(item) for item in value or [] if isinstance(item, Mapping)]


_SOURCE_TIMESTAMP_FIELDS = {
    "price_oi": ("timestamp_utc", "price_timestamp_utc", "oi_timestamp_utc"),
    "futures_cvd": ("timestamp_utc",),
    "spot_cvd": ("timestamp_utc",),
}


def _source_timestamp_items(
    source_inputs: Mapping[str, Any],
) -> list[tuple[str, str, Any]]:
    """Return every canonical or additional source-availability timestamp."""
    output: list[tuple[str, str, Any]] = []
    for family, required_names in _SOURCE_TIMESTAMP_FIELDS.items():
        values = source_inputs.get(family)
        mapping = values if isinstance(values, Mapping) else {}
        names = set(required_names)
        names.update(
            str(key)
            for key in mapping
            if str(key) == "timestamp_utc" or str(key).endswith("_timestamp_utc")
        )
        for name in sorted(names):
            output.append((family, name, mapping.get(name)))
    return output


def _decision_cohort_identity(
    *,
    formula: Mapping[str, Any],
    event: Mapping[str, Any],
    snapshot: Mapping[str, Any],
) -> tuple[str, datetime]:
    """Compute the versioned identity used by both writer and verifier."""
    source_inputs = snapshot.get("source_inputs")
    if not isinstance(source_inputs, Mapping):
        source_inputs = {}
    timestamps: list[datetime] = []
    canonical_timestamps: list[str] = []
    for family, name, value in _source_timestamp_items(source_inputs):
        label = f"{family}.{name}"
        if value in (None, ""):
            canonical_timestamps.append(f"{label}:missing")
            continue
        timestamp = _as_utc(value)
        timestamps.append(timestamp)
        canonical_timestamps.append(f"{label}:{timestamp.isoformat()}")
    for family in ("price_oi", "futures_cvd", "spot_cvd"):
        values = source_inputs.get(family)
        mapping = values if isinstance(values, Mapping) else {}
        canonical_timestamps.extend(
            (
                f"{family}.prospective_anchor_slot_id:"
                + str(mapping.get("prospective_anchor_slot_id") or "missing"),
                f"{family}.prospective_input_fingerprint:"
                + str(mapping.get("prospective_input_fingerprint") or "missing"),
                f"{family}.prospective_slot_created_at_utc:"
                + str(mapping.get("prospective_slot_created_at_utc") or "missing"),
            )
        )
    prospective = snapshot.get("prospective_evidence")
    if not isinstance(prospective, Mapping):
        prospective = {}
    canonical_timestamps.extend(
        (
            "prospective.sampler_version:"
            + str(prospective.get("sampler_version") or "missing"),
            "prospective.feature_bundle_policy_version:"
            + str(
                prospective.get("feature_bundle_policy_version") or "missing"
            ),
            "prospective.anchor_slot_id:"
            + str(prospective.get("anchor_slot_id") or "missing"),
            "prospective.input_fingerprint:"
            + str(prospective.get("input_fingerprint") or "missing"),
            "prospective.feature_bundle_sha256:"
            + str(prospective.get("feature_bundle_sha256") or "missing"),
            "prospective.source_timestamps:"
            + _canonical_json(prospective.get("source_timestamps") or {}),
            "prospective.source_provenance:"
            + _canonical_json(prospective.get("source_provenance") or {}),
        )
    )
    max_pain = snapshot.get("max_pain_provenance")
    if isinstance(max_pain, Mapping):
        provenance = max_pain.get("provenance")
        current = (
            provenance.get("current")
            if isinstance(provenance, Mapping)
            else None
        )
        available = (
            current.get("available_at_utc")
            if isinstance(current, Mapping)
            else None
        )
        if available in (None, ""):
            canonical_timestamps.append("max_pain:missing")
        else:
            try:
                max_pain_timestamp = _as_utc(available)
            except (TypeError, ValueError, OverflowError):
                # The writer still has to persist a fail-closed UNEVALUABLE
                # check when malformed Max-Pain evidence is encountered.  Its
                # contract is rejected before readiness selection; this stable
                # marker prevents the bad timestamp from aborting the entire
                # Shadow cycle while ensuring it cannot masquerade as a valid
                # availability anchor.
                canonical_timestamps.append("max_pain:invalid")
            else:
                timestamps.append(max_pain_timestamp)
                canonical_timestamps.append(
                    f"max_pain:{max_pain_timestamp.isoformat()}"
                )
        canonical_timestamps.append(
            "max_pain_provenance_sha256:"
            + str(max_pain.get("provenance_sha256") or "missing")
        )
    alert_time = _as_utc(event["alert_time_utc"])
    if timestamps:
        anchor = max(timestamps)
    else:
        anchor = alert_time.replace(
            minute=(alert_time.minute // 30) * 30,
            second=0,
            microsecond=0,
        )
        canonical_timestamps.append(f"fallback_30m:{anchor.isoformat()}")
    payload = "|".join(
        [
            _DECISION_COHORT_POLICY_VERSION,
            str(formula.get("formula_id") or ""),
            str(event.get("symbol") or "").upper(),
            str(event.get("direction") or "").upper(),
            str(int(formula.get("horizon_minutes") or 0)),
            *canonical_timestamps,
            json.dumps(
                snapshot.get("formula_key_features") or {},
                ensure_ascii=False,
                sort_keys=True,
                default=str,
                separators=(",", ":"),
                allow_nan=False,
            ),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest(), anchor


def _v6_max_pain_condition_features(
    formula: Mapping[str, Any],
) -> tuple[str, ...]:
    """Return Max-Pain inputs for every supported Shadow formula schema."""
    if str(formula.get("formula_schema_version") or "") not in (
        _SHADOW_COMPATIBLE_FORMULA_SCHEMAS
    ):
        return ()
    return tuple(
        sorted(
            {
                str(condition.get("feature") or "")
                for condition in _formula_conditions(formula)
                if str(condition.get("feature") or "").startswith("max_pain.")
            }
        )
    )


def _v6_snapshot_base_contract(
    formula: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    *,
    decision_time_utc: Any,
    expected_symbol: Any,
) -> tuple[bool, str]:
    """Bind every v6 check to its complete frozen decision-time inputs."""
    if not _shadow_executable_formula_contract(formula):
        return False, "v6 formula execution contract versions are incompatible"
    frozen_versions = {
        "formula_schema_version": formula.get("formula_schema_version"),
        "engine_version": formula.get("engine_version"),
        "feature_schema_version": research_feature_matrix.FEATURE_SCHEMA_VERSION,
        "outcome_method_version": research_feature_matrix.VERIFIED_OUTCOME_METHOD,
    }
    if any(snapshot.get(key) != expected for key, expected in frozen_versions.items()):
        return False, "frozen v6 formula contract versions are incompatible"
    if snapshot.get("snapshot_policy_version") != _SHADOW_INPUT_SNAPSHOT_POLICY_VERSION:
        return False, "Shadow input snapshot policy version is incompatible"
    if snapshot.get("decision_cohort_policy_version") != (
        _DECISION_COHORT_POLICY_VERSION
    ):
        return False, "Shadow decision cohort policy version is incompatible"
    if snapshot.get("decision_input_policy_version") != (
        _PROSPECTIVE_EVIDENCE_POLICY_VERSION
    ):
        return False, "v6 decision inputs are not from frozen prospective slots"
    if snapshot.get("evidence_policy_version") != (
        _PROSPECTIVE_EVIDENCE_POLICY_VERSION
    ):
        return False, "prospective Shadow evidence policy version is incompatible"
    formula_id = _strict_int(formula.get("formula_id"))
    frozen_formula_id = _strict_int(snapshot.get("formula_id"))
    if (
        formula_id is None
        or formula_id <= 0
        or frozen_formula_id != formula_id
    ):
        return False, "frozen v6 formula_id does not match the formula"
    prospective = _as_mapping(snapshot.get("prospective_evidence"))
    slot_id = _strict_int(prospective.get("anchor_slot_id"))
    input_fingerprint = str(prospective.get("input_fingerprint") or "")
    bundle_sha256 = str(prospective.get("feature_bundle_sha256") or "")
    if prospective.get("sampler_version") != _PROSPECTIVE_ANCHOR_SAMPLER_VERSION:
        return False, "frozen prospective sampler version is incompatible"
    if prospective.get("feature_bundle_policy_version") != (
        _FEATURE_BUNDLE_POLICY_VERSION
    ):
        return False, "frozen decision feature bundle policy is incompatible"
    if slot_id is None or slot_id <= 0:
        return False, "frozen prospective anchor slot id is malformed"
    for value, label in (
        (input_fingerprint, "input fingerprint"),
        (bundle_sha256, "feature bundle hash"),
    ):
        if len(value) != 64 or any(
            character not in "0123456789abcdef" for character in value
        ):
            return False, f"frozen prospective {label} is malformed"
    if not isinstance(prospective.get("source_timestamps"), Mapping):
        return False, "frozen prospective source timestamps are missing"
    if not isinstance(prospective.get("source_provenance"), Mapping):
        return False, "frozen prospective source provenance is missing"

    expected_conditions = [
        {
            "feature": str(item.get("feature") or ""),
            "operator": str(item.get("operator") or ""),
            "value": item.get("value"),
        }
        for item in _formula_conditions(formula)
    ]
    recorded_conditions = [
        {
            "feature": str(item.get("feature") or ""),
            "operator": str(item.get("operator") or ""),
            "value": item.get("value"),
        }
        for item in _formula_conditions(
            {"conditions": snapshot.get("conditions")}
        )
    ]
    if not expected_conditions or not _type_strict_json_equal(
        recorded_conditions, expected_conditions
    ):
        return False, "frozen v6 conditions do not match the formula"
    if any(
        item["feature"].startswith(("sequence.", "aligned_sequence."))
        or item["feature"].startswith("model.snapshot.prospective_anchor.")
        for item in expected_conditions
    ):
        return False, "v6 formula uses a source family not frozen prospectively"

    features = snapshot.get("formula_key_features")
    if not isinstance(features, Mapping):
        return False, "frozen v6 formula feature values are missing"
    expected_feature_names = {item["feature"] for item in expected_conditions}
    if set(str(key) for key in features) != expected_feature_names:
        return False, "frozen v6 formula feature keys do not match conditions"

    results = snapshot.get("condition_results")
    if not isinstance(results, Sequence) or isinstance(results, (str, bytes)):
        return False, "frozen v6 condition results are missing"
    if len(results) != len(expected_conditions):
        return False, "frozen v6 condition result count differs from formula"
    for condition, result in zip(expected_conditions, results):
        if not isinstance(result, Mapping):
            return False, "frozen v6 condition result is malformed"
        feature = condition["feature"]
        feature_available = features.get(feature) is not None
        expected_passed = feature_available and (
            research_formula_engine.condition_matches(features, condition)
        )
        if (
            str(result.get("feature") or "") != feature
            or str(result.get("operator") or "") != condition["operator"]
            or not _type_strict_json_equal(
                result.get("expected"), condition["value"]
            )
            or not _type_strict_json_equal(
                result.get("actual"), features.get(feature)
            )
            or not isinstance(result.get("available"), bool)
            or not isinstance(result.get("passed"), bool)
            or result.get("available") is not feature_available
            or result.get("passed") is not expected_passed
        ):
            return False, "frozen v6 condition result differs from inputs"

    source_inputs = snapshot.get("source_inputs")
    if not isinstance(source_inputs, Mapping):
        return False, "frozen v6 source inputs are missing"
    decision_time = _as_utc(decision_time_utc)
    slot_identities: set[tuple[int, str]] = set()
    for family in ("price_oi", "futures_cvd", "spot_cvd"):
        values = source_inputs.get(family)
        if not isinstance(values, Mapping):
            return False, "frozen v6 prospective source family is missing"
        slot_id = values.get("prospective_anchor_slot_id")
        fingerprint = str(values.get("prospective_input_fingerprint") or "")
        if (
            isinstance(slot_id, bool)
            or not isinstance(slot_id, int)
            or slot_id <= 0
            or len(fingerprint) != 64
            or any(character not in "0123456789abcdef" for character in fingerprint)
        ):
            return False, "frozen v6 prospective slot identity is malformed"
        try:
            created_at = _as_utc(values.get("prospective_slot_created_at_utc"))
        except (TypeError, ValueError, OverflowError):
            return False, "frozen v6 prospective slot creation time is malformed"
        if created_at > decision_time + timedelta(minutes=5):
            return False, "frozen v6 prospective slot was inserted after its decision"
        slot_identities.add((slot_id, fingerprint))
    if len(slot_identities) != 1:
        return False, "frozen v6 source families do not share one prospective slot"
    if slot_identities != {(slot_id, input_fingerprint)}:
        return False, "frozen v6 source families differ from prospective evidence"

    price_input = _as_mapping(source_inputs.get("price_oi"))
    try:
        route = canonical_price_path.validated_route(
            expected_symbol,
            {
                "exchange": price_input.get("price_exchange"),
                "market": price_input.get("price_market"),
                "pair": price_input.get("price_pair"),
                "interval": price_input.get("price_timeframe"),
                "interval_seconds": price_input.get("price_interval_seconds"),
                "api_coin": price_input.get("price_instrument_id"),
                "complete": True,
                "provenance": "FROZEN_PROSPECTIVE_SLOT",
            },
        )
    except (TypeError, ValueError, OverflowError) as exc:
        return False, f"frozen v6 price route is not canonical: {exc}"
    if (
        price_input.get("canonical_price_method_version")
        != canonical_price_path.METHOD_VERSION
        or price_input.get("canonical_price_provenance_version")
        != canonical_price_path.PRICE_PROVENANCE_VERSION
    ):
        return False, "frozen v6 canonical price versions are incompatible"
    recorded_route = _as_mapping(price_input.get("canonical_price_provenance"))
    for key in (
        "provenance_version",
        "method_version",
        "symbol",
        "exchange",
        "market",
        "pair",
        "instrument",
        "interval",
        "interval_seconds",
    ):
        if recorded_route.get(key) != route.get(key):
            return False, "frozen v6 canonical price provenance is inconsistent"
    expected_price_source = (
        "hyperliquid_spot_@107"
        if str(expected_symbol or "").upper() == "HYPE"
        else "binance_spot"
    )
    if (
        str(price_input.get("price_source") or "").lower()
        != expected_price_source
        or str(price_input.get("source") or "").lower()
        != expected_price_source
    ):
        return False, "frozen v6 price source conflicts with the canonical route"
    for family, expected_source in (
        ("futures_cvd", "coinglass_futures_aggregated_cvd"),
        ("spot_cvd", "coinglass_spot_aggregated_cvd"),
    ):
        flow_input = _as_mapping(source_inputs.get(family))
        if str(flow_input.get("source") or "").lower() != expected_source:
            return False, f"frozen v6 {family} source provenance is incompatible"
        exchanges = {
            part.strip().upper()
            for part in str(flow_input.get("exchange_list") or "").split(",")
            if part.strip()
        }
        if exchanges != {"BINANCE", "OKX", "BYBIT"}:
            return False, f"frozen v6 {family} exchange set is incompatible"
    source_timestamps: Dict[str, Dict[str, datetime]] = {}
    try:
        for family in ("price_oi", "futures_cvd", "spot_cvd"):
            values = source_inputs.get(family)
            if values is None:
                continue
            if not isinstance(values, Mapping):
                return False, "frozen v6 source input family is malformed"
        for family, name, timestamp in _source_timestamp_items(source_inputs):
            if timestamp in (None, ""):
                continue
            parsed = _as_utc(timestamp)
            if parsed > decision_time:
                return False, "frozen v6 source input is newer than decision time"
            source_timestamps.setdefault(family, {})[name] = parsed
    except (TypeError, ValueError, OverflowError):
        return False, "frozen v6 source input timestamp is malformed"

    def required_timestamps(feature: str) -> Dict[str, set[str]]:
        name = feature.lower()
        required: Dict[str, set[str]] = {}

        def add(family: str, *fields: str) -> None:
            required.setdefault(family, set()).update(fields or ("timestamp_utc",))

        if "spot_to_futures_abs_cvd_ratio" in name or "spot_futures_alignment" in name:
            add("spot_cvd", "timestamp_utc")
            add("futures_cvd", "timestamp_utc")
        if "price_spot_alignment" in name:
            add("price_oi", "timestamp_utc", "price_timestamp_utc")
            add("spot_cvd", "timestamp_utc")
        if "price_futures_alignment" in name:
            add("price_oi", "timestamp_utc", "price_timestamp_utc")
            add("futures_cvd", "timestamp_utc")
        if "futures_continuous_cvd" in name or "futures_api_cvd" in name:
            add("futures_cvd", "timestamp_utc")
        if "spot_continuous_cvd" in name or "spot_api_cvd" in name:
            add("spot_cvd", "timestamp_utc")
        if "price_oi_state" in name:
            add(
                "price_oi",
                "timestamp_utc",
                "price_timestamp_utc",
                "oi_timestamp_utc",
            )
        else:
            if "price_change" in name:
                add("price_oi", "timestamp_utc", "price_timestamp_utc")
            if "oi_change" in name or "open_interest" in name:
                add("price_oi", "oi_timestamp_utc")
        if name.startswith("latest.price_oi."):
            add(
                "price_oi",
                "timestamp_utc",
                "price_timestamp_utc",
                "oi_timestamp_utc",
            )
        if name.startswith("latest.futures_cvd."):
            add("futures_cvd", "timestamp_utc")
        if name.startswith("latest.spot_cvd."):
            add("spot_cvd", "timestamp_utc")
        return required

    for condition in expected_conditions:
        missing: list[str] = []
        for family, names in required_timestamps(condition["feature"]).items():
            present_names = set(source_timestamps.get(family, {}))
            missing.extend(
                f"{family}.{name}" for name in sorted(names - present_names)
            )
        if missing:
            return False, "frozen v6 condition lacks required source timestamp: " + ",".join(missing)

    width_reference = _as_mapping(snapshot.get("movement_width_reference"))
    compatible, reason = research_session_width.validate_movement_width_reference(
        width_reference,
        expected_symbol=expected_symbol,
        event_time=decision_time_utc,
        horizon_minutes=int(formula.get("horizon_minutes") or 0),
    )
    if not compatible:
        return False, reason
    return True, "complete v6 decision-time input snapshot is bound"


def _max_pain_previous_required(features: Sequence[str]) -> bool:
    return any(
        feature.startswith("max_pain.delta.")
        or feature.endswith("_trend")
        or ".trend" in feature
        for feature in features
    )


def _canonical_max_pain_snapshot_evidence(
    formula: Mapping[str, Any], row: Optional[Mapping[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Build the one canonical evidence wrapper used by writer and verifier."""
    condition_features = _v6_max_pain_condition_features(formula)
    if not condition_features:
        return None
    wrapper = row.get("max_pain_features") if isinstance(row, Mapping) else {}
    if not isinstance(wrapper, Mapping):
        wrapper = {}
    requires_previous = _max_pain_previous_required(condition_features)
    wrapper_features = _as_mapping(wrapper.get("features"))
    provenance = (
        dict(wrapper.get("provenance"))
        if isinstance(wrapper.get("provenance"), Mapping)
        else {}
    )
    if provenance and not requires_previous:
        provenance.update(
            {
                "previous": None,
                "used_for_delta": False,
                "previous_gap_minutes": None,
            }
        )
    provenance_sha256 = (
        research_max_pain_archive.canonical_provenance_sha256(provenance)
        if provenance
        else wrapper.get("provenance_sha256")
    )
    return {
        "condition_features": list(condition_features),
        "condition_values": {
            feature: wrapper_features.get(feature) for feature in condition_features
        },
        "requires_previous": requires_previous,
        "evaluation_status": str(
            wrapper.get("evaluation_status") or "UNEVALUABLE"
        ).upper(),
        "evaluation_reason": wrapper.get("reason"),
        "change_evaluation_status": str(
            wrapper.get("change_evaluation_status") or "UNEVALUABLE"
        ).upper(),
        "change_reason": wrapper.get("change_reason"),
        "provenance": provenance,
        "provenance_sha256": provenance_sha256,
    }


def _legacy_v5_snapshot_base_contract(
    formula: Mapping[str, Any], snapshot: Mapping[str, Any]
) -> tuple[bool, str]:
    """Validate current frozen evidence without rewriting v5 identities."""
    if not _shadow_executable_formula_contract(formula) or str(
        formula.get("formula_schema_version") or ""
    ) != _LEGACY_V5_FORMULA_SCHEMA_VERSION:
        return False, "legacy formula runtime tuple is not the supported v5 adapter"
    if snapshot.get("legacy_v5_shadow_adapter_version") != (
        _LEGACY_V5_SHADOW_ADAPTER_VERSION
    ):
        return False, "legacy frozen feature adapter version is incompatible"
    for key, expected in (
        ("snapshot_policy_version", _SHADOW_INPUT_SNAPSHOT_POLICY_VERSION),
        ("decision_cohort_policy_version", _DECISION_COHORT_POLICY_VERSION),
        ("decision_input_policy_version", _PROSPECTIVE_EVIDENCE_POLICY_VERSION),
        ("evidence_policy_version", _PROSPECTIVE_EVIDENCE_POLICY_VERSION),
    ):
        if snapshot.get(key) != expected:
            return False, f"legacy frozen {key} is incompatible"
    for key in (
        "formula_key",
        "formula_schema_version",
        "engine_version",
        "feature_schema_version",
        "outcome_method_version",
    ):
        if snapshot.get(key) != formula.get(key):
            return False, f"legacy frozen {key} differs from the formula"
    for key in ("formula_id", "formula_version", "horizon_minutes"):
        actual = _strict_int(snapshot.get(key))
        expected = _strict_int(formula.get(key))
        if actual is None or actual <= 0 or actual != expected:
            return False, f"legacy frozen {key} differs from the formula"
    conditions = _formula_conditions(formula)
    if not conditions or not _type_strict_json_equal(
        snapshot.get("conditions"), conditions
    ):
        return False, "legacy frozen conditions differ from the formula"
    features = _as_mapping(snapshot.get("formula_key_features"))
    if set(features) != {
        str(condition.get("feature") or "") for condition in conditions
    }:
        return False, "legacy frozen formula feature keys differ from conditions"
    direct = research_formula_engine.evaluate_frozen_feature_values(
        features,
        direction=formula.get("direction"),
        event_direction=formula.get("direction"),
        conditions=conditions,
    )
    if not _type_strict_json_equal(
        snapshot.get("condition_results"), direct.get("condition_results")
    ):
        return False, "legacy frozen condition results differ from feature values"
    prospective = _as_mapping(snapshot.get("prospective_evidence"))
    if (
        prospective.get("sampler_version")
        != _PROSPECTIVE_ANCHOR_SAMPLER_VERSION
        or prospective.get("feature_bundle_policy_version")
        != _FEATURE_BUNDLE_POLICY_VERSION
    ):
        return False, "legacy formula is not bound to sampler-v4 evidence"
    slot_id = _strict_int(prospective.get("anchor_slot_id"))
    if slot_id is None or slot_id <= 0:
        return False, "legacy frozen anchor slot id is malformed"
    for key in ("input_fingerprint", "feature_bundle_sha256"):
        value = str(prospective.get(key) or "")
        if len(value) != 64 or any(
            character not in "0123456789abcdef" for character in value
        ):
            return False, f"legacy frozen {key} is malformed"
    if not isinstance(prospective.get("source_timestamps"), Mapping) or not isinstance(
        prospective.get("source_provenance"), Mapping
    ):
        return False, "legacy frozen source evidence is missing"
    return (
        True,
        "legacy v5 formula is bound to current frozen evidence through "
        + _LEGACY_V5_SHADOW_ADAPTER_VERSION,
    )


def _max_pain_snapshot_contract(
    formula: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    *,
    decision_time_utc: Any,
    symbol: Any,
) -> tuple[bool, str]:
    features = _v6_max_pain_condition_features(formula)
    formula_schema = str(formula.get("formula_schema_version") or "")
    if formula_schema in {
        str(research_formula_engine.LEGACY_V6_FORMULA_SCHEMA_VERSION),
        str(research_formula_engine.FORMULA_SCHEMA_VERSION),
    }:
        if str(formula.get("feature_schema_version") or "") != str(
            research_feature_matrix.FEATURE_SCHEMA_VERSION
        ):
            return False, "v6 Max-Pain formula feature schema is incompatible"
        compatible, reason = _v6_snapshot_base_contract(
            formula,
            snapshot,
            decision_time_utc=decision_time_utc,
            expected_symbol=symbol,
        )
    elif formula_schema == "research-formula-v5-safe-replay":
        compatible, reason = _legacy_v5_snapshot_base_contract(
            formula, snapshot
        )
    else:
        return True, "non-executable diagnostic formula has no Shadow contract"
    if not compatible:
        return False, reason
    if not features:
        return True, "complete v6 snapshot contract; Max-Pain is not required"
    evidence = _as_mapping(snapshot.get("max_pain_provenance"))
    if not evidence:
        return False, "Max-Pain provenance evidence is missing"
    raw_recorded_features = evidence.get("condition_features")
    if not isinstance(raw_recorded_features, Sequence) or isinstance(
        raw_recorded_features, (str, bytes)
    ):
        return False, "Max-Pain provenance condition feature list is invalid"
    recorded_features = tuple(sorted(str(value) for value in raw_recorded_features))
    if recorded_features != features:
        return False, "Max-Pain provenance condition features do not match the formula"
    recorded_values = _as_mapping(evidence.get("condition_values"))
    frozen_formula_values = _as_mapping(snapshot.get("formula_key_features"))
    if set(str(key) for key in recorded_values) != set(features):
        return False, "Max-Pain frozen condition values are missing"
    if any(
        not _type_strict_json_equal(
            recorded_values.get(feature), frozen_formula_values.get(feature)
        )
        for feature in features
    ):
        return False, "Max-Pain formula values differ from frozen Max-Pain evidence"
    if str(evidence.get("evaluation_status") or "").upper() != "EVALUABLE":
        return False, str(evidence.get("evaluation_reason") or "current snapshot is unevaluable")
    require_previous = _max_pain_previous_required(features)
    if require_previous and str(
        evidence.get("change_evaluation_status") or ""
    ).upper() != "EVALUABLE":
        return False, str(
            evidence.get("change_reason")
            or "delta/trend condition has no eligible earlier snapshot"
        )
    if bool(evidence.get("requires_previous")) != require_previous:
        return False, "Max-Pain previous-snapshot requirement is inconsistent"
    return research_max_pain_archive.validate_shadow_provenance(
        evidence.get("provenance"),
        evidence.get("provenance_sha256"),
        decision_time_utc=decision_time_utc,
        expected_symbol=symbol,
        require_previous=require_previous,
    )


def _max_pain_shadow_check_contract(
    formula: Mapping[str, Any], row: Mapping[str, Any]
) -> tuple[bool, str]:
    features = _v6_max_pain_condition_features(formula)
    if str(formula.get("formula_schema_version") or "") not in (
        _SHADOW_COMPATIBLE_FORMULA_SCHEMAS
    ):
        return True, "non-executable diagnostic formula has no Shadow contract"
    snapshot = _as_mapping(row.get("input_snapshot"))
    compatible, reason = _max_pain_snapshot_contract(
        formula,
        snapshot,
        decision_time_utc=row.get("alert_time_utc"),
        symbol=row.get("symbol"),
    )
    if not compatible:
        return False, reason

    frozen_event = _as_mapping(snapshot.get("event"))
    frozen_event_id = _strict_int(frozen_event.get("event_id"))
    row_event_id = _strict_int(row.get("event_id"))
    if (
        frozen_event_id is None
        or frozen_event_id <= 0
        or row_event_id is None
        or row_event_id <= 0
    ):
        return False, "frozen Shadow event_id is invalid"
    if frozen_event_id != row_event_id:
        return False, "frozen Shadow event_id does not match the check"
    if str(frozen_event.get("symbol") or "").upper() != str(
        row.get("symbol") or ""
    ).upper():
        return False, "frozen Shadow symbol does not match the check"
    if str(frozen_event.get("direction") or "").upper() != str(
        row.get("direction") or ""
    ).upper():
        return False, "frozen Shadow direction does not match the check"
    formula_direction = str(formula.get("direction") or "").upper()
    if formula_direction not in {"LONG", "SHORT"} or formula_direction != str(
        row.get("direction") or ""
    ).upper():
        return False, "Shadow event direction does not match the formula"
    if str(frozen_event.get("event_type") or "") != str(
        row.get("event_type") or ""
    ):
        return False, "frozen Shadow event_type does not match the check"
    if str(frozen_event.get("setup_key") or "") != str(
        row.get("setup_key") or ""
    ):
        return False, "frozen Shadow setup_key does not match the check"
    try:
        if _as_utc(frozen_event.get("alert_time_utc")) != _as_utc(
            row.get("alert_time_utc")
        ):
            return False, "frozen Shadow decision time does not match the check"
    except (TypeError, ValueError):
        return False, "frozen Shadow decision time is invalid"
    if str(snapshot.get("formula_key") or "") != str(
        formula.get("formula_key") or ""
    ):
        return False, "frozen formula_key does not match the Shadow formula"
    frozen_formula_version = _strict_int(snapshot.get("formula_version"))
    formula_version = _strict_int(formula.get("formula_version"))
    frozen_horizon = _strict_int(snapshot.get("horizon_minutes"))
    formula_horizon = _strict_int(formula.get("horizon_minutes"))
    if any(
        value is None or value <= 0
        for value in (
            frozen_formula_version,
            formula_version,
            frozen_horizon,
            formula_horizon,
        )
    ):
        return False, "frozen Formula identity contains invalid numerics"
    if frozen_formula_version != formula_version:
        return False, "frozen formula_version does not match the Shadow formula"
    if frozen_horizon != formula_horizon:
        return False, "frozen horizon does not match the Shadow formula"
    if str(snapshot.get("feature_schema_version") or "") != str(
        formula.get("feature_schema_version") or ""
    ):
        return False, "frozen feature schema does not match the Shadow formula"
    if not _type_strict_json_equal(
        row.get("condition_results") or [],
        snapshot.get("condition_results") or [],
    ):
        return False, "stored condition results differ from frozen snapshot"
    evaluation_status = str(row.get("evaluation_status") or "").upper()
    if evaluation_status in {"MATCHED", "UNMATCHED"}:
        condition_results = snapshot.get("condition_results") or []
        if not all(item.get("available") is True for item in condition_results):
            return False, "evaluable check has unavailable frozen conditions"
        expected_match = all(item.get("passed") is True for item in condition_results)
        if expected_match != (evaluation_status == "MATCHED"):
            return False, "evaluation status differs from frozen condition results"
        if bool(row.get("matched")) != expected_match:
            return False, "matched flag differs from frozen condition results"
    event = {
        "event_id": row.get("event_id"),
        "alert_time_utc": row.get("alert_time_utc"),
        "symbol": row.get("symbol"),
        "direction": row.get("direction"),
    }
    try:
        expected_key, expected_anchor = _decision_cohort_identity(
            formula=formula,
            event=event,
            snapshot=snapshot,
        )
        recorded_anchor = _as_utc(row.get("decision_anchor_time_utc"))
        decision_time = _as_utc(row.get("alert_time_utc"))
    except (TypeError, ValueError, OverflowError) as exc:
        return False, f"Shadow decision cohort evidence is invalid: {exc}"
    if recorded_anchor != expected_anchor:
        return False, "decision_anchor_time_utc does not match frozen inputs"
    if expected_anchor > decision_time:
        return False, "decision anchor contains an input unavailable at decision time"
    if str(row.get("decision_cohort_key") or "") != expected_key:
        return False, "decision_cohort_key does not match frozen inputs"
    if features:
        return True, "complete Max-Pain provenance and decision cohort are bound"
    return True, "complete frozen snapshot and decision cohort are bound"


def _authoritative_frozen_row_contract(
    formula: Mapping[str, Any],
    event: Mapping[str, Any],
    result: Mapping[str, Any],
    row: Optional[Mapping[str, Any]],
    authorized: Optional[Mapping[str, Any]] = None,
) -> tuple[bool, str]:
    """Verify a Shadow write using only its immutable sampler-v4 bundle.

    This applies equally to active legacy-v5 and current-v6 Shadow formulas.
    The only computation repeated here is each stored formula operator against
    the flat values already frozen at decision time.  No archive feature
    extraction or later reconstruction is permitted at this write boundary.
    """
    if not isinstance(row, Mapping):
        return False, "authoritative frozen feature row is unavailable"
    if not isinstance(authorized, Mapping):
        return False, "authoritative sampler-v4 database row is unavailable"
    if row.get("decision_input_policy_version") != _PROSPECTIVE_EVIDENCE_POLICY_VERSION:
        return False, "authoritative row uses an incompatible evidence policy"

    snapshot = _as_mapping(result.get("input_snapshot"))
    if snapshot.get("snapshot_policy_version") != _SHADOW_INPUT_SNAPSHOT_POLICY_VERSION:
        return False, "submitted Shadow snapshot policy is incompatible"
    if snapshot.get("decision_cohort_policy_version") != _DECISION_COHORT_POLICY_VERSION:
        return False, "submitted decision cohort policy is incompatible"
    if snapshot.get("decision_input_policy_version") != _PROSPECTIVE_EVIDENCE_POLICY_VERSION:
        return False, "submitted decision input policy is incompatible"
    if snapshot.get("evidence_policy_version") != _PROSPECTIVE_EVIDENCE_POLICY_VERSION:
        return False, "submitted evidence policy is incompatible"

    formula_identity = {
        "formula_id": _strict_int(formula.get("formula_id")),
        "formula_key": str(formula.get("formula_key") or ""),
        "formula_version": _strict_int(formula.get("formula_version")),
        "formula_schema_version": str(formula.get("formula_schema_version") or ""),
        "engine_version": formula.get("engine_version"),
        "feature_schema_version": str(formula.get("feature_schema_version") or ""),
        "outcome_method_version": str(formula.get("outcome_method_version") or ""),
        "horizon_minutes": _strict_int(formula.get("horizon_minutes")),
    }
    if (
        formula_identity["formula_id"] is None
        or formula_identity["formula_id"] <= 0
        or formula_identity["formula_version"] is None
        or formula_identity["formula_version"] <= 0
        or formula_identity["horizon_minutes"] not in {60, 240, 720, 1440}
    ):
        return False, "authoritative formula identity is malformed"
    for key, expected in formula_identity.items():
        actual = _strict_int(snapshot.get(key)) if key in {
            "formula_id", "formula_version", "horizon_minutes"
        } else snapshot.get(key)
        if actual != expected:
            return False, f"submitted {key} differs from the database formula"
    expected_conditions = _formula_conditions(formula)
    if not expected_conditions or not _type_strict_json_equal(
        snapshot.get("conditions"), expected_conditions
    ):
        return False, "submitted conditions differ from the database formula"

    row_event = _as_mapping(row.get("event"))
    try:
        row_event_id = _strict_int(row_event.get("event_id"))
        event_id = _strict_int(event.get("event_id"))
        if (
            row_event_id is None
            or row_event_id <= 0
            or event_id is None
            or event_id <= 0
        ):
            return False, "authoritative row event_id is malformed"
        if row_event_id != event_id:
            return False, "authoritative row event_id does not match"
        if _as_utc(row_event.get("alert_time_utc")) != _as_utc(
            event.get("alert_time_utc")
        ):
            return False, "authoritative row decision time does not match"
    except (TypeError, ValueError, OverflowError):
        return False, "authoritative row event identity is malformed"
    if str(row_event.get("symbol") or "").upper() != str(
        event.get("symbol") or ""
    ).upper():
        return False, "authoritative row symbol does not match"
    if str(row_event.get("direction") or "").upper() != str(
        event.get("direction") or ""
    ).upper():
        return False, "authoritative row direction does not match"
    if str(row_event.get("event_type") or "") != str(
        event.get("event_type") or ""
    ):
        return False, "authoritative row event_type does not match"
    if str(row_event.get("setup_key") or "") != str(
        event.get("setup_key") or ""
    ):
        return False, "authoritative row setup_key does not match"
    for key in ("source_side", "timeframe", "strategy_version", "code_version"):
        if str(row_event.get(key) or "") != str(event.get(key) or ""):
            return False, f"authoritative row {key} does not match"

    frozen_event = _as_mapping(snapshot.get("event"))
    for key in (
        "event_id",
        "alert_time_utc",
        "symbol",
        "direction",
        "event_type",
        "setup_key",
        "source_side",
        "timeframe",
        "strategy_version",
        "code_version",
    ):
        expected = event.get(key)
        actual = frozen_event.get(key)
        if key == "event_id":
            actual, expected = _strict_int(actual), _strict_int(expected)
        elif key == "alert_time_utc":
            try:
                actual, expected = _as_utc(actual), _as_utc(expected)
            except (TypeError, ValueError, OverflowError):
                return False, "submitted frozen event time is malformed"
        elif key in {"symbol", "direction"}:
            actual, expected = str(actual or "").upper(), str(expected or "").upper()
        else:
            actual, expected = str(actual or ""), str(expected or "")
        if actual != expected:
            return False, f"submitted frozen event {key} differs from database"

    bundle = _as_mapping(authorized.get("decision_feature_bundle"))
    bundle_hash = str(authorized.get("feature_bundle_sha256") or "").strip()
    compatible, reason = research_prospective_feature_freeze.validate_feature_bundle(
        bundle,
        expected_sha256=bundle_hash,
        expected_symbol=event.get("symbol"),
        expected_decision_time_utc=event.get("alert_time_utc"),
    )
    if not compatible:
        return False, "authoritative decision feature bundle rejected: " + reason
    if row.get("feature_schema_version") != bundle.get("feature_schema_version"):
        return False, "loaded feature schema differs from authoritative bundle"
    if authorized.get("sampler_version") != _PROSPECTIVE_ANCHOR_SAMPLER_VERSION:
        return False, "authoritative sampler version is incompatible"
    if authorized.get("feature_bundle_policy_version") != _FEATURE_BUNDLE_POLICY_VERSION:
        return False, "authoritative feature bundle policy is incompatible"
    try:
        expected_input_fingerprint = research_prospective_anchors.compute_input_fingerprint(
            sampler_version=authorized.get("sampler_version"),
            coverage_policy_version=authorized.get("coverage_policy_version"),
            coverage_snapshot=_as_mapping(authorized.get("coverage_snapshot")),
            symbol=authorized.get("symbol"),
            source_candle_open_utc=authorized.get("source_candle_open_utc"),
            source_candle_close_utc=authorized.get("source_candle_close_utc"),
            base_eligible_at_utc=authorized.get("base_eligible_at_utc"),
            expires_at_utc=authorized.get("expires_at_utc"),
            evaluation_status=research_prospective_anchors.EVALUABLE,
            decision_time_utc=authorized.get("decision_time_utc"),
            source_timestamps=_as_mapping(authorized.get("source_timestamps")),
            source_provenance=_as_mapping(authorized.get("source_provenance")),
            frozen_inputs=_as_mapping(authorized.get("frozen_inputs")),
            feature_bundle_policy_version=authorized.get(
                "feature_bundle_policy_version"
            ),
            feature_bundle_sha256=bundle_hash,
        )
    except (TypeError, ValueError, OverflowError) as exc:
        return False, f"authoritative prospective input fingerprint is malformed: {exc}"
    recorded_input_fingerprint = str(
        authorized.get("input_fingerprint") or ""
    ).strip()
    if expected_input_fingerprint != recorded_input_fingerprint:
        return False, "authoritative prospective input fingerprint mismatch"

    prospective = _as_mapping(snapshot.get("prospective_evidence"))
    row_prospective = _as_mapping(row.get("prospective_evidence"))
    expected_prospective = {
        "sampler_version": _PROSPECTIVE_ANCHOR_SAMPLER_VERSION,
        "feature_bundle_policy_version": _FEATURE_BUNDLE_POLICY_VERSION,
        "anchor_slot_id": _strict_int(authorized.get("anchor_slot_id")),
        "input_fingerprint": recorded_input_fingerprint,
        "feature_bundle_sha256": bundle_hash,
        "source_timestamps": dict(_as_mapping(authorized.get("source_timestamps"))),
        "source_provenance": dict(_as_mapping(authorized.get("source_provenance"))),
    }
    if expected_prospective["anchor_slot_id"] is None or expected_prospective[
        "anchor_slot_id"
    ] <= 0:
        return False, "authoritative prospective anchor slot id is malformed"
    if not _type_strict_json_equal(row_prospective, expected_prospective):
        return False, "loaded prospective evidence differs from database slot"
    if not _type_strict_json_equal(prospective, expected_prospective):
        return False, "submitted prospective evidence differs from database slot"

    bundle_features_by_direction = _as_mapping(bundle.get("features_by_direction"))
    authoritative_features = _as_mapping(
        bundle_features_by_direction.get(str(formula.get("direction") or "").upper())
    )
    if str(authoritative_features.get("event.symbol") or "").upper() != str(
        event.get("symbol") or ""
    ).upper():
        return False, "authoritative bundle event symbol does not match"
    if str(authoritative_features.get("event.event_type") or "") != str(
        event.get("event_type") or ""
    ):
        return False, "authoritative bundle event type does not match"
    if not _type_strict_json_equal(
        row.get("frozen_decision_features"), authoritative_features
    ):
        return False, "loaded frozen feature values differ from authoritative bundle"
    evaluation = research_formula_engine.evaluate_frozen_feature_values(
        authoritative_features,
        direction=formula.get("direction"),
        event_direction=event.get("direction"),
        conditions=expected_conditions,
    )
    expected_features = evaluation.get("features")
    if not isinstance(expected_features, Mapping):
        expected_features = {}
    condition_names = {
        str(item.get("feature") or "") for item in expected_conditions
    }
    authoritative_formula_features = {
        name: expected_features.get(name) for name in condition_names
    }
    if not _type_strict_json_equal(
        snapshot.get("formula_key_features") or {},
        authoritative_formula_features,
    ):
        return False, "submitted formula feature values differ from authoritative bundle"
    if not _type_strict_json_equal(
        result.get("condition_results") or [],
        evaluation.get("condition_results") or [],
    ):
        return False, "submitted condition results differ from frozen operator evaluation"
    expected_status = str(evaluation.get("status") or "UNEVALUABLE").upper()
    submitted_status = str(result.get("evaluation_status") or "").upper()
    if submitted_status != expected_status:
        return False, "submitted evaluation status differs from frozen operator evaluation"
    if str(result.get("evaluation_reason") or "") != str(
        evaluation.get("reason") or ""
    ):
        return False, "submitted evaluation reason differs from frozen operator evaluation"
    if str(snapshot.get("evaluation_status") or "").upper() != expected_status:
        return False, "snapshot evaluation status differs from frozen operator evaluation"
    if str(snapshot.get("evaluation_reason") or "") != str(
        evaluation.get("reason") or ""
    ):
        return False, "snapshot evaluation reason differs from frozen operator evaluation"
    expected_matched = expected_status == "MATCHED"
    if (
        type(result.get("matched")) is not bool
        or result.get("matched") is not expected_matched
    ):
        return False, "submitted matched flag differs from frozen operator evaluation"

    raw = _as_mapping(row.get("raw_features"))
    latest = _as_mapping(raw.get("latest_at_or_before_alert"))
    authoritative_sources = {
        family: dict(values) if isinstance(values, Mapping) else {}
        for family, values in latest.items()
        if family in {"price_oi", "futures_cvd", "spot_cvd"}
    }
    if not _type_strict_json_equal(
        snapshot.get("source_inputs") or {}, authoritative_sources
    ):
        return False, "submitted source inputs differ from authoritative frozen slots"

    horizon = str(int(formula_identity["horizon_minutes"]))
    context = _as_mapping(_as_mapping(bundle.get("horizon_context")).get(horizon))
    context_session = _as_mapping(context.get("session"))
    expected_session = {
        "session_active_ratio": context_session.get("active_ratio"),
        "session_weekend_ratio": context_session.get("weekend_ratio"),
        "session_segments": context_session.get("segments"),
        "session_composition": context_session.get("composition"),
    }
    width_reference = _as_mapping(context.get("movement_width_reference"))
    outcome_label = _as_mapping(row.get("outcome_label"))
    loaded_session = {
        key: outcome_label.get(key) for key in expected_session
    }
    if not _type_strict_json_equal(loaded_session, expected_session):
        return False, "loaded horizon session differs from authoritative bundle"
    if not _type_strict_json_equal(
        outcome_label.get("movement_width_reference"), width_reference
    ):
        return False, "loaded movement-width reference differs from authoritative bundle"
    if not _type_strict_json_equal(
        snapshot.get("outcome_window_session") or {}, expected_session
    ):
        return False, "submitted horizon session differs from authoritative bundle"
    if not _type_strict_json_equal(
        snapshot.get("movement_width_reference") or {}, width_reference
    ):
        return False, "submitted movement-width reference differs from authoritative bundle"

    expected_max_pain = _canonical_max_pain_snapshot_evidence(formula, row)
    recorded_max_pain = snapshot.get("max_pain_provenance")
    if not _type_strict_json_equal(recorded_max_pain, expected_max_pain):
        return False, "submitted Max-Pain evidence differs from the frozen slot bundle"
    try:
        expected_cohort_key, expected_anchor = _decision_cohort_identity(
            formula=formula,
            event=event,
            snapshot=snapshot,
        )
        recorded_anchor = _as_utc(result.get("decision_anchor_time_utc"))
        decision_time = _as_utc(event.get("alert_time_utc"))
    except (TypeError, ValueError, OverflowError) as exc:
        return False, f"submitted decision cohort evidence is malformed: {exc}"
    if str(result.get("decision_cohort_key") or "") != expected_cohort_key:
        return False, "submitted decision cohort key differs from frozen evidence"
    if recorded_anchor != expected_anchor or expected_anchor > decision_time:
        return False, "submitted decision cohort anchor differs from frozen evidence"
    return True, "Shadow check matches the authoritative frozen decision bundle"


def _authoritative_v6_row_contract(
    formula: Mapping[str, Any],
    event: Mapping[str, Any],
    result: Mapping[str, Any],
    row: Optional[Mapping[str, Any]],
    authorized: Optional[Mapping[str, Any]] = None,
) -> tuple[bool, str]:
    """Compatibility alias for callers of the former v6-only verifier."""
    return _authoritative_frozen_row_contract(
        formula, event, result, row, authorized
    )


def _max_pain_validation_evidence(
    formula: Mapping[str, Any],
    *,
    source_rows: Sequence[Mapping[str, Any]],
    completed_independent_rows: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    features = _v6_max_pain_condition_features(formula)
    if not features:
        return {
            "required": False,
            "condition_features": [],
            "event_provenance_refs": [],
            "incompatible_event_ids": [],
            "max_pain_provenance_excluded_event_ids": [],
            "incompatible_reasons": {},
            "canonical_evidence_sha256": None,
        }

    incompatible: Dict[int, str] = {}
    for row in source_rows:
        compatible, reason = _max_pain_shadow_check_contract(
            formula, row
        )
        if not compatible:
            incompatible[int(row["event_id"])] = reason

    refs: list[Dict[str, Any]] = []
    completed_incompatible_event_ids: list[int] = []
    for row in completed_independent_rows:
        snapshot = _as_mapping(row.get("input_snapshot"))
        evidence = _as_mapping(snapshot.get("max_pain_provenance"))
        compatible, _ = _max_pain_shadow_check_contract(
            formula, row
        )
        if not compatible:
            completed_incompatible_event_ids.append(int(row["event_id"]))
        refs.append(
            {
                "event_id": int(row["event_id"]),
                "evaluation_status": str(
                    row.get("evaluation_status") or ""
                ).upper(),
                "decision_cohort_key": row.get("decision_cohort_key"),
                "max_pain_provenance_sha256": evidence.get(
                    "provenance_sha256"
                ),
            }
        )
    refs.sort(key=lambda item: (item["event_id"], item["evaluation_status"]))
    canonical = {
        "snapshot_policy_version": _SHADOW_INPUT_SNAPSHOT_POLICY_VERSION,
        "provenance_policy_version": (
            research_max_pain_archive.SHADOW_PROVENANCE_POLICY_VERSION
        ),
        "formula_id": int(formula.get("formula_id") or 0),
        "formula_version": int(formula.get("formula_version") or 0),
        "condition_features": list(features),
        "event_provenance_refs": refs,
    }
    return {
        "required": True,
        "snapshot_policy_version": _SHADOW_INPUT_SNAPSHOT_POLICY_VERSION,
        "provenance_policy_version": (
            research_max_pain_archive.SHADOW_PROVENANCE_POLICY_VERSION
        ),
        "condition_features": list(features),
        "requires_previous": _max_pain_previous_required(features),
        "event_provenance_refs": refs,
        "completed_independent_provenance_complete": (
            len(refs) == len(completed_independent_rows)
            and not completed_incompatible_event_ids
        ),
        "completed_independent_incompatible_event_ids": sorted(
            completed_incompatible_event_ids
        ),
        "incompatible_event_ids": sorted(incompatible),
        "max_pain_provenance_excluded_event_ids": sorted(incompatible),
        "incompatible_reasons": {
            str(event_id): incompatible[event_id] for event_id in sorted(incompatible)
        },
        "canonical_evidence_sha256": (
            research_max_pain_archive.canonical_provenance_sha256(canonical)
        ),
    }


def _terminal_threshold_matches_snapshot(
    source: Mapping[str, Any],
    *,
    horizon_minutes: int,
    require_v6_contract: bool = False,
) -> tuple[bool, str]:
    """Require terminal first-touch semantics to match frozen Shadow width."""
    if not bool(source.get("first_touch_available")):
        return True, "no terminal first-touch row"
    snapshot = _as_mapping(source.get("input_snapshot"))
    reference = _as_mapping(snapshot.get("movement_width_reference"))
    if require_v6_contract:
        if snapshot.get("snapshot_policy_version") != (
            _SHADOW_INPUT_SNAPSHOT_POLICY_VERSION
        ):
            return False, "terminal frozen snapshot policy version is incompatible"
        if snapshot.get("decision_cohort_policy_version") != (
            _DECISION_COHORT_POLICY_VERSION
        ):
            return False, "terminal frozen decision cohort policy is incompatible"
        if snapshot.get("evidence_policy_version") != (
            _PROSPECTIVE_EVIDENCE_POLICY_VERSION
        ):
            return False, "terminal frozen evidence policy is incompatible"
        if not reference:
            return False, "terminal frozen movement-width reference is missing"
    raw_scale = (
        reference.get("threshold_scale_factor")
        if reference.get("threshold_scale_factor") is not None
        else reference.get("floor_scale_factor")
    )
    try:
        expected_scale = float(raw_scale) if raw_scale is not None else 1.0
        actual_scale = float(source["first_touch_threshold_scale_factor"])
        actual_threshold = float(source["qualifying_move_threshold_pct"])
    except (KeyError, TypeError, ValueError):
        return False, "terminal threshold metadata is incomplete"
    if not all(math.isfinite(value) for value in (expected_scale, actual_scale, actual_threshold)):
        return False, "terminal threshold metadata is non-finite"
    relaxed = expected_scale < 1.0 - 1e-9
    if relaxed and reference.get("applied") is not True:
        return False, "frozen relaxed width was not marked applied"
    if not 0.50 <= expected_scale <= 1.00:
        return False, "frozen threshold scale is outside 0.50-1.00"
    if require_v6_contract:
        try:
            decision_time = _as_utc(source.get("alert_time_utc"))
            compatible, reason = (
                research_session_width.validate_movement_width_reference(
                    reference,
                    expected_symbol=source.get("symbol"),
                    event_time=decision_time,
                    horizon_minutes=horizon_minutes,
                )
            )
            if not compatible:
                return False, "terminal " + reason
            expected_policy = research_no_dwell_outcome.freeze_threshold_policy(
                horizon_minutes=horizon_minutes,
                decision_time=decision_time,
                # Even scale=1 is an explicit prior-only decision-time
                # calibration result, not permission to substitute a static
                # threshold later.
                prior_only_reference=reference,
            )
            actual_policy = _as_mapping(
                source.get("first_touch_threshold_policy")
            )
            if not _type_strict_json_equal(actual_policy, expected_policy):
                return False, "terminal threshold policy differs from frozen snapshot"
        except (TypeError, ValueError, OverflowError):
            return False, "terminal frozen width provenance is malformed"
    if not math.isclose(
        actual_scale, expected_scale, rel_tol=0.0, abs_tol=1e-8
    ):
        return False, "terminal threshold scale differs from frozen width"
    expected_kind = (
        "PRIOR_ONLY_SESSION_CALIBRATION"
        if require_v6_contract or relaxed
        else "STATIC_HORIZON_FLOOR"
    )
    if str(source.get("first_touch_threshold_source_kind") or "").upper() != expected_kind:
        return False, "terminal threshold source differs from frozen width"
    expected_threshold = (
        research_no_dwell_outcome.base_favorable_width_pct(horizon_minutes)
        * expected_scale
    )
    if not math.isclose(
        actual_threshold, expected_threshold, rel_tol=0.0, abs_tol=1e-8
    ):
        return False, "terminal qualifying width differs from frozen width"
    return True, "terminal threshold matches frozen Shadow width"


def _as_utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        timestamp = value
    else:
        timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def _windows_overlap(
    left_start: datetime,
    right_start: datetime,
    horizon: timedelta,
) -> bool:
    return left_start < right_start + horizon and right_start < left_start + horizon


def _select_independent_shadow_rows(
    rows: Sequence[Mapping[str, Any]], *, horizon_minutes: int
) -> Dict[str, Any]:
    """Select outcome-blind, non-overlapping prospective evidence units.

    Exact immutable cohorts collapse first. A mixed MATCHED/UNMATCHED cohort
    fails closed. Market episodes then group all symbols together for at least
    24 hours, so one broad move cannot become several proofs. Membership never
    reads MFE, MAE, return, outcome availability or success.
    """
    eligible = sorted(
        (
            dict(row)
            for row in rows
            if str(row.get("evaluation_status") or "").upper()
            in {"MATCHED", "UNMATCHED", "UNEVALUABLE"}
        ),
        key=lambda row: (
            _as_utc(
                row.get("decision_anchor_time_utc") or row["alert_time_utc"]
            ),
            _as_utc(row["alert_time_utc"]),
            int(row["event_id"]),
        ),
    )
    exact_cohorts: Dict[tuple[str, str], list[Dict[str, Any]]] = {}
    malformed_cohort_event_ids: list[int] = []
    membership_censors: list[Dict[str, Any]] = []
    for row in eligible:
        symbol = str(row.get("symbol") or "").upper()
        cohort_key = str(row.get("decision_cohort_key") or "")
        if len(cohort_key) != 64:
            malformed_cohort_event_ids.append(int(row["event_id"]))
            censored = dict(row)
            censored["evaluation_status"] = "UNEVALUABLE"
            membership_censors.append(censored)
            continue
        exact_cohorts.setdefault((symbol, cohort_key), []).append(row)
    collapsed: list[Dict[str, Any]] = []
    exact_cohort_exclusions: list[int] = []
    conflicting_cohort_event_ids: list[int] = []
    for cohort_rows in exact_cohorts.values():
        statuses = {
            str(row.get("evaluation_status") or "").upper()
            for row in cohort_rows
        }
        if len(statuses) != 1:
            conflicting_cohort_event_ids.extend(
                int(row["event_id"]) for row in cohort_rows
            )
            censored = dict(
                min(
                    cohort_rows,
                    key=lambda row: (
                        _as_utc(row["alert_time_utc"]), int(row["event_id"])
                    ),
                )
            )
            censored["evaluation_status"] = "UNEVALUABLE"
            membership_censors.append(censored)
            continue
        ordered = sorted(
            cohort_rows,
            key=lambda row: (
                _as_utc(
                    row.get("decision_anchor_time_utc")
                    or row["alert_time_utc"]
                ),
                _as_utc(row["alert_time_utc"]),
                int(row["event_id"]),
            ),
        )
        collapsed.append(ordered[0])
        exact_cohort_exclusions.extend(int(row["event_id"]) for row in ordered[1:])
    selected = research_market_episode.select_independent(
        [
            row
            for row in (*collapsed, *membership_censors)
            if str(row.get("evaluation_status") or "").upper()
            in {"MATCHED", "UNEVALUABLE"}
        ],
        [
            row
            for row in collapsed
            if str(row.get("evaluation_status") or "").upper() == "UNMATCHED"
        ],
        horizon_minutes=int(horizon_minutes),
        presorted=False,
    )
    censor_ids = {
        int(row["event_id"])
        for row in (*collapsed, *membership_censors)
        if str(row.get("evaluation_status") or "").upper() == "UNEVALUABLE"
    }
    return {
        **selected,
        "matches": [
            row
            for row in selected["matches"]
            if str(row.get("evaluation_status") or "").upper() == "MATCHED"
        ],
        "rows": [
            row
            for row in selected["rows"]
            if str(row.get("evaluation_status") or "").upper()
            != "UNEVALUABLE"
        ],
        "excluded_match_event_ids": sorted(
            set(selected["excluded_match_event_ids"]) | censor_ids
        ),
        "exact_cohort_excluded_event_ids": sorted(exact_cohort_exclusions),
        "conflicting_cohort_event_ids": sorted(conflicting_cohort_event_ids),
        "malformed_cohort_event_ids": sorted(malformed_cohort_event_ids),
        "membership_censor_event_ids": sorted(censor_ids),
    }


def _metric_row(
    source: Mapping[str, Any], *, horizon_minutes: int
) -> Dict[str, Any]:
    snapshot = _as_mapping(source.get("input_snapshot"))
    session = _as_mapping(snapshot.get("outcome_window_session"))
    width_reference = _as_mapping(snapshot.get("movement_width_reference"))
    return {
        "event": {
            "event_id": int(source["event_id"]),
            "alert_time_utc": source["alert_time_utc"],
            "forecast_start_time_utc": (
                source.get("forecast_start_time_utc")
                or source["alert_time_utc"]
            ),
            "decision_anchor_time_utc": (
                source.get("decision_anchor_time_utc")
                or source["alert_time_utc"]
            ),
            "symbol": source.get("symbol"),
            "event_type": source.get("event_type"),
            "direction": source.get("direction"),
        },
        "outcome_label": {
            "horizon_minutes": int(horizon_minutes),
            "session_active_ratio": session.get("session_active_ratio"),
            "session_weekend_ratio": session.get("session_weekend_ratio"),
            "session_segments": session.get("session_segments") or [],
            "session_composition": session.get("session_composition"),
            "movement_width_reference": dict(width_reference),
            "directional_return_pct": source.get("directional_return_pct"),
            "path_success": source.get("path_success"),
            "first_touch_status": source.get("first_touch_status"),
            "mfe_pct": source.get("mfe_pct"),
            "mae_pct": source.get("mae_pct"),
            "full_horizon_mae_pct": source.get("full_horizon_mae_pct"),
            "time_to_first_progress_seconds": source.get(
                "time_to_first_progress_seconds"
            ),
            "time_to_first_qualifying_move_seconds": source.get(
                "time_to_first_qualifying_move_seconds"
            ),
            "qualifying_move_threshold_pct": source.get(
                "qualifying_move_threshold_pct"
            ),
            "qualifying_candle_order_ambiguous": source.get(
                "qualifying_candle_order_ambiguous"
            ),
            "time_to_mfe_seconds": source.get("time_to_mfe_seconds"),
            "target_progress_ratio": source.get("target_progress_ratio"),
            "target_reached": source.get("target_reached"),
        },
    }


def _build_shadow_validation(
    formula: Mapping[str, Any],
    source_rows: Sequence[Mapping[str, Any]],
    *,
    evaluated_at_utc: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Build one side-effect-free prospective readiness snapshot.

    Rolling monitoring and the manual owner-approval transaction deliberately
    call this same function.  It consumes terminal first-touch labels, keeps
    full-horizon MAE diagnostic-only, and accepts silent anchors only after the
    authoritative migration-008 view admitted them upstream.
    """
    horizon_minutes = int(formula["horizon_minutes"])
    guarded_source_rows: list[Dict[str, Any]] = []
    for source in source_rows:
        row = dict(source)
        compatible, reason = _max_pain_shadow_check_contract(
            formula, row
        )
        row["max_pain_provenance_compatible"] = compatible
        row["max_pain_provenance_reason"] = reason
        if not compatible:
            row["matched"] = False
            row["evaluation_status"] = "UNEVALUABLE"
            row["evaluation_reason"] = (
                "Max-Pain provenance rejected: " + reason
            )[:1000]
        guarded_source_rows.append(row)
    source_rows = guarded_source_rows
    independent = _select_independent_shadow_rows(
        source_rows, horizon_minutes=horizon_minutes
    )
    match_episodes = list(independent["match_episodes"])
    control_episodes = list(independent["control_episodes"])
    evaluation_time = evaluated_at_utc or datetime.now(timezone.utc)
    finalized_match_episodes, open_match_episodes = (
        research_market_episode.partition_finalized(
            match_episodes,
            horizon_minutes=horizon_minutes,
            as_of_utc=evaluation_time,
        )
    )
    finalized_control_episodes, open_control_episodes = (
        research_market_episode.partition_finalized(
            control_episodes,
            horizon_minutes=horizon_minutes,
            as_of_utc=evaluation_time,
        )
    )
    complete_match_episodes = [
        episode
        for episode in finalized_match_episodes
        if research_market_episode.episode_evidence_rows(episode)
        and all(
            str(row.get("evaluation_status") or "").upper() == "MATCHED"
            and bool(row.get("outcome_available"))
            for row in research_market_episode.episode_evidence_rows(episode)
        )
    ]
    complete_control_episodes = [
        episode
        for episode in finalized_control_episodes
        if research_market_episode.episode_evidence_rows(episode)
        and all(
            str(row.get("evaluation_status") or "").upper() == "UNMATCHED"
            and bool(row.get("outcome_available"))
            for row in research_market_episode.episode_evidence_rows(episode)
        )
    ]

    def aggregate_episode(episode: Mapping[str, Any]) -> Dict[str, Any]:
        member_metrics = [
            _metric_row(row, horizon_minutes=horizon_minutes)
            for row in episode["rows"]
        ]
        return research_market_episode.aggregate_metric_episode(
            member_metrics,
            episode_key=str(episode["episode_key"]),
            episode_start_utc=_as_utc(episode["start_time_utc"]),
            episode_end_utc=_as_utc(episode["end_time_utc"]),
        )

    selected = [aggregate_episode(episode) for episode in complete_match_episodes]
    control_metrics = [
        aggregate_episode(episode) for episode in complete_control_episodes
    ]
    universe = selected + control_metrics
    complete_rows = [
        row
        for episode in (*complete_match_episodes, *complete_control_episodes)
        for row in research_market_episode.episode_evidence_rows(episode)
    ]
    metrics = research_formula_engine.summarize_preaggregated_market_episodes(
        selected,
        universe,
        evidence_as_of_utc=evaluation_time,
    )
    priority = research_formula_engine.rank_prospective_metrics(
        metrics, horizon_minutes=horizon_minutes
    )
    improvement = metrics.get("session_hit_rate_improvement_pct_points")
    if improvement is None:
        improvement = metrics.get("hit_rate_improvement_pct_points")
    movement_percentile = metrics.get("session_adjusted_mfe_percentile_pct")
    if movement_percentile is None:
        movement_percentile = metrics.get("median_mfe_percentile_pct")
    efficiency = research_mfe_mae_efficiency.from_metrics(metrics)
    max_pain_evidence = _max_pain_validation_evidence(
        formula,
        source_rows=source_rows,
        completed_independent_rows=complete_rows,
    )
    raw_status_counts = {
        status: sum(
            1
            for row in source_rows
            if str(row.get("evaluation_status") or "").upper() == status
        )
        for status in ("MATCHED", "UNMATCHED", "UNEVALUABLE")
    }
    observed_member_rows = [
        row
        for episode in (*match_episodes, *control_episodes)
        for row in episode["rows"]
    ]
    independent_member_rows = [
        row
        for episode in (*match_episodes, *control_episodes)
        for row in research_market_episode.episode_evidence_rows(episode)
    ]
    evidence_event_ids = {int(row["event_id"]) for row in independent_member_rows}
    correlated_overdue_outcome_event_ids = [
        int(row["event_id"])
        for row in observed_member_rows
        if int(row["event_id"]) not in evidence_event_ids
        and not bool(row.get("outcome_available"))
        and bool(row.get("outcome_due"))
    ]
    pending_outcome_event_ids = [
        int(row["event_id"])
        for row in independent_member_rows
        if not bool(row.get("outcome_available"))
        and not bool(row.get("outcome_due"))
    ]
    overdue_outcome_event_ids = [
        int(row["event_id"])
        for row in independent_member_rows
        if not bool(row.get("outcome_available"))
        and bool(row.get("outcome_due"))
    ]
    threshold_policy_mismatch_event_ids = [
        int(row["event_id"])
        for row in independent_member_rows
        if row.get("first_touch_threshold_policy_compatible") is False
    ]
    early_first_touch_terminal_event_ids = [
        int(row["event_id"])
        for row in independent_member_rows
        if bool(row.get("first_touch_available"))
    ]
    early_first_touch_hit_event_ids = [
        int(row["event_id"])
        for row in independent_member_rows
        if bool(row.get("first_touch_available"))
        and bool(row.get("first_touch_hit"))
    ]
    early_matched_first_touch_hit_event_ids = [
        int(row["event_id"])
        for row in independent_member_rows
        if str(row.get("evaluation_status") or "").upper() == "MATCHED"
        and bool(row.get("first_touch_available"))
        and bool(row.get("first_touch_hit"))
    ]
    gate_results = {
        "matched future outcomes": int(metrics.get("sample_size") or 0)
        >= _SHADOW_MIN_MATCHES,
        "unmatched future controls": int(metrics.get("control_sample_size") or 0)
        >= _SHADOW_MIN_CONTROLS,
        "future temporal span": float(metrics.get("time_span_hours") or 0.0)
        >= float(_SHADOW_MIN_SPAN_HOURS),
        "future UTC dates": int(metrics.get("distinct_utc_dates") or 0)
        >= _SHADOW_MIN_DATES,
        "no overdue canonical outcome gaps": not overdue_outcome_event_ids,
        "authoritative cohort conflicts absent": not independent.get(
            "conflicting_cohort_event_ids"
        ),
        "authoritative cohort keys valid": not independent.get(
            "malformed_cohort_event_ids"
        ),
        "future session-composition baseline coverage": bool(
            metrics.get("session_baseline_complete")
        ),
        "future hit rate": float(metrics.get("hit_rate_pct") or 0.0) >= 65.0,
        "future Wilson lower bound": float(
            metrics.get("wilson_95_lower_pct") or 0.0
        )
        >= 50.0,
        "future improvement over controls": improvement is not None
        and float(improvement) >= 5.0,
        "wide favorable movement floor": float(
            metrics.get("median_mfe_pct") or 0.0
        )
        >= research_formula_engine.minimum_wide_move_pct(
            horizon_minutes, metrics
        ),
        "future wide movement percentile": float(movement_percentile or 0.0)
        >= 70.0,
        "future MFE/MAE efficiency": efficiency.meets_threshold(1.50),
        "future favorable exceeds p90 adverse": float(
            metrics.get("favorable_minus_p90_adverse_pct") or -999.0
        )
        > 0.0,
        "complete Max-Pain provenance chain": (
            not bool(max_pain_evidence["required"])
            or bool(
                max_pain_evidence[
                    "completed_independent_provenance_complete"
                ]
            )
        ),
    }
    research_acceptance = research_formula_acceptance.evaluate(
        metrics,
        phase="PROSPECTIVE",
        minimum_matches=_SHADOW_MIN_MATCHES,
        minimum_controls=_SHADOW_MIN_CONTROLS,
        minimum_recent_matches=3,
        minimum_recent_effective_samples=6.0,
        minimum_recent_control_effective_samples=6.0,
        maximum_last_match_age_hours=21.0 * 24.0,
        require_multiple_testing=False,
        mandatory_checks={
            "current v7 adaptive formula contract": (
                _current_live_formula_contract(formula)
            ),
            "no overdue canonical outcome gaps": not overdue_outcome_event_ids,
            "authoritative cohort conflicts absent": not independent.get(
                "conflicting_cohort_event_ids"
            ),
            "authoritative cohort keys valid": not independent.get(
                "malformed_cohort_event_ids"
            ),
            "prospective control baseline is sufficient": float(
                metrics.get("session_matched_control_effective_samples") or 0.0
            )
            >= float(_SHADOW_MIN_CONTROLS),
            "future wide movement percentile": float(
                movement_percentile or 0.0
            )
            >= 70.0,
            "complete Max-Pain provenance chain": (
                not bool(max_pain_evidence["required"])
                or bool(
                    max_pain_evidence[
                        "completed_independent_provenance_complete"
                    ]
                )
            ),
        },
        early_mandatory_checks={
            "current v7 adaptive formula contract": (
                _current_live_formula_contract(formula)
            ),
            "no overdue canonical outcome gaps": not overdue_outcome_event_ids,
            "authoritative cohort conflicts absent": not independent.get(
                "conflicting_cohort_event_ids"
            ),
            "authoritative cohort keys valid": not independent.get(
                "malformed_cohort_event_ids"
            ),
            "future wide movement percentile": float(
                movement_percentile or 0.0
            )
            >= 70.0,
            "complete Max-Pain provenance chain": (
                not bool(max_pain_evidence["required"])
                or bool(
                    max_pain_evidence[
                        "completed_independent_provenance_complete"
                    ]
                )
            ),
        },
    )
    episode_fingerprint = hashlib.sha256(
        _canonical_json(
            {
                "policy_version": research_market_episode.POLICY_VERSION,
                "matches": [
                    {
                        "key": episode["episode_key"],
                        "member_event_ids": sorted(
                            int(row["event_id"]) for row in episode["rows"]
                        ),
                        "evidence_event_ids": sorted(
                            int(row["event_id"])
                            for row in research_market_episode.episode_evidence_rows(
                                episode
                            )
                        ),
                    }
                    for episode in complete_match_episodes
                ],
                "controls": [
                    {
                        "key": episode["episode_key"],
                        "member_event_ids": sorted(
                            int(row["event_id"]) for row in episode["rows"]
                        ),
                        "evidence_event_ids": sorted(
                            int(row["event_id"])
                            for row in research_market_episode.episode_evidence_rows(
                                episode
                            )
                        ),
                    }
                    for episode in complete_control_episodes
                ],
            }
        ).encode("utf-8")
    ).hexdigest()
    matched_episode_ids = sorted(
        str(episode["episode_key"]) for episode in complete_match_episodes
    )
    control_episode_ids = sorted(
        str(episode["episode_key"]) for episode in complete_control_episodes
    )
    return {
        "policy_version": _SHADOW_MONITORING_POLICY_VERSION,
        "input_snapshot_policy_version": _SHADOW_INPUT_SNAPSHOT_POLICY_VERSION,
        "mfe_mae_efficiency_policy_version": (
            research_mfe_mae_efficiency.POLICY_VERSION
        ),
        "outcome_method_version": research_feature_matrix.VERIFIED_OUTCOME_METHOD,
        "evaluated_at_utc": evaluation_time,
        "stage_ceiling": "SHADOW_PENDING_EXPLICIT_APPROVAL",
        "statistical_use": (
            "rolling descriptive monitoring only; explicit owner approval "
            "requires a separately frozen prospective review"
        ),
        "sampling_frame": (
            "authoritative sampler-v4 silent DECISION_SAMPLE LONG/SHORT "
            "anchors with verified frozen feature-bundle evidence; controls "
            "are evaluable formula nonmatches within that population"
        ),
        "metrics": metrics,
        "priority_ranking": priority,
        "research_acceptance": research_acceptance,
        "evidence_snapshot_inputs": {
            "matched_market_episode_ids": matched_episode_ids,
            "control_market_episode_ids": control_episode_ids,
            # The Market Episode policy already groups all symbols in one
            # outcome-blind broad-move unit.  Until a separate cross-formula
            # parent namespace is introduced, that same immutable key is the
            # conservative parent identity and never increases N_eff.
            "matched_parent_market_episode_ids": matched_episode_ids,
            "control_parent_market_episode_ids": control_episode_ids,
            "raw_match_count": int(raw_status_counts["MATCHED"]),
            "raw_control_count": int(raw_status_counts["UNMATCHED"]),
            "matched_n_eff": float(
                metrics.get("recency_effective_sample_size") or 0.0
            ),
            "control_n_eff": float(
                metrics.get("recency_control_effective_sample_size") or 0.0
            ),
        },
        "formula_contract": {
            "current_v7": _current_live_formula_contract(formula),
            "exact_retained_v5": bool(
                str(formula.get("formula_schema_version") or "")
                == _LEGACY_V5_FORMULA_SCHEMA_VERSION
                and _shadow_executable_formula_contract(formula)
            ),
            "legacy_v5_adapter_version": (
                _LEGACY_V5_SHADOW_ADAPTER_VERSION
                if str(formula.get("formula_schema_version") or "")
                == _LEGACY_V5_FORMULA_SCHEMA_VERSION
                else None
            ),
            "research_acceptance_eligible": (
                _current_live_formula_contract(formula)
            ),
        },
        "evidence": {
            "raw_checks": len(source_rows),
            "raw_evaluation_status": raw_status_counts,
            "independent_matches": len(match_episodes),
            "independent_controls": len(control_episodes),
            "finalized_independent_matches": len(finalized_match_episodes),
            "finalized_independent_controls": len(finalized_control_episodes),
            "complete_independent_matches": len(complete_match_episodes),
            "complete_independent_controls": len(complete_control_episodes),
            "open_match_episode_keys": [
                str(episode["episode_key"]) for episode in open_match_episodes
            ],
            "open_control_episode_keys": [
                str(episode["episode_key"]) for episode in open_control_episodes
            ],
            "episode_finalization_rule": (
                "count only after a full quiet gap has closed membership and "
                "the earliest forecast cohort's outcome horizon has matured"
            ),
            "market_episode_policy_version": (
                research_market_episode.POLICY_VERSION
            ),
            "market_episode_fingerprint": episode_fingerprint,
            "market_episode_minutes": max(
                research_market_episode.MINIMUM_EPISODE_MINUTES,
                horizon_minutes,
            ),
            "correlated_match_exclusions": len(
                independent["excluded_match_event_ids"]
            ),
            "correlated_control_exclusions": len(
                independent["excluded_control_event_ids"]
            ),
            "exact_cohort_exclusions": len(
                independent["exact_cohort_excluded_event_ids"]
            ),
            "conflicting_cohort_event_ids": independent.get(
                "conflicting_cohort_event_ids"
            )
            or [],
            "malformed_cohort_event_ids": independent.get(
                "malformed_cohort_event_ids"
            )
            or [],
            "membership_censor_event_ids": independent.get(
                "membership_censor_event_ids"
            )
            or [],
            "pending_outcome_event_ids": pending_outcome_event_ids,
            "overdue_outcome_event_ids": overdue_outcome_event_ids,
            "correlated_overdue_outcome_event_ids": (
                correlated_overdue_outcome_event_ids
            ),
            "threshold_policy_mismatch_event_ids": (
                threshold_policy_mismatch_event_ids
            ),
            "early_first_touch": {
                "terminal_event_ids": early_first_touch_terminal_event_ids,
                "hit_event_ids": early_first_touch_hit_event_ids,
                "matched_hit_event_ids": (
                    early_matched_first_touch_hit_event_ids
                ),
                "readiness_treatment": (
                    "observational immediately; statistical LIVE gates still "
                    "require the verified full-horizon diagnostic row"
                ),
                "hold_requirement": (
                    "none; first favorable touch is final and a later "
                    "reversal cannot cancel it"
                ),
            },
            "max_pain_provenance": max_pain_evidence,
            "independence_policy": (
                "exact decision cohorts collapse first; all symbols inside "
                "one outcome-blind market episode carry total evidence weight "
                "one; a new episode requires the fixed window to end and a "
                "decision-time directional price reset"
            ),
        },
        "gates": gate_results,
        "failed_gates": [
            name for name, passed in gate_results.items() if not passed
        ],
        "thresholds_met": bool(gate_results) and all(gate_results.values()),
        "live_eligible": False,
        "live_blocker": "a separate explicit owner approval record is absent",
    }


def _formula_evidence_contract(formula: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "formula_key": str(formula.get("formula_key") or ""),
        "formula_version": int(formula.get("formula_version") or 0),
        "formula_schema_version": str(formula.get("formula_schema_version") or ""),
        "engine_version": str(formula.get("engine_version") or ""),
        "feature_schema_version": str(formula.get("feature_schema_version") or ""),
        "outcome_method_version": str(formula.get("outcome_method_version") or ""),
        "direction": str(formula.get("direction") or "").upper(),
        "horizon_minutes": int(formula.get("horizon_minutes") or 0),
    }


def _formula_family_id(formula: Mapping[str, Any]) -> Optional[str]:
    if not _current_live_formula_contract(formula):
        return None
    multiple_testing = _as_mapping(formula.get("latest_multiple_testing"))
    family = _as_mapping(multiple_testing.get("evidence_family"))
    identifier = str(family.get("family_id") or "").strip().lower()
    if len(identifier) != 64 or any(
        character not in "0123456789abcdef" for character in identifier
    ):
        raise RuntimeError("current v7 formula is missing its frozen evidence family id")
    return identifier


def _build_shadow_evidence_snapshot(
    formula: Mapping[str, Any], validation: Mapping[str, Any]
) -> research_evidence_contract.EvidenceSnapshot:
    assessment = research_evidence_contract.FormulaAssessment.from_acceptance(
        _as_mapping(validation.get("research_acceptance")),
        phase="PROSPECTIVE",
    )
    inputs = _as_mapping(validation.get("evidence_snapshot_inputs"))
    evidence = _as_mapping(validation.get("evidence"))
    provenance = {
        "shadow_monitoring_policy_version": validation.get("policy_version"),
        "input_snapshot_policy_version": validation.get(
            "input_snapshot_policy_version"
        ),
        "outcome_method_version": validation.get("outcome_method_version"),
        "formula_contract": validation.get("formula_contract") or {},
        "max_pain_provenance": evidence.get("max_pain_provenance") or {},
        "live_effect": "NONE",
        "delivery_channel": "NONE",
    }
    return research_evidence_contract.EvidenceSnapshot.build(
        formula_contract=_formula_evidence_contract(formula),
        assessment=assessment,
        assessed_at_utc=validation["evaluated_at_utc"],
        formula_family_id=_formula_family_id(formula),
        matched_market_episode_ids=(
            inputs.get("matched_market_episode_ids") or []
        ),
        control_market_episode_ids=(
            inputs.get("control_market_episode_ids") or []
        ),
        matched_parent_market_episode_ids=(
            inputs.get("matched_parent_market_episode_ids") or []
        ),
        control_parent_market_episode_ids=(
            inputs.get("control_parent_market_episode_ids") or []
        ),
        raw_match_count=int(inputs.get("raw_match_count") or 0),
        raw_control_count=int(inputs.get("raw_control_count") or 0),
        matched_n_eff=float(inputs.get("matched_n_eff") or 0.0),
        control_n_eff=float(inputs.get("control_n_eff") or 0.0),
        metrics=_as_mapping(validation.get("metrics")),
        evidence=evidence,
        provenance=provenance,
    )


def _relevance_assessment_by_observation(
    conn: Any, *, formula_id: int, formula_version: int, observation_fingerprint: str
) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT snapshot_id, decision_payload
        FROM research_formula_relevance_assessments
        WHERE formula_id=%s
          AND formula_version=%s
          AND relevance_policy_version=%s
          AND observation_fingerprint=%s
        """,
        (
            int(formula_id),
            int(formula_version),
            research_formula_relevance.POLICY_VERSION,
            observation_fingerprint,
        ),
    ).fetchone()
    if not row:
        return None
    payload = dict(_as_mapping(row.get("decision_payload")))
    if str(row.get("snapshot_id") or "") != str(payload.get("snapshot_id") or ""):
        raise RuntimeError("stored relevance decision conflicts with its snapshot id")
    return payload


def _latest_relevance_assessment(
    conn: Any, *, formula_id: int, formula_version: int
) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT decision_payload
        FROM research_formula_relevance_assessments
        WHERE formula_id=%s
          AND formula_version=%s
          AND relevance_policy_version=%s
        ORDER BY relevance_assessment_id DESC
        LIMIT 1
        """,
        (
            int(formula_id),
            int(formula_version),
            research_formula_relevance.POLICY_VERSION,
        ),
    ).fetchone()
    return None if not row else dict(_as_mapping(row.get("decision_payload")))


def _persist_relevance_assessment_conn(
    conn: Any,
    *,
    formula_id: int,
    formula_version: int,
    decision: Mapping[str, Any],
) -> Dict[str, Any]:
    payload = dict(decision)
    conn.execute(
        """
        INSERT INTO research_formula_relevance_assessments (
            formula_id, formula_version, relevance_policy_version,
            observation_fingerprint, evidence_fingerprint, snapshot_id,
            observed_at_utc, observation_utc_date, compatibility, observation,
            previous_state, relevance_state, transition,
            weak_observation_streak, recovery_evidence_streak,
            experimental_relevance_eligible, decision_payload
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s::jsonb
        )
        ON CONFLICT (
            formula_id, formula_version, relevance_policy_version,
            observation_fingerprint
        ) DO NOTHING
        """,
        (
            int(formula_id),
            int(formula_version),
            payload["policy_version"],
            payload["observation_fingerprint"],
            payload["evidence_fingerprint"],
            payload["snapshot_id"],
            payload["observed_at_utc"],
            payload["observation_utc_date"],
            payload["compatibility"],
            payload["observation"],
            payload.get("previous_state"),
            payload["state"],
            payload["transition"],
            int(payload["weak_observation_streak"]),
            int(payload["recovery_evidence_streak"]),
            bool(payload["experimental_relevance_eligible"]),
            _json(payload),
        ),
    )
    stored = _relevance_assessment_by_observation(
        conn,
        formula_id=formula_id,
        formula_version=formula_version,
        observation_fingerprint=str(payload["observation_fingerprint"]),
    )
    if not stored or not _type_strict_json_equal(stored, payload):
        raise RuntimeError(
            "stored relevance assessment conflicts with its observation fingerprint"
        )
    return stored


def _persist_shadow_evidence_and_relevance(
    conn: Any,
    *,
    formula: Mapping[str, Any],
    validation: Mapping[str, Any],
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    formula_id = int(formula["formula_id"])
    formula_version = int(formula["formula_version"])
    formula_contract = _formula_evidence_contract(formula)
    assessment = research_evidence_contract.FormulaAssessment.from_acceptance(
        _as_mapping(validation.get("research_acceptance")), phase="PROSPECTIVE"
    )
    compatibility = research_evidence_contract.compatibility_for_formula_schema(
        formula_contract["formula_schema_version"]
    )
    evidence_fingerprint = str(
        _as_mapping(validation.get("evidence")).get("market_episode_fingerprint")
        or ""
    )
    observed_at_utc = validation["evaluated_at_utc"]
    observation_fingerprint = research_formula_relevance.observation_fingerprint(
        formula_contract=formula_contract,
        assessment=assessment,
        evidence_fingerprint=evidence_fingerprint,
        observed_at_utc=observed_at_utc,
    )
    existing = _relevance_assessment_by_observation(
        conn,
        formula_id=formula_id,
        formula_version=formula_version,
        observation_fingerprint=observation_fingerprint,
    )
    if existing:
        snapshot = conn.execute(
            """
            SELECT snapshot_payload
            FROM research_formula_evidence_snapshots
            WHERE snapshot_id=%s
            """,
            (existing["snapshot_id"],),
        ).fetchone()
        if not snapshot:
            raise RuntimeError("relevance assessment references a missing snapshot")
        resolved_snapshot = research_evidence_contract.EvidenceSnapshot.from_dict(
            snapshot["snapshot_payload"]
        )
        current = dict(existing)
        current["duplicate_observation"] = True
        return resolved_snapshot.to_dict(), current

    snapshot = _build_shadow_evidence_snapshot(formula, validation)
    _persist_evidence_snapshot_conn(
        conn,
        snapshot,
        formula_id=formula_id,
        source_run_id=_strict_int(formula.get("latest_evaluation_run_id")),
    )
    previous = _latest_relevance_assessment(
        conn, formula_id=formula_id, formula_version=formula_version
    )
    decision = research_formula_relevance.advance(
        previous=previous,
        formula_contract=formula_contract,
        compatibility=compatibility,
        assessment=assessment,
        evidence_fingerprint=evidence_fingerprint,
        observed_at_utc=observed_at_utc,
        snapshot_id=snapshot.snapshot_id,
    )
    stored = _persist_relevance_assessment_conn(
        conn,
        formula_id=formula_id,
        formula_version=formula_version,
        decision=decision,
    )
    return snapshot.to_dict(), stored


def evaluate_shadow_readiness() -> Dict[str, Any]:
    """Persist rolling evidence and relevance without changing a formula stage.

    These metrics are monitoring evidence only.  Controls are independent
    formula nonmatches drawn from delivered alerts and authoritative neutral
    anchors. Repeated rolling looks are not an approval test. No path in this
    function writes APPROVED/LIVE or ``live_alert_approved``.
    """
    evaluated = 0
    thresholds_met: list[int] = []
    research_ready: list[int] = []
    experimentally_relevant: list[int] = []
    relevance_state_counts: Dict[str, int] = {}
    ranked: list[Dict[str, Any]] = []
    evaluated_at_utc = datetime.now(timezone.utc)
    with _connect(read_only=False) as conn:
        formulas = conn.execute(
            """
            SELECT formula_id, formula_key, formula_version, formula_schema_version,
                   engine_version, feature_schema_version,
                   outcome_method_version, direction, conditions, horizon_minutes,
                   latest_evaluation_run_id, shadow_started_at_utc,
                   last_shadow_event_id,
                   (
                     SELECT evaluation.multiple_testing
                     FROM research_formula_evaluations evaluation
                     WHERE evaluation.formula_id=research_formulas.formula_id
                       AND evaluation.run_id=research_formulas.latest_evaluation_run_id
                     LIMIT 1
                   ) AS latest_multiple_testing
            FROM research_formulas
            WHERE active=TRUE
              AND current_stage='SHADOW'
              AND (
                    (
                      formula_schema_version=%s
                      AND engine_version=%s
                      AND feature_schema_version=%s
                      AND outcome_method_version=%s
                    )
                    OR (
                      formula_schema_version=%s
                      AND engine_version=%s
                      AND feature_schema_version=%s
                      AND outcome_method_version=%s
                    )
                  )
            ORDER BY formula_id
            FOR UPDATE
            """,
            (
                research_formula_engine.LEGACY_V6_FORMULA_SCHEMA_VERSION,
                research_formula_engine.LEGACY_V6_ENGINE_VERSION,
                research_feature_matrix.FEATURE_SCHEMA_VERSION,
                research_feature_matrix.VERIFIED_OUTCOME_METHOD,
                research_formula_engine.FORMULA_SCHEMA_VERSION,
                research_formula_engine.ENGINE_VERSION,
                research_feature_matrix.FEATURE_SCHEMA_VERSION,
                research_feature_matrix.VERIFIED_OUTCOME_METHOD,
            ),
        ).fetchall()
        for formula_row in formulas:
            formula = dict(formula_row)
            source_rows = _shadow_outcome_rows(conn, formula)
            horizon_minutes = int(formula["horizon_minutes"])
            validation = _build_shadow_validation(
                formula,
                source_rows,
                evaluated_at_utc=evaluated_at_utc,
            )
            snapshot, relevance = _persist_shadow_evidence_and_relevance(
                conn,
                formula=formula,
                validation=validation,
            )
            validation["evidence_snapshot"] = {
                "snapshot_id": snapshot["snapshot_id"],
                "compatibility": snapshot["compatibility"],
                "formula_family_id": snapshot["formula_family_id"],
                "live_eligible": False,
                "delivery_channel": "NONE",
            }
            validation["relevance"] = relevance
            priority = dict(validation["priority_ranking"])
            evaluated += 1
            formula_id = int(formula["formula_id"])
            ranked.append(
                {
                    "formula_id": formula_id,
                    "horizon_minutes": horizon_minutes,
                    "relevance_state": relevance["state"],
                    "experimental_relevance_eligible": bool(
                        relevance["experimental_relevance_eligible"]
                    ),
                    **priority,
                }
            )
            conn.execute(
                """
                UPDATE research_formulas
                SET shadow_validation_metrics=%s::jsonb, updated_at_utc=NOW()
                WHERE formula_id=%s
                """,
                (_json(validation), formula_id),
            )
            if bool(validation["thresholds_met"]):
                thresholds_met.append(formula_id)
            if bool(
                (validation.get("research_acceptance") or {}).get(
                    "research_ready"
                )
            ):
                research_ready.append(formula_id)
            relevance_state = str(relevance["state"])
            relevance_state_counts[relevance_state] = (
                relevance_state_counts.get(relevance_state, 0) + 1
            )
            if bool(relevance["experimental_relevance_eligible"]):
                experimentally_relevant.append(formula_id)
        conn.commit()
    ranked.sort(key=lambda item: (float(item["score"]), -int(item["formula_id"])), reverse=True)
    for rank, item in enumerate(ranked, start=1):
        item["rank"] = rank
    return {
        "policy_version": _SHADOW_MONITORING_POLICY_VERSION,
        "evaluated": evaluated,
        "thresholds_met": thresholds_met,
        "ready_for_explicit_review": thresholds_met,
        "legacy_live_review_ready": thresholds_met,
        "research_ready": research_ready,
        "research_ready_for_experimental_review": research_ready,
        "experimentally_relevant": experimentally_relevant,
        "relevance_policy_version": research_formula_relevance.POLICY_VERSION,
        "relevance_state_counts": relevance_state_counts,
        "prospective_ranking": ranked,
        "promoted": [],
        "automatic_stage_ceiling": "SHADOW_PENDING_EXPLICIT_APPROVAL",
    }


def promote_eligible_shadow_formulas() -> Dict[str, Any]:
    """Deprecated compatibility wrapper; automatic promotion is forbidden."""
    return evaluate_shadow_readiness()


def set_alert_subscription(
    chat_id: int,
    *,
    active: bool,
    requested_by_user_id: Optional[int] = None,
) -> Dict[str, Any]:
    identifier = int(chat_id)
    with _connect(read_only=False) as conn:
        row = conn.execute(
            """
            INSERT INTO research_formula_alert_subscriptions (
                chat_id, active, requested_by_user_id,
                subscribed_at_utc, updated_at_utc
            ) VALUES (%s, %s, %s, NOW(), NOW())
            ON CONFLICT (chat_id) DO UPDATE SET
                active=EXCLUDED.active,
                requested_by_user_id=EXCLUDED.requested_by_user_id,
                subscribed_at_utc=CASE
                    WHEN EXCLUDED.active THEN NOW()
                    ELSE research_formula_alert_subscriptions.subscribed_at_utc
                END,
                updated_at_utc=NOW()
            RETURNING chat_id, active, requested_by_user_id,
                      subscribed_at_utc, updated_at_utc
            """,
            (identifier, bool(active), requested_by_user_id),
        ).fetchone()
        conn.commit()
    return _json_safe(dict(row))


def alert_subscription_status(chat_id: int) -> Dict[str, Any]:
    with _connect(read_only=True) as conn:
        row = conn.execute(
            """
            SELECT chat_id, active, requested_by_user_id,
                   subscribed_at_utc, updated_at_utc
            FROM research_formula_alert_subscriptions
            WHERE chat_id=%s
            """,
            (int(chat_id),),
        ).fetchone()
        active_count = conn.execute(
            """
            SELECT COUNT(*)::bigint AS count
            FROM research_formula_alert_subscriptions WHERE active=TRUE
            """
        ).fetchone()
    return _json_safe(
        {
            "configured": bool(row),
            "active": bool(row and row["active"]),
            "subscription": dict(row) if row else None,
            "active_subscriptions": int(active_count["count"] or 0),
        }
    )


def load_pending_live_deliveries(limit: int = 50) -> list[Dict[str, Any]]:
    row_limit = max(1, min(int(limit), 200))
    with _connect(read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT d.delivery_id, d.chat_id, d.attempts,
                   f.formula_id, f.formula_version, f.formula_text,
                   f.direction, f.horizon_minutes,
                   f.shadow_validation_metrics,
                   e.event_id, e.alert_time_utc, e.symbol, e.event_type,
                   e.current_price, e.target_price,
                   ev.ranking_score, ev.holdout_metrics
            FROM research_formula_live_deliveries d
            JOIN research_formulas f ON f.formula_id=d.formula_id
            JOIN research_events e ON e.event_id=d.event_id
            LEFT JOIN research_formula_evaluations ev
              ON ev.run_id=f.latest_evaluation_run_id
             AND ev.formula_id=f.formula_id
            JOIN research_formula_alert_subscriptions s
              ON s.chat_id=d.chat_id AND s.active=TRUE
            WHERE f.active=TRUE AND f.current_stage='LIVE'
              AND f.formula_schema_version=%s
              AND f.engine_version=%s
              AND f.feature_schema_version=%s
              AND f.outcome_method_version=%s
              AND e.event_kind='ALERT'
              AND e.delivery_status='DELIVERED'
              AND f.live_alert_approved=TRUE
              AND BTRIM(COALESCE(f.live_alert_approved_by, ''))<>''
              AND EXISTS (
                  SELECT 1
                  FROM research_formula_live_approvals a
                  WHERE a.formula_id=f.formula_id
                    AND a.formula_version=f.formula_version
                    AND a.horizon_minutes=f.horizon_minutes
                    AND a.formula_schema_version=f.formula_schema_version
                    AND a.engine_version=f.engine_version
                    AND a.feature_schema_version=f.feature_schema_version
                    AND a.outcome_method_version=f.outcome_method_version
                    AND a.approval_operation_version=%s
                    AND a.delivery_environment_enabled=FALSE
                    AND a.review_kind='FROZEN_PROSPECTIVE'
                    AND a.thresholds_met=TRUE
                    AND a.validated_future_matches>=%s
                    AND a.validated_future_controls>=%s
                    AND a.validated_span_hours>=%s
                    AND a.validated_utc_dates>=%s
              )
              AND e.alert_time_utc >= NOW() - (%s * INTERVAL '1 minute')
              AND d.attempts<%s
              AND (
                  d.status='PENDING'
                  OR (
                      d.status='FAILED'
                      AND d.last_attempt_at_utc < NOW() - INTERVAL '15 minutes'
                  )
              )
            ORDER BY d.created_at_utc, d.delivery_id
            LIMIT %s
            """,
            (
                research_formula_engine.FORMULA_SCHEMA_VERSION,
                research_formula_engine.ENGINE_VERSION,
                research_feature_matrix.FEATURE_SCHEMA_VERSION,
                research_feature_matrix.VERIFIED_OUTCOME_METHOD,
                _LIVE_APPROVAL_OPERATION_VERSION,
                _SHADOW_MIN_MATCHES,
                _SHADOW_MIN_CONTROLS,
                float(_SHADOW_MIN_SPAN_HOURS),
                _SHADOW_MIN_DATES,
                _DELIVERY_MAX_AGE_MINUTES,
                _DELIVERY_MAX_ATTEMPTS,
                row_limit,
            ),
        ).fetchall()
    return _json_safe([dict(row) for row in rows])


def mark_live_delivery(
    delivery_id: int,
    *,
    sent: bool,
    error: Optional[str] = None,
) -> None:
    status = "SENT" if sent else "FAILED"
    with _connect(read_only=False) as conn:
        conn.execute(
            """
            UPDATE research_formula_live_deliveries
            SET status=%s,
                attempts=attempts+1,
                last_attempt_at_utc=NOW(),
                sent_at_utc=CASE WHEN %s THEN NOW() ELSE sent_at_utc END,
                last_error=%s
            WHERE delivery_id=%s
            """,
            (
                status,
                bool(sent),
                None if sent else str(error or "unknown delivery failure")[:1000],
                int(delivery_id),
            ),
        )
        conn.commit()
