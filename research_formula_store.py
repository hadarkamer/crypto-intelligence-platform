"""Auditable PostgreSQL registry for discovered and shadow-tested formulas."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from typing import Any, Dict, Mapping, Optional, Sequence

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None

import canonical_price_path
import research_feature_matrix
import research_formula_engine


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
_SHADOW_COMPATIBLE_FORMULA_SCHEMAS = (
    "research-formula-v5-safe-replay",
    research_formula_engine.FORMULA_SCHEMA_VERSION,
)
_SHADOW_MONITORING_POLICY_VERSION = (
    os.getenv("FORMULA_SHADOW_MONITORING_POLICY_VERSION", "").strip()
    or "shadow-monitoring-v1-independent-alert-episodes-2026-08-29"
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
    return json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))


def _json_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False, default=str))


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
    )
    required_columns = {
        "research_formula_shadow_checks": (
            "evaluation_status",
            "evaluation_reason",
            "input_snapshot",
            "condition_results",
            "decision_cohort_key",
            "decision_anchor_time_utc",
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
        ),
        "research_prospective_anchor_slots": (
            "long_event_id",
            "short_event_id",
            "source_timestamps",
            "source_provenance",
            "frozen_inputs",
            "input_fingerprint",
        ),
    }
    base = {
        "configured": bool(_database_url()),
        "schema_present": False,
        "missing_tables": list(required),
        "missing_columns": [],
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
              (SELECT COUNT(*) FROM research_formula_live_deliveries WHERE status='PENDING') AS pending_live_deliveries
            """
        ).fetchone()
        base.update({key: int(counts[key] or 0) for key in counts})
    return _json_safe(base)


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


def persist_discovery_run(
    *,
    dataset: Mapping[str, Any],
    discovery: Mapping[str, Any],
    lookback_days: int,
) -> Dict[str, Any]:
    """Atomically store one completed run, immutable formulas and evaluations."""
    if not discovery.get("available"):
        raise ValueError("cannot persist an unavailable discovery result")
    formulas = list(discovery.get("formulas") or [])
    coverage = dict(dataset.get("coverage") or {})
    replacement_ready = bool(coverage.get("replacement_ready"))
    dataset_kind = str(coverage.get("dataset_kind") or "unknown")
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
                candidates_evaluated, config, coverage
            ) VALUES (
                %s, %s, %s, %s, %s, 'RUNNING', %s, %s, %s,
                %s, %s, %s, %s, %s::jsonb, %s::jsonb
            ) RETURNING run_id
            """,
            (
                discovery["engine_version"],
                discovery["feature_schema_version"],
                dataset.get("outcome_method_version") or research_feature_matrix.VERIFIED_OUTCOME_METHOD,
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
                _json(coverage),
            ),
        ).fetchone()
        run_id = int(run["run_id"])
        # A new schema may retire its predecessor only after the replacement
        # dataset has broad chronological coverage.  This prevents a tiny
        # post-deploy alert sample from deleting the last auditable cohort.
        superseded = conn.execute(
            """
            SELECT formula_id, current_stage
            FROM research_formulas
            WHERE active=TRUE
              AND formula_schema_version<>%s
              AND horizon_minutes=%s
              AND current_stage NOT IN ('SHADOW', 'APPROVED', 'LIVE', 'RETIRED')
            FOR UPDATE
            """,
            (
                research_formula_engine.FORMULA_SCHEMA_VERSION,
                int(discovery["horizon_minutes"]),
            ),
        ).fetchall() if replacement_ready else []
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
                        "superseded by hierarchical evidence-family formula schema v6 "
                        f"from {dataset_kind}"
                    ),
                ),
            )
        if superseded:
            conn.execute(
                """
                UPDATE research_formulas
                SET current_stage='RETIRED', active=FALSE, updated_at_utc=NOW()
                WHERE active=TRUE
                  AND formula_schema_version<>%s
                  AND horizon_minutes=%s
                  AND current_stage NOT IN ('SHADOW', 'APPROVED', 'LIVE', 'RETIRED')
                """,
                (
                    research_formula_engine.FORMULA_SCHEMA_VERSION,
                    int(discovery["horizon_minutes"]),
                ),
            )
        persisted = 0
        stage_counts: Dict[str, int] = {}
        for global_rank, formula in enumerate(formulas, start=1):
            requested_stage, gate_notes = _requested_stage_for_dataset(
                formula,
                replacement_ready=replacement_ready,
            )
            existing = conn.execute(
                "SELECT formula_id, current_stage FROM research_formulas WHERE formula_key=%s",
                (formula["formula_key"],),
            ).fetchone()
            if existing:
                formula_id = int(existing["formula_id"])
                current_stage = str(existing["current_stage"])
                conn.execute(
                    """
                    UPDATE research_formulas
                    SET engine_version=%s, latest_evaluation_run_id=%s,
                        formula_text=%s, updated_at_utc=NOW()
                    WHERE formula_id=%s
                    """,
                    (
                        formula["engine_version"],
                        run_id,
                        formula["formula_text"],
                        formula_id,
                    ),
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
                        dataset.get("outcome_method_version") or research_feature_matrix.VERIFIED_OUTCOME_METHOD,
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
              AND horizon_minutes=%s
              AND current_stage IN ('DISCOVERED', 'BACKTESTED', 'HOLDOUT_PASSED')
              AND latest_evaluation_run_id IS DISTINCT FROM %s
            FOR UPDATE
            """,
            (
                research_formula_engine.FORMULA_SCHEMA_VERSION,
                int(discovery["horizon_minutes"]),
                run_id,
            ),
        ).fetchall() if replacement_ready else []
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
            conn.execute(
                """
                UPDATE research_formulas
                SET current_stage='RETIRED', active=FALSE, updated_at_utc=NOW()
                WHERE active=TRUE
                  AND formula_schema_version=%s
                  AND horizon_minutes=%s
                  AND current_stage IN ('DISCOVERED', 'BACKTESTED', 'HOLDOUT_PASSED')
                  AND latest_evaluation_run_id IS DISTINCT FROM %s
                """,
                (
                    research_formula_engine.FORMULA_SCHEMA_VERSION,
                    int(discovery["horizon_minutes"]),
                    run_id,
                ),
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
                   f.direction, f.horizon_minutes, f.conditions,
                   f.formula_text, f.current_stage, f.active,
                   f.live_alert_approved, f.shadow_started_at_utc,
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
            "formulas": [dict(row) for row in rows],
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
            "live_delivery": {
                "active_subscriptions": int(subscriptions["count"] or 0),
                "by_status": {
                    row["status"]: int(row["count"] or 0)
                    for row in delivery_counts
                },
            },
        }
    )


def load_shadow_work(max_events_per_formula: int = 100) -> list[Dict[str, Any]]:
    """Load future event pairs for active Shadow and validated Live formulas."""
    limit = max(1, min(int(max_events_per_formula), 250))
    work: list[Dict[str, Any]] = []
    with _connect(read_only=True) as conn:
        formulas = conn.execute(
            """
            SELECT f.formula_id, f.formula_key, f.formula_version, f.formula_text,
                   f.formula_schema_version, f.direction, f.horizon_minutes,
                   f.conditions, f.feature_schema_version,
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
                  AND f.formula_schema_version=ANY(%s)
                )
                OR (
                  f.current_stage='LIVE'
                  AND f.formula_schema_version=%s
                )
              )
            ORDER BY
              CASE WHEN f.current_stage='LIVE' THEN 1 ELSE 0 END DESC,
              e.ranking_score DESC NULLS LAST,
              f.formula_id
            """,
            (
                list(_SHADOW_COMPATIBLE_FORMULA_SCHEMAS),
                research_formula_engine.FORMULA_SCHEMA_VERSION,
            ),
        ).fetchall()
        for formula in formulas:
            events = conn.execute(
                """
                SELECT event_id, alert_time_utc, symbol, direction,
                       event_type, setup_key, event_kind, delivery_status
                FROM research_events candidate
                WHERE direction=%s
                  AND event_id>%s
                  AND alert_time_utc>=COALESCE(%s, '-infinity'::timestamptz)
                  AND (
                    (event_kind='ALERT' AND delivery_status='DELIVERED')
                    OR (
                      event_kind='DECISION_SAMPLE'
                      AND delivery_status='NOT_APPLICABLE'
                      AND EXISTS (
                        SELECT 1
                        FROM research_prospective_shadow_events authorized
                        WHERE authorized.event_id=candidate.event_id
                      )
                    )
                  )
                ORDER BY event_id ASC
                LIMIT %s
                """,
                (
                    formula["direction"],
                    int(formula["last_shadow_event_id"] or 0),
                    formula["shadow_started_at_utc"],
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
    with _connect(read_only=False) as conn:
        current = conn.execute(
            """
            SELECT current_stage, active, formula_version,
                   live_alert_approved, live_alert_approved_by
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
            SELECT candidate.event_id, candidate.event_kind,
                   candidate.delivery_status,
                   (
                     (candidate.event_kind='ALERT'
                      AND candidate.delivery_status='DELIVERED')
                     OR EXISTS (
                       SELECT 1
                       FROM research_prospective_shadow_events authorized
                       WHERE authorized.event_id=candidate.event_id
                     )
                   ) AS shadow_eligible
            FROM research_events candidate
            WHERE event_id=ANY(%s)
            """,
            (requested_event_ids,),
        ).fetchall()
        event_delivery = {
            int(row["event_id"]): (
                str(row["event_kind"]),
                str(row["delivery_status"]),
                bool(row["shadow_eligible"]),
            )
            for row in event_rows
        }
        for result in results:
            event_id = int(result["event_id"])
            event_kind, event_delivery_status, event_shadow_eligible = event_delivery.get(
                event_id, ("", "", False)
            )
            if not event_shadow_eligible:
                continue
            max_event_id = max(max_event_id, event_id)
            evaluation_status = str(
                result.get("evaluation_status")
                or ("MATCHED" if result.get("matched") else "UNMATCHED")
            ).upper()
            if evaluation_status not in {"MATCHED", "UNMATCHED", "UNEVALUABLE"}:
                evaluation_status = "UNEVALUABLE"
            is_match = evaluation_status == "MATCHED"
            inserted = conn.execute(
                """
                INSERT INTO research_formula_shadow_checks (
                    formula_id, event_id, matched, feature_schema_version,
                    evaluation_status, evaluation_reason, input_snapshot,
                    condition_results, decision_cohort_key,
                    decision_anchor_time_utc
                ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s)
                ON CONFLICT (formula_id, event_id) DO NOTHING
                RETURNING formula_id
                """,
                (
                    formula_id,
                    event_id,
                    is_match,
                    formula["feature_schema_version"],
                    evaluation_status,
                    str(result.get("evaluation_reason") or "")[:1000] or None,
                    _json(result.get("input_snapshot") or {}),
                    _json(result.get("condition_results") or []),
                    result.get("decision_cohort_key"),
                    result.get("decision_anchor_time_utc"),
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
                        result.get("alert_time_utc") or datetime.now(timezone.utc),
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
                            int(formula["horizon_minutes"]),
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
               c.input_snapshot, c.condition_results,
               c.decision_cohort_key, c.decision_anchor_time_utc,
               e.event_id, e.alert_time_utc, e.symbol, e.event_type,
               e.direction, e.setup_key,
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
               ft.qualifying_candle_order_ambiguous,
               o.time_to_mfe_seconds,
               o.target_progress_ratio, o.target_reached
        FROM research_formula_shadow_checks c
        JOIN research_events e ON e.event_id=c.event_id
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
        ),
    ).fetchall()
    return [dict(row) for row in rows]


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

    Formula matches are selected first because they are the target population.
    Unmatched controls are then retained only when they overlap neither a
    retained match nor an earlier retained control for the same symbol.  The
    selection first collapses exact decision cohorts, then uses their frozen
    decision anchors. It never reads MFE, MAE, return or outcome availability.
    """
    horizon = timedelta(minutes=int(horizon_minutes))
    eligible = sorted(
        (
            dict(row)
            for row in rows
            if str(row.get("evaluation_status") or "").upper()
            in {"MATCHED", "UNMATCHED"}
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
    for row in eligible:
        symbol = str(row.get("symbol") or "").upper()
        cohort_key = str(row.get("decision_cohort_key") or f"event:{row['event_id']}")
        exact_cohorts.setdefault((symbol, cohort_key), []).append(row)
    collapsed: list[Dict[str, Any]] = []
    exact_cohort_exclusions: list[int] = []
    for cohort_rows in exact_cohorts.values():
        ordered = sorted(
            cohort_rows,
            key=lambda row: (
                0
                if str(row.get("evaluation_status") or "").upper() == "MATCHED"
                else 1,
                _as_utc(row["alert_time_utc"]),
                int(row["event_id"]),
            ),
        )
        collapsed.append(ordered[0])
        exact_cohort_exclusions.extend(int(row["event_id"]) for row in ordered[1:])
    eligible = sorted(
        collapsed,
        key=lambda row: (
            _as_utc(
                row.get("decision_anchor_time_utc") or row["alert_time_utc"]
            ),
            _as_utc(row["alert_time_utc"]),
            int(row["event_id"]),
        ),
    )
    retained_matches: list[Dict[str, Any]] = []
    retained_controls: list[Dict[str, Any]] = []
    excluded_matches: list[int] = []
    excluded_controls: list[int] = []

    def overlaps_retained(
        candidate: Mapping[str, Any], retained: Sequence[Mapping[str, Any]]
    ) -> bool:
        symbol = str(candidate.get("symbol") or "").upper()
        start = _as_utc(
            candidate.get("decision_anchor_time_utc")
            or candidate["alert_time_utc"]
        )
        return any(
            str(other.get("symbol") or "").upper() == symbol
            and _windows_overlap(
                start,
                _as_utc(
                    other.get("decision_anchor_time_utc")
                    or other["alert_time_utc"]
                ),
                horizon,
            )
            for other in retained
        )

    for row in eligible:
        if str(row.get("evaluation_status") or "").upper() != "MATCHED":
            continue
        if overlaps_retained(row, retained_matches):
            excluded_matches.append(int(row["event_id"]))
        else:
            retained_matches.append(row)

    for row in eligible:
        if str(row.get("evaluation_status") or "").upper() != "UNMATCHED":
            continue
        if overlaps_retained(row, retained_matches) or overlaps_retained(
            row, retained_controls
        ):
            excluded_controls.append(int(row["event_id"]))
        else:
            retained_controls.append(row)

    return {
        "rows": retained_matches + retained_controls,
        "matches": retained_matches,
        "controls": retained_controls,
        "excluded_match_event_ids": excluded_matches,
        "excluded_control_event_ids": excluded_controls,
        "exact_cohort_excluded_event_ids": sorted(exact_cohort_exclusions),
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
            "symbol": source.get("symbol"),
            "event_type": source.get("event_type"),
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
    """Build one side-effect-free v6 prospective readiness snapshot.

    Rolling monitoring and the manual owner-approval transaction deliberately
    call this same function.  It consumes terminal first-touch labels, keeps
    full-horizon MAE diagnostic-only, and accepts silent anchors only after the
    authoritative migration-008 view admitted them upstream.
    """
    horizon_minutes = int(formula["horizon_minutes"])
    independent = _select_independent_shadow_rows(
        source_rows, horizon_minutes=horizon_minutes
    )
    independent_rows = list(independent["rows"])
    complete_rows = [
        row for row in independent_rows if bool(row.get("outcome_available"))
    ]
    universe = [
        _metric_row(row, horizon_minutes=horizon_minutes)
        for row in complete_rows
    ]
    selected = [
        _metric_row(row, horizon_minutes=horizon_minutes)
        for row in complete_rows
        if str(row.get("evaluation_status") or "").upper() == "MATCHED"
    ]
    metrics = research_formula_engine.summarize_outcomes(selected, universe)
    priority = research_formula_engine.rank_prospective_metrics(
        metrics, horizon_minutes=horizon_minutes
    )
    improvement = metrics.get("session_hit_rate_improvement_pct_points")
    if improvement is None:
        improvement = metrics.get("hit_rate_improvement_pct_points")
    movement_percentile = metrics.get("session_adjusted_mfe_percentile_pct")
    if movement_percentile is None:
        movement_percentile = metrics.get("median_mfe_percentile_pct")
    raw_status_counts = {
        status: sum(
            1
            for row in source_rows
            if str(row.get("evaluation_status") or "").upper() == status
        )
        for status in ("MATCHED", "UNMATCHED", "UNEVALUABLE")
    }
    pending_outcome_event_ids = [
        int(row["event_id"])
        for row in independent_rows
        if not bool(row.get("outcome_available"))
        and not bool(row.get("outcome_due"))
    ]
    overdue_outcome_event_ids = [
        int(row["event_id"])
        for row in independent_rows
        if not bool(row.get("outcome_available"))
        and bool(row.get("outcome_due"))
    ]
    early_first_touch_terminal_event_ids = [
        int(row["event_id"])
        for row in independent_rows
        if bool(row.get("first_touch_available"))
    ]
    early_first_touch_hit_event_ids = [
        int(row["event_id"])
        for row in independent_rows
        if bool(row.get("first_touch_available"))
        and bool(row.get("first_touch_hit"))
    ]
    early_matched_first_touch_hit_event_ids = [
        int(row["event_id"])
        for row in independent_rows
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
        "future MFE/MAE efficiency": float(
            metrics.get("median_mfe_mae_ratio") or 0.0
        )
        >= 1.50,
        "future favorable exceeds p90 adverse": float(
            metrics.get("favorable_minus_p90_adverse_pct") or -999.0
        )
        > 0.0,
    }
    return {
        "policy_version": _SHADOW_MONITORING_POLICY_VERSION,
        "outcome_method_version": research_feature_matrix.VERIFIED_OUTCOME_METHOD,
        "evaluated_at_utc": evaluated_at_utc or datetime.now(timezone.utc),
        "stage_ceiling": "SHADOW_PENDING_EXPLICIT_APPROVAL",
        "statistical_use": (
            "rolling descriptive monitoring only; explicit owner approval "
            "requires a separately frozen prospective review"
        ),
        "sampling_frame": (
            "new delivered bot ALERT rows plus authorized atomic silent "
            "DECISION_SAMPLE LONG/SHORT anchors; controls are evaluable formula "
            "nonmatches within that prospective population"
        ),
        "metrics": metrics,
        "priority_ranking": priority,
        "evidence": {
            "raw_checks": len(source_rows),
            "raw_evaluation_status": raw_status_counts,
            "independent_matches": len(independent["matches"]),
            "independent_controls": len(independent["controls"]),
            "correlated_match_exclusions": len(
                independent["excluded_match_event_ids"]
            ),
            "correlated_control_exclusions": len(
                independent["excluded_control_event_ids"]
            ),
            "exact_cohort_exclusions": len(
                independent["exact_cohort_excluded_event_ids"]
            ),
            "pending_outcome_event_ids": pending_outcome_event_ids,
            "overdue_outcome_event_ids": overdue_outcome_event_ids,
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
            "independence_policy": (
                "exact decision cohorts collapse first; same-symbol frozen "
                "decision-anchor windows must not overlap"
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


def evaluate_shadow_readiness() -> Dict[str, Any]:
    """Persist rolling prospective metrics without changing a formula stage.

    These metrics are monitoring evidence only.  Controls are independent
    formula nonmatches drawn from delivered alerts and authoritative neutral
    anchors. Repeated rolling looks are not an approval test. No path in this
    function writes APPROVED/LIVE or ``live_alert_approved``.
    """
    evaluated = 0
    thresholds_met: list[int] = []
    ranked: list[Dict[str, Any]] = []
    with _connect(read_only=False) as conn:
        formulas = conn.execute(
            """
            SELECT formula_id, formula_version, horizon_minutes,
                   latest_evaluation_run_id, shadow_started_at_utc,
                   last_shadow_event_id
            FROM research_formulas
            WHERE active=TRUE
              AND current_stage='SHADOW'
              AND formula_schema_version=ANY(%s)
            ORDER BY formula_id
            FOR UPDATE
            """,
            (list(_SHADOW_COMPATIBLE_FORMULA_SCHEMAS),),
        ).fetchall()
        for formula in formulas:
            source_rows = _shadow_outcome_rows(conn, formula)
            horizon_minutes = int(formula["horizon_minutes"])
            validation = _build_shadow_validation(
                formula,
                source_rows,
                evaluated_at_utc=datetime.now(timezone.utc),
            )
            priority = dict(validation["priority_ranking"])
            evaluated += 1
            formula_id = int(formula["formula_id"])
            ranked.append(
                {
                    "formula_id": formula_id,
                    "horizon_minutes": horizon_minutes,
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
        conn.commit()
    ranked.sort(key=lambda item: (float(item["score"]), -int(item["formula_id"])), reverse=True)
    for rank, item in enumerate(ranked, start=1):
        item["rank"] = rank
    return {
        "policy_version": _SHADOW_MONITORING_POLICY_VERSION,
        "evaluated": evaluated,
        "thresholds_met": thresholds_met,
        "ready_for_explicit_review": thresholds_met,
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
