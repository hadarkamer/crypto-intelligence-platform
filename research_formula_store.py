"""Auditable PostgreSQL registry for discovered and shadow-tested formulas."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from typing import Any, Dict, Mapping, Optional, Sequence

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None

import research_feature_matrix
import research_formula_engine


_TRUE = {"1", "true", "yes", "on"}
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
_AUTONOMOUS_POLICY_VERSION = (
    os.getenv("FORMULA_AUTONOMOUS_ALERT_POLICY_VERSION", "").strip()
    or "owner-policy-v3-session-composition-2026-08-29"
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
    )
    base = {
        "configured": bool(_database_url()),
        "schema_present": False,
        "missing_tables": list(required),
    }
    if not base["configured"] or psycopg is None:
        return base
    with _connect(read_only=True) as conn:
        missing = [table for table in required if not _table_exists(conn, table)]
        base["missing_tables"] = missing
        base["schema_present"] = not missing
        if missing:
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
              AND current_stage NOT IN ('LIVE', 'RETIRED')
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
                        "superseded by safe historical-replay formula schema v5 "
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
                  AND current_stage NOT IN ('LIVE', 'RETIRED')
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
            "automatic_stage_ceiling": "LIVE_AFTER_FUTURE_SHADOW_POLICY",
            "live_alerts_require": [
                f"owner-approved policy {_AUTONOMOUS_POLICY_VERSION}",
                "strict chronological holdout plus future Shadow validation",
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
            SELECT f.formula_id, f.formula_version, f.formula_text,
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
              AND f.formula_schema_version=%s
            ORDER BY
              CASE WHEN f.current_stage='LIVE' THEN 1 ELSE 0 END DESC,
              e.ranking_score DESC NULLS LAST,
              f.formula_id
            """,
            (research_formula_engine.FORMULA_SCHEMA_VERSION,),
        ).fetchall()
        for formula in formulas:
            events = conn.execute(
                """
                SELECT event_id, alert_time_utc
                FROM research_events
                WHERE event_kind='ALERT' AND delivery_status='DELIVERED'
                  AND direction=%s
                  AND event_id>%s
                  AND alert_time_utc>=COALESCE(%s, '-infinity'::timestamptz)
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
            SELECT current_stage, active, live_alert_approved
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
        for result in results:
            event_id = int(result["event_id"])
            max_event_id = max(max_event_id, event_id)
            is_match = bool(result.get("matched"))
            inserted = conn.execute(
                """
                INSERT INTO research_formula_shadow_checks (
                    formula_id, event_id, matched, feature_schema_version
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (formula_id, event_id) DO NOTHING
                RETURNING formula_id
                """,
                (
                    formula_id,
                    event_id,
                    is_match,
                    formula["feature_schema_version"],
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
                    current["current_stage"] == "LIVE"
                    and bool(current["live_alert_approved"])
                ):
                    inserted_deliveries = conn.execute(
                        """
                        INSERT INTO research_formula_live_deliveries (
                            formula_id, event_id, chat_id, status
                        )
                        SELECT %s, %s, s.chat_id, 'PENDING'
                        FROM research_formula_alert_subscriptions s
                        WHERE s.active=TRUE
                        ON CONFLICT (event_id, chat_id) DO NOTHING
                        RETURNING delivery_id
                        """,
                        (formula_id, event_id),
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
    rows = conn.execute(
        """
        SELECT c.matched,
               e.event_id, e.alert_time_utc, e.symbol, e.event_type,
               o.directional_return_pct, o.mfe_pct, o.mae_pct,
               o.time_to_first_progress_seconds, o.time_to_mfe_seconds,
               o.target_progress_ratio, o.target_reached
        FROM research_formula_shadow_checks c
        JOIN research_events e ON e.event_id=c.event_id
        JOIN research_alert_outcomes o
          ON o.event_id=e.event_id
         AND o.horizon_minutes=%s
         AND o.outcome_method_version=%s
         AND o.data_quality_status=ANY(%s)
        WHERE c.formula_id=%s
        ORDER BY e.alert_time_utc, e.event_id
        """,
        (
            int(formula["horizon_minutes"]),
            research_feature_matrix.VERIFIED_OUTCOME_METHOD,
            list(research_feature_matrix.VERIFIED_OUTCOME_QUALITIES),
            int(formula["formula_id"]),
        ),
    ).fetchall()
    return [dict(row) for row in rows]


def _metric_row(
    source: Mapping[str, Any], *, horizon_minutes: int
) -> Dict[str, Any]:
    return {
        "event": {
            "event_id": int(source["event_id"]),
            "alert_time_utc": source["alert_time_utc"],
            "symbol": source.get("symbol"),
            "event_type": source.get("event_type"),
        },
        "outcome_label": {
            "horizon_minutes": int(horizon_minutes),
            "directional_return_pct": source.get("directional_return_pct"),
            "mfe_pct": source.get("mfe_pct"),
            "mae_pct": source.get("mae_pct"),
            "time_to_first_progress_seconds": source.get(
                "time_to_first_progress_seconds"
            ),
            "time_to_mfe_seconds": source.get("time_to_mfe_seconds"),
            "target_progress_ratio": source.get("target_progress_ratio"),
            "target_reached": source.get("target_reached"),
        },
    }


def promote_eligible_shadow_formulas() -> Dict[str, Any]:
    """Apply the owner-approved deterministic future-Shadow live policy."""
    evaluated = 0
    promoted: list[int] = []
    with _connect(read_only=False) as conn:
        formulas = conn.execute(
            """
            SELECT formula_id, horizon_minutes, latest_evaluation_run_id
            FROM research_formulas
            WHERE active=TRUE
              AND current_stage='SHADOW'
              AND formula_schema_version=%s
            ORDER BY formula_id
            FOR UPDATE
            """,
            (research_formula_engine.FORMULA_SCHEMA_VERSION,),
        ).fetchall()
        for formula in formulas:
            source_rows = _shadow_outcome_rows(conn, formula)
            horizon_minutes = int(formula["horizon_minutes"])
            universe = [
                _metric_row(row, horizon_minutes=horizon_minutes)
                for row in source_rows
            ]
            selected = [
                _metric_row(row, horizon_minutes=horizon_minutes)
                for row in source_rows
                if bool(row.get("matched"))
            ]
            metrics = research_formula_engine.summarize_outcomes(selected, universe)
            evaluated += 1
            improvement = metrics.get("session_hit_rate_improvement_pct_points")
            if improvement is None:
                improvement = metrics.get("hit_rate_improvement_pct_points")
            movement_percentile = metrics.get(
                "session_adjusted_mfe_percentile_pct"
            )
            if movement_percentile is None:
                movement_percentile = metrics.get("median_mfe_percentile_pct")
            gate_results = {
                "matched future outcomes": int(metrics.get("sample_size") or 0)
                >= _SHADOW_MIN_MATCHES,
                "unmatched future controls": int(metrics.get("control_sample_size") or 0)
                >= _SHADOW_MIN_CONTROLS,
                "future temporal span": float(metrics.get("time_span_hours") or 0.0)
                >= float(_SHADOW_MIN_SPAN_HOURS),
                "future UTC dates": int(metrics.get("distinct_utc_dates") or 0)
                >= _SHADOW_MIN_DATES,
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
                    horizon_minutes,
                    metrics,
                ),
                "future wide movement percentile": float(
                    movement_percentile or 0.0
                )
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
            validation = {
                "policy_version": _AUTONOMOUS_POLICY_VERSION,
                "evaluated_at_utc": datetime.now(timezone.utc),
                "metrics": metrics,
                "gates": gate_results,
                "failed_gates": [
                    name for name, passed in gate_results.items() if not passed
                ],
            }
            formula_id = int(formula["formula_id"])
            conn.execute(
                """
                UPDATE research_formulas
                SET shadow_validation_metrics=%s::jsonb, updated_at_utc=NOW()
                WHERE formula_id=%s
                """,
                (_json(validation), formula_id),
            )
            if not all(gate_results.values()):
                continue
            run_id = formula.get("latest_evaluation_run_id")
            conn.execute(
                """
                INSERT INTO research_formula_stage_history (
                    formula_id, run_id, from_stage, to_stage, reason, actor
                ) VALUES (%s, %s, 'SHADOW', 'APPROVED', %s, %s)
                ON CONFLICT (formula_id, run_id, to_stage) DO NOTHING
                """,
                (
                    formula_id,
                    run_id,
                    "owner policy plus deterministic future Shadow gates passed",
                    "automatic-shadow-validator",
                ),
            )
            conn.execute(
                """
                INSERT INTO research_formula_stage_history (
                    formula_id, run_id, from_stage, to_stage, reason, actor
                ) VALUES (%s, %s, 'APPROVED', 'LIVE', %s, %s)
                ON CONFLICT (formula_id, run_id, to_stage) DO NOTHING
                """,
                (
                    formula_id,
                    run_id,
                    f"autonomous alert policy {_AUTONOMOUS_POLICY_VERSION}",
                    "automatic-shadow-validator",
                ),
            )
            conn.execute(
                """
                UPDATE research_formulas
                SET current_stage='LIVE',
                    live_alert_approved=TRUE,
                    live_alert_approved_at_utc=NOW(),
                    live_alert_approved_by=%s,
                    shadow_validated_at_utc=NOW(),
                    live_alert_policy_version=%s,
                    shadow_validation_metrics=%s::jsonb,
                    updated_at_utc=NOW()
                WHERE formula_id=%s AND current_stage='SHADOW'
                """,
                (
                    _AUTONOMOUS_POLICY_VERSION,
                    _AUTONOMOUS_POLICY_VERSION,
                    _json(validation),
                    formula_id,
                ),
            )
            promoted.append(formula_id)
        conn.commit()
    return {
        "policy_version": _AUTONOMOUS_POLICY_VERSION,
        "evaluated": evaluated,
        "promoted": promoted,
    }


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
            (_DELIVERY_MAX_ATTEMPTS, row_limit),
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
