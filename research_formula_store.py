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
              (SELECT COUNT(*) FROM research_formula_shadow_hits) AS shadow_hits,
              (SELECT COUNT(*) FROM research_legacy_alert_messages) AS legacy_messages
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
                _json(dataset.get("coverage") or {}),
            ),
        ).fetchone()
        run_id = int(run["run_id"])
        persisted = 0
        stage_counts: Dict[str, int] = {}
        for global_rank, formula in enumerate(formulas, start=1):
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
                    formula["recommended_stage"],
                    _json(formula.get("gate_notes") or []),
                ),
            )
            final_stage = _advance_stage(
                conn,
                formula_id=formula_id,
                current_stage=current_stage,
                target_stage=formula["recommended_stage"],
                run_id=run_id,
                reason="automatic chronological discovery/holdout evaluation",
            )
            stage_counts[final_stage] = stage_counts.get(final_stage, 0) + 1
            persisted += 1

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
                   (SELECT COUNT(*) FROM research_formula_shadow_checks c
                    WHERE c.formula_id=f.formula_id) AS shadow_checks,
                   (SELECT COUNT(*) FROM research_formula_shadow_hits h
                    WHERE h.formula_id=f.formula_id) AS shadow_hits
            FROM research_formulas f
            LEFT JOIN research_formula_evaluations e
              ON e.run_id=f.latest_evaluation_run_id AND e.formula_id=f.formula_id
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
            "automatic_stage_ceiling": "SHADOW",
            "live_alerts_require": [
                "explicit human approval stored on the formula",
                "FORMULA_LIVE_ALERTS_ENABLED=1",
                "a separately approved Telegram delivery integration",
            ],
            "filters": {
                "stage": normalized_stage,
                "direction": normalized_direction,
                "horizon_minutes": horizon,
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
    return _json_safe(
        {
            "available": True,
            "stages": {row["current_stage"]: int(row["count"] or 0) for row in stages},
            "recent_shadow_hits": [dict(row) for row in recent],
            "delivery": "NOT_SENT",
        }
    )


def load_shadow_work(max_events_per_formula: int = 100) -> list[Dict[str, Any]]:
    """Load formula/event pairs that occurred strictly after Shadow activation."""
    limit = max(1, min(int(max_events_per_formula), 250))
    work: list[Dict[str, Any]] = []
    with _connect(read_only=True) as conn:
        formulas = conn.execute(
            """
            SELECT formula_id, direction, horizon_minutes, conditions,
                   feature_schema_version, last_shadow_event_id,
                   shadow_started_at_utc
            FROM research_formulas
            WHERE active=TRUE AND current_stage='SHADOW'
            ORDER BY formula_id
            """
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
) -> Dict[str, int]:
    if not results:
        return {"checked": 0, "matched": 0}
    formula_id = int(formula["formula_id"])
    checked = 0
    matched = 0
    max_event_id = 0
    with _connect(read_only=False) as conn:
        current = conn.execute(
            """
            SELECT current_stage, active, live_alert_approved
            FROM research_formulas WHERE formula_id=%s FOR UPDATE
            """,
            (formula_id,),
        ).fetchone()
        if not current or current["current_stage"] != "SHADOW" or not current["active"]:
            return {"checked": 0, "matched": 0}
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
    return {"checked": checked, "matched": matched}
