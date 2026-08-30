"""Explicit one-shot installer for all Formula Research migrations."""

from __future__ import annotations

import os
from pathlib import Path

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None


_TRUE = {"1", "true", "yes", "on"}
MIGRATION_PATHS = (
    Path(__file__).resolve().parent / "migrations" / "001_research_archive_v1.sql",
    Path(__file__).resolve().parent / "migrations" / "002_formula_research_v1.sql",
    Path(__file__).resolve().parent / "migrations" / "003_formula_autonomous_alerts_v1.sql",
    Path(__file__).resolve().parent
    / "migrations"
    / "004_historical_opportunity_replay_v1.sql",
    Path(__file__).resolve().parent
    / "migrations"
    / "005_formula_shadow_safety_v1.sql",
    Path(__file__).resolve().parent
    / "migrations"
    / "006_no_dwell_first_touch_outcomes_v6.sql",
    Path(__file__).resolve().parent
    / "migrations"
    / "007_max_pain_watch_archive_v1.sql",
    Path(__file__).resolve().parent
    / "migrations"
    / "008_prospective_neutral_anchors_v1.sql",
    Path(__file__).resolve().parent
    / "migrations"
    / "009_formula_owner_live_approval_v1.sql",
    Path(__file__).resolve().parent
    / "migrations"
    / "010_prospective_max_pain_freeze_v1.sql",
    Path(__file__).resolve().parent
    / "migrations"
    / "011_formula_owner_live_engine_binding_v2.sql",
    Path(__file__).resolve().parent
    / "migrations"
    / "012_historical_replay_v2_streaming_index.sql",
    Path(__file__).resolve().parent
    / "migrations"
    / "013_prospective_decision_feature_freeze_v1.sql",
    Path(__file__).resolve().parent
    / "migrations"
    / "014_outcome_rejection_audit_v1.sql",
)
SCHEMA_LOCK_ID = 94837242


def _enabled() -> bool:
    return os.getenv("FORMULA_SCHEMA_APPLY", "").strip().lower() in _TRUE


def _database_url() -> tuple[str, str | None]:
    dedicated = os.getenv("RESEARCH_DATABASE_URL", "").strip()
    if dedicated:
        return dedicated, "RESEARCH_DATABASE_URL"
    use_primary = os.getenv("RESEARCH_USE_PRIMARY_DATABASE", "").strip().lower() in _TRUE
    primary = os.getenv("DATABASE_URL", "").strip()
    if use_primary and primary:
        return primary, "DATABASE_URL_EXPLICIT_PRIMARY"
    return "", None


def status() -> dict:
    database_url, source = _database_url()
    return {
        "schema_apply_enabled": _enabled(),
        "database_configured": bool(database_url),
        "database_source": source,
        "migration_paths": [str(path) for path in MIGRATION_PATHS],
        "runtime_imported_by_watch": False,
    }


def apply_schema() -> None:
    if not _enabled():
        raise RuntimeError("Refusing schema mutation: set FORMULA_SCHEMA_APPLY=1 explicitly")
    database_url, source = _database_url()
    if not database_url:
        raise RuntimeError(
            "Refusing schema mutation: configure RESEARCH_DATABASE_URL or explicitly "
            "set RESEARCH_USE_PRIMARY_DATABASE=1 with DATABASE_URL"
        )
    if psycopg is None:
        raise RuntimeError("psycopg is unavailable")
    missing = [str(path) for path in MIGRATION_PATHS if not path.exists()]
    if missing:
        raise RuntimeError(f"Migration files not found: {missing}")
    with psycopg.connect(database_url, connect_timeout=5) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (SCHEMA_LOCK_ID,))
        for path in MIGRATION_PATHS:
            conn.execute(path.read_text(encoding="utf-8"))
        conn.commit()
    print(f"Formula Research schema applied successfully via {source}.", flush=True)


if __name__ == "__main__":
    apply_schema()
