"""Explicit one-shot installer for Formula Research v1 tables."""

from __future__ import annotations

import os
from pathlib import Path

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None


_TRUE = {"1", "true", "yes", "on"}
MIGRATION_PATH = Path(__file__).resolve().parent / "migrations" / "002_formula_research_v1.sql"
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
        "migration_path": str(MIGRATION_PATH),
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
    if not MIGRATION_PATH.exists():
        raise RuntimeError(f"Migration file not found: {MIGRATION_PATH}")
    sql = MIGRATION_PATH.read_text(encoding="utf-8")
    with psycopg.connect(database_url, connect_timeout=5) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (SCHEMA_LOCK_ID,))
        conn.execute(sql)
        conn.commit()
    print(f"Formula Research v1 schema applied successfully via {source}.", flush=True)


if __name__ == "__main__":
    apply_schema()
