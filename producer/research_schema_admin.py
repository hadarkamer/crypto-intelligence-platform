"""One-shot schema installer for the Research Archive.

This module is intentionally NOT imported by Watch, Telegram, or recurring jobs.
It can only apply the migration when explicit safety gates are present:

- RESEARCH_SCHEMA_APPLY=1
- and either:
  - RESEARCH_DATABASE_URL=<approved database URL>, or
  - RESEARCH_USE_PRIMARY_DATABASE=1 with the existing DATABASE_URL.

The primary database path is never implicit. Normal candidate/production runtime
never calls this file.
"""
from __future__ import annotations

import os
from pathlib import Path

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None

_TRUE = {"1", "true", "yes", "on"}
MIGRATION_PATH = Path(__file__).resolve().parent / "migrations" / "001_research_archive_v1.sql"
SCHEMA_LOCK_ID = 94837241


def _enabled() -> bool:
    return os.getenv("RESEARCH_SCHEMA_APPLY", "").strip().lower() in _TRUE


def _use_primary() -> bool:
    return os.getenv("RESEARCH_USE_PRIMARY_DATABASE", "").strip().lower() in _TRUE


def _database_url() -> tuple[str, str | None]:
    dedicated = os.getenv("RESEARCH_DATABASE_URL", "").strip()
    if dedicated:
        return dedicated, "RESEARCH_DATABASE_URL"
    if _use_primary():
        primary = os.getenv("DATABASE_URL", "").strip()
        if primary:
            return primary, "DATABASE_URL_EXPLICIT_PRIMARY"
    return "", None


def status() -> dict:
    database_url, source = _database_url()
    return {
        "schema_apply_enabled": _enabled(),
        "database_configured": bool(database_url),
        "database_source": source,
        "explicit_primary_database": _use_primary(),
        "migration_path": str(MIGRATION_PATH),
        "runtime_imported_by_watch": False,
    }


def apply_schema() -> None:
    if not _enabled():
        raise RuntimeError("Refusing schema mutation: set RESEARCH_SCHEMA_APPLY=1 explicitly")
    database_url, source = _database_url()
    if not database_url:
        raise RuntimeError(
            "Refusing schema mutation: provide RESEARCH_DATABASE_URL or explicitly set "
            "RESEARCH_USE_PRIMARY_DATABASE=1 with DATABASE_URL"
        )
    if psycopg is None:
        raise RuntimeError("psycopg is unavailable")
    if not MIGRATION_PATH.exists():
        raise RuntimeError(f"Migration file not found: {MIGRATION_PATH}")

    sql = MIGRATION_PATH.read_text(encoding="utf-8")
    with psycopg.connect(database_url, connect_timeout=5) as conn:
        # Serialize accidental concurrent manual invocations. This lock exists
        # only for the one-shot migration process, never inside Watch.
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (SCHEMA_LOCK_ID,))
        conn.execute(sql)
        conn.commit()

    print(f"Research Archive v1 schema applied successfully via {source}.", flush=True)


if __name__ == "__main__":
    apply_schema()
