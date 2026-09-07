"""Explicit, idempotent persistence of a reviewed source-staging manifest.

No LIVE event, price label, formula or candidate eligibility is created here.
The caller installs migration 028 separately and opts in to the database.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sqlite3
from typing import Any, Mapping

from research_telegram_export_import import _database_url
from research_telegram_html_archive import (
    PARSER_VERSION, SCOPE_VERSION, _hash, classify, normalize_time, scope_membership, stage_digest,
)

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None


def load_stage(directory: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    manifest = json.loads((directory / "archive_manifest.json").read_text(encoding="utf-8"))
    with (directory / "archive_source_messages.jsonl").open(encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle if line.strip()]
    validate_stage(manifest, rows)
    return manifest, rows


def validate_stage(manifest: Mapping[str, Any], rows: list[dict[str, Any]]) -> None:
    if manifest.get("prepared_stage_digest") != stage_digest(manifest, rows):
        raise ValueError("Prepared stage digest does not match source annotations and manifest")
    if manifest.get("parser_version") != PARSER_VERSION or manifest.get("scope_version") != SCOPE_VERSION:
        raise ValueError("Unknown source staging version")
    if manifest.get("source_revision_rows") != len(rows):
        raise ValueError("Source row count does not match manifest")
    identities = set()
    for row in rows:
        if row.get("candidate_eligible") is not False or row.get("training_eligible") is not False:
            raise ValueError("Archive staging may not contain eligible formula evidence")
        if row.get("record_mode") != "ARCHIVE" or row.get("source_kind") != "TELEGRAM_DESKTOP_HTML_ARCHIVE":
            raise ValueError("Archive staging may not contain other sources or LIVE rows")
        if any(row.get(field) is not None for field in ("outcome_method_version", "btc_parent_movement_id", "watch_scan_id", "analysis_direction")):
            raise ValueError("Staging cannot assign outcomes or market waves")
        if row.get("event_time_verified", False) is not False:
            raise ValueError("Source staging cannot promote a timestamp to verified")
        if row.get("parser_version") != PARSER_VERSION or row.get("scope_version") != SCOPE_VERSION or row.get("source_filter") != "ARCHIVE_ONLY" or row.get("source_scope") != "ARCHIVE_ONLY":
            raise ValueError("Source row uses an unknown parser, scope or source filter")
        if row.get("message_family") != classify(row["message_text"]):
            raise ValueError("Message family differs from source classification")
        if row["source_chat_key"] != manifest["source_chat_key"]:
            raise ValueError("Source chat differs from manifest")
        identity = _hash("telegram-message-identity-v1", row["source_chat_key"], row["source_message_id"])
        revision = _hash(row["raw_time_title"], row["message_text"])
        if identity != row["source_identity_key"] or revision != row["source_revision_sha256"]:
            raise ValueError("Source identity or text revision digest mismatch")
        if (identity, revision) in identities:
            raise ValueError("Duplicate source revision in staging input")
        identities.add((identity, revision))
        timing = normalize_time(row["raw_time_title"], manifest["time_policy"])
        if row.get("normalization_version") != manifest["time_policy"]:
            raise ValueError("Mixed time normalization policies")
        for field in ("normalized_message_time_utc", "normalized_message_time_israel", "header_message_time_utc", "raw_utc_offset", "time_status"):
            if row.get(field) != timing.get(field):
                raise ValueError("Proposed timestamp does not match the explicit time policy")
        if row["period_scope_ids"] != scope_membership(timing.get("normalized_message_time_utc")):
            raise ValueError("Date-period membership differs from the proposed source time")
        if timing.get("normalized_message_time_utc") and not row["period_scope_ids"]:
            raise ValueError("Pre-August-16 source cannot enter this archive intake")
    if _hash(sorted(identities)) != manifest.get("archive_revision_digest"):
        raise ValueError("Archive revision digest does not match manifest")
    if any(manifest.get(field) != 0 for field in ("formula_evidence_rows", "v7_outcomes_created", "live_events_created", "independent_waves_created")):
        raise ValueError("Staging manifest may not claim reconstructed evidence")


def intake_key(manifest: Mapping[str, Any]) -> str:
    return _hash(
        "telegram-archive-intake-v1", manifest["archive_revision_digest"],
        manifest["parser_version"], manifest["scope_version"],
        manifest["time_policy"], manifest.get("existing_import_sha256"),
        manifest.get("source_files"), manifest["prepared_stage_digest"],
    )


def _validate_apply(manifest: dict[str, Any], rows: list[dict[str, Any]], expected_digest: str, expected_stage_digest: str, batch_size: int) -> None:
    validate_stage(manifest, rows)
    if expected_digest != manifest["archive_revision_digest"]:
        raise ValueError("Expected digest does not match the prepared archive")
    if expected_stage_digest != manifest["prepared_stage_digest"]:
        raise ValueError("Expected prepared stage digest does not match the reviewed intake")
    if not 1 <= batch_size <= 1000:
        raise ValueError("batch_size must be between 1 and 1000")


def _verify_persisted(manifest: dict[str, Any], records: Any) -> None:
    annotations = []
    for annotation, identity, revision, raw_time, message_text in records:
        row = json.loads(annotation) if isinstance(annotation, str) else annotation
        if (row["source_identity_key"], row["source_revision_sha256"], row["raw_time_title"], row["message_text"]) != (identity, revision, raw_time, message_text):
            raise ValueError("Persisted raw source does not match its reviewed annotation")
        annotations.append(row)
    validate_stage(manifest, annotations)


def apply_stage(manifest: dict[str, Any], rows: list[dict[str, Any]], *, expected_digest: str, expected_stage_digest: str, batch_size: int = 250) -> dict[str, Any]:
    _validate_apply(manifest, rows, expected_digest, expected_stage_digest, batch_size)
    if os.getenv("RESEARCH_ARCHIVE_IMPORT_APPLY", "").strip().lower() not in {"1", "true", "yes", "on"}:
        raise RuntimeError("Set RESEARCH_ARCHIVE_IMPORT_APPLY=1 to persist isolated archive sources")
    database_url = _database_url()
    if not database_url or psycopg is None:
        raise RuntimeError("An explicitly configured research database and psycopg are required")
    key = intake_key(manifest)
    new_sources = new_members = 0
    with psycopg.connect(database_url, connect_timeout=5, options="-c statement_timeout=15000 -c lock_timeout=1000") as conn:
        conn.execute(
            """INSERT INTO research_archive_intake_batches (
                intake_batch_key, archive_revision_digest, parser_version,
                normalization_version, source_chat_key, manifest, expected_source_revision_rows
            ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)
            ON CONFLICT (intake_batch_key) DO NOTHING""",
            (key, manifest["archive_revision_digest"], manifest["parser_version"], manifest["time_policy"], manifest["source_chat_key"], json.dumps(manifest, ensure_ascii=False), len(rows)),
        )
        conn.commit()
        for offset in range(0, len(rows), batch_size):
            # Two bounded statements per batch avoid one network trip per field
            # or message; committing each batch makes interrupted runs resumable.
            encoded = json.dumps(rows[offset:offset + batch_size], ensure_ascii=False)
            result = conn.execute(
                """INSERT INTO research_archive_source_messages (
                    source_identity_key, source_revision_sha256, source_chat_key,
                    source_message_id, source_kind, message_text, raw_time_title,
                    header_message_time_utc
                ) SELECT r->>'source_identity_key', r->>'source_revision_sha256',
                    r->>'source_chat_key', r->>'source_message_id', r->>'source_kind',
                    r->>'message_text', r->>'raw_time_title',
                    (r->>'header_message_time_utc')::timestamptz
                FROM jsonb_array_elements(%s::jsonb) AS r
                ON CONFLICT (source_identity_key, source_revision_sha256) DO NOTHING""",
                (encoded,),
            )
            new_sources += result.rowcount
            result = conn.execute(
                """INSERT INTO research_archive_intake_members (
                    intake_batch_key, source_identity_key, source_revision_sha256,
                    proposed_message_time_utc, normalization_version, time_status,
                    message_family, period_scope_ids, source_annotation
                ) SELECT %s, r->>'source_identity_key', r->>'source_revision_sha256',
                    (r->>'normalized_message_time_utc')::timestamptz,
                    r->>'normalization_version', r->>'time_status',
                    r->>'message_family', r->'period_scope_ids',
                    CASE WHEN EXISTS (
                        SELECT 1 FROM research_archive_source_messages s
                        WHERE s.source_identity_key = r->>'source_identity_key'
                          AND s.source_revision_sha256 <> r->>'source_revision_sha256'
                    ) THEN r || '{"identity_status":"SOURCE_ID_REVISION_CONFLICT","import_action":"QUARANTINE_REVISION_CONFLICT"}'::jsonb
                    ELSE r END
                FROM jsonb_array_elements(%s::jsonb) AS r
                ON CONFLICT (intake_batch_key, source_identity_key, source_revision_sha256) DO NOTHING""",
                (key, encoded),
            )
            new_members += result.rowcount
            conn.commit()
        persisted = conn.execute("SELECT COUNT(*) FROM research_archive_intake_members WHERE intake_batch_key = %s", (key,)).fetchone()[0]
        if persisted != len(rows):
            raise RuntimeError("Archive intake member count failed verification")
        _verify_persisted(manifest, conn.execute("""SELECT m.source_annotation, s.source_identity_key,
            s.source_revision_sha256, s.raw_time_title, s.message_text
            FROM research_archive_intake_members m JOIN research_archive_source_messages s
            USING (source_identity_key, source_revision_sha256)
            WHERE m.intake_batch_key = %s""", (key,)))
        conn.execute("UPDATE research_archive_intake_batches SET intake_status = 'COMPLETE', completed_at_utc = COALESCE(completed_at_utc, NOW()) WHERE intake_batch_key = %s", (key,))
        conn.commit()
    return {"applied": True, "intake_batch_key": key, "source_rows_inserted": new_sources, "batch_members_inserted": new_members, "persisted_batch_members": persisted, "candidate_eligible": False, "canonical_times_promoted": 0}


def apply_sqlite_stage(
    database_path: Path, manifest: dict[str, Any], rows: list[dict[str, Any]], *,
    expected_digest: str, expected_stage_digest: str, batch_size: int = 250,
) -> dict[str, Any]:
    """Persist an isolated portable intake without production credentials.

    This is a source archive, not a replica of the research event database.
    A later PostgreSQL apply uses the same manifest and immutable identities.
    """
    _validate_apply(manifest, rows, expected_digest, expected_stage_digest, batch_size)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    key = intake_key(manifest)
    sources_added = members_added = 0
    with sqlite3.connect(database_path, timeout=15) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS archive_intake_batches (
                intake_batch_key TEXT PRIMARY KEY,
                archive_revision_digest TEXT NOT NULL,
                prepared_stage_digest TEXT NOT NULL,
                manifest_json TEXT NOT NULL,
                expected_source_revision_rows INTEGER NOT NULL,
                intake_status TEXT NOT NULL DEFAULT 'INGESTING'
                    CHECK (intake_status IN ('INGESTING', 'COMPLETE'))
            );
            CREATE TABLE IF NOT EXISTS archive_source_messages (
                source_identity_key TEXT NOT NULL,
                source_revision_sha256 TEXT NOT NULL,
                source_chat_key TEXT NOT NULL,
                source_message_id TEXT NOT NULL,
                raw_time_title TEXT NOT NULL,
                message_text TEXT NOT NULL,
                canonical_message_time_utc TEXT CHECK (canonical_message_time_utc IS NULL),
                candidate_eligible INTEGER NOT NULL DEFAULT 0 CHECK (candidate_eligible = 0),
                PRIMARY KEY (source_identity_key, source_revision_sha256)
            );
            CREATE TABLE IF NOT EXISTS archive_intake_members (
                intake_batch_key TEXT NOT NULL REFERENCES archive_intake_batches(intake_batch_key),
                source_identity_key TEXT NOT NULL,
                source_revision_sha256 TEXT NOT NULL,
                proposed_message_time_utc TEXT,
                normalization_version TEXT NOT NULL,
                time_status TEXT NOT NULL,
                message_family TEXT NOT NULL,
                period_scope_ids_json TEXT NOT NULL,
                source_annotation_json TEXT NOT NULL,
                PRIMARY KEY (intake_batch_key, source_identity_key, source_revision_sha256),
                FOREIGN KEY (source_identity_key, source_revision_sha256)
                    REFERENCES archive_source_messages(source_identity_key, source_revision_sha256)
            );
            CREATE INDEX IF NOT EXISTS archive_intake_time
                ON archive_intake_members(intake_batch_key, proposed_message_time_utc);
        """)
        conn.execute(
            "INSERT OR IGNORE INTO archive_intake_batches (intake_batch_key, archive_revision_digest, prepared_stage_digest, manifest_json, expected_source_revision_rows) VALUES (?, ?, ?, ?, ?)",
            (key, manifest["archive_revision_digest"], manifest["prepared_stage_digest"], json.dumps(manifest, ensure_ascii=False), len(rows)),
        )
        conn.commit()
        for offset in range(0, len(rows), batch_size):
            batch = rows[offset:offset + batch_size]
            before = conn.total_changes
            conn.executemany(
                "INSERT OR IGNORE INTO archive_source_messages (source_identity_key, source_revision_sha256, source_chat_key, source_message_id, raw_time_title, message_text) VALUES (?, ?, ?, ?, ?, ?)",
                [(row["source_identity_key"], row["source_revision_sha256"], row["source_chat_key"], row["source_message_id"], row["raw_time_title"], row["message_text"]) for row in batch],
            )
            sources_added += conn.total_changes - before
            before = conn.total_changes
            conn.executemany(
                "INSERT OR IGNORE INTO archive_intake_members (intake_batch_key, source_identity_key, source_revision_sha256, proposed_message_time_utc, normalization_version, time_status, message_family, period_scope_ids_json, source_annotation_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [(key, row["source_identity_key"], row["source_revision_sha256"], row.get("normalized_message_time_utc"), row["normalization_version"], row["time_status"], row["message_family"], json.dumps(row["period_scope_ids"]), json.dumps(row, ensure_ascii=False)) for row in batch],
            )
            members_added += conn.total_changes - before
            conn.commit()
        persisted = conn.execute("SELECT COUNT(*) FROM archive_intake_members WHERE intake_batch_key = ?", (key,)).fetchone()[0]
        if persisted != len(rows):
            raise RuntimeError("Archive intake member count failed verification")
        _verify_persisted(manifest, conn.execute("""SELECT m.source_annotation_json, s.source_identity_key,
            s.source_revision_sha256, s.raw_time_title, s.message_text
            FROM archive_intake_members m JOIN archive_source_messages s
            USING (source_identity_key, source_revision_sha256)
            WHERE m.intake_batch_key = ?""", (key,)))
        if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok" or conn.execute("PRAGMA foreign_key_check").fetchone():
            raise RuntimeError("Archive database integrity verification failed")
        conn.execute("UPDATE archive_intake_batches SET intake_status = 'COMPLETE' WHERE intake_batch_key = ?", (key,))
        conn.commit()
    return {"applied": True, "storage": "ISOLATED_SQLITE_SOURCE_ARCHIVE", "intake_batch_key": key, "source_rows_inserted": sources_added, "batch_members_inserted": members_added, "persisted_batch_members": persisted, "candidate_eligible": False, "canonical_times_promoted": 0, "production_rows_inserted": 0}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage-input-dir", type=Path, required=True)
    parser.add_argument("--apply-staging", action="store_true")
    parser.add_argument("--expected-archive-digest")
    parser.add_argument("--expected-stage-digest")
    parser.add_argument("--sqlite-path", type=Path, help="Persist a portable source archive instead of connecting to PostgreSQL")
    parser.add_argument("--batch-size", type=int, default=250)
    args = parser.parse_args()
    manifest, rows = load_stage(args.stage_input_dir)
    if args.apply_staging:
        if not args.expected_archive_digest or not args.expected_stage_digest:
            parser.error("--apply-staging requires both --expected-archive-digest and --expected-stage-digest")
        apply_args = dict(expected_digest=args.expected_archive_digest, expected_stage_digest=args.expected_stage_digest, batch_size=args.batch_size)
        result = apply_sqlite_stage(args.sqlite_path, manifest, rows, **apply_args) if args.sqlite_path else apply_stage(manifest, rows, **apply_args)
    else:
        result = {"applied": False, "validated_source_revision_rows": len(rows), "intake_batch_key": intake_key(manifest), "archive_revision_digest": manifest["archive_revision_digest"], "prepared_stage_digest": manifest["prepared_stage_digest"], "candidate_eligible": False}
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
