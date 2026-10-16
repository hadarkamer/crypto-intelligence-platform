"""Durable bounded source parts for full-history BTC wave report generations.

Each operation reads at most one parent source body at a time. Final validation
must run in one repeatable-read transaction: it uses bounded memory, while total
verification time grows with the retained history. This module never publishes
reports, sends messages, reads network prices, or commits the caller's transaction.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from uuid import UUID, uuid4

import research_btc_wave_endpoint_report as report


STORAGE_VERSION = "btc-wave-parts-v1"
MAX_PART_BYTES = 16 * 1024 * 1024
SOURCE_START = datetime(2026, 9, 3, 21, 0, tzinfo=timezone.utc)
_PART_METADATA = """generation_id,ordinal,parent_id,parent_start_utc,
    source_digest,source_bytes,report_digest,report_bytes,updated_at_utc"""
_PART_FIELDS = _PART_METADATA + ",source,job,report"


def canonical(value):
    return json.dumps(value, sort_keys=True, default=report._json_default,
                      separators=(",", ":"), allow_nan=False, ensure_ascii=True)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _id(generation):
    value = generation["generation_id"] if isinstance(generation, dict) else generation
    return str(UUID(str(value)))


def _metadata(row):
    if row is None:
        return None
    result = dict(row)
    if "generation_id" in result:
        result["generation_id"] = str(result["generation_id"])
    return result


def schema_ready(conn):
    return bool(conn.execute("""SELECT
        to_regclass('research_btc_wave_report_generations') IS NOT NULL
        AND to_regclass('research_btc_wave_report_parts') IS NOT NULL
        AND (SELECT count(*) FROM pg_attribute
             WHERE attrelid=to_regclass('research_btc_wave_report_parts')
               AND attname IN ('source','job','report') AND atttypid='json'::regtype
               AND NOT attisdropped)=3 AS ready""").fetchone()["ready"])


def _require_snapshot(conn):
    isolation = conn.execute("SHOW transaction_isolation").fetchone()["transaction_isolation"]
    if isolation not in {"repeatable read", "serializable"}:
        raise ValueError("Full-history source operation requires one REPEATABLE READ transaction")


def get_generation(conn, generation):
    value = _metadata(conn.execute("""SELECT generation_id,worker_key,observed_at_utc,
        parent_count,status,created_at_utc FROM research_btc_wave_report_generations
        WHERE generation_id=%s::uuid""", (_id(generation),)).fetchone())
    if value is None:
        raise ValueError("Unknown full-wave report generation")
    return value


def freeze(conn, worker_key, observed_at):
    """Freeze every ordered parent placeholder in the caller's snapshot/commit."""
    _require_snapshot(conn)
    generation_id = str(uuid4())
    observed_at = report.utc(observed_at)
    conn.execute("""INSERT INTO research_btc_wave_report_generations
        (generation_id,worker_key,observed_at_utc,parent_count,status)
        VALUES (%s::uuid,%s,%s,0,'CAPTURED')""", (generation_id, worker_key, observed_at))
    conn.execute("""INSERT INTO research_btc_wave_report_parts
        (generation_id,ordinal,parent_id,parent_start_utc)
        SELECT %s::uuid,(row_number() OVER (ORDER BY start_time_utc,btc_parent_movement_id)-1)::integer,
            btc_parent_movement_id,start_time_utc
        FROM research_btc_parent_movements
        WHERE episode_policy_version=%s AND start_time_utc<=%s
          AND (end_time_utc IS NULL OR end_time_utc>=%s)""",
        (generation_id, report.POLICY_VERSION, observed_at, SOURCE_START))
    conn.execute("""UPDATE research_btc_wave_report_generations SET parent_count=(
        SELECT count(*) FROM research_btc_wave_report_parts WHERE generation_id=%s::uuid)
        WHERE generation_id=%s::uuid""", (generation_id, generation_id))
    return get_generation(conn, generation_id)


create_generation = freeze
load_generation = get_generation


def iter_parts(conn, generation, *, include_payload=True):
    """Keyset one part at a time; never fetchall an entire generation's bodies."""
    fields = _PART_FIELDS if include_payload else _PART_METADATA + ",source IS NOT NULL AS has_source,report IS NOT NULL AS has_report"
    ordinal = -1
    while True:
        row = _metadata(conn.execute(f"""SELECT {fields} FROM research_btc_wave_report_parts
            WHERE generation_id=%s::uuid AND ordinal>%s ORDER BY ordinal LIMIT 1""",
            (_id(generation), ordinal)).fetchone())
        if row is None:
            return
        ordinal = row["ordinal"]
        yield row
        del row


def get_part(conn, generation, ordinal):
    return _metadata(conn.execute(f"""SELECT {_PART_FIELDS} FROM research_btc_wave_report_parts
        WHERE generation_id=%s::uuid AND ordinal=%s""", (_id(generation), ordinal)).fetchone())


def next_unfinished_part(conn, generation):
    return _metadata(conn.execute(f"""SELECT {_PART_FIELDS} FROM research_btc_wave_report_parts
        WHERE generation_id=%s::uuid AND report IS NULL ORDER BY ordinal LIMIT 1""",
        (_id(generation),)).fetchone())


def has_unfinished_parts(conn, generation):
    return bool(conn.execute("""SELECT EXISTS(SELECT 1 FROM research_btc_wave_report_parts
        WHERE generation_id=%s::uuid AND report IS NULL) AS unfinished""",
        (_id(generation),)).fetchone()["unfinished"])


def _clamp_wave(wave, observed_at):
    value = dict(wave)
    if value.get("end_time_utc") and report.utc(value["end_time_utc"]) > observed_at:
        value["end_time_utc"] = None
        value["closing_boundary_verified"] = False
    if value.get("observed_through_utc"):
        value["observed_through_utc"] = min(report.utc(value["observed_through_utc"]),
                                             report.latest_closed_cutoff(observed_at))
    return value


def load_part_source(conn, parent_id, observed_at):
    """Use the exact native bounded loader and PR170 compact/filter contract."""
    from research_btc_wave_report_worker import _compact_source_event
    observed_at = report.utc(observed_at)
    hype_representatives = {}
    def project(event, wave):
        nonlocal hype_representatives
        if (not SOURCE_START <= report.utc(event["alert_time_utc"]) <= observed_at
                or wave["evidence_eligible"] is not True):
            return None
        keep = False
        if str(event.get("symbol") or "").strip().upper() == "HYPE":
            selected = report.select_representatives([*hype_representatives.values(), event])
            hype_representatives = {int(row["event_id"]): row for row in selected}
            keep = int(event["event_id"]) in hype_representatives
        return _compact_source_event(event, retain_hype_snapshot=keep)
    waves, events = report.load_source(conn, [parent_id], event_projector=project)
    wave = _clamp_wave(waves[0], observed_at)
    context = []
    if wave.get("end_time_utc") and wave.get("closing_boundary_verified") is True:
        # Only the fields used by the native closed-cache proof are needed.
        # The successor's later price evolution cannot change this evidence.
        context = [dict(row) for row in conn.execute("""SELECT btc_parent_movement_id,
            episode_policy_version,start_time_utc,evidence_eligible,boundary_reason
            FROM research_btc_parent_movements WHERE episode_policy_version=%s
              AND start_time_utc=%s AND start_time_utc<=%s AND evidence_eligible
              AND boundary_reason='CAUSAL_CLOSE_REVERSAL' LIMIT 2""",
            (report.POLICY_VERSION, wave["end_time_utc"], observed_at)).fetchall()]
        if len(context) != 1:
            raise ValueError("Verified closed parent lacks its exact causal successor")
    return {"waves": [wave], "events": events, "successor_context": context}


load_one_part_source = load_part_source


def cache_source(source):
    """Add bounded successor evidence only for native closed-cache verification."""
    return {"waves": [*source["waves"], *source.get("successor_context", [])], "events": source["events"]}


def _payload(value, label):
    encoded = canonical(value)
    if len(encoded) > MAX_PART_BYTES:
        raise ValueError(f"FULL_WAVE_{label}_PART_EXCEEDS_16_MIB")
    return encoded, hashlib.sha256(encoded.encode()).hexdigest(), len(encoded)


def save_checkpoint(conn, generation, ordinal, *, source, job, completed_report=None):
    """Persist source and cursor/results atomically, without rewriting a source."""
    if len(source.get("waves", [])) != 1 or len(source.get("successor_context", [])) > 1:
        raise ValueError("A source part must contain exactly one parent and bounded successor context")
    source_json, source_hash, source_bytes = _payload(source, "SOURCE")
    stored_job = None
    if job is not None:
        if job.get("source_digest") != source_hash:
            raise ValueError("Checkpoint source digest differs from its frozen parent source")
        if "source" in job and digest(job["source"]) != source_hash:
            raise ValueError("Checkpoint contains a different frozen parent source")
        stored_job = {key: value for key, value in job.items() if key != "source"}
    job_json = _payload(stored_job, "CHECKPOINT")[0] if stored_job is not None else None
    report_json, report_hash, report_bytes = (None, None, None)
    if completed_report is not None:
        if completed_report.get("source_digest") != source_hash:
            raise ValueError("Completed report source digest differs from its frozen parent source")
        report_json, report_hash, report_bytes = _payload(completed_report, "REPORT")
    parent = source["waves"][0]
    updated = conn.execute("""UPDATE research_btc_wave_report_parts part SET source=%s::json,
        source_digest=%s,source_bytes=%s,job=%s::json,report=%s::json,
        report_digest=%s,report_bytes=%s,updated_at_utc=NOW()
        FROM research_btc_wave_report_generations generation
        WHERE part.generation_id=generation.generation_id AND generation.status='CAPTURED'
          AND part.generation_id=%s::uuid AND part.ordinal=%s AND part.parent_id=%s
          AND part.parent_start_utc=%s AND (part.source_digest IS NULL OR part.source_digest=%s)
          AND part.report IS NULL RETURNING part.ordinal""",
        (source_json, source_hash, source_bytes, job_json, report_json, report_hash, report_bytes,
         _id(generation), ordinal, parent["btc_parent_movement_id"], report.utc(parent["start_time_utc"]), source_hash)).fetchone()
    if updated is None:
        raise ValueError("Frozen source part changed, completed, or no longer belongs to an active generation")
    return {"source_digest": source_hash, "source_bytes": source_bytes,
            "report_digest": report_hash, "report_bytes": report_bytes}


def mark_generation(conn, generation, status):
    if status not in {"PUBLISHED", "ABANDONED"}:
        raise ValueError("Only terminal generation transitions are allowed")
    result = conn.execute("""UPDATE research_btc_wave_report_generations SET status=%s
        WHERE generation_id=%s::uuid AND status='CAPTURED' RETURNING generation_id""",
        (status, _id(generation))).fetchone()
    if result is None:
        raise ValueError("Generation is not active")


def _iter_native_parents(conn, observed_at):
    previous_time, previous_id = None, None
    while True:
        row = conn.execute("""SELECT btc_parent_movement_id,start_time_utc
            FROM research_btc_parent_movements
            WHERE episode_policy_version=%s AND start_time_utc<=%s
              AND (end_time_utc IS NULL OR end_time_utc>=%s)
              AND (%s::timestamptz IS NULL OR (start_time_utc,btc_parent_movement_id)>(%s,%s))
            ORDER BY start_time_utc,btc_parent_movement_id LIMIT 1""",
            (report.POLICY_VERSION, observed_at, SOURCE_START, previous_time, previous_time, previous_id)).fetchone()
        if row is None:
            return
        previous_time, previous_id = row["start_time_utc"], row["btc_parent_movement_id"]
        yield dict(row)


def validate_generation(conn, generation):
    """Verify all staged sources against ONE current snapshot, without writes."""
    _require_snapshot(conn)
    generation = get_generation(conn, generation)
    if generation["status"] != "CAPTURED":
        return {"valid": False, "reason": "GENERATION_NOT_CAPTURED"}
    manifest = {"storage_version": STORAGE_VERSION, "generation_id": generation["generation_id"],
        "parent_count": generation["parent_count"], "observed_at_utc": report.utc(generation["observed_at_utc"]).isoformat(),
        "source_start_utc": SOURCE_START.isoformat()}
    stream = hashlib.sha256((canonical(manifest)+"\n").encode())
    count = 0
    # The suspended iterator retains only metadata; payload ownership ends
    # before the native source for this parent is hydrated.
    parts = iter_parts(conn, generation, include_payload=False)
    for parent in _iter_native_parents(conn, generation["observed_at_utc"]):
        metadata = next(parts, None)
        if (metadata is None or metadata["ordinal"] != count or metadata["parent_id"] != parent["btc_parent_movement_id"]
                or report.utc(metadata["parent_start_utc"]) != report.utc(parent["start_time_utc"])):
            return {"valid": False, "reason": "PARENT_POPULATION_CHANGED", "ordinal": count}
        part = get_part(conn, generation, count)
        if part["source"] is None or part["report"] is None:
            return {"valid": False, "reason": "INCOMPLETE_PART", "ordinal": count}
        _, stored_hash, stored_bytes = _payload(part["source"], "SOURCE")
        _, report_hash, report_bytes = _payload(part["report"], "REPORT")
        if (stored_hash != part["source_digest"] or stored_bytes != part["source_bytes"]
                or report_hash != part["report_digest"] or report_bytes != part["report_bytes"]
                or part["report"].get("source_digest") != stored_hash):
            return {"valid": False, "reason": "STORED_PART_INTEGRITY_FAILED", "ordinal": count}
        # Do not retain the stored body while the original source is hydrated.
        expected_hash, expected_bytes = stored_hash, stored_bytes
        del part
        current = load_part_source(conn, parent["btc_parent_movement_id"], generation["observed_at_utc"])
        _, current_hash, current_bytes = _payload(current, "SOURCE")
        del current
        if (current_hash, current_bytes) != (expected_hash, expected_bytes):
            return {"valid": False, "reason": "SOURCE_CHANGED", "ordinal": count}
        stream.update((canonical({"ordinal": count, "parent_id": parent["btc_parent_movement_id"],
            "parent_start_utc": report.utc(parent["start_time_utc"]).isoformat(),
            "source_digest": current_hash, "source_bytes": current_bytes})+"\n").encode())
        count += 1
    if next(parts, None) is not None or count != generation["parent_count"]:
        return {"valid": False, "reason": "PARENT_POPULATION_CHANGED", "ordinal": count}
    manifest["source_manifest_digest"] = stream.hexdigest()
    return {"valid": True, "manifest": manifest, "source_digest": stream.hexdigest()}


def get_previous_parent_cache(conn, worker_key, parent_id, previous_source=None):
    """Read only this parent's previous source/results, including legacy state."""
    if previous_source is None:
        header = conn.execute("""SELECT CASE WHEN source->>'storage_version'=%s
            THEN source ELSE NULL END AS source FROM research_btc_wave_report_state
            WHERE worker_key=%s""", (STORAGE_VERSION, worker_key)).fetchone()
        previous_source = header["source"] if header else None
    if previous_source and previous_source.get("storage_version") == STORAGE_VERSION:
        row = conn.execute("""SELECT part.source,part.source_digest,part.source_bytes,
            part.report,part.report_digest,part.report_bytes FROM research_btc_wave_report_parts part
            JOIN research_btc_wave_report_generations generation USING(generation_id)
            WHERE part.generation_id=%s::uuid AND part.parent_id=%s
              AND generation.worker_key=%s AND generation.status='PUBLISHED'""",
            (_id(previous_source), parent_id, worker_key)).fetchone()
        if row is None or row["source"] is None or row["report"] is None:
            return None
        _, source_hash, source_bytes = _payload(row["source"], "SOURCE")
        _, report_hash, report_bytes = _payload(row["report"], "REPORT")
        if ((source_hash, source_bytes) != (row["source_digest"], row["source_bytes"])
                or (report_hash, report_bytes) != (row["report_digest"], row["report_bytes"])
                or row["report"].get("source_digest") != source_hash):
            raise ValueError("Previous published parent part failed integrity verification")
        return {"source": cache_source(row["source"]), "report": row["report"]}
    # Filtering runs in PostgreSQL. The client never hydrates the whole legacy
    # source/report graphs just to reuse one parent's already verified paths.
    row = conn.execute("""WITH previous AS (
        SELECT source,report FROM research_btc_wave_report_state WHERE worker_key=%s
          AND source->>'storage_version' IS NULL
    ), target AS (
        SELECT wave FROM previous,jsonb_array_elements(source->'waves') wave
        WHERE wave->>'btc_parent_movement_id'=%s
    ) SELECT jsonb_build_object('waves',COALESCE((
            SELECT jsonb_agg(wave) FROM previous,jsonb_array_elements(source->'waves') wave
            WHERE wave->>'btc_parent_movement_id'=%s
               OR ((wave->>'start_time_utc')::timestamptz=(SELECT (wave->>'end_time_utc')::timestamptz FROM target)
                   AND wave->>'episode_policy_version'=%s AND wave->'evidence_eligible'='true'::jsonb
                   AND wave->>'boundary_reason'='CAUSAL_CLOSE_REVERSAL')),'[]'::jsonb),
            'events',COALESCE((SELECT jsonb_agg(event) FROM previous,jsonb_array_elements(source->'events') event
                WHERE event->>'btc_parent_movement_id'=%s),'[]'::jsonb)) AS source,
        jsonb_build_object('method_version',report->'method_version','records',COALESCE((
            SELECT jsonb_agg(record) FROM jsonb_array_elements(report->'records') record
            WHERE record->>'btc_parent_movement_id'=%s),'[]'::jsonb)) AS report
        FROM previous WHERE EXISTS(SELECT 1 FROM target)""",
        (worker_key, parent_id, parent_id, report.POLICY_VERSION, parent_id, parent_id)).fetchone()
    if row is None or row["source"] is None or row["report"] is None:
        return None
    _payload(row["source"], "PREVIOUS_SOURCE")
    _payload(row["report"], "PREVIOUS_REPORT")
    return {"source": row["source"], "report": row["report"]}
