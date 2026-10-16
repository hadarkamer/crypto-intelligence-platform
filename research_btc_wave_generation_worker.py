"""Resumable full-history orchestration with one bounded native wave at a time.

The outer worker retains the advisory lock, frozen-cutoff, exact Sheet ACK and
refresh gates. This controller never commits partial report publications: source
revalidation, the aggregate, outbox writes and generation publication share one
repeatable-read transaction. Price reads are from the existing local archive.
"""
from __future__ import annotations

from datetime import timedelta
import time

import research_btc_wave_generation_store as store
import research_btc_wave_stream_aggregate as aggregate
import research_btc_wave_report_worker as native

STORAGE_VERSION = "btc-wave-parts-v1"


def is_reference(job):
    return isinstance(job, dict) and job.get("storage_version") == STORAGE_VERSION


def reference(generation):
    return {"storage_version": STORAGE_VERSION, "generation_id": str(generation["generation_id"])}


def prepare_part_job(source, observed_at, *, previous=None):
    """Keep native representative selection/math and verify causal closure.

    Successor metadata proves closure for cache reuse, but is not another
    analytical parent. It must never appear in the part's report population.
    """
    job = native.prepare_job(source, observed_at)
    if not previous:
        return job
    cache = native.report.verified_closed_cache(
        previous_report=previous["report"], previous_source=previous["source"],
        waves=store.cache_source(source)["waves"], events=source["events"],
        observed_at=observed_at, include_hype_perp=True)
    job["pending_event_ids"] = [event_id for event_id in job["pending_event_ids"]
        if not any(key[1] == int(event_id) for key in cache)]
    job["records"] = list(cache.values())
    job["reused_closed_paths"] = len(cache)
    return job


def _persist_reference(conn, generation):
    conn.execute("""UPDATE research_btc_wave_report_state
        SET pending_job=%s::jsonb,last_error=NULL,updated_at_utc=NOW()
        WHERE worker_key=%s""", (native.canonical(reference(generation)), native.VERSION))


def _check_source(source):
    if len(native.canonical(source)) > native._MAX_SOURCE_JSON_BYTES:
        raise ValueError("FULL_WAVE_PART_SOURCE_TOO_LARGE_FOR_DATABASE_PUBLICATION")


def _restore_job(part, observed_at):
    source, saved = part.get("source"), part.get("job")
    if not source or not saved:
        raise ValueError("Incomplete part checkpoint lacks its source or native job")
    _check_source(source)
    if (saved.get("version") != native.VERSION
            or native.report.utc(saved["observed_at"]) != native.report.utc(observed_at)
            or saved.get("source_digest") != native.digest(source)):
        raise ValueError("Frozen part checkpoint does not match its generation")
    return {**saved, "source": source}


def _fetch(conn, symbol, start, end):
    route = native.archive.HYPERLIQUID_PERP if symbol == "HYPE" else native.archive.BINANCE_SPOT
    return native.archive.read_path(route, symbol, start, end, connection=conn)


def run_generation(conn, *, state, pending, now, monotonic=time.monotonic):
    """Advance at most eight paths total, retaining at most one current part.

    Caller has already checked the previous exact report's Sheet ACK, refresh
    deadline and row version, and holds the existing session advisory lock.
    """
    started = monotonic()
    if pending:
        if not is_reference(pending):
            raise ValueError("Partition controller cannot consume a legacy checkpoint")
        generation = store.get_generation(conn, pending["generation_id"])
        conn.commit()
        if (generation["worker_key"] != native.VERSION
                or generation["status"] != "CAPTURED"):
            raise ValueError("Pending generation is not an active matching worker generation")
    else:
        with conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            generation = store.freeze(conn, native.VERSION, now)
            if not generation["parent_count"]:
                store.mark_generation(conn, generation, "ABANDONED")
                return {"waiting_for": "BTC_PARENT_POPULATION", "storage_version": STORAGE_VERSION}
            _persist_reference(conn, generation)
    observed_at = native.report.utc(generation["observed_at_utc"])
    progress = {"storage_version": STORAGE_VERSION,
        "generation_id": str(generation["generation_id"]),
        "target_observed_at_utc": observed_at.isoformat(),
        "parent_count": generation["parent_count"], "processed_paths": 0,
        "completed_parts_this_pass": 0, "published": False}
    while (progress["processed_paths"] < native._PATHS_PER_PASS
           and monotonic()-started < native._PASS_SECONDS):
        with conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            part = store.next_unfinished_part(conn, generation)
            if part is None:
                break
            if part.get("job"):
                job = _restore_job(part, observed_at)
            else:
                source = store.load_part_source(conn, part["parent_id"], observed_at)
                _check_source(source)
                previous = store.get_previous_parent_cache(conn, native.VERSION,
                    part["parent_id"], previous_source=state.get("source"))
                job = prepare_part_job(source, observed_at, previous=previous)
                del previous, source
            advanced = native.advance_job(job,
                fetcher=lambda symbol, start, end: _fetch(conn, symbol, start, end),
                path_limit=native._PATHS_PER_PASS-progress["processed_paths"],
                seconds_budget=max(0., native._PASS_SECONDS-(monotonic()-started)),
                monotonic=monotonic)
            progress["processed_paths"] += advanced["processed_paths"]
            completed = native.finish_job(job) if advanced["completed"] else None
            store.save_checkpoint(conn, generation, part["ordinal"], source=job["source"],
                job=None if completed is not None else job, completed_report=completed)
            if completed is not None:
                progress["completed_parts_this_pass"] += 1
            progress["remaining_paths_in_current_part"] = advanced["remaining_paths"]
            # Release both previous and current source/report owners before the
            # next database read can hydrate another part.
            del part, job, completed
        if not advanced["completed"]:
            break
    # The completeness probe is metadata-only in the store; a remaining part
    # never triggers aggregate/outbox work, regardless of completed part count.
    ready = not store.has_unfinished_parts(conn, generation)
    conn.commit()
    progress["completed"] = ready
    if not ready or monotonic()-started >= native._PASS_SECONDS:
        progress["awaiting_final_validation"] = ready
        return progress
    with conn.transaction():
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        verified = store.validate_generation(conn, generation)
        if not verified["valid"]:
            # Never move the cutoff forward to conceal a late committed event.
            # Keep all old parts as immutable evidence and restart atomically.
            store.mark_generation(conn, generation, "ABANDONED")
            replacement = store.freeze(conn, native.VERSION, observed_at)
            _persist_reference(conn, replacement)
            progress.update(source_changed=True, completed=False,
                replaced_generation_id=str(generation["generation_id"]),
                generation_id=str(replacement["generation_id"]),
                validation_reason=verified.get("reason"))
            return progress
        manifest = verified["manifest"]
        completed = aggregate.aggregate_generation(conn, generation["generation_id"],
            observed_at, manifest)
        completed.update(worker_version=native.VERSION,
            source_population="ALL_NATIVE_DELIVERED_MAX_PAIN_ALERT_SCORE_GE_65_SINCE_20260904_ISRAEL",
            source_start_utc=native.SOURCE_START)
        report_json, source_json = native.canonical(completed), native.canonical(manifest)
        if max(len(report_json), len(source_json)) > native._MAX_SOURCE_JSON_BYTES:
            raise ValueError("FULL_WAVE_ROOT_TOO_LARGE_FOR_DATABASE_PUBLICATION")
        staged = native.research_sheet_outbox.stage_upserts(conn,
            native.sheet_upserts(completed, evaluated_at=now, previous_report=state["report"]))
        conn.execute("""UPDATE research_btc_wave_report_state SET pending_job=NULL,
            report=%s::jsonb,source=%s::jsonb,report_observed_at_utc=%s,last_completed_at_utc=%s,
            next_report_at_utc=%s,last_error=NULL,updated_at_utc=NOW() WHERE worker_key=%s""",
            (report_json, source_json, observed_at, now,
             now+timedelta(minutes=native._REFRESH_MINUTES), native.VERSION))
        store.mark_generation(conn, generation, "PUBLISHED")
        progress.update(published=True, staged_rows=staged,
            report_observed_at_utc=observed_at.isoformat(),
            missing_representatives=completed["missing_representatives"],
            source_digest=completed["source_digest"], report_digest=native.digest(completed))
    return progress
