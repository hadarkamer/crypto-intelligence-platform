"""Bounded automatic full-wave reports over the immutable local price archive.

Each report generation freezes source selection and observation boundaries before
reading prices. At most eight unique paths are read per minute; the dated report
is published atomically after the complete selected population is attempted.
No network price requests, trades or canonical fixed-horizon writes occur here.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import time
from typing import Any, Callable, Mapping

import research_btc_wave_endpoint_report as report
import research_price_archive as archive
import research_sheet_outbox
try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:
    psycopg = None
    dict_row = None

VERSION = "native-maxpain-fullwave-archive-perp-v1"
SHEET_NAME = "MaxPain_Wave_Live"
HEADERS = (
    "source_scope", "coin_scope", "direction", "threshold_pct", "success_probability",
    "probability_denominator", "closed_successes", "closed_failures", "closed_unresolved",
    "closed_representatives", "active_representatives", "missing_representatives", "asymmetry_ratio",
    "provisional_success_probability", "provisional_probability_denominator", "provisional_asymmetry_ratio",
    "coverage_status", "source_event_ids", "btc_parent_movement_ids", "method_version", "source_contract",
    "observation_as_of_utc", "btc_observed_through_utc", "last_evaluated_at", "source_digest", "report_digest", "notes",
)
_TRUE = {"1", "true", "yes", "on"}
_ENABLED = os.getenv("RESEARCH_BTC_WAVE_REPORT_ENABLED", os.getenv("RESEARCH_PRICE_ARCHIVE_ENABLED", "")).lower() in _TRUE
_POLL_SECONDS = 60
_REFRESH_MINUTES = 15
_PATHS_PER_PASS = 8
_PASS_SECONDS = 30
_LOCK_ID = 70108260909452001
# Explicit complete population from the approved September 4 research period.
SOURCE_START = datetime(2026, 9, 3, 21, 0, tzinfo=timezone.utc)


def canonical(value):
    return json.dumps(value, sort_keys=True, default=report._json_default, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _iso_or_none(value):
    return report.utc(value).isoformat() if value is not None else None


def _database_url():
    dedicated = os.getenv("RESEARCH_DATABASE_URL", "").strip()
    return dedicated or (os.getenv("DATABASE_URL", "").strip()
        if os.getenv("RESEARCH_USE_PRIMARY_DATABASE", "").lower() in _TRUE else "")


def _connect(url):
    return psycopg.connect(url, row_factory=dict_row, connect_timeout=5,
        options="-c statement_timeout=15000 -c lock_timeout=1000")


def load_job_source(conn, observed_at):
    """No newest-N window: exceeding the declared bound stops publication."""
    parents = conn.execute("""SELECT btc_parent_movement_id FROM research_btc_parent_movements
        WHERE episode_policy_version=%s AND start_time_utc<=%s
          AND (end_time_utc IS NULL OR end_time_utc>=%s)
        ORDER BY start_time_utc LIMIT %s""", (report.POLICY_VERSION, observed_at,
            SOURCE_START, report.MAX_WAVES + 1)).fetchall()
    if len(parents) > report.MAX_WAVES:
        raise ValueError("FULL_WAVE_POPULATION_EXCEEDS_32_PARENTS; no truncated headline published")
    if not parents:
        return {"waves": [], "events": []}
    waves, events = report.load_source(conn, [p["btc_parent_movement_id"] for p in parents])
    for wave in waves:
        # Freeze only information observable by this report's source cutoff.
        if wave.get("end_time_utc") and report.utc(wave["end_time_utc"]) > observed_at:
            wave["end_time_utc"] = None
            wave["closing_boundary_verified"] = False
        if wave.get("observed_through_utc"):
            wave["observed_through_utc"] = min(report.utc(wave["observed_through_utc"]),
                                              report.latest_closed_cutoff(observed_at))
    events = [event for event in events if SOURCE_START <= report.utc(event["alert_time_utc"]) <= observed_at]
    # Versioned policy excludes its initial boundary-unverified warmup parent;
    # no source or price availability condition selects the eligible universe.
    eligible = {wave["btc_parent_movement_id"] for wave in waves if wave["evidence_eligible"] is True}
    return {"waves": waves, "events": [event for event in events if event["btc_parent_movement_id"] in eligible]}


def prepare_job(source, observed_at, *, previous_report=None, previous_source=None):
    cache = {}
    if previous_report and previous_source:
        cache = report.verified_closed_cache(previous_report=previous_report, previous_source=previous_source,
            waves=source["waves"], events=source["events"], observed_at=observed_at, include_hype_perp=True)
    representatives = report.select_representatives(source["events"])
    unique = {int(event["event_id"]): event for event in representatives}
    pending = [event["event_id"] for event in sorted(unique.values(), key=lambda e: (report.utc(e["alert_time_utc"]), e["event_id"]))
        if (report.PERP_SCOPE if event["symbol"] == "HYPE" else report.SPOT_SCOPE, event["event_id"]) not in cache]
    # Each stored source path appears once even when ALL and a coin share it.
    return {"version": VERSION, "observed_at": observed_at, "source": source, "source_digest": digest(source),
        "pending_event_ids": pending, "next_index": 0, "records": list(cache.values()),
        "reused_closed_paths": len(cache), "selected_unique_events": len(unique)}


def _cache(job):
    return {(row["source_scope"], int(row["source_event_id"])): row for row in job["records"]}


def advance_job(job, *, fetcher: Callable, path_limit=_PATHS_PER_PASS,
                seconds_budget=_PASS_SECONDS, monotonic=time.monotonic):
    """Resumable unit bounded by unique events, with no outcome-based selection."""
    cache = _cache(job)
    events = {int(event["event_id"]): event for event in job["source"]["events"]}
    started, processed = monotonic(), 0
    pending = job["pending_event_ids"]
    while job["next_index"] < len(pending) and processed < path_limit:
        if processed and monotonic() - started >= seconds_budget:
            break
        event = events[int(pending[job["next_index"]])]
        partial = report.build_report(waves=job["source"]["waves"], events=[event],
            observed_at=job["observed_at"], include_hype_perp=True, fetcher=fetcher)
        for row in partial["records"]:
            if row["source_scope"] in {report.SPOT_SCOPE, report.PERP_SCOPE}:
                cache[(row["source_scope"], int(row["source_event_id"]))] = row
        job["next_index"] += 1
        processed += 1
    job["records"] = list(cache.values())
    return {"processed_paths": processed, "remaining_paths": len(pending)-job["next_index"],
            "completed": job["next_index"] == len(pending)}


def finish_job(job):
    if job["next_index"] != len(job["pending_event_ids"]):
        raise ValueError("Cannot publish a partial representative processing generation")
    cache = _cache(job)
    # A cached complete HYPE PERP path still has an intentionally rejected
    # canonical Spot diagnostic; populate it without any price request.
    parents = {wave["btc_parent_movement_id"]: wave for wave in job["source"]["waves"]}
    for event in report.select_representatives(job["source"]["events"]):
        if event["symbol"] == "HYPE" and (report.SPOT_SCOPE, event["event_id"]) not in cache:
            cache[(report.SPOT_SCOPE, event["event_id"])] = report._unavailable(event,
                parents[event["btc_parent_movement_id"]], report.utc(job["observed_at"]), report.SPOT_SCOPE,
                "HYPE_PERPETUAL_MEASUREMENT_HAS_SEPARATE_SOURCE_CONTRACT")
    expected = {(source, int(event["event_id"]))
        for event in report.select_representatives(job["source"]["events"])
        for source in ([report.SPOT_SCOPE, report.PERP_SCOPE] if event["symbol"] == "HYPE" else [report.SPOT_SCOPE])}
    if not expected.issubset(cache):
        raise ValueError("Finished job lacks an exact selected source record")
    def no_fetch(*args):
        raise AssertionError("Finished generation must contain every selected source record")
    completed = report.build_report(waves=job["source"]["waves"], events=job["source"]["events"],
        observed_at=job["observed_at"], include_hype_perp=True, fetcher=no_fetch,
        _verified_closed_cache=cache)
    completed.update(worker_version=VERSION, source_digest=job["source_digest"],
        reused_complete_closed_source_paths=job["reused_closed_paths"],
        source_population="ALL_NATIVE_DELIVERED_MAX_PAIN_ALERT_SCORE_GE_65_SINCE_20260904_ISRAEL",
        source_start_utc=SOURCE_START)
    return completed


def sheet_upserts(completed, *, evaluated_at, previous_report=None):
    rows = []
    report_hash = digest(completed)
    active_cutoffs = [wave["observed_through_utc"] for wave in completed["waves"]
                      if not wave.get("end_time_utc") and wave.get("observed_through_utc")]
    btc_cutoff = max((report.utc(value) for value in active_cutoffs), default=None)
    for summary in completed["summary_rows"]:
        if summary["source_scope"] != report.PERP_MIXED_SCOPE:
            continue
        counts = summary["closed_cohort_status_counts"]
        row = {"source_scope": summary["source_scope"], "coin_scope": summary["coin_scope"],
            "direction": summary["direction"], "threshold_pct": summary["threshold_pct"],
            "success_probability": summary["success_probability"],
            "probability_denominator": summary["probability_denominator"],
            "closed_successes": counts.get("SUCCESS", 0), "closed_failures": counts.get("FAILURE", 0),
            "closed_unresolved": counts.get("UNRESOLVED", 0),
            "closed_representatives": summary["closed_representatives"],
            "active_representatives": summary["active_representatives"],
            "missing_representatives": summary["missing_representatives"],
            "asymmetry_ratio": summary["asymmetry_ratio"],
            "provisional_success_probability": summary["provisional_observed_success_probability"],
            "provisional_probability_denominator": summary["provisional_observed_probability_denominator"],
            "provisional_asymmetry_ratio": summary["provisional_observed_asymmetry_ratio"],
            "coverage_status": "COMPLETE_OBSERVED_PREFIX" if summary["observed_cohort_coverage_complete"] else "DATA_MISSING",
            "source_event_ids": canonical(summary["source_event_ids"]),
            "btc_parent_movement_ids": canonical(summary["btc_parent_movement_ids"]),
            "method_version": report.METHOD_VERSION, "source_contract": VERSION,
            "observation_as_of_utc": report.utc(completed["observed_at_utc"]).isoformat(),
            "btc_observed_through_utc": btc_cutoff.isoformat() if btc_cutoff else "",
            "last_evaluated_at": report.utc(evaluated_at).isoformat(),
            "source_digest": completed["source_digest"], "report_digest": report_hash,
            "notes": "גלים סגורים: תוצאות סופיות לחלון שנבדק; גל פעיל: תוצאות זמניות. כל גל נספר פעם אחת בכל השוואה; אין לחבר את ALL עם המטבעות או את LONG עם SHORT. מחיר HYPE נמדד בחוזים של Hyperliquid, מפתיחת הדקה המלאה הבאה. אסימטריה: סכום התנועה המרבית לטובת הכיוון חלקי סכום התנועה המרבית נגדו, לאורך הגל כולו; לכן זהה בשמונת הספים. דוח תיאורי נפרד מחישובי חלונות הזמן ומהסמכת נוסחאות."}
        rows.append({"sheet": SHEET_NAME, "key": "source_scope,coin_scope,direction,threshold_pct", "row": row})
    # A source correction can remove a formerly present stratum. Explicitly
    # withdraw its previous row rather than leaving an old positive headline.
    current_keys = {(x["row"]["source_scope"], x["row"]["coin_scope"], x["row"]["direction"], x["row"]["threshold_pct"]) for x in rows}
    for old in (previous_report or {}).get("summary_rows", []):
        key = (old["source_scope"], old["coin_scope"], old["direction"], old["threshold_pct"])
        if old["source_scope"] == report.PERP_MIXED_SCOPE and key not in current_keys:
            row = dict(zip(("source_scope", "coin_scope", "direction", "threshold_pct"), key))
            row.update(coverage_status="REMOVED_FROM_CURRENT_SOURCE_POPULATION", last_evaluated_at=report.utc(evaluated_at).isoformat(),
                       observation_as_of_utc=report.utc(completed["observed_at_utc"]).isoformat(), source_digest=completed["source_digest"], report_digest=report_hash)
            rows.append({"sheet": SHEET_NAME, "key": "source_scope,coin_scope,direction,threshold_pct", "row": row})
    return rows


def sheet_delivery_status(conn, completed):
    """Do not replace a report generation until its exact rows were ACKed.

    The sheet/row key is the outbox primary key. The bounded read covers the
    current 9 coin scopes x 2 directions x 8 thresholds plus withdrawals.
    Older SYNCED rows cannot stand in for the current report digest.
    """
    report_hash = digest(completed)
    expected_keys = {
        research_sheet_outbox._json([str(summary[name]) for name in
            ("source_scope", "coin_scope", "direction", "threshold_pct")])
        for summary in completed.get("summary_rows", [])
        if summary["source_scope"] == report.PERP_MIXED_SCOPE
    }
    rows = conn.execute("""SELECT row_key,sync_status,
            payload->'row'->>'report_digest' AS report_digest
        FROM research_sheet_upsert_outbox WHERE sheet_name=%s
        ORDER BY row_key LIMIT 257""", (SHEET_NAME,)).fetchall()
    if len(rows) > 256:
        raise ValueError("Full-wave Sheet delivery audit exceeds its 256-row bound")
    current = [row for row in rows if row["report_digest"] == report_hash]
    found = {row["row_key"] for row in current}
    missing = len(expected_keys-found)
    pending = sum(row["sync_status"] != "SYNCED" for row in current)
    return {"complete": missing == 0 and pending == 0,
        "report_digest": report_hash, "expected_rows": len(expected_keys),
        "current_generation_rows": len(current), "synced_rows": len(current)-pending,
        "pending_rows": pending, "missing_or_other_generation_rows": missing}


class ResearchBTCWaveReportWorker:
    def __init__(self):
        self._task = None
        self._ready = False
        self.metrics = {"runs": 0, "last_result": None, "last_error": None}

    def status(self):
        return {"enabled": _ENABLED, "configured": bool(_database_url()),
            "running": bool(self._task and not self._task.done()), "schema_ready": self._ready,
            "version": VERSION, "source_scope": report.PERP_MIXED_SCOPE,
            "refresh_minutes": _REFRESH_MINUTES, "poll_seconds": _POLL_SECONDS,
            "refresh_policy": "AT_LEAST_15_MINUTES_AFTER_COMPLETION_AND_PREVIOUS_SHEET_GENERATION_FULLY_ACKED",
            "path_limit_per_pass": _PATHS_PER_PASS, "sheet": SHEET_NAME,
            "canonical_fixed_horizon_modified": False, **self.metrics}

    def run_once(self, *, now=None):
        now = report.utc(now or datetime.now(timezone.utc))
        with _connect(_database_url()) as conn:
            held = conn.execute("SELECT pg_try_advisory_lock(%s) AS held", (_LOCK_ID,)).fetchone()["held"]
            conn.commit()
            if not held:
                return {"locked": True}
            try:
                state = conn.execute("SELECT * FROM research_btc_wave_report_state WHERE worker_key=%s", (VERSION,)).fetchone()
                conn.commit()
                if state is None:
                    raise RuntimeError("Full-wave report migration045 required")
                job = state["pending_job"]
                # A previous deployment may already have checkpointed a newer
                # job while the old report is still being delivered. Preserve
                # that checkpoint, but never advance it past unACKed evidence.
                if state["report"]:
                    delivery = sheet_delivery_status(conn, state["report"])
                    conn.commit()
                    if not delivery["complete"]:
                        waiting = {"waiting_for_sheet_delivery": True, "delivery": delivery,
                            "report_observed_at_utc": _iso_or_none(state["report_observed_at_utc"]),
                            "earliest_next_report_at_utc": _iso_or_none(state["next_report_at_utc"]),
                            "pending_job_preserved": bool(job)}
                        self.metrics.update(last_result=waiting, last_error=None)
                        return waiting
                if not job:
                    if state["next_report_at_utc"] and now < state["next_report_at_utc"]:
                        waiting = {"waiting_until": _iso_or_none(state["next_report_at_utc"]),
                                   "report_observed_at_utc": _iso_or_none(state["report_observed_at_utc"])}
                        self.metrics.update(last_result=waiting, last_error=None)
                        return waiting
                    with conn.transaction():
                        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                        source = load_job_source(conn, now)
                    if not source["waves"]:
                        return {"waiting_for": "BTC_PARENT_POPULATION"}
                    job = prepare_job(source, now, previous_report=state["report"], previous_source=state["source"])
                def fetch(symbol, start, end):
                    route = archive.HYPERLIQUID_PERP if symbol == "HYPE" else archive.BINANCE_SPOT
                    return archive.read_path(route, symbol, start, end, connection=conn)
                progress = advance_job(job, fetcher=fetch)
                # Source/record checkpoints and eventual outbox rows are committed
                # together; restart cannot acknowledge an unpublished generation.
                if progress["completed"]:
                    completed = finish_job(job)
                    # A source transaction that committed late can introduce an
                    # earlier representative. Recheck the same frozen cutoff
                    # before publication; later outcomes never select a substitute.
                    conn.commit()
                    with conn.transaction():
                        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                        current_source = load_job_source(conn, report.utc(job["observed_at"]))
                    if digest(current_source) != job["source_digest"]:
                        revised = prepare_job(current_source, job["observed_at"],
                            previous_report=completed, previous_source=job["source"])
                        conn.execute("""UPDATE research_btc_wave_report_state SET pending_job=%s::jsonb,
                            updated_at_utc=NOW() WHERE worker_key=%s""", (canonical(revised), VERSION))
                        conn.commit()
                        progress.update(published=False, source_changed=True,
                            remaining_paths=len(revised["pending_event_ids"]))
                        self.metrics.update(runs=self.metrics["runs"]+1, last_result=progress, last_error=None)
                        return progress
                    staged = research_sheet_outbox.stage_upserts(conn, sheet_upserts(completed, evaluated_at=now, previous_report=state["report"]))
                    conn.execute("""UPDATE research_btc_wave_report_state SET pending_job=NULL,
                        report=%s::jsonb,source=%s::jsonb,report_observed_at_utc=%s,last_completed_at_utc=%s,
                        next_report_at_utc=%s,last_error=NULL,updated_at_utc=NOW() WHERE worker_key=%s""",
                        (canonical(completed), canonical(job["source"]), job["observed_at"], now,
                         now+timedelta(minutes=_REFRESH_MINUTES), VERSION))
                    progress.update(published=True, staged_rows=staged, report_observed_at_utc=str(job["observed_at"]),
                        missing_representatives=sum(row["status"] == "DATA_MISSING" for row in completed["records"] if row["source_scope"] == report.PERP_MIXED_SCOPE))
                else:
                    conn.execute("""UPDATE research_btc_wave_report_state SET pending_job=%s::jsonb,
                        last_error=NULL,updated_at_utc=NOW() WHERE worker_key=%s""", (canonical(job), VERSION))
                    progress.update(published=False, target_observed_at_utc=str(job["observed_at"]))
                conn.commit()
                self.metrics.update(runs=self.metrics["runs"]+1, last_result=progress, last_error=None)
                return progress
            finally:
                conn.rollback()
                conn.execute("SELECT pg_advisory_unlock(%s)", (_LOCK_ID,))
                conn.commit()

    async def start(self):
        if not _ENABLED:
            return False
        if self._task and not self._task.done():
            return True
        if psycopg is None or not _database_url():
            raise RuntimeError("Full-wave report database is unavailable")
        def check():
            with _connect(_database_url()) as conn:
                return bool(conn.execute("SELECT to_regclass('research_btc_wave_report_state') IS NOT NULL AS ready").fetchone()["ready"] and archive.schema_ready(conn))
        self._ready = await asyncio.to_thread(check)
        if not self._ready:
            raise RuntimeError("Full-wave report migrations044 and045 required")
        self._task = asyncio.create_task(self._run(), name="research-btc-wave-report")
        return True

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self):
        while True:
            try:
                await asyncio.to_thread(self.run_once)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.metrics["last_error"] = f"{type(exc).__name__}: {exc}"
                print(f"[btc-wave-report] {self.metrics['last_error']}", flush=True)
            await asyncio.sleep(_POLL_SECONDS)


WORKER = ResearchBTCWaveReportWorker()
