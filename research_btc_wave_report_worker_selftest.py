"""Actual report math across bounded restartable jobs and Sheet source contracts."""
from datetime import timedelta
import json
from unittest.mock import patch

import research_btc_wave_report_worker as worker
import research_btc_wave_endpoint_report as report
from research_btc_wave_endpoint_report_selftest import START, ROUTE, candle, event, wave


def fixtures():
    first = wave("first")
    second = {**wave("second"), "start_time_utc": first["end_time_utc"], "end_time_utc": None,
              "observed_through_utc": START+timedelta(minutes=120)-report.MILLISECOND}
    events = [event(1, wave_id="first"), event(2, seconds=3610, wave_id="second")]
    return {"waves": [first, second], "events": events}


def fetch(symbol, start, end):
    first = int((start-START)//report.MINUTE)
    count = int((end+report.MILLISECOND-start)//report.MINUTE)
    return {**ROUTE, "symbol": symbol, "pair": symbol+"USDT",
            "candles": [candle(first+i, high=101., low=99.5) for i in range(count)]}


def run():
    source = fixtures()
    now = START+timedelta(minutes=120)
    job = worker.prepare_job(source, now)
    first = worker.advance_job(job, fetcher=fetch, path_limit=1)
    assert first == {"processed_paths": 1, "remaining_paths": 1, "completed": False}
    try:
        worker.finish_job(job)
    except ValueError:
        pass
    else:
        raise AssertionError("Incomplete processing generation was published")
    # JSON round trip simulates process restart, retaining the exact frozen
    # target even while live BTC keeps advancing outside this job.
    job = json.loads(worker.canonical(job))
    second = worker.advance_job(job, fetcher=fetch, path_limit=1)
    assert second["completed"] and second["processed_paths"] == 1
    completed = worker.finish_job(job)
    mixed = [r for r in completed["records"] if r["source_scope"] == report.PERP_MIXED_SCOPE]
    assert len(mixed) == 4 and {r["status"] for r in mixed} == {"READY", "OPEN"}
    assert report.utc(completed["observed_at_utc"]) == now
    rows = worker.sheet_upserts(completed, evaluated_at=now+timedelta(minutes=2))
    assert len(rows) == 16
    assert all(item["sheet"] == "MaxPain_Wave_Live" for item in rows)
    assert all(item["row"]["source_scope"] == report.PERP_MIXED_SCOPE for item in rows)
    assert all(set(item["row"]) == set(worker.HEADERS) for item in rows)
    assert {r["row"]["threshold_pct"] for r in rows} == set(report.SUPPORTED_THRESHOLDS_PCT)
    assert all(r["row"]["closed_representatives"] == 1 and r["row"]["active_representatives"] == 1 for r in rows)
    assert all(r["row"]["source_digest"] == job["source_digest"] for r in rows)
    class DeliveryRows:
        def __init__(self, rows): self.rows = rows
        def execute(self, sql, params):
            assert "WHERE sheet_name=%s" in sql and "LIMIT 257" in sql
            assert params == (worker.SHEET_NAME,)
            return self
        def fetchall(self): return self.rows
    acknowledgments = [{"row_key": worker.research_sheet_outbox._json([str(item["row"][key])
        for key in ("source_scope", "coin_scope", "direction", "threshold_pct")]),
        "sync_status": "SYNCED", "report_digest": worker.digest(completed)} for item in rows]
    assert worker.sheet_delivery_status(DeliveryRows(acknowledgments), completed)["complete"]
    acknowledgments[0]["sync_status"] = "IN_FLIGHT"
    pending = worker.sheet_delivery_status(DeliveryRows(acknowledgments), completed)
    assert not pending["complete"] and pending["pending_rows"] == 1
    acknowledgments[0]["sync_status"] = "SYNCED"
    acknowledgments[0]["report_digest"] = "older-report-generation"
    stale = worker.sheet_delivery_status(DeliveryRows(acknowledgments), completed)
    assert not stale["complete"] and stale["missing_or_other_generation_rows"] == 1
    missing_row = worker.sheet_delivery_status(DeliveryRows(acknowledgments[1:]), completed)
    assert not missing_row["complete"] and missing_row["missing_or_other_generation_rows"] == 1
    # Health serializes worker.status with standard json.dumps. Exercise the
    # actual wait path with PostgreSQL-shaped datetime values, including a
    # saved pending job, without hiding the failure with default=str.
    class WaitingConnection:
        def __init__(self, ack_rows, pending_job=None, *, allow_deferred=False, version_changed=False):
            self.rows = ack_rows
            self.state = {"report": completed, "report_observed_at_utc": now,
                "next_report_at_utc": now+timedelta(minutes=15)}
            self.pending_job = pending_job
            self.allow_deferred = allow_deferred
            self.version_changed = version_changed
            self.deferred_reads = 0
            self.writes = []
            self.unlocked = False
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def execute(self, sql, params=()):
            self.sql = " ".join(sql.split())
            if self.sql.startswith("SELECT report,report_observed_at_utc,next_report_at_utc,"):
                assert "xmin::text AS state_xmin" in self.sql and "AS has_pending_job" in self.sql
                assert params == (worker.VERSION,)
            elif self.sql.startswith("SELECT pending_job,source"):
                assert self.allow_deferred, "Waiting poll hydrated source/checkpoint"
                assert "WHERE worker_key=%s AND xmin::text=%s" in self.sql
                assert params == (worker.VERSION, "101")
                self.deferred_reads += 1
            elif self.sql.startswith("SELECT row_key,sync_status,"):
                assert "WHERE sheet_name=%s" in self.sql and "LIMIT 257" in self.sql
                assert params == (worker.SHEET_NAME,)
            elif self.sql.startswith("UPDATE research_btc_wave_report_state"):
                assert self.allow_deferred
                self.writes.append((self.sql, params))
            elif self.sql.startswith("SELECT pg_advisory_unlock"):
                self.unlocked = True
            else:
                assert self.sql.startswith("SELECT pg_try_advisory_lock"), self.sql
            return self
        def fetchone(self):
            if self.sql.startswith("SELECT report,"):
                return {**self.state, "has_pending_job": bool(self.pending_job), "state_xmin": "101"}
            if self.sql.startswith("SELECT pending_job,source"):
                return None if self.version_changed else json.loads(worker.canonical(
                    {"pending_job": self.pending_job, "source": source}))
            return {"held": True}
        def fetchall(self): return self.rows
        def commit(self): pass
        def rollback(self): pass
    for saved_job in (None, {}, [], False, 0, 0.0, "", {"next_index": 1}):
        for elapsed in (1, 20):
            connection = WaitingConnection(acknowledgments, saved_job)
            service = worker.ResearchBTCWaveReportWorker()
            before = worker.canonical(saved_job)
            with patch.object(worker, "_connect", lambda url: connection):
                waiting = service.run_once(now=now+timedelta(minutes=elapsed))
            # The stale ACK gate wins even before the report deadline, and
            # neither wait may read or alter an already-saved checkpoint.
            assert waiting["waiting_for_sheet_delivery"]
            assert waiting["pending_job_preserved"] is bool(saved_job)
            assert waiting["report_observed_at_utc"] == now.isoformat()
            assert connection.deferred_reads == 0 and not connection.writes and connection.unlocked
            assert worker.canonical(connection.pending_job) == before
            json.dumps(service.status(), allow_nan=False)
            json.dumps(waiting, allow_nan=False)
    acknowledgments[0]["report_digest"] = worker.digest(completed)
    connection = WaitingConnection(acknowledgments)
    service = worker.ResearchBTCWaveReportWorker()
    with patch.object(worker, "_connect", lambda url: connection):
        waiting = service.run_once(now=now+timedelta(minutes=1))
    assert "waiting_until" in waiting
    assert service.status()["last_result"] == waiting
    assert connection.deferred_reads == 0 and not connection.writes and connection.unlocked
    json.dumps(service.status(), allow_nan=False)
    json.dumps(waiting, allow_nan=False)
    # A pending checkpoint bypasses only the time gate, after exact ACKs.
    # Stop before path work: this exercises the real deferred read/compaction
    # and checkpoint write without adding provider calls to the fake database.
    pending_job = worker.prepare_job(source, now)
    connection = WaitingConnection(acknowledgments, pending_job, allow_deferred=True)
    service = worker.ResearchBTCWaveReportWorker()
    with patch.object(worker, "_connect", lambda url: connection), \
         patch.object(worker, "load_job_source", side_effect=AssertionError("Pending job reloaded source")), \
         patch.object(worker, "advance_job", return_value={"processed_paths": 0,
             "remaining_paths": len(pending_job["pending_event_ids"]), "completed": False}) as advance:
        resumed = service.run_once(now=now+timedelta(minutes=1))
    assert resumed["completed"] is False and advance.call_count == 1
    assert connection.deferred_reads == 1 and len(connection.writes) == 1 and connection.unlocked
    assert json.loads(connection.writes[0][1][0]) == advance.call_args.args[0]
    # Another writer ignoring the advisory lock cannot combine a new source
    # or checkpoint with the old report header. Both due-new and resume paths
    # must stop before source loading, calculation, publication, or any write.
    for saved_job in (None, pending_job):
        connection = WaitingConnection(acknowledgments, saved_job,
            allow_deferred=True, version_changed=True)
        service = worker.ResearchBTCWaveReportWorker()
        with patch.object(worker, "_connect", lambda url: connection), \
             patch.object(worker, "load_job_source", side_effect=AssertionError("Stale state loaded source")), \
             patch.object(worker, "advance_job", side_effect=AssertionError("Stale state advanced job")), \
             patch.object(worker, "sheet_upserts", side_effect=AssertionError("Stale state published rows")):
            try:
                service.run_once(now=now+timedelta(minutes=20))
            except RuntimeError as exc:
                assert str(exc) == "Full-wave report state changed during deferred read; retry next poll"
            else:
                raise AssertionError("Changed state generation was accepted")
        assert connection.deferred_reads == 1 and not connection.writes and connection.unlocked
    # Reload a stored report: JSON timestamp types must not prevent safe
    # complete closed path reuse on the next actual DB-backed generation.
    previous = json.loads(worker.canonical(completed))
    assert worker.digest(previous) == worker.digest(completed)
    updated = fixtures()
    updated["waves"][1]["observed_through_utc"] = START+timedelta(minutes=180)-report.MILLISECOND
    fresh = worker.prepare_job(updated, now+timedelta(hours=1), previous_report=previous,
        previous_source=json.loads(worker.canonical(source)))
    assert fresh["reused_closed_paths"] == 1
    assert fresh["pending_event_ids"] == [2]
    changed = fixtures()
    changed["events"][0]["current_price"] = 100.1
    invalid = worker.prepare_job(changed, now, previous_report=previous, previous_source=source)
    assert invalid["reused_closed_paths"] == 0
    # Old checkpoints include large snapshots on every event. A digest preserves
    # cache invalidation while keeping both the checkpoint and published source small.
    bulky = fixtures()
    for item in bulky["events"]:
        item["engine_snapshot"]["history"] = "x" * 20000
    old_job = worker.prepare_job(bulky, now)
    migrated = worker.compact_job(old_job)
    assert migrated["source_digest"] == worker.digest(migrated["source"])
    assert worker.compact_job(migrated) is migrated
    assert len(worker.canonical(migrated["source"])) < len(worker.canonical(bulky)) // 10
    assert all(item["engine_snapshot_compacted"] and "history" not in item["engine_snapshot"]
               and len(item["engine_snapshot_digest"]) == 64
               for item in migrated["source"]["events"])
    worker.advance_job(migrated, fetcher=fetch)
    bulky_report = worker.finish_job(migrated)
    reused = worker.prepare_job(worker.compact_source(bulky), now+timedelta(hours=1),
        previous_report=bulky_report, previous_source=bulky)
    assert reused["reused_closed_paths"] == 1
    corrected = fixtures()
    for item in corrected["events"]:
        item["engine_snapshot"]["history"] = "x" * 20000
    corrected["events"][0]["engine_snapshot"]["history"] += "changed"
    rejected = worker.prepare_job(worker.compact_source(corrected), now+timedelta(hours=1),
        previous_report=bulky_report, previous_source=bulky)
    assert rejected["reused_closed_paths"] == 0
    hype = fixtures()
    hype["events"][0]["symbol"] = "HYPE"
    hype["events"][0]["engine_snapshot"]["history"] = "h" * 20000
    preserved = worker.compact_source(hype)["events"][0]
    assert preserved["engine_snapshot"] == hype["events"][0]["engine_snapshot"]
    assert preserved["engine_snapshot_digest"] == report.snapshot_digest(preserved["engine_snapshot"])
    assert not preserved.get("engine_snapshot_compacted")
    # Missing first selected source never falls through to a later winner.
    missing_source = fixtures()
    missing_source["events"].append(event(3, seconds=10, wave_id="first"))
    missing = worker.prepare_job(missing_source, now)
    def empty(symbol, start, end):
        return {**ROUTE, "candles": []}
    worker.advance_job(missing, fetcher=empty)
    missing_result = worker.finish_job(missing)
    missing_rows = worker.sheet_upserts(missing_result, evaluated_at=now)
    assert 3 not in missing["pending_event_ids"]
    assert all(r["row"]["success_probability"] is None and r["row"]["asymmetry_ratio"] is None for r in missing_rows)
    # If a source correction removes a direction/coin, do not retain its old
    # published positive values in the idempotent sheet table.
    reduced = {**completed, "summary_rows": [], "source_digest": "new-source"}
    withdrawn = worker.sheet_upserts(reduced, evaluated_at=now, previous_report=completed)
    assert len(withdrawn) == 16
    assert all(r["row"]["coverage_status"] == "REMOVED_FROM_CURRENT_SOURCE_POPULATION" for r in withdrawn)
    assert all("success_probability" not in r["row"] for r in withdrawn)
    # A data source claiming future close bars never advances a frozen prefix.
    print("PASS full-wave worker bounded resume, deferred waiting reads, row-version guard, JSON closed cache, frozen cutoff, all8 cells, missing representative and stale row withdrawal")


if __name__ == "__main__":
    run()
