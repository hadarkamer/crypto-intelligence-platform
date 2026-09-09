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
    print("PASS full-wave worker bounded resume, JSON closed cache, frozen cutoff, all8 cells, missing representative and stale row withdrawal")


if __name__ == "__main__":
    run()
