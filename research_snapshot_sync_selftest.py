"""Network-free snapshot replay, frozen provenance and deferred-source checks."""
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import patch

import google_sheets_sync as sheets
from google_sheets_sync_selftest import Event, DirectEvent, NeutralEvent
import research_snapshot_sync_worker as worker


def _alert(raw, event_id):
    event = raw.to_dict()
    event.update(event_id=event_id, event_kind="ALERT", delivery_status="DELIVERED",
                 delivered_at_utc=event["alert_time_utc"])
    return event


def _snapshot(upserts):
    return next(item["row"] for item in upserts if item["sheet"] == "Snapshots")


class Result:
    def __init__(self, rows):
        self.rows = rows
    def fetchall(self):
        return self.rows
    def fetchone(self):
        return self.rows[0] if self.rows else None


class SourceConnection:
    def __init__(self, candidates, children=(), neutral=None):
        self.candidates = candidates
        self.children = children
        self.neutral = neutral
        self.calls = []
    def execute(self, query, params=()):
        self.calls.append((query, params))
        if "pg_try_advisory_xact_lock" in query:
            return Result([{"held": True}])
        if "picked AS MATERIALIZED" in query:
            return Result(self.candidates)
        if "ORDER BY event.event_id LIMIT" in query:
            return Result(self.children)
        if "research_prospective_shadow_events WHERE event_id" in query:
            return Result([self.neutral] if self.neutral else [])
        if "INSERT INTO research_snapshot_sheet_sources" in query:
            return Result([])
        raise AssertionError("unexpected source SQL")


def run():
    primary = _alert(Event(), 1)
    primary["engine_snapshot"].update(near_share_pct=70, near_amount=700, far_amount=300)
    child = _alert(DirectEvent(), 2)
    child["current_price"] = 105
    child["alert_time_utc"] = "2026-09-05T09:31:00Z"
    sources = [primary, child]
    before_cache = deepcopy(sheets._SNAPSHOT_CACHE)
    rebuilt = worker.rebuild_alert_group(sources)
    snapshot = _snapshot(rebuilt)
    assert snapshot["telegram_event_count"] == 2
    assert snapshot["primary_alert_type"] == "MAX_PAIN_ALERT"
    assert snapshot["reference_price"] == 100
    assert snapshot["parent_event_id"] == primary["event_fingerprint"]
    assert snapshot["liquidity_long_pct"] == 70
    assert snapshot["liquidity_short_pct"] == 30
    assert snapshot["liquidity_timeframe"] == "4h"
    assert rebuilt[0]["row"]["מחיר ייחוס"] == 100
    assert rebuilt[0]["row"]["זמן סריקה"] == snapshot["timestamp_israel"]
    assert rebuilt == worker.rebuild_alert_group(list(reversed(sources)))
    assert sheets._SNAPSHOT_CACHE == before_cache
    assert len(rebuilt) == 4  # One aggregate per live/Snapshots + two Telegram IDs.
    assert len({item["row"]["event_id"] for item in rebuilt[2:]}) == 2

    # A later, higher-priority 1h source may not inherit a previous 4h balance.
    combined = _alert(Event(), 3)
    combined["event_fingerprint"] = "combined-child"
    combined["event_type"] = "COMBINED_CONFIRMATION"
    combined["timeframe"] = "1h"
    third = _snapshot(worker.rebuild_alert_group(sources + [combined]))
    assert third["telegram_event_count"] == 3
    assert third["maxpain_selected_timeframe"] == "1h"
    assert third["liquidity_long_pct"] is None
    assert third["liquidity_short_pct"] is None
    assert third["liquidity_timeframe"] is None
    combined["engine_snapshot"]["liquidity_imbalances"] = [{"timeframe": "1h", "share_pct": 80}]
    combined["source_side"] = "LONG"
    latest = _snapshot(worker.rebuild_alert_group([primary, combined]))
    assert latest["liquidity_long_pct"] == 20
    assert latest["liquidity_short_pct"] == 80
    assert latest["liquidity_timeframe"] == "1h"
    for invalid in ([primary, primary], [dict(primary, direction="SHORT"), child],
                    [dict(primary, delivery_status="APPROVED_FOR_DELIVERY")],
                    [dict(primary, engine_snapshot={}), child]):
        try:
            worker.rebuild_alert_group(invalid)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid source group was accepted")

    staged = []
    def stage(conn, upserts):
        staged.append(deepcopy(upserts))
        return len(upserts)
    conn = SourceConnection(sources, sources)
    with patch.object(worker.research_sheet_outbox, "stage_upserts", stage):
        result = worker.reconcile_sources(conn)
    assert result == {"sources": 2, "groups": 1, "staged_rows": 4, "rejected": 0, "deferred": 0}
    assert staged == [rebuilt]
    markers = [params for query, params in conn.calls if "INSERT INTO research_snapshot_sheet_sources" in query]
    assert [params[0] for params in markers] == [1, 2]
    assert all(params[3] == "STAGED" for params in markers)

    neutral = NeutralEvent().to_dict()
    neutral.update(event_id=9, event_kind="DECISION_SAMPLE", delivery_status="NOT_APPLICABLE")
    pending = SourceConnection([neutral])
    staged.clear()
    with patch.object(worker.research_sheet_outbox, "stage_upserts", stage):
        result = worker.reconcile_sources(pending)
    assert not staged
    assert result["deferred"] == 1 and result["rejected"] == 0
    deferred = [params for query, params in pending.calls if "INSERT INTO research_snapshot_sheet_sources" in query]
    assert deferred[0][3] == "DEFERRED"
    candidate_query = next(query for query, _ in pending.calls if "picked AS MATERIALIZED" in query)
    assert "staged.next_attempt_at_utc>NOW()" in candidate_query
    assert "ORDER BY event_id ASC" in candidate_query
    assert "ORDER BY event_id DESC" in candidate_query
    authorized = SourceConnection([neutral], neutral={
        "decision_feature_bundle": {"model_score_status": "ABSENT"},
        "anchor_slot_id": 8, "feature_bundle_policy_version": "formula-visible-v1",
        "feature_bundle_sha256": "b" * 64,
    })
    with patch.object(worker.research_sheet_outbox, "stage_upserts", stage):
        result = worker.reconcile_sources(authorized)
    assert result["sources"] == 1 and result["staged_rows"] == 2
    neutral_row = _snapshot(staged[-1])
    assert neutral_row["no_alert_snapshot"] is True and neutral_row["alert_sent"] is False
    assert "price_oi_total_score" not in neutral_row

    # No Sheet HTTP until source staging has committed and closed its context.
    open_connections = 0
    class Transaction:
        def __enter__(self):
            nonlocal open_connections
            open_connections += 1
            return self
        def __exit__(self, *args):
            nonlocal open_connections
            open_connections -= 1
    def drain(*args, **kwargs):
        assert open_connections == 0
        return {"synced": 1}
    with patch.object(worker, "psycopg", SimpleNamespace(connect=lambda *a, **kw: Transaction())), \
         patch.object(worker, "_database_url", lambda: "postgresql://selftest"), \
         patch.object(sheets, "enabled", lambda: True), \
         patch.object(worker, "reconcile_sources", lambda conn: {"staged_rows": 1}), \
         patch.object(worker.research_sheet_outbox, "drain", drain):
        assert worker.SnapshotSyncWorker().run_once()["delivery"]["synced"] == 1

    # Switching delivery ownership blocks both newly enqueued and pre-start
    # snapshot envelopes; unrelated durable payloads still use their ACK path.
    with patch.object(sheets, "_DURABLE_SNAPSHOT_MODE", True), \
         patch.object(sheets, "enabled", lambda: True), \
         patch.object(sheets, "_WEBHOOK_URL", "https://example.invalid/sheets"), \
         patch.object(sheets, "urlopen", side_effect=AssertionError("stale snapshot made HTTP")):
        assert sheets.enqueue_delivered_event(primary) is False
        assert sheets._deliver_envelope({"payload": {"kind": "alert"}}, attempts=1) is False
    print("committed snapshot Sheet replay self-test: PASS")


if __name__ == "__main__":
    run()
