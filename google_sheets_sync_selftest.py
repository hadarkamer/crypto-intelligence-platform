import os
os.environ.setdefault("GOOGLE_SHEETS_SYNC_ENABLED", "0")

import google_sheets_sync


class Event:
    def to_dict(self):
        return {
            "event_fingerprint": "evt-1",
            "event_type": "MAX_PAIN_ALERT",
            "alert_time_utc": "2026-09-05T09:30:00Z",
            "symbol": "BTC",
            "direction": "LONG",
            "source_side": "SHORT",
            "timeframe": "4h",
            "score": 78,
            "current_price": 100,
            "target_price": 101,
            "initial_target_distance_pct": 1,
            "categories": ["MAX_PAIN"],
            "strategy_version": "test",
            "code_version": "test",
            "engine_snapshot": {
                "watch_scan_id": "shared-watch:test",
                "sheet_snapshot_id": "sheet-snapshot-1",
                "displayed_direction": "SHORT",
                "analysis_direction": "LONG",
                "opposite_score": 40,
                "average_score_all_timeframes": 70,
                "opposite_average_score_all_timeframes": 45,
                "market_evidence": {"modules": {
                    "positioning": {"direction": "BULLISH", "score": 68},
                    "futures_flow": {"direction": "BULLISH", "score": 71},
                    "spot_flow": {"direction": "BULLISH", "score": 69},
                }},
            },
        }


def run():
    assert google_sheets_sync.enabled() is False
    assert google_sheets_sync.enqueue({"kind": "test"}) is False
    assert google_sheets_sync.status()["fail_open"] is True
    original_enabled = google_sheets_sync.enabled
    original_enqueue = google_sheets_sync.enqueue
    captured = []
    try:
        google_sheets_sync.enabled = lambda: True
        google_sheets_sync.enqueue = lambda payload: captured.append(payload) or True
        assert google_sheets_sync.enqueue_delivered_event(Event()) is True
    finally:
        google_sheets_sync.enabled = original_enabled
        google_sheets_sync.enqueue = original_enqueue
    assert captured[0]["upserts"][0]["row"]["שלישייה 65+"] == "כן"
    assert captured[0]["upserts"][1]["row"]["strict_triple_65_match"] is True
    assert captured[0]["upserts"][1]["row"]["snapshot_id"] == "sheet-snapshot-1"
    assert captured[0]["upserts"][0]["row"]["כיוון מוצג"] == "SHORT"
    assert captured[0]["upserts"][0]["row"]["כיוון ניתוח"] == "LONG"
    assert captured[0]["upserts"][1]["row"]["displayed_direction"] == "SHORT"
    assert captured[0]["upserts"][1]["row"]["analysis_direction"] == "LONG"
    assert captured[0]["upserts"][2]["row"]["event_id"] == "evt-1"
    assert captured[0]["upserts"][2]["row"]["snapshot_id"] == "sheet-snapshot-1"
    print("google_sheets_sync_selftest: PASS")


if __name__ == "__main__":
    run()
