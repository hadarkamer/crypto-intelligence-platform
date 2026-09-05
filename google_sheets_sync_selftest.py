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


class DirectEvent:
    def to_dict(self):
        data = Event().to_dict()
        data.update({
            "event_fingerprint": "evt-2",
            "event_type": "OI_PRICE_HIGH",
            "direction": "LONG",
            "source_side": "BULLISH",
            "timeframe": None,
            "score": 99,
            "target_price": None,
            "initial_target_distance_pct": None,
            "categories": ["DERIVATIVES_HIGH_65", "positioning"],
        })
        data["engine_snapshot"] = dict(data["engine_snapshot"])
        data["engine_snapshot"].update({
            "displayed_direction": "LONG",
            "analysis_direction": "LONG",
            "opposite_score": None,
            "average_score_all_timeframes": None,
            "opposite_average_score_all_timeframes": None,
        })
        return data


class NeutralEvent:
    def to_dict(self):
        return {
            "event_fingerprint": "neutral-btc-long-1",
            "event_type": "PROSPECTIVE_NEUTRAL_30M",
            "alert_time_utc": "2026-09-05T10:00:00Z",
            "symbol": "BTC",
            "direction": "LONG",
            "current_price": 101.25,
            "strategy_version": "prospective-neutral-v4",
            "code_version": "test",
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
        google_sheets_sync._SNAPSHOT_CACHE.clear()
        assert google_sheets_sync.enqueue_delivered_event(Event()) is True
        assert google_sheets_sync.enqueue_delivered_event(DirectEvent()) is True
        assert google_sheets_sync.enqueue_neutral_snapshot(
            NeutralEvent(),
            decision_feature_bundle={
                "sampler_version": "prospective-neutral-v4",
                "model_score_status": "ABSENT",
                "features_by_direction": {"LONG": {
                    "time.market_session": "WEEKEND",
                    "time.is_market_weekend": True,
                }},
            },
            anchor_slot_id=42,
            feature_bundle_policy_version="formula-visible-v1",
            feature_bundle_sha256="abcdef0123456789fedcba",
        ) is True
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
    merged = captured[1]["upserts"][1]["row"]
    assert merged["primary_alert_type"] == "MAX_PAIN_ALERT"
    assert merged["displayed_direction"] == "SHORT"
    assert merged["analysis_direction"] == "LONG"
    assert merged["maxpain_selected_score"] == 78
    assert captured[1]["upserts"][0]["row"]["כיוון מוצג"] == "SHORT"
    assert captured[1]["upserts"][2]["row"]["displayed_direction"] == "LONG"
    neutral = captured[2]
    assert neutral["kind"] == "neutral_snapshot"
    assert len(neutral["upserts"]) == 2
    assert {item["sheet"] for item in neutral["upserts"]} == {
        "תצוגת לייב", "Snapshots"
    }
    neutral_snapshot = neutral["upserts"][1]["row"]
    assert neutral_snapshot["no_alert_snapshot"] is True
    assert neutral_snapshot["alert_sent"] is False
    assert neutral_snapshot["telegram_event_count"] == 0
    assert neutral_snapshot["market_session"] == "WEEKEND"
    assert "price_oi_total_score" not in neutral_snapshot
    assert neutral["upserts"][0]["row"]["נשלחה התראה"] == "לא"
    assert neutral["upserts"][0]["row"]["שלישייה 65+"] == "לא נמדד"
    print("google_sheets_sync_selftest: PASS")


if __name__ == "__main__":
    run()
