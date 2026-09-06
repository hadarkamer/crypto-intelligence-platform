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


def _outcome_event(event_id=841):
    return {
        "event_id": event_id,
        "event_fingerprint": f"outcome-event-{event_id}",
        "alert_time_utc": "2026-09-05T12:00:00Z",
        "symbol": "SOL",
        "direction": "LONG",
        "engine_snapshot": {"sheet_snapshot_id": "shared-snapshot"},
    }


def _ordered_outcome(**overrides):
    row = {
        "window_minutes": 1440,
        "threshold_bps": 50,
        "threshold_pct": 0.50,
        "method_version": "ordered-first-touch-v7",
        "direction": "LONG",
        "status": "FAILURE",
        "first_touch_side": "ADVERSE",
        "terminal_reason": "ADVERSE_FIRST",
        "measurement_start_utc": "2026-09-05T12:00:00Z",
        # Use the persisted-table field names here.  The pure calculator also
        # supplies aliases, and the Sheets mapper accepts both representations.
        "first_observed_open_utc": "2026-09-05T12:00:00Z",
        "observed_through_utc": "2026-09-05T12:03:00Z",
        "decision_time_utc": "2026-09-05T12:03:00Z",
        "time_to_decision_seconds": 180,
        "favorable_barrier_price": 100.50,
        "adverse_barrier_price": 99.50,
        "mfe_pct": 0.18,
        "mae_pct": 0.50,
        "favorable_touch_price": None,
        "adverse_touch_price": 99.50,
        "max_favorable_price": 100.18,
        "max_adverse_price": 99.50,
        "path_samples": 3,
        "initial_gap_seconds": 0,
        "initial_gap_unobserved": False,
        "data_quality_note": "COMPLETE_CLOSED_1M_PATH",
        "path_complete": True,
    }
    row.update(overrides)
    return row


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

    # Max-Pain names the liquidation side; the two exported price directions
    # must invert it. Combined alerts store their captured balances by timeframe.
    for source_side, expected_long in (("SHORT", 70.0), ("LONG", 30.0)):
        data = Event().to_dict()
        data["source_side"] = source_side
        snapshot = {"near_share_pct": 70, "near_amount": 700, "far_amount": 300}
        liquidity = google_sheets_sync._liquidity_fields(data, snapshot)
        assert liquidity["liquidity_balance_pct"] == 70.0
        assert liquidity["liquidity_long_pct"] == expected_long
        assert liquidity["liquidity_short_pct"] == 100.0 - expected_long
        assert liquidity["selected_liquidity_usd"] == 700
        assert liquidity["opposite_liquidity_usd"] == 300
        assert liquidity["liquidity_timeframe"] == "4h"
        data["event_type"] = "COMBINED_CONFIRMATION"
        combined = google_sheets_sync._liquidity_fields(data, {
            "liquidity_imbalances": [
                {"timeframe": "1h", "share_pct": 80},
                {"timeframe": "4h", "share_pct": 70},
            ],
        })
        assert combined["liquidity_long_pct"] == expected_long
        assert combined["liquidity_short_pct"] == 100.0 - expected_long
        assert combined["liquidity_timeframe"] == "4h"
        assert combined["liquidity_data_source"] == "COMBINED_CAPTURED_TIMEFRAME"
        assert '"1h"' in combined["liquidity_by_timeframe_json"]
        assert '"4h"' in combined["liquidity_by_timeframe_json"]
        # A sole captured balance may belong to a different timeframe. Preserve
        # that source instead of relabeling it as the top item's timeframe.
        one = google_sheets_sync._liquidity_fields(data, {
            "liquidity_imbalances": [{"timeframe": "15m", "share_pct": 70}],
        })
        assert one["liquidity_long_pct"] == expected_long
        assert one["liquidity_timeframe"] == "15m"
        unknown = google_sheets_sync._liquidity_fields(data, {
            "liquidity_imbalances": [
                {"timeframe": "15m", "share_pct": 70},
                {"timeframe": "1h", "share_pct": 90},
            ],
        })
        assert unknown["liquidity_balance_pct"] is None
        assert unknown["liquidity_long_pct"] is None
        assert unknown["liquidity_by_timeframe_json"] is not None
        amounts = google_sheets_sync._liquidity_fields(data, {"near_amount": 700, "far_amount": 300})
        assert amounts["liquidity_long_pct"] == expected_long
    for invalid_share in (None, -1, 101, "nan", "inf"):
        missing = google_sheets_sync._liquidity_fields(Event().to_dict(), {"near_share_pct": invalid_share})
        assert missing["liquidity_long_pct"] is None
        assert missing["liquidity_short_pct"] is None
    magnet = Event().to_dict()
    magnet["event_type"] = "MAGNET_ALERT"
    missing = google_sheets_sync._liquidity_fields(magnet, {"magnet": {"liquidity_edge_pct": 70}})
    assert missing["liquidity_balance_pct"] is None

    # Legacy v6 is a one-sided target-touch label.  A MISS must therefore not
    # be exported as an adverse First Touch failure.  Extrema are still useful
    # diagnostics, including when path candles are dictionaries.
    legacy_captured = []
    original_enabled = google_sheets_sync.enabled
    original_enqueue = google_sheets_sync.enqueue
    try:
        google_sheets_sync.enabled = lambda: True
        google_sheets_sync.enqueue = (
            lambda payload: legacy_captured.append(payload) or True
        )
        event = _outcome_event()
        path = {
            "pair": "SOLUSDT",
            "candles": [
                {"high": 100.80, "low": 99.75},
                {"high": 100.40, "low": 99.60},
            ],
        }
        assert google_sheets_sync.enqueue_first_touch_outcome(
            event=event,
            horizon=60,
            reference_price=100.0,
            reference_source="ALERT_CURRENT_PRICE",
            path_result=path,
            first_touch={
                "direction": "LONG",
                "qualifying_move_threshold_pct": 0.50,
                "status": "HIT",
                "success": True,
                "failure_final": False,
                "first_qualifying_move_time_utc": "2026-09-05T12:05:00Z",
                "observed_through_utc": "2026-09-05T13:00:00Z",
                "qualifying_move_price": 100.50,
                "method_version": "no-dwell-first-touch-v6",
            },
            quality="VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES",
        ) is True
        for distinct_event, distinct_horizon, distinct_threshold in (
            (_outcome_event(842), 60, 0.50),
            (_outcome_event(841), 240, 1.00),
        ):
            assert google_sheets_sync.enqueue_first_touch_outcome(
                event=distinct_event,
                horizon=distinct_horizon,
                reference_price=100.0,
                reference_source="ALERT_CURRENT_PRICE",
                path_result=path,
                first_touch={
                    "direction": "LONG",
                    "qualifying_move_threshold_pct": distinct_threshold,
                    "status": "PENDING",
                    "success": None,
                    "failure_final": False,
                    "first_qualifying_move_time_utc": None,
                    "observed_through_utc": "2026-09-05T12:30:00Z",
                    "qualifying_move_price": None,
                    "method_version": "no-dwell-first-touch-v6",
                },
                quality="VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES",
            ) is True
        assert google_sheets_sync.enqueue_first_touch_outcome(
            event=event,
            horizon=60,
            reference_price=100.0,
            reference_source="ALERT_CURRENT_PRICE",
            path_result=path,
            first_touch={
                "direction": "LONG",
                "qualifying_move_threshold_pct": 0.50,
                "status": "MISS",
                "success": False,
                "failure_final": True,
                "first_qualifying_move_time_utc": None,
                "observed_through_utc": "2026-09-05T13:00:00Z",
                "qualifying_move_price": None,
                "method_version": "no-dwell-first-touch-v6",
            },
            quality="VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES",
        ) is True
        assert google_sheets_sync.enqueue_first_touch_outcome(
            event=event,
            horizon=60,
            reference_price=100.0,
            reference_source="ALERT_CURRENT_PRICE",
            path_result=path,
            first_touch={
                "direction": "LONG",
                "qualifying_move_threshold_pct": 0.50,
                "status": "PENDING",
                "success": None,
                "failure_final": False,
                "first_qualifying_move_time_utc": None,
                "observed_through_utc": "2026-09-05T12:30:00Z",
                "qualifying_move_price": None,
                "method_version": "no-dwell-first-touch-v6",
            },
            quality="VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES",
        ) is True
    finally:
        google_sheets_sync.enabled = original_enabled
        google_sheets_sync.enqueue = original_enqueue

    legacy_hit = legacy_captured[0]["upserts"][0]["row"]
    assert legacy_captured[0]["upserts"][0]["key"] == "outcome_id"
    assert legacy_hit["event_id"] == "841"
    assert legacy_hit["outcome_id"] == "841|60|50|no-dwell-first-touch-v6"
    assert legacy_hit["window_minutes"] == 60
    assert legacy_hit["threshold_bps"] == 50
    assert legacy_hit["status"] == "SUCCESS"
    assert legacy_hit["first_touch_side"] == "FAVORABLE"
    assert legacy_hit["decision_time_utc"] == "2026-09-05T12:05:00Z"
    assert legacy_hit["minutes_to_decision"] == 5.0
    assert legacy_hit["favorable_touch_price"] == 100.50
    assert legacy_hit["adverse_touch_price"] is None
    assert round(legacy_hit["mfe_pct"], 8) == 0.80
    assert round(legacy_hit["mae_pct"], 8) == 0.40
    assert legacy_hit["max_favorable_price"] == 100.80
    assert legacy_hit["max_adverse_price"] == 99.60
    assert legacy_hit["outcome_method_version"] == "no-dwell-first-touch-v6"

    legacy_miss = legacy_captured[3]["upserts"][0]["row"]
    assert legacy_miss["status"] == "UNRESOLVED"
    assert legacy_miss["first_touch_side"] == "NONE"
    assert legacy_miss["decision_time_utc"] == "2026-09-05T13:00:00Z"
    assert legacy_miss["minutes_to_decision"] == 60.0
    assert legacy_miss["favorable_touch_price"] is None
    assert legacy_miss["adverse_touch_price"] is None

    legacy_pending = legacy_captured[4]["upserts"][0]["row"]
    assert legacy_pending["status"] == "OPEN"
    assert legacy_pending["first_touch_side"] == "NONE"
    assert legacy_pending["decision_time_utc"] is None
    assert legacy_pending["minutes_to_decision"] is None
    assert {
        legacy_captured[index]["upserts"][0]["row"]["outcome_id"]
        for index in (0, 1, 2)
    } == {
        "841|60|50|no-dwell-first-touch-v6",
        "842|60|50|no-dwell-first-touch-v6",
        "841|240|100|no-dwell-first-touch-v6",
    }

    # The ordered v7 row carries both sides of the barrier decision.  In
    # particular, a real adverse-first failure has a decision time and price.
    ordered = google_sheets_sync.ordered_outcome_row(
        event=_outcome_event(),
        reference_source="ALERT_CURRENT_PRICE",
        path_result={"pair": "SOLUSDT"},
        outcome=_ordered_outcome(),
        quality="VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES",
    )
    assert set(ordered) == {
        "event_id",
        "snapshot_id",
        "symbol",
        "direction",
        "threshold_pct",
        "measurement_start_utc",
        "status",
        "first_touch_side",
        "decision_time_utc",
        "minutes_to_decision",
        "mfe_pct",
        "mae_pct",
        "favorable_touch_price",
        "adverse_touch_price",
        "max_favorable_price",
        "max_adverse_price",
        "market_source",
        "market_pair",
        "candle_interval",
        "candle_count",
        "data_quality_status",
        "outcome_method_version",
        "outcome_id",
        "window_minutes",
        "threshold_bps",
        "observed_from_utc",
        "observed_through_utc",
        "terminal_reason",
        "initial_gap_seconds",
        "favorable_barrier_price",
        "adverse_barrier_price",
        "initial_gap_unobserved",
        "data_quality_note",
        "path_complete",
    }
    assert ordered["event_id"] == "841"
    assert ordered["snapshot_id"] == "shared-snapshot"
    assert ordered["direction"] == "LONG"
    assert ordered["status"] == "FAILURE"
    assert ordered["first_touch_side"] == "ADVERSE"
    assert ordered["decision_time_utc"] == "2026-09-05T12:03:00Z"
    assert ordered["minutes_to_decision"] == 3.0
    assert ordered["favorable_touch_price"] is None
    assert ordered["adverse_touch_price"] == 99.50
    assert ordered["favorable_barrier_price"] == 100.50
    assert ordered["adverse_barrier_price"] == 99.50
    assert ordered["observed_from_utc"] == "2026-09-05T12:00:00Z"
    assert ordered["initial_gap_unobserved"] is False
    assert ordered["data_quality_note"] == "COMPLETE_CLOSED_1M_PATH"
    assert ordered["path_complete"] is True
    assert ordered["data_quality_status"] == (
        "VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES"
    )
    assert ordered["outcome_id"] == "841|1440|50|ordered-first-touch-v7"

    # Sheet identity mirrors the database primary key.  Rows sharing a merged
    # snapshot no longer overwrite each other, while a direction correction
    # for the same event updates the existing row instead of orphaning it.
    def outcome_id(event_id, **changes):
        return google_sheets_sync.ordered_outcome_row(
            event=_outcome_event(event_id),
            reference_source="ALERT_CURRENT_PRICE",
            path_result={"pair": "SOLUSDT"},
            outcome=_ordered_outcome(**changes),
            quality="VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES",
        )["outcome_id"]

    keys = {
        outcome_id(event_id, window_minutes=window, threshold_bps=threshold)
        for event_id in (841, 842)
        for window in (60, 240, 720, 1440)
        for threshold in (25, 50, 75, 100, 125, 150, 175, 200)
    }
    assert len(keys) == 64
    assert outcome_id(841, direction="SHORT") == outcome_id(841)
    assert outcome_id(842) != outcome_id(841)
    assert outcome_id(841, window_minutes=240) != outcome_id(841)
    assert outcome_id(841, threshold_bps=75) != outcome_id(841)
    assert outcome_id(841, method_version="ordered-first-touch-v8") != outcome_id(
        841
    )

    for invalid_event_id in (None, "", 0, -1, True):
        invalid_event = _outcome_event()
        invalid_event["event_id"] = invalid_event_id
        try:
            google_sheets_sync.ordered_outcome_row(
                event=invalid_event,
                reference_source="ALERT_CURRENT_PRICE",
                path_result={"pair": "SOLUSDT"},
                outcome=_ordered_outcome(),
                quality="VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES",
            )
        except ValueError as exc:
            assert "positive event_id" in str(exc)
        else:
            raise AssertionError(f"invalid event_id accepted: {invalid_event_id!r}")

    try:
        google_sheets_sync.ordered_outcome_row(
            event=_outcome_event(),
            reference_source="ALERT_CURRENT_PRICE",
            path_result={"pair": "SOLUSDT"},
            outcome=_ordered_outcome(),
            quality="",
        )
    except ValueError as exc:
        assert "data quality" in str(exc)
    else:
        raise AssertionError("blank ordered outcome quality was accepted")

    # Delivery is one synchronous, idempotent batch keyed only by outcome_id.
    delivered = []
    original_enabled = google_sheets_sync.enabled
    original_deliver_now = google_sheets_sync.deliver_now
    try:
        google_sheets_sync.enabled = lambda: True
        google_sheets_sync.deliver_now = (
            lambda payload: delivered.append(payload) or True
        )
        assert google_sheets_sync.deliver_ordered_first_touch_outcomes(
            event=_outcome_event(),
            reference_source="ALERT_CURRENT_PRICE",
            path_result={"pair": "SOLUSDT"},
            outcomes=[
                _ordered_outcome(threshold_bps=25, threshold_pct=0.25),
                _ordered_outcome(threshold_bps=50, threshold_pct=0.50),
            ],
            quality="VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES",
        ) is True
    finally:
        google_sheets_sync.enabled = original_enabled
        google_sheets_sync.deliver_now = original_deliver_now
    assert delivered[0]["kind"] == "ordered_first_touch_outcomes"
    assert len(delivered[0]["upserts"]) == 2
    assert all(item["sheet"] == "Outcomes" for item in delivered[0]["upserts"])
    assert all(item["key"] == "outcome_id" for item in delivered[0]["upserts"])
    assert [item["row"]["outcome_id"] for item in delivered[0]["upserts"]] == [
        "841|1440|25|ordered-first-touch-v7",
        "841|1440|50|ordered-first-touch-v7",
    ]
    # A durable delivery uses one bounded attempt. Retrying inside this call
    # could exceed the DB lease; the outbox is responsible for future retries.
    original_urlopen = google_sheets_sync.urlopen
    original_webhook = google_sheets_sync._WEBHOOK_URL
    original_enabled = google_sheets_sync.enabled
    original_receiver_version = google_sheets_sync._RECEIVER_VERSION
    original_batch_fallback = google_sheets_sync._ORDERED_BATCH_FALLBACK
    original_http_seconds = google_sheets_sync._LAST_HTTP_SECONDS
    calls = []
    class Response:
        body = b'{"ok":true}'
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return False
        def read(self):
            return self.body
    def successful_request(request, *, timeout):
        calls.append(timeout)
        return Response()
    def failing_request(request, *, timeout):
        calls.append(timeout)
        raise TimeoutError("simulated webhook timeout")
    try:
        google_sheets_sync.enabled = lambda: True
        google_sheets_sync._WEBHOOK_URL = "https://example.invalid/sheets-test"
        google_sheets_sync.urlopen = successful_request
        assert google_sheets_sync.deliver_now({"upserts": []}) is True
        assert calls == [google_sheets_sync._HTTP_TIMEOUT_SECONDS]
        assert 5 <= calls[0] <= 60
        calls.clear()
        google_sheets_sync.urlopen = failing_request
        assert google_sheets_sync.deliver_now({"upserts": []}) is False
        assert len(calls) == 1

        # A timed-out multirow request may already have changed the Sheet.
        # Later leases shrink, but it is never treated as acknowledged.
        google_sheets_sync._ORDERED_BATCH_FALLBACK = False
        assert google_sheets_sync.ordered_outcome_batch_limit() == 8
        outcome_payload = {"kind": "ordered_first_touch_outcomes", "upserts": []}
        assert google_sheets_sync.deliver_now(outcome_payload) is False
        assert google_sheets_sync.ordered_outcome_batch_limit() == 1
        google_sheets_sync.urlopen = successful_request
        assert google_sheets_sync.deliver_now(outcome_payload) is True
        assert google_sheets_sync.status()["receiver_version"] == "unversioned"
        assert google_sheets_sync.ordered_outcome_batch_limit() == 1
        # Only the successfully deployed batch implementation ends fallback.
        Response.body = b'{"ok":true,"version":"sheets-batch-v2"}'
        assert google_sheets_sync.deliver_now(outcome_payload) is True
        assert google_sheets_sync.ordered_outcome_batch_limit() == 8
        assert google_sheets_sync.status()["receiver_version"] == "sheets-batch-v2"
        assert google_sheets_sync.status()["last_http_seconds"] >= 0
        google_sheets_sync.urlopen = failing_request
        assert google_sheets_sync.deliver_now({"kind": "delivered_event"}) is False
        assert google_sheets_sync.ordered_outcome_batch_limit() == 8
    finally:
        google_sheets_sync.urlopen = original_urlopen
        google_sheets_sync._WEBHOOK_URL = original_webhook
        google_sheets_sync.enabled = original_enabled
        google_sheets_sync._RECEIVER_VERSION = original_receiver_version
        google_sheets_sync._ORDERED_BATCH_FALLBACK = original_batch_fallback
        google_sheets_sync._LAST_HTTP_SECONDS = original_http_seconds
    print("google_sheets_sync_selftest: PASS")


if __name__ == "__main__":
    run()
