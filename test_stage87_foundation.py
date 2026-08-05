from datetime import datetime, timezone
import sqlite3

import coinglass_flow_foundation as flow
import coinglass_history_backfill as history
import coinglass_oi_regime_service as regime


def test_extended_oi_reference_windows():
    assert history.WINDOWS["48h"] == 96
    assert history.WINDOWS["72h"] == 144
    assert history.WINDOWS["7d"] == 336
    assert history.BACKFILL_DAYS == 180
    assert history.MAX_BACKFILL_DAYS == 365


def test_price_oi_quality_boundaries():
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert regime._quality_status(t0, t0)[1] == "PASS"
    assert regime._quality_status(t0, t0.replace(second=45))[1] == "WARNING"
    assert regime._quality_status(t0, t0.replace(minute=2))[1] == "INVALID"


def test_cvd_normalization_supports_seconds_and_milliseconds():
    payload = {"data": [
        {"time": 1762254000, "agg_taker_buy_vol": 100, "agg_taker_sell_vol": 80, "cum_vol_delta": 20},
        {"time": 1762257600000, "agg_taker_buy_vol": 90, "agg_taker_sell_vol": 110, "cum_vol_delta": 0},
    ]}
    rows = flow._normalize(payload)
    assert 1762254000000 in rows
    assert 1762257600000 in rows
    assert rows[1762254000000] == (100.0, 80.0, 20.0)


def test_chunk_cvd_is_stitched_without_losing_api_value():
    chunks = [
        {1000: (100.0, 80.0, 20.0), 2000: (90.0, 100.0, 10.0)},
        {3000: (120.0, 100.0, 20.0), 4000: (130.0, 100.0, 50.0)},
    ]
    rows = flow._stitch_chunks(chunks)
    assert rows[1000][2] == 20.0  # API CVD untouched
    assert rows[3000][2] == 20.0  # API reset visible
    assert rows[1000][3] == 20.0
    assert rows[2000][3] == 10.0
    assert rows[3000][3] == 10.0
    assert rows[4000][3] == 40.0


def test_flow_tables_upsert_without_duplicates_and_store_cvd(tmp_path):
    flow.DATABASE_URL = ""
    flow.DB_PATH = str(tmp_path / "flow.db")
    rows = {
        1_760_000_000_000: (10.0, 8.0, 2.0, 2.0),
        1_760_001_800_000: (12.0, 9.0, 5.0, 5.0),
    }
    assert flow._store("BTC", "futures", rows) == 2
    assert flow._store("BTC", "futures", rows) == 2
    with sqlite3.connect(flow.DB_PATH) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM futures_taker_history WHERE symbol='BTC'"
        ).fetchone()[0]
        saved = conn.execute(
            "SELECT api_cum_vol_delta_usd,continuous_cum_vol_delta_usd "
            "FROM futures_taker_history WHERE symbol='BTC' ORDER BY candle_time LIMIT 1"
        ).fetchone()
    assert count == 2
    assert saved == (2.0, 2.0)


def test_flow_foundation_uses_official_cvd_endpoints_without_flow_decision():
    assert flow.FUTURES_ENDPOINT == "/api/futures/aggregated-cvd/history"
    assert flow.SPOT_ENDPOINT == "/api/spot/aggregated-cvd/history"
    assert flow.INTERVAL == "30m"
    assert not hasattr(flow, "calculate_flow")


def test_reference_chooses_nearest_snapshot_on_either_side(monkeypatch):
    from datetime import timedelta
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    target = now - timedelta(hours=1)
    rows = [
        {"collected_at": (target - timedelta(minutes=19)).isoformat(), "price": 100, "open_interest_usd": 1000},
        {"collected_at": (target + timedelta(minutes=11)).isoformat(), "price": 101, "open_interest_usd": 1001},
    ]
    monkeypatch.setattr(history, "historical_point_nearest", lambda symbol, target: None)
    ref = regime._reference_for_window(rows, now, 60, "BTC")
    assert ref is not None
    assert ref["price"] == 101
    assert ref["reference_offset_seconds"] == 11 * 60
    assert ref["reference_signed_offset_seconds"] == 11 * 60


def test_reference_falls_back_to_closer_historical_snapshot(monkeypatch):
    from datetime import timedelta
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    target = now - timedelta(hours=4)
    rows = [
        {"collected_at": (target - timedelta(minutes=40)).isoformat(), "price": 90, "open_interest_usd": 900},
    ]
    monkeypatch.setattr(history, "historical_point_nearest", lambda symbol, requested: {
        "collected_at": (target + timedelta(minutes=5)).isoformat(),
        "price": 105,
        "open_interest_usd": 1100,
        "source": "historical_backfill",
    })
    ref = regime._reference_for_window(rows, now, 240, "BTC")
    assert ref is not None
    assert ref["source"] == "historical_backfill"
    assert ref["reference_offset_seconds"] == 5 * 60


def test_reference_rejects_nearest_outside_tolerance(monkeypatch):
    from datetime import timedelta
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    target = now - timedelta(hours=12)
    rows = [{"collected_at": (target - timedelta(minutes=21)).isoformat(), "price": 100, "open_interest_usd": 1000}]
    monkeypatch.setattr(history, "historical_point_nearest", lambda symbol, target: None)
    assert regime._reference_for_window(rows, now, 720, "BTC") is None


def test_rate_limit_message_detection():
    assert flow._is_rate_limit_message("Too Many Requests")
    assert flow._is_rate_limit_message("rate limit exceeded")
    assert not flow._is_rate_limit_message("invalid symbol")


def test_current_coverage_is_skipped():
    from datetime import timedelta
    now = datetime(2026, 8, 5, 10, 47, tzinfo=timezone.utc)
    # At 10:47 with 2m grace, the newest eligible open timestamp is 10:00.
    assert flow.latest_eligible_candle_time(now) == datetime(2026, 8, 5, 10, 0, tzinfo=timezone.utc)
    assert flow._is_current({"count": 4319, "max_time": now.replace(minute=0)}, now)
    # 09:30 is one closed candle behind and must never be treated as current.
    assert not flow._is_current({"count": 4319, "max_time": now.replace(hour=9, minute=30)}, now)
    assert not flow._is_current({"count": 0, "max_time": None}, now)


def test_latest_eligible_candle_respects_grace_period():
    # 10:31 is still inside the 2-minute grace window after the 10:30 close.
    before_grace = datetime(2026, 8, 5, 10, 31, tzinfo=timezone.utc)
    assert flow.latest_eligible_candle_time(before_grace) == datetime(2026, 8, 5, 9, 30, tzinfo=timezone.utc)
    # At 10:32, the 10:00 open / 10:30 close candle becomes eligible.
    after_grace = datetime(2026, 8, 5, 10, 32, tzinfo=timezone.utc)
    assert flow.latest_eligible_candle_time(after_grace) == datetime(2026, 8, 5, 10, 0, tzinfo=timezone.utc)


def test_continuous_cvd_rebuilt_from_buy_minus_sell(tmp_path):
    flow.DATABASE_URL = ""
    flow.DB_PATH = str(tmp_path / "cvd_rebuild.db")
    rows = {
        1_760_000_000_000: (10.0, 8.0, 999.0, 0.0),
        1_760_001_800_000: (7.0, 12.0, 500.0, 0.0),
    }
    flow._store("BTC", "futures", rows)
    assert flow._rebuild_continuous_cvd("BTC", "futures") == 2
    with sqlite3.connect(flow.DB_PATH) as conn:
        values = [r[0] for r in conn.execute(
            "SELECT continuous_cum_vol_delta_usd FROM futures_taker_history "
            "WHERE symbol='BTC' ORDER BY candle_time"
        ).fetchall()]
    assert values == [2.0, -3.0]


def test_current_backfill_rebuilds_continuous_cvd_before_skip(monkeypatch):
    from datetime import timedelta
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    monkeypatch.setattr(flow, "coverage", lambda symbol, market: {
        "count": 4320,
        "min_time": now - timedelta(days=180),
        "max_time": now,
    })
    rebuilt = []
    monkeypatch.setattr(flow, "_rebuild_continuous_cvd", lambda symbol, market: rebuilt.append((symbol, market)) or 4320)
    result = flow.backfill_symbol("BTC", "futures", 180, force=False)
    assert result["skipped"] is True
    assert result["total_rows"] == 4320
    assert rebuilt == [("BTC", "futures")]
    assert "rebuilt" in result["message"]
