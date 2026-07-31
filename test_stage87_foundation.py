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
