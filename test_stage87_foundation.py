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


def test_flow_tables_upsert_without_duplicates(tmp_path):
    flow.DATABASE_URL = ""
    flow.DB_PATH = str(tmp_path / "flow.db")
    rows = {1000: (10.0, 8.0, 2.0), 2000: (12.0, 9.0, 5.0)}
    assert flow._store("BTC", "futures", rows) == 2
    assert flow._store("BTC", "futures", rows) == 2
    with sqlite3.connect(flow.DB_PATH) as conn:
        count = conn.execute("SELECT COUNT(*) FROM futures_taker_history WHERE symbol='BTC'").fetchone()[0]
    assert count == 2


def test_flow_foundation_stores_official_cvd_without_flow_calculation():
    assert flow.FUTURES_ENDPOINT == "/api/futures/aggregated-cvd/history"
    assert flow.SPOT_ENDPOINT == "/api/spot/aggregated-cvd/history"
    assert not hasattr(flow, "calculate_flow")
    assert flow.INTERVAL == "30m"


def test_official_cvd_payload_normalization():
    rows = flow._normalize({"data": [{
        "time": 1000,
        "agg_taker_buy_vol": 12.0,
        "agg_taker_sell_vol": 7.0,
        "cum_vol_delta": 5.0,
    }]})
    assert rows == {1000: (12.0, 7.0, 5.0)}
