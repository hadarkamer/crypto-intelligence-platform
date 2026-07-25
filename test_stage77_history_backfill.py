from datetime import datetime, timedelta, timezone

import coinglass_history_backfill as h


def test_percentile_and_distribution():
    d = h._distribution([1, 2, 3, 4, 5])
    assert d["count"] == 5
    assert d["median"] == 3
    assert d["p25"] == 2
    assert d["p75"] == 4


def test_reference_ranges_are_per_window(tmp_path):
    h.DATABASE_URL = ""
    h.DB_PATH = str(tmp_path / "history.db")
    h.init_db()

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    matched = []
    # 60 half-hour candles. Price rises 1% per candle; OI rises 2% per candle.
    price = 100.0
    oi = 1000.0
    for i in range(60):
        ts = int((start + timedelta(minutes=30 * i)).timestamp() * 1000)
        matched.append((ts, price, oi))
        price *= 1.01
        oi *= 1.02
    h._store_rows("BTC", matched, "Binance", "BTCUSDT")
    stats = h.calculate_reference_ranges("BTC")
    assert stats["available"] is True
    assert set(stats["windows"]) == {"30m", "1h", "4h", "12h", "24h"}
    assert stats["windows"]["30m"]["oi_abs_change_pct"]["median"] > 1.9
    assert stats["windows"]["1h"]["oi_abs_change_pct"]["median"] > 4.0
    assert stats["windows"]["24h"]["samples"] == 12


def test_backfill_table_is_separate(tmp_path):
    h.DATABASE_URL = ""
    h.DB_PATH = str(tmp_path / "history.db")
    h.init_db()
    import sqlite3
    with sqlite3.connect(h.DB_PATH) as conn:
        names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "oi_price_history" in names
    assert "oi_regime_snapshots" not in names
