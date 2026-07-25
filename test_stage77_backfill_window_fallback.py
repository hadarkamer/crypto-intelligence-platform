from datetime import datetime, timezone

import coinglass_oi_regime_service as svc


def _hist_row(ts, price=100.0, oi=1000.0):
    return {"collected_at": ts, "price": price, "open_interest_usd": oi}


def test_live_reference_has_priority(monkeypatch):
    now = datetime(2026, 7, 25, 20, 0, tzinfo=timezone.utc)
    rows = [_hist_row("2026-07-25T15:30:00+00:00", 90, 900)]
    monkeypatch.setattr(
        svc.history_reference,
        "historical_point_at_or_before",
        lambda symbol, target: {"collected_at": "2026-07-25T15:00:00+00:00", "price": 80, "open_interest_usd": 800, "source": "historical_backfill"},
    )
    ref = svc._reference_for_window(rows, now, 240, "BTC")
    assert ref["price"] == 90
    assert ref["source"] == "live_snapshot"


def test_backfill_reference_used_when_live_history_too_short(monkeypatch):
    now = datetime(2026, 7, 25, 20, 0, tzinfo=timezone.utc)
    rows = [_hist_row("2026-07-25T19:00:00+00:00", 99, 990)]
    monkeypatch.setattr(
        svc.history_reference,
        "historical_point_at_or_before",
        lambda symbol, target: {"collected_at": "2026-07-25T16:00:00+00:00", "price": 95, "open_interest_usd": 950, "source": "historical_backfill"},
    )
    ref = svc._reference_for_window(rows, now, 240, "BTC")
    assert ref["price"] == 95
    assert ref["source"] == "historical_backfill"


def test_window_result_calculates_from_backfill_fallback(monkeypatch):
    now = datetime(2026, 7, 25, 20, 0, tzinfo=timezone.utc)
    rows = [_hist_row("2026-07-25T19:30:00+00:00", 100, 1000)]

    def fake_point(symbol, target):
        # Return a valid reference for all requested windows.
        return {"collected_at": target.isoformat(), "price": 100.0, "open_interest_usd": 1000.0, "source": "historical_backfill"}

    monkeypatch.setattr(svc.history_reference, "historical_point_at_or_before", fake_point)
    monkeypatch.setattr(svc.history_reference, "reference_for_window", lambda symbol, label: {})

    windows = svc._window_results("BTC", 101.0, 1010.0, now, rows)
    # 4h is too old for the one live row, so it must come from backfill.
    assert windows["4h"]["available"] is True
    assert windows["4h"]["comparison_source"] == "historical_backfill"
    assert round(windows["4h"]["price_change_pct"], 6) == 1.0
    assert round(windows["4h"]["oi_change_pct"], 6) == 1.0
