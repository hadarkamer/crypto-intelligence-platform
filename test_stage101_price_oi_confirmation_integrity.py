import ast
from datetime import datetime, timedelta, timezone
import html
from pathlib import Path
from threading import Barrier
from typing import Any, Dict

import coinglass_oi_regime_service as regime
import live_price_provider
import market_confidence_engine as confidence


def _bullish_window(rank):
    return {
        "available": True,
        "state": "BULLISH_BUILDUP",
        "price_strength": {"rank": rank},
        "oi_strength": {"rank": rank},
    }


def test_all_bullish_price_oi_families_reach_confirmation_module():
    windows = {
        "30m": _bullish_window(2),
        "1h": _bullish_window(2),
        "4h": _bullish_window(1),
        "12h": _bullish_window(1),
        "24h": _bullish_window(1),
        "48h": _bullish_window(1),
        "72h": _bullish_window(1),
        "7d": _bullish_window(1),
    }
    payload = {
        "available": True,
        "data_quality_status": "PASS",
        "windows": windows,
        "overall": {"state": "BULLISH_BUILDUP", "label": "Bullish Build-up"},
    }
    out = confidence._positioning_module(payload, "BULLISH")
    assert out["direction"] == "BULLISH"
    assert out["relation"] == "SUPPORT"
    assert out["score"] == 60.0


def test_invalid_price_oi_snapshot_is_not_recomputed_as_bullish(monkeypatch):
    current = {
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "price": 100.0,
        "open_interest_usd": 1_000_000.0,
        "data_quality_status": "INVALID",
        "time_gap_seconds": 75.0,
        "price_fetched_at": datetime.now(timezone.utc).isoformat(),
        "oi_fetched_at": datetime.now(timezone.utc).isoformat(),
        "price_source": "binance_spot",
        "oi_source": "coinglass_all",
    }
    monkeypatch.setattr(regime, "_history", lambda _symbol: [current])
    monkeypatch.setattr(
        regime,
        "_window_results",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("invalid snapshot must not be recomputed")
        ),
    )
    out = regime.latest("ZEC")
    assert out["available"] is False
    assert out["overall"]["state"] == "UNAVAILABLE"
    assert all(not window.get("available") for window in out["windows"].values())


def test_invalid_snapshot_is_not_used_as_a_future_window_reference(monkeypatch):
    now = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)
    target = now - timedelta(hours=1)
    rows = [
        {
            "collected_at": target.isoformat(),
            "price": 101.0,
            "open_interest_usd": 1_010_000.0,
            "data_quality_status": "INVALID",
        },
        {
            "collected_at": (target - timedelta(minutes=10)).isoformat(),
            "price": 100.0,
            "open_interest_usd": 1_000_000.0,
            "data_quality_status": "PASS",
        },
    ]
    monkeypatch.setattr(
        regime.history_reference, "historical_point_nearest", lambda *_args: None
    )
    monkeypatch.setattr(
        regime.history_reference, "historical_point_at_or_before", lambda *_args: None
    )
    ref = regime._reference_for_window(rows, now, 60, "ZEC")
    assert ref is not None
    assert ref["price"] == 100.0
    assert ref["reference_offset_seconds"] == 10 * 60


def test_collect_many_fetches_oi_in_parallel_but_processes_serially(monkeypatch):
    barrier = Barrier(2)
    processed = []

    def fake_fetch(symbol):
        barrier.wait(timeout=2)
        return {
            "value": 1_000_000.0,
            "fetched_at": datetime.now(timezone.utc),
            "source": "test",
            "symbol": symbol,
        }

    def fake_process(symbol, price, oi_meta):
        processed.append((symbol, price, oi_meta["symbol"]))
        return {"symbol": symbol, "available": True}

    monkeypatch.setattr(regime, "fetch_aggregated_oi_with_meta", fake_fetch)
    monkeypatch.setattr(regime, "_collect_symbol_with_oi_meta", fake_process)
    out = regime.collect_many({"BTC": {"price": 1}, "ZEC": {"price": 2}})
    assert set(out) == {"BTC", "ZEC"}
    assert processed == [
        ("BTC", {"price": 1}, "BTC"),
        ("ZEC", {"price": 2}, "ZEC"),
    ]


def test_each_price_uses_its_source_response_timestamp(monkeypatch):
    moments = iter(
        [
            datetime(2026, 8, 10, 12, 0, 1, tzinfo=timezone.utc),
            datetime(2026, 8, 10, 12, 0, 4, tzinfo=timezone.utc),
            datetime(2026, 8, 10, 12, 0, 5, tzinfo=timezone.utc),
        ]
    )

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return next(moments)

    monkeypatch.setattr(live_price_provider, "datetime", Clock)
    monkeypatch.setattr(
        live_price_provider,
        "_fetch_futures_mark_prices",
        lambda: {"ETHUSDT": 2_500.0},
    )
    monkeypatch.setattr(
        live_price_provider,
        "_fetch_spot_prices",
        lambda: {"BTCUSDT": 60_000.0},
    )
    result = live_price_provider.fetch_binance_usdt_prices(["BTC", "ETH"])
    assert result["prices"]["ETH"]["fetched_at_utc"].endswith("12:00:01+00:00")
    assert result["prices"]["BTC"]["fetched_at_utc"].endswith("12:00:04+00:00")
    assert result["fetched_at_utc"].endswith("12:00:05+00:00")


def test_confirmation_display_separates_core_engines_from_spot():
    source = Path(__file__).with_name("main.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    wanted = {
        "_flow_direction_icon",
        "_direction_display_he",
        "_display_text_he",
        "_market_evidence_block",
    }
    functions = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    namespace = {"html": html, "Any": Any, "Dict": Dict}
    exec(compile(ast.Module(body=functions, type_ignores=[]), "main.py", "exec"), namespace)
    rendered = namespace["_market_evidence_block"](
        {
            "market_evidence": {
                "modules": {
                    "positioning": {
                        "available": False,
                        "direction": "NEUTRAL",
                        "score": 0,
                        "label": "Price/OI timestamp gap too large",
                    },
                    "futures_flow": {
                        "available": True,
                        "direction": "BULLISH",
                        "score": 50,
                        "label": "Bullish",
                    },
                    "spot_flow": {
                        "available": True,
                        "direction": "BULLISH",
                        "score": 40,
                    },
                },
                "core_supporting_families": 1,
                "core_opposing_families": 0,
                "classification_label": "עדות חלקית",
                "confirmation": {"status": "UNCONFIRMED"},
            }
        }
    )
    assert "מחיר+OI: <b>לא זמין</b>" in rendered
    assert "מנועי ליבה תומכים: <b>1/2</b>" in rendered
    assert "Spot משני בלבד" in rendered
    assert "אינו מצביע באישור" in rendered
    assert "הסכמה: 🟢" not in rendered
