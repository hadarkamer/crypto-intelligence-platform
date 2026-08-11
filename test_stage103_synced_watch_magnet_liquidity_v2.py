import ast
import asyncio
from datetime import datetime, timezone
from pathlib import Path

import magnet_v1


ROOT = Path(__file__).parent


def test_liquidity_v2_uses_named_baseline_and_incremental_layers_without_distance():
    result = magnet_v1._liquidity_diagnostics([
        {
            "timeframe": "12h",
            "candidate_liquidity": 60_000_000,
            "opposite_liquidity": 40_000_000,
        },
        {
            "timeframe": "24h",
            "candidate_liquidity": 75_000_000,
            "opposite_liquidity": 100_000_000,
        },
    ])
    base, increment = result["liquidity_details"]
    assert base["layer_type"] == "BASE"
    assert base["timeframe"] == "12h"
    assert base["previous_timeframe"] is None
    assert increment["layer_type"] == "INCREMENT"
    assert increment["previous_timeframe"] == "12h"
    assert increment["candidate_liquidity"] == 15_000_000
    assert increment["opposite_liquidity"] == 60_000_000
    assert result["liquidity_edge_pct"] == -14.29
    assert result["consistency_pct"] == 38.46
    assert result["distance_weighting_enabled"] is False
    assert all(not item["distance_weight_applied"] for item in result["liquidity_details"])


def test_non_monotonic_cumulative_layer_is_flagged_and_excluded():
    result = magnet_v1._liquidity_diagnostics([
        {
            "timeframe": "12h",
            "candidate_liquidity": 60,
            "opposite_liquidity": 40,
        },
        {
            "timeframe": "24h",
            "candidate_liquidity": 55,
            "opposite_liquidity": 90,
        },
    ])
    assert result["non_monotonic_layers"] == ["24h"]
    assert result["liquidity_details"][1]["valid"] is False
    # Only the valid 12h baseline remains in the edge.
    assert result["liquidity_edge_pct"] == 20.0


def test_magnet_quality_and_legacy_score_contract_are_unchanged():
    assert magnet_v1._concentration_quality(1.0) == 50.0
    assert magnet_v1._concentration_quality(0.8) == 60.0
    source = (ROOT / "magnet_v1.py").read_text(encoding="utf-8")
    assert "does not participate in the legacy alert score" in source


def test_watch_deadline_is_aligned_to_next_half_hour_not_cycle_completion():
    # Importing main does not start Telegram or the web server.
    import main

    assert main.WATCH_INTERVAL_MINUTES >= 30
    main.WATCH_INTERVAL_MINUTES = 30
    main.WATCH_SYNC_GRACE_SECONDS = 135
    assert main._next_aligned_watch_time(
        datetime(2026, 8, 11, 14, 10, tzinfo=timezone.utc)
    ) == datetime(2026, 8, 11, 14, 32, 15, tzinfo=timezone.utc)
    assert main._next_aligned_watch_time(
        datetime(2026, 8, 11, 14, 40, tzinfo=timezone.utc)
    ) == datetime(2026, 8, 11, 15, 2, 15, tzinfo=timezone.utc)


def test_commands_and_shared_watch_coordinator_are_wired_once():
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    function_names = [
        node.name for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]
    for name in (
        "oi_refresh_cmd",
        "market_state_cmd",
        "watch_magnet_v1_cmd",
        "watch_magnet_v1_status_cmd",
        "watch_magnet_v1_stop_cmd",
    ):
        assert function_names.count(name) == 1
    for command in (
        "oi_refresh",
        "market_state",
        "watch_magnet_v1",
        "watch_magnet_v1_status",
        "watch_magnet_v1_stop",
    ):
        assert source.count(f'CommandHandler("{command}"') == 1
    assert 'name="shared-watch-coordinator"' in source
    assert "_send_magnet_watch_reports" in source
    assert "await asyncio.gather(" in source
    assert "_ensure_watch_derivatives_ready()" in source


def test_backfill_partial_run_does_not_advance_daily_freshness():
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    assert "if ok_count == total_count:" in source
    assert "partial; freshness NOT advanced" in source
    assert "_HISTORY_BACKFILL_LOCK_ID" in source


def test_oi_refresh_single_flight_joins_one_collection(monkeypatch):
    import main

    calls = 0

    async def fake_collect():
        nonlocal calls
        calls += 1
        await asyncio.sleep(0)
        return {"BTC": {"ok": True}}

    async def run_concurrently():
        main.OI_REFRESH_TASK = None
        return await asyncio.gather(
            main._request_oi_refresh(),
            main._request_oi_refresh(),
        )

    monkeypatch.setattr(main, "_collect_oi_regime_once", fake_collect)
    results = asyncio.run(run_concurrently())

    assert calls == 1
    assert results[0] == results[1]
    assert main.OI_REFRESH_TASK is None


def test_flow_refresh_single_flight_joins_one_collection(monkeypatch):
    import main

    calls = 0

    async def fake_collect():
        nonlocal calls
        calls += 1
        await asyncio.sleep(0)
        return {"BTC": {"30m": {"status": "ok"}}}

    async def run_concurrently():
        main.FLOW_REFRESH_TASK = None
        return await asyncio.gather(
            main._request_flow_refresh(),
            main._request_flow_refresh(),
        )

    monkeypatch.setattr(main, "_collect_flow_once", fake_collect)
    results = asyncio.run(run_concurrently())

    assert calls == 1
    assert results[0] == results[1]
    assert main.FLOW_REFRESH_TASK is None
