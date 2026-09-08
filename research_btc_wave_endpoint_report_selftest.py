"""Behavioral regression checks for the separate full-wave report contract."""
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
import math

import research_btc_wave_endpoint_report as report
import binance_futures_mark_price_path as mark

START = datetime(2026, 9, 6, 0, 0, tzinfo=timezone.utc)
POLICY = report.POLICY_VERSION
ROUTE = {"symbol": "BTC", "exchange": "binance", "market": "spot", "pair": "BTCUSDT",
         "interval": "1m", "interval_seconds": 60, "complete": True}


def wave(wave_id="wave", minutes=60):
    return {"btc_parent_movement_id": wave_id, "episode_policy_version": POLICY,
        "start_time_utc": START - timedelta(minutes=1),
        "end_time_utc": START + timedelta(minutes=minutes) - report.MILLISECOND,
        "observed_through_utc": START + timedelta(minutes=minutes) - report.MILLISECOND,
        "closing_boundary_verified": True,
        "evidence_eligible": True, "boundary_reason": "CAUSAL_CLOSE_REVERSAL"}


def event(event_id=1, symbol="BTC", direction="LONG", seconds=0, wave_id="wave"):
    stamp = START + timedelta(seconds=seconds)
    return {"event_id": event_id, "event_kind": "ALERT", "event_type": "MAX_PAIN_ALERT",
        "alert_time_utc": stamp, "symbol": symbol, "direction": direction, "score": 70.,
        "current_price": 100., "delivery_status": "DELIVERED", "coin_scope": "ALL",
        "engine_snapshot": {"price_source": "binance_spot", "price_pair": symbol + "USDT"},
        "btc_parent_movement_id": wave_id, "membership_parent_id": wave_id,
        "membership_status": "LIVE", "membership_policy_version": POLICY,
        "decision_time_utc": stamp, "btc_observed_close_utc": stamp - report.MILLISECOND,
        "event_fingerprint": format(event_id, "064x"), "strategy_version": "test", "code_version": "test"}


def candle(i, high=100., low=100., reference=100.):
    opened = START + timedelta(minutes=i)
    return {"open_time_utc": opened, "close_time_utc": opened + report.MINUTE - report.MILLISECOND,
            "open": reference, "high": high, "low": low, "close": reference}


def compute(bars, **kwargs):
    args = dict(event=event(), wave=wave(), path_result={**ROUTE, "candles": bars},
                observed_at=START + timedelta(hours=2))
    args.update(kwargs)
    return report.calculate_wave_record(**args)


def check_full_wave_after_first_touch_and_active():
    bars = [candle(i) for i in range(60)]
    bars[0] = candle(0, high=101., low=99.9)
    bars[-1] = candle(59, high=105., low=97.)
    row = compute(bars)
    assert row["status"] == "READY" and row["mfe_pct"] == 5 and row["mae_pct"] == 3
    assert len(row["thresholds"]) == 8
    assert row["thresholds"][0]["status"] == "SUCCESS"
    assert row["thresholds"][0]["mfe_pct"] == 1
    assert row["observed_through_utc"] == wave()["end_time_utc"]
    active = compute(bars, wave={**wave(), "end_time_utc": None}, observed_at=START + timedelta(minutes=60, seconds=30))
    assert active["status"] == "OPEN" and not active["path_complete"]
    assert active["mfe_pct"] is None and active["provisional_observed_mfe_pct"] == 5
    assert active["thresholds"][0]["status"] == "SUCCESS"  # Does not close the wave.
    rows = report.aggregate_records([active])
    assert len(rows) == 8 and rows[0]["success_probability"] is None
    assert rows[0]["provisional_observed_success_probability"] == 1
    assert math.isclose(rows[0]["provisional_observed_asymmetry_ratio"], 5/3)
    stale = compute(bars, wave={**wave(), "end_time_utc": None,
        "observed_through_utc": START + timedelta(minutes=30) - report.MILLISECOND},
        observed_at=START + timedelta(hours=2))
    assert stale["status"] == "OPEN" and stale["path_samples"] == 30
    assert stale["provisional_observed_mfe_pct"] == 1  # Newer coin bars cannot extend an unverified parent.


def check_complete_prefix_and_ambiguity():
    bars = [candle(i) for i in range(60)]
    bars[0] = candle(0, high=101., low=99.)
    row = compute(bars)
    assert row["thresholds"][0]["first_touch_side"] == "AMBIGUOUS"
    assert row["status"] == "READY"  # Ambiguous order does not erase full excursions.
    for bad in (bars[1:], bars[:-1], bars + [bars[-1]], bars[:20] + bars[21:]):
        missing = compute(bad)
        assert missing["status"] == "DATA_MISSING" and missing["mfe_pct"] is None
        assert all(label["status"] == "DATA_MISSING" for label in missing["thresholds"])
    # Later high/low and overlapping pre-alert candle never leak into metrics.
    partial = compute(bars + [candle(90, high=999., low=1.)], event=event(seconds=30))
    assert partial["path_samples"] == 59 and partial["initial_gap_seconds"] == 30
    assert partial["mfe_pct"] == 0 and partial["mae_pct"] == 0
    invalid = compute(bars, event={**event(), "membership_status": "BTC_DATA_MISSING"})
    assert invalid["status"] == "DATA_MISSING"
    gap_closed = compute(bars, wave={**wave(), "closing_boundary_verified": False})
    assert gap_closed["status"] == "DATA_MISSING"
    assert "BTC_PARENT_END_IS_NOT_A_VERIFIED_CAUSAL_REVERSAL" in gap_closed["missing_reasons"]


def check_selection_is_pre_outcome_and_independence():
    earliest = event(8, "HYPE", "SHORT", 1)
    later = event(2, "SOL", "SHORT", 2)
    tied = event(7, "BTC", "SHORT", 1)
    rows = report.select_representatives([later, earliest, tied, event(9, "BTC", "LONG", 5)])
    all_short = [row for row in rows if row["coin_scope"] == "ALL" and row["direction"] == "SHORT"]
    assert [row["event_id"] for row in all_short] == [7]
    assert any(row["coin_scope"] == "HYPE" and row["event_id"] == 8 for row in rows)
    assert len([row for row in rows if row["coin_scope"] == "ALL"]) == 2
    first = compute([candle(i, high=101., low=99.5) for i in range(60)])
    try:
        report.aggregate_records([first, first])
    except ValueError:
        pass
    else:
        raise AssertionError("Duplicate same-wave representatives counted as independent")
    missing = report._unavailable(event(wave_id="second"), wave("second"), START+timedelta(hours=2), report.SPOT_SCOPE, "missing")
    summary = report.aggregate_records([first, missing])
    assert len(summary) == 8 and summary[0]["success_probability"] is None
    assert summary[0]["asymmetry_ratio"] is None and summary[0]["missing_representatives"] == 1
    assert summary[0]["all_observed_status_counts"]["DATA_MISSING"] == 1


def check_fullwave_paging_beyond_provider_cap():
    calls = []
    def fetch(symbol, start, end):
        calls.append((start, end))
        count = int((end + report.MILLISECOND - start) // report.MINUTE)
        first_i = int((start - START) // report.MINUTE)
        return {**ROUTE, "candles": [candle(first_i+i) for i in range(count)]}
    cutoff = START + timedelta(minutes=3000) - report.MILLISECOND
    path = report.fetch_full_path("BTC", START, cutoff, source_scope=report.SPOT_SCOPE, fetcher=fetch)
    assert len(calls) == 3 and len(path["candles"]) == 3000
    assert calls[1][0] == calls[0][1] + report.MILLISECOND
    row = compute(path["candles"], wave=wave(minutes=3000), observed_at=cutoff + report.MILLISECOND)
    assert row["status"] == "READY" and row["path_samples"] == 3000


def check_real_hype_adapter_and_mixed_contract():
    hype = event(12, "HYPE", "SHORT", seconds=10)
    hype["engine_snapshot"] = {"price_source": "hyperliquid", "price_pair": "HYPEUSDT"}
    hype["current_price"] = 99.  # Original perp reference must not become MARK entry.
    route = {"symbol": "HYPE", "exchange": "binance", "market": "futures", "pair": "HYPEUSDT",
        "interval": "1m", "interval_seconds": 60, "price_kind": "MARK", "method_version": mark.METHOD_VERSION,
        "provenance": mark.PROVENANCE, "source_url": mark.SOURCE_URL}
    def fetch(symbol, start, end):
        assert symbol == "HYPE"
        return {**route, "candles": [candle(i, high=100.1, low=98.) for i in range(1, 60)]}
    result = report.build_report(waves=[wave()], events=[hype], observed_at=START+timedelta(hours=2),
                                 include_hype_mark=True, fetcher=fetch)
    canonical = [row for row in result["records"] if row["source_scope"] == report.SPOT_SCOPE]
    assert len(canonical) == 2 and all(row["status"] == "DATA_MISSING" for row in canonical)
    derived = [row for row in result["records"] if row["source_scope"] == report.MARK_SCOPE]
    assert len(derived) == 2 and all(row["status"] == "READY" for row in derived), derived
    row = derived[0]
    assert row["original_alert_reference_price"] == 99 and row["reference_price"] == 100
    assert row["measurement_start_utc"] == START+report.MINUTE
    assert row["mfe_pct"] == 2 and math.isclose(row["mae_pct"], .1)
    mixed = [row for row in result["summary_rows"] if row["source_scope"] == report.MIXED_SCOPE and row["coin_scope"] == "ALL"]
    assert len(mixed) == 8 and all(row["success_probability"] == 1 for row in mixed)
    assert all(math.isclose(row["asymmetry_ratio"], 20) for row in mixed)
    assert result["fixed_horizon_outcomes_modified"] is False


def main():
    check_full_wave_after_first_touch_and_active()
    check_complete_prefix_and_ambiguity()
    check_selection_is_pre_outcome_and_independence()
    check_fullwave_paging_beyond_provider_cap()
    check_real_hype_adapter_and_mixed_contract()
    print("PASS full-wave endpoint, active prefix, full excursions, all eight thresholds, causal selection, source separation and real HYPE derived entry")


if __name__ == "__main__":
    main()
