"""Source-explicit pure next-minute futures measurements; no I/O or source routing."""
from datetime import timedelta
import hashlib

from research_btc_parent_movement import validate_candle
from research_ordered_first_touch import METHOD_VERSION, calculate_all_ordered_first_touch_outcomes, _excursion_metrics
from research_telegram_archive_backfill import canonical
from research_telegram_html_archive import _hash

WINDOWS = (60, 240, 720, 1440)


def calculate(*, derived_id, direction, entry, bars, observed_at, policy):
    if entry > observed_at:
        return {"calculation_status": "NOT_YET_ENTRY"}, [], []
    if entry not in bars:
        return {"calculation_status": "DATA_MISSING_ENTRY_CANDLE"}, [], []
    reference = validate_candle(bars[entry])["open"]
    source, labels, metrics = policy["source"], [], []
    event_id = _hash(derived_id, "NORMAL")
    for horizon in WINDOWS:
        end = entry + timedelta(minutes=horizon)
        cutoff = min(end, observed_at.replace(second=0, microsecond=0))
        expected = [entry + timedelta(minutes=i) for i in range(max(0, int((cutoff-entry).total_seconds() // 60)))]
        path = [validate_candle(bars[opened]) for opened in expected if opened in bars]
        prefix = [bar["open_time_utc"] for bar in path] == expected
        complete = bool(prefix and observed_at >= end)
        path_sha = hashlib.sha256(canonical({"source": source, "candles": path}).encode()).hexdigest()
        full = {"method_version": policy["metric_version"], "measurement_kind": "FIXED_WINDOW",
            "event_id": event_id, "symbol": "HYPE", "direction": direction, "signal_variant": "NORMAL",
            "window_minutes": horizon, "entry_policy_version": policy["entry_version"],
            "source_scope": policy["scope"], "reference_price": reference,
            "measurement_start_utc": entry, "window_end_utc": end, "observed_at_utc": observed_at,
            "observed_from_utc": path[0]["open_time_utc"] if path else None,
            "observed_through_utc": path[-1]["close_time_utc"] if path else None,
            "status": "READY" if complete else "OPEN" if prefix else "DATA_MISSING",
            "observation_closed": observed_at >= end, "path_complete": complete,
            "observed_prefix_complete": prefix, "expected_candles": len(expected),
            "path_samples": len(path), "initial_gap_seconds": 0, "trailing_partial_minute_seconds": 0,
            "candle_interval_seconds": 60, "source": dict(source),
            "data_quality_status": policy["verified_quality"] if complete else policy["partial_quality"],
            "path_sha256": path_sha, "boundary_policy": policy["boundary_policy"],
            "mfe_pct": None, "mae_pct": None, "asymmetry_ratio": None,
            "asymmetry_method": "sum_mfe_pct_over_sum_mae_pct_same_full_window_v1",
            "asymmetry_status": "WINDOW_NOT_READY"}
        if complete:
            full.update(_excursion_metrics(reference_price=reference, direction=direction,
                maximum=max([reference]+[bar["high"] for bar in path]),
                minimum=min([reference]+[bar["low"] for bar in path])))
            full["asymmetry_ratio"] = full["mfe_pct"] / full["mae_pct"] if full["mae_pct"] > 0 else None
            full["asymmetry_status"] = "DEFINED" if full["mae_pct"] > 0 else "UNDEFINED_ZERO_MAE"
        metrics.append(full)
        for label in calculate_all_ordered_first_touch_outcomes(reference_price=reference, direction=direction,
                event_time=entry, candles=path, observation_closed=observed_at >= end, path_complete=prefix):
            label.update({"event_id": event_id, "outcome_id": _hash(event_id, METHOD_VERSION, horizon, label["threshold_bps"]),
                "window_minutes": horizon, "outcome_method_version": METHOD_VERSION, "signal_variant": "NORMAL",
                "source_scope": policy["scope"], "record_mode": "DERIVED", "entry_policy_version": policy["entry_version"],
                "data_quality_status": policy["verified_quality"] if prefix else policy["partial_quality"],
                "source_price_exchange": source["exchange"], "source_price_market": source["market"],
                "source_price_pair": source["pair"], "source_price_kind": source["price_kind"],
                "price_source_contract": dict(source), "path_sha256": path_sha})
            labels.append(label)
    return {"entry_price": reference, "calculation_status": "COMPLETE_32_LABELS"
        if all(metric["status"] == "READY" for metric in metrics) else "RETRY_INCOMPLETE_PATH"}, labels, metrics
