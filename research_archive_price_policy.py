"""Explicit mixed-price policy for descriptive archive research only.

Native Spot evidence remains unchanged. MARK labels keep their real provenance;
they are never relabelled as Spot or counted as extra independent market waves.
"""
from collections import Counter
from datetime import timedelta
import math

from research_common_window_metrics import aggregate_common_window_metrics, ASYMMETRY_METHOD
from research_telegram_archive_features import ENTRY_VERSION as SPOT_ENTRY, utc
import research_telegram_archive_hype_supplement as hype

POLICY_VERSION = "archive-spot-plus-hype-mark-v1"
SPOT_SOURCE = "BINANCE_SPOT_1M"
MARK_SOURCE = "BINANCE_FUTURES_HYPE_MARK_1M"


def archive_outcome_price_verified(row):
    label = row.get("ordered_outcome") or {}
    if (row.get("source_scope") != "ARCHIVE_ONLY" or row.get("record_mode") != "ARCHIVE"
            or row.get("live_union_eligible") is not False
            or label.get("source_scope") != "ARCHIVE_ONLY" or label.get("record_mode") != "ARCHIVE"):
        return False
    entry = row.get("entry_policy_version")
    if label.get("entry_policy_version") != entry:
        return False
    if entry == SPOT_ENTRY:
        return (row.get("symbol") != "HYPE" and label.get("source_price_exchange") == "binance"
                and label.get("source_price_market") == "spot"
                and label.get("data_quality_status") == "VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES")
    return (entry == hype.ENTRY_VERSION and row.get("symbol") == "HYPE"
            and row.get("price_source_contract") == hype.SOURCE
            and label.get("price_source_contract") == hype.SOURCE
            and label.get("source_price_exchange") == "binance"
            and label.get("source_price_market") == "futures"
            and label.get("source_price_pair") == "HYPEUSDT"
            and label.get("source_price_kind") == "MARK"
            and label.get("data_quality_status") == hype.VERIFIED_QUALITY)


def _metric(row, window):
    """Validate before any cross-source simultaneous-cohort reduction."""
    if not isinstance(row, dict):
        return None
    if row.get("entry_policy_version") == SPOT_ENTRY:
        aggregate = aggregate_common_window_metrics([row], expected_count=1,
            window_minutes=window, threshold_bps=25)
        if aggregate["coverage_complete"]:
            return float(row["mfe_pct"]), float(row["mae_pct"]), SPOT_SOURCE
        return None
    try:
        start, end = utc(row["measurement_start_utc"]), utc(row["window_end_utc"])
        values = row["mfe_pct"], row["mae_pct"]
        if any(isinstance(v, bool) or not isinstance(v, (float, int)) or not math.isfinite(v) or v < 0 for v in values):
            return None
        valid = (row.get("entry_policy_version") == hype.ENTRY_VERSION
            and row.get("source_scope") == "ARCHIVE_ONLY" and row.get("symbol") == "HYPE"
            and row.get("method_version") == hype.METRIC_VERSION
            and row.get("source") == hype.SOURCE and row.get("data_quality_status") == hype.VERIFIED_QUALITY
            and row.get("measurement_kind") == "FIXED_WINDOW" and row.get("window_minutes") == window
            and row.get("status") == "READY" and row.get("observation_closed") is True
            and row.get("path_complete") is True and end == start + timedelta(minutes=window)
            and utc(row["observed_at_utc"]) >= end and row.get("path_samples") == window
            and row.get("expected_candles") == window and row.get("candle_interval_seconds") == 60)
        return (*values, MARK_SOURCE) if valid else None
    except (KeyError, TypeError, ValueError, OverflowError):
        return None


def aggregate_archive_wave_metrics(cohorts, *, window_minutes, threshold_bps):
    """Each supplied list is all simultaneous earliest members of one wave."""
    if window_minutes not in hype.WINDOWS or threshold_bps not in range(25, 201, 25):
        raise ValueError("Unsupported archive metric cell")
    valid, sources = [], Counter()
    for members in cohorts:
        measured = [_metric(row, window_minutes) for row in members]
        if not measured or any(item is None for item in measured):
            continue
        valid.append((min(item[0] for item in measured), max(item[1] for item in measured)))
        sources.update(item[2] for item in measured)
    expected = len(cohorts)
    complete = expected > 0 and len(valid) == expected
    total_mfe, total_mae = sum(v[0] for v in valid), sum(v[1] for v in valid)
    return {"method_version": POLICY_VERSION, "asymmetry_method": ASYMMETRY_METHOD,
        "window_minutes": window_minutes, "threshold_bps": threshold_bps,
        "expected_representatives": expected, "valid_representatives": len(valid),
        "coverage_complete": complete, "mfe_pct": total_mfe / expected if complete else None,
        "mae_pct": total_mae / expected if complete else None,
        "asymmetry_ratio": total_mfe / total_mae if complete and total_mae > 0 else None,
        "asymmetry_status": "INCOMPLETE_COHORT" if not complete else "UNDEFINED_ZERO_MAE" if total_mae == 0 else "DEFINED",
        "source_member_counts": dict(sources), "source_scope": "ARCHIVE_ONLY"}
