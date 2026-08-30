"""Deterministic checks for shared prior-only session-width calibration."""

from datetime import datetime, timedelta, timezone

import research_session_width as width


def _series(samples):
    ordered = sorted(samples, key=lambda item: item[0])
    return width.PriceWidthSeries(
        times=tuple(item[0] for item in ordered),
        abs_return_pcts=tuple(item[1] for item in ordered),
        active_ratios=tuple(item[2] for item in ordered),
    )


def run() -> None:
    event = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)  # Saturday
    cutoff = event - timedelta(microseconds=1)
    samples = []
    for index in range(15):
        monday = datetime(2026, 5, 4, 12, tzinfo=timezone.utc) + timedelta(
            weeks=index
        )
        saturday = datetime(2026, 5, 9, 12, tzinfo=timezone.utc) + timedelta(
            weeks=index
        )
        samples.extend(
            [
                (monday, 2.0, 1.0),
                (monday + timedelta(hours=2), 2.0, 1.0),
                (saturday, 0.4, 0.0),
                (saturday + timedelta(hours=2), 0.4, 0.0),
            ]
        )
    baseline = _series(samples)
    relaxed = width.movement_width_reference(
        symbol="BTC",
        event_time=event,
        horizon_minutes=60,
        as_of_utc=cutoff,
        historical_index={("BTC", 60): baseline},
    )
    assert relaxed["session_composition"] == "WEEKEND_ONLY"
    assert relaxed["session_weekend_ratio"] == 1.0
    assert relaxed["threshold_scale_factor"] == 0.5
    assert relaxed["applied"] is True
    coherent, _ = width.validate_movement_width_reference(
        relaxed,
        expected_symbol="BTC",
        event_time=event,
        horizon_minutes=60,
    )
    assert coherent is True

    strict_number_fields = (
        "composition_tolerance",
        "floor_scale_factor",
        "threshold_scale_factor",
        "session_active_ratio",
        "session_weekend_ratio",
        "session_matched_effective_samples",
        "active_reference_effective_samples",
        "session_matched_abs_return_p90_pct",
        "active_reference_abs_return_p90_pct",
    )
    for field in strict_number_fields:
        for forged_value in (
            str(relaxed[field]),
            True,
            float("nan"),
            float("inf"),
        ):
            forged = {**relaxed, field: forged_value}
            coherent, _ = width.validate_movement_width_reference(
                forged,
                expected_symbol="BTC",
                event_time=event,
                horizon_minutes=60,
            )
            assert coherent is False, (field, forged_value)

    strict_count_fields = (
        "horizon_minutes",
        "lookback_days",
        "minimum_effective_samples",
        "session_segments",
        "prior_points",
        "session_matched_samples",
        "active_reference_samples",
    )
    for field in strict_count_fields:
        for forged_value in (str(relaxed[field]), float(relaxed[field]), True):
            forged = {**relaxed, field: forged_value}
            coherent, _ = width.validate_movement_width_reference(
                forged,
                expected_symbol="BTC",
                event_time=event,
                horizon_minutes=60,
            )
            assert coherent is False, (field, forged_value)

    # A point exactly at the cutoff and a later point are both unavailable at
    # the strict prior-only boundary, regardless of their extreme values.
    with_future = _series(
        samples
        + [
            (cutoff, 9998.0, 0.0),
            (event + timedelta(minutes=1), 9999.0, 0.0),
        ]
    )
    unchanged = width.movement_width_reference(
        symbol="BTC",
        event_time=event,
        horizon_minutes=60,
        as_of_utc=cutoff,
        historical_index={("BTC", 60): with_future},
    )
    assert unchanged == relaxed

    # An ACTIVE-only outcome never receives a reduced width.
    active_event = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
    active = width.movement_width_reference(
        symbol="BTC",
        event_time=active_event,
        horizon_minutes=60,
        as_of_utc=active_event - timedelta(microseconds=1),
        historical_index={("BTC", 60): baseline},
    )
    assert active["session_composition"] == "ACTIVE_ONLY"
    assert active["threshold_scale_factor"] == 1.0
    assert active["applied"] is False
    assert "ACTIVE-only" in active["reason"]
    coherent, _ = width.validate_movement_width_reference(
        active,
        expected_symbol="BTC",
        event_time=active_event,
        horizon_minutes=60,
    )
    assert coherent is True
    forged_weekend = {
        **active,
        "session_active_ratio": 0.0,
        "session_weekend_ratio": 1.0,
        "session_composition": "WEEKEND_ONLY",
        "threshold_scale_factor": 0.5,
        "floor_scale_factor": 0.5,
        "applied": True,
    }
    coherent, reason = width.validate_movement_width_reference(
        forged_weekend,
        expected_symbol="BTC",
        event_time=active_event,
        horizon_minutes=60,
    )
    assert coherent is False and "New York calendar" in reason
    inconsistent_scales = {
        **relaxed,
        "floor_scale_factor": 0.9,
    }
    coherent, reason = width.validate_movement_width_reference(
        inconsistent_scales,
        expected_symbol="BTC",
        event_time=event,
        horizon_minutes=60,
    )
    assert coherent is False and "inconsistent" in reason

    # Too little prior evidence remains the unchanged static width.
    insufficient = _series(samples[:10])
    no_evidence = width.movement_width_reference(
        symbol="BTC",
        event_time=event,
        horizon_minutes=60,
        as_of_utc=cutoff,
        historical_index={("BTC", 60): insufficient},
    )
    assert no_evidence["threshold_scale_factor"] == 1.0
    assert no_evidence["applied"] is False
    assert no_evidence["reason"].startswith("insufficient")

    # New York's 2026 DST shift has already occurred here.  Sunday 18:00 ET
    # is 22:00 UTC, so 21:00-01:00 UTC contains one WEEKEND hour and three
    # ACTIVE hours; the shared policy must retain that exact 0.75/0.25 mix.
    dst_event = datetime(2026, 3, 8, 21, 0, tzinfo=timezone.utc)
    dst_samples = []
    for index in range(30):
        dst_samples.append(
            (
                datetime(2025, 10, 1, tzinfo=timezone.utc)
                + timedelta(days=index),
                0.4,
                0.75,
            )
        )
        dst_samples.append(
            (
                datetime(2025, 11, 15, tzinfo=timezone.utc)
                + timedelta(days=index),
                2.0,
                1.0,
            )
        )
    dst = width.movement_width_reference(
        symbol="BTC",
        event_time=dst_event,
        horizon_minutes=240,
        as_of_utc=dst_event - timedelta(microseconds=1),
        historical_index={("BTC", 240): _series(dst_samples)},
    )
    assert dst["session_composition"] == "MIXED"
    assert dst["session_active_ratio"] == 0.75
    assert dst["session_weekend_ratio"] == 0.25
    assert dst["threshold_scale_factor"] == 0.5

    try:
        width.movement_width_reference(
            symbol="BTC",
            event_time=event,
            horizon_minutes=60,
            as_of_utc=event + timedelta(seconds=1),
            historical_index={("BTC", 60): baseline},
        )
    except ValueError:
        pass
    else:  # pragma: no cover - explicit future-as-of rejection
        raise AssertionError("future calibration as_of was accepted")

    print("research session width self-test: PASS")


if __name__ == "__main__":
    run()
