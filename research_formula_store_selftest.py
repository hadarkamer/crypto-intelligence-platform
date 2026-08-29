"""Deterministic checks for prospective Formula Shadow evidence handling."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json

import research_formula_store as store


def _shadow_row(
    event_id: int,
    *,
    at: datetime,
    symbol: str = "BTC",
    status: str = "MATCHED",
) -> dict:
    return {
        "event_id": event_id,
        "alert_time_utc": at,
        "symbol": symbol,
        "event_type": "SELFTEST_ALERT",
        "evaluation_status": status,
        "outcome_available": True,
        "directional_return_pct": 1.0,
        "mfe_pct": 2.0,
        "mae_pct": 0.25,
        "time_to_first_progress_seconds": 60,
        "time_to_mfe_seconds": 600,
        "target_progress_ratio": 1.0,
        "target_reached": True,
    }


def run() -> None:
    start = datetime(2026, 8, 29, 0, 0, tzinfo=timezone.utc)
    rows = [
        _shadow_row(1, at=start),
        # Same-symbol outcome windows overlap, so only the first match is an
        # independent prospective evidence unit.
        _shadow_row(2, at=start + timedelta(minutes=30)),
        # An interval beginning exactly at the prior horizon boundary does not
        # overlap the half-open outcome interval and must remain eligible.
        _shadow_row(3, at=start + timedelta(minutes=60)),
        # Simultaneous evidence for another symbol is independent.
        _shadow_row(4, at=start + timedelta(minutes=30), symbol="ETH"),
        # A same-symbol control overlapping a retained match is excluded.
        _shadow_row(
            5,
            at=start + timedelta(minutes=10),
            status="UNMATCHED",
        ),
        # This control starts exactly at the second retained match's boundary.
        _shadow_row(
            6,
            at=start + timedelta(minutes=120),
            status="UNMATCHED",
        ),
        # A later control overlapping the retained control is excluded.
        _shadow_row(
            7,
            at=start + timedelta(minutes=150),
            status="UNMATCHED",
        ),
        # Missing decision-time inputs are neither matches nor controls.
        _shadow_row(8, at=start, symbol="SOL", status="UNEVALUABLE"),
    ]
    selected = store._select_independent_shadow_rows(rows, horizon_minutes=60)
    assert [row["event_id"] for row in selected["matches"]] == [1, 4, 3]
    assert [row["event_id"] for row in selected["controls"]] == [6]
    assert selected["excluded_match_event_ids"] == [2]
    assert selected["excluded_control_event_ids"] == [5, 7]
    assert selected["exact_cohort_excluded_event_ids"] == []
    assert 8 not in {row["event_id"] for row in selected["rows"]}

    # Exact decision cohorts collapse before non-overlap selection. If an
    # unexpected mixed-status duplicate exists, the matched observation is the
    # conservative representative and the other event remains auditable.
    exact_duplicate = [
        {
            **_shadow_row(10, at=start, status="UNMATCHED"),
            "decision_cohort_key": "a" * 64,
            "decision_anchor_time_utc": start - timedelta(minutes=2),
        },
        {
            **_shadow_row(11, at=start + timedelta(minutes=1)),
            "decision_cohort_key": "a" * 64,
            "decision_anchor_time_utc": start - timedelta(minutes=2),
        },
    ]
    collapsed = store._select_independent_shadow_rows(
        exact_duplicate, horizon_minutes=60
    )
    assert [row["event_id"] for row in collapsed["matches"]] == [11]
    assert collapsed["controls"] == []
    assert collapsed["exact_cohort_excluded_event_ids"] == [10]

    # Session composition and weekend width calibration are frozen at the
    # decision timestamp. Realized outcomes populate labels only and never
    # alter either prior-only input.
    frozen_session = {
        "session_active_ratio": 0.25,
        "session_weekend_ratio": 0.75,
        "session_segments": [
            {"market_session": "WEEKEND", "minutes": 180},
            {"market_session": "ACTIVE", "minutes": 60},
        ],
        "session_composition": "MIXED",
    }
    frozen_width = {
        "floor_scale_factor": 0.60,
        "source": "prior raw-price session calibration",
        "samples": 240,
    }
    source = {
        **_shadow_row(20, at=start),
        "input_snapshot": json.dumps(
            {
                "outcome_window_session": frozen_session,
                "movement_width_reference": frozen_width,
            }
        ),
        "mfe_pct": 99.0,
        "mae_pct": 88.0,
    }
    metric = store._metric_row(source, horizon_minutes=240)
    label = metric["outcome_label"]
    assert label["session_active_ratio"] == 0.25
    assert label["session_weekend_ratio"] == 0.75
    assert label["session_segments"] == frozen_session["session_segments"]
    assert label["session_composition"] == "MIXED"
    assert label["movement_width_reference"] == frozen_width
    assert label["mfe_pct"] == 99.0 and label["mae_pct"] == 88.0

    changed_outcome = {**source, "mfe_pct": 0.01, "mae_pct": 500.0}
    changed_label = store._metric_row(
        changed_outcome, horizon_minutes=240
    )["outcome_label"]
    assert changed_label["movement_width_reference"] == frozen_width
    assert changed_label["session_active_ratio"] == 0.25

    missing_reference = {
        **source,
        "input_snapshot": {"outcome_window_session": frozen_session},
    }
    assert store._metric_row(
        missing_reference, horizon_minutes=240
    )["outcome_label"]["movement_width_reference"] == {}

    print("research formula store self-test: PASS")


if __name__ == "__main__":
    run()
