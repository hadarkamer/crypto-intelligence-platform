"""Deterministic regressions for outcome-blind Market Episode evidence."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import random
import time

import research_market_episode as episodes


START = datetime(2026, 8, 1, tzinfo=timezone.utc)


def _row(
    event_id: int,
    minutes: int,
    *,
    symbol: str = "BTC",
    direction: str = "LONG",
    mfe: float = 2.0,
    mae: float = 0.5,
    success: bool = True,
    price: float = 100.0,
) -> dict:
    return {
        "event": {
            "event_id": event_id,
            "alert_time_utc": START + timedelta(minutes=minutes),
            "decision_anchor_time_utc": START + timedelta(minutes=minutes),
            "symbol": symbol,
            "direction": direction,
            "event_type": "MARKET_EPISODE_SELFTEST",
            "current_price": price,
        },
        "outcome_label": {
            "horizon_minutes": 1440,
            "path_success": success,
            "first_touch_status": "HIT" if success else "MISS",
            "mfe_pct": mfe,
            "mae_pct": mae,
            "session_active_ratio": 1.0,
            "session_weekend_ratio": 0.0,
            "movement_width_reference": {"floor_scale_factor": 1.0},
        },
    }


def _signature(grouped: list[dict]) -> list[tuple[str, tuple[int, ...]]]:
    return [
        (
            str(episode["episode_key"]),
            tuple(
                sorted(int(row["event"]["event_id"]) for row in episode["rows"])
            ),
        )
        for episode in grouped
    ]


def run() -> None:
    same_anchor = [
        _row(30, 0, symbol="BTC"),
        _row(10, 0, symbol="ETH"),
        _row(20, 0, symbol="SOL"),
        _row(5, 30, symbol="DOGE"),
    ]
    grouped = episodes.group_rows(same_anchor, horizon_minutes=60)
    assert len(grouped) == 1
    assert sorted(
        row["event"]["event_id"]
        for row in episodes.episode_evidence_rows(grouped[0])
    ) == [10, 20, 30]

    dense = [_row(index + 1, index * 30) for index in range(96)]
    dense_groups = episodes.group_rows(dense, horizon_minutes=1440)
    assert len(dense_groups) == 2
    assert dense_groups[0]["start_time_utc"] == START
    exact_gap = episodes.group_rows(
        [_row(1000, 0), _row(1001, 1440)], horizon_minutes=1440
    )
    assert len(exact_gap) == 2
    same_rise = episodes.group_rows(
        [_row(1002, 0, price=100.0), _row(1003, 1440, price=101.0)],
        horizon_minutes=1440,
    )
    assert len(same_rise) == 1
    assert [
        row["event"]["event_id"]
        for row in same_rise[0]["correlated_rows"]
    ] == [1003]
    reset_rise = episodes.group_rows(
        [_row(1004, 0, price=100.0), _row(1005, 1440, price=99.9)],
        horizon_minutes=1440,
    )
    assert len(reset_rise) == 2
    missing_price = _row(1008, 1440, symbol="ETH", price=99.0)
    missing_price["event"].pop("current_price")
    incomplete_reset = episodes.group_rows(
        [
            _row(1006, 0, symbol="BTC", price=100.0),
            _row(1007, 0, symbol="ETH", price=100.0),
            _row(1009, 1440, symbol="BTC", price=99.0),
            missing_price,
        ],
        horizon_minutes=1440,
    )
    assert len(incomplete_reset) == 1

    before_final = START + timedelta(hours=24) - timedelta(microseconds=1)
    at_final = START + timedelta(hours=24)
    single_episode = episodes.group_rows([_row(2000, 0)], horizon_minutes=1440)[
        0
    ]
    assert not episodes.is_finalized(
        single_episode, horizon_minutes=1440, as_of_utc=before_final
    )
    assert episodes.is_finalized(
        single_episode, horizon_minutes=1440, as_of_utc=at_final
    )

    # Once finalized, a later continuation of the same directional move is
    # diagnostic only. It cannot mutate the evidence window/key or retract the
    # sample. A later decision-time price reset may open a fresh fixed window.
    finalized_key = single_episode["episode_key"]
    finalized_end = single_episode["end_time_utc"]
    nonreset_later = episodes.group_rows(
        [_row(2000, 0, price=100.0), _row(2001, 48 * 60, price=101.0)],
        horizon_minutes=1440,
    )
    assert len(nonreset_later) == 1
    assert nonreset_later[0]["episode_key"] == finalized_key
    assert nonreset_later[0]["end_time_utc"] == finalized_end
    assert [
        row["event"]["event_id"] for row in nonreset_later[0]["rows"]
    ] == [2000]
    assert [
        row["event"]["event_id"]
        for row in nonreset_later[0]["correlated_rows"]
    ] == [2001]
    assert episodes.is_finalized(
        nonreset_later[0],
        horizon_minutes=1440,
        as_of_utc=START + timedelta(hours=48),
    )
    reset_after_continuation = episodes.group_rows(
        [
            _row(2000, 0, price=100.0),
            _row(2001, 48 * 60, price=101.0),
            _row(2002, 72 * 60, price=99.0),
        ],
        horizon_minutes=1440,
    )
    assert len(reset_after_continuation) == 2
    assert reset_after_continuation[0]["episode_key"] == finalized_key
    assert reset_after_continuation[1]["start_time_utc"] == START + timedelta(
        hours=72
    )

    shuffled = list(dense)
    random.Random(73).shuffle(shuffled)
    assert _signature(episodes.group_rows(shuffled, horizon_minutes=1440)) == (
        _signature(dense_groups)
    )
    mutated = []
    for row in dense:
        copy = {**row, "outcome_label": dict(row["outcome_label"])}
        copy["outcome_label"].update(
            {
                "path_success": not bool(row["outcome_label"]["path_success"]),
                "first_touch_status": (
                    "MISS" if row["outcome_label"]["path_success"] else "HIT"
                ),
                "mfe_pct": 999.0,
                "mae_pct": 999.0,
            }
        )
        mutated.append(copy)
    assert _signature(episodes.group_rows(mutated, horizon_minutes=1440)) == (
        _signature(dense_groups)
    )

    # Dense formula nonmatches accumulate as fixed control cohorts instead of
    # extending one episode forever. Every raw match interval, including a
    # post-window non-reset continuation, still blocks overlapping controls.
    dense_controls = [_row(20_000 + index, index * 30) for index in range(14 * 48)]
    control_selection = episodes.select_independent(
        [], dense_controls, horizon_minutes=1440
    )
    assert len(control_selection["control_episodes"]) == 14
    finalized_controls, open_controls = episodes.partition_finalized(
        control_selection["control_episodes"],
        horizon_minutes=1440,
        as_of_utc=START + timedelta(days=14),
    )
    assert len(finalized_controls) == 14
    assert not open_controls
    shuffled_controls = list(dense_controls)
    random.Random(91).shuffle(shuffled_controls)
    assert _signature(
        episodes.select_independent(
            [], shuffled_controls, horizon_minutes=1440, presorted=True
        )["control_episodes"]
    ) == _signature(control_selection["control_episodes"])
    mutated_controls = []
    for row in dense_controls:
        copy = {**row, "outcome_label": dict(row["outcome_label"])}
        copy["outcome_label"].update(
            {
                "path_success": not bool(row["outcome_label"]["path_success"]),
                "first_touch_status": "MISS",
                "mfe_pct": -999.0,
                "mae_pct": 999.0,
            }
        )
        mutated_controls.append(copy)
    assert _signature(
        episodes.select_independent(
            [], mutated_controls, horizon_minutes=1440
        )["control_episodes"]
    ) == _signature(control_selection["control_episodes"])
    continued_match_selection = episodes.select_independent(
        [_row(21_000, 0, price=100.0), _row(21_001, 48 * 60, price=101.0)],
        [_row(21_010, 48 * 60), _row(21_011, 72 * 60)],
        horizon_minutes=1440,
    )
    assert [
        row["event"]["event_id"]
        for row in continued_match_selection["matches"]
    ] == [21_000]
    assert 21_001 in continued_match_selection["excluded_match_event_ids"]
    assert 21_010 in continued_match_selection["excluded_control_event_ids"]
    assert [
        row["event"]["event_id"]
        for row in continued_match_selection["controls"]
    ] == [21_011]

    # Same-anchor paired outcomes are aggregated as pairs. Independent medians
    # would incorrectly call this favorable: median MFE=99, median MAE=2, while
    # only one of three member pairs is favorable.
    counterexample = [
        _row(300, 0, mfe=1.0, mae=2.0),
        _row(200, 0, mfe=100.0, mae=101.0),
        _row(100, 0, mfe=99.0, mae=0.0),
        _row(1, 60, mfe=500.0, mae=0.0),
    ]
    aggregate = episodes.aggregate_metric_episode(
        counterexample,
        episode_key="e" * 64,
        episode_start_utc=START,
    )
    assert aggregate["outcome_label"]["favorable_dominance"] is False
    assert aggregate["outcome_label"]["paired_favorable_minus_adverse_pct"] == -1.0
    assert aggregate["outcome_label"]["adverse_tail_mae_pct"] == 101.0
    assert aggregate["event"]["event_id"] == 100
    assert aggregate["event"]["market_episode_evidence_event_ids"] == [100, 200, 300]
    assert aggregate["event"]["market_episode_member_event_ids"] == [1, 100, 200, 300]

    selected = episodes.select_independent(
        [_row(1, 0), _row(2, 30), _row(3, 1470)],
        [_row(10, 60), _row(11, 3000)],
        horizon_minutes=60,
    )
    assert [row["event"]["event_id"] for row in selected["matches"]] == [1, 3]
    assert [row["event"]["event_id"] for row in selected["controls"]] == [11]
    assert selected["excluded_match_event_ids"] == [2]
    assert 10 in selected["excluded_control_event_ids"]
    assert "formula-local" in selected["identity_scope"]
    permuted_selected = episodes.select_independent(
        [_row(3, 1470), _row(2, 30), _row(1, 0)],
        [_row(11, 3000), _row(10, 60)],
        horizon_minutes=60,
        presorted=True,
    )
    assert [row["event"]["event_id"] for row in permuted_selected["matches"]] == [1, 3]
    assert [row["event"]["event_id"] for row in permuted_selected["controls"]] == [11]

    try:
        episodes.group_rows(
            [_row(1, 0, direction="LONG"), _row(2, 30, direction="SHORT")],
            horizon_minutes=60,
        )
    except ValueError as exc:
        assert "partition" in str(exc)
    else:
        raise AssertionError("mixed formula directions were accepted")
    try:
        episodes.select_independent(
            [_row(1, 0, direction="LONG")],
            [_row(2, 1440, direction="SHORT")],
            horizon_minutes=60,
        )
    except ValueError as exc:
        assert "direction" in str(exc)
    else:
        raise AssertionError("mixed directions across matches/controls were accepted")

    forecast_shifted = [_row(70, 0), _row(71, 1440)]
    forecast_shifted[0]["event"]["forecast_start_time_utc"] = START
    forecast_shifted[1]["event"]["forecast_start_time_utc"] = START + timedelta(
        hours=23
    )
    assert len(episodes.group_rows(forecast_shifted, horizon_minutes=60)) == 1

    pending = _row(72, 0)
    pending["outcome_label"]["path_success"] = None
    pending["outcome_label"]["first_touch_status"] = "PENDING"
    try:
        episodes.aggregate_metric_episode(
            [_row(73, 0), pending],
            episode_key="p" * 64,
            episode_start_utc=START,
        )
    except ValueError as exc:
        assert "terminal" in str(exc)
    else:
        raise AssertionError("partial earliest forecast cohort was aggregated")
    retry_boundary = _row(97, 24 * 60)
    retry_boundary["event"]["decision_anchor_time_utc"] = START + timedelta(
        hours=24
    )
    delayed_first = _row(96, 30)
    delayed_first["event"]["decision_anchor_time_utc"] = START
    assert len(
        episodes.group_rows([delayed_first, retry_boundary], horizon_minutes=60)
    ) == 1
    non_overlapping = _row(95, 24 * 60 + 30)
    non_overlapping["event"]["decision_anchor_time_utc"] = START + timedelta(
        hours=24
    )
    assert len(
        episodes.group_rows([delayed_first, non_overlapping], horizon_minutes=60)
    ) == 2
    secondary_malformed = _row(98, 0)
    secondary_malformed["event"]["alert_time_utc"] = "not-a-timestamp"
    try:
        episodes.group_rows([secondary_malformed], horizon_minutes=60)
    except ValueError:
        pass
    else:
        raise AssertionError("malformed forecast-start timestamp was accepted")
    malformed = _row(99, 0)
    malformed["event"]["alert_time_utc"] = "not-a-timestamp"
    malformed["event"]["decision_anchor_time_utc"] = "not-a-timestamp"
    try:
        episodes.group_rows([malformed], horizon_minutes=60)
    except ValueError:
        pass
    else:
        raise AssertionError("malformed timestamp was accepted")

    symbols = ("BTC", "ETH", "SOL", "BNB", "DOGE", "XRP", "ZEC", "HYPE")
    dense_market = [
        _row(slot * len(symbols) + offset + 1, slot * 30, symbol=symbol)
        for slot in range(96)
        for offset, symbol in enumerate(symbols)
    ]
    started = time.perf_counter()
    dense_result = episodes.select_independent(
        dense_market, [], horizon_minutes=1440
    )
    elapsed = time.perf_counter() - started
    assert len(dense_result["match_episodes"]) == 2
    assert len(dense_result["matches"]) == 16
    assert elapsed < 5.0, elapsed

    print("research Market Episode self-test: PASS")


if __name__ == "__main__":
    run()
