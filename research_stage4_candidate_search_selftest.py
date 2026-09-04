"""Deterministic checks for the pure Stage-4 experimental candidate search."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
from pathlib import Path

import research_signal_formula_exploration as exploration
import research_stage4_candidate_search as search


UTC = timezone.utc
BASE = datetime(2026, 9, 1, 12, 3, tzinfo=UTC)
HORIZON = 60
AS_OF = BASE + timedelta(days=3)


def _h(value: object) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def _cycle(value: datetime) -> datetime:
    minute = 30 if value.minute >= 30 else 0
    return value.replace(minute=minute, second=0, microsecond=0)


def _features(
    *,
    max_pain: bool = True,
    max_pain_strong: bool = False,
    magnet: bool = False,
    magnet_strong: bool = False,
    combined_sources: tuple[str, ...] = (),
) -> dict:
    sources = set(combined_sources)
    combined = bool(sources)
    return {
        exploration.FEATURE_MAX_PAIN_CONFIRMED: max_pain,
        exploration.FEATURE_MAX_PAIN_STRONG: max_pain_strong,
        exploration.FEATURE_MAGNET_CONFIRMED: magnet,
        exploration.FEATURE_MAGNET_STRONG: magnet_strong,
        exploration.FEATURE_COMBINED_CONFIRMED: combined,
        exploration.FEATURE_COMBINED_COINGLASS: "COINGLASS_MAX_PAIN" in sources,
        exploration.FEATURE_COMBINED_PRICE_OI: "PRICE_OI" in sources,
        exploration.FEATURE_COMBINED_FUTURES_CVD: "FUTURES_CVD" in sources,
        exploration.FEATURE_COMBINED_VOTE_COUNT: len(sources) if sources else None,
    }


def _price_source(
    *, decision: datetime, symbol: str, snapshot_set_id: int, snapshot_key: str
) -> str:
    observed = decision - timedelta(seconds=10)
    fetched = decision - timedelta(seconds=5)
    pair = f"{symbol}USDT"
    return (
        "reference=reference_policy="
        + exploration.STAGE4_OUTCOME_REFERENCE_POLICY_VERSION
        + "|admission_policy="
        + exploration.STAGE4_OUTCOME_ADMISSION_POLICY_VERSION
        + "|semantics="
        + exploration.STAGE4_OUTCOME_SEMANTICS
        + "|source=binance_spot|exchange=binance|market=spot|pair="
        + pair
        + "|instrument="
        + pair
        + "|observed_at_utc="
        + observed.isoformat()
        + "|fetched_at_utc="
        + fetched.isoformat()
        + "|observed_age_seconds=10|fetched_age_seconds=5|snapshot_set_id="
        + str(snapshot_set_id)
        + "|snapshot_key="
        + snapshot_key
        + "|path=binance_spot:"
        + pair.lower()
        + ":1m|provenance=EXCHANGE_API_HISTORICAL_CANDLES_IMPORTED"
    )


def _observation(
    index: int,
    *,
    parent: str,
    decision: datetime,
    mfe: float,
    mae: float,
    feature_values: dict | None = None,
    outcome_available: bool = True,
    symbol: str = "ETH",
    direction: str = "LONG",
) -> exploration.ExplorationObservation:
    feature_values = dict(feature_values or _features())
    snapshot_set_id = 1000 + index
    snapshot_key = _h(f"snapshot-{index}")
    cycle = _cycle(decision)
    source_event_ids = [100_000 + index]
    source_families: set[str] = set()
    if (
        feature_values[exploration.FEATURE_MAX_PAIN_CONFIRMED]
        or feature_values[exploration.FEATURE_MAGNET_CONFIRMED]
    ):
        source_families.add("COINGLASS_MAX_PAIN")
    for feature, family in (
        (exploration.FEATURE_COMBINED_COINGLASS, "COINGLASS_MAX_PAIN"),
        (exploration.FEATURE_COMBINED_PRICE_OI, "PRICE_OI"),
        (exploration.FEATURE_COMBINED_FUTURES_CVD, "FUTURES_CVD"),
    ):
        if feature_values[feature]:
            source_families.add(family)

    if outcome_available:
        reference = 100.0
        directional_return = min(max(mfe / 2.0, 0.01), 0.10)
        raw_return = directional_return if direction == "LONG" else -directional_return
        if direction == "LONG":
            favorable_price = reference * (1.0 + mfe / 100.0)
            adverse_price = reference * (1.0 - mae / 100.0)
        else:
            favorable_price = reference * (1.0 - mfe / 100.0)
            adverse_price = reference * (1.0 + mae / 100.0)
        measured = decision + timedelta(minutes=HORIZON, milliseconds=-1)
        outcome = {
            "status": "AVAILABLE",
            "reason": None,
            "policy_version": exploration.OUTCOME_BINDING_POLICY_VERSION,
            "horizon_minutes": HORIZON,
            "carrier_type": "STAGE4_SIGNAL_EVENTS",
            "carrier_payload_sha256": None,
            "source_event_ids": source_event_ids,
            "path": {
                "reference_price": reference,
                "price_at_horizon": reference * (1.0 + raw_return / 100.0),
                "raw_return_pct": raw_return,
                "directional_return_pct": directional_return,
                "max_favorable_price": favorable_price,
                "max_adverse_price": adverse_price,
                "mfe_pct": mfe,
                "mae_pct": mae,
                "time_to_first_progress_seconds": 60,
                "time_to_mfe_seconds": 60,
                "path_resolution_seconds": 60,
                "path_samples": exploration._expected_path_samples(
                    decision, HORIZON
                ),
                "outcome_method_version": exploration.STAGE4_OUTCOME_METHOD_VERSION,
                "data_quality_status": "VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES",
                "price_source": _price_source(
                    decision=decision,
                    symbol=symbol,
                    snapshot_set_id=snapshot_set_id,
                    snapshot_key=snapshot_key,
                ),
            },
            "measured_at_utc": measured.isoformat(),
            "label_fields_exposed_as_features": False,
        }
    else:
        outcome = {
            "status": "OUTCOME_UNAVAILABLE",
            "reason": "SELFTEST_OUTCOME_MISSING",
            "policy_version": exploration.OUTCOME_BINDING_POLICY_VERSION,
            "horizon_minutes": HORIZON,
            "label_fields_exposed_as_features": False,
        }

    body = {
        "policy_version": exploration.POLICY_VERSION,
        "feature_schema_version": exploration.FEATURE_SCHEMA_VERSION,
        "projection_event_id": 10_000 + index,
        "projection_event_fingerprint": _h(f"projection-{index}"),
        "snapshot_set_id": snapshot_set_id,
        "snapshot_key": snapshot_key,
        "projection_decision_time_utc": decision.isoformat(),
        "archive_cycle_time_utc": cycle.isoformat(),
        "cohort_evaluable_symbols": [symbol],
        "cohort_expected_observation_count": 2,
        "projection_signal_event_count": 1,
        "projection_signal_events_payload_sha256": _h(f"signals-{index}"),
        "symbol": symbol,
        "direction": direction,
        "symbol_evaluation_status": "EVALUABLE",
        "symbol_evaluation_reason": None,
        "features": feature_values,
        "source_families": sorted(source_families),
        "source_event_ids": source_event_ids,
        "source_event_fingerprints": [_h(f"signal-event-{index}")],
        "explicit_no_signal": False,
        "absence_basis": "COMPLETED_PROJECTION_EVALUABLE_SYMBOL",
        "wave_binding": {
            "status": "BOUND",
            "reason": None,
            "policy_version": exploration.WAVE_BINDING_POLICY_VERSION,
            "expected_eligible_at_utc": (cycle + timedelta(minutes=2)).isoformat(),
            "symbol_membership_receipt_sha256": _h(f"sm-{index}"),
            "symbol_transition_receipt_sha256": _h(f"st-{index}"),
            "symbol_stream_id": _h(f"ss-{symbol}"),
            "symbol_movement_id": _h(f"symbol-movement-{parent}-{symbol}"),
            "btc_parent_membership_receipt_sha256": _h(f"bm-{index}"),
            "btc_parent_transition_receipt_sha256": _h(f"bt-{index}"),
            "btc_parent_stream_id": _h("btc-parent-stream"),
            "btc_parent_movement_id": _h(parent),
            "role": "INDEPENDENCE_ONLY_NOT_FORMULA_PREDICATE",
        },
        "outcome": outcome,
        "authority_effect": "NONE",
        "formula_registry_effect": "NONE",
        "delivery_channel": "NONE",
        "live_eligible": False,
        "telegram_delivery_allowed": False,
        "trade_execution_allowed": False,
    }
    return exploration.ExplorationObservation._from_payload(body)


def _candidate_for_feature(result: dict, feature: str) -> dict:
    for candidate in result["candidates"]:
        if candidate["conditions"] == [
            {"feature": feature, "operator": "==", "value": True}
        ]:
            return candidate
    raise AssertionError(f"candidate not returned for {feature}")


def _check_probability_gate_and_no_time_spacing() -> None:
    rows = [
        _observation(
            index,
            parent=f"probability-parent-{index}",
            decision=BASE + timedelta(minutes=index * 4),
            mfe=0.60,
            mae=0.50,
        )
        for index in range(5)
    ]
    result = search.search_experimental_candidates(
        rows,
        horizon_minutes=HORIZON,
        analysis_as_of_utc=AS_OF,
    )
    candidate = _candidate_for_feature(
        result, exploration.FEATURE_MAX_PAIN_CONFIRMED
    )
    assert candidate["occurrence_counts"]["completed"] == 5
    assert candidate["raw_match_count"] == 5
    assert candidate["experimental_formula_eligible"] is True
    assert candidate["accepted_paths"] == ["PROBABILITY"]
    assert candidate["eligibility_gate"]["atomic"] is True
    assert candidate["eligibility_gate"]["separate_later_probability_gate"] is False
    assert candidate["metrics"]["hit_rate_pct"] == 100.0
    assert candidate["metrics"]["median_mfe_mae_ratio"] == 1.2
    assert candidate["multiple_testing"]["decision_effect"] == (
        "DISCLOSURE_ONLY_EXPERIMENTAL"
    )
    assert result["formula_registry_effect"] == "NONE"
    assert result["telegram_delivery_allowed"] is False
    assert result["trade_execution_allowed"] is False
    assert all(
        condition["feature"] in exploration.ALLOWED_FEATURES
        and "outcome" not in condition["feature"]
        and "wave" not in condition["feature"]
        for item in result["candidates"]
        for condition in item["conditions"]
    )


def _check_one_wave_is_one_occurrence() -> None:
    rows = [
        _observation(
            20,
            parent="one-parent-only",
            decision=BASE,
            mfe=2.0,
            mae=0.1,
            symbol="ETH",
        ),
        _observation(
            21,
            parent="one-parent-only",
            decision=BASE,
            mfe=0.1,
            mae=0.2,
            symbol="SOL",
        ),
        *[
            _observation(
                22 + index,
                parent="one-parent-only",
                decision=BASE + timedelta(minutes=6 + index * 6),
                mfe=2.0,
                mae=0.1,
            )
            for index in range(6)
        ],
    ]
    result = search.search_experimental_candidates(
        rows,
        horizon_minutes=HORIZON,
        analysis_as_of_utc=AS_OF,
    )
    candidate = _candidate_for_feature(
        result, exploration.FEATURE_MAX_PAIN_CONFIRMED
    )
    assert candidate["raw_match_count"] == 8
    assert candidate["occurrence_counts"]["completed"] == 1
    assert candidate["experimental_formula_eligible"] is False
    occurrence = candidate["completed_occurrences"][0]
    assert occurrence["evidence_symbols"] == ["ETH", "SOL"]
    assert occurrence["favorable_move_member_hits"] == 1
    assert occurrence["favorable_move_member_count"] == 2
    assert occurrence["favorable_move_hit"] is False
    assert (
        "minimum independent BTC parent occurrences"
        in candidate["metrics"]["missing_by_route"]["COMMON"]
    )


def _check_first_match_is_frozen_before_outcome() -> None:
    rows = [
        _observation(
            40,
            parent="parent-with-missing-first",
            decision=BASE,
            mfe=1.0,
            mae=0.1,
            outcome_available=False,
        ),
        _observation(
            41,
            parent="parent-with-missing-first",
            decision=BASE + timedelta(minutes=30),
            mfe=4.0,
            mae=0.1,
            outcome_available=True,
        ),
        *[
            _observation(
                42 + index,
                parent=f"complete-parent-{index}",
                decision=BASE + timedelta(minutes=2 + index),
                mfe=1.0,
                mae=0.1,
            )
            for index in range(4)
        ],
    ]
    result = search.search_experimental_candidates(
        rows,
        horizon_minutes=HORIZON,
        analysis_as_of_utc=AS_OF,
    )
    candidate = _candidate_for_feature(
        result, exploration.FEATURE_MAX_PAIN_CONFIRMED
    )
    assert candidate["occurrence_counts"]["completed"] == 4
    assert candidate["occurrence_counts"]["mature_outcome_unavailable"] == 1
    assert candidate["experimental_formula_eligible"] is False
    unavailable = candidate["unavailable_occurrences"][0]
    assert unavailable["first_match_time_utc"] == BASE.isoformat()
    assert unavailable["evidence_observation_ids"] == [
        rows[0].observation_id
    ]


def _check_asymmetry_route_is_independent_of_probability_route() -> None:
    rows = [
        _observation(
            60 + index,
            parent=f"asymmetry-parent-{index}",
            decision=BASE + timedelta(minutes=index * 3),
            mfe=0.80 if index < 3 else 0.40,
            mae=0.10,
        )
        for index in range(5)
    ]
    result = search.search_experimental_candidates(
        rows,
        horizon_minutes=HORIZON,
        analysis_as_of_utc=AS_OF,
    )
    candidate = _candidate_for_feature(
        result, exploration.FEATURE_MAX_PAIN_CONFIRMED
    )
    assert candidate["metrics"]["successes"] == 3
    assert candidate["metrics"]["routes"]["PROBABILITY"]["passed"] is False
    assert candidate["metrics"]["routes"]["ASYMMETRY"]["passed"] is True
    assert candidate["accepted_paths"] == ["ASYMMETRY"]
    assert candidate["experimental_formula_eligible"] is True


def _check_combined_dependencies_and_disclosure_only_q() -> None:
    winning = [
        _observation(
            80 + index,
            parent=f"winning-parent-{index}",
            decision=BASE + timedelta(minutes=index * 3),
            mfe=0.60,
            mae=0.50,
        )
        for index in range(5)
    ]
    rich_features = _features(
        max_pain=False,
        magnet=True,
        magnet_strong=True,
        combined_sources=("COINGLASS_MAX_PAIN", "FUTURES_CVD", "PRICE_OI"),
    )
    losing = [
        _observation(
            90 + index,
            parent=f"losing-parent-{index}",
            decision=BASE + timedelta(minutes=30 + index * 3),
            mfe=0.10,
            mae=0.50,
            feature_values=rich_features,
        )
        for index in range(5)
    ]
    result = search.search_experimental_candidates(
        [*winning, *losing],
        horizon_minutes=HORIZON,
        analysis_as_of_utc=AS_OF,
    )
    max_pain = _candidate_for_feature(
        result, exploration.FEATURE_MAX_PAIN_CONFIRMED
    )
    assert max_pain["experimental_formula_eligible"] is True
    assert max_pain["multiple_testing"]["probability_q_value"] > 0.20
    assert max_pain["multiple_testing"]["eligibility_changed"] is False
    assert result["counts"]["hypotheses_disclosed"] == (
        result["counts"]["candidates_evaluated"] * 2
    )
    assert result["counts"]["display_equivalent_candidates_collapsed"] > 0
    for candidate in result["candidates"]:
        names = {condition["feature"] for condition in candidate["conditions"]}
        if exploration.FEATURE_COMBINED_CONFIRMED in names:
            assert not names.intersection(
                {
                    exploration.FEATURE_COMBINED_COINGLASS,
                    exploration.FEATURE_COMBINED_PRICE_OI,
                    exploration.FEATURE_COMBINED_FUTURES_CVD,
                    exploration.FEATURE_COMBINED_VOTE_COUNT,
                }
            )


def _check_bounds_and_pure_boundary() -> None:
    row = _observation(
        120,
        parent="bounded-parent",
        decision=BASE,
        mfe=1.0,
        mae=0.1,
    )
    try:
        search.search_experimental_candidates(
            [row, row],
            horizon_minutes=HORIZON,
            analysis_as_of_utc=AS_OF,
        )
    except ValueError as exc:
        assert "duplicate" in str(exc)
    else:
        raise AssertionError("duplicate observations did not fail closed")

    second = _observation(
        121,
        parent="second-parent",
        decision=BASE + timedelta(minutes=1),
        mfe=1.0,
        mae=0.1,
    )
    try:
        search.search_experimental_candidates(
            [row, second],
            horizon_minutes=HORIZON,
            analysis_as_of_utc=AS_OF,
            config=search.Stage4SearchConfig(
                max_observations=1,
                max_candidates_returned=1,
            ),
        )
    except ValueError as exc:
        assert "max_observations" in str(exc)
    else:
        raise AssertionError("observation bound did not fail closed")

    try:
        search.Stage4SearchConfig(minimum_independent_occurrences=4)
        search.search_experimental_candidates(
            [row],
            horizon_minutes=HORIZON,
            analysis_as_of_utc=AS_OF,
            config=search.Stage4SearchConfig(minimum_independent_occurrences=4),
        )
    except ValueError as exc:
        assert "below five" in str(exc)
    else:
        raise AssertionError("experimental floor below five was accepted")

    descriptor = search.descriptor()
    assert descriptor["fixed_time_spacing_rule"] is None
    assert descriptor["outcome_fields_allowed_as_predicates"] is False
    assert descriptor["formula_registry_effect"] == "NONE"
    assert descriptor["delivery_channel"] == "NONE"
    assert descriptor["live_eligible"] is False
    assert descriptor["telegram_delivery_allowed"] is False
    assert descriptor["trade_execution_allowed"] is False
    source = Path(search.__file__).read_text(encoding="utf-8")
    for forbidden in (
        "import research_formula_store",
        "send_message(",
        "persist_discovery_run(",
        "record_shadow_results(",
    ):
        assert forbidden not in source


def main() -> None:
    _check_probability_gate_and_no_time_spacing()
    _check_one_wave_is_one_occurrence()
    _check_first_match_is_frozen_before_outcome()
    _check_asymmetry_route_is_independent_of_probability_route()
    _check_combined_dependencies_and_disclosure_only_q()
    _check_bounds_and_pure_boundary()
    print("research Stage-4 candidate search self-test passed")


if __name__ == "__main__":
    main()
