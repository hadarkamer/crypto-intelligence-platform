"""Deterministic checks for automatic formula discovery and holdout safety."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from statistics import mean

import research_formula_engine as engine


def _row(index: int):
    event_time = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(
        hours=index * 30
    )
    input_active, input_weekend, _ = engine.market_session_baseline.session_ratios(
        event_time - timedelta(minutes=60), event_time
    )
    outcome_active, outcome_weekend, _ = engine.market_session_baseline.session_ratios(
        event_time, event_time + timedelta(minutes=240)
    )
    market_session = (
        "ACTIVE"
        if engine.market_session_baseline.is_active_market(event_time)
        else "WEEKEND"
    )
    direction = "LONG" if index % 2 == 0 else "SHORT"
    signal = 2.0 if index % 5 in {0, 1, 2} else -2.0
    directional_return = 1.0 if signal > 0 else -0.4
    snapshot = {f"snapshot.synthetic.feature_{feature}": float((index + feature) % 11) for feature in range(24)}
    return {
        "feature_schema_version": "selftest-matrix-v2",
        "event": {
            "event_id": index + 1,
            "alert_time_utc": event_time,
            "symbol": "BTC" if index % 3 else "ETH",
            "direction": direction,
            "source_side": "SHORT" if direction == "LONG" else "LONG",
            "timeframe": "1h",
            "event_type": "SELFTEST_SIGNAL",
            "current_price": 100.0,
            "strategy_version": "selftest-v1",
            "code_version": "abc123",
        },
        "time_features": {
            "utc_hour": event_time.hour,
            "utc_weekday": event_time.weekday(),
            "utc_weekday_name": event_time.strftime("%A").upper(),
            "is_calendar_weekend_utc": event_time.weekday() >= 5,
            "is_market_weekend": market_session == "WEEKEND",
            "market_session": market_session,
            "market_regime": market_session,
            "fixed_utc_session_bucket": "SELFTEST",
        },
        "historical_context": {
            "event_market_session": market_session,
            "windows": {
                "60m": {
                    "session_active_ratio": input_active,
                    "session_weekend_ratio": input_weekend,
                    "session_composition": (
                        "ACTIVE_ONLY"
                        if input_active == 1.0
                        else "WEEKEND_ONLY"
                        if input_active == 0.0
                        else "MIXED"
                    ),
                    "prior_points": 120,
                    "sufficient_history": True,
                    "price_change_pct_percentile_session_matched": (
                        85.0 if signal > 0 else 15.0
                    ),
                    "price_change_pct_abs_percentile_session_matched": 80.0,
                    "oi_change_pct_percentile_session_matched": float(index % 100),
                }
            },
        },
        "raw_features": {
            "captured_event_inputs": {
                "event_initial_target_distance_pct": 1.0 + (index % 4) / 10.0,
                "snapshot_inputs": {"near_share_pct": 60.0 + index % 20},
            },
            "latest_at_or_before_alert": {
                "price_oi": {"available": True, "age_minutes": 1.0},
                "futures_cvd": {"available": True, "age_minutes": 1.0, "buy_sell_ratio": 1.2},
                "spot_cvd": {"available": True, "age_minutes": 1.0, "buy_sell_ratio": 1.4},
            },
            "windows": {
                "60m": {
                    "session_active_ratio": input_active,
                    "session_weekend_ratio": input_weekend,
                    "session_composition": (
                        "ACTIVE_ONLY"
                        if input_active == 1.0
                        else "WEEKEND_ONLY"
                        if input_active == 0.0
                        else "MIXED"
                    ),
                    "price_change_pct": signal if direction == "LONG" else -signal,
                    "oi_change_pct": float(index % 7) - 3.0,
                    "futures_continuous_cvd_change_usd": signal * 1_000_000,
                    "spot_continuous_cvd_change_usd": signal * 500_000,
                    "futures_api_cvd_change_usd": signal * 800_000,
                    "spot_api_cvd_change_usd": signal * 400_000,
                    "spot_to_futures_abs_cvd_ratio": 0.5,
                    "price_oi_state": "PRICE_UP__OI_UP" if signal > 0 else "PRICE_DOWN__OI_UP",
                    "spot_futures_alignment": "ALIGNED",
                    "price_spot_alignment": "ALIGNED",
                    "price_futures_alignment": "ALIGNED",
                    "complete": True,
                }
            },
        },
        "model_features": {
            "alert_score": 60.0 + index % 35,
            "initial_target_distance_pct": 1.0,
            "categories": ["SYNTHETIC"],
            "snapshot_features": snapshot,
        },
        "sequence_features": {
            "30m": {
                "same_symbol_alerts": index % 4,
                "same_symbol_same_direction": index % 3,
                "same_symbol_distinct_event_types": 1,
                "same_setup_repetitions": index % 5,
                "market_alerts": index % 9,
                "market_distinct_symbols": 2,
                "market_long_alerts": index % 6,
                "market_short_alerts": (index + 2) % 6,
                "market_direction_balance_pct": float((index % 11) - 5) * 10.0,
            }
        },
        "outcome_label": {
            "horizon_minutes": 240,
            "session_active_ratio": outcome_active,
            "session_weekend_ratio": outcome_weekend,
            "session_composition": (
                "ACTIVE_ONLY"
                if outcome_active == 1.0
                else "WEEKEND_ONLY"
                if outcome_active == 0.0
                else "MIXED"
            ),
            "movement_width_reference": {"floor_scale_factor": 1.0},
            "directional_return_pct": directional_return,
            "path_success": directional_return > 0,
            "first_touch_status": (
                "HIT" if directional_return > 0 else "MISS"
            ),
            "mfe_pct": 1.8 if directional_return > 0 else 0.4,
            "mae_pct": 0.2 if directional_return > 0 else 1.1,
            "full_horizon_mae_pct": 8.0 if directional_return > 0 else 9.0,
            "time_to_first_progress_seconds": 120 if directional_return > 0 else 1800,
            "time_to_mfe_seconds": 900,
            "target_progress_ratio": 0.9 if directional_return > 0 else 0.2,
            "target_reached": directional_return > 0,
        },
    }


def _hierarchy_row(index: int) -> dict:
    """Five bounded feature families with stable incremental information."""
    event_time = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(
        hours=index * 30
    )
    residue = index % 17
    false_residues = (
        {0, 1},
        {2, 3},
        {4, 5},
        {6, 7},
        {8, 9},
    )
    factors = [residue not in blocked for blocked in false_residues]
    success = all(factors)
    return {
        "feature_schema_version": "hierarchy-selftest-v1",
        "event": {
            "event_id": 100_000 + index,
            "alert_time_utc": event_time,
            "symbol": "BTC",
            "direction": "LONG",
            "event_type": "HIERARCHY_SELFTEST",
            "current_price": 100.0,
        },
        "model_features": {
            "alert_score": None,
            "initial_target_distance_pct": None,
            "categories": [],
            "snapshot_features": {
                f"hierarchy.factor_{offset}": float(value)
                for offset, value in enumerate(factors)
            },
        },
        "outcome_label": {
            "horizon_minutes": 240,
            "session_active_ratio": 1.0,
            "session_weekend_ratio": 0.0,
            "session_composition": "ACTIVE_ONLY",
            "movement_width_reference": {"floor_scale_factor": 1.0},
            "directional_return_pct": 2.0 if success else -0.5,
            "path_success": success,
            "first_touch_status": "HIT" if success else "MISS",
            "mfe_pct": 3.0 if success else 0.2,
            "mae_pct": 0.2 if success else 2.0,
            "time_to_first_progress_seconds": 60 if success else 1800,
            "time_to_mfe_seconds": 600,
            "target_progress_ratio": 1.0 if success else 0.1,
            "target_reached": success,
        },
    }


def _summarize(selected, universe):
    """Bypass episode construction in tests of unrelated metric semantics."""

    return engine._metrics(
        selected, universe, already_independent_episodes=True
    )


def run() -> None:
    assert engine.candidate_feature_allowed(
        "raw.60m.futures_continuous_cvd_change_usd"
    )
    assert not engine.discovery_candidate_feature_allowed(
        "raw.60m.futures_continuous_cvd_change_usd"
    )
    assert engine.candidate_feature_allowed(
        "aligned_log.60m.spot_api_cvd_change_usd"
    )
    assert not engine.discovery_candidate_feature_allowed(
        "aligned_log.60m.spot_api_cvd_change_usd"
    )
    assert engine.candidate_feature_allowed(
        "historical.60m.spot_continuous_cvd_change_usd_"
        "median_session_matched"
    )
    assert not engine.discovery_candidate_feature_allowed(
        "historical.60m.spot_continuous_cvd_change_usd_"
        "median_session_matched"
    )
    assert engine.discovery_candidate_feature_allowed(
        "historical.60m.futures_continuous_cvd_change_usd_"
        "percentile_session_matched"
    )
    assert abs(
        engine._one_sided_two_proportion_p(10, 10, 8, 10)
        - 0.23684210526315788
    ) < 1e-12
    joint_route_q = engine._bh_q_values([0.15, 0.80])
    assert joint_route_q == [0.30, 0.80]
    assert joint_route_q[0] > 0.20
    asymmetry_fit = {
        "hit_rate_pct": 40.0,
        "hit_rate_improvement_pct_points": -5.0,
        "favorable_dominance_rate_pct": 80.0,
        "favorable_dominance_improvement_pct_points": 15.0,
        "median_paired_favorable_minus_adverse_pct": 2.0,
    }
    asymmetry_selection = {
        **asymmetry_fit,
        "hit_rate_pct": 70.0,
        "favorable_dominance_rate_pct": 75.0,
    }
    assert engine._stable_route_names(
        asymmetry_fit,
        asymmetry_selection,
        maximum_rate_gap=20.0,
    ) == ("ASYMMETRY",)
    no_stable_route = {
        **asymmetry_selection,
        "favorable_dominance_improvement_pct_points": -1.0,
        "median_paired_favorable_minus_adverse_pct": -0.1,
    }
    assert not engine._stable_route_names(
        asymmetry_fit,
        no_stable_route,
        maximum_rate_gap=20.0,
    )
    assert engine.condition_matches(
        {"x": 1.0}, {"feature": "x", "operator": "==", "value": 1}
    )
    assert not engine.condition_matches(
        {"x": "1"}, {"feature": "x", "operator": "==", "value": 1}
    )
    assert not engine.condition_matches(
        {"x": "0.8"}, {"feature": "x", "operator": ">=", "value": 0.8}
    )
    assert not engine.condition_matches(
        {"x": True}, {"feature": "x", "operator": "==", "value": 1}
    )
    assert engine.MIN_MEDIAN_MFE_BY_HORIZON == dict(
        engine.research_no_dwell_outcome.BASE_FAVORABLE_WIDTH_PCT_BY_HORIZON
    )
    rows = [_row(index) for index in range(140)]

    # Evidence-family overlap is based on independent Market Episodes, not on
    # the several raw alerts that happened inside the same broad move.
    episode_start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    correlated = []
    for offset, minute in enumerate((0, 30, 60, 48 * 60), start=1):
        item = _row(offset)
        item["event"].update(
            {
                "event_id": 9000 + offset,
                "alert_time_utc": episode_start + timedelta(minutes=minute),
                "symbol": "BTC",
                "direction": "LONG",
            }
        )
        item["outcome_label"].update(
            {"path_success": True, "first_touch_status": "HIT"}
        )
        correlated.append(item)
    evidence_a = engine._metrics(
        correlated[:3],
        correlated,
        evidence_as_of_utc=episode_start + timedelta(hours=72),
        include_private_evidence_keys=True,
    )
    evidence_b = engine._metrics(
        [correlated[0], correlated[2]],
        correlated,
        evidence_as_of_utc=episode_start + timedelta(hours=72),
        include_private_evidence_keys=True,
    )
    assert evidence_a["raw_sample_size"] == 3
    assert evidence_b["raw_sample_size"] == 2
    assert evidence_a["_market_episode_evidence_intervals"] == evidence_b[
        "_market_episode_evidence_intervals"
    ]
    assert len(evidence_a["_market_episode_evidence_intervals"]) == 1
    tail_rows = []
    for offset, adverse in enumerate((0.2, 0.3, 10.0), start=1):
        item = _row(offset + 20)
        item["event"].update(
            {
                "event_id": 9100 + offset,
                "alert_time_utc": episode_start,
                "symbol": ("BTC", "ETH", "SOL")[offset - 1],
                "direction": "LONG",
            }
        )
        item["outcome_label"].update(
            {
                "path_success": True,
                "first_touch_status": "HIT",
                "mfe_pct": 2.0,
                "mae_pct": adverse,
            }
        )
        tail_rows.append(item)
    tail_control = correlated[-1]
    tail_metrics = engine._metrics(
        tail_rows,
        [*tail_rows, tail_control],
        evidence_as_of_utc=episode_start + timedelta(hours=72),
    )
    assert tail_metrics["median_mae_pct"] == 0.3
    assert tail_metrics["mae_p90_pct"] == 10.0
    assert tail_metrics["mae_p95_pct"] == 10.0
    shifted_evidence = engine._metrics(
        correlated[1:3],
        correlated,
        evidence_as_of_utc=episode_start + timedelta(hours=72),
        include_private_evidence_keys=True,
    )
    assert engine.research_formula_families.evidence_interval_overlap(
        [
            ("discovery", start, end)
            for start, end in evidence_a[
                "_market_episode_evidence_intervals"
            ]
        ],
        [
            ("discovery", start, end)
            for start, end in shifted_evidence[
                "_market_episode_evidence_intervals"
            ]
        ],
    ) >= 0.75

    # Success is the explicit final first-touch label, never the endpoint
    # return.  A later negative close cannot erase a prior touch, while a
    # positive close cannot fabricate one.  Full-horizon MAE is diagnostic;
    # ranking risk is the pre-qualifying `mae_pct` supplied by the matrix.
    endpoint_positive_miss = _row(0)
    endpoint_positive_miss["outcome_label"].update(
        {
            "directional_return_pct": 9.0,
            "path_success": False,
            "first_touch_status": "MISS",
            "mae_pct": 0.25,
            "full_horizon_mae_pct": 25.0,
        }
    )
    endpoint_negative_hit = _row(1)
    endpoint_negative_hit["outcome_label"].update(
        {
            "directional_return_pct": -9.0,
            "path_success": True,
            "first_touch_status": "HIT",
            "mae_pct": 0.75,
            "full_horizon_mae_pct": 50.0,
        }
    )
    explicit_metrics = _summarize(
        [endpoint_positive_miss, endpoint_negative_hit],
        [endpoint_positive_miss, endpoint_negative_hit],
    )
    assert explicit_metrics["successes"] == 1
    assert explicit_metrics["hit_rate_pct"] == 50.0
    assert explicit_metrics["median_mae_pct"] == 0.5
    positive_miss_metrics = _summarize(
        [endpoint_positive_miss], [endpoint_positive_miss]
    )
    negative_hit_metrics = _summarize(
        [endpoint_negative_hit], [endpoint_negative_hit]
    )
    assert positive_miss_metrics["successes"] == 0
    assert positive_miss_metrics["hit_rate_pct"] == 0.0
    assert negative_hit_metrics["successes"] == 1
    assert negative_hit_metrics["hit_rate_pct"] == 100.0

    session_selected = _row(500)
    session_selected["event"]["event_id"] = 900_000
    session_selected["event"]["symbol"] = "BTC"
    session_selected["outcome_label"].update(
        {
            "directional_return_pct": -5.0,
            "path_success": True,
            "first_touch_status": "HIT",
            "session_active_ratio": 1.0,
            "session_weekend_ratio": 0.0,
            "session_composition": "ACTIVE_ONLY",
        }
    )
    endpoint_positive_controls = []
    for index in range(30):
        control = _row(600 + index)
        control["event"]["event_id"] = 901_000 + index
        control["event"]["symbol"] = "BTC"
        control["outcome_label"].update(
            {
                "directional_return_pct": 5.0,
                "path_success": False,
                "first_touch_status": "MISS",
                "session_active_ratio": 1.0,
                "session_weekend_ratio": 0.0,
                "session_composition": "ACTIVE_ONLY",
            }
        )
        endpoint_positive_controls.append(control)
    baseline_metrics = _summarize(
        [session_selected],
        [session_selected, *endpoint_positive_controls],
    )
    assert baseline_metrics["control_sample_size"] == 30
    assert baseline_metrics["control_hit_rate_pct"] == 0.0
    assert baseline_metrics["session_matched_hit_rate_baseline_pct"] == 0.0
    assert baseline_metrics["hit_rate_improvement_pct_points"] == 100.0
    assert baseline_metrics["session_hit_rate_improvement_pct_points"] == 100.0
    assert baseline_metrics["unadjusted_one_sided_p_value"] is not None
    assert baseline_metrics["one_sided_p_value"] is not None

    missing_label = _row(2)
    missing_label["outcome_label"].pop("path_success")
    missing_metrics = _summarize([missing_label], [missing_label])
    assert missing_metrics["successes"] == 0
    assert missing_metrics["hit_rate_pct"] is None

    malformed_labels = []
    for event_id, path_success, status in (
        (20, "true", "HIT"),
        (21, None, "PENDING"),
        (22, True, "MISS"),
        (23, False, "HIT"),
    ):
        malformed = _row(event_id)
        malformed["outcome_label"].update(
            {"path_success": path_success, "first_touch_status": status}
        )
        malformed_labels.append(malformed)
    malformed_metrics = _summarize(
        malformed_labels, malformed_labels
    )
    assert malformed_metrics["successes"] == 0
    assert malformed_metrics["hit_rate_pct"] is None

    eligibility_rows = [_row(index) for index in range(8)]
    eligibility_rows[2]["outcome_label"].pop("path_success")
    eligibility_rows[3]["outcome_label"].update(
        {"path_success": True, "first_touch_status": "MISS"}
    )
    eligibility = engine.discover_formulas(
        eligibility_rows,
        horizon_minutes=240,
        feature_schema_version="explicit-first-touch-selftest",
        config=engine.DiscoveryConfig(
            discovery_fraction=0.5,
            min_discovery_samples=1,
            min_holdout_samples=1,
            max_single_predicates=2,
            max_pair_candidates=0,
            max_triple_candidates=0,
            max_candidates_evaluated=2,
            max_formulas_returned=1,
        ),
    )
    assert (
        eligibility["discovery_sample_size"]
        + eligibility["selection_sample_size"]
        + eligibility["holdout_sample_size"]
        == 8
    ), "decision rows must freeze membership before terminal labels are inspected"
    pending_same_anchor = _row(500)
    pending_same_anchor["event"] = dict(pending_same_anchor["event"])
    pending_same_anchor["event"]["event_id"] = 50_000
    pending_same_anchor["outcome_label"] = dict(
        pending_same_anchor["outcome_label"]
    )
    pending_same_anchor["outcome_label"].update(
        {"path_success": None, "first_touch_status": "PENDING"}
    )
    hit_same_anchor = _row(500)
    incomplete_cohort = engine.summarize_outcomes(
        [hit_same_anchor, pending_same_anchor],
        [hit_same_anchor, pending_same_anchor],
        evidence_as_of_utc=(
            hit_same_anchor["event"]["alert_time_utc"] + timedelta(days=2)
        ),
    )
    assert incomplete_cohort["sample_size"] == 0
    assert incomplete_cohort["market_episode_open_matches"] == 1
    corrected = engine._bh_q_values([0.01, None, 0.02])
    assert corrected[1] is None
    assert corrected[0] is not None and corrected[0] >= 0.03 - 1e-12
    assert corrected[2] is not None and corrected[2] >= 0.03 - 1e-12

    # Prospective matching is explicit about all three possible states. It
    # reads decision-time features only: the guarded outcome mapping must never
    # be inspected while evaluating a formula.
    class _OutcomeGuard(dict):
        def get(self, key, default=None):
            assert key != "outcome_label", "Shadow matching read an outcome"
            return super().get(key, default)

        def __getitem__(self, key):
            assert key != "outcome_label", "Shadow matching read an outcome"
            return super().__getitem__(key)

    guarded = _OutcomeGuard(_row(0))
    condition = {
        "feature": "aligned.60m.price_change_pct",
        "operator": ">=",
        "value": 1.0,
    }
    matched = engine.evaluate_formula(
        guarded,
        direction="LONG",
        conditions=[condition],
    )
    assert matched["status"] == "MATCHED" and matched["matched"] is True
    assert matched["condition_results"][0]["actual"] == 2.0
    assert all(not key.startswith("outcome") for key in matched["features"])

    unmatched = engine.evaluate_formula(
        guarded,
        direction="LONG",
        conditions=[{**condition, "value": 3.0}],
    )
    assert unmatched["status"] == "UNMATCHED"
    assert unmatched["matched"] is False

    unevaluable = engine.evaluate_formula(
        guarded,
        direction="LONG",
        conditions=[
            {
                "feature": "raw.60m.feature_that_was_not_captured",
                "operator": ">=",
                "value": 1.0,
            }
        ],
    )
    assert unevaluable["status"] == "UNEVALUABLE"
    assert unevaluable["condition_results"][0]["available"] is False
    known_false_with_unknown = engine.evaluate_formula(
        guarded,
        direction="LONG",
        conditions=[
            {**condition, "value": 3.0},
            {
                "feature": "raw.60m.feature_that_was_not_captured",
                "operator": ">=",
                "value": 1.0,
            },
        ],
    )
    assert known_false_with_unknown["status"] == "UNMATCHED"
    assert known_false_with_unknown["matched"] is False
    assert engine.evaluate_formula(
        None, direction="LONG", conditions=[condition]
    )["status"] == "UNEVALUABLE"
    paired_rows = []
    for index in range(60):
        original = _row(index)
        opposite = _row(index)
        opposite["event"] = dict(opposite["event"])
        opposite["event"]["event_id"] = 10_000 + index
        opposite["event"]["direction"] = (
            "SHORT" if original["event"]["direction"] == "LONG" else "LONG"
        )
        paired_rows.extend((original, opposite))
    paired_result = engine.discover_formulas(
        paired_rows,
        horizon_minutes=240,
        feature_schema_version="paired-time-selftest",
        config=engine.DiscoveryConfig(
            max_single_predicates=8,
            max_pair_candidates=4,
            max_triple_candidates=0,
            max_candidates_evaluated=20,
            max_formulas_returned=5,
        ),
    )
    assert paired_result["discovery_sample_size"] == 58
    assert paired_result["selection_sample_size"] == 26
    assert paired_result["holdout_sample_size"] == 36
    assert "identical timestamps never split" in paired_result["split_policy"]
    selected_ratios = [0.0, 0.2, 0.5, 0.8, 1.0]
    optimized_profile = engine._composition_profile_weights(
        rows[:12], selected_ratios
    )
    optimized_by_id = {
        int(row["event"]["event_id"]): weight
        for row, weight in optimized_profile
    }
    for row in rows[:12]:
        historical_ratio = engine._row_outcome_active_ratio(row)
        brute = mean(
            engine.market_session_baseline.composition_weight(
                selected_ratio, historical_ratio
            )
            for selected_ratio in selected_ratios
        )
        optimized = optimized_by_id.get(int(row["event"]["event_id"]), 0.0)
        assert abs(brute - optimized) < 1e-12

    result = engine.discover_formulas(
        rows,
        horizon_minutes=240,
        feature_schema_version="selftest-matrix-v2",
    )
    assert result["available"] is True
    assert result["discovery_sample_size"] == 68
    assert result["selection_sample_size"] == 30
    assert result["holdout_sample_size"] == 42
    assert result["candidates_evaluated"] >= 1000
    assert result["formulas"]
    assert result["automatic_stage_ceiling"] == "SHADOW"
    assert "future-Shadow validation" in result["live_activation"]
    assert result["condition_family_policy_version"] == (
        engine.research_formula_families.CONDITION_FAMILY_POLICY_VERSION
    )
    assert sum(
        int(direction["family_policy_rejections"])
        for direction in result["directions"]
    ) > 0
    assert all(
        engine.research_formula_families.condition_family_policy(
            candidate["conditions"],
            justified_exceptions=(
                candidate["multiple_testing"]["condition_family_policy"][
                    "justified_exceptions"
                ]
            ),
            enforce_correlated_families=True,
        )["valid"]
        for candidate in result["formulas"]
    )
    assert all(
        candidate["multiple_testing"]["condition_family_policy"][
            "policy_version"
        ]
        == engine.research_formula_families.CONDITION_FAMILY_POLICY_VERSION
        and candidate["multiple_testing"]["condition_family_policy"][
            "enforcement"
        ]
        == "ALL_CONDITION_DEPTHS"
        for candidate in result["formulas"]
    )
    assert any(
        int(candidate["condition_count"]) > 1
        for candidate in result["formulas"]
    ), "strict family enforcement must preserve valid multi-condition formulas"

    # Exercise every ordinary pair/triple family path deterministically.  The
    # production-sized fixture above happens to expose correlated candidates,
    # but its ranking must not be the only thing proving that all-depth
    # enforcement reaches price and OI pairs and triples.
    family_rows = []
    for index in range(180):
        row = _row(index)
        row["model_features"]["snapshot_features"].update(
            {
                "snapshot.synthetic.price_change_a": float(index % 3),
                "snapshot.synthetic.price_change_b": float(index % 5),
                "snapshot.synthetic.price_change_c": float(index % 7),
                "snapshot.synthetic.open_interest_a": float(index % 4),
                "snapshot.synthetic.open_interest_b": float(index % 6),
                "snapshot.synthetic.open_interest_c": float(index % 11),
                "snapshot.synthetic.spot_cvd_guard": float(index % 13),
            }
        )
        family_rows.append(row)
    family_features = (
        "model.snapshot.synthetic.price_change_a",
        "model.snapshot.synthetic.price_change_b",
        "model.snapshot.synthetic.price_change_c",
        "model.snapshot.synthetic.open_interest_a",
        "model.snapshot.synthetic.open_interest_b",
        "model.snapshot.synthetic.open_interest_c",
        "model.snapshot.synthetic.spot_cvd_guard",
    )
    family_catalog = [
        {
            "feature": feature,
            "operator": ">=",
            "value": 1.0,
            "_source": "NUMERIC_QUANTILE",
            "_quantile_fraction": 0.50,
        }
        for feature in family_features
    ]
    original_predicate_catalog = engine._predicate_catalog
    original_family_policy = (
        engine.research_formula_families.condition_family_policy
    )
    rejected_family_depths = set()

    def _audited_family_policy(
        conditions,
        *,
        justified_exceptions=(),
        enforce_correlated_families=True,
    ):
        policy = original_family_policy(
            conditions,
            justified_exceptions=justified_exceptions,
            enforce_correlated_families=enforce_correlated_families,
        )
        if policy["valid"] is False and enforce_correlated_families is True:
            depth = len(conditions)
            for family in ("price", "open_interest"):
                if policy["families"].count(family) > 1:
                    rejected_family_depths.add((family, depth))
        return policy

    engine._predicate_catalog = lambda rows, feature_rows, config: [
        dict(predicate) for predicate in family_catalog
    ]
    engine.research_formula_families.condition_family_policy = (
        _audited_family_policy
    )
    try:
        family_result = engine.discover_formulas(
            family_rows,
            horizon_minutes=240,
            feature_schema_version="condition-family-engine-selftest-v1",
            config=engine.DiscoveryConfig(
                numeric_quantiles=(0.50,),
                max_single_predicates=7,
                max_pair_candidates=30,
                max_triple_candidates=100,
                max_candidates_evaluated=200,
                max_formulas_returned=40,
            ),
        )
    finally:
        engine._predicate_catalog = original_predicate_catalog
        engine.research_formula_families.condition_family_policy = (
            original_family_policy
        )
    assert {
        ("price", 2),
        ("price", 3),
        ("open_interest", 2),
        ("open_interest", 3),
    }.issubset(rejected_family_depths)
    assert all(
        original_family_policy(candidate["conditions"])["valid"] is True
        for candidate in family_result["formulas"]
    )
    assert any(
        int(candidate["condition_count"]) > 1
        for candidate in family_result["formulas"]
    ), "all-depth rejection must retain independent multi-condition candidates"

    formula = result["formulas"][0]
    assert formula["conditions"]
    assert formula["recommended_stage"] in {
        "DISCOVERED", "BACKTESTED", "HOLDOUT_PASSED", "SHADOW"
    }
    assert formula["live_alert_approved"] is False
    assert "sample_size" in formula["discovery_metrics"]
    assert "mae_p95_pct" in formula["holdout_metrics"]
    assert "median_mfe_percentile_pct" in formula["holdout_metrics"]
    assert "session_adjusted_mfe_percentile_pct" in formula["holdout_metrics"]
    assert "outcome_session_composition_counts" in formula["holdout_metrics"]
    assert "movement_width_floor_effective_pct" in formula["holdout_metrics"]
    assert "favorable_minus_p90_adverse_pct" in formula["holdout_metrics"]
    assert "q_value" in formula["multiple_testing"]
    assert all(
        candidate["recommended_stage"] != "SHADOW"
        for candidate in result["formulas"]
    ), "a sub-day holdout must never enter Shadow"

    features = engine.extract_decision_features(rows[0])
    assert all(not key.startswith("outcome") for key in features)
    assert "aligned.60m.price_change_pct" in features
    assert "raw.60m.session_active_ratio" in features
    assert "historical.60m.price_change_pct_percentile_session_matched" in features

    max_pain_row = _row(0)
    max_pain_row["max_pain_features"] = {
        "evaluation_status": "EVALUABLE",
        "available_at_utc": max_pain_row["event"]["alert_time_utc"]
        - timedelta(minutes=5),
        "features": {
            "max_pain.12h.short_target_signed_distance_pct": 2.5,
            "max_pain.aggregate.short_long_liquidity_ratio": 1.8,
            # A malformed unnamespaced value must never become a predicate.
            "source_snapshot_set_id": 123,
        },
    }
    max_pain_decision = engine.extract_decision_features(max_pain_row)
    assert max_pain_decision[
        "max_pain.12h.short_target_signed_distance_pct"
    ] == 2.5
    assert max_pain_decision[
        "max_pain.aggregate.short_long_liquidity_ratio"
    ] == 1.8
    assert "source_snapshot_set_id" not in max_pain_decision
    max_pain_row["max_pain_features"]["evaluation_status"] = "UNEVALUABLE"
    assert not any(
        key.startswith("max_pain.")
        for key in engine.extract_decision_features(max_pain_row)
    )
    assert engine.candidate_feature_allowed(
        "historical.60m.price_change_pct_percentile_session_matched"
    )
    for technical_feature in (
        "historical.60m.price_change_pct_history_samples",
        "historical.60m.price_change_pct_session_matched_samples",
        "historical.60m.prior_points",
        "historical.60m.sufficient_history",
        "latest.price_oi.age_minutes",
        "time.utc_hour",
        "time.market_utc_offset_minutes",
    ):
        assert not engine.candidate_feature_allowed(technical_feature)
    assert all(
        engine.candidate_feature_allowed(condition["feature"])
        for candidate in result["formulas"]
        for condition in candidate["conditions"]
    )
    assert result["config"]["hierarchical_search_enabled"] is False
    assert all(candidate["condition_count"] <= 3 for candidate in result["formulas"])
    assert all(
        direction["hierarchical_search"]["quad_candidates_attempted"] == 0
        and direction["hierarchical_search"]["quint_candidates_attempted"] == 0
        for direction in result["directions"]
    )
    assert engine.formula_key(
        direction=formula["direction"],
        horizon_minutes=240,
        feature_schema_version="selftest-matrix-v2",
        conditions=list(reversed(formula["conditions"])),
    ) == formula["formula_key"]
    assert engine.formula_key(
        direction=formula["direction"],
        horizon_minutes=240,
        feature_schema_version="selftest-matrix-v2",
        conditions=formula["conditions"],
        condition_family_exceptions=(
            "price: independent multi-window confirmation retained for audit",
        ),
    ) != formula["formula_key"]

    # A tiny 100%-hit move must not outrank a materially wider, still reliable
    # move. This guards the production issue observed in formula v1.
    common = {
        "sample_size": 20,
        "sample_share_pct": 10.0,
        "median_time_to_first_progress_seconds": 300,
        "avg_target_progress_ratio": 0.5,
    }
    narrow = {
        **common,
        "hit_rate_pct": 100.0,
        "wilson_95_lower_pct": 83.0,
        "hit_rate_improvement_pct_points": 30.0,
        "median_mfe_pct": 0.385,
        "universe_p90_mfe_pct": 3.0,
        "median_mfe_percentile_pct": 30.0,
        "median_mae_pct": 0.10,
        "mae_p90_pct": 0.25,
        "median_mfe_mae_ratio": 3.85,
    }
    wide = {
        **common,
        "hit_rate_pct": 75.0,
        "wilson_95_lower_pct": 55.0,
        "hit_rate_improvement_pct_points": 15.0,
        "median_mfe_pct": 2.50,
        "universe_p90_mfe_pct": 3.0,
        "median_mfe_percentile_pct": 85.0,
        "median_mae_pct": 0.40,
        "mae_p90_pct": 0.80,
        "median_mfe_mae_ratio": 6.25,
    }
    narrow_score = engine._final_score(
        narrow, narrow, horizon_minutes=240, q_value=0.01, complexity=2
    )
    wide_score = engine._final_score(
        wide, wide, horizon_minutes=240, q_value=0.01, complexity=2
    )
    assert wide_score > narrow_score
    assert engine._final_score(
        wide, wide, horizon_minutes=240, q_value=0.01, complexity=5
    ) < engine._final_score(
        wide, wide, horizon_minutes=240, q_value=0.01, complexity=3
    )
    narrow_stage, narrow_reasons = engine._recommended_stage(
        {
            **narrow,
            "time_span_hours": 96,
            "distinct_utc_dates": 4,
        },
        {
            **narrow,
            "time_span_hours": 48,
            "distinct_utc_dates": 3,
        },
        horizon_minutes=240,
        q_value=0.01,
        config=engine.DiscoveryConfig(),
    )
    assert narrow_stage == "BACKTESTED"
    assert "wide favorable movement floor" in narrow_reasons
    _, hierarchical_sample_reasons = engine._recommended_stage(
        {
            **wide,
            "sample_size": 20,
            "time_span_hours": 96,
            "distinct_utc_dates": 4,
        },
        {
            **wide,
            "sample_size": 10,
            "time_span_hours": 48,
            "distinct_utc_dates": 3,
        },
        horizon_minutes=240,
        q_value=0.01,
        config=engine.DiscoveryConfig(),
        complexity=4,
    )
    assert "hierarchical discovery sample" in hierarchical_sample_reasons
    assert "hierarchical holdout sample" in hierarchical_sample_reasons

    # Weekend calibration changes only the absolute width floor. The hit,
    # Wilson, improvement, risk and percentile gates remain unchanged.
    assert engine.minimum_wide_move_pct(
        240, {"movement_width_floor_scale_factor": 0.60}
    ) == 0.60

    # A missing prior-only reference must never be reconstructed from future
    # control MFE. This population deliberately has small weekend future MFE
    # and large active future MFE, which used to imply a lower fallback scale.
    def _outcome_row(
        event_id: int,
        *,
        active_ratio: float,
        mfe_pct: float,
        width_scale=None,
    ):
        label = {
            "horizon_minutes": 240,
            "session_active_ratio": active_ratio,
            "session_weekend_ratio": 1.0 - active_ratio,
            "session_composition": (
                "ACTIVE_ONLY" if active_ratio == 1.0 else "WEEKEND_ONLY"
            ),
            "directional_return_pct": 1.0,
            "path_success": True,
            "first_touch_status": "HIT",
            "mfe_pct": mfe_pct,
            "mae_pct": 0.20,
            "time_to_first_progress_seconds": 60,
            "time_to_mfe_seconds": 600,
            "target_progress_ratio": 1.0,
            "target_reached": True,
        }
        if width_scale is not None:
            label["movement_width_reference"] = {
                "floor_scale_factor": width_scale,
                "source": "frozen prior-only self-test calibration",
            }
        return {
            "event": {
                "event_id": event_id,
                "alert_time_utc": datetime(2026, 8, 29, tzinfo=timezone.utc)
                + timedelta(minutes=event_id),
                "symbol": "BTC",
                "event_type": "SELFTEST_SIGNAL",
            },
            "outcome_label": label,
        }

    selected_without_reference = [
        _outcome_row(50_000, active_ratio=0.0, mfe_pct=1.25)
    ]
    weekend_controls = [
        _outcome_row(50_100 + index, active_ratio=0.0, mfe_pct=0.10)
        for index in range(30)
    ]
    active_controls = [
        _outcome_row(50_200 + index, active_ratio=1.0, mfe_pct=5.0)
        for index in range(30)
    ]
    missing_reference_metrics = _summarize(
        selected_without_reference,
        [*selected_without_reference, *weekend_controls, *active_controls],
    )
    assert missing_reference_metrics["session_matched_mfe_effective_samples"] >= 30
    assert missing_reference_metrics["active_reference_mfe_effective_samples"] >= 30
    assert (
        missing_reference_metrics["session_matched_control_p90_mfe_pct"]
        < missing_reference_metrics["active_reference_mfe_p90_pct"]
    )
    assert missing_reference_metrics["movement_width_floor_scale_factor"] == 1.0
    assert (
        missing_reference_metrics["movement_width_floor_effective_pct"]
        == missing_reference_metrics["movement_width_floor_base_pct"]
    )
    assert "no relaxation" in missing_reference_metrics["movement_width_floor_source"]

    selected_with_reference = [
        _outcome_row(
            50_000,
            active_ratio=0.0,
            mfe_pct=1.25,
            width_scale=0.60,
        )
    ]
    frozen_reference_metrics = _summarize(
        selected_with_reference,
        [*selected_with_reference, *weekend_controls, *active_controls],
    )
    assert frozen_reference_metrics["movement_width_floor_scale_factor"] == 0.60
    assert frozen_reference_metrics["movement_width_floor_effective_pct"] == 0.60
    for probability_or_risk_metric in (
        "hit_rate_pct",
        "wilson_95_lower_pct",
        "median_mae_pct",
        "mae_p90_pct",
        "median_mfe_mae_ratio",
    ):
        assert (
            frozen_reference_metrics[probability_or_risk_metric]
            == missing_reference_metrics[probability_or_risk_metric]
        )

    # Four/five-condition search is opt-in and hierarchical: only stable triple
    # parents enter the bounded beam, and every child must improve in both
    # halves of a nested chronological screen inside discovery.  The outer
    # holdout may validate/rank frozen finalists but must never shape the beam.
    hierarchy_rows = [_hierarchy_row(index) for index in range(816)]
    hierarchy_config = engine.DiscoveryConfig(
        numeric_quantiles=(0.50,),
        max_single_predicates=10,
        max_pair_candidates=20,
        max_triple_candidates=30,
        max_candidates_evaluated=100,
        max_formulas_returned=40,
        hierarchical_search_enabled=True,
        hierarchical_beam_width=10,
        hierarchical_extension_predicates=10,
        max_quad_candidates=30,
        max_quint_candidates=20,
        hierarchical_min_discovery_samples=40,
        hierarchical_min_holdout_samples=20,
        hierarchical_discovery_sample_increment=0,
        hierarchical_holdout_sample_increment=0,
        hierarchical_min_parent_gain=0.25,
        evidence_family_overlap_threshold=0.95,
    )
    hierarchy_result = engine.discover_formulas(
        hierarchy_rows,
        horizon_minutes=240,
        feature_schema_version="hierarchy-selftest-v1",
        config=hierarchy_config,
    )
    long_diagnostics = next(
        direction
        for direction in hierarchy_result["directions"]
        if direction["direction"] == "LONG"
    )
    hierarchy_diagnostics = long_diagnostics["hierarchical_search"]
    assert hierarchy_diagnostics["stable_triple_parents"] > 0
    assert hierarchy_diagnostics["quad_candidates_passed_gain"] > 0
    assert hierarchy_diagnostics["quint_candidates_passed_gain"] > 0
    assert hierarchy_diagnostics["quad_candidates_attempted"] <= 30
    assert hierarchy_diagnostics["quint_candidates_attempted"] <= 20
    assert hierarchy_diagnostics["fit_rows"] > 0
    assert hierarchy_diagnostics["selection_rows"] > 0
    assert (
        hierarchy_diagnostics["outer_holdout_used_for_hierarchical_selection"]
        is False
    )
    assert long_diagnostics["candidates_evaluated"] <= 100
    hierarchical_formulas = [
        formula
        for formula in long_diagnostics["formulas"]
        if formula["condition_count"] >= 4
    ]
    assert all(
        formula["condition_count"] <= hierarchy_config.hierarchical_max_conditions
        for formula in long_diagnostics["formulas"]
    )
    assert hierarchy_result["walk_forward_policy_version"] == (
        engine.WALK_FORWARD_POLICY_VERSION
    )
    assert hierarchy_result["purge_policy_version"] == engine.PURGE_POLICY_VERSION
    assert hierarchy_result["embargo_policy_version"] == (
        engine.EMBARGO_POLICY_VERSION
    )
    for formula in long_diagnostics["formulas"]:
        walk_forward = formula["discovery_metrics"][
            "walk_forward_validation"
        ]
        assert walk_forward["complete"] is True
        assert walk_forward["completed_folds"] == 3
        assert walk_forward["outer_test_used"] is False
        assert walk_forward["purge_minutes"] == 1440
        assert walk_forward["embargo_minutes"] == 240
        assert all(
            fold["training_cutoff_utc"] < fold["validation_start_utc"]
            and fold["status"] == "COMPLETED"
            for fold in walk_forward["folds"]
        )
    assert any(formula["condition_count"] == 5 for formula in hierarchical_formulas)
    for formula in hierarchical_formulas:
        validation = formula["hierarchical_validation"]
        assert validation["passed"] is True
        assert validation["fit_incremental_score_gain"] >= 0.25
        assert validation["selection_incremental_score_gain"] >= 0.25
        assert validation["outer_holdout_used_for_selection"] is False
        assert "holdout_incremental_score_gain" not in validation
        families = [
            engine.research_formula_families.feature_correlation_family(
                condition["feature"]
            )
            for condition in formula["conditions"]
        ]
        assert len(families) == len(set(families))
        assert (
            formula["multiple_testing"]["hypotheses_tested_per_route"]
            == long_diagnostics["statistical_hypotheses_tested"]
        )
        assert (
            formula["multiple_testing"]["total_route_hypotheses_tested"]
            == 2 * long_diagnostics["statistical_hypotheses_tested"]
        )
        assert "evidence_family" in formula["multiple_testing"]

    # Changing only the outer holdout must not change which hierarchical
    # hypotheses were generated or their discovery-only q-value fingerprint.
    adversarial_holdout_rows = [_hierarchy_row(index) for index in range(816)]
    holdout_start = engine._utc(hierarchy_result["holdout_start_time_utc"])
    for row in adversarial_holdout_rows:
        if engine._utc(row["event"]["alert_time_utc"]) < holdout_start:
            continue
        row["model_features"]["snapshot_features"] = {
            feature: 0.0
            for feature in row["model_features"]["snapshot_features"]
        }
        row["outcome_label"].update(
            {
                "directional_return_pct": -9.0,
                "path_success": False,
                "first_touch_status": "MISS",
                "mfe_pct": 0.0,
                "mae_pct": 9.0,
                "target_progress_ratio": 0.0,
                "target_reached": False,
            }
        )
    adversarial_result = engine.discover_formulas(
        adversarial_holdout_rows,
        horizon_minutes=240,
        feature_schema_version="hierarchy-selftest-v1",
        config=hierarchy_config,
    )
    adversarial_long = next(
        direction
        for direction in adversarial_result["directions"]
        if direction["direction"] == "LONG"
    )
    adversarial_diagnostics = adversarial_long["hierarchical_search"]
    for diagnostic in (
        "stable_triple_parents",
        "quad_candidates_attempted",
        "quad_candidates_tested",
        "quad_candidates_passed_gain",
        "stable_quad_parents",
        "quint_candidates_attempted",
        "quint_candidates_tested",
        "quint_candidates_passed_gain",
        "hypothesis_family_fingerprint",
    ):
        assert adversarial_diagnostics[diagnostic] == hierarchy_diagnostics[diagnostic]
    def frozen_formula_signature(direction_result):
        return [
            (
                formula["formula_key"],
                formula["rank"],
                formula["ranking_score"],
                formula["multiple_testing"]["evidence_family"]["family_id"],
                tuple(
                    formula["multiple_testing"]["evidence_family"][
                        "family_member_formula_keys"
                    ]
                ),
            )
            for formula in direction_result["formulas"]
        ]

    assert frozen_formula_signature(adversarial_long) == frozen_formula_signature(
        long_diagnostics
    ), "final Test changed a frozen formula identity, rank or evidence family"

    # The public Selection partition is supplied explicitly to the hierarchy;
    # the final Test remains a separate third input.
    nested_boundary_rows = [_hierarchy_row(index) for index in range(40)]
    nested_boundary_rows[27]["event"]["alert_time_utc"] = nested_boundary_rows[
        28
    ]["event"]["alert_time_utc"]
    nested_boundary_result = engine._search_direction(
        nested_boundary_rows[:27],
        nested_boundary_rows[27:],
        [],
        direction="LONG",
        horizon_minutes=240,
        feature_schema_version="hierarchy-boundary-selftest-v1",
        config=hierarchy_config,
    )
    nested_boundary_diagnostics = nested_boundary_result["hierarchical_search"]
    assert nested_boundary_diagnostics["fit_rows"] == 27
    assert nested_boundary_diagnostics["selection_rows"] == 13
    assert nested_boundary_diagnostics["final_test_used_for_hierarchical_selection"] is False

    four_only_result = engine.discover_formulas(
        hierarchy_rows,
        horizon_minutes=240,
        feature_schema_version="hierarchy-four-only-selftest-v1",
        config=replace(
            hierarchy_config,
            hierarchical_max_conditions=4,
            max_quad_candidates=10,
            max_quint_candidates=20,
        ),
    )
    four_only_long = next(
        direction
        for direction in four_only_result["directions"]
        if direction["direction"] == "LONG"
    )
    assert four_only_long["hierarchical_search"]["quad_candidates_attempted"] <= 10
    assert four_only_long["hierarchical_search"]["quint_candidates_attempted"] == 0
    assert all(
        formula["condition_count"] <= 4
        for formula in four_only_long["formulas"]
    )

    print("research formula engine self-test: PASS")


if __name__ == "__main__":
    run()
