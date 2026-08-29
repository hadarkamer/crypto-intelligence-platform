"""Deterministic checks for prospective Formula Shadow evidence handling."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import inspect
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
        "direction": "LONG",
        "event_type": "SELFTEST_ALERT",
        "evaluation_status": status,
        "first_touch_available": True,
        "first_touch_hit": True,
        "full_horizon_outcome_available": True,
        "outcome_available": True,
        "directional_return_pct": 1.0,
        "path_success": True,
        "first_touch_status": "HIT",
        "mfe_pct": 2.0,
        "mae_pct": 0.25,
        "full_horizon_mae_pct": 7.5,
        "time_to_first_progress_seconds": 60,
        "time_to_first_qualifying_move_seconds": 60,
        "qualifying_move_threshold_pct": 0.5,
        "qualifying_candle_order_ambiguous": False,
        "time_to_mfe_seconds": 600,
        "target_progress_ratio": 1.0,
        "target_reached": True,
    }


def _max_pain_snapshot(*, decision_time: datetime, tampered_hash: bool = False) -> dict:
    archive = store.research_max_pain_archive
    available = decision_time - timedelta(minutes=5)
    record = {
        "snapshot_set_id": int(decision_time.timestamp()),
        "snapshot_key": "1" * 64,
        "set_payload_sha256": "2" * 64,
        "symbol": "BTC",
        "symbol_manifest_payload_sha256": "3" * 64,
        "row_payload_sha256": [
            {"timeframe": timeframe, "payload_sha256": "4" * 63 + suffix}
            for timeframe, suffix in zip(
                archive.REQUIRED_TIMEFRAMES, "56789ab"
            )
        ],
        "archive_schema_version": archive.ARCHIVE_SCHEMA_VERSION,
        "method_version": archive.METHOD_VERSION,
        "cutover_marker": archive.CUTOVER_MARKER,
        "cutover_time_utc": archive.CUTOVER_TIME_UTC.isoformat(),
        "available_at_utc": available.isoformat(),
        "cycle_id": f"selftest:{int(decision_time.timestamp())}",
        "cycle_time_utc": (available - timedelta(minutes=5)).isoformat(),
        "source": "WATCH_SHARED",
        "collector_version": "selftest-v1",
    }
    provenance = {
        "policy_version": archive.SHADOW_PROVENANCE_POLICY_VERSION,
        "symbol": "BTC",
        "current": record,
        "previous": None,
        "used_for_delta": False,
        "previous_gap_minutes": None,
        "previous_gap_policy_minutes": archive.DEFAULT_MAX_PREVIOUS_GAP_MINUTES,
    }
    provenance_hash = archive.canonical_provenance_sha256(provenance)
    return {
        "snapshot_policy_version": store._SHADOW_INPUT_SNAPSHOT_POLICY_VERSION,
        "decision_cohort_policy_version": store._DECISION_COHORT_POLICY_VERSION,
        "max_pain_provenance": {
            "condition_features": [
                "max_pain.aggregate.short_long_liquidity_ratio"
            ],
            "requires_previous": False,
            "evaluation_status": "EVALUABLE",
            "evaluation_reason": "current snapshot is coherent",
            "change_evaluation_status": "UNEVALUABLE",
            "change_reason": "previous snapshot is not required",
            "provenance": provenance,
            "provenance_sha256": "0" * 64 if tampered_hash else provenance_hash,
        },
    }


def _bind_max_pain_check(
    formula: dict, row: dict, snapshot: dict
) -> dict:
    frozen = {
        **snapshot,
        "formula_key": formula["formula_key"],
        "formula_version": formula["formula_version"],
        "horizon_minutes": formula["horizon_minutes"],
        "feature_schema_version": formula["feature_schema_version"],
        "event": {
            "event_id": row["event_id"],
            "alert_time_utc": row["alert_time_utc"],
            "symbol": row["symbol"],
            "direction": row["direction"],
            "event_type": row["event_type"],
        },
        "formula_key_features": {
            "max_pain.aggregate.short_long_liquidity_ratio": 2.0
        },
        "source_inputs": {
            family: {
                "timestamp_utc": (
                    row["alert_time_utc"] - timedelta(minutes=10)
                ).isoformat()
            }
            for family in ("price_oi", "futures_cvd", "spot_cvd")
        },
    }
    cohort_key, cohort_anchor = store._decision_cohort_identity(
        formula=formula,
        event=row,
        snapshot=frozen,
    )
    return {
        **row,
        "input_snapshot": frozen,
        "decision_cohort_key": cohort_key,
        "decision_anchor_time_utc": cohort_anchor,
    }


def run() -> None:
    rebound_policy = store._bind_max_pain_policy("legacy-render-override")
    for max_pain_policy_binding in (
        store.research_max_pain_archive.SHADOW_PROVENANCE_POLICY_VERSION,
        store._DECISION_COHORT_POLICY_VERSION,
    ):
        assert max_pain_policy_binding in store._SHADOW_MONITORING_POLICY_VERSION
        assert max_pain_policy_binding in rebound_policy
    compatible_shadow_schemas = set(
        store._SHADOW_COMPATIBLE_FORMULA_SCHEMAS
    )
    assert compatible_shadow_schemas == {
        "research-formula-v5-safe-replay",
        store.research_formula_engine.FORMULA_SCHEMA_VERSION,
    }
    persist_source = " ".join(
        inspect.getsource(store.persist_discovery_run).split()
    )
    assert (
        "current_stage NOT IN ('SHADOW', 'APPROVED', 'LIVE', 'RETIRED')"
        in persist_source
    )

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

    # Formula schema v6 changes how outcomes are evaluated, but it must not
    # make the 19 already-active v5 Shadow formulas disappear. LIVE work,
    # unlike Shadow monitoring, remains restricted to the current schema.
    class _Rows:
        def __init__(self, rows):
            self._rows = list(rows)

        def fetchall(self):
            return self._rows

    class _ShadowConnection:
        def __init__(self):
            self.queries = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def execute(self, query, params=()):
            assert query.count("%s") == len(params)
            normalized = " ".join(query.split())
            dense = normalized.replace(" ", "")
            self.queries.append(normalized)
            if "FROM research_formulas" in normalized:
                assert "f.current_stage='SHADOW'" in normalized
                assert "f.formula_schema_version=ANY(%s)" in dense
                assert "f.current_stage='LIVE'" in normalized
                assert set(params[0]) == compatible_shadow_schemas
                assert params[1] == store.research_formula_engine.FORMULA_SCHEMA_VERSION
                return _Rows(
                    [
                        {
                            "formula_id": 3153,
                            "formula_key": "legacy-v5-shadow",
                            "formula_version": 1,
                            "formula_text": "legacy formula",
                            "formula_schema_version": "research-formula-v5-safe-replay",
                            "direction": "SHORT",
                            "horizon_minutes": 720,
                            "conditions": [],
                            "feature_schema_version": "research-feature-matrix-v5",
                            "last_shadow_event_id": 0,
                            "shadow_started_at_utc": start,
                            "current_stage": "SHADOW",
                            "live_alert_approved": False,
                            "ranking_score": 1.0,
                            "holdout_metrics": {},
                        }
                    ]
                )
            assert "FROM research_events candidate" in normalized
            assert "research_prospective_shadow_events" in normalized
            return _Rows(
                [
                    {
                        "event_id": 9001,
                        "alert_time_utc": start + timedelta(hours=1),
                        "symbol": "BTC",
                        "direction": "SHORT",
                        "event_type": "SELFTEST_ALERT",
                        "setup_key": "selftest",
                        "event_kind": "ALERT",
                        "delivery_status": "DELIVERED",
                    }
                ]
            )

    shadow_connection = _ShadowConnection()
    original_connect = store._connect
    store._connect = lambda *, read_only: shadow_connection
    try:
        legacy_work = store.load_shadow_work(max_events_per_formula=5)
    finally:
        store._connect = original_connect
    assert len(legacy_work) == 1
    assert legacy_work[0]["formula_schema_version"].startswith(
        "research-formula-v5"
    )
    assert legacy_work[0]["events"][0]["event_id"] == 9001

    class _ReadinessConnection:
        def __init__(self):
            self.updated = False

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def execute(self, query, params=()):
            assert query.count("%s") == len(params)
            normalized = " ".join(query.split())
            dense = normalized.replace(" ", "")
            if "FROM research_formulas" in normalized:
                assert "current_stage='SHADOW'" in normalized
                assert "formula_schema_version=ANY(%s)" in dense
                assert "formula_schema_version" in normalized
                assert "formula_key" in normalized
                assert "feature_schema_version" in normalized
                assert "conditions" in normalized
                assert set(params[0]) == compatible_shadow_schemas
                return _Rows(
                    [
                        {
                            "formula_id": 3153,
                            "formula_key": "legacy-v5-shadow",
                            "formula_version": 1,
                            "formula_schema_version": (
                                "research-formula-v5-safe-replay"
                            ),
                            "feature_schema_version": (
                                "research-feature-matrix-v5"
                            ),
                            "conditions": [],
                            "horizon_minutes": 720,
                            "latest_evaluation_run_id": 29,
                            "shadow_started_at_utc": start,
                            "last_shadow_event_id": 0,
                        }
                    ]
                )
            if "FROM research_formula_shadow_checks" in normalized:
                assert "research_first_touch_outcomes" in normalized
                assert "ft.success AS path_success" in normalized
                assert "AS first_touch_available" in normalized
                assert "AS first_touch_hit" in normalized
                assert "AS full_horizon_outcome_available" in normalized
                assert "ft.pre_qualifying_mae_pct AS mae_pct" in normalized
                assert "first_touch_threshold_scale_factor" in normalized
                assert "first_touch_threshold_source_kind" in normalized
                assert "ft.status IN ('HIT', 'MISS')" in normalized
                assert "ft.method_version=%s" in normalized
                assert "ft.data_quality_status=ANY(%s)" in normalized
                assert store.research_feature_matrix.VERIFIED_OUTCOME_METHOD in params
                assert list(store.research_feature_matrix.VERIFIED_OUTCOME_QUALITIES) in params
                return _Rows([])
            if "UPDATE research_formulas" in normalized:
                self.updated = True
                return _Rows([])
            raise AssertionError(f"unexpected readiness query: {normalized}")

        def commit(self):
            return None

    readiness_connection = _ReadinessConnection()
    store._connect = lambda *, read_only: readiness_connection
    try:
        readiness = store.evaluate_shadow_readiness()
    finally:
        store._connect = original_connect
    assert readiness["evaluated"] == 1
    assert readiness_connection.updated is True

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
    assert label["path_success"] is True
    assert label["first_touch_status"] == "HIT"
    assert label["full_horizon_mae_pct"] == 7.5

    # A verified early touch is visible immediately but cannot enter the
    # readiness sample until the separate full-horizon diagnostic is present.
    early_only = {
        **_shadow_row(21, at=start + timedelta(hours=4)),
        "outcome_available": False,
        "full_horizon_outcome_available": False,
        "outcome_due": False,
    }
    early_validation = store._build_shadow_validation(
        {"horizon_minutes": 240}, [early_only], evaluated_at_utc=start
    )
    assert early_validation["metrics"]["sample_size"] == 0
    assert early_validation["evidence"]["early_first_touch"][
        "matched_hit_event_ids"
    ] == [21]
    assert early_validation["evidence"]["pending_outcome_event_ids"] == [21]

    unlabeled_source = {
        **source,
        "directional_return_pct": 99.0,
        "path_success": None,
        "first_touch_status": None,
    }
    unlabeled = store._metric_row(
        unlabeled_source, horizon_minutes=240
    )["outcome_label"]
    assert unlabeled["path_success"] is None
    assert unlabeled["first_touch_status"] is None

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

    relaxed_snapshot = {
        "movement_width_reference": {
            "floor_scale_factor": 0.60,
            "threshold_scale_factor": 0.60,
            "session_weekend_ratio": 1.0,
            "applied": True,
        }
    }
    compatible, reason = store._terminal_threshold_matches_snapshot(
        {
            "first_touch_available": True,
            "input_snapshot": relaxed_snapshot,
            "first_touch_threshold_scale_factor": 0.60,
            "first_touch_threshold_source_kind": (
                "PRIOR_ONLY_SESSION_CALIBRATION"
            ),
            "qualifying_move_threshold_pct": 0.60,
        },
        horizon_minutes=240,
    )
    assert compatible is True and "matches" in reason

    mismatched_terminal = {
        **_shadow_row(30, at=start + timedelta(hours=8)),
        "input_snapshot": relaxed_snapshot,
        "outcome_due": True,
        "first_touch_threshold_scale_factor": 1.0,
        "first_touch_threshold_source_kind": "STATIC_HORIZON_FLOOR",
        "qualifying_move_threshold_pct": 1.0,
    }

    class _TerminalConnection:
        def execute(self, query, params=()):
            assert query.count("%s") == len(params)
            assert "first_touch_threshold_scale_factor" in query
            return _Rows([mismatched_terminal])

    guarded_rows = store._shadow_outcome_rows(
        _TerminalConnection(), {"formula_id": 1, "horizon_minutes": 240}
    )
    guarded = guarded_rows[0]
    assert guarded["first_touch_threshold_policy_compatible"] is False
    assert guarded["first_touch_available"] is False
    assert guarded["outcome_available"] is False
    guarded_validation = store._build_shadow_validation(
        {"horizon_minutes": 240}, guarded_rows, evaluated_at_utc=start
    )
    assert guarded_validation["metrics"]["sample_size"] == 0
    assert guarded_validation["evidence"][
        "threshold_policy_mismatch_event_ids"
    ] == [30]

    # v6 Max-Pain proof/cohort is checked before independence selection. A
    # malformed historical check remains listed for audit but cannot overlap-exclude, or
    # permanently poison, sufficient later valid evidence.
    max_pain_formula = {
        "formula_id": 42,
        "formula_key": "f" * 64,
        "formula_version": 1,
        "formula_schema_version": store.research_formula_engine.FORMULA_SCHEMA_VERSION,
        "feature_schema_version": store.research_feature_matrix.FEATURE_SCHEMA_VERSION,
        "direction": "LONG",
        "horizon_minutes": 60,
        "conditions": [
            {
                "feature": "max_pain.aggregate.short_long_liquidity_ratio",
                "operator": ">=",
                "value": 1.0,
            }
        ],
    }
    malformed_at = start + timedelta(minutes=10)
    valid_at = start + timedelta(minutes=40)
    malformed = _bind_max_pain_check(
        max_pain_formula,
        {**_shadow_row(40, at=malformed_at), "outcome_due": True},
        _max_pain_snapshot(decision_time=malformed_at),
    )
    malformed["decision_cohort_key"] = "0" * 64
    valid_max_pain = _bind_max_pain_check(
        max_pain_formula,
        {**_shadow_row(41, at=valid_at), "outcome_due": True},
        _max_pain_snapshot(decision_time=valid_at),
    )
    max_pain_validation = store._build_shadow_validation(
        max_pain_formula,
        [malformed, valid_max_pain],
        evaluated_at_utc=valid_at + timedelta(hours=1),
    )
    max_pain_evidence = max_pain_validation["evidence"][
        "max_pain_provenance"
    ]
    assert max_pain_evidence["required"] is True
    assert max_pain_evidence["incompatible_event_ids"] == [40]
    assert [
        item["event_id"] for item in max_pain_evidence["event_provenance_refs"]
    ] == [41]
    assert max_pain_validation["gates"][
        "complete Max-Pain provenance chain"
    ] is True
    assert max_pain_validation["metrics"]["sample_size"] == 1
    assert max_pain_validation["evidence"]["raw_evaluation_status"] == {
        "MATCHED": 1,
        "UNMATCHED": 0,
        "UNEVALUABLE": 1,
    }
    reordered_validation = store._build_shadow_validation(
        max_pain_formula,
        [valid_max_pain, malformed],
        evaluated_at_utc=valid_at + timedelta(hours=1),
    )
    assert reordered_validation["evidence"]["max_pain_provenance"][
        "canonical_evidence_sha256"
    ] == max_pain_evidence["canonical_evidence_sha256"]
    canonical = store.research_max_pain_archive.canonical_provenance_sha256
    provenance = _max_pain_snapshot(decision_time=valid_at)[
        "max_pain_provenance"
    ]["provenance"]
    assert canonical(provenance) == canonical(
        {key: provenance[key] for key in reversed(list(provenance))}
    )

    print("research formula store self-test: PASS")


if __name__ == "__main__":
    run()
