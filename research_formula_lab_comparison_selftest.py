"""Deterministic regressions for the read-only same-anchor Formula Lab."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json

import ai_tools
import research_evidence_contract as evidence_contract
import research_formula_engine
import research_formula_families
import research_formula_lab_comparison as comparison
import research_formula_store


START = datetime(2026, 8, 30, 0, 0, tzinfo=timezone.utc)


def _family_metadata(conditions: list[dict], exceptions=()) -> dict:
    policy = research_formula_families.condition_family_policy(
        conditions,
        justified_exceptions=exceptions,
        enforce_correlated_families=True,
    )
    return {
        "condition_family_policy": {
            "policy_version": (
                research_formula_families.CONDITION_FAMILY_POLICY_VERSION
            ),
            "enforcement": "ALL_CONDITION_DEPTHS",
            "families": list(policy["families"]),
            "justified_exceptions": list(exceptions),
        }
    }


def _formula(*, current: bool, key: str, feature: str) -> dict:
    conditions = [{"feature": feature, "operator": ">=", "value": 1.0}]
    formula_key = (
        research_formula_engine.formula_key(
            direction="LONG",
            horizon_minutes=720,
            feature_schema_version=(
                evidence_contract.CURRENT_FEATURE_SCHEMA_VERSION
            ),
            conditions=conditions,
        )
        if current
        else key * 64
    )
    formula = {
        "formula_id": 101 if current else 202,
        "formula_key": formula_key,
        "formula_version": 1,
        "formula_schema_version": (
            research_formula_engine.FORMULA_SCHEMA_VERSION
            if current
            else research_formula_engine.LEGACY_V6_FORMULA_SCHEMA_VERSION
        ),
        "engine_version": (
            research_formula_engine.ENGINE_VERSION
            if current
            else research_formula_engine.LEGACY_V6_ENGINE_VERSION
        ),
        "feature_schema_version": (
            evidence_contract.CURRENT_FEATURE_SCHEMA_VERSION
        ),
        "outcome_method_version": (
            evidence_contract.CURRENT_OUTCOME_METHOD_VERSION
        ),
        "direction": "LONG",
        "horizon_minutes": 720,
        "conditions": conditions,
        "current_stage": "HOLDOUT_PASSED" if current else "SHADOW",
        "ranking_score": 9.0 if current else 8.0,
        "holdout_metrics": {"sample_size": 10},
    }
    if current:
        formula["multiple_testing"] = _family_metadata(conditions)
    return formula


def _anchor(
    index: int,
    *,
    minutes: int,
    price: float,
    legacy: float,
    symbol: str | None = None,
) -> dict:
    return {
        "authoritative_verified": True,
        "prospective_anchor_slot_id": index,
        "frozen_decision_features": {
            "signal.current": 1.0,
            "signal.legacy": legacy,
        },
        "max_pain_features": {
            "evaluation_status": "UNEVALUABLE",
            "evaluation_reason": "selftest unavailable at decision time",
        },
        "prospective_evidence": {
            "source_provenance": {
                "official_price": {"source": "fixture"},
                "price_oi": {"source": "fixture"},
                "futures_cvd": {"source": "fixture"},
                "spot_cvd": {"source": "fixture"},
            }
        },
        "event": {
            "event_id": 1000 + index,
            "alert_time_utc": START + timedelta(minutes=minutes),
            "symbol": symbol or ("BTC" if index % 2 else "ETH"),
            "direction": "LONG",
            "current_price": price,
            "prospective_anchor_slot_id": index,
        },
    }


def _snapshot(
    *,
    current: bool,
    path: str | None,
    key: str,
    family: str | None,
    horizon_minutes: int = 720,
    retained_v7_1: bool = False,
) -> dict:
    schema = (
        evidence_contract.CURRENT_FORMULA_SCHEMA_VERSION
        if current
        else evidence_contract.LEGACY_V6_FORMULA_SCHEMA_VERSION
    )
    engine = (
        (
            evidence_contract.RETAINED_V7_1_ENGINE_VERSION
            if retained_v7_1
            else evidence_contract.CURRENT_ENGINE_VERSION
        )
        if current
        else evidence_contract.LEGACY_V6_ENGINE_VERSION
    )
    acceptance_payload = (
        {
            "policy_version": "research-acceptance-v1-probability-or-asymmetry",
            "research_ready": True,
            "accepted_paths": [path],
            "early_current_paths": [path],
            "maturity": "RESEARCH_READY",
            "missing_by_path": {
                "COMMON": [],
                "PROBABILITY": [] if path == "PROBABILITY" else ["fixture"],
                "ASYMMETRY": [] if path == "ASYMMETRY" else ["fixture"],
            },
            "live_effect": "NONE; Formula Lab fixture",
        }
        if current
        else {
            "policy_version": "research-acceptance-v1-probability-or-asymmetry",
            "research_ready": False,
            "accepted_paths": [],
            "early_current_paths": [],
            "maturity": "ACCUMULATING_EVIDENCE",
            "missing_by_path": {
                "COMMON": ["current V7.2 contract"],
                "PROBABILITY": ["legacy read-only"],
                "ASYMMETRY": ["legacy read-only"],
            },
            "live_effect": "NONE; retained legacy Shadow read-only",
        }
    )
    acceptance = evidence_contract.FormulaAssessment.from_acceptance(
        acceptance_payload,
        phase="PROSPECTIVE",
    )
    return evidence_contract.EvidenceSnapshot.build(
        formula_contract={
            "formula_key": key * 64,
            "formula_version": 1,
            "formula_schema_version": schema,
            "engine_version": engine,
            "feature_schema_version": (
                evidence_contract.CURRENT_FEATURE_SCHEMA_VERSION
            ),
            "outcome_method_version": (
                evidence_contract.CURRENT_OUTCOME_METHOD_VERSION
            ),
            "direction": "LONG",
            "horizon_minutes": horizon_minutes,
        },
        assessment=acceptance,
        assessed_at_utc=START + timedelta(days=2),
        formula_family_id=family,
        matched_market_episode_ids=["a" * 64],
        control_market_episode_ids=["b" * 64],
        matched_parent_market_episode_ids=["c" * 64],
        control_parent_market_episode_ids=["d" * 64],
        raw_match_count=3,
        raw_control_count=4,
        matched_n_eff=1.0,
        control_n_eff=1.0,
        metrics={"sample_size": 1},
        evidence={"symbol": "BTC"},
        provenance={"source": "formula-lab-selftest"},
    ).to_dict()


def _raises(expected: str, function) -> None:
    try:
        function()
    except ValueError as exc:
        assert expected in str(exc), exc
    else:
        raise AssertionError(f"expected ValueError containing {expected!r}")


class _Rows:
    def __init__(self, rows):
        self.rows = list(rows)

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None


class _ReadOnlyLabConnection:
    def __init__(self, *, current, legacy, anchors, snapshots):
        self.current = current
        self.legacy = legacy
        self.anchors = anchors
        self.snapshots = snapshots
        self.queries = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query, params=()):
        normalized = " ".join(str(query).split())
        assert normalized.upper().startswith("SELECT"), normalized
        assert query.count("%s") == len(params)
        self.queries.append((normalized, params))
        if "FROM research_formulas f" in normalized:
            formula = (
                self.current
                if params[0] == research_formula_engine.FORMULA_SCHEMA_VERSION
                else self.legacy
            )
            return _Rows([formula])
        if "FROM research_prospective_shadow_events candidate" in normalized:
            return _Rows(
                [
                    {
                        "event_id": row["event"]["event_id"],
                        "alert_time_utc": row["event"]["alert_time_utc"],
                        "symbol": row["event"]["symbol"],
                        "direction": row["event"]["direction"],
                        "event_type": "PROSPECTIVE_NEUTRAL_30M",
                        "setup_key": "lab-selftest",
                        "source_side": "RAW_NEUTRAL",
                        "timeframe": "30m",
                        "strategy_version": "formula-prospective-neutral-v4",
                        "code_version": "selftest",
                        "current_price": row["event"]["current_price"],
                        "anchor_slot_id": row["prospective_anchor_slot_id"],
                        "input_fingerprint": "a" * 64,
                        "feature_bundle_policy_version": (
                            research_formula_store._FEATURE_BUNDLE_POLICY_VERSION
                        ),
                        "feature_bundle_sha256": "b" * 64,
                        "source_timestamps": {},
                        "source_provenance": {},
                    }
                    for row in self.anchors
                ]
            )
        if "FROM research_formula_evidence_snapshots snapshot" in normalized:
            return _Rows(
                [{"snapshot_payload": snapshot} for snapshot in self.snapshots]
            )
        if "FROM research_prospective_anchor_attempts" in normalized:
            if "GROUP BY evaluation_status" in normalized:
                return _Rows(
                    [
                        {
                            "evaluation_status": "COVERAGE_EXCLUDED",
                            "count": 4,
                            "latest_checked_at_utc": START,
                        }
                    ]
                )
            return _Rows(
                [
                    {
                        "evaluation_status": "COVERAGE_EXCLUDED",
                        "evaluation_reason": "coverage gates not met",
                        "missing_sources": [],
                        "checked_at_utc": START,
                        "source_provenance": {},
                    }
                ]
            )
        if "FROM research_prospective_anchor_slots" in normalized:
            return _Rows([{"count": 0}])
        raise AssertionError(f"unexpected Formula Lab SQL: {normalized}")


def run() -> None:
    current = _formula(current=True, key="1", feature="signal.current")
    legacy = _formula(current=False, key="2", feature="signal.legacy")
    anchors = [
        _anchor(1, minutes=0, price=100.0, legacy=1.0),
        _anchor(2, minutes=30, price=101.0, legacy=1.0),
        _anchor(3, minutes=60, price=102.0, legacy=0.0),
        # The fixed 24h episode ended and LONG price reset below its start.
        _anchor(4, minutes=1500, price=99.0, legacy=1.0, symbol="BTC"),
    ]
    snapshots = [
        _snapshot(
            current=True,
            path="PROBABILITY",
            key="3",
            family="e" * 64,
        ),
        _snapshot(
            current=True,
            path="ASYMMETRY",
            key="4",
            family="f" * 64,
        ),
        _snapshot(
            current=False,
            path=None,
            key="5",
            family=None,
        ),
    ]
    ready = comparison.compare_same_anchors(
        current_formulas=[current],
        legacy_formulas=[legacy],
        anchor_rows=anchors,
        evidence_snapshots=snapshots,
        hype_status={
            "symbol": "HYPE",
            "separate_from_other_symbols": True,
            "blocks_other_symbols": False,
            "anchor_slot_count": 0,
            "attempt_counts": {"COVERAGE_EXCLUDED": 4},
        },
        direction="LONG",
        horizon_minutes=720,
    )
    assert ready["status"] == "READY", ready["blockers"]
    assert ready["mode"] == "LAB_REPLAY_SHADOW_READ_ONLY"
    assert ready["same_anchor_contract"]["anchor_count"] == 4
    assert ready["same_anchor_contract"]["input_shared_by_both_cohorts"] is True
    provenance = ready["same_anchor_contract"]["decision_time_provenance"]
    assert provenance["source_provenance_rows"] == 4
    assert provenance["max_pain_status_counts"] == {"UNEVALUABLE": 4}
    assert provenance["later_snapshot_lookup"] is False
    assert provenance["runtime_evidence_mixed"] is False
    current_result = ready["current_v7_2"]["formulas"][0]
    legacy_result = ready["legacy_v6_2"]["formulas"][0]
    assert current_result["raw_match_count"] == 4
    assert current_result["independent_market_episode_count"] == 2
    assert legacy_result["raw_match_count"] == 3
    assert legacy_result["independent_market_episode_count"] == 2
    assert current_result["sample_inflation_prevented"] is True
    assert legacy_result["sample_inflation_prevented"] is True
    assert ready["cross_cohort_overlap"]["positive_overlap_pairs"] == 1
    assert ready["telegram_dry_run"]["both_acceptance_paths_exercised"] is True
    assert (
        ready["telegram_dry_run"]["both_runtime_compatibilities_rendered"]
        is True
    )
    renderer = ready["telegram_dry_run"]["renderer"]
    assert renderer["mode"] == "DRY_RUN"
    assert renderer["delivery_attempts"] == 0
    assert renderer["delivery_channel"] == "NONE"
    assert renderer["live_effect"] == "NONE"

    retained_dry_run = comparison._dry_run_summary(
        [
            _snapshot(
                current=True,
                path="PROBABILITY",
                key="8",
                family="8" * 64,
                retained_v7_1=True,
            ),
            _snapshot(
                current=True,
                path="ASYMMETRY",
                key="9",
                family="9" * 64,
                retained_v7_1=True,
            ),
            snapshots[2],
        ],
        direction="LONG",
        horizon_minutes=720,
    )
    assert retained_dry_run["both_acceptance_paths_exercised"] is False
    assert (
        retained_dry_run["both_runtime_compatibilities_rendered"] is False
    )
    assert evidence_contract.RETAINED_V7_1_READ_ONLY in (
        retained_dry_run["accepted_path_counts_by_compatibility"]
    )
    assert ready["hype_isolation"]["blocks_other_symbols"] is False
    assert ready["safety"] == {
        **ready["safety"],
        "reads_outcomes": False,
        "database_writes": False,
        "delivery_attempts": 0,
        "delivery_channel": "NONE",
        "live_effect": "NONE",
    }

    repeated_price_conditions = [
        {
            "feature": "aligned.60m.price_change_pct",
            "operator": ">=",
            "value": 1.0,
        },
        {
            "feature": (
                "historical.60m.price_change_pct_percentile_session_matched"
            ),
            "operator": ">=",
            "value": 80.0,
        },
    ]
    correlated = {
        **current,
        "formula_key": research_formula_engine.formula_key(
            direction="LONG",
            horizon_minutes=720,
            feature_schema_version=(
                evidence_contract.CURRENT_FEATURE_SCHEMA_VERSION
            ),
            conditions=repeated_price_conditions,
        ),
        "conditions": repeated_price_conditions,
        "multiple_testing": _family_metadata(repeated_price_conditions),
    }
    correlated_result = comparison.compare_same_anchors(
        current_formulas=[correlated],
        legacy_formulas=[legacy],
        anchor_rows=anchors,
        evidence_snapshots=snapshots,
        direction="LONG",
        horizon_minutes=720,
    )
    assert correlated_result["status"] == "WAITING_DATA"
    assert correlated["formula_key"] in correlated_result["current_v7_2"][
        "invalid_condition_family_formula_keys"
    ]
    assert (
        "current cohort contains an invalid correlated condition family"
        in correlated_result["blockers"]
    )

    exception = (
        "price: independent multi-window confirmation retained for audit"
    )
    excepted = {
        **correlated,
        "formula_key": research_formula_engine.formula_key(
            direction="LONG",
            horizon_minutes=720,
            feature_schema_version=(
                evidence_contract.CURRENT_FEATURE_SCHEMA_VERSION
            ),
            conditions=repeated_price_conditions,
            condition_family_exceptions=(exception,),
        ),
        "multiple_testing": _family_metadata(
            repeated_price_conditions, (exception,)
        ),
    }
    excepted_summary = comparison._formula_summary(excepted, anchors)
    assert excepted_summary["condition_family_policy"]["valid"] is True
    stale_exception_key = {
        **excepted,
        "formula_key": correlated["formula_key"],
    }
    stale_exception_summary = comparison._formula_summary(
        stale_exception_key, anchors
    )
    assert stale_exception_summary["condition_family_policy"]["valid"] is False
    assert "formula_key does not bind" in " ".join(
        stale_exception_summary["condition_family_policy"]["reasons"]
    )

    waiting = comparison.compare_same_anchors(
        current_formulas=[],
        legacy_formulas=[legacy],
        anchor_rows=[
            {
                **row,
                "frozen_decision_features": {"signal.current": 1.0},
            }
            for row in anchors
        ],
        evidence_snapshots=[snapshots[0]],
        direction="LONG",
        horizon_minutes=720,
    )
    assert waiting["status"] == "WAITING_DATA"
    assert "exact current V7.2 formula cohort is unavailable" in waiting["blockers"]
    assert (
        "legacy V6.2 cohort has no evaluable same-anchor rows"
        in waiting["blockers"]
    )
    assert any("both PROBABILITY and ASYMMETRY" in item for item in waiting["blockers"])
    assert any("both current V7.2 and legacy V6.2" in item for item in waiting["blockers"])

    drifted = {**current, "engine_version": "formula-discovery-v7-old"}
    _raises(
        "another runtime",
        lambda: comparison.compare_same_anchors(
            current_formulas=[drifted],
            legacy_formulas=[legacy],
            anchor_rows=anchors,
            evidence_snapshots=snapshots,
            direction="LONG",
            horizon_minutes=720,
        ),
    )
    for runtime_field, stale_value in (
        ("formula_version", 2),
        ("feature_schema_version", "stale-feature-schema"),
        ("outcome_method_version", "stale-outcome-method"),
    ):
        stale_contract = {**current, runtime_field: stale_value}
        _raises(
            "another runtime",
            lambda stale_contract=stale_contract: comparison.compare_same_anchors(
                current_formulas=[stale_contract],
                legacy_formulas=[legacy],
                anchor_rows=anchors,
                evidence_snapshots=snapshots,
                direction="LONG",
                horizon_minutes=720,
            ),
        )
    _raises(
        "duplicate anchors or events",
        lambda: comparison.compare_same_anchors(
            current_formulas=[current],
            legacy_formulas=[legacy],
            anchor_rows=[anchors[0], anchors[0]],
            evidence_snapshots=snapshots,
            direction="LONG",
            horizon_minutes=720,
        ),
    )
    wrong_horizon = _snapshot(
        current=True,
        path="PROBABILITY",
        key="6",
        family="9" * 64,
        horizon_minutes=240,
    )
    _raises(
        "direction/horizon",
        lambda: comparison.compare_same_anchors(
            current_formulas=[current],
            legacy_formulas=[legacy],
            anchor_rows=anchors,
            evidence_snapshots=[wrong_horizon, snapshots[1]],
            direction="LONG",
            horizon_minutes=720,
        ),
    )

    tool_specs = {spec["name"]: spec for spec in ai_tools.TOOL_SPECS}
    assert "research_formula_lab_comparison" in tool_specs
    assert "research_formula_lab_comparison" in ai_tools.tool_names()
    assert (
        ai_tools._EXECUTORS["research_formula_lab_comparison"]
        is ai_tools._research_formula_lab_comparison
    )
    spec_text = str(tool_specs["research_formula_lab_comparison"])
    assert "read-only" in spec_text
    assert "never reads outcomes" in spec_text
    assert "never" in spec_text and "sends Telegram" in spec_text

    # The storage entry point is public but retains bounded defaults.
    defaults = research_formula_store.formula_lab_comparison.__kwdefaults__
    assert defaults == {
        "direction": "LONG",
        "horizon_minutes": 720,
        "max_formulas_per_cohort": 20,
        "max_anchors": 250,
        "max_snapshots": 20,
    }

    connection = _ReadOnlyLabConnection(
        current=current,
        legacy=legacy,
        anchors=anchors,
        snapshots=snapshots,
    )
    original_schema_status = research_formula_store.schema_status
    original_connect = research_formula_store._connect
    original_feature_loader = (
        research_formula_store.research_feature_matrix.load_shadow_feature_rows_by_horizon
    )
    research_formula_store.schema_status = lambda: {"schema_present": True}
    research_formula_store._connect = lambda *, read_only: (
        connection if read_only is True else (_ for _ in ()).throw(
            AssertionError("Formula Lab attempted a write connection")
        )
    )
    research_formula_store.research_feature_matrix.load_shadow_feature_rows_by_horizon = (
        lambda requested: {
            (row["event"]["event_id"], 720): row
            for row in anchors
            if row["event"]["event_id"] in requested[720]
        }
    )
    try:
        stored = research_formula_store.formula_lab_comparison(
            direction="LONG",
            horizon_minutes=720,
            max_formulas_per_cohort=5,
            max_anchors=4,
            max_snapshots=5,
        )
    finally:
        research_formula_store.schema_status = original_schema_status
        research_formula_store._connect = original_connect
        research_formula_store.research_feature_matrix.load_shadow_feature_rows_by_horizon = (
            original_feature_loader
        )
    assert stored["available"] is True
    assert stored["status"] == "READY", stored["blockers"]
    assert stored["data_watermark"]["loaded_event_count"] == 4
    assert stored["data_watermark"]["authoritative_verified_anchor_count"] == 4
    assert stored["hype_isolation"]["blocks_other_symbols"] is False
    assert len(connection.queries) == 7

    # Maximum public tool bounds remain deterministic and compact.  The raw
    # match sets are fingerprinted and sampled rather than repeated 50 times.
    stress_anchors = [
        _anchor(
            index,
            minutes=(index - 1) * 30,
            price=100.0 + index / 100.0,
            legacy=1.0,
        )
        for index in range(1, 251)
    ]
    stress_current = []
    stress_legacy = []
    for index in range(25):
        current_formula = dict(current)
        current_formula["formula_id"] = 10000 + index
        current_conditions = [
            {
                "feature": "signal.current",
                "operator": ">=",
                "value": 0.5 + index / 1000.0,
            }
        ]
        current_formula["conditions"] = current_conditions
        current_formula["multiple_testing"] = _family_metadata(
            current_conditions
        )
        current_formula["formula_key"] = research_formula_engine.formula_key(
            direction="LONG",
            horizon_minutes=720,
            feature_schema_version=(
                evidence_contract.CURRENT_FEATURE_SCHEMA_VERSION
            ),
            conditions=current_conditions,
        )
        legacy_formula = dict(legacy)
        legacy_formula["formula_id"] = 20000 + index
        legacy_formula["formula_key"] = hashlib.sha256(
            f"legacy-{index}".encode("utf-8")
        ).hexdigest()
        stress_current.append(current_formula)
        stress_legacy.append(legacy_formula)
    stress = comparison.compare_same_anchors(
        current_formulas=stress_current,
        legacy_formulas=stress_legacy,
        anchor_rows=stress_anchors,
        evidence_snapshots=snapshots,
        direction="LONG",
        horizon_minutes=720,
    )
    assert stress["status"] == "READY"
    assert stress["cross_cohort_overlap"]["pair_count"] == 625
    assert stress["cross_cohort_overlap"]["details_truncated"] == 615
    assert len(json.dumps(stress, default=str, ensure_ascii=False)) < 55000


if __name__ == "__main__":
    run()
    print("research_formula_lab_comparison_selftest: OK")
