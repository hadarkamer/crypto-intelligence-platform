"""Deterministic checks for prospective Formula Shadow evidence handling."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import inspect
import json

import market_session_baseline
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
        "decision_cohort_key": f"{event_id:064x}"[-64:],
        "decision_anchor_time_utc": at,
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
        "created_at_utc": (available + timedelta(seconds=5)).isoformat(),
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
            "condition_values": {
                "max_pain.aggregate.short_long_liquidity_ratio": 2.0
            },
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
    conditions = [dict(item) for item in formula.get("conditions") or []]
    feature_values = {
        str(condition["feature"]): 2.0 for condition in conditions
    }
    condition_results = [
        {
            "feature": str(condition["feature"]),
            "operator": str(condition["operator"]),
            "expected": condition.get("value"),
            "actual": feature_values[str(condition["feature"])],
            "available": True,
            "passed": True,
        }
        for condition in conditions
    ]
    frozen = {
        **snapshot,
        "evidence_policy_version": (
            store._PROSPECTIVE_EVIDENCE_POLICY_VERSION
        ),
        "decision_input_policy_version": (
            store._PROSPECTIVE_EVIDENCE_POLICY_VERSION
        ),
        "formula_id": formula["formula_id"],
        "formula_key": formula["formula_key"],
        "formula_version": formula["formula_version"],
        "formula_schema_version": formula.get("formula_schema_version"),
        "engine_version": formula.get("engine_version"),
        "outcome_method_version": formula.get("outcome_method_version"),
        "horizon_minutes": formula["horizon_minutes"],
        "feature_schema_version": formula["feature_schema_version"],
        "event": {
            "event_id": row["event_id"],
            "alert_time_utc": row["alert_time_utc"],
            "symbol": row["symbol"],
            "direction": row["direction"],
            "event_type": row["event_type"],
        },
        "formula_key_features": feature_values,
        "conditions": conditions,
        "condition_results": condition_results,
        "evaluation_status": "MATCHED",
        "evaluation_reason": "all formula conditions passed",
        "source_inputs": {},
        "movement_width_reference": (
            store.research_session_width.movement_width_reference(
                symbol=row["symbol"],
                event_time=row["alert_time_utc"],
                horizon_minutes=formula["horizon_minutes"],
                as_of_utc=row["alert_time_utc"] - timedelta(minutes=10),
                historical_index={},
            )
        ),
    }
    slot = {
        "prospective_anchor_slot_id": 17,
        "prospective_input_fingerprint": "d" * 64,
        "prospective_slot_created_at_utc": row["alert_time_utc"].isoformat(),
    }
    frozen["prospective_evidence"] = {
        "sampler_version": store._PROSPECTIVE_ANCHOR_SAMPLER_VERSION,
        "feature_bundle_policy_version": store._FEATURE_BUNDLE_POLICY_VERSION,
        "anchor_slot_id": 17,
        "input_fingerprint": "d" * 64,
        "feature_bundle_sha256": "b" * 64,
        "source_timestamps": {},
        "source_provenance": {},
    }
    source_time = (row["alert_time_utc"] - timedelta(minutes=10)).isoformat()
    frozen["source_inputs"] = {
        "price_oi": {
            "timestamp_utc": source_time,
            "price_timestamp_utc": source_time,
            "oi_timestamp_utc": source_time,
            "price_exchange": "binance",
            "price_market": "spot",
            "price_pair": "BTCUSDT",
            "price_instrument_id": None,
            "price_source": "binance_spot",
            "source": "binance_spot",
            "price_timeframe": "1m",
            "price_interval_seconds": 60,
            "canonical_price_method_version": (
                store.canonical_price_path.METHOD_VERSION
            ),
            "canonical_price_provenance_version": (
                store.canonical_price_path.PRICE_PROVENANCE_VERSION
            ),
            "canonical_price_provenance": {
                "provenance_version": store.canonical_price_path.PRICE_PROVENANCE_VERSION,
                "method_version": store.canonical_price_path.METHOD_VERSION,
                "symbol": row["symbol"],
                "exchange": "binance",
                "market": "spot",
                "pair": "BTCUSDT",
                "instrument": None,
                "interval": "1m",
                "interval_seconds": 60,
            },
            **slot,
        },
        "futures_cvd": {
            "timestamp_utc": source_time,
            "source": "coinglass_futures_aggregated_cvd",
            "exchange_list": "Binance,OKX,Bybit",
            **slot,
        },
        "spot_cvd": {
            "timestamp_utc": source_time,
            "source": "coinglass_spot_aggregated_cvd",
            "exchange_list": "Binance,OKX,Bybit",
            **slot,
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
        "condition_results": condition_results,
        "matched": True,
        "evaluation_status": "MATCHED",
        "evaluation_reason": "all formula conditions passed",
        "decision_cohort_key": cohort_key,
        "decision_anchor_time_utc": cohort_anchor,
    }


def _authoritative_bundle_fixture(
    check: dict, formula: dict
) -> tuple[dict, dict, dict]:
    """Bind a check to one independently hashed sampler-v4 slot fixture."""
    check = deepcopy(check)
    snapshot = check["input_snapshot"]
    max_pain = snapshot.get("max_pain_provenance")
    decision_time = check["alert_time_utc"]
    features = deepcopy(snapshot["formula_key_features"])
    features.update(
        {
            "event.symbol": check["symbol"],
            "event.event_type": check["event_type"],
        }
    )
    contexts = {}
    for horizon in (60, 240, 720, 1440):
        active, weekend, segments = (
            market_session_baseline.session_ratios(
                decision_time,
                decision_time + timedelta(minutes=horizon),
            )
        )
        contexts[str(horizon)] = {
            "session": {
                "active_ratio": round(active, 6),
                "weekend_ratio": round(weekend, 6),
                "composition": (
                    "ACTIVE_ONLY"
                    if active >= 1.0 - 1e-9
                    else "WEEKEND_ONLY"
                    if active <= 1e-9
                    else "MIXED"
                ),
                "segments": segments,
            },
            "movement_width_reference": (
                store.research_session_width.movement_width_reference(
                    symbol=check["symbol"],
                    event_time=decision_time,
                    horizon_minutes=horizon,
                    as_of_utc=decision_time - timedelta(minutes=10),
                    historical_index={},
                )
            ),
        }
    bundle = {
        "bundle_schema_version": (
            store.research_prospective_feature_freeze.BUNDLE_SCHEMA_VERSION
        ),
        "feature_policy_version": store._FEATURE_BUNDLE_POLICY_VERSION,
        "feature_schema_version": store.research_feature_matrix.FEATURE_SCHEMA_VERSION,
        "decision_time_utc": decision_time.isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z"),
        "symbol": check["symbol"],
        "source_series_manifest": {
            "count": 0,
            "first_decision_time_utc": None,
            "last_decision_time_utc": None,
            "sha256": "0" * 64,
            "sampler_versions": [],
        },
        "features_by_direction": {
            "LONG": deepcopy(features),
            "SHORT": deepcopy(features),
        },
        "horizon_context": contexts,
        "model_score_status": "ABSENT",
    }
    bundle_hash = (
        store.research_prospective_feature_freeze.compute_feature_bundle_sha256(
            bundle
        )
    )
    slot_id = 17
    slot_open = decision_time.replace(minute=0, second=0, microsecond=0)
    slot_close = slot_open + timedelta(minutes=30)
    base_eligible = slot_close + timedelta(minutes=2)
    expires = base_eligible + timedelta(minutes=30)
    frozen_inputs = {}
    authorized = {
        "anchor_slot_id": slot_id,
        "sampler_version": store._PROSPECTIVE_ANCHOR_SAMPLER_VERSION,
        "coverage_policy_version": "selftest-coverage-v1",
        "coverage_snapshot": {},
        "symbol": check["symbol"],
        "source_candle_open_utc": slot_open,
        "source_candle_close_utc": slot_close,
        "base_eligible_at_utc": base_eligible,
        "expires_at_utc": expires,
        "decision_time_utc": decision_time,
        "source_timestamps": {},
        "source_provenance": {},
        "frozen_inputs": frozen_inputs,
        "feature_bundle_policy_version": store._FEATURE_BUNDLE_POLICY_VERSION,
        "feature_bundle_sha256": bundle_hash,
        "decision_feature_bundle": bundle,
    }
    input_fingerprint = store.research_prospective_anchors.compute_input_fingerprint(
        sampler_version=authorized["sampler_version"],
        coverage_policy_version=authorized["coverage_policy_version"],
        coverage_snapshot=authorized["coverage_snapshot"],
        symbol=authorized["symbol"],
        source_candle_open_utc=slot_open,
        source_candle_close_utc=slot_close,
        base_eligible_at_utc=base_eligible,
        expires_at_utc=expires,
        evaluation_status=store.research_prospective_anchors.EVALUABLE,
        decision_time_utc=decision_time,
        source_timestamps={},
        source_provenance={},
        frozen_inputs=frozen_inputs,
        feature_bundle_policy_version=store._FEATURE_BUNDLE_POLICY_VERSION,
        feature_bundle_sha256=bundle_hash,
    )
    authorized["input_fingerprint"] = input_fingerprint
    prospective = {
        "sampler_version": store._PROSPECTIVE_ANCHOR_SAMPLER_VERSION,
        "feature_bundle_policy_version": store._FEATURE_BUNDLE_POLICY_VERSION,
        "anchor_slot_id": slot_id,
        "input_fingerprint": input_fingerprint,
        "feature_bundle_sha256": bundle_hash,
        "source_timestamps": {},
        "source_provenance": {},
    }
    snapshot["prospective_evidence"] = deepcopy(prospective)
    snapshot["outcome_window_session"] = {
        "session_active_ratio": contexts[str(formula["horizon_minutes"])][
            "session"
        ]["active_ratio"],
        "session_weekend_ratio": contexts[str(formula["horizon_minutes"])][
            "session"
        ]["weekend_ratio"],
        "session_segments": contexts[str(formula["horizon_minutes"])][
            "session"
        ]["segments"],
        "session_composition": contexts[str(formula["horizon_minutes"])][
            "session"
        ]["composition"],
    }
    snapshot["movement_width_reference"] = deepcopy(
        contexts[str(formula["horizon_minutes"])]["movement_width_reference"]
    )
    for source in snapshot["source_inputs"].values():
        source["prospective_anchor_slot_id"] = slot_id
        source["prospective_input_fingerprint"] = input_fingerprint
    cohort_key, cohort_anchor = store._decision_cohort_identity(
        formula=formula,
        event=check,
        snapshot=snapshot,
    )
    check["decision_cohort_key"] = cohort_key
    check["decision_anchor_time_utc"] = cohort_anchor
    row = {
        "feature_schema_version": (
            store.research_feature_matrix.FEATURE_SCHEMA_VERSION
        ),
        "decision_input_policy_version": (
            store.research_feature_matrix.PROSPECTIVE_FROZEN_INPUT_POLICY_VERSION
        ),
        "event": deepcopy(snapshot["event"]),
        "frozen_decision_features": deepcopy(features),
        "prospective_evidence": deepcopy(prospective),
        "raw_features": {
            "latest_at_or_before_alert": deepcopy(snapshot["source_inputs"]),
        },
        "outcome_label": {
            **deepcopy(snapshot["outcome_window_session"]),
            "movement_width_reference": deepcopy(
                snapshot["movement_width_reference"]
            ),
        },
    }
    if isinstance(max_pain, dict):
        row["max_pain_features"] = {
            "features": deepcopy(max_pain["condition_values"]),
            "evaluation_status": max_pain["evaluation_status"],
            "reason": max_pain["evaluation_reason"],
            "change_evaluation_status": max_pain[
                "change_evaluation_status"
            ],
            "change_reason": max_pain["change_reason"],
            "provenance": deepcopy(max_pain["provenance"]),
            "provenance_sha256": max_pain["provenance_sha256"],
        }
    return check, row, authorized


def _replacement_contract() -> tuple[dict, dict, list[dict]]:
    symbols = ("BTC", "DOGE", "ETH", "SOL")
    policy = {
        "minimum_anchors_per_symbol": (
            store.research_feature_matrix.REPLAY_MIN_ANCHORS_PER_SYMBOL
        ),
        "minimum_eligible_symbols": (
            store.research_feature_matrix.REPLAY_MIN_ELIGIBLE_SYMBOLS
        ),
        "minimum_utc_dates_per_symbol": (
            store.research_feature_matrix.REPLAY_MIN_UTC_DATES_PER_SYMBOL
        ),
        "minimum_span_hours_per_symbol": (
            store.research_feature_matrix.REPLAY_MIN_SPAN_HOURS_PER_SYMBOL
        ),
        "continuity_gap_is_diagnostic_only": True,
    }
    coverage = {
        "dataset_kind": "historical_raw_opportunity_replay",
        "replacement_ready": True,
        "replay_version": store.research_historical_replay.REPLAY_VERSION,
        "first_touch_method_version": (
            store.research_feature_matrix.VERIFIED_OUTCOME_METHOD
        ),
        "movement_width_calibration_version": (
            store.research_session_width.CALIBRATION_VERSION
        ),
        "canonical_price_provenance_version": (
            store.canonical_price_path.PRICE_PROVENANCE_VERSION
        ),
        "readiness_policy": policy,
        "eligible_symbols": list(symbols),
        "symbols": len(symbols),
        "distinct_utc_dates": 14,
        "span_hours": 336.0,
        "by_symbol": {
            symbol: {
                "anchors": 673,
                "utc_dates": 14,
                "span_hours": 336.0,
                "maximum_anchor_gap_minutes": 30.0,
                "eligible": True,
                "failed_gates": [],
            }
            for symbol in symbols
        },
    }
    dataset = {
        "available": True,
        "coverage": coverage,
        "replay_version": store.research_historical_replay.REPLAY_VERSION,
        "first_touch_method_version": (
            store.research_feature_matrix.VERIFIED_OUTCOME_METHOD
        ),
        "movement_width_calibration_version": (
            store.research_session_width.CALIBRATION_VERSION
        ),
        "canonical_price_provenance_version": (
            store.canonical_price_path.PRICE_PROVENANCE_VERSION
        ),
        "outcome_method_version": (
            store.research_feature_matrix.VERIFIED_OUTCOME_METHOD
        ),
        "feature_schema_version": (
            store.research_feature_matrix.FEATURE_SCHEMA_VERSION
        ),
        "horizon_minutes": 240,
    }
    discovery = {
        "available": True,
        "engine_version": store.research_formula_engine.ENGINE_VERSION,
        "formula_schema_version": (
            store.research_formula_engine.FORMULA_SCHEMA_VERSION
        ),
        "feature_schema_version": (
            store.research_feature_matrix.FEATURE_SCHEMA_VERSION
        ),
        "horizon_minutes": 240,
    }
    formulas = [
        {
            "formula_version": 1,
            "formula_schema_version": (
                store.research_formula_engine.FORMULA_SCHEMA_VERSION
            ),
            "engine_version": store.research_formula_engine.ENGINE_VERSION,
            "feature_schema_version": (
                store.research_feature_matrix.FEATURE_SCHEMA_VERSION
            ),
            "horizon_minutes": 240,
            "recommended_stage": "BACKTESTED",
        }
    ]
    return dataset, discovery, formulas


def run() -> None:
    assert store._type_strict_json_equal({"value": 1.0}, {"value": 1.0})
    assert not store._type_strict_json_equal({"value": True}, {"value": 1.0})

    class _EnforcementRows:
        def __init__(self, *, rows=None, row=None):
            self.rows = list(rows or [])
            self.row = row

        def fetchall(self):
            return self.rows

        def fetchone(self):
            return self.row

    class _EnforcementConnection:
        def __init__(
            self,
            *,
            disabled_trigger=None,
            replica_only_trigger=None,
            wrong_type_trigger=None,
            include_index=True,
            wrong_index_keys=False,
            wrong_index_predicate=False,
        ):
            self.disabled_trigger = disabled_trigger
            self.replica_only_trigger = replica_only_trigger
            self.wrong_type_trigger = wrong_type_trigger
            self.include_index = include_index
            self.wrong_index_keys = wrong_index_keys
            self.wrong_index_predicate = wrong_index_predicate

        def execute(self, query, params=()):
            normalized = " ".join(str(query).split())
            if "FROM pg_trigger" in normalized:
                rows = []
                for name, contract in store._LIVE_APPROVAL_TRIGGER_CONTRACTS.items():
                    rows.append(
                        {
                            "trigger_name": name,
                            "table_name": contract["table"],
                            "enabled_state": (
                                "D"
                                if name == self.disabled_trigger
                                else "R"
                                if name == self.replica_only_trigger
                                else "O"
                            ),
                            "trigger_type": (
                                7
                                if name == self.wrong_type_trigger
                                else contract["trigger_type"]
                            ),
                            "update_columns": list(contract["update_columns"]),
                            "function_name": contract["function"],
                            "function_definition": " ".join(contract["tokens"]),
                        }
                    )
                return _EnforcementRows(rows=rows)
            if "FROM pg_index" in normalized:
                if not self.include_index:
                    return _EnforcementRows(row=None)
                return _EnforcementRows(
                    row={
                        "indisunique": True,
                        "indisvalid": True,
                        "indisready": True,
                        "key_columns": [
                            (
                                "approval_id"
                                if self.wrong_index_keys
                                else "formula_id"
                            ),
                            "formula_version",
                            "formula_schema_version",
                            "engine_version",
                            "feature_schema_version",
                            "outcome_method_version",
                        ],
                        "predicate_definition": (
                            "(formula_id IS NOT NULL)"
                            if self.wrong_index_predicate
                            else "(engine_version IS NOT NULL)"
                        ),
                    }
                )
            raise AssertionError(f"unexpected enforcement query: {normalized}")

    assert store._live_approval_enforcement_status(
        _EnforcementConnection()
    )["ready"] is True
    assert store._live_approval_enforcement_status(
        _EnforcementConnection(
            disabled_trigger="trg_require_formula_owner_live_approval"
        )
    )["ready"] is False
    assert store._live_approval_enforcement_status(
        _EnforcementConnection(
            replica_only_trigger="trg_require_formula_owner_live_approval"
        )
    )["ready"] is False
    assert store._live_approval_enforcement_status(
        _EnforcementConnection(
            disabled_trigger="trg_formula_live_approvals_append_only"
        )
    )["ready"] is False
    assert store._live_approval_enforcement_status(
        _EnforcementConnection(
            wrong_type_trigger="trg_require_formula_owner_live_approval"
        )
    )["ready"] is False
    assert store._live_approval_enforcement_status(
        _EnforcementConnection(wrong_index_keys=True)
    )["ready"] is False
    assert store._live_approval_enforcement_status(
        _EnforcementConnection(wrong_index_predicate=True)
    )["ready"] is False
    assert store._live_approval_enforcement_status(
        _EnforcementConnection(include_index=False)
    )["ready"] is False
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
        store.research_formula_engine.LEGACY_V6_FORMULA_SCHEMA_VERSION,
        store.research_formula_engine.FORMULA_SCHEMA_VERSION,
    }
    persist_source_raw = inspect.getsource(store.persist_discovery_run)
    persist_source = " ".join(persist_source_raw.split())
    assert (
        "current_stage NOT IN ('SHADOW', 'APPROVED', 'LIVE', 'RETIRED')"
        in persist_source
    )
    assert persist_source_raw.index("for global_rank") < persist_source_raw.index(
        "superseded = conn.execute"
    ), "predecessors may be retired only after the replacement cohort is persisted"
    assert "if retirement_ready else []" in persist_source_raw
    assert "horizon_minutes=%s" in persist_source_raw
    assert "if current_stage not in _PROTECTED_FORMULA_STAGES" in persist_source_raw
    assert "SET engine_version" not in persist_source_raw
    assert (
        "current_stage NOT IN (\n"
        "                              'SHADOW', 'APPROVED', 'LIVE', 'RETIRED'"
        in persist_source_raw
    )
    assert persist_source_raw.count("WHERE formula_id=ANY(%s)") == 2
    assert "rowcount != len(superseded_ids)" in persist_source_raw
    assert "rowcount != len(stale_candidate_ids)" in persist_source_raw

    valid_dataset, valid_discovery, valid_formulas = _replacement_contract()
    identity_formula = {
        "formula_version": 1,
        "formula_schema_version": store.research_formula_engine.FORMULA_SCHEMA_VERSION,
        "engine_version": store.research_formula_engine.ENGINE_VERSION,
        "feature_schema_version": store.research_feature_matrix.FEATURE_SCHEMA_VERSION,
        "direction": "LONG",
        "horizon_minutes": 240,
        "conditions": [
            {"feature": "price.return_60m", "operator": ">=", "value": 1.0}
        ],
        "condition_count": 1,
    }
    identity_stored = {
        **identity_formula,
        "outcome_method_version": (
            store.research_feature_matrix.VERIFIED_OUTCOME_METHOD
        ),
    }
    assert store._formula_identity_matches_discovery(
        identity_stored,
        identity_formula,
        outcome_method_version=store.research_feature_matrix.VERIFIED_OUTCOME_METHOD,
    )
    assert not store._formula_identity_matches_discovery(
        {**identity_stored, "engine_version": "stale-engine"},
        identity_formula,
        outcome_method_version=store.research_feature_matrix.VERIFIED_OUTCOME_METHOD,
    )
    assert not store._formula_identity_matches_discovery(
        {
            **identity_stored,
            "conditions": [
                {
                    "feature": "price.return_60m",
                    "operator": ">=",
                    "value": True,
                }
            ],
        },
        identity_formula,
        outcome_method_version=store.research_feature_matrix.VERIFIED_OUTCOME_METHOD,
    )
    record_source = inspect.getsource(store.record_shadow_results)
    pending_source = inspect.getsource(store.load_pending_live_deliveries)
    for approval_binding in (
        "formula_schema_version",
        "engine_version",
        "feature_schema_version",
        "outcome_method_version",
        "approval_operation_version",
        "delivery_environment_enabled",
    ):
        assert approval_binding in record_source
        assert approval_binding in pending_source
    for runtime_guard in (
        "f.formula_schema_version=%s",
        "f.engine_version=%s",
        "f.feature_schema_version=%s",
        "f.outcome_method_version=%s",
    ):
        assert runtime_guard in pending_source

    class _PendingRows:
        def fetchall(self):
            return []

    class _PendingConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def execute(self, query, params=()):
            assert str(query).count("%s") == len(params)
            return _PendingRows()

    original_pending_connect = store._connect
    store._connect = lambda *, read_only: _PendingConnection()
    try:
        assert store.load_pending_live_deliveries(limit=5) == []
    finally:
        store._connect = original_pending_connect
    verified = store._verified_replacement_readiness(
        dataset=valid_dataset,
        discovery=valid_discovery,
        formulas=valid_formulas,
    )
    assert "outcome_method_version" not in valid_formulas[0]
    assert verified["verified"] is True, verified
    assert verified["eligible_symbols"] == ["BTC", "DOGE", "ETH", "SOL"]
    assert verified["qualifying_formula_count"] == 1
    active_replacement = {
        "active": True,
        "formula_version": 1,
        "formula_schema_version": (
            store.research_formula_engine.FORMULA_SCHEMA_VERSION
        ),
        "engine_version": store.research_formula_engine.ENGINE_VERSION,
        "feature_schema_version": (
            store.research_feature_matrix.FEATURE_SCHEMA_VERSION
        ),
        "outcome_method_version": (
            store.research_feature_matrix.VERIFIED_OUTCOME_METHOD
        ),
        "horizon_minutes": 240,
        "latest_evaluation_run_id": 70,
        "current_stage": "BACKTESTED",
    }
    assert store._replacement_cohort_supports_retirement(
        True, [active_replacement], horizon_minutes=240, run_id=70
    ) is True
    assert store._replacement_cohort_supports_retirement(
        True, [], horizon_minutes=240, run_id=70
    ) is False
    assert store._replacement_cohort_supports_retirement(
        True,
        [{**active_replacement, "active": False, "current_stage": "SHADOW"}],
        horizon_minutes=240,
        run_id=70,
    ) is False
    assert store._replacement_cohort_supports_retirement(
        True,
        [{**active_replacement, "latest_evaluation_run_id": 69}],
        horizon_minutes=240,
        run_id=70,
    ) is False
    assert store._replacement_cohort_supports_retirement(
        False, [active_replacement], horizon_minutes=240, run_id=70
    ) is False
    for version_field, wrong_value in (
        ("formula_version", 2),
        ("formula_schema_version", "wrong-formula-schema"),
        ("engine_version", "wrong-engine"),
        ("feature_schema_version", "wrong-feature-schema"),
        ("outcome_method_version", "wrong-outcome-method"),
    ):
        assert store._replacement_cohort_supports_retirement(
            True,
            [{**active_replacement, version_field: wrong_value}],
            horizon_minutes=240,
            run_id=70,
        ) is False

    unavailable_dataset = dict(valid_dataset)
    unavailable_dataset["available"] = False
    unavailable_result = store._verified_replacement_readiness(
        dataset=unavailable_dataset,
        discovery=valid_discovery,
        formulas=valid_formulas,
    )
    assert unavailable_result["verified"] is False
    assert "dataset_available" in unavailable_result["reasons"]

    missing_versions = dict(valid_dataset)
    missing_versions.pop("replay_version")
    missing_versions.pop("first_touch_method_version")
    missing_versions["coverage"] = {
        key: value
        for key, value in valid_dataset["coverage"].items()
        if key not in {"replay_version", "first_touch_method_version"}
    }
    missing_version_result = store._verified_replacement_readiness(
        dataset=missing_versions,
        discovery=valid_discovery,
        formulas=valid_formulas,
    )
    assert missing_version_result["verified"] is False
    assert "replay_version" in missing_version_result["reasons"]
    assert "first_touch_method_version" in missing_version_result["reasons"]

    wrong_outcome_method = dict(valid_dataset)
    wrong_outcome_method["outcome_method_version"] = "wrong-outcome-method"
    wrong_outcome_result = store._verified_replacement_readiness(
        dataset=wrong_outcome_method,
        discovery=valid_discovery,
        formulas=valid_formulas,
    )
    assert wrong_outcome_result["verified"] is False
    assert "outcome_method_version" in wrong_outcome_result["reasons"]

    missing_coverage_versions = dict(valid_dataset)
    missing_coverage_versions["coverage"] = {
        key: value
        for key, value in valid_dataset["coverage"].items()
        if key
        not in {
            "replay_version",
            "first_touch_method_version",
            "canonical_price_provenance_version",
        }
    }
    missing_coverage_version_result = store._verified_replacement_readiness(
        dataset=missing_coverage_versions,
        discovery=valid_discovery,
        formulas=valid_formulas,
    )
    assert missing_coverage_version_result["verified"] is False
    assert "coverage_replay_version_conflict" in (
        missing_coverage_version_result["reasons"]
    )
    assert "coverage_first_touch_method_version_conflict" in (
        missing_coverage_version_result["reasons"]
    )
    assert "coverage_canonical_price_provenance_version_conflict" in (
        missing_coverage_version_result["reasons"]
    )

    missing_calibration = dict(valid_dataset)
    missing_calibration.pop("movement_width_calibration_version")
    missing_calibration_result = store._verified_replacement_readiness(
        dataset=missing_calibration,
        discovery=valid_discovery,
        formulas=valid_formulas,
    )
    assert missing_calibration_result["verified"] is False
    assert "movement_width_calibration_version" in (
        missing_calibration_result["reasons"]
    )

    conflicting_calibration = dict(valid_dataset)
    conflicting_calibration_coverage = dict(valid_dataset["coverage"])
    conflicting_calibration_coverage[
        "movement_width_calibration_version"
    ] = "wrong-calibration"
    conflicting_calibration["coverage"] = conflicting_calibration_coverage
    conflicting_calibration_result = store._verified_replacement_readiness(
        dataset=conflicting_calibration,
        discovery=valid_discovery,
        formulas=valid_formulas,
    )
    assert conflicting_calibration_result["verified"] is False
    assert "coverage_movement_width_calibration_version" in (
        conflicting_calibration_result["reasons"]
    )

    missing_provenance = dict(valid_dataset)
    missing_provenance.pop("canonical_price_provenance_version")
    missing_provenance_result = store._verified_replacement_readiness(
        dataset=missing_provenance,
        discovery=valid_discovery,
        formulas=valid_formulas,
    )
    assert missing_provenance_result["verified"] is False
    assert "canonical_price_provenance_version" in (
        missing_provenance_result["reasons"]
    )

    conflicting_provenance = dict(valid_dataset)
    conflicting_provenance_coverage = dict(valid_dataset["coverage"])
    conflicting_provenance_coverage[
        "canonical_price_provenance_version"
    ] = "wrong-provenance"
    conflicting_provenance["coverage"] = conflicting_provenance_coverage
    conflicting_provenance_result = store._verified_replacement_readiness(
        dataset=conflicting_provenance,
        discovery=valid_discovery,
        formulas=valid_formulas,
    )
    assert conflicting_provenance_result["verified"] is False
    assert "coverage_canonical_price_provenance_version_conflict" in (
        conflicting_provenance_result["reasons"]
    )

    conflicting_versions = dict(valid_dataset)
    conflicting_coverage = dict(valid_dataset["coverage"])
    conflicting_coverage["replay_version"] = "conflicting-replay-version"
    conflicting_versions["coverage"] = conflicting_coverage
    conflicting_result = store._verified_replacement_readiness(
        dataset=conflicting_versions,
        discovery=valid_discovery,
        formulas=valid_formulas,
    )
    assert conflicting_result["verified"] is False
    assert "coverage_replay_version_conflict" in conflicting_result["reasons"]

    forged_boolean = dict(valid_dataset)
    forged_coverage = dict(valid_dataset["coverage"])
    forged_coverage["dataset_kind"] = "delivered_alert_outcomes"
    forged_boolean["coverage"] = forged_coverage
    assert store._verified_replacement_readiness(
        dataset=forged_boolean,
        discovery=valid_discovery,
        formulas=valid_formulas,
    )["verified"] is False

    malformed_coverage_dataset = dict(valid_dataset)
    malformed_coverage = dict(valid_dataset["coverage"])
    malformed_by_symbol = {
        key: dict(value)
        for key, value in valid_dataset["coverage"]["by_symbol"].items()
    }
    malformed_by_symbol["BTC"]["anchors"] = 249
    malformed_coverage["by_symbol"] = malformed_by_symbol
    malformed_coverage_dataset["coverage"] = malformed_coverage
    malformed_result = store._verified_replacement_readiness(
        dataset=malformed_coverage_dataset,
        discovery=valid_discovery,
        formulas=valid_formulas,
    )
    assert malformed_result["verified"] is False
    assert "by_symbol.BTC.eligibility_mismatch" in malformed_result["reasons"]

    nonfinite_dataset = dict(valid_dataset)
    nonfinite_coverage = dict(valid_dataset["coverage"])
    nonfinite_coverage["span_hours"] = float("nan")
    nonfinite_dataset["coverage"] = nonfinite_coverage
    nonfinite_result = store._verified_replacement_readiness(
        dataset=nonfinite_dataset,
        discovery=valid_discovery,
        formulas=valid_formulas,
    )
    assert nonfinite_result["verified"] is False
    assert "span_hours" in nonfinite_result["reasons"]

    empty_result = store._verified_replacement_readiness(
        dataset=valid_dataset,
        discovery=valid_discovery,
        formulas=[],
    )
    assert empty_result["verified"] is False
    assert "nonempty_replacement_cohort" in empty_result["reasons"]
    assert "qualifying_replacement_formula" in empty_result["reasons"]

    discovered_only = [{**valid_formulas[0], "recommended_stage": "DISCOVERED"}]
    discovered_result = store._verified_replacement_readiness(
        dataset=valid_dataset,
        discovery=valid_discovery,
        formulas=discovered_only,
    )
    assert discovered_result["verified"] is False
    assert "qualifying_replacement_formula" in discovered_result["reasons"]

    wrong_horizon = [{**valid_formulas[0], "horizon_minutes": 60}]
    horizon_result = store._verified_replacement_readiness(
        dataset=valid_dataset,
        discovery=valid_discovery,
        formulas=wrong_horizon,
    )
    assert horizon_result["verified"] is False
    assert "formula_version_or_horizon" in horizon_result["reasons"]

    missing_formula_version = [dict(valid_formulas[0])]
    missing_formula_version[0].pop("formula_version")
    formula_version_result = store._verified_replacement_readiness(
        dataset=valid_dataset,
        discovery=valid_discovery,
        formulas=missing_formula_version,
    )
    assert formula_version_result["verified"] is False
    assert "formula_version_or_horizon" in formula_version_result["reasons"]

    start = datetime(2026, 8, 29, 0, 0, tzinfo=timezone.utc)
    rows = [
        _shadow_row(1, at=start),
        # Every later symbol/anchor inside the same fixed 24-hour Market
        # Episode is correlated audit evidence, not another proof.
        _shadow_row(2, at=start + timedelta(minutes=30)),
        # A one-hour boundary is still inside the 24-hour Market Episode.
        _shadow_row(3, at=start + timedelta(minutes=60)),
        # Another symbol in the same broad move is not independent.
        _shadow_row(4, at=start + timedelta(minutes=30), symbol="ETH"),
        # A same-symbol control overlapping a retained match is excluded.
        _shadow_row(
            5,
            at=start + timedelta(minutes=10),
            status="UNMATCHED",
        ),
        # Controls inside the retained Market Episode are excluded.
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
    assert [row["event_id"] for row in selected["matches"]] == [1]
    assert selected["controls"] == []
    assert selected["excluded_match_event_ids"] == [2, 3, 4, 8]
    assert selected["excluded_control_event_ids"] == [5, 6, 7]
    assert selected["exact_cohort_excluded_event_ids"] == []
    assert 8 not in {row["event_id"] for row in selected["rows"]}
    assert selected["membership_censor_event_ids"] == [8]

    # Active v5 formulas stay in SHADOW, but all new work now comes only from
    # the exact sampler-v4 authority view. Delivered ALERTs and older sampler
    # rows are never loaded, and v5 can never enter LIVE execution.
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
                assert dense.count("f.formula_schema_version=%sAND") >= 3
                assert "f.current_stage='LIVE'" in normalized
                assert (
                    "f.current_stage='LIVE'AND"
                    "f.formula_schema_version=%sAND"
                    "f.engine_version=%sAND"
                    "f.feature_schema_version=%sAND"
                    "f.outcome_method_version=%s"
                ) in dense
                current_contract = (
                    store.research_formula_engine.FORMULA_SCHEMA_VERSION,
                    store.research_formula_engine.ENGINE_VERSION,
                    store.research_feature_matrix.FEATURE_SCHEMA_VERSION,
                    store.research_feature_matrix.VERIFIED_OUTCOME_METHOD,
                )
                legacy_v6_contract = (
                    store.research_formula_engine.LEGACY_V6_FORMULA_SCHEMA_VERSION,
                    store.research_formula_engine.LEGACY_V6_ENGINE_VERSION,
                    store.research_feature_matrix.FEATURE_SCHEMA_VERSION,
                    store.research_feature_matrix.VERIFIED_OUTCOME_METHOD,
                )
                assert params == (
                    *legacy_v6_contract,
                    *current_contract,
                    *current_contract,
                )
                return _Rows(
                    [
                        {
                            "formula_id": 3153,
                            "formula_key": "3" * 64,
                            "formula_version": 1,
                            "formula_text": "legacy formula",
                            "formula_schema_version": store.research_formula_engine.LEGACY_V6_FORMULA_SCHEMA_VERSION,
                            "engine_version": store.research_formula_engine.LEGACY_V6_ENGINE_VERSION,
                            "direction": "SHORT",
                            "horizon_minutes": 720,
                            "conditions": [],
                            "feature_schema_version": store.research_feature_matrix.FEATURE_SCHEMA_VERSION,
                            "outcome_method_version": store.research_feature_matrix.VERIFIED_OUTCOME_METHOD,
                            "last_shadow_event_id": 0,
                            "shadow_started_at_utc": start,
                            "current_stage": "SHADOW",
                            "live_alert_approved": False,
                            "ranking_score": 1.0,
                            "holdout_metrics": {},
                        }
                    ]
                )
            assert "FROM research_prospective_shadow_events candidate" in normalized
            assert "research_events candidate" not in normalized
            assert "candidate.sampler_version=%s" in normalized
            assert "candidate.feature_bundle_policy_version=%s" in normalized
            assert params[-3:] == (
                store._PROSPECTIVE_ANCHOR_SAMPLER_VERSION,
                store._FEATURE_BUNDLE_POLICY_VERSION,
                5,
            )
            return _Rows(
                [
                    {
                        "event_id": 9001,
                        "alert_time_utc": start + timedelta(hours=1),
                        "symbol": "BTC",
                        "direction": "SHORT",
                        "event_type": "PROSPECTIVE_NEUTRAL_30M",
                        "setup_key": "selftest",
                        "event_kind": "DECISION_SAMPLE",
                        "delivery_status": "NOT_APPLICABLE",
                        "prospective_anchor_slot_id": 17,
                        "prospective_input_fingerprint": "d" * 64,
                        "feature_bundle_policy_version": (
                            store._FEATURE_BUNDLE_POLICY_VERSION
                        ),
                        "feature_bundle_sha256": "b" * 64,
                        "decision_feature_bundle": {},
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
    assert legacy_work[0]["formula_schema_version"] == (
        store.research_formula_engine.LEGACY_V6_FORMULA_SCHEMA_VERSION
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
                assert dense.count("formula_schema_version=%sAND") >= 2
                assert "engine_version=%s" in dense
                assert "feature_schema_version=%s" in dense
                assert "outcome_method_version=%s" in dense
                assert "formula_schema_version" in normalized
                assert "formula_key" in normalized
                assert "engine_version" in normalized
                assert "feature_schema_version" in normalized
                assert "outcome_method_version" in normalized
                assert "direction" in normalized
                assert "conditions" in normalized
                assert params == (
                    store.research_formula_engine.LEGACY_V6_FORMULA_SCHEMA_VERSION,
                    store.research_formula_engine.LEGACY_V6_ENGINE_VERSION,
                    store.research_feature_matrix.FEATURE_SCHEMA_VERSION,
                    store.research_feature_matrix.VERIFIED_OUTCOME_METHOD,
                    store.research_formula_engine.FORMULA_SCHEMA_VERSION,
                    store.research_formula_engine.ENGINE_VERSION,
                    store.research_feature_matrix.FEATURE_SCHEMA_VERSION,
                    store.research_feature_matrix.VERIFIED_OUTCOME_METHOD,
                )
                return _Rows(
                    [
                        {
                            "formula_id": 3153,
                            "formula_key": "legacy-v6-shadow",
                            "formula_version": 1,
                            "formula_schema_version": (
                                store.research_formula_engine.LEGACY_V6_FORMULA_SCHEMA_VERSION
                            ),
                            "engine_version": store.research_formula_engine.LEGACY_V6_ENGINE_VERSION,
                            "feature_schema_version": (
                                store.research_feature_matrix.FEATURE_SCHEMA_VERSION
                            ),
                            "outcome_method_version": (
                                store.research_feature_matrix.VERIFIED_OUTCOME_METHOD
                            ),
                            "direction": "SHORT",
                            "conditions": [],
                            "horizon_minutes": 720,
                            "latest_evaluation_run_id": 29,
                            "shadow_started_at_utc": start,
                            "last_shadow_event_id": 0,
                        }
                    ]
                )
            if "FROM research_formula_shadow_checks" in normalized:
                assert "JOIN research_prospective_shadow_events authorized" in normalized
                assert "c.evidence_policy_version" in normalized
                assert "c.authoritative_verified" in normalized
                assert "c.authoritative_verified IS TRUE" not in normalized
                assert "authorized.sampler_version=%s" in normalized
                assert "authorized.feature_bundle_policy_version=%s" in normalized
                assert "c.input_snapshot->>'evidence_policy_version'=%s" not in normalized
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
    original_relevance_persist = store._persist_shadow_evidence_and_relevance
    store._persist_shadow_evidence_and_relevance = lambda conn, *, formula, validation: (
        {
            "snapshot_id": "4" * 64,
            "compatibility": "LEGACY_SHADOW_READ_ONLY",
            "formula_family_id": "5" * 64,
        },
        {
            "state": "LEGACY_READ_ONLY",
            "experimental_relevance_eligible": False,
        },
    )
    try:
        readiness = store.evaluate_shadow_readiness()
    finally:
        store._connect = original_connect
        store._persist_shadow_evidence_and_relevance = original_relevance_persist
    assert readiness["evaluated"] == 1
    assert readiness_connection.updated is True

    # Exact decision cohorts collapse before episode selection. A mixed-status
    # authoritative cohort is contradictory and therefore fails closed.
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
    assert collapsed["matches"] == []
    assert collapsed["controls"] == []
    assert collapsed["exact_cohort_excluded_event_ids"] == []
    assert collapsed["conflicting_cohort_event_ids"] == [10, 11]

    # Only the forecast-start cohort can supply an episode outcome. Source
    # availability anchors may sort differently after retries, but they must
    # never select a later forecast or shorten the independence window.
    later_forecast = _shadow_row(
        200, at=start + timedelta(minutes=30)
    )
    later_forecast["decision_anchor_time_utc"] = start
    later_forecast["mfe_pct"] = 1.0
    earlier_forecast = _shadow_row(
        201, at=start + timedelta(minutes=15)
    )
    earlier_forecast["decision_anchor_time_utc"] = start + timedelta(
        minutes=15
    )
    earlier_forecast["mfe_pct"] = 9.0
    forecast_validation = store._build_shadow_validation(
        {"horizon_minutes": 60},
        [later_forecast, earlier_forecast],
        evaluated_at_utc=start + timedelta(hours=25),
    )
    assert forecast_validation["metrics"]["sample_event_ids"] == [201]
    assert forecast_validation["metrics"]["median_mfe_pct"] == 9.0

    # Missing evidence at the episode's first forecast fails closed. A later
    # correlated row remains audit-visible but cannot veto or rescue it.
    first = _shadow_row(100, at=start)
    later = _shadow_row(101, at=start + timedelta(hours=1))
    missing_first = {
        **first,
        "outcome_available": False,
        "outcome_due": True,
    }
    missing_first_validation = store._build_shadow_validation(
        {"horizon_minutes": 60},
        [missing_first, later],
        evaluated_at_utc=start + timedelta(hours=25),
    )
    assert missing_first_validation["metrics"]["sample_size"] == 0
    assert missing_first_validation["evidence"]["overdue_outcome_event_ids"] == [
        100
    ]
    assert missing_first_validation["evidence"][
        "correlated_overdue_outcome_event_ids"
    ] == []
    assert (
        missing_first_validation["gates"][
            "no overdue canonical outcome gaps"
        ]
        is False
    )
    missing_later = {
        **later,
        "outcome_available": False,
        "outcome_due": True,
    }
    missing_later_validation = store._build_shadow_validation(
        {"horizon_minutes": 60},
        [first, missing_later],
        evaluated_at_utc=start + timedelta(hours=25),
    )
    assert missing_later_validation["metrics"]["sample_size"] == 1
    assert missing_later_validation["evidence"]["overdue_outcome_event_ids"] == []
    assert missing_later_validation["evidence"][
        "correlated_overdue_outcome_event_ids"
    ] == [101]
    assert (
        missing_later_validation["gates"][
            "no overdue canonical outcome gaps"
        ]
        is True
    )

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

    strict_width_times = tuple(
        start - timedelta(minutes=100 - index) for index in range(80)
    )
    strict_reference = store.research_session_width.movement_width_reference(
        symbol="BTC",
        event_time=start,
        horizon_minutes=240,
        as_of_utc=start - timedelta(minutes=1),
        historical_index={
            ("BTC", 240): store.research_session_width.PriceWidthSeries(
                times=strict_width_times,
                abs_return_pcts=tuple([1.0] * 40 + [0.6] * 40),
                active_ratios=tuple([1.0] * 40 + [0.0] * 40),
            )
        },
    )
    assert strict_reference["threshold_scale_factor"] == 0.60
    strict_policy = store.research_no_dwell_outcome.freeze_threshold_policy(
        horizon_minutes=240,
        decision_time=start,
        prior_only_reference=strict_reference,
    )
    strict_source = {
        "first_touch_available": True,
        "alert_time_utc": start,
        "symbol": "BTC",
        "input_snapshot": {
            "snapshot_policy_version": (
                store._SHADOW_INPUT_SNAPSHOT_POLICY_VERSION
            ),
            "decision_cohort_policy_version": (
                store._DECISION_COHORT_POLICY_VERSION
            ),
            "evidence_policy_version": (
                store._PROSPECTIVE_EVIDENCE_POLICY_VERSION
            ),
            "movement_width_reference": strict_reference,
        },
        "first_touch_threshold_scale_factor": 0.60,
        "first_touch_threshold_source_kind": (
            "PRIOR_ONLY_SESSION_CALIBRATION"
        ),
        "first_touch_threshold_policy": strict_policy,
        "qualifying_move_threshold_pct": 0.60,
    }
    compatible, reason = store._terminal_threshold_matches_snapshot(
        strict_source,
        horizon_minutes=240,
        require_v6_contract=True,
    )
    assert compatible is True and "matches" in reason
    forged_reference = {
        **strict_reference,
        "calibration_version": "forged-calibration",
    }
    compatible, _ = store._terminal_threshold_matches_snapshot(
        {
            **strict_source,
            "input_snapshot": {
                **strict_source["input_snapshot"],
                "movement_width_reference": forged_reference,
            },
        },
        horizon_minutes=240,
        require_v6_contract=True,
    )
    assert compatible is False
    future_reference = {
        **strict_reference,
        "as_of_utc": start + timedelta(days=10),
    }
    compatible, _ = store._terminal_threshold_matches_snapshot(
        {
            **strict_source,
            "input_snapshot": {
                **strict_source["input_snapshot"],
                "movement_width_reference": future_reference,
            },
        },
        horizon_minutes=240,
        require_v6_contract=True,
    )
    assert compatible is False
    forged_policy = deepcopy(strict_policy)
    forged_policy["threshold_reference_hash"] = "0" * 64
    compatible, _ = store._terminal_threshold_matches_snapshot(
        {**strict_source, "first_touch_threshold_policy": forged_policy},
        horizon_minutes=240,
        require_v6_contract=True,
    )
    assert compatible is False
    boolean_policy = deepcopy(strict_policy)
    boolean_policy["base_threshold_pct"] = True
    compatible, _ = store._terminal_threshold_matches_snapshot(
        {**strict_source, "first_touch_threshold_policy": boolean_policy},
        horizon_minutes=240,
        require_v6_contract=True,
    )
    assert compatible is False

    mismatched_terminal = {
        **_shadow_row(30, at=start + timedelta(hours=8)),
        "authoritative_verified": True,
        "evidence_policy_version": store._PROSPECTIVE_EVIDENCE_POLICY_VERSION,
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

    # v7 Max-Pain proof/cohort is checked before independence selection. A
    # malformed observation remains an outcome-blind membership censor: a
    # later correlated match cannot erase the missing first-cohort evidence.
    max_pain_formula = {
        "formula_id": 42,
        "formula_key": "f" * 64,
        "formula_version": 1,
        "formula_schema_version": store.research_formula_engine.FORMULA_SCHEMA_VERSION,
        "engine_version": store.research_formula_engine.ENGINE_VERSION,
        "feature_schema_version": store.research_feature_matrix.FEATURE_SCHEMA_VERSION,
        "outcome_method_version": store.research_feature_matrix.VERIFIED_OUTCOME_METHOD,
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
    no_max_pain_formula = {
        **max_pain_formula,
        "formula_key": "e" * 64,
        "conditions": [
            {"feature": "price.return_60m", "operator": ">=", "value": 0.1}
        ],
    }
    compatible, _ = store._max_pain_shadow_check_contract(
        no_max_pain_formula,
        {
            **_shadow_row(39, at=start + timedelta(minutes=5)),
            "input_snapshot": {},
            "decision_cohort_key": "0" * 64,
            "decision_anchor_time_utc": start + timedelta(days=1),
        },
    )
    assert compatible is False
    header_only_snapshot = {
        "formula_schema_version": (
            store.research_formula_engine.FORMULA_SCHEMA_VERSION
        ),
        "engine_version": store.research_formula_engine.ENGINE_VERSION,
        "feature_schema_version": (
            store.research_feature_matrix.FEATURE_SCHEMA_VERSION
        ),
        "outcome_method_version": (
            store.research_feature_matrix.VERIFIED_OUTCOME_METHOD
        ),
        "snapshot_policy_version": store._SHADOW_INPUT_SNAPSHOT_POLICY_VERSION,
        "decision_cohort_policy_version": (
            store._DECISION_COHORT_POLICY_VERSION
        ),
        "decision_input_policy_version": (
            store.research_feature_matrix.PROSPECTIVE_FROZEN_INPUT_POLICY_VERSION
        ),
        "movement_width_reference": (
            store.research_session_width.movement_width_reference(
                symbol="BTC",
                event_time=start + timedelta(minutes=5),
                horizon_minutes=60,
                as_of_utc=start,
                historical_index={},
            )
        ),
    }
    compatible, reason = store._max_pain_snapshot_contract(
        no_max_pain_formula,
        header_only_snapshot,
        decision_time_utc=start + timedelta(minutes=5),
        symbol="BTC",
    )
    assert compatible is False
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
    for location, field, corrupt_value in (
        ("event", "event_id", str(valid_max_pain["event_id"])),
        ("event", "event_id", True),
        ("snapshot", "formula_version", "1"),
        ("snapshot", "formula_version", True),
        ("snapshot", "horizon_minutes", "60"),
        ("snapshot", "horizon_minutes", True),
    ):
        malformed_identity = deepcopy(valid_max_pain)
        if location == "event":
            malformed_identity["input_snapshot"]["event"][field] = corrupt_value
        else:
            malformed_identity["input_snapshot"][field] = corrupt_value
        compatible, _ = store._max_pain_shadow_check_contract(
            max_pain_formula, malformed_identity
        )
        assert compatible is False

    # The write boundary does not trust a caller merely because all submitted
    # fields agree. It reloads the slot-only bundle, verifies its hash, and
    # repeats only the formula operators over its frozen flat values.
    valid_max_pain, authoritative_row, authorized_slot = (
        _authoritative_bundle_fixture(valid_max_pain, max_pain_formula)
    )
    compatible, reason = store._authoritative_v6_row_contract(
        max_pain_formula,
        valid_max_pain,
        valid_max_pain,
        authoritative_row,
        authorized_slot,
    )
    assert compatible is True and "authoritative" in reason, reason

    # The exact same authoritative write contract applies to the retained v5
    # Shadow cohort.  Its historical formula versions stay intact; only the
    # new sampler-v4 evidence is executable.
    legacy_formula = {
        **max_pain_formula,
        "formula_schema_version": "research-formula-v5-safe-replay",
        "engine_version": "formula-discovery-v5-safe-replay",
        "feature_schema_version": "research-feature-matrix-v5",
    }
    legacy_check = deepcopy(valid_max_pain)
    for key in (
        "formula_schema_version",
        "engine_version",
        "feature_schema_version",
    ):
        legacy_check["input_snapshot"][key] = legacy_formula[key]
    legacy_check["input_snapshot"]["legacy_v5_shadow_adapter_version"] = (
        store._LEGACY_V5_SHADOW_ADAPTER_VERSION
    )
    legacy_key, legacy_anchor = store._decision_cohort_identity(
        formula=legacy_formula,
        event=legacy_check,
        snapshot=legacy_check["input_snapshot"],
    )
    legacy_check["decision_cohort_key"] = legacy_key
    legacy_check["decision_anchor_time_utc"] = legacy_anchor
    compatible, reason = store._authoritative_frozen_row_contract(
        legacy_formula,
        legacy_check,
        legacy_check,
        authoritative_row,
        authorized_slot,
    )
    assert compatible is True, reason

    tampered_authority = deepcopy(authorized_slot)
    tampered_authority["feature_bundle_sha256"] = "0" * 64
    compatible, reason = store._authoritative_frozen_row_contract(
        max_pain_formula,
        valid_max_pain,
        valid_max_pain,
        authoritative_row,
        tampered_authority,
    )
    assert compatible is False and "bundle" in reason

    max_pain_feature = (
        "max_pain.aggregate.short_long_liquidity_ratio"
    )
    boolean_condition = deepcopy(valid_max_pain)
    boolean_condition["input_snapshot"]["conditions"][0]["value"] = True
    boolean_condition["input_snapshot"]["condition_results"][0][
        "expected"
    ] = True
    boolean_condition["condition_results"][0]["expected"] = True
    compatible, _ = store._max_pain_shadow_check_contract(
        max_pain_formula, boolean_condition
    )
    assert compatible is False

    # Even a caller that forges every submitted copy consistently cannot use
    # Python's True == 1.0 equivalence at the authoritative write boundary.
    boolean_feature = deepcopy(valid_max_pain)
    boolean_authoritative = deepcopy(authoritative_row)
    boolean_authoritative["max_pain_features"]["features"][
        max_pain_feature
    ] = 1.0
    boolean_feature["input_snapshot"]["formula_key_features"][
        max_pain_feature
    ] = True
    boolean_feature["input_snapshot"]["condition_results"][0].update(
        {"actual": True, "passed": True}
    )
    boolean_feature["condition_results"][0].update(
        {"actual": True, "passed": True}
    )
    boolean_feature["input_snapshot"]["max_pain_provenance"][
        "condition_values"
    ][max_pain_feature] = True
    compatible, reason = store._authoritative_v6_row_contract(
        max_pain_formula,
        boolean_feature,
        boolean_feature,
        boolean_authoritative,
        authorized_slot,
    )
    assert compatible is False and "feature values" in reason

    consistent_forgery = deepcopy(valid_max_pain)
    forged_value = 0.25
    consistent_forgery["input_snapshot"]["formula_key_features"][
        max_pain_feature
    ] = forged_value
    consistent_forgery["input_snapshot"]["condition_results"][0].update(
        {"actual": forged_value, "passed": False}
    )
    consistent_forgery["condition_results"][0].update(
        {"actual": forged_value, "passed": False}
    )
    consistent_forgery["input_snapshot"]["max_pain_provenance"][
        "condition_values"
    ][max_pain_feature] = forged_value
    consistent_forgery["evaluation_status"] = "UNMATCHED"
    consistent_forgery["matched"] = False
    compatible, reason = store._authoritative_v6_row_contract(
        max_pain_formula,
        consistent_forgery,
        consistent_forgery,
        authoritative_row,
        authorized_slot,
    )
    assert compatible is False and "feature values" in reason

    forged_sources = deepcopy(valid_max_pain)
    forged_sources["input_snapshot"]["source_inputs"]["futures_cvd"][
        "prospective_input_fingerprint"
    ] = "0" * 64
    compatible, reason = store._authoritative_v6_row_contract(
        max_pain_formula,
        forged_sources,
        forged_sources,
        authoritative_row,
        authorized_slot,
    )
    assert compatible is False and "source inputs" in reason

    forged_width = deepcopy(valid_max_pain)
    forged_width["input_snapshot"]["movement_width_reference"][
        "threshold_scale_factor"
    ] = 0.01
    compatible, reason = store._authoritative_v6_row_contract(
        max_pain_formula,
        forged_width,
        forged_width,
        authoritative_row,
        authorized_slot,
    )
    assert compatible is False and "movement-width" in reason

    forged_max_pain = deepcopy(valid_max_pain)
    forged_max_pain["input_snapshot"]["max_pain_provenance"][
        "provenance_sha256"
    ] = "0" * 64
    compatible, reason = store._authoritative_v6_row_contract(
        max_pain_formula,
        forged_max_pain,
        forged_max_pain,
        authoritative_row,
        authorized_slot,
    )
    assert compatible is False and "Max-Pain" in reason

    missing_row_compatible, missing_row_reason = (
        store._authoritative_v6_row_contract(
            max_pain_formula,
            valid_max_pain,
            valid_max_pain,
            None,
            authorized_slot,
        )
    )
    assert missing_row_compatible is False
    assert "unavailable" in missing_row_reason

    class _WriteRows:
        def __init__(self, rows):
            self._rows = list(rows)

        def fetchone(self):
            return self._rows[0] if self._rows else None

        def fetchall(self):
            return self._rows

    class _AuthoritativeWriteConnection:
        def __init__(self, *, authorized=True):
            self.queries = []
            self.inserted_check = None
            self.committed = False
            self.authorized = authorized

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def execute(self, query, params=()):
            assert query.count("%s") == len(params)
            normalized = " ".join(query.split())
            self.queries.append(normalized)
            if "FROM research_formulas WHERE formula_id=" in normalized:
                return _WriteRows(
                    [
                        {
                            **max_pain_formula,
                            "current_stage": "SHADOW",
                            "active": True,
                            "shadow_started_at_utc": valid_at
                            - timedelta(minutes=1),
                            "last_shadow_event_id": 0,
                            "live_alert_approved": False,
                            "live_alert_approved_by": None,
                        }
                    ]
                )
            if "FROM research_prospective_shadow_events authorized" in normalized:
                if not self.authorized:
                    return _WriteRows([])
                return _WriteRows(
                    [
                        {
                            **authorized_slot,
                            "event_id": valid_max_pain["event_id"],
                            "alert_time_utc": valid_at,
                            "symbol": "BTC",
                            "direction": "LONG",
                            "event_type": "SELFTEST_ALERT",
                            "setup_key": None,
                            "event_kind": "DECISION_SAMPLE",
                            "delivery_status": "NOT_APPLICABLE",
                            "shadow_eligible": True,
                        }
                    ]
                )
            if "FROM research_events" in normalized:
                assert "event_kind='DECISION_SAMPLE'" in normalized
                return _WriteRows(
                    [
                        {
                            "event_id": valid_max_pain["event_id"],
                            "alert_time_utc": valid_at,
                            "symbol": "BTC",
                            "direction": "LONG",
                            "event_type": "SELFTEST_ALERT",
                            "setup_key": None,
                            "source_side": None,
                            "timeframe": None,
                            "strategy_version": None,
                            "code_version": None,
                            "event_kind": "DECISION_SAMPLE",
                            "delivery_status": "NOT_APPLICABLE",
                            "shadow_eligible": False,
                        }
                    ]
                )
            if "INSERT INTO research_formula_shadow_checks" in normalized:
                self.inserted_check = params
                return _WriteRows([{"formula_id": max_pain_formula["formula_id"]}])
            if "INSERT INTO research_formula_shadow_hits" in normalized:
                return _WriteRows([])
            if "UPDATE research_formulas" in normalized:
                return _WriteRows([])
            raise AssertionError(f"unexpected authoritative write query: {normalized}")

        def commit(self):
            self.committed = True

    # If the authoritative rebuild crashes, the submitted MATCHED payload is
    # persisted only as UNEVALUABLE. It must never create a hit or LIVE queue.
    write_connection = _AuthoritativeWriteConnection()
    original_authoritative_loader = (
        store.research_feature_matrix.load_shadow_feature_rows_by_horizon
    )
    original_write_connect = store._connect
    store.research_feature_matrix.load_shadow_feature_rows_by_horizon = (
        lambda _requested: (_ for _ in ()).throw(
            RuntimeError("selftest frozen-slot archive unavailable")
        )
    )
    store._connect = lambda *, read_only: write_connection
    try:
        write_result = store.record_shadow_results(
            formula=max_pain_formula,
            results=[valid_max_pain],
        )
    finally:
        store._connect = original_write_connect
        store.research_feature_matrix.load_shadow_feature_rows_by_horizon = (
            original_authoritative_loader
        )
    assert write_result == {
        "checked": 1,
        "matched": 0,
        "queued": 0,
        "new_hit_event_ids": [],
    }
    assert write_connection.committed is True
    assert write_connection.inserted_check is not None
    assert write_connection.inserted_check[2] is False
    assert write_connection.inserted_check[4] == "UNEVALUABLE"
    assert "authoritative frozen-bundle load failed" in str(
        write_connection.inserted_check[5]
    )
    assert write_connection.inserted_check[10] == (
        store._REJECTED_PROSPECTIVE_EVIDENCE_POLICY_VERSION
    )
    assert write_connection.inserted_check[11] == 17
    assert write_connection.inserted_check[12] == authorized_slot[
        "input_fingerprint"
    ]
    assert write_connection.inserted_check[13] == authorized_slot[
        "feature_bundle_sha256"
    ]
    assert write_connection.inserted_check[14] is False
    assert not any(
        "INSERT INTO research_formula_shadow_hits" in query
        or "INSERT INTO research_formula_live_deliveries" in query
        for query in write_connection.queries
    )

    # A fully matching DB formula/event/slot/bundle payload writes the exact
    # current evidence policy and authoritative_verified=true.  The silent
    # DECISION_SAMPLE may create a Shadow hit, but never a LIVE delivery.
    successful_connection = _AuthoritativeWriteConnection()
    store.research_feature_matrix.load_shadow_feature_rows_by_horizon = (
        lambda _requested: {
            (
                int(valid_max_pain["event_id"]),
                int(max_pain_formula["horizon_minutes"]),
            ): authoritative_row
        }
    )
    store._connect = lambda *, read_only: successful_connection
    try:
        successful_write = store.record_shadow_results(
            formula=max_pain_formula,
            results=[valid_max_pain],
        )
    finally:
        store._connect = original_write_connect
        store.research_feature_matrix.load_shadow_feature_rows_by_horizon = (
            original_authoritative_loader
        )
    assert successful_write == {
        "checked": 1,
        "matched": 1,
        "queued": 0,
        "new_hit_event_ids": [int(valid_max_pain["event_id"])],
    }
    assert successful_connection.inserted_check[10] == (
        store._PROSPECTIVE_EVIDENCE_POLICY_VERSION
    )
    assert successful_connection.inserted_check[14] is True
    assert any(
        "INSERT INTO research_formula_shadow_hits" in query
        for query in successful_connection.queries
    )
    assert not any(
        "INSERT INTO research_formula_live_deliveries" in query
        for query in successful_connection.queries
    )

    # If the exact v4 authority view no longer authorizes the event, preserve
    # a rejected UNEVALUABLE audit row with nullable evidence refs.  It must
    # not silently disappear or produce a hit/queue, and ALERT fallback rows
    # are excluded directly by the read query.
    missing_authority_connection = _AuthoritativeWriteConnection(
        authorized=False
    )
    store.research_feature_matrix.load_shadow_feature_rows_by_horizon = (
        lambda _requested: {}
    )
    store._connect = lambda *, read_only: missing_authority_connection
    try:
        missing_authority_write = store.record_shadow_results(
            formula=max_pain_formula,
            results=[valid_max_pain],
        )
    finally:
        store._connect = original_write_connect
        store.research_feature_matrix.load_shadow_feature_rows_by_horizon = (
            original_authoritative_loader
        )
    assert missing_authority_write == {
        "checked": 1,
        "matched": 0,
        "queued": 0,
        "new_hit_event_ids": [],
    }
    assert missing_authority_connection.inserted_check[4] == "UNEVALUABLE"
    assert missing_authority_connection.inserted_check[10] == (
        store._REJECTED_PROSPECTIVE_EVIDENCE_POLICY_VERSION
    )
    assert missing_authority_connection.inserted_check[11:14] == (
        None,
        None,
        None,
    )
    assert missing_authority_connection.inserted_check[14] is False
    assert not any(
        "INSERT INTO research_formula_shadow_hits" in query
        or "INSERT INTO research_formula_live_deliveries" in query
        for query in missing_authority_connection.queries
    )

    max_pain_validation = store._build_shadow_validation(
        max_pain_formula,
        [malformed, valid_max_pain],
        evaluated_at_utc=valid_at + timedelta(hours=25),
    )
    max_pain_evidence = max_pain_validation["evidence"][
        "max_pain_provenance"
    ]
    assert max_pain_evidence["required"] is True
    assert max_pain_evidence["incompatible_event_ids"] == [40]
    assert [
        item["event_id"] for item in max_pain_evidence["event_provenance_refs"]
    ] == []
    assert max_pain_validation["gates"][
        "complete Max-Pain provenance chain"
    ] is True
    assert max_pain_validation["metrics"]["sample_size"] == 0
    assert max_pain_validation["evidence"]["raw_evaluation_status"] == {
        "MATCHED": 1,
        "UNMATCHED": 0,
        "UNEVALUABLE": 1,
    }
    reordered_validation = store._build_shadow_validation(
        max_pain_formula,
        [valid_max_pain, malformed],
        evaluated_at_utc=valid_at + timedelta(hours=25),
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
