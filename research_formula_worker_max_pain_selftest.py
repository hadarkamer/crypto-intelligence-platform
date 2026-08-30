"""Database-free checks for frozen Max-Pain Shadow provenance."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone

import research_feature_matrix
import research_formula_engine
import research_formula_store
import research_formula_worker
import research_max_pain_archive


BASE = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)


def _record(set_id: int, *, available_at: datetime, marker: str) -> dict:
    return {
        "snapshot_set_id": set_id,
        "snapshot_key": marker * 64,
        "set_payload_sha256": "a" * 64,
        "symbol": "BTC",
        "symbol_manifest_payload_sha256": "b" * 64,
        "row_payload_sha256": [
            {"timeframe": timeframe, "payload_sha256": marker * 63 + suffix}
            for timeframe, suffix in zip(
                research_max_pain_archive.REQUIRED_TIMEFRAMES,
                "3456789",
            )
        ],
        "archive_schema_version": research_max_pain_archive.ARCHIVE_SCHEMA_VERSION,
        "method_version": research_max_pain_archive.METHOD_VERSION,
        "cutover_marker": research_max_pain_archive.CUTOVER_MARKER,
        "cutover_time_utc": (
            research_max_pain_archive.CUTOVER_TIME_UTC.isoformat()
        ),
        "available_at_utc": available_at.isoformat(),
        "created_at_utc": (available_at + timedelta(seconds=5)).isoformat(),
        "cycle_id": f"selftest:{set_id}",
        "cycle_time_utc": (available_at - timedelta(minutes=5)).isoformat(),
        "source": "WATCH_SHARED",
        "collector_version": "selftest-v1",
    }


def _provenance(*, delta: bool) -> dict:
    current_available = BASE + timedelta(minutes=5)
    value = {
        "policy_version": (
            research_max_pain_archive.SHADOW_PROVENANCE_POLICY_VERSION
        ),
        "symbol": "BTC",
        "current": _record(2, available_at=current_available, marker="2"),
        "previous": (
            _record(
                1,
                available_at=current_available - timedelta(minutes=30),
                marker="1",
            )
            if delta
            else None
        ),
        "used_for_delta": delta,
        "previous_gap_minutes": 30.0 if delta else None,
        "previous_gap_policy_minutes": (
            research_max_pain_archive.DEFAULT_MAX_PREVIOUS_GAP_MINUTES
        ),
    }
    return {
        "provenance": value,
        "provenance_sha256": (
            research_max_pain_archive.canonical_provenance_sha256(value)
        ),
    }


def _formula(feature: str, *, schema: str | None = None) -> dict:
    return {
        "formula_id": 42,
        "formula_key": "f" * 64,
        "formula_version": 1,
        "formula_schema_version": (
            schema or research_formula_engine.FORMULA_SCHEMA_VERSION
        ),
        "engine_version": research_formula_engine.ENGINE_VERSION,
        "feature_schema_version": research_feature_matrix.FEATURE_SCHEMA_VERSION,
        "outcome_method_version": research_feature_matrix.VERIFIED_OUTCOME_METHOD,
        "direction": "LONG",
        "horizon_minutes": 240,
        "conditions": [{"feature": feature, "operator": ">=", "value": 1.0}],
    }


def _event() -> dict:
    return {
        "event_id": 9001,
        "alert_time_utc": BASE + timedelta(minutes=10),
        "symbol": "BTC",
        "direction": "LONG",
        "event_type": "SELFTEST_ALERT",
        "setup_key": "selftest",
    }


def _row(*, delta: bool) -> dict:
    provenance = _provenance(delta=delta)
    slot = {
        "prospective_anchor_slot_id": 7,
        "prospective_input_fingerprint": "c" * 64,
        "prospective_slot_created_at_utc": (
            BASE + timedelta(minutes=10)
        ).isoformat(),
    }
    return {
        "decision_input_policy_version": (
            research_feature_matrix.PROSPECTIVE_FROZEN_INPUT_POLICY_VERSION
        ),
        "raw_features": {
            "latest_at_or_before_alert": {
                "price_oi": {
                    "timestamp_utc": BASE.isoformat(),
                    "price_timestamp_utc": BASE.isoformat(),
                    "oi_timestamp_utc": BASE.isoformat(),
                    "price_exchange": "binance",
                    "price_market": "spot",
                    "price_pair": "BTCUSDT",
                    "price_instrument_id": None,
                    "price_source": "binance_spot",
                    "source": "binance_spot",
                    "price_timeframe": "1m",
                    "price_interval_seconds": 60,
                    "canonical_price_method_version": (
                        research_formula_store.canonical_price_path.METHOD_VERSION
                    ),
                    "canonical_price_provenance_version": (
                        research_formula_store.canonical_price_path.PRICE_PROVENANCE_VERSION
                    ),
                    "canonical_price_provenance": {
                        "provenance_version": research_formula_store.canonical_price_path.PRICE_PROVENANCE_VERSION,
                        "method_version": research_formula_store.canonical_price_path.METHOD_VERSION,
                        "symbol": "BTC",
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
                    "timestamp_utc": BASE.isoformat(),
                    "source": "coinglass_futures_aggregated_cvd",
                    "exchange_list": "Binance,OKX,Bybit",
                    **slot,
                },
                "spot_cvd": {
                    "timestamp_utc": BASE.isoformat(),
                    "source": "coinglass_spot_aggregated_cvd",
                    "exchange_list": "Binance,OKX,Bybit",
                    **slot,
                },
            }
        },
        "outcome_label": {
            "movement_width_reference": (
                research_formula_store.research_session_width.movement_width_reference(
                    symbol="BTC",
                    event_time=BASE + timedelta(minutes=10),
                    horizon_minutes=240,
                    as_of_utc=BASE,
                    historical_index={},
                )
            )
        },
        "max_pain_features": {
            "evaluation_status": "EVALUABLE",
            "reason": "current snapshot is coherent",
            "change_evaluation_status": "EVALUABLE" if delta else "UNEVALUABLE",
            "change_reason": (
                "strictly prior coherent snapshot compared"
                if delta
                else "no eligible earlier snapshot is available"
            ),
            "features": {
                "max_pain.aggregate.short_long_liquidity_ratio": 2.0,
                "max_pain.delta.upside_liquidity_usd_trend": 2.0,
            },
            **provenance,
        },
    }


def _evaluation(feature: str) -> dict:
    return {
        "status": "MATCHED",
        "matched": True,
        "reason": "all formula conditions passed",
        "features": {feature: 2.0},
        "condition_results": [
            {
                "feature": feature,
                "operator": ">=",
                "expected": 1.0,
                "actual": 2.0,
                "available": True,
                "passed": True,
            }
        ],
    }


def run() -> None:
    current_feature = "max_pain.aggregate.short_long_liquidity_ratio"
    formula = _formula(current_feature)
    event = _event()
    row = _row(delta=False)
    snapshot = research_formula_worker._shadow_snapshot(
        formula=formula,
        event=event,
        row=row,
        evaluation=_evaluation(current_feature),
    )
    assert snapshot["snapshot_policy_version"] == (
        research_formula_store._SHADOW_INPUT_SNAPSHOT_POLICY_VERSION
    )
    assert snapshot["decision_cohort_policy_version"] == (
        research_formula_worker._DECISION_COHORT_POLICY_VERSION
    )
    assert snapshot["formula_key_features"] == {current_feature: 2.0}
    assert "snapshot_set_id" not in snapshot["formula_key_features"]
    audit = snapshot["max_pain_provenance"]
    assert audit["condition_features"] == [current_feature]
    assert audit["provenance"]["current"]["snapshot_set_id"] == 2
    assert audit["provenance"]["previous"] is None
    assert len(
        audit["provenance"]["current"]["row_payload_sha256"]
    ) == 7
    compatible, reason = research_formula_store._max_pain_snapshot_contract(
        formula,
        snapshot,
        decision_time_utc=event["alert_time_utc"],
        symbol="BTC",
    )
    assert compatible is True and "complete" in reason
    missing_cohort_policy = deepcopy(snapshot)
    missing_cohort_policy.pop("decision_cohort_policy_version")
    assert research_formula_store._max_pain_snapshot_contract(
        formula,
        missing_cohort_policy,
        decision_time_utc=event["alert_time_utc"],
        symbol="BTC",
    )[0] is False

    current_from_delta_capable_row = research_formula_worker._shadow_snapshot(
        formula=formula,
        event=event,
        row=_row(delta=True),
        evaluation=_evaluation(current_feature),
    )
    projected = current_from_delta_capable_row["max_pain_provenance"]
    assert projected["provenance"]["previous"] is None
    assert projected["provenance"]["used_for_delta"] is False
    assert research_formula_store._max_pain_snapshot_contract(
        formula,
        current_from_delta_capable_row,
        decision_time_utc=event["alert_time_utc"],
        symbol="BTC",
    )[0] is True

    cohort_key, anchor = research_formula_worker._decision_cohort(
        formula=formula,
        event=event,
        snapshot=snapshot,
    )
    assert anchor == BASE + timedelta(minutes=5)
    frozen_check = {
        **event,
        "input_snapshot": snapshot,
        "condition_results": snapshot["condition_results"],
        "evaluation_status": "MATCHED",
        "matched": True,
        "decision_cohort_key": cohort_key,
        "decision_anchor_time_utc": anchor,
    }
    assert research_formula_store._max_pain_shadow_check_contract(
        formula, frozen_check
    )[0] is True
    assert research_formula_store._max_pain_shadow_check_contract(
        formula,
        {**frozen_check, "decision_cohort_key": "0" * 64},
    )[0] is False
    alternate_row = deepcopy(row)
    alternate_provenance = alternate_row["max_pain_features"]["provenance"]
    alternate_current = alternate_provenance["current"]
    alternate_current["snapshot_set_id"] = 3
    alternate_current["snapshot_key"] = "d" * 64
    alternate_row["max_pain_features"]["provenance_sha256"] = (
        research_max_pain_archive.canonical_provenance_sha256(
            alternate_provenance
        )
    )
    alternate_snapshot = research_formula_worker._shadow_snapshot(
        formula=formula,
        event=event,
        row=alternate_row,
        evaluation=_evaluation(current_feature),
    )
    alternate_key, alternate_anchor = research_formula_worker._decision_cohort(
        formula=formula,
        event=event,
        snapshot=alternate_snapshot,
    )
    assert alternate_anchor == anchor
    assert alternate_key != cohort_key

    malformed_snapshot = deepcopy(snapshot)
    malformed_audit = malformed_snapshot["max_pain_provenance"]
    malformed_provenance = malformed_audit["provenance"]
    malformed_provenance["current"]["available_at_utc"] = "bad"
    malformed_audit["provenance_sha256"] = (
        research_max_pain_archive.canonical_provenance_sha256(
            malformed_provenance
        )
    )
    assert research_formula_store._max_pain_snapshot_contract(
        formula,
        malformed_snapshot,
        decision_time_utc=event["alert_time_utc"],
        symbol="BTC",
    )[0] is False
    malformed_key, malformed_anchor = research_formula_worker._decision_cohort(
        formula=formula,
        event=event,
        snapshot=malformed_snapshot,
    )
    assert len(malformed_key) == 64
    assert malformed_anchor == BASE
    assert research_formula_store._max_pain_shadow_check_contract(
        formula,
        {
            **event,
            "input_snapshot": malformed_snapshot,
            "decision_cohort_key": malformed_key,
            "decision_anchor_time_utc": malformed_anchor,
        },
    )[0] is False

    non_max_pain = _formula("latest.price_oi.available")
    plain_snapshot = research_formula_worker._shadow_snapshot(
        formula=non_max_pain,
        event=event,
        row=row,
        evaluation=_evaluation("latest.price_oi.available"),
    )
    assert "max_pain_provenance" not in plain_snapshot

    legacy = _formula(current_feature, schema="research-formula-v5-safe-replay")
    legacy_snapshot = research_formula_worker._shadow_snapshot(
        formula=legacy,
        event=event,
        row=row,
        evaluation=_evaluation(current_feature),
    )
    assert "max_pain_provenance" not in legacy_snapshot
    assert research_formula_store._max_pain_snapshot_contract(
        legacy,
        legacy_snapshot,
        decision_time_utc=event["alert_time_utc"],
        symbol="BTC",
    )[0] is True

    delta_feature = "max_pain.delta.upside_liquidity_usd_trend"
    delta_formula = _formula(delta_feature)
    missing_previous = research_formula_worker._shadow_snapshot(
        formula=delta_formula,
        event=event,
        row=row,
        evaluation=_evaluation(delta_feature),
    )
    assert research_formula_store._max_pain_snapshot_contract(
        delta_formula,
        missing_previous,
        decision_time_utc=event["alert_time_utc"],
        symbol="BTC",
    )[0] is False

    delta_row = _row(delta=True)
    delta_snapshot = research_formula_worker._shadow_snapshot(
        formula=delta_formula,
        event=event,
        row=delta_row,
        evaluation=_evaluation(delta_feature),
    )
    assert research_formula_store._max_pain_snapshot_contract(
        delta_formula,
        delta_snapshot,
        decision_time_utc=event["alert_time_utc"],
        symbol="BTC",
    )[0] is True

    tampered = dict(delta_snapshot)
    tampered_audit = dict(delta_snapshot["max_pain_provenance"])
    tampered_provenance = dict(tampered_audit["provenance"])
    tampered_provenance["previous_gap_minutes"] = 31.0
    tampered_audit["provenance"] = tampered_provenance
    tampered["max_pain_provenance"] = tampered_audit
    assert research_formula_store._max_pain_snapshot_contract(
        delta_formula,
        tampered,
        decision_time_utc=event["alert_time_utc"],
        symbol="BTC",
    )[0] is False

    print("research formula worker Max-Pain self-test: PASS")


if __name__ == "__main__":
    run()
