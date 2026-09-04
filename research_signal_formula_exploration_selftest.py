"""Deterministic and adversarial checks for the Stage4/Wave-v5 cohort."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import ast
import hashlib
from pathlib import Path

import research_signal_formula_exploration as exploration


UTC = timezone.utc
CYCLE = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)
AVAILABLE = CYCLE + timedelta(minutes=2, seconds=20)
READ_STARTED = CYCLE + timedelta(minutes=2, seconds=25)
READ_COMPLETED = CYCLE + timedelta(minutes=2, seconds=40)
DECISION = CYCLE + timedelta(minutes=3)
AS_OF = DECISION + timedelta(days=2)


def _h(label: object) -> str:
    return hashlib.sha256(str(label).encode("utf-8")).hexdigest()


def _iso(value: datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _base_event(
    event_id: int,
    event_type: str,
    *,
    symbol: str,
    direction: str,
    categories: list[str],
    engine_snapshot: dict,
    decision: datetime = DECISION,
    code_version: str = "stage6-test-code",
    runtime_session_id: str = "stage6-test-session",
) -> dict:
    return {
        "event_id": event_id,
        "schema_version": "research-event-v1",
        "event_kind": "DECISION_SAMPLE",
        "event_type": event_type,
        "alert_time_utc": _iso(decision),
        "symbol": symbol,
        "direction": direction,
        "source_side": None,
        "timeframe": None,
        "score": None,
        "current_price": None,
        "target_price": None,
        "initial_target_distance_pct": None,
        "categories": sorted(categories),
        "setup_key": _h(f"setup-{event_id}"),
        "event_fingerprint": _h(f"event-{event_id}"),
        "strategy_version": exploration.STAGE4_STRATEGY_VERSION,
        "code_version": code_version,
        "runtime_session_id": runtime_session_id,
        "capture_stage": exploration.STAGE4_CAPTURE_STAGE,
        "delivery_status": "NOT_APPLICABLE",
        "delivery_attempted_at_utc": None,
        "delivered_at_utc": None,
        "engine_snapshot": engine_snapshot,
    }


def _metadata(
    family: str,
    tier: str,
    *,
    snapshot_set_id: int = 71,
    snapshot_key: str | None = None,
) -> dict:
    key = snapshot_key or _h("snapshot-71")
    return {
        "contract_version": exploration.STAGE4_CONTRACT_VERSION,
        "signal_family": family,
        "tier": tier,
        "decision_time_utc": _iso(DECISION),
        "archive_reference": {
            "snapshot_set_id": snapshot_set_id,
            "snapshot_key": key,
        },
        "derivatives_reference": {},
        "dependency_lineage": {},
        "formula_authorized": False,
        "outcome_authorized": False,
        "telegram_delivery_allowed": False,
        "trade_execution_allowed": False,
    }


def _signal(
    event_id: int,
    event_type: str,
    *,
    symbol: str = "ETH",
    direction: str = "LONG",
    tier: str = "CONFIRMED",
    combined_sources: tuple[str, ...] = (),
) -> dict:
    family = {
        exploration.MAX_PAIN_EVENT_TYPE: "MAX_PAIN",
        exploration.MAGNET_EVENT_TYPE: "MAGNET",
        exploration.COMBINED_EVENT_TYPE: "COMBINED",
    }[event_type]
    engine = {"signal_snapshot": _metadata(family, tier)}
    if event_type == exploration.COMBINED_EVENT_TYPE:
        sources = sorted(combined_sources)
        engine.update(
            {
                "vote_count": len(sources),
                "source_families": sources,
                "source_vote_policy": "INDEPENDENT_RAW_SOURCE_FAMILIES_V1",
            }
        )
    row = _base_event(
        event_id,
        event_type,
        symbol=symbol,
        direction=direction,
        categories=(
            [
                "COINGLASS_MAX_PAIN",
                "DECISION_SAMPLE",
                "PRICE_OI",
                "SILENT",
            ]
            if event_type == exploration.COMBINED_EVENT_TYPE
            else ["DECISION_SAMPLE", "SILENT", tier]
        ),
        engine_snapshot=engine,
    )
    row.update(
        {
            "source_side": "SHORT" if direction == "LONG" else "LONG",
            "timeframe": "24h" if event_type == exploration.MAX_PAIN_EVENT_TYPE else None,
            "score": 81.5,
            "current_price": 2500.0,
            "target_price": 2600.0 if direction == "LONG" else 2400.0,
            "initial_target_distance_pct": 4.0,
        }
    )
    return row


def _archive_set() -> dict:
    return {
        "snapshot_set_id": 71,
        "snapshot_key": _h("snapshot-71"),
        "payload_sha256": _h("archive-payload-71"),
        "cycle_time_utc": _iso(CYCLE),
        "available_at_utc": _iso(AVAILABLE),
        "source": "RESEARCH_PASSIVE",
        "research_eligible": True,
    }


def _projection(
    signals: list[dict],
    *,
    evaluations: list[dict] | None = None,
    status: str = "COMPLETED",
) -> dict:
    evaluations = evaluations or [
        {"symbol": "ETH", "status": "EVALUABLE", "reason": None}
    ]
    counts = {"max_pain": 0, "magnet": 0, "combined": 0}
    for signal in signals:
        counts[
            {
                exploration.MAX_PAIN_EVENT_TYPE: "max_pain",
                exploration.MAGNET_EVENT_TYPE: "magnet",
                exploration.COMBINED_EVENT_TYPE: "combined",
            }[signal["event_type"]]
        ] += 1
    evaluated = sum(item["status"] == "EVALUABLE" for item in evaluations)
    evaluation_status = (
        "EVALUABLE"
        if evaluated == len(evaluations)
        else "PARTIAL" if evaluated else "UNEVALUABLE"
    )
    projection = {
        "status": status,
        "evaluation_status": evaluation_status,
        "reason": None,
        "snapshot_set_id": 71,
        "snapshot_key": _h("snapshot-71"),
        "set_payload_sha256": _h("archive-payload-71"),
        "available_at_utc": _iso(AVAILABLE),
        "eligible_symbols": sorted(item["symbol"] for item in evaluations),
        "symbol_evaluations": evaluations,
        "decision_time_utc": _iso(DECISION),
        "derivatives_read_started_at_utc": _iso(READ_STARTED),
        "derivatives_read_completed_at_utc": _iso(READ_COMPLETED),
        "counts": counts,
        "signal_event_count": len(signals),
        "signal_events_payload_sha256": exploration.signal_event_set_commitment(
            signals
        ),
    }
    return _base_event(
        900,
        exploration.PROJECTION_EVENT_TYPE,
        symbol="RESEARCH",
        direction="NEUTRAL",
        categories=["COMPLETED", "DECISION_SAMPLE", "SILENT"],
        engine_snapshot={
            "signal_snapshot": {
                "contract_version": exploration.STAGE4_CONTRACT_VERSION,
                "signal_family": "PROJECTION",
                "tier": "COMPLETED",
                "formula_authorized": False,
                "outcome_authorized": False,
                "telegram_delivery_allowed": False,
                "trade_execution_allowed": False,
            },
            "projection": projection,
        },
    )


def _wave_rows(
    symbol: str = "ETH",
    *,
    parent_label: str = "parent-1",
    decision: datetime | None = None,
    slot: datetime | None = None,
    suffix: str = "a",
) -> tuple[list[dict], list[dict]]:
    eligible = slot or CYCLE + timedelta(minutes=2)
    decided = decision or eligible + timedelta(seconds=15)
    memberships: list[dict] = []
    transitions: list[dict] = []
    for namespace, row_symbol, label in (
        ("SYMBOL", symbol, f"local-{symbol}-{suffix}"),
        ("BTC_PARENT", "BTC", f"parent-{parent_label}-{suffix}"),
    ):
        transition_receipt = _h(f"transition-{label}")
        stream_id = _h(f"stream-{namespace}-{row_symbol}")
        movement_id = _h(parent_label if namespace == "BTC_PARENT" else label)
        anchor_id = _h(f"anchor-{label}")
        transitions.append(
            {
                "transition_receipt_sha256": transition_receipt,
                "contract_version": exploration.WAVE_CONTRACT_VERSION,
                "stream_id": stream_id,
                "movement_id": movement_id,
                "namespace": namespace,
                "symbol": row_symbol,
                "trigger_anchor_id": anchor_id,
                "trigger_eligible_at_utc": _iso(eligible),
                "trigger_decision_time_utc": _iso(decided),
            }
        )
        memberships.append(
            {
                "membership_receipt_sha256": _h(f"membership-{label}"),
                "emitted_by_transition_receipt_sha256": transition_receipt,
                "contract_version": exploration.WAVE_CONTRACT_VERSION,
                "stream_id": stream_id,
                "movement_id": movement_id,
                "anchor_id": anchor_id,
                "anchor_receipt_sha256": _h(f"anchor-receipt-{label}"),
                "eligible_at_utc": _iso(eligible),
                "decision_time_utc": _iso(decided),
            }
        )
    return memberships, transitions


def _outcome(
    event_id: int,
    *,
    method: str | None = None,
    horizon: int = 60,
    event_time: datetime = DECISION,
    measured_at: datetime | None = None,
) -> dict:
    observed = event_time - timedelta(seconds=10)
    fetched = event_time - timedelta(seconds=5)
    return {
        "event_id": event_id,
        "horizon_minutes": horizon,
        "measured_at_utc": _iso(
            measured_at
            or event_time + timedelta(minutes=horizon, milliseconds=-1)
        ),
        "reference_price": 2500.0,
        "price_at_horizon": 2550.0,
        "raw_return_pct": 2.0,
        "directional_return_pct": 2.0,
        "max_favorable_price": 2575.0,
        "max_adverse_price": 2475.0,
        "mfe_pct": 3.0,
        "mae_pct": 1.0,
        "time_to_first_progress_seconds": 60,
        "time_to_mfe_seconds": 900,
        "path_resolution_seconds": 60,
        "path_samples": exploration._expected_path_samples(event_time, horizon),
        "outcome_method_version": method
        or exploration.STAGE4_OUTCOME_METHOD_VERSION,
        "data_quality_status": "VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES",
        "price_source": (
            "reference=reference_policy="
            + exploration.STAGE4_OUTCOME_REFERENCE_POLICY_VERSION
            + "|admission_policy="
            + exploration.STAGE4_OUTCOME_ADMISSION_POLICY_VERSION
            + "|semantics="
            + exploration.STAGE4_OUTCOME_SEMANTICS
            + "|source=binance_spot|exchange=binance|market=spot|"
            "pair=ETHUSDT|instrument=ETHUSDT|observed_at_utc="
            + observed.isoformat()
            + "|fetched_at_utc="
            + fetched.isoformat()
            + "|observed_age_seconds=10.000000|fetched_age_seconds=5.000000|"
            "snapshot_set_id=71|snapshot_key="
            + _h("snapshot-71")
            + "|path=binance_spot:ETHUSDT:1m|"
            "provenance=EXCHANGE_API_HISTORICAL_CANDLES_IMPORTED"
        ),
    }


def _no_signal_outcome(
    observation: exploration.ExplorationObservation,
    *,
    horizon: int = 60,
) -> dict:
    body = observation.to_dict()
    row = _outcome(1, horizon=horizon)
    row.pop("event_id")
    row.update(
        {
            "projection_event_id": body["projection_event_id"],
            "projection_event_fingerprint": body[
                "projection_event_fingerprint"
            ],
            "snapshot_set_id": body["snapshot_set_id"],
            "snapshot_key": body["snapshot_key"],
            "symbol": body["symbol"],
            "direction": body["direction"],
            "decision_time_utc": body["projection_decision_time_utc"],
            "absence_basis": exploration.NO_SIGNAL_ABSENCE_BASIS,
            "outcome_method_version": (
                exploration.STAGE4_NO_SIGNAL_OUTCOME_METHOD_VERSION
            ),
            "outcome_payload_sha256": _h(
                f"no-signal-{body['projection_event_id']}-"
                f"{body['symbol']}-{body['direction']}-{horizon}"
            ),
        }
    )
    row["price_source"] = row["price_source"].replace(
        exploration.STAGE4_OUTCOME_REFERENCE_POLICY_VERSION,
        exploration.STAGE4_NO_SIGNAL_OUTCOME_REFERENCE_POLICY_VERSION,
    ).replace(
        exploration.STAGE4_OUTCOME_ADMISSION_POLICY_VERSION,
        exploration.STAGE4_NO_SIGNAL_OUTCOME_ADMISSION_POLICY_VERSION,
    )
    if body["direction"] == "SHORT":
        row.update(
            {
                "directional_return_pct": -2.0,
                "max_favorable_price": 2475.0,
                "max_adverse_price": 2575.0,
                "mfe_pct": 1.0,
                "mae_pct": 3.0,
            }
        )
    return row


def _expect_value_error(callable_, contains: str) -> None:
    try:
        callable_()
    except ValueError as exc:
        assert contains in str(exc), (contains, str(exc))
    else:
        raise AssertionError(f"expected ValueError containing {contains!r}")


def _cohort_clone(
    sources: tuple[exploration.ExplorationObservation, ...],
    index: int,
    *,
    parent_index: int | None = None,
) -> tuple[exploration.ExplorationObservation, ...]:
    parent = index if parent_index is None else parent_index
    output = []
    for source in sources:
        payload = source.to_dict()
        payload.pop("observation_id")
        payload["projection_event_id"] = 10_000 + index
        payload["projection_event_fingerprint"] = _h(f"projection-clone-{index}")
        payload["wave_binding"]["btc_parent_movement_id"] = _h(
            f"parent-clone-{parent}"
        )
        payload["wave_binding"]["btc_parent_membership_receipt_sha256"] = _h(
            f"parent-membership-clone-{parent}"
        )
        payload["wave_binding"]["btc_parent_transition_receipt_sha256"] = _h(
            f"parent-transition-clone-{parent}"
        )
        output.append(exploration.ExplorationObservation._from_payload(payload))
    return tuple(output)


def run() -> None:
    descriptor = exploration.descriptor()
    assert descriptor["authority_effect"] == "NONE"
    assert descriptor["delivery_channel"] == "NONE"
    assert descriptor["live_eligible"] is False
    assert descriptor["exploration_parent_floor"] == 5
    assert descriptor["maturity_parent_floor"] == 20

    # A terminal PARTIAL projection still admits its exact EVALUABLE symbols,
    # and absence is explicit only because the complete event set is committed.
    evaluations = [
        {"symbol": "ETH", "status": "EVALUABLE", "reason": None},
        {
            "symbol": "SOL",
            "status": "UNEVALUABLE",
            "reason": "PRICE_OI_UNAVAILABLE",
        },
    ]
    empty_projection = _projection([], evaluations=evaluations)
    empty_frames = exploration.build_stage4_frames(
        empty_projection, _archive_set(), [], analysis_as_of_utc=AS_OF
    )
    assert [(row.to_dict()["symbol"], row.to_dict()["direction"]) for row in empty_frames] == [
        ("ETH", "LONG"),
        ("ETH", "SHORT"),
    ]
    for frame in empty_frames:
        row = frame.to_dict()
        assert row["explicit_no_signal"] is True
        assert row["absence_basis"] == "COMPLETED_PROJECTION_EVALUABLE_SYMBOL"
        assert all(
            value is False
            for key, value in row["features"].items()
            if key != exploration.FEATURE_COMBINED_VOTE_COUNT
        )
        assert row["features"][exploration.FEATURE_COMBINED_VOTE_COUNT] is None
        assert row["authority_effect"] == "NONE"
        assert row["formula_registry_effect"] == "NONE"
        assert row["telegram_delivery_allowed"] is False
        assert row["trade_execution_allowed"] is False

    malformed_projection = deepcopy(empty_projection)
    malformed_projection["engine_snapshot"]["projection"]["symbol_evaluations"].append(
        {"symbol": "ETH", "status": "EVALUABLE", "reason": None}
    )
    _expect_value_error(
        lambda: exploration.build_stage4_frames(
            malformed_projection, _archive_set(), [], analysis_as_of_utc=AS_OF
        ),
        "duplicate symbol evaluation",
    )
    missed = deepcopy(empty_projection)
    missed["engine_snapshot"]["projection"]["status"] = "MISSED_CAUSAL_WINDOW"
    _expect_value_error(
        lambda: exploration.build_stage4_frames(
            missed, _archive_set(), [], analysis_as_of_utc=AS_OF
        ),
        "not terminal COMPLETED",
    )

    max_pain = _signal(
        101,
        exploration.MAX_PAIN_EVENT_TYPE,
        tier="STRONG_CONFIRMED",
    )
    magnet = _signal(102, exploration.MAGNET_EVENT_TYPE)
    signal_projection = _projection([max_pain, magnet])
    assert exploration.signal_event_set_commitment(
        [max_pain, magnet]
    ) == exploration.signal_event_set_commitment([magnet, max_pain])
    positive_zero = deepcopy(max_pain)
    positive_zero["score"] = 0.0
    negative_zero = deepcopy(positive_zero)
    negative_zero["score"] = -0.0
    assert exploration._signal_event_payload(positive_zero)["score"] == (
        "0000000000000000"
    )
    assert exploration._signal_event_payload(negative_zero)["score"] == (
        "8000000000000000"
    )
    assert exploration.signal_event_set_commitment(
        [positive_zero]
    ) != exploration.signal_event_set_commitment([negative_zero])
    _expect_value_error(
        lambda: exploration.canonical_json(Decimal("9007199254740992.1")),
        "fractional Decimal",
    )
    signal_frames = exploration.build_stage4_frames(
        signal_projection,
        _archive_set(),
        [magnet, max_pain],
        analysis_as_of_utc=AS_OF,
    )
    long_frame = next(
        item for item in signal_frames if item.to_dict()["direction"] == "LONG"
    )
    features = long_frame.to_dict()["features"]
    assert features[exploration.FEATURE_MAX_PAIN_CONFIRMED] is True
    assert features[exploration.FEATURE_MAX_PAIN_STRONG] is True
    assert features[exploration.FEATURE_MAGNET_CONFIRMED] is True
    assert long_frame.to_dict()["source_families"] == ["COINGLASS_MAX_PAIN"]
    assert signal_frames == exploration.build_stage4_frames(
        signal_projection,
        _archive_set(),
        [max_pain, magnet],
        analysis_as_of_utc=AS_OF,
    )

    forged = deepcopy(max_pain)
    forged["engine_snapshot"]["signal_snapshot"]["formula_authorized"] = True
    forged_projection = _projection([forged])
    _expect_value_error(
        lambda: exploration.build_stage4_frames(
            forged_projection, _archive_set(), [forged], analysis_as_of_utc=AS_OF
        ),
        "formula_authorized",
    )
    bad_runtime = deepcopy(max_pain)
    bad_runtime["runtime_session_id"] = "other-session"
    bad_runtime_projection = _projection([bad_runtime])
    _expect_value_error(
        lambda: exploration.build_stage4_frames(
            bad_runtime_projection,
            _archive_set(),
            [bad_runtime],
            analysis_as_of_utc=AS_OF,
        ),
        "runtime identity mismatch",
    )
    incomplete_projection = _projection([max_pain])
    _expect_value_error(
        lambda: exploration.build_stage4_frames(
            incomplete_projection, _archive_set(), [], analysis_as_of_utc=AS_OF
        ),
        "committed event count",
    )

    combined = _signal(
        103,
        exploration.COMBINED_EVENT_TYPE,
        combined_sources=("COINGLASS_MAX_PAIN", "PRICE_OI"),
    )
    assert "CONFIRMED" not in combined["categories"]
    combined_frames = exploration.build_stage4_frames(
        _projection([combined]),
        _archive_set(),
        [combined],
        analysis_as_of_utc=AS_OF,
    )
    combined_features = next(
        item.to_dict()["features"]
        for item in combined_frames
        if item.to_dict()["direction"] == "LONG"
    )
    assert combined_features[exploration.FEATURE_COMBINED_CONFIRMED] is True
    assert combined_features[exploration.FEATURE_COMBINED_COINGLASS] is True
    assert combined_features[exploration.FEATURE_COMBINED_PRICE_OI] is True
    assert combined_features[exploration.FEATURE_COMBINED_FUTURES_CVD] is False
    assert combined_features[exploration.FEATURE_COMBINED_VOTE_COUNT] == 2
    bad_combined = deepcopy(combined)
    bad_combined["engine_snapshot"]["source_families"].append("SPOT_CVD")
    bad_combined["engine_snapshot"]["vote_count"] = 3
    _expect_value_error(
        lambda: exploration.build_stage4_frames(
            _projection([bad_combined]),
            _archive_set(),
            [bad_combined],
            analysis_as_of_utc=AS_OF,
        ),
        "not canonical",
    )

    memberships, transitions = _wave_rows()
    bound = exploration.bind_wave_v5(
        signal_frames,
        list(reversed(memberships)),
        list(reversed(transitions)),
        analysis_as_of_utc=AS_OF,
    )
    assert all(item.to_dict()["wave_binding"]["status"] == "BOUND" for item in bound)
    assert all(
        item.to_dict()["wave_binding"]["role"]
        == "INDEPENDENCE_ONLY_NOT_FORMULA_PREDICATE"
        for item in bound
    )
    assert bound == exploration.bind_wave_v5(
        signal_frames,
        memberships,
        transitions,
        analysis_as_of_utc=AS_OF,
    )
    future_payload = signal_frames[0].to_dict()
    future_payload.pop("observation_id")
    future_payload["projection_decision_time_utc"] = _iso(
        AS_OF + timedelta(minutes=1)
    )
    future_observation = exploration.ExplorationObservation._from_payload(
        future_payload
    )
    _expect_value_error(
        lambda: exploration.bind_wave_v5(
            [future_observation],
            memberships,
            transitions,
            analysis_as_of_utc=AS_OF,
        ),
        "after analysis_as_of_utc",
    )

    missing_parent = exploration.bind_wave_v5(
        signal_frames,
        memberships[:1],
        transitions[:1],
        analysis_as_of_utc=AS_OF,
    )
    assert {
        item.to_dict()["wave_binding"]["reason"] for item in missing_parent
    } == {"BTC_PARENT_WAVE_MEMBERSHIP_MISSING"}
    wrong_slot_memberships, wrong_slot_transitions = _wave_rows(
        slot=CYCLE + timedelta(minutes=32), suffix="wrong-slot"
    )
    wrong_slot = exploration.bind_wave_v5(
        signal_frames,
        wrong_slot_memberships,
        wrong_slot_transitions,
        analysis_as_of_utc=AS_OF,
    )
    assert {
        item.to_dict()["wave_binding"]["reason"] for item in wrong_slot
    } == {"SYMBOL_WAVE_MEMBERSHIP_MISSING"}
    late_memberships, late_transitions = _wave_rows(
        decision=DECISION + timedelta(seconds=1), suffix="late"
    )
    late = exploration.bind_wave_v5(
        signal_frames,
        late_memberships,
        late_transitions,
        analysis_as_of_utc=AS_OF,
    )
    assert {
        item.to_dict()["wave_binding"]["reason"] for item in late
    } == {"SYMBOL_WAVE_MEMBERSHIP_MISSING"}
    extra_memberships, extra_transitions = _wave_rows(suffix="duplicate")
    ambiguous = exploration.bind_wave_v5(
        signal_frames,
        memberships + extra_memberships,
        transitions + extra_transitions,
        analysis_as_of_utc=AS_OF,
    )
    assert {
        item.to_dict()["wave_binding"]["reason"] for item in ambiguous
    } == {"SYMBOL_WAVE_MEMBERSHIP_AMBIGUOUS"}

    labeled = exploration.attach_closed_outcomes(
        bound,
        [_outcome(101), _outcome(102)],
        horizon_minutes=60,
        analysis_as_of_utc=AS_OF,
    )
    assert labeled == exploration.attach_closed_outcomes(
        list(reversed(bound)),
        [_outcome(102), _outcome(101)],
        horizon_minutes=60,
        analysis_as_of_utc=AS_OF,
    )
    labeled_long = next(
        item for item in labeled if item.to_dict()["direction"] == "LONG"
    )
    labeled_short = next(
        item for item in labeled if item.to_dict()["direction"] == "SHORT"
    )
    assert labeled_long.to_dict()["outcome"]["status"] == "AVAILABLE"
    assert labeled_long.to_dict()["outcome"]["label_fields_exposed_as_features"] is False
    assert labeled_short.to_dict()["outcome"] == {
        "horizon_minutes": 60,
        "label_fields_exposed_as_features": False,
        "policy_version": exploration.OUTCOME_BINDING_POLICY_VERSION,
        "reason": "CANONICAL_NO_SIGNAL_OUTCOME_NOT_MATERIALIZED",
        "status": "OUTCOME_UNAVAILABLE",
    }
    no_signal_labeled = exploration.attach_closed_outcomes(
        empty_frames,
        [],
        no_signal_outcomes=[
            _no_signal_outcome(item) for item in empty_frames
        ],
        horizon_minutes=60,
        analysis_as_of_utc=AS_OF,
    )
    assert all(
        item.to_dict()["outcome"]["status"] == "AVAILABLE"
        and item.to_dict()["outcome"]["carrier_type"]
        == "STAGE4_NO_SIGNAL_CELL"
        and item.to_dict()["outcome"]["source_event_ids"] == []
        for item in no_signal_labeled
    )
    bound_empty = exploration.bind_wave_v5(
        empty_frames,
        memberships,
        transitions,
        analysis_as_of_utc=AS_OF,
    )
    complete_no_signal = exploration.attach_closed_outcomes(
        bound_empty,
        [],
        no_signal_outcomes=[
            _no_signal_outcome(item) for item in bound_empty
        ],
        horizon_minutes=60,
        analysis_as_of_utc=AS_OF,
    )
    complete_readiness = exploration.dataset_readiness(
        complete_no_signal,
        source_authority_attested=True,
        statistical_label_contract_implemented=True,
        wave_identity_candidate_search_implemented=True,
    )
    assert complete_readiness["ready_for_formula_effect_research"] is True
    assert complete_readiness["label_coverage_complete"] is True
    assert complete_readiness["distinct_effect_btc_parent_movements"] == 1
    assert complete_readiness["meets_exploration_parent_floor"] is False
    assert complete_readiness["blockers"] == []
    forged_cell = _no_signal_outcome(empty_frames[0])
    forged_cell["symbol"] = "SOL"
    _expect_value_error(
        lambda: exploration.attach_closed_outcomes(
            empty_frames,
            [],
            no_signal_outcomes=[forged_cell],
            horizon_minutes=60,
            analysis_as_of_utc=AS_OF,
        ),
        "outside the supplied cohort",
    )
    signal_targeted_carrier = _no_signal_outcome(empty_frames[0])
    signal_targeted_carrier.update(
        {
            "projection_event_id": long_frame.to_dict()["projection_event_id"],
            "projection_event_fingerprint": long_frame.to_dict()[
                "projection_event_fingerprint"
            ],
        }
    )
    _expect_value_error(
        lambda: exploration.attach_closed_outcomes(
            [long_frame],
            [_outcome(101), _outcome(102)],
            no_signal_outcomes=[signal_targeted_carrier],
            horizon_minutes=60,
            analysis_as_of_utc=AS_OF,
        ),
        "signal-bearing cell",
    )
    readiness = exploration.dataset_readiness(labeled)
    assert readiness["ready_for_formula_effect_research"] is False
    assert readiness["distinct_effect_btc_parent_movements"] == 0
    assert readiness["descriptive_distinct_labeled_btc_parent_movements"] == 1
    assert readiness["outcome_horizons_minutes"] == [60]
    assert "CANONICAL_NO_SIGNAL_OUTCOME_NOT_MATERIALIZED" in readiness["blockers"]
    assert "AUTHORITATIVE_SOURCE_ATTESTATION_NOT_IMPLEMENTED" in readiness["blockers"]
    assert readiness["edge_established"] is False
    assert readiness["maturity_established"] is False

    incomplete = exploration.dataset_readiness([labeled_long])
    assert incomplete["cohort_structurally_complete"] is False
    assert "INCOMPLETE_PROJECTION_COHORT" in incomplete["blockers"]
    duplicate_cell = exploration.dataset_readiness(
        [labeled_long, labeled_long, labeled_short]
    )
    assert "DUPLICATE_PROJECTION_COHORT_CELL" in duplicate_cell["blockers"]

    forged_no_signal = labeled_short.to_dict()
    forged_no_signal.pop("observation_id")
    forged_no_signal["outcome"] = deepcopy(labeled_long.to_dict()["outcome"])
    _expect_value_error(
        lambda: exploration.ExplorationObservation._from_payload(
            forged_no_signal
        ),
        "no-signal observation",
    )
    _expect_value_error(
        lambda: exploration.ExplorationObservation("0" * 64, "{}"),
        "policy mismatch",
    )

    wrong_method = exploration.attach_closed_outcomes(
        [bound[0]],
        [
            _outcome(101, method="canonical-spot-1m-ohlc-path-v3"),
            _outcome(102, method="canonical-spot-1m-ohlc-path-v3"),
        ],
        horizon_minutes=60,
        analysis_as_of_utc=AS_OF,
    )[0].to_dict()["outcome"]
    assert wrong_method["status"] == "OUTCOME_UNAVAILABLE"
    assert "outcome method" in wrong_method["reason"]

    incoherent = _outcome(101)
    incoherent["raw_return_pct"] = 99.0
    invalid_metrics = exploration.attach_closed_outcomes(
        [bound[0]],
        [incoherent, _outcome(102)],
        horizon_minutes=60,
        analysis_as_of_utc=AS_OF,
    )[0].to_dict()["outcome"]
    assert invalid_metrics["status"] == "OUTCOME_UNAVAILABLE"
    assert "path metrics are inconsistent" in invalid_metrics["reason"]

    sibling_time_mismatch = exploration.attach_closed_outcomes(
        [bound[0]],
        [
            _outcome(101),
            _outcome(
                102,
                measured_at=DECISION + timedelta(minutes=60, milliseconds=-2),
            ),
        ],
        horizon_minutes=60,
        analysis_as_of_utc=AS_OF,
    )[0].to_dict()["outcome"]
    assert sibling_time_mismatch["status"] == "OUTCOME_UNAVAILABLE"
    assert "not the final closed 1m candle" in sibling_time_mismatch["reason"]

    immature = exploration.attach_closed_outcomes(
        [bound[0]],
        [_outcome(101), _outcome(102)],
        horizon_minutes=60,
        analysis_as_of_utc=DECISION + timedelta(minutes=59),
    )[0].to_dict()["outcome"]
    assert immature["status"] == "OUTCOME_UNAVAILABLE"
    assert "horizon is not mature" in immature["reason"]

    off_minute = DECISION + timedelta(seconds=20)
    assert exploration._expected_path_samples(off_minute, 60) == 59
    off_minute_end = off_minute + timedelta(minutes=60)
    off_minute_measured = off_minute_end.replace(second=0, microsecond=0) - timedelta(
        milliseconds=1
    )
    assert exploration._normalized_outcome(
        _outcome(101, event_time=off_minute, measured_at=off_minute_measured),
        horizon_minutes=60,
        event_time=off_minute,
        analysis_as_of_utc=off_minute_end,
        direction="LONG",
        symbol="ETH",
        snapshot_set_id=71,
        snapshot_key=_h("snapshot-71"),
    )["path_samples"] == 59

    valid_features = exploration.validate_candidate_feature_set(
        [
            exploration.FEATURE_COMBINED_COINGLASS,
            exploration.FEATURE_COMBINED_PRICE_OI,
        ]
    )
    assert valid_features["deduplicated_sources"] == [
        "COINGLASS_MAX_PAIN",
        "PRICE_OI",
    ]
    assert valid_features == exploration.validate_candidate_feature_set(
        [
            exploration.FEATURE_COMBINED_PRICE_OI,
            exploration.FEATURE_COMBINED_COINGLASS,
        ]
    )
    _expect_value_error(
        lambda: exploration.validate_candidate_feature_set(
            [
                exploration.FEATURE_MAX_PAIN_CONFIRMED,
                exploration.FEATURE_MAGNET_CONFIRMED,
            ]
        ),
        "double-counts",
    )
    _expect_value_error(
        lambda: exploration.validate_candidate_feature_set(
            [
                exploration.FEATURE_COMBINED_CONFIRMED,
                exploration.FEATURE_COMBINED_PRICE_OI,
            ]
        ),
        "double-counts",
    )
    _expect_value_error(
        lambda: exploration.validate_candidate_feature_set(["stage4.spot_cvd.vote"]),
        "forbidden",
    )

    # Parent floors are descriptive only.  A full two-direction cohort still
    # remains label-incomplete until canonical no-signal outcomes exist, and
    # source authenticity remains unattested in this pure module.
    four = [
        item
        for index in range(4)
        for item in _cohort_clone(labeled, index)
    ]
    four_status = exploration.dataset_readiness(four)
    assert four_status["cohort_structurally_complete"] is True
    assert four_status["descriptive_distinct_labeled_btc_parent_movements"] == 4
    assert four_status["distinct_effect_btc_parent_movements"] == 0
    assert four_status["meets_exploration_parent_floor"] is False
    duplicate_parent = _cohort_clone(labeled, 4, parent_index=0)
    four_plus_duplicate = exploration.dataset_readiness(
        [*four, *duplicate_parent]
    )
    assert (
        four_plus_duplicate["descriptive_distinct_labeled_btc_parent_movements"]
        == 4
    )
    assert four_plus_duplicate["distinct_effect_btc_parent_movements"] == 0
    assert four_plus_duplicate["meets_exploration_parent_floor"] is False
    five_status = exploration.dataset_readiness(
        [
            item
            for index in range(5)
            for item in _cohort_clone(labeled, index)
        ]
    )
    assert five_status["descriptive_distinct_labeled_btc_parent_movements"] == 5
    assert five_status["descriptive_reaches_exploration_parent_floor"] is False
    assert five_status["meets_exploration_parent_floor"] is False
    assert five_status["meets_maturity_parent_floor"] is False
    twenty_status = exploration.dataset_readiness(
        [
            item
            for index in range(20)
            for item in _cohort_clone(labeled, index)
        ]
    )
    assert twenty_status["descriptive_distinct_labeled_btc_parent_movements"] == 20
    assert twenty_status["descriptive_reaches_maturity_parent_floor"] is False
    assert twenty_status["meets_exploration_parent_floor"] is False
    assert twenty_status["meets_maturity_parent_floor"] is False
    assert twenty_status["edge_established"] is False
    assert twenty_status["maturity_established"] is False
    assert twenty_status["authority_effect"] == "NONE"
    assert twenty_status["live_eligible"] is False

    labeled_240 = exploration.attach_closed_outcomes(
        bound,
        [_outcome(101, horizon=240), _outcome(102, horizon=240)],
        horizon_minutes=240,
        analysis_as_of_utc=AS_OF,
    )
    mixed = exploration.dataset_readiness(
        [*_cohort_clone(labeled, 100), *_cohort_clone(labeled_240, 101)]
    )
    assert mixed["outcome_horizons_minutes"] == [60, 240]
    assert "MIXED_OUTCOME_HORIZONS" in mixed["blockers"]
    assert mixed["label_coverage_complete"] is False

    # Import boundary: standard library only, with no DB/runtime/delivery seam.
    module_path = Path(__file__).with_name("research_signal_formula_exploration.py")
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    imported_roots = {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imported_roots.update(
        (node.module or "").split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    )
    assert imported_roots <= {
        "__future__",
        "dataclasses",
        "datetime",
        "decimal",
        "hashlib",
        "json",
        "math",
        "re",
        "struct",
        "typing",
    }
    source = module_path.read_text(encoding="utf-8")
    for forbidden in (
        "research_formula_store",
        "research_formula_worker",
        "psycopg",
        "requests",
    ):
        assert forbidden not in source

    print("research_signal_formula_exploration_selftest: PASS")


if __name__ == "__main__":
    run()
