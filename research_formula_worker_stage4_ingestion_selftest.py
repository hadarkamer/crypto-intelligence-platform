"""Database-free checks for the Stage-4 Discovery ingestion boundary."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import os

import research_formula_worker as worker_module


SLOT = datetime(2026, 9, 4, 12, 0, tzinfo=timezone.utc)
DUE_AT = SLOT + timedelta(seconds=worker_module._DISCOVERY_SLOT_GRACE_SECONDS)


@contextmanager
def _reader_database_url(value: str | None):
    name = worker_module.research_signal_formula_exploration_reader.DATABASE_URL_ENV
    previous = os.environ.get(name)
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = previous


class _Corpus:
    def __init__(self, *, reader_ready: bool = False) -> None:
        self.reader_ready = reader_ready

    def to_dict(self) -> dict:
        available_outcomes = 3 if self.reader_ready else 2
        return {
            "attestation_receipt_sha256": "a" * 64,
            "analysis_as_of_utc": "2026-09-04T12:06:00+00:00",
            "database_snapshot_id": "00000001-00000001-1",
            "source_attestation": {
                "source_contract_version": "selftest-authoritative-v1",
                "source_catalog_sha256": "b" * 64,
                "outcomes_view_definition_sha256": "c" * 64,
                "outcomes_stage4_source_catalog_sha256": "b" * 64,
                "no_signal_outcomes_view_definition_sha256": "d" * 64,
                "no_signal_outcomes_stage4_source_catalog_sha256": "b" * 64,
                "no_signal_outcomes_raw_catalog_sha256": "e" * 64,
                "no_signal_outcomes_trigger_catalog_sha256": "f" * 64,
                "no_signal_reference_hash_contract_version": (
                    "stage4-no-signal-reference-receipt-hash-v1"
                ),
                "no_signal_outcome_hash_contract_version": (
                    "stage4-no-signal-outcome-payload-hash-v1"
                ),
            },
            "counts": {
                "projections": 3,
                "stage4_events": 5,
                "signal_events": 2,
                "wave_rows": 9,
                "outcome_rows": available_outcomes,
                "observations": 3,
                "available_outcomes": available_outcomes,
                "unavailable_outcomes": 3 - available_outcomes,
                "explicit_no_signal_observations": 1,
                "distinct_btc_parent_movements": 2,
            },
            "cursor": {
                "order": (
                    "projection_decision_time_utc DESC, projection_event_id DESC"
                ),
                "before": None,
                "next": {
                    "projection_decision_time_utc": (
                        "2026-09-01T12:00:00+00:00"
                    ),
                    "projection_event_id": 101,
                },
                "has_more": True,
            },
            "blockers": [] if self.reader_ready else ["OUTCOME_UNAVAILABLE"],
            "ready_for_candidate_search": self.reader_ready,
            "formula_registry_effect": "NONE",
            "authority_effect": "NONE",
            "delivery_channel": "NONE",
            "live_eligible": False,
            "telegram_delivery_allowed": False,
            "trade_execution_allowed": False,
            "observations": [
                {
                    "projection_event_id": 101,
                    "projection_decision_time_utc": (
                        "2026-09-01T12:00:00+00:00"
                    ),
                    "source_event_ids": [501],
                    "wave_binding": {
                        "status": "BOUND",
                        "btc_parent_movement_id": "parent-a",
                    },
                    "outcome": {"status": "AVAILABLE"},
                },
                {
                    "projection_event_id": 102,
                    "projection_decision_time_utc": (
                        "2026-09-02T12:00:00+00:00"
                    ),
                    "source_event_ids": [502],
                    "wave_binding": {
                        "status": "BOUND",
                        "btc_parent_movement_id": "parent-a",
                    },
                    "outcome": {"status": "AVAILABLE"},
                },
                {
                    "projection_event_id": 103,
                    "projection_decision_time_utc": (
                        "2026-09-03T12:00:00+00:00"
                    ),
                    "source_event_ids": [],
                    "wave_binding": {
                        "status": "BOUND",
                        "btc_parent_movement_id": "parent-b",
                    },
                    "outcome": (
                        {"status": "AVAILABLE"}
                        if self.reader_ready
                        else {
                            "status": "UNAVAILABLE",
                            "reason": "OUTCOME_UNAVAILABLE",
                        }
                    ),
                },
            ],
        }


def _search_result(*, horizon_minutes: int) -> dict:
    candidate = {
        "candidate_key": "d" * 64,
        "candidate_schema_version": "selftest-candidate-v1",
        "engine_version": "selftest-search-v1",
        "feature_schema_version": "selftest-features-v1",
        "label_policy_version": "selftest-label-v1",
        "independence_policy_version": "selftest-wave-v1",
        "direction": "LONG",
        "horizon_minutes": horizon_minutes,
        "conditions": [
            {
                "feature": "stage4.max_pain.confirmed",
                "operator": "==",
                "value": True,
            }
        ],
        "formula_text": "LONG WHEN stage4.max_pain.confirmed == true",
        "condition_source_closure": {
            "stage4.max_pain.confirmed": ["COINGLASS_MAX_PAIN"]
        },
        "condition_evidence_sources": ["COINGLASS_MAX_PAIN"],
        "raw_match_count": 9,
        "match_set_sha256": "e" * 64,
        "occurrence_counts": {
            "independent_parent_movements_seen": 5,
            "completed": 5,
            "pending_horizon": 0,
            "mature_outcome_unavailable": 0,
            "wave_unbound_matches": 0,
        },
        "metrics": {
            "sample_size": 5,
            "hit_rate_pct": 100.0,
            "accepted_paths": ["PROBABILITY"],
        },
        "accepted_paths": ["PROBABILITY"],
        "experimental_formula_eligible": True,
        "eligibility_gate": {
            "atomic": True,
            "minimum_independent_occurrences": 5,
            "independence_unit": "DISTINCT_BTC_PARENT_MARKET_MOVEMENT",
            "passed": True,
            "separate_later_probability_gate": False,
        },
        "multiple_testing": {
            "decision_effect": "DISCLOSURE_ONLY_EXPERIMENTAL",
            "eligibility_changed": False,
            "probability_q_value": 0.5,
            "asymmetry_q_value": 0.5,
        },
        "experimental_caveats": [
            "NO_CONTROL_RELATIVE_CLAIM",
            "NO_HOLDOUT_CLAIM",
        ],
        "display_equivalent_candidates": 1,
        "display_equivalent_candidate_keys": ["d" * 64],
        "formula_registry_effect": "NONE",
        "authority_effect": "NONE",
        "delivery_channel": "NONE",
        "live_eligible": False,
        "telegram_delivery_allowed": False,
        "trade_execution_allowed": False,
    }
    return {
        "status": "ELIGIBLE_EXPERIMENTAL_CANDIDATES_FOUND",
        "ready_for_candidate_search": True,
        "search_receipt_sha256": "f" * 64,
        "engine_version": "selftest-search-v1",
        "candidate_schema_version": "selftest-candidate-v1",
        "feature_schema_version": "selftest-features-v1",
        "label_policy_version": "selftest-label-v1",
        "independence_policy_version": "selftest-wave-v1",
        "analysis_as_of_utc": "2026-09-04T12:06:00+00:00",
        "horizon_minutes": horizon_minutes,
        "qualifying_favorable_move_pct": 1.0,
        "counts": {
            "observations": 3,
            "candidates_evaluated": 1,
            "display_candidates": 1,
            "eligible_experimental_candidates": 1,
        },
        "search_budget_exhausted": False,
        "atomic_eligibility": {
            "minimum_independent_occurrences": 5,
            "independence_unit": "DISTINCT_BTC_PARENT_MARKET_MOVEMENT",
            "separate_later_probability_gate": False,
        },
        "statistical_scope": {
            "controls_evaluated": False,
            "holdout_evaluated": False,
            "multiple_testing_effect": "DISCLOSURE_ONLY_EXPERIMENTAL",
        },
        "candidates": [candidate],
        "eligible_candidates": [candidate],
        "formula_registry_effect": "NONE",
        "authority_effect": "NONE",
        "delivery_channel": "NONE",
        "live_eligible": False,
        "telegram_delivery_allowed": False,
        "trade_execution_allowed": False,
    }


def _forbidden(*_args, **_kwargs):
    raise AssertionError(
        "Stage-4 ingestion must not discover, persist or deliver a formula"
    )


def _expect_value_error(callback, contains: str) -> None:
    try:
        callback()
    except ValueError as exc:
        assert contains in str(exc)
    else:
        raise AssertionError("expected the Stage-4 receipt to fail closed")


def _check_authority_boundary_is_attested() -> None:
    base = _Corpus().to_dict()

    technically_ready = dict(base)
    technically_ready["ready_for_candidate_search"] = True
    receipt = worker_module._stage4_corpus_observability_receipt(
        technically_ready,
        horizon_minutes=60,
        schedule_slot_utc=SLOT,
        due_at_utc=DUE_AT,
        duration_ms=1,
    )
    assert receipt["reader_ready_for_candidate_search"] is True
    assert receipt["ready_for_candidate_search"] is False
    assert receipt["source_attestation"] == technically_ready[
        "source_attestation"
    ]

    elevated = dict(base)
    elevated["telegram_delivery_allowed"] = True
    _expect_value_error(
        lambda: worker_module._stage4_corpus_observability_receipt(
            elevated,
            horizon_minutes=60,
            schedule_slot_utc=SLOT,
            due_at_utc=DUE_AT,
            duration_ms=1,
        ),
        "authority boundary",
    )

    false_like = dict(base)
    false_like["ready_for_candidate_search"] = 0
    _expect_value_error(
        lambda: worker_module._stage4_corpus_observability_receipt(
            false_like,
            horizon_minutes=60,
            schedule_slot_utc=SLOT,
            due_at_utc=DUE_AT,
            duration_ms=1,
        ),
        "readiness must be boolean",
    )

    malformed_blockers = dict(base)
    malformed_blockers["blockers"] = "NOT_A_SEQUENCE_OF_BLOCKERS"
    _expect_value_error(
        lambda: worker_module._stage4_corpus_observability_receipt(
            malformed_blockers,
            horizon_minutes=60,
            schedule_slot_utc=SLOT,
            due_at_utc=DUE_AT,
            duration_ms=1,
        ),
        "blockers",
    )

    inconsistent_parent_count = dict(base)
    inconsistent_parent_count["counts"] = dict(base["counts"])
    inconsistent_parent_count["counts"]["distinct_btc_parent_movements"] = 3
    _expect_value_error(
        lambda: worker_module._stage4_corpus_observability_receipt(
            inconsistent_parent_count,
            horizon_minutes=60,
            schedule_slot_utc=SLOT,
            due_at_utc=DUE_AT,
            duration_ms=1,
        ),
        "parent-movement count",
    )


def _check_missing_configuration() -> None:
    original_reader = (
        worker_module.research_signal_formula_exploration_reader
        .load_authoritative_stage4_corpus
    )
    calls = []
    reader = worker_module.research_signal_formula_exploration_reader
    candidate_search = worker_module.research_stage4_candidate_search
    original_search = candidate_search.search_experimental_candidates
    reader.load_authoritative_stage4_corpus = (
        lambda **kwargs: calls.append(kwargs) or _Corpus()
    )
    candidate_search.search_experimental_candidates = _forbidden
    try:
        with _reader_database_url(None):
            worker = worker_module.FormulaResearchWorker()
            receipt = worker._ingest_authoritative_stage4_corpus_once(
                60,
                schedule_slot_utc=SLOT,
                due_at_utc=DUE_AT,
            )
            assert receipt["status"] == "CONFIGURATION_REQUIRED"
            assert receipt["formula_registry_effect"] == "NONE"
            assert receipt["telegram_delivery_allowed"] is False
            assert calls == []
            reused = worker._ingest_authoritative_stage4_corpus_once(
                60,
                schedule_slot_utc=SLOT,
                due_at_utc=DUE_AT,
            )
            assert reused["same_slot_deduplicated"] is True
            assert calls == []
            status = worker.status()["stage4_authoritative_ingestion"]
            assert status["status"] == "CONFIGURATION_REQUIRED"
            assert status["configured"] is False
            assert status["receipts_by_horizon"]["60"]["status"] == (
                "CONFIGURATION_REQUIRED"
            )
    finally:
        reader.load_authoritative_stage4_corpus = original_reader
        candidate_search.search_experimental_candidates = original_search


def _check_success_and_same_slot_dedupe() -> None:
    reader = worker_module.research_signal_formula_exploration_reader
    candidate_search = worker_module.research_stage4_candidate_search
    store = worker_module.research_formula_store
    engine = worker_module.research_formula_engine
    original_reader = reader.load_authoritative_stage4_corpus
    original_search = candidate_search.search_experimental_candidates
    original_discover = engine.discover_formulas
    original_persist = store.persist_discovery_run
    original_pending = store.load_pending_live_deliveries
    calls = []
    search_calls = []

    def load_corpus(**kwargs):
        calls.append(kwargs)
        return _Corpus(reader_ready=True)

    def run_search(observations, **kwargs):
        search_calls.append(
            {
                "observations": observations,
                **kwargs,
            }
        )
        return _search_result(horizon_minutes=int(kwargs["horizon_minutes"]))

    reader.load_authoritative_stage4_corpus = load_corpus
    candidate_search.search_experimental_candidates = run_search
    engine.discover_formulas = _forbidden
    store.persist_discovery_run = _forbidden
    store.load_pending_live_deliveries = _forbidden
    try:
        with _reader_database_url(
            "postgresql://stage4_reader:secret@database.example/research"
        ):
            worker = worker_module.FormulaResearchWorker()
            receipt = worker._ingest_authoritative_stage4_corpus_once(
                240,
                schedule_slot_utc=SLOT,
                due_at_utc=DUE_AT,
            )
            assert receipt["status"] == "INGESTED_AND_SEARCHED"
            assert receipt["dataset_kind"] == (
                "authoritative_stage4_wave_closed_path_v1"
            )
            assert receipt["corpus_attestation_receipt_sha256"] == "a" * 64
            assert receipt["source_attestation"]["source_catalog_sha256"] == (
                "b" * 64
            )
            assert receipt["counts"] == {
                "observations": 3,
                "labeled_observations": 3,
                "wave_rows": 9,
                "wave_bound_observations": 3,
                "distinct_btc_parent_movements": 2,
                "distinct_labeled_btc_parent_movements": 2,
                "projections": 3,
                "stage4_events": 5,
                "signal_events": 2,
                "outcome_rows": 3,
            }
            assert receipt["source_bounds"] == {
                "max_projection_event_id": 103,
                "max_projection_decision_time_utc": (
                    "2026-09-03T12:00:00+00:00"
                ),
                "max_source_event_id": 502,
            }
            assert receipt["blockers"] == []
            assert "CANDIDATE_SEARCH_ADAPTER_NOT_IMPLEMENTED" not in receipt[
                "blockers"
            ]
            assert "CANDIDATE_SEARCH_NOT_YET_RUN" not in receipt["blockers"]
            assert "EXPERIMENTAL_ELIGIBILITY_GATE_NOT_EVALUATED" not in receipt[
                "blockers"
            ]
            assert receipt["reader_ready_for_candidate_search"] is True
            assert receipt["ready_for_candidate_search"] is True
            assert receipt["formula_registry_effect"] == "NONE"
            gate = receipt["candidate_eligibility_gate"]
            assert gate["policy_version"] == "experimental-formula-eligibility-v1"
            assert gate["minimum_independent_occurrences"] == (
                worker_module.research_signal_formula_exploration
                .EXPLORATION_MIN_BTC_PARENT_MOVEMENTS
            )
            assert gate["separate_later_probability_gate"] is False
            assert gate["status"] == "EVALUATED"
            assert gate["eligible_experimental_candidates"] == 1
            search_receipt = receipt["candidate_search"]
            assert search_receipt["ran"] is True
            assert search_receipt["ready_for_candidate_search"] is True
            assert search_receipt["counts"][
                "eligible_experimental_candidates"
            ] == 1
            assert len(search_receipt["candidates"]) == 1
            candidate = search_receipt["candidates"][0]
            assert candidate["experimental_formula_eligible"] is True
            assert candidate["occurrence_counts"]["completed"] == 5
            assert candidate["accepted_paths"] == ["PROBABILITY"]
            assert candidate["multiple_testing"]["probability_q_value"] == 0.5
            assert candidate["multiple_testing"]["eligibility_changed"] is False
            assert candidate["formula_registry_effect"] == "NONE"
            assert candidate["telegram_delivery_allowed"] is False
            assert calls == [
                {
                    "horizon_minutes": 240,
                    "lookback_days": worker_module._STAGE4_CORPUS_LOOKBACK_DAYS,
                    "projection_limit": (
                        worker_module._STAGE4_CORPUS_PROJECTION_LIMIT
                    ),
                    "before_cursor": None,
                    "database_url": (
                        "postgresql://stage4_reader:secret@database.example/research"
                    ),
                }
            ]
            assert len(search_calls) == 1
            assert search_calls[0]["observations"] == _Corpus(
                reader_ready=True
            ).to_dict()["observations"]
            assert search_calls[0]["horizon_minutes"] == 240
            assert search_calls[0]["analysis_as_of_utc"] == (
                "2026-09-04T12:06:00+00:00"
            )

            reused = worker._ingest_authoritative_stage4_corpus_once(
                240,
                schedule_slot_utc=SLOT,
                due_at_utc=DUE_AT,
            )
            assert reused["same_slot_deduplicated"] is True
            assert len(calls) == 1
            assert len(search_calls) == 1

            next_slot = SLOT + timedelta(hours=4)
            next_due = next_slot + timedelta(
                seconds=worker_module._DISCOVERY_SLOT_GRACE_SECONDS
            )
            worker._ingest_authoritative_stage4_corpus_once(
                240,
                schedule_slot_utc=next_slot,
                due_at_utc=next_due,
            )
            assert len(calls) == 2
            assert len(search_calls) == 2
            status = worker.status()["stage4_authoritative_ingestion"]
            assert status["status"] == "CANDIDATE_SEARCH_OBSERVED"
            assert status["ready_for_candidate_search"] is True
            assert status["candidate_eligibility_gate"]["status"] == (
                "EVALUATED_BY_HORIZON"
            )
            assert status["receipts_by_horizon"]["240"]["status"] == (
                "INGESTED_AND_SEARCHED"
            )
            assert worker.metrics.stage4_ingestion_attempts == 2
            assert worker.metrics.stage4_ingestion_successes == 2
            assert worker.metrics.stage4_ingestion_same_slot_reuses == 1
            assert worker.metrics.stage4_candidate_search_attempts == 2
            assert worker.metrics.stage4_candidate_search_successes == 2
            assert worker.metrics.stage4_candidate_search_failures == 0
    finally:
        reader.load_authoritative_stage4_corpus = original_reader
        candidate_search.search_experimental_candidates = original_search
        engine.discover_formulas = original_discover
        store.persist_discovery_run = original_persist
        store.load_pending_live_deliveries = original_pending


def _check_search_does_not_override_reader_not_ready() -> None:
    reader = worker_module.research_signal_formula_exploration_reader
    candidate_search = worker_module.research_stage4_candidate_search
    original_reader = reader.load_authoritative_stage4_corpus
    original_search = candidate_search.search_experimental_candidates

    reader.load_authoritative_stage4_corpus = lambda **_kwargs: _Corpus(
        reader_ready=False
    )
    candidate_search.search_experimental_candidates = (
        lambda _observations, **kwargs: _search_result(
            horizon_minutes=int(kwargs["horizon_minutes"])
        )
    )
    try:
        with _reader_database_url(
            "postgresql://stage4_reader:secret@database.example/research"
        ):
            worker = worker_module.FormulaResearchWorker()
            receipt = worker._ingest_authoritative_stage4_corpus_once(
                240,
                schedule_slot_utc=SLOT,
                due_at_utc=DUE_AT,
            )
            assert receipt["status"] == "INGESTED_AND_SEARCHED"
            assert receipt["reader_ready_for_candidate_search"] is False
            assert receipt["candidate_search"]["ready_for_candidate_search"] is True
            assert receipt["ready_for_candidate_search"] is False
            assert "OUTCOME_UNAVAILABLE" in receipt["blockers"]
            assert "CANDIDATE_SEARCH_ADAPTER_NOT_IMPLEMENTED" not in receipt[
                "blockers"
            ]
            assert "CANDIDATE_SEARCH_NOT_YET_RUN" not in receipt["blockers"]
            assert "EXPERIMENTAL_ELIGIBILITY_GATE_NOT_EVALUATED" not in receipt[
                "blockers"
            ]
            assert worker.metrics.stage4_candidate_search_successes == 1
            status = worker.status()["stage4_authoritative_ingestion"]
            assert status["ready_for_candidate_search"] is False
            assert status["candidate_eligibility_gate"][
                "searched_horizons_minutes"
            ] == [240]
            assert status["candidate_eligibility_gate"][
                "ready_horizons_minutes"
            ] == []
    finally:
        reader.load_authoritative_stage4_corpus = original_reader
        candidate_search.search_experimental_candidates = original_search


def _check_reader_failure_is_fail_open() -> None:
    reader = worker_module.research_signal_formula_exploration_reader
    store = worker_module.research_formula_store
    engine = worker_module.research_formula_engine
    original_reader = reader.load_authoritative_stage4_corpus
    original_lock = store.discovery_horizon_lock
    original_state = store.load_discovery_schedule_state
    original_discover = engine.discover_formulas
    original_persist = store.persist_discovery_run

    @contextmanager
    def acquired(_horizon):
        yield True

    reader.load_authoritative_stage4_corpus = (
        lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeError("selftest authoritative reader failure")
        )
    )
    store.discovery_horizon_lock = acquired
    store.load_discovery_schedule_state = lambda _horizon: {
        "last_slot_utc": SLOT,
    }
    engine.discover_formulas = _forbidden
    store.persist_discovery_run = _forbidden
    try:
        with _reader_database_url(
            "postgresql://stage4_reader:secret@database.example/research"
        ):
            worker = worker_module.FormulaResearchWorker()
            result = worker.run_discovery_horizon_once(
                60,
                schedule_slot_utc=SLOT,
                due_at_utc=DUE_AT,
            )
            assert result["status"] == "SKIPPED_ALREADY_TERMINAL"
            receipt = worker.status()["stage4_authoritative_ingestion"][
                "receipts_by_horizon"
            ]["60"]
            assert receipt["status"] == "FAILED"
            assert receipt["formula_registry_effect"] == "NONE"
            assert receipt["telegram_delivery_allowed"] is False
            assert worker.metrics.stage4_ingestion_failures == 1
            assert "selftest authoritative reader failure" in (
                worker.metrics.last_stage4_ingestion_error or ""
            )
    finally:
        reader.load_authoritative_stage4_corpus = original_reader
        store.discovery_horizon_lock = original_lock
        store.load_discovery_schedule_state = original_state
        engine.discover_formulas = original_discover
        store.persist_discovery_run = original_persist


def _check_candidate_search_failure_is_fail_open() -> None:
    reader = worker_module.research_signal_formula_exploration_reader
    candidate_search = worker_module.research_stage4_candidate_search
    store = worker_module.research_formula_store
    engine = worker_module.research_formula_engine
    original_reader = reader.load_authoritative_stage4_corpus
    original_search = candidate_search.search_experimental_candidates
    original_discover = engine.discover_formulas
    original_persist = store.persist_discovery_run
    original_pending = store.load_pending_live_deliveries

    reader.load_authoritative_stage4_corpus = lambda **_kwargs: _Corpus(
        reader_ready=False
    )
    candidate_search.search_experimental_candidates = (
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("selftest Stage-4 candidate search failure")
        )
    )
    engine.discover_formulas = _forbidden
    store.persist_discovery_run = _forbidden
    store.load_pending_live_deliveries = _forbidden
    try:
        with _reader_database_url(
            "postgresql://stage4_reader:secret@database.example/research"
        ):
            worker = worker_module.FormulaResearchWorker()
            receipt = worker._ingest_authoritative_stage4_corpus_once(
                60,
                schedule_slot_utc=SLOT,
                due_at_utc=DUE_AT,
            )
            assert receipt["status"] == "INGESTED_SEARCH_FAILED"
            assert receipt["candidate_search"]["status"] == "FAILED"
            assert receipt["candidate_search"]["ran"] is False
            assert receipt["ready_for_candidate_search"] is False
            assert "CANDIDATE_SEARCH_FAILED" in receipt["blockers"]
            assert "CANDIDATE_SEARCH_NOT_YET_RUN" not in receipt["blockers"]
            assert "CANDIDATE_SEARCH_ADAPTER_NOT_IMPLEMENTED" not in receipt[
                "blockers"
            ]
            assert "EXPERIMENTAL_ELIGIBILITY_GATE_NOT_EVALUATED" in receipt[
                "blockers"
            ]
            assert receipt["formula_registry_effect"] == "NONE"
            assert receipt["telegram_delivery_allowed"] is False
            assert receipt["trade_execution_allowed"] is False
            assert worker.metrics.stage4_ingestion_successes == 1
            assert worker.metrics.stage4_candidate_search_attempts == 1
            assert worker.metrics.stage4_candidate_search_successes == 0
            assert worker.metrics.stage4_candidate_search_failures == 1
            assert "candidate search failure" in (
                worker.metrics.last_stage4_candidate_search_error or ""
            )
    finally:
        reader.load_authoritative_stage4_corpus = original_reader
        candidate_search.search_experimental_candidates = original_search
        engine.discover_formulas = original_discover
        store.persist_discovery_run = original_persist
        store.load_pending_live_deliveries = original_pending


def run() -> None:
    _check_authority_boundary_is_attested()
    _check_missing_configuration()
    _check_success_and_same_slot_dedupe()
    _check_search_does_not_override_reader_not_ready()
    _check_reader_failure_is_fail_open()
    _check_candidate_search_failure_is_fail_open()


if __name__ == "__main__":
    run()
    print("research_formula_worker_stage4_ingestion_selftest: PASS")
