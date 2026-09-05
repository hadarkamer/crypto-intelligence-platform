"""Database-free checks for complete Stage-4 Discovery corpus ingestion."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import hashlib
import json
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
    def __init__(
        self, *, reader_ready: bool = False, horizon_minutes: int = 60
    ) -> None:
        self.reader_ready = reader_ready
        self.horizon_minutes = horizon_minutes

    def to_dict(self) -> dict:
        available_outcomes = 3 if self.reader_ready else 2
        analysis_as_of = datetime(
            2026, 9, 4, 12, 6, tzinfo=timezone.utc
        )
        page_receipt = "1" * 64
        page_chain = json.dumps(
            {
                "kind": "authoritative-full-corpus-page-chain-v1",
                "database_snapshot_id": "00000001-00000001-1",
                "page_attestation_receipts": [page_receipt],
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        page_chain_sha256 = hashlib.sha256(
            page_chain.encode("utf-8")
        ).hexdigest()
        payload = {
            "attestation_receipt_sha256": "a" * 64,
            "source_contract_version": (
                worker_module.research_signal_formula_exploration_reader
                .CORPUS_SOURCE_CONTRACT_VERSION
            ),
            "analysis_as_of_utc": analysis_as_of.isoformat(),
            "database_snapshot_id": "00000001-00000001-1",
            "request": {
                "horizon_minutes": self.horizon_minutes,
                "lookback_days": worker_module._STAGE4_CORPUS_LOOKBACK_DAYS,
                "projection_limit": (
                    worker_module._STAGE4_CORPUS_PROJECTION_LIMIT
                ),
            },
            "source_attestation": {
                "source_contract_version": "selftest-authoritative-v1",
                "analysis_as_of_utc": analysis_as_of.isoformat(),
                "database_snapshot_id": "00000001-00000001-1",
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
                "signal_outcome_rows": 2,
                "no_signal_outcome_rows": available_outcomes - 2,
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
                "next": None,
                "has_more": False,
            },
            "traversal": {
                "status": "COMPLETE",
                "single_database_snapshot": True,
                "eof_proven": True,
                "analysis_as_of_utc": analysis_as_of.isoformat(),
                "database_snapshot_id": "00000001-00000001-1",
                "page_count": 1,
                "page_size": worker_module._STAGE4_CORPUS_PROJECTION_LIMIT,
                "max_pages": (
                    worker_module.research_signal_formula_exploration_reader
                    .MAX_FULL_CORPUS_PAGES
                ),
                "max_projections": (
                    worker_module.research_signal_formula_exploration_reader
                    .MAX_FULL_CORPUS_PROJECTIONS
                ),
                "max_observations": (
                    worker_module.research_signal_formula_exploration_reader
                    .MAX_FULL_CORPUS_OBSERVATIONS
                ),
                "wall_budget_ms": worker_module._STAGE4_CORPUS_WALL_BUDGET_MS,
                "bounds": {
                    "projection_decision_time_lower_inclusive_utc": (
                        analysis_as_of
                        - timedelta(
                            days=worker_module._STAGE4_CORPUS_LOOKBACK_DAYS
                        )
                    ).isoformat(),
                    "projection_decision_time_upper_inclusive_utc": (
                        analysis_as_of
                        - timedelta(minutes=self.horizon_minutes)
                    ).isoformat(),
                },
                "page_receipts_sha256": page_chain_sha256,
                "aggregate_page_sha256": page_chain_sha256,
                "pages": [
                    {
                        "page_number": 1,
                        "before": None,
                        "next": None,
                        "has_more": False,
                        "projections": 3,
                        "observations": 3,
                        "page_attestation_receipt_sha256": page_receipt,
                    }
                ],
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
                    "observation_id": "observation-a",
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
                    "observation_id": "observation-b",
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
                    "observation_id": "observation-c",
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
        payload.pop("observations")
        observations = self.candidate_observations
        candidate_search = worker_module.research_stage4_candidate_search
        payload["observation_storage"] = {
            "format": "DETACHED_IMMUTABLE_COMPACT_TUPLE",
            "schema_version": (
                candidate_search.COMPACT_OBSERVATION_SCHEMA_VERSION
            ),
            "hash_contract_version": (
                candidate_search.COMPACT_OBSERVATION_CHAIN_HASH_VERSION
            ),
            "count": len(observations),
            "ordered_chain_sha256": (
                candidate_search.compact_observation_chain_sha256(
                    observations
                )
            ),
        }
        return payload

    @property
    def candidate_observations(self):
        candidate_search = worker_module.research_stage4_candidate_search
        parent_a = hashlib.sha256(b"parent-a").hexdigest()
        parent_b = hashlib.sha256(b"parent-b").hexdigest()
        features = candidate_search._CompactFeatureMapping(0, None)

        def outcome(available: bool):
            return candidate_search._CompactOutcome(
                status="AVAILABLE" if available else "OUTCOME_UNAVAILABLE",
                reason=None if available else "OUTCOME_UNAVAILABLE",
                horizon_minutes=self.horizon_minutes,
                path=(
                    candidate_search._CompactOutcomePath(1.0, 1.0, 0.5)
                    if available
                    else None
                ),
            )

        return tuple(
            candidate_search.CompactStage4CandidateObservation(
                observation_id=hashlib.sha256(
                    f"observation-{index}".encode("utf-8")
                ).hexdigest(),
                projection_event_id=100 + index,
                projection_decision_time_utc=(
                    f"2026-09-0{index}T12:00:00+00:00"
                ),
                symbol="ETH",
                direction="LONG",
                features=features,
                wave_binding=candidate_search._CompactWaveBinding(
                    status="BOUND",
                    reason=None,
                    btc_parent_movement_id=(parent_a if index < 3 else parent_b),
                ),
                outcome=outcome(index < 3 or self.reader_ready),
            )
            for index in (1, 2, 3)
        )

    def complete_result(self):
        reader = worker_module.research_signal_formula_exploration_reader
        return reader.CompleteAuthoritativeStage4CorpusResult._from_payload(
            self.to_dict(),
            self.candidate_observations,
        )


def _replace_traversal_pages(payload: dict, pages: list[dict]) -> dict:
    updated = dict(payload)
    updated["counts"] = dict(payload["counts"])
    updated["traversal"] = dict(payload["traversal"])
    updated["traversal"]["pages"] = [dict(page) for page in pages]
    updated["traversal"]["page_count"] = len(pages)
    chain = json.dumps(
        {
            "kind": "authoritative-full-corpus-page-chain-v1",
            "database_snapshot_id": updated["database_snapshot_id"],
            "page_attestation_receipts": [
                page["page_attestation_receipt_sha256"] for page in pages
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    updated["traversal"]["page_receipts_sha256"] = hashlib.sha256(
        chain.encode("utf-8")
    ).hexdigest()
    updated["traversal"]["aggregate_page_sha256"] = updated["traversal"][
        "page_receipts_sha256"
    ]
    return updated


def _search_result(
    *, horizon_minutes: int, input_observations=None
) -> dict:
    search = worker_module.research_stage4_candidate_search
    exploration = worker_module.research_signal_formula_exploration
    conditions = [
        {
            "feature": exploration.FEATURE_MAX_PAIN_CONFIRMED,
            "operator": "==",
            "value": True,
        }
    ]
    candidate_key = search.candidate_key_sha256(
        direction="LONG",
        horizon_minutes=horizon_minutes,
        conditions=conditions,
    )
    if input_observations is None:
        input_observations = _Corpus(
            reader_ready=True,
            horizon_minutes=horizon_minutes,
        ).candidate_observations
    candidate = {
        "candidate_key": candidate_key,
        "candidate_schema_version": search.CANDIDATE_SCHEMA_VERSION,
        "engine_version": search.ENGINE_VERSION,
        "feature_schema_version": exploration.FEATURE_SCHEMA_VERSION,
        "label_policy_version": search.LABEL_POLICY_VERSION,
        "independence_policy_version": search.INDEPENDENCE_POLICY_VERSION,
        "direction": "LONG",
        "horizon_minutes": horizon_minutes,
        "conditions": conditions,
        "formula_text": "LONG WHEN stage4.max_pain.confirmed == true",
        "condition_source_closure": {
            "stage4.max_pain.confirmed": ["COINGLASS_MAX_PAIN"]
        },
        "condition_evidence_sources": ["COINGLASS_MAX_PAIN"],
        "raw_match_count": 9,
        "match_set_sha256": "e" * 64,
        "occurrence_evidence_sha256": "a" * 64,
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
            "policy_version": search.MULTIPLE_TESTING_POLICY_VERSION,
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
        "display_equivalent_candidate_keys": [candidate_key],
        "formula_registry_effect": "NONE",
        "authority_effect": "NONE",
        "delivery_channel": "NONE",
        "live_eligible": False,
        "telegram_delivery_allowed": False,
        "trade_execution_allowed": False,
    }
    result = {
        "available": True,
        "status": "ELIGIBLE_EXPERIMENTAL_CANDIDATES_FOUND",
        "ready_for_candidate_search": True,
        "engine_version": search.ENGINE_VERSION,
        "candidate_schema_version": search.CANDIDATE_SCHEMA_VERSION,
        "feature_schema_version": exploration.FEATURE_SCHEMA_VERSION,
        "label_policy_version": search.LABEL_POLICY_VERSION,
        "independence_policy_version": search.INDEPENDENCE_POLICY_VERSION,
        "multiple_testing_policy_version": search.MULTIPLE_TESTING_POLICY_VERSION,
        "compact_observation_schema_version": (
            search.COMPACT_OBSERVATION_SCHEMA_VERSION
        ),
        "historical_threshold_source_policy_version": (
            worker_module.research_formula_acceptance.POLICY_VERSION
        ),
        "analysis_as_of_utc": "2026-09-04T12:06:00+00:00",
        "horizon_minutes": horizon_minutes,
        "input_observation_schema_version": (
            search.COMPACT_OBSERVATION_SCHEMA_VERSION
        ),
        "input_observation_hash_contract_version": (
            search.COMPACT_OBSERVATION_CHAIN_HASH_VERSION
        ),
        "input_observation_count": len(input_observations),
        "input_observation_chain_sha256": (
            search.compact_observation_chain_sha256(input_observations)
        ),
        "config": {
            "minimum_independent_occurrences": 5,
            "max_observations": 131_072,
            "max_conditions": 3,
            "max_candidates_evaluated": 256,
            "max_candidates_returned": 40,
            "wall_budget_ms": 60_000,
        },
        "qualifying_favorable_move_pct": 1.0,
        "counts": {
            "observations": 3,
            "candidates_evaluated": 1,
            "evidence_match_sets_evaluated": 1,
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
    result["search_receipt_sha256"] = search.candidate_search_receipt_sha256(
        result
    )
    return result


def _rehash_search_result(value: dict) -> dict:
    value.pop("search_receipt_sha256", None)
    value["search_receipt_sha256"] = (
        worker_module.research_stage4_candidate_search
        .candidate_search_receipt_sha256(value)
    )
    return value


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


def _assert_no_authority(payload: dict) -> None:
    assert payload["formula_registry_effect"] == "NONE"
    assert payload["authority_effect"] == "NONE"
    assert payload["delivery_channel"] == "NONE"
    assert payload["live_eligible"] is False
    assert payload["telegram_delivery_allowed"] is False
    assert payload["trade_execution_allowed"] is False


def _observe_stage4_corpus(payload: dict, **kwargs):
    horizon = int(kwargs["horizon_minutes"])
    reader_ready = payload["counts"]["available_outcomes"] == payload[
        "counts"
    ]["observations"]
    observations = _Corpus(
        reader_ready=reader_ready,
        horizon_minutes=horizon,
    ).candidate_observations
    return worker_module._stage4_corpus_observability_receipt(
        payload,
        observations=observations,
        **kwargs,
    )


def _check_authority_boundary_is_attested() -> None:
    base = _Corpus().to_dict()

    technically_ready = dict(base)
    technically_ready["ready_for_candidate_search"] = True
    receipt = _observe_stage4_corpus(
        technically_ready,
        horizon_minutes=60,
        schedule_slot_utc=SLOT,
        due_at_utc=DUE_AT,
        duration_ms=1,
    )
    assert receipt["reader_ready_for_candidate_search"] is True
    assert receipt["ready_for_candidate_search"] is False
    _assert_no_authority(receipt)
    assert receipt["source_attestation"] == technically_ready[
        "source_attestation"
    ]
    assert receipt["traversal"] == technically_ready["traversal"]
    assert receipt["traversal"]["status"] == "COMPLETE"
    assert receipt["traversal"]["single_database_snapshot"] is True

    incomplete = dict(base)
    incomplete["traversal"] = dict(base["traversal"])
    incomplete["traversal"]["status"] = "PAGE_LIMIT_EXHAUSTED"
    _expect_value_error(
        lambda: _observe_stage4_corpus(
            incomplete,
            horizon_minutes=60,
            schedule_slot_utc=SLOT,
            due_at_utc=DUE_AT,
            duration_ms=1,
        ),
        "traversal",
    )

    multiple_snapshots = dict(base)
    multiple_snapshots["traversal"] = dict(base["traversal"])
    multiple_snapshots["traversal"]["single_database_snapshot"] = False
    _expect_value_error(
        lambda: _observe_stage4_corpus(
            multiple_snapshots,
            horizon_minutes=60,
            schedule_slot_utc=SLOT,
            due_at_utc=DUE_AT,
            duration_ms=1,
        ),
        "traversal",
    )

    elevated = dict(base)
    elevated["telegram_delivery_allowed"] = True
    _expect_value_error(
        lambda: _observe_stage4_corpus(
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
        lambda: _observe_stage4_corpus(
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
        lambda: _observe_stage4_corpus(
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
        lambda: _observe_stage4_corpus(
            inconsistent_parent_count,
            horizon_minutes=60,
            schedule_slot_utc=SLOT,
            due_at_utc=DUE_AT,
            duration_ms=1,
        ),
        "parent-movement count",
    )


def _check_complete_traversal_receipt_validation() -> None:
    base = _Corpus().to_dict()
    cursor = {
        "projection_decision_time_utc": "2026-09-02T12:00:00+00:00",
        "projection_event_id": 102,
    }
    pages = [
        {
            "page_number": 1,
            "before": None,
            "next": cursor,
            "has_more": True,
            "projections": worker_module._STAGE4_CORPUS_PROJECTION_LIMIT,
            "observations": 2,
            "page_attestation_receipt_sha256": "2" * 64,
        },
        {
            "page_number": 2,
            "before": cursor,
            "next": None,
            "has_more": False,
            "projections": 1,
            "observations": 1,
            "page_attestation_receipt_sha256": "3" * 64,
        },
    ]
    complete = _replace_traversal_pages(base, pages)
    complete["counts"]["projections"] = (
        worker_module._STAGE4_CORPUS_PROJECTION_LIMIT + 1
    )
    receipt = _observe_stage4_corpus(
        complete,
        horizon_minutes=60,
        schedule_slot_utc=SLOT,
        due_at_utc=DUE_AT,
        duration_ms=1,
    )
    assert receipt["traversal"]["page_count"] == 2
    assert receipt["counts"]["projections"] == (
        worker_module._STAGE4_CORPUS_PROJECTION_LIMIT + 1
    )

    malformed_cursor_pages = [dict(page) for page in pages]
    malformed_cursor_pages[0]["next"] = "not-a-keyset-cursor"
    malformed_cursor_pages[1]["before"] = "not-a-keyset-cursor"
    malformed_cursor = _replace_traversal_pages(
        complete, malformed_cursor_pages
    )
    _expect_value_error(
        lambda: _observe_stage4_corpus(
            malformed_cursor,
            horizon_minutes=60,
            schedule_slot_utc=SLOT,
            due_at_utc=DUE_AT,
            duration_ms=1,
        ),
        "cursor",
    )

    newer_cursor = {
        "projection_decision_time_utc": "2026-09-03T12:00:00+00:00",
        "projection_event_id": 103,
    }
    non_descending_pages = [
        dict(pages[0]),
        {
            **dict(pages[0]),
            "page_number": 2,
            "before": cursor,
            "next": newer_cursor,
            "page_attestation_receipt_sha256": "4" * 64,
        },
        {
            **dict(pages[1]),
            "page_number": 3,
            "before": newer_cursor,
            "page_attestation_receipt_sha256": "5" * 64,
        },
    ]
    non_descending = _replace_traversal_pages(
        complete, non_descending_pages
    )
    non_descending["counts"]["projections"] = (
        (2 * worker_module._STAGE4_CORPUS_PROJECTION_LIMIT) + 1
    )
    non_descending["traversal"]["pages"][1]["observations"] = 0
    _expect_value_error(
        lambda: _observe_stage4_corpus(
            non_descending,
            horizon_minutes=60,
            schedule_slot_utc=SLOT,
            due_at_utc=DUE_AT,
            duration_ms=1,
        ),
        "descend",
    )

    negative_page = _replace_traversal_pages(base, [dict(pages[1])])
    negative_page["traversal"]["pages"][0].update(
        {"page_number": 1, "before": None, "projections": -1}
    )
    _expect_value_error(
        lambda: _observe_stage4_corpus(
            negative_page,
            horizon_minutes=60,
            schedule_slot_utc=SLOT,
            due_at_utc=DUE_AT,
            duration_ms=1,
        ),
        "page counts",
    )

    count_mismatch = _replace_traversal_pages(base, [dict(pages[1])])
    count_mismatch["traversal"]["pages"][0].update(
        {"page_number": 1, "before": None}
    )
    _expect_value_error(
        lambda: _observe_stage4_corpus(
            count_mismatch,
            horizon_minutes=60,
            schedule_slot_utc=SLOT,
            due_at_utc=DUE_AT,
            duration_ms=1,
        ),
        "aggregate counts",
    )

    for field, invalid in (
        ("eof_proven", False),
        ("database_snapshot_id", "different-snapshot"),
        ("analysis_as_of_utc", "2026-09-04T12:05:00+00:00"),
    ):
        inconsistent = dict(base)
        inconsistent["traversal"] = dict(base["traversal"])
        inconsistent["traversal"][field] = invalid
        _expect_value_error(
            lambda value=inconsistent: (
                _observe_stage4_corpus(
                    value,
                    horizon_minutes=60,
                    schedule_slot_utc=SLOT,
                    due_at_utc=DUE_AT,
                    duration_ms=1,
                )
            ),
            "traversal",
        )

    broken_chain = dict(base)
    broken_chain["traversal"] = dict(base["traversal"])
    broken_chain["traversal"]["page_receipts_sha256"] = "0" * 64
    _expect_value_error(
        lambda: _observe_stage4_corpus(
            broken_chain,
            horizon_minutes=60,
            schedule_slot_utc=SLOT,
            due_at_utc=DUE_AT,
            duration_ms=1,
        ),
        "receipt chain",
    )


def _check_candidate_search_is_bound_to_corpus() -> None:
    source = _Corpus(reader_ready=True).to_dict()
    receipt = _observe_stage4_corpus(
        source,
        horizon_minutes=60,
        schedule_slot_utc=SLOT,
        due_at_utc=DUE_AT,
        duration_ms=1,
    )
    attached = worker_module._attach_stage4_candidate_search(
        receipt, _search_result(horizon_minutes=60)
    )
    assert attached["status"] == "INGESTED_AND_SEARCHED"

    wrong_horizon = _search_result(horizon_minutes=240)
    _expect_value_error(
        lambda: worker_module._attach_stage4_candidate_search(
            receipt, wrong_horizon
        ),
        "corpus",
    )
    wrong_time = _search_result(horizon_minutes=60)
    wrong_time["analysis_as_of_utc"] = "2026-09-04T12:05:00+00:00"
    _rehash_search_result(wrong_time)
    _expect_value_error(
        lambda: worker_module._attach_stage4_candidate_search(
            receipt, wrong_time
        ),
        "corpus",
    )
    wrong_count = _search_result(horizon_minutes=60)
    wrong_count["counts"] = dict(wrong_count["counts"])
    wrong_count["counts"]["observations"] = 2
    wrong_count["input_observation_count"] = 2
    _rehash_search_result(wrong_count)
    _expect_value_error(
        lambda: worker_module._attach_stage4_candidate_search(
            receipt, wrong_count
        ),
        "corpus",
    )

    tampered_receipt = _search_result(horizon_minutes=60)
    tampered_receipt["engine_version"] = "forged-engine"
    _expect_value_error(
        lambda: worker_module._attach_stage4_candidate_search(
            receipt, tampered_receipt
        ),
        "receipt hash mismatch",
    )

    wrong_version = _search_result(horizon_minutes=60)
    wrong_version["engine_version"] = "forged-engine"
    _rehash_search_result(wrong_version)
    _expect_value_error(
        lambda: worker_module._attach_stage4_candidate_search(
            receipt, wrong_version
        ),
        "version mismatch",
    )

    wrong_candidate_version = _search_result(horizon_minutes=60)
    wrong_candidate_version["candidates"][0]["engine_version"] = "forged-engine"
    _rehash_search_result(wrong_candidate_version)
    _expect_value_error(
        lambda: worker_module._attach_stage4_candidate_search(
            receipt, wrong_candidate_version
        ),
        "candidate version mismatch",
    )

    wrong_candidate_key = _search_result(horizon_minutes=60)
    wrong_candidate_key["candidates"][0]["candidate_key"] = "0" * 64
    _rehash_search_result(wrong_candidate_key)
    _expect_value_error(
        lambda: worker_module._attach_stage4_candidate_search(
            receipt, wrong_candidate_key
        ),
        "candidate key",
    )

    wrong_input_chain = _search_result(horizon_minutes=60)
    wrong_input_chain["input_observation_chain_sha256"] = "0" * 64
    _rehash_search_result(wrong_input_chain)
    _expect_value_error(
        lambda: worker_module._attach_stage4_candidate_search(
            receipt, wrong_input_chain
        ),
        "corpus",
    )


def _check_wall_budget_environment_fallback() -> None:
    name = "FORMULA_STAGE4_CORPUS_WALL_BUDGET_MS"
    previous = os.environ.get(name)
    try:
        os.environ[name] = "not-an-integer"
        assert worker_module._bounded_int_environment(
            name,
            default=240_000,
            minimum=30_000,
            maximum=600_000,
        ) == 240_000
    finally:
        if previous is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = previous

    search_name = "FORMULA_STAGE4_CANDIDATE_SEARCH_WALL_BUDGET_MS"
    previous_search = os.environ.get(search_name)
    try:
        os.environ[search_name] = "not-an-integer"
        assert worker_module._bounded_int_environment(
            search_name,
            default=60_000,
            minimum=5_000,
            maximum=300_000,
        ) == 60_000
    finally:
        if previous_search is None:
            os.environ.pop(search_name, None)
        else:
            os.environ[search_name] = previous_search


def _check_missing_configuration() -> None:
    reader = worker_module.research_signal_formula_exploration_reader
    original_complete_reader = reader.load_complete_authoritative_stage4_corpus
    original_paged_reader = reader.load_authoritative_stage4_corpus
    calls = []
    candidate_search = worker_module.research_stage4_candidate_search
    original_search = candidate_search.search_experimental_candidates
    reader.load_complete_authoritative_stage4_corpus = (
        lambda **kwargs: calls.append(kwargs) or _Corpus().complete_result()
    )
    reader.load_authoritative_stage4_corpus = _forbidden
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
            _assert_no_authority(receipt)
            assert receipt["traversal"]["single_database_snapshot"] is False
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
        reader.load_complete_authoritative_stage4_corpus = original_complete_reader
        reader.load_authoritative_stage4_corpus = original_paged_reader
        candidate_search.search_experimental_candidates = original_search


def _check_success_and_same_slot_dedupe() -> None:
    reader = worker_module.research_signal_formula_exploration_reader
    candidate_search = worker_module.research_stage4_candidate_search
    store = worker_module.research_formula_store
    engine = worker_module.research_formula_engine
    original_complete_reader = reader.load_complete_authoritative_stage4_corpus
    original_paged_reader = reader.load_authoritative_stage4_corpus
    original_search = candidate_search.search_experimental_candidates
    original_discover = engine.discover_formulas
    original_persist = store.persist_discovery_run
    original_pending = store.load_pending_live_deliveries
    calls = []
    search_calls = []

    def load_corpus(**kwargs):
        calls.append(kwargs)
        return _Corpus(
            reader_ready=True,
            horizon_minutes=int(kwargs["horizon_minutes"]),
        ).complete_result()

    def run_search(observations, **kwargs):
        search_calls.append(
            {
                "observations": observations,
                **kwargs,
            }
        )
        return _search_result(horizon_minutes=int(kwargs["horizon_minutes"]))

    reader.load_complete_authoritative_stage4_corpus = load_corpus
    reader.load_authoritative_stage4_corpus = _forbidden
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
            expected_complete = _Corpus(
                reader_ready=True,
                horizon_minutes=240,
            ).complete_result()
            assert receipt["corpus_attestation_receipt_sha256"] == (
                expected_complete.attestation_receipt_sha256
            )
            assert receipt["source_attestation"]["source_catalog_sha256"] == (
                "b" * 64
            )
            assert receipt["cursor"] == {
                "order": (
                    "projection_decision_time_utc DESC, projection_event_id DESC"
                ),
                "before": None,
                "next": None,
                "has_more": False,
            }
            assert receipt["traversal"]["status"] == "COMPLETE"
            assert receipt["traversal"]["single_database_snapshot"] is True
            assert receipt["traversal"]["page_count"] == 1
            assert receipt["traversal"]["max_pages"] == (
                reader.MAX_FULL_CORPUS_PAGES
            )
            assert receipt["traversal"]["max_projections"] == (
                reader.MAX_FULL_CORPUS_PROJECTIONS
            )
            assert receipt["traversal"]["max_observations"] == 131_072
            assert receipt["traversal"]["wall_budget_ms"] == (
                worker_module._STAGE4_CORPUS_WALL_BUDGET_MS
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
                "max_source_event_id": None,
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
            _assert_no_authority(receipt)
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
            assert search_receipt["config"]["wall_budget_ms"] == 60_000
            assert search_receipt["counts"][
                "eligible_experimental_candidates"
            ] == 1
            assert len(search_receipt["candidates"]) == 1
            candidate = search_receipt["candidates"][0]
            assert candidate["experimental_formula_eligible"] is True
            assert candidate["occurrence_counts"]["completed"] == 5
            assert candidate["occurrence_evidence_sha256"] == "a" * 64
            assert "completed_occurrences" not in candidate
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
                    "wall_budget_ms": (
                        worker_module._STAGE4_CORPUS_WALL_BUDGET_MS
                    ),
                    "database_url": (
                        "postgresql://stage4_reader:secret@database.example/research"
                    ),
                }
            ]
            assert len(search_calls) == 1
            expected_observations = _Corpus(
                reader_ready=True,
                horizon_minutes=240,
            ).candidate_observations
            assert search_calls[0]["observations"] == expected_observations
            assert len(search_calls[0]["observations"]) == len(
                expected_observations
            )
            assert len(
                {
                    row["projection_event_id"]
                    for row in search_calls[0]["observations"]
                }
            ) == len(expected_observations)
            assert search_calls[0]["horizon_minutes"] == 240
            assert search_calls[0]["analysis_as_of_utc"] == (
                "2026-09-04T12:06:00+00:00"
            )
            search_config = search_calls[0]["config"]
            assert isinstance(
                search_config,
                candidate_search.Stage4SearchConfig,
            )
            assert candidate_search.MAX_OBSERVATIONS == 131_072
            assert search_config.max_observations == 131_072
            assert search_config.wall_budget_ms == (
                worker_module._STAGE4_CANDIDATE_SEARCH_WALL_BUDGET_MS
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
            bounded = status["bounded_request"]
            assert bounded["pagination"] == "FULL_SNAPSHOT_KEYSET"
            assert bounded["projection_page_limit"] == (
                worker_module._STAGE4_CORPUS_PROJECTION_LIMIT
            )
            assert bounded["max_pages"] == reader.MAX_FULL_CORPUS_PAGES
            assert bounded["max_projections"] == (
                reader.MAX_FULL_CORPUS_PROJECTIONS
            )
            assert bounded["max_observations"] == 131_072
            assert bounded["wall_budget_ms"] == (
                worker_module._STAGE4_CORPUS_WALL_BUDGET_MS
            )
            assert bounded["candidate_search_wall_budget_ms"] == (
                worker_module._STAGE4_CANDIDATE_SEARCH_WALL_BUDGET_MS
            )
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
        reader.load_complete_authoritative_stage4_corpus = original_complete_reader
        reader.load_authoritative_stage4_corpus = original_paged_reader
        candidate_search.search_experimental_candidates = original_search
        engine.discover_formulas = original_discover
        store.persist_discovery_run = original_persist
        store.load_pending_live_deliveries = original_pending


def _check_search_does_not_override_reader_not_ready() -> None:
    reader = worker_module.research_signal_formula_exploration_reader
    candidate_search = worker_module.research_stage4_candidate_search
    original_complete_reader = reader.load_complete_authoritative_stage4_corpus
    original_paged_reader = reader.load_authoritative_stage4_corpus
    original_search = candidate_search.search_experimental_candidates

    reader.load_complete_authoritative_stage4_corpus = (
        lambda **kwargs: _Corpus(
            reader_ready=False,
            horizon_minutes=int(kwargs["horizon_minutes"]),
        ).complete_result()
    )
    reader.load_authoritative_stage4_corpus = _forbidden
    candidate_search.search_experimental_candidates = (
        lambda _observations, **kwargs: _search_result(
            horizon_minutes=int(kwargs["horizon_minutes"]),
            input_observations=_observations,
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
            _assert_no_authority(receipt)
            status = worker.status()["stage4_authoritative_ingestion"]
            assert status["ready_for_candidate_search"] is False
            assert status["candidate_eligibility_gate"][
                "searched_horizons_minutes"
            ] == [240]
            assert status["candidate_eligibility_gate"][
                "ready_horizons_minutes"
            ] == []
    finally:
        reader.load_complete_authoritative_stage4_corpus = original_complete_reader
        reader.load_authoritative_stage4_corpus = original_paged_reader
        candidate_search.search_experimental_candidates = original_search


def _check_reader_failure_is_fail_open() -> None:
    reader = worker_module.research_signal_formula_exploration_reader
    store = worker_module.research_formula_store
    engine = worker_module.research_formula_engine
    original_complete_reader = reader.load_complete_authoritative_stage4_corpus
    original_paged_reader = reader.load_authoritative_stage4_corpus
    original_lock = store.discovery_horizon_lock
    original_state = store.load_discovery_schedule_state
    original_discover = engine.discover_formulas
    original_persist = store.persist_discovery_run

    @contextmanager
    def acquired(_horizon):
        yield True

    reader.load_complete_authoritative_stage4_corpus = (
        lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeError("selftest authoritative reader failure")
        )
    )
    reader.load_authoritative_stage4_corpus = _forbidden
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
            _assert_no_authority(receipt)
            assert worker.metrics.stage4_ingestion_failures == 1
            assert "selftest authoritative reader failure" in (
                worker.metrics.last_stage4_ingestion_error or ""
            )
    finally:
        reader.load_complete_authoritative_stage4_corpus = original_complete_reader
        reader.load_authoritative_stage4_corpus = original_paged_reader
        store.discovery_horizon_lock = original_lock
        store.load_discovery_schedule_state = original_state
        engine.discover_formulas = original_discover
        store.persist_discovery_run = original_persist


def _check_candidate_search_failure_is_fail_open() -> None:
    reader = worker_module.research_signal_formula_exploration_reader
    candidate_search = worker_module.research_stage4_candidate_search
    store = worker_module.research_formula_store
    engine = worker_module.research_formula_engine
    original_complete_reader = reader.load_complete_authoritative_stage4_corpus
    original_paged_reader = reader.load_authoritative_stage4_corpus
    original_search = candidate_search.search_experimental_candidates
    original_discover = engine.discover_formulas
    original_persist = store.persist_discovery_run
    original_pending = store.load_pending_live_deliveries

    reader.load_complete_authoritative_stage4_corpus = (
        lambda **kwargs: _Corpus(
            reader_ready=False,
            horizon_minutes=int(kwargs["horizon_minutes"]),
        ).complete_result()
    )
    reader.load_authoritative_stage4_corpus = _forbidden
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
            _assert_no_authority(receipt)
            assert worker.metrics.stage4_ingestion_successes == 1
            assert worker.metrics.stage4_candidate_search_attempts == 1
            assert worker.metrics.stage4_candidate_search_successes == 0
            assert worker.metrics.stage4_candidate_search_failures == 1
            assert "candidate search failure" in (
                worker.metrics.last_stage4_candidate_search_error or ""
            )
    finally:
        reader.load_complete_authoritative_stage4_corpus = original_complete_reader
        reader.load_authoritative_stage4_corpus = original_paged_reader
        candidate_search.search_experimental_candidates = original_search
        engine.discover_formulas = original_discover
        store.persist_discovery_run = original_persist
        store.load_pending_live_deliveries = original_pending


def run() -> None:
    _check_authority_boundary_is_attested()
    _check_complete_traversal_receipt_validation()
    _check_candidate_search_is_bound_to_corpus()
    _check_wall_budget_environment_fallback()
    _check_missing_configuration()
    _check_success_and_same_slot_dedupe()
    _check_search_does_not_override_reader_not_ready()
    _check_reader_failure_is_fail_open()
    _check_candidate_search_failure_is_fail_open()


if __name__ == "__main__":
    run()
    print("research_formula_worker_stage4_ingestion_selftest: PASS")
