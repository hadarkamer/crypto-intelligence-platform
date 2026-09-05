"""Focused regressions for Stage-4 persistence and experimental delivery."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from typing import Any, Iterator

import research_formula_worker as worker_module
import research_formula_worker_stage4_ingestion_selftest as stage4_fixture
import research_stage4_experimental_store as experimental_store


_MISSING = object()


@contextmanager
def _patched(target: Any, **replacements: Any) -> Iterator[None]:
    originals = {
        name: getattr(target, name, _MISSING) for name in replacements
    }
    for name, replacement in replacements.items():
        setattr(target, name, replacement)
    try:
        yield
    finally:
        for name, original in originals.items():
            if original is _MISSING:
                delattr(target, name)
            else:
                setattr(target, name, original)


def _forbidden(*_args: Any, **_kwargs: Any) -> Any:
    raise AssertionError("guarded worker dependency was called")


def _search_result_with_all_eligible_variants(
    observations: Any,
    *,
    horizon_minutes: int,
) -> dict[str, Any]:
    result = stage4_fixture._search_result(
        horizon_minutes=horizon_minutes,
        input_observations=observations,
    )
    result["eligible_candidate_variants"] = list(
        result["eligible_candidates"]
    )
    result["counts"]["eligible_candidate_variants"] = len(
        result["eligible_candidate_variants"]
    )
    return stage4_fixture._rehash_search_result(result)


def _check_raw_search_result_reaches_persistence_and_failure_is_open() -> None:
    reader = worker_module.research_signal_formula_exploration_reader
    candidate_search = worker_module.research_stage4_candidate_search
    emitted: list[dict[str, Any]] = []
    persistence_calls: list[dict[str, Any]] = []
    source_receipts: list[str] = []

    def load_corpus(**kwargs: Any) -> Any:
        corpus = stage4_fixture._Corpus(
            reader_ready=True,
            horizon_minutes=int(kwargs["horizon_minutes"]),
        ).complete_result()
        source_receipts.append(corpus.attestation_receipt_sha256)
        return corpus

    def search(observations: Any, **kwargs: Any) -> dict[str, Any]:
        result = _search_result_with_all_eligible_variants(
            observations,
            horizon_minutes=int(kwargs["horizon_minutes"]),
        )
        emitted.append(result)
        return result

    def persist(result: Any, **kwargs: Any) -> Any:
        assert result is emitted[-1]
        assert result["eligible_candidate_variants"]
        assert result["eligible_candidate_variants"] is not (
            result["eligible_candidates"]
        )
        assert result["counts"]["eligible_candidate_variants"] == len(
            result["eligible_candidate_variants"]
        )
        persistence_calls.append({"result": result, **kwargs})
        raise RuntimeError("selftest isolated search persistence failure")

    with (
        _patched(
            worker_module,
            _EXPERIMENTAL_ALERTS_ENABLED=True,
        ),
        _patched(
            reader,
            load_complete_authoritative_stage4_corpus=load_corpus,
            load_authoritative_stage4_corpus=_forbidden,
        ),
        _patched(
            candidate_search,
            search_experimental_candidates=search,
        ),
        _patched(
            experimental_store,
            persist_search_run=persist,
        ),
        stage4_fixture._reader_database_url(
            "postgresql://stage4_reader:secret@database.example/research"
        ),
    ):
        worker = worker_module.FormulaResearchWorker()
        worker._experimental_schema_ready = True
        receipt = worker._ingest_authoritative_stage4_corpus_once(
            60,
            schedule_slot_utc=stage4_fixture.SLOT,
            due_at_utc=stage4_fixture.DUE_AT,
        )

    assert len(emitted) == 1
    assert len(persistence_calls) == 1
    assert persistence_calls[0]["result"] is emitted[0]
    assert persistence_calls[0]["source_corpus_receipt_sha256"] == (
        source_receipts[0]
    )
    assert persistence_calls[0]["schedule_slot_utc"] == stage4_fixture.SLOT
    assert receipt["status"] == "INGESTED_AND_SEARCHED"
    assert receipt["candidate_search"]["ran"] is True
    assert receipt["experimental_persistence"]["status"] == "FAILED_OPEN"
    assert "isolated search persistence failure" in (
        receipt["experimental_persistence"]["error"]
    )
    assert worker.metrics.stage4_candidate_search_successes == 1
    assert worker.metrics.stage4_candidate_search_failures == 0
    assert worker.metrics.experimental_search_persistence_failures == 1


class _TelegramResult:
    message_id = 90210


class _SuccessfulBot:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def send_message(self, **kwargs: Any) -> _TelegramResult:
        self.calls.append(dict(kwargs))
        return _TelegramResult()


def _delivery() -> dict[str, Any]:
    return {
        "delivery_key": "d" * 64,
        "alert_occurrence_id": "a" * 64,
        "chat_id": 123456,
        "attempt_count": 1,
        "claim_token": "c" * 64,
        "rendered_message": "ניסיוני, לא מאושר למסחר",
    }


def _check_confirmed_send_with_completion_failure_is_not_resent() -> None:
    async def scenario() -> None:
        state = "PENDING"
        claim_calls = 0
        completion_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

        def claim(*, limit: int) -> list[dict[str, Any]]:
            nonlocal state, claim_calls
            assert limit == 1
            claim_calls += 1
            if state != "PENDING":
                return []
            state = "IN_FLIGHT"
            return [_delivery()]

        def complete(*args: Any, **kwargs: Any) -> Any:
            completion_calls.append((args, dict(kwargs)))
            assert kwargs["sent"] is True
            assert kwargs["telegram_message_id"] == 90210
            # Model a database failure after Telegram positively acknowledged
            # the send: the lease remains IN_FLIGHT and cannot be reclaimed.
            raise RuntimeError("selftest completion database failure")

        bot = _SuccessfulBot()
        with (
            _patched(
                worker_module,
                _EXPERIMENTAL_ALERTS_ENABLED=True,
                _EXPERIMENTAL_CLAIM_BATCH=4,
            ),
            _patched(
                experimental_store,
                claim_pending_deliveries=claim,
                complete_delivery=complete,
            ),
        ):
            worker = worker_module.FormulaResearchWorker()
            worker._experimental_schema_ready = True
            worker.bind_telegram(bot)
            first = await worker._deliver_pending_experimental_alerts()
            second = await worker._deliver_pending_experimental_alerts()

        assert state == "IN_FLIGHT"
        assert claim_calls == 3
        assert len(bot.calls) == 1
        assert len(completion_calls) == 1
        assert first == {
            "status": "COMPLETED",
            "claimed": 1,
            "sent": 0,
            "ambiguous": 0,
            "completion_failures": 1,
        }
        assert second == {
            "status": "COMPLETED",
            "claimed": 0,
            "sent": 0,
            "ambiguous": 0,
            "completion_failures": 0,
        }

    asyncio.run(scenario())


def _check_missing_bot_or_schema_never_claims() -> None:
    async def scenario() -> None:
        with (
            _patched(
                worker_module,
                _EXPERIMENTAL_ALERTS_ENABLED=True,
            ),
            _patched(
                experimental_store,
                claim_pending_deliveries=_forbidden,
            ),
        ):
            no_bot = worker_module.FormulaResearchWorker()
            no_bot._experimental_schema_ready = True
            no_bot_result = await no_bot._deliver_pending_experimental_alerts()

            no_schema = worker_module.FormulaResearchWorker()
            no_schema._experimental_schema_ready = False
            no_schema.bind_telegram(_SuccessfulBot())
            no_schema_result = (
                await no_schema._deliver_pending_experimental_alerts()
            )

        for result in (no_bot_result, no_schema_result):
            assert result == {
                "status": "DISABLED",
                "claimed": 0,
                "sent": 0,
                "ambiguous": 0,
                "completion_failures": 0,
            }

    asyncio.run(scenario())


def run() -> None:
    _check_raw_search_result_reaches_persistence_and_failure_is_open()
    _check_confirmed_send_with_completion_failure_is_not_resent()
    _check_missing_bot_or_schema_never_claims()
    print("research_formula_worker_delivery_regression_selftest: PASS")


if __name__ == "__main__":
    run()
