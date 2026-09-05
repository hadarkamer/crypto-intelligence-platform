"""Database-free checks for the isolated experimental Formula worker lane."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterator

import research_experimental_formula_alert as experimental_alert
import research_formula_worker as worker_module
import research_signal_formula_exploration_reader as stage4_reader
import research_stage4_experimental_store as experimental_store


_MISSING = object()


def _forbidden(*_args: Any, **_kwargs: Any) -> Any:
    raise AssertionError("legacy LIVE/Shadow path was called by experimental lane")


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


@contextmanager
def _forbid_legacy_formula_paths() -> Iterator[None]:
    store = worker_module.research_formula_store
    names = (
        "load_shadow_work",
        "load_pending_live_deliveries",
        "mark_live_delivery",
        "record_shadow_results",
        "evaluate_shadow_readiness",
    )
    replacements = {
        name: _forbidden for name in names if hasattr(store, name)
    }
    with _patched(store, **replacements):
        yield


class _Envelope:
    def __init__(self, horizon_minutes: int) -> None:
        self.horizon_minutes = horizon_minutes

    def to_dict(self) -> dict[str, Any]:
        return {
            "horizon_minutes": self.horizon_minutes,
            "eligible_candidates": [{"candidate_key": f"candidate-{self.horizon_minutes}"}],
        }


class _CurrentResult:
    def __init__(self, *, available: bool = True) -> None:
        self._available = available
        self._observations = ("current-observation",) if available else ()
        self.attestation_receipt_sha256 = "e" * 64

    @property
    def current_observations(self) -> tuple[Any, ...]:
        return self._observations

    @property
    def observations(self) -> tuple[Any, ...]:
        return self._observations

    def receipt_dict(self) -> dict[str, Any]:
        return {
            "status": "AVAILABLE" if self._available else "NO_TERMINAL_PROJECTION",
            "available": self._available,
            "analysis_as_of_utc": "2026-09-05T12:05:00+00:00",
            "blockers": [] if self._available else ["NO_TERMINAL_STAGE4_PROJECTION"],
            "outcomes_loaded": False,
        }


@dataclass(frozen=True)
class _Alert:
    marker: str


def _search_rows(*horizons: int) -> list[dict[str, Any]]:
    return [
        {
            "search_run_id": f"search-{horizon}",
            "horizon_minutes": horizon,
            "compact_envelope": _Envelope(horizon),
        }
        for horizon in horizons
    ]


class _NeverEndingLane:
    def __init__(self, marker: str, started: list[str]) -> None:
        self.marker = marker
        self.started = started

    async def __call__(self) -> None:
        self.started.append(self.marker)
        await asyncio.Event().wait()


def _check_experimental_only_lifecycle() -> None:
    async def scenario() -> None:
        lanes: list[str] = []
        base_schema_calls = 0

        def base_schema() -> dict[str, Any]:
            nonlocal base_schema_calls
            base_schema_calls += 1
            return {"schema_present": True, "missing_tables": []}

        with (
            _patched(
                worker_module,
                _DISCOVERY_ENABLED=False,
                _SHADOW_ENABLED=False,
                _LIVE_ALERTS_ENABLED=False,
                _EXPERIMENTAL_ALERTS_ENABLED=True,
            ),
            _patched(worker_module.research_formula_store, schema_status=base_schema),
            _patched(
                experimental_store,
                schema_status=lambda: {
                    "schema_present": True,
                    "ready": True,
                    "missing_tables": [],
                },
            ),
        ):
            worker = worker_module.FormulaResearchWorker()
            worker._experimental_loop = _NeverEndingLane("experimental", lanes)
            worker._discovery_loop = _NeverEndingLane("discovery", lanes)
            worker._shadow_loop = _NeverEndingLane("shadow", lanes)
            started = await worker.start()
            await asyncio.sleep(0)
            assert started is True
            assert lanes == ["experimental"]
            assert worker._experimental_task is not None
            assert not worker._experimental_task.done()
            assert worker._discovery_task is None
            assert worker._shadow_task is None
            assert base_schema_calls == 0
            await worker.stop()
            assert worker._experimental_task is None
            assert worker._discovery_task is None
            assert worker._shadow_task is None

    asyncio.run(scenario())


def _check_missing_experimental_schema_isolated_from_discovery() -> None:
    async def scenario() -> None:
        lanes: list[str] = []
        with (
            _patched(
                worker_module,
                _DISCOVERY_ENABLED=True,
                _SHADOW_ENABLED=False,
                _LIVE_ALERTS_ENABLED=False,
                _EXPERIMENTAL_ALERTS_ENABLED=True,
            ),
            _patched(
                worker_module.research_formula_store,
                schema_status=lambda: {
                    "schema_present": True,
                    "missing_tables": [],
                },
            ),
            _patched(
                experimental_store,
                schema_status=lambda: {
                    "schema_present": True,
                    "ready": False,
                    "missing": ["exact_table_acl"],
                },
            ),
        ):
            worker = worker_module.FormulaResearchWorker()
            worker._discovery_loop = _NeverEndingLane("discovery", lanes)
            worker._experimental_loop = _NeverEndingLane("experimental", lanes)
            started = await worker.start()
            await asyncio.sleep(0)
            assert started is True
            assert lanes == ["discovery"]
            assert worker._discovery_task is not None
            assert worker._experimental_task is None
            assert worker._schema_ready is True
            assert worker._experimental_schema_ready is False
            await worker.stop()

    asyncio.run(scenario())


def _check_missing_legacy_schema_isolated_from_experimental() -> None:
    async def scenario() -> None:
        lanes: list[str] = []
        with (
            _patched(
                worker_module,
                _DISCOVERY_ENABLED=True,
                _SHADOW_ENABLED=True,
                _LIVE_ALERTS_ENABLED=False,
                _EXPERIMENTAL_ALERTS_ENABLED=True,
            ),
            _patched(
                worker_module.research_formula_store,
                schema_status=lambda: {
                    "schema_present": False,
                    "missing_tables": ["research_formulas"],
                },
            ),
            _patched(
                experimental_store,
                schema_status=lambda: {
                    "schema_present": True,
                    "ready": True,
                    "missing_tables": [],
                },
            ),
        ):
            worker = worker_module.FormulaResearchWorker()
            worker._discovery_loop = _NeverEndingLane("discovery", lanes)
            worker._shadow_loop = _NeverEndingLane("shadow", lanes)
            worker._experimental_loop = _NeverEndingLane("experimental", lanes)
            started = await worker.start()
            await asyncio.sleep(0)
            assert started is True
            assert lanes == ["experimental"]
            assert worker._schema_ready is False
            assert worker._experimental_schema_ready is True
            assert worker._discovery_task is None
            assert worker._shadow_task is None
            assert worker._experimental_task is not None
            assert worker.metrics.last_discovery_error_phase == (
                "SCHEMA_ATTESTATION"
            )
            assert worker.metrics.last_shadow_error_phase == (
                "SCHEMA_ATTESTATION"
            )
            assert "research_formulas" in (worker.metrics.last_error or "")
            await worker.stop()

    asyncio.run(scenario())


def _check_legacy_schema_exception_isolated_from_experimental() -> None:
    async def scenario() -> None:
        lanes: list[str] = []

        def fail_legacy_attestation() -> dict[str, Any]:
            raise RuntimeError("selftest legacy database unavailable")

        with (
            _patched(
                worker_module,
                _DISCOVERY_ENABLED=True,
                _SHADOW_ENABLED=False,
                _LIVE_ALERTS_ENABLED=False,
                _EXPERIMENTAL_ALERTS_ENABLED=True,
            ),
            _patched(
                worker_module.research_formula_store,
                schema_status=fail_legacy_attestation,
            ),
            _patched(
                experimental_store,
                schema_status=lambda: {
                    "schema_present": True,
                    "ready": True,
                },
            ),
        ):
            worker = worker_module.FormulaResearchWorker()
            worker._discovery_loop = _NeverEndingLane("discovery", lanes)
            worker._experimental_loop = _NeverEndingLane("experimental", lanes)
            started = await worker.start()
            await asyncio.sleep(0)
            assert started is True
            assert lanes == ["experimental"]
            assert worker._discovery_task is None
            assert worker._experimental_task is not None
            assert "legacy database unavailable" in (
                worker.metrics.last_discovery_error or ""
            )
            await worker.stop()

    asyncio.run(scenario())


def _check_one_current_read_for_all_horizons_and_no_legacy_calls() -> None:
    reader_calls = 0
    built_horizons: list[int] = []
    persisted: list[list[_Alert]] = []

    def load_current(*_args: Any, **_kwargs: Any) -> _CurrentResult:
        nonlocal reader_calls
        reader_calls += 1
        return _CurrentResult()

    def build(
        observations: Any,
        envelope: _Envelope,
        *,
        current_time_utc: Any,
    ) -> list[_Alert]:
        assert observations == ("current-observation",)
        assert current_time_utc is not None
        built_horizons.append(envelope.horizon_minutes)
        return [_Alert(str(envelope.horizon_minutes))]

    def persist(alerts: Any) -> dict[str, int]:
        batch = list(alerts)
        persisted.append(batch)
        return {
            "alerts_supplied": len(batch),
            "alerts_inserted": len(batch),
            "same_wave_duplicates": 0,
            "deliveries_queued": len(batch),
        }

    with (
        _patched(
            stage4_reader,
            LatestAuthoritativeStage4CurrentResult=_CurrentResult,
            load_latest_authoritative_stage4_current=load_current,
        ),
        _patched(
            experimental_store,
            load_latest_search_runs=lambda: _search_rows(60, 240, 720),
            persist_experimental_alerts=persist,
        ),
        _patched(
            experimental_alert,
            CompactEligibleSearchEnvelope=_Envelope,
            build_experimental_alerts=build,
        ),
        _forbid_legacy_formula_paths(),
    ):
        worker = worker_module.FormulaResearchWorker()
        worker._experimental_schema_ready = True
        worker.run_shadow_once = _forbidden
        result = worker.run_experimental_evaluation_once()

    assert reader_calls == 1
    assert built_horizons == [60, 240, 720]
    assert [alert.marker for batch in persisted for alert in batch] == [
        "60",
        "240",
        "720",
    ]
    assert isinstance(result, dict)


def _check_unavailable_current_result_creates_no_alerts() -> None:
    persisted: list[list[Any]] = []

    def persist(alerts: Any) -> dict[str, int]:
        batch = list(alerts)
        persisted.append(batch)
        return {
            "alerts_supplied": len(batch),
            "alerts_inserted": 0,
            "same_wave_duplicates": 0,
            "deliveries_queued": 0,
        }

    with (
        _patched(
            stage4_reader,
            LatestAuthoritativeStage4CurrentResult=_CurrentResult,
            load_latest_authoritative_stage4_current=lambda **_kwargs: _CurrentResult(
                available=False
            ),
        ),
        _patched(
            experimental_store,
            load_latest_search_runs=lambda: _search_rows(60, 240),
            persist_experimental_alerts=persist,
        ),
        _patched(
            experimental_alert,
            CompactEligibleSearchEnvelope=_Envelope,
            build_experimental_alerts=_forbidden,
        ),
        _forbid_legacy_formula_paths(),
    ):
        worker = worker_module.FormulaResearchWorker()
        worker._experimental_schema_ready = True
        result = worker.run_experimental_evaluation_once()

    assert not persisted or all(not batch for batch in persisted)
    assert isinstance(result, dict)
    assert int(result.get("alerts_built", 0)) == 0


def _check_stale_horizon_isolation() -> None:
    attempted: list[int] = []
    persisted: list[_Alert] = []

    def build(
        _observations: Any,
        envelope: _Envelope,
        *,
        current_time_utc: Any,
    ) -> list[_Alert]:
        assert current_time_utc is not None
        attempted.append(envelope.horizon_minutes)
        if envelope.horizon_minutes == 60:
            raise ValueError("eligible candidate search is not current")
        return [_Alert(str(envelope.horizon_minutes))]

    def persist(alerts: Any) -> dict[str, int]:
        batch = list(alerts)
        persisted.extend(batch)
        return {
            "alerts_supplied": len(batch),
            "alerts_inserted": len(batch),
            "same_wave_duplicates": 0,
            "deliveries_queued": len(batch),
        }

    with (
        _patched(
            stage4_reader,
            LatestAuthoritativeStage4CurrentResult=_CurrentResult,
            load_latest_authoritative_stage4_current=lambda **_kwargs: _CurrentResult(),
        ),
        _patched(
            experimental_store,
            load_latest_search_runs=lambda: _search_rows(60, 240),
            persist_experimental_alerts=persist,
        ),
        _patched(
            experimental_alert,
            CompactEligibleSearchEnvelope=_Envelope,
            build_experimental_alerts=build,
        ),
        _forbid_legacy_formula_paths(),
    ):
        worker = worker_module.FormulaResearchWorker()
        worker._experimental_schema_ready = True
        result = worker.run_experimental_evaluation_once()

    assert attempted == [60, 240]
    assert [alert.marker for alert in persisted] == ["240"]
    assert isinstance(result, dict)
    assert len(result.get("skipped_horizons") or []) == 1
    assert result["skipped_horizons"][0]["horizon_minutes"] == 60


def _check_evaluation_failure_does_not_block_delivery() -> None:
    async def scenario() -> None:
        delivery_calls = 0

        def fail_evaluation() -> dict[str, Any]:
            raise RuntimeError("selftest evaluation failure")

        async def deliver() -> dict[str, int]:
            nonlocal delivery_calls
            delivery_calls += 1
            return {"claimed": 0, "sent": 0, "ambiguous": 0}

        worker = worker_module.FormulaResearchWorker()
        worker._experimental_schema_ready = True
        worker._run_experimental_evaluation_once = fail_evaluation
        worker.run_experimental_evaluation_once = fail_evaluation
        worker._deliver_pending_experimental_alerts = deliver
        result = await worker._experimental_cycle_once()
        assert delivery_calls == 1
        assert isinstance(result, dict)
        assert result["evaluation"]["status"] == "FAILED_OPEN"
        assert "selftest evaluation failure" in (
            worker.metrics.last_experimental_error or ""
        )

    asyncio.run(scenario())


class _TelegramResult:
    def __init__(self, message_id: int) -> None:
        self.message_id = message_id


class _SuccessfulBot:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def send_message(self, **kwargs: Any) -> _TelegramResult:
        self.calls.append(dict(kwargs))
        return _TelegramResult(777)


class _FailingBot:
    async def send_message(self, **_kwargs: Any) -> Any:
        raise RuntimeError("selftest Telegram transport failure")


def _claimed_delivery() -> dict[str, Any]:
    return {
        "delivery_key": "d" * 64,
        "alert_occurrence_id": "a" * 64,
        "chat_id": 123456,
        "attempt_count": 1,
        "claim_token": "c" * 64,
        "rendered_message": "🧪 ניסיוני\nניסיוני, לא מאושר למסחר",
    }


def _single_claim_queue(
    delivery: dict[str, Any], calls: list[int] | None = None
) -> Callable[..., list[dict[str, Any]]]:
    queue = [delivery]

    def claim(*, limit: int) -> list[dict[str, Any]]:
        assert limit == 1
        if calls is not None:
            calls.append(limit)
        return [queue.pop(0)] if queue else []

    return claim


def _check_success_records_message_id_and_claim_token() -> None:
    async def scenario() -> None:
        completions: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        claim_calls: list[int] = []

        def complete(*args: Any, **kwargs: Any) -> dict[str, Any]:
            completions.append((args, dict(kwargs)))
            return {"status": "SENT", "sent": True, "ambiguous": False}

        bot = _SuccessfulBot()
        with (
            _patched(
                worker_module,
                _EXPERIMENTAL_ALERTS_ENABLED=True,
                _LIVE_ALERTS_ENABLED=False,
            ),
            _patched(
                experimental_store,
                claim_pending_deliveries=_single_claim_queue(
                    _claimed_delivery(), claim_calls
                ),
                complete_delivery=complete,
            ),
            _forbid_legacy_formula_paths(),
        ):
            worker = worker_module.FormulaResearchWorker()
            worker._experimental_schema_ready = True
            worker.bind_telegram(bot)
            worker.run_shadow_once = _forbidden
            result = await worker._deliver_pending_experimental_alerts()

        assert bot.calls == [
            {
                "chat_id": 123456,
                "text": "🧪 ניסיוני\nניסיוני, לא מאושר למסחר",
            }
        ]
        assert claim_calls == [1, 1]
        assert len(completions) == 1
        args, kwargs = completions[0]
        assert args == ("d" * 64, "c" * 64)
        assert kwargs["sent"] is True
        assert kwargs["telegram_message_id"] == 777
        assert kwargs.get("ambiguous", False) is False
        assert result["sent"] == 1
        assert result.get("ambiguous", 0) == 0

    asyncio.run(scenario())


def _check_send_exception_is_marked_ambiguous() -> None:
    async def scenario() -> None:
        completions: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

        def complete(*args: Any, **kwargs: Any) -> dict[str, Any]:
            completions.append((args, dict(kwargs)))
            return {"status": "AMBIGUOUS", "sent": False, "ambiguous": True}

        with (
            _patched(
                worker_module,
                _EXPERIMENTAL_ALERTS_ENABLED=True,
                _LIVE_ALERTS_ENABLED=False,
            ),
            _patched(
                experimental_store,
                claim_pending_deliveries=_single_claim_queue(
                    _claimed_delivery()
                ),
                complete_delivery=complete,
            ),
            _forbid_legacy_formula_paths(),
        ):
            worker = worker_module.FormulaResearchWorker()
            worker._experimental_schema_ready = True
            worker.bind_telegram(_FailingBot())
            result = await worker._deliver_pending_experimental_alerts()

        assert len(completions) == 1
        args, kwargs = completions[0]
        assert args == ("d" * 64, "c" * 64)
        assert kwargs["sent"] is False
        assert kwargs["ambiguous"] is True
        assert kwargs.get("telegram_message_id") is None
        assert "Telegram transport failure" in kwargs["error"]
        assert result["sent"] == 0
        assert result["ambiguous"] == 1

    asyncio.run(scenario())


def _check_claim_occurs_immediately_before_each_send() -> None:
    async def scenario() -> None:
        events: list[str] = []
        first = _claimed_delivery()
        second = {
            **_claimed_delivery(),
            "delivery_key": "e" * 64,
            "claim_token": "f" * 64,
            "chat_id": 654321,
        }
        claim_calls = 0

        def claim(*, limit: int) -> list[dict[str, Any]]:
            nonlocal claim_calls
            assert limit == 1
            claim_calls += 1
            events.append(f"claim:{claim_calls}")
            if claim_calls == 1:
                return [first]
            # The next queued row expired/was terminalized while the first
            # network call ran.  It was never leased into worker memory.
            events.append(f"terminalized:{second['chat_id']}")
            return []

        def complete(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            events.append("complete:1")
            return {"status": "SENT", "sent": True, "ambiguous": False}

        class RecordingBot:
            async def send_message(self, **kwargs: Any) -> _TelegramResult:
                events.append(f"send:{kwargs['chat_id']}")
                return _TelegramResult(778)

        with (
            _patched(
                worker_module,
                _EXPERIMENTAL_ALERTS_ENABLED=True,
                _EXPERIMENTAL_CLAIM_BATCH=2,
            ),
            _patched(
                experimental_store,
                claim_pending_deliveries=claim,
                complete_delivery=complete,
            ),
            _forbid_legacy_formula_paths(),
        ):
            worker = worker_module.FormulaResearchWorker()
            worker._experimental_schema_ready = True
            worker.bind_telegram(RecordingBot())
            result = await worker._deliver_pending_experimental_alerts()

        assert events == [
            "claim:1",
            "send:123456",
            "complete:1",
            "claim:2",
            "terminalized:654321",
        ]
        assert result["claimed"] == 1
        assert result["sent"] == 1

    asyncio.run(scenario())


def _check_no_bot_means_no_claim() -> None:
    async def scenario() -> None:
        with (
            _patched(
                worker_module,
                _EXPERIMENTAL_ALERTS_ENABLED=True,
                _LIVE_ALERTS_ENABLED=False,
            ),
            _patched(experimental_store, claim_pending_deliveries=_forbidden),
            _forbid_legacy_formula_paths(),
        ):
            worker = worker_module.FormulaResearchWorker()
            worker._experimental_schema_ready = True
            assert worker._telegram_bot is None
            result = await worker._deliver_pending_experimental_alerts()
        assert result["sent"] == 0
        assert result.get("claimed", 0) == 0

    asyncio.run(scenario())


def run() -> None:
    _check_experimental_only_lifecycle()
    _check_missing_experimental_schema_isolated_from_discovery()
    _check_missing_legacy_schema_isolated_from_experimental()
    _check_legacy_schema_exception_isolated_from_experimental()
    _check_one_current_read_for_all_horizons_and_no_legacy_calls()
    _check_unavailable_current_result_creates_no_alerts()
    _check_stale_horizon_isolation()
    _check_evaluation_failure_does_not_block_delivery()
    _check_success_records_message_id_and_claim_token()
    _check_send_exception_is_marked_ambiguous()
    _check_claim_occurs_immediately_before_each_send()
    _check_no_bot_means_no_claim()
    print("research_experimental_formula_worker_selftest: PASS")


if __name__ == "__main__":
    run()
