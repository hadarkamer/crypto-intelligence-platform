"""Async fake-bot regressions for the isolated PREVIEW_ONLY dispatcher."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from pathlib import Path

import research_experimental_preview_contract as preview_contract
import research_experimental_preview_contract_selftest as preview_fixtures
import research_experimental_preview_telegram_dispatcher as dispatcher_module


ROOT = Path(__file__).resolve().parent
BASE_COMMIT = "bca34f536099a55b9ac8c78f368ff3e725108364"
APPROVAL_ID = "a" * 64


def _plan() -> dict:
    gate_plan = preview_fixtures._gate_plan(stage5_status="WAITING_DATA")
    return preview_contract.plan_preview_only(
        gate_plan,
        policy=preview_fixtures._preview_policy(),
    )


def _transport_policy() -> dict:
    return {
        "enabled": True,
        "kill_switch_engaged": False,
        "owner_transport_approved": True,
        "test_chat_id": -1001,
    }


def _dispatcher_policy(**overrides) -> dict:
    policy = {
        "enabled": True,
        "kill_switch_engaged": False,
        "owner_dispatch_approved": True,
        "test_chat_id": -1001,
        "runtime_commit": BASE_COMMIT,
        "activation_approval_id": APPROVAL_ID,
    }
    policy.update(overrides)
    return policy


class FakeBot:
    def __init__(self) -> None:
        self.calls = []

    async def send_message(self, **kwargs):
        self.calls.append(deepcopy(kwargs))
        return {"fake": True}


class BlockingFakeBot(FakeBot):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def send_message(self, **kwargs):
        self.started.set()
        await self.release.wait()
        return await super().send_message(**kwargs)


async def _run_async() -> None:
    plan = _plan()
    default_bot = FakeBot()
    default_dispatcher = dispatcher_module.PreviewTelegramDispatcher(
        default_bot,
        client_classification=dispatcher_module.TEST_DOUBLE,
    )
    default_blocked = await default_dispatcher.dispatch(
        plan,
        transport_policy=_transport_policy(),
    )
    assert default_blocked["requests_considered"] == 1
    assert default_blocked["fake_bot_calls"] == 0
    assert default_blocked["suppressed"] == 1
    assert default_bot.calls == []

    runtime_bot = FakeBot()
    runtime_dispatcher = dispatcher_module.PreviewTelegramDispatcher(runtime_bot)
    runtime_blocked = await runtime_dispatcher.dispatch(
        plan,
        transport_policy=_transport_policy(),
        dispatcher_policy=_dispatcher_policy(),
    )
    assert runtime_blocked["client_classification"] == (
        dispatcher_module.RUNTIME_BOT_UNREGISTERED
    )
    assert runtime_blocked["fake_bot_calls"] == 0
    assert runtime_bot.calls == []
    assert "runtime Bot dispatch is forbidden in this stage" in (
        runtime_blocked["decisions"][0]["blockers"]
    )

    fake_bot = FakeBot()
    dispatcher = dispatcher_module.PreviewTelegramDispatcher(
        fake_bot,
        client_classification=dispatcher_module.TEST_DOUBLE,
        connector_registered=True,
    )
    result = await dispatcher.dispatch(
        plan,
        transport_policy=_transport_policy(),
        dispatcher_policy=_dispatcher_policy(),
    )
    assert result["owner"] == dispatcher_module.OWNER
    assert result["lifecycle_status"] == dispatcher_module.LIFECYCLE_STATUS
    assert result["runtime_commit"] == BASE_COMMIT
    assert result["test_ids"] == list(dispatcher_module.TEST_IDS)
    assert result["runtime_evidence"] == "FAKE_BOT_ASYNC_ONLY"
    assert result["activation_approval_id"] == APPROVAL_ID
    assert result["activation_scope"] == "PRIVATE_TEST_CHAT_PREVIEW_ONLY"
    checklist = result["checklist_metadata"]
    assert checklist["owner"] == dispatcher_module.OWNER
    assert checklist["lifecycle_status"] == dispatcher_module.LIFECYCLE_STATUS
    assert checklist["commit"] == BASE_COMMIT
    assert checklist["test_ids"] == list(dispatcher_module.TEST_IDS)
    assert checklist["runtime_evidence"] == "FAKE_BOT_ASYNC_ONLY"
    assert checklist["activation_approval"]["id"] == APPROVAL_ID
    assert checklist["activation_approval"]["production_authorized"] is False
    assert result["scheduler_registered"] is False
    assert result["worker_registered"] is False
    assert result["production_imported"] is False
    assert result["fake_bot_calls"] == 1
    assert result["fake_messages_recorded"] == 1
    assert result["delivery_attempts"] == 0
    assert result["telegram_api_calls"] == 0
    assert result["database_writes"] == 0
    assert result["research_evidence_writes"] == 0
    assert result["research_evidence_effect"] == "NONE"
    assert result["delivery_channel"] == "NONE"
    assert result["live_effect"] == "NONE"
    assert len(fake_bot.calls) == 1
    assert fake_bot.calls[0]["chat_id"] == -1001
    assert fake_bot.calls[0]["text"].startswith(preview_contract.LABEL)

    repeated = await dispatcher.dispatch(
        deepcopy(plan),
        transport_policy=deepcopy(_transport_policy()),
        dispatcher_policy=deepcopy(_dispatcher_policy()),
    )
    assert repeated["fake_bot_calls"] == 0
    assert repeated["duplicates_skipped"] == 1
    assert len(fake_bot.calls) == 1

    restart_bot = FakeBot()
    restarted = dispatcher_module.PreviewTelegramDispatcher(
        restart_bot,
        client_classification=dispatcher_module.TEST_DOUBLE,
        connector_registered=True,
    )
    restart_result = await restarted.dispatch(
        plan,
        transport_policy=_transport_policy(),
        dispatcher_policy=_dispatcher_policy(),
        existing_request_keys=[result["decisions"][0]["request_key"]],
    )
    assert restart_result["duplicates_skipped"] == 1
    assert restart_bot.calls == []

    blocking_bot = BlockingFakeBot()
    concurrent = dispatcher_module.PreviewTelegramDispatcher(
        blocking_bot,
        client_classification=dispatcher_module.TEST_DOUBLE,
        connector_registered=True,
    )
    first = asyncio.create_task(
        concurrent.dispatch(
            plan,
            transport_policy=_transport_policy(),
            dispatcher_policy=_dispatcher_policy(),
        )
    )
    await blocking_bot.started.wait()
    second = await concurrent.dispatch(
        plan,
        transport_policy=_transport_policy(),
        dispatcher_policy=_dispatcher_policy(),
    )
    assert second["single_flight_skipped"] == 1
    assert second["fake_bot_calls"] == 0
    blocking_bot.release.set()
    first_result = await first
    assert first_result["fake_bot_calls"] == 1
    assert len(blocking_bot.calls) == 1

    cancellation_bot = BlockingFakeBot()
    cancellable = dispatcher_module.PreviewTelegramDispatcher(
        cancellation_bot,
        client_classification=dispatcher_module.TEST_DOUBLE,
        connector_registered=True,
    )
    task = asyncio.create_task(
        cancellable.dispatch(
            plan,
            transport_policy=_transport_policy(),
            dispatcher_policy=_dispatcher_policy(),
        )
    )
    await cancellation_bot.started.wait()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    else:
        raise AssertionError("cancelled fake dispatch must propagate cancellation")
    assert cancellable.inflight_count == 0
    assert cancellation_bot.calls == []
    cancellation_bot.release.set()
    recovered = await cancellable.dispatch(
        plan,
        transport_policy=_transport_policy(),
        dispatcher_policy=_dispatcher_policy(),
    )
    assert recovered["fake_bot_calls"] == 1
    assert len(cancellation_bot.calls) == 1


def run() -> None:
    asyncio.run(_run_async())

    try:
        dispatcher_module.PreviewTelegramDispatcher(
            FakeBot(),
            connector_registered=True,
        )
    except ValueError as exc:
        assert "runtime connector registration is forbidden" in str(exc)
    else:
        raise AssertionError("runtime Bot registration must be forbidden")

    source = (
        ROOT / "research_experimental_preview_telegram_dispatcher.py"
    ).read_text(encoding="utf-8").lower()
    for forbidden in (
        "import telegram",
        "from telegram",
        "import psycopg",
        "import sqlite",
        "os.getenv",
        "execute(",
        "reply_text(",
        "research_formula_store",
        "research_formula_worker",
    ):
        assert forbidden not in source
    assert "await self._bot.send_message(" in source

    for production_file in (
        "main.py",
        "ai_telegram.py",
        "research_formula_worker.py",
        "research_formula_store.py",
        "research_formula_schema_admin.py",
    ):
        production_source = (ROOT / production_file).read_text(encoding="utf-8")
        assert "research_experimental_preview_telegram_dispatcher" not in (
            production_source
        )

    print("research_experimental_preview_telegram_dispatcher_selftest: ok")


if __name__ == "__main__":
    run()
