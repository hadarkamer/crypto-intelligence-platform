"""No-send regressions for disabled PREVIEW_ONLY staging registration."""

from __future__ import annotations

from pathlib import Path

import research_experimental_preview_staging_registration as registration_module
import research_experimental_preview_staging_config as staging_config
import research_experimental_preview_telegram_dispatcher as dispatcher_module


ROOT = Path(__file__).resolve().parent


class FakeStagingBot:
    def __init__(self) -> None:
        self.calls = []

    async def send_message(self, **kwargs):
        self.calls.append(dict(kwargs))
        return {"fake": True}


def run() -> None:
    registration = registration_module.DisabledStagingPreviewRegistration()
    initial = registration.status()
    assert initial["runtime_bot_bound"] is False
    assert initial["binding_id"] is None
    assert initial["enabled"] is False
    assert initial["kill_switch_engaged"] is True

    bot = FakeStagingBot()
    configured = staging_config.resolve_staging_configuration(
        {
            staging_config.OWNER_APPROVED_ENV: "1",
            staging_config.TEST_CHAT_ID_ENV: "-1001",
            staging_config.RUNTIME_COMMIT_ENV: "9" * 40,
            staging_config.ACTIVATION_APPROVAL_ENV: "a" * 64,
        }
    )
    bound = registration.bind_disabled_runtime_bot(
        bot,
        configuration=configured,
    )
    assert bound["mode"] == registration_module.MODE
    assert bound["owner"] == dispatcher_module.OWNER
    assert bound["lifecycle_status"] == registration_module.LIFECYCLE_STATUS
    assert bound["base_commit"] == registration_module.BASE_COMMIT
    assert len(bound["binding_id"]) == 64
    assert bound["runtime_bot_bound"] is True
    assert bound["client_classification"] == (
        dispatcher_module.RUNTIME_BOT_UNREGISTERED
    )
    assert bound["enabled"] is False
    assert bound["kill_switch_engaged"] is True
    assert bound["test_chat_configured"] is True
    assert len(bound["test_chat_binding_sha256"]) == 64
    assert bound["runtime_commit_configured"] is True
    assert bound["activation_approval_configured"] is True
    assert bound["configuration_prerequisites_complete"] is True
    assert bound["connector_registered"] is False
    assert bound["activation_allowed"] is False
    assert bound["handler_registered"] is False
    assert bound["scheduler_registered"] is False
    assert bound["worker_registered"] is False
    assert bound["public_opt_in"] is False
    assert bound["stage6_activated"] is False
    assert bound["delivery_attempts"] == 0
    assert bound["telegram_api_calls"] == 0
    assert bound["database_writes"] == 0
    assert bound["research_evidence_writes"] == 0
    assert bound["research_evidence_effect"] == "NONE"
    assert bound["delivery_channel"] == "NONE"
    assert bound["live_effect"] == "NONE"
    assert bot.calls == []

    repeated = registration.bind_disabled_runtime_bot(
        bot,
        configuration=configured,
    )
    assert repeated == bound
    assert bot.calls == []

    other = FakeStagingBot()
    try:
        registration.bind_disabled_runtime_bot(
            other,
            configuration=configured,
        )
    except RuntimeError as exc:
        assert "different staging Bot" in str(exc)
    else:
        raise AssertionError("a second staging Bot must not replace the binding")

    try:
        registration.unbind_runtime_bot(other)
    except RuntimeError as exc:
        assert "different staging Bot" in str(exc)
    else:
        raise AssertionError("a different staging Bot must not unbind the owner")

    unbound = registration.unbind_runtime_bot(bot)
    assert unbound["runtime_bot_bound"] is False
    assert unbound["binding_id"] is None
    assert bot.calls == []

    source = (
        ROOT / "research_experimental_preview_staging_registration.py"
    ).read_text(encoding="utf-8").lower()
    for forbidden in (
        "import telegram",
        "from telegram",
        "import psycopg",
        "import sqlite",
        "os.getenv",
        "execute(",
        "send_message(",
        "reply_text(",
        "create_task(",
        "commandhandler(",
    ):
        assert forbidden not in source

    candidate = (ROOT / "ai_candidate_main.py").read_text(encoding="utf-8")
    assert (
        "import research_experimental_preview_staging_registration as "
        "preview_staging" in candidate
    )
    assert "preview_staging.REGISTRATION.bind_disabled_runtime_bot" in candidate
    assert "preview_staging.REGISTRATION.unbind_runtime_bot" in candidate
    assert "preview_registration.dispatch" not in candidate

    for production_file in (
        "main.py",
        "ai_telegram.py",
        "research_formula_worker.py",
        "research_formula_store.py",
        "research_formula_schema_admin.py",
    ):
        production_source = (ROOT / production_file).read_text(encoding="utf-8")
        assert "research_experimental_preview_staging_registration" not in (
            production_source
        )

    print("research_experimental_preview_staging_registration_selftest: ok")


if __name__ == "__main__":
    run()
