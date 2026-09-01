"""No-send regressions for disabled PREVIEW_ONLY staging registration."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import research_experimental_preview_staging_activation_gate as activation_gate
import research_experimental_preview_staging_activation_gate_selftest as gate_fixtures
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
    assert initial["activation_gate_observed"] is False
    assert initial["activation_gate_id"] is None
    assert initial["runtime_connector_candidate_prepared"] is False
    assert initial["runtime_connector_candidate_id"] is None
    assert initial["runtime_connector_registration_id"] is None
    assert initial["runtime_connector_registered_no_dispatch"] is False
    assert initial["dispatch_exposed"] is False

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
    assert bound["activation_gate_observed"] is False
    assert bound["activation_gate_id"] is None
    assert bound["runtime_connector_candidate_prepared"] is False
    assert bound["runtime_connector_candidate_id"] is None
    assert bound["runtime_connector_registration_id"] is None
    assert bound["runtime_connector_registered_no_dispatch"] is False
    assert bound["connector_registered"] is False
    assert bound["activation_allowed"] is False
    assert bound["dispatch_exposed"] is False
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

    blocked_gate = activation_gate.observe_activation_gate(
        configured,
        actual_runtime_commit="9" * 40,
        approval_record_json=None,
    )
    blocked_candidate = registration.prepare_runtime_connector_candidate(
        blocked_gate
    )
    assert blocked_candidate["activation_gate_observed"] is True
    assert blocked_candidate["activation_gate_id"] == blocked_gate[
        "activation_gate_id"
    ]
    assert blocked_candidate["runtime_connector_candidate_prepared"] is False
    assert blocked_candidate["runtime_connector_candidate_id"] is None
    assert "activation gate prerequisites are incomplete" in (
        blocked_candidate["runtime_connector_registration_blockers"]
    )
    assert blocked_candidate["connector_registered"] is False
    assert blocked_candidate["activation_allowed"] is False
    assert blocked_candidate["dispatch_exposed"] is False
    assert bot.calls == []

    blocked_registration = (
        registration.register_runtime_connector_no_dispatch(blocked_gate)
    )
    assert blocked_registration["runtime_connector_candidate_prepared"] is False
    assert blocked_registration["runtime_connector_registration_id"] is None
    assert (
        blocked_registration["runtime_connector_registered_no_dispatch"]
        is False
    )
    assert "runtime connector candidate is not prepared" in (
        blocked_registration["runtime_connector_registration_blockers"]
    )
    assert blocked_registration["connector_registered"] is False
    assert blocked_registration["dispatch_exposed"] is False
    assert blocked_registration["delivery_attempts"] == 0
    assert blocked_registration["telegram_api_calls"] == 0
    assert bot.calls == []

    blocked_repeated = registration.prepare_runtime_connector_candidate(
        deepcopy(blocked_gate)
    )
    assert blocked_repeated == blocked_candidate
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
    assert unbound["activation_gate_observed"] is False
    assert unbound["activation_gate_id"] is None
    assert unbound["runtime_connector_candidate_prepared"] is False
    assert unbound["runtime_connector_registration_id"] is None
    assert unbound["runtime_connector_registered_no_dispatch"] is False
    assert bot.calls == []

    future_approval = gate_fixtures._approval_record(authorized=True)
    future_configuration = gate_fixtures._configuration(
        future_approval["activation_approval_id"],
        enabled=True,
        kill_switch=False,
    )
    future_gate = activation_gate.observe_activation_gate(
        future_configuration,
        actual_runtime_commit=gate_fixtures.RUNTIME_COMMIT,
        approval_record_json=future_approval,
    )
    ready_registration = registration_module.DisabledStagingPreviewRegistration()
    ready_bot = FakeStagingBot()
    ready_registration.bind_disabled_runtime_bot(
        ready_bot,
        configuration=future_configuration,
    )
    ready_candidate = ready_registration.prepare_runtime_connector_candidate(
        future_gate
    )
    assert ready_candidate["activation_gate_observed"] is True
    assert ready_candidate["activation_gate_id"] == future_gate[
        "activation_gate_id"
    ]
    assert ready_candidate["runtime_connector_candidate_prepared"] is True
    assert len(ready_candidate["runtime_connector_candidate_id"]) == 64
    assert ready_candidate["runtime_connector_registration_blockers"] == []
    assert ready_candidate["client_classification"] == (
        dispatcher_module.RUNTIME_BOT_UNREGISTERED
    )
    assert ready_candidate["connector_registered"] is False
    assert ready_candidate["activation_allowed"] is False
    assert ready_candidate["dispatch_exposed"] is False
    assert ready_candidate["handler_registered"] is False
    assert ready_candidate["scheduler_registered"] is False
    assert ready_candidate["worker_registered"] is False
    assert ready_candidate["telegram_api_calls"] == 0
    assert ready_bot.calls == []

    registered = ready_registration.register_runtime_connector_no_dispatch(
        future_gate
    )
    assert registered["runtime_connector_candidate_prepared"] is True
    assert len(registered["runtime_connector_candidate_id"]) == 64
    assert len(registered["runtime_connector_registration_id"]) == 64
    assert registered["runtime_connector_registered_no_dispatch"] is True
    assert registered["runtime_connector_registration_blockers"] == []
    assert registered["client_classification"] == (
        dispatcher_module.RUNTIME_BOT_REGISTERED_NO_DISPATCH
    )
    assert registered["connector_registered"] is True
    assert registered["activation_allowed"] is False
    assert registered["dispatch_exposed"] is False
    assert registered["handler_registered"] is False
    assert registered["scheduler_registered"] is False
    assert registered["worker_registered"] is False
    assert registered["delivery_attempts"] == 0
    assert registered["telegram_api_calls"] == 0
    assert registered["database_writes"] == 0
    assert registered["live_effect"] == "NONE"
    assert ready_bot.calls == []

    verified_registration = (
        registration_module.verify_registered_no_dispatch_status(registered)
    )
    assert verified_registration["runtime_connector_registration_id"] == (
        registered["runtime_connector_registration_id"]
    )
    assert verified_registration["activation_gate_id"] == future_gate[
        "activation_gate_id"
    ]
    assert verified_registration["connector_registered"] is True
    assert verified_registration["dispatch_exposed"] is False
    assert verified_registration["telegram_api_calls"] == 0

    repeated_registration = (
        ready_registration.register_runtime_connector_no_dispatch(
            deepcopy(future_gate)
        )
    )
    assert repeated_registration == registered
    assert ready_bot.calls == []

    tampered_registration = deepcopy(registered)
    tampered_registration["runtime_connector_registration_id"] = "0" * 64
    try:
        registration_module.verify_registered_no_dispatch_status(
            tampered_registration
        )
    except ValueError as exc:
        assert "registration fingerprint mismatch" in str(exc)
    else:
        raise AssertionError("tampered registration receipt must be rejected")

    tampered_gate = deepcopy(future_gate)
    tampered_gate["activation_gate_id"] = "0" * 64
    try:
        ready_registration.prepare_runtime_connector_candidate(tampered_gate)
    except ValueError as exc:
        assert "fingerprint mismatch" in str(exc)
    else:
        raise AssertionError("tampered activation gate must be rejected")
    assert ready_bot.calls == []

    ready_registration.unbind_runtime_bot(ready_bot)

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
    assert (
        "preview_staging.REGISTRATION.prepare_runtime_connector_candidate"
        in candidate
    )
    assert (
        "preview_staging.REGISTRATION.register_runtime_connector_no_dispatch"
        in candidate
    )
    assert "preview_staging.REGISTRATION.unbind_runtime_bot" in candidate
    assert "preview_registration.dispatch" not in candidate
    assert "runtime_connector_candidate.dispatch" not in candidate

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
