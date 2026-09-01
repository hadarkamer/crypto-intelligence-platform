"""Regressions for the zero-authority first PREVIEW message candidate."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import research_experimental_preview_contract as preview_contract
import research_experimental_preview_contract_selftest as preview_fixtures
import research_experimental_preview_first_message_authorization as authorization
import research_experimental_preview_staging_activation_gate as activation_gate
import research_experimental_preview_staging_activation_gate_selftest as gate_fixtures
import research_experimental_preview_staging_config as staging_config
import research_experimental_preview_staging_registration as staging_registration


ROOT = Path(__file__).resolve().parent
CHAT_ID = -1001


class FakeStagingBot:
    def __init__(self) -> None:
        self.calls = []

    async def send_message(self, **kwargs):
        self.calls.append(dict(kwargs))
        return {"fake": True}


def _plan() -> dict:
    return preview_contract.plan_preview_only(
        preview_fixtures._gate_plan(),
        policy=preview_fixtures._preview_policy(),
    )


def _transport_policy() -> dict:
    return {
        "enabled": True,
        "kill_switch_engaged": False,
        "owner_transport_approved": True,
        "test_chat_id": CHAT_ID,
    }


def _ready_registration(*, chat_id: int = CHAT_ID):
    approval = gate_fixtures._approval_record(
        authorized=True,
        chat_id=chat_id,
    )
    configuration = staging_config.resolve_staging_configuration(
        {
            staging_config.ENABLED_ENV: "1",
            staging_config.KILL_SWITCH_ENV: "0",
            staging_config.OWNER_APPROVED_ENV: "1",
            staging_config.TEST_CHAT_ID_ENV: str(chat_id),
            staging_config.RUNTIME_COMMIT_ENV: gate_fixtures.RUNTIME_COMMIT,
            staging_config.ACTIVATION_APPROVAL_ENV: approval[
                "activation_approval_id"
            ],
        }
    )
    gate = activation_gate.observe_activation_gate(
        configuration,
        actual_runtime_commit=gate_fixtures.RUNTIME_COMMIT,
        approval_record_json=approval,
    )
    registration = staging_registration.DisabledStagingPreviewRegistration()
    bot = FakeStagingBot()
    registration.bind_disabled_runtime_bot(bot, configuration=configuration)
    registration.prepare_runtime_connector_candidate(gate)
    status = registration.register_runtime_connector_no_dispatch(gate)
    return status, gate, bot


def _expect_error(fragment: str, callback) -> None:
    try:
        callback()
    except ValueError as exc:
        assert fragment in str(exc), exc
    else:
        raise AssertionError(f"expected ValueError containing {fragment!r}")


def run() -> None:
    plan = _plan()

    safe_configuration = staging_config.resolve_staging_configuration({})
    safe_gate = activation_gate.observe_activation_gate(
        safe_configuration,
        actual_runtime_commit=None,
        approval_record_json=None,
    )
    safe_registration = (
        staging_registration.DisabledStagingPreviewRegistration()
    )
    safe_bot = FakeStagingBot()
    safe_registration.bind_disabled_runtime_bot(
        safe_bot,
        configuration=safe_configuration,
    )
    safe_registration.prepare_runtime_connector_candidate(safe_gate)
    safe_status = safe_registration.register_runtime_connector_no_dispatch(
        safe_gate
    )
    blocked = authorization.prepare_first_message_authorization_candidate(
        plan,
        registration_status=safe_status,
        activation_gate_status=safe_gate,
        transport_policy=_transport_policy(),
    )
    assert blocked["authorization_candidate_prepared"] is False
    assert blocked["authorization_candidate"] is None
    assert blocked["runtime_connector_registration_id"] is None
    assert blocked["one_shot_key"] is None
    assert "activation gate prerequisites are incomplete" in blocked[
        "authorization_blockers"
    ]
    assert "registered-no-dispatch runtime receipt is absent" in blocked[
        "authorization_blockers"
    ]
    assert blocked["authorization_granted"] is False
    assert blocked["dispatch_allowed"] is False
    assert blocked["delivery_allowed"] is False
    assert blocked["delivery_attempts"] == 0
    assert blocked["telegram_api_calls"] == 0
    assert safe_bot.calls == []

    registered, ready_gate, ready_bot = _ready_registration()
    receipt = staging_registration.verify_registered_no_dispatch_status(
        registered
    )
    assert receipt["connector_registered"] is True
    assert receipt["dispatch_exposed"] is False
    assert receipt["telegram_api_calls"] == 0

    prepared = authorization.prepare_first_message_authorization_candidate(
        plan,
        registration_status=registered,
        activation_gate_status=ready_gate,
        transport_policy=_transport_policy(),
    )
    assert prepared["mode"] == authorization.MODE
    assert prepared["lifecycle_status"] == authorization.LIFECYCLE_STATUS
    assert prepared["authorization_candidate_prepared"] is True
    assert prepared["authorization_blockers"] == []
    assert len(prepared["one_shot_key"]) == 64
    assert prepared["authorization_required"] is True
    assert prepared["authorization_granted"] is False
    assert prepared["authorization_consumed"] is False
    assert prepared["dispatch_allowed"] is False
    assert prepared["delivery_allowed"] is False
    assert prepared["handler_registered"] is False
    assert prepared["scheduler_registered"] is False
    assert prepared["worker_registered"] is False
    assert prepared["public_opt_in"] is False
    assert prepared["stage6_activated"] is False
    assert prepared["delivery_attempts"] == 0
    assert prepared["telegram_api_calls"] == 0
    assert prepared["database_writes"] == 0
    assert prepared["research_evidence_writes"] == 0
    assert prepared["research_evidence_effect"] == "NONE"
    assert prepared["delivery_channel"] == "NONE"
    assert prepared["live_effect"] == "NONE"
    assert ready_bot.calls == []

    candidate = prepared["authorization_candidate"]
    assert candidate["status"] == authorization.STATUS
    assert candidate["owner"] == activation_gate.OWNER
    assert candidate["scope"] == activation_gate.SCOPE
    assert candidate["route"] == activation_gate.ROUTE
    assert len(candidate["authorization_candidate_id"]) == 64
    assert candidate["one_shot_key"] == prepared["one_shot_key"]
    assert candidate["chunk_count"] == 1
    assert candidate["authorization_granted"] is False
    assert candidate["authorization_consumed"] is False
    assert candidate["candidate_id_may_be_used_as_authorization_id"] is False
    assert candidate["dispatch_allowed"] is False
    assert candidate["delivery_allowed"] is False
    assert "chat_id" not in candidate
    assert "text" not in candidate
    assert (
        authorization.verify_first_message_authorization_candidate(candidate)
        == candidate
    )

    repeated = authorization.prepare_first_message_authorization_candidate(
        deepcopy(plan),
        registration_status=deepcopy(registered),
        activation_gate_status=deepcopy(ready_gate),
        transport_policy=deepcopy(_transport_policy()),
    )
    assert repeated == prepared
    assert ready_bot.calls == []

    duplicate = authorization.prepare_first_message_authorization_candidate(
        plan,
        registration_status=registered,
        activation_gate_status=ready_gate,
        transport_policy=_transport_policy(),
        existing_one_shot_keys=[prepared["one_shot_key"]],
    )
    assert duplicate["authorization_candidate_prepared"] is False
    assert duplicate["authorization_candidate"] is None
    assert "first-message one-shot key already exists" in duplicate[
        "authorization_blockers"
    ]

    other_registration, other_gate, other_bot = _ready_registration(chat_id=1001)
    wrong_destination = (
        authorization.prepare_first_message_authorization_candidate(
            plan,
            registration_status=other_registration,
            activation_gate_status=other_gate,
            transport_policy=_transport_policy(),
        )
    )
    assert wrong_destination["authorization_candidate_prepared"] is False
    assert "first-message destination differs from registration" in (
        wrong_destination["authorization_blockers"]
    )
    assert other_bot.calls == []

    tampered_registration = deepcopy(registered)
    tampered_registration["runtime_connector_registration_id"] = "0" * 64
    _expect_error(
        "registration fingerprint mismatch",
        lambda: authorization.prepare_first_message_authorization_candidate(
            plan,
            registration_status=tampered_registration,
            activation_gate_status=ready_gate,
            transport_policy=_transport_policy(),
        ),
    )
    tampered_gate = deepcopy(ready_gate)
    tampered_gate["activation_gate_id"] = "0" * 64
    _expect_error(
        "activation gate fingerprint mismatch",
        lambda: authorization.prepare_first_message_authorization_candidate(
            plan,
            registration_status=registered,
            activation_gate_status=tampered_gate,
            transport_policy=_transport_policy(),
        ),
    )
    tampered_candidate = deepcopy(candidate)
    tampered_candidate["message_sha256"] = "0" * 64
    _expect_error(
        "one-shot key fingerprint mismatch",
        lambda: authorization.verify_first_message_authorization_candidate(
            tampered_candidate
        ),
    )
    assert ready_bot.calls == []

    source = (
        ROOT / "research_experimental_preview_first_message_authorization.py"
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
        "research_formula_store",
        "research_formula_worker",
    ):
        assert forbidden not in source

    for disconnected_file in (
        "ai_candidate_main.py",
        "main.py",
        "ai_telegram.py",
        "research_formula_worker.py",
        "research_formula_store.py",
        "research_formula_schema_admin.py",
    ):
        disconnected_source = (ROOT / disconnected_file).read_text(
            encoding="utf-8"
        )
        assert (
            "research_experimental_preview_first_message_authorization"
            not in disconnected_source
        )

    print("research_experimental_preview_first_message_authorization_selftest: ok")


if __name__ == "__main__":
    run()
