"""Regressions for the disconnected PREVIEW_ONLY transport boundary."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import research_experimental_preview_contract as preview_contract
import research_experimental_preview_contract_selftest as preview_fixtures
import research_experimental_preview_transport_contract as transport


ROOT = Path(__file__).resolve().parent


def _plan(*, stage5_status: str = "WAITING_DATA", disabled: bool = False) -> dict:
    gate_plan = preview_fixtures._gate_plan(stage5_status=stage5_status)
    policy = None if disabled else preview_fixtures._preview_policy()
    return preview_contract.plan_preview_only(gate_plan, policy=policy)


def _transport_policy(**overrides) -> dict:
    policy = {
        "enabled": True,
        "kill_switch_engaged": False,
        "owner_transport_approved": True,
        "test_chat_id": -1001,
    }
    policy.update(overrides)
    return policy


def _raises(expected: str, function) -> None:
    try:
        function()
    except ValueError as exc:
        assert expected in str(exc), exc
    else:
        raise AssertionError(f"expected ValueError containing {expected!r}")


def run() -> None:
    plan = _plan()
    default_blocked = transport.prepare_private_test_chat_envelopes(plan)
    assert default_blocked["transport_envelopes_prepared"] == 0
    assert default_blocked["suppressed"] == 1
    blockers = default_blocked["decisions"][0]["blockers"]
    assert "preview transport feature flag is disabled" in blockers
    assert "preview transport kill switch is engaged" in blockers
    assert "owner approval for preview transport is absent" in blockers
    assert "private test chat is not configured" in blockers

    ready = transport.prepare_private_test_chat_envelopes(
        plan,
        policy=_transport_policy(),
    )
    assert ready["mode"] == transport.MODE
    assert ready["transport_envelopes_prepared"] == 1
    assert ready["suppressed"] == 0
    assert ready["connector_registered"] is False
    assert ready["transport_connected"] is False
    assert ready["public_opt_in"] is False
    assert ready["stage6_activated"] is False
    assert ready["delivery_attempts"] == 0
    assert ready["telegram_api_calls"] == 0
    assert ready["database_writes"] == 0
    assert ready["research_evidence_writes"] == 0
    assert ready["research_evidence_effect"] == "NONE"
    assert ready["delivery_channel"] == "NONE"
    assert ready["live_effect"] == "NONE"
    assert ready["decisions"][0]["status"] == transport.ENVELOPE_PREPARED

    envelope = ready["transport_envelopes"][0]
    assert envelope["transport"] == transport.TRANSPORT
    assert envelope["chat_id"] == -1001
    assert envelope["text"].startswith(preview_contract.LABEL)
    assert envelope["parse_mode"] is None
    assert envelope["disable_web_page_preview"] is True
    assert envelope["protect_content"] is True
    assert envelope["public_opt_in"] is False
    assert envelope["stage6_activated"] is False
    assert envelope["research_evidence_effect"] == "NONE"

    repeated = transport.prepare_private_test_chat_envelopes(
        deepcopy(plan),
        policy=deepcopy(_transport_policy()),
    )
    assert repeated == ready

    duplicate = transport.prepare_private_test_chat_envelopes(
        plan,
        policy=_transport_policy(),
        existing_transport_keys=[ready["decisions"][0]["transport_key"]],
    )
    assert duplicate["transport_envelopes_prepared"] == 0
    assert "preview transport idempotency key already exists" in (
        duplicate["decisions"][0]["blockers"]
    )

    wrong_chat = transport.prepare_private_test_chat_envelopes(
        plan,
        policy=_transport_policy(test_chat_id=-9999),
    )
    assert wrong_chat["transport_envelopes_prepared"] == 0
    assert "transport destination differs from preview chat" in (
        wrong_chat["decisions"][0]["blockers"]
    )

    stage5_ready = transport.prepare_private_test_chat_envelopes(
        _plan(stage5_status="READY"),
        policy=_transport_policy(),
    )
    assert stage5_ready["transport_envelopes_prepared"] == 0
    assert "PREVIEW_ONLY transport closes when Stage 5 is READY" in (
        stage5_ready["decisions"][0]["blockers"]
    )

    disabled_preview = transport.prepare_private_test_chat_envelopes(
        _plan(disabled=True),
        policy=_transport_policy(),
    )
    assert disabled_preview["transport_envelopes_prepared"] == 0
    assert "source PREVIEW_ONLY decision is suppressed" in (
        disabled_preview["decisions"][0]["blockers"]
    )

    tampered = deepcopy(plan)
    tampered["previews"][0]["text"] += " tampered"
    _raises(
        "decision fingerprint mismatch",
        lambda: transport.prepare_private_test_chat_envelopes(
            tampered,
            policy=_transport_policy(),
        ),
    )
    _raises(
        "unknown fields",
        lambda: transport.prepare_private_test_chat_envelopes(
            plan,
            policy={**_transport_policy(), "public_opt_in": True},
        ),
    )

    source = (
        ROOT / "research_experimental_preview_transport_contract.py"
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
        "research_formula_store",
        "research_formula_worker",
    ):
        assert forbidden not in source

    for production_file in (
        "main.py",
        "ai_telegram.py",
        "research_formula_worker.py",
        "research_formula_store.py",
        "research_formula_schema_admin.py",
    ):
        production_source = (ROOT / production_file).read_text(encoding="utf-8")
        assert "research_experimental_preview_transport_contract" not in (
            production_source
        )

    print("research_experimental_preview_transport_contract_selftest: ok")


if __name__ == "__main__":
    run()
