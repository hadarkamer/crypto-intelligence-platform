"""Safety regressions for the unregistered PREVIEW_ONLY Telegram adapter."""

from __future__ import annotations

from copy import deepcopy
from inspect import signature
from pathlib import Path

from telegram import Bot

import research_experimental_preview_contract as preview_contract
import research_experimental_preview_contract_selftest as preview_fixtures
import research_experimental_preview_telegram_adapter as adapter


ROOT = Path(__file__).resolve().parent


def _plan(*, stage5_status: str = "WAITING_DATA") -> dict:
    gate_plan = preview_fixtures._gate_plan(stage5_status=stage5_status)
    return preview_contract.plan_preview_only(
        gate_plan,
        policy=preview_fixtures._preview_policy(),
    )


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
    default_blocked = adapter.prepare_unregistered_telegram_requests(plan)
    assert default_blocked["transport_envelopes_received"] == 0
    assert default_blocked["adapter_requests_prepared"] == 0
    assert default_blocked["telegram_api_calls"] == 0

    prepared = adapter.prepare_unregistered_telegram_requests(
        plan,
        transport_policy=_transport_policy(),
    )
    assert prepared["mode"] == adapter.MODE
    assert prepared["registration_state"] == adapter.REGISTRATION_STATE
    assert prepared["connector_registered"] is False
    assert prepared["activation_allowed"] is False
    assert prepared["transport_connected"] is False
    assert prepared["transport_envelopes_received"] == 1
    assert prepared["adapter_requests_prepared"] == 1
    assert prepared["adapter_requests_blocked"] == 1
    assert prepared["public_opt_in"] is False
    assert prepared["stage6_activated"] is False
    assert prepared["delivery_attempts"] == 0
    assert prepared["telegram_api_calls"] == 0
    assert prepared["database_writes"] == 0
    assert prepared["research_evidence_writes"] == 0
    assert prepared["research_evidence_effect"] == "NONE"
    assert prepared["delivery_channel"] == "NONE"
    assert prepared["live_effect"] == "NONE"

    request = prepared["requests"][0]
    assert request["method"] == "Bot.send_message"
    assert request["status"] == adapter.REQUEST_STATUS
    assert request["kwargs"]["chat_id"] == -1001
    assert request["kwargs"]["text"].startswith(preview_contract.LABEL)
    assert request["kwargs"]["parse_mode"] is None
    assert request["kwargs"]["disable_web_page_preview"] is True
    assert request["kwargs"]["protect_content"] is True
    assert request["chunk_index"] == 1
    assert request["chunk_count"] == 1
    assert request["research_evidence_effect"] == "NONE"

    sdk_parameters = set(signature(Bot.send_message).parameters)
    assert set(request["kwargs"]).issubset(sdk_parameters)

    repeated = adapter.prepare_unregistered_telegram_requests(
        deepcopy(plan),
        transport_policy=deepcopy(_transport_policy()),
    )
    assert repeated == prepared

    duplicate = adapter.prepare_unregistered_telegram_requests(
        plan,
        transport_policy=_transport_policy(),
        existing_transport_keys=[
            prepared["requests"][0]["transport_key"]
        ],
    )
    assert duplicate["adapter_requests_prepared"] == 0

    stage5_ready = adapter.prepare_unregistered_telegram_requests(
        _plan(stage5_status="READY"),
        transport_policy=_transport_policy(),
    )
    assert stage5_ready["adapter_requests_prepared"] == 0

    long_text = preview_contract.LABEL + "\n\n" + ("בדיקה ארוכה " * 1000)
    chunks = adapter._chunks(long_text)
    assert len(chunks) > 1
    assert all(0 < len(chunk) <= adapter.TELEGRAM_MESSAGE_LIMIT for chunk in chunks)
    assert "".join(chunks).replace(" ", "") == long_text.replace(" ", "")

    _raises("between 1 and 4096", lambda: adapter._chunks("x", limit=4097))
    tampered = deepcopy(plan)
    tampered["previews"][0]["text"] += " tampered"
    _raises(
        "decision fingerprint mismatch",
        lambda: adapter.prepare_unregistered_telegram_requests(
            tampered,
            transport_policy=_transport_policy(),
        ),
    )

    source = (
        ROOT / "research_experimental_preview_telegram_adapter.py"
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
        assert "research_experimental_preview_telegram_adapter" not in (
            production_source
        )

    print("research_experimental_preview_telegram_adapter_selftest: ok")


if __name__ == "__main__":
    run()
