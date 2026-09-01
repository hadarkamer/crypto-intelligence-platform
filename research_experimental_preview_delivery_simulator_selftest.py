"""Safety regressions for the disconnected PREVIEW_ONLY delivery simulator."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import research_experimental_preview_contract as preview_contract
import research_experimental_preview_contract_selftest as preview_fixtures
import research_experimental_preview_delivery_simulator as simulator


ROOT = Path(__file__).resolve().parent


def _plan(*, stage5_status: str = "WAITING_DATA", disabled: bool = False) -> dict:
    gate_plan = preview_fixtures._gate_plan(stage5_status=stage5_status)
    policy = None if disabled else preview_fixtures._preview_policy()
    return preview_contract.plan_preview_only(gate_plan, policy=policy)


def _raises(expected: str, function) -> None:
    try:
        function()
    except ValueError as exc:
        assert expected in str(exc), exc
    else:
        raise AssertionError(f"expected ValueError containing {expected!r}")


def run() -> None:
    plan = _plan()
    recorder = simulator.InMemoryPreviewTelegramDouble()
    result = simulator.simulate_preview_delivery(plan, recorder=recorder)
    assert result["mode"] == simulator.MODE
    assert result["simulation_channel"] == simulator.SIMULATION_CHANNEL
    assert result["decisions_considered"] == 1
    assert result["messages_recorded_in_memory"] == 1
    assert result["duplicates_skipped"] == 0
    assert result["suppressed"] == 0
    assert result["public_opt_in"] is False
    assert result["stage6_activated"] is False
    assert result["delivery_attempts"] == 0
    assert result["telegram_api_calls"] == 0
    assert result["database_writes"] == 0
    assert result["research_evidence_writes"] == 0
    assert result["research_evidence_effect"] == "NONE"
    assert result["delivery_channel"] == "NONE"
    assert result["live_effect"] == "NONE"
    assert result["decisions"][0]["status"] == simulator.RECORDED

    records = recorder.records()
    assert len(records) == 1
    assert records[0]["text"].startswith(preview_contract.LABEL)
    assert records[0]["preview_key"] == plan["previews"][0]["preview_key"]
    assert records[0]["research_evidence_effect"] == "NONE"

    replay = simulator.simulate_preview_delivery(
        deepcopy(plan), recorder=recorder
    )
    assert replay["messages_recorded_in_memory"] == 0
    assert replay["duplicates_skipped"] == 1
    assert replay["decisions"][0]["status"] == simulator.SKIPPED_DUPLICATE
    assert len(recorder.records()) == 1

    disabled = simulator.simulate_preview_delivery(
        _plan(disabled=True),
        recorder=simulator.InMemoryPreviewTelegramDouble(),
    )
    assert disabled["messages_recorded_in_memory"] == 0
    assert disabled["suppressed"] == 1

    stage5_ready = simulator.simulate_preview_delivery(
        _plan(stage5_status="READY"),
        recorder=simulator.InMemoryPreviewTelegramDouble(),
    )
    assert stage5_ready["messages_recorded_in_memory"] == 0
    assert stage5_ready["suppressed"] == 1

    tampered = deepcopy(plan)
    tampered["previews"][0]["text"] += " tampered"
    _raises(
        "decision fingerprint mismatch",
        lambda: simulator.simulate_preview_delivery(
            tampered,
            recorder=simulator.InMemoryPreviewTelegramDouble(),
        ),
    )

    class UnsafeSender(simulator.InMemoryPreviewTelegramDouble):
        pass

    _raises(
        "sealed in-memory",
        lambda: simulator.simulate_preview_delivery(
            plan,
            recorder=UnsafeSender(),
        ),
    )

    source = (
        ROOT / "research_experimental_preview_delivery_simulator.py"
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
        assert "research_experimental_preview_delivery_simulator" not in (
            production_source
        )

    print("research_experimental_preview_delivery_simulator_selftest: ok")


if __name__ == "__main__":
    run()
