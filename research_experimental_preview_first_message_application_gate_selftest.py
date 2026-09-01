"""Regressions for the observe-only first-message application gate."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import research_experimental_preview_first_message_application_gate as application_gate
import research_experimental_preview_first_message_owner_approval as owner_approval
import research_experimental_preview_first_message_owner_approval_selftest as approval_fixtures


ROOT = Path(__file__).resolve().parent


def _approved():
    candidate, bot = approval_fixtures._candidate()
    prepared = owner_approval.prepare_first_message_owner_approval(
        candidate,
        approval_input=approval_fixtures._approval_input(),
    )
    assert prepared["owner_approval_record_prepared"] is True
    return candidate, prepared["owner_approval_record"], bot


def _expect_error(fragment: str, callback) -> None:
    try:
        callback()
    except ValueError as exc:
        assert fragment in str(exc), exc
    else:
        raise AssertionError(f"expected ValueError containing {fragment!r}")


def run() -> None:
    candidate, approval_record, bot = _approved()

    ready = application_gate.evaluate_first_message_application_gate(
        approval_record,
        authorization_candidate=candidate,
        observed_at_utc="2026-09-01T09:05:00Z",
    )
    assert ready["application_gate_version"] == (
        application_gate.APPLICATION_GATE_VERSION
    )
    assert ready["mode"] == application_gate.MODE
    assert ready["lifecycle_status"] == application_gate.LIFECYCLE_STATUS
    assert ready["status"] == application_gate.READY
    assert len(ready["application_gate_id"]) == 64
    assert ready["approval_current"] is True
    assert ready["owner_approval_unconsumed"] is True
    assert ready["one_shot_unconsumed"] is True
    assert ready["application_prerequisites_satisfied"] is True
    assert ready["application_blockers"] == []
    assert ready["application_allowed"] is False
    assert ready["approval_applied"] is False
    assert ready["authorization_consumed"] is False
    assert ready["dispatch_allowed"] is False
    assert ready["delivery_allowed"] is False
    assert ready["handler_registered"] is False
    assert ready["scheduler_registered"] is False
    assert ready["worker_registered"] is False
    assert ready["public_opt_in"] is False
    assert ready["stage6_activated"] is False
    assert ready["delivery_attempts"] == 0
    assert ready["telegram_api_calls"] == 0
    assert ready["database_writes"] == 0
    assert ready["research_evidence_writes"] == 0
    assert ready["research_evidence_effect"] == "NONE"
    assert ready["delivery_channel"] == "NONE"
    assert ready["live_effect"] == "NONE"
    serialized = json.dumps(ready, ensure_ascii=False, sort_keys=True)
    assert '"chat_id"' not in serialized
    assert '"text"' not in serialized
    assert bot.calls == []

    verified = application_gate.verify_first_message_application_gate_status(
        ready
    )
    assert verified == ready

    repeated = application_gate.evaluate_first_message_application_gate(
        deepcopy(approval_record),
        authorization_candidate=deepcopy(candidate),
        observed_at_utc="2026-09-01T09:05:00Z",
    )
    assert repeated == ready
    assert bot.calls == []

    at_approval = application_gate.evaluate_first_message_application_gate(
        approval_record,
        authorization_candidate=candidate,
        observed_at_utc="2026-09-01T09:00:00Z",
    )
    assert at_approval["status"] == application_gate.READY
    assert at_approval["application_prerequisites_satisfied"] is True

    too_early = application_gate.evaluate_first_message_application_gate(
        approval_record,
        authorization_candidate=candidate,
        observed_at_utc="2026-09-01T08:59:59Z",
    )
    assert too_early["status"] == application_gate.BLOCKED
    assert too_early["approval_current"] is False
    assert too_early["application_prerequisites_satisfied"] is False
    assert "first-message owner approval is not yet valid" in too_early[
        "application_blockers"
    ]
    assert too_early["application_allowed"] is False
    assert too_early["dispatch_allowed"] is False
    assert application_gate.verify_first_message_application_gate_status(
        too_early
    ) == too_early

    expired = application_gate.evaluate_first_message_application_gate(
        approval_record,
        authorization_candidate=candidate,
        observed_at_utc="2026-09-01T09:10:00Z",
    )
    assert expired["status"] == application_gate.BLOCKED
    assert expired["approval_current"] is False
    assert "first-message owner approval has expired" in expired[
        "application_blockers"
    ]
    assert expired["application_allowed"] is False
    assert expired["dispatch_allowed"] is False

    approval_consumed = (
        application_gate.evaluate_first_message_application_gate(
            approval_record,
            authorization_candidate=candidate,
            observed_at_utc="2026-09-01T09:05:00Z",
            consumed_owner_approval_ids=[approval_record["owner_approval_id"]],
        )
    )
    assert approval_consumed["status"] == application_gate.BLOCKED
    assert approval_consumed["owner_approval_unconsumed"] is False
    assert "first-message owner approval id is already consumed" in (
        approval_consumed["application_blockers"]
    )
    assert approval_consumed["authorization_consumed"] is False
    assert approval_consumed["dispatch_allowed"] is False

    one_shot_consumed = (
        application_gate.evaluate_first_message_application_gate(
            approval_record,
            authorization_candidate=candidate,
            observed_at_utc="2026-09-01T09:05:00Z",
            consumed_one_shot_keys=[candidate["one_shot_key"]],
        )
    )
    assert one_shot_consumed["status"] == application_gate.BLOCKED
    assert one_shot_consumed["one_shot_unconsumed"] is False
    assert "first-message one-shot key is already consumed" in (
        one_shot_consumed["application_blockers"]
    )
    assert one_shot_consumed["authorization_consumed"] is False
    assert one_shot_consumed["dispatch_allowed"] is False

    both_consumed = application_gate.evaluate_first_message_application_gate(
        approval_record,
        authorization_candidate=candidate,
        observed_at_utc="2026-09-01T09:05:00Z",
        consumed_owner_approval_ids=[approval_record["owner_approval_id"]],
        consumed_one_shot_keys=[candidate["one_shot_key"]],
    )
    assert both_consumed["status"] == application_gate.BLOCKED
    assert len(both_consumed["application_blockers"]) == 2
    assert both_consumed["application_allowed"] is False
    assert both_consumed["telegram_api_calls"] == 0

    _expect_error(
        "consumed_one_shot_keys item",
        lambda: application_gate.evaluate_first_message_application_gate(
            approval_record,
            authorization_candidate=candidate,
            observed_at_utc="2026-09-01T09:05:00Z",
            consumed_one_shot_keys=["not-an-id"],
        ),
    )
    _expect_error(
        "must use YYYY-MM-DDTHH:MM:SSZ",
        lambda: application_gate.evaluate_first_message_application_gate(
            approval_record,
            authorization_candidate=candidate,
            observed_at_utc="2026-09-01 09:05:00",
        ),
    )

    tampered_approval = deepcopy(approval_record)
    tampered_approval["expires_at_utc"] = "2026-09-01T09:09:00Z"
    _expect_error(
        "owner approval fingerprint mismatch",
        lambda: application_gate.evaluate_first_message_application_gate(
            tampered_approval,
            authorization_candidate=candidate,
            observed_at_utc="2026-09-01T09:05:00Z",
        ),
    )
    tampered_gate = deepcopy(ready)
    tampered_gate["application_allowed"] = True
    _expect_error(
        "safety invariant failed: application_allowed",
        lambda: application_gate.verify_first_message_application_gate_status(
            tampered_gate
        ),
    )
    tampered_time_state = deepcopy(ready)
    tampered_time_state["approval_current"] = False
    payload = {
        key: value
        for key, value in tampered_time_state.items()
        if key not in {"application_gate_id", *set(application_gate._SAFETY)}
    }
    tampered_time_state["application_gate_id"] = application_gate._hash(payload)
    _expect_error(
        "time state is inconsistent",
        lambda: application_gate.verify_first_message_application_gate_status(
            tampered_time_state
        ),
    )
    assert bot.calls == []

    source = (
        ROOT / "research_experimental_preview_first_message_application_gate.py"
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
        "research_experimental_preview_first_message_authorization.py",
        "research_experimental_preview_first_message_owner_approval.py",
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
            "research_experimental_preview_first_message_application_gate"
            not in disconnected_source
        )

    print("research_experimental_preview_first_message_application_gate_selftest: ok")


if __name__ == "__main__":
    run()
