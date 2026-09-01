"""Regressions for separate, unapplied first-message owner approval."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import research_experimental_preview_first_message_authorization as authorization
import research_experimental_preview_first_message_authorization_selftest as authorization_fixtures
import research_experimental_preview_first_message_owner_approval as approval


ROOT = Path(__file__).resolve().parent


def _candidate():
    registration, gate, bot = authorization_fixtures._ready_registration()
    prepared = authorization.prepare_first_message_authorization_candidate(
        authorization_fixtures._plan(),
        registration_status=registration,
        activation_gate_status=gate,
        transport_policy=authorization_fixtures._transport_policy(),
    )
    assert prepared["authorization_candidate_prepared"] is True
    return prepared["authorization_candidate"], bot


def _approval_input(**overrides) -> dict:
    supplied = {
        "owner_approved": True,
        "approver_name": approval.OWNER,
        "approver_role": approval.OWNER_ROLE,
        "approved_at_utc": "2026-09-01T09:00:00Z",
        "expires_at_utc": "2026-09-01T09:10:00Z",
        "approval_statement": (
            "Synthetic self-test approval for exactly one PREVIEW message."
        ),
        "single_use_required": True,
        "telegram_dispatch_authorized": True,
        "first_preview_message_authorized": True,
        "production_authorized": False,
        "public_opt_in_authorized": False,
        "stage6_authorized": False,
        "research_evidence_authorized": False,
        "live_authorized": False,
    }
    supplied.update(overrides)
    return supplied


def _expect_error(fragment: str, callback) -> None:
    try:
        callback()
    except ValueError as exc:
        assert fragment in str(exc), exc
    else:
        raise AssertionError(f"expected ValueError containing {fragment!r}")


def run() -> None:
    candidate, bot = _candidate()

    absent = approval.prepare_first_message_owner_approval(candidate)
    assert absent["status"] == "NOT_APPROVED"
    assert absent["owner_approval_record_prepared"] is False
    assert absent["owner_approval_id"] is None
    assert absent["owner_approval_record"] is None
    assert "explicit first-message owner approval is absent" in absent[
        "approval_blockers"
    ]
    assert absent["owner_approval_verified"] is False
    assert absent["approval_applied"] is False
    assert absent["dispatch_allowed"] is False
    assert absent["delivery_allowed"] is False
    assert absent["delivery_attempts"] == 0
    assert absent["telegram_api_calls"] == 0
    assert bot.calls == []

    not_granted = approval.prepare_first_message_owner_approval(
        candidate,
        approval_input=_approval_input(owner_approved=False),
    )
    assert not_granted["owner_approval_record_prepared"] is False
    assert "first-message owner approval is not granted" in not_granted[
        "approval_blockers"
    ]
    assert not_granted["dispatch_allowed"] is False
    assert bot.calls == []

    prepared = approval.prepare_first_message_owner_approval(
        candidate,
        approval_input=_approval_input(),
    )
    assert prepared["mode"] == approval.MODE
    assert prepared["status"] == approval.STATUS
    assert prepared["owner_approval_record_prepared"] is True
    assert prepared["approval_blockers"] == []
    assert len(prepared["owner_approval_id"]) == 64
    assert prepared["owner_approval_id"] != candidate[
        "authorization_candidate_id"
    ]
    assert prepared["owner_approval_verified"] is True
    assert prepared["approval_applied"] is False
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
    assert bot.calls == []

    record = prepared["owner_approval_record"]
    assert record["status"] == approval.STATUS
    assert record["approver"] == {
        "name": approval.OWNER,
        "role": approval.OWNER_ROLE,
    }
    assert record["candidate_binding"]["authorization_candidate_id"] == (
        candidate["authorization_candidate_id"]
    )
    assert record["candidate_binding"]["one_shot_key"] == candidate[
        "one_shot_key"
    ]
    assert record["candidate_binding"]["chunk_count"] == 1
    assert record["authorization"]["single_use_required"] is True
    assert record["authorization"]["telegram_dispatch_authorized"] is True
    assert record["authorization"]["first_preview_message_authorized"] is True
    assert record["authorization"]["production_authorized"] is False
    assert record["authorization"]["public_opt_in_authorized"] is False
    assert record["authorization"]["stage6_authorized"] is False
    assert record["authorization"]["research_evidence_authorized"] is False
    assert record["authorization"]["live_authorized"] is False
    assert record["application_state"]["applied_to_runtime"] is False
    assert record["application_state"]["authorization_consumed"] is False
    assert record["application_state"]["dispatch_allowed"] is False
    assert record["application_state"]["delivery_allowed"] is False
    serialized = json.dumps(record, ensure_ascii=False, sort_keys=True)
    assert '"chat_id"' not in serialized
    assert '"text"' not in serialized

    verified = approval.verify_first_message_owner_approval(
        record,
        authorization_candidate=candidate,
    )
    assert verified["owner_approval_id"] == prepared["owner_approval_id"]
    assert verified["authorization_candidate_id"] == candidate[
        "authorization_candidate_id"
    ]
    assert verified["approval_window_seconds"] == 600
    assert verified["owner_approval_verified"] is True
    assert verified["approval_applied"] is False
    assert verified["dispatch_allowed"] is False
    assert verified["telegram_api_calls"] == 0

    repeated = approval.prepare_first_message_owner_approval(
        deepcopy(candidate),
        approval_input=deepcopy(_approval_input()),
    )
    assert repeated == prepared
    assert bot.calls == []

    missing_action = approval.prepare_first_message_owner_approval(
        candidate,
        approval_input=_approval_input(first_preview_message_authorized=False),
    )
    assert missing_action["owner_approval_record_prepared"] is False
    assert "first-message owner approval requires first_preview_message_authorized" in (
        missing_action["approval_blockers"]
    )
    assert missing_action["dispatch_allowed"] is False

    _expect_error(
        "window exceeds 15 minutes",
        lambda: approval.prepare_first_message_owner_approval(
            candidate,
            approval_input=_approval_input(
                expires_at_utc="2026-09-01T09:15:01Z"
            ),
        ),
    )
    _expect_error(
        "must expire after approval",
        lambda: approval.prepare_first_message_owner_approval(
            candidate,
            approval_input=_approval_input(
                expires_at_utc="2026-09-01T08:59:59Z"
            ),
        ),
    )
    _expect_error(
        "name is incompatible",
        lambda: approval.prepare_first_message_owner_approval(
            candidate,
            approval_input=_approval_input(approver_name="Another Owner"),
        ),
    )
    _expect_error(
        "may not authorize production_authorized",
        lambda: approval.prepare_first_message_owner_approval(
            candidate,
            approval_input=_approval_input(production_authorized=True),
        ),
    )
    _expect_error(
        "unknown fields",
        lambda: approval.prepare_first_message_owner_approval(
            candidate,
            approval_input={**_approval_input(), "dispatch_now": True},
        ),
    )

    tampered_fingerprint = deepcopy(record)
    tampered_fingerprint["approval_statement"] += " changed"
    _expect_error(
        "approval fingerprint mismatch",
        lambda: approval.verify_first_message_owner_approval(
            tampered_fingerprint,
            authorization_candidate=candidate,
        ),
    )
    tampered_binding = deepcopy(record)
    tampered_binding.pop("owner_approval_id")
    tampered_binding["candidate_binding"]["message_sha256"] = "0" * 64
    tampered_binding["owner_approval_id"] = approval._hash(tampered_binding)
    _expect_error(
        "candidate binding mismatch",
        lambda: approval.verify_first_message_owner_approval(
            tampered_binding,
            authorization_candidate=candidate,
        ),
    )
    tampered_application = deepcopy(record)
    tampered_application.pop("owner_approval_id")
    tampered_application["application_state"]["dispatch_allowed"] = True
    tampered_application["owner_approval_id"] = approval._hash(
        tampered_application
    )
    _expect_error(
        "applied state is forbidden",
        lambda: approval.verify_first_message_owner_approval(
            tampered_application,
            authorization_candidate=candidate,
        ),
    )
    assert bot.calls == []

    source = (
        ROOT / "research_experimental_preview_first_message_owner_approval.py"
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
            "research_experimental_preview_first_message_owner_approval"
            not in disconnected_source
        )

    print("research_experimental_preview_first_message_owner_approval_selftest: ok")


if __name__ == "__main__":
    run()
