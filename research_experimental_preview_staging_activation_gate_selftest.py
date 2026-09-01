"""Fail-closed tests for the local-only staging activation gate."""

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path

import research_experimental_preview_staging_activation_gate as gate
import research_experimental_preview_staging_config as staging_config


ROOT = Path(__file__).resolve().parent
CHAT_ID = 1001
RUNTIME_COMMIT = "9" * 40
CANDIDATE_ID = "8" * 64
CANDIDATE_CONFIGURATION_ID = "7" * 64


def _hash(value) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _chat_binding(chat_id: int = CHAT_ID) -> str:
    return _hash({"test_chat_id": chat_id})


def _approval_record(*, authorized: bool, chat_id: int = CHAT_ID) -> dict:
    payload = {
        "activation_approval_record_version": gate.APPROVAL_RECORD_VERSION,
        "status": gate.APPROVAL_STATUS,
        "approved_at_utc": "2026-09-01T00:00:00Z",
        "approver": {"name": gate.OWNER, "role": gate.OWNER_ROLE},
        "approval_statement": "Synthetic self-test approval.",
        "candidate_binding": {
            "activation_candidate_id": CANDIDATE_ID,
            "activation_record_version": (
                "preview-staging-activation-record-v1-prepared"
            ),
            "candidate_status": "PREPARED_NOT_APPROVED",
            "configuration_id": CANDIDATE_CONFIGURATION_ID,
        },
        "target": {
            "branch": gate.STAGING_BRANCH,
            "entrypoint": gate.STAGING_ENTRYPOINT,
            "runtime_commit": RUNTIME_COMMIT,
            "service_id": gate.STAGING_SERVICE_ID,
            "service_name": gate.STAGING_SERVICE_NAME,
        },
        "private_route": {
            "public_opt_in": False,
            "route": gate.ROUTE,
            "scope": gate.SCOPE,
            "test_chat_binding_sha256": _chat_binding(chat_id),
            "test_chat_count": 1,
        },
        "authorization": {
            "scope": gate.SCOPE,
            "approval_prerequisite_granted": True,
            "render_configuration_application_authorized": authorized,
            "deployment_authorized": authorized,
            "connector_registration_authorized": authorized,
            "telegram_dispatch_authorized": authorized,
            "first_preview_message_authorized": authorized,
            "production_authorized": False,
            "public_opt_in_authorized": False,
            "stage6_authorized": False,
            "research_evidence_authorized": False,
            "live_authorized": False,
        },
        "application_state": {
            "applied_to_render": False,
            "effective_enabled": False,
            "kill_switch_release_authorized": authorized,
            "connector_registered": False,
            "handler_registered": False,
            "scheduler_registered": False,
            "worker_registered": False,
            "delivery_attempts": 0,
            "telegram_api_calls": 0,
            "database_writes": 0,
            "research_evidence_writes": 0,
            "research_evidence_effect": "NONE",
            "delivery_channel": "NONE",
            "live_effect": "NONE",
        },
    }
    return {**payload, "activation_approval_id": _hash(payload)}


def _configuration(
    approval_id: str,
    *,
    enabled: bool,
    kill_switch: bool,
) -> dict:
    return staging_config.resolve_staging_configuration(
        {
            staging_config.ENABLED_ENV: "1" if enabled else "0",
            staging_config.KILL_SWITCH_ENV: "1" if kill_switch else "0",
            staging_config.OWNER_APPROVED_ENV: "1",
            staging_config.TEST_CHAT_ID_ENV: str(CHAT_ID),
            staging_config.RUNTIME_COMMIT_ENV: RUNTIME_COMMIT,
            staging_config.ACTIVATION_APPROVAL_ENV: approval_id,
        }
    )


def _expect_error(fragment: str, callback) -> None:
    try:
        callback()
    except ValueError as exc:
        assert fragment in str(exc)
    else:
        raise AssertionError(f"expected ValueError containing {fragment!r}")


def run() -> None:
    existing_approval = _approval_record(authorized=False)
    safe_configuration = _configuration(
        existing_approval["activation_approval_id"],
        enabled=False,
        kill_switch=True,
    )
    blocked = gate.evaluate_activation_gate(
        safe_configuration,
        actual_runtime_commit=RUNTIME_COMMIT,
        approval_record=existing_approval,
    )
    assert blocked["mode"] == gate.MODE
    assert blocked["lifecycle_status"] == gate.LIFECYCLE_STATUS
    assert blocked["activation_prerequisites_satisfied"] is False
    assert "staging PREVIEW flag is disabled" in blocked["activation_blockers"]
    assert "staging PREVIEW kill switch is engaged" in (
        blocked["activation_blockers"]
    )
    assert any(
        item.startswith("approval does not authorize ")
        for item in blocked["activation_blockers"]
    )
    assert blocked["registration_required"] is True
    assert blocked["activation_allowed"] is False
    assert blocked["connector_registration_allowed"] is False
    assert blocked["delivery_allowed"] is False
    assert blocked["telegram_api_calls"] == 0
    assert blocked["database_writes"] == 0
    assert blocked["research_evidence_effect"] == "NONE"
    assert blocked["live_effect"] == "NONE"

    future_approval = _approval_record(authorized=True)
    hypothetical_configuration = _configuration(
        future_approval["activation_approval_id"],
        enabled=True,
        kill_switch=False,
    )
    prepared = gate.evaluate_activation_gate(
        hypothetical_configuration,
        actual_runtime_commit=RUNTIME_COMMIT,
        approval_record=future_approval,
    )
    assert prepared["activation_prerequisites_satisfied"] is True
    assert prepared["activation_blockers"] == []
    assert prepared["registration_required"] is True
    assert prepared["activation_allowed"] is False
    assert prepared["connector_registration_allowed"] is False
    assert prepared["delivery_allowed"] is False
    assert prepared["handler_registered"] is False
    assert prepared["scheduler_registered"] is False
    assert prepared["worker_registered"] is False
    assert prepared["public_opt_in"] is False
    assert prepared["stage6_activated"] is False
    assert prepared["delivery_attempts"] == 0
    assert prepared["telegram_api_calls"] == 0
    assert prepared["research_evidence_writes"] == 0
    assert prepared["delivery_channel"] == "NONE"

    missing_observation = gate.observe_activation_gate(
        safe_configuration,
        actual_runtime_commit=None,
        approval_record_json=None,
    )
    assert missing_observation["approval_record_configured"] is False
    assert missing_observation["approval_record_valid"] is False
    assert missing_observation["runtime_commit_metadata_configured"] is False
    assert "activation approval record is not configured" in (
        missing_observation["activation_blockers"]
    )
    assert "runtime commit metadata is unavailable" in (
        missing_observation["activation_blockers"]
    )
    assert missing_observation["activation_allowed"] is False

    blocked_observation = gate.observe_activation_gate(
        safe_configuration,
        actual_runtime_commit=RUNTIME_COMMIT,
        approval_record_json=json.dumps(existing_approval),
    )
    assert blocked_observation["approval_record_configured"] is True
    assert blocked_observation["approval_record_valid"] is True
    assert blocked_observation["runtime_commit_metadata_configured"] is True
    assert blocked_observation["runtime_commit_matches_configuration"] is True
    assert blocked_observation["approval_id_matches_configuration"] is True
    assert blocked_observation["approval_runtime_commit_matches"] is True
    assert blocked_observation["approval_test_chat_matches"] is True
    assert blocked_observation["action_authorizations_complete"] is False
    assert blocked_observation["activation_prerequisites_satisfied"] is False
    assert blocked_observation["activation_allowed"] is False
    serialized_observation = json.dumps(blocked_observation)
    assert existing_approval["activation_approval_id"] not in (
        serialized_observation
    )
    assert str(CHAT_ID) not in serialized_observation

    ready_observation = gate.observe_activation_gate(
        hypothetical_configuration,
        actual_runtime_commit=RUNTIME_COMMIT,
        approval_record_json=future_approval,
    )
    assert ready_observation["approval_record_valid"] is True
    assert ready_observation["action_authorizations_complete"] is True
    assert ready_observation["activation_prerequisites_satisfied"] is True
    assert ready_observation["activation_blockers"] == []
    assert ready_observation["registration_required"] is True
    assert ready_observation["activation_allowed"] is False
    assert ready_observation["connector_registration_allowed"] is False
    assert ready_observation["delivery_allowed"] is False
    assert ready_observation["telegram_api_calls"] == 0
    assert gate.verify_observe_activation_gate_status(ready_observation) == (
        ready_observation
    )

    tampered_observation = deepcopy(ready_observation)
    tampered_observation["activation_allowed"] = True
    _expect_error(
        "safety invariant failed: activation_allowed",
        lambda: gate.verify_observe_activation_gate_status(
            tampered_observation
        ),
    )

    tampered_fingerprint = deepcopy(ready_observation)
    tampered_fingerprint["approval_test_chat_matches"] = False
    _expect_error(
        "fingerprint mismatch",
        lambda: gate.verify_observe_activation_gate_status(
            tampered_fingerprint
        ),
    )

    malformed_observation = gate.observe_activation_gate(
        safe_configuration,
        actual_runtime_commit="not-a-commit",
        approval_record_json="{not-json}",
    )
    assert malformed_observation["approval_record_configured"] is True
    assert malformed_observation["approval_record_valid"] is False
    assert "activation approval record is invalid" in (
        malformed_observation["activation_blockers"]
    )
    assert "runtime commit metadata is invalid" in (
        malformed_observation["activation_blockers"]
    )
    assert malformed_observation["activation_allowed"] is False

    wrong_commit = gate.evaluate_activation_gate(
        hypothetical_configuration,
        actual_runtime_commit="6" * 40,
        approval_record=future_approval,
    )
    assert wrong_commit["activation_prerequisites_satisfied"] is False
    assert "configured runtime commit differs from deployed commit" in (
        wrong_commit["activation_blockers"]
    )
    assert "approval runtime commit differs from deployed commit" in (
        wrong_commit["activation_blockers"]
    )

    wrong_chat = _approval_record(authorized=True, chat_id=2002)
    wrong_chat_configuration = _configuration(
        wrong_chat["activation_approval_id"],
        enabled=True,
        kill_switch=False,
    )
    chat_blocked = gate.evaluate_activation_gate(
        wrong_chat_configuration,
        actual_runtime_commit=RUNTIME_COMMIT,
        approval_record=wrong_chat,
    )
    assert "approval test chat differs from configured test chat" in (
        chat_blocked["activation_blockers"]
    )

    tampered = deepcopy(future_approval)
    tampered["target"]["runtime_commit"] = "5" * 40
    _expect_error(
        "fingerprint mismatch",
        lambda: gate.evaluate_activation_gate(
            hypothetical_configuration,
            actual_runtime_commit=RUNTIME_COMMIT,
            approval_record=tampered,
        ),
    )

    forbidden = _approval_record(authorized=True)
    forbidden["authorization"]["production_authorized"] = True
    payload = dict(forbidden)
    payload.pop("activation_approval_id")
    forbidden["activation_approval_id"] = _hash(payload)
    _expect_error(
        "may not authorize production_authorized",
        lambda: gate.evaluate_activation_gate(
            _configuration(
                forbidden["activation_approval_id"],
                enabled=True,
                kill_switch=False,
            ),
            actual_runtime_commit=RUNTIME_COMMIT,
            approval_record=forbidden,
        ),
    )

    source = (
        ROOT / "research_experimental_preview_staging_activation_gate.py"
    ).read_text(encoding="utf-8").lower()
    for forbidden_source in (
        "import telegram",
        "from telegram",
        "import psycopg",
        "os.getenv",
        "os.environ",
        "send_message(",
        "create_task(",
    ):
        assert forbidden_source not in source

    for production_file in (
        "main.py",
        "ai_telegram.py",
        "research_formula_worker.py",
        "research_formula_store.py",
        "research_formula_schema_admin.py",
    ):
        production_source = (ROOT / production_file).read_text(encoding="utf-8")
        assert "research_experimental_preview_staging_activation_gate" not in (
            production_source
        )

    candidate_source = (ROOT / "ai_candidate_main.py").read_text(
        encoding="utf-8"
    )
    assert (
        "import research_experimental_preview_staging_activation_gate as "
        "preview_gate" in candidate_source
    )
    assert "preview_gate.observe_activation_gate" in candidate_source
    assert '"preview_staging_activation_gate"' in candidate_source
    assert "preview_gate.dispatch" not in candidate_source
    assert "activation_gate_status.dispatch" not in candidate_source

    print("research_experimental_preview_staging_activation_gate_selftest: ok")


if __name__ == "__main__":
    run()
