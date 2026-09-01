"""Regressions for the non-authoritative staging activation record."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import research_experimental_preview_activation_record as activation_record
import research_experimental_preview_staging_config as staging_config


ROOT = Path(__file__).resolve().parent
CHAT_ID = -1001
RUNTIME_COMMIT = "9" * 40


def _configuration(**overrides) -> dict:
    environment = {
        staging_config.ENABLED_ENV: "0",
        staging_config.KILL_SWITCH_ENV: "1",
        staging_config.OWNER_APPROVED_ENV: "0",
        staging_config.TEST_CHAT_ID_ENV: str(CHAT_ID),
        staging_config.RUNTIME_COMMIT_ENV: RUNTIME_COMMIT,
    }
    environment.update(overrides)
    return staging_config.resolve_staging_configuration(environment)


def _raises(expected: str, function) -> None:
    try:
        function()
    except ValueError as exc:
        assert expected in str(exc), exc
    else:
        raise AssertionError(f"expected ValueError containing {expected!r}")


def run() -> None:
    configuration = _configuration()
    candidate = activation_record.prepare_activation_candidate(configuration)
    assert candidate["status"] == activation_record.STATUS
    assert candidate["target"]["service_name"] == (
        activation_record.STAGING_SERVICE_NAME
    )
    assert candidate["target"]["service_id"] == activation_record.STAGING_SERVICE_ID
    assert candidate["target"]["runtime_commit"] == RUNTIME_COMMIT
    assert candidate["private_route"]["scope"] == activation_record.SCOPE
    assert candidate["private_route"]["route"] == activation_record.ROUTE
    assert candidate["private_route"]["test_chat_count"] == 1
    assert len(candidate["private_route"]["test_chat_binding_sha256"]) == 64
    assert candidate["approval_boundary"]["owner_approved"] is False
    assert candidate["approval_boundary"]["activation_approval_id"] is None
    assert (
        candidate["approval_boundary"][
            "candidate_id_may_be_used_as_approval_id"
        ]
        is False
    )
    assert len(candidate["activation_candidate_id"]) == 64
    assert candidate["approval_granted"] is False
    assert candidate["connector_registration_allowed"] is False
    assert candidate["delivery_allowed"] is False
    assert candidate["public_opt_in"] is False
    assert candidate["stage6_activated"] is False
    assert candidate["delivery_attempts"] == 0
    assert candidate["telegram_api_calls"] == 0
    assert candidate["database_writes"] == 0
    assert candidate["research_evidence_writes"] == 0
    assert candidate["research_evidence_effect"] == "NONE"
    assert candidate["delivery_channel"] == "NONE"
    assert candidate["live_effect"] == "NONE"
    assert str(CHAT_ID) not in repr(candidate)

    repeated = activation_record.prepare_activation_candidate(
        deepcopy(configuration)
    )
    assert repeated == candidate

    status = activation_record.verify_activation_candidate(
        candidate,
        configuration=configuration,
    )
    assert status["activation_candidate_id"] == candidate[
        "activation_candidate_id"
    ]
    assert status["approval_granted"] is False
    assert status["test_chat_count"] == 1
    assert str(CHAT_ID) not in repr(status)

    tampered = deepcopy(candidate)
    tampered["target"]["runtime_commit"] = "8" * 40
    _raises(
        "fingerprint mismatch",
        lambda: activation_record.verify_activation_candidate(
            tampered,
            configuration=configuration,
        ),
    )
    _raises(
        "flag disabled",
        lambda: activation_record.prepare_activation_candidate(
            _configuration(**{staging_config.ENABLED_ENV: "1"})
        ),
    )
    _raises(
        "kill switch engaged",
        lambda: activation_record.prepare_activation_candidate(
            _configuration(**{staging_config.KILL_SWITCH_ENV: "0"})
        ),
    )
    _raises(
        "owner approval must remain absent",
        lambda: activation_record.prepare_activation_candidate(
            _configuration(**{staging_config.OWNER_APPROVED_ENV: "1"})
        ),
    )
    _raises(
        "approval must remain absent",
        lambda: activation_record.prepare_activation_candidate(
            _configuration(
                **{staging_config.ACTIVATION_APPROVAL_ENV: "a" * 64}
            )
        ),
    )
    _raises(
        "test chat must be configured",
        lambda: activation_record.prepare_activation_candidate(
            _configuration(**{staging_config.TEST_CHAT_ID_ENV: ""})
        ),
    )
    _raises(
        "runtime commit must be configured",
        lambda: activation_record.prepare_activation_candidate(
            _configuration(**{staging_config.RUNTIME_COMMIT_ENV: ""})
        ),
    )

    source = (
        ROOT / "research_experimental_preview_activation_record.py"
    ).read_text(encoding="utf-8").lower()
    for forbidden in (
        "os.getenv",
        "os.environ",
        "import telegram",
        "from telegram",
        "send_message(",
        "reply_text(",
        "execute(",
        "create_task(",
        "import psycopg",
        "import sqlite",
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
        assert "research_experimental_preview_activation_record" not in (
            production_source
        )

    print("research_experimental_preview_activation_record_selftest: ok")


if __name__ == "__main__":
    run()
