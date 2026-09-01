"""Regressions for fail-closed PREVIEW_ONLY staging configuration."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import research_experimental_preview_staging_config as config


ROOT = Path(__file__).resolve().parent
COMMIT = "9" * 40
APPROVAL = "a" * 64


def _complete_environment(**overrides) -> dict:
    environment = {
        config.ENABLED_ENV: "0",
        config.KILL_SWITCH_ENV: "1",
        config.OWNER_APPROVED_ENV: "1",
        config.TEST_CHAT_ID_ENV: "-1001",
        config.RUNTIME_COMMIT_ENV: COMMIT,
        config.ACTIVATION_APPROVAL_ENV: APPROVAL,
    }
    environment.update(overrides)
    return environment


def _raises(expected: str, function) -> None:
    try:
        function()
    except ValueError as exc:
        assert expected in str(exc), exc
    else:
        raise AssertionError(f"expected ValueError containing {expected!r}")


def run() -> None:
    defaults = config.resolve_staging_configuration({})
    assert defaults["requested_enabled"] is False
    assert defaults["effective_enabled"] is False
    assert defaults["kill_switch_engaged"] is True
    assert defaults["owner_staging_approved"] is False
    assert defaults["test_chat_id"] is None
    assert defaults["prerequisites_complete"] is False
    assert defaults["connector_registration_allowed"] is False
    assert defaults["delivery_allowed"] is False

    complete_off = config.resolve_staging_configuration(
        _complete_environment()
    )
    assert complete_off["prerequisites_complete"] is True
    assert complete_off["requested_enabled"] is False
    assert complete_off["effective_enabled"] is False
    assert complete_off["test_chat_id"] == -1001
    assert "staging PREVIEW flag is disabled" in (
        complete_off["activation_blockers"]
    )

    attempted_activation = config.resolve_staging_configuration(
        _complete_environment(
            **{
                config.ENABLED_ENV: "1",
                config.KILL_SWITCH_ENV: "0",
            }
        )
    )
    assert attempted_activation["requested_enabled"] is True
    assert attempted_activation["kill_switch_engaged"] is False
    assert attempted_activation["effective_enabled"] is False
    assert attempted_activation["connector_registration_allowed"] is False
    assert attempted_activation["delivery_allowed"] is False
    assert attempted_activation["activation_blockers"] == [
        "current configuration lifecycle forbids activation"
    ]

    status = config.sanitized_status(complete_off)
    assert status["test_chat_configured"] is True
    assert len(status["test_chat_binding_sha256"]) == 64
    assert status["runtime_commit_configured"] is True
    assert status["activation_approval_configured"] is True
    serialized = repr(status)
    assert "-1001" not in serialized
    assert APPROVAL not in serialized

    tampered = deepcopy(complete_off)
    tampered["test_chat_id"] = -2002
    _raises(
        "fingerprint mismatch",
        lambda: config.sanitized_status(tampered),
    )
    unsafe = deepcopy(complete_off)
    unsafe["delivery_allowed"] = True
    _raises(
        "delivery_allowed",
        lambda: config.sanitized_status(unsafe),
    )

    _raises(
        "explicit boolean",
        lambda: config.resolve_staging_configuration(
            {config.ENABLED_ENV: "perhaps"}
        ),
    )
    _raises(
        "must be an integer",
        lambda: config.resolve_staging_configuration(
            {config.TEST_CHAT_ID_ENV: "private-chat"}
        ),
    )
    _raises(
        "invalid fingerprint",
        lambda: config.resolve_staging_configuration(
            {config.RUNTIME_COMMIT_ENV: "not-a-commit"}
        ),
    )

    source = (
        ROOT / "research_experimental_preview_staging_config.py"
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
    ):
        assert forbidden not in source

    print("research_experimental_preview_staging_config_selftest: ok")


if __name__ == "__main__":
    run()
