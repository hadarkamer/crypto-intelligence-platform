"""No-dispatch PREVIEW_ONLY registration for the dedicated staging Bot.

The registration retains the staging Bot interface so lifecycle wiring can be
verified.  Only an exact, verified activation-gate candidate may transition to
``RUNTIME_BOT_REGISTERED_NO_DISPATCH``.  The dispatcher still rejects every
runtime delivery and this registration exposes no dispatch method.  It adds no
command, handler, scheduler, chat id, token lookup or Telegram API operation.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, Mapping

import research_experimental_preview_staging_activation_gate as activation_gate
import research_experimental_preview_telegram_dispatcher as dispatcher_module
import research_experimental_preview_staging_config as staging_config


REGISTRATION_VERSION = "preview-staging-registration-v4-verifiable-receipt"
MODE = "PREVIEW_STAGING_RUNTIME_CONNECTOR_REGISTERED_NO_DISPATCH"
BASE_COMMIT = "a6b32c8fb33d8a9507da19efcb53409c1999b3f5"
LIFECYCLE_STATUS = "RUNTIME_CONNECTOR_REGISTRATION_DISPATCH_UNAVAILABLE"

_HEX_64 = re.compile(r"^[0-9a-f]{64}$")


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


class DisabledStagingPreviewRegistration:
    """Idempotently bind one runtime Bot without granting dispatch authority."""

    def __init__(self) -> None:
        self._bot: Any | None = None
        self._dispatcher: dispatcher_module.PreviewTelegramDispatcher | None = None
        self._binding_id: str | None = None
        self._configuration = staging_config.resolve_staging_configuration({})
        self._activation_gate_status: Dict[str, Any] | None = None
        self._runtime_connector_candidate_id: str | None = None
        self._runtime_connector_registration_id: str | None = None
        self._runtime_connector_registered = False
        self._runtime_connector_registration_blockers = [
            "activation gate has not been observed"
        ]

    def bind_disabled_runtime_bot(
        self,
        bot: Any,
        *,
        configuration: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        if bot is None or not callable(getattr(bot, "send_message", None)):
            raise ValueError("staging Bot must expose an async send_message method")
        supplied = (
            staging_config.resolve_staging_configuration({})
            if configuration is None
            else dict(configuration)
        )
        safe_configuration = staging_config.sanitized_status(supplied)
        if safe_configuration["effective_enabled"] is not False:
            raise ValueError("staging registration requires effective disabled state")
        if safe_configuration["delivery_allowed"] is not False:
            raise ValueError("staging registration may not allow delivery")
        if self._bot is not None and self._bot is not bot:
            raise RuntimeError("a different staging Bot is already bound")
        if self._bot is not None and supplied != self._configuration:
            raise RuntimeError("staging configuration changed while Bot is bound")
        if self._bot is None:
            self._bot = bot
            self._configuration = supplied
            self._dispatcher = dispatcher_module.PreviewTelegramDispatcher(bot)
            self._binding_id = _hash(
                {
                    "registration_version": REGISTRATION_VERSION,
                    "mode": MODE,
                    "base_commit": BASE_COMMIT,
                    "client_classification": (
                        dispatcher_module.RUNTIME_BOT_UNREGISTERED
                    ),
                    "connector_registered": False,
                    "requested_enabled": safe_configuration[
                        "requested_enabled"
                    ],
                    "effective_enabled": False,
                    "kill_switch_engaged": safe_configuration[
                        "kill_switch_engaged"
                    ],
                    "configuration_id": supplied["configuration_id"],
                }
            )
        return self.status()

    def register_runtime_connector_no_dispatch(
        self,
        gate_status: Mapping[str, Any],
    ) -> Dict[str, Any]:
        """Register the runtime connector while keeping dispatch unavailable."""

        if self._bot is None or self._binding_id is None:
            raise RuntimeError("staging runtime Bot must be bound first")
        verified = activation_gate.verify_observe_activation_gate_status(
            gate_status
        )
        if self._activation_gate_status is None:
            raise RuntimeError("runtime connector candidate must be prepared first")
        if verified != self._activation_gate_status:
            raise RuntimeError("activation gate changed after candidate preparation")
        if self._runtime_connector_candidate_id is None:
            blockers = list(self._runtime_connector_registration_blockers)
            blockers.append("runtime connector candidate is not prepared")
            self._runtime_connector_registration_blockers = list(
                dict.fromkeys(blockers)
            )
            return self.status()
        if not self._runtime_connector_registered:
            self._dispatcher = dispatcher_module.PreviewTelegramDispatcher(
                self._bot,
                client_classification=(
                    dispatcher_module.RUNTIME_BOT_REGISTERED_NO_DISPATCH
                ),
                connector_registered=True,
            )
            registration_payload = {
                "registration_version": REGISTRATION_VERSION,
                "mode": MODE,
                "binding_id": self._binding_id,
                "runtime_connector_candidate_id": (
                    self._runtime_connector_candidate_id
                ),
                "activation_gate_id": verified["activation_gate_id"],
                "client_classification": (
                    dispatcher_module.RUNTIME_BOT_REGISTERED_NO_DISPATCH
                ),
                "connector_registered": True,
                "dispatch_exposed": False,
            }
            self._runtime_connector_registration_id = _hash(
                registration_payload
            )
            self._runtime_connector_registered = True
            self._runtime_connector_registration_blockers = []
        return self.status()

    def prepare_runtime_connector_candidate(
        self,
        gate_status: Mapping[str, Any],
    ) -> Dict[str, Any]:
        """Fingerprint one connector candidate without registering or dispatching."""

        if self._bot is None or self._binding_id is None:
            raise RuntimeError("staging runtime Bot must be bound first")
        verified = activation_gate.verify_observe_activation_gate_status(
            gate_status
        )
        if (
            self._activation_gate_status is not None
            and verified != self._activation_gate_status
        ):
            raise RuntimeError("activation gate changed while runtime Bot is bound")
        self._activation_gate_status = verified
        blockers = list(verified["activation_blockers"])
        if not verified["activation_prerequisites_satisfied"]:
            blockers.append("activation gate prerequisites are incomplete")
        blockers = list(dict.fromkeys(blockers))
        self._runtime_connector_registration_blockers = blockers
        if not blockers:
            candidate_payload = {
                "registration_version": REGISTRATION_VERSION,
                "mode": MODE,
                "binding_id": self._binding_id,
                "activation_gate_id": verified["activation_gate_id"],
                "client_classification": (
                    dispatcher_module.RUNTIME_BOT_UNREGISTERED
                ),
                "connector_registered": False,
                "dispatch_exposed": False,
            }
            self._runtime_connector_candidate_id = _hash(candidate_payload)
        else:
            self._runtime_connector_candidate_id = None
        return self.status()

    def unbind_runtime_bot(self, bot: Any) -> Dict[str, Any]:
        if self._bot is not None and self._bot is not bot:
            raise RuntimeError("cannot unbind a different staging Bot")
        self._dispatcher = None
        self._bot = None
        self._binding_id = None
        self._configuration = staging_config.resolve_staging_configuration({})
        self._activation_gate_status = None
        self._runtime_connector_candidate_id = None
        self._runtime_connector_registration_id = None
        self._runtime_connector_registered = False
        self._runtime_connector_registration_blockers = [
            "activation gate has not been observed"
        ]
        return self.status()

    def status(self) -> Dict[str, Any]:
        bound = self._bot is not None
        configuration_status = staging_config.sanitized_status(
            self._configuration
        )
        return {
            "registration_version": REGISTRATION_VERSION,
            "mode": MODE,
            "owner": dispatcher_module.OWNER,
            "lifecycle_status": LIFECYCLE_STATUS,
            "base_commit": BASE_COMMIT,
            "binding_id": self._binding_id,
            "runtime_bot_bound": bound,
            "client_classification": (
                dispatcher_module.RUNTIME_BOT_REGISTERED_NO_DISPATCH
                if self._runtime_connector_registered
                else (
                    dispatcher_module.RUNTIME_BOT_UNREGISTERED
                    if bound
                    else "NONE"
                )
            ),
            "requested_enabled": configuration_status["requested_enabled"],
            "enabled": False,
            "kill_switch_engaged": configuration_status[
                "kill_switch_engaged"
            ],
            "owner_staging_approved": configuration_status[
                "owner_staging_approved"
            ],
            "test_chat_configured": configuration_status[
                "test_chat_configured"
            ],
            "test_chat_binding_sha256": configuration_status[
                "test_chat_binding_sha256"
            ],
            "runtime_commit_configured": configuration_status[
                "runtime_commit_configured"
            ],
            "activation_approval_configured": configuration_status[
                "activation_approval_configured"
            ],
            "configuration_prerequisites_complete": configuration_status[
                "prerequisites_complete"
            ],
            "configuration_blockers": configuration_status[
                "activation_blockers"
            ],
            "activation_gate_observed": self._activation_gate_status is not None,
            "activation_gate_id": (
                self._activation_gate_status["activation_gate_id"]
                if self._activation_gate_status is not None
                else None
            ),
            "runtime_connector_candidate_prepared": (
                self._runtime_connector_candidate_id is not None
            ),
            "runtime_connector_candidate_id": (
                self._runtime_connector_candidate_id
            ),
            "runtime_connector_registration_id": (
                self._runtime_connector_registration_id
            ),
            "runtime_connector_registered_no_dispatch": (
                self._runtime_connector_registered
            ),
            "runtime_connector_registration_blockers": list(
                self._runtime_connector_registration_blockers
            ),
            "connector_registered": self._runtime_connector_registered,
            "activation_allowed": False,
            "dispatch_exposed": False,
            "handler_registered": False,
            "scheduler_registered": False,
            "worker_registered": False,
            "public_opt_in": False,
            "stage6_activated": False,
            "delivery_attempts": 0,
            "telegram_api_calls": 0,
            "database_writes": 0,
            "research_evidence_writes": 0,
            "research_evidence_effect": "NONE",
            "delivery_channel": "NONE",
            "live_effect": "NONE",
        }


def verify_registered_no_dispatch_status(
    status: Mapping[str, Any],
) -> Dict[str, Any]:
    """Verify a content-addressed registered-no-dispatch runtime receipt."""

    if not isinstance(status, Mapping):
        raise ValueError("staging registration status must be an object")
    supplied = dict(status)
    expected_constants = {
        "registration_version": REGISTRATION_VERSION,
        "mode": MODE,
        "owner": dispatcher_module.OWNER,
        "lifecycle_status": LIFECYCLE_STATUS,
        "base_commit": BASE_COMMIT,
        "client_classification": (
            dispatcher_module.RUNTIME_BOT_REGISTERED_NO_DISPATCH
        ),
    }
    for name, expected in expected_constants.items():
        if supplied.get(name) != expected:
            raise ValueError(f"staging registration receipt {name} is incompatible")

    expected_true = (
        "runtime_bot_bound",
        "requested_enabled",
        "owner_staging_approved",
        "test_chat_configured",
        "runtime_commit_configured",
        "activation_approval_configured",
        "configuration_prerequisites_complete",
        "activation_gate_observed",
        "runtime_connector_candidate_prepared",
        "runtime_connector_registered_no_dispatch",
        "connector_registered",
    )
    expected_false = (
        "enabled",
        "kill_switch_engaged",
        "activation_allowed",
        "dispatch_exposed",
        "handler_registered",
        "scheduler_registered",
        "worker_registered",
        "public_opt_in",
        "stage6_activated",
    )
    for name in expected_true:
        if supplied.get(name) is not True:
            raise ValueError(f"staging registration receipt requires {name}")
    for name in expected_false:
        if supplied.get(name) is not False:
            raise ValueError(f"staging registration receipt forbids {name}")
    for name in (
        "delivery_attempts",
        "telegram_api_calls",
        "database_writes",
        "research_evidence_writes",
    ):
        if supplied.get(name) != 0:
            raise ValueError(f"staging registration receipt requires zero {name}")
    for name, expected in {
        "research_evidence_effect": "NONE",
        "delivery_channel": "NONE",
        "live_effect": "NONE",
    }.items():
        if supplied.get(name) != expected:
            raise ValueError(f"staging registration receipt forbids {name}")
    if supplied.get("runtime_connector_registration_blockers") != []:
        raise ValueError("staging registration receipt contains blockers")

    identifiers = {}
    for name in (
        "binding_id",
        "activation_gate_id",
        "runtime_connector_candidate_id",
        "runtime_connector_registration_id",
        "test_chat_binding_sha256",
    ):
        value = str(supplied.get(name) or "").strip().lower()
        if not _HEX_64.fullmatch(value):
            raise ValueError(f"staging registration receipt {name} is invalid")
        identifiers[name] = value

    candidate_payload = {
        "registration_version": REGISTRATION_VERSION,
        "mode": MODE,
        "binding_id": identifiers["binding_id"],
        "activation_gate_id": identifiers["activation_gate_id"],
        "client_classification": dispatcher_module.RUNTIME_BOT_UNREGISTERED,
        "connector_registered": False,
        "dispatch_exposed": False,
    }
    if identifiers["runtime_connector_candidate_id"] != _hash(candidate_payload):
        raise ValueError("runtime connector candidate fingerprint mismatch")
    registration_payload = {
        "registration_version": REGISTRATION_VERSION,
        "mode": MODE,
        "binding_id": identifiers["binding_id"],
        "runtime_connector_candidate_id": identifiers[
            "runtime_connector_candidate_id"
        ],
        "activation_gate_id": identifiers["activation_gate_id"],
        "client_classification": (
            dispatcher_module.RUNTIME_BOT_REGISTERED_NO_DISPATCH
        ),
        "connector_registered": True,
        "dispatch_exposed": False,
    }
    if identifiers["runtime_connector_registration_id"] != _hash(
        registration_payload
    ):
        raise ValueError("runtime connector registration fingerprint mismatch")

    return {
        "registration_version": REGISTRATION_VERSION,
        "mode": MODE,
        "lifecycle_status": LIFECYCLE_STATUS,
        **identifiers,
        "client_classification": (
            dispatcher_module.RUNTIME_BOT_REGISTERED_NO_DISPATCH
        ),
        "connector_registered": True,
        "activation_allowed": False,
        "dispatch_exposed": False,
        "handler_registered": False,
        "scheduler_registered": False,
        "worker_registered": False,
        "delivery_attempts": 0,
        "telegram_api_calls": 0,
        "research_evidence_effect": "NONE",
        "live_effect": "NONE",
    }


REGISTRATION = DisabledStagingPreviewRegistration()
