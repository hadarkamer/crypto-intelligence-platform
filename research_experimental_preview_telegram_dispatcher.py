"""Fail-closed async dispatcher preparation for PREVIEW_ONLY requests.

The dispatcher exercises the real ``Bot.send_message`` interface only against
an explicitly classified test double.  A runtime Bot may now be classified as
connector-registered, but runtime dispatch remains structurally forbidden and
therefore cannot reach the method call.  The module is not imported by
production, a worker or a scheduler.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from typing import Any, Dict, Iterable, Mapping

import research_experimental_preview_telegram_adapter as telegram_adapter


DISPATCHER_VERSION = "preview-telegram-dispatcher-v2-runtime-no-dispatch"
MODE = "PREVIEW_TELEGRAM_DISPATCHER_RUNTIME_DISPATCH_FORBIDDEN"
OWNER = "Hadar Kamar"
LIFECYCLE_STATUS = "RUNTIME_CONNECTOR_SUPPORTED_DISPATCH_FORBIDDEN"

TEST_DOUBLE = "TEST_DOUBLE"
RUNTIME_BOT_UNREGISTERED = "RUNTIME_BOT_UNREGISTERED"
RUNTIME_BOT_REGISTERED_NO_DISPATCH = "RUNTIME_BOT_REGISTERED_NO_DISPATCH"

FAKE_RECORDED = "FAKE_DISPATCH_RECORDED"
SUPPRESSED = "DISPATCH_SUPPRESSED"
DUPLICATE = "DISPATCH_SKIPPED_DUPLICATE"
SINGLE_FLIGHT_BUSY = "DISPATCH_SKIPPED_SINGLE_FLIGHT"
FAILED = "FAKE_DISPATCH_FAILED"

TEST_IDS = (
    "preview-dispatch-default-deny",
    "preview-dispatch-fake-success",
    "preview-dispatch-single-flight",
    "preview-dispatch-cancellation-release",
    "preview-dispatch-restart-idempotency",
)

_HEX_40 = re.compile(r"^[0-9a-f]{40}$")
_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_POLICY_KEYS = {
    "enabled",
    "kill_switch_engaged",
    "owner_dispatch_approved",
    "test_chat_id",
    "runtime_commit",
    "activation_approval_id",
}


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


def _optional_hex(value: Any, *, pattern: re.Pattern[str], name: str) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if not pattern.fullmatch(normalized):
        raise ValueError(f"{name} has an invalid fingerprint")
    return normalized


def _chat_id(value: Any) -> int | None:
    if value is None:
        return None
    if type(value) is not int or value == 0:
        raise ValueError("dispatcher test_chat_id must be a non-zero integer")
    return value


def _policy(value: Mapping[str, Any] | None) -> Dict[str, Any]:
    supplied = {} if value is None else value
    if not isinstance(supplied, Mapping):
        raise ValueError("preview dispatcher policy must be an object")
    unknown = sorted(set(supplied).difference(_POLICY_KEYS))
    if unknown:
        raise ValueError(
            "preview dispatcher policy contains unknown fields: "
            + ", ".join(unknown)
        )
    enabled = supplied.get("enabled", False)
    kill_switch = supplied.get("kill_switch_engaged", True)
    owner_approved = supplied.get("owner_dispatch_approved", False)
    for name, item in (
        ("enabled", enabled),
        ("kill_switch_engaged", kill_switch),
        ("owner_dispatch_approved", owner_approved),
    ):
        if type(item) is not bool:
            raise ValueError(f"preview dispatcher policy {name} must be boolean")
    return {
        "enabled": enabled,
        "kill_switch_engaged": kill_switch,
        "owner_dispatch_approved": owner_approved,
        "test_chat_id": _chat_id(supplied.get("test_chat_id")),
        "runtime_commit": _optional_hex(
            supplied.get("runtime_commit"),
            pattern=_HEX_40,
            name="runtime_commit",
        ),
        "activation_approval_id": _optional_hex(
            supplied.get("activation_approval_id"),
            pattern=_HEX_64,
            name="activation_approval_id",
        ),
    }


def _request_keys(values: Iterable[str]) -> set[str]:
    if isinstance(values, (str, bytes, bytearray, Mapping)):
        raise ValueError("existing_request_keys must be an iterable of ids")
    normalized = set()
    for value in values:
        key = str(value or "").strip().lower()
        if not _HEX_64.fullmatch(key):
            raise ValueError("existing request key has an invalid fingerprint")
        normalized.add(key)
    return normalized


class PreviewTelegramDispatcher:
    """Single-flight dispatcher whose active path is limited to a test double."""

    def __init__(
        self,
        bot: Any,
        *,
        client_classification: str = RUNTIME_BOT_UNREGISTERED,
        connector_registered: bool = False,
    ) -> None:
        if bot is None or not callable(getattr(bot, "send_message", None)):
            raise ValueError("dispatcher bot must expose an async send_message method")
        if client_classification not in {
            TEST_DOUBLE,
            RUNTIME_BOT_UNREGISTERED,
            RUNTIME_BOT_REGISTERED_NO_DISPATCH,
        }:
            raise ValueError("dispatcher client classification is invalid")
        if type(connector_registered) is not bool:
            raise ValueError("connector_registered must be boolean")
        if (
            client_classification == RUNTIME_BOT_UNREGISTERED
            and connector_registered
        ):
            raise ValueError("runtime connector registration is forbidden")
        if (
            client_classification == RUNTIME_BOT_REGISTERED_NO_DISPATCH
            and not connector_registered
        ):
            raise ValueError("registered runtime connector flag is required")
        self._bot = bot
        self._client_classification = client_classification
        self._connector_registered = connector_registered
        self._state_lock = asyncio.Lock()
        self._inflight_keys: set[str] = set()
        self._completed_keys: set[str] = set()

    @property
    def inflight_count(self) -> int:
        return len(self._inflight_keys)

    async def _reserve(self, key: str, existing: set[str]) -> str:
        async with self._state_lock:
            if key in existing or key in self._completed_keys:
                return DUPLICATE
            if key in self._inflight_keys:
                return SINGLE_FLIGHT_BUSY
            self._inflight_keys.add(key)
            return "RESERVED"

    async def _release(self, key: str, *, completed: bool) -> None:
        async with self._state_lock:
            self._inflight_keys.discard(key)
            if completed:
                self._completed_keys.add(key)

    def _global_blockers(
        self,
        *,
        policy: Mapping[str, Any],
        request: Mapping[str, Any],
    ) -> list[str]:
        blockers = []
        if not self._connector_registered:
            blockers.append("preview dispatcher connector is not registered")
        if self._client_classification != TEST_DOUBLE:
            blockers.append("runtime Bot dispatch is forbidden in this stage")
        if not policy["enabled"]:
            blockers.append("preview dispatcher feature flag is disabled")
        if policy["kill_switch_engaged"]:
            blockers.append("preview dispatcher kill switch is engaged")
        if not policy["owner_dispatch_approved"]:
            blockers.append("owner approval for preview dispatch is absent")
        if policy["test_chat_id"] is None:
            blockers.append("dispatcher private test chat is not configured")
        elif policy["test_chat_id"] != request["kwargs"]["chat_id"]:
            blockers.append("dispatcher destination differs from prepared request")
        if policy["runtime_commit"] is None:
            blockers.append("dispatcher runtime commit is not bound")
        if policy["activation_approval_id"] is None:
            blockers.append("dispatcher activation approval is not bound")
        if request.get("status") != telegram_adapter.REQUEST_STATUS:
            blockers.append("prepared adapter request status is incompatible")
        if request.get("research_evidence_effect") != "NONE":
            blockers.append("preview dispatch may not affect research evidence")
        return list(dict.fromkeys(blockers))

    async def dispatch(
        self,
        plan: Mapping[str, Any],
        *,
        transport_policy: Mapping[str, Any] | None = None,
        dispatcher_policy: Mapping[str, Any] | None = None,
        existing_transport_keys: Iterable[str] = (),
        existing_request_keys: Iterable[str] = (),
    ) -> Dict[str, Any]:
        """Exercise dispatch only for a registered test double."""

        adapter = telegram_adapter.prepare_unregistered_telegram_requests(
            plan,
            transport_policy=transport_policy,
            existing_transport_keys=existing_transport_keys,
        )
        policy = _policy(dispatcher_policy)
        existing = _request_keys(existing_request_keys)
        decisions = []
        fake_calls = 0
        for request in adapter["requests"]:
            blockers = self._global_blockers(policy=policy, request=request)
            status = SUPPRESSED
            error_type = None
            if not blockers:
                reservation = await self._reserve(request["request_key"], existing)
                if reservation != "RESERVED":
                    status = reservation
                else:
                    completed = False
                    try:
                        await self._bot.send_message(**request["kwargs"])
                        completed = True
                        fake_calls += 1
                        status = FAKE_RECORDED
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        status = FAILED
                        error_type = type(exc).__name__
                    finally:
                        await self._release(
                            request["request_key"], completed=completed
                        )
            decision_payload = {
                "dispatcher_version": DISPATCHER_VERSION,
                "adapter_request_id": request["adapter_request_id"],
                "request_key": request["request_key"],
                "status": status,
                "blockers": blockers,
                "error_type": error_type,
                "research_evidence_effect": "NONE",
            }
            decisions.append(
                {
                    **decision_payload,
                    "dispatch_decision_id": _hash(decision_payload),
                }
            )

        batch_payload = {
            "dispatcher_version": DISPATCHER_VERSION,
            "adapter_batch_id": adapter["adapter_batch_id"],
            "client_classification": self._client_classification,
            "connector_registered": self._connector_registered,
            "policy": policy,
            "dispatch_decision_ids": [
                decision["dispatch_decision_id"] for decision in decisions
            ],
        }
        if self._client_classification == TEST_DOUBLE:
            runtime_evidence = "FAKE_BOT_ASYNC_ONLY"
        elif self._client_classification == RUNTIME_BOT_REGISTERED_NO_DISPATCH:
            runtime_evidence = "RUNTIME_CONNECTOR_REGISTERED_NO_DISPATCH"
        else:
            runtime_evidence = "NONE"
        checklist_metadata = {
            "owner": OWNER,
            "lifecycle_status": LIFECYCLE_STATUS,
            "commit": policy["runtime_commit"],
            "test_ids": list(TEST_IDS),
            "runtime_evidence": runtime_evidence,
            "activation_approval": {
                "id": policy["activation_approval_id"],
                "scope": "PRIVATE_TEST_CHAT_PREVIEW_ONLY",
                "production_authorized": False,
            },
        }
        return {
            "dispatcher_version": DISPATCHER_VERSION,
            "mode": MODE,
            "dispatch_batch_id": _hash(batch_payload),
            "adapter_batch_id": adapter["adapter_batch_id"],
            "owner": OWNER,
            "lifecycle_status": LIFECYCLE_STATUS,
            "runtime_commit": policy["runtime_commit"],
            "test_ids": list(TEST_IDS),
            "runtime_evidence": runtime_evidence,
            "activation_approval_id": policy["activation_approval_id"],
            "activation_scope": "PRIVATE_TEST_CHAT_PREVIEW_ONLY",
            "checklist_metadata": checklist_metadata,
            "client_classification": self._client_classification,
            "connector_registered": self._connector_registered,
            "scheduler_registered": False,
            "worker_registered": False,
            "production_imported": False,
            "requests_considered": len(decisions),
            "fake_bot_calls": fake_calls,
            "fake_messages_recorded": sum(
                decision["status"] == FAKE_RECORDED for decision in decisions
            ),
            "suppressed": sum(
                decision["status"] == SUPPRESSED for decision in decisions
            ),
            "duplicates_skipped": sum(
                decision["status"] == DUPLICATE for decision in decisions
            ),
            "single_flight_skipped": sum(
                decision["status"] == SINGLE_FLIGHT_BUSY
                for decision in decisions
            ),
            "failed": sum(
                decision["status"] == FAILED for decision in decisions
            ),
            "public_opt_in": False,
            "stage6_activated": False,
            "delivery_attempts": 0,
            "telegram_api_calls": 0,
            "database_writes": 0,
            "research_evidence_writes": 0,
            "research_evidence_effect": "NONE",
            "delivery_channel": "NONE",
            "live_effect": "NONE",
            "decisions": decisions,
        }
