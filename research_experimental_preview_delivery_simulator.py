"""In-memory delivery simulation for fingerprint-verified PREVIEW_ONLY output.

This module deliberately accepts only its own recording double.  It cannot
receive a Telegram bot, token or production sender and performs no network,
database, worker, environment or LIVE operation.  A recorded message is a test
transcript only and never becomes research evidence.
"""

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import re
from typing import Any, Dict, Mapping

import research_experimental_preview_contract as preview_contract
import research_experimental_preview_storage_contract as preview_storage


SIMULATOR_VERSION = "preview-fake-telegram-delivery-v1-disconnected"
MODE = "PREVIEW_FAKE_TELEGRAM_IN_MEMORY_ONLY"
SIMULATION_CHANNEL = "FAKE_TELEGRAM_IN_MEMORY"

RECORDED = "SIMULATION_RECORDED"
SKIPPED_DUPLICATE = "SIMULATION_SKIPPED_DUPLICATE"
SUPPRESSED = "SIMULATION_SUPPRESSED"

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


def _hex(value: Any, *, name: str) -> str:
    normalized = str(value or "").strip().lower()
    if not _HEX_64.fullmatch(normalized):
        raise ValueError(f"{name} must be a lowercase 64-character SHA-256 id")
    return normalized


class InMemoryPreviewTelegramDouble:
    """A sealed recorder with no Telegram-compatible sending method."""

    def __init__(self) -> None:
        self._records_by_key: Dict[str, Dict[str, Any]] = {}

    def record_preview(self, record: Mapping[str, Any]) -> bool:
        if not isinstance(record, Mapping):
            raise ValueError("fake Telegram record must be an object")
        supplied = deepcopy(dict(record))
        key = _hex(supplied.get("preview_key"), name="preview_key")
        if key in self._records_by_key:
            return False
        self._records_by_key[key] = supplied
        return True

    def records(self) -> list[Dict[str, Any]]:
        return [
            deepcopy(self._records_by_key[key])
            for key in sorted(self._records_by_key)
        ]


def simulate_preview_delivery(
    plan: Mapping[str, Any],
    *,
    recorder: InMemoryPreviewTelegramDouble,
) -> Dict[str, Any]:
    """Record eligible previews in memory after re-verifying every fingerprint."""

    if type(recorder) is not InMemoryPreviewTelegramDouble:
        raise ValueError("only the sealed in-memory Telegram double is accepted")
    prepared = preview_storage.prepare_preview_append_only_records(plan)
    batch = prepared["batch_record"]
    if batch["public_opt_in"] is not False:
        raise ValueError("PREVIEW_ONLY public opt-in must remain disabled")
    if batch["stage6_activated"] is not False:
        raise ValueError("PREVIEW_ONLY may not activate Stage 6")

    decisions = []
    for record in prepared["decision_records"]:
        payload = json.loads(record["decision_payload_json"])
        eligible = record["status"] == preview_contract.PREVIEW_SIMULATED_ELIGIBLE
        if eligible and (
            batch["stage5_status"] != "WAITING_DATA"
            or batch["route"] != "TEST_ALLOWLIST"
        ):
            raise ValueError("fake delivery eligibility violates PREVIEW_ONLY")

        simulation_status = SUPPRESSED
        recorded = False
        if eligible:
            fake_record = {
                "preview_batch_id": batch["preview_batch_id"],
                "preview_decision_id": record["preview_decision_id"],
                "source_audit_decision_id": record["source_audit_decision_id"],
                "chat_id": record["chat_id"],
                "route": record["route"],
                "formula_family_id": record["formula_family_id"],
                "representative_snapshot_id": record[
                    "representative_snapshot_id"
                ],
                "preview_key": record["preview_key"],
                "preview_message_sha256": record["preview_message_sha256"],
                "text": payload["text"],
                "research_evidence_effect": "NONE",
                "simulation_channel": SIMULATION_CHANNEL,
            }
            recorded = recorder.record_preview(fake_record)
            simulation_status = RECORDED if recorded else SKIPPED_DUPLICATE

        decision_payload = {
            "simulator_version": SIMULATOR_VERSION,
            "preview_decision_id": record["preview_decision_id"],
            "preview_key": record["preview_key"],
            "status": simulation_status,
            "recorded_in_memory": recorded,
            "telegram_api_calls": 0,
            "delivery_attempts": 0,
            "database_writes": 0,
            "research_evidence_writes": 0,
            "research_evidence_effect": "NONE",
            "live_effect": "NONE",
        }
        decisions.append(
            {
                **decision_payload,
                "simulation_decision_id": _hash(decision_payload),
            }
        )

    batch_payload = {
        "simulator_version": SIMULATOR_VERSION,
        "preview_batch_id": batch["preview_batch_id"],
        "simulation_decision_ids": [
            decision["simulation_decision_id"] for decision in decisions
        ],
    }
    return {
        "simulator_version": SIMULATOR_VERSION,
        "mode": MODE,
        "simulation_batch_id": _hash(batch_payload),
        "preview_batch_id": batch["preview_batch_id"],
        "simulation_channel": SIMULATION_CHANNEL,
        "decisions_considered": len(decisions),
        "messages_recorded_in_memory": sum(
            decision["status"] == RECORDED for decision in decisions
        ),
        "duplicates_skipped": sum(
            decision["status"] == SKIPPED_DUPLICATE for decision in decisions
        ),
        "suppressed": sum(
            decision["status"] == SUPPRESSED for decision in decisions
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
