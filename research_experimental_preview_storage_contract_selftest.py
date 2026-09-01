"""Regressions for the unapplied PREVIEW_ONLY audit storage boundary."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import research_experimental_preview_contract as preview_contract
import research_experimental_preview_contract_selftest as preview_fixtures
import research_experimental_preview_storage_contract as preview_storage


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
    prepared = preview_storage.prepare_preview_append_only_records(plan)
    assert prepared["storage_contract_version"] == (
        preview_storage.STORAGE_CONTRACT_VERSION
    )
    assert prepared["mode"] == preview_storage.MODE
    assert prepared["migration_name"] == preview_storage.MIGRATION_NAME
    assert prepared["migration_registered"] is False
    assert prepared["database_writes"] == 0
    assert prepared["research_evidence_writes"] == 0
    assert prepared["research_evidence_effect"] == "NONE"
    assert prepared["delivery_attempts"] == 0
    assert prepared["delivery_channel"] == "NONE"
    assert prepared["live_effect"] == "NONE"
    assert prepared["batch_record"]["preview_batch_id"] == plan[
        "preview_batch_id"
    ]
    assert prepared["batch_record"]["public_opt_in"] is False
    assert prepared["batch_record"]["stage6_activated"] is False
    assert len(prepared["decision_records"]) == 1
    decision = prepared["decision_records"][0]
    assert decision["preview_decision_id"] == plan["previews"][0][
        "preview_decision_id"
    ]
    assert decision["preview_key"] == plan["previews"][0]["preview_key"]
    stored_payload = json.loads(decision["decision_payload_json"])
    assert stored_payload["text"].startswith(preview_contract.LABEL)
    assert stored_payload["research_evidence_effect"] == "NONE"

    repeated = preview_storage.prepare_preview_append_only_records(deepcopy(plan))
    assert repeated == prepared

    disabled = preview_storage.prepare_preview_append_only_records(
        _plan(disabled=True)
    )
    assert disabled["decision_records"][0]["status"] == (
        preview_contract.PREVIEW_SUPPRESSED
    )

    stage5_ready = preview_storage.prepare_preview_append_only_records(
        _plan(stage5_status="READY")
    )
    assert stage5_ready["batch_record"]["stage5_status"] == "READY"
    assert stage5_ready["decision_records"][0]["status"] == (
        preview_contract.PREVIEW_SUPPRESSED
    )

    tampered_decision = deepcopy(plan)
    tampered_decision["previews"][0]["text"] += " tampered"
    _raises(
        "decision fingerprint mismatch",
        lambda: preview_storage.prepare_preview_append_only_records(
            tampered_decision
        ),
    )
    tampered_batch = deepcopy(plan)
    tampered_batch["preview_batch_id"] = "0" * 64
    _raises(
        "batch fingerprint mismatch",
        lambda: preview_storage.prepare_preview_append_only_records(
            tampered_batch
        ),
    )
    unsafe = deepcopy(plan)
    unsafe["research_evidence_writes"] = 1
    _raises(
        "research_evidence_writes",
        lambda: preview_storage.prepare_preview_append_only_records(unsafe),
    )

    source = (
        ROOT / "research_experimental_preview_storage_contract.py"
    ).read_text(encoding="utf-8").lower()
    for forbidden in (
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

    migration_name = preview_storage.MIGRATION_NAME
    migration = (ROOT / "migrations" / migration_name).read_text(encoding="utf-8")
    for required in (
        "CREATE TABLE IF NOT EXISTS research_experimental_preview_audit_batches",
        "CREATE TABLE IF NOT EXISTS research_experimental_preview_audit_decisions",
        "BEFORE UPDATE OR DELETE ON research_experimental_preview_audit_batches",
        "BEFORE UPDATE OR DELETE ON research_experimental_preview_audit_decisions",
        "BEFORE TRUNCATE ON research_experimental_preview_audit_batches",
        "BEFORE TRUNCATE ON research_experimental_preview_audit_decisions",
        "public_opt_in = FALSE",
        "stage6_activated = FALSE",
        "research_evidence_effect' = 'NONE'",
    ):
        assert required in migration
    assert "CREATE TABLE IF NOT EXISTS research_formula_live_deliveries" not in migration
    assert "CREATE TABLE IF NOT EXISTS research_formula_alert_subscriptions" not in migration

    schema_admin = (ROOT / "research_formula_schema_admin.py").read_text(
        encoding="utf-8"
    )
    assert migration_name not in schema_admin
    for production_file in (
        "main.py",
        "ai_telegram.py",
        "research_formula_worker.py",
        "research_formula_store.py",
        "research_formula_schema_admin.py",
    ):
        production_source = (ROOT / production_file).read_text(encoding="utf-8")
        assert "research_experimental_preview_storage_contract" not in production_source

    print("research_experimental_preview_storage_contract_selftest: ok")


if __name__ == "__main__":
    run()
