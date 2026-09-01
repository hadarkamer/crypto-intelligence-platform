"""Regressions for the unapplied Experimental storage contract."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path

import research_evidence_contract as evidence_contract
import research_experimental_delivery_gate as gate
import research_experimental_storage_contract as storage_contract
import research_formula_relevance


ROOT = Path(__file__).resolve().parent
FIXTURE = ROOT / "fixtures" / "evidence" / "current_v7_probability.json"
NOW = datetime(2026, 9, 1, 13, 5, tzinfo=timezone.utc)


def _snapshot() -> evidence_contract.EvidenceSnapshot:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assessment = evidence_contract.FormulaAssessment.from_acceptance(
        fixture["assessment"], phase=fixture["phase"]
    )
    return evidence_contract.EvidenceSnapshot.build(
        formula_contract=fixture["formula_contract"],
        assessment=assessment,
        assessed_at_utc=fixture["assessed_at_utc"],
        formula_family_id=fixture.get("formula_family_id"),
        matched_market_episode_ids=fixture["matched_market_episode_ids"],
        control_market_episode_ids=fixture["control_market_episode_ids"],
        matched_parent_market_episode_ids=fixture[
            "matched_parent_market_episode_ids"
        ],
        control_parent_market_episode_ids=fixture[
            "control_parent_market_episode_ids"
        ],
        raw_match_count=fixture["raw_match_count"],
        raw_control_count=fixture["raw_control_count"],
        matched_n_eff=fixture["matched_n_eff"],
        control_n_eff=fixture["control_n_eff"],
        metrics=fixture["metrics"],
        evidence=fixture["evidence"],
        provenance=fixture["provenance"],
    )


def _relevance(snapshot: evidence_contract.EvidenceSnapshot) -> dict:
    payload = snapshot.to_dict()
    return research_formula_relevance.advance(
        previous=None,
        formula_contract=payload["formula"],
        compatibility=snapshot.compatibility,
        assessment=payload["assessment"],
        evidence_fingerprint="e" * 64,
        observed_at_utc=NOW,
        snapshot_id=snapshot.snapshot_id,
    )


def _policy() -> dict:
    return {
        "enabled": True,
        "kill_switch_engaged": False,
        "allow_opt_in": False,
        "test_chat_ids": [-1001],
        "opted_in_chat_ids": [],
        "cooldown_seconds": 1800,
    }


def _plan(*, stage5_status: str = "READY") -> dict:
    snapshot = _snapshot()
    relevance = _relevance(snapshot)
    policy = _policy() if stage5_status == "READY" else None
    return gate.plan_experimental_dry_run(
        [snapshot],
        relevance_by_snapshot={snapshot.snapshot_id: relevance},
        chat_id=-1001,
        stage5_status=stage5_status,
        policy=policy,
        now_utc=NOW,
    )


def _raises(expected: str, function) -> None:
    try:
        function()
    except ValueError as exc:
        assert expected in str(exc), exc
    else:
        raise AssertionError(f"expected ValueError containing {expected!r}")


def run() -> None:
    plan = _plan()
    prepared = storage_contract.prepare_append_only_records(plan)
    assert prepared["storage_contract_version"] == (
        storage_contract.STORAGE_CONTRACT_VERSION
    )
    assert prepared["mode"] == storage_contract.MODE
    assert prepared["migration_name"] == storage_contract.MIGRATION_NAME
    assert prepared["migration_registered"] is False
    assert prepared["database_writes"] == 0
    assert prepared["research_evidence_writes"] == 0
    assert prepared["research_evidence_effect"] == "NONE"
    assert prepared["delivery_attempts"] == 0
    assert prepared["delivery_channel"] == "NONE"
    assert prepared["live_effect"] == "NONE"
    assert prepared["batch_record"]["audit_batch_id"] == plan["audit_batch_id"]
    assert len(prepared["decision_records"]) == 1
    decision = prepared["decision_records"][0]
    assert decision["audit_decision_id"] == plan["audits"][0][
        "audit_decision_id"
    ]
    assert decision["delivery_key"] == plan["audits"][0]["delivery_key"]
    assert json.loads(decision["decision_payload_json"])[
        "research_evidence_effect"
    ] == "NONE"

    repeated = storage_contract.prepare_append_only_records(deepcopy(plan))
    assert repeated == prepared

    waiting = storage_contract.prepare_append_only_records(
        _plan(stage5_status="WAITING_DATA")
    )
    assert waiting["batch_record"]["stage5_status"] == "WAITING_DATA"
    assert waiting["decision_records"][0]["status"] == gate.SUPPRESSED

    tampered_decision = deepcopy(plan)
    tampered_decision["audits"][0]["research_evidence_effect"] = "ADDS_EVIDENCE"
    _raises(
        "fingerprint mismatch",
        lambda: storage_contract.prepare_append_only_records(tampered_decision),
    )
    tampered_batch = deepcopy(plan)
    tampered_batch["audit_batch_id"] = "0" * 64
    _raises(
        "batch fingerprint mismatch",
        lambda: storage_contract.prepare_append_only_records(tampered_batch),
    )
    unsafe = deepcopy(plan)
    unsafe["research_evidence_writes"] = 1
    _raises(
        "research_evidence_writes",
        lambda: storage_contract.prepare_append_only_records(unsafe),
    )

    source = (ROOT / "research_experimental_storage_contract.py").read_text(
        encoding="utf-8"
    ).lower()
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

    migration_name = storage_contract.MIGRATION_NAME
    migration = (ROOT / "migrations" / migration_name).read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS research_experimental_audit_batches" in migration
    assert "CREATE TABLE IF NOT EXISTS research_experimental_audit_decisions" in migration
    assert "BEFORE UPDATE OR DELETE ON research_experimental_audit_batches" in migration
    assert "BEFORE UPDATE OR DELETE ON research_experimental_audit_decisions" in migration
    assert "BEFORE TRUNCATE ON research_experimental_audit_batches" in migration
    assert "BEFORE TRUNCATE ON research_experimental_audit_decisions" in migration
    assert "research_evidence_effect' = 'NONE'" in migration
    assert "CREATE TABLE IF NOT EXISTS research_formula_alert_subscriptions" not in migration
    assert "CREATE TABLE IF NOT EXISTS research_formula_live_deliveries" not in migration

    schema_admin = (ROOT / "research_formula_schema_admin.py").read_text(
        encoding="utf-8"
    )
    assert migration_name not in schema_admin
    for production_file in (
        "main.py",
        "ai_telegram.py",
        "research_formula_worker.py",
        "research_formula_store.py",
    ):
        production_source = (ROOT / production_file).read_text(encoding="utf-8")
        assert "research_experimental_storage_contract" not in production_source

    print("research_experimental_storage_contract_selftest: ok")


if __name__ == "__main__":
    run()
