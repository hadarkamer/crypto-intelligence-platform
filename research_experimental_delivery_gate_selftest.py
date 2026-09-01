"""Deterministic regressions for the disconnected Experimental gate."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import research_evidence_contract as evidence_contract
import research_experimental_delivery_gate as gate
import research_formula_relevance


ROOT = Path(__file__).resolve().parent
FIXTURES = ROOT / "fixtures" / "evidence"
NOW = datetime(2026, 9, 1, 13, 5, tzinfo=timezone.utc)


def _fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _snapshot(fixture: dict) -> evidence_contract.EvidenceSnapshot:
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
        matched_parent_market_episode_ids=fixture["matched_parent_market_episode_ids"],
        control_parent_market_episode_ids=fixture["control_parent_market_episode_ids"],
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


def _policy(**overrides) -> dict:
    value = {
        "enabled": True,
        "kill_switch_engaged": False,
        "allow_opt_in": False,
        "test_chat_ids": [-1001],
        "opted_in_chat_ids": [],
        "cooldown_seconds": 1800,
    }
    value.update(overrides)
    return value


def _raises(expected: str, function) -> None:
    try:
        function()
    except ValueError as exc:
        assert expected in str(exc), exc
    else:
        raise AssertionError(f"expected ValueError containing {expected!r}")


def run() -> None:
    current_fixture = _fixture("current_v7_probability.json")
    current = _snapshot(current_fixture)
    relevance = _relevance(current)
    ready = gate.plan_experimental_dry_run(
        [current, current.to_dict()],
        relevance_by_snapshot={current.snapshot_id: relevance},
        chat_id=-1001,
        stage5_status="READY",
        policy=_policy(),
        now_utc=NOW,
    )
    assert ready["mode"] == gate.MODE
    assert ready["families_considered"] == 1
    assert ready["simulated_eligible"] == 1
    assert ready["suppressed"] == 0
    assert ready["delivery_attempts"] == 0
    assert ready["telegram_api_calls"] == 0
    assert ready["database_writes"] == 0
    assert ready["research_evidence_writes"] == 0
    assert ready["research_evidence_effect"] == "NONE"
    assert ready["delivery_channel"] == "NONE"
    assert ready["live_effect"] == "NONE"
    assert ready["audit_contract_version"] == gate.AUDIT_CONTRACT_VERSION
    assert len(ready["audit_batch_id"]) == 64
    audit = ready["audits"][0]
    assert audit["status"] == gate.SIMULATED_ELIGIBLE
    assert audit["route"] == "TEST_ALLOWLIST"
    assert len(audit["aggregated_snapshot_ids"]) == 1
    assert len(audit["audit_decision_id"]) == 64
    assert len(audit["relevance_decision_sha256"]) == 64
    assert audit["research_evidence_effect"] == "NONE"

    identical = gate.plan_experimental_dry_run(
        [current, current.to_dict()],
        relevance_by_snapshot={current.snapshot_id: relevance},
        chat_id=-1001,
        stage5_status="READY",
        policy=_policy(),
        now_utc=NOW,
    )
    assert identical["audit_batch_id"] == ready["audit_batch_id"]
    assert (
        identical["audits"][0]["audit_decision_id"]
        == audit["audit_decision_id"]
    )

    waiting = gate.plan_experimental_dry_run(
        [current],
        relevance_by_snapshot={current.snapshot_id: relevance},
        chat_id=-1001,
        stage5_status="WAITING_DATA",
        now_utc=NOW,
    )
    waiting_blockers = waiting["audits"][0]["blockers"]
    assert waiting["simulated_eligible"] == 0
    assert "Experimental feature flag is disabled" in waiting_blockers
    assert "Experimental kill switch is engaged" in waiting_blockers
    assert "Stage 5 is not READY" in waiting_blockers
    assert "Experimental cooldown is not configured" in waiting_blockers

    duplicate = gate.plan_experimental_dry_run(
        [current],
        relevance_by_snapshot={current.snapshot_id: relevance},
        chat_id=-1001,
        stage5_status="READY",
        policy=_policy(),
        existing_delivery_keys=[audit["delivery_key"]],
        now_utc=NOW,
    )
    assert "delivery idempotency key already exists" in duplicate["audits"][0][
        "blockers"
    ]

    cooling = gate.plan_experimental_dry_run(
        [current],
        relevance_by_snapshot={current.snapshot_id: relevance},
        chat_id=-1001,
        stage5_status="READY",
        policy=_policy(),
        last_delivery_at_by_chat_family={
            f"-1001:{current.formula_family_id}": NOW - timedelta(minutes=29)
        },
        now_utc=NOW,
    )
    assert cooling["audits"][0]["cooldown_remaining_seconds"] == 60
    assert "chat/family cooldown is active" in cooling["audits"][0]["blockers"]

    cooldown_complete = gate.plan_experimental_dry_run(
        [current],
        relevance_by_snapshot={current.snapshot_id: relevance},
        chat_id=-1001,
        stage5_status="READY",
        policy=_policy(),
        last_delivery_at_by_chat_family={
            f"-1001:{current.formula_family_id}": NOW - timedelta(minutes=30)
        },
        now_utc=NOW,
    )
    assert cooldown_complete["simulated_eligible"] == 1

    opt_in = gate.plan_experimental_dry_run(
        [current],
        relevance_by_snapshot={current.snapshot_id: relevance},
        chat_id=-2002,
        stage5_status="READY",
        policy=_policy(
            allow_opt_in=True,
            test_chat_ids=[],
            opted_in_chat_ids=[-2002],
        ),
        now_utc=NOW,
    )
    assert opt_in["route"] == "OPT_IN"
    assert opt_in["simulated_eligible"] == 1

    no_access = gate.plan_experimental_dry_run(
        [current],
        relevance_by_snapshot={current.snapshot_id: relevance},
        chat_id=-3003,
        stage5_status="READY",
        policy=_policy(),
        now_utc=NOW,
    )
    assert (
        "chat is neither test-allowlisted nor separately opted in"
        in no_access["audits"][0]["blockers"]
    )

    suspended = deepcopy(relevance)
    suspended["state"] = research_formula_relevance.SUSPENDED
    suspended["experimental_relevance_eligible"] = False
    suspended_result = gate.plan_experimental_dry_run(
        [current],
        relevance_by_snapshot={current.snapshot_id: suspended},
        chat_id=-1001,
        stage5_status="READY",
        policy=_policy(),
        now_utc=NOW,
    )
    assert "current relevance state is not delivery-eligible" in suspended_result[
        "audits"
    ][0]["blockers"]
    assert "relevance decision blocks Experimental delivery" in suspended_result[
        "audits"
    ][0]["blockers"]
    assert (
        suspended_result["audits"][0]["audit_decision_id"]
        != audit["audit_decision_id"]
    )

    legacy = _snapshot(_fixture("legacy_v6_shadow.json"))
    legacy_relevance = _relevance(legacy)
    legacy_result = gate.plan_experimental_dry_run(
        [legacy],
        relevance_by_snapshot={legacy.snapshot_id: legacy_relevance},
        chat_id=-1001,
        stage5_status="READY",
        policy=_policy(),
        now_utc=NOW,
    )
    legacy_blockers = legacy_result["audits"][0]["blockers"]
    assert "Legacy Shadow evidence is read-only" in legacy_blockers
    assert "relevance decision blocks Experimental delivery" in legacy_blockers

    newer_fixture = deepcopy(current_fixture)
    newer_fixture["formula_contract"]["formula_key"] = "3" * 64
    newer_fixture["assessed_at_utc"] = "2026-08-31T12:00:00Z"
    newer = _snapshot(newer_fixture)
    newer_relevance = _relevance(newer)
    aggregated = gate.plan_experimental_dry_run(
        [current, newer],
        relevance_by_snapshot={
            current.snapshot_id: relevance,
            newer.snapshot_id: newer_relevance,
        },
        chat_id=-1001,
        stage5_status="READY",
        policy=_policy(),
        now_utc=NOW,
    )
    assert aggregated["families_considered"] == 1
    assert aggregated["audits"][0]["representative_snapshot_id"] == newer.snapshot_id
    assert len(aggregated["audits"][0]["aggregated_snapshot_ids"]) == 2

    tampered = current.to_dict()
    tampered["evidence"]["symbol"] = "ETH"
    _raises(
        "fingerprint mismatch",
        lambda: gate.plan_experimental_dry_run(
            [tampered],
            relevance_by_snapshot={},
            chat_id=-1001,
            stage5_status="READY",
            policy=_policy(),
            now_utc=NOW,
        ),
    )
    _raises(
        "unknown fields",
        lambda: gate.plan_experimental_dry_run(
            [current],
            relevance_by_snapshot={current.snapshot_id: relevance},
            chat_id=-1001,
            stage5_status="READY",
            policy={**_policy(), "live_enabled": True},
            now_utc=NOW,
        ),
    )

    source = (ROOT / "research_experimental_delivery_gate.py").read_text(
        encoding="utf-8"
    ).lower()
    for forbidden in (
        "from telegram",
        "import telegram",
        "send_message(",
        "reply_text(",
        "research_formula_store",
        "research_formula_worker",
        ".advance(",
        "os.getenv",
        "requests.",
        "aiohttp.",
    ):
        assert forbidden not in source
    for production_file in ("main.py", "ai_telegram.py", "research_formula_worker.py"):
        production_source = (ROOT / production_file).read_text(encoding="utf-8")
        assert "research_experimental_delivery_gate" not in production_source
        assert "/experimental_on" not in production_source

    print("research_experimental_delivery_gate_selftest: ok")


if __name__ == "__main__":
    run()
