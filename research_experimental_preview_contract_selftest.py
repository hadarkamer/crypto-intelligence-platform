"""Deterministic safety regressions for PREVIEW_ONLY authorization."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path

import research_evidence_contract as evidence_contract
import research_experimental_delivery_gate as gate
import research_experimental_preview_contract as preview_contract
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


def _gate_plan(*, stage5_status: str = "WAITING_DATA", opt_in: bool = False) -> dict:
    snapshot = _snapshot()
    relevance = _relevance(snapshot)
    chat_id = -2002 if opt_in else -1001
    return gate.plan_experimental_dry_run(
        [snapshot],
        relevance_by_snapshot={snapshot.snapshot_id: relevance},
        chat_id=chat_id,
        stage5_status=stage5_status,
        policy={
            "enabled": True,
            "kill_switch_engaged": False,
            "allow_opt_in": opt_in,
            "test_chat_ids": [] if opt_in else [-1001],
            "opted_in_chat_ids": [-2002] if opt_in else [],
            "cooldown_seconds": 1800,
        },
        now_utc=NOW,
    )


def _preview_policy(**overrides) -> dict:
    policy = {
        "enabled": True,
        "kill_switch_engaged": False,
        "owner_preview_approved": True,
        "test_chat_ids": [-1001],
    }
    policy.update(overrides)
    return policy


def _raises(expected: str, function) -> None:
    try:
        function()
    except ValueError as exc:
        assert expected in str(exc), exc
    else:
        raise AssertionError(f"expected ValueError containing {expected!r}")


def run() -> None:
    source_plan = _gate_plan()
    ready = preview_contract.plan_preview_only(
        source_plan,
        policy=_preview_policy(),
    )
    assert ready["mode"] == preview_contract.MODE
    assert ready["stage5_status"] == "WAITING_DATA"
    assert ready["preview_simulated_eligible"] == 1
    assert ready["preview_suppressed"] == 0
    assert ready["public_opt_in"] is False
    assert ready["stage6_activated"] is False
    assert ready["delivery_attempts"] == 0
    assert ready["telegram_api_calls"] == 0
    assert ready["database_writes"] == 0
    assert ready["research_evidence_writes"] == 0
    assert ready["research_evidence_effect"] == "NONE"
    assert ready["delivery_channel"] == "NONE"
    assert ready["live_effect"] == "NONE"
    preview = ready["previews"][0]
    assert preview["status"] == preview_contract.PREVIEW_SIMULATED_ELIGIBLE
    assert preview["blockers"] == []
    assert preview["text"].startswith(preview_contract.LABEL)
    assert len(preview["preview_key"]) == 64
    assert len(preview["preview_decision_id"]) == 64

    repeated = preview_contract.plan_preview_only(
        deepcopy(source_plan),
        policy=_preview_policy(),
    )
    assert repeated == ready

    broader_allowlist = preview_contract.plan_preview_only(
        source_plan,
        policy=_preview_policy(test_chat_ids=[-1001, -9999]),
    )
    assert broader_allowlist["preview_simulated_eligible"] == 1
    assert (
        broader_allowlist["previews"][0]["preview_decision_id"]
        != preview["preview_decision_id"]
    )

    disabled = preview_contract.plan_preview_only(source_plan)
    disabled_blockers = disabled["previews"][0]["blockers"]
    assert "PREVIEW_ONLY feature flag is disabled" in disabled_blockers
    assert "PREVIEW_ONLY kill switch is engaged" in disabled_blockers
    assert "owner approval for the test-chat preview is absent" in disabled_blockers

    duplicate = preview_contract.plan_preview_only(
        source_plan,
        policy=_preview_policy(),
        existing_preview_keys=[preview["preview_key"]],
    )
    assert "preview idempotency key already exists" in duplicate["previews"][0][
        "blockers"
    ]

    normal_ready = preview_contract.plan_preview_only(
        _gate_plan(stage5_status="READY"),
        policy=_preview_policy(),
    )
    assert normal_ready["preview_simulated_eligible"] == 0
    assert (
        "Stage 5 is not WAITING_DATA; use the normal Experimental review"
        in normal_ready["previews"][0]["blockers"]
    )

    opt_in = preview_contract.plan_preview_only(
        _gate_plan(opt_in=True),
        policy=_preview_policy(test_chat_ids=[-2002]),
    )
    opt_in_blockers = opt_in["previews"][0]["blockers"]
    assert "source gate route is not TEST_ALLOWLIST" in opt_in_blockers
    assert "public opt-in is forbidden in PREVIEW_ONLY" in opt_in_blockers

    tampered = deepcopy(source_plan)
    tampered["audits"][0]["text"] += " tampered"
    _raises(
        "fingerprint mismatch",
        lambda: preview_contract.plan_preview_only(
            tampered,
            policy=_preview_policy(),
        ),
    )
    _raises(
        "unknown fields",
        lambda: preview_contract.plan_preview_only(
            source_plan,
            policy={**_preview_policy(), "public_opt_in": True},
        ),
    )

    source = (ROOT / "research_experimental_preview_contract.py").read_text(
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
    for production_file in (
        "main.py",
        "ai_telegram.py",
        "research_formula_worker.py",
        "research_formula_store.py",
        "research_formula_schema_admin.py",
    ):
        production_source = (ROOT / production_file).read_text(encoding="utf-8")
        assert "research_experimental_preview_contract" not in production_source
        assert "/experimental_preview" not in production_source

    print("research_experimental_preview_contract_selftest: ok")


if __name__ == "__main__":
    run()
