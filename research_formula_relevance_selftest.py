"""Deterministic regressions for versioned Formula relevance hysteresis."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import inspect
import json
from pathlib import Path

import research_evidence_contract as evidence_contract
import research_formula_relevance as relevance
import research_formula_schema_admin
import research_formula_store
import research_formula_worker


ROOT = Path(__file__).resolve().parent
FIXTURES = ROOT / "fixtures" / "evidence"


def _fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _assessment(source: dict, *, maturity: str, research_ready: bool):
    payload = deepcopy(source["assessment"])
    payload["maturity"] = maturity
    payload["research_ready"] = research_ready
    payload["accepted_paths"] = ["PROBABILITY"] if research_ready else []
    return evidence_contract.FormulaAssessment.from_acceptance(
        payload, phase="PROSPECTIVE"
    )


def _advance(previous, fixture, assessment, evidence, when, snapshot):
    return relevance.advance(
        previous=previous,
        formula_contract=fixture["formula_contract"],
        compatibility=evidence_contract.CURRENT_V7,
        assessment=assessment,
        evidence_fingerprint=evidence,
        observed_at_utc=when,
        snapshot_id=snapshot,
    )


class _Rows:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None


class _Stage3BConnection:
    def __init__(self, formula):
        self.formula = dict(formula)
        self.snapshots = {}
        self.relevance = []

    def execute(self, query, params=()):
        normalized = " ".join(str(query).split())
        assert normalized.count("%s") == len(params)
        if "FROM research_formulas" in normalized:
            return _Rows([dict(self.formula)])
        if normalized.startswith("INSERT INTO research_formula_evidence_snapshots"):
            payload = json.loads(params[21])
            self.snapshots.setdefault(
                payload["snapshot_id"],
                {
                    "formula_id": int(params[5]),
                    "source_run_id": params[6],
                    "snapshot_payload": payload,
                },
            )
            return _Rows()
        if "FROM research_formula_evidence_snapshots" in normalized:
            stored = self.snapshots.get(str(params[0]))
            return _Rows([] if stored is None else [dict(stored)])
        if normalized.startswith("INSERT INTO research_formula_relevance_assessments"):
            payload = json.loads(params[16])
            if not any(
                row["decision_payload"]["observation_fingerprint"]
                == payload["observation_fingerprint"]
                for row in self.relevance
            ):
                self.relevance.append(
                    {"snapshot_id": payload["snapshot_id"], "decision_payload": payload}
                )
            return _Rows()
        if "FROM research_formula_relevance_assessments" in normalized:
            if "observation_fingerprint=%s" in normalized:
                fingerprint = str(params[3])
                rows = [
                    row
                    for row in self.relevance
                    if row["decision_payload"]["observation_fingerprint"] == fingerprint
                ]
                return _Rows(rows)
            return _Rows(list(reversed(self.relevance)))
        raise AssertionError(f"unexpected Stage 3B SQL: {normalized}")


def run() -> None:
    current = _fixture("current_v7_probability.json")
    strong = _assessment(
        current, maturity="RESEARCH_READY", research_ready=True
    )
    weak = _assessment(
        current,
        maturity="EVIDENCE_PRESENT_EDGE_NOT_ESTABLISHED",
        research_ready=False,
    )
    start = datetime(2026, 8, 31, 8, tzinfo=timezone.utc)

    first = _advance(None, current, strong, "1" * 64, start, "a" * 64)
    assert first["state"] == relevance.RELEVANT
    assert first["transition"] == "BECAME_RELEVANT"
    assert first["experimental_relevance_eligible"] is True
    assert first["live_effect"] == "NONE"
    assert first["delivery_channel"] == "NONE"

    duplicate = _advance(first, current, strong, "1" * 64, start, "a" * 64)
    assert duplicate["duplicate_observation"] is True
    assert duplicate["state"] == relevance.RELEVANT

    weakening = _advance(
        first, current, weak, "2" * 64, start + timedelta(hours=1), "b" * 64
    )
    assert weakening["state"] == relevance.WEAKENING
    assert weakening["weak_observation_streak"] == 1
    assert weakening["experimental_relevance_eligible"] is True

    suspended = _advance(
        weakening,
        current,
        weak,
        "2" * 64,
        start + timedelta(days=1),
        "c" * 64,
    )
    assert suspended["state"] == relevance.SUSPENDED
    assert suspended["transition"] == "SUSPENDED"
    assert suspended["experimental_relevance_eligible"] is False

    recovering = _advance(
        suspended,
        current,
        strong,
        "3" * 64,
        start + timedelta(days=1, hours=1),
        "d" * 64,
    )
    assert recovering["state"] == relevance.RECOVERING
    assert recovering["recovery_evidence_streak"] == 1
    assert recovering["evidence_advanced"] is True

    same_rise = _advance(
        recovering,
        current,
        strong,
        "3" * 64,
        start + timedelta(days=2),
        "e" * 64,
    )
    assert same_rise["state"] == relevance.RECOVERING
    assert same_rise["recovery_evidence_streak"] == 1
    assert same_rise["evidence_advanced"] is False
    assert same_rise["experimental_relevance_eligible"] is False

    restored = _advance(
        same_rise,
        current,
        strong,
        "4" * 64,
        start + timedelta(days=2, hours=1),
        "f" * 64,
    )
    assert restored["state"] == relevance.RELEVANT
    assert restored["transition"] == "REACTIVATED"
    assert restored["experimental_relevance_eligible"] is True

    legacy = _fixture("legacy_v6_shadow.json")
    legacy_assessment = _assessment(
        legacy, maturity="ACCUMULATING_EVIDENCE", research_ready=False
    )
    legacy_state = relevance.advance(
        previous=None,
        formula_contract=legacy["formula_contract"],
        compatibility=evidence_contract.LEGACY_SHADOW_READ_ONLY,
        assessment=legacy_assessment,
        evidence_fingerprint="5" * 64,
        observed_at_utc=start,
        snapshot_id="6" * 64,
    )
    assert legacy_state["state"] == relevance.LEGACY_READ_ONLY
    assert legacy_state["experimental_relevance_eligible"] is False
    legacy_duplicate = relevance.advance(
        previous=legacy_state,
        formula_contract=legacy["formula_contract"],
        compatibility=evidence_contract.LEGACY_SHADOW_READ_ONLY,
        assessment=legacy_assessment,
        evidence_fingerprint="5" * 64,
        observed_at_utc=start + timedelta(days=1),
        snapshot_id="6" * 64,
    )
    assert legacy_duplicate["duplicate_observation"] is True

    # A policy-version change never carries an old streak into a new contract.
    prior_policy = {**weakening, "policy_version": "old-policy"}
    reset = _advance(
        prior_policy,
        current,
        weak,
        "7" * 64,
        start + timedelta(days=3),
        "8" * 64,
    )
    assert reset["state"] == relevance.OBSERVING
    assert reset["weak_observation_streak"] == 0

    descriptor = relevance.descriptor()
    assert descriptor["same_market_episode_adds_evidence"] is False
    assert descriptor["identical_polling_advances_state"] is False
    assert descriptor["live_effect"] == "NONE"

    current_formula = {
        "formula_id": 42,
        **current["formula_contract"],
        "latest_evaluation_run_id": 17,
        "latest_multiple_testing": {
            "evidence_family": {"family_id": current["formula_family_id"]}
        },
    }
    validation = {
        "policy_version": "shadow-selftest-v1",
        "input_snapshot_policy_version": "input-selftest-v1",
        "outcome_method_version": current["formula_contract"][
            "outcome_method_version"
        ],
        "evaluated_at_utc": current["assessed_at_utc"],
        "research_acceptance": current["assessment"],
        "metrics": current["metrics"],
        "evidence": {
            **current["evidence"],
            "market_episode_fingerprint": "9" * 64,
        },
        "formula_contract": {"current_v7": True},
        "evidence_snapshot_inputs": {
            "matched_market_episode_ids": current[
                "matched_market_episode_ids"
            ],
            "control_market_episode_ids": current[
                "control_market_episode_ids"
            ],
            "matched_parent_market_episode_ids": current[
                "matched_parent_market_episode_ids"
            ],
            "control_parent_market_episode_ids": current[
                "control_parent_market_episode_ids"
            ],
            "raw_match_count": current["raw_match_count"],
            "raw_control_count": current["raw_control_count"],
            "matched_n_eff": current["matched_n_eff"],
            "control_n_eff": current["control_n_eff"],
        },
    }
    built_snapshot = research_formula_store._build_shadow_evidence_snapshot(
        current_formula, validation
    )
    assert isinstance(built_snapshot, evidence_contract.EvidenceSnapshot)
    assert built_snapshot.compatibility == evidence_contract.CURRENT_V7
    assert built_snapshot.formula_family_id == current["formula_family_id"]
    assert built_snapshot.to_dict()["delivery_channel"] == "NONE"

    connection = _Stage3BConnection(current_formula)
    first_snapshot, first_decision = (
        research_formula_store._persist_shadow_evidence_and_relevance(
            connection, formula=current_formula, validation=validation
        )
    )
    duplicate_snapshot, duplicate_decision = (
        research_formula_store._persist_shadow_evidence_and_relevance(
            connection, formula=current_formula, validation=validation
        )
    )
    assert first_snapshot == duplicate_snapshot
    assert first_decision["state"] == relevance.RELEVANT
    assert duplicate_decision["duplicate_observation"] is True
    assert len(connection.snapshots) == 1
    assert len(connection.relevance) == 1

    migration = ROOT / "migrations" / "016_formula_relevance_hysteresis_v1.sql"
    migration_text = migration.read_text(encoding="utf-8")
    assert research_formula_schema_admin.MIGRATION_PATHS[-1] == migration
    assert "CREATE TABLE IF NOT EXISTS research_formula_relevance_assessments" in migration_text
    assert "research_formula_relevance_assessments is append-only" in migration_text
    assert "trg_formula_relevance_assessments_no_truncate" in migration_text
    assert "delivery_channel' = 'NONE" in migration_text
    assert "live_effect' = 'NONE" in migration_text

    store_source = inspect.getsource(research_formula_store.evaluate_shadow_readiness)
    assert "_persist_shadow_evidence_and_relevance" in store_source
    assert "current_stage='SHADOW'" in store_source
    assert "SET current_stage" not in store_source
    assert "SET live_alert_approved" not in store_source
    helper_source = inspect.getsource(
        research_formula_store._persist_shadow_evidence_and_relevance
    )
    assert "send_message" not in helper_source
    assert "telegram" not in helper_source.lower()
    worker_status = research_formula_worker.WORKER.status()
    assert worker_status["relevance_hysteresis"] == descriptor

    print("research_formula_relevance_selftest: PASS")


if __name__ == "__main__":
    run()
