"""Deterministic regressions for the shared Formula Evidence envelope."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import research_evidence_contract as contract
import research_feature_matrix
import research_formula_acceptance
import research_formula_engine
import research_formula_families
import research_formula_schema_admin
import research_formula_store
import research_market_episode


ROOT = Path(__file__).resolve().parent
FIXTURES = ROOT / "fixtures" / "evidence"


def _fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _snapshot(fixture: dict) -> tuple[contract.FormulaAssessment, contract.EvidenceSnapshot]:
    assessment = contract.FormulaAssessment.from_acceptance(
        fixture["assessment"], phase=fixture["phase"]
    )
    snapshot = contract.EvidenceSnapshot.build(
        formula_contract=fixture["formula_contract"],
        assessment=assessment,
        assessed_at_utc=fixture["assessed_at_utc"],
        formula_family_id=fixture.get("formula_family_id"),
        matched_market_episode_ids=fixture["matched_market_episode_ids"],
        control_market_episode_ids=fixture["control_market_episode_ids"],
        matched_parent_market_episode_ids=(
            fixture["matched_parent_market_episode_ids"]
        ),
        control_parent_market_episode_ids=(
            fixture["control_parent_market_episode_ids"]
        ),
        raw_match_count=fixture["raw_match_count"],
        raw_control_count=fixture["raw_control_count"],
        matched_n_eff=fixture["matched_n_eff"],
        control_n_eff=fixture["control_n_eff"],
        metrics=fixture["metrics"],
        evidence=fixture["evidence"],
        provenance=fixture["provenance"],
    )
    return assessment, snapshot


def _raises(expected: str, function) -> None:
    try:
        function()
    except ValueError as exc:
        assert expected in str(exc), exc
    else:
        raise AssertionError(f"expected ValueError containing {expected!r}")


class _Cursor:
    def __init__(self, row=None):
        self._row = row

    def fetchone(self):
        return self._row


class _SnapshotConnection:
    def __init__(self, formula: dict):
        self.formula = dict(formula)
        self.stored = None
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query, params=()):
        compact = " ".join(str(query).split())
        if "FROM research_formulas" in compact:
            return _Cursor(dict(self.formula))
        if compact.startswith("INSERT INTO research_formula_evidence_snapshots"):
            payload = json.loads(params[21])
            candidate = {
                "formula_id": int(params[5]),
                "source_run_id": params[6],
                "snapshot_payload": payload,
            }
            if self.stored is None:
                self.stored = candidate
            return _Cursor()
        if "FROM research_formula_evidence_snapshots" in compact:
            return _Cursor(None if self.stored is None else dict(self.stored))
        raise AssertionError(f"unexpected SQL: {compact}")

    def commit(self):
        self.commits += 1


def run() -> None:
    assert contract.CURRENT_FORMULA_SCHEMA_VERSION == research_formula_engine.FORMULA_SCHEMA_VERSION
    assert contract.LEGACY_V6_FORMULA_SCHEMA_VERSION == (
        research_formula_engine.LEGACY_V6_FORMULA_SCHEMA_VERSION
    )
    assert contract.LEGACY_V5_FORMULA_SCHEMA_VERSION == (
        research_formula_store._LEGACY_V5_FORMULA_SCHEMA_VERSION
    )
    assert contract.CURRENT_ENGINE_VERSION == research_formula_engine.ENGINE_VERSION
    assert contract.LEGACY_V6_ENGINE_VERSION == (
        research_formula_engine.LEGACY_V6_ENGINE_VERSION
    )
    assert contract.LEGACY_V5_ENGINE_VERSION == (
        research_formula_store._LEGACY_V5_ENGINE_VERSION
    )
    assert contract.CURRENT_FEATURE_SCHEMA_VERSION == (
        research_feature_matrix.FEATURE_SCHEMA_VERSION
    )
    assert contract.LEGACY_V5_FEATURE_SCHEMA_VERSION == (
        research_formula_store._LEGACY_V5_FEATURE_SCHEMA_VERSION
    )
    assert contract.CURRENT_OUTCOME_METHOD_VERSION == (
        research_feature_matrix.VERIFIED_OUTCOME_METHOD
    )
    assert contract.LEGACY_V5_OUTCOME_METHOD_VERSION == (
        research_formula_store._LEGACY_V5_OUTCOME_METHOD_VERSION
    )
    assert research_formula_acceptance.POLICY_VERSION == (
        "research-acceptance-v1-probability-or-asymmetry"
    )

    current_fixture = _fixture("current_v7_probability.json")
    current_assessment, current = _snapshot(current_fixture)
    expected = current_fixture["expected"]
    assert current.snapshot_id == (
        "7dcbea1191f423a6a64a756830621def7ef8a4cd27e9ace400ccf7475cf2333f"
    )
    assert current.snapshot_id == expected["snapshot_id"]
    assert current_assessment.assessment_id == expected["assessment_id"]
    assert current.compatibility == expected["compatibility"]
    assert current.runtime_compatibility == contract.CURRENT_V7
    assert current.assessment.research_ready is expected["research_ready"]
    assert current.assessment.accepted_paths == ("PROBABILITY",)
    assert current.to_dict()["live_eligible"] is False
    assert current.to_dict()["delivery_channel"] == "NONE"
    assert current.to_dict()["formula_family_id"] == current_fixture["formula_family_id"]

    # Equivalent order and timezone representations produce the same content id.
    reordered = deepcopy(current_fixture)
    reordered["assessment"] = dict(reversed(list(reordered["assessment"].items())))
    reordered["metrics"] = dict(reversed(list(reordered["metrics"].items())))
    for key in (
        "matched_market_episode_ids",
        "control_market_episode_ids",
        "matched_parent_market_episode_ids",
        "control_parent_market_episode_ids",
    ):
        reordered[key] = list(reversed(reordered[key]))
    reordered["assessed_at_utc"] = "2026-08-30T23:00:00+02:00"
    reordered_assessment, reordered_snapshot = _snapshot(reordered)
    assert reordered_assessment.assessment_id == current_assessment.assessment_id
    assert reordered_snapshot.snapshot_id == current.snapshot_id
    assert reordered_snapshot.to_dict() == current.to_dict()

    round_trip = contract.EvidenceSnapshot.from_dict(current.to_dict())
    assert round_trip == current
    assert contract.interpret_snapshot(round_trip).to_dict() == (
        contract.interpret_snapshot(current.to_dict()).to_dict()
    )

    retained_v7_1_fixture = _fixture("retained_v7_1_probability.json")
    retained_v7_1_assessment, retained_v7_1 = _snapshot(retained_v7_1_fixture)
    assert retained_v7_1.snapshot_id == (
        "b7ced230183f1d6e4db3d3f9ceb36ed97b57c4631d12dc7f184cb2f273bf73b0"
    )
    assert retained_v7_1.snapshot_id == (
        retained_v7_1_fixture["expected"]["snapshot_id"]
    )
    assert retained_v7_1_assessment.assessment_id == current_assessment.assessment_id
    assert retained_v7_1.compatibility == contract.CURRENT_V7
    assert retained_v7_1.runtime_compatibility == (
        contract.RETAINED_V7_1_READ_ONLY
    )
    assert retained_v7_1.snapshot_id != current.snapshot_id
    assert (
        contract.EvidenceSnapshot.from_dict(retained_v7_1.to_dict())
        == retained_v7_1
    )

    detached = current.to_dict()
    detached["metrics"]["sample_size"] = 999
    assert current.to_dict()["metrics"]["sample_size"] == 2
    _raises("fingerprint mismatch", lambda: contract.EvidenceSnapshot.from_dict(detached))

    unknown_field = current.to_dict()
    unknown_field["renderer_guess"] = True
    _raises(
        "non-canonical or unknown fields",
        lambda: contract.EvidenceSnapshot.from_dict(unknown_field),
    )

    missing_family = deepcopy(current_fixture)
    missing_family.pop("formula_family_id")
    _raises("require formula_family_id", lambda: _snapshot(missing_family))

    excessive_weight = deepcopy(current_fixture)
    excessive_weight["matched_n_eff"] = 2.0
    _raises("matched_n_eff exceeds", lambda: _snapshot(excessive_weight))

    legacy_fixture = _fixture("legacy_v6_shadow.json")
    legacy_assessment, legacy = _snapshot(legacy_fixture)
    legacy_expected = legacy_fixture["expected"]
    assert legacy.snapshot_id == legacy_expected["snapshot_id"]
    assert legacy_assessment.assessment_id == legacy_expected["assessment_id"]
    assert legacy.formula_family_id == legacy_expected["formula_family_id"]
    assert legacy.compatibility == legacy_expected["compatibility"]
    assert legacy.assessment.research_ready is legacy_expected["research_ready"]
    assert legacy.to_dict()["legacy_adapter_version"] == contract.LEGACY_ADAPTER_VERSION
    assert contract.EvidenceSnapshot.from_dict(legacy.to_dict()) == legacy

    retained_v5 = deepcopy(legacy_fixture)
    retained_v5["formula_contract"].update(
        {
            "formula_key": "7777777777777777777777777777777777777777777777777777777777777777",
            "formula_schema_version": contract.LEGACY_V5_FORMULA_SCHEMA_VERSION,
            "engine_version": research_formula_store._LEGACY_V5_ENGINE_VERSION,
            "feature_schema_version": (
                research_formula_store._LEGACY_V5_FEATURE_SCHEMA_VERSION
            ),
            "outcome_method_version": (
                research_formula_store._LEGACY_V5_OUTCOME_METHOD_VERSION
            ),
        }
    )
    _, v5_snapshot = _snapshot(retained_v5)
    assert v5_snapshot.compatibility == contract.LEGACY_SHADOW_READ_ONLY
    assert v5_snapshot.assessment.research_ready is False
    assert v5_snapshot.formula_family_id != legacy.formula_family_id

    legacy_ready = deepcopy(legacy_fixture)
    legacy_ready["assessment"].update(
        {
            "research_ready": True,
            "accepted_paths": ["PROBABILITY"],
            "maturity": "RESEARCH_READY",
        }
    )
    _raises("must remain research-not-ready", lambda: _snapshot(legacy_ready))

    unsupported = deepcopy(legacy_fixture)
    unsupported["formula_contract"]["formula_schema_version"] = "invented-v99"
    _raises("unsupported formula schema", lambda: _snapshot(unsupported))

    mismatched_runtime = deepcopy(current_fixture)
    mismatched_runtime["formula_contract"]["outcome_method_version"] = (
        contract.LEGACY_V5_OUTCOME_METHOD_VERSION
    )
    _raises("runtime versions do not match", lambda: _snapshot(mismatched_runtime))

    invented_runtime = deepcopy(current_fixture)
    invented_runtime["formula_contract"]["engine_version"] = (
        "formula-discovery-v7.3-invented"
    )
    _raises("runtime versions do not match", lambda: _snapshot(invented_runtime))

    descriptor = contract.contract_descriptor()
    assert descriptor == research_formula_worker_descriptor()
    assert descriptor["current_runtime"]["engine_version"] == (
        research_formula_engine.ENGINE_VERSION
    )
    assert descriptor["retained_read_only_runtimes"] == [
        {
            "formula_schema_version": contract.CURRENT_FORMULA_SCHEMA_VERSION,
            "engine_version": contract.RETAINED_V7_1_ENGINE_VERSION,
            "feature_schema_version": contract.CURRENT_FEATURE_SCHEMA_VERSION,
            "outcome_method_version": contract.CURRENT_OUTCOME_METHOD_VERSION,
            "runtime_compatibility": contract.RETAINED_V7_1_READ_ONLY,
        }
    ]
    assert descriptor["live_effect"] == "NONE"
    assert descriptor["delivery_channel"] == "NONE"

    migration = ROOT / "migrations" / "015_formula_evidence_snapshots_v1.sql"
    migration_text = migration.read_text(encoding="utf-8")
    assert migration in research_formula_schema_admin.MIGRATION_PATHS
    assert "CREATE TABLE IF NOT EXISTS research_formula_evidence_snapshots" in migration_text
    assert "research_formula_evidence_snapshots is append-only" in migration_text
    assert "snapshot_payload -> 'live_eligible' = 'false'::jsonb" in migration_text
    assert "snapshot_payload ->> 'delivery_channel' = 'NONE'" in migration_text

    # The public store remains idempotent; Stage 3B uses the same transactional
    # helper when a distinct rolling relevance observation is persisted.
    store_source = (ROOT / "research_formula_store.py").read_text(encoding="utf-8")
    assert store_source.count("persist_evidence_snapshot(") == 1
    fake = _SnapshotConnection(current_fixture["formula_contract"])
    original_connect = research_formula_store._connect
    research_formula_store._connect = lambda *, read_only=False: fake
    try:
        first = research_formula_store.persist_evidence_snapshot(
            current, formula_id=42, source_run_id=17
        )
        second = research_formula_store.persist_evidence_snapshot(
            current.to_dict(), formula_id=42, source_run_id=17
        )
        loaded = research_formula_store.load_evidence_snapshot(current.snapshot_id)
    finally:
        research_formula_store._connect = original_connect
    assert first == second
    assert first["live_effect"] == "NONE"
    assert fake.commits == 2
    assert loaded == current

    print("research_evidence_contract_selftest: ok")


def research_formula_worker_descriptor() -> dict:
    # Import late so the contract self-test remains focused and avoids hiding
    # circular-import regressions at module import time.
    import research_formula_worker

    return research_formula_worker.WORKER.status()["evidence_contract"]


if __name__ == "__main__":
    run()
