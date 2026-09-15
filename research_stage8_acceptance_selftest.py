from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
from unittest.mock import patch
import unittest

import research_btc_parent_movement as btc_parent
from research_operational_score_source_audit_selftest import _anchor, _snapshot
import research_stage8_acceptance as acceptance
import research_stage8_contract as contract
import research_stage8_feature_projection as projection
from research_stage8_feature_projection_selftest import (
    _fresh, _selection_attestation, _set_model,
)


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _parent_start(number: int) -> datetime:
    return datetime(2026, 9, 13, tzinfo=timezone.utc) + timedelta(minutes=number + 1)


def _parent(number: int) -> str:
    return btc_parent._identity(_parent_start(number))


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(
        timespec="microseconds"
    ).replace("+00:00", "Z")


class Stage8AcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.binding = contract.exact_binding(
            scope_id="BINANCE_BTC",
            candidate_id="POSITIONING_ALIGNED65_LONG",
            threshold_bps=50,
        )
        self.identity = acceptance.representative_binding(self.binding)
        self.frozen_at = "2026-09-13T00:00:00.000000Z"
        self.freeze_id = _sha("stage8-test-freeze")
        self.registry_record_sha256 = _sha("stage8-test-durable-registry-record")
        self.registry_verification_receipt_sha256 = _sha("stage8-test-registry-read-receipt")
        self.cohort_query_sha256 = _sha("stage8-test-fixed-query-and-filters")
        self.population_receipt_sha256 = _sha("stage8-test-complete-source-population")
        self.source_high_water_attempt_id = 10000

    def row(self, number: int, *, label: str | None = "SUCCESS",
            window: tuple[float, float] | None = (2.0, 1.0)) -> dict:
        parent_start = _parent_start(number)
        representative = {
            "selection_fact_identity_sha256": _sha(
                f"selection-fact-identity-{number}"
            ),
            "attempt_fingerprint": _sha(f"attempt-{number}"),
            "anchor_slot_id": 1000 + number,
            "event_id": 2000 + number,
            "event_fingerprint": _sha(f"event-{number}"),
            "symbol": "BTC", "direction": "LONG",
            "decision_time_utc": _iso(parent_start + timedelta(seconds=30)),
            "candidate_match_knowledge_status": "KNOWN", "candidate_match": True,
        }
        row = {
            "binding": deepcopy(self.identity),
            "btc_parent_movement_id": _parent(number),
            "representative_status": "VALID",
            "parent_policy_version": btc_parent.POLICY_VERSION,
            "membership_status": "LIVE", "parent_evidence_eligible": True,
            "freeze_id": self.freeze_id,
            "registry_record_sha256": self.registry_record_sha256,
            "registry_verification_receipt_sha256": self.registry_verification_receipt_sha256,
            "parent_start_time_utc": _iso(parent_start),
            "representative": representative,
            "delivery_status": "NOT_APPLICABLE",
        }
        representative_sha256 = acceptance.representative_identity_sha256(self.binding, row)
        row["representative_identity_sha256"] = representative_sha256
        evidence_identity = {
            "exact_binding_sha256": self.binding["binding_sha256"],
            "btc_parent_movement_id": row["btc_parent_movement_id"],
            "representative_identity_sha256": representative_sha256,
            "event_id": representative["event_id"],
            "selection_fact_identity_sha256": representative[
                "selection_fact_identity_sha256"
            ],
            "direction": "LONG",
        }
        if label is not None:
            row["probability_evidence"] = {
                "validation_status": "VALID", **evidence_identity,
                "method_version": "ordered-first-touch-v7",
                "reported_status": label, "window_minutes": 60, "threshold_bps": 50,
            }
        if window is not None:
            row["asymmetry_evidence"] = {
                "validation_status": "VALID", **evidence_identity,
                "method_version": "common-window-spot-1m-v1",
                "measurement_kind": "FIXED_WINDOW", "reported_status": "READY",
                "window_minutes": 60, "scope_price_route": "BINANCE_SPOT_1M",
                "path_complete": True, "observation_closed": True,
                "coverage_complete": True, "mfe_pct": window[0], "mae_pct": window[1],
            }
        return row

    def prepare(self, rows):
        prepared = deepcopy(rows)
        selection = acceptance.bind_registry_selection_receipt(
            self.binding, prepared, freeze_id=self.freeze_id,
            frozen_at_utc=self.frozen_at,
            registry_record_sha256=self.registry_record_sha256,
            registry_verification_receipt_sha256=self.registry_verification_receipt_sha256,
            cohort_query_sha256=self.cohort_query_sha256,
            population_receipt_sha256=self.population_receipt_sha256,
            source_high_water_attempt_id=self.source_high_water_attempt_id,
        )
        for row in prepared:
            row["selection_attestation_sha256"] = selection["attestation_sha256"]
        return prepared, selection

    def evaluate(self, rows, **kwargs):
        prepared, selection = self.prepare(rows)
        return acceptance.evaluate(
            self.binding, prepared, selection_provenance=selection, **kwargs
        )

    def assert_atomic_pass(self, result):
        self.assertTrue(result["atomic_gate_passed"], result)
        self.assertFalse(result["research_qualified"], result)
        self.assertEqual(result["status"], "AWAITING_DURABLE_REGISTRY_VERIFICATION")

    def test_exact_frozen_manifest_policy_is_compatible(self):
        policy = acceptance.validate_policy_compatibility()
        self.assertEqual(policy["version"], contract.ACCEPTANCE_VERSION)
        self.assertEqual(policy["minimum_distinct_parents_per_passing_route"], 5)
        self.assertEqual(policy["combination"], "PROBABILITY_OR_ASYMMETRY")
        self.assertFalse(policy["fresh_three_parent_route"])

    def test_probability_only_passes_atomic_gate_without_delivery_or_asymmetry(self):
        result = self.evaluate([self.row(number, window=None) for number in range(5)])
        self.assert_atomic_pass(result)
        self.assertEqual(result["routes"]["PROBABILITY"]["status"], "PASS")
        self.assertEqual(result["routes"]["ASYMMETRY"]["status"], "UNAVAILABLE")
        self.assertFalse(result["delivery_status_required"])

    def test_asymmetry_only_passes_atomic_gate_when_probability_unavailable(self):
        result = self.evaluate([self.row(number, label=None) for number in range(5)])
        self.assert_atomic_pass(result)
        self.assertEqual(result["routes"]["PROBABILITY"]["status"], "UNAVAILABLE")
        self.assertEqual(result["routes"]["ASYMMETRY"]["status"], "PASS")

    def test_fabricated_hashes_never_yield_research_qualification(self):
        result = self.evaluate([self.row(number, window=None) for number in range(5)])
        self.assertTrue(result["atomic_gate_passed"])
        self.assertFalse(result["research_qualified"])
        self.assertIn("DURABLE_REGISTRY_PERSISTENCE_NOT_VERIFIED",
                      result["qualification_blockers"])
        self.assertFalse(result["registry_selection_receipt"]
                         ["persistence_verified_by_evaluator"])

    def test_five_rows_from_one_parent_fail_without_internal_selection(self):
        result = self.evaluate([self.row(7) for _ in range(5)])
        self.assertFalse(result["atomic_gate_passed"])
        self.assertIn("DUPLICATE_BTC_PARENT_MOVEMENT_ID", result["common"]["blockers"])
        self.assertEqual(result["common"]["duplicate_btc_parent_movement_ids"], [_parent(7)])

    def test_four_distinct_parents_fail_both_route_sample_floors(self):
        result = self.evaluate([self.row(number) for number in range(4)])
        self.assertFalse(result["atomic_gate_passed"])
        self.assertEqual(result["routes"]["PROBABILITY"]["status"], "INSUFFICIENT")
        self.assertEqual(result["routes"]["ASYMMETRY"]["status"], "INSUFFICIENT")

    def test_neither_route_passing_fails_atomic_gate(self):
        result = self.evaluate([self.row(n, label="FAILURE", window=(0.5, 1.0))
                                for n in range(5)])
        self.assertFalse(result["atomic_gate_passed"])
        self.assertEqual(result["routes"]["PROBABILITY"]["status"], "FAIL")
        self.assertEqual(result["routes"]["ASYMMETRY"]["status"], "FAIL")

    def test_hit_rate_without_wilson_floor_does_not_pass_probability(self):
        rows = [self.row(n, label="SUCCESS", window=None) for n in range(5)]
        rows += [self.row(n, label="FAILURE", window=None) for n in range(5, 7)]
        route = self.evaluate(rows)["routes"]["PROBABILITY"]
        self.assertGreaterEqual(route["hit_rate_pct"], 70)
        self.assertLess(route["wilson_95_lower_pct"], 40)
        self.assertFalse(route["passed"])

    def test_module_constant_monkeypatch_cannot_weaken_frozen_policy(self):
        rows = [self.row(n, label="FAILURE", window=None) for n in range(5)]
        with patch.object(acceptance, "PROBABILITY_HIT_RATE_FLOOR_PCT", 0.0), \
             patch.object(acceptance, "PROBABILITY_WILSON_FLOOR_PCT", 0.0), \
             patch.object(acceptance, "MINIMUM_DISTINCT_PARENTS", 0):
            result = self.evaluate(rows)
        self.assertFalse(result["routes"]["PROBABILITY"]["passed"], result)
        self.assertFalse(result["atomic_gate_passed"])

    def test_asymmetry_exact_ratio_and_dominance_boundaries_pass(self):
        pairs = [(2.0, 1.0)] * 3 + [(0.75, 1.0)] * 2
        route = self.evaluate([self.row(n, label=None, window=pair)
                               for n, pair in enumerate(pairs)])["routes"]["ASYMMETRY"]
        self.assertTrue(route["passed"], route)
        self.assertEqual(route["common_window_asymmetry_ratio"], 1.5)
        self.assertEqual(route["common_window_favorable_dominance_pct"], 60.0)

    def test_large_ratio_cannot_hide_failed_dominance_and_edge(self):
        pairs = [(10.0, 1.0)] * 2 + [(0.0, 1.0)] * 3
        route = self.evaluate([self.row(n, label=None, window=pair)
                               for n, pair in enumerate(pairs)])["routes"]["ASYMMETRY"]
        self.assertGreater(route["common_window_asymmetry_ratio"], 1.5)
        self.assertEqual(route["common_window_favorable_dominance_pct"], 40.0)
        self.assertLess(route["common_window_median_paired_edge_pct"], 0.0)
        self.assertFalse(route["passed"])

    def test_invalid_missing_and_noncanonical_parents_are_shared_blockers(self):
        rows = [self.row(n, window=None) for n in range(5)]
        invalid, missing, noncanonical = self.row(50), self.row(51), self.row(52)
        invalid["btc_parent_movement_id"] = "not-a-parent"
        missing.pop("btc_parent_movement_id")
        noncanonical["btc_parent_movement_id"] = _sha("wrong-parent-start")
        result = self.evaluate(rows + [invalid, missing, noncanonical])
        self.assertFalse(result["atomic_gate_passed"])
        self.assertTrue(result["routes"]["PROBABILITY"]["passed"])
        self.assertIn("SHARED_REPRESENTATIVE_PROVENANCE_INVALID",
                      result["common"]["blockers"])

    def test_ambiguous_missing_open_and_unresolved_labels_are_not_failures(self):
        labels = ["SUCCESS", "SUCCESS", "SUCCESS", "AMBIGUOUS", "DATA_MISSING",
                  "OPEN", "UNRESOLVED"]
        result = self.evaluate([self.row(n, label=label, window=None)
                                for n, label in enumerate(labels)])
        route = result["routes"]["PROBABILITY"]
        self.assertEqual(route["distinct_parent_count"], 3)
        self.assertEqual(route["successes"], 3)
        self.assertEqual(route["failures"], 0)
        self.assertEqual(sum(route["exclusions"].values()), 4)

    def test_route_specific_parent_sets_are_not_borrowed(self):
        rows = [self.row(n, window=None) for n in range(3)]
        rows += [self.row(n, label=None) for n in range(3, 6)]
        result = self.evaluate(rows)
        self.assertEqual(result["routes"]["PROBABILITY"]["distinct_parent_count"], 3)
        self.assertEqual(result["routes"]["ASYMMETRY"]["distinct_parent_count"], 3)
        self.assertFalse(result["atomic_gate_passed"])

    def test_zero_mae_is_not_promoted_to_infinite_asymmetry(self):
        route = self.evaluate([self.row(n, label=None, window=(2.0, 0.0))
                               for n in range(5)])["routes"]["ASYMMETRY"]
        self.assertEqual(route["status"], "UNAVAILABLE")
        self.assertEqual(route["common_window_asymmetry_state"], "ZERO_DENOMINATOR")
        self.assertIsNone(route["common_window_asymmetry_ratio"])

    def test_invalid_numeric_inputs_and_aggregate_overflow_never_pass(self):
        bad = [float("nan"), float("inf"), "2.0", True, -1.0, 10 ** 10000]
        rows = [self.row(n, label=None) for n in range(len(bad))]
        for row, value in zip(rows, bad):
            row["asymmetry_evidence"]["mfe_pct"] = value
        self.assertEqual(self.evaluate(rows)["routes"]["ASYMMETRY"]
                         ["distinct_parent_count"], 0)
        overflow = self.evaluate([self.row(n, label=None, window=(1.7e308, 1.0))
                                  for n in range(5)])["routes"]["ASYMMETRY"]
        self.assertEqual(overflow["status"], "UNAVAILABLE")
        self.assertIsNone(overflow["common_window_asymmetry_ratio"])

    def test_policy_mismatch_fails_closed_using_frozen_diagnostics(self):
        policy = contract.frozen_manifest()["acceptance"]
        policy["probability"]["hit_rate_pct_gte"] = 0
        rows = [self.row(n, label="FAILURE", window=None) for n in range(5)]
        result = self.evaluate(rows, acceptance_policy=policy)
        self.assertIn("ACCEPTANCE_POLICY_MISMATCH", result["common"]["blockers"])
        self.assertFalse(result["routes"]["PROBABILITY"]["passed"])

    def test_selection_receipt_cannot_be_reused_for_cherry_picked_subset(self):
        rows = [self.row(n, label="SUCCESS", window=None) for n in range(5)]
        rows += [self.row(n, label="FAILURE", window=None) for n in range(5, 10)]
        prepared, selection = self.prepare(rows)
        full = acceptance.evaluate(self.binding, prepared, selection_provenance=selection)
        self.assertFalse(full["routes"]["PROBABILITY"]["passed"])
        cherry = acceptance.evaluate(self.binding, prepared[:5],
                                     selection_provenance=selection)
        self.assertFalse(cherry["atomic_gate_passed"])
        self.assertIn("REGISTRY_SELECTION_ATTESTATION_INVALID",
                      cherry["common"]["blockers"])
        self.assertEqual(selection["representative_count"], 10)

    def test_replay_downgrade_preserves_selection_but_excludes_that_parent(self):
        prepared, selection = self.prepare([
            self.row(n, label="SUCCESS", window=None) for n in range(5)
        ])
        prepared[0]["representative_status"] = "UNKNOWN"
        result = acceptance.evaluate(
            self.binding, prepared, selection_provenance=selection,
        )
        self.assertTrue(result["common"]["selection_provenance_complete"])
        self.assertEqual(result["common"]["blockers"], [
            "SHARED_REPRESENTATIVE_PROVENANCE_INVALID",
        ])
        self.assertEqual(
            result["routes"]["PROBABILITY"]["distinct_parent_count"], 4,
        )
        self.assertFalse(result["atomic_gate_passed"])

    def test_selection_digest_is_outcome_blind_but_event_set_sensitive(self):
        prepared, selection = self.prepare([self.row(n, window=None) for n in range(5)])
        changed_label = deepcopy(prepared)
        changed_label[0]["probability_evidence"]["reported_status"] = "FAILURE"
        self.assertEqual(acceptance._representative_set(self.binding, prepared),
                         acceptance._representative_set(self.binding, changed_label))
        changed_event = deepcopy(prepared)
        changed_event[0]["representative"]["event_id"] += 100
        self.assertNotEqual(acceptance._representative_set(self.binding, prepared),
                            acceptance._representative_set(self.binding, changed_event))
        result = acceptance.evaluate(self.binding, changed_event,
                                     selection_provenance=selection)
        self.assertIn("REGISTRY_SELECTION_ATTESTATION_INVALID",
                      result["common"]["blockers"])

    def test_probability_record_cannot_substitute_event_in_same_parent(self):
        prepared, selection = self.prepare([self.row(n, window=None) for n in range(5)])
        prepared[0]["probability_evidence"]["event_id"] += 999
        result = acceptance.evaluate(self.binding, prepared,
                                     selection_provenance=selection)
        self.assertTrue(result["common"]["passed"], result)
        self.assertEqual(result["routes"]["PROBABILITY"]["distinct_parent_count"], 4)
        self.assertIn("PROBABILITY_REPRESENTATIVE_EVENT_MISMATCH",
                      result["routes"]["PROBABILITY"]["exclusions"])

    def test_asymmetry_record_cannot_substitute_selection_fact_identity(self):
        prepared, selection = self.prepare([self.row(n, label=None) for n in range(5)])
        prepared[0]["asymmetry_evidence"]["selection_fact_identity_sha256"] = _sha(
            "other-selection-fact-identity"
        )
        result = acceptance.evaluate(self.binding, prepared,
                                     selection_provenance=selection)
        self.assertTrue(result["common"]["passed"], result)
        self.assertEqual(result["routes"]["ASYMMETRY"]["distinct_parent_count"], 4)

    def test_no_registry_selection_receipt_cannot_pass_atomic_gate(self):
        result = acceptance.evaluate(self.binding,
                                     [self.row(n, window=None) for n in range(5)],
                                     selection_provenance=None)
        self.assertFalse(result["atomic_gate_passed"])
        self.assertIn("REGISTRY_SELECTION_ATTESTATION_INVALID",
                      result["common"]["blockers"])

    def test_public_receipt_helper_does_not_claim_db_verification(self):
        prepared, selection = self.prepare([self.row(n, window=None) for n in range(5)])
        result = acceptance.evaluate(self.binding, prepared,
                                     selection_provenance=selection)
        self.assertEqual(selection["registration_evidence"],
                         "CALLER_SUPPLIED_REGISTRY_REFERENCES_NOT_DB_VERIFIED")
        receipt = result["registry_selection_receipt"]
        self.assertFalse(receipt["persistence_verified_by_evaluator"])

    def test_real_projected_fact_timestamp_and_identity_feed_representative(self):
        attempt, slot, events = _anchor()
        snapshot = _set_model(_fresh(_snapshot()))
        binding = contract.exact_binding(
            scope_id="BINANCE_BTC",
            candidate_id="FUTURES_FLOW_ALIGNED65_SHORT",
            threshold_bps=50,
        )
        watch_selection_attestation = _selection_attestation(attempt, snapshot)
        fact = projection.project_binding_fact(
            attempt=attempt, anchor_slot=slot, anchor_events=events,
            selected_watch_snapshot=snapshot,
            watch_selection_attestation=watch_selection_attestation,
            binding=binding,
        )
        # Simulate the three values read independently from the durable freeze;
        # validate_fact deliberately does not trust hashes carried only by fact.
        frozen_fact_sha256 = fact["fact_sha256"]
        projection.validate_fact(
            fact,
            expected_fact_sha256=frozen_fact_sha256,
            expected_watch_selection_attestation_sha256=
            watch_selection_attestation["attestation_sha256"],
            expected_watch_code_manifest_sha256=
            projection._watch_code_manifest(snapshot)[1],
        )
        self.assertEqual(fact["knowledge_status"], "KNOWN", fact)
        self.assertTrue(fact["candidate_match"])
        decision = datetime.fromisoformat(
            fact["identity"]["decision_time_utc"].replace("Z", "+00:00")
        )
        parent_start = decision - timedelta(seconds=30)
        identity = acceptance.representative_binding(binding)
        row = {
            "binding": identity,
            "btc_parent_movement_id": btc_parent._identity(parent_start),
            "representative_status": "VALID",
            "parent_policy_version": btc_parent.POLICY_VERSION,
            "membership_status": "LIVE", "parent_evidence_eligible": True,
            "freeze_id": self.freeze_id,
            "registry_record_sha256": self.registry_record_sha256,
            "registry_verification_receipt_sha256": self.registry_verification_receipt_sha256,
            "parent_start_time_utc": _iso(parent_start),
            "representative": {
                # In production this value is computed by the durable fact
                # trigger from its closed structural payload.  The pure
                # acceptance layer consumes only the independently supplied
                # digest and never the free-form full-fact hash.
                "selection_fact_identity_sha256": _sha(
                    "projected-fixture-selection-fact-identity"
                ),
                "attempt_fingerprint": fact["identity"]["attempt_fingerprint"],
                "anchor_slot_id": fact["identity"]["anchor_slot_id"],
                "event_id": fact["identity"]["event_id"],
                "event_fingerprint": fact["identity"]["event_fingerprint"],
                "symbol": fact["identity"]["symbol"],
                "direction": fact["identity"]["direction"],
                "decision_time_utc": fact["identity"]["decision_time_utc"],
                "candidate_match_knowledge_status": fact["knowledge_status"],
                "candidate_match": fact["candidate_match"],
            },
        }
        row["representative_identity_sha256"] = (
            acceptance.representative_identity_sha256(binding, row)
        )
        selection = acceptance.bind_registry_selection_receipt(
            binding, [row], freeze_id=self.freeze_id,
            frozen_at_utc="2026-08-28T00:00:00.000000Z",
            registry_record_sha256=self.registry_record_sha256,
            registry_verification_receipt_sha256=self.registry_verification_receipt_sha256,
            cohort_query_sha256=self.cohort_query_sha256,
            population_receipt_sha256=self.population_receipt_sha256,
            source_high_water_attempt_id=self.source_high_water_attempt_id,
        )
        row["selection_attestation_sha256"] = selection["attestation_sha256"]
        result = acceptance.evaluate(binding, [row], selection_provenance=selection)
        self.assertTrue(result["common"]["passed"], result)
        self.assertEqual(result["excluded_representatives"], [])
        self.assertEqual(result["routes"]["PROBABILITY"]["status"], "UNAVAILABLE")

    def test_parent_must_start_strictly_after_freeze(self):
        rows = [self.row(n, window=None) for n in range(5)]
        rows[0]["parent_start_time_utc"] = self.frozen_at
        result = self.evaluate(rows)
        self.assertFalse(result["atomic_gate_passed"])
        self.assertIn("SHARED_REPRESENTATIVE_PROVENANCE_INVALID",
                      result["common"]["blockers"])

    def test_incomplete_population_receipt_blocks_both_routes(self):
        prepared, selection = self.prepare([self.row(n) for n in range(5)])
        selection["population_coverage_complete"] = False
        result = acceptance.evaluate(self.binding, prepared,
                                     selection_provenance=selection)
        self.assertFalse(result["atomic_gate_passed"])
        self.assertIn("REGISTRY_SELECTION_ATTESTATION_INVALID",
                      result["common"]["blockers"])

    def test_row_binding_ties_scope_candidate_direction_cell_and_source(self):
        for key in ("scope_id", "scope_symbols", "scope_price_route", "candidate_id",
                    "candidate_model", "direction", "window_minutes", "threshold_bps",
                    "source_version", "projection_version", "label_version",
                    "independence_version", "acceptance_version", "parent_policy_version"):
            rows = [self.row(n) for n in range(5)]
            current = rows[0]["binding"][key]
            rows[0]["binding"][key] = ["wrong"] if isinstance(current, list) else "wrong"
            result = self.evaluate(rows)
            self.assertIn("REPRESENTATIVE_BINDING_MISMATCH",
                          result["common"]["blockers"], key)


if __name__ == "__main__":
    unittest.main()
