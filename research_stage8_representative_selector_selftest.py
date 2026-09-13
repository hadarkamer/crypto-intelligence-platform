"""Adversarial, network-free checks for the Stage-8 representative selector."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import patch

import research_btc_parent_movement as btc_parent
import research_operational_score_source_audit_selftest as source_fixtures
import research_stage8_contract as contract
import research_stage8_coverage_receipt as coverage_receipt
import research_stage8_feature_projection as projection
import research_stage8_feature_projection_selftest as projection_fixtures
from research_stage8_feature_projection_selftest import (
    DECISION,
    _anchor,
    _fresh,
    _selection_attestation,
    _set_model,
    _snapshot,
)
import research_stage8_representative_selector as selector


UTC = timezone.utc
HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
HASH_D = "d" * 64
HASH_E = "e" * 64
HASH_F = "f" * 64


def _iso(value):
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )
    return parsed.astimezone(UTC).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _binding(scope="BINANCE_BTC"):
    return contract.exact_binding(
        scope_id=scope,
        candidate_id="FUTURES_FLOW_ALIGNED65_SHORT",
        threshold_bps=50,
    )


def _fact(*, attempt_id=1, symbol="BTC", binding=None, score=-70,
          available=True, decision=None):
    binding = binding or _binding()
    contexts = [] if decision is None else [
        patch.object(source_fixtures, "BASE", decision - timedelta(minutes=34)),
        patch.object(source_fixtures, "DECISION", decision),
        patch.object(projection_fixtures, "DECISION", decision),
    ]
    for context in contexts:
        context.start()
    try:
        attempt, slot, events = _anchor(symbol=symbol, attempt_id=attempt_id)
        snapshot = _set_model(
            _fresh(_snapshot(snapshot_id=100 + attempt_id)),
            symbol=symbol, score=score,
            direction="BEARISH" if score < 0 else "BULLISH",
            available=available,
            capture_status="AVAILABLE" if available else "UNAVAILABLE",
        )
        attestation = _selection_attestation(attempt, snapshot)
        fact = projection.project_binding_fact(
            attempt=attempt, anchor_slot=slot, anchor_events=events,
            selected_watch_snapshot=snapshot,
            watch_selection_attestation=attestation,
            binding=binding,
        )
    finally:
        for context in reversed(contexts):
            context.stop()
    event = events[1]
    return fact, event, attestation, snapshot


def _bar(decision):
    opened = decision - timedelta(minutes=1)
    return {
        "open_time_utc": opened,
        "close_time_utc": decision - timedelta(milliseconds=1),
        "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5,
        "price_source": btc_parent.SOURCE,
    }


def _parent_source(event, *, start_offset_minutes=2, eligible=True):
    decision = datetime.fromisoformat(str(event["alert_time_utc"]).replace("Z", "+00:00"))
    start = decision - timedelta(minutes=start_offset_minutes, milliseconds=1)
    bar = _bar(decision)
    parent = {
        "btc_parent_movement_id": btc_parent._identity(start),
        "episode_policy_version": btc_parent.POLICY_VERSION,
        "start_time_utc": start,
        "end_time_utc": None,
        "confirmed_at_utc": start if eligible else None,
        "direction": "DOWN" if eligible else "UNKNOWN",
        "evidence_eligible": eligible,
        "boundary_reason": "CAUSAL_CLOSE_REVERSAL" if eligible else "BTC_DATA_GAP",
        "observed_through_utc": bar["close_time_utc"],
        "price_source": btc_parent.SOURCE,
        "state_json": {"reversal_bps": btc_parent.REVERSAL_BPS},
    }
    event_minimal = {
        "event_id": event["event_id"],
        "event_fingerprint": event["event_fingerprint"],
        "alert_time_utc": event["alert_time_utc"],
        "symbol": event["symbol"],
        "direction": event["direction"],
    }
    membership = btc_parent.membership(event_minimal, parent=parent, btc_bar=bar)
    expected = "LIVE" if eligible else "BOUNDARY_UNVERIFIED"
    assert membership["membership_status"] == expected
    return {"event": event_minimal, "membership": membership,
            "parent": parent, "btc_bar": bar}


def _same_parent_source(existing, event):
    """Bind another exact direction event to the same outcome-blind BTC parent."""
    source = deepcopy(existing)
    source["event"] = {
        "event_id": event["event_id"],
        "event_fingerprint": event["event_fingerprint"],
        "alert_time_utc": event["alert_time_utc"],
        "symbol": event["symbol"],
        "direction": event["direction"],
    }
    decision = datetime.fromisoformat(
        str(event["alert_time_utc"]).replace("Z", "+00:00")
    )
    source["btc_bar"] = _bar(decision)
    source["parent"]["observed_through_utc"] = source["btc_bar"]["close_time_utc"]
    source["membership"] = btc_parent.membership(
        source["event"], parent=source["parent"], btc_bar=source["btc_bar"]
    )
    assert source["membership"]["membership_status"] == "LIVE"
    return source


def _registry(binding, *, frozen_at=None, code_hash=HASH_F):
    return {
        "status": selector.REGISTRY_STATUS,
        "exact_binding_sha256": binding["binding_sha256"],
        "manifest_sha256": contract.MANIFEST_SHA256,
        "freeze_id": HASH_A,
        "frozen_at_utc": _iso(
            frozen_at or DECISION - timedelta(minutes=10)
        ),
        "registry_record_sha256": HASH_B,
        "registry_verification_receipt_sha256": HASH_C,
        "verifier_profile_sha256": HASH_D,
        "expected_projection_source_sha256": HASH_E,
        "expected_selector_source_sha256": HASH_F,
        "expected_watch_code_manifest_sha256": code_hash,
    }


def _observed_hashes():
    return {"projection_source_sha256": HASH_E, "selector_source_sha256": HASH_F}


def _receipt(attempt_ids, binding, *, partial=False):
    manifest = contract.frozen_manifest()
    query = {
        "adapter_version": manifest["source"]["audit_version"],
        "sampler_version": manifest["source"]["sampler_version"],
        "symbols": sorted(binding["binding"]["scope"]["symbols"]),
        "start_utc": (DECISION - timedelta(hours=1)).isoformat(),
        "end_utc": (DECISION + timedelta(hours=1)).isoformat(),
        "max_capture_age_seconds": float(manifest["source"]["max_capture_age_seconds"]),
        "windows": [manifest["labels"]["window_minutes"]],
        "thresholds_bps": manifest["labels"]["thresholds_bps"],
        "page_size": max(1, len(attempt_ids)),
        "capture_version": manifest["source"]["watch_version"],
        "outcome_version": manifest["labels"]["method_version"],
        "parent_policy": manifest["independence"]["parent_policy_version"],
    }
    query = {**query, "query_sha256": contract.digest(query)}
    high_water = max(attempt_ids, default=0)
    population = {
        "version": selector.POPULATION_VERSION,
        "manifest_sha256": contract.MANIFEST_SHA256,
        "query_sha256": query["query_sha256"],
        "transaction_identity_sha256": HASH_A,
        "high_water_attempt_id": high_water,
        "attempt_ids": sorted(attempt_ids),
    }
    result = {
        "version": coverage_receipt.VERSION,
        "manifest_sha256": contract.MANIFEST_SHA256,
        "manifest_axes_verified": True,
        "status": "BOUNDED_PARTIAL" if partial else "COMPLETE_BOUNDED_COHORT",
        "database_connection_attempted": True,
        "pages_read": 1,
        "query_scope": query,
        "attempts_examined": len(attempt_ids),
        "high_water_attempt_id": high_water,
        "attempt_population_version": selector.POPULATION_VERSION,
        "attempt_population_sha256": contract.digest(population),
        "read_started_at_utc": DECISION.isoformat(),
        "last_page_read_started_at_utc": DECISION.isoformat(),
        "transaction_identity_sha256": HASH_A,
        "stop_reason": "MAX_PAGES" if partial else None,
        "counts": {
            "attempt_status": {"EVALUABLE": len(attempt_ids)},
            "anchor_authority_status": {"VALID": len(attempt_ids)},
            "capture_status": {"VALID": len(attempt_ids)},
            "outcome_source_status": {}, "outcome_reported_status": {},
            "parent_membership_status": {"VALID": len(attempt_ids)},
            "distinct_valid_btc_parent_movement_ids": len(attempt_ids),
            "valid_anchor_capture_parent_outcome_cell_intersections": 0,
        },
        "reason_counts": {"anchor": {}, "capture": {}, "outcome": {}, "parent": {}},
        "formula_qualification_evaluated": False,
        "candidate_score_values_returned": False,
        "interpretation": "fixture structural source coverage only",
    }
    result["outcome_free_population_receipt_sha256"] = contract.digest(
        coverage_receipt.outcome_free_population_payload(result)
    )
    return {**result, "receipt_sha256": contract.digest(result)}


def _row_and_authority(*, attempt_id=1, symbol="BTC", binding=None,
                       score=-70, available=True, parent_source=None):
    binding = binding or _binding()
    fact, event, attestation, snapshot = _fact(
        attempt_id=attempt_id, symbol=symbol, binding=binding,
        score=score, available=available,
    )
    parent_source = parent_source or _parent_source(event)
    evidence = selector.canonical_parent_membership_evidence(
        parent_source, fact=fact, direction=binding["binding"]["candidate"]["direction"]
    )[0]
    row = {
        "attempt_id": attempt_id,
        "fact": fact,
        "parent_membership_source": parent_source,
        "noneligibility_proof": None,
    }
    authority = {
        "attempt_id": attempt_id,
        "expected_fact_sha256": fact["fact_sha256"],
        "expected_watch_selection_attestation_sha256":
            attestation["attestation_sha256"],
        "expected_parent_membership_evidence_sha256": contract.digest(evidence),
        "expected_noneligibility_proof_sha256": None,
    }
    code_hash = projection._watch_code_manifest(snapshot)[1]
    return row, authority, code_hash


def _select(rows, authorities, binding, *, receipt=None, registry=None):
    receipt = receipt or _receipt([row["attempt_id"] for row in rows], binding)
    registry = registry or _registry(binding)
    return selector.select_representatives(
        binding, rows, fact_authorities=authorities,
        population_receipt=receipt,
        expected_outcome_free_population_receipt_sha256=
            receipt["outcome_free_population_receipt_sha256"],
        registry_reference=registry,
        observed_source_hashes=_observed_hashes(),
    )


def _rehash_projected_fact(row, authority):
    fact = row["fact"]
    fact["fact_sha256"] = contract.digest({
        key: value for key, value in fact.items() if key != "fact_sha256"
    })
    authority["expected_fact_sha256"] = fact["fact_sha256"]


def _selection_batch(result):
    envelope = {
        "source_audit_receipt_sha256", "selection_attestation_sha256",
        "representatives", "selector_receipt_sha256",
    }
    return {key: value for key, value in result.items() if key not in envelope}


class RepresentativeSelectorTests(unittest.TestCase):
    def test_single_valid_match_has_exact_predicted_identity_and_no_db_claim(self):
        binding = _binding()
        row, authority, code_hash = _row_and_authority(binding=binding)
        result = _select(
            [row], [authority], binding,
            registry=_registry(binding, code_hash=code_hash),
        )
        self.assertEqual(result["status"], "COMPLETE", result)
        self.assertTrue(result["population_coverage_complete"])
        self.assertTrue(result["candidate_match_coverage_complete"])
        self.assertEqual(result["representative_count"], 1)
        representative = result["representatives"][0]
        self.assertEqual(
            representative["representative_identity_sha256"],
            contract.digest(selector._representative_identity(representative)),
        )
        evidence = selector.canonical_parent_membership_evidence(
            row["parent_membership_source"], fact=row["fact"], direction="SHORT",
        )[0]
        predicted = selector.canonical_selection_fact_identity(
            binding, row["fact"],
            source_attempt_evaluation_status="EVALUABLE",
            parent_authority_class="LIVE",
            parent_membership_evidence=evidence,
        )
        expected_keys = {
            "version", "exact_binding_sha256", "attempt_id",
            "attempt_fingerprint", "sampler_version",
            "source_candle_open_utc", "source_attempt_evaluation_status",
            "anchor_slot_id", "event_id", "event_fingerprint", "symbol",
            "direction", "decision_time_utc", "knowledge_status",
            "candidate_match", "parent_authority_class",
            "btc_parent_movement_id", "parent_start_time_utc",
            "parent_policy_version", "membership_status",
            "parent_evidence_eligible",
        }
        self.assertEqual(set(predicted), expected_keys)
        self.assertEqual(
            predicted["version"], selector.SELECTION_FACT_IDENTITY_VERSION
        )
        predicted_sha = contract.digest(predicted)
        source = representative["representative"]
        self.assertEqual(
            source["expected_selection_fact_identity_sha256"], predicted_sha
        )
        self.assertNotIn("selection_fact_identity_sha256", source)
        self.assertNotIn("candidate_fact_sha256", source)
        self.assertEqual(
            result["source_authority_ledger_sha256"],
            contract.digest({
                "version": "stage8-selector-source-authority-ledger-v1",
                "exact_binding_sha256": binding["binding_sha256"],
                "attempt_population_sha256": result["attempt_population_sha256"],
                "entries": [{
                    "attempt_id": 1,
                    "expected_selection_fact_identity_sha256": predicted_sha,
                }],
            }),
        )
        self.assertEqual(
            result["selection_attestation_sha256"],
            contract.digest(_selection_batch(result)),
        )
        self.assertIsNone(representative["selection_attestation_sha256"])
        self.assertNotIn("acceptance_selection_provenance", result)
        self.assertEqual(result["structural_authority"], selector.STRUCTURAL_AUTHORITY)
        self.assertFalse(result["qualification_evaluated"])
        self.assertFalse(result["database_verification_asserted_by_selector"])
        self.assertNotIn("research_qualified", result)

    def test_cross_symbol_same_parent_is_one_pooled_vote_and_exact_order_wins(self):
        binding = _binding("ALL_BINANCE7")
        btc_row, btc_auth, code_hash = _row_and_authority(
            attempt_id=2, symbol="BTC", binding=binding,
        )
        eth_fact, eth_event, eth_attestation, eth_snapshot = _fact(
            attempt_id=1, symbol="ETH", binding=binding,
        )
        # One actual market wave across symbols: use BTC's canonical parent for ETH too.
        parent_source = _same_parent_source(
            btc_row["parent_membership_source"], eth_event
        )
        eth_evidence = selector.canonical_parent_membership_evidence(
            parent_source, fact=eth_fact, direction="SHORT"
        )[0]
        eth_row = {
            "attempt_id": 1, "fact": eth_fact,
            "parent_membership_source": parent_source,
            "noneligibility_proof": None,
        }
        eth_auth = {
            "attempt_id": 1,
            "expected_fact_sha256": eth_fact["fact_sha256"],
            "expected_watch_selection_attestation_sha256":
                eth_attestation["attestation_sha256"],
            "expected_parent_membership_evidence_sha256": contract.digest(eth_evidence),
            "expected_noneligibility_proof_sha256": None,
        }
        self.assertEqual(code_hash, projection._watch_code_manifest(eth_snapshot)[1])
        result = _select(
            [eth_row, btc_row], [eth_auth, btc_auth], binding,
            registry=_registry(binding, code_hash=code_hash),
        )
        self.assertEqual(result["status"], "COMPLETE", result)
        self.assertEqual(result["representative_count"], 1)
        # Decision times tie; BTC sorts before ETH even though its attempt ID is later.
        self.assertEqual(
            btc_row["fact"]["identity"]["decision_time_utc"],
            eth_row["fact"]["identity"]["decision_time_utc"],
        )
        self.assertEqual(result["representatives"][0]["representative"]["symbol"], "BTC")

    def test_earliest_decision_time_wins_independent_of_input_order(self):
        binding = _binding()
        first, first_event, first_attestation, first_snapshot = _fact(
            attempt_id=1, binding=binding, decision=DECISION,
        )
        second, second_event, second_attestation, second_snapshot = _fact(
            attempt_id=2, binding=binding, decision=DECISION + timedelta(minutes=60),
        )
        first_source = _parent_source(first_event, start_offset_minutes=2)
        second_source = _same_parent_source(first_source, second_event)
        rows, authorities = [], []
        for attempt_id, fact, source, attestation in (
            (1, first, first_source, first_attestation),
            (2, second, second_source, second_attestation),
        ):
            evidence = selector.canonical_parent_membership_evidence(
                source, fact=fact, direction="SHORT"
            )[0]
            rows.append({"attempt_id": attempt_id, "fact": fact,
                         "parent_membership_source": source,
                         "noneligibility_proof": None})
            authorities.append({
                "attempt_id": attempt_id,
                "expected_fact_sha256": fact["fact_sha256"],
                "expected_watch_selection_attestation_sha256":
                    attestation["attestation_sha256"],
                "expected_parent_membership_evidence_sha256": contract.digest(evidence),
                "expected_noneligibility_proof_sha256": None,
            })
        code_hash = projection._watch_code_manifest(first_snapshot)[1]
        self.assertEqual(code_hash, projection._watch_code_manifest(second_snapshot)[1])
        result = _select(
            list(reversed(rows)), list(reversed(authorities)), binding,
            registry=_registry(binding, code_hash=code_hash),
        )
        self.assertEqual(result["status"], "COMPLETE", result)
        self.assertEqual(
            result["representatives"][0]["representative"]["decision_time_utc"],
            first["identity"]["decision_time_utc"],
        )

    def test_earlier_unknown_blocks_later_match_without_winner_replacement(self):
        binding = _binding("ALL_BINANCE7")
        unknown, unknown_auth, code_hash = _row_and_authority(
            attempt_id=1, symbol="BTC", binding=binding, score=0, available=False,
        )
        later_fact, later_event, later_attestation, later_snapshot = _fact(
            attempt_id=2, symbol="ETH", binding=binding,
        )
        later_source = _same_parent_source(
            unknown["parent_membership_source"], later_event
        )
        later_evidence = selector.canonical_parent_membership_evidence(
            later_source, fact=later_fact, direction="SHORT"
        )[0]
        later = {"attempt_id": 2, "fact": later_fact,
                 "parent_membership_source": later_source,
                 "noneligibility_proof": None}
        later_auth = {
            "attempt_id": 2,
            "expected_fact_sha256": later_fact["fact_sha256"],
            "expected_watch_selection_attestation_sha256":
                later_attestation["attestation_sha256"],
            "expected_parent_membership_evidence_sha256": contract.digest(later_evidence),
            "expected_noneligibility_proof_sha256": None,
        }
        self.assertEqual(code_hash, projection._watch_code_manifest(later_snapshot)[1])
        result = _select(
            [later, unknown], [later_auth, unknown_auth], binding,
            registry=_registry(binding, code_hash=code_hash),
        )
        self.assertEqual(result["status"], "BLOCKED", result)
        self.assertFalse(result["candidate_match_coverage_complete"])
        self.assertEqual(result["representative_count"], 0)
        self.assertEqual(
            result["blocked_parents"][0]["later_known_match_attempt_id"], 2
        )
        self.assertEqual(result["blocked_parents"][0]["blocking_attempt_ids"], [1])

    def test_only_unknown_match_cannot_be_reported_as_complete_zero(self):
        binding = _binding()
        row, authority, code_hash = _row_and_authority(
            attempt_id=1, binding=binding, score=0, available=False,
        )
        result = _select(
            [row], [authority], binding,
            registry=_registry(binding, code_hash=code_hash),
        )
        self.assertEqual(result["status"], "BLOCKED", result)
        self.assertEqual(result["representative_count"], 0)
        self.assertFalse(result["candidate_match_coverage_complete"])
        self.assertEqual(len(result["blocked_parents"]), 1)
        self.assertNotIn("acceptance_selection_provenance", result)
        self.assertEqual(
            result["selection_attestation_sha256"],
            contract.digest(_selection_batch(result)),
        )

    def test_known_false_does_not_block_the_later_earliest_true_match(self):
        binding = _binding("ALL_BINANCE7")
        false_row, false_auth, code_hash = _row_and_authority(
            attempt_id=1, symbol="BTC", binding=binding, score=70,
        )
        fact, event, attestation, snapshot = _fact(
            attempt_id=2, symbol="ETH", binding=binding,
        )
        source = _same_parent_source(false_row["parent_membership_source"], event)
        evidence = selector.canonical_parent_membership_evidence(
            source, fact=fact, direction="SHORT"
        )[0]
        true_row = {"attempt_id": 2, "fact": fact,
                    "parent_membership_source": source,
                    "noneligibility_proof": None}
        true_auth = {
            "attempt_id": 2, "expected_fact_sha256": fact["fact_sha256"],
            "expected_watch_selection_attestation_sha256":
                attestation["attestation_sha256"],
            "expected_parent_membership_evidence_sha256": contract.digest(evidence),
            "expected_noneligibility_proof_sha256": None,
        }
        self.assertEqual(code_hash, projection._watch_code_manifest(snapshot)[1])
        result = _select(
            [false_row, true_row], [false_auth, true_auth], binding,
            registry=_registry(binding, code_hash=code_hash),
        )
        self.assertEqual(result["status"], "COMPLETE", result)
        self.assertEqual(result["representatives"][0]["representative"]["event_id"],
                         fact["identity"]["event_id"])

    def test_complete_zero_is_allowed_for_a_known_false_full_population(self):
        binding = _binding()
        row, authority, code_hash = _row_and_authority(
            attempt_id=1, binding=binding, score=70,
        )
        result = _select(
            [row], [authority], binding,
            registry=_registry(binding, code_hash=code_hash),
        )
        self.assertEqual(result["status"], "COMPLETE", result)
        self.assertEqual(result["representative_count"], 0)
        self.assertTrue(result["candidate_match_coverage_complete"])
        self.assertNotIn("acceptance_selection_provenance", result)
        self.assertEqual(
            result["selection_attestation_sha256"],
            contract.digest(_selection_batch(result)),
        )

    def test_proven_no_decision_attempt_can_complete_without_membership(self):
        binding = _binding()
        row, authority, code_hash = _row_and_authority(binding=binding)
        fact = deepcopy(row["fact"])
        fact["identity"]["anchor_slot_id"] = None
        fact["identity"]["event_id"] = None
        fact["identity"]["event_fingerprint"] = None
        fact["identity"]["decision_time_utc"] = None
        fact["knowledge_status"] = "UNKNOWN"
        fact["candidate_match"] = None
        fact["reasons"] = ["ATTEMPT_NOT_EVALUABLE"]
        fact["source_reasons"] = {"anchor": [], "selection": [], "capture": []}
        fact["feature"]["raw_score"] = None
        fact["feature"]["source_direction"] = None
        fact["feature"]["aligned_score"] = None
        fact["fact_sha256"] = contract.digest({
            key: value for key, value in fact.items() if key != "fact_sha256"
        })
        unsigned_proof = {
            "version": selector.NONELIGIBILITY_VERSION,
            "attempt_id": 1,
            "attempt_fingerprint": fact["identity"]["attempt_fingerprint"],
            "sampler_version": fact["identity"]["sampler_version"],
            "symbol": fact["identity"]["symbol"],
            "evaluation_status": "UNEVALUABLE",
            "decision_time_utc": None,
            "reason": "ATTEMPT_UNEVALUABLE_NO_DECISION",
        }
        proof = {**unsigned_proof, "proof_sha256": contract.digest(unsigned_proof)}
        source = {"attempt_id": 1, "fact": fact,
                  "parent_membership_source": None,
                  "noneligibility_proof": proof}
        authority = {
            "attempt_id": 1, "expected_fact_sha256": fact["fact_sha256"],
            "expected_watch_selection_attestation_sha256": None,
            "expected_parent_membership_evidence_sha256": None,
            "expected_noneligibility_proof_sha256": proof["proof_sha256"],
        }
        result = _select(
            [source], [authority], binding,
            registry=_registry(binding, code_hash=code_hash),
        )
        self.assertEqual(result["status"], "COMPLETE", result)
        self.assertEqual(result["proven_noneligible_attempt_ids"], [1])
        self.assertEqual(result["representative_count"], 0)
        predicted = selector.canonical_selection_fact_identity(
            binding, fact,
            source_attempt_evaluation_status="UNEVALUABLE",
            parent_authority_class="PROVEN_NOT_CANDIDATE_ELIGIBLE",
            parent_membership_evidence=None,
        )
        self.assertEqual(
            {
                key: predicted[key] for key in (
                    "anchor_slot_id", "event_id", "event_fingerprint",
                    "decision_time_utc", "btc_parent_movement_id",
                    "parent_start_time_utc", "parent_policy_version",
                    "membership_status", "parent_evidence_eligible",
                )
            },
            {
                "anchor_slot_id": None, "event_id": None,
                "event_fingerprint": None, "decision_time_utc": None,
                "btc_parent_movement_id": None,
                "parent_start_time_utc": None,
                "parent_policy_version": None, "membership_status": None,
                "parent_evidence_eligible": None,
            },
        )
        self.assertEqual(
            predicted["source_attempt_evaluation_status"], "UNEVALUABLE"
        )

    def test_evaluable_missing_slot_identity_has_fixed_nulls_golden_digest(self):
        binding = _binding()
        row, _, _ = _row_and_authority(binding=binding)
        fact = deepcopy(row["fact"])
        for key in ("anchor_slot_id", "event_id", "event_fingerprint"):
            fact["identity"][key] = None
        # An EVALUABLE source attempt can lack the candidate-direction slot.
        # Its source attempt still has a decision time, but all slot-derived
        # selection identity fields are fixed JSON nulls in Python and SQL.
        fact["knowledge_status"] = "UNKNOWN"
        fact["candidate_match"] = None
        evidence, parent_class = selector.canonical_parent_membership_evidence(
            {"event": None, "membership": None, "parent": None, "btc_bar": None},
            fact=fact, direction="SHORT",
        )
        predicted = selector.canonical_selection_fact_identity(
            binding, fact,
            source_attempt_evaluation_status="EVALUABLE",
            parent_authority_class=parent_class,
            parent_membership_evidence=evidence,
        )
        self.assertEqual(parent_class, "UNKNOWN")
        for key in (
            "anchor_slot_id", "event_id", "event_fingerprint",
            "decision_time_utc", "btc_parent_movement_id",
            "parent_start_time_utc", "parent_policy_version",
            "membership_status", "parent_evidence_eligible",
        ):
            self.assertIsNone(predicted[key], key)
        self.assertEqual(
            contract.digest(predicted),
            "d0e002df6cd6080b0f3b55c236b5d44c2215b81887a7b7708c41f61ec81d373d",
        )

    def test_missing_membership_blocks_globally_but_boundary_proof_is_excluded(self):
        binding = _binding()
        row, authority, code_hash = _row_and_authority(binding=binding)
        missing = deepcopy(row)
        missing["parent_membership_source"]["membership"] = None
        result = _select(
            [missing], [authority], binding,
            registry=_registry(binding, code_hash=code_hash),
        )
        self.assertEqual(result["status"], "BLOCKED")
        self.assertIn("PARENT_MEMBERSHIP_UNKNOWN_OR_INVALID", result["global_blockers"])

        fact, event, attestation, snapshot = _fact(attempt_id=1, binding=binding)
        boundary = _parent_source(event, eligible=False)
        evidence = selector.canonical_parent_membership_evidence(
            boundary, fact=fact, direction="SHORT"
        )[0]
        boundary_row = {"attempt_id": 1, "fact": fact,
                        "parent_membership_source": boundary,
                        "noneligibility_proof": None}
        boundary_auth = {
            "attempt_id": 1, "expected_fact_sha256": fact["fact_sha256"],
            "expected_watch_selection_attestation_sha256":
                attestation["attestation_sha256"],
            "expected_parent_membership_evidence_sha256": contract.digest(evidence),
            "expected_noneligibility_proof_sha256": None,
        }
        complete = _select(
            [boundary_row], [boundary_auth], binding,
            registry=_registry(
                binding, code_hash=projection._watch_code_manifest(snapshot)[1]
            ),
        )
        self.assertEqual(complete["status"], "COMPLETE", complete)
        self.assertEqual(complete["representative_count"], 0)
        self.assertEqual(complete["proven_noneligible_attempt_ids"], [1])

    def test_partial_or_cherry_picked_population_cannot_be_complete(self):
        binding = _binding()
        first, first_auth, code_hash = _row_and_authority(attempt_id=1, binding=binding)
        second, second_auth, _ = _row_and_authority(attempt_id=2, binding=binding)
        complete_receipt = _receipt([1, 2], binding)
        with self.assertRaisesRegex(ValueError, "population receipt"):
            _select(
                [first], [first_auth], binding, receipt=complete_receipt,
                registry=_registry(binding, code_hash=code_hash),
            )
        partial = _receipt([1, 2], binding, partial=True)
        with self.assertRaisesRegex(ValueError, "population receipt"):
            _select(
                [first, second], [first_auth, second_auth], binding,
                receipt=partial,
                registry=_registry(binding, code_hash=code_hash),
            )

    def test_receipt_and_source_hashes_are_out_of_band_authority_inputs(self):
        binding = _binding()
        row, authority, code_hash = _row_and_authority(binding=binding)
        receipt = _receipt([1], binding)
        tampered = deepcopy(receipt)
        tampered["attempts_examined"] = 0
        with self.assertRaisesRegex(ValueError, "receipt hash"):
            selector.select_representatives(
                binding, [row], fact_authorities=[authority],
                population_receipt=tampered,
                expected_outcome_free_population_receipt_sha256=
                    receipt["outcome_free_population_receipt_sha256"],
                registry_reference=_registry(binding, code_hash=code_hash),
                observed_source_hashes=_observed_hashes(),
            )
        registry = _registry(binding, code_hash=code_hash)
        registry["expected_selector_source_sha256"] = HASH_A
        with self.assertRaisesRegex(ValueError, "implementation"):
            _select([row], [authority], binding, registry=registry)

    def test_pre_freeze_parent_is_discovery_only_not_prospective_evidence(self):
        binding = _binding()
        row, authority, code_hash = _row_and_authority(binding=binding)
        parent_start = row["parent_membership_source"]["parent"]["start_time_utc"]
        result = _select(
            [row], [authority], binding,
            registry=_registry(
                binding, frozen_at=parent_start, code_hash=code_hash
            ),
        )
        self.assertEqual(result["status"], "COMPLETE", result)
        self.assertEqual(result["representative_count"], 0)
        self.assertEqual(len(result["excluded_pre_freeze_parent_ids"]), 1)

    def test_duplicate_or_conflicting_anchor_event_cannot_count_twice(self):
        binding = _binding("ALL_BINANCE7")
        first, first_auth, code_hash = _row_and_authority(
            attempt_id=1, symbol="BTC", binding=binding
        )
        second, second_auth, _ = _row_and_authority(
            attempt_id=2, symbol="ETH", binding=binding
        )
        fact = second["fact"]
        # Reuse the first exact anchor/event IDs with a different fact identity.
        fact["identity"]["anchor_slot_id"] = first["fact"]["identity"]["anchor_slot_id"]
        fact["identity"]["event_id"] = first["fact"]["identity"]["event_id"]
        fact["fact_sha256"] = contract.digest({
            key: value for key, value in fact.items() if key != "fact_sha256"
        })
        second_auth["expected_fact_sha256"] = fact["fact_sha256"]
        source = second["parent_membership_source"]
        source["event"]["event_id"] = fact["identity"]["event_id"]
        source["membership"]["event_id"] = fact["identity"]["event_id"]
        source["event"]["event_fingerprint"] = fact["identity"]["event_fingerprint"]
        evidence = selector.canonical_parent_membership_evidence(
            source, fact=fact, direction="SHORT"
        )[0]
        second_auth["expected_parent_membership_evidence_sha256"] = contract.digest(evidence)
        result = _select(
            [first, second], [first_auth, second_auth], binding,
            registry=_registry(binding, code_hash=code_hash),
        )
        self.assertEqual(result["status"], "BLOCKED", result)
        self.assertIn("CONFLICTING_EXACT_ANCHOR_EVENT", result["global_blockers"])
        self.assertLessEqual(result["representative_count"], 1)

    def test_canonical_time_is_required_and_noncanonical_tie_cannot_reorder(self):
        binding = _binding()
        row, authority, code_hash = _row_and_authority(binding=binding)
        malformed = deepcopy(row)
        malformed["fact"]["identity"]["decision_time_utc"] = (
            malformed["fact"]["identity"]["decision_time_utc"].replace(".000000Z", "Z")
        )
        malformed["fact"]["fact_sha256"] = contract.digest({
            key: value for key, value in malformed["fact"].items()
            if key != "fact_sha256"
        })
        changed = deepcopy(authority)
        changed["expected_fact_sha256"] = malformed["fact"]["fact_sha256"]
        result = _select(
            [malformed], [changed], binding,
            registry=_registry(binding, code_hash=code_hash),
        )
        self.assertEqual(result["status"], "BLOCKED")
        self.assertIn("PROJECTED_FACT_AUTHORITY_INVALID", result["global_blockers"])

    def test_outcomes_have_no_input_channel_and_cannot_change_selection(self):
        binding = _binding()
        row, authority, code_hash = _row_and_authority(binding=binding)
        registry = _registry(binding, code_hash=code_hash)
        baseline = _select([row], [authority], binding, registry=registry)
        hypothetical_outcomes = {"event": "SUCCESS", "mfe_pct": 999.0}
        hypothetical_outcomes.update(event="FAILURE", mfe_pct=0.0)
        repeated = _select([deepcopy(row)], [deepcopy(authority)], binding,
                           registry=deepcopy(registry))
        self.assertEqual(
            baseline["representatives"][0]["representative_identity_sha256"],
            repeated["representatives"][0]["representative_identity_sha256"],
        )
        poisoned = deepcopy(row)
        poisoned["outcome"] = hypothetical_outcomes
        with self.assertRaisesRegex(ValueError, "outcome/label fields"):
            _select([poisoned], [authority], binding, registry=registry)

    def test_covert_audit_metadata_changes_full_fact_not_selection_identity(self):
        binding = _binding()
        row, authority, code_hash = _row_and_authority(binding=binding)
        registry = _registry(binding, code_hash=code_hash)
        baseline = _select([row], [authority], binding, registry=registry)

        changed = deepcopy(row)
        changed_authority = deepcopy(authority)
        changed["fact"]["source"]["watch_snapshot"]["cycle_id"] = (
            "different-audit-cycle"
        )
        _rehash_projected_fact(changed, changed_authority)
        repeated = _select(
            [changed], [changed_authority], binding, registry=deepcopy(registry),
        )

        self.assertNotEqual(row["fact"]["fact_sha256"], changed["fact"]["fact_sha256"])
        self.assertEqual(repeated["status"], "COMPLETE", repeated)
        for key in (
            "source_authority_ledger_sha256", "representative_set_sha256",
            "selection_attestation_sha256",
        ):
            self.assertEqual(baseline[key], repeated[key], key)
        self.assertEqual(baseline["representatives"], repeated["representatives"])

    def test_unknown_reason_metadata_cannot_change_blocked_selection_identity(self):
        binding = _binding()
        row, authority, code_hash = _row_and_authority(
            binding=binding, score=0, available=False,
        )
        registry = _registry(binding, code_hash=code_hash)
        baseline = _select([row], [authority], binding, registry=registry)

        changed = deepcopy(row)
        changed_authority = deepcopy(authority)
        changed["fact"]["reasons"] = sorted(
            changed["fact"]["reasons"] + ["ZZZ_AUDIT_ONLY_REASON"]
        )
        _rehash_projected_fact(changed, changed_authority)
        repeated = _select(
            [changed], [changed_authority], binding, registry=deepcopy(registry),
        )

        self.assertNotEqual(row["fact"]["fact_sha256"], changed["fact"]["fact_sha256"])
        self.assertEqual(baseline["status"], "BLOCKED")
        self.assertEqual(repeated["status"], "BLOCKED")
        self.assertEqual(
            baseline["source_authority_ledger_sha256"],
            repeated["source_authority_ledger_sha256"],
        )
        self.assertEqual(
            baseline["selection_attestation_sha256"],
            repeated["selection_attestation_sha256"],
        )
        self.assertEqual(baseline["blocked_parents"], repeated["blocked_parents"])

    def test_causal_source_time_mutation_changes_every_selection_identity_layer(self):
        binding = _binding()
        row, authority, code_hash = _row_and_authority(binding=binding)
        registry = _registry(binding, code_hash=code_hash)
        baseline = _select([row], [authority], binding, registry=registry)

        changed = deepcopy(row)
        changed_authority = deepcopy(authority)
        source_open = datetime.fromisoformat(
            changed["fact"]["identity"]["source_candle_open_utc"].replace(
                "Z", "+00:00"
            )
        )
        changed["fact"]["identity"]["source_candle_open_utc"] = _iso(
            source_open + timedelta(minutes=1)
        )
        _rehash_projected_fact(changed, changed_authority)
        repeated = _select(
            [changed], [changed_authority], binding, registry=deepcopy(registry),
        )

        self.assertEqual(repeated["status"], "COMPLETE", repeated)
        self.assertNotEqual(
            baseline["representatives"][0]["representative"]
                ["expected_selection_fact_identity_sha256"],
            repeated["representatives"][0]["representative"]
                ["expected_selection_fact_identity_sha256"],
        )
        for key in (
            "source_authority_ledger_sha256", "representative_set_sha256",
            "selection_attestation_sha256",
        ):
            self.assertNotEqual(baseline[key], repeated[key], key)
        self.assertNotEqual(
            baseline["representatives"][0]["representative_identity_sha256"],
            repeated["representatives"][0]["representative_identity_sha256"],
        )

    def test_outcome_receipt_metamorphism_cannot_change_selection_identity(self):
        binding = _binding()
        row, authority, code_hash = _row_and_authority(binding=binding)
        registry = _registry(binding, code_hash=code_hash)
        receipt = _receipt([1], binding)
        baseline = _select(
            [row], [authority], binding, receipt=receipt, registry=registry
        )
        changed = deepcopy(receipt)
        changed["counts"]["outcome_source_status"] = {
            "SUCCESS": 999999, "FAILURE": 999999,
        }
        changed["counts"]["outcome_reported_status"] = {"DATA_MISSING": 12345}
        changed["counts"]["valid_anchor_capture_parent_outcome_cell_intersections"] = 0
        changed["reason_counts"]["outcome"] = {"UNRECOGNIZED": 777}
        changed["receipt_sha256"] = contract.digest({
            key: value for key, value in changed.items() if key != "receipt_sha256"
        })
        self.assertEqual(
            changed["outcome_free_population_receipt_sha256"],
            receipt["outcome_free_population_receipt_sha256"],
        )
        repeated = _select(
            [deepcopy(row)], [deepcopy(authority)], binding,
            receipt=changed, registry=deepcopy(registry),
        )
        self.assertEqual(
            baseline["representative_set_sha256"], repeated["representative_set_sha256"]
        )
        self.assertEqual(
            baseline["selection_attestation_sha256"],
            repeated["selection_attestation_sha256"],
        )
        self.assertEqual(baseline["representatives"], repeated["representatives"])
        self.assertNotEqual(
            baseline["source_audit_receipt_sha256"],
            repeated["source_audit_receipt_sha256"],
        )

    def test_wrong_binding_parent_or_authority_fails_closed(self):
        binding = _binding()
        row, authority, code_hash = _row_and_authority(binding=binding)
        wrong_parent = deepcopy(row)
        wrong_parent["parent_membership_source"]["parent"][
            "btc_parent_movement_id"
        ] = HASH_A
        result = _select(
            [wrong_parent], [authority], binding,
            registry=_registry(binding, code_hash=code_hash),
        )
        self.assertEqual(result["status"], "BLOCKED")
        self.assertIn("PARENT_MEMBERSHIP_UNKNOWN_OR_INVALID", result["global_blockers"])

        wrong_authority = deepcopy(authority)
        wrong_authority["expected_fact_sha256"] = HASH_A
        result = _select(
            [row], [wrong_authority], binding,
            registry=_registry(binding, code_hash=code_hash),
        )
        self.assertEqual(result["status"], "BLOCKED")
        self.assertIn("PROJECTED_FACT_AUTHORITY_INVALID", result["global_blockers"])

    def test_runtime_dependency_monkeypatch_cannot_weaken_validation(self):
        binding = _binding()
        row, authority, code_hash = _row_and_authority(binding=binding)
        receipt = _receipt([1], binding)
        arguments = dict(
            exact_binding=binding, source_rows=[row], fact_authorities=[authority],
            population_receipt=receipt,
            expected_outcome_free_population_receipt_sha256=
                receipt["outcome_free_population_receipt_sha256"],
            registry_reference=_registry(binding, code_hash=code_hash),
            observed_source_hashes=_observed_hashes(),
        )
        for target, replacement in (
            ("research_stage8_representative_selector.projection.validate_fact",
             lambda *args, **kwargs: None),
            ("research_stage8_representative_selector.coverage_receipt.VERSION", "forged"),
            ("research_stage8_representative_selector.source_audit.VERSION", "forged"),
        ):
            with self.subTest(target=target), patch(target, replacement), \
                    self.assertRaisesRegex(ValueError, "frozen runtime contract"):
                selector.select_representatives(**arguments)


if __name__ == "__main__":
    unittest.main(verbosity=2)
