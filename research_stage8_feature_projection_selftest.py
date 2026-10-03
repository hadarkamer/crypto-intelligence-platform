"""Adversarial, network-free checks for the Stage-8 Watch-v2 projection."""
from __future__ import annotations

from copy import deepcopy
from datetime import timedelta
import unittest
from unittest.mock import patch

import research_operational_score_source_audit as source_audit
from research_operational_score_source_audit_selftest import (
    DECISION,
    _anchor,
    _block,
    _rehash,
    _snapshot,
    inputs,
)
import research_stage8_contract as contract
import research_stage8_feature_projection as projection


def _fresh(snapshot):
    snapshot["available_at_utc"] = DECISION - timedelta(minutes=2)
    snapshot["created_at_utc"] = DECISION - timedelta(minutes=1)
    return snapshot


def _set_model(snapshot, *, symbol="BTC", model="futures_flow", score=-70,
               direction="BEARISH", available=True, capture_status="AVAILABLE"):
    observation = _block(snapshot)["coins"][symbol]["models"][model]
    observation.update(score=score, direction=direction, available=available,
                       capture_status=capture_status)
    return _rehash(snapshot)


def _iso(value):
    return projection._iso(value)


def _selection_attestation(attempt, snapshot):
    selected = snapshot is not None
    durable = max(snapshot["available_at_utc"], snapshot["created_at_utc"]) if selected else None
    query_scope = {
        "version": projection.WATCH_SELECTION_QUERY_VERSION,
        "source_audit_version": source_audit.VERSION,
        "source": "WATCH_SHARED",
        "selection_policy": projection.WATCH_SELECTION_POLICY,
        "attempt_id": attempt.get("attempt_id"),
        "attempt_fingerprint": attempt.get("attempt_fingerprint"),
        "symbol": attempt.get("symbol"),
        "decision_time_utc": _iso(attempt.get("decision_time_utc")),
        "max_capture_age_seconds": contract.frozen_manifest()["source"]["max_capture_age_seconds"],
        "archive_snapshot_high_water_id": snapshot.get("snapshot_set_id") if selected else 0,
        "database_snapshot_id": "fixture-repeatable-read-snapshot-v1",
    }
    _, code_manifest_hash = projection._watch_code_manifest(snapshot)
    value = {
        "version": projection.WATCH_SELECTION_ATTESTATION_VERSION,
        "source_audit_version": source_audit.VERSION,
        "query_scope": query_scope,
        "query_binding_sha256": contract.digest(query_scope),
        "transaction_snapshot": "CALLER_TRANSACTION_SNAPSHOT",
        "population_complete": True,
        "selection_policy": projection.WATCH_SELECTION_POLICY,
        "attempt_id": attempt.get("attempt_id"),
        "attempt_fingerprint": attempt.get("attempt_fingerprint"),
        "symbol": attempt.get("symbol"),
        "decision_time_utc": _iso(attempt.get("decision_time_utc")),
        "max_capture_age_seconds": contract.frozen_manifest()["source"]["max_capture_age_seconds"],
        "read_started_at_utc": _iso(DECISION + timedelta(days=1)),
        "selection_status": "SELECTED" if selected else "NO_MATCH",
        "selected_snapshot_set_id": snapshot.get("snapshot_set_id") if selected else None,
        "selected_snapshot_key": snapshot.get("snapshot_key") if selected else None,
        "selected_payload_sha256": snapshot.get("payload_sha256") if selected else None,
        "selected_durably_available_at_utc": _iso(durable) if selected else None,
        "watch_code_manifest_sha256": code_manifest_hash if selected else None,
    }
    value["attestation_sha256"] = contract.digest(value)
    return value


class FeatureProjectionTests(unittest.TestCase):
    def setUp(self):
        self.attempt, slot, events = _anchor()
        self.slot, self.events = slot, events
        self.authority = source_audit.validate_anchor_authority(self.attempt, slot, events)
        self.assertEqual(self.authority["status"], "VALID", self.authority)
        self.snapshot = _set_model(_fresh(_snapshot()))
        self.binding = contract.exact_binding(
            scope_id="BINANCE_BTC",
            candidate_id="FUTURES_FLOW_ALIGNED65_SHORT",
            threshold_bps=50,
        )

    def project(self, snapshot=None, *, binding=None, attempt=None, slot=None, events=None,
                attestation=None):
        selected = self.snapshot if snapshot is None else snapshot
        actual_attempt = self.attempt if attempt is None else attempt
        return projection.project_binding_fact(
            attempt=actual_attempt,
            anchor_slot=self.slot if slot is None else slot,
            anchor_events=self.events if events is None else events,
            selected_watch_snapshot=selected,
            watch_selection_attestation=(
                _selection_attestation(actual_attempt, selected)
                if attestation is None else attestation
            ),
            binding=self.binding if binding is None else binding,
        )

    def capture_result(self, snapshot=None):
        snapshot = self.snapshot if snapshot is None else snapshot
        return source_audit.validate_capture(
            snapshot, symbol=self.attempt["symbol"],
            decision_time_utc=self.attempt["decision_time_utc"],
            max_capture_age_seconds=contract.frozen_manifest()["source"]["max_capture_age_seconds"],
        )

    def test_known_directional_fact_is_detached_and_anchor_scores_stay_absent(self):
        before = deepcopy((self.attempt, self.slot, self.events, self.snapshot))
        fact = self.project()
        self.assertEqual(fact["knowledge_status"], projection.KNOWN, fact)
        self.assertTrue(fact["candidate_match"])
        self.assertEqual(fact["feature"]["raw_score"], -70.0)
        self.assertEqual(fact["feature"]["aligned_score"], 70.0)
        self.assertEqual(fact["feature"]["source_direction"], "BEARISH")
        self.assertEqual(fact["source"]["anchor_model_score_status"], "ABSENT")
        self.assertEqual(fact["source"]["watch_version"], "watch-operational-scores-v2")
        projection.validate_fact(
            fact,
            expected_fact_sha256=fact["fact_sha256"],
            expected_watch_selection_attestation_sha256=
            _selection_attestation(self.attempt, self.snapshot)["attestation_sha256"],
            expected_watch_code_manifest_sha256=
            projection._watch_code_manifest(self.snapshot)[1],
        )
        self.assertEqual((self.attempt, self.slot, self.events, self.snapshot), before)
        self.assertEqual(fact["identity"]["anchor_slot_id"], self.slot["anchor_slot_id"])
        self.assertEqual(fact["identity"]["event_id"], self.slot["short_event_id"])
        self.assertEqual(
            fact["identity"]["event_fingerprint"], self.events[1]["event_fingerprint"]
        )

        # The opposite candidate is a known false fact from the same raw score,
        # not a score recomputation and not an outcome-derived inverse.
        opposite = self.project(binding=contract.exact_binding(
            scope_id="BINANCE_BTC",
            candidate_id="FUTURES_FLOW_ALIGNED65_LONG",
            threshold_bps=50,
        ))
        self.assertEqual(opposite["knowledge_status"], projection.KNOWN)
        self.assertFalse(opposite["candidate_match"])
        self.assertEqual(opposite["feature"]["aligned_score"], -70.0)

    def test_frozen_direction_and_aligned_threshold_boundaries_are_exact(self):
        cases = (
            (-65.0, "BEARISH"),
            (-64.999, "BEARISH"),
            (-12.0, "BEARISH"),
            (-11.999, "NEUTRAL"),
            (0.0, "NEUTRAL"),
            (11.999, "NEUTRAL"),
            (12.0, "BULLISH"),
            (64.999, "BULLISH"),
            (65.0, "BULLISH"),
        )
        for raw_score, source_direction in cases:
            snapshot = _set_model(
                deepcopy(self.snapshot), score=raw_score,
                direction=source_direction,
            )
            for candidate_direction, multiplier in (("LONG", 1), ("SHORT", -1)):
                with self.subTest(
                        raw_score=raw_score,
                        candidate_direction=candidate_direction):
                    binding = contract.exact_binding(
                        scope_id="BINANCE_BTC",
                        candidate_id=(
                            "FUTURES_FLOW_ALIGNED65_" + candidate_direction
                        ),
                        threshold_bps=50,
                    )
                    fact = self.project(snapshot, binding=binding)
                    aligned = raw_score * multiplier
                    self.assertEqual(fact["knowledge_status"], projection.KNOWN, fact)
                    self.assertEqual(
                        fact["feature"]["source_direction"], source_direction,
                    )
                    self.assertEqual(fact["feature"]["raw_score"], raw_score)
                    self.assertEqual(fact["feature"]["aligned_score"], aligned)
                    self.assertIs(fact["candidate_match"], aligned >= 65.0)

    def test_v4_model_absent_marker_cannot_be_replaced_by_anchor_data(self):
        slot = deepcopy(self.slot)
        slot["decision_feature_bundle"]["model_score_status"] = "CAPTURED"
        fact = self.project(slot=slot)
        self.assertEqual(fact["knowledge_status"], projection.UNKNOWN)
        self.assertIsNone(fact["candidate_match"])
        self.assertIsNone(fact["feature"]["raw_score"])
        self.assertIn("ANCHOR_MODEL_SCORE_STATUS_NOT_ABSENT", fact["reasons"])

    def test_stale_and_post_decision_captures_remain_unknown(self):
        for case, when in (
            ("stale", DECISION - timedelta(seconds=301)),
            ("post-decision", DECISION + timedelta(microseconds=1)),
        ):
            with self.subTest(case=case):
                snapshot = deepcopy(self.snapshot)
                snapshot["available_at_utc"] = when
                snapshot["created_at_utc"] = when
                fact = self.project(snapshot)
                self.assertEqual(fact["knowledge_status"], projection.UNKNOWN)
                self.assertIsNone(fact["candidate_match"])
                self.assertIsNone(fact["feature"]["aligned_score"])
                self.assertIn(
                    "WATCH_SELECTION_ATTESTATION_INVALID",
                    fact["source_reasons"]["selection"],
                )

    def test_invalid_inner_hash_never_projects_a_score(self):
        snapshot = deepcopy(self.snapshot)
        _block(snapshot)["coins"]["BTC"]["models"]["futures_flow"]["score"] = -90
        fact = self.project(snapshot)
        self.assertEqual(fact["knowledge_status"], projection.UNKNOWN)
        self.assertIsNone(fact["candidate_match"])
        self.assertIsNone(fact["feature"]["raw_score"])
        self.assertIn("CAPTURE_INNER_HASH_MISMATCH", fact["reasons"])

    def test_newer_malformed_capture_does_not_fall_back_to_older_valid_score(self):
        older = deepcopy(self.snapshot)
        older["available_at_utc"] = DECISION - timedelta(minutes=3)
        older["created_at_utc"] = DECISION - timedelta(minutes=3)
        older["snapshot_set_id"] = 100
        newer = deepcopy(self.snapshot)
        newer["available_at_utc"] = DECISION - timedelta(minutes=1)
        newer["created_at_utc"] = DECISION - timedelta(minutes=1)
        newer["snapshot_set_id"] = 102
        _block(newer)["coins"]["BTC"]["models"]["futures_flow"]["score"] = -99
        fact = projection.project_binding_fact(
            attempt=self.attempt, anchor_slot=self.slot, anchor_events=self.events,
            selected_watch_snapshot=newer,
            watch_selection_attestation=_selection_attestation(self.attempt, newer),
            binding=self.binding,
        )
        self.assertEqual(fact["knowledge_status"], projection.UNKNOWN)
        self.assertIsNone(fact["candidate_match"])
        self.assertIn("CAPTURE_INNER_HASH_MISMATCH", fact["reasons"])
        self.assertEqual(fact["source"]["watch_snapshot"]["snapshot_set_id"], 102)

    def test_unavailable_fallback_zero_and_missing_model_are_unknown_not_false(self):
        unavailable = _set_model(
            deepcopy(self.snapshot), score=0, direction="NEUTRAL",
            available=False, capture_status="UNAVAILABLE",
        )
        fact = self.project(unavailable)
        self.assertEqual(fact["knowledge_status"], projection.UNKNOWN)
        self.assertIsNone(fact["candidate_match"])
        self.assertIsNone(fact["feature"]["raw_score"])
        self.assertIn("MODEL_NOT_AVAILABLE", fact["reasons"])
        self.assertIn("MODEL_CAPTURE_STATUS_NOT_AVAILABLE", fact["reasons"])

        missing = deepcopy(self.snapshot)
        _block(missing)["coins"]["BTC"]["models"].pop("futures_flow")
        _rehash(missing)
        result = self.project(missing)
        self.assertEqual(result["knowledge_status"], projection.UNKNOWN)
        self.assertIsNone(result["candidate_match"])
        self.assertIn("MODEL_OBSERVATION_MISSING", result["reasons"])

    def test_model_source_direction_and_family_are_not_inferred_from_sign(self):
        wrong_direction = _set_model(
            deepcopy(self.snapshot), score=-70, direction="BULLISH"
        )
        # The source audit checks capture integrity/causality; projection adds
        # the model-specific signed-direction invariant.
        self.assertEqual(self.capture_result(wrong_direction)["status"], "VALID")
        fact = self.project(wrong_direction)
        self.assertEqual(fact["knowledge_status"], projection.UNKNOWN)
        self.assertIn("MODEL_DIRECTION_SCORE_MISMATCH", fact["reasons"])

        wrong_family = deepcopy(self.snapshot)
        model = _block(wrong_family)["coins"]["BTC"]["models"]["futures_flow"]
        model["family"] = "Spot Flow"
        _rehash(wrong_family)
        fact = self.project(wrong_family)
        self.assertEqual(fact["knowledge_status"], projection.UNKNOWN)
        self.assertIn("MODEL_FAMILY_MISMATCH", fact["reasons"])

    def test_outcomes_do_not_change_projection_and_first_tranche_order_is_stable(self):
        first = self.project()
        second = self.project()
        self.assertEqual(first, second)
        with self.assertRaises(TypeError):
            projection.project_binding_fact(
                attempt=self.attempt, anchor_slot=self.slot, anchor_events=self.events,
                selected_watch_snapshot=self.snapshot,
                watch_selection_attestation=_selection_attestation(self.attempt, self.snapshot),
                binding=self.binding,
                outcome_cells=[{"success": True}],
            )

        tranche1 = projection.project_first_tranche(
            attempt=self.attempt, anchor_slot=self.slot, anchor_events=self.events,
            selected_watch_snapshot=self.snapshot,
            watch_selection_attestation=_selection_attestation(self.attempt, self.snapshot),
        )
        tranche2 = projection.project_first_tranche(
            attempt=self.attempt, anchor_slot=self.slot, anchor_events=self.events,
            selected_watch_snapshot=self.snapshot,
            watch_selection_attestation=_selection_attestation(self.attempt, self.snapshot),
        )
        self.assertEqual(tranche1, tranche2)
        # BTC belongs to its singleton and ALL_BINANCE7: 2 * 6 * 8.
        self.assertEqual(tranche1["fact_count"], 96)
        self.assertEqual(tranche1["facts_sha256"], tranche2["facts_sha256"])
        self.assertEqual(
            tranche1["facts"][0]["identity"]["scope_id"], "BINANCE_BTC"
        )

    def test_numeric_roundtrip_and_fact_hash_are_deterministic(self):
        integer = self.project()
        floated_snapshot = _set_model(deepcopy(self.snapshot), score=-70.0)
        floated = self.project(floated_snapshot)
        self.assertEqual(integer, floated)
        self.assertEqual(integer["fact_sha256"], self.project()["fact_sha256"])
        tampered = deepcopy(integer)
        tampered["candidate_match"] = False
        with self.assertRaisesRegex(ValueError, "STAGE8_PROJECTED_FACT_INVALID"):
            projection.validate_fact(
                tampered,
                expected_fact_sha256=integer["fact_sha256"],
                expected_watch_selection_attestation_sha256=
                integer["source"]["watch_selection_attestation_sha256"],
                expected_watch_code_manifest_sha256=
                integer["source"]["watch_code_manifest_sha256"],
            )

    def test_rehashed_semantic_forgeries_fail_fact_validation(self):
        original = self.project()
        expected_code = original["source"]["watch_code_manifest_sha256"]

        def resigned(mutator):
            value = deepcopy(original)
            mutator(value)
            value.pop("fact_sha256", None)
            value["fact_sha256"] = contract.digest(value)
            return value

        mutations = (
            lambda value: value.update(version="different"),
            lambda value: value.update(manifest_sha256="0" * 64),
            lambda value: value["identity"].update(candidate_id="OTHER"),
            lambda value: value["feature"].update(value=64),
            lambda value: value.update(candidate_match=None),
            lambda value: value["source_reasons"]["capture"].append("HIDDEN_REASON"),
            lambda value: value["feature"].update(raw_score=-70),
            lambda value: value["source"]["watch_snapshot"].update(cycle_id=""),
            lambda value: value["source"]["watch_snapshot"].update(
                durably_available_at_utc=value["source"]["watch_snapshot"]["available_at_utc"]
            ),
            lambda value: value["source"]["price_provenance"].update(
                hype_label_instrument="BAD"
            ),
            lambda value: value["source"]["price_provenance"].update(
                operational_identities=[],
                operational_identities_sha256=projection._watch_hash([]),
            ),
        )
        for index, mutation in enumerate(mutations):
            with self.subTest(index=index):
                with self.assertRaisesRegex(ValueError, "STAGE8_PROJECTED_FACT_INVALID"):
                    projection.validate_fact(
                        resigned(mutation),
                        expected_fact_sha256=original["fact_sha256"],
                        expected_watch_selection_attestation_sha256=
                        original["source"]["watch_selection_attestation_sha256"],
                        expected_watch_code_manifest_sha256=expected_code,
                    )

    def test_raw_anchor_identity_is_revalidated_and_attempt_id_is_attested(self):
        changed = deepcopy(self.attempt)
        changed["source_candle_open_utc"] += timedelta(minutes=30)
        fact = self.project(attempt=changed)
        self.assertEqual(fact["knowledge_status"], projection.UNKNOWN)
        self.assertIn("ANCHOR_AUTHORITY_NOT_VALID", fact["reasons"])

        changed_id = deepcopy(self.attempt)
        changed_id["attempt_id"] += 1000
        fact = self.project(
            attempt=changed_id,
            # Reusing the DB receipt for a different row identity must fail.
            attestation=_selection_attestation(self.attempt, self.snapshot),
        )
        self.assertEqual(fact["knowledge_status"], projection.UNKNOWN)
        self.assertIn("WATCH_SELECTION_ATTESTATION_INVALID", fact["reasons"])
        self.assertEqual(self.project()["identity"]["attempt_id"], self.attempt["attempt_id"])

    def test_selection_query_scope_and_full_receipt_are_revalidated(self):
        attestation = _selection_attestation(self.attempt, self.snapshot)
        known = self.project(attestation=attestation)
        self.assertEqual(
            known["source"]["watch_selection_attestation"], attestation
        )
        projection.validate_fact(
            known,
            expected_fact_sha256=known["fact_sha256"],
            expected_watch_selection_attestation_sha256=
            attestation["attestation_sha256"],
            expected_watch_code_manifest_sha256=
            known["source"]["watch_code_manifest_sha256"],
        )

        for mutation in ("query_hash", "query_scope", "high_water", "population"):
            changed = deepcopy(attestation)
            if mutation == "query_hash":
                changed["query_binding_sha256"] = "f" * 64
            elif mutation == "query_scope":
                changed["query_scope"]["symbol"] = "ETH"
                changed["query_binding_sha256"] = contract.digest(changed["query_scope"])
            elif mutation == "high_water":
                changed["query_scope"]["archive_snapshot_high_water_id"] = 1
                changed["query_binding_sha256"] = contract.digest(changed["query_scope"])
            else:
                changed["population_complete"] = False
            changed.pop("attestation_sha256")
            changed["attestation_sha256"] = contract.digest(changed)
            with self.subTest(mutation=mutation):
                fact = self.project(attestation=changed)
                self.assertEqual(fact["knowledge_status"], projection.UNKNOWN)
                self.assertIn("WATCH_SELECTION_ATTESTATION_INVALID", fact["reasons"])

    def test_thin_selection_reference_requires_trusted_code_generation(self):
        fact = self.project()
        attestation = fact["source"]["watch_selection_attestation"]
        thin_snapshot = fact["source"]["watch_snapshot"]
        code_manifest = fact["source"]["watch_code_sha256"]
        code_digest = projection._watch_hash(code_manifest)

        with self.assertRaisesRegex(
                ValueError, "STAGE8_WATCH_SELECTION_ATTESTATION_INVALID"):
            projection.validate_watch_selection_attestation(
                attestation, attempt=self.attempt,
                selected_snapshot=thin_snapshot,
                selected_code_manifest=code_manifest,
            )
        projection.validate_watch_selection_attestation(
            attestation, attempt=self.attempt,
            selected_snapshot=thin_snapshot,
            selected_code_manifest=code_manifest,
            expected_watch_code_manifest_sha256=code_digest,
        )

        forged_manifest = {key: "d" * 64 for key in code_manifest}
        with self.assertRaisesRegex(
                ValueError, "STAGE8_WATCH_SELECTION_ATTESTATION_INVALID"):
            projection.validate_watch_selection_attestation(
                attestation, attempt=self.attempt,
                selected_snapshot=thin_snapshot,
                selected_code_manifest=forged_manifest,
                expected_watch_code_manifest_sha256=code_digest,
            )

    def test_watch_scoring_code_generation_is_explicit_and_fact_bound(self):
        first = self.project()
        changed = deepcopy(self.snapshot)
        block = _block(changed)
        block["code_sha256"]["alert_engine.py"] = "e" * 64
        _rehash(changed)
        second = self.project(changed)
        self.assertEqual(second["knowledge_status"], projection.KNOWN, second)
        self.assertNotEqual(
            first["source"]["watch_code_manifest_sha256"],
            second["source"]["watch_code_manifest_sha256"],
        )
        self.assertEqual(
            second["source"]["score_generation_status"],
            "OBSERVED_REQUIRES_DURABLE_FREEZE_BINDING",
        )
        with self.assertRaisesRegex(ValueError, "STAGE8_PROJECTED_FACT_INVALID"):
            projection.validate_fact(
                second,
                expected_fact_sha256=second["fact_sha256"],
                expected_watch_selection_attestation_sha256=
                second["source"]["watch_selection_attestation_sha256"],
            )
        with self.assertRaisesRegex(ValueError, "STAGE8_PROJECTED_FACT_INVALID"):
            projection.validate_fact(
                second,
                expected_fact_sha256=second["fact_sha256"],
                expected_watch_selection_attestation_sha256=
                second["source"]["watch_selection_attestation_sha256"],
                expected_watch_code_manifest_sha256=
                first["source"]["watch_code_manifest_sha256"],
            )
        projection.validate_fact(
            second,
            expected_fact_sha256=second["fact_sha256"],
            expected_watch_selection_attestation_sha256=
            second["source"]["watch_selection_attestation_sha256"],
            expected_watch_code_manifest_sha256=
            second["source"]["watch_code_manifest_sha256"],
        )

    def test_out_of_scope_does_not_hide_broken_anchor_authority(self):
        binding = contract.exact_binding(
            scope_id="BINANCE_ETH",
            candidate_id="FUTURES_FLOW_ALIGNED65_SHORT",
            threshold_bps=50,
        )
        broken_slot = deepcopy(self.slot)
        broken_slot["input_fingerprint"] = "0" * 64
        fact = self.project(binding=binding, slot=broken_slot)
        self.assertEqual(fact["knowledge_status"], projection.UNKNOWN)
        self.assertNotEqual(fact["applicability_status"], projection.NOT_APPLICABLE)
        self.assertIn("ANCHOR_AUTHORITY_NOT_VALID", fact["reasons"])

        fact = self.project(binding=binding, events=self.events[:1])
        self.assertEqual(fact["knowledge_status"], projection.UNKNOWN)
        self.assertIn("ANCHOR_AUTHORITY_NOT_VALID", fact["reasons"])

    def test_first_tranche_scope_resolution_requires_valid_anchor_authority(self):
        cases = []

        broken_slot = deepcopy(self.slot)
        broken_slot["input_fingerprint"] = "0" * 64
        cases.append(("corrupt-slot", self.attempt, broken_slot, self.events))

        changed_symbol = deepcopy(self.attempt)
        changed_symbol["symbol"] = "ETH"
        cases.append(("unbound-symbol", changed_symbol, self.slot, self.events))

        invalid_id = deepcopy(self.attempt)
        invalid_id["attempt_id"] = 0
        cases.append(("invalid-attempt-id", invalid_id, self.slot, self.events))

        wrong_sampler = deepcopy(self.attempt)
        wrong_sampler["sampler_version"] = "formula-prospective-neutral-v3"
        cases.append(("sampler-mismatch", wrong_sampler, self.slot, self.events))

        for name, attempt, slot, events in cases:
            with self.subTest(name=name):
                tranche = projection.project_first_tranche(
                    attempt=attempt, anchor_slot=slot, anchor_events=events,
                    selected_watch_snapshot=self.snapshot,
                    watch_selection_attestation=_selection_attestation(
                        attempt, self.snapshot
                    ),
                )
                self.assertEqual(
                    tranche["scope_resolution_status"], projection.UNKNOWN,
                    tranche,
                )
                self.assertTrue(tranche["reasons"], tranche)
                self.assertIn("ANCHOR_AUTHORITY_NOT_VALID", tranche["reasons"])
                self.assertTrue(tranche["facts"], tranche)
                self.assertTrue(all(
                    fact["knowledge_status"] == projection.UNKNOWN
                    for fact in tranche["facts"]
                ), tranche)

    def test_malformed_validator_reason_shapes_fail_closed(self):
        with patch.object(source_audit, "validate_anchor_authority", return_value={
            "status": "VALID", "reasons": [123], "source_status": "EVALUABLE",
            "model_score_status": "ABSENT",
        }):
            fact = self.project()
        self.assertEqual(fact["knowledge_status"], projection.UNKNOWN)
        self.assertIn("ANCHOR_AUDIT_REASONS_INVALID", fact["reasons"])

        valid_capture = self.capture_result()
        valid_capture["reasons"] = [123]
        with patch.object(source_audit, "validate_capture", return_value=valid_capture):
            fact = self.project()
        self.assertEqual(fact["knowledge_status"], projection.UNKNOWN)
        self.assertIn("CAPTURE_AUDIT_REASONS_INVALID", fact["reasons"])

    def test_hype_operational_quote_is_never_relabelled_as_spot_107(self):
        rows = inputs()
        for row in rows:
            if row["symbol"] == "HYPE":
                row.update(
                    price_source="bybit_futures", price_pair="HYPEUSDT",
                    price_market="PERP", price_instrument="HYPEUSDT",
                )
        snapshot = _set_model(
            _fresh(_snapshot(rows=rows)), symbol="HYPE", score=-70,
            direction="BEARISH",
        )
        attempt, slot, events = _anchor(symbol="HYPE")
        authority = source_audit.validate_anchor_authority(attempt, slot, events)
        binding = contract.exact_binding(
            scope_id="HYPE_SPOT_107",
            candidate_id="FUTURES_FLOW_ALIGNED65_SHORT",
            threshold_bps=25,
        )
        fact = projection.project_from_watch_snapshots(
            attempt=attempt, anchor_slot=slot, anchor_events=events,
            watch_snapshots=[snapshot], binding=binding,
        )
        self.assertEqual(fact["knowledge_status"], projection.UNKNOWN, fact)
        self.assertIn("WATCH_SELECTION_ATTESTATION_INVALID", fact["reasons"])
        fact = projection.project_binding_fact(
            attempt=attempt, anchor_slot=slot, anchor_events=events,
            selected_watch_snapshot=snapshot,
            watch_selection_attestation=_selection_attestation(attempt, snapshot),
            binding=binding,
        )
        self.assertEqual(fact["knowledge_status"], projection.KNOWN, fact)
        provenance = fact["source"]["price_provenance"]
        self.assertEqual(provenance["declared_label_price_route"], "HYPERLIQUID_SPOT_@107_1M")
        self.assertEqual(provenance["label_route_status"], "DECLARATION_ONLY_NOT_AUDITED")
        self.assertEqual(provenance["hype_label_instrument"], "@107")
        self.assertIs(provenance["hype_operational_route_relabelled_to_spot"], False)
        self.assertEqual(
            {row["price_source"] for row in provenance["operational_identities"]},
            {"bybit_futures"},
        )
        self.assertEqual(
            {row["price_market"] for row in provenance["operational_identities"]},
            {"PERP"},
        )

    def test_out_of_scope_is_not_a_negative_observation(self):
        binding = contract.exact_binding(
            scope_id="BINANCE_ETH",
            candidate_id="FUTURES_FLOW_ALIGNED65_SHORT",
            threshold_bps=50,
        )
        fact = projection.project_binding_fact(
            attempt=self.attempt, anchor_slot=self.slot, anchor_events=self.events,
            selected_watch_snapshot=self.snapshot,
            watch_selection_attestation=_selection_attestation(self.attempt, self.snapshot),
            binding=binding,
        )
        self.assertEqual(fact["applicability_status"], projection.NOT_APPLICABLE)
        self.assertEqual(fact["knowledge_status"], projection.KNOWN)
        self.assertIsNone(fact["candidate_match"])
        self.assertEqual(fact["reasons"], ["SYMBOL_OUTSIDE_FROZEN_SCOPE"])
        with self.assertRaisesRegex(ValueError, "STAGE8_PROJECTED_FACT_INVALID"):
            projection.validate_fact(fact)
        projection.validate_fact(
            fact, expected_fact_sha256=fact["fact_sha256"]
        )

        # A coherent self-rehash cannot replace the independently frozen fact.
        forged = deepcopy(fact)
        forged["identity"].update(
            attempt_id=None, anchor_slot_id=None, event_id=None,
            source_candle_open_utc=None, decision_time_utc=None,
        )
        forged.pop("fact_sha256")
        forged["fact_sha256"] = contract.digest(forged)
        with self.assertRaisesRegex(ValueError, "STAGE8_PROJECTED_FACT_INVALID"):
            projection.validate_fact(
                forged, expected_fact_sha256=fact["fact_sha256"]
            )
        with self.assertRaisesRegex(ValueError, "STAGE8_PROJECTED_FACT_INVALID"):
            projection.validate_fact(
                forged, expected_fact_sha256=forged["fact_sha256"]
            )

        def assert_structural_forgery_rejected(mutator):
            changed = deepcopy(fact)
            mutator(changed)
            changed.pop("fact_sha256")
            changed["fact_sha256"] = contract.digest(changed)
            with self.assertRaisesRegex(
                    ValueError, "STAGE8_PROJECTED_FACT_INVALID"):
                projection.validate_fact(
                    changed, expected_fact_sha256=changed["fact_sha256"]
                )

        assert_structural_forgery_rejected(
            lambda changed: changed["identity"].update(symbol="FAKE")
        )
        assert_structural_forgery_rejected(
            lambda changed: changed["source_reasons"]["anchor"].append(
                "SYMBOL_OUTSIDE_FROZEN_SCOPE"
            )
        )
        assert_structural_forgery_rejected(
            lambda changed: changed["identity"].update(
                source_candle_open_utc=changed["identity"]["decision_time_utc"]
            )
        )

    def test_malformed_empty_record_is_unknown_not_empty_or_out_of_scope(self):
        fact = projection.project_binding_fact(
            attempt={}, anchor_slot=None, anchor_events=[], selected_watch_snapshot=None,
            watch_selection_attestation={},
            binding=self.binding,
        )
        self.assertEqual(fact["applicability_status"], projection.UNKNOWN)
        self.assertEqual(fact["knowledge_status"], projection.UNKNOWN)
        self.assertIsNone(fact["candidate_match"])
        self.assertEqual(fact["reasons"], ["AUDIT_ATTEMPT_IDENTITY_INVALID"])
        tranche = projection.project_first_tranche(
            attempt={}, anchor_slot=None, anchor_events=[], selected_watch_snapshot=None,
            watch_selection_attestation={},
        )
        self.assertEqual(tranche["scope_resolution_status"], projection.UNKNOWN)
        self.assertIn("AUDIT_ATTEMPT_IDENTITY_INVALID", tranche["reasons"])
        self.assertIn("ANCHOR_AUTHORITY_NOT_VALID", tranche["reasons"])
        self.assertEqual(tranche["fact_count"], 0)


if __name__ == "__main__":
    unittest.main()
