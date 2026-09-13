"""Network/DB-free mutation and identity tests for the first Stage-8 tranche."""
from __future__ import annotations

import ast
from copy import deepcopy
import json
from pathlib import Path
import unittest
from unittest.mock import patch

import research_stage8_contract as contract


class FrozenContractTests(unittest.TestCase):
    def setUp(self):
        self.manifest = contract.frozen_manifest()

    def test_frozen_digest_is_literal_and_exact(self):
        self.assertEqual(contract.MANIFEST_SHA256,
                         "cb8f23b6cfbefa637fec18f47200c7432ed106bdaa7bc85374cab2b81b5ba262")
        self.assertEqual(contract.digest(self.manifest), contract.MANIFEST_SHA256)
        contract.validate_manifest(self.manifest)

    def test_independent_copies_cannot_mutate_frozen_definition(self):
        self.manifest["candidates"]["scopes"][0]["symbols"].append("HYPE")
        self.assertEqual(contract.frozen_manifest()["candidates"]["scopes"][0]["symbols"], ["BTC"])
        with self.assertRaises(ValueError):
            contract.validate_manifest(self.manifest)

    def test_json_round_trip_and_dictionary_order_preserve_identity(self):
        reordered = dict(reversed(list(self.manifest.items())))
        contract.validate_manifest(reordered)
        self.assertEqual(contract.digest(reordered), contract.MANIFEST_SHA256)

    def test_non_json_and_nonfinite_rejected(self):
        for value in ((1, 2), {1: "number key"}, {"value": object()},
                      {"value": float("nan")}, {"value": float("inf")}):
            with self.subTest(value=repr(value)), self.assertRaises(ValueError):
                contract.canonical(value)

    def test_postgresql_numeric_canonical_vectors_and_sha256(self):
        vectors = (
            (-0.0, "0.0", "8aed642bf5118b9d3c859bd4be35ecac75b6e873cce34e7b6f554b06f75550d7"),
            (1e-6, "0.000001", "159fb29a827ad04b260aa6c8ab6d8637f8f2b38af5c4f3cb49d6a21205e040f8"),
            (1e20, "100000000000000000000", "c344e9487bfbd5c4e03c9fb90d62a5dde5e00b54d55c46e9f4a803aea162b80c"),
            (1.0, "1.0", "d0ff5974b6aa52cf562bea5921840c032a860a91a3512f7fe8f768f6bbe005f6"),
            (-2.5e-7, "-0.00000025", "5238ecbc315d7a0dc35548ae7baa539499aeb98b7f0ca9bcc9784bb5c4f66836"),
        )
        for value, expected, expected_sha256 in vectors:
            with self.subTest(value=value):
                self.assertEqual(contract.canonical(value), expected)
                self.assertEqual(contract.digest(value), expected_sha256)
                self.assertEqual(contract.canonical(json.loads(expected)), expected)

    def test_nested_unicode_escape_and_sorted_key_canonical_vector(self):
        value = {"é": "שלום 😀", "a": [-0.0, 1e-6, 1e20, True, False, None],
                 "esc": 'quote" slash\\ newline\n tab\t'}
        expected = ('{"a":[0.0,0.000001,100000000000000000000,true,false,null],'
                    '"esc":"quote\\" slash\\\\ newline\\n tab\\t","é":"שלום 😀"}')
        self.assertEqual(contract.canonical(value), expected)
        self.assertEqual(contract.digest(value),
                         "f9faca74d5720041d04d883d6c06ae1f5496656df9032a89d538ee01d970cc93")
        self.assertEqual(contract.canonical(json.loads(expected)), expected)
        self.assertEqual(contract.canonical({"😀": 1, "é": 2, "a": 3}),
                         '{"a":3,"é":2,"😀":1}')

    def test_finite_extremes_expand_without_binary_decimal_artifacts(self):
        self.assertEqual(contract.canonical(5e-324), "0." + "0" * 323 + "5")
        self.assertEqual(contract.canonical(1.7976931348623157e308),
                         "17976931348623157" + "0" * 292)
        self.assertEqual(contract.canonical(0.1), "0.1")
        self.assertEqual(contract.canonical(-0.0), contract.canonical(0.0))
        self.assertEqual(contract.canonical([True, 1, 1.0, False, 0, 0.0]),
                         "[true,1,1.0,false,0,0.0]")

    def test_numeric_subclasses_and_nested_nonfinite_cannot_coerce(self):
        class IntegerAlias(int):
            pass

        class FloatAlias(float):
            pass

        for value in (IntegerAlias(1), FloatAlias(1.0),
                      {"a": [float("-inf")]}, {True: "boolean-key"}):
            with self.subTest(value=repr(value)), self.assertRaises(ValueError):
                contract.canonical(value)

    def test_circular_containers_rejected_but_shared_values_allowed(self):
        cyclic_list = []
        cyclic_list.append(cyclic_list)
        cyclic_dict = {}
        cyclic_dict["itself"] = cyclic_dict
        for value in (cyclic_list, cyclic_dict):
            with self.assertRaisesRegex(ValueError, "circular"):
                contract.canonical(value)
        shared = {"number": 1e-6}
        self.assertEqual(contract.canonical([shared, shared]),
                         '[{"number":0.000001},{"number":0.000001}]')

    def test_critical_nested_mutations_rejected(self):
        changes = [
            ("source", "anchor_model_score_status", "PRESENT"),
            ("source", "max_capture_age_seconds", 301),
            ("source", "capture_fallback_to_older_valid", True),
            ("projection", "storage", "REWRITE_ANCHOR"),
            ("projection", "missing_value", 0),
            ("labels", "window_minutes", 240),
            ("labels", "first_touch_truncated_excursions_allowed_for_asymmetry", True),
            ("independence", "unit", "event_id"),
            ("independence", "pool_cross_symbol_votes_per_parent", 2),
            ("acceptance", "minimum_distinct_parents_per_passing_route", 3),
            ("acceptance", "combination", "PROBABILITY_AND_ASYMMETRY"),
            ("acceptance", "fresh_three_parent_route", True),
            ("acceptance", "unavailable_other_route_blocks_passing_route", True),
            ("prospective_registration", "local_definition_is_durable_registry_freeze", True),
            ("runtime", "telegram", True),
        ]
        for section, key, value in changes:
            changed = deepcopy(self.manifest)
            changed[section][key] = value
            with self.subTest(section=section, key=key), self.assertRaises(ValueError):
                contract.validate_manifest(changed)

    def test_bool_and_float_are_not_frozen_integer_aliases(self):
        for value in (True, 5.0):
            changed = deepcopy(self.manifest)
            changed["acceptance"]["minimum_distinct_parents_per_passing_route"] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                contract.validate_manifest(changed)

    def test_extra_fields_and_recomputed_hash_do_not_authorize_change(self):
        self.manifest["acceptance"]["minimum_distinct_parents_per_passing_route"] = 3
        self.manifest["manifest_sha256"] = contract.digest(self.manifest)
        with self.assertRaises(ValueError):
            contract.validate_manifest(self.manifest)

    def test_atomic_five_plus_or_without_fresh_exception(self):
        policy = self.manifest["acceptance"]
        self.assertEqual(policy["minimum_distinct_parents_per_passing_route"], 5)
        self.assertEqual(policy["combination"], "PROBABILITY_OR_ASYMMETRY")
        self.assertEqual(policy["atomic_expression"],
                         "N_ROUTE_DISTINCT_BTC_PARENTS >= 5 AND (PROBABILITY OR ASYMMETRY)")
        self.assertFalse(policy["fresh_three_parent_route"])
        self.assertFalse(policy["unavailable_other_route_blocks_passing_route"])
        self.assertTrue(policy["shared_coverage_or_provenance_failure_blocks_all_routes"])

    def test_predeclared_numerical_routes(self):
        policy = self.manifest["acceptance"]
        self.assertEqual(policy["probability"], {"hit_rate_pct_gte": 70, "wilson_95_lower_pct_gte": 40})
        self.assertEqual(policy["asymmetry"], {"common_window_asymmetry_ratio_gte": 1.5,
                         "common_window_favorable_dominance_pct_gte": 60,
                         "common_window_median_paired_edge_pct_gt": 0})

    def test_metric_formula_and_zero_denominator_definitions_frozen(self):
        metrics = self.manifest["acceptance"]["metric_definitions"]
        self.assertEqual(metrics["wilson_z"], 1.959963984540054)
        self.assertEqual(metrics["common_window_asymmetry_ratio"],
                         "sum(representative_mfe_pct)/sum(representative_mae_pct)")
        self.assertEqual(metrics["positive_mfe_zero_mae"],
                         "ZERO_DENOMINATOR_RATIO_NULL_ASYMMETRY_ROUTE_UNAVAILABLE")
        self.assertFalse(metrics["json_nonfinite_allowed"])

    def test_manifest_is_not_qualification_or_prospective_clock(self):
        self.assertEqual(self.manifest["definition_state"], "LOCAL_DEFINITION_ONLY")
        self.assertTrue(all(value is False for value in self.manifest["runtime"].values()))
        registration = self.manifest["prospective_registration"]
        self.assertFalse(registration["implemented"])
        self.assertFalse(registration["local_definition_is_durable_registry_freeze"])
        self.assertEqual(registration["eligible_parent_rule"], "PARENT_START_STRICTLY_AFTER_REAL_DURABLE_FREEZE")

    def test_first_tranche_not_full_stage8_catalog(self):
        catalog = self.manifest["candidates"]
        self.assertEqual(catalog["scope_of_work"],
                         "FIRST_FIXED_MODEL_ONLY_VERTICAL_TRANCHE_NOT_COMPLETE_STAGE8_CATALOG")
        self.assertEqual(len(catalog["definitions"]), 6)
        self.assertEqual(len(catalog["scopes"]), 9)
        self.assertEqual(catalog["family_capacity"], 432)
        self.assertTrue(catalog["scope_expansion_requires_new_version_and_freeze"])

    def test_all_existing_thresholds_fixed_at_60m(self):
        self.assertEqual(self.manifest["labels"]["window_minutes"], 60)
        self.assertEqual(self.manifest["labels"]["thresholds_bps"], list(range(25, 201, 25)))
        self.assertFalse(self.manifest["labels"]["alternative_window_or_threshold_fallback"])

    def test_hype_separate_scope_never_dynamic_binance_filter(self):
        scopes = {scope["scope_id"]: scope for scope in self.manifest["candidates"]["scopes"]}
        self.assertEqual(scopes["ALL_BINANCE7"]["symbols"], list(contract.BINANCE_SYMBOLS))
        self.assertNotIn("HYPE", scopes["ALL_BINANCE7"]["symbols"])
        self.assertEqual(scopes["HYPE_SPOT_107"]["symbols"], ["HYPE"])
        self.assertNotIn("ALL", scopes)
        self.assertFalse(self.manifest["labels"]["hype_operational_route_relabel_to_spot"])

    def test_v4_and_watch_sidecar_remain_separate(self):
        self.assertEqual(self.manifest["source"]["sampler_version"],
                         "prospective-neutral-anchor-v4-decision-features-frozen")
        self.assertEqual(self.manifest["source"]["watch_version"],
                         "watch-operational-scores-v2")
        self.assertEqual(self.manifest["labels"]["method_version"],
                         "ordered-first-touch-v7")
        self.assertEqual(self.manifest["independence"]["parent_policy_version"],
                         "btc-parent-close-reversal-200bps-v1")
        self.assertEqual(self.manifest["source"]["anchor_model_score_status"], "ABSENT")
        self.assertEqual(self.manifest["projection"]["storage"], "SEPARATE_SIDECAR_NO_ANCHOR_OR_EVENT_REWRITE")
        self.assertFalse(self.manifest["source"]["model_unavailable_fallback_zero_is_observed"])

    def test_maxpain_source_semantics_not_first_formula_predicates(self):
        maxpain = self.manifest["source"]["maxpain"]
        self.assertEqual(maxpain["liquidated_side_to_candidate_direction"], {"SHORT": "LONG", "LONG": "SHORT"})
        self.assertEqual(len(maxpain["additive_components"]), 4)
        self.assertFalse(maxpain["selected_is_delivery_or_signal"])
        self.assertFalse(maxpain["first_tranche_predicates"])
        self.assertFalse(maxpain["missing_amount_is_zero"])

    def test_raw_status_and_reported_state_are_distinct(self):
        labels = self.manifest["labels"]
        self.assertIn("UNRESOLVED", labels["persisted_statuses"])
        self.assertNotIn("AMBIGUOUS", labels["persisted_statuses"])
        self.assertIn("AMBIGUOUS", labels["reported_nondecisive_states"])
        self.assertTrue(labels["preserve_raw_status_and_terminal_reason"])
        self.assertFalse(labels["first_touch_truncated_excursions_allowed_for_asymmetry"])

    def test_representative_selection_does_not_favor_completed_labels(self):
        rule = self.manifest["independence"]
        self.assertEqual(rule["representative_order_asc"],
                         ["decision_time_utc", "symbol", "anchor_slot_id", "event_id"])
        self.assertFalse(rule["replace_unknown_representative_with_later_decisive"])
        self.assertFalse(rule["statistical_independence_claim"])

    def test_all_432_exact_bindings_unique(self):
        identities = set()
        for scope in self.manifest["candidates"]["scopes"]:
            for candidate in self.manifest["candidates"]["definitions"]:
                for threshold in contract.THRESHOLDS_BPS:
                    value = contract.exact_binding(scope_id=scope["scope_id"],
                        candidate_id=candidate["candidate_id"], threshold_bps=threshold)
                    contract.validate_exact_binding(value)
                    identities.add(value["binding_sha256"])
        self.assertEqual(len(identities), 432)

    def test_rehashed_binding_mutation_rejected(self):
        value = contract.exact_binding(scope_id="ALL_BINANCE7",
            candidate_id="POSITIONING_ALIGNED65_LONG", threshold_bps=50)
        value["binding"]["scope"]["symbols"].remove("BTC")
        value["binding_sha256"] = contract.digest(value["binding"])
        with self.assertRaises(ValueError):
            contract.validate_exact_binding(value)

    def test_binding_rejects_unfrozen_axes(self):
        base = {"scope_id": "BINANCE_BTC", "candidate_id": "POSITIONING_ALIGNED65_LONG", "threshold_bps": 50}
        for update in ({"scope_id": "ALL"}, {"candidate_id": "MAXPAIN"},
                       {"threshold_bps": 51}, {"threshold_bps": 50.0},
                       {"threshold_bps": True}, {"scope_id": []}):
            with self.subTest(update=update), self.assertRaises(ValueError):
                contract.exact_binding(**{**base, **update})

    def test_binding_invalid_shape_rejected(self):
        for value in ({}, {"binding": None}, {"binding": {"scope": []}},
                      {"binding": {"scope": {"scope_id": "BINANCE_BTC"}}}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                contract.validate_exact_binding(value)

    def test_exported_constants_cannot_change_hashed_binding_axes(self):
        arguments = {"scope_id": "BINANCE_BTC", "candidate_id": "POSITIONING_ALIGNED65_LONG", "threshold_bps": 50}
        expected = contract.exact_binding(**arguments)
        with patch.object(contract, "THRESHOLDS_BPS", (999,)), \
                patch.object(contract, "ACCEPTANCE_VERSION", "old-and-policy"), \
                patch.object(contract, "VERSION", "changed"):
            self.assertEqual(contract.exact_binding(**arguments), expected)
            with self.assertRaises(ValueError):
                contract.exact_binding(**{**arguments, "threshold_bps": 999})

    def test_module_imports_only_standard_library_no_runtime(self):
        tree = ast.parse(Path(contract.__file__).read_text(encoding="utf-8"))
        imports = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
        imports |= {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
        self.assertEqual(imports, {"__future__", "decimal", "hashlib", "json", "typing"})
        names = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
        self.assertFalse(names & {"open", "exec", "eval", "connect", "requests", "socket"})


if __name__ == "__main__":
    unittest.main()
