"""Frozen catalog-grid denominator and pre-registration boundary regressions."""
from copy import deepcopy
from datetime import timedelta
import hashlib
from pathlib import Path
import unittest
from unittest.mock import patch

import research_no_horizon_calendar as calendar
import research_no_horizon_cohort as cohort
import research_no_horizon_contract as contracts
import research_no_horizon_discovery as discovery
import research_no_horizon_manifest as manifest
import research_no_horizon_ranking as ranking
import research_watch_scan_formula as formulas

_FAKE_CALENDAR = {
    "planner_version": calendar.VERSION, "planner_sha256": "a" * 64,
    "acquisition": {"version": "test-acquisition", "files": {"one.py": "b" * 64}},
}
_FAKE_RANKING = {"version": "test-ranking", "ranking_sha256": "c" * 64,
                 "policy": "fixed-test-policy"}


def declaration():
    return {"cohort_version": cohort.VERSION, "cohort_key": "synthetic-discovery-template",
        "declared_at_utc": "2026-01-01T00:00:00+00:00", "prior_outcomes_observed": False,
        "symbol": "BTC", "price_route": manifest.ROUTE,
        "source_start_utc": "2026-01-02T00:00:00+00:00",
        "source_end_utc": "2026-01-03T00:00:00+00:00",
        "cutoff_utc": "2026-01-04T08:00:00+00:00",
        "scopes": [{"candidate_key": "FUTURES_CVD_TOTAL_65",
                    "base_direction": "SHORT", "threshold_pct": .25}],
        "parts": [{"ordinal": 0, "source_start_utc": "2026-01-02T00:00:00+00:00",
                   "source_end_utc": "2026-01-03T00:00:00+00:00"}]}


class Backend:
    def __init__(self):
        self.versions = deepcopy(_FAKE_CALENDAR["acquisition"])
        self.now = "2026-01-01T12:00:00+00:00"
        self.rows, self.calls = {}, []
        self.fail_call = None

    def register_request(self, frozen, *, request_key=None, require_before_start=False):
        self.calls.append((deepcopy(frozen), request_key, require_before_start))
        if self.fail_call == len(self.calls):
            raise ConnectionError("synthetic interrupted registration")
        request_id = contracts.digest({"declaration": frozen, "implementation": self.versions})
        existing = self.rows.get(request_id)
        created = existing["created_at_utc"] if existing else self.now
        before = contracts.utc(created) <= contracts.utc(frozen["source_start_utc"])
        if require_before_start and not before:
            raise ValueError("REGISTRATION_AFTER_SOURCE_START")
        self.rows.setdefault(request_id, {"request_id": request_id,
            "declaration_sha256": contracts.digest(frozen), "created_at_utc": created,
            "source_start_utc": frozen["source_start_utc"],
            "registered_before_source_start": before})
        return request_id

    def registration_info(self, request_id):
        return deepcopy(self.rows[request_id])


class DiscoveryTests(unittest.TestCase):
    def setUp(self):
        for module, value in ((calendar, _FAKE_CALENDAR), (ranking, _FAKE_RANKING)):
            patcher = patch.object(module, "implementation",
                                   side_effect=lambda frozen=value: deepcopy(frozen))
            patcher.start()
            self.addCleanup(patcher.stop)

    def plan(self, **kwargs):
        first = kwargs.pop("first_declaration", declaration())
        kwargs.setdefault("base_directions", ["LONG"])
        kwargs.setdefault("thresholds_pct", [.25])
        return discovery.build_plan(first, **kwargs)

    def test_full_catalog_denominator_and_unsupported_definitions_are_visible(self):
        plan = self.plan()
        records = plan["catalog_manifest"]
        self.assertEqual(len(records), 298)
        self.assertEqual(sum(row["supported"] for row in records), 34)
        self.assertEqual(sum(row["selected"] for row in records), 34)
        self.assertEqual(plan["selection_mode"], "ALL_SUPPORTED")
        self.assertEqual(plan["summary"]["unsupported_candidates"], 264)
        self.assertEqual(plan["summary"]["omitted_supported_candidates"], 0)
        self.assertEqual(plan["summary"]["scopes_per_window"], 34)
        self.assertTrue(all(row["supported"] or
                            (row["unsupported_features"] and not row["selected"]) for row in records))

    def test_catalog_aliases_remain_distinct_without_independence_claim(self):
        plan = self.plan()
        original = "PRICE_OI_TOTAL_65"
        alias = "captured-question-search-v3-experimental-binding:CORE_PRICE_OI_TOTAL_65"
        chosen = {row["candidate_key"]: row for row in plan["catalog_manifest"] if row["selected"]}
        self.assertIn(original, chosen)
        self.assertIn(alias, chosen)
        self.assertNotEqual(chosen[original]["definition_sha256"], chosen[alias]["definition_sha256"])
        self.assertEqual(sum(row["orientation"] == "INVERSE" for row in chosen.values()), 17)
        self.assertIs(plan["catalog_entries_are_independent_strategies"], False)

    def test_oversized_grid_rejects_instead_of_splitting_or_truncating(self):
        for directions, thresholds in ((["LONG", "SHORT"], [.25]), (["LONG"], [.25, .5])):
            with self.subTest(directions=directions, thresholds=thresholds):
                with self.assertRaisesRegex(ValueError, "GRID_EXCEEDS"):
                    self.plan(base_directions=directions, thresholds_pct=thresholds)
        supported = [row["candidate_key"] for row in formulas.catalog_records() if row["supported"]]
        plan = self.plan(candidate_keys=supported[:32], base_directions=["SHORT", "LONG"])
        self.assertEqual(plan["summary"]["scopes_per_window"], 64)
        self.assertEqual(len(plan["calendar_plan"]["windows"]), 1)

    def test_explicit_subset_retains_all_omitted_and_unsupported_entries(self):
        plan = self.plan(candidate_keys=["PRICE_OI_TOTAL_65"], base_directions=["SHORT", "LONG"],
                         thresholds_pct=[.5, .25], window_count=2)
        self.assertEqual(plan["selection_mode"], "EXPLICIT_SUBSET")
        self.assertEqual(len(plan["catalog_manifest"]), 298)
        self.assertEqual(plan["summary"]["selected_candidates"], 1)
        self.assertEqual(plan["summary"]["omitted_supported_candidates"], 33)
        self.assertEqual(plan["summary"]["total_declared_scopes"], 8)
        self.assertEqual(plan["summary"]["scopes_per_window"], 4)

    def test_decision_budget_cap_is_exposed_without_changing_declared_resource_caps(self):
        first = declaration()
        before = cohort.normalize_declaration(first)
        plan = self.plan(first_declaration=first)
        self.assertEqual(plan["summary"]["max_source_rows_from_decision_budget"], 481)
        self.assertEqual(plan["summary"]["max_actual_source_rows_per_window"], 256)
        frozen = plan["calendar_plan"]["windows"][0]["declaration"]
        self.assertEqual(frozen["parts"], before["parts"])
        self.assertEqual(frozen["resource_budget"], before["resource_budget"])
        small = self.plan(candidate_keys=["PRICE_OI_TOTAL_65"])
        self.assertEqual(small["summary"]["max_source_rows_from_decision_budget"], 16384)
        self.assertEqual(small["summary"]["max_actual_source_rows_per_window"], 256)

    def test_grid_and_plan_deterministic_under_selection_order_and_numeric_normalization(self):
        args = {"candidate_keys": ["PRICE_OI_TOTAL_65", "FUTURES_CVD_TOTAL_65"],
                "base_directions": ["SHORT", "LONG"], "thresholds_pct": [1, .25]}
        first = self.plan(**args)
        reversed_args = {key: list(reversed(value)) for key, value in args.items()}
        second = self.plan(**reversed_args)
        self.assertEqual(first, second)
        self.assertEqual(discovery.validate_plan(first), first)
        self.assertEqual(first["plan_sha256"],
                         contracts.digest({key: value for key, value in first.items() if key != "plan_sha256"}))
        self.assertEqual(self.plan(thresholds_pct=[1]), self.plan(thresholds_pct=[1.0]))

    def test_explicit_all_selection_is_distinct_from_implicit_all_policy(self):
        automatic = self.plan()
        explicit = self.plan(candidate_keys=automatic["candidate_keys"])
        self.assertNotEqual(automatic["plan_sha256"], explicit["plan_sha256"])
        self.assertEqual(automatic["candidate_keys"], explicit["candidate_keys"])
        self.assertNotEqual(automatic["calendar_plan"]["plan_sha256"], explicit["calendar_plan"]["plan_sha256"])

    def test_original_template_unmodified_and_every_calendar_window_gets_new_identity(self):
        first = declaration()
        original = deepcopy(first)
        plan = self.plan(first_declaration=first, window_count=3)
        self.assertEqual(first, original)
        self.assertEqual(plan["template_declaration"], cohort.normalize_declaration(original))
        keys = [row["declaration"]["cohort_key"] for row in plan["calendar_plan"]["windows"]]
        self.assertEqual(len(set(keys)), 3)
        self.assertNotIn(first["cohort_key"], keys)
        self.assertTrue(all(len(key.encode()) <= 256 for key in keys))
        for ordinal, row in enumerate(plan["calendar_plan"]["windows"]):
            frozen = row["declaration"]
            self.assertEqual(len(frozen["scopes"]), 34)
            for key in ("source_start_utc", "source_end_utc", "cutoff_utc"):
                self.assertEqual(contracts.utc(frozen[key]),
                                 contracts.utc(first[key]) + timedelta(days=ordinal))

    def test_direction_threshold_and_candidate_choices_are_explicit_unique_and_bounded(self):
        bad_directions = ([], "LONG", ["long"], ["UP"], ["LONG", "LONG"], [True], None)
        for value in bad_directions:
            with self.subTest(directions=value), self.assertRaises(ValueError):
                self.plan(base_directions=value)
        for value in ([], .25, [0], [100], [-1], [True], [float("inf")], [float("nan")],
                      [.25, .25], [1, 1.0], list(range(1, 66)), None):
            with self.subTest(thresholds=value), self.assertRaises(ValueError):
                self.plan(thresholds_pct=value)
        unsupported = next(row["candidate_key"] for row in formulas.catalog_records() if not row["supported"])
        for value in ([], "PRICE_OI_TOTAL_65", ["missing"], [unsupported],
                      ["PRICE_OI_TOTAL_65", "PRICE_OI_TOTAL_65"], [True], [{}]):
            with self.subTest(candidates=value), self.assertRaises(ValueError):
                self.plan(candidate_keys=value)

    def test_existing_calendar_time_and_count_limits_are_preserved(self):
        for count in (0, 33, True, 1.0):
            with self.subTest(count=count), self.assertRaises(ValueError):
                self.plan(window_count=count)
        first = declaration()
        first["declared_at_utc"] = "2026-01-02T00:00:00.000001+00:00"
        with self.assertRaisesRegex(ValueError, "DECLARATION_AFTER"):
            self.plan(first_declaration=first)

    def test_rehashed_manifest_grid_calendar_and_authority_changes_reject_before_write(self):
        for mutation in ("manifest", "selected", "window", "omit_window", "scope",
                         "cap", "authority", "unknown", "mode", "implementation"):
            plan = self.plan(window_count=2)
            if mutation == "manifest":
                plan["catalog_manifest"][0]["definition_sha256"] = "d" * 64
            elif mutation == "selected":
                plan["candidate_keys"].pop()
            elif mutation == "window":
                plan["calendar_plan"]["windows"][-1]["declaration"]["cutoff_utc"] = "2026-01-20T00:00:00+00:00"
            elif mutation == "omit_window":
                plan["calendar_plan"]["windows"].pop()
            elif mutation == "scope":
                plan["calendar_plan"]["windows"][-1]["declaration"]["scopes"].pop()
            elif mutation == "cap":
                plan["summary"]["max_actual_source_rows_per_window"] += 1
            elif mutation == "authority":
                plan["runtime_authorized"] = True
            elif mutation == "unknown":
                plan["unknown"] = True
            elif mutation == "mode":
                plan["selection_mode"] = "anything"
            else:
                plan["implementation"]["ranking"]["ranking_sha256"] = "e" * 64
            plan["plan_sha256"] = contracts.digest({key: value for key, value in plan.items() if key != "plan_sha256"})
            backend = Backend()
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                discovery.register_plan(backend, plan)
            self.assertEqual(backend.calls, [])

    def test_current_ranking_policy_change_invalidates_plan_and_new_grid_identity(self):
        old = self.plan()
        changed = deepcopy(_FAKE_RANKING)
        changed["policy"] = "new-policy"
        with patch.object(ranking, "implementation", return_value=changed):
            with self.assertRaisesRegex(ValueError, "PLAN_OR_IMPLEMENTATION"):
                discovery.validate_plan(old)
            new = self.plan()
        self.assertNotEqual(old["plan_sha256"], new["plan_sha256"])
        self.assertNotEqual(old["calendar_plan"]["windows"][0]["declaration_sha256"],
                            new["calendar_plan"]["windows"][0]["declaration_sha256"])

    def test_backend_version_mismatch_rejects_before_registration(self):
        backend, plan = Backend(), self.plan()
        backend.versions["version"] = "different"
        with self.assertRaisesRegex(ValueError, "BACKEND_IMPLEMENTATION"):
            discovery.register_plan(backend, plan)
        self.assertEqual(backend.calls, [])

    def test_nested_registration_binds_full_search_and_is_idempotent(self):
        backend, plan = Backend(), self.plan(window_count=3)
        first = discovery.register_plan(backend, plan)
        self.assertEqual(discovery.register_plan(backend, plan), first)
        self.assertEqual(len(backend.rows), 3)
        self.assertEqual(first["plan_sha256"], plan["plan_sha256"])
        self.assertEqual(first["calendar_registration"]["plan_sha256"], plan["calendar_plan"]["plan_sha256"])
        self.assertEqual(first["calendar_plan_sha256"], plan["calendar_plan"]["plan_sha256"])
        self.assertEqual(first["receipt_sha256"],
                         contracts.digest({key: value for key, value in first.items() if key != "receipt_sha256"}))
        self.assertTrue(all(key is None and before is True for _, key, before in backend.calls))
        self.assertTrue(first["registration_complete"])

    def test_partial_registration_reuses_exact_windows_and_does_not_skip_late_window(self):
        backend, plan = Backend(), self.plan(window_count=3)
        backend.fail_call = 2
        with self.assertRaises(ConnectionError):
            discovery.register_plan(backend, plan)
        self.assertEqual(len(backend.rows), 1)
        backend.fail_call = None
        backend.now = "2026-01-03T00:00:00.000001+00:00"
        with self.assertRaisesRegex(ValueError, "AFTER_SOURCE_START"):
            discovery.register_plan(backend, plan)
        self.assertEqual(len(backend.rows), 1)

    def test_partial_registration_recovers_same_finite_population(self):
        backend, plan = Backend(), self.plan(window_count=3)
        backend.fail_call = 2
        with self.assertRaises(ConnectionError):
            discovery.register_plan(backend, plan)
        original_ids = set(backend.rows)
        backend.fail_call = None
        receipt = discovery.register_plan(backend, plan)
        self.assertTrue(receipt["registration_complete"])
        self.assertEqual(len(backend.rows), 3)
        self.assertTrue(original_ids <= set(backend.rows))

    def test_prior_knowledge_and_false_authority_are_preserved_without_outcome_reads(self):
        first = declaration()
        first["prior_outcomes_observed"] = True
        plan = self.plan(first_declaration=first)
        receipt = discovery.register_plan(Backend(), plan)
        self.assertTrue(all(row["declaration"]["prior_outcomes_observed"]
                            for row in plan["calendar_plan"]["windows"]))
        self.assertEqual(plan["source_reads_performed"], 0)
        self.assertEqual(plan["outcome_reads_performed"], 0)
        for value in (plan, receipt):
            for key in ("runtime_authorized", "telegram_authorized", "trading_authorized",
                        "is_prospective_formula_evidence", "validated_discovery",
                        "multiple_testing_adjusted", "scope_pooling",
                        "cross_window_outcome_pooling", "cross_window_parent_independence_proven",
                        "catalog_entries_are_independent_strategies"):
                self.assertIs(value[key], False)

    def test_implementation_binds_planner_bytes_calendar_and_ranking(self):
        current = discovery.implementation()
        self.assertEqual(current["planner_sha256"],
                         hashlib.sha256(Path(discovery.__file__).read_bytes()).hexdigest())
        self.assertEqual(current["ranking"], _FAKE_RANKING)
        self.assertEqual(current["calendar"], _FAKE_CALENDAR)


if __name__ == "__main__":
    unittest.main()
