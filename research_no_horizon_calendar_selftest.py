"""Calendar identity, time boundaries and interrupted-registration regressions."""
from copy import deepcopy
from datetime import timedelta
import hashlib
from pathlib import Path
import unittest
from unittest.mock import patch

import research_no_horizon_calendar as calendar
import research_no_horizon_cohort as cohort
import research_no_horizon_contract as contracts
import research_no_horizon_manifest as manifest

_REAL_IMPLEMENTATION = calendar.implementation
_FAKE_IMPLEMENTATION = {
    "planner_version": calendar.VERSION, "planner_sha256": "a" * 64,
    "acquisition": {"version": "test-acquisition", "files": {"one.py": "b" * 64}},
}


def declaration():
    return {"cohort_version": cohort.VERSION, "cohort_key": "synthetic-calendar",
        "declared_at_utc": "2026-01-01T00:00:00+00:00", "prior_outcomes_observed": False,
        "symbol": "BTC", "price_route": manifest.ROUTE,
        "source_start_utc": "2026-01-02T00:00:00+00:00",
        "source_end_utc": "2026-01-03T00:00:00+00:00",
        "cutoff_utc": "2026-01-04T08:00:00+00:00",
        "scopes": [{"candidate_key": "FUTURES_CVD_TOTAL_65",
                    "base_direction": "SHORT", "threshold_pct": .25}],
        "parts": [{"ordinal": 0, "source_start_utc": "2026-01-02T00:00:00+00:00",
                   "source_end_utc": "2026-01-02T12:00:00+00:00"},
                  {"ordinal": 1, "source_start_utc": "2026-01-02T12:00:00+00:00",
                   "source_end_utc": "2026-01-03T00:00:00+00:00"}]}


class Backend:
    def __init__(self):
        self.versions = deepcopy(_FAKE_IMPLEMENTATION["acquisition"])
        self.now = "2026-01-01T12:00:00+00:00"
        self.rows, self.calls = {}, []
        self.fail_call = None

    def register_request(self, frozen, *, request_key=None, require_before_start=False):
        self.calls.append((deepcopy(frozen), request_key, require_before_start))
        if self.fail_call == len(self.calls):
            raise ConnectionError("synthetic registration interruption")
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


class CalendarTests(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(calendar, "implementation",
                               side_effect=lambda: deepcopy(_FAKE_IMPLEMENTATION))
        patcher.start()
        self.addCleanup(patcher.stop)

    def plan(self, count=3, first=None):
        return calendar.build_plan(first or declaration(), window_count=count)

    def test_deterministic_finite_plan_does_not_mutate_template(self):
        first = declaration()
        original = deepcopy(first)
        plan = self.plan(first=first)
        self.assertEqual(first, original)
        self.assertEqual(plan, self.plan(first=first))
        self.assertEqual(calendar.validate_plan(plan), plan)
        self.assertEqual(len(plan["windows"]), 3)
        self.assertEqual(plan["plan_sha256"],
                         contracts.digest({k: v for k, v in plan.items() if k != "plan_sha256"}))

    def test_exact_utc_cadence_shifts_parts_and_cutoff_not_declaration(self):
        plan = self.plan()
        first = cohort.normalize_declaration(declaration())
        for ordinal, window in enumerate(plan["windows"]):
            frozen = window["declaration"]
            self.assertEqual(window["ordinal"], ordinal)
            self.assertEqual(window["declaration_sha256"], contracts.digest(frozen))
            for key in ("source_start_utc", "source_end_utc", "cutoff_utc"):
                self.assertEqual(contracts.utc(frozen[key]),
                                 contracts.utc(first[key]) + timedelta(days=ordinal))
            for key in ("declared_at_utc", "prior_outcomes_observed", "scopes", "gate_policy",
                        "resource_budget", "version_bindings"):
                self.assertEqual(frozen[key], first[key])
            for part, original in zip(frozen["parts"], first["parts"]):
                for key in ("source_start_utc", "source_end_utc"):
                    self.assertEqual(contracts.utc(part[key]),
                                     contracts.utc(original[key]) + timedelta(days=ordinal))
                for key in ("source_row_limit", "max_candle_rows", "page_size", "source_byte_limit"):
                    self.assertEqual(part[key], original[key])
            if ordinal:
                self.assertEqual(plan["windows"][ordinal - 1]["declaration"]["source_end_utc"],
                                 frozen["source_start_utc"])

    def test_all_windows_have_new_bounded_cohort_keys_including_zero(self):
        plan = self.plan()
        keys = [item["declaration"]["cohort_key"] for item in plan["windows"]]
        self.assertEqual(len(set(keys)), 3)
        self.assertNotIn(declaration()["cohort_key"], keys)
        self.assertTrue(all(len(key.encode()) <= 256 for key in keys))
        changed = declaration()
        changed["cohort_key"] = "another-template"
        self.assertNotEqual(keys, [w["declaration"]["cohort_key"] for w in self.plan(first=changed)["windows"]])

    def test_timezone_equivalent_template_has_same_identity(self):
        first = declaration()
        for key in ("declared_at_utc", "source_start_utc", "source_end_utc", "cutoff_utc"):
            first[key] = contracts.utc(first[key]).isoformat().replace("+00:00", "Z")
        self.assertEqual(self.plan(first=first), self.plan())

    def test_count_strict_type_and_bounds(self):
        for count in (True, False, 0, -1, 33, 1.0, "2", None):
            with self.subTest(count=count), self.assertRaises(ValueError):
                self.plan(count=count)
        self.assertEqual(self.plan(count=32)["window_count"], 32)

    def test_declared_time_cannot_follow_first_source_start(self):
        first = declaration()
        first["declared_at_utc"] = first["source_start_utc"]
        self.assertEqual(self.plan(first=first)["window_count"], 3)
        first["declared_at_utc"] = "2026-01-02T00:00:00.000001+00:00"
        with self.assertRaisesRegex(ValueError, "DECLARATION_AFTER"):
            self.plan(first=first)

    def test_total_calendar_span_includes_final_cutoff(self):
        first = declaration()
        start = contracts.utc(first["source_start_utc"])
        # Twelve 30-day windows plus six days would exceed each cohort's
        # 31-day bound. Use 12-day windows: 30 windows plus six days of lag.
        end = start + timedelta(days=12)
        first["source_end_utc"] = end.isoformat()
        first["cutoff_utc"] = (end + timedelta(days=6)).isoformat()
        first["parts"] = [{"ordinal": 0, "source_start_utc": start.isoformat(),
                          "source_end_utc": end.isoformat()}]
        self.assertEqual(self.plan(count=30, first=first)["window_count"], 30)
        first["cutoff_utc"] = (end + timedelta(days=6, microseconds=1)).isoformat()
        with self.assertRaisesRegex(ValueError, "TOTAL_SPAN"):
            self.plan(count=30, first=first)

    def test_invalid_final_input_fails_before_any_registration(self):
        for field in ("declaration_sha256", "ordinal"):
            plan = self.plan()
            plan["windows"][-1][field] = "tampered"
            plan["plan_sha256"] = contracts.digest({k: v for k, v in plan.items() if k != "plan_sha256"})
            backend = Backend()
            with self.assertRaisesRegex(ValueError, "PLAN_OR_IMPLEMENTATION"):
                calendar.register_plan(backend, plan)
            self.assertEqual(backend.calls, [])

    def test_plan_cannot_omit_reorder_add_or_relabel_windows(self):
        for change in ("omit", "reorder", "extra", "unknown", "authority", "count_bool"):
            plan = self.plan()
            if change == "omit":
                plan["windows"].pop()
            elif change == "reorder":
                plan["windows"].reverse()
            elif change == "extra":
                plan["windows"].append(deepcopy(plan["windows"][-1]))
            elif change == "unknown":
                plan["unknown"] = True
            elif change == "authority":
                plan["runtime_authorized"] = True
            else:
                plan["window_count"] = True
            with self.subTest(change=change), self.assertRaises(ValueError):
                calendar.validate_plan(plan)

    def test_rehashed_implementation_change_cannot_resume(self):
        plan = self.plan()
        for key in ("planner_sha256", "acquisition"):
            changed = deepcopy(plan)
            changed["implementation"][key] = "different"
            changed["plan_sha256"] = contracts.digest({k: v for k, v in changed.items() if k != "plan_sha256"})
            with self.subTest(key=key), self.assertRaises(ValueError):
                calendar.validate_plan(changed)
        backend = Backend()
        backend.versions["version"] = "new-version"
        with self.assertRaisesRegex(ValueError, "BACKEND_IMPLEMENTATION"):
            calendar.register_plan(backend, plan)
        self.assertEqual(backend.calls, [])

    def test_full_registration_is_ordered_idempotent_and_strict(self):
        plan, backend = self.plan(), Backend()
        first = calendar.register_plan(backend, plan)
        second = calendar.register_plan(backend, plan)
        self.assertEqual(first, second)
        self.assertEqual(len(backend.rows), 3)
        self.assertEqual([row["ordinal"] for row in first["registrations"]], [0, 1, 2])
        self.assertTrue(all(key is None and strict is True for _, key, strict in backend.calls))
        self.assertTrue(first["registration_complete"])

    def test_partial_commits_are_reused_without_skipping_later_windows(self):
        plan, backend = self.plan(), Backend()
        backend.fail_call = 2
        with self.assertRaises(ConnectionError):
            calendar.register_plan(backend, plan)
        initial_ids = set(backend.rows)
        self.assertEqual(len(initial_ids), 1)
        backend.fail_call = None
        receipt = calendar.register_plan(backend, plan)
        self.assertEqual(len(backend.rows), 3)
        self.assertTrue(initial_ids <= set(backend.rows))
        self.assertEqual(len(receipt["registrations"]), 3)

    def test_existing_timely_registration_can_recover_after_start(self):
        plan, backend = self.plan(), Backend()
        receipt = calendar.register_plan(backend, plan)
        backend.now = "2027-01-01T00:00:00+00:00"
        self.assertEqual(calendar.register_plan(backend, plan), receipt)

    def test_missing_late_window_rejects_without_skipping_to_future_window(self):
        plan, backend = self.plan(), Backend()
        backend.fail_call = 2
        with self.assertRaises(ConnectionError):
            calendar.register_plan(backend, plan)
        backend.fail_call = None
        backend.now = "2026-01-03T00:00:00.000001+00:00"
        with self.assertRaisesRegex(ValueError, "AFTER_SOURCE_START"):
            calendar.register_plan(backend, plan)
        self.assertEqual(len(backend.rows), 1)

    def test_false_registration_evidence_is_not_accepted(self):
        for change in ("request_id", "declaration_sha256", "source_start_utc",
                       "created_at_utc", "registered_before_source_start"):
            backend, plan = Backend(), self.plan(count=1)
            original = backend.registration_info
            def corrupted(request_id):
                info = original(request_id)
                info[change] = (False if change == "registered_before_source_start" else
                    "2027-01-01T00:00:00+00:00" if change.endswith("_utc") else "bad")
                return info
            backend.registration_info = corrupted
            with self.subTest(change=change), self.assertRaises(ValueError):
                calendar.register_plan(backend, plan)

    def test_prior_knowledge_is_preserved_without_validation_or_pooling_claims(self):
        first = declaration()
        first["prior_outcomes_observed"] = True
        plan = self.plan(first=first)
        receipt = calendar.register_plan(Backend(), plan)
        self.assertTrue(all(w["declaration"]["prior_outcomes_observed"] for w in plan["windows"]))
        for value in (plan, receipt):
            for key in ("runtime_authorized", "telegram_authorized", "trading_authorized",
                        "is_prospective_formula_evidence", "validated_discovery",
                        "multiple_testing_adjusted", "scope_pooling",
                        "cross_window_outcome_pooling", "cross_window_parent_independence_proven"):
                self.assertIs(value[key], False)

    def test_real_implementation_binds_planner_bytes_and_acquisition(self):
        current = _REAL_IMPLEMENTATION()
        self.assertEqual(current["planner_version"], calendar.VERSION)
        self.assertEqual(current["planner_sha256"],
                         hashlib.sha256(Path(calendar.__file__).read_bytes()).hexdigest())
        self.assertIn("implementation_files", current["acquisition"])


if __name__ == "__main__":
    unittest.main()
