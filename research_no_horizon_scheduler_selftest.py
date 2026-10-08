"""Focused no-network scheduler orchestration and provenance regressions.

These tests use synthetic context and fake storage, with the native acquisition
run_once method for claim-before-source ordering. They are not PostgreSQL/market
acceptance; the existing database integration suite owns those contracts.
"""
from copy import deepcopy
import json
import unittest
from unittest.mock import Mock, patch

import research_no_horizon_scheduler as scheduler
import research_no_horizon_acquisition_store as native_acquisition


def context():
    plans, registrations = {}, {}
    for group in ("a", "b", "c", "d"):
        windows, ids = [], []
        for ordinal in range(2):
            request_id = f"{group}{ordinal}"
            declaration = {"source_start_utc": f"2026-10-{7 + ordinal * 4:02}T00:00:00+00:00",
                           "group": group}
            registration = {"request_id": request_id,
                "declaration_sha256": scheduler.contracts.digest(declaration),
                "created_at_utc": "2026-10-06T00:00:00+00:00",
                "registered_before_source_start": True}
            registrations[request_id] = {"registration": registration,
                "declaration": declaration, "group": group, "window_ordinal": ordinal}
            windows.append({"declaration": declaration})
            ids.append(request_id)
        plan = {"plan_sha256": group,
                "discovery_plan": {"calendar_plan": {"windows": windows}}}
        plans[group] = {"plan": plan, "requests": ids,
                        "receipt": {"plan_sha256": group}}
    return plans, registrations


class Intake:
    # Exercise the real due-claim gate; the fake process_claim is reached only if
    # native run_once received a due claim. No replica of run_once is used here.
    run_once = native_acquisition.AcquisitionStore.run_once

    def __init__(self, registrations):
        self.connection = Mock()
        self.registrations = registrations
        self.due = set()
        self.calls = []
        self.fail = {}
        self.rows = {request_id: {"request_id": request_id, "status": "WAITING",
            "proof_count": 0, "executor_plan_id": None, "report_sha256": request_id}
            for request_id in registrations}

    def claim_request(self, worker_id, *, lease_seconds, request_id):
        self.calls.append(("claim", request_id))
        return {"request_id": request_id} if request_id in self.due else None

    def process_claim(self, claim, *, source_connection_factory, leaf_budget):
        request_id = claim["request_id"]
        self.calls.append(("process", request_id))
        source_connection_factory()
        if request_id in self.fail:
            raise self.fail.pop(request_id)
        row = self.rows[request_id]
        row.update(status="ADMITTED", proof_count=1, executor_plan_id="plan-" + request_id)
        return {**row, "plan_id": row["executor_plan_id"], "queries_this_run": 1}

    def report(self, request_id):
        return deepcopy(self.rows[request_id])

    def registration_info(self, request_id):
        return deepcopy(self.registrations[request_id]["registration"])

    def _request(self, request_id, *, compatible):
        return {"declaration_json": json.dumps(self.registrations[request_id]["declaration"])}


class Executor:
    def __init__(self):
        self.calls = []
        self.reports = {}
        self.fail = {}
        self.after_pass = None

    def run_once(self, worker_id, **kwargs):
        self.calls.append(kwargs)
        plan_id = kwargs["plan_id"]
        if plan_id in self.fail:
            raise self.fail.pop(plan_id)
        if self.after_pass:
            self.after_pass()
        return {"plan_id": plan_id, "scope_ordinal": 0, "status": "RUNNING",
                "candle_evaluations_this_run": 1}

    def report(self, plan_id):
        return deepcopy(self.reports.get(plan_id, {"plan_id": plan_id,
            "report_sha256": "report-" + plan_id, "all_scopes_processed": False}))


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.plans, self.registrations = context()
        self.intake = Intake(self.registrations)
        self.backend = Executor()
        self.source = Mock(return_value=object())
        self.verify = Mock()
        self.provenance = Mock(return_value=True)
        self.selected_calls = []
        def select(plan, reports):
            self.selected_calls.append(deepcopy(reports))
            complete = all(report["all_scopes_processed"] for report in reports)
            return {"selection_complete": complete,
                "selected_scope_ids": ["scope"] if complete else [],
                "policy_registration_verified": False,
                "source_provenance_verified_by_this_tool": False,
                "scope_pooling": False, "window_pooling": False}
        for target, name, value in (
            (scheduler.acquisition, "AcquisitionStore", Mock(return_value=self.intake)),
            (scheduler.executor, "PostgresCohortStore", Mock(return_value=self.backend)),
            (scheduler.selection, "validate_plan", lambda plan: plan),
            (scheduler.selection, "select_reports", select),
        ):
            patcher = patch.object(target, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def run_tick(self, **kwargs):
        arguments = {"source_connection_factory": self.source, "worker_id": "test-worker",
                     "provenance_verifier": self.provenance, "verifier": self.verify}
        arguments.update(kwargs)
        return scheduler.run_tick(object(), self.plans, self.registrations, **arguments)

    def admit(self, *request_ids):
        for request_id in request_ids:
            self.intake.rows[request_id].update(status="ADMITTED", proof_count=1,
                executor_plan_id="plan-" + request_id)

    def test_not_due_never_opens_source_or_executes_and_keeps_original_denominator(self):
        result = self.run_tick()
        self.source.assert_not_called()
        self.assertEqual(self.backend.calls, [])
        self.assertEqual(result["visited_request_ids"], ["a0", "b0", "c0", "d0", "a1", "b1", "c1", "d1"])
        self.assertEqual(len(self.intake.calls), 8)
        self.assertEqual(len(result["groups"]), 4)
        self.assertFalse(result["selection_complete"])
        self.assertTrue(all(group["status"] == "WAITING_FOR_BOTH_WINDOW_REPORTS"
                            for group in result["groups"]))

    def test_small_budget_cursor_preserves_fairness_across_restarts(self):
        visited, cursor = [], 0
        for _ in range(4):
            result = self.run_tick(cursor=cursor, request_budget=2)
            visited.extend(result["visited_request_ids"])
            cursor = result["next_cursor"]
        self.assertEqual(len(set(visited)), 8)
        self.assertEqual(cursor, 0)

    def test_due_source_then_bounded_native_execution_with_exact_plan(self):
        self.intake.due.add("a0")
        result = self.run_tick(request_budget=1, acquisition_leaf_budget=1,
                               execution_passes=2, candle_budget=3, entry_budget=4,
                               batch_size=5, lease_seconds=60)
        self.source.assert_called_once_with()
        self.assertEqual(self.intake.calls, [("claim", "a0"), ("process", "a0")])
        self.assertEqual(self.backend.calls, [dict(plan_id="plan-a0", lease_seconds=60,
            candle_budget=3, entry_budget=4, batch_size=5)] * 2)
        self.assertTrue(result["requests"][0]["acquisition_claimed"])
        self.assertEqual(result["source_transport"], "DIRECT_POSTGRES_NATIVE")

    def test_transient_error_sanitized_retry_succeeds_and_other_request_progresses(self):
        self.intake.due.update(("a0", "b0"))
        self.intake.fail["a0"] = RuntimeError("postgresql://password@example/private payload")
        first = self.run_tick(request_budget=2)
        self.assertEqual(self.intake.rows["a0"]["status"], "WAITING")
        self.assertEqual(self.intake.rows["b0"]["status"], "ADMITTED")
        self.assertEqual(first["errors"], [{"request_id": "a0", "stage": "ACQUISITION",
                                          "error_type": "RuntimeError"}])
        self.assertNotIn("password", repr(first))
        second = self.run_tick(request_budget=1)
        self.assertEqual(self.intake.rows["a0"]["status"], "ADMITTED")
        self.assertEqual(second["errors"], [])

    def test_blocked_acquisition_is_terminal_and_does_not_call_source(self):
        self.intake.rows["a0"]["status"] = "BLOCKED"
        self.intake.due.add("a0")
        result = self.run_tick(request_budget=1)
        self.assertEqual(self.intake.calls, [])
        self.source.assert_not_called()
        self.assertEqual(result["groups"][0]["status"], "BLOCKED_ACQUISITION")
        self.assertEqual(self.selected_calls, [])

    def test_selection_uses_window_order_and_preserves_incomplete_native_flags(self):
        self.admit("a0", "a1")
        result = self.run_tick()
        self.assertEqual([report["plan_id"] for report in self.selected_calls[0]],
                         ["plan-a0", "plan-a1"])
        group = result["groups"][0]
        self.assertEqual(group["status"], "INCOMPLETE")
        self.assertEqual(group["native_selection"]["selected_scope_ids"], [])
        self.assertIs(group["native_selection"]["policy_registration_verified"], False)
        self.assertIs(group["native_selection"]["source_provenance_verified_by_this_tool"], False)
        self.assertFalse(result["group_or_window_pooling"])

    def test_all_completed_selections_are_separate_and_keep_registration_linkage(self):
        self.admit(*self.registrations)
        for request_id in self.registrations:
            self.backend.reports["plan-" + request_id] = {"plan_id": "plan-" + request_id,
                "report_sha256": "report-" + request_id, "all_scopes_processed": True}
        result = self.run_tick()
        self.assertTrue(result["selection_complete"])
        self.assertEqual(len(self.selected_calls), 4)
        for group in result["groups"]:
            ids = [item["request_id"] for item in group["registration_linkage"]]
            self.assertEqual(ids, self.plans[group["group"]]["requests"])
        for authority in ("runtime_authorized", "telegram_authorized", "trading_authorized"):
            self.assertIs(result[authority], False)

    def test_stop_after_unit_prevents_further_passes_source_and_selection(self):
        self.admit("a0")
        state = {"stop": False}
        self.backend.after_pass = lambda: state.update(stop=True)
        result = self.run_tick(execution_passes=3, cancelled=lambda: state["stop"])
        self.assertEqual(len(self.backend.calls), 1)
        self.assertEqual(result["visited_request_ids"], ["a0"])
        self.assertTrue(result["cancelled"])
        self.assertEqual(result["next_cursor"], 1)
        self.source.assert_not_called()
        self.assertEqual(self.selected_calls, [])

    def test_initial_stop_performs_no_mutation_but_still_verifies_identity_and_provenance(self):
        result = self.run_tick(cancelled=lambda: True)
        self.verify.assert_called_once()
        self.provenance.assert_called_once()
        self.assertTrue(result["cancelled"])
        self.assertEqual(result["visited_request_ids"], [])
        self.assertEqual(self.intake.calls, [])

    def test_unverified_native_route_or_altered_registration_fails_before_work(self):
        self.provenance.return_value = False
        with self.assertRaisesRegex(ValueError, "PROVENANCE"):
            self.run_tick()
        self.assertEqual(self.intake.calls, [])
        self.provenance.return_value = True
        self.verify.side_effect = ValueError("ORIGINAL_REGISTRATION_CHANGED")
        with self.assertRaisesRegex(ValueError, "REGISTRATION_CHANGED"):
            self.run_tick()
        self.source.assert_not_called()

    def test_missing_reordered_or_duplicated_original_request_context_is_rejected(self):
        original = deepcopy(self.plans)
        for change in (lambda: self.plans["a"]["requests"].reverse(),
                       lambda: self.plans["a"]["requests"].__setitem__(1, "a0"),
                       lambda: self.plans.pop("d")):
            self.plans = deepcopy(original)
            change()
            with self.assertRaises(ValueError):
                self.run_tick()
        self.source.assert_not_called()
        self.assertEqual(self.intake.calls, [])

    def test_invalid_bounds_fail_before_store_or_verification(self):
        for kwargs in ({"request_budget": 0}, {"execution_passes": 33},
                       {"acquisition_leaf_budget": True}, {"cursor": 8},
                       {"candle_budget": 65537}, {"entry_budget": 1025}):
            with self.assertRaises(ValueError):
                self.run_tick(**kwargs)
        self.verify.assert_not_called()
        self.source.assert_not_called()

    def test_native_identity_verifier_detects_original_timestamp_change(self):
        store = self.intake
        conn = store.connection
        conn.transaction.return_value.__enter__ = Mock()
        conn.transaction.return_value.__exit__ = Mock(return_value=False)
        conn.execute.return_value.fetchall.return_value = [
            {"request_id": request_id} for request_id in self.registrations]
        with patch.object(scheduler.acquisition, "schema_status", return_value={"compatible": True}):
            scheduler.verify_original_registrations(store, self.registrations)
            changed = deepcopy(self.registrations)
            changed["a0"]["registration"]["created_at_utc"] = "2026-10-08T00:00:00+00:00"
            with self.assertRaisesRegex(ValueError, "REGISTRATION_CHANGED"):
                scheduler.verify_original_registrations(store, changed)
        self.source.assert_not_called()


if __name__ == "__main__":
    unittest.main()
