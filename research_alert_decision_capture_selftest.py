"""Pure decision-observation regressions; no providers, DB, or bot imports."""
from copy import deepcopy
import hashlib
import json
import unittest
from unittest.mock import patch

import research_alert_decision_capture as capture


class CaptureTests(unittest.TestCase):
    def setUp(self):
        self.tokens = []

    def tearDown(self):
        for token in reversed(self.tokens):
            capture.reset(token)

    def start(self, **kwargs):
        token = capture.begin("shared-watch:2026-10-29T17:02:15.112702+00:00", **kwargs)
        self.tokens.append(token)
        capture.bind(score_payload_sha256="a" * 64, source_code_sha256={"main.py": "b" * 64})
        return token

    def observe(self, native=lambda: None, **kwargs):
        return capture.observe_call("special_native_batch", native,
                                    state=kwargs.pop("state", {}), source={"items": []}, **kwargs)

    def sealed(self):
        result = capture.seal()
        self.assertIsNotNone(result)
        self.assertTrue(capture.validate_capsule(result))
        return result

    def test_no_context_does_not_touch_state_or_change_result(self):
        class Explosive:
            def __iter__(self):
                raise AssertionError("diagnostics touched disabled path")
        result = object()
        self.assertIs(capture.observe_call("manual", lambda: result, state=Explosive()), result)
        self.assertIsNone(capture.seal())

    def test_native_once_result_identity_and_arguments(self):
        self.start()
        calls = []
        result = ["הודעה"]
        def native(value, *, flag):
            calls.append((value, flag))
            return result
        self.assertIs(self.observe(native, args=(0,), kwargs={"flag": False}), result)
        self.assertEqual(calls, [(0, False)])
        record = self.sealed()["payload"]["records"][0]
        self.assertEqual(record["result"]["outcome"], "OUTPUT")
        self.assertFalse(record["result"]["transport_delivery_proven"])

    def test_key_absent_then_zero_is_not_fallback_absence(self):
        self.start()
        state = {}
        def native():
            state["DOGE|12h|SHORT"] = 0
        self.observe(native, state=state, state_key="DOGE|12h|SHORT")
        record = self.sealed()["payload"]["records"][0]
        self.assertEqual(record["pre_state"]["presence"], "ABSENT")
        self.assertEqual(record["post_state"]["presence"], "PRESENT")
        self.assertEqual(record["post_state"]["snapshot"]["value"], 0)

    def test_present_none_false_empty_and_zero_are_distinct(self):
        self.start()
        for index, value in enumerate((None, False, "", [], {}, 0, 0.0)):
            self.observe(state={"key": value}, state_key="key")
        records = self.sealed()["payload"]["records"]
        for record, value in zip(records, (None, False, "", [], {}, 0, 0.0)):
            snapshot = record["pre_state"]
            self.assertEqual(snapshot["presence"], "PRESENT")
            self.assertEqual(snapshot["snapshot"]["value"], value)
            self.assertIs(type(snapshot["snapshot"]["value"]), type(value))

    def test_batch_references_and_sets_are_deterministic(self):
        self.start()
        confirmation = {"DOGE|12h|LONG": "CONFIRMED"}
        combined = {"DOGE|LONG": {"spot", "positioning"}}
        state = {"confirmation": confirmation, "combined": combined}
        before = deepcopy(state)
        def native():
            confirmation["DOGE|12h|LONG"] = "CONFLICT"
            combined.pop("DOGE|LONG")
            return []
        self.observe(native, state=state)
        record = self.sealed()["payload"]["records"][0]
        self.assertEqual(record["pre_state"]["snapshot"]["value"]["combined"]["DOGE|LONG"],
                         {"captured_type": "set", "items": ["positioning", "spot"]})
        self.assertEqual(record["post_state"]["snapshot"]["value"]["combined"], {})
        self.assertNotEqual(state, before)

    def test_native_exception_identity_and_partial_poststate_preserved(self):
        self.start()
        error = RuntimeError("private native detail")
        state = {}
        def native():
            state["key"] = True
            raise error
        try:
            self.observe(native, state=state)
        except RuntimeError as caught:
            self.assertIs(caught, error)
            names = []
            trace = caught.__traceback__
            while trace:
                names.append(trace.tb_frame.f_code.co_name)
                trace = trace.tb_next
            self.assertIn("native", names)
        else:
            self.fail("native exception swallowed")
        capsule = self.sealed()
        record = capsule["payload"]["records"][0]
        self.assertEqual(record["result"], {"outcome": "RAISED", "native_error_type": "RuntimeError"})
        self.assertTrue(record["post_state"]["snapshot"]["value"]["key"])
        self.assertNotIn("private native detail", capture.canonical_bytes(capsule).decode())

    def test_native_baseexception_is_not_swallowed(self):
        self.start()
        error = KeyboardInterrupt()
        def native():
            raise error
        with self.assertRaises(KeyboardInterrupt) as caught:
            self.observe(native)
        self.assertIs(caught.exception, error)

    def test_begin_failure_does_not_change_native_result(self):
        self.start()
        result = object()
        with patch.object(capture, "begin_decision", side_effect=RuntimeError("diagnostic")):
            self.assertIs(self.observe(lambda: result), result)
        self.assertEqual(self.sealed()["status"], "PARTIAL")

    def test_end_failure_does_not_change_native_result_or_error(self):
        self.start()
        result = object()
        error = ValueError("native")
        def native_error():
            raise error
        with patch.object(capture, "end_decision", side_effect=RuntimeError("diagnostic")):
            self.assertIs(self.observe(lambda: result), result)
            with self.assertRaises(ValueError) as caught:
                self.observe(native_error)
            self.assertIs(caught.exception, error)
        self.assertEqual(self.sealed()["status"], "PARTIAL")

    def test_clock_failure_is_visible_and_native_unchanged(self):
        with patch.object(capture, "_clock", side_effect=RuntimeError("diagnostic")):
            self.start()
            self.assertIsNone(self.observe())
            capsule = self.sealed()
        self.assertEqual(capsule["status"], "PARTIAL")
        self.assertIn("CAPTURE_CLOCK_UNAVAILABLE", capsule["payload"]["omissions"])

    def test_public_gap_is_visible_and_does_not_fabricate_invocation(self):
        self.start()
        capture.gap("BRIDGE_PROJECTION_FAILED")
        capsule = self.sealed()
        self.assertEqual(capsule["status"], "PARTIAL")
        self.assertEqual(capsule["payload"]["native_invocation_count"], 0)
        self.assertEqual(capsule["payload"]["records"], [])
        self.assertIn("BRIDGE_PROJECTION_FAILED", capsule["payload"]["omissions"])

    def test_activation_failure_restores_previous_context(self):
        self.start()
        with patch.object(capture, "stage", side_effect=RuntimeError("diagnostic")):
            self.assertIsNone(capture.begin("failed-nested"))
        self.assertEqual(capture.seal()["cycle_id"], "shared-watch:2026-10-29T17:02:15.112702+00:00")

    def test_copy_failure_is_visible_and_native_unchanged(self):
        self.start()
        state = {"key": object()}
        self.assertIsNone(self.observe(state=state))
        capsule = self.sealed()
        self.assertEqual(capsule["status"], "PARTIAL")
        self.assertEqual(capsule["payload"]["records"][0]["pre_state"]["snapshot"]["status"], "UNKNOWN")

    def test_cyclic_state_never_raises_or_mutates_native_state(self):
        self.start()
        state = {}
        state["self"] = state
        self.observe(state=state)
        self.assertIs(state["self"], state)
        self.assertEqual(self.sealed()["status"], "PARTIAL")

    def test_custom_mapping_never_runs_its_methods(self):
        self.start()
        class Explosive(dict):
            def __iter__(self):
                raise AssertionError("custom iterator")
            def __contains__(self, key):
                raise AssertionError("custom membership")
        self.observe(state=Explosive())
        self.assertEqual(self.sealed()["status"], "PARTIAL")

    def test_no_output_does_not_infer_suppression(self):
        self.start()
        for value in (None, "", [], ()):
            self.observe(lambda value=value: value)
        capsule = self.sealed()
        self.assertEqual(capsule["status"], "COMPLETE")
        for record in capsule["payload"]["records"]:
            self.assertEqual(record["result"]["outcome"], "NO_OUTPUT")
            self.assertEqual(record["reason_scope"], "UNKNOWN")
            self.assertEqual(record["reason_code"], "UNKNOWN")

    def test_explicit_witness_is_separate_from_inference(self):
        self.start()
        token = capture.begin_decision("Confirmation", state={}, source={"score": 0})
        capture.end_decision(token, result=None, reason_code="NATIVE_EXPLICIT_BRANCH")
        record = self.sealed()["payload"]["records"][0]
        self.assertEqual(record["reason_scope"], "NATIVE_BRANCH_WITNESS")

    def test_combined_metadata_result_never_retains_candidate_or_text(self):
        self.start()
        message = "עברית הודעה פרטית"
        candidate = {"private": object()}
        result = [{"text": message, "candidate": candidate, "decision_time": object()}]
        self.assertIs(capture.observe_call("combined_native_batch", lambda: result,
                                          state={}, source={"key": "DOGE|LONG"}), result)
        capsule = self.sealed()
        raw = capture.canonical_bytes(capsule).decode()
        self.assertNotIn(message, raw)
        self.assertNotIn("candidate", raw)
        self.assertEqual(capsule["payload"]["records"][0]["result"]["outputs"][0]["text_sha256"],
                         hashlib.sha256(message.encode()).hexdigest())

    def test_recipient_and_secret_fields_are_not_persisted(self):
        self.start()
        capture.observe_call("batch", lambda: None, state={}, source={"chat_id": "secret recipient"})
        capsule = self.sealed()
        self.assertNotIn("secret recipient", capture.canonical_bytes(capsule).decode())
        self.assertEqual(capsule["status"], "PARTIAL")

    def test_oversized_value_visible_not_silently_truncated(self):
        self.start()
        capture.observe_call("batch", lambda: [], state={}, source={"large": "x" * 100_000})
        capsule = self.sealed()
        self.assertEqual(capsule["status"], "PARTIAL")
        self.assertEqual(capsule["payload"]["records"][0]["source"]["status"], "UNKNOWN")

    def test_deep_bounded_source_is_exact_canonical_text_not_dropped(self):
        self.start()
        value = {"zero": 0}
        for _ in range(7):
            value = {"nested": value}
        capture.observe_call("batch", lambda: [], state={}, source=value)
        capsule = self.sealed()
        self.assertEqual(capsule["status"], "COMPLETE")
        projection = capsule["payload"]["records"][0]["source"]
        self.assertEqual(projection["encoding"], "CANONICAL_JSON_TEXT")
        self.assertEqual(json.loads(projection["canonical_json"]), value)
        self.assertEqual(projection["sha256"], hashlib.sha256(projection["canonical_json"].encode()).hexdigest())
        tampered = deepcopy(capsule)
        tampered["payload"]["records"][0]["source"]["canonical_json"] = '{"changed":0}'
        tampered["payload_sha256"] = capture._digest({key: part for key, part in tampered.items() if key != "payload_sha256"})
        with self.assertRaisesRegex(ValueError, "CAPSULE_VALUE_HASH_INVALID"):
            capture.validate_capsule(tampered)

    def test_batch_shaped_source_interoperates_with_store(self):
        try:
            import research_alert_decision_store as store
        except ImportError:
            self.skipTest("store module not present in this isolated collector checkout")
        self.start()
        item = {"symbol": "DOGE", "score": 0, "market_evidence": {"modules": {
            "spot_flow": {"time_families": {
                "now": {"direction": "NEUTRAL", "quality": 0, "windows": ["5m", "15m"]}}}}}}
        source = {"items": [item], "combined_candidates": [{"key": "DOGE|LONG", "signal_keys": []}]}
        capture.observe_call("special_native_batch", lambda: [],
                             state={"confirmation": {"DOGE|12h|LONG": "UNCONFIRMED"},
                                    "combined": {"DOGE|LONG": {"positioning", "spot"}}}, source=source)
        capsule = self.sealed()
        self.assertEqual(capsule["status"], "COMPLETE")
        result = store.prepare(capsule)
        # The store may return its own immutable preparation envelope, but must
        # accept this real collector capsule without changing any retained bytes.
        self.assertIsNotNone(result)

    def test_oversized_output_does_not_change_result(self):
        self.start()
        value = "x" * (capture.MAX_MESSAGE_BYTES + 1)
        self.assertIs(self.observe(lambda: value), value)
        self.assertEqual(self.sealed()["status"], "PARTIAL")

    def test_record_limit_and_unfinished_records_are_visible(self):
        self.start()
        for _ in range(capture.MAX_RECORDS + 2):
            self.observe()
        capture.begin_decision("unfinished", state={}, source={})
        payload = self.sealed()["payload"]
        self.assertEqual(payload["native_invocation_count"], capture.MAX_RECORDS + 3)
        self.assertEqual(payload["record_count"], capture.MAX_RECORDS)
        self.assertEqual(payload["omitted_record_count"], 3)
        self.assertIn("UNFINISHED_OR_OMITTED_NATIVE_INVOCATIONS", payload["omissions"])

    def test_full_capsule_budget_drops_records_visibly(self):
        self.start()
        source = {str(index): "x" * 1000 for index in range(70)}
        for _ in range(5):
            capture.observe_call("batch", lambda: [], state={}, source=source)
        capsule = self.sealed()
        self.assertLessEqual(len(capture.canonical_bytes(capsule)), capture.MAX_CAPSULE_BYTES)
        self.assertEqual(capsule["status"], "PARTIAL")
        self.assertGreater(capsule["payload"]["omitted_record_count"], 0)
        self.assertIn("CAPSULE_BUDGET_EXCEEDED", capsule["payload"]["omissions"])

    def test_complete_source_bound_and_default_not_reached_stages(self):
        self.start()
        self.observe()
        capsule = self.sealed()
        self.assertEqual(capsule["status"], "COMPLETE")
        self.assertEqual(capsule["payload"]["stage_status"]["combined_native_batch"], "NOT_REACHED")
        self.assertEqual(capsule["payload"]["score_payload_sha256"], "a" * 64)
        self.assertIn("research_alert_decision_capture.py", capsule["payload"]["source_code_sha256"])

    def test_gate_and_error_stages_are_explicit(self):
        self.start()
        capture.stage("ordinary_lifecycle_gate", "SKIPPED_GATE", reason_code="ORDINARY_DISABLED")
        capture.stage("special_native_batch", "ERROR")
        capsule = self.sealed()
        self.assertEqual(capsule["payload"]["stage_status"]["ordinary_lifecycle_gate"], "SKIPPED_GATE")
        self.assertEqual(capsule["payload"]["stage_status"]["special_native_batch"], "ERROR")

    def test_unknown_source_projection_marks_partial(self):
        self.start()
        capture.observe_call("batch", lambda: [], state={}, source=capture.source_projection(object()))
        self.assertEqual(self.sealed()["status"], "PARTIAL")

    def test_source_zero_and_priority_presence_are_preserved(self):
        zero = capture.source_projection({"score": 0, "priority": 99})
        missing = capture.source_projection({"priority": 99})
        null = capture.source_projection({"score": None, "priority": 99})
        self.assertEqual(zero["native_score_selected_field"], "score")
        self.assertEqual(missing["native_score_selected_field"], "priority")
        self.assertEqual(zero["fields"]["score"]["value"], 0)
        self.assertIsNone(null["fields"]["score"]["value"])
        self.assertEqual(missing["fields"]["score"]["presence"], "ABSENT")

    def test_source_model_digest_and_no_input_mutation(self):
        item = {"symbol": "DOGE", "score": 0, "market_evidence": {"modules": {
            "positioning": {"score": 0, "quality": 0, "available": False},
            "spot_flow": {"time_families": {"now": {"quality": 0, "direction": "NEUTRAL", "windows": []}}}}}}
        before = deepcopy(item)
        projection = capture.source_projection(item)
        self.assertEqual(item, before)
        self.assertEqual(projection["model_evidence_sha256"],
                         hashlib.sha256(capture.canonical_bytes(projection["model_evidence"])).hexdigest())
        self.assertIs(projection["model_evidence"]["positioning"]["available"], False)

    def test_source_model_falsy_values_do_not_become_missing(self):
        for value in (False, 0, "", [], None):
            projection = capture.source_projection({"maxpain_confirmation": value,
                "market_evidence": {"modules": {"positioning": value}}})
            observed = projection["model_evidence"]["positioning"]
            self.assertEqual(observed["presence"], "PRESENT")
            self.assertIs(type(observed["value"]), type(value))
            self.assertEqual(observed["value"], value)
            self.assertEqual(projection["raw_maxpain_confirmation_presence"], "PRESENT")
            self.assertIs(type(projection["raw_maxpain_confirmation_fallback_value"]), type(value))
        absent = capture.source_projection({})
        self.assertEqual(absent["model_evidence"]["positioning"]["presence"], "ABSENT")
        self.assertEqual(absent["raw_maxpain_confirmation_presence"], "ABSENT")

    def test_hash_is_whole_capsule_without_hash_field(self):
        self.start()
        self.observe()
        capsule = self.sealed()
        unsigned = {key: value for key, value in capsule.items() if key != "payload_sha256"}
        self.assertEqual(capsule["payload_sha256"], hashlib.sha256(capture.canonical_bytes(unsigned)).hexdigest())
        tampered = deepcopy(capsule)
        tampered["cycle_id"] = "different"
        with self.assertRaises(ValueError):
            capture.validate_capsule(tampered)

    def test_seal_is_independent_idempotent_and_stops_later_observations(self):
        self.start()
        self.observe()
        first = self.sealed()
        raw = capture.canonical_bytes(first)
        first["payload"]["records"].clear()
        self.observe()
        capture.stage("late", "ERROR")
        second = self.sealed()
        self.assertEqual(capture.canonical_bytes(second), raw)

    def test_nested_watch_contexts_reset_to_original(self):
        first = self.start()
        self.observe()
        second = capture.begin("nested")
        capture.bind(score_payload_sha256="c" * 64)
        self.observe()
        self.assertEqual(capture.seal()["cycle_id"], "nested")
        capture.reset(second)
        self.assertEqual(capture.seal()["payload"]["score_payload_sha256"], "a" * 64)
        capture.reset(first)
        self.assertIsNone(capture.seal())

    def test_missing_score_binding_is_partial_not_fabricated(self):
        token = capture.begin("missing-score")
        self.tokens.append(token)
        capsule = self.sealed()
        self.assertEqual(capsule["status"], "PARTIAL")
        self.assertIsNone(capsule["payload"]["score_payload_sha256"])

    def test_rebinding_different_score_does_not_replace_original(self):
        self.start()
        capture.bind(score_payload_sha256="c" * 64)
        capsule = self.sealed()
        self.assertEqual(capsule["status"], "PARTIAL")
        self.assertEqual(capsule["payload"]["score_payload_sha256"], "a" * 64)

    def test_nonfinite_numbers_are_unknown_without_native_change(self):
        self.start()
        self.observe(state={"value": float("nan")})
        capsule = self.sealed()
        self.assertEqual(capsule["status"], "PARTIAL")
        json.loads(capture.canonical_bytes(capsule))


def run():
    result = unittest.TextTestRunner(verbosity=1).run(unittest.defaultTestLoader.loadTestsFromTestCase(CaptureTests))
    if not result.wasSuccessful():
        raise SystemExit(1)
    print("research_alert_decision_capture_selftest: PASS (%s tests)" % result.testsRun)


if __name__ == "__main__":
    run()
