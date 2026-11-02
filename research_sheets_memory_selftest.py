"""Offline semantic checks for the pure Sheets sizing library."""
from collections import Counter, OrderedDict
from datetime import datetime, timezone
import json
import sys
import unittest
import weakref

import research_sheets_memory as memory


class SheetsMemoryTests(unittest.TestCase):
    def test_alias_sharing_and_cycle_count_once(self):
        shared = ["private-payload-sentinel", 140513]
        left = [shared, shared]
        right = {"node": shared}
        left.append(left)
        receipt = memory.measure_roots({"children": left, "upserts": right})
        self.assertTrue(receipt["complete"], receipt)
        expected_left = sum(sys.getsizeof(value) for value in (left, shared, shared[0], shared[1]))
        expected_right = sum(sys.getsizeof(value) for value in (right, "node", shared, shared[0], shared[1]))
        self.assertEqual(receipt["roots"]["children"]["inclusive_bytes"], expected_left)
        self.assertEqual(receipt["roots"]["upserts"]["inclusive_bytes"], expected_right)
        overlap = sum(sys.getsizeof(value) for value in (shared, shared[0], shared[1]))
        self.assertEqual(receipt["shared_union_bytes"], overlap)
        self.assertEqual(receipt["union_bytes"], expected_left + expected_right - overlap)

    def test_node_bound_is_coverage_never_total(self):
        receipt = memory.measure_roots({"children": [object(), ["a", "b"]]}, max_nodes=2)
        self.assertFalse(receipt["complete"])
        self.assertIsNone(receipt["union_bytes"])
        row = receipt["roots"]["children"]
        self.assertGreater(row["covered_bytes"], 0)
        self.assertIsNone(row["inclusive_bytes"])
        self.assertIn("NODE_BOUND", receipt["reason_codes"])

    def test_workspace_bound_is_not_total(self):
        receipt = memory.measure_roots({"children": [1, 2]}, max_workspace_bytes=1024)
        self.assertFalse(receipt["complete"])
        self.assertIn("WORKSPACE_BOUND", receipt["reason_codes"])
        self.assertIsNone(receipt["union_bytes"])

    def test_unsupported_never_calls_hooks_or_foreign_metaclass(self):
        calls = []
        class HostileMeta(type):
            def __hash__(self):
                calls.append("meta_hash")
                raise AssertionError("must not hash class")
        class Hostile(metaclass=HostileMeta):
            def __sizeof__(self):
                calls.append("size")
                raise AssertionError("must not size")
            def __iter__(self):
                calls.append("iter")
                raise AssertionError("must not iterate")
            def __repr__(self):
                calls.append("repr")
                raise AssertionError("must not repr")
        class HostileList(list):
            def __sizeof__(self):
                calls.append("list_size")
                raise AssertionError("must not size subclass")
        receipt = memory.measure_roots({"children": [Hostile(), HostileList()]})
        self.assertFalse(receipt["complete"])
        self.assertIn("UNSUPPORTED_TYPE", receipt["reason_codes"])
        self.assertEqual(calls, [])

    def test_primitive_receipt_has_no_contents_or_addresses(self):
        sentinel = "DO-NOT-EXPORT-THIS-BUSINESS-VALUE-928179"
        value = {sentinel: [sentinel]}
        receipt = memory.measure_roots({"children": value})
        serialized = json.dumps(receipt, allow_nan=False)
        self.assertNotIn(sentinel, serialized)
        self.assertNotIn(str(id(value)), serialized)
        self.assertNotIn(hex(id(value)), serialized)
        self.assertIsNone(memory.measure_roots({sentinel: value})["union_bytes"])
        def check(item):
            self.assertIn(type(item), (dict, list, str, int, float, bool, type(None)))
            if type(item) is dict:
                for key, child in item.items():
                    self.assertIs(type(key), str)
                    check(child)
            elif type(item) is list:
                for child in item:
                    check(child)
        check(receipt)

    def test_no_strong_reference_survives_success_or_partial(self):
        for limit in (120000, 1):
            value = OrderedDict((str(index), index) for index in range(12))
            reference = weakref.ref(value)
            receipt = memory.measure_roots({"snapshot_cache": value}, max_nodes=limit)
            del value
            self.assertIsNone(reference(), receipt)

    def test_time_bound_with_deterministic_clock(self):
        old_clock = memory._clock
        tick = [0.]
        def clock():
            tick[0] += .01
            return tick[0]
        memory._clock = clock
        try:
            receipt = memory.measure_roots({"children": list(range(300))}, max_seconds=.025)
        finally:
            memory._clock = old_clock
        self.assertFalse(receipt["complete"])
        self.assertIsNone(receipt["union_bytes"])
        self.assertIn("TIME_BOUND", receipt["reason_codes"])

    def test_exact_datetime_and_counter_supported_projection(self):
        payload = {"when": datetime(2026, 11, 2, tzinfo=timezone.utc), "count": Counter(a=2)}
        receipt = memory.measure_roots({"audit_expected": payload})
        self.assertTrue(receipt["complete"], receipt)
        self.assertIn("tzinfo", receipt["projection_exclusions"])

    def test_depth_bound(self):
        value = []
        for _ in range(memory.MAX_DEPTH + 4):
            value = [value]
        receipt = memory.measure_roots({"children": value})
        self.assertFalse(receipt["complete"])
        self.assertIn("DEPTH_BOUND", receipt["reason_codes"])
        self.assertIsNone(receipt["union_bytes"])

    def test_repeated_aliases_still_obey_edge_budget(self):
        receipt = memory.measure_roots({"children": [111] * 2000}, max_nodes=2)
        self.assertFalse(receipt["complete"])
        self.assertIn("EDGE_BOUND", receipt["reason_codes"])
        self.assertIsNone(receipt["union_bytes"])

    def test_counter_custom_size_and_length_hooks_are_bypassed(self):
        value = Counter(a=2)
        def hostile(*_):
            raise AssertionError("Counter hook must not be called")
        Counter.__sizeof__ = hostile
        Counter.__len__ = hostile
        try:
            receipt = memory.measure_roots({"audit_seen": value})
        finally:
            del Counter.__sizeof__
            del Counter.__len__
        self.assertTrue(receipt["complete"], receipt)

    def test_observed_mutation_invalidates_shared_totals(self):
        value = list(range(300))
        roots = {"children": value, "upserts": value}
        old_clock = memory._clock
        ticks = [0]
        def clock():
            ticks[0] += 1
            if ticks[0] == 3:
                value.append(900001)
            return ticks[0] * .0001
        memory._clock = clock
        try:
            receipt = memory.measure_roots(roots)
        finally:
            memory._clock = old_clock
        self.assertEqual(receipt["status"], "CONCURRENT", receipt)
        self.assertFalse(receipt["measurement_valid"])
        self.assertIsNone(receipt["union_bytes"])
        self.assertTrue(all(row["inclusive_bytes"] is None for row in receipt["roots"].values()))

    def test_invalid_configuration_does_not_export_foreign_keys(self):
        for options in ({"max_seconds": float("nan")}, {"max_nodes": True},
                        {"max_workspace_bytes": memory.MAX_WORKSPACE_BYTES + 1}):
            receipt = memory.measure_roots({"children": []}, **options)
            self.assertEqual(receipt["status"], "UNAVAILABLE")


if __name__ == "__main__":
    unittest.main()
