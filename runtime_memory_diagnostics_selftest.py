"""Synthetic filesystem regressions; no process commands or live services."""
from contextlib import ExitStack, redirect_stdout
import io
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import runtime_memory_diagnostics as diagnostics


def stat(pid, parent, pages, name="python3", start=100):
    fields = ["0"] * 22
    fields[0], fields[1], fields[19], fields[21] = "S", str(parent), str(start), str(pages)
    return f"{pid} ({name}) " + " ".join(fields) + "\n"


class Entries:
    def __init__(self, names):
        self.names = names
        self.seen = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def __iter__(self):
        for name in self.names:
            self.seen += 1
            yield SimpleNamespace(name=str(name))


class Fixture:
    def __init__(self):
        self.files = {
            "/proc/self/stat": stat(10, 1, 3),
            "/proc/20/stat": stat(20, 10, 5, "node"),
            "/proc/30/stat": stat(30, 20, 7, "chrome"),
            "/proc/40/stat": stat(40, 1, 11, "chrome_crashpad"),
            "/proc/50/stat": stat(50, 30, 13, "private) sensitive name"),
            "/proc/self/cgroup": "0::/fixture\n",
            "/sys/fs/cgroup/fixture/memory.current": "123456\n",
            "/sys/fs/cgroup/fixture/memory.max": "2147483648\n",
            "/sys/fs/cgroup/fixture/memory.stat": "anon 1000\nfile 2000\nsecretignored 999\n",
            "/sys/fs/cgroup/fixture/memory.events": "low 0\nhigh 1\nmax 2\noom 3\noom_kill 4\n",
        }
        self.paths = []
        self.entries = Entries(["self", "10", "30", "40", "20", "50"])

    def open(self, path, mode):
        path = str(path)
        self.paths.append(path)
        assert mode == "rb", mode
        value = self.files.get(path, FileNotFoundError("private missing path"))
        if isinstance(value, Exception):
            raise value
        return io.BytesIO(value.encode() if isinstance(value, str) else value)

    def install(self, stack):
        stack.enter_context(patch("builtins.open", side_effect=self.open))
        stack.enter_context(patch.object(diagnostics.os, "scandir", return_value=self.entries))
        stack.enter_context(patch.object(diagnostics.os, "getpid", return_value=10))
        stack.enter_context(patch.object(diagnostics.os, "sysconf", return_value=4096))
        stack.enter_context(patch.object(diagnostics.time, "monotonic", return_value=0))


class MemoryDiagnosticsTests(unittest.TestCase):
    def sample(self, fixture=None, phase="collector_start", **kwargs):
        fixture = fixture or Fixture()
        with ExitStack() as stack:
            fixture.install(stack)
            output = io.StringIO()
            with redirect_stdout(output):
                diagnostics.emit_memory_sample(phase, **kwargs)
        text = output.getvalue()
        self.assertTrue(text.startswith(diagnostics.PREFIX + " "))
        self.assertEqual(len(text.splitlines()), 1)
        return json.loads(text[len(diagnostics.PREFIX) + 1:]), text, fixture

    def test_current_rss_cgroup_and_child_attribution(self):
        value, text, fixture = self.sample(pages=1, counters={"history_rows": 42})
        self.assertFalse(value["partial"])
        self.assertEqual(value["main_rss_bytes"], 3 * 4096)
        self.assertEqual(value["process_start_ticks"], 100)
        self.assertEqual(value["process_tree"]["rss_bytes"], (3 + 5 + 7 + 13) * 4096)
        self.assertEqual(value["process_tree"]["processes"], 4)
        self.assertEqual(value["process_tree"]["categories"]["chromium"]["processes"], 1)
        self.assertEqual(value["process_tree"]["visible_categories"]["chromium"]["processes"], 2)
        self.assertEqual(value["process_tree"]["visible_rss_bytes"], (3 + 5 + 7 + 11 + 13) * 4096)
        self.assertEqual(value["cgroup"]["current_bytes"], 123456)
        self.assertEqual(value["cgroup"]["max_bytes"], 2147483648)
        self.assertFalse(value["cgroup"]["max_unlimited"])
        self.assertEqual(value["cgroup"]["stat"], {"anon": 1000, "file": 2000})
        self.assertEqual(value["cgroup"]["events"]["oom_kill"], 4)
        self.assertNotIn("sensitive", text)
        self.assertNotIn("secretignored", text)
        self.assertTrue(all(path.endswith("/stat") or path == "/proc/self/cgroup" or
                            path.startswith("/sys/fs/cgroup/fixture/memory.")
                            for path in fixture.paths))
        self.assertFalse(any("cmdline" in path or "environ" in path for path in fixture.paths))

    def test_unlimited_memory(self):
        fixture = Fixture()
        fixture.files["/sys/fs/cgroup/fixture/memory.max"] = "max\n"
        value, _, _ = self.sample(fixture)
        self.assertIsNone(value["cgroup"]["max_bytes"])
        self.assertTrue(value["cgroup"]["max_unlimited"])

    def test_proc_pid_namespace_can_differ_from_runtime_pid(self):
        fixture = Fixture()
        fixture.files["/proc/self/stat"] = stat(77, 1, 3)
        fixture.files["/proc/20/stat"] = stat(20, 77, 5, "node")
        fixture.entries = Entries(["self", "77", "30", "40", "20", "50"])
        value, _, _ = self.sample(fixture)
        self.assertEqual(value["pid"], 10)
        self.assertEqual(value["proc_pid"], 77)
        self.assertTrue(value["pid_namespace_differs"])
        self.assertEqual(value["main_rss_bytes"], 3 * 4096)
        self.assertEqual(value["process_tree"]["root_proc_pid"], 77)
        self.assertEqual(value["process_tree"]["processes"], 4)
        self.assertEqual(value["process_tree"]["rss_bytes"], (3 + 5 + 7 + 13) * 4096)
        self.assertFalse(value["partial"])

    def test_identity_stable_and_refreshes_after_fork(self):
        first, _, _ = self.sample()
        second, _, _ = self.sample()
        self.assertEqual(first["boot_uuid"], second["boot_uuid"])
        self.assertEqual(first["diagnostics_initialized_at_utc"], second["diagnostics_initialized_at_utc"])
        child = diagnostics._identity(999)
        self.assertNotEqual(first["boot_uuid"], child["boot_uuid"])

    def test_permission_and_disappearing_process_are_partial(self):
        fixture = Fixture()
        fixture.files["/proc/20/stat"] = PermissionError("PRIVATE credential")
        fixture.files.pop("/proc/30/stat")
        value, text, _ = self.sample(fixture)
        self.assertTrue(value["partial"])
        self.assertIn("proc_stat_unavailable", value["partial_reasons"])
        self.assertNotIn("PRIVATE", text)
        self.assertNotIn("credential", text)

    def test_truncation_never_parsed_as_complete(self):
        fixture = Fixture()
        fixture.files["/proc/self/stat"] = stat(10, 1, 3) + "x" * diagnostics.MAX_FILE_BYTES
        value, _, _ = self.sample(fixture)
        self.assertIsNone(value["main_rss_bytes"])
        self.assertIn("main_stat_truncated", value["partial_reasons"])
        self.assertFalse(value["process_tree"]["includes_main_process"])
        self.assertFalse(value["process_tree"]["attribution_available"])
        self.assertIsNone(value["process_tree"]["processes"])
        self.assertIsNone(value["process_tree"]["categories"])

    def test_malformed_fields_and_duplicate_cgroup_keys(self):
        fixture = Fixture()
        fixture.files["/proc/self/stat"] = "malformed SECRET"
        fixture.files["/proc/20/stat"] = stat(99, 10, 5)
        fixture.files["/sys/fs/cgroup/fixture/memory.current"] = "-1"
        fixture.files["/sys/fs/cgroup/fixture/memory.stat"] = "anon 2\nanon 3\nfile 4\n"
        value, text, _ = self.sample(fixture)
        self.assertIsNone(value["main_rss_bytes"])
        self.assertIsNone(value["cgroup"]["current_bytes"])
        self.assertIsNone(value["cgroup"]["stat"])
        self.assertTrue({"main_stat_malformed", "proc_stat_malformed", "cgroup_current_bytes_malformed", "cgroup_stat_malformed"}.issubset(value["partial_reasons"]))
        self.assertNotIn("SECRET", text)

    def test_unknown_self_keeps_unrelated_matching_pid_in_visible_totals(self):
        fixture = Fixture()
        fixture.files["/proc/self/stat"] = "malformed"
        fixture.files["/proc/10/stat"] = stat(10, 1, 17, "node")
        value, _, _ = self.sample(fixture)
        self.assertFalse(value["process_tree"]["attribution_available"])
        self.assertIsNone(value["process_tree"]["processes"])
        self.assertEqual(value["process_tree"]["visible_processes"], 5)
        self.assertEqual(value["process_tree"]["visible_rss_bytes"], (17 + 5 + 7 + 11 + 13) * 4096)

    def test_invalid_utf8_and_missing_cgroup_keys(self):
        fixture = Fixture()
        fixture.files["/proc/20/stat"] = b"\xff"
        fixture.files["/sys/fs/cgroup/fixture/memory.events"] = "oom_kill 1\n"
        value, _, _ = self.sample(fixture)
        self.assertIn("proc_stat_unavailable", value["partial_reasons"])
        self.assertIn("cgroup_events_malformed", value["partial_reasons"])

    def test_cgroup_membership_does_not_escape_or_fallback(self):
        for membership in ("0::/../../private\n", "1:memory:/elsewhere\n", "0::/one\n0::/two\n"):
            fixture = Fixture()
            fixture.files["/proc/self/cgroup"] = membership
            value, _, _ = self.sample(fixture)
            self.assertIn("cgroup_membership_malformed", value["partial_reasons"])
            self.assertFalse(any(path.startswith("/sys/") for path in fixture.paths))
        fixture = Fixture()
        fixture.files["/proc/self/cgroup"] = "0::/missing-child\n"
        value, _, _ = self.sample(fixture)
        self.assertIsNone(value["cgroup"]["current_bytes"])
        self.assertNotIn("/sys/fs/cgroup/memory.current", fixture.paths)

    def test_payload_is_whitelisted_integer_aggregates_only(self):
        for counters in ({"history_rows": "SECRET", "password": 123, "active_watch": True},
                         {"history_rows": -1, "windows": 2**64}, ["SECRET"]):
            value, text, _ = self.sample(phase="SECRET", pages="SECRET", counters=counters)
            self.assertEqual(value["counters"], {})
            self.assertNotIn("SECRET", text)
            self.assertNotIn("password", text)
            self.assertNotIn("pages", value)
            self.assertTrue({"invalid_phase", "invalid_pages", "invalid_counters"}.issubset(value["partial_reasons"]))

    def test_excess_counter_items_are_not_iterated(self):
        value, text, _ = self.sample(counters={"SECRET" + str(i): i for i in range(100)})
        self.assertEqual(value["counters"], {})
        self.assertNotIn("SECRET", text)

    def test_process_entry_budget(self):
        fixture = Fixture()
        fixture.entries = Entries(range(100, 10000))
        with patch.object(diagnostics, "MAX_PROC_ENTRIES", 3):
            value, _, _ = self.sample(fixture)
        self.assertEqual(value["process_tree"]["proc_entries_examined"], 3)
        self.assertLessEqual(fixture.entries.seen, 4)
        self.assertEqual(sum(path.startswith("/proc/1") for path in fixture.paths), 3)
        self.assertIn("proc_entry_budget", value["partial_reasons"])

    def test_byte_and_time_budgets(self):
        with patch.object(diagnostics, "MAX_TOTAL_BYTES", 20):
            value, _, fixture = self.sample()
        self.assertLessEqual(value["bytes_read"], 21)
        self.assertEqual(len(fixture.paths), 1)
        self.assertIn("byte_budget", value["partial_reasons"])
        with patch.object(diagnostics, "MAX_SAMPLE_SECONDS", 0):
            value, _, fixture = self.sample()
        self.assertEqual(fixture.paths, [])
        self.assertIn("time_budget", value["partial_reasons"])

    def test_unavailable_platform(self):
        fixture = Fixture()
        with ExitStack() as stack:
            fixture.install(stack)
            stack.enter_context(patch.object(diagnostics.os, "scandir", side_effect=PermissionError("SECRET")))
            stack.enter_context(patch.object(diagnostics.os, "sysconf", side_effect=ValueError("SECRET")))
            output = io.StringIO()
            with redirect_stdout(output):
                diagnostics.emit_memory_sample("runtime_poll")
        value = json.loads(output.getvalue().split(" ", 1)[1])
        self.assertIn("page_size_unavailable", value["partial_reasons"])
        self.assertIn("proc_scan_unavailable", value["partial_reasons"])
        self.assertIsNone(value["main_rss_bytes"])
        self.assertIsNone(value["process_tree"]["rss_bytes"])
        self.assertNotIn("SECRET", output.getvalue())

    def test_sampling_and_output_failure_isolation(self):
        output = io.StringIO()
        with patch.object(diagnostics, "_sample", side_effect=RuntimeError("SECRET")), redirect_stdout(output):
            self.assertIsNone(diagnostics.emit_memory_sample("collector_start"))
        value = json.loads(output.getvalue().split(" ", 1)[1])
        self.assertEqual(value["partial_reasons"], ["sample_failed"])
        self.assertNotIn("SECRET", output.getvalue())
        with patch.object(diagnostics, "_sample", return_value={}), patch("builtins.print", side_effect=BrokenPipeError("SECRET")):
            self.assertIsNone(diagnostics.emit_memory_sample("collector_end"))


if __name__ == "__main__":
    unittest.main()
