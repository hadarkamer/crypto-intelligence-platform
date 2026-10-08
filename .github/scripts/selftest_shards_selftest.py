"""Shard completion evidence rejects lost, duplicated and mismatched coverage."""
from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import call, patch

_spec = importlib.util.spec_from_file_location("selftest_shards", Path(__file__).with_name("selftest_shards.py"))
helper = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(helper)


class ShardTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.snapshot = {"commit": "a" * 40, "tree": "b" * 40,
                         "scripts": [f"case_{index}_selftest.py" for index in range(9)]}
        mocked = patch.object(helper, "_snapshot", return_value=self.snapshot)
        mocked.start()
        self.addCleanup(mocked.stop)
        quiet = redirect_stdout(io.StringIO())
        quiet.__enter__()
        self.addCleanup(quiet.__exit__, None, None, None)

    def write_receipts(self):
        for index in range(4):
            scripts = self.snapshot["scripts"][index::4]
            self.save(index, helper._manifest(self.snapshot, index, 4, scripts))

    def save(self, index, value):
        (self.root / f"ci-selftests-{index}.json").write_text(helper.canonical(value), encoding="utf-8")

    def test_run_uses_exact_python_arguments_and_order_then_seals_completed_partition(self):
        output = self.root / "ci-selftests-0.json"
        with patch.object(helper.subprocess, "run") as child:
            receipt = helper.run_shard(0, 4, output)
        scripts = self.snapshot["scripts"][::4]
        self.assertEqual(child.call_args_list, [call([sys.executable, script], check=True) for script in scripts])
        self.assertEqual(receipt["planned_scripts"], scripts)
        self.assertEqual(receipt["completed_scripts"], scripts)
        self.assertEqual(output.read_text(), helper.canonical(receipt) + "\n")

    def test_failed_child_stops_partition_and_cannot_write_completed_manifest(self):
        output = self.root / "ci-selftests-0.json"
        with patch.object(helper.subprocess, "run", side_effect=[None, subprocess.CalledProcessError(1, ["python"])]) as child:
            with self.assertRaises(subprocess.CalledProcessError):
                helper.run_shard(0, 4, output)
        self.assertEqual(child.call_count, 2)
        self.assertFalse(output.exists())

    def test_existing_receipt_is_preserved_before_any_child_runs(self):
        output = self.root / "ci-selftests-0.json"
        output.write_text("existing evidence")
        with patch.object(helper.subprocess, "run") as child, self.assertRaises(ValueError):
            helper.run_shard(0, 4, output)
        child.assert_not_called()
        self.assertEqual(output.read_text(), "existing evidence")

    def test_complete_receipts_cover_inventory_once_and_cli_verifies(self):
        self.write_receipts()
        self.assertEqual(helper.verify_shards(4, self.root), 9)
        self.assertEqual(helper.main(["verify", "--shard-count", "4", "--manifests-directory", str(self.root)]), 0)

    def test_missing_or_extra_receipt_is_not_partial_success(self):
        self.write_receipts()
        path = self.root / "ci-selftests-3.json"
        original = path.read_text()
        path.unlink()
        with self.assertRaisesRegex(ValueError, "complete shard"):
            helper.verify_shards(4, self.root)
        path.write_text(original)
        (self.root / "ci-selftests-4.json").write_text(original)
        with self.assertRaisesRegex(ValueError, "complete shard"):
            helper.verify_shards(4, self.root)

    def test_wrong_checkout_rehashed_inventory_and_duplicate_or_lost_completion_reject(self):
        for mutation in ("commit", "tree", "digest", "count", "id", "planned", "duplicate", "missing", "unknown", "version"):
            self.write_receipts()
            scripts = self.snapshot["scripts"][::4]
            receipt = helper._manifest(self.snapshot, 0, 4, deepcopy(scripts))
            if mutation in ("commit", "tree"):
                receipt["checkout_" + mutation + "_sha"] = "c" * 40
            elif mutation == "digest":
                changed = {**self.snapshot, "scripts": self.snapshot["scripts"][:-1]}
                receipt["inventory_sha256"] = helper._manifest(changed, 0, 4, scripts)["inventory_sha256"]
            elif mutation == "count": receipt["inventory_count"] -= 1
            elif mutation == "id": receipt["shard_index"] = 1
            elif mutation == "planned": receipt["planned_scripts"] = list(reversed(scripts))
            elif mutation == "duplicate": receipt["completed_scripts"].append(scripts[0])
            elif mutation == "missing": receipt["completed_scripts"].pop()
            elif mutation == "unknown": receipt["completed_scripts"][-1] = "untracked_selftest.py"
            else: receipt["version"] = "obsolete"
            self.save(0, receipt)
            with self.subTest(mutation=mutation), self.assertRaisesRegex(ValueError, "mismatch"):
                helper.verify_shards(4, self.root)

    def test_duplicate_json_keys_reject_and_cli_reports_failed_verification(self):
        self.write_receipts()
        (self.root / "ci-selftests-0.json").write_text('{"shard_index":0,"shard_index":0}')
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            helper.verify_shards(4, self.root)
        error = io.StringIO()
        with redirect_stderr(error):
            self.assertEqual(helper.main(["verify", "--shard-count", "4", "--manifests-directory", str(self.root)]), 1)
        self.assertIn("SELFTEST_SHARDS_FAILED: ValueError", error.getvalue())

    def test_git_inventory_is_nul_delimited_sorted_and_bound_to_head_tree(self):
        outputs = [b"nested/z_selftest.py\0space name_selftest.py\0a_selftest.py\0", b"a" * 40 + b"\n", b"b" * 40 + b"\n"]
        # Restore the actual snapshot function while keeping Git entirely mocked.
        with patch.object(helper, "_snapshot", wraps=_SNAPSHOT) as snapshot, \
             patch.object(helper, "_git", side_effect=outputs) as git:
            value = snapshot()
        self.assertEqual(value["scripts"], ["a_selftest.py", "nested/z_selftest.py", "space name_selftest.py"])
        self.assertEqual(value["commit"], "a" * 40)
        self.assertEqual(value["tree"], "b" * 40)
        self.assertEqual(git.call_args_list, [call("ls-files", "-z", "--", "*_selftest.py"),
                                           call("rev-parse", "HEAD"), call("rev-parse", "HEAD^{tree}")])


_SNAPSHOT = helper._snapshot

if __name__ == "__main__":
    unittest.main()
