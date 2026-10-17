"""Local command workflow and immutable receipt export."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import research_no_horizon_local as cli
from research_no_horizon_replay_selftest import fixture


class LocalCommands(unittest.TestCase):
    def test_submit_bounded_run_status_and_export_without_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            snapshot = base / "snapshot.json"
            snapshot.write_text(json.dumps(fixture()), encoding="utf-8")
            database = base / "research.sqlite"

            def run(*args):
                return subprocess.run([sys.executable, cli.__file__, "--database", str(database), *args],
                    capture_output=True, text=True)

            self.assertEqual(run("status", "missing").returncode, 2)
            self.assertFalse(database.exists())
            submitted = run("submit", str(snapshot), "--job-key", "fixture")
            self.assertEqual(submitted.returncode, 0, submitted.stderr)
            job = json.loads(submitted.stdout)["job_id"]
            partial = run("run", "--job-id", job, "--worker-id", "cli", "--candle-budget", "2")
            self.assertEqual(partial.returncode, 0, partial.stderr)
            self.assertEqual(json.loads(partial.stdout)["status"], "PENDING")
            output = base / "receipt.json"
            self.assertEqual(run("receipt", job, "--output", str(output)).returncode, 2)
            self.assertFalse(output.exists())
            self.assertEqual(run("run", "--job-id", job, "--worker-id", "cli").returncode, 0)
            self.assertEqual(json.loads(run("status", job).stdout)["status"], "COMPLETE")
            self.assertEqual(run("receipt", job, "--output", str(output)).returncode, 0)
            saved = output.read_bytes()
            self.assertEqual(run("receipt", job, "--output", str(output)).returncode, 2)
            self.assertEqual(output.read_bytes(), saved)


if __name__ == "__main__":
    unittest.main()
