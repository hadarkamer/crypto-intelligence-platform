"""Offline safety tests. Synthetic tasks never contact a source or AI provider."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("experiment", HERE / "coinglass_concurrency_20261031.py")
experiment = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(experiment)


class ExperimentTests(unittest.TestCase):
    def test_credentials_allowlist_strips_database_and_unrelated_keys(self):
        source = {"OPENAI_API_KEY": "synthetic-ai", "COINGLASS_COOKIE_HEADER": "synthetic-session",
                  "DATABASE_URL": "must-not-pass", "SUPABASE_SERVICE_ROLE_KEY": "must-not-pass",
                  "RENDER_API_KEY": "must-not-pass", "LOVABLE_API_KEY": "must-not-pass",
                  "PYTHONPATH": "must-not-pass", "COLLECTION_BRIDGE_ENABLED": "true"}
        child = experiment.safe_environment(source)
        self.assertEqual(child["DATABASE_URL"], "")
        self.assertEqual(child["SUPABASE_SERVICE_ROLE_KEY"], "")
        self.assertEqual(child["COLLECTION_BRIDGE_ENABLED"], "false")
        self.assertNotIn("RENDER_API_KEY", child)
        self.assertNotIn("LOVABLE_API_KEY", child)
        self.assertNotIn("PYTHONPATH", child)
        self.assertEqual(child["OPENAI_API_KEY"], "synthetic-ai")

    def test_identity_and_image_tampering_are_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            image = b"synthetic-image"
            (root / "image.png").write_bytes(image)
            result = {"run_id": "one", "symbol": "BTC", "heatmap_model": 3, "timeframe": "12H",
                      "provider": "OpenAI", "model": "synthetic", "hover_snapshot": {"complete": True},
                      "evidence": {"artifact_id": "one", "sha256": hashlib.sha256(image).hexdigest()}}
            (root / "result.json").write_text(json.dumps(result))
            self.assertTrue(experiment.validate_result(root, "one", "12H")[0])
            self.assertFalse(experiment.validate_result(root, "two", "12H")[0])
            self.assertFalse(experiment.validate_result(root, "one", "24H")[0])
            (root / "image.png").write_bytes(b"changed")
            self.assertEqual(experiment.validate_result(root, "one", "12H")[1], "image_digest_mismatch")

    def test_process_tree_includes_child_and_reparented_group_member(self):
        stats = {10: {"ppid": 1, "group": 10}, 11: {"ppid": 10, "group": 11},
                 12: {"ppid": 11, "group": 11}, 13: {"ppid": 1, "group": 10},
                 14: {"ppid": 1, "group": 14}}
        self.assertEqual(experiment.process_members(10, stats), {10, 11, 12, 13})

    def test_group_cleanup_does_not_target_unrelated_process(self):
        owned = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(30)"], start_new_session=True)
        other = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(30)"], start_new_session=True)
        try:
            experiment.clean_group(owned)
            self.assertIsNotNone(owned.poll())
            self.assertIsNone(other.poll())
        finally:
            experiment.clean_group(other)

    def test_hard_job_limit_never_launches_thirteenth(self):
        with tempfile.TemporaryDirectory() as d, patch.object(experiment.subprocess, "Popen") as spawn:
            batch = experiment.run_batch(Path(d), Path(d), ("12H", "24H"), "parallel", 1,
                                         time.monotonic() + 3, {"launched": 12})
            spawn.assert_not_called()
            self.assertEqual(batch["stop_reason"], "experiment_limit")

    def test_evidence_chunks_round_trip_and_digest(self):
        import base64, gzip
        report = {"value": "בדיקה", "jobs": list(range(12000))}
        events = []
        with patch.object(experiment, "emit", side_effect=lambda name, **kw: events.append((name, kw))):
            experiment.emit_evidence(report)
        chunks = [kw for name, kw in events if name == "evidence_chunk"]
        raw = gzip.decompress(base64.b64decode("".join(c["data"] for c in chunks)))
        self.assertEqual(json.loads(raw), report)
        self.assertEqual(hashlib.sha256(raw).hexdigest(), events[-1][1]["sha256"])
        self.assertTrue(all(len(c["data"]) <= 6000 for c in chunks))

    def test_observation_wrapper_keeps_failures_and_stage_boundaries(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            runtime = root / "runtime"
            runtime.mkdir()
            (runtime / "model1_execution.py").write_text(
                "import contextlib\n_analysis_facts={}\n"
                "def stage(value): pass\n"
                "@contextlib.contextmanager\ndef capture_operation(name): yield\n"
                "def remember_analysis(raw): _analysis_facts.update(readable=True)\n")
            (runtime / "collection_model1_task.py").write_text(
                "from model1_execution import stage,capture_operation,remember_analysis\n"
                "stage('capture')\n"
                "with capture_operation('browser_launch'): pass\n"
                "stage('analysis')\nremember_analysis({})\nraise SystemExit(7)\n")
            output = root / "output"
            process = subprocess.run([sys.executable, str(HERE / "coinglass_concurrency_20261031.py"),
                                      "--child", "--runtime", str(runtime), "--output", str(output),
                                      "--timeframe", "12H", "--job-id", "synthetic"],
                                     env=experiment.safe_environment(os.environ), capture_output=True, timeout=10)
            self.assertEqual(process.returncode, 7)
            measurement = json.loads((output / "measurement.json").read_text())
            self.assertEqual(measurement["exit_code"], 7)
            self.assertEqual(measurement["forbidden_modules_loaded"], [])
            self.assertEqual(measurement["analysis_facts"], {"readable": True})
            stages = [x["name"] for x in measurement["events"] if x["kind"] == "stage"]
            self.assertEqual(stages, ["startup", "capture", "analysis"])

    def test_complete_twelve_job_protocol_with_synthetic_capture_and_analysis(self):
        import base64, gzip
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            runtime = root / "runtime"
            (runtime / "market_vision").mkdir(parents=True)
            (runtime / "model1_execution.py").write_text(
                "import contextlib\n_analysis_facts={}\n"
                "def stage(value): pass\n"
                "@contextlib.contextmanager\ndef capture_operation(name): yield\n"
                "def remember_analysis(raw): _analysis_facts.update(readable=True)\n")
            for name in ("heatmap_hover.py", "market_vision/coinglass_heatmap_capture.py", "market_vision/openai_heatmap_scanner.py"):
                (runtime / name).write_text("# Synthetic offline fixture\n")
            (runtime / "provenance.json").write_text('{"synthetic":true}')
            (runtime / "collection_model1_task.py").write_text(
                "import sys,json,time,hashlib,os\nfrom pathlib import Path\n"
                "from model1_execution import stage,capture_operation,remember_analysis\n"
                "assert not os.getenv('DATABASE_URL')\nassert not os.getenv('SUPABASE_SERVICE_ROLE_KEY')\n"
                "tf,jid,out=sys.argv[1:]; p=Path(out)\n"
                "stage('capture')\n"
                "with capture_operation('browser_launch'): time.sleep(.15)\n"
                "stage('analysis');time.sleep(.15);remember_analysis({})\n"
                "image=b'synthetic-only';(p/'image.png').write_bytes(image)\n"
                "stage('saving')\n"
                "result={'run_id':jid,'symbol':'BTC','heatmap_model':3,'timeframe':tf,"
                "'provider':'OpenAI','model':'synthetic','hover_snapshot':{'complete':True},"
                "'evidence':{'artifact_id':jid,'sha256':hashlib.sha256(image).hexdigest()}}\n"
                "(p/'result.json').write_text(json.dumps(result))\n")
            events = []
            env = {"PATH": os.environ.get("PATH", ""), "HOME": str(root),
                   "CG2_ISOLATED_ONE_OFF": "1", "RENDER_SERVICE_ID": experiment.SERVICE,
                   "RENDER_GIT_COMMIT": experiment.BASE_SHA,
                   "COINGLASS_COOKIE_HEADER": "synthetic-only", "OPENAI_API_KEY": "synthetic-only",
                   "OPENAI_MARKET_SCANNER_MODEL": "gpt-5.4-mini", "DATABASE_URL": "must-be-removed"}
            cgroup = {"available": True, "memory.max": 4*1024**3, "memory.current": 10000000,
                      "cpu.max": "200000 100000", "cpu.stat": {}, "memory.events": {}}
            # This host virtualizes subprocess PIDs independently of /proc.
            # Use an explicitly synthetic resource observer for this protocol
            # test; process cancellation is separately tested via Popen.poll.
            real_getpid = int(Path("/proc/self/stat").read_text().split(" ", 1)[0])
            with patch.dict(os.environ, env, clear=True), \
                 patch.object(sys, "argv", ["experiment", "--runtime", str(runtime), "--run"]), \
                 patch.object(experiment, "cgroup_snapshot", return_value=cgroup), \
                 patch.object(experiment.os, "getpid", return_value=real_getpid), \
                 patch.object(experiment, "proc_stats", return_value={}), \
                 patch.object(experiment, "emit", side_effect=lambda name, **kw: events.append((name, kw))):
                self.assertEqual(experiment.main(), 0, [e for e in events if e[0] == "job_finished"])
            chunks = [kw for name, kw in events if name == "evidence_chunk"]
            report = json.loads(gzip.decompress(base64.b64decode("".join(c["data"] for c in chunks))))
            self.assertEqual(report["jobs_launched"], 12)
            jobs = [j for b in report["batches"] for j in b["jobs"]]
            self.assertEqual(len({j["id"] for j in jobs}), 12)
            self.assertTrue(all(j["valid"] for j in jobs))
            self.assertTrue(all(j["surviving_process_count"] == 0 for j in jobs))
            pairs = [b for b in report["batches"] if b["mode"] == "parallel"]
            self.assertEqual(len(pairs), 3)
            from datetime import datetime
            for pair in pairs:
                a, b = [j["measurement"] for j in pair["jobs"]]
                self.assertLess(datetime.fromisoformat(a["started_at"]), datetime.fromisoformat(b["finished_at"]))
                self.assertLess(datetime.fromisoformat(b["started_at"]), datetime.fromisoformat(a["finished_at"]))


if __name__ == "__main__":
    unittest.main()
