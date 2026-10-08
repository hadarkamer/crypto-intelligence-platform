"""Focused local wrapper checks; no network, live registry, or market evidence.

Real original-eight checkpoint and PostgreSQL acceptance are run separately.
These checks exercise provenance seals, isolated restore files, source connection
configuration, and lock behavior without modifying preregistered requests.
"""
from contextlib import nullcontext
from copy import deepcopy
import fcntl
import io
import json
import os
from pathlib import Path
import select
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import threading
import traceback
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import zipfile

import psycopg
import research_no_horizon_registered_runner as runner


class RunnerTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="no-horizon-wrapper-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.work = self.root / "work"
        self.work.mkdir()
        self.args = SimpleNamespace(work_dir=self.work, source_id="fixture-source",
            source_url_env="NO_HORIZON_SELFTEST_SOURCE_URL", allow_local_source=False)
        self.identity = {"version": runner.VERSION, "transport": runner.MODE,
                         "source_id": self.args.source_id}
        self.rows = {"request": {"status": "WAITING", "proof_count": 0,
                                "executor_plan_id": None, "terminal_receipt": None}}
        self.store = Mock()
        self.store.report.side_effect = lambda key: deepcopy(self.rows[key])
        patcher = patch.object(runner, "store_ids", return_value=list(self.rows))
        patcher.start()
        self.addCleanup(patcher.stop)

    def seal_mode(self):
        runner.atomic_json(self.work / "native_transport_identity.json", self.identity)

    def seal_source(self, **changes):
        value = {"transport": runner.MODE, "source_id": self.args.source_id,
                 "endpoint_sha256": "a" * 64, "sslmode": "verify-full"}
        value.update(changes)
        runner.atomic_json(self.work / "native_source_identity.json", value)

    def test_exclusive_lock_rejects_same_directory_allows_other_and_releases(self):
        with runner.work_lock(self.work):
            with self.assertRaisesRegex(RuntimeError, "ALREADY_ACTIVE"):
                with runner.work_lock(self.work):
                    self.fail("same directory acquired twice")
            with runner.work_lock(self.root / "independent"):
                pass
        with runner.work_lock(self.work):
            pass

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux parent-death guard")
    def test_parent_sigkill_stops_guarded_node_while_retaining_lock_until_exit(self):
        node = os.getenv("CODEX_PRIMARY_RUNTIME_NODE") or shutil.which("node")
        if node is None:
            self.skipTest("Node runtime unavailable")
        script = self.root / "guard_fixture.mjs"
        script.write_text("""
process.on('SIGTERM', () => {
  console.log(JSON.stringify({stopping: true}));
  setTimeout(() => process.exit(0), 500);
});
console.log(JSON.stringify({ready: true, pid: process.pid}));
setInterval(() => {}, 1000);
""")
        parent_code = """
import json, os, subprocess, sys
env = dict(os.environ)
env.update(NO_HORIZON_RUNNER_PID=str(os.getpid()),
    NO_HORIZON_RUNNER_NODE=sys.argv[3], NO_HORIZON_REGISTRY_DATA_DIR=sys.argv[4])
child = subprocess.Popen([sys.argv[1], sys.argv[2]], env=env)
print(json.dumps({'spawned': child.pid}), flush=True)
child.wait()
"""
        guard = Path(runner.__file__).parent / "tools/no_horizon_guarded_node.py"
        parent = subprocess.Popen([sys.executable, "-u", "-c", parent_code, str(guard),
            str(script), node, str(self.work / "registry_data")], stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, bufsize=0)
        child_pid, child_exited = None, False
        lock = None
        def event():
            self.assertTrue(select.select([parent.stdout], [], [], 5)[0], "guard event timed out")
            line = parent.stdout.readline()
            self.assertTrue(line, "guard exited before expected event")
            return json.loads(line)
        try:
            spawned = event()
            child_pid = spawned["spawned"]
            ready = event()
            self.assertEqual(ready, {"ready": True, "pid": child_pid})
            lock = os.open(self.work / "registry-server.lock", os.O_RDWR)
            with self.assertRaises(BlockingIOError):
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            parent.kill()
            self.assertEqual(parent.wait(timeout=5), -signal.SIGKILL)
            self.assertEqual(event(), {"stopping": True})
            with self.assertRaises(BlockingIOError):
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertTrue(select.select([parent.stdout], [], [], 5)[0], "child exit timed out")
            self.assertEqual(parent.stdout.read(1), b"")
            child_exited = True
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertEqual(parent.stderr.read(), b"")
        finally:
            if parent.poll() is None:
                parent.kill()
                parent.wait(timeout=5)
            if child_pid is not None and not child_exited:
                try:
                    os.kill(child_pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            if lock is not None:
                os.close(lock)
            parent.stdout.close()
            parent.stderr.close()

    def test_source_factory_is_lazy_and_missing_config_never_connects(self):
        with patch.object(runner.os, "getenv", return_value="") as getenv, \
                patch.object(psycopg, "connect") as connect:
            factory = runner.source_factory(self.args)
            getenv.assert_not_called()
            connect.assert_not_called()
            with self.assertRaisesRegex(RuntimeError, "NOT_CONFIGURED"):
                factory()
            connect.assert_not_called()

    def test_remote_tls_readonly_budgets_and_endpoint_pin(self):
        dsn = "postgresql://reader:fixture-password@source.example/market?sslmode=disable"
        connection = Mock()
        with patch.dict(runner.os.environ, {self.args.source_url_env: dsn}), \
                patch.object(psycopg, "connect", return_value=connection) as connect:
            self.assertIs(runner.source_factory(self.args)(), connection)
            kwargs = connect.call_args.kwargs
            self.assertEqual(kwargs["sslmode"], "verify-full")
            self.assertEqual(kwargs["sslrootcert"], "system")
            self.assertEqual(kwargs["connect_timeout"], 5)
            self.assertIn("default_transaction_read_only=on", kwargs["options"])
            self.assertIn("statement_timeout=15000", kwargs["options"])
            saved = (self.work / "native_source_identity.json").read_text()
            self.assertNotIn("fixture-password", saved)
            self.assertNotIn("source.example", saved)
            self.assertEqual(json.loads(saved)["source_id"], self.args.source_id)
            connect.reset_mock()
            runner.os.environ[self.args.source_url_env] = dsn.replace("source.example", "other.example")
            with self.assertRaisesRegex(RuntimeError, "READ_SOURCE_CONNECTION_FAILED"):
                runner.source_factory(self.args)()
            connect.assert_not_called()

    def test_invalid_endpoints_rejected_before_connect(self):
        rejected = ("host=localhost user=reader dbname=market",
            "host=source.example,other.example user=reader dbname=market",
            "host=/tmp user=reader dbname=market",
            "host=source.example user=reader dbname=market options='-c read_only=off'",
            "host=source.example hostaddr=127.0.0.1 user=reader dbname=market",
            "host=source.example user=reader")
        with patch.object(psycopg, "connect") as connect:
            for dsn in rejected:
                with self.subTest(dsn=dsn), patch.dict(runner.os.environ, {self.args.source_url_env: dsn}):
                    with self.assertRaisesRegex(RuntimeError, "READ_SOURCE_CONNECTION_FAILED"):
                        runner.source_factory(self.args)()
            connect.assert_not_called()

    def test_local_source_requires_explicit_test_switch(self):
        self.args.allow_local_source = True
        with patch.dict(runner.os.environ, {self.args.source_url_env:
                "host=127.0.0.1 user=reader dbname=fixture port=5432"}), \
                patch.object(psycopg, "connect", return_value=Mock()) as connect:
            runner.source_factory(self.args)()
            self.assertEqual(connect.call_args.kwargs["sslmode"], "disable")
            self.assertNotIn("sslrootcert", connect.call_args.kwargs)

    def test_driver_failure_suppresses_credentials_and_does_not_seal_endpoint(self):
        secret = "fixture-private-password"
        with patch.dict(runner.os.environ, {self.args.source_url_env:
                f"postgresql://reader:{secret}@source.example/market"}), \
                patch.object(psycopg, "connect", side_effect=RuntimeError(secret)):
            try:
                runner.source_factory(self.args)()
            except RuntimeError as error:
                rendered = "".join(traceback.format_exception(error))
                self.assertEqual(str(error), "READ_SOURCE_CONNECTION_FAILED")
                self.assertNotIn(secret, rendered)
            else:
                self.fail("driver failure unexpectedly accepted")
        self.assertFalse((self.work / "native_source_identity.json").exists())

    def test_only_pristine_native_state_can_adopt_transport(self):
        for changed in ({"proof_count": 1}, {"status": "BLOCKED"}, {"executor_plan_id": "plan"}):
            with self.subTest(changed=changed):
                original = deepcopy(self.rows["request"])
                self.rows["request"].update(changed)
                with self.assertRaisesRegex(ValueError, "ONLY_UNACQUIRED"):
                    runner.native_mode(self.store, self.work, self.identity, adopt=True)
                self.rows["request"] = original
        self.assertTrue(runner.native_mode(self.store, self.work, self.identity, adopt=True))
        self.assertEqual(json.loads((self.work / "native_transport_identity.json").read_bytes()), self.identity)

    def test_external_transport_evidence_cannot_be_relabelled_native(self):
        (self.work / "transport_evidence").mkdir()
        with self.assertRaisesRegex(ValueError, "EXTERNAL_TRANSPORT"):
            runner.native_mode(self.store, self.work, self.identity, adopt=True)
        self.assertFalse((self.work / "native_transport_identity.json").exists())

    def test_source_identity_required_for_accepted_rejected_and_oversize_evidence(self):
        self.seal_mode()
        for changed in ({"proof_count": 1},
                {"terminal_receipt": {"raw_response_retained": True}},
                {"terminal_receipt": {"rejected_proof_summary": {"proof_sha256": "x"}}},
                {"terminal_receipt": {"diagnostic": {"raw_response_sha256": "x"}}}):
            with self.subTest(changed=changed):
                original = deepcopy(self.rows["request"])
                self.rows["request"].update(changed)
                with self.assertRaisesRegex(ValueError, "RETAINED_SOURCE_IDENTITY"):
                    runner.native_mode(self.store, self.work, self.identity)
                self.rows["request"] = original
        # A pre-fetch terminal budget decision has no source identity to retain.
        self.rows["request"].update(status="BLOCKED", terminal_receipt={"raw_response_retained": False})
        self.assertTrue(runner.native_mode(self.store, self.work, self.identity))

    def test_source_and_transport_identity_changes_rejected(self):
        self.seal_mode()
        self.rows["request"]["proof_count"] = 1
        self.seal_source()
        self.assertTrue(runner.native_mode(self.store, self.work, self.identity))
        for changed in ({"source_id": "other"}, {"endpoint_sha256": "invalid"}, {"sslmode": "require"}):
            with self.subTest(changed=changed):
                self.seal_source(**changed)
                with self.assertRaisesRegex(ValueError, "SOURCE_IDENTITY_INVALID"):
                    runner.native_mode(self.store, self.work, self.identity)
        self.seal_source()
        with self.assertRaisesRegex(ValueError, "TRANSPORT_IDENTITY_CHANGED"):
            runner.native_mode(self.store, self.work, {**self.identity, "source_id": "other"})

    @staticmethod
    def snapshot(member_name="/PG_VERSION", *, symlink=False):
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
            root = tarfile.TarInfo("/")
            root.type = tarfile.DIRTYPE
            archive.addfile(root)
            item = tarfile.TarInfo(member_name)
            if symlink:
                item.type = tarfile.SYMTYPE
                item.linkname = "../../escaped"
                archive.addfile(item)
            else:
                raw = b"17\n"
                item.size = len(raw)
                archive.addfile(item, io.BytesIO(raw))
        return buffer.getvalue()

    def archive_payloads(self, *, snapshot=None):
        return {"working_registry_snapshot.tar.gz": self.snapshot() if snapshot is None else snapshot,
            "working_registry_identity.json": runner.encoded({"original": "fixture"}),
            "native_transport_identity.json": runner.encoded(self.identity),
            "scheduler_cursor.json": runner.encoded({"next_cursor": 3}),
            "receipts/20261008T120000-" + "a" * 32 + ".json": runner.encoded({"fixture": True})}

    def make_archive(self, name, payloads, *, bad_hash=None):
        manifest = {"version": runner.VERSION, "transport": runner.MODE,
            "files": {key: {"sha256": runner.digest(raw), "bytes": len(raw)} for key, raw in payloads.items()}}
        if bad_hash:
            manifest["files"][bad_hash]["sha256"] = "0" * 64
        path = self.root / name
        with zipfile.ZipFile(path, "x", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("manifest.json", runner.encoded(manifest))
            for key, raw in payloads.items():
                archive.writestr(key, raw)
        return path

    def restore(self, archive, name="restored"):
        args = SimpleNamespace(work_dir=self.root / name, archive=archive)
        with runner.work_lock(args.work_dir):
            runner.restore_state(args)
        return args.work_dir

    def test_export_restore_roundtrip_accepts_portable_pglite_leading_root(self):
        payloads = self.archive_payloads()
        for name, raw in payloads.items():
            path = self.work / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
        args = SimpleNamespace(work_dir=self.work, archive=self.root / "state.zip")
        with runner.work_lock(self.work):
            receipt = runner.export_state(args)
        self.assertEqual(receipt["archive_sha256"], runner.digest(args.archive.read_bytes()))
        restored = self.restore(args.archive)
        for name, raw in payloads.items():
            self.assertEqual((restored / name).read_bytes(), raw)
        self.assertEqual((restored / "registry_data/PG_VERSION").read_bytes(), b"17\n")

    def test_archive_digest_or_missing_native_mode_rejected_before_extraction(self):
        payloads = self.archive_payloads()
        bad = self.make_archive("bad-digest.zip", payloads, bad_hash="working_registry_snapshot.tar.gz")
        with self.assertRaisesRegex(ValueError, "PAYLOAD_CHANGED"):
            self.restore(bad, "bad-digest")
        self.assertFalse((self.root / "bad-digest/registry_data").exists())
        del payloads["native_transport_identity.json"]
        missing = self.make_archive("missing-mode.zip", payloads)
        with self.assertRaisesRegex(ValueError, "MANIFEST_MISMATCH"):
            self.restore(missing, "missing-mode")
        self.assertFalse((self.root / "missing-mode/registry_data").exists())

    def test_archive_paths_and_inner_links_cannot_escape_restore(self):
        payloads = self.archive_payloads()
        payloads["../escaped"] = b"no"
        bad = self.make_archive("outer-path.zip", payloads)
        with self.assertRaisesRegex(ValueError, "UNEXPECTED_PATH"):
            self.restore(bad, "outer-path")
        for name, raw in (("inner-path", self.snapshot("/../escaped")),
                          ("inner-link", self.snapshot("/link", symlink=True))):
            with self.subTest(name=name):
                bad = self.make_archive(name + ".zip", self.archive_payloads(snapshot=raw))
                with self.assertRaisesRegex(ValueError, "UNSAFE_PATH"):
                    self.restore(bad, name)
        self.assertFalse((self.root / "escaped").exists())

    def test_restore_requires_new_work_directory(self):
        archive = self.make_archive("existing.zip", self.archive_payloads())
        (self.work / "keep.txt").write_text("existing")
        with runner.work_lock(self.work), self.assertRaisesRegex(ValueError, "NEW_WORK_DIRECTORY"):
            runner.restore_state(SimpleNamespace(work_dir=self.work, archive=archive))
        self.assertEqual((self.work / "keep.txt").read_text(), "existing")

    def cycle_args(self, command):
        return SimpleNamespace(**vars(self.args), command=command, checkpoint=self.root / "checkpoint",
            repo=Path(runner.__file__).parent, node="/fixture/node")

    def test_invalid_restored_cursor_rejected_before_destination_start(self):
        adapter = Mock()
        adapter.checkpoint_context.return_value = ({}, {}, {})
        with patch.object(runner, "mode_identity", return_value=self.identity):
            for cursor in (-1, 8, True, "3"):
                with self.subTest(cursor=cursor):
                    runner.atomic_json(self.work / "scheduler_cursor.json", {"next_cursor": cursor})
                    with self.assertRaisesRegex(ValueError, "INVALID_RESTORED_CURSOR"):
                        runner.run_cycle(self.cycle_args("restore"), adapter, threading.Event())
            adapter.destination.assert_not_called()

    def test_export_restore_validate_transport_before_success_receipt(self):
        adapter = Mock()
        adapter.checkpoint_context.return_value = ({}, {}, {})
        adapter.destination.side_effect = lambda *_: nullcontext(object())
        with patch.object(runner, "mode_identity", return_value=self.identity), \
                patch("research_no_horizon_acquisition_store.AcquisitionStore", return_value=self.store), \
                patch.object(runner, "native_mode", side_effect=ValueError("UNVERIFIED_MODE")) as verify:
            for command in ("export", "restore"):
                with self.subTest(command=command), self.assertRaisesRegex(ValueError, "UNVERIFIED_MODE"):
                    runner.run_cycle(self.cycle_args(command), adapter, threading.Event())
            self.assertEqual(verify.call_count, 2)
        self.assertFalse((self.work / "receipts").exists())


if __name__ == "__main__":
    unittest.main()
