"""Isolated worker lifecycle, read-only transport and idempotent persistence."""
import asyncio
import contextlib
import copy
from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import research_forward_evaluation_worker as module

BASE = datetime(2026, 10, 25, 12, 0, tzinfo=timezone.utc)


def snapshot():
    return {"study_id": module.STUDY_ID, "journal_seq": 10, "dataset": 1,
            "policy_sha256": "policy", "policy_json": {"poll_seconds": 10},
            "updated_at_utc": (BASE - timedelta(seconds=1)).isoformat(),
            "last_status": {"state": "OBSERVING", "leader": True,
                            "last_source_received_utc": (BASE - timedelta(seconds=2)).isoformat(),
                            "last_quote_received_utc": (BASE - timedelta(seconds=1)).isoformat()},
            "state_json": {"books": {"DOGE_only": {"closed": []}}}}


class FakeCore:
    EVALUATOR_VERSION = "worker-fixture-v1"

    def __init__(self, statuses=None):
        self.statuses = list(statuses or ["PASS"])
        self.calls = []

    def stable_fingerprint(self, source):
        return module._digest(source["dataset"])

    def assess(self, source, packets, **kwargs):
        self.calls.append((threading.get_ident(), copy.deepcopy(source), copy.deepcopy(packets), kwargs))
        status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        return {"status": status, "evaluator_version": self.EVALUATOR_VERSION,
                "dataset_fingerprint": self.stable_fingerprint(source),
                "decision": "DESCRIPTIVE_ONLY", "unique_economic_cases": 4}

    def safe_summary(self, result):
        return {k: result[k] for k in ("status", "decision", "unique_economic_cases")}


class FakeStoreFactory:
    def __init__(self):
        self.snapshot = snapshot()
        self.server_time = BASE
        self.packets = [{"seq": 1, "payload_sha256": "quote-digest"}]
        self.rows = {}
        self.trace = []
        self.writes = 0
        self.closed = 0
        self.leader = True
        self.fail_after_commit = False
        self.slow_seconds = 0

    def trace_call(self, name):
        self.trace.append((name, threading.get_ident()))

    def __call__(self, dsn):
        factory = self
        factory.trace_call("store_create")

        class Store:
            def acquire(self):
                factory.trace_call("acquire")
                return factory.leader

            @contextlib.contextmanager
            def source(self):
                factory.trace_call("source_read")

                class Read:
                    snapshot = copy.deepcopy(factory.snapshot)
                    server_time = factory.server_time

                    def quotes(self):
                        factory.trace_call("quote_read")
                        time.sleep(factory.slow_seconds)
                        return copy.deepcopy(factory.packets)

                try:
                    yield Read()
                finally:
                    factory.trace_call("source_close")

            def find_pass(self, version, fingerprint):
                factory.trace_call("find_pass")
                result = factory.rows.get((version, fingerprint))
                if result and result["assessment"]["status"] == "PASS":
                    return copy.deepcopy(result)
                return None

            def latest(self, *_):
                factory.trace_call("latest")
                return copy.deepcopy(next(reversed(factory.rows.values()))["assessment"]) if factory.rows else None

            def persist(self, result, metadata):
                factory.trace_call("persist")
                key = metadata["version"], metadata["dataset_sha256"]
                prior = factory.rows.get(key)
                if prior is None or prior["assessment"]["status"] != "PASS" and result["status"] == "PASS":
                    factory.rows[key] = {"assessment": copy.deepcopy(result),
                                         "completed_at_utc": BASE.isoformat(),
                                         "source_journal_seq": metadata["source_journal_seq"]}
                    factory.writes += 1
                    if factory.fail_after_commit:
                        factory.fail_after_commit = False
                        raise module.EvaluationStorageError("Acknowledgement lost")
                    return BASE.isoformat()
                return None

            def close(self):
                factory.trace_call("store_close")
                factory.closed += 1

        return Store()


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.package = Path(self.temp.name)
        self.write_baseline("policy")
        self.factory = FakeStoreFactory()
        self.core = FakeCore()
        self.worker = self.make_worker()
        self.addCleanup(self.worker._discard_owned)

    def write_baseline(self, policy):
        (self.package / "baseline.json").write_text(json.dumps({"study_id": module.STUDY_ID,
            "policy_sha256": policy, "config_sha256": "config", "study_start_utc": BASE.isoformat(),
            "runtime_commit": "old-original-runtime"}), encoding="utf8")

    def make_worker(self, core=None):
        worker = module.ForwardEvaluationWorker(package=self.package, store_factory=self.factory,
                                                core_module=core or self.core)
        worker._dsn = "never-publish-this-private-dsn"
        return worker

    def cycle(self, worker=None):
        with contextlib.redirect_stdout(io.StringIO()):
            (worker or self.worker)._cycle_owned()

    def test_pass_is_persisted_after_read_closes_and_quiet_inputs_skip(self):
        self.cycle()
        self.assertEqual(self.factory.writes, 1)
        names = [x[0] for x in self.factory.trace]
        self.assertLess(names.index("source_close"), names.index("persist"))
        self.assertEqual(self.worker.status()["state"], "AUDIT_PASSED")
        self.factory.snapshot["journal_seq"] = 200
        self.cycle()
        self.assertEqual(self.factory.writes, 1)
        self.assertEqual(len(self.core.calls), 1)
        self.assertEqual(names.count("quote_read"), 1)
        self.assertEqual(self.worker.status()["state"], "WAITING_FOR_NEW_DATA")
        self.assertEqual(self.worker.status()["source_journal_seq"], 200)
        self.assertEqual(self.worker.status()["assessment_source_journal_seq"], 10)

    def test_pending_is_not_persisted_or_cached_and_can_complete(self):
        self.core.statuses = ["PENDING_PRICE_DATA", "PASS"]
        self.cycle()
        self.assertEqual(self.factory.writes, 0)
        self.assertEqual(self.worker.status()["state"], "PENDING_PRICE_DATA")
        self.cycle()
        self.assertEqual(len(self.core.calls), 2)
        self.assertEqual(self.factory.writes, 1)
        self.assertEqual(self.worker.status()["state"], "AUDIT_PASSED")

    def test_failed_input_retries_and_only_upgrades_to_pass_once(self):
        self.core.statuses = ["FAIL", "FAIL", "PASS"]
        self.cycle(); self.cycle()
        self.assertEqual(len(self.core.calls), 2)
        self.assertEqual(self.factory.writes, 1)
        self.cycle(); self.cycle()
        self.assertEqual(len(self.core.calls), 3)
        self.assertEqual(self.factory.writes, 2)
        self.assertEqual(self.worker.status()["state"], "WAITING_FOR_NEW_DATA")

    def test_commit_acknowledgement_loss_reloads_pass_without_duplicate_write(self):
        self.factory.fail_after_commit = True
        with self.assertRaises(module.EvaluationStorageError):
            self.cycle()
        self.assertIsNone(self.worker._store)
        self.cycle()
        self.assertEqual(self.factory.writes, 1)
        self.assertEqual(len(self.core.calls), 1)
        self.assertEqual(self.worker.status()["state"], "WAITING_FOR_NEW_DATA")

    def test_cached_pass_never_masks_stale_source_receipts(self):
        self.cycle()
        self.factory.snapshot["last_status"]["last_source_received_utc"] = (BASE - timedelta(seconds=90)).isoformat()
        self.cycle()
        self.assertEqual(self.worker.status()["state"], "SOURCE_STALE")
        self.assertEqual(self.factory.writes, 1)
        self.assertEqual(len(self.core.calls), 1)

    def test_source_not_observing_or_missing_receipts_does_not_assess(self):
        self.factory.snapshot["last_status"]["state"] = "RETRYING"
        self.cycle()
        self.assertEqual(self.worker.status()["state"], "SOURCE_NOT_OBSERVING")
        self.factory.snapshot["last_status"]["state"] = "OBSERVING"
        del self.factory.snapshot["last_status"]["last_quote_received_utc"]
        self.cycle()
        self.assertEqual(self.worker.status()["last_error_type"], "SourceReceiptClockMissing")
        self.assertEqual(len(self.core.calls), 0)

    def test_changed_baseline_cannot_reuse_previously_cached_pass(self):
        self.cycle()
        first = self.worker.status()["dataset_sha256"]
        self.worker._discard_owned()
        self.write_baseline("different-frozen-policy")
        self.cycle()
        self.assertNotEqual(first, self.worker.status()["dataset_sha256"])
        self.assertEqual(len(self.core.calls), 2)

    def test_changed_core_source_is_part_of_the_persistent_key(self):
        source = self.package / "fixture_core.py"
        source.write_text("version_one = True\n", encoding="utf8")
        self.core.__file__ = str(source)
        self.cycle()
        first = self.worker.status()["dataset_sha256"]
        self.worker._discard_owned()
        source.write_text("version_two = True\n", encoding="utf8")
        self.cycle()
        self.assertNotEqual(first, self.worker.status()["dataset_sha256"])
        self.assertEqual(self.factory.writes, 2)

    def test_standby_does_not_read_or_write_source(self):
        self.factory.leader = False
        self.cycle()
        self.assertEqual(self.worker.status()["state"], "STANDBY")
        self.assertNotIn("source_read", [v[0] for v in self.factory.trace])
        self.assertEqual(self.factory.writes, 0)

    def test_evidence_limit_is_explicit_and_never_persists_partial_assessment(self):
        self.factory.packets = None
        original = self.core.assess
        def limit(*args, **kwargs):
            raise module.EvidenceLimitExceeded("limit")
        self.core.assess = limit
        self.cycle()
        self.assertEqual(self.worker.status()["state"], "EVIDENCE_LIMIT")
        self.assertEqual(self.factory.writes, 0)
        self.core.assess = original

    def test_actual_core_contract_assesses_and_quietly_reuses_verified_closed_case(self):
        import research_forward_evaluation as real_core
        from research_forward_evaluation_selftest import empty, add_case, journals
        source, baseline = empty()
        _, packets = add_case(source)
        self.factory.snapshot = source
        self.factory.server_time = module._time(source["updated_at_utc"]) + timedelta(seconds=1)
        source["last_status"].update(state="OBSERVING", leader=True,
            last_source_received_utc=source["updated_at_utc"], last_quote_received_utc=source["updated_at_utc"])
        self.factory.packets = journals(packets)
        (self.package / "baseline.json").write_text(json.dumps(baseline), encoding="utf8")
        actual = self.make_worker(real_core)
        self.addCleanup(actual._discard_owned)
        self.cycle(actual)
        self.assertEqual(actual.status()["state"], "AUDIT_PASSED")
        self.assertFalse(actual.status()["assessment"]["strategy_approved"])
        self.assertEqual(actual.status()["assessment"]["counts"]["unique_closed_cases"], 1)
        self.cycle(actual)
        self.assertEqual(actual.status()["state"], "WAITING_FOR_NEW_DATA")
        self.assertEqual(self.factory.writes, 1)


class FakeCursor:
    def __init__(self, connection, name=None):
        self.connection = connection
        self.name = name
        self.rows = []
        self.rowcount = -1

    def __enter__(self): return self
    def __exit__(self, *_): self.close()
    def close(self): pass
    def execute(self, sql, params=()):
        conn = self.connection
        conn.statements.append((sql, params, self.name))
        upper = " ".join(sql.split()).upper()
        self.rows = []
        if "PG_TRY_ADVISORY_LOCK" in upper:
            self.rows = [(conn.acquired, 7)]
        elif upper == "SELECT PG_BACKEND_PID()":
            self.rows = [(7,)]
        elif "ROW_TO_JSON(S)" in upper:
            self.rows = [(copy.deepcopy(conn.snapshot), BASE)]
        elif "WITH WINDOWS" in upper:
            self.rows = [(copy.deepcopy(p),) for p in conn.quotes]
        elif upper.startswith("SELECT ASSESSMENT_JSON,COMPLETED"):
            self.rows = list(conn.cached_rows)
        elif upper.startswith("SELECT ASSESSMENT_JSON,ASSESSMENT_SHA256"):
            self.rows = list(conn.previous_rows)
        elif upper.startswith("INSERT INTO RESEARCH_FORWARD_PAPER_EVALUATIONS"):
            if conn.fail_write:
                raise RuntimeError("driver included a private connection string")
            self.rows = [(BASE,)]

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def fetchmany(self, count):
        rows, self.rows = self.rows[:count], self.rows[count:]
        return rows


class FakeConnection:
    def __init__(self, acquired=True):
        self.acquired = acquired
        self.snapshot = snapshot()
        self.quotes = []
        self.cached_rows = []
        self.previous_rows = []
        self.fail_write = False
        self.autocommit = None
        self.closed = False
        self.rolled_back = False
        self.statements = []

    def cursor(self, name=None): return FakeCursor(self, name)
    def rollback(self): self.rolled_back = True
    def close(self): self.closed = True


class StoreTests(unittest.TestCase):
    def test_default_driver_bounds_and_timezone(self):
        connect = Mock(return_value=object())
        with patch.dict("sys.modules", {"psycopg": SimpleNamespace(connect=connect)}):
            module._connect("private-connection-data")
        options = connect.call_args.kwargs
        self.assertEqual(options["connect_timeout"], 5)
        self.assertEqual(options["tcp_user_timeout"], 10000)
        self.assertIn("statement_timeout=5000", options["options"])
        self.assertIn("lock_timeout=2000", options["options"])
        self.assertIn("timezone=UTC", options["options"])

    def test_competing_writer_never_creates_tables(self):
        conn = FakeConnection(acquired=False)
        store = module.EvaluationStore("private", connect=lambda _: conn)
        self.assertFalse(store.acquire())
        self.assertTrue(conn.closed)
        self.assertFalse(any("CREATE" in sql for sql, _, _ in conn.statements))

    def test_source_snapshot_and_quotes_are_repeatable_read_only_bounded_and_closed(self):
        conn = FakeConnection()
        conn.snapshot["state_json"]["books"]["DOGE_only"]["closed"] = [{
            "selected_at_utc": BASE.isoformat(), "exit_received_utc": (BASE + timedelta(minutes=1)).isoformat()}]
        conn.quotes = [{"seq": 8, "payload_json": {"quotes": []}, "payload_sha256": "fixture"}]
        store = module.EvaluationStore("private", connect=lambda _: conn)
        with store.source() as source:
            data = source.quotes()
        sql = [v[0] for v in conn.statements]
        self.assertEqual(sql[0], "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        self.assertIn("SET LOCAL TIME ZONE 'UTC'", sql)
        quote_sql, params, name = next(v for v in conn.statements if "WITH windows" in v[0])
        self.assertIn("j.seq<=%s", quote_sql)
        self.assertEqual(params[-3:], [module.STUDY_ID, 10, module.MAX_QUOTE_PACKETS + 1])
        self.assertTrue(name.startswith("forward_evaluation_"))
        self.assertEqual(len(data), 1)
        self.assertTrue(conn.rolled_back and conn.closed)
        self.assertFalse(any(v.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for v in sql))

    def test_evidence_cap_raises_instead_of_returning_partial_quotes(self):
        conn = FakeConnection()
        conn.snapshot["state_json"]["books"]["DOGE_only"]["closed"] = [{
            "selected_at_utc": BASE.isoformat(), "exit_received_utc": (BASE + timedelta(minutes=1)).isoformat()}]
        conn.quotes = [{"seq": i} for i in range(3)]
        store = module.EvaluationStore("private", connect=lambda _: conn, max_packets=2)
        with self.assertRaises(module.EvidenceLimitExceeded):
            with store.source() as source:
                source.quotes()
        self.assertTrue(conn.rolled_back and conn.closed)

    def test_cached_json_integrity_checked_before_pass_is_returned(self):
        conn = FakeConnection()
        store = module.EvaluationStore("private", connect=lambda _: conn)
        self.assertTrue(store.acquire())
        result = {"status": "PASS", "dataset_fingerprint": "fingerprint", "evaluator_version": "v1"}
        conn.cached_rows = [(result, BASE, 10, module._digest(result))]
        self.assertEqual(store.find_pass("v1", "dataset")["assessment"], result)
        conn.cached_rows = [(result, BASE, 10, "wrong-digest")]
        with self.assertRaises(module.EvaluationStorageError):
            store.find_pass("v1", "dataset")
        conn.previous_rows = [(result, "wrong-digest")]
        with self.assertRaises(module.EvaluationStorageError):
            store.latest("v1", "source-code")
        store.close()

    def test_persistence_sql_only_upgrades_failed_rows_and_sanitizes_errors(self):
        conn = FakeConnection()
        store = module.EvaluationStore("private", connect=lambda _: conn)
        store.acquire()
        result = {"status": "PASS", "dataset_fingerprint": "fingerprint", "evaluator_version": "v1"}
        metadata = {"version": "v1", "dataset_sha256": "dataset", "source_fingerprint": "fingerprint",
                    "code_sha256": "code", "baseline_sha256": "baseline", "source_journal_seq": 10,
                    "policy_sha256": "policy", "evidence_sha256": "evidence", "runtime_commit": "a" * 40}
        self.assertEqual(store.persist(result, metadata), BASE.isoformat())
        sql, params, _ = next(v for v in conn.statements if v[0].startswith("INSERT INTO"))
        self.assertIn("research_forward_paper_evaluations.audit_status<>'PASS'", sql)
        self.assertIn("EXCLUDED.audit_status='PASS'", sql)
        self.assertEqual(len(params), 13)
        with self.assertRaises(ValueError):
            store.persist({**result, "status": "PENDING_PRICE_DATA"}, metadata)
        conn.fail_write = True
        with self.assertRaises(module.EvaluationStorageError) as error:
            store.persist(result, metadata)
        self.assertNotIn("private", str(error.exception))
        self.assertTrue(conn.closed)


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_does_not_allocate_resources(self):
        worker = module.ForwardEvaluationWorker()
        self.assertFalse(await worker.start(enabled=False, database_url="private"))
        self.assertIsNone(worker._executor)

    async def test_stop_in_initial_delay_does_not_open_database(self):
        factory = FakeStoreFactory()
        worker = module.ForwardEvaluationWorker(store_factory=factory, first_delay=60)
        self.assertTrue(await worker.start(enabled=True, database_url="private"))
        await worker.stop()
        self.assertEqual(factory.trace, [])

    async def test_owned_thread_leaves_loop_responsive_and_closes_on_that_thread(self):
        factory = FakeStoreFactory(); factory.slow_seconds = .1
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)
            (path / "baseline.json").write_text("{}", encoding="utf8")
            core = FakeCore()
            worker = module.ForwardEvaluationWorker(package=path, store_factory=factory, core_module=core,
                                                    first_delay=.001, poll_seconds=60)
            ticks = 0
            async def ticker():
                nonlocal ticks
                for _ in range(8):
                    await asyncio.sleep(.01)
                    ticks += 1
            with contextlib.redirect_stdout(io.StringIO()):
                await worker.start(enabled=True, database_url="secret-private", runtime_commit="b" * 40)
                await ticker()
                await worker.stop()
            self.assertEqual(ticks, 8)
            threads = {item[1] for item in factory.trace}
            self.assertEqual(len(threads), 1)
            self.assertNotIn(threading.get_ident(), threads)
            self.assertEqual(core.calls[0][0], next(iter(threads)))
            self.assertEqual(worker.status()["state"], "STOPPED")
            self.assertNotIn("secret-private", json.dumps(worker.status()))
            self.assertFalse(worker.status()["trade_execution"])
            self.assertFalse(worker.status()["strategy_approved"])


if __name__ == "__main__":
    unittest.main()
