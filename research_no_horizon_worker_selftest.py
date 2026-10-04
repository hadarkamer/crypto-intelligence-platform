"""No-network opt-in, failure isolation and graceful shutdown regressions."""
import asyncio
import os
import threading
import unittest
from unittest.mock import Mock, patch

import research_no_horizon_worker as runtime


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)
        self.worker = runtime.ResearchNoHorizonWorker()

    async def asyncTearDown(self):
        await self.worker.stop()

    def configure(self):
        os.environ.update(RESEARCH_NO_HORIZON_ENABLED="1",
            RESEARCH_NO_HORIZON_DATABASE_URL="postgresql://explicit-test-only")

    async def test_disabled_does_not_inherit_legacy_flags_or_connect(self):
        os.environ.update(RESEARCH_OUTCOME_ENRICHMENT_ENABLED="1",
            RESEARCH_USE_PRIMARY_DATABASE="1", DATABASE_URL="postgresql://unused",
            RESEARCH_DATABASE_URL="postgresql://also-unused")
        with patch.object(runtime, "_connect", side_effect=AssertionError("connected")):
            self.assertFalse(await self.worker.start())
            self.assertEqual(self.worker.run_once()["reason"], "DISABLED")
        self.assertFalse(self.worker.status()["configured"])
        self.assertIsNone(self.worker._task)

    async def test_enabled_requires_explicit_database_before_connection(self):
        os.environ["RESEARCH_NO_HORIZON_ENABLED"] = "1"
        os.environ["DATABASE_URL"] = "postgresql://not-a-fallback"
        with patch.object(runtime, "_connect", side_effect=AssertionError("connected")):
            with self.assertRaisesRegex(RuntimeError, "Explicit"):
                await self.worker.start()
        self.assertIsNone(self.worker._task)

    async def test_invalid_budget_fails_before_connection(self):
        self.configure()
        os.environ["RESEARCH_NO_HORIZON_CANDLE_BUDGET"] = "0"
        with patch.object(runtime, "_connect", side_effect=AssertionError("connected")):
            with self.assertRaisesRegex(ValueError, "Out-of-range"):
                await self.worker.start()
        self.assertIsNone(self.worker._task)

    async def test_missing_schema_creates_no_runtime_task(self):
        self.configure()
        with patch.object(self.worker, "_check_schema", side_effect=RuntimeError("missing")):
            with self.assertRaisesRegex(RuntimeError, "missing"):
                await self.worker.start()
        self.assertFalse(self.worker.status()["running"])

    async def test_stop_waits_for_inflight_pass_and_start_is_idempotent(self):
        self.configure()
        entered, release = threading.Event(), threading.Event()
        calls = []
        def work():
            calls.append("start")
            entered.set()
            if not release.wait(5):
                raise AssertionError("test did not release pass")
            calls.append("finish")
        with patch.object(self.worker, "_check_schema", return_value=True), \
             patch.object(self.worker, "run_once", side_effect=work):
            self.assertTrue(await self.worker.start())
            original = self.worker._task
            self.assertTrue(await asyncio.to_thread(entered.wait, 3))
            self.assertTrue(await self.worker.start())
            self.assertIs(self.worker._task, original)
            stopping = asyncio.create_task(self.worker.stop())
            await asyncio.sleep(0)
            self.assertFalse(stopping.done())
            release.set()
            await asyncio.wait_for(stopping, timeout=3)
        self.assertEqual(calls, ["start", "finish"])
        self.assertFalse(self.worker.status()["running"])

    async def test_bounded_call_and_health_do_not_publish_receipts_or_secrets(self):
        self.configure()
        connection = Mock()
        connector = Mock()
        connector.__enter__ = Mock(return_value=connection)
        connector.__exit__ = Mock(return_value=False)
        backend = Mock()
        backend.run_once.return_value = {"plan_id": "plan", "scope_ordinal": 2,
            "candle_evaluations_this_run": 9, "private_payload": "not-public"}
        with patch.object(runtime, "_connect", return_value=connector), \
             patch.object(runtime.store, "PostgresCohortStore", return_value=backend):
            result = self.worker.run_once()
        self.assertTrue(result["claimed"])
        self.assertEqual(backend.run_once.call_args.kwargs,
            dict(lease_seconds=120, candle_budget=4096, entry_budget=64, batch_size=128))
        status = self.worker.status()
        self.assertEqual(status["metrics"]["claimed_passes"], 1)
        self.assertNotIn("private_payload", repr(status))
        self.assertNotIn("postgresql://", repr(status))
        self.assertFalse(status["automatic_source_admission"])
        for name in ("runtime_authorized", "telegram_authorized", "trading_authorized"):
            self.assertIs(status[name], False)

    async def test_transient_failure_is_sanitized_and_next_pass_can_recover(self):
        self.configure()
        connector = Mock()
        connector.__enter__ = Mock(return_value=Mock())
        connector.__exit__ = Mock(return_value=False)
        backend = Mock()
        backend.run_once.return_value = None
        with patch.object(runtime, "_connect", side_effect=[
                RuntimeError("secret DSN or query text"), connector]), \
             patch.object(runtime.store, "PostgresCohortStore", return_value=backend):
            with self.assertRaises(RuntimeError):
                self.worker.run_once()
            self.assertEqual(self.worker.metrics["last_error_type"], "RuntimeError")
            self.assertNotIn("secret", repr(self.worker.status()))
            self.assertFalse(self.worker.run_once()["claimed"])
        self.assertEqual(self.worker.metrics["failures"], 1)
        self.assertEqual(self.worker.metrics["runs"], 1)
        self.assertIsNone(self.worker.metrics["last_error_type"])

    def acquisition_backend(self, *, result=None):
        self.configure()
        os.environ["RESEARCH_NO_HORIZON_ACQUISITION_ENABLED"] = "true"
        os.environ["RESEARCH_NO_HORIZON_READ_DATABASE_URL"] = "postgresql://read-only-test-source"
        backend = Mock()
        backend.run_once.return_value = result
        return backend

    async def test_acquisition_flag_alone_does_not_enable_worker(self):
        os.environ["RESEARCH_NO_HORIZON_ACQUISITION_ENABLED"] = "1"
        with patch.object(runtime, "_connect", side_effect=AssertionError("connected")), \
             patch.object(runtime, "_connect_source", side_effect=AssertionError("source connected")):
            self.assertFalse(await self.worker.start())
            self.assertEqual(self.worker.run_once()["reason"], "DISABLED")
        self.assertFalse(self.worker.status()["automatic_source_admission"])

    async def test_disabled_acquisition_never_opens_source(self):
        self.configure()
        with patch.object(runtime, "_connect_source", side_effect=AssertionError("source connected")), \
             patch.object(runtime.acquisition, "schema_status", side_effect=AssertionError("intake inspected")):
            result = self.worker._acquire_once(Mock(), runtime._settings())
        self.assertEqual(result["reason"], "DISABLED")

    async def test_no_due_request_does_not_open_source_connection(self):
        backend = self.acquisition_backend()
        with patch.object(runtime.acquisition, "schema_status", return_value={"schema_present": True}), \
             patch.object(runtime.acquisition, "AcquisitionStore", return_value=backend), \
             patch.object(runtime, "_connect_source", side_effect=AssertionError("source connected")):
            result = self.worker._acquire_once(Mock(), runtime._settings())
        self.assertFalse(result["claimed"])
        self.assertEqual(backend.run_once.call_args.kwargs["leaf_budget"], 4)
        self.assertEqual(self.worker.acquisition_metrics["runs"], 1)

    async def test_source_factory_uses_explicit_read_database_and_compact_health(self):
        backend = self.acquisition_backend()
        def run(worker_id, **kwargs):
            kwargs["source_connection_factory"]()
            return {"request_id": "request", "status": "ADMITTED", "plan_id": "plan",
                    "proof_count": 3, "raw_response_text": "private payload"}
        backend.run_once.side_effect = run
        with patch.object(runtime.acquisition, "schema_status", return_value={"schema_present": True}), \
             patch.object(runtime.acquisition, "AcquisitionStore", return_value=backend), \
             patch.object(runtime, "_connect_source", return_value=Mock()) as connect_source:
            result = self.worker._acquire_once(Mock(), runtime._settings())
        connect_source.assert_called_once_with("postgresql://read-only-test-source")
        self.assertTrue(result["claimed"])
        status = self.worker.status()
        self.assertTrue(status["automatic_source_admission"])
        self.assertNotIn("private payload", repr(status))
        self.assertNotIn("postgresql://", repr(status))
        self.assertFalse(status["provider_requests"])

    async def test_invalid_acquisition_budget_or_missing_source_is_isolated(self):
        self.acquisition_backend()
        for budget, read_url in (("0", "configured"), ("4", "")):
            os.environ["RESEARCH_NO_HORIZON_ACQUISITION_LEAF_BUDGET"] = budget
            os.environ["RESEARCH_NO_HORIZON_READ_DATABASE_URL"] = read_url
            with patch.object(runtime.acquisition, "schema_status", side_effect=AssertionError("DB touched")), \
                 patch.object(runtime, "_connect_source", side_effect=AssertionError("source connected")):
                result = self.worker._acquire_once(Mock(), runtime._settings())
            self.assertIn(result["error_type"], ("ValueError", "RuntimeError"))
        self.assertEqual(self.worker.acquisition_metrics["failures"], 2)

    async def test_acquisition_failure_preserves_successful_execution_pass(self):
        intake = self.acquisition_backend()
        intake.run_once.side_effect = RuntimeError("secret DSN and private query payload")
        connector = Mock()
        connector.__enter__ = Mock(return_value=Mock())
        connector.__exit__ = Mock(return_value=False)
        executor = Mock()
        executor.run_once.return_value = {"plan_id": "existing", "scope_ordinal": 0,
                                          "candle_evaluations_this_run": 9}
        with patch.object(runtime, "_connect", return_value=connector), \
             patch.object(runtime.store, "PostgresCohortStore", return_value=executor), \
             patch.object(runtime.acquisition, "schema_status", return_value={"schema_present": True}), \
             patch.object(runtime.acquisition, "AcquisitionStore", return_value=intake):
            result = self.worker.run_once()
        self.assertTrue(result["claimed"])
        self.assertEqual(result["work"]["plan_id"], "existing")
        self.assertEqual(self.worker.metrics["failures"], 0)
        self.assertEqual(self.worker.acquisition_metrics["failures"], 1)
        self.assertNotIn("secret", repr(self.worker.status()))


if __name__ == "__main__":
    unittest.main()
