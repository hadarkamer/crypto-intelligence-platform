"""Isolated worker lifecycle, checkpoint ordering and read-only transport tests."""
import asyncio
import copy
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import research_forward_paper_worker as worker_module


class FakeEngine:
    def __init__(self, config, trace):
        self.policy = {**config, "study_start_utc": "2026-10-21T00:00:00+00:00"}
        self.trace = trace
        self.bootstrap = False
        self.keys = []
        self.error = None
        self.observations = 0
        self.closed = False

    def summary(self):
        return {"bootstrap_complete": self.bootstrap, "study_start_utc": self.policy["study_start_utc"],
                "policy_sha256": "policy", "raw_first_seen_count": self.observations,
                "first_eligible_seen_count": len(self.keys), "journal_events": self.observations,
                "last_observation_rejected": self.error, "books": {}}

    def observe_candidates(self, packet):
        self.trace.append(("candidates_apply", threading.get_ident()))
        self.observations += 1
        self.error = "invalid" if packet.get("reject") else None
        if not self.error:
            self.bootstrap = True
            self.keys.extend(r["key"] for r in packet["rows"] if r.get("eligible"))
        return self.summary()

    def observe_quotes(self, packet):
        self.trace.append(("quotes_apply", threading.get_ident()))
        self.observations += 1
        self.error = None
        return self.summary()

    def eligible_event_keys(self):
        return list(self.keys)

    def close(self):
        self.closed = True
        self.trace.append(("engine_close", threading.get_ident()))


class StoreFactory:
    def __init__(self, trace, *, leader=True, fail_checkpoint=None):
        self.trace = trace
        self.leader = leader
        self.fail_checkpoint = fail_checkpoint
        self.stores = []
        self.checkpoints = []
        self.health = []

    def __call__(self, dsn, study_id, config):
        factory = self
        class Store:
            def __init__(self):
                self.closed = False
                self.calls = 0
                self._seq = 100
                self.config = config
            @property
            def journal_sequence(self):
                return self._seq
            def acquire(self):
                factory.trace.append(("acquire", threading.get_ident()))
                return factory.leader
            def restore_engine(self, engine_class):
                factory.trace.append(("restore", threading.get_ident()))
                return engine_class(config, factory.trace)
            def checkpoint(self, engine, status):
                self.calls += 1
                factory.trace.append(("checkpoint", threading.get_ident()))
                if factory.fail_checkpoint == self.calls:
                    raise RuntimeError("deliberately-private-dsn")
                self._seq += 1
                factory.checkpoints.append(copy.deepcopy(engine.summary()))
                factory.health.append(copy.deepcopy(status))
            def update_health(self, status):
                factory.trace.append(("health", threading.get_ident()))
                factory.health.append(copy.deepcopy(status))
            def close(self):
                self.closed = True
                factory.trace.append(("store_close", threading.get_ident()))
        value = Store()
        self.stores.append(value)
        return value


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.package = Path(self.temp.name)
        self.config = {"version": "test-render-v1", "poll_seconds": .05,
                       "scopes": [{"scope_key": "s", "candidate_key": "c", "symbol": "DOGE", "direction": "SHORT"}],
                       "quote_symbols": {"DOGE": "DOGEUSDT", "ETH": "ETHUSDT"},
                       "quote_max_elapsed_seconds": 5, "quote_source": "BINANCE_SPOT_BOOK_TICKER"}
        (self.package / "policy_template.json").write_text(json.dumps(self.config))
        self.template = ("WITH requested(scope_key,candidate_key,symbol,direction,period_start_utc,source_cutoff_utc) "
                         "AS (VALUES ('old','old','old','old','2026-10-17','2026-10-17')), "
                         "matches AS MATERIALIZED (SELECT 1) SELECT * FROM matches;")
        (self.package / "source_query_template.sql").write_text(self.template)
        self.trace = []
        self.factory = StoreFactory(self.trace)
        self.fetch_arguments = []

    def candidates(self, dsn, policy, template, *, bootstrap, eligible_keys):
        self.trace.append(("candidates_fetch", threading.get_ident()))
        self.fetch_arguments.append((bootstrap, list(eligible_keys)))
        return {"poll_id": str(len(self.fetch_arguments)), "response_received_utc": "2026-10-21T00:01:00Z",
                "rows": [{"key": "s:1", "eligible": True}]}

    def quotes(self, policy):
        self.trace.append(("quotes_fetch", threading.get_ident()))
        return {"packet_id": "quote", "response_received_utc": "2026-10-21T00:01:01Z", "quotes": []}

    def make_worker(self, **extra):
        worker = worker_module.ForwardPaperWorker(package=self.package, store_factory=self.factory,
            engine_class=FakeEngine, candidate_fetch=self.candidates, quote_fetch=self.quotes, **extra)
        worker._dsn = "secret-dsn"
        worker._log = lambda **kwargs: None
        return worker

    def test_every_candidate_is_durable_before_quote_and_every_quote_before_heartbeat(self):
        worker = self.make_worker()
        self.addCleanup(worker._discard_owned)
        worker._cycle_owned()
        names = [x[0] for x in self.trace]
        self.assertLess(names.index("candidates_apply"), names.index("checkpoint"))
        self.assertLess(names.index("checkpoint"), names.index("quotes_fetch"))
        second_checkpoint = names.index("checkpoint", names.index("checkpoint") + 1)
        self.assertLess(names.index("quotes_apply"), second_checkpoint)
        self.assertLess(second_checkpoint, names.index("health"))
        self.assertEqual(worker.status()["cycles"], 1)
        self.assertEqual(worker.status()["journal_events"], 102)
        self.assertEqual(worker.status()["durable_journal_seq"], 102)

    def test_failed_candidate_checkpoint_never_requests_quote_and_discards_unpersisted_engine(self):
        self.factory.fail_checkpoint = 1
        worker = self.make_worker()
        with self.assertRaises(RuntimeError):
            worker._cycle_owned()
        self.assertNotIn("quotes_fetch", [x[0] for x in self.trace])
        self.assertIsNone(worker._engine)
        self.assertIsNone(worker._store)
        self.assertTrue(self.factory.stores[0].closed)
        self.assertEqual(self.factory.checkpoints, [])
        self.factory.fail_checkpoint = None
        worker._cycle_owned()
        self.addCleanup(worker._discard_owned)
        self.assertEqual(len(self.factory.stores), 2)
        self.assertTrue(self.fetch_arguments[-1][0], "Restore must not inherit failed in-memory bootstrap")

    def test_failed_quote_checkpoint_discards_engine_after_candidate_was_durable(self):
        self.factory.fail_checkpoint = 2
        worker = self.make_worker()
        with self.assertRaises(RuntimeError):
            worker._cycle_owned()
        self.assertEqual(len(self.factory.checkpoints), 1)
        self.assertEqual(self.factory.checkpoints[0]["journal_events"], 1)
        self.assertIsNone(worker._engine)
        self.assertTrue(self.factory.stores[0].closed)

    def test_source_rejection_is_checkpointed_and_stops_before_quote(self):
        worker = self.make_worker()
        def reject(*args, **kwargs):
            packet = self.candidates(*args, **kwargs)
            packet["reject"] = True
            return packet
        worker._candidate_fetch = reject
        with self.assertRaises(worker_module.ObservationRejected):
            worker._cycle_owned()
        self.assertEqual(len(self.factory.checkpoints), 1)
        self.assertEqual(self.factory.checkpoints[0]["last_observation_rejected"], "invalid")
        self.assertNotIn("quotes_fetch", [x[0] for x in self.trace])

    def test_standby_does_not_observe_or_overwrite_leader_health(self):
        self.factory.leader = False
        worker = self.make_worker()
        worker._cycle_owned()
        self.assertEqual(worker.status()["state"], "STANDBY")
        self.assertNotIn("candidates_fetch", [x[0] for x in self.trace])
        self.assertEqual(self.factory.health, [])
        self.assertTrue(self.factory.stores[0].closed)

    def test_only_checkpointed_eligible_keys_are_excluded_and_bootstrap_not_repeated(self):
        worker = self.make_worker()
        self.addCleanup(worker._discard_owned)
        worker._cycle_owned()
        worker._cycle_owned()
        self.assertEqual(self.fetch_arguments[0], (True, []))
        self.assertEqual(self.fetch_arguments[1], (False, ["s:1"]))
        sql = worker_module.source_sql(worker._engine.policy, self.template, eligible_keys=["s:1"])
        self.assertIn("NOT IN ('s:1')", sql)
        self.assertNotIn("event_id >", sql)
        self.assertIn("statement_timestamp()", sql)
        self.assertNotIn("2026-10-17", sql)

    def test_source_query_and_transport_are_pinned_and_health_has_no_private_payload(self):
        worker = self.make_worker()
        self.addCleanup(worker._discard_owned)
        worker._cycle_owned()
        config = self.factory.stores[0].config
        self.assertEqual(config["transport_version"], worker_module.TRANSPORT_VERSION)
        self.assertEqual(len(config["source_query_sha256"]), 64)
        self.assertEqual(len(config["engine_source_sha256"]), 64)
        self.assertEqual(config["quote_endpoint"], worker_module.QUOTE_URL)
        encoded = json.dumps(worker.status())
        self.assertNotIn("secret-dsn", encoded)
        self.assertNotIn("rows", encoded)
        self.assertFalse(worker.status()["trade_execution"])
        self.assertFalse(worker.status()["notifications"])


class AsyncLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_and_unconfigured_start_do_not_allocate_executor(self):
        worker = worker_module.ForwardPaperWorker()
        self.assertFalse(await worker.start(database_url="dsn", enabled=False))
        self.assertIsNone(worker._executor)
        self.assertFalse(await worker.start(database_url=None, enabled=True))
        self.assertIsNone(worker._executor)

    async def test_one_thread_owns_store_engine_transport_and_shutdown_without_blocking_loop(self):
        with tempfile.TemporaryDirectory() as td:
            package = Path(td)
            (package / "policy_template.json").write_text(json.dumps({"version": "test", "poll_seconds": .05}))
            (package / "source_query_template.sql").write_text("WITH x AS(SELECT 1), matches AS MATERIALIZED(SELECT 1) SELECT * FROM matches")
            trace = []
            factory = StoreFactory(trace)
            def candidates(*args, **kwargs):
                trace.append(("candidates_fetch", threading.get_ident()))
                return {"response_received_utc": "2026-10-21T00:01:00Z", "rows": []}
            def quotes(*args, **kwargs):
                trace.append(("quotes_fetch", threading.get_ident()))
                time.sleep(.03)
                return {"response_received_utc": "2026-10-21T00:01:01Z"}
            worker = worker_module.ForwardPaperWorker(package=package, store_factory=factory,
                engine_class=FakeEngine, candidate_fetch=candidates, quote_fetch=quotes, first_delay=.001)
            worker._log = lambda **kwargs: None
            main_thread = threading.get_ident()
            self.assertTrue(await worker.start(database_url="private-dsn", enabled=True,
                                               runtime_commit="a" * 40))
            ticks = 0
            for _ in range(100):
                if worker.status()["cycles"] >= 2:
                    break
                await asyncio.sleep(.005)
                ticks += 1
            await worker.stop()
            ids = {item[1] for item in trace}
            self.assertEqual(len(ids), 1)
            self.assertNotIn(main_thread, ids)
            self.assertGreater(ticks, 2)
            self.assertEqual(worker.status()["state"], "STOPPED")
            self.assertIsNone(worker._executor)
            self.assertTrue(factory.stores[0].closed)

    async def test_stop_during_initial_delay_never_opens_database(self):
        worker = worker_module.ForwardPaperWorker(first_delay=10)
        self.assertTrue(await worker.start(database_url="private", enabled=True))
        await worker.stop()
        self.assertEqual(worker.status()["state"], "STOPPED")


class TransportTests(unittest.TestCase):
    def test_source_connection_has_bounded_network_failure_detection(self):
        captured = {}
        class Driver:
            @staticmethod
            def connect(dsn, **kwargs):
                captured.update(kwargs)
                return object()
        with patch.dict("sys.modules", {"psycopg2": Driver}):
            worker_module._connect("private")
        self.assertEqual(captured["connect_timeout"], 5)
        self.assertEqual(captured["tcp_user_timeout"], 10000)
        self.assertEqual(captured["keepalives_count"], 2)

    def test_source_connection_is_read_only_and_closed(self):
        queries = []
        class Cursor:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def execute(self, sql): queries.append(sql)
            def fetchone(self): return ({"server_statement_utc": "2026-10-21T00:00:00Z", "rows": []},)
        class Connection:
            closed = False
            rolled_back = False
            def cursor(self): return Cursor()
            def rollback(self): self.rolled_back = True
            def close(self): self.closed = True
        conn = Connection()
        policy = {"study_start_utc": "2026-10-21T00:00:00Z", "scopes": []}
        template = "WITH x AS(SELECT 1), matches AS MATERIALIZED(SELECT 1) SELECT * FROM matches"
        packet = worker_module.fetch_candidates("private", policy, template, connect=lambda _: conn)
        self.assertEqual(queries[0], "SET TRANSACTION READ ONLY")
        self.assertIn("statement_timeout", queries[1])
        self.assertEqual(queries[2], "SET LOCAL TIME ZONE 'UTC'")
        self.assertTrue(conn.closed and conn.rolled_back)
        self.assertEqual(packet["rows"], [])
        self.assertGreaterEqual(packet["monotonic_elapsed_seconds"], 0)

    def test_quotes_use_public_get_only_and_preserve_bid_ask_without_exchange_clock(self):
        policy = {"quote_symbols": {"DOGE": "DOGEUSDT", "ETH": "ETHUSDT"},
                  "quote_max_elapsed_seconds": 5, "quote_source": "BINANCE_SPOT_BOOK_TICKER"}
        seen = {}
        class Response:
            headers = {"Date": "Wed, 21 Oct 2026 10:00:00 GMT"}
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read(self, limit):
                return json.dumps([{"symbol": "DOGEUSDT", "bidPrice": "0.1", "askPrice": "0.101"},
                                   {"symbol": "ETHUSDT", "bidPrice": "2000", "askPrice": "2001"}]).encode()
        def open_url(req, timeout):
            seen.update(method=req.get_method(), url=req.full_url, headers=dict(req.header_items()), timeout=timeout)
            return Response()
        packet = worker_module.fetch_quotes(policy, open_url=open_url)
        self.assertEqual(seen["method"], "GET")
        self.assertTrue(seen["url"].startswith(worker_module.QUOTE_URL + "?"))
        self.assertTrue(seen["url"].startswith("https://data-api.binance.vision/"))
        self.assertEqual(packet["endpoint"], worker_module.QUOTE_URL)
        self.assertNotIn("Authorization", seen["headers"])
        self.assertEqual(seen["timeout"], 5)
        self.assertIsNone(packet["exchange_quote_timestamp"])
        self.assertEqual(packet["quotes"][0]["bid"], "0.1")

    def test_malformed_or_duplicate_quotes_fail_before_engine(self):
        policy = {"quote_symbols": {"DOGE": "DOGEUSDT", "ETH": "ETHUSDT"}, "quote_max_elapsed_seconds": 5}
        class Response:
            headers = {}
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read(self, limit):
                return b'[{"symbol":"DOGEUSDT","bidPrice":"1","askPrice":"1"},{"symbol":"DOGEUSDT","bidPrice":"1","askPrice":"1"}]'
        with self.assertRaises(ValueError):
            worker_module.fetch_quotes(policy, open_url=lambda *a, **k: Response())



class RealCoreWorkerIntegrationTests(unittest.TestCase):
    def test_real_core_bootstrap_and_quotes_checkpoint_without_entering_existing_rows(self):
        from research_forward_paper_engine import PaperStudy
        checkpoints = []
        class Store:
            def __init__(self, dsn, study_id, config):
                self.config = config
                self.engine = None
                self.journal_sequence = 0
            def acquire(self): return True
            def restore_engine(self, cls):
                self.engine = cls(":memory:", {**self.config, "study_start_utc": "2026-10-21T00:00:00Z"}, resume=False)
                return self.engine
            def checkpoint(self, engine, status):
                self.journal_sequence = engine.db.execute("SELECT COUNT(*) FROM journal").fetchone()[0]
                checkpoints.append(copy.deepcopy(engine.summary()))
            def update_health(self, status): pass
            def close(self): pass
        def candidates(dsn, policy, template, **kwargs):
            return {"poll_id": "bootstrap", "request_started_utc": "2026-10-21T00:00:00.01Z",
                    "response_received_utc": "2026-10-21T00:00:00.1Z", "bootstrap": True, "rows": []}
        def quotes(policy):
            return {"packet_id": "quote", "request_started_utc": "2026-10-21T00:00:01Z",
                    "response_received_utc": "2026-10-21T00:00:02Z", "monotonic_elapsed_seconds": 1,
                    "source": policy["quote_source"],
                    "quotes": [{"symbol": "DOGE", "bid": ".1", "ask": ".101"},
                               {"symbol": "ETH", "bid": "2000", "ask": "2001"}]}
        worker = worker_module.ForwardPaperWorker(store_factory=Store, engine_class=PaperStudy,
            candidate_fetch=candidates, quote_fetch=quotes)
        worker._dsn = "unused"
        worker._log = lambda **kwargs: None
        try:
            worker._cycle_owned()
            self.assertEqual(len(checkpoints), 2)
            self.assertTrue(checkpoints[-1]["bootstrap_complete"])
            self.assertEqual(set(checkpoints[-1]["books"]), {"DOGE_only", "ETH_only", "first_per_parent"})
            self.assertTrue(all(not b["choices"] and not b["active"] for b in checkpoints[-1]["books"].values()))
            self.assertEqual(worker.status()["journal_events"], 4)
        finally:
            worker._discard_owned()


if __name__ == "__main__":
    unittest.main()

