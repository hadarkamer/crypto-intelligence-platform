"""Finite journal tests; actual PostgreSQL is opt-in on an isolated CI/local DSN."""
from copy import deepcopy
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import sys
import threading
import time
from types import ModuleType
import unittest
from unittest.mock import patch
from uuid import uuid4

import research_runtime_journal as journal
import research_runtime_memory as memory

BASE = "shared-watch:2026-10-31T12:02:15.000001+00:00"
OTHER = "shared-watch:2026-10-31T12:32:15.000001+00:00"
STAMP = "2026-10-31T12:02:15.000002+00:00"
COMMIT = "a" * 40


def fresh():
    spec = importlib.util.spec_from_file_location("isolated_runtime_journal_" + uuid4().hex, Path(journal.__file__))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def pins(module):
    return {"main.py":"1" * 64, "research_runtime_memory.py":"2" * 64,
            "research_watch_runtime.py":"3" * 64, "research_runtime_journal.py":module._writer_code_sha256()}


def event(module=journal, cycle=BASE, with_memory=False):
    handle = module._Handle(cycle, ("a" * 32, STAMP), pins(module), [], COMMIT)
    handle._sequence = 1
    sample = memory._empty() if with_memory else None
    if sample is not None: sample["elapsed_ms"] = -0.0
    with patch.object(module, "_utc_now", return_value=STAMP):
        return module._event(handle, "START_REQUESTED", "STARTING", memory=sample)


def wait_for(predicate, seconds=3.):
    deadline = time.monotonic() + seconds
    while not predicate() and time.monotonic() < deadline: time.sleep(.005)
    if not predicate(): raise AssertionError("bounded worker wait timed out")


class Sink:
    def __init__(self, module): self.module, self.events = module, []
    def __call__(self, value):
        prepared = self.module.prepare(value)
        self.events.append(json.loads(prepared["payload_json"]))
        return {"version":self.module.VERSION, "status":"QUEUED", "durable":False, **self.module._summary(prepared)}


class Backend:
    def __init__(self): self.items = []
    def persist_prepared(self, prepared, enqueued_at):
        self.items.append(deepcopy(prepared))
        return {**journal._summary(prepared), "persisted":True, "idempotent_existing":False, "created_at_utc":enqueued_at or STAMP}


class JournalTests(unittest.TestCase):
    def test_import_body_creates_no_thread_connection_or_source_file_io(self):
        text = Path(journal.__file__).read_text(encoding="utf-8")
        code = compile(text, "<isolated_import_body>", "exec")
        namespace = {"__file__":str(Path(journal.__file__)), "__name__":"lazy_runtime_journal"}
        with patch("threading.Thread", side_effect=AssertionError("thread on import")), \
             patch.object(Path, "read_bytes", side_effect=AssertionError("source read on import")), \
             patch("builtins.open", side_effect=AssertionError("open on import")):
            exec(code, namespace)
        self.assertIsNone(namespace["_WORKER"])
        self.assertIsNone(namespace["_STORE"])
        self.assertIsNone(namespace["_SESSION"])

    def test_canonical_text_preserves_scalar_preimage_and_sample_hash(self):
        value = event(with_memory=True)
        prepared = journal.prepare(value)
        self.assertEqual(prepared["payload_json"], journal.canonical(value))
        self.assertIn("-0.0", prepared["payload_json"])
        self.assertEqual(prepared["payload_bytes"], len(prepared["payload_json"].encode("utf-8")))
        self.assertEqual(value["memory"]["sample_sha256"], journal.digest(value["memory"]["sample"]))
        self.assertEqual(value["memory"]["sample"]["status"], "UNAVAILABLE")
        self.assertIn("MEMORY_SAMPLE_UNAVAILABLE", value["capture_flags"])
        self.assertFalse(journal._jsonb_equal(False, 0))
        self.assertTrue(journal._jsonb_equal(-0.0, 0))

    def test_strict_event_schema_bytes_types_and_private_fields(self):
        invalid = [None, [], {"unknown":True}]
        for key, bad in (("sequence", True), ("sequence", 33), ("cycle_id", "shared-watch:not-a-clock"),
                         ("runtime_git_commit", "private endpoint"), ("reason_class", "RuntimeError: secret"),
                         ("phase_revision", True), ("event_time_utc", "2026-01-01")):
            value = event(); value[key] = bad; invalid.append(value)
        value = event(); value["memory"] = {"status":"CAPTURED", "sample":{"token":"private"}, "sample_sha256":"0" * 64}; invalid.append(value)
        value = event(); value["extra"] = "x" * (journal.MAX_EVENT_BYTES + 1); invalid.append(value)
        value = event(); value["source_code_sha256"] = {"main.py":float("nan")}; invalid.append(value)
        value = event(); value["source_code_sha256"] = {"main.py":object()}; invalid.append(value)
        value = event(); value["source_code_sha256"] = {"main.py":"f" * 63}; invalid.append(value)
        value = event(); value["memory"]["loop"] = value; invalid.append(value)
        for value in invalid:
            with self.subTest(kind=type(value).__name__):
                with self.assertRaises(Exception): journal.prepare(value)

    def test_start_is_enqueued_before_finish_and_database_ack(self):
        module = fresh()
        entered, release = threading.Event(), threading.Event()
        class Slow(Backend):
            def persist_prepared(self, prepared, enqueued_at):
                entered.set()
                if not release.wait(3.): raise RuntimeError("test release deadline")
                return super().persist_prepared(prepared, enqueued_at)
        module._STORE = backend = Slow()
        try:
            handle = module.begin(BASE, runtime_commit=COMMIT, source_code_sha256=pins(module))
            self.assertEqual(handle.start_receipt["status"], "QUEUED")
            self.assertFalse(handle.start_receipt["durable"])
            self.assertTrue(entered.wait(2.))
            self.assertEqual(module.status()["start_persisted"], 0)
            self.assertIsNone(module.status()["last_durable_start"])
        finally: release.set()
        wait_for(lambda:module.status()["start_persisted"] == 1)
        self.assertFalse(handle._closed)
        self.assertEqual(json.loads(backend.items[0]["payload_json"])["event_type"], "START_REQUESTED")
        module.finish(handle)
        wait_for(lambda:module.status()["queued_items"] == 0)
        self.assertEqual(module.status()["finish_persisted"], 1)
        self.assertIsNone(module.current())

    def test_nested_context_reset_and_failed_initialization_restore_previous(self):
        module = fresh(); sink = Sink(module)
        with patch.object(module, "submit", side_effect=sink):
            outer = module.begin(BASE, runtime_commit=COMMIT, source_code_sha256=pins(module))
            inner = module.begin(OTHER, runtime_commit=COMMIT, source_code_sha256=pins(module))
            self.assertIs(module.current(), inner)
            module.finish(inner, status="cancelled", reason_class="CancelledError")
            self.assertIs(module.current(), outer)
            with patch.object(module, "_event", side_effect=SystemExit("private initialization message")):
                self.assertIsNone(module.begin(OTHER))
            self.assertIs(module.current(), outer)
            module.finish(outer)
        self.assertIsNone(module.current())
        self.assertNotIn("private initialization", json.dumps(module.status()))

    def test_partial_contextvar_set_failure_attempts_previous_scope_restore(self):
        module = fresh(); previous = object()
        class FaultyContext:
            def __init__(self): self.value, self.failed = previous, False
            def get(self): return self.value
            def set(self, value):
                self.value = value
                if not self.failed:
                    self.failed = True
                    raise MemoryError("private context allocation")
            def reset(self, token): raise AssertionError("no token was returned")
        context = FaultyContext()
        with patch.object(module, "_CURRENT", context):
            self.assertIsNone(module.begin(BASE, source_code_sha256=pins(module)))
            self.assertIs(context.value, previous)

    def test_out_of_order_nested_finish_unwinds_closed_outer_without_clobbering_live_inner(self):
        module = fresh(); sink = Sink(module); foreign = object()
        token = module._CURRENT.set(foreign)
        try:
            with patch.object(module, "submit", side_effect=sink):
                outer = module.begin(BASE, source_code_sha256=pins(module))
                inner = module.begin(OTHER, source_code_sha256=pins(module))
                module.finish(outer)
                module.reset(outer)
                self.assertIs(module._CURRENT.get(), inner)
                self.assertIs(module.current(), inner)
                module.finish(inner)
                self.assertIs(module._CURRENT.get(), foreign)
                self.assertIsNone(module.current())
        finally: module._CURRENT.reset(token)

    def test_finish_diagnostic_baseexception_does_not_mask_native_exception(self):
        module = fresh(); sink = Sink(module); native_error = RuntimeError("native sentinel")
        with patch.object(module, "submit", side_effect=sink):
            handle = module.begin(BASE, source_code_sha256=pins(module))
            try:
                try: raise native_error
                finally:
                    with patch.object(module, "_event", side_effect=KeyboardInterrupt("private diagnostic failure")):
                        module.finish(handle, status="FAILED", reason_class="RuntimeError")
            except RuntimeError as error: self.assertIs(error, native_error)
        self.assertTrue(handle._closed)
        self.assertIsNone(module.current())
        self.assertNotIn("private diagnostic", json.dumps(module.status()))

    def test_finish_preserves_native_cancelled_error_identity(self):
        import asyncio
        module = fresh(); sink = Sink(module); cancelled = asyncio.CancelledError("native cancel sentinel")
        async def native():
            handle = module.begin(BASE, source_code_sha256=pins(module))
            try: raise cancelled
            finally:
                with patch.object(module, "submit", side_effect=SystemExit("private observer failure")):
                    module.finish(handle, status="CANCELLED", reason_class="CancelledError")
        with patch.object(module, "submit", side_effect=sink):
            try: asyncio.run(native())
            except asyncio.CancelledError as error: self.assertIs(error, cancelled)
            else: self.fail("native cancellation was swallowed")

    def test_thirty_two_event_cap_reserves_finish_and_rejects_late_stages(self):
        module = fresh(); sink = Sink(module)
        with patch.object(module, "submit", side_effect=sink):
            handle = module.begin(BASE, source_code_sha256=pins(module))
            responses = [module.stage("PERIODIC_MEMORY", handle=handle, phase="COLLECTING_INPUTS") for _ in range(40)]
            self.assertEqual(sum(row["status"] == "QUEUED" for row in responses), 30)
            self.assertEqual(module.finish(handle)["status"], "QUEUED")
            before = len(sink.events)
            self.assertEqual(module.stage("INPUTS_READY", handle=handle)["reason_code"], "HANDLE_CLOSED")
            self.assertEqual(module.finish(handle)["reason_code"], "ALREADY_FINISHED")
            self.assertEqual(len(sink.events), before)
        self.assertEqual([row["sequence"] for row in sink.events], list(range(1, 33)))
        self.assertEqual(sink.events[-1]["event_type"], "FINISH_REQUESTED")
        self.assertEqual(sink.events[-1]["finish_scope"], module.FINISH_SCOPE)

    def test_native_completed_with_gap_statuses_keep_exact_outcome_semantics(self):
        module = fresh(); sink = Sink(module)
        outcomes = ("COMPLETED", "COMPLETED_WITH_DELIVERY_ERRORS", "COMPLETED_WITH_TRANSITION_GAPS", "FAILED", "CANCELLED", "UNKNOWN")
        with patch.object(module, "submit", side_effect=sink):
            for index, outcome in enumerate(outcomes):
                with self.subTest(outcome=outcome):
                    handle = module.begin("shared-watch:2026-10-31T12:03:" + str(index).zfill(2) + "+00:00", source_code_sha256=pins(module))
                    module.finish(handle, status=outcome.lower())
                    value = sink.events[-1]
                    self.assertEqual(value["outcome"], outcome)
                    self.assertEqual(value["stage"], "FINALLY_ENTERED")
                    self.assertEqual(value["finish_scope"], module.FINISH_SCOPE)
                    self.assertEqual("OUTCOME_UNAVAILABLE" in value["capture_flags"], outcome == "UNKNOWN")

    def test_session_clock_is_named_for_lazy_observer_session_not_process_birth(self):
        module = fresh(); sink = Sink(module)
        with patch.object(module, "submit", side_effect=sink), patch.object(module, "_utc_now", return_value=STAMP):
            handle = module.begin(BASE, source_code_sha256=pins(module))
            module.finish(handle)
        self.assertEqual(sink.events[0]["process_session_started_at_utc"], STAMP)
        self.assertNotIn("process_started_at_utc", sink.events[0])
        self.assertEqual(module.status()["process_session_started_at_utc"], STAMP)
        self.assertIn("not operating-system process birth", module.status()["session_clock_semantics"])

    def test_explicit_handle_stages_race_finish_without_duplicate_or_postfinish_records(self):
        module = fresh(); sink = Sink(module); started = threading.Event()
        with patch.object(module, "submit", side_effect=sink):
            handle = module.begin(BASE, source_code_sha256=pins(module))
            def probes():
                started.set()
                for _ in range(100): module.stage("PERIODIC_MEMORY", handle=handle, phase="COLLECTING_INPUTS")
            thread = threading.Thread(target=probes)
            thread.start(); self.assertTrue(started.wait(2.))
            module.finish(handle)
            thread.join(2.); self.assertFalse(thread.is_alive())
        sequences = [value["sequence"] for value in sink.events]
        self.assertEqual(sequences, sorted(set(sequences)))
        self.assertLessEqual(len(sequences), 32)
        self.assertEqual(sink.events[-1]["event_type"], "FINISH_REQUESTED")
        self.assertIsNone(module.current())

    def test_memory_is_validated_without_probing_and_inputs_are_frozen(self):
        module = fresh(); sink = Sink(module); sample = memory._empty(); supplied = pins(module)
        with patch.object(module, "submit", side_effect=sink), patch.object(memory, "sample_memory", side_effect=AssertionError("journal must not probe")):
            handle = module.begin(BASE, source_code_sha256=supplied)
            module.stage("PERIODIC_MEMORY", handle=handle, memory=sample, phase="COLLECTING_INPUTS", phase_revision=2,
                         phase_observed_at_utc=STAMP, sample_started_at_utc=STAMP, sample_completed_at_utc=STAMP)
            supplied["main.py"] = "f" * 64; sample["elapsed_ms"] = 999
            module.finish(handle)
        self.assertEqual(sink.events[0]["memory"], {"status":"NOT_REQUESTED"})
        self.assertEqual(sink.events[1]["memory"]["sample"]["elapsed_ms"], 0.)
        self.assertEqual(sink.events[1]["source_code_sha256"]["main.py"], "1" * 64)
        self.assertEqual(sink.events[1]["phase_revision"], 2)

    def test_bad_memory_reason_and_marker_metadata_are_small_explicit_gaps(self):
        module = fresh(); sink = Sink(module)
        class Dangerous:
            def __repr__(self): raise AssertionError("custom repr must not execute")
            def __str__(self): raise AssertionError("custom str must not execute")
        with patch.object(module, "submit", side_effect=sink):
            handle = module.begin(BASE, source_code_sha256=pins(module))
            module.stage("PERIODIC_MEMORY", handle=handle, memory={"message":"private endpoint and token"},
                reason_class=Dangerous(), phase="private raw phase", phase_revision=True,
                phase_observed_at_utc="private clock", sample_started_at_utc=STAMP,
                sample_completed_at_utc="2026-10-31T12:01:00+00:00")
            module.finish(handle)
        value = sink.events[1]
        self.assertEqual(value["memory"], {"status":"UNAVAILABLE", "reason_code":"MEMORY_SNAPSHOT_INVALID"})
        self.assertEqual(value["reason_class"], "UNKNOWN")
        self.assertIsNone(value["phase_revision"])
        self.assertIn("SAMPLE_CLOCK_ORDER_INVALID", value["capture_flags"])
        self.assertNotIn("private", json.dumps(value))

    def test_sixteen_outstanding_items_include_inflight_and_freeze_payload(self):
        module = fresh(); entered, release = threading.Event(), threading.Event()
        class Slow(Backend):
            def persist_prepared(self, prepared, enqueued_at):
                entered.set()
                if not release.wait(3.): raise RuntimeError("test release deadline")
                return super().persist_prepared(prepared, enqueued_at)
        module._STORE = backend = Slow()
        original = event(module, with_memory=True)
        try:
            self.assertEqual(module.submit(original)["status"], "QUEUED")
            self.assertTrue(entered.wait(2.))
            original["memory"]["sample"]["elapsed_ms"] = 777
            for i in range(1, 16):
                value = event(module, cycle="shared-watch:2026-10-31T12:03:" + str(i).zfill(2) + "+00:00")
                self.assertEqual(module.submit(value)["status"], "QUEUED")
            self.assertEqual(module.submit(event(module, cycle=OTHER))["status"], "DROPPED")
            self.assertEqual(module.status()["queued_items"], 16)
            self.assertLessEqual(module.status()["queued_bytes"], module.MAX_QUEUE_BYTES)
            self.assertEqual(module.status()["persisted"], 0)
        finally: release.set()
        wait_for(lambda:module.status()["queued_items"] == 0)
        self.assertEqual(module.status()["persisted"], 16)
        self.assertEqual(json.loads(backend.items[0]["payload_json"])["memory"]["sample"]["elapsed_ms"], -0.)

    def test_queue_byte_budget_is_independent_of_item_budget(self):
        module = fresh(); entered, release = threading.Event(), threading.Event()
        class Slow(Backend):
            def persist_prepared(self, prepared, enqueued_at):
                entered.set()
                if not release.wait(3.): raise RuntimeError("test release deadline")
                return super().persist_prepared(prepared, enqueued_at)
        module._STORE = Slow(); value = event(module); size = module.prepare(value)["payload_bytes"]
        try:
            with patch.object(module, "MAX_QUEUE_BYTES", 2 * size):
                self.assertEqual(module.submit(value)["status"], "QUEUED")
                self.assertTrue(entered.wait(2.))
                self.assertEqual(module.submit(deepcopy(value))["status"], "QUEUED")
                self.assertEqual(module.submit(deepcopy(value))["reason_code"], "QUEUE_BOUND")
                self.assertEqual(module.status()["queued_items"], 2)
                self.assertEqual(module.status()["queued_bytes"], 2 * size)
        finally: release.set()
        wait_for(lambda:module.status()["queued_items"] == 0)

    def test_invalid_prepare_never_starts_worker_or_retains_sensitive_text(self):
        module = fresh()
        with patch.object(module, "_ensure_worker", side_effect=AssertionError("must not start")):
            response = module.submit({"message":"private secret endpoint"})
        self.assertEqual(response["status"], "INVALID")
        self.assertNotIn("private secret", json.dumps(module.status()))
        self.assertIsNone(module._WORKER)

    def test_unconfigured_start_is_not_retried_after_later_event_succeeds(self):
        module = fresh()
        with patch.dict(os.environ, {"DATABASE_URL":"", "RESEARCH_DATABASE_URL":""}):
            handle = module.begin(BASE, source_code_sha256=pins(module))
        self.assertEqual(handle.start_receipt["status"], "UNAVAILABLE")
        self.assertIsNone(module._WORKER)
        module._STORE = backend = Backend()
        module.stage("INPUTS_READY", handle=handle); module.finish(handle)
        wait_for(lambda:module.status()["queued_items"] == 0)
        self.assertEqual([json.loads(row["payload_json"])["sequence"] for row in backend.items], [2, 3])
        self.assertEqual(module.status()["start_persisted"], 0)
        self.assertEqual(module.status()["finish_persisted"], 1)

    def test_worker_baseexception_is_one_gap_without_retry_or_raw_text(self):
        module = fresh()
        class Broken:
            def __init__(self): self.calls = 0
            def persist_prepared(self, *args):
                self.calls += 1
                raise SystemExit("private database endpoint and password")
        module._STORE = backend = Broken()
        self.assertEqual(module.submit(event(module))["status"], "QUEUED")
        wait_for(lambda:module.status()["queued_items"] == 0)
        state = module.status()
        self.assertEqual(backend.calls, 1)
        self.assertEqual(state["failed"], 1)
        self.assertEqual(state["persisted"], 0)
        self.assertIsNone(state["last_durable"])
        self.assertTrue(state["worker_alive"])
        self.assertNotIn("private database", json.dumps(state))

    def test_clock_fault_does_not_mask_durable_backend_ack(self):
        module = fresh(); prepared = module.prepare(event(module)); module._STORE = Backend()
        module._METRICS["queued_items"], module._METRICS["queued_bytes"] = 1, prepared["payload_bytes"]
        with patch.object(module, "_utc_now", side_effect=SystemExit("private clock")):
            module._persist_one(prepared, STAMP)
        self.assertEqual(module.status()["persisted"], 1)
        self.assertIsNone(module.status()["last_durable"]["at_utc"])
        self.assertEqual(module.status()["queued_items"], 0)
        self.assertNotIn("private clock", json.dumps(module.status()))

    def test_identity_collision_and_jsonb_semantics_are_checked(self):
        prepared = journal.prepare(event(with_memory=True))
        row = {**prepared, "payload":json.loads(prepared["payload_json"]), "writer_version":journal.VERSION,
               "writer_code_sha256":journal._writer_code_sha256()}
        journal._verify_existing(row, prepared)
        row["payload_json"] += " "
        with self.assertRaisesRegex(journal.JournalValidationError, "EVENT_IDENTITY_COLLISION"):
            journal._verify_existing(row, prepared)
        row["payload_json"] = prepared["payload_json"]
        row["payload"]["memory"]["sample"]["coverage"]["budget_exhausted"] = 0
        with self.assertRaisesRegex(journal.JournalValidationError, "EVENT_JSONB_MISMATCH"):
            journal._verify_existing(row, prepared)

    def test_existing_writer_metadata_corruption_is_not_idempotent_ack(self):
        prepared = journal.prepare(event())
        row = {**prepared, "payload":json.loads(prepared["payload_json"]), "writer_version":journal.VERSION,
               "writer_code_sha256":journal._writer_code_sha256()}
        for key, bad in (("writer_version", "another-writer"), ("writer_code_sha256", "f" * 64)):
            changed = deepcopy(row); changed[key] = bad
            with self.subTest(field=key):
                with self.assertRaisesRegex(journal.JournalValidationError, "EVENT_WRITER_IDENTITY_MISMATCH"):
                    journal._verify_existing(changed, prepared)

    def test_prepared_event_tampering_is_rejected_before_driver_import(self):
        prepared = journal.prepare(event()); prepared["payload_sha256"] = "0" * 64
        with self.assertRaisesRegex(journal.JournalValidationError, "PREPARED_EVENT_CHANGED"):
            journal.PostgresStore("not a real DSN").persist_prepared(prepared, STAMP)

    def test_postgres_connect_timeout_does_not_replace_test_search_path_options(self):
        psycopg, rows = ModuleType("psycopg"), ModuleType("psycopg.rows")
        captured = []
        rows.dict_row = object()
        psycopg.connect = lambda *args, **kwargs: captured.append((args,kwargs)) or "connection"
        with patch.dict(sys.modules, {"psycopg":psycopg, "psycopg.rows":rows}):
            backend = journal.PostgresStore("host=postgres dbname=test_runtime options='-c search_path=test_only'")
            self.assertEqual(backend._connect(), "connection")
        self.assertEqual(captured[0][1]["connect_timeout"], 3)
        self.assertIn("search_path=test_only", captured[0][0][0])

    def test_commit_failure_is_not_reported_as_durable(self):
        module = fresh(); prepared = module.prepare(event(module)); calls = []
        class Result:
            def __init__(self, value): self.value = value
            def fetchone(self): return self.value
        class Connection:
            def __enter__(self): return self
            def __exit__(self, *args): raise SystemExit("private commit uncertainty")
            def execute(self, text, params=None):
                calls.append(text)
                if text.startswith("INSERT"):
                    return Result({"sequence":1})
                if text.startswith("SELECT process_session_id"):
                    return Result({**prepared, "payload":json.loads(prepared["payload_json"]), "created_at_utc":datetime.now(timezone.utc),
                                   "writer_version":module.VERSION, "writer_code_sha256":module._writer_code_sha256()})
                return Result(None)
        fake_json = ModuleType("psycopg.types.json"); fake_json.Jsonb = lambda value:value
        backend = module.PostgresStore("unused test DSN"); module._STORE = backend
        module._METRICS["queued_items"], module._METRICS["queued_bytes"] = 1, prepared["payload_bytes"]
        with patch.dict(sys.modules, {"psycopg.types.json":fake_json}), patch.object(backend, "_connect", return_value=Connection()):
            module._persist_one(prepared, STAMP)
        self.assertEqual(calls[:2], ["SET LOCAL statement_timeout = 3000", "SET LOCAL lock_timeout = 1000"])
        self.assertTrue(any("PRIMARY KEY(process_session_id,cycle_id,sequence)" in sql for sql in calls))
        self.assertTrue(any("DO NOTHING" in sql for sql in calls))
        self.assertEqual(module.status()["failed"], 1)
        self.assertEqual(module.status()["persisted"], 0)
        self.assertIsNone(module.status()["last_durable"])
        self.assertFalse(backend.initialized)


_TEST_URL = os.getenv("TEST_DATABASE_URL") or os.getenv("RESEARCH_TEST_POSTGRES_URL")


@unittest.skipUnless(_TEST_URL, "Explicit local/CI PostgreSQL test DSN required")
class PostgreSQLTests(unittest.TestCase):
    def test_real_immutable_runtime_journal_text_jsonb_and_start_row(self):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict, make_conninfo
        info = conninfo_to_dict(_TEST_URL)
        if info.get("host") not in {"localhost", "127.0.0.1", "::1", "postgres"} or not (
                info.get("dbname", "").startswith("test_") or info.get("dbname", "").endswith("_test")):
            raise ValueError("Explicit local test database required")
        schema = "runtime_journal_test_" + uuid4().hex
        admin = psycopg.connect(_TEST_URL, autocommit=True)
        created = False
        try:
            admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema))); created = True
            isolated = dict(info); isolated["options"] = (info.get("options", "") + " -c search_path=" + schema).strip()
            backend = journal.PostgresStore(make_conninfo(**isolated))
            value = event(with_memory=True)
            first = backend.persist(value, STAMP)
            second = backend.persist(deepcopy(value), STAMP)
            self.assertTrue(first["persisted"])
            self.assertFalse(first["idempotent_existing"])
            self.assertTrue(second["idempotent_existing"])
            self.assertEqual(first["event_type"], "START_REQUESTED")
            changed = deepcopy(value); changed["phase"] = "UNKNOWN"
            with self.assertRaisesRegex(journal.JournalValidationError, "EVENT_IDENTITY_COLLISION"):
                backend.persist(changed, STAMP)
            with backend._connect() as connection:
                rows = connection.execute("SELECT payload_json,payload,payload_sha256 FROM research_runtime_journal_events").fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["payload_json"], journal.canonical(value))
            self.assertEqual(rows[0]["payload_sha256"], journal.prepare(value)["payload_sha256"])
            self.assertTrue(journal._jsonb_equal(rows[0]["payload"], value))
            self.assertIn("-0.0", rows[0]["payload_json"])
            # Deliberate corruption only inside this disposable local/CI schema:
            # the journal must not acknowledge an existing row with wrong writer identity.
            with backend._connect() as connection:
                connection.execute("UPDATE research_runtime_journal_events SET writer_code_sha256=%s WHERE process_session_id=%s AND cycle_id=%s AND sequence=1",
                                   ("f" * 64, value["process_session_id"], value["cycle_id"]))
            with self.assertRaisesRegex(journal.JournalValidationError, "EVENT_WRITER_IDENTITY_MISMATCH"):
                backend.persist(deepcopy(value), STAMP)
        finally:
            if created: admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
            admin.close()


if __name__ == "__main__": unittest.main()
