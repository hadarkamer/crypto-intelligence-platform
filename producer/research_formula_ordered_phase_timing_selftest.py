"""Bounded phase receipts must not alter formula research control flow."""
from contextlib import ExitStack
from datetime import datetime, timezone
import json
from types import SimpleNamespace
from unittest.mock import patch

import research_formula_ordered_worker as worker


RESEARCH_NOW = datetime(2026, 9, 6, tzinfo=timezone.utc)
WALL_NOW = datetime(2030, 1, 1, tzinfo=timezone.utc)


class WallClock(datetime):
    @classmethod
    def now(cls, tz=None):
        return WALL_NOW


class Clock:
    def __init__(self, mode=None):
        self.ns = 1_000_000_000
        self.mode = mode
        self.reads = 0

    def advance(self, microseconds):
        self.ns += microseconds * 1000

    def read(self):
        self.reads += 1
        if self.mode == "failed":
            raise RuntimeError("private clock error")
        if self.mode == "regressed" and self.reads > 1:
            return 999_999_999
        return self.ns


class Connection:
    def __init__(self, clock, *, locked, cleanup_error, exit_error):
        self.clock = clock
        self.locked = locked
        self.cleanup_error = cleanup_error
        self.exit_error = exit_error
        self.events = []
        self.pending = set()
        self.committed = set()

    def __enter__(self):
        self.events.append("enter")
        return self

    def __exit__(self, *args):
        self.events.append("exit")
        if self.exit_error is not None:
            raise self.exit_error
        return False

    def execute(self, sql, params=()):
        if "pg_try_advisory_lock" in sql:
            self.events.append("lock")
            self.clock.advance(3)
        elif "pg_advisory_unlock" in sql:
            self.events.append("unlock")
            self.clock.advance(3)
        elif "SET evaluation_input_sha256=" in sql:
            self.events.append("changed-update")
            self.pending.add(params[1])
            self.clock.advance(4)
        elif "SET last_evaluated_at_utc=" in sql:
            self.events.append("unchanged-update")
            self.pending.add(params[1])
            self.clock.advance(8)
        else:
            assert "SELECT COUNT(*)" in sql, sql
            self.events.append("attempt-count")
        return self

    def fetchone(self):
        return {"acquired": self.locked, "n": 3}

    def commit(self):
        self.events.append("commit")
        self.clock.advance(1)
        self.committed.update(self.pending)
        self.pending.clear()

    def rollback(self):
        self.events.append("rollback")
        self.clock.advance(2)
        if self.cleanup_error is not None:
            raise self.cleanup_error
        self.pending.clear()


def exercise(*, locked=True, failed_step=None, cleanup_fails=False,
             exit_fails=False, clock_mode=None, log_fails=False, fault=None):
    clock = Clock(clock_mode)
    step_error = RuntimeError("private step sentinel")
    cleanup_error = RuntimeError("private cleanup sentinel") if cleanup_fails else None
    exit_error = RuntimeError("private exit sentinel") if exit_fails else None
    conn = Connection(clock, locked=locked, cleanup_error=cleanup_error, exit_error=exit_error)
    calls = []
    logs = []
    candidate = {"formula_id": "private-candidate", "research_orientation": "NORMAL"}
    period = "SINCE_20260904"
    scopes = [{"scope_key": f"private-scope-{index}", "candidate_key": candidate["formula_id"],
               "symbol": "ALL", "direction": "LONG", "window_minutes": 60,
               "threshold_bps": index * 25, "period_key": period,
               "period_start_utc": worker.store.PERIODS[period], "result": {}}
              for index in (1, 2, 3)]
    scopes[1]["evaluation_input_sha256"] = worker.store.digest(
        {"input": "fixture-input", "validation_available": True, "registered_attempts": 3})

    def timed(name, delay, value):
        def call(*args, **kwargs):
            calls.append(name)
            clock.advance(delay(*args, **kwargs) if callable(delay) else delay)
            if failed_step == name:
                raise step_error
            return value(*args, **kwargs) if callable(value) else value
        return call

    def connect(url):
        assert url == "postgresql://phase-selftest"
        clock.advance(2)
        return conn

    def emit(*args, **kwargs):
        logs.append((args, kwargs))
        if log_fails:
            raise RuntimeError("private log error")

    def diagnostic_fault(*args, **kwargs):
        raise RuntimeError("private diagnostic fault")

    service = worker.ResearchFormulaOrderedWorker()
    service.metrics["last_timing_receipt_json"] = "stale prior receipt"
    result = None
    error = None
    with ExitStack() as stack:
        overrides = [
            (worker, "psycopg", object()),
            (worker, "_database_url", lambda: "postgresql://phase-selftest"),
            (worker, "_connect", connect),
            (worker, "_PASS_SECONDS", 40),
            (worker, "datetime", WallClock),
            (worker.time, "monotonic", lambda: 0.0),
            (worker.time, "perf_counter_ns", clock.read),
            (worker.store, "register_catalog", timed("catalog", 4, [candidate])),
            (worker.store, "ingest_matches", timed("intake", 5, {"inverse_source_event_ids": []})),
            (worker.inverse_store, "available", timed("inverse", 6, False)),
            (worker.validation_store, "schema_status", timed("schema", 1, {"schema_present": True})),
            (worker.experimental_worker, "enabled", lambda: False),
            (worker.store, "source_population_complete", lambda *a, **kw: True),
            (worker.store, "due_scopes", timed("queue", 8, scopes)),
            (worker.store, "candidate_feature_coverage_complete", timed("feature", 9, {"complete": True})),
            (worker.store, "load_scope_rows", timed("scope_rows",
                lambda conn, scope, **kw: scope["threshold_bps"] * 10 // 25, ([], False))),
            (worker.store, "membership_complete", timed("membership", 2, True)),
            (worker.store, "common_window_rows", timed("common", 4, {})),
            (worker.question_store, "evaluation_input", timed("input", 7, "fixture-input")),
            (worker.evaluator, "summarize_scope", timed("summary", 11,
                lambda *a, **kw: {"exclusion_reasons": {}})),
            (worker.store, "period_coverage", timed("period", 13, {})),
            (worker.validation_store, "register_supported_acceptance", timed("acceptance", 2, None)),
            (worker.validation_store, "evaluate_scope", timed("validation", 11,
                {"research_ready": False, "validation_status": "VALIDATING_PROSPECTIVE"})),
            (worker.store, "persist_scope", timed("persist", 14, {"episodes": 1, "upserts": 1})),
            (worker.question_store, "record_scope_trial", timed("trial", 3, None)),
            (worker.store.publication, "seed_missing_scope", timed("seed", 5, 1)),
        ]
        for target, name, value in overrides:
            stack.enter_context(patch.object(target, name, value))
        stack.enter_context(patch.object(worker, "print", emit, create=True))
        if fault == "construct":
            stack.enter_context(patch.object(worker, "_PassTiming", diagnostic_fault))
        elif fault in ("phase", "finish"):
            stack.enter_context(patch.object(worker._PassTiming, fault, diagnostic_fault))
        elif fault == "serialize":
            stack.enter_context(patch.object(worker, "json", SimpleNamespace(dumps=diagnostic_fault)))
        try:
            result = service.run_once(now=RESEARCH_NOW)
        except Exception as exc:
            error = exc
    return SimpleNamespace(result=result, error=error, service=service, conn=conn,
                           calls=calls, logs=logs, clock=clock, step_error=step_error,
                           cleanup_error=cleanup_error, exit_error=exit_error)


EXPECTED_PHASES = {
    "connect_lock": (1, 6, 6),
    "catalog": (1, 4, 4),
    "intake": (1, 5, 5),
    "inverse": (1, 7, 7),
    "queue": (1, 10, 10),
    "feature_coverage": (3, 9, 9),
    "scope_rows": (3, 60, 30),
    "membership": (3, 6, 2),
    "common_windows": (3, 12, 4),
    "input_hash": (3, 21, 7),
    "summary_and_period": (2, 48, 24),
    "validation": (2, 26, 13),
    "persist_changed": (2, 44, 22),
    "persist_unchanged": (1, 14, 14),
    "cleanup": (1, 6, 6),
}


def receipt(case):
    encoded = case.service.metrics["last_timing_receipt_json"]
    assert type(encoded) is str and len(encoded.encode("utf-8")) < 8192
    value = json.loads(encoded)
    assert set(value) == {"version", "outcome", "started_at_utc", "finished_at_utc",
                          "timing_complete", "overflow", "elapsed_us", "phases"}
    assert value["version"] == "ordered-formula-timing-v1"
    assert value["outcome"] in ("completed", "locked", "failed")
    assert type(value["timing_complete"]) is bool
    assert type(value["overflow"]) is bool
    assert set(value["phases"]) == set(EXPECTED_PHASES)
    for phase in value["phases"].values():
        assert set(phase) == {"count", "total_us", "max_us"}
        assert all(type(number) is int and 0 <= number <= 2**63 - 1
                   for number in phase.values())
        assert phase["max_us"] <= phase["total_us"]
    elapsed = value["elapsed_us"]
    assert elapsed is None or type(elapsed) is int and 0 <= elapsed <= 2**63 - 1
    for key in ("started_at_utc", "finished_at_utc"):
        assert datetime.fromisoformat(value[key].replace("Z", "+00:00")) == WALL_NOW
    assert "private" not in encoded and "postgresql" not in encoded
    assert RESEARCH_NOW.isoformat() not in encoded
    assert len(case.logs) == 1
    assert case.logs[0] == (("[ORDERED_FORMULA_TIMING] " + encoded,), {"flush": True})
    return value


def functional_parity(case, baseline):
    assert case.error is None
    assert case.result == baseline.result
    assert case.calls == baseline.calls
    assert case.conn.events == baseline.conn.events
    assert case.conn.committed == baseline.conn.committed


def run():
    normal = exercise()
    assert normal.error is None
    assert normal.result["scopes_evaluated"] == 2
    assert normal.result["unchanged_scopes_skipped"] == 1
    assert normal.result["episodes"] == 2 and normal.result["upserts"] == 3
    assert normal.result["evaluation_budget_seconds"] == 40
    assert normal.result["evaluation_budget_exhausted"] is False
    assert normal.result["preparation_seconds"] == normal.result["evaluation_seconds"] == 0
    assert normal.conn.committed == {f"private-scope-{index}" for index in (1, 2, 3)}
    assert normal.conn.events.count("commit") == 7
    assert normal.conn.events[-4:] == ["rollback", "unlock", "commit", "exit"]
    assert normal.calls.count("feature") == 1
    recorded = receipt(normal)
    assert recorded["outcome"] == "completed"
    assert recorded["timing_complete"] is True and recorded["overflow"] is False
    assert recorded["elapsed_us"] == 278
    for name, expected in EXPECTED_PHASES.items():
        phase = recorded["phases"][name]
        assert (phase["count"], phase["total_us"], phase["max_us"]) == expected, name

    denied = exercise(locked=False)
    assert denied.error is None and denied.result["locked"] is True
    assert denied.calls == [] and denied.conn.committed == set()
    assert denied.conn.events == ["enter", "lock", "commit", "exit"]
    denied_receipt = receipt(denied)
    assert denied_receipt["outcome"] == "locked"
    assert denied_receipt["elapsed_us"] == 6
    assert denied_receipt["phases"]["connect_lock"] == {"count": 1, "total_us": 6, "max_us": 6}
    assert all(phase["count"] == phase["total_us"] == phase["max_us"] == 0
               for name, phase in denied_receipt["phases"].items() if name != "connect_lock")

    failed = exercise(failed_step="scope_rows")
    assert failed.error is failed.step_error and failed.result is None
    assert failed.conn.committed == set()
    assert failed.conn.events[-4:] == ["rollback", "unlock", "commit", "exit"]
    failed_receipt = receipt(failed)
    assert failed_receipt["outcome"] == "failed"
    assert failed_receipt["phases"]["scope_rows"] == {"count": 1, "total_us": 10, "max_us": 10}
    assert failed_receipt["phases"]["cleanup"] == {"count": 1, "total_us": 6, "max_us": 6}

    partial_failure = exercise(failed_step="seed")
    assert partial_failure.error is partial_failure.step_error
    assert partial_failure.result is None
    assert partial_failure.conn.committed == {"private-scope-1"}
    assert partial_failure.conn.pending == set()
    assert partial_failure.conn.events[-4:] == ["rollback", "unlock", "commit", "exit"]
    partial_receipt = receipt(partial_failure)
    assert partial_receipt["outcome"] == "failed"
    assert partial_receipt["phases"]["persist_changed"]["count"] == 1
    assert partial_receipt["phases"]["persist_unchanged"]["count"] == 1

    partial_log_failure = exercise(failed_step="seed", log_fails=True)
    assert partial_log_failure.error is partial_log_failure.step_error
    assert partial_log_failure.result is None
    assert partial_log_failure.conn.committed == {"private-scope-1"}
    assert partial_log_failure.conn.pending == set()
    assert partial_log_failure.conn.events == partial_failure.conn.events
    assert receipt(partial_log_failure) == partial_receipt

    cleanup = exercise(failed_step="scope_rows", cleanup_fails=True)
    assert cleanup.error is cleanup.cleanup_error
    assert cleanup.conn.events[-2:] == ["rollback", "exit"]
    assert "unlock" not in cleanup.conn.events
    assert cleanup.conn.events.count("commit") == 3
    assert receipt(cleanup)["outcome"] == "failed"

    exited = exercise(exit_fails=True)
    assert exited.error is exited.exit_error
    assert exited.conn.committed == normal.conn.committed
    assert exited.conn.events == normal.conn.events
    assert receipt(exited)["outcome"] == "failed"

    for mode in ("failed", "regressed"):
        clock_failure = exercise(clock_mode=mode)
        functional_parity(clock_failure, normal)
        partial = receipt(clock_failure)
        assert partial["outcome"] == "completed"
        assert partial["timing_complete"] is False
        assert partial["elapsed_us"] is None

    logged = exercise(log_fails=True)
    functional_parity(logged, normal)
    assert receipt(logged) == recorded

    phase_failure = exercise(fault="phase")
    functional_parity(phase_failure, normal)
    assert receipt(phase_failure)["timing_complete"] is False

    for fault in ("construct", "finish", "serialize"):
        unavailable = exercise(fault=fault)
        functional_parity(unavailable, normal)
        assert unavailable.service.metrics["last_timing_receipt_json"] is None
        assert unavailable.logs == []

    print("ordered formula phase timing: bounded receipts, separate clocks and failure isolation PASS")


if __name__ == "__main__":
    run()
