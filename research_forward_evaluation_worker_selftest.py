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
            "state_json": {"observation_sequence": 2, "books": {"DOGE_only": {"closed": []}}}}


def coverage(cutoff=10):
    return {"candidate_packets_total": 1, "candidate_nonempty_packets_total": 0,
            "candidate_empty_packets_total": 1, "candidate_bootstrap_packets_total": 0,
            "candidate_selected_packets_total": 0, "candidate_packet_rows": 0,
            "candidate_invalid_rows_packets_total": 0, "quote_packets_total": 1,
            "total_input_packets": 2, "rejected_input_packets": 0,
            "source_decision_rows_total": 0, "total_journal_rows": cutoff,
            "min_journal_seq": 1, "max_journal_seq": cutoff,
            "max_candidate_selected_seq": None, "max_source_decision_seq": None}


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
        self.coverage = coverage()
        self.source_packets = []
        self.source_decisions = []
        self.predicate_evidence_value = {"matches": [{"scope_key": "doge", "event_id": 1,
            "decision_features": {"condition": 2, "non_condition": "retained"}, "source_direction": "SHORT"}],
            "definitions": [{"candidate_key": "doge-c", "formula_version": "v1", "definition": {"value": 1}}],
            "expected_keys": ["doge:1"], "counts": {"expected_positive_keys": 1,
            "matched_rows": 1, "definition_rows": 1}, "extraction_complete": True}
        self.rows = {}
        self.trace = []
        self.writes = 0
        self.metadata = []
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

                    def counts(self):
                        factory.trace_call("coverage_read")
                        return copy.deepcopy(factory.coverage)

                    def source_evidence(self, counts):
                        factory.trace_call("source_evidence_read")
                        return {"packets": copy.deepcopy(factory.source_packets),
                                "decisions": copy.deepcopy(factory.source_decisions),
                                "counts": copy.deepcopy(counts), "selection_complete": True,
                                "source_journal_seq": self.snapshot["journal_seq"]}

                    def predicate_evidence(self):
                        factory.trace_call("predicate_evidence_read")
                        result = copy.deepcopy(factory.predicate_evidence_value)
                        result.update(source_journal_seq=self.snapshot["journal_seq"],
                                      read_utc=self.server_time.isoformat())
                        return result

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

            def latest_pass(self, version, policy_sha):
                factory.trace_call("latest_pass")
                for row in reversed(factory.rows.values()):
                    result = row["assessment"]
                    if result["status"] == "PASS" and result["evaluator_version"] == version and result["policy_sha256"] == policy_sha:
                        return copy.deepcopy(result)
                return None

            def persist(self, result, metadata):
                factory.trace_call("persist")
                factory.metadata.append(copy.deepcopy(metadata))
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
        self.factory.snapshot["state_json"]["observation_sequence"] += 190
        for key in ("candidate_packets_total", "candidate_empty_packets_total", "quote_packets_total"):
            self.factory.coverage[key] += 95
        self.factory.coverage["total_input_packets"] += 190
        self.factory.coverage["total_journal_rows"] = self.factory.coverage["max_journal_seq"] = 200
        self.cycle()
        self.assertEqual(self.factory.writes, 1)
        self.assertEqual(len(self.core.calls), 1)
        self.assertEqual(names.count("quote_read"), 1)
        self.assertEqual(self.worker.status()["state"], "WAITING_FOR_NEW_DATA")
        self.assertEqual(self.worker.status()["source_journal_seq"], 200)
        self.assertEqual(self.worker.status()["assessment_source_journal_seq"], 10)
        self.assertEqual([x[0] for x in self.factory.trace].count("coverage_read"), 2)
        self.assertEqual([x[0] for x in self.factory.trace].count("source_evidence_read"), 1)
        self.assertEqual([x[0] for x in self.factory.trace].count("predicate_evidence_read"), 2)
        names = [x[0] for x in self.factory.trace]
        self.assertLess(names.index("predicate_evidence_read"), names.index("find_pass"))

    def test_pending_is_not_persisted_or_cached_and_can_complete(self):
        self.core.statuses = ["PENDING_PRICE_DATA", "PASS"]
        self.cycle()
        self.assertEqual(self.factory.writes, 0)
        self.assertEqual(self.worker.status()["state"], "PENDING_PRICE_DATA")
        self.cycle()
        self.assertEqual(len(self.core.calls), 2)
        self.assertEqual(self.factory.writes, 1)
        self.assertEqual(self.worker.status()["state"], "AUDIT_PASSED")

    def test_pending_source_evidence_is_also_retried_not_persisted(self):
        self.core.statuses = ["PENDING_SOURCE_EVIDENCE", "PASS"]
        self.cycle()
        self.assertEqual(self.factory.writes, 0)
        self.assertEqual(self.worker.status()["state"], "PENDING_SOURCE_EVIDENCE")
        self.cycle()
        self.assertEqual(len(self.core.calls), 2)
        self.assertEqual(self.factory.writes, 1)

    def test_pending_predicate_evidence_is_retried_without_persistence(self):
        self.core.statuses = ["PENDING_PREDICATE_EVIDENCE", "PASS"]
        self.cycle()
        self.assertEqual(self.factory.writes, 0)
        self.assertEqual(self.worker.status()["state"], "PENDING_PREDICATE_EVIDENCE")
        self.cycle()
        self.assertEqual(len(self.core.calls), 2)
        self.assertEqual(self.factory.writes, 1)

    def test_each_current_retained_field_invalidates_pass_before_cache_lookup(self):
        mutations = (
            lambda e: e["matches"][0]["decision_features"].update(non_condition="changed"),
            lambda e: e["definitions"][0]["definition"].update(value=2),
            lambda e: e["definitions"][0].update(formula_version="v2"),
            lambda e: e["matches"][0].update(source_direction="LONG"),
            lambda e: e["matches"][0].update(source_symbol="ETH"),
        )
        self.cycle()
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                before = self.worker.status()["dataset_sha256"]
                calls = len(self.core.calls)
                mutate(self.factory.predicate_evidence_value)
                self.cycle()
                self.assertNotEqual(before, self.worker.status()["dataset_sha256"])
                self.assertEqual(len(self.core.calls), calls + 1)

    def test_new_predicate_read_time_and_cutoff_alone_reuse_cached_pass(self):
        self.cycle()
        before = self.worker.status()["dataset_sha256"]
        self.factory.server_time += timedelta(seconds=1)
        self.factory.snapshot["journal_seq"] = 12
        self.factory.snapshot["state_json"]["observation_sequence"] = 4
        self.factory.coverage.update(total_journal_rows=12, max_journal_seq=12,
                                    candidate_packets_total=2, candidate_empty_packets_total=2,
                                    quote_packets_total=2, total_input_packets=4)
        self.cycle()
        self.assertEqual(before, self.worker.status()["dataset_sha256"])
        self.assertEqual(len(self.core.calls), 1)
        self.assertEqual([x[0] for x in self.factory.trace].count("predicate_evidence_read"), 2)

    def test_new_nonempty_source_packet_invalidates_cache_without_core_projection_change(self):
        self.cycle()
        first = self.worker.status()["dataset_sha256"]
        self.factory.coverage.update(candidate_nonempty_packets_total=1, candidate_empty_packets_total=0,
                                     candidate_selected_packets_total=1, candidate_packet_rows=1,
                                     max_candidate_selected_seq=2)
        self.factory.source_packets = [{"seq": 2, "payload_sha256": "new", "input_ordinal": 1,
                                        "rejected_input_count_before": 0, "observation_sequence": 1}]
        self.cycle()
        self.assertNotEqual(first, self.worker.status()["dataset_sha256"])
        self.assertEqual(self.factory.writes, 2)
        self.assertEqual(len(self.core.calls), 2)

    def test_worker_and_source_audit_implementation_hashes_invalidate_cache(self):
        source = self.package / "fixture_core.py"
        source.write_text("core = True\n", encoding="utf8")
        self.core.__file__ = str(source)
        source_audit = self.package / "research_forward_source_audit.py"
        source_audit.write_text("audit_v1 = True\n", encoding="utf8")
        predicate_audit = self.package / "research_forward_predicate_audit.py"
        predicate_audit.write_text("predicate_v1 = True\n", encoding="utf8")
        self.cycle()
        first = self.worker.status()["dataset_sha256"]
        self.worker._discard_owned()
        source_audit.write_text("audit_v2 = True\n", encoding="utf8")
        self.cycle()
        second = self.worker.status()["dataset_sha256"]
        self.assertNotEqual(first, second)
        self.assertIn("source_audit", self.worker.status()["evaluator_source_components"])
        self.assertIn("predicate_audit", self.worker.status()["evaluator_source_components"])
        self.worker._discard_owned()
        predicate_audit.write_text("predicate_v2 = True\n", encoding="utf8")
        self.cycle()
        third = self.worker.status()["dataset_sha256"]
        self.assertNotEqual(second, third)
        self.worker._discard_owned()
        worker_copy = self.package / "fixture_worker.py"
        worker_copy.write_text("changed_worker = True\n", encoding="utf8")
        with patch.object(module, "__file__", str(worker_copy)):
            self.cycle()
        self.assertNotEqual(third, self.worker.status()["dataset_sha256"])

    def test_rejection_coverage_changes_marker_even_when_snapshot_recovers(self):
        self.cycle()
        first = self.worker.status()["dataset_sha256"]
        self.factory.coverage["rejected_input_packets"] = 1
        self.factory.snapshot["state_json"]["observation_sequence"] = 1
        self.core.statuses = ["PENDING_SOURCE_EVIDENCE"]
        self.cycle()
        self.assertNotEqual(first, self.worker.status()["dataset_sha256"])
        self.assertEqual(len(self.core.calls), 2)
        self.assertEqual(self.worker.status()["state"], "PENDING_SOURCE_EVIDENCE")
        self.assertEqual(self.factory.writes, 1)

    def test_source_next_input_bounds_are_in_the_persisted_evidence_identity(self):
        packet = {"seq": 2, "payload_sha256": "source-hash", "input_ordinal": 1,
                  "rejected_input_count_before": 0, "observation_sequence": 1, "next_input_seq": 4}
        self.factory.source_packets = [packet]
        self.factory.coverage.update(candidate_nonempty_packets_total=1, candidate_empty_packets_total=0,
                                     candidate_selected_packets_total=1, candidate_packet_rows=1,
                                     max_candidate_selected_seq=2)
        self.cycle()
        evidence = self.core.calls[0][3]["source_evidence"]
        expected = module._digest({"quotes": [[p.get("seq"), p.get("payload_sha256")] for p in self.factory.packets],
            "source_packets": [[2, "source-hash", 1, 0, 1, 4]], "source_decisions": [],
            "source_counts": evidence["counts"], "source_journal_seq": evidence["source_journal_seq"],
            "predicate_evidence": self.core.calls[0][3]["predicate_evidence"]})
        self.assertEqual(self.factory.metadata[0]["evidence_sha256"], expected)
        self.assertNotEqual(expected, module._digest({"quotes": [[p.get("seq"), p.get("payload_sha256")] for p in self.factory.packets],
            "source_packets": [[2, "source-hash", 1, 0, 1, 9]], "source_decisions": [],
            "source_counts": evidence["counts"], "source_journal_seq": evidence["source_journal_seq"],
            "predicate_evidence": self.core.calls[0][3]["predicate_evidence"]}))

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

    def test_actual_core_contract_pins_retained_evidence_retries_drift_and_recovers(self):
        import research_forward_evaluation as real_core
        from research_forward_source_audit_selftest import PaperStudy, policy, candidate, event, quote, capture, predicate_envelope
        engine = PaperStudy(":memory:", policy(), resume=False)
        try:
            engine.observe_candidates(candidate("bootstrap", 1, [], True))
            engine.observe_candidates(candidate("source", 10, [event()]))
            engine.observe_quotes(quote("context", 50.2))
            engine.observe_quotes(quote("entry", 60.2))
            exit_packet = quote("exit", 70.2)
            exit_packet["quotes"][0].update(bid="99.5", ask="99.5")
            engine.observe_quotes(exit_packet)
            source, evidence, baseline = capture(engine)
            packets = []
            for row in engine.db.execute("SELECT event_id,kind,utc,input_key,payload_json,payload_sha256 FROM journal WHERE kind='INPUT_QUOTES' ORDER BY event_id"):
                record = dict(zip(("seq", "kind", "utc", "input_key", "payload_json", "payload_sha256"), row))
                record["payload_json"] = json.loads(record["payload_json"])
                packets.append(record)
            engine.observe_candidates(candidate("new-source", 80, [event(event_id=2, parent="p2", at=75)]))
            newer_source, newer_evidence, _ = capture(engine)
        finally:
            engine.close()
        self.factory.snapshot = source
        self.factory.server_time = module._time(source["updated_at_utc"]) + timedelta(seconds=1)
        source["last_status"].update(state="OBSERVING", leader=True,
            last_source_received_utc=source["updated_at_utc"], last_quote_received_utc=source["updated_at_utc"])
        self.factory.packets = packets
        self.factory.coverage = evidence["counts"]
        self.factory.source_packets = evidence["packets"]
        self.factory.source_decisions = evidence["decisions"]
        self.factory.predicate_evidence_value = predicate_envelope(source, evidence)
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
        original = copy.deepcopy(self.factory.predicate_evidence_value)
        # Every formula condition still passes; only an unrelated retained map
        # field changes. The successful prior record remains the pin baseline.
        self.factory.predicate_evidence_value["matches"][0]["decision_features"]["test.non_condition"] = 123
        self.cycle(actual)
        self.assertEqual(actual.status()["state"], "AUDIT_FAILED")
        failed = next(reversed(self.factory.rows.values()))["assessment"]
        self.assertEqual([failure["code"] for failure in failed["predicate_audit"]["failures"]], ["RETAINED_RECORD_UNCHANGED"])
        self.assertEqual(self.factory.writes, 2)
        self.cycle(actual)
        self.assertEqual(actual.status()["state"], "AUDIT_FAILED")
        self.assertEqual(self.factory.writes, 2)
        self.assertEqual(actual.status()["assessment"]["predicate_audit_summary"]["previous_pinned_keys"], 1)
        self.factory.predicate_evidence_value = original
        self.cycle(actual)
        self.assertEqual(actual.status()["state"], "WAITING_FOR_NEW_DATA")
        self.assertEqual(self.factory.writes, 2)
        self.assertEqual([name for name, _ in self.factory.trace].count("latest_pass"), 5)
        # A later successful capture adds a positive key. Restoring the older
        # exact input must not erase that newer pin via its existing PASS cache.
        newer_source["last_status"].update(state="OBSERVING", leader=True,
            last_source_received_utc=newer_source["updated_at_utc"], last_quote_received_utc=newer_source["updated_at_utc"])
        self.factory.snapshot = newer_source
        self.factory.coverage = newer_evidence["counts"]
        self.factory.source_packets = newer_evidence["packets"]
        self.factory.source_decisions = newer_evidence["decisions"]
        self.factory.predicate_evidence_value = predicate_envelope(newer_source, newer_evidence)
        self.cycle(actual)
        self.assertEqual(actual.status()["state"], "AUDIT_PASSED")
        self.assertEqual(self.factory.writes, 3)
        self.factory.snapshot = source
        self.factory.coverage = evidence["counts"]
        self.factory.source_packets = evidence["packets"]
        self.factory.source_decisions = evidence["decisions"]
        self.factory.predicate_evidence_value = original
        self.cycle(actual)
        self.assertEqual(actual.status()["state"], "AUDIT_FAILED")
        self.assertTrue(actual.status()["pin_context_identity_applied"])
        self.assertEqual(self.factory.writes, 4)
        failed = next(reversed(self.factory.rows.values()))["assessment"]
        self.assertIn("PREVIOUS_POSITIVE_REMAINS_PRESENT", [f["code"] for f in failed["predicate_audit"]["failures"]])
        self.assertEqual(failed["worker_evaluation_context"]["input_dataset_sha256"],
                         actual.status()["input_dataset_sha256"])
        self.assertIsNotNone(failed["worker_evaluation_context"]["prospective_pin_baseline_sha256"])
        self.cycle(actual)
        self.assertEqual(actual.status()["state"], "AUDIT_FAILED")
        self.assertEqual(self.factory.writes, 4)


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
        elif "PREDICATE_DEFINITIONS" in upper or "PREDICATE_MATCHES" in upper:
            records = conn.definition_rows if "PREDICATE_DEFINITIONS" in upper else conn.match_rows
            if upper.startswith("SELECT COUNT(*)"):
                byte_count = conn.predicate_bytes_override
                if byte_count is None:
                    byte_count = sum(len(module._json(r).encode()) for r in records)
                self.rows = [(len(records), byte_count)]
            else:
                self.rows = [(copy.deepcopy(r),) for r in records]
        elif "'CANDIDATE_PACKETS_TOTAL'" in upper:
            self.rows = [(copy.deepcopy(conn.coverage),)]
        elif "WITH RANKED" in upper:
            self.rows = [(copy.deepcopy(p),) for p in conn.source_packets]
        elif "JSON_BUILD_OBJECT" in upper and "'PARENT_CHOICE_FROZEN'" in upper:
            self.rows = [(copy.deepcopy(p),) for p in conn.source_decisions]
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
        self.coverage = coverage()
        self.source_packets = []
        self.source_decisions = []
        self.definition_rows = []
        self.match_rows = []
        self.predicate_bytes_override = None
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
    def predicate_connection(self):
        conn = FakeConnection()
        conn.snapshot["policy_json"]["scopes"] = [
            {"scope_key": "doge", "candidate_key": "doge-c"},
            {"scope_key": "eth", "candidate_key": "eth-c"}]
        # Includes both an old bootstrap and an incomplete first observation.
        conn.snapshot["state_json"]["first_seen"] = {"doge:1": {"bootstrap": True}, "eth:2": {"incomplete": True}}
        conn.definition_rows = [{"candidate_key": key, "definition_present": True,
            "formula_version": "v1", "definition_sha256": "retained-hash", "definition": {"all": "retained"}}
            for key in ("doge-c", "eth-c")]
        conn.match_rows = [{"scope_key": scope, "candidate_key": scope+"-c", "event_id": event,
            "match_present": True, "source_present": True, "decision_features": {"condition": 2, "extra": 9},
            "source_direction": "SHORT", "source_symbol": scope.upper(), "source_alert_time_utc": BASE.isoformat()}
            for scope,event in (("doge", 1), ("eth", 2))]
        return conn

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

    def test_source_packet_selection_ordinals_decision_bounds_and_counts_share_read_snapshot(self):
        conn = FakeConnection()
        conn.coverage.update(candidate_packets_total=4, candidate_nonempty_packets_total=2,
            candidate_empty_packets_total=2, candidate_bootstrap_packets_total=1,
            candidate_selected_packets_total=3, candidate_packet_rows=4, quote_packets_total=4,
            total_input_packets=8, rejected_input_packets=1, source_decision_rows_total=2,
            max_candidate_selected_seq=9, max_source_decision_seq=10)
        conn.source_packets = [
            {"seq": 2, "input_ordinal": 1, "rejected_input_count_before": 0, "observation_sequence": 1,
             "next_input_seq": 4, "payload_json": {"bootstrap": True, "rows": []}},
            {"seq": 6, "input_ordinal": 3, "rejected_input_count_before": 1, "observation_sequence": 2,
             "next_input_seq": 7, "payload_json": {"rows": [{"x": 1}, {"x": 2}]}},
            {"seq": 9, "input_ordinal": 5, "rejected_input_count_before": 1, "observation_sequence": 4,
             "next_input_seq": None, "payload_json": {"rows": [{"x": 3}, {"x": 4}]}}]
        conn.source_decisions = [{"seq": 3, "kind": "BOOTSTRAP_EXCLUDED"}, {"seq": 5, "kind": "INPUT_REJECTED"}]
        store = module.EvaluationStore("private", connect=lambda _: conn)
        with store.source() as source:
            counts = source.counts()
            evidence = source.source_evidence(counts)
        self.assertEqual(evidence["source_journal_seq"], conn.snapshot["journal_seq"])
        self.assertTrue(evidence["selection_complete"])
        self.assertEqual(evidence["packets"], conn.source_packets)
        self.assertEqual(evidence["decisions"], conn.source_decisions)
        packet_sql, params, cursor_name = next(x for x in conn.statements if "WITH ranked" in x[0])
        self.assertIn("row_number() OVER(ORDER BY seq)", packet_sql)
        self.assertIn("lead(seq) OVER(ORDER BY seq) AS next_input_seq", packet_sql)
        self.assertIn("WHERE kind IN('INPUT_CANDIDATES','INPUT_QUOTES')", packet_sql)
        self.assertLess(packet_sql.index("row_number()"), packet_sql.index("WHERE r.kind='INPUT_CANDIDATES'"))
        self.assertIn("i.input_ordinal-r.rejected_input_count_before", packet_sql)
        self.assertIn("COALESCE((j.payload_json->>'bootstrap')::boolean,false)", packet_sql)
        self.assertEqual(params, (module.STUDY_ID, 10, module.STUDY_ID, module.MAX_SOURCE_PACKETS+1))
        self.assertTrue(cursor_name.startswith("forward_evaluation_"))
        self.assertTrue(conn.rolled_back and conn.closed)
        sqls = [x[0].lstrip().upper() for x in conn.statements]
        self.assertEqual(sqls[0], "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        self.assertFalse(any(x.startswith(("INSERT", "UPDATE", "DELETE")) for x in sqls))

    def test_retained_predicates_left_join_all_first_seen_keys_in_same_read_only_snapshot(self):
        conn = self.predicate_connection()
        store = module.EvaluationStore("private", connect=lambda _: conn)
        with store.source() as source:
            evidence = source.predicate_evidence()
        self.assertEqual(evidence["expected_keys"], ["doge:1", "eth:2"])
        self.assertEqual(evidence["counts"], {"expected_positive_keys": 2, "matched_rows": 2, "definition_rows": 2})
        self.assertEqual(evidence["matches"], conn.match_rows)
        self.assertEqual(evidence["definitions"], conn.definition_rows)
        self.assertEqual(evidence["source_journal_seq"], 10)
        self.assertEqual(evidence["read_utc"], BASE.isoformat())
        self.assertTrue(evidence["extraction_complete"])
        sql = "\n".join(row[0] for row in conn.statements)
        self.assertIn("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY", sql)
        self.assertIn("LEFT JOIN research_ordered_formula_matches", sql)
        self.assertIn("LEFT JOIN research_events", sql)
        self.assertIn("LEFT JOIN research_ordered_formula_candidates", sql)
        for field in ("m.decision_features", "e.direction AS source_direction", "e.symbol AS source_symbol",
                      "e.alert_time_utc AS source_alert_time_utc", "c.formula_version,c.definition_sha256,c.definition"):
            self.assertIn(field, sql)
        self.assertIn("octet_length(row_to_json(p)::text)", sql)
        self.assertNotIn("feature_screens", sql)
        self.assertTrue(conn.rolled_back and conn.closed)
        self.assertFalse(any(s.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for s, _, _ in conn.statements))

    def test_missing_retained_match_and_definition_are_preserved_as_explicit_left_rows(self):
        conn = self.predicate_connection()
        conn.match_rows[0] = {"scope_key": "doge", "candidate_key": "doge-c", "event_id": 1,
                              "match_present": False, "source_present": False, "decision_features": None}
        conn.definition_rows[0] = {"candidate_key": "doge-c", "definition_present": False,
                                   "formula_version": None, "definition_sha256": None, "definition": None}
        store = module.EvaluationStore("private", connect=lambda _: conn)
        with store.source() as source:
            evidence = source.predicate_evidence()
        self.assertEqual(len(evidence["matches"]), 2)
        self.assertEqual(len(evidence["definitions"]), 2)
        self.assertEqual(evidence["counts"]["matched_rows"], 1)
        self.assertEqual(evidence["counts"]["definition_rows"], 1)
        self.assertIs(evidence["matches"][0]["match_present"], False)

    def test_predicate_row_limit_is_checked_before_any_wide_data_query(self):
        conn = self.predicate_connection()
        conn.snapshot["state_json"]["first_seen"] = {"doge:"+str(i): {} for i in range(module.MAX_PREDICATE_ROWS)}
        store = module.EvaluationStore("private", connect=lambda _: conn)
        with self.assertRaises(module.EvidenceLimitExceeded):
            with store.source() as source:
                source.predicate_evidence()
        self.assertFalse(any("PREDICATE_" in s for s, _, _ in conn.statements))

    def test_predicate_byte_budget_fails_during_server_preflight_before_wide_fetch(self):
        conn = self.predicate_connection()
        conn.predicate_bytes_override = module.MAX_PREDICATE_BYTES + 1
        store = module.EvaluationStore("private", connect=lambda _: conn)
        with self.assertRaises(module.EvidenceLimitExceeded):
            with store.source() as source:
                source.predicate_evidence()
        self.assertTrue(any("PREDICATE_DEFINITIONS" in s for s, _, _ in conn.statements))
        self.assertFalse(any(s.startswith("SELECT row_to_json(p)") for s, _, _ in conn.statements))
        self.assertTrue(conn.closed and conn.rolled_back)

    def test_duplicate_retained_rows_exceed_exact_requested_bound(self):
        conn = self.predicate_connection()
        conn.match_rows.append(copy.deepcopy(conn.match_rows[0]))
        store = module.EvaluationStore("private", connect=lambda _: conn)
        with self.assertRaises(module.EvidenceLimitExceeded):
            with store.source() as source:
                source.predicate_evidence()
        self.assertFalse(any(s.startswith("SELECT row_to_json(p)") and "PREDICATE_MATCHES" in s
                             for s, _, _ in conn.statements))

    def test_predicate_fetch_shares_existing_evidence_deadline(self):
        conn = self.predicate_connection()
        store = module.EvaluationStore("private", connect=lambda _: conn)
        with self.assertRaises(module.EvidenceLimitExceeded):
            with store.source() as source:
                source._evidence_started = time.monotonic() - module.MAX_EVIDENCE_SECONDS - 1
                source.predicate_evidence()
        self.assertFalse(any("PREDICATE_" in s for s, _, _ in conn.statements))

    def test_unknown_or_noncanonical_raw_positive_id_never_gets_silently_dropped(self):
        for key in ("unknown:1", "doge:01", "doge:-1", "doge:"+str(1 << 63)):
            with self.subTest(key=key):
                conn = self.predicate_connection()
                conn.snapshot["state_json"]["first_seen"] = {key: {}}
                store = module.EvaluationStore("private", connect=lambda _: conn)
                with self.assertRaises(module.EvaluationStorageError):
                    with store.source() as source:
                        source.predicate_evidence()
                self.assertFalse(any("PREDICATE_" in s for s, _, _ in conn.statements))

    def test_source_selection_count_mismatch_never_claims_complete_evidence(self):
        conn = FakeConnection()
        conn.coverage.update(candidate_selected_packets_total=1)
        store = module.EvaluationStore("private", connect=lambda _: conn)
        with self.assertRaises(module.EvaluationStorageError):
            with store.source() as source:
                source.source_evidence()
        self.assertTrue(conn.closed and conn.rolled_back)

    def test_source_packet_row_and_decision_caps_fail_before_partial_fetch(self):
        for key, value in (("candidate_selected_packets_total", module.MAX_SOURCE_PACKETS+1),
                           ("candidate_packet_rows", module.MAX_SOURCE_ROWS+1),
                           ("source_decision_rows_total", module.MAX_SOURCE_DECISIONS+1)):
            with self.subTest(key=key):
                conn = FakeConnection(); conn.coverage[key] = value
                store = module.EvaluationStore("private", connect=lambda _: conn)
                with self.assertRaises(module.EvidenceLimitExceeded):
                    with store.source() as source:
                        source.source_evidence()
                self.assertFalse(any("WITH ranked" in x[0] for x in conn.statements))
                self.assertTrue(conn.closed and conn.rolled_back)

    def test_malformed_candidate_rows_are_not_excluded_and_cached_as_success(self):
        conn = FakeConnection(); conn.coverage["candidate_invalid_rows_packets_total"] = 1
        store = module.EvaluationStore("private", connect=lambda _: conn)
        with self.assertRaises(module.EvaluationStorageError):
            with store.source() as source:
                source.counts()
        self.assertTrue(conn.closed and conn.rolled_back)

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

    def test_latest_successful_pin_requires_verified_pass_version_and_policy_without_code_filter(self):
        conn = FakeConnection()
        store = module.EvaluationStore("private", connect=lambda _: conn)
        store.acquire()
        result = {"status": "PASS", "evaluator_version": "forward-evaluation-v3", "policy_sha256": "frozen"}
        conn.previous_rows = [(result, module._digest(result))]
        self.assertEqual(store.latest_pass("forward-evaluation-v3", "frozen"), result)
        sql, parameters, _ = conn.statements[-1]
        self.assertIn("audit_status='PASS'", sql)
        self.assertIn("policy_sha256=%s", sql)
        self.assertNotIn("evaluator_source_sha256", sql)
        self.assertEqual(parameters, (module.STUDY_ID, "forward-evaluation-v3", "frozen"))
        for key, value in (("status", "FAIL"), ("evaluator_version", "old"), ("policy_sha256", "other")):
            with self.subTest(key=key):
                altered = {**result, key: value}
                conn.previous_rows = [(altered, module._digest(altered))]
                with self.assertRaises(module.EvaluationStorageError):
                    store.latest_pass("forward-evaluation-v3", "frozen")
        conn.previous_rows = [(result, "corrupted")]
        with self.assertRaises(module.EvaluationStorageError):
            store.latest_pass("forward-evaluation-v3", "frozen")
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
