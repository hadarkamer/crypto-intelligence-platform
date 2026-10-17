"""Real SQLite lease/fencing, restart and replay-parity checks."""
from __future__ import annotations

import copy
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest

import research_no_horizon_replay as replay
from research_no_horizon_replay_selftest import fixture
from research_no_horizon_store import LeaseLost, LocalResearchStore


class StoreChecks(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "research.sqlite"

    def tearDown(self):
        self.directory.cleanup()

    def finish(self, store, job_id, **kwargs):
        for _ in range(2000):
            if store.get_job(job_id)["status"] in ("COMPLETE", "BLOCKED"):
                return store.get_receipt(job_id)
            store.run_once("worker", job_id=job_id, **kwargs)
        self.fail("bounded job did not converge")

    def test_restart_budget_resume_matches_uninterrupted_replay(self):
        source = fixture()
        for bar in source["candles"]:
            bar.update(high=100.2, low=99.8)
        source["candles"][75]["high"] = 101.5
        expected = replay.replay_snapshot(source, batch_size=13)
        with LocalResearchStore(self.path) as store:
            job = store.submit_snapshot(source)
            for _ in range(3):
                status = store.run_once("before-restart", job_id=job, candle_budget=7, entry_budget=2, batch_size=3)
                self.assertEqual(status["status"], "PENDING")
                self.assertIsNone(store.get_receipt(job))
            evaluations = status["candle_evaluations"]
        with LocalResearchStore(self.path) as store:
            self.assertEqual(store.get_job(job)["candle_evaluations"], evaluations)
            result = self.finish(store, job, candle_budget=11, entry_budget=2, batch_size=4)
            for field in ("snapshot_sha256", "outcomes", "gate", "status_counts", "candle_evaluations",
                          "incomplete_entry_ids", "computation_complete", "cutoff_utc"):
                self.assertEqual(result[field], expected[field], field)
            self.assertTrue(result["gate"]["experimental_eligible"])
            self.assertIsNone(store.claim_job("other", job_id=job))

    def test_submit_idempotency_key_conflict_and_new_cutoff_receipt(self):
        source = fixture()
        with LocalResearchStore(self.path) as store:
            job = store.submit_snapshot(source, job_key="frozen-cohort")
            self.assertEqual(store.submit_snapshot(copy.deepcopy(source), job_key="frozen-cohort"), job)
            original = self.finish(store, job)
            altered = copy.deepcopy(source)
            altered["cutoff_utc"] = "2026-01-01T01:39:00+00:00"
            with self.assertRaisesRegex(ValueError, "job_key"):
                store.submit_snapshot(altered, job_key="frozen-cohort")
            revision = store.submit_snapshot(altered, job_key="earlier-cutoff")
            self.assertNotEqual(revision, job)
            second = self.finish(store, revision)
            self.assertNotEqual(second["receipt_sha256"], original["receipt_sha256"])
            self.assertEqual(store.get_receipt(job), original)
            with self.assertRaises(sqlite3.IntegrityError):
                store.connection.execute("UPDATE local_receipts SET payload_json='{}' WHERE job_id=?", (job,))

    def test_atomic_concurrent_claim_has_one_owner(self):
        with LocalResearchStore(self.path) as store:
            job = store.submit_snapshot(fixture())
        barrier = threading.Barrier(2)

        def claimant(name):
            with LocalResearchStore(self.path) as store:
                barrier.wait(timeout=5)
                return store.claim_job(name, job_id=job)

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(claimant, name) for name in ("one", "two")]
            claims = [future.result(timeout=10) for future in futures]
        self.assertEqual(sum(claim is not None for claim in claims), 1)

    def test_old_implementation_job_cannot_starve_current_dispatch(self):
        with LocalResearchStore(self.path) as store:
            current_versions = dict(store.versions)
            store.versions["implementation_sha256"] = "0" * 64
            old_job = store.submit_snapshot(fixture(), job_key="old-implementation")
            store.versions = current_versions
            new_job = store.submit_snapshot(fixture(), job_key="current-implementation")
            with self.assertRaisesRegex(ValueError, "frozen engine implementation"):
                store.claim_job("explicit-old", job_id=old_job)
            old_status = store.get_job(old_job)
            self.assertEqual(old_status["status"], "PENDING")
            self.assertEqual(old_status["fencing_token"], 0)
            self.assertIsNone(old_status["worker_id"])
            claim = store.claim_job("dispatch")
            self.assertEqual(claim["job_id"], new_job)
            self.assertEqual(store.process_claim(claim)["status"], "COMPLETE")
            self.assertIsNone(store.claim_job("dispatch-again"))

    def test_unknown_schema_version_rejected_without_mutating_database(self):
        with sqlite3.connect(self.path) as connection:
            connection.execute("CREATE TABLE local_store_version(singleton INTEGER PRIMARY KEY,version TEXT)")
            connection.execute("INSERT INTO local_store_version VALUES(1,'future-version')")
        before = self.path.read_bytes()
        with self.assertRaisesRegex(ValueError, "unsupported local store schema version"):
            LocalResearchStore(self.path)
        self.assertEqual(self.path.read_bytes(), before)
        with sqlite3.connect(self.path) as connection:
            tables = connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            self.assertEqual(tables, [("local_store_version",)])

    def test_expired_lease_reclaim_fences_old_owner_and_crash_recovers(self):
        with LocalResearchStore(self.path) as first, LocalResearchStore(self.path) as second:
            job = first.submit_snapshot(fixture())
            old = first.claim_job("crashed", job_id=job)
            self.assertIsNone(second.claim_job("waiting", job_id=job))
            # Simulate elapsed wall clock without sleeping or caller-controlled clock APIs.
            first.connection.execute("UPDATE local_jobs SET lease_until=0 WHERE job_id=?", (job,))
            new = second.claim_job("replacement", job_id=job)
            self.assertGreater(new["fencing_token"], old["fencing_token"])
            with self.assertRaises(LeaseLost):
                first.process_claim(old)
            self.assertEqual(second.process_claim(new)["status"], "COMPLETE")
            with self.assertRaises(LeaseLost):
                second.process_claim(new)
            self.assertEqual(first.get_receipt(job), second.get_receipt(job))

    def test_expiry_during_computation_rolls_back_checkpoint(self):
        from unittest.mock import patch
        import research_no_horizon_first_touch as engine
        with LocalResearchStore(self.path) as store:
            job = store.submit_snapshot(fixture())
            claim = store.claim_job("expires", job_id=job)
            real_advance = engine.advance

            def expire(*args, **kwargs):
                result = real_advance(*args, **kwargs)
                store.connection.execute("UPDATE local_jobs SET lease_until=0 WHERE job_id=?", (job,))
                return result

            with patch.object(engine, "advance", side_effect=expire):
                with self.assertRaises(LeaseLost):
                    store.process_claim(claim)
            self.assertEqual(store.get_job(job)["candle_evaluations"], 0)
            self.assertEqual(store.get_job(job)["next_ordinal"], 0)
            self.assertIsNone(store.get_receipt(job))
            self.assertTrue(self.finish(store, job)["gate"]["experimental_eligible"])

    def test_missing_source_or_parent_never_qualifies(self):
        for missing in ("parent", "source", "tail", "gap"):
            source = fixture()
            if missing == "parent":
                del source["opportunities"][0]["btc_parent_movement_id"]
            elif missing == "source":
                source["source_coverage_complete"] = False
            else:
                for bar in source["candles"]:
                    bar.update(high=100.2, low=99.8)
                if missing == "tail":
                    source["candles"] = source["candles"][:60]
                else:
                    del source["candles"][2]
            with LocalResearchStore(self.path) as store:
                job = store.submit_snapshot(source, job_key=missing)
                result = self.finish(store, job, candle_budget=7)
                expected = replay.replay_snapshot(source)
                self.assertFalse(result["gate"]["experimental_eligible"])
                self.assertEqual(result["outcomes"], expected["outcomes"])
                self.assertEqual(result["gate"], expected["gate"])
                self.assertFalse(result["trading_authorized"])

    def test_invalid_input_leaves_no_job_or_snapshot(self):
        sources = []
        duplicated = fixture()
        duplicated["opportunities"].append(copy.deepcopy(duplicated["opportunities"][0]))
        sources.append(duplicated)
        bad_policy = fixture()
        bad_policy["gate_policy"] = {"minimum_waves": 3}
        sources.append(bad_policy)
        bad_route = fixture()
        bad_route["source_route"] = {"exchange": "UNKNOWN"}
        sources.append(bad_route)
        nonfinite = fixture()
        nonfinite["candles"][0]["open"] = float("nan")
        sources.append(nonfinite)
        malformed = fixture()
        malformed["candles"][0]["high"] = 90
        sources.append(malformed)
        with LocalResearchStore(self.path) as store:
            for source in sources:
                with self.assertRaises((ValueError, TypeError)):
                    store.submit_snapshot(source)
            self.assertEqual(store.connection.execute("SELECT count(*) FROM local_jobs").fetchone()[0], 0)
            self.assertEqual(store.connection.execute("SELECT count(*) FROM local_snapshots").fetchone()[0], 0)

    def test_oversize_input_and_invalid_budget_fail_before_work(self):
        from unittest.mock import patch
        with LocalResearchStore(self.path) as store:
            with patch.object(replay, "MAX_INPUT_BYTES", 100):
                with self.assertRaisesRegex(ValueError, "bounded input size"):
                    store.submit_snapshot(fixture())
            self.assertEqual(store.connection.execute("SELECT count(*) FROM local_jobs").fetchone()[0], 0)
            job = store.submit_snapshot(fixture())
            with self.assertRaises(ValueError):
                store.run_once("bad-budget", job_id=job, candle_budget=0)
            self.assertEqual(store.get_job(job)["status"], "PENDING")


if __name__ == "__main__":
    unittest.main()
