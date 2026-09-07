"""Behavioral archive transfer checks; emulated SQL is not production proof."""
from copy import deepcopy
from datetime import timedelta
import json
from pathlib import Path
import re
import sqlite3
from tempfile import TemporaryDirectory
import unittest

import research_telegram_archive_runtime_importer as importer
from research_telegram_archive_backfill import initialize, canonical, calculate_event
from research_telegram_archive_reconstruction_selftest import source, MANIFEST, ENTRY, cache
from research_telegram_archive_features import extract_message


class Cursor:
    def __init__(self, row=None, count=0):
        self.row, self.rowcount = row, count
    def fetchone(self):
        return self.row


class ArchivePGFixture:
    def __init__(self):
        self.tables = {name: {} for name in importer.TABLES}
        self.saved = deepcopy(self.tables)
        self.fail_once = False
        self.commits = 0
    def execute(self, sql, args=()):
        if "to_regclass" in sql:
            return Cursor({"present": args[0] in self.tables})
        if sql.startswith("INSERT INTO "):
            table = re.match(r"INSERT INTO (\w+)", sql)[1]
            if self.fail_once and table == "research_archive_common_window_metrics":
                self.fail_once = False
                raise RuntimeError("simulated connection interruption")
            count = 0
            for record in json.loads(args[0]):
                key = tuple(record[field] for field in importer.KEYS[table])
                if key not in self.tables[table]:
                    self.tables[table][key] = record
                    count += 1
            return Cursor(count=count)
        if sql.startswith("SELECT EXISTS"):
            table = re.search(r"LEFT JOIN (\w+) existing", sql)[1]
            conflict = False
            for record in json.loads(args[0]):
                key = tuple(record[field] for field in importer.KEYS[table])
                conflict |= self.tables[table].get(key) != record
            return Cursor({"conflict": conflict})
        raise AssertionError("Unexpected destination SQL: " + sql)
    def commit(self):
        self.saved = deepcopy(self.tables)
        self.commits += 1
    def rollback(self):
        self.tables = deepcopy(self.saved)


def artifact(path):
    contract = {"backfill_version": importer.BACKFILL_VERSION, "prepared_stage_digest": "a" * 64,
        "entry_policy_version": importer.ENTRY_VERSION, "feature_version": importer.FEATURE_VERSION,
        "direction_version": importer.DIRECTION_VERSION, "time_version": importer.TIME_VERSION,
        "parent_policy_version": importer.parent_policy.POLICY_VERSION, "source_scope": "ARCHIVE_ONLY",
        "threshold_bps": list(range(25,201,25)), "window_minutes": list(importer.WINDOWS),
        "variants": ["NORMAL", "INVERSE"], "cache_sha256": "b" * 64,
        "live_union_eligible": False, "phase": "DISCOVERY", "formula_relevance": "NOT_EVALUATED"}
    run_key = importer._hash(contract)
    event = extract_message(source("#1 BTC / 24h | 🔴 SHORT | 78\nמחיר נוכחי: $88"), MANIFEST)
    event, outcomes, metrics = calculate_event(event, cache(), [], observed_at=ENTRY+timedelta(days=2))
    key = event["archive_event_key"]
    with sqlite3.connect(path) as conn:
        initialize(conn)
        conn.execute("INSERT INTO archive_reconstruction_runs VALUES (?,?)", (run_key, canonical(contract)))
        conn.execute("INSERT INTO archive_reconstructed_events VALUES (?,?,?,?,?,?,?)", (run_key,key,event["source_message_time_utc"],event["symbol"],event["reconstruction_status"],event["calculation_status"],canonical(event)))
        conn.executemany("INSERT INTO archive_delayed_entry_outcomes VALUES (?,?,?,?,?,?,?,?)", [(run_key,key,row["signal_variant"],row["window_minutes"],row["threshold_bps"],row["outcome_id"],row["status"],canonical(row)) for row in outcomes])
        conn.executemany("INSERT INTO archive_common_window_metrics VALUES (?,?,?,?,?,?)", [(run_key,key,row["signal_variant"],row["window_minutes"],row["status"],canonical(row)) for row in metrics])
    return run_key


class RuntimeImporterTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.path = Path(self.temp.name) / "archive.sqlite"
        self.run_key = artifact(self.path)
        self.sha = importer.file_sha256(self.path)
        self.pg = ArchivePGFixture()
    def tearDown(self):
        self.temp.cleanup()
    def transfer(self):
        return importer.import_artifact(self.pg,self.path,expected_sha256=self.sha,expected_run_key=self.run_key,batch_size=1)
    def test_all_exact_cells_import_once_and_rerun_zero_new_rows(self):
        report = self.transfer()
        self.assertEqual(report["verified_events"], 1)
        self.assertEqual(report["inserted_by_table"]["research_archive_delayed_entry_outcomes"], 64)
        self.assertEqual(report["inserted_by_table"]["research_archive_common_window_metrics"], 8)
        self.assertEqual(report["live_events_inserted"], 0)
        self.assertTrue(all(count == 0 for count in self.transfer()["inserted_by_table"].values()))
    def test_interrupted_event_transaction_does_not_leave_ready_event_without_labels(self):
        self.pg.fail_once = True
        with self.assertRaises(RuntimeError):
            self.transfer()
        self.assertEqual(self.pg.tables["research_archive_reconstructed_events"], {})
        self.assertEqual(self.pg.tables["research_archive_delayed_entry_outcomes"], {})
        report = self.transfer()
        self.assertEqual(report["verified_events"], 1)
        self.assertEqual(len(self.pg.tables["research_archive_delayed_entry_outcomes"]), 64)
    def test_existing_conflicting_source_is_preserved_and_not_overwritten(self):
        self.transfer()
        key = next(iter(self.pg.tables["research_archive_reconstructed_events"]))
        self.pg.tables["research_archive_reconstructed_events"][key]["event_payload"]["original_printed_price"] = 999
        self.pg.commit()
        with self.assertRaises(ValueError):
            self.transfer()
        self.assertEqual(self.pg.tables["research_archive_reconstructed_events"][key]["event_payload"]["original_printed_price"], 999)
    def test_wrong_digest_or_active_writer_is_rejected_before_database_writes(self):
        with self.assertRaises(ValueError):
            importer.import_artifact(self.pg,self.path,expected_sha256="0"*64,expected_run_key=self.run_key)
        self.assertEqual(self.pg.commits, 0)
        Path(str(self.path)+"-wal").write_bytes(b"active")
        with self.assertRaises(ValueError):
            self.transfer()
        self.assertEqual(self.pg.commits, 0)
    def test_other_method_or_live_scope_is_rejected(self):
        with sqlite3.connect(self.path) as conn:
            key, encoded = conn.execute("SELECT outcome_id,outcome_json FROM archive_delayed_entry_outcomes LIMIT 1").fetchone()
            payload = json.loads(encoded)
            payload["method_version"] = "first-touch-v6"
            conn.execute("UPDATE archive_delayed_entry_outcomes SET outcome_json=? WHERE outcome_id=?", (canonical(payload), key))
        self.sha = importer.file_sha256(self.path)
        with self.assertRaises(ValueError):
            self.transfer()
        self.assertEqual(self.pg.tables["research_archive_reconstructed_events"], {})
    def test_complete_claim_requires_all_threshold_horizon_variant_cells(self):
        with sqlite3.connect(self.path) as conn:
            conn.execute("DELETE FROM archive_delayed_entry_outcomes WHERE rowid=(SELECT MIN(rowid) FROM archive_delayed_entry_outcomes)")
        self.sha = importer.file_sha256(self.path)
        with self.assertRaises(ValueError):
            self.transfer()
        self.assertEqual(self.pg.tables["research_archive_reconstructed_events"], {})


if __name__ == "__main__":
    unittest.main()
