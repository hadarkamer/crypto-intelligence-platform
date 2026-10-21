"""Fake-PG fault injection plus real SQLite-core recovery. No external writes."""
import copy
import json
import unittest
from unittest.mock import Mock, patch
from types import SimpleNamespace
from datetime import datetime, timedelta
from pathlib import Path

from research_forward_paper_engine import PaperStudy
from research_forward_paper_store import CheckpointError, ConfigurationDrift, PostgresCheckpoint, _default_connect


class FakeDatabase:
    def __init__(self):
        self.studies = {}
        self.journal = []
        self.locks = {}
        self.connections = []
        self.fail_journal_insert = False
        self.fail_commit_ack = False

    def connect(self, _dsn):
        connection = FakeConnection(self, len(self.connections) + 1)
        self.connections.append(connection)
        return connection


class FakeConnection:
    def __init__(self, database, pid):
        self.database = database
        self.pid = pid
        self.closed = False
        self.autocommit = False
        self.snapshot = None
        self.statements = []

    def cursor(self):
        if self.closed:
            raise RuntimeError("simulated private connection string must not escape")
        return FakeCursor(self)

    def close(self):
        if self.snapshot is not None:
            self.database.studies, self.database.journal = self.snapshot
            self.snapshot = None
        self.closed = True
        self.database.locks = {key: owner for key, owner in self.database.locks.items() if owner != self.pid}


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []
        self.rowcount = -1

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def close(self):
        pass

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def fetchmany(self, count):
        answer, self.rows = self.rows[:count], self.rows[count:]
        return answer

    def execute(self, statement, parameters=()):
        conn, db = self.connection, self.connection.database
        if conn.closed:
            raise RuntimeError("simulated lost session with private DSN")
        sql = " ".join(statement.split()).upper()
        conn.statements.append(sql)
        self.rows, self.rowcount = [], -1
        if sql == "BEGIN":
            if conn.snapshot is not None:
                raise RuntimeError("Nested transaction")
            conn.snapshot = copy.deepcopy((db.studies, db.journal))
        elif sql == "COMMIT":
            conn.snapshot = None
            if db.fail_commit_ack:
                db.fail_commit_ack = False
                conn.close()
                raise OSError("commit persisted but acknowledgement lost: private DSN")
        elif sql == "ROLLBACK":
            if conn.snapshot is not None:
                db.studies, db.journal = conn.snapshot
                conn.snapshot = None
        elif sql.startswith("SELECT PG_TRY_ADVISORY_LOCK"):
            key = parameters[0]
            acquired = key not in db.locks or db.locks[key] == conn.pid
            if acquired:
                db.locks[key] = conn.pid
            self.rows = [(acquired, conn.pid)]
        elif sql.startswith("SELECT PG_ADVISORY_UNLOCK"):
            self.rows = [(db.locks.pop(parameters[0], None) == conn.pid,)]
        elif sql == "SELECT PG_BACKEND_PID()":
            self.rows = [(conn.pid,)]
        elif sql.startswith("CREATE TABLE IF NOT EXISTS"):
            pass
        elif sql.startswith("SET "):
            pass
        elif sql.startswith("SELECT CONFIG_SHA256,POLICY_SHA256,POLICY_JSON,STATE_JSON,JOURNAL_SEQ,LAST_STATUS"):
            row = db.studies.get(parameters[0])
            if row:
                self.rows = [(row["config_sha256"], row["policy_sha256"], copy.deepcopy(row["policy_json"]),
                              copy.deepcopy(row["state_json"]), row["journal_seq"], copy.deepcopy(row["last_status"]))]
        elif sql.startswith("INSERT INTO RESEARCH_FORWARD_PAPER_STUDIES"):
            identifier, config_sha, policy_sha, policy_text, status_text = parameters
            if identifier in db.studies:
                raise RuntimeError("duplicate study")
            db.studies[identifier] = {"config_sha256": config_sha, "policy_sha256": policy_sha,
                                      "policy_json": json.loads(policy_text), "state_json": None,
                                      "journal_seq": 0, "last_status": json.loads(status_text)}
            self.rowcount = 1
        elif sql.startswith("SELECT INPUT_KEY,PAYLOAD_SHA256 FROM RESEARCH_FORWARD_PAPER_JOURNAL"):
            self.rows = [(row["input_key"], row["payload_sha256"]) for row in sorted(db.journal, key=lambda r: r["seq"])
                         if row["study_id"] == parameters[0] and row["input_key"] is not None]
        elif sql.startswith("SELECT JOURNAL_SEQ,CONFIG_SHA256,POLICY_SHA256"):
            row = db.studies.get(parameters[0])
            if row:
                self.rows = [(row["journal_seq"], row["config_sha256"], row["policy_sha256"])]
        elif sql.startswith("INSERT INTO RESEARCH_FORWARD_PAPER_JOURNAL"):
            if db.fail_journal_insert:
                raise OSError("injected journal write failure: private DSN")
            identifier, sequence, kind, utc, input_key, payload, payload_sha = parameters
            if any(row["study_id"] == identifier and (row["seq"] == sequence or (input_key is not None and row["input_key"] == input_key)) for row in db.journal):
                raise RuntimeError("duplicate durable event identity")
            db.journal.append({"study_id": identifier, "seq": sequence, "kind": kind,
                               "utc": utc, "input_key": input_key, "payload_json": json.loads(payload),
                               "payload_sha256": payload_sha})
            self.rowcount = 1
        elif sql.startswith("UPDATE RESEARCH_FORWARD_PAPER_STUDIES SET STATE_JSON"):
            state, sequence, status, identifier, expected = parameters
            row = db.studies.get(identifier)
            self.rowcount = 0
            if row and row["journal_seq"] == expected:
                row.update(state_json=json.loads(state), journal_seq=sequence, last_status=json.loads(status))
                self.rowcount = 1
        elif sql.startswith("UPDATE RESEARCH_FORWARD_PAPER_STUDIES SET LAST_STATUS"):
            status, identifier, expected, config_sha = parameters
            row = db.studies.get(identifier)
            self.rowcount = 0
            if row and row["journal_seq"] == expected and row["config_sha256"] == config_sha:
                row["last_status"] = json.loads(status)
                self.rowcount = 1
        else:
            raise AssertionError("Unimplemented fake statement: " + sql)


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.database = FakeDatabase()
        self.config = json.loads((Path(__file__).parent / "forward_paper_runtime" / "policy_template.json").read_text())
        self.config.pop("study_start_utc", None)
        self.store = self.new_store()
        self.assertTrue(self.store.acquire())
        self.engine = self.store.restore_engine(PaperStudy)
        self.start = datetime.fromisoformat(self.engine.policy["study_start_utc"])
        self.addCleanup(self.cleanup)

    def cleanup(self):
        try:
            self.engine.close()
        except Exception:
            pass
        self.store.close()

    def new_store(self, config=None):
        return PostgresCheckpoint("private-DSN-must-never-escape", "test-study", self.config if config is None else config,
                                  connect=self.database.connect)

    def stamp(self, seconds):
        return (self.start + timedelta(seconds=seconds)).isoformat()

    def bootstrap(self):
        packet = {"poll_id": "bootstrap-identity", "request_started_utc": self.stamp(0.01),
                  "response_received_utc": self.stamp(0.1), "rows": [], "bootstrap": True, "source": "psycopg"}
        self.engine.observe_candidates(packet)
        return packet

    def checkpoint(self):
        self.store.checkpoint(self.engine, self.engine.summary())

    def restart(self):
        self.engine.close()
        self.store.close()
        self.store = self.new_store()
        self.assertTrue(self.store.acquire())
        self.engine = self.store.restore_engine(PaperStudy)

    def test_competing_session_gets_no_write_authority(self):
        follower = self.new_store()
        self.assertFalse(follower.acquire())
        self.assertFalse(any(sql.startswith("CREATE") for sql in self.database.connections[-1].statements))
        with self.assertRaises(CheckpointError):
            follower.initialize_or_load()
        follower.close()

    def test_freezes_actual_start_once_and_rejects_configuration_drift(self):
        frozen = self.engine.policy["study_start_utc"]
        self.bootstrap()
        self.checkpoint()
        self.restart()
        self.assertEqual(self.engine.policy["study_start_utc"], frozen)
        self.engine.close()
        self.store.close()
        changed = copy.deepcopy(self.config)
        changed["net_cost_pp"] = 0.11
        changed_store = self.new_store(changed)
        self.assertTrue(changed_store.acquire())
        with self.assertRaises(ConfigurationDrift):
            changed_store.initialize_or_load()
        self.assertFalse(changed_store._leader)
        changed_store.close()

    def test_state_and_all_journal_events_commit_atomically(self):
        self.bootstrap()
        self.checkpoint()
        durable = self.database.studies["test-study"]
        self.assertTrue(durable["state_json"]["bootstrap_complete"])
        self.assertEqual(durable["state_json"], self.engine._load())
        self.assertEqual(durable["journal_seq"], len(self.database.journal))
        self.assertEqual([r["seq"] for r in self.database.journal], list(range(1, durable["journal_seq"] + 1)))
        self.assertEqual(durable["last_status"]["durable_journal_seq"], durable["journal_seq"])

    def test_partial_write_failure_rolls_back_state_and_journal_and_poison_writer(self):
        self.bootstrap()
        self.checkpoint()
        before = copy.deepcopy((self.database.studies, self.database.journal))
        self.engine.observe_quotes({"packet_id": "new", "request_started_utc": self.stamp(1),
                                    "response_received_utc": self.stamp(1.1), "quotes": [],
                                    "source": "BINANCE_SPOT_BOOK_TICKER"})
        self.database.fail_journal_insert = True
        with self.assertRaises(CheckpointError) as failure:
            self.checkpoint()
        self.assertNotIn("private", str(failure.exception))
        self.assertEqual((self.database.studies, self.database.journal), before)
        self.assertFalse(self.store._leader)
        with self.assertRaises(CheckpointError):
            self.checkpoint()
        self.database.fail_journal_insert = False
        self.restart()
        self.assertEqual(self.engine._load()["last_response_utc"], before[0]["test-study"]["state_json"]["last_response_utc"])

    def test_leader_connection_loss_stops_writes_and_no_automatic_reconnect(self):
        self.bootstrap()
        self.checkpoint()
        count = len(self.database.connections)
        self.store._conn.close()
        with self.assertRaises(CheckpointError):
            self.store.update_health({"state": "must-not-write"})
        self.assertEqual(len(self.database.connections), count)
        self.assertFalse(self.store._leader)

    def test_cross_restart_input_dedupe_and_metadata_not_rearchived(self):
        packet = self.bootstrap()
        self.checkpoint()
        before = len(self.database.journal)
        self.restart()
        summary = self.engine.observe_candidates(packet)
        self.assertTrue(summary["idempotent_duplicate"])
        self.checkpoint()
        self.assertEqual(sum(r["input_key"] == "CANDIDATES:bootstrap-identity" for r in self.database.journal), 1)
        self.assertEqual(len(self.database.journal), before + 1)  # actual PROCESS_RESTART only
        self.assertFalse(any(r["kind"] == "RESTORED_INPUT_IDENTITY_METADATA" for r in self.database.journal))

    def test_commit_ack_loss_restores_committed_state_without_duplicate_events(self):
        packet = self.bootstrap()
        self.database.fail_commit_ack = True
        with self.assertRaises(CheckpointError):
            self.checkpoint()
        self.assertTrue(self.database.studies["test-study"]["state_json"]["bootstrap_complete"])
        old_count = len(self.database.journal)
        self.restart()
        self.assertTrue(self.engine.observe_candidates(packet)["idempotent_duplicate"])
        self.checkpoint()
        self.assertEqual(len(self.database.journal), old_count + 1)

    def test_health_update_never_advances_uncheckpointed_state_or_journal(self):
        self.bootstrap()
        self.checkpoint()
        before_state = copy.deepcopy(self.database.studies["test-study"]["state_json"])
        before_journal = copy.deepcopy(self.database.journal)
        self.engine.observe_quotes({"packet_id": "not-committed", "request_started_utc": self.stamp(1),
                                    "response_received_utc": self.stamp(1.1), "quotes": [], "source": "BINANCE_SPOT_BOOK_TICKER"})
        self.store.update_health({"state": "TRANSIENT_SOURCE_ERROR", "error_code": "SAFE_CLASS_ONLY"})
        self.assertEqual(self.database.studies["test-study"]["state_json"], before_state)
        self.assertEqual(self.database.journal, before_journal)
        self.assertEqual(self.database.studies["test-study"]["last_status"]["state"], "TRANSIENT_SOURCE_ERROR")

    def test_rejected_input_is_durable_and_does_not_advance_observation_clock(self):
        self.bootstrap()
        self.checkpoint()
        original_clock = self.engine._load()["last_response_utc"]
        result = self.engine.observe_candidates({"poll_id": "bad", "request_started_utc": self.stamp(1),
                                                 "response_received_utc": self.stamp(1.1), "rows": "invalid", "source": "psycopg"})
        self.assertTrue(result["last_observation_rejected"])
        self.checkpoint()
        self.assertEqual(self.database.studies["test-study"]["state_json"]["last_response_utc"], original_clock)
        self.assertTrue(any(r["kind"] == "INPUT_REJECTED" for r in self.database.journal))

    def test_restores_open_positions_and_marks_restart_uncertainty(self):
        self.bootstrap()
        scope = next(s for s in self.config["scopes"] if s["symbol"] == "DOGE")
        row = {"scope_key": scope["scope_key"], "candidate_key": scope["candidate_key"],
               "candidate_definition_sha256": scope["candidate_definition_sha256"],
               "candidate_formula_version": scope["formula_version"], "symbol": "DOGE",
               "formula_direction": "SHORT", "event_id": 1, "source_event_id": 1,
               "event_kind": "ALERT", "delivery_status": "DELIVERED", "membership_status": "LIVE",
               "parent_evidence_eligible": True, "btc_parent_movement_id": "future-parent",
               "parent_start_time_utc": self.stamp(0.5), "match_source_time_utc": self.stamp(1)}
        row.update({key: self.stamp(1) for key in self.config["required_markers"]})
        row["parent_confirmed_at_utc"] = self.stamp(0.5)
        row["btc_observed_close_utc"] = self.stamp(0.9)
        self.engine.observe_candidates({"poll_id": "candidate", "request_started_utc": self.stamp(1.9),
                                        "response_received_utc": self.stamp(2), "rows": [row], "source": "psycopg"})
        planned = self.engine.summary()["books"]["DOGE_only"]["choices"]["future-parent"]["planned_entry_utc"]
        quote_start = datetime.fromisoformat(planned.replace("Z", "+00:00"))
        self.engine.observe_quotes({"packet_id": "entry", "request_started_utc": quote_start.isoformat(),
                                    "response_received_utc": (quote_start + timedelta(seconds=0.1)).isoformat(),
                                    "quotes": [{"symbol": "DOGE", "bid": "100", "ask": "100"}], "source": "BINANCE_SPOT_BOOK_TICKER"})
        self.assertIsNotNone(self.engine.summary()["books"]["DOGE_only"]["active"])
        self.checkpoint()
        self.restart()
        active = self.engine.summary()["books"]["DOGE_only"]["active"]
        self.assertTrue(active["path_uncertain"])
        self.assertIn("PROCESS_RESTART", active["uncertainty_reasons"])
        self.checkpoint()
        self.assertTrue(self.database.studies["test-study"]["state_json"]["books"]["DOGE_only"]["active"]["path_uncertain"])

    def test_connect_error_is_sanitized(self):
        def bad_connect(_dsn):
            raise RuntimeError("private-DSN-must-never-escape")
        broken = PostgresCheckpoint("private", "broken", self.config, connect=bad_connect)
        with self.assertRaises(CheckpointError) as failure:
            broken.acquire()
        self.assertNotIn("private", str(failure.exception))

    def test_bounded_driver_settings_both_supported_drivers(self):
        for driver in ("psycopg2", "psycopg"):
            with self.subTest(driver=driver):
                connect = Mock(return_value=object())
                modules = {driver: SimpleNamespace(connect=connect)}
                if driver == "psycopg":
                    modules["psycopg2"] = None
                with patch.dict("sys.modules", modules):
                    _default_connect("unit-test-only-dsn")
                kwargs = connect.call_args.kwargs
                self.assertEqual(kwargs["connect_timeout"], 5)
                self.assertEqual(kwargs["application_name"], "forward_paper_checkpoint")
                self.assertEqual(kwargs["tcp_user_timeout"], 10000)
                self.assertEqual(kwargs["keepalives_idle"], 5)
                self.assertEqual(kwargs["keepalives_interval"], 2)
                self.assertEqual(kwargs["keepalives_count"], 2)
                self.assertIn("statement_timeout=5000", kwargs["options"])
                self.assertIn("lock_timeout=2000", kwargs["options"])
                self.assertIn("timezone=UTC", kwargs["options"])
        self.assertIn("SET STATEMENT_TIMEOUT = '5S'", self.store._conn.statements)
        self.assertIn("SET LOCK_TIMEOUT = '2S'", self.store._conn.statements)
        self.assertIn("SET TIME ZONE 'UTC'", self.store._conn.statements)
        self.assertTrue(self.store._conn.autocommit)

    def test_disposable_file_cache_and_durable_status_sequence(self):
        cache_path = Path(self.engine.db.execute("PRAGMA database_list").fetchone()[2])
        self.assertEqual(cache_path.name, "working.sqlite")
        self.assertTrue(cache_path.exists())
        self.assertEqual(self.engine.db.execute("PRAGMA cache_size").fetchone()[0], -2048)
        self.bootstrap()
        self.checkpoint()
        self.assertEqual(self.store.journal_sequence, len(self.database.journal))
        self.store.update_health({"journal_events": 999999})
        self.assertEqual(self.database.studies["test-study"]["last_status"]["journal_events"], self.store.journal_sequence)
        self.engine.close()
        self.store.close()
        self.assertFalse(cache_path.exists())


if __name__ == "__main__":
    unittest.main()
