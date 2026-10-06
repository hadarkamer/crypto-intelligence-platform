"""Real PostgreSQL atomicity, immutable reference, idempotence and open refresh.

Only an explicit local/CI TEST_DATABASE_URL and disposable schema are used.
"""
from datetime import timedelta
import os
from pathlib import Path
import unittest
from uuid import uuid4

import research_native_hype_mark_supplement as supplement
from research_native_hype_mark_supplement_selftest import event, membership, path, ENTRY


@unittest.skipUnless(os.environ.get("TEST_DATABASE_URL"), "TEST_DATABASE_URL required for PostgreSQL integration")
class NativeMarkPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg.conninfo import conninfo_to_dict
        from psycopg.rows import dict_row
        cls.dsn = os.environ["TEST_DATABASE_URL"]
        info = conninfo_to_dict(cls.dsn)
        if (info.get("host") not in {"localhost", "127.0.0.1", "::1", "postgres"}
                or "test" not in info.get("dbname", "").lower()):
            raise RuntimeError("Only an explicit local PostgreSQL test database is allowed")
        cls.conn = psycopg.connect(cls.dsn, autocommit=True, row_factory=dict_row)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def setUp(self):
        self.schema = "native_mark_test_" + uuid4().hex
        self.conn.execute(f'CREATE SCHEMA "{self.schema}"')
        self.conn.execute(f'SET search_path TO "{self.schema}"')
        types = {"event_id": "bigint PRIMARY KEY", "alert_time_utc": "timestamptz",
            "engine_snapshot": "jsonb", **{key: "double precision" for key in (
                "score", "current_price", "target_price", "initial_target_distance_pct")}}
        self.conn.execute("CREATE TABLE research_events (" + ",".join(
            key + " " + types.get(key, "text") for key in supplement.SOURCE_FIELDS) + ")")
        self.conn.execute("""CREATE TABLE research_event_btc_movements (
            event_id bigint, episode_policy_version text, btc_parent_movement_id text,
            decision_time_utc timestamptz, btc_observed_close_utc timestamptz, membership_status text)""")
        migration = Path(__file__).parent / "migrations/042_native_hype_mark_supplement.sql"
        self.conn.execute(migration.read_text())
        self.conn.execute(migration.read_text())
        original = event()
        self.conn.execute("INSERT INTO research_events VALUES (" + ",".join(
            "%s::jsonb" if key == "engine_snapshot" else "%s" for key in supplement.SOURCE_FIELDS) + ")",
            tuple(supplement.canonical(original[key]) if key == "engine_snapshot" else original[key]
                for key in supplement.SOURCE_FIELDS))
        m = membership()
        self.conn.execute("INSERT INTO research_event_btc_movements VALUES (%s,%s,%s,%s,%s,%s)", tuple(m.values()))

    def tearDown(self):
        self.conn.execute('SET search_path TO public')
        self.conn.execute(f'DROP SCHEMA "{self.schema}" CASCADE')

    def measure(self, minutes, price_path=None):
        source = supplement._load(self.conn, [901])[0]
        return supplement.derive_measurement(source, source["btc_membership"],
            price_path or path(minutes), observed_at=ENTRY + timedelta(minutes=minutes))

    def scalar(self, sql):
        return next(iter(self.conn.execute(sql).fetchone().values()))

    def test_idempotence_open_advance_and_unchanged_native_source(self):
        source_before = supplement._load(self.conn, [901])[0]
        early = self.measure(60)
        self.assertTrue(supplement.write_measurement(self.conn, early))
        self.assertTrue(supplement.write_measurement(self.conn, early))
        self.assertTrue(supplement.write_measurement(self.conn, self.measure(240)))
        self.assertEqual(self.scalar("SELECT count(*) FROM research_native_hype_mark_outcomes"), 32)
        self.assertEqual(self.scalar("SELECT count(*) FROM research_native_hype_mark_metrics"), 4)
        self.assertEqual(self.scalar("SELECT count(*) FROM research_native_hype_mark_metrics WHERE status='READY'"), 2)
        self.assertEqual(supplement._load(self.conn, [901])[0], source_before)
        saved = self.conn.execute("SELECT measurement_payload FROM research_native_hype_mark_measurements").fetchone()["measurement_payload"]
        self.assertEqual(saved["entry_price"], 100)
        self.assertEqual(saved["original_reference_price"], 88)
        self.assertEqual(saved["btc_parent_movement_id"], "original-wave-9")
        self.assertFalse(saved["live_union_eligible"])

    def test_old_or_partial_refresh_cannot_regress_good_coverage(self):
        self.assertTrue(supplement.write_measurement(self.conn, self.measure(240)))
        self.assertFalse(supplement.write_measurement(self.conn, self.measure(60)))
        broken = path(720)
        del broken["candles"][90]
        self.assertFalse(supplement.write_measurement(self.conn, self.measure(720, broken)))
        self.assertEqual(self.scalar("SELECT count(*) FROM research_native_hype_mark_metrics WHERE status='READY'"), 2)

    def test_changed_entry_or_original_source_is_rejected(self):
        self.assertTrue(supplement.write_measurement(self.conn, self.measure(60)))
        changed = path(240)
        changed["candles"][0]["open"] = 100.5
        with self.assertRaisesRegex(ValueError, "frozen MARK entry"):
            supplement.write_measurement(self.conn, self.measure(240, changed))
        pending = self.measure(240)
        self.conn.execute("UPDATE research_events SET current_price=89 WHERE event_id=901")
        with self.assertRaisesRegex(ValueError, "Native source changed"):
            supplement.write_measurement(self.conn, pending)
        self.assertEqual(self.scalar("SELECT measurement_payload->>'entry_price' FROM research_native_hype_mark_measurements"), "100.0")

    def test_partial_write_rolls_back_and_retry_is_safe(self):
        self.conn.execute("ALTER TABLE research_native_hype_mark_metrics ADD CONSTRAINT injected_failure CHECK(window_minutes<>1440)")
        with self.assertRaises(Exception):
            supplement.write_measurement(self.conn, self.measure(1440))
        self.assertEqual(self.scalar("SELECT count(*) FROM research_native_hype_mark_measurements"), 0)
        self.assertEqual(self.scalar("SELECT count(*) FROM research_native_hype_mark_outcomes"), 0)
        self.conn.execute("ALTER TABLE research_native_hype_mark_metrics DROP CONSTRAINT injected_failure")
        self.assertTrue(supplement.write_measurement(self.conn, self.measure(1440)))
        self.assertEqual(self.scalar("SELECT count(*) FROM research_native_hype_mark_metrics WHERE status='READY'"), 4)

    def test_terminal_decision_and_ready_window_revisions_are_rejected(self):
        self.assertTrue(supplement.write_measurement(self.conn, self.measure(60)))
        changed = path(240)
        changed["candles"][0]["high"] = 100.05
        with self.assertRaisesRegex(ValueError, "frozen terminal evidence"):
            supplement.write_measurement(self.conn, self.measure(240, changed))
        changed = path(240)
        # This candle occurs after the winning touch, preserving the terminal
        # decision, but it revises an already-complete common-window path.
        changed["candles"][30]["high"] = 100.1
        with self.assertRaisesRegex(ValueError, "frozen complete-window evidence"):
            supplement.write_measurement(self.conn, self.measure(240, changed))
        self.assertEqual(self.scalar("SELECT count(*) FROM research_native_hype_mark_metrics WHERE status='READY'"), 1)


if __name__ == "__main__":
    unittest.main()
