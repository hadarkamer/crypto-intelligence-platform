"""Real PostgreSQL persistence checks against an explicitly supplied test DB.

No DATABASE_URL fallback: production credentials are never selected by tests.
Every run owns a unique schema and drops only that verified test schema.
"""
import base64
from copy import deepcopy
import os
import re
import unittest
from uuid import uuid4
import zlib

import research_consumed_inputs as capture


@unittest.skipUnless(os.getenv("RESEARCH_TEST_POSTGRES_URL"), "explicit PostgreSQL test database not configured")
class PostgresCaptureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import make_conninfo
        cls.psycopg, cls.sql = psycopg, sql
        cls.base_url = os.environ["RESEARCH_TEST_POSTGRES_URL"]
        cls.schema = "consumed_capture_test_" + uuid4().hex
        if not re.fullmatch(r"consumed_capture_test_[0-9a-f]{32}", cls.schema):
            raise ValueError("unverified test schema")
        with psycopg.connect(cls.base_url, connect_timeout=5) as connection:
            connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(cls.schema)))
        cls.test_url = make_conninfo(cls.base_url, options="-c search_path=" + cls.schema)
        cls.store = capture.PostgresStore(cls.test_url)

    @classmethod
    def tearDownClass(cls):
        if not re.fullmatch(r"consumed_capture_test_[0-9a-f]{32}", cls.schema):
            raise ValueError("unverified test schema")
        with cls.psycopg.connect(cls.base_url, connect_timeout=5) as connection:
            connection.execute(cls.sql.SQL("DROP SCHEMA {} CASCADE").format(cls.sql.Identifier(cls.schema)))

    def cycle(self, cycle_id=None, value=None):
        cycle_id = cycle_id or "integration:" + uuid4().hex
        value = value or uuid4().hex
        with capture.collect() as collector:
            capture.record("postgres_integration_fixture", {"id": value, "numbers": [1, 0.0, 1.25], "text": "נתוני בדיקה"})
        return capture.prepare_cycle(cycle_id, collector,
            rows=[{"symbol": "ETH", "value": value}], snapshot={}, score_block={},
            code_versions={"test_fixture": "a" * 64})

    def object_count(self):
        with self.psycopg.connect(self.test_url) as connection:
            return connection.execute("SELECT count(*) FROM research_consumed_input_objects").fetchone()[0]

    def test_schema_option_is_preserved_by_the_actual_store_connection(self):
        cycle = self.cycle()
        self.store.persist(cycle)
        with self.psycopg.connect(self.test_url) as connection:
            self.assertEqual(connection.execute("SELECT current_schema()").fetchone()[0], self.schema)
            namespaces = connection.execute("""SELECT c.relname,n.nspname FROM pg_class c
                JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE c.oid IN (to_regclass('research_consumed_input_objects'),
                               to_regclass('research_consumed_input_cycles'))""").fetchall()
        self.assertEqual(dict(namespaces), {"research_consumed_input_objects": self.schema,
                                            "research_consumed_input_cycles": self.schema})

    def test_real_binary_and_unicode_round_trip(self):
        cycle = self.cycle()
        result = self.store.persist(cycle)
        self.assertTrue(result["persisted"])
        self.assertEqual(self.store.read_cycle(cycle["cycle_id"]), cycle)
        package = capture.package_from_cycle(self.store.read_cycle(cycle["cycle_id"]))
        body = capture.event_payloads(package, "postgres_integration_fixture")[0][1]
        self.assertEqual(body["text"], "נתוני בדיקה")
        self.assertEqual(body["numbers"], [1, 0, 1.25])

    def test_idempotent_insert_deduplicates_and_keeps_first_body(self):
        cycle = self.cycle()
        self.store.persist(cycle)
        count = self.object_count()
        self.store.persist(deepcopy(cycle))
        self.assertEqual(self.object_count(), count)
        self.assertEqual(self.store.read_cycle(cycle["cycle_id"]), cycle)

    def test_conflicting_cycle_rolls_back_every_new_object(self):
        original = self.cycle()
        self.store.persist(original)
        count = self.object_count()
        changed = self.cycle(cycle_id=original["cycle_id"])
        with self.assertRaisesRegex(ValueError, "CYCLE_CONFLICT"):
            self.store.persist(changed)
        self.assertEqual(self.object_count(), count)
        self.assertEqual(self.store.read_cycle(original["cycle_id"]), original)

    def test_equivalent_compression_preserves_existing_valid_representation(self):
        original = self.cycle()
        self.store.persist(original)
        alternative = deepcopy(original)
        differences = 0
        for item in alternative["objects"].values():
            packed = base64.b64decode(item["compressed_b64"])
            recompressed = zlib.compress(zlib.decompress(packed), 9)
            differences += packed != recompressed
            item["compressed_b64"] = base64.b64encode(recompressed).decode("ascii")
        self.assertGreater(differences, 0)
        # Object identifiers and manifest still bind the same decoded nodes.
        capture.package_from_cycle(alternative)
        self.store.persist(alternative)
        self.assertEqual(self.store.read_cycle(original["cycle_id"]), original)

    def test_corrupt_existing_binary_body_is_rejected(self):
        original = self.cycle()
        self.store.persist(original)
        sha = next(iter(original["objects"]))
        try:
            with self.psycopg.connect(self.test_url) as connection:
                connection.execute("UPDATE research_consumed_input_objects SET payload=%s WHERE sha256=%s", (b"bad-zlib", sha))
            with self.assertRaises(Exception):
                self.store.persist(original)
            with self.assertRaises(Exception):
                self.store.read_cycle(original["cycle_id"])
        finally:
            # Repair only this test's unique object inside the owned schema.
            payload = base64.b64decode(original["objects"][sha]["compressed_b64"])
            with self.psycopg.connect(self.test_url) as connection:
                connection.execute("UPDATE research_consumed_input_objects SET payload=%s WHERE sha256=%s", (payload, sha))


if __name__ == "__main__":
    unittest.main()
