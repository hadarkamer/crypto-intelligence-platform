"""Optional real-PostgreSQL gate for the Stage-8 Shadow read-only seam.

The test requires a dedicated, fully explicit local/CI reader DSN in
``TEST_STAGE8_SHADOW_READER_DATABASE_URL``.  Its login must be exactly
``research_stage8_reader_v1``.  No runtime or production DSN is inspected.
"""
from __future__ import annotations

import hashlib
import os
import unittest

import research_stage8_shadow_postgres as shadow_postgres


TEST_DSN_ENV = "TEST_STAGE8_SHADOW_READER_DATABASE_URL"


@unittest.skipUnless(
    os.environ.get(TEST_DSN_ENV),
    f"{TEST_DSN_ENV} is required for PostgreSQL integration",
)
class ShadowPostgresRealDatabaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg.conninfo import conninfo_to_dict

        cls.psycopg = psycopg
        cls.dsn = os.environ[TEST_DSN_ENV]
        target = conninfo_to_dict(cls.dsn)
        required = {"host", "port", "dbname", "user", "password", "sslmode"}
        if (not required.issubset(target)
                or target.get("host") not in {
                    "localhost", "127.0.0.1", "::1", "postgres",
                }
                or not (target.get("dbname", "").startswith("test_")
                        or target.get("dbname", "").endswith("_test"))
                or target.get("user") != shadow_postgres.EXPECTED_ROLE):
            raise ValueError(
                "Shadow integration requires a fully explicit local/CI "
                "test database DSN for the dedicated Stage-8 reader role"
            )

    def test_real_session_is_dict_row_read_only_repeatable_read_and_closed(self):
        dependencies = shadow_postgres.build_dependencies()
        target_hash = hashlib.sha256(self.dsn.encode("utf-8")).hexdigest()
        with dependencies.open_read_only_session(
            database_url=self.dsn,
            expected_role=shadow_postgres.EXPECTED_ROLE,
            database_target_sha256=target_hash,
        ) as connection:
            before = dependencies.verify_read_only_session(connection)
            row = connection.execute("""SELECT
                current_setting('transaction_read_only') AS read_only,
                current_setting('transaction_isolation') AS isolation,
                current_setting('statement_timeout') AS statement_timeout,
                current_setting('lock_timeout') AS lock_timeout
            """).fetchone()
            self.assertIsInstance(row, dict)
            self.assertEqual(row["read_only"], "on")
            self.assertEqual(row["isolation"], "repeatable read")
            self.assertEqual(row["statement_timeout"], "10s")
            self.assertEqual(row["lock_timeout"], "1s")
            after = dependencies.verify_read_only_session(connection)
            self.assertEqual(before, after)
            with self.assertRaises(self.psycopg.errors.ReadOnlySqlTransaction):
                connection.execute(
                    "CREATE TEMPORARY TABLE stage8_shadow_must_not_write(id int)"
                )
        self.assertTrue(connection.closed)


if __name__ == "__main__":
    unittest.main(verbosity=2)
