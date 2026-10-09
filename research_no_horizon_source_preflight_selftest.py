"""Source preflight mocks and optional isolated PostgreSQL privilege acceptance.

Only local test-named database credentials enable the optional fixture, which
creates and removes its own empty database and role. No market rows are used.
"""
from copy import deepcopy
import json
import os
import unittest
from unittest.mock import MagicMock, patch
from uuid import uuid4

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg import sql

import research_no_horizon_source_preflight as preflight
import research_no_horizon_registered_runner as runner


def metadata_fixture():
    return {"checked_at_utc": "2026-10-08T12:00:00+00:00",
        "database_matches": True, "user_matches": True, "session_matches": True,
        "read_only": "on", "ssl_in_use": True, "public_schema_usage": True,
        "role": {"rolcanlogin": True, **{key: False for key in preflight.ADMIN_FLAGS}},
        "database_owner": False, "database_create": False,
        **{key: 0 for key in preflight.FORBIDDEN_COUNTS},
        "relations": [{"name": name, "present": True, "kind": "r", "selectable": True,
            "row_security": False, "force_row_security": False, "columns": deepcopy(columns)}
            for name, columns in preflight.TABLE_COLUMNS.items()]}


class PreflightTests(unittest.TestCase):
    def setUp(self):
        self.dsn = "postgresql://reader:fixture-private-password@source.example:5432/market"
        self.source_id = "fixture-source"
        patcher = patch.dict(os.environ, {key: "" for key in ("PGHOSTADDR", "PGSERVICE", "PGOPTIONS", "PGPORT")})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.identity = preflight.endpoint_identity(self.dsn, source_id=self.source_id)

    def test_endpoint_fingerprint_matches_original_runner_format(self):
        expected = {"host": "source.example", "port": "5432", "dbname": "market", "user": "reader"}
        self.assertEqual(self.identity["endpoint_sha256"], runner.digest(runner.encoded(expected)))
        self.assertEqual(self.identity["sslmode"], "verify-full")

    def test_configuration_rejects_local_implicit_multi_endpoint_and_ambient_overrides(self):
        for raw in ("", "host=localhost user=reader dbname=market", "service=fixture",
                    "host=a,b user=reader dbname=market", "host=a hostaddr=127.0.0.1 user=u dbname=d",
                    "host=a user=u dbname=d options='-c readonly=off'", "host=a user=u dbname=d port=99999"):
            with self.subTest(raw=raw), self.assertRaises(Exception):
                preflight.endpoint_identity(raw, source_id=self.source_id)
        with patch.dict(os.environ, {"PGHOSTADDR": "127.0.0.1"}), self.assertRaisesRegex(ValueError, "AMBIENT"):
            preflight.endpoint_identity(self.dsn, source_id=self.source_id)

    def test_metadata_query_has_only_catalog_relations_and_driver_parameters(self):
        query = preflight.metadata_sql()
        self.assertNotIn("FROM public.", query)
        self.assertNotIn("JOIN public.", query)
        self.assertNotIn("SELECT *", query)
        self.assertEqual(query.count("%"), 2)
        # Compile the real psycopg placeholder parser without a server.
        from psycopg._queries import PostgresQuery
        parsed = PostgresQuery(psycopg.adapt.Transformer())
        parsed.convert(query, ("fixture_database", "fixture_user"))
        self.assertIn(b"$1", parsed.query)
        self.assertIn(b"$2", parsed.query)

    def test_pass_receipt_is_sealed_and_does_not_claim_global_or_deployment_proof(self):
        receipt = preflight.assess(metadata_fixture(), self.identity)
        self.assertEqual(receipt["status"], "PASS")
        self.assertTrue(preflight.validate_receipt(receipt, source_id=self.source_id,
                                                  endpoint_sha256=self.identity["endpoint_sha256"]))
        self.assertFalse(receipt["global_privilege_proof"])
        self.assertFalse(receipt["deployment_verified"])
        self.assertFalse(receipt["source_endpoint_seal_written"])
        self.assertFalse(receipt["market_rows_read"])
        self.assertNotIn("fixture-private-password", json.dumps(receipt))

    def test_admin_membership_ownership_and_write_privileges_fail_closed(self):
        fixture = metadata_fixture()
        changes = [("role", name) for name in preflight.ADMIN_FLAGS]
        changes += [(None, name) for name in preflight.FORBIDDEN_COUNTS]
        changes += [(None, "database_owner"), (None, "database_create")]
        for section, name in changes:
            data = deepcopy(fixture)
            target = data if section is None else data[section]
            target[name] = 1 if name in preflight.FORBIDDEN_COUNTS else True
            with self.subTest(section=section, field=name):
                receipt = preflight.assess(data, self.identity)
                self.assertEqual(receipt["status"], "FAIL")
                with self.assertRaises(ValueError):
                    preflight.validate_receipt(receipt, source_id=self.source_id,
                                                endpoint_sha256=self.identity["endpoint_sha256"])

    def test_source_table_select_shape_and_rls_contract_failures(self):
        changes = ({"present": False}, {"selectable": False}, {"kind": "v"},
                   {"row_security": True}, {"force_row_security": True}, {"columns": {}})
        for changed in changes:
            payload = metadata_fixture()
            payload["relations"][0].update(changed)
            with self.subTest(changed=changed):
                receipt = preflight.assess(payload, self.identity)
                self.assertFalse(receipt["checks"]["source_relations_readable_and_compatible"])
        payload = metadata_fixture()
        payload["relations"].pop()
        with self.assertRaisesRegex(ValueError, "EXACT_REQUIRED_RELATION"):
            preflight.assess(payload, self.identity)

    def test_transport_identity_and_readonly_mismatch_rejected(self):
        for key, value in (("database_matches", False), ("user_matches", False),
                           ("session_matches", False), ("ssl_in_use", False), ("read_only", "off")):
            payload = metadata_fixture()
            payload[key] = value
            with self.subTest(key=key):
                self.assertEqual(preflight.assess(payload, self.identity)["status"], "FAIL")

    def test_changed_receipt_endpoint_or_local_transport_cannot_gate_production(self):
        receipt = preflight.assess(metadata_fixture(), self.identity)
        receipt["checked_at_utc"] = "2026-10-09T00:00:00+00:00"
        with self.assertRaisesRegex(ValueError, "BINDING"):
            preflight.validate_receipt(receipt, source_id=self.source_id, endpoint_sha256=self.identity["endpoint_sha256"])
        receipt = preflight.assess(metadata_fixture(), {**self.identity, "sslmode": "disable"})
        with self.assertRaisesRegex(ValueError, "BINDING"):
            preflight.validate_receipt(receipt, source_id=self.source_id, endpoint_sha256=self.identity["endpoint_sha256"])

    def test_execute_uses_tls_readonly_and_catalog_query_without_sealing_state(self):
        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.execute.return_value.fetchmany.return_value = [{"preflight": metadata_fixture()}]
        with patch.object(psycopg, "connect", return_value=connection) as connect:
            receipt = preflight.execute(self.dsn, source_id=self.source_id)
        self.assertEqual(receipt["status"], "PASS")
        self.assertEqual(connect.call_args.kwargs["sslmode"], "verify-full")
        self.assertEqual(connect.call_args.kwargs["sslrootcert"], "system")
        self.assertIn("default_transaction_read_only=on", connect.call_args.kwargs["options"])
        calls = connection.execute.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0].args, ("SET TRANSACTION READ ONLY",))
        self.assertEqual(calls[1].args, (preflight.metadata_sql(), ("market", "reader")))

    def test_driver_and_configuration_errors_never_emit_credentials(self):
        with patch.object(psycopg, "connect", side_effect=psycopg.OperationalError(self.dsn)):
            receipt = preflight.execute(self.dsn, source_id=self.source_id)
        self.assertEqual(receipt["status"], "ERROR")
        self.assertEqual(receipt["error_type"], "OperationalError")
        self.assertNotIn(self.dsn, json.dumps(receipt))
        self.assertNotIn("fixture-private-password", json.dumps(receipt))
        with patch.object(psycopg, "connect") as connect:
            receipt = preflight.execute("", source_id=self.source_id)
            self.assertEqual(receipt["status"], "ERROR")
            connect.assert_not_called()


ADMIN_DSN = os.getenv("SOURCE_PREFLIGHT_TEST_DATABASE_URL") or os.getenv("TEST_DATABASE_URL")


@unittest.skipUnless(ADMIN_DSN, "Explicit local test database required for real role metadata acceptance")
class PostgreSQLPrivilegeTests(unittest.TestCase):
    def test_empty_source_role_pass_then_column_write_and_admin_flag_fail(self):
        info = conninfo_to_dict(ADMIN_DSN)
        if (info.get("host") not in ("localhost", "127.0.0.1", "::1", "postgres")
                or "test" not in info.get("dbname", "").lower()
                or any(key in info for key in ("service", "hostaddr", "options"))):
            self.fail("Real privilege fixture requires explicit local test-named database")
        token = uuid4().hex[:12]
        database, role = "test_source_preflight_" + token, "source_preflight_reader_" + token
        password = uuid4().hex
        created_database = created_role = False
        admin = psycopg.connect(ADMIN_DSN, autocommit=True, connect_timeout=5)
        try:
            admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
            created_database = True
            admin.execute(sql.SQL("CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT "
                                 "NOREPLICATION NOBYPASSRLS PASSWORD {}").format(sql.Identifier(role), sql.Literal(password)))
            created_role = True
            source_admin = make_conninfo(ADMIN_DSN, dbname=database)
            with psycopg.connect(source_admin, autocommit=True) as connection:
                for name, columns in preflight.TABLE_COLUMNS.items():
                    fields = sql.SQL(",").join(sql.SQL("{} {}").format(sql.Identifier(key), sql.SQL(kind))
                                              for key, kind in columns.items())
                    connection.execute(sql.SQL("CREATE TABLE public.{} ({})").format(sql.Identifier(name), fields))
                    connection.execute(sql.SQL("GRANT SELECT ON TABLE public.{} TO {}").format(sql.Identifier(name), sql.Identifier(role)))
                connection.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(role)))
                connection.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(database), sql.Identifier(role)))
            readonly = make_conninfo(source_admin, user=role, password=password)
            result = preflight.execute(readonly, source_id="ENGINEERING_ONLY", allow_local_source=True)
            self.assertEqual(result["status"], "PASS", result)
            with psycopg.connect(source_admin, autocommit=True) as connection:
                connection.execute(sql.SQL("GRANT UPDATE (consumer_version) ON public.research_watch_scan_intakes TO {}").format(sql.Identifier(role)))
            result = preflight.execute(readonly, source_id="ENGINEERING_ONLY", allow_local_source=True)
            self.assertEqual(result["status"], "FAIL", result)
            self.assertFalse(result["checks"]["no_relation_ownership_or_writes"])
            admin.execute(sql.SQL("ALTER ROLE {} CREATEDB").format(sql.Identifier(role)))
            result = preflight.execute(readonly, source_id="ENGINEERING_ONLY", allow_local_source=True)
            self.assertFalse(result["checks"]["login_without_admin_flags"])
        finally:
            if created_database:
                admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))
            if created_role:
                admin.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
            admin.close()


if __name__ == "__main__":
    unittest.main()
