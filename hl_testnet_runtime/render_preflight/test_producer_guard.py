"""Producer fixture/native libpq identity restrictions; no real database access."""
from pathlib import Path
import importlib.util
import os
import subprocess
import sys
import unittest
from unittest.mock import patch

import psycopg
from psycopg.conninfo import make_conninfo

from . import guard


class ProducerGuardTests(unittest.TestCase):
    def setUp(self):
        self.runtime_url = "postgresql://preflight:synthetic@127.0.0.1:55432/hl_journal_ci"
        self.producer_url = self.runtime_url.replace("/hl_journal_ci", "/test_experimental_ci")
        self.runtime = guard.database_target(self.runtime_url, "hl_journal_ci")
        self.producer = guard.producer_target(self.producer_url, self.runtime)

    def allowed(self, args=(), kwargs=None):
        return guard.connection_allowed(args, kwargs or {}, self.runtime, self.producer)

    def test_all_three_fixture_database_names_and_admin_match_fresh_target(self):
        for name in ("test_experimental_ci", *["test_" + family + "_" + "a" * 32
                                            for family in ("r2732", "row71205", "sol_g65", "approved_alert")]):
            dsn = make_conninfo(self.producer_url, dbname=name)
            self.assertTrue(self.allowed((dsn,), {"connect_timeout": 5,
                "options": "-c statement_timeout=8000 -c lock_timeout=3000"}))

    def test_other_database_names_lookalikes_and_uuid_suffixes_are_refused(self):
        for name in ("postgres", "hl_journal_ci", "production", "test_anything", "test_r2732_short",
                     "test_row71205_" + "z" * 32, "test_sol_g65_" + "a" * 33):
            self.assertFalse(self.allowed((make_conninfo(self.producer_url, dbname=name),)))

    def test_url_target_must_share_exact_ephemeral_identity(self):
        for bad in (self.producer_url.replace("synthetic", "different"),
                    self.producer_url.replace("55432", "55433"),
                    self.producer_url.replace("127.0.0.1", "localhost"),
                    self.producer_url.replace("preflight", "owner"),
                    self.producer_url + "?hostaddr=example.invalid",
                    self.producer_url + "#fragment"):
            with self.assertRaises(RuntimeError) as caught:
                guard.producer_target(bad, self.runtime)
            self.assertNotIn("synthetic", str(caught.exception))
        with self.assertRaises(RuntimeError):
            guard.producer_target(self.producer_url, None)

    def test_native_dsn_cannot_change_host_port_user_or_password(self):
        for key, value in (("host", "example.invalid"), ("host", "127.0.0.1,example.invalid"),
                           ("port", "55433"), ("user", "owner"), ("password", "different")):
            self.assertFalse(self.allowed((make_conninfo(self.producer_url, **{key: value}),)))

    def test_native_dsn_cannot_hide_target_overrides_or_external_configuration(self):
        for key, value in (("hostaddr", "203.0.113.1"), ("service", "production"),
                           ("passfile", "/private/path"), ("options", "-c session_preload_libraries=x")):
            self.assertFalse(self.allowed((make_conninfo(self.producer_url, **{key: value}),)))
            self.assertFalse(self.allowed((self.producer_url,), {key: value}))

    def test_keyword_override_and_arbitrary_kwargs_are_refused(self):
        self.assertFalse(self.allowed((self.producer_url,), {"host": "127.0.0.1"}))
        self.assertFalse(self.allowed((self.producer_url,), {"conninfo": self.producer_url}))
        self.assertFalse(self.allowed((self.producer_url, self.producer_url)))

    def test_runtime_contract_kept_and_hostaddr_bypass_refused(self):
        kwargs = {**self.runtime, "sslmode": "disable", "connect_timeout": 4,
                  "options": "-c statement_timeout=5000 -c lock_timeout=3000 "
                             "-c idle_in_transaction_session_timeout=5000 -c synchronous_commit=on"}
        self.assertTrue(self.allowed(kwargs=kwargs))
        for extra in ({"hostaddr": "203.0.113.1"}, {"service": "production"},
                      {"options": "-c session_preload_libraries=x"}, {"connect_timeout": 0}):
            self.assertFalse(self.allowed(kwargs={**kwargs, **extra}))

    def test_missing_producer_configuration_keeps_positional_connections_blocked(self):
        self.assertFalse(guard.connection_allowed((self.producer_url,), {}, self.runtime, None))

    def test_source_archive_connection_uses_exact_runtime_keywords_and_readonly_on(self):
        from ..experimental_source_archive import ReadOnlySourceArchive
        reader = ReadOnlySourceArchive.for_ci(self.runtime_url)
        with patch.object(psycopg, 'connect') as native:
            reader._connect()
        args, kwargs = native.call_args
        self.assertEqual(args, ())
        self.assertTrue(guard.connection_allowed(args, kwargs, self.runtime, None))
        self.assertIn('-c default_transaction_read_only=on', kwargs['options'])
        for field, value in (('host', 'example.invalid'), ('port', 55433),
                ('dbname', 'production'), ('user', 'owner'), ('password', 'different')):
            self.assertFalse(guard.connection_allowed(args, {**kwargs, field: value}, self.runtime, None))
        self.assertFalse(guard.connection_allowed((self.runtime_url,), kwargs, self.runtime, None))

    def test_readonly_allowance_cannot_disable_readonly_or_add_other_options(self):
        base = {**self.runtime, 'sslmode': 'disable'}
        for options in ('-c default_transaction_read_only=off',
                '-c default_transaction_read_only=true', '-c default_transaction_read_only=1',
                '-c default_transaction_read_only=on -c default_transaction_read_only=off',
                '-c default_transaction_read_only=on -c search_path=public',
                '-c default_transaction_read_only=on -c session_preload_libraries=x',
                '-c default_transaction_read_only=on;SELECT 1'):
            self.assertFalse(guard.connection_allowed((), {**base, 'options': options}, self.runtime, None))

    def test_native_connect_delegates_only_validated_target(self):
        # Load a fresh guard module while mocking the native function and audit
        # hook; no global audit hooks or actual sockets escape this test.
        spec = importlib.util.spec_from_file_location("fresh_producer_guard", Path(guard.__file__))
        isolated = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(isolated)
        environment = {"HL_JOURNAL_CI_URL": self.runtime_url, "TEST_DATABASE_URL": self.producer_url}
        with patch.dict(os.environ, environment, clear=True), \
                patch.object(sys, "addaudithook"), patch.object(subprocess, "Popen"), \
                patch.object(psycopg, "connect") as native:
            native.return_value = "synthetic connection"
            attempts = isolated.install()
            self.assertEqual(psycopg.connect(self.producer_url, autocommit=True), "synthetic connection")
            native.assert_called_once_with(self.producer_url, autocommit=True)
            with self.assertRaisesRegex(RuntimeError, "PREFLIGHT_NONLOCAL_DATABASE_DISABLED"):
                psycopg.connect(self.producer_url, hostaddr="203.0.113.1")
            self.assertEqual(native.call_count, 1)
            self.assertEqual(attempts, ["database_blocked"])

    def test_invalid_initialization_does_not_mark_guard_installed(self):
        spec = importlib.util.spec_from_file_location("invalid_producer_guard", Path(guard.__file__))
        isolated = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(isolated)
        with patch.dict(os.environ, {"TEST_DATABASE_URL": self.producer_url}, clear=True):
            with self.assertRaises(RuntimeError):
                isolated.install()
        self.assertIsNone(isolated._attempts)

    def test_python_descendants_keep_same_database_identity_and_strip_unrelated_environment(self):
        spec = importlib.util.spec_from_file_location("descendant_producer_guard", Path(guard.__file__))
        isolated = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(isolated)
        captured = []
        class CapturePopen:
            def __init__(self, args, *positional, **kwargs):
                captured.append((args, kwargs))
        environment = {"HL_JOURNAL_CI_URL": self.runtime_url, "TEST_DATABASE_URL": self.producer_url,
                       "UNRELATED_FAKE_KEY": "not-inherited"}
        with patch.dict(os.environ, environment, clear=True), patch.object(sys, "addaudithook"), \
                patch.object(subprocess, "Popen", CapturePopen), patch.object(psycopg, "connect"):
            isolated.install()
            subprocess.Popen([sys.executable, "-c", "pass"], env=environment)
            actual = captured[0][1]["env"]
            self.assertEqual(actual["TEST_DATABASE_URL"], self.producer_url)
            self.assertEqual(actual["HL_JOURNAL_CI_URL"], self.runtime_url)
            self.assertNotIn("UNRELATED_FAKE_KEY", actual)
            self.assertIn("install();", captured[0][0][2])
            for bad in ({**environment, "TEST_DATABASE_URL": self.producer_url.replace("55432", "55433")},
                        {"TEST_DATABASE_URL": self.producer_url}):
                with self.assertRaisesRegex(RuntimeError, "PREFLIGHT_CHILD_DATABASE_CHANGED"):
                    subprocess.Popen([sys.executable, "-c", "pass"], env=bad)
            self.assertEqual(len(captured), 1)
