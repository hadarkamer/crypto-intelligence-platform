"""Offline configured startup -> real source reader -> authenticated gateway."""
from contextlib import ExitStack, redirect_stdout
from copy import deepcopy
import io
import json
from pathlib import Path
import socket
import sys
import tempfile
import threading
from types import ModuleType
import unittest
from unittest.mock import Mock, patch

import experimental_execution_bridge as bridge
import experimental_execution_contract as contract
import experimental_execution_forwarder as forwarder
import experimental_execution_transport as transport
from experimental_execution_bridge_selftest import maxpain_pending_fixture
from experimental_execution_extended_source_selftest import source_fixture
from experimental_execution_source_audit_selftest import BASE, SCOPE, ReadOnlySource, ms, r2732_fixture
from experimental_execution_transport_selftest import FakeHttps, HOST, KEY
from maxpain_experimental_specs import SPECS
from hl_testnet_runtime.experimental_execution_gateway import IsolatedGateway
from hl_testnet_runtime import experimental_execution_runtime as runtime
from hl_testnet_runtime.experimental_execution_state import ExecutionState
from hl_testnet_runtime.test_experimental_execution_runtime import SoftwareExchange, ROUTES


def environment(**changes):
    result = dict(EXPERIMENTAL_EXECUTION_FORWARD_MODE=transport.MODE,
        EXPERIMENTAL_EXECUTION_BRIDGE_MODE=bridge.MODE,
        EXPERIMENTAL_EXECUTION_FORWARD_SCOPES=json.dumps([SCOPE]),
        EXPERIMENTAL_EXECUTION_FORWARD_ALLOWED_HOSTS=json.dumps([HOST]),
        EXPERIMENTAL_EXECUTION_FORWARD_NOT_BEFORE=BASE.isoformat(),
        EXPERIMENTAL_EXECUTION_FORWARD_ENDPOINT='https://'+HOST+transport.PATH,
        EXPERIMENTAL_EXECUTION_FORWARD_SECRET=KEY,
        DATABASE_URL='postgresql://isolated:isolated@dpg-d94d641kh4rs73evvih0-a/crypto_intelligence_db')
    result.update(changes)
    return result


class ForwarderTests(unittest.TestCase):
    def setUp(self):
        for name in ('socket.socket.connect', 'socket.socket.connect_ex', 'http.client.HTTPSConnection'):
            guard = patch(name, side_effect=AssertionError('OFFLINE_ONLY'))
            guard.start(); self.addCleanup(guard.stop)

    def test_disabled_startup_and_config_probe_have_no_io(self):
        with patch.object(bridge, 'read_experimental', side_effect=AssertionError('NO_SOURCE')), \
             patch.object(transport, 'post_plan', side_effect=AssertionError('NO_HTTP')), \
             redirect_stdout(io.StringIO()) as out:
            self.assertIsNone(forwarder.configuration({}))
            self.assertEqual(forwarder.main([], env={}), 0)
            self.assertEqual(forwarder.main(['--check-config'], env=environment()), 0)
        self.assertIn('DISABLED', out.getvalue())
        self.assertIn('CONFIGURED', out.getvalue())
        self.assertNotIn(KEY, out.getvalue())

    def test_explicit_config_requires_source_capture_mode_hash_scopes_and_known_https_host(self):
        for changes in (
            {'EXPERIMENTAL_EXECUTION_BRIDGE_MODE': ''},
            {'EXPERIMENTAL_EXECUTION_FORWARD_SCOPES': '[]'},
            {'EXPERIMENTAL_EXECUTION_FORWARD_SCOPES': json.dumps([SCOPE]*2)},
            {'EXPERIMENTAL_EXECUTION_FORWARD_SCOPES': json.dumps([str(i)*64 for i in range(5)])},
            {'EXPERIMENTAL_EXECUTION_FORWARD_SCOPES': json.dumps(['chat-id'])},
            {'EXPERIMENTAL_EXECUTION_FORWARD_SCOPES': '{}'},
            {'EXPERIMENTAL_EXECUTION_FORWARD_ALLOWED_HOSTS': '[1]'},
            {'EXPERIMENTAL_EXECUTION_FORWARD_ENDPOINT': 'https://unapproved.invalid'+transport.PATH},
            {'EXPERIMENTAL_EXECUTION_FORWARD_SECRET': 'private-invalid-key'},
            {'EXPERIMENTAL_EXECUTION_FORWARD_NOT_BEFORE': '2026-10-06T00:00:00'},
        ):
            with self.subTest(changes=changes), self.assertRaisesRegex(transport.TransportError,
                                                                     '^FORWARD_CONFIGURATION_REQUIRED$'):
                forwarder.configuration(environment(**changes))
        self.assertNotIn(KEY, repr(forwarder.configuration(environment())))

    def test_main_errors_are_redacted_and_do_not_attempt_io(self):
        with redirect_stdout(io.StringIO()) as out:
            self.assertEqual(forwarder.main(['--once'], env=environment(
                EXPERIMENTAL_EXECUTION_FORWARD_SECRET='private-invalid-key')), 2)
        self.assertNotIn('private-invalid-key', out.getvalue())
        self.assertIn('FORWARD_INITIALIZATION_OR_CYCLE_UNAVAILABLE', out.getvalue())

    def test_periodic_worker_never_overlaps_or_catches_up_and_honors_shutdown(self):
        worker = forwarder.Forwarder(forwarder.configuration(environment()))
        blocked = threading.Event(); release = threading.Event()
        def held_tick(*args):
            blocked.set(); release.wait(2)
            return dict(status='COMPLETED', attempted=0)
        with patch.object(worker.sender, 'tick', side_effect=held_tick) as tick:
            thread = threading.Thread(target=worker.run_once)
            thread.start()
            try:
                self.assertTrue(blocked.wait(1))
                self.assertEqual(worker.run_once()['status'], 'FORWARD_CYCLE_ALREADY_RUNNING')
                self.assertEqual(tick.call_count, 1)
            finally:
                release.set(); thread.join(2)
        stop = Mock()
        stop.is_set.side_effect = [False, False]
        stop.wait.side_effect = [False, True]
        with patch.object(worker, 'run_once', return_value={'status': 'COMPLETED'}) as once:
            output = []
            worker.run(stop, emit=output.append)
        self.assertEqual(once.call_count, 2)
        self.assertEqual([c.args for c in stop.wait.call_args_list], [(30,), (30,)])

    def test_all_eight_configured_source_states_reach_actual_authenticated_gateway(self):
        fixtures = [r2732_fixture(), source_fixture('hype'), source_fixture('g65')]
        fixtures += [maxpain_pending_fixture(spec) for spec in SPECS.values()]
        self.assertEqual(len(fixtures), 8)
        for f in fixtures:
            now = f.get('now_ms') or ms(f['now'])
            with self.subTest(source_key=f['key']), tempfile.TemporaryDirectory() as directory:
                source = ReadOnlySource({f['key']: json.dumps(f['state'])})
                before = deepcopy(source.values)
                store = ExecutionState(Path(directory)/'relay.isolated-experimental.sqlite3')
                store.initialize(ROUTES, not_before_ms=ms(BASE))
                venue = SoftwareExchange(now)
                runtime_worker = runtime.IsolatedExecutionRuntime(store, venue, mode=runtime.MODE)
                gateway = IsolatedGateway(runtime_worker, key=KEY, mode=runtime.MODE)
                def respond(request):
                    receipt = gateway.accept_authenticated(request['body'], request['headers'],
                        path=request['path'], method=request['method'])
                    return 200, json.dumps(receipt).encode()
                fake = FakeHttps(respond)
                psycopg, rows = ModuleType('psycopg'), ModuleType('psycopg.rows')
                psycopg.connect, rows.dict_row = source.connect, object()
                cfg = environment()
                # Only the network/database drivers and wall clock are faked.
                # Startup configuration, DSN guard, eight real SQL key reads,
                # projection, serialization, signature and gateway are intact.
                with ExitStack() as stack:
                    stack.enter_context(patch.dict('os.environ', cfg, clear=True))
                    stack.enter_context(patch.dict(sys.modules, {'psycopg': psycopg, 'psycopg.rows': rows}))
                    stack.enter_context(patch('http.client.HTTPSConnection', side_effect=fake.connect))
                    stack.enter_context(patch.object(transport.time, 'time', return_value=now/1000))
                    worker = forwarder.Forwarder(forwarder.configuration(cfg))
                    first = worker.run_once()
                    # A process restart re-reads the actual source; the same
                    # durable receiver acknowledges a duplicate, not a trade.
                    second = forwarder.Forwarder(forwarder.configuration(cfg)).run_once()
                self.assertEqual((first['recorded'], second['duplicates']), (1, 1))
                self.assertEqual(first['status'], 'COMPLETED')
                self.assertEqual(len(source.calls), 16)
                self.assertTrue(all(sql.startswith('SELECT ') for sql, _ in source.calls))
                self.assertIn('default_transaction_read_only=on', source.connections[0][1]['options'])
                self.assertEqual(source.values, before)
                self.assertEqual(len(fake.calls), 2)
                self.assertEqual(venue.requests, [])
                self.assertEqual(len(store.load()['sources']), 1)


if __name__ == '__main__':
    unittest.main()
