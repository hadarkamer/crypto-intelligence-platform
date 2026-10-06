"""Fully isolated source -> HTTPS wire -> authenticated actual receiver tests."""
from copy import deepcopy
from datetime import timedelta
import json
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import Mock, patch

import experimental_execution_bridge as bridge
import experimental_execution_contract as contract
import experimental_execution_transport as transport
from experimental_execution_extended_source_selftest import source_fixture, project, advance
from experimental_execution_source_audit_selftest import BASE, SCOPE, ReadOnlySource, ms
from experimental_execution_fixtures import hype_row71205_message, sol_g65_message, maxpain_message
from hl_testnet_runtime import experimental_plan_intake as intake
from hl_testnet_runtime.experimental_plan_store import PlanStore
from hl_testnet_runtime.test_experimental_plan_store import FakeJournal

HOST = 'isolated-receiver.example.invalid'
KEY = 'f'*64


def config(**changes):
    values = dict(endpoint='https://' + HOST + transport.PATH,
                  allowed_hosts=(HOST,), secret=KEY, fence=BASE, mode=transport.MODE)
    values.update(changes)
    return transport.Config(**values)


def ack(value, status='RECORDED'):
    return dict(occurrence_id=value['occurrence_id'], revision=1, status=status,
                record_only=True, entry_permission='RETIRED' if value['kind'] == 'CANCEL' else 'WAITING')


class FakeHttps:
    def __init__(self, responder):
        self.responder = responder
        self.calls = []
        self.closed = 0
        self.read_limits = []

    def connect(self, host, *, port, timeout):
        assert (host, port, timeout) == (HOST, 443, 3)
        owner = self
        class Connection:
            def request(self, method, path, *, body, headers):
                owner.calls.append(dict(method=method, path=path, body=body, headers=headers))
                self.reply = owner.responder(owner.calls[-1])
            def getresponse(self):
                reply = self.reply
                class Response:
                    status = reply[0]
                    def read(self, limit):
                        owner.read_limits.append(limit)
                        return reply[1][:limit]
                    def getheader(self, key, default=''):
                        assert key == 'Content-Type'
                        return reply[2] if len(reply) > 2 else 'application/json'
                return Response()
            def close(self): owner.closed += 1
        return Connection()


class TransportTests(unittest.TestCase):
    def setUp(self):
        for name in ('socket.socket.connect', 'socket.socket.connect_ex', 'http.client.HTTPSConnection'):
            guard = patch(name, side_effect=AssertionError('OFFLINE_ONLY'))
            guard.start(); self.addCleanup(guard.stop)

    def test_disabled_never_reads_source_opens_http_or_validates_live_config(self):
        reader, post = Mock(side_effect=AssertionError()), Mock(side_effect=AssertionError())
        sender = transport.Sender()
        self.assertEqual(sender.tick([], reader=reader, post=post)['status'], 'DISABLED')
        self.assertEqual(sender.tick([], config(endpoint='invalid', secret='', mode=''),
                                     reader=reader, post=post)['status'], 'DISABLED')
        reader.assert_not_called(); post.assert_not_called()

    def test_exact_https_destination_path_and_no_credentials_query_or_port(self):
        for endpoint in ('http://' + HOST + transport.PATH, 'https://unapproved.invalid'+transport.PATH,
            'https://user:password@'+HOST+transport.PATH, 'https://'+HOST+':444'+transport.PATH,
            'https://'+HOST+transport.PATH+'?secret=abc', 'https://'+HOST+transport.PATH+'#fragment',
            'https://'+HOST+'/exchange', 'https://'+HOST+transport.PATH+'/'):
            with self.subTest(endpoint=endpoint), self.assertRaises(transport.TransportError):
                config(endpoint=endpoint).validate()
        self.assertNotIn(KEY, repr(config()))

    def test_wire_signature_matches_receiver_exactly_and_closes_connection(self):
        value = hype_row71205_message(); now = contract.moment_ms(value['source_as_of'])
        fake = FakeHttps(lambda _: (200, json.dumps(ack(value)).encode()))
        reply = transport.post_plan(value, config(), now_ms=now, connection_factory=fake.connect)
        request = fake.calls[0]
        self.assertEqual(request['method'], 'POST'); self.assertEqual(request['path'], intake.PATH)
        self.assertTrue(intake.authenticate(KEY, request['headers']['X-Plan-Timestamp'],
            request['headers']['X-Plan-Signature'], request['body'], now/1000))
        self.assertEqual(json.loads(request['body']), value)
        self.assertEqual(reply['occurrence_id'], value['occurrence_id'])
        self.assertEqual(fake.closed, 1)
        self.assertEqual(fake.read_limits, [transport.MAX_RESPONSE_BYTES+1])

    def test_redirect_errors_unbounded_wrong_identity_and_ambiguous_ack_not_success(self):
        value = hype_row71205_message(); raw = json.dumps(ack(value)).encode()
        wrong = ack(value); wrong['occurrence_id'] = '0'*64
        truthy = ack(value); truthy['record_only'] = 1
        revision = ack(value); revision['revision'] = True
        for response in ((302, raw), (503, b'private diagnostic'), (200, b'x'*9000),
            (200, json.dumps(wrong).encode()), (200, b'{"status":"RECORDED","status":"DUPLICATE"}'),
            (200, raw, 'text/html'), (200, b'NaN'), (200, json.dumps(truthy).encode()),
            (200, json.dumps(revision).encode())):
            fake = FakeHttps(lambda _: response)
            with self.subTest(status=response[0]), self.assertRaises(transport.TransportError):
                transport.post_plan(value, config(), now_ms=contract.moment_ms(value['source_as_of']),
                                    connection_factory=fake.connect)
            self.assertEqual(len(fake.calls), 1)
            self.assertEqual(fake.closed, 1)

    def test_source_to_real_authenticated_receiver_and_durable_duplicate_receipt(self):
        for kind in ('hype', 'g65'):
            f = source_fixture(kind)
            source = ReadOnlySource({f['key']: json.dumps(f['state'])})
            journal, sender = FakeJournal(), transport.Sender()
            def responder(request):
                self.assertTrue(intake.authenticate(KEY, request['headers']['X-Plan-Timestamp'],
                    request['headers']['X-Plan-Signature'], request['body'], ms(f['now'])/1000))
                result = intake.accept(request['body'], PlanStore(journal), now=f['now'].isoformat(),
                                       not_before=BASE.isoformat())
                return 200, json.dumps(result).encode()
            fake = FakeHttps(responder)
            def reader(*args, **kwargs):
                with patch('alert_cards_forwarder._source_dsn', return_value='mock-only'):
                    return bridge.read_experimental(*args, **kwargs, connect=source.connect)
            def post(value, cfg, **kwargs):
                return transport.post_plan(value, cfg, **kwargs, connection_factory=fake.connect)
            result = sender.tick({SCOPE}, config(), now_ms=ms(f['now']), reader=reader, post=post)
            self.assertEqual((result['status'], result['recorded']), ('COMPLETED', 1))
            identity = project(f)[0]['occurrence_id']
            stored = PlanStore(journal).load(identity)
            self.assertEqual(stored['initial_source'], project(f)[0])
            self.assertEqual(len(journal.events), 1)
            # Process restart forgets sender memo. Actual inbox commit remains
            # idempotent, and no strategy/order is manufactured by reception.
            second = transport.Sender().tick({SCOPE}, config(), now_ms=ms(f['now']), reader=reader, post=post)
            self.assertEqual(second['duplicates'], 1)
            self.assertEqual(len(journal.events), 1)
            self.assertIsNone(PlanStore(journal).load(identity)['strategy'])

    def test_source_transport_gateway_consumed_by_complete_isolated_runtime(self):
        from hl_testnet_runtime.experimental_execution_gateway import IsolatedGateway
        from hl_testnet_runtime import experimental_execution_runtime as runtime
        from hl_testnet_runtime.experimental_execution_state import ExecutionState
        from hl_testnet_runtime.test_experimental_execution_runtime import SoftwareExchange, ROUTES
        for kind in ('hype', 'g65'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                f = source_fixture(kind)
                source = ReadOnlySource({f['key']: json.dumps(f['state'])})
                store = ExecutionState(Path(directory)/'relay.isolated-experimental.sqlite3')
                store.initialize(ROUTES, not_before_ms=ms(BASE))
                venue = SoftwareExchange(ms(f['now']))
                worker = runtime.IsolatedExecutionRuntime(store, venue, mode=runtime.MODE)
                gateway = IsolatedGateway(worker, key=KEY, mode=runtime.MODE)
                def respond(request):
                    result = gateway.accept_authenticated(request['body'], request['headers'],
                        path=request['path'], method=request['method'])
                    return 200, json.dumps(result).encode()
                fake = FakeHttps(respond)
                def reader(*args, **kwargs):
                    with patch('alert_cards_forwarder._source_dsn', return_value='mock-only'):
                        return bridge.read_experimental(*args, **kwargs, connect=source.connect)
                def post(value, cfg, **kwargs):
                    return transport.post_plan(value, cfg, **kwargs, connection_factory=fake.connect)
                result = transport.Sender().tick({SCOPE}, config(), now_ms=venue.now(), reader=reader, post=post)
                self.assertEqual(result['recorded'], 1)
                self.assertEqual(venue.requests, [])
                self.assertEqual(worker.run_once()['operation'], 'ENTRY')
                identity = project(f)[0]['occurrence_id']
                if kind == 'g65':
                    entry_oid = venue.oid('ENTRY')
                    venue.fill(entry_oid, venue.orders[entry_oid]['view']['wire_order']['s'])
                for _ in range(3): worker.run_once()
                trade = store.load()['trades'][identity]
                self.assertEqual(trade['phase'], 'OPEN')
                venue.fill(venue.oid('TAKE_PROFIT'), trade['quantity'])
                for _ in range(3): worker.run_once()
                self.assertEqual(store.load()['trades'][identity]['phase'], 'CLOSED')
                self.assertEqual(runtime.readiness()['real_exchange_requests_sent'], 0)

    def test_real_receiver_lost_commit_response_retry_is_duplicate(self):
        f = source_fixture('g65'); value = project(f)[0]
        journal = FakeJournal(); journal.lose_commit_ack_once = True
        sender = transport.Sender()
        def responder(request):
            try:
                receipt = intake.accept(request['body'], PlanStore(journal), now=f['now'].isoformat(),
                                        not_before=BASE.isoformat())
                return 200, json.dumps(receipt).encode()
            except RuntimeError:
                return 503, b'{}'
        fake = FakeHttps(responder)
        post = lambda v, cfg, **kw: transport.post_plan(v, cfg, **kw, connection_factory=fake.connect)
        reader = lambda *args, **kwargs: [value]
        first = sender.tick({SCOPE}, config(), now_ms=ms(f['now']), reader=reader, post=post)
        second = sender.tick({SCOPE}, config(), now_ms=ms(f['now']), reader=reader, post=post)
        self.assertEqual((first['deferred'], second['duplicates']), (1, 1))
        self.assertEqual(len(journal.events), 1)

    def test_cancellation_priority_retry_retention_and_tombstone_stale_source(self):
        now = ms(BASE)+70_000
        pending = sol_g65_message()
        cancelled = sol_g65_message(kind='CANCEL', cancel_reason='SOURCE_ENTRY_OBSERVED')
        market = hype_row71205_message()
        sender = transport.Sender(); sent = []
        def fail(value, cfg, **kw):
            sent.append(value['kind']); raise TimeoutError()
        first = sender.tick({SCOPE}, config(), now_ms=now,
            reader=lambda *a, **kw: [market, pending, cancelled], post=fail)
        self.assertEqual(sent, ['CANCEL', 'PLAN'])
        self.assertEqual(first['deferred'], 2)
        def unavailable(*a, **kw): raise RuntimeError('source unavailable')
        sent.clear()
        def success(value, cfg, **kw): sent.append(value['kind']); return ack(value)
        second = sender.tick({SCOPE}, config(), now_ms=now+200_000, reader=unavailable, post=success)
        self.assertEqual(sent, ['CANCEL'])
        self.assertEqual(second['recorded'], 1)
        third = sender.tick({SCOPE}, config(), now_ms=now, reader=lambda *a, **kw: [pending], post=success)
        self.assertEqual(third['attempted'], 0)

    def test_failed_records_rotate_with_bounded_requests(self):
        now = ms(BASE)+70_000
        values = [sol_g65_message(decision_ms=ms(BASE)-i*1800_000, as_of_ms=now,
                    kind='CANCEL', cancel_reason='SOURCE_OBSERVATION_ENDED') for i in range(40)]
        sender = transport.Sender(); sent = []
        def fail(value, cfg, **kw): sent.append(value['occurrence_id']); raise TimeoutError()
        for _ in range(3):
            result = sender.tick({SCOPE}, config(), now_ms=now, reader=lambda *a, **kw: values, post=fail)
            self.assertEqual(result['attempted'], transport.MAX_PER_TICK)
        self.assertEqual(len(set(sent[:40])), 40)
        self.assertEqual(len(sender.pending), 40)

    def test_elapsed_budget_stops_cycle_without_losing_cancellation(self):
        value = sol_g65_message(kind='CANCEL', cancel_reason='SOURCE_ENTRY_OBSERVED')
        sender = transport.Sender(); post = Mock()
        times = iter((0., 13.))
        result = sender.tick({SCOPE}, config(), now_ms=ms(BASE)+70_000,
            reader=lambda *a, **kw: [value], post=post, monotonic=lambda: next(times))
        self.assertEqual((result['attempted'], result['deferred']), (0, 1))
        post.assert_not_called()

    def test_recipient_metadata_swaps_do_not_conflict_on_same_source_sequence(self):
        value = maxpain_message(); other = deepcopy(value)
        other['proof']['episode_generation'] += 1
        other['proof']['episode_first_ms'] += 1
        sender = transport.Sender(); now = contract.moment_ms(value['source_as_of'])
        first = sender.tick({SCOPE}, config(), now_ms=now,
            reader=lambda *a, **kw: [value], post=lambda v, cfg, **kw: ack(v))
        second = sender.tick({SCOPE}, config(), now_ms=now,
            reader=lambda *a, **kw: [other], post=Mock(side_effect=AssertionError('NO_DUPLICATE_POST')))
        self.assertEqual((first['recorded'], second['status'], second['attempted']), (1, 'COMPLETED', 0))
        conflict = deepcopy(other); conflict['valid_until'] = contract.iso_ms(now+1_000)
        third = sender.tick({SCOPE}, config(), now_ms=now,
            reader=lambda *a, **kw: [conflict], post=Mock(side_effect=AssertionError('NO_CONFLICT_POST')))
        self.assertEqual(third['status'], 'SOURCE_UNAVAILABLE')

    def test_source_default_clock_quantizes_only_runtime_clock(self):
        f = source_fixture('g65'); source = ReadOnlySource({f['key']: json.dumps(f['state'])})
        clock = type('Clock', (), {'now': staticmethod(lambda timezone: f['now'].replace(microsecond=123456))})
        with patch.object(bridge, 'datetime', clock), patch('alert_cards_forwarder._source_dsn', return_value='mock-only'):
            result = bridge.read_experimental({SCOPE}, BASE, env={'EXPERIMENTAL_EXECUTION_BRIDGE_MODE':bridge.MODE},
                                              connect=source.connect)
        self.assertEqual(len(result), 1)

    def test_read_deadline_stops_between_bounded_queries(self):
        source = ReadOnlySource({})
        times = iter((0., 1., 13.))
        with patch('alert_cards_forwarder._source_dsn', return_value='mock-only'), \
             self.assertRaisesRegex(contract.ContractError, 'SOURCE_READ_DEADLINE'):
            bridge.read_experimental({SCOPE}, BASE, now=BASE,
                env={'EXPERIMENTAL_EXECUTION_BRIDGE_MODE':bridge.MODE}, connect=source.connect,
                deadline_monotonic=12., clock=lambda: next(times))
        self.assertEqual(len(source.calls), 1)


if __name__ == '__main__': unittest.main()
