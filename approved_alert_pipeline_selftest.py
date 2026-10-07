"""Offline source decision -> durable outbox -> authenticated GTC execution.

Runs in the paired receiver/producer verification harness. The local transport
uses real encoding, HMAC verification and the receiver's durable SQLite inbox;
neither side constructs an HTTP socket or signs an exchange request.
"""
import json
from pathlib import Path
import tempfile
import unittest

import approved_alert_outbox as outbox
import experimental_execution_transport as transport
from approved_alert_producer_selftest import approved_fixture, projected
from experimental_execution_transport_selftest import config
from maxpain_experimental_specs import SPECS
from hl_testnet_runtime.experimental_execution_gateway import IsolatedGateway
from hl_testnet_runtime.experimental_execution_runtime import IsolatedExecutionRuntime, MODE
from hl_testnet_runtime.experimental_execution_state import ExecutionState
from hl_testnet_runtime.experimental_execution_prices import prepare
from hl_testnet_runtime.test_experimental_execution_runtime import ROUTES, META
from hl_testnet_runtime.test_approved_alert_lifecycle import CurrentOnlyExchange


class ApprovedPipelineTests(unittest.TestCase):
    def test_all_five_source_decisions_authenticate_then_submit_once_at_alert_limit(self):
        for spec in SPECS.values():
            with self.subTest(symbol=spec.coin), tempfile.TemporaryDirectory() as directory:
                fixture = approved_fixture(spec, gap=True)
                message, = projected(fixture)
                state = ExecutionState(Path(directory) / 'pipeline.isolated-experimental.sqlite3')
                state.initialize(ROUTES, not_before_ms=fixture['fence_ms'])
                venue = CurrentOnlyExchange(fixture['now_ms'])
                worker = IsolatedExecutionRuntime(state, venue, mode=MODE)
                cfg = config(mode=transport.APPROVED_MODE)
                gateway = IsolatedGateway(worker, key=cfg.secret, mode=MODE)
                delivery_errors = []

                class LocalConnection:
                    def __init__(self, *args, **kwargs): pass
                    def request(self, method, path, *, body, headers):
                        result = gateway.accept_authenticated(body, headers, path=path, method=method)
                        self.body = json.dumps(result).encode()
                    def getresponse(self): return self
                    status = 200
                    def getheader(self, name, default=''): return 'application/json'
                    def read(self, size): return self.body[:size]
                    def close(self): pass

                def reader(*args, **kwargs):
                    with fixture['database'].connect() as conn:
                        return outbox.read(conn, [fixture['key']])
                def acknowledge(scopes, value, **kwargs):
                    with fixture['database'].connect() as conn:
                        outbox.acknowledge(conn, [fixture['key']], value)
                def post(value, configuration, *, now_ms):
                    try:
                        return transport.post_plan(value, configuration, now_ms=now_ms,
                                                   connection_factory=LocalConnection)
                    except Exception as error:
                        delivery_errors.append(str(error))
                        raise
                sender = transport.Sender()
                result = sender.tick(['a' * 64], cfg, now_ms=venue.now(),
                                     reader=reader, post=post, acknowledge=acknowledge)
                self.assertEqual(result['recorded'], 1, (result, delivery_errors))
                self.assertEqual(venue.requests, [])  # Intake itself cannot trade.
                self.assertEqual(worker.run_once()['operation'], 'ENTRY')
                order = venue.requests[0]['proposal']['action']['orders'][0]
                self.assertEqual(order['p'], prepare(message, META)['execution']['entry'])
                self.assertEqual(order['t'], {'limit': {'tif': 'Gtc'}})
                self.assertEqual(state.load()['trades'][message['occurrence_id']]['source'], message)
                # Lost source ack / sender restart repeats the same identity.
                transport.post_plan(message, cfg, now_ms=venue.now(), connection_factory=LocalConnection)
                worker.run_once()
                self.assertEqual(len(venue.requests), 1)
                self.assertEqual(state.load()['trades'][message['occurrence_id']]['execution_audit']['received_at'],
                                 state.load()['sources'][message['occurrence_id']]['created_at'])


if __name__ == '__main__':
    unittest.main()
