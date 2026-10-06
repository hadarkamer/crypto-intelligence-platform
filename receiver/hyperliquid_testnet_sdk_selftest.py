"""Official SDK compatibility, entirely offline with a fresh unconnected key."""
from copy import deepcopy
import importlib.metadata
import json
import socket
import unittest
from unittest.mock import patch

from eth_account import Account
from hyperliquid.utils import signing
from hyperliquid.utils.types import Cloid
import hyperliquid_testnet_executor as adapter
from hyperliquid_testnet_executor_selftest import ACCOUNT, META, signal


class SDKTests(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket, 'create_connection', side_effect=AssertionError('Offline SDK test'))
        self.network.start()
    def tearDown(self):
        self.network.stop()
    def test_pinned_official_sdk(self):
        self.assertEqual(importlib.metadata.version('hyperliquid-python-sdk'), adapter.SDK_VERSION)
    def test_wire_structure_and_key_order_match_official_sdk(self):
        for exit_type in ('market', 'limit'):
            action = adapter.build_action(signal(), META, ACCOUNT, exit_type=exit_type)
            sdk_orders = []
            for wire in action['orders']:
                typ = deepcopy(wire['t'])
                if 'trigger' in typ: typ['trigger']['triggerPx'] = float(typ['trigger']['triggerPx'])
                request = {'coin': 'DEMO', 'is_buy': wire['b'], 'sz': float(wire['s']),
                           'limit_px': float(wire['p']), 'reduce_only': wire['r'],
                           'order_type': typ, 'cloid': Cloid.from_str(wire['c'])}
                sdk_orders.append(signing.order_request_to_order_wire(request, wire['a']))
            expected = signing.order_wires_to_order_action(sdk_orders, grouping='normalTpsl')
            # Exact serialization also checks the msgpack map-key ordering.
            self.assertEqual(json.dumps(action), json.dumps(expected))
    def test_testnet_signature_cannot_be_replayed_as_mainnet(self):
        wallet = Account.create()  # Ephemeral, unfunded, never used on any network.
        action = adapter.build_action(signal(), META, ACCOUNT, exit_type='market')
        nonce = adapter.now_ms()
        with patch.object(signing, 'sign_l1_action', wraps=signing.sign_l1_action) as sign:
            body = adapter._signed_body(wallet, action, nonce)
            self.assertIs(sign.call_args.args[-1], False)
            self.assertIsNone(sign.call_args.args[2])
        self.assertEqual(body['expiresAfter'], nonce + 30000)
        arguments = [body['action'], body['signature'], None, nonce, body['expiresAfter']]
        testnet = signing.recover_agent_or_user_from_l1_action(*arguments, False)
        mainnet = signing.recover_agent_or_user_from_l1_action(*arguments, True)
        self.assertEqual(testnet.lower(), wallet.address.lower())
        self.assertNotEqual(mainnet.lower(), wallet.address.lower())
        self.assertNotIn('builder', body['action'])
    def test_agrees_with_existing_signal_contract(self):
        from paper_execution_feed import signal_message
        message = signal()
        self.assertEqual(adapter._signal(message), signal_message(message))


if __name__ == '__main__':
    unittest.main(verbosity=2)
