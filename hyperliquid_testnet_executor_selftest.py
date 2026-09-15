"""Offline tests of TESTNET wire conversion, persistence and receipt checking.

No production imports, no real account, no real HTTP call, no printed key.
A separate SDK check in CI tests official signing with a fresh unconnected key.
"""
from contextlib import redirect_stdout
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import io
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import hyperliquid_testnet_executor as mod

ACCOUNT = '0x' + '1' * 40
AGENT = '0x' + '2' * 40
SECOND = '0x' + '3' * 40
META = {'universe': [{'name': 'OTHER', 'szDecimals': 2}, {'name': 'DEMO', 'szDecimals': 2}]}


def signal(identity='synthetic-1', side='LONG'):
    return {'kind': 'SIGNAL', 'event_id': identity, 'symbol': 'DEMO', 'side': side,
            'entry': '100.00', 'stop': '98' if side == 'LONG' else '102',
            'take_profit': '104' if side == 'LONG' else '96',
            'at': (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()}


def action(message=None, mode='market'):
    return mod.build_action(message or signal(), META, ACCOUNT, exit_type=mode)


def order_response(expected, *, state='open', symbol='DEMO', remaining=None):
    trigger = expected['t'].get('trigger')
    return {'status': 'order', 'order': {'status': state, 'statusTimestamp': mod.now_ms(), 'order': {
        'coin': symbol, 'side': 'B' if expected['b'] else 'A', 'limitPx': expected['p'],
        'origSz': expected['s'], 'sz': remaining if remaining is not None else ('0' if state == 'filled' else expected['s']),
        'cloid': expected['c'], 'oid': 100, 'reduceOnly': expected['r'], 'isTrigger': bool(trigger),
        'triggerPx': trigger['triggerPx'] if trigger else '0',
        'orderType': ('Take Profit' if trigger['tpsl'] == 'tp' else 'Stop') + (' Market' if trigger['isMarket'] else ' Limit') if trigger else 'Limit'
    }}}


class FakeExchange:
    def __init__(self):
        self.calls = []
        self.action = None
        self.reply = None
        self.timeout = False
        self.busy = False
        self.partial = False
        self.pending = False
        self.wrong_role = False
        self.mismatch = False
        self.missing_child = False
        self.status_code = 200
        self.too_large = False
    def connection(self, host, timeout):
        if host != 'api.hyperliquid-testnet.xyz' or timeout != 4:
            raise AssertionError('Only the fixed TESTNET destination is allowed')
        owner = self
        class Connection:
            def request(self, method, path, raw, headers):
                if method != 'POST': raise AssertionError('Wrong method')
                self.path, self.body = path, json.loads(raw)
                owner.calls.append((path, self.body))
                if path == '/exchange': owner.action = self.body['action']
            def getresponse(self):
                if self.path == '/exchange' and owner.timeout: raise TimeoutError('SECRET_SHOULD_NOT_APPEAR')
                self.status = owner.status_code
                return self
            def read(self, maximum):
                if owner.too_large: return b'x' * maximum
                return json.dumps(owner.response(self.path, self.body)).encode()
            def close(self): pass
        return Connection()
    def response(self, path, body):
        if path == '/exchange':
            if self.reply is not None: return self.reply
            return {'status': 'ok', 'response': {'type': 'order', 'data': {'statuses': [
                {'resting': {'oid': 101}} if self.pending else {'filled': {'oid': 101, 'totalSz': '10', 'avgPx': '100'}},
                'waitingForFill' if self.pending else {'resting': {'oid': 102}},
                'waitingForFill' if self.pending else {'resting': {'oid': 103}}]}}}
        kind = body['type']
        if kind == 'userRole':
            return {'role': 'user'} if body['user'] != AGENT or self.wrong_role else {'role': 'agent', 'data': {'user': ACCOUNT}}
        if kind == 'meta': return META
        if kind == 'frontendOpenOrders': return [{'coin': 'OTHER'}] if self.busy else []
        if kind == 'clearinghouseState':
            positions = []
            if self.action and not self.pending:
                entry = self.action['orders'][0]
                size = Decimal(entry['s']) * (1 if entry['b'] else -1)
                if self.partial: size /= 2
                positions = [{'position': {'coin': 'DEMO', 'szi': str(size)}}]
            return {'assetPositions': positions}
        if kind == 'orderStatus':
            for index, expected in enumerate(self.action['orders']):
                if expected['c'] != body['oid']: continue
                if index and self.missing_child: return {'status': 'unknownOid'}
                state = 'open' if index or self.pending or self.partial else 'filled'
                remaining = str(Decimal(expected['s']) / 2) if index == 0 and self.partial else None
                response = order_response(expected, state=state, remaining=remaining)
                if self.mismatch and index == 2: response['order']['order']['reduceOnly'] = False
                return response
        raise AssertionError('Unexpected request type')


class BuildTests(unittest.TestCase):
    def test_three_orders_and_native_grouping(self):
        built = action()
        self.assertEqual(built['grouping'], 'normalTpsl')
        self.assertEqual(len(built['orders']), 3)
        self.assertEqual([o['a'] for o in built['orders']], [1, 1, 1])
    def test_three_prices_same_value_no_rounding(self):
        msg = signal(); built = action(msg)
        for key, order in zip(('entry', 'take_profit', 'stop'), built['orders']):
            self.assertEqual(Decimal(msg[key]), Decimal(order['p']))
        self.assertEqual(built['orders'][0]['p'], '100')
    def test_long_and_short_sides(self):
        self.assertEqual([o['b'] for o in action()['orders']], [True, False, False])
        self.assertEqual([o['b'] for o in action(signal(side='SHORT'))['orders']], [False, True, True])
    def test_exits_only_reduce(self):
        self.assertEqual([o['r'] for o in action()['orders']], [False, True, True])
    def test_risk_quantity_is_not_twenty_dollars_notional(self):
        orders = action()['orders']
        self.assertEqual(orders[0]['s'], '10')
        self.assertEqual(Decimal(orders[0]['s']) * Decimal(orders[0]['p']), 1000)
    def test_size_floored_not_prices(self):
        msg = signal(); msg['stop'] = '97'
        self.assertEqual(action(msg)['orders'][0]['s'], '6.66')
    def test_exit_type_must_be_explicit(self):
        with self.assertRaises(mod.TestnetError): action(mode='automatic')
    def test_limit_exit_copies_trigger_and_limit(self):
        for order in action(mode='limit')['orders'][1:]:
            self.assertFalse(order['t']['trigger']['isMarket'])
            self.assertEqual(order['p'], order['t']['trigger']['triggerPx'])
    def test_market_exit_keeps_trigger(self):
        orders = action()['orders']
        self.assertTrue(orders[1]['t']['trigger']['isMarket'])
        self.assertEqual(orders[1]['t']['trigger']['triggerPx'], '104')
        self.assertEqual(orders[2]['t']['trigger']['triggerPx'], '98')
    def test_stable_cloid_and_distinct_roles(self):
        msg = signal(); first = action(msg)
        self.assertEqual(first, action(msg))
        self.assertEqual(len({o['c'] for o in first['orders']}), 3)
    def test_new_duplicate_signal_has_new_ids(self):
        self.assertNotEqual(action(signal('a'))['orders'][0]['c'], action(signal('b'))['orders'][0]['c'])
    def test_account_in_cloid_namespace(self):
        msg = signal()
        self.assertNotEqual(action(msg)['orders'][0]['c'], mod.build_action(msg, META, SECOND, exit_type='market')['orders'][0]['c'])
    def test_missing_extra_and_secret_fields_rejected(self):
        for kind in ('missing', 'extra'):
            msg = signal()
            if kind == 'missing': del msg['stop']
            else: msg['private_key'] = 'SECRET_SHOULD_NOT_APPEAR'
            with self.assertRaises(mod.TestnetError): action(msg)
    def test_invalid_decimal_inputs(self):
        for price in ('nan', 'Infinity', '0', '-1', 100.0, True, '1e9999', '1e-9999'):
            msg = signal(); msg['entry'] = price
            with self.assertRaises(mod.TestnetError): action(msg)
    def test_price_precision_not_silently_changed(self):
        msg = signal(); msg['entry'] = '100.001'
        with self.assertRaisesRegex(mod.TestnetError, 'PRECISION'): action(msg)
    def test_bad_direction_and_price_order(self):
        for side in ('BULLISH', 'SHORT', 'sell'):
            msg = signal(); msg['side'] = side
            with self.assertRaises(mod.TestnetError): action(msg)
    def test_missing_timezone(self):
        msg = signal(); msg['at'] = '2026-01-01T10:00:00'
        with self.assertRaises(mod.TestnetError): action(msg)
    def test_symbol_missing_delisted_duplicate_and_bad_precision(self):
        for meta in ({'universe': []}, {'universe': [META['universe'][1]]*2},
                     {'universe': [{**META['universe'][1], 'isDelisted': True}]},
                     {'universe': [{**META['universe'][1], 'szDecimals': True}]}):
            with self.assertRaises(mod.TestnetError): mod.build_action(signal(), meta, ACCOUNT, exit_type='market')
    def test_invalid_account(self):
        for value in ('', '0x'+'0'*40, 'https://example.com', None):
            with self.assertRaises(mod.TestnetError): mod.build_action(signal(), META, value, exit_type='market')
    def test_lab_cap_not_increased_to_force_acceptance(self):
        msg = signal(); msg['stop'] = '99.99'
        with self.assertRaisesRegex(mod.TestnetError, 'SIZE_BOUNDS'): action(msg)
    def test_frozen_original_not_mutated(self):
        msg = signal(); before = deepcopy(msg); meta = deepcopy(META)
        action(msg)
        self.assertEqual(msg, before); self.assertEqual(meta, META)
    def test_no_builder_or_transfer_or_leverage_action(self):
        self.assertEqual(set(action()), {'type', 'orders', 'grouping'})


class ExecutionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name); os.chmod(self.root, 0o700)
        self.journal = self.root / 'attempts.hl-testnet.sqlite3'
        self.msg = signal()
        self.server = FakeExchange()
        self.network = patch.object(mod.http.client, 'HTTPSConnection', side_effect=self.server.connection)
        self.network.start()
        self.wallet_patch = patch.object(mod, '_wallet', return_value=SimpleNamespace(address=AGENT))
        self.wallet = self.wallet_patch.start()
        self.sign_patch = patch.object(mod, '_signed_body', side_effect=lambda wallet, act, nonce: {
            'action': act, 'nonce': nonce, 'signature': {'r': '0x1', 's': '0x2', 'v': 27}, 'expiresAfter': nonce+30000})
        self.signer = self.sign_patch.start()
    def tearDown(self):
        self.sign_patch.stop(); self.wallet_patch.stop(); self.network.stop(); self.temp.cleanup()
    def submit(self, message=None, enabled=True):
        return mod.submit_once(message if message is not None else self.msg, account=ACCOUNT,
                               journal=self.journal, exit_type='market', enable_testnet=enabled)
    def exchange_calls(self):
        return [call for call in self.server.calls if call[0] == '/exchange']
    def test_disabled_before_any_io(self):
        self.assertEqual(self.submit(enabled=False)['status'], 'DISABLED')
        self.assertFalse(self.server.calls); self.wallet.assert_not_called(); self.assertFalse(self.journal.exists())
    def test_truthy_strings_cannot_activate(self):
        for enabled in ('true', 'testnet', 1, None):
            self.assertEqual(self.submit(enabled=enabled)['status'], 'DISABLED')
        self.assertFalse(self.server.calls)
    def test_success_requires_read_back_all_three(self):
        result = self.submit()
        self.assertEqual(result['status'], 'VERIFIED_OPEN_PROTECTED')
        self.assertTrue(result['verified']); self.assertTrue(result['protection_active'])
        self.assertEqual(result['order_batches_sent'], 1)
        self.assertEqual(len(self.exchange_calls()[0][1]['action']['orders']), 3)
        self.assertEqual(sum(body.get('type') == 'orderStatus' for _, body in self.server.calls), 3)
    def test_short_roundtrip(self):
        self.assertEqual(self.submit(signal(side='SHORT'))['status'], 'VERIFIED_OPEN_PROTECTED')
    def test_stops_not_claimed_active_while_entry_pending(self):
        self.server.pending = True
        result = self.submit()
        self.assertEqual(result['status'], 'VERIFIED_WAITING_ENTRY')
        self.assertFalse(result['protection_active'])
    def test_partial_entry_not_claimed_protected(self):
        self.server.partial = True
        result = self.submit()
        self.assertEqual(result['status'], 'PARTIAL_ENTRY_REQUIRES_REVIEW')
        self.assertFalse(result['verified'])
    def test_missing_child_requires_review(self):
        self.server.missing_child = True
        self.assertFalse(self.submit()['verified'])
    def test_wrong_child_fields_requires_review(self):
        self.server.mismatch = True
        self.assertFalse(self.submit()['verified'])
    def test_mixed_ack_errors_not_success(self):
        self.server.reply = {'status': 'ok', 'response': {'type': 'order', 'data': {'statuses': [
            {'filled': {'oid': 101}}, {'error': 'SECRET_SHOULD_NOT_APPEAR'}, {'resting': {'oid': 103}}]}}}
        result = self.submit()
        self.assertEqual(result['status'], 'RESPONSE_REQUIRES_REVIEW')
        self.assertNotIn('SECRET', json.dumps(result))
    def test_single_batch_rejection_not_three_successes(self):
        self.server.reply = {'status': 'ok', 'response': {'type': 'order', 'data': {'statuses': [{'error': 'rejected'}]}}}
        self.assertFalse(self.submit()['verified'])
    def test_timeout_persists_and_does_not_retry(self):
        self.server.timeout = True
        self.assertEqual(self.submit()['status'], 'UNCERTAIN_REQUIRES_REVIEW')
        again = self.submit()
        self.assertTrue(again['replayed']); self.assertEqual(again['order_batches_sent'], 0)
        self.assertEqual(len(self.exchange_calls()), 1)
    def test_restart_replay_same_id(self):
        self.submit()
        self.assertTrue(self.submit()['replayed'])
        self.assertEqual(len(self.exchange_calls()), 1)
    def test_same_id_with_different_prices_is_blocked(self):
        self.submit(); updated = deepcopy(self.msg); updated['take_profit'] = '105'
        with self.assertRaisesRegex(mod.TestnetError, 'EXISTING_ID_CHANGED'): self.submit(updated)
        self.assertEqual(len(self.exchange_calls()), 1)
    def test_same_signal_key_order_does_not_matter(self):
        self.submit()
        self.assertTrue(self.submit(dict(reversed(list(self.msg.items()))))['replayed'])
    def test_account_reservation_not_bypassed_by_new_signal(self):
        self.submit()
        self.server.action = None  # Even an apparently empty account must keep its reservation.
        with self.assertRaisesRegex(mod.TestnetError, 'RESERVED'): self.submit(signal('new'))
        self.assertEqual(len(self.exchange_calls()), 1)
    def test_readonly_inspect_no_signing_or_credentials(self):
        self.submit(); self.wallet.reset_mock(); self.signer.reset_mock()
        result = mod.inspect_once(self.msg['event_id'], journal=self.journal)
        self.assertTrue(result['verified']); self.assertEqual(result['order_batches_sent'], 0)
        self.wallet.assert_not_called(); self.signer.assert_not_called(); self.assertEqual(len(self.exchange_calls()), 1)
    def test_unrelated_existing_orders_block_send(self):
        self.server.busy = True
        with self.assertRaisesRegex(mod.TestnetError, 'EMPTY'): self.submit()
        self.assertFalse(self.exchange_calls())
    def test_wrong_account_agent_is_blocked(self):
        self.server.wrong_role = True
        with self.assertRaisesRegex(mod.TestnetError, 'AGENT_NOT_AUTHORIZED'): self.submit()
        self.signer.assert_not_called(); self.assertFalse(self.exchange_calls())
    def test_master_key_is_refused(self):
        self.wallet.return_value = SimpleNamespace(address=ACCOUNT)
        with self.assertRaisesRegex(mod.TestnetError, 'NOT_MASTER'): self.submit()
        self.assertFalse(self.server.calls)
    def test_old_future_and_incomplete_test_messages_no_send(self):
        for seconds in (-300, 300):
            msg = signal(); msg['at'] = (datetime.now(timezone.utc)+timedelta(seconds=seconds)).isoformat()
            with self.assertRaises(mod.TestnetError): self.submit(msg)
        self.wallet.assert_not_called(); self.assertFalse(self.server.calls)
    def test_changed_host_cannot_send(self):
        with patch.object(mod, 'TESTNET_HOST', 'api.hyperliquid.xyz'):
            with self.assertRaisesRegex(mod.TestnetError, 'HOST_CHANGED'): self.submit()
        self.assertFalse(self.server.calls)
    def test_http_redirect_is_not_followed(self):
        self.server.status_code = 302
        with self.assertRaises(mod.TestnetError): mod.TestnetHTTP().info('meta')
        self.assertEqual(len(self.server.calls), 1)
    def test_response_size_limit(self):
        self.server.too_large = True
        with self.assertRaises(mod.TestnetError): mod.TestnetHTTP().info('meta')
    def test_readonly_http_cannot_submit(self):
        with self.assertRaises(mod.TestnetError): mod.TestnetHTTP()._post('/exchange', {'action': action()})
        self.assertFalse(self.server.calls)
    def test_withdrawal_leverage_and_unknown_endpoint_refused(self):
        client = mod.TestnetHTTP(allow_orders=True)
        for path, body in (('/exchange', {'action': {'type': 'withdraw3'}}), ('/foo', {})):
            with self.assertRaises(mod.TestnetError): client._post(path, body)
        with self.assertRaises(mod.TestnetError): client.info('anything')
        self.assertFalse(self.server.calls)
    def test_no_credentials_or_signatures_in_log_and_stdout(self):
        output = io.StringIO()
        with redirect_stdout(output): self.submit()
        self.assertEqual(output.getvalue(), '')
        raw = self.journal.read_bytes()
        self.assertNotIn(b'signature', raw); self.assertNotIn(b'HL_TESTNET_AGENT_KEY', raw)
        self.assertEqual(self.journal.stat().st_mode & 0o777, 0o600)
    def test_foreign_database_rejected(self):
        db = sqlite3.connect(self.journal); db.execute('CREATE TABLE alien(x)'); db.close(); self.journal.chmod(0o600)
        with self.assertRaisesRegex(mod.TestnetError, 'FOREIGN'): mod.AttemptLog(self.journal)
    def test_symlink_log_rejected(self):
        self.journal.symlink_to(self.root/'other')
        with self.assertRaises(mod.TestnetError): mod.AttemptLog(self.journal)
    def test_shared_directory_rejected(self):
        self.root.chmod(0o755)
        with self.assertRaises(mod.TestnetError): mod.AttemptLog(self.journal)
    def test_reservation_is_durable_before_signing(self):
        def signer(wallet, act, nonce):
            log = mod.AttemptLog(self.journal)
            try: self.assertEqual(log.get(self.msg['event_id'])['result']['status'], 'RESERVED_OR_UNCERTAIN')
            finally: log.close()
            raise RuntimeError('simulated process failure')
        self.signer.side_effect = signer
        self.assertFalse(self.submit()['verified'])
        self.assertTrue(self.submit()['replayed'])
        self.assertFalse(self.exchange_calls())


class ResponseTests(unittest.TestCase):
    def test_invalid_ack_shapes(self):
        for response in (None, {}, [], {'status': 'err', 'response': 'error'}, {'status': 'ok', 'response': []}):
            self.assertEqual(mod.acknowledgement(response), 'RESPONSE_REQUIRES_REVIEW')
    def test_duplicate_json_and_nonfinite_are_rejected(self):
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b'bad', b'\xff'):
            with self.assertRaises(mod.TestnetError): mod._decode(raw)
    def test_waiting_children_ack_not_verified(self):
        response = {'status': 'ok', 'response': {'type': 'order', 'data': {'statuses': [{'resting': {'oid': 1}}, 'waitingForFill', 'waitingForFill']}}}
        self.assertEqual(mod.acknowledgement(response), 'ACKNOWLEDGED_NOT_VERIFIED')
    def test_zero_or_boolean_order_id_is_not_acceptance(self):
        for oid in (0, True, '1'):
            response = {'status': 'ok', 'response': {'type': 'order', 'data': {'statuses': [{'resting': {'oid': oid}}]*3}}}
            self.assertEqual(mod.acknowledgement(response), 'RESPONSE_REQUIRES_REVIEW')
    def test_missing_key_does_not_load_sdk(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(mod.importlib.metadata, 'version') as version:
            with self.assertRaisesRegex(mod.TestnetError, 'KEY_REQUIRED'): mod._wallet()
            version.assert_not_called()
    def test_position_duplicate_cannot_hide_opposite_amounts(self):
        state = {'assetPositions': [{'position': {'coin': 'DEMO', 'szi': value}} for value in ('1', '-1')]}
        with self.assertRaisesRegex(mod.TestnetError, 'DUPLICATE'): mod._position(state, 'DEMO')


if __name__ == '__main__':
    unittest.main(verbosity=2)
