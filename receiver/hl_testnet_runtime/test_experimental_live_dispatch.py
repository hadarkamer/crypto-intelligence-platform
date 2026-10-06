"""The concrete Testnet sender, with only SDK/key/HTTPS boundaries replaced."""
from copy import deepcopy
import importlib.metadata
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from . import experimental_live_dispatch as live
from . import experimental_execution_dispatch as boundary
from . import card_lifecycle as life, filled_quantity_dispatch as wire, request_budget, checks
from .test_experimental_execution_dispatch import fixture, Permit
from .test_two_account_execution import D


class LiveDispatchTests(unittest.TestCase):
    def setUp(self):
        for target in ('socket.create_connection', 'socket.socket.connect'):
            guard = patch(target, side_effect=AssertionError('NETWORK_FORBIDDEN'))
            guard.start(); self.addCleanup(guard.stop)
        self.request, self.context, self.t = fixture()
        self.request['domain'] = 'testnet'
        self.persisted = deepcopy(self.request)
        self.env = deepcopy(self.context['env'])
        self.env['HL_TESTNET_EXPERIMENTAL_DISPATCH'] = live.APPROVAL
        self.release = dict(domain='testnet', release_id='d'*64, dispatch_enabled=True,
            protection_enabled=True, entries_enabled=True,
            not_before_ms=self.context['safety']['not_before_ms'],
            entry_expires_at_ms=self.t+60000,
            routes={role: live.roles.route_for(self.env, role)['account'] for role in live.roles.ROLES})
        self.budget = object.__new__(request_budget.Budget)
        self.acquisitions, self.signed, self.http, self.closed = [], [], [], []
        self.sign_hook = None; self.http_error = None; self.status = 200
        self.raw = json.dumps(dict(status='ok', response=dict(type='order',
            data=dict(statuses=[dict(resting=dict(oid=10))])))).encode()
        def acquire(_budget, path, body, **kw):
            self.acquisitions.append((path, deepcopy(body), kw)); return Permit()
        p = patch.object(request_budget.Budget, 'acquire', acquire)
        p.start(); self.addCleanup(p.stop)
        self.wallet = SimpleNamespace(address=D)
        p = patch.object(live.roles, 'wallet_for_role', return_value=self.wallet)
        self.key_loader = p.start(); self.addCleanup(p.stop)
        def sign(wallet, action, vault, nonce, expires, mainnet):
            self.signed.append((wallet.address, deepcopy(action), vault, nonce, expires, mainnet))
            if self.sign_hook: self.sign_hook(action)
            return dict(r='0x1', s='0x2', v=27)
        p = patch.dict('sys.modules', {'hyperliquid.utils.signing': SimpleNamespace(sign_l1_action=sign)})
        p.start(); self.addCleanup(p.stop)
        test = self
        class Connection:
            def __init__(self, host, timeout):
                self.host, self.timeout = host, timeout
            def request(self, method, path, body, headers):
                test.http.append((self.host, self.timeout, method, path, json.loads(body), headers))
                if test.http_error: raise test.http_error
            def getresponse(self):
                return SimpleNamespace(status=test.status, read=lambda n: test.raw[:n])
            def close(self): test.closed.append(True)
        p = patch.object(live.http.client, 'HTTPSConnection', Connection)
        p.start(); self.addCleanup(p.stop)
        self.port = self.build()

    def build(self, **kw):
        def claim(request, token):
            if self.persisted != request or self.persisted.get('transport_claim') is not None:
                raise ValueError('CLAIM_REFUSED')
            self.persisted['transport_claim'] = token
            return deepcopy(self.persisted)
        args = dict(context_loader=lambda request: deepcopy(self.context),
            request_loader=lambda rid: deepcopy(self.persisted),
            release_loader=lambda: deepcopy(self.release), claim_transport=claim,
            budget=self.budget, clock=lambda: self.t)
        args.update(kw)
        return live.LiveDispatchPort(self.env, **args)

    def admission(self, request=None):
        request = request or self.request
        value = self.port.reserve_transport(request['proposal'])
        value.bind(request)
        return value

    def send(self, admission=None):
        return self.port.send(self.request, admission=admission or self.admission())

    def exit(self):
        p = self.request['proposal']; source = self.context['source']
        owner = dict(card_id=p['card_id'], card_digest=life.digest(source), account=p['account'],
            role=p['role'], symbol=p['symbol'], side='SHORT', planned_quantity='1000',
            prices=dict(entry='2', stop='2.01', take_profit='1.84'),
            orders=dict(ENTRY=['1'], STOP=[], TAKE_PROFIT=[]), environment='testnet')
        common = dict(account=p['account'], symbol=p['symbol'], oid='1')
        self.context.update(owner=owner, owner_snapshot=dict(environment='testnet',
            account=p['account'], symbol=p['symbol'], at_ms=self.t, history_complete=True,
            orders_complete=True, position_quantity='-1000', open_orders=[],
            terminal_orders=[dict(**common, state='FILLED', filled_quantity='1000', at_ms=self.t)],
            fills=[dict(**common, fill_id='f1', quantity='1000', price='2', at_ms=self.t,
                fee='0', fee_token='USDC', side='A')]))
        p.update(leg='STOP', operation='CREATE_EXIT')
        p['action']['orders'][0].update(b=True, p='2.01', r=True,
            t=dict(trigger=dict(isMarket=True, triggerPx='2.01', tpsl='sl')))
        self.persisted = deepcopy(self.request)

    def emergency(self, elapsed):
        self.exit(); self.t += elapsed
        p = self.request['proposal']; p['operation'] = 'EMERGENCY_CLOSE'
        p['sample'] = dict(mark_price='2', at_ms=self.t)
        p['observed_at_ms'] = self.t
        self.request.update(prepared_at_ms=self.t, attempt_at_ms=self.t, nonce=self.t)
        from .emergency_close import close_price
        p['action']['orders'][0].update(p=close_price('2', 1, buy=True), t=dict(limit=dict(tif='Ioc')))
        p['action'] = wire.canonical_wire_action(p['action'])
        self.context['owner_snapshot']['at_ms'] = self.t
        self.persisted = deepcopy(self.request)

    def test_default_off_constructs_no_signer_or_network(self):
        self.env.pop('HL_TESTNET_EXPERIMENTAL_DISPATCH')
        with self.assertRaisesRegex(live.LiveDispatchError, 'NOT_RELEASED'): self.build()
        self.assertEqual(self.http, []); self.key_loader.assert_not_called()

    def test_budget_must_be_actual_shared_budget_not_software_permit(self):
        with self.assertRaisesRegex(live.LiveDispatchError, 'SHARED_REQUEST_BUDGET'):
            self.build(budget=SimpleNamespace(acquire=lambda *a, **k: Permit()))

    def test_construction_does_not_access_secrets_or_network(self):
        self.key_loader.assert_not_called(); self.assertEqual(self.http, []); self.assertEqual(self.acquisitions, [])

    def test_real_wire_is_testnet_domain_native_canonical_and_one_request(self):
        reply = self.send()
        self.assertEqual(reply, dict(state='ACCEPTED_UNVERIFIED', code=None, oid='10'))
        host, timeout, method, path, body, headers = self.http[0]
        self.assertEqual((host, timeout, method, path), (live.HOST, 4, 'POST', '/exchange'))
        self.assertEqual(body['action'], self.request['proposal']['action'])
        self.assertEqual(set(body), {'action', 'nonce', 'signature', 'expiresAfter'})
        self.assertFalse(self.signed[0][-1]); self.assertIsNone(self.signed[0][2])
        self.assertEqual(self.port.sent, 1); self.assertEqual(len(self.closed), 1)
        self.assertEqual(self.acquisitions[0][2], dict(host=live.HOST, priority='background'))

    def test_missing_admission_cannot_mint_transport(self):
        with self.assertRaisesRegex(live.LiveDispatchError, 'SINGLE_USE'):
            self.port.send(self.request)
        self.assertEqual(self.acquisitions, []); self.key_loader.assert_not_called()

    def test_software_or_foreign_admission_cannot_reach_signer(self):
        admission = wire.TransportAdmission(self.request['proposal'], Permit()); admission.bind(self.request)
        with self.assertRaisesRegex(live.LiveDispatchError, 'SHARED_BUDGET_ADMISSION'):
            self.send(admission)
        self.key_loader.assert_not_called()

    def test_software_request_is_not_accepted_even_with_live_admission(self):
        self.request['domain'] = 'software'; self.persisted = deepcopy(self.request)
        with self.assertRaisesRegex(live.LiveDispatchError, 'DURABLE_TESTNET'):
            self.send()
        self.assertEqual(self.http, [])

    def test_missing_or_changed_durable_request_blocks(self):
        for change in (None, {**self.request, 'phase': 'OBSERVED'}):
            self.persisted = change
            with self.assertRaisesRegex(live.LiveDispatchError, 'EXACT_DURABLE'): self.send()
        self.key_loader.assert_not_called()

    def test_current_request_is_loaded_again_after_signing(self):
        self.sign_hook = lambda _: self.persisted.update(phase='OBSERVED')
        with self.assertRaisesRegex(live.DefinitelyNotSubmitted, 'EXACT_DURABLE'): self.send()
        self.assertEqual(self.http, [])

    def test_source_retired_during_signing_blocks_http(self):
        self.sign_hook = lambda _: self.context['current_source'].update(entry_permission='RETIRED')
        with self.assertRaisesRegex(live.DefinitelyNotSubmitted, 'SOURCE_ENTRY_PERMISSION'): self.send()
        self.assertEqual(self.http, [])

    def test_price_crossing_during_signing_blocks_http(self):
        self.sign_hook = lambda _: self.context['market'].update(mark_price='2.02')
        with self.assertRaisesRegex(live.DefinitelyNotSubmitted, 'OUTSIDE_FROZEN_EXITS'): self.send()
        self.assertEqual(self.http, [])

    def test_global_release_withdrawn_after_signing_blocks_http(self):
        self.sign_hook = lambda _: self.release.update(dispatch_enabled=False)
        with self.assertRaisesRegex(live.DefinitelyNotSubmitted, 'CURRENT_TESTNET_RELEASE'): self.send()
        self.assertEqual(self.http, [])

    def test_same_valid_release_replaced_during_signing_still_blocks(self):
        self.sign_hook = lambda _: self.release.update(release_id='e'*64)
        with self.assertRaisesRegex(live.DefinitelyNotSubmitted, 'CONTEXT_CHANGED'): self.send()
        self.assertEqual(self.http, [])

    def test_entry_halt_or_expiry_does_not_reserve_request(self):
        self.release['entries_enabled'] = False
        with self.assertRaisesRegex(live.LiveDispatchError, 'ENTRY_RELEASE_CLOSED'): self.send()
        self.release['entries_enabled'] = True; self.t += 60000
        with self.assertRaisesRegex(live.LiveDispatchError, 'ENTRY_RELEASE_CLOSED'): self.send()
        self.assertEqual(self.acquisitions, [])

    def test_protective_exit_works_while_entry_release_is_closed(self):
        self.exit(); self.release['entries_enabled'] = False
        self.release['entry_expires_at_ms'] = self.t-1
        self.context['current_source']['entry_permission'] = 'RETIRED'
        self.context['safety']['entry_enabled'] = False
        self.send()
        self.assertEqual(self.acquisitions[0][2]['priority'], 'protection')
        self.assertTrue(self.http[0][4]['action']['orders'][0]['r'])

    def test_exit_quantity_cannot_exceed_reconciled_owner(self):
        self.exit(); self.request['proposal']['quantity'] = '1000.1'
        self.request['proposal']['action']['orders'][0]['s'] = '1000.1'
        self.persisted = deepcopy(self.request)
        with self.assertRaisesRegex(live.DefinitelyNotSubmitted, 'OWN_REMAINING'): self.send()
        self.assertEqual(self.http, [])

    def test_caller_cannot_substitute_another_signer_environment(self):
        self.context['env']['HL_TESTNET_SHORT_AGENT_ADDRESS'] = '0x'+'5'*40
        with self.assertRaisesRegex(live.DefinitelyNotSubmitted, 'ROLE_ACCOUNT'): self.send()
        self.key_loader.assert_not_called()

    def test_signing_failure_redacts_secret_text(self):
        self.key_loader.side_effect = ValueError('secret-private-key-do-not-print')
        with self.assertRaisesRegex(live.DefinitelyNotSubmitted, '^ROLE_LOCAL_SIGNING_FAILED$'): self.send()
        self.assertEqual(self.http, [])

    def test_wrong_derived_signer_fails_closed(self):
        self.wallet.address = '0x'+'5'*40
        with self.assertRaisesRegex(live.DefinitelyNotSubmitted, 'SIGNING_FAILED'): self.send()
        self.assertEqual(self.signed, [])

    def test_signer_cannot_mutate_frozen_action(self):
        self.sign_hook = lambda action: action['orders'][0].update(s='900')
        with self.assertRaisesRegex(live.DefinitelyNotSubmitted, 'SIGNER_MUTATED'): self.send()
        self.assertEqual(self.http, [])

    def test_slow_signing_cannot_extend_evidence_or_attempt_lifetime(self):
        self.sign_hook = lambda _: setattr(self, 't', self.t+5001)
        with self.assertRaisesRegex(live.DefinitelyNotSubmitted, 'DURABLE_ATTEMPT_EXPIRED'): self.send()
        self.assertEqual(self.http, [])

    def test_response_loss_keeps_unknown_and_cannot_retry_same_admission(self):
        self.http_error = TimeoutError('untrusted-secret-response')
        admission = self.admission()
        with self.assertRaisesRegex(live.LiveDispatchError, '^TRANSPORT_OUTCOME_UNKNOWN$'): self.send(admission)
        with self.assertRaisesRegex(live.LiveDispatchError, 'EXACT_DURABLE'): self.send(admission)
        self.assertEqual(len(self.http), 1); self.assertEqual(len(self.closed), 1)
        self.assertEqual(self.persisted['phase'], 'OUTCOME_UNKNOWN')

    def test_new_admission_does_not_retry_previously_attempted_request(self):
        self.send()
        with self.assertRaisesRegex(live.LiveDispatchError, 'EXACT_DURABLE'): self.send()
        self.assertEqual(len(self.http), 1)

    def test_bad_status_or_oversize_or_invalid_json_never_returns_success(self):
        for status, raw in ((500, b'{}'), (200, b'not json'), (200, b'x'*(checks.MAX_BYTES+1))):
            with self.subTest(status=status, size=len(raw)):
                self.persisted = deepcopy(self.request)
                self.port = self.build(); self.status = status; self.raw = raw
                with self.assertRaisesRegex(live.LiveDispatchError, 'TRANSPORT_OUTCOME_UNKNOWN'): self.send()

    def test_exchange_human_text_is_not_persisted_or_returned(self):
        self.raw = json.dumps(dict(status='err', response='untrusted-private-key')).encode()
        reply = self.send()
        self.assertNotIn('venue_reason', reply); self.assertNotIn('untrusted', str(reply))

    def test_restart_and_new_admission_cannot_replay_claimed_request(self):
        self.send()
        self.port = self.build(); self.request = deepcopy(self.persisted)
        with self.assertRaisesRegex(live.LiveDispatchError, 'ALREADY_CLAIMED'): self.send()
        self.assertEqual(len(self.http), 1)

    def test_unknown_claim_commit_never_signs_or_certifies_unsent(self):
        def unknown_commit(request, token):
            self.persisted['transport_claim'] = token
            raise TimeoutError('COMMIT_ACK_LOST')
        self.port = self.build(claim_transport=unknown_commit)
        with self.assertRaisesRegex(live.LiveDispatchError, 'CLAIM_UNCONFIRMED'): self.send()
        self.key_loader.assert_not_called(); self.assertEqual(self.http, [])
        self.port = self.build(); self.request = deepcopy(self.persisted)
        with self.assertRaisesRegex(live.LiveDispatchError, 'ALREADY_CLAIMED'): self.send()

    def test_certified_local_failure_binds_exact_claim_but_not_other_attempt(self):
        self.context['market']['mark_price'] = '2.02'
        with self.assertRaises(live.DefinitelyNotSubmitted) as caught: self.send()
        self.assertTrue(caught.exception.matches(self.persisted))
        changed = deepcopy(self.persisted); changed['transport_claim'] = 'f'*32
        self.assertFalse(caught.exception.matches(changed))
        self.assertEqual(self.http, [])

    def test_read_failure_after_acknowledged_claim_certifies_only_unsent_claim(self):
        calls = []
        def read(rid):
            calls.append(rid)
            if len(calls) > 1: raise OSError('private database connection details')
            return deepcopy(self.persisted)
        self.port = self.build(request_loader=read)
        with self.assertRaisesRegex(live.DefinitelyNotSubmitted, '^DURABLE_REQUEST_READ_FAILED$') as caught:
            self.send()
        self.assertTrue(caught.exception.matches(self.persisted))
        self.key_loader.assert_not_called(); self.assertEqual(self.http, [])

    def test_uncovered_deadline_allows_exact_reduce_only_close_without_price_crossing(self):
        self.emergency(5000)
        reply = self.send()
        order = self.http[0][4]['action']['orders'][0]
        self.assertEqual(reply['state'], 'ACCEPTED_UNVERIFIED')
        self.assertTrue(order['r']); self.assertTrue(order['b'])
        self.assertEqual(order['s'], '1000'); self.assertEqual(order['t'], {'limit': {'tif': 'Ioc'}})

    def test_uncovered_deadline_does_not_close_early_or_trust_proposal_reason(self):
        self.emergency(4999)
        self.request['proposal']['emergency_reason'] = 'STOP_VERIFICATION_DEADLINE'
        self.persisted = deepcopy(self.request)
        with self.assertRaisesRegex(live.DefinitelyNotSubmitted, 'CROSSED_FROZEN_EXIT_OR_UNCOVERED_DEADLINE'):
            self.send()
        self.assertEqual(self.http, [])

    def test_observed_full_stop_coverage_defeats_claimed_uncovered_deadline(self):
        self.emergency(5000)
        owner = self.context['owner']; owner['orders']['STOP'] = ['2']
        self.context['owner_snapshot']['open_orders'] = [dict(account=owner['account'], symbol='XRP',
            oid='2', quantity='1000', price='2.01', trigger_price='2.01', side='B',
            reduce_only=True, state='ACTIVE', order_type='SL_MARKET')]
        with self.assertRaisesRegex(live.DefinitelyNotSubmitted, 'CROSSED_FROZEN_EXIT_OR_UNCOVERED_DEADLINE'):
            self.send()
        self.assertEqual(self.http, [])

    def test_deadline_close_requires_independent_owner_snapshot_within_five_seconds(self):
        self.emergency(10001)
        self.context['owner_snapshot']['at_ms'] -= 5001
        with self.assertRaisesRegex(live.DefinitelyNotSubmitted, 'EMERGENCY_OWNER_SNAPSHOT_EXPIRED'):
            self.send()
        self.assertEqual(self.http, [])

    def test_new_partial_fill_cannot_reset_old_uncovered_deadline(self):
        self.emergency(5000)
        snap = self.context['owner_snapshot']; first = snap['fills'][0]
        first['quantity'] = '600'
        snap['fills'].append({**first, 'fill_id': 'f2', 'quantity': '400', 'at_ms': self.t})
        self.send()
        self.assertEqual(len(self.http), 1)

    def test_coverage_of_old_tranche_leaves_younger_uncovered_fill_its_own_deadline(self):
        self.emergency(5000)
        owner = self.context['owner']; snap = self.context['owner_snapshot']; first = snap['fills'][0]
        first['quantity'] = '600'
        snap['fills'].append({**first, 'fill_id': 'f2', 'quantity': '400', 'at_ms': self.t})
        owner['orders']['STOP'] = ['2']
        snap['open_orders'] = [dict(account=owner['account'], symbol='XRP', oid='2',
            quantity='600', price='2.01', trigger_price='2.01', side='B',
            reduce_only=True, state='ACTIVE', order_type='SL_MARKET')]
        with self.assertRaisesRegex(live.DefinitelyNotSubmitted, 'CROSSED_FROZEN_EXIT_OR_UNCOVERED_DEADLINE'):
            self.send()
        self.assertEqual(self.http, [])

    def test_uncovered_deadline_never_bypasses_working_entry_remainder(self):
        self.emergency(5000)
        owner = self.context['owner']; snap = self.context['owner_snapshot']
        snap['fills'][0]['quantity'] = '500'; snap['position_quantity'] = '-500'
        snap['terminal_orders'] = []
        snap['open_orders'] = [dict(account=owner['account'], symbol='XRP', oid='1',
            quantity='500', price='2', trigger_price=None, side='A',
            reduce_only=False, state='ACTIVE', order_type='LIMIT')]
        self.request['proposal']['quantity'] = '500'
        self.request['proposal']['action']['orders'][0]['s'] = '500'
        self.persisted = deepcopy(self.request)
        with self.assertRaisesRegex(live.DefinitelyNotSubmitted, 'EMERGENCY_ENTRY_REMAINDER_NOT_FINAL'):
            self.send()
        self.assertEqual(self.http, [])


class ActualSDKSigningTests(unittest.TestCase):
    def test_pinned_sdk_signature_recovers_only_exact_testnet_action_and_nonce(self):
        try:
            version = importlib.metadata.version('hyperliquid-python-sdk')
        except importlib.metadata.PackageNotFoundError:
            self.skipTest('Run with the repository Testnet audit venv for actual SDK signing')
        self.assertEqual(version, '0.24.0')
        from eth_account import Account
        from hyperliquid.utils.signing import recover_agent_or_user_from_l1_action
        # Public synthetic fixture key 1, never a user or exchange credential.
        fixture_key = '0x'+'0'*63+'1'
        agent = Account.from_key(fixture_key).address.lower()
        request, context, now = fixture(); request['domain'] = 'testnet'
        env = {**context['env'], 'HL_TESTNET_EXPERIMENTAL_DISPATCH': live.APPROVAL,
            'HL_TESTNET_SHORT_AGENT_ADDRESS': agent, 'HL_TESTNET_SHORT_AGENT_KEY': fixture_key}
        context['env'] = {k:v for k,v in env.items() if not k.endswith('_KEY')}
        context['agent'] = agent
        release = dict(domain='testnet', release_id='d'*64, dispatch_enabled=True,
            protection_enabled=True, entries_enabled=True,
            not_before_ms=context['safety']['not_before_ms'], entry_expires_at_ms=now+60000,
            routes={role: live.roles.route_for(env, role)['account'] for role in live.roles.ROLES})
        persisted = deepcopy(request); sent = []
        def claim(value, token):
            if persisted != value or persisted.get('transport_claim') is not None: raise ValueError()
            persisted['transport_claim'] = token
            return deepcopy(persisted)
        class HTTP:
            def __init__(self, host, timeout):
                if host != live.HOST or timeout != 4: raise AssertionError('TESTNET_ONLY')
            def request(self, method, path, body, headers):
                if (method, path) != ('POST', '/exchange'): raise AssertionError('EXACT_EXCHANGE')
                sent.append(json.loads(body))
            def getresponse(self):
                return SimpleNamespace(status=200, read=lambda _: json.dumps(dict(status='ok',
                    response=dict(type='order', data=dict(statuses=[dict(resting=dict(oid=10))])))).encode())
            def close(self): pass
        with patch('socket.create_connection', side_effect=AssertionError('NO_NETWORK')), \
                patch('socket.socket.connect', side_effect=AssertionError('NO_NETWORK')), \
                patch.object(live.http.client, 'HTTPSConnection', HTTP), \
                patch.object(request_budget.Budget, 'acquire', return_value=Permit()):
            port = live.LiveDispatchPort(env, context_loader=lambda _: deepcopy(context),
                request_loader=lambda _: deepcopy(persisted), release_loader=lambda: deepcopy(release),
                claim_transport=claim, budget=object.__new__(request_budget.Budget), clock=lambda: now)
            admission = port.reserve_transport(request['proposal']); admission.bind(request)
            self.assertEqual(port.send(request, admission=admission)['state'], 'ACCEPTED_UNVERIFIED')
        self.assertEqual(len(sent), 1)
        body = sent[0]
        def recovered(action, nonce, mainnet):
            return recover_agent_or_user_from_l1_action(action, body['signature'], None,
                nonce, body['expiresAfter'], mainnet).lower()
        self.assertEqual(recovered(body['action'], body['nonce'], False), agent)
        self.assertNotEqual(recovered(body['action'], body['nonce'], True), agent)
        self.assertNotEqual(recovered(body['action'], body['nonce']+1, False), agent)
        changed = deepcopy(body['action']); changed['orders'][0]['s'] = '999'
        self.assertNotEqual(recovered(changed, body['nonce'], False), agent)


if __name__ == '__main__': unittest.main()
