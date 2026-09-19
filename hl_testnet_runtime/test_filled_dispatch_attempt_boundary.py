"""Final adapter-boundary tests. Network, user keys and real signing forbidden.

A fake signing module below only advances a synthetic clock and returns a dummy
value. It does not compute a signature. The only HTTP class used is a local double.
"""
from copy import deepcopy
import json
import sys
from types import ModuleType
import unittest
from unittest.mock import patch

from . import filled_quantity_dispatch as m
from . import test_filled_quantity_dispatch as fixtures
from .filled_dispatch_store import DispatchError

T=fixtures.T


def request():
    state=fixtures.state_from_case()
    proposal=m.choose(state,fixtures.ROUTES2,fixtures.META,
        dict(mark_price='10',at_ms=T),now_ms=T)
    return dict(proposal=proposal,domain='testnet',phase='OUTCOME_UNKNOWN',attempts=1,
        prepared_at_ms=T+1,attempt_at_ms=T+2,nonce=T+2)


def dummy_signer(clock,delay):
    module=ModuleType('hyperliquid.utils.signing')
    module.calls=[]
    def fake(*args):
        module.calls.append(args)
        clock[0]+=delay
        return dict(r='NOT_A_SIGNATURE',s='NOT_A_SIGNATURE',v=0)
    module.sign_l1_action=fake
    return module


class FinalAttemptBoundaryTests(fixtures.NoExternal):
    def venue(self,clock):
        venue=m.TestnetVenue({})
        venue.now=lambda:clock[0]
        return venue

    def test_fresh_request_passes_without_mutation_or_io(self):
        value=request();before=deepcopy(value)
        venue=self.venue([T+3]);venue._fresh_attempt(value)
        self.assertEqual(value,before);self.assertEqual(venue.sent,0)

    def test_false_domain_phase_or_boolean_attempt_count_rejected(self):
        for field,value in (('domain','software'),('domain','mainnet'),
                ('phase','PREPARED'),('phase','OBSERVED'),('attempts',True),
                ('attempts',0),('attempts',2)):
            candidate=request();candidate[field]=value
            with self.subTest(field=field,value=value):
                with self.assertRaisesRegex(DispatchError,'^DURABLE_TESTNET_REQUEST_REQUIRED$'):
                    self.venue([T+3])._fresh_attempt(candidate)

    def test_expired_attempt_rejected(self):
        with self.assertRaisesRegex(DispatchError,'^DURABLE_ATTEMPT_EXPIRED$'):
            self.venue([T+5003])._fresh_attempt(request())

    def test_clock_backwards_and_future_observation_rejected(self):
        with self.assertRaisesRegex(DispatchError,'^DURABLE_ATTEMPT_EXPIRED$'):
            self.venue([T+1])._fresh_attempt(request())
        value=request();value['proposal']['observed_at_ms']=T+3
        with self.assertRaisesRegex(DispatchError,'^DURABLE_ATTEMPT_EXPIRED$'):
            self.venue([T+4])._fresh_attempt(value)
        value=request();value['prepared_at_ms']=T+3
        with self.assertRaisesRegex(DispatchError,'^DURABLE_ATTEMPT_EXPIRED$'):
            self.venue([T+4])._fresh_attempt(value)

    def test_recent_attempt_does_not_refresh_old_evidence(self):
        value=request();value.update(prepared_at_ms=T+14999,attempt_at_ms=T+15000,nonce=T+15000)
        with self.assertRaisesRegex(DispatchError,'^FINAL_EVIDENCE_EXPIRED$'):
            self.venue([T+15001])._fresh_attempt(value)
        # Existing 15-second bound is retained, not silently shortened.
        value.update(attempt_at_ms=T+14999,nonce=T+14999)
        self.venue([T+15000])._fresh_attempt(value)

    def test_nonce_must_match_persisted_attempt_time_window(self):
        for nonce in (T+1,T+1003):
            value=request();value['nonce']=nonce
            with self.assertRaisesRegex(DispatchError,'^PERSISTED_NONCE_TIME_INVALID$'):
                self.venue([T+3])._fresh_attempt(value)

    def test_missing_or_invalid_timestamps_have_fixed_error(self):
        for field in ('prepared_at_ms','attempt_at_ms','nonce'):
            for bad in (None,True,'123',-1):
                value=request();value[field]=bad
                with self.assertRaisesRegex(DispatchError,'^DURABLE_ATTEMPT_TIMELINE_REQUIRED$'):
                    self.venue([T+3])._fresh_attempt(value)
        value=request();del value['proposal']['observed_at_ms']
        with self.assertRaisesRegex(DispatchError,'^DURABLE_ATTEMPT_TIMELINE_REQUIRED$'):
            self.venue([T+3])._fresh_attempt(value)

    def test_real_send_path_rechecks_evidence_before_wallet_access(self):
        value=request();value.update(prepared_at_ms=T+14999,attempt_at_ms=T+15000,nonce=T+15000)
        venue=self.venue([T+15001])
        with patch.object(venue,'_gate',return_value=fixtures.ROUTES2['long_account']),\
             patch.object(m.roles,'wallet_for_role',side_effect=AssertionError('NO_KEY_ACCESS')) as wallet:
            with self.assertRaisesRegex(DispatchError,'^FINAL_EVIDENCE_EXPIRED$'):
                venue.send(value)
            wallet.assert_not_called()
        self.assertEqual(venue.sent,0)

    def test_slow_dummy_local_preparation_is_blocked_before_http(self):
        clock=[T+3];venue=self.venue(clock);fake=dummy_signer(clock,5001)
        with patch.object(venue,'_gate',return_value=fixtures.ROUTES2['long_account']),\
             patch.object(m.roles,'wallet_for_role',return_value=object()),\
             patch.dict(sys.modules,{'hyperliquid.utils.signing':fake}),\
             patch.object(m.http.client,'HTTPSConnection',side_effect=AssertionError('NO_HTTP')) as http:
            with self.assertRaisesRegex(DispatchError,'^DURABLE_ATTEMPT_EXPIRED$'):
                venue.send(request())
            http.assert_not_called()
        self.assertEqual(len(fake.calls),1);self.assertEqual(venue.sent,0)

    def test_evidence_can_expire_even_when_attempt_remains_recent(self):
        value=request();value.update(prepared_at_ms=T+14000,attempt_at_ms=T+14001,nonce=T+14001)
        clock=[T+14002];venue=self.venue(clock);fake=dummy_signer(clock,1000)
        with patch.object(venue,'_gate',return_value=fixtures.ROUTES2['long_account']),\
             patch.object(m.roles,'wallet_for_role',return_value=object()),\
             patch.dict(sys.modules,{'hyperliquid.utils.signing':fake}),\
             patch.object(m.http.client,'HTTPSConnection',side_effect=AssertionError('NO_HTTP')) as http:
            with self.assertRaisesRegex(DispatchError,'^FINAL_EVIDENCE_EXPIRED$'):
                venue.send(value)
            http.assert_not_called()
        self.assertEqual(venue.sent,0)

    def test_actual_adapter_serialization_uses_only_fixed_testnet_in_local_double(self):
        clock=[T+3];venue=self.venue(clock);fake=dummy_signer(clock,0);value=request()
        before=deepcopy(value);calls=[]
        class Connection:
            status=200
            def request(self,method,path,body,headers):
                calls.append((method,path,json.loads(body),headers))
            def getresponse(self):return self
            def read(self,limit):
                return b'{"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":9001}}]}}}'
            def close(self):pass
        with patch.object(venue,'_gate',return_value=fixtures.ROUTES2['long_account']),\
             patch.object(m.roles,'wallet_for_role',return_value=object()),\
             patch.dict(sys.modules,{'hyperliquid.utils.signing':fake}),\
             patch.object(m.http.client,'HTTPSConnection',return_value=Connection()) as http:
            result=venue.send(value)
            http.assert_called_once_with('api.hyperliquid-testnet.xyz',timeout=4)
        self.assertEqual(value,before);self.assertEqual(len(calls),1)
        self.assertEqual(calls[0][0:2],('POST','/exchange'))
        self.assertEqual(calls[0][2]['action'],value['proposal']['action'])
        self.assertEqual(calls[0][2]['expiresAfter'],value['nonce']+15000)
        self.assertIs(fake.calls[0][-1],False)
        self.assertEqual(m.normalized_reply(result,'order')['state'],'ACCEPTED_UNVERIFIED')


if __name__=='__main__':unittest.main()
