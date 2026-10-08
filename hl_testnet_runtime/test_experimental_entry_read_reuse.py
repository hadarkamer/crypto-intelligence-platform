"""Collection-local admission reuse; every transport and signer is isolated."""
from collections import Counter
from copy import deepcopy
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import approved_alert_contract as contract
from approved_alert_fixtures import maxpain_alert
from . import experimental_execution_runtime as runtime
from .experimental_live_provider import ProviderError
from .experimental_plan_store import reduce_source
from . import test_experimental_live_provider as provider_fixtures
from .test_experimental_execution_runtime import T


class EntryReadReuseTests(unittest.TestCase):
    setUp = provider_fixtures.ProviderTests.setUp

    def candidate(self, *, symbol='HYPE', side='LONG', cycle='first'):
        message = maxpain_alert(approved_ms=T)
        message.update(source_at=contract.iso_ms(T-240000+len(self.state['sources'])*60000),
            created_at=contract.iso_ms(T-230000+len(self.state['sources'])*60000))
        message.update(symbol=symbol, side=side,
            rule_id=next(k for k,v in contract.SPECS.items() if v[0] == symbol))
        if side == 'SHORT':
            message['stop'], message['take_profit'] = message['take_profit'], message['stop']
        message['policy']['source_price'] = f'HYPERLIQUID_{symbol}_PERPETUAL_TRADE_1M'
        message['proof']['cycle_id'] = cycle
        message['occurrence_id'] = contract.occurrence_id(message)
        message = contract.validate(message)
        record, _ = reduce_source(None, message, now=contract.iso_ms(self.exchange.t),
            not_before=contract.iso_ms(self.state['not_before_ms']), domain='testnet')
        self.state['sources'][message['occurrence_id']] = record
        account = self.state['routes']['long_account' if side == 'LONG' else 'short_account']
        self.exchange.mark[runtime._lane(account, symbol)] = message['entry']
        self.provider.safety = object()
        return message['occurrence_id']

    def collect(self):
        return self.provider.collect(self.state, entries_enabled=True)

    def test_multiple_candidates_share_only_fresh_account_binding_and_allowance(self):
        first=self.candidate();second=self.candidate(symbol='SOL',cycle='second')
        third=self.candidate(cycle='third')
        self.exchange.mark[runtime._lane(self.state['routes']['long_account'],'HYPE')]='99'
        result=self.collect()
        self.assertEqual(set(result['entry_accounts']),{first,second})
        self.assertEqual(result['entry_blocked'],{third:'ENTRY_DEFERRED_TO_NEXT_CYCLE'})
        self.assertEqual(Counter(c[0] for c in self.raw.calls),Counter(
            ['frontendOpenOrders','clearinghouseState','metaAndAssetCtxs','userRole','userRateLimit']))
        result['entry_accounts'][first]['action_headroom']=0
        self.assertEqual(result['entry_accounts'][second]['action_headroom'],1000)

    def test_only_first_admissible_account_is_prepared(self):
        first=self.candidate();second=self.candidate(side='SHORT',cycle='second')
        result=self.collect()
        self.assertEqual(set(result['entry_accounts']),{first})
        self.assertEqual(result['entry_blocked'],{second:'ENTRY_DEFERRED_TO_NEXT_CYCLE'})
        self.assertEqual(Counter(c[0] for c in self.raw.calls),Counter(
            ['frontendOpenOrders','clearinghouseState']*2+
            ['metaAndAssetCtxs','userRole','userRateLimit']))
        self.state['sources'].pop(first);self.raw.calls.clear()
        result=self.collect()
        self.assertEqual(set(result['entry_accounts']),{second})
        self.assertEqual(result['entry_blocked'],{})
        self.assertEqual(len(self.raw.calls),4)
        self.assertEqual(result['entry_accounts'][second]['agent'],
            self.provider.env['HL_TESTNET_SHORT_AGENT_ADDRESS'])

    def test_continuous_release_unapproved_source_cannot_defer_later_approved_alert(self):
        from experimental_execution_fixtures import maxpain_message
        from .experimental_live_release import CONTINUOUS_ENTRY
        self.state['not_before_ms']=T-360000
        legacy=maxpain_message(created_ms=T-300000)
        cid=legacy['occurrence_id']
        record,_=reduce_source(None,legacy,now=contract.iso_ms(T-300000),
            not_before=contract.iso_ms(self.state['not_before_ms']),domain='testnet')
        for stamp in [*range(T-240000,self.exchange.t,60000),self.exchange.t]:
            heartbeat=maxpain_message(created_ms=T-300000,as_of_ms=stamp,kind='HEARTBEAT')
            record,_=reduce_source(record,heartbeat,now=contract.iso_ms(stamp),
                not_before=contract.iso_ms(self.state['not_before_ms']),domain='testnet')
        self.state['sources'][cid]=record
        approved=self.candidate(symbol='SOL')
        account=self.state['routes']['long_account']
        self.exchange.mark[runtime._lane(account,'HYPE')]='99'
        paths=self.exchange.collect(self.state)['ranges'][cid]
        self.provider.prices=SimpleNamespace(source_range=lambda *args:paths['source'],
            mark_window=lambda *args:paths['testnet'])
        release=dict(domain='testnet',release_id='a'*64,dispatch_enabled=True,
            protection_enabled=True,entries_enabled=True,routes=self.state['routes'],
            not_before_ms=self.state['not_before_ms'],entry_policy=CONTINUOUS_ENTRY,
            entry_expires_at_ms=None)
        self.provider.safety=SimpleNamespace(release_loader=lambda:release)
        result=self.collect()
        self.assertIn(cid,result['entry_accounts'])  # It reached the unchanged source-policy gate.
        self.assertIn(approved,result['entry_accounts'])
        self.assertNotIn(approved,result['entry_blocked'])

    def test_each_collection_refreshes_identity_allowance_and_account_inventory(self):
        first=self.candidate();self.collect();self.raw.calls.clear()
        original=self.raw.read
        def changed(kind,*args,**kwargs):
            result=original(kind,*args,**kwargs)
            if kind=='userRateLimit':result['nRequestsUsed']=100
            return result
        with patch.object(self.raw,'read',side_effect=changed):result=self.collect()
        self.assertEqual(result['entry_accounts'][first]['action_headroom'],900)
        self.assertEqual(len(self.raw.calls),4)
        self.state['revision']+=1;self.raw.extra_orders=[dict(coin='DOGE',oid=999)]
        changed=self.collect()
        self.assertIn(first,changed['entry_blocked'])
        self.assertNotIn(first,changed['entry_accounts'])

    def test_failed_or_wrong_account_binding_is_not_reused(self):
        first=self.candidate();second=self.candidate(cycle='second')
        original=self.raw.read;attempts=[]
        def fail_once(kind,*args,**kwargs):
            result=original(kind,*args,**kwargs)
            if kind=='userRole':
                attempts.append(kind)
                if len(attempts)==1:result['data']['user']=self.state['routes']['short_account']
            return result
        with patch.object(self.raw,'read',side_effect=fail_once):result=self.collect()
        self.assertEqual(result['entry_blocked'][first],'AGENT_ACCOUNT_MISMATCH')
        self.assertIn(second,result['entry_accounts'])
        self.assertEqual(len(attempts),2)
        recovered=self.collect()
        self.assertEqual(set(recovered['entry_accounts']),{first})

    def test_cached_binding_preserves_original_observation_time(self):
        first=self.candidate();second=self.candidate(cycle='second')
        self.exchange.mark[runtime._lane(self.state['routes']['long_account'],'HYPE')]='99'
        observed=[]
        def tick(kind):
            observed.append((kind,self.exchange.t));self.exchange.t+=10
        self.raw.mutate=tick;result=self.collect()
        first_at=result['entry_accounts'][first]['at_ms']
        self.assertEqual(first_at,result['entry_accounts'][second]['at_ms'])
        self.assertEqual(first_at,next(at for kind,at in observed if kind=='userRole'))
        self.assertLess(first_at,self.exchange.t)

    def test_bad_short_binding_does_not_poison_other_account_or_later_candidate(self):
        first_long=self.candidate(symbol='SOL')
        failed_short=self.candidate(side='SHORT',cycle='short-first')
        second_long=self.candidate(symbol='SOL',cycle='long-second')
        recovered_short=self.candidate(side='SHORT',cycle='short-second')
        self.exchange.mark[runtime._lane(self.state['routes']['long_account'],'SOL')]='99'
        original=self.raw.read;attempts=[]
        def malformed_once(kind,*args,**kwargs):
            value=original(kind,*args,**kwargs)
            if kind=='userRole' and kwargs['user']==self.provider.env['HL_TESTNET_SHORT_AGENT_ADDRESS']:
                attempts.append(kind)
                if len(attempts)==1:value['data']={}
            return value
        with patch.object(self.raw,'read',side_effect=malformed_once):result=self.collect()
        self.assertEqual(set(result['entry_blocked']),{failed_short})
        self.assertEqual(set(result['entry_accounts']),{first_long,second_long,recovered_short})
        self.assertEqual(len(attempts),2)
        self.assertEqual(sum(c[0]=='userRole' for c in self.raw.calls),3)

    def test_slow_collection_still_expires_and_does_not_reuse_next_pass(self):
        self.candidate();self.candidate(cycle='second')
        def expire(kind):
            if kind=='userRateLimit':self.exchange.t+=15001
        self.raw.mutate=expire;expired=self.collect()
        self.assertEqual(expired['inventory_accounts'],[])
        self.assertEqual(set(expired['account_entry_blocked'].values()),{'ACCOUNT_COLLECTION_EXPIRED'})
        self.assertEqual(expired['snapshots'],[])
        self.raw.mutate=None;self.raw.calls.clear();result=self.collect()
        self.assertEqual(len(result['entry_accounts']),1)
        self.assertEqual(len(self.raw.calls),5)

    def test_entry_keeps_order_allowance_for_protection_without_balance_queries(self):
        first=self.candidate();original=self.raw.read
        def low_allowance(kind,*args,**kwargs):
            if kind in ('activeAssetData','spotClearinghouseState','userAbstraction'):
                raise AssertionError('NO_FINANCIAL_PREFLIGHT')
            value=original(kind,*args,**kwargs)
            if kind=='clearinghouseState':value['withdrawable']='0'
            if kind=='userRateLimit':value['nRequestsUsed']=999
            return value
        with patch.object(self.raw,'read',side_effect=low_allowance):result=self.collect()
        self.assertEqual(result['entry_blocked'][first],'ENTRY_ACTION_HEADROOM_INSUFFICIENT')
        self.assertNotIn(first,result['entry_accounts'])

    def test_available_margin_is_decided_by_exchange_not_an_extra_lab_model(self):
        first=self.candidate();original=self.raw.read
        def no_local_funds(kind,*args,**kwargs):
            if kind in ('activeAssetData','spotClearinghouseState','userAbstraction'):
                raise AssertionError('NO_FINANCIAL_PREFLIGHT')
            value=original(kind,*args,**kwargs)
            if kind=='clearinghouseState':value['withdrawable']='0'
            return value
        with patch.object(self.raw,'read',side_effect=no_local_funds):result=self.collect()
        self.assertEqual(set(result['entry_accounts']),{first})
        self.assertEqual(result['entry_blocked'],{})
        self.assertEqual(len(self.raw.calls),5)

    def test_stalled_entry_collection_cannot_block_protection_collection(self):
        self.candidate()
        entered, release = threading.Event(), threading.Event()
        completed, errors = [], []
        def stall(kind):
            if kind == 'userRole' and threading.current_thread().name == 'entry-fixture':
                entered.set()
                if not release.wait(3):
                    raise AssertionError('FIXTURE_WAIT_TIMED_OUT')
        self.raw.mutate = stall
        def run(entries):
            try:
                completed.append((entries, self.provider.collect(deepcopy(self.state), entries_enabled=entries)))
            except BaseException as exc:
                errors.append(exc)
        entry = threading.Thread(target=run, args=(True,), name='entry-fixture', daemon=True)
        protection = threading.Thread(target=run, args=(False,), name='protection-fixture', daemon=True)
        entry.start()
        try:
            self.assertTrue(entered.wait(2))
            protection.start()
            protection.join(timeout=2)
            self.assertFalse(protection.is_alive())
            self.assertTrue(entry.is_alive())
            self.assertEqual([enabled for enabled,_ in completed], [False])
        finally:
            release.set()
            entry.join(timeout=3)
            if protection.ident is not None:
                protection.join(timeout=3)
        self.assertFalse(entry.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(completed), 2)

    def test_idle_collections_read_each_account_once_without_unused_market_read(self):
        self.provider.collect(self.state)
        self.assertEqual(len(self.raw.calls), 4)
        self.provider.collect(self.state)
        self.assertEqual(len(self.raw.calls), 8)
        self.assertNotIn('metaAndAssetCtxs',[c[0] for c in self.raw.calls])


if __name__ == '__main__':
    unittest.main()
