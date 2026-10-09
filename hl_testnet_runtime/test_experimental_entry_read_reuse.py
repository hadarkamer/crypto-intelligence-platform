"""Bounded admission reuse; every transport and signer is isolated."""
from collections import Counter
from copy import deepcopy
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import approved_alert_contract as contract
from approved_alert_fixtures import maxpain_alert
from . import experimental_execution_runtime as runtime
from .experimental_live_provider import ProviderError
from .experimental_plan_store import reduce_source
from .request_budget import request_weight
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

        # A filled short owns the next stop action. A candidate-only long
        # account must consume neither preparation time nor exchange reads.
        from .experimental_live_runtime import TestnetExecutionRuntime
        planner=object.__new__(TestnetExecutionRuntime)
        for obstruction in ('none','inventory_failure','unknown_entry','rejection_circuit'):
            with self.subTest(protection_obstruction=obstruction):
                active=provider_fixtures.ProviderTests.active(self,steps=1)
                pending=self.candidate(side='LONG',cycle=obstruction)
                original=deepcopy(self.state['sources'][pending])
                self.provider._market=None;self.provider._entry_checks.clear()
                self.raw.calls.clear();self.raw.mutate=None
                if obstruction=='inventory_failure':
                    self.raw.mutate=Mock(side_effect=[ProviderError('PUBLIC_READ_UNAVAILABLE')]+[None]*20)
                elif obstruction=='unknown_entry':
                    self.exchange.orders.clear()
                elif obstruction=='rejection_circuit':
                    context=self.provider.collect(self.state,entries_enabled=False)
                    projected=deepcopy(self.state)
                    stop=planner._cycle_proposal(projected,context,self.exchange.t,entries_enabled=False)
                    self.assertEqual((stop['operation'],stop['leg']),('CREATE_EXIT','STOP'))
                    projected['trades'][active['occurrence_id']]['rejection_circuit']={
                        planner._rejection_key(stop):'isolated_rejected_stop'}
                    self.state.clear();self.state.update(projected)
                    self.raw.calls.clear()
                result=self.collect()
                proposal=planner._cycle_proposal(deepcopy(self.state),result,self.exchange.t,entries_enabled=True)
                self.assertEqual(self.state['sources'][pending],original)
                if obstruction=='none':
                    self.assertEqual((proposal['operation'],proposal['leg'],proposal['role']),
                        ('CREATE_EXIT','STOP','short_account'))
                    self.assertNotIn(pending,result['entry_accounts'])
                    self.assertEqual(result['entry_blocked'][pending],
                        'ENTRY_DEFERRED_FOR_OWNED_PROTECTION_AND_RECONCILIATION')
                    self.assertEqual([row[0] for row in self.raw.calls],
                        ['frontendOpenOrders','clearinghouseState','lookup','userFillsByTime','metaAndAssetCtxs'])
                    self.assertFalse(any(row[1]==self.state['routes']['long_account']
                        for row in self.raw.calls))
                else:
                    self.assertIn(pending,result['entry_accounts'])
                    self.assertEqual((proposal['operation'],proposal['role']),('ENTRY','long_account'))
                if obstruction=='inventory_failure':
                    # A previously blocked lane is reconsidered from fresh
                    # evidence and immediately regains protection priority.
                    self.state['blocked_lanes']=deepcopy(result['blocked_lanes'])
                    self.raw.mutate=None;self.raw.calls.clear()
                    result=self.collect()
                    proposal=planner._cycle_proposal(deepcopy(self.state),result,self.exchange.t,entries_enabled=True)
                    self.assertEqual((proposal['operation'],proposal['leg']),('CREATE_EXIT','STOP'))
                    self.assertNotIn(pending,result['entry_accounts'])
                    self.assertFalse(any(row[1]==self.state['routes']['long_account']
                        for row in self.raw.calls))

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
        self.provider.budget=self.raw.budget=self.budget=SimpleNamespace(
            capacity=Mock(return_value=dict(eligible=True)))
        first=self.candidate();initial=self.collect();started=self.exchange.t
        account=self.state['routes']['long_account'];revision=initial['legacy_account_revisions'][account]
        self.assertEqual(sum(request_weight('/info',dict(type=c[0])) for c in self.raw.calls),122)
        self.budget.capacity.assert_called_with(requested_weight=123,priority='background')
        self.raw.calls.clear()
        original=self.raw.read
        def changed(kind,*args,**kwargs):
            result=original(kind,*args,**kwargs)
            if kind=='userRateLimit':result['nRequestsUsed']=100
            return result
        with patch.object(self.raw,'read',side_effect=changed):result=self.collect()
        self.assertEqual(result['entry_accounts'][first]['action_headroom'],1000)
        self.assertEqual([c[0] for c in self.raw.calls],['frontendOpenOrders','clearinghouseState'])
        self.assertEqual(sum(request_weight('/info',dict(type=c[0])) for c in self.raw.calls),22)
        self.budget.capacity.assert_called_with(requested_weight=23,priority='background')
        self.exchange.t=started+5000;self.raw.calls.clear();result=self.collect()
        self.assertEqual([c[0] for c in self.raw.calls],
            ['frontendOpenOrders','clearinghouseState','metaAndAssetCtxs'])
        self.assertEqual(sum(request_weight('/info',dict(type=c[0])) for c in self.raw.calls),42)
        self.budget.capacity.assert_called_with(requested_weight=43,priority='background')
        self.assertEqual(result['entry_accounts'][first]['signer_at_ms'],started)
        self.assertEqual(result['entry_accounts'][first]['allowance_at_ms'],started)
        self.assertEqual(result['entry_accounts'][first]['at_ms'],started+5000)
        self.exchange.t=started+59999;self.raw.calls.clear()
        self.assertEqual(self.provider._entry_account(account,'long_account',state=self.state,
            legacy_revision=revision,plan_only=True),[])
        self.assertEqual(self.provider._entry_account(account,'long_account',state=self.state,
            legacy_revision=revision)['action_headroom'],1000)
        self.assertEqual(self.raw.calls,[])
        self.exchange.t=started+60000
        self.assertEqual(self.provider._entry_account(account,'long_account',state=self.state,
            legacy_revision=revision,plan_only=True),['userRateLimit'])
        with patch.object(self.raw,'read',side_effect=changed):
            renewed=self.provider._entry_account(account,'long_account',state=self.state,
                legacy_revision=revision)
        self.assertEqual(renewed['action_headroom'],900)
        self.assertEqual(renewed['signer_at_ms'],started)
        self.assertEqual(renewed['allowance_at_ms'],started+60000)
        self.assertEqual([c[0] for c in self.raw.calls],['userRateLimit'])
        self.exchange.t=started+299999;self.raw.calls.clear()
        self.assertEqual(self.provider._entry_account(account,'long_account',state=self.state,
            legacy_revision=revision,plan_only=True),['userRateLimit'])
        self.provider._entry_account(account,'long_account',state=self.state,legacy_revision=revision)
        self.assertEqual([c[0] for c in self.raw.calls],['userRateLimit'])
        self.exchange.t=started+300000;self.raw.calls.clear()
        self.assertEqual(self.provider._entry_account(account,'long_account',state=self.state,
            legacy_revision=revision,plan_only=True),['userRole','userRateLimit'])
        renewed=self.provider._entry_account(account,'long_account',state=self.state,legacy_revision=revision)
        self.assertEqual(renewed['signer_at_ms'],started+300000)
        self.assertEqual([c[0] for c in self.raw.calls],['userRole','userRateLimit'])
        self.exchange.t=started;self.raw.calls.clear()
        self.assertEqual(self.provider._entry_account(account,'long_account',state=self.state,
            legacy_revision=revision,plan_only=True),['userRole','userRateLimit'])
        self.provider._entry_account(account,'long_account',state=self.state,legacy_revision=revision)
        self.state['revision']+=1;self.raw.extra_orders=[dict(coin='DOGE',oid=999)]
        changed=self.collect()
        self.assertNotIn(first,changed['entry_blocked'])
        self.assertIn(first,changed['entry_accounts'])
        self.assertEqual(changed['external_observation']['accounts'][account]['orders'][0]['origin'],'EXTERNAL')
        restarted=provider_fixtures.LiveEvidenceProvider(self.provider.env,legacy_store=self.legacy,
            experimental_store=self.store,price_evidence=self.provider.prices,budget=self.budget,
            clock=self.exchange.now,info_reader=self.raw,public_reader=self.raw,lookup_reader=self.raw.lookup)
        self.assertEqual(restarted._entry_account(account,'long_account',state=self.state,
            legacy_revision=revision,plan_only=True),['userRole','userRateLimit'])

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
        account=self.state['routes']['long_account'];revision=recovered['legacy_account_revisions'][account]
        old_agent=self.provider.env['HL_TESTNET_LONG_AGENT_ADDRESS'];new_agent='0x'+'9'*40
        with patch.dict(self.provider.env,HL_TESTNET_LONG_AGENT_ADDRESS=new_agent):
            self.assertEqual(self.provider._entry_account(account,'long_account',state=self.state,
                legacy_revision=revision,plan_only=True),['userRole','userRateLimit'])
            with patch.object(self.raw,'read',side_effect=[dict(role='agent',data=dict(user=account)),
                    dict(nRequestsCap=1000,nRequestsUsed=100,nRequestsSurplus=0)]) as reader:
                renewed=self.provider._entry_account(account,'long_account',state=self.state,
                    legacy_revision=revision)
            self.assertEqual(renewed['agent'],new_agent)
            self.assertEqual(renewed['action_headroom'],900)
            self.assertEqual(reader.call_count,2)
        self.assertEqual(self.provider.env['HL_TESTNET_LONG_AGENT_ADDRESS'],old_agent)
        self.assertEqual(self.provider._entry_account(account,'long_account',state=self.state,
            legacy_revision=revision,plan_only=True),['userRole','userRateLimit'])
        for invalid in (None,{},dict(role='user'),dict(role='agent',data=dict(user=account))):
            self.provider._entry_checks.clear()
            with patch.object(self.raw,'read',side_effect=[invalid,dict(nRequestsCap='1000',
                    nRequestsUsed=0,nRequestsSurplus=0)]):
                with self.assertRaises(ValueError):
                    self.provider._entry_account(account,'long_account',state=self.state,
                        legacy_revision=revision)
            self.assertNotIn('allowance_at_ms',self.provider._entry_checks.get(account,{}))
            value=self.provider._entry_account(account,'long_account',state=self.state,
                legacy_revision=revision)
            self.assertEqual(value['agent'],old_agent)
            self.assertEqual(value['action_headroom'],1000)

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
        first_value=deepcopy(result['entry_accounts'][first])
        self.assertEqual(first_value['signer_at_ms'],first_at)
        self.assertEqual(first_value['allowance_at_ms'],next(at for kind,at in observed if kind=='userRateLimit'))
        self.raw.calls.clear();observed.clear();self.exchange.t+=1000
        reused=self.collect()['entry_accounts'][first]
        self.assertGreater(reused['at_ms'],first_value['at_ms'])
        self.assertEqual(reused['signer_at_ms'],first_value['signer_at_ms'])
        self.assertEqual(reused['allowance_at_ms'],first_value['allowance_at_ms'])
        self.assertNotIn('userRole',[c[0] for c in self.raw.calls])
        self.assertNotIn('userRateLimit',[c[0] for c in self.raw.calls])

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
        long=self.state['routes']['long_account'];short=self.state['routes']['short_account']
        revisions=result['legacy_account_revisions'];self.raw.calls.clear()
        direct=deepcopy(self.state)
        direct['requests']['long-only']=dict(proposal=dict(account=long,action=dict(type='order',orders=[{}])),
            phase='OUTCOME_UNKNOWN')
        long_value=self.provider._entry_account(long,'long_account',state=direct,legacy_revision=revisions[long])
        short_value=self.provider._entry_account(short,'short_account',state=direct,legacy_revision=revisions[short])
        self.assertEqual(long_value['action_headroom'],995)
        self.assertEqual(short_value['action_headroom'],1000)
        self.assertEqual(self.raw.calls,[])

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
        self.assertEqual([c[0] for c in self.raw.calls],
            ['frontendOpenOrders','clearinghouseState','metaAndAssetCtxs'])
        self.assertLess(next(iter(result['entry_accounts'].values()))['allowance_at_ms'],self.exchange.t)
        self.provider._entry_checks.clear();self.exchange.t+=1
        self.raw.mutate=expire
        account=self.state['routes']['long_account'];revision=result['legacy_account_revisions'][account]
        self.provider._entry_account(account,'long_account',state=self.state,legacy_revision=revision)
        allowance_at=self.provider._entry_checks[account]['allowance_at_ms']
        self.exchange.t=allowance_at+60000;self.raw.mutate=None;self.raw.calls.clear()
        self.provider._entry_account(account,'long_account',state=self.state,legacy_revision=revision)
        self.assertEqual([c[0] for c in self.raw.calls],['userRateLimit'])

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
        account=self.state['routes']['long_account'];revision=result['legacy_account_revisions'][account]
        self.assertNotIn('allowance_at_ms',self.provider._entry_checks[account])
        with patch.object(self.raw,'read',return_value=dict(nRequestsCap=1000,nRequestsUsed=990,nRequestsSurplus=0)):
            initial=self.provider._entry_account(account,'long_account',state=self.state,legacy_revision=revision)
        self.assertEqual(initial['action_headroom'],10)
        direct=deepcopy(self.state);self.raw.calls.clear()
        direct['requests']['unknown']=dict(proposal=dict(account=account,action=dict(type='order',orders=[{}])),
            phase='OUTCOME_UNKNOWN')
        edge=self.provider._entry_account(account,'long_account',state=direct,legacy_revision=revision)
        self.assertEqual(edge['action_headroom'],5)
        self.assertEqual(self.raw.calls,[])
        direct['requests']['unsent']=dict(proposal=dict(account=account,action=dict(type='cancel',cancels=[{}])),
            phase='ABORTED_UNSENT')
        self.assertEqual(self.provider._entry_account(account,'long_account',state=direct,
            legacy_revision=revision,plan_only=True),['userRateLimit'])
        with patch.object(self.raw,'read',side_effect=low_allowance):
            with self.assertRaisesRegex(ValueError,'ENTRY_ACTION_HEADROOM_INSUFFICIENT'):
                self.provider._entry_account(account,'long_account',state=direct,legacy_revision=revision)
        self.assertNotIn('allowance_at_ms',self.provider._entry_checks[account])
        renewed=self.provider._entry_account(account,'long_account',state=direct,legacy_revision=revision)
        self.assertEqual(renewed['action_headroom'],1000)
        direct['requests']['batch']=dict(proposal=dict(account=account,
            action=dict(type='batchModify',modifies=[{},{}])),phase='OBSERVED')
        direct['requests']['rejected']=dict(proposal=dict(account=account,
            action=dict(type='order',orders=[{},{}])),phase='OBSERVED',reply=dict(state='REJECTED'))
        self.raw.calls.clear()
        spent=self.provider._entry_account(account,'long_account',state=direct,legacy_revision=revision)
        self.assertEqual(spent['action_headroom'],980)
        repeated=self.provider._entry_account(account,'long_account',state=direct,legacy_revision=revision)
        self.assertEqual(repeated['action_headroom'],980)
        self.assertEqual(self.raw.calls,[])
        direct['requests'].pop('unknown');direct['archived_request_count']=1
        self.assertEqual(self.provider._entry_account(account,'long_account',state=direct,
            legacy_revision=revision,plan_only=True),['userRateLimit'])
        refreshed=self.provider._entry_account(account,'long_account',state=direct,legacy_revision=revision)
        self.assertEqual(refreshed['action_headroom'],1000)
        self.assertEqual([c[0] for c in self.raw.calls],['userRateLimit'])
        self.raw.calls.clear()
        self.assertEqual(self.provider._entry_account(account,'long_account',state=direct,
            legacy_revision='changed-legacy',plan_only=True),['userRateLimit'])
        self.provider._entry_account(account,'long_account',state=direct,legacy_revision='changed-legacy')
        self.assertEqual([c[0] for c in self.raw.calls],['userRateLimit'])

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
        second_entry = threading.Thread(target=run, args=(True,), name='second-entry-fixture', daemon=True)
        protection = threading.Thread(target=run, args=(False,), name='protection-fixture', daemon=True)
        entry.start()
        try:
            self.assertTrue(entered.wait(2))
            self.assertEqual(self.provider._entry_account(self.state['routes']['long_account'],
                'long_account',state=self.state,legacy_revision='planning',plan_only=True),
                ['userRole','userRateLimit'])
            self.assertTrue(entry.is_alive())
            second_entry.start()
            protection.start()
            protection.join(timeout=2)
            self.assertFalse(protection.is_alive())
            self.assertTrue(entry.is_alive())
            self.assertEqual([enabled for enabled,_ in completed], [False])
        finally:
            release.set()
            entry.join(timeout=3)
            if second_entry.ident is not None:
                second_entry.join(timeout=3)
            if protection.ident is not None:
                protection.join(timeout=3)
        self.assertFalse(entry.is_alive())
        self.assertFalse(second_entry.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(completed), 3)
        self.assertEqual(sum(c[0]=='userRole' for c in self.raw.calls),1)
        self.assertEqual(sum(c[0]=='userRateLimit' for c in self.raw.calls),1)

    def test_idle_collections_read_each_account_once_without_unused_market_read(self):
        self.provider.collect(self.state)
        self.assertEqual(len(self.raw.calls), 4)
        self.provider.collect(self.state)
        self.assertEqual(len(self.raw.calls), 8)
        self.assertNotIn('metaAndAssetCtxs',[c[0] for c in self.raw.calls])


if __name__ == '__main__':
    unittest.main()
