"""Raw /info fixtures exercise the real collectors; external I/O is forbidden."""
from copy import deepcopy
from decimal import Decimal
import unittest
from unittest.mock import patch

from experimental_execution_fixtures import r2732_message
from . import card_lifecycle as life, experimental_execution_runtime as runtime
from . import experimental_execution_evidence as proof
from .experimental_live_provider import LiveEvidenceProvider, ProviderError
from .test_experimental_execution_runtime import SoftwareExchange, ROUTES, META, T
from . import test_experimental_dispatch_integration as integration


ENV=dict(HL_TESTNET_LONG_ACCOUNT_ADDRESS=ROUTES['long_account']['account'],
    HL_TESTNET_SHORT_ACCOUNT_ADDRESS=ROUTES['short_account']['account'],
    HL_TESTNET_LONG_AGENT_ADDRESS='0x'+'3'*40,HL_TESTNET_SHORT_AGENT_ADDRESS='0x'+'4'*40,
    HL_TESTNET_LONG_ENTRY_ENABLED='false',HL_TESTNET_SHORT_ENTRY_ENABLED='false')


class FixtureStore:
    def __init__(self,state): self.state=state
    def load(self): return deepcopy(self.state)


class LegacyFixture:
    def __init__(self): self.rows=[]
    def for_account(self,account): return deepcopy([s for s in self.rows if s['account']==account])


class UnavailablePrices:
    def closed_bars(self,msg,now): return []
    def source_range(self,msg,now):
        raise ValueError('CONTINUOUS_REFERENCE_MARK_HISTORY_UNAVAILABLE')
    def mark_window(self,*args):
        raise ValueError('CONTINUOUS_REFERENCE_MARK_HISTORY_UNAVAILABLE')


class RawTestnetFixture:
    """Deterministic raw HTTP payload oracle usable by integrated adapter tests.

    ``exchange`` may be the software price/fill oracle. Production modules see
    only raw official-shape responses and their actual shared budget identity.
    """
    parallel=False
    def __init__(self,exchange,state_loader,budget):
        self.exchange=exchange; self.state_loader=state_loader; self.budget=budget
        self.extra_orders=[];self.extra_positions=[];self.calls=[];self.mutate=None

    def status(self,oid):
        item=self.exchange.orders.get(str(oid))
        if item is None:return {'status':'unknownOid'}
        row=item['view'];o=row['wire_order'];t=o['t'].get('trigger')
        state=self.state_loader();request=next(r for r in state['requests'].values()
            if r['proposal']['action']['type']!='cancel'
            and runtime.wire.requested_order(r['proposal']['action'])['c']==row['cloid'])
        p=request['proposal'];filled=sum((Decimal(f['quantity']) for f in row['fills']),Decimal(0))
        order=dict(oid=int(oid),cloid=row['cloid'],coin=p['symbol'],side='B' if o['b'] else 'A',
            reduceOnly=o['r'],origSz=o['s'],sz=life.text(Decimal(o['s'])-filled) if row['status']=='OPEN' else o['s'],
            limitPx=o['p'],orderType='Limit' if t is None else 'Stop Market' if t['tpsl']=='sl' else 'Take Profit Limit',
            isPositionTpsl=False,isTrigger=t is not None,triggerPx='0' if t is None else t['triggerPx'],timestamp=request['attempt_at_ms'])
        return dict(status='order',order=dict(status=dict(OPEN='open',FILLED='filled',CANCELED='canceled',REJECTED='rejected')[row['status']],
            statusTimestamp=row['at_ms'],order=order))

    def lookup(self,account,cloid):
        self.calls.append(('lookup',account,cloid))
        for oid,item in self.exchange.orders.items():
            if item['account']==account and item['view']['cloid']==cloid:return self.status(oid)
        return {'status':'unknownOid'}

    def read(self,kind,account=None,*,user=None,coin=None,oid=None,start=None,end=None):
        account=account or user;self.calls.append((kind,account,coin))
        if self.mutate:self.mutate(kind)
        if kind=='meta':return deepcopy(META)
        if kind=='metaAndAssetCtxs':
            contexts=[]
            for asset in META['universe']:
                symbol=asset['name']
                marks={self.exchange.mark[runtime._lane(route['account'],symbol)]
                    for route in ROUTES.values() if runtime._lane(route['account'],symbol) in self.exchange.mark}
                if len(marks)>1:raise AssertionError('ONE_PUBLIC_MARK_PER_SYMBOL_REQUIRED')
                contexts.append(dict(markPx=next(iter(marks),'2.3')))
            return [deepcopy(META),contexts]
        if kind=='frontendOpenOrders':
            return [self.status(oid)['order']['order'] for oid,item in self.exchange.orders.items()
                    if item['account']==account and item['view']['status']=='OPEN']+deepcopy(self.extra_orders)
        if kind=='clearinghouseState':
            context=self.exchange.collect(self.state_loader())
            values=[dict(position=dict(coin=s['symbol'],szi=s['position_quantity']))
                for s in context['snapshots'] if s['account']==account and Decimal(s['position_quantity'])]
            return dict(assetPositions=values+deepcopy(self.extra_positions),withdrawable='50000',
                marginSummary=dict(accountValue='50000',totalRawUsd='50000',totalMarginUsed='0',totalNtlPos='0'))
        if kind=='orderStatus':return self.status(oid)
        if kind=='userFillsByTime':
            rows=[]
            for oid,item in self.exchange.orders.items():
                if item['account']!=account:continue
                row=item['view'];o=row['wire_order']
                symbol=self.status(oid)['order']['order']['coin']
                for f in row['fills']:
                    if start<=f['at_ms']<=end:
                        # Stable globally unique exchange trade ids.
                        tid=int(life.digest(f['fill_id'])[:12],16)
                        rows.append(dict(coin=symbol,tid=tid,oid=int(oid),sz=f['quantity'],px=f['price'],
                            fee='0',feeToken='USDC',side='B' if o['b'] else 'A',time=f['at_ms']))
            return rows
        if kind=='activeAssetData':
            return dict(user=account,coin=coin,
                availableToTrade=['50000','50000'],maxTradeSzs=['100000','100000'],leverage=dict(type='cross',value=10))
        if kind=='userAbstraction':return 'disabled'
        if kind=='userRole':
            for role in ('LONG','SHORT'):
                if account==ENV['HL_TESTNET_'+role+'_AGENT_ADDRESS']:
                    return dict(role='agent',data=dict(user=ENV['HL_TESTNET_'+role+'_ACCOUNT_ADDRESS']))
            return dict(role='user')
        if kind=='userRateLimit':return dict(nRequestsCap=1000,nRequestsUsed=0,nRequestsSurplus=0)
        if kind=='spotClearinghouseState':return dict(balances=[])
        raise AssertionError('UNEXPECTED_RAW_FIXTURE_REQUEST:'+kind)


class ProviderTests(unittest.TestCase):
    def setUp(self):
        for target in ('http.client.HTTPSConnection','socket.socket.connect','hl_testnet_runtime.two_account_execution.wallet_for_role'):
            p=patch(target,side_effect=AssertionError('NO_EXTERNAL_IO'));p.start();self.addCleanup(p.stop)
        self.exchange=SoftwareExchange(T+10000)
        self.state=dict(domain='testnet',revision=0,not_before_ms=T-60000,routes={r:x['account'] for r,x in ROUTES.items()},
            sources={},trades={},requests={},snapshots={},budget=[],events=[])
        self.store=FixtureStore(self.state);self.legacy=LegacyFixture();self.budget=object()
        self.raw=RawTestnetFixture(self.exchange,self.store.load,self.budget)
        self.provider=LiveEvidenceProvider(ENV,legacy_store=self.legacy,experimental_store=self.store,
            price_evidence=UnavailablePrices(),budget=self.budget,clock=self.exchange.now,
            info_reader=self.raw,public_reader=self.raw,lookup_reader=self.raw.lookup)

    def pending(self):
        from .experimental_plan_store import reduce_source
        msg=r2732_message(entry=2.3,decision_ms=T-60000)
        record,_=reduce_source(None,msg,now=runtime.contract.iso_ms(self.exchange.t),
            not_before=runtime.contract.iso_ms(self.state['not_before_ms']),domain='testnet')
        self.state['sources'][msg['occurrence_id']]=record
        return msg

    def approved(self):
        from approved_alert_fixtures import maxpain_alert
        from .experimental_plan_store import reduce_source
        msg=maxpain_alert(approved_ms=T)
        record,_=reduce_source(None,msg,now=runtime.contract.iso_ms(self.exchange.t),
            not_before=runtime.contract.iso_ms(self.state['not_before_ms']),domain='testnet')
        self.state['sources'][msg['occurrence_id']]=record
        account=self.state['routes']['long_account']
        self.exchange.mark[runtime._lane(account,msg['symbol'])]=msg['entry']
        return msg

    def active(self,*,partial=False,steps=1):
        # Generate a consistent executed fixture using the already-tested
        # software engine, then independently reconstruct all raw HTTP facts.
        h=integration.DispatchIntegrationTests();h.setUp();self.addCleanup(h.doCleanups)
        if partial:h.venue.instant_fraction=Decimal('.5')
        h.worker.receive([h.msg])
        for _ in range(steps):h.worker.run_once()
        state=h.store.load()
        self.state.update(deepcopy(state));self.state['domain']='testnet'
        for request in self.state['requests'].values():request['domain']='testnet'
        self.exchange.orders=deepcopy(h.venue.orders);self.exchange.requests=deepcopy(h.venue.requests)
        self.exchange.mark=deepcopy(h.venue.mark)
        self.exchange.t=h.venue.t
        return h.msg

    def test_constructor_has_no_io_and_rejects_unshared_budget(self):
        self.assertEqual(self.raw.calls,[])
        with self.assertRaisesRegex(ProviderError,'ONE_SHARED'):
            LiveEvidenceProvider(ENV,legacy_store=self.legacy,experimental_store=self.store,
                price_evidence=UnavailablePrices(),budget=object(),info_reader=self.raw,public_reader=self.raw)

    def test_rejects_software_state_instead_of_relabeling_it(self):
        self.state['domain']='software'
        with self.assertRaisesRegex(ProviderError,'NO_SOFTWARE_RELABELING'):self.provider.collect(self.state)
        self.assertEqual(self.raw.calls,[])

    def test_missing_mark_history_blocks_entry_with_fresh_own_account_inventory(self):
        msg=self.pending();value=self.provider.collect(self.state,entries_enabled=True)
        self.assertTrue(value['inventory_complete'])
        self.assertEqual(value['entry_blocked'][msg['occurrence_id']],'CONTINUOUS_REFERENCE_MARK_HISTORY_UNAVAILABLE')
        self.assertEqual(value['inventory_accounts'],[ROUTES['short_account']['account']])
        self.assertEqual(len(value['snapshots']),1)
        self.assertEqual(sum(c[0]=='frontendOpenOrders' for c in self.raw.calls),1)

    def test_approved_entry_collects_actual_account_without_source_history(self):
        msg=self.approved();self.provider.safety=object()
        with patch.object(self.provider.prices,'source_range',side_effect=AssertionError('NO_SOURCE_PATH')) as source, \
                patch.object(self.provider.prices,'mark_window',side_effect=AssertionError('NO_MARK_HISTORY')) as history:
            context=self.provider.collect(self.state,entries_enabled=True)
        source.assert_not_called();history.assert_not_called()
        self.assertEqual(context['entry_blocked'],{})
        self.assertEqual(context['ranges'],{})
        self.assertIn(msg['occurrence_id'],context['capacity'])
        self.assertTrue(context['inventory_complete'])
        self.assertIn('activeAssetData',[c[0] for c in self.raw.calls])

    def test_approved_entry_still_requires_supervisor_and_owned_inventory(self):
        msg=self.approved()
        context=self.provider.collect(self.state,entries_enabled=True)
        self.assertEqual(context['entry_blocked'][msg['occurrence_id']],
            'VERIFIED_SUPERVISOR_AND_FEED_CAPABILITY_REQUIRED')
        self.provider.safety=object();self.raw.extra_orders=[dict(coin='DOGE',oid=999)]
        context=self.provider.collect(self.state,entries_enabled=True)
        self.assertIn(msg['occurrence_id'],context['entry_blocked'])
        self.assertNotIn(msg['occurrence_id'],context['capacity'])

    def test_approved_dispatch_replays_real_admission_with_no_range_lookup(self):
        from .experimental_live_runtime import TestnetExecutionRuntime
        from .experimental_execution_dispatch import review
        msg=self.approved();test=self
        class FixtureSafety:
            def verify_checkpoint(self,*,state,request,collected_at_ms,ownership_revision):
                p=request['proposal']
                return dict(account=p['account'],role=p['role'],at_ms=collected_at_ms,
                    entry_enabled=True,emergency_healthy=True,feed_reconciled=True,
                    entry_circuit_clear=True,supervisor_at_ms=collected_at_ms,
                    not_before_ms=test.state['not_before_ms'])
        self.provider.safety=FixtureSafety()
        ctx=self.provider.collect(self.state,entries_enabled=True)
        planner=object.__new__(TestnetExecutionRuntime)
        proposal=planner._cycle_proposal(self.state,ctx,self.exchange.t,entries_enabled=True)
        self.assertIsNotNone(proposal)
        request=planner._reserve_live(self.state,proposal,self.exchange.t,self.exchange.t)
        calls=len(self.raw.calls)
        context=self.provider.dispatch_context(request)
        self.assertNotIn('source_range',context);self.assertNotIn('testnet_range',context)
        checked=review(request,context,now_ms=self.exchange.t)
        self.assertEqual(checked['action']['orders'][0]['p'],msg['entry'])
        self.assertEqual(checked['action']['orders'][0]['t'],{'limit':{'tif':'Gtc'}})
        self.assertEqual(len(self.raw.calls),calls)

    def test_maintenance_uses_one_shared_market_context_and_no_entry_capacity_reads(self):
        self.active(steps=4);self.provider.collect(self.state)
        kinds=[call[0] for call in self.raw.calls]
        self.assertEqual(kinds.count('metaAndAssetCtxs'),1)
        self.assertNotIn('meta',kinds);self.assertNotIn('activeAssetData',kinds)
        self.assertNotIn('userAbstraction',kinds);self.assertNotIn('userRole',kinds)
        self.assertNotIn('userRateLimit',kinds)

    def test_foreign_order_on_other_symbol_blocks_account_entry(self):
        msg=self.pending();self.raw.extra_orders=[dict(coin='DOGE',oid=999)]
        value=self.provider.collect(self.state,entries_enabled=True)
        self.assertIn(ROUTES['short_account']['account'],value['account_entry_blocked'])
        self.assertIn(msg['occurrence_id'],value['entry_blocked'])

    def test_next_collection_reads_current_inventory_and_blocks_unknown_exposure(self):
        self.pending()
        first=self.provider.collect(self.state)
        self.assertEqual(first['account_entry_blocked'],{})
        self.raw.extra_orders=[dict(coin='DOGE',oid=999)]
        second=self.provider.collect(self.state)
        self.assertEqual(set(second['account_entry_blocked']),set(self.state['routes'].values()))
        self.assertEqual(sum(c[0]=='frontendOpenOrders' for c in self.raw.calls),4)

    def test_real_collectors_reconstruct_exact_fill_without_source_history(self):
        msg=self.active();value=self.provider.collect(self.state)
        self.assertEqual(value['blocked_lanes'],{})
        snapshot=value['snapshots'][0]
        self.assertEqual(snapshot['orders'][0]['status'],'FILLED')
        self.assertEqual(len(snapshot['orders'][0]['fills']),1)
        self.assertEqual(snapshot['orders'][0]['wire_order'],next(iter(self.state['requests'].values()))['proposal']['action']['orders'][0])
        self.assertIn(runtime._lane(snapshot['account'],snapshot['symbol']),value['marks'])

    def test_partial_ioc_cancel_retains_original_wire_size_and_actual_fill(self):
        self.active(partial=True);value=self.provider.collect(self.state)
        self.assertEqual(value['blocked_lanes'],{})
        order=value['snapshots'][0]['orders'][0]
        self.assertEqual(order['status'],'CANCELED')
        self.assertEqual(Decimal(order['wire_order']['s']),2*Decimal(order['fills'][0]['quantity']))

    def test_changed_raw_cloid_terms_are_blocked_and_not_given_owner(self):
        self.active();original=self.raw.lookup
        def changed(account,cloid):
            raw=original(account,cloid);raw['order']['order']['origSz']='1';return raw
        self.provider._lookup_reader=changed
        value=self.provider.collect(self.state)
        self.assertEqual(value['snapshots'],[])
        self.assertIn('ORDER_TERMS_NOT_BOUND_TO_INTENT',value['blocked_lanes'].values())

    def test_unknown_other_account_does_not_erase_protection_snapshot(self):
        self.active();self.raw.extra_positions=[dict(position=dict(coin='DOGE',szi='3'))]
        value=self.provider.collect(self.state)
        self.assertEqual(len(value['snapshots']),1)
        self.assertEqual(value['snapshots'][0]['orders'][0]['status'],'FILLED')
        self.assertEqual(set(value['account_entry_blocked']),{self.state['routes']['short_account']})

    def test_dispatch_requires_prior_collection_and_native_durable_request(self):
        self.active();request=next(iter(self.state['requests'].values()))
        with self.assertRaisesRegex(ProviderError,'FRESH_COLLECTOR'):self.provider.dispatch_context(request)
        self.provider.collect(self.state);changed=deepcopy(request);changed['nonce']+=1
        with self.assertRaisesRegex(ProviderError,'EXACT_NATIVE'):self.provider.dispatch_context(changed)

    def test_context_cache_is_thread_local(self):
        import threading
        self.pending();self.provider.collect(self.state);seen=[]
        thread=threading.Thread(target=lambda:seen.append(self.provider._last));thread.start();thread.join()
        self.assertEqual(seen,[None]);self.assertIsNotNone(self.provider._last)

    def cancel_request(self):
        msg=self.active(steps=4);trade=self.state['trades'][msg['occurrence_id']]
        old=next(r for r in self.state['requests'].values() if r['proposal']['leg']=='STOP')
        request=deepcopy(old);oid=request['observed_oid'];request['request_id']=life.digest(['new-cancel',oid])
        request.update(phase='OUTCOME_UNKNOWN',observed_oid=None,reply=None,
            nonce=self.exchange.t,attempt_at_ms=self.exchange.t,prepared_at_ms=self.exchange.t)
        request['proposal'].update(operation='CANCEL',quantity='0',
            action=dict(type='cancel',cancels=[dict(a=trade['asset']['index'],o=int(oid))]),
            basis=life.digest(self.state['snapshots']),observed_at_ms=self.exchange.t)
        self.state['requests'][request['request_id']]=request
        return request

    def test_source_unavailability_does_not_block_exact_owned_cancel_context(self):
        from .experimental_execution_dispatch import review
        request=self.cancel_request();value=self.provider.collect(self.state)
        self.assertEqual(value['blocked_lanes'],{})
        calls=len(self.raw.calls);context=self.provider.dispatch_context(request)
        self.assertTrue(review(request,context,now_ms=self.exchange.t)['reviewed'])
        self.assertFalse(context['safety']['entry_enabled'])
        self.assertEqual(self.provider.dispatch_context(request),context)
        self.assertEqual(len(self.raw.calls),calls)

    def test_new_observed_fill_after_collection_fences_old_exit_context(self):
        request=self.cancel_request();self.provider.collect(self.state)
        self.state['snapshots']['new-reconciliation']=dict(at_ms=self.exchange.t)
        with self.assertRaisesRegex(ProviderError,'EXPERIMENTAL_OWNERSHIP_CHANGED'):
            self.provider.dispatch_context(request)

    def test_new_unresolved_same_market_request_fences_old_exit_context(self):
        request=self.cancel_request();self.provider.collect(self.state)
        other=deepcopy(request);other['request_id']=life.digest('peer-request')
        self.state['requests'][other['request_id']]=other
        with self.assertRaisesRegex(ProviderError,'UNRESOLVED_EXPERIMENTAL_PEER'):
            self.provider.dispatch_context(request)

    def test_collection_timestamp_is_not_refreshed_at_dispatch(self):
        request=self.cancel_request();self.provider.collect(self.state)
        self.exchange.t+=5001
        with self.assertRaisesRegex(ProviderError,'FRESH_COLLECTOR'):
            self.provider.dispatch_context(request)

    def test_known_rejection_and_unknown_oid_produce_exact_no_order_proof(self):
        self.active();request=next(iter(self.state['requests'].values()))
        self.exchange.orders.clear();request['reply']=dict(state='REJECTED',code='ORDER_REJECTED')
        value=self.provider.collect(self.state)
        self.assertEqual(value['blocked_lanes'],{})
        self.assertEqual(value['rejected_requests'],[dict(request_id=request['request_id'],
            account=request['proposal']['account'],symbol=request['proposal']['symbol'],
            at_ms=self.exchange.t,lookup_status='unknownOid',reply_digest=life.digest(request['reply']))])

    def test_unknown_oid_without_definitive_rejection_keeps_outcome_fenced(self):
        self.active();self.exchange.orders.clear()
        value=self.provider.collect(self.state)
        self.assertEqual(value['rejected_requests'],[])
        self.assertEqual(next(iter(self.state['requests'].values()))['phase'],'OUTCOME_UNKNOWN')

    def test_cancel_rejection_requires_independently_unchanged_open_order(self):
        request=self.cancel_request();request['reply']=dict(state='REJECTED',code='CANCEL_REJECTED')
        value=self.provider.collect(self.state);fact=value['rejected_requests'][0]
        self.assertEqual(fact['lookup_status'],'orderStillOpen')
        self.assertEqual(fact['old_order_id'],str(request['proposal']['action']['cancels'][0]['o']))
        old=self.state['trades'][request['proposal']['card_id']]['orders'][fact['old_order_id']]
        old['at_ms']-=1
        self.assertEqual(self.provider.collect(self.state)['rejected_requests'],[])

    def test_modify_rejection_proves_new_cloid_absent_and_old_order_unchanged(self):
        request=self.cancel_request();p=request['proposal'];oid=str(p['action']['cancels'][0]['o'])
        old=self.state['trades'][p['card_id']]['orders'][oid]
        order=deepcopy(old['wire_order']);order['c']='0x'+'f'*32
        p.update(operation='AMEND_EXIT',quantity=order['s'],action=dict(type='batchModify',modifies=[dict(oid=int(oid),order=order)]))
        request['reply']=dict(state='REJECTED',code='ORDER_REJECTED')
        facts=self.provider.collect(self.state)['rejected_requests']
        self.assertEqual(len(facts),1);self.assertEqual(facts[0]['lookup_status'],'unknownOid')
        self.assertEqual(facts[0]['old_order_id'],oid)

    def test_default_readers_are_fresh_each_cycle_with_same_global_budget(self):
        # Exercise the construction policy rather than reset a transport's
        # per-request ceiling or the actual shared database quota.
        self.pending();info=[];public=[];priorities=[]
        def new_info(**kwargs):
            self.assertIs(kwargs['budget'],self.budget);value=RawTestnetFixture(self.exchange,self.store.load,self.budget)
            priorities.append(kwargs['priority'])
            info.append(value);return value
        def new_public(**kwargs):
            self.assertIs(kwargs['budget'],self.budget);value=RawTestnetFixture(self.exchange,self.store.load,self.budget)
            public.append(value);return value
        with patch('hl_testnet_runtime.experimental_live_provider.checks.InfoReader',side_effect=new_info),\
             patch('hl_testnet_runtime.experimental_live_provider.sync.PublicReader',side_effect=new_public):
            p=LiveEvidenceProvider(ENV,legacy_store=self.legacy,experimental_store=self.store,
                price_evidence=UnavailablePrices(),budget=self.budget,clock=self.exchange.now,
                lookup_reader=self.raw.lookup)
            for _ in range(26):p.collect(self.state)
        self.assertEqual(len(public),27);self.assertEqual(len(info),54)
        self.assertEqual(priorities,['protection','background']*27)
        self.assertEqual(sum(len(r.calls) for r in public),104)
        self.assertTrue(all(len(r.calls)<=4 for r in public))

    def test_ineligible_source_alone_does_not_fetch_market_or_source_prices(self):
        self.approved()
        value=self.provider.collect(self.state,entries_enabled=False)
        self.assertEqual(value['marks'],{})
        self.assertEqual([c[0] for c in self.raw.calls],
            ['frontendOpenOrders','clearinghouseState']*2)

    def test_budget_retry_skips_entry_reads_until_original_retry_time(self):
        from .request_budget import BudgetError
        msg=self.approved();cid=msg['occurrence_id'];self.provider.safety=object()
        failure=BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED',retry_after_ms=17000)
        with patch.object(self.provider,'_capacity',side_effect=failure) as capacity:
            context=self.provider.collect(self.state,entries_enabled=True)
        retry=self.exchange.t+17000
        self.assertEqual(context['entry_retry_after_ms'][cid],retry)
        self.state['entry_decisions']={cid:dict(reason=context['entry_blocked'][cid],retry_after_ms=retry)}
        self.raw.calls.clear()
        with patch.object(self.provider,'_capacity',side_effect=AssertionError('NO_CAPACITY_UNTIL_DUE')):
            context=self.provider.collect(self.state,entries_enabled=True)
        self.assertEqual(context['entry_retry_after_ms'][cid],retry)
        self.assertNotIn('metaAndAssetCtxs',[c[0] for c in self.raw.calls])
        self.assertEqual(context['capacity'],{})

    def test_insufficient_whole_preflight_budget_stops_before_optional_reads(self):
        from types import SimpleNamespace
        msg=self.approved();self.provider.safety=object();requested=[]
        def capacity(*,requested_weight,priority):
            requested.append((requested_weight,priority))
            return dict(eligible=False,used_weight=658,requested_weight=requested_weight,
                        ceiling=800,retry_after_ms=9000)
        self.provider.budget=SimpleNamespace(capacity=capacity)
        context=self.provider.collect(self.state,entries_enabled=True)
        self.assertEqual(requested,[(183,'background')])
        self.assertEqual([c[0] for c in self.raw.calls],
                         ['frontendOpenOrders','clearinghouseState','metaAndAssetCtxs'])
        self.assertEqual(context['entry_retry_after_ms'][msg['occurrence_id']],self.exchange.t+9000)

    def test_nested_preflight_keeps_budget_delay_and_stops_remaining_reads(self):
        from .request_budget import BudgetError
        msg=self.approved();self.provider.safety=object();original=self.raw.read
        def deny_role(kind,*args,**kwargs):
            if kind=='userRole':
                raise BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED',retry_after_ms=9000)
            return original(kind,*args,**kwargs)
        with patch.object(self.raw,'read',side_effect=deny_role):
            context=self.provider.collect(self.state,entries_enabled=True)
        self.assertEqual(context['entry_blocked'][msg['occurrence_id']],'TESTNET_REQUEST_BUDGET_EXHAUSTED')
        self.assertEqual(context['entry_retry_after_ms'][msg['occurrence_id']],self.exchange.t+9000)
        self.assertNotIn('userRateLimit',[c[0] for c in self.raw.calls])

    def test_known_order_identity_and_terminal_status_are_reused_only_with_checkpoint(self):
        msg=self.active(steps=4);cid=msg['occurrence_id']
        first=self.provider.collect(self.state)
        self.assertEqual(first['blocked_lanes'],{})
        self.assertNotIn('lookup',[c[0] for c in self.raw.calls])
        lane=runtime._lane(self.state['trades'][cid]['account'],msg['symbol'])
        snapshot=first['snapshots'][0]
        # Install the exact normalized raw exchange proof as a durable fixture.
        self.state['snapshots'][lane]=deepcopy(snapshot)
        self.state['trades'][cid]['orders']={o['oid']:deepcopy(o) for o in snapshot['orders']}
        self.state['collector_checkpoints']=deepcopy(first['collector_checkpoints'])
        self.raw.calls.clear()
        result=self.provider.collect(self.state)
        self.assertEqual(result['blocked_lanes'],{})
        terminal=[o for o in snapshot['orders'] if o['status']!='OPEN']
        self.assertTrue(terminal)
        self.assertEqual(sum(c[0]=='orderStatus' for c in self.raw.calls),
                         sum(o['status']=='OPEN' for o in snapshot['orders']))
        self.assertNotIn('lookup',[c[0] for c in self.raw.calls])
        self.assertEqual(sum(c[0]=='userFillsByTime' for c in self.raw.calls),1)
        terminal[0]['wire_order']['s']='1'
        self.state['trades'][cid]['orders'][terminal[0]['oid']]=terminal[0]
        result=self.provider.collect(self.state)
        self.assertIn('PREVIOUSLY_OWNED_ORDER_TERMS_CHANGED',result['blocked_lanes'].values())

    def legacy_active(self,*,pending=None):
        from .experimental_live_provider import _empty
        msg=self.active(steps=4);external=deepcopy(self.state)
        trade=external['trades'][msg['occurrence_id']]
        raw_snapshot=next(s for s in self.exchange.collect(external)['snapshots']
            if s['account']==trade['account'] and s['symbol']==trade['symbol'])
        binding,_=integration.legacy_view(trade,raw_snapshot)
        self.raw.state_loader=lambda:deepcopy(external)
        self.legacy.rows=[dict(account=trade['account'],symbol=trade['symbol'],bucket=life.digest('legacy-xrp'),
            revision=1,pending=pending,bindings=[binding],evidence=dict(snapshot=_empty(
                trade['account'],trade['symbol'],self.state['not_before_ms'])))]
        self.state['requests']={};self.state['trades']={};self.state['snapshots']={}
        return msg

    def test_known_legacy_active_market_stays_owned_and_same_market_entry_blocked(self):
        msg=self.legacy_active();value=self.provider.collect(self.state,entries_enabled=True)
        self.assertEqual(value['account_entry_blocked'],{})
        self.assertEqual(value['snapshots'],[])
        self.assertEqual(value['entry_blocked'][msg['occurrence_id']],'LEGACY_PREDECESSOR_NOT_FINAL')
        account=self.state['routes']['short_account']
        self.assertEqual(len(self.provider._last['buckets'][account][0]['bindings']),1)
        self.assertEqual(self.provider._last['buckets'][account][0]['evidence']['snapshot']['position_quantity'],
            self.raw.read('clearinghouseState',account)['assetPositions'][0]['position']['szi'])

    def test_legacy_unresolved_request_fences_account_admission(self):
        self.legacy_active(pending=life.digest('unknown-legacy-attempt'))
        value=self.provider.collect(self.state)
        self.assertEqual(value['account_entry_blocked'][self.state['routes']['short_account']],
            'UNRESOLVED_ACCOUNT_REQUEST_NO_NEW_ENTRY')

    def test_same_order_cannot_be_bound_to_both_journals(self):
        msg=self.active(steps=4);trade=self.state['trades'][msg['occurrence_id']]
        raw_snapshot=next(s for s in self.exchange.collect(self.state)['snapshots'] if s['account']==trade['account'])
        binding,snapshot=integration.legacy_view(trade,raw_snapshot)
        self.legacy.rows=[dict(account=trade['account'],symbol=trade['symbol'],bucket=life.digest('collision'),
            revision=1,pending=None,bindings=[binding],evidence=dict(snapshot=snapshot))]
        value=self.provider.collect(self.state)
        self.assertEqual(value['snapshots'],[])
        self.assertTrue(set(value['blocked_lanes'].values()) & {'DUPLICATE_CARD_ID','ORDER_ID_ALREADY_BOUND'})

    def test_default_account_capacity_allows_only_proven_owned_other_market(self):
        from . import two_account_execution as roles
        account=roles.PHANTOM;provider=self.provider
        provider.env={**ENV,'HL_TESTNET_SHORT_ACCOUNT_ADDRESS':account,
            'HL_TESTNET_RUNTIME_MODE':'experimental_connection_disabled_by_default'}
        pos=dict(assetPositions=[dict(position=dict(coin='DOGE',szi='-1'))])
        old=self.raw.read
        def read(kind,account_arg=None,*,user=None,coin=None,**kw):
            if kind=='userAbstraction':return 'default'
            if kind=='userRole':return dict(role='user') if user==account else dict(role='agent',data=dict(user=account))
            if kind=='clearinghouseState':
                return dict(**pos,withdrawable='50000',marginSummary=dict(accountValue='50000',totalRawUsd='50000',totalMarginUsed='1',totalNtlPos='1'))
            return old(kind,account_arg,user=user,coin=coin,**kw)
        self.raw.read=read
        msg=r2732_message(entry=2.3,decision_ms=T-60000)
        bucket=dict(account=account,symbol='DOGE',pending=None,bindings=[],
            evidence=dict(snapshot=dict(position_quantity='-1',terminal_orders=[])))
        from .experimental_market_context import MarketSnapshot
        market=MarketSnapshot(self.raw.read('metaAndAssetCtxs'),observed_at_ms=self.exchange.t)
        capacity,report=provider._capacity(msg,account,'short_account',META,
            buckets=[bucket],inventory=dict(orders=[],positions=pos),market=market)
        self.assertEqual(capacity['account'],account)
        self.assertEqual(report['account_mode'],'default')
        self.assertFalse(report['mode_was_renamed'])
        with self.assertRaisesRegex(ValueError,'UNOWNED_ACCOUNT_POSITION'):
            provider._capacity(msg,account,'short_account',META,
                buckets=[],inventory=dict(orders=[],positions=pos),market=market)


if __name__=='__main__':unittest.main()
