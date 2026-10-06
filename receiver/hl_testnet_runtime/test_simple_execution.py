"""Offline request counts, immediate protection and delayed-audit regressions."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import threading
import time
import os
import unittest
from unittest.mock import Mock, patch

from . import simple_execution as simple, checks, card_sync_evidence as evidence
from . import filled_quantity_dispatch as dispatch, emergency_close as emergency
from . import long_stream_runtime as stream
from .request_budget import InfoPlan, request_weight
from .test_filled_quantity_dispatch import NoExternal, state_from_case, ROUTES2, Venue
from .test_filled_quantity_exits import original, META
from .test_card_lifecycle import A, T
from .test_card_sync import Reader, evidence as fixture
from .test_history_gap_recovery import MemoryStore, PlannedReader


class Feed:
    simple_execution=True
    clean=True
    def entry_allowed(self, account): return self.clean
    def dirty_symbols(self, account): return () if self.clean else ('DOGE',)


class SimpleExecutionTests(NoExternal):
    def setUp(self):
        super().setUp()
        simple.STATIC_READS.clear()
        self.addCleanup(simple.STATIC_READS.clear)

    def test_static_cache_expires_from_request_start_and_does_not_cache_failure(self):
        clock=[0.0]
        cache=simple.ReadCache(simple.STATIC_TYPES,300,clock=lambda:clock[0])
        body=dict(type='userRole',user=A)
        fetched=Mock(return_value={'role':'user'})
        value=cache.read(body,fetched);value['role']='tampered'
        clock[0]=299
        self.assertEqual(cache.read(body,fetched),{'role':'user'})
        self.assertEqual(fetched.call_count,1)
        clock[0]=300
        cache.read(body,fetched)
        self.assertEqual(fetched.call_count,2)
        cache.clear()
        with self.assertRaises(ValueError):cache.read(body,Mock(side_effect=ValueError()))
        self.assertFalse(cache.has(body))

    def test_same_question_joins_but_independent_questions_stay_parallel(self):
        cache=simple.ReadCache(simple.STATIC_TYPES,300)
        entered=threading.Barrier(2)
        def fetch(): entered.wait(timeout=2);return {'role':'user'}
        with ThreadPoolExecutor(max_workers=2) as pool:
            jobs=[pool.submit(cache.read,dict(type='userRole',user=user),fetch)
                  for user in (A,'0x'+'2'*40)]
            self.assertTrue(all(job.result(timeout=3)=={'role':'user'} for job in jobs))
        called=Mock(return_value={'role':'user'})
        cache.clear()
        with ThreadPoolExecutor(max_workers=4) as pool:
            jobs=[pool.submit(cache.read,dict(type='userRole',user=A),called) for _ in range(4)]
            for job in jobs:job.result(timeout=2)
        called.assert_called_once()

    def test_cycle_reuse_does_not_escape_the_cycle_or_cover_fill_and_status_reads(self):
        body=dict(type='clearinghouseState',user=A)
        fetch=Mock(return_value={'assetPositions':[]})
        with simple.cycle_reads(True):
            for _ in range(3):simple.read(body,fetch,cycle=True)
            for _ in range(2):simple.read(dict(type='userFillsByTime',user=A),fetch,cycle=True)
            for _ in range(2):simple.read(dict(type='orderStatus',user=A,oid=1),fetch,cycle=True)
        self.assertEqual(fetch.call_count,5)
        with simple.cycle_reads(True):simple.read(body,fetch,cycle=True)
        self.assertEqual(fetch.call_count,6)

    def test_quick_collection_is_one_pass_and_full_audit_is_two_independent_passes(self):
        value=fixture(is_open=True)
        for passes,calls,weight in ((1,5,146),(2,10,292)):
            reader=PlannedReader(value)
            result=evidence.collect(value,reader,clock=lambda:T+1000,
                elapsed=lambda:0,reuse_verified_terminals=True,verification_passes=passes)
            self.assertEqual(reader.calls,calls)
            self.assertEqual(sum(request_weight('/info',b) for b in reader.plans[0]),weight)
            self.assertEqual(result['verification_passes'],passes)
            self.assertFalse(result['report']['needs_review'])

    def test_single_pass_still_exposes_position_and_coverage_mismatches(self):
        value=fixture(is_open=True);reader=Reader(value)
        reader.data['position']['assetPositions'][0]['position']['szi']='99'
        result=evidence.collect(value,reader,clock=lambda:T+1,reuse_verified_terminals=True,
                                verification_passes=1)
        self.assertIn('POSITION_DOES_NOT_MATCH_CARDS',result['report']['bucket_issues'])

    def test_audit_runs_at_three_minutes_without_changing_evidence_time(self):
        for quantity in ('40','100'):
            state=state_from_case(q=quantity,stop=quantity,take=quantity)
            before=deepcopy(state['evidence']);feed=Feed()
            simple.checkpoint(state,{'verification_passes':1},T)
            self.assertEqual(state['simple_execution_audit']['due_ms'],T+180000)
            self.assertTrue(simple.quiet(state,now_ms=T+179999,feed=feed))
            self.assertTrue(emergency.recent_normal_checkpoint(state,now_ms=T+179999,fill_wakeups=feed))
            self.assertFalse(simple.quiet(state,now_ms=T+180000,feed=feed))
            self.assertEqual(state['evidence'],before)
            simple.checkpoint(state,{'verification_passes':1},T+100000)
            self.assertEqual(state['simple_execution_audit']['due_ms'],T+180000)
            simple.checkpoint(state,{'verification_passes':2},T+180000)
            self.assertEqual(state['simple_execution_audit']['due_ms'],T+360000)

    def test_notifications_unknown_work_missing_exits_and_gaps_bypass_delay(self):
        good=state_from_case(q='100',stop='100',take='100')
        for mutate in (lambda s:s.update(pending='unknown'),
                       lambda s:s.update(emergency={'phase':'ACTIVE'}),
                       lambda s:s.update(history_gap_recovery={}),
                       lambda s:s['evidence']['snapshot'].update(history_complete=False),
                       lambda s:s['evidence']['snapshot']['open_orders'].pop()):
            state=deepcopy(good);mutate(state)
            self.assertFalse(simple.quiet(state,now_ms=T+20000,feed=Feed()))
        feed=Feed();feed.clean=False
        self.assertFalse(simple.quiet(good,now_ms=T+20000,feed=feed))

    def test_source_expiry_wakes_partial_entry_even_with_complete_protection(self):
        state=state_from_case(q='40',stop='40',take='40',expiry_seconds=120)
        self.assertTrue(simple.quiet(state,now_ms=T+99999,feed=Feed()))
        self.assertFalse(simple.quiet(state,now_ms=T+100000,feed=Feed()))

    def test_unfilled_entry_polls_only_price_every_ten_seconds_and_crossing_wakes_cancel(self):
        state=state_from_case(q='0',expiry_seconds=600)
        venue=dispatch.TestnetVenue(dict(RENDER_SERVICE_ID=dispatch.roles.SERVICE,
            HL_TESTNET_RUNTIME_MODE='long_stream_testnet_v1'))
        venue.fill_wakeups=Feed();clock=[T];venue.now=lambda:clock[0]
        with patch.object(venue,'sample',return_value=dict(mark_price='10',at_ms=T)) as sample:
            self.assertTrue(venue.waiting_entry_quiet(state))
            clock[0]=T+9999
            self.assertTrue(venue.waiting_entry_quiet(state));sample.assert_called_once()
            clock[0]=T+10000
            self.assertTrue(venue.waiting_entry_quiet(state));self.assertEqual(sample.call_count,2)
            clock[0]=T+20000
            sample.return_value=dict(mark_price='10.1',at_ms=clock[0])
            self.assertFalse(venue.waiting_entry_quiet(state))
            venue.fill_wakeups.clean=False
            self.assertFalse(venue.waiting_entry_quiet(state));self.assertEqual(sample.call_count,3)

    def test_adapter_selects_single_pass_before_deadline_and_full_pass_at_deadline(self):
        state=state_from_case(q='100',stop='100',take='100')
        simple.checkpoint(state,{'verification_passes':1},T)
        venue=dispatch.TestnetVenue(dict(RENDER_SERVICE_ID=dispatch.roles.SERVICE,
            HL_TESTNET_RUNTIME_MODE='long_stream_testnet_v1'))
        venue.store=MemoryStore(state)
        clock=[T+1];venue.now=lambda:clock[0]
        with patch.object(venue,'request_budget',return_value=None), \
             patch.object(evidence,'PublicReader',side_effect=lambda **kw:Reader(state['evidence'])), \
             patch.object(evidence,'collect',wraps=evidence.collect) as collect:
            # Inspect the adapter's selection; its real collector is tested above.
            collect.side_effect=lambda value,reader,**kw: {'verification_passes':kw['verification_passes']}
            self.assertEqual(venue.collect_checkpoint(state['evidence'],pending=None)['verification_passes'],1)
            clock[0]=T+180000
            self.assertEqual(venue.collect_checkpoint(state['evidence'],pending=None)['verification_passes'],2)

    def test_warm_first_entry_uses_four_http_queries_and_62_weight(self):
        env=dict(RENDER_SERVICE_ID=dispatch.roles.SERVICE,
                 HL_TESTNET_RUNTIME_MODE='long_stream_testnet_v1')
        venue=dispatch.TestnetVenue(env);venue.now=lambda:T+1
        record=original(expiry_seconds=300)[1];card=record['card']
        state=dict(account=A,symbol='DOGE',bindings=[],originals={card['card_id']:record})
        proposal=dict(operation='ENTRY',card_id=card['card_id'],account=A,symbol='DOGE',
            role='long_account',source_at=card['prepared']['source']['at'],
            observed_at_ms=T)
        route=ROUTES2['long_account'];calls=[];funded=[];owner=Mock()
        budget=Mock()
        def fund(bodies,**kw):
            funded.append(deepcopy(bodies))
            from .request_budget import _info_plan_key
            entries=[(_info_plan_key(b),format(i+1,'032x'),request_weight('/info',b))
                     for i,b in enumerate(bodies)]
            return InfoPlan(owner,entries,'background',time.monotonic_ns()+15_000_000_000)
        budget.reserve_info_plan.side_effect=fund
        def response(body):
            venue.request_budget().acquire('/info',body,priority='background',host=dispatch.HOST).check()
            calls.append(deepcopy(body));kind=body['type']
            if kind=='userRole':return ({'role':'user'} if body['user']==A
                                       else {'role':'agent','data':{'user':A}})
            if kind=='userAbstraction':return 'disabled'
            if kind=='meta':return {'universe':[dict(name='DOGE',szDecimals=2,maxLeverage=50)]}
            if kind=='frontendOpenOrders':return []
            if kind=='clearinghouseState':return dict(assetPositions=[],withdrawable='100000',
                                                   marginSummary=dict(accountValue='100000'))
            if kind=='activeAssetData':return dict(user=A,coin='DOGE',markPx='10',
                availableToTrade=['100000','100000'],maxTradeSzs=['100000','100000'],
                leverage=dict(type='cross',value=1))
            if kind=='userRateLimit':return dict(nRequestsCap=100,nRequestsUsed=0,nRequestsSurplus=0)
            raise AssertionError(kind)
        venue.store=Mock();venue.store.for_account.return_value=[]
        with patch.object(venue,'request_budget',return_value=budget), \
             patch.object(venue,'_gate',return_value=route), \
             patch.object(dispatch.roles,'route_for',return_value=route), \
             patch.object(dispatch.roles,'wallet_for_role',return_value=object()), \
             patch.object(checks.InfoReader,'_transport',side_effect=response), \
             patch.object(evidence.PublicReader,'_transport',side_effect=response):
            venue.warm_configuration([('long_account',route,None,None)])
            calls.clear()
            with simple.cycle_reads(True):
                venue.empty_snapshot(A,'DOGE')
                venue.sample(A,'DOGE');venue.metadata()
                venue.authorize(state,proposal,dispatch.AFTER_EXIT)
        self.assertCountEqual([b['type'] for b in calls],
            ['frontendOpenOrders','clearinghouseState','activeAssetData','userRateLimit'])
        self.assertEqual(sum(request_weight('/info',body) for body in calls),62)
        self.assertEqual(sum(request_weight('/info',body) for body in funded[0]),22)
        self.assertEqual(venue.sent,0)


class FastVenue(Venue):
    def __init__(self):
        super().__init__()
        self.simple_execution=True
        self.env={}
        self.fill_wakeups=Feed()
        self.passes=[]
    def request_budget(self): return None
    def collect_checkpoint(self,value,*,pending,emergency_active=False):
        owner=self
        class BoundReader:
            def __init__(self,**kw): pass
            def read(self,*args,**kw): return owner.read(*args,**kw)
        with patch.object(evidence,'PublicReader',BoundReader):
            result=dispatch.TestnetVenue._collect(self,value,pending_clear=pending is None,
                                                 force_protection=pending is not None or emergency_active)
        self.passes.append(result['verification_passes'])
        return result


@unittest.skipUnless(os.environ.get('HL_JOURNAL_CI_URL'),'Disposable loopback PostgreSQL required')
class SimpleDatabaseTests(NoExternal):
    def setUp(self):
        super().setUp()
        from .postgres_journal import PostgresJournal
        from .filled_dispatch_store import DispatchStore,SCHEMA
        from .trade_card_store import CardStore
        self.journal=PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL'])
        with self.journal._transaction() as conn:
            for name in (SCHEMA,'hl_testnet_recovery_rehearsal_v1','hl_testnet_cards_v1','hl_testnet_execution_v1'):
                conn.execute(f'DROP SCHEMA IF EXISTS {name} CASCADE')
        self.journal.bootstrap()
        self.cards=CardStore(self.journal);self.cards.initialize()
        self.store=DispatchStore(self.journal);self.store.initialize()
        self.venue=FastVenue()
        self.controller=dispatch.Controller(self.store,self.venue,ROUTES2,after_exit_policy=dispatch.AFTER_EXIT)

    def cycle(self,bucket,send=True):
        self.venue.t+=1
        return self.controller.cycle(bucket,send=send)

    def open_partial(self,side,*,lost=False):
        binding,record=original(1,side,expiry_seconds=600)
        self.cards.record(record['card']);state=self.controller.register(binding['card_id'])
        bucket=state['bucket'];self.venue.lose_reply=lost
        result=self.cycle(bucket)
        self.assertEqual(result['status'],'OUTCOME_UNKNOWN' if lost else 'ACCEPTED_UNVERIFIED')
        self.venue.fill('1000','40')
        self.cycle(bucket);self.cycle(bucket);self.cycle(bucket,False)
        state=self.store.load(bucket)
        self.assertTrue(simple.protected(state))
        self.assertTrue(all(p==1 for p in self.venue.passes))
        self.assertEqual(len([r for r in self.venue.requests if r['proposal']['operation']=='ENTRY']),1)
        return bucket,state

    def test_partial_long_gets_both_exits_before_audit_and_more_fills_resize_without_resetting_deadline(self):
        bucket,state=self.open_partial('LONG')
        due=state['simple_execution_audit']['due_ms']
        self.venue.fill('1000','20')
        self.cycle(bucket);self.cycle(bucket);self.cycle(bucket,False)
        state=self.store.load(bucket)
        self.assertTrue(simple.protected(state))
        self.assertEqual(state['simple_execution_audit']['due_ms'],due)
        for row in state['evidence']['snapshot']['open_orders']:
            if row['reduce_only']:self.assertEqual(row['quantity'],'60')
        self.venue.t=due
        self.cycle(bucket,False)
        self.assertEqual(self.venue.passes[-1],2)
        saved=self.store.load(bucket)['simple_execution_audit']
        self.assertEqual(saved['verified_at_ms'],self.store.load(bucket)['evidence']['snapshot']['at_ms'])

    def test_partial_short_survives_controller_restart_without_repeating_entry(self):
        bucket,state=self.open_partial('SHORT')
        self.controller=dispatch.Controller(self.store,self.venue,ROUTES2,after_exit_policy=dispatch.AFTER_EXIT)
        self.assertTrue(simple.quiet(self.store.load(bucket),now_ms=self.venue.now()+10000,feed=self.venue.fill_wakeups))
        self.cycle(bucket,False)
        self.assertEqual(len([r for r in self.venue.requests if r['proposal']['operation']=='ENTRY']),1)
        self.assertEqual(self.store.load(bucket)['simple_execution_audit'],state['simple_execution_audit'])

    def test_lost_entry_reply_is_resolved_and_protected_without_resubmission(self):
        self.open_partial('SHORT',lost=True)
