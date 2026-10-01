"""Fault-injected actual controller/store tests; no keys or real exchange calls."""
from copy import deepcopy
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
from decimal import Decimal
import os
import threading
import unittest
from unittest.mock import patch

from . import emergency_close as m, card_lifecycle as life
from . import filled_quantity_dispatch as dispatch
from .filled_dispatch_store import DispatchStore,DispatchError,SCHEMA
from .test_filled_quantity_dispatch import (NoExternal,Venue as NormalVenue,
    state_from_case,ROUTES2,AGENT)
from . import test_filled_quantity_dispatch as fixtures
from .test_filled_quantity_exits import original,META
from .test_card_lifecycle import T,A,B,fill,terminal

CI=os.environ.get('HL_JOURNAL_CI_URL')


class ReconciledFillFeed:
    def entry_allowed(self,account):return account in (A,B)


class Venue(NormalVenue):
    def __init__(self):
        super().__init__();self.mark='10';self.reject_cancel=False
    def sample(self,account,symbol):
        return dict(mark_price=self.mark,at_ms=self.t)
    def authorize(self,state,proposal):
        if not m.Controller.proposal_without_io(state,proposal):
            raise DispatchError('EMERGENCY_WIRE_NOT_VERIFIED')
    def send(self,request):
        if self.reject_cancel and request['proposal']['operation']=='EMERGENCY_CANCEL':
            self.sent+=1;self.requests.append(deepcopy(request));self.t+=10
            return dict(status='err',response='Temporary cancellation rejection')
        modified=deepcopy(request)
        if modified['proposal']['operation']=='EMERGENCY_CLOSE':
            modified['proposal']['operation']='CLOSE_PASSED_TAKE'
        reply=super().send(modified)
        self.requests[-1]=deepcopy(request)
        return reply


class EmergencyPureTests(NoExternal):
    @staticmethod
    def closed_release_state():
        state=state_from_case(q='40',stop='40');b=state['bindings'][0]
        snap=state['evidence']['snapshot'];snap['open_orders']=[];snap['position_quantity']='0'
        snap['fills'].append(fill(b,'STOP',qty='40'))
        snap['terminal_orders']=[{**terminal(b,'ENTRY','40'),'state':'CANCELED'},terminal(b,'STOP','40')]
        state['emergency']=dict(version=m.VERSION,phase='CLOSED_VERIFIED',latched_at_ms=T-10,
            closed_at_ms=T,card_id=b['card_id'],pending_close=None,pending_cancel=None,
            requests=[dict(phase='OBSERVED',proposal=dict(operation='EMERGENCY_CLOSE'),
                           observed_oid=b['orders']['STOP'][0])])
        return state

    def test_closed_release_requires_fresh_complete_exact_finality(self):
        good=self.closed_release_state()
        self.assertTrue(m.closed_release_proof(good,now_ms=T,not_before_ms=T))
        for invalid in ('stale','pre_release','future','account','symbol','pending','unknown',
                        'active','incomplete','closure_time','working_order'):
            with self.subTest(invalid=invalid):
                state=deepcopy(good);snap=state['evidence']['snapshot'];now=T;start=T
                if invalid=='stale':now=T+5001
                elif invalid=='pre_release':start=T+1
                elif invalid=='future':now=T-1
                elif invalid=='account':snap['account']=B
                elif invalid=='symbol':snap['symbol']='BTC'
                elif invalid=='pending':state['pending']='a'*64
                elif invalid=='unknown':state['emergency']['requests'][0]['phase']='OUTCOME_UNKNOWN'
                elif invalid=='active':state['emergency']['phase']='ACTIVE'
                elif invalid=='incomplete':snap['history_complete']=False
                elif invalid=='closure_time':state['emergency']['closed_at_ms']=T+1
                else:
                    opened=state_from_case(q='40',stop='40')['evidence']['snapshot']['open_orders']
                    snap['open_orders']=opened
                self.assertFalse(m.closed_release_proof(state,now_ms=now,not_before_ms=start))

    def test_release_inventory_rejects_unknown_or_target_orders_and_positions(self):
        state=self.closed_release_state();account=state['account'];symbol=state['symbol']
        self.assertEqual(m._release_inventory(account,symbol,'long_account',[state],[],
                         dict(assetPositions=[])),dict(orders=[],positions=[]))
        for orders,positions in (([dict(coin=symbol,oid=1000)],[]),
                                ([dict(coin='ETH',oid=123)],[]),
                                ([],[dict(position=dict(coin=symbol,szi='1'))]),
                                ([],[dict(position=dict(coin='ETH',szi='1'))])):
            with self.subTest(orders=orders,positions=positions):
                with self.assertRaises(DispatchError):
                    m._release_inventory(account,symbol,'long_account',[state],orders,
                                         dict(assetPositions=positions))

    def test_release_inventory_allows_only_consistent_unrelated_owned_position(self):
        closed=self.closed_release_state();other=state_from_case(q='40',stop='40',take='40')
        other['symbol']='ETH'
        for b in other['bindings']:b['symbol']='ETH'
        other['evidence']['snapshot']['symbol']='ETH'
        orders=[dict(coin='ETH',oid=int(oid)) for b in other['bindings']
                for ids in b['orders'].values() for oid in ids]
        positions=dict(assetPositions=[dict(position=dict(coin='ETH',szi='40'))])
        result=m._release_inventory(closed['account'],closed['symbol'],'long_account',
                                     [closed,other],orders,positions)
        self.assertEqual(result['positions'],[('ETH','40')])
        other['pending']='b'*64
        with self.assertRaisesRegex(DispatchError,'REQUEST_OR_OWNER'):
            m._release_inventory(closed['account'],closed['symbol'],'long_account',
                                 [closed,other],orders,positions)

    def test_recent_protected_pending_entry_uses_durable_normal_checkpoint(self):
        state=state_from_case(q='40',stop='40',take='40')
        # The remaining ENTRY can fill later; the existing 40-unit tranche is
        # currently protected. A pending identity does not age this checkpoint.
        state['pending']='a'*64
        self.assertTrue(any(o['oid'] in state['bindings'][0]['orders']['ENTRY']
                            for o in state['evidence']['snapshot']['open_orders']))
        self.assertTrue(m.recent_normal_checkpoint(state,now_ms=T+4999))
        self.assertFalse(m.recent_normal_checkpoint(state,now_ms=T+5000))

    def test_recent_checkpoint_never_suppresses_dirty_gap_or_uncovered_tranche(self):
        state=state_from_case(q='40',stop='40',take='40')
        class Feed:
            def __init__(self,dirty):self.dirty=dirty
            def dirty_symbols(self,account):return self.dirty
        for dirty, expected in (((),True),(('BTC',),True),(('DOGE',),False),(None,False)):
            with self.subTest(dirty=dirty):
                self.assertEqual(m.recent_normal_checkpoint(state,now_ms=T+1,
                                 fill_wakeups=Feed(dirty)),expected)
        uncovered=state_from_case(q='40',stop='20',take='40')
        uncovered['evidence']['snapshot']['fills'][0]['at_ms']=T
        self.assertIsNone(m.trigger(uncovered,now_ms=T+1))
        self.assertFalse(m.recent_normal_checkpoint(uncovered,now_ms=T+1))

    def test_recent_checkpoint_requires_matching_complete_current_evidence(self):
        good=state_from_case(q='40',stop='40',take='40')
        for invalid in ('future','history','orders','bindings','account','symbol','position'):
            with self.subTest(invalid=invalid):
                state=deepcopy(good);snap=state['evidence']['snapshot']
                if invalid=='future':snap['at_ms']=T+1
                elif invalid=='history':snap['history_complete']=False
                elif invalid=='orders':snap['orders_complete']=False
                elif invalid=='bindings':state['evidence']['bindings']=[]
                elif invalid=='account':state['account']=B
                elif invalid=='symbol':state['symbol']='BTC'
                else:snap['position_quantity']='41'
                self.assertFalse(m.recent_normal_checkpoint(state,now_ms=T))

    def test_fresh_stop_coverage_never_relabels_uncertain_or_uncovered_evidence(self):
        good=state_from_case(q='40',stop='40')
        # Missing TP is ordinary maintenance; an exact original STOP already
        # covers the observed partial fill, even while the ENTRY can fill more.
        self.assertTrue(m.fresh_stop_coverage(good,now_ms=T))
        for invalid in ('age','future','history','orders','bindings','account','symbol',
                        'pending','position','uncovered','oversized','terms','incident'):
            with self.subTest(invalid=invalid):
                state=deepcopy(good);snap=state['evidence']['snapshot'];now=T
                if invalid=='age':now=T+5001
                elif invalid=='future':snap['at_ms']=T+1
                elif invalid=='history':snap['history_complete']=False
                elif invalid=='orders':snap['orders_complete']=False
                elif invalid=='bindings':state['evidence']['bindings']=[]
                elif invalid=='account':state['account']=B
                elif invalid=='symbol':state['symbol']='BTC'
                elif invalid=='pending':state['pending']='a'*64
                elif invalid=='position':snap['position_quantity']='41'
                elif invalid in ('uncovered','oversized','terms'):
                    stop=next(o for o in snap['open_orders'] if o['oid'] in state['bindings'][0]['orders']['STOP'])
                    if invalid=='terms':stop['trigger_price']='9.7'
                    else:stop['quantity']='20' if invalid=='uncovered' else '41'
                else:state['emergency']=dict(phase='ACTIVE')
                self.assertFalse(m.fresh_stop_coverage(state,now_ms=now))

    def test_reconciled_stop_coverage_does_not_spend_metadata_or_mark_requests(self):
        for take in (None,'40'):
            with self.subTest(take=take):
                state=state_from_case(q='40',stop='40',take=take)
                class Store:
                    domain='software'
                    def load(self,bucket):return deepcopy(state)
                venue=Venue();venue.mark='1'  # beyond the original stop
                normal=dispatch.Controller(Store(),venue,ROUTES2)
                current=m.Controller(normal,venue)
                with patch.object(normal,'refresh',return_value=deepcopy(state)) as refresh, \
                        patch.object(venue,'metadata',side_effect=AssertionError('NO_UNUSED_METADATA')), \
                        patch.object(venue,'sample',side_effect=AssertionError('NO_UNUSED_MARK')):
                    result=current.cycle(state['bucket'],send=True)
                self.assertEqual(result['status'],'STOP_OBSERVED_OR_NO_EXPOSURE')
                refresh.assert_called_once_with(state['bucket'],emergency=True,emergency_wait_ms=250)
                self.assertEqual(venue.sent,0)

    def test_younger_uncovered_partial_fill_checks_crossed_stop_before_deadline(self):
        state=state_from_case(q='40',stop='20',take='40')
        state['evidence']['snapshot']['fills'][0]['at_ms']=T
        class Store:
            domain='software'
            def load(self,bucket):return deepcopy(state)
        venue=Venue();venue.t=T+100;venue.mark=state['bindings'][0]['prices']['stop']
        self.assertIsNone(m.trigger(state,now_ms=venue.t))
        normal=dispatch.Controller(Store(),venue,ROUTES2)
        current=m.Controller(normal,venue)
        with patch.object(normal,'refresh',return_value=deepcopy(state)), \
                patch.object(venue,'sample',wraps=venue.sample) as sample, \
                patch.object(venue,'metadata',side_effect=AssertionError('NO_PREVIEW_METADATA')):
            result=current.cycle(state['bucket'],send=False)
        self.assertEqual(result['status'],'EMERGENCY_PREVIEW')
        self.assertEqual(result['cause']['reason'],'STOP_LEVEL_PASSED_UNPROTECTED')
        self.assertEqual(result['cause']['uncovered_quantity'],'20')
        sample.assert_called_once_with(state['account'],state['symbol'])
        self.assertEqual(venue.sent,0)

    def test_reconciliation_failure_never_spends_metadata_or_mark_quota(self):
        state=state_from_case(q='40',stop='40',take='40')
        class Store:
            domain='software'
            def load(self,bucket):return deepcopy(state)
        venue=Venue();normal=dispatch.Controller(Store(),venue,ROUTES2)
        current=m.Controller(normal,venue)
        with patch.object(normal,'refresh',side_effect=DispatchError('TESTNET_REQUEST_BUDGET_EXHAUSTED')), \
                patch.object(venue,'metadata',side_effect=AssertionError('NO_UNUSED_METADATA')), \
                patch.object(venue,'sample',side_effect=AssertionError('NO_UNUSED_MARK')):
            with self.assertRaisesRegex(DispatchError,'TESTNET_REQUEST_BUDGET_EXHAUSTED'):
                current.cycle(state['bucket'],send=True)
        self.assertEqual(venue.sent,0)

    def test_join_wait_never_crosses_original_uncovered_fill_deadline(self):
        state=state_from_case(q='40',stop='20',take='40')
        state['evidence']['snapshot']['fills'][0]['at_ms']=T
        class Store:
            domain='software'
            def load(self,bucket):return deepcopy(state)
        venue=Venue();normal=dispatch.Controller(Store(),venue,ROUTES2)
        current=m.Controller(normal,venue)
        for age,wait in ((0,250),(4750,250),(4999,1),(5000,0),(6000,0)):
            with self.subTest(age=age):
                venue.t=T+age
                with patch.object(normal,'refresh',return_value=deepcopy(state)) as refresh:
                    current._refresh_normal(state)
                refresh.assert_called_once_with(state['bucket'],emergency=True,emergency_wait_ms=wait)
        self.assertEqual(venue.sent,0)

    def test_unresolved_pending_close_spends_no_metadata_during_no_resend_window(self):
        state=state_from_case(q='40',take='40');eid='a'*64
        state['emergency']=dict(version=m.VERSION,phase='ACTIVE',card_id=state['bindings'][0]['card_id'],
            pending_close=eid,pending_cancel=None,requests=[dict(request_id=eid,phase='OUTCOME_UNKNOWN',
                attempt_at_ms=T-1,proposal=dict(operation='EMERGENCY_CLOSE',
                    action=dict(orders=[dict(c='0x'+'a'*32)])))])
        class Store:
            domain='software'
            def load(self,bucket):return deepcopy(state)
        venue=Venue();normal=dispatch.Controller(Store(),venue,ROUTES2)
        current=m.Controller(normal,venue)
        with patch.object(normal,'refresh',side_effect=AssertionError('CLOSE_NOT_RESOLVED')), \
                patch.object(venue,'lookup',return_value=dict(status='unknownOid')) as lookup, \
                patch.object(venue,'metadata',side_effect=AssertionError('NO_UNUSED_METADATA')), \
                patch.object(venue,'sample',side_effect=AssertionError('NO_UNUSED_MARK')):
            with self.assertRaisesRegex(DispatchError,'EMERGENCY_CLOSE_OUTCOME_UNKNOWN_NO_RESEND'):
                current.cycle(state['bucket'],send=True)
        lookup.assert_called_once_with(state['account'],'0x'+'a'*32)
        self.assertEqual(venue.sent,0)
        self.assertEqual(len(state['emergency']['requests']),1)

    def test_emergency_controller_preserves_same_explicit_fill_feed(self):
        class Store:domain='software'
        normal_venue=NormalVenue();feed=object();normal_venue.fill_wakeups=feed
        normal=dispatch.Controller(Store(),normal_venue,ROUTES2)
        emergency=m.Controller(normal,Venue())
        self.assertIs(emergency.venue.fill_wakeups,feed)

    def test_supervisor_coalesces_pending_entry_then_takes_over_aged_checkpoint(self):
        selected=state_from_case(q='40',stop='40',take='40')
        for age,dirty,expected in ((4999,(),0),(5000,(),1),(1,('DOGE',),1),(1,None,1)):
            with self.subTest(age=age,dirty=dirty):
                stop=threading.Event()
                class Feed:
                    def dirty_symbols(self,account):return dirty
                class Store:
                    domain='software'
                    def load(self,bucket):return deepcopy(selected)
                    def for_account(self,account):
                        stop.set()  # complete exactly one bounded scan
                        return [deepcopy(selected)]
                venue=Venue();venue.t=T+age;venue.fill_wakeups=Feed()
                venue.env=dict(HL_TESTNET_EMERGENCY_CLOSE=m.APPROVAL,
                    HL_TESTNET_LONG_ENTRY_ENABLED='false',HL_TESTNET_SHORT_ENTRY_ENABLED='false')
                normal=dispatch.Controller(Store(),venue,ROUTES2)
                current=m.Controller(normal,venue)
                status=dict(running=False,last_status=None,last_pass_at_ms=None)
                with patch.object(m,'_thread',None),patch.object(m,'_stop_event',None), \
                        patch.object(m,'_health',status), \
                        patch.object(m,'Controller',return_value=current), \
                        patch.object(venue,'metadata',wraps=venue.metadata) as metadata, \
                        patch.object(normal,'refresh',return_value=deepcopy(selected)) as refresh:
                    self.assertTrue(m.start(normal,[('long_account',dict(account=A),None,None)],stop))
                    m._thread.join(1)
                    self.assertFalse(m._thread.is_alive())
                    self.assertEqual(metadata.call_count,0)
                    self.assertEqual(refresh.call_count,expected)
                    self.assertEqual(status['last_status'],'PASS_COMPLETE')
                self.assertEqual(venue.sent,0)

    def test_known_deadline_latches_before_reconciliation_io_and_never_sends_old_quantity(self):
        class MemoryStore:
            domain='software'
            def __init__(self):self.state=state_from_case(q='40',take='40');self.events=[]
            def load(self,bucket):return deepcopy(self.state)
            def change(self,bucket,revision,event,now,update):
                if revision!=self.state['revision']:
                    raise DispatchError('CONCURRENT_DISPATCH_RELOAD_REQUIRED')
                value=deepcopy(self.state);update(None,value);value['revision']+=1
                self.state=value;self.events.append(event)
                return deepcopy(value)
        for send in (False,True):
            with self.subTest(send=send):
                store=MemoryStore();venue=Venue();normal=dispatch.Controller(store,venue,ROUTES2)
                emergency=m.Controller(normal,venue)
                def unavailable(bucket,*,emergency=False,emergency_wait_ms=250):
                    self.assertEqual('emergency' in store.state,send)
                    if send:
                        self.assertEqual(store.state['emergency']['requests'],[])
                        self.assertIs(store.state['emergency']['provisional'],True)
                    raise DispatchError('PUBLIC_READ_UNAVAILABLE')
                with patch.object(normal,'refresh',side_effect=unavailable):
                    with self.assertRaisesRegex(DispatchError,'PUBLIC_READ_UNAVAILABLE'):
                        emergency.cycle(store.state['bucket'],send=send)
                self.assertEqual(venue.sent,0)
                self.assertEqual(store.events,['EMERGENCY_LATCHED_NEW_ENTRIES_BLOCKED'] if send else [])

    def test_provisional_withdrawal_requires_fresh_exact_stop_and_no_attempts(self):
        protected=state_from_case(q='100',stop='100')
        protected['emergency']=dict(version=m.VERSION,phase='ACTIVE',provisional=True,
            latched_at_ms=T,card_id=protected['bindings'][0]['card_id'],
            reason='STOP_VERIFICATION_DEADLINE',uncovered_since_ms=T-10000,
            uncovered_quantity='100',requests=[],pending_close=None,pending_cancel=None)
        self.assertTrue(m.provisional_stop_proof(protected,now_ms=T))
        for variant in ('explicit','promoted','attempted','pending-close','pending-cancel',
                        'normal-pending','before-freeze','old','incomplete-history',
                        'incomplete-orders','wrong-account','wrong-symbol','bindings',
                        'position','uncovered-stop','wrong-stop-terms','flat-not-final'):
            with self.subTest(variant=variant):
                state=deepcopy(protected);now=T;snap=state['evidence']['snapshot']
                if variant=='explicit':del state['emergency']['provisional']
                elif variant=='promoted':state['emergency']['provisional']=False
                elif variant=='attempted':state['emergency']['requests']=[dict(phase='ABORTED_UNSENT')]
                elif variant=='pending-close':state['emergency']['pending_close']='a'*64
                elif variant=='pending-cancel':state['emergency']['pending_cancel']='a'*64
                elif variant=='normal-pending':state['pending']='a'*64
                elif variant=='before-freeze':state['emergency']['latched_at_ms']=T+1;now=T+1
                elif variant=='old':now=T+5001
                elif variant=='incomplete-history':snap['history_complete']=False
                elif variant=='incomplete-orders':snap['orders_complete']=False
                elif variant=='wrong-account':state['account']=B
                elif variant=='wrong-symbol':state['symbol']='BTC'
                elif variant=='bindings':state['evidence']['bindings']=[]
                elif variant=='position':snap['position_quantity']='101'
                elif variant=='uncovered-stop':snap['open_orders'][0]['quantity']='99'
                elif variant=='wrong-stop-terms':snap['open_orders'][0]['reduce_only']=False
                else:
                    empty=state_from_case(q='0');empty['emergency']=state['emergency'];state=empty
                self.assertFalse(m.provisional_stop_proof(state,now_ms=now))
        from .test_card_lifecycle import closed
        flat=state_from_case(q='100',stop='100',take='100')
        flat['evidence']['snapshot']=closed(flat['bindings'][0]);flat['emergency']=protected['emergency']
        self.assertTrue(m.provisional_stop_proof(flat,now_ms=T))

    def test_provisional_stop_proof_allows_only_unambiguous_owned_tp_resize(self):
        state=state_from_case(q='100',stop='100',take='120')
        state['emergency']=dict(version=m.VERSION,phase='ACTIVE',provisional=True,
            latched_at_ms=T,card_id=state['bindings'][0]['card_id'],
            reason='STOP_VERIFICATION_DEADLINE',uncovered_since_ms=T-10000,
            uncovered_quantity='100',requests=[],pending_close=None,pending_cancel=None)
        self.assertTrue(m.provisional_stop_proof(state,now_ms=T))
        proposal=dispatch.choose({key:value for key,value in state.items() if key!='emergency'},
            ROUTES2,META,dict(mark_price='10',at_ms=T),now_ms=T)
        self.assertEqual((proposal['operation'],proposal['leg'],proposal['quantity']),
                         ('MODIFY_EXIT','TAKE_PROFIT','100'))
        self.assertEqual(dispatch.requested_order(proposal['action'])['p'],'10.2')
        for variant in ('other-live-card','foreign-account','non-reducing-tp','wrong-tp-price'):
            with self.subTest(variant=variant):
                current=deepcopy(state)
                if variant in ('other-live-card','foreign-account'):
                    other=state_from_case(q='100',n=2,side='SHORT' if variant=='foreign-account' else 'LONG',
                        stop='100',take='100')
                    current['bindings']+=other['bindings']
                    current['originals'].update(other['originals'])
                    current['evidence']['bindings']=deepcopy(current['bindings'])
                    if variant=='other-live-card':
                        for key in ('fills','open_orders','terminal_orders'):
                            current['evidence']['snapshot'][key]+=other['evidence']['snapshot'][key]
                        current['evidence']['snapshot']['position_quantity']='200'
                else:
                    take=current['evidence']['snapshot']['open_orders'][1]
                    if variant=='non-reducing-tp':take['reduce_only']=False
                    else:take['price']='10.3'
                self.assertFalse(m.provisional_stop_proof(current,now_ms=T))

    def test_early_freeze_is_withdrawn_only_after_fresh_stop_checkpoint(self):
        class MemoryStore:
            domain='software'
            def __init__(self):self.state=state_from_case(q='100');self.events=[]
            def load(self,bucket):return deepcopy(self.state)
            def change(self,bucket,revision,event,now,update):
                if revision!=self.state['revision']:
                    raise DispatchError('CONCURRENT_DISPATCH_RELOAD_REQUIRED')
                value=deepcopy(self.state);update(None,value);value['revision']+=1
                self.state=value;self.events.append(event)
                return deepcopy(value)
        for invalid in (False,True):
            with self.subTest(incomplete=invalid):
                store=MemoryStore();venue=Venue();normal=dispatch.Controller(store,venue,ROUTES2)
                emergency=m.Controller(normal,venue)
                def observed(bucket,*,emergency=False,emergency_wait_ms=250):
                    self.assertIs(store.state['emergency']['provisional'],True)
                    state=state_from_case(q='100',stop='100');state['revision']=store.state['revision']+1
                    state['emergency']=deepcopy(store.state['emergency'])
                    if invalid:state['evidence']['snapshot']['history_complete']=False
                    store.state=state
                    return deepcopy(state)
                with patch.object(normal,'refresh',side_effect=observed):
                    result=emergency.cycle(store.state['bucket'],send=True)
                self.assertEqual(venue.sent,0)
                if invalid:
                    self.assertEqual(result['status'],'PROVISIONAL_STOP_RECONCILIATION_REQUIRED')
                    self.assertIs(store.state['emergency']['provisional'],True)
                else:
                    self.assertEqual(result['status'],'STOP_OBSERVED_OR_NO_EXPOSURE')
                    self.assertNotIn('emergency',store.state)
                    self.assertEqual(store.events,['EMERGENCY_LATCHED_NEW_ENTRIES_BLOCKED',
                        'UNSENT_EMERGENCY_WITHDRAWN_AFTER_FRESH_STOP_PROOF'])

    def test_provisional_latch_cannot_begin_an_emergency_attempt(self):
        from unittest.mock import Mock
        store=Mock();store.domain='software';venue=Venue();normal=dispatch.Controller(store,venue,ROUTES2)
        emergency=m.Controller(normal,venue)
        state=state_from_case(q='100');state['emergency']=dict(provisional=True)
        with self.assertRaisesRegex(DispatchError,'PROVISIONAL_EMERGENCY_HAS_NO_SEND_AUTHORITY'):
            emergency._begin(state,{})
        store.change.assert_not_called();self.assertEqual(venue.sent,0)

    def test_provisional_cleanup_needs_actual_filled_exit_and_exact_flat_ownership(self):
        state=state_from_case(q='40',stop='40');binding=state['bindings'][0]
        snap=state['evidence']['snapshot'];snap['fills'].append(fill(binding,'STOP',qty='40'))
        snap['open_orders']=[order for order in snap['open_orders'] if order['oid'] not in binding['orders']['STOP']]
        snap['terminal_orders'].append(terminal(binding,'STOP','40'));snap['position_quantity']='0'
        state['emergency']=dict(version=m.VERSION,phase='ACTIVE',provisional=True,
            latched_at_ms=T,card_id=binding['card_id'],reason='STOP_VERIFICATION_DEADLINE',
            uncovered_since_ms=T-10000,uncovered_quantity='40',
            requests=[],pending_close=None,pending_cancel=None)
        self.assertFalse(m.provisional_stop_proof(state,now_ms=T))
        self.assertTrue(m.provisional_cleanup_proof(state,now_ms=T))
        for variant in ('old','incomplete','foreign-position','unconfirmed-exit','missing-fill','attempted'):
            with self.subTest(variant=variant):
                current=deepcopy(state);now=T;snap=current['evidence']['snapshot']
                if variant=='old':now=T+5001
                elif variant=='incomplete':snap['orders_complete']=False
                elif variant=='foreign-position':snap['position_quantity']='1'
                elif variant=='unconfirmed-exit':snap['terminal_orders']=[]
                elif variant=='missing-fill':snap['fills']=snap['fills'][:1]
                else:current['emergency']['requests']=[dict(phase='ABORTED_UNSENT')]
                self.assertFalse(m.provisional_cleanup_proof(current,now_ms=now))
        other=state_from_case(q='0',n=2)
        current=deepcopy(state);current['bindings']+=other['bindings']
        current['originals'].update(other['originals'])
        current['evidence']['bindings']=deepcopy(current['bindings'])
        current['evidence']['snapshot']['open_orders']+=other['evidence']['snapshot']['open_orders']
        self.assertFalse(m.provisional_cleanup_proof(current,now_ms=T))

    def test_supervisor_shutdown_timeout_is_reported_until_own_worker_exits(self):
        selected=state_from_case(q='40',stop='40',take='40')
        entered=threading.Event();release=threading.Event();stop=threading.Event()
        class Store:
            def for_account(self,account):return [deepcopy(selected)]
        class Adapter:
            env=dict(HL_TESTNET_EMERGENCY_CLOSE=m.APPROVAL,
                HL_TESTNET_LONG_ENTRY_ENABLED='false',HL_TESTNET_SHORT_ENTRY_ENABLED='false')
            sent=0
            def now(self):return T+6000
        class Supervisor:
            def __init__(self,normal):self.store=normal.store;self.venue=normal.venue
            def cycle(self,bucket,*,send):
                entered.set()
                if not release.wait(2):raise AssertionError('TEST_WORKER_WAS_NOT_RELEASED')
                return dict(status='STOP_OBSERVED_OR_NO_EXPOSURE')
        class Normal:
            store=Store();venue=Adapter()
        with patch.object(m,'_thread',None),patch.object(m,'_stop_event',None),patch.object(m,'Controller',Supervisor):
            self.assertTrue(m.start(Normal(),[('long_account',dict(account=A),None,None)],stop))
            try:
                self.assertTrue(entered.wait(1))
                with self.assertRaisesRegex(DispatchError,'STOP_EVENT_MISMATCH'):
                    m.stop_supervisor(threading.Event(),timeout=0)
                self.assertFalse(stop.is_set())
                self.assertFalse(m.stop_supervisor(stop,timeout=0))
                self.assertTrue(m._thread.is_alive())
            finally:
                release.set()
                self.assertTrue(m.stop_supervisor(stop,timeout=1))
            self.assertFalse(m._thread.is_alive())
            self.assertFalse(m.health()['running'])

    def test_scoped_supervisor_ignores_unrelated_market_conflicts(self):
        selected=state_from_case(q='40',stop='40',take='40')
        other=deepcopy(selected);other['bucket']='b'*64
        called=threading.Event();stop=threading.Event();seen=[]
        environment=dict(HL_TESTNET_EMERGENCY_CLOSE=m.APPROVAL,
            HL_TESTNET_LONG_ENTRY_ENABLED='false',HL_TESTNET_SHORT_ENTRY_ENABLED='false')
        class Store:
            def load(self,bucket):
                self.assert_bucket=bucket
                return deepcopy(selected)
            def for_account(self,account):
                return [deepcopy(other),deepcopy(selected)]
        class Adapter:
            env=environment;sent=0
            def now(self):return T+6000
        class Supervisor:
            def __init__(self,normal):self.store=normal.store;self.venue=normal.venue
            def cycle(self,bucket,*,send):
                seen.append(bucket)
                if bucket!=selected['bucket']:
                    raise DispatchError('CONCURRENT_DISPATCH_RELOAD_REQUIRED')
                called.set()
                return dict(status='STOP_OBSERVED_OR_NO_EXPOSURE')
        class Normal:
            store=Store();venue=Adapter()
        health=dict(running=False,last_status=None,last_pass_at_ms=None)
        with patch.object(m,'_thread',None),patch.object(m,'_health',health),patch.object(m,'Controller',Supervisor):
            self.assertTrue(m.start(Normal(),[('long_account',dict(account=A),None,None)],
                stop,only_bucket=selected['bucket']))
            try:self.assertTrue(called.wait(1))
            finally:
                stop.set();m._thread.join(1)
            self.assertEqual(seen,[selected['bucket']])
            self.assertEqual(health['last_status'],'PASS_COMPLETE')
            self.assertFalse(m._thread.is_alive())

    def test_scoped_supervisor_rejects_another_account_before_start(self):
        selected=state_from_case(q='40',stop='40',take='40')
        class Store:
            def load(self,bucket):return deepcopy(selected)
        class Adapter:
            env=dict(HL_TESTNET_EMERGENCY_CLOSE=m.APPROVAL,
                HL_TESTNET_LONG_ENTRY_ENABLED='false',HL_TESTNET_SHORT_ENTRY_ENABLED='false')
        class Normal:
            store=Store();venue=Adapter()
        with self.assertRaisesRegex(DispatchError,'SCOPE_ACCOUNT_NOT_SELECTED'):
            m.start(Normal(),[('short_account',dict(account=B),None,None)],
                threading.Event(),only_bucket=selected['bucket'])

    def test_emergency_observation_overtakes_stalled_normal_read_without_stale_write(self):
        # Hold a real normal refresh in public I/O while the emergency cycle
        # completes its own checkpoint. The old reader must discard its result,
        # preserving the newer revision instead of overwriting safety evidence.
        class MemoryStore:
            domain='software'
            def __init__(self):
                self.state=state_from_case(q='100',stop='100',take='100')
            def load(self,bucket):
                return deepcopy(self.state)
            def pending_record(self,conn,state):
                return None
            def change(self,bucket,revision,event,now,update):
                if revision!=self.state['revision']:
                    raise DispatchError('CONCURRENT_DISPATCH_RELOAD_REQUIRED')
                value=deepcopy(self.state)
                update(None,value)
                value['revision']+=1
                self.state=value
                return deepcopy(value)
        store=MemoryStore();normal_venue=NormalVenue()
        entered=threading.Event();release=threading.Event();calls=[]
        def collect(value):
            calls.append(1)
            if len(calls)==1:
                entered.set()
                if not release.wait(2):
                    raise AssertionError('TEST_PUBLIC_READ_WAS_NOT_RELEASED')
            observed=deepcopy(value)
            normal_venue.t+=1
            observed['snapshot']['at_ms']=normal_venue.t
            return observed
        normal_venue.collect=collect
        normal=dispatch.Controller(store,normal_venue,ROUTES2,after_exit_policy=dispatch.AFTER_EXIT)
        emergency=m.Controller(normal,Venue())
        with ThreadPoolExecutor(max_workers=2) as pool:
            first=pool.submit(normal.refresh,store.state['bucket'])
            self.assertTrue(entered.wait(1))
            second_done=threading.Event()
            def inspect_emergency():
                try:
                    return emergency.cycle(store.state['bucket'],send=False)
                finally:
                    second_done.set()
            second=pool.submit(inspect_emergency)
            try:
                self.assertTrue(second_done.wait(1))
                self.assertEqual(second.result(timeout=1)['status'],'STOP_OBSERVED_OR_NO_EXPOSURE')
                advanced=deepcopy(store.state)
            finally:
                release.set()
            with self.assertRaisesRegex(DispatchError,'CONCURRENT_DISPATCH_RELOAD_REQUIRED'):
                first.result(timeout=2)
        self.assertEqual(store.state,advanced)
        self.assertEqual(store.state['revision'],2)
        self.assertEqual(normal_venue.sent,0)
        self.assertEqual(emergency.venue.sent,0)

    def test_emergency_join_allows_fast_normal_checkpoint_to_commit_once(self):
        class MemoryStore:
            domain='software'
            def __init__(self):self.state=state_from_case(q='100',stop='100',take='100')
            def load(self,bucket):return deepcopy(self.state)
            def pending_record(self,conn,state):return None
            def change(self,bucket,revision,event,now,update):
                if revision!=self.state['revision']:
                    raise DispatchError('CONCURRENT_DISPATCH_RELOAD_REQUIRED')
                value=deepcopy(self.state);update(None,value)
                value['revision']+=1;self.state=value
                return deepcopy(value)
        store=MemoryStore();venue=NormalVenue()
        entered=threading.Event();release=threading.Event();joined=threading.Event();calls=[]
        def collect(value):
            calls.append(1);entered.set()
            if not release.wait(2):
                raise AssertionError('TEST_PUBLIC_READ_WAS_NOT_RELEASED')
            observed=deepcopy(value);venue.t+=1
            observed['snapshot']['at_ms']=venue.t
            return observed
        venue.collect=collect
        normal=dispatch.Controller(store,venue,ROUTES2)
        emergency=m.Controller(normal,venue)
        with ThreadPoolExecutor(max_workers=2) as pool:
            first=pool.submit(normal.refresh,store.state['bucket'])
            self.assertTrue(entered.wait(1))
            flight=normal._observation_flights[store.state['bucket']][0]
            wait=flight.done.wait
            def wait_for_normal(seconds):
                self.assertEqual(seconds,0.25);joined.set()
                return wait(seconds)
            try:
                with patch.object(flight.done,'wait',side_effect=wait_for_normal), \
                        patch.object(venue,'metadata',side_effect=AssertionError('NO_UNUSED_METADATA')), \
                        patch.object(venue,'sample',side_effect=AssertionError('NO_UNUSED_MARK')):
                    second=pool.submit(emergency.cycle,store.state['bucket'],send=False)
                    self.assertTrue(joined.wait(1))
                    release.set()
                    committed=first.result(timeout=1)
                    result=second.result(timeout=1)
            finally:
                release.set()
        self.assertEqual(result['status'],'STOP_OBSERVED_OR_NO_EXPOSURE')
        self.assertEqual(len(calls),1)
        self.assertEqual(store.state,committed)
        self.assertEqual(store.state['revision'],2)
        self.assertEqual(venue.sent,0)

    def test_changed_durable_bucket_before_action_rejects_collected_basis(self):
        class Store:
            domain='software'
            def __init__(self):self.state=state_from_case(q='40',stop='40',take='40')
            def load(self,bucket):return deepcopy(self.state)
        store=Store();venue=Venue();normal=dispatch.Controller(store,venue,ROUTES2)
        emergency=m.Controller(normal,venue)
        def another_checkpoint(bucket,**kwargs):
            collected=deepcopy(store.state)
            store.state['revision']+=1
            store.state['evidence']['snapshot']['at_ms']+=1
            return collected
        with patch.object(normal,'refresh',side_effect=another_checkpoint), \
                patch.object(venue,'metadata',side_effect=AssertionError('NO_STALE_PLAN')), \
                patch.object(venue,'sample',side_effect=AssertionError('NO_STALE_MARK')):
            with self.assertRaisesRegex(DispatchError,'CONCURRENT_DISPATCH_RELOAD_REQUIRED'):
                emergency.cycle(store.state['bucket'],send=True)
        self.assertEqual(store.state['revision'],2)
        self.assertEqual(venue.sent,0)
        self.assertNotIn('emergency',store.state)

    def test_repeated_safety_checkpoints_do_not_starve_normal_maintenance(self):
        # Both the ordinary collection and its later market sample are overtaken
        # on every pass. The safety worker's actual controller saves the winning
        # checkpoint; normal planning must consume it without repeating any send.
        # A still-working partial ENTRY needs planning rather than the completed
        # fully-protected-position fast path.
        class MemoryStore:
            domain='software'
            def __init__(self):
                self.state=state_from_case(q='40',stop='40',take='40')
                self.writes=[]
            def load(self,bucket):return deepcopy(self.state)
            def pending_record(self,conn,state):return None
            def change(self,bucket,revision,event,now,update):
                if revision!=self.state['revision']:
                    raise DispatchError('CONCURRENT_DISPATCH_RELOAD_REQUIRED')
                value=deepcopy(self.state);update(None,value)
                value['revision']+=1;self.state=value;self.writes.append(event)
                return deepcopy(value)
            def reserve(self,*args):raise AssertionError('NO_REQUEST_RESERVATION')
            def begin(self,*args):raise AssertionError('NO_ATTEMPT')
        store=MemoryStore();venue=NormalVenue();safety_read=False
        normal=dispatch.Controller(store,venue,ROUTES2,after_exit_policy=dispatch.AFTER_EXIT)
        emergency=m.Controller(normal,Venue())
        def safety_checkpoint():
            nonlocal safety_read
            safety_read=True
            try:
                result=emergency.cycle(store.state['bucket'],send=False)
                self.assertEqual(result['status'],'STOP_OBSERVED_OR_NO_EXPOSURE')
            finally:safety_read=False
        def collect(value):
            if not safety_read:safety_checkpoint()
            observed=deepcopy(value);venue.t+=1
            observed['snapshot']['at_ms']=venue.t
            return observed
        def sample(account,symbol):
            safety_checkpoint()
            return dict(mark_price='10',at_ms=venue.t)
        venue.collect=collect;venue.sample=sample
        for _ in range(10):
            before=store.state['revision']
            result=normal.cycle(store.state['bucket'],send=True,allow_new_entries=False)
            self.assertEqual(result,dict(status='NO_ACTION_NEEDED',order_requests_sent=0))
            self.assertEqual(store.state['revision'],before+2)
        self.assertEqual(store.writes,['PUBLIC_RECONCILIATION']*20)
        self.assertEqual(venue.sent,0);self.assertEqual(emergency.venue.sent,0)

    def test_plan_reload_recomputes_quantity_from_winning_partial_fill(self):
        state=state_from_case(q='40',stop='40',take='40')
        class Store:
            domain='software'
            def load(self,bucket):return deepcopy(current)
        current=deepcopy(state);venue=NormalVenue()
        normal=dispatch.Controller(Store(),venue,ROUTES2,after_exit_policy=dispatch.AFTER_EXIT)
        def sample(account,symbol):
            snap=current['evidence']['snapshot'];binding=current['bindings'][0]
            second=fill(binding,qty='60');second.update(fill_id='later-entry-fill',at_ms=T+1)
            snap['fills'].append(second);snap['position_quantity']='100';snap['at_ms']=T+2
            from .test_card_lifecycle import terminal
            snap['open_orders']=[o for o in snap['open_orders'] if o['oid'] not in binding['orders']['ENTRY']]
            ending=terminal(binding,'ENTRY','100');ending['at_ms']=T+2
            snap['terminal_orders'].append(ending)
            current['revision']+=1;venue.t=T+2
            return dict(mark_price='10',at_ms=venue.t)
        with patch.object(normal,'refresh',return_value=state),patch.object(venue,'sample',side_effect=sample):
            result=normal.cycle(state['bucket'],send=False)
        proposal=result['proposal']
        self.assertEqual((proposal['operation'],proposal['leg'],proposal['quantity'],proposal['old_oid']),
                         ('MODIFY_EXIT','STOP','100','11'))
        self.assertEqual(proposal['basis'],life.digest(current['evidence']))
        self.assertEqual(proposal['observed_at_ms'],T+2)
        self.assertEqual(venue.sent,0)

    def test_authorization_checkpoint_can_preserve_exact_action_and_begin_once(self):
        from unittest.mock import Mock
        state=state_from_case(q='40',take='40');venue=NormalVenue()
        sample=venue.sample(state['account'],state['symbol'])
        authorized=dispatch.choose(state,ROUTES2,META,sample,now_ms=T,
                                   after_exit_policy=dispatch.AFTER_EXIT)
        latest=deepcopy(state);latest['revision']+=1
        latest['evidence']['snapshot']['at_ms']=T+1;venue.t=T+1
        store=Mock(domain='software');store.load.return_value=latest
        request=dict(proposal=None,phase='OUTCOME_UNKNOWN',attempts=1)
        def committed(current,proposal,agent,now):
            self.assertEqual(current,latest)
            request['proposal']=deepcopy(proposal)
            return deepcopy(current),deepcopy(request)
        store.prepare_and_begin.side_effect=committed
        normal=dispatch.Controller(store,venue,ROUTES2,after_exit_policy=dispatch.AFTER_EXIT)
        result=normal._begin_cycle(state['bucket'],state,None,authorized,sample=sample,meta=META)
        self.assertEqual(result[2]['action'],authorized['action'])
        self.assertEqual(result[2]['sequence'],authorized['sequence'])
        self.assertEqual(result[2]['basis'],life.digest(latest['evidence']))
        self.assertEqual(result[2]['observed_at_ms'],T+1)
        store.prepare_and_begin.assert_called_once()
        store.reserve.assert_not_called();store.begin.assert_not_called()
        self.assertEqual(venue.sent,0)

    def test_authorization_reload_cannot_reuse_approval_for_larger_fill(self):
        from unittest.mock import Mock
        state=state_from_case(q='40',take='40');venue=NormalVenue()
        sample=venue.sample(state['account'],state['symbol'])
        authorized=dispatch.choose(state,ROUTES2,META,sample,now_ms=T,
                                   after_exit_policy=dispatch.AFTER_EXIT)
        latest=state_from_case(q='70',take='40');latest['revision']+=1
        latest['evidence']['snapshot']['at_ms']=T+1;venue.t=T+1
        store=Mock(domain='software');store.load.return_value=latest
        normal=dispatch.Controller(store,venue,ROUTES2,after_exit_policy=dispatch.AFTER_EXIT)
        with self.assertRaisesRegex(DispatchError,'AUTHORIZED_PLAN_CHANGED_REPREPARE_REQUIRED'):
            normal._begin_cycle(state['bucket'],state,None,authorized,sample=sample,meta=META)
        store.prepare_and_begin.assert_not_called();self.assertEqual(venue.sent,0)

    def test_authorization_reload_retains_emergency_priority(self):
        from unittest.mock import Mock
        state=state_from_case(q='40',take='40');venue=NormalVenue()
        sample=venue.sample(state['account'],state['symbol'])
        authorized=dispatch.choose(state,ROUTES2,META,sample,now_ms=T,
                                   after_exit_policy=dispatch.AFTER_EXIT)
        latest=deepcopy(state);latest['revision']+=1;latest['emergency']=dict(phase='ACTIVE')
        store=Mock(domain='software');store.load.return_value=latest
        normal=dispatch.Controller(store,venue,ROUTES2,after_exit_policy=dispatch.AFTER_EXIT)
        result=normal._begin_cycle(state['bucket'],state,None,authorized,sample=sample,meta=META)
        self.assertEqual(result,dict(status='EMERGENCY_BUCKET_MANAGED_BY_SEPARATE_LANE',order_requests_sent=0))
        store.prepare_and_begin.assert_not_called();self.assertEqual(venue.sent,0)

    def test_fresh_checkpoint_does_not_extend_old_authorization_evidence_age(self):
        from unittest.mock import Mock
        state=state_from_case(q='40',take='40');venue=NormalVenue()
        sample=venue.sample(state['account'],state['symbol'])
        authorized=dispatch.choose(state,ROUTES2,META,sample,now_ms=T,
                                   after_exit_policy=dispatch.AFTER_EXIT)
        latest=deepcopy(state);latest['revision']+=1
        venue.t=T+15001;latest['evidence']['snapshot']['at_ms']=venue.t
        venue.local_authorize=lambda p,policy:dispatch.TestnetVenue._fresh_entry_evidence(venue,p)
        store=Mock(domain='software');store.load.return_value=latest
        normal=dispatch.Controller(store,venue,ROUTES2,after_exit_policy=dispatch.AFTER_EXIT)
        with self.assertRaisesRegex(DispatchError,'ENTRY_EVIDENCE_EXPIRED_BEFORE_RESERVATION'):
            normal._begin_cycle(state['bucket'],state,None,authorized,sample=sample,meta=META)
        store.prepare_and_begin.assert_not_called();self.assertEqual(venue.sent,0)

    def test_plan_reload_retains_new_unknown_attempt_and_emergency_barriers(self):
        from unittest.mock import Mock
        for barrier in ('unknown','emergency'):
            with self.subTest(barrier=barrier):
                state=state_from_case(q='40')
                latest=deepcopy(state);latest['revision']+=1
                if barrier=='unknown':latest['pending']='a'*64
                else:latest['emergency']=dict(phase='ACTIVE')
                store=Mock(domain='software');store.load.return_value=latest
                store.request.return_value=dict(phase='OUTCOME_UNKNOWN')
                venue=NormalVenue()
                normal=dispatch.Controller(store,venue,ROUTES2,after_exit_policy=dispatch.AFTER_EXIT)
                with patch.object(normal,'refresh',return_value=state), \
                        patch.object(dispatch,'choose',side_effect=AssertionError('NO_STALE_PLAN')):
                    result=normal.cycle(state['bucket'],send=True)
                self.assertEqual(result['status'],'OUTCOME_UNKNOWN' if barrier=='unknown'
                                 else 'EMERGENCY_BUCKET_MANAGED_BY_SEPARATE_LANE')
                store.reserve.assert_not_called();store.begin.assert_not_called()
                store.change.assert_not_called();self.assertEqual(venue.sent,0)

    def test_lost_refresh_cannot_fall_back_to_incomplete_or_expired_checkpoint(self):
        from unittest.mock import Mock
        for bad in ('stale','future','incomplete','bindings'):
            with self.subTest(checkpoint=bad):
                state=state_from_case(q='100',stop='100',take='100')
                if bad=='future':state['evidence']['snapshot']['at_ms']=T+1
                elif bad=='incomplete':state['evidence']['snapshot']['history_complete']=False
                else:state['evidence']['bindings']=[]
                store=Mock(domain='software');store.load.return_value=state
                venue=NormalVenue()
                if bad=='stale':venue.t=T+15001
                normal=dispatch.Controller(store,venue,ROUTES2,after_exit_policy=dispatch.AFTER_EXIT)
                with patch.object(normal,'refresh',side_effect=DispatchError('CONCURRENT_DISPATCH_RELOAD_REQUIRED')), \
                        self.assertRaisesRegex(DispatchError,'OBSERVATION_INCOMPLETE_OR_STALE'):
                    normal.cycle(state['bucket'],send=True)
                store.change.assert_not_called();store.reserve.assert_not_called();store.begin.assert_not_called()
                self.assertEqual(venue.sent,0)

    def test_old_protected_trade_never_closes_just_because_fill_is_old(self):
        state=state_from_case(q='100',stop='100',take='100')
        self.assertIsNone(m.trigger(state,now_ms=T+9999999,mark='9'))

    def test_deadline_is_fill_time_not_ack_or_observation_time(self):
        state=state_from_case(q='40')
        at=state['evidence']['snapshot']['fills'][0]['at_ms']
        self.assertIsNone(m.trigger(state,now_ms=at+4999))
        result=m.trigger(state,now_ms=at+5000)
        self.assertEqual(result['uncovered_since_ms'],at)
        self.assertEqual(result['uncovered_quantity'],'40')

    def test_crossed_stop_short_and_long_trigger_without_waiting(self):
        for side,mark in [('LONG','9.9'),('SHORT','10.1')]:
            state=state_from_case(q='40',side=side)
            at=state['evidence']['snapshot']['fills'][0]['at_ms']
            cause=m.trigger(state,now_ms=at,mark=mark)
            self.assertEqual(cause['reason'],'STOP_LEVEL_PASSED_UNPROTECTED')

    def test_zero_fill_never_triggers(self):
        self.assertIsNone(m.trigger(state_from_case(q='0'),now_ms=T+99999,mark='1'))

    def test_later_partial_fill_does_not_reset_old_uncovered_deadline(self):
        state=state_from_case(q='40')
        snap=state['evidence']['snapshot'];b=state['bindings'][0]
        old=snap['fills'][0]['at_ms']
        newer=deepcopy(snap['fills'][0]);newer.update(fill_id='second',quantity='20',at_ms=old+4000)
        snap['fills'].append(newer)
        snap['position_quantity']='60'
        snap['open_orders'][0]['quantity']='40'
        self.assertEqual(m.trigger(state,now_ms=old+5000)['uncovered_since_ms'],old)

    def test_old_fully_covered_tranche_does_not_age_new_uncovered_quantity(self):
        state=state_from_case(q='40',stop='40')
        snap=state['evidence']['snapshot'];old=snap['fills'][0]['at_ms']
        newer=deepcopy(snap['fills'][0]);newer.update(fill_id='second',quantity='20',at_ms=old+4000)
        snap['fills'].append(newer);snap['position_quantity']='60';snap['open_orders'][0]['quantity']='40'
        self.assertIsNone(m.trigger(state,now_ms=old+5000))
        self.assertEqual(m.trigger(state,now_ms=old+9000)['uncovered_since_ms'],old+4000)

    def test_price_rounding_stays_inside_one_percent_band(self):
        for decimals in range(7):
            for mark in ('0.00012345','0.09530265','10','12345.678','123456'):
                px=Decimal(mark)
                for buy in (True,False):
                    try:
                        price=m.close_price(mark,decimals,buy=buy)
                    except DispatchError:
                        continue
                    dispatch.precise(price,'1',decimals)
                    self.assertTrue(Decimal(price)<=px*Decimal('1.01') if buy
                                    else Decimal(price)>=px*Decimal('.99'))

    def test_default_venue_rejects_before_wallet_or_transport(self):
        with self.assertRaisesRegex(DispatchError,'NOT_AUTHORIZED'):
            m.Venue({}).authorize({}, {})

    def test_identity_requires_terminal_exact_ioc_reduce_only(self):
        action=dict(type='order',grouping='na',orders=[dict(a=0,b=False,p='9.9',s='40',r=True,
            t=dict(limit=dict(tif='Ioc')),c='0x'+'a'*32)])
        request=dict(proposal=dict(operation='EMERGENCY_CLOSE',leg='STOP',symbol='DOGE',action=action),
                     attempt_at_ms=T-100,reply=None)
        raw=dict(status='order',order=dict(status='filled',statusTimestamp=T,
            order=dict(oid=12,cloid=action['orders'][0]['c'],coin='DOGE',side='A',reduceOnly=True,
                origSz='40',limitPx='9.9',orderType='Limit',isTrigger=False,isPositionTpsl=False)))
        self.assertEqual(dispatch.identity(raw,request,T),'12')
        for key,value in [('reduceOnly',False),('origSz','41'),('limitPx','9'),('isTrigger',True),('cloid','0x'+'b'*32)]:
            broken=deepcopy(raw);broken['order']['order'][key]=value
            with self.assertRaises(DispatchError):dispatch.identity(broken,request,T)
        raw['order']['status']='open'
        with self.assertRaises(DispatchError):dispatch.identity(raw,request,T)

    def test_no_healthy_entry_approval_before_first_pass_or_after_error(self):
        with patch.dict(m._health,dict(running=True,last_pass_at_ms=T,last_status='PASS_COMPLETE')):
            self.assertTrue(m.healthy(T))
            self.assertFalse(m.healthy(T+15001))
            m._health['last_status']='RECONCILIATION_REQUIRED_NO_BLIND_RETRY'
            self.assertFalse(m.healthy(T))

    def test_timing_separates_public_activation_from_saved_verification(self):
        state=state_from_case(q='40',stop='40')
        cid=state['bindings'][0]['card_id'];first=state['evidence']['snapshot']['fills'][0]['at_ms']
        state['entry_timing_armed']={cid:first-1}
        request=dict(proposal=dict(card_id=cid,leg='STOP',operation='CREATE_EXIT'))
        lookup=dict(status='order',order=dict(status='open',statusTimestamp=T-2000,
            order=dict(oid=int(state['bindings'][0]['orders']['STOP'][0]))))
        m.record_timing(state,request,lookup,T)
        item=state['protection_timing'][cid]
        self.assertEqual(item['fill_to_stop_public_ms'],8000)
        self.assertEqual(item['fill_to_stop_observation_ms'],10000)
        self.assertEqual(item['timing_semantics'],'public_observation_before_commit')
        self.assertNotIn('fill_to_saved_verification_ms',item)
        self.assertNotIn('stop_verified_at_ms',item)
        m.record_timing(state,request,lookup,T+1)
        self.assertEqual(state['protection_timing'][cid],item)

    def test_old_evidence_and_ack_never_become_new_live_measurement(self):
        state=state_from_case(q='40',stop='40')
        cid=state['bindings'][0]['card_id']
        m.record_timing(state,None,None,T)
        self.assertNotIn(cid,state.get('protection_timing',{}))
        state['entry_timing_armed']={cid:T-20001}
        m.record_timing(state,dict(proposal=dict(card_id=cid,leg='STOP',operation='CREATE_EXIT')),
                        dict(status='ok',response='ack'),T)
        self.assertIsNone(state['protection_timing'][cid]['fill_to_stop_public_ms'])

    def test_timing_trial_rejects_synthetic_expired_or_enabled_continuous_entries(self):
        from . import protection_timing_trial as trial
        from .test_long_stream_runtime import env
        environment={**env(),'HL_TESTNET_EMERGENCY_CLOSE':m.APPROVAL,'HL_TESTNET_SHORT_ENTRY_ENABLED':'false'}
        _,record=original(expiry_seconds=300)
        card=record['card'];before=deepcopy(environment)
        self.assertEqual(trial.validate(environment,card,T),T+90000)
        self.assertEqual(environment,before)
        for mutation in [{'HL_TESTNET_LONG_ENTRY_ENABLED':'true'},
                         {'HL_TESTNET_SHORT_ENTRY_ENABLED':'true'},
                         {'HL_TESTNET_EMERGENCY_CLOSE':''},
                         {'RENDER_SERVICE_ID':'production'}]:
            with self.assertRaises(DispatchError):trial.validate({**environment,**mutation},card,T)
        with self.assertRaises(DispatchError):trial.validate(environment,original()[1]['card'],T)
        with self.assertRaises(DispatchError):trial.validate(environment,card,T+600000)

    def test_emergency_enabled_entry_requires_exact_expiring_trial_card(self):
        from .test_long_stream_runtime import env
        card=original(expiry_seconds=300)[1]['card'];cid=card['card_id']
        environment={**env(),'HL_TESTNET_EMERGENCY_CLOSE':m.APPROVAL,
            'HL_TESTNET_LONG_ENTRY_ENABLED':'true','HL_TESTNET_LONG_NOT_BEFORE':datetime.fromtimestamp((T-60000)/1000,timezone.utc).isoformat(),
            'HL_TESTNET_PROTECTION_TIMING_CARD_ID':cid,'HL_TESTNET_PROTECTION_TIMING_EXPIRES_MS':str(T+90000)}
        venue=dispatch.TestnetVenue(environment);venue.now=lambda:T
        venue.fill_wakeups=ReconciledFillFeed()
        proposal=dict(operation='ENTRY',card_id=cid,role='long_account',account=A,symbol='DOGE',
            source_at=card['prepared']['execution']['at'],source_expires_at=card['source_expires_at'])
        with patch.object(m,'healthy',return_value=True):
            self.assertEqual(venue._gate(proposal,dispatch.AFTER_EXIT)['account'],A)
            for mutation in [dict(card_id='f'*64),{}]:
                if mutation:
                    with self.assertRaisesRegex(DispatchError,'EXACT_TIMING'):venue._gate({**proposal,**mutation},dispatch.AFTER_EXIT)
            for deadline in [T-1,T,T+120001]:
                venue.env['HL_TESTNET_PROTECTION_TIMING_EXPIRES_MS']=str(deadline)
                with self.assertRaisesRegex(DispatchError,'EXACT_TIMING'):venue._gate(proposal,dispatch.AFTER_EXIT)

    def test_lane_wait_expiry_blocks_before_reservation_or_attempt(self):
        from .test_long_stream_runtime import env
        from unittest.mock import Mock
        for expires_source,expires_grant,advance,code in (
                (T+280000,T+1000,1001,'EXACT_TIMING_TRIAL_APPROVAL_REQUIRED'),
                (T+1000,T+90000,1001,'NEW_TRIAL_SOURCE_NOT_FRESH'),
                (T+280000,T+90000,15001,'ENTRY_EVIDENCE_EXPIRED_BEFORE_RESERVATION')):
            with self.subTest(code=code):
                card=original(expiry_seconds=300)[1]['card'];cid=card['card_id']
                now={'at':T};state=dict(revision=1,bucket='c'*64)
                environment={**env(),'HL_TESTNET_EMERGENCY_CLOSE':m.APPROVAL,
                    'HL_TESTNET_LONG_ENTRY_ENABLED':'true',
                    'HL_TESTNET_LONG_NOT_BEFORE':datetime.fromtimestamp((T-60000)/1000,timezone.utc).isoformat(),
                    'HL_TESTNET_PROTECTION_TIMING_CARD_ID':cid,
                    'HL_TESTNET_PROTECTION_TIMING_EXPIRES_MS':str(expires_grant)}
                proposal=dict(operation='ENTRY',card_id=cid,role='long_account',account=A,symbol='DOGE',
                    source_at=card['prepared']['execution']['at'],observed_at_ms=T,
                    source_expires_at=datetime.fromtimestamp(expires_source/1000,timezone.utc).isoformat())
                store=Mock();store.domain='testnet';store.load.return_value=state
                venue=dispatch.TestnetVenue(environment);venue.now=lambda:now['at']
                venue.fill_wakeups=ReconciledFillFeed()
                controller=dispatch.Controller(store,venue,ROUTES2,after_exit_policy=dispatch.AFTER_EXIT)
                entered=threading.Event();release=threading.Event()
                @dispatch.market_lane
                def hold_lane(owner,bucket):
                    entered.set()
                    if not release.wait(2):raise AssertionError('TEST_LANE_WAS_NOT_RELEASED')
                with patch.object(m,'healthy',return_value=True):
                    venue.local_authorize(proposal,dispatch.AFTER_EXIT)
                    with ThreadPoolExecutor(max_workers=2) as pool:
                        holder=pool.submit(hold_lane,controller,state['bucket'])
                        try:
                            self.assertTrue(entered.wait(1))
                            attempt=pool.submit(controller._begin_cycle,state['bucket'],state,None,proposal)
                            now['at']+=advance
                        finally:
                            release.set()
                        holder.result(timeout=1)
                        with self.assertRaisesRegex(DispatchError,code):attempt.result(timeout=1)
                store.reserve.assert_not_called()
                store.begin.assert_not_called()

    def test_disabled_entry_tick_skips_idle_flat_unbound_candidate(self):
        from . import long_stream_runtime as stream
        from unittest.mock import Mock
        controller=Mock();controller.venue.now.return_value=T
        state=dict(bucket='a'*64,bindings=[],pending=None,
                   evidence=dict(snapshot=dict(at_ms=T,position_quantity='0',open_orders=[])))
        controller.store.for_account.return_value=[state]
        with patch.object(stream,'_unfinished',return_value=True):
            result=stream.tick(controller,{'account':A},datetime.fromtimestamp(T/1000,timezone.utc),new_entries=False)
        self.assertEqual(result['status'],'ENTRIES_DISABLED')
        controller.cycle.assert_not_called()

    def test_idle_flat_never_skips_unresolved_owned_order(self):
        from .long_stream_runtime import idle_flat
        state=state_from_case(q='0')
        state['evidence']['snapshot']['open_orders']=[]
        self.assertFalse(idle_flat(state))

    def test_normal_tick_does_not_compete_with_latched_emergency_lane(self):
        from . import long_stream_runtime as stream
        from unittest.mock import Mock
        controller=Mock();controller.venue.now.return_value=T
        controller.store.for_account.return_value=[dict(emergency=dict(phase='ACTIVE'))]
        result=stream.tick(controller,{'account':A},datetime.fromtimestamp(T/1000,timezone.utc),new_entries=False)
        self.assertEqual(result['maintenance_active'],1)
        controller.cycle.assert_not_called()

    def test_emergency_whitelist_cannot_assign_unowned_or_active_order(self):
        state=state_from_case(q='40',stop='40')
        with self.assertRaisesRegex(life.LifecycleError,'NOT_OWNED'):
            life.review(state['bindings'],state['evidence']['snapshot'],now_ms=T,emergency_stop_oids=['999'])
        with self.assertRaisesRegex(life.LifecycleError,'NOT_TERMINAL'):
            life.review(state['bindings'],state['evidence']['snapshot'],now_ms=T,
                        emergency_stop_oids=state['bindings'][0]['orders']['STOP'])


@unittest.skipUnless(CI,'Disposable loopback PostgreSQL required')
class EmergencyDatabaseTests(NoExternal):
    def setUp(self):
        super().setUp()
        self.j=fixtures.PostgresJournal.for_ci(CI)
        with self.j._transaction() as conn:
            for name in (SCHEMA,'hl_testnet_recovery_rehearsal_v1','hl_testnet_cards_v1','hl_testnet_execution_v1'):
                conn.execute(f'DROP SCHEMA IF EXISTS {name} CASCADE')
        self.j.bootstrap();self.cards=fixtures.CardStore(self.j);self.cards.initialize()
        self.store=DispatchStore(self.j);self.store.initialize();self.v=NormalVenue()
        self.c=dispatch.Controller(self.store,self.v,ROUTES2)
        self.b,self.o=original();self.cards.record(self.o['card'])
        self.s=self.c.register(self.b['card_id']);self.bucket=self.s['bucket']
    cycle=fixtures.DispatchDatabaseTests.cycle
    entry=fixtures.DispatchDatabaseTests.entry
    protect=fixtures.DispatchDatabaseTests.protect

    def setup_emergency(self,q='40'):
        self.entry(q)
        self.v.t+=1
        self.c.refresh(self.bucket)
        self.v=VenueFromExisting(self.v)
        self.c.venue=self.v;self.v.store=self.store
        self.em=m.Controller(self.c,self.v)
        self.v.t+=5001

    def emergency(self,send=True):
        self.v.t+=1
        return self.em.cycle(self.bucket,send=send)

    def incident(self):return self.store.load(self.bucket)['emergency']

    def closed_release_setup(self):
        self.setup_emergency('100');self.emergency();self.emergency()
        state=self.store.load(self.bucket)
        self.assertEqual(state['emergency']['phase'],'CLOSED_VERIFIED')
        self.v.env=dict(HL_TESTNET_EMERGENCY_CLOSE=m.APPROVAL,
            RENDER_SERVICE_ID=dispatch.roles.SERVICE,HL_TESTNET_RUNTIME_MODE='long_stream_testnet_v1',
            HL_TESTNET_TWO_ACCOUNT_EXECUTION='disabled',HL_TESTNET_FILLED_DISPATCH='approved_long_stream_v1',
            HL_TESTNET_LONG_STREAM='approved_alerts_v1',HL_TESTNET_FILLED_AFTER_EXIT_POLICY=dispatch.AFTER_EXIT,
            HL_TESTNET_LONG_ENTRY_ENABLED='false',HL_TESTNET_SHORT_ENTRY_ENABLED='false')
        # Public reads do not advance this deterministic venue's clock. A
        # release must obtain a genuinely newer checkpoint, just as real
        # network reads do; equal timestamps correctly fail _continues.
        self.v.t+=1
        return state

    @staticmethod
    def flat_release_inventory(kind,account):
        return [] if kind=='frontendOpenOrders' else dict(assetPositions=[])

    def test_explicit_closed_release_archives_incident_and_unblocks_only_its_fence(self):
        state=self.closed_release_setup();identity=m.incident_identity(state);sent=self.v.sent
        from .card_sync_evidence import PublicReader
        with patch.object(PublicReader,'read',side_effect=self.flat_release_inventory):
            result=self.em.release_closed_incident(self.bucket,account=state['account'],incident_id=identity)
        current=self.store.load(self.bucket)
        self.assertEqual(result['status'],'CLOSED_INCIDENT_RELEASED')
        self.assertNotIn('emergency',current)
        self.assertEqual(current['emergency_history'][0]['incident'],state['emergency'])
        self.assertEqual(current['emergency_history'][0]['incident_id'],identity)
        self.assertEqual(self.v.sent,sent)
        self.assertEqual(self.v.env['HL_TESTNET_LONG_ENTRY_ENABLED'],'false')
        self.assertEqual(self.v.env['HL_TESTNET_SHORT_ENTRY_ENABLED'],'false')
        with self.j._transaction() as conn:
            m.fence(conn,current,'ENTRY')
            event=conn.execute(f'SELECT event FROM {SCHEMA}.events WHERE bucket=%s ORDER BY revision DESC LIMIT 1',
                               (self.bucket,)).fetchone()
        self.assertEqual(event[0],'EMERGENCY_FINAL_INCIDENT_EXPLICITLY_ARCHIVED_ENTRIES_STAY_DISABLED')
        self.assertEqual(life.review(current['bindings'],current['evidence']['snapshot'],
                                    now_ms=self.v.now())['cards'][0]['state'],'CLOSED')

    def test_closed_release_refuses_wrong_scope_active_unknown_and_entry_enabled(self):
        state=self.closed_release_setup();identity=m.incident_identity(state)
        with self.assertRaisesRegex(DispatchError,'INCIDENT_OR_ACCOUNT'):
            self.em.release_closed_incident(self.bucket,account=B,incident_id=identity)
        with self.assertRaisesRegex(DispatchError,'INCIDENT_OR_ACCOUNT'):
            self.em.release_closed_incident(self.bucket,account=A,incident_id='a'*64)
        self.v.env['HL_TESTNET_LONG_ENTRY_ENABLED']='true'
        with self.assertRaisesRegex(DispatchError,'ENTRIES_DISABLED'):
            self.em.release_closed_incident(self.bucket,account=A,incident_id=identity)
        self.v.env['HL_TESTNET_LONG_ENTRY_ENABLED']='false'
        def corrupt(conn,current):
            current['emergency']['requests'][0]['phase']='OUTCOME_UNKNOWN'
            return None
        current=self.store.change(self.bucket,self.store.load(self.bucket)['revision'],'TEST_UNKNOWN_REQUEST',
                                  self.v.now(),corrupt)
        with self.assertRaisesRegex(DispatchError,'FRESH_FINALITY'):
            self.em.release_closed_incident(self.bucket,account=A,incident_id=m.incident_identity(current))
        self.assertIn('emergency',self.store.load(self.bucket))

    def test_closed_release_rechecks_inventory_age_and_revision_at_commit(self):
        state=self.closed_release_setup();identity=m.incident_identity(state)
        from .card_sync_evidence import PublicReader
        calls=[]
        def stale(kind,account):
            calls.append(kind)
            if len(calls)==4:self.v.t+=5001
            return self.flat_release_inventory(kind,account)
        with patch.object(PublicReader,'read',side_effect=stale):
            with self.assertRaisesRegex(DispatchError,'FRESH_FINALITY'):
                self.em.release_closed_incident(self.bucket,account=A,incident_id=identity)
        self.assertIn('emergency',self.store.load(self.bucket))
        calls.clear()
        def race(kind,account):
            calls.append(kind)
            if len(calls)==4:
                current=self.store.load(self.bucket)
                self.store.change(self.bucket,current['revision'],'TEST_REVISION_RACE',self.v.now(),
                                  lambda conn,value:None)
            return self.flat_release_inventory(kind,account)
        with patch.object(PublicReader,'read',side_effect=race):
            with self.assertRaisesRegex(DispatchError,'CONCURRENT_DISPATCH'):
                self.em.release_closed_incident(self.bucket,account=A,incident_id=identity)
        self.assertIn('emergency',self.store.load(self.bucket))

    @contextmanager
    def actual_emergency_authorization(self):
        self.v.env=dict(HL_TESTNET_EMERGENCY_CLOSE=m.APPROVAL,
            RENDER_SERVICE_ID=dispatch.roles.SERVICE,
            HL_TESTNET_RUNTIME_MODE='long_stream_testnet_v1',
            HL_TESTNET_TWO_ACCOUNT_EXECUTION='disabled',
            HL_TESTNET_FILLED_DISPATCH='approved_long_stream_v1',
            HL_TESTNET_LONG_STREAM='approved_alerts_v1',
            HL_TESTNET_FILLED_AFTER_EXIT_POLICY=dispatch.AFTER_EXIT)
        with patch.object(dispatch.roles,'route_for',return_value=ROUTES2[self.b['role']]), \
                patch.object(dispatch.roles,'wallet_for_role',return_value=object()), \
                patch.object(self.v,'authorize',side_effect=lambda s,p:m.Venue.authorize(self.v,s,p)):
            yield

    def test_preview_never_latches_reserves_or_sends(self):
        self.setup_emergency()
        result=self.emergency(False)
        self.assertEqual(result['status'],'EMERGENCY_PREVIEW')
        self.assertNotIn('emergency',self.store.load(self.bucket))
        self.assertEqual(self.v.sent,1)

    def test_cancel_partial_entry_then_close_only_filled_quantity(self):
        self.setup_emergency()
        first=self.emergency()
        self.assertEqual(first['operation'],'EMERGENCY_CANCEL')
        second=self.emergency()
        self.assertEqual(second['operation'],'EMERGENCY_CLOSE')
        self.assertEqual(self.v.requests[-1]['proposal']['quantity'],'40')
        self.emergency()
        state=self.store.load(self.bucket)
        self.assertEqual(state['emergency']['phase'],'CLOSED_VERIFIED')
        self.assertEqual(state['evidence']['snapshot']['position_quantity'],'0')
        self.assertTrue(m.view(state,self.v.now())['cards'][0]['closure_verified'])
        self.assertEqual(self.v.sent,3)

    def test_slow_metadata_is_read_before_final_quantity_checkpoint(self):
        self.setup_emergency('100')
        order=[];collect=self.v.collect
        def metadata():
            order.append('metadata');self.v.t+=6001
            return META
        def sample(account,symbol):
            order.append('sample')
            return dict(mark_price=self.v.mark,at_ms=self.v.now())
        def observed(value):
            order.append('quantity')
            return collect(value)
        with patch.object(self.v,'metadata',side_effect=metadata), \
                patch.object(self.v,'sample',side_effect=sample), \
                patch.object(self.v,'collect',side_effect=observed), \
                self.actual_emergency_authorization():
            result=self.emergency()
        self.assertEqual(result['operation'],'EMERGENCY_CLOSE')
        self.assertEqual(order,['metadata','quantity','sample'])
        self.assertEqual(self.v.requests[-1]['proposal']['quantity'],'100')
        self.assertEqual(self.v.sent,2)
        self.assertEqual(self.incident()['requests'][0]['attempts'],1)

    def test_new_crossed_stop_keeps_price_clock_then_known_incident_prefetches(self):
        self.entry('100');self.v.t+=1;self.c.refresh(self.bucket)
        self.v=VenueFromExisting(self.v);self.c.venue=self.v;self.v.store=self.store
        self.em=m.Controller(self.c,self.v);self.v.mark=self.b['prices']['stop']
        self.assertIsNone(m.trigger(self.store.load(self.bucket),now_ms=self.v.now()))
        order=[];collect=self.v.collect
        def metadata():
            order.append('metadata');self.v.t+=6001
            return META
        def sample(account,symbol):
            order.append('sample')
            return dict(mark_price=self.v.mark,at_ms=self.v.now())
        def observed(value):
            order.append('quantity')
            return collect(value)
        with patch.object(self.v,'metadata',side_effect=metadata), \
                patch.object(self.v,'sample',side_effect=sample), \
                patch.object(self.v,'collect',side_effect=observed), \
                self.actual_emergency_authorization():
            with self.assertRaisesRegex(DispatchError,'EMERGENCY_PRICE_SAMPLE_EXPIRED'):
                self.emergency()
            self.assertEqual(order,['quantity','sample','metadata'])
            self.assertEqual(self.v.sent,1)
            self.assertEqual(self.incident()['reason'],'STOP_LEVEL_PASSED_UNPROTECTED')
            self.assertEqual(self.incident()['requests'],[])
            order.clear()
            result=self.emergency()
        self.assertEqual(result['operation'],'EMERGENCY_CLOSE')
        self.assertEqual(order,['metadata','quantity','sample'])
        self.assertEqual(self.v.sent,2)
        self.assertEqual(self.incident()['requests'][0]['attempts'],1)

    def test_slow_price_read_cannot_renew_its_original_sample_timestamp(self):
        self.setup_emergency('100')
        samples=[]
        def sample(account,symbol):
            samples.append(self.v.now())
            self.v.t+=6001
            return dict(mark_price=self.v.mark,at_ms=samples[-1])
        with patch.object(self.v,'sample',side_effect=sample):
            with self.assertRaisesRegex(DispatchError,'EMERGENCY_PRICE_SAMPLE_EXPIRED'):
                self.emergency()
        self.assertEqual(len(samples),1)
        self.assertEqual(self.v.sent,1)
        self.assertEqual(self.incident()['requests'],[])
        self.assertEqual(self.incident()['phase'],'ACTIVE')

    def test_fresh_price_cannot_authorize_expired_final_quantity_evidence(self):
        self.setup_emergency('100')
        collect=self.v.collect
        def stale_quantity(value):
            observed=collect(value)
            self.v.t+=6001
            return observed
        with patch.object(self.v,'collect',side_effect=stale_quantity), \
                self.actual_emergency_authorization():
            with self.assertRaisesRegex(DispatchError,'EMERGENCY_FINAL_QUANTITY_EVIDENCE_EXPIRED'):
                self.emergency()
        self.assertEqual(self.v.sent,1)
        self.assertEqual(self.incident()['requests'],[])

    def test_metadata_mapping_change_is_rejected_before_emergency_attempt(self):
        self.setup_emergency('100')
        metadata=deepcopy(META);metadata['universe'][0]['szDecimals']-=1
        with patch.object(self.v,'metadata',return_value=metadata):
            with self.assertRaisesRegex(DispatchError,'EMERGENCY_WIRE_NOT_VERIFIED'):
                self.emergency()
        self.assertEqual(self.v.sent,1)
        self.assertEqual(self.incident()['requests'],[])

    def test_independent_preread_failure_still_latches_known_unprotected_fill(self):
        self.setup_emergency('100')
        with patch.object(self.v,'metadata',side_effect=DispatchError('PUBLIC_READ_UNAVAILABLE')):
            with self.assertRaisesRegex(DispatchError,'PUBLIC_READ_UNAVAILABLE'):
                self.emergency()
        self.assertEqual(self.v.sent,1)
        self.assertEqual(self.incident()['phase'],'ACTIVE')
        self.assertEqual(self.incident()['requests'],[])

    def test_deadline_latch_is_committed_before_public_metadata_read(self):
        self.setup_emergency('100')
        def unavailable():
            # Inspect through the real PostgreSQL store while public I/O has
            # not returned. The entry fence must already be durable.
            incident=self.incident()
            self.assertEqual(incident['phase'],'ACTIVE')
            self.assertEqual(incident['reason'],'STOP_VERIFICATION_DEADLINE')
            self.assertEqual(incident['requests'],[])
            self.assertIs(incident['provisional'],True)
            with self.j._transaction() as conn:
                with self.assertRaisesRegex(DispatchError,'EMERGENCY_CIRCUIT_LATCHED_NO_NEW_ENTRY'):
                    m.fence(conn,{},'ENTRY')
            raise DispatchError('PUBLIC_READ_UNAVAILABLE')
        with patch.object(self.v,'metadata',side_effect=unavailable):
            with self.assertRaisesRegex(DispatchError,'PUBLIC_READ_UNAVAILABLE'):
                self.emergency()
        self.assertEqual(self.v.sent,1)
        self.assertEqual(self.incident()['requests'],[])

    def test_emergency_begin_returns_commit_local_sender_without_reload(self):
        self.setup_emergency('100');self.v.t+=1
        state=self.em._refresh_normal(self.store.load(self.bucket))
        state=self.em.latch(state,m.trigger(state,now_ms=self.v.now()))
        proposal=self.em.proposal(state)
        with patch.object(self.store,'load',side_effect=AssertionError('NO_POSTCOMMIT_RELOAD')):
            committed,request=self.em._begin(state,proposal)
        self.assertEqual(committed['revision'],state['revision']+1)
        self.assertEqual(request,committed['emergency']['requests'][-1])
        self.assertEqual((request['phase'],request['attempts']),('OUTCOME_UNKNOWN',1))
        self.assertEqual(request['proposal'],proposal)
        self.assertEqual(self.v.sent,1)

    def test_lost_close_reply_resolves_original_identity_without_replay(self):
        self.setup_emergency('100');self.v.lose_reply=True
        self.assertEqual(self.emergency()['status'],'OUTCOME_UNKNOWN')
        rid=self.incident()['pending_close'];count=self.v.sent
        self.emergency()
        self.assertEqual(self.v.sent,count)
        self.assertIsNone(self.incident()['pending_close'])
        r=next(r for r in self.incident()['requests'] if r['request_id']==rid)
        self.assertEqual(r['filled_quantity'],'100')
        self.assertEqual(r['phase'],'OBSERVED')

    def test_partial_ioc_next_intent_uses_exact_new_remainder_and_cloid(self):
        self.setup_emergency('100');self.v.ioc_fill='40'
        self.emergency();first=self.v.requests[-1]
        self.v.ioc_fill=None
        self.emergency();second=self.v.requests[-1]
        self.assertEqual(second['proposal']['quantity'],'60')
        self.assertNotEqual(first['proposal']['action']['orders'][0]['c'],second['proposal']['action']['orders'][0]['c'])
        self.emergency()
        self.assertEqual(self.incident()['phase'],'CLOSED_VERIFIED')
        self.assertEqual([r['filled_quantity'] for r in self.incident()['requests']],['40','60'])

    def test_entry_fill_during_cancel_is_reconciled_before_close(self):
        self.setup_emergency('40')
        self.v.cancel_race=lambda oid:self.v.fill(oid,'20')
        self.emergency();self.emergency()
        self.assertEqual(self.v.requests[-1]['proposal']['quantity'],'60')
        self.emergency()
        self.assertEqual(self.incident()['phase'],'CLOSED_VERIFIED')

    def test_uncertain_cancel_does_not_block_verified_close(self):
        self.setup_emergency('40');self.v.lose_reply=True
        self.assertEqual(self.emergency()['status'],'OUTCOME_UNKNOWN')
        self.assertEqual(self.emergency()['operation'],'EMERGENCY_CLOSE')
        self.emergency()
        self.assertEqual(self.incident()['phase'],'CLOSED_VERIFIED')

    def test_rejected_cancel_does_not_block_close(self):
        self.setup_emergency('40');self.v.reject_cancel=True
        self.assertEqual(self.emergency()['status'],'REJECTED')
        self.assertEqual(self.emergency()['operation'],'EMERGENCY_CLOSE')
        self.assertEqual(self.v.requests[-1]['proposal']['quantity'],'40')
        self.emergency()
        self.assertNotEqual(self.incident()['phase'],'CLOSED_VERIFIED')
        self.assertEqual(self.incident()['pending_cancel'],self.incident()['requests'][0]['request_id'])

    def test_no_order_unknown_close_is_not_blindly_reissued(self):
        self.setup_emergency('100')
        with patch.object(self.v,'send',side_effect=TimeoutError()):self.emergency()
        self.assertIsNotNone(self.incident()['pending_close'])
        with self.assertRaisesRegex(DispatchError,'NO_RESEND'):self.emergency()
        self.assertEqual(len(self.incident()['requests']),1)

    def test_restart_resolves_persisted_close_without_new_send(self):
        self.setup_emergency('100');self.v.lose_reply=True;self.emergency()
        self.em=m.Controller(dispatch.Controller(DispatchStore(self.j),self.v,ROUTES2),self.v)
        count=self.v.sent;self.emergency()
        self.assertEqual(self.v.sent,count)
        self.assertEqual(self.incident()['phase'],'CLOSED_VERIFIED')

    def test_normal_requests_are_fenced_after_latch_and_circuit_persists_after_close(self):
        self.setup_emergency('100');self.emergency();self.emergency()
        with self.assertRaisesRegex(DispatchError,'SEPARATE_LANE'):
            self.store.action_allowed(dict(account=A,symbol='DOGE',operation='CREATE_EXIT'))
        b,o=original(2,'SHORT');self.cards.record(o['card']);state=self.c.register(b['card_id'])
        proposal=dict(account=B,symbol='DOGE',operation='ENTRY')
        with self.assertRaisesRegex(DispatchError,'CIRCUIT_LATCHED'):
            self.store.action_allowed(proposal)
        self.assertEqual(self.incident()['phase'],'CLOSED_VERIFIED')

    def test_normal_unknown_stop_lane_does_not_prevent_emergency_reduce(self):
        self.entry('100')
        def lost(_):self.v.sent+=1;self.v.t+=10;raise TimeoutError()
        with patch.object(self.v,'send',side_effect=lost):self.cycle()
        pending=self.store.load(self.bucket)['pending']
        self.v=VenueFromExisting(self.v);self.c.venue=self.v;self.v.store=self.store
        self.em=m.Controller(self.c,self.v);self.v.t+=5001
        result=self.emergency()
        self.assertEqual(result['operation'],'EMERGENCY_CLOSE')
        self.emergency()
        self.assertEqual(self.store.load(self.bucket)['evidence']['snapshot']['position_quantity'],'0')
        self.assertEqual(self.store.load(self.bucket)['pending'],pending)

    def test_stop_http_call_still_in_progress_does_not_hold_emergency_lane(self):
        self.entry('100')
        emergency_venue=VenueFromExisting(self.v)
        emergency_venue.t+=5001
        emergency=m.Controller(self.c,emergency_venue)
        entered=threading.Event();release=threading.Event()
        def delayed_stop(request):
            self.assertEqual(request['proposal']['leg'],'STOP')
            self.v.sent+=1
            entered.set()
            if not release.wait(3):
                raise AssertionError('TEST_STOP_TRANSPORT_WAS_NOT_RELEASED')
            raise TimeoutError()
        with patch.object(self.v,'send',side_effect=delayed_stop):
            with ThreadPoolExecutor(max_workers=2) as pool:
                ordinary=pool.submit(self.cycle)
                try:
                    self.assertTrue(entered.wait(1))
                    pending=self.store.load(self.bucket)['pending']
                    close=pool.submit(emergency.cycle,self.bucket,send=True).result(timeout=2)
                    self.assertEqual(close['operation'],'EMERGENCY_CLOSE')
                    self.assertEqual(emergency_venue.requests[-1]['proposal']['quantity'],'100')
                    self.assertEqual(self.store.load(self.bucket)['pending'],pending)
                finally:
                    release.set()
                self.assertEqual(ordinary.result(timeout=2)['status'],'OUTCOME_UNKNOWN')

    def test_stalled_normal_public_read_does_not_prevent_verified_emergency_close(self):
        self.setup_emergency('100')
        entered=threading.Event();release=threading.Event();calls=[]
        collect=self.v.collect
        def delayed_first_read(value):
            observed=collect(value)
            calls.append(1)
            if len(calls)==1:
                entered.set()
                if not release.wait(3):
                    raise AssertionError('TEST_PUBLIC_READ_WAS_NOT_RELEASED')
            return observed
        with patch.object(self.v,'collect',side_effect=delayed_first_read):
            with ThreadPoolExecutor(max_workers=2) as pool:
                ordinary=pool.submit(self.cycle)
                try:
                    self.assertTrue(entered.wait(1))
                    close=pool.submit(self.emergency).result(timeout=2)
                    self.assertEqual(close['operation'],'EMERGENCY_CLOSE')
                    self.emergency()
                    advanced=self.store.load(self.bucket)
                    self.assertEqual(advanced['emergency']['phase'],'CLOSED_VERIFIED')
                    self.assertEqual(advanced['evidence']['snapshot']['position_quantity'],'0')
                    self.assertEqual(self.v.sent,2)  # one entry, one emergency close
                finally:
                    release.set()
                result=ordinary.result(timeout=2)
                self.assertEqual(result,dict(status='EMERGENCY_BUCKET_MANAGED_BY_SEPARATE_LANE',
                                             order_requests_sent=0))
        self.assertEqual(self.store.load(self.bucket),advanced)
        self.assertEqual(self.v.sent,2)
        self.assertEqual([r['proposal']['operation'] for r in self.v.requests],
                         ['ENTRY','EMERGENCY_CLOSE'])

    def test_unowned_position_blocks_send_but_latches_new_entries(self):
        self.setup_emergency('100')
        original_read=self.v.read
        def foreign(kind,*args,**kwargs):
            value=original_read(kind,*args,**kwargs)
            if kind=='clearinghouseState':value['assetPositions'][0]['position']['szi']='101'
            return value
        with patch.object(self.v,'read',side_effect=foreign):
            with self.assertRaisesRegex(DispatchError,'NOT_VERIFIED'):self.emergency()
        self.assertEqual(self.v.sent,1)
        self.assertEqual(self.incident()['phase'],'ACTIVE')

    def test_tampered_wire_never_begins(self):
        self.setup_emergency('100')
        self.v.t+=1;state=self.em._refresh_normal(self.store.load(self.bucket))
        state=self.em.latch(state,m.trigger(state,now_ms=self.v.now()))
        proposal=self.em.proposal(state)
        for field,value in [('r',False),('b',True),('p','1'),('s','101'),('a',9),('c','0x'+'a'*32)]:
            broken=deepcopy(proposal);broken['action']['orders'][0][field]=value
            with self.assertRaises(DispatchError):self.em._begin(state,broken)
        self.assertEqual(len(self.incident()['requests']),0)

    def test_expired_unknown_normal_stop_finalizes_only_after_public_no_order_proof(self):
        self.entry('100')
        with patch.object(self.v,'send',side_effect=TimeoutError()):self.cycle()
        self.v=VenueFromExisting(self.v);self.c.venue=self.v;self.v.store=self.store
        self.em=m.Controller(self.c,self.v);self.v.t+=5001
        self.emergency();self.emergency()
        self.assertIsNotNone(self.store.load(self.bucket)['pending'])
        self.v.t+=121000;self.emergency()
        self.assertIsNone(self.store.load(self.bucket)['pending'])
        self.assertEqual(self.incident()['phase'],'CLOSED_VERIFIED')

    def test_expired_unknown_close_creates_new_intent_after_verified_no_order(self):
        self.setup_emergency('100')
        with patch.object(self.v,'send',side_effect=TimeoutError()):self.emergency()
        first=self.incident()['requests'][0]
        self.v.t+=121000;self.emergency()
        second=self.incident()['requests'][1]
        self.assertEqual(first['phase'],'OUTCOME_UNKNOWN')
        self.assertEqual(self.incident()['requests'][0]['phase'],'EXPIRED_NO_PUBLIC_ORDER')
        self.assertNotEqual(first['request_id'],second['request_id'])
        self.emergency();self.assertEqual(self.incident()['phase'],'CLOSED_VERIFIED')

    def test_rejected_cancel_reconciles_expiry_before_new_exact_cancel(self):
        self.setup_emergency('40');self.v.reject_cancel=True
        self.emergency();self.emergency();self.emergency()
        self.assertIsNotNone(self.incident()['pending_cancel'])
        self.v.reject_cancel=False;self.v.t+=121000
        self.assertEqual(self.emergency()['operation'],'EMERGENCY_CANCEL')
        self.emergency();self.assertEqual(self.incident()['phase'],'CLOSED_VERIFIED')

    def test_late_stop_already_active_prevents_unnecessary_emergency(self):
        self.entry('100');self.cycle()
        self.v=VenueFromExisting(self.v);self.c.venue=self.v;self.v.store=self.store
        self.em=m.Controller(self.c,self.v);self.v.t+=5001
        self.assertEqual(self.emergency()['status'],'STOP_OBSERVED_OR_NO_EXPOSURE')
        self.assertNotIn('emergency',self.store.load(self.bucket))
        self.assertEqual(self.v.sent,2)

    def test_actual_complete_stop_checkpoint_avoids_both_unused_public_reads(self):
        self.protect('100')
        self.v=VenueFromExisting(self.v);self.c.venue=self.v;self.v.store=self.store
        self.em=m.Controller(self.c,self.v);self.v.t+=5001
        before=self.store.load(self.bucket)['evidence']['snapshot']['at_ms']
        with patch.object(self.v,'metadata',side_effect=AssertionError('NO_UNUSED_METADATA')), \
                patch.object(self.v,'sample',side_effect=AssertionError('NO_UNUSED_MARK')):
            result=self.emergency()
        current=self.store.load(self.bucket)
        self.assertEqual(result['status'],'STOP_OBSERVED_OR_NO_EXPOSURE')
        self.assertGreater(current['evidence']['snapshot']['at_ms'],before)
        self.assertTrue(m.fresh_stop_coverage(current,now_ms=self.v.now()))
        self.assertNotIn('emergency',current)
        self.assertEqual(self.v.sent,3)

    def test_late_stop_withdraws_provisional_freeze_after_failed_read_and_restart(self):
        self.entry('100');self.cycle()
        self.v=VenueFromExisting(self.v);self.c.venue=self.v;self.v.store=self.store
        self.em=m.Controller(self.c,self.v);self.v.t+=5001
        with patch.object(self.v,'metadata',side_effect=DispatchError('PUBLIC_READ_UNAVAILABLE')):
            with self.assertRaisesRegex(DispatchError,'PUBLIC_READ_UNAVAILABLE'):
                self.emergency()
        self.assertIs(self.incident()['provisional'],True)
        self.assertEqual(self.incident()['requests'],[])
        self.em=m.Controller(self.c,self.v)
        self.assertEqual(self.emergency()['status'],'STOP_OBSERVED_OR_NO_EXPOSURE')
        self.assertNotIn('emergency',self.store.load(self.bucket))
        self.assertEqual(self.v.sent,2)
        with self.j._transaction() as conn:
            events=[row[0] for row in conn.execute(
                f'SELECT event FROM {SCHEMA}.events WHERE bucket=%s ORDER BY revision',
                (self.bucket,)).fetchall()]
        self.assertIn('EMERGENCY_LATCHED_NEW_ENTRIES_BLOCKED',events)
        self.assertIn('UNSENT_EMERGENCY_WITHDRAWN_AFTER_FRESH_STOP_PROOF',events)
        # Restore the ordinary software adapter; normal maintenance must now
        # be able to create the still-missing TP without another ENTRY.
        ordinary=NormalVenue();ordinary.__dict__.update(deepcopy(
            {key:value for key,value in self.v.__dict__.items() if key!='store'}))
        self.v=ordinary;self.c.venue=ordinary
        self.assertEqual(self.cycle()['status'],'ACCEPTED_UNVERIFIED')
        self.assertEqual(self.v.requests[-1]['proposal']['leg'],'TAKE_PROFIT')
        self.assertEqual(self.v.sent,3)

    def test_fresh_closed_partial_fill_cancels_remaining_entry_without_market_close(self):
        self.entry('40');self.cycle();self.v.fill('1001','40')
        self.v=VenueFromExisting(self.v);self.c.venue=self.v;self.v.store=self.store
        self.em=m.Controller(self.c,self.v);self.v.t+=5001
        first=self.emergency()
        self.assertEqual(first['operation'],'EMERGENCY_CANCEL')
        self.assertEqual(self.v.requests[-1]['proposal']['leg'],'ENTRY')
        self.assertEqual(self.v.requests[-1]['proposal']['quantity'],'0')
        self.assertIs(self.incident()['provisional'],False)
        self.emergency()
        self.assertEqual(self.incident()['phase'],'CLOSED_VERIFIED')
        self.assertEqual(self.store.load(self.bucket)['evidence']['snapshot']['position_quantity'],'0')
        self.assertEqual(self.v.sent,3)
        self.assertNotIn('EMERGENCY_CLOSE',[request['proposal']['operation'] for request in self.v.requests])

    def test_exact_stop_with_oversized_tp_withdraws_freeze_and_resumes_owned_resize(self):
        self.protect('100');self.v.fill('1001','40')
        # Simulate recovery of an unsent provisional freeze. The actual venue
        # now has STOP 60 covering the remainder and an oversized sibling TP 100.
        self.v=VenueFromExisting(self.v);self.c.venue=self.v;self.v.store=self.store
        self.em=m.Controller(self.c,self.v)
        self.em.latch(self.store.load(self.bucket),dict(card_id=self.b['card_id'],
            reason='STOP_VERIFICATION_DEADLINE',uncovered_since_ms=self.v.now()-5000,
            uncovered_quantity='60'),provisional=True)
        before=self.v.sent
        self.assertEqual(self.emergency()['status'],'STOP_OBSERVED_OR_NO_EXPOSURE')
        self.assertNotIn('emergency',self.store.load(self.bucket))
        self.assertEqual(self.v.sent,before)
        ordinary=NormalVenue();ordinary.__dict__.update(deepcopy(
            {key:value for key,value in self.v.__dict__.items() if key!='store'}))
        self.v=ordinary;self.c.venue=ordinary
        self.assertEqual(self.cycle()['status'],'ACCEPTED_UNVERIFIED')
        proposal=self.v.requests[-1]['proposal']
        self.assertEqual((proposal['operation'],proposal['leg'],proposal['quantity'],proposal['old_oid']),
                         ('MODIFY_EXIT','TAKE_PROFIT','60','1002'))
        self.assertEqual(dispatch.requested_order(proposal['action'])['p'],'10.2')
        self.cycle(False)
        report=life.review(self.store.load(self.bucket)['bindings'],
            self.store.load(self.bucket)['evidence']['snapshot'],now_ms=self.v.now())
        self.assertEqual((report['cards'][0]['stop_quantity_observed'],
                          report['cards'][0]['take_profit_quantity_observed']),('60','60'))
        self.assertEqual(self.v.orders['1001']['order']['sz'],'60')
        self.assertNotIn('EMERGENCY_CLOSE',[request['proposal']['operation'] for request in self.v.requests])

    def test_concurrent_begin_commits_only_one_emergency_intent(self):
        self.setup_emergency('100');self.v.t+=1
        state=self.em._refresh_normal(self.store.load(self.bucket))
        state=self.em.latch(state,m.trigger(state,now_ms=self.v.now()))
        proposal=self.em.proposal(state)
        def begin():
            try:return self.em._begin(state,proposal)[1]['request_id']
            except DispatchError:return None
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda _:begin(),range(2)))
        self.assertEqual(sum(r is not None for r in results),1)
        self.assertEqual(len(self.incident()['requests']),1)
        self.assertEqual(self.v.sent,1)

    def test_lost_commit_ack_never_reaches_emergency_sender(self):
        self.setup_emergency('100');change=self.store.change
        def uncertain(*args,**kwargs):
            result=change(*args,**kwargs)
            if args[2]=='EMERGENCY_ATTEMPT_BEGUN':
                raise fixtures.JournalError('COMMIT_ACKNOWLEDGEMENT_LOST')
            return result
        with patch.object(self.store,'change',side_effect=uncertain):
            with self.assertRaises(fixtures.JournalError):self.emergency()
        self.assertEqual(self.v.sent,1)
        self.assertIsNotNone(self.incident()['pending_close'])
        with self.assertRaisesRegex(DispatchError,'NO_RESEND'):self.emergency()

    def test_stop_fill_race_does_not_double_allocate_closed_quantity(self):
        self.entry('100');self.cycle();self.cycle(False)
        self.v=VenueFromExisting(self.v);self.c.venue=self.v;self.v.store=self.store
        self.em=m.Controller(self.c,self.v);self.v.t+=5001
        # Deliberately latch a verified incident before a late stop fill.
        self.v.t+=1;state=self.em._refresh_normal(self.store.load(self.bucket))
        self.em.latch(state,dict(card_id=self.b['card_id'],reason='FAULT_INJECTION_TEST',
            uncovered_since_ms=self.v.now()-5000,uncovered_quantity='100'))
        send=self.v.send
        def race(request):
            if request['proposal']['operation']=='EMERGENCY_CLOSE':
                self.v.fill('1001','40');self.v.ioc_fill='60'
            return send(request)
        with patch.object(self.v,'send',side_effect=race):self.emergency()
        self.v.ioc_fill=None
        self.emergency()  # cancels the remaining reduce-only stop
        self.emergency()
        state=self.store.load(self.bucket)
        self.assertEqual(state['evidence']['snapshot']['position_quantity'],'0')
        self.assertEqual(self.incident()['phase'],'CLOSED_VERIFIED')
        self.assertEqual(m.view(state,self.v.now())['cards'][0]['exit_quantity'],'100')

    def test_real_store_records_new_fill_to_stop_timing(self):
        self.protect('40')
        state=self.store.load(self.bucket)
        item=state['protection_timing'][self.b['card_id']]
        first=min(f['at_ms'] for f in state['evidence']['snapshot']['fills'])
        self.assertEqual(item['first_fill_at_ms'],first)
        self.assertGreaterEqual(item['fill_to_stop_observation_ms'],item['fill_to_stop_public_ms'])
        self.assertGreaterEqual(item['fill_to_stop_public_ms'],0)

    def test_short_close_is_reduce_only_buy(self):
        b,o=original(2,'SHORT');self.cards.record(o['card']);state=self.c.register(b['card_id'])
        self.bucket=state['bucket'];self.b=b
        self.setup_emergency('100');self.emergency()
        wire=self.v.requests[-1]['proposal']['action']['orders'][0]
        self.assertTrue(wire['r']);self.assertTrue(wire['b']);self.assertEqual(wire['s'],'100')
        self.emergency();self.assertEqual(self.incident()['phase'],'CLOSED_VERIFIED')


def VenueFromExisting(venue):
    result=Venue();result.__dict__.update(deepcopy({k:v for k,v in venue.__dict__.items() if k!='store'}))
    return result
