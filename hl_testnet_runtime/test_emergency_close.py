"""Fault-injected actual controller/store tests; no keys or real exchange calls."""
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
from decimal import Decimal
import os
import unittest
from unittest.mock import patch

from . import emergency_close as m, card_lifecycle as life
from . import filled_quantity_dispatch as dispatch
from .filled_dispatch_store import DispatchStore,DispatchError,SCHEMA
from .test_filled_quantity_dispatch import (NoExternal,Venue as NormalVenue,
    state_from_case,ROUTES2,AGENT)
from . import test_filled_quantity_dispatch as fixtures
from .test_filled_quantity_exits import original,META
from .test_card_lifecycle import T,A,B,fill

CI=os.environ.get('HL_JOURNAL_CI_URL')


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
        self.assertEqual(item['fill_to_saved_verification_ms'],10000)
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
        self.assertGreaterEqual(item['fill_to_saved_verification_ms'],item['fill_to_stop_public_ms'])
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
