"""Manual exit identities, public finality and no-send boundaries."""
from copy import deepcopy
import os
import unittest
from unittest.mock import patch
from . import card_lifecycle as life, card_sync_evidence as sync, manual_exit_reconciliation as m
from .emergency_close import recent_flat_unassigned_checkpoint
from .filled_dispatch_store import DispatchError, SCHEMA
from .card_sync_evidence import SyncError
from .test_card_lifecycle import binding, fill, terminal, snapshot, order, T
from .test_card_sync import Reader


def fixture(side='SHORT'):
    b=binding(side=side)
    entry=fill(b); entry['fill_id']='hl:1000'
    target=fill(b,'TAKE_PROFIT',qty='20',fid='hl:1100')
    manual=deepcopy(target); manual.update(oid='99',fill_id='hl:9900',quantity='80',at_ms=T-500)
    terms=[terminal(b,'ENTRY'),terminal(b,'STOP','0'),terminal(b,'TAKE_PROFIT','20')]
    terms[2]['state']='CANCELED'
    s=snapshot(b,fills=[entry,target,manual],terms=terms)
    state=dict(bucket='a'*64,account=b['account'],symbol=b['symbol'],revision=1,
               bindings=[b],evidence=dict(bindings=[deepcopy(b)],snapshot=s),pending=None,
               emergency=dict(phase='ACTIVE',pending_close=None,pending_cancel=None))
    reader=Reader(state['evidence'])
    reader.data['statuses']['99']=dict(status='order',order=dict(status='filled',
        statusTimestamp=T-500,order=dict(oid=99,coin='DOGE',side=manual['side'],
            origSz='80',sz='0',limitPx=manual['price'],reduceOnly=True,
            orderType='Limit',isTrigger=False,triggerPx='0')))
    return state,reader


def automatic_fixture(side='LONG'):
    from .test_filled_quantity_dispatch import state_from_case
    state=state_from_case(q='100',side=side,stop='100',take='100')
    b=state['bindings'][0];snap=state['evidence']['snapshot']
    snap['fills'][0]['fill_id']='hl:1000'
    manual=fill(b,'STOP',qty='100',fid='hl:9900')
    manual.update(oid='99',at_ms=T-50)
    snap['fills'].append(manual);snap['position_quantity']='0'
    reader=Reader(state['evidence'])
    reader.data['statuses']['99']=dict(status='order',order=dict(status='filled',
        statusTimestamp=T-50,order=dict(oid=99,coin='DOGE',side=manual['side'],
            origSz='100',sz='0',limitPx=manual['price'],reduceOnly=True,
            orderType='Limit',isTrigger=False,triggerPx='0')))
    return state,reader


class Connection:
    def execute(self,*args): return self
    def fetchone(self): return None


class Store:
    def __init__(self,state): self.state=deepcopy(state); self.events=[]; self.changed=False
    def load(self,bucket): return deepcopy(self.state)
    def change(self,bucket,revision,event,now,callback):
        if self.changed: self.state['revision']+=1
        value=deepcopy(self.state); callback(Connection(),value)
        value['revision']+=1; self.state=value; self.events.append(event)
        return deepcopy(value)


class ManualExitTests(unittest.TestCase):
    def setUp(self):
        network=patch('http.client.HTTPSConnection',side_effect=AssertionError('NO_NETWORK'))
        network.start();self.addCleanup(network.stop)

    def test_partial_target_then_manual_close_both_directions(self):
        for side in ('SHORT','LONG'):
            state,reader=fixture(side); store=Store(state)
            out=m.adopt(store,reader,state['bucket'],state['bindings'][0]['card_id'],'99',clock=lambda:T+1000)
            report=life.review(out['bindings'],out['evidence']['snapshot'],now_ms=T+1000)
            row=report['cards'][0]
            self.assertTrue(row['closure_verified'])
            self.assertEqual(row['exit_quantity'],'100')
            self.assertEqual(row['closure_origin'],'MANUAL')
            self.assertEqual(out['bindings'][0]['orders']['STOP'],state['bindings'][0]['orders']['STOP'])
            self.assertEqual(out['manual_exit_audit'][0]['origin'],'MANUAL')
            self.assertFalse(out['manual_exit_audit'][0]['entry_or_exit_order_sent'])
            self.assertEqual(out['emergency']['phase'],'CLOSED_VERIFIED')
            self.assertEqual(store.events,['MANUAL_EXIT_PUBLICLY_RECONCILED'])

    def test_opposite_side_or_wrong_size_is_not_assigned(self):
        for update in ({'side':'A'},{'quantity':'79'}):
            state,reader=fixture(); state['evidence']['snapshot']['fills'][-1].update(update)
            with self.assertRaises(DispatchError):
                m.candidate(state,state['bindings'][0]['card_id'],'99',now_ms=T)

    def test_empty_emergency_requests_require_exact_manual_audit_for_release(self):
        from .emergency_close import closed_release_proof, VERSION
        state,reader=fixture();store=Store(state)
        store.state['emergency'].update(version=VERSION,requests=[],
            card_id=state['bindings'][0]['card_id'],latched_at_ms=T-1000)
        saved=m.adopt(store,reader,state['bucket'],state['bindings'][0]['card_id'],'99',clock=lambda:T+1000)
        self.assertTrue(closed_release_proof(saved,now_ms=T+1000,not_before_ms=T))
        for field,value in (('origin','BOT'),('order_id','98'),('proof_digest','bad'),
                            ('entry_or_exit_order_sent',True)):
            broken=deepcopy(saved);broken['manual_exit_audit'][0][field]=value
            self.assertFalse(closed_release_proof(broken,now_ms=T+1000,not_before_ms=T))
        broken=deepcopy(saved);broken.pop('manual_exit_audit')
        self.assertFalse(closed_release_proof(broken,now_ms=T+1000,not_before_ms=T))

    def test_flat_alone_incomplete_or_pending_never_proves_closure(self):
        for field in ('history_complete','orders_complete'):
            state,_=fixture();state['evidence']['snapshot'][field]=False
            with self.assertRaises(DispatchError):m.candidate(state,state['bindings'][0]['card_id'],'99',now_ms=T)
        state,_=fixture();state['pending']='b'*64
        with self.assertRaises(DispatchError):m.candidate(state,state['bindings'][0]['card_id'],'99',now_ms=T)

    def test_manual_order_must_be_reduce_only_and_terminal_in_public_reads(self):
        for change in ('reduceOnly','status'):
            state,reader=fixture(); store=Store(state)
            raw=reader.data['statuses']['99']['order']
            if change=='reduceOnly':raw['order']['reduceOnly']=False
            else:raw['status']='open'
            with self.assertRaises((SyncError,DispatchError)):
                m.adopt(store,reader,state['bucket'],state['bindings'][0]['card_id'],'99',clock=lambda:T+1000)
            self.assertEqual(store.events,[])

    def test_stale_and_concurrent_state_never_committed(self):
        state,reader=fixture()
        with self.assertRaises(DispatchError):m.candidate(state,state['bindings'][0]['card_id'],'99',now_ms=T+15001)
        store=Store(state);store.changed=True
        with self.assertRaises(DispatchError):
            m.adopt(store,reader,state['bucket'],state['bindings'][0]['card_id'],'99',clock=lambda:T+1000)
        self.assertEqual(store.events,[])

    def test_second_public_pass_change_blocks_adoption(self):
        state,reader=fixture(); store=Store(state); original=reader.read; calls=[]
        def changed(kind,*args,**kw):
            result=original(kind,*args,**kw)
            if kind=='clearinghouseState':
                calls.append(kind)
                if len(calls)==2:result['assetPositions'][0]['position']['szi']='-1'
            return result
        reader.read=changed
        with self.assertRaises(SyncError):
            m.adopt(store,reader,state['bucket'],state['bindings'][0]['card_id'],'99',clock=lambda:T+1000)
        self.assertEqual(store.events,[])

    def test_flat_duplicate_poll_bound_never_applies_to_exposure_or_uncertainty(self):
        state,_=fixture()
        self.assertTrue(recent_flat_unassigned_checkpoint(state,now_ms=T+4999))
        self.assertFalse(recent_flat_unassigned_checkpoint(state,now_ms=T+5000))
        for quantity in ('1','-1'):
            other=deepcopy(state);other['evidence']['snapshot']['position_quantity']=quantity
            self.assertFalse(recent_flat_unassigned_checkpoint(other,now_ms=T))
        state['emergency']['pending_close']='b'*64
        self.assertFalse(recent_flat_unassigned_checkpoint(state,now_ms=T))


class AutomaticManualExitTests(unittest.TestCase):
    def setUp(self):
        network=patch('http.client.HTTPSConnection',side_effect=AssertionError('NO_NETWORK'))
        network.start();self.addCleanup(network.stop)

    def reconcile(self,store,reader):
        return m.reconcile_automatic(store,store.state['bucket'],clock=lambda:T+1000,
            collect=lambda ev:sync.collect(ev,reader,clock=lambda:T+1000,reuse_verified_terminals=True))

    def test_both_accounts_bind_full_manual_close_before_exact_owned_cleanup(self):
        from .test_residual_exit_fence import select
        for side in ('LONG','SHORT'):
            with self.subTest(side=side):
                state,reader=automatic_fixture(side);store=Store(state)
                card=state['bindings'][0]['card_id']
                out=self.reconcile(store,reader)
                self.assertEqual(out['status'],'MANUAL_EXIT_BOUND_AWAITING_OWNED_CLEANUP')
                self.assertEqual(out['order_requests_sent'],0)
                self.assertEqual(store.state['manual_exit_audit'][0]['phase'],'OWNED_ORDER_CLEANUP_PENDING')
                view=life.review(store.state['bindings'],store.state['evidence']['snapshot'],now_ms=T+1000)
                self.assertEqual(view['cards'][0]['remaining_quantity'],'0')
                self.assertFalse(view['cards'][0]['closure_verified'])
                self.assertFalse(view['bucket_issues'])
                proposal=select(store.state)
                self.assertEqual(proposal['card_id'],card)
                self.assertEqual(proposal['operation'],'CANCEL_ORPHAN_EXIT')
                self.assertIn(proposal['old_oid'],state['bindings'][0]['orders']['STOP']+
                                               state['bindings'][0]['orders']['TAKE_PROFIT'])
                self.assertEqual(proposal['action']['type'],'cancel')

    def test_repeat_does_not_read_or_rebind_while_owned_cleanup_pending(self):
        state,reader=automatic_fixture();store=Store(state)
        self.reconcile(store,reader);calls=reader.calls
        self.reconcile(store,reader)
        self.assertEqual(reader.calls,calls)
        self.assertEqual(len(store.state['manual_exit_audit']),1)
        self.assertEqual(len(store.events),1)

    def test_finality_requires_both_owned_exits_terminal_and_survives_repeat(self):
        state,reader=automatic_fixture();store=Store(state)
        self.reconcile(store,reader)
        b=store.state['bindings'][0]
        for leg in ('STOP','TAKE_PROFIT'):
            oid=b['orders'][leg][0]
            reader.data['statuses'][oid]['order'].update(status='canceled',statusTimestamp=T+500)
        reader.data['inventory']=[]
        # The existing dispatcher has independently observed the cancellations.
        observed=sync.collect(store.state['evidence'],reader,clock=lambda:T+1000,
                              reuse_verified_terminals=True)
        store.state['evidence']=dict(bindings=observed['bindings'],snapshot=observed['snapshot'])
        result=self.reconcile(store,reader)
        self.assertEqual(result['status'],'MANUAL_EXIT_CLOSURE_VERIFIED')
        saved=deepcopy(store.state)
        self.assertEqual(saved['manual_exit_audit'][0]['phase'],'CLOSED_VERIFIED')
        self.assertTrue(life.review(saved['bindings'],saved['evidence']['snapshot'],now_ms=T+1000)['cards'][0]['closure_verified'])
        calls=reader.calls
        self.assertEqual(self.reconcile(store,reader)['status'],'NO_MANUAL_EXIT_CANDIDATE')
        self.assertEqual(store.state,saved);self.assertEqual(reader.calls,calls)

    def test_bad_manual_order_never_creates_audit_or_cleanup_authority(self):
        for change in ('reduceOnly','status','side','origSz'):
            with self.subTest(change=change):
                state,reader=automatic_fixture();store=Store(state)
                raw=reader.data['statuses']['99']['order']
                if change=='status':raw['status']='canceled'
                else:raw['order'][change]={'reduceOnly':False,'side':'B','origSz':'101'}[change]
                with self.assertRaises((DispatchError,SyncError)):
                    self.reconcile(store,reader)
                self.assertEqual(store.events,[])
                self.assertNotIn('MANUAL_EXIT',store.state['bindings'][0]['orders'])

    def test_ambiguous_history_working_entry_or_unowned_order_blocks_adoption(self):
        for change in ('multiple_unknown','two_active_cards','working_entry','unowned_exit','changed_original',
                       'wrong_size','wrong_side','earlier_manual','pending','history_incomplete'):
            with self.subTest(change=change):
                state,_=automatic_fixture();b=state['bindings'][0];snap=state['evidence']['snapshot']
                if change=='multiple_unknown':
                    extra=deepcopy(snap['fills'][-1]);extra.update(oid='98',fill_id='hl:9800')
                    snap['fills'].append(extra)
                elif change=='two_active_cards':
                    other=binding(2,qty='100');state['bindings'].append(other)
                    state['evidence']['bindings']=deepcopy(state['bindings']);snap['fills'].append(fill(other))
                elif change=='working_entry':
                    snap['open_orders'].append(order(b,'ENTRY',qty='1'))
                    snap['terminal_orders']=[]
                elif change=='unowned_exit':
                    extra=deepcopy(snap['open_orders'][0]);extra['oid']='98';snap['open_orders'].append(extra)
                elif change=='changed_original':b['card_digest']='0'*64
                elif change=='wrong_size':snap['fills'][-1]['quantity']='99'
                elif change=='wrong_side':snap['fills'][-1]['side']='B'
                elif change=='earlier_manual':snap['fills'][-1]['at_ms']=T-10001
                elif change=='pending':state['pending']='a'*64
                else:snap['history_complete']=False
                with self.assertRaises((DispatchError,life.LifecycleError)):
                    m.automatic_candidate(state,now_ms=T)

    def test_changed_second_pass_or_concurrent_bucket_does_not_adopt(self):
        state,reader=automatic_fixture();store=Store(state);original=reader.read;calls=[]
        def changed(kind,*args,**kwargs):
            result=original(kind,*args,**kwargs)
            if kind=='clearinghouseState':
                calls.append(kind)
                if len(calls)==2:result['assetPositions'][0]['position']['szi']='1'
            return result
        reader.read=changed
        with self.assertRaises(SyncError):self.reconcile(store,reader)
        self.assertEqual(store.events,[])
        state,reader=automatic_fixture();store=Store(state);store.changed=True
        with self.assertRaises(DispatchError):self.reconcile(store,reader)
        self.assertEqual(store.events,[])

    def test_manual_close_retains_emergency_latch_through_cleanup(self):
        from .emergency_close import VERSION
        state,reader=automatic_fixture();card=state['bindings'][0]['card_id']
        state['emergency']=dict(version=VERSION,phase='ACTIVE',card_id=card,
            requests=[],pending_close=None,pending_cancel=None,latched_at_ms=T-100)
        store=Store(state);self.reconcile(store,reader)
        self.assertEqual(store.state['emergency']['phase'],'ACTIVE')
        self.assertEqual(store.state['emergency']['requests'],[])

    def test_provisional_incident_confirms_only_to_owned_cancel_and_stays_latched(self):
        from types import SimpleNamespace
        from .emergency_close import Controller, VERSION, fence, closed_release_proof
        from .test_filled_quantity_dispatch import Venue, ROUTES2
        state,reader=automatic_fixture();card=state['bindings'][0]['card_id']
        state['emergency']=dict(version=VERSION,phase='ACTIVE',card_id=card,
            provisional=True,requests=[],pending_close=None,pending_cancel=None,latched_at_ms=T-100)
        store=Store(state);store.domain='software'
        self.reconcile(store,reader)
        self.assertFalse(store.state['emergency']['provisional'])
        self.assertEqual(store.state['emergency']['phase'],'ACTIVE')
        self.assertTrue(store.state['manual_exit_audit'][0]['provisional_emergency_confirmed_by_manual_proof'])
        venue=Venue();venue.t=T+1000
        normal=SimpleNamespace(store=store,venue=venue,routes=ROUTES2)
        proposal=Controller(normal,venue=venue).proposal(store.state)
        self.assertEqual(proposal['operation'],'EMERGENCY_CANCEL')
        self.assertEqual(proposal['action']['type'],'cancel')
        self.assertEqual(proposal['quantity'],'0')
        self.assertIn(proposal['old_oid'],state['bindings'][0]['orders']['STOP']+
                                         state['bindings'][0]['orders']['TAKE_PROFIT'])
        with self.assertRaisesRegex(DispatchError,'EMERGENCY_BUCKET_MANAGED_BY_SEPARATE_LANE'):
            fence(Connection(),store.state,'ENTRY')
        self.assertFalse(closed_release_proof(store.state,now_ms=T+1000,not_before_ms=T))
        for leg in ('STOP','TAKE_PROFIT'):
            oid=store.state['bindings'][0]['orders'][leg][0]
            reader.data['statuses'][oid]['order'].update(status='canceled',statusTimestamp=T+500)
        reader.data['inventory']=[]
        observed=sync.collect(store.state['evidence'],reader,clock=lambda:T+1000,
                              reuse_verified_terminals=True)
        store.state['evidence']=dict(bindings=observed['bindings'],snapshot=observed['snapshot'])
        self.reconcile(store,reader)
        self.assertEqual(store.state['emergency']['phase'],'CLOSED_VERIFIED')
        self.assertFalse(store.state['emergency']['provisional'])
        self.assertTrue(store.state['manual_exit_audit'][0]['provisional_emergency_confirmed_by_manual_proof'])
        self.assertTrue(closed_release_proof(store.state,now_ms=T+1000,not_before_ms=T))
        with self.assertRaisesRegex(DispatchError,'EMERGENCY_BUCKET_MANAGED_BY_SEPARATE_LANE'):
            fence(Connection(),store.state,'ENTRY')

    def test_auto_full_final_close_records_audit_and_retains_closed_circuit(self):
        from .emergency_close import VERSION, closed_release_proof
        for side in ('LONG','SHORT'):
            state,reader=automatic_fixture(side);b=state['bindings'][0]
            snap=state['evidence']['snapshot']
            snap['open_orders']=[]
            snap['terminal_orders'] += [terminal(b,leg,'0') for leg in ('STOP','TAKE_PROFIT')]
            manual_status=reader.data['statuses']['99']
            reader=Reader(state['evidence']);reader.data['statuses']['99']=manual_status
            state['emergency']=dict(version=VERSION,phase='ACTIVE',card_id=b['card_id'],
                requests=[],pending_close=None,pending_cancel=None,latched_at_ms=T-100)
            store=Store(state);result=self.reconcile(store,reader)
            self.assertEqual(result['status'],'MANUAL_EXIT_CLOSURE_VERIFIED')
            self.assertEqual(store.state['emergency']['phase'],'CLOSED_VERIFIED')
            self.assertEqual(store.state['emergency']['closure_origin'],'MANUAL')
            self.assertTrue(closed_release_proof(store.state,now_ms=T+1000,not_before_ms=T))

    def test_duplicate_fill_identity_does_not_double_count_manual_quantity(self):
        state,_=automatic_fixture();snap=state['evidence']['snapshot']
        snap['fills'].append(deepcopy(snap['fills'][-1]))
        self.assertIsNotNone(m.automatic_candidate(state,now_ms=T))

    def test_unowned_terminal_is_never_reused_to_bypass_two_manual_status_reads(self):
        state,reader=automatic_fixture();snap=state['evidence']['snapshot']
        snap['terminal_orders'].append(dict(account=state['account'],symbol=state['symbol'],
            oid='99',state='FILLED',filled_quantity='100',at_ms=T-50))
        store=Store(state);original=reader.read;reads=[]
        def observed(kind,*args,**kwargs):
            if kind=='orderStatus' and kwargs.get('oid')=='99':reads.append('99')
            return original(kind,*args,**kwargs)
        reader.read=observed;self.reconcile(store,reader)
        self.assertEqual(reads,['99','99'])
        state['bindings'][0]['orders'].pop('MANUAL_EXIT',None)
        reader.data['statuses']['99']['order']['order']['reduceOnly']=False
        with self.assertRaises(SyncError):self.reconcile(Store(state),reader)


@unittest.skipUnless(os.environ.get('HL_JOURNAL_CI_URL'),'Disposable loopback PostgreSQL required')
class ManualExitDatabaseTests(unittest.TestCase):
    def setUp(self):
        from .postgres_journal import PostgresJournal
        from .filled_dispatch_store import DispatchStore, SCHEMA
        self.schema=SCHEMA
        self.j=PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL'])
        with self.j._transaction() as conn:
            for name in (SCHEMA,'hl_testnet_recovery_rehearsal_v1','hl_testnet_execution_v1'):
                conn.execute(f'DROP SCHEMA IF EXISTS {name} CASCADE')
        self.j.bootstrap();self.store=DispatchStore(self.j);self.store.initialize()
        state,self.reader=fixture()
        created=self.store.create_bucket(state['account'],state['symbol'])
        self.bucket=created['bucket'];self.card=state['bindings'][0]['card_id']
        def seed(conn,current):
            current.update(bindings=state['bindings'],evidence=state['evidence'],emergency=state['emergency'])
        self.store.change(self.bucket,created['revision'],'FIXTURE',T,seed)

    def test_audit_survives_restart_and_repeat_cannot_reassign(self):
        from .filled_dispatch_store import DispatchStore
        m.adopt(self.store,self.reader,self.bucket,self.card,'99',clock=lambda:T+1000)
        saved=DispatchStore(self.j).load(self.bucket)
        self.assertTrue(life.review(saved['bindings'],saved['evidence']['snapshot'],now_ms=T+1000)['cards'][0]['closure_verified'])
        self.assertEqual(saved['manual_exit_audit'][0]['origin'],'MANUAL')
        with self.assertRaises(DispatchError):
            m.adopt(self.store,self.reader,self.bucket,self.card,'99',clock=lambda:T+1000)
        with self.j._transaction() as conn:
            events=conn.execute(f'SELECT event FROM {self.schema}.events WHERE bucket=%s ORDER BY revision',(self.bucket,)).fetchall()
            self.assertEqual([x[0] for x in events],['FIXTURE','MANUAL_EXIT_PUBLICLY_RECONCILED'])
            self.assertEqual(conn.execute(f'SELECT count(*) FROM {self.schema}.requests').fetchone()[0],0)

    def test_concurrent_checkpoint_rolls_back_audit_and_binding(self):
        read=self.reader.read;updated=[]
        def concurrent(kind,*args,**kwargs):
            result=read(kind,*args,**kwargs)
            if not updated:
                current=self.store.load(self.bucket)
                self.store.change(self.bucket,current['revision'],'OTHER_READER',T,lambda conn,s:None)
                updated.append(True)
            return result
        self.reader.read=concurrent
        with self.assertRaises(DispatchError):
            m.adopt(self.store,self.reader,self.bucket,self.card,'99',clock=lambda:T+1000)
        saved=self.store.load(self.bucket)
        self.assertNotIn('manual_exit_audit',saved)
        self.assertNotIn('MANUAL_EXIT',saved['bindings'][0]['orders'])


@unittest.skipUnless(os.environ.get('HL_JOURNAL_CI_URL'),'Disposable loopback PostgreSQL required')
class AutomaticManualExitDatabaseTests(unittest.TestCase):
    def setUp(self):
        from .postgres_journal import PostgresJournal
        from .filled_dispatch_store import DispatchStore
        from .trade_card_store import CardStore
        self.j=PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL'])
        with self.j._transaction() as conn:
            for name in (SCHEMA,'hl_testnet_recovery_rehearsal_v1','hl_testnet_cards_v1','hl_testnet_execution_v1'):
                conn.execute(f'DROP SCHEMA IF EXISTS {name} CASCADE')
        self.j.bootstrap();self.cards=CardStore(self.j);self.cards.initialize()
        self.store=DispatchStore(self.j);self.store.initialize()
        for target in ('http.client.HTTPSConnection','hyperliquid_testnet_executor._wallet',
                       'hyperliquid_testnet_executor._signed_body'):
            forbidden=patch(target,side_effect=AssertionError('NO_NETWORK_OR_SIGNER'))
            forbidden.start();self.addCleanup(forbidden.stop)

    def test_both_accounts_actual_controller_cleans_owned_orders_after_restart(self):
        from . import filled_quantity_dispatch as dispatch
        from .long_stream_runtime import _maintain_bucket
        from .filled_dispatch_store import DispatchStore
        from .test_filled_quantity_dispatch import Venue, ROUTES2
        from .test_filled_quantity_exits import original
        for n,side in enumerate(('LONG','SHORT'),1):
            with self.subTest(side=side):
                venue=Venue();controller=dispatch.Controller(self.store,venue,ROUTES2,
                    after_exit_policy=dispatch.AFTER_EXIT)
                binding,record=original(n,side=side);card_id=binding['card_id']
                self.cards.record(record['card'])
                bucket=controller.register(card_id)['bucket']
                controller.cycle(bucket,send=True)
                venue.fill('1000','100')
                controller.cycle(bucket,send=True)  # owned STOP
                controller.cycle(bucket,send=True)  # owned TAKE_PROFIT
                controller.refresh(bucket)
                # The independent human close is synthetic external evidence;
                # the controller does not send this order or claim its origin.
                closing='A' if side=='LONG' else 'B'
                venue.orders['9999']=dict(account=binding['account'],status='open',
                    statusTimestamp=venue.now(),order=dict(oid=9999,coin='DOGE',side=closing,
                    cloid='0x'+'f'*32,origSz='100',sz='100',limitPx='10',reduceOnly=True,
                    isTrigger=False,triggerPx='0',isPositionTpsl=False,orderType='Limit'))
                venue.fill('9999','100');controller.refresh(bucket)
                sent_before=venue.sent
                first=m.reconcile_controller(controller,bucket)
                self.assertEqual(first['status'],'MANUAL_EXIT_BOUND_AWAITING_OWNED_CLEANUP')
                self.assertEqual(venue.sent,sent_before)
                restarted=dispatch.Controller(DispatchStore(self.j),venue,ROUTES2,
                    after_exit_policy=dispatch.AFTER_EXIT)
                results=list(_maintain_bucket(restarted,bucket))
                self.assertEqual(sum(result['order_requests_sent'] for result in results),2)
                final=m.reconcile_controller(restarted,bucket)
                self.assertEqual(final['status'],'NO_MANUAL_EXIT_CANDIDATE')
                saved=DispatchStore(self.j).load(bucket)
                view=life.review(saved['bindings'],saved['evidence']['snapshot'],now_ms=venue.now())
                self.assertTrue(view['cards'][0]['closure_verified'])
                self.assertEqual(view['cards'][0]['closure_origin'],'MANUAL')
                self.assertEqual(saved['manual_exit_audit'][0]['phase'],'CLOSED_VERIFIED')
                self.assertEqual(venue.sent,sent_before+2)
                self.assertTrue(all(request['proposal']['operation']=='CANCEL_ORPHAN_EXIT'
                    for request in venue.requests[sent_before:]))
                self.assertFalse(saved['evidence']['snapshot']['open_orders'])
                self.assertEqual(m.reconcile_controller(restarted,bucket)['status'],'NO_MANUAL_EXIT_CANDIDATE')
                self.assertEqual(venue.sent,sent_before+2)
        with self.j._transaction() as conn:
            events=[row[0] for row in conn.execute(f'SELECT event FROM {SCHEMA}.events').fetchall()]
            self.assertEqual(events.count('MANUAL_EXIT_AUTOMATIC_BOUND_OWNED_CLEANUP_PENDING'),2)
            self.assertEqual(events.count('MANUAL_EXIT_AUTOMATIC_CLOSURE_VERIFIED'),2)

    def test_failed_audit_commit_does_not_bind_or_authorize_cancellation(self):
        state,reader=automatic_fixture();created=self.store.create_bucket(state['account'],state['symbol'])
        def seed(conn,current):
            current.update(bindings=state['bindings'],evidence=state['evidence'],originals=state['originals'])
        self.store.change(created['bucket'],created['revision'],'FIXTURE',T,seed)
        previous=self.store.load(created['bucket'])
        change=self.store.change
        def fail_commit(bucket,revision,event,now,callback):
            def fail(conn,current):
                callback(conn,current)
                raise DispatchError('AUDIT_COMMIT_UNAVAILABLE')
            return change(bucket,revision,event,now,fail)
        with patch.object(self.store,'change',side_effect=fail_commit):
            with self.assertRaisesRegex(DispatchError,'AUDIT_COMMIT_UNAVAILABLE'):
                m.reconcile_automatic(self.store,created['bucket'],clock=lambda:T+1000,
                    collect=lambda ev:sync.collect(ev,reader,clock=lambda:T+1000,reuse_verified_terminals=True))
        self.assertEqual(self.store.load(created['bucket']),previous)
        self.assertNotIn('MANUAL_EXIT',previous['bindings'][0]['orders'])
        with self.j._transaction() as conn:
            self.assertEqual(conn.execute(f'SELECT count(*) FROM {SCHEMA}.requests').fetchone()[0],0)


if __name__=='__main__':unittest.main()
