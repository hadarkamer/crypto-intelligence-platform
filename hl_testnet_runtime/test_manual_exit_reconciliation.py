"""Manual exit identities, public finality and no-send boundaries."""
from copy import deepcopy
import os
import unittest
from unittest.mock import patch
from . import card_lifecycle as life, manual_exit_reconciliation as m
from .emergency_close import recent_flat_unassigned_checkpoint
from .filled_dispatch_store import DispatchError
from .card_sync_evidence import SyncError
from .test_card_lifecycle import binding, fill, terminal, snapshot, T
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


if __name__=='__main__':unittest.main()
