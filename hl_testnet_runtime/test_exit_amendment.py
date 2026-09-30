"""Owned exit amendments and restart safety against a real disposable journal."""
from copy import deepcopy
import json
import unittest
from . import filled_quantity_dispatch as m, card_lifecycle as life
from . import residual_exit_fence as fence, residual_exit_contract as contract
from .filled_dispatch_store import DispatchStore, DispatchError
from .test_filled_quantity_dispatch import NoExternal, DispatchDatabaseTests, state_from_case, ROUTES2, META, CI
from .test_card_lifecycle import T


class ExitAmendmentPureTests(NoExternal):
    def proposal(self,side='LONG'):
        s=state_from_case(q='60',stop='40',take='40',side=side)
        return s,m.choose(s,ROUTES2,META,dict(mark_price='10',at_ms=T),now_ms=T)

    def test_both_directions_keep_stop_price_side_and_exact_owned_quantity(self):
        for side in ('LONG','SHORT'):
            s,p=self.proposal(side);o=m.requested_order(p['action'])
            self.assertEqual(o['p'],s['bindings'][0]['prices']['stop'])
            self.assertEqual(o['b'],side=='SHORT');self.assertTrue(o['r'])
            self.assertEqual(o['s'],'60')
            self.assertTrue(fence.validate_proposal(s,p,now_ms=T))
            self.assertTrue(contract.validate_wire_proposal(s,p,now_ms=T))

    def test_foreign_oid_asset_price_side_size_and_entry_labels_fail_closed(self):
        for change in ('oid','asset','price','side','size','entry'):
            s,p=self.proposal();item=p['action']['modifies'][0];o=item['order']
            if change=='oid':item['oid']+=999
            if change=='asset':o['a']+=1
            if change=='price':o['p']='1'
            if change=='side':o['b']=not o['b']
            if change=='size':o['s']=p['quantity']='70'
            if change=='entry':p['leg']='ENTRY'
            with self.assertRaises(life.LifecycleError):
                fence.validate_proposal(s,p,now_ms=T)
                contract.validate_wire_proposal(s,p,now_ms=T)

    def test_jsonb_key_sorting_produces_sdk_wire_order_and_hash(self):
        from hyperliquid.utils.signing import action_hash
        _,p=self.proposal();a=p['action']
        wire=m.canonical_wire_action(json.loads(json.dumps(a,sort_keys=True)))
        self.assertEqual(list(wire),['type','modifies'])
        self.assertEqual(list(wire['modifies'][0]),['oid','order'])
        self.assertEqual(list(wire['modifies'][0]['order']),['a','b','p','s','r','t','c'])
        self.assertEqual(action_hash(wire,None,T,T+15000),action_hash(a,None,T,T+15000))


@unittest.skipUnless(CI,'Disposable loopback PostgreSQL required')
class ExitAmendmentDatabaseTests(NoExternal):
    def setUp(self):
        super().setUp();self.h=DispatchDatabaseTests('runTest');self.h.setUp()
        self.addCleanup(self.h.doCleanups)

    def test_lost_modify_reply_reopens_same_journal_without_resending(self):
        h=self.h;h.protect();h.v.fill('1000','20');h.v.lose_reply=True
        self.assertEqual(h.cycle()['status'],'OUTCOME_UNKNOWN');sent=h.v.sent
        h.c=m.Controller(DispatchStore(h.j),h.v,ROUTES2)
        h.cycle(False)
        self.assertIsNone(h.store.load(h.bucket)['pending']);self.assertEqual(h.v.sent,sent)
        self.assertEqual(h.remaining()[0]['stop_quantity_observed'],'60')
        self.assertEqual(h.v.orders['1003']['status'],'open')
        self.assertEqual(h.v.orders['1001']['status'],'canceled')

    def test_never_sent_quantity_change_retires_stale_amendment(self):
        h=self.h;h.protect();h.v.fill('1000','20');h.v.t+=1
        s=h.c.refresh(h.bucket)
        p=m.choose(s,ROUTES2,META,h.v.sample(s['account'],s['symbol']),now_ms=h.v.now())
        s=h.store.reserve(s,p,h.v.now());rid=s['pending']
        h.v.fill('1000','10');h.cycle()
        self.assertEqual(h.store.request(rid)['phase'],'ABORTED_UNSENT')
        self.assertEqual(m.requested_order(h.v.requests[-1]['proposal']['action'])['s'],'70')
        self.assertEqual(h.store.request(rid)['attempts'],0)

    def test_rejected_modify_keeps_prior_stop_and_blocks_new_work(self):
        from unittest.mock import patch
        h=self.h;h.protect();h.v.fill('1000','20')
        with patch.object(h.v,'send',return_value=dict(status='err',response='Invalid TP/SL price.')):h.cycle()
        self.assertEqual(h.v.orders['1001']['status'],'open')
        before=h.v.sent
        h.c=m.Controller(DispatchStore(h.j),h.v,ROUTES2)
        with self.assertRaises(DispatchError):h.cycle()
        self.assertEqual(h.v.sent,before)
        self.assertIsNotNone(h.store.load(h.bucket)['pending'])

    def test_supervised_unchanged_size_probe_uses_same_durable_send_boundary(self):
        h=self.h;h.protect();h.v.t+=1
        s=h.c.refresh(h.bucket)
        p=m.exit_modify_proposal(s,META,h.v.sample(s['account'],s['symbol']),
            card_id=s['bindings'][0]['card_id'],leg='STOP',old_oid='1001',now_ms=h.v.now())
        h.v.authorize(s,p,h.c.after_exit_policy)
        s=h.store.reserve(s,p,h.v.now())
        self.assertIsNotNone(fence.retire_obsolete_unsent(h.store,s,now_ms=h.v.now())['pending'])
        s=h.store.begin(s,p,ROUTES2[p['role']]['agent'],h.v.now())
        request=h.store.request(s['pending'])
        raw=h.v.send(request)
        h.store.reply(s,m.normalized_reply(raw,'batchModify'),h.v.now())
        h.v.t+=1;h.c.refresh(h.bucket)
        self.assertIsNone(h.store.load(h.bucket)['pending'])
        self.assertEqual(h.v.orders['1001']['status'],'canceled')
        self.assertEqual(h.remaining()[0]['stop_quantity_observed'],'40')

    def test_unknown_modify_keeps_durable_barrier_even_after_another_entry_fill(self):
        from unittest.mock import patch
        h=self.h;h.protect();h.v.fill('1000','20')
        with patch.object(h.v,'send',side_effect=TimeoutError()):h.cycle()
        h.v.fill('1000','10');before=h.v.sent
        with self.assertRaises(DispatchError):h.cycle()
        self.assertEqual(h.v.sent,before);self.assertEqual(h.v.orders['1001']['status'],'open')

    def test_partial_take_shrinks_stop_without_canceling_its_guard(self):
        h=self.h;h.c.after_exit_policy=m.AFTER_EXIT;h.protect();h.v.fill('1002','10')
        h.cycle();h.cycle();h.cycle(False)
        self.assertEqual(h.v.requests[-1]['proposal']['operation'],'MODIFY_EXIT')
        self.assertEqual(h.remaining()[0]['stop_quantity_observed'],'30')
        self.assertEqual(h.remaining()[0]['take_profit_quantity_observed'],'30')
        self.assertEqual(h.v.orders['1003']['status'],'open')
        self.assertEqual([r['proposal']['leg'] for r in h.v.requests if r['proposal']['action']['type']=='cancel'],['ENTRY'])
