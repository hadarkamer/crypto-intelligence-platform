"""Half-threshold integration: actual controller/store, software venue ONLY."""
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
import os
import subprocess
import sys
import unittest
from unittest.mock import patch

from . import filled_quantity_dispatch as m, filled_pending_cancel as half
from . import card_lifecycle as life, filled_trial_runtime as runtime
from . import test_filled_quantity_dispatch as fx
from .test_filled_quantity_exits import original, META
from .filled_dispatch_store import DispatchStore, DispatchError, SCHEMA
from .postgres_journal import PostgresJournal, JournalError
from .trade_card_store import CardStore

T=fx.T
CI=os.environ.get('HL_JOURNAL_CI_URL')


def sample(price, at=T):
    return dict(mark_price=price, at_ms=at)


class HalfThresholdPureTests(fx.NoExternal):
    def select(self, state, price, now=T):
        return m.choose(state,fx.ROUTES2,META,sample(price,now),now_ms=now)

    def test_exact_boundary_uses_half_formula_not_stop_or_take_distance(self):
        for side,boundary,below,opposite in (
                ('LONG','10.075','10.074999','9.925'),
                ('SHORT','9.925','9.925001','10.075')):
            s=fx.state_from_case(q='0',side=side);before=deepcopy(s)
            self.assertIsNone(self.select(s,below));self.assertIsNone(self.select(s,opposite))
            p=self.select(s,boundary)
            self.assertEqual(p['operation'],half.OPERATION)
            self.assertEqual(p['old_oid'],s['bindings'][0]['orders']['ENTRY'][0])
            self.assertEqual(p['action'],dict(type='cancel',cancels=[dict(a=0,o=int(p['old_oid']))]))
            self.assertEqual(s,before)

    def test_different_formula_threshold_and_newer_rule_names_are_preserved(self):
        from . import trade_cards, filled_quantity_exits as selected
        for threshold,price in (('1','10.05'),('2','10.1'),('0.25','10.0125')):
            s=fx.state_from_case(q='0');cid=s['bindings'][0]['card_id'];old=s['originals'][cid]
            card=trade_cards.prepare_card(old['card']['prepared']['source'],META,
                rule_id='formula:lower-case.1',threshold_pct=threshold,record_kind='synthetic_test')
            draft=selected.prepare_entry(card,META,s['account'],fx.ROUTES2)
            b=life.binding_from_card(card,s['account'],fx.ROUTES2,s['bindings'][0]['orders'])
            s['originals'][cid]=dict(card=card,draft=draft);s['bindings']=[b]
            s['evidence']['bindings']=[b]
            self.assertEqual(self.select(s,price)['operation'],half.OPERATION)
            self.assertEqual(s['originals'][cid]['card']['rule']['id'],'formula:lower-case.1')

    def test_any_partial_fill_excludes_half_cancel_and_keeps_pending_entry(self):
        s=fx.state_from_case(q='40',stop='40',take='40')
        self.assertIsNone(self.select(s,'10.08'))
        self.assertEqual(s['evidence']['snapshot']['open_orders'][0]['quantity'],'60')
        s=fx.state_from_case(q='40')
        p=self.select(s,'10.08');self.assertEqual((p['operation'],p['quantity']),('CREATE_EXIT','40'))

    def test_stale_price_or_order_snapshot_never_authorizes_half_cancel(self):
        s=fx.state_from_case(q='0')
        self.assertIsNone(self.select(s,'10.08',now=T+10001))
        report=half.scan(s,fx.ROUTES2,sample('10.08',T-10001),now_ms=T)
        self.assertEqual(report['candidates'],[]);self.assertEqual(report['new_observations'],{})

    def test_incomplete_history_or_inventory_cannot_mean_zero_fill(self):
        for field in ('history_complete','orders_complete'):
            s=fx.state_from_case(q='0');s['evidence']['snapshot'][field]=False
            with self.assertRaises(DispatchError):half.scan(s,fx.ROUTES2,sample('10.08'),now_ms=T)

    def test_position_discrepancy_cannot_cancel(self):
        s=fx.state_from_case(q='0');s['evidence']['snapshot']['position_quantity']='1'
        with self.assertRaises(DispatchError):self.select(s,'10.08')

    def test_latch_survives_recross_but_cannot_override_a_fill(self):
        s=fx.state_from_case(q='0')
        report=half.scan(s,fx.ROUTES2,sample('10.08'),now_ms=T)
        s[half.FIELD]=report['new_observations']
        self.assertEqual(self.select(s,'10')['operation'],half.OPERATION)
        filled=fx.state_from_case(q='40',stop='40',take='40');filled[half.FIELD]=s[half.FIELD]
        self.assertIsNone(self.select(filled,'10'))

    def test_foreign_or_forged_latch_rejected(self):
        for field,bad in (('oid','999'),('rule_digest','0'*64),('mark_price','10')):
            s=fx.state_from_case(q='0');s[half.FIELD]=half.scan(s,fx.ROUTES2,sample('10.08'),now_ms=T)['new_observations']
            s[half.FIELD][s['bindings'][0]['card_id']][field]=bad
            with self.assertRaises((DispatchError,ValueError)):self.select(s,'10')

    def test_own_unfilled_entry_not_other_cards_position(self):
        s=fx.state_from_case(q='0');other=fx.state_from_case(q='100',n=2,stop='100',take='100')
        s['bindings']+=other['bindings'];s['originals'].update(other['originals'])
        a=s['evidence']['snapshot'];b=other['evidence']['snapshot']
        for key in ('open_orders','terminal_orders','fills'):a[key]+=b[key]
        a['position_quantity']='100';s['evidence']['bindings']=s['bindings']
        p=self.select(s,'10.08')
        self.assertEqual(p['card_id'],s['bindings'][0]['card_id'])
        self.assertEqual(p['action']['cancels'],[dict(a=0,o=10)])

    def test_existing_unprotected_card_is_serviced_before_unfilled_cancel(self):
        s=fx.state_from_case(q='0');other=fx.state_from_case(q='40',n=2)
        s['bindings']+=other['bindings'];s['originals'].update(other['originals'])
        a=s['evidence']['snapshot'];b=other['evidence']['snapshot']
        for key in ('open_orders','terminal_orders','fills'):a[key]+=b[key]
        a['position_quantity']='40';s['evidence']['bindings']=s['bindings']
        p=self.select(s,'10.08');self.assertEqual((p['operation'],p['leg']),('CREATE_EXIT','STOP'))
        self.assertEqual(p['card_id'],other['bindings'][0]['card_id'])

    def test_app_copy_or_changed_source_threshold_cannot_control_cancellation(self):
        with self.assertRaises((ValueError,KeyError)):half.scan(dict(display_copy=True),fx.ROUTES2,sample('10.08'),now_ms=T)
        s=fx.state_from_case(q='0');cid=s['bindings'][0]['card_id']
        s['originals'][cid]['card']['rule']['threshold_pct']='0.01'
        with self.assertRaises(ValueError):self.select(s,'10.08')

    def test_final_adapter_checks_ten_second_bound_without_reading_key(self):
        p=self.select(fx.state_from_case(q='0'),'10.08')
        req=dict(proposal=p,domain='testnet',phase='OUTCOME_UNKNOWN',attempts=1,
            prepared_at_ms=T+9000,attempt_at_ms=T+9001,nonce=T+9001)
        venue=m.TestnetVenue({})
        with patch.object(venue,'now',return_value=T+10001),patch.object(venue,'_gate',return_value=fx.ROUTES2['long_account']),\
             patch.object(m.roles,'wallet_for_role',side_effect=AssertionError('NO_KEY_ACCESS')) as key:
            with self.assertRaisesRegex(DispatchError,'^HALF_THRESHOLD_FINAL_EVIDENCE_EXPIRED$'):venue.send(req)
            key.assert_not_called()
        self.assertEqual(venue.sent,0)

    def test_current_disabled_release_does_not_send_cancel(self):
        p=self.select(fx.state_from_case(q='0'),'10.08');venue=m.TestnetVenue({})
        with self.assertRaisesRegex(DispatchError,'^FILLED_DISPATCH_NOT_AUTHORIZED$'):venue.send(dict(proposal=p))
        self.assertEqual(venue.sent,0)


@unittest.skipUnless(CI,'Requires isolated loopback PostgreSQL')
class HalfThresholdDatabaseTests(fx.NoExternal):
    def setUp(self):
        super().setUp();self.j=PostgresJournal.for_ci(CI)
        with self.j._transaction() as conn:
            for name in (SCHEMA,'hl_testnet_recovery_rehearsal_v1','hl_testnet_cards_v1','hl_testnet_execution_v1'):
                conn.execute(f'DROP SCHEMA IF EXISTS {name} CASCADE')
        self.j.bootstrap();self.cards=CardStore(self.j);self.cards.initialize()
        self.store=DispatchStore(self.j);self.store.initialize();self.v=fx.Venue();self.mark='10'
        self.v.sample=lambda account,symbol:sample(self.mark,self.v.now())
        self.c=m.Controller(self.store,self.v,fx.ROUTES2,after_exit_policy=m.AFTER_EXIT)
        self.b,self.o=original();self.cards.record(self.o['card'])
        self.bucket=self.c.register(self.b['card_id'])['bucket']

    def cycle(self,send=True):
        self.v.t+=1
        return self.c.cycle(self.bucket,send=send)

    def entry(self):
        self.assertEqual(self.cycle()['status'],'ACCEPTED_UNVERIFIED')
        self.cycle(False)  # Confirm the placed entry, still entirely unfilled.

    def prepared_cancel(self):
        self.entry();self.mark='10.08';p=self.cycle(False)['proposal']
        s=self.store.reserve(self.store.load(self.bucket),p,self.v.now())
        return s['pending']

    def test_full_worker_chain_cancels_unfilled_then_finishes_without_reentry(self):
        self.entry();self.mark='10.075';r=self.cycle()
        self.assertEqual(r['status'],'ACCEPTED_UNVERIFIED')
        self.assertEqual(self.v.requests[-1]['proposal']['operation'],half.OPERATION)
        self.v.t+=1;done=runtime.tick(self.c,self.bucket,send=True)
        self.assertTrue(done['finished']);self.assertEqual(self.v.sent,2)
        self.assertEqual(self.v.orders['1000']['status'],'canceled')
        for _ in range(3):self.cycle()
        self.assertEqual(self.v.sent,2)

    def test_short_account_cancel_uses_own_order(self):
        self.b,self.o=original(2,'SHORT');self.cards.record(self.o['card'])
        self.bucket=self.c.register(self.b['card_id'])['bucket'];self.entry();self.mark='9.925'
        self.cycle();self.assertEqual(self.v.requests[-1]['proposal']['account'],fx.B)
        self.assertEqual(self.v.requests[-1]['proposal']['action']['cancels'],[dict(a=0,o=1000)])

    def test_preview_latches_but_does_not_reserve_or_send_cancellation(self):
        self.entry();count=self.v.sent;self.mark='10.08';r=self.cycle(False)
        self.assertEqual(r['proposal']['operation'],half.OPERATION);self.assertEqual(self.v.sent,count)
        s=self.store.load(self.bucket);self.assertIsNone(s['pending']);self.assertIn(self.b['card_id'],s[half.FIELD])
        with self.j._transaction() as conn:self.assertEqual(conn.execute(f'SELECT count(*) FROM {SCHEMA}.requests').fetchone()[0],1)

    def test_latched_crossing_survives_new_process_and_returning_price(self):
        self.entry();self.mark='10.08';self.cycle(False)
        script="import os; from hl_testnet_runtime.postgres_journal import PostgresJournal; from hl_testnet_runtime.filled_dispatch_store import DispatchStore; s=DispatchStore(PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL'])).load('"+self.bucket+"'); print(len(s['half_threshold_observations']),s['pending'])"
        r=subprocess.run([sys.executable,'-c',script],capture_output=True,text=True,check=True,timeout=15)
        self.assertEqual(r.stdout.strip(),'1 None')
        self.store=DispatchStore(PostgresJournal.for_ci(CI));self.c=m.Controller(self.store,self.v,fx.ROUTES2,after_exit_policy=m.AFTER_EXIT)
        self.mark='10';self.cycle();self.assertEqual(self.v.requests[-1]['proposal']['operation'],half.OPERATION)

    def test_prepared_cancel_revalidated_after_price_recross(self):
        rid=self.prepared_cancel();self.mark='10';self.cycle()
        self.assertEqual(self.store.request(rid)['attempts'],1)
        self.assertEqual(self.v.requests[-1]['proposal']['operation'],half.OPERATION)

    def test_lost_cancel_response_reconciles_exact_terminal_once(self):
        self.entry();self.mark='10.08';self.v.lose_reply=True
        self.assertEqual(self.cycle()['status'],'OUTCOME_UNKNOWN');count=self.v.sent
        self.cycle(False);self.assertIsNone(self.store.load(self.bucket)['pending'])
        self.assertEqual(self.v.sent,count)

    def test_unknown_cancel_not_retried_when_price_changes_or_after_restart(self):
        self.entry();self.mark='10.08'
        with patch.object(self.v,'send',side_effect=TimeoutError()):self.cycle()
        s=self.store.load(self.bucket);rid=s['pending'];count=self.v.sent
        self.c=m.Controller(DispatchStore(PostgresJournal.for_ci(CI)),self.v,fx.ROUTES2,after_exit_policy=m.AFTER_EXIT)
        for price in ('10','10.1','9.95'):
            self.mark=price;self.cycle()
        self.assertEqual(self.store.request(rid)['attempts'],1);self.assertEqual(self.v.sent,count)
        self.assertEqual(self.store.load(self.bucket)['pending'],rid)

    def test_lost_begin_commit_ack_never_reaches_cancel_sender(self):
        self.entry();self.mark='10.08';before=self.v.sent;real=self.store.begin
        def lost(*args,**kwargs):real(*args,**kwargs);raise JournalError('SIMULATED_COMMIT_ACK_LOSS')
        with patch.object(self.store,'begin',side_effect=lost):
            with self.assertRaises(JournalError):self.cycle()
        self.assertEqual(self.v.sent,before)
        rid=self.store.load(self.bucket)['pending'];self.assertEqual(self.store.request(rid)['phase'],'OUTCOME_UNKNOWN')

    def test_partial_fill_before_unsent_cancel_yields_to_protection_without_cancel(self):
        rid=self.prepared_cancel();self.v.fill('1000','40');self.cycle()
        self.assertEqual(self.store.request(rid)['phase'],'ABORTED_UNSENT')
        self.assertEqual(self.store.request(rid)['attempts'],0)
        self.assertEqual(self.v.orders['1000']['status'],'open')
        self.assertEqual(self.v.orders['1000']['order']['sz'],'60')
        self.assertEqual((self.v.requests[-1]['proposal']['leg'],self.v.requests[-1]['proposal']['quantity']),('STOP','40'))
        self.assertFalse(any(r['proposal']['action']['type']=='cancel' for r in self.v.requests))

    def test_partial_fill_during_cancel_is_retained_and_protected_not_hidden(self):
        self.entry();self.mark='10.08';self.v.cancel_race=lambda oid:self.v.fill(oid,'40')
        self.cycle();rid=self.store.load(self.bucket)['pending']
        self.cycle();self.cycle();self.cycle(False)
        s=self.store.load(self.bucket);view=life.review(s['bindings'],s['evidence']['snapshot'],now_ms=self.v.now())['cards'][0]
        self.assertEqual((view['entry_quantity'],view['stop_quantity_observed'],view['take_profit_quantity_observed']),('40','40','40'))
        self.assertEqual(self.store.request(rid)['terminal_state'],'CANCELED')
        self.assertFalse(self.store.request(rid)['cancellation_caused_terminal_state'])
        self.assertEqual(self.v.orders['1000']['order']['sz'],'60')

    def test_full_fill_wins_cancel_race_then_full_quantity_is_protected(self):
        self.entry();self.mark='10.08';self.v.cancel_race=lambda oid:self.v.fill(oid,'100')
        self.cycle();rid=self.store.load(self.bucket)['pending'];self.cycle();self.cycle();self.cycle(False)
        self.assertEqual(self.store.request(rid)['terminal_state'],'FILLED')
        self.assertEqual(self.v.requests[-1]['proposal']['quantity'],'100')
        self.assertEqual(self.v.orders['1000']['status'],'filled')

    def test_after_exit_cancel_remains_separate_from_unfilled_half_rule(self):
        self.entry();self.v.fill('1000','40');self.cycle();self.cycle();self.cycle(False)
        self.v.fill('1002','10');self.cycle()
        self.assertEqual(self.v.requests[-1]['proposal']['operation'],'CANCEL_ENTRY_AFTER_EXIT')
        self.assertEqual(self.v.orders['1000']['status'],'canceled')

    def test_concurrent_crossing_checkpoints_keep_one_durable_record(self):
        self.entry();self.mark='10.08';self.v.t+=1
        s=self.c.refresh(self.bucket);price=sample(self.mark,self.v.now())
        def record(_):
            try:half.checkpoint(self.store,s,fx.ROUTES2,price,now_ms=self.v.now());return True
            except JournalError:return False
        with ThreadPoolExecutor(max_workers=4) as pool:results=list(pool.map(record,range(4)))
        self.assertEqual(sum(results),1);self.assertEqual(len(self.store.load(self.bucket)[half.FIELD]),1)
        self.assertEqual(self.v.sent,1)


if __name__=='__main__':unittest.main()
