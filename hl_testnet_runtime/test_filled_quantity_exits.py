"""New policy tests. Synthetic records only; exchange and signing are blocked."""
from copy import deepcopy
from datetime import datetime, timezone
import os
import unittest
from unittest.mock import patch
from . import filled_quantity_exits as m, card_lifecycle as life
from . import card_exit_recovery as recovery, card_recovery_journal as durable
from . import trade_cards, card_display_projection as display, persistent_execution
from .postgres_journal import PostgresJournal
from .test_card_lifecycle import fill, order, terminal, snapshot, A, B, T
from .test_card_exit_recovery import controls, ack

CI = os.environ.get('HL_JOURNAL_CI_URL')
META = {'universe':[{'name':'DOGE','szDecimals':2}]}
ROUTES = {'long_account':{'account':A},'short_account':{'account':B}}


def original(n=1, side='LONG'):
    source=dict(kind='SIGNAL',event_id='filled-policy-'+str(n),symbol='DOGE',side=side,
        entry='10',stop='9.9' if side=='LONG' else '10.1',
        take_profit='10.2' if side=='LONG' else '9.8',
        at=datetime.fromtimestamp((T-20000)/1000,timezone.utc).isoformat())
    card=trade_cards.prepare_card(source,META,rule_id='SOFTWARE_TEST',threshold_pct='1.5',record_kind='synthetic_test')
    account=ROUTES[card['account_role']]['account']
    draft=m.prepare_entry(card,META,account,ROUTES)
    b=life.binding_from_card(card,account,ROUTES,dict(ENTRY=[str(n*10)],STOP=[],TAKE_PROFIT=[]))
    return b,dict(card=card,draft=draft)


def case(q='40',*,side='LONG',n=1,stop=None,take=None):
    b,record=original(n,side)
    fs=[] if q=='0' else [fill(b,qty=q)]
    terms=[]; opens=[]
    if q=='100':terms.append(terminal(b,'ENTRY','100'))
    else:opens.append(order(b,'ENTRY',str(100-int(q))))
    for leg,size,offset in (('STOP',stop,1),('TAKE_PROFIT',take,2)):
        if size is not None:
            b['orders'][leg]=[str(n*10+offset)]
            opens.append(order(b,leg,size))
    s=snapshot(b,fills=fs,opens=opens,terms=terms,position=('-' if side=='SHORT' and q!='0' else '')+q)
    c=controls([b],s,mark='10')
    return [b],s,c,{b['card_id']:record}


def assess(ev,now=T):
    bs,s,c,originals=ev
    return m.assess(bs,s,c,originals=originals,routes=ROUTES,now_ms=now)


class NoOrders(unittest.TestCase):
    def setUp(self):
        for target in ('http.client.HTTPSConnection','socket.create_connection',
                       'hyperliquid_testnet_executor._wallet','hyperliquid_testnet_executor._signed_body'):
            guard=patch(target,side_effect=AssertionError('NO_EXCHANGE_OR_SIGNATURE'))
            guard.start();self.addCleanup(guard.stop)


class FilledQuantityTests(NoOrders):
    def test_selected_policy_does_not_authorize_trading_or_partial_cancel(self):
        selected=m.selection()
        self.assertTrue(selected['design_selected'])
        self.assertTrue(selected['preserve_unfilled_entry_on_partial_fill'])
        self.assertFalse(selected['execution_authorized'])
        self.assertFalse(selected['partial_cancel_policy_selected'])
        self.assertFalse(selected['close_entire_account_position'])

    def test_draft_contains_one_independent_entry_not_dormant_children(self):
        for side in ('LONG','SHORT'):
            b,record=original(side=side);d=record['draft'];action=d['entry_action']
            self.assertEqual(action['grouping'],'na');self.assertEqual(len(action['orders']),1)
            self.assertEqual(action['orders'][0]['s'],'100')
            self.assertFalse(action['orders'][0]['r'])
            self.assertEqual(action['orders'][0]['t'],{'limit':{'tif':'Gtc'}})
            self.assertTrue(d['unsigned_draft_only']);self.assertFalse(d['dispatch_enabled'])
            self.assertEqual(d['source_at'],record['card']['prepared']['execution']['at'])
            self.assertEqual(record['card']['risk']['planned_usd'],'10')

    def test_new_draft_cannot_reuse_native_group_identifier(self):
        import hyperliquid_testnet_executor as old
        b,r=original();native=old.build_action(r['card']['prepared']['execution'],META,A,exit_type='tp_limit_sl_market')
        self.assertNotEqual(native['orders'][0]['c'],r['draft']['entry_action']['orders'][0]['c'])
        self.assertEqual(native['grouping'],'normalTpsl')
        self.assertEqual(len(native['orders']),3)

    def test_replay_stable_and_distinct_alerts_not_deduplicated(self):
        b,r=original();other,rr=original(2)
        self.assertEqual(r['draft'],m.prepare_entry(r['card'],META,A,ROUTES))
        self.assertNotEqual(r['draft']['entry_action']['orders'][0]['c'],rr['draft']['entry_action']['orders'][0]['c'])

    def test_altered_plan_and_extra_app_fields_rejected(self):
        b,r=original()
        for key,value in (('policy','close-all'),('dispatch_enabled',True),('source_at','now'),
                          ('prices',dict(entry='10',stop='9',take_profit='12')),('command','buy')):
            d={**r['draft'],key:value}
            with self.assertRaises(life.LifecycleError):m.validate_draft(r['card'],d,ROUTES)
        d=deepcopy(r['draft']);d['entry_action']['orders'][0]['s']='500'
        with self.assertRaises(life.LifecycleError):m.validate_draft(r['card'],d,ROUTES)

    def test_historical_review_cannot_become_new_entry(self):
        b,r=original();c=r['card']
        old=trade_cards.prepare_card(c['prepared']['source'],META,rule_id=c['rule']['id'],
            threshold_pct=c['rule']['threshold_pct'],record_kind='historical_review')
        with self.assertRaises(life.LifecycleError):m.prepare_entry(old,META,A,ROUTES)

    def test_wrong_account_cannot_receive_draft(self):
        b,r=original()
        with self.assertRaises(life.LifecycleError):m.prepare_entry(r['card'],META,B,ROUTES)

    def test_oversized_source_still_hits_existing_lab_cap(self):
        import hyperliquid_testnet_executor as old
        b,r=original();src={**r['card']['prepared']['source'],entry='100',stop='99.9',take_profit='100.2'}
        card=trade_cards.prepare_card(src,META,rule_id='SOFTWARE_TEST',threshold_pct='1.5',record_kind='synthetic_test')
        with self.assertRaisesRegex(old.TestnetError,'OUTSIDE_LAB_SIZE_BOUNDS'):
            m.prepare_entry(card,META,A,ROUTES)

    def test_zero_fill_places_no_exit(self):
        out=assess(case('0'))
        self.assertEqual(out['state'],'NO_CORRECTION_NEEDED');self.assertIsNone(out['next_step'])

    def test_40_of_100_protects_40_and_preserves_60_waiting(self):
        ev=case();before=deepcopy(ev);out=assess(ev)
        self.assertEqual(out['next_step']['leg'],'STOP')
        self.assertEqual(out['next_step']['target_quantity'],'40')
        self.assertEqual(out['next_step']['operation'],'CREATE_EXIT')
        self.assertTrue(out['preserve_entry_orders'])
        self.assertEqual(ev,before);self.assertFalse(out['execution_authorized'])
        self.assertFalse(out['entry_waiting_for_full_fill'])

    def test_after_observed_stop_take_is_also_only_40(self):
        out=assess(case(stop='40'))
        self.assertEqual(out['next_step']['leg'],'TAKE_PROFIT')
        self.assertEqual(out['next_step']['target_quantity'],'40')

    def test_partial_with_two_correct_exits_needs_no_replacement(self):
        out=assess(case(stop='40',take='40'))
        self.assertEqual(out['state'],'NO_CORRECTION_NEEDED')
        self.assertFalse(out['native_per_card_isolation'])
        self.assertFalse(out['gap_free_protection_guaranteed'])

    def test_further_fill_resizes_same_card_without_new_pair(self):
        out=assess(case('60',stop='40',take='40'))
        self.assertEqual(out['next_step']['operation'],'RESIZE_EXIT')
        self.assertEqual(out['next_step']['target_quantity'],'60')
        self.assertEqual(out['next_step']['order_id'],'11')
        self.assertEqual(out['next_step']['original_price'],'9.9')

    def test_short_uses_its_own_account_and_buy_to_reduce(self):
        ev=case(side='SHORT');out=assess(ev)
        self.assertEqual(out['next_step']['account'],B)
        self.assertEqual(out['next_step']['side'],'B')
        self.assertTrue(out['next_step']['reduce_only'])
        self.assertEqual(out['next_step']['target_quantity'],'40')

    def test_same_coin_two_cards_never_use_combined_140_quantity(self):
        one=case();two=case('100',n=2,stop='100',take='100')
        bs=one[0]+two[0];s=deepcopy(one[1])
        for key in ('fills','open_orders','terminal_orders'):s[key]+=two[1][key]
        s['position_quantity']='140'
        ev=bs,s,controls(bs,s,mark='10'),{**one[3],**two[3]}
        before=deepcopy(ev);out=assess(ev)
        self.assertEqual(out['next_step']['card_id'],one[0][0]['card_id'])
        self.assertEqual(out['next_step']['target_quantity'],'40');self.assertEqual(ev,before)

    def test_duplicate_fills_not_counted_twice(self):
        ev=case();ev[1]['fills']*=2
        self.assertEqual(assess(ev)['next_step']['target_quantity'],'40')

    def test_old_native_children_not_relabelled_independent(self):
        ev=case(stop='100',take='100')
        for o in ev[1]['open_orders']:
            if o['reduce_only']:o['state']='WAITING_PARENT'
        with self.assertRaisesRegex(life.LifecycleError,'LEGACY_CHILDREN'):assess(ev)
        ev=case();ev[2]['cards'][ev[0][0]['card_id']]['grouping']='parent_linked'
        with self.assertRaisesRegex(life.LifecycleError,'ORIGINAL_INDEPENDENT'):assess(ev)

    def test_pending_unknown_or_rejection_cannot_become_repeat_permission(self):
        for state in ('OUTCOME_UNKNOWN','ACCEPTED_UNVERIFIED','REJECTED'):
            ev=case();ev[2]['cards'][ev[0][0]['card_id']]['requests']['STOP']=dict(
                state=state,code='REDUCE_ONLY' if state=='REJECTED' else None)
            self.assertIsNone(assess(ev)['next_step'])

    def test_stale_or_incomplete_evidence_cannot_prepare_exit(self):
        self.assertIsNone(assess(case(),now=T+16000)['next_step'])
        for key in ('history_complete','orders_complete'):
            ev=case();ev[1][key]=False
            self.assertIsNone(assess(ev)['next_step'])

    def test_crossed_price_does_not_silently_reprice(self):
        ev=case();ev[2]['mark_price']='9.8'
        out=assess(ev);self.assertIsNone(out['next_step'])
        self.assertIn('EXIT_LEVEL_REACHED_NO_AUTOMATIC_REPRICE',out['reasons'])

    def test_exit_started_while_entry_still_waits_is_explicitly_not_decided(self):
        bs,s,c,records=case(take='40');b=bs[0]
        s['fills'].append(fill(b,'TAKE_PROFIT',qty='10',fid='close10'))
        for o in s['open_orders']:
            if o['oid'] in b['orders']['TAKE_PROFIT']:o['quantity']='30'
        s['position_quantity']='30'
        out=assess((bs,s,c,records));self.assertIsNone(out['next_step'])
        self.assertIn('ENTRY_REMAINDER_AFTER_EXIT_POLICY_REQUIRED',out['reasons'])
        self.assertEqual(s['open_orders'][0]['quantity'],'60')

    def test_closed_entry_partial_exit_keeps_only_remaining_60(self):
        bs,s,c,records=case('100',stop='100',take='100');b=bs[0]
        s['fills'].append(fill(b,'TAKE_PROFIT',qty='40',fid='exit40'))
        s['position_quantity']='60'
        for o in s['open_orders']:
            if o['oid'] in b['orders']['TAKE_PROFIT']:o['quantity']='60'
        out=assess((bs,s,c,records))
        self.assertEqual(out['next_step']['target_quantity'],'60')
        self.assertEqual(out['next_step']['order_id'],'11')

    def test_app_copy_cannot_supply_original_or_change_source(self):
        ev=case(stop='40',take='40');before=deepcopy(ev)
        copy=display.project(ev[0],ev[1],revision=1,now_ms=T)
        copy['cards'][0]['prices']['stop']='1'
        with self.assertRaises(life.LifecycleError):
            m.assess(ev[0],ev[1],ev[2],originals=copy,routes=ROUTES,now_ms=T)
        self.assertEqual(ev,before)

    def test_existing_runtime_send_guard_stays_locked_for_new_draft(self):
        ev=case();d=next(iter(ev[3].values()))['draft']
        with patch.dict(os.environ,{'HL_TESTNET_SAFETY_PIPELINE':'integrated_readonly_v1'},clear=True):
            out=persistent_execution.submit_persisted(d,account=A,agent=B,enable_testnet=True)
        self.assertEqual(out['status'],'INTEGRATED_SAFETY_NO_SEND')
        self.assertEqual(out['order_requests_sent'],0)


@unittest.skipUnless(CI,'Disposable loopback PostgreSQL required')
class DurableFilledQuantityTests(NoOrders):
    def setUp(self):
        super().setUp();self.j=PostgresJournal.for_ci(CI)
        with self.j._transaction() as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS {durable.SCHEMA} CASCADE')
            conn.execute('DROP SCHEMA IF EXISTS hl_testnet_execution_v1 CASCADE')
        self.j.bootstrap();self.store=durable.RecoveryJournal(self.j);self.store.initialize()

    def test_partial_entry_through_existing_durable_stop_and_take_chain(self):
        ev=case();bs,s,c,records=ev
        self.assertEqual(assess(ev)['next_step']['target_quantity'],'40')
        revision=0
        for i,leg in enumerate(('STOP','TAKE_PROFIT')):
            start=T+i*10;s['at_ms']=c['at_ms']=start;oid=str(501+i)
            p=self.store.prepare(bs,s,c,event_id='prepare-'+leg,expected_revision=revision,now_ms=start)
            rid=p['request']['request_id']
            p=self.store.begin_simulated_attempt(rid,bs,s,c,event_id='begin-'+leg,expected_revision=p['revision'],now_ms=start+1)
            p=self.store.record_reply(rid,ack(oid),event_id='reply-'+leg,expected_revision=p['revision'],now_ms=start+2)
            bs[0]['orders'][leg].append(oid)
            s['open_orders'].append(order(bs[0],leg,'40'))
            s['at_ms']=c['at_ms']=start+3
            p=self.store.confirm_observed(rid,bs,s,c,event_id='observed-'+leg,expected_revision=p['revision'],now_ms=start+3)
            revision=p['revision'];self.assertEqual(p['request']['phase'],'OBSERVED')
        self.assertEqual(assess(ev,now=T+13)['state'],'NO_CORRECTION_NEEDED')
        self.assertEqual(s['open_orders'][0]['quantity'],'60')
        loaded=durable.RecoveryJournal(PostgresJournal.for_ci(CI)).load(rid)
        self.assertEqual(loaded['request']['phase'],'OBSERVED')
        self.assertFalse(loaded['dispatch_enabled'])

    def test_restart_unknown_does_not_create_second_attempt(self):
        bs,s,c,records=case()
        p=self.store.prepare(bs,s,c,event_id='p',expected_revision=0,now_ms=T)
        rid=p['request']['request_id']
        p=self.store.begin_simulated_attempt(rid,bs,s,c,event_id='b',expected_revision=p['revision'],now_ms=T+1)
        other=durable.RecoveryJournal(PostgresJournal.for_ci(CI))
        with self.assertRaises(durable.RecoveryStorageError):
            other.begin_simulated_attempt(rid,bs,s,c,event_id='again',expected_revision=p['revision'],now_ms=T+2)
        self.assertEqual(other.load(rid)['request']['simulated_attempt_count'],1)
