"""Offline recovery fixtures only. No real orders, keys, or application writes.

The PostgreSQL test can use only the existing loopback CI database factory.
Rehearsal JSON restore is NOT a claim of production dispatch durability.
"""
from copy import deepcopy
import inspect
import json
import os
import unittest
from unittest.mock import patch
from . import card_exit_recovery as r
from .test_card_lifecycle import binding, fill, order, terminal, snapshot, opened, closed, T, A, B


def controls(bindings, s, mark='101'):
    chosen=[b for b in bindings if b['account'].lower()==s['account'].lower() and b['symbol']==s['symbol']]
    return dict(symbol=s['symbol'],mark_price=mark,at_ms=s['at_ms'],cards={b['card_id']:
        dict(grouping='independent_fixed',requests={leg:dict(state='NONE',code=None) for leg in r.EXITS}) for b in chosen})


def plan(b,s,c=None,now=None):
    bs=b if isinstance(b,list) else [b]
    return r.plan(bs,s,c or controls(bs,s),now_ms=now or s['at_ms'])


def partial(b, quantity='40', stop=True, dormant=False):
    amount=str(100-int(quantity))
    opens=[order(b,'ENTRY',amount),order(b,'TAKE_PROFIT',quantity)]
    if stop:opens.append(order(b,'STOP',quantity))
    if dormant:
        for o in opens[1:]:o['quantity']='100';o['state']='WAITING_PARENT'
    s=snapshot(b,fills=[fill(b,qty=quantity)],opens=opens,position=('-' if b['side']=='SHORT' else '')+quantity)
    if not stop:b['orders']['STOP']=[]
    return s


def partly_closed(b):
    return snapshot(b,fills=[fill(b),fill(b,'TAKE_PROFIT',qty='40')],
        opens=[order(b,'STOP'),order(b,'TAKE_PROFIT',qty='60')],
        terms=[terminal(b,'ENTRY')],position=('-' if b['side']=='SHORT' else '')+'60')


def rejected_stop(b):
    s=opened(b)
    s['open_orders']=[o for o in s['open_orders'] if o['oid']!=b['orders']['STOP'][0]]
    t=terminal(b,'STOP','0');t['state']='REJECTED';s['terminal_orders'].append(t)
    return s


def merged(a, sa, b, sb, position):
    s=deepcopy(sa)
    for field in ('fills','open_orders','terminal_orders'):s[field]+=deepcopy(sb[field])
    s['position_quantity']=position
    return s


def reserve(p):
    m=r.Rehearsal(p['bucket'])
    request=m.reserve(p,expected_revision=0,now_ms=T)
    return m,request


def ack(oid):return dict(state='ACCEPTED_UNVERIFIED',code=None,oid=str(oid))


class NoNetworkCase(unittest.TestCase):
    def setUp(self):
        guard=patch('socket.socket',side_effect=AssertionError('NETWORK_FORBIDDEN'))
        guard.start();self.addCleanup(guard.stop)


class RecoveryPlannerTests(NoNetworkCase):
    def test_healthy_partial_entry_is_not_canceled(self):
        b=binding();s=partial(b);p=plan(b,s)
        self.assertEqual(p['state'],'NO_CORRECTION_NEEDED')
        self.assertTrue(p['preserve_entry_orders']);self.assertIsNone(p['next_step'])

    def test_missing_stop_targets_only_filled_quantity(self):
        b=binding();s=partial(b,stop=False);before=deepcopy((b,s));p=plan(b,s);step=p['next_step']
        self.assertEqual((step['operation'],step['leg'],step['target_quantity']),('CREATE_EXIT','STOP','40'))
        self.assertEqual(step['original_price'],'98');self.assertTrue(step['reduce_only'])
        self.assertEqual((b,s),before);self.assertEqual(p['urgent_card_ids'],[b['card_id']])

    def test_short_direction_uses_short_account_and_buy_exit(self):
        b=binding(side='SHORT');s=partial(b,stop=False);p=plan(b,s,controls([b],s,mark='99'))
        self.assertEqual(p['next_step']['side'],'B');self.assertEqual(p['next_step']['account'],B)
        self.assertEqual(p['next_step']['original_price'],'102')

    def test_increasing_partial_fill_recomputes_both_exit_sizes(self):
        b=binding();s=partial(b);s['fills'].append(fill(b,qty='10',fid='extra'))
        s['open_orders'][0]['quantity']='50';s['position_quantity']='50';p=plan(b,s)
        self.assertEqual(p['next_step']['operation'],'RESIZE_EXIT')
        self.assertEqual(p['next_step']['leg'],'STOP');self.assertEqual(p['next_step']['target_quantity'],'50')

    def test_partial_exit_shrinks_stop_not_original_entry(self):
        b=binding();p=plan(b,partly_closed(b))
        self.assertEqual(p['next_step']['leg'],'STOP');self.assertEqual(p['next_step']['target_quantity'],'60')
        self.assertEqual(p['desired_exits'][0]['planned_entry_quantity'],'100')

    def test_second_card_remains_untouched(self):
        a,b=binding(),binding(2,qty='60');sa,sb=partly_closed(a),opened(b)
        s=merged(a,sa,b,sb,'120');before=deepcopy((a,b,s));p=plan([a,b],s)
        self.assertEqual(p['next_step']['card_id'],a['card_id'])
        self.assertEqual(p['next_step']['order_id'],a['orders']['STOP'][0])
        self.assertEqual((a,b,s),before);self.assertEqual(p['desired_exits'][1]['remaining_quantity'],'60')

    def test_two_accounts_are_not_netted(self):
        a,b=binding(),binding(2,side='SHORT');s=partly_closed(b)
        p=plan([a,b],s,controls([a,b],s,mark='99'))
        self.assertEqual(len(p['desired_exits']),1);self.assertEqual(p['next_step']['card_id'],b['card_id'])

    def test_closed_card_orphan_is_exact_not_cancel_all(self):
        a,b=binding(),binding(2,qty='60');sa=closed(a)
        sa['terminal_orders']=[x for x in sa['terminal_orders'] if x['oid']!=a['orders']['STOP'][0]]
        sa['open_orders']=[order(a,'STOP')];s=merged(a,sa,b,opened(b),'60');p=plan([a,b],s)
        self.assertEqual(p['next_step']['operation'],'CANCEL_ORPHAN_EXIT')
        self.assertEqual(p['next_step']['order_id'],a['orders']['STOP'][0])
        self.assertEqual(p['next_step']['target_quantity'],'0')

    def test_fully_closed_card_needs_no_new_exit(self):
        b=binding();p=plan(b,closed(b))
        self.assertIsNone(p['next_step']);self.assertEqual(p['state'],'NO_CORRECTION_NEEDED')

    def test_no_fill_canceled_entry_does_not_create_trade(self):
        b=binding();s=snapshot(b,terms=[terminal(b,leg,'0') for leg in r.life.LEGS]);p=plan(b,s)
        self.assertEqual(p['desired_exits'][0]['remaining_quantity'],'0');self.assertIsNone(p['next_step'])

    def test_unfilled_pending_entry_does_not_lose_its_exits(self):
        b=binding();s=snapshot(b,opens=[order(b,leg) for leg in r.life.LEGS]);p=plan(b,s)
        self.assertIn('UNFILLED_ENTRY_WITH_WORKING_EXITS_REQUIRES_REVIEW',p['reasons'])
        self.assertIsNone(p['next_step'])

    def test_parent_linked_children_do_not_get_duplicate_stop(self):
        b=binding();s=partial(b,dormant=True);c=controls([b],s)
        c['cards'][b['card_id']]['grouping']='parent_linked';p=plan(b,s,c)
        self.assertIsNone(p['next_step']);self.assertIn('LINKED_EXIT_HANDOFF_REQUIRES_REVIEW',p['reasons'])
        self.assertEqual(p['urgent_card_ids'],[b['card_id']])

    def test_dormant_orders_cannot_be_overridden_by_independent_claim(self):
        b=binding();s=partial(b,dormant=True)
        self.assertIn('LINKED_EXIT_HANDOFF_REQUIRES_REVIEW',plan(b,s)['reasons'])

    def test_unknown_grouping_requires_review(self):
        b=binding();s=partly_closed(b);c=controls([b],s);c['cards'][b['card_id']]['grouping']='unknown'
        self.assertIsNone(plan(b,s,c)['next_step'])

    def test_rejected_stop_preserves_remainder_and_requires_recheck(self):
        b=binding();s=rejected_stop(b);c=controls([b],s)
        c['cards'][b['card_id']]['requests']['STOP']=dict(state='REJECTED',code='REDUCE_ONLY');p=plan(b,s,c)
        self.assertEqual(p['desired_exits'][0]['remaining_quantity'],'100')
        self.assertIn('REJECTED_EXIT_REQUIRES_RECHECK_REDUCE_ONLY',p['reasons']);self.assertIsNone(p['next_step'])

    def test_rejection_without_oid_uses_no_invented_order(self):
        b=binding();s=partial(b,stop=False);c=controls([b],s)
        c['cards'][b['card_id']]['requests']['STOP']=dict(state='REJECTED',code='MINIMUM_NOTIONAL');p=plan(b,s,c)
        self.assertIsNone(p['next_step']);self.assertEqual(b['orders']['STOP'],[])
        self.assertEqual(p['desired_exits'][0]['remaining_quantity'],'40')

    def test_known_rejection_causes_never_change_trade_terms(self):
        for code in set(r.ERRORS.values())|{'OTHER_REJECTION'}:
            with self.subTest(code=code):
                b=binding();s=rejected_stop(b);c=controls([b],s)
                c['cards'][b['card_id']]['requests']['STOP']=dict(state='REJECTED',code=code)
                p=plan(b,s,c);self.assertIsNone(p['next_step']);self.assertEqual(p['desired_exits'][0]['stop_price'],'98')

    def test_missing_reply_is_not_rejection_or_resend_permission(self):
        b=binding();s=partial(b,stop=False);c=controls([b],s)
        for state in ('OUTCOME_UNKNOWN','ACCEPTED_UNVERIFIED'):
            c['cards'][b['card_id']]['requests']['STOP']=dict(state=state,code=None)
            self.assertIn('PRIOR_REQUEST_RECONCILIATION_REQUIRED',plan(b,s,c)['reasons'])

    def test_crossed_stop_is_not_moved_to_make_order_valid(self):
        for side,mark in (('LONG','97'),('SHORT','103')):
            b=binding(side=side);s=partial(b,stop=False);p=plan(b,s,controls([b],s,mark=mark))
            self.assertIsNone(p['next_step']);self.assertIn('EXIT_LEVEL_REACHED_NO_AUTOMATIC_REPRICE',p['reasons'])

    def test_crossed_take_does_not_become_market_exit(self):
        b=binding();s=opened(b);b['orders']['TAKE_PROFIT']=[]
        s['open_orders']=[o for o in s['open_orders'] if o['oid']==b['orders']['STOP'][0]]
        p=plan(b,s,controls([b],s,mark='105'))
        self.assertIsNone(p['next_step']);self.assertIn('EXIT_LEVEL_REACHED_NO_AUTOMATIC_REPRICE',p['reasons'])

    def test_stale_and_future_observations_cannot_plan_corrections(self):
        b=binding();s=partly_closed(b)
        for now in (T+15001,T-1):self.assertIsNone(plan(b,s,now=now)['next_step'])

    def test_stale_mark_cannot_authorize_fresh_snapshot(self):
        b=binding();s=partly_closed(b);c=controls([b],s);c['at_ms']=T-15001
        self.assertIsNone(plan(b,s,c)['next_step'])

    def test_unknown_position_or_missing_history_blocks_whole_bucket(self):
        b=binding();s=partly_closed(b)
        for key,value in (('position_quantity','61'),('history_complete',False),('orders_complete',False)):
            self.assertIsNone(plan(b,{**s,key:value})['next_step'])

    def test_foreign_order_blocks_bucket_not_canceled_by_guess(self):
        b=binding();s=partly_closed(b);s['open_orders'].append(order(binding(8),'STOP'));p=plan(b,s)
        self.assertIsNone(p['next_step']);self.assertIn('UNASSIGNED_EXCHANGE_ACTIVITY',p['reasons'])

    def test_unresolved_order_blocks_replacement(self):
        b=binding();s=opened(b);s['open_orders']=[]
        self.assertIn('ORDER_STATUS_UNRESOLVED',plan(b,s)['reasons'])

    def test_rejected_order_with_fill_is_inconsistent(self):
        b=binding();s=closed(b);s['terminal_orders'][1]['state']='REJECTED'
        self.assertIn('INCONSISTENT_TERMINAL_FILL',plan(b,s)['reasons'])

    def test_post_terminal_fill_is_inconsistent(self):
        b=binding();s=closed(b);s['terminal_orders'][0]['at_ms']=T-10001
        self.assertIn('INCONSISTENT_TERMINAL_FILL',plan(b,s)['reasons'])

    def test_flat_card_with_waiting_entry_requires_policy_not_auto_cancel(self):
        b=binding();s=partial(b);s['fills'].append(fill(b,'TAKE_PROFIT',qty='40'));s['position_quantity']='0';p=plan(b,s)
        self.assertIn('FLAT_WITH_ENTRY_REMAINDER_POLICY_REQUIRED',p['reasons']);self.assertIsNone(p['next_step'])

    def test_duplicate_fills_and_reordered_input_have_same_basis(self):
        b=binding();s=partly_closed(b);p=plan(b,s);ss=deepcopy(s)
        ss['fills']*=3;ss['open_orders'].reverse()
        self.assertEqual(p['basis'],plan(b,ss)['basis']);self.assertEqual(p['next_step'],plan(b,ss)['next_step'])

    def test_heartbeat_does_not_create_new_intent(self):
        b=binding();s=partly_closed(b);p=plan(b,s);s['at_ms']+=1;other=plan(b,s)
        self.assertEqual(p['basis'],other['basis']);self.assertEqual(p['next_step']['intent_id'],other['next_step']['intent_id'])

    def test_changed_fill_invalidates_old_basis(self):
        b=binding();s=partly_closed(b);p=plan(b,s)
        s['fills'][1]['quantity']='41';s['open_orders'][1]['quantity']='59';s['position_quantity']='59';q=plan(b,s)
        self.assertNotEqual(p['basis'],q['basis']);self.assertEqual(q['next_step']['target_quantity'],'59')

    def test_missing_card_control_is_not_assumed_safe(self):
        b=binding();s=opened(b);c=controls([b],s);c['cards']={}
        with self.assertRaises(r.RecoveryError):plan(b,s,c)

    def test_wrong_mark_symbol_is_rejected(self):
        b=binding();s=opened(b);c=controls([b],s);c['symbol']='BTC'
        with self.assertRaises(r.RecoveryError):plan(b,s,c)

    def test_all_proposals_are_explicitly_offline(self):
        b=binding();p=plan(b,partly_closed(b))
        self.assertFalse(p['dispatch_enabled']);self.assertFalse(p['live_recovery_ready'])
        self.assertEqual(p['order_requests_sent'],0);self.assertTrue(p['simulation_only'])
        for key in ('nonce','signature','action','orders'):self.assertNotIn(key,p)

    def test_no_network_signer_runtime_or_environment_imports(self):
        text=inspect.getsource(r)
        for forbidden in ('import os','import requests','import http','import socket','sign_l1_action','submit_persisted','HL_TESTNET_AGENT_KEY','/exchange'):
            self.assertNotIn(forbidden,text)


class ReplyTests(NoNetworkCase):
    def test_mixed_batch_preserves_each_leg_outcome(self):
        raw={'status':'ok','response':{'type':'order','data':{'statuses':[
            {'filled':{'oid':1}},{'resting':{'oid':2}},{'error':'Invalid TP/SL price.'}]}}}
        out=r.classify_reply(raw,list(r.life.LEGS))
        self.assertEqual(out['ENTRY']['state'],'ACCEPTED_UNVERIFIED')
        self.assertEqual(out['TAKE_PROFIT']['oid'],'2');self.assertEqual(out['STOP']['code'],'INVALID_TRIGGER')

    def test_whole_batch_rejection(self):
        out=r.classify_reply({'status':'err','response':'Insufficient margin to place order.'},list(r.life.LEGS))
        self.assertTrue(all(x['state']=='REJECTED' for x in out.values()))

    def test_single_error_vector_applies_to_whole_batch(self):
        raw={'status':'ok','response':{'type':'order','data':{'statuses':[{'error':'Invalid TP/SL price.'}]}}}
        out=r.classify_reply(raw,list(r.life.LEGS))
        self.assertTrue(all(x['state']=='REJECTED' and x['code']=='INVALID_TRIGGER' for x in out.values()))

    def test_unknown_or_malformed_reply_is_not_resend_permission(self):
        for raw in (None,{},'timeout',{'status':'ok'},{'status':'err','response':{}}):
            self.assertEqual(r.classify_reply(raw,['STOP'])['STOP']['state'],'OUTCOME_UNKNOWN')

    def test_short_success_vector_is_not_assigned_to_all_legs(self):
        raw={'status':'ok','response':{'type':'order','data':{'statuses':[{'resting':{'oid':1}}]}}}
        self.assertTrue(all(x['state']=='OUTCOME_UNKNOWN' for x in r.classify_reply(raw,list(r.life.LEGS)).values()))

    def test_invalid_order_ids_are_unknown(self):
        for oid in (0,-1,True,'1',2**64):
            raw={'status':'ok','response':{'type':'order','data':{'statuses':[{'resting':{'oid':oid}}]}}}
            self.assertEqual(r.classify_reply(raw,['STOP'])['STOP']['state'],'OUTCOME_UNKNOWN')

    def test_reused_oid_for_different_legs_is_unknown(self):
        raw={'status':'ok','response':{'type':'order','data':{'statuses':[{'resting':{'oid':1}},{'resting':{'oid':1}}]}}}
        self.assertTrue(all(x['state']=='OUTCOME_UNKNOWN' for x in r.classify_reply(raw,list(r.EXITS)).values()))

    def test_remote_message_never_echoed(self):
        result=r.classify_reply({'status':'err','response':'private material or arbitrary text'},['STOP'])
        self.assertNotIn('private',json.dumps(result));self.assertEqual(result['STOP']['code'],'OTHER_REJECTION')


class RehearsalTests(NoNetworkCase):
    def test_timeout_stays_locked_across_json_restore(self):
        b=binding();p=plan(b,partly_closed(b));m,key=reserve(p)
        m.reply(key,dict(state='OUTCOME_UNKNOWN',code=None,oid=None))
        restored=r.Rehearsal(p['bucket'],json.loads(json.dumps(m.export())))
        with self.assertRaises(r.RecoveryError):restored.reserve(p,expected_revision=restored.state['revision'],now_ms=T)
        self.assertEqual(restored.state['attempts'],1)

    def test_lost_reply_after_reservation_cannot_resend(self):
        b=binding();p=plan(b,partly_closed(b));m,key=reserve(p);restored=r.Rehearsal(p['bucket'],m.export())
        with self.assertRaises(r.RecoveryError):restored.reserve(p,expected_revision=1,now_ms=T)

    def test_ack_alone_never_confirms_protection(self):
        b=binding();s=partly_closed(b);p=plan(b,s);m,key=reserve(p);m.reply(key,ack(b['orders']['STOP'][0]))
        self.assertEqual(m.state['status'],'WAITING_FOR_EVIDENCE');s['at_ms']=T+1
        with self.assertRaises(r.RecoveryError):m.confirm([b],s,controls([b],s),now_ms=T+1)

    def test_partial_close_repair_observed_then_unlocks(self):
        b=binding();s=partly_closed(b);p=plan(b,s);m,key=reserve(p);m.reply(key,ack(b['orders']['STOP'][0]))
        s['open_orders'][0]['quantity']='60';s['at_ms']=T+1
        current=m.confirm([b],s,controls([b],s),now_ms=T+1)
        self.assertEqual(m.state['status'],'IDLE');self.assertEqual(current['state'],'NO_CORRECTION_NEEDED')
        self.assertFalse(current['recovered_card_needs_more_work'])

    def test_create_stop_requires_new_oid_bound_to_own_card(self):
        b=binding();s=partial(b,stop=False);p=plan(b,s);m,key=reserve(p);m.reply(key,ack(999));s['at_ms']=T+1
        with self.assertRaises(r.RecoveryError):m.confirm([b],s,controls([b],s),now_ms=T+1)
        b['orders']['STOP']=['999'];s['open_orders'].append(order(b,'STOP','40'))
        m.confirm([b],s,controls([b],s),now_ms=T+1);self.assertEqual(m.state['status'],'IDLE')

    def test_late_additional_fill_replans_without_duplicate_creation(self):
        b=binding();s=partial(b,stop=False);p=plan(b,s);m,key=reserve(p);m.reply(key,ack(999))
        b['orders']['STOP']=['999'];s['open_orders'].append(order(b,'STOP','40'))
        s['fills'].append(fill(b,qty='10',fid='late'));s['position_quantity']='50'
        s['open_orders'][0]['quantity']='50';s['at_ms']=T+1
        current=m.confirm([b],s,controls([b],s),now_ms=T+1)
        self.assertTrue(current['recovered_card_needs_more_work']);self.assertEqual(m.state['status'],'IDLE')
        self.assertEqual(current['next_step']['operation'],'RESIZE_EXIT')
        self.assertEqual(current['next_step']['order_id'],'999')
        self.assertEqual(current['next_step']['target_quantity'],'50')
        m.reserve(current,expected_revision=m.state['revision'],now_ms=T+1)
        self.assertEqual(m.state['attempts'],2)

    def test_rejected_request_cannot_retry_in_an_endless_loop(self):
        b=binding();p=plan(b,partly_closed(b));m,key=reserve(p)
        m.reply(key,dict(state='REJECTED',code='NO_LIQUIDITY',oid=None))
        for _ in range(20):
            with self.assertRaises(r.RecoveryError):m.reserve(p,expected_revision=m.state['revision'],now_ms=T)
        self.assertEqual(m.state['attempts'],1)

    def test_reply_retry_is_idempotent_but_conflict_blocks(self):
        b=binding();p=plan(b,partly_closed(b));m,key=reserve(p);value=ack(12)
        self.assertTrue(m.reply(key,value));rev=m.state['revision']
        self.assertFalse(m.reply(key,value));self.assertEqual(m.state['revision'],rev)
        with self.assertRaises(r.RecoveryError):m.reply(key,ack(99))
        self.assertEqual(m.state['status'],'CONFLICT_RECONCILIATION_REQUIRED')

    def test_reply_for_another_request_is_rejected(self):
        b=binding();p=plan(b,partly_closed(b));m,key=reserve(p)
        with self.assertRaises(r.RecoveryError):m.reply('other',ack(12))

    def test_stale_worker_revision_rejected(self):
        b=binding();p=plan(b,partly_closed(b));m,key=reserve(p)
        with self.assertRaises(r.RecoveryError):m.reserve(p,expected_revision=0,now_ms=T)

    def test_expired_proposal_not_reserved(self):
        b=binding();p=plan(b,partly_closed(b));m=r.Rehearsal(p['bucket'])
        with self.assertRaises(r.RecoveryError):m.reserve(p,expected_revision=0,now_ms=T+15001)

    def test_old_mark_expires_before_snapshot(self):
        b=binding();s=partly_closed(b);c=controls([b],s);c['at_ms']=T-14000;p=plan(b,s,c)
        m=r.Rehearsal(p['bucket'])
        with self.assertRaises(r.RecoveryError):m.reserve(p,expected_revision=0,now_ms=T+1001)

    def test_one_operation_per_bucket_even_for_another_card(self):
        a,b=binding(),binding(2);s=merged(a,partly_closed(a),b,partly_closed(b),'120');p=plan([a,b],s);m,key=reserve(p)
        with self.assertRaises(r.RecoveryError):m.reserve(p,expected_revision=m.state['revision'],now_ms=T)

    def test_json_export_cannot_mutate_running_state(self):
        b=binding();p=plan(b,partly_closed(b));m,key=reserve(p);copy=m.export()
        copy['pending']['step']['target_quantity']='1000';self.assertEqual(m.state['pending']['step']['target_quantity'],'60')

    def test_wrong_bucket_cannot_restore(self):
        b=binding();p=plan(b,partly_closed(b));m,key=reserve(p)
        with self.assertRaises(r.RecoveryError):r.Rehearsal('a'*64,m.export())

    def test_simulation_cannot_be_marked_live(self):
        b=binding();p=plan(b,partly_closed(b));p['dispatch_enabled']=True
        with self.assertRaises(r.RecoveryError):reserve(p)

    def test_changed_proposal_cannot_be_reserved(self):
        b=binding();p=plan(b,partly_closed(b));p['next_step']['target_quantity']='999'
        with self.assertRaises(r.RecoveryError):reserve(p)

    def test_changed_original_prices_are_not_confirmed(self):
        b=binding();s=partly_closed(b);p=plan(b,s);m,key=reserve(p);m.reply(key,ack(12))
        b['prices']['stop']='97';s['open_orders'][0].update(quantity='60',price='97',trigger_price='97');s['at_ms']=T+1
        with self.assertRaises(r.RecoveryError):m.confirm([b],s,controls([b],s),now_ms=T+1)

    def test_invalid_restored_state_cannot_claim_idle_with_pending(self):
        b=binding();p=plan(b,partly_closed(b));m,key=reserve(p);state=m.export();state['status']='IDLE'
        with self.assertRaises(r.RecoveryError):r.Rehearsal(p['bucket'],state)

    def test_confirm_requires_new_evidence_not_same_timestamp(self):
        b=binding();s=partly_closed(b);p=plan(b,s);m,key=reserve(p);m.reply(key,ack(12))
        s['open_orders'][0]['quantity']='60'
        with self.assertRaises(r.RecoveryError):m.confirm([b],s,controls([b],s),now_ms=T)

    def test_cancel_orphan_confirms_only_its_terminal_order(self):
        b=binding();s=closed(b);s['terminal_orders']=[t for t in s['terminal_orders'] if t['oid']!='12']
        s['open_orders']=[order(b,'STOP')];p=plan(b,s);m,key=reserve(p);m.reply(key,ack(12))
        s=closed(b);s['at_ms']=T+1
        current=m.confirm([b],s,controls([b],s),now_ms=T+1)
        self.assertEqual(current['state'],'NO_CORRECTION_NEEDED')


@unittest.skipUnless(os.environ.get('HL_JOURNAL_CI_URL'),'Requires disposable localhost PostgreSQL')
class RecoveryLifecyclePersistenceTests(unittest.TestCase):
    def test_partial_exit_rehearsal_preserves_other_card_in_existing_store(self):
        from .postgres_journal import PostgresJournal
        from .card_lifecycle_store import LifecycleStore
        journal=PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL'])
        journal.bootstrap();store=LifecycleStore(journal);store.initialize()
        account='0x'+'8'*40
        a,b=binding(80,account=account),binding(81,account=account,qty='60')
        sa,sb=partly_closed(a),opened(b);s=merged(a,sa,b,sb,'120')
        initial=store.save([a,b],s,expected_revision=0,now_ms=T)
        p=plan([a,b],s);self.assertEqual(p['next_step']['card_id'],a['card_id'])
        before=deepcopy(b);s['open_orders'][0]['quantity']='60';s['at_ms']=T+1
        updated=store.save([a,b],s,expected_revision=initial['revision'],now_ms=T+1)
        reread=LifecycleStore(journal).load(account,'DOGE',now_ms=T+1)
        self.assertEqual(updated['revision'],2);self.assertEqual(reread['revision'],2)
        self.assertEqual(b,before);self.assertFalse(reread['report']['needs_review'])
        self.assertEqual(reread['report']['cards'][1]['remaining_quantity'],'60')
        replay=store.save([a,b],s,expected_revision=2,now_ms=T+1);self.assertTrue(replay['duplicate'])


if __name__=='__main__':unittest.main(verbosity=2)
