"""Offline parent/child race assessment. No venue operations."""
from copy import deepcopy
import inspect
import unittest
from unittest.mock import patch
from . import parent_exit_handoff as h, card_exit_recovery as r
from .test_card_lifecycle import binding, fill, order, terminal, T
from .test_card_exit_recovery import controls, partial, merged


def fixture(side='LONG'):
    b = binding(side=side)
    s = partial(b, dormant=True)
    c = controls([b], s, mark='101' if side=='LONG' else '99')
    c['cards'][b['card_id']]['grouping'] = 'parent_linked'
    link = dict(card_id=b['card_id'], card_digest=b['card_digest'], grouping='normalTpsl',
        parent_oid=b['orders']['ENTRY'][0], children={leg:b['orders'][leg][0] for leg in r.EXITS})
    return [b], s, c, link


def ended(ev, *, full=False, late=False, waiting=False, margin=False, at=T+5):
    bs,s,c,link = deepcopy(ev); b=bs[0]
    q = '100' if full else '50' if late else '40'
    if full or late:
        s['fills'].append({**fill(b,qty='60' if full else '10',fid='late'), 'at_ms':T+2})
    s['position_quantity'] = ('-' if b['side']=='SHORT' else '') + q
    entry = {**terminal(b,'ENTRY',q), 'state':'FILLED' if full else 'CANCELED', 'at_ms':T+3}
    s['terminal_orders'] = [entry]
    if full or margin:
        s['open_orders'] = [order(b,'STOP'),order(b,'TAKE_PROFIT')]
    elif waiting:
        s['open_orders'] = [o for o in s['open_orders'] if o['oid']!=link['parent_oid']]
    else:
        s['open_orders'] = []
        s['terminal_orders'] += [{**terminal(b,leg,'0'),'at_ms':T+4} for leg in r.EXITS]
    s['at_ms']=at; c['at_ms']=at
    return bs,s,c,link


def check(ev, *, policy=h.PRESERVE, now=None):
    return h.assess(*ev,policy=policy,now_ms=ev[1]['at_ms'] if now is None else now)


class ParentHandoffTests(unittest.TestCase):
    def setUp(self):
        guard=patch('socket.socket',side_effect=AssertionError('NETWORK_FORBIDDEN'))
        guard.start();self.addCleanup(guard.stop)

    def test_default_preserves_partial_entry_and_never_claims_protection(self):
        ev=fixture();before=deepcopy(ev);p=check(ev)
        self.assertEqual(p['state'],'POLICY_APPROVAL_REQUIRED');self.assertIsNone(p['next_step'])
        self.assertTrue(p['parent_remainder_preserved']);self.assertFalse(p['stop_verified'])
        self.assertEqual(p['unprotected_quantity'],'40');self.assertEqual(ev,before)

    def test_explicit_rehearsal_targets_only_exact_parent(self):
        ev=fixture();p=check(ev,policy=h.REHEARSE);step=p['next_step']
        self.assertEqual(step['operation'],'CANCEL_PARTIAL_PARENT_OFFLINE')
        self.assertEqual(step['order_id'],ev[3]['parent_oid']);self.assertEqual(step['card_id'],ev[0][0]['card_id'])
        self.assertEqual(step['original_prices'],ev[0][0]['prices'])
        self.assertFalse(p['policy_approved_for_trading']);self.assertFalse(p['dispatch_enabled'])
        self.assertFalse(p['gap_free_guaranteed']);self.assertEqual(p['order_requests_sent'],0)

    def test_short_account_not_netted_or_redirected(self):
        ev=fixture('SHORT');p=check(ev,policy=h.REHEARSE)
        self.assertEqual(p['next_step']['role'],'short_account')
        self.assertEqual(p['next_step']['account'],ev[0][0]['account'])
        self.assertEqual(p['remaining_quantity'],'40')

    def test_unfilled_entry_waits_without_cancellation(self):
        bs,s,c,l=fixture();s['fills']=[];s['position_quantity']='0';s['open_orders'][0]['quantity']='100'
        for policy in (h.PRESERVE,h.REHEARSE):
            p=check((bs,s,c,l),policy=policy);self.assertEqual(p['state'],'WAITING_ENTRY');self.assertIsNone(p['next_step'])

    def test_canceled_parent_is_not_enough_while_children_are_dormant(self):
        p=check(ended(fixture(),waiting=True),policy=h.REHEARSE)
        self.assertEqual(p['state'],'WAITING_CHILD_FINALITY');self.assertIsNone(p['next_step'])
        self.assertIsNone(p['independent_context']);self.assertFalse(p['stop_verified'])

    def test_originals_retired_delegate_fixed_size_stop_before_take(self):
        ev=ended(fixture());before=deepcopy(ev);p=check(ev)
        self.assertEqual(p['state'],'ORIGINAL_GROUP_RETIRED');self.assertIsNone(p['next_step'])
        step=p['recovery_proposal']['next_step']
        self.assertEqual((step['leg'],step['target_quantity']),('STOP','40'))
        self.assertEqual(p['independent_context']['cards'][ev[3]['card_id']]['grouping'],'independent_fixed')
        self.assertEqual(ev,before);self.assertFalse(p['stop_verified'])

    def test_late_fill_is_included_after_parent_cancel(self):
        p=check(ended(fixture(),late=True))
        self.assertEqual(p['remaining_quantity'],'50')
        self.assertEqual(p['recovery_proposal']['next_step']['target_quantity'],'50')

    def test_parent_filled_in_cancel_race_keeps_native_pair(self):
        p=check(ended(fixture(),full=True),policy=h.REHEARSE)
        self.assertEqual(p['state'],'NATIVE_EXITS_VERIFIED');self.assertTrue(p['stop_verified'])
        self.assertIsNone(p['next_step']);self.assertIsNone(p['independent_context'])

    def test_margin_cancel_does_not_assume_children_were_canceled(self):
        ev=ended(fixture(),margin=True)
        self.assertEqual(check(ev)['state'],'POLICY_APPROVAL_REQUIRED')
        p=check(ev,policy=h.REHEARSE)
        self.assertEqual(p['next_step']['operation'],'CANCEL_LINKED_CHILD_OFFLINE')
        self.assertEqual(p['next_step']['leg'],'TAKE_PROFIT');self.assertIsNone(p['independent_context'])

    def test_each_native_child_must_retire_before_replacement(self):
        bs,s,c,l=ended(fixture(),margin=True)
        p=check((bs,s,c,l),policy=h.REHEARSE)
        self.assertEqual(p['next_step']['order_id'],l['children']['TAKE_PROFIT'])
        s['open_orders']=[o for o in s['open_orders'] if o['oid']!=l['children']['TAKE_PROFIT']]
        s['terminal_orders'].append({**terminal(bs[0],'TAKE_PROFIT','0'),'at_ms':T+6})
        s['at_ms']=c['at_ms']=T+7
        p=check((bs,s,c,l),policy=h.REHEARSE)
        self.assertEqual(p['next_step']['order_id'],l['children']['STOP']);self.assertIsNone(p['independent_context'])
        s['open_orders']=[];s['terminal_orders'].append({**terminal(bs[0],'STOP','0'),'at_ms':T+8})
        s['at_ms']=c['at_ms']=T+9
        self.assertEqual(check((bs,s,c,l))['state'],'ORIGINAL_GROUP_RETIRED')

    def test_second_card_orders_and_quantities_are_unchanged(self):
        ev=fixture();bs,s,c,l=ev
        b=binding(2,qty='60');from .test_card_lifecycle import opened
        s=merged(bs[0],s,b,opened(b),'100');bs=bs+[b]
        c2=controls(bs,s);c2['cards'][l['card_id']]['grouping']='parent_linked'
        before=deepcopy((bs,s,c2,l));p=check((bs,s,c2,l),policy=h.REHEARSE)
        self.assertEqual(p['next_step']['card_id'],l['card_id']);self.assertEqual((bs,s,c2,l),before)

    def test_cannot_call_a_working_native_pair_independent(self):
        bs,s,c,l=fixture();c['cards'][l['card_id']]['grouping']='independent_fixed'
        p=check((bs,s,c,l),policy=h.REHEARSE)
        self.assertIsNone(p['next_step']);self.assertIn('CANNOT_RELABEL_WORKING_LINKED_ORDERS',p['reasons'])

    def test_one_active_child_during_partial_entry_requires_recheck(self):
        bs,s,c,l=fixture();s['open_orders'][1]['state']='ACTIVE'
        p=check((bs,s,c,l),policy=h.REHEARSE)
        self.assertIsNone(p['next_step']);self.assertIn('LINKED_ACTIVATION_RACE_RECHECK',p['reasons'])

    def test_stale_snapshot_cannot_propose(self):
        p=check(fixture(),policy=h.REHEARSE,now=T+16000)
        self.assertIsNone(p['next_step']);self.assertIn('STALE_OR_FUTURE_SNAPSHOT',p['reasons'])

    def test_stale_mark_cannot_propose(self):
        bs,s,c,l=fixture();c['at_ms']=T-16000
        self.assertIsNone(check((bs,s,c,l),policy=h.REHEARSE)['next_step'])

    def test_incomplete_history_does_not_mean_unfilled(self):
        bs,s,c,l=fixture();s['history_complete']=False
        p=check((bs,s,c,l),policy=h.REHEARSE);self.assertIsNone(p['next_step'])
        self.assertIn('FILL_HISTORY_INCOMPLETE',p['reasons'])

    def test_account_discrepancy_blocks_handoff(self):
        bs,s,c,l=fixture();s['position_quantity']='41'
        self.assertIsNone(check((bs,s,c,l),policy=h.REHEARSE)['next_step'])

    def test_pending_or_rejected_recovery_never_bypassed(self):
        for state in ('OUTCOME_UNKNOWN','ACCEPTED_UNVERIFIED','REJECTED'):
            bs,s,c,l=fixture();c['cards'][l['card_id']]['requests']['STOP']=dict(state=state,code='REDUCE_ONLY' if state=='REJECTED' else None)
            self.assertIsNone(check((bs,s,c,l),policy=h.REHEARSE)['next_step'])

    def test_wrong_original_link_rejected(self):
        for field,value in (('card_id','a'*64),('card_digest','a'*64),('parent_oid','999'),('grouping','na')):
            ev=fixture();ev[3][field]=value
            with self.assertRaises(h.HandoffError):check(ev,policy=h.REHEARSE)

    def test_child_of_another_card_cannot_be_canceled(self):
        ev=fixture();ev[3]['children']['STOP']='999'
        with self.assertRaises(h.HandoffError):check(ev,policy=h.REHEARSE)

    def test_missing_order_is_not_terminal_confirmation(self):
        bs,s,c,l=ended(fixture());s['terminal_orders'].pop()
        p=check((bs,s,c,l),policy=h.REHEARSE)
        self.assertIsNone(p['independent_context']);self.assertIn('ORDER_STATUS_UNRESOLVED',p['reasons'])

    def test_crossed_stop_is_not_moved_after_retirement(self):
        bs,s,c,l=ended(fixture());c['mark_price']='97'
        p=check((bs,s,c,l));self.assertIsNone(p['recovery_proposal']['next_step'])
        self.assertIn('EXIT_LEVEL_REACHED_NO_AUTOMATIC_REPRICE',p['recovery_proposal']['reasons'])

    def test_flat_no_fill_does_not_reopen_entry(self):
        bs,s,c,l=ended(fixture());s['fills']=[];s['position_quantity']='0'
        s['terminal_orders'][0]['filled_quantity']='0'
        p=check((bs,s,c,l));self.assertEqual(p['recovery_proposal']['state'],'NO_CORRECTION_NEEDED')

    def test_unknown_policy_rejected(self):
        for p in ('automatic',True,None):
            with self.assertRaises(h.HandoffError):check(fixture(),policy=p)

    def test_parent_identifier_requires_positive_bounded_integer(self):
        for oid in ('0',str(2**64),'01'):
            bs,s,c,l=fixture();old=l['parent_oid'];l['parent_oid']=oid
            bs[0]['orders']['ENTRY']=[oid]
            for collection in ('fills','open_orders'):
                for row in s[collection]:
                    if row['oid']==old:row['oid']=oid
            with self.assertRaises(h.life.LifecycleError):check((bs,s,c,l),policy=h.REHEARSE)

    def test_mixed_native_and_new_stop_not_verified_even_if_total_matches(self):
        bs,s,c,l=ended(fixture(),full=True)
        bs[0]['orders']['STOP'].append('901')
        for row in s['open_orders']:
            if row['oid']==l['children']['STOP']:row['quantity']='50'
        s['open_orders'].append({**order(bs[0],'STOP','50'),'oid':'901'})
        p=check((bs,s,c,l),policy=h.REHEARSE)
        self.assertIn('MIXED_NATIVE_AND_REPLACEMENT_EXITS_REVIEW',p['reasons'])
        self.assertFalse(p['stop_verified']);self.assertIsNone(p['next_step'])
        self.assertEqual(p['unprotected_quantity'],'100')

    def test_replacements_allowed_after_all_original_children_are_terminal(self):
        bs,s,c,l=ended(fixture())
        for leg,oid in (('STOP','901'),('TAKE_PROFIT','902')):
            bs[0]['orders'][leg].append(oid)
            s['open_orders'].append({**order(bs[0],leg,'40'),'oid':oid})
        p=check((bs,s,c,l))
        self.assertEqual(p['state'],'ORIGINAL_GROUP_RETIRED')
        self.assertEqual(p['recovery_proposal']['state'],'NO_CORRECTION_NEEDED')
        self.assertTrue(p['stop_verified'])

    def test_module_has_no_transport_keys_startup_or_timer(self):
        source=inspect.getsource(h)
        for text in ('import os','import http','import requests','import threading','sign_l1_action','def startup','def start(', '/exchange'):
            self.assertNotIn(text,source)
