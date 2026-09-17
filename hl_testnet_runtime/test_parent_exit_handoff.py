"""Offline parent/child races and shared durable journal. No venue operations."""
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
import inspect
import os
import unittest
from unittest.mock import patch
from . import parent_exit_handoff as h, card_exit_recovery as r, card_recovery_journal as d
from .postgres_journal import PostgresJournal
from .test_card_lifecycle import binding, fill, order, terminal, T
from .test_card_exit_recovery import controls, partial, merged, ack

CI = os.environ.get('HL_JOURNAL_CI_URL')


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


@unittest.skipUnless(CI,'Requires disposable loopback PostgreSQL')
class DurableHandoffTests(unittest.TestCase):
    def setUp(self):
        self.journal=PostgresJournal.for_ci(CI)
        with self.journal._transaction() as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS {d.SCHEMA} CASCADE')
            conn.execute('DROP SCHEMA IF EXISTS hl_testnet_execution_v1 CASCADE')
        self.journal.bootstrap();self.store=h.ParentHandoffJournal(self.journal);self.store.initialize()
        self.ev=fixture()
        for target in ('http.client.HTTPSConnection','socket.create_connection'):
            guard=patch(target,side_effect=AssertionError('VENUE_FORBIDDEN'));guard.start();self.addCleanup(guard.stop)

    def prepare(self):
        return self.store.prepare_handoff(*self.ev,policy=h.REHEARSE,event_id='prepare',expected_revision=0,now_ms=T)

    def begin(self,p):
        return self.store.begin_handoff(p['request']['request_id'],*self.ev,policy=h.REHEARSE,
            event_id='begin',expected_revision=p['revision'],now_ms=T+1)

    def finish(self,p,ev=None):
        ev=ev or ended(self.ev)
        return self.store.confirm_terminal(p['request']['request_id'],*ev,policy=h.REHEARSE,
            event_id='terminal',expected_revision=p['revision'],now_ms=ev[1]['at_ms'])

    def test_preserve_policy_never_creates_cancellation_request(self):
        # The shared journal intentionally normalizes its domain exceptions.
        with self.assertRaisesRegex(d.RecoveryStorageError,'^EXPLICIT_FRESH_HANDOFF_REHEARSAL_REQUIRED$'):
            self.store.prepare_handoff(*self.ev,event_id='not-approved',expected_revision=0,now_ms=T)
        self.assertIsNone(self.store.pending(self.ev[1]['account'],'DOGE')['request'])

    def test_restart_preserves_uncertainty_and_blocks_repeat(self):
        p=self.begin(self.prepare());rid=p['request']['request_id']
        self.store=h.ParentHandoffJournal(PostgresJournal.for_ci(CI))
        self.assertEqual(self.store.load(rid)['request']['phase'],'OUTCOME_UNKNOWN')
        with self.assertRaises(d.RecoveryStorageError):
            self.store.begin_handoff(rid,*self.ev,policy=h.REHEARSE,event_id='repeat',expected_revision=p['revision'],now_ms=T+2)
        self.assertEqual(self.store.load(rid)['request']['simulated_attempt_count'],1)

    def test_unknown_cancel_can_resolve_only_from_terminal_evidence(self):
        p=self.begin(self.prepare());p=self.finish(p,ended(self.ev,late=True))
        self.assertEqual(p['request']['phase'],'OBSERVED')
        self.assertEqual(p['request']['confirmation']['next_assessment']['remaining_quantity'],'50')
        self.assertTrue(p['request']['confirmation']['recovered_card_needs_more_work'])

    def test_receipt_alone_is_not_protection(self):
        p=self.begin(self.prepare())
        p=self.store.record_reply(p['request']['request_id'],ack(self.ev[3]['parent_oid']),
            event_id='ack',expected_revision=p['revision'],now_ms=T+2)
        self.assertEqual(p['request']['phase'],'WAITING_FOR_EVIDENCE')
        self.assertIsNone(p['request']['confirmation'])

    def test_confirm_requires_new_observation(self):
        p=self.begin(self.prepare())
        with self.assertRaises(d.RecoveryStorageError):self.finish(p,self.ev)
        self.assertEqual(self.store.load(p['request']['request_id'])['request']['phase'],'OUTCOME_UNKNOWN')

    def test_changed_policy_cannot_complete_old_request(self):
        p=self.begin(self.prepare());ev=ended(self.ev)
        with self.assertRaises(d.RecoveryStorageError):
            self.store.confirm_terminal(p['request']['request_id'],*ev,policy=h.PRESERVE,event_id='different',expected_revision=p['revision'],now_ms=T+5)

    def test_race_full_fill_resolves_without_fabricating_cancel(self):
        p=self.finish(self.begin(self.prepare()),ended(self.ev,full=True))
        c=p['request']['confirmation'];self.assertEqual(c['terminal_state'],'FILLED')
        self.assertFalse(c['cancellation_caused_terminal_state'])
        self.assertEqual(c['next_assessment']['state'],'NATIVE_EXITS_VERIFIED')

    def test_no_new_recovery_until_old_request_is_resolved(self):
        p=self.begin(self.prepare());ev=ended(self.ev);assessment=check(ev)
        with self.assertRaises(d.RecoveryStorageError):
            d.RecoveryJournal(self.journal).prepare(ev[0],ev[1],assessment['independent_context'],
                event_id='competing',expected_revision=p['revision'],now_ms=T+5)

    def test_changed_old_fill_cannot_confirm(self):
        p=self.begin(self.prepare());ev=ended(self.ev);ev[1]['fills'][0]['price']='99'
        with self.assertRaises(d.RecoveryStorageError):self.finish(p,ev)

    def test_terminal_before_working_observation_does_not_unlock(self):
        p=self.begin(self.prepare());ev=ended(self.ev);ev[1]['terminal_orders'][0]['at_ms']=T-1
        with self.assertRaisesRegex(d.RecoveryStorageError,'^TERMINAL_PREDATES_WORKING_OBSERVATION$'):
            self.finish(p,ev)
        self.assertEqual(self.store.load(p['request']['request_id'])['request']['phase'],'OUTCOME_UNKNOWN')

    def test_future_time_does_not_release_unknown_request(self):
        p=self.begin(self.prepare());rid=p['request']['request_id']
        with self.assertRaises(d.RecoveryStorageError):
            self.store.begin_handoff(rid,*self.ev,policy=h.REHEARSE,event_id='after-long-delay',
                expected_revision=p['revision'],now_ms=T+86400000)
        self.assertEqual(self.store.load(rid)['request']['simulated_attempt_count'],1)

    def test_concurrent_deliveries_keep_one_request(self):
        with ThreadPoolExecutor(max_workers=4) as pool:
            results=list(pool.map(lambda _:self.prepare(),range(4)))
        self.assertEqual(len({p['request']['request_id'] for p in results}),1)
        self.assertEqual(sum(not p['duplicate'] for p in results),1)

    def test_pending_children_hold_replacements_after_parent_resolution(self):
        p=self.finish(self.begin(self.prepare()),ended(self.ev,waiting=True))
        self.assertEqual(p['request']['confirmation']['next_assessment']['state'],'WAITING_CHILD_FINALITY')
        self.assertTrue(p['request']['confirmation']['recovered_card_needs_more_work'])

    def test_handoff_then_existing_durable_stop_and_take_recovery(self):
        p=self.finish(self.begin(self.prepare()));ev=ended(self.ev,at=T+6)
        bs,s,c,l=ev;adapted=check(ev)['independent_context'];store=d.RecoveryJournal(self.journal)
        current=p
        for i,(leg,oid) in enumerate((('STOP','901'),('TAKE_PROFIT','902'))):
            start=T+6+i*5;s['at_ms']=adapted['at_ms']=start
            current=store.prepare(bs,s,adapted,event_id='prepare-'+leg,expected_revision=current['revision'],now_ms=start)
            rid=current['request']['request_id'];self.assertEqual(current['request']['proposal']['next_step']['leg'],leg)
            current=store.begin_simulated_attempt(rid,bs,s,adapted,event_id='begin-'+leg,expected_revision=current['revision'],now_ms=start+1)
            current=store.record_reply(rid,ack(oid),event_id='ack-'+leg,expected_revision=current['revision'],now_ms=start+2)
            bs=deepcopy(bs);s=deepcopy(s);bs[0]['orders'][leg].append(oid)
            s['open_orders'].append({**order(bs[0],leg,'40'),'oid':oid})
            s['at_ms']=adapted['at_ms']=start+3
            current=store.confirm_observed(rid,bs,s,adapted,event_id='verify-'+leg,expected_revision=current['revision'],now_ms=start+3)
            self.assertEqual(current['request']['phase'],'OBSERVED')
        self.assertFalse(current['request']['confirmation']['recovered_card_needs_more_work'])
        self.assertEqual(r.plan(bs,s,adapted,now_ms=s['at_ms'])['state'],'NO_CORRECTION_NEEDED')
