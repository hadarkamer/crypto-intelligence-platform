"""Partial-close residual regressions on the actual controller and durable store.

No real network, keys or signatures. Unsafe two-card starting states are created
ONLY in the venue double, as legacy evidence to clean up; no release is enabled.
"""
from copy import deepcopy
from decimal import Decimal
import os
import unittest
from unittest.mock import patch

from . import residual_exit_fence as fence, residual_exit_contract as contract
from . import filled_quantity_dispatch as dispatch, card_lifecycle as life
from . import test_residual_exit_fence as existing
from .test_filled_quantity_dispatch import NoExternal, Venue, ROUTES2, META, state_from_case
from .test_filled_quantity_exits import original
from .test_card_lifecycle import fill, T
from .filled_dispatch_store import DispatchStore, DispatchError, SCHEMA
from .postgres_journal import PostgresJournal
from .trade_card_store import CardStore

CI = os.environ.get('HL_JOURNAL_CI_URL')


def partially_closed(*, side='LONG', leg='TAKE_PROFIT', quantity='20', q='100', n=1, second=True):
    s = state_from_case(q=q, side=side, n=n, stop=q, take=q)
    b = s['bindings'][0]; snap = s['evidence']['snapshot']
    snap['fills'].append(fill(b, leg, qty=quantity, fid='partial-exit-'+str(n)))
    remaining = str(Decimal(q)-Decimal(quantity))
    next(o for o in snap['open_orders'] if o['oid']==b['orders'][leg][0])['quantity'] = remaining
    snap['position_quantity'] = ('-' if side=='SHORT' else '')+remaining
    if second:
        s = existing.merged(s, state_from_case(n=n+1, q='60', side=side, stop='60', take='60'))
    return s


def select(s, mark='10'):
    at = s['evidence']['snapshot']['at_ms']
    return dispatch.choose(s, ROUTES2, META, dict(mark_price=mark, at_ms=at),
        now_ms=at, after_exit_policy=dispatch.AFTER_EXIT)


class PartialResidualPureTests(NoExternal):
    def test_partial_tp_and_sl_both_directions_cancel_only_oversized_sibling(self):
        for side in ('LONG','SHORT'):
            for leg in ('STOP','TAKE_PROFIT'):
                with self.subTest(side=side, leg=leg):
                    s=partially_closed(side=side,leg=leg);before=deepcopy(s);p=select(s)
                    sibling='STOP' if leg=='TAKE_PROFIT' else 'TAKE_PROFIT'
                    b=s['bindings'][0]
                    self.assertEqual((p['operation'],p['leg']),('CANCEL_FOR_RESIZE',sibling))
                    self.assertEqual(p['old_oid'],b['orders'][sibling][0])
                    self.assertEqual(p['card_id'],b['card_id'])
                    self.assertTrue(fence.validate_proposal(s,p,now_ms=T))
                    self.assertTrue(contract.validate_wire_proposal(s,p,now_ms=T))
                    self.assertEqual(s,before)

    def test_crossed_level_does_not_leave_oversized_sibling_live(self):
        for side,mark in (('LONG','9'),('SHORT','11')):
            s=partially_closed(side=side);p=select(s,mark)
            self.assertEqual(p['operation'],'CANCEL_FOR_RESIZE')
            self.assertEqual(p['card_id'],s['bindings'][0]['card_id'])
            self.assertEqual(p['leg'],'STOP')
            self.assertEqual(p['action']['type'],'cancel')

    def test_other_card_missing_protection_does_not_suppress_exact_cleanup(self):
        s=partially_closed(second=False)
        s=existing.merged(s,state_from_case(n=2,q='60'))
        p=select(s,mark='9')
        self.assertEqual(p['old_oid'],s['bindings'][0]['orders']['STOP'][0])
        self.assertEqual(p['action']['cancels'],[dict(a=0,o=int(p['old_oid']))])

    def test_pending_entry_after_exit_still_has_approved_priority(self):
        s=partially_closed(q='40',quantity='10');p=select(s)
        self.assertEqual(p['operation'],'CANCEL_ENTRY_AFTER_EXIT')
        self.assertEqual(p['old_oid'],s['bindings'][0]['orders']['ENTRY'][0])

    def test_unapproved_entry_cancellation_policy_is_not_bypassed(self):
        s=partially_closed(q='40',quantity='10',second=False)
        self.assertIsNone(fence.cleanup(s,now_ms=T))

    def test_correct_size_is_not_a_cleanup_candidate(self):
        s=partially_closed(second=False);b=s['bindings'][0]
        next(o for o in s['evidence']['snapshot']['open_orders'] if o['oid']==b['orders']['STOP'][0])['quantity']='80'
        self.assertIsNone(fence.cleanup(s,now_ms=T,after_exit_policy=dispatch.AFTER_EXIT))

    def test_without_any_exit_existing_growth_recovery_is_unchanged(self):
        s=state_from_case(q='60',stop='40',take='40')
        self.assertIsNone(fence.cleanup(s,now_ms=T,after_exit_policy=dispatch.AFTER_EXIT))
        self.assertEqual(select(s)['operation'],'CANCEL_FOR_RESIZE')

    def test_subunit_exit_quantity_does_not_round_away_residual_risk(self):
        s=partially_closed(quantity='0.01');p=select(s)
        self.assertEqual(p['leg'],'STOP')
        view=life.review(s['bindings'],s['evidence']['snapshot'],now_ms=T)
        self.assertEqual(next(c for c in view['cards'] if c['card_id']==p['card_id'])['remaining_quantity'],'99.99')

    def test_duplicate_fill_reports_do_not_change_the_cleanup_target(self):
        s=partially_closed();expected=select(s)
        s['evidence']['snapshot']['fills']*=3
        actual=select(s)
        for k in ('card_id','old_oid','leg','action','operation'):
            self.assertEqual(actual[k],expected[k])

    def test_flat_residual_takes_precedence_over_partial_resize(self):
        s=existing.merged(existing.orphan(second=False),partially_closed(n=2,second=False))
        self.assertEqual(select(s)['operation'],'CANCEL_ORPHAN_EXIT')
        self.assertEqual(select(s)['card_id'],s['bindings'][0]['card_id'])

    def test_quantity_mismatch_does_not_authorize_guessing_cleanup(self):
        s=partially_closed();s['evidence']['snapshot']['position_quantity']='1'
        self.assertIsNone(fence.cleanup(s,now_ms=T,after_exit_policy=dispatch.AFTER_EXIT))

    def test_two_independent_healthy_siblings_are_still_not_native_oco(self):
        s=partially_closed();p=select(s)
        self.assertEqual(p['action']['type'],'cancel')
        report=fence.exposure(s,now_ms=T)
        self.assertFalse(report['native_per_card_oco_verified'])
        self.assertFalse(report['shared_market_release_authorized'])


@unittest.skipUnless(CI,'Disposable loopback PostgreSQL required')
class PartialResidualDatabaseTests(NoExternal):
    def setUp(self):
        super().setUp();self.j=PostgresJournal.for_ci(CI)
        with self.j._transaction() as conn:
            for name in (SCHEMA,'hl_testnet_recovery_rehearsal_v1','hl_testnet_cards_v1','hl_testnet_execution_v1'):
                conn.execute(f'DROP SCHEMA IF EXISTS {name} CASCADE')
        self.j.bootstrap();self.cards=CardStore(self.j);self.cards.initialize()
        self.store=DispatchStore(self.j);self.store.initialize();self.v=Venue()
        self.c=dispatch.Controller(self.store,self.v,ROUTES2,after_exit_policy=dispatch.AFTER_EXIT)
        self.b,self.o=original();self.cards.record(self.o['card'])
        self.bucket=self.c.register(self.b['card_id'])['bucket']

    # Reuse helpers without inheriting/recounting the existing test methods.
    cycle=existing.ResidualDatabaseTests.cycle
    protect=existing.ResidualDatabaseTests.protect
    unsafe_legacy_two_card_setup=existing.ResidualDatabaseTests.unsafe_legacy_two_card_setup

    def test_partial_tp_cleanup_preserves_every_other_card_order(self):
        other=self.unsafe_legacy_two_card_setup()
        before={oid:deepcopy(self.v.orders[oid]) for oid in ('1003','1004','1005')}
        self.v.fill('1002','20');self.cycle()
        self.assertEqual(self.v.requests[-1]['proposal']['operation'],'CANCEL_FOR_RESIZE')
        self.assertEqual(self.v.requests[-1]['proposal']['old_oid'],'1001')
        self.cycle(False);s=self.store.load(self.bucket)
        self.assertIsNone(s['pending'])
        view=life.review(s['bindings'],s['evidence']['snapshot'],now_ms=self.v.now())
        own=next(c for c in view['cards'] if c['card_id']==self.b['card_id'])
        second=next(c for c in view['cards'] if c['card_id']==other['card_id'])
        self.assertEqual(own['remaining_quantity'],'80');self.assertFalse(own['closure_verified'])
        self.assertEqual(second['remaining_quantity'],'100')
        self.assertEqual(before,{oid:self.v.orders[oid] for oid in before})
        self.assertEqual(self.v.orders['1002']['status'],'open')
        self.assertEqual(Decimal(self.v.orders['1002']['order']['sz']),Decimal('80'))

    def test_partial_stop_cleanup_does_not_cancel_the_remaining_stop(self):
        self.unsafe_legacy_two_card_setup();self.v.fill('1001','20');self.cycle()
        self.assertEqual(self.v.requests[-1]['proposal']['old_oid'],'1002')
        self.assertEqual(self.v.orders['1001']['status'],'open')
        self.assertEqual(Decimal(self.v.orders['1001']['order']['sz']),Decimal('80'))

    def test_stale_never_sent_resize_becomes_exact_orphan_cleanup(self):
        self.protect();self.v.fill('1002','20');preview=self.cycle(False)
        state=self.store.load(self.bucket)
        state=self.store.reserve(state,preview['proposal'],self.v.now());rid=state['pending']
        self.v.fill('1002','80');before=self.v.sent
        result=self.cycle(False)
        old=self.store.request(rid)
        self.assertEqual(old['phase'],'ABORTED_UNSENT');self.assertEqual(old['attempts'],0)
        self.assertIsNone(old['nonce']);self.assertIsNone(old['attempt_at_ms'])
        self.assertEqual(result['proposal']['operation'],'CANCEL_ORPHAN_EXIT')
        self.assertEqual(result['proposal']['old_oid'],'1001')
        self.assertEqual(self.v.sent,before)
        self.cycle();self.cycle(False)
        s=self.store.load(self.bucket)
        self.assertTrue(life.review(s['bindings'],s['evidence']['snapshot'],now_ms=self.v.now())['cards'][0]['closure_verified'])

    def test_stale_resize_cannot_cancel_a_now_correct_stop(self):
        self.cycle();self.v.fill('1000','40');self.cycle();self.cycle();self.cycle(False)
        self.v.fill('1000','20');preview=self.cycle(False)
        self.assertEqual(preview['proposal']['old_oid'],'1001')
        state=self.store.load(self.bucket)
        state=self.store.reserve(state,preview['proposal'],self.v.now());rid=state['pending']
        self.v.fill('1002','20');before=self.v.sent
        result=self.cycle(False)
        self.assertEqual(self.store.request(rid)['phase'],'ABORTED_UNSENT')
        self.assertEqual(result['proposal']['operation'],'CANCEL_ENTRY_AFTER_EXIT')
        self.assertEqual(self.v.sent,before)
        self.assertEqual(self.v.orders['1001']['status'],'open')
        self.assertEqual(Decimal(self.v.orders['1001']['order']['sz']),Decimal('40'))

    def test_started_unknown_resize_is_not_retired_when_card_closes(self):
        self.protect();self.v.fill('1002','20')
        def unknown(request):
            self.v.sent+=1;self.v.t+=10
            return None  # No evidence that the venue applied the cancellation.
        with patch.object(self.v,'send',side_effect=unknown):self.cycle()
        state=self.store.load(self.bucket);rid=state['pending'];before=self.v.sent
        self.v.fill('1002','80');out=self.cycle(False)
        self.assertEqual(out['status'],'OUTCOME_UNKNOWN')
        self.assertEqual(self.store.load(self.bucket)['pending'],rid)
        self.assertEqual(self.store.request(rid)['attempts'],1)
        self.assertEqual(self.v.sent,before)

    def test_fill_racing_partial_cleanup_is_counted_without_reattribution(self):
        other=self.unsafe_legacy_two_card_setup();self.v.fill('1002','20')
        self.v.cancel_race=lambda oid:self.v.fill(oid,'10')
        self.cycle();self.cycle(False)
        s=self.store.load(self.bucket)
        report=life.review(s['bindings'],s['evidence']['snapshot'],now_ms=self.v.now())
        a=next(c for c in report['cards'] if c['card_id']==self.b['card_id'])
        b=next(c for c in report['cards'] if c['card_id']==other['card_id'])
        self.assertEqual(a['remaining_quantity'],'70');self.assertEqual(b['remaining_quantity'],'100')
        self.assertIn('BOTH_EXIT_LEGS_FILLED_REVIEW',a['issues'])
        req=self.store.request(s['last_request'])
        self.assertFalse(req['residual_verification']['other_cards_reassigned'])
        self.assertFalse(req['residual_verification']['native_oco_or_pre_cancel_isolation_proven'])

    def test_partial_cleanup_works_with_crossed_stop_without_repricing(self):
        self.unsafe_legacy_two_card_setup();self.v.fill('1002','20')
        with patch.object(self.v,'sample',side_effect=lambda account,symbol:dict(mark_price='9',at_ms=self.v.now())):
            self.cycle()
        self.assertEqual(self.v.requests[-1]['proposal']['old_oid'],'1001')
        self.assertEqual(self.v.requests[-1]['proposal']['action'],dict(type='cancel',cancels=[dict(a=0,o=1001)]))
