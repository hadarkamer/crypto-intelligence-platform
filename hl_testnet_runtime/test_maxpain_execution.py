"""Offline prospective entry, source leases and actual-fill race tests."""
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import json
import unittest
from unittest.mock import patch

import experimental_execution_contract as contract
from . import maxpain_execution as execution
from .trade_cards import checksum

CREATED = int(datetime(2026, 10, 6, 10, 0, 10, tzinfo=timezone.utc).timestamp()*1000)
ARM = (CREATED//60000+1)*60000
METADATA = {'universe': [{'name': 'HYPE', 'szDecimals': 2}]}

# Independently transcribed from retrieved Active_Experimental_Formulas_20261005.html
# v2 and Hyperliquid_Formula_Comparison_20261006.html v4. This checks saved report
# requirements, not an assertion that the original Yoni chat was retrieved.
# Timeframe labels never imply trade direction. Growth-tier thresholds are
# existing implementation evidence and are not reapproved by these reports.
REPORT_FORMULAS = (
    ('SOL_MAXPAIN_DIST1_3_RANGE24', 'SOL', 1., 3., 2., 1., False,
     ('12h','24h','48h','3d','1w','2w','1m'), True, ('LONG','SHORT')),
    ('HYPE_MAXPAIN_DIST05_15_LONG_TF', 'HYPE', .5, 1.5, 2., .5, True,
     ('3d','1w','2w','1m'), False, ('LONG','SHORT')),
    ('DOGE_MAXPAIN_DIST15_25_LONG_TF', 'DOGE', 1.5, 2.5, 2., .5, False,
     ('3d','1w','2w','1m'), False, ('LONG','SHORT')),
    ('XRP_MAXPAIN_LONG_DIST2_4_SHORT_TF', 'XRP', 2., 4., .5, 1., True,
     ('12h','24h','48h'), False, ('LONG',)),
    ('ETH_MAXPAIN_LONG_DIST1_3', 'ETH', 1., 3., 2., .5, True,
     ('12h','24h','48h','3d','1w','2w','1m'), False, ('LONG',)),
)


def plan(*, timeframe='3d', target=101., source=100., kind='PLAN', at=CREATED,
         cycle='cycle-1', comparisons=None, liquidity='100', direction='LONG'):
    """Real frozen HYPE definition; float source outputs preserved exactly."""
    signed = target-source
    value = dict(version=contract.VERSION, kind=kind, family='maxpain', occurrence_id='',
        rule_id='HYPE_MAXPAIN_DIST05_15_LONG_TF', symbol='HYPE', side=direction,
        source_environment='mainnet', execution_environment='testnet', source_price_kind='TRADE_1M',
        source_at=contract.iso_ms(CREATED-10000), created_at=contract.iso_ms(CREATED),
        arm_at=contract.iso_ms(ARM), expires_at=contract.iso_ms(ARM+86400000),
        entry=contract.price(source-2*signed), stop=contract.price(source-5*signed),
        take_profit=contract.price(source+.5*signed), original_target=contract.price(target),
        policy=dict(name='MAXPAIN_LIMIT_PRETOUCH_V1', entry_adverse='2.0', take_fraction='0.5',
            stop_distance_multiplier='5', overlap_target_fraction='0.002', liquidity_growth=True,
            source_price='HYPERLIQUID_HYPE_PERPETUAL_TRADE_1M'),
        proof=dict(source_contract_version=contract.SOURCE_CONTRACT, source_config_sha256='1'*64,
            cycle_id=cycle, episode_key=format(Decimal(str(target)).normalize(), 'f'),
            episode_generation=1, episode_first_ms=CREATED, timeframe=timeframe,
            source_side='SHORT' if direction == 'LONG' else 'LONG', source_observed_ms=CREATED-10000,
            source_quote=dict(price_source='HYPERLIQUID_HYPE_PERPETUAL_TRADE_1M', price_market='perpetual',
                              price_pair='HYPE-PERP', price_instrument='HYPE'),
            liquidation_amount=liquidity, cluster_comparisons=comparisons or [],
            range24_low=None, range24_high=None, source_price=contract.price(source)),
        source_sequence=at*10+contract.RANK[kind], source_as_of=contract.iso_ms(at),
        source_state='PENDING', valid_until=contract.iso_ms(min(ARM+86400000, at+90000)),
        cancel_reason=None)
    if kind == 'CANCEL':
        value.update(source_state='OPEN', cancel_reason='SOURCE_ENTRY_OBSERVED')
    value['occurrence_id'] = contract.occurrence_id(value)
    return contract.validate(value)


def update(value, kind, at, *, reason=None):
    result = deepcopy(value)
    result.update(kind=kind, source_sequence=at*10+contract.RANK[kind],
                  source_as_of=contract.iso_ms(at),
                  valid_until=contract.iso_ms(min(contract.moment_ms(value['expires_at']), at+90000)))
    if kind == 'CANCEL':
        result.update(source_state='OPEN', cancel_reason=reason or 'SOURCE_ENTRY_OBSERVED')
    return contract.validate(result)


def report_plan(spec, distance, side='LONG', timeframe=None):
    rule, symbol, _, _, adverse, take, growth, frames, requires_range, _ = spec
    value = plan()
    source = 100.
    target = source+distance if side == 'LONG' else source-distance
    signed = target-source
    source_name = 'HYPERLIQUID_' + symbol + '_PERPETUAL_TRADE_1M'
    value.update(rule_id=rule, symbol=symbol, side=side, entry=contract.price(source-adverse*signed),
                 stop=contract.price(source-5*signed), take_profit=contract.price(source+take*signed),
                 original_target=contract.price(target))
    value['policy'].update(entry_adverse=str(adverse), take_fraction=str(take),
                            liquidity_growth=growth, source_price=source_name)
    value['proof'].update(timeframe=timeframe or frames[0], source_side='SHORT' if side=='LONG' else 'LONG',
        episode_key=format(Decimal(str(target)).normalize(),'f'),
        range24_low='90' if requires_range else None, range24_high='110' if requires_range else None,
        source_quote=dict(price_source=source_name,price_market='perpetual',
                          price_pair=symbol+'-PERP',price_instrument=symbol))
    value['occurrence_id'] = contract.occurrence_id(value)
    return value


def market(now=ARM, price='100', environment='testnet'):
    return dict(environment=environment, symbol='HYPE', at_ms=now, mark_price=price)


def ownership(*, final=True, pending=False, role='long_account'):
    return dict(account_role=role, symbol='HYPE', all_prior_cards_final=final, unresolved_request=pending)


def ready(value=None):
    value = plan() if value is None else value
    return execution.receive(execution.initial(), value, now_ms=CREATED), value['occurrence_id']


def admission(state, cid, **kwargs):
    now = kwargs.pop('now_ms', ARM)
    return execution.entry_admission(state, cid, kwargs.pop('metadata', METADATA),
        kwargs.pop('market', market(now)), kwargs.pop('ownership', ownership()), now_ms=now, **kwargs)


def attempted():
    state, cid = ready()
    action = admission(state, cid)['action']
    return execution.begin_entry(state, action, METADATA, market(), ownership(), now_ms=ARM), cid


def evidence(state, cid, *, at=ARM+1000, status='OPEN', fills=None):
    return dict(environment='testnet', symbol='HYPE', side='LONG',
        request_id=state['records'][cid]['request']['request_id'], order_id='41',
        at_ms=at, status=status, history_complete=True, fills=fills or [])


def fill(quantity='1', identity='f1', at=ARM+500):
    return dict(fill_id=identity, quantity=quantity, price='98', at_ms=at)


class MaxPainExecutionTests(unittest.TestCase):
    def setUp(self):
        for name in ('http.client.HTTPSConnection', 'socket.create_connection',
                     'socket.socket.connect', 'socket.socket.connect_ex'):
            blocker = patch(name, side_effect=AssertionError('OFFLINE_TEST_ONLY'))
            blocker.start(); self.addCleanup(blocker.stop)

    def test_source_plan_preserved_no_order_or_fill_on_receipt(self):
        source = plan(); before = deepcopy(source)
        state, cid = ready(source)
        self.assertEqual(source, before)
        self.assertEqual(state['records'][cid]['plan'], source)
        self.assertIsNone(state['records'][cid]['request'])
        self.assertEqual(state['records'][cid]['fills'], {})

    def test_prearm_required_and_heartbeat_cannot_bootstrap(self):
        for at in (ARM, ARM+1):
            with self.assertRaisesRegex(execution.PlanError, 'PROSPECTIVE_PREARM'):
                execution.receive(execution.initial(), plan(), now_ms=at)
        with self.assertRaisesRegex(execution.PlanError, 'HEARTBEAT_CANNOT_CREATE'):
            execution.receive(execution.initial(), plan(kind='HEARTBEAT'), now_ms=CREATED)

    def test_future_source_rejected(self):
        with self.assertRaisesRegex(execution.PlanError, 'FUTURE_SOURCE'):
            execution.receive(execution.initial(), plan(), now_ms=CREATED-1)

    def test_recipient_duplicate_does_not_rearm_even_after_restart(self):
        state, cid = ready(); duplicate = plan()
        duplicate['proof']['episode_generation'] = 7
        duplicate['proof']['episode_first_ms'] -= 1000
        restored = json.loads(json.dumps(state))
        self.assertEqual(execution.receive(restored, duplicate, now_ms=CREATED+1), state)
        self.assertEqual(len(restored['records']), 1)

    def test_changed_immutable_source_conflicts(self):
        state, _ = ready(); changed = plan()
        changed['proof']['source_config_sha256'] = '2'*64
        with self.assertRaisesRegex(execution.PlanError, 'IMMUTABLE_PLAN_CHANGED'):
            execution.receive(state, changed, now_ms=CREATED)

    def test_sequence_conflict_and_regression_rejected(self):
        state, cid = ready()
        newer = update(plan(), 'HEARTBEAT', CREATED+1000)
        state = execution.receive(state, newer, now_ms=CREATED+1000)
        with self.assertRaisesRegex(execution.PlanError, 'SOURCE_SEQUENCE_REGRESSION'):
            execution.receive(state, plan(), now_ms=CREATED+2000)
        bad = deepcopy(newer); bad['valid_until'] = contract.iso_ms(CREATED+80000)
        with self.assertRaisesRegex(execution.PlanError, 'SOURCE_SEQUENCE_CONFLICT'):
            execution.receive(state, bad, now_ms=CREATED+2000)

    def test_delayed_cancel_retires_newer_heartbeat_without_regressing_watermark(self):
        state, cid = ready()
        beat = update(plan(), 'HEARTBEAT', CREATED+2000)
        state = execution.receive(state, beat, now_ms=CREATED+2000)
        cancel = update(plan(), 'CANCEL', CREATED+1000)
        state = execution.receive(state, cancel, now_ms=CREATED+3000)
        self.assertEqual(state['records'][cid]['source']['source_sequence'], beat['source_sequence'])
        self.assertEqual(state['records'][cid]['source_cancellation'], cancel)
        self.assertEqual(admission(state,cid)['status'],'SOURCE_CANCELED')

    def test_stale_duplicate_persists_lease_lapse_before_next_renewal(self):
        state,cid=ready()
        state=execution.receive(state,plan(),now_ms=CREATED+90000)
        self.assertEqual(state['records'][cid]['cancel_reason'],'SOURCE_LEASE_EXPIRED')
        renewed=update(plan(),'HEARTBEAT',CREATED+90001)
        state=execution.receive(state,renewed,now_ms=CREATED+90001)
        self.assertEqual(admission(state,cid,now_ms=CREATED+90001)['status'],'SOURCE_CANCELED')

    def test_tombstone_precedes_plan_and_cannot_revive(self):
        cancel = update(plan(), 'CANCEL', CREATED+100)
        state = execution.receive(execution.initial(), cancel, now_ms=CREATED+100)
        with self.assertRaisesRegex(execution.PlanError, 'SOURCE_SEQUENCE_REGRESSION'):
            execution.receive(state, plan(), now_ms=CREATED+200)
        renewed = update(plan(), 'HEARTBEAT', CREATED+500)
        state = execution.receive(state, renewed, now_ms=CREATED+500)
        self.assertEqual(admission(state, plan()['occurrence_id'])['status'], 'SOURCE_CANCELED')

    def test_lease_expiry_permanent_across_late_renewal(self):
        state, cid = ready()
        state = execution.receive(state, update(plan(), 'HEARTBEAT', CREATED+90000), now_ms=CREATED+90000)
        self.assertEqual(state['records'][cid]['cancel_reason'], 'SOURCE_LEASE_EXPIRED')
        self.assertIsNone(admission(state, cid, now_ms=CREATED+90000)['action'])

    def test_timely_heartbeat_preserves_original_24h_expiry(self):
        state, cid = ready()
        beat = update(plan(), 'HEARTBEAT', CREATED+60000)
        state = execution.receive(state, beat, now_ms=CREATED+60000)
        self.assertEqual(state['records'][cid]['plan']['expires_at'], plan()['expires_at'])
        self.assertIsNotNone(admission(state, cid, now_ms=CREATED+61000)['action'])

    def test_source_plan_expiry_retained_after_restart(self):
        state, cid = ready()
        expiry = contract.moment_ms(plan()['expires_at'])
        state = execution.advance(json.loads(json.dumps(state)), now_ms=expiry)
        self.assertEqual(state['records'][cid]['cancel_reason'], 'SOURCE_PLAN_EXPIRED')

    def test_arm_and_exact_source_prices_size(self):
        state, cid = ready()
        self.assertEqual(admission(state, cid, now_ms=ARM-1)['status'], 'WAITING_FOR_ORIGINAL_ARM')
        action = admission(state, cid)['action']
        self.assertEqual(action['prepared']['source']['entry'], plan()['entry'])
        self.assertEqual(action['prepared']['execution']['entry'], '98')
        self.assertEqual(Decimal(action['quantity']), Decimal('3.33'))
        self.assertFalse(action['dispatch_enabled'])

    def test_mark_after_limit_cannot_recreate_historical_entry(self):
        state, cid = ready()
        for px in ('98', '97'):
            self.assertEqual(admission(state, cid, market=market(price=px))['status'],
                             'ENTRY_ALREADY_REACHED_NO_LATE_ORDER')

    def test_price_outside_stop_take_blocked(self):
        state, cid = ready()
        for px in ('95', '100.5', '102'):
            self.assertEqual(admission(state, cid, market=market(price=px))['status'],
                             'PRICE_OUTSIDE_ORIGINAL_EXITS')

    def test_source_market_and_stale_evidence_cannot_authorize_demo_entry(self):
        state, cid = ready()
        for sample in (market(environment='mainnet'), market(now=ARM-15001), market(now=ARM+1)):
            with self.assertRaisesRegex(execution.PlanError, 'FRESH_EXACT_TESTNET'):
                admission(state, cid, market=sample)

    def test_missing_delisted_alias_duplicate_asset_fail_closed(self):
        state, cid = ready()
        for rows in ([{'name':'OTHER','szDecimals':2}], [{'name':'HYPE-PERP','szDecimals':2}],
                     [{'name':'HYPE','szDecimals':2,'isDelisted':True}],
                     [{'name':'HYPE','szDecimals':2}]*2):
            self.assertEqual(admission(state, cid, metadata={'universe':rows})['status'], 'ASSET_UNAVAILABLE')

    def test_shared_symbol_and_account_direction_fences_remain(self):
        state, cid = ready()
        for gate in (ownership(final=False), ownership(pending=True), ownership(role='short_account'), {}):
            self.assertEqual(admission(state, cid, ownership=gate)['status'], 'SHARED_MARKET_PREDECESSOR_NOT_FINAL')

    def test_target_touch_cancels_without_inventing_fill(self):
        state, cid = ready()
        state = execution.observe_market(state, cid, market(price='101'), now_ms=ARM)
        self.assertEqual(state['records'][cid]['fills'], {})
        self.assertEqual(admission(state, cid)['status'], 'SOURCE_CANCELED')

    def test_observed_missed_demo_entry_cannot_revive_after_price_rebounds(self):
        state,cid=ready()
        state=execution.observe_market(state,cid,market(price='97'),now_ms=ARM)
        self.assertEqual(state['records'][cid]['cancel_reason'],'TESTNET_ENTRY_SAFETY_WINDOW_MISSED')
        restored=json.loads(json.dumps(state))
        self.assertEqual(admission(restored,cid,market=market(ARM+1,'100'),now_ms=ARM+1)['status'],'SOURCE_CANCELED')
        self.assertEqual(restored['records'][cid]['fills'],{})

    def test_boundary_target_overlap_is_inclusive(self):
        self.assertTrue(execution.near('100', '100.2'))
        self.assertFalse(execution.near('100', '100.2000001'))
        first = plan(); near = plan(target=101.202, cycle='cycle-2')
        far = plan(target=101.203, cycle='cycle-3')
        self.assertEqual(execution.overlap_admission(near, [first]), 'OVERLAPPING_TARGET')
        self.assertEqual(execution.overlap_admission(far, [first]), 'FORMULA_OVERLAP_ALLOWED')

    def test_same_occurrence_deduplicated(self):
        self.assertEqual(execution.overlap_admission(plan(), [plan()]), 'DUPLICATE_OCCURRENCE')

    def test_complete_adjacent_liquidity_exception_and_exchange_block(self):
        first = plan()
        tiers = [dict(timeframe=tf, target_price='101', liquidation_amount=amount,
                      source_side='SHORT', provider_valid=True)
                 for tf, amount in [('3d','100'),('1w','125'),('2w','156.25')]]
        second = plan(timeframe='2w', cycle='cycle-1', liquidity='156.25', comparisons=[
            dict(previous_target='101', previous_timeframe='3d', previous_direction=1, tiers=tiers)])
        self.assertEqual(execution.overlap_admission(second, [first]), 'FORMULA_OVERLAP_ALLOWED')
        state, _ = ready(first)
        state = execution.receive(state, second, now_ms=CREATED)
        result = admission(state, second['occurrence_id'], ownership=ownership(final=False))
        self.assertEqual(result['status'], 'SHARED_MARKET_PREDECESSOR_NOT_FINAL')

    def test_later_growth_tier_cannot_retroactively_block_earlier_plan(self):
        first=plan(timeframe='3d',cycle='same-watch')
        tiers=[dict(timeframe=tf,target_price='101',liquidation_amount=amount,
                    source_side='SHORT',provider_valid=True)
               for tf,amount in [('3d','100'),('1w','125')]]
        second=plan(timeframe='1w',cycle='same-watch',liquidity='125',comparisons=[
            dict(previous_target='101',previous_timeframe='3d',previous_direction=1,tiers=tiers)])
        for order in ((first,second),(second,first)):
            state=execution.initial()
            for incoming in order:
                state=execution.receive(state,incoming,now_ms=CREATED)
            for incoming in (first,second):
                cid=incoming['occurrence_id']
                self.assertEqual(admission(state,cid)['status'],'PROPOSED_SOFTWARE_ONLY')
                self.assertEqual(admission(state,cid,ownership=ownership(final=False))['status'],
                                 'SHARED_MARKET_PREDECESSOR_NOT_FINAL')

    def test_actual_attempted_younger_peer_is_not_ignored_by_priority(self):
        first=plan(timeframe='3d',cycle='same-watch')
        second=plan(timeframe='1w',cycle='same-watch')
        state,cid=ready(second)
        action=admission(state,cid)['action']
        state=execution.begin_entry(state,action,METADATA,market(),ownership(),now_ms=ARM)
        # A captured state that combines an already-owned younger peer with an
        # earlier waiting plan must not discard that attempted allocation.
        earlier,_=ready(first)
        state['records'].update(earlier['records'])
        self.assertEqual(admission(state,first['occurrence_id'])['status'],'OVERLAPPING_TARGET')

    def test_later_watch_generation_cannot_block_earlier_pending_admission(self):
        first=plan(cycle='older-watch')
        second=plan(cycle='newer-watch')
        second.update(created_at=contract.iso_ms(CREATED+1000),source_as_of=contract.iso_ms(CREATED+1000),
                      source_sequence=(CREATED+1000)*10+1,valid_until=contract.iso_ms(CREATED+91000))
        second['proof']['episode_first_ms']=CREATED+1000
        second=contract.validate(second)
        state,_=ready(first)
        state=execution.receive(state,second,now_ms=CREATED+1000)
        self.assertEqual(admission(state,first['occurrence_id'])['status'],'PROPOSED_SOFTWARE_ONLY')
        self.assertEqual(admission(state,second['occurrence_id'])['status'],'OVERLAPPING_TARGET')

    def test_same_time_different_cycles_or_unproven_order_fail_closed(self):
        for second in (plan(cycle='other-cycle'),plan(timeframe='1w',cycle='cycle-1')):
            first=plan()
            for order in ((first,second),(second,first)):
                state=execution.initial()
                for incoming in order:state=execution.receive(state,incoming,now_ms=CREATED)
                for incoming in (first,second):
                    self.assertEqual(admission(state,incoming['occurrence_id'])['status'],
                                     'OVERLAP_SOURCE_ORDER_AMBIGUOUS')

    def test_same_time_distinct_targets_need_no_fabricated_priority(self):
        first=plan();second=plan(target=101.3,cycle='unrelated-cycle')
        state,_=ready(first);state=execution.receive(state,second,now_ms=CREATED)
        for incoming in (first,second):
            self.assertEqual(admission(state,incoming['occurrence_id'])['status'],'PROPOSED_SOFTWARE_ONLY')

    def test_other_formula_family_and_other_coin_do_not_suppress_source_admission(self):
        from experimental_execution_fixtures import r2732_message
        incoming=plan()
        eth=report_plan(REPORT_FORMULAS[-1],1.)
        self.assertEqual(eth['original_target'],incoming['original_target'])
        self.assertEqual(execution.overlap_admission(incoming,[eth,r2732_message()]),
                         'FORMULA_OVERLAP_ALLOWED')
        xrp=report_plan(REPORT_FORMULAS[3],2.)
        self.assertEqual(execution.overlap_admission(xrp,[r2732_message()]),
                         'FORMULA_OVERLAP_ALLOWED')

    def test_larger_liquidity_without_tier_evidence_still_blocks(self):
        self.assertEqual(execution.overlap_admission(plan(timeframe='2w', liquidity='999999', cycle='c2'),
                                                   [plan()]), 'OVERLAPPING_TARGET')

    def test_missing_intermediate_tier_or_too_small_growth_rejected(self):
        tiers = [dict(timeframe=tf, target_price='101', liquidation_amount=amount,
                      source_side='SHORT', provider_valid=True)
                 for tf, amount in [('3d','100'),('1w','125'),('2w','156.25')]]
        for changed in (tiers[::2], [tiers[0], {**tiers[1], 'liquidation_amount':'124.99'}, tiers[2]]):
            with self.assertRaises(contract.ContractError):
                plan(timeframe='2w', liquidity='156.25', comparisons=[dict(
                    previous_target='101', previous_timeframe='3d', previous_direction=1, tiers=changed)])

    def test_request_journal_survives_restart_and_never_resends(self):
        state, cid = attempted()
        state = json.loads(json.dumps(state))
        self.assertEqual(admission(state, cid)['status'], 'ENTRY_ALREADY_ATTEMPTED')
        report = execution.maintenance(state, cid, now_ms=ARM)
        self.assertEqual([x['kind'] for x in report['needs']], ['RECONCILE_ENTRY_OUTCOME'])

    def test_changed_source_between_proposal_and_begin_blocks(self):
        state, cid = ready(); action = admission(state,cid)['action']
        state = execution.receive(state, update(plan(),'CANCEL',ARM), now_ms=ARM)
        with self.assertRaisesRegex(execution.PlanError, 'EXACT_FRESH_ENTRY'):
            execution.begin_entry(state, action, METADATA, market(), ownership(), now_ms=ARM)

    def test_rehashed_quantity_or_prices_cannot_bypass_reconstruction(self):
        state, cid = ready()
        for change in ('quantity','price'):
            action = admission(state,cid)['action']
            if change == 'quantity': action['quantity'] = '10000'
            else: action['prepared']['execution']['stop'] = '90'
            action['proposal_id'] = checksum({k:v for k,v in action.items() if k != 'proposal_id'})
            with self.assertRaisesRegex(execution.PlanError,'ENTRY_REVALIDATION_FAILED'):
                execution.begin_entry(state,action,METADATA,market(),ownership(),now_ms=ARM)

    def test_late_begin_after_lease_or_new_market_touch_blocks(self):
        state,cid=ready();action=admission(state,cid)['action']
        with self.assertRaises(execution.PlanError):
            execution.begin_entry(state,action,METADATA,market(CREATED+90000),ownership(),now_ms=CREATED+90000)
        with self.assertRaisesRegex(execution.PlanError,'ENTRY_REVALIDATION_FAILED'):
            execution.begin_entry(state,action,METADATA,market(ARM+1,'98'),ownership(),now_ms=ARM+1)

    def test_acknowledged_order_does_not_imply_fill(self):
        state,cid=attempted()
        state=execution.observe_entry(state,cid,evidence(state,cid),now_ms=ARM+1000)
        self.assertEqual(execution.maintenance(state,cid,now_ms=ARM+1000)['needs'], [])

    def test_partial_fill_racing_source_cancel_preserves_protection_and_remainder(self):
        state,cid=attempted()
        state=execution.receive(state,update(plan(),'CANCEL',ARM+100),now_ms=ARM+100)
        row=evidence(state,cid,fills=[fill()])
        state=execution.observe_entry(state,cid,row,now_ms=ARM+1000)
        report=execution.maintenance(json.loads(json.dumps(state)),cid,now_ms=ARM+1000)
        self.assertEqual([x['kind'] for x in report['needs']],
                         ['MAINTAIN_OWN_FILLED_PROTECTION','CANCEL_OWN_ENTRY_REMAINDER'])
        self.assertEqual(report['needs'][0]['confirmed_entry_quantity'],'1')
        self.assertEqual(Decimal(report['needs'][1]['remainder_quantity']),Decimal('2.33'))
        self.assertFalse(report['protection_verified'])

    def test_cancel_finality_with_racing_fill_keeps_filled_allocation(self):
        state,cid=attempted()
        state=execution.observe_entry(state,cid,evidence(state,cid,fills=[fill()]),now_ms=ARM+1000)
        state=execution.receive(state,update(plan(),'CANCEL',ARM+1100),now_ms=ARM+1100)
        final=evidence(state,cid,at=ARM+2000,status='CANCELED',
                       fills=[fill(),fill('0.5','f2',ARM+1500)])
        state=execution.observe_entry(state,cid,final,now_ms=ARM+2000)
        needs=execution.maintenance(state,cid,now_ms=ARM+2000)['needs']
        self.assertEqual([x['kind'] for x in needs],['MAINTAIN_OWN_FILLED_PROTECTION'])
        self.assertEqual(needs[0]['confirmed_entry_quantity'],'1.5')

    def test_fill_removal_overfill_unknown_owner_or_mainnet_cannot_be_adopted(self):
        state,cid=attempted()
        state=execution.observe_entry(state,cid,evidence(state,cid,fills=[fill()]),now_ms=ARM+1000)
        for change in ({'fills':[]},{'fills':[fill('99')]},{'order_id':'42'},
                       {'environment':'mainnet'},{'history_complete':False},{'request_id':'x'*64}):
            row=evidence(state,cid,at=ARM+2000,fills=[fill()]);row.update(change)
            with self.assertRaises(execution.PlanError):
                execution.observe_entry(state,cid,row,now_ms=ARM+2000)

    def test_fill_limit_and_terminal_rejection_invariants(self):
        state,cid=attempted()
        for row in (evidence(state,cid,status='REJECTED',fills=[fill()]),
                    evidence(state,cid,status='FILLED',fills=[fill()]),
                    evidence(state,cid,fills=[{**fill(),'price':'99'}])):
            with self.assertRaises(execution.PlanError):
                execution.observe_entry(state,cid,row,now_ms=ARM+1000)

    def test_final_evidence_cannot_reopen_or_gain_fills(self):
        state,cid=attempted()
        state=execution.observe_entry(state,cid,evidence(state,cid,status='CANCELED',fills=[fill()]),now_ms=ARM+1000)
        for row in (evidence(state,cid,at=ARM+2000,status='OPEN',fills=[fill()]),
                    evidence(state,cid,at=ARM+2000,status='CANCELED',fills=[fill(),fill('1','f2',ARM+1500)])):
            with self.assertRaisesRegex(execution.PlanError,'FINAL_ENTRY_EVIDENCE_CHANGED'):
                execution.observe_entry(state,cid,row,now_ms=ARM+2000)

    def test_full_fill_maintained_after_source_expiry(self):
        state,cid=attempted();quantity=state['records'][cid]['request']['action']['quantity']
        state=execution.observe_entry(state,cid,evidence(state,cid,status='FILLED',fills=[fill(quantity)]),now_ms=ARM+1000)
        report=execution.maintenance(state,cid,now_ms=ARM+86400000)
        self.assertEqual([x['kind'] for x in report['needs']],['MAINTAIN_OWN_FILLED_PROTECTION'])
        self.assertEqual(report['state']['records'][cid]['cancel_reason'],'SOURCE_PLAN_EXPIRED')

    def test_short_maxpain_routes_only_to_short_account(self):
        value=plan(target=99.,direction='SHORT');state,cid=ready(value)
        result=admission(state,cid,market=market(price='100'),ownership=ownership(role='short_account'))
        self.assertEqual(result['status'],'PROPOSED_SOFTWARE_ONLY')
        self.assertEqual(result['action']['prepared']['execution']['entry'],'102')
        self.assertEqual(admission(state,cid)['status'],'SHARED_MARKET_PREDECESSOR_NOT_FINAL')

    def test_report_all_five_formula_exact_distance_boundaries(self):
        for spec in REPORT_FORMULAS:
            for side in spec[-1]:
                for distance,valid in ((spec[2],True),(spec[3]-0.000001,True),
                                       (spec[2]-0.000001,False),(spec[3],False)):
                    with self.subTest(rule=spec[0],side=side,distance=distance):
                        value=report_plan(spec,distance,side)
                        if valid:self.assertEqual(contract.validate(value),value)
                        else:
                            with self.assertRaisesRegex(contract.ContractError,'MAXPAIN_DISTANCE'):
                                contract.validate(value)

    def test_report_allowed_timeframes_and_direction_separate_from_names(self):
        for spec in REPORT_FORMULAS:
            for side in ('LONG','SHORT'):
                for timeframe in execution.TIMEFRAMES:
                    value=report_plan(spec,(spec[2]+spec[3])/2,side,timeframe)
                    with self.subTest(rule=spec[0],side=side,timeframe=timeframe):
                        if side in spec[-1] and timeframe in spec[7]:
                            self.assertEqual(contract.validate(value),value)
                        else:
                            with self.assertRaises(contract.ContractError):contract.validate(value)

    def test_report_original_target_and_halfway_take_are_distinct(self):
        for spec in REPORT_FORMULAS:
            for side in spec[-1]:
                value=contract.validate(report_plan(spec, spec[2], side))
                source=Decimal(value['proof']['source_price']);target=Decimal(value['original_target'])
                self.assertEqual(Decimal(value['entry']),source-Decimal(str(spec[4]))*(target-source))
                self.assertEqual(Decimal(value['stop']),source-5*(target-source))
                self.assertEqual(Decimal(value['take_profit']),source+Decimal(str(spec[5]))*(target-source))
                state=execution.receive(execution.initial(),value,now_ms=CREATED)
                cid=value['occurrence_id']
                sample=dict(environment='testnet',symbol=spec[1],at_ms=ARM,mark_price=value['take_profit'])
                state=execution.observe_market(state,cid,sample,now_ms=ARM)
                if spec[5]==.5:
                    self.assertIsNone(state['records'][cid]['cancel_reason'])
                else:
                    self.assertEqual(state['records'][cid]['cancel_reason'],'TESTNET_TARGET_SAFETY_CANCEL_REMAINDER')

    def test_report_only_sol_requires_prior_24h_target_range(self):
        sol=report_plan(REPORT_FORMULAS[0],1.)
        sol['proof']['range24_high']='100.99'
        with self.assertRaisesRegex(contract.ContractError,'MAXPAIN_RANGE24'):contract.validate(sol)
        for spec in REPORT_FORMULAS[1:]:
            value=report_plan(spec,spec[2])
            self.assertIsNone(contract.validate(value)['proof']['range24_low'])
            value['proof'].update(range24_low='90',range24_high='110')
            with self.assertRaisesRegex(contract.ContractError,'MAXPAIN_RANGE24'):contract.validate(value)

    def test_report_source_and_pending_lifetime_do_not_silently_change(self):
        for spec in REPORT_FORMULAS:
            value=contract.validate(report_plan(spec,spec[2]))
            self.assertEqual(contract.moment_ms(value['expires_at'])-contract.moment_ms(value['arm_at']),86400000)
            self.assertEqual(value['policy']['source_price'],'HYPERLIQUID_'+spec[1]+'_PERPETUAL_TRADE_1M')
            wrong=deepcopy(value);wrong['policy']['source_price']='BINANCE_SPOT_'+spec[1]+'USDT_TRADE_1M'
            with self.assertRaises(contract.ContractError):contract.validate(wrong)
            wrong=deepcopy(value);wrong['expires_at']=contract.iso_ms(ARM+86400001)
            with self.assertRaisesRegex(contract.ContractError,'MAXPAIN_TIMES'):contract.validate(wrong)

    def test_missing_liquidity_is_allowed_for_initial_nonoverlap_each_formula(self):
        for spec in REPORT_FORMULAS:
            value=report_plan(spec,spec[2]);value['proof']['liquidation_amount']=None
            state=execution.receive(execution.initial(),value,now_ms=CREATED)
            state=json.loads(json.dumps(state))
            self.assertIsNone(state['records'][value['occurrence_id']]['plan']['proof']['liquidation_amount'])
            result=execution.entry_admission(state,value['occurrence_id'],
                {'universe':[{'name':spec[1],'szDecimals':2}]},
                dict(environment='testnet',symbol=spec[1],at_ms=ARM,mark_price='100'),
                dict(account_role='long_account',symbol=spec[1],all_prior_cards_final=True,unresolved_request=False),
                now_ms=ARM)
            self.assertEqual(result['status'],'PROPOSED_SOFTWARE_ONLY')

    def test_missing_liquidity_cannot_supply_overlap_exception(self):
        first=plan();second=plan(timeframe='1w')
        second['proof']['liquidation_amount']=None
        self.assertEqual(execution.overlap_admission(second,[first]),'OVERLAPPING_TARGET')
        tiers=[dict(timeframe=tf,target_price='101',liquidation_amount=amount,
                    source_side='SHORT',provider_valid=True)
               for tf,amount in [('3d','100'),('1w','125')]]
        second['proof']['cluster_comparisons']=[dict(previous_target='101',previous_timeframe='3d',
                                                    previous_direction=1,tiers=tiers)]
        with self.assertRaises(contract.ContractError):execution.overlap_admission(second,[first])

    def test_growth_uses_complete_current_tiers_not_missing_historical_amount(self):
        first=plan();first['proof']['liquidation_amount']=None
        tiers=[dict(timeframe=tf,target_price='101',liquidation_amount=amount,
                    source_side='SHORT',provider_valid=True)
               for tf,amount in [('3d','100'),('1w','125')]]
        second=plan(timeframe='1w',liquidity='125',comparisons=[dict(
            previous_target='101',previous_timeframe='3d',previous_direction=1,tiers=tiers)])
        self.assertEqual(execution.overlap_admission(second,[first]),'FORMULA_OVERLAP_ALLOWED')
        for index in (0,1):
            missing=deepcopy(second)
            missing['proof']['cluster_comparisons'][0]['tiers'][index]['liquidation_amount']=None
            with self.assertRaises(contract.ContractError):execution.overlap_admission(missing,[first])


class MaxPainExitLifecycleTests(unittest.TestCase):
    def entered(self, quantity='1'):
        state, cid = attempted()
        state = execution.observe_entry(state, cid,
            evidence(state, cid, status='CANCELED', fills=[fill(quantity)]), now_ms=ARM+1000)
        return state, cid

    def reserve(self, state, cid, kind='STOP', quantity='1', at=ARM+1100):
        state = execution.begin_exit(state, cid, kind, quantity, now_ms=at)
        return state, next(reversed(state['records'][cid]['exits']))

    def observation(self, rid, oid='42', status='OPEN', fills=None, at=ARM+1200):
        return dict(environment='testnet', account_role='long_account', symbol='HYPE', side='SELL',
            request_id=rid, order_id=oid, at_ms=at, status=status, history_complete=True,
            reduce_only=True, fills=fills or [])

    def exit_fill(self, quantity='1', identity='xf1', at=ARM+1150):
        return dict(fill_id=identity, quantity=quantity, price='95', at_ms=at)

    def final(self, at=ARM+2000, orders=None):
        return dict(environment='testnet', account_role='long_account', symbol='HYPE', at_ms=at,
            history_complete=True, orders_complete=True, position_complete=True,
            position_quantity='0', open_order_ids=[], terminal_order_ids=orders or ['41','42'])

    def test_partial_exit_protects_only_remaining_after_cancel_and_restart(self):
        state, cid = self.entered()
        state, rid = self.reserve(state, cid)
        row = self.observation(rid, fills=[self.exit_fill('.4')])
        state = execution.observe_exit(state, cid, rid, row, now_ms=ARM+1200)
        state = execution.receive(state, update(plan(),'CANCEL',ARM+1300),now_ms=ARM+1300)
        state = json.loads(json.dumps(state))
        report = execution.maintenance(state,cid,now_ms=ARM+1300)
        self.assertEqual(report['needs'][0]['confirmed_remaining_quantity'],'0.6')
        self.assertFalse(report['finality_verified'])

    def test_fill_cannot_be_reassigned_between_owned_occurrences(self):
        state,cid=self.entered()
        second=plan(target=101.203,cycle='another-occurrence')
        state=execution.receive(state,second,now_ms=CREATED)
        second_id=second['occurrence_id']
        action=admission(state,second_id)['action']
        state=execution.begin_entry(state,action,METADATA,market(),ownership(),now_ms=ARM)
        sample=evidence(state,second_id,status='CANCELED',fills=[fill()])
        sample['order_id']='99';sample['fills'][0]['price']=action['prepared']['execution']['entry']
        with self.assertRaisesRegex(execution.PlanError,'FILL_ALREADY_OWNED'):
            execution.observe_entry(state,second_id,sample,now_ms=ARM+1000)

    def test_entry_and_exit_replay_do_not_change_revision(self):
        state,cid=self.entered()
        state=execution.observe_entry(state,cid,state['records'][cid]['entry_evidence'],now_ms=ARM+1100)
        state,rid=self.reserve(state,cid)
        row=self.observation(rid,fills=[self.exit_fill('.4')])
        state=execution.observe_exit(state,cid,rid,row,now_ms=ARM+1200)
        self.assertEqual(execution.observe_exit(json.loads(json.dumps(state)),cid,rid,row,now_ms=ARM+1300),state)

    def test_exit_fill_then_sibling_cancel_then_flat_finality(self):
        state,cid=self.entered();state,stop=self.reserve(state,cid)
        state=execution.observe_exit(state,cid,stop,self.observation(stop),now_ms=ARM+1200)
        state,take=self.reserve(state,cid,'TAKE_PROFIT',at=ARM+1300)
        state=execution.observe_exit(state,cid,take,self.observation(take,'43',at=ARM+1400),now_ms=ARM+1400)
        state=execution.observe_exit(state,cid,stop,self.observation(stop,status='FILLED',
            fills=[self.exit_fill(at=ARM+1500)],at=ARM+1600),now_ms=ARM+1600)
        needs=execution.maintenance(state,cid,now_ms=ARM+1600)['needs']
        self.assertEqual([n['kind'] for n in needs],['CANCEL_OWN_EXIT_REMAINDER','RECONCILE_ALLOCATION_FINALITY'])
        with self.assertRaisesRegex(execution.PlanError,'OWN_ALLOCATION_NOT_FINAL'):
            execution.reconcile_finality(state,cid,self.final(orders=['41','42','43']),now_ms=ARM+2000)
        state=execution.observe_exit(state,cid,take,self.observation(take,'43','CANCELED',at=ARM+1700),now_ms=ARM+1700)
        final=self.final(orders=['41','42','43'])
        state=execution.reconcile_finality(state,cid,final,now_ms=ARM+2000)
        report=execution.maintenance(json.loads(json.dumps(state)),cid,now_ms=ARM+2100)
        self.assertTrue(report['finality_verified']);self.assertEqual(report['needs'],[])
        self.assertEqual(execution.reconcile_finality(state,cid,final,now_ms=ARM+2200),state)

    def test_canceled_exit_with_racing_partial_fill_replacement_exact_remainder(self):
        state,cid=self.entered();state,rid=self.reserve(state,cid)
        row=self.observation(rid,status='CANCELED',fills=[self.exit_fill('.4')])
        state=execution.observe_exit(state,cid,rid,row,now_ms=ARM+1200)
        state,new=self.reserve(state,cid,quantity='.6',at=ARM+1300)
        self.assertNotEqual(rid,new)
        self.assertEqual(state['records'][cid]['exits'][new]['quantity'],'.6')

    def test_reused_order_entry_or_exit_ownership_rejected(self):
        state,cid=self.entered();state,rid=self.reserve(state,cid)
        with self.assertRaisesRegex(execution.PlanError,'ORDER_ALREADY_OWNED'):
            execution.observe_exit(state,cid,rid,self.observation(rid,'41'),now_ms=ARM+1200)
        state=execution.observe_exit(state,cid,rid,self.observation(rid),now_ms=ARM+1200)
        state,second=self.reserve(state,cid,'TAKE_PROFIT',at=ARM+1300)
        with self.assertRaisesRegex(execution.PlanError,'ORDER_ALREADY_OWNED'):
            execution.observe_exit(state,cid,second,self.observation(second,at=ARM+1400),now_ms=ARM+1400)

    def test_no_exit_before_attempt_or_unknown_account_or_incomplete_evidence(self):
        state,cid=self.entered();state,rid=self.reserve(state,cid)
        changes=({'account_role':'short_account'},{'side':'BUY'},{'history_complete':False},
                 {'environment':'mainnet'},{'reduce_only':False},
                 {'fills':[self.exit_fill(at=ARM+1000)]})
        for change in changes:
            row=self.observation(rid);row.update(change)
            with self.subTest(change=change),self.assertRaises(execution.PlanError):
                execution.observe_exit(state,cid,rid,row,now_ms=ARM+1200)

    def test_exit_final_history_cannot_reopen_or_lose_fills(self):
        state,cid=self.entered();state,rid=self.reserve(state,cid)
        row=self.observation(rid,status='CANCELED',fills=[self.exit_fill('.4')])
        state=execution.observe_exit(state,cid,rid,row,now_ms=ARM+1200)
        for change in ({'fills':[]},{'status':'OPEN'}):
            newer=deepcopy(row);newer.update(at_ms=ARM+1300,**change)
            with self.assertRaises(execution.PlanError):
                execution.observe_exit(state,cid,rid,newer,now_ms=ARM+1300)

    def test_replayed_fill_ids_and_overexit_are_rejected(self):
        state,cid=self.entered();state,stop=self.reserve(state,cid)
        state=execution.observe_exit(state,cid,stop,self.observation(stop),now_ms=ARM+1200)
        state,take=self.reserve(state,cid,'TAKE_PROFIT',at=ARM+1300)
        state=execution.observe_exit(state,cid,take,self.observation(take,'43',at=ARM+1400),now_ms=ARM+1400)
        state=execution.observe_exit(state,cid,stop,self.observation(stop,status='FILLED',
            fills=[self.exit_fill(at=ARM+1500)],at=ARM+1600),now_ms=ARM+1600)
        for identity in ('xf1','xf2'):
            with self.assertRaises(execution.PlanError):
                execution.observe_exit(state,cid,take,self.observation(take,'43','FILLED',
                    [self.exit_fill(identity=identity,at=ARM+1700)],at=ARM+1800),now_ms=ARM+1800)

    def test_lost_exit_reply_blocks_replacement_and_cannot_be_final(self):
        state,cid=self.entered();state,rid=self.reserve(state,cid)
        with self.assertRaisesRegex(execution.PlanError,'FRESH_RECONCILED_EXIT_ALLOCATION'):
            self.reserve(state,cid,at=ARM+1200)
        self.assertEqual(execution.maintenance(state,cid,now_ms=ARM+1200)['needs'][-1]['kind'],
                         'RECONCILE_EXIT_OUTCOME')

    def test_stale_remaining_evidence_cannot_authorize_an_exit(self):
        state,cid=self.entered()
        with self.assertRaisesRegex(execution.PlanError,'FRESH_RECONCILED_EXIT_ALLOCATION'):
            self.reserve(state,cid,at=ARM+16001)
        row=state['records'][cid]['entry_evidence'];row=deepcopy(row);row['at_ms']=ARM+16001
        state=execution.observe_entry(state,cid,row,now_ms=ARM+16001)
        self.reserve(state,cid,at=ARM+16002)

    def test_zero_entry_rejection_requires_complete_flat_finality(self):
        state,cid=attempted()
        state=execution.observe_entry(state,cid,evidence(state,cid,status='REJECTED'),now_ms=ARM+1000)
        for change in ({'open_order_ids':['99']},{'position_quantity':'1'},{'history_complete':False},
                       {'orders_complete':False},{'position_complete':False},{'terminal_order_ids':[]}):
            sample=self.final(orders=['41']);sample.update(change)
            with self.subTest(change=change),self.assertRaises(execution.PlanError):
                execution.reconcile_finality(state,cid,sample,now_ms=ARM+2000)
        state=execution.reconcile_finality(state,cid,self.final(orders=['41']),now_ms=ARM+2000)
        self.assertTrue(execution.maintenance(state,cid,now_ms=ARM+2000)['finality_verified'])

    def test_finality_cannot_precede_latest_fill_or_be_stale(self):
        state,cid=self.entered();state,rid=self.reserve(state,cid)
        state=execution.observe_exit(state,cid,rid,self.observation(rid,status='FILLED',
            fills=[self.exit_fill()]),now_ms=ARM+1200)
        for at,now in ((ARM+1100,ARM+2000),(ARM+2000,ARM+17001)):
            with self.assertRaises(execution.PlanError):
                execution.reconcile_finality(state,cid,self.final(at),now_ms=now)


if __name__ == '__main__':
    unittest.main()
