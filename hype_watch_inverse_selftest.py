"""Synthetic boundary cases; frozen research parity is tested separately."""
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
import json
import os
import unittest

import alert_delivery_policy as policy
import approved_alert_contract as contract
import selected_six_formula_source as source
import selected_six_formula_signal as rules
import selected_six_formula_store as store

AT=1_794_700_800_000

def stamp(n): return datetime.fromtimestamp(n/1000,timezone.utc).isoformat()

def joined_capture(*, share=45., snapshot=7, wave='existing-btc-parent-17', usable=None, ready=None):
    usable=AT+1000 if usable is None else usable
    ready=usable+1000 if ready is None else ready
    observed=usable-1000
    entry=rules.next_full_minute_ms(usable)
    common=dict(consumer_version=source.HYPE_CONSUMER,population_version=source.HYPE_POPULATION,
        snapshot_set_id=snapshot,symbol='HYPE',bundle_sha256='a'*64,parent_payload_sha256='b'*64,
        usable_from_utc=stamp(usable))
    raw=dict(timeframe='24h',current_price=100,short_max_pain=102,long_max_pain=98,
        price_source='hyperliquid',price_pair='HYPE-PERP',source_observed_at_utc=stamp(observed),
        price_fetched_at_utc=stamp(observed),short_liquidation_amount=share,long_liquidation_amount=100-share)
    slots={}
    for side,score in [('SHORT',60),('LONG',50)]:
        slots[side]=dict(timeframe='24h',source_side=side,status='SCORED',selected=side=='SHORT',
            score=score,target_price=102 if side=='SHORT' else 98,
            components=dict(directional_alignment=score,target_proximity=0,cluster_confidence=0,relative_gap=0),
            consensus_hits=1,consensus_total=1,cluster_count=1,
            near_amount=share if side=='SHORT' else 100-share,
            far_amount=100-share if side=='SHORT' else share,
            near_share_pct=share if side=='SHORT' else 100-share)
    observation=dict(common,observed_at_utc=stamp(observed),source_time_errors=[],
        sources={'maxpain_operational_rows':[raw]},maxpain_slots=list(slots.values()))
    tf_payload=dict(common,version=source.HYPE_EVALUATION,
        feature_version='watch-captured-selected-timeframe-liquidity-and-score-difference-v1',
        selection_version='watch-selected-timeframe-first-arm-unknown-blocks-v1',
        timeframe_provenance={'24h':dict(source_row=deepcopy(raw),slots_by_source_side=deepcopy(slots),
            selection_status='SELECTED',selected_source_side='SHORT',selected_base_direction='LONG')},
        evaluations=[dict(candidate_key=source.HYPE_CANDIDATE_KEY,timeframe='24h',base_direction='LONG',
            match_status='MATCH' if 40<=share<50 else 'NO_MATCH',selection_status='SELECTED',analysis_direction='SHORT')])
    mp=dict(common,version=source.HYPE_MEASUREMENT,entry_version='watch-next-full-minute-route-open-v1',
        outcome_method_version='ordered-first-touch-v7',entry_time_utc=stamp(entry),
        price_route='HYPERLIQUID_HYPE_PERP_TRADE_1M',status='OPEN',
        membership=dict(btc_parent_movement_id=wave,membership_status='LIVE',
            episode_policy_version=source.HYPE_PARENT_POLICY,decision_time_utc=stamp(usable)))
    return dict(intake=dict(common,intake_status='ACCEPTED',observed_at_utc=stamp(observed),
            source_available_at_utc=stamp(usable),source_created_at_utc=stamp(usable)),
        timeframe=dict(consumer_version=source.HYPE_CONSUMER,snapshot_set_id=snapshot,symbol='HYPE',
            evaluation_version=source.HYPE_EVALUATION,status='READY',payload=tf_payload,
            created_at_utc=stamp(ready),updated_at_utc=stamp(ready),usable_from_utc=stamp(usable)),
        measurement=dict(consumer_version=source.HYPE_CONSUMER,snapshot_set_id=snapshot,symbol='HYPE',
            measurement_version=source.HYPE_MEASUREMENT,price_route=mp['price_route'],
            usable_from_utc=stamp(usable),entry_time_utc=stamp(entry),payload=mp),
        observation=observation)


def mapped(row=None, now=AT+65_000):
    return source.map_hype_watch_row(row or joined_capture(),now)


def calculation_snapshot(row=None, now=AT+65_000):
    record=mapped(row,now)
    assert record['arm']=='MATCH',record
    snapshot=record['candidate']['snapshot']
    ref=rules.next_full_minute_ms(snapshot['received_at_ms'])
    snapshot['reference']=source.dated('100',ref,now,snapshot['valid_until_ms'],
        venue='HYPERLIQUID_PERPETUAL',reference_kind='NEXT_FULL_MINUTE_OPEN')
    return snapshot

class HypeSourceTests(unittest.TestCase):
    def test_exact_share_boundaries_without_oi_cvd(self):
        for value,expected in [(39.999,'NO_MATCH'),(40.,'MATCH'),(49.999,'MATCH'),(50.,'NO_MATCH')]:
            with self.subTest(value=value): self.assertEqual(mapped(joined_capture(share=value))['arm'],expected)

    def test_missing_amount_is_unknown_not_zero(self):
        row=joined_capture()
        row['observation']['sources']['maxpain_operational_rows'][0]['short_liquidation_amount']=None
        row['timeframe']['payload']['timeframe_provenance']['24h']['source_row']['short_liquidation_amount']=None
        result=mapped(row)
        self.assertEqual(result['arm'],'UNKNOWN')
        self.assertEqual(result['wave'],'existing-btc-parent-17')

    def test_capture_hash_clock_and_identity_guards(self):
        mutations=[
            lambda x:x['measurement']['payload'].update(bundle_sha256='c'*64),
            lambda x:x['timeframe']['payload'].update(symbol='XRP'),
            lambda x:x['measurement']['payload'].update(price_route='BINANCE_SPOT_TRADE_1M'),
            lambda x:x['measurement']['payload']['membership'].update(decision_time_utc=stamp(AT)),
            lambda x:x['timeframe'].update(status='ERROR'),
            lambda x:x['timeframe'].update(updated_at_utc=stamp(AT+70_000)),
        ]
        for mutate in mutations:
            row=joined_capture(); mutate(row)
            self.assertEqual(mapped(row)['arm'],'UNKNOWN')

    def test_real_parent_required_not_snapshot_fallback(self):
        row=joined_capture(wave=None)
        result=mapped(row)
        self.assertEqual(result['arm'],'UNKNOWN'); self.assertIsNone(result['wave'])

    def test_selected_scorer_and_component_validation(self):
        for mutate in ('selected','components','ratio'):
            row=joined_capture()
            for slots in (row['observation']['maxpain_slots'],list(row['timeframe']['payload']['timeframe_provenance']['24h']['slots_by_source_side'].values())):
                slot=next(x for x in slots if x['source_side']=='SHORT')
                if mutate=='selected':slot['selected']=False
                elif mutate=='components':slot['components']['relative_gap']=10
                else:slot['near_share_pct']=41
            self.assertEqual(mapped(row)['arm'],'UNKNOWN')

    def test_raw_short_base_long_execution_short_and_fixed_geometry(self):
        result=rules.evaluate_hype_watch_inverse(calculation_snapshot(),AT+65_000)
        self.assertTrue(result['signal']); d=result['decision']
        self.assertEqual((d.formula_id,d.symbol,d.source_direction,d.direction),
            (rules.HYPE_WATCH_INVERSE,'HYPE','LONG','SHORT'))
        self.assertEqual((d.entry_price,d.stop_loss,d.take_profit),('100','102.00','98.00'))
        self.assertEqual(d.holding_time_limit_ms,86_400_000)
        self.assertEqual(d.pending_entry_expires_at_ms,AT+120_000)
        self.assertIsNone(d.cancel_target_price)
        self.assertNotIn('HYPE_ROW71205',d.event_id)
        self.assertIn('capture_guard_evidence',json.loads(d.frozen_inputs_json))

    def test_reference_and_entry_clock_never_moves_to_poll(self):
        row=joined_capture(ready=AT+32_000)
        snapshot=calculation_snapshot(row)
        self.assertEqual(snapshot['reference']['observed_at_ms'],AT+60_000)
        snapshot['reference']['observed_at_ms']=AT
        self.assertFalse(rules.evaluate_hype_watch_inverse(snapshot,AT+65_000)['valid'])
        late=calculation_snapshot()
        late['valid_until_ms']=AT+999_000
        for x in late['inputs'].values(): x['valid_until_ms']=AT+999_000
        late['reference']['valid_until_ms']=AT+999_000
        self.assertEqual(rules.evaluate_hype_watch_inverse(late,AT+120_000)['reason'],'original_entry_minute_expired')

    def test_late_ready_match_uses_original_ready_minute_without_reselection(self):
        now=AT+source.SOURCE_WINDOW_MS+65_000
        first=mapped(joined_capture(snapshot=7,ready=now-1000),now)
        self.assertEqual(first['arm'],'MATCH')
        snapshot=first['candidate']['snapshot']
        original_reference=rules.next_full_minute_ms(now-1000)
        self.assertEqual(snapshot['received_at_ms'],now-1000)
        self.assertEqual(snapshot['valid_until_ms'],original_reference+60_000-1)
        state=store._initial(AT-1); state['hype_activated_ms']=AT-1
        self.assertEqual(source.select_hype_wave_candidates(state,[first],now),0)
        self.assertEqual(state['episodes'],{})
        self.assertEqual(source.select_hype_wave_candidates(state,[first],original_reference+5000),1)
        # Arriving after that original minute cannot move the first entry to
        # a later capture in the same source wave, including after restart.
        expired=original_reference+60_000
        later=mapped(joined_capture(snapshot=9,usable=expired-2000,ready=expired-1000),expired)
        self.assertEqual(later['arm'],'MATCH')
        state=store._initial(AT-1); state['hype_activated_ms']=AT-1
        self.assertEqual(source.select_hype_wave_candidates(state,[later,first],expired),0)
        outcome=next(iter(state['episodes'].values()))
        self.assertEqual(outcome['representative_snapshot_set_id'],7)
        self.assertEqual(outcome['status'],'ORIGINAL_ENTRY_WINDOW_EXPIRED')
        self.assertEqual(state['candidates'],{})
        restarted=json.loads(json.dumps(state))
        self.assertEqual(source.select_hype_wave_candidates(restarted,[later],expired+1),0)
        self.assertEqual(restarted['episodes'],state['episodes'])
        self.assertEqual(restarted['intents'],[])

    def test_frozen_snapshot_1294_keeps_actual_delayed_availability_and_prices(self):
        fixture=json.loads((Path(__file__).parent/'docs/formula-activation-20261115/fixtures/hype_snapshot_1294.json').read_text())
        row,expected=fixture['joined_capture'],fixture['expected']
        reference=source.ms(expected['reference_at_utc']); now=reference+5000
        record=source.map_hype_watch_row(row,now)
        self.assertEqual((record['arm'],record['snapshot_set_id']),('MATCH',1294))
        snapshot=record['candidate']['snapshot']
        actual_available=max(source.ms(row['intake']['usable_from_utc']),
            source.ms(row['timeframe']['created_at_utc']),source.ms(row['timeframe']['updated_at_utc']))
        self.assertEqual(snapshot['received_at_ms'],actual_available)
        self.assertGreater(actual_available-snapshot['observed_at_ms'],source.SOURCE_WINDOW_MS)
        self.assertEqual(rules.next_full_minute_ms(actual_available),reference)
        self.assertEqual(snapshot['valid_until_ms'],reference+60_000-1)
        snapshot['reference']=source.dated(expected['entry'],reference,now,snapshot['valid_until_ms'],
            venue='HYPERLIQUID_PERPETUAL',reference_kind='NEXT_FULL_MINUTE_OPEN')
        result=rules.evaluate_hype_watch_inverse(snapshot,now)
        self.assertTrue(result['signal'],result)
        decision=result['decision']
        self.assertEqual((decision.entry_price,decision.stop_loss,decision.take_profit),
            (expected['entry'],expected['stop'],expected['take_profit']))
        self.assertEqual(decision.holding_time_limit_ms,expected['holding_time_limit_ms'])
        self.assertEqual(decision.reference_at_ms,reference)
        self.assertFalse(rules.evaluate_hype_watch_inverse(snapshot,reference+60_000)['signal'])

    def test_pending_earlier_capture_can_finish_before_original_entry_minute(self):
        first=joined_capture(snapshot=7,ready=AT+50_000)
        later=joined_capture(snapshot=9,usable=AT+3000,ready=AT+18_000)
        state=store._initial(AT-1); state['hype_activated_ms']=AT-1
        early=[source.map_hype_watch_row(row,AT+20_000) for row in (first,later)]
        self.assertEqual([r['arm'] for r in early],['UNKNOWN','MATCH'])
        self.assertEqual(source.select_hype_wave_candidates(state,early,AT+20_000),0)
        self.assertEqual(state['episodes'],{})
        ready=[source.map_hype_watch_row(row,AT+60_000) for row in (first,later)]
        self.assertEqual(source.select_hype_wave_candidates(state,ready,AT+60_000),1)
        outcome=next(iter(state['episodes'].values()))
        self.assertEqual(outcome['representative_snapshot_set_id'],7)
        self.assertEqual(outcome['status'],'SELECTED')

    def test_ended_parent_keeps_capacity_decision_through_original_entry_window(self):
        class EndedParent:
            def execute(self,*args):
                class Rows:
                    def fetchall(self):
                        return [dict(btc_parent_movement_id='existing-btc-parent-17',
                                     end_time_utc=stamp(AT-86_400_000))]
                return Rows()
        state=store._initial(AT-1); state['hype_activated_ms']=AT-1
        state['active']=[dict(rule_id=rules.HYPE_WATCH_INVERSE,symbol='HYPE')]
        self.assertEqual(source.select_hype_wave_candidates(state,[mapped()],AT+65_000),0)
        self.assertEqual(next(iter(state['episodes'].values()))['status'],'CAPACITY_REJECTED')
        state['active']=[]
        self.assertEqual(source.prune_hype_waves(EndedParent(),state,AT+70_000),0)
        restarted=json.loads(json.dumps(state))
        self.assertEqual(source.select_hype_wave_candidates(restarted,[mapped()],AT+70_000),0)
        # prune_hype_waves avoids a query for a single wave. Add a second,
        # unrelated record so the expiry proof is exercised here.
        restarted['episodes'][rules.HYPE_WATCH_INVERSE+':wave:other']=dict(identity='other')
        self.assertEqual(source.prune_hype_waves(EndedParent(),restarted,AT+70_000),0)
        self.assertEqual(source.prune_hype_waves(EndedParent(),restarted,AT+120_000),1)

    def test_early_or_equal_unknown_blocks_but_later_unknown_does_not(self):
        for delta,blocked in [(-1,True),(0,True),(1,False)]:
            state=store._initial(AT-1); state['hype_activated_ms']=AT-1; match=mapped()
            unknown=dict(match,arm='UNKNOWN',usable_ms=match['usable_ms']+delta,snapshot_set_id=1,candidate=None)
            with patch.object(policy,'selected_rule_enabled',return_value=True):
                created=source.select_hype_wave_candidates(state,[match,unknown],AT+65_000)
            self.assertEqual(created,0 if blocked else 1)
            outcome=next(iter(state['episodes'].values()))
            self.assertEqual(outcome['status'],'UNKNOWN_BLOCKED' if blocked else 'SELECTED')

    def test_first_match_id_order_and_capacity_rejection_consumes_wave(self):
        state=store._initial(AT-1); state['hype_activated_ms']=AT-1
        rows=[mapped(joined_capture(snapshot=i)) for i in (9,7)]
        state['active']=[dict(rule_id=rules.HYPE_WATCH_INVERSE,symbol='HYPE')]
        with patch.object(policy,'selected_rule_enabled',return_value=True):
            self.assertEqual(source.select_hype_wave_candidates(state,rows,AT+65_000),0)
            outcome=next(iter(state['episodes'].values()))
            self.assertEqual(outcome['representative_snapshot_set_id'],7)
            self.assertEqual(outcome['status'],'CAPACITY_REJECTED')
            state['active']=[]
            restarted=json.loads(json.dumps(state))
            self.assertEqual(source.select_hype_wave_candidates(restarted,rows,AT+66_000),0)
            self.assertEqual(restarted['candidates'],{})

    def test_pre_activation_first_match_cannot_be_replaced_by_later_one(self):
        state=store._initial(AT+2000); state['hype_activated_ms']=AT+2000
        records=[mapped(),mapped(joined_capture(snapshot=9,usable=AT+3000))]
        with patch.object(policy,'selected_rule_enabled',return_value=True):
            self.assertEqual(source.select_hype_wave_candidates(state,records,AT+65_000),0)
        self.assertEqual(next(iter(state['episodes'].values()))['status'],'BEFORE_ACTIVATION')

    def test_pre_activation_intake_is_not_renewed_by_later_timeframe_readiness(self):
        row=joined_capture(snapshot=7,usable=AT+1000,ready=AT+50_000)
        state=store._initial(AT-86_400_000); state['hype_activated_ms']=AT+10_000
        record=mapped(row)
        self.assertEqual(record['arm'],'MATCH')
        self.assertGreater(record['candidate']['snapshot']['received_at_ms'],state['hype_activated_ms'])
        self.assertEqual(source.select_hype_wave_candidates(state,[record],AT+65_000),0)
        self.assertEqual(next(iter(state['episodes'].values()))['status'],'BEFORE_ACTIVATION')
        decision=rules.evaluate_hype_watch_inverse(calculation_snapshot(row),AT+65_000)['decision']
        self.assertEqual(store.approve_state(state,decision,
            dict(cycle_id='7',episode_key='existing-btc-parent-17',timeframe='24h'),AT+65_000),
            dict(created=False,reason='BEFORE_ACTIVATION'))
        self.assertEqual(state['intents'],[])

    def test_disabled_hype_issues_no_source_query(self):
        class NoQuery:
            def execute(self,*a):raise AssertionError('HYPE not active')
        state=store._initial(AT); state['hype_activated_ms']=AT
        with patch.dict(os.environ,{'ALERT_DELIVERY_PROFILE':'OFF'}):
            self.assertEqual(source.read_hype_watch(NoQuery(),state,AT),0)
        self.assertEqual(store.CONFIG_SHA256,'63a7a1478c5d800f99ba21b8ae939ac33d19fbb53c57b0d1d7d42c05c7b18988')

    def test_product_calculation_approval_and_notification_share_exact_plan(self):
        state=store._initial(AT-1); state['hype_activated_ms']=AT-1
        with patch.dict(os.environ,{'ALERT_DELIVERY_PROFILE':'ALL'}):
            self.assertEqual(source.select_hype_wave_candidates(state,[mapped()],AT+65_000),1)
            rows=[(AT+60_000,100,101,99,100)]
            self.assertEqual(source.evaluate_ready(state,rows,AT+65_000),1)
            self.assertEqual(source.evaluate_ready(state,rows,AT+66_000),0)
        intent=state['intents'][0]
        payload=contract.validate(intent['payload'])
        self.assertEqual(payload['policy']['entry_kind'],'NEXT_FULL_MINUTE_IOC')
        self.assertEqual(payload['policy']['holding_time_limit_ms'],86_400_000)
        self.assertEqual(contract.moment_ms(payload['expires_at']),AT+120_000)
        self.assertEqual(intent['expires_ms'],AT+120_000)
        self.assertEqual(payload['proof']['episode_key'],'existing-btc-parent-17')
        self.assertEqual(payload['proof']['source_config_sha256'],store.HYPE_CONFIG_SHA256)
        for payload_key,decision_key in [('entry','entry_price'),('stop','stop_loss'),('take_profit','take_profit')]:
            self.assertEqual(payload[payload_key],intent['decision'][decision_key])
            self.assertIn(payload[payload_key],intent['text'])
        self.assertEqual(len(state['intents']),1)
        self.assertLess(len(store._encode(state)),store.MAX_BYTES)
        store.prune(state,AT+120_000)
        self.assertEqual(state['intents'][0]['status'],'EXPIRED')

    def test_new_identity_activation_is_once_and_preserves_existing_state(self):
        state=store._initial(AT-86_400_000)
        with patch.dict(os.environ,{'ALERT_DELIVERY_PROFILE':'OFF'}):
            self.assertFalse(store.ensure_hype_activation(state,AT+3000))
            self.assertIsNone(state['hype_activated_ms'])
        with patch.dict(os.environ,{'ALERT_DELIVERY_PROFILE':'ALL'}):
            self.assertTrue(store.ensure_hype_activation(state,AT+3000))
            self.assertFalse(store.ensure_hype_activation(state,AT+4000))
            before=deepcopy(state)
            restarted=json.loads(json.dumps(state))
            self.assertFalse(store.ensure_hype_activation(restarted,AT+5000))
            self.assertEqual(restarted,before)
            # A fresh-looking first match from before the new activation is
            # not rescued by the old shared state activation time.
            self.assertEqual(source.select_hype_wave_candidates(state,[mapped()],AT+65_000),0)
            self.assertEqual(next(iter(state['episodes'].values()))['status'],'BEFORE_ACTIVATION')
            newer=mapped(joined_capture(wave='next-parent',usable=AT+5000))
            self.assertEqual(source.select_hype_wave_candidates(state,[newer],AT+65_000),1)
        self.assertEqual(state['activated_ms'],AT-86_400_000)

    def test_proved_closed_waves_compact_without_open_wave_rearm(self):
        state=store._initial(AT-1); state['hype_activated_ms']=AT-1
        ends={}
        class Parents:
            def execute(self,sql,args):
                policy_id,ids,cutoff=args
                assert policy_id==source.HYPE_PARENT_POLICY
                class Rows:
                    def fetchall(self):
                        return [dict(btc_parent_movement_id=w,end_time_utc=stamp(e))
                            for w,e in ends.items() if w in ids and e < int(cutoff.timestamp()*1000)]
                return Rows()
        for index in range(1000):
            now=AT+index*1_200_000+180_000
            wave='existing-parent-'+str(index)
            if index: ends['existing-parent-'+str(index-1)]=now-1_200_000
            record=mapped(joined_capture(wave=wave,usable=now-179_000),now)
            with patch.object(policy,'selected_rule_enabled',return_value=True):
                source.select_hype_wave_candidates(state,[record],now)
            source.prune_hype_waves(Parents(),state,now)
            self.assertLessEqual(len(state['episodes']),1)
        self.assertLess(len(store._encode(state)),3000)
        restarted=json.loads(json.dumps(state))
        with patch.object(policy,'selected_rule_enabled',return_value=True):
            self.assertEqual(source.select_hype_wave_candidates(restarted,[record],now+1),0)
        self.assertEqual(restarted['episodes'],state['episodes'])

if __name__=='__main__': unittest.main()
