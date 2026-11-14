"""Network-free regression of real Watch mapping and source causal reducers."""
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import unittest

import selected_six_formula_source as source
import selected_six_formula_store as store
import selected_six_formula_signal as rules
import selected_six_formula_signal_selftest as fixtures
from selected_six_formula_source_fixtures import watch_capture

AT = 1_794_657_600_000


def stamp(value):
    return datetime.fromtimestamp(value/1000, timezone.utc)


def native_event(**updates):
    row = dict(event_id=17, symbol='BTC', direction='SHORT', timeframe='48h', event_kind='ALERT',
        event_type='MAX_PAIN_ALERT', capture_stage='TELEGRAM_ALERT', delivery_status='DELIVERED',
        alert_time_utc=stamp(AT), delivered_at_utc=stamp(AT+1000), created_at=stamp(AT+1100),
        target_price=98., score=80., engine_snapshot={})
    row.update(updates)
    return row


class SourceTests(unittest.TestCase):
    def test_real_watch_shape_without_invented_oi_window_timestamp(self):
        score, decision, raw = watch_capture(AT)
        self.assertNotIn('latest_time',raw['SOL']['regime']['windows']['1h'])
        mapped = source.map_watch_capture(score,decision,raw,AT+10)
        self.assertEqual(mapped['errors'],[])
        self.assertEqual({x['formula_id'] for x in mapped['candidates']},
            {rules.SFF1361,rules.OCONF0034_13,rules.XRP_LIQUIDITY_INVERSE})
        sol = next(x['snapshot'] for x in mapped['candidates'] if x['formula_id']==rules.SFF1361)
        self.assertEqual(sol['inputs']['one_hour_price_change_fraction']['observed_at_ms'],AT)
        self.assertEqual(Decimal(sol['inputs']['one_hour_price_change_fraction']['value']),Decimal('.001'))
        self.assertNotIn('price_history_complete_24h',sol['inputs'])

    def test_pass_does_not_renew_old_positioning_and_missing_member_rejects(self):
        score,_,_ = watch_capture(AT)
        coin = deepcopy(score['coins']['SOL'])
        for name in ('price_fetched_at','oi_fetched_at'):
            coin['sources']['positioning'][name]=stamp(AT-660_000).isoformat()
        with self.assertRaises(ValueError):
            source._family(coin,'positioning','now',AT,AT+10)
        coin=deepcopy(score['coins']['SOL'])
        coin['models']['positioning']['time_families']['short']['members'][1]['available']=False
        with self.assertRaises(ValueError):
            source._family(coin,'positioning','short',AT,AT+10)

    def test_ct_real_delivery_availability_never_moves_to_poll_time(self):
        candidate=source.map_delivered_alert(native_event(),AT+120_000)
        self.assertEqual(candidate['snapshot']['received_at_ms'],AT+1100)
        state=store._initial(AT+2000)
        self.assertEqual(source._capture_candidates(state,[candidate]),0)

    def test_ct_unapproved_nonnative_and_bad_values_fail_closed(self):
        changes=[dict(symbol=s) for s in ('BNB','ZEC','ADA')]+[
            dict(capture_stage='OBSERVED'),dict(delivery_status='UNKNOWN'),dict(target_price=None),
            dict(target_price=float('nan')),dict(score=float('inf')),
            *[dict(engine_snapshot={k:None}) for k in ('inverse_analysis','archive_only','telegram_archive')],
            dict(engine_snapshot={'source_scope':'DEMO'})]
        for change in changes:
            with self.subTest(change=change),self.assertRaises((ValueError,ArithmeticError)):
                source.map_delivered_alert(native_event(**change))

    def test_xrp_episodes_are_distinct_by_timeframe(self):
        state=store._initial(AT-1)
        identities=[]
        for tf in ('48h','3d'):
            snapshot=source._base(tf,'XRP','LONG',AT,AT+1)
            source._field(snapshot,'target_price',102)
            identities.append(source._episode(state,dict(formula_id=rules.XRP_LIQUIDITY_INVERSE,
                snapshot=snapshot,metadata=dict(timeframe=tf)))[0])
        self.assertNotEqual(*identities)

    def test_forming_target_touch_suppresses_before_any_approval(self):
        current,prior=fixtures.make_ct009()
        at=fixtures.AT
        state=store._initial(at-1000)
        state['delivered_alerts']=prior
        source._capture_candidates(state,[dict(formula_id=rules.CT009,snapshot=current,
            metadata=dict(cycle_id='veto',timeframe='3d'))])
        self.assertEqual(source.evaluate_ready(state,[(at+60_000,100,101,97,100)],at+70_000),0)
        self.assertEqual(state['active'],[])
        self.assertEqual(state['intents'],[])
        self.assertEqual(state['counters']['target_reached_before_approval'],1)

    def test_midminute_oconf_ignores_earlier_low_but_honors_new_live_trade(self):
        current,prior=fixtures.make_oconf()
        at=fixtures.AT
        state=store._initial(at-1000)
        state['confirmations']=prior
        source._capture_candidates(state,[dict(formula_id=rules.OCONF0034_13,snapshot=current,
            metadata=dict(cycle_id='live',timeframe='3d'))])
        self.assertEqual(source.evaluate_ready(state,[(at,100,101,97,100)],at+10_000),1)
        self.assertEqual(source.observe_cancellations(state,[(at,100,101,97,100)],at+11_000),0)
        self.assertEqual(source.observe_cancellations(state,[(at,100,101,97,97)],at+12_000),1)

    def test_ct_compact_history_keeps_values_and_original_ids_within_bound(self):
        rows=[[str(i),'BTC',AT-i*1000,AT-i*1000+10,AT-i*1000+600_000,'48h','98',80]
              for i in range(source.MAX_HISTORY)]
        state=store._initial(AT)
        state['delivered_alerts']=rows
        self.assertLess(len(store._encode(state)),store.MAX_BYTES)
        expanded=source._delivered_history(rows)
        self.assertEqual(expanded[-1]['source_event_id'],rows[-1][0])
        self.assertEqual(expanded[-1]['inputs']['maxpain_score']['value'],80)

    def test_delayed_oconf_pins_causal_predecessor_at_24h_boundary(self):
        current,history=fixtures.make_oconf()
        at=fixtures.AT
        older=deepcopy(history[0])
        for key in ('observed_at_ms','received_at_ms','valid_until_ms'):
            shift=at-store.DAY_MS-older['observed_at_ms']
            if key=='observed_at_ms': delta=shift
            older[key]+=delta
        for field in older['inputs'].values():
            for key in ('observed_at_ms','received_at_ms','valid_until_ms'): field[key]+=delta
        state=store._initial(at-1000)
        state['confirmations']=[older]
        item=dict(formula_id=rules.OCONF0034_13,snapshot=current,metadata=dict(cycle_id='delayed',timeframe='3d'))
        source._capture_candidates(state,[item])
        pending=next(iter(state['candidates'].values()))
        self.assertEqual(pending['prior_confirmations'][0]['source_event_id'],older['source_event_id'])
        state['confirmations']=[]
        source.store.prune(state,at+10_000)
        self.assertEqual(source.evaluate_ready(state,[(at,100,101,99.9,100)],at+10_000),1)


if __name__=='__main__':
    unittest.main()
