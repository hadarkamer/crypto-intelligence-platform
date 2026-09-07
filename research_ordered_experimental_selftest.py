"""Network-free real-policy causal qualification and notification regressions."""
import asyncio
from copy import deepcopy
from datetime import timedelta
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import research_formula_ordered_store as formulas
import research_ordered_acceptance_policy as acceptance
import research_ordered_experimental as contract
import research_ordered_experimental_store as store
import research_ordered_experimental_worker as worker
import research_ordered_question_catalog as questions
import research_ordered_validation as validation
import research_ordered_validation_store as validation_store
from research_ordered_validation_selftest import START, SCOPE, item, common


def fixture(n=5, inverse=False):
    key = ':INVERSE:PRICE_OI_TOTAL_65' if inverse else ':CORE_PRICE_OI_TOTAL_65'
    c = next(c for c in validation.evidence.candidate_catalog(include_extended=True) if c['formula_id'].endswith(key))
    c = {**c,'direction_mode':c.get('research_orientation','NORMAL')}
    scope = {**SCOPE,'candidate_key':c['formula_id'],'parent_policy_version':formulas.PARENT_POLICY}
    exact = validation.binding(scope,c)
    reg = validation.freeze(scope,c,frozen_at_utc=START,acceptance_policy=acceptance.policy_for(exact,c))
    rows = [{**item(i),'episode_policy_version':formulas.PARENT_POLICY} for i in range(1,n+1)]
    windows = {r['event_id']:common(r) for r in rows}
    assessed = START+timedelta(hours=10)
    result = validation.evaluate(reg,rows,analysis_as_of_utc=assessed,source_coverage_complete=True,common_window_rows=windows)
    result['representative_entries_frozen'] = True
    result['evidence_sha256'] = validation.digest(result)
    when = assessed+timedelta(minutes=1)
    source_direction = 'SHORT' if inverse else 'LONG'
    event = {'event_id':100,'event_fingerprint':'a'*64,'event_type':'COMBINED_CONFIRMATION',
        'event_kind':'ALERT','delivery_status':'DELIVERED','source_scope':'LIVE','symbol':'BTC',
        'candidate_key':c['formula_id'],'direction':'LONG','source_direction':source_direction,
        'alert_time_utc':when,'entry_price':100.,'current_price':100.,
        'engine_snapshot':{'price_source':'binance_spot','price_pair':'BTCUSDT'},
        'decision_features':{'price_oi.aligned_score':70,'event.direction_mapping_valid':True,'event.analysis_direction':source_direction},
        'btc_parent_movement_id':'new-wave','episode_policy_version':formulas.PARENT_POLICY,
        'membership_status':'LIVE','parent_evidence_eligible':True,
        'parent_start_time_utc':when-timedelta(seconds=30),'parent_confirmed_at_utc':when-timedelta(seconds=30),
        'decision_time_utc':when,'btc_observed_close_utc':when-timedelta(milliseconds=1)}
    return scope,reg,result,event,assessed,rows,windows


class ExperimentalTests(unittest.TestCase):
    def notify(self,fixture_value=None):
        _,reg,result,event,assessed,*_ = fixture_value or fixture()
        return contract.notification(reg,result,event,evaluated_at=assessed,published_at=assessed+timedelta(seconds=1),now=event['alert_time_utc']+timedelta(seconds=1))

    def test_actual_policy_regular_and_fresh_both_pass_without_human_gate(self):
        for n,route in ((5,'REGULAR'),(3,'FRESH')):
            payload = self.notify(fixture(n))
            self.assertEqual(payload['route'],route)
            self.assertEqual(payload['metrics']['resolved_waves'],n)
            self.assertIn('ניסיוני, לא למסחר',contract.render(payload))
            self.assertEqual(payload['live_effect'],'EXPERIMENTAL_NOTIFICATION_ONLY')

    def test_one_or_two_waves_cannot_qualify(self):
        for n in (1,2):
            with self.assertRaises(ValueError):
                self.notify(fixture(n))

    def test_inverse_uses_match_direction_once(self):
        value = fixture(inverse=True)
        payload = self.notify(value)
        self.assertEqual(payload['direction'],'LONG')
        self.assertEqual(payload['orientation'],'INVERSE')
        value[3]['direction'] = 'SHORT'
        with self.assertRaises(ValueError):
            self.notify(value)

    def test_invalid_source_price_archive_demo_and_mapping_rejected(self):
        invalid = [('event_kind','DECISION_SAMPLE'),('delivery_status','IMPORTED'),('source_scope','ARCHIVE'),
            ('event_type','ORDERED_INVERSE_ANALYSIS'),('current_price',101),('entry_price',True),
            ('engine_snapshot',{'price_source':'fallback'}),
            ('engine_snapshot',{'price_source':'binance_spot','price_pair':'BTCUSDT','mode':'DEMO'}),
            ('engine_snapshot',{'price_source':'binance_spot','price_pair':'BTCUSDT','archive_run_key':'x'}),
            ('decision_features',{'price_oi.aligned_score':70,'event.direction_mapping_valid':False})]
        for key,value in invalid:
            data = fixture(); data[3][key]=value
            with self.subTest(field=key,value=value),self.assertRaises(ValueError):
                self.notify(data)

    def test_future_or_prequalification_trigger_and_stale_event_rejected(self):
        data = fixture(); assessed=data[4]
        for when in (assessed,assessed-timedelta(seconds=1),assessed+timedelta(minutes=40)):
            data[3]['alert_time_utc']=when
            with self.assertRaises(ValueError):
                self.notify(data)
        data = fixture()
        # Pass began before the alert, but actual qualification was published
        # only later: this must not backdate authorization to the pass start.
        with self.assertRaises(ValueError):
            contract.notification(data[1],data[2],data[3],evaluated_at=data[4],
                published_at=data[3]['alert_time_utc']+timedelta(seconds=1),now=data[3]['alert_time_utc']+timedelta(seconds=2))
        with self.assertRaises(ValueError):
            contract.notification(data[1],data[2],data[3],evaluated_at=data[4],published_at=data[4],now=data[3]['alert_time_utc']+timedelta(minutes=11))

    def test_training_wave_or_noncausal_parent_cannot_trigger(self):
        for key,value in [('btc_parent_movement_id','wave-1'),('membership_status','BOUNDARY_UNVERIFIED'),
                ('parent_confirmed_at_utc',START+timedelta(days=2)),('btc_observed_close_utc',START+timedelta(days=2))]:
            data = fixture(); data[3][key]=value
            with self.assertRaises(ValueError):
                self.notify(data)

    def test_changed_definition_missing_policy_conflicts_and_failed_gates(self):
        for mutate in (
            lambda d:d[1].update(acceptance_policy=None),
            lambda d:d[1]['binding'].update(source_scope='ARCHIVE'),
            lambda d:d[2].update(source_coverage_complete=False),
            lambda d:d[2].update(representative_entries_frozen=False),
            lambda d:d[2].update(representative_conflicts=['wave-1']),
            lambda d:d[2]['prospective'].update(hit_rate_pct=20),
            lambda d:d[2]['prospective']['fresh'].update(hit_rate_pct=20),
        ):
            data=fixture(); mutate(data)
            # A regular/fresh alternate must also be disabled when testing
            # a metric failure, so neither independent route can pass.
            if data[2]['prospective']['hit_rate_pct']==20 or data[2]['prospective']['fresh']['hit_rate_pct']==20:
                data[2]['prospective']['hit_rate_pct']=data[2]['prospective']['fresh']['hit_rate_pct']=20
            with self.assertRaises((ValueError,TypeError)):
                self.notify(data)

    def test_new_pending_wave_does_not_revoke_unchanged_unexpired_proof(self):
        _,reg,prior,event,assessed,rows,windows=fixture()
        newer={**item(100,start=event['alert_time_utc'],wave='new-wave',status='OPEN'),
               'episode_policy_version':formulas.PARENT_POLICY}
        latest=validation.evaluate(reg,[*rows,newer],analysis_as_of_utc=event['alert_time_utc']+timedelta(seconds=1),
            source_coverage_complete=True,common_window_rows=windows)
        latest['representative_entries_frozen']=True
        self.assertFalse(latest['research_ready'])
        self.assertTrue(store.pending_additions_only(prior,latest,published_at=assessed))
        self.assertEqual(self.notify((None,reg,prior,event,assessed))['event_id'],100)
        latest['episodes'][-1]['status']='FAILURE'
        self.assertFalse(store.pending_additions_only(prior,latest,published_at=assessed))
        latest['episodes'][-1]['status']='OPEN';latest['episodes'][0]['mae_pct']=99
        self.assertFalse(store.pending_additions_only(prior,latest,published_at=assessed))

    def test_fresh_evidence_expiration_is_checked_at_current_clock(self):
        data=fixture(3)
        old_assessed=data[4]
        # Reassess just before the first complete wave crosses 14 days, then
        # exercise expiry within the otherwise-valid 30-minute lease.
        starts=[validation.evidence._utc(ep['parent_start_time_utc']) for ep in data[2]['episodes']]
        assessed=min(starts)+timedelta(days=14)-timedelta(seconds=10)
        result=validation.evaluate(data[1],data[5],analysis_as_of_utc=assessed,source_coverage_complete=True,common_window_rows=data[6])
        result['representative_entries_frozen']=True
        q=contract.qualification(data[1],result,evaluated_at=assessed,now=assessed)
        self.assertEqual(q['eligible_until_utc'],assessed+timedelta(seconds=10))
        with self.assertRaises(ValueError):
            contract.qualification(data[1],result,evaluated_at=assessed,now=assessed+timedelta(seconds=11))


class TransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_commit_before_send_and_confirmed_id(self):
        payload=ExperimentalTests().notify()
        events=[]; item_={'delivery_id':1,'claim_token':'token','chat_id':42,'payload':payload}
        queue=[item_,None]
        def transact(fn,*args,**kwargs):
            events.append(fn.__name__)
            if fn is store.enqueue:return {'enqueued':1}
            if fn is store.claim:return queue.pop(0)
            return True
        class Bot:
            async def send_message(self,**kwargs):
                events.append('telegram'); return SimpleNamespace(message_id=17)
        obj=worker.OrderedExperimentalWorker();obj.bind_telegram(Bot())
        with patch.object(worker,'_ENABLED',True),patch.object(worker,'_transaction',transact):
            result=await obj.run_once()
        self.assertEqual(result['sent'],1)
        self.assertLess(events.index('begin_send'),events.index('telegram'))
        self.assertLess(events.index('telegram'),events.index('finish'))

    async def test_ambiguous_network_result_recorded_unknown_without_retry(self):
        payload=ExperimentalTests().notify(); queue=[{'delivery_id':1,'claim_token':'t','chat_id':42,'payload':payload},None]
        records=[]
        def transact(fn,*args,**kwargs):
            if fn is store.enqueue:return {'enqueued':1}
            if fn is store.claim:return queue.pop(0)
            if fn is store.finish:records.append(kwargs)
            return True
        class Bot:
            calls=0
            async def send_message(self,**kwargs):
                self.calls+=1;raise TimeoutError('uncertain transport result')
        bot=Bot();obj=worker.OrderedExperimentalWorker();obj.bind_telegram(bot)
        with patch.object(worker,'_ENABLED',True),patch.object(worker,'_transaction',transact):
            result=await obj.run_once()
        self.assertEqual(bot.calls,1);self.assertEqual(result['unknown'],1)
        self.assertIsNone(records[0]['message_id'])

    async def test_revocation_before_send_prevents_network(self):
        payload=ExperimentalTests().notify();queue=[{'delivery_id':1,'chat_id':42,'payload':payload},None]
        def transact(fn,*args,**kwargs):
            if fn is store.enqueue:return {'enqueued':1}
            if fn is store.claim:return queue.pop(0)
            if fn is store.begin_send:return False
            raise AssertionError('unexpected acknowledgement')
        class Bot:
            async def send_message(self,**kwargs): raise AssertionError('revoked message sent')
        obj=worker.OrderedExperimentalWorker();obj.bind_telegram(Bot())
        with patch.object(worker,'_ENABLED',True),patch.object(worker,'_transaction',transact):
            result=await obj.run_once()
        self.assertEqual(result['sent'],0)


class PublicationTests(unittest.TestCase):
    def test_pending_revalidation_rechecks_old_prices_and_never_renews_lease(self):
        scope,reg,prior,event,assessed,rows,windows=fixture()
        now=event['alert_time_utc']+timedelta(seconds=1)
        newer={**item(100,start=event['alert_time_utc'],wave='new-wave',status='OPEN'),
               'episode_policy_version':formulas.PARENT_POLICY}
        latest=validation.evaluate(reg,[*rows,newer],analysis_as_of_utc=now,
            source_coverage_complete=True,common_window_rows=windows)
        latest['representative_entries_frozen']=True
        latest['evidence_sha256']=validation.digest(latest)
        prior_row={'result':prior,'published_at_utc':assessed+timedelta(seconds=1),
                   'eligible_until_utc':assessed+timedelta(minutes=30)}
        class Cursor:
            def __init__(self,value):self.value=value
            def fetchone(self):return self.value
        class Conn:
            def __init__(self):self.writes=[]
            def execute(self,sql,args=()):
                if sql.startswith('SELECT registration'):return Cursor({'registration':reg})
                if sql.startswith('SELECT clock_timestamp()'):return Cursor({'now':now+timedelta(seconds=3)})
                if 'SELECT a.*,v.result' in sql:return Cursor(prior_row)
                if 'INSERT INTO research_ordered_experimental_eligibility' in sql:
                    self.writes.append(args);return Cursor(None)
                raise AssertionError(sql)
        conn=Conn()
        with patch.object(validation_store,'_enrich_rows',lambda conn,rows,binding:rows):
            self.assertTrue(store.publish_evaluation(conn,scope,latest,now=now,rows=[*rows,newer],common_window_rows=windows))
        self.assertEqual(conn.writes,[])  # No changed digest or extended TTL.
        damaged=deepcopy(windows);damaged[1]['mfe_pct']=0;damaged[1]['mae_pct']=20
        with patch.object(validation_store,'_enrich_rows',lambda conn,rows,binding:rows):
            self.assertFalse(store.publish_evaluation(conn,scope,latest,now=now,rows=[*rows,newer],common_window_rows=damaged))
        self.assertFalse(conn.writes[-1][-1])
        # Actual DB publication is later than the worker pass timestamp.
        self.assertEqual(conn.writes[-1][4],now+timedelta(seconds=3))

    def test_old_v2_candidate_cannot_get_new_exact_acceptance(self):
        _,reg,_,_,_,_,_=fixture()
        old=deepcopy(reg['binding']['candidate_definition'])
        old['catalog_version']='captured-question-search-v2-sequence-regime-acceptance'
        self.assertIsNone(acceptance.policy_for(reg['binding'],old))


if __name__=='__main__':
    unittest.main()
