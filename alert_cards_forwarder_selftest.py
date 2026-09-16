"""Offline contract and producer tests: no DB, Telegram or exchange writes."""
import asyncio
from copy import deepcopy
from datetime import datetime,timezone,timedelta
import io
import json
import unittest
from unittest.mock import patch,Mock

import alert_cards_wire as w
import alert_cards_forwarder as f

KEY='ab'*32
SCOPE='1'*64


def delivery(side='LONG',identity='intent-1',family='manual'):
    stop,take=('98','102') if side=='LONG' else ('102','98')
    txt=(f'<b>סף 2%</b>\n🧪 ניסיוני\n<b>DEMO | עלייה — {side}</b>\n'
         f'<b>שער בסיס — תחילת נתוני הבסיס:</b> 100\n<b>סטופלוס:</b> {stop}\n<b>טייק פרופיט:</b> {take}')
    return dict(version=w.VERSION,family=family,scope_hash=SCOPE,intent_id=identity,
        rule_id='C1',symbol='DEMO',side=side,source_at='2026-09-16T12:05:00+00:00',
        delivered_at='2026-09-16T12:05:01+00:00',expires_at='2026-09-16T12:15:00+00:00',
        message_id=123 if family=='manual' else None,threshold_bps=200 if family=='manual' else None,
        text=txt,reference=dict(status='READY',symbol='DEMO',price='100',
            price_time_utc='2026-09-16T12:00:00+00:00',anchor_time_utc='2026-09-16T12:02:00+00:00',source='BINANCE_SPOT_TRADE_1M'))


class WireTests(unittest.TestCase):
    def test_both_directions_and_sources(self):
        for side in ('LONG','SHORT'):
            for family in ('manual','dual_cvd65'):
                v=delivery(side,family=family);before=deepcopy(v);r=w.normalize(v)
                self.assertEqual(r['signal']['side'],side);self.assertEqual(v,before)
                self.assertEqual(r['signal']['at'],v['source_at'])
                self.assertEqual(r['signal']['entry'],'100')
    def test_realistic_175_template_no_price_reconstruction(self):
        v=delivery('SHORT');v.update(threshold_bps=175)
        v['text']=v['text'].replace('סף 2%','🧪 סף 1.75% — ניסיוני, לא למסחר').replace('102','101.751').replace('98','98.249')
        r=w.normalize(v)
        self.assertEqual(r['threshold_pct'],'1.75');self.assertEqual(r['signal']['stop'],'101.751')
    def test_same_source_event_across_different_rules_does_not_collapse_delivery_ids(self):
        a=delivery(identity='a');b=delivery(identity='b')
        self.assertNotEqual(w.normalize(a)['signal']['event_id'],w.normalize(b)['signal']['event_id'])
    def test_missing_or_duplicate_price_is_not_guessed(self):
        for txt in ('<b>סף 2%</b>',delivery()['text']+'\nסטופלוס: 98'):
            with self.assertRaises(w.WireError):w.normalize({**delivery(),'text':txt})
    def test_direction_and_symbol_must_match(self):
        for k,v in [('side','SHORT'),('symbol','BTC')]:
            with self.assertRaises(w.WireError):w.normalize({**delivery(),k:v})
    def test_no_threshold_fallback(self):
        v=delivery();v['threshold_bps']=175
        with self.assertRaises(w.WireError):w.normalize(v)
    def test_reference_must_match(self):
        for k,v in [('status','MISSING'),('price','99'),('symbol','BTC')]:
            d=delivery();d['reference'][k]=v
            with self.assertRaises(w.WireError):w.normalize(d)
    def test_times_not_fabricated(self):
        for key,value in [('source_at','bad'),('delivered_at','2026-09-16T11:00:00Z'),('expires_at','2026-09-16T20:00:00Z')]:
            with self.assertRaises(w.WireError):w.normalize({**delivery(),key:value})
    def test_expired_delivery_can_be_recorded_never_retimestamped(self):
        self.assertEqual(w.normalize(delivery())['signal']['at'],'2026-09-16T12:05:00+00:00')
    def test_bad_markup_or_price_order_rejected(self):
        for txt in (delivery()['text']+'<script>x</script>',delivery()['text'].replace('סטופלוס:</b> 98','סטופלוס:</b> 103')):
            with self.assertRaises(w.WireError):w.normalize({**delivery(),'text':txt})
    def test_duplicate_json_and_extra_secret_refused(self):
        with self.assertRaises(w.WireError):w.decoded(b'{"x":1,"x":2}')
        with self.assertRaises(w.WireError):w.normalize({**delivery(),'secret':'NO_STORE'})
    def test_signatures_require_body_key_and_recent_time(self):
        raw=w.encoded(delivery());stamp='1789550000';sig=w.signature(KEY,stamp,raw)
        self.assertTrue(w.authenticate(KEY,stamp,sig,raw,1789550000))
        self.assertFalse(w.authenticate(KEY,stamp,sig,raw+b' ',1789550000))
        self.assertFalse(w.authenticate('cd'*32,stamp,sig,raw,1789550000))
        self.assertFalse(w.authenticate(KEY,stamp,sig,raw,1789550061))
        self.assertFalse(w.authenticate(KEY,stamp,None,raw,1789550000))
    def test_projection_strips_unrelated_payload(self):
        v=delivery();p=dict(rule_id=v['rule_id'],symbol=v['symbol'],direction=v['side'],event_time=v['source_at'],threshold_bps=200,
            price_reference={**v['reference'],'secret':'NO_COPY'})
        item=dict(intent_id=v['intent_id'],payload=p,acknowledged_at=v['delivered_at'],expires_at=v['expires_at'],message_id=123,text=v['text'],private_key='NO_COPY')
        self.assertNotIn('NO_COPY',str(w.manual_delivery(item,SCOPE)))
        self.assertEqual(w.normalize(w.manual_delivery(item,SCOPE)),w.normalize(v))
    def test_dual_projection_keeps_db_receipt_without_faking_message_id(self):
        v=delivery(family='dual_cvd65')
        row=dict(intent_id=v['intent_id'],rule_id=v['rule_id'],symbol=v['symbol'],direction=v['side'],source_at_utc=v['source_at'],finished_at_utc=v['delivered_at'],expires_at=v['expires_at'],text=v['text'],payload={'observation':{'price_reference':v['reference']}})
        self.assertEqual(w.normalize(w.dual_delivery(row,SCOPE)),w.normalize(v))


class ForwarderTests(unittest.TestCase):
    def setUp(self):
        self.f=f.Forwarder();self.fence=datetime(2026,9,16,tzinfo=timezone.utc)
        p=patch('http.client.HTTPSConnection',side_effect=AssertionError('No real network'))
        p.start();self.addCleanup(p.stop)
    def call(self,values,post):
        return self.f.pass_once((SCOPE,),KEY,self.fence,reader=lambda *_:values,post=post)
    def test_disabled_is_noop(self):
        with patch.dict(f.os.environ,{},clear=True),patch.object(f.asyncio,'get_running_loop',side_effect=AssertionError('No start')):
            f.maybe_start(123)
        self.assertIsNone(f.config({}))
    def test_source_service_bound(self):
        with self.assertRaises(w.WireError):f.config(dict(ALERT_CARDS_FORWARD_MODE=f.MODE,ALERT_CARDS_FORWARD_SECRET=KEY,RENDER_SERVICE_ID='wrong'))
    def test_one_delivery_then_cached_no_extra_post(self):
        post=Mock(return_value='RECORDED')
        self.assertEqual(self.call([delivery()],post)['recorded'],1)
        self.assertEqual(self.call([delivery()],post)['attempted'],0);post.assert_called_once()
    def test_similar_alerts_are_distinct(self):
        r=self.call([delivery(identity='a'),delivery(identity='b')],lambda *_:'RECORDED')
        self.assertEqual(r['recorded'],2)
    def test_retry_does_not_hold_telegram_or_other_records(self):
        def post(v,_):
            if v['intent_id']=='bad':raise TimeoutError('DO_NOT_LOG')
            return 'RECORDED'
        r=self.call([delivery(identity='bad'),delivery(identity='ok')],post)
        self.assertEqual(r['recorded'],1);self.assertEqual(r['deferred'],1)
        self.assertNotIn('DO_NOT_LOG',str(r))
    def test_restart_replay_is_harmless_by_receiver_ack(self):
        self.assertEqual(self.call([delivery()],lambda *_:'DUPLICATE')['duplicates'],1)
    def test_poison_record_does_not_block_good(self):
        r=self.call([delivery(identity='bad'),delivery(identity='ok')],lambda v,_:'REJECTED' if v['intent_id']=='bad' else 'RECORDED')
        self.assertEqual((r['rejected'],r['recorded']),(1,1))
    def test_batch_limit_is_not_a_trading_cap(self):
        values=[delivery(identity=str(i)) for i in range(20)];post=Mock(return_value='RECORDED')
        self.assertEqual(self.call(values,post)['recorded'],16)
        self.assertEqual(self.call(values,post)['recorded'],4)
    def test_source_read_failure_says_unavailable(self):
        def bad(*_):raise ValueError('secret')
        r=self.f.pass_once((SCOPE,),KEY,self.fence,reader=bad,post=Mock())
        self.assertEqual(r['status'],'FORWARD_SOURCE_UNAVAILABLE');self.assertNotIn('secret',str(r))
    def test_source_connection_is_read_only_and_does_not_import_trading(self):
        import inspect
        text=inspect.getsource(f.read_delivered)
        self.assertIn('default_transaction_read_only=on',text)
        for x in ('INSERT','UPDATE','DELETE','CREATE','_wallet','getUpdates'):
            self.assertNotIn(x,text)
    def test_network_destination_is_fixed_and_does_not_send_key(self):
        raw=w.encoded(delivery());identity=__import__('hashlib').sha256(raw).hexdigest()
        response=Mock(status=200);response.read.return_value=json.dumps(dict(receipt_id=identity,record_only=True,status='RECORDED')).encode()
        conn=Mock();conn.getresponse.return_value=response
        with patch.object(f.http.client,'HTTPSConnection',return_value=conn) as ctor:
            self.assertEqual(f.post_record(delivery(),KEY),'RECORDED')
        ctor.assert_called_once_with(w.HOST,timeout=4)
        self.assertNotIn(KEY,str(conn.request.call_args))
        self.assertEqual(conn.request.call_args.args[1],w.PATH)
    def test_redirects_are_not_followed(self):
        conn=Mock();conn.getresponse.return_value=Mock(status=302)
        conn.getresponse.return_value.read.return_value=b'{}'
        with patch.object(f.http.client,'HTTPSConnection',return_value=conn):
            with self.assertRaises(w.WireError):f.post_record(delivery(),KEY)
        self.assertEqual(conn.request.call_count,1)


if __name__=='__main__':unittest.main()
