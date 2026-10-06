"""Signed app projection cannot place an order or invent unverified PnL."""
from datetime import datetime, timezone
from copy import deepcopy
from unittest.mock import patch, Mock
import base64
import json
import unittest

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from . import app_card_delivery as delivery, trade_cards
from .test_filled_quantity_exits import original, META
from .test_card_lifecycle import T


class ProjectionTests(unittest.TestCase):
    def card(self, side='LONG'):
        _,row=original(501,side)
        return trade_cards.prepare_card(row['card']['prepared']['source'],META,
            rule_id='FORMULA_501',threshold_pct='1.5',record_kind='received_alert')

    def test_record_only_card_has_no_fill_or_pnl_claim(self):
        card=self.card()
        at=datetime.fromtimestamp(T/1000,timezone.utc)
        result=delivery.project(card,None,created_at=at)
        self.assertEqual(result['revision'],1)
        self.assertEqual(result['payload']['status'],'RECORDED_ONLY')
        self.assertIsNone(result['payload']['quantity_entered'])
        self.assertIsNone(result['payload']['realized_pnl'])
        self.assertFalse(result['payload']['pnl_verified'])
        self.assertFalse(result['payload']['closure_verified'])
        self.assertEqual(result['observed_at'],at.isoformat())
        self.assertEqual(result['workspace_id'],delivery.WORKSPACE)

    def test_both_roles_use_original_side_and_unverified_results_stay_unknown(self):
        for side in ('LONG','SHORT'):
            card=self.card(side)
            value=delivery.project(card,None,created_at=datetime.fromtimestamp(T/1000,timezone.utc))
            self.assertEqual(value['payload']['side'],side.lower())
            self.assertIsNone(value['payload']['quantity_entered'])
            self.assertFalse(value['payload']['closure_verified'])
            self.assertFalse(value['payload']['pnl_verified'])

    def test_projection_refuses_other_original_or_symbol(self):
        card=self.card()
        for other in (self.card('SHORT'),card):
            state=dict(originals={card['card_id']:dict(card=other)},symbol='BTC')
            with self.assertRaisesRegex(Exception,'APP_PROJECTION_SOURCE_STATE_MISMATCH'):
                delivery.project(card,state,created_at=datetime.fromtimestamp(T/1000,timezone.utc))

    def test_signed_wire_is_exact_timestamp_dot_raw_body(self):
        key=ECC.generate(curve='Ed25519')
        private=base64.b64encode(key.export_key(format='DER')).decode()
        value=delivery.project(self.card(),None,
            created_at=datetime.fromtimestamp(T/1000,timezone.utc))
        calls=[]
        class Response:
            status=200
            def read(self,size): return b'{"status":"APPLIED"}'
        class Connection:
            def __init__(self,host,timeout):
                self.assert_host=host
            def request(self,method,path,raw,headers):
                calls.append((method,path,raw,headers))
            def getresponse(self): return Response()
            def close(self): pass
        with patch.object(delivery.http.client,'HTTPSConnection',Connection),\
             patch.object(delivery.time,'time',return_value=1790433900):
            delivery.signed_post(value,private)
        method,path,raw,headers=calls[0]
        self.assertEqual((method,path),('POST',delivery.PATH))
        self.assertEqual(json.loads(raw),value)
        self.assertEqual(headers['X-Card-Timestamp'],'1790433900')
        eddsa.new(key.public_key(),'rfc8032').verify(
            headers['X-Card-Timestamp'].encode()+b'.'+raw,
            base64.b64decode(headers['X-Card-Signature']))

    def test_many_changing_cards_rotate_without_starving_later_ids(self):
        cards=[dict(card_id=f'{i:064x}',prepared=dict(source=dict(symbol='DOGE')))
               for i in range(1,7)]
        class Store:
            journal=object()
            def for_account(self,account): return []
        class Controller:
            store=Store()
        delivered=[]
        def rows(journal,*,start_after=None,role='long_account'):
            for card in cards:
                if start_after is None or card['card_id']>start_after:
                    yield card,datetime.fromtimestamp(T/1000,timezone.utc)
        def projection(card,state,*,created_at,pending_request=None):
            return dict(card_id=card['card_id'],revision=1,payload={},observed_at=created_at.isoformat())
        publisher=delivery.Publisher()
        with patch.object(delivery,'records',side_effect=rows),\
             patch.object(delivery,'project',side_effect=projection):
            self.assertEqual(publisher.pass_once(Controller(),dict(account='ignored'),'',
                post=lambda value,key:delivered.append(value['card_id'])),4)
            self.assertEqual(publisher.pass_once(Controller(),dict(account='ignored'),'',
                post=lambda value,key:delivered.append(value['card_id'])),2)
        self.assertEqual(delivered,[card['card_id'] for card in cards])

    def test_timestamp_only_changes_do_not_send_and_changed_payload_does(self):
        card=self.card();created=datetime.fromtimestamp(T/1000,timezone.utc)
        class Store:
            journal=object()
            def for_account(self,account):return []
        class Controller:store=Store()
        publisher=delivery.Publisher();sent=[]
        value=dict(revision=1,payload=dict(status='WAITING_ENTRY'),observed_at=created.isoformat())
        with patch.object(delivery,'records',return_value=[(card,created)]),\
             patch.object(delivery,'project',side_effect=lambda *a,**k:deepcopy(value)):
            self.assertEqual(publisher.pass_once(Controller(),dict(account='ignored'),'',post=lambda v,k:sent.append(v)),1)
            value.update(revision=99,observed_at='2026-10-03T19:30:00+00:00')
            self.assertEqual(publisher.pass_once(Controller(),dict(account='ignored'),'',post=lambda v,k:sent.append(v)),0)
            value['payload']['status']='OPEN'
            self.assertEqual(publisher.pass_once(Controller(),dict(account='ignored'),'',post=lambda v,k:sent.append(v)),1)
        self.assertEqual(len(sent),2)

    def test_failed_card_backs_off_while_other_cards_deliver_and_retry_counts_no_success(self):
        cards=[dict(card_id=f'{i:064x}',prepared=dict(source=dict(symbol='DOGE'))) for i in range(1,3)]
        created=datetime.fromtimestamp(T/1000,timezone.utc)
        class Store:
            journal=object()
            def for_account(self,account):return []
        class Controller:store=Store()
        now=[100.0];calls=[]
        def post(value,key):
            calls.append(value['card_id'])
            if value['card_id']==cards[0]['card_id']:raise RuntimeError('private')
        def rows(journal,*,start_after=None,role='long_account'):
            return [(c,created) for c in cards if start_after is None or c['card_id']>start_after]
        publisher=delivery.Publisher()
        with patch.object(delivery,'records',side_effect=rows),\
             patch.object(delivery,'project',side_effect=lambda c,*a,**k:dict(card_id=c['card_id'],payload={},revision=1)):
            self.assertEqual(publisher.pass_once(Controller(),dict(account='ignored'),'',post=post,clock=lambda:now[0]),1)
            self.assertEqual(publisher.last_status,'DELIVERY_UNAVAILABLE_RETRY')
            self.assertNotIn(cards[0]['card_id'],publisher.sent)
            self.assertEqual(publisher.pass_once(Controller(),dict(account='ignored'),'',post=post,clock=lambda:now[0]),0)
            self.assertEqual(len(calls),2)
            now[0]=121
            self.assertEqual(publisher.pass_once(Controller(),dict(account='ignored'),'',post=post,clock=lambda:now[0]),0)
            self.assertEqual(len(calls),3)

    def test_one_display_account_failure_does_not_suppress_other_account(self):
        from . import long_stream_runtime as stream
        broken=Mock();broken.pass_once.side_effect=RuntimeError('private')
        healthy=Mock();healthy.pass_once.return_value=1;healthy.last_status='DELIVERED'
        stop=Mock();stop.is_set.side_effect=[False,True]
        routes=[('long_account',dict(account='long'),None,'long_flag'),
                ('short_account',dict(account='short'),None,'short_flag')]
        with patch.object(delivery,'Publisher',side_effect=[broken,healthy]),\
             patch.object(stream,'_stop',stop),patch('builtins.print'):
            stream._app_loop(object(),routes,'key')
        healthy.pass_once.assert_called_once()
        self.assertEqual(healthy.pass_once.call_args.kwargs['role'],'short_account')


if __name__ == '__main__':
    unittest.main()
