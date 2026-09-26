"""Signed app projection cannot place an order or invent unverified PnL."""
from datetime import datetime, timezone
from unittest.mock import patch
import base64
import json
import unittest

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from . import app_card_delivery as delivery, trade_cards
from .test_filled_quantity_exits import original, META
from .test_card_lifecycle import T


class ProjectionTests(unittest.TestCase):
    def card(self):
        _,row=original(501,'LONG')
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


if __name__ == '__main__':
    unittest.main()
