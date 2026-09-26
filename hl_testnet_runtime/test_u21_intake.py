"""U21 authenticated data boundary; software tests never contact an exchange."""
from copy import deepcopy
import unittest

import alert_cards_wire as wire
from alert_cards_u21_selftest import delivered, SCOPE, FIRST, SECOND
from . import trade_cards
from .alert_cards_intake import accept
from .filled_dispatch_store import DispatchError
from .filled_quantity_dispatch import Controller
from .unavailable_asset_cards import VERSION as UNAVAILABLE


class Cards:
    def __init__(self): self.saved={}
    def record(self, card):
        trade_cards.validate_card(card)
        identity=card['card_id']; prior=self.saved.get(identity)
        if prior is not None and prior!=card: raise AssertionError('immutable card changed')
        self.saved[identity]=deepcopy(card)
        return {'card_id':identity,'created':prior is None}


class Receipts:
    def __init__(self): self.cards=Cards();self.receipts={}
    def get(self, identity): return self.receipts.get(identity)
    def save(self, identity, status, card_id, source, reason=None):
        self.receipts.setdefault(identity,dict(status=status,card_id=card_id,reason=reason))


def value(item=None):
    return wire.u21_delivery(item or delivered(),SCOPE,wire.U21_CONFIG)


class U21IntakeTests(unittest.TestCase):
    def setUp(self): self.store=Receipts()
    def metadata(self): return {'universe':[{'name':'XRP','szDecimals':1}]}
    def accept(self,v): return accept(wire.encoded(v),self.store,read_metadata=self.metadata)

    def test_records_precise_source_and_rounded_execution_with_no_invented_threshold(self):
        result=self.accept(value())
        self.assertEqual(result['status'],'RECORDED')
        card=next(iter(self.store.cards.saved.values()))
        self.assertEqual(card['account_role'],'short_account')
        self.assertEqual(card['rule']['threshold_pct'],None)
        self.assertIsNone(card['planning']['cancel_price'])
        self.assertEqual(card['source_expires_at'],value()['expires_at'])
        self.assertEqual(card['prepared']['source']['stop'],'1.2401699999999998')
        self.assertEqual(card['prepared']['execution']['stop'],'1.2402')
        self.assertEqual(card['risk']['planned_usd'],'10')

    def test_replay_same_notification_and_distinct_notifications(self):
        first=value(); second=value(delivered(SECOND))
        self.assertEqual(self.accept(first)['status'],'RECORDED')
        self.assertEqual(self.accept(first)['status'],'DUPLICATE')
        self.assertEqual(self.accept(second)['status'],'RECORDED')
        self.assertEqual(len(self.store.cards.saved),2)
        ids={c['event_id'] for c in self.store.cards.saved.values()}
        self.assertEqual(ids,{FIRST,SECOND})

    def test_invalid_display_is_rejected_before_card(self):
        v=value();v['text']=v['text'].replace('1.24017','1.24018')
        self.assertEqual(self.accept(v)['status'],'REJECTED')
        self.assertFalse(self.store.cards.saved)

    def test_missing_testnet_market_stays_data_only(self):
        self.metadata=lambda:{'universe':[{'name':'BTC','szDecimals':5}]}
        self.assertEqual(self.accept(value())['status'],'RECORDED')
        card=next(iter(self.store.cards.saved.values()))
        self.assertEqual(card['version'],UNAVAILABLE)
        self.assertIsNone(card['prepared']['execution'])
        self.assertEqual(card['source_expires_at'],value()['expires_at'])

    def test_u21_cannot_enter_existing_half_threshold_controller(self):
        self.accept(value());cid=next(iter(self.store.cards.saved))
        class Store:
            domain='testnet'
            journal=object()
        controller=Controller(Store(),type('Venue',(),{'domain':'testnet'})(),{})
        from unittest.mock import patch
        with patch('hl_testnet_runtime.filled_quantity_dispatch.CardStore') as card_store:
            card_store.return_value.load.return_value=self.store.cards.saved[cid]
            with self.assertRaisesRegex(DispatchError,'SOURCE_CANCEL_POLICY_REQUIRES_OWNER_DECISION'):
                controller.register(cid)


if __name__=='__main__':unittest.main()
