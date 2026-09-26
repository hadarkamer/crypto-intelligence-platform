"""U21 authenticated data boundary; software tests never contact an exchange."""
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
import unittest
from unittest.mock import patch

import alert_cards_wire as wire
from alert_cards_u21_selftest import delivered, SCOPE, FIRST, SECOND
from . import trade_cards
from .alert_cards_intake import accept
from .filled_dispatch_store import DispatchError
from .filled_quantity_dispatch import Controller
from .filled_quantity_dispatch import TestnetVenue, AFTER_EXIT
from . import two_account_execution as accounts
from . import filled_pending_cancel as cancel, filled_quantity_exits as selected
from .source_window import timestamp
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

    def test_u21_owner_policy_uses_half_of_half_percent_without_changing_source(self):
        self.accept(value());card=next(iter(self.store.cards.saved.values()))
        account='0x'+'1'*40;routes={'short_account':dict(account=account)}
        draft=selected.prepare_entry(card,self.metadata(),account,routes)
        with self.assertRaisesRegex(DispatchError,'SOURCE_CANCEL_POLICY_REQUIRES_OWNER_DECISION'):
            cancel._rule(dict(card=card,draft=draft),draft)
        original=dict(card=card,draft=draft,cancel_policy=deepcopy(cancel.U21_OWNER_POLICY))
        rule=cancel._rule(original,draft)
        entry=Decimal(draft['entry_action']['orders'][0]['p'])
        self.assertEqual(rule['threshold_pct'],'0.5')
        self.assertEqual(rule['cancel_move_pct'],'0.25')
        self.assertEqual(Decimal(rule['cancel_price']),entry*Decimal('0.9975'))
        self.assertIsNone(card['rule']['threshold_pct'])
        original['cancel_policy']['threshold_pct']='8'
        with self.assertRaisesRegex(DispatchError,'SOURCE_CANCEL_POLICY_REQUIRES_OWNER_DECISION'):
            cancel._rule(original,draft)

    def test_u21_registration_freezes_policy_only_while_source_is_fresh(self):
        self.accept(value());cid=next(iter(self.store.cards.saved))
        source_at=timestamp(value()['source_at'])
        account='0x'+'1'*40
        class Store:
            domain='testnet'
            journal=object()
            def __init__(self): self.state=dict(originals={},pending=None,revision=0,bucket='a'*64)
            def create_bucket(self,account,symbol): return self.state
            def change(self,bucket,revision,event,now,update):
                update(None,self.state)
                return self.state
        class Venue:
            domain='testnet'
            at=int((source_at+timedelta(seconds=30)).timestamp()*1000)
            def now(self): return self.at
            def metadata(self): return {'universe':[{'name':'XRP','szDecimals':1}]}
        store=Store();venue=Venue()
        controller=Controller(store,venue,{'short_account':dict(account=account)})
        with patch('hl_testnet_runtime.filled_quantity_dispatch.CardStore') as card_store:
            card_store.return_value.load.return_value=self.store.cards.saved[cid]
            state=controller.register(cid)
            self.assertEqual(state['originals'][cid]['cancel_policy'],cancel.U21_OWNER_POLICY)
            self.assertIsNone(state['originals'][cid]['card']['rule']['threshold_pct'])
            self.assertIs(controller.register(cid),state)
            venue.at=int((source_at+timedelta(seconds=90)).timestamp()*1000)
            self.assertIs(controller.register(cid),state)  # keep servicing an existing registration
            new_controller=Controller(Store(),venue,{'short_account':dict(account=account)})
            with self.assertRaisesRegex(DispatchError,'U21_ORIGINAL_SOURCE_EXPIRED'):
                new_controller.register(cid)

    def test_u21_original_expiry_controls_final_entry_even_with_longer_env_deadline(self):
        source_at=timestamp(value()['source_at']);expires=value()['expires_at']
        account='0x'+'1'*40
        env=dict(RENDER_SERVICE_ID=accounts.SERVICE,
            HL_TESTNET_RUNTIME_MODE='filled_card_controlled_v1',
            HL_TESTNET_FILLED_DISPATCH='approved_single_card_v1',
            HL_TESTNET_TWO_ACCOUNT_EXECUTION='disabled',
            HL_TESTNET_FILLED_CARD_ID='a'*64,
            HL_TESTNET_FILLED_AFTER_EXIT_POLICY=AFTER_EXIT,
            HL_TESTNET_FILLED_APPROVAL_EXPIRES_MS=str(int((source_at+timedelta(hours=1)).timestamp()*1000)),
            HL_TESTNET_FILLED_SOURCE_EXPIRES_AT=(source_at+timedelta(minutes=5)).isoformat())
        proposal=dict(operation='ENTRY',source_at=value()['source_at'],
            source_expires_at=expires,card_id='a'*64,role='short_account',account=account)
        venue=TestnetVenue(env)
        with patch('hl_testnet_runtime.filled_quantity_dispatch.roles.route_for',return_value={'account':account}):
            with patch.object(venue,'now',return_value=int((source_at+timedelta(seconds=89)).timestamp()*1000)):
                self.assertEqual(venue._gate(proposal,AFTER_EXIT),{'account':account})
            with patch.object(venue,'now',return_value=int((source_at+timedelta(seconds=90)).timestamp()*1000)):
                with self.assertRaisesRegex(DispatchError,'NEW_TRIAL_SOURCE_NOT_FRESH'):
                    venue._gate(proposal,AFTER_EXIT)


if __name__=='__main__':unittest.main()
