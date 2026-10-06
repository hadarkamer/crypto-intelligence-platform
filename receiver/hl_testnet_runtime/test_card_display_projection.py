"""Display isolation checks: an app can receive a copy, never control execution."""
from copy import deepcopy
import inspect
import io
import json
import os
import unittest
from unittest.mock import patch
from . import card_display_projection as p, card_lifecycle as life, app
from .test_card_lifecycle import binding, closed, T


class DisplayOnlyTests(unittest.TestCase):
    def setUp(self):
        guard=patch('socket.socket',side_effect=AssertionError('NETWORK_FORBIDDEN'))
        guard.start();self.addCleanup(guard.stop)
        self.b=binding();self.s=closed(self.b)

    def export(self):
        return p.project([self.b],self.s,revision=1,now_ms=T)

    def test_export_contains_data_only_and_no_authority(self):
        out=self.export()
        self.assertTrue(out['read_only']);self.assertEqual(out['environment'],'testnet')
        card=out['cards'][0];self.assertTrue(card['closure_verified'])
        self.assertEqual(card['prices'],self.b['prices'])
        self.assertEqual(set(card),set(p.FIELDS)|{'symbol','direction','prices'})
        self.assertNotIn(self.b['account'],json.dumps(out))
        for word in ('order_id','client_reference','private_key','agent_key','dispatch_enabled','next_step','callback_url'):
            self.assertNotIn(word,json.dumps(out))

    def test_app_edit_cannot_change_local_cards_prices_or_calculations(self):
        before=deepcopy((self.b,self.s));out=self.export()
        out['cards'][0]['prices']['stop']='1';out['cards'][0]['remaining_quantity']='999'
        out['cards'][0]['fees_by_token']['USDC']='999';out['cards'].clear()
        self.assertEqual((self.b,self.s),before)
        self.assertEqual(self.export()['cards'][0]['prices']['stop'],'98')

    def test_delivery_receipt_is_not_a_trade_receipt(self):
        out=self.export();receipt=dict(delivery_id=out['delivery_id'],received=True)
        self.assertTrue(p.receipt_matches(receipt,out['delivery_id']))
        self.assertIs(type(p.receipt_matches(receipt,out['delivery_id'])),bool)

    def test_receipt_with_commands_or_order_data_is_rejected(self):
        out=self.export()
        for field in ('buy','sell','cancel','stop','account','commands','policy','next_step','webhook','redirect'):
            receipt=dict(delivery_id=out['delivery_id'],received=True)
            receipt[field]='do something'
            with self.assertRaises(life.LifecycleError):p.receipt_matches(receipt,out['delivery_id'])

    def test_mismatched_and_malformed_receipts_rejected(self):
        identity=self.export()['delivery_id']
        for value in (None,[],{},dict(delivery_id='a'*64,received=True),dict(delivery_id=identity,received=1),
                      dict(delivery_id=identity,received=False),dict(delivery_id=identity,received='true')):
            with self.assertRaises(life.LifecycleError):p.receipt_matches(value,identity)

    def test_display_projection_cannot_be_execution_evidence(self):
        out=self.export()
        with self.assertRaises(life.LifecycleError):life.validate_snapshot(out)
        with self.assertRaises(life.LifecycleError):life.validate_bindings(out['cards'])

    def test_no_import_of_exchange_or_recovery_from_display_module(self):
        source=inspect.getsource(p)
        for forbidden in ('import os','import http','import requests','import threading','card_recovery_journal',
                          'card_exit_recovery','parent_exit_handoff','sign_l1_action','/exchange'):
            self.assertNotIn(forbidden,source)
        self.assertFalse(hasattr(p,'send'));self.assertFalse(hasattr(p,'apply'))

    def test_stale_evidence_does_not_display_verified_closure(self):
        out=p.project([self.b],self.s,revision=1,now_ms=T+16000)
        self.assertFalse(out['cards'][0]['closure_verified'])
        self.assertIn('STALE_OR_FUTURE_SNAPSHOT',out['bucket_issues'])

    def test_unknown_funding_never_displays_as_zero(self):
        out=self.export()['cards'][0]
        self.assertIsNone(out['funding_usdc']);self.assertIsNone(out['final_net_usdc'])

    def test_revision_and_duplicate_projection(self):
        self.assertEqual(self.export(),self.export())
        for revision in (0,-1,True,'1'):
            with self.assertRaises(life.LifecycleError):p.project([self.b],self.s,revision=revision,now_ms=T)

    def test_runtime_exposes_no_app_trading_controls(self):
        for method in ('POST','PUT','PATCH','DELETE'):
            for path in ('/','/healthz','/buy','/sell','/cancel','/exchange','/internal/recovery','/internal/execute'):
                response=[]
                env=dict(REQUEST_METHOD=method,PATH_INFO=path,QUERY_STRING='',
                    CONTENT_TYPE='application/json',CONTENT_LENGTH='2',
                    HTTP_ORIGIN='https://journal.example',**{'wsgi.input':io.BytesIO(b'{}')})
                app.application(env,lambda status,headers:response.append(status))
                self.assertEqual(response[0],'405 Method Not Allowed')

    def test_app_claim_does_not_authenticate_as_alert_producer(self):
        config=dict(HL_TESTNET_CARDS_INTAKE='record_only_v1',HL_TESTNET_CARDS_PHASE1='record_only_v1',
            HL_TESTNET_JOURNAL_BACKEND='staging_postgres_v1',HL_TESTNET_RUNTIME_MODE='read_only',
            RENDER_SERVICE_ID='srv-dakptbh594qs7395460g',HL_TESTNET_CARDS_INTAKE_SECRET='a'*64)
        response=[]
        env=dict(REQUEST_METHOD='POST',PATH_INFO='/internal/testnet-cards/v1',QUERY_STRING='',
            CONTENT_TYPE='application/json',CONTENT_LENGTH='2',HTTP_ORIGIN='https://journal.example',
            HTTP_AUTHORIZATION='Bearer app-display-token',**{'wsgi.input':io.BytesIO(b'{}')})
        with patch.dict(os.environ,config,clear=True), patch('hl_testnet_runtime.alert_cards_intake.PostgresJournal.from_env',side_effect=AssertionError('NO_STORAGE_ACCESS')):
            app.application(env,lambda status,headers:response.append(status))
        self.assertEqual(response[0],'403 Forbidden')
