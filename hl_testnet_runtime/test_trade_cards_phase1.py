"""Record/routing tests only. Disposable CI DB; no user keys or exchange orders."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
from decimal import Decimal
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

from . import trade_cards as c, trade_cards_startup as startup

META = {'universe':[{'name':'DEMO','szDecimals':2}]}
A, B, C, D = ['0x'+digit*40 for digit in '1234']
CI = os.environ.get('HL_JOURNAL_CI_URL')


def source(identity='event-1', side='LONG'):
    return dict(kind='SIGNAL',event_id=identity,symbol='DEMO',side=side,
        entry='100',stop='98' if side=='LONG' else '102',
        take_profit='104' if side=='LONG' else '96',at='2026-09-16T12:00:00+00:00')


def card(src=None, **kw):
    return c.prepare_card(src or source(),META,
        **dict(rule_id='MAX_PAIN_INVERTED',threshold_pct='1.5',record_kind='synthetic_test',**kw))


def routes():
    return dict(HL_TESTNET_LONG_ACCOUNT_ADDRESS=A,HL_TESTNET_LONG_AGENT_ADDRESS=B,
                HL_TESTNET_SHORT_ACCOUNT_ADDRESS=C,HL_TESTNET_SHORT_AGENT_ADDRESS=D)


class CardTests(unittest.TestCase):
    def setUp(self):
        p=patch('http.client.HTTPSConnection',side_effect=AssertionError('No exchange access'))
        p.start();self.addCleanup(p.stop)

    def test_direction_is_final_not_inverted_again(self):
        for side,role in [('LONG','long_account'),('SHORT','short_account')]:
            self.assertEqual(card(source(side=side))['account_role'],role)

    def test_raw_prices_and_source_time_preserved(self):
        src=source();src.update(entry='96.88',stop='94.9424',take_profit='98.8176')
        before=deepcopy(src);item=card(src)
        self.assertEqual(item['prepared']['source'],before)
        self.assertEqual(src,before)
        self.assertEqual(item['prepared']['execution']['stop'],'94.942')
        self.assertEqual(item['prepared']['execution']['take_profit'],'98.818')
        self.assertEqual(item['prepared']['execution']['at'],src['at'])

    def test_ten_dollars_before_costs(self):
        item=card()
        self.assertEqual(item['risk']['planned_usd'],'10')
        self.assertFalse(item['risk']['costs_included'])
        self.assertEqual(item['planning']['quantity'],'5')
        self.assertEqual(item['planning']['distance_risk_usd'],'10')

    def test_quantity_is_floored_after_price_rounding(self):
        src=source();src['stop']='97'
        self.assertEqual(card(src)['planning']['quantity'],'3.33')
        self.assertLessEqual(Decimal(card(src)['planning']['distance_risk_usd']),10)

    def test_zero_rounded_size_recorded_not_silently_increased(self):
        src=source();src.update(entry='100000',stop='50000',take_profit='110000')
        item=card(src)
        self.assertEqual(item['planning']['quantity'],'0')
        self.assertFalse(item['planning']['positive_quantity'])
        self.assertFalse(item['dispatch_enabled'])

    def test_half_threshold_both_directions(self):
        self.assertEqual(card()['planning']['cancel_price'],'100.75')
        self.assertEqual(card(source(side='SHORT'))['planning']['cancel_price'],'99.25')

    def test_record_is_not_an_order_or_a_filled_trade(self):
        item=card()
        self.assertEqual(item['state'],'RECORDED_ONLY')
        self.assertIsNone(item['actual_execution'])
        self.assertFalse(item['dispatch_enabled'])
        self.assertNotIn('action',item)

    def test_no_new_identity_on_key_reordering(self):
        self.assertEqual(card(),card(dict(reversed(list(source().items())))))

    def test_distinct_legitimate_alerts_have_distinct_cards(self):
        self.assertNotEqual(card()['card_id'],card(source('event-2'))['card_id'])

    def test_changed_direction_does_not_bypass_original_identity(self):
        self.assertEqual(card()['card_id'],card(source(side='SHORT'))['card_id'])
        self.assertNotEqual(c.checksum(card()),c.checksum(card(source(side='SHORT'))))

    def test_unknown_side_not_guessed(self):
        for side in ('BUY','UP','long','',None):
            with self.assertRaises(c.CardError):card(source(side=side))

    def test_incomplete_and_secret_fields_rejected(self):
        for mode in ('missing','secret'):
            src=source()
            if mode=='missing':src.pop('stop')
            else:src['private_key']='DO_NOT_STORE'
            with self.assertRaises(c.CardError):card(src)

    def test_invalid_time_or_id_rejected(self):
        for key,value in [('at','2026-09-16T12:00:00'),('event_id',''),('event_id','!bad')]:
            with self.assertRaises(c.CardError):card({**source(),key:value})

    def test_invalid_threshold_rejected(self):
        for threshold in ('0','-1','NaN','Infinity','100.1',1.5):
            with self.assertRaises(c.CardError):
                c.prepare_card(source(),META,rule_id='C1',threshold_pct=threshold,record_kind='synthetic_test')

    def test_explicit_record_kind_required(self):
        with self.assertRaises(c.CardError):
            c.prepare_card(source(),META,rule_id='C1',threshold_pct='2',record_kind='live_trade')

    def test_changed_card_fields_fail_validation(self):
        for key,value in [('account_role','short_account'),('dispatch_enabled',True),
                          ('environment','mainnet'),('actual_execution',{}),('revision',2)]:
            item=card();item[key]=value
            with self.assertRaises(c.CardError):c.validate_card(item)

    def test_extra_secret_in_card_rejected(self):
        item=card();item['secret']='DO_NOT_STORE'
        with self.assertRaises(c.CardError):c.validate_card(item)

    def test_no_accounts_needed_to_record_logical_route(self):
        self.assertEqual(c.route_summary(card(),{})['account_status'],'WAITING_FOR_ACCOUNT')

    def test_existing_single_account_not_silently_reused(self):
        env={'HL_TESTNET_ACCOUNT_ADDRESS':A,'HL_TESTNET_AGENT_ADDRESS':B}
        self.assertEqual(c.route_summary(card(),env)['account_status'],'WAITING_FOR_ACCOUNT')

    def test_two_distinct_account_routes_not_reported_as_verified(self):
        r=c.account_routes(routes())
        self.assertEqual(r['long_account']['account'],A)
        self.assertEqual(r['short_account']['account'],C)
        summary=c.route_summary(card(),routes())
        self.assertEqual(summary['account_status'],'CONFIGURED_NOT_VERIFIED')
        self.assertFalse(summary['exchange_mapping_verified'])
        self.assertFalse(summary['dispatch_enabled'])

    def test_partial_route_does_not_send_to_other_account(self):
        env=routes();env.pop('HL_TESTNET_SHORT_ACCOUNT_ADDRESS');env.pop('HL_TESTNET_SHORT_AGENT_ADDRESS')
        self.assertEqual(c.route_summary(card(source(side='SHORT')),env)['account_status'],'WAITING_FOR_ACCOUNT')

    def test_same_account_or_master_as_agent_rejected(self):
        for k,v in [('HL_TESTNET_SHORT_ACCOUNT_ADDRESS',A),('HL_TESTNET_LONG_AGENT_ADDRESS',A),
                    ('HL_TESTNET_SHORT_AGENT_ADDRESS',B)]:
            with self.assertRaises(c.CardError):c.account_routes({**routes(),k:v})

    def test_keys_in_address_fields_rejected(self):
        with self.assertRaises(c.CardError):
            c.account_routes({**routes(),'HL_TESTNET_LONG_ACCOUNT_ADDRESS':'0x'+'a'*64})

    def test_partial_configuration_is_not_a_connected_account(self):
        with self.assertRaises(c.CardError):c.account_routes({'HL_TESTNET_LONG_ACCOUNT_ADDRESS':A})

    def test_unrelated_secret_values_are_not_read(self):
        class Env(dict):
            def get(self,k,*args):
                self_test.assertNotIn('KEY',k)
                return super().get(k,*args)
        self_test=self
        c.account_routes(Env(routes()))

    def test_journal_projection_has_no_private_or_manual_fields(self):
        payload=c.journal_projection(card())
        self.assertEqual(payload['external_id'],card()['card_id'])
        self.assertFalse(payload['delivery_enabled'])
        self.assertEqual(payload['environment'],'testnet')
        self.assertIsNone(payload['machine_fields']['pnl'])
        text=json.dumps(payload)
        for value in ('notes','conclusions','private_key','workspace_id','agent_address'):
            self.assertNotIn(value,text)

    def test_projection_cannot_mutate_card(self):
        item=card();before=deepcopy(item)
        c.journal_projection(item)['machine_fields']['source']['entry']='1'
        self.assertEqual(item,before)

    def test_disabled_startup_has_no_io(self):
        report=startup.run({},journal=object())
        self.assertEqual(report['status'],'DISABLED')
        self.assertEqual(report['order_requests_sent'],0)

    def test_phase1_modules_do_not_import_order_code(self):
        for name in ('trade_cards.py','trade_card_store.py','trade_cards_startup.py'):
            src=Path(__file__).with_name(name).read_text()
            for forbidden in ('import hyperliquid_testnet_executor','sign_l1_action','_wallet(',
                              'submit_persisted(',"'/exchange'",'HL_TESTNET_AGENT_KEY'):
                self.assertNotIn(forbidden,src)


@unittest.skipUnless(CI,'Requires disposable localhost PostgreSQL')
class CardPostgresTests(unittest.TestCase):
    def setUp(self):
        import psycopg
        from .postgres_journal import PostgresJournal
        from .trade_card_store import CardStore, SCHEMA
        self.journal=PostgresJournal.for_ci(CI)
        with psycopg.connect(**self.journal._parameters) as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS {SCHEMA} CASCADE')
            conn.execute('DROP SCHEMA IF EXISTS hl_testnet_execution_v1 CASCADE')
        self.journal.bootstrap();self.store=CardStore(self.journal);self.store.initialize()
        p=patch('http.client.HTTPSConnection',side_effect=AssertionError('No exchange access'))
        p.start();self.addCleanup(p.stop)

    def count(self):
        with self.journal._transaction() as conn:
            return conn.execute('SELECT count(*) FROM hl_testnet_cards_v1.cards').fetchone()[0]

    def test_duplicate_has_one_record_and_preserves_timestamp(self):
        item=card();one=self.store.record(item)
        with self.journal._transaction() as conn:
            stamp=conn.execute('SELECT created_at FROM hl_testnet_cards_v1.cards').fetchone()[0]
        two=self.store.record(item)
        self.assertTrue(one['created']);self.assertTrue(two['duplicate']);self.assertEqual(self.count(),1)
        with self.journal._transaction() as conn:
            self.assertEqual(conn.execute('SELECT created_at FROM hl_testnet_cards_v1.cards').fetchone()[0],stamp)

    def test_record_survives_independent_process(self):
        item=card();self.store.record(item)
        program='''import os,sys
from hl_testnet_runtime.postgres_journal import PostgresJournal
from hl_testnet_runtime.trade_card_store import CardStore
s=CardStore(PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL']))
c=s.load(sys.argv[1]); assert s.record(c)['duplicate']
print('RESTART_AND_DUPLICATE_VERIFIED')'''
        r=subprocess.run([sys.executable,'-c',program,item['card_id']],capture_output=True,text=True,timeout=10)
        self.assertEqual(r.returncode,0,r.stderr)
        self.assertIn('RESTART_AND_DUPLICATE_VERIFIED',r.stdout)

    def test_concurrent_delivery_creates_exactly_one(self):
        item=card()
        with ThreadPoolExecutor(max_workers=6) as pool:
            results=list(pool.map(lambda _:self.store.record(item),range(6)))
        self.assertEqual(sum(r['created'] for r in results),1);self.assertEqual(self.count(),1)

    def test_changed_source_or_formula_cannot_overwrite_or_switch_route(self):
        from .postgres_journal import JournalError
        item=card();self.store.record(item)
        for changed in (card(source(side='SHORT')),card({**source(),'take_profit':'105'}),
                c.prepare_card(source(),META,rule_id='OTHER',threshold_pct='1.5',record_kind='synthetic_test')):
            with self.assertRaises(JournalError):self.store.record(changed)
            self.assertEqual(self.store.load(item['card_id']),item)

    def test_more_than_32_parallel_same_symbol_alerts_are_records_not_positions(self):
        for n in range(40):self.assertTrue(self.store.record(card(source(f'event-{n}')))['created'])
        self.assertEqual(self.count(),40)
        with self.journal._transaction() as conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM hl_testnet_execution_v1.attempts').fetchone()[0],0)

    def test_lost_commit_reply_cannot_create_second_card(self):
        from .postgres_journal import JournalError
        original=self.journal._transaction
        @contextmanager
        def lost_reply():
            with original() as conn:yield conn
            raise JournalError('SIMULATED_COMMIT_ACK_LOSS')
        with patch.object(self.journal,'_transaction',lost_reply):
            with self.assertRaises(JournalError):self.store.record(card())
        self.assertTrue(self.store.record(card())['duplicate']);self.assertEqual(self.count(),1)

    def test_corrupt_stored_record_cannot_be_exported(self):
        from .postgres_journal import JournalError
        item=card();self.store.record(item)
        with self.journal._transaction() as conn:
            conn.execute("UPDATE hl_testnet_cards_v1.cards SET manifest=jsonb_set(manifest,'{account_role}','\"short_account\"')")
        with self.assertRaises(JournalError):self.store.load(item['card_id'])

    def test_startup_imports_archived_source_without_new_execution(self):
        prepared=c.precision.prepare_signal(source(),META)
        key,_=self.journal.save_prepared(A,prepared)
        rule=dict(event_id=source()['event_id'],source_digest=c.checksum(source()),rule_id='C1',threshold_pct='1.5')
        with self.journal._transaction() as conn:
            conn.execute('CREATE TABLE hl_testnet_execution_v1.limit_cancel_rules(plan_key text PRIMARY KEY,policy jsonb)')
            conn.execute('INSERT INTO hl_testnet_execution_v1.limit_cancel_rules VALUES(%s,%s::jsonb)',(key,json.dumps(rule)))
        env=dict(HL_TESTNET_CARDS_PHASE1='record_only_v1',RENDER_SERVICE_ID='srv-dakptbh594qs7395460g',
            HL_TESTNET_RUNTIME_MODE='cancel_monitor_testnet_v1',HL_TESTNET_JOURNAL_BACKEND='staging_postgres_v1')
        result=startup.run(env,journal=self.journal)
        self.assertEqual(result['status'],'CARD_STORAGE_AND_ROUTING_REVIEW_PASSED',result)
        self.assertEqual(result['new_review_cards'],1)
        self.assertEqual(result['duplicate_card_checks'],1)
        self.assertEqual(startup.run(env,journal=self.journal)['new_review_cards'],0)
        self.assertEqual(result['account_slots']['short_account'],'WAITING_FOR_ACCOUNT')
        self.assertFalse(result['live_alert_feed_connected'])
        self.assertFalse(result['entry_sending_enabled'])
        self.assertEqual(self.count(),1)
        with self.journal._transaction() as conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM hl_testnet_execution_v1.attempts').fetchone()[0],0)
            self.assertEqual(conn.execute('SELECT manifest FROM hl_testnet_execution_v1.prepared').fetchone()[0],prepared)

    def test_unsafe_runtime_modes_cannot_initialize_storage(self):
        from .trade_card_store import CardStore
        for mode in ('mainnet','single_testnet_attempt_v1','cancel_rehearsal_testnet_v1'):
            env=dict(HL_TESTNET_CARDS_PHASE1='record_only_v1',RENDER_SERVICE_ID='srv-dakptbh594qs7395460g',
                HL_TESTNET_RUNTIME_MODE=mode,HL_TESTNET_JOURNAL_BACKEND='staging_postgres_v1')
            with patch.object(CardStore,'initialize',side_effect=AssertionError('No storage setup')):
                self.assertEqual(startup.run(env,journal=self.journal)['status'],'CARD_PHASE1_REQUIRES_REVIEW')
        self.assertEqual(self.count(),0)

    def test_schema_and_probe_are_repeatable(self):
        self.assertFalse(self.store.initialize())
        self.assertTrue(self.store.probe()['separate_connection_verified'])
        self.assertTrue(self.store.probe()['previous_probe_seen'])


if __name__=='__main__':unittest.main(verbosity=2)
