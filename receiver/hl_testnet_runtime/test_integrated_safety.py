"""Integrated software tests; real PostgreSQL only on disposable loopback.

The venue is substituted and both signing and exchange HTTP are forbidden.
Synthetic registrations below represent a test fixture, not a live dispatcher.
"""
from copy import deepcopy
from datetime import datetime, timezone
import json
import os
import unittest
from unittest.mock import patch

from . import integrated_safety as s, card_lifecycle as life, card_sync as worker
from . import price_precision, persistent_execution, gunicorn_conf
from .card_sync_store import SyncStore, TABLE
from .card_lifecycle_store import LifecycleStore, SCHEMA
from .card_recovery_journal import RecoveryJournal, SCHEMA as RS
from .postgres_journal import PostgresJournal, JournalError, digest
from .test_card_lifecycle import binding, opened, closed, snapshot, order, fill, A, B, T
from .test_card_sync import evidence, Reader, collected
from .test_card_exit_recovery import controls, partial
import hyperliquid_testnet_executor as sender

CI = os.environ.get('HL_JOURNAL_CI_URL')
META = {'universe':[{'name':'DOGE','szDecimals':2}]}


def env():
    return dict(HL_TESTNET_SAFETY_PIPELINE=s.MODE,RENDER_SERVICE_ID=s.SERVICE,
        HL_TESTNET_RUNTIME_MODE='read_only',HL_TESTNET_CARD_SYNC='registered_readonly_v1',
        HL_TESTNET_JOURNAL_BACKEND='staging_postgres_v1',HL_TESTNET_TWO_ACCOUNT_EXECUTION='disabled')


def sample(symbol='DOGE',at=T,mark='101'):
    return dict(symbol=symbol,mark_price=mark,at_ms=at,completed_at_ms=at)


def original(side='LONG',source=None,metadata=META):
    account=A if side=='LONG' else B
    source=source or dict(kind='SIGNAL',event_id='integrated-'+side,symbol='DOGE',side=side,
        entry='100',stop='98' if side=='LONG' else '102',take_profit='104' if side=='LONG' else '96',
        at=datetime.fromtimestamp((T-20000)/1000,timezone.utc).isoformat())
    prepared=price_precision.prepare_signal(source,metadata)
    action=sender.build_action(prepared['execution'],metadata,account,exit_type='tp_limit_sl_market')
    key=digest(['testnet',account,source['event_id']])
    b=binding(side=side,account=account,qty=action['orders'][0]['s'])
    b.update(symbol=source['symbol'],card_id=life.digest(['legacy_executed_testnet',account,key]),
        card_digest=life.digest(dict(plan_key=key,prepared=prepared,action=action)),
        prices={k:prepared['execution'][k] for k in ('entry','stop','take_profit')})
    return b,(account,key,prepared,action)


def direct(b,row,*,snap=None,mark='101',pending=None,now=T,links=None):
    snap=snap or closed(b)
    return s.assess([b],snap,sample(b['symbol'],now,mark),
        s.links_from_records([b],[row]) if links is None else links,pending,revision=1,now_ms=now)


class NoVenue(unittest.TestCase):
    def setUp(self):
        for name in ('http.client.HTTPSConnection','socket.create_connection',
                     'hyperliquid_testnet_executor._wallet','hyperliquid_testnet_executor._signed_body'):
            p=patch(name,side_effect=AssertionError('NO_VENUE_OR_SIGNING'))
            p.start();self.addCleanup(p.stop)


class IntegratedSafetyTests(NoVenue):
    def test_full_closed_chain_both_accounts(self):
        for side in ('LONG','SHORT'):
            b,row=original(side);out=direct(b,row,mark='101' if side=='LONG' else '99')
            self.assertEqual(out['status'],'CHECKED_NO_ACTION_NEEDED')
            self.assertEqual(out['handoff_states'],['ORIGINAL_GROUP_RETIRED'])
            self.assertEqual(out['linked_cards'],1);self.assertFalse(out['execution_authorized'])
            self.assertTrue(out['display_copy']['cards'][0]['closure_verified'])
            self.assertEqual(out['order_requests_sent'],0)

    def test_native_healthy_pair_is_not_replaced(self):
        b,row=original();out=direct(b,row,snap=opened(b))
        self.assertEqual(out['handoff_states'],['NATIVE_EXITS_VERIFIED'])
        self.assertEqual(out['correction_proposals'],0)
        self.assertFalse(out['requires_review'])

    def test_partial_dormant_pair_requests_decision_without_cancellation(self):
        b,row=original()
        snap=snapshot(b,fills=[fill(b,qty='2')],opens=[order(b,'ENTRY','3'),
            order(b,'STOP',state='WAITING_PARENT'),order(b,'TAKE_PROFIT',state='WAITING_PARENT')],position='2')
        out=direct(b,row,snap=snap)
        self.assertEqual(out['partial_cards'],1)
        self.assertEqual(out['handoff_states'],['POLICY_APPROVAL_REQUIRED'])
        self.assertIn('PARTIAL_FILL_POLICY_PENDING_OWNERS_DECISION',out['reasons'])
        self.assertEqual(out['correction_proposals'],0);self.assertFalse(out['dispatch_enabled'])

    def test_immutable_original_identity_cannot_be_replaced_by_same_prices(self):
        b,row=original();b['card_digest']='a'*64
        with self.assertRaises(s.SafetyError):direct(b,row)

    def test_missing_execution_registration_is_not_inferred(self):
        b,row=original();out=direct(b,row,links={})
        self.assertIn('REGISTERED_ORDER_GROUPING_REQUIRED',out['reasons'])
        self.assertFalse(out['execution_authorized'])

    def test_bad_mark_never_reuses_an_old_success(self):
        b,row=original();links=s.links_from_records([b],[row])
        for bad in (None,{**sample(),'symbol':'BTC'},{**sample(),'mark_price':'NaN'},
                    sample(at=T-16000),{**sample(),'completed_at_ms':T+1}):
            with self.assertRaises(life.LifecycleError):
                s.assess([b],closed(b),bad,links,None,revision=1,now_ms=T)

    def test_pending_uncertainty_blocks_even_a_flat_card(self):
        b,row=original();pending=dict(bucket=RecoveryJournal.bucket(A,'DOGE'),phase='OUTCOME_UNKNOWN')
        out=direct(b,row,pending=pending)
        self.assertIn('DURABLE_REQUEST_MUST_BE_RESOLVED_BEFORE_NEW_WORK',out['reasons'])
        self.assertEqual(out['pending_phase'],'OUTCOME_UNKNOWN')

    def test_other_account_pending_evidence_rejected(self):
        b,row=original()
        with self.assertRaises(s.SafetyError):direct(b,row,pending=dict(bucket=RecoveryJournal.bucket(B,'DOGE'),phase='OUTCOME_UNKNOWN'))

    def test_display_edit_does_not_modify_source_or_pending(self):
        b,row=original();before=deepcopy((b,row));out=direct(b,row)
        out['display_copy']['cards'][0]['prices']['stop']='1'
        out['display_copy']['cards'].clear()
        self.assertEqual((b,row),before)
        self.assertEqual(direct(b,row)['display_copy']['cards'][0]['prices']['stop'],'98')

    def test_log_summary_never_exports_authority_or_card_records(self):
        b,row=original();text=json.dumps(s.summary(direct(b,row)))
        for secret in (A,b['card_id'],'display_copy','order_id','client_reference'):
            self.assertNotIn(secret,text)

    def test_configuration_blocks_all_executing_modes(self):
        self.assertTrue(s.config(env()))
        for mode in ('single_testnet_attempt_v1','cancel_monitor_testnet_v1','cancel_rehearsal_testnet_v1'):
            with self.assertRaises(s.SafetyError):s.config({**env(),'HL_TESTNET_RUNTIME_MODE':mode})

    def test_unknown_option_cannot_turn_review_into_execution(self):
        for k,v in [('HL_TESTNET_SAFETY_PIPELINE','on'),('HL_TESTNET_TWO_ACCOUNT_EXECUTION','approved_single_attempt_v1'),
                    ('RENDER_SERVICE_ID','other-service')]:
            with self.assertRaises(s.SafetyError):s.config({**env(),k:v})

    def test_startup_guard_runs_before_any_thread(self):
        with patch.dict(os.environ,{**env(),'HL_TESTNET_RUNTIME_MODE':'single_testnet_attempt_v1'},clear=True),\
             patch('threading.Thread.start',side_effect=AssertionError('MUST_NOT_START')):
            with self.assertRaises(s.SafetyError):gunicorn_conf.post_worker_init(None)

    def test_explicit_submit_is_blocked_before_storage_or_wallet(self):
        with patch.dict(os.environ,env(),clear=True),\
             patch.object(persistent_execution,'prepare_and_store',side_effect=AssertionError('NO_PREPARATION')):
            for role in (None,'long_account','short_account'):
                out=persistent_execution.submit_persisted({},account=A,agent=B,enable_testnet=True,account_role=role)
                self.assertEqual(out['status'],'INTEGRATED_SAFETY_NO_SEND')
                self.assertFalse(out['signing_tested']);self.assertEqual(out['order_requests_sent'],0)

    def test_market_sample_has_explicit_source_and_bounded_time(self):
        class Market:
            def read(self,kind,**kw):
                assert kind=='activeAssetData' and kw==dict(user=A,coin='DOGE')
                return dict(coin='DOGE',markPx='101')
        times=iter((T,T+2))
        out=s.market_sample(Market(),A,'DOGE',lambda:next(times))
        self.assertEqual(out['at_ms'],T);self.assertEqual(out['completed_at_ms'],T+2)


@unittest.skipUnless(CI,'Disposable loopback PostgreSQL required')
class IntegratedDatabaseTests(NoVenue):
    def setUp(self):
        super().setUp()
        self.j=PostgresJournal.for_ci(CI)
        with self.j._transaction() as conn:
            for name in (SCHEMA,RS,'hl_testnet_cards_v1','hl_testnet_execution_v1'):
                conn.execute(f'DROP SCHEMA IF EXISTS {name} CASCADE')
        self.j.bootstrap();self.store=SyncStore(self.j,safety_enabled=True);self.store.initialize()
        self.ls=LifecycleStore(self.j)

    def register(self,side='LONG',*,is_open=True,source=None,metadata=META):
        b,row=original(side,source=source,metadata=metadata)
        account,key,prepared,action=row
        saved,_=self.j.save_prepared(account,prepared);self.assertEqual(saved,key)
        self.j.reserve(key,action)  # Local simulated registration only, NEVER a send.
        ev=evidence(b,is_open=is_open)
        self.ls.save(ev['bindings'],ev['snapshot'],expected_revision=0,now_ms=T)
        return b,row,ev

    def due(self):
        with self.j._transaction() as conn:conn.execute(f'UPDATE {TABLE} SET next_check=NULL')

    def run_tick(self,ev,now=T+10000,store=None):
        b=ev['bindings'][0]
        class Market:
            def read(self,kind,**kw):
                assert kind=='activeAssetData' and kw==dict(user=b['account'],coin=b['symbol'])
                return dict(coin=b['symbol'],markPx='101' if b['side']=='LONG' else '99')
        return worker.tick(store or self.store,{b['role']:dict(account=b['account'])},
            reader_factory=lambda:Reader(ev),market_reader_factory=Market,clock=lambda:now)

    def persisted(self):
        with self.j._transaction() as conn:
            return conn.execute(f'SELECT report,cursor_ms,checked_revision FROM {TABLE}').fetchone()

    def test_raw_open_to_closed_whole_chain_and_restart(self):
        b,row,ev=self.register();original_hash=life.digest(self.j.load(row[1]))
        out=self.run_tick(ev)
        self.assertEqual(out['safety']['handoff_states'],['NATIVE_EXITS_VERIFIED'])
        self.due();target=evidence(b)
        out=self.run_tick(target,now=T+20000)
        self.assertEqual(out['status'],'SYNC_PASS_COMPLETED')
        self.assertEqual(out['card_states'],{'CLOSED':1});self.assertEqual(out['public_reads'],13)
        report,cursor,revision=self.persisted()
        self.assertEqual((cursor,revision),(T+20000,2))
        self.assertTrue(report['safety']['display_copy']['cards'][0]['closure_verified'])
        self.due();fresh=SyncStore(PostgresJournal.for_ci(CI),safety_enabled=True)
        out=self.run_tick(target,now=T+30000,store=fresh)
        self.assertFalse(out['changed']);self.assertEqual(self.persisted()[2],2)
        self.assertEqual(life.digest(self.j.load(row[1])),original_hash)
        with self.j._transaction() as conn:
            self.assertEqual(conn.execute(f'SELECT count(*) FROM {RS}.requests').fetchone()[0],0)

    def test_new_alert_is_recorded_not_registered_or_sent(self):
        from .trade_card_store import CardStore
        from . import alert_cards_intake as intake
        from .test_alert_cards_intake import request,invoke,ENV
        from alert_cards_forwarder_selftest import delivery
        from .test_trade_cards_phase1 import META as dm
        cards=CardStore(self.j);cards.initialize();intake.initialize(self.j)
        for side in ('LONG','SHORT'):
            value=delivery(side,identity='whole-chain-'+side)
            with patch.dict(os.environ,{**ENV,**env()},clear=True),\
                 patch.object(intake.PostgresJournal,'from_env',return_value=self.j),\
                 patch.object(intake,'metadata',return_value=dm):
                # accept's metadata default is a function object; patch its globals
                # via the narrow cache so the real authenticated accept path runs.
                with patch.object(intake,'_META',dm),patch.object(intake,'_META_AT',float('inf')):
                    status,result=invoke(request(value))
            self.assertEqual(status,'200 OK')
            self.assertIn(result['status'],('RECORDED','DUPLICATE'))
            receipt=intake.ReceiptStore(self.j).get(result['receipt_id'])
            card=cards.load(receipt['card_id'])
            self.assertEqual(card['state'],'RECORDED_ONLY')
            self.assertEqual(card['risk']['planned_usd'],'10')
            self.assertIsNone(card['actual_execution'])
            with self.j._transaction() as conn:
                self.assertEqual(conn.execute('SELECT count(*) FROM hl_testnet_execution_v1.attempts').fetchone()[0],0)
        out=worker.tick(self.store,{},reader_factory=lambda:(_ for _ in ()).throw(AssertionError('NO_REGISTERED_EXECUTIONS')))
        self.assertEqual(out['status'],'WAITING_FOR_REGISTERED_EXECUTIONS')
        self.assertEqual(out['public_reads'],0)
        # Feed the same preserved plan into a SOFTWARE-ONLY execution fixture.
        for side in ('LONG','SHORT'):
            value=delivery(side,identity='whole-chain-'+side)
            import alert_cards_wire as wire
            import hashlib
            receipt=intake.ReceiptStore(self.j).get(hashlib.sha256(wire.encoded(value)).hexdigest())
            card=cards.load(receipt['card_id'])
            b,row,ev=self.register(side,source=card['prepared']['source'],metadata=dm)
            # Restrict each test pass to the newly registered bucket by next_check.
            with self.j._transaction() as conn:
                conn.execute(f"UPDATE {TABLE} SET next_check=clock_timestamp()+interval '1 hour'")
            out=self.run_tick(evidence(b))
            self.assertEqual(out['status'],'SYNC_PASS_COMPLETED')
            self.assertEqual(out['safety']['linked_cards'],1)
            self.assertFalse(out['safety']['execution_authorized'])

    def test_pipeline_failure_does_not_commit_new_closure(self):
        b,row,ev=self.register()
        with patch.object(s,'from_database',side_effect=ValueError('DO_NOT_LOG_SECRET')):
            out=self.run_tick(evidence(b))
        self.assertEqual(out['status'],'SYNC_REQUIRES_REVIEW')
        self.assertNotIn('DO_NOT_LOG_SECRET',json.dumps(out))
        self.assertEqual(self.ls.load(A,'DOGE',now_ms=T)['revision'],1)
        self.assertIsNone(self.persisted()[1]);self.assertEqual(self.store.summary()['fresh_verified_buckets'],0)

    def test_unverified_grouping_keeps_checkpoint_and_blocks_success(self):
        ev=evidence();self.ls.save(ev['bindings'],ev['snapshot'],expected_revision=0,now_ms=T)
        out=self.run_tick(ev)
        self.assertEqual(out['status'],'SYNC_REQUIRES_REVIEW')
        self.assertIn('REGISTERED_ORDER_GROUPING_REQUIRED',out['safety']['reasons'])
        self.assertIsNone(self.persisted()[1])

    def test_durable_pending_request_reaches_integrated_gate(self):
        b,row,ev=self.register(is_open=False)
        other=binding();snap=partial(other,stop=False)
        RecoveryJournal(self.j).prepare([other],snap,controls([other],snap),
            event_id='synthetic-unresolved',expected_revision=0,now_ms=T)
        out=self.run_tick(ev)
        self.assertEqual(out['safety']['pending_phase'],'PREPARED_NOT_SENT')
        self.assertEqual(out['status'],'SYNC_REQUIRES_REVIEW')
        with self.j._transaction() as conn:
            self.assertEqual(conn.execute(f'SELECT count(*) FROM {RS}.requests').fetchone()[0],1)

    def test_old_lifecycle_only_success_is_not_integrated_success(self):
        b,row,ev=self.register(is_open=False);plain=SyncStore(self.j)
        claim=plain.claim();plain.save(claim,collected(ev),now_ms=T+10000)
        self.assertEqual(plain.summary()['fresh_verified_buckets'],1)
        self.assertEqual(self.store.summary()['fresh_verified_buckets'],0)

    def test_unknown_child_status_is_blocked_without_inventing_activation(self):
        b,row,ev=self.register();reader=Reader(ev)
        reader.data['statuses'][b['orders']['STOP'][0]]={'status':'unknownOid'}
        out=worker.tick(self.store,{b['role']:dict(account=b['account'])},reader_factory=lambda:reader,
            market_reader_factory=lambda:(_ for _ in ()).throw(AssertionError('NO_MARK_ON_FAILED_EVIDENCE')),clock=lambda:T+10000)
        self.assertEqual(out['status'],'SYNC_REQUIRES_REVIEW')
        self.assertEqual(out['reason'],'ORDER_STATUS_UNRESOLVED');self.assertEqual(out['order_requests_sent'],0)

    def test_stale_market_failure_keeps_original_and_no_success(self):
        b,row,ev=self.register()
        class Market:
            def read(self,*a,**kw):return dict(markPx='101')
        times=iter((T+10000,T+10000,T+10000,T+30000,T+30000))
        # Test the exact atomic save boundary with a deliberately stale sample.
        claim=self.store.claim();obs=collected(ev);obs['safety_sample']=sample(at=T-20000)
        with self.assertRaises(JournalError):self.store.save(claim,obs,now_ms=T+10000)
        self.assertEqual(self.ls.load(A,'DOGE',now_ms=T)['revision'],1)
