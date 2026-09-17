"""Software-only durability/race/fault tests. PostgreSQL is disposable loopback.

No user account/key, exchange HTTP, app writes, or live recovery dispatch. A
simulated attempt only marks a database record; it never sends an instruction.
"""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
import inspect
import json
import os
import subprocess
import sys
import unittest
from unittest.mock import patch
from . import card_recovery_journal as m, card_exit_recovery as recovery
from .postgres_journal import PostgresJournal, JournalError
from .test_card_exit_recovery import controls, partial, partly_closed, ack, merged
from .test_card_lifecycle import binding, fill, order, terminal, closed, T, A, B

CI = os.environ.get('HL_JOURNAL_CI_URL')
UNKNOWN = dict(state='OUTCOME_UNKNOWN',code=None,oid=None)
REJECTED = dict(state='REJECTED',code='MINIMUM_NOTIONAL',oid=None)


def fixture():
    b = binding(); s = partial(b,stop=False)
    return [b],s,controls([b],s)


def applied(bs,s,oid='901',late_fill=False):
    bs,s = deepcopy((bs,s)); b=bs[0]; b['orders']['STOP'].append(oid)
    s['open_orders'].append({**order(b,'STOP','40'),'oid':oid})
    if late_fill:
        s['fills'].append({**fill(b,qty='10',fid='late'), 'at_ms':T+2})
        s['open_orders'][0]['quantity']='50'; s['position_quantity']='50'
    s['at_ms']=T+3
    return bs,s,controls(bs,s)


class RecoveryJournalUnitTests(unittest.TestCase):
    def test_module_has_no_sender_environment_timer_or_startup(self):
        source=inspect.getsource(m)
        for text in ('import os','import http','import requests','import threading',
                     'sign_l1_action','HL_TESTNET_AGENT_KEY','HL_TESTNET_SHORT_AGENT_KEY',
                     'submit_persisted','/exchange','def startup','def start('):
            self.assertNotIn(text,source)
        self.assertFalse(hasattr(m.RecoveryJournal,'send'))

    def test_bucket_separates_accounts_and_symbols(self):
        self.assertNotEqual(m.RecoveryJournal.bucket(A,'DOGE'),m.RecoveryJournal.bucket(B,'DOGE'))
        self.assertNotEqual(m.RecoveryJournal.bucket(A,'DOGE'),m.RecoveryJournal.bucket(A,'BTC'))

    def test_invalid_reply_never_stores_arbitrary_remote_text(self):
        for item in ({**ack(1),'secret':'DO_NOT_STORE'},dict(state='REJECTED',code='raw private message',oid=None),
                     {**UNKNOWN,'oid':'1'},ack(2**64),{**ack(1),'oid':True}):
            with self.assertRaises((m.life.LifecycleError,ValueError)):m._reply(item)

    def test_mixed_account_evidence_rejected(self):
        bs,s,c=fixture()
        with self.assertRaises(m.RecoveryStorageError):m._evidence(bs+[binding(2,side='SHORT')],s,c)

    def test_record_size_is_bounded(self):
        with self.assertRaises(m.RecoveryStorageError):m._encode('x'*262145)


@unittest.skipUnless(CI,'Requires disposable loopback PostgreSQL')
class DurableRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.journal=PostgresJournal.for_ci(CI)
        with self.journal._transaction() as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS {m.SCHEMA} CASCADE')
            conn.execute('DROP SCHEMA IF EXISTS hl_testnet_execution_v1 CASCADE')
        self.journal.bootstrap()
        self.store=m.RecoveryJournal(self.journal);self.store.initialize()
        self.bs,self.s,self.c=fixture()
        for target in ('http.client.HTTPSConnection','socket.create_connection'):
            guard=patch(target,side_effect=AssertionError('EXCHANGE_NETWORK_FORBIDDEN'))
            guard.start();self.addCleanup(guard.stop)

    def prepare(self,event='prepare',rev=0,now=T,ev=None):
        return self.store.prepare(*(ev or (self.bs,self.s,self.c)),event_id=event,expected_revision=rev,now_ms=now)

    def begin(self,p,event='attempt',now=T+1,ev=None):
        return self.store.begin_simulated_attempt(p['request']['request_id'],*(ev or (self.bs,self.s,self.c)),
            event_id=event,expected_revision=p['revision'],now_ms=now)

    def reply(self,p,value=None,event='reply',now=T+2):
        return self.store.record_reply(p['request']['request_id'],value or ack(901),
            event_id=event,expected_revision=p['revision'],now_ms=now)

    def confirm(self,p,ev=None,event='confirm',now=T+3):
        return self.store.confirm_observed(p['request']['request_id'],*(ev or applied(self.bs,self.s)),
            event_id=event,expected_revision=p['revision'],now_ms=now)

    def counts(self):
        with self.journal._transaction() as conn:
            return tuple(conn.execute(f'SELECT count(*) FROM {m.SCHEMA}.{name}').fetchone()[0]
                         for name in ('requests','events'))

    @contextmanager
    def fault(self,*,after_commit,call=1):
        original=self.journal._transaction; count=0
        @contextmanager
        def transaction():
            nonlocal count
            count+=1; fail=count==call
            with original() as conn:
                yield conn
                if fail and not after_commit:raise JournalError('TEST_ROLLBACK')
            if fail and after_commit:raise JournalError('TEST_LOST_COMMIT_ACK')
        with patch.object(self.journal,'_transaction',transaction):yield

    def test_explicit_migration_repeats_without_deleting_data(self):
        p=self.prepare();self.assertFalse(self.store.initialize())
        self.assertEqual(self.store.load(p['request']['request_id'])['request'],p['request'])

    def test_prepare_preserves_exact_terms_but_never_marks_attempt(self):
        p=self.prepare();v=p['request']
        self.assertEqual(v['phase'],'PREPARED_NOT_SENT');self.assertIsNone(v['attempt_started_at_ms'])
        self.assertEqual(v['simulated_attempt_count'],0)
        self.assertEqual(v['original_evidence'],dict(bindings=self.bs,snapshot=self.s,context=self.c))
        step=v['proposal']['next_step'];self.assertEqual(step['target_quantity'],'40')
        self.assertEqual(step['original_prices'],self.bs[0]['prices'])
        self.assertFalse(p['dispatch_enabled']);self.assertFalse(p['live_recovery_ready'])

    def test_repeated_prepare_with_old_timestamp_is_idempotent_not_fresh_authority(self):
        p=self.prepare();q=self.prepare(now=T+86400000)
        self.assertTrue(q['duplicate']);self.assertEqual(p['request'],q['request'])
        self.assertEqual(self.counts(),(1,1))
        with self.assertRaises(m.RecoveryStorageError):self.begin(q,now=T+86400000)

    def test_same_event_with_changed_input_is_rejected_without_overwrite(self):
        p=self.prepare();ev=deepcopy((self.bs,self.s,self.c));ev[2]['mark_price']='100.5'
        with self.assertRaises(m.RecoveryStorageError):self.prepare(ev=ev)
        self.assertEqual(self.store.load(p['request']['request_id'])['request'],p['request'])

    def test_active_request_blocks_new_request_even_with_controls_none(self):
        self.prepare()
        with self.assertRaises(m.RecoveryStorageError):self.prepare(event='another',rev=1)
        self.assertEqual(self.counts(),(1,1))

    def test_expired_unsent_plan_requires_fresh_replan(self):
        p=self.prepare()
        with self.assertRaises(m.RecoveryStorageError):self.begin(p,now=T+16000)
        self.assertEqual(self.store.pending(A,'DOGE')['request']['phase'],'PREPARED_NOT_SENT')

    def test_partial_fill_change_between_prepare_and_attempt_blocks_old_quantity(self):
        p=self.prepare();bs,s,c=deepcopy((self.bs,self.s,self.c))
        s['fills'].append(fill(bs[0],qty='10',fid='new'));s['position_quantity']='50'
        s['open_orders'][0]['quantity']='50';s['at_ms']=T+1;c=controls(bs,s)
        with self.assertRaises(m.RecoveryStorageError):self.begin(p,ev=(bs,s,c))
        self.assertEqual(self.store.load(p['request']['request_id'])['request']['simulated_attempt_count'],0)

    def test_attempt_marker_is_unknown_before_any_response(self):
        p=self.begin(self.prepare());self.assertEqual(p['request']['phase'],'OUTCOME_UNKNOWN')
        self.assertEqual(p['restart_action'],'RECONCILE_DO_NOT_RESEND')
        self.assertEqual(p['request']['simulated_attempt_count'],1);self.assertEqual(p['order_requests_sent'],0)

    def test_duplicate_attempt_marker_never_creates_second_attempt(self):
        p=self.prepare();first=self.begin(p);again=self.begin(p)
        self.assertTrue(again['duplicate']);self.assertEqual(again['request'],first['request'])
        with self.assertRaises(m.RecoveryStorageError):self.begin(first,event='another-attempt')
        self.assertEqual(self.counts(),(1,2))

    def test_unknown_does_not_expire_into_permission_to_resend(self):
        p=self.begin(self.prepare());p=self.reply(p,UNKNOWN)
        with self.assertRaises(m.RecoveryStorageError):self.begin(p,event='later',now=T+86400000)
        with self.assertRaises(m.RecoveryStorageError):
            self.store.abandon_unsent(p['request']['request_id'],event_id='discard',expected_revision=p['revision'],now_ms=T+86400000)
        self.assertEqual(self.store.pending(A,'DOGE')['request']['phase'],'OUTCOME_UNKNOWN')

    def test_receipt_is_not_confirmation_or_protection(self):
        p=self.reply(self.begin(self.prepare()))
        self.assertEqual(p['request']['phase'],'WAITING_FOR_EVIDENCE');self.assertIsNone(p['request']['confirmation'])
        with self.assertRaises(m.RecoveryStorageError):self.confirm(p,ev=(self.bs,self.s,self.c))

    def test_unknown_then_late_ack_advances_same_request_without_retry(self):
        p=self.reply(self.begin(self.prepare()),UNKNOWN)
        q=self.reply(p,ack(901),event='late-ack',now=T+3)
        self.assertEqual(q['request']['phase'],'WAITING_FOR_EVIDENCE')
        self.assertEqual(q['request']['simulated_attempt_count'],1)
        self.assertEqual(q['request']['request_id'],p['request']['request_id'])

    def test_late_timeout_cannot_downgrade_known_ack(self):
        p=self.reply(self.begin(self.prepare()))
        q=self.reply(p,UNKNOWN,event='late-timeout',now=T+3)
        self.assertEqual(q['request']['phase'],'WAITING_FOR_EVIDENCE');self.assertEqual(q['request']['reply'],ack(901))

    def test_rejection_persists_without_price_change_or_release(self):
        p=self.reply(self.begin(self.prepare()),REJECTED)
        self.assertEqual(p['request']['phase'],'REJECTED_DO_NOT_RETRY')
        self.assertEqual(p['request']['proposal']['next_step']['original_price'],'98')
        with self.assertRaises(m.RecoveryStorageError):self.prepare(event='retry',rev=p['revision'])

    def test_conflicting_replies_are_committed_not_lost_on_rollback(self):
        p=self.reply(self.begin(self.prepare()))
        q=self.reply(p,ack(902),event='conflict',now=T+3)
        loaded=m.RecoveryJournal(PostgresJournal.for_ci(CI)).load(p['request']['request_id'])
        self.assertEqual(loaded['request']['phase'],'CONFLICT_RECONCILIATION_REQUIRED')
        self.assertEqual(loaded['request']['reply'],ack(901));self.assertEqual(loaded['request']['conflicting_reply'],ack(902))
        with self.journal._transaction() as conn:
            row=conn.execute(f'SELECT record FROM {m.SCHEMA}.events WHERE event_id=%s',('conflict',)).fetchone()[0]
        self.assertEqual(row['received_reply'],ack(902))
        r=self.reply(q,ack(901),event='repeat-original',now=T+4)
        self.assertEqual(r['request']['phase'],'CONFLICT_RECONCILIATION_REQUIRED')

    def test_unstarted_request_does_not_accept_reply(self):
        p=self.prepare()
        with self.assertRaises(m.RecoveryStorageError):self.reply(p)
        self.assertEqual(self.counts(),(1,1))

    def test_stale_worker_reply_cannot_overwrite_newer_state(self):
        p=self.begin(self.prepare());q=self.reply(p)
        with self.assertRaises(m.RecoveryStorageError):self.reply(p,REJECTED,event='stale-reply')
        self.assertEqual(self.store.load(p['request']['request_id'])['request'],q['request'])

    def test_confirmation_releases_only_after_new_observed_effect(self):
        p=self.reply(self.begin(self.prepare()));q=self.confirm(p)
        self.assertEqual(q['request']['phase'],'OBSERVED')
        self.assertTrue(q['request']['confirmation']['acknowledged_effect_observed'])
        self.assertIsNone(self.store.pending(A,'DOGE')['request'])
        again=self.confirm(p);self.assertTrue(again['duplicate']);self.assertEqual(self.counts(),(1,4))

    def test_wrong_order_cannot_confirm_other_cards_correction(self):
        p=self.reply(self.begin(self.prepare()));ev=applied(self.bs,self.s,oid='902')
        with self.assertRaises(m.RecoveryStorageError):self.confirm(p,ev=ev)
        self.assertEqual(self.store.pending(A,'DOGE')['request']['phase'],'WAITING_FOR_EVIDENCE')

    def test_confirm_cannot_forget_previously_recorded_fill(self):
        p=self.reply(self.begin(self.prepare()));bs,s,c=applied(self.bs,self.s)
        s['fills'][0]['price']='99'
        with self.assertRaises(m.RecoveryStorageError):self.confirm(p,ev=(bs,s,c))
        self.assertEqual(self.counts(),(1,3))

    def test_old_snapshot_cannot_create_duplicate_after_confirmation(self):
        p=self.confirm(self.reply(self.begin(self.prepare())))
        with self.assertRaises((m.life.LifecycleError,JournalError)):
            self.prepare(event='old-source',rev=p['revision'],now=T+4)
        self.assertEqual(self.counts(),(1,4))

    def test_late_fill_requires_new_resize_not_duplicate_creation(self):
        p=self.reply(self.begin(self.prepare()));ev=applied(self.bs,self.s,late_fill=True)
        q=self.confirm(p,ev=ev);self.assertTrue(q['request']['confirmation']['recovered_card_needs_more_work'])
        bs,s,c=deepcopy(ev);s['at_ms']=T+4;c=controls(bs,s)
        new=self.prepare(event='resize',rev=q['revision'],now=T+4,ev=(bs,s,c))
        self.assertEqual(new['request']['proposal']['next_step']['operation'],'RESIZE_EXIT')
        self.assertEqual(new['request']['proposal']['next_step']['target_quantity'],'50')
        self.assertNotEqual(new['request']['client_reference'],q['request']['client_reference'])
        self.assertEqual(self.counts(),(2,5))

    def test_only_unsent_plan_can_be_abandoned_and_replanned(self):
        p=self.prepare();q=self.store.abandon_unsent(p['request']['request_id'],event_id='abandon',expected_revision=1,now_ms=T+1)
        self.assertEqual(q['request']['phase'],'ABORTED_NOT_SENT')
        bs,s,c=deepcopy((self.bs,self.s,self.c));s['at_ms']=T+2;c=controls(bs,s)
        new=self.prepare(event='replan',rev=2,now=T+2,ev=(bs,s,c))
        self.assertNotEqual(new['request']['request_id'],p['request']['request_id'])
        self.assertEqual(self.store.load(p['request']['request_id'])['request']['phase'],'ABORTED_NOT_SENT')

    def test_completed_request_late_reply_cannot_modify_next_request(self):
        p=self.confirm(self.reply(self.begin(self.prepare())))
        with self.assertRaises(m.RecoveryStorageError):self.reply(p,ack(999),event='late',now=T+5)
        self.assertEqual(self.store.load(p['request']['request_id'])['request']['phase'],'OBSERVED')

    def test_duplicate_old_event_returns_current_not_stale_state(self):
        p=self.prepare();self.reply(self.begin(p))
        again=self.prepare(now=T+50000)
        self.assertEqual(again['request']['phase'],'WAITING_FOR_EVIDENCE');self.assertTrue(again['duplicate'])
        self.assertFalse(again['dispatch_enabled'])

    def test_two_accounts_have_independent_persistent_requests(self):
        first=self.prepare();b=binding(2,side='SHORT');s=partial(b,stop=False)
        second=self.prepare(event='short',ev=([b],s,controls([b],s,mark='99')))
        self.assertNotEqual(first['request']['bucket'],second['request']['bucket'])
        self.assertEqual(self.store.pending(B,'DOGE')['request']['proposal']['next_step']['side'],'B')
        self.assertEqual(self.counts(),(2,2))

    def test_two_cards_same_bucket_cannot_have_competing_corrections(self):
        a,b=binding(),binding(2);sa,sb=partly_closed(a),partly_closed(b)
        s=merged(a,sa,b,sb,'120');ev=([a,b],s,controls([a,b],s))
        p=self.prepare(ev=ev)
        self.assertEqual(p['request']['proposal']['next_step']['card_id'],a['card_id'])
        with self.assertRaises(m.RecoveryStorageError):self.prepare(event='competing',rev=1,ev=ev)
        self.assertEqual(p['request']['original_evidence']['bindings'][1],b)

    def test_resize_uses_same_durable_workflow(self):
        b=binding();s=partly_closed(b);ev=([b],s,controls([b],s))
        p=self.prepare(ev=ev);p=self.begin(p,ev=ev);p=self.reply(p,ack(b['orders']['STOP'][0]))
        s=deepcopy(s);s['at_ms']=T+3
        for item in s['open_orders']:
            if item['oid']==b['orders']['STOP'][0]:item['quantity']='60'
        q=self.confirm(p,ev=([b],s,controls([b],s)))
        self.assertEqual(q['request']['phase'],'OBSERVED')

    def test_orphan_cancel_confirms_only_exact_terminal_order(self):
        b=binding();s=closed(b);oid=b['orders']['STOP'][0]
        s['terminal_orders']=[t for t in s['terminal_orders'] if t['oid']!=oid]
        s['open_orders']=[order(b,'STOP')];ev=([b],s,controls([b],s))
        p=self.prepare(ev=ev);self.assertEqual(p['request']['proposal']['next_step']['operation'],'CANCEL_ORPHAN_EXIT')
        p=self.reply(self.begin(p,ev=ev),ack(oid))
        s=deepcopy(s);s['at_ms']=T+3;s['open_orders']=[]
        s['terminal_orders'].append(terminal(b,'STOP','0'))
        q=self.confirm(p,ev=([b],s,controls([b],s)))
        self.assertEqual(q['request']['phase'],'OBSERVED')

    def test_missing_secret_or_extra_input_is_not_accepted_into_journal(self):
        bs,s,c=deepcopy((self.bs,self.s,self.c));c['private_key']='DO_NOT_STORE'
        with self.assertRaises(m.life.LifecycleError):self.prepare(ev=(bs,s,c))
        self.assertEqual(self.counts(),(0,0))

    def test_hard_process_exit_after_marker_preserves_uncertainty(self):
        program="""import os
from hl_testnet_runtime.postgres_journal import PostgresJournal
from hl_testnet_runtime.card_recovery_journal import RecoveryJournal
from hl_testnet_runtime.test_card_recovery_journal import fixture,T
s=RecoveryJournal(PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL']))
ev=fixture();p=s.prepare(*ev,event_id='child-prepare',expected_revision=0,now_ms=T)
s.begin_simulated_attempt(p['request']['request_id'],*ev,event_id='child-attempt',expected_revision=1,now_ms=T+1)
os._exit(23)
"""
        out=subprocess.run([sys.executable,'-c',program],capture_output=True,text=True,timeout=10)
        self.assertEqual(out.returncode,23,out.stderr)
        p=self.store.pending(A,'DOGE')
        self.assertEqual(p['request']['phase'],'OUTCOME_UNKNOWN');self.assertEqual(p['request']['simulated_attempt_count'],1)
        with self.assertRaises(m.RecoveryStorageError):self.begin(p,event='parent-retry')

    def test_database_outage_does_not_replace_pending_with_empty_state(self):
        p=self.begin(self.prepare())
        with patch.object(self.journal,'_transaction',side_effect=JournalError('SIMULATED_STORAGE_DOWN')):
            with self.assertRaises(JournalError):self.store.pending(A,'DOGE')
            with self.assertRaises(JournalError):self.begin(p,event='during-outage')
        self.assertEqual(self.store.pending(A,'DOGE')['request']['phase'],'OUTCOME_UNKNOWN')
        self.assertEqual(self.counts(),(1,2))

    def test_parent_linked_handoff_is_still_blocked(self):
        bs,s,c=deepcopy((self.bs,self.s,self.c));c['cards'][bs[0]['card_id']]['grouping']='parent_linked'
        with self.assertRaises(m.RecoveryStorageError):self.prepare(ev=(bs,s,c))
        self.assertEqual(self.counts(),(0,0))

    def test_concurrent_duplicate_prepare_creates_one_request(self):
        with ThreadPoolExecutor(max_workers=5) as pool:
            out=list(pool.map(lambda _:self.prepare(),range(5)))
        self.assertEqual(sum(not x['duplicate'] for x in out),1);self.assertEqual(self.counts(),(1,1))

    def test_concurrent_different_prepare_only_one_wins(self):
        def send(n):
            try:self.prepare(event='p'+str(n));return 'saved'
            except m.RecoveryStorageError:return 'blocked'
        with ThreadPoolExecutor(max_workers=5) as pool:out=list(pool.map(send,range(5)))
        self.assertEqual(out.count('saved'),1);self.assertEqual(self.counts(),(1,1))

    def test_concurrent_attempt_markers_only_one_wins(self):
        p=self.prepare()
        def send(n):
            try:self.begin(p,event='a'+str(n));return 'saved'
            except m.RecoveryStorageError:return 'blocked'
        with ThreadPoolExecutor(max_workers=5) as pool:out=list(pool.map(send,range(5)))
        self.assertEqual(out.count('saved'),1)
        self.assertEqual(self.store.load(p['request']['request_id'])['request']['simulated_attempt_count'],1)

    def test_crash_before_prepare_commit_rolls_back_all_parts(self):
        with self.fault(after_commit=False):
            with self.assertRaises(JournalError):self.prepare()
        self.assertEqual(self.counts(),(0,0));self.assertEqual(self.store.pending(A,'DOGE')['revision'],0)

    def test_lost_prepare_commit_ack_replay_finds_original(self):
        with self.fault(after_commit=True):
            with self.assertRaises(JournalError):self.prepare()
        p=self.prepare();self.assertTrue(p['duplicate']);self.assertEqual(self.counts(),(1,1))

    def test_crash_before_attempt_commit_keeps_unsent_not_partial_state(self):
        p=self.prepare()
        with self.fault(after_commit=False,call=2):
            with self.assertRaises(JournalError):self.begin(p)
        self.assertEqual(self.store.load(p['request']['request_id'])['request']['phase'],'PREPARED_NOT_SENT')
        self.assertEqual(self.counts(),(1,1))

    def test_lost_attempt_commit_ack_remains_unknown_no_retry(self):
        p=self.prepare()
        with self.fault(after_commit=True,call=2):
            with self.assertRaises(JournalError):self.begin(p)
        q=self.begin(p);self.assertTrue(q['duplicate']);self.assertEqual(q['request']['phase'],'OUTCOME_UNKNOWN')
        self.assertEqual(q['request']['simulated_attempt_count'],1);self.assertEqual(self.counts(),(1,2))

    def test_lost_reply_commit_ack_keeps_known_ack(self):
        p=self.begin(self.prepare())
        with self.fault(after_commit=True,call=2):
            with self.assertRaises(JournalError):self.reply(p)
        q=self.reply(p);self.assertTrue(q['duplicate']);self.assertEqual(q['request']['phase'],'WAITING_FOR_EVIDENCE')
        self.assertEqual(self.counts(),(1,3))

    def test_lost_confirmation_ack_does_not_repeat_or_reopen(self):
        p=self.reply(self.begin(self.prepare()))
        with self.fault(after_commit=True,call=2):
            with self.assertRaises(JournalError):self.confirm(p)
        q=self.confirm(p);self.assertTrue(q['duplicate']);self.assertEqual(q['request']['phase'],'OBSERVED')
        self.assertIsNone(self.store.pending(A,'DOGE')['request']);self.assertEqual(self.counts(),(1,4))

    def test_restart_in_separate_process_reads_database_not_exported_json(self):
        for value in (None,UNKNOWN,ack(901),REJECTED):
            # Each iteration uses the same independently reconstructed pending request.
            if value is None:p=self.begin(self.prepare())
            elif value==UNKNOWN:p=self.reply(p,UNKNOWN)
            elif value==ack(901):p=self.reply(p,value,event='late')
            else:p=self.reply(p,value,event='conflicting-rejection')
            program='''import json,os,sys
from hl_testnet_runtime.postgres_journal import PostgresJournal
from hl_testnet_runtime.card_recovery_journal import RecoveryJournal
s=RecoveryJournal(PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL']))
p=s.pending(sys.argv[1],'DOGE')
print(json.dumps({'phase':p['request']['phase'],'reference':p['request']['client_reference'],'attempts':p['request']['simulated_attempt_count'],'dispatch':p['dispatch_enabled']}))'''
            out=subprocess.run([sys.executable,'-c',program,A],capture_output=True,text=True,timeout=10)
            self.assertEqual(out.returncode,0,out.stderr);data=json.loads(out.stdout)
            self.assertEqual(data['phase'],p['request']['phase']);self.assertFalse(data['dispatch'])
            self.assertEqual(data['reference'],p['request']['client_reference']);self.assertEqual(data['attempts'],1)

    def test_corrupt_request_fails_closed(self):
        p=self.prepare()
        with self.journal._transaction() as conn:
            conn.execute(f"UPDATE {m.SCHEMA}.requests SET data=jsonb_set(data,'{{phase}}','\"OBSERVED\"')")
        with self.assertRaises(m.RecoveryStorageError):self.store.load(p['request']['request_id'])

    def test_corrupt_event_cannot_pass_idempotent_replay(self):
        self.prepare()
        with self.journal._transaction() as conn:
            conn.execute(f"UPDATE {m.SCHEMA}.events SET record=jsonb_set(record,'{{phase}}','\"OBSERVED\"')")
        with self.assertRaises(m.RecoveryStorageError):self.prepare()

    def test_invalid_time_or_revision_cannot_advance_request(self):
        p=self.prepare()
        with self.assertRaises(m.RecoveryStorageError):self.begin({**p,'revision':True})
        with self.assertRaises(m.RecoveryStorageError):self.reply(self.begin(p),now=T)
        self.assertEqual(self.counts(),(1,2))

    def test_initialization_is_not_implicitly_repeated_by_operations(self):
        with patch.object(self.store,'initialize',side_effect=AssertionError('NO_HOT_PATH_DDL')):
            p=self.confirm(self.reply(self.begin(self.prepare())))
        self.assertEqual(p['request']['phase'],'OBSERVED')

    def test_legacy_attempts_remain_untouched(self):
        self.confirm(self.reply(self.begin(self.prepare())))
        with self.journal._transaction() as conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM hl_testnet_execution_v1.attempts').fetchone()[0],0)


if __name__=='__main__':unittest.main(verbosity=2)
