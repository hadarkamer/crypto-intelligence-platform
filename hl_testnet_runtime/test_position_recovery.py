"""Recovery acceptance through the sole runtime and durable inbox; no real venue."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import io
import json
import os
import threading
import unittest
from unittest.mock import patch

from . import position_recovery as recovery, position_recovery_http as api
from . import test_experimental_live_runtime as support
from .experimental_live_dispatch import DefinitelyNotSubmitted, LiveDispatchError


class RecoveryContract:
    setUp=support.RuntimeContract.setUp
    make_worker=support.RuntimeContract.make_worker
    cycle=support.RuntimeContract.cycle
    start=support.RuntimeContract.start
    restart=support.RuntimeContract.restart
    protect=support.RuntimeContract.protect
    real_pg=False

    def unsent(self):
        def fail(request,*,admission=None):
            claimed=self.store.claim_transport(request,'c'*32)
            raise DefinitelyNotSubmitted(claimed,LiveDispatchError('FINAL_EVIDENCE_EXPIRED'))
        return patch.object(self.port,'send',side_effect=fail)

    def incident(self):
        msg,_=self.start();self.cid=msg['occurrence_id']
        with self.unsent(): self.cycle(False)
        return msg

    def row(self): return self.store.load()['position_recovery'][self.cid]

    def wait(self):
        self.incident()
        with self.unsent():
            for _ in range(5): self.venue.t+=1001;self.cycle(False)
        self.assertEqual(self.row()['phase'],'WAITING')

    def choose(self,choice='auto',actor='7',token=None,round_number=None):
        row=self.row()
        return self.store.mutate(lambda s: recovery.choose(s,token=token or row['event_id'],
            round_number=row['round'] if round_number is None else round_number,
            choice=choice,actor=actor,allowed_users={'7'},now=self.venue.now()))

    # Historical approval policy fixture; current roster has separate tests.
    @patch('approved_alert_contract.new_entry_allowed',return_value=True)
    def test_new_same_market_signal_has_one_refusal_notice_no_choices(self,_historical_policy):
        from .test_approved_alert_lifecycle import alert
        self.start(alert(symbol='SOL',side='SHORT',approved_ms=support.T,cycle='first'))
        peer=alert(symbol='SOL',side='SHORT',approved_ms=support.T,cycle='peer')
        self.worker.receive([peer]);self.cycle();self.restart();self.cycle()
        value=self.store.load();rows=value['position_recovery_entry_notices']
        self.assertEqual(list(rows),[peer['occurrence_id']])
        self.assertEqual(len(rows[peer['occurrence_id']]['notices']),1)
        self.assertNotIn(peer['occurrence_id'],value['trades'])
        result=self.store.mutate(lambda s:api.operate(s,{'op':'claim'},users={'7'},chat='7',now=self.venue.now()))
        self.assertEqual(result['result']['phase'],'ENTRY_REFUSED')
        self.assertFalse(result['result']['retryable'])

    def test_normal_success_has_no_incident_or_extra_dispatch(self):
        msg,_=self.start();self.protect()
        self.assertFalse(self.store.load().get('position_recovery'))
        self.assertEqual(len(self.venue.requests),3)

    def test_second_attempt_success_has_one_result_and_no_buttons_during_round(self):
        self.incident();self.venue.t+=1001;self.cycle(False)
        self.assertEqual(self.row()['phase'],'RECOVERING');self.assertFalse(self.row()['notices'])
        for _ in range(3): self.venue.t+=1001;self.cycle(False)
        row=self.row();self.assertEqual(row['phase'],'RESOLVED')
        self.assertEqual(len(row['notices']),1)
        self.assertEqual(row['facts']['exits'],{'STOP':'PRESENT','TAKE_PROFIT':'PRESENT'})
        before=len(self.venue.requests);self.restart();self.cycle(False)
        self.assertEqual(len(self.venue.requests),before)
        self.assertEqual(len(self.row()['notices']),1)

    def test_only_missing_take_profit_retries_and_stop_is_retained(self):
        msg,_=self.start();self.cid=msg['occurrence_id'];self.cycle(False)
        stop=self.venue.oid('STOP')
        with self.unsent():self.cycle(False)
        for _ in range(4):self.venue.t+=1001;self.cycle(False)
        self.assertEqual(self.row()['phase'],'RESOLVED')
        self.assertEqual(self.venue.oid('STOP'),stop)
        self.assertEqual(sum(r['proposal']['leg']=='STOP' for r in self.store.load()['requests'].values()),1)

    def test_partial_fill_change_uses_latest_quantity(self):
        from decimal import Decimal
        from .test_approved_alert_lifecycle import alert
        # This historical GTC trade predates the entry-roster withdrawal.
        import approved_alert_contract as contract
        with patch.object(contract,'new_entry_allowed',return_value=True):
            msg,_=self.start(alert(symbol='SOL',side='SHORT',approved_ms=support.T,cycle='partial-recovery'))
        self.cid=msg['occurrence_id']
        oid=self.venue.oid('ENTRY');o=self.venue.orders[oid]['view']
        half=(Decimal(o['wire_order']['s'])/2).quantize(Decimal('.01'),rounding='ROUND_DOWN')
        self.venue.fill(oid,str(half))
        with self.unsent():self.cycle(False)
        remaining=Decimal(o['wire_order']['s'])-sum(Decimal(f['quantity']) for f in o['fills'])
        self.venue.fill(oid,str(remaining))
        for _ in range(4):self.venue.t+=1001;self.cycle(False)
        self.assertEqual(self.row()['phase'],'RESOLVED')
        for leg in ('STOP','TAKE_PROFIT'):
            self.assertEqual(self.venue.orders[self.venue.oid(leg)]['view']['wire_order']['s'],o['wire_order']['s'])

    def test_position_closes_during_recovery_without_new_take_profit(self):
        msg,_=self.start();self.cid=msg['occurrence_id'];self.cycle(False)
        with self.unsent():self.cycle(False)
        self.venue.fill(self.venue.oid('STOP'),self.store.load()['trades'][self.cid]['quantity'])
        before=len(self.venue.requests)
        for _ in range(3):self.venue.t+=1001;self.cycle(False)
        self.assertEqual(self.row()['phase'],'RESOLVED');self.assertTrue(self.row()['facts']['closed'])
        self.assertEqual(len(self.venue.requests),before)

    def test_two_additional_sends_then_wait_survives_restart(self):
        self.wait();row=self.row()
        self.assertEqual(row['sends'],{'STOP':2})
        before=deepcopy(self.store.load()['requests'])
        self.restart();self.venue.t+=60001
        for _ in range(5): self.cycle(False)
        self.assertEqual(self.store.load()['requests'],before)
        self.assertEqual(self.row()['round'],1)
        self.assertEqual(len(self.row()['notices']),1)

    def test_auto_choice_records_only_then_single_owner_handles_one_new_round(self):
        self.wait();before=len(self.store.load()['requests'])
        self.assertEqual(self.choose()['status'],'RECORDED')
        self.assertEqual(self.row()['phase'],'RESUME_REQUESTED')
        self.assertEqual(len(self.store.load()['requests']),before)
        self.assertEqual(self.choose()['status'],'STALE_OR_ALREADY_APPLIED')
        self.restart()
        with self.unsent():
            for _ in range(5): self.venue.t+=1001;self.cycle(False)
        self.assertEqual(self.row()['round'],2)
        self.assertEqual(self.row()['phase'],'WAITING')
        self.assertEqual(len(self.store.load()['requests']),before+2)
        self.assertEqual(len(self.row()['notices']),2)
        self.assertEqual(self.choose(round_number=1)['status'],'STALE_OR_ALREADY_APPLIED')

    def test_manual_choice_persists_and_prevents_all_new_actions(self):
        self.wait();self.choose('manual');self.restart()
        before=deepcopy(self.store.load()['requests'])
        self.venue.t+=120000;self.cycle(False)
        self.assertEqual(self.store.load()['requests'],before)
        self.assertTrue(self.store.load()['trades'][self.cid]['manual_management'])
        self.assertEqual(self.row()['phase'],'MANUAL')
        self.assertEqual(self.choose()['status'],'STALE_OR_ALREADY_APPLIED')

    def test_parallel_double_click_has_one_durable_effect(self):
        self.wait()
        with ThreadPoolExecutor(2) as pool: results=list(pool.map(lambda _:self.choose(),range(2)))
        self.assertEqual(sorted(r['status'] for r in results),['RECORDED','STALE_OR_ALREADY_APPLIED'])
        self.assertEqual(sum(e['kind']=='POSITION_RECOVERY_CHOICE' for e in self.store.load()['events']),1)

    def test_wrong_user_and_old_position_do_not_change_state(self):
        self.wait();before=self.store.load()
        with self.assertRaisesRegex(ValueError,'NOT_AUTHORIZED|PERSISTENCE_UNAVAILABLE_NO_SEND'):self.choose(actor='8')
        self.assertEqual(self.choose(token='f'*24)['status'],'STALE_OR_ALREADY_APPLIED')
        # A successful no-op transaction may advance revision; business state stays.
        after=self.store.load();after['revision']=before['revision'];self.assertEqual(after,before)

    def test_unknown_stop_is_never_resent_and_survives_round_and_restart(self):
        msg,_=self.start();self.cid=msg['occurrence_id']
        with patch.object(self.port,'send',side_effect=TimeoutError()):self.cycle(False)
        self.venue.t+=5001;self.cycle(False)
        self.venue.t+=60001;self.cycle(False);self.restart()
        before=self.store.load()['requests'];self.assertEqual(self.row()['phase'],'WAITING')
        self.choose();self.venue.t+=1001;self.cycle(False)
        self.assertEqual(self.store.load()['requests'],before)
        self.assertTrue(any(r['phase']=='OUTCOME_UNKNOWN' for r in before.values()))
        self.assertEqual(self.row()['facts']['exits']['STOP'],'UNKNOWN')

    def test_inquiry_reservations_survive_restart_and_are_bounded(self):
        self.incident();self.venue.t+=1001
        with self.unsent():self.cycle(False)
        first=self.row();self.restart()
        for _ in range(5):
            self.store.mutate(lambda s:recovery.reserve_reads(s,self.venue.now()+1001))
        row=self.row();self.assertLessEqual(row['read_calls'],24);self.assertLessEqual(row['read_weight'],720)
        self.assertGreaterEqual(row['read_calls'],first['read_calls'])
        self.assertEqual(row['phase'],'WAITING')
        before=deepcopy(row);self.store.mutate(lambda s:recovery.reserve_reads(s,self.venue.now()+1001))
        self.assertEqual(self.row(),before)

    def test_durable_claim_rejects_manual_after_request_prepared(self):
        self.wait();self.choose()
        captured=[]
        def hold(request,*,admission=None):
            captured.append(request);raise TimeoutError()
        self.venue.t+=1001
        with patch.object(self.port,'send',side_effect=hold):self.cycle(False)
        self.store.mutate(lambda s:s['position_recovery'][self.cid].update(phase='WAITING'))
        self.choose('manual')
        with self.assertRaisesRegex(ValueError,'POSITION_MANAGEMENT_PAUSED'):
            self.store.claim_transport(captured[0],'d'*32)

    def test_existing_unknown_position_is_bounded_before_first_collection(self):
        msg,_=self.start();self.cid=msg['occurrence_id']
        with patch.object(self.port,'send',side_effect=TimeoutError()):self.cycle(False)
        self.store.mutate(lambda s:s.pop('position_recovery')) # Journal from the prior binary.
        self.venue.t+=100000
        self.assertEqual(recovery.inherited_incidents(self.store.load(),self.venue.now()),[self.cid])
        self.store.mutate(lambda s:recovery.adopt_incidents(s,self.venue.now()))
        self.assertTrue(self.row()['facts']['unresolved_requests'])
        self.assertEqual(recovery.inherited_incidents(self.store.load(),self.venue.now()),[])

    def test_existing_manual_position_is_never_adopted(self):
        msg,_=self.start();self.cid=msg['occurrence_id']
        with patch.object(self.port,'send',side_effect=TimeoutError()):self.cycle(False)
        def manual(s):
            s.pop('position_recovery');s['trades'][self.cid]['manual_management']={'status':'existing operator choice'}
        self.store.mutate(manual);self.venue.t+=100000
        self.assertEqual(recovery.inherited_incidents(self.store.load(),self.venue.now()),[])

    def test_final_transport_rechecks_deadline_and_exact_round(self):
        self.incident();row=self.row();request=next(r for r in self.store.load()['requests'].values() if r['proposal']['leg']=='STOP')
        state=self.store.load()
        self.assertTrue(recovery.transport_allowed(state,request,row['deadline_ms']-1))
        self.assertFalse(recovery.transport_allowed(state,request,row['deadline_ms']))
        request['recovery_round']=[row['event_id'],row['round']-1]
        self.assertFalse(recovery.transport_allowed(state,request,row['deadline_ms']-1))

    def test_delivery_claim_ack_and_revision_do_not_reset_execution(self):
        self.wait();before=deepcopy(self.store.load()['requests'])
        op=lambda c:self.store.mutate(lambda s:api.operate(s,c,users={'7'},chat='7',now=self.venue.now()))
        claimed=op({'op':'claim'});self.assertEqual(claimed['status'],'CLAIMED')
        self.assertEqual(op({'op':'claim'})['status'],'EMPTY')
        self.store.mutate(lambda s:recovery._finish(s,s['position_recovery'][self.cid],'RESOLVED',self.venue.now(),'EXITS_PRESENT'))
        self.assertEqual(op({'op':'claim'})['status'],'EMPTY','In-flight revision must not create another send')
        ack=dict(op='ack',id=claimed['id'],claim=claimed['claim'],revision=claimed['revision'],status='SENT',message_id=123)
        op(ack);self.restart();edit=op({'op':'claim'})
        self.assertEqual(edit['message_id'],123);self.assertEqual(edit['revision'],2)
        self.assertEqual(self.store.load()['requests'],before)

    def test_unknown_notification_does_not_resend_or_touch_exchange(self):
        self.wait();before=deepcopy(self.store.load()['requests'])
        def op(c):return self.store.mutate(lambda s:api.operate(s,c,users={'7'},chat='7',now=self.venue.now()))
        claim=op({'op':'claim'})
        op(dict(op='ack',id=claim['id'],claim=claim['claim'],revision=1,status='UNKNOWN',message_id=None))
        self.restart();self.assertEqual(op({'op':'claim'})['status'],'EMPTY')
        self.assertEqual(self.row()['notices'][0]['delivery'],'UNKNOWN')
        self.assertEqual(self.store.load()['requests'],before)

    def test_http_auth_and_recording_failure_never_wake_or_send(self):
        self.wait();row=self.row();wake=threading.Event();code=[]
        env={'POSITION_RECOVERY_API_SECRET':'a'*64,'POSITION_RECOVERY_USER_IDS':'7','POSITION_RECOVERY_CHAT_ID':'7'}
        body=json.dumps(dict(op='choose',event=row['event_id'],round=1,choice='auto',actor='7',chat='7')).encode()
        stamp=str(self.venue.now()//1000)
        def call(sig):
            e=dict(REQUEST_METHOD='POST',CONTENT_TYPE='application/json',CONTENT_LENGTH=str(len(body)),
                HTTP_X_RECOVERY_TIMESTAMP=stamp,HTTP_X_RECOVERY_SIGNATURE=sig,**{'wsgi.input':io.BytesIO(body)})
            return api.application(e,lambda status,headers:code.append(status),store=self.store,clock=self.venue.now,env=env,wake=wake)
        call('bad');self.assertEqual(code[-1],'404 Not Found');self.assertFalse(wake.is_set())
        with patch.object(self.store,'mutate',side_effect=OSError('SQL unavailable')):call(api.signature('a'*64,stamp,body))
        self.assertEqual(code[-1],'503 Service Unavailable');self.assertFalse(wake.is_set())
        call(api.signature('a'*64,stamp,body));self.assertEqual(code[-1],'200 OK');self.assertTrue(wake.is_set())
        self.assertEqual(self.row()['phase'],'RESUME_REQUESTED')


class RecoveryTests(RecoveryContract,unittest.TestCase):pass

@unittest.skipUnless(os.environ.get('HL_JOURNAL_CI_URL'),'disposable loopback PostgreSQL required')
class PostgresRecoveryTests(RecoveryContract,unittest.TestCase):real_pg=True


class InquiryMeterTests(unittest.TestCase):
    def test_weight_count_deadline_and_cached_clock(self):
        now=[10];m=recovery.InquiryMeter(dict(calls=8,weight=240,deadline=100),lambda:now[0])
        m.debit('userFillsByTime');m.debit('userFillsByTime')
        with self.assertRaisesRegex(ValueError,'RECOVERY_INQUIRY_LIMIT'):m.debit('orderStatus')
        self.assertEqual(m.calls,6)
        m=recovery.InquiryMeter(dict(calls=1,weight=240,deadline=100),lambda:now[0]);now[0]=100
        with self.assertRaisesRegex(ValueError,'RECOVERY_INQUIRY_LIMIT'):m.debit('orderStatus')
