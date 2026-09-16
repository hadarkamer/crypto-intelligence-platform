"""Offline cancellation tests. No user keys, live account or exchange writes."""
from copy import deepcopy
from decimal import Decimal, localcontext
import json
import os
import time
import unittest
from unittest.mock import patch, Mock

from . import half_threshold_cancel as policy
from . import pending_cancel_executor as executor

A = '0x'+'1'*40
ENTRY = '0x'+'a'*32


def rule(side='LONG', threshold='1.5', entry='100'):
    return policy.make_rule(rule_id='DEMO_FORMULA',event_id='fixture-1',symbol='DEMO',
        side=side,entry=entry,size='10',entry_cloid=ENTRY,threshold_pct=threshold,
        source_digest='b'*64,execution_digest='c'*64)


def observation(r=None, status='open', remaining='10', position='0', mark='100.75', age=0):
    r = rule() if r is None else r
    order = {'status':'order','order':{'status':status,'order':{
        'cloid':ENTRY,'coin':'DEMO','side':'B' if r['side']=='LONG' else 'A',
        'reduceOnly':False,'isTrigger':False,'orderType':'Limit','limitPx':r['entry'],
        'origSz':'10','sz':remaining}}}
    return dict(order=order,position_size=position,mark=mark,sample_age_seconds=age)


def record(r=None):
    r = rule() if r is None else r
    orders = []
    for i,typ in enumerate(({'limit':{'tif':'Gtc'}},
            {'trigger':{'isMarket':False,'triggerPx':'102','tpsl':'tp'}},
            {'trigger':{'isMarket':True,'triggerPx':'98','tpsl':'sl'}})):
        orders.append(dict(a=0,b=(r['side']=='LONG')==(i==0),p=r['entry'] if i==0 else str(102 if i==1 else 98),
            s='10',r=i!=0,t=typ,c=ENTRY if i==0 else '0x'+str(i)*32))
    return dict(account=A,action=dict(type='order',grouping='normalTpsl',orders=orders),
                prepared=dict(execution=dict(symbol='DEMO',side=r['side'],event_id='fixture-1')))


class RuleTests(unittest.TestCase):
    def test_half_of_each_formula_not_one_global_percent(self):
        for threshold,half in [('0.25','0.125'),('1','0.5'),('1.5','0.75'),('2','1')]:
            self.assertEqual(rule(threshold=threshold)['cancel_move_pct'],half)
    def test_long_exact_boundary_included(self):
        self.assertTrue(policy.evaluate(rule(),**observation())['cancel_candidate'])
    def test_long_below_boundary_keeps_order(self):
        self.assertFalse(policy.evaluate(rule(),**observation(mark='100.7499999'))['cancel_candidate'])
    def test_short_exact_boundary_included(self):
        r=rule(side='SHORT')
        self.assertEqual(r['cancel_price'],'99.25')
        self.assertTrue(policy.evaluate(r,**observation(r,mark='99.25'))['cancel_candidate'])
    def test_short_before_boundary_keeps_order(self):
        r=rule(side='SHORT')
        self.assertFalse(policy.evaluate(r,**observation(r,mark='99.250001'))['cancel_candidate'])
    def test_adverse_move_never_triggers(self):
        for side,mark in [('LONG','50'),('SHORT','150')]:
            r=rule(side=side)
            self.assertFalse(policy.evaluate(r,**observation(r,mark=mark))['cancel_candidate'])
    def test_uses_submitted_entry_not_a_hypothetical_fill_or_current_mark(self):
        r=rule(entry='99.8')
        self.assertEqual(r['cancel_price'],'100.5485')
        self.assertFalse(policy.evaluate(r,**observation(r,mark='100.54'))['cancel_candidate'])
    def test_formula_threshold_not_inferred_from_rounded_exits(self):
        r=rule(threshold='2',entry='0.07963')
        self.assertEqual(r['cancel_price'],'0.0804263')
    def test_missing_or_invalid_threshold_blocks(self):
        for value in (None,'',0,'0','-1','NaN','Infinity','100',1.5):
            with self.assertRaises(policy.RuleError):rule(threshold=value)
    def test_decimal_context_and_no_float_rounding(self):
        with localcontext() as ctx:
            ctx.prec=4
            self.assertEqual(rule(entry='99.8')['cancel_price'],'100.5485')
    def test_rule_is_immutable_and_validated(self):
        original=rule();changed=deepcopy(original);changed['cancel_price']='100'
        with self.assertRaises(policy.RuleError):policy.validate_rule(changed)
        self.assertEqual(original,policy.validate_rule(original))
    def test_time_cancellation_not_added(self):
        self.assertFalse(rule()['time_cancel_enabled'])
        self.assertNotIn('expires_at',rule())
    def test_filled_entry_never_cancelled_even_if_flat_now(self):
        for pos in ('10','0'):
            out=policy.evaluate(rule(),**observation(status='filled',remaining='0',position=pos,mark='150'))
            self.assertEqual(out['decision'],'ENTRY_ALREADY_FILLED_NO_CANCEL')
            self.assertFalse(out['cancel_candidate'])
    def test_any_partial_fill_excluded(self):
        for remaining,pos in [('9.99','0.01'),('9.99','0'),('10','0.01')]:
            out=policy.evaluate(rule(),**observation(remaining=remaining,position=pos))
            self.assertEqual(out['decision'],'FILL_OBSERVED_DO_NOT_CANCEL')
    def test_threshold_latch_cannot_override_fill(self):
        out=policy.evaluate(rule(),**observation(remaining='5',position='5'),threshold_seen=True)
        self.assertFalse(out['cancel_candidate'])
    def test_observed_crossing_not_erased_by_return(self):
        out=policy.evaluate(rule(),**observation(mark='100.2'),threshold_seen=True)
        self.assertTrue(out['cancel_candidate']);self.assertFalse(out['threshold_crossed'])
    def test_status_closed_or_unknown_never_cancelled(self):
        for status in ('canceled','rejected','triggered','waiting'):
            self.assertFalse(policy.evaluate(rule(),**observation(status=status))['cancel_candidate'])
    def test_order_identity_side_price_or_size_mismatch_blocks(self):
        for field,value in [('cloid','0x'+'d'*32),('coin','OTHER'),('side','A'),
                             ('limitPx','101'),('origSz','11'),('reduceOnly',True),
                             ('isTrigger',True),('orderType','Market')]:
            sample=observation();sample['order']['order']['order'][field]=value
            self.assertFalse(policy.evaluate(rule(),**sample)['cancel_candidate'])
    def test_unknown_order_does_not_mean_unfilled(self):
        sample=observation();sample['order']={'status':'unknownOid'}
        self.assertFalse(policy.evaluate(rule(),**sample)['cancel_candidate'])
    def test_invalid_remaining_and_positions_block(self):
        for remaining,position in [('-1','0'),('11','0'),('NaN','0'),('10','NaN')]:
            self.assertFalse(policy.evaluate(rule(),**observation(remaining=remaining,position=position))['cancel_candidate'])
    def test_invalid_mark_never_treated_as_zero(self):
        for mark in (None,'0','NaN','Infinity','bad',100.75):
            self.assertFalse(policy.evaluate(rule(),**observation(mark=mark))['cancel_candidate'])
    def test_stale_negative_or_nan_age_blocks(self):
        for age in (-1,10.1,float('nan'),float('inf'),True,None):
            self.assertFalse(policy.evaluate(rule(),**observation(age=age))['cancel_candidate'])
    def test_cancellation_packet_is_entry_only(self):
        self.assertEqual(executor.cancel_action(record()),
            {'type':'cancelByCloid','cancels':[{'asset':0,'cloid':ENTRY}]})
    def test_repair_never_opens_position_or_changes_exit_prices(self):
        r=record();action=executor.repair_action(r,'3')
        self.assertEqual(action['grouping'],'positionTpsl')
        self.assertEqual(len(action['orders']),2)
        for before,after in zip(r['action']['orders'][1:],action['orders']):
            self.assertTrue(after['r']);self.assertEqual(after['s'],'3')
            self.assertEqual(before['p'],after['p']);self.assertEqual(before['t'],after['t'])
            self.assertNotEqual(before['c'],after['c'])
    def test_repair_rejects_unknown_extra_or_opposite_position(self):
        for size in ('0','-1','10.1','NaN'):
            with self.assertRaises((policy.RuleError,executor.CancelError)):executor.repair_action(record(),size)
    def test_repair_rejects_unapproved_exit_types(self):
        rec=record();rec['action']['orders'][1]['t']['trigger']['isMarket']=True
        with self.assertRaises(executor.CancelError):executor.repair_action(rec,'3')
    def test_disabled_dispatch_does_not_read_environment_or_key(self):
        for enabled in (False,None,'true',1):
            self.assertEqual(executor.cancel_registered_once('wrong',enable_testnet=enabled)['status'],'DISABLED')
    def test_review_wrong_service_or_mode_disabled(self):
        for env in ({},{'RENDER_SERVICE_ID':policy.SERVICE,'HL_TESTNET_RUNTIME_MODE':'live'}):
            self.assertEqual(policy.review_configured(env)['status'],'DISABLED')


class MemoryOps:
    def __init__(self):self.rows={};self.reservations=[];self.fail_commit=False
    def get(self,key,phase):return deepcopy(self.rows.get(phase))
    def reserve(self,key,phase,action):
        if phase in self.rows:return False,self.rows[phase]['nonce']
        nonce=time.time_ns()//1_000_000
        self.rows[phase]=dict(action=deepcopy(action),nonce=nonce)
        self.reservations.append(phase)
        if self.fail_commit:raise executor.CancelError('LOST_COMMIT_ACK')
        return True,nonce
    def save(self,key,phase,status):self.rows[phase]['status']=status


class FakeExchange:
    def __init__(self,ops):
        self.ops=ops;self.samples=[];self.default=observation();self.calls=[]
        self.cancel_requests_attempted=0;self.requests_attempted=0
        self.race=None;self.timeout=False;self.repaired=False;self.verify_ok=True
        self.children=['canceled','canceled'];self.late_accept=False
    def observe(self):
        if self.samples:return deepcopy(self.samples.pop(0))
        if self.late_accept and self.calls:
            self.default=observation(status='canceled');self.late_accept=False
        return deepcopy(self.default)
    def child_states(self):return self.children
    def verify_original(self):return {'verified':self.verify_ok}
    def write(self,action,nonce,*,phase,position=None):
        assert phase in self.ops.rows, 'Commit must precede signing/request'
        assert self.ops.rows[phase]['action']==action
        self.calls.append((phase,deepcopy(action)));self.requests_attempted+=1
        if phase=='cancel':
            self.cancel_requests_attempted+=1
            if self.race=='partial':self.default=observation(status='canceled',remaining='7',position='3')
            elif self.race=='full':self.default=observation(status='filled',remaining='0',position='10')
            elif not self.late_accept:self.default=observation(status='canceled')
        else:self.repaired=True
        if self.timeout:raise executor.CancelError('UNCERTAIN')
    def verify_repair(self,action,position):return self.repaired and self.verify_ok


class CancelFlowTests(unittest.TestCase):
    def setUp(self):
        self.ops=MemoryOps();self.ex=FakeExchange(self.ops)
        self.patcher=patch.object(executor.time,'sleep');self.patcher.start();self.addCleanup(self.patcher.stop)
    def run_case(self):return executor.run_once('key',record(),rule(),self.ops,self.ex)
    def test_threshold_cross_then_cancel_only_entry_and_verify(self):
        result=self.run_case()
        self.assertEqual(result['status'],'CANCELLATION_VERIFIED_FLAT')
        self.assertEqual(len(self.ex.calls),1);self.assertEqual(self.ex.calls[0][0],'cancel')
    def test_below_half_no_signature_or_reservation(self):
        self.ex.default=observation(mark='100.74')
        self.assertEqual(self.run_case()['status'],'WAITING_BELOW_CANCEL_THRESHOLD')
        self.assertFalse(self.ex.calls);self.assertFalse(self.ops.reservations)
    def test_preexisting_partial_does_not_cancel(self):
        self.ex.default=observation(remaining='7',position='3')
        self.assertEqual(self.run_case()['status'],'FILL_OBSERVED_DO_NOT_CANCEL')
        self.assertFalse(self.ex.calls)
    def test_fill_between_first_and_second_read_stops(self):
        self.ex.samples=[observation(),observation(status='filled',remaining='0',position='10')]
        self.assertEqual(self.run_case()['status'],'ENTRY_ALREADY_FILLED_NO_CANCEL');self.assertFalse(self.ex.calls)
    def test_partial_between_reads_does_not_cancel(self):
        self.ex.samples=[observation(),observation(remaining='9',position='1')]
        self.assertEqual(self.run_case()['status'],'FILL_OBSERVED_DO_NOT_CANCEL');self.assertFalse(self.ex.calls)
    def test_return_after_observed_crossing_remains_candidate(self):
        self.ex.samples=[observation(),observation(mark='100.1')]
        self.assertEqual(self.run_case()['status'],'CANCELLATION_VERIFIED_FLAT')
    def test_cancel_commit_failure_prevents_write(self):
        self.ops.fail_commit=True
        with self.assertRaises(executor.CancelError):self.run_case()
        self.assertFalse(self.ex.calls)
    def test_lost_commit_ack_then_repeat_never_resends(self):
        self.ops.fail_commit=True
        with self.assertRaises(executor.CancelError):self.run_case()
        self.ops.fail_commit=False;result=self.run_case()
        self.assertTrue(result['replayed']);self.assertFalse(self.ex.calls)
    def test_timeout_but_accepted_readback_not_resend(self):
        self.ex.timeout=True
        self.assertEqual(self.run_case()['status'],'CANCELLATION_VERIFIED_FLAT')
        self.run_case();self.assertEqual(len(self.ex.calls),1)
    def test_unchanged_retry_never_duplicates_cancel(self):
        self.run_case();self.assertTrue(self.run_case()['replayed']);self.assertEqual(len(self.ex.calls),1)
    def test_full_fill_race_keeps_original_protection(self):
        self.ex.race='full'
        self.assertEqual(self.run_case()['status'],'FILLED_BEFORE_CANCEL_PROTECTION_VERIFIED')
        self.assertEqual(len(self.ex.calls),1)
    def test_partial_race_recreates_only_original_exits_after_commit(self):
        self.ex.race='partial'
        self.assertEqual(self.run_case()['status'],'RACED_FILL_PROTECTION_RESTORED')
        self.assertEqual([x[0] for x in self.ex.calls],['cancel','repair'])
        self.assertTrue(all(o['s']=='3' and o['r'] for o in self.ex.calls[1][1]['orders']))
    def test_repair_timeout_readback_and_retry_do_not_duplicate(self):
        self.ex.race='partial';self.ex.timeout=True
        self.assertEqual(self.run_case()['status'],'RACED_FILL_PROTECTION_RESTORED')
        self.run_case();self.assertEqual(len(self.ex.calls),2)
    def test_failed_repair_never_claims_protected(self):
        self.ex.race='partial';self.ex.verify_ok=False
        result=self.run_case()
        self.assertEqual(result['status'],'PROTECTION_REPAIR_UNVERIFIED_DO_NOT_RESEND')
        self.assertEqual(len(self.ex.calls),2)
    def test_unknown_children_do_not_claim_success(self):
        self.ex.children=['open','unknown']
        self.assertEqual(self.run_case()['status'],'FLAT_BUT_CHILD_STATUS_REQUIRES_REVIEW')
    def test_wrong_position_does_not_get_new_orders(self):
        self.ops.reserve('key','cancel',executor.cancel_action(record()))
        self.ex.default=observation(status='canceled',remaining='7',position='4')
        self.assertEqual(self.run_case()['status'],'POSITION_OR_PROTECTION_REQUIRES_REVIEW')
        self.assertFalse(self.ex.calls)
    def test_late_accept_after_timeout_is_read_not_resent(self):
        self.ex.timeout=True;self.ex.late_accept=True
        self.assertEqual(self.run_case()['status'],'CANCELLATION_VERIFIED_FLAT')
        self.assertEqual(len(self.ex.calls),1)


@unittest.skipUnless(os.environ.get('HL_JOURNAL_CI_URL'),'Needs disposable localhost PostgreSQL')
class PostgresCancelTests(unittest.TestCase):
    def setUp(self):
        from .test_postgres_journal import PostgresTests
        PostgresTests.setUp(self)
        from . import postgres_journal as pg
        import hyperliquid_testnet_executor as sender
        self.pg=pg
        self.key,_=self.journal.save_prepared(A,self.prepared)
        self.action=sender.build_action(self.prepared['execution'],self.meta,A,exit_type='tp_limit_sl_market')
        self.journal.reserve(self.key,self.action)
        self.rec=self.journal.load(self.key)
        self.spec={'event_id':self.source['event_id'],'rule_id':'DEMO_FORMULA','threshold_pct':'1.5',
                   'source_digest':policy.digest(self.source)}
        self.r=policy.rule_from_record(self.rec,self.spec)
        policy.register_rule(self.journal,self.key,self.r)
        self.ops=executor.Operations(self.journal);self.ops.initialize()
    def test_rule_survives_new_connection_and_cannot_change_threshold(self):
        other=self.pg.PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL'])
        self.assertFalse(policy.register_rule(other,self.key,self.r))
        altered=policy.rule_from_record(self.rec,{**self.spec,'threshold_pct':'2'})
        with self.assertRaises(policy.RuleError):policy.register_rule(other,self.key,altered)
    def test_six_concurrent_cancellers_receive_one_reservation(self):
        from concurrent.futures import ThreadPoolExecutor
        action=executor.cancel_action(self.rec)
        def reserve(_):
            j=self.pg.PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL'])
            return executor.Operations(j).reserve(self.key,'cancel',action)[0]
        with ThreadPoolExecutor(max_workers=6) as pool:answers=list(pool.map(reserve,range(6)))
        self.assertEqual(sum(answers),1)
    def test_uncertain_cancellation_survives_restart(self):
        action=executor.cancel_action(self.rec)
        self.ops.reserve(self.key,'cancel',action)
        self.ops.save(self.key,'cancel','CANCEL_TRANSPORT_UNCERTAIN')
        other=executor.Operations(self.pg.PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL']))
        self.assertFalse(other.reserve(self.key,'cancel',action)[0])
        self.assertEqual(other.get(self.key,'cancel')['outcome'],'CANCEL_TRANSPORT_UNCERTAIN')
    def test_recorded_crossing_stays_after_restart(self):
        policy.remember_crossing(self.journal,self.key,self.r,{'threshold_crossed':True,'cancel_candidate':True})
        self.assertTrue(policy.register_rule(self.journal,self.key,self.r))
    def test_original_attempt_receipt_is_not_overwritten(self):
        before=self.journal.load(self.key)
        self.ops.reserve(self.key,'cancel',executor.cancel_action(self.rec))
        self.ops.save(self.key,'cancel','CANCELLATION_VERIFIED_FLAT')
        self.assertEqual(before,self.journal.load(self.key))
    def test_changed_rule_or_source_rejected(self):
        with self.assertRaises(policy.RuleError):policy.rule_from_record(self.rec,{**self.spec,'source_digest':'f'*64})
    def test_repair_operation_separate_and_not_replayed(self):
        act=executor.repair_action(self.rec,'1')
        self.assertTrue(self.ops.reserve(self.key,'repair',act)[0])
        self.assertFalse(self.ops.reserve(self.key,'repair',act)[0])
    def test_foreign_repair_payload_cannot_replace_reserved_action(self):
        act=executor.repair_action(self.rec,'1');self.ops.reserve(self.key,'repair',act)
        changed=deepcopy(act);changed['orders'][0]['p']='1'
        with self.assertRaises(self.pg.JournalError):self.ops.reserve(self.key,'repair',changed)


class TransportTests(unittest.TestCase):
    def setUp(self):
        from types import SimpleNamespace
        self.agent='0x'+'2'*40
        self.sign=Mock(return_value={'r':'0x1','s':'0x2','v':27})
        self.reader=SimpleNamespace(info=Mock(return_value={'role':'agent','data':{'user':A}}))
        self.fake=SimpleNamespace(TestnetHTTP=Mock(return_value=self.reader), TESTNET_HOST=executor.HOST,
            _wallet=Mock(return_value=SimpleNamespace(address=self.agent)),_account=lambda x:x.lower(),
            now_ms=lambda:1000,_json=json.dumps,_decode=json.loads,MAX_BYTES=2048)
        import sys
        self.modules=patch.dict(sys.modules,{'hyperliquid_testnet_executor':self.fake,
                    'hyperliquid.utils.signing':SimpleNamespace(sign_l1_action=self.sign)})
        self.modules.start();self.addCleanup(self.modules.stop)
        self.http=executor.Exchange(record(),self.agent)
        self.reply=Mock(status=200);self.reply.read.return_value=b'{"status":"ok"}'
        self.conn=Mock();self.conn.getresponse.return_value=self.reply
        self.net=patch.object(executor.http.client,'HTTPSConnection',return_value=self.conn)
        self.ctor=self.net.start();self.addCleanup(self.net.stop)
    def test_cancel_only_entry_fixed_testnet_and_signature_domain(self):
        self.http.write(executor.cancel_action(record()),1000,phase='cancel')
        self.ctor.assert_called_once_with('api.hyperliquid-testnet.xyz',timeout=4)
        args=self.conn.request.call_args.args
        self.assertEqual(args[:2],('POST','/exchange'))
        body=json.loads(args[2]);self.assertEqual(body['action'],executor.cancel_action(record()))
        self.assertEqual(body['expiresAfter'],6000)
        self.assertIs(self.sign.call_args.args[-1],False)
    def test_mainnet_is_refused_before_key(self):
        with patch.object(executor,'HOST','api.hyperliquid.xyz'):
            with self.assertRaises(executor.CancelError):self.http.write(executor.cancel_action(record()),1000,phase='cancel')
        self.fake._wallet.assert_not_called();self.ctor.assert_not_called()
    def test_arbitrary_and_cancel_all_actions_blocked(self):
        for action in ({'type':'scheduleCancel'},{'type':'cancelByCloid','cancels':[]},
                       {'type':'withdraw3'},{'type':'cancelByCloid','cancels':[{'asset':0,'cloid':'0x'+'f'*32}]}):
            with self.assertRaises(executor.CancelError):self.http.write(action,1000,phase='cancel')
        self.ctor.assert_not_called();self.fake._wallet.assert_not_called()
    def test_expired_nonce_cannot_sign(self):
        with self.assertRaises(executor.CancelError):self.http.write(executor.cancel_action(record()),-10000,phase='cancel')
        self.sign.assert_not_called();self.ctor.assert_not_called()
    def test_wrong_agent_cannot_sign(self):
        self.fake._wallet.return_value.address='0x'+'3'*40
        with self.assertRaises(executor.CancelError):self.http.write(executor.cancel_action(record()),1000,phase='cancel')
        self.sign.assert_not_called();self.ctor.assert_not_called()
    def test_wrong_role_cannot_sign(self):
        self.reader.info.return_value={'role':'user'}
        with self.assertRaises(executor.CancelError):self.http.write(executor.cancel_action(record()),1000,phase='cancel')
        self.sign.assert_not_called()
    def test_redirect_not_followed_or_retried(self):
        self.reply.status=302
        with self.assertRaises(executor.CancelError):self.http.write(executor.cancel_action(record()),1000,phase='cancel')
        self.conn.request.assert_called_once()
    def test_network_timeout_no_replay_no_error_echo(self):
        self.conn.getresponse.side_effect=TimeoutError('DO_NOT_ECHO')
        with self.assertRaises(executor.CancelError) as err:self.http.write(executor.cancel_action(record()),1000,phase='cancel')
        self.assertNotIn('DO_NOT_ECHO',str(err.exception));self.conn.request.assert_called_once()
    def test_repair_only_two_reduce_only_exits(self):
        act=executor.repair_action(record(),'3')
        self.http.write(act,1000,phase='repair',position='3')
        sent=json.loads(self.conn.request.call_args.args[2])['action']
        self.assertEqual(len(sent['orders']),2);self.assertTrue(all(o['r'] for o in sent['orders']))
        self.assertEqual(self.http.cancel_requests_attempted,0)
    def test_altered_repair_rejected_before_key(self):
        act=executor.repair_action(record(),'3');act['orders'][0]['r']=False
        with self.assertRaises(executor.CancelError):self.http.write(act,1000,phase='repair',position='3')
        self.fake._wallet.assert_not_called()


if __name__=='__main__':unittest.main(verbosity=2)
