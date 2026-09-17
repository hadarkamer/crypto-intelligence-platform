"""Data-forwarding completion tests: no user credentials or network required."""
import asyncio
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import inspect
import io
import json
import unittest
from unittest.mock import Mock, patch

import alert_cards_forwarder as f
import alert_cards_wire as w
from alert_cards_forwarder_selftest import delivery, KEY, SCOPE


class CompletionTests(unittest.TestCase):
    def setUp(self):
        self.worker=f.Forwarder()
        self.fence=datetime(2026,9,17,tzinfo=timezone.utc)
        p=patch('http.client.HTTPSConnection',side_effect=AssertionError('No external connection'))
        p.start();self.addCleanup(p.stop)

    def run_pass(self, values, post):
        return self.worker.pass_once((SCOPE,),KEY,self.fence,reader=lambda *_:values,post=post)

    def test_cooldown_is_pending_not_false_success(self):
        post=Mock(side_effect=TimeoutError('PRIVATE_DETAIL'))
        first=self.run_pass([delivery()],post)
        second=self.run_pass([delivery()],post)
        self.assertEqual(first['deferred'],1)
        self.assertEqual(second['attempted'],0)
        self.assertEqual(second['deferred'],1)
        self.assertEqual(second['status'],'FORWARD_PENDING_RETRY')
        self.assertIsNone(self.worker.last_ok)
        self.assertEqual(second['pending_records'],1)
        self.assertNotIn('PRIVATE_DETAIL',str(second))
        post.assert_called_once()

    def test_pending_source_survives_source_retention_expiry(self):
        item=delivery()
        self.run_pass([item],Mock(side_effect=TimeoutError()))
        self.worker.retry_after.clear()
        post=Mock(return_value='RECORDED')
        result=self.run_pass([],post)
        self.assertEqual(result['recorded'],1)
        self.assertEqual(result['pending_records'],0)
        self.assertEqual(post.call_args.args[0],item)

    def test_pending_can_retry_while_source_is_down_without_hiding_failure(self):
        self.run_pass([delivery()],Mock(side_effect=TimeoutError()))
        self.worker.retry_after.clear()
        result=self.worker.pass_once((SCOPE,),KEY,self.fence,
            reader=Mock(side_effect=RuntimeError('DO_NOT_LOG')),
            post=Mock(return_value='DUPLICATE'))
        self.assertEqual(result['duplicates'],1)
        self.assertEqual(result['status'],'FORWARD_SOURCE_UNAVAILABLE')
        self.assertNotIn('DO_NOT_LOG',str(result))

    def test_stored_payload_is_immutable_copy(self):
        value=delivery(); original=deepcopy(value)
        self.run_pass([value],Mock(side_effect=TimeoutError()))
        value['text']='changed after read'
        self.worker.retry_after.clear()
        post=Mock(return_value='RECORDED')
        self.run_pass([],post)
        self.assertEqual(post.call_args.args[0],original)

    def test_receiver_mutation_cannot_change_retry_payload(self):
        original=delivery()
        def corrupt(v,key):
            v['text']='corrupted by faulty callback'
            raise TimeoutError()
        self.run_pass([original],corrupt)
        self.worker.retry_after.clear()
        post=Mock(return_value='RECORDED')
        self.run_pass([],post)
        self.assertEqual(post.call_args.args[0],original)

    def test_eighty_legitimate_alerts_all_transferred_in_bounded_passes(self):
        values=[delivery(identity=f'alert-{n}') for n in range(80)]
        post=Mock(return_value='RECORDED');total=0
        for _ in range(5):
            result=self.run_pass(values,post)
            self.assertLessEqual(result['attempted'],16)
            total+=result['recorded']
        self.assertEqual(total,80)
        self.assertEqual(post.call_count,80)
        self.assertEqual(result['pending_records'],0)
        self.assertEqual(self.run_pass(values,post)['attempted'],0)

    def test_one_failed_item_does_not_block_other_alerts(self):
        def post(v,key):
            if v['intent_id']=='bad':raise TimeoutError()
            return 'RECORDED'
        result=self.run_pass([delivery(identity='bad'),delivery(identity='good')],post)
        self.assertEqual((result['recorded'],result['deferred']),(1,1))

    def test_rejected_data_is_reported_and_not_retried_forever(self):
        post=Mock(return_value='REJECTED')
        result=self.run_pass([delivery()],post)
        self.assertEqual(result['status'],'FORWARD_RECORD_REJECTED_REVIEW')
        self.assertEqual(result['rejected'],1)
        self.assertEqual(self.run_pass([delivery()],post)['attempted'],0)

    def test_unknown_reply_does_not_mark_complete(self):
        result=self.run_pass([delivery()],lambda *_:'ok')
        self.assertEqual(result['status'],'FORWARD_PENDING_RETRY')
        self.assertEqual(len(self.worker.done),0)

    def test_gap_detected_even_when_no_initial_success(self):
        self.worker.started-=3601
        result=self.worker.pass_once((SCOPE,),KEY,self.fence,
            reader=Mock(side_effect=TimeoutError()),post=Mock())
        self.assertTrue(result['coverage_gap_observed'])
        self.assertFalse(result['prior_process_coverage_verified'])

    def test_buffer_bound_is_reported_not_silently_dropped(self):
        with patch.object(f,'MAX_BUFFER',2):
            result=self.run_pass([delivery(identity=str(n)) for n in range(3)],Mock())
        self.assertEqual(result['status'],'FORWARD_SOURCE_UNAVAILABLE')
        self.assertEqual(result['attempted'],0)

    def test_corrupt_source_response_is_unavailable(self):
        self.assertEqual(self.run_pass(None,Mock())['status'],'FORWARD_SOURCE_UNAVAILABLE')

    def test_actual_network_reply_must_be_object_and_exact_receipt(self):
        for payload in ([],{'receipt_id':'0'*64,'status':'RECORDED','record_only':True},
                        {'receipt_id':hashlib.sha256(w.encoded(delivery())).hexdigest(),'status':'RECORDED','record_only':False}):
            conn=Mock();reply=Mock(status=200);reply.read.return_value=json.dumps(payload).encode()
            conn.getresponse.return_value=reply
            with patch.object(f.http.client,'HTTPSConnection',return_value=conn):
                with self.assertRaises(w.WireError):f.post_record(delivery(),KEY)
            conn.request.assert_called_once()

    def test_destination_cannot_be_replaced(self):
        with patch.object(w,'HOST','elsewhere.example'):
            with self.assertRaises(w.WireError):f.post_record(delivery(),KEY)

    def test_source_dsn_is_only_existing_production_database(self):
        for dsn in ('postgresql://x:y@elsewhere/x','postgresql://x:y@dpg-d94d641kh4rs73evvih0-a/other',''):
            with patch.dict(f.os.environ,{'DATABASE_URL':dsn}):
                with self.assertRaises(w.WireError):f._source_dsn()

    def test_source_queries_are_readonly_without_cross_system_credentials(self):
        for function in (f.read_delivered,f.audit_existing_delivery):
            source=inspect.getsource(function)
            self.assertIn('default_transaction_read_only=on',source)
            for word in ('INSERT','DELETE','UPDATE','CREATE','HL_TESTNET_AGENT_KEY','getUpdates'):
                self.assertNotIn(word,source)

    def test_audit_replays_exact_delivered_record_twice(self):
        v=delivery(family='dual_cvd65',identity='a'*64)
        row=dict(intent_id=v['intent_id'],rule_id=v['rule_id'],symbol=v['symbol'],direction=v['side'],
            source_at_utc=v['source_at'],finished_at_utc=v['delivered_at'],expires_at=v['expires_at'],
            text=v['text'],payload={'observation':{'price_reference':v['reference']}},
            subscription_scope='general-watch:'+SCOPE)
        conn=Mock();conn.execute.return_value.fetchall.return_value=[row]
        context=Mock();context.__enter__=Mock(return_value=conn);context.__exit__=Mock(return_value=False)
        post=Mock(side_effect=['RECORDED','DUPLICATE'])
        with patch('psycopg.connect',return_value=context),patch.object(f,'_source_dsn',return_value='unused'):
            result=f.audit_existing_delivery((SCOPE,),KEY,'a'*64,post=post)
        self.assertEqual(result['status'],'HISTORICAL_RECORD_AND_REPLAY_VERIFIED')
        self.assertTrue(result['historical_validation_only'])
        self.assertEqual(result['source_at'],v['source_at'])
        self.assertEqual(post.call_args_list[0].args[0],v)
        self.assertEqual(post.call_args_list[1].args[0],v)
        sql,params=conn.execute.call_args.args
        self.assertIn("status='DELIVERED'",sql)
        self.assertIn('subscription_scope=ANY',sql)
        self.assertEqual(params,('a'*64,['general-watch:'+SCOPE]))

    def test_audit_missing_record_never_manufactures_one(self):
        conn=Mock();conn.execute.return_value.fetchall.return_value=[]
        context=Mock();context.__enter__=Mock(return_value=conn);context.__exit__=Mock(return_value=False)
        post=Mock()
        with patch('psycopg.connect',return_value=context),patch.object(f,'_source_dsn',return_value='unused'):
            with self.assertRaises(w.WireError):f.audit_existing_delivery((SCOPE,),KEY,'a'*64,post=post)
        post.assert_not_called()


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_many_initializations_create_one_nonblocking_task(self):
        env={'ALERT_CARDS_FORWARD_MODE':f.MODE,'ALERT_CARDS_FORWARD_SECRET':KEY,
             'ALERT_CARDS_FORWARD_NOT_BEFORE':'2026-09-17T00:00:00Z','RENDER_SERVICE_ID':f.SERVICE}
        runs=[]
        async def wait_forever(*args):
            runs.append(args)
            await asyncio.Event().wait()
        with patch.dict(f.os.environ,env,clear=True),patch.object(f,'_TASK',None),patch.object(f,'_SCOPES',set()),patch.object(f,'_loop',wait_forever):
            for _ in range(10):f.maybe_start(123)
            f.maybe_start(456)
            self.assertEqual(len(f._SCOPES),2)
            await asyncio.sleep(0)
            self.assertEqual(len(runs),1)
            task=f._TASK;task.cancel()
            with self.assertRaises(asyncio.CancelledError):await task

    async def test_invalid_config_does_not_propagate_to_alert_delivery(self):
        with patch.dict(f.os.environ,{'ALERT_CARDS_FORWARD_MODE':f.MODE},clear=True),patch.object(f,'_TASK',None),patch('sys.stdout',new_callable=io.StringIO) as output:
            f.maybe_start(123)
            self.assertIsNone(f._TASK)
            self.assertIn('FORWARD_INITIALIZATION_UNAVAILABLE',output.getvalue())


if __name__=='__main__':unittest.main(verbosity=2)
