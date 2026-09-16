"""Monitor tests run locally with no database, exchange, or credentials."""
from copy import deepcopy
import json
import threading
import unittest
from unittest.mock import Mock, patch
from . import pending_cancel_monitor as m

A, B = '0x'+'1'*40, '0x'+'2'*40


def env():
    return {'RENDER_SERVICE_ID':m.SERVICE,'HL_TESTNET_RUNTIME_MODE':m.MODE,
        'HL_TESTNET_JOURNAL_BACKEND':'staging_postgres_v1',
        'HL_TESTNET_PRICE_ROUNDING':'nearest-half-up-perp-v1',
        'HL_TESTNET_EXIT_TYPE':'tp_limit_sl_market',
        'HL_TESTNET_CANCEL_MONITOR_POLICY':m.POLICY,
        'HL_TESTNET_ACCOUNT_ADDRESS':A,'HL_TESTNET_AGENT_ADDRESS':B}


class MemoryStore:
    def __init__(self):
        self.journal=object();self.keys=['one'];self.states={};self.reads=0
    def active_keys(self):
        self.reads+=1
        return [k for k in self.keys if self.states.get(k) not in m.TERMINAL]
    def save(self,key,status):self.states[key]=status


class FakeStop:
    def __init__(self,passes):self.passes=passes;self.waits=[]
    def is_set(self):return len(self.waits)>=self.passes
    def wait(self,seconds):self.waits.append(seconds)


class ConfigurationTests(unittest.TestCase):
    def test_exact_configuration(self):self.assertEqual(m.Config.from_env(env()),m.Config(A,B))
    def test_configuration_missing_any_gate_is_refused(self):
        for key in env():
            value=env();del value[key]
            with self.assertRaises(m.MonitorError):m.Config.from_env(value)
    def test_read_only_cannot_cancel(self):
        value=env();value['HL_TESTNET_RUNTIME_MODE']='read_only'
        with self.assertRaises(m.MonitorError):m.Config.from_env(value)
    def test_wrong_service_and_exit_policy(self):
        for key,value in [('RENDER_SERVICE_ID','production'),('HL_TESTNET_EXIT_TYPE','market'),
                          ('HL_TESTNET_CANCEL_MONITOR_POLICY','anything')]:
            cfg=env();cfg[key]=value
            with self.assertRaises(m.MonitorError):m.Config.from_env(cfg)
    def test_addresses_are_not_keys(self):
        for value in ('0x'+'1'*64,'0x'+'0'*40,None,''):
            cfg=env();cfg['HL_TESTNET_AGENT_ADDRESS']=value
            with self.assertRaises(m.MonitorError):m.Config.from_env(cfg)
    def test_master_key_address_is_refused(self):
        cfg=env();cfg['HL_TESTNET_AGENT_ADDRESS']=A
        with self.assertRaises(m.MonitorError):m.Config.from_env(cfg)


class LoopTests(unittest.TestCase):
    def setUp(self):
        self.store=MemoryStore()
        self.send=Mock(return_value={'status':'WAITING_BELOW_CANCEL_THRESHOLD','cancel_requests_sent':0,'exchange_write_attempts':0})
        self.read=Mock(return_value={'open_entry_orders':1,'open_exit_orders':0})
        self.monitor=m.Monitor(m.Config(A,B),self.store,self.send,self.read)
        self.stop=threading.Event()
    def test_only_registered_key_is_passed_to_existing_dispatcher(self):
        self.monitor.tick(self.stop)
        self.send.assert_called_once_with('one',journal=self.store.journal,agent=B,enable_testnet=True)
    def test_repeated_observations_not_one_boot_sample(self):
        self.monitor.run(FakeStop(3),Mock())
        self.assertEqual(self.send.call_count,3);self.assertEqual(self.monitor.cycles,3)
    def test_delay_follows_each_completed_pass(self):
        stop=FakeStop(3);self.monitor.run(stop,Mock())
        self.assertEqual(stop.waits,[5,5,5])
    def test_full_fill_is_excluded_on_next_pass(self):
        self.send.return_value={'status':'ENTRY_ALREADY_FILLED_NO_CANCEL'}
        self.monitor.tick(self.stop);self.monitor.tick(self.stop)
        self.send.assert_called_once()
    def test_partial_fill_is_excluded_and_not_cancelled(self):
        self.send.return_value={'status':'FILL_OBSERVED_DO_NOT_CANCEL'}
        self.monitor.tick(self.stop);self.monitor.tick(self.stop)
        self.assertEqual(self.monitor.cancels,0);self.send.assert_called_once()
    def test_verified_cancel_stops_handling_that_entry(self):
        self.send.return_value={'status':'CANCELLATION_VERIFIED_FLAT','cancel_requests_sent':1,'exchange_write_attempts':1}
        first=self.monitor.tick(self.stop);self.monitor.tick(self.stop)
        self.send.assert_called_once();self.assertEqual(first['cancel_requests_sent_total'],1)
    def test_new_registered_key_discovered_without_restart(self):
        self.monitor.tick(self.stop);self.store.keys.append('two');self.monitor.tick(self.stop)
        self.assertEqual([c.args[0] for c in self.send.call_args_list],['one','one','two'])
    def test_unknown_unregistered_order_is_not_selected(self):
        self.store.keys=[];self.read.return_value={'open_entry_orders':1,'open_exit_orders':2}
        report=self.monitor.tick(self.stop)
        self.send.assert_not_called();self.assertEqual(report['active_registered_rules'],0)
    def test_stop_before_pass_does_no_reads(self):
        self.stop.set();self.monitor.tick(self.stop)
        self.read.assert_not_called();self.send.assert_not_called()
    def test_stop_before_dispatch_does_not_call_sender(self):
        def inventory(a):self.stop.set();return {}
        self.monitor.read_inventory=inventory;self.monitor.tick(self.stop)
        self.send.assert_not_called()
    def test_database_failure_prevents_dispatch_and_backs_off(self):
        self.store.active_keys=Mock(side_effect=RuntimeError('SECRET'))
        stop=FakeStop(4);emit=Mock();self.monitor.run(stop,emit)
        self.send.assert_not_called();self.assertEqual(stop.waits,[10,20,40,60])
        self.assertNotIn('SECRET',str(emit.call_args_list))
    def test_inventory_failure_prevents_dispatch(self):
        self.read.side_effect=RuntimeError('SECRET');self.monitor.run(FakeStop(1),Mock())
        self.send.assert_not_called()
    def test_invalid_result_is_not_persisted(self):
        self.send.return_value={'status':'SECRET: bad'}
        emit=Mock();self.monitor.run(FakeStop(1),emit)
        self.assertEqual(self.store.states,{});self.assertNotIn('SECRET',str(emit.call_args_list))
    def test_counters_are_strict_integers(self):
        for value in (True,-1,'1',3):
            self.send.return_value={'status':'WAITING_BELOW_CANCEL_THRESHOLD','cancel_requests_sent':value,'exchange_write_attempts':value}
            with self.assertRaises(m.MonitorError):self.monitor.tick(self.stop)
    def test_no_prices_account_keys_or_raw_reply_in_report(self):
        self.send.return_value.update(private_key='SECRET',response='RAW')
        body=json.dumps(self.monitor.tick(self.stop))
        for value in (A,B,'SECRET','RAW','triggerPx','entry_price'):
            self.assertNotIn(value,body)
    def test_database_save_failure_does_not_clear_existing_reservation(self):
        self.send.side_effect=[{'status':'CANCELLATION_VERIFIED_FLAT','cancel_requests_sent':1,'exchange_write_attempts':1},
                               {'status':'CANCELLATION_VERIFIED_FLAT','cancel_requests_sent':0,'exchange_write_attempts':0}]
        save=self.store.save;attempts=[0]
        def save_once(k,s):
            attempts[0]+=1
            if attempts[0]==1:raise RuntimeError('DB unavailable')
            save(k,s)
        self.store.save=save_once
        self.monitor.run(FakeStop(2),Mock())
        self.assertEqual(self.monitor.cancels,1)
    def test_steady_passes_do_not_spam_logs(self):
        emit=Mock()
        with patch.object(m.time,'monotonic',return_value=100):self.monitor.run(FakeStop(3),emit)
        emit.assert_called_once()


if __name__=='__main__':unittest.main()
