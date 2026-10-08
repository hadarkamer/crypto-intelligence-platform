"""Complete candidate worker against actual SQLite and a deterministic venue.

Only the exchange is simulated; requests, transactions, nonce fences, source
contracts, account binding, rounding, allocation and reconciliation are real.
Network and signer constructors are blocked for the entire test case.
"""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from decimal import Decimal
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

import experimental_execution_contract as contract
from experimental_execution_fixtures import r2732_message, hype_row71205_message, sol_g65_message
from . import experimental_execution_runtime as runtime
from .experimental_execution_state import ExecutionState, StateError
from .test_maxpain_execution import plan, ARM, CREATED, report_plan, REPORT_FORMULAS

T=1791288960000
ROUTES=dict(long_account=dict(account='0x'+'1'*40),short_account=dict(account='0x'+'2'*40))
META={'universe':[dict(name=s,szDecimals=2,maxLeverage=10) for s in ('XRP','HYPE','SOL','DOGE','ETH')]}


class SoftwareExchange:
    domain='software'
    software_only=True

    def __init__(self,at_ms):
        self.t=at_ms; self.orders={}; self.requests=[]; self.mark={}; self.paths={}; self.bars={}
        self.lose_reply=False; self.before_send=None; self.entry_account_override={}; self.omit_order=None
        self.instant_fraction=Decimal(1)

    def now(self):return self.t

    def _key(self,account,symbol):return runtime._lane(account,symbol)

    def collect(self,state):
        snapshots=[];ranges={};entry_accounts={};marks={}
        lanes={(state['routes']['long_account' if s['source']['side']=='LONG' else 'short_account'],s['source']['symbol']) for s in state['sources'].values()}
        lanes|={(t['account'],t['symbol']) for t in state['trades'].values()}
        lanes|={(o['account'],o['symbol']) for o in self.orders.values()}
        for account,symbol in sorted(lanes):
            orders=[deepcopy(o['view']) for o in self.orders.values() if o['account']==account and o['symbol']==symbol and o['view']['oid']!=self.omit_order]
            position=sum((sum((Decimal(f['quantity']) for f in o['view']['fills']),Decimal(0))*(1 if o['view']['wire_order']['b'] else -1)
                for o in self.orders.values() if o['account']==account and o['symbol']==symbol),Decimal(0))
            snapshots.append(dict(environment='testnet',account=account,symbol=symbol,at_ms=self.t,history_complete=True,
                orders_complete=True,position_complete=True,position_quantity=str(position),orders=orders))
        for cid,s in state['sources'].items():
            msg=s['source'];account=state['routes']['long_account' if msg['side']=='LONG' else 'short_account'];key=self._key(account,msg['symbol'])
            origin=msg['proof'].get('source_price',msg['entry'])
            if msg['family']=='sol_g65':origin=msg['proof']['reference_price']
            price=self.mark.get(key,str(origin));marks[key]=dict(environment='testnet',account=account,symbol=msg['symbol'],at_ms=self.t,mark_price=price)
            values=dict(low=price,high=price,price=price)
            src=dict(environment='mainnet',symbol=msg['symbol'],reference_at_ms=contract.moment_ms(msg['source_at']),at_ms=self.t,
                history_complete=True,price_source=msg['policy']['source_price'],**values)
            demo=dict(environment='testnet',symbol=msg['symbol'],reference_at_ms=contract.moment_ms(msg['source_at']),at_ms=self.t,
                history_complete=True,account=account,price_kind='MARK',**values)
            ranges[cid]=deepcopy(self.paths.get(cid,dict(source=src,testnet=demo)))
            for v in ranges[cid].values():v['at_ms']=self.t
            role='long_account' if msg['side']=='LONG' else 'short_account'
            agent=state.get('agents',{}).get(role,'0x'+('3' if role=='long_account' else '4')*40)
            entry_accounts[cid]=dict(account=account,agent=agent,at_ms=self.t,action_headroom=1000)
            entry_accounts[cid].update(self.entry_account_override.get(cid,{}))
        return dict(basis_revision=state['revision'],inventory_complete=True,inventory_accounts=sorted(state['routes'].values()),inventory_at_ms=self.t,metadata=deepcopy(META),snapshots=snapshots,ranges=ranges,
                    entry_accounts=entry_accounts,bars=deepcopy(self.bars),marks=marks)

    def send(self,request,*,admission):
        admission.consume(request)
        if self.before_send:self.before_send(request)
        self.requests.append(deepcopy(request));self.t+=1
        p=request['proposal']; action=p['action']
        if action['type']=='cancel':
            oid=str(action['cancels'][0]['o']);self.orders[oid]['view'].update(status='CANCELED',at_ms=self.t)
            reply=dict(status='CANCELED',oid=oid)
        else:
            if action['type']=='batchModify':
                item=action['modifies'][0];old=str(item['oid'])
                self.orders[old]['view'].update(status='CANCELED',at_ms=self.t);order=item['order']
            else:order=action['orders'][0]
            oid=str(len(self.orders)+1)
            view=dict(oid=oid,cloid=order['c'],wire_order=deepcopy(order),status='OPEN',at_ms=self.t,fills=[])
            self.orders[oid]=dict(account=p['account'],symbol=p['symbol'],leg=p['leg'],view=view)
            if order['t']=={'limit':{'tif':'Ioc'}}:
                q=Decimal(order['s'])*self.instant_fraction
                if q:self.fill(oid,str(q),order['p'])
                if view['status']=='OPEN':view.update(status='CANCELED',at_ms=self.t)
            reply=dict(status='ACK',oid=oid)
        if self.lose_reply:
            self.lose_reply=False
            raise TimeoutError('SIMULATED_REPLY_LOST')
        return reply

    def fill(self,oid,quantity,price=None):
        self.t+=1;o=self.orders[oid]['view'];q=Decimal(quantity)
        o['fills'].append(dict(fill_id='fill'+str(oid)+'_'+str(len(o['fills'])),quantity=str(q),price=price or o['wire_order']['p'],at_ms=self.t))
        total=sum((Decimal(f['quantity']) for f in o['fills']),Decimal(0))
        o.update(status='FILLED' if total==Decimal(o['wire_order']['s']) else 'OPEN',at_ms=self.t)

    def oid(self,leg,index=-1):
        return [oid for oid,o in self.orders.items() if o['leg']==leg][index]


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        for target in ('http.client.HTTPSConnection','socket.create_connection','hyperliquid_testnet_executor._wallet','hyperliquid_testnet_executor._signed_body'):
            p=patch(target,side_effect=AssertionError('ISOLATED_NO_NETWORK_OR_SIGNER'));p.start();self.addCleanup(p.stop)
        self.path=Path(self.tmp.name)/'worker.isolated-experimental.sqlite3'
        self.venue=SoftwareExchange(T+10000)
        self.store=ExecutionState(self.path);self.store.initialize(ROUTES,not_before_ms=T-60000)
        self.worker=runtime.IsolatedExecutionRuntime(self.store,self.venue,mode=runtime.MODE)

    def r2732(self):return r2732_message(entry=2.3,decision_ms=T-60000)
    def start(self,msg=None):
        msg=self.r2732() if msg is None else msg
        self.worker.receive([msg]); result=self.worker.run_once();return msg,result
    def protect(self):
        self.worker.run_once();self.worker.run_once();self.worker.run_once()
    def restart(self):
        self.store=ExecutionState(self.path);self.worker=runtime.IsolatedExecutionRuntime(self.store,self.venue,mode=runtime.MODE)
    def cancel(self,msg):
        value=deepcopy(msg);value.update(kind='CANCEL',source_state='OPEN',cancel_reason='SOURCE_ENTRY_OBSERVED',
            source_sequence=self.venue.t*10+contract.RANK['CANCEL'],source_as_of=contract.iso_ms(self.venue.t),valid_until=contract.iso_ms(min(self.venue.t+90000,contract.moment_ms(msg['expires_at'])) if msg['expires_at'] else self.venue.t+90000))
        self.worker.receive([contract.validate(value)])

    def test_complete_entry_protection_take_cleanup_finality_restart(self):
        msg,result=self.start();self.assertEqual(result['operation'],'ENTRY');self.protect()
        state=self.store.load();trade=state['trades'][msg['occurrence_id']]
        self.assertEqual(trade['phase'],'OPEN');self.assertEqual(len(self.venue.requests),3)
        self.venue.fill(self.venue.oid('TAKE_PROFIT'),trade['quantity'])
        result=self.worker.run_once();self.assertEqual(result['operation'],'CANCEL')
        self.worker.run_once();self.worker.run_once();self.restart();self.worker.run_once()
        self.assertEqual(self.store.load()['trades'][msg['occurrence_id']]['phase'],'CLOSED')
        self.assertEqual(len(self.venue.requests),4)
        self.assertFalse(result['live_dispatch_enabled']);self.assertEqual(result['real_exchange_requests_sent'],0)

    def test_reply_loss_reconciles_exact_request_never_retries(self):
        self.venue.lose_reply=True;msg,result=self.start()
        self.assertEqual(result['status'],'OUTCOME_UNKNOWN_RECONCILIATION_REQUIRED')
        self.restart();self.protect()
        entries=[r for r in self.venue.requests if r['proposal']['operation']=='ENTRY']
        self.assertEqual(len(entries),1);self.assertEqual(len(self.venue.requests),3)

    def test_duplicate_receipt_never_reenters(self):
        msg,_=self.start();self.worker.receive([msg,msg]);self.protect()
        self.assertEqual(len(self.store.load()['trades']),1)
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in self.venue.requests),1)

    def test_cancel_after_partial_entry_keeps_actual_protection(self):
        self.venue.instant_fraction=Decimal('.5');msg,_=self.start();self.cancel(msg);self.protect()
        trade=self.store.load()['trades'][msg['occurrence_id']];remaining=runtime._remaining(trade)
        self.assertEqual(remaining,Decimal(trade['quantity'])/2)
        for leg in ('STOP','TAKE_PROFIT'):
            self.assertEqual(Decimal(self.venue.orders[self.venue.oid(leg)]['view']['wire_order']['s']),remaining)

    def test_halt_entries_continues_protection(self):
        msg,_=self.start();self.worker.run_once(entries_enabled=False);self.worker.run_once(entries_enabled=False)
        self.assertEqual(len(self.venue.requests),3)

    def test_source_before_send_cancellation_prevents_wire_attempt(self):
        msg=self.r2732();self.worker.receive([msg]);original=self.store.mutate;calls=[0]
        def mutate(fn):
            result=original(fn);calls[0]+=1
            if calls[0]==1:self.cancel(msg)
            return result
        with patch.object(self.store,'mutate',side_effect=mutate):result=self.worker.run_once()
        self.assertEqual(result['status'],'CANCELED_BEFORE_TRANSPORT');self.assertFalse(self.venue.requests)

    def test_entry_final_commit_ack_loss_never_sends_or_retries(self):
        msg=self.r2732();self.worker.receive([msg]);original=self.store.mutate
        def lose(fn):
            original(fn);raise RuntimeError('COMMIT_ACK_LOST')
        with patch.object(self.store,'mutate',side_effect=lose),self.assertRaisesRegex(RuntimeError,'COMMIT_ACK_LOST'):
            self.worker.run_once()
        self.restart();self.worker.run_once();self.assertFalse(self.venue.requests)
        self.assertEqual(len(self.store.load()['requests']),1)

    def test_missing_owned_order_fails_closed(self):
        self.start();self.protect();self.venue.omit_order=self.venue.oid('STOP');before=len(self.venue.requests)
        with self.assertRaisesRegex(runtime.RuntimeError,'OWNED_ORDER_MISSING'):
            self.worker.run_once()
        self.assertEqual(len(self.venue.requests),before)

    def test_account_action_headroom_is_a_real_admission_gate(self):
        msg=self.r2732();self.worker.receive([msg]);self.venue.entry_account_override[msg['occurrence_id']]={'action_headroom':0}
        with self.assertRaisesRegex(runtime.RuntimeError,'ENTRY_ACCOUNT'):
            self.worker.run_once()
        self.assertFalse(self.store.load()['trades']);self.assertFalse(self.venue.requests)

    def test_local_workers_cannot_both_claim_different_same_formula_occurrences(self):
        msg=self.r2732();self.worker.receive([msg]);worker2=runtime.IsolatedExecutionRuntime(ExecutionState(self.path),self.venue,mode=runtime.MODE)
        def run(w):
            try:return w.run_once()
            except runtime.RuntimeError as exc:return str(exc)
        with ThreadPoolExecutor(2) as pool:list(pool.map(run,[self.worker,worker2]))
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in self.venue.requests),1)

    def test_maxpain_partial_fill_resize_cancel_race_and_finality(self):
        self.venue.t=CREATED
        path=Path(self.tmp.name)/'mp.isolated-experimental.sqlite3'
        self.store=ExecutionState(path);self.store.initialize(ROUTES,not_before_ms=CREATED-60000)
        self.worker=runtime.IsolatedExecutionRuntime(self.store,self.venue,mode=runtime.MODE)
        msg=plan();self.worker.receive([msg]);self.venue.t=ARM;self.worker.run_once()
        oid=self.venue.oid('ENTRY');qty=Decimal(self.venue.orders[oid]['view']['wire_order']['s'])
        self.venue.fill(oid,'1',msg['entry']);self.venue.mark[runtime._lane(ROUTES['long_account']['account'],'HYPE')]=msg['entry']
        self.protect();self.assertEqual(self.venue.orders[self.venue.oid('STOP')]['view']['wire_order']['s'],'1')
        self.venue.fill(oid,str(qty-1),msg['entry']);self.protect()
        self.assertEqual(Decimal(self.venue.orders[self.venue.oid('STOP')]['view']['wire_order']['s']),qty)
        self.assertEqual(Decimal(self.venue.orders[self.venue.oid('TAKE_PROFIT')]['view']['wire_order']['s']),qty)
        self.venue.fill(self.venue.oid('STOP'),str(qty));self.worker.run_once();self.worker.run_once();self.worker.run_once()
        self.assertEqual(self.store.load()['trades'][msg['occurrence_id']]['phase'],'CLOSED')

    def test_eight_approved_formulas_reach_observed_initial_protection(self):
        formulas=[]
        for spec in REPORT_FORMULAS:
            formulas.append(report_plan(spec,(spec[2]+spec[3])/2))
        formulas.extend([r2732_message(entry=2.3,decision_ms=ARM-60000),
            hype_row71205_message(entry=100,decision_ms=ARM-60000),sol_g65_message(reference=100,decision_ms=ARM-60000)])
        for i,msg in enumerate(formulas):
            with self.subTest(rule=msg['rule_id']):
                venue=SoftwareExchange(CREATED if msg['family']=='maxpain' else ARM+10000)
                store=ExecutionState(Path(self.tmp.name)/('all'+str(i)+'.isolated-experimental.sqlite3'))
                store.initialize(ROUTES,not_before_ms=CREATED-60000)
                worker=runtime.IsolatedExecutionRuntime(store,venue,mode=runtime.MODE)
                worker.receive([msg]);venue.t=max(venue.t,ARM)
                result=worker.run_once();self.assertEqual(result['operation'],'ENTRY')
                if msg['family'] in ('maxpain','sol_g65'):
                    oid=venue.oid('ENTRY');venue.fill(oid,venue.orders[oid]['view']['wire_order']['s'])
                    venue.mark[runtime._lane(ROUTES['long_account' if msg['side']=='LONG' else 'short_account']['account'],msg['symbol'])]=msg['entry']
                worker.run_once();worker.run_once();worker.run_once()
                self.assertEqual(store.load()['trades'][msg['occurrence_id']]['phase'],'OPEN')
                self.assertEqual({o['leg'] for o in venue.orders.values()},{'ENTRY','STOP','TAKE_PROFIT'})

    def test_r2732_closed_minute_lock_runs_through_owned_modify_and_restart(self):
        msg,_=self.start();self.protect();self.venue.t=T+60000
        self.venue.mark[runtime._lane(ROUTES['short_account']['account'],'XRP')]='2.275'
        self.venue.bars[msg['occurrence_id']]=[dict(open_at_ms=T,open='2.3',high='2.305',low='2.27',close='2.275')]
        self.venue.lose_reply=True;result=self.worker.run_once();self.assertEqual(result['status'],'OUTCOME_UNKNOWN_RECONCILIATION_REQUIRED')
        self.restart();self.worker.run_once()
        stop=self.venue.orders[self.venue.oid('STOP')]['view']
        self.assertEqual(stop['wire_order']['p'],'2.2943')
        self.assertEqual(len([r for r in self.venue.requests if r['proposal']['operation']=='AMEND_EXIT']),1)

    def test_same_symbol_parallel_denied_but_opposite_account_other_symbol_runs(self):
        self.venue.t=CREATED;self.store=ExecutionState(Path(self.tmp.name)/'parallel.isolated-experimental.sqlite3')
        self.store.initialize(ROUTES,not_before_ms=CREATED-60000);self.worker=runtime.IsolatedExecutionRuntime(self.store,self.venue,mode=runtime.MODE)
        first=plan();second=plan(target=101.3,cycle='cycle-2')
        self.worker.receive([first,second]);self.venue.t=ARM;self.worker.run_once()
        oid=self.venue.oid('ENTRY');self.venue.fill(oid,self.venue.orders[oid]['view']['wire_order']['s']);self.protect()
        self.worker.run_once();self.assertEqual(len(self.store.load()['trades']),1)
        msg=r2732_message(entry=2.3,decision_ms=ARM-60000);self.venue.t=ARM+10000;self.worker.receive([msg]);self.worker.run_once();self.protect()
        state=self.store.load();self.assertEqual(len(state['trades']),2)
        self.assertEqual({t['role'] for t in state['trades'].values()},{'short_account','long_account'})

    def test_crossed_stop_after_fill_uses_existing_emergency_price_and_cleans_up(self):
        msg,_=self.start();key=runtime._lane(ROUTES['short_account']['account'],'XRP');self.venue.mark[key]='2.4'
        result=self.worker.run_once();self.assertEqual(result['operation'],'EMERGENCY_CLOSE')
        self.worker.run_once();self.worker.run_once()
        self.assertEqual(self.store.load()['trades'][msg['occurrence_id']]['phase'],'CLOSED')
        self.assertEqual(sum(r['proposal']['operation']=='EMERGENCY_CLOSE' for r in self.venue.requests),1)

    def test_bad_source_candle_does_not_delay_initial_stop(self):
        msg,_=self.start();self.venue.bars[msg['occurrence_id']]=[dict(open_at_ms=T,open='bad')]
        result=self.worker.run_once();self.assertEqual(result['operation'],'CREATE_EXIT')
        self.assertEqual(self.venue.orders[self.venue.oid('STOP')]['view']['wire_order']['p'],'2.3115')

    def test_g65_fill_minute_close_and_later_low_lock_are_connected(self):
        self.venue.t=ARM+10000
        self.store=ExecutionState(Path(self.tmp.name)/'g65-lock.isolated-experimental.sqlite3')
        self.store.initialize(ROUTES,not_before_ms=CREATED-60000);self.worker=runtime.IsolatedExecutionRuntime(self.store,self.venue,mode=runtime.MODE)
        msg=sol_g65_message(reference=100,decision_ms=ARM-60000);self.start(msg)
        oid=self.venue.oid('ENTRY');self.venue.fill(oid,self.venue.orders[oid]['view']['wire_order']['s'])
        key=runtime._lane(ROUTES['short_account']['account'],'SOL');self.venue.mark[key]='100.5';self.protect()
        self.venue.t=ARM+60000;self.venue.mark[key]='99.5'
        self.venue.bars[msg['occurrence_id']]=[dict(open_at_ms=ARM,open='100',high='100.6',low='99.2',close='99.5')]
        self.worker.run_once();self.assertEqual(self.venue.orders[self.venue.oid('STOP')]['view']['wire_order']['p'],'101.25')
        self.venue.t=ARM+120000;self.venue.mark[key]='99'
        self.venue.bars[msg['occurrence_id']].append(dict(open_at_ms=ARM+60000,open='99.5',high='100',low='98.7',close='99'))
        result=self.worker.run_once();self.assertEqual(result['operation'],'AMEND_EXIT')
        self.worker.run_once();self.restart()
        stop=self.venue.orders[self.venue.oid('STOP')]['view']['wire_order']['p']
        self.assertEqual(stop,'99.937')

    def test_maxpain_half_take_does_not_cancel_pending_but_original_target_does(self):
        self.venue.t=CREATED;self.store=ExecutionState(Path(self.tmp.name)/'target.isolated-experimental.sqlite3')
        self.store.initialize(ROUTES,not_before_ms=CREATED-60000);self.worker=runtime.IsolatedExecutionRuntime(self.store,self.venue,mode=runtime.MODE)
        msg=plan();self.worker.receive([msg]);self.venue.t=ARM;self.worker.run_once()
        key=runtime._lane(ROUTES['long_account']['account'],'HYPE');self.venue.mark[key]='100.6'
        self.assertEqual(self.worker.run_once()['status'],'OBSERVED_NO_ACTION')
        self.venue.mark[key]='101';self.assertEqual(self.worker.run_once()['operation'],'CANCEL')
        self.worker.run_once();self.worker.run_once()
        self.assertEqual(self.store.load()['trades'][msg['occurrence_id']]['phase'],'CANCELED_WITHOUT_FILL')

    def test_emergency_partial_terminal_allows_only_reconciled_residual(self):
        msg,_=self.start();self.venue.instant_fraction=Decimal('.5')
        self.venue.mark[runtime._lane(ROUTES['short_account']['account'],'XRP')]='2.4'
        self.worker.run_once();self.venue.instant_fraction=Decimal(1);self.venue.lose_reply=True
        result=self.worker.run_once();self.assertEqual(result['status'],'OUTCOME_UNKNOWN_RECONCILIATION_REQUIRED')
        requests=[r for r in self.venue.requests if r['proposal']['operation']=='EMERGENCY_CLOSE']
        self.assertEqual(len(requests),2)
        self.assertEqual(Decimal(requests[1]['proposal']['quantity']),Decimal(requests[0]['proposal']['quantity'])/2)
        self.restart();self.worker.run_once();self.worker.run_once()
        self.assertEqual(self.store.load()['trades'][msg['occurrence_id']]['phase'],'CLOSED')
        self.assertEqual(len(self.venue.requests),3)

    def test_stale_mark_cannot_be_freshened_into_emergency(self):
        self.start();original=self.venue.collect
        def stale(state):
            context=original(state)
            for mark in context['marks'].values():mark['at_ms']=self.venue.t-15001
            return context
        before=len(self.venue.requests)
        with patch.object(self.venue,'collect',side_effect=stale),self.assertRaisesRegex(runtime.RuntimeError,'EXACT_FRESH_TESTNET_MARK'):
            self.worker.run_once()
        self.assertEqual(len(self.venue.requests),before)

    def test_durable_nonce_strictly_increases_with_frozen_clock(self):
        msg,_=self.start();trade=self.store.load()['trades'][msg['occurrence_id']]
        now=self.venue.t;proposal=self.venue.requests[0]['proposal']
        values=[]
        for _ in range(2):values.append(self.store.mutate(lambda state:self.worker._reserve(state,deepcopy(proposal),now)))
        self.assertEqual(values[1]['nonce'],values[0]['nonce']+1)
        self.restart();value=self.store.mutate(lambda state:self.worker._reserve(state,deepcopy(proposal),now))
        self.assertEqual(value['nonce'],values[1]['nonce']+1)

    def test_cancel_remainder_racing_fill_resizes_and_keeps_protection(self):
        self.venue.t=CREATED;self.store=ExecutionState(Path(self.tmp.name)/'cancel-race.isolated-experimental.sqlite3')
        self.store.initialize(ROUTES,not_before_ms=CREATED-60000);self.worker=runtime.IsolatedExecutionRuntime(self.store,self.venue,mode=runtime.MODE)
        msg=plan();self.worker.receive([msg]);self.venue.t=ARM;self.worker.run_once()
        oid=self.venue.oid('ENTRY');self.venue.fill(oid,'1');self.protect();self.cancel(msg)
        def racing(request):
            if request['proposal']['operation']=='CANCEL':
                self.venue.before_send=None;self.venue.fill(oid,'1')
        self.venue.before_send=racing;self.worker.run_once();self.protect()
        trade=self.store.load()['trades'][msg['occurrence_id']]
        self.assertEqual(runtime._remaining(trade),Decimal(2))
        self.assertEqual(self.venue.orders[self.venue.oid('STOP')]['view']['wire_order']['s'],'2')
        self.assertEqual(self.venue.orders[self.venue.oid('TAKE_PROFIT')]['view']['wire_order']['s'],'2')

    def test_account_inventory_completeness_is_required_before_attempt(self):
        self.worker.receive([self.r2732()]);original=self.venue.collect
        def incomplete(state):
            result=original(state);result['inventory_complete']=False;return result
        with patch.object(self.venue,'collect',side_effect=incomplete),self.assertRaisesRegex(runtime.RuntimeError,'TWO_ACCOUNT_INVENTORY'):
            self.worker.run_once()
        self.assertFalse(self.venue.requests)

    def test_no_live_adapter_or_default_startup(self):
        with self.assertRaisesRegex(runtime.RuntimeError,'EXPLICIT_ISOLATED'):
            runtime.IsolatedExecutionRuntime(self.store,self.venue)
        self.venue.domain='testnet'
        with self.assertRaisesRegex(runtime.RuntimeError,'SOFTWARE_EXCHANGE'):
            runtime.IsolatedExecutionRuntime(self.store,self.venue,mode=runtime.MODE)


if __name__=='__main__':unittest.main()
