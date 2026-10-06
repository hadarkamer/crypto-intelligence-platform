"""Actual isolated worker -> disabled wire boundary -> simulated venue orders."""
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from . import card_lifecycle as life, experimental_execution_dispatch as boundary
from . import experimental_execution_runtime as runtime, checks
from .experimental_execution_state import ExecutionState
from .test_experimental_execution_runtime import SoftwareExchange, ROUTES, META, T
from .test_experimental_execution_dispatch import Signer
from experimental_execution_fixtures import r2732_message


def legacy_view(trade,snapshot):
    binding=dict(card_id=trade['cid'],card_digest=life.digest(trade['source']),account=trade['account'],role=trade['role'],
        symbol=trade['symbol'],side=trade['side'],planned_quantity=trade['quantity'],prices=deepcopy(trade['prices']),
        orders={k:[oid for oid,leg in trade['order_legs'].items() if leg==k] for k in ('ENTRY','STOP','TAKE_PROFIT')},environment='testnet')
    view=dict(environment='testnet',account=trade['account'],symbol=trade['symbol'],at_ms=snapshot['at_ms'],history_complete=True,
        orders_complete=True,position_quantity=snapshot['position_quantity'],fills=[],open_orders=[],terminal_orders=[])
    for order in snapshot['orders']:
        o=order['wire_order']; common=dict(account=trade['account'],symbol=trade['symbol'],oid=order['oid'])
        filled=sum((Decimal(f['quantity']) for f in order['fills']),Decimal(0))
        view['fills'] += [dict(**common,**f,fee='0',fee_token='USDC',side='B' if o['b'] else 'A') for f in order['fills']]
        if order['status']=='OPEN':
            trigger=o['t'].get('trigger')
            view['open_orders'].append(dict(**common,quantity=life.text(Decimal(o['s'])-filled),price=o['p'],trigger_price=None if not trigger else trigger['triggerPx'],side='B' if o['b'] else 'A',reduce_only=o['r'],state='ACTIVE',order_type='LIMIT' if not trigger else 'SL_MARKET' if trigger['tpsl']=='sl' else 'TP_LIMIT'))
        else:
            view['terminal_orders'].append(dict(**common,state=order['status'],filled_quantity=life.text(filled),at_ms=order['at_ms']))
    return binding,view


class BoundaryExchange(SoftwareExchange):
    def __init__(self,at_ms,store):
        super().__init__(at_ms);self.store=store;self.boundary_calls=[];self.port=boundary.DisabledDispatchPort()
    def context_for(self,request):
        state=self.store.load();p=request['proposal'];trade=state['trades'][p['card_id']]
        context=self.collect(state);snapshot=next(s for s in context['snapshots'] if s['account']==trade['account'] and s['symbol']==trade['symbol'])
        env=dict(HL_TESTNET_LONG_ACCOUNT_ADDRESS=ROUTES['long_account']['account'],HL_TESTNET_LONG_AGENT_ADDRESS='0x'+'3'*40,
            HL_TESTNET_SHORT_ACCOUNT_ADDRESS=ROUTES['short_account']['account'],HL_TESTNET_SHORT_AGENT_ADDRESS='0x'+'4'*40)
        agent=env['HL_TESTNET_LONG_AGENT_ADDRESS' if p['role']=='long_account' else 'HL_TESTNET_SHORT_AGENT_ADDRESS']
        cap=context['capacity'][p['card_id']];diagnostics={};plan=dict(symbol=p['symbol'],side=trade['side'],**trade['prices'])
        checks.plan_check(plan,p['symbol'],trade['asset']['decimals'],Decimal(cap['unheld']),Decimal(cap['available']),Decimal(cap['max_size']),active=cap['active'],metadata_max_leverage=cap['max_leverage'],diagnostics=diagnostics)
        result=dict(source=trade['source'],current_source=state['sources'][p['card_id']],env=env,agent=agent,host=boundary.HOST,metadata=META,
            safety=dict(account=p['account'],role=p['role'],at_ms=self.t,entry_enabled=True,emergency_healthy=True,feed_reconciled=True,
                entry_circuit_clear=True,supervisor_at_ms=self.t,not_before_ms=state['not_before_ms']),buckets=[],open_orders=[],positions={'assetPositions':[]},
            market={k:v for k,v in context['marks'][runtime._lane(p['account'],p['symbol'])].items() if k!='account'},
            budget_at_ms=self.t,entry_action_headroom=cap['action_headroom'],budget_report=dict(status='PRECHECK_PASSED_NOT_ORDER_AUTHORIZATION',test_plan_checked=True,budget_diagnostics=diagnostics))
        if trade['orders']:
            result['owner'],result['owner_snapshot']=legacy_view(trade,snapshot)
            result['lock_proof']=trade['condition']
            result['observed_stop_requests']=[deepcopy(r) for r in state['requests'].values() if r['proposal']['card_id']==trade['cid'] and r['proposal']['leg']=='STOP' and r['phase']=='OBSERVED']
        return result
    def send(self,request,*,admission):
        exchange=self; signer=Signer();context=self.context_for(request);signer.address=context['agent']
        class Transport:
            domain='software'
            def current_context(self):return exchange.context_for(request)
            def exchange(self,host,path,body):
                exchange.boundary_calls.append(deepcopy(body))
                class AlreadyConsumed:
                    def consume(self,again):
                        assert admission._used and again==request
                reply=SoftwareExchange.send(exchange,request,admission=AlreadyConsumed())
                if reply['status']=='CANCELED':return dict(status='ok',response=dict(type='cancel',data=dict(statuses=['success'])))
                return dict(status='ok',response=dict(type='order',data=dict(statuses=[dict(resting=dict(oid=int(reply['oid'])))])))
        return self.port.rehearse(request,context,admission=admission,signer=signer,transport=Transport(),clock=self.now)


class DispatchIntegrationTests(unittest.TestCase):
    def setUp(self):
        for target in ('http.client.HTTPSConnection','socket.create_connection','socket.socket.connect','hl_testnet_runtime.two_account_execution.wallet_for_role'):
            guard=patch(target,side_effect=AssertionError('ISOLATED_ONLY'));guard.start();self.addCleanup(guard.stop)
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        self.store=ExecutionState(Path(tmp.name)/'wire.isolated-experimental.sqlite3');self.store.initialize(ROUTES,not_before_ms=T-60000)
        self.venue=BoundaryExchange(T+10000,self.store);self.worker=runtime.IsolatedExecutionRuntime(self.store,self.venue,mode=runtime.MODE)
        self.msg=r2732_message(entry=2.3,decision_ms=T-60000)
    def test_worker_entry_partial_fill_stops_take_cleanup_uses_same_wire_boundary(self):
        self.venue.instant_fraction=Decimal('.5');self.worker.receive([self.msg]);self.worker.run_once()
        for _ in range(3):self.worker.run_once()
        self.assertEqual(len(self.venue.boundary_calls),3)
        trade=self.store.load()['trades'][self.msg['occurrence_id']]
        self.assertEqual(trade['phase'],'OPEN')
        remaining=runtime._remaining(trade);self.assertEqual(remaining,Decimal(trade['quantity'])/2)
        for leg in ('STOP','TAKE_PROFIT'):self.assertEqual(Decimal(self.venue.orders[self.venue.oid(leg)]['view']['wire_order']['s']),remaining)
        self.venue.fill(self.venue.oid('TAKE_PROFIT'),life.text(remaining))
        for _ in range(3):self.worker.run_once()
        self.assertEqual(len(self.venue.boundary_calls),4)
        self.assertEqual(self.store.load()['trades'][self.msg['occurrence_id']]['phase'],'CLOSED')
    def test_closed_minute_lock_and_cleanup_validates_exact_replacement_stop(self):
        self.worker.receive([self.msg])
        for _ in range(4): self.worker.run_once()
        self.venue.t=T+60000
        self.venue.bars[self.msg['occurrence_id']]=[dict(open_at_ms=T,open='2.3',high='2.3',low='2.277',close='2.28')]
        self.venue.mark[runtime._lane(ROUTES['short_account']['account'],'XRP')]='2.28'
        result=self.worker.run_once()
        self.assertEqual(result.get('operation'),'AMEND_EXIT')
        self.worker.run_once()
        trade=self.store.load()['trades'][self.msg['occurrence_id']]
        self.venue.fill(self.venue.oid('TAKE_PROFIT'),trade['quantity'])
        for _ in range(3):self.worker.run_once()
        self.assertEqual(self.store.load()['trades'][self.msg['occurrence_id']]['phase'],'CLOSED')
    def test_crossed_unprotected_stop_uses_exact_owned_emergency_ioc(self):
        self.worker.receive([self.msg]);self.worker.run_once()
        self.venue.mark[runtime._lane(ROUTES['short_account']['account'],'XRP')]='2.32'
        result=self.worker.run_once()
        self.assertEqual(result.get('operation'),'EMERGENCY_CLOSE')
        self.worker.run_once()
        self.assertEqual(self.store.load()['trades'][self.msg['occurrence_id']]['phase'],'CLOSED')
    def protected_request(self):
        self.worker.receive([self.msg])
        for _ in range(4):self.worker.run_once()
        state=self.store.load()
        request=deepcopy(next(r for r in state['requests'].values() if r['proposal']['leg']=='STOP'))
        request.update(phase='OUTCOME_UNKNOWN',reply=None,observed_oid=None,attempt_at_ms=self.venue.t,prepared_at_ms=self.venue.t,nonce=self.venue.t)
        request['proposal']['observed_at_ms']=self.venue.t
        return request
    def test_duplicate_exit_create_does_not_oversubscribe_owner(self):
        request=self.protected_request()
        with self.assertRaisesRegex(boundary.BoundaryError,'CREATE_EXIT_ALREADY_ACTIVE'):
            boundary.review(request,self.venue.context_for(request),now_ms=self.venue.t)
    def test_emergency_cannot_close_before_any_frozen_barrier_is_crossed(self):
        request=self.protected_request();p=request['proposal'];p['operation']='EMERGENCY_CLOSE'
        p['sample']=dict(mark_price='2.3',at_ms=self.venue.t)
        from .emergency_close import close_price
        o=p['action']['orders'][0];o['t']={'limit':{'tif':'Ioc'}};o['p']=close_price('2.3',2,buy=True)
        p['action']=boundary.wire.canonical_wire_action(p['action'])
        with self.assertRaisesRegex(boundary.BoundaryError,'EMERGENCY_REQUIRES_CROSSED'):
            boundary.review(request,self.venue.context_for(request),now_ms=self.venue.t)
    def test_source_retirement_and_entry_halt_do_not_stop_owned_exit_cleanup(self):
        request=self.protected_request();p=request['proposal'];oid=self.venue.oid('STOP')
        p.update(operation='CANCEL',quantity='0',action=dict(type='cancel',cancels=[dict(a=0,o=int(oid))]))
        context=self.venue.context_for(request)
        context['current_source']['entry_permission']='RETIRED';context['safety'].update(entry_enabled=False,emergency_healthy=False,feed_reconciled=False,entry_circuit_clear=False)
        self.assertTrue(boundary.review(request,context,now_ms=self.venue.t)['reviewed'])
    def test_lost_wire_reply_reconciles_and_never_resends_entry(self):
        self.venue.lose_reply=True;self.worker.receive([self.msg]);self.worker.run_once()
        self.worker=runtime.IsolatedExecutionRuntime(self.store,self.venue,mode=runtime.MODE)
        for _ in range(3):self.worker.run_once()
        self.assertEqual(len(self.venue.boundary_calls),3)
        self.assertEqual(sum(r['proposal']['leg']=='ENTRY' for r in self.venue.requests),1)

if __name__=='__main__':unittest.main()
