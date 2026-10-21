"""Real split-exit boundary and supervisor checks, with no network or keys."""
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import approved_alert_contract as contract
from approved_alert_fixtures import doge_partial_alert
from . import card_lifecycle as life, doge_partial_execution as doge
from . import experimental_execution_dispatch as boundary
from . import experimental_execution_runtime as runtime
from . import experimental_live_safety as safety
from .experimental_execution_state import ExecutionState
from .test_experimental_execution_runtime import ROUTES, T
from .test_experimental_dispatch_integration import BoundaryExchange


class DogeBoundaryExchange(BoundaryExchange):
    def context_for(self,request):
        result=super().context_for(request)
        trade=self.store.load()['trades'][request['proposal']['card_id']]
        if doge.is_doge(trade['source']) and trade['orders']:
            result['exit_trade']=deepcopy(trade)
            result['observed_exit_requests']=[deepcopy(r) for r in self.store.load()['requests'].values()
                if r['proposal']['card_id']==trade['cid'] and r['proposal']['leg']=='TAKE_PROFIT'
                and r['proposal']['action']['type']!='cancel' and r['phase']=='OBSERVED']
        return result


class DogeDispatchTests(unittest.TestCase):
    def setUp(self):
        for target in ('http.client.HTTPSConnection','socket.create_connection','socket.socket.connect',
                       'hl_testnet_runtime.two_account_execution.wallet_for_role'):
            guard=patch(target,side_effect=AssertionError('OFFLINE_NO_NETWORK_OR_KEYS'))
            guard.start(); self.addCleanup(guard.stop)
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        self.store=ExecutionState(Path(tmp.name)/'doge.isolated-experimental.sqlite3')
        self.store.initialize(ROUTES,not_before_ms=T-60000)
        self.venue=DogeBoundaryExchange(T+10000,self.store)
        self.worker=runtime.IsolatedExecutionRuntime(self.store,self.venue,mode=runtime.MODE)

    def start(self,side='LONG',quantity=None):
        self.msg=doge_partial_alert(side=side,approved_ms=T)
        self.worker.receive([self.msg]);result=self.worker.run_once()
        self.assertEqual(result.get('operation'),'ENTRY',result)
        trade=self.store.load()['trades'][self.msg['occurrence_id']]
        self.venue.fill(self.venue.oid('ENTRY'),quantity or trade['quantity'])
        return trade

    def protect(self):
        for _ in range(5):self.worker.run_once(entries_enabled=False)
        return self.store.load()['trades'][self.msg['occurrence_id']]

    def take_request(self,partial=True):
        price=doge.partial_price(self.msg,2) if partial else self.msg['take_profit']
        return deepcopy(next(r for r in self.store.load()['requests'].values()
            if r['proposal']['leg']=='TAKE_PROFIT' and r['proposal']['action']['type']!='cancel'
            and boundary.wire.requested_order(r['proposal']['action'])['p']==price))

    def fresh(self,request):
        request.update(nonce=self.venue.t,attempt_at_ms=self.venue.t,prepared_at_ms=self.venue.t,
                       phase='OUTCOME_UNKNOWN',reply=None,observed_oid=None)
        request['proposal']['observed_at_ms']=self.venue.t
        return request

    def test_both_sides_split_and_reconcile_through_actual_signing_boundary(self):
        self.start();trade=self.protect()
        expected=doge.desired_orders(trade)
        self.assertEqual(len(expected),3)
        self.assertEqual(len(self.venue.boundary_calls),4)
        self.assertEqual(sum(Decimal(q) for leg,p,q in expected if leg=='TAKE_PROFIT'),Decimal(trade['quantity']))
        partial=self.take_request();oid=partial['observed_oid']
        self.venue.fill(oid,partial['proposal']['quantity'])
        for _ in range(3):self.worker.run_once(entries_enabled=False)
        trade=self.store.load()['trades'][self.msg['occurrence_id']]
        self.assertEqual(runtime._remaining(trade),Decimal(trade['quantity'])*Decimal('.25'))
        final=self.take_request(False)
        self.venue.fill(final['observed_oid'],final['proposal']['quantity'])
        for _ in range(4):self.worker.run_once(entries_enabled=False)
        self.assertEqual(self.store.load()['trades'][self.msg['occurrence_id']]['phase'],'CLOSED')
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in self.venue.requests),1)

    def test_short_split_price_and_quantities_pass_independent_boundary(self):
        self.start('SHORT');trade=self.protect()
        self.assertEqual(len(self.venue.boundary_calls),4)
        self.assertEqual([(x['price'],Decimal(x['quantity'])) for x in doge.take_orders(trade)],
                         [('99.05',Decimal('1.5')),('98.4',Decimal('0.5'))])

    def test_wrong_quantity_same_target_duplicate_and_other_oid_rejected(self):
        self.start();self.protect();request=self.fresh(self.take_request())
        ctx=self.venue.context_for(request)
        with self.assertRaisesRegex(boundary.BoundaryError,'CREATE_EXIT_ALREADY_ACTIVE'):
            boundary.review(request,ctx,now_ms=self.venue.t)
        bad=deepcopy(request);bad['proposal']['quantity']='2';bad['proposal']['action']['orders'][0]['s']='2'
        with self.assertRaisesRegex(boundary.BoundaryError,'EXIT_EXCEEDS_OBSERVED'):
            boundary.review(bad,ctx,now_ms=self.venue.t)
        wrong=deepcopy(request);order=wrong['proposal']['action']['orders'][0]
        wrong['proposal'].update(operation='AMEND_EXIT',action=dict(type='batchModify',modifies=[
            dict(oid=int(self.take_request(False)['observed_oid']),order=order)]))
        with self.assertRaisesRegex(boundary.BoundaryError,'MODIFY_OID_NOT_EXCLUSIVELY_ACTIVE'):
            boundary.review(wrong,ctx,now_ms=self.venue.t)

    def test_fabricated_trade_fills_or_lost_durable_tp_proof_rejected(self):
        self.start();self.protect();original=self.take_request();request=self.fresh(deepcopy(original));ctx=self.venue.context_for(request)
        bad=deepcopy(ctx);next(iter(bad['exit_trade']['entry_fills'].values()))['quantity']='999'
        with self.assertRaisesRegex(boundary.BoundaryError,'EXACT_DOGE_OBSERVED_FILLS'):
            boundary.review(request,bad,now_ms=self.venue.t)
        bad=deepcopy(ctx);bad['observed_exit_requests']=[]
        with self.assertRaisesRegex(boundary.BoundaryError,'DOGE_TAKE_NOT_BOUND'):
            boundary.review(request,bad,now_ms=self.venue.t)
        bad=deepcopy(ctx);next(o for o in bad['owner_snapshot']['open_orders']
            if o['oid']==original['observed_oid'])['price']='100.94'
        with self.assertRaisesRegex(boundary.BoundaryError,'DOGE_TAKE_OBSERVED_TERMS'):
            boundary.review(request,bad,now_ms=self.venue.t)

    def time_request(self):
        request=self.fresh(deepcopy(next(r for r in self.store.load()['requests'].values()
            if r['proposal']['leg']=='STOP')))
        from .emergency_close import close_price
        p=request['proposal'];p['operation']='TIME_EXIT';p['sample']=dict(mark_price=self.msg['entry'],at_ms=self.venue.t)
        o=p['action']['orders'][0];o['t']={'limit':{'tif':'Ioc'}}
        o['p']=close_price(self.msg['entry'],2,buy=self.msg['side']=='SHORT')
        p['action']=boundary.wire.canonical_wire_action(p['action'])
        return request

    def test_timeout_is_independently_bound_to_original_deadline(self):
        self.start();self.protect();request=self.time_request()
        with self.assertRaisesRegex(boundary.BoundaryError,'HOLDING_DEADLINE_NOT_REACHED'):
            boundary.review(request,self.venue.context_for(request),now_ms=self.venue.t)
        self.venue.t=contract.moment_ms(self.msg['policy']['hold_until'])
        request=self.time_request()
        self.assertTrue(boundary.review(request,self.venue.context_for(request),now_ms=self.venue.t)['reviewed'])
        result=self.worker.run_once(entries_enabled=False)
        self.assertEqual(result.get('operation'),'TIME_EXIT',result)
        for _ in range(5):self.worker.run_once(entries_enabled=False)
        self.assertEqual(self.store.load()['trades'][self.msg['occurrence_id']]['phase'],'CLOSED')

    def test_crossed_partial_uses_only_partial_ioc_at_original_limit(self):
        trade=self.start();lane=runtime._lane(trade['account'],trade['symbol'])
        self.venue.mark[lane]='101'
        self.worker.run_once(entries_enabled=False) # original full stop first
        result=self.worker.run_once(entries_enabled=False)
        self.assertEqual(result.get('operation'),'PARTIAL_TAKE',result)
        sent=self.venue.requests[-1]['proposal'];self.assertEqual(Decimal(sent['quantity']),Decimal('1.5'))
        self.assertEqual(sent['action']['orders'][0]['p'],'100.95')
        for _ in range(4):self.worker.run_once(entries_enabled=False)
        trade=self.store.load()['trades'][self.msg['occurrence_id']]
        self.assertEqual(runtime._remaining(trade),Decimal('.5'))

    def test_native_ioc_lookup_requires_exact_approved_doge_scope_and_terminality(self):
        for operation,leg in (('TIME_EXIT','STOP'),('PARTIAL_TAKE','TAKE_PROFIT')):
            with self.subTest(operation=operation):
                order=dict(a=3,b=False,p='100.95',s='1.5',r=True,
                    t={'limit':{'tif':'Ioc'}},c='0x'+'f'*32)
                request=dict(attempt_at_ms=T,reply=None,proposal=dict(symbol='DOGE',leg=leg,
                    operation=operation,source_rule_id=doge.RULE,source_version=contract.VERSION,
                    action=dict(type='order',orders=[order],grouping='na')))
                raw=dict(status='order',order=dict(status='filled',statusTimestamp=T+1,order=dict(
                    oid=17,cloid=order['c'],coin='DOGE',side='A',reduceOnly=True,origSz='1.5',
                    sz='0',limitPx='100.95',orderType='Limit',isTrigger=False,triggerPx='0',
                    isPositionTpsl=False)))
                self.assertEqual(boundary.wire.identity(raw,request,T+2),'17')
                for mutation in ('rule','version','symbol','leg','reduce_only','tif','open','price','cloid'):
                    with self.subTest(mutation=mutation):
                        req=deepcopy(request);view=deepcopy(raw)
                        if mutation=='rule':req['proposal']['source_rule_id']='DOGE_OTHER'
                        elif mutation=='version':req['proposal']['source_version']='experimental-execution-plan-v1'
                        elif mutation=='symbol':req['proposal']['symbol']=view['order']['order']['coin']='ETH'
                        elif mutation=='leg':req['proposal']['leg']='ENTRY'
                        elif mutation=='reduce_only':req['proposal']['action']['orders'][0]['r']=False
                        elif mutation=='tif':req['proposal']['action']['orders'][0]['t']={'limit':{'tif':'Gtc'}}
                        elif mutation=='open':view['order']['status']='open'
                        elif mutation=='price':view['order']['order']['limitPx']='101'
                        else:view['order']['order']['cloid']='0x'+'e'*32
                        with self.assertRaises(ValueError):boundary.wire.identity(view,req,T+2)

    def test_supervisor_accepts_exact_split_only_and_flags_due_timeout(self):
        self.start();trade=self.protect();state=self.store.load()
        cap=object.__new__(safety.LiveSafetyProvider);cap.clock=self.venue.now
        self.assertTrue(cap._peers_protected(state))
        bad=deepcopy(state);t=bad['trades'][trade['cid']]
        partial=self.take_request()['observed_oid'];t['orders'][partial]['wire_order']['s']='2'
        self.assertFalse(cap._peers_protected(bad))
        bad=deepcopy(state);t=bad['trades'][trade['cid']]
        t['orders'][partial]['wire_order']['t']={'limit':{'tif':'Ioc'}}
        self.assertFalse(cap._peers_protected(bad))
        self.venue.t=contract.moment_ms(self.msg['policy']['hold_until'])
        state['snapshots'][runtime._lane(trade['account'],trade['symbol'])]['at_ms']=self.venue.t
        self.assertFalse(cap._peers_protected(state))


if __name__=='__main__':unittest.main()
