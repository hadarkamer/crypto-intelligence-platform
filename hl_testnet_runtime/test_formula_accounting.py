"""Exact economic accounting and real worker paths, with exchange I/O blocked."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from decimal import Decimal
import os
import unittest
from unittest.mock import Mock, patch

from approved_alert_fixtures import formula_alert, BASE
from . import formula_accounting as accounting
from . import card_lifecycle as life
from . import experimental_live_runtime as live
from . import experimental_execution_archive as archive
from . import experimental_execution_dispatch as boundary
from . import experimental_execution_reporting as reporting
from .test_experimental_live_runtime import RuntimeContract
from .test_experimental_live_state import LIVE_ROUTES
from .experimental_live_state import initial, TestnetExecutionState
from .test_experimental_execution_dispatch import approved_fixture
from . import test_experimental_live_provider as provider_support


def completed(state, *, cid='a'*64, formula='A', side='LONG', gross='10', fees='0.4'):
    """Reconciled synthetic $100 entry/$110 exit, not a planned-price result."""
    risk = accounting.risk_for(state, dict(rule_id=formula))
    account = state['routes']['long_account' if side == 'LONG' else 'short_account']
    source = dict(rule_id=formula, family='fixture', entry='100', stop='98', take_profit='110')
    entry = dict(fill_id='e'+cid, order_id='1', quantity='1', price='100', at_ms=BASE)
    exit_price = Decimal(100) + Decimal(gross) * (1 if side == 'LONG' else -1)
    exit_fill = dict(fill_id='x'+cid, order_id='2', quantity='1', price=str(exit_price), at_ms=BASE+2000)
    trade = dict(cid=cid, source=source, account=account, role='long_account' if side=='LONG' else 'short_account',
        symbol='HYPE', side=side, phase='CLOSED', prices=dict(entry='100',stop='98',take_profit='110'),
        entry_risk=risk, entry_fills={entry['fill_id']:entry}, exit_fills={exit_fill['fill_id']:exit_fill},
        orders={'1':dict(status='FILLED'),'2':dict(status='FILLED')}, fee_facts={})
    for fill, side_code, fee in ((entry, 'B' if side=='LONG' else 'A', fees),
                                 (exit_fill, 'A' if side=='LONG' else 'B', '0')):
        trade['fee_facts'][fill['fill_id']] = dict(oid=fill['order_id'],
            **{k:fill[k] for k in ('quantity','price','at_ms')}, side=side_code,fee=fee,fee_token='USDC')
    state['trades'][cid] = trade
    state['events'].append(dict(kind='CLOSED', occurrence_id=cid, at_ms=BASE+3000))
    return trade


def fund(state, amount='-0.1', *, cid='a'*64):
    trade = state['trades'][cid]
    task = accounting.funding_task(state, BASE+4000)
    assert task['cid'] == cid
    raw = [dict(time=BASE+1000, hash='0x'+'1'*64, delta=dict(type='funding', coin='HYPE',
        szi='1' if trade['side']=='LONG' else '-1', usdc=amount))] if amount else []
    accounting.record_funding(state, task, accounting.funding_page(task, raw), BASE+4000)


class AccountingTests(unittest.TestCase):
    def setUp(self):
        self.state = initial(LIVE_ROUTES, BASE-60000)
        blocker = patch('http.client.HTTPSConnection',side_effect=AssertionError('NO_EXCHANGE_IO'))
        blocker.start(); self.addCleanup(blocker.stop)

    def test_gain_loss_compound_only_own_formula(self):
        state=self.state
        self.assertEqual(Decimal(accounting.risk_for(state,dict(rule_id='B'))['planned_usd']),2)
        trade=completed(state);fund(state)
        accounting.settle(state,BASE+5000)
        self.assertEqual(Decimal(trade['settlement']['net_usd']),Decimal('9.5'))
        self.assertEqual(Decimal(accounting.risk_for(state,dict(rule_id='A'))['planned_usd']),Decimal('2.475'))
        self.assertEqual(Decimal(accounting.risk_for(state,dict(rule_id='B'))['planned_usd']),2)
        completed(state,cid='b'*64,gross='-2.475',fees='0.025');fund(state,'',cid='b'*64)
        accounting.settle(state,BASE+6000)
        self.assertEqual(Decimal(accounting.risk_for(state,dict(rule_id='A'))['planned_usd']),Decimal('2.35'))

    def test_short_funding_income_and_negative_fee_rebate(self):
        trade=completed(self.state,side='SHORT',fees='-0.2');fund(self.state,'0.3')
        accounting.settle(self.state,BASE+5000)
        self.assertEqual(Decimal(trade['settlement']['net_usd']),Decimal('10.5'))

    def test_duplicate_settlement_restart_and_report(self):
        trade=completed(self.state);fund(self.state)
        accounting.settle(self.state,BASE+5000)
        expected=deepcopy(self.state['formula_accounting'])
        restored=deepcopy(self.state)
        for _ in range(4):accounting.settle(restored,BASE+6000)
        self.assertEqual(restored['formula_accounting'],expected)
        row=reporting._project_rows(restored)[0]
        self.assertEqual(row['net_pnl_status'],'ACTUAL_NET_SETTLED')
        self.assertEqual(row['net_pnl'],trade['settlement']['net_usd'])

    def test_missing_fees_funding_unknown_orders_or_manual_never_assume_zero(self):
        for kind in ('fees','funding','orders','manual'):
            with self.subTest(kind=kind):
                state=initial(LIVE_ROUTES,BASE-60000);trade=completed(state)
                if kind!='funding':fund(state)
                if kind=='fees':trade['fee_facts'].clear()
                if kind=='orders':trade['orders']['2']['status']='OPEN'
                if kind=='manual':trade['manual_management']=dict(status='HUMAN_MANAGED')
                accounting.settle(state,BASE+5000)
                self.assertNotIn('settlement',trade)
                with self.assertRaisesRegex(accounting.AccountingError,'SETTLEMENT_PENDING'):
                    accounting.risk_for(state,dict(rule_id='A'))
                self.assertEqual(Decimal(accounting.risk_for(state,dict(rule_id='B'))['planned_usd']),2)

    def test_no_retroactive_changes_to_legacy_trades(self):
        trade=completed(self.state);trade.pop('entry_risk');self.state.pop('formula_accounting')
        original=deepcopy(trade)
        accounting.retain_fees(self.state,{})
        accounting.settle(self.state,BASE+5000)
        self.assertIsNone(accounting.funding_task(self.state,BASE+5000))
        self.assertEqual(trade,original)
        self.assertEqual(Decimal(accounting.risk_for(self.state,dict(rule_id='A'))['planned_usd']),2)

    def test_same_formula_both_accounts_share_capital(self):
        completed(self.state);fund(self.state);accounting.settle(self.state,BASE+5000)
        short=completed(self.state,cid='b'*64,side='SHORT')
        self.assertEqual(Decimal(short['entry_risk']['planned_usd']),Decimal('2.475'))
        self.assertEqual(len(self.state['formula_accounting']['portfolios']),1)

    def test_zero_or_negative_balance_never_resets_to_40(self):
        for loss in ('-40','-45'):
            state=initial(LIVE_ROUTES,BASE-60000);completed(state,gross=loss,fees='0');fund(state,'')
            accounting.settle(state,BASE+5000)
            with self.assertRaisesRegex(accounting.AccountingError,'CAPITAL_EXHAUSTED'):
                accounting.risk_for(state,dict(rule_id='A'))

    def test_bad_fee_currency_and_changed_fee_are_not_booked(self):
        trade=completed(self.state);fund(self.state)
        trade['fee_facts']['e'+'a'*64]['fee_token']='ETH'
        accounting.settle(self.state,BASE+5000)
        self.assertNotIn('settlement',trade)
        self.assertEqual(trade['accounting_pending'],'FORMULA_FEE_CURRENCY_REQUIRES_REVIEW')

    def test_conflicting_retained_fee_cannot_be_replaced_by_later_replay(self):
        trade=completed(self.state);fund(self.state)
        fid='e'+'a'*64
        row=dict(**trade['fee_facts'][fid],fill_id=fid,account=trade['account'],symbol=trade['symbol'])
        row['fee']='0.001'
        context=dict(collector_checkpoints={'lane':dict(fills=[row])})
        accounting.retain_fees(self.state,context)
        row['fee']='0.4'
        accounting.retain_fees(self.state,context)
        accounting.settle(self.state,BASE+5000)
        self.assertEqual(trade['accounting_pending'],'FORMULA_FILL_COST_CONFLICT')
        self.assertNotIn('settlement',trade)

    def test_funding_quantity_or_timestamp_ambiguity_never_allocates_account_cash(self):
        for change in ('quantity','time'):
            state=initial(LIVE_ROUTES,BASE-60000);trade=completed(state);fund(state)
            row=next(iter(trade['funding_scan']['rows'].values()))
            row['signed_quantity' if change=='quantity' else 'at_ms']='2' if change=='quantity' else BASE
            accounting.settle(state,BASE+5000)
            self.assertEqual(trade['accounting_pending'],'FORMULA_FUNDING_OWNERSHIP_AMBIGUOUS')

    def test_funding_empty_page_multiday_progress_and_retry(self):
        trade=completed(self.state)
        next(iter(trade['exit_fills'].values()))['at_ms']=BASE+3*accounting.DAY_MS
        next(f for f in trade['fee_facts'].values() if f['oid']=='2')['at_ms']=BASE+3*accounting.DAY_MS
        now=BASE+4*accounting.DAY_MS
        task=accounting.funding_task(self.state,now)
        accounting.record_funding(self.state,task,None,now,error='READ_PENDING')
        self.assertIsNone(accounting.funding_task(self.state,now+59999))
        pages=0
        while (task:=accounting.funding_task(self.state,now+60000)) is not None:
            self.assertLessEqual(task['end']-task['start'],accounting.DAY_MS)
            accounting.record_funding(self.state,task,accounting.funding_page(task,[]),now+60000)
            pages+=1
        self.assertEqual(pages,3)
        accounting.settle(self.state,now+60000)
        self.assertEqual(trade['settlement']['funding_usd'],'0')

    def test_full_funding_page_repeats_boundary_and_deduplicates(self):
        completed(self.state)
        task=accounting.funding_task(self.state,BASE+5000)
        raw=[dict(time=BASE+100+i, hash='0x'+format(i,'064x'),
            delta=dict(type='funding',coin='HYPE',szi='1',usdc='0.001')) for i in range(500)]
        page=accounting.funding_page(task,raw)
        self.assertFalse(page['complete']);self.assertEqual(page['next_ms'],BASE+599)
        accounting.record_funding(self.state,task,page,BASE+5000)
        next_task=accounting.funding_task(self.state,BASE+5001)
        accounting.record_funding(self.state,next_task,accounting.funding_page(next_task,[raw[-1]]),BASE+5001)
        accounting.settle(self.state,BASE+5002)
        self.assertEqual(Decimal(self.state['trades']['a'*64]['settlement']['funding_usd']),Decimal('.5'))
        with self.assertRaisesRegex(accounting.AccountingError,'PAGE_INCOMPLETE'):
            accounting.funding_page(next_task,[raw[-1]]*500)

    def test_funding_response_bound_to_exact_trade_revision(self):
        trade=completed(self.state);task=accounting.funding_task(self.state,BASE+5000)
        trade['manual_management']=dict(status='HUMAN_MANAGED')
        self.assertFalse(accounting.record_funding(self.state,task,accounting.funding_page(task,[]),BASE+5000))
        self.assertNotIn('funding_scan',trade)

    def test_risk_snapshot_rejects_wrong_formula_arithmetic_and_corrupt_balance(self):
        risk=accounting.risk_for(self.state,dict(rule_id='A'))
        for key,value in (('formula','B'),('planned_usd','20'),('fraction','0.5'),('revision',True)):
            bad={**risk,key:value}
            with self.assertRaises(accounting.AccountingError):accounting.validate_risk(bad,'A')
        self.state['formula_accounting']['portfolios']['A']['balance_usd']='100'
        with self.assertRaisesRegex(accounting.AccountingError,'INTEGRITY'):
            accounting.risk_for(self.state,dict(rule_id='A'))


class AccountingRuntimeContract:
    setUp_runtime = RuntimeContract.setUp
    make_worker = RuntimeContract.make_worker
    cycle = RuntimeContract.cycle

    def setUp(self):
        self.setUp_runtime()
        self.venue.t=BASE+1000
        self.make_worker(BASE-60000)
        self.funding=[];self.funding_calls=[];self.costs={};self.omit_cost=False
        self.provider.collect=self.collect
        self.provider.collect_funding=self.collect_funding

    def collect(self,state,**kwargs):
        from .test_experimental_dispatch_integration import legacy_view
        context=self.venue.collect(state);context['collector_checkpoints']={}
        for snapshot in context['snapshots']:
            trade=next((t for t in state['trades'].values() if t['account']==snapshot['account']
                        and t['symbol']==snapshot['symbol']),None)
            if trade is None:continue
            _,view=legacy_view(trade,snapshot)
            for fill in view['fills']:fill['fee']=self.costs.get(fill['fill_id'],'0.1')
            lane=life.digest([snapshot['account'],snapshot['symbol']])
            context['collector_checkpoints'][lane]=view
        return context

    def collect_funding(self,task):
        self.funding_calls.append(deepcopy(task))
        if isinstance(self.funding,Exception):raise self.funding
        return deepcopy(self.funding)

    def start(self,family='hype_row71205'):
        # Fixture decisions retain their existing half-hour alignment.
        stamp=(self.venue.t//1800000+1)*1800000+(120000 if family=='sol_g65' else 60000)
        self.venue.t=stamp+1000
        msg=formula_alert(family,approved_ms=stamp)
        account=self.store.load()['routes']['short_account']
        self.venue.mark[self.venue._key(account,msg['symbol'])]=msg['entry']
        self.worker.receive([msg]);self.assertEqual(self.cycle()['operation'],'ENTRY')
        return msg,self.venue.oid('ENTRY')

    def close(self,msg,entry_oid,leg='TAKE_PROFIT'):
        quantity=self.venue.orders[entry_oid]['view']['wire_order']['s']
        self.venue.fill(entry_oid,quantity)
        for _ in range(3):self.cycle(False)
        self.venue.fill(self.venue.oid(leg),quantity)
        for _ in range(3):self.cycle(False)
        return self.store.load()['trades'][msg['occurrence_id']]

    def test_real_worker_profit_next_size_frozen_exits_and_restart(self):
        msg,oid=self.start();initial_request=deepcopy(self.venue.requests[0])
        trade=self.close(msg,oid)
        self.assertIn('settlement',trade)
        self.assertEqual(len(self.funding_calls),1)
        frozen=deepcopy(self.store.load()['requests'][initial_request['request_id']]['proposal'])
        balance=Decimal(trade['settlement']['balance_after_usd'])
        RuntimeContract.restart(self)
        self.cycle(False);self.worker.receive([msg]);self.cycle(False)
        self.assertEqual(len(self.funding_calls),1)
        msg2,oid2=self.start()
        new=self.store.load()['trades'][msg2['occurrence_id']]
        self.assertEqual(Decimal(new['entry_risk']['planned_usd']),balance*Decimal('.05'))
        self.assertEqual(new['source']['entry'],msg2['entry'])
        self.assertEqual(new['source']['stop'],msg2['stop'])
        self.assertGreater(Decimal(new['quantity']),Decimal(trade['quantity']))
        self.assertEqual(self.store.load()['requests'][initial_request['request_id']]['proposal'],frozen)
        self.assertTrue(all(r['proposal']['action']['orders'][0]['r'] for r in self.venue.requests
            if r['proposal']['operation']=='CREATE_EXIT'))

    def test_funding_outage_blocks_only_own_formula_and_does_not_repeat_each_cycle(self):
        msg,oid=self.start();self.funding=OSError('UNAVAILABLE')
        trade=self.close(msg,oid)
        self.assertNotIn('settlement',trade)
        calls=len(self.funding_calls)
        for _ in range(3):self.cycle(False)
        self.assertEqual(len(self.funding_calls),calls)
        # Another formula still enters and its observed fills receive exits.
        sol,sol_oid=self.start('sol_g65')
        self.venue.fill(sol_oid,self.venue.orders[sol_oid]['view']['wire_order']['s'])
        self.assertEqual(self.cycle(False)['operation'],'CREATE_EXIT')
        self.assertEqual(self.cycle(False)['operation'],'CREATE_EXIT')
        self.assertNotIn('settlement',self.store.load()['trades'][msg['occurrence_id']])

    def test_stop_loss_reduces_next_quantity_without_moving_source_prices(self):
        msg,oid=self.start();trade=self.close(msg,oid,leg='STOP')
        self.assertEqual(Decimal(trade['settlement']['net_usd']),Decimal('-2.2'))
        msg2,oid2=self.start()
        next_trade=self.store.load()['trades'][msg2['occurrence_id']]
        self.assertEqual(Decimal(next_trade['entry_risk']['planned_usd']),Decimal('1.89'))
        self.assertEqual(Decimal(next_trade['quantity']),Decimal('3.78'))
        self.assertEqual(next_trade['prices'],trade['prices'])

    def test_actual_collected_funding_is_included_once_after_exit(self):
        msg,oid=self.start();quantity=self.venue.orders[oid]['view']['wire_order']['s']
        self.venue.fill(oid,quantity)
        entry_time=self.venue.t
        self.funding=[dict(time=entry_time+500,hash='0x'+'f'*64,
            delta=dict(type='funding',coin=msg['symbol'],szi='-'+quantity,usdc='0.25'))]
        for _ in range(3):self.cycle(False)
        self.venue.t+=1000
        self.venue.fill(self.venue.oid('TAKE_PROFIT'),quantity)
        for _ in range(4):self.cycle(False)
        trade=self.store.load()['trades'][msg['occurrence_id']]
        self.assertEqual(Decimal(trade['settlement']['net_usd']),Decimal('8.05'))
        self.assertEqual(len(self.funding_calls),1)

    def test_partial_entry_and_exit_fees_settle_only_after_final_cleanup(self):
        msg,oid=self.start();quantity=Decimal(self.venue.orders[oid]['view']['wire_order']['s'])
        half=(quantity/2).quantize(Decimal('.01'),rounding='ROUND_DOWN')
        self.venue.fill(oid,str(half));self.cycle(False);self.cycle(False)
        self.venue.fill(oid,str(quantity-half))
        for _ in range(4):self.cycle(False)
        take=self.venue.oid('TAKE_PROFIT')
        self.venue.fill(take,str(half));self.cycle(False)
        self.assertFalse(self.funding_calls)
        self.assertEqual(self.store.load()['formula_accounting']['portfolios'][msg['rule_id']]['balance_usd'],'40')
        self.venue.fill(take,str(quantity-half))
        for _ in range(6):self.cycle(False)
        trade=self.store.load()['trades'][msg['occurrence_id']]
        self.assertEqual(len(trade['fee_facts']),4)
        self.assertEqual(Decimal(trade['settlement']['fees_usd']),Decimal('.4'))
        self.assertEqual(len(self.funding_calls),1)

    def test_concurrent_settlement_uses_existing_transaction_and_is_once_only(self):
        msg,oid=self.start();self.funding=OSError('DEFER')
        trade=self.close(msg,oid);cid=msg['occurrence_id']
        def ready(state):
            state['trades'][cid]['funding_scan']=dict(rows={},complete=True)
        self.store.mutate(ready)
        def settle(_):self.store.mutate(lambda state:accounting.settle(state,self.venue.now()))
        with ThreadPoolExecutor(max_workers=4) as pool:list(pool.map(settle,range(8)))
        state=self.store.load();receipt=state['trades'][cid]['settlement']
        self.assertEqual(state['formula_accounting']['portfolios'][msg['rule_id']]['revision'],1)
        self.assertEqual(Decimal(receipt['balance_after_usd']),40+Decimal(receipt['net_usd']))

    def test_unsettled_trade_cannot_archive(self):
        msg,oid=self.start();self.funding=OSError('DEFER')
        self.close(msg,oid);state=self.store.load();cid=msg['occurrence_id']
        self.assertFalse(archive._eligible(state,cid,self.venue.now()))
        state['trades'][cid]['funding_scan']=dict(rows={},complete=True)
        accounting.settle(state,self.venue.now())
        self.assertTrue(archive._eligible(state,cid,self.venue.now()))

    def test_small_balance_is_local_refusal_not_cycle_failure(self):
        formula=formula_alert('hype_row71205',approved_ms=BASE)['rule_id']
        def tiny(state):
            accounting.risk_for(state,dict(rule_id=formula))
            state['formula_accounting']['portfolios'][formula].update(balance_usd='0.00001',realized_net_usd='-39.99999')
        self.store.mutate(tiny)
        msg=formula_alert('hype_row71205',approved_ms=BASE)
        self.worker.receive([msg]);self.cycle()
        self.assertFalse(self.venue.requests)
        self.assertEqual(self.store.load()['entry_decisions'][msg['occurrence_id']]['reason'],'OUTSIDE_LAB_SIZE_BOUNDS')


class MemoryAccountingRuntimeTests(AccountingRuntimeContract,unittest.TestCase):
    real_pg=False


@unittest.skipUnless(os.environ.get('HL_JOURNAL_CI_URL'),'disposable loopback PostgreSQL required')
class PostgresAccountingRuntimeTests(AccountingRuntimeContract,unittest.TestCase):
    real_pg=True

    def test_sql_rollback_and_lost_result_cannot_separate_receipt_from_balance(self):
        msg,oid=self.start();self.funding=OSError('DEFER');self.close(msg,oid)
        cid=msg['occurrence_id']
        self.store.mutate(lambda state:state['trades'][cid].update(funding_scan=dict(rows={},complete=True)))
        before=self.store.load()
        def fail_after_settle(state):
            accounting.settle(state,self.venue.now())
            raise ValueError('SIMULATED_TRANSACTION_ROLLBACK')
        with self.assertRaises(ValueError):self.store.mutate(fail_after_settle)
        self.assertEqual(self.store.load(),before)
        # The caller loses the result after a successful commit, then restarts.
        self.store.mutate(lambda state:accounting.settle(state,self.venue.now()))
        self.store=TestnetExecutionState.for_ci(self.journal)
        self.store.mutate(lambda state:accounting.settle(state,self.venue.now()))
        state=self.store.load()
        self.assertEqual(state['formula_accounting']['portfolios'][msg['rule_id']]['revision'],1)
        self.assertIn('settlement',state['trades'][cid])

    def test_archive_restart_retains_capital_and_receipt_without_reentering(self):
        msg,oid=self.start();trade=self.close(msg,oid)
        before=deepcopy(self.store.load()['formula_accounting'])
        self.store.initialize_history()
        self.store.compact_history(now_ms=self.venue.now(),force=True)
        self.assertNotIn(msg['occurrence_id'],self.store.load()['trades'])
        self.store=TestnetExecutionState.for_ci(self.journal)
        RuntimeContract.restart(self)
        self.worker.receive([msg])
        self.assertEqual(self.store.load()['formula_accounting'],before)
        rows=self.worker.history_report()['trades']
        self.assertEqual(rows[0]['net_pnl'],trade['settlement']['net_usd'])
        self.assertNotIn(msg['occurrence_id'],self.store.load()['trades'])


class DynamicDispatchTests(unittest.TestCase):
    def setUp(self):
        self.request,self.context,self.now=approved_fixture()
        self.source=self.context['source']
        state=initial(LIVE_ROUTES,BASE-60000)
        risk=accounting.risk_for(state,self.source)
        risk.update(balance_usd='80',planned_usd='4',revision=1)
        self.request['proposal']['entry_risk']=deepcopy(risk)
        self.context['entry_risk']=deepcopy(risk)
        self.request['proposal']['quantity']='1.5'
        self.request['proposal']['action']['orders'][0]['s']='1.5'

    def test_above_two_requires_matching_durable_risk_and_still_obeys_five_percent(self):
        boundary.review(self.request,self.context,now_ms=self.now)
        self.request['proposal']['quantity']='2'
        self.request['proposal']['action']['orders'][0]['s']='2'
        with self.assertRaisesRegex(ValueError,'EXCEEDS_CURRENT_RISK'):
            boundary.review(self.request,self.context,now_ms=self.now)

    def test_caller_cannot_remove_or_increase_snapshot(self):
        for change in ('remove','increase','formula'):
            request=deepcopy(self.request)
            if change=='remove':request['proposal'].pop('entry_risk')
            elif change=='increase':request['proposal']['entry_risk'].update(balance_usd='800',planned_usd='40')
            else:request['proposal']['entry_risk']['formula']='OTHER'
            with self.assertRaisesRegex(ValueError,'DURABLE_FORMULA_RISK_MISMATCH'):
                boundary.review(request,self.context,now_ms=self.now)

    def test_legacy_entry_cannot_use_historical_or_unbound_higher_budget(self):
        self.request['proposal'].pop('entry_risk');self.context.pop('entry_risk')
        with self.assertRaisesRegex(ValueError,'EXCEEDS_CURRENT_RISK'):
            boundary.review(self.request,self.context,now_ms=self.now)


class DynamicProviderTests(unittest.TestCase):
    setUp=provider_support.ProviderTests.setUp
    approved=provider_support.ProviderTests.approved

    def test_final_collector_keeps_committed_risk_when_another_trade_settles(self):
        msg=self.approved();test=self
        accounting.risk_for(self.state,msg)
        portfolio=self.state['formula_accounting']['portfolios'][msg['rule_id']]
        portfolio.update(balance_usd='80',realized_net_usd='40',revision=1)
        class Safety:
            def verify_checkpoint(self,*,state,request,collected_at_ms,ownership_revision):
                p=request['proposal']
                return dict(account=p['account'],role=p['role'],at_ms=collected_at_ms,
                    entry_enabled=True,emergency_healthy=True,feed_reconciled=True,
                    entry_circuit_clear=True,supervisor_at_ms=collected_at_ms,
                    not_before_ms=test.state['not_before_ms'])
        self.provider.safety=Safety()
        context=self.provider.collect(self.state,entries_enabled=True)
        planner=object.__new__(live.TestnetExecutionRuntime)
        proposal=planner._cycle_proposal(self.state,context,self.exchange.t,entries_enabled=True)
        request=planner._reserve_live(self.state,proposal,self.exchange.t,self.exchange.t)
        self.assertEqual(Decimal(proposal['entry_risk']['planned_usd']),4)
        calls=len(self.raw.calls)
        # A later realized result must not rewrite an already prepared order.
        self.state['formula_accounting']['portfolios'][msg['rule_id']].update(
            balance_usd='60',realized_net_usd='20',revision=2)
        final=self.provider.dispatch_context(request)
        checked=boundary.review(request,final,now_ms=self.exchange.t)
        self.assertEqual(checked['action'],proposal['action'])
        self.assertEqual(len(self.raw.calls),calls)
        request['proposal']['entry_risk'].update(balance_usd='800',planned_usd='40')
        with self.assertRaisesRegex(ValueError,'FINAL_SOURCE_ENTRY_PROPOSAL_CHANGED'):
            self.provider.dispatch_context(request)

    def test_funding_transport_uses_existing_shared_background_budget_once(self):
        from .card_sync_evidence import PublicReader, HOST
        budget=Mock()
        with patch('http.client.HTTPSConnection') as connection:
            response=connection.return_value.getresponse.return_value
            response.status=200;response.read.return_value=b'[]'
            reader=PublicReader(budget=budget,priority='background')
            account=self.state['routes']['long_account']
            self.assertEqual(reader.read('userFunding',account,start=BASE,end=BASE+1000),[])
            body=dict(type='userFunding',user=account,startTime=BASE,endTime=BASE+1000)
            budget.acquire.assert_called_once_with('/info',body,priority='background',host=HOST)
            budget.acquire.return_value.check.assert_called_once()
            budget.acquire.return_value.finish.assert_called_once_with([])
            connection.return_value.request.assert_called_once()
