"""Nov 21 incident acceptance through the real PostgreSQL execution manager.

The existing raw exchange/HTTPS/signing fixtures are the only venue boundary.
The journal, request budget, intake, provider, transitions and final sender are
product code. Every controlled read advances the observation clock; real CPU,
wall time and RSS are reported separately from that controlled elapsed time.
"""
from copy import deepcopy
from decimal import Decimal
import json
import os
from pathlib import Path
import resource
import time
from types import MethodType
import unittest
from unittest.mock import patch

import approved_alert_contract as contract
from approved_alert_fixtures import selected_formula_alert
from . import experimental_live_state as live
from . import experimental_live_release as releases
from . import experimental_execution_runtime as core
from . import experimental_shared_market as shared
from . import position_recovery as recovery
from . import request_budget
from . import fill_wakeups as wake
from . import test_experimental_live_service as connection
from .experimental_live_provider import LiveEvidenceProvider
from .filled_dispatch_store import DispatchStore, SCHEMA as DISPATCH_SCHEMA
from .postgres_journal import PostgresJournal
from .test_experimental_live_provider import LegacyFixture
from .test_experimental_live_state import LIVE_ROUTES

CI = os.environ.get('HL_JOURNAL_CI_URL')
SOL_CID = 'a449231f4b2e6f02034e7e514fbde34751c97cdd135f3a9923b4af2f14dbd2ee'
XRP_CID = 'd356b1064f4688182030971aab5d0e78670e315fc4c46c91598408742518c364'
SOL_OIDS = ('612268082509', '612268286101', '612268333021')
REFERENCE = 1795239480000
APPROVED = 1795239492508
FROZEN = json.loads((Path(__file__).parent/'fixtures/sol_xrp_recovery_20261121.json').read_text())


@unittest.skipUnless(CI, 'Disposable loopback PostgreSQL is required; skips are not evidence')
class RecoveryIsolationPostgresTests(unittest.TestCase):
    metrics = []

    def setUp(self):
        self.fx = connection.FullConnectionTests(methodName='runTest')
        self.read_latency_ms = 75
        self.read_failures = {}
        self.truncate_fills = False
        self.read_metrics = []
        self.cycle_metrics = []
        self._test_cpu = time.process_time()
        self._test_wall = time.perf_counter()
        owner = self

        def build(fx, not_before):
            connection.FullConnectionTests.build(fx, not_before)
            journal = PostgresJournal.for_ci(CI)
            journal.bootstrap(); DispatchStore(journal).initialize()
            budget = request_budget.Budget(journal); budget.initialize()
            with journal._transaction() as conn:
                conn.execute(f'DROP SCHEMA IF EXISTS {live.PG_SCHEMA} CASCADE')
                conn.execute(f'DELETE FROM {DISPATCH_SCHEMA}.nonces WHERE agent=ANY(%s)',
                    ([r['agent'] for r in LIVE_ROUTES.values()],))
                conn.execute(f'DELETE FROM {request_budget.TICKETS}')
            fx.store = live.TestnetExecutionState.for_ci(journal)
            fx.store.initialize(LIVE_ROUTES, not_before_ms=REFERENCE-86400000)
            fx.store.initialize_history()
            fx.oracle.t = APPROVED+1000
            fx.release.update(not_before_ms=REFERENCE-86400000,
                entry_policy=releases.CONTINUOUS_ENTRY, entry_expires_at_ms=None)
            fx.budget = budget
            fx.raw.budget = budget
            fx.raw.state_loader = fx.store.load
            original_read, original_lookup = fx.raw.read, fx.raw.lookup

            def observed(kind, call):
                body = dict(type='orderStatus' if kind=='lookup' else kind)
                permit = budget.acquire('/info', body, priority='protection')
                permit.check()
                start = fx.oracle.now(); cpu = time.process_time()
                fx.oracle.t += owner.read_latency_ms
                row = dict(kind=kind, at_ms=start,
                    reserved_weight=request_budget.request_weight('/info', body), success=False)
                owner.read_metrics.append(row)
                if owner.read_failures.get(kind, 0):
                    owner.read_failures[kind] -= 1
                    row['elapsed_ms'] = fx.oracle.now()-start
                    row['cpu_ms'] = (time.process_time()-cpu)*1000
                    raise ValueError('PUBLIC_READ_UNAVAILABLE')
                value = call()
                permit.finish(value)
                row.update(success=True, elapsed_ms=fx.oracle.now()-start,
                    cpu_ms=(time.process_time()-cpu)*1000,
                    response_weight=(20+(len(value)+19)//20 if body['type'] in request_budget.SIZED
                        and isinstance(value,list) else row['reserved_weight']))
                return value

            def raw_read(kind,*args,**kwargs):
                def response():
                    value=original_read(kind,*args,**kwargs)
                    if kind=='userFillsByTime':
                        for row in value:
                            frozen=next((f for f in FROZEN['sol_fills'] if f['oid']==str(row['oid'])),None)
                            if frozen:
                                row.update(tid=int(frozen['fill_id'].split(':')[1]),
                                    fee=frozen['fee'],feeToken=frozen['fee_token'])
                        if owner.truncate_fills:
                            row=dict(coin='SOL',oid=int(SOL_OIDS[0]),sz='0.001',px='65.469',
                                fee='0',feeToken='USDC',side='A',time=kwargs['end'])
                            value=[{**row,'tid':900000+i} for i in range(2000)]
                    return value
                try:return observed(kind,response)
                finally:
                    if owner.read_metrics:
                        owner.read_metrics[-1]['query']={k:v for k,v in kwargs.items() if k in ('start','end','oid')}
            def raw_lookup(account,cloid):
                return observed('lookup',lambda:original_lookup(account,cloid))
            fx.raw.read,fx.raw.lookup=raw_read,raw_lookup
            fx.provider=LiveEvidenceProvider(fx.env,legacy_store=LegacyFixture(),experimental_store=fx.store,
                price_evidence=connection.SyntheticPrices(fx.oracle,fx.store),budget=budget,clock=fx.oracle.now,
                info_reader=fx.raw,public_reader=fx.raw,lookup_reader=fx.raw.lookup,
                safety_provider=connection.SyntheticSafety(fx.release))
            fx.reconnect()
            send=fx.oracle.send
            def send_with_recorded_order_ids(request,*,admission):
                value=send(request,admission=admission)
                if request['proposal']['symbol']=='SOL' and request['proposal']['operation'] in ('ENTRY','CREATE_EXIT'):
                    index={'ENTRY':0,'STOP':1,'TAKE_PROFIT':2}[request['proposal']['leg']]
                    old=str(value['oid']);new=SOL_OIDS[index]
                    item=fx.oracle.orders.pop(old);item['view']['oid']=new
                    fx.oracle.orders[new]=item;value['oid']=new
                return value
            fx.oracle.send=send_with_recorded_order_ids

        self.fx.build=MethodType(build,self.fx)
        self.addCleanup(self.fx.doCleanups);self.fx.setUp()
        self.account=self.fx.store.load()['routes']['short_account']

    def tearDown(self):
        self.metrics.append(dict(test=self.id().split('.')[-1],cycles=self.cycle_metrics,
            attempted_reads=len(self.read_metrics),successful_reads=sum(x['success'] for x in self.read_metrics),
            conservative_attempt_weight=sum(x['reserved_weight'] for x in self.read_metrics),
            confirmed_response_weight=sum(x.get('response_weight',0) for x in self.read_metrics),
            attempted_actions=len(self.fx.http),
            attempted_action_weight=sum(request_budget.request_weight('/exchange',row) for row in self.fx.http),
            cpu_ms=(time.process_time()-self._test_cpu)*1000,
            wall_ms=(time.perf_counter()-self._test_wall)*1000,
            peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))

    @classmethod
    def tearDownClass(cls):
        result=dict(version='recovery-isolation-postgres-v1',production_requests=0,
            clock='Advances 75ms per controlled read (20s in the slow-provider test) plus explicit cycle/budget waits; CPU/wall are actual local runtime.',
            policy=dict(maximum=request_budget.LIMIT,background=request_budget.BACKGROUND_LIMIT,
                        window_ms=request_budget.WINDOW_MS),tests=cls.metrics)
        target=os.environ.get('RECOVERY_ACCEPTANCE_METRICS')
        if target:Path(target).write_text(json.dumps(result,indent=2)+'\n')
        print('RECOVERY_ACCEPTANCE_METRICS='+json.dumps(result,separators=(',',':')))

    def tick(self,wait_ms=0):
        self.fx.oracle.t+=wait_ms
        started=self.fx.oracle.now();cpu=time.process_time();wall=time.perf_counter();reads=len(self.read_metrics)
        try:return self.fx.service.tick()
        finally:
            calls=self.read_metrics[reads:]
            self.cycle_metrics.append(dict(started_ms=started,elapsed_ms=self.fx.oracle.now()-started,
                reads=len(calls),attempt_weight=sum(row['reserved_weight'] for row in calls),
                confirmed_response_weight=sum(row.get('response_weight',0) for row in calls),
                cpu_ms=(time.process_time()-cpu)*1000,wall_ms=(time.perf_counter()-wall)*1000))

    def signal(self,family,*,cycle='post-recovery',approved_ms=None):
        approved_ms=self.fx.oracle.now() if approved_ms is None else approved_ms
        msg=selected_formula_alert(family,reference_ms=approved_ms//60000*60000,
            approved_ms=approved_ms,cycle=cycle)
        if family=='sol_sff1361':
            p=Decimal('65.469')
            msg.update(entry=str(p),stop=str(p*Decimal('1.01')),take_profit=str(p*Decimal('.98')))
            msg['proof']['reference_price']=str(p)
            msg['occurrence_id']=contract.occurrence_id(msg)
            contract.validate(msg)
        return msg

    def protect_sol(self):
        msg=deepcopy(FROZEN['sol_source']);contract.validate(msg)
        self.assertEqual(msg['occurrence_id'],SOL_CID)
        self.fx.seed(msg);self.assertEqual(self.tick().get('operation'),'ENTRY')
        oid=self.fx.oracle.oid('ENTRY');q=self.fx.oracle.orders[oid]['view']['wire_order']['s']
        self.assertEqual(Decimal(q),Decimal('3.05'))
        self.fx.oracle.t=FROZEN['sol_fills'][0]['at_ms']-1
        self.fx.oracle.fill(oid,q,'65.469')
        for _ in range(4):self.tick(250)
        state=self.fx.store.load();self.cid=msg['occurrence_id']
        self.assertEqual(set(state['trades'][self.cid]['order_legs']),set(SOL_OIDS))
        self.assertTrue(all(r['phase']=='OBSERVED' for r in state['requests'].values()))
        return msg

    def waiting(self):
        self.fx.oracle.t=max(self.fx.oracle.t,FROZEN['recovery']['last_complete_checkpoint_ms'])
        self.tick()
        self.fx.oracle.t=FROZEN['recovery']['finished_ms']
        def apply(state):
            row=recovery._start(state,state['trades'][self.cid],self.fx.oracle.now())
            row.update(read_calls=recovery.READ_CALLS,read_weight=recovery.READ_WEIGHT)
            recovery._finish(state,row,'WAITING',self.fx.oracle.now(),'INQUIRY_LIMIT')
        self.fx.store.mutate(apply)
        return deepcopy(self.fx.store.load()['position_recovery'][self.cid])

    def restart(self):
        self.fx.store=live.TestnetExecutionState.for_ci(PostgresJournal.for_ci(CI))
        self.fx.provider.experimental_store=self.fx.store
        self.fx.raw.state_loader=self.fx.store.load
        self.fx.reconnect()

    def stream_stop_fill(self):
        """Existing websocket ingestion commits a fill without inventing status."""
        feed=wake.FillWakeups(self.fx.store.load()['routes'],clock=lambda:self.fx.oracle.now()/1000)
        self.fx.provider.safety.feed=feed
        for account in self.fx.store.load()['routes'].values():
            generation=feed._opened(account)
            for channel in wake.CHANNELS:
                feed._receive(account,generation,json.dumps(dict(channel='subscriptionResponse',data=dict(
                    method='subscribe',subscription=dict(type=channel,user=account)))))
            feed._receive(account,generation,json.dumps(dict(channel='userFills',data=dict(
                user=account,isSnapshot=True,fills=[]))))
            self.assertTrue(feed.finish_reconciliation(feed.begin_reconciliation(account),complete=True))
        fill=FROZEN['sol_fills'][1]
        self.fx.oracle.t=fill['at_ms']-1
        self.fx.oracle.fill(fill['oid'],fill['quantity'],fill['price'])
        row=dict(coin='SOL',tid=int(fill['fill_id'].split(':')[1]),oid=int(fill['oid']),
            sz=fill['quantity'],px=fill['price'],fee=fill['fee'],feeToken=fill['fee_token'],
            side=fill['side'],time=fill['at_ms'])
        generation=feed._states[self.account]['generation']
        self.assertTrue(feed._receive(self.account,generation,json.dumps(dict(channel='userFills',data=dict(
            user=self.account,isSnapshot=False,fills=[row])))))
        self.fx.worker._ingest_pending_fills()
        state=self.fx.store.load();trade=state['trades'][self.cid]
        self.assertEqual(trade['orders'][SOL_OIDS[1]]['status'],'OPEN')
        self.assertEqual(core._remaining(trade),Decimal(0))
        self.assertEqual(list(trade['exit_fills']),[fill['fill_id']])
        return feed,row

    def close_tp_at_venue(self):
        self.fx.oracle.orders[SOL_OIDS[2]]['view'].update(status='CANCELED',at_ms=self.fx.oracle.now())

    def test_waiting_healthy_exits_resolve_without_action_or_reset(self):
        self.protect_sol();before=self.waiting();sent=len(self.fx.http)
        self.tick(5000)
        row=self.fx.store.load()['position_recovery'][self.cid]
        self.assertEqual((row['phase'],row['reason']),('RESOLVED','EXITS_PRESENT'))
        for k in ('round','read_calls','read_weight','sends'):self.assertEqual(row[k],before[k])
        self.assertEqual(len(self.fx.http),sent)
        self.restart();self.tick(5000)
        self.assertEqual(len(self.fx.http),sent)

    def test_temporary_missing_observation_stays_waiting_then_resolves(self):
        self.protect_sol();before=self.waiting();sent=len(self.fx.http)
        self.read_failures['frontendOpenOrders']=1
        self.tick(5000)
        self.assertEqual(self.fx.store.load()['position_recovery'][self.cid]['phase'],'WAITING')
        self.tick(5000)
        row=self.fx.store.load()['position_recovery'][self.cid]
        self.assertEqual(row['phase'],'RESOLVED')
        self.assertEqual(row['read_weight'],before['read_weight'])
        self.assertEqual(len(self.fx.http),sent)

    def test_late_stop_fill_active_tp_blocks_then_terminal_proof_closes_once(self):
        self.protect_sol();before=self.waiting();sent=len(self.fx.http)
        feed,row=self.stream_stop_fill()
        self.tick(5000)
        state=self.fx.store.load()
        self.assertEqual(state['position_recovery'][self.cid]['phase'],'WAITING')
        self.assertEqual(state['trades'][self.cid]['orders'][SOL_OIDS[1]]['status'],'FILLED')
        self.assertEqual(state['trades'][self.cid]['orders'][SOL_OIDS[2]]['status'],'OPEN')
        self.assertNotEqual(state['trades'][self.cid]['phase'],'CLOSED')
        self.assertEqual(len(self.fx.http),sent)
        self.close_tp_at_venue();self.tick(5000)
        state=self.fx.store.load();trade=state['trades'][self.cid]
        self.assertEqual(trade['phase'],'CLOSED')
        self.assertEqual(state['position_recovery'][self.cid]['phase'],'RESOLVED')
        self.assertEqual(state['position_recovery'][self.cid]['read_calls'],before['read_calls'])
        self.assertEqual(sum(Decimal(x['fee']) for x in trade['fee_facts'].values()),Decimal('.120795'))
        self.assertEqual(Decimal(trade['settlement']['fees_usd']),Decimal('.120795'))
        accounting=deepcopy(state['formula_accounting'])
        self.restart();self.tick(5000)
        again=self.fx.store.load()
        self.assertEqual(again['formula_accounting'],accounting)
        self.assertEqual(again['trades'][self.cid]['exit_fills'],trade['exit_fills'])
        self.assertEqual(len(self.fx.http),sent)

    def test_open_fully_filled_sol_stop_does_not_abort_exact_xrp_signal(self):
        self.protect_sol();self.waiting();self.stream_stop_fill()
        msg=deepcopy(FROZEN['xrp_source']);self.assertEqual(msg['occurrence_id'],XRP_CID)
        self.fx.oracle.t=contract.moment_ms(msg['approved_at'])+1000
        self.fx.seed(msg);received=self.fx.oracle.now()
        # A failed SOL status proof must not erase its known fills or stop XRP.
        self.read_failures['orderStatus']=1
        result=self.tick()
        state=self.fx.store.load()
        self.assertEqual(result.get('operation'),'ENTRY',result)
        self.assertEqual(result.get('card_id',result.get('occurrence_id',XRP_CID)),XRP_CID)
        self.assertIn(XRP_CID,state['trades'])
        self.assertEqual(state['trades'][self.cid]['orders'][SOL_OIDS[1]]['status'],'OPEN')
        self.assertEqual(state['position_recovery'][self.cid]['phase'],'WAITING')
        lane=shared.assess(state)['lanes'][core._lane(self.account,'SOL')]
        self.assertEqual(lane['active_occurrences'],1)
        self.assertIsNotNone(lane['new_shared_entry_blocked_reason'])
        self.cycle_metrics[-1]['intake_to_decision_ms']=self.fx.oracle.now()-received
        self.assertLess(self.fx.oracle.now()-received,90000)
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' and r['proposal']['symbol']=='XRP'
            for r in self.fx.oracle.requests),1)
        xrp_oid=next(oid for oid,item in self.fx.oracle.orders.items() if item['symbol']=='XRP' and item['leg']=='ENTRY')
        self.fx.oracle.fill(xrp_oid,state['trades'][XRP_CID]['quantity'])
        self.read_failures['orderStatus']=1
        result=self.tick(250)
        self.assertEqual((result.get('operation'),self.fx.oracle.requests[-1]['proposal']['leg']),('CREATE_EXIT','STOP'))
        self.assertEqual(self.fx.oracle.requests[-1]['proposal']['card_id'],XRP_CID)
        self.assertEqual(self.fx.store.load()['position_recovery'][self.cid]['phase'],'WAITING')

    def test_sql_rollback_records_durable_failure_then_restart_decides_once(self):
        msg=self.signal('xrp_liquidity_inverse');cid=msg['occurrence_id'];self.fx.seed(msg)
        original=self.fx.store.commit_attempt
        def rollback(transition,*,role,now_ms):
            def fail_after_preparation(state,nonce):
                transition(state,nonce)
                raise ValueError('EXACT_SHARED_REDUCING_CAPACITY_REQUIRED')
            return original(fail_after_preparation,role=role,now_ms=now_ms)
        with patch.object(self.fx.store,'commit_attempt',side_effect=rollback):
            with self.assertRaisesRegex(ValueError,'PERSISTENCE_UNAVAILABLE_NO_SEND'):self.tick()
        state=self.fx.store.load()
        self.assertEqual(state['last_cycle_failure']['reason'],'EXACT_SHARED_REDUCING_CAPACITY_REQUIRED')
        self.assertEqual(state['entry_decisions'][cid]['reason'],'EXACT_SHARED_REDUCING_CAPACITY_REQUIRED')
        self.assertFalse(state['requests']);self.assertFalse(self.fx.http)
        self.restart();self.assertEqual(self.tick(5000).get('operation'),'ENTRY')
        self.restart();self.tick(5000)
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in self.fx.oracle.requests),1)

    def test_lost_entry_reply_restart_reconciles_without_replay(self):
        msg=self.signal('xrp_liquidity_inverse');self.fx.seed(msg);self.fx.fail_reply=True
        result=self.tick();self.assertIn('UNKNOWN',result['status'])
        state=self.fx.store.load();self.assertEqual(len(state['requests']),1)
        self.restart()
        for _ in range(3):self.tick(250)
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in self.fx.oracle.requests),1)
        self.assertTrue(all(r['phase']=='OBSERVED' for r in self.fx.store.load()['requests'].values()))

    def test_waiting_unknown_request_does_not_gain_another_send_or_round(self):
        msg=self.signal('xrp_liquidity_inverse');cid=msg['occurrence_id'];self.fx.seed(msg)
        self.fx.fail_reply=True;self.assertIn('UNKNOWN',self.tick()['status'])
        def exhausted(state):
            row=recovery._start(state,state['trades'][cid],self.fx.oracle.now())
            row.update(read_calls=recovery.READ_CALLS,read_weight=recovery.READ_WEIGHT)
            recovery._finish(state,row,'WAITING',self.fx.oracle.now(),'INQUIRY_LIMIT')
        self.fx.store.mutate(exhausted)
        before=deepcopy(self.fx.store.load()['position_recovery'][cid])
        sent=len(self.fx.http);self.restart()
        for _ in range(2):self.tick(5000)
        state=self.fx.store.load()
        self.assertEqual(state['position_recovery'][cid]['phase'],'WAITING')
        self.assertEqual(len(self.fx.http),sent)
        self.assertTrue(any(r['phase'] not in ('OBSERVED','ABORTED_UNSENT') for r in state['requests'].values()))
        for key in ('round','read_calls','read_weight','sends'):
            self.assertEqual(state['position_recovery'][cid][key],before[key])

    def test_full_history_pages_stay_bounded_without_new_recovery_round(self):
        self.protect_sol();before=self.waiting();sent=len(self.fx.http)
        self.truncate_fills=True
        for _ in range(2):
            start=len(self.read_metrics);self.tick(5000)
            calls=self.read_metrics[start:]
            pages=[r for r in calls if r['kind']=='userFillsByTime']
            self.assertEqual(len(pages),1,calls)
            self.assertEqual(len({r['query']['end'] for r in pages}),1)
            self.assertLessEqual(len(calls),recovery.READ_CALLS)
            row=self.fx.store.load()['position_recovery'][self.cid]
            self.assertEqual(row['phase'],'WAITING')
            for key in ('round','read_calls','read_weight','sends'):self.assertEqual(row[key],before[key])
            self.assertEqual(self.fx.service._next_wait_seconds(5),5)
        self.truncate_fills=False;self.tick(5000)
        self.assertEqual(self.fx.store.load()['position_recovery'][self.cid]['phase'],'RESOLVED')
        self.assertEqual(len(self.fx.http),sent)

    def test_slow_current_read_expires_signal_and_archives_explicit_reason(self):
        msg=self.signal('xrp_liquidity_inverse');cid=msg['occurrence_id'];self.fx.seed(msg)
        self.read_latency_ms=20000
        try:self.tick()
        except ValueError:pass  # Exact existing freshness refusal is persisted by service.
        self.assertFalse(self.fx.http)
        self.assertEqual(self.fx.store.load()['last_cycle_failure']['reason'],
            'COMPLETE_FRESH_TWO_ACCOUNT_INVENTORY_REQUIRED')
        self.read_latency_ms=75
        self.fx.oracle.t=max(self.fx.oracle.now(),contract.moment_ms(msg['expires_at'])+1)
        self.restart();self.tick(5000)
        self.assertFalse(self.fx.http)
        self.fx.store.compact_history(now_ms=self.fx.oracle.now(),force=True)
        archived=self.fx.store.archive_record(cid)
        self.assertEqual(archived['source']['source'],msg)
        result=archived['source']['entry_result']
        self.assertEqual(result['outcome'],'NEVER_SUBMITTED')
        self.assertEqual(result['reason'],'ENTRY_WINDOW_EXPIRED')
        self.assertEqual(result['deadline'],msg['expires_at'])
        self.assertEqual(sum(e['kind']=='ENTRY_FINISHED' for e in archived['events']),1)

    def test_new_signal_transient_read_returns_inside_unchanged_acceptance_window(self):
        msg=self.signal('xrp_liquidity_inverse');cid=msg['occurrence_id'];self.fx.seed(msg)
        received=self.fx.oracle.now();self.read_failures['frontendOpenOrders']=1
        self.assertNotEqual(self.tick().get('operation'),'ENTRY')
        state=self.fx.store.load()
        self.assertIn(cid,state['entry_decisions']);self.assertFalse(self.fx.http)
        self.restart();self.assertEqual(self.tick(5000).get('operation'),'ENTRY')
        state=self.fx.store.load()
        self.assertEqual(state['sources'][cid]['source'],msg)
        self.assertEqual(next(iter(state['requests'].values()))['proposal']['source_expires_at'],msg['expires_at'])
        self.cycle_metrics[-1]['intake_to_decision_ms']=self.fx.oracle.now()-received
        self.assertLess(self.fx.oracle.now()-received,90000)
        self.assertEqual(self.fx.ingest(msg)['status'],'DUPLICATE')
        self.tick(5000)
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in self.fx.oracle.requests),1)

    def test_real_shared_budget_wait_resumes_without_refreshing_source(self):
        msg=self.signal('xrp_liquidity_inverse');cid=msg['occurrence_id'];self.fx.seed(msg)
        received=self.fx.oracle.now()
        # Controlled concurrent pages consume their permits at the fake HTTP
        # boundary, then report empty responses after this candidate waits.
        tickets=[self.fx.budget.acquire('/info',dict(type='userFillsByTime'),priority='protection') for _ in range(7)]
        for ticket in tickets:ticket.check()
        self.assertNotEqual(self.tick().get('operation'),'ENTRY')
        state=self.fx.store.load()
        self.assertEqual(state['entry_decisions'][cid]['reason'],'TESTNET_REQUEST_BUDGET_EXHAUSTED')
        self.assertFalse(self.fx.http);self.assertEqual(len(self.read_metrics),0)
        for ticket in tickets:ticket.finish([])
        retry=state['entry_decisions'][cid]['retry_after_ms']
        self.assertGreater(retry,self.fx.oracle.now())
        result=self.tick(retry-self.fx.oracle.now())
        self.assertEqual(result.get('operation'),'ENTRY',(result,self.fx.store.load().get('entry_decisions')))
        self.assertEqual(self.fx.store.load()['sources'][cid]['source'],msg)
        self.cycle_metrics[-1]['intake_to_decision_ms']=self.fx.oracle.now()-received
        self.cycle_metrics[-1]['other_reserved_pages']=7
        self.cycle_metrics[-1]['other_controlled_page_attempts']=7
        self.assertLess(self.fx.oracle.now()-received,90000)
