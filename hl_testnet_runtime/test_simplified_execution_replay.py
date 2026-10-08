"""Offline execution reduction replay; exchange, SQL and OS clock are doubled.
Actual collectors, source expiry, runtime, signing boundary, and lifecycle run unchanged.
Synthetic results never establish live Testnet success.
"""
from copy import deepcopy
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

import approved_alert_contract as contract
from approved_alert_fixtures import maxpain_alert
from . import request_budget as quota
from . import experimental_execution_runtime as core
from .card_sync_evidence import PublicReader
from . import test_experimental_live_safety as safety_fixture
from .test_experimental_execution_runtime import T


class RollingBudget(quota.Budget):
    """In-memory SQL boundary; use production finite-plan and permit classes."""
    def __init__(self, clock):
        self.clock = clock
        self.tickets = {}
        self.admissions = []
        self.denials = []
        self.claims = []
        self.releases = []
        self.plan_weights = []
        self.capacity_queries = []

    def used(self):
        return sum(row['weight'] for row in self.tickets.values()
                   if row['at_ms'] > self.clock() - quota.WINDOW_MS)

    def seed(self, at_ms, weight):
        assert at_ms <= self.clock() and 0 < weight <= quota.LIMIT
        self.tickets[uuid.uuid4().hex] = dict(at_ms=at_ms, weight=weight, claimed=True)

    def capacity(self, *, requested_weight=1, priority='background'):
        ceiling = quota.LIMIT if priority == 'protection' else quota.BACKGROUND_LIMIT
        used = self.used()
        eligible = used + requested_weight <= ceiling
        retry = 0 if eligible else None
        if not eligible and requested_weight <= ceiling:
            remaining = used
            for row in sorted(self.tickets.values(), key=lambda row: row['at_ms']):
                if row['at_ms'] <= self.clock() - quota.WINDOW_MS:
                    continue
                remaining -= row['weight']
                if remaining + requested_weight <= ceiling:
                    retry = row['at_ms'] + quota.WINDOW_MS - self.clock()
                    break
        result = dict(version=quota.VERSION, priority=priority,
            maximum_weight=quota.LIMIT, background_maximum_weight=quota.BACKGROUND_LIMIT,
            window_ms=quota.WINDOW_MS, used_weight=used,
            requested_weight=requested_weight, ceiling=ceiling,
            eligible=eligible, retry_after_ms=retry)
        self.capacity_queries.append(dict(at_ms=self.clock(), **result))
        return result

    def _fund_observation(self, entries, priority, deadline):
        if self.clock() * 1000000 > deadline:
            raise quota.BudgetError('TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED')
        ceiling = quota.LIMIT if priority == 'protection' else quota.BACKGROUND_LIMIT
        total = sum(weight for _, _, weight in entries)
        used = self.used()
        if used + total > ceiling:
            retry = None
            remaining = used
            for row in sorted(self.tickets.values(), key=lambda row: row['at_ms']):
                if row['at_ms'] <= self.clock() - quota.WINDOW_MS:
                    continue
                remaining -= row['weight']
                if remaining + total <= ceiling:
                    retry = row['at_ms'] + quota.WINDOW_MS - self.clock()
                    break
            value = dict(at_ms=self.clock(), priority=priority,
                used_weight=used, requested_weight=total, ceiling=ceiling,
                retry_after_ms=retry)
            self.denials.append(value)
            raise quota.BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED',
                **{key: value[key] for key in ('used_weight', 'requested_weight',
                                                'ceiling', 'retry_after_ms')})
        self.plan_weights.append(total)
        for _, token, weight in entries:
            self.tickets[token] = dict(at_ms=self.clock(), weight=weight, claimed=False)
        self.admissions.append(dict(at_ms=self.clock(), priority=priority,
            before=used, requested=total, after=self.used(), ceiling=ceiling))

    def _claim_observation(self, token, weight, deadline):
        row = self.tickets[token]
        if row['claimed'] or row['weight'] != weight:
            raise AssertionError('CLAIM_MUST_BE_EXACT_AND_SINGLE_USE')
        if self.clock() * 1000000 > deadline:
            raise quota.BudgetError('TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED')
        row['claimed'] = True
        row['at_ms'] = self.clock()
        self.claims.append((token, self.clock(), weight))

    def _release_unclaimed_observation(self, entries):
        for token, weight in entries:
            row = self.tickets[token]
            assert not row['claimed'] and row['weight'] == weight
            del self.tickets[token]
            self.releases.append((token, weight))

    def acquire(self, path, body, *, priority='background', host=quota.HOST):
        if priority not in quota.PRIORITIES:
            raise AssertionError('UNKNOWN_PRIORITY')
        weight = quota.request_weight(path, body, host=host)
        token = uuid.uuid4().hex
        deadline = self.clock() * 1000000 + quota.PERMIT_MS * 1000000
        self._fund_observation([(json.dumps(body, sort_keys=True), token, weight)], priority, deadline)
        self._claim_observation(token, weight, deadline)
        return quota.Permit(self, token, body.get('type', 'exchange'), weight, deadline)

    def _settle(self, token, weight):
        row = self.tickets[token]
        assert row['claimed'] and 0 < weight <= row['weight']
        if row.get('settled'):
            return False
        row['weight'] = weight
        row['settled'] = True
        return True


class MeteredRaw:
    """Injected raw endpoint honoring copied production plan budget identity."""
    parallel = False
    # Use production batching/pagination verbatim. Without these methods,
    # sync.collect would take its intentionally simpler legacy-fixture path.
    observation_batch = PublicReader.observation_batch
    reserve_history_split = PublicReader.reserve_history_split
    observation_inputs = PublicReader.observation_inputs

    def __init__(self, raw, budget, priority, latency_ms):
        self.raw, self.budget, self.priority = raw, budget, priority
        self.latency_ms = latency_ms
        self._observation_budget = None

    def read(self, kind, account=None, **kwargs):
        body = dict(type=kind)
        user = account or kwargs.get('user')
        if user is not None:
            body['user'] = user
        for key in ('coin', 'oid'):
            if kwargs.get(key) is not None:
                body[key] = kwargs[key]
        if kind == 'orderStatus':
            body['oid'] = int(body['oid'])
        if kind == 'userFillsByTime':
            body.update(startTime=kwargs['start'], endTime=kwargs['end'], aggregateByTime=False)
        budget = self._observation_budget or self.budget
        permit = budget.acquire('/info', body, priority=self.priority)
        permit.check()
        self.raw.exchange.t += self.latency_ms
        result = self.raw.read(kind, account, **kwargs)
        permit.finish(result)
        return result

    def lookup(self, account, cloid):
        # The actual production lookup also pays ordinary orderStatus weight.
        permit = self.budget.acquire('/info', dict(type='orderStatus', user=account, oid=cloid),
                                     priority=self.priority)
        permit.check()
        self.raw.exchange.t += self.latency_ms
        result = self.raw.lookup(account, cloid)
        permit.finish(result)
        return result


def sol_alert(at_ms, *, side='LONG', cycle='first'):
    msg = maxpain_alert(approved_ms=at_ms)
    msg.update(symbol='SOL', side=side,
        rule_id=next(key for key, spec in contract.SPECS.items() if spec[0] == 'SOL'))
    msg['policy']['source_price'] = 'HYPERLIQUID_SOL_PERPETUAL_TRADE_1M'
    msg['proof']['cycle_id'] = cycle
    if side == 'SHORT':
        msg['stop'], msg['take_profit'] = msg['take_profit'], msg['stop']
    msg['occurrence_id'] = contract.occurrence_id(msg)
    return contract.validate(msg)


class ReductionReplay(unittest.TestCase):
    def fixture(self, latency_ms=0):
        fixture = safety_fixture.LiveSafetyTests(methodName='runTest')
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fx = fixture.fx
        ledger = RollingBudget(fx.oracle.now)
        fx.budget = fx.provider.budget = fx.port.budget = ledger
        fx.raw.budget = ledger
        protection = MeteredRaw(fx.raw, ledger, 'protection', latency_ms)
        entry = MeteredRaw(fx.raw, ledger, 'background', latency_ms)
        fx.provider._info_reader = fx.provider._public_reader = protection
        fx.provider._entry_reader = entry
        fx.provider._lookup_reader = protection.lookup
        fixture.supervisor._thread = SimpleNamespace(is_alive=lambda: True, join=lambda **kwargs: None)
        from .experimental_live_service import ConnectionService
        fx.service=ConnectionService(fx.worker,key=fx.key,release_loader=lambda:deepcopy(fx.release),
            feed=fixture.feed,supervisor=fixture.supervisor)
        fx.service._thread=SimpleNamespace(is_alive=lambda:True,join=lambda **kwargs:None)
        fixture.ready_feed()
        clock = patch.object(quota.time, 'monotonic_ns', lambda: fx.oracle.now() * 1000000)
        clock.start(); self.addCleanup(clock.stop)
        return fixture, ledger

    def counts(self):
        from collections import Counter
        fixture, ledger = self.fixture()
        fx = fixture.fx
        base = fx.oracle.now()
        fixture.supervisor.pass_once(force_collect=True)
        empty = dict(reads=len(fx.raw.calls), weight=ledger.used(),
                     by_kind=dict(Counter(row[0] for row in fx.raw.calls)))
        before=len(fx.raw.calls); weight=ledger.used()
        message=sol_alert((base//60000+1)*60000)
        fx.oracle.t=contract.moment_ms(message['approved_at'])
        account=fx.release['routes']['long_account']
        fx.oracle.mark[core._lane(account,message['symbol'])]=message['entry']
        fx.worker.receive([message])
        fx.provider.collect(fx.store.load(),entries_enabled=True)
        entry=dict(reads=len(fx.raw.calls)-before,weight=ledger.used()-weight,
                   by_kind=dict(Counter(row[0] for row in fx.raw.calls[before:])))
        result=dict(empty_inventory=empty,entry_collection=entry)
        # Measure complete, unchanged HTTP boundaries. Setup calls are excluded
        # from each named scenario, but the real weighted limiter stays active.
        for waiting in (False,True):
            idle,charged=self.fixture(); idle_fx=idle.fx
            idle.supervisor.pass_once(force_collect=True)
            began=idle_fx.oracle.now()
            if waiting:
                deferred=sol_alert((began//60000)*60000,cycle='waiting')
                idle_fx.worker.receive([deferred])
                idle_fx.store.mutate(lambda state:state.setdefault('entry_decisions',{}).update({
                    deferred['occurrence_id']:dict(reason='TESTNET_REQUEST_BUDGET_EXHAUSTED',
                                                   retry_after_ms=began+45000)}))
            first=len(idle_fx.raw.calls); used=charged.used(); states=[]
            for offset in range(5000,30001,5000):
                idle_fx.oracle.t=began+offset
                states.append(idle_fx.service.tick()['status'])
                idle.supervisor.pass_once()
            result['budget_wait_30_seconds' if waiting else 'idle_30_seconds']=dict(
                reads=len(idle_fx.raw.calls)-first,weight=charged.used()-used,
                by_kind=dict(Counter(row[0] for row in idle_fx.raw.calls[first:])),states=states,
                synthetic_actions=len(idle_fx.http))
        stable,charged=self.fixture(); stable_fx=stable.fx
        stable.supervisor.pass_once(force_collect=True)
        message=sol_alert((stable_fx.oracle.now()//60000)*60000,cycle='protected')
        account=stable_fx.release['routes']['long_account']
        stable_fx.oracle.mark[core._lane(account,message['symbol'])]=message['entry']
        stable_fx.worker.receive([message])
        entered=stable_fx.worker.run_once(entries_enabled=True)
        self.assertEqual(entered.get('operation'),'ENTRY',entered)
        trade=stable_fx.store.load()['trades'][message['occurrence_id']]
        stable_fx.oracle.fill(stable_fx.oracle.oid('ENTRY'),trade['quantity'])
        for _ in range(3):stable_fx.worker.run_once(entries_enabled=False)
        trade=stable_fx.store.load()['trades'][message['occurrence_id']]
        self.assertEqual(trade['phase'],'OPEN',trade)
        for blocked in (False,True):
            if blocked:
                peer=sol_alert((stable_fx.oracle.now()//60000)*60000,cycle='blocked-peer')
                stable_fx.worker.receive([peer])
            first=len(stable_fx.raw.calls); used=charged.used()
            collected=stable_fx.provider.collect(stable_fx.store.load(),entries_enabled=True)
            result['blocked_peer_collection' if blocked else 'stable_protected_collection']=dict(
                reads=len(stable_fx.raw.calls)-first,weight=charged.used()-used,
                by_kind=dict(Counter(row[0] for row in stable_fx.raw.calls[first:])),
                entry_capacity_count=len(collected['capacity']),entry_blocked=collected['entry_blocked'])
        return result

    def replay(self, *, latency_ms=0, warm_weight=616, cadence_ms=5000,
               burst=False, side='LONG', lifecycle=False, partial=False,
               lost_response=False, finish_leg='TAKE_PROFIT'):
        fixture, ledger=self.fixture(latency_ms)
        fx=fixture.fx
        base=(T//60000+2)*60000
        fx.oracle.t=base-5000
        fixture.supervisor.pass_once(force_collect=True)
        warm_weight=max(warm_weight,ledger.used())
        remaining=warm_weight-ledger.used()
        # Historic load uses original 88-weight empty scan spacing. The last
        # actual scan is charged at its current cost; retain the measured total.
        times=[-65000,-55000,-45000,-35000,-25000,-15000]
        while remaining:
            at=times.pop(0) if times else -15000
            amount=min(88,remaining)
            ledger.seed(base+at,amount);remaining-=amount
        self.assertEqual(ledger.used(),warm_weight)
        fx.oracle.t=base
        messages=[sol_alert(base,side=side)]
        if burst:messages += [sol_alert(base,cycle='second'),sol_alert(base,side='SHORT',cycle='third')]
        for message in messages:
            account=fx.release['routes']['long_account' if message['side']=='LONG' else 'short_account']
            fx.oracle.mark[core._lane(account,message['symbol'])]=message['entry']
        fx.worker.receive(messages)
        fx.fail_reply=lost_response
        worker_at=supervisor_at=base
        expires=contract.moment_ms(messages[0]['expires_at'])
        samples=[]
        while min(worker_at,supervisor_at)<expires:
            if worker_at<=supervisor_at:
                fx.oracle.t=max(fx.oracle.t,worker_at)
                fx.service._wake.clear()
                try: result=fx.service.tick()
                except Exception as exc: result=dict(status=type(exc).__name__,error=str(exc))
                worker_at=fx.oracle.now()+round(fx.service._next_wait_seconds(cadence_ms/1000)*1000) if hasattr(fx.service,'_next_wait_seconds') else fx.oracle.now()+cadence_ms
                actor='worker'
            else:
                fx.oracle.t=max(fx.oracle.t,supervisor_at)
                result=fixture.supervisor.pass_once()
                supervisor_at=fx.oracle.now()+round(fixture.supervisor.interval*1000)
                actor='supervisor'
                if fx.service._wake.is_set():worker_at=min(worker_at,fx.oracle.now())
            samples.append(dict(at_ms=fx.oracle.now()-base,actor=actor,status=result['status'],error=result.get('error'),weight=ledger.used()))
            if fx.http:break
        result=dict(warm_weight=warm_weight,latency_ms=latency_ms,side=side,burst=burst,
            submitted_ms=fx.oracle.now()-base if fx.http else None,synthetic_actions=len(fx.http),
            denials=deepcopy(ledger.denials),samples=samples,source_expiry_unchanged=True,final_entry_decisions=fx.store.load().get('entry_decisions',{}))
        for message in messages:
            self.assertEqual(fx.store.load()['sources'][message['occurrence_id']]['source']['expires_at'],message['expires_at'])
        self.assertTrue(all(row['after']<=row['ceiling'] for row in ledger.admissions))
        if lifecycle and fx.http:
            result['lifecycle']=self.lifecycle(fixture,ledger,messages[0],partial=partial,
                                               finish_leg=finish_leg,lost_response=lost_response)
        return result

    def lifecycle(self,fixture,ledger,message,*,partial=False,finish_leg='TAKE_PROFIT',lost_response=False):
        fx=fixture.fx;cid=message['occurrence_id'];start=fx.oracle.now()
        trade=fx.store.load()['trades'][cid];account=trade['account'];role=trade['role']
        def fill(oid,quantity):
            fx.oracle.fill(oid,quantity)
            generation=fixture.feed.health()[role]['generation']
            self.assertTrue(fixture.feed._receive(account,generation,json.dumps(dict(
                channel='orderUpdates',data=[dict(order=dict(coin=trade['symbol'],oid=int(oid)),
                    status='filled',statusTimestamp=fx.oracle.now())]))))
        from decimal import Decimal
        quantity=Decimal(trade['quantity'])
        partial_quantity=(quantity/2).quantize(Decimal('.01'),rounding='ROUND_DOWN')
        fill(fx.oracle.oid('ENTRY'),partial_quantity if partial else quantity)
        # Lost response leaves an unknown request, to be resolved by collection.
        worker_at=supervisor_at=start
        closed=protected=filled_exit=None;samples=[]
        while min(worker_at,supervisor_at)<start+3*quota.WINDOW_MS:
            if worker_at<=supervisor_at:
                fx.oracle.t=max(fx.oracle.t,worker_at)
                fx.service._wake.clear()
                try: answer=fx.service.tick()
                except Exception as exc: answer=dict(status=type(exc).__name__,error=str(exc))
                worker_at=fx.oracle.now()+round(fx.service._next_wait_seconds(5)*1000) if hasattr(fx.service,'_next_wait_seconds') else fx.oracle.now()+5000;actor='worker'
            else:
                fx.oracle.t=max(fx.oracle.t,supervisor_at)
                answer=fixture.supervisor.pass_once();supervisor_at=fx.oracle.now()+fixture.supervisor.interval*1000;actor='supervisor'
                if fx.service._wake.is_set():worker_at=min(worker_at,fx.oracle.now())
            current=fx.store.load()['trades'][cid]
            legs={current['order_legs'][oid] for oid,order in current['orders'].items() if order['status']=='OPEN'}
            pending=[r for r in fx.store.load()['requests'].values() if r['proposal']['card_id']==cid and r['phase'] not in ('OBSERVED','ABORTED_UNSENT')]
            samples.append(dict(at_ms=fx.oracle.now()-start,actor=actor,status=answer['status'],error=answer.get('error'),phase=current['phase'],legs=sorted(legs),weight=ledger.used()))
            if protected is None and {'STOP','TAKE_PROFIT'}<=legs and not pending:
                protected=fx.oracle.now()-start
                fill(fx.oracle.oid(finish_leg),core._remaining(current));filled_exit=fx.oracle.now()-start
                worker_at=supervisor_at=fx.oracle.now()
            if current['phase']=='CLOSED':
                self.assertFalse(pending);self.assertFalse(legs);self.assertEqual(core._remaining(current),0)
                closed=fx.oracle.now()-start;break
        wire=[row for row in fx.oracle.requests if row['proposal']['card_id']==cid]
        operations=[row['proposal']['operation']+':'+row['proposal']['leg'] for row in wire]
        self.assertEqual(operations.count('ENTRY:ENTRY'),1)
        return dict(protected_ms=protected,filled_exit_ms=filled_exit,closed_ms=closed,wire_actions=operations,
            wire_exit_times_ms={r['proposal']['leg']:r['attempt_at_ms']-start for r in wire if r['proposal']['operation']=='CREATE_EXIT'},samples=samples,request_results=[{k:v for k,v in r.items() if k in ('phase','abort_reason','last_error','outcome','unsent_proof')} for r in fx.store.load()['requests'].values()])

    def test_original_warm_budget_reaches_entry_and_full_lifecycle(self):
        result=self.replay(lifecycle=True)
        self.assertIsNotNone(result['submitted_ms'],result)
        self.assertLess(result['submitted_ms'],90000)
        self.assertIsNotNone(result['lifecycle']['closed_ms'],result)

    def test_partial_fill_lost_reply_stop_and_cancel_complete_without_duplicate_entry(self):
        result=self.replay(latency_ms=50,lifecycle=True,partial=True,lost_response=True,finish_leg='STOP')
        self.assertIsNotNone(result['submitted_ms'],result)
        self.assertIsNotNone(result['lifecycle']['closed_ms'],result)

    def test_parallel_alerts_and_short_account_keep_the_original_entry_expiry(self):
        for options in (dict(burst=True),dict(side='SHORT',latency_ms=150)):
            with self.subTest(**options):
                result=self.replay(**options)
                self.assertIsNotNone(result['submitted_ms'],result)
                self.assertLess(result['submitted_ms'],90000)
                self.assertEqual(result['synthetic_actions'],1)

    def concurrent(self, *, simultaneous_fills=True, warm_weight=616):
        fixture,ledger=self.fixture(50);fx=fixture.fx
        base=(T//60000+2)*60000;fx.oracle.t=base-5000
        fixture.supervisor.pass_once(force_collect=True)
        warm_weight=max(warm_weight,ledger.used())
        remaining=warm_weight-ledger.used()
        times=[-65000,-55000,-45000,-35000,-25000,-15000]
        while remaining:
            at=times.pop(0) if times else -15000
            amount=min(88,remaining)
            ledger.seed(base+at,amount);remaining-=amount
        fx.oracle.t=base
        messages=[sol_alert(base),sol_alert(base,side='SHORT',cycle='independent-short')]
        for msg in messages:
            account=fx.release['routes']['long_account' if msg['side']=='LONG' else 'short_account']
            fx.oracle.mark[core._lane(account,msg['symbol'])]=msg['entry']
        fx.worker.receive(messages)
        worker_at=supervisor_at=base;entry_fills=set();exit_fills=set();samples=[];closed=None
        def fill(trade,oid,quantity):
            fx.oracle.fill(oid,quantity)
            generation=fixture.feed.health()[trade['role']]['generation']
            self.assertTrue(fixture.feed._receive(trade['account'],generation,json.dumps(dict(
                channel='orderUpdates',data=[dict(order=dict(coin=trade['symbol'],oid=int(oid)),
                    status='filled',statusTimestamp=fx.oracle.now())]))))
        while min(worker_at,supervisor_at)<base+3*quota.WINDOW_MS:
            if worker_at<=supervisor_at:
                fx.oracle.t=max(fx.oracle.t,worker_at)
                fx.service._wake.clear()
                try:answer=fx.service.tick()
                except Exception as exc:answer=dict(status=type(exc).__name__,error=str(exc))
                worker_at=fx.oracle.now()+round(fx.service._next_wait_seconds(5)*1000) if hasattr(fx.service,'_next_wait_seconds') else fx.oracle.now()+5000
            else:
                fx.oracle.t=max(fx.oracle.t,supervisor_at)
                answer=fixture.supervisor.pass_once();supervisor_at=fx.oracle.now()+5000
            if fx.service._wake.is_set():worker_at=min(worker_at,fx.oracle.now())
            state=fx.store.load();trades=state['trades']
            for cid,trade in trades.items():
                entry=next((r for r in fx.oracle.requests if r['proposal']['card_id']==cid and r['proposal']['operation']=='ENTRY'),None)
                both_submitted=sum(r['proposal']['operation']=='ENTRY' for r in fx.oracle.requests)==2
                if entry is not None and cid not in entry_fills and (both_submitted or not simultaneous_fills):
                    oid=next(oid for oid,item in fx.oracle.orders.items() if item['view']['cloid']==entry['proposal']['action']['orders'][0]['c'])
                    fill(trade,oid,trade['quantity']);entry_fills.add(cid)
                orders={trade['order_legs'][oid]:(oid,row) for oid,row in trade['orders'].items() if row['status']=='OPEN'}
                all_protected=len(trades)==2 and all(t['phase']=='CLOSED' or {'STOP','TAKE_PROFIT'}<={t['order_legs'][o] for o,r in t['orders'].items() if r['status']=='OPEN'} for t in trades.values())
                if cid not in exit_fills and {'STOP','TAKE_PROFIT'}<=set(orders) and (all_protected or not simultaneous_fills):
                    fill(trade,orders['TAKE_PROFIT'][0],core._remaining(trade));exit_fills.add(cid)
            samples.append(dict(at_ms=fx.oracle.now()-base,status=answer['status'],error=answer.get('error'),weight=ledger.used(),
                phases={r['role']:r['phase'] for r in trades.values()}))
            if len(trades)==2 and all(t['phase']=='CLOSED' for t in trades.values()):closed=fx.oracle.now()-base;break
        state=fx.store.load()
        entries=[r for r in fx.oracle.requests if r['proposal']['operation']=='ENTRY']
        return dict(warm_weight=warm_weight,entries=[dict(side=r['proposal']['role'],at_ms=r['attempt_at_ms']-base) for r in entries],
            closed_ms=closed,phases={t['role']:t['phase'] for t in state['trades'].values()},
            samples=samples,denials=ledger.denials,synthetic_actions=len(fx.http),entry_decisions=state.get('entry_decisions',{}),wire_actions=[r['proposal']['role']+':'+r['proposal']['operation']+':'+r['proposal']['leg'] for r in fx.oracle.requests],orders=fx.oracle.orders,final_requests=[{k:v for k,v in r.items() if k in ('phase','observed_oid','result','outcome')} for r in state['requests'].values()],final_context={k:v for k,v in fx.provider._last['context'].items() if k in ('entry_blocked','account_entry_blocked','blocked_lanes')})

    def test_two_accounts_finish_separate_trades_under_one_weighted_budget(self):
        result=self.concurrent(warm_weight=44)
        self.assertEqual(len(result['entries']),2,result)
        self.assertTrue(all(r['at_ms']<90000 for r in result['entries']),result)
        self.assertIsNotNone(result['closed_ms'],result)
