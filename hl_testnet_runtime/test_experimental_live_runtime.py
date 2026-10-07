"""Testnet-domain worker integration with explicit simulated ports only.

The local unit suite substitutes transactional storage and endpoint I/O. The
optional duplicate contract suite uses real disposable loopback PostgreSQL.
No fixture data or private key can be dispatched to a real exchange.
"""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from decimal import Decimal
import os
import threading
import unittest
from unittest.mock import patch

from experimental_execution_fixtures import r2732_message, hype_row71205_message, sol_g65_message
from . import experimental_execution_runtime as isolated
from . import experimental_live_runtime as runtime
from . import experimental_live_state as state
from . import card_lifecycle as life, filled_quantity_dispatch as wire
from .filled_dispatch_store import DispatchStore, SCHEMA as LEGACY_SCHEMA
from .postgres_journal import PostgresJournal
from .test_experimental_execution_runtime import SoftwareExchange, T
from .test_experimental_live_state import LIVE_ROUTES
from .test_maxpain_execution import plan, ARM, CREATED, report_plan, REPORT_FORMULAS

CI=os.environ.get('HL_JOURNAL_CI_URL')


class MemoryTransactions:
    """Unit stand-in only; never exposed as a selectable production backend."""
    def __init__(self, routes, not_before):
        self.value=state.initial(routes,not_before);self.lock=threading.RLock();self.nonces={}
    def load(self):
        with self.lock:return deepcopy(self.value)
    def mutate(self,fn):
        with self.lock:
            value=deepcopy(self.value);result=fn(value);value['revision']+=1
            state.checked(value,life.digest(value));self.value=value
            return deepcopy(result)
    def commit_attempt(self,fn,*,role,now_ms):
        with self.lock:
            agent=self.value['agents'][role];nonce=max(now_ms,self.nonces.get(agent,0)+1)
            result=self.mutate(lambda value:fn(value,nonce));self.nonces[agent]=nonce
            return result


class RuntimeContract:
    real_pg=False
    def setUp(self):
        from .experimental_live_provider import LiveEvidenceProvider
        from .experimental_live_dispatch import LiveDispatchPort
        self.Provider,self.Port=LiveEvidenceProvider,LiveDispatchPort
        for target in ('http.client.HTTPSConnection','socket.create_connection',
                'hyperliquid_testnet_executor._wallet','hyperliquid_testnet_executor._signed_body'):
            p=patch(target,side_effect=AssertionError('NO_LIVE_NETWORK_OR_KEYS'));p.start();self.addCleanup(p.stop)
        self.venue=SoftwareExchange(T+10000);self.reservations=[]
        self.make_worker(not_before=T-60000)

    def make_worker(self,not_before):
        self.journal=PostgresJournal.for_ci(CI or 'postgresql://fixture:unused@127.0.0.1:5432/hl_journal_ci')
        self.store=state.TestnetExecutionState.for_ci(self.journal)
        if self.real_pg:
            self.journal.bootstrap();DispatchStore(self.journal).initialize()
            with self.journal._transaction() as conn:
                conn.execute(f'DROP SCHEMA IF EXISTS {state.PG_SCHEMA} CASCADE')
                conn.execute(f'DELETE FROM {LEGACY_SCHEMA}.nonces WHERE agent=ANY(%s)',([v['agent'] for v in LIVE_ROUTES.values()],))
            self.store.initialize(LIVE_ROUTES,not_before_ms=not_before)
        else:
            self.memory=MemoryTransactions(LIVE_ROUTES,not_before)
            self.store.load=self.memory.load;self.store.mutate=self.memory.mutate
            self.store.commit_attempt=self.memory.commit_attempt
        self.release=dict(domain='testnet',release_id='a'*64,dispatch_enabled=True,
            protection_enabled=True,entries_enabled=True,not_before_ms=not_before,
            entry_expires_at_ms=max(T,ARM)+86400000,
            routes={role:v['account'] for role,v in LIVE_ROUTES.items()})
        self.provider=object.__new__(self.Provider)
        self.provider.collect=lambda value,**kwargs:self.venue.collect(value);self.provider.now=self.venue.now
        self.port=object.__new__(self.Port)
        self.port.now=self.venue.now;self.port.domain='testnet'
        def reserve(proposal):
            self.reservations.append(deepcopy(proposal))
            return wire.TransportAdmission(proposal,isolated._Permit(self.venue.now,self.venue.now()))
        def send(request,*,admission):
            claimed=self.store.claim_transport(request,'b'*32)
            return self.venue.send(claimed,admission=admission)
        self.port.reserve_transport=reserve;self.port.send=send
        self.worker=runtime.TestnetExecutionRuntime(self.store,self.provider,self.port,
            release_loader=lambda:deepcopy(self.release),mode=runtime.MODE)

    def cycle(self,entries=True):return self.worker.run_once(entries_enabled=entries)
    def start(self,msg=None):
        msg=msg or r2732_message(entry=2.3,decision_ms=T-60000)
        self.worker.receive([msg]);return msg,self.cycle()
    def protect(self):
        for _ in range(3):self.cycle(False)
    def restart(self):
        self.worker=runtime.TestnetExecutionRuntime(self.store,self.provider,self.port,
            release_loader=lambda:deepcopy(self.release),mode=runtime.MODE)

    def test_entries_are_off_by_default_and_never_reserve_budget(self):
        self.worker.receive([r2732_message(entry=2.3,decision_ms=T-60000)])
        result=self.worker.run_once()
        self.assertEqual(result['status'],'OBSERVED_NO_ACTION')
        self.assertFalse(self.reservations);self.assertFalse(self.store.load()['trades'])

    def test_full_fill_protection_close_cleanup_persists_testnet_domain(self):
        msg,result=self.start();self.assertEqual(result['operation'],'ENTRY');self.protect()
        trade=self.store.load()['trades'][msg['occurrence_id']]
        self.venue.fill(self.venue.oid('TAKE_PROFIT'),trade['quantity'])
        for _ in range(3):self.cycle(False)
        self.restart();self.assertEqual(self.store.load()['trades'][msg['occurrence_id']]['phase'],'CLOSED')
        value=self.store.load()
        self.assertEqual({r['domain'] for r in value['requests'].values()},{'testnet'})
        self.assertEqual({s['domain'] for s in value['sources'].values()},{'testnet'})
        self.assertNotIn('budget',value)
        self.assertEqual(len(self.reservations),len(self.venue.requests))
        self.assertEqual(self.worker.report()['domain'],'testnet')

    def test_release_expiry_and_entry_halt_keep_partial_fill_protection(self):
        self.venue.instant_fraction=Decimal('.5');msg,_=self.start()
        self.release['entries_enabled']=False;self.release['entry_expires_at_ms']=self.venue.t
        self.protect()
        trade=self.store.load()['trades'][msg['occurrence_id']]
        for leg in ('STOP','TAKE_PROFIT'):
            self.assertEqual(Decimal(self.venue.orders[self.venue.oid(leg)]['view']['wire_order']['s']),Decimal(trade['quantity'])/2)
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in self.venue.requests),1)

    def test_lost_reply_and_restart_never_replays_entry(self):
        self.venue.lose_reply=True;msg,result=self.start()
        self.assertEqual(result['status'],'OUTCOME_UNKNOWN_RECONCILIATION_REQUIRED')
        self.restart();self.protect()
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in self.venue.requests),1)
        self.assertEqual(len(self.venue.requests),3)

    def test_unknown_commit_reply_stops_before_transport_and_stays_fenced(self):
        msg=r2732_message(entry=2.3,decision_ms=T-60000);self.worker.receive([msg])
        original=self.store.commit_attempt
        def lose(*args,**kwargs):original(*args,**kwargs);raise state.StateError('COMMIT_ACK_LOST')
        with patch.object(self.store,'commit_attempt',side_effect=lose),self.assertRaisesRegex(state.StateError,'COMMIT_ACK_LOST'):
            self.cycle()
        self.restart();self.cycle()
        self.assertFalse(self.venue.requests);self.assertEqual(len(self.store.load()['requests']),1)

    def test_shared_budget_denial_cannot_leave_trade_or_attempt(self):
        from .request_budget import BudgetError
        msg=r2732_message(entry=2.3,decision_ms=T-60000);self.worker.receive([msg])
        with patch.object(self.port,'reserve_transport',side_effect=BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED')):
            with self.assertRaises(BudgetError):self.cycle()
        self.assertFalse(self.store.load()['trades']);self.assertFalse(self.store.load()['requests'])

    def test_release_halt_between_reservation_and_send_aborts_never_sent_entry(self):
        original=self.store.commit_attempt
        def halt(*args,**kwargs):
            result=original(*args,**kwargs);self.release['entries_enabled']=False;return result
        with patch.object(self.store,'commit_attempt',side_effect=halt):msg,result=self.start()
        self.assertEqual(result['status'],'CANCELED_BEFORE_TRANSPORT');self.assertFalse(self.venue.requests)
        self.assertEqual(next(iter(self.store.load()['requests'].values()))['phase'],'ABORTED_UNSENT')

    def test_missing_mark_history_blocks_entry_but_keeps_protection(self):
        original=self.provider.collect
        def blocked(value,**kwargs):
            result=original(value);result['entry_blocked']={cid:'COMPLETE_MARK_HISTORY_REQUIRED' for cid in value['sources']}
            return result
        self.provider.collect=blocked
        msg,result=self.start();self.assertEqual(result['status'],'OBSERVED_NO_ACTION')
        self.assertEqual(self.worker.report()['entry_blocked'][msg['occurrence_id']],'COMPLETE_MARK_HISTORY_REQUIRED')
        self.provider.collect=original;self.cycle();self.provider.collect=blocked;self.protect()
        self.assertEqual(len(self.venue.requests),3)

    def test_checkpoint_and_ownership_revision_survive_observation_cycle(self):
        original=self.provider.collect
        def collect(value,**kwargs):
            result=original(value);result.update(collector_checkpoints={'fixture':{'cursor':1}},ownership_revision='c'*64);return result
        self.provider.collect=collect;self.worker.run_once()
        self.assertEqual(self.store.load()['collector_checkpoints'],{'fixture':{'cursor':1}})
        self.assertEqual(self.store.load()['ownership_revision'],'c'*64)

    def test_concurrent_workers_cannot_both_send_same_admission(self):
        msg=r2732_message(entry=2.3,decision_ms=T-60000);self.worker.receive([msg])
        other=runtime.TestnetExecutionRuntime(self.store,self.provider,self.port,
            release_loader=lambda:deepcopy(self.release),mode=runtime.MODE)
        barrier=threading.Barrier(2);original=self.provider.collect
        def collect(value,**kwargs):result=original(value);barrier.wait(timeout=10);return result
        self.provider.collect=collect
        def run(w):
            try:return w.run_once(entries_enabled=True)['status']
            except isolated.RuntimeError as exc:return str(exc)
        with ThreadPoolExecutor(2) as pool:results=list(pool.map(run,[self.worker,other]))
        self.assertIn('CONCURRENT_OBSERVATION_RELOAD_REQUIRED',results)
        self.assertEqual(sum(r['proposal']['operation']=='ENTRY' for r in self.venue.requests),1)

    def test_durable_wire_claim_is_one_use_including_new_worker(self):
        captured=[]
        def hold(request,*,admission):captured.append(deepcopy(request));raise TimeoutError()
        with patch.object(self.port,'send',side_effect=hold):self.start()
        request=captured[0];claimed=self.store.claim_transport(request,'d'*32)
        self.restart()
        with self.assertRaisesRegex(state.StateError,'UNCLAIMED'):
            self.store.claim_transport(request,'e'*32)
        self.assertEqual(self.store.request(request['request_id']),claimed)

    def test_certified_unsent_initial_stop_recovers_without_blocking_filled_trade(self):
        from .experimental_live_dispatch import DefinitelyNotSubmitted, LiveDispatchError
        self.venue.instant_fraction=Decimal('.5');msg,_=self.start()
        original=self.port.send
        def no_wire(request,*,admission):
            claimed=self.store.claim_transport(request,'c'*32)
            raise DefinitelyNotSubmitted(claimed,LiveDispatchError('FINAL_EVIDENCE_EXPIRED'))
        with patch.object(self.port,'send',side_effect=no_wire):result=self.cycle(False)
        self.assertEqual(result['status'],'DEFINITELY_NOT_SUBMITTED_REOBSERVE')
        self.assertEqual(len(self.venue.requests),1)
        self.assertEqual(self.store.load()['trades'][msg['occurrence_id']]['phase'],'OPEN')
        self.protect()
        self.assertEqual(len(self.venue.requests),3)
        self.assertEqual({o['leg'] for o in self.venue.orders.values()},{'ENTRY','STOP','TAKE_PROFIT'})

    def rejection_context(self):
        original=self.provider.collect
        def collect(value,**kwargs):
            result=original(value);result['rejected_requests']=[]
            for rid,r in value['requests'].items():
                if r['phase']=='OUTCOME_UNKNOWN' and (r.get('reply') or {}).get('state')=='REJECTED':
                    action=r['proposal']['action'];kind=action['type']
                    fact=dict(request_id=rid,account=r['proposal']['account'],symbol=r['proposal']['symbol'],
                        at_ms=self.venue.t,lookup_status='orderStillOpen' if kind=='cancel' else 'unknownOid',reply_digest=life.digest(r['reply']))
                    if kind in ('batchModify','cancel'):
                        fact['old_order_id']=str(action['modifies'][0]['oid'] if kind=='batchModify' else action['cancels'][0]['o'])
                    result['rejected_requests'].append(fact)
            return result
        self.provider.collect=collect

    def test_exact_rejected_no_order_entry_retires_without_replay(self):
        def reject(request,*,admission):
            self.store.claim_transport(request,'c'*32)
            return dict(state='REJECTED',code='OTHER_REJECTION',oid=None)
        with patch.object(self.port,'send',side_effect=reject):msg,_=self.start()
        self.rejection_context();self.cycle();self.restart();self.cycle()
        value=self.store.load();self.assertEqual(value['trades'][msg['occurrence_id']]['phase'],'CANCELED_WITHOUT_FILL')
        self.assertEqual(len(value['requests']),1);self.assertFalse(self.venue.requests)
        self.assertEqual(next(iter(value['requests'].values()))['terminal_state'],'REJECTED_NO_ORDER')

    def test_rejected_initial_exit_reconciles_then_can_restore_protection(self):
        msg,_=self.start()
        def reject(request,*,admission):
            self.store.claim_transport(request,'c'*32)
            return dict(state='REJECTED',code='OTHER_REJECTION',oid=None)
        with patch.object(self.port,'send',side_effect=reject):self.cycle(False)
        self.rejection_context();self.protect()
        self.assertEqual(len(self.venue.requests),3)
        self.assertEqual({o['leg'] for o in self.venue.orders.values()},{'ENTRY','STOP','TAKE_PROFIT'})
        rejected=[r for r in self.store.load()['requests'].values() if r.get('terminal_state')=='REJECTED_NO_ORDER']
        self.assertEqual(len(rejected),1)

    def test_missing_order_without_rejection_never_retires_unknown_request(self):
        with patch.object(self.port,'send',side_effect=TimeoutError()):self.start()
        self.rejection_context();self.cycle()
        self.assertEqual(next(iter(self.store.load()['requests'].values()))['phase'],'OUTCOME_UNKNOWN')
        self.assertEqual(len(self.reservations),1)

    def test_unproven_peer_lane_does_not_block_proven_filled_lane_protection(self):
        self.venue.t=ARM+10000;self.make_worker(not_before=CREATED-60000)
        first,_=self.start(r2732_message(entry=2.3,decision_ms=ARM-60000));self.protect()
        second=hype_row71205_message(entry=100,decision_ms=ARM-60000)
        self.worker.receive([second]);self.cycle()
        original=self.provider.collect
        def broken(value,**kwargs):
            result=original(value)
            for snap in result['snapshots']:
                if snap['symbol']=='XRP':snap['orders']=[]
            return result
        self.provider.collect=broken;self.protect()
        target=self.store.load()['trades'][second['occurrence_id']]
        self.assertEqual({target['order_legs'][oid] for oid in target['orders']},{'ENTRY','STOP','TAKE_PROFIT'})
        lane=isolated._lane(LIVE_ROUTES['short_account']['account'],'XRP')
        self.assertIn(lane,self.worker.report()['blocked_lanes'])

    def test_certified_unsent_emergency_does_not_create_phantom_order_fence(self):
        from .experimental_live_dispatch import DefinitelyNotSubmitted, LiveDispatchError
        msg,_=self.start();self.venue.mark[isolated._lane(LIVE_ROUTES['short_account']['account'],'XRP')]='2.4'
        def no_wire(request,*,admission):
            self.assertEqual(request['proposal']['operation'],'EMERGENCY_CLOSE')
            claimed=self.store.claim_transport(request,'c'*32)
            raise DefinitelyNotSubmitted(claimed,LiveDispatchError('FINAL_EVIDENCE_EXPIRED'))
        with patch.object(self.port,'send',side_effect=no_wire):result=self.cycle(False)
        self.assertEqual(result['status'],'DEFINITELY_NOT_SUBMITTED_REOBSERVE')
        self.assertEqual(self.cycle(False)['operation'],'EMERGENCY_CLOSE')
        self.cycle(False)
        self.assertEqual(self.store.load()['trades'][msg['occurrence_id']]['phase'],'CLOSED')
        self.assertEqual(sum(r['proposal']['operation']=='EMERGENCY_CLOSE' for r in self.venue.requests),1)

    def test_rejected_no_order_emergency_allows_fresh_owned_close(self):
        msg,_=self.start();self.venue.mark[isolated._lane(LIVE_ROUTES['short_account']['account'],'XRP')]='2.4'
        def reject(request,*,admission):
            self.assertEqual(request['proposal']['operation'],'EMERGENCY_CLOSE')
            self.store.claim_transport(request,'c'*32)
            return dict(state='REJECTED',code='OTHER_REJECTION',oid=None)
        with patch.object(self.port,'send',side_effect=reject):self.cycle(False)
        self.rejection_context();self.assertEqual(self.cycle(False)['operation'],'EMERGENCY_CLOSE');self.cycle(False)
        self.assertEqual(self.store.load()['trades'][msg['occurrence_id']]['phase'],'CLOSED')

    def test_unknown_emergency_transport_never_repeats_without_resolution(self):
        msg,_=self.start();self.venue.mark[isolated._lane(LIVE_ROUTES['short_account']['account'],'XRP')]='2.4'
        with patch.object(self.port,'send',side_effect=TimeoutError()):self.cycle(False)
        before=len(self.reservations);self.cycle(False);self.restart();self.cycle(False)
        self.assertEqual(len(self.reservations),before)
        self.assertEqual(self.store.load()['trades'][msg['occurrence_id']]['phase'],'OPEN')

    def test_rejected_modify_retains_old_stop_and_waits_for_material_change(self):
        msg,_=self.start();self.protect();old=self.venue.oid('STOP')
        self.venue.t=T+60000;self.venue.mark[isolated._lane(LIVE_ROUTES['short_account']['account'],'XRP')]='2.275'
        self.venue.bars[msg['occurrence_id']]=[dict(open_at_ms=T,open='2.3',high='2.305',low='2.27',close='2.275')]
        def reject(request,*,admission):
            self.assertEqual(request['proposal']['operation'],'AMEND_EXIT')
            self.store.claim_transport(request,'c'*32)
            return dict(state='REJECTED',code='OTHER_REJECTION',oid=None)
        with patch.object(self.port,'send',side_effect=reject):self.cycle(False)
        self.rejection_context();before=len(self.reservations)
        for _ in range(3):self.cycle(False)
        self.assertEqual(len(self.reservations),before)
        self.assertEqual(self.venue.orders[old]['view']['status'],'OPEN')
        self.assertEqual(self.venue.orders[old]['view']['wire_order']['p'],'2.3115')
        take=self.venue.oid('TAKE_PROFIT');qty=Decimal(self.venue.orders[take]['view']['wire_order']['s'])
        self.venue.fill(take,str(qty/2))
        self.assertEqual(self.cycle(False)['operation'],'AMEND_EXIT')
        self.assertEqual(self.venue.orders[self.venue.oid('STOP')]['view']['wire_order']['p'],'2.2943')

    def test_rejected_cancel_does_not_storm_or_invent_finality(self):
        msg,_=self.start();self.protect();trade=self.store.load()['trades'][msg['occurrence_id']]
        self.venue.fill(self.venue.oid('TAKE_PROFIT'),trade['quantity']);old=self.venue.oid('STOP')
        def reject(request,*,admission):
            self.assertEqual(request['proposal']['operation'],'CANCEL')
            self.store.claim_transport(request,'c'*32)
            return dict(state='REJECTED',code='OTHER_REJECTION',oid=None)
        with patch.object(self.port,'send',side_effect=reject):self.cycle(False)
        self.rejection_context();before=len(self.reservations)
        for _ in range(3):self.cycle(False)
        self.assertEqual(len(self.reservations),before)
        self.assertEqual(self.venue.orders[old]['view']['status'],'OPEN')
        self.assertNotEqual(self.store.load()['trades'][msg['occurrence_id']]['phase'],'CLOSED')
        self.venue.t+=1;self.venue.orders[old]['view'].update(status='CANCELED',at_ms=self.venue.t)
        self.cycle(False)
        self.assertEqual(self.store.load()['trades'][msg['occurrence_id']]['phase'],'CLOSED')

    def test_original_software_worker_still_rejects_testnet_state_and_port(self):
        with self.assertRaisesRegex(isolated.RuntimeError,'EXPLICIT_ISOLATED_WORKER_REQUIRED'):
            isolated.IsolatedExecutionRuntime(self.store,self.provider,mode=isolated.MODE)

    def test_eight_approved_formulas_share_entry_and_protection_lifecycle(self):
        formulas=[report_plan(spec,(spec[2]+spec[3])/2) for spec in REPORT_FORMULAS]
        formulas += [r2732_message(entry=2.3,decision_ms=ARM-60000),
            hype_row71205_message(entry=100,decision_ms=ARM-60000),
            sol_g65_message(reference=100,decision_ms=ARM-60000)]
        for msg in formulas:
            with self.subTest(rule=msg['rule_id']):
                self.venue=SoftwareExchange(CREATED if msg['family']=='maxpain' else ARM+10000)
                self.make_worker(not_before=CREATED-60000)
                self.worker.receive([msg]);self.venue.t=max(ARM,self.venue.t)
                result=self.cycle();self.assertEqual(result['operation'],'ENTRY')
                if msg['family'] in ('maxpain','sol_g65'):
                    oid=self.venue.oid('ENTRY');self.venue.fill(oid,self.venue.orders[oid]['view']['wire_order']['s'])
                    role='long_account' if msg['side']=='LONG' else 'short_account'
                    self.venue.mark[isolated._lane(LIVE_ROUTES[role]['account'],msg['symbol'])]=msg['entry']
                self.protect()
                self.assertEqual(self.store.load()['trades'][msg['occurrence_id']]['phase'],'OPEN')
                self.assertEqual({o['leg'] for o in self.venue.orders.values()},{'ENTRY','STOP','TAKE_PROFIT'})


class RuntimeUnitTests(RuntimeContract,unittest.TestCase):
    pass


@unittest.skipUnless(CI,'Requires disposable loopback PostgreSQL; memory transactions are unit evidence only')
class RuntimePostgresTests(RuntimeContract,unittest.TestCase):
    real_pg=True
