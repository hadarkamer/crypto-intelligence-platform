"""Offline weight tests and disposable PostgreSQL cross-process admission races."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import os
import subprocess
import sys
import time
import threading
from types import ModuleType
import unittest
from unittest.mock import Mock, patch

from . import postgres_journal as pg, request_budget as budget

CI_URL = os.environ.get('HL_JOURNAL_CI_URL')


def observation_bodies():
    """Two complete passes, two live OIDs; every body is owned and explicit."""
    account='0x'+'1'*40
    one=[dict(type='userFillsByTime',user=account,startTime=1000,endTime=2000,
              aggregateByTime=False),dict(type='frontendOpenOrders',user=account),
         dict(type='clearinghouseState',user=account),
         dict(type='orderStatus',user=account,oid=1),
         dict(type='orderStatus',user=account,oid=2)]
    return one+one


def entry_info_bodies():
    account='0x'+'1'*40;agent='0x'+'2'*40
    return [dict(type='userAbstraction',user=account)]*2 + [
        dict(type='userRole',user=account),dict(type='userRole',user=agent),
        dict(type='clearinghouseState',user=account),
        dict(type='clearinghouseState',user=account),
        dict(type='spotClearinghouseState',user=account),
        dict(type='activeAssetData',user=account,coin='BTC'),dict(type='meta'),
        dict(type='userRateLimit',user=account),dict(type='frontendOpenOrders',user=account)]


class CoordinatorTests(unittest.TestCase):
    """Simulated round-trip latency with real local locking; never a real DSN."""
    def setUp(self):
        self.registry=patch.dict(budget._coordinators,{},clear=True)
        self.registry.start();self.addCleanup(self.registry.stop)
        self.addCleanup(budget._close_coordinators)
        self.journal=pg.PostgresJournal.for_ci('postgresql://offline:PRIVATE_VALUE@localhost/hl_journal_ci')
        self.budget=budget.Budget(self.journal);self.weights=[];self.connections=[]
        owner=self
        class Connection:
            closed=False
            fail_commit=False
            fail_ready=False
            def execute(self,sql,args=()):
                time.sleep(0.025)
                if self.fail_ready:raise RuntimeError('PRIVATE_VALUE')
                if 'current_database()' in sql:
                    row=('hl_journal_ci',budget.VERSION,budget.HOST,budget.LIMIT,
                         budget.BACKGROUND_LIMIT,budget.WINDOW_MS,budget.TICKETS)
                elif 'pg_advisory_xact_lock' in sql:row=(None,)
                elif 'SELECT ticket.weight' in sql:
                    return Mock(fetchall=lambda:[(weight,budget.WINDOW_MS) for weight in owner.weights])
                elif 'WITH stamp' in sql:
                    _,_,token,weight,_,ceiling=args
                    if sum(owner.weights)+weight>ceiling:row=None
                    else:owner.weights.append(weight);row=(token,)
                else:raise AssertionError('Unexpected coordinator statement')
                return Mock(fetchone=lambda:row)
            def commit(self):
                time.sleep(0.025)
                if self.fail_commit:raise RuntimeError('PRIVATE_VALUE')
            def rollback(self):pass
            def close(self):self.closed=True
        self.Connection=Connection
        def connect(**kwargs):
            connection=Connection();self.connections.append(connection);return connection
        module=ModuleType('psycopg');module.connect=Mock(side_effect=connect)
        module.IsolationLevel=Mock(READ_COMMITTED='READ_COMMITTED')
        self.connector=module.connect
        patcher=patch.dict(sys.modules,{'psycopg':module})
        patcher.start();self.addCleanup(patcher.stop)

    def test_four_delayed_parallel_clients_reuse_one_connection_and_all_receive_fresh_permits(self):
        barrier=threading.Barrier(4)
        def admit(_):
            barrier.wait(timeout=2)
            permit=budget.Budget(self.journal).acquire('/info',{'type':'meta'})
            permit.check();return permit
        with ThreadPoolExecutor(max_workers=4) as pool:
            self.assertEqual(len(list(pool.map(admit,range(4)))),4)
        self.assertEqual(self.connector.call_count,1)
        self.assertEqual(self.connections[0].isolation_level,'READ_COMMITTED')
        self.assertEqual(sum(self.weights),80)

    def test_unknown_commit_discards_connection_without_retry_or_permit(self):
        self.budget.acquire('/info',{'type':'meta'})
        self.connections[0].fail_commit=True
        with self.assertRaises(budget.BudgetError) as caught:
            self.budget.acquire('/info',{'type':'meta'})
        self.assertEqual(caught.exception.stage,'COMMIT')
        self.assertNotIn('PRIVATE_VALUE',str(caught.exception))
        self.assertTrue(self.connections[0].closed)
        self.assertEqual(self.connector.call_count,1)
        self.budget.acquire('/info',{'type':'meta'})
        self.assertEqual(self.connector.call_count,2)

    def test_quota_denial_rolls_back_without_reconnecting_or_overspending(self):
        self.weights.append(800)
        with self.assertRaisesRegex(budget.BudgetError,'EXHAUSTED') as caught:
            self.budget.acquire('/info',{'type':'meta'})
        self.assertEqual((caught.exception.used_weight,caught.exception.requested_weight,
                          caught.exception.ceiling,caught.exception.retry_after_ms),
                         (800,20,800,budget.WINDOW_MS))
        self.budget.acquire('/info',{'type':'meta'},priority='protection')
        self.assertEqual(self.connector.call_count,1)
        self.assertEqual(sum(self.weights),820)

    def test_expired_local_wait_cannot_start_a_database_transaction(self):
        coordinator=budget._coordinator(self.journal)
        coordinator.lock.acquire()
        try:
            with self.assertRaises(budget.BudgetError) as caught:
                with self.budget._transaction(deadline_ns=time.monotonic_ns()+20_000_000):pass
            self.assertEqual(caught.exception.stage,'LOCAL_WAIT')
            self.connector.assert_not_called()
        finally:coordinator.lock.release()

    def test_read_only_capacity_does_not_spend_or_refresh_requests(self):
        self.weights.extend([500,300])
        status=self.budget.capacity(requested_weight=248)
        self.assertFalse(status['eligible'])
        self.assertEqual(status['used_weight'],800)
        self.assertEqual(status['retry_after_ms'],budget.WINDOW_MS)
        protected=self.budget.capacity(requested_weight=248,priority='protection')
        self.assertTrue(protected['eligible'])
        self.assertEqual(protected['retry_after_ms'],0)
        self.assertEqual(self.weights,[500,300])
        self.assertEqual(self.connector.call_count,1)


class CapacityTests(unittest.TestCase):
    def capacity(self,rows,request=248,ceiling=800):
        conn=Mock()
        conn.execute.return_value.fetchall.return_value=rows
        return budget._quota_capacity(conn,request,ceiling)

    def test_entry_waits_until_enough_cumulative_capacity_expires(self):
        # Earliest expiry alone is too small for the exact finite entry plan.
        status=self.capacity([(100,1000),(500,25000),(200,60000)])
        self.assertEqual((status['used_weight'],status['eligible'],status['retry_after_ms']),
                         (800,False,25000))
        status=self.capacity([(100,1000),(500,25000),(200,60000)],ceiling=1200)
        self.assertEqual((status['eligible'],status['retry_after_ms']),(True,0))

    def test_empty_ledger_immediately_eligible_and_oversized_plan_has_no_wait(self):
        self.assertEqual(self.capacity([])['retry_after_ms'],0)
        status=self.capacity([],request=801)
        self.assertFalse(status['eligible'])
        self.assertIsNone(status['retry_after_ms'])

    def test_invalid_ledger_fails_closed_with_no_private_values(self):
        for rows in ([(1200,1),(1,2)],[(1,0)],[(1,69001)],[(True,100)],[(1,'PRIVATE_VALUE')]):
            with self.assertRaises(budget.BudgetError) as caught:self.capacity(rows)
            self.assertNotIn('PRIVATE_VALUE',str(caught.exception))

    def test_invalid_capacity_request_or_priority_never_opens_database(self):
        owner=budget.Budget(pg.PostgresJournal.for_ci(
            'postgresql://offline:PRIVATE_VALUE@localhost/hl_journal_ci'))
        with patch.object(owner,'_transaction') as transaction:
            for request in (0,True,1201,'PRIVATE_VALUE'):
                with self.assertRaisesRegex(budget.BudgetError,'WEIGHT_INVALID'):
                    owner.capacity(requested_weight=request)
            for priority in ('PRIVATE_VALUE',None,[]):
                with self.assertRaisesRegex(budget.BudgetError,'PRIORITY_INVALID'):
                    owner.capacity(priority=priority)
            transaction.assert_not_called()

    def test_fresh_entry_retry_uses_expiry_and_grades_only_unknown_delays(self):
        denied=budget.BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED',
            requested_weight=248,used_weight=800,ceiling=800,retry_after_ms=25000)
        self.assertEqual(budget.fresh_entry_retry_ms(denied),25000)
        busy=budget.BudgetError('TESTNET_REQUEST_BUDGET_BUSY')
        self.assertEqual([budget.fresh_entry_retry_ms(busy,consecutive_failures=n)
                          for n in (1,2,3,6,10000)],[500,1000,2000,10000,10000])
        impossible=budget.BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED',
            requested_weight=801,ceiling=800)
        self.assertIsNone(budget.fresh_entry_retry_ms(impossible))

    def test_backoff_never_classifies_storage_or_unknown_order_outcome_for_retry(self):
        for error in (budget.BudgetError('TESTNET_SHARED_REQUEST_BUDGET_UNAVAILABLE'),
                      ValueError('UNCERTAIN_REQUIRES_REVIEW')):
            with self.assertRaisesRegex(budget.BudgetError,'BACKOFF_REASON_INVALID'):
                budget.fresh_entry_retry_ms(error)
        for count in (0,True,10001,'PRIVATE_VALUE'):
            with self.assertRaisesRegex(budget.BudgetError,'BACKOFF_COUNT_INVALID'):
                budget.fresh_entry_retry_ms(budget.BudgetError('TESTNET_REQUEST_BUDGET_BUSY'),
                                            consecutive_failures=count)


class PureTests(unittest.TestCase):
    def test_documented_weights_and_conservative_fill_upper_bound(self):
        for kind in budget.LIGHT:
            self.assertEqual(budget.request_weight('/info', {'type': kind}), 2)
        self.assertEqual(budget.request_weight('/info', {'type': 'userRole'}), 60)
        for kind in budget.NORMAL:
            self.assertEqual(budget.request_weight('/info', {'type': kind}), 20)
        for kind in budget.SIZED:
            self.assertEqual(budget.request_weight('/info', {'type': kind}), 120)
        for length, expected in ((1, 1), (39, 1), (40, 2), (79, 2), (80, 3)):
            for kind, key in (('order', 'orders'), ('cancel', 'cancels'),
                              ('batchModify', 'modifies'), ('cancelByCloid', 'cancels')):
                self.assertEqual(budget.request_weight('/exchange',
                    {'action': {'type': kind, key: [{}] * length}}), expected)

    def test_other_host_unknown_types_and_unbounded_bodies_fail_closed(self):
        cases = (('/info', {'type': 'unknown'}), ('/info', {'type': []}),
                 ('/foo', {}), ('/info', []), ('/exchange', {'action': {'type': 'withdraw3'}}),
                 ('/exchange', {'action': {'type': []}}),
                 ('/exchange', {'action': {'type': 'order', 'orders': []}}),
                 ('/exchange', {'action': {'type': 'order', 'orders': [{}] * 1001}}))
        for path, body in cases:
            with self.assertRaises(budget.BudgetError):
                budget.request_weight(path, body)
        with self.assertRaises(budget.BudgetError):
            budget.request_weight('/info', {'type': 'meta'}, host='api.hyperliquid.xyz')

    def test_only_unconfigured_isolated_readonly_tools_are_optional(self):
        self.assertIsNone(budget.Budget.from_env({}))
        self.assertIsNone(budget.Budget.from_env({'HL_TESTNET_RUNTIME_MODE': 'read_only'}))
        for env in ({'HL_TESTNET_JOURNAL_BACKEND': 'staging_postgres_v1'},
                    {'HL_TESTNET_RUNTIME_MODE': 'long_stream_testnet_v1'},
                    {'RENDER_SERVICE_ID': 'srv-dakptbh594qs7395460g'},
                    {'HL_TESTNET_DATABASE_URL': 'DO_NOT_EXPOSE'}):
            with self.assertRaises(budget.BudgetError) as caught:
                budget.Budget.from_env(env)
            self.assertNotIn('DO_NOT_EXPOSE', str(caught.exception))

    def permit(self, kind='userFillsByTime', *, deadline=None):
        owner = Mock()
        owner._settle.return_value = True
        return budget.Permit(owner, 'a' * 32, kind, 120,
            time.monotonic_ns() + 1000000000 if deadline is None else deadline), owner

    def test_one_permit_cannot_authorize_two_transports_even_concurrently(self):
        permit, _ = self.permit()
        def consume(_):
            try:
                permit.check()
                return True
            except budget.BudgetError:
                return False
        with ThreadPoolExecutor(max_workers=4) as pool:
            self.assertEqual(sum(pool.map(consume, range(12))), 1)

    def test_expiry_cannot_reset_time_or_refund_an_unsent_request(self):
        permit, owner = self.permit(deadline=time.monotonic_ns() - 1)
        with self.assertRaisesRegex(budget.BudgetError, 'PERMIT_EXPIRED'):
            permit.check()
        self.assertFalse(permit.finish([]))
        owner._settle.assert_not_called()


    def test_refund_uses_ceiling_including_incomplete_twenty_item_group(self):
        for count, weight in ((0, 20), (1, 21), (20, 21), (21, 22), (2000, 120)):
            permit, owner = self.permit()
            permit.check()
            self.assertTrue(permit.finish([{}] * count))
            owner._settle.assert_called_once_with('a' * 32, weight)

    def test_invalid_or_oversized_response_never_refunds(self):
        for response in (None, {}, 'INVALID', [{}] * 2001, [1]):
            permit, owner = self.permit()
            permit.check()
            self.assertFalse(permit.finish(response))
            owner._settle.assert_not_called()
        permit, owner = self.permit(kind='clearinghouseState')
        permit.check()
        self.assertFalse(permit.finish({}))
        owner._settle.assert_not_called()


class ObservationBatchTests(unittest.TestCase):
    def batch(self,bodies=None):
        owner=Mock()
        bodies=observation_bodies() if bodies is None else bodies
        entries=[(budget._observation_key(body),format(i+1,'032x'),
                  budget.request_weight('/info',body)) for i,body in enumerate(bodies)]
        return budget.ObservationBatch(owner,entries,'protection',
            time.monotonic_ns()+budget.OBSERVATION_MS*1000000),owner

    def test_exact_duplicates_can_be_claimed_only_declared_times_concurrently(self):
        body=observation_bodies()[0];batch,owner=self.batch([body,body])
        def claim(_):
            try:
                permit=batch.acquire('/info',body,priority='protection')
                permit.check();return True
            except budget.BudgetError:return False
        with ThreadPoolExecutor(max_workers=4) as pool:
            self.assertEqual(sum(pool.map(claim,range(12))),2)
        self.assertEqual(owner._claim_observation.call_count,2)
        self.assertEqual(len({c.args[0] for c in owner._claim_observation.call_args_list}),2)

    def test_altered_body_host_path_priority_and_unplanned_pagination_never_claim(self):
        batch,owner=self.batch();body=observation_bodies()[0]
        for altered in ({**body,'startTime':999},{**body,'user':'0x'+'2'*40},
                        {**body,'endTime':2001}):
            with self.assertRaisesRegex(budget.BudgetError,'UNDECLARED'):
                batch.acquire('/info',altered,priority='protection')
        for path,host,priority in (('/exchange',budget.HOST,'protection'),
                ('/info','api.hyperliquid.xyz','protection'),
                ('/info',budget.HOST,'background')):
            with self.assertRaisesRegex(budget.BudgetError,'MISMATCH'):
                batch.acquire(path,body,priority=priority,host=host)
        owner._claim_observation.assert_not_called()

    def test_failed_or_uncertain_credit_claim_is_burned_without_retry(self):
        body=observation_bodies()[0];batch,owner=self.batch([body])
        owner._claim_observation.side_effect=budget.BudgetError('TESTNET_SHARED_REQUEST_BUDGET_UNAVAILABLE')
        with self.assertRaisesRegex(budget.BudgetError,'UNAVAILABLE'):
            batch.acquire('/info',body,priority='protection')
        with self.assertRaisesRegex(budget.BudgetError,'UNDECLARED'):
            batch.acquire('/info',body,priority='protection')
        owner._claim_observation.assert_called_once()
        owner._settle.assert_not_called()

    def test_child_clock_begins_before_sql_and_never_restarts_after_claim(self):
        body=observation_bodies()[0];clock=Mock(return_value=1000000000)
        with patch.object(budget.time,'monotonic_ns',clock):
            batch,owner=self.batch([body])
            def slow_claim(token,weight,deadline):
                self.assertEqual(deadline,2000000000)
                clock.return_value=2000000001
            owner._claim_observation.side_effect=slow_claim
            with self.assertRaisesRegex(budget.BudgetError,'PERMIT_EXPIRED'):
                batch.acquire('/info',body,priority='protection')
        owner._settle.assert_not_called()

    def test_close_releases_only_unclaimed_children_and_fences_issued_permits(self):
        body=observation_bodies()[0];batch,owner=self.batch([body,body])
        with patch.object(budget.time,'monotonic_ns',return_value=batch._deadline_ns-100):
            permit=batch.acquire('/info',body,priority='protection')
            self.assertEqual(permit._deadline_ns,batch._deadline_ns)
        batch.close()
        with self.assertRaisesRegex(budget.BudgetError,'CLOSED'):permit.check()
        with self.assertRaisesRegex(budget.BudgetError,'CLOSED'):
            batch.acquire('/info',body,priority='protection')
        owner._settle.assert_not_called()
        owner._release_unclaimed_observation.assert_called_once_with([(format(1,'032x'),120)])
        batch.close()
        owner._release_unclaimed_observation.assert_called_once()

    def test_close_retains_failed_and_unknown_claims_without_retry(self):
        body=observation_bodies()[0];batch,owner=self.batch([body])
        owner._claim_observation.side_effect=budget.BudgetError('TESTNET_SHARED_REQUEST_BUDGET_UNAVAILABLE')
        with self.assertRaisesRegex(budget.BudgetError,'UNAVAILABLE'):
            batch.acquire('/info',body,priority='protection')
        batch.close()
        owner._release_unclaimed_observation.assert_not_called()
        owner._claim_observation.assert_called_once()

    def test_close_racing_claim_burns_popped_ticket_and_releases_only_untouched_sibling(self):
        body=observation_bodies()[0];batch,owner=self.batch([body,body])
        claimed=threading.Event();release=threading.Event()
        def pause_claim(*args):
            claimed.set()
            self.assertTrue(release.wait(2))
        owner._claim_observation.side_effect=pause_claim
        with ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(batch.acquire,'/info',body,priority='protection')
            self.assertTrue(claimed.wait(2))
            try:
                batch.close()
                owner._release_unclaimed_observation.assert_called_once_with([(format(1,'032x'),120)])
            finally:release.set()
            with self.assertRaisesRegex(budget.BudgetError,'CLOSED'):future.result(timeout=2)
        owner._claim_observation.assert_called_once()

    def test_close_winning_claim_race_sends_no_claim_sql(self):
        body=observation_bodies()[0];batch,owner=self.batch([body])
        batch.close()
        with self.assertRaisesRegex(budget.BudgetError,'CLOSED'):
            batch.acquire('/info',body,priority='protection')
        owner._claim_observation.assert_not_called()
        owner._release_unclaimed_observation.assert_called_once_with([(format(1,'032x'),120)])

    def test_extension_with_unknown_commit_during_close_never_releases_its_unknown_tokens(self):
        parent=observation_bodies()[0];batch,owner=self.batch([parent,parent])
        batch.acquire('/info',parent,priority='protection').check()
        children=[{**parent,'endTime':1500},{**parent,'startTime':1501}]
        def lost_extension(*args):
            batch.close()
            raise budget.BudgetError('TESTNET_SHARED_REQUEST_BUDGET_UNAVAILABLE')
        owner._fund_observation.side_effect=lost_extension
        with self.assertRaisesRegex(budget.BudgetError,'UNAVAILABLE'):batch.reserve_extra(children)
        owner._release_unclaimed_observation.assert_called_once_with([(format(1,'032x'),120)])
        owner._fund_observation.assert_called_once()
        batch.close()
        owner._release_unclaimed_observation.assert_called_once()

    def test_expired_or_forked_batch_does_not_touch_sql_or_inherited_lock(self):
        body=observation_bodies()[0];batch,owner=self.batch([body])
        batch._deadline_ns=time.monotonic_ns()-1
        with self.assertRaisesRegex(budget.BudgetError,'EXPIRED'):
            batch.acquire('/info',body,priority='protection')
        batch._pid=-1;batch._lock.acquire()
        try:
            with self.assertRaisesRegex(budget.BudgetError,'PROCESS_CHANGED'):
                batch.acquire('/info',body,priority='protection')
            batch.close()
        finally:batch._lock.release()
        owner._claim_observation.assert_not_called()
        owner._release_unclaimed_observation.assert_not_called()

    def test_forked_permit_cannot_send_or_refund_parent_charge(self):
        body=observation_bodies()[0];batch,owner=self.batch([body])
        permit=batch.acquire('/info',body,priority='protection');permit.check()
        permit._used[0]=False
        with patch.object(budget.os,'getpid',return_value=-1):
            with self.assertRaisesRegex(budget.BudgetError,'PROCESS_CHANGED'):permit.check()
            self.assertFalse(permit.finish([]))
        owner._settle.assert_not_called()

    def test_both_passes_can_extend_their_own_claimed_parent_and_nested_children(self):
        parent=observation_bodies()[0];batch,owner=self.batch([parent,parent])
        left={**parent,'endTime':1500};right={**parent,'startTime':1501}
        for _ in range(2):
            batch.acquire('/info',parent,priority='protection').check()
            batch.reserve_extra([left,right])
            batch.acquire('/info',left,priority='protection').check()
            batch.acquire('/info',right,priority='protection').check()
        self.assertEqual(owner._fund_observation.call_count,2)
        with self.assertRaisesRegex(budget.BudgetError,'EXTENSION_UNDECLARED'):
            batch.reserve_extra([left,right])
        nested=[{**left,'endTime':1250},{**left,'startTime':1251}]
        batch.reserve_extra(nested)
        self.assertEqual(owner._fund_observation.call_count,3)

    def test_failed_extension_cannot_retry_or_issue_unfunded_children(self):
        parent=observation_bodies()[0];batch,owner=self.batch([parent])
        batch.acquire('/info',parent,priority='protection').check()
        children=[{**parent,'endTime':1500},{**parent,'startTime':1501}]
        owner._fund_observation.side_effect=budget.BudgetError('TESTNET_SHARED_REQUEST_BUDGET_UNAVAILABLE')
        with self.assertRaisesRegex(budget.BudgetError,'UNAVAILABLE'):batch.reserve_extra(children)
        with self.assertRaisesRegex(budget.BudgetError,'EXTENSION_UNDECLARED'):batch.reserve_extra(children)
        with self.assertRaisesRegex(budget.BudgetError,'UNDECLARED_REQUEST'):
            batch.acquire('/info',children[0],priority='protection')
        owner._fund_observation.assert_called_once()

    def test_extension_requires_exact_claimed_parent_and_existing_total_read_bound(self):
        parent=observation_bodies()[0];batch,owner=self.batch([parent])
        children=[{**parent,'endTime':1500},{**parent,'startTime':1501}]
        with self.assertRaisesRegex(budget.BudgetError,'EXTENSION_UNDECLARED'):batch.reserve_extra(children)
        batch.acquire('/info',parent,priority='protection').check()
        for invalid in ([children[0],{**children[1],'startTime':1500}],
                [children[0],{**children[1],'user':'0x'+'2'*40}],
                [children[0],{**children[1],'aggregateByTime':True}]):
            with self.assertRaisesRegex(budget.BudgetError,'EXTENSION_INVALID'):batch.reserve_extra(invalid)
        batch._count=199
        with self.assertRaisesRegex(budget.BudgetError,'EXTENSION_UNDECLARED'):batch.reserve_extra(children)
        owner._fund_observation.assert_not_called()



class InfoPlanTests(unittest.TestCase):
    def setUp(self):
        self.owner=budget.Budget(pg.PostgresJournal.for_ci(
            'postgresql://offline:PRIVATE_VALUE@localhost/hl_journal_ci'))
        self.funding=patch.object(self.owner,'_fund_observation')
        self.fund=self.funding.start();self.addCleanup(self.funding.stop)

    def test_complete_preflight_is_one_exact_finite_admission_before_any_child(self):
        bodies=entry_info_bodies()
        plan=self.owner.reserve_info_plan(bodies)
        self.assertIs(type(plan),budget.InfoPlan)
        self.assertEqual(sum(weight for _,_,weight in self.fund.call_args.args[0]),246)
        self.assertEqual(self.fund.call_args.args[1],'background')
        with patch.object(self.owner,'_claim_observation') as claim, \
                patch.object(self.owner,'_release_unclaimed_observation') as release:
            for body in bodies:
                plan.acquire('/info',body).check()
            self.assertEqual(claim.call_count,len(bodies))
            with self.assertRaisesRegex(budget.BudgetError,'UNDECLARED'):
                plan.acquire('/info',bodies[0])
            plan.close()
            release.assert_not_called()

    def test_info_and_lifecycle_plans_do_not_widen_each_others_request_types(self):
        invalid=[dict(type='userFillsByTime',user='0x'+'1'*40,startTime=1,endTime=2,
                      aggregateByTime=False),dict(type='orderStatus',user='0x'+'1'*40,oid=1),
                 dict(type='userRole',user='0x'+'0'*40),dict(type='meta',user='0x'+'1'*40),
                 dict(type='activeAssetData',user='0x'+'1'*40,coin='btc'),
                 dict(type='userRole',user='0x'+'1'*40,extra='PRIVATE_VALUE'),
                 dict(type=['meta'])]
        for body in invalid:
            with self.subTest(body=body),self.assertRaises(budget.BudgetError):
                self.owner.reserve_info_plan([body])
        with self.assertRaisesRegex(budget.BudgetError,'OBSERVATION_BATCH_TYPE'):
            self.owner.reserve_observation([dict(type='meta')])
        self.fund.assert_not_called()

    def test_altered_exact_body_or_host_priority_and_extension_have_no_claim(self):
        plan=self.owner.reserve_info_plan(entry_info_bodies())
        with patch.object(self.owner,'_claim_observation') as claim:
            with self.assertRaisesRegex(budget.BudgetError,'UNDECLARED'):
                plan.acquire('/info',dict(type='activeAssetData',user='0x'+'1'*40,coin='ETH'))
            for path,host,priority in (('/exchange',budget.HOST,'background'),
                    ('/info','api.hyperliquid.xyz','background'),
                    ('/info',budget.HOST,'protection')):
                with self.assertRaisesRegex(budget.BudgetError,'MISMATCH'):
                    plan.acquire(path,dict(type='meta'),host=host,priority=priority)
            with self.assertRaisesRegex(budget.BudgetError,'EXTENSION_NOT_ALLOWED'):
                plan.reserve_extra(observation_bodies()[:2])
            claim.assert_not_called()


class TransportAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.network = patch('http.client.HTTPSConnection', side_effect=AssertionError('NO_HTTP_BEFORE_ADMISSION'))
        self.http = self.network.start()
        self.addCleanup(self.network.stop)

    def denied(self):
        denied = Mock()
        denied.acquire.side_effect = budget.BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED')
        return denied

    def test_legacy_entry_denied_without_recording_an_http_attempt(self):
        import hyperliquid_testnet_executor as sender
        from hyperliquid_testnet_executor_selftest import action
        client = sender.TestnetHTTP(allow_orders=True, budget=self.denied())
        with self.assertRaisesRegex(sender.TestnetError, 'BUDGET_EXHAUSTED'):
            client._post('/exchange', {'action': action()})
        self.assertEqual(client.order_attempts, 0)
        self.http.assert_not_called()

    def test_legacy_exchange_requires_budget_even_in_isolated_unconfigured_process(self):
        import hyperliquid_testnet_executor as sender
        from hyperliquid_testnet_executor_selftest import action
        with patch.object(budget.Budget, 'from_env', return_value=None):
            client = sender.TestnetHTTP(allow_orders=True)
        with self.assertRaisesRegex(sender.TestnetError, 'SHARED_REQUEST_BUDGET_REQUIRED'):
            client._post('/exchange', {'action': action()})
        self.assertEqual(client.order_attempts, 0)
        self.http.assert_not_called()

    def test_all_public_raw_transport_classes_admit_before_http(self):
        from . import checks, card_sync_evidence, account_ledger_review, doge_lifecycle_review
        account = '0x' + '1' * 40
        cases = (
            (checks.InfoReader(budget=self.denied()), lambda reader: reader.read('meta')),
            (card_sync_evidence.PublicReader(budget=self.denied()),
             lambda reader: reader.read('clearinghouseState', account)),
            (account_ledger_review.HistoryReader(budget=self.denied()),
             lambda reader: reader.read('userFillsByTime', account, 1, 2)),
            (doge_lifecycle_review.PublicReader(budget=self.denied()),
             lambda reader: reader.read('clearinghouseState', account)),
        )
        for reader, call in cases:
            with self.assertRaisesRegex(ValueError, 'BUDGET_EXHAUSTED'):
                call(reader)
        self.http.assert_not_called()

@unittest.skipUnless(CI_URL, 'Requires disposable PostgreSQL CI service')
class PostgresTests(unittest.TestCase):
    def setUp(self):
        self.journal = pg.PostgresJournal.for_ci(CI_URL)
        with self.journal._transaction() as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS {pg.SCHEMA} CASCADE')
        self.journal.bootstrap()
        self.budget = budget.Budget(self.journal)
        self.budget.initialize()

    def weights(self):
        with self.journal._transaction() as conn:
            return conn.execute(f'SELECT COALESCE(sum(weight),0) FROM {budget.TICKETS}').fetchone()[0]

    def fill_background(self):
        for _ in range(40):
            self.budget.acquire('/info', {'type': 'meta'})
        self.assertEqual(self.weights(), 800)

    def test_background_cannot_consume_reserved_protection_capacity(self):
        self.fill_background()
        other = budget.Budget(pg.PostgresJournal.for_ci(CI_URL))
        with self.assertRaisesRegex(budget.BudgetError, 'EXHAUSTED'):
            other.acquire('/info', {'type': 'clearinghouseState'})
        for _ in range(20):
            other.acquire('/info', {'type': 'meta'}, priority='protection')
        self.assertEqual(self.weights(), 1200)
        with self.assertRaisesRegex(budget.BudgetError, 'EXHAUSTED'):
            self.budget.acquire('/exchange', {'action': {'type': 'cancel', 'cancels': [{}]}},
                                priority='protection')

    def test_capacity_and_denial_preserve_ledger_and_report_cumulative_expiry(self):
        with self.journal._transaction() as conn:
            for token,weight,age in (('1'*32,100,68000),('2'*32,500,44000),('3'*32,200,9000)):
                conn.execute(f'''INSERT INTO {budget.TICKETS}(token,weight,admitted_at)
                    VALUES(%s,%s,clock_timestamp()-(%s*interval '1 millisecond'))''',
                    (token,weight,age))
            before=conn.execute(f'''SELECT token,admitted_at,weight,settled
                FROM {budget.TICKETS} ORDER BY token''').fetchall()
        capacity=self.budget.capacity(requested_weight=248)
        self.assertEqual(capacity['used_weight'],800)
        self.assertFalse(capacity['eligible'])
        self.assertTrue(23000<=capacity['retry_after_ms']<=25000)
        protective=self.budget.capacity(requested_weight=248,priority='protection')
        self.assertTrue(protective['eligible'])
        self.assertEqual(protective['retry_after_ms'],0)
        with self.assertRaisesRegex(budget.BudgetError,'EXHAUSTED') as caught:
            self.budget.reserve_info_plan(entry_info_bodies())
        self.assertEqual((caught.exception.used_weight,caught.exception.requested_weight,
                          caught.exception.ceiling),(800,246,800))
        self.assertTrue(23000<=caught.exception.retry_after_ms<=25000)
        with self.journal._transaction() as conn:
            after=conn.execute(f'''SELECT token,admitted_at,weight,settled
                FROM {budget.TICKETS} ORDER BY token''').fetchall()
        self.assertEqual(before,after)

    def test_concurrent_workers_never_overspend_and_busy_is_bounded(self):
        self.fill_background()
        with self.journal._transaction() as conn:
            conn.execute(f'UPDATE {budget.TICKETS} SET weight=weight-1')
        self.assertEqual(self.weights(), 760)
        def admit(_):
            try:
                budget.Budget(pg.PostgresJournal.for_ci(CI_URL)).acquire('/info', {'type': 'meta'})
                return True
            except budget.BudgetError as exc:
                self.assertIn(str(exc), ('TESTNET_REQUEST_BUDGET_BUSY', 'TESTNET_REQUEST_BUDGET_EXHAUSTED'))
                return False
        with ThreadPoolExecutor(max_workers=8) as pool:
            granted = sum(pool.map(admit, range(24)))
        self.assertLessEqual(granted, 2)
        self.assertEqual(self.weights(), 760 + 20 * granted)

    def test_new_process_shares_old_admissions_without_reinitializing_or_reading_keys(self):
        self.fill_background()
        program = '''import os
from hl_testnet_runtime.postgres_journal import PostgresJournal
from hl_testnet_runtime.request_budget import Budget,BudgetError
b=Budget(PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL']))
try: b.acquire('/info',{'type':'meta'})
except BudgetError as e: assert str(e)=='TESTNET_REQUEST_BUDGET_EXHAUSTED'
else: raise AssertionError('Separate process overspent shared IP allowance')
b.acquire('/exchange',{'action':{'type':'cancel','cancels':[{}]}},priority='protection')
'''
        subprocess.run([sys.executable, '-c', program], check=True, timeout=10,
                       env={'HL_JOURNAL_CI_URL': CI_URL, 'PATH': os.environ.get('PATH', '')})
        self.assertEqual(self.weights(), 801)

    def test_dynamic_size_refund_commits_once_and_lost_refund_retains_full_charge(self):
        permit = self.budget.acquire('/info', {'type': 'userFillsByTime'}, priority='protection')
        permit.check()
        self.assertEqual(self.weights(), 120)
        self.assertTrue(permit.finish([{}] * 21))
        self.assertEqual(self.weights(), 22)
        self.assertFalse(permit.finish([]))
        self.assertEqual(self.weights(), 22)
        permit = self.budget.acquire('/info', {'type': 'userFillsByTime'}, priority='protection')
        permit.check()
        with patch.object(self.budget, '_transaction', side_effect=budget.BudgetError('TESTNET_SHARED_REQUEST_BUDGET_UNAVAILABLE')):
            self.assertFalse(permit.finish([]))
        self.assertEqual(self.weights(), 142)

    def test_uncertain_admission_commit_returns_no_permit(self):
        original = self.budget._transaction
        @contextmanager
        def lost_ack(**kwargs):
            with original(**kwargs) as conn:
                yield conn
            raise budget.BudgetError('TESTNET_SHARED_REQUEST_BUDGET_UNAVAILABLE')
        with patch.object(self.budget, '_transaction', lost_ack):
            with self.assertRaises(budget.BudgetError):
                self.budget.acquire('/info', {'type': 'meta'})
        self.assertEqual(self.weights(), 20)

    def test_bounded_lock_does_not_wait_for_quota_or_another_process(self):
        with self.journal._transaction() as conn:
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (budget.LOCK,))
            started = time.monotonic()
            with self.assertRaisesRegex(budget.BudgetError, 'BUDGET_BUSY'):
                self.budget.acquire('/info', {'type': 'meta'}, priority='protection')
            self.assertLess(time.monotonic() - started, 2)
        self.assertEqual(self.weights(), 0)

    def test_four_parallel_read_admissions_have_no_spurious_denial_with_headroom(self):
        from threading import Barrier
        barrier = Barrier(4)
        def admit(_):
            local = budget.Budget(pg.PostgresJournal.for_ci(CI_URL))
            barrier.wait(timeout=2)
            return local.acquire('/info', {'type': 'meta'}, priority='protection')
        with ThreadPoolExecutor(max_workers=4) as pool:
            permits = list(pool.map(admit, range(4)))
        self.assertEqual(len(permits), 4)
        self.assertEqual(self.weights(), 80)

    def test_missing_or_changed_schema_is_not_repaired_by_admission(self):
        with self.journal._transaction() as conn:
            conn.execute(f'UPDATE {budget.POLICY} SET maximum=1201')
        with self.assertRaisesRegex(budget.BudgetError, 'SCHEMA_REQUIRES_REVIEW'):
            self.budget.acquire('/info', {'type': 'meta'})
        with self.journal._transaction() as conn:
            conn.execute(f'DROP TABLE {budget.POLICY}')
        with self.assertRaisesRegex(budget.BudgetError, 'UNAVAILABLE'):
            self.budget.acquire('/info', {'type': 'meta'})
        with self.journal._transaction() as conn:
            self.assertIsNone(conn.execute('SELECT to_regclass(%s)', (budget.POLICY,)).fetchone()[0])

    def test_rolling_window_keeps_60_seconds_plus_admission_tcp_and_tls_margin(self):
        self.fill_background()
        with self.journal._transaction() as conn:
            conn.execute(f"UPDATE {budget.TICKETS} SET admitted_at=clock_timestamp()-interval '68 seconds'")
        with self.assertRaisesRegex(budget.BudgetError, 'EXHAUSTED'):
            self.budget.acquire('/info', {'type': 'meta'})
        with self.journal._transaction() as conn:
            conn.execute(f"UPDATE {budget.TICKETS} SET admitted_at=clock_timestamp()-interval '69.1 seconds'")
        self.budget.acquire('/info', {'type': 'meta'})
        self.assertEqual(self.weights(), 20)

    def test_slow_commit_does_not_restart_freshness_clock(self):
        original=self.budget._transaction
        clock=Mock(return_value=1000000000)
        @contextmanager
        def slow_commit(**kwargs):
            with original(**kwargs) as conn:yield conn
            clock.return_value=2000000001
        with patch.object(budget.time,'monotonic_ns',clock), \
                patch.object(self.budget,'_transaction',slow_commit):
            with self.assertRaisesRegex(budget.BudgetError, 'PERMIT_EXPIRED'):
                self.budget.acquire('/info', {'type': 'meta'})
        self.assertEqual(self.weights(), 20)

    @contextmanager
    def delayed_connection(self,delay=0.025):
        coordinator=budget._coordinator(self.journal)
        original=coordinator.connection
        class Delayed:
            statements=0
            commits=0
            def __getattr__(self,name):return getattr(original,name)
            def execute(self,*args,**kwargs):
                self.statements+=1;time.sleep(delay)
                return original.execute(*args,**kwargs)
            def commit(self):
                self.commits+=1;time.sleep(delay);return original.commit()
        delayed=Delayed();coordinator.connection=delayed
        try:yield delayed
        finally:
            if coordinator.connection is delayed:coordinator.connection=original

    def test_delayed_real_pg_four_parallel_admissions_have_one_connection_and_no_lock_failures(self):
        barrier=threading.Barrier(4)
        def admit(_):
            barrier.wait(timeout=2)
            permit=budget.Budget(pg.PostgresJournal.for_ci(CI_URL)).acquire('/info',{'type':'meta'})
            permit.check();return permit
        with self.delayed_connection() as connection,ThreadPoolExecutor(max_workers=4) as pool:
            self.assertEqual(len(list(pool.map(admit,range(4)))),4)
        self.assertEqual(connection.statements,12)
        self.assertEqual(connection.commits,4)
        self.assertEqual(self.weights(),80)

    def test_delayed_real_pg_race_preserves_background_and_protective_ceilings(self):
        with self.journal._transaction() as conn:
            conn.execute(f'''INSERT INTO {budget.TICKETS}(token,weight)
                SELECT md5(i::text),20 FROM generate_series(1,38) i''')
        def admit(_):
            try:self.budget.acquire('/info',{'type':'meta'});return True
            except budget.BudgetError as exc:
                self.assertEqual(str(exc),'TESTNET_REQUEST_BUDGET_EXHAUSTED');return False
        with self.delayed_connection(),ThreadPoolExecutor(max_workers=4) as pool:
            self.assertEqual(sum(pool.map(admit,range(4))),2)
        self.assertEqual(self.weights(),800)
        for _ in range(20):self.budget.acquire('/info',{'type':'meta'},priority='protection')
        with self.assertRaisesRegex(budget.BudgetError,'EXHAUSTED'):
            self.budget.acquire('/info',{'type':'clearinghouseState'},priority='protection')
        self.assertEqual(self.weights(),1200)

    def test_sql_failure_discards_cached_connection_and_next_call_reconnects_once(self):
        import psycopg
        coordinator=budget._coordinator(self.journal)
        previous=coordinator.connection
        with self.assertRaises(budget.BudgetError):
            with self.budget._transaction() as conn:conn.execute('SELECT 1/0')
        self.assertTrue(previous.closed);self.assertIsNone(coordinator.connection)
        with patch.object(psycopg,'connect',wraps=psycopg.connect) as connect:
            self.budget.acquire('/info',{'type':'meta'})
            self.budget.acquire('/info',{'type':'meta'})
        self.assertEqual(connect.call_count,1)

    def test_accounting_snapshot_after_wait_sees_other_connections_committed_charge(self):
        waiting=threading.Event();coordinator=budget._coordinator(self.journal)
        original=coordinator.connection
        class Signaled:
            def __getattr__(self,name):return getattr(original,name)
            def execute(self,sql,*args):
                if 'pg_advisory_xact_lock' in sql:waiting.set()
                return original.execute(sql,*args)
        coordinator.connection=Signaled()
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                with self.journal._transaction() as conn:
                    conn.execute('SELECT pg_advisory_xact_lock(%s)',(budget.LOCK,))
                    conn.execute(f'''INSERT INTO {budget.TICKETS}(token,weight)
                        SELECT md5(i::text),20 FROM generate_series(1,40) i''')
                    future=pool.submit(self.budget.acquire,'/info',{'type':'meta'})
                    self.assertTrue(waiting.wait(timeout=1));time.sleep(0.08)
                with self.assertRaisesRegex(budget.BudgetError,'EXHAUSTED'):future.result(timeout=2)
            self.assertEqual(self.weights(),800)
        finally:
            if coordinator.connection is not None:coordinator.connection=original

    @unittest.skipUnless(hasattr(os,'fork'),'POSIX fork required')
    def test_child_resets_inherited_connection_without_terminating_parent_session(self):
        parent=budget._coordinator(self.journal).connection
        child=os.fork()
        if child==0:
            try:
                self.budget.acquire('/info',{'type':'meta'}).check()
            except BaseException:os._exit(1)
            os._exit(0)
        _,status=os.waitpid(child,0)
        self.assertEqual(os.waitstatus_to_exitcode(status),0)
        self.budget.acquire('/info',{'type':'meta'}).check()
        self.assertIs(budget._coordinator(self.journal).connection,parent)
        self.assertFalse(parent.closed)
        self.assertEqual(self.weights(),40)

    def test_partial_starvation_ledger_denies_whole_observation_without_partial_tickets(self):
        with self.journal._transaction() as conn:
            conn.execute(f'''INSERT INTO {budget.TICKETS}(token,weight)
                SELECT md5(i::text),20 FROM generate_series(1,50) i''')
            conn.execute(f'''INSERT INTO {budget.TICKETS}(token,weight)
                SELECT md5((i+50)::text),2 FROM generate_series(1,96) i''')
        self.assertEqual(self.weights(),1192)
        for _ in range(3):
            with self.assertRaisesRegex(budget.BudgetError,'EXHAUSTED'):
                self.budget.reserve_observation(observation_bodies(),priority='protection')
        self.assertEqual(self.weights(),1192)
        with self.journal._transaction() as conn:
            conn.execute(f"UPDATE {budget.TICKETS} SET admitted_at=clock_timestamp()-interval '69.1 seconds'")
        batch=self.budget.reserve_observation(observation_bodies(),priority='protection')
        self.assertEqual(self.weights(),292)
        def observe(body):
            permit=batch.acquire('/info',body,priority='protection');permit.check()
            if body['type']=='userFillsByTime':self.assertTrue(permit.finish([]))
        with self.delayed_connection(),ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(observe,observation_bodies()))
        batch.close()
        self.assertEqual(self.weights(),92)

    def test_quiet_live_position_funds_both_passes_after_552_weight_without_background_starvation(self):
        from . import filled_quantity_dispatch as dispatch, card_sync_evidence as evidence
        from .test_filled_quantity_dispatch import state_from_case
        from .test_card_lifecycle import T
        class Feed:
            def dirty_symbols(self,account):return ()
            def entry_allowed(self,account):return True
        state=state_from_case(q='100',stop='100',take='100')
        self.assertTrue(dispatch._fully_protected_no_work(state,T+1))
        priority=dispatch._collection_priority(state['evidence'],T+1,
            fill_wakeups=Feed(),pending_clear=True)
        bodies=evidence._planned_observation_reads(state['bindings'],state['evidence']['snapshot'],
            T,T+1,reuse_verified_terminals=True)
        self.assertEqual(sum(budget.request_weight('/info',body) for body in bodies),292)
        self.assertEqual((budget.LIMIT,budget.BACKGROUND_LIMIT),(1200,800))
        with self.journal._transaction() as conn:
            conn.execute(f'''INSERT INTO {budget.TICKETS}(token,weight)
                SELECT md5(i::text),20 FROM generate_series(1,27) i''')
            conn.execute(f'''INSERT INTO {budget.TICKETS}(token,weight)
                SELECT md5((i+27)::text),2 FROM generate_series(1,6) i''')
        self.assertEqual(self.weights(),552)
        with self.assertRaisesRegex(budget.BudgetError,'EXHAUSTED'):
            self.budget.reserve_observation(bodies,priority='background')
        self.assertEqual(self.weights(),552)
        self.budget.reserve_observation(bodies,priority=priority)
        self.assertEqual(self.weights(),844)
        self.budget.reserve_observation(bodies,priority=priority)
        self.assertEqual(self.weights(),1136)
        with self.assertRaisesRegex(budget.BudgetError,'EXHAUSTED'):
            self.budget.reserve_observation(bodies,priority=priority)
        self.assertEqual(self.weights(),1136)

    def test_batch_background_ceiling_and_other_process_share_all_reserved_children(self):
        self.budget.reserve_observation(observation_bodies())
        self.budget.reserve_observation(observation_bodies())
        self.assertEqual(self.weights(),584)
        program='''import os
from hl_testnet_runtime.postgres_journal import PostgresJournal
from hl_testnet_runtime.request_budget import Budget,BudgetError
from hl_testnet_runtime.test_request_budget import observation_bodies
b=Budget(PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL']))
try:b.reserve_observation(observation_bodies())
except BudgetError as e:assert str(e)=='TESTNET_REQUEST_BUDGET_EXHAUSTED'
else:raise AssertionError('Partial/background overspend')
b.reserve_observation(observation_bodies(),priority='protection').close()
'''
        subprocess.run([sys.executable,'-c',program],check=True,timeout=10,
            env={'HL_JOURNAL_CI_URL':CI_URL,'PATH':os.environ.get('PATH','')})
        self.assertEqual(self.weights(),584)

    def test_concurrent_whole_batches_never_partially_spend_protective_ceiling(self):
        barrier=threading.Barrier(4)
        def reserve(_):
            barrier.wait(timeout=2)
            return self.budget.reserve_observation(observation_bodies(),priority='protection')
        with self.delayed_connection(),ThreadPoolExecutor(max_workers=4) as pool:
            batches=list(pool.map(reserve,range(4)))
        self.assertEqual(len(batches),4)
        self.assertEqual(self.weights(),1168)
        with self.assertRaisesRegex(budget.BudgetError,'EXHAUSTED'):
            self.budget.reserve_observation(observation_bodies(),priority='protection')
        self.assertEqual(self.weights(),1168)

    def test_abandoned_second_pass_releases_no_http_credits_but_keeps_every_started_read(self):
        bodies=observation_bodies()
        batch=self.budget.reserve_observation(bodies,priority='protection')
        for body in bodies[:5]:
            permit=batch.acquire('/info',body,priority='protection');permit.check()
            if body['type']=='userFillsByTime':self.assertTrue(permit.finish([]))
        self.assertEqual(self.weights(),192)
        batch.close()
        self.assertEqual(self.weights(),46)
        # Repeated early failures retain real first-pass cost, not phantom
        # reservations for a verification pass that never began.
        other=self.budget.reserve_observation(bodies,priority='protection')
        self.assertEqual(self.weights(),338)
        other.close()
        self.assertEqual(self.weights(),46)

    def test_close_racing_a_slow_claim_preserves_its_ticket_and_invalidates_local_permission(self):
        body=observation_bodies()[0]
        batch=self.budget.reserve_observation([body,body],priority='protection')
        original=self.budget._claim_observation
        entered=threading.Event();release=threading.Event()
        def paused(*args):
            entered.set()
            self.assertTrue(release.wait(2))
            return original(*args)
        with patch.object(self.budget,'_claim_observation',side_effect=paused), \
                ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(batch.acquire,'/info',body,priority='protection')
            self.assertTrue(entered.wait(2))
            try:
                batch.close()
                self.assertEqual(self.weights(),120)
            finally:release.set()
            with self.assertRaisesRegex(budget.BudgetError,'CLOSED'):future.result(timeout=2)
        self.assertEqual(self.weights(),120)

    def test_failed_cleanup_keeps_conservative_charge_and_never_reopens_or_retries(self):
        batch=self.budget.reserve_observation(observation_bodies(),priority='protection')
        with patch.object(self.budget,'_transaction',side_effect=budget.BudgetError(
                'TESTNET_SHARED_REQUEST_BUDGET_UNAVAILABLE')) as transaction:
            batch.close()
            batch.close()
            transaction.assert_called_once()
        self.assertEqual(self.weights(),292)
        batch.close()
        self.assertEqual(self.weights(),292)
        with self.assertRaisesRegex(budget.BudgetError,'CLOSED'):
            batch.acquire('/info',observation_bodies()[0],priority='protection')

    def test_unknown_extension_commit_keeps_unknown_children_and_claimed_parent_after_close(self):
        parent=observation_bodies()[0]
        children=[{**parent,'endTime':1500},{**parent,'startTime':1501}]
        batch=self.budget.reserve_observation([parent,parent],priority='protection')
        batch.acquire('/info',parent,priority='protection').check()
        original=self.budget._transaction
        @contextmanager
        def lost_ack(**kwargs):
            with original(**kwargs) as conn:yield conn
            raise budget.BudgetError('TESTNET_SHARED_REQUEST_BUDGET_UNAVAILABLE')
        with patch.object(self.budget,'_transaction',lost_ack):
            with self.assertRaises(budget.BudgetError):batch.reserve_extra(children)
        self.assertEqual(self.weights(),480)
        batch.close()
        self.assertEqual(self.weights(),360)

    def test_preflight_plan_funds_all_or_none_before_background_headroom_then_retains_claimed_reads(self):
        with self.journal._transaction() as conn:
            conn.execute(f'INSERT INTO {budget.TICKETS}(token,weight) VALUES(%s,%s)',
                         ('f'*32,555))
        with self.assertRaisesRegex(budget.BudgetError,'EXHAUSTED'):
            self.budget.reserve_info_plan(entry_info_bodies())
        self.assertEqual(self.weights(),555)
        with self.journal._transaction() as conn:
            self.assertEqual(conn.execute(f'SELECT count(*) FROM {budget.TICKETS}').fetchone()[0],1)
            conn.execute(f'UPDATE {budget.TICKETS} SET weight=552 WHERE token=%s',('f'*32,))
        plan=self.budget.reserve_info_plan(entry_info_bodies())
        self.assertEqual(self.weights(),798)
        permit=plan.acquire('/info',dict(type='meta'));permit.check()
        plan.close()
        self.assertEqual(self.weights(),572)
        with self.assertRaisesRegex(budget.BudgetError,'CLOSED'):
            plan.acquire('/info',dict(type='meta'))

    def test_claim_refreshes_live_ticket_and_close_releases_only_never_claimed_credits(self):
        bodies=observation_bodies();batch=self.budget.reserve_observation(bodies,priority='protection')
        with self.journal._transaction() as conn:
            conn.execute(f"UPDATE {budget.TICKETS} SET admitted_at=clock_timestamp()-interval '68 seconds'")
        permit=batch.acquire('/info',bodies[0],priority='protection');permit.check()
        with self.journal._transaction() as conn:
            age=conn.execute(f'SELECT EXTRACT(epoch FROM clock_timestamp()-admitted_at) FROM {budget.TICKETS} WHERE token=%s',
                             (permit._token,)).fetchone()[0]
        self.assertLess(age,1)
        batch.close()
        self.assertEqual(self.weights(),120)
        other=self.budget.reserve_observation([bodies[0]],priority='protection')
        with self.journal._transaction() as conn:
            conn.execute(f"UPDATE {budget.TICKETS} SET admitted_at=clock_timestamp()-interval '69.1 seconds'")
        with self.assertRaisesRegex(budget.BudgetError,'CREDIT_UNAVAILABLE'):
            other.acquire('/info',bodies[0],priority='protection')
        with self.assertRaisesRegex(budget.BudgetError,'UNDECLARED'):
            other.acquire('/info',bodies[0],priority='protection')

    def test_lost_batch_reservation_commit_leaves_all_credits_without_batch_permission(self):
        original=self.budget._transaction
        @contextmanager
        def lost_ack(**kwargs):
            with original(**kwargs) as conn:yield conn
            raise budget.BudgetError('TESTNET_SHARED_REQUEST_BUDGET_UNAVAILABLE')
        with patch.object(self.budget,'_transaction',lost_ack):
            with self.assertRaises(budget.BudgetError):
                self.budget.reserve_observation(observation_bodies(),priority='protection')
        self.assertEqual(self.weights(),292)

    def test_slow_batch_commit_cannot_restart_initial_or_child_permission_clocks(self):
        original=self.budget._transaction;clock=Mock(return_value=1000000000)
        @contextmanager
        def slow_commit(**kwargs):
            with original(**kwargs) as conn:yield conn
            clock.return_value=2000000001
        with patch.object(budget.time,'monotonic_ns',clock), \
                patch.object(self.budget,'_transaction',slow_commit):
            with self.assertRaisesRegex(budget.BudgetError,'PERMIT_EXPIRED'):
                self.budget.reserve_observation(observation_bodies(),priority='protection')
        self.assertEqual(self.weights(),292)
        batch=self.budget.reserve_observation([observation_bodies()[0]],priority='protection')
        clock.return_value=1000000000
        with patch.object(budget.time,'monotonic_ns',clock), \
                patch.object(self.budget,'_transaction',slow_commit):
            with self.assertRaisesRegex(budget.BudgetError,'PERMIT_EXPIRED'):
                batch.acquire('/info',observation_bodies()[0],priority='protection')
        with self.assertRaisesRegex(budget.BudgetError,'UNDECLARED'):
            batch.acquire('/info',observation_bodies()[0],priority='protection')
        self.assertEqual(self.weights(),412)

    def test_lost_claim_commit_burns_one_credit_without_deadline_restart_or_reissue(self):
        body=observation_bodies()[0]
        batch=self.budget.reserve_observation([body],priority='protection')
        original=self.budget._transaction
        @contextmanager
        def lost_ack(**kwargs):
            with original(**kwargs) as conn:yield conn
            raise budget.BudgetError('TESTNET_SHARED_REQUEST_BUDGET_UNAVAILABLE')
        with patch.object(self.budget,'_transaction',lost_ack):
            with self.assertRaises(budget.BudgetError):batch.acquire('/info',body,priority='protection')
        with self.assertRaisesRegex(budget.BudgetError,'UNDECLARED'):
            batch.acquire('/info',body,priority='protection')
        batch.close()
        self.assertEqual(self.weights(),120)

    def test_extension_atomicity_and_both_paginated_passes_preserve_shared_limit(self):
        parent=observation_bodies()[0]
        children=[{**parent,'endTime':1500},{**parent,'startTime':1501}]
        batch=self.budget.reserve_observation([parent,parent],priority='protection')
        for _ in range(2):
            permit=batch.acquire('/info',parent,priority='protection');permit.check()
            self.assertTrue(permit.finish([{}]*2000))
            batch.reserve_extra(children)
            for body in children:
                child=batch.acquire('/info',body,priority='protection');child.check()
                self.assertTrue(child.finish([]))
        self.assertEqual(self.weights(),320)
        self.budget.reserve_observation(observation_bodies(),priority='protection')
        self.budget.reserve_observation(observation_bodies(),priority='protection')
        # 320+292+292+120=1024; the full sibling pair would exceed 1200.
        fresh=self.budget.reserve_observation([parent],priority='protection')
        fresh.acquire('/info',parent,priority='protection').check()
        with self.assertRaisesRegex(budget.BudgetError,'EXHAUSTED'):fresh.reserve_extra(children)
        self.assertEqual(self.weights(),1024)


if __name__ == '__main__':
    unittest.main()
