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
        with self.assertRaisesRegex(budget.BudgetError,'EXHAUSTED'):
            self.budget.acquire('/info',{'type':'meta'})
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

    def test_issued_child_is_clipped_to_batch_deadline_and_closing_never_refunds(self):
        body=observation_bodies()[0];batch,owner=self.batch([body,body])
        with patch.object(budget.time,'monotonic_ns',return_value=batch._deadline_ns-100):
            permit=batch.acquire('/info',body,priority='protection')
            self.assertEqual(permit._deadline_ns,batch._deadline_ns)
        batch.close()
        with self.assertRaisesRegex(budget.BudgetError,'CLOSED'):permit.check()
        with self.assertRaisesRegex(budget.BudgetError,'CLOSED'):
            batch.acquire('/info',body,priority='protection')
        owner._settle.assert_not_called()

    def test_expired_or_forked_batch_does_not_touch_sql_or_inherited_lock(self):
        body=observation_bodies()[0];batch,owner=self.batch([body])
        batch._deadline_ns=time.monotonic_ns()-1
        with self.assertRaisesRegex(budget.BudgetError,'EXPIRED'):
            batch.acquire('/info',body,priority='protection')
        batch._pid=-1;batch._lock.acquire()
        try:
            with self.assertRaisesRegex(budget.BudgetError,'PROCESS_CHANGED'):
                batch.acquire('/info',body,priority='protection')
        finally:batch._lock.release()
        owner._claim_observation.assert_not_called()

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
        self.assertEqual(self.weights(),876)

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

    def test_claim_refreshes_only_live_funded_ticket_without_releasing_unused_credits(self):
        bodies=observation_bodies();batch=self.budget.reserve_observation(bodies,priority='protection')
        with self.journal._transaction() as conn:
            conn.execute(f"UPDATE {budget.TICKETS} SET admitted_at=clock_timestamp()-interval '68 seconds'")
        permit=batch.acquire('/info',bodies[0],priority='protection');permit.check()
        with self.journal._transaction() as conn:
            age=conn.execute(f'SELECT EXTRACT(epoch FROM clock_timestamp()-admitted_at) FROM {budget.TICKETS} WHERE token=%s',
                             (permit._token,)).fetchone()[0]
        self.assertLess(age,1)
        batch.close()
        self.assertEqual(self.weights(),292)
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
        self.budget.reserve_observation(observation_bodies(),priority='protection').close()
        self.budget.reserve_observation(observation_bodies(),priority='protection').close()
        # 320+292+292+120=1024; the full sibling pair would exceed 1200.
        fresh=self.budget.reserve_observation([parent],priority='protection')
        fresh.acquire('/info',parent,priority='protection').check()
        with self.assertRaisesRegex(budget.BudgetError,'EXHAUSTED'):fresh.reserve_extra(children)
        self.assertEqual(self.weights(),1024)


if __name__ == '__main__':
    unittest.main()
