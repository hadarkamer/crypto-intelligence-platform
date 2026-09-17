"""Mock public API and disposable PostgreSQL only; no exchange orders or keys."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
import inspect
import os
import subprocess
import sys
import threading
import unittest
from unittest.mock import patch
from . import card_lifecycle as life, card_sync_evidence as e, card_sync as worker
from .card_lifecycle_store import LifecycleStore, SCHEMA
from .card_sync_store import SyncStore, TABLE
from .postgres_journal import PostgresJournal, JournalError
from .test_card_lifecycle import binding, opened, closed, snapshot, order, fill, terminal, A, B, T

CI=os.environ.get('HL_JOURNAL_CI_URL')


def evidence(b=None,*,is_open=False):
    b=b or binding()
    s=opened(b) if is_open else closed(b)
    for f in s['fills']: f['fill_id']='hl:'+f['fill_id']
    return dict(bindings=[b],snapshot=s)


def raw_state(ev):
    s=ev['snapshot']; bs=ev['bindings']; statuses={}; inventory=[]
    opened_by={x['oid']:x for x in s['open_orders']}; terminal_by={x['oid']:x for x in s['terminal_orders']}
    for b in bs:
        for leg in life.LEGS:
            oid=b['orders'][leg][0]; terminal_row=terminal_by.get(oid); op=opened_by.get(oid)
            is_entry=leg=='ENTRY'; side=('B' if b['side']=='LONG' else 'A') if is_entry else ('A' if b['side']=='LONG' else 'B')
            status=('filled' if terminal_row['state']=='FILLED' else 'siblingFilledCanceled') if terminal_row else 'open'
            row=dict(oid=int(oid),coin=b['symbol'],side=side,origSz=b['planned_quantity'],
                sz=op['quantity'] if op else '0',limitPx=b['prices']['entry' if is_entry else 'stop' if leg=='STOP' else 'take_profit'],
                reduceOnly=not is_entry,orderType='Limit' if is_entry else 'Stop Market' if leg=='STOP' else 'Take Profit Limit',
                isTrigger=not is_entry,triggerPx='0' if is_entry else b['prices']['stop' if leg=='STOP' else 'take_profit'])
            statuses[oid]=dict(status='order',order=dict(order=row,status=status,statusTimestamp=terminal_row['at_ms'] if terminal_row else T-100))
            if op: inventory.append(deepcopy(row))
    rawfills=[dict(coin=f['symbol'],tid=int(f['fill_id'].split(':')[-1]),oid=int(f['oid']),
        sz=f['quantity'],px=f['price'],fee=f['fee'],feeToken=f['fee_token'],side=f['side'],time=f['at_ms']) for f in s['fills']]
    position=dict(assetPositions=[dict(position=dict(coin=s['symbol'],szi=s['position_quantity']))])
    return dict(statuses=statuses,fills=rawfills,inventory=inventory,position=position)


class Reader:
    def __init__(self,ev): self.data=raw_state(ev);self.calls=0;self.windows=[]
    def read(self,kind,account,*,oid=None,start=None,end=None):
        self.calls+=1
        if kind=='orderStatus': return deepcopy(self.data['statuses'][oid])
        if kind=='userFillsByTime':
            self.windows.append((start,end))
            return deepcopy([f for f in self.data['fills'] if start<=f['time']<=end])
        return deepcopy(self.data['position' if kind=='clearinghouseState' else 'inventory'])


def collected(ev,target=None,**kw):
    return e.collect(ev,Reader(target or ev),clock=lambda:T+10000,**kw)


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        p=patch('http.client.HTTPSConnection',side_effect=AssertionError('PUBLIC_NETWORK_NOT_ALLOWED'))
        p.start();self.addCleanup(p.stop)

    def test_closed_remains_closed_without_recounting_old_fills(self):
        ev=evidence();r=collected(ev)
        self.assertTrue(r['report']['cards'][0]['closure_verified'])
        self.assertEqual(len(r['snapshot']['fills']),2)
        self.assertFalse(r['report']['dispatch_enabled'])

    def test_both_directions(self):
        for side in ('LONG','SHORT'):
            r=collected(evidence(binding(side=side)))
            self.assertTrue(r['report']['cards'][0]['closure_verified'])

    def test_retains_old_fills_outside_delta_window(self):
        ev=evidence();reader=Reader(ev)
        r=e.collect(ev,reader,cursor_ms=T+120000,clock=lambda:T+130000)
        self.assertEqual(len(r['snapshot']['fills']),2)
        self.assertEqual(reader.windows[0][0],T+60000)

    def test_open_to_closed_only_on_complete_evidence(self):
        ev=evidence(is_open=True);target=evidence()
        r=collected(ev,target)
        self.assertTrue(r['report']['cards'][0]['closure_verified'])
        self.assertEqual(ev['snapshot']['position_quantity'],'100')

    def test_two_same_coin_cards_close_only_one(self):
        a,b=binding(),binding(2,qty='60'); b['prices']=dict(entry='110',stop='108',take_profit='116')
        ev=evidence(a,is_open=True); eb=evidence(b,is_open=True)
        ev['bindings']+=eb['bindings']
        for k in ('fills','open_orders','terminal_orders'):ev['snapshot'][k]+=eb['snapshot'][k]
        ev['snapshot']['position_quantity']='160'
        target=evidence(a)
        target['bindings']+=eb['bindings']
        for k in ('fills','open_orders','terminal_orders'):target['snapshot'][k]+=eb['snapshot'][k]
        target['snapshot']['position_quantity']='60'
        r=collected(ev,target)['report']['cards']
        self.assertTrue(r[0]['closure_verified']);self.assertEqual(r[1]['state'],'OPEN')
        self.assertEqual(r[1]['remaining_quantity'],'60')

    def test_partial_entry_and_waiting_children_do_not_get_false_protection(self):
        b=binding();s=snapshot(b,fills=[{**fill(b,qty='40'), 'fill_id':'hl:1000'}],
            opens=[order(b,'ENTRY',qty='60'),order(b,'STOP'),order(b,'TAKE_PROFIT')],position='40')
        ev=dict(bindings=[b],snapshot=s)
        r=collected(ev)['report']
        self.assertTrue(r['needs_review'])
        self.assertEqual(r['cards'][0]['state'],'PARTIALLY_OPEN')

    def test_duplicate_api_fill_counted_once(self):
        ev=evidence();reader=Reader(ev);reader.data['fills']*=3
        r=e.collect(ev,reader,clock=lambda:T+10000)
        self.assertEqual(r['report']['cards'][0]['entry_quantity'],'100')

    def test_changed_duplicate_rejected(self):
        ev=evidence();reader=Reader(ev)
        reader.data['fills'].append({**reader.data['fills'][0],'sz':'99'})
        with self.assertRaises(e.SyncError):e.collect(ev,reader,clock=lambda:T+10000)

    def test_missing_known_fill_in_overlap_rejected(self):
        ev=evidence();reader=Reader(ev);reader.data['fills']=[]
        with self.assertRaisesRegex(e.SyncError,'PREVIOUS_FILL_MISSING'):e.collect(ev,reader,clock=lambda:T+10000)

    def test_missing_new_fill_rejected(self):
        ev=evidence(is_open=True);reader=Reader(evidence());reader.data['fills']=reader.data['fills'][:1]
        with self.assertRaisesRegex(e.SyncError,'HISTORY_INCOMPLETE'):e.collect(ev,reader,clock=lambda:T+10000)

    def test_unknown_fill_is_visible_as_problem(self):
        ev=evidence();reader=Reader(ev)
        reader.data['fills'].append({**reader.data['fills'][0],'oid':999,'tid':999,'time':T+100})
        r=e.collect(ev,reader,clock=lambda:T+10000)['report']
        self.assertTrue(r['needs_review']);self.assertFalse(r['cards'][0]['closure_verified'])

    def test_unknown_order_is_not_canceled_or_hidden(self):
        ev=evidence();reader=Reader(ev);reader.data['inventory']=[dict(coin='DOGE',oid=999)]
        with self.assertRaisesRegex(e.SyncError,'UNASSIGNED'):e.collect(ev,reader,clock=lambda:T+10000)

    def test_account_position_mismatch_blocks_green(self):
        ev=evidence();reader=Reader(ev);reader.data['position']['assetPositions'][0]['position']['szi']='1'
        r=e.collect(ev,reader,clock=lambda:T+10000)['report']
        self.assertIn('POSITION_DOES_NOT_MATCH_CARDS',r['bucket_issues'])
        self.assertIsNone(r['cards'][0]['net_before_funding_usdc'])

    def test_wrong_order_identity_rejected(self):
        ev=evidence();reader=Reader(ev);reader.data['statuses']['10']['order']['order']['coin']='BTC'
        with self.assertRaisesRegex(e.SyncError,'BINDING_MISMATCH'):e.collect(ev,reader,clock=lambda:T+10000)

    def test_newer_status_retried(self):
        ev=evidence();reader=Reader(ev);reader.data['statuses']['10']['order']['statusTimestamp']=T+20000
        with self.assertRaisesRegex(e.SyncError,'CHANGED_RETRY'):e.collect(ev,reader,clock=lambda:T+10000)

    def test_unknown_status_not_treated_as_closed(self):
        ev=evidence();reader=Reader(ev);reader.data['statuses']['10']['order']['status']='triggered'
        with self.assertRaises(e.SyncError):e.collect(ev,reader,clock=lambda:T+10000)

    def test_two_inconsistent_observations_defer(self):
        ev=evidence();reader=Reader(ev);original=reader.read
        def read(*args,**kwargs):
            result=original(*args,**kwargs)
            if args[0]=='clearinghouseState' and reader.calls>6:result['assetPositions'][0]['position']['szi']='1'
            return result
        reader.read=read
        with self.assertRaisesRegex(e.SyncError,'CHANGED_RETRY'):e.collect(ev,reader,clock=lambda:T+10000)

    def test_old_or_future_checkpoint_never_guessed(self):
        for value in (T-1,T+20000,T-e.DAY_MS):
            with self.assertRaises(e.SyncError):collected(evidence(),cursor_ms=value)

    def test_long_gap_explicitly_blocked(self):
        with self.assertRaisesRegex(e.SyncError,'GAP'):e.collect(evidence(),Reader(evidence()),clock=lambda:T+e.DAY_MS)

    def test_incomplete_bootstrap_rejected(self):
        ev=evidence();ev['snapshot']['history_complete']=False
        with self.assertRaisesRegex(e.SyncError,'BOOTSTRAP'):collected(ev)

    def test_slow_read_not_fresh(self):
        times=iter((0,16))
        with self.assertRaisesRegex(e.SyncError,'TOO_SLOW'):collected(evidence(),elapsed=lambda:next(times))

    def test_equal_timestamp_full_page_blocks_instead_of_skipping(self):
        class Full:
            def read(self,*a,**kw):return [dict(time=T)]*2000
        with self.assertRaisesRegex(e.SyncError,'TRUNCATED'):e.history(Full(),A,T,T)

    def test_history_bisection_preserves_inclusive_boundary(self):
        class Pages:
            def read(self,*a,start=None,end=None,**kw):return [dict(time=t) for t in range(start,end+1)]
        rows=e.history(Pages(),A,T,T+999)
        self.assertEqual(len(rows),1000);self.assertEqual(len({r['time'] for r in rows}),1000)

    def test_public_transport_rejects_exchange_or_mainnet(self):
        with self.assertRaises(e.SyncError):e.PublicReader().read('order',A)
        with patch.object(e,'HOST','api.hyperliquid.xyz'):
            with self.assertRaises(e.SyncError):e.PublicReader().read('clearinghouseState',A)

    def test_read_budget_checked_before_transport(self):
        r=worker.BudgetReader();r.weight=399
        with self.assertRaisesRegex(e.SyncError,'RATE_BUDGET'):r.read('clearinghouseState',A)
        self.assertEqual(r.calls,0)

    def test_config_reads_no_secrets(self):
        class Env(dict):
            def get(self,k,*a):
                if k.endswith('_KEY'):raise AssertionError('SECRET_READ')
                return super().get(k,*a)
        env=Env(HL_TESTNET_CARD_SYNC=worker.MODE,RENDER_SERVICE_ID=worker.SERVICE,
            HL_TESTNET_RUNTIME_MODE='read_only',HL_TESTNET_JOURNAL_BACKEND='staging_postgres_v1')
        worker.config(env)

    def test_trading_enabled_modes_rejected(self):
        env=dict(HL_TESTNET_CARD_SYNC=worker.MODE,RENDER_SERVICE_ID=worker.SERVICE,
            HL_TESTNET_RUNTIME_MODE='single_testnet_attempt_v1',HL_TESTNET_JOURNAL_BACKEND='staging_postgres_v1')
        with self.assertRaises(e.SyncError):worker.config(env)

    def test_loop_waits_after_completion_and_stops(self):
        calls=[]
        class Stop:
            done=False
            def is_set(self):return self.done
            def wait(self,delay):calls.append(('wait',delay));self.done=True
        def runner(*a,**kw):calls.append(('read',));return dict(status='SYNC_PASS_COMPLETED')
        worker.run_loop(None,None,Stop(),emit=lambda r:calls.append(('emit',)),runner=runner)
        self.assertEqual(calls,[('read',),('emit',),('wait',30)])

    def test_arbitrary_exception_text_not_logged(self):
        self.assertEqual(worker.error_code(ValueError('secret contents')),'SYNC_READ_FAILED')

    def test_no_execution_imports_in_new_modules(self):
        import hl_testnet_runtime.card_sync_store as store
        for module in (e,worker,store):
            text=inspect.getsource(module)
            for forbidden in ('import hyperliquid_testnet_executor','wallet_for_role(', 'sign_l1_action(', 'submit_persisted('):
                self.assertNotIn(forbidden,text)


@unittest.skipUnless(CI,'Disposable PostgreSQL required')
class SyncPostgresTests(unittest.TestCase):
    def setUp(self):
        self.j=PostgresJournal.for_ci(CI)
        with self.j._transaction() as conn:conn.execute(f'DROP SCHEMA IF EXISTS {SCHEMA} CASCADE')
        self.j.bootstrap();self.life=LifecycleStore(self.j);self.life.initialize()
        self.ev=evidence();self.life.save(self.ev['bindings'],self.ev['snapshot'],expected_revision=0,now_ms=T)
        self.s=SyncStore(self.j);self.s.initialize()
        p=patch('http.client.HTTPSConnection',side_effect=AssertionError('EXCHANGE_FORBIDDEN'))
        p.start();self.addCleanup(p.stop)

    def due(self):
        with self.j._transaction() as conn:conn.execute(f"UPDATE {TABLE} SET next_check=NULL")

    def counts(self):
        with self.j._transaction() as conn:return conn.execute(f'SELECT count(*) FROM {SCHEMA}.history').fetchone()[0]

    def test_claim_excludes_overlapping_processes(self):
        with ThreadPoolExecutor(max_workers=4) as pool:values=list(pool.map(lambda _:self.s.claim(),range(4)))
        self.assertEqual(sum(v is not None for v in values),1)

    def test_unchanged_facts_only_heartbeat_not_full_history(self):
        c=self.s.claim();result=self.s.save(c,collected(self.ev),now_ms=T+10000)
        self.assertFalse(result['changed']);self.assertEqual(self.counts(),1)
        self.assertEqual(self.s.summary()['fresh_verified_buckets'],1)

    def test_two_repeated_checks_do_not_create_duplicate_fills_or_history(self):
        for now in (T+10000,T+20000):
            self.due();c=self.s.claim()
            o=e.collect(c['evidence'],Reader(self.ev),cursor_ms=c['cursor_ms'],clock=lambda:now)
            self.s.save(c,o,now_ms=now)
        self.assertEqual(self.counts(),1)

    def test_lease_expiry_rejects_stale_worker_write(self):
        c=self.s.claim()
        with self.j._transaction() as conn:conn.execute(f"UPDATE {TABLE} SET lease_until=clock_timestamp()-interval '1 second'")
        with self.assertRaisesRegex(life.LifecycleError,'LEASE_LOST'):self.s.save(c,collected(self.ev),now_ms=T+10000)
        self.assertEqual(self.counts(),1)

    def test_failure_keeps_evidence_and_checkpoint_and_clears_green(self):
        c=self.s.claim();self.s.save(c,collected(self.ev),now_ms=T+10000);self.due();c=self.s.claim()
        self.s.fail(c,'PUBLIC_READ_UNAVAILABLE')
        self.assertEqual(self.s.summary()['fresh_verified_buckets'],0)
        self.assertEqual(self.s.summary()['problem_buckets'],1)
        with self.j._transaction() as conn:
            self.assertEqual(conn.execute(f'SELECT cursor_ms,report FROM {TABLE}').fetchone(),(T+10000,None))
        self.assertEqual(self.counts(),1)

    def test_stale_claim_cannot_clear_new_claim(self):
        old=self.s.claim()
        with self.j._transaction() as conn:conn.execute(f"UPDATE {TABLE} SET lease_until=clock_timestamp()-interval '1 second'")
        new=self.s.claim();self.s.fail(old,'OLD_WORKER_FAILED')
        self.s.save(new,collected(self.ev),now_ms=T+10000)
        self.assertEqual(self.s.summary()['fresh_verified_buckets'],1)

    def test_problem_snapshot_does_not_advance_evidence(self):
        c=self.s.claim();reader=Reader(self.ev);reader.data['position']['assetPositions'][0]['position']['szi']='1'
        o=e.collect(self.ev,reader,clock=lambda:T+10000)
        self.assertEqual(self.s.save(c,o,now_ms=T+10000)['status'],'NEEDS_REVIEW')
        self.assertEqual(self.counts(),1)
        with self.j._transaction() as conn:self.assertIsNone(conn.execute(f'SELECT cursor_ms FROM {TABLE}').fetchone()[0])

    def test_concurrent_binding_revision_change_not_overwritten(self):
        c=self.s.claim();snapshot2=deepcopy(self.ev['snapshot']);snapshot2['at_ms']=T+1
        self.life.save(self.ev['bindings'],snapshot2,expected_revision=1,now_ms=T+1)
        with self.assertRaisesRegex(life.LifecycleError,'CONCURRENT'):self.s.save(c,collected(self.ev),now_ms=T+10000)

    def test_read_only_tick_updates_registered_card(self):
        routes={'long_account':dict(account=A)}
        r=worker.tick(self.s,routes,reader_factory=lambda:Reader(self.ev),clock=lambda:T+10000)
        self.assertEqual(r['card_states'],{'CLOSED':1});self.assertEqual(r['order_requests_sent'],0)
        self.assertEqual(r['fresh_verified_buckets'],1)

    def test_wrong_account_not_read(self):
        reader=Reader(self.ev)
        r=worker.tick(self.s,{'long_account':dict(account=B)},reader_factory=lambda:reader,clock=lambda:T+10000)
        self.assertEqual(reader.calls,0);self.assertEqual(r['status'],'SYNC_REQUIRES_REVIEW')

    def test_restart_retains_checkpoint(self):
        c=self.s.claim();self.s.save(c,collected(self.ev),now_ms=T+10000)
        code=f'''import os
from hl_testnet_runtime.postgres_journal import PostgresJournal
from hl_testnet_runtime.card_sync_store import SyncStore,TABLE
j=PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL'])
s=SyncStore(j)
assert s.summary()['fresh_verified_buckets']==1
with j._transaction() as c: assert c.execute('SELECT cursor_ms FROM '+TABLE).fetchone()[0]=={T+10000}
print('RESTART_OK')'''
        p=subprocess.run([sys.executable,'-c',code],capture_output=True,text=True,timeout=10)
        self.assertEqual(p.returncode,0,p.stderr);self.assertIn('RESTART_OK',p.stdout)

    def test_new_record_only_alert_is_not_treated_as_an_executed_trade(self):
        # The scheduler only scans registered lifecycle heads, not alert payloads.
        with self.j._transaction() as conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM hl_testnet_execution_v1.attempts').fetchone()[0],0)
        self.assertEqual(self.s.summary()['registered_buckets'],1)


if __name__=='__main__':unittest.main()
