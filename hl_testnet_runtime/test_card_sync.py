"""Mock public API and disposable PostgreSQL only; no exchange orders or keys."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
from collections import Counter
import inspect
import json
import os
import subprocess
import sys
import threading
import unittest
from unittest.mock import patch, Mock
from . import card_lifecycle as life, card_sync_evidence as e, card_sync as worker
from .card_lifecycle_store import LifecycleStore, SCHEMA
from .card_sync_store import SyncStore, TABLE
from .postgres_journal import PostgresJournal, JournalError
from .test_card_lifecycle import binding, opened, closed, snapshot, order, fill, terminal, A, B, T

CI=os.environ.get('HL_JOURNAL_CI_URL')
DAY_AND_THREE_HOURS=e.DAY_MS+3*60*60*1000


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
            for oid in b['orders'][leg]:
                terminal_row=terminal_by.get(oid); op=opened_by.get(oid)
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


class TerminalCertificateTests(unittest.TestCase):
    def setUp(self):
        forbidden=patch('http.client.HTTPSConnection',side_effect=AssertionError('PUBLIC_NETWORK_NOT_ALLOWED'))
        forbidden.start();self.addCleanup(forbidden.stop)

    def collect(self,ev,reader=None,**kwargs):
        return e.collect(ev,reader or Reader(ev),clock=lambda:T+10000,
                         reuse_verified_terminals=True,**kwargs)

    def test_many_completed_cards_do_not_expand_live_status_reads(self):
        ev=evidence(binding(31),is_open=True)
        for n in range(1,31):
            old=evidence(binding(n))
            ev['bindings']+=old['bindings']
            for key in ('fills','open_orders','terminal_orders'):
                ev['snapshot'][key]+=old['snapshot'][key]
        reader=Reader(ev);status_reads=[];original=reader.read
        def read(kind,*args,**kwargs):
            if kind=='orderStatus':status_reads.append(kwargs['oid'])
            return original(kind,*args,**kwargs)
        reader.read=read
        result=self.collect(ev,reader)
        self.assertEqual(status_reads,['311','312','311','312'])
        self.assertEqual(reader.calls,10)  # Two statuses + three account reads per pass.
        self.assertEqual(reader.windows,[(T-60000,T+10000)]*2)
        self.assertEqual(result['snapshot']['at_ms'],T+10000)
        self.assertEqual(result['snapshot']['terminal_orders'],sorted(ev['snapshot']['terminal_orders'],key=lambda r:r['oid']))
        self.assertFalse(result['report']['needs_review'])

    def test_default_strict_path_still_rereads_every_terminal_identity(self):
        ev=evidence();reader=Reader(ev)
        result=e.collect(ev,reader,clock=lambda:T+10000)
        self.assertEqual(reader.calls,12)
        self.assertTrue(result['report']['cards'][0]['closure_verified'])

    def test_dispatch_reuses_final_facts_but_reads_fresh_account_state_twice(self):
        from .filled_quantity_dispatch import TestnetVenue
        ev=evidence();reader=Reader(ev);original=e.collect
        def timed(value,public_reader,**kwargs):
            return original(value,public_reader,clock=lambda:T+10000,**kwargs)
        with patch.object(e,'PublicReader',return_value=reader), \
                patch.object(e,'collect',side_effect=timed):
            result=TestnetVenue({}).collect(ev)
        self.assertEqual(reader.calls,6)
        self.assertEqual(reader.windows,[(T-60000,T+10000)]*2)
        self.assertEqual(result['snapshot']['terminal_orders'],ev['snapshot']['terminal_orders'])
        self.assertTrue(result['report']['cards'][0]['closure_verified'])

    def test_first_time_final_order_is_independently_read_in_both_passes(self):
        ev=evidence(is_open=True);reader=Reader(evidence())
        status_reads=[];original=reader.read
        def read(kind,*args,**kwargs):
            if kind=='orderStatus':status_reads.append(kwargs['oid'])
            return original(kind,*args,**kwargs)
        reader.read=read
        result=self.collect(ev,reader)
        self.assertEqual(status_reads,['11','12','11','12'])
        self.assertTrue(result['report']['cards'][0]['closure_verified'])
        after=Reader(dict(bindings=ev['bindings'],snapshot=result['snapshot']))
        self.collect(dict(bindings=ev['bindings'],snapshot=result['snapshot']),after)
        self.assertEqual(after.calls,6)  # Only fresh fills/inventory/position in each pass.

    def test_reappearing_certified_oid_blocks_even_with_unchanged_fill_total(self):
        ev=evidence();reader=Reader(ev)
        reader.data['inventory'].append(deepcopy(reader.data['statuses']['10']['order']['order']))
        with self.assertRaisesRegex(e.SyncError,'TERMINAL_ORDER_BECAME_ACTIVE'):
            self.collect(ev,reader)

    def test_added_terminal_fill_before_or_after_terminal_time_blocks(self):
        for at in (T-1000,T+100):
            with self.subTest(at=at):
                ev=evidence();reader=Reader(ev)
                reader.data['fills'].append({**reader.data['fills'][0],'tid':99999,'sz':'1','time':at})
                with self.assertRaisesRegex(e.SyncError,'TERMINAL_FACT_CHANGED'):
                    self.collect(ev,reader)

    def test_changed_or_missing_terminal_fill_in_overlap_still_blocks(self):
        for change,code in ((lambda rows:rows[0].update(px='101'),'FILL_FACT_CHANGED'),
                            (lambda rows:rows.pop(0),'PREVIOUS_FILL_MISSING_IN_OVERLAP')):
            with self.subTest(code=code):
                ev=evidence();reader=Reader(ev);change(reader.data['fills'])
                with self.assertRaisesRegex(e.SyncError,code):self.collect(ev,reader)

    def test_unverified_terminal_certificates_block_before_public_reads(self):
        cases=('quantity','filled_zero','rejected_with_fill','fill_after_terminal','unknown_oid','open_and_terminal','history','inventory')
        for case in cases:
            with self.subTest(case=case):
                ev=evidence();row=ev['snapshot']['terminal_orders'][0]
                if case=='quantity':row['filled_quantity']='99'
                elif case=='filled_zero':
                    row['filled_quantity']='0';ev['snapshot']['fills']=ev['snapshot']['fills'][1:]
                elif case=='rejected_with_fill':row['state']='REJECTED'
                elif case=='fill_after_terminal':row['at_ms']=T-10001
                elif case=='unknown_oid':row['oid']='999'
                elif case=='open_and_terminal':ev['snapshot']['open_orders'].append(order(ev['bindings'][0],'ENTRY'))
                elif case=='history':ev['snapshot']['history_complete']=False
                else:ev['snapshot']['orders_complete']=False
                reader=Reader(evidence())
                with self.assertRaisesRegex(e.SyncError,'TERMINAL_CERTIFICATE_NOT_VERIFIED|VERIFIED_HISTORY_BOOTSTRAP_REQUIRED'):
                    self.collect(ev,reader)
                self.assertEqual(reader.calls,0)

    def test_second_pass_changes_still_reject_full_checkpoint(self):
        ev=evidence();reader=Reader(ev);original=reader.read
        def read(kind,*args,**kwargs):
            response=original(kind,*args,**kwargs)
            if kind=='clearinghouseState' and reader.calls>3:
                response['assetPositions'][0]['position']['szi']='1'
            return response
        reader.read=read
        with self.assertRaisesRegex(e.SyncError,'OBSERVATION_CHANGED_RETRY'):
            self.collect(ev,reader)

    def test_duplicate_exact_durable_fills_do_not_invalidate_certificate(self):
        ev=evidence();ev['snapshot']['fills']*=2
        result=self.collect(ev)
        self.assertTrue(result['report']['cards'][0]['closure_verified'])
        self.assertEqual(len(result['snapshot']['fills']),2)

    def test_opt_in_does_not_extend_original_observation_time_or_read_deadline(self):
        ev=evidence();before=deepcopy(ev);times=iter((0,16))
        with self.assertRaisesRegex(e.SyncError,'OBSERVATION_TOO_SLOW'):
            self.collect(ev,elapsed=lambda:next(times))
        self.assertEqual(ev,before)
        with self.assertRaisesRegex(e.SyncError,'TERMINAL_CERTIFICATE_OPT_IN_INVALID'):
            e.collect(ev,Reader(ev),reuse_verified_terminals=1,clock=lambda:T+10000)


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

    def test_day_old_checkpoint_replays_bounded_overlapping_history(self):
        ev=evidence();reader=Reader(ev)
        end=T+DAY_AND_THREE_HOURS
        result=e.collect(ev,reader,clock=lambda:end,elapsed=lambda:0)
        self.assertEqual(result['snapshot']['fills'],ev['snapshot']['fills'])
        self.assertEqual(result['cursor_ms'],end)
        self.assertGreaterEqual(len(reader.windows),4)
        self.assertTrue(all(0<=b-a<=e.DAY_MS for a,b in reader.windows))
        self.assertEqual(reader.windows[0],reader.windows[1])
        self.assertLessEqual(reader.windows[2][0],reader.windows[0][1])

    def test_catchup_keeps_missing_old_fill_blocked(self):
        ev=evidence();reader=Reader(ev);reader.data['fills']=[]
        with self.assertRaisesRegex(e.SyncError,'PREVIOUS_FILL_MISSING'):
            e.collect(ev,reader,clock=lambda:T+DAY_AND_THREE_HOURS,elapsed=lambda:0)

    def test_older_than_two_days_requires_review(self):
        ev=evidence()
        with self.assertRaisesRegex(e.SyncError,'HISTORY_GAP_REQUIRES_REVIEW'):
            e.collect(ev,Reader(ev),clock=lambda:T+2*e.DAY_MS+1)

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

    def test_disagreeing_live_order_still_blocks_with_safe_field_code(self):
        ev=evidence(is_open=True)
        for change, code in ((lambda row: row.update(sz='99'), 'ORDER_VIEW_SZ_DIFF'),
                             (lambda row: row.update(limitPx=f"{float(row['limitPx']):.2f}"), 'ORDER_VIEW_LIMITPX_FORMAT_ONLY')):
            reader=Reader(ev)
            change(reader.data['inventory'][0])
            with self.assertRaisesRegex(e.SyncError,code) as error:
                e.collect(ev,reader,clock=lambda:T+10000)
            self.assertNotIn(str(reader.data['inventory'][0]['limitPx']),str(error.exception))
            self.assertEqual(ev['snapshot']['position_quantity'],'100')

    def test_partly_filled_short_uses_verified_remaining_size_without_hiding_missing_exits(self):
        b=binding(side='SHORT')
        b['orders']['STOP']=[];b['orders']['TAKE_PROFIT']=[]
        s=snapshot(b,fills=[{**fill(b,qty='40'),'fill_id':'hl:1000'}],
                   opens=[order(b,'ENTRY',qty='60')],position='-40')
        ev=dict(bindings=[b],snapshot=s)
        reader=Reader(ev)
        reader.data['statuses']['10']['order']['order']['sz']='100'
        result=e.collect(ev,reader,clock=lambda:T+10000)
        self.assertEqual(result['snapshot']['open_orders'][0]['quantity'],'60')
        self.assertEqual(result['report']['cards'][0]['entry_quantity'],'40')
        self.assertIn('STOP_COVERAGE_MISSING',result['report']['cards'][0]['issues'])
        self.assertIn('TAKE_PROFIT_COVERAGE_MISSING',result['report']['cards'][0]['issues'])
        self.assertTrue(result['report']['needs_review'])
        self.assertEqual(ev['snapshot'],s)

        for bad_remaining in ('59','61'):
            bad=Reader(ev)
            bad.data['statuses']['10']['order']['order']['sz']='100'
            bad.data['inventory'][0]['sz']=bad_remaining
            with self.assertRaisesRegex(e.SyncError,'ORDER_VIEW_SZ_DIFF'):
                e.collect(ev,bad,clock=lambda:T+10000)

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
        with self.assertRaisesRegex(e.SyncError,'GAP'):e.collect(evidence(),Reader(evidence()),clock=lambda:T+2*e.DAY_MS+1)

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

    def test_parallel_reads_overlap_with_four_workers_and_sequential_verification(self):
        ev=evidence(is_open=True)
        source=Reader(ev)
        barrier=threading.Barrier(4,timeout=3)
        lock=threading.Lock()
        calls_per_pass=6  # Three statuses, fills, inventory and position.
        class Parallel(e.PublicReader):
            def __init__(self):
                super().__init__(parallel=True)
                self.started=0;self.active=0;self.peak=0
            def read(self,*args,**kwargs):
                with lock:
                    ordinal=self.started;self.started+=1
                    if ordinal % calls_per_pass == 0:
                        # A new pass must not overlap an old observation.
                        if self.active: raise AssertionError('OVERLAPPING_VERIFICATION_PASSES')
                    self.active+=1;self.peak=max(self.peak,self.active)
                try:
                    if ordinal % calls_per_pass < 4:barrier.wait()
                    with lock: return source.read(*args,**kwargs)
                finally:
                    with lock: self.active-=1
        reader=Parallel()
        result=e.collect(ev,reader,clock=lambda:T+10000)
        self.assertEqual(result,collected(ev))
        self.assertEqual((reader.started,reader.peak,reader.active),(12,4,0))
        self.assertEqual(source.windows,[source.windows[0]]*2)

    def test_multi_card_parallel_reads_keep_all_statuses_and_fixed_fanout_bound(self):
        barrier=threading.Barrier(e.MAX_OBSERVATION_READ_WORKERS,timeout=3)
        lock=threading.Lock()
        class Parallel(e.PublicReader):
            def __init__(self):
                super().__init__(parallel=True,parallel_workers=12)
                self.started=0;self.active=0;self.peak=0;self.statuses=[]
            def read(self,kind,account,*,oid=None,**kwargs):
                with lock:
                    ordinal=self.started;self.started+=1;self.active+=1
                    self.peak=max(self.peak,self.active)
                    if kind=='orderStatus':self.statuses.append(oid)
                try:
                    if ordinal<e.MAX_OBSERVATION_READ_WORKERS:barrier.wait()
                    if kind=='userFillsByTime':return []
                    return dict(kind=kind,oid=oid)
                finally:
                    with lock:self.active-=1
        reader=Parallel();oids=[str(n) for n in range(1,16)]
        statuses,fills,inventory,position=reader.observation_inputs(A,oids,T,T+1)
        self.assertEqual(set(statuses),set(oids))
        self.assertCountEqual(reader.statuses,oids)
        self.assertEqual((reader.started,reader.peak,reader.active),(18,12,0))
        self.assertEqual(fills,[])
        self.assertEqual(inventory['kind'],'frontendOpenOrders')
        self.assertEqual(position['kind'],'clearinghouseState')

    def test_parallel_fanout_is_explicit_and_rejects_unbounded_or_ambiguous_values(self):
        self.assertEqual(e.PublicReader(parallel=True).parallel_workers,4)
        for bad in (0,13,-1,True,12.0,'12',None):
            with self.subTest(bound=bad),self.assertRaisesRegex(e.SyncError,'PARALLEL_BOUND_INVALID'):
                e.PublicReader(parallel=True,parallel_workers=bad)

    def test_parallel_inconsistent_second_pass_is_rejected(self):
        ev=evidence(is_open=True);source=Reader(ev);lock=threading.Lock()
        class Parallel(e.PublicReader):
            def __init__(self): super().__init__(parallel=True);self.positions=0
            def read(self,kind,*args,**kwargs):
                with lock:
                    result=source.read(kind,*args,**kwargs)
                    if kind=='clearinghouseState':
                        self.positions+=1
                        if self.positions==2: result['assetPositions'][0]['position']['szi']='99'
                    return result
        original=deepcopy(ev)
        with self.assertRaisesRegex(e.SyncError,'OBSERVATION_CHANGED_RETRY'):
            e.collect(ev,Parallel(),clock=lambda:T+10000)
        self.assertEqual(ev,original)

    def test_parallel_read_failure_returns_no_partial_evidence_and_joins_workers(self):
        ev=evidence();source=Reader(ev);lock=threading.Lock()
        class Parallel(e.PublicReader):
            def __init__(self): super().__init__(parallel=True,parallel_workers=12);self.active=0
            def read(self,kind,*args,**kwargs):
                with lock: self.active+=1
                try:
                    if kind=='userFillsByTime': raise e.SyncError('PUBLIC_READ_UNAVAILABLE')
                    with lock: return source.read(kind,*args,**kwargs)
                finally:
                    with lock: self.active-=1
        reader=Parallel();original=deepcopy(ev)
        with self.assertRaisesRegex(e.SyncError,'PUBLIC_READ_UNAVAILABLE'):
            e.collect(ev,reader,clock=lambda:T+10000)
        self.assertEqual((reader.active,ev),(0,original))

    def test_parallel_missing_fill_cannot_be_reported_as_closed(self):
        ev=evidence(is_open=True);source=Reader(evidence());source.data['fills']=source.data['fills'][:1]
        lock=threading.Lock()
        reader=e.PublicReader(parallel=True)
        def read(*args,**kwargs):
            with lock: return source.read(*args,**kwargs)
        reader.read=read
        with self.assertRaisesRegex(e.SyncError,'HISTORY_INCOMPLETE'):
            e.collect(ev,reader,clock=lambda:T+10000)

    def test_public_read_call_budget_is_shared_by_parallel_workers(self):
        reader=e.PublicReader(parallel=True);reader.calls=198
        class Response:
            status=200
            def read(self,*args): return b'[]'
        class Connection:
            def __init__(self,*args,**kwargs): pass
            def request(self,*args,**kwargs): pass
            def getresponse(self): return Response()
            def close(self): pass
        def read(_):
            try: reader.read('frontendOpenOrders',A);return 'OK'
            except e.SyncError as exc: return str(exc)
        with patch.object(e.http.client,'HTTPSConnection',Connection):
            with ThreadPoolExecutor(max_workers=8) as pool:
                results=list(pool.map(read,range(8)))
        self.assertEqual(results.count('OK'),2)
        self.assertEqual(results.count('READ_BUDGET_EXCEEDED'),6)
        self.assertEqual(reader.calls,200)

    def test_weighted_monitor_reader_remains_serial(self):
        reader=worker.BudgetReader();source=Reader(evidence())
        with patch.object(reader,'read',side_effect=source.read), \
                patch.object(e,'ThreadPoolExecutor',side_effect=AssertionError('MONITOR_MUST_REMAIN_SERIAL')):
            result=e.collect(evidence(),reader,clock=lambda:T+10000)
        self.assertTrue(result['report']['cards'][0]['closure_verified'])

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


class ObservationFundingTests(unittest.TestCase):
    def test_denied_complete_plan_sends_no_partial_status_or_inventory_reads(self):
        from .request_budget import BudgetError
        budget=Mock()
        budget.reserve_observation.side_effect=BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED')
        reader=e.PublicReader(parallel=True,budget=budget,priority='protection')
        original=evidence(is_open=True)
        with patch.object(e.http.client,'HTTPSConnection') as connection:
            with self.assertRaisesRegex(BudgetError,'EXHAUSTED'):
                e.collect(original,reader,clock=lambda:T+10000,reuse_verified_terminals=True)
        connection.assert_not_called()
        budget.acquire.assert_not_called()
        plan=budget.reserve_observation.call_args.args[0]
        from .request_budget import request_weight
        self.assertEqual(sum(request_weight('/info',body) for body in plan),292)
        self.assertEqual(Counter(body['type'] for body in plan),
                         {'userFillsByTime':2,'frontendOpenOrders':2,
                          'clearinghouseState':2,'orderStatus':4})
        self.assertEqual(original,evidence(is_open=True))

    def test_complete_funded_plan_preserves_two_independent_passes_and_refunds(self):
        source=Reader(evidence(is_open=True));lock=threading.Lock()
        remaining=Counter();claimed=[];finished=[];closed=[]
        class Permit:
            def check(self):pass
            def finish(self,response):finished.append(deepcopy(response))
        class Batch:
            def acquire(self,path,body,**kw):
                key=json.dumps(body,sort_keys=True)
                with lock:
                    if not remaining[key]:raise AssertionError('UNDECLARED_HTTP')
                    remaining[key]-=1;claimed.append(body['type'])
                return Permit()
            def close(self):closed.append(True)
        class Budget:
            def reserve_observation(self,bodies,**kw):
                remaining.update(json.dumps(body,sort_keys=True) for body in bodies)
                return Batch()
            def acquire(self,*a,**kw):raise AssertionError('BATCH_BYPASS')
        class Connection:
            def __init__(self,*a,**kw):self.body=None
            def request(self,method,path,body,headers):self.body=json.loads(body)
            def getresponse(self):
                body=self.body;kind=body['type'];kwargs={}
                if kind=='orderStatus':kwargs['oid']=str(body['oid'])
                if kind=='userFillsByTime':kwargs.update(start=body['startTime'],end=body['endTime'])
                with lock:self.response=source.read(kind,body['user'],**kwargs)
                self.status=200
                return self
            def read(self,*a):return json.dumps(self.response).encode()
            def close(self):pass
        reader=e.PublicReader(parallel=True,budget=Budget(),priority='protection')
        with patch.object(e.http.client,'HTTPSConnection',Connection):
            result=e.collect(evidence(is_open=True),reader,clock=lambda:T+10000,
                             reuse_verified_terminals=True)
        self.assertEqual(result,collected(evidence(is_open=True)))
        self.assertEqual((len(claimed),len(finished),closed),(10,10,[True]))
        self.assertFalse(any(remaining.values()))
        self.assertEqual(source.windows,[source.windows[0]]*2)
        self.assertIsNone(reader._observation_budget)

    def test_plan_includes_both_catchup_reads_and_only_original_terminal_exclusions(self):
        ev=evidence(is_open=True)
        bodies=e._planned_observation_reads(ev['bindings'],ev['snapshot'],T,
            T+DAY_AND_THREE_HOURS,reuse_verified_terminals=True)
        fills=[body for body in bodies if body['type']=='userFillsByTime']
        self.assertEqual(len(fills),4)
        self.assertEqual(fills[0],fills[1]);self.assertEqual(fills[2],fills[3])
        self.assertEqual(fills[0]['endTime'],T+e.DAY_MS-2*e.OVERLAP_MS)
        self.assertEqual(fills[2]['startTime'],fills[0]['endTime']-e.OVERLAP_MS)
        self.assertEqual(Counter(body['oid'] for body in bodies if body['type']=='orderStatus'),
                         {11:2,12:2})

    def test_recursive_history_split_funds_both_siblings_before_any_child_http(self):
        from .request_budget import BudgetError
        events=[]
        class Batch:
            def reserve_extra(self,bodies):
                events.append(('reserve',deepcopy(bodies)))
                raise BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED')
        class ReaderWithBatch(e.PublicReader):
            def __init__(self):
                super().__init__(budget=None)
                self._observation_budget=Batch()
            def read(self,kind,account,*,start=None,end=None,**kw):
                events.append(('http',start,end))
                return [dict(time=T+i) for i in range(500)]
        with self.assertRaisesRegex(BudgetError,'EXHAUSTED'):
            e.history(ReaderWithBatch(),A,T,T+999)
        self.assertEqual(events[0],('http',T,T+999))
        self.assertEqual(len(events),2)
        self.assertEqual(events[1][0],'reserve')
        self.assertEqual([(row['startTime'],row['endTime']) for row in events[1][1]],
                         [(T,T+499),(T+500,T+999)])


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
            table=conn.execute("SELECT to_regclass('hl_testnet_execution_v1.attempts')").fetchone()[0]
            before=(conn.execute('SELECT count(*) FROM hl_testnet_execution_v1.attempts').fetchone()[0]
                    if table is not None else None)
        self.assertEqual(self.s.summary()['registered_buckets'],1)
        with self.j._transaction() as conn:
            current=conn.execute("SELECT to_regclass('hl_testnet_execution_v1.attempts')").fetchone()[0]
            after=(conn.execute('SELECT count(*) FROM hl_testnet_execution_v1.attempts').fetchone()[0]
                   if current is not None else None)
        self.assertEqual((current is not None,after),(table is not None,before))


if __name__=='__main__':unittest.main()
