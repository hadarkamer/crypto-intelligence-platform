"""Read-only gap recovery: original clocks, retention, CAS, and real storage."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
import os
import multiprocessing
import threading
import unittest
from unittest.mock import patch

from . import history_gap_recovery as gap, card_lifecycle as life
from . import card_sync_evidence as evidence, filled_quantity_dispatch as dispatch
from .filled_dispatch_store import DispatchStore,DispatchError,SCHEMA
from .postgres_journal import PostgresJournal
from .test_card_lifecycle import closed,terminal,T
from .test_card_sync import Reader
from .test_filled_quantity_exits import original,ROUTES
from .test_filled_quantity_dispatch import NoExternal,Venue

CI=os.environ.get('HL_JOURNAL_CI_URL')


def final_state(*,side='LONG',n=1,zero=False):
    binding,record=original(n,side)
    binding['orders']['STOP']=[str(n*10+1)]
    binding['orders']['TAKE_PROFIT']=[str(n*10+2)]
    snap=closed(binding)
    for fill in snap['fills']:
        fill['fill_id']='hl:'+fill['fill_id']
    if zero:
        snap['fills']=[]
        snap['terminal_orders']=[terminal(binding,leg,'0') for leg in life.LEGS]
    return dict(version='fixture',domain='software',bucket=life.digest(['testnet',binding['account'],'DOGE']),
        account=binding['account'],symbol='DOGE',revision=1,originals={binding['card_id']:record},
        bindings=[binding],evidence=dict(bindings=[binding],snapshot=snap),pending=None,last_request=None)


class MemoryStore:
    domain='software'
    def __init__(self,*states):
        self.values={s['bucket']:deepcopy(s) for s in states};self.guard=threading.RLock();self.events=[]
    def load(self,bucket):
        with self.guard:return deepcopy(self.values[bucket])
    def for_account(self,account):
        with self.guard:return [deepcopy(s) for s in self.values.values() if s['account']==account]
    def change(self,bucket,revision,event,now,update):
        with self.guard:
            value=deepcopy(self.values[bucket])
            if value['revision']!=revision:raise DispatchError('CONCURRENT_DISPATCH_RELOAD_REQUIRED')
            update(None,value);value['revision']+=1
            self.values[bucket]=value;self.events.append(event)
        return self.load(bucket)


class PlannedReader(Reader):
    def __init__(self,ev):super().__init__(ev);self.plans=[]
    @contextmanager
    def observation_batch(self,bodies):
        self.plans.append(deepcopy(bodies))
        yield


def process_recovery(state,ci_url,at_ms,barrier,results):
    """Separate process lanes share only the real PostgreSQL CAS and quota."""
    venue=Venue();venue.t=at_ms
    controller=dispatch.Controller(DispatchStore(PostgresJournal.for_ci(ci_url)),venue,ROUTES)
    reader=PlannedReader(state['evidence']);read=reader.read
    def together(*args,**kwargs):
        value=read(*args,**kwargs)
        if reader.calls==4:barrier.wait(timeout=10)
        return value
    reader.read=together
    try:results.put((gap.step(controller,state,reader=reader)['status'],reader.calls))
    except life.LifecycleError as exc:results.put((str(exc),reader.calls))


class RecoveryPureTests(NoExternal):
    def setup_case(self,side='LONG',state=None,staged=True):
        state=state or final_state(side=side)
        store=MemoryStore(state);venue=Venue();venue.t=T+3*evidence.DAY_MS
        controller=dispatch.Controller(store,venue,ROUTES)
        if staged:
            state=gap.step(controller,state,reader=PlannedReader(state['evidence']))['state']
        return state,store,venue,controller,PlannedReader(state['evidence'])

    def tick(self,controller,state,reader):
        return gap.step(controller,state,reader=reader)

    def finish(self,controller,state,reader):
        while True:
            result=self.tick(controller,state,reader);state=result['state']
            if result['status']=='HISTORY_GAP_RECOVERY_COMPLETE':return result

    def test_both_accounts_stage_actual_windows_without_advancing_live_clock_then_final_proof(self):
        for side in ('LONG','SHORT'):
            with self.subTest(side=side):
                state,store,venue,controller,reader=self.setup_case(side)
                old=deepcopy(state['evidence']);previous=T
                while True:
                    result=self.tick(controller,state,reader);state=result['state']
                    self.assertEqual(result['order_requests_sent'],0)
                    self.assertEqual(venue.sent,0)
                    if result['status']=='HISTORY_GAP_RECOVERY_COMPLETE':break
                    self.assertEqual(state['evidence'],old)
                    self.assertEqual(result['cursor_ms'],previous+gap.WINDOW_MS)
                    previous=result['cursor_ms']
                    self.assertTrue(gap.needed(state,venue.now()))
                self.assertNotIn(gap.KEY,state)
                self.assertEqual(state['evidence']['snapshot']['at_ms'],venue.now())
                self.assertEqual(evidence.facts(state['evidence']['snapshot']),evidence.facts(old['snapshot']))
                self.assertEqual(state['history_gap_recovery_last']['chunks'],5)
                self.assertFalse(gap.needed(state,venue.now()))
                self.assertEqual(store.events,['HISTORY_GAP_RECOVERY_STARTED']+['HISTORY_GAP_RECOVERY_CHUNK']*5+['HISTORY_GAP_RECOVERY_COMPLETE'])
                self.assertEqual([len(plan) for plan in reader.plans],[1,2,1]*5+[1,6,1])
                self.assertTrue(all(body['endTime']-body['startTime']<=gap.WINDOW_MS+evidence.OVERLAP_MS
                    for plan in reader.plans for body in plan if body['type']=='userFillsByTime'))

    def test_restart_continues_exact_cursor_and_old_evidence(self):
        state,store,venue,controller,reader=self.setup_case()
        first=self.tick(controller,state,reader)['state']
        replacement=dispatch.Controller(store,venue,ROUTES)
        second=self.tick(replacement,store.load(state['bucket']),reader)['state']
        self.assertEqual(first['evidence'],second['evidence'])
        self.assertEqual(second[gap.KEY]['cursor_ms'],T+2*gap.WINDOW_MS)
        self.assertEqual(second[gap.KEY]['anchor'],first[gap.KEY]['anchor'])

    def test_failed_second_pass_or_anchor_never_advances_durable_cursor(self):
        for failure_call in (3,4):
            with self.subTest(call=failure_call):
                state,store,venue,controller,reader=self.setup_case();read=reader.read
                def failed(*args,**kwargs):
                    if reader.calls+1==failure_call:raise evidence.SyncError('PUBLIC_READ_UNAVAILABLE')
                    return read(*args,**kwargs)
                reader.read=failed
                with self.assertRaisesRegex(evidence.SyncError,'PUBLIC_READ_UNAVAILABLE'):
                    self.tick(controller,state,reader)
                self.assertEqual(store.load(state['bucket']),state)

    def test_missing_overlap_or_changed_fill_stops_without_staging(self):
        for mode,code in (('missing','PREVIOUS_FILL_MISSING_IN_OVERLAP'),('changed','FILL_FACT_CHANGED')):
            with self.subTest(mode=mode):
                state,store,venue,controller,reader=self.setup_case()
                if mode=='missing':reader.data['fills'].pop(0)
                else:reader.data['fills'][0]['px']='11'
                with self.assertRaisesRegex(evidence.SyncError,code):self.tick(controller,state,reader)
                self.assertEqual(store.load(state['bucket']),state)

    def test_unknown_same_symbol_roundtrip_even_flat_blocks(self):
        state,store,venue,controller,reader=self.setup_case()
        for tid,side in ((90001,'B'),(90002,'A')):
            reader.data['fills'].append({**reader.data['fills'][0], 'tid':tid,'oid':9999,
                                        'sz':'1','side':side,'time':T+100})
        with self.assertRaisesRegex(gap.RecoveryError,'NEW_OR_UNRESOLVED_ACTIVITY'):
            self.tick(controller,state,reader)
        self.assertEqual(store.load(state['bucket']),state)

    def test_retention_anchor_evicted_on_second_bracket_blocks(self):
        state,store,venue,controller,reader=self.setup_case();read=reader.read
        def evicted(kind,*args,**kwargs):
            result=read(kind,*args,**kwargs)
            return [] if reader.calls==4 else result
        reader.read=evicted
        with self.assertRaisesRegex(evidence.SyncError,'PREVIOUS_FILL_MISSING_IN_OVERLAP'):
            self.tick(controller,state,reader)
        self.assertEqual(store.load(state['bucket']),state)

    def test_null_or_corrupt_stage_is_never_reinitialized(self):
        for stage in (None,{},dict(version=gap.VERSION,cursor_ms=T+gap.WINDOW_MS)):
            state=final_state();state[gap.KEY]=stage
            state,store,venue,controller,reader=self.setup_case(state=state,staged=False)
            self.assertTrue(gap.needed(state,T))
            with self.assertRaisesRegex(gap.RecoveryError,'STAGE_CHANGED'):
                self.tick(controller,state,reader)
            self.assertEqual(reader.calls,0)

    def test_changed_original_or_corrupted_cursor_or_regressed_clock_blocks_restart(self):
        for mode in ('original','cursor','clock'):
            state,store,venue,controller,reader=self.setup_case()
            staged=self.tick(controller,state,reader)['state']
            if mode=='original':staged['originals']['f'*64]={'new':'card'}
            elif mode=='cursor':staged[gap.KEY]['cursor_ms']+=1
            else:venue.t=T
            store.values[state['bucket']]=deepcopy(staged);reader.calls=0
            with self.assertRaisesRegex(gap.RecoveryError,'STAGE_CHANGED|CLOCK_REGRESSED'):
                self.tick(controller,staged,reader)
            self.assertEqual(reader.calls,0)

    def test_pending_state_change_during_http_loses_cas(self):
        state,store,venue,controller,reader=self.setup_case();read=reader.read
        def concurrent(kind,*args,**kwargs):
            result=read(kind,*args,**kwargs)
            if reader.calls==2:
                store.change(state['bucket'],state['revision'],'CONCURRENT_PREPARE',venue.now(),
                             lambda conn,current:current.update(pending='new-request'))
            return result
        reader.read=concurrent
        with self.assertRaisesRegex(life.LifecycleError,'CONCURRENT_DISPATCH_RELOAD_REQUIRED|FINAL_IDLE_BUCKET_REQUIRED'):
            self.tick(controller,state,reader)
        self.assertEqual(store.load(state['bucket'])[gap.KEY],state[gap.KEY])

    def test_feed_changed_during_reads_blocks_checkpoint(self):
        state,store,venue,controller,reader=self.setup_case();stamp=[(1,1)]
        controller._feed_stamp=lambda account:stamp[0];read=reader.read
        def change(*args,**kwargs):
            result=read(*args,**kwargs)
            if reader.calls==3:stamp[0]=(1,2)
            return result
        reader.read=change
        with self.assertRaisesRegex(gap.RecoveryError,'FEED_CHANGED'):self.tick(controller,state,reader)
        self.assertEqual(store.load(state['bucket']),state)

    def test_canceled_only_bucket_uses_old_owned_same_account_other_symbol_anchor(self):
        target=final_state(zero=True);source=final_state(n=2)
        source['symbol']='ETH';source['bucket']=life.digest(['testnet',source['account'],'ETH'])
        # Prepare a real immutable ETH original rather than rewriting a binding.
        from . import trade_cards,filled_quantity_exits
        source_card=deepcopy(source['originals'][source['bindings'][0]['card_id']]['card'])
        signal=deepcopy(source_card['prepared']['source']);signal['symbol']='ETH'
        meta=dict(universe=[dict(name='ETH',szDecimals=2)])
        card=trade_cards.prepare_card(signal,meta,rule_id='SOFTWARE_TEST',threshold_pct='1.5',record_kind='synthetic_test')
        bound=life.binding_from_card(card,source['account'],ROUTES,source['bindings'][0]['orders'])
        source['bindings']=[bound];source['originals']={card['card_id']:dict(card=card,
            draft=filled_quantity_exits.prepare_entry(card,meta,source['account'],ROUTES))}
        snap=closed(bound)
        for fill in snap['fills']:fill['fill_id']='hl:'+fill['fill_id']
        source['evidence']=dict(bindings=[bound],snapshot=snap)
        store=MemoryStore(target,source);venue=Venue();venue.t=T+3*evidence.DAY_MS
        controller=dispatch.Controller(store,venue,ROUTES);reader=PlannedReader(source['evidence'])
        reader.data['position']=dict(assetPositions=[])
        result=self.finish(controller,target,reader)
        self.assertEqual(result['state']['evidence']['snapshot']['fills'],[])
        self.assertEqual(store.load(source['bucket']),source)

    def test_no_retained_owned_anchor_fails_before_http(self):
        state,store,venue,controller,reader=self.setup_case(state=final_state(zero=True),staged=False)
        with self.assertRaisesRegex(gap.RecoveryError,'DURABLE_ANCHOR_REQUIRED'):
            self.tick(controller,state,reader)
        self.assertEqual(reader.calls,0)

    def test_start_is_durable_entry_fence_before_any_public_read(self):
        state,store,venue,controller,reader=self.setup_case(staged=False)
        result=self.tick(controller,state,reader)
        self.assertEqual(reader.calls,0)
        self.assertEqual(result['state'][gap.KEY]['chunks'],0)
        self.assertEqual(result['cursor_ms'],T)
        self.assertEqual(result['state']['evidence'],state['evidence'])
        self.assertEqual(store.events,['HISTORY_GAP_RECOVERY_STARTED'])

    def test_truncated_same_timestamp_anchor_retains_durable_fence(self):
        state,store,venue,controller,reader=self.setup_case()
        row=deepcopy(reader.data['fills'][-1]);reader.data['fills']=[{**row,'tid':n} for n in range(2000)]
        with self.assertRaisesRegex(evidence.SyncError,'FILL_HISTORY_TRUNCATED'):
            self.tick(controller,state,reader)
        self.assertEqual(store.load(state['bucket']),state)
        self.assertEqual(reader.calls,1)

    def test_same_process_concurrent_recovery_does_not_duplicate_public_reads(self):
        state,store,venue,controller,reader=self.setup_case();barrier=threading.Barrier(2)
        def run():
            local=PlannedReader(state['evidence']);barrier.wait(timeout=5)
            try:return self.tick(controller,state,local)['status'],local.calls
            except life.LifecycleError as exc:return str(exc),local.calls
        with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(lambda _:run(),range(2)))
        self.assertEqual(sorted(calls for status,calls in results),[0,4])
        self.assertEqual(sum(status=='HISTORY_GAP_RECOVERY_PROGRESS' for status,calls in results),1)

    def test_final_reuses_whole_account_inventory_and_blocks_unknown_other_coin(self):
        for mode in ('order','position'):
            with self.subTest(mode=mode):
                state,store,venue,controller,reader=self.setup_case()
                for _ in range(5):state=self.tick(controller,state,reader)['state']
                if mode=='order':reader.data['inventory'].append(dict(coin='ETH',oid=9999))
                else:reader.data['position']['assetPositions'].append(dict(position=dict(coin='ETH',szi='1')))
                with self.assertRaisesRegex(DispatchError,'UNOWNED_ACCOUNT_(ORDER|POSITION)'):
                    self.tick(controller,state,reader)
                self.assertEqual(store.load(state['bucket']),state)

    def test_other_account_bucket_changes_after_final_pass_abort_commit(self):
        state,store,venue,controller,reader=self.setup_case()
        for _ in range(5):state=self.tick(controller,state,reader)['state']
        other=final_state(n=2);other['symbol']='ETH';other['bucket']='e'*64
        other['bindings']=[];other['originals']={};other['evidence']=None
        store.values[other['bucket']]=deepcopy(other)
        read=reader.read;initial=reader.calls
        def changed(kind,*args,**kwargs):
            result=read(kind,*args,**kwargs)
            if reader.calls-initial==8:
                store.change(other['bucket'],other['revision'],'OTHER_BUCKET_CHANGED',venue.now(),
                             lambda conn,current:current.update(pending='prepared'))
            return result
        reader.read=changed
        with self.assertRaisesRegex(gap.RecoveryError,'ACCOUNT_BASIS_CHANGED'):
            self.tick(controller,state,reader)
        self.assertEqual(store.load(state['bucket']),state)

    def test_final_tail_finds_new_activity_without_promoting_live_clock(self):
        for mode in ('unknown-fill','position','inventory','evicted-anchor'):
            with self.subTest(mode=mode):
                state,store,venue,controller,reader=self.setup_case()
                for _ in range(5):state=self.tick(controller,state,reader)['state']
                before=deepcopy(state)
                if mode=='unknown-fill':reader.data['fills'].append({**reader.data['fills'][0],
                    'tid':99999,'oid':9999,'time':state[gap.KEY]['cursor_ms']+10})
                elif mode=='position':reader.data['position']['assetPositions'][0]['position']['szi']='1'
                elif mode=='inventory':reader.data['inventory'].append(deepcopy(reader.data['statuses']['10']['order']['order']))
                else:
                    read=reader.read;initial=reader.calls
                    def evict(kind,*args,**kwargs):
                        result=read(kind,*args,**kwargs)
                        return [] if reader.calls-initial==8 else result
                    reader.read=evict
                with self.assertRaises(life.LifecycleError):self.tick(controller,state,reader)
                self.assertEqual(store.load(state['bucket']),before)

    def test_no_pending_or_active_bucket_is_selected_for_recovery(self):
        for mode in ('pending','emergency','position','open','missing-terminal'):
            state=final_state()
            if mode=='pending':state['pending']='unknown'
            elif mode=='emergency':state['emergency']={'latched':True}
            elif mode=='position':state['evidence']['snapshot']['position_quantity']='1'
            elif mode=='open':state['evidence']['snapshot']['open_orders']=[{}]
            else:state['evidence']['snapshot']['terminal_orders'].pop()
            self.assertFalse(gap.needed(state,T+3*evidence.DAY_MS))


@unittest.skipUnless(CI,'disposable PostgreSQL required')
class RecoveryPostgresTests(NoExternal):
    def setUp(self):
        super().setUp();self.journal=PostgresJournal.for_ci(CI)
        with self.journal._transaction() as conn:
            for schema in (SCHEMA,'hl_testnet_recovery_rehearsal_v1','hl_testnet_execution_v1'):
                conn.execute(f'DROP SCHEMA IF EXISTS {schema} CASCADE')
        self.journal.bootstrap();self.store=DispatchStore(self.journal);self.store.initialize()
        self.venue=Venue();self.venue.t=T+3*evidence.DAY_MS
        self.controller=dispatch.Controller(self.store,self.venue,ROUTES)

    def seed(self,side='LONG'):
        fixture=final_state(side=side);state=self.store.create_bucket(fixture['account'],fixture['symbol'])
        def update(conn,current):
            for key in ('bindings','originals','evidence'):current[key]=deepcopy(fixture[key])
        final=self.store.change(state['bucket'],state['revision'],'CI_FINAL_FIXTURE',T,update)
        return gap.step(self.controller,final,reader=PlannedReader(final['evidence']))['state']

    def counts(self):
        with self.journal._transaction() as conn:
            return tuple(conn.execute(f'SELECT count(*) FROM {SCHEMA}.{name}').fetchone()[0]
                         for name in ('requests','nonces'))

    def test_restart_both_accounts_never_consumes_request_or_nonce_and_preserves_clock(self):
        for side in ('LONG','SHORT'):
            state=self.seed(side);reader=PlannedReader(state['evidence'])
            first=gap.step(self.controller,state,reader=reader)['state']
            reopened=DispatchStore(PostgresJournal.for_ci(CI))
            restarted=dispatch.Controller(reopened,self.venue,ROUTES)
            second=gap.step(restarted,reopened.load(state['bucket']),reader=reader)['state']
            self.assertEqual(second['evidence'],state['evidence'])
            self.assertEqual(second[gap.KEY]['cursor_ms'],T+2*gap.WINDOW_MS)
        self.assertEqual(self.counts(),(0,0));self.assertEqual(self.venue.sent,0)

    def test_concurrent_same_revision_advances_one_chunk_only(self):
        state=self.seed();barrier=threading.Barrier(2)
        def work():
            controller=dispatch.Controller(DispatchStore(PostgresJournal.for_ci(CI)),Venue(),ROUTES)
            controller.venue.t=self.venue.t;reader=PlannedReader(state['evidence']);barrier.wait(timeout=10)
            try:return gap.step(controller,state,reader=reader)['status'],reader.calls
            except life.LifecycleError as exc:return str(exc),reader.calls
        with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(lambda _:work(),range(2)))
        self.assertEqual(sum(status=='HISTORY_GAP_RECOVERY_PROGRESS' for status,calls in results),1)
        self.assertEqual(sorted(calls for status,calls in results),[0,4])
        self.assertEqual(self.store.load(state['bucket'])[gap.KEY]['chunks'],1)
        self.assertEqual(self.counts(),(0,0))

    def test_lost_commit_ack_reloads_committed_stage_without_replaying_window(self):
        state=self.seed();reader=PlannedReader(state['evidence']);change=self.store.change
        def lost(*args,**kwargs):
            change(*args,**kwargs)
            raise RuntimeError('CI_LOST_COMMIT_ACK')
        with patch.object(self.store,'change',side_effect=lost),self.assertRaisesRegex(RuntimeError,'LOST_COMMIT_ACK'):
            gap.step(self.controller,state,reader=reader)
        committed=self.store.load(state['bucket'])
        second=gap.step(self.controller,committed,reader=reader)['state']
        self.assertEqual(second[gap.KEY]['chunks'],2)
        self.assertEqual(second['evidence'],state['evidence'])

    def test_commit_failure_leaves_original_state_and_audit_unchanged(self):
        state=self.seed();reader=PlannedReader(state['evidence'])
        with patch.object(self.store,'change',side_effect=RuntimeError('CI_COMMIT_FAILED')):
            with self.assertRaisesRegex(RuntimeError,'COMMIT_FAILED'):gap.step(self.controller,state,reader=reader)
        self.assertEqual(self.store.load(state['bucket']),state)
        self.assertEqual(self.counts(),(0,0))

    def test_full_recovery_persists_final_event_and_exact_old_facts(self):
        state=self.seed();old=deepcopy(state['evidence']);reader=PlannedReader(old)
        while True:
            result=gap.step(self.controller,state,reader=reader);state=result['state']
            if result['status']=='HISTORY_GAP_RECOVERY_COMPLETE':break
        self.assertEqual(state['evidence']['snapshot']['at_ms'],self.venue.now())
        self.assertEqual(evidence.facts(state['evidence']['snapshot']),evidence.facts(old['snapshot']))
        with self.journal._transaction() as conn:
            events=[r[0] for r in conn.execute(f'SELECT event FROM {SCHEMA}.events WHERE bucket=%s ORDER BY revision',
                                              (state['bucket'],)).fetchall()]
        self.assertEqual(events[-6:],['HISTORY_GAP_RECOVERY_CHUNK']*5+['HISTORY_GAP_RECOVERY_COMPLETE'])
        self.assertEqual(self.counts(),(0,0))

    def test_two_processes_share_postgres_cas_and_only_one_cursor_commit(self):
        state=self.seed();context=multiprocessing.get_context('fork')
        barrier=context.Barrier(2);results=context.Queue()
        processes=[context.Process(target=process_recovery,args=(state,CI,self.venue.now(),barrier,results))
                   for _ in range(2)]
        try:
            for process in processes:process.start()
            replies=[results.get(timeout=20) for _ in processes]
            for process in processes:
                process.join(timeout=5);self.assertEqual(process.exitcode,0)
        finally:
            for process in processes:
                if process.is_alive():process.terminate();process.join(timeout=5)
        self.assertEqual(sum(status=='HISTORY_GAP_RECOVERY_PROGRESS' for status,calls in replies),1)
        self.assertEqual([calls for status,calls in replies],[4,4])
        self.assertEqual(self.store.load(state['bucket'])[gap.KEY]['chunks'],1)
        self.assertEqual(self.counts(),(0,0))

    def test_unknown_new_fills_keep_started_fence_and_do_not_write_main_evidence(self):
        state=self.seed();reader=PlannedReader(state['evidence'])
        reader.data['fills'].append({**reader.data['fills'][0],'tid':999999,'oid':8888,'time':T+100})
        with self.assertRaisesRegex(gap.RecoveryError,'NEW_OR_UNRESOLVED_ACTIVITY'):
            gap.step(self.controller,state,reader=reader)
        self.assertEqual(self.store.load(state['bucket']),state)
        self.assertEqual(state[gap.KEY]['chunks'],0)
        self.assertEqual(self.counts(),(0,0))

    def test_failed_final_commit_preserves_staged_cursor_and_old_live_clock(self):
        state=self.seed();reader=PlannedReader(state['evidence'])
        for _ in range(5):state=gap.step(self.controller,state,reader=reader)['state']
        with patch.object(self.store,'change',side_effect=RuntimeError('CI_FINAL_COMMIT_FAILED')):
            with self.assertRaisesRegex(RuntimeError,'FINAL_COMMIT_FAILED'):
                gap.step(self.controller,state,reader=reader)
        self.assertEqual(self.store.load(state['bucket']),state)
        self.assertEqual(state['evidence']['snapshot']['at_ms'],T)
        self.assertEqual(self.counts(),(0,0))
