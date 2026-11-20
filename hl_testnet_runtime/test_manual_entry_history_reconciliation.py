"""Old manual ENTRY proof through product collectors/reducers; no external I/O."""
from contextlib import contextmanager
from copy import deepcopy
from decimal import Decimal
from types import SimpleNamespace
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch

from . import experimental_external_activity as ext, formula_accounting
from .experimental_live_provider import _empty
from .test_experimental_execution_runtime import ROUTES, T


ACCOUNT = ROUTES['short_account']['account']
CID, RID, OID = 'a'*64, 'b'*64, '591986903854'


def raw_fill(tid, quantity, *, oid=OID, at=T+1000, side='A', price='87.975', fee='0.005938'):
    return dict(tid=tid, oid=int(oid), coin='HYPE', sz=quantity, px=price,
                fee=fee, feeToken='USDC', side=side, time=at)


class HistoryReader:
    """Only the public fill boundary is replaced, with an explicit call plan."""
    def __init__(self, rows):
        self.rows=deepcopy(rows);self.calls=[];self.plans=[];self.responses=None
    @contextmanager
    def observation_batch(self, bodies):
        self.plans.append(deepcopy(bodies));yield
    def read(self, kind, account, *, start, end):
        assert kind=='userFillsByTime' and account==ACCOUNT
        self.calls.append((kind,account,start,end))
        rows=self.rows if self.responses is None else self.responses.pop(0)
        return deepcopy([r for r in rows if start<=r['time']<=end])


class ManualEntryHistoryTests(unittest.TestCase):
    def setUp(self):
        self.io=patch('socket.socket.connect',side_effect=AssertionError('NO_EXTERNAL_IO'))
        self.io.start();self.addCleanup(self.io.stop)
        self.now=T+20*ext.sync.DAY_MS
        self.order=dict(a=0,b=False,p='87.94',s='5.42',r=False,
                        t={'limit':{'tif':'Gtc'}},c='0x'+'1'*32)
        self.rows=[raw_fill(468682583552118,'0.15'),
                   raw_fill(65596426522445,'3.94',price='87.973',fee='0.155976'),
                   raw_fill(90001,'4.09',oid='603910608725',at=T+12*ext.sync.DAY_MS,
                            side='B',price='74.25',fee='0.136655')]
        known=[dict(ext.sync.normalize_fill(r,ACCOUNT,'HYPE',1,self.now),origin='UNATTRIBUTED')
               for r in self.rows]
        key=ext.lane(ACCOUNT,'HYPE')
        self.state=dict(routes={k:v['account'] for k,v in ROUTES.items()},events=[],
            trades={CID:dict(cid=CID,account=ACCOUNT,symbol='HYPE',side='SHORT',phase='ENTRY_PENDING',
                orders={},order_legs={},entry_fills={},exit_fills={},entry_risk={'risk_usd':'2'},
                manual_management=dict(status=ext.HUMAN,reason='EXPLICIT_TRADER_CHOICE',
                                       accounting_allocation='EXTERNAL_ACTIVITY_UNALLOCATED'))},
            requests={RID:dict(phase='OUTCOME_UNKNOWN',attempts=1,attempt_at_ms=T,
                observed_oid=None,reply=dict(state='ACCEPTED_UNVERIFIED',oid=OID),
                proposal=dict(account=ACCOUNT,symbol='HYPE',card_id=CID,leg='ENTRY',operation='ENTRY',
                              action=dict(type='order',orders=[self.order],grouping='na')))},
            collector_checkpoints={key:_empty(ACCOUNT,'HYPE',T-100)},
            external_activity=dict(version=ext.VERSION,events=[],evicted_events=0,
                accounts={ACCOUNT:dict(account=ACCOUNT,fills=known,history_complete=False,
                    history_cursor_ms=T-10*ext.sync.DAY_MS,history_started_at_ms=T-11*ext.sync.DAY_MS)},
                human_managed={key:dict(account=ACCOUNT,symbol='HYPE',card_ids=[CID],
                    status=ext.HUMAN,reason='EXPLICIT_TRADER_CHOICE',automatic_resume=False)}))
        self.reader=HistoryReader(self.rows)

    def status(self, status='filled', size='1.33'):
        return dict(status='order',order=dict(status=status,statusTimestamp=T+1000,
            order=dict(oid=int(OID),cloid=self.order['c'],coin='HYPE',side='A',reduceOnly=False,
                origSz='5.42',sz=size,limitPx='87.94',orderType='Limit',isPositionTpsl=False,
                isTrigger=False,triggerPx='0',timestamp=T)))

    def observe(self, *, status='filled', size='1.33', orders=(), quantity='0', reader=True, cached=None):
        samples={('orderStatus',ACCOUNT,OID,None,None):self.status(status,size)}
        if cached is not None:
            cursor=self.state['external_activity']['accounts'][ACCOUNT]['history_cursor_ms']
            samples[('userFillsByTime',ACCOUNT,None,cursor-ext.sync.OVERLAP_MS,self.now)]=cached
        positions=[] if quantity=='0' else [dict(position=dict(coin='HYPE',szi=quantity))]
        return ext.observe(self.state,{ACCOUNT:[]},{ACCOUNT:dict(orders=list(orders),
            positions=dict(assetPositions=positions))},SimpleNamespace(samples=[samples]),
            now_ms=self.now,fill_reader=self.reader if reader else None)

    def test_actual_filled_partial_preserves_unknown_with_precise_remainder_reason(self):
        before=deepcopy(self.state['requests'][RID])
        obs=self.observe();ext.apply(self.state,obs)
        account=self.state['external_activity']['accounts'][ACCOUNT]
        self.assertTrue(account['history_complete'])
        self.assertEqual(account['history_proof']['kind'],'ACCOUNT_LIFETIME_TWO_UNSATURATED_PASSES')
        self.assertEqual([(c[2],c[3]) for c in self.reader.calls],[(1,self.now)]*2)
        from .request_budget import request_weight
        self.assertEqual(len(self.reader.plans),1)
        self.assertEqual(sum(request_weight('/info',b) for b in self.reader.plans[0]),240)
        self.assertEqual(self.state['requests'][RID],before)
        self.assertEqual(self.state['trades'][CID]['entry_fills'],{})
        error=self.state['external_activity']['request_observation_errors'][RID]
        self.assertEqual(error['code'],'BOT_TERMINAL_REMAINDER_UNPROVEN')
        self.assertEqual((error['requested_quantity'],error['observed_quantity'],error['reported_size']),
                         ('5.42','4.09','1.33'))
        self.assertTrue(ext.managed(self.state,ACCOUNT,'HYPE'))
        self.assertEqual(len(account['fills']),3)

    def test_late_complete_entry_fill_remains_eligible_and_resolves_only_from_full_history(self):
        extra=raw_fill(777777,'1.33',price='87.974',fee='0.05')
        close=raw_fill(888888,'1.33',oid='603910608726',at=T+12*ext.sync.DAY_MS,
                       side='B',price='74.25',fee='0.04')
        self.rows += [extra,close];self.reader.rows=deepcopy(self.rows)
        self.state['external_activity']['accounts'][ACCOUNT]['fills'] += [
            dict(ext.sync.normalize_fill(r,ACCOUNT,'HYPE',1,self.now),origin='UNATTRIBUTED') for r in (extra,close)]
        self.assertEqual(ext.manual_history_accounts(self.state),{ACCOUNT})
        obs=self.observe(size='0');ext.apply(self.state,obs)
        self.assertEqual(self.state['requests'][RID]['phase'],'OBSERVED')
        self.assertEqual(sum(Decimal(f['quantity']) for f in self.state['trades'][CID]['entry_fills'].values()),Decimal('5.42'))
        self.assertTrue(ext.managed(self.state,ACCOUNT,'HYPE'))
        self.now+=31000;ext.apply(self.state,self.observe(size='0'))
        self.assertTrue(ext.closure_verified(self.state['trades'][CID]))

    def test_nonfinite_reported_size_does_not_escape_or_release(self):
        obs=self.observe(size='NaN')
        self.assertFalse(obs['owned_request_observations'])
        ext.apply(self.state,obs)
        self.assertEqual(self.state['requests'][RID]['phase'],'OUTCOME_UNKNOWN')
        self.assertTrue(ext.managed(self.state,ACCOUNT,'HYPE'))

    def test_frozen_actual_four_hype_fills_preserve_unknown_and_manual_accounting(self):
        frozen=json.loads((Path(__file__).parent/'fixtures/manual_hype_entry_reconciliation_20261120.json').read_text())
        self.now=frozen['proof_fixed_end_ms'];self.rows=frozen['hype_fills_after_request']
        self.reader.rows=deepcopy(self.rows)
        request=self.state['requests'][RID]
        request['proposal']['action']=frozen['original_order_action']
        request['attempt_at_ms']=frozen['attempt_at_ms']
        self.state['collector_checkpoints'][ext.lane(ACCOUNT,'HYPE')]['at_ms']=frozen['pre_send_checkpoint_ms']
        account=self.state['external_activity']['accounts'][ACCOUNT]
        account.update(history_cursor_ms=frozen['external_history_cursor_ms'],
            history_started_at_ms=frozen['external_history_started_at_ms'],
            fills=[dict(ext.sync.normalize_fill(r,ACCOUNT,'HYPE',1,self.now),origin='UNATTRIBUTED') for r in self.rows])
        with patch.object(self,'status',return_value=frozen['order_status_both_passes']):
            obs=self.observe();ext.apply(self.state,obs)
        self.assertEqual(self.state['requests'][RID]['phase'],'OUTCOME_UNKNOWN')
        self.assertEqual(self.state['external_activity']['request_observation_errors'][RID]['code'],
                         'BOT_TERMINAL_REMAINDER_UNPROVEN')
        self.assertEqual(len(self.state['external_activity']['accounts'][ACCOUNT]['fills']),4)
        self.assertEqual(sum(Decimal(f['fee']) for f in self.state['external_activity']['accounts'][ACCOUNT]['fills']),
                         Decimal('0.298569'))
        self.assertFalse(self.state['trades'][CID]['entry_fills'])
        self.assertNotIn('settlement',self.state['trades'][CID])
        self.assertNotIn('formula_accounting',self.state)
        self.assertTrue(ext.managed(self.state,ACCOUNT,'HYPE'))
        # Only the four relevant frozen facts are replayed here. The production
        # 113-row coverage observation is recorded evidence, not fabricated data.
        self.assertEqual(len(self.reader.calls),2)

    def test_later_proven_cancel_retains_partial_fees_and_requires_two_flat_observations(self):
        ext.apply(self.state,self.observe())
        self.state=deepcopy(self.state)  # restart after completed history, unresolved terminal remainder
        self.now+=31000
        obs=self.observe(status='canceled');ext.apply(self.state,obs)
        trade=self.state['trades'][CID]
        self.assertEqual(self.state['requests'][RID]['phase'],'OBSERVED')
        self.assertEqual(sum(Decimal(f['quantity']) for f in trade['entry_fills'].values()),Decimal('4.09'))
        self.assertTrue(ext.managed(self.state,ACCOUNT,'HYPE'))
        formula_accounting.retain_fees(self.state,{})
        self.assertEqual(sum(Decimal(f['fee']) for f in trade['fee_facts'].values()),Decimal('0.161914'))
        ext.apply(self.state,obs)  # duplicate observation cannot supply the second flat proof
        self.assertTrue(ext.managed(self.state,ACCOUNT,'HYPE'))
        self.state=deepcopy(self.state);self.now+=31000
        ext.apply(self.state,self.observe(status='canceled'))
        trade=self.state['trades'][CID]
        self.assertTrue(ext.closure_verified(trade))
        self.assertEqual(len(trade['manual_closure_snapshot']['fills']),3)
        self.assertEqual(trade['manual_management']['reason'],'EXPLICIT_TRADER_CHOICE')
        self.assertNotIn('settlement',trade)
        formula_accounting.retain_fees(self.state,{})
        self.assertEqual(len(trade['fee_facts']),2)
        self.assertEqual(self.state['requests'][RID]['attempts'],1)

    def test_cached_old_interval_cannot_bypass_new_lifetime_proof(self):
        obs=self.observe(reader=False,cached=self.rows)
        self.assertFalse(obs['accounts'][ACCOUNT]['history_complete'])
        self.assertFalse(obs['owned_request_observations'])
        self.assertEqual(self.reader.calls,[])

    def test_missing_known_fill_cannot_advance_cursor(self):
        before=deepcopy(self.state)
        self.reader.rows=self.rows[1:]
        obs=self.observe();ext.apply(self.state,obs)
        self.assertFalse(obs['accounts'][ACCOUNT]['history_complete'])
        self.assertEqual(self.state['external_activity']['accounts'][ACCOUNT]['history_cursor_ms'],
                         before['external_activity']['accounts'][ACCOUNT]['history_cursor_ms'])
        self.assertEqual(self.state['requests'],before['requests'])

    def test_changed_second_pass_and_lost_reply_are_closed(self):
        altered=deepcopy(self.rows);altered[-1]['fee']='1'
        self.reader.responses=[self.rows,altered]
        obs=self.observe();self.assertFalse(obs['accounts'][ACCOUNT]['history_complete'])
        self.assertFalse(obs['owned_request_observations'])
        self.reader.responses=None
        with patch.object(self.reader,'read',side_effect=OSError('lost')):
            obs=self.observe()
        self.assertFalse(obs['accounts'][ACCOUNT]['history_complete'])

    def test_all_time_saturation_and_wrong_account_anchor_do_not_certify(self):
        for count,timestamps in ((2000,1),(500,500),(500,1)):
            with self.subTest(count=count,timestamps=timestamps):
                self.reader.rows=[raw_fill(100000+i,'1',oid='7',at=T-5000+i%timestamps)
                                  for i in range(count)]
                obs=self.observe()
                self.assertEqual(obs['accounts'][ACCOUNT]['history_error'],'EXTERNAL_LIFETIME_HISTORY_SATURATED')
        self.reader.rows=self.rows
        other=ROUTES['long_account']['account']
        self.state['collector_checkpoints']['wrong']=dict(account=other,history_complete=True,
            fills=[dict(ext.sync.normalize_fill(raw_fill(8,'1',at=T-12*ext.sync.DAY_MS),other,'HYPE',1,self.now))])
        obs=self.observe()
        self.assertEqual(obs['accounts'][ACCOUNT]['history_proof']['start_ms'],1)

    def test_all_time_499_rows_can_prove_history_without_releasing_partial_terminal(self):
        extras=[raw_fill(100000+i,'1',oid='7',at=T-5000) for i in range(496)]
        self.reader.rows=self.rows+extras
        obs=self.observe()
        self.assertTrue(obs['accounts'][ACCOUNT]['history_complete'])
        self.assertEqual(len(obs['accounts'][ACCOUNT]['fills']),499)
        self.assertFalse(obs['owned_request_observations'])
        self.assertEqual(obs['owned_request_errors'][RID]['code'],'BOT_TERMINAL_REMAINDER_UNPROVEN')

    def test_history_exception_or_budget_denial_keeps_manual_and_request(self):
        before=deepcopy(self.state['requests'])
        with patch.object(self.reader,'read',side_effect=ValueError('TESTNET_REQUEST_BUDGET_EXHAUSTED')):
            obs=self.observe()
        ext.apply(self.state,obs)
        self.assertEqual(obs['accounts'][ACCOUNT]['history_error'],'TESTNET_REQUEST_BUDGET_EXHAUSTED')
        self.assertEqual(self.state['requests'],before)
        self.assertTrue(ext.managed(self.state,ACCOUNT,'HYPE'))

    def test_slow_first_optional_read_defers_second_without_advancing_history(self):
        with patch.object(ext.time,'monotonic',side_effect=[0,0,4.1]):
            obs=self.observe()
        self.assertEqual(len(self.reader.calls),1)
        self.assertEqual(obs['accounts'][ACCOUNT]['history_error'],'EXTERNAL_HISTORY_OBSERVATION_DEFERRED')
        self.assertFalse(obs['accounts'][ACCOUNT]['history_complete'])

    def test_eligibility_requires_sent_manual_entry_known_fill_and_pre_send_checkpoint(self):
        self.assertEqual(ext.manual_history_accounts(self.state),{ACCOUNT})
        for mutate in (lambda s:s['requests'][RID].update(attempts=0),
                       lambda s:s['requests'][RID]['proposal'].update(leg='STOP'),
                       lambda s:s['external_activity'].update(human_managed={}),
                       lambda s:s['external_activity']['accounts'][ACCOUNT].update(fills=[]),
                       lambda s:s['collector_checkpoints'][ext.lane(ACCOUNT,'HYPE')].update(at_ms=T+1)):
            state=deepcopy(self.state);mutate(state)
            self.assertEqual(ext.manual_history_accounts(state),set())

    def provider_fixture(self):
        from .test_experimental_live_provider import ProviderTests
        fx=ProviderTests(methodName='runTest');fx.setUp();self.addCleanup(fx.doCleanups)
        fx.state.update(deepcopy(self.state));fx.exchange.t=self.now
        fx.state['trades'][CID].update(source={'family':'maxpain'},role='short_account',
            quantity='5.42',prices=dict(entry='87.94',stop='89.6988',take_profit='86.1812'))
        original=fx.raw.read
        def read(kind,account=None,**kw):
            account=account or kw.get('user')
            if kind=='userFillsByTime':
                return self.reader.read(kind,account,start=kw['start'],end=kw['end'])
            if kind=='frontendOpenOrders':return []
            if kind=='clearinghouseState':return dict(assetPositions=[])
            if kind=='orderStatus':return self.status()
            return original(kind,account,**kw)
        fx.raw.read=read
        fx.provider._lookup_reader=lambda account,cloid:self.status()
        return fx

    def test_real_provider_collect_reaches_manual_history_and_keeps_remainder_blocked(self):
        fx=self.provider_fixture()
        context=fx.provider.collect(fx.state,entries_enabled=False)
        observation=context['external_observation']
        self.assertTrue(observation['accounts'][ACCOUNT]['history_complete'])
        self.assertEqual(observation['owned_request_errors'][RID]['code'],'BOT_TERMINAL_REMAINDER_UNPROVEN')
        self.assertEqual(sum(c[2]==1 for c in self.reader.calls),2)
        self.assertEqual(context['blocked_lanes'][ext.lane(ACCOUNT,'HYPE')],ext.HUMAN)
        self.assertTrue(context['inventory_complete'])

    def test_additional_pending_stop_prevents_optional_history_read(self):
        fx=self.provider_fixture()
        other=deepcopy(fx.state['requests'][RID]);other['proposal']['leg']='STOP'
        # The additional unknown request keeps its own evidence/dispatch path;
        # only optional account-wide reads are denied by this test.
        other['proposal']['action']['orders'][0]['c']='0x'+'2'*32
        fx.state['requests']['c'*64]=other
        context=fx.provider.collect(fx.state,entries_enabled=False)
        self.assertFalse(any(c[2]==1 for c in self.reader.calls))
        self.assertFalse(context['external_observation']['accounts'][ACCOUNT]['history_complete'])
        self.assertEqual(fx.state['requests']['c'*64]['phase'],'OUTCOME_UNKNOWN')

    def test_pinned_history_capacity_failure_is_account_scoped_before_apply(self):
        account=self.state['external_activity']['accounts'][ACCOUNT]
        account['history_cursor_ms']=self.now-31000
        extras=[raw_fill(100000+i,'1',oid='7',at=T+2000+i) for i in range(4093)]
        account['fills'] += [dict(ext.sync.normalize_fill(r,ACCOUNT,'HYPE',1,self.now),origin='UNATTRIBUTED')
                             for r in extras]
        self.reader.rows=[raw_fill(999999,'1',oid='9',at=self.now-1)]
        obs=self.observe()
        self.assertFalse(obs['accounts'][ACCOUNT]['history_complete'])
        ext.apply(self.state,obs)  # overflow must not escape into the cycle transaction
        self.assertEqual(len(self.state['external_activity']['accounts'][ACCOUNT]['fills']),4096)


@unittest.skipUnless(os.environ.get('HL_JOURNAL_CI_URL'),'Requires disposable loopback PostgreSQL')
class ManualHistoryPostgresTests(unittest.TestCase):
    def test_partial_proof_lost_commit_reply_restart_and_two_flat_observations(self):
        from .postgres_journal import PostgresJournal
        from .filled_dispatch_store import DispatchStore
        from . import experimental_live_state as live
        from .test_experimental_live_state import LIVE_ROUTES
        case=ManualEntryHistoryTests(methodName='runTest');case.setUp();self.addCleanup(case.doCleanups)
        journal=PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL']);journal.bootstrap()
        DispatchStore(journal).initialize()
        with journal._transaction() as conn:
            conn.execute('DROP SCHEMA IF EXISTS '+live.PG_SCHEMA+' CASCADE')
        store=live.TestnetExecutionState.for_ci(journal)
        store.initialize(LIVE_ROUTES,not_before_ms=T-11*ext.sync.DAY_MS)
        self.assertEqual(store.load()['routes'],case.state['routes'])
        case.state['requests'][RID]['domain']='mainnet'
        seed=deepcopy(case.state)
        store.mutate(lambda s:s.update({k:v for k,v in seed.items() if k!='routes'}))
        def commit(obs, lose_reply=False):
            def transition(state):
                ext.apply(state,obs);formula_accounting.retain_fees(state,{})
            store.mutate(transition)
            if lose_reply:raise OSError('COMMIT_REPLY_LOST_AFTER_COMMIT')
        case.state=store.load();commit(case.observe())
        self.assertEqual(store.load()['requests'][RID]['phase'],'OUTCOME_UNKNOWN')
        case.state=store.load();case.now+=31000;obs=case.observe(status='canceled')
        with self.assertRaisesRegex(OSError,'COMMIT_REPLY_LOST'):
            commit(obs,lose_reply=True)
        store=live.TestnetExecutionState.for_ci(PostgresJournal.for_ci(os.environ['HL_JOURNAL_CI_URL']))
        commit(obs)  # no replayed request or duplicate fee on an unknown acknowledgement
        saved=store.load();self.assertTrue(ext.managed(saved,ACCOUNT,'HYPE'))
        self.assertEqual(saved['requests'][RID]['attempts'],1)
        self.assertEqual(len(saved['trades'][CID]['entry_fills']),2)
        self.assertEqual(len(saved['trades'][CID]['fee_facts']),2)
        case.state=saved;case.now+=31000;commit(case.observe(status='canceled'))
        saved=store.load();self.assertTrue(ext.closure_verified(saved['trades'][CID]))
        self.assertEqual(len(saved['trades'][CID]['manual_closure_snapshot']['fills']),3)
        self.assertNotIn('settlement',saved['trades'][CID])
        self.assertNotIn('formula_accounting',saved)
        self.assertEqual(len(saved['requests']),1)


if __name__=='__main__':unittest.main()
