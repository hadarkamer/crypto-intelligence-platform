"""Independent adversarial review of manual observation; no external I/O."""
from copy import deepcopy
from decimal import Decimal
from types import SimpleNamespace
import json
from pathlib import Path
import tempfile
import unittest

from . import experimental_external_activity as ext
from .test_experimental_execution_runtime import ROUTES, T
from . import test_experimental_live_service as full_connection
from . import test_execution_history_pg as pg_fixture
from experimental_execution_fixtures import r2732_message


ACCOUNT = ROUTES['long_account']['account']
OTHER = ROUTES['short_account']['account']


def empty_state():
    return dict(routes={r: v['account'] for r, v in ROUTES.items()},
                trades={}, requests={})


def order(oid=901, coin='SOL'):
    return dict(oid=oid, coin=coin, side='A', sz='5', origSz='5',
                limitPx='90', triggerPx='90', orderType='Stop Market',
                reduceOnly=True, isTrigger=True, isPositionTpsl=False,
                timestamp=T)


def fill(*, tid=7001, oid=901, coin='SOL', quantity='2', at=T+1000):
    return dict(tid=tid, oid=oid, coin=coin, sz=quantity, px='95',
                fee='0.01', feeToken='USDC', side='A', time=at)


def inventory(*, orders=(), quantity='0', symbol='SOL'):
    positions=[] if quantity=='0' else [dict(position=dict(coin=symbol, szi=quantity))]
    return dict(orders=list(orders), positions=dict(assetPositions=positions))


class ReviewTests(unittest.TestCase):
    def observed(self, state, *, inv=None, fills=(), now=T+20000):
        cache=SimpleNamespace(samples=[{('userFillsByTime', ACCOUNT, None,
            max(1,now-ext.sync.OVERLAP_MS),now):list(fills)}])
        return ext.observe(state, {ACCOUNT:[],OTHER:[]},
            {ACCOUNT: inv or inventory()}, cache, now_ms=now)

    def test_external_observation_does_not_create_trade_or_request(self):
        state=empty_state();before=deepcopy(state)
        obs=self.observed(state,inv=inventory(orders=[order()],quantity='5'),fills=[fill()])
        self.assertEqual(state,before)
        ext.apply(state,obs)
        self.assertEqual(state['trades'],{})
        self.assertEqual(state['requests'],{})
        self.assertEqual(state['external_activity']['accounts'][ACCOUNT]['positions'],{'SOL':'5'})
        self.assertEqual(state['external_activity']['accounts'][ACCOUNT]['fills'][0]['origin'],'EXTERNAL')

    def test_repeat_and_restart_do_not_duplicate_external_fill(self):
        state=empty_state();obs=self.observed(state,fills=[fill()])
        ext.apply(state,obs);saved=deepcopy(state)
        ext.apply(state,obs)
        self.assertEqual(state,saved)
        restarted=deepcopy(saved);ext.apply(restarted,obs)
        self.assertEqual(restarted,saved)

    def test_changed_external_fill_is_rejected_atomically_by_caller(self):
        state=empty_state();ext.apply(state,self.observed(state,fills=[fill()]))
        with self.assertRaisesRegex(ext.ExternalActivityError,'FILL_FACT_CHANGED'):
            self.observed(state,fills=[fill(quantity='3')])
        changed=self.observed(empty_state(),fills=[fill(quantity='3')])
        candidate=deepcopy(state)
        with self.assertRaisesRegex(ext.ExternalActivityError,'FILL_FACT_CHANGED'):
            ext.apply(candidate,changed)
        self.assertEqual(state['external_activity']['accounts'][ACCOUNT]['fills'][0]['quantity'],'2')

    def test_manual_handoff_survives_flat_account_and_restart(self):
        state=empty_state();state['trades']['active']=dict(account=ACCOUNT,symbol='SOL',phase='OPEN',orders={})
        obs=self.observed(state,inv=inventory(orders=[order()],quantity='5'))
        ext.apply(state,obs)
        self.assertTrue(ext.managed(state,ACCOUNT,'SOL'))
        ext.apply(state,self.observed(state,now=T+20001))
        self.assertTrue(ext.managed(deepcopy(state),ACCOUNT,'SOL'))
        self.assertFalse(ext.managed(state,OTHER,'SOL'))
        self.assertFalse(ext.managed(state,ACCOUNT,'BTC'))

    def test_persistent_handoff_keeps_unrelated_admission_clear_after_facts_age_out(self):
        state=empty_state();state['trades']['active']=dict(account=ACCOUNT,symbol='SOL',phase='OPEN',orders={})
        ext.apply(state,self.observed(state,inv=inventory(orders=[order()],quantity='5')))
        current=self.observed(deepcopy(state),now=T+20000+3*ext.sync.OVERLAP_MS)
        old_bucket=dict(account=ACCOUNT,symbol='SOL',pending=None,bindings=[],
            evidence=dict(snapshot=dict(position_quantity='5',terminal_orders=[])))
        self.assertTrue(ext.review_inventory(ACCOUNT,[old_bucket],[],dict(assetPositions=[]),
            role='long_account',observation=current,target_symbol='BTC'))

    def test_external_other_coin_does_not_close_bot_account_admission(self):
        state=empty_state();inv=inventory(orders=[order()],quantity='5')
        obs=self.observed(state,inv=inv)
        self.assertTrue(ext.review_inventory(ACCOUNT,[],inv['orders'],inv['positions'],
            role='long_account',observation=obs,target_symbol='BTC'))
        with self.assertRaisesRegex(ext.ExternalActivityError,ext.OCCUPIED):
            ext.review_inventory(ACCOUNT,[],inv['orders'],inv['positions'],
                role='long_account',observation=obs,target_symbol='SOL')

    def test_opposite_manual_position_does_not_rewrite_bot_route(self):
        state=empty_state();inv=inventory(quantity='-5')
        obs=self.observed(state,inv=inv)
        self.assertTrue(ext.review_inventory(ACCOUNT,[],inv['orders'],inv['positions'],
            role='long_account',observation=obs,target_symbol='BTC'))
        ext.apply(state,obs)
        self.assertEqual(state['routes']['long_account'],ACCOUNT)
        self.assertEqual(state['external_activity']['accounts'][ACCOUNT]['positions']['SOL'],'-5')

    def test_changed_inventory_certificate_cannot_hide_external_order(self):
        state=empty_state();inv=inventory(orders=[order()],quantity='5')
        obs=self.observed(state,inv=inv)
        changed=deepcopy(inv['orders']);changed[0]['sz']='4'
        with self.assertRaisesRegex(ext.ExternalActivityError,'CERTIFICATE_CHANGED'):
            ext.review_inventory(ACCOUNT,[],changed,inv['positions'],
                role='long_account',observation=obs,target_symbol='BTC')

    def test_manual_only_venue_symbols_are_not_limited_to_bot_asset_names(self):
        for symbol in ('kPEPE','@107','xyz:TSLA'):
            with self.subTest(symbol=symbol):
                state=empty_state();inv=inventory(orders=[order(coin=symbol)])
                obs=self.observed(state,inv=inv)
                self.assertTrue(ext.review_inventory(ACCOUNT,[],inv['orders'],inv['positions'],
                    role='long_account',observation=obs,target_symbol='BTC'))

    def test_pending_bot_request_is_not_reclassified_as_manual(self):
        state=empty_state()
        state['trades']['active']=dict(account=ACCOUNT,symbol='SOL',phase='OPEN',orders={})
        state['requests']['pending']=dict(phase='OUTCOME_UNKNOWN',observed_oid=None,
            attempt_at_ms=T,proposal=dict(account=ACCOUNT,symbol='SOL',card_id='active',
                action=dict(type='order',orders=[dict(c='0x'+'1'*32)])))
        obs=self.observed(state,inv=inventory(orders=[order()],quantity='5'),fills=[fill()])
        self.assertEqual(obs['accounts'][ACCOUNT]['orders'][0]['origin'],'UNATTRIBUTED')
        self.assertEqual(obs['accounts'][ACCOUNT]['fills'][0]['origin'],'UNATTRIBUTED')
        self.assertFalse(obs['human_managed'])

    def test_old_manual_fill_does_not_handoff_later_bot_trade(self):
        state=empty_state()
        state['trades'].update(old=dict(cid='old',account=ACCOUNT,symbol='SOL',phase='CLOSED',orders={}),
            new=dict(cid='new',account=ACCOUNT,symbol='SOL',phase='OPEN',orders={}))
        for cid,stamp in (('old',T),('new',T+10000)):
            state['requests'][cid]=dict(phase='OBSERVED',observed_oid=None,attempt_at_ms=stamp,
                proposal=dict(account=ACCOUNT,symbol='SOL',card_id=cid,leg='ENTRY',operation='ENTRY',
                    action=dict(type='order',orders=[dict(c='0x'+('1' if cid=='old' else '2')*32)])))
        obs=self.observed(state,fills=[fill(at=T+5000)])
        self.assertTrue(obs['accounts'][ACCOUNT]['history_complete'])
        self.assertFalse(obs['human_managed'], 'historical manual fill predates current bot exposure')

    def test_duplicate_fill_identity_across_symbols_is_not_double_recorded(self):
        state=empty_state();obs=self.observed(state,fills=[fill(),fill(coin='BTC')])
        with self.assertRaisesRegex(ext.ExternalActivityError,'FILL_FACT_CHANGED'):
            ext.apply(state,obs)

    def test_manual_release_uses_alert_decision_not_receipt_or_source_reference(self):
        from approved_alert_fixtures import maxpain_alert
        state=empty_state()
        state['external_activity']=dict(retired_markets={
            ext.lane(ACCOUNT,'HYPE'):dict(confirmed_at_ms=T)})
        old=maxpain_alert(approved_ms=T,as_of_ms=T+10000)
        self.assertEqual(ext.entry_reason(state,ACCOUNT,old),'NEW_ALERT_REQUIRED_AFTER_MANUAL_RELEASE')
        later=maxpain_alert(approved_ms=T+60000)
        self.assertIsNone(ext.entry_reason(state,ACCOUNT,later),
            'new approval remains valid when its underlying source predates release')
        self.assertIsNone(ext.entry_reason(state,OTHER,old),'release cutoff is account-scoped')
        self.assertIsNone(ext.entry_reason(state,ACCOUNT,dict(old,symbol='SOL')),
            'release cutoff is coin-scoped')

    def test_reapproval_cannot_change_original_alert_decision(self):
        from approved_alert_fixtures import maxpain_alert
        import approved_alert_contract as contract
        from .experimental_plan_store import PlanStoreError, reduce_source
        original=maxpain_alert(approved_ms=T)
        now=contract.iso_ms(T+10000);not_before=contract.iso_ms(T-60000)
        record,_=reduce_source(None,original,now=now,not_before=not_before,domain='testnet')
        reapproved=maxpain_alert(approved_ms=T+60000)
        self.assertEqual(original['occurrence_id'],reapproved['occurrence_id'])
        with self.assertRaisesRegex(PlanStoreError,'IMMUTABLE_PLAN_CHANGED'):
            reduce_source(record,reapproved,now=contract.iso_ms(T+70000),
                not_before=not_before,domain='testnet')


class IntegratedReviewTests(unittest.TestCase):
    def setUp(self):
        self.fx=full_connection.FullConnectionTests(methodName='runTest')
        self.fx.setUp();self.addCleanup(self.fx.doCleanups)
        self.msg=r2732_message(entry=2.3,decision_ms=T-60000)
        self.fx.seed(self.msg)

    def test_external_order_with_unresolved_bot_entry_does_not_strand_request(self):
        first=self.fx.service.tick()
        self.assertEqual(first.get('operation'),'ENTRY')
        before=self.fx.store.load();rid=next(iter(before['requests']))
        self.assertEqual(before['requests'][rid]['phase'],'OUTCOME_UNKNOWN')
        account=before['routes']['short_account'];symbol=self.msg['symbol']
        manual=order(oid=909,coin=symbol)
        manual.update(side='B',limitPx='2.5',triggerPx='2.5',timestamp=self.fx.oracle.t)
        original=self.fx.raw.read
        def read(kind,account_arg=None,**kw):
            result=original(kind,account_arg,**kw)
            if kind=='frontendOpenOrders' and (account_arg or kw.get('user'))==account:
                return result+[deepcopy(manual)]
            return result
        self.fx.raw.read=read
        sent=len(self.fx.http)
        for _ in range(3):
            self.fx.service.tick()
        after=self.fx.store.load()
        self.assertEqual(len(self.fx.http),sent,'human intervention must not race new stop/TP sends')
        self.assertEqual(after['requests'][rid]['phase'],'OBSERVED','known bot entry must still resolve')
        self.assertTrue(ext.managed(after,account,symbol))
        self.assertTrue(any(e['kind']==ext.HUMAN for e in after['external_activity']['events']))

    def test_handoff_cannot_accept_wrong_side_fill_as_owned_bot_evidence(self):
        self.assertEqual(self.fx.service.tick().get('operation'),'ENTRY')
        before=self.fx.store.load();rid=next(iter(before['requests']))
        account=before['routes']['short_account'];manual=order(oid=909,coin=self.msg['symbol'])
        manual.update(side='B',limitPx='2.5',triggerPx='2.5',timestamp=self.fx.oracle.t)
        read=self.fx.raw.read
        def corrupted(kind,account_arg=None,**kw):
            value=read(kind,account_arg,**kw)
            if (account_arg or kw.get('user'))!=account:return value
            if kind=='frontendOpenOrders':return value+[deepcopy(manual)]
            if kind=='userFillsByTime':
                for row in value:row['side']='B'  # Bot short ENTRY is a sell.
            return value
        self.fx.raw.read=corrupted
        self.fx.service.tick()
        after=self.fx.store.load()
        self.assertEqual(after['requests'][rid]['phase'],'OUTCOME_UNKNOWN')
        self.assertFalse(after['trades'][self.msg['occurrence_id']]['entry_fills'])

    def test_durable_handoff_between_preparation_and_send_prevents_exit_transport(self):
        self.assertEqual(self.fx.service.tick().get('operation'),'ENTRY')
        previous=self.fx.port.context_loader
        injected=[]
        def final_context(request):
            p=request['proposal']
            if p['operation']=='CREATE_EXIT' and not injected:
                injected.append(True)
                def handoff(state):
                    ext.apply(state,dict(version=ext.VERSION,accounts={},human_managed={
                        ext.lane(p['account'],p['symbol']):dict(account=p['account'],symbol=p['symbol'],
                            at_ms=self.fx.oracle.t,reason='EXTERNAL_OPEN_ORDER',order_id='909')}))
                self.fx.store.mutate(handoff)
            return previous(request)
        self.fx.port.context_loader=final_context
        sent=len(self.fx.http)
        self.fx.service.tick()
        self.assertTrue(injected)
        self.assertEqual(len(self.fx.http),sent)
        exit_requests=[r for r in self.fx.store.load()['requests'].values()
            if r['proposal']['operation']=='CREATE_EXIT']
        self.assertEqual(exit_requests[0]['phase'],'ABORTED_UNSENT')

    def test_durable_handoff_between_preparation_and_send_prevents_entry_transport(self):
        previous=self.fx.port.context_loader
        injected=[]
        def final_context(request):
            p=request['proposal']
            if p['operation']=='ENTRY' and not injected:
                injected.append(True)
                def handoff(state):
                    ext.apply(state,dict(version=ext.VERSION,accounts={},human_managed={
                        ext.lane(p['account'],p['symbol']):dict(account=p['account'],symbol=p['symbol'],
                            at_ms=self.fx.oracle.t,reason='EXTERNAL_OPEN_ORDER',order_id='909')}))
                self.fx.store.mutate(handoff)
            return previous(request)
        self.fx.port.context_loader=final_context
        self.fx.service.tick()
        self.assertTrue(injected)
        self.assertEqual(self.fx.http,[])
        requests=list(self.fx.store.load()['requests'].values())
        self.assertEqual(len(requests),1)
        self.assertEqual(requests[0]['phase'],'ABORTED_UNSENT')

    def test_same_coin_notification_after_collection_prevents_stale_exit_transport(self):
        self._notification_after_collection(reconnect=False)

    def test_reconnected_same_coin_notification_prevents_stale_exit_transport(self):
        self._notification_after_collection(reconnect=True)

    def _notification_after_collection(self, *, reconnect):
        from .fill_wakeups import FillWakeups, CHANNELS
        feed=FillWakeups(self.fx.release['routes'])
        self.fx.provider.safety.feed=feed
        for account in self.fx.release['routes'].values():
            generation=feed._opened(account)
            for channel in CHANNELS:
                feed._receive(account,generation,json.dumps(dict(channel='subscriptionResponse',
                    data=dict(method='subscribe',subscription=dict(type=channel,user=account)))))
            feed._receive(account,generation,json.dumps(dict(channel='userFills',
                data=dict(user=account,isSnapshot=True,fills=[]))))
        self.assertEqual(self.fx.service.tick().get('operation'),'ENTRY')
        previous=self.fx.port.context_loader;injected=[]
        def final_context(request):
            p=request['proposal']
            if p['operation']=='CREATE_EXIT' and not injected:
                injected.append(True)
                token=feed.begin_reconciliation(p['account'])
                if reconnect:
                    generation=feed._opened(p['account'])
                    for channel in CHANNELS:
                        feed._receive(p['account'],generation,json.dumps(dict(channel='subscriptionResponse',
                            data=dict(method='subscribe',subscription=dict(type=channel,user=p['account'])))))
                    feed._receive(p['account'],generation,json.dumps(dict(channel='userFills',
                        data=dict(user=p['account'],isSnapshot=True,fills=[]))))
                    token=feed.begin_reconciliation(p['account'])
                accepted=feed._receive(p['account'],token.generation,json.dumps(dict(channel='orderUpdates',
                    data=[dict(order=dict(coin=p['symbol'],oid=909),status='open',
                        statusTimestamp=self.fx.oracle.t)])))
                self.assertTrue(accepted)
            return previous(request)
        self.fx.port.context_loader=final_context
        sent=len(self.fx.http)
        self.fx.service.tick()
        self.assertTrue(injected)
        self.assertEqual(len(self.fx.http),sent,'event newer than snapshot must force recollection before exit')
        exit_requests=[r for r in self.fx.store.load()['requests'].values()
            if r['proposal']['operation']=='CREATE_EXIT']
        self.assertEqual(exit_requests[0]['phase'],'ABORTED_UNSENT')

    def test_manual_edit_of_bot_stop_is_not_silently_restored(self):
        for _ in range(4):
            self.fx.service.tick()
        state=self.fx.store.load();trade=state['trades'][self.msg['occurrence_id']]
        oid=next(oid for oid,leg in trade['order_legs'].items() if leg=='STOP')
        self.fx.oracle.orders[oid]['view']['wire_order']['p']='2.7'
        self.fx.oracle.orders[oid]['view']['wire_order']['t']['trigger']['triggerPx']='2.7'
        sent=len(self.fx.http)
        for _ in range(2):self.fx.service.tick()
        after=self.fx.store.load()
        self.assertTrue(ext.managed(after,trade['account'],trade['symbol']))
        self.assertEqual(len(self.fx.http),sent)
        self.assertEqual(self.fx.oracle.orders[oid]['view']['wire_order']['p'],'2.7')

    def test_manual_cancel_of_bot_stop_is_not_silently_recreated(self):
        for _ in range(4):
            self.fx.service.tick()
        state=self.fx.store.load();trade=state['trades'][self.msg['occurrence_id']]
        oid=next(oid for oid,leg in trade['order_legs'].items() if leg=='STOP')
        self.fx.oracle.orders[oid]['view']['status']='CANCELED'
        self.fx.oracle.orders[oid]['view']['at_ms']=self.fx.oracle.t
        sent=len(self.fx.http)
        for _ in range(2):self.fx.service.tick()
        after=self.fx.store.load()
        self.assertTrue(ext.managed(after,trade['account'],trade['symbol']))
        self.assertEqual(len(self.fx.http),sent)

    def test_normal_take_trigger_activation_is_not_manual_intervention(self):
        for _ in range(4):self.fx.service.tick()
        state=self.fx.store.load();trade=state['trades'][self.msg['occurrence_id']]
        oid=next(oid for oid,leg in trade['order_legs'].items() if leg=='TAKE_PROFIT')
        self.fx.oracle.t+=1;activation=self.fx.oracle.t
        status=self.fx.raw.status
        def activated_status(value):
            raw=status(value)
            if str(value)==oid:
                raw['order'].update(status='triggered',statusTimestamp=activation)
                raw['order']['order']['sz']=raw['order']['order']['origSz']
            return raw
        self.fx.raw.status=activated_status
        read=self.fx.raw.read
        def activated_read(kind,*args,**kw):
            rows=read(kind,*args,**kw)
            if kind=='frontendOpenOrders':
                for row in rows:
                    if str(row['oid'])==oid:
                        row.update(isTrigger=False,triggerPx='0',triggerCondition='Triggered',timestamp=activation)
            return rows
        self.fx.raw.read=activated_read
        self.fx.service.tick()
        after=self.fx.store.load()
        self.assertFalse(ext.managed(after,trade['account'],trade['symbol']),
            'venue trigger activation is an expected state change, not a manual edit')

    def _manual_full_close(self):
        for _ in range(4):self.fx.service.tick()
        state=self.fx.store.load();trade=state['trades'][self.msg['occurrence_id']]
        account=trade['account'];symbol=trade['symbol'];quantity=trade['quantity']
        self.fx.oracle.t+=1;closed_at=self.fx.oracle.t
        for oid,leg in trade['order_legs'].items():
            if leg in ('STOP','TAKE_PROFIT'):
                self.fx.oracle.orders[oid]['view'].update(status='CANCELED',at_ms=closed_at)
        closing=fill(tid=900001,oid=909,coin=symbol,quantity=quantity,at=closed_at)
        closing.update(side='B',px='2.2')
        read=self.fx.raw.read
        def manual_close(kind,account_arg=None,**kw):
            result=read(kind,account_arg,**kw)
            if (account_arg or kw.get('user'))!=account:return result
            if kind=='userFillsByTime' and kw['start']<=closed_at<=kw['end']:
                return result+[deepcopy(closing)]
            if kind=='clearinghouseState':
                match=next((r for r in result['assetPositions'] if r['position']['coin']==symbol),None)
                actual=Decimal(match['position']['szi'] if match else '0')+Decimal(quantity)
                result['assetPositions']=[r for r in result['assetPositions'] if r['position']['coin']!=symbol]
                if actual:result['assetPositions'].append(dict(position=dict(coin=symbol,szi=str(actual))))
            return result
        self.fx.raw.read=manual_close
        sent=len(self.fx.http)
        self.fx.service.tick();self.fx.oracle.t+=1000;self.fx.service.tick()
        after=self.fx.store.load()
        self.assertEqual(len(self.fx.http),sent)
        self.assertEqual(after['trades'][self.msg['occurrence_id']]['phase'],'MANUALLY_CLOSED')
        self.assertFalse(ext.managed(after,account,symbol))
        return after,trade,sent

    def test_manual_full_close_allows_later_independent_same_coin_trade(self):
        after,trade,sent=self._manual_full_close()
        self.fx.oracle.t=T+900000+10000
        later=r2732_message(entry=2.3,decision_ms=T+900000-60000)
        self.fx.seed(later)
        result=self.fx.service.tick()
        self.assertEqual(result.get('operation'),'ENTRY',result)
        self.assertEqual(len(self.fx.http),sent+1)

    def test_manual_full_close_does_not_replay_older_unsubmitted_same_coin_alert(self):
        for _ in range(4):self.fx.service.tick()
        # A second decision is still fresh but predates the manual release.
        # Delayed delivery must not turn it into a new independent signal.
        self.fx.oracle.t=T+900000+65000
        after,trade,sent=self._manual_full_close()
        older=r2732_message(entry=2.3,decision_ms=T+900000-60000)
        self.fx.seed(older)
        result=self.fx.service.tick()
        self.assertNotEqual(result.get('operation'),'ENTRY',result)
        self.assertEqual(len(self.fx.http),sent)
        state=self.fx.store.load()
        self.assertNotIn(older['occurrence_id'],state['trades'])
        self.assertEqual(state['entry_blocked'][older['occurrence_id']],
            'NEW_ALERT_REQUIRED_AFTER_MANUAL_RELEASE')

    def test_manual_close_real_archive_accepts_unchanged_overlapping_entry_fill(self):
        from .experimental_execution_state import ExecutionState, VERSION
        after,trade,sent=self._manual_full_close()
        account,symbol=trade['account'],trade['symbol']
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        store=ExecutionState(Path(tmp.name)/'review.isolated-experimental.sqlite3')
        store.initialize(ROUTES,not_before_ms=after['not_before_ms'])
        software=deepcopy(after);software.update(domain='software',version=VERSION)
        for request in software['requests'].values():request['domain']='software'
        def install(value):value.clear();value.update(software)
        store.mutate(install);store.initialize_history()
        archived=store.compact_history(now_ms=self.fx.oracle.t,force=True)
        self.assertEqual(archived['archived'],1)
        rows=self.fx.raw.read('userFillsByTime',account,start=T,end=self.fx.oracle.t)
        fills=ext.sync.merge_fills([],rows,account,symbol,T,self.fx.oracle.t)
        snapshot=dict(account=account,symbol=symbol,at_ms=self.fx.oracle.t,
            history_complete=True,orders_complete=True,position_quantity='0',
            fills=fills,open_orders=[],terminal_orders=[])
        cleaned=store.strip_archived_collector_snapshot(snapshot)
        self.assertFalse(any(f['oid'] in trade['orders'] for f in cleaned['fills']))


@unittest.skipUnless(pg_fixture.CI, 'Requires disposable loopback PostgreSQL; skipped is not verified')
class PostgresExternalActivityReviewTests(unittest.TestCase):
    """Reuse existing real-Postgres Testnet harness, with synthetic exchange only."""
    setUp=pg_fixture.ExecutionHistoryPostgresTests.setUp
    reconnect=pg_fixture.ExecutionHistoryPostgresTests.reconnect
    oracle_state=pg_fixture.ExecutionHistoryPostgresTests.oracle_state
    cycle=pg_fixture.ExecutionHistoryPostgresTests.cycle
    start=pg_fixture.ExecutionHistoryPostgresTests.start
    compact=pg_fixture.ExecutionHistoryPostgresTests.compact

    def _manual_exchange_observation(self, account, symbol, external_fills):
        read=self.raw.read
        def current(kind,account_arg=None,**kw):
            result=read(kind,account_arg,**kw)
            if (account_arg or kw.get('user'))!=account:return result
            if kind=='userFillsByTime':
                return result+[deepcopy(f) for f in external_fills if kw['start']<=f['time']<=kw['end']]
            if kind=='clearinghouseState':
                existing=next((r for r in result['assetPositions'] if r['position']['coin']==symbol),None)
                original=Decimal(existing['position']['szi'] if existing else '0')
                manual=sum((Decimal(f['sz'])*(1 if f['side']=='B' else -1) for f in external_fills),Decimal(0))
                result['assetPositions']=[r for r in result['assetPositions'] if r['position']['coin']!=symbol]
                if original+manual:
                    result['assetPositions'].append(dict(position=dict(coin=symbol,szi=str(original+manual))))
            return result
        self.raw.read=current

    def test_real_pg_manual_handoff_restart_flat_release_archive_and_new_same_coin_entry(self):
        first=pg_fixture.alert(cycle='pg-manual-first')
        cid=first['occurrence_id'];symbol=first['symbol']
        account=self.release['routes']['long_account']
        oid=self.start(first)
        quantity=self.oracle.orders[oid]['view']['wire_order']['s']
        self.oracle.fill(oid,quantity)
        self.assertEqual(self.cycle().get('operation'),'CREATE_EXIT')
        self.assertEqual(self.cycle().get('operation'),'CREATE_EXIT')
        self.cycle()
        self.oracle.t+=1000
        half=Decimal(quantity)/2
        manual_fills=[fill(tid=930001,oid=930001,coin=symbol,quantity=str(half),at=self.oracle.t)]
        manual_fills[0].update(side='A',px=first['entry'])
        self._manual_exchange_observation(account,symbol,manual_fills)
        sent=len(self.oracle.requests)
        self.cycle()
        handed=self.store.load()
        self.assertTrue(ext.managed(handed,account,symbol))
        self.assertEqual(len(self.oracle.requests),sent)
        self.assertEqual(Decimal(handed['trades'][cid]['manual_management']['actual_position_quantity']),half)
        self.assertEqual(Decimal(pg_fixture.core._remaining(handed['trades'][cid])),Decimal(quantity))
        # Reconstruct store/provider/worker using real PostgreSQL, not a copy of
        # Python state. No source or observed order is resubmitted on restart.
        self.reconnect();self._manual_exchange_observation(account,symbol,manual_fills)
        self.assertTrue(ext.managed(self.store.load(),account,symbol))
        self.oracle.t+=1000;self.cycle()
        self.assertEqual(len(self.oracle.requests),sent)
        self.oracle.t+=1000
        manual_fills.append(fill(tid=930002,oid=930002,coin=symbol,quantity=str(half),at=self.oracle.t))
        manual_fills[-1].update(side='A',px=first['entry'])
        for order_id,item in self.oracle.orders.items():
            if order_id!=oid:item['view'].update(status='CANCELED',at_ms=self.oracle.t)
        self.cycle()
        self.assertTrue(ext.managed(self.store.load(),account,symbol),'one flat sample cannot release ownership')
        self.oracle.t+=1000;self.cycle()
        closed=self.store.load()
        self.assertFalse(ext.managed(closed,account,symbol))
        self.assertEqual(closed['trades'][cid]['phase'],ext.MANUAL_CLOSED)
        self.assertEqual(len(self.oracle.requests),sent)
        self.assertIsNone(self.worker.report()['cards'][0]['summary']['net_pnl'])
        self.assertEqual(self.compact()['archived'],1)
        self.reconnect();self._manual_exchange_observation(account,symbol,manual_fills)
        self.assertEqual(self.store.archive_record(cid)['trade']['phase'],ext.MANUAL_CLOSED)
        released_at=closed['external_activity']['retired_markets'][ext.lane(account,symbol)]['confirmed_at_ms']
        # Rounding the current time down yields a closed-minute decision from
        # BEFORE release. Archive/restart must not make that stale alert eligible.
        stale_at=self.oracle.t//60000*60000
        self.assertLessEqual(stale_at,released_at)
        stale=pg_fixture.alert(cycle='pg-manual-stale-before-release',approved_ms=stale_at)
        self.worker.receive([stale])
        self.assertNotEqual(self.cycle(entries=True).get('operation'),'ENTRY')
        after_stale=self.store.load()
        self.assertEqual(len(self.oracle.requests),sent)
        self.assertEqual(len(self.replayed_entries),1)
        self.assertNotIn(stale['occurrence_id'],after_stale['trades'])
        self.assertEqual(after_stale['entry_blocked'][stale['occurrence_id']],
            'NEW_ALERT_REQUIRED_AFTER_MANUAL_RELEASE')
        # A genuinely new independent alert is approved at the next closed
        # minute, strictly after the durable second clean observation.
        next_approval=(max(self.oracle.t,released_at)//60000+1)*60000
        self.assertGreater(next_approval,released_at)
        self.oracle.t=next_approval+1000
        second=pg_fixture.alert(cycle='pg-manual-successor',approved_ms=next_approval)
        self.start(second)
        self.cycle()
        self.assertEqual(len(self.replayed_entries),2)
        self.assertIn(second['occurrence_id'],self.store.load()['trades'])
        self.assertFalse(self.store.load().get('blocked_lanes'))


if __name__=='__main__':
    unittest.main()
