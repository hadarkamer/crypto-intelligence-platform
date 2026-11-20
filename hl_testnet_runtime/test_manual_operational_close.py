"""Manual historical uncertainty is not active risk after two fresh flat proofs.

Real PostgreSQL, authenticated intake, collectors, runtime, final dispatch and
transport claim. Only raw exchange responses, signing and HTTP are controlled;
no connection to an exchange or production database is possible. The four
frozen fills are a narrow evidence projection, not fabricated full history.
"""
from copy import deepcopy
from decimal import Decimal
import json
import os
from pathlib import Path
from types import MethodType
import unittest
from unittest.mock import patch

import approved_alert_contract as contract
from experimental_execution_fixtures import hype_row71205_message, r2732_message
from . import experimental_external_activity as ext
from . import experimental_live_state as live
from . import experimental_live_release as releases
from . import experimental_plan_store as plans
from . import formula_accounting as accounting
from .experimental_live_provider import LiveEvidenceProvider, _empty
from .filled_dispatch_store import DispatchStore, SCHEMA as DISPATCH_SCHEMA
from .postgres_journal import PostgresJournal
from . import test_experimental_live_service as connection_fixture
from .test_experimental_live_service import SyntheticPrices, SyntheticSafety
from .test_experimental_live_provider import LegacyFixture
from .test_experimental_live_state import LIVE_ROUTES
from .test_experimental_execution_runtime import META
from .test_hype_watch_execution import hype_alert
from .test_hype_formula_capital import capital_context

CI = os.environ.get('HL_JOURNAL_CI_URL')
FROZEN = json.loads((Path(__file__).parent/'fixtures/manual_hype_operational_close_20261120.json').read_text())
RID = '246a9fcfc5c578b327dc8dadf8f788026d0f03b425a074ddfdac3b9453985ffb'
OID = '591986903854'
NOW = FROZEN['proof_fixed_end_ms']
ATTEMPT = FROZEN['attempt_at_ms']


def historical_state(state, worker):
    """Legal durable product record; old formula is never re-enabled/admitted."""
    msg = hype_row71205_message(entry=87.94, decision_ms=(ATTEMPT-60000)//900000*900000, as_of_ms=ATTEMPT)
    cid = msg['occurrence_id']; account = state['routes']['short_account']
    state['sources'][cid], _ = plans.reduce_source(None, msg, now=contract.iso_ms(ATTEMPT),
        not_before=contract.iso_ms(state['not_before_ms']), domain='mainnet')
    trade = dict(cid=cid, source=msg, account=account, role='short_account', symbol='HYPE',
        side='SHORT', quantity='5.42', prices=dict(entry=msg['entry'], stop=msg['stop'], take_profit=msg['take_profit']),
        asset=dict(index=1, decimals=2), phase='OUTCOME_UNKNOWN', orders={}, order_legs={},
        entry_fills={}, exit_fills={}, entry_request=RID, condition=None, desired_stop=msg['stop'],
        manual_management=dict(status=ext.HUMAN, started_at_ms=ATTEMPT+10000,
            reason='EXPLICIT_TRADER_CHOICE', accounting_allocation='EXTERNAL_ACTIVITY_UNALLOCATED'))
    state['trades'][cid] = trade
    proposal = worker._order(state, trade, 'ENTRY', '5.42', '87.94', META, ATTEMPT)
    # Preserve the exact observed historical action (asset 159 and GTC), even
    # though today's controlled metadata/old formula would produce other terms.
    proposal['action'] = deepcopy(FROZEN['original_order_action'])
    state['requests'][RID] = dict(request_id=RID, domain='mainnet', bucket=ext.lane(account,'HYPE'),
        proposal=proposal, nonce=ATTEMPT, attempt_at_ms=ATTEMPT, prepared_at_ms=ATTEMPT,
        attempts=1, phase='OUTCOME_UNKNOWN', observed_oid=None,
        reply=dict(state='ACCEPTED_UNVERIFIED', oid=OID), transport_claim='b'*32)
    key=ext.lane(account,'HYPE')
    known=[dict(ext.sync.normalize_fill(r, account, 'HYPE', 1, NOW), origin='UNATTRIBUTED')
           for r in FROZEN['hype_fills_after_request']]
    state['collector_checkpoints']={key:_empty(account,'HYPE',FROZEN['pre_send_checkpoint_ms'])}
    state['external_activity']=dict(version=ext.VERSION, events=[], evicted_events=0,
        accounts={account:dict(account=account, fills=known, history_complete=False,
            history_cursor_ms=FROZEN['external_history_cursor_ms'],
            history_started_at_ms=FROZEN['external_history_started_at_ms'])},
        human_managed={key:dict(account=account,symbol='HYPE',card_ids=[cid],
            status=ext.HUMAN,reason='EXPLICIT_TRADER_CHOICE',automatic_resume=False)})
    return cid


def current_observation(state, at, **changes):
    """Current raw inventory truth, explicitly independent of missing history."""
    account=state['routes']['short_account']
    row=dict(account=account, observed_at_ms=at, inventory_at_ms=at, venue_at_ms=at,
        orders_complete=True, positions_complete=True, current_action_symbols=[],
        orders=[], positions={}, bot_symbols=['HYPE'], fills=[], history_complete=False,
        history_cursor_ms=FROZEN['external_history_cursor_ms'],
        history_started_at_ms=FROZEN['external_history_started_at_ms'],
        history_error='EXTERNAL_HISTORY_GAP_REQUIRES_REVIEW')
    row.update(changes)
    return dict(version=ext.VERSION,accounts={account:row},human_managed={},owned_request_observations={})


@unittest.skipUnless(CI, 'Requires disposable loopback PostgreSQL; skipped is not verified')
class ManualOperationalPostgresTests(unittest.TestCase):
    def setUp(self):
        self.fx=connection_fixture.FullConnectionTests(methodName='runTest')
        def build(fx, not_before):
            connection_fixture.FullConnectionTests.build(fx, not_before)
            journal=PostgresJournal.for_ci(CI)
            journal.bootstrap(); DispatchStore(journal).initialize()
            with journal._transaction() as conn:
                conn.execute(f'DROP SCHEMA IF EXISTS {live.PG_SCHEMA} CASCADE')
                conn.execute(f'DELETE FROM {DISPATCH_SCHEMA}.nonces WHERE agent=ANY(%s)',
                    ([r['agent'] for r in LIVE_ROUTES.values()],))
            fx.store=live.TestnetExecutionState.for_ci(journal)
            fx.store.initialize(LIVE_ROUTES,not_before_ms=ATTEMPT-86400000)
            fx.store.initialize_history(); fx.oracle.t=NOW
            fx.release.update(not_before_ms=ATTEMPT-86400000,
                entry_policy=releases.CONTINUOUS_ENTRY,entry_expires_at_ms=None)
            fx.raw.state_loader=fx.store.load
            read=fx.raw.read; lookup=fx.raw.lookup
            def raw_read(kind,*args,**kwargs):
                if kind=='orderStatus' and str(kwargs.get('oid'))==OID:
                    fx.raw.calls.append((kind,args,kwargs)); return deepcopy(FROZEN['order_status_both_passes'])
                if kind=='userFillsByTime' and kwargs['start']<NOW-86400000:
                    fx.raw.calls.append((kind,args,kwargs)); raise ValueError('HISTORICAL_INFORMATION_UNAVAILABLE')
                value=read(kind,*args,**kwargs)
                if kind=='clearinghouseState': value['time']=fx.oracle.now()
                return value
            def raw_lookup(account,cloid):
                if cloid==FROZEN['original_order_action']['orders'][0]['c']:
                    fx.raw.calls.append(('lookup',account,cloid)); return deepcopy(FROZEN['order_status_both_passes'])
                return lookup(account,cloid)
            fx.raw.read=raw_read; fx.raw.lookup=raw_lookup
            fx.provider=LiveEvidenceProvider(fx.env,legacy_store=LegacyFixture(),experimental_store=fx.store,
                price_evidence=SyntheticPrices(fx.oracle,fx.store),budget=fx.budget,clock=fx.oracle.now,
                info_reader=fx.raw,public_reader=fx.raw,lookup_reader=fx.raw.lookup,
                safety_provider=SyntheticSafety(fx.release))
            fx.reconnect()
        self.fx.build=MethodType(build,self.fx)
        self.addCleanup(self.fx.doCleanups); self.fx.setUp()
        seed=self.fx.store.load(); self.cid=historical_state(seed,self.fx.worker)
        live.checked(seed,ext.life.digest(seed)); live.encode(seed)
        self.fx.store.mutate(lambda state:state.update(seed))
        self.account=self.fx.store.load()['routes']['short_account']
        self.before=deepcopy(self.fx.store.load()['requests'][RID])

    def apply_current(self,at,**changes):
        return self.fx.store.mutate(lambda state:ext.apply(state,current_observation(state,at,**changes),now_ms=at))

    def close(self):
        self.apply_current(NOW); self.apply_current(NOW+1000)
        state=self.fx.store.load()
        self.assertTrue(ext.request_retired(state,state['requests'][RID]))
        self.fx.oracle.t=NOW+1001
        return state

    def restart(self):
        self.fx.store=live.TestnetExecutionState.for_ci(PostgresJournal.for_ci(CI))
        self.fx.provider.experimental_store=self.fx.store
        self.fx.raw.state_loader=self.fx.store.load
        self.fx.reconnect()

    def test_two_current_observations_preserve_unknown_fills_and_restart_idempotence(self):
        self.apply_current(NOW)
        self.assertTrue(ext.managed(self.fx.store.load(),self.account,'HYPE'))
        self.apply_current(NOW)
        self.assertTrue(ext.managed(self.fx.store.load(),self.account,'HYPE'))
        self.restart(); self.apply_current(NOW+1000)
        state=self.fx.store.load(); trade=state['trades'][self.cid]; request=state['requests'][RID]
        self.assertTrue(ext.closure_verified(trade)); self.assertFalse(ext.managed(state,self.account,'HYPE'))
        self.assertTrue(ext.request_retired(state,request)); self.assertFalse(ext.request_pending(state,request))
        self.assertEqual({k:v for k,v in request.items() if k!='operational_retirement'},self.before)
        self.assertFalse(trade['manual_closure_snapshot']['history_complete'])
        fills=trade['manual_closure_snapshot']['fills']
        self.assertEqual(len(fills),4)
        self.assertEqual(sum(Decimal(f['fee']) for f in fills),Decimal('0.298569'))
        self.assertEqual(sum(Decimal(f['quantity']) for f in fills if f['oid']==OID),Decimal('4.09'))
        self.assertFalse(trade['entry_fills']); self.assertNotIn('settlement',trade)
        self.assertNotIn('formula_accounting',state)
        from .experimental_execution_reporting import _project_rows
        row=next(r for r in _project_rows(state) if r['occurrence_id']==self.cid)
        self.assertEqual((row['unresolved_attempts'],row['historical_unresolved_attempts']),(0,1))
        self.assertIsNone(row['net_pnl']);self.assertIsNone(row['gross_pnl_before_costs'])
        self.assertEqual(self.fx.worker.report()['unresolved_attempts'],0)
        proof=deepcopy(trade['manual_closure']); self.restart()
        for _ in range(3): self.apply_current(NOW+2000)
        after=self.fx.store.load()
        self.assertEqual(after['trades'][self.cid]['manual_closure'],proof)
        self.assertEqual(after['trades'][self.cid]['manual_closure_snapshot']['fills'],fills)
        self.assertFalse(self.fx.http)

    def test_bad_current_state_or_live_attempt_never_releases(self):
        baseline=self.fx.store.load()
        cases=dict(stale=dict(inventory_at_ms=NOW-15001),venue_stale=dict(venue_at_ms=NOW-15001),
            future=dict(inventory_at_ms=NOW+2000),venue_unknown=dict(venue_at_ms=None),
            inventory_unknown=dict(orders_complete=False),positions_unknown=dict(positions_complete=False),
            position=dict(positions={'HYPE':'-0.01'}),pending_legacy=dict(current_action_symbols=['HYPE']))
        for reduce_only in (False,True):
            raw=dict(oid=999,coin='HYPE',side='B',sz='0.1',origSz='0.1',limitPx='100',reduceOnly=reduce_only)
            cases['open_order_'+str(reduce_only)]=dict(orders=[dict(ext.normalize_order(self.account,raw,NOW),origin='UNATTRIBUTED')])
        for name,changes in cases.items():
            with self.subTest(name=name):
                state=deepcopy(baseline)
                for at in (NOW,NOW+1000):
                    ext.apply(state,current_observation(state,at,**changes),now_ms=at)
                self.assertTrue(ext.managed(state,self.account,'HYPE'))
                self.assertTrue(ext.request_pending(state,state['requests'][RID]))

    def test_missing_intermediate_observation_or_stale_commit_resets_confirmation(self):
        state=self.fx.store.load()
        ext.apply(state,current_observation(state,NOW),now_ms=NOW)
        ext.apply(state,dict(version=ext.VERSION,accounts={},human_managed={},errors={self.account:'CURRENT_UNAVAILABLE'}),now_ms=NOW+1000)
        ext.apply(state,current_observation(state,NOW+2000),now_ms=NOW+2000)
        self.assertTrue(ext.managed(state,self.account,'HYPE'))
        ext.apply(state,current_observation(state,NOW+3000),now_ms=NOW+3000+15001)
        self.assertTrue(ext.managed(state,self.account,'HYPE'))
        ext.apply(state,current_observation(state,NOW+20000),now_ms=NOW+20000)
        self.assertTrue(ext.managed(state,self.account,'HYPE'))
        ext.apply(state,current_observation(state,NOW+21000),now_ms=NOW+21000)
        self.assertTrue(ext.request_retired(state,state['requests'][RID]))

    def test_lost_operational_close_commit_ack_and_replay_do_not_resend_or_rebook(self):
        self.apply_current(NOW); original=self.fx.store.mutate
        def lost(transition):
            original(transition);raise live.StateError('COMMIT_ACK_LOST')
        with patch.object(self.fx.store,'mutate',side_effect=lost),self.assertRaisesRegex(ValueError,'COMMIT_ACK_LOST'):
            self.apply_current(NOW+1000)
        self.restart();state=self.fx.store.load();before=deepcopy(state['trades'][self.cid])
        self.assertTrue(ext.request_retired(state,state['requests'][RID]))
        self.apply_current(NOW+2000);self.fx.oracle.t=NOW+2000
        self.fx.service.tick();self.fx.service.tick()
        archived=self.fx.store.history_page()['records'][0]
        self.assertEqual(archived['trade']['manual_closure'],before['manual_closure'])
        self.assertEqual(archived['trade']['manual_closure_snapshot'],before['manual_closure_snapshot'])
        self.assertNotIn('settlement',archived['trade']);self.assertFalse(self.fx.http)
        row=self.fx.worker.history_report()['trades'][0]
        self.assertEqual((row['unresolved_attempts'],row['historical_unresolved_attempts']),(0,1))
        self.assertIsNone(row['net_pnl'])

    def test_active_manual_hype_does_not_starve_new_xrp_or_its_protections(self):
        self.fx.raw.extra_positions=[dict(position=dict(coin='HYPE',szi='-4.09'))]
        self.fx.oracle.t=(NOW//900000+1)*900000+65000
        msg=r2732_message(entry=2.3,decision_ms=self.fx.oracle.t-65000,as_of_ms=self.fx.oracle.t)
        self.fx.seed(msg);first=self.fx.service.tick()
        self.assertEqual(first.get('operation'),'ENTRY',first)
        for _ in range(4):self.fx.oracle.t+=1000;self.fx.service.tick()
        state=self.fx.store.load()
        self.assertTrue(ext.managed(state,self.account,'HYPE'))
        self.assertTrue(ext.request_pending(state,state['requests'][RID]))
        self.assertEqual([r['proposal']['operation'] for r in self.fx.oracle.requests],['ENTRY','CREATE_EXIT','CREATE_EXIT'])
        self.assertEqual({r['proposal']['symbol'] for r in self.fx.oracle.requests},{'XRP'})
        self.assertNotIn('settlement',state['trades'][self.cid])

    def test_inflight_other_pending_and_repeated_venue_clock_block_closure(self):
        baseline=self.fx.store.load()
        for mutate in ('recent_attempt','other_card','same_venue_clock','missing_nonce'):
            with self.subTest(name=mutate):
                state=deepcopy(baseline); req=state['requests'][RID]
                if mutate=='recent_attempt':
                    req.update(nonce=NOW,attempt_at_ms=NOW,prepared_at_ms=NOW)
                elif mutate=='other_card':
                    other=deepcopy(req);other.update(request_id='f'*64);other['proposal']['card_id']='f'*64
                    state['requests']['f'*64]=other
                elif mutate=='missing_nonce':req.pop('nonce')
                for at in (NOW,NOW+1000):
                    changes=dict(venue_at_ms=NOW) if mutate=='same_venue_clock' else {}
                    ext.apply(state,current_observation(state,at,**changes),now_ms=at)
                self.assertTrue(ext.managed(state,self.account,'HYPE'))
                self.assertTrue(ext.request_pending(state,state['requests'][RID]))

    def test_old_notional_freed_only_by_verified_certificate_and_capital_once(self):
        state=self.fx.store.load(); ctx=capital_context(state,amount='300',at=NOW)
        with self.assertRaisesRegex(ValueError,'CAPITAL'):
            accounting.risk_for(state,dict(rule_id=accounting.HYPE_FORMULA),context=ctx,now_ms=NOW)
        self.assertEqual(sum(Decimal(v) for v in accounting._capital_obligations(state,ctx['capital_observations']).values()),Decimal('476.6348'))
        state=self.close(); before=deepcopy(state['trades'][self.cid])
        active=deepcopy(state['requests'][RID]);active.pop('operational_retirement')
        active.update(request_id='d'*64)
        active['proposal'].update(card_id='d'*64,symbol='XRP')
        active['proposal']['action']['orders'][0].update(a=0,p='2',s='10',c='0x'+'d'*32)
        state['requests']['d'*64]=active
        self.assertEqual(sum(Decimal(v) for v in accounting._capital_obligations(state,ctx['capital_observations']).values()),Decimal('20'))
        value=accounting.risk_for(state,dict(rule_id=accounting.HYPE_FORMULA),context=ctx,now_ms=NOW+1001)
        self.assertEqual((Decimal(value['balance_usd']),Decimal(value['planned_usd'])),(Decimal('40'),Decimal('2')))
        receipt=deepcopy(state['formula_accounting'])
        for _ in range(3): accounting.risk_for(state,dict(rule_id=accounting.HYPE_FORMULA),context={},now_ms=NOW+2000)
        self.assertEqual(state['formula_accounting'],receipt);self.assertEqual(state['trades'][self.cid],before)
        self.assertEqual(sum(Decimal(v) for v in accounting._capital_obligations(state,ctx['capital_observations']).values()),Decimal('20'))
        tampered=deepcopy(state);tampered['requests'][RID]['nonce']+=1
        self.assertFalse(ext.request_retired(tampered,tampered['requests'][RID]))
        self.assertEqual(sum(Decimal(v) for v in accounting._capital_obligations(tampered,ctx['capital_observations']).values()),Decimal('496.6348'))

    def test_real_provider_current_cycles_close_then_new_hype_full_server_dispatch(self):
        self.fx.service.tick()
        first=self.fx.store.load()
        self.assertTrue(ext.managed(first,self.account,'HYPE'))
        self.fx.oracle.t+=1000;self.fx.service.tick()
        state=self.fx.store.load()
        self.assertTrue(ext.request_retired(state,state['requests'][RID]))
        self.assertFalse(self.fx.http)
        self.restart(); self.fx.oracle.t=(self.fx.oracle.t//60000+1)*60000+5000
        msg=hype_alert(reference=self.fx.oracle.t-5000,cycle='after-manual-operational-close')
        self.fx.seed(msg); result=self.fx.service.tick()
        self.assertEqual(result.get('operation'),'ENTRY',result)
        for _ in range(4):self.fx.oracle.t+=1000;self.fx.service.tick()
        state=self.fx.store.load(); new=state['trades'][msg['occurrence_id']]
        self.assertEqual(new['source']['rule_id'],accounting.HYPE_FORMULA)
        self.assertEqual([r['proposal']['operation'] for r in self.fx.oracle.requests],['ENTRY','CREATE_EXIT','CREATE_EXIT'])
        self.assertEqual(Decimal(new['entry_risk']['planned_usd']),Decimal('2'))
        self.assertEqual(state['requests'][RID]['phase'],'OUTCOME_UNKNOWN')
        self.assertEqual(state['requests'][RID]['attempts'],1)
        self.assertEqual({r['nonce'] for r in self.fx.http}.intersection({ATTEMPT}),set())
        before=len(self.fx.http);self.restart();self.fx.ingest(msg);self.fx.service.tick()
        self.assertEqual(len(self.fx.http),before)
        self.assertTrue(ext.request_retired(self.fx.store.load(),self.fx.store.load()['requests'][RID]))
        with self.assertRaises(ValueError):self.fx.store.claim_transport(state['requests'][RID],'c'*32)

