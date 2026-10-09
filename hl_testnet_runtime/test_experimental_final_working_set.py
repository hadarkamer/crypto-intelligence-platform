"""Proven-final experimental ownership stays audited outside recurring history."""
from collections import Counter
from copy import deepcopy
import unittest

import approved_alert_contract as contract
from experimental_execution_fixtures import r2732_message
from types import SimpleNamespace
from . import card_lifecycle as life, experimental_execution_runtime as runtime
from .experimental_live_provider import _retired_experimental
from .experimental_plan_store import reduce_source
from . import test_experimental_live_provider as provider_tests
from . import test_experimental_dispatch_integration as integration
from .test_card_sync import e as sync


class FinalWorkingSetTests(unittest.TestCase):
    setUp=provider_tests.ProviderTests.setUp

    def final_trade(self):
        fixture=integration.DispatchIntegrationTests();fixture.setUp();self.addCleanup(fixture.doCleanups)
        fixture.test_worker_entry_partial_fill_stops_take_cleanup_uses_same_wire_boundary()
        value=fixture.store.load();value['domain']='testnet'
        for row in value['requests'].values():row['domain']='testnet'
        for row in value['sources'].values():row['domain']='testnet'
        self.state.update(deepcopy(value))
        cid=fixture.msg['occurrence_id'];trade=self.state['trades'][cid]
        lane=runtime._lane(trade['account'],trade['symbol'])
        binding,raw=integration.legacy_view(trade,self.state['snapshots'][lane])
        self.state['collector_checkpoints']={lane:raw}
        self.exchange.t=fixture.venue.t+3*sync.DAY_MS
        return cid,trade,lane

    def candidate_same_lane(self, trade):
        msg=r2732_message(entry=2.3,decision_ms=(self.exchange.t//60000-1)*60000)
        row,_=reduce_source(None,msg,now=contract.iso_ms(self.exchange.t),
            not_before=contract.iso_ms(self.state['not_before_ms']),domain='testnet')
        self.state['sources'][msg['occurrence_id']]=row
        self.exchange.mark[runtime._lane(trade['account'],trade['symbol'])]=msg['entry']
        cid=msg['occurrence_id']
        self.provider.prices=SimpleNamespace(
            source_range=lambda *args:self.exchange.collect(self.state)['ranges'][cid]['source'],
            mark_window=lambda *args:self.exchange.collect(self.state)['ranges'][cid]['testnet'],
            closed_bars=lambda *args:[])
        self.provider.safety=object()
        return cid

    def test_final_older_than_history_limit_does_not_reopen_history_or_refresh_old_facts(self):
        cid,trade,lane=self.final_trade();before=deepcopy(self.state)
        self.assertIn(cid,_retired_experimental(self.state,self.exchange.t))
        result=self.provider.collect(self.state)
        self.assertEqual(result['blocked_lanes'],{});self.assertEqual(result['account_entry_blocked'],{})
        self.assertEqual(Counter(c[0] for c in self.raw.calls),Counter(frontendOpenOrders=2,clearinghouseState=2))
        self.assertEqual(self.state,before)

    def test_new_candidate_same_lane_keeps_exact_final_orders_and_uses_no_old_history(self):
        cid,trade,lane=self.final_trade();new=self.candidate_same_lane(trade)
        old_orders=deepcopy(trade['orders']);old_raw=deepcopy(self.state['collector_checkpoints'][lane])
        result=self.provider.collect(self.state,entries_enabled=True)
        self.assertEqual(result['entry_blocked'],{});self.assertIn(new,result['entry_accounts'])
        snapshot=next(s for s in result['snapshots'] if s['account']==trade['account'])
        self.assertEqual({o['oid']:o for o in snapshot['orders']},old_orders)
        self.assertEqual(self.state['collector_checkpoints'][lane],old_raw)
        self.assertNotIn('userFillsByTime',[c[0] for c in self.raw.calls])
        self.assertNotIn('lookup',[c[0] for c in self.raw.calls])
        replay=deepcopy(self.state);planner=object.__new__(runtime.IsolatedExecutionRuntime)
        planner._snapshot(replay,snapshot,self.exchange.t)
        self.assertEqual(replay['trades'][cid]['orders'],old_orders)

    def test_reappearing_final_oid_and_unknown_position_remain_account_fences(self):
        cid,trade,lane=self.final_trade();new=self.candidate_same_lane(trade)
        self.raw.extra_orders=[dict(coin=trade['symbol'],oid=int(next(iter(trade['orders']))))]
        result=self.provider.collect(self.state,entries_enabled=True)
        self.assertIn(trade['account'],result['account_entry_blocked'])
        self.assertNotIn(new,result['entry_accounts'])
        self.raw.extra_orders=[];self.raw.extra_positions=[dict(position=dict(coin=trade['symbol'],szi='-1'))]
        result=self.provider.collect(self.state,entries_enabled=True)
        self.assertIn(trade['account'],result['account_entry_blocked']);self.assertNotIn(new,result['entry_accounts'])

    def test_prior_terminal_contradiction_cannot_be_hidden_by_later_flat_inventory(self):
        cid,trade,lane=self.final_trade()
        self.state['blocked_lanes']={lane:'TERMINAL_FACT_CHANGED'}
        self.assertNotIn(cid,_retired_experimental(self.state,self.exchange.t))
        self.state['blocked_lanes'][lane]='HISTORY_GAP_REQUIRES_REVIEW'
        self.assertIn(cid,_retired_experimental(self.state,self.exchange.t))

    def test_changed_final_owner_cannot_be_retired_by_phase_label(self):
        cid,trade,lane=self.final_trade()
        next(iter(trade['orders'].values()))['wire_order']['s']='999'
        self.assertNotIn(cid,_retired_experimental(self.state,self.exchange.t))
        result=self.provider.collect(self.state)
        self.assertIn(trade['account'],result['account_entry_blocked'])


if __name__=='__main__':unittest.main()
