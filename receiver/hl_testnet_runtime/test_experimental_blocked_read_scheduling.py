"""State-scoped read scheduling with real collectors and synthetic boundaries.

No venue, credentials or external database. Read counters distinguish optional
entry work from live maintenance and independently consumed formula candles.
"""
from collections import Counter
from contextlib import contextmanager
from copy import deepcopy
import unittest
from unittest.mock import patch

from approved_alert_fixtures import formula_alert, maxpain_alert
from experimental_execution_fixtures import r2732_message, hype_row71205_message
from . import experimental_execution_runtime as runtime
from . import sol_g65_conditional_stop
from . import test_experimental_live_service as support


class BlockedReadSchedulingTests(unittest.TestCase):
    def setUp(self):
        self.h = support.FullConnectionTests()
        self.h.setUp()
        self.addCleanup(self.h.doCleanups)

    def open_trade(self, family):
        h = self.h
        builder = {'r2732': r2732_message, 'hype_row71205': hype_row71205_message}[family]
        if family == 'hype_row71205':
            h.oracle.t = (h.oracle.t//1800000+1)*1800000+70000
        msg = builder(entry=2.3 if family == 'r2732' else 100,
                      decision_ms=h.oracle.t//60000*60000-60000)
        h.seed(msg)
        for _ in range(4):
            h.service.tick()
        trade = h.store.load()['trades'][msg['occurrence_id']]
        self.assertEqual(trade['phase'], 'OPEN')
        self.assertEqual({trade['order_legs'][oid] for oid, order in trade['orders'].items()
                          if order['status'] == 'OPEN'}, {'STOP', 'TAKE_PROFIT'})
        return msg

    def candidate(self, family):
        h = self.h
        if family == 'sol_g65':
            h.oracle.t = (h.oracle.t//1800000+1)*1800000+130000
            msg = formula_alert(family, approved_ms=h.oracle.t//1800000*1800000+120000)
        else:
            h.oracle.t = (h.oracle.t//900000+1)*900000+70000
            msg = r2732_message(entry=2.3, decision_ms=h.oracle.t//60000*60000-60000)
        h.seed(msg)
        return msg

    @contextmanager
    def observations(self, *, missing_symbol=None):
        h = self.h
        h.provider._market = None
        h.raw.calls.clear()
        bars = []
        read, closed = h.raw.read, h.provider.prices.closed_bars

        def public(kind, *args, **kwargs):
            value = read(kind, *args, **kwargs)
            if kind == 'userFillsByTime' and missing_symbol:
                return [row for row in value if row['coin'] != missing_symbol]
            return value

        def candles(msg, *args, **kwargs):
            bars.append(msg['symbol'])
            return closed(msg, *args, **kwargs)

        with patch.object(h.raw, 'read', side_effect=public), \
                patch.object(h.provider.prices, 'closed_bars', side_effect=candles):
            yield bars

    def counts(self):
        return Counter(call[0] for call in self.h.raw.calls)

    def test_history_block_skips_source_quote_and_owned_xrp_bars_but_keeps_sol_checkpoint(self):
        h = self.h
        owned = self.open_trade('r2732')
        candidate = self.candidate('sol_g65')
        cid = candidate['occurrence_id']
        before = h.store.load()
        condition = before['formula_states'][cid]
        price = candidate['entry']
        rows = [dict(open_at_ms=condition['cursor_ms']+60000,
                     open=price, high=price, low=price, close=price)]
        h.oracle.bars[cid] = rows
        with self.observations(missing_symbol='XRP') as bars:
            context = h.provider.collect(before, entries_enabled=True)
        owned_lane = runtime._lane(before['trades'][owned['occurrence_id']]['account'], 'XRP')
        self.assertEqual(context['blocked_lanes'][owned_lane], 'PREVIOUS_FILL_MISSING_IN_OVERLAP')
        self.assertEqual(context['entry_blocked'][cid], 'PREVIOUS_FILL_MISSING_IN_OVERLAP')
        self.assertEqual(self.counts()['metaAndAssetCtxs'], 0)
        self.assertEqual(bars, ['SOL'])
        self.assertEqual(context['bars'][cid], rows)
        projected = deepcopy(before)
        self.assertIsNone(h.worker._cycle_proposal(projected, context, h.oracle.now(), entries_enabled=True))
        expected = sol_g65_conditional_stop.advance(condition, rows, now_ms=h.oracle.now(),
            price_source=candidate['policy']['source_price'])
        self.assertEqual(projected['formula_states'][cid], expected)
        self.assertGreater(expected['cursor_ms'], condition['cursor_ms'])
        self.assertEqual(h.store.load(), before)  # Read collection cannot commit a checkpoint.

    def test_account_block_skips_source_only_xrp_candles_and_quote(self):
        h = self.h
        self.open_trade('hype_row71205')
        candidate = self.candidate('r2732')
        before = h.store.load()
        with self.observations(missing_symbol='HYPE') as bars:
            context = h.provider.collect(before, entries_enabled=True)
        account = before['routes']['short_account']
        self.assertIn(account, context['account_entry_blocked'])
        self.assertNotIn(runtime._lane(account, 'XRP'), context['blocked_lanes'])
        self.assertEqual(context['entry_blocked'][candidate['occurrence_id']], 'PREVIOUS_FILL_MISSING_IN_OVERLAP')
        self.assertEqual(self.counts()['metaAndAssetCtxs'], 0)
        self.assertEqual(bars, [])
        self.assertEqual(context['marks'], {})
        self.assertEqual(self.counts()['userFillsByTime'], 1)
        self.assertEqual(h.store.load(), before)

    def test_lane_only_block_skips_candidate_bars_without_blocking_account(self):
        h = self.h
        self.open_trade('hype_row71205')
        candidate = self.candidate('r2732')
        h.raw.extra_positions = [dict(position=dict(coin='XRP', szi='1'))]
        before = h.store.load()
        with self.observations() as bars:
            context = h.provider.collect(before, entries_enabled=True)
        account = before['routes']['short_account']
        lane = runtime._lane(account, 'XRP')
        self.assertNotIn(account, context['account_entry_blocked'])
        self.assertIn(lane, context['blocked_lanes'])
        self.assertEqual(context['entry_blocked'][candidate['occurrence_id']], context['blocked_lanes'][lane])
        self.assertEqual(bars, [])
        self.assertEqual(self.counts()['metaAndAssetCtxs'], 0)

    def test_pending_entry_retains_cancel_mark_during_account_entry_block(self):
        h = self.h
        msg = maxpain_alert(approved_ms=support.T)
        h.seed(msg)
        h.service.tick()
        h.service.tick()
        h.oracle.t += 5000
        before = h.store.load()
        trade = before['trades'][msg['occurrence_id']]
        self.assertFalse(trade['entry_fills'])
        lane = runtime._lane(trade['account'], trade['symbol'])
        h.oracle.mark[lane] = msg['original_target']
        with self.observations(), patch(
                'hl_testnet_runtime.experimental_live_provider.external.review_inventory',
                side_effect=ValueError('SYNTHETIC_ACCOUNT_ENTRY_BLOCK')):
            context = h.provider.collect(before, entries_enabled=False)
        self.assertIn(trade['account'], context['account_entry_blocked'])
        self.assertEqual(self.counts()['metaAndAssetCtxs'], 1)
        self.assertEqual(context['marks'][lane]['mark_price'], msg['original_target'])
        proposal = h.worker._cycle_proposal(deepcopy(before), context, h.oracle.now(), entries_enabled=False)
        self.assertEqual(proposal['operation'], 'CANCEL')
        self.assertEqual(proposal['card_id'], msg['occurrence_id'])

    def test_flat_trade_retains_owned_cancel_metadata_while_candidate_is_blocked(self):
        h = self.h
        msg = self.open_trade('hype_row71205')
        trade = h.store.load()['trades'][msg['occurrence_id']]
        take = next(oid for oid, leg in trade['order_legs'].items() if leg == 'TAKE_PROFIT')
        h.oracle.fill(take, trade['quantity'])
        candidate = self.candidate('r2732')
        before = h.store.load()
        with self.observations(), patch(
                'hl_testnet_runtime.experimental_live_provider.external.review_inventory',
                side_effect=ValueError('SYNTHETIC_ACCOUNT_ENTRY_BLOCK')):
            context = h.provider.collect(before, entries_enabled=True)
        self.assertEqual(self.counts()['metaAndAssetCtxs'], 0)
        self.assertEqual(self.counts()['meta'], 1)
        self.assertTrue(context['metadata'])
        self.assertEqual(context['entry_blocked'][candidate['occurrence_id']],
                         'ENTRY_DEFERRED_FOR_OWNED_PROTECTION_AND_RECONCILIATION')
        proposal = h.worker._cycle_proposal(deepcopy(before), context, h.oracle.now(), entries_enabled=True)
        self.assertEqual(proposal['operation'], 'CANCEL')
        self.assertEqual(proposal['card_id'], msg['occurrence_id'])

    def test_eligible_xrp_entry_retains_quote_bars_and_admission(self):
        h = self.h
        msg = r2732_message(entry=2.3, decision_ms=support.T-60000)
        h.seed(msg)
        before = h.store.load()
        with self.observations() as bars:
            context = h.provider.collect(before, entries_enabled=True)
        self.assertEqual(bars, ['XRP'])
        self.assertEqual(self.counts()['metaAndAssetCtxs'], 1)
        self.assertIn(msg['occurrence_id'], context['entry_accounts'])
        proposal = h.worker._cycle_proposal(deepcopy(before), context, h.oracle.now(), entries_enabled=True)
        self.assertEqual(proposal['operation'], 'ENTRY')
        self.assertEqual(proposal['card_id'], msg['occurrence_id'])
