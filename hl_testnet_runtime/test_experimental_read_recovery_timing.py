"""Read scheduling recovery against production collectors and pure admission.

Only the existing in-memory journal and synthetic raw exchange/price fixtures
are used. No provider decision is stubbed, and network sockets are forbidden.
"""
from copy import deepcopy
import unittest

import approved_alert_contract as contract
from approved_alert_fixtures import formula_alert, maxpain_alert
from experimental_execution_fixtures import r2732_message
from . import card_lifecycle as life
from . import experimental_execution_runtime as runtime
from . import experimental_plan_store as plan_store
from . import sol_g65_conditional_stop
from . import test_experimental_live_service as support


class RecordingPrices(support.SyntheticPrices):
    def __init__(self, oracle, store):
        super().__init__(oracle, store)
        self.calls = []

    def closed_bars_since(self, msg, now, cursor):
        self.calls.append(('bars', msg['occurrence_id'], now, cursor))
        return self.closed_bars(msg, now)

    def source_range(self, msg, now):
        self.calls.append(('source', msg['occurrence_id'], now))
        return super().source_range(msg, now)

    def mark_window(self, account, symbol, reference, now):
        self.calls.append(('window', account, symbol, reference, now))
        return super().mark_window(account, symbol, reference, now)


class ReadRecoveryTimingTests(unittest.TestCase):
    def setUp(self):
        # Composition avoids unittest rediscovering the support class's tests.
        self.h = support.FullConnectionTests()
        self.h.setUp()
        self.addCleanup(self.h.doCleanups)
        self.prices = RecordingPrices(self.h.oracle, self.h.store)
        self.h.provider.prices = self.prices

    def candidate(self):
        msg = r2732_message(entry=2.3, decision_ms=support.T-60000)
        self.h.seed(msg)
        return msg

    def block_account(self, role='short_account'):
        account = self.h.store.load()['routes'][role]
        # An unresolved legacy request is an existing entry-only account
        # fence. Its empty journal is synthetic; the real reviewer enforces it.
        self.h.provider.legacy_store.rows = [dict(account=account, symbol='DOGE',
            bucket=life.digest(['synthetic-unresolved-doge', account]), revision=1,
            pending=life.digest('synthetic-pending-request'), bindings=[], evidence=None)]
        return account

    def collect(self, *, entries=True):
        self.h.raw.calls.clear()
        self.prices.calls.clear()
        state = self.h.store.load()
        context = self.h.provider.collect(state, entries_enabled=entries)
        return state, context

    def project(self, state, context, *, entries=True):
        projected = deepcopy(state)
        proposal = self.h.worker._cycle_proposal(projected, context,
            self.h.oracle.now(), entries_enabled=entries)
        return projected, proposal

    def kinds(self):
        return [call[0] for call in self.h.raw.calls]

    def open_xrp(self):
        msg = self.candidate()
        for _ in range(4):
            self.h.service.tick()
        self.assertEqual(self.h.store.load()['trades'][msg['occurrence_id']]['phase'], 'OPEN')
        return msg

    def cancel_source(self, msg):
        h = self.h
        at = h.oracle.now()
        value = deepcopy(msg)
        value.update(kind='CANCEL', source_state='ENDED',
            cancel_reason='SOURCE_ENTRY_OBSERVED', source_as_of=contract.iso_ms(at),
            source_sequence=at*10+3,
            valid_until=contract.iso_ms(min(at+90000, contract.moment_ms(msg['expires_at']))))
        value = contract.validate(value)

        def retire(state):
            # The support fixture's source reducer originated in software
            # tests; use the real mainnet reducer for this cancellation.
            previous = deepcopy(state['sources'][msg['occurrence_id']])
            previous['domain'] = 'mainnet'
            state['sources'][msg['occurrence_id']] = plan_store.reduce_source(
                previous, value, now=contract.iso_ms(at),
                not_before=contract.iso_ms(state['not_before_ms']), domain='mainnet')[0]
        h.store.mutate(retire)

    def test_account_fence_release_uses_fresh_evidence_in_the_next_cycle(self):
        msg = self.candidate()
        cid = msg['occurrence_id']
        account = self.block_account()
        before, blocked = self.collect()
        self.assertEqual(blocked['account_entry_blocked'][account],
                         'UNRESOLVED_ACCOUNT_REQUEST_NO_NEW_ENTRY')
        self.assertNotIn('metaAndAssetCtxs', self.kinds())
        self.assertEqual(self.prices.calls, [])
        self.assertIsNone(self.project(before, blocked)[1])
        self.h.service.tick()
        self.assertEqual(self.h.store.load()['entry_decisions'][cid]['reason'],
                         'UNRESOLVED_ACCOUNT_REQUEST_NO_NEW_ENTRY')
        self.h.provider.legacy_store.rows.clear()
        self.h.oracle.t += 6000
        now = self.h.oracle.now()
        lane = runtime._lane(account, 'XRP')
        self.h.oracle.mark[lane] = '2.299'
        after, ready = self.collect()
        self.assertEqual(ready['entry_blocked'], {})
        self.assertEqual(ready['marks'][lane]['mark_price'], '2.299')
        self.assertEqual(ready['marks'][lane]['at_ms'], now)
        self.assertEqual(ready['account_inventory_at_ms'][account], now)
        self.assertEqual(ready['ranges'][cid]['source']['at_ms'], now)
        self.assertEqual(ready['ranges'][cid]['testnet']['at_ms'], now)
        for field in ('at_ms', 'signer_at_ms', 'allowance_at_ms'):
            self.assertEqual(ready['entry_accounts'][cid][field], now)
        for kind in ('frontendOpenOrders', 'clearinghouseState', 'metaAndAssetCtxs',
                     'userRole', 'userRateLimit'):
            self.assertIn(kind, self.kinds())
        proposal = self.project(after, ready)[1]
        self.assertEqual((proposal['operation'], proposal['card_id']), ('ENTRY', cid))
        self.assertEqual(after['sources'][cid]['source'], before['sources'][cid]['source'])
        self.assertEqual(after['sources'][cid]['source']['expires_at'], msg['expires_at'])

    def test_external_lane_exposure_clear_permits_entry_in_the_next_cycle(self):
        msg = self.candidate()
        cid = msg['occurrence_id']
        self.h.raw.extra_positions = [dict(position=dict(coin='XRP', szi='1'))]
        before, blocked = self.collect()
        account = before['routes']['short_account']
        lane = runtime._lane(account, 'XRP')
        self.assertNotIn(account, blocked['account_entry_blocked'])
        self.assertIn(lane, blocked['blocked_lanes'])
        self.assertEqual(self.prices.calls, [])
        self.assertIsNone(self.project(before, blocked)[1])
        self.h.service.tick()
        self.assertEqual(self.h.store.load()['entry_decisions'][cid]['reason'],
                         blocked['blocked_lanes'][lane])
        self.h.raw.extra_positions.clear()
        self.h.oracle.t += 6000
        after, ready = self.collect()
        self.assertNotIn(lane, ready['blocked_lanes'])
        self.assertEqual(ready['marks'][lane]['at_ms'], self.h.oracle.now())
        self.assertIn(cid, ready['ranges'])
        self.assertIn(cid, ready['entry_accounts'])
        proposal = self.project(after, ready)[1]
        self.assertEqual((proposal['operation'], proposal['card_id']), ('ENTRY', cid))

    def test_expired_source_does_not_revive_when_account_fence_clears(self):
        msg = self.candidate()
        self.block_account()
        before, blocked = self.collect()
        self.assertIsNone(self.project(before, blocked)[1])
        self.h.provider.legacy_store.rows.clear()
        self.h.oracle.t = contract.moment_ms(msg['expires_at'])
        after, ready = self.collect()
        self.assertFalse(runtime._source_active(after['sources'][msg['occurrence_id']], self.h.oracle.now()))
        self.assertIsNone(self.project(after, ready)[1])
        self.assertEqual(ready['entry_accounts'], {})
        self.assertEqual(ready['marks'], {})
        self.assertEqual(self.prices.calls, [])
        self.assertEqual(after['sources'][msg['occurrence_id']]['source'], msg)
        self.assertFalse(self.h.http)

    def test_canceled_source_does_not_revive_when_account_fence_clears(self):
        msg = self.candidate()
        self.block_account()
        before, blocked = self.collect()
        self.assertIsNone(self.project(before, blocked)[1])
        self.h.oracle.t += 6000
        self.cancel_source(msg)
        self.h.provider.legacy_store.rows.clear()
        after, ready = self.collect()
        self.assertFalse(runtime._source_active(after['sources'][msg['occurrence_id']], self.h.oracle.now()))
        self.assertIsNone(self.project(after, ready)[1])
        self.assertEqual(ready['entry_accounts'], {})
        self.assertEqual(ready['marks'], {})
        self.assertEqual(self.prices.calls, [])
        self.assertFalse(self.h.http)

    def test_expired_canceled_source_and_entry_halt_preserve_owned_stop_promotion(self):
        msg = self.open_xrp()
        cid = msg['occurrence_id']
        self.h.oracle.t = support.T+60000
        self.cancel_source(msg)
        self.h.oracle.t = support.T+120000
        unrelated = maxpain_alert(approved_ms=self.h.oracle.now())
        self.h.seed(unrelated)
        account = self.block_account()
        lane = runtime._lane(account, 'XRP')
        self.h.oracle.mark[lane] = '2.28'
        self.h.oracle.bars[cid] = [
            dict(open_at_ms=support.T, open='2.3', high='2.3', low='2.277', close='2.28'),
            dict(open_at_ms=support.T+60000, open='2.28', high='2.29', low='2.27', close='2.28')]
        before, context = self.collect(entries=False)
        self.assertIn(account, context['account_entry_blocked'])
        self.assertNotIn(lane, context['blocked_lanes'])
        self.assertEqual(context['marks'][lane]['at_ms'], self.h.oracle.now())
        self.assertEqual(set(context['marks']), {lane})
        self.assertEqual(context['bars'][cid], self.h.oracle.bars[cid])
        self.assertEqual(context['entry_accounts'], {})
        self.assertEqual(context['ranges'], {})
        self.assertEqual(self.kinds().count('metaAndAssetCtxs'), 1)
        self.assertEqual([call[0] for call in self.prices.calls], ['bars'])
        self.assertEqual(self.prices.calls[0][3], before['trades'][cid]['condition']['cursor_ms'])
        proposal = self.project(before, context, entries=False)[1]
        self.assertEqual((proposal['operation'], proposal['leg'], proposal['card_id']),
                         ('AMEND_EXIT', 'STOP', cid))

    def test_blocked_sol_formula_reuses_original_cursor_and_processes_overlap(self):
        h = self.h
        base = (h.oracle.now()//1800000+1)*1800000
        h.oracle.t = base+130000
        msg = formula_alert('sol_g65', approved_ms=base+120000)
        h.seed(msg)
        cid = msg['occurrence_id']
        self.block_account()
        initial = deepcopy(h.store.load()['formula_states'][cid])
        first = dict(open_at_ms=base+60000, open='100.6', high='100.7', low='99.5', close='100')
        second = dict(open_at_ms=base+120000, open='100', high='100.1', low='98.8', close='100')
        h.oracle.bars[cid] = [first]
        self.prices.calls.clear()
        h.service.tick()
        prior = deepcopy(h.store.load()['formula_states'][cid])
        self.assertEqual(prior['cursor_ms'], base+60000)
        self.assertEqual(self.prices.calls[0][3], initial['cursor_ms'])
        self.assertIsNone(prior['lock_effective_at_ms'])
        h.oracle.t = base+190000
        h.oracle.bars[cid] = [first, second]
        before, context = self.collect()
        self.assertIn(before['routes']['short_account'], context['account_entry_blocked'])
        self.assertEqual(self.prices.calls, [('bars', cid, h.oracle.now(), prior['cursor_ms'])])
        self.assertEqual(context['bars'][cid], [first, second])
        self.assertEqual(context['marks'], {})
        self.assertNotIn('metaAndAssetCtxs', self.kinds())
        projected, proposal = self.project(before, context)
        expected = sol_g65_conditional_stop.advance(prior, [first, second], now_ms=h.oracle.now())
        self.assertIsNone(proposal)
        self.assertEqual(projected['formula_states'][cid], expected)
        self.assertEqual(expected['cursor_ms'], base+120000)
        self.assertEqual(expected['lock_effective_at_ms'], base+180000)
        self.assertEqual(h.store.load()['formula_states'][cid], prior)
        self.assertFalse(h.http)

    def test_shared_symbol_quote_is_retained_only_for_the_owned_account_lane(self):
        owned = self.open_xrp()
        h = self.h
        h.oracle.t += 6000
        new = maxpain_alert(approved_ms=support.T)
        new.update(symbol='XRP', rule_id='XRP_MAXPAIN_LONG_DIST2_4_SHORT_TF',
                   entry='2.3', stop='2.1', take_profit='2.4', original_target='2.5')
        new['policy']['source_price'] = 'HYPERLIQUID_XRP_PERPETUAL_TRADE_1M'
        new['proof']['episode_key'] = '2.5'
        new['occurrence_id'] = contract.occurrence_id(new)
        new = contract.validate(new)
        h.seed(new)
        blocked_account = self.block_account('long_account')
        before, context = self.collect()
        owned_lane = runtime._lane(before['routes']['short_account'], 'XRP')
        blocked_lane = runtime._lane(blocked_account, 'XRP')
        self.assertIn(blocked_account, context['account_entry_blocked'])
        self.assertNotIn(blocked_lane, context['blocked_lanes'])
        self.assertEqual(self.kinds().count('metaAndAssetCtxs'), 1)
        self.assertEqual(set(context['marks']), {owned_lane})
        self.assertEqual(set(context['bars']), {owned['occurrence_id']})
        self.assertNotIn(new['occurrence_id'], context['entry_accounts'])
        self.assertEqual(context['entry_blocked'][new['occurrence_id']],
                         'UNRESOLVED_ACCOUNT_REQUEST_NO_NEW_ENTRY')
        self.assertIsNone(self.project(before, context)[1])


if __name__ == '__main__':
    unittest.main()
