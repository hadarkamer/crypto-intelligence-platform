"""Collector -> adapter and worker -> final wire boundary exit regressions.

All venue facts and sends are software fixtures. The imported harness disables
HTTP, sockets and wallet construction; no production account is contacted.
"""
from copy import deepcopy
from decimal import Decimal, ROUND_DOWN
import unittest

from . import card_lifecycle as life, card_sync_evidence as sync
from . import experimental_execution_evidence as evidence
from . import experimental_execution_dispatch as boundary
from . import experimental_execution_runtime as runtime
from . import test_experimental_dispatch_integration as integration
from .test_experimental_live_provider import RawTestnetFixture
from .test_approved_alert_lifecycle import alert


def protected_harness(test, side):
    harness = integration.DispatchIntegrationTests(); harness.setUp()
    test.addCleanup(harness.doCleanups)
    if side == 'LONG':
        harness.msg = alert(side=side, approved_ms=harness.venue.t-10000)
        account = harness.store.load()['routes']['long_account']
        harness.venue.mark[runtime._lane(account, harness.msg['symbol'])] = harness.msg['entry']
    harness.worker.receive([harness.msg]); harness.worker.run_once()
    entry_oid = harness.venue.oid('ENTRY'); entry = harness.venue.orders[entry_oid]['view']
    if entry['status'] == 'OPEN': harness.venue.fill(entry_oid, entry['wire_order']['s'])
    for _ in range(3): harness.worker.run_once()
    return harness


class ActivatedTakeFixture(RawTestnetFixture):
    """Original trigger remains in orderStatus; same OID rests after activation."""
    def __init__(self, harness, oid, activation):
        super().__init__(harness.venue, harness.store.load, object())
        self.oid, self.activation = oid, activation
        self.converged = False

    def status(self, oid):
        raw = super().status(oid)
        if str(oid) == self.oid and not self.converged:
            raw['order'].update(status='triggered', statusTimestamp=self.activation)
            raw['order']['order']['sz'] = raw['order']['order']['origSz']
        return raw

    def read(self, kind, *args, **kwargs):
        rows = super().read(kind, *args, **kwargs)
        if kind == 'frontendOpenOrders':
            for row in rows:
                if str(row['oid']) == self.oid:
                    view = self.exchange.orders[self.oid]['view']
                    filled = sum((Decimal(f['quantity']) for f in view['fills']), Decimal(0))
                    row.update(isTrigger=False, triggerPx='0', triggerCondition='Triggered',
                               timestamp=self.activation,
                               sz=life.text(Decimal(row['origSz']) - filled))
        return rows


class TriggeredTakeAdapterTests(unittest.TestCase):
    side = 'SHORT'
    def setUp(self):
        self.h = protected_harness(self, self.side)
        self.state = self.h.store.load()
        # The software venue names executions ``fill1_0``; RawTestnetFixture
        # exposes that same execution using a deterministic numeric exchange
        # trade ID. The adapter's saved facts must use the identical raw-domain
        # identity, rather than presenting every pre-existing fill as replaced.
        def raw_fill(fill):
            return {**deepcopy(fill),
                'fill_id': 'hl:' + str(int(life.digest(fill['fill_id'])[:12], 16))}
        for trade in self.state['trades'].values():
            for key in ('entry_fills', 'exit_fills'):
                rows = [raw_fill(fill) for fill in trade[key].values()]
                trade[key] = {fill['fill_id']: fill for fill in rows}
            for order in trade['orders'].values():
                order['fills'] = [raw_fill(fill) for fill in order['fills']]
        for snapshot in self.state['snapshots'].values():
            for order in snapshot['orders']:
                order['fills'] = [raw_fill(fill) for fill in order['fills']]
        self.trade = self.state['trades'][self.h.msg['occurrence_id']]
        self.partial_quantity = life.text((Decimal(self.trade['quantity'])/2).quantize(Decimal('.01'), rounding=ROUND_DOWN))
        original = next(s for s in self.h.venue.collect(self.state)['snapshots']
                        if s['account'] == self.trade['account'])
        self.owner, self.previous = integration.legacy_view(self.trade, original)
        # Bootstrap from the independently known flat point, so raw exchange
        # trade IDs are used throughout this collector replay.
        self.previous.update(at_ms=self.state['not_before_ms'], position_quantity='0',
                             fills=[], open_orders=[], terminal_orders=[])
        self.oid = self.h.venue.oid('TAKE_PROFIT')
        self.h.venue.t += 1
        self.raw = ActivatedTakeFixture(self.h, self.oid, self.h.venue.t)

    def collect(self):
        result = sync.collect(dict(bindings=[self.owner], snapshot=self.previous), self.raw,
                              clock=self.h.venue.now, elapsed=lambda: 0)
        lookups = {runtime.wire.requested_order(r['proposal']['action'])['c']:
                   self.raw.lookup(self.trade['account'], runtime.wire.requested_order(r['proposal']['action'])['c'])
                   for r in self.state['requests'].values() if r['proposal']['action']['type'] != 'cancel'}
        return result['snapshot'], lookups

    def convert(self, snapshot, lookups):
        return evidence.normalize_snapshot(self.state, snapshot, lookups, now_ms=self.h.venue.t)

    def test_collector_activated_partial_take_retains_original_wire_and_exact_remainder(self):
        self.h.venue.fill(self.oid, self.partial_quantity)
        snapshot, lookups = self.collect()
        take = next(o for o in snapshot['open_orders'] if o['oid'] == self.oid)
        self.assertEqual((take['order_type'], take['trigger_price']), ('TRIGGERED_TP_LIMIT', None))
        before = deepcopy((self.state, snapshot, lookups))
        normalized = self.convert(snapshot, lookups)
        result = next(o for o in normalized['orders'] if o['oid'] == self.oid)
        expected = self.h.venue.orders[self.oid]['view']['wire_order']
        self.assertEqual(result['wire_order'], expected)
        self.assertEqual(result['status'], 'OPEN')
        self.assertEqual(sum(Decimal(f['quantity']) for f in result['fills']), Decimal(self.partial_quantity))
        self.assertEqual(Decimal(take['quantity']), Decimal(expected['s']) - Decimal(self.partial_quantity))
        self.assertEqual((self.state, snapshot, lookups), before)

    def test_trigger_activation_without_any_fill_remains_open(self):
        snapshot, lookups = self.collect()
        result = next(o for o in self.convert(snapshot, lookups)['orders'] if o['oid'] == self.oid)
        self.assertEqual((result['status'], result['fills']), ('OPEN', []))

    def test_collector_full_fill_at_triggered_status_preserves_terminal_time_on_convergence(self):
        self.h.venue.fill(self.oid, self.trade['quantity'])
        snapshot, lookups = self.collect()
        normalized = self.convert(snapshot, lookups)
        take = next(o for o in normalized['orders'] if o['oid'] == self.oid)
        self.assertEqual(take['status'], 'FILLED')
        self.assertGreater(take['at_ms'], self.raw.activation)
        replay = deepcopy(self.state)
        replay['snapshots'] = {}
        trade = replay['trades'][self.h.msg['occurrence_id']]
        trade.update(orders={}, order_legs={}, entry_fills={}, exit_fills={})
        for request in replay['requests'].values():
            request.update(phase='OUTCOME_UNKNOWN', observed_oid=None)
        self.h.worker._snapshot(replay, normalized, self.h.venue.t)
        # The venue can later publish `filled`; replay must retain the original
        # terminal certificate, not change already committed order facts.
        self.previous = snapshot
        self.h.venue.t += 10
        self.h.venue.orders[self.oid]['view']['at_ms'] = self.h.venue.t
        self.raw.converged = True
        again, lookups = self.collect()
        normalized_again = self.convert(again, lookups)
        later = next(o for o in normalized_again['orders'] if o['oid'] == self.oid)
        self.assertEqual(later, take)
        self.h.worker._snapshot(replay, normalized_again, self.h.venue.t)
        self.assertEqual(trade['orders'][self.oid], take)

    def test_activated_type_cannot_be_claimed_without_triggered_lookup(self):
        snapshot, lookups = self.collect()
        take = next(v for v in lookups.values() if str(v['order']['order']['oid']) == self.oid)
        take['order']['status'] = 'open'
        with self.assertRaisesRegex(evidence.EvidenceError, 'OPEN_ORDER_TRIGGER_TERMS_CHANGED'):
            self.convert(snapshot, lookups)

    def test_triggered_lookup_cannot_keep_fictitious_live_trigger_or_plain_limit(self):
        snapshot, lookups = self.collect()
        for change in (dict(order_type='LIMIT'), dict(order_type='TP_LIMIT'),
                       dict(trigger_price=self.trade['prices']['take_profit'])):
            with self.subTest(change=change):
                changed = deepcopy(snapshot)
                next(o for o in changed['open_orders'] if o['oid'] == self.oid).update(change)
                with self.assertRaisesRegex(evidence.EvidenceError, 'OPEN_ORDER_TRIGGER_TERMS_CHANGED'):
                    self.convert(changed, lookups)

    def test_activated_terms_size_direction_and_ownership_remain_exact(self):
        self.h.venue.fill(self.oid, self.partial_quantity)
        snapshot, lookups = self.collect()
        wrong_side = 'A' if self.side == 'SHORT' else 'B'
        for change in (dict(quantity='1'), dict(price='1'), dict(side=wrong_side), dict(reduce_only=False)):
            with self.subTest(change=change):
                changed = deepcopy(snapshot)
                next(o for o in changed['open_orders'] if o['oid'] == self.oid).update(change)
                with self.assertRaisesRegex(evidence.EvidenceError, 'OPEN_ORDER_TERMS_OR_QUANTITY_CHANGED'):
                    self.convert(changed, lookups)
        for change in (dict(cloid='0x'+'f'*32), dict(origSz='1'), dict(triggerPx='1')):
            with self.subTest(parent=change):
                changed = deepcopy(lookups)
                next(v for v in changed.values() if str(v['order']['order']['oid']) == self.oid)['order']['order'].update(change)
                with self.assertRaises(ValueError): self.convert(snapshot, changed)

    def test_fills_before_trigger_and_changed_parent_creation_are_rejected(self):
        self.h.venue.fill(self.oid, self.partial_quantity)
        snapshot, lookups = self.collect()
        changed = deepcopy(snapshot)
        next(f for f in changed['fills'] if f['oid'] == self.oid)['at_ms'] = self.raw.activation - 1
        with self.assertRaisesRegex(evidence.EvidenceError, 'TRIGGERED_TAKE_PROOF_INVALID'):
            self.convert(changed, lookups)
        parent = next(v for v in lookups.values() if str(v['order']['order']['oid']) == self.oid)['order']['order']
        parent['timestamp'] = self.raw.activation + 1
        with self.assertRaisesRegex(evidence.EvidenceError, 'TRIGGERED_TAKE_PROOF_INVALID'):
            self.convert(snapshot, lookups)

    def test_activation_with_missing_remainder_never_becomes_full_fill(self):
        self.h.venue.fill(self.oid, self.partial_quantity)
        snapshot, lookups = self.collect()
        take = next(o for o in snapshot['open_orders'] if o['oid'] == self.oid)
        snapshot['open_orders'].remove(take)
        snapshot['terminal_orders'].append(dict(account=take['account'], symbol=take['symbol'], oid=self.oid,
            state='FILLED', filled_quantity=self.partial_quantity, at_ms=self.h.venue.t))
        with self.assertRaisesRegex(evidence.EvidenceError, 'LOOKUP_AND_SNAPSHOT_TERMINALITY_CONFLICT'):
            self.convert(snapshot, lookups)

    def test_full_fill_requires_real_last_fill_time_and_no_excess_quantity(self):
        self.h.venue.fill(self.oid, self.trade['quantity'])
        snapshot, lookups = self.collect()
        changed = deepcopy(snapshot)
        next(t for t in changed['terminal_orders'] if t['oid'] == self.oid)['at_ms'] = self.raw.activation
        with self.assertRaisesRegex(evidence.EvidenceError, 'LOOKUP_AND_SNAPSHOT_TERMINALITY_CONFLICT'):
            self.convert(changed, lookups)
        next(f for f in snapshot['fills'] if f['oid'] == self.oid)['quantity'] = life.text(Decimal(self.trade['quantity'])+1)
        with self.assertRaisesRegex(evidence.EvidenceError, 'FILLS_EXCEED_OWNED_ORDER_SIZE'):
            self.convert(snapshot, lookups)

    def test_changed_previously_observed_oid_is_rejected_even_without_reply_oid(self):
        snapshot, lookups = self.collect()
        request = next(r for r in self.state['requests'].values() if r.get('observed_oid') == self.oid)
        request['observed_oid'] = '999'
        with self.assertRaisesRegex(evidence.EvidenceError, 'PREVIOUSLY_OWNED_ORDER_ID_CHANGED'):
            self.convert(snapshot, lookups)


class MixedExitFinalBoundaryTests(unittest.TestCase):
    side = 'SHORT'
    def setUp(self):
        self.h = protected_harness(self, self.side)

    def card(self): return self.h.store.load()['trades'][self.h.msg['occurrence_id']]

    def partial_take_then_stop(self, *, partial_stop=False, lost_reply=False):
        quantity = Decimal(self.card()['quantity'])
        self.take_quantity = (quantity/2).quantize(Decimal('.01'), rounding=ROUND_DOWN)
        stop_remainder = quantity-self.take_quantity
        take = self.h.venue.oid('TAKE_PROFIT')
        self.h.venue.fill(take, life.text(self.take_quantity))
        self.h.venue.lose_reply = lost_reply
        result = self.h.worker.run_once()
        self.assertEqual(self.h.venue.requests[-1]['proposal']['operation'], 'AMEND_EXIT')
        if lost_reply:
            self.assertEqual(result['status'], 'OUTCOME_UNKNOWN_RECONCILIATION_REQUIRED')
        self.h.worker = runtime.IsolatedExecutionRuntime(self.h.store, self.h.venue, mode=runtime.MODE)
        for _ in range(3): self.h.worker.run_once()
        stop = self.h.venue.oid('STOP')
        self.assertEqual(Decimal(self.h.venue.orders[stop]['view']['wire_order']['s']), stop_remainder)
        stop_fill = (stop_remainder/2).quantize(Decimal('.01'), rounding=ROUND_DOWN) if partial_stop else stop_remainder
        self.expected_remaining = stop_remainder-stop_fill
        self.h.venue.fill(stop, life.text(stop_fill))
        return quantity, take, stop

    def test_partial_take_then_remainder_stop_cleans_up_at_actual_final_boundary(self):
        original = deepcopy(self.card())
        quantity, take, stop = self.partial_take_then_stop(lost_reply=True)
        before = len(self.h.venue.boundary_calls)
        self.h.venue.lose_reply = True
        result = self.h.worker.run_once()
        self.assertEqual(self.h.venue.requests[-1]['proposal']['operation'], 'CANCEL')
        self.assertEqual(result['status'], 'OUTCOME_UNKNOWN_RECONCILIATION_REQUIRED')
        self.assertEqual(self.h.venue.boundary_calls[-1]['action']['cancels'][0]['o'], int(take))
        self.h.worker = runtime.IsolatedExecutionRuntime(self.h.store, self.h.venue, mode=runtime.MODE)
        for _ in range(4): self.h.worker.run_once()
        self.assertEqual(len(self.h.venue.boundary_calls)-before, 1)
        card = self.card()
        self.assertEqual(card['phase'], 'CLOSED')
        self.assertEqual(runtime._remaining(card), 0)
        self.assertEqual(card['source'], original['source'])
        self.assertEqual(card['prices'], original['prices'])
        snap = next(s for s in self.h.venue.collect(self.h.store.load())['snapshots'] if s['account'] == card['account'])
        owner, owned = integration.legacy_view(card, snap)
        result = life.review([owner], owned, now_ms=self.h.venue.t)
        self.assertTrue(result['cards'][0]['closure_verified'])
        self.assertEqual(Decimal(result['cards'][0]['exit_quantity']), quantity)
        self.assertFalse(owned['open_orders'])

    def test_partial_fills_on_both_legs_resize_to_remaining_at_actual_boundary(self):
        quantity, take, stop = self.partial_take_then_stop(partial_stop=True)
        before = len(self.h.venue.boundary_calls)
        result = self.h.worker.run_once()
        self.assertEqual(result['operation'], 'AMEND_EXIT')
        for _ in range(3): self.h.worker.run_once()
        self.assertEqual(len(self.h.venue.boundary_calls)-before, 1)
        self.assertEqual(runtime._remaining(self.card()), self.expected_remaining)
        for leg in ('STOP', 'TAKE_PROFIT'):
            order = self.h.venue.orders[self.h.venue.oid(leg)]['view']
            left = Decimal(order['wire_order']['s'])-sum(Decimal(f['quantity']) for f in order['fills'])
            self.assertEqual(left, self.expected_remaining)

    def test_cleanup_still_refuses_overfill_unowned_fill_wrong_side_and_early_exit(self):
        quantity, take, stop = self.partial_take_then_stop()
        self.h.worker.run_once()
        request = deepcopy(self.h.venue.requests[-1])
        # Construct the genuine just-before-send context from the same exact
        # owned orders, then falsify one evidence property per negative case.
        self.h.venue.orders[take]['view']['status'] = 'OPEN'
        context = self.h.venue.context_for(request)
        self.assertTrue(boundary.review(request, context, now_ms=self.h.venue.t)['reviewed'])
        for kind in ('overfill', 'unowned', 'wrong_side', 'early_exit', 'incomplete'):
            with self.subTest(kind=kind):
                bad = deepcopy(context); snap = bad['owner_snapshot']
                exit_fill = next(f for f in snap['fills'] if f['oid'] == stop)
                if kind == 'overfill': exit_fill['quantity'] = life.text(quantity)
                elif kind == 'unowned': exit_fill['oid'] = '999'
                elif kind == 'wrong_side': exit_fill['side'] = 'A' if exit_fill['side'] == 'B' else 'B'
                elif kind == 'early_exit': exit_fill['at_ms'] = min(f['at_ms'] for f in snap['fills'])-1
                else: snap['history_complete'] = False
                with self.assertRaisesRegex(boundary.BoundaryError, 'EXIT_OWNERSHIP_UNRECONCILED'):
                    boundary.review(request, bad, now_ms=self.h.venue.t)


class TriggeredTakeAdapterLongTests(TriggeredTakeAdapterTests):
    side = 'LONG'


class MixedExitFinalBoundaryLongTests(MixedExitFinalBoundaryTests):
    side = 'LONG'


if __name__ == '__main__': unittest.main()
