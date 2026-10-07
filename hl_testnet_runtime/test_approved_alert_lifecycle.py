"""Approved-alert lifecycle regressions using disposable state and fake I/O.

The source approval is the entry decision. These tests deliberately provide no
source-price or Testnet MARK history; only current market and account evidence
are available. No exchange transport, signer or credentials are constructed.
"""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

import approved_alert_contract as contract
from approved_alert_fixtures import BASE, maxpain_alert
from . import experimental_execution_runtime as isolated
from . import experimental_live_runtime as live
from . import experimental_live_release as releases
from .experimental_execution_state import ExecutionState
from . import test_experimental_execution_runtime as software_support
from . import test_experimental_live_runtime as live_support
from .test_maxpain_execution import CREATED, ARM, plan


RULES = {
    'HYPE': 'HYPE_MAXPAIN_DIST05_15_LONG_TF',
    'ETH': 'ETH_MAXPAIN_LONG_DIST1_3',
    'SOL': 'SOL_MAXPAIN_DIST1_3_RANGE24',
}


def alert(*, symbol='HYPE', side='LONG', cycle='approved-case', approved_ms=BASE):
    value = maxpain_alert(approved_ms=approved_ms)
    value.update(symbol=symbol, side=side, rule_id=RULES[symbol])
    value['policy']['source_price'] = f'HYPERLIQUID_{symbol}_PERPETUAL_TRADE_1M'
    value['proof']['cycle_id'] = cycle
    if side == 'SHORT':
        value.update(entry='106.456', stop='108.595', take_profit='104.6735',
                     original_target='103.961')
        value['proof']['episode_key'] = '103.961'
    value['occurrence_id'] = contract.occurrence_id(value)
    return contract.validate(value)


def cancellation(value, at_ms):
    result = deepcopy(value)
    result.update(kind='CANCEL', source_state='CANCELED',
                  cancel_reason='SOURCE_APPROVAL_WITHDRAWN',
                  source_as_of=contract.iso_ms(at_ms),
                  source_sequence=at_ms * 10 + contract.RANK['CANCEL'])
    return contract.validate(result)


class CurrentOnlyExchange(software_support.SoftwareExchange):
    def collect(self, state):
        # Price equal to the approved entry would fail the former pre-touch
        # gate. It is a valid current quote after approval.
        for source in state['sources'].values():
            msg = source['source']
            role = 'long_account' if msg['side'] == 'LONG' else 'short_account'
            key = isolated._lane(state['routes'][role], msg['symbol'])
            self.mark.setdefault(key, msg['entry'])
        result = super().collect(state)
        result.pop('ranges')
        return result


class ApprovedAlertLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        for name in ('http.client.HTTPSConnection', 'socket.create_connection',
                     'hyperliquid_testnet_executor._wallet',
                     'hyperliquid_testnet_executor._signed_body',
                     'hl_testnet_runtime.experimental_execution_runtime._range'):
            blocker = patch(name, side_effect=AssertionError('NO_NETWORK_SIGNER_OR_PRICE_HISTORY'))
            blocker.start()
            self.addCleanup(blocker.stop)
        self.path = Path(self.tmp.name) / 'approved.isolated-experimental.sqlite3'
        self.venue = CurrentOnlyExchange(BASE + 1000)
        self.store = ExecutionState(self.path)
        self.store.initialize(software_support.ROUTES, not_before_ms=BASE - 60000)
        self.restart()

    def restart(self):
        self.store = ExecutionState(self.path)
        self.worker = isolated.IsolatedExecutionRuntime(self.store, self.venue, mode=isolated.MODE)

    def start(self, value=None):
        value = value or alert()
        self.worker.receive([value])
        return value, self.worker.run_once()

    def settle(self, cycles=4):
        for _ in range(cycles):
            self.worker.run_once(entries_enabled=False)

    def entry_requests(self):
        return [request for request in self.venue.requests
                if request['proposal']['operation'] == 'ENTRY']

    def test_screenshot_limit_is_submitted_without_retouche_or_history(self):
        value, result = self.start()
        self.assertEqual(result['operation'], 'ENTRY')
        self.assertEqual(len(self.venue.requests), 1)
        request = self.venue.requests[0]
        order = request['proposal']['action']['orders'][0]
        self.assertEqual(order['p'], '93.544')
        self.assertEqual(order['t'], {'limit': {'tif': 'Gtc'}})
        self.assertTrue(order['b'])
        self.assertFalse(order['r'])
        self.assertEqual(request['attempt_at_ms'], BASE + 1000)
        trade = self.store.load()['trades'][value['occurrence_id']]
        self.assertEqual(trade['source']['stop'], '91.405')
        self.assertEqual(trade['source']['take_profit'], '95.3265')
        self.assertFalse(trade['entry_fills'])
        self.assertEqual(result['real_exchange_requests_sent'], 0)

    def test_mark_below_buy_limit_does_not_request_another_adverse_move(self):
        key = isolated._lane(software_support.ROUTES['long_account']['account'], 'HYPE')
        self.venue.mark[key] = '93'
        _, result = self.start()
        self.assertEqual(result['operation'], 'ENTRY')
        self.assertEqual(self.entry_requests()[0]['proposal']['action']['orders'][0]['p'], '93.544')

    def test_mark_above_sell_limit_keeps_exact_sell_limit(self):
        value = alert(side='SHORT')
        key = isolated._lane(software_support.ROUTES['short_account']['account'], 'HYPE')
        self.venue.mark[key] = '107'
        _, result = self.start(value)
        self.assertEqual(result['operation'], 'ENTRY')
        order = self.entry_requests()[0]['proposal']['action']['orders'][0]
        # A venue-invalid six-digit sell limit is rounded upward, never down.
        self.assertGreaterEqual(Decimal(order['p']), Decimal(value['entry']))
        self.assertFalse(order['b'])

    def test_duplicate_recipient_restart_and_lost_reply_never_duplicate_entry(self):
        self.venue.lose_reply = True
        value, result = self.start()
        self.assertEqual(result['status'], 'OUTCOME_UNKNOWN_RECONCILIATION_REQUIRED')
        self.restart()
        self.worker.receive([value, deepcopy(value)])
        self.settle()
        self.assertEqual(len(self.entry_requests()), 1)
        self.assertEqual(len(self.store.load()['trades']), 1)

    def test_cancel_before_alert_is_durable_tombstone(self):
        value = alert()
        self.worker.receive([cancellation(value, BASE + 500)])
        self.restart()
        self.worker.receive([value, value])
        self.worker.run_once()
        self.assertFalse(self.venue.requests)
        source = self.store.load()['sources'][value['occurrence_id']]
        self.assertEqual(source['entry_permission'], 'RETIRED')

    def test_older_cancel_after_newer_recipient_delivery_still_retires_entry(self):
        value = alert()
        delivered = deepcopy(value)
        delivered.update(source_as_of=contract.iso_ms(BASE + 900),
                         source_sequence=(BASE + 900) * 10 + contract.RANK['ALERT'])
        self.worker.receive([contract.validate(delivered)])
        self.worker.receive([cancellation(value, BASE + 500)])
        self.worker.receive([delivered])
        self.worker.run_once()
        self.assertFalse(self.venue.requests)

    def test_accepted_gtc_survives_first_admission_expiry_and_restart(self):
        value, _ = self.start()
        entry_oid = self.venue.oid('ENTRY')
        self.venue.t = BASE + 2 * 86400000
        self.restart()
        self.worker.receive([value])
        self.settle()
        self.assertEqual(self.venue.orders[entry_oid]['view']['status'], 'OPEN')
        self.assertEqual(len(self.venue.requests), 1)
        self.venue.fill(entry_oid, self.venue.orders[entry_oid]['view']['wire_order']['s'])
        self.settle()
        self.assertEqual(self.store.load()['trades'][value['occurrence_id']]['phase'], 'OPEN')
        self.assertEqual({order['leg'] for order in self.venue.orders.values()},
                         {'ENTRY', 'STOP', 'TAKE_PROFIT'})

    def test_expired_first_receipt_cannot_admit_or_revive_after_restart(self):
        value = alert()
        self.venue.t = contract.moment_ms(value['expires_at'])
        self.worker.receive([value])
        self.restart()
        self.worker.receive([value])
        self.worker.run_once()
        self.assertFalse(self.venue.requests)
        self.assertEqual(self.store.load()['sources'][value['occurrence_id']]['terminal_reason'],
                         'APPROVED_ALERT_EXPIRED_BEFORE_ACCEPTANCE')

    def test_cancel_unfilled_gtc_proves_flat_then_cleans_up(self):
        value, _ = self.start()
        self.worker.receive([cancellation(value, self.venue.t)])
        self.settle()
        self.assertEqual(self.store.load()['trades'][value['occurrence_id']]['phase'],
                         'CANCELED_WITHOUT_FILL')
        self.assertEqual([request['proposal']['operation'] for request in self.venue.requests],
                         ['ENTRY', 'CANCEL'])

    def test_cancellation_between_durable_claim_and_transport_stays_unsent(self):
        value = alert()
        self.worker.receive([value])
        original = self.store.mutate
        calls = 0
        def mutate(transition):
            nonlocal calls
            result = original(transition)
            calls += 1
            if calls == 1:
                self.worker.receive([cancellation(value, self.venue.t)])
            return result
        with patch.object(self.store, 'mutate', side_effect=mutate):
            result = self.worker.run_once()
        self.assertEqual(result['status'], 'CANCELED_BEFORE_TRANSPORT')
        self.assertFalse(self.venue.requests)
        self.restart()
        self.worker.receive([value])
        self.worker.run_once()
        self.assertFalse(self.venue.requests)

    def test_cancel_after_partial_fill_protects_actual_fill_and_cancels_remainder(self):
        value, _ = self.start()
        entry_oid = self.venue.oid('ENTRY')
        self.venue.fill(entry_oid, '1', value['entry'])
        self.worker.receive([cancellation(value, self.venue.t)])
        self.settle()
        trade = self.store.load()['trades'][value['occurrence_id']]
        self.assertEqual(isolated._remaining(trade), Decimal('1'))
        self.assertEqual(self.venue.orders[entry_oid]['view']['status'], 'CANCELED')
        for leg in ('STOP', 'TAKE_PROFIT'):
            order = self.venue.orders[self.venue.oid(leg)]['view']['wire_order']
            self.assertEqual(order['s'], '1')
            self.assertTrue(order['r'])
        self.assertEqual([request['proposal']['operation'] for request in self.venue.requests],
                         ['ENTRY', 'CREATE_EXIT', 'CREATE_EXIT', 'CANCEL'])

    def test_fill_racing_entry_cancellation_is_reconciled_and_exits_resized(self):
        value, _ = self.start()
        entry_oid = self.venue.oid('ENTRY')
        self.venue.fill(entry_oid, '1')
        self.worker.receive([cancellation(value, self.venue.t)])
        def before_send(request):
            if request['proposal']['operation'] == 'CANCEL':
                self.venue.fill(entry_oid, '1')
                self.venue.before_send = None
        self.venue.before_send = before_send
        self.settle(cycles=7)
        trade = self.store.load()['trades'][value['occurrence_id']]
        self.assertEqual(isolated._remaining(trade), Decimal('2'))
        self.assertEqual(self.venue.orders[entry_oid]['view']['status'], 'CANCELED')
        for leg in ('STOP', 'TAKE_PROFIT'):
            active = [order['view']['wire_order'] for order in self.venue.orders.values()
                      if order['leg'] == leg and order['view']['status'] == 'OPEN']
            self.assertEqual(len(active), 1)
            self.assertEqual(active[0]['s'], '2')
        self.assertEqual(len(self.entry_requests()), 1)

    def test_full_take_and_stop_closes_cleanup_peer_exit_after_restart(self):
        for leg in ('TAKE_PROFIT', 'STOP'):
            with self.subTest(exit=leg):
                value, _ = self.start(alert(cycle='close-' + leg))
                entry_oid = self.venue.oid('ENTRY')
                quantity = self.venue.orders[entry_oid]['view']['wire_order']['s']
                self.venue.fill(entry_oid, quantity)
                self.settle()
                self.venue.fill(self.venue.oid(leg), quantity)
                self.restart()
                self.settle()
                self.assertEqual(self.store.load()['trades'][value['occurrence_id']]['phase'], 'CLOSED')
                self.assertFalse(any(order['view']['status'] == 'OPEN'
                                     for order in self.venue.orders.values()))

    def test_parallel_alerts_route_long_short_accounts_and_fence_same_symbol(self):
        values = [alert(), alert(symbol='ETH'), alert(side='SHORT'),
                  alert(cycle='second-hype-long')]
        self.worker.receive(values)
        for _ in range(6):
            self.worker.run_once()
        trades = list(self.store.load()['trades'].values())
        self.assertEqual(len(trades), 3)
        self.assertEqual(len({(trade['account'], trade['symbol']) for trade in trades}), 3)
        for trade in trades:
            expected = 'long_account' if trade['side'] == 'LONG' else 'short_account'
            self.assertEqual(trade['role'], expected)
            self.assertEqual(trade['account'], software_support.ROUTES[expected]['account'])
        self.assertEqual(len(self.entry_requests()), 3)

    def test_concurrent_receivers_share_one_durable_entry_claim(self):
        value = alert()
        self.worker.receive([value])
        second = isolated.IsolatedExecutionRuntime(ExecutionState(self.path), self.venue,
                                                   mode=isolated.MODE)
        barrier = threading.Barrier(2)
        original = self.venue.collect
        def collect(state):
            result = original(state)
            barrier.wait(timeout=5)
            return result
        def run(worker):
            try:
                return worker.run_once()['status']
            except isolated.RuntimeError as error:
                return str(error)
        with patch.object(self.venue, 'collect', side_effect=collect):
            with ThreadPoolExecutor(2) as pool:
                outcomes = list(pool.map(run, [self.worker, second]))
        self.assertIn('CONCURRENT_OBSERVATION_RELOAD_REQUIRED', outcomes)
        self.assertEqual(len(self.entry_requests()), 1)


class ApprovedContinuousReleaseTests(unittest.TestCase):
    """Testnet worker composition, but all ports are software test doubles."""
    real_pg = False
    make_worker = live_support.RuntimeContract.make_worker
    cycle = live_support.RuntimeContract.cycle
    restart = live_support.RuntimeContract.restart

    def setUp(self):
        live_support.RuntimeContract.setUp(self)
        self.venue = CurrentOnlyExchange(BASE + 1000)
        self.make_worker(not_before=BASE - 60000)
        self.release.update(entry_policy=releases.CONTINUOUS_ENTRY,
                            entry_expires_at_ms=None)
        self.restart()

    def test_continuous_release_accepts_new_approval_two_days_later(self):
        self.venue.t = BASE + 2 * 86400000 + 1000
        value = alert(approved_ms=BASE + 2 * 86400000, cycle='later-fresh')
        self.worker.receive([value])
        result = self.cycle()
        self.assertEqual(result['operation'], 'ENTRY')
        self.assertEqual(len(self.venue.requests), 1)
        self.assertEqual(self.venue.requests[0]['proposal']['action']['orders'][0]['p'], '93.544')

    def test_continuous_release_does_not_bypass_individual_alert_expiry(self):
        self.venue.t = BASE + 2 * 86400000
        self.worker.receive([alert()])
        self.cycle()
        self.assertFalse(self.venue.requests)

    def test_continuous_release_refuses_legacy_pretouch_maxpain(self):
        self.venue.t = CREATED
        self.make_worker(not_before=CREATED - 60000)
        self.release.update(entry_policy=releases.CONTINUOUS_ENTRY,
                            entry_expires_at_ms=None)
        self.restart()
        value = plan()
        self.worker.receive([value])
        self.venue.t = ARM
        self.cycle()
        self.assertFalse(self.venue.requests)
        self.assertEqual(self.store.load()['entry_blocked'][value['occurrence_id']],
                         'CONTINUOUS_MAXPAIN_REQUIRES_APPROVED_ALERT')


if __name__ == '__main__':
    unittest.main()
