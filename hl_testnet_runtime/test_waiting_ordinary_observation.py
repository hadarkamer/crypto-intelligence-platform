"""Waiting stops extra actions; normal verified information still advances.

Real service/provider/lifecycle composition with controlled raw venue replies.
PostgreSQL acceptance is kept in the separate end-to-end regression suite.
"""
from copy import deepcopy
import unittest
from unittest.mock import patch

from experimental_execution_fixtures import r2732_message
from . import position_recovery as recovery
from . import experimental_execution_runtime as core
from . import test_experimental_live_service as support


class WaitingOrdinaryObservationTests(unittest.TestCase):
    def setUp(self):
        self.fx = support.FullConnectionTests(methodName='runTest')
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        self.msg = r2732_message(entry=2.3, decision_ms=support.T-60000)
        self.cid = self.msg['occurrence_id']
        self.fx.seed(self.msg)
        for _ in range(4):
            self.fx.service.tick()
            self.fx.oracle.t += 50
        self.assertEqual(self.fx.store.load()['trades'][self.cid]['phase'], 'OPEN')
        self.assertEqual(len(self.fx.http), 3)

    def row(self):
        return self.fx.store.load()['position_recovery'][self.cid]

    def wait_after_missing_observation(self):
        raw = self.fx.raw.read
        def fail(kind, *args, **kwargs):
            if kind == 'userFillsByTime':
                raise ValueError('PUBLIC_READ_UNAVAILABLE')
            return raw(kind, *args, **kwargs)
        with patch.object(self.fx.raw, 'read', side_effect=fail):
            self.fx.service.tick()
            self.assertEqual(self.row()['phase'], 'RECOVERING')
            # The real wall-clock window expires; no fixed-time retry illusion.
            self.fx.oracle.t += recovery.WINDOW_MS + 1
            self.fx.service.tick()
        self.assertEqual(self.row()['phase'], 'WAITING')
        return deepcopy(self.row())

    def test_fresh_valid_exits_resolve_waiting_without_choice_or_send(self):
        before = self.wait_after_missing_observation()
        count = len(self.fx.http)
        self.fx.oracle.t += 5000
        calls = len(self.fx.raw.calls)
        self.fx.service.tick()
        row = self.row()
        self.assertEqual(row['phase'], 'RESOLVED')
        self.assertEqual(row['reason'], 'EXITS_PRESENT')
        self.assertEqual(row['facts']['exits'], {'STOP': 'PRESENT', 'TAKE_PROFIT': 'PRESENT'})
        self.assertNotIn('choice', row)
        for key in ('round', 'read_calls', 'read_weight', 'sends', 'started_ms', 'deadline_ms'):
            self.assertEqual(row[key], before[key])
        self.assertEqual(len(self.fx.http), count)
        observed = self.fx.raw.calls[calls:]
        self.assertFalse(any(call[0] == 'lookup' for call in observed))
        self.assertEqual(sum(call[0] == 'userFillsByTime' for call in observed), 1)

    def test_exit_fill_after_waiting_closes_only_after_all_orders_terminal(self):
        before = self.wait_after_missing_observation()
        count = len(self.fx.http)
        quantity = self.fx.store.load()['trades'][self.cid]['quantity']
        self.fx.oracle.fill(self.fx.oracle.oid('STOP'), quantity)
        self.fx.oracle.t += 5000
        self.fx.service.tick()
        state = self.fx.store.load()
        self.assertEqual(self.row()['phase'], 'WAITING')
        self.assertEqual(core._remaining(state['trades'][self.cid]), 0)
        self.assertNotIn(state['trades'][self.cid]['phase'], core.FINAL)
        self.assertEqual(len(self.fx.http), count)
        # The independent controlled venue reports TP already terminal.
        self.fx.oracle.orders[self.fx.oracle.oid('TAKE_PROFIT')]['view'].update(
            status='CANCELED', at_ms=self.fx.oracle.now())
        self.fx.oracle.t += 5000
        self.fx.service.tick()
        state = self.fx.store.load()
        self.assertEqual(state['trades'][self.cid]['phase'], 'CLOSED')
        self.assertEqual(self.row()['phase'], 'RESOLVED')
        self.assertEqual(self.row()['reason'], 'POSITION_CLOSED')
        self.assertEqual(self.row()['round'], before['round'])
        self.assertEqual(len(self.fx.http), count)
        previous = deepcopy(state['trades'][self.cid])
        self.fx.reconnect()
        self.fx.oracle.t += 5000
        self.fx.service.tick()
        self.assertEqual(self.fx.store.load()['trades'][self.cid]['entry_fills'], previous['entry_fills'])
        self.assertEqual(self.fx.store.load()['trades'][self.cid]['exit_fills'], previous['exit_fills'])
        self.assertEqual(len(self.fx.http), count)

    def test_fresh_account_inventory_cannot_certify_old_lane_orders(self):
        self.wait_after_missing_observation()
        state = self.fx.store.load()
        trade = state['trades'][self.cid]
        lane = core._lane(trade['account'], trade['symbol'])
        old = state['snapshots'][lane]['at_ms']
        context = dict(blocked_lanes={}, snapshots=[], external_observation=dict(accounts={
            trade['account']: dict(observed_at_ms=self.fx.oracle.now(), positions={trade['symbol']: '-1'}, orders=[])}))
        facts = recovery._facts(state, trade, context, self.fx.oracle.now())
        self.assertGreater(self.fx.oracle.now()-old, 15000)
        self.assertFalse(facts['complete'])
        self.assertFalse(facts['closed'])
        self.assertNotEqual(facts['exits'], {'STOP': 'PRESENT', 'TAKE_PROFIT': 'PRESENT'})

    def test_missing_exit_stays_waiting_without_another_repair_round(self):
        before = self.wait_after_missing_observation()
        count = len(self.fx.http)
        take = self.fx.oracle.oid('TAKE_PROFIT')
        self.fx.oracle.orders[take]['view'].update(
            status='CANCELED', at_ms=self.fx.oracle.now())
        status = self.fx.raw.status
        def system_cancel(oid):
            value = status(oid)
            if str(oid) == take:
                value['order']['status'] = 'marginCanceled'
            return value
        # A venue system cancellation leaves a recovery obligation. Generic
        # canceled without a bot intent correctly invokes manual ownership.
        with patch.object(self.fx.raw, 'status', side_effect=system_cancel):
            for _ in range(3):
                self.fx.oracle.t += 5000
                self.fx.service.tick()
        self.assertEqual(self.row()['phase'], 'WAITING')
        self.assertEqual(self.row()['facts']['exits'], {'STOP': 'PRESENT', 'TAKE_PROFIT': 'MISSING'})
        for key in ('round', 'read_calls', 'read_weight', 'sends'):
            self.assertEqual(self.row()[key], before[key])
        self.assertEqual(len(self.fx.http), count)

    def test_ordinary_observation_does_not_split_truncated_history(self):
        before = self.wait_after_missing_observation()
        count = len(self.fx.http)
        raw = self.fx.raw.read
        def truncated(kind, *args, **kwargs):
            rows = raw(kind, *args, **kwargs)
            if kind == 'userFillsByTime':
                # Every requested window is full, including hypothetical
                # account fallback/child windows; a one-shot page masks retries.
                return [dict(time=kwargs['end'], coin='XRP', tid=991000,
                    oid=int(self.fx.oracle.oid('ENTRY')), sz='1', px='2.3',
                    fee='0', feeToken='USDC', side='A')] * 2000
            return rows
        start = len(self.fx.raw.calls)
        with patch.object(self.fx.raw, 'read', side_effect=truncated):
            self.fx.oracle.t += 5000
            self.fx.service.tick()
        state = self.fx.store.load()
        lane = core._lane(state['trades'][self.cid]['account'], 'XRP')
        self.assertEqual(state['blocked_lanes'][lane], 'RECOVERY_EXTRA_HISTORY_INQUIRY_REQUIRES_CHOICE')
        self.assertEqual(self.row()['phase'], 'WAITING')
        self.assertEqual(self.row()['read_calls'], before['read_calls'])
        # The failed lane page must not restart through account background
        # history, nor trigger recursive child-page calls.
        reads = [call for call in self.fx.raw.calls[start:] if call[0] == 'userFillsByTime']
        self.assertEqual(len(reads), 1)
        self.assertEqual(len(self.fx.http), count)

    def test_waiting_defers_only_its_accounts_uncovered_background_history(self):
        self.wait_after_missing_observation()
        state = self.fx.store.load()
        waiting_account = state['trades'][self.cid]['account']
        other_account = next(a for a in state['routes'].values() if a != waiting_account)
        background = object()
        cache = object()
        observed = {}
        def observe(value, legacy, inventories, shared, *, now_ms, fill_reader):
            account = next(iter(inventories))
            observed[account] = (shared, fill_reader)
            return dict(accounts={}, human_managed={}, owned_request_observations={})
        with patch('hl_testnet_runtime.experimental_live_provider.external.observe', side_effect=observe):
            self.fx.provider._observe_external(state, {},
                {waiting_account: {}, other_account: {}}, cache,
                self.fx.oracle.now(), fill_reader=background)
        self.assertEqual(observed[waiting_account], (cache, None))
        self.assertEqual(observed[other_account], (cache, background))

    def test_settled_request_without_oid_cannot_start_cloid_inquiry_while_waiting(self):
        before = self.wait_after_missing_observation()
        state = self.fx.store.load()
        rejected = deepcopy(next(r for r in state['requests'].values()
            if r['proposal']['leg'] == 'TAKE_PROFIT'))
        rejected.update(request_id='e'*64, observed_oid=None,
            terminal_state='REJECTED_NO_ORDER', phase='OBSERVED',
            reply=dict(state='REJECTED', code='OTHER_REJECTION', oid=None))
        rejected['proposal']['action']['orders'][0]['c'] = '0x'+'e'*32
        state['requests'][rejected['request_id']] = rejected
        self.assertFalse(recovery.pending(state, self.cid))
        account = state['trades'][self.cid]['account']
        self.fx.oracle.t += 5000
        with patch.object(self.fx.provider, '_lookup') as lookup:
            context = self.fx.provider.collect(state)
        lookup.assert_not_called()
        self.assertEqual(context['blocked_lanes'][core._lane(account, 'XRP')],
            'RECOVERY_EXTRA_HISTORY_INQUIRY_REQUIRES_CHOICE')
        self.assertEqual(self.row()['phase'], 'WAITING')
        self.assertEqual(self.row()['read_calls'], before['read_calls'])
        self.assertEqual(len(self.fx.http), 3)

    def test_waiting_long_sparse_history_uses_original_retention_proof(self):
        before = self.wait_after_missing_observation()
        count = len(self.fx.http)
        self.fx.oracle.t += 3 * 86400000
        start = len(self.fx.raw.calls)
        self.fx.service.tick()
        state = self.fx.store.load()
        lane = core._lane(state['trades'][self.cid]['account'], 'XRP')
        self.assertNotIn(lane, state['blocked_lanes'])
        self.assertEqual(self.row()['phase'], 'RESOLVED')
        self.assertEqual(self.row()['round'], before['round'])
        self.assertFalse(any(call[0]=='lookup' for call in self.fx.raw.calls[start:]))
        self.assertLessEqual(len(self.fx.raw.calls[start:]), recovery.READ_CALLS)
        self.assertEqual(len(self.fx.http), count)


if __name__ == '__main__':
    unittest.main()
