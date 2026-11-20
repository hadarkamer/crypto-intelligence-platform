"""Independent archive and irreversible-send checks for manual operational close."""
from copy import deepcopy
from decimal import Decimal
import unittest

import approved_alert_contract as contract
from . import experimental_execution_dispatch as boundary
from . import experimental_external_activity as external
from . import position_recovery as recovery
from . import test_manual_operational_close as support
from .test_hype_watch_execution import hype_alert


@unittest.skipUnless(support.CI, 'Requires disposable loopback PostgreSQL; skipped is not verified')
class ManualOperationalReviewTests(unittest.TestCase):
    def setUp(self):
        # Composition avoids collecting the support test suite a second time.
        self.case = support.ManualOperationalPostgresTests(methodName='runTest')
        self.addCleanup(self.case.doCleanups)
        self.case.setUp()
        self.fx = self.case.fx

    def test_archive_keeps_incomplete_bound_order_without_terminal_index(self):
        def bind_known_partial(state):
            trade = state['trades'][self.case.cid]
            request = state['requests'][support.RID]
            order = request['proposal']['action']['orders'][0]
            facts = state['external_activity']['accounts'][self.case.account]['fills']
            fills = [{k:f[k] for k in ('fill_id','quantity','price','at_ms')}
                     for f in facts if f['oid'] == support.OID]
            trade['orders'][support.OID] = dict(oid=support.OID, cloid=order['c'],
                wire_order=deepcopy(order), status='OPEN', at_ms=support.ATTEMPT, fills=fills)
            trade['order_legs'][support.OID] = 'ENTRY'
            trade['entry_fills'] = {f['fill_id']:dict(**f, order_id=support.OID) for f in fills}
        self.fx.store.mutate(bind_known_partial)
        state = self.case.close()
        before = deepcopy(state['trades'][self.case.cid])
        result = self.fx.store.compact_history(now_ms=support.NOW+2000, force=True)
        self.assertEqual(result['archived'], 1)
        record = self.fx.store.archive_record(self.case.cid)
        self.assertEqual(record['trade'], before)
        self.assertEqual(record['requests'][support.RID]['phase'], 'OUTCOME_UNKNOWN')
        self.assertFalse(record['trade']['manual_closure_snapshot']['history_complete'])
        self.assertEqual(self.fx.store.archived_order_evidence(
            self.case.account, 'HYPE', [support.OID]), {})
        # A historical terminal answer/new historical fill is documentation,
        # not a violation of a terminal status that was never established.
        sample = deepcopy(before['manual_closure_snapshot'])
        late = deepcopy(sample['fills'][0])
        late.update(fill_id='hl:999999999999991', at_ms=late['at_ms']+1)
        sample['fills'].append(late)
        sample['terminal_orders'] = [dict(oid=support.OID, status='CANCELED')]
        self.assertEqual(self.fx.store.strip_archived_collector_snapshot(sample), sample)
        # Canonical late facts stay unallocated; they cannot resurrect a trade
        # or transfer fees/PnL to the new formula.
        delta = dict(late, side='A', origin='UNATTRIBUTED')
        self.fx.store.mutate(lambda state: external.record_fill_deltas(
            state, [delta], now_ms=support.NOW+2001))
        after = self.fx.store.load()
        self.assertNotIn(self.case.cid, after['trades'])
        self.assertNotIn(support.RID, after['requests'])
        self.assertNotIn('formula_accounting', after)
        self.assertTrue(any(f['fill_id']==late['fill_id'] for f in
            after['external_activity']['accounts'][self.case.account]['fills']))
        self.assertEqual(self.fx.store.archive_record(self.case.cid), record)
        self.case.restart()
        with self.assertRaises(ValueError):
            self.fx.store.request(support.RID)
        self.assertFalse(self.fx.http)
        # The retained late facts must survive the actual new-signal path,
        # including the provider, authenticated intake and final live boundary.
        self.fx.oracle.t = (self.fx.oracle.t//60000+1)*60000+5000
        msg = hype_alert(reference=self.fx.oracle.t-5000,
                         cycle='new-hype-after-archived-late-history')
        self.fx.seed(msg)
        result = self.fx.service.tick()
        self.assertEqual(result.get('operation'), 'ENTRY', result)
        for _ in range(4):
            self.fx.oracle.t += 1000
            self.fx.service.tick()
        state = self.fx.store.load()
        current = state['trades'][msg['occurrence_id']]
        self.assertEqual(current['source']['rule_id'], support.accounting.HYPE_FORMULA)
        self.assertEqual(Decimal(current['entry_risk']['planned_usd']), Decimal('2'))
        self.assertEqual([r['proposal']['operation'] for r in self.fx.oracle.requests],
                         ['ENTRY', 'CREATE_EXIT', 'CREATE_EXIT'])
        self.assertFalse(set(current['entry_fills']) & {f['fill_id'] for f in sample['fills']})
        self.assertTrue(any(f['fill_id']==late['fill_id'] for f in
            state['external_activity']['accounts'][self.case.account]['fills']))
        self.assertEqual(state['formula_accounting']['portfolios'][support.accounting.HYPE_FORMULA],
                         dict(balance_usd='40', realized_net_usd='0', revision=0))
        self.assertEqual(self.fx.store.archive_record(self.case.cid), record)
        self.assertNotIn(support.ATTEMPT, {r['nonce'] for r in self.fx.http})

    def test_preclosure_approved_signal_remains_blocked_after_restart(self):
        self.case.close()
        self.case.restart()
        reference = support.NOW//60000*60000
        msg = hype_alert(reference=reference, approved=support.NOW,
                         cycle='pre-operational-closure-queued-signal')
        self.assertGreater(contract.moment_ms(msg['expires_at']), self.fx.oracle.t)
        self.fx.seed(msg)
        self.fx.service.tick()
        state = self.fx.store.load()
        self.assertEqual(external.entry_reason(state,self.case.account,msg),
                         'NEW_ALERT_REQUIRED_AFTER_MANUAL_RELEASE')
        self.assertNotIn(msg['occurrence_id'], state['trades'])
        self.case.restart()
        self.fx.ingest(msg)
        self.fx.service.tick()
        self.assertNotIn(msg['occurrence_id'], self.fx.store.load()['trades'])
        self.assertFalse(self.fx.http)

    def test_retirement_presence_permanently_fences_send_even_if_certificate_changes(self):
        self.case.close()
        self.case.restart()
        request = self.fx.store.request(support.RID)
        for tamper in (False, True):
            with self.subTest(tampered=tamper):
                value = deepcopy(request)
                if tamper:
                    value['operational_retirement']['request_digest'] = '0'*64
                    self.assertFalse(external.request_retired(self.fx.store.load(), value))
                    self.assertTrue(external.request_pending(self.fx.store.load(), value))
                with self.assertRaisesRegex(ValueError, 'OPERATIONALLY_RETIRED_REQUEST_NO_SEND'):
                    boundary.review(value, {}, now_ms=support.ATTEMPT)
                with self.assertRaisesRegex(ValueError, 'OPERATIONALLY_RETIRED_REQUEST_NO_SEND'):
                    self.fx.port._durable(value)
                with self.assertRaisesRegex(ValueError, 'OPERATIONALLY_RETIRED_REQUEST_NO_SEND'):
                    self.fx.store.claim_transport(value, 'c'*32)
                self.assertFalse(recovery.transport_allowed(self.fx.store.load(),value,support.NOW+2000))
        self.assertFalse(self.fx.http)


if __name__ == '__main__':
    unittest.main()
