"""Focused operational-final gates, using the isolated PostgreSQL fixture."""
from copy import deepcopy
from decimal import Decimal
import unittest

from . import experimental_external_activity as external
from . import experimental_execution_archive as archive
from . import experimental_live_provider as provider
from . import formula_accounting as accounting
from . import test_manual_operational_close as fixture

CI, NOW, RID = fixture.CI, fixture.NOW, fixture.RID


@unittest.skipUnless(CI, 'Requires disposable loopback PostgreSQL; skipped is not verified')
class OperationalGatePostgresTests(unittest.TestCase):
    def setUp(self):
        self.case = fixture.ManualOperationalPostgresTests(methodName='runTest')
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.fx = self.case.fx

    def test_pending_audit_notice_delays_archive_without_reopening_provider_lane(self):
        state = self.case.close()
        state['pending_notifications'] = [dict(kind='PRESERVED_AUDIT_NOTICE')]
        self.fx.store.mutate(lambda value:value.update(state))
        reloaded = self.fx.store.load()
        self.assertFalse(archive._eligible(reloaded,self.case.cid,NOW+1001))
        self.assertIn(self.case.cid,provider._retired_experimental(reloaded,NOW+1001))
        self.assertFalse(external.request_pending(reloaded,reloaded['requests'][RID]))
        self.assertEqual(reloaded['pending_notifications'],state['pending_notifications'])
        self.assertFalse(self.fx.http)

    def test_same_formula_unsettled_history_keeps_balance_but_not_operational_block(self):
        state = self.fx.store.load()
        trade = state['trades'][self.case.cid]
        formula = trade['source']['rule_id']
        # A previously committed balance is independent of this manual case.
        state['formula_accounting'] = dict(version=accounting.VERSION,portfolios={
            formula:dict(balance_usd='47.73',realized_net_usd='7.73',revision=1)})
        trade['entry_risk'] = dict(policy=accounting.VERSION,formula=formula,
            balance_usd='40',revision=0,fraction='0.05',planned_usd='2')
        with self.assertRaisesRegex(ValueError,'FORMULA_NET_SETTLEMENT_PENDING'):
            accounting.risk_for(state,dict(rule_id=formula),now_ms=NOW)
        self.fx.store.mutate(lambda value:value.update(state))
        closed = self.case.close()
        ledger = deepcopy(closed['formula_accounting'])
        risk = accounting.risk_for(closed,dict(rule_id=formula),now_ms=NOW+1001)
        self.assertEqual(Decimal(risk['planned_usd']),Decimal('2.3865'))
        self.assertEqual(closed['formula_accounting'],ledger)
        self.assertNotIn('settlement',closed['trades'][self.case.cid])
        self.assertEqual(closed['requests'][RID]['phase'],'OUTCOME_UNKNOWN')
        self.assertTrue(archive._eligible(closed,self.case.cid,NOW+1001))
        accounting.settle(closed,NOW+1001)
        self.assertEqual(closed['formula_accounting'],ledger)
        self.assertNotIn('settlement',closed['trades'][self.case.cid])
        self.assertFalse(self.fx.http)
