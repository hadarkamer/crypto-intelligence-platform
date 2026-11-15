"""Authorized HYPE IOC/holding with real funded-receipt admission, isolated I/O."""
from copy import deepcopy
from decimal import Decimal
import os
import unittest
from unittest.mock import patch

import approved_alert_contract as contract
from approved_alert_fixtures import selected_formula_alert
from . import formula_accounting as accounting, hype_watch_execution as hype
from . import experimental_execution_dispatch as boundary
from .experimental_live_dispatch import DefinitelyNotSubmitted
from . import test_experimental_live_runtime as support
from .test_experimental_execution_runtime import META

T = support.T


def hype_alert(*, reference=T, approved=None, cycle='new-hype'):
    approved = reference + 5000 if approved is None else approved
    msg = selected_formula_alert('sol_sff1361', reference_ms=reference,
        approved_ms=approved, cycle=cycle)
    msg.update(family='hype_watch_liq4050', rule_id=hype.RULE, symbol='HYPE',
        entry='100', stop='102', take_profit='98', original_target=None,
        expires_at=contract.iso_ms(reference + 60000))
    msg['policy'] = dict(name=hype.RULE + '_V1', entry_kind='NEXT_FULL_MINUTE_IOC',
        source_price='HYPERLIQUID_HYPE_PERPETUAL_TRADE_1M',
        entry_expires_at=contract.iso_ms(reference + 60000), holding_time_limit_ms=86400000)
    msg['proof'].update(timeframe='24h', source_target=None)
    msg['occurrence_id'] = contract.occurrence_id(msg)
    return contract.validate(msg)


class HypeWatchContract:
    real_pg = False
    setUp = support.RuntimeContract.setUp
    make_worker = support.RuntimeContract.make_worker
    cycle = support.RuntimeContract.cycle
    restart = support.RuntimeContract.restart

    def authorize_fixture(self, *, risk=True):
        """Provide controlled exchange evidence; never patch allocation policy."""
        if risk:
            from .test_hype_formula_capital import capital_context
            collect=self.provider.collect
            def observed(state,**kwargs):
                value=collect(state,**kwargs)
                value.update(capital_context(state,at=self.venue.now()))
                return value
            self.provider.collect=observed

    def start_hype(self, *, risk=True):
        self.authorize_fixture(risk=risk)
        msg = hype_alert()
        self.worker.receive([msg]); result = self.cycle()
        return msg, result

    def protect(self):
        for _ in range(3): self.cycle(False)

    def test_active_roster_without_capital_evidence_cannot_create_portfolio(self):
        msg = hype_alert(); self.worker.receive([msg]); self.cycle()
        self.assertFalse(self.venue.requests)
        self.assertNotIn(hype.RULE, self.store.load().get('formula_accounting', {}).get('portfolios', {}))

    def test_active_roster_alone_cannot_fund_capital_or_fall_back_to_two_dollars(self):
        msg, result = self.start_hype(risk=False)
        self.assertFalse(self.venue.requests)
        saved = self.store.load()
        self.assertEqual(saved['entry_decisions'][msg['occurrence_id']]['reason'],
                         'FORMULA_EXISTING_CAPITAL_NOT_VERIFIED')
        self.assertNotIn(hype.RULE, saved.get('formula_accounting', {}).get('portfolios', {}))
        with self.assertRaisesRegex(accounting.AccountingError, 'EXISTING_CAPITAL_NOT_VERIFIED'):
            accounting.risk_for(saved, msg)

    def test_entry_is_one_ioc_at_frozen_price_then_actual_filled_protection(self):
        msg, result = self.start_hype()
        self.assertEqual(result['operation'], 'ENTRY')
        req = self.venue.requests[0]
        order = req['proposal']['action']['orders'][0]
        self.assertEqual(order['t'], {'limit': {'tif': 'Ioc'}})
        self.assertEqual(order['p'], msg['entry'])
        boundary._terms(req, msg, META, formula_risk=req['proposal']['entry_risk'])
        self.protect(); self.worker.receive([msg, msg]); self.restart(); self.cycle()
        self.assertEqual([r['proposal']['operation'] for r in self.venue.requests],
                         ['ENTRY', 'CREATE_EXIT', 'CREATE_EXIT'])
        trade = self.store.load()['trades'][msg['occurrence_id']]
        self.assertEqual(trade['prices'], dict(entry='100', stop='102', take_profit='98'))
        first = min(f['at_ms'] for f in trade['entry_fills'].values())
        self.assertGreater(first, T)
        self.assertEqual(hype.holding_deadline(msg, trade['entry_fills'].values()), first + 86400000)

    def test_original_entry_minute_is_not_extended_by_delivery_or_restart(self):
        self.authorize_fixture(); msg = hype_alert()
        self.worker.receive([msg]); self.venue.t = T + 60000
        self.restart(); self.cycle()
        self.assertFalse(self.venue.requests)
        current = self.store.load()['sources'][msg['occurrence_id']]
        with self.assertRaisesRegex(boundary.BoundaryError, 'ORIGINAL_SOURCE_WINDOW_EXPIRED|HYPE_ORIGINAL_ENTRY_MINUTE_EXPIRED'):
            boundary._source(msg, current, self.venue.t, entry=True)

    def test_zero_fill_ioc_is_final_without_late_gtc_or_retry(self):
        self.venue.instant_fraction = Decimal(0)
        msg, _ = self.start_hype()
        for _ in range(3): self.cycle(False)
        self.restart(); self.cycle()
        self.assertEqual(len(self.venue.requests), 1)
        trade = self.store.load()['trades'][msg['occurrence_id']]
        self.assertEqual(trade['phase'], 'CANCELED_WITHOUT_FILL')
        self.assertIsNone(hype.holding_deadline(msg, trade['entry_fills'].values()))

    def test_partial_ioc_protects_only_observed_fill(self):
        self.venue.instant_fraction = Decimal('.5')
        msg, _ = self.start_hype(); self.protect()
        trade = self.store.load()['trades'][msg['occurrence_id']]
        quantity = sum(Decimal(f['quantity']) for f in trade['entry_fills'].values())
        self.assertLess(quantity, Decimal(trade['quantity']))
        for leg in ('STOP', 'TAKE_PROFIT'):
            self.assertEqual(Decimal(self.venue.orders[self.venue.oid(leg)]['view']['wire_order']['s']), quantity)

    def test_time_exit_uses_actual_fill_not_research_clock_and_is_not_repeated(self):
        from .experimental_live_provider import _fixed_exits_stable
        msg, _ = self.start_hype(); self.protect()
        trade = self.store.load()['trades'][msg['occurrence_id']]
        deadline = hype.holding_deadline(msg, trade['entry_fills'].values())
        self.assertTrue(_fixed_exits_stable(trade, deadline - 1))
        self.assertFalse(_fixed_exits_stable(trade, deadline))
        self.venue.t = T + 86400000
        self.cycle(False)
        self.assertFalse(any(r['proposal']['operation'] == 'TIME_EXIT' for r in self.venue.requests))
        self.venue.t = deadline; result = self.cycle(False)
        self.assertEqual(result['operation'], 'TIME_EXIT')
        req = self.venue.requests[-1]
        boundary._terms(req, msg, META)
        self.assertTrue(req['proposal']['action']['orders'][0]['r'])
        from .test_experimental_live_provider import RawTestnetFixture
        from . import filled_quantity_dispatch as wire
        raw = RawTestnetFixture(self.venue, self.store.load, None).status(self.venue.oid('STOP'))
        wire.identity(raw, req, self.venue.t)
        for key, value in (('source_rule_id', 'HYPE_ROW71205_SHORT'),
                           ('symbol', 'DOGE'), ('operation', 'PARTIAL_TAKE'),
                           ('leg', 'TAKE_PROFIT')):
            altered = deepcopy(req); altered['proposal'][key] = value
            with self.assertRaisesRegex(wire.DispatchError, 'EXACT_APPROVED_DOGE_IMMEDIATE_INTENT_REQUIRED'):
                wire.identity(raw, altered, self.venue.t)
        self.restart()
        for _ in range(5): self.cycle(False)
        self.assertEqual(sum(r['proposal']['operation'] == 'TIME_EXIT' for r in self.venue.requests), 1)
        self.assertEqual(self.store.load()['trades'][msg['occurrence_id']]['phase'], 'CLOSED')

    def test_take_profit_before_time_exit_prevents_second_close(self):
        msg, _ = self.start_hype(); self.protect()
        trade = self.store.load()['trades'][msg['occurrence_id']]
        self.venue.t = hype.holding_deadline(msg, trade['entry_fills'].values()) - 1
        self.venue.fill(self.venue.oid('TAKE_PROFIT'), trade['quantity'])
        for _ in range(4): self.cycle(False)
        self.assertFalse(any(r['proposal']['operation'] == 'TIME_EXIT' for r in self.venue.requests))
        self.assertEqual(self.store.load()['trades'][msg['occurrence_id']]['phase'], 'CLOSED')

    def test_unknown_time_exit_reply_never_sends_another_close(self):
        msg, _ = self.start_hype(); self.protect()
        trade = self.store.load()['trades'][msg['occurrence_id']]
        self.venue.t = hype.holding_deadline(msg, trade['entry_fills'].values())
        self.venue.lose_reply = True; self.cycle(False)
        self.venue.omit_order = self.venue.oid('STOP')
        self.restart()
        for _ in range(3): self.cycle(False)
        self.assertEqual(sum(r['proposal']['operation'] == 'TIME_EXIT' for r in self.venue.requests), 1)
        self.venue.omit_order = None
        for _ in range(4): self.cycle(False)
        self.assertEqual(sum(r['proposal']['operation'] == 'TIME_EXIT' for r in self.venue.requests), 1)

    def test_capacity_rejection_is_permanent_for_first_event(self):
        first, _ = self.start_hype(); self.protect()
        second = hype_alert(cycle='second-wave')
        receipt = self.worker.receive([second])
        self.assertEqual(receipt['receipts'][0]['entry_permission'], 'RETIRED')
        self.assertEqual(self.store.load()['sources'][second['occurrence_id']]['terminal_reason'],
                         'HYPE_FIRST_EVENT_CAPACITY_REJECTED')
        trade = self.store.load()['trades'][first['occurrence_id']]
        self.venue.fill(self.venue.oid('TAKE_PROFIT'), trade['quantity'])
        for _ in range(4): self.cycle()
        self.restart(); self.worker.receive([second]); self.cycle()
        self.assertEqual(sum(r['proposal']['operation'] == 'ENTRY' for r in self.venue.requests), 1)

    def test_verified_external_capacity_rejection_cannot_retry_after_lane_clears(self):
        from .experimental_execution_runtime import _lane
        from .experimental_external_activity import OCCUPIED
        self.authorize_fixture(); msg = hype_alert(); self.worker.receive([msg])
        account = self.store.load()['routes']['short_account']
        collect = self.provider.collect
        def busy(*args, **kwargs):
            context = collect(*args, **kwargs)
            context['blocked_lanes'] = {_lane(account, 'HYPE'): OCCUPIED}
            return context
        with patch.object(self.provider, 'collect', side_effect=busy):
            self.cycle()
        source = self.store.load()['sources'][msg['occurrence_id']]
        self.assertEqual(source['terminal_reason'], 'HYPE_FIRST_EVENT_CAPACITY_REJECTED')
        self.restart(); self.cycle()
        self.assertFalse(self.venue.requests)


class HypeWatchTests(HypeWatchContract, unittest.TestCase):
    pass


@unittest.skipUnless(os.environ.get('HL_JOURNAL_CI_URL'), 'Disposable PostgreSQL required')
class HypeWatchPostgresTests(HypeWatchContract, unittest.TestCase):
    real_pg = True


class HypeWireTests(unittest.TestCase):
    """The actual sender with only signing/key/HTTPS boundaries controlled."""
    def setUp(self):
        from .test_experimental_live_dispatch import LiveDispatchTests
        for name in ('build', 'send'):
            setattr(self, name, getattr(LiveDispatchTests, name).__get__(self))
        LiveDispatchTests.setUp(self)
        source = hype_alert()
        self.t = T + 59990
        risk = dict(policy=accounting.VERSION, formula=hype.RULE, balance_usd='40',
            revision=0, fraction='0.05', planned_usd='2.00')
        p = self.request['proposal']
        p.update(card_id=source['occurrence_id'], symbol='HYPE', quantity='1',
            source_at=source['source_at'], source_expires_at=source['expires_at'],
            observed_at_ms=self.t, entry_risk=deepcopy(risk),
            source_rule_id=hype.RULE, source_version=source['version'])
        p['action']['orders'][0].update(p='100', s='1', t={'limit': {'tif': 'Ioc'}})
        self.request.update(prepared_at_ms=self.t, attempt_at_ms=self.t, nonce=self.t)
        self.persisted = deepcopy(self.request)
        self.context.update(source=source,
            current_source=dict(source=source, plan_digest=contract.plan_digest(source),
                entry_permission='WAITING', cancellation=None),
            metadata={'universe': [dict(name='HYPE', szDecimals=2, maxLeverage=10)]},
            entry_risk=deepcopy(risk),
            market=dict(environment='mainnet', symbol='HYPE', at_ms=self.t, mark_price='100'))
        self.context['entry_account']['at_ms'] = self.t
        self.context['safety'].update(at_ms=self.t, supervisor_at_ms=self.t, not_before_ms=T - 1)
        self.release.update(not_before_ms=T - 1, entry_expires_at_ms=T + 86400000)
        from . import two_account_execution as roles
        from .experimental_live_state import initial
        from .test_hype_formula_capital import capital_context
        funded=initial({role:roles.route_for(self.env,role) for role in roles.ROLES},T-1)
        funded_risk=accounting.risk_for(funded,source,
            context=capital_context(funded,at=self.t),now_ms=self.t)
        self.assertEqual(funded_risk,risk)
        self.context['formula_allocation']=deepcopy(funded['formula_accounting']['allocations'][hype.RULE])

    def test_signed_exchange_expiry_cannot_extend_the_original_entry_minute(self):
        self.send()
        self.assertEqual(len(self.http), 1)
        self.assertEqual(self.http[0][4]['expiresAfter'], T + 60000)
        self.assertEqual(self.signed[0][4], T + 60000)
        self.assertEqual(self.http[0][4]['action']['orders'][0]['t'], {'limit': {'tif': 'Ioc'}})

    def test_entry_minute_expiring_while_signing_never_reaches_http(self):
        self.sign_hook = lambda action: setattr(self, 't', T + 60000)
        with self.assertRaisesRegex(DefinitelyNotSubmitted, 'ORIGINAL_SOURCE_WINDOW_EXPIRED'):
            self.send()
        self.assertEqual(len(self.signed), 1)
        self.assertEqual(self.http, [])

    def test_missing_funded_receipt_never_reaches_signing_or_http(self):
        self.context.pop('formula_allocation')
        with self.assertRaisesRegex(DefinitelyNotSubmitted, 'FORMULA_EXISTING_CAPITAL_RECEIPT_REQUIRED'):
            self.send()
        self.assertEqual(self.signed, [])
        self.assertEqual(self.http, [])
