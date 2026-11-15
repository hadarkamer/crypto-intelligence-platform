"""Explicit HYPE 40/5 funding from existing cash; no live I/O or account edits."""
from copy import deepcopy
from decimal import Decimal
import unittest
import threading
from unittest.mock import patch

from . import formula_accounting as accounting
from .experimental_live_state import initial
from .test_experimental_live_state import LIVE_ROUTES
from .test_formula_accounting import completed, fund
from approved_alert_fixtures import BASE

RULE=accounting.HYPE_FORMULA


def capital_context(state, *, amount='300', both=False, at=BASE):
    accounts=list(state['routes'].values()) if both else [state['routes']['short_account']]
    return dict(capital_observations={account:dict(status='NATIVE_PERP_CASH_VERIFIED',
        at_ms=at,mode='disabled',mode_after='disabled',open_orders=[],account_state=dict(
            marginSummary=dict(accountValue=amount,totalRawUsd=amount,totalMarginUsed='0',totalNtlPos='0'),
            withdrawable=amount,assetPositions=[])) for account in accounts})


class HypeCapitalTests(unittest.TestCase):
    def setUp(self):
        self.state=initial(LIVE_ROUTES,BASE-60000)
        p=patch('http.client.HTTPSConnection',side_effect=AssertionError('NO_LIVE_IO'))
        p.start();self.addCleanup(p.stop)

    def risk(self,state=None,context=None):
        state=self.state if state is None else state
        return accounting.risk_for(state,dict(rule_id=RULE),context=context,now_ms=BASE)

    def test_missing_capital_does_not_inherit_default_or_old_hype_history(self):
        old=accounting.risk_for(self.state,dict(rule_id='HYPE_ROW71205_SHORT'))
        original=deepcopy(self.state['formula_accounting']['portfolios'])
        with self.assertRaisesRegex(ValueError,'EXISTING_CAPITAL_NOT_VERIFIED'):
            self.risk()
        self.assertEqual(self.state['formula_accounting']['portfolios'],original)
        self.assertEqual(old['balance_usd'],'40')

    def test_exact_40_allocation_without_changing_other_balances_or_history(self):
        old=completed(self.state,formula='HYPE_ROW71205_SHORT',gross='10',fees='0')
        fund(self.state,'');accounting.settle(self.state,BASE+5000)
        before=deepcopy(self.state['trades']);portfolios=deepcopy(self.state['formula_accounting']['portfolios'])
        value=self.risk(context=capital_context(self.state,amount='300'))
        self.assertEqual(Decimal(value['planned_usd']),2)
        for formula,portfolio in portfolios.items():
            self.assertEqual(self.state['formula_accounting']['portfolios'][formula],portfolio)
        self.assertEqual(self.state['trades'],before)
        receipt=self.state['formula_accounting']['allocations'][RULE]
        self.assertEqual(receipt['scope'],'GLOBAL_EXISTING_CAPITAL')
        self.assertEqual(receipt['unassigned_before_usd'],'50')
        self.assertEqual(receipt['unassigned_after_usd'],'10')
        self.assertEqual(receipt['existing_claims']['HYPE_ROW71205_SHORT']['usd'],'50')
        self.assertEqual(receipt['funding_transfers_sent'],0)

    def test_actual_settlement_40_50_30_risks_and_duplicate_costs_once(self):
        self.risk(context=capital_context(self.state))
        first=completed(self.state,formula=RULE,gross='10.5',fees='0.5');fund(self.state,'')
        accounting.settle(self.state,BASE+5000)
        self.assertEqual(Decimal(self.risk()['planned_usd']),Decimal('2.50'))
        receipt=deepcopy(self.state['formula_accounting']['allocations'][RULE])
        second=completed(self.state,formula=RULE,cid='b'*64,gross='-19.5',fees='0.5');fund(self.state,'',cid='b'*64)
        accounting.settle(self.state,BASE+6000)
        for _ in range(3):accounting.settle(self.state,BASE+7000)
        restored=deepcopy(self.state)
        self.assertEqual(Decimal(self.risk(restored)['planned_usd']),Decimal('1.50'))
        self.assertEqual(restored['formula_accounting']['portfolios'][RULE]['revision'],2)
        self.assertEqual(restored['formula_accounting']['allocations'][RULE],receipt)
        self.assertEqual(first['settlement']['fees_usd'],'0.5')
        self.assertEqual(second['settlement']['fees_usd'],'0.5')

    def test_restart_no_reset_or_new_observation_required_after_valid_receipt(self):
        self.risk(context=capital_context(self.state))
        before=deepcopy(self.state['formula_accounting'])
        for _ in range(3):self.risk(context={})
        self.assertEqual(self.state['formula_accounting'],before)

    def test_partial_observed_pool_and_no_duplicate_account_routes(self):
        # One fresh account may prove the global pool; no fictitious second cash.
        self.risk(context=capital_context(self.state,amount='280'))
        receipt=self.state['formula_accounting']['allocations'][RULE]
        self.assertEqual(len(receipt['source_observations']),1)
        other=initial(LIVE_ROUTES,BASE-60000)
        self.risk(other,capital_context(other,amount='140',both=True))
        self.assertEqual(len(other['formula_accounting']['allocations'][RULE]['source_observations']),2)
        bad=initial(LIVE_ROUTES,BASE-60000);bad['routes']['long_account']=bad['routes']['short_account']
        with self.assertRaisesRegex(ValueError,'ROUTE_INTEGRITY'):
            self.risk(bad,capital_context(bad,amount='140',both=True))

    def test_precise_shortfall_keeps_absent_prior_entitlements_reserved(self):
        with self.assertRaises(accounting.AccountingError) as caught:
            self.risk(context=capital_context(self.state,amount='279'))
        diagnostic=caught.exception.capital_diagnostic
        self.assertEqual(diagnostic['existing_claims_usd'],'240')
        self.assertEqual(diagnostic['unassigned_lower_bound_usd'],'39')
        self.assertEqual(diagnostic['shortfall_to_verified_lower_bound_usd'],'1')
        self.assertNotIn(RULE,self.state['formula_accounting']['portfolios'])

    def test_missing_stale_future_unified_or_changed_mode_never_counts_as_zero_data(self):
        for change in ('missing','stale','future','unified','changed'):
            state=initial(LIVE_ROUTES,BASE-60000);ctx=capital_context(state)
            sample=next(iter(ctx['capital_observations'].values()))
            if change=='missing':sample['status']='UNAVAILABLE'
            elif change=='stale':sample['at_ms']=BASE-15001
            elif change=='future':sample['at_ms']=BASE+1
            elif change=='unified':sample['mode']=sample['mode_after']='unifiedAccount'
            else:sample['mode_after']='unifiedAccount'
            with self.subTest(change=change),self.assertRaisesRegex(ValueError,'NOT_VERIFIED'):
                self.risk(state,ctx)
            self.assertNotIn(RULE,state['formula_accounting']['portfolios'])

    def test_withdrawable_and_raw_cash_bound_and_pending_entry_counted_once(self):
        ctx=capital_context(self.state,amount='1000');sample=next(iter(ctx['capital_observations'].values()))
        sample['account_state']['withdrawable']='300'
        account=self.state['routes']['short_account'];cloid='0x'+'a'*32
        order=dict(oid=42,cloid=cloid,reduceOnly=False,sz='1',limitPx='20')
        sample['open_orders']=[order]
        self.state['requests']['pending']=dict(phase='OUTCOME_UNKNOWN',proposal=dict(operation='ENTRY',
            account=account,action=dict(orders=[dict(c=cloid,r=False,p='20',s='1')])))
        self.risk(context=ctx)
        receipt=self.state['formula_accounting']['allocations'][RULE]
        self.assertEqual(len(receipt['outstanding_entry_obligations']),1)
        self.assertEqual(receipt['unassigned_before_usd'],'40')
        self.assertEqual(receipt['unassigned_after_usd'],'0')
        self.assertEqual(self.state['requests']['pending']['phase'],'OUTCOME_UNKNOWN')

    def test_negative_known_cash_cannot_disappear_from_global_pool(self):
        context=capital_context(self.state,amount='1000',both=True)
        context['capital_observations'][self.state['routes']['long_account']]['account_state']['marginSummary']['totalRawUsd']='-1'
        with self.assertRaisesRegex(ValueError,'NEGATIVE_CASH_UNRESOLVED'):
            self.risk(context=context)
        self.assertNotIn(RULE,self.state['formula_accounting']['portfolios'])

    def test_bad_cash_or_unbounded_order_is_local_funding_refusal(self):
        context=capital_context(self.state);sample=next(iter(context['capital_observations'].values()))
        sample['account_state']['marginSummary'].pop('totalRawUsd')
        with self.assertRaisesRegex(ValueError,'NOT_VERIFIED'):
            self.risk(context=context)
        context=capital_context(self.state);sample=next(iter(context['capital_observations'].values()))
        sample['open_orders']=[dict(oid=1,reduceOnly=False)]
        with self.assertRaisesRegex(ValueError,'OBLIGATIONS_UNRESOLVED'):
            self.risk(context=context)

    def test_seed_without_receipt_tampering_and_route_change_fail(self):
        accounting.risk_for(self.state,dict(rule_id='OTHER'))
        self.state['formula_accounting']['portfolios'][RULE]=dict(balance_usd='40',realized_net_usd='0',revision=0)
        with self.assertRaisesRegex(ValueError,'RECEIPT_REQUIRED'):self.risk()
        self.state['formula_accounting']['portfolios'].pop(RULE)
        self.risk(context=capital_context(self.state,amount='400'))
        for field,value in (('initial_usd','80'),('unassigned_after_usd','999')):
            state=deepcopy(self.state);state['formula_accounting']['allocations'][RULE][field]=value
            with self.assertRaisesRegex(ValueError,'RECEIPT_REQUIRED'):self.risk(state)
        state=deepcopy(self.state);state['routes']['long_account']='0x'+'9'*40
        with self.assertRaisesRegex(ValueError,'RECEIPT_REQUIRED'):self.risk(state)


class CapitalInventoryTests(unittest.TestCase):
    def provider(self, *, mode='disabled', after=None, failure=False, default_spot=None):
        from .experimental_live_provider import LiveEvidenceProvider
        from . import two_account_execution as roles
        state=initial(LIVE_ROUTES,BASE-60000)
        account=roles.PHANTOM if mode=='default' else state['routes']['short_account']
        value=next(iter(capital_context(state)['capital_observations'].values()))['account_state']
        calls=[]
        class ExistingInfo:
            def read(self,kind,**kwargs):
                calls.append((kind,kwargs))
                if kind=='userAbstraction':
                    if failure:raise OSError('UNAVAILABLE')
                    n=sum(k=='userAbstraction' for k,_ in calls)
                    return after if n>1 and after is not None else mode
                if kind=='spotClearinghouseState':return default_spot or dict(balances=[])
                raise AssertionError(kind)
        class ExistingPublic:
            def read(self,kind,user):
                calls.append((kind,user))
                return [] if kind=='frontendOpenOrders' else deepcopy(value)
        provider=object.__new__(LiveEvidenceProvider);provider._thread=threading.local()
        provider.now=lambda:BASE;provider.entry_info=ExistingInfo();provider.public=ExistingPublic()
        return provider,account,calls

    def test_original_inventory_bracketed_by_two_mode_reads_only_for_first_funding(self):
        provider,account,calls=self.provider()
        inventory=provider._inventory(account,capital=True)
        self.assertEqual([x[0] for x in calls],['userAbstraction','frontendOpenOrders','clearinghouseState','userAbstraction'])
        self.assertEqual(inventory['capital_observation']['at_ms'],BASE)
        calls.clear();provider._inventory(account,capital=False)
        self.assertEqual([x[0] for x in calls],['frontendOpenOrders','clearinghouseState'])

    def test_default_is_not_renamed_and_requires_native_scope_empty_spot(self):
        provider,account,calls=self.provider(mode='default')
        value=provider._inventory(account,capital=True)
        self.assertEqual(value['capital_observation']['mode'],'default')
        self.assertEqual([x[0] for x in calls],['userAbstraction','frontendOpenOrders','clearinghouseState','spotClearinghouseState','userAbstraction'])

    def test_failed_unified_changed_modes_keep_protection_inventory(self):
        for kwargs in (dict(mode='unifiedAccount'),dict(mode='portfolioMargin'),dict(after='unifiedAccount'),dict(failure=True)):
            provider,account,calls=self.provider(**kwargs)
            value=provider._inventory(account,capital=True)
            self.assertIn('capital_error',value)
            self.assertNotIn('capital_observation',value)
            self.assertEqual(value['orders'],[])
            self.assertEqual(value['positions']['assetPositions'],[])


if __name__=='__main__':unittest.main()
