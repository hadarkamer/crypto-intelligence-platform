"""Manual partial-close behavior and reporting through the real local pipeline."""
from copy import deepcopy
from decimal import Decimal
import unittest

from experimental_execution_fixtures import r2732_message, hype_row71205_message
from . import experimental_external_activity as external
from . import experimental_execution_cards as cards
from . import experimental_execution_reporting as reporting
from . import test_experimental_live_service as full_connection
from .test_experimental_execution_runtime import T
from . import test_experimental_live_provider as provider_tests


class PartialCloseTests(unittest.TestCase):
    def setUp(self):
        self.fx=full_connection.FullConnectionTests(methodName='runTest')
        self.fx.setUp();self.addCleanup(self.fx.doCleanups)
        self.msg=r2732_message(entry=2.3,decision_ms=T-60000)
        self.fx.seed(self.msg)
        for _ in range(4):self.fx.service.tick()

    def manual_partial_close(self):
        state=self.fx.store.load();trade=state['trades'][self.msg['occurrence_id']]
        account=trade['account'];symbol=trade['symbol']
        half=Decimal(trade['quantity'])/2
        self.fx.oracle.t+=1;stamp=self.fx.oracle.t
        read=self.fx.raw.read
        def observed(kind,account_arg=None,**kw):
            result=read(kind,account_arg,**kw)
            if (account_arg or kw.get('user'))!=account:return result
            if kind=='userFillsByTime' and kw['start']<=stamp<=kw['end']:
                result.append(dict(coin=symbol,oid=998877,tid=998877,sz=str(half),px='2.2',
                    side='B',time=stamp,fee='0.001',feeToken='USDC'))
            if kind=='clearinghouseState':
                row=next(r for r in result['assetPositions'] if r['position']['coin']==symbol)
                row['position']['szi']=str(Decimal(row['position']['szi'])+half)
            return result
        self.fx.raw.read=observed
        self.fx.service.tick()
        return trade,half

    def test_manual_partial_close_keeps_original_exchange_orders_and_separate_actual_quantity(self):
        sent=len(self.fx.http);old,half=self.manual_partial_close()
        for _ in range(3):
            self.fx.oracle.t+=1000;self.fx.service.tick()
        state=self.fx.store.load();trade=state['trades'][old['cid']]
        self.assertEqual(len(self.fx.http),sent)
        self.assertTrue(external.managed(state,old['account'],old['symbol']))
        self.assertEqual(trade['orders'],old['orders'])
        self.assertEqual(trade['entry_fills'],old['entry_fills'])
        self.assertEqual(trade['exit_fills'],old['exit_fills'])
        card=cards.cards_from_state(state,domain='testnet')[0]
        self.assertEqual(Decimal(card['summary']['actual_position_quantity']),-half)
        self.assertIsNone(card['summary']['remaining_quantity'])
        self.assertEqual(card['summary']['historical_bot_remaining_quantity'],old['quantity'])
        self.assertIsNone(card['summary']['net_pnl'])
        self.assertEqual(card['summary']['net_pnl_status'],'EXTERNAL_ACTIVITY_UNALLOCATED')
        self.assertEqual(card['manual_management']['status'],external.HUMAN)
        facts=state['external_activity']['accounts'][old['account']]['fills']
        manual=next(f for f in facts if f['oid']=='998877')
        self.assertEqual(manual['quantity'],str(half));self.assertEqual(manual['price'],'2.2')
        self.assertEqual(manual['origin'],'EXTERNAL')
        self.assertEqual(sum(e['kind']=='EXTERNAL_FILL_OBSERVED' and e['oid']=='998877'
            for e in state['external_activity']['events']),1)

    def test_human_managed_partial_position_does_not_stop_different_coin_entry(self):
        old,half=self.manual_partial_close()
        decision=(self.fx.oracle.now()//1800000+1)*1800000
        self.fx.oracle.t=decision+70000
        msg=hype_row71205_message(entry=100,decision_ms=decision)
        self.fx.seed(msg)
        result=self.fx.service.tick()
        self.assertEqual(result.get('operation'),'ENTRY',result)
        state=self.fx.store.load()
        self.assertTrue(external.managed(state,old['account'],old['symbol']))
        self.assertIn(msg['occurrence_id'],state['trades'])


class QuietObservationTests(unittest.TestCase):
    def test_deferred_idle_history_preserves_start_and_catches_completed_manual_roundtrip(self):
        fixture=provider_tests.ProviderTests(methodName='runTest');fixture.setUp();self.addCleanup(fixture.doCleanups)
        account=fixture.state['routes']['long_account'];start=fixture.exchange.t
        first=fixture.provider.collect(fixture.state,entries_enabled=False)
        self.assertFalse(any(c[0]=='userFillsByTime' for c in fixture.raw.calls))
        external.apply(fixture.state,first['external_observation'])
        initial=fixture.state['external_activity']['accounts'][account]['history_started_at_ms']
        raw=fixture.raw.read
        rows=[dict(coin='SOL',oid=800+n,tid=900+n,sz='2',px='100',side=side,
                   time=start+1000+n,fee='0',feeToken='USDC') for n,side in enumerate(('B','A'))]
        def read(kind,account_arg=None,**kw):
            result=raw(kind,account_arg,**kw)
            if kind=='userFillsByTime' and (account_arg or kw.get('user'))==account:
                self.assertEqual(kw['start'],initial)
                return result+[deepcopy(r) for r in rows if kw['start']<=r['time']<=kw['end']]
            return result
        fixture.raw.read=read;fixture.exchange.t+=65000
        next_pass=fixture.provider.collect(fixture.state,entries_enabled=False)
        external.apply(fixture.state,next_pass['external_observation'])
        observed=fixture.state['external_activity']['accounts'][account]
        self.assertTrue(observed['history_complete'])
        self.assertEqual({f['oid'] for f in observed['fills']},{'800','801'})
        self.assertEqual(observed['positions'],{})
        self.assertEqual(fixture.state['requests'],{})
        self.assertEqual(len([e for e in fixture.state['external_activity']['events'] if e['kind']=='EXTERNAL_FILL_OBSERVED']),2)


if __name__=='__main__':unittest.main()
