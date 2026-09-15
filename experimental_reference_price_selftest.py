"""Contract tests for formula clocks, bounded lookup and source provenance."""
import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest
from threading import Event
from unittest.mock import patch

import experimental_reference_price as refs

T = datetime(2026, 9, 15, 12, tzinfo=timezone.utc)


def coin():
    return {'sources': {
        'positioning': {'price': 100, 'price_fetched_at': (T+timedelta(minutes=2, seconds=16)).isoformat(),
                        'oi_fetched_at': (T+timedelta(minutes=2, seconds=18)).isoformat(), 'price_source': 'binance_spot'},
        'futures': {'quality': {'candle_close': T.isoformat()}},
        'spot': {'quality': {'candle_close': T.isoformat()}},
        'maxpain_operational_rows': [{'timeframe': str(i),
              'source_observed_at_utc': (T+timedelta(minutes=2, seconds=34+10*i)).isoformat()}
              for i in range(7)]}}


def bundle():
    return {'cycle_id': 'test-scan', 'computed_at_utc': (T+timedelta(minutes=6)).isoformat(),
            'coins': {s:coin() for s in refs.SYMBOLS}}


def bars(symbol, boundaries):
    return {b: {'close': 200 if symbol == 'HYPE' else 101,
                 'open_time_utc': b-timedelta(minutes=1),
                 'close_time_utc': b-timedelta(milliseconds=1)} for b in boundaries}


class ReferenceTests(unittest.TestCase):
    def prepare(self, b=None, fetch=bars):
        return asyncio.run(refs.prepare_reference_prices(b or bundle(), fetch_coin=fetch))

    def test_each_current_clock_and_quote_is_distinct(self):
        b=bundle(); original=deepcopy(b); out=self.prepare(b)
        self.assertEqual(b, original)  # hashed input bundle is never modified
        r=out['BTC']
        self.assertEqual(r['PRICE_OI']['price'], '100')
        self.assertEqual(r['PRICE_OI']['price_time_utc'], (T+timedelta(minutes=2, seconds=16)).isoformat())
        self.assertEqual(r['FUTURES_CVD']['price_time_utc'], T.isoformat())
        self.assertEqual(r['MAX_PAIN']['price_time_utc'], (T+timedelta(minutes=2)).isoformat())
        self.assertEqual(r['MAX_PAIN']['anchor_time_utc'], (T+timedelta(minutes=2,seconds=34)).isoformat())
        chosen=refs.select_reference(r, ('MAX_PAIN','FUTURES_CVD'), symbol='BTC', as_of=T+timedelta(minutes=6))
        self.assertEqual(chosen['component'],'FUTURES_CVD')

    def test_hype_remains_binance_futures_mark(self):
        out = self.prepare()
        self.assertEqual(out['HYPE']['MAX_PAIN']['source'],'BINANCE_HYPE_FUTURES_MARK_1M')
        self.assertEqual(refs.status()['ready_references'], 32)
        self.assertEqual(refs.status()['missing_by_symbol'], {})

    def test_hype_transport_failure_isolated_to_three_quote_components(self):
        from binance_futures_mark_price_path import BinanceFuturesMarkPathError

        def fetch(symbol, boundaries):
            if symbol == 'HYPE':
                raise BinanceFuturesMarkPathError('blocked')
            return bars(symbol, boundaries)

        out = self.prepare(fetch=fetch)
        self.assertEqual(out['HYPE']['PRICE_OI']['status'], 'READY')
        self.assertEqual(
            [component for component in refs.COMPONENTS
             if out['HYPE'][component]['status'] != 'READY'],
            ['MAX_PAIN', 'FUTURES_CVD', 'SPOT_CVD'])
        status = refs.status()
        self.assertEqual(status['ready_references'], 29)
        self.assertEqual(status['missing_references'], 3)
        self.assertEqual(status['missing_by_symbol'], {
            'HYPE': ['MAX_PAIN', 'FUTURES_CVD', 'SPOT_CVD']})
        self.assertEqual(status['error_types_by_symbol'], {
            'HYPE': 'BinanceFuturesMarkPathError'})

    def test_earlier_oi_uses_its_clock_without_later_price(self):
        b=bundle()
        earlier=(T+timedelta(minutes=2,seconds=10)).isoformat()
        b['coins']['BTC']['sources']['positioning']['oi_fetched_at']=earlier
        r=self.prepare(b)['BTC']['PRICE_OI']
        self.assertEqual(r['anchor_time_utc'],earlier)
        self.assertEqual(r['price'],'101')
        self.assertEqual(r['precision'],'CLOSED_1M')
        self.assertEqual(r['price_time_utc'],(T+timedelta(minutes=2)).isoformat())

    def test_coin_requests_coalesce_shared_cvd_clocks(self):
        calls=[]
        def fetch(s,bb):
            calls.append((s,bb)); return bars(s,bb)
        self.prepare(fetch=fetch)
        self.assertEqual(len(calls),8)
        self.assertTrue(all(len(bb)==2 for _,bb in calls))

    def test_missing_minute_never_becomes_current_quote(self):
        r=self.prepare(fetch=lambda s,bb: {})['BTC']
        self.assertEqual(r['PRICE_OI']['status'],'READY')
        self.assertEqual(r['SPOT_CVD']['status'],'UNAVAILABLE')
        self.assertEqual(refs.select_reference(r,('PRICE_OI','SPOT_CVD'))['status'],'UNAVAILABLE')

    def test_unknown_reference_does_not_suppress_plain_explanation(self):
        self.assertIn('לא חושבו',refs.render_reference_levels(None,200,'LONG'))

    def test_quote_deadline_returns_missing_without_late_repricing(self):
        release=Event()
        def slow_fetch(symbol,boundaries):
            release.wait(timeout=1)
            return bars(symbol,boundaries)
        async def run():
            try:
                with patch.object(refs,'TIMEOUT_SECONDS',0.001):
                    return await refs.prepare_reference_prices(bundle(),fetch_coin=slow_fetch)
            finally:
                release.set()
        out=asyncio.run(run())  # waits for released threads before inspecting output
        for row in out.values():
            self.assertEqual(row['PRICE_OI']['status'],'READY')
            self.assertEqual(row['SPOT_CVD']['status'],'UNAVAILABLE')

    def test_wrong_coin_and_future_component_fail_closed(self):
        r=self.prepare()['BTC']
        self.assertEqual(refs.select_reference(r,('SPOT_CVD',),symbol='ZEC')['status'],'UNAVAILABLE')
        self.assertEqual(refs.select_reference(r,('SPOT_CVD','MAX_PAIN'),as_of=T+timedelta(minutes=1))['status'],'UNAVAILABLE')

    def test_missing_any_maxpain_clock_is_not_partial_anchor(self):
        b=bundle(); del b['coins']['BTC']['sources']['maxpain_operational_rows'][2]['source_observed_at_utc']
        self.assertEqual(self.prepare(b)['BTC']['MAX_PAIN']['status'],'UNAVAILABLE')

    def test_future_current_inputs_rejected(self):
        b=bundle(); b['coins']['BTC']['sources']['spot']['quality']['candle_close']=(T+timedelta(hours=1)).isoformat()
        self.assertEqual(self.prepare(b)['BTC']['SPOT_CVD']['status'],'UNAVAILABLE')

    def test_minute_approximation_and_unknown_threshold_are_explicit(self):
        r=self.prepare()['BTC']['MAX_PAIN']
        text=refs.render_reference_levels(r,200,'SHORT')
        self.assertIn('בקירוב לפי נר דקה',text)
        self.assertIn('15:02:00',text)
        self.assertIn('15:02:34',text)
        self.assertIn('103.02',text)
        self.assertIn('98.98',text)
        self.assertIn('לא הוגדר סף תנועה יחיד',refs.render_reference_levels(r,None,'LONG'))

    def test_runtime_only_attaches_this_symbols_frozen_reference(self):
        import research_event_runtime as runtime
        from types import SimpleNamespace
        refs_by_symbol=self.prepare()
        token=runtime.set_watch_context(watch_scan_id='test',experimental_reference_prices_by_symbol=refs_by_symbol)
        try:
            fake=SimpleNamespace(engine_snapshot={},symbol='BTC',direction='LONG',source_side='LONG',event_type='MAGNET_ALERT')
            with patch.object(runtime,'replace',side_effect=lambda event,**kw: kw):
                captured=runtime._with_watch_context(fake)['engine_snapshot']
            self.assertEqual(captured['experimental_price_references'],refs_by_symbol['BTC'])
            self.assertNotIn('experimental_reference_prices_by_symbol',captured)
            refs_by_symbol['BTC']['PRICE_OI']['price']='999'
            self.assertEqual(captured['experimental_price_references']['PRICE_OI']['price'],'100')
        finally:
            runtime.reset_watch_context(token)


if __name__=='__main__':
    unittest.main()
