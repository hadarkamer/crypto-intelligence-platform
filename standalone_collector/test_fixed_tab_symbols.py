"""Offline regression for the two fixed slots on the existing QueueDrain."""
import asyncio
from contextlib import nullcontext
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import modern_map_cycle as cycle
from model1_execution import StageFailure
from modern_map_basic_numbers import BASIC_RANGES


class FixedTabTests(unittest.IsolatedAsyncioTestCase):
    async def test_pair_order_independence_and_failure(self):
        targets = [(s, m, t) for s in ('BTC', 'ETH') for m in (1, 2, 3) for t in BASIC_RANGES]
        ledger = {target: dict(id=f'job-{i}', symbol=target[0], heatmap_model=target[1], timeframe=target[2])
                  for i, target in enumerate(reversed(targets))}
        first = ledger.pop(('ETH', 1, '12H'))
        delayed = ledger.pop(('BTC', 1, '12H'))
        observed = [[], []]
        published = {}
        btc_reading = asyncio.Event()
        eth_published_four = asyncio.Event()

        def claim(conn, guard, *, symbols, model, timeframe):
            self.assertEqual(len(symbols), 1)
            return ledger.pop((symbols[0], model, timeframe), None)

        async def process(index, row):
            identity = (row['symbol'], row['heatmap_model'], row['timeframe'])
            observed[index].append(identity)
            if identity == ('ETH', 1, '12H'):
                ledger[('BTC', 1, '12H')] = delayed
                await btc_reading.wait()
            if identity == ('BTC', 1, '12H'):
                btc_reading.set()
                await eth_published_four.wait()
            if identity == ('ETH', 1, '1W'):
                raise StageFailure('source_map_unavailable')
            return identity, b''

        async def finish(row, result, image, error=None):
            self.assertNotIn(row['id'], published)
            published[row['id']] = (row['symbol'], error)
            if row['symbol'] == 'ETH' and row['heatmap_model'] == 1 and row['timeframe'] == '1W':
                self.assertNotIn(delayed['id'], published)
                eth_published_four.set()
            return True

        env = {'COINGLASS_CYCLE_FIXED_SYMBOLS':'BTC,ETH','COINGLASS_VERIFIED_SYMBOLS':'BTC,ETH'}
        with patch.dict(os.environ, env), patch.object(cycle.ManagedSession,'from_environment',
                return_value=SimpleNamespace(connection_guard=nullcontext())), patch.object(cycle,'next_claim',claim):
            drain = cycle.QueueDrain(None, None, first, None, 2, cycle.fixed_tab_symbols(2))
            drain.process, drain.finish = process, finish
            await asyncio.wait_for(drain.run(), 8)
        self.assertEqual(observed, [targets[:12], targets[12:]])
        self.assertEqual(len(published), 24)
        self.assertEqual([error for _, error in published.values() if error], ['source_map_unavailable'])
        self.assertFalse(ledger)

    def test_invalid_binding_does_not_overrule_symbol_scope_or_width(self):
        for symbols in ('ETH,ETH', 'BTC,DOGE', 'BTC'):
            with patch.dict(os.environ, {'COINGLASS_CYCLE_FIXED_SYMBOLS': symbols,
                                        'COINGLASS_VERIFIED_SYMBOLS':'BTC,ETH'}), self.assertRaises(ValueError):
                cycle.fixed_tab_symbols(2)
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(cycle.fixed_tab_symbols(1), ())


if __name__ == '__main__':
    unittest.main()
