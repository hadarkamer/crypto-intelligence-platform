"""Offline regressions for the October 2026 Max Pain layout change."""
import asyncio
from copy import deepcopy
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import coinglass_dom_reader as reader


# Cell boundaries and nested values match the site's ranking table. Values are synthetic.
def table(count=30, short='81,558.27'):
    headers = ['', '#↑', 'Symbol', 'Price · 24h', 'Long pain below',
               'Short pain above', '24h touch ↓ / ↑', 'Pull', 'Confluence']
    def cell(text='', primary=None, amount=''):
        return {'text': text, 'primary': text if primary is None else primary, 'amount': amount}
    rows = [[cell(), cell(str(i+1)), cell('BTC' if i == 0 else f'COIN{i}'),
             cell('80,586.90\n+0.66%', '80,586.90'),
             cell('78,853.93\n$40.7M\n−2.15%', '78,853.93', '$40.7M'),
             cell(f'+1.21%\n{short}\n$26.5M', short, '$26.5M'),
             cell('12% / 40%'), cell('Above 68%'), cell('↓ 5/7')]
            for i in range(count)]
    return {'headers': headers, 'rows': rows}


class TableTests(unittest.TestCase):
    def test_prices_and_amounts_do_not_use_change_touch_or_confluence(self):
        rows = reader._parse_maxpain_table_rows([table()], '24h')
        self.assertEqual(len(rows), 30)
        btc = rows[0]
        self.assertEqual((btc['price'], btc['max_short_price'], btc['max_long_price']),
                         (80586.9, 81558.27, 78853.93))
        self.assertEqual((btc['short_amount_usd'], btc['long_amount_usd']), (26500000, 40700000))

    def test_columns_can_move_without_swapping_long_and_short(self):
        data = table()
        order = [8, 5, 3, 2, 1, 4, 6, 0, 7]
        data['headers'] = [data['headers'][i] for i in order]
        data['rows'] = [[row[i] for i in order] for row in data['rows']]
        row = reader._parse_maxpain_table_rows([data], '12h')[0]
        self.assertEqual((row['max_long_price'], row['max_short_price']), (78853.93, 81558.27))

    def test_unrecognized_or_ambiguous_schema_is_rejected(self):
        for data in ([], [dict(table(), headers=['Unknown'])], [table(), table()]):
            with self.subTest(data=len(data)), self.assertRaises(ValueError):
                reader._parse_maxpain_table_rows(data, '24h')

    def test_percentage_nonfinite_missing_and_negative_prices_are_rejected(self):
        for value in ('0.33%', 'nan', 'inf', '0', '-1', '—'):
            with self.subTest(value=value):
                data = table(1, short=value)
                self.assertEqual(reader._parse_maxpain_table_rows([data], '24h'), [])

    def test_missing_amount_does_not_become_price_or_zero(self):
        data = table(1)
        data['rows'][0][5]['amount'] = ''
        self.assertEqual(reader._parse_maxpain_table_rows([data], '24h'), [])

    def test_empty_loading_row_is_not_a_symbol(self):
        data = table(0)
        data['rows'] = [[{'text': 'Loading…', 'primary': 'Loading…', 'amount': ''}]]
        self.assertEqual(reader._parse_maxpain_table_rows([data], '24h'), [])


class ReadTests(unittest.IsolatedAsyncioTestCase):
    async def read(self, data=None, *, timeframe='24h', labels=None, **kwargs):
        self.page = SimpleNamespace(wait_for_timeout=AsyncMock())
        with patch.object(reader, '_extract_tables', AsyncMock(return_value=[data or table()])), \
             patch.object(reader, '_click_timeframe_verified', AsyncMock(return_value=True)), \
             patch.object(reader, '_active_timeframe_label', AsyncMock(
                 side_effect=labels, return_value=timeframe)):
            return await reader.read_timeframe(self.page, timeframe, **kwargs)

    async def test_accept_only_after_two_stable_reads(self):
        result = await self.read()
        self.assertTrue(result['verified'])
        self.assertEqual(self.page.wait_for_timeout.await_count, 2)

    async def test_requested_window_already_selected_on_fresh_retry(self):
        self.assertTrue((await self.read(timeframe='12h'))['verified'])

    async def test_wrong_active_timeframe_rejected(self):
        result = await self.read(labels=['12h'] * 25)
        self.assertFalse(result['verified'])
        self.assertFalse(result['debug']['active_ok'])

    async def test_too_few_rows_rejected(self):
        result = await self.read(table(29))
        self.assertFalse(result['verified'])
        self.assertFalse(result['debug']['row_count_ok'])

    async def test_duplicate_symbols_rejected(self):
        data = table()
        data['rows'][1][2] = deepcopy(data['rows'][0][2])
        self.assertFalse((await self.read(data))['verified'])

    async def test_cross_timeframe_duplicate_rejected(self):
        fp = reader._rows_fingerprint(reader._parse_maxpain_table_rows([table()], '12h'))
        result = await self.read(forbidden_fingerprints={'12h': fp})
        self.assertFalse(result['verified'])
        self.assertEqual(result['debug']['duplicate_of'], '12h')

    async def test_previous_fingerprint_rejected(self):
        fp = reader._rows_fingerprint(reader._parse_maxpain_table_rows([table()], '12h'))
        self.assertFalse((await self.read(previous_fingerprint=fp))['verified'])

    async def test_unknown_schema_stops_before_clicking_and_polling(self):
        page = SimpleNamespace(wait_for_timeout=AsyncMock())
        with patch.object(reader, '_extract_tables', AsyncMock(return_value=[])), \
             patch.object(reader, '_click_timeframe_verified', AsyncMock()) as click:
            result = await reader.read_timeframe(page, '24h')
        self.assertFalse(result['retryable'])
        click.assert_not_awaited()
        page.wait_for_timeout.assert_not_awaited()

    async def test_missing_selector_stops_without_polling(self):
        page = SimpleNamespace(wait_for_timeout=AsyncMock())
        with patch.object(reader, '_extract_tables', AsyncMock(return_value=[table()])), \
             patch.object(reader, '_click_timeframe_verified', AsyncMock(return_value=False)), \
             patch.object(reader, '_active_timeframe_label', AsyncMock(return_value='24h')):
            result = await reader.read_timeframe(page, '24h')
        self.assertFalse(result['retryable'])
        page.wait_for_timeout.assert_not_awaited()

    async def test_layout_failure_does_not_open_three_pages(self):
        page = SimpleNamespace(close=AsyncMock())
        failed = {'verified': False, 'rows': [], 'retryable': False}
        with patch.object(reader, '_new_ready_page', AsyncMock(return_value=page)) as opened, \
             patch.object(reader, 'read_timeframe', AsyncMock(return_value=failed)), \
             patch.object(reader, '_record_dom_memory'), \
             patch.object(reader.asyncio, 'sleep', AsyncMock()) as sleep:
            result = await reader._retry_timeframe_on_fresh_page(None, 'fixture://', '24h', None, 3)
        self.assertEqual(result, failed)
        self.assertEqual(opened.await_count, 1)
        page.close.assert_awaited_once()
        sleep.assert_not_awaited()

    async def test_layout_failure_aborts_atomic_collection_and_closes_browser(self):
        context = SimpleNamespace(close=AsyncMock())
        browser = SimpleNamespace(new_context=AsyncMock(return_value=context), close=AsyncMock())
        api = SimpleNamespace(chromium=SimpleNamespace(launch=AsyncMock(return_value=browser)))
        manager = AsyncMock()
        manager.__aenter__.return_value = api
        failed = {'verified': False, 'rows': [], 'retryable': False, 'debug': {}}
        with patch.object(reader, 'async_playwright', return_value=manager), \
             patch.object(reader, '_retry_timeframe_on_fresh_page', AsyncMock(return_value=failed)) as read, \
             patch.object(reader, '_record_dom_memory'):
            result = await reader.collect_coinglass_dom_snapshot()
        self.assertFalse(result['ok'])
        self.assertEqual(result['rows'], [])
        self.assertEqual(set(result['missing_timeframes']), set(reader.DEFAULT_TIMEFRAMES))
        self.assertEqual(read.await_count, 1)
        context.close.assert_awaited_once()
        browser.close.assert_awaited_once()


if __name__ == '__main__':
    unittest.main()
