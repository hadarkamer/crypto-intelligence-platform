from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import Mock

from modern_map_overview import overview, parse_overview


CLOCK = datetime(2026, 11, 26, 12, 9, tzinfo=timezone.utc)


class OverviewTest(TestCase):
    def test_preserves_genuine_market_tokens_in_one_browser_operation(self):
        page = Mock()
        # Market tokens from the earlier 12:08 Firefox observation. The source
        # only displays a minute; the clock supplies the date and UTC context.
        page.evaluate.return_value = {
            'values': {'raw_above_total': ['478.3M'], 'raw_below_total': ['537.7M'],
                       'raw_total': ['1.02B'], 'raw_price_step': ['45.65'],
                       'raw_column_period': ['5m'], 'raw_updated_at': ['12:08']},
            'rail': [['63,326', '17.9M', '+4.18%'], ['62,915', '11.1M', '+3.50%'],
                     ['62,094', '33.2M', '+2.15%'], ['60,268', '66.5M', '-0.86%'],
                     ['60,039', '25.4M', '-1.23%'], ['59,902', '41.6M', '-1.46%']],
            'unreadable_rail': 0,
        }
        result = overview(page, CLOCK)
        page.evaluate.assert_called_once()
        self.assertEqual(result['source_time'], '2026-11-26T12:08:00Z')
        self.assertEqual(result['display_values']['raw_above_total'], ['478.3M'])
        self.assertEqual(result['parsed_values']['raw_above_total'], '478300000.0')
        self.assertEqual(len(result['sidebar_levels']), 6)
        self.assertIn('raw_realized_total', result['missing_fields'])
        self.assertIsNone(result['raw_realized_total'])

    def test_conflicting_levels_and_updates_are_retained_as_evidence_only(self):
        batch = {'values': {'raw_updated_at': ['12:08', '12:09'], 'raw_realized_total': ['$0.0M']},
                 'rail': [['60,268', '66.5M', '−0.86%'], ['60268', '$66.5M', '-0.86%'],
                          ['60,268', '66.6M', '-0.86%']], 'unreadable_rail': 1}
        result = parse_overview(batch, CLOCK)
        self.assertEqual(result['sidebar_levels'], [])
        self.assertIsNone(result['source_time'])
        self.assertEqual(result['duplicate_sidebar_levels'], 1)
        self.assertEqual(len(result['raw_sidebar_rows']), 3)
        self.assertEqual(result['ambiguous_fields'], ['raw_updated_at', 'sidebar_levels', 'sidebar_shape'])
        self.assertEqual(result['raw_realized_total'], '0.0M')
        self.assertNotIn('raw_realized_total', result['missing_fields'])

    def test_old_source_time_and_unmarked_largest_price_are_never_fabricated(self):
        result = parse_overview({'values': {'raw_updated_at': ['11:01'], 'raw_largest_value': ['86.3M']},
                                 'rail': [['60,908', '86.3M', '+1.45%']], 'unreadable_rail': 0}, CLOCK)
        self.assertIsNone(result['raw_largest_price'])
        self.assertIsNone(result['source_time'])
        self.assertIn('source_time', result['ambiguous_fields'])
        self.assertEqual(result['raw_largest_value'], '86.3M')


# Layout replay of the November market panel; values from the earlier chart
# are deliberately dated and must never be published as a current observation.
MARKET_HTML = '''<main><nav><span>Range</span><div role="combobox">12 hour</div>
<div role="combobox">Single chart</div><time>Updated <span>12:08</span></time></nav>
<nav><time>Updated 12:00</time></nav><aside><h2>Market overview</h2>
<div><span>Open liquidation levels (total)</span>1.29B</div>
<div><span>Longs (below price)</span><b>471.1M</b><b>36%</b></div>
<div><span>Shorts (above price)</span><b>822.5M</b><b>64%</b></div>
<div><span>Liquidated in 24h</span><b>$356.3M</b></div>
<div><span>of which longs / shorts</span>$156.8M / $199.4M</div>
<div><span>Largest single level</span>86.3M</div>
<div><span>Price step / column period</span>49.33 / 5m</div></aside>
<aside><h2>Key liquidation levels</h2><div data-lv><span>60,908</span><b>86.3M</b><span>+1.45%</span>
<span>59,724</span><b>36.9M</b><span>-0.52%</span></div></aside></main>
<form><div><span>Largest single level</span>999M</div><input value="private-sentinel"></form>'''


class BrowserOverviewTest(TestCase):
    def test_market_cards_rendered_including_glued_amount_and_percent(self):
        import os
        from pathlib import Path
        import shutil
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            bundled = Path(pw.chromium.executable_path)
            binary = str(bundled) if bundled.is_file() else shutil.which('chromium')
            if os.environ.get('COINGLASS_QA_USE_SYSTEM_CHROME') == '1':
                binary = '/opt/google/chrome/chrome'
            if not binary or (binary == '/usr/bin/chromium' and Path('/etc/debian_version').exists()
                              and not Path('/usr/lib/chromium/chromium').is_file()):
                self.skipTest('No offline Chromium executable installed')
            browser = pw.chromium.launch(executable_path=binary, headless=True, chromium_sandbox=True)
            try:
                page = browser.new_page()
                page.context.set_offline(True)
                page.set_content(MARKET_HTML)
                result = overview(page, CLOCK)
            finally:
                browser.close()
        self.assertEqual((result['raw_above_total'], result['raw_above_pct']), ('822.5M', '64%'))
        self.assertEqual((result['raw_below_total'], result['raw_below_pct']), ('471.1M', '36%'))
        self.assertEqual((result['raw_realized_longs'], result['raw_realized_shorts']), ('156.8M', '199.4M'))
        self.assertEqual(result['display_values']['raw_realized_total'], ['$356.3M'])
        self.assertEqual(result['raw_largest_value'], '86.3M')
        self.assertEqual(len(result['sidebar_levels']), 2)
        self.assertEqual(result['raw_updated_at'], '12:08')
        self.assertEqual((result['raw_price_step'], result['raw_column_period']), ('49.33', '5m'))
        self.assertNotIn('private-sentinel', str(result))


class FailureReceiptTest(TestCase):
    def test_stamp_survives_missing_or_conflicting_market_cards(self):
        from modern_map_overview import after_read_failure
        page, guard, emit = Mock(), Mock(), Mock()
        stamp = datetime.now(timezone.utc).strftime('%H:%M')
        page.locator.return_value.evaluate.return_value = {
            'values': {'raw_updated_at': [stamp], 'raw_total': ['1B'], 'raw_above_total': ['1M', '2M']},
            'clock_complete': True, 'totals_complete': True}
        after_read_failure(page, guard, emit)
        guard.assert_called_once_with()
        page.locator.return_value.evaluate.assert_called_once()
        kind, record = emit.call_args.args
        self.assertEqual((kind, record['name'], record['status']), ('stage', 'post_failure_market_observation', 'observed'))
        self.assertEqual(record['post_failure_raw_updated_at'], stamp)
        self.assertTrue(record['post_failure_source_time'].endswith('Z'))
        self.assertEqual(record['post_failure_raw_total'], '1B')
        self.assertNotIn('post_failure_raw_above_total', record)
        self.assertNotIn('source_time', record)

    def test_refused_guard_and_page_loss_cannot_replace_original_failure(self):
        from modern_map_overview import after_read_failure
        page, emit = Mock(), Mock()
        after_read_failure(page, Mock(side_effect=ValueError('private error')), emit)
        page.locator.assert_not_called()
        page.locator.return_value.evaluate.side_effect = RuntimeError('page gone')
        after_read_failure(page, None, emit)
        emit.assert_not_called()
