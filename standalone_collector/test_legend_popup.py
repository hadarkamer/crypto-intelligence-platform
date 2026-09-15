"""Local HTML fixtures only. No source navigation, credentials or model calls."""
from pathlib import Path
import sys
import unittest
from playwright.sync_api import sync_playwright
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'runtime'))
from model1_legend import dismiss_legend,prepare_legend_for_capture,CARD
from model1_execution import StageFailure
from model1_price_range import validate_price_range

HTML='''<div class="MuiCard-root" id="help">
<div class="shou" data-first-child=""><svg width="20" height="20"><path d="M405 136.798L375.202 107 256 226.202"/></svg></div>
<div data-last-child=""><a href="https://legend.coinglass.com"><div>Legend</div><div>NEW</div></a></div>
</div><div role="dialog" id="account">Log in to unlock full data <button>Close</button></div>'''

class LegendTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.p=sync_playwright().start();cls.browser=cls.p.chromium.launch(headless=True)
    @classmethod
    def tearDownClass(cls):
        cls.browser.close();cls.p.stop()
    def setUp(self):
        self.context=self.browser.new_context()
        self.context.route('**/*',lambda route:route.abort())
        self.page=self.context.new_page()
    def tearDown(self):self.context.close()
    def test_normal_x_closes_help_not_account_gate(self):
        self.page.set_content(HTML)
        self.page.evaluate("() => { document.querySelector('#help > div').onclick=()=>document.querySelector('#help').remove(); }")
        self.assertEqual(self.page.locator(CARD).count(),1)
        self.assertTrue(self.page.locator('#help').is_visible())
        self.assertTrue(dismiss_legend(self.page,timeout_ms=500))
        self.assertEqual(self.page.locator(CARD).count(),0)
        self.assertTrue(self.page.locator('#account').is_visible())
        self.assertEqual(self.page.url,'about:blank')
    def test_no_help_no_click(self):
        self.page.set_content('<button>Close</button>')
        self.assertFalse(dismiss_legend(self.page,timeout_ms=200))
    def test_missing_verified_x_does_not_guess(self):
        self.page.set_content(HTML.replace('data-first-child','data-unrelated'))
        with self.assertRaises(StageFailure):dismiss_legend(self.page,timeout_ms=200)
        self.assertTrue(self.page.locator('#help').is_visible())
    def test_failed_close_never_hides_with_css(self):
        self.page.set_content(HTML)
        with self.assertRaises(StageFailure):dismiss_legend(self.page,timeout_ms=200)
        self.assertTrue(self.page.locator('#help').is_visible())
    def test_similar_card_with_other_link_is_untouched(self):
        self.page.set_content(HTML.replace('https://legend.coinglass.com','https://example.invalid'))
        self.assertFalse(dismiss_legend(self.page,timeout_ms=200))
        self.assertTrue(self.page.locator('#help').is_visible())
    def test_numeric_tick_order_regression(self):
        value={'low':75300,'high':76050,'axis_low':76000,'axis_high':74000,
               'confidence':'medium','basis':'last_candle_axis_bracket'}
        with self.assertRaises(StageFailure):validate_price_range(value)
        self.assertEqual(validate_price_range({**value,'axis_low':74000,'axis_high':78000})['high'],76050)
    def test_unresolved_help_does_not_destroy_screenshot_evidence(self):
        self.page.set_content(HTML)
        self.assertEqual(prepare_legend_for_capture(self.page,timeout_ms=200),'unresolved')
        raw=self.page.screenshot()
        self.assertTrue(raw.startswith(b'\x89PNG\r\n\x1a\n'))
        self.assertTrue(self.page.locator('#help').is_visible())
        self.assertTrue(self.page.locator('#account').is_visible())
        self.assertEqual(self.page.url,'about:blank')
    def test_preparation_distinguishes_absent_and_closed(self):
        self.page.set_content('<p>No help card</p>')
        self.assertEqual(prepare_legend_for_capture(self.page,timeout_ms=200),'absent')
        self.page.set_content(HTML)
        self.page.evaluate("() => { document.querySelector('#help > div').onclick=()=>document.querySelector('#help').remove(); }")
        self.assertEqual(prepare_legend_for_capture(self.page,timeout_ms=500),'closed')
        self.assertTrue(self.page.locator('#account').is_visible())
    def test_no_guess_when_close_markup_changes(self):
        self.page.set_content(HTML.replace('data-first-child','data-other'))
        self.assertEqual(prepare_legend_for_capture(self.page,timeout_ms=200),'unresolved')
        self.assertTrue(self.page.locator('#help').is_visible())
    def test_unresolved_help_is_not_a_numeric_approval(self):
        from test_original_flow import sample
        import collection_model1_task as task
        raw=sample();raw['scans'][0]['readable']=False
        with self.assertRaises((ValueError,StageFailure)):
            task.normalize(raw,timeframe='12H',run_id='00000000-0000-4000-8000-000000000001',
                captured_at='2026-01-01T00:00:00Z',image=b'synthetic')

if __name__=='__main__':unittest.main()
