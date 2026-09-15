"""Local HTML fixtures only. No source navigation, credentials or model calls."""
from pathlib import Path
import sys
import unittest
from playwright.sync_api import sync_playwright
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'runtime'))
from model1_legend import dismiss_legend,CARD
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
        self.page.evaluate("document.querySelector('#help > div').onclick=()=>document.querySelector('#help').remove()")
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

if __name__=='__main__':unittest.main()
