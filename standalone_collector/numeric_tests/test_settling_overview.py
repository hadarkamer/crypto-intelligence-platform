"""Keep the grouped readiness sample equivalent to the protected chart's original field reads."""
from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import patch

from playwright.sync_api import sync_playwright

import modern_map_reader as reader
from model1_execution import StageFailure


CARDS = """<main>
  <div id="total"><span>Open liquidation levels (total)</span> 100M</div>
  <div><span>Longs (below price)</span> 20M 20%</div>
  <div><span>Shorts (above price)</span> 80M 80%</div>
  <div id="step"><span>Price step / column period</span> 1 / 5m</div>
</main>"""


class SettlingOverviewTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.driver = sync_playwright().start()
        cls.browser = cls.driver.firefox.launch(headless=True)

    @classmethod
    def tearDownClass(cls):
        cls.browser.close()
        cls.driver.stop()

    def setUp(self):
        self.page = self.browser.new_page()
        self.addCleanup(self.page.close)
        self.updated = datetime.now(timezone.utc).strftime('Updated %H:%M')

    def test_unique_cards_and_hidden_duplicate_match_original_observation(self):
        self.page.set_content(CARDS + f'<p>{self.updated}</p><p hidden>{self.updated}</p>')
        expected = reader.details(self.page)
        with patch.object(reader, 'details', side_effect=AssertionError('unexpected fallback')):
            self.assertEqual(reader._settling_details(self.page), expected)

    def test_per_selector_visibility_cap_and_ambiguity(self):
        for hidden, extra, accepted in ((59, '', True), (60, '', False), (0, self.updated, False)):
            with self.subTest(hidden=hidden, extra=extra):
                self.page.set_content(f'<p hidden>{self.updated}</p>' * hidden + CARDS +
                                      f'<p>{self.updated}</p><p>{extra}</p>')
                for read in (reader.details, reader._settling_details):
                    if accepted:
                        self.assertEqual(read(self.page)['raw_above_total'], '80M')
                    else:
                        with self.assertRaises(StageFailure):
                            read(self.page)

    def test_duplicate_label_and_embedded_update_cannot_replace_timestamp(self):
        # Playwright normalizes the duplicate label's whitespace. Its rendered
        # innerText is different. The printed time is part of a price card.
        self.page.set_content(CARDS +
            '<div><span style="white-space:pre">Longs  (below price)</span> 20M</div>')
        self.page.locator('#step').evaluate("(element,t)=>element.append(' (example '+t+')')", self.updated)
        with patch.object(reader, 'details', wraps=reader.details) as fallback:
            with self.assertRaises(StageFailure):
                reader._settling_details(self.page)
            fallback.assert_called_once_with(self.page)
