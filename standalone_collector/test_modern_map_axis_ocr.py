from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from PIL import Image, ImageDraw
from modern_map_axis_ocr import axis_scale_live


FIXTURES=Path(__file__).parent/'fixtures'
BOX=(0,0,140,774)
PLOT=(0,0,65,773)


class PrintedPriceTests(TestCase):
    def test_real_filtered_charts_without_numeric_dom_or_runtime_ocr(self):
        # Each crop contains the rightmost 66 plot pixels and the original price axis.
        for stamp,shown,center in [('1343','60,861',373.5),('1500','60,677',397.5),('1608','60,652',381.0)]:
            with self.subTest(stamp=stamp), Image.open(FIXTURES/f'btc_12h_{stamp}_axis_edge.png') as image:
                raw,where=axis_scale_live(image,BOX,PLOT,[])
                self.assertEqual(raw,shown)
                self.assertLess(abs(where(float(shown.replace(',','')))-center),2.2)
                self.assertLess(where(63000),where(61000))

    def test_inconsistent_native_mark_missing_stripe_or_corrupted_tag_abstains(self):
        with Image.open(FIXTURES/'btc_12h_1500_axis_edge.png') as image:
            self.assertIsNone(axis_scale_live(image,BOX,PLOT,[('59,000',110,397)]))
            no_line=image.copy()
            ImageDraw.Draw(no_line).rectangle((0,391,65,405),fill=(44,24,78))
            self.assertIsNone(axis_scale_live(no_line,BOX,PLOT,[]))
            damaged=image.copy()
            ImageDraw.Draw(damaged).rectangle((73,391,79,402),fill=(240,240,240))
            self.assertIsNone(axis_scale_live(damaged,BOX,PLOT,[]))
            no_ticks=image.copy()
            ImageDraw.Draw(no_ticks).rectangle((66,0,139,384),fill=(18,21,27))
            self.assertIsNone(axis_scale_live(no_ticks,BOX,PLOT,[]))

    def test_scoped_panel_reference_corroborated_by_price_axis_in_retained_1841_chart(self):
        with Image.open(FIXTURES/'btc_12h_1841_axis_edge.png') as image:
            self.assertIsNone(axis_scale_live(image,BOX,PLOT,[]))
            for raw in ('60,588','60587.70'):
                with self.subTest(raw=raw):
                    displayed,where=axis_scale_live(image,BOX,PLOT,[],panel_reference=raw)
                    self.assertEqual(displayed,'60,588')
                    self.assertLess(abs(where(60588)-386.5),2.2)
            for raw in ('60,488','60,988','60,888','-1','unknown'):
                with self.subTest(raw=raw):
                    self.assertIsNone(axis_scale_live(image,BOX,PLOT,[],panel_reference=raw))

    def test_panel_reference_requires_readable_tag_numerals_calibrated_ticks_and_line(self):
        with Image.open(FIXTURES/'btc_12h_1841_axis_edge.png') as image:
            regions=[(75,377,137,395),(66,0,139,371),(0,377,65,397)]
            for region in regions:
                with self.subTest(region=region):
                    altered=image.copy()
                    ImageDraw.Draw(altered).rectangle(region,fill=(243,243,243) if region==regions[0] else (20,22,26))
                    self.assertIsNone(axis_scale_live(altered,BOX,PLOT,[],panel_reference='60,588'))
            self.assertIsNone(axis_scale_live(image,BOX,PLOT,[('61,010',101,386)],panel_reference='60,588'))
        # A confidently read digit of an earlier source remains decisive even
        # when two prices would be indistinguishable geometrically at this zoom.
        with Image.open(FIXTURES/'btc_12h_1500_axis_edge.png') as image:
            self.assertIsNone(axis_scale_live(image,BOX,PLOT,[],panel_reference='60,678'))

    def test_valid_native_scale_skips_pixel_character_reader(self):
        from test_modern_map_axis import MARKS
        with Image.open(FIXTURES/'btc_12h_1343_axis_edge.png') as image, patch('modern_map_axis_ocr._printed') as decode:
            self.assertEqual(axis_scale_live(image,BOX,PLOT,MARKS)[0], '60,861')
            decode.assert_not_called()
