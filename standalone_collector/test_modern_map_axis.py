"""Recalculate significant bins from the retained, market-only November 26 chart."""
from pathlib import Path
import unittest

from PIL import Image
from modern_map_axis import match_displayed_bins

# Filtered source SHA256 belongs to job d0199f9e-f493-4cf8-818d-72acf9685ed2.
# Fixture contains rightmost 66 plot pixels and the axis only. These centres
# are the bounding boxes of the independently readable printed prices in it.
FIXTURE=Path(__file__).parent/'fixtures/btc_12h_1343_axis_edge.png'
TICKS=[(63500,16.875),(63000,83.875),(62500,151.875),(62000,218.875),(61500,285.875),
       (61000,353.875),(60500,420.875),(60000,487.875),(59500,555.875),(59000,622.875),
       (58500,689.875),(58000,757.875),(60861,373.5)]
MARKS=[(f'{p:,}',91,y) for p,y in TICKS]
RAIL=[('63,305','10.6M'),('62,913','7.1M'),('62,085','21.2M'),('61,867','8.6M'),
      ('61,737','16.8M'),('61,475','42.4M'),('61,127','28.5M'),('60,255','42.4M'),
      ('60,038','19.0M'),('59,907','28.5M'),('59,646','21.2M'),('59,297','14.2M'),('58,426','10.6M')]
OVERVIEW={'raw_largest_value':'42.4M',
          'sidebar_levels':[{'raw_price':p,'raw_value':a} for p,a in RAIL]}


class SourceAxisTests(unittest.TestCase):
    def read(self,marks=MARKS,overview=OVERVIEW):
        with Image.open(FIXTURE) as im:
            return match_displayed_bins(im,(0,0,140,774),(0,0,65,773),marks,overview,1,63305,43.56)

    def test_native_edge_and_visible_tied_maximum_are_entirely_covered_by_side_labels(self):
        raw_last,_,runs,book,_=self.read()
        self.assertEqual(raw_last,'60,861')
        self.assertEqual([(colour,len(rows)) for colour,rows in runs],[('yellow',1),('yellow',1)])
        self.assertEqual([v for v in book.values()],
                         [{'raw_price':'60,255','raw_value':'42.4M'},{'raw_price':'61,475','raw_value':'42.4M'}])

    def test_uncovered_colour_unproven_reference_or_non_linear_ticks_require_more_source_data(self):
        missing=dict(OVERVIEW,sidebar_levels=[r for r in OVERVIEW['sidebar_levels'] if r['raw_price']!='60,255'])
        self.assertIsNone(self.read(overview=missing))
        self.assertIsNone(self.read(marks=MARKS[:-1]))
        self.assertIsNone(self.read(marks=[*MARKS[:2],('62,500',91,174),*MARKS[3:]]))
        self.assertIsNone(self.read(overview=dict(OVERVIEW,raw_largest_value='44.1M')))

    def test_weaker_sidebar_amount_requires_targeted_independent_read(self):
        weaker=[dict(r,raw_value='36.1M') if r['raw_price']=='60,255' else r for r in OVERVIEW['sidebar_levels']]
        self.assertIsNone(self.read(overview=dict(OVERVIEW,sidebar_levels=weaker)))
