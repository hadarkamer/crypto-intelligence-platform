"""Market-only rendered labels, latest-time matching and source resolution."""
from datetime import datetime,timezone
import unittest

from modern_map_cells import merge_cells
from modern_map_tooltip import member_sum,table_rows


class RenderedTableTests(unittest.TestCase):
    at=datetime(2026,11,26,0,2,tzinfo=timezone.utc)
    labels=[('23:55',90,40),('00:00',147,40),('60,000',30,66),
            ('3.0M',91,66),('4.1M',147,66),('60,050',29,86),('6M',89,86),('4.2M',146,86)]

    def test_multirow_multicolumn_and_midnight(self):
        rows=table_rows(self.labels,self.at)
        self.assertEqual({r['time'] for r in rows},{'11/25 23:55','11/26 00:00'})
        book={}
        counts=merge_cells(book,rows,'11/26 00:00','50')
        self.assertEqual(counts,{'accepted':2,'duplicates':0,'unresolved':0,'historical':2})
        self.assertEqual(member_sum(list(book.values())),'8.3M')

    def test_ambiguous_header_or_missing_cell_does_not_shift_columns(self):
        rows=table_rows([m for m in self.labels if m[0]!='3.0M'],self.at)
        self.assertEqual({r['price'] for r in rows},{'60,050'})
        self.assertEqual(table_rows([('23:55',148,40)]+self.labels,self.at),[])

    def test_right_price_axis_with_multitime_cells_and_uncertain_orientation(self):
        right = [(text, 209 if x in (29,30) else x, y) for text,x,y in self.labels]
        self.assertEqual(table_rows(right,self.at),table_rows(self.labels,self.at))
        self.assertEqual(table_rows([('00:00',80,50),('12',65,74),('14',95,74)],self.at),[])

    def test_captured_right_axis_without_amounts_and_coarser_price_bins(self):
        # Sanitized Firefox hover on 26 November 2026. The original receipt
        # exposes row and column headers, but contains no numeric amount cells.
        at = datetime(2026,11,26,13,43,tzinfo=timezone.utc)
        headers = [(stamp,x,728) for stamp,x in zip(
            ('13:20','13:25','13:30','13:35','13:40'),(634,696,758,820,882))]
        prices = [('61,018',950,750),('60,931',950,775),('60,844',950,800),
                  ('60,756',950,825),('60,669',950,850),('60,582',950,875),('60,495',950,900)]
        actual = [('11/26 13:40',639,681),*headers,*prices]
        self.assertEqual(table_rows(actual,at),[])
        # An isolated shaped replay adds synthetic amounts; it must still
        # reject a row ladder spanning two native steps for per-bin amounts.
        amounts = [('1.0M',x,y) for _,x,_ in headers for _,_,y in prices]
        rows = table_rows([*actual,*amounts],at)
        self.assertEqual(len(rows),35)
        book = {}
        counts = merge_cells(book,rows,'11/26 13:40','43.56')
        self.assertEqual((book,counts['unresolved'],counts['historical']),({},7,28))

    def test_source_sum_preserves_coarsest_precision(self):
        self.assertEqual(member_sum([{'raw_value':'1.1M'},{'raw_value':'12.34K'}]),'1.1M')
