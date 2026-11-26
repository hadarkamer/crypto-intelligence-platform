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

    def test_source_sum_preserves_coarsest_precision(self):
        self.assertEqual(member_sum([{'raw_value':'1.1M'},{'raw_value':'12.34K'}]),'1.1M')
