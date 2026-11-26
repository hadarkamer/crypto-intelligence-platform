"""Market-only rendered labels, latest-time matching and source resolution."""
from datetime import datetime,timezone
import unittest

from modern_map_cells import merge_cells
from modern_map_tooltip import member_sum,price_labels,read_tooltip,table_rows


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


class PriceLabelReceiptTests(unittest.TestCase):
    # Stored market-only E observation, 26 November. These are row/column
    # headers in the tooltip, with zero monetary cells in the capture.
    at = datetime(2026,11,26,15,0,tzinfo=timezone.utc)
    headings = [('11/26 15:00',639,681),('14:40',634,728),('14:45',696,728),
                ('14:50',758,728),('14:55',820,728),('15:00',882,728)]
    prices = [('61,018',950,750),('60,931',950,775),('60,844',950,800),('60,756',950,825),
              ('60,669',950,850),('60,582',950,875),('60,495',950,900)]

    def test_e_observation_retains_seven_printed_prices_without_amounts(self):
        from unittest.mock import Mock
        page = Mock()
        page.evaluate.side_effect = [[],None,self.headings+self.prices]
        page.evaluate_handle.return_value.evaluate.side_effect = [[],None]
        event=read_tooltip(page,(0,0,1000,1000),(0,0,1000,1000),self.at,990,790,'paint')
        self.assertEqual(event['raw_price_labels'],[p for p,_,_ in self.prices])
        self.assertEqual((event['rows'],event['price_label_source'],event['dom_marks']),([], 'dom', 13))
        self.assertEqual(event['source'],'unresolved')

    def test_left_axis_and_rejection_of_stale_or_ambiguous_evidence(self):
        left=[(p,580,y) for p,_,y in self.prices]
        self.assertEqual(price_labels(self.headings+left,self.at),[p for p,_,_ in self.prices])
        bad_prices=[(p,580,y) for p,_,y in reversed(self.prices[1:])]
        self.assertEqual(price_labels(self.headings+self.prices+bad_prices,self.at),[])
        self.assertEqual(price_labels(self.headings[1:]+self.prices,self.at),[])
        self.assertEqual(price_labels(self.headings[:-1]+self.prices,self.at),[])
        self.assertEqual(price_labels(self.headings+[('11/26 15:00',640,659)]+self.prices,self.at),[])
        self.assertEqual(price_labels(self.headings+[('11/26 14:55',640,659)]+self.prices,self.at),[])
        self.assertEqual(price_labels(self.headings+self.prices,self.at.replace(minute=5)),[])

    def test_short_irregular_or_spatially_jagged_price_axis_is_rejected(self):
        candidates = [self.prices[:2],
                      self.prices[:3]+[('60,482',950,825)],
                      [(p,x,y) for (p,x,_),y in zip(self.prices,(750,775,788,825))]]
        for candidate in candidates:
            with self.subTest(candidate=candidate):
                self.assertEqual(price_labels(self.headings+candidate,self.at),[])
        # A single native ladder is compatible with adjacent displayed gaps of
        # 87 and 88, because all seven actual prices were rounded to integers.
        self.assertEqual(len(price_labels(self.headings+self.prices,self.at)),7)

    def test_header_requires_uniform_source_cadence_and_horizontal_spacing(self):
        wrong_time=[*self.headings[:3],('14:51',758,728),*self.headings[4:]]
        wrong_x=[*self.headings[:3],('14:50',773,728),*self.headings[4:]]
        for header in (wrong_time,wrong_x,[self.headings[0],*self.headings[-2:]]):
            self.assertEqual(price_labels(header+self.prices,self.at),[])
        ten_minute=[self.headings[0], *[(t,x,y) for (_,x,y),t in zip(self.headings[1:],
                    ('14:20','14:30','14:40','14:50','15:00'))]]
        self.assertEqual(len(price_labels(ten_minute+self.prices,self.at)),7)
