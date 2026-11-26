"""Local single-column arithmetic using printed market numbers; no network or credentials."""
from copy import deepcopy
from decimal import Decimal
from unittest import TestCase

from model1_execution import StageFailure
from modern_map_cells import display_facts_summary, map_metrics, merge_cells, metrics_from_overview


def cell(price, amount, time='latest', **fields):
    return dict(price=price, amount=amount, time=time, **fields)


def snapshot():
    return dict(raw_last_price='58,500', raw_price_step='800', raw_above_total='120.2M',
                raw_below_total='80.0M', source_time='2026-11-26T14:08:00Z',
                source_time_after='2026-11-26T14:08:00Z', column_time='2026-11-26T14:05:00Z',
                column_time_after='2026-11-26T14:05:00Z', groups=[
                    dict(side='above', colour='green', raw_total='20M', members=[
                        dict(raw_price='64,700', raw_value='8.1M'), dict(raw_price='65,500', raw_value='11.8M')]),
                    dict(side='below', colour='yellow', raw_total='6.0M',
                         members=[dict(raw_price='57,500', raw_value='6.0M')])])


class CellTests(TestCase):
    def test_one_current_grid_promotes_several_cells_and_never_history(self):
        rows = [cell('61,000', '10.0M'), cell('61,046', '8.0M'),
                cell('61,000', '99M', 'older'), cell('61,046', '88M', 'older')]
        book = {}
        self.assertEqual(merge_cells(book, rows, 'latest', '45.65'),
                         dict(accepted=2, duplicates=0, historical=2, unresolved=0))
        self.assertEqual(book[Decimal(61000)]['raw_value'], '10.0M')
        self.assertEqual(merge_cells(book, rows[:2], 'latest', '45.65')['duplicates'], 2)
        self.assertEqual(set(book), {Decimal(61000), Decimal(61046)})

    def test_one_new_level_preceding_a_conflict_does_not_change_the_book(self):
        book = {Decimal(61046): dict(raw_price='61,046', raw_value='1.00M')}
        original = deepcopy(book)
        with self.assertRaises(StageFailure) as err:
            merge_cells(book, [cell('61,000','8.0M'), cell('61,046','1.01M')], 'latest', '45.65')
        self.assertEqual(err.exception.reason, 'hover_evidence_mismatch')
        self.assertEqual(book, original)

    def test_unknown_single_grid_or_coarse_resolution_and_historical_evidence(self):
        for rows in ([cell('61,000', '2M')], [cell('61,000', '2M', step='91.3')],
                     [cell('61,000', '2M'), cell('61,091', '4M')],
                     [cell('61,000', '2M'), cell('61,046', '4M', 'history')],
                     [cell('61,000', '2M', ''), cell('61,046', '4M', '')]):
            with self.subTest(rows=rows):
                book = {}
                counts = merge_cells(book, rows, 'latest', '45.65')
                self.assertFalse(book)
                self.assertEqual(counts['unresolved'] + counts['historical'], len(rows))

    def test_absent_amount_is_not_zero_and_overlap_keeps_better_precision(self):
        book = {}
        counts = merge_cells(book, [cell('61,000', '1M'), cell('61,045', None)], 'latest', '45.65')
        self.assertEqual((counts['accepted'], counts['unresolved']), (1, 1))
        new = merge_cells(book, [cell('61,000','0.98M',step='45.65')], 'latest', '45.65')
        self.assertEqual(new['duplicates'], 1)
        self.assertEqual(book[Decimal(61000)]['raw_value'], '0.98M')
        merge_cells(book, [cell('61,045','0',step='45.65')], 'latest', '45.65')
        self.assertEqual(book[Decimal(61045)]['raw_value'], '0')

    def test_cluster_midpoint_signed_distance_share_and_real_rounding_intervals(self):
        s = snapshot()
        results = map_metrics(s, '0.20B', {'above':'60%', 'below':'40%'})
        group, below = results['clusters']
        self.assertEqual((group['low'], group['high'], group['center']), (64700,65500,65100))
        self.assertEqual((group['distance_pct'], below['distance_pct']), (11.28, -1.71))
        self.assertEqual((group['amount'], group['raw_total'], group['liquidity_share_pct']), (19900000,'20M',9.95))
        self.assertEqual((results['difference_above_minus_below'], results['difference_percentage_points']),
                         (40200000,20.1))
        self.assertEqual(results['denominator_basis'], 'displayed_total')
        self.assertEqual(results['side_percent'], {'above':60.1,'below':40.0})
        self.assertEqual(map_metrics(s)['denominator_basis'], 'sum_of_displayed_side_amounts')
        self.assertIsNone(map_metrics(s)['raw_total'])
        for total, pct in [('220M',None), ('200M','95%'), ('200M','101%')]:
            with self.subTest(total=total, pct=pct), self.assertRaises(StageFailure):
                map_metrics(s,total,{'above':pct})

    def test_binding_overview_compare_signed_distance_amount_and_map_update(self):
        s = snapshot()
        row = dict(raw_price='64,700', raw_value='8.1M', raw_distance_pct='+10.60%')
        overview = dict(source_time=s['source_time'],raw_total='200M',sidebar_levels=[row])
        self.assertEqual(metrics_from_overview(s,overview)['clusters'][0]['center'],65100)
        for changes in ({'source_time':'prior'}, {'raw_last_price':'59,000'},
                        {'sidebar_levels':[dict(row, raw_value='81M')]},
                        {'sidebar_levels':[dict(row, raw_distance_pct='-10.60%')]}):
            with self.subTest(changes=changes), self.assertRaises(StageFailure):
                metrics_from_overview(s,dict(overview,**changes))

    def test_observed_leaders_respect_display_precision_and_realized_are_separate(self):
        s = snapshot()
        common = dict(source_time=s['source_time'], raw_realized_total='100M',
                      raw_realized_longs='60.2M', raw_realized_shorts='40.0M')
        overview = dict(common, sidebar_levels=[dict(raw_price='63,000',raw_value='12M')])
        details = metrics_from_overview(s, overview)
        self.assertEqual(details['realized_24h_status'], 'rounding_consistent')
        self.assertEqual([r['raw_price'] for r in details['strongest_observed_levels']['above']],
                         ['65,500','63,000'])
        self.assertEqual(details['total'], 200200000)
        self.assertEqual(metrics_from_overview(s,dict(overview,raw_realized_total='80M'))['realized_24h_status'],
                         'component_total_conflict')
        self.assertEqual(metrics_from_overview(s,dict(overview,raw_realized_total=None))['realized_24h_status'],
                         'incomplete')

    def test_display_uses_printed_total_distinct_from_reconstructed_denominator(self):
        s = dict(snapshot(),raw_above_total='478.3M',raw_below_total='537.7M')
        s['groups'][0]['members'][0]['raw_value'] = '100.0M'
        s['groups'][0]['members'][1]['raw_value'] = '35.0M'
        s['groups'][0]['raw_total'] = '135.0M'
        facts = dict(source_time=s['source_time'], raw_total='1.02B', raw_column_period='5m')
        metrics = metrics_from_overview(s,facts)
        text = display_facts_summary(s,facts,metrics,4000)
        self.assertIn('מהסך המוצג 1.02B',text)
        self.assertIn('64,700–65,500 · ≈135.0M · +11.3% · ≈13.2%',text)
        self.assertEqual(metrics['clusters'][0]['liquidity_share_pct'],13.24)
        self.assertEqual(map_metrics(s)['clusters'][0]['liquidity_share_pct'],13.29)
        self.assertIn('חיסולים שבוצעו ב־24 שעות · סך חסר',text)
        short = display_facts_summary(s,facts,metrics,180)
        self.assertLessEqual(len(short),180)
        self.assertIn('2 קלאסטרים',short)

    def test_reject_changed_epoch_or_duplicate_noncontiguous_cross_price_groups(self):
        s = snapshot()
        for mutate in (lambda x:x.update(column_time_after='prior'),
                       lambda x:x['groups'].append(x['groups'][0]),
                       lambda x:x['groups'][0]['members'][1].update(raw_price='67,400'),
                       lambda x:x['groups'][0].update(side='below')):
            mutated = deepcopy(s)
            mutate(mutated)
            with self.assertRaises(StageFailure):
                map_metrics(mutated)
