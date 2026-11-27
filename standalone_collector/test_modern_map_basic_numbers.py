"""Numerical BASIC contract. All inputs here are local synthetic observations."""
from copy import deepcopy
from datetime import datetime,timezone
from zoneinfo import ZoneInfo
import json
from pathlib import Path
import unittest
from model1_execution import StageFailure
from modern_map_basic_numbers import SOURCE_URL, build_basic_snapshot,validate_basic_snapshot
from modern_map_evidence import iso,source_clock

STAMP='2026-11-26T15:00:00Z'

def live_inputs():
    side=[dict(raw_price='110',raw_value='10.0M',raw_distance_pct='+10.00%'),
          dict(raw_price='90',raw_value='10.0M',raw_distance_pct='-10.00%')]
    market=dict(raw_total='100M',raw_above_total='55.1M',raw_below_total='44.9M',
                raw_realized_total='100.0K',raw_realized_longs='65.0K',raw_realized_shorts='35.0K',
                raw_largest_value='10.0M',raw_largest_price=None,raw_above_pct='55%',raw_below_pct='45%',
                raw_price_step='5.00',raw_column_period='5m',raw_last_price=None,source_time=STAMP,sidebar_levels=side)
    end=dict(market, raw_last_price='100',symbol='BTC',heatmap_model=1,timeframe='12H',mode='Coin',column_time=STAMP,
             same_document=True,settings_verified=True,same_market_labels=True,sidebar_rows=2,chart_regions=2,native_bins_inspected=30)
    regions=[dict(side='below',colour='yellow',prices=[90]),dict(side='above',colour='green',prices=[110,115])]
    args=dict(symbol='BTC',model=1,timeframe='12H',source_url=SOURCE_URL,source_time_after=STAMP,
              column_time=STAMP,column_time_after=STAMP,raw_last_price='100',captured_at='2026-11-26T15:00:04Z',
              source_sha256='a'*64,chart_regions=regions,native_bins_inspected=30,
              provenance='live',end_observation=end)
    return market,args

def live_sample():
    market,args=live_inputs()
    return build_basic_snapshot(market,**args)


class BasicNumbersTests(unittest.TestCase):
    def test_retained_1700_local_reconstruction(self):
        f=json.loads((Path(__file__).parent/'fixtures/btc_basic_1500_replay.json').read_text())
        snapshot=build_basic_snapshot(f['overview'],symbol='BTC',model=1,timeframe='12H',source_url=SOURCE_URL,
                  source_time_after=f['source_time'],column_time=f['column_time'],column_time_after=None,
                  raw_last_price=f['axis_reference'],source_sha256=f['sha256'],captured_at=f['closed_at'],
                  chart_regions=f['chart_regions'],native_bins_inspected=f['native_bins_inspected'],
                  end_observation=f['closing'],provenance='retained_reconstruction')
        self.assertEqual(validate_basic_snapshot(snapshot),snapshot)
        c=snapshot['coverage']
        self.assertEqual([c[k] for k in ('sidebar_count','verified_count','sidebar_amount','verified_share_pct',
                                         'significant_bins','priced_bins','unpriced_bins','regions_total','regions_with_amount')],
                         [14,14,259800000,29.69,8,2,6,6,2])
        self.assertEqual([g['share_pct'] for g in snapshot['chart_regions'] if g['amount'] is not None],[4.25,4.25])
        self.assertEqual(snapshot['provenance'],'retained_reconstruction')
        self.assertFalse(snapshot['end_reference_verified'] or snapshot['end_column_verified'])

    def test_single_map_verified_and_unknown_are_separate(self):
        snapshot=live_sample()
        self.assertEqual(validate_basic_snapshot(snapshot),snapshot)
        self.assertEqual(snapshot['collection_tier'],'basic')
        self.assertEqual(snapshot['enrichment']['additional_verified_count'],0)
        self.assertEqual(snapshot['coverage']['verified_amount'],20_000_000)
        self.assertEqual(snapshot['coverage']['verified_share_pct'],20)
        green=snapshot['chart_regions'][1]
        self.assertEqual((green['coverage'],green['known_amount'],green['amount'],green['share_pct']),
                         ('partial',10_000_000,None,None))
        self.assertEqual(green['distance_pct'],12.5)
        self.assertEqual(snapshot['coverage']['full_map_amounts_verified'],False)
        self.assertTrue(snapshot['same_document'] and snapshot['end_reference_verified'] and snapshot['end_column_verified'])

    def test_region_distance_uses_its_displayed_bounds_and_restores_only_exact_old_arithmetic(self):
        market,args=live_inputs()
        args['chart_regions'][0]['prices']=[90.25]
        current=build_basic_snapshot(market,**args)
        below=current['chart_regions'][0]
        self.assertEqual((below['price_low_estimate'],below['price_high_estimate']), (90.25,90.25))
        self.assertEqual(below['distance_pct'],-9.75)
        self.assertEqual((below['verified_prices'],below['amount']),(['90'],10000000))
        historical=deepcopy(current)
        historical['chart_regions'][0]['distance_pct']=-10.0
        with self.assertRaises(StageFailure):validate_basic_snapshot(historical)
        self.assertEqual(validate_basic_snapshot(historical,restored=True),current)
        self.assertEqual(historical['chart_regions'][0]['distance_pct'],-10)
        historical['chart_regions'][0]['amount']=1
        with self.assertRaises(StageFailure):validate_basic_snapshot(historical,restored=True)

    def test_cell_overlaps_reported_once_and_new_value_can_complete_one_cluster(self):
        market,args=live_inputs()
        geo=dict(source='hover',raw_price_step='5.00',source_time=STAMP,column_time=STAMP)
        args['enrichment']=[dict(geo,raw_price='110',raw_value='10.00M'),
                            dict(geo,raw_price='115',raw_value='2.0M')]
        result=build_basic_snapshot(market,**args)
        self.assertEqual(validate_basic_snapshot(result),result)
        self.assertEqual(result['collection_tier'],'enriched')
        self.assertEqual(result['enrichment'],dict(observations=2,additional_verified_count=1,
                                                   additional_verified_prices=['115'],overlapping_observations=1))
        self.assertEqual([result['coverage'][k] for k in ('verified_count','verified_amount','sidebar_amount','duplicates_removed')],
                         [3,22_000_000,20_000_000,1])
        self.assertEqual((result['chart_regions'][1]['coverage'],result['chart_regions'][1]['share_pct']),('verified',12))
        for field,wrong in [('raw_price_step','10.0'),('source_time','2026-11-26T14:59:00Z'),
                            ('column_time','2026-11-26T14:55:00Z'),('raw_value','99.0M')]:
            bad=deepcopy(args);bad['enrichment'][0][field]=wrong
            with self.assertRaises(StageFailure):build_basic_snapshot(market,**bad)

    def test_duplicate_sidebar_uses_unique_source_price_and_rejects_conflict(self):
        market,args=live_inputs()
        market['sidebar_levels'].append(market['sidebar_levels'][0].copy())
        result=build_basic_snapshot(market,**args)
        self.assertEqual((result['coverage']['sidebar_count'],result['coverage']['sidebar_amount']),(2,20_000_000))
        market['sidebar_levels'][-1]['raw_value']='19.0M'
        with self.assertRaises(StageFailure):build_basic_snapshot(market,**args)

    def test_live_end_and_supplement_consistency(self):
        for key,bad in [('source_time','2026-11-26T14:00:00Z'),('column_time','2026-11-26T14:55:00Z'),
                        ('same_market_labels',False),('same_document',False),('same_document',1),('raw_last_price','102'),
                        ('raw_realized_total','95.0K'),('raw_above_total','54.1M')]:
            market,args=live_inputs();args['end_observation'][key]=bad
            with self.subTest(key=key),self.assertRaises(StageFailure):build_basic_snapshot(market,**args)

    def test_replay_gates_and_computed_data_tampering(self):
        market,args=live_inputs();end=args['end_observation']
        args.update(provenance='retained_reconstruction',column_time_after=None,
                    end_observation={k:end[k] for k in ('source_time','raw_total','raw_above_total','raw_below_total')})
        snapshot=build_basic_snapshot(market,**args)
        self.assertFalse(snapshot['end_reference_verified'] or snapshot['end_column_verified'] or snapshot['same_document'])
        self.assertEqual(validate_basic_snapshot(snapshot),snapshot)
        for mutate in (lambda x:x.update(provenance='live'),lambda x:x['coverage'].update(full_map_amounts_verified=True),
                       lambda x:x['chart_regions'][1].update(amount=0),lambda x:x['levels'][0].update(share_pct=99)):
            bad=deepcopy(snapshot);mutate(bad)
            with self.assertRaises(StageFailure):validate_basic_snapshot(bad)

    def test_dom_reference_preserves_precision_separate_from_axis_tag(self):
        market,args=live_inputs()
        market.update(raw_last_price='100.25',display_values={'raw_last_price':['$100.25']})
        snapshot=build_basic_snapshot(market,**args)
        self.assertEqual(snapshot['raw_last_price'],'100')
        self.assertEqual(snapshot['source_evidence']['sidebar_reference'],'100.25')
        self.assertEqual(validate_basic_snapshot(snapshot),snapshot)

    def test_json_numeric_semantics_survive_transport_but_booleans_do_not(self):
        sample=live_sample()
        equivalent=json.loads(json.dumps(sample),parse_float=lambda x:int(float(x)) if float(x).is_integer() else float(x))
        self.assertIs(validate_basic_snapshot(equivalent),equivalent)
        equivalent['coverage']['duplicates_removed']=False
        with self.assertRaises(StageFailure):validate_basic_snapshot(equivalent)

    def test_inconsistent_open_or_realized_totals_and_duplicate_native_bin(self):
        for key,bad in [('raw_total','87.0M'),('raw_realized_longs','75.0K'),('raw_above_pct','80%')]:
            market,args=live_inputs();market[key]=bad;args['end_observation'][key]=bad
            with self.subTest(key=key),self.assertRaises(StageFailure):build_basic_snapshot(market,**args)
        market,args=live_inputs();args['chart_regions'].append(dict(side='above',colour='yellow',prices=[110.01]))
        args['end_observation']['chart_regions']=3
        with self.assertRaises(StageFailure):build_basic_snapshot(market,**args)

    def test_displayed_column_period_limits_latest_bucket_across_utc_and_israel_midnight(self):
        instant=datetime(2027,1,1,0,2,tzinfo=timezone.utc)
        at=iso(source_clock('00:02',instant))
        self.assertEqual(iso(source_clock('12/31 23:59',instant,column=True)),'2026-12-31T23:59:00Z')
        local=datetime(2026,11,27,0,1,tzinfo=ZoneInfo('Asia/Jerusalem'))
        self.assertEqual(iso(source_clock('21:58',local.astimezone(timezone.utc))),'2026-11-26T21:58:00Z')
        for col,ok in [('2026-12-31T23:59:00Z',True),('2026-12-31T23:57:00Z',True),
                       ('2026-12-31T23:56:00Z',False),('2027-01-01T00:03:00Z',False)]:
            market,args=live_inputs();market['source_time']=at
            args.update(source_time_after=at,column_time=col,column_time_after=col,captured_at=at)
            args['end_observation'].update(source_time=at,column_time=col)
            with self.subTest(column=col):
                if ok:self.assertEqual(build_basic_snapshot(market,**args)['column_time'],col)
                else:
                    with self.assertRaises(StageFailure) as caught:build_basic_snapshot(market,**args)
                    self.assertEqual(caught.exception.basic_reason,'column_outside_displayed_period')

    def test_actual_sidebar_duplicates_compacted_by_dom_reader_are_counted_on_restart(self):
        market,args=live_inputs()
        dup=[['110','10.0M','+10.00%'],['90','10.0M','-10.00%'],['110','$10.00M','+10.0%']]
        for obs in [market,args['end_observation']]:obs.update(raw_sidebar_rows=deepcopy(dup),duplicate_sidebar_levels=1)
        source=build_basic_snapshot(market,**args);self.assertEqual(source['coverage']['duplicates_removed'],1)
        self.assertEqual((source['coverage']['verified_amount'],len(source['source_evidence']['sidebar_rows'])),(20_000_000,3))
        self.assertEqual(validate_basic_snapshot(source),source)
        market['raw_sidebar_rows'][-1][1]='11.0M'
        with self.assertRaises(StageFailure):build_basic_snapshot(market,**args)

    def test_real_zero_observed_side_and_realized_amount_are_numerically_valid(self):
        market,args=live_inputs()
        market.update(raw_above_total='100M',raw_below_total='0',raw_above_pct='100%',raw_below_pct='0%',
                      raw_realized_total='35.0K',raw_realized_longs='0',raw_realized_shorts='35.0K')
        market['sidebar_levels'][1]['raw_value']='0';args['end_observation'].update(market,raw_last_price='100')
        source=build_basic_snapshot(market,**args)
        self.assertEqual(source['parsed_amounts']['realized_longs'],0)
        self.assertEqual(source['side_percent'],{'above':100,'below':0})
        self.assertEqual((source['chart_regions'][0]['amount'],source['chart_regions'][0]['share_pct']),(0,0))
        market['raw_below_total']=None;args['end_observation']['raw_below_total']=None
        with self.assertRaises(StageFailure) as caught:build_basic_snapshot(market,**args)
        self.assertEqual(caught.exception.basic_reason,'below_total_missing')

    def test_explicit_empty_denominator_never_converted_to_absent_or_false_zero_percent(self):
        for total,above,largest,reason in [(None,'0',None,'total_missing'),('0','0',None,'printed_zero_total_percent_undefined'),
                                   ('0','55M','20M','printed_zero_total_conflicts_with_sides'),
                                   ('0','0','3M','printed_zero_total_conflicts_with_levels')]:
            market,args=live_inputs();market.update(raw_total=total,raw_above_total=above,raw_below_total='0',raw_largest_value=largest,sidebar_levels=[])
            args['end_observation'].update(market)
            with self.subTest(total=total,above=above),self.assertRaises(StageFailure) as caught:
                build_basic_snapshot(market,**args)
            self.assertEqual(caught.exception.basic_reason,reason)

    def test_tied_rounded_maxima_and_named_source_max_price(self):
        market,args=live_inputs();market['sidebar_levels'].append(dict(raw_price='115',raw_value='10.0M',raw_distance_pct='+15.00%'))
        market['raw_largest_price']='110';args['end_observation'].update(market,sidebar_rows=3,raw_last_price='100')
        result=build_basic_snapshot(market,**args)
        self.assertEqual(result['strongest_observed_prices'],{'above':['110','115'],'below':['90']})
        market['sidebar_levels'][0]['raw_value']='8.0M'
        with self.assertRaises(StageFailure):build_basic_snapshot(market,**args)

    def test_unprinted_global_max_stays_unknown_despite_verified_sidebar_and_regions(self):
        market,args=live_inputs();market['raw_largest_value']=None;args['end_observation']['raw_largest_value']=None
        source=build_basic_snapshot(market,**args)
        self.assertIsNone(source['raw_largest_value']);self.assertIsNone(source['parsed_amounts']['largest_value'])
        self.assertEqual(source['strongest_observed_prices'],{'above':['110'],'below':['90']})
        self.assertEqual(source['coverage']['regions_with_amount'],1)
        self.assertEqual(validate_basic_snapshot(source),source)
        market['raw_largest_value']='0';args['end_observation']['raw_largest_value']='0'
        with self.assertRaises(StageFailure):build_basic_snapshot(market,**args)
        for row in market['sidebar_levels']:row['raw_value']='0'
        source=build_basic_snapshot(market,**args)
        self.assertEqual((source['raw_largest_value'],source['parsed_amounts']['largest_value']),('0',0))

    def test_native_group_cannot_cross_or_touch_reference_price(self):
        for rates in ([95,100,105],[100,105],[99,104]):
            market,args=live_inputs();args['chart_regions']=[dict(side='above',colour='green',prices=rates)]
            args['end_observation']['chart_regions']=1
            with self.subTest(prices=rates),self.assertRaises(StageFailure) as caught:build_basic_snapshot(market,**args)
            self.assertEqual(caught.exception.reason,'side_conflicts_with_price')
