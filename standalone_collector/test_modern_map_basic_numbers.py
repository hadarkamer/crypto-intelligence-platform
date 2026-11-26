"""Numerical BASIC contract. All inputs here are local synthetic observations."""
from copy import deepcopy
import json
from pathlib import Path
import unittest
from model1_execution import StageFailure
from modern_map_basic_numbers import SOURCE_URL, build_basic_snapshot,validate_basic_snapshot

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
