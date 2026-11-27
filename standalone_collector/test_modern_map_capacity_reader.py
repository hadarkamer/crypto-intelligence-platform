"""Offline identity, price, printed source range and shared numeric contract tests."""
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
from io import BytesIO
from itertools import product
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from PIL import Image
from model1_execution import StageFailure
import modern_map_basic as basic
import modern_map_reader as reader
from modern_map_axis_ocr import axis_scale_live
from modern_map_basic_numbers import (
    BASIC_SYMBOLS, BASIC_MODELS, BASIC_RANGES, basic_source_url, basic_target,
    build_basic_snapshot, period_seconds, range_matches, validate_basic_snapshot,
)
from modern_map_evidence import number, sample_groups
from test_modern_map_basic_numbers import live_inputs


class CapacityReader(unittest.TestCase):
    def test_all_96_combinations_use_the_same_basic_reader_and_numeric_contract(self):
        # Display formats simulate independent observations. They are not
        # representations of which source combinations exist or today's prices.
        reference = dict(BTC='60,000', ETH='1,432.98', SOL='143.298', XRP='1.8462',
                         HYPE='36.78', BNB='613.75', DOGE='0.12485', ZEC='128.753')
        seen = set()
        with patch.object(basic, 'capture_basic') as capture, patch.object(reader, 'read_strict_selected_map') as alternate:
            for symbol, model, tf in product(BASIC_SYMBOLS,BASIC_MODELS,BASIC_RANGES):
                with self.subTest(target=(symbol,model,tf)):
                    self.assertTrue(basic_target(symbol,model,tf))
                    page,out=object(),BytesIO()
                    reader.read_selected_map(page,symbol,model,tf,out,base_only=True)
                    capture.assert_called_with(page,symbol,model,tf,out,guard=None,observe=None,base_only=True)
                    overview,args=live_inputs()
                    raw=reference[symbol]
                    value=number(raw)[0]
                    step=value/100
                    as_price=lambda n:format(n,'f')
                    for row,factor in zip(overview['sidebar_levels'],['1.10','.90']):
                        row['raw_price']=as_price(value*Decimal(factor))
                    overview.update(raw_price_step=as_price(step),raw_column_period='1h' if tf=='1W' else '5m')
                    label='7 days' if tf=='1W' else reader.RANGES[tf]
                    selected={'label':label,'value':'7d' if tf=='1W' else tf.lower()}
                    end=args['end_observation']
                    end.update(overview,symbol=symbol,heatmap_model=model,timeframe=tf,
                               raw_last_price=raw,range_control=selected)
                    args.update(symbol=symbol,model=model,timeframe=tf,source_url=basic_source_url(symbol),raw_last_price=raw)
                    args['chart_regions']=[dict(side='below',colour='yellow',prices=[as_price(value*Decimal('.90'))]),
                                           dict(side='above',colour='yellow',prices=[as_price(value*Decimal('1.10'))])]
                    for group in args['chart_regions']:
                        group['prices']=[float(v) for v in group['prices']]
                    s=build_basic_snapshot(overview,**args)
                    self.assertEqual(validate_basic_snapshot(s),s)
                    self.assertEqual(s['source_evidence']['closing']['range_control'],selected)
                    self.assertEqual([l['distance_pct'] for l in s['levels']],[10,-10])
                    self.assertEqual([l['share_pct'] for l in s['levels']],[10,10])
                    self.assertEqual(s['coverage']['regions_with_amount'],2)
                    self.assertEqual(s['coverage']['verified_share_pct'],20)
                    seen.add((s['symbol'],s['heatmap_model'],s['timeframe']))
            alternate.assert_not_called()
        self.assertEqual(len(seen),96)

    def test_mixed_identity_wrong_week_source_url_and_boolean_model_are_rejected(self):
        for symbol,model,tf in [('DOG',1,'12H'),('ZCASH',1,'12H'),('DOGE',True,'12H'),('BTC',2,'7D')]:
            self.assertFalse(basic_target(symbol,model,tf))
        overview,args=live_inputs()
        for k,v in [('source_url',basic_source_url('ETH')),('model',True)]:
            with self.assertRaises(StageFailure):
                build_basic_snapshot(overview,**dict(args,**{k:v}))
        for k,v in [('symbol','ETH'),('heatmap_model',3),('range_control',{'label':'7 days','value':'1W'})]:
            changed=deepcopy(args)
            changed['end_observation'][k]=v
            with self.assertRaises(StageFailure):
                build_basic_snapshot(overview,**changed)
        self.assertTrue(range_matches('1W','1 week') and range_matches('1W','7 days'))
        for s in ('1 month','2 weeks','14 days','1 week (+3d)','7 hours'):
            self.assertFalse(range_matches('1W', s))

    def test_weekly_column_uses_printed_period_crossing_midnight(self):
        observation='2026-01-01T00:02:00Z'
        stamp=basic.read_epoch('12/31 23:35',observation,'30min')
        self.assertEqual(stamp,datetime(2025,12,31,23,35,tzinfo=timezone.utc))
        with self.assertRaises(StageFailure):
            basic.read_epoch('12/31 23:31',observation,'30min')
        for printed,value in [('4h',14400),('1day',86400),('90seconds',90),('15min',900)]:
            self.assertEqual(period_seconds(printed),value)
        for invalid in ('1month','0m','five minutes'):
            with self.assertRaises(StageFailure):period_seconds(invalid)

    def test_selects_the_real_printed_week_option_and_records_its_value(self):
        # Simulate Playwright locators for a source Range portal. Values identify
        # options on that source; they need not be called "1W".
        page,menu,option,control=Mock(),Mock(),Mock(),Mock()
        state={'label':'24 hour'}
        control.inner_text.side_effect=lambda:state['label']
        control.get_attribute.return_value=None
        option.evaluate_handle.return_value.as_element.return_value=option
        option.is_disabled.return_value=False
        option.get_attribute.side_effect=lambda k:'week-7' if k=='data-value' else None
        option.click.side_effect=lambda **_:state.update(label='7 days')
        page.get_by_role.side_effect=lambda kind:[menu] if kind=='listbox' else []
        menu.get_by_text.side_effect=[[],[option]]
        menu.evaluate.return_value=False
        with patch.object(reader,'option_control',return_value=control),patch.object(reader,'visible',side_effect=list):
            result=reader.select_range(page,'1W')
        self.assertEqual(result,{'label':'7 days','value':'week-7'})
        self.assertEqual(menu.get_by_text.call_count,2)
        self.assertEqual(option.click.call_count,1)
        self.assertEqual(control.click.call_count,1)
        option.evaluate_handle.return_value.dispose.assert_called_once()
        self.assertEqual(control.dispose.call_count,2)

    def test_incomplete_menu_explicitly_disabled_and_missing_complete_range_are_distinct(self):
        page,menu,option,control=Mock(),Mock(),Mock(),Mock()
        control.inner_text.return_value='12 hour'
        page.get_by_role.side_effect=lambda kind:[menu] if kind=='listbox' else []
        option.evaluate_handle.return_value.as_element.return_value=option
        option.is_disabled.return_value=True
        for shown,complete,code in [([],False,'source_not_readable'),([],True,'source_map_unavailable'),
                                    ([option],False,'source_map_unavailable')]:
            menu.get_by_text.return_value=shown
            menu.evaluate.return_value=complete
            with self.subTest(code=code,disabled=bool(shown)),patch.object(reader,'option_control',return_value=control), \
                 patch.object(reader,'visible',side_effect=list),patch.object(reader,'gate'), \
                 self.assertRaises(StageFailure) as caught:
                reader.select_range(page,'1W')
            self.assertEqual(caught.exception.code,code)
            option.click.assert_not_called()

    def test_symbol_document_404_does_not_declare_combination_unsupported(self):
        page = Mock()
        page.url = basic_source_url('DOGE')
        page.goto.return_value.status = 404
        with self.assertRaises(StageFailure) as caught:
            reader.open_verified_map(page,'DOGE',3,'1W',guard=Mock())
        self.assertEqual(caught.exception.code,'source_navigation_changed')
        self.assertEqual(caught.exception.basic_reason,'map_document_http_404')
        page.get_by_role.assert_not_called()

    def test_decimal_grid_bounds_follow_printed_step(self):
        im=Image.new('RGB',(22,30),(67,5,84))
        for yy in (9,10,11):
            im.putpixel((19,yy),(250,230,40))
        # Binary float arithmetic would store 0.10003000000000001.
        anchor,step=0.10001,0.00001
        where=lambda p:14-2*(p-anchor)/step
        _,runs=sample_groups(im,(0,0,20,28),1,anchor,step,where,0.10015,audit={})
        self.assertTrue(runs)
        for _,rows in runs:
            for row in rows:
                self.assertEqual(Decimal(str(row['price'])) % Decimal('0.00001'),0)

    def test_packaged_ocr_decimals_and_contradictory_axis(self):
        # Market-only synthetic chart strips, in an ordinary 16px sans font.
        # The local engine reads the label and all ticks together, without
        # direct labels fed to OCR or prices inferred from other inputs.
        cases=('0.12485','1.8462','36.78','1,432.98','613.75')
        with Image.open(Path(__file__).parent/'fixtures/synthetic_decimal_axes.png') as panel:
            for i,raw in enumerate(cases):
                with self.subTest(reference=raw),panel.crop((255*i,0,255*(i+1),560)) as chart:
                    audit={}
                    result=axis_scale_live(chart,(0,0,255,560),(0,0,148,559),[],observe=audit.update)
                    self.assertEqual(audit['ocr_calls'],1)
                    if i==4 and result is None:
                        # Some labels in this crop lose dots in OCR. A rejected
                        # calibration must never be presented as a valid price.
                        self.assertIn(audit['condition'],('tick_span_insufficient','axis_position_conflict'))
                    else:
                        self.assertEqual(result[0],raw,audit)
                        self.assertEqual(audit['condition'],'verified')
