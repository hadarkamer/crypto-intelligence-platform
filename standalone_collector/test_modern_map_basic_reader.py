"""Exercise the real BTC entry point with synthetic UI I/O and live closing checks."""
from contextlib import ExitStack, contextmanager
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from io import BytesIO
from unittest import TestCase
from unittest.mock import Mock, patch

from PIL import Image, ImageDraw
from model1_execution import StageFailure
import modern_map_basic as basic
import modern_map_reader as ui


class BasicSourceReader(TestCase):
    def replay(self, *, fail_hover=False, conflicting_hover=False, change=None, native=False, box_case=None):
        clock=datetime.now(timezone.utc).replace(second=0,microsecond=0)
        stamp=clock.isoformat().replace('+00:00','Z')
        token=clock.strftime('%m/%d %H:%M')
        prev=(clock-timedelta(hours=2)).strftime('%m/%d %H:%M')
        fmt=lambda p:f'{p:.2f}'
        rail=[{'raw_price':fmt(p),'raw_value':v,'raw_distance_pct':f'{p-100:+.2f}%'}
              for p,v in ((103,'25M'),(97,'20M'),(104,'10M'))]
        side=dict(source_time=stamp,raw_price_step='1.00',raw_column_period='5m',raw_total='100M',
                  raw_above_total='60M',raw_below_total='40M',raw_realized_total='5.0M',
                  raw_realized_longs='3.0M',raw_realized_shorts='2.0M',raw_largest_value='25M',
                  raw_above_pct='60%',raw_below_pct='40%',raw_last_price=None,raw_largest_price=None,
                  raw_updated_at=clock.strftime('%H:%M'),sidebar_levels=rail,
                  raw_sidebar_rows=[[r['raw_price'],r['raw_value'],r['raw_distance_pct']] for r in rail])
        final={**side}
        if change=='amount':final['raw_above_total']='65M'
        if change=='sidebar':final['raw_sidebar_rows']=final['raw_sidebar_rows'][:-1]
        if change=='source_epoch':final['source_time']=(clock+timedelta(minutes=1)).isoformat().replace('+00:00','Z')
        width,height=540,440
        box=(0,35,width,400)
        bounds=(10,49,480,400)
        where=lambda p:220-(p-100)*12
        chart=Image.new('RGB',(width,height),(15,19,23))
        draw=ImageDraw.Draw(chart)
        draw.rectangle(bounds,fill=(68,0,83))
        for p in (96,97,103):
            y=int(where(p))
            draw.rectangle((20,y-3,480,y+3),fill=(253,231,37))
        page=Mock()
        page.is_closed.return_value=False
        doc=page.evaluate_handle.return_value
        doc.evaluate.return_value=True
        page.url=ui.map_url('BTC')
        control={'Range':'12 hour','Layout':'Single chart','Lens':'Heat','Colours':'Viridis Default'}
        events=[]
        with ExitStack() as stack:
            def stub(owner,k,**opts):
                return stack.enter_context(patch.object(owner,k,**opts))
            stub(ui,'chart_box',return_value=box)
            stub(ui,'clear_cookie_notice');stub(ui,'display_ready');stub(ui,'gate');stub(ui,'select_choice')
            stub(ui,'choice',return_value=True);stub(ui,'symbol_control')
            stub(ui,'full_intensity',return_value=change!='intensity')
            stub(ui,'option_control',side_effect=lambda page,k:Mock(inner_text=lambda:control[k]))
            stub(ui,'evidence_controls',return_value=[dict(x=4,y=1,width=32,height=15)])
            @contextmanager
            def position(*_):yield box
            stub(ui,'position_chart',side_effect=position)
            stub(basic,'prepare_surface',return_value=dict(unknown=[],covered=[],eligible=0,cards=0,close_attempts=0))
            stub(ui,'market_surface',side_effect=lambda *_:chart.copy())
            stub(basic,'source_screen',side_effect=lambda *_:chart.copy())
            stub(ui,'dark_bounds',return_value=bounds)
            stub(ui,'read_paint',side_effect=[(prev,[]),(token,[]),(prev if change=='column' else token,[])])
            stub(basic,'axis_marks',return_value=[])
            scales=[None,None] if native else [('100.00',where),('101.00' if change=='reference' else '100.00',where)]
            stub(basic,'axis_scale_live',side_effect=scales)
            measured=stub(basic,'from_native_box',return_value=('100.00',where))
            stub(basic,'overview',side_effect=[side,final])
            stub(basic,'after_read_failure')
            tip=stub(basic,'read_tooltip',side_effect=(StageFailure('source_timeout') if fail_hover else None),
                     return_value={'source':'dom','rows':([{'time':token,'price':'103.00','amount':'49M','step':'1.00'}]
                                                         if conflicting_hover else []),'marks':[],
                            'raw_price_labels':['96.00'] if box_case in ('witness','wrong_witness') else []})
            def one_bin(page, bounds, first, last, *, require_leader):
                from decimal import Decimal
                from modern_map_measure_panel import PanelMeasurement
                if box_case in (None,'timeout'):raise StageFailure('source_timeout')
                selected=round(100+((220-(first+last)/2)/12))
                if selected==103:
                    self.assertFalse(require_leader)
                    value='16M' if box_case=='wrong_witness' else '25M'
                else:
                    self.assertEqual(selected,96)
                    self.assertEqual(require_leader,box_case not in ('witness','wrong_witness'))
                    value='8M'
                raw=fmt(selected)
                leader={'raw_price':raw,'raw_value':value} if require_leader else None
                low,high=('92.00','100.00') if box_case=='wide' else (fmt(selected-.30),fmt(selected+.30))
                return PanelMeasurement(('100.00',value,leader,Decimal(low),Decimal(high)),(low,high))
            extra=stub(ui,'measure_box',side_effect=one_bin)
            output=BytesIO()
            result=ui.read_selected_map(page,'BTC',1,'12H',output,observe=lambda k,v:events.append((k,v)))
            self.assertEqual(tip.call_count,1)
            self.assertEqual(extra.call_count,2 if box_case in ('witness','wrong_witness') else 1)
            self.assertEqual(measured.call_count,1 if native else 0)
            self.assertEqual(result['source_sha256'],sha256(output.getvalue()).hexdigest())
            self.assertEqual(doc.dispose.call_count,1)
            self.assertEqual(sum(k=='png' for k,_ in events),1)
            self.assertTrue(any(k=='stage' and v['name']=='basic_end_observation' and v['same_market_labels']
                                for k,v in events))
            return result

    def test_normal_capture_retains_unpriced_regions_and_weak_sidebar_rows(self):
        result=self.replay(fail_hover=True)
        self.assertEqual(result['result_type'],'verified_basic')
        self.assertEqual((result['coverage']['sidebar_count'],result['coverage']['unpriced_bins']),(3,1))
        self.assertTrue(any(r['amount'] is None for r in result['chart_regions']))

    def test_conflicting_optional_cell_does_not_replace_verified_sidebar(self):
        result=self.replay(conflicting_hover=True)
        self.assertEqual(result['collection_tier'],'basic')
        self.assertEqual(next(r['raw_value'] for r in result['levels'] if r['raw_price']=='103.00'),'25M')

    def test_domain_reference_preserves_targeted_enrichment_budget(self):
        self.assertEqual(self.replay(native=True)['coverage']['sidebar_count'],3)

    def test_single_target_measure_enriches_after_incomplete_hover(self):
        for case in ('leader','witness'):
            with self.subTest(case=case):
                result=self.replay(box_case=case)
                self.assertEqual(result['collection_tier'],'enriched')
                self.assertEqual(result['coverage']['unpriced_bins'],0)
                member=next(item for item in result['levels'] if item['raw_price']=='96.00')
                self.assertEqual((member['raw_value'],member['source']),('8M','measure'))
        for case in ('wide','wrong_witness','timeout'):
            with self.subTest(case=case):
                self.assertEqual(self.replay(box_case=case)['coverage']['unpriced_bins'],1)

    def test_inconsistent_closing_observations_and_changed_settings_fail(self):
        for change in ('amount','sidebar','source_epoch','column','reference','intensity'):
            with self.subTest(change=change),self.assertRaises(StageFailure):
                self.replay(change=change)

    def test_obstruction_only_disqualifies_current_edge(self):
        self.assertTrue(basic.edge_open({'unknown':[],'covered':[(0,60,40,100)]},
                                       (10,30,100,200),(0,0,150,250)))
        self.assertFalse(basic.edge_open({'unknown':[(95,150,110,180)],'covered':[]},
                                        (10,30,100,200),(0,0,150,250)))
