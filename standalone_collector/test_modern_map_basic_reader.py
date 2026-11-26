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
    def replay(self, *, fail_hover=False, conflicting_hover=False, change=None, native=False, box_case=None,
               optional_change=None, ack=True, observed=None, base_only=False, shifted=False):
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
        page.viewport_size={'width':width+12,'height':height}
        current_box=[box]
        delta=(8,-7) if shifted else (0,0)
        closing_box=(box[0]+delta[0],box[1]+delta[1],box[2],box[3])
        closing_bounds=tuple(v+delta[i%2] for i,v in enumerate(bounds))
        control={'Range':'12 hour','Layout':'Single chart','Lens':'Heat','Colours':'Viridis Default'}
        events=[] if observed is None else observed
        with ExitStack() as stack:
            def stub(owner,k,**opts):
                return stack.enter_context(patch.object(owner,k,**opts))
            stub(ui,'chart_box',side_effect=lambda _:current_box[0])
            stub(ui,'clear_cookie_notice');stub(ui,'display_ready');stub(ui,'gate');stub(ui,'select_choice')
            stub(ui,'choice',return_value=True);stub(ui,'symbol_control')
            stub(ui,'full_intensity',return_value=change!='intensity')
            stub(ui,'option_control',side_effect=lambda page,k:Mock(inner_text=lambda:control[k]))
            stub(ui,'evidence_controls',return_value=[dict(x=4,y=1,width=32,height=15)])
            @contextmanager
            def position(*_):
                prior=current_box[0]
                if positions:current_box[0]=closing_box
                positions.append(current_box[0])
                try:yield current_box[0]
                finally:current_box[0]=prior
            positions=[]
            stub(ui,'position_chart',side_effect=position)
            clean=dict(unknown=[],covered=[],eligible=0,cards=0,close_attempts=0)
            obstructed=dict(clean,unknown=[(452,403,532,432)]) if change=='end_time_overlay' else clean
            stub(basic,'prepare_surface',side_effect=[clean,obstructed])
            stub(ui,'market_surface',side_effect=lambda *_:chart.copy())
            def closing_image(*_):
                image=Image.new('RGB',(width+12,height))
                image.paste(chart,delta)
                return image
            stub(basic,'source_screen',side_effect=closing_image)
            stub(ui,'dark_bounds',side_effect=lambda _,now:closing_bounds if now!=box else bounds)
            optional_time=prev if optional_change=='column' else token
            optional_paint=StageFailure('source_login_required') if optional_change=='auth' else (optional_time,[])
            paint=stub(ui,'read_paint',side_effect=[(prev,[]),(token,[]),(prev if change=='column' else token,[]),optional_paint])
            stub(basic,'axis_marks',return_value=[])
            scales=[None,None] if native else [('100.00',where),('101.00' if change=='reference' else '100.00',where)]
            if shifted and not native:scales[-1]=('100.00',lambda p:where(p)+delta[1])
            scales.append(scales[-1])
            stub(basic,'axis_scale_live',side_effect=scales)
            measured=stub(basic,'from_native_box',return_value=('100.00',where))
            later=dict(final, source_time=(clock+timedelta(minutes=1)).isoformat().replace('+00:00','Z')) if optional_change=='epoch' else final
            stub(basic,'overview',side_effect=[side,final,later])
            stub(basic,'after_read_failure')
            tip=stub(basic,'read_tooltip',side_effect=(StageFailure('source_timeout') if fail_hover else None),
                     return_value={'source':'dom','rows':([{'time':token,'price':'103.00','amount':('49M' if conflicting_hover is True else conflicting_hover),'step':'1.00'}]
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
            def collect(k,v):
                events.append((k,v))
                if k=='verified_basic':
                    self.assertEqual(v['collection_tier'],'basic')
                    self.assertTrue(v['end_reference_verified'] and v['end_column_verified'])
                    self.assertEqual(v['source_sha256'],sha256(output.getvalue()).hexdigest())
                    tip.assert_not_called()
                    extra.assert_not_called()
                    return ack
            result=ui.read_selected_map(page,'BTC',1,'12H',output,observe=collect,base_only=base_only)
            self.assertEqual(sum(k=='verified_basic' for k,_ in events),1)
            self.assertEqual(paint.call_count,4 if not base_only and box_case in ('leader','witness') else 3)
            self.assertEqual(tip.call_count,0 if base_only else 1)
            self.assertEqual(extra.call_count,0 if base_only else 2 if box_case in ('witness','wrong_witness') else 1)
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
        events=[]
        result=self.replay(conflicting_hover=True,observed=events)
        self.assertTrue(any(k=='stage' and v.get('condition')=='optional_overlap_amount_conflict' for k,v in events))
        self.assertEqual(result['collection_tier'],'basic')
        self.assertEqual(next(r['raw_value'] for r in result['levels'] if r['raw_price']=='103.00'),'25M')

    def test_matching_optional_duplicate_rounds_within_native_precision_without_new_close(self):
        events=[]
        result=self.replay(conflicting_hover='25.3M',observed=events)
        self.assertEqual(result['collection_tier'],'basic')
        self.assertTrue(any(k=='stage' and v.get('name')=='optional_overlap_only' and v['count']==1
                            for k,v in events))
        self.assertFalse(any(k=='stage' and v.get('name','').startswith('enrichment_recheck_') for k,v in events))

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

    def test_previously_closed_basic_survives_newer_epoch_or_column_during_enrichment(self):
        for changed in ('epoch','column'):
            with self.subTest(changed=changed):
                events=[]
                result=self.replay(native=True,box_case='leader',optional_change=changed,observed=events)
                self.assertEqual(result,next(v for k,v in events if k=='verified_basic'))
                self.assertEqual(result['enrichment']['additional_verified_count'],0)
                self.assertEqual(result['source_time'],result['source_time_after'])
                self.assertTrue(any(k=='stage' and v['name']=='enrichment_discarded' for k,v in events))

    def test_no_private_base_release_before_valid_close_and_security_errors_propagate(self):
        for failure in ('column','source_epoch','reference'):
            events=[]
            with self.subTest(failure=failure),self.assertRaises(StageFailure):
                self.replay(change=failure,observed=events)
            self.assertFalse(any(k=='verified_basic' for k,_ in events))
        with self.assertRaises(StageFailure) as failed:
            self.replay(ack=False)
        self.assertEqual(failed.exception.code,'state_store_failed')
        with self.assertRaises(StageFailure) as denied:
            self.replay(box_case='leader',optional_change='auth')
        self.assertEqual(denied.exception.code,'source_login_required')

    def test_closing_crosshair_requires_fresh_independent_visible_time(self):
        page=Mock()
        column=datetime(2026,11,26,16,5,tzinfo=timezone.utc)
        args=page,(0,0,1074,850),(0,50,999,823),column,Mock(),Mock()
        # F returned no bottom label and no tooltip marks. Unchanged source
        # time 16:08 cannot supply a missing latest column of 16:05.
        empty={'source':'unresolved','marks':[],'price_label_source':None,'raw_price_labels':[]}
        with patch.object(ui,'read_paint',side_effect=StageFailure('source_not_readable')), \
             patch.object(basic,'read_tooltip',return_value=empty) as tip:
            with self.assertRaises(StageFailure) as failed:
                basic.displayed_column(*args)
            self.assertEqual(failed.exception.basic_reason,'closing_column_display_unavailable')
            base=dict(price_label_source='dom',raw_price_labels=['60,495','60,582','60,669'])
            # These calls model a new tooltip whose parser already verified its
            # dated title, regularly spaced times and row labels (separate tests).
            tip.return_value=dict(base,marks=[('11/26 16:05',640,680)])
            self.assertEqual(basic.displayed_column(*args),('11/26 16:05',[]))
            for marks in [[('11/26 16:00',640,680)], [('11/26 16:05',640,680),('11/26 16:10',640,681)]]:
                tip.return_value=dict(base,marks=marks)
                with self.assertRaises(StageFailure):basic.displayed_column(*args)
        with patch.object(ui,'read_paint',side_effect=StageFailure('source_login_required')), \
             patch.object(basic,'read_tooltip') as tip, self.assertRaises(StageFailure):
            basic.displayed_column(*args)
        tip.assert_not_called()

    def test_base_only_persists_then_stops_with_unpriced_areas(self):
        events=[]
        result=self.replay(base_only=True,box_case='leader',observed=events)
        self.assertEqual('basic',result['collection_tier'])
        self.assertEqual(1,result['coverage']['unpriced_bins'])
        self.assertEqual(events[-1],('verified_basic',result))
        self.assertFalse(any('optional' in v.get('name','') for k,v in events if k=='stage'))
        self.assertTrue(any(v['name']=='basic_closing_geometry' for k,v in events if k=='stage'))

    def test_base_only_repositions_from_measured_same_chart_after_translation(self):
        for native in (False,True):
            events=[]
            result=self.replay(base_only=True,shifted=True,native=native,observed=events)
            evidence=next(v for k,v in events if k=='stage' and v['name']=='basic_closing_geometry')
            self.assertTrue(evidence['geometry_changed'])
            self.assertEqual(evidence['plot_rect'],[18,42,488,393])
            current=next(v for k,v in events if k=='stage' and v['name']=='closing_column_from_visible_crosshair')
            self.assertEqual(current['pointer_x'],487)
            self.assertEqual(result['collection_tier'],'basic')

    def test_base_only_blocked_time_axis_is_not_replaced_with_latest_estimate(self):
        events=[]
        with self.assertRaises(StageFailure) as failure:
            self.replay(change='end_time_overlay',base_only=True,observed=events)
        self.assertEqual(failure.exception.basic_reason,'closing_price_or_time_axis_obstructed')
        self.assertFalse(any(k=='verified_basic' for k,_ in events))
        self.assertTrue(any(k=='stage' and v.get('name')=='basic_closing_geometry' and v['status']=='blocked'
                            for k,v in events))

    def test_base_only_alt_timestamp_requires_dated_spatial_header_and_printed_cadence(self):
        from modern_map_tooltip import dated_column
        page=Mock()
        column=datetime(2026,11,26,15,0,tzinfo=timezone.utc)
        box=(0,0,1074,850); bounds=(0,50,999,823)
        args=(page,box,bounds,column,Mock(),Mock())
        # Rendered headers retained in the real E market receipt.
        labels=[('11/26 15:00',639,681),('14:40',634,728),('14:45',696,728),
                ('14:50',758,728),('14:55',820,728),('15:00',882,728)]
        structured=dated_column(labels,column)
        self.assertEqual(structured['period_seconds'],300)
        with patch.object(ui,'read_paint',side_effect=StageFailure('source_not_readable')), \
             patch.object(ui,'chart_box',return_value=box),patch.object(basic,'read_tooltip') as tip:
            tip.return_value={'column_evidence':[dict(structured,source='dom')]}
            self.assertEqual(basic.displayed_column(*args,base_only=True,period='5m'),('11/26 15:00',[]))
            for bad in ([],[dict(structured,dated_title='11/26 14:55')],[dict(structured,period_seconds=600)],
                        [structured,dict(structured,dated_title='11/26 14:55')]):
                tip.return_value={'column_evidence':bad}
                with self.subTest(bad=bad), self.assertRaises(StageFailure):
                    basic.displayed_column(*args,base_only=True,period='5m')
        self.assertIsNone(dated_column([],column))  # actual F closing receipt
        self.assertIsNone(dated_column(labels[:1],column))  # dated title alone
        self.assertIsNone(dated_column(labels[:-1],column))  # historical headers

    def test_base_only_does_not_select_other_job_or_change_defaults(self):
        with patch.object(ui,'read_strict_selected_map') as legacy:
            with self.assertRaises(ValueError):ui.read_selected_map(Mock(),'BTC',1,'48H',BytesIO(),base_only=True)
            legacy.assert_not_called()
            ui.read_selected_map(Mock(),'BTC',1,'48H',BytesIO())
            legacy.assert_called_once()

    def test_opening_alternate_returns_observed_time_not_source_clock(self):
        page=Mock()
        box=(0,0,1074,850);bounds=(0,50,999,823)
        source=datetime(2026,11,26,16,8,tzinfo=timezone.utc)
        args=(page,box,bounds,source,Mock(),Mock())
        with patch.object(ui,'read_paint',side_effect=StageFailure('source_not_readable')), \
             patch.object(ui,'chart_box',return_value=box),patch.object(basic,'read_tooltip') as tip:
            tip.return_value={'column_evidence':[{'dated_title':'11/26 16:05','period_seconds':300}]}
            stamp,_=basic.displayed_column(*args,base_only=True,period='5m',opening=True)
            self.assertEqual(stamp,'11/26 16:05')
            self.assertTrue(tip.call_args.kwargs['clock_only'])
            for other in ('11/26 16:00','11/26 16:10'):
                tip.return_value={'column_evidence':[{'dated_title':other,'period_seconds':300}]}
                with self.subTest(other=other), self.assertRaises(StageFailure):
                    basic.displayed_column(*args,base_only=True,period='5m',opening=True)
