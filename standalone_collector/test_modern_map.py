"""Offline replay of sanitized market evidence. No CoinGlass or provider calls."""
from datetime import datetime,timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import unittest

from PIL import Image

from modern_map_evidence import (equivalent, measurement, observation, sample_groups, source_clock,
                                 validate_groups, verified_member)
from modern_map_reader import GATE, map_url
from modern_map_task import normalized
from model1_execution import StageFailure

FIXTURE=Path(__file__).parent/'fixtures/btc_12h_1211_edge.png'


def frozen_result():
    # Only the eight right-edge chart pixels per row are retained. The original
    # native chart was 2976 × 2133; crop origin is x=2735, y=145. The centre
    # anchors correspond to the two measured yellow bands in the source PNG.
    with Image.open(FIXTURE) as chart:
        relative_y=lambda p:986.2-145+(60908-p)*(1432.6-986.2)/(60908-59132)
        _,runs=sample_groups(chart,(0,0,7,chart.height-1),1,60908,49.33,relative_y,60036)
    observed={60908:('60,908','86.3M'),59132:('59,132','69.1M'),61500:('61,500','51.0M'),61549:('61,549','40.1M')}
    groups=[]
    for colour,rows in runs:
        members=[]
        for row in rows:
            price,value=observed[round(row['price'])]
            member={'raw_price':price,'raw_value':value}
            verified_member(row,member,49.33)
            members.append(member)
        groups.append(dict(side=rows[0]['side'],colour=colour,members=members,
                           raw_total=members[0]['raw_value'] if len(members)==1 else '91.1M'))
    source='2026-11-24T12:11:00Z'
    # The fixture's machine timestamps and vision verdicts are synthetic.
    column='2026-11-24T12:05:00Z'
    snapshot=dict(schema='coinglass.right-edge-measure.v1',source_url=map_url('BTC'),symbol='BTC',heatmap_model=1,
                  timeframe='12H',mode='Coin',lens='Heat',palette='Viridis',theme='dark',intensity=[0,100],count='Right edge',
                  source_time=source,source_time_after=source,column_time=column,column_time_after=column,
                  captured_at='2026-11-24T12:11:15Z',raw_last_price='60,036',raw_price_step='49.33',raw_above_total='822.5M',
                  raw_below_total='471.1M',groups=groups)
    vision=dict(model='offline-fixture',observed_symbol='BTC',observed_model='Model 1',observed_timeframe='12 hour',
                observed_mode='Coin',readable=True,blocking_condition='none',short_summary='offline fixture',
                prominent_levels=[dict(candidate_id=f'c{i}',right_edge_visible=True,strength_score=.8) for i in range(len(groups))])
    return normalized(vision,snapshot,'53186e52-39e1-43b8-b4a2-06712afb6704',FIXTURE.read_bytes())


class ModernMapTests(unittest.TestCase):
    def test_frozen_pixel_members_and_measured_values(self):
        result=frozen_result()
        self.assertEqual(result['observed_price'],60036)
        self.assertNotIn('hover_snapshot',result)
        self.assertEqual([(z['price_low'],z['price_high'],z['intensity']) for z in result['zones']],
                         [(60908,60908,'many'),(61500,61549,'normal'),(59132,59132,'many')])
        self.assertEqual([g['raw_total'] for g in result['measure_snapshot']['groups']],['69.1M','86.3M','91.1M'])

    def test_reject_noncontiguous_and_mismatched_totals(self):
        groups=frozen_result()['measure_snapshot']['groups']
        args=(Decimal(60036),'49.33','822.5M','471.1M')
        groups[-1]['members'][1]['raw_price']='61,599'
        with self.assertRaises(StageFailure):validate_groups(groups,*args)
        groups[-1]['members'][1]['raw_price']='61,549'
        groups[-1]['raw_total']='91.8M'
        with self.assertRaises(StageFailure):validate_groups(groups,*args)

    def test_readonly_observation_clock_and_gate(self):
        at=datetime(2026,1,1,0,4,tzinfo=timezone.utc)
        self.assertEqual(source_clock('12/31 23:59',at,column=True).year,2025)
        with self.assertRaises(StageFailure):source_clock('21:04',at)
        with self.assertRaises(StageFailure):source_clock('11/99 00:00',at,column=True)
        self.assertIsNone(GATE.search('Log in'))
        self.assertIsNotNone(GATE.search('Sign in to view the liquidation heatmap'))
        with self.assertRaises(ValueError):map_url('BTC/ETH')
        got=observation('Updated 00:03\nOpen liquidation levels (total) 1.29B\nLongs (below price) 471.1M 36%\n'
                        'Shorts (above price) 822.5M\nPrice step / column period 49.33 / 5m',at)
        self.assertEqual(got['raw_above_total'],'822.5M')
        with self.assertRaises(StageFailure):equivalent('1.29B',['47.1M','822.5M'])
        with self.assertRaises(StageFailure):verified_member({'price':61_500},{'raw_price':'61,549','raw_value':'40.1M'},49.33)

    def test_adjacent_side_amount_and_percentage(self):
        at = datetime(2026, 11, 26, 11, 59, tzinfo=timezone.utc)
        # The shorts cell is the recorded live fragment. The longs amount
        # below is an independent test value for the existing sum check.
        def read_side(shorts, longs='530M52%'):
            return observation(
                'Open liquidation levels (total) 1.01B\n'
                f'Longs (below price) {longs}\nShorts (above price) {shorts}\n'
                'Price step / column period 45.69 / 5m\nUpdated 11:59', at)

        for raw in ('479.9M48%', '479.9M 48%', '479.9M'):
            with self.subTest(raw=raw):
                totals = read_side(raw)
                self.assertEqual(totals['raw_above_total'], '479.9M')
                self.assertEqual(totals['raw_below_total'], '530M')
        for raw in ('479.948%', '479.9M148%', '479.9M100.1%', '479.9M48%%',
                    '479.9M48', '479.9M48%other'):
            with self.subTest(raw=raw), self.assertRaises(StageFailure):
                read_side(raw)
        with self.assertRaises(StageFailure):
            read_side('479.9M48%', '12M52%')

    def test_poll_reports_last_unreadable_side(self):
        from model1_execution import failure_detail
        from modern_map_reader import wait_for

        at = datetime(2026, 11, 26, 11, 59, tzinfo=timezone.utc)
        for shorts, longs, reason in (
            ('bad', '530M52%', 'map_shorts_amount_unreadable'),
            ('479.9M48%', 'bad', 'map_longs_amount_unreadable'),
        ):
            text = f'Shorts (above price) {shorts}\nLongs (below price) {longs}'
            with self.subTest(reason=reason), self.assertRaises(StageFailure) as caught:
                wait_for(None, lambda: observation(text, at), timeout=0, requested=False)
            self.assertEqual(caught.exception.code, 'source_not_readable')
            self.assertEqual(failure_detail(caught.exception)['validation_reason'], reason)

    def test_position_entire_chart_after_measured_bottom_overflow(self):
        from unittest.mock import Mock, patch
        from modern_map_reader import position_chart, market_surface

        page = Mock()
        page.viewport_size = {'width': 1440, 'height': 1280}
        # Captured viewport and canvas coordinates from the Firefox BTC pilot.
        observed = (17, 495, 1074, 800)
        shown = (17, 480, 1074, 800)
        with patch('modern_map_reader.chart_box', side_effect=[observed, shown]):
            guard = Mock()
            with position_chart(page, guard) as box:
                self.assertEqual(box, shown)
            self.assertEqual(guard.call_count, 2)
            self.assertEqual(page.evaluate_handle.call_args.args[1], 15)
            page.evaluate_handle.return_value.evaluate.assert_called_once()
            page.evaluate_handle.return_value.dispose.assert_called_once()
        page.reset_mock()
        with patch('modern_map_reader.chart_box', return_value=shown):
            with position_chart(page) as box:
                self.assertEqual(box, shown)
            page.evaluate_handle.assert_not_called()
        # A scroll which fails to expose the last 15 rows cannot authorize a partial read.
        with patch('modern_map_reader.chart_box', return_value=observed):
            with self.assertRaises(StageFailure) as caught, position_chart(page) as unchanged:
                market_surface(page, unchanged, [], None)
            self.assertEqual(caught.exception.reason, 'map_clip_outside_viewport')
            page.evaluate_handle.return_value.evaluate.assert_called_once()
            page.evaluate_handle.return_value.dispose.assert_called_once()
        page.reset_mock()
        oversized = (17, 495, 1074, 1300)
        with patch('modern_map_reader.chart_box', return_value=oversized):
            with position_chart(page) as box:
                self.assertEqual(box, oversized)
            page.evaluate_handle.assert_not_called()

    def test_market_surface_reports_only_bounded_clip_geometry(self):
        from contextlib import redirect_stdout
        from io import BytesIO, StringIO
        from tempfile import TemporaryDirectory
        from unittest.mock import Mock, patch
        from model1_execution import failure_detail, read_failure
        from modern_map_reader import market_surface

        page = Mock()
        page.viewport_size = {'width': 80, 'height': 60}
        with BytesIO() as data:
            with Image.new('RGB', (40, 20)) as sample:
                sample.save(data, 'PNG')
            png = data.getvalue()
        for box, controls, reason, expected in (
            ((0, 0, 40, 20), [None], 'map_clip_missing', {'rect_index': 1}),
            ((0, 0, 40, 20), [dict(x=-.2, y=1, width=1, height=1)], 'map_clip_outside_viewport',
             {'rect_index': 1, 'css_edges': [-1, 1, 1, 2]}),
            ((0, 0, 20, 10), [], 'map_clip_size_mismatch',
             {'rect_index': 0, 'css_edges': [0, 0, 20, 10], 'image_size': [40, 20]}),
        ):
            with self.subTest(reason=reason), patch('modern_map_reader.protected_clip', return_value=png):
                with self.assertRaises(StageFailure) as caught:
                    market_surface(page, box, controls, None)
                detail = failure_detail(caught.exception)
                self.assertEqual(detail['validation_reason'], reason)
                self.assertEqual(detail['market_clip'], {'viewport': [80, 60], **expected})
        with TemporaryDirectory() as folder, redirect_stdout(StringIO()) as printed:
            error_file = Path(folder)/'error.json'
            detail['market_clip']['private_text'] = 'do-not-export'
            error_file.write_text(json.dumps({'code': 'source_not_readable', **detail}))
            read_failure(error_file, 'fixture', 1)
            self.assertNotIn('do-not-export', printed.getvalue())
            self.assertIn('"image_size": [40, 20]', printed.getvalue())
        page.viewport_size = None
        with self.assertRaises(StageFailure) as caught:
            market_surface(page, (0, 0, 20, 10), [], None)
        self.assertEqual(caught.exception.reason, 'map_viewport_unavailable')

    def test_market_notice_blocks_even_a_painted_right_edge_time(self):
        from unittest.mock import Mock, patch
        from modern_map_reader import read_paint, read_selected_map

        page = Mock()
        notice = Mock()
        notice.count.return_value = 1
        notice.nth.return_value.is_visible.return_value = True
        time_labels = Mock()
        time_labels.count.return_value = 0
        page.get_by_text.side_effect = lambda rx: notice if 'essential cookies' in rx.pattern else time_labels
        decline = page.locator.return_value.get_by_role.return_value
        decline.count.return_value = 0
        with patch('modern_map_reader.display_ready') as navigate:
            with self.assertRaises(StageFailure) as blocked:
                read_selected_map(page, 'BTC', 1, '12H', None)
            self.assertEqual(blocked.exception.reason, 'hover_cookie_overlay')
            navigate.assert_not_called()
        painted = page.evaluate_handle.return_value
        painted.evaluate.return_value = [['11/26 12:08', 800, 794, 'top']]
        with patch('modern_map_reader.wait_for', side_effect=lambda p, read, **kw: read()):
            with self.assertRaises(StageFailure) as late:
                read_paint(page, (0, 0, 1074, 800), 800, 400)
            self.assertEqual(late.exception.reason, 'hover_cookie_overlay')
            painted.dispose.assert_called_once()
        decline.press.assert_not_called()

    def test_read_native_measurement(self):
        last,total,leader,lo,hi=measurement('Box total · 61,418 → 61,680 (0.44%)\n91.1M\n'
                                           'Largest cluster\n61,500 · 51.0M\nReference (last price)\n60,036')
        self.assertEqual((last,total,leader['raw_price'],lo,hi),('60,036','91.1M','61,500',61418,61680))


class BrowserReplay(unittest.TestCase):
    def test_rendered_controls_canvas_measure_and_capture(self):
        import shutil
        import socket
        import tempfile
        from playwright.sync_api import sync_playwright
        from modern_map_reader import read_map
        from unittest.mock import Mock,patch
        try:
            with socket.socket(socket.AF_UNIX):pass
        except PermissionError:self.skipTest('Host disallows the local socket required by Chromium')
        with sync_playwright() as pw:
            bundled = Path(pw.chromium.executable_path)
            binary = str(bundled) if bundled.is_file() else shutil.which('chromium')
            # The hosted Ubuntu runner ships a sandbox profile for its signed
            # Chrome package. This selector applies only to the offline replay.
            if os.environ.get('COINGLASS_QA_USE_SYSTEM_CHROME') == '1':
                binary = '/opt/google/chrome/chrome'
                self.assertTrue(Path(binary).is_file(), 'CI system Chrome is missing')
            if not binary:self.skipTest('Chromium is required for offline browser replay')
            browser=pw.chromium.launch(executable_path=binary,headless=True,chromium_sandbox=True,args=['--disable-dev-shm-usage'])
            try:
                page=browser.new_page(viewport={'width':1440,'height':1280},timezone_id='UTC')
                url=map_url('BTC')
                calls=[]
                def route(req):
                    calls.append(req.request.url)
                    if req.request.url==url:
                        req.fulfill(content_type='text/html; charset=utf-8',body=(FIXTURE.parent/'modern_chart.html').read_bytes())
                    else:req.abort()
                page.route('**/*',route)
                with tempfile.TemporaryDirectory() as out, patch.dict('sys.modules',{'session_capture':Mock()}):
                    path=Path(out)/'map.png'
                    got=read_map(page,'BTC',1,'12H',path)
                    self.assertEqual(calls,[url])
                    self.assertEqual(got['raw_last_price'],'105')
                    self.assertEqual([g['raw_total'] for g in got['groups']],['15M','27M','30M'])
                    self.assertEqual(got['source_time'],got['source_time_after'])
                    self.assertEqual(got['column_time'],got['column_time_after'])
                    self.assertTrue(path.read_bytes().startswith(b'\x89PNG'))
            finally:
                browser.close()


class ReaderReplay(unittest.TestCase):
    """Exercise read_map through file/evidence generation, with UI I/O simulated."""
    def replay(self, changed=False, tooltip=False, buffered=False, covered=False):
        import io
        import tempfile
        from contextlib import ExitStack
        from datetime import timedelta
        from unittest.mock import Mock,patch
        from PIL import ImageDraw
        import modern_map_reader as reader
        page=Mock()
        page.get_by_text.return_value.count.return_value=0
        page.locator.return_value.get_by_role.return_value.count.return_value=0
        page.url=map_url('BTC')
        clock=datetime.now(timezone.utc).replace(second=0,microsecond=0)
        data={'source_time':clock.isoformat().replace('+00:00','Z'),'raw_price_step':'1',
              'raw_above_total':'80M','raw_below_total':'20M'}
        chart=Image.new('RGB',(890,520),(14,19,24))
        draw=ImageDraw.Draw(chart)
        draw.rectangle((20,100,719,400),fill=(68,0,83))
        levels=[(99,15,(253,231,37)),(110,15,(53,183,121)),(111,12,(53,183,121)),(115,30,(253,231,37))]
        for price,value,col in levels:
            y=100+(120-price)*10
            draw.rectangle((480,y-4,719,y+4),fill=col)
        page.viewport_size={'width':chart.width,'height':chart.height}
        def screenshot(*,clip,animations,mask):
            x,y,w,h=(clip[k] for k in ('x','y','width','height'))
            with chart.crop((x,y,x+w,y+h)) as part:
                image=io.BytesIO();part.save(image,format='PNG')
                return image.getvalue()
        page.screenshot.side_effect=screenshot
        def measure(page,bounds,upper,lower):
            lo,hi=(120-(v-100)/10 for v in (lower,upper))
            points=[(p,v) for p,v,_ in levels if lo<=p<=hi]
            p,v=max(points,key=lambda pair:pair[1])
            total=100 if hi-lo>28 else sum(v for p,v in points)
            return '105',f'{total}M',{'raw_price':str(p),'raw_value':f'{v}M'},Decimal(str(lo)),Decimal(str(hi))
        def stamp(at):return at.strftime('%m/%d %H:%M')
        labels=[(stamp(clock-timedelta(hours=1)),[]),(stamp(clock),[]),
                (stamp(clock-timedelta(minutes=5) if changed else clock),[])]
        with tempfile.TemporaryDirectory() as folder, ExitStack() as stack:
            def stub(k,**kw):return stack.enter_context(patch.object(reader,k,**kw))
            stub('open_verified_map');stub('display_ready');stub('details',return_value=data)
            stack.enter_context(patch('modern_map_surface.prepare_surface',return_value={
                'covered':[],'unknown':[],'eligible':0,'cards':0,'close_attempts':0}))
            side=dict(data,raw_total='100M',raw_column_period='5m',raw_largest_value=None,sidebar_levels=[])
            if covered:
                side['raw_largest_value']='30M'
                side['sidebar_levels']=[{'raw_price':str(p),'raw_value':f'{v}M'} for p,v,_ in reversed(levels)]
                bank={number['raw_price']:number for number in side['sidebar_levels']}
                def coverage(screen,box,bounds,marks,overview,model,anchor,step):
                    h,groups=sample_groups(screen,bounds,model,anchor,step,lambda p:100+(120-p)*10,105)
                    return '105',h,groups,{Decimal(p):m for p,m in bank.items()},bank['115']
                stack.enter_context(patch('modern_map_axis.axis_marks',return_value=[]))
                stack.enter_context(patch('modern_map_axis.match_displayed_bins',side_effect=coverage))
            stack.enter_context(patch('modern_map_overview.overview',return_value=side))
            rows=[]
            # Supplying a complete grid tests reuse of one effective hover.
            if tooltip:
                all_prices={p:v for p,v,_ in levels}
                rows=[{'price':str(p),'amount':f'{all_prices.get(p,0)}M','time':stamp(clock)}
                      for p in range(min(all_prices),max(all_prices)+1)]
            tip={'source':'dom' if tooltip else 'unresolved','rows':rows,'marks':[]}
            hover=stack.enter_context(patch('modern_map_tooltip.read_tooltip',side_effect=lambda *a:dict(tip)))
            stub('chart_box',return_value=(20,100,750,335));stub('rail_anchor',return_value=115)
            stub('read_paint',side_effect=labels);measures=stub('measure_box',side_effect=measure)
            stub('select_choice');stub('choice',return_value=True);stub('gate')
            control_text={'Range':'12 hour','Layout':'Single chart','Lens':'Heat','Colours':'Viridis Default'}
            stub('option_control',side_effect=lambda page,key:Mock(inner_text=lambda:control_text[key]))
            stub('evidence_controls',return_value=[dict(x=20,y=10,width=50,height=20)])
            stub('symbol_control').return_value.bounding_box.return_value=dict(x=160,y=10,width=45,height=20)
            path=Path(folder)/'evidence.png'
            if buffered:
                buffer=io.BytesIO()
                events=[]
                result=reader.read_selected_map(page,'BTC',1,'12H',buffer,guard=lambda:None,
                                                observe=lambda k,v:events.append((k,v)))
                evidence=[v for k,v in events if k=='png']
                self.assertEqual(evidence,[buffer.getvalue()])
                self.assertTrue(evidence[0].startswith(b'\x89PNG'))
            else:
                result=reader.read_map(page,'BTC',1,'12H',path)
                self.assertTrue(path.read_bytes().startswith(b'\x89PNG'))
            self.assertEqual(measures.call_count,0 if covered else 1 if tooltip else 5)
            self.assertEqual(hover.call_count,0 if covered else 1 if tooltip else 2)
            return result

    def test_complete_sidebar_skips_hover_and_measure_but_still_rechecks_column(self):
        result=self.replay(covered=True,buffered=True)
        self.assertEqual([g['raw_total'] for g in result['groups']],['15M','27M','30M'])
        with self.assertRaises(StageFailure):self.replay(covered=True,changed=True)

    def test_covered_hover_grid_skips_all_per_bin_measurements(self):
        result=self.replay(tooltip=True,buffered=True)
        self.assertEqual([g['raw_total'] for g in result['groups']],['15M','27M','30M'])

    def test_raw_verification_through_saved_image(self):
        s=self.replay()
        self.assertEqual([g['raw_total'] for g in s['groups']],['15M','27M','30M'])
        self.assertEqual(s['source_time'],s['source_time_after'])
        self.assertEqual(s['column_time'],s['column_time_after'])

    def test_changing_right_edge_rejects_snapshot(self):
        with self.assertRaises(StageFailure):self.replay(changed=True)
