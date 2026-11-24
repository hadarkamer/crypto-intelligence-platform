"""Offline replay of sanitized market evidence. No CoinGlass or provider calls."""
from datetime import datetime,timezone
from decimal import Decimal
import json
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
            if not binary:self.skipTest('Chromium is required for offline browser replay')
            browser=pw.chromium.launch(executable_path=binary,headless=True,args=['--disable-dev-shm-usage'])
            try:
                page=browser.new_page(viewport={'width':1440,'height':1280},timezone_id='UTC')
                url=map_url('BTC')
                calls=[]
                def route(req):
                    calls.append(req.request.url)
                    if req.request.url==url:
                        req.fulfill(content_type='text/html',body=(FIXTURE.parent/'modern_chart.html').read_text())
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
    def replay(self, changed=False):
        import io
        import tempfile
        from contextlib import ExitStack
        from datetime import timedelta
        from unittest.mock import Mock,patch
        from PIL import ImageDraw
        import modern_map_reader as reader
        page=Mock()
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
        stream=io.BytesIO();chart.save(stream,format='PNG')
        page.screenshot.return_value=stream.getvalue()
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
            stub('chart_box',return_value=(20,100,750,335));stub('rail_anchor',return_value=115)
            stub('read_paint',side_effect=labels);stub('measure_box',side_effect=measure)
            stub('select_choice');stub('choice',return_value=True);stub('gate')
            stub('option_control').return_value.inner_text.return_value='12 hour'
            stub('evidence_controls',return_value=[dict(x=20,y=10,width=50,height=20)])
            stub('symbol_control').return_value.bounding_box.return_value=dict(x=160,y=10,width=45,height=20)
            path=Path(folder)/'evidence.png'
            result=reader.read_map(page,'BTC',1,'12H',path)
            self.assertTrue(path.read_bytes().startswith(b'\x89PNG'))
            return result

    def test_raw_verification_through_saved_image(self):
        s=self.replay()
        self.assertEqual([g['raw_total'] for g in s['groups']],['15M','27M','30M'])
        self.assertEqual(s['source_time'],s['source_time_after'])
        self.assertEqual(s['column_time'],s['column_time_after'])

    def test_changing_right_edge_rejects_snapshot(self):
        with self.assertRaises(StageFailure):self.replay(changed=True)
