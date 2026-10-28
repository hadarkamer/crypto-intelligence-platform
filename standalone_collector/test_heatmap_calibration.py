"""Offline row-grid regression through positions, band membership and amounts.

The fixture models the observed renderer's half-pixel cell overlap and integer
mouse coordinates. Its known rows/colors are independent of the reader's fit.
The downloaded actual renderer is additionally exercised during diagnosis;
these portable tests need no CDN, source account or provider.
"""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'runtime'))
import heatmap_hover as hover


def fixture_rows(count):
    rows=[]
    for index in range(count):
        value=([.276,0,.435,0,.475,0,0,.583,0][index] if index<9 else
               1.1+(index-20)*.1 if 20<=index<=23 else 2 if index==25 else
               1.3 if index==26 else 1.2 if index in (28,29) else
               2 if index>=count-3 else 0)
        colour=('#f2e805' if index==25 or index>=count-3 else
                '#30ad76' if 20<=index<=23 or index in (26,28,29) else
                '#297090' if value else '#46035c')
        rows.append({'price':round(77556+(count-1-index)*56.73,2),
                     'value':round(value*1000000),'colour':colour})
    return rows


class CalibrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from playwright.sync_api import sync_playwright
        cls.playwright=sync_playwright().start()
        cls.browser=cls.playwright.chromium.launch(headless=True)

    @classmethod
    def tearDownClass(cls):
        cls.browser.close();cls.playwright.stop()

    def setUp(self):
        self.context=self.browser.new_context(viewport={'width':1600,'height':1000},timezone_id='UTC')
        self.context.route('**/*',lambda route:route.abort())
        self.page=self.context.new_page()
        self.addCleanup(self.context.close)
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)

    def fixture(self, *, count=122, order='source', mode='normal'):
        rows=fixture_rows(count)
        if mode in ('antialias','clipped'):
            rows[0].update(colour='#f2e805',value=2000000)
        if mode=='clipped':
            rows[0]['price']=round(rows[0]['price']-.4*56.73,2)
            rows[-1]['price']=round(rows[-1]['price']+.3*56.73,2)
        stamp=('2020-01-01 00:00' if mode=='stale' else
               datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M'))
        self.page.set_content('''<style>body{margin:0;height:2000px}.cg-toolti-box{position:fixed;display:none;background:white;pointer-events:none}</style>
            <canvas width="1260" height="730" style="position:absolute;left:300px;top:536px"></canvas>
            <div class="cg-toolti-box"></div>''')
        self.page.evaluate(r'''({rows,stamp,order,mode})=>{
            const canvas=document.querySelector('canvas'),tip=document.querySelector('.cg-toolti-box');
            const ctx=canvas.getContext('2d'),height=mode==='clipped'?592/(rows.length-2):597/rows.length;
            const boundary=index=>mode==='clipped'?(index===0?40:index===rows.length?637:42.5+(index-1)*height):40+index*height;
            const entries=rows.map((row,index)=>({...row,index,top:boundary(index),bottom:boundary(index+1)+.5})).reverse();
            const cells=order==='source'?[...entries.filter(r=>r.value),...entries.filter(r=>!r.value)]:entries;
            ctx.fillStyle='white';ctx.fillRect(0,0,1260,730);
            for(const row of cells){ctx.fillStyle=row.colour;ctx.fillRect(72,row.top,1123,row.bottom-row.top)}
            if(mode==='antialias'||mode==='clipped'){
                ctx.fillStyle='rgb(247,245,188)';ctx.fillRect(72,40,1123,1);ctx.fillRect(72,637,1123,1);
            }
            ctx.fillStyle='rgb(43,184,153)';ctx.fillRect(660,10,10,10);
            ctx.fillStyle='rgb(14,203,129)';ctx.fillRect(1192,342,1,5);
            let shown=true,bottomSeen=false;
            window.fixtureReads=[];
            canvas.onclick=e=>{if(e.offsetX>=660&&e.offsetX<=670&&e.offsetY>=10&&e.offsetY<=20)shown=!shown};
            canvas.onmouseleave=()=>tip.style.display='none';
            canvas.onmousemove=e=>{
                tip.style.left=(e.clientX-270)+'px';tip.style.top=(e.clientY+12)+'px';
                if(shown&&e.offsetX>=1191&&e.offsetX<=1193&&e.offsetY>=342&&e.offsetY<=347){
                    tip.style.display='block';tip.innerText=stamp+' Open 81000 High 81050 Low 80999 Close 81040';return;
                }
                if(e.offsetY>630)bottomSeen=true;
                let cell=[...cells].reverse().find(row=>row.top<=e.offsetY&&e.offsetY<=row.bottom);
                if(mode==='nonuniform'&&e.offsetY>=40&&e.offsetY<600){
                    const index=e.offsetY<45?0:e.offsetY<60?1:e.offsetY<64?2:e.offsetY<68?3:null;
                    if(index!==null)cell={...rows[index],index};
                }
                if(!cell){tip.style.display='none';return}
                let price=cell.price,value=cell.value,time=stamp;
                if(mode==='missing_price'&&cell.index===60)price=rows[61].price;
                if(bottomSeen&&cell.index===1&&mode==='changed_value')value+=500000;
                if(bottomSeen&&cell.index===1&&mode==='changed_time')time='2020-01-01 00:00';
                tip.style.display='block';tip.innerText=time+' Price '+price+' Liquidation Leverage '+(value/1000000).toFixed(3)+'M';
                fixtureReads.push({index:cell.index,price,value,y:e.clientY+scrollY,afterBottom:bottomSeen});
            };
        }''',{'rows':rows,'stamp':stamp,'order':order,'mode':mode})
        image=Path(self.temp.name)/'map.png'
        self.page.screenshot(path=str(image),full_page=True)
        return image,rows,{'x':300,'y':536,'width':1260,'height':730}

    def assert_complete_output(self, snapshot, expected, image):
        self.assertEqual(snapshot['complete'],True)
        self.assertEqual([r['id'] for r in snapshot['rows']],[f'r{i}' for i in range(len(expected))])
        self.assertEqual([r['price'] for r in snapshot['rows']],[r['price'] for r in expected])
        self.assertEqual([r['value'] for r in snapshot['rows']],[r['value'] for r in expected])
        total=sum(row['value'] for row in expected)
        self.assertEqual(snapshot['total_value'],total)
        self.assertEqual(snapshot['source_candle']['close'],81040)
        # These bands are explicitly drawn in the fixture; do not calculate
        # expected membership from the reader's positions or color samples.
        expected_groups=[list(range(20,24)),[25],[28,29],list(range(len(expected)-3,len(expected)))]
        if expected[0]['colour']=='#f2e805':expected_groups.insert(0,[0])
        pools=hover.candidate_pools(snapshot,image)
        self.assertEqual([p['source_row_ids'] for p in pools],[[f'r{i}' for i in ids] for ids in expected_groups])
        for pool,ids in zip(pools,expected_groups):
            self.assertEqual(pool['price_low'],expected[ids[-1]]['price'])
            self.assertEqual(pool['price_high'],expected[ids[0]]['price'])
            amount=sum(expected[i]['value'] for i in ids)
            self.assertEqual(pool['source_value'],amount)
            self.assertEqual(pool['percent'],amount/total*100)
        snapshot['candidates']=pools
        raw={'scans':[{'readable':True,'blocking_condition':'none','prominent_levels':[
            {'candidate_id':p['id'],'right_edge_visible':True,'strength_score':.5} for p in pools]}]}
        grounded=hover.ground_analysis(raw,snapshot,image_path=image,source_sha256=hashlib.sha256(image.read_bytes()).hexdigest())
        levels=grounded['scans'][0]['prominent_levels']
        result={'zones':[{k:p[k] for k in ('price_low','price_high','side')} for p in levels],
            'source_url':'https://www.coinglass.com/liquidation-heatmap','captured_at':snapshot['source_time'],
            'run_id':'offline-calibration','observed_price_range':None,'observed_price':81040,'price_reference_type':'exact_point'}
        attached=hover.attach_measurements(result,grounded,snapshot,image=image.read_bytes())
        self.assertEqual([z['source_value'] for z in attached['zones']],[sum(expected[i]['value'] for i in ids) for ids in expected_groups])
        # In particular, the reused initial calibration rows must have actual
        # native mouse evidence at the final positions saved for consumers.
        reads=self.page.evaluate('()=>fixtureReads')
        bounds=snapshot['plot_bounds']
        for index in (1,2,3):
            row=snapshot['rows'][index]
            y=bounds['top']+row['y_fraction']*(bounds['bottom']-bounds['top'])
            # MouseEvent coordinates are integer-quantized in Chromium. Require
            # this observation AFTER bottom calibration, not an earlier sample
            # that happened to cross the eventual fitted centre.
            self.assertTrue(any(r['afterBottom'] and r['index']==index and r['price']==row['price'] and
                r['value']==row['value'] and abs(r['y']-y)<=1.001 for r in reads))

    def test_source_draw_order_passes_complete_rows_bands_and_quantities(self):
        for model in (1,2):
            with self.subTest(model=model):
                image,expected,geometry=self.fixture(order='source')
                snapshot=hover.capture_column(self.page,image,geometry,model=model,timeframe='12H')
                self.assert_complete_output(snapshot,expected,image)
                self.assertLess(snapshot['duration_seconds'],55)

    def test_existing_uniform_grid_and_different_row_count_remain_correct(self):
        for count in (122,125):
            with self.subTest(count=count):
                image,expected,geometry=self.fixture(count=count,order='row')
                snapshot=hover.capture_column(self.page,image,geometry,model=1,timeframe='12H')
                self.assert_complete_output(snapshot,expected,image)

    def test_fitted_initial_positions_reject_changed_values_or_timestamps(self):
        for mode in ('changed_value','changed_time'):
            with self.subTest(mode=mode):
                image,_,geometry=self.fixture(mode=mode)
                with self.assertRaisesRegex(hover.HoverError,'hover_snapshot_changed'):
                    hover.capture_column(self.page,image,geometry,model=1,timeframe='12H')

    def test_antialiased_and_clipped_edges_keep_source_prices_and_edge_bands(self):
        for model,mode in ((1,'antialias'),(2,'clipped')):
            with self.subTest(model=model,mode=mode):
                image,expected,geometry=self.fixture(order='row',mode=mode)
                snapshot=hover.capture_column(self.page,image,geometry,model=model,timeframe='12H')
                self.assert_complete_output(snapshot,expected,image)
                self.assertGreater(snapshot['rows'][0]['y_fraction'],.000419)
                self.assertLess(snapshot['rows'][-1]['y_fraction'],1)

    def test_true_grid_failures_missing_rows_and_stale_data_still_fail(self):
        for mode,count,reason in [('nonuniform',122,'hover_resolution_insufficient'),
                ('normal',400,'hover_resolution_insufficient'),
                ('missing_price',122,'hover_update_timeout'),('stale',122,'hover_source_stale')]:
            with self.subTest(mode=mode,count=count):
                image,_,geometry=self.fixture(count=count,mode=mode)
                with self.assertRaisesRegex(hover.HoverError,reason):
                    hover.capture_column(self.page,image,geometry,model=1,timeframe='12H')


if __name__=='__main__':unittest.main()
