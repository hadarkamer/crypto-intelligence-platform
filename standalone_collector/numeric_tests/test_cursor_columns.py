"""Regression checks for pointer-linked market times, including stationary canvas text."""
from datetime import datetime,timezone
from unittest import TestCase

from model1_execution import StageFailure
from modern_map_cursor import caption_time, decode, read_cursor_pair, validate_receipt
from playwright.sync_api import sync_playwright


# A local market chart with observed CoinGlass header/body typography. Its axis can
# deliberately repaint only a stationary midnight tick after every real mouse move.
HTML = """<meta charset=utf-8><style>
body{margin:0} #plot{position:absolute;top:40px;left:20px;width:640px;height:380px}
canvas{position:absolute;inset:0} #tip{position:absolute;top:55px;left:260px;background:white}
#axis{position:absolute;top:346px;background:white;white-space:nowrap;transform:translateX(-50%)} [hidden]{display:none}</style>
<div id=plot><canvas id=c width=640 height=380></canvas><div id=tip hidden><div id=title></div>
<div><span id=relative></span><span> · </span><span id=period></span></div></div><span id=axis hidden></span></div>
<script>
window.cfg={};window.steps=[];
const ctx=c.getContext('2d');const panel=document.getElementById('tip');
const titleNode=document.getElementById('title'), off='11/27 00:00';
function draw(e){
  const rect=c.getBoundingClientRect(), x=e?e.clientX-rect.left:-1;
  const on=0<=x&&x<=590, last=x>=587, stamp=last?cfg.latest:cfg.prior;
  ctx.fillStyle='white';ctx.fillRect(0,0,640,380);ctx.font='14px sans-serif';ctx.textAlign='center';
  ctx.fillStyle='#333';if(!cfg.dirty||!last)ctx.fillText(off,cfg.staticX||435,357);
  if(cfg.dirty&&on&&!last){ctx.fillStyle='blue';ctx.fillRect(435,344,2,18);}
  panel.hidden=!on||cfg.noTip;
  if(!panel.hidden){
    titleNode.textContent=cfg.staleTitle||stamp;
    if(cfg.split){const parts=titleNode.textContent.split(' ');titleNode.replaceChildren(...parts.map(t=>{
      const child=document.createElement('span');child.textContent=t;return child;}));}
    relative.textContent=last?(cfg.fakeLatest||'Latest column'):cfg.relative;
    period.textContent=(cfg.period||'30 min')+' per column';
  }
  if(cfg.staticDom){
    if(cfg.replaceDom){const copy=axis.cloneNode(false);axis.replaceWith(copy);}
    axis.textContent=off;axis.style.left='435px';axis.hidden=false;}
  else {
    axis.hidden=!on||cfg.layer!=='dom';
    if(!axis.hidden){axis.textContent=stamp;axis.style.left=x+'px';}
  }
  if(on&&cfg.layer==='canvas'&&(!cfg.dirty||last)){
    ctx.fillStyle='#304572';ctx.fillRect(x-32,344,64,18);
    ctx.fillStyle='white';ctx.fillText(stamp,x,357);
  }
  steps.push(on?(last?'latest':'prior'):'out');
}
c.onmousemove=draw;c.onmouseleave=()=>draw();draw();
</script>"""


class CursorTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.driver=sync_playwright().start()
        try:cls.browser=cls.driver.firefox.launch()
        except Exception:
            cls.driver.stop()
            raise

    @classmethod
    def tearDownClass(cls):
        cls.browser.close()
        cls.driver.stop()

    def setUp(self):
        self.page=self.browser.new_page(viewport={'width':800,'height':500},timezone_id='UTC')
        self.addCleanup(self.page.close)
        self.page.route('**/*',lambda route:route.abort())
        self.page.set_content(HTML)
        self.anchor=datetime(2026,11,27,21,42,tzinfo=timezone.utc)

    def pair(self,**updates):
        cfg={'prior':'11/25 21:30','latest':'11/27 21:30','relative':'2 d 0 min before the latest','layer':'canvas',**updates}
        self.page.evaluate('cfg=>{window.cfg=cfg; draw()}',cfg)
        return read_cursor_pair(self.page,(20,40,640,380),(31,58,610,375),self.anchor,'1W','30m')

    def test_off_prior_latest_canvas_and_dom(self):
        for layer in ('canvas','dom'):
            with self.subTest(layer=layer):
                prior,current,s=self.pair(layer=layer)
                self.assertEqual((prior['stamp'],current['stamp']),('11/25 21:30','11/27 21:30'))
                self.assertEqual(s['prior_axis_sources'],[layer])
                self.assertEqual(s['latest_axis_sources'],[layer])
                validate_receipt({'schema':'coinglass.cursor.v1','opening':s,'closing':s},'30m','1W',s['latest_time'])
                self.assertTrue(self.page.locator('#tip').is_hidden())

    def test_real_midnight_can_be_the_earlier_label(self):
        for layer in ('canvas','dom'):
            s=self.pair(layer=layer,staticX=427,prior='11/27 00:00',relative='21 h 30 min before the latest')[2]
            self.assertEqual(s['prior_time'],'2026-11-27T00:00:00Z')

    def test_static_repaint_with_matching_midnight_tooltip_fails(self):
        for static_dom in (False,True):
            with self.subTest(dom=static_dom),self.assertRaises(StageFailure):
                self.pair(layer='none',staticDom=static_dom,prior='11/27 00:00',relative='21 h 30 min before the latest')

    def test_partial_repaint_and_unrelated_line_do_not_prove_a_stationary_tick(self):
        with self.assertRaises(StageFailure):
            self.pair(dirty=True,prior='11/27 00:00',relative='21 h 30 min before the latest')

    def test_partial_receipt_filters_page_data(self):
        from modern_map_checkpoint import _observation_receipts
        stages=[dict(name='opening_prior_cursor_probe',status='observed',displayed_time='11/27 00:00',
                     tooltip_count=1,axis_time_count=1,displayed_lag_seconds=77400,extraneous='ignore'),
                dict(name='opening_latest_cursor_probe',status='failed',displayed_time='invalid content',
                     tooltip_count=0,axis_time_count=1,displayed_lag_seconds=604801)]
        receipts=_observation_receipts(stages)
        self.assertEqual(receipts[stages[0]['name']],{k:v for k,v in stages[0].items() if k not in ('name','extraneous')})
        self.assertEqual(receipts[stages[1]['name']],{'status':'failed','tooltip_count':0,'axis_time_count':1})

    def test_relative_prior_at_edge_cannot_be_published(self):
        with self.assertRaises(StageFailure):
            self.pair(fakeLatest='30 min before the latest')

    def test_changed_epoch_and_period_and_zone(self):
        for changes in ({'relative':'1 d 0 min before the latest'}, {'period':'5 min'},
                        {'staleTitle':'11/26 02:30'}, {'noTip':True}):
            with self.subTest(changes=changes),self.assertRaises(StageFailure):
                self.pair(**changes)

    def test_nested_date_and_semantic_caption(self):
        s=self.pair(layer='canvas',split=True)[2]
        self.assertEqual(s['latest_time'],'2026-11-27T21:30:00Z')

    def test_identical_replaced_dom_node_cannot_certify_midnight(self):
        with self.assertRaises(StageFailure):
            self.pair(layer='none',staticDom=True,replaceDom=True,prior='11/27 00:00',relative='21 h 30 min before the latest')

    def test_closing_revalidates_prior_and_latest_and_rejects_new_epoch(self):
        s=self.pair()[2]
        stable=read_cursor_pair(self.page,(20,40,640,380),(31,58,610,375),self.anchor,'1W','30m',
                                expected_column=datetime.fromisoformat(s['latest_time']))[2]
        self.assertEqual(stable['latest_time'],s['latest_time'])
        with self.assertRaises(StageFailure):
            read_cursor_pair(self.page,(20,40,640,380),(31,58,610,375),self.anchor,'1W','30m',
                             expected_column=datetime(2026,11,27,21,0,tzinfo=timezone.utc))
        bad=dict(stable,prior_lag_seconds=1800)
        with self.assertRaises(StageFailure):
            validate_receipt({'schema':'coinglass.cursor.v1','opening':s,'closing':bad},'30m','1W',s['latest_time'])

    def test_forward_rollover_requires_new_source_clock_and_agreeing_control_cell(self):
        from datetime import timedelta
        from unittest.mock import Mock
        old = datetime(2026,11,27,21,30,tzinfo=timezone.utc)
        clock = Mock(return_value=old+timedelta(minutes=31))
        self.pair()  # Establish an on-screen prior/latest observation.
        def closing():
            return read_cursor_pair(self.page,(20,40,640,380),(31,58,610,375),self.anchor,'1W','30m',
                                    expected_column=old,refresh_anchor=clock)
        self.assertEqual(closing()[2]['latest_time'],'2026-11-27T21:30:00Z')
        clock.assert_not_called()
        self.page.evaluate("()=>Object.assign(cfg,{prior:'11/25 22:00',latest:'11/27 22:00'})")
        with self.assertRaises(StageFailure) as advanced:
            closing()
        self.assertEqual(advanced.exception.verified_change['closing_column_time'],'2026-11-27T22:00:00Z')
        clock.assert_called_once()
        self.page.evaluate("()=>Object.assign(cfg,{prior:'11/25 21:30'})")
        with self.assertRaises(StageFailure) as mismatch:
            closing()
        self.assertEqual(mismatch.exception.basic_reason,'cursor_column_distance_disagrees')
        self.assertFalse(hasattr(mismatch.exception,'verified_change'))

    def test_caption_and_conflicting_bottom_date(self):
        self.assertEqual(caption_time('1 d 19 h before the latest · 30 min per column',1800),154800)
        self.assertIsNone(caption_time('zero before the latest · 30 min per column',1800))
        self.assertIsNotNone(decode(dict(tooltip=[['11/25 21:30','2 d 0 min before the latest · 30 min per column']],
            axis=[['11/25 21:30',40,390,'dom',1],['11/27 00:00',45,390,'canvas',0]]),40,1800,False))
