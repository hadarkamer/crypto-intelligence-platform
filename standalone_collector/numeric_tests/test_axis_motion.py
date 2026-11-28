"""Local Firefox and the installed OCR read two prices at a single verified source column."""
from datetime import datetime, timezone
from io import BytesIO
from unittest import TestCase

from PIL import Image
from playwright.sync_api import sync_playwright

import modern_map_axis_motion as motion
from modern_map_axis import axis_marks
from modern_map_axis_ocr import PrintedWord, axis_scale_live
from modern_map_cursor import read_cursor_pair
from test_cursor_columns import HTML


# Chart geometry and all numeric labels are derived from one scale. The price tag is
# obscured at the initial pointer height and returns when the pointer leaves.
PRICE_DISPLAY = """<script>
const drawClock=draw;
function display(event) {
  drawClock(event);
  ctx.save();
  ctx.fillStyle='#121720';ctx.fillRect(10,18,630,318);
  ctx.font='14px DejaVu Sans';ctx.textAlign='left';
  const text=(raw,x,y)=>{
    const m=ctx.measureText(raw);
    ctx.fillText(raw,x,y+(m.actualBoundingBoxAscent-m.actualBoundingBoxDescent)/2);
  };
  const printed=y=>new Intl.NumberFormat('en-US').format(2000-2*(y-60));
  for(let y=85;y<365;y+=40){
    ctx.fillStyle='white';text(printed(y),594,y-40);
  }
  const anchor=220;
  ctx.fillStyle='white';
  for(let x=11;x<583;x+=8)ctx.fillRect(x,anchor-40,4,1);
  ctx.fillRect(591,anchor-49,49,19);
  ctx.fillStyle='black';text(printed(anchor),594,anchor-40);
  if(event && event.clientX<=610){
    const y=event.clientY;
    ctx.fillStyle='#07512a';ctx.fillRect(591,y-49,49,19);
    ctx.fillStyle='white';
    const n=window.badScale?printed(y-32):printed(y);
    text(n,594,y-40);
  }
  ctx.restore();
}
c.onmousemove=display;c.onmouseleave=()=>display();display();
</script>"""


class MotionPriceTests(TestCase):
    box=(20,40,640,380)
    bounds=(31,58,610,375)
    anchor=datetime(2026,11,27,21,42,tzinfo=timezone.utc)

    @classmethod
    def setUpClass(cls):
        cls.driver=sync_playwright().start()
        try:
            cls.browser=cls.driver.firefox.launch()
        except Exception:
            cls.driver.stop()
            raise

    @classmethod
    def tearDownClass(cls):
        cls.browser.close()
        cls.driver.stop()

    def setUp(self):
        self.page=self.browser.new_page(viewport=dict(width=800,height=500),timezone_id='UTC')
        self.addCleanup(self.page.close)
        self.page.route('**/*',lambda route:route.abort())
        self.page.set_content(HTML+PRICE_DISPLAY)
        self.page.evaluate("()=>{cfg={prior:'11/27 19:30',latest:'11/27 21:30',"
                           "relative:'2 h before the latest',layer:'dom',period:'30 min'};display()}")
        self.events=[]

    def collect(self, *, bad_scale=False, change_epoch=False, overlay=False):
        self.page.evaluate('value=>{window.badScale=value;display()}',bad_scale)
        source=Image.open(BytesIO(self.page.screenshot())).convert('RGB')
        self.addCleanup(source.close)
        prior,current,sample=read_cursor_pair(self.page,self.box,self.bounds,self.anchor,'12H','30m')
        marks=axis_marks(self.page,self.box,prior['marks'],current['marks'])
        audit={}
        cache={}
        scale=axis_scale_live(source,self.box,self.bounds,marks,pointer_y=sample['pointer_y'],
                              cache=cache,observe=audit.update)
        self.assertIsNone(scale,audit)
        mark=motion.ambiguous_mark(audit)
        self.assertIsNotNone(mark,audit)
        if change_epoch:
            self.page.evaluate("()=>{cfg.latest='11/27 22:00';display()}")
        if overlay:
            self.page.evaluate("""()=>{const d=document.createElement('div');d.setAttribute('popover','manual');
                d.style.cssText='position:fixed;inset:200px auto auto 567px;margin:0;width:70px;height:30px';
                d.textContent='private';document.body.append(d);d.showPopover()}""")
        return motion.resolve(self.page,source,self.box,self.bounds,marks,None,sample,cache,audit,mark,
                              'opening',lambda:None,lambda *event:self.events.append(event))

    def test_transient_covering_badge_is_excluded_only_after_motion_and_scale(self):
        result=self.collect()
        self.assertIsNotNone(result,self.events[-1])
        self.assertEqual(result[0],'1,680')
        self.assertAlmostEqual(result[1](1680),220,delta=2.2)
        frames=[v for kind,v in self.events if kind=='motion_frame']
        self.assertEqual([v['state'] for v in frames],['y1','y2','off'])
        self.assertEqual({f['column_time'] for f in frames},{'2026-11-27T21:30:00Z'})
        stats=self.events[-1][1]
        self.assertEqual((stats['status'],stats['ocr_calls']),('ok',2),stats)

    def test_plausible_moving_prices_with_wrong_scale_are_rejected(self):
        self.assertIsNone(self.collect(bad_scale=True))
        self.assertEqual(self.events[-1][1]['condition'],'motion_cursor_scale_disagrees')

    def test_newer_column_is_not_assigned_to_an_older_capture(self):
        self.assertIsNone(self.collect(change_epoch=True))
        self.assertEqual(self.events[-1][1]['captures'],0)

    def test_occluding_private_overlay_is_never_captured(self):
        self.assertIsNone(self.collect(overlay=True))
        self.assertEqual(self.events[-1][1]['captures'],0)

    def test_clipped_glyph_crosses_band_without_its_pointer_center(self):
        # Lower part of a cursor glyph overlaps a light reference tag. A repaint
        # confined to the other end of the tag, or to uniform background, is insufficient.
        off=Image.new('RGB',(40,30),'white')
        self.addCleanup(off.close)
        image=off.copy()
        self.addCleanup(image.close)
        from PIL import ImageDraw
        draw=ImageDraw.Draw(image)
        draw.rectangle((4,5,27,23),fill='green')
        draw.rectangle((10,8,12,19),fill='white')
        draw.rectangle((21,8,23,19),fill='white')
        word=PrintedWord('42',4,5,24,19,99)
        frames={'off':dict(image=off,words=[]),'y2':dict(image=off,words=[]),
                'y1':dict(image=image,words=[word],point=(0,14),
                          marks=[dict(text='42',x=12,y=14)])}
        self.assertEqual(motion._moving_value(frames,'y1','y2',(0,0,40,30),(18,25),True),word)
        draw.rectangle((4,25,27,26),fill='red')
        for nonoverlap in ((25,26),(20,24)):
            with self.subTest(tag_band=nonoverlap), self.assertRaises(motion.UnverifiedMotion):
                motion._moving_value(frames,'y1','y2',(0,0,40,30),nonoverlap,True)
