"""Offline browser tests for the laboratory's native clean-frame helper."""
from pathlib import Path
import tempfile
import unittest
from playwright.sync_api import sync_playwright
from PIL import Image
import clean_frame


class CleanFrameTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.p=sync_playwright().start()
        try:cls.browser=cls.p.chromium.launch(headless=True)
        except Exception:
            cls.p.stop();raise
    @classmethod
    def tearDownClass(cls):
        try:cls.browser.close()
        finally:cls.p.stop()
    def setUp(self):
        self.ctx=self.browser.new_context(viewport={'width':400,'height':300})
        self.addCleanup(self.ctx.close)
        self.page=self.ctx.new_page()
        self.ctx.route('**/*',lambda route:route.abort())
        self.page.set_content('''<style>body{margin:0}canvas{position:absolute;left:40px;top:40px}
            .cg-toolti-box{position:absolute;left:100px;top:100px;background:white;pointer-events:none}</style>
            <canvas width="240" height="180"></canvas><div class="cg-toolti-box">Price 123</div>''')
        self.page.evaluate('''() => {
          const c=document.querySelector('canvas'),t=document.querySelector('.cg-toolti-box');
          window.sourceState={price:123,value:456,hover:false,paints:0,leaves:0};
          window.sourcePaint=()=>{const s=window.sourceState,ctx=c.getContext('2d');s.paints++;
             ctx.fillStyle='#123456';ctx.fillRect(0,0,240,180);
             if(s.hover){ctx.fillStyle='#ffffff';ctx.fillRect(0,40,240,2)}};
          c.onpointermove=()=>{window.sourceState.hover=true;t.style.display='block';sourcePaint()};
          c.onpointerleave=()=>{window.sourceState.leaves++;
            setTimeout(()=>{t.style.display='none';requestAnimationFrame(()=>{window.sourceState.hover=false})},45)};
          sourcePaint();t.style.display='none';
        }''')
        self.page.mouse.move(100,100)
    def test_delayed_real_leave_removes_tip_and_crosshair_without_touching_values(self):
        self.assertFalse(self.page.evaluate(clean_frame.NO_TOOLTIP))
        before=self.page.evaluate('()=>({price:sourceState.price,value:sourceState.value})')
        paints=self.page.evaluate('()=>sourceState.paints')
        result=clean_frame.prepare_frame(self.page,lambda page:page.evaluate('()=>sourcePaint()'))
        self.assertTrue(result['tooltip_hidden']);self.assertEqual(result['waited_animation_frames'],2)
        self.assertEqual(self.page.evaluate('()=>sourceState.paints'),paints+1)
        self.assertEqual(self.page.locator('.cg-toolti-box').count(),1)
        self.assertEqual(self.page.locator('.cg-toolti-box').text_content(),'Price 123')
        self.assertEqual(self.page.evaluate('()=>({price:sourceState.price,value:sourceState.value})'),before)
        self.assertFalse(self.page.evaluate('()=>sourceState.hover'))
        self.assertGreater(self.page.evaluate('()=>sourceState.leaves'),0)
        self.assertIn('fresh_paint',result['operations'])
        with tempfile.TemporaryDirectory() as d:
            target=Path(d)/'frame.png';self.page.screenshot(path=str(target))
            with Image.open(target) as im:self.assertEqual(im.convert('RGB').getpixel((120,80)),(18,52,86))
    def test_stuck_tooltip_is_rejected_and_not_removed(self):
        self.page.evaluate("()=>document.querySelector('canvas').onpointerleave=()=>{}")
        with self.assertRaises(ValueError):clean_frame.prepare_frame(self.page,lambda p:p.evaluate('()=>sourcePaint()'),timeout_ms=200)
        self.assertFalse(self.page.evaluate(clean_frame.NO_TOOLTIP))
        self.assertEqual(self.page.locator('.cg-toolti-box').count(),1)
    def test_multiple_visible_tooltips_are_not_mistaken_for_absence(self):
        self.page.evaluate("()=>{const t=document.querySelector('.cg-toolti-box').cloneNode(true);document.body.append(t)}")
        with self.assertRaises(ValueError):clean_frame.prepare_frame(self.page,lambda p:p.evaluate('()=>sourcePaint()'),timeout_ms=200)
    def test_no_hover_does_not_change_source_state_and_cleaning_repeats(self):
        for _ in range(2):
            clean_frame.prepare_frame(self.page,lambda p:p.evaluate('()=>sourcePaint()'))
            self.assertTrue(self.page.evaluate(clean_frame.NO_TOOLTIP))
            self.assertEqual(self.page.evaluate('()=>sourceState.price'),123)
    def test_reappearing_tooltip_after_paint_is_rejected(self):
        with self.assertRaisesRegex(ValueError,'tooltip_reappeared'):
            clean_frame.prepare_frame(self.page,lambda p:p.evaluate("()=>{sourcePaint();document.querySelector('.cg-toolti-box').style.display='block'}"))
    def test_resized_canvas_during_paint_is_rejected(self):
        with self.assertRaisesRegex(ValueError,'geometry_changed'):
            clean_frame.prepare_frame(self.page,lambda p:p.evaluate("()=>{sourcePaint();document.querySelector('canvas').width=241}"))
    def test_moved_canvas_during_paint_is_rejected(self):
        with self.assertRaisesRegex(ValueError,'geometry_changed'):
            clean_frame.prepare_frame(self.page,lambda p:p.evaluate("()=>{sourcePaint();document.querySelector('canvas').style.left='41px'}"))
    def test_missing_animation_frames_are_bounded(self):
        self.page.evaluate("()=>{document.querySelector('canvas').onpointerleave=()=>document.querySelector('.cg-toolti-box').style.display='none';window.requestAnimationFrame=()=>42}")
        with self.assertRaises(ValueError):clean_frame.prepare_frame(self.page,lambda p:p.evaluate('()=>sourcePaint()'),timeout_ms=200)
    def test_invalid_budget_is_rejected_before_input(self):
        for value in (True,0,49,15001,float('nan')):
            with self.assertRaisesRegex(ValueError,'invalid_budget'):
                clean_frame.prepare_frame(self.page,lambda p:None,timeout_ms=value)
        self.assertEqual(self.page.evaluate('()=>sourceState.leaves'),0)
    def test_no_outside_point_is_rejected(self):
        with self.assertRaisesRegex(ValueError,'no_outside_point'):
            clean_frame.outside_point({'x':0,'y':0,'width':400,'height':300},{'width':400,'height':300})
    def test_multiple_canvases_are_rejected_before_input(self):
        self.page.evaluate("()=>document.body.append(document.querySelector('canvas').cloneNode())")
        with self.assertRaisesRegex(ValueError,'unique_canvas_required'):
            clean_frame.prepare_frame(self.page,lambda p:None)
        self.assertEqual(self.page.evaluate('()=>sourceState.leaves'),0)


if __name__=='__main__':unittest.main()
