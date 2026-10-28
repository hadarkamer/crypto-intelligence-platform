"""Offline differential checks for passive candle-failure observations."""
import ast
import copy
from datetime import datetime, timezone
import inspect
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'runtime'))

import heatmap_hover as hover
from capture_failure_diagnostics import _safe_candle


def poll_expression(expression, observed):
    # Exercise the actual installed poll expression, without exposing another
    # production entry point or substituting a second implementation of it.
    tree = ast.parse(inspect.getsource(hover.capture_column))
    function = next(node for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == 'wait_for_tooltip')
    assignment = next(node for node in function.body if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == 'poll' for target in node.targets))
    poll = eval(compile(ast.Expression(assignment.value), '<existing-poll>', 'eval'), {'expression':expression})
    if observed:
        branch = next(node for node in function.body if isinstance(node, ast.If)
            and isinstance(node.test, ast.Compare)
            and isinstance(node.test.left, ast.Name) and node.test.left.id == 'candle_attempt')
        scope = {'poll':poll}
        exec(compile(ast.Module(body=branch.body, type_ignores=[]), '<observed-poll>', 'exec'), scope)
        poll = scope['poll']
    return poll


def diagnostic_from_exception(exc):
    trace = exc.__traceback__
    while trace:
        if trace.tb_frame.f_code.co_name == 'capture_column':
            return copy.deepcopy(trace.tb_frame.f_locals['candle_diagnostics'])
        trace = trace.tb_next
    raise AssertionError('capture frame missing')


class CandleDiagnosticBrowserTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from playwright.sync_api import sync_playwright
        cls.playwright = sync_playwright().start()
        cls.browser = cls.playwright.chromium.launch(headless=True)

    @classmethod
    def tearDownClass(cls):
        cls.browser.close()
        cls.playwright.stop()

    def setUp(self):
        self.context = self.browser.new_context(viewport={'width':900,'height':600}, timezone_id='UTC')
        self.context.route('**/*', lambda route: route.abort())
        self.page = self.context.new_page()
        self.addCleanup(self.context.close)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)

    def test_predicate_result_and_dom_read_counts_match_original(self):
        candle = '2026-10-28 12:00 Open 986 High 987 Low 985 Close 986'
        cases = [
            ('', 'no_visible_tooltip', 'none'),
            ('<div class="cg-toolti-box">'+candle+'</div>', 'accepted', 'candle'),
            ('<div class="cg-toolti-box">Open 1 Close 2</div>', 'accepted', 'candle'),
            ('<div class="cg-toolti-box">Price 986 Liquidation Leverage 2M</div>', 'not_candle', 'liquidity'),
            ('<div class="cg-toolti-box">private source text</div>', 'not_candle', 'other'),
            ('<div class="cg-toolti-box">'+('private-'+('x'*230))+'</div>', 'text_too_long', 'other'),
            ('<div style="opacity:0"><div class="cg-toolti-box">'+candle+'</div></div>', 'no_visible_tooltip', 'none'),
            ('<div class="cg-toolti-box" style="position:absolute;top:900px">'+candle+'</div>', 'no_visible_tooltip', 'none'),
            ('<div class="cg-toolti-box">'+candle+'</div><div class="cg-toolti-box">other</div>', 'multiple_visible_tooltips', 'multiple'),
        ]
        for html, reason, kind in cases:
            with self.subTest(reason=reason, html=html[:35]):
                self.page.set_content(html)
                result = self.page.evaluate(r'''arg=>{
                    const observed=eval('('+arg.observed+')');
                    const original=eval('('+arg.original+')');
                    const query=document.querySelectorAll,rect=Element.prototype.getBoundingClientRect,
                          style=window.getComputedStyle;
                    let counts;
                    document.querySelectorAll=function(...args){counts.query++;return query.apply(this,args)};
                    Element.prototype.getBoundingClientRect=function(...args){counts.rect++;return rect.apply(this,args)};
                    window.getComputedStyle=function(...args){counts.style++;return style.apply(this,args)};
                    try {
                        counts={query:0,rect:0,style:0};const before=original('candle'),beforeCounts={...counts};
                        counts={query:0,rect:0,style:0};const after=observed(),afterCounts={...counts};
                        return {before,after,beforeCounts,afterCounts,diagnostic:observed.failure().tooltip};
                    } finally {
                        document.querySelectorAll=query;Element.prototype.getBoundingClientRect=rect;
                        window.getComputedStyle=style;
                    }
                }''', {'original':hover.READ_TOOLTIP,'observed':hover.READ_CANDLE_TOOLTIP})
                self.assertEqual(result['before'], result['after'])
                self.assertEqual(result['beforeCounts'], result['afterCounts'])
                self.assertEqual(result['diagnostic']['last']['reason'], reason)
                self.assertEqual(result['diagnostic']['last']['kind'], kind)
                self.assertNotIn('private', json.dumps(result['diagnostic']))
                self.assertNotIn('raw_text', json.dumps(result['diagnostic']))

    def test_same_350ms_poll_schedule_for_delayed_missing_multiple_and_late_tooltips(self):
        self.page.set_content('<div id="tip" class="cg-toolti-box">initial</div><div id="other" class="cg-toolti-box" style="display:none">other</div>')
        original = poll_expression('() => ('+hover.READ_TOOLTIP+")('candle')", False)
        observed = poll_expression(hover.READ_CANDLE_TOOLTIP, True)
        for mode, delay in [('delayed',100),('late',400),('missing',0),('multiple',0)]:
            outcomes = []
            for script in (original, observed):
                result = self.page.evaluate(r'''async arg=>{
                    let now=0;const scheduled=[],delays=[];
                    const nativeTimer=window.setTimeout,descriptor=Object.getOwnPropertyDescriptor(performance,'now');
                    const tip=document.querySelector('#tip'),other=document.querySelector('#other');
                    const candle='2026-10-28 12:00 Open 986 High 987 Low 985 Close 986';
                    function update(){
                        tip.style.display=arg.mode==='missing'?'none':'block';
                        other.style.display=arg.mode==='multiple'?'block':'none';
                        tip.innerText=now>=arg.delay?candle:'Price 986 Liquidation Leverage 1M';
                    }
                    Object.defineProperty(performance,'now',{configurable:true,value:()=>now});
                    window.setTimeout=(callback,delay)=>{delays.push(delay);scheduled.push([callback,delay]);return scheduled.length};
                    try {
                        update();const pending=eval('('+arg.script+')')({value:null,timeout:350});
                        while(scheduled.length){const [callback,delay]=scheduled.shift();now+=delay;update();callback();}
                        return {value:await pending,now,delays};
                    } finally {
                        window.setTimeout=nativeTimer;
                        if(descriptor)Object.defineProperty(performance,'now',descriptor);else delete performance.now;
                    }
                }''', {'script':script,'mode':mode,'delay':delay})
                outcomes.append(result)
            before, after = outcomes
            envelope = after['value'] if isinstance(after['value'],dict) else None
            self.assertEqual(before['value'], None if envelope else after['value'])
            self.assertEqual(before['delays'], after['delays'])
            self.assertEqual(before['now'], after['now'])
            self.assertEqual(after['now'],100 if mode=='delayed' else 350)
            if envelope:
                self.assertTrue(envelope['_candle_poll_timeout'])
                self.assertEqual(envelope['tooltip']['poll_count'],15)
                self.assertLess(len(json.dumps(envelope)),1000)

    def test_observer_is_bounded_and_observer_error_cannot_reject_a_candle(self):
        self.page.set_content('<div class="cg-toolti-box">private</div>'*12)
        bounded=self.page.evaluate(r'''source=>{
            const predicate=eval('('+source+')');
            for(let i=0;i<1200;i++)predicate();
            return predicate.failure().tooltip;
        }''',hover.READ_CANDLE_TOOLTIP)
        self.assertEqual(bounded['poll_count'],1000)
        self.assertEqual(bounded['reason_counts']['multiple_visible_tooltips'],1000)
        self.assertEqual(bounded['last']['visible_count'],9)
        self.assertNotIn('private',json.dumps(bounded))
        self.page.set_content('<div class="cg-toolti-box">Open 1 Close 2</div>')
        value=self.page.evaluate(r'''source=>{
            const predicate=eval('('+source+')'),original=Math.min;
            Math.min=()=>{throw new Error('optional observer failed')};
            try{return predicate()}finally{Math.min=original}
        }''',hover.READ_CANDLE_TOOLTIP)
        self.assertEqual(value,'Open 1 Close 2')

    def chart_fixture(self, mode):
        stamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')
        html = '''<canvas width="600" height="340" style="position:absolute;left:20px;top:40px"></canvas>
        <div id="tip" class="cg-toolti-box" style="position:absolute;left:630px;top:50px;display:none"></div>
        <script>(()=>{
        const canvas=document.querySelector('canvas'),tip=document.querySelector('#tip');
        const ctx=canvas.getContext('2d');ctx.fillStyle='white';ctx.fillRect(0,0,600,340);
        ctx.fillStyle='rgb(68,1,84)';ctx.fillRect(0,40,600,300);
        ctx.fillStyle='rgb(43,184,153)';ctx.fillRect(300,10,10,10);let priceVisible=true,toggles=0;
        ctx.fillStyle='rgb(14,203,129)';ctx.fillRect(596,180,1,4);
        canvas.onclick=e=>{if(e.offsetX>=300&&e.offsetX<=310&&e.offsetY>=10&&e.offsetY<=20){priceVisible=!priceVisible;toggles++;}};
        canvas.onmousemove=e=>{if(e.offsetY<41||e.offsetY>338){tip.style.display='none';return;}
          const row=Math.min(29,Math.floor((e.offsetY-40)/10));
          if(priceVisible&&row===14&&MODE!=='initial_timeout'&&!(MODE==='final_timeout'&&toggles===2)){
            tip.style.display='block';tip.innerText=MODE==='parse_rejected'?'Open 1 Close 2':'STAMP Open 986 High 987 Low 985 Close 986';return;}
          tip.style.display='block';tip.innerText='STAMP Price '+(1000-row)+' Liquidation Leverage '+(row+1)+'.00M';};
        canvas.onmouseleave=()=>tip.style.display='none';
        })();</script>'''.replace('STAMP',stamp).replace('MODE',json.dumps(mode))
        self.page.set_content(html)
        path = Path(self.temp.name)/'fixture.png'
        self.page.screenshot(path=str(path), full_page=True)
        return path

    def test_real_capture_initial_final_and_parse_failures_keep_codes_and_safe_schema(self):
        geometry={'x':20,'y':40,'width':600,'height':340}
        for mode, reason, phase in [('initial_timeout','hover_candle_not_found','initial'),
                ('final_timeout','hover_candle_not_found','final'),
                ('parse_rejected','hover_candle_text_invalid','initial')]:
            with self.subTest(mode=mode):
                path=self.chart_fixture(mode)
                try: hover.capture_column(self.page,path,geometry,model=1,timeframe='24h')
                except hover.HoverError as exc:
                    self.assertEqual(exc.reason, reason)
                    diagnostic=diagnostic_from_exception(exc)
                else:self.fail('expected fixture failure')
                self.assertEqual(_safe_candle(diagnostic), diagnostic)
                self.assertEqual(diagnostic['phase'], phase)
                attempt=diagnostic['attempts'][-1]
                self.assertEqual(attempt['timeout_ms'],350)
                self.assertEqual(attempt['outcome'],'parse_rejected' if mode=='parse_rejected' else 'timeout')
                if mode!='parse_rejected':
                    self.assertEqual(attempt['tooltip']['last']['reason'],'not_candle')
                    self.assertEqual(attempt['tooltip']['last']['kind'],'liquidity')
                if phase=='final':
                    self.assertEqual(len(diagnostic['attempts']),2)
                    self.assertEqual(diagnostic['initial_candle']['close'],986)
                self.assertNotIn('raw_text',json.dumps(diagnostic))
        path=self.chart_fixture('success')
        result=hover.capture_column(self.page,path,geometry,model=2,timeframe='24h')
        self.assertEqual(len(result['rows']),30)
        self.assertEqual([row['price'] for row in result['rows']],list(range(1000,970,-1)))
        self.assertEqual(result['total_value'],465000000)
        self.assertEqual(result['source_candle']['close'],986)

    def test_missing_legend_keeps_failure_and_no_candle_attempt(self):
        path=self.chart_fixture('success')
        with patch.object(hover,'price_overlay_point',return_value=None):
            try: hover.capture_column(self.page,path,{'x':20,'y':40,'width':600,'height':340},model=1,timeframe='12h')
            except hover.HoverError as exc:
                self.assertEqual(exc.reason,'hover_candle_not_found')
                diagnostic=diagnostic_from_exception(exc)
            else:self.fail('expected missing legend')
        self.assertEqual(diagnostic['outcome'],'legend_missing')
        self.assertEqual(diagnostic['attempts'],[])
        self.assertEqual(_safe_candle(diagnostic),diagnostic)


class CandlePixelDiagnosticTests(unittest.TestCase):
    def test_pixel_selection_and_failure_codes_unchanged_and_groups_bounded(self):
        from PIL import Image
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'pixels.png'
            bounds={'left':0,'right':50,'top':0,'bottom':59}
            for count in (0,1,2,9):
                image=Image.new('RGB',(60,60),(68,1,84))
                for index in range(count):image.putpixel((49,3+index*5),(14,203,129))
                image.save(path);image.close()
                before=path.read_bytes();diagnostic={}
                if count in (0,9):
                    for kwargs in ({},{'diagnostics':diagnostic}):
                        with self.assertRaisesRegex(hover.HoverError,'hover_candle_not_found'):
                            hover.candle_points(path,bounds,**kwargs)
                    self.assertEqual(diagnostic['outcome'],'no_pixel_groups' if count==0 else 'ambiguous_pixel_groups')
                else:
                    expected=hover.candle_points(path,bounds)
                    self.assertEqual(hover.candle_points(path,bounds,diagnostics=diagnostic),expected)
                    self.assertEqual(diagnostic['points'],[{'x':x,'y':y} for x,y in expected])
                self.assertEqual(diagnostic['pixel_count'],count)
                self.assertEqual(diagnostic['pixel_group_count'],count)
                self.assertLessEqual(len(diagnostic['pixel_groups']),8)
                self.assertEqual(_safe_candle(diagnostic),diagnostic)
                self.assertEqual(path.read_bytes(),before)


if __name__=='__main__':unittest.main()
