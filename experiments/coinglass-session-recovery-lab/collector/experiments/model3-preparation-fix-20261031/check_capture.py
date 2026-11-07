"""Offline task boundary check: actual browser/capture/reader/AI client/validator.

Only HTML and the Responses HTTP reply are fixtures. No source account, external
network, application writes or paid AI. Run each model in a fresh subprocess.
"""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import uuid
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[2]
HTML=r'''<style>body{margin:0}#panel{margin:40px;width:800px}canvas{display:block}
.cg-toolti-box{position:absolute;pointer-events:none;background:white;z-index:10}</style>
<div id="panel" class="LiqHeatmap3_box1__fixture"><h1>BTC Liquidation Heatmap</h1>
<button>Model MODEL</button><button>Symbol</button>
<button role="combobox" onclick="document.querySelector('#options').hidden=false">48 hour</button>
<div id="options" hidden><button role="option" onclick="select(12)">12 hour</button>
<button role="option" onclick="select(24)">24 hour</button>
<button role="option" onclick="select(48)">48 hour</button></div>
<canvas width="800" height="420"></canvas><div data-axis="y" role="button">96.1 100</div>
<div class="MuiCard-root"><a href="https://legend.coinglass.com">Legend NEW</a>
<div class="shou" data-first-child onclick="this.parentElement.remove()">
<svg width="20" height="20"><path d="M405 136.798L375.202 107"/></svg></div></div></div>
<div class="cg-toolti-box" hidden></div><script>
const model=MODEL,canvas=document.querySelector('canvas'),ctx=canvas.getContext('2d');
const tip=document.querySelector('.cg-toolti-box'),listeners=[];
let candles=true;window.hours=48;window.paints=0;window.dataUpdates=0;
const ticker={started:true,get count(){return listeners.length},add(fn,scope){listeners.push({fn,scope})},
remove(fn,scope){for(let i=listeners.length-1;i>=0;i--)if(listeners[i].fn===fn&&listeners[i].scope===scope)listeners.splice(i,1)}};
const renderer={canvas,render(){paints++;ctx.fillStyle='#111';ctx.fillRect(0,0,800,420);
ctx.fillStyle='rgb(68,1,84)';ctx.fillRect(0,20,800,400);
ctx.fillStyle='rgb(48,173,118)';ctx.fillRect(0,20,800,100);
if(model!==3){ctx.fillStyle='rgb(0,200,100)';ctx.fillRect(10,3,10,10)}
if(candles){ctx.fillStyle=model===3?'rgb(234,51,35)':'rgb(0,200,120)';ctx.fillRect(796,210,4,2)}}};
window.app={renderer,ticker,render(){this.renderer.render()}};
ticker.add(()=>dataUpdates++,null);ticker.add(app.render,app);
if(model===3)__PIXI_APP_INIT__(app,'8.21.0');
function frame(){for(const l of [...listeners])l.fn.call(l.scope);requestAnimationFrame(frame)}requestAnimationFrame(frame);
window.select=h=>{hours=h;document.querySelector('[role=combobox]').textContent=h+' hour';document.querySelector('#options').hidden=true};
canvas.addEventListener('click',e=>{const r=canvas.getBoundingClientRect();if(e.clientY-r.y<20){candles=!candles;app.render()}});
canvas.addEventListener('pointermove',e=>{const r=canvas.getBoundingClientRect(),y=e.clientY-r.y,x=e.clientX-r.x;
if(y<20||y>=420){tip.hidden=true;return}
const index=Math.min(39,Math.floor((y-20)/10)),price=(100-index*.1).toFixed(1);
tip.textContent=(model!==3&&candles&&x>=795&&y>=209&&y<=213)
?'STAMP Open 98.1 High 98.3 Low 97.9 Close 98.05'
:'STAMP Price '+price+' Liquidation Leverage 1M';
tip.style.left='300px';tip.style.top=(e.clientY+1)+'px';tip.hidden=false});
canvas.addEventListener('pointerleave',()=>tip.hidden=true);
</script>'''


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--model',type=int,choices=(1,2,3),required=True)
    parser.add_argument('--timeframe',choices=('12H','24H','48H'),required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    # Tests cannot inherit live source sessions or database connections.
    for name in ('DATABASE_URL','COINGLASS_COOKIE_HEADER','COINGLASS_STORAGE_STATE_JSON'):
        os.environ.pop(name,None)
    os.environ.update(COLLECTION_HEATMAP_MODEL=str(args.model),COLLECTION_BRIDGE_ENABLED='false',
                      OPENAI_API_KEY='offline-only-fixture')
    sys.path.insert(0,str(ROOT/'standalone_collector/runtime'))
    import collection_model1_task as task
    import model3_rendering as rendering
    from market_vision import coinglass_heatmap_capture as capture,openai_heatmap_scanner as scanner
    from playwright.sync_api import Browser,Page
    from types import SimpleNamespace
    original_context=Browser.new_context;original_shot=Page.screenshot
    original_preparation=rendering.preparation;original_observation=rendering.observation
    counts={'contexts':0,'screenshots':0,'ai_requests':0,'early_active':0,'late_active':0,'restored':0}
    browsers=[];contexts=[];operations=[]
    stamp=datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')
    html=HTML.replace('MODEL',str(args.model)).replace('STAMP',stamp)
    def context(browser,*a,**kw):
        result=original_context(browser,*a,**kw);counts['contexts']+=1;contexts.append(result)
        if browser not in browsers:browsers.append(browser)
        result.route('**/*',lambda route:route.fulfill(status=200,content_type='text/html',body=html))
        return result
    def shot(page,*a,**kw):counts['screenshots']+=1;return original_shot(page,*a,**kw)
    def enabled(page):return page.evaluate('()=>globalThis.__cg29RenderLab?.snapshot().some(x=>x.enabled)||false')
    @contextmanager
    def early(page,model):
        with original_preparation(page,model):
            counts['early_active']+=int(enabled(page));yield
        assert not enabled(page);counts['restored']+=1
    @contextmanager
    def late(page,model,geometry):
        with original_observation(page,model,geometry) as clean:
            counts['late_active']+=int(enabled(page));yield clean
        assert not enabled(page)
    def response(url,**kw):
        assert url==scanner.OPENAI_RESPONSES_URL
        assert all(not b.is_connected() for b in browsers),'Browser must close before analysis'
        counts['ai_requests']+=1
        contents=kw['json']['input'][0]['content']
        supplied=next(c['text'] for c in contents if c.get('text','').startswith('Code-computed bands'))
        data=json.loads(supplied.split('\n',1)[1].split('\nCoordinate mapping',1)[0])
        scan={'timeframe':args.timeframe.lower(),'observed_timeframe':args.timeframe.lower(),
              'observed_symbol':'BTC','observed_mode':'Symbol','observed_model':f'Model {args.model}',
              'readable':True,'blocking_condition':'none','short_summary':'Offline fixture',
              'price_axis_range':{'low':96.1,'high':100,'price_evidence':'96.1 100'},
              'prominent_levels':[{'candidate_id':c['id'],'right_edge_visible':True,'strength_score':.8}
                                  for c in data['candidates']]}
        value={'status':'completed','output_text':json.dumps({'symbol':'BTC','analysis_mode':'visual_screenshot','scans':[scan]}),
               'usage':{'input_tokens':0,'output_tokens':0,'total_tokens':0}}
        return SimpleNamespace(ok=True,status_code=200,json=lambda:value)
    args.output.mkdir(parents=True,exist_ok=True)
    with patch.object(Browser,'new_context',context),patch.object(Page,'screenshot',shot),\
         patch.object(rendering,'preparation',early),patch.object(rendering,'observation',late),\
         patch.object(scanner.requests,'post',response):
        task.main(args.timeframe,str(uuid.uuid4()),str(args.output))
    result=json.loads((args.output/'result.json').read_text())
    assert result['heatmap_model']==args.model and result['timeframe']==args.timeframe
    assert result['hover_snapshot']['complete'] is True
    assert len(result['hover_snapshot']['rows'])==40
    assert len(result['zones'])==1
    assert counts['contexts']==counts['screenshots']==counts['ai_requests']==1
    assert counts['early_active']==int(args.model==3)
    # The late observer is also used internally by the early scope.
    assert counts['late_active']==(2 if args.model==3 else 0)
    assert all(not b.is_connected() for b in browsers)
    summary={'model':args.model,'timeframe':args.timeframe,'status':'passed',**counts,
             'rows':40,'external_source':False,'external_ai':False,'database_writes':0}
    (args.output/'check.json').write_text(json.dumps(summary,indent=2))
    print(json.dumps(summary))

if __name__=='__main__':main()
