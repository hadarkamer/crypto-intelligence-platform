"""Prove the selected time from the visible CoinGlass tooltip and bottom crosshair."""
from contextlib import nullcontext
from datetime import timedelta
from math import isfinite
import re

import modern_map_reader as ui
from model1_execution import StageFailure
from modern_map_basic_numbers import HORIZON_SECONDS, period_seconds
from modern_map_evidence import iso, source_clock

# The bottom timestamp can be canvas text, while the floating tooltip is accessible
# DOM. Only return public chart labels, their positions, and numeric axis marks.
# Store pointer-off pixels of the bottom strip locally inside the page. A fixed
# axis tick repainted by a mouse event has exactly its original appearance.
_INSTALL = r"""(watch, [box, plot]) => {
  const [bx,by,bw,bh]=box, [left,top,right,bottom]=plot;
  const date=/^\d{1,2}\/\d{1,2} \d{1,2}:\d{2}$/;
  const caption=/^(?:Latest column|(?:\d+\s*(?:days?|d|hours?|h|minutes?|min|seconds?|s)\s*)+before the latest)\s*[·•]\s*\d+\s*(?:seconds?|s|minutes?|min|hours?|h|days?|d)\s+per column$/i;
  const normalize=s=>s.replace(/\s+/g,' ').trim();
  const plotCanvas=[...document.querySelectorAll('canvas')].filter(c=>{
    const r=c.getBoundingClientRect();
    return Math.abs(r.x-bx)<=3 && Math.abs(r.y-by)<=3 && Math.abs(r.width-bw)<=4 && Math.abs(r.height-bh)<=4;
  });
  const strip=document.createElement('canvas');
  strip.width=Math.ceil(bw); strip.height=Math.max(1,Math.ceil(by+bh-bottom+2));
  const ctx=strip.getContext('2d',{willReadFrequently:true});
  function pixels(){
    ctx.clearRect(0,0,strip.width,strip.height);
    for(const c of plotCanvas) {
      const r=c.getBoundingClientRect();
      ctx.drawImage(c,r.x-bx,r.y-bottom+2,r.width,r.height);
    }
    try { return ctx.getImageData(0,0,strip.width,strip.height).data; }
    catch { return null; }
  }
  let offPixels=null, lastMarks=[], offDom=new Set();
  const glyph=document.createElement('canvas');glyph.width=strip.width;glyph.height=strip.height;
  const ink=glyph.getContext('2d',{willReadFrequently:true});
  function changed(mark,current) {
    const g=mark[6];
    if(!offPixels || !current || !g || !g.colour || g.rotated || g.sx<=0 || g.sy<=0)return false;
    // Compare actual ink pixels of THIS text to pointer-off pixels. A moving
    // line or an unrelated badge behind a fixed tick cannot certify that tick.
    ink.setTransform(1,0,0,1,0,0);ink.clearRect(0,0,glyph.width,glyph.height);
    ink.fillStyle=g.colour;ink.fillRect(0,0,1,1);
    const rgb=ink.getImageData(0,0,1,1).data;
    if(rgb[3]<200)return false;
    ink.clearRect(0,0,1,1);
    const x=mark[1]-bx,y=mark[2]-bottom+2;
    ink.setTransform(g.sx,0,0,g.sy,x,y);ink.font=g.font;ink.textAlign=g.align;ink.textBaseline=g.baseline;
    if(g.direction==='ltr'||g.direction==='rtl')ink.direction=g.direction;
    ink.fillStyle='white';ink.fillText(mark[0],0,0);
    const t=ink.measureText(mark[0]);
    const x1=Math.max(0,Math.floor(x-g.sx*t.actualBoundingBoxLeft)-1);
    const y1=Math.max(0,Math.floor(y-g.sy*t.actualBoundingBoxAscent)-1);
    const w=Math.min(glyph.width,Math.ceil(x+g.sx*t.actualBoundingBoxRight)+1)-x1;
    const h=Math.min(glyph.height,Math.ceil(y+g.sy*t.actualBoundingBoxDescent)+1)-y1;
    if(w<=0 || h<=0)return false;
    const mask=ink.getImageData(x1,y1,w,h).data;
    const visible=[0,0,0,0],appeared=[0,0,0,0];
    const delta=(a,i,b,j)=>Math.max(Math.abs(a[i]-b[j]),Math.abs(a[i+1]-b[j+1]),Math.abs(a[i+2]-b[j+2]),Math.abs(a[i+3]-b[j+3]));
    for(let yy=1;yy<h-1;yy++)for(let xx=1;xx<w-1;xx++){
      const m=(yy*w+xx)*4;
      if(mask[m+3]<175)continue;
      const i=((yy+y1)*strip.width+xx+x1)*4;
      if(delta(current,i,rgb,0)>68)continue;
      let activeContrast=0,offContrast=0;
      for(let dy=-1;dy<=1;dy++)for(let dx=-1;dx<=1;dx++){
        if(mask[m+4*(dy*w+dx)+3]>65)continue;
        const j=i+4*(dy*strip.width+dx);
        activeContrast=Math.max(activeContrast,delta(current,i,current,j));
        offContrast=Math.max(offContrast,delta(offPixels,i,offPixels,j));
      }
      if(activeContrast<40)continue;
      const quarter=Math.min(3,Math.floor(xx*4/w));
      visible[quarter]++;
      // A light letter on a new dark badge may have the same centre colour
      // as an empty light axis. Verify the presence of the printed edges.
      if(delta(offPixels,i,rgb,0)>90 || offContrast<24)appeared[quarter]++;
    }
    // Require changed letter edges across the width of a date, not a
    // pixel threshold in one area a vertical guide could cross.
    return visible.filter((v,k)=>v>=3 && appeared[k]>=3 && appeared[k]*3>=v).length>=3;
  }
  function displayed(e){
    if(e.closest('nav,header,footer,form,dialog,input,iframe,[role=dialog],[contenteditable=true]'))return null;
    if(!e.checkVisibility({checkOpacity:true,checkVisibilityCSS:true}))return null;
    const r=e.getBoundingClientRect();
    if(!r.width || !r.height || r.left<bx-2 || r.right>bx+bw+2 ||
       r.top<by-2 || r.bottom>by+bh+2)return null;
    return r;
  }
  function scan(){
    const dates=[], bodies=[], axis=[], samples=[];
    // Seed from short text nodes only. Ancestor text is read solely for the
    // small visible market labels (the caption sometimes has nested spans).
    const walker=document.createTreeWalker(document.body,NodeFilter.SHOW_TEXT);
    const seen=new Set();
    while(walker.nextNode()){
      const node=walker.currentNode;
      if(!/\d{1,2}\/\d{1,2}|latest|column/i.test(node.nodeValue))continue;
      let e=node.parentElement;
      while(e && !seen.has(e) && e!==document.body && e.textContent.length<=180){
        seen.add(e);
        const inline=normalize(e.innerText||'');
        // Adjacent inline spans need not contain a literal text-space.
        const pieces=[...e.childNodes].map(n=>n.nodeType===Node.TEXT_NODE?n.nodeValue:
          n.nodeType===Node.ELEMENT_NODE && n.checkVisibility({checkOpacity:true,checkVisibilityCSS:true})?n.innerText:'');
        const joined=normalize(pieces.join(' '));
        const t=(date.test(inline)||caption.test(inline))?inline:joined;
        if(date.test(t) || caption.test(t)){
          const r=displayed(e);
          if(r) {
            const m={e,t,r};
            if(date.test(t)) {
              if(r.top>=bottom-2){
                const style=getComputedStyle(e);
                // Replacing a DOM node with the same printed text, style and
                // geometry is still a fixed tick, just like a canvas repaint.
                axis.push([t,Math.round((r.left+r.right)/2),Math.round(r.top),'dom',
                  [style.font,style.color,style.backgroundColor,Math.round(r.width)].join('|')]);
              }
              else dates.push(m);
            } else if(r.top>=top && r.bottom<=bottom) bodies.push(m);
            // Prefer the leaf node to a wrapper repeating its entire text.
            break;
          }
        }
        e=e.parentElement;
      }
    }
    for(const b of bodies) {
      let e=b.e.parentElement;
      while(e && e!==document.body){
        const r=displayed(e);
        if(!r || r.right<left || r.left>right || r.top<top-2 || r.bottom>bottom+2)break;
        const titles=dates.filter(h=>e.contains(h.e));
        if(titles.length){
          // Body and header must be in the same compact tooltip. The enclosing
          // chart is never an acceptable common ancestor of unrelated labels.
          if(bodies.filter(q=>e.contains(q.e)).length===1 && titles.length===1 &&
             r.width < (right-left)*.9 && r.height < (bottom-top)*.95 &&
             titles[0].r.top<=b.r.top && b.r.top-titles[0].r.bottom<=Math.max(36,2*b.r.height))
            samples.push([titles[0].t,b.t]);
          break;
        }
        e=e.parentElement;
      }
    }
    return {axis,samples};
  }
  watch.cursorOff=()=>{
    const {axis,samples}=scan();
    if(samples.length)return false;
    offDom=new Set(axis.map(m=>JSON.stringify(m)));
    offPixels=pixels(); watch.labels.length=0; lastMarks=[]; return true;
  };
  watch.cursorMove=()=>{watch.labels.length=0;lastMarks=[];};
  watch.cursorRead=()=>{
    const {axis,samples}=scan();
    const now=watch.labels.splice(0);
    if(now.some(m=>date.test(m[0]) && m[2]>=bottom-2))lastMarks=now;
    else if(!lastMarks.length && now.length)lastMarks=now;
    const dated=lastMarks.filter(m=>date.test(m[0]) && m[2]>=bottom-2 && m[2]<=by+bh+2);
    const current=dated.length?pixels():null;
    const marks=dated.map(m=>[m[0],Math.round(m[1]),Math.round(m[2]),'canvas',m[5]||'']);
    return { tooltip:samples, all_axis:[...axis,...marks], axis:[
      ...axis.filter(m=>!offDom.has(JSON.stringify(m))),
      ...marks.filter((m,i)=>changed(dated[i],current))
    ], marks:lastMarks.filter(m=>!date.test(m[0])).slice(-1800)};
  };
}"""

_CAPTION = re.compile(r'(Latest column|(.+?) before the latest)\s*[·•]\s*(\d+)\s*(\w+) per column', re.I)
_DURATION = re.compile(r'(\d+)\s*(days?|d|hours?|h|minutes?|min|seconds?|s)\b',re.I)


def _seconds(parts):
    return sum(int(n)*{'d':86400,'h':3600,'m':60,'s':1}[unit.lower()[0]] for n,unit in parts)


def caption_time(text, period):
    match=_CAPTION.fullmatch(text)
    if not match or not _DURATION.fullmatch(match[3]+' '+match[4]):
        return None
    if _seconds([(match[3],match[4])])!=period:
        return None
    if match[2] is None:
        return 0
    if _DURATION.sub('',match[2]).strip():
        return None
    lag=_seconds(_DURATION.findall(match[2]))
    return lag if lag>0 and lag%period==0 else None


def _fail(reason):
    error=StageFailure('source_not_readable','hover_evidence_mismatch')
    error.basic_reason=reason
    raise error


def decode(frame, x, period, latest):
    tips=set(tuple(t) for t in frame['tooltip'])
    if len(tips)!=1:
        return None
    stamp,body=tips.pop()
    lag=caption_time(body,period)
    if lag is None or (lag==0)!=latest:
        return None
    nearby={tuple(m) for m in frame['axis'] if abs(m[1]-x)<95}
    if stamp not in {m[0] for m in nearby}:
        return None
    return stamp,lag,nearby


def read_cursor_pair(page, box, bounds, anchor, timeframe, period, *, expected_column=None, observe=None, refresh_anchor=None):
    """Establish two source-selected cells in one document. Leave the pointer off."""
    seconds=period_seconds(period)
    left,top,right,bottom=bounds
    # The inclusive last native pixel is the only candidate for the edge.
    # Coordinates only select a cell; source text verifies which one it is.
    prior_x=left+.72*(right-left)
    y=round((top+bottom)/2)
    off=(max(0,box[0]-2),max(0,box[1]-2))
    watch=page.evaluate_handle(ui.PAINT,[*box,True])
    restored=False
    def restore():
        nonlocal restored
        if not restored:
            restored=True
            try:watch.evaluate('w=>w.restore()')
            finally:watch.dispose()
    def finish_probe():
        try:restore()
        finally:page.mouse.move(*off)
    try:
        watch.evaluate(_INSTALL,[box,bounds])
        def outside():
            page.mouse.move(*off)
            ui.wait_for(page,lambda:watch.evaluate('w=>w.cursorOff()'),timeout=3)
        def at(x,latest):
            outside()
            watch.evaluate('w=>w.cursorMove()')
            page.mouse.move(x,y)
            pending=None
            last={}
            def settled():
                nonlocal pending
                frame=watch.evaluate('w=>w.cursorRead()')
                tips=frame['tooltip']
                last.pop('displayed_time',None)
                last.pop('displayed_lag_seconds',None)
                last['tooltip_count']=len(tips)
                last['axis_time_count']=len({m[0] for m in frame['axis'] if abs(m[1]-x)<95})
                if len(tips)==1:
                    last['displayed_time']=tips[0][0]
                    lag=caption_time(tips[0][1],seconds)
                    if lag is not None:last['displayed_lag_seconds']=lag
                result=decode(frame,x,seconds,latest)
                if not result:
                    pending=None
                    return None
                key=(result[0],result[1],frozenset(result[2]))
                if key!=pending:
                    pending=key
                    return None
                return dict(stamp=result[0],lag=result[1],proof=result[2],caption=frame['tooltip'][0][1],
                            all_axis=set(tuple(a) for a in frame['all_axis']),marks=frame['marks'])
            try:
                result=ui.wait_for(page,settled,timeout=3)
                if observe:observe('latest' if latest else 'prior','observed',last)
                return result
            except StageFailure as error:
                if error.code!='source_not_readable':raise
                if observe:observe('latest' if latest else 'prior','failed',last)
                _fail('latest_cursor_time_unconfirmed' if latest else 'prior_cursor_time_unconfirmed')

        # The off-frame and both consistent polls must refer to the same pointer.
        # Synchronous standalone readers already own their browser's only page.
        pointer_scope = getattr(page, 'pointer_scope', nullcontext)
        def probe(x, latest):
            with pointer_scope():
                try:
                    observed = at(x, latest)
                except BaseException:
                    # Release while this reader still owns the pointer. Preserve
                    # the original source failure if a closed/blocked tab cannot move.
                    try:
                        finish_probe()
                    except Exception:
                        pass
                    raise
                if latest:
                    finish_probe()
                return observed
        prior=probe(prior_x,False)
        current=probe(right,True)
        # Even an identical midnight/axis text can be a valid cursor label
        # if its moving rendering has its own witness. A stationary repaint
        # shared by the two locations can never be that witness.
        if ({m[0] for m in prior['proof']-current['all_axis']}!={prior['stamp']} or
                {m[0] for m in current['proof']-prior['all_axis']}!={current['stamp']}):
            _fail('stationary_cursor_time')
        if expected_column is not None and refresh_anchor is not None:
            # Only a different visible minute needs a new clock. Never turn an
            # unproved tooltip, neighbouring cell or malformed date into a rollover.
            components = tuple(map(int, re.findall(r'\d+', current['stamp'])))
            expected = (expected_column.month, expected_column.day, expected_column.hour, expected_column.minute)
            if components != expected:
                anchor = refresh_anchor()
        column=source_clock(current['stamp'],anchor,column=True,max_lag=seconds)
        previous=source_clock(prior['stamp'],column,column=True,max_lag=HORIZON_SECONDS[timeframe])
        if column>anchor or column-previous!=timedelta(seconds=prior['lag']):
            _fail('cursor_column_distance_disagrees')
        if expected_column is not None and column!=expected_column:
            if (refresh_anchor is not None and column>expected_column and
                    (column-expected_column).total_seconds()%seconds == 0):
                error = StageFailure('source_not_readable','hover_evidence_mismatch')
                error.basic_reason = 'closing_column_changed'
                error.verified_change = dict(condition=error.basic_reason, opening_column_time=iso(expected_column),
                                             closing_column_time=iso(column), closing_source_time=iso(anchor))
                raise error
            _fail('closing_column_changed')
        sample = dict(period_seconds=seconds,prior_time=iso(previous),latest_time=iso(column),
                      prior_lag_seconds=prior['lag'],prior_x=prior_x,latest_x=right,pointer_y=y,
                      prior_axis_sources=sorted({m[3] for m in prior['proof']-current['all_axis']}),
                      latest_axis_sources=sorted({m[3] for m in current['proof']-prior['all_axis']}),
                      prior_tooltip=prior['caption'],latest_tooltip=current['caption'])
        return prior,current,sample
    except BaseException:
        # If acquisition or installation failed, restore just the observer. No
        # pointer move is needed, and cleanup must not mask the first failure.
        try:restore()
        except Exception:pass
        raise



def validate_receipt(proof, period, horizon, column):
    from modern_map_basic_numbers import stamp
    if not isinstance(proof,dict) or set(proof)!={'schema','opening','closing'} or proof['schema']!='coinglass.cursor.v1':
        _fail('cursor_receipt_invalid')
    seconds=period_seconds(period)
    for phase in ('opening','closing'):
        s=proof[phase]
        required={'period_seconds','prior_time','latest_time','prior_lag_seconds','prior_x','latest_x','pointer_y',
                  'prior_axis_sources','latest_axis_sources','prior_tooltip','latest_tooltip'}
        if not isinstance(s,dict) or set(s)!=required:
            _fail('cursor_receipt_invalid')
        lag=s['prior_lag_seconds']
        if (type(s['period_seconds']) is not int or s['period_seconds']!=seconds or type(lag) is not int
                or not 0<lag<=HORIZON_SECONDS[horizon] or lag%seconds or s['latest_time']!=column
                or any(not isinstance(s[k],str) or len(s[k])>128 for k in ('prior_tooltip','latest_tooltip','prior_time','latest_time'))
                or caption_time(s['latest_tooltip'],seconds)!=0 or caption_time(s['prior_tooltip'],seconds)!=lag
                or iso(stamp(s['prior_time']))!=s['prior_time'] or iso(stamp(column))!=column
                or (stamp(column)-stamp(s['prior_time'])).total_seconds()!=lag
                or any(type(s[k]) not in (int,float) or not isfinite(s[k]) or not 0<=s[k]<=100000
                       for k in ('prior_x','latest_x','pointer_y'))
                or s['prior_x']>=s['latest_x'] or any(s[k] not in (['canvas'],['dom'],['canvas','dom'])
                       for k in ('prior_axis_sources','latest_axis_sources'))):
            _fail('cursor_receipt_invalid')
