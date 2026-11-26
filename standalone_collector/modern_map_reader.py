"""Read the public, rendered CoinGlass map with a single isolated browser page."""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from io import BytesIO
import math
import re
import time

from PIL import Image

from modern_map_evidence import (PRICE, check, equivalent, iso, measurement, number, observation,
                                 sample_groups, source_clock, validate_groups, verified_member)
from model1_execution import StageFailure

RANGES = {'12H':'12 hour', '24H':'24 hour', '48H':'48 hour'}
GATE = re.compile(r'sign in to view (?:the )?liquidation heatmap', re.I)


_DEADLINE = ContextVar('protected_probe_deadline', default=None)


@contextmanager
def map_access_budget(seconds):
    token = _DEADLINE.set(time.monotonic() + seconds)
    try:
        yield
    finally:
        _DEADLINE.reset(token)


def limited_timeout(milliseconds):
    deadline = _DEADLINE.get()
    if deadline is None:
        return milliseconds
    remaining = int((deadline - time.monotonic()) * 1000)
    if remaining <= 0:
        raise StageFailure('source_timeout')
    return min(remaining, milliseconds)


def map_url(symbol):
    if not isinstance(symbol,str) or not re.fullmatch('[A-Z0-9]{1,20}',symbol):
        raise ValueError('Invalid configured symbol')
    return f'https://www.coinglass.com/LiquidationHeatMap/{symbol}?mode=coin'


def visible(items):
    limited_timeout(3000)
    return [items.nth(index) for index in range(min(60,items.count())) if items.nth(index).is_visible()]


def one(items):
    matches = visible(items)
    check(len(matches) == 1)
    return matches[0]


def gate(page):
    if visible(page.get_by_text(GATE)):
        raise StageFailure('source_login_required')


def wait_for(page, read, timeout=8, *, requested=True):
    end = time.monotonic()+limited_timeout(timeout*1000)/1000
    while True:
        limited_timeout(3000)
        if requested:gate(page)
        last_reason = None
        try:
            value = read()
            if value:
                return value
        except StageFailure as error:
            if error.code != 'source_not_readable':
                raise
            last_reason = error.reason
        if time.monotonic() >= end:
            raise StageFailure('source_not_readable', last_reason)
        page.wait_for_timeout(limited_timeout(120))


def label(page, text):
    return one(page.get_by_text(text, exact=True))


def choice_target(page, wanted):
    # The source paints a second copy of every label for sizing/animation.
    # Only the LI in the actual segmented control owns its selected state.
    scoped = page.locator('li,[role=tab],button').filter(has=page.get_by_text(wanted,exact=True))
    matches = visible(scoped)
    if len(matches)==1:return matches[0]
    return one(page.get_by_role('button',name=wanted,exact=True))


def choice(page,wanted):
    return choice_target(page,wanted).evaluate("""n => {
      const active=e=>e.getAttribute('aria-pressed')==='true' || e.getAttribute('aria-selected')==='true' ||
          e.getAttribute('data-state')==='active' || [...e.classList].some(c=>/(?:^|_)(?:on|active|selected)(?:__|$)/i.test(c));
      const group=[...n.parentElement.children].filter(e=>e.matches('li,[role=tab],button'));
      return group.includes(n) && group.length>=2 && group.filter(active).length===1 && active(n);
    }""")


def symbol_control(page,symbol):
    # Coin is the accessible name of the picker. Its text names the asset.
    picker=one(page.get_by_role('button',name='Coin',exact=True))
    check(picker.inner_text().strip()==symbol)
    return picker


def select_choice(page,wanted,*,requested=True):
    if not choice(page,wanted):
        choice_target(page,wanted).click(timeout=limited_timeout(2500))
    wait_for(page,lambda:choice(page,wanted),requested=requested)


def option_control(page, name):
    # Use the visible caption's closest control group. Never select a nearby
    # menu by ordinal when the source changes layout.
    caption = label(page,name)
    handle = caption.evaluate_handle("""el => {
      for(let n=el; n && n!==document.body; n=n.parentElement) {
        const controls=[...n.querySelectorAll('[role=combobox]')]
          .filter(e=>e.getBoundingClientRect().width);
        if(controls.length) return controls.length===1 ? controls[0] : null;
      }
      return null;
    }""").as_element()
    check(handle)
    return handle


def select_option(page, name, value):
    control = option_control(page,name)
    def matches():
        text = option_control(page,name).inner_text().strip()
        return text == value or (name == 'Colours' and re.fullmatch('Viridis(?:\\s*Default)?',text))
    if not matches():
        control.click(timeout=limited_timeout(2500))
        menu = visible(page.get_by_role('listbox')) or visible(page.get_by_role('menu'))
        scope = menu[0] if len(menu)==1 else page
        option = scope.get_by_text(value, exact=True)
        one(option).click(timeout=limited_timeout(2500))
    wait_for(page,matches)


def details(page):
    # Restrict account-independent observations to chart controls and the two
    # market sidebars. Collect each metric within its own nearest card/row.
    def field(title):
        el = label(page,title)
        return el.evaluate("""el => {
          const title=el.innerText.trim();
          for(let p=el.parentElement;p && p!==document.body;p=p.parentElement){
            const text=p.innerText.trim();
            if(text!==title && /[0-9]/.test(text.slice(title.length))) return text;
          }
          return '';
        }""")
    fields = [field(k) for k in ('Open liquidation levels (total)','Longs (below price)',
                                'Shorts (above price)','Price step / column period')]
    updated = one(page.get_by_text(re.compile(r'^Updated\s+\d{2}:\d{2}$',re.I))).inner_text()
    return observation('\n'.join(fields + [updated]),datetime.now(timezone.utc))


def prefer_dark(page):
    # If the source explicitly offers a dark-theme action, choose it. The
    # independently checked plot pixels below remain authoritative.
    options = visible(page.get_by_role('button',name=re.compile(
        r'^(?:switch to dark(?: mode| theme)?|use dark (?:theme|mode)|dark (?:theme|mode))$',re.I)))
    if len(options)==1:options[0].click(timeout=limited_timeout(2500))


from collection_metrics import phase


@phase('map_access')
def open_verified_map(page, symbol, model, timeframe, *, guard=None):
    if type(model) is not int or model not in (1,2,3) or timeframe not in RANGES:
        raise ValueError('Invalid requested heatmap identity')
    url = map_url(symbol)
    response = page.goto(url,wait_until='domcontentloaded',timeout=limited_timeout(40000))
    if guard:
        if response and response.status == 429 and page.url == url:
            from dedicated_login_form import retry_after_seconds
            error = StageFailure('source_rate_limited')
            error.retry_after_seconds = retry_after_seconds(response.header_value('retry-after'))
            raise error
        if response and response.status == 404 and page.url == url:
            raise StageFailure('source_map_unavailable')
        if response and response.status >= 500:
            raise StageFailure('source_capture_failed')
        # Controls can initially show a different, gated horizon. Handle
        # redirects now. Apply the access gate after switching to the request.
        if page.url != url:
            guard()
    page.set_default_timeout(limited_timeout(1000 if _DEADLINE.get() is not None else 3000))
    wait_for(page,lambda: choice_target(page, f'Model {model}'),timeout=25,requested=False)
    prefer_dark(page)
    select_choice(page,f'Model {model}',requested=False)
    select_choice(page,'Coin',requested=False)
    select_option(page,'Range',RANGES[timeframe])
    def ready():
        check(page.url == url)
        check(not visible(page.get_by_text(re.compile(r'^Loading(?:…|\.\.\.)$'))))
        check(choice(page,f'Model {model}') and choice(page,'Coin'))
        # The public name and the visible symbol must both agree with the URL.
        check(visible(page.get_by_text(re.compile('^'+re.escape(symbol)+r' Liquidation Heatmap$',re.I))))
        symbol_control(page,symbol)
        check(option_control(page,'Range').inner_text().strip() == RANGES[timeframe])
        check(visible(page.get_by_text('Market overview',exact=True)))
        return details(page)
    result = wait_for(page,ready,timeout=24)
    # A public shell can retain sidebars while the protected chart is still
    # loading. Require real chart geometry, source level labels and multiple
    # coloured liquidation rows before reporting authentication success.
    def plotted():
        rail_anchor(page)
        x,y,w,h=chart_box(page)
        check(x>=0 and y>=0)
        shot=protected_clip(page, {'x':x,'y':y,'width':w,'height':h}, guard)
        with Image.open(BytesIO(shot)) as chart:
            pixels=chart.convert('RGB')
            try:
                # Sample the interior; exclude toolbar text, axes and overlay
                # controls. A blank canvas or login-pressure card cannot pass.
                for fraction in (.72,.81,.90):
                    xx=round(w*fraction)
                    palette={tuple(c//20 for c in pixels.getpixel((xx,yy))) for yy in range(h//8,h*7//8,2)
                             if max(pixels.getpixel((xx,yy)))-min(pixels.getpixel((xx,yy)))>55}
                    if len(palette)>=5:return True
                return False
            finally:pixels.close()
    wait_for(page,plotted,timeout=10)
    if guard:guard()
    if timeframe=='12H':
        from session_capture import note_protected_map_ready
        note_protected_map_ready(page)
    return result


def chart_box(page):
    canvases = page.locator('canvas')
    boxes = [c.bounding_box() for c in visible(canvases)]
    boxes = [b for b in boxes if b and b['width']>=400 and b['height']>=200]
    check(boxes)
    box = max(boxes,key=lambda b:b['width']*b['height'])
    return tuple(round(box[k]) for k in ('x','y','width','height'))


def position_chart(page):
    box = chart_box(page)
    viewport = page.viewport_size
    if not viewport:
        return box
    x, y, width, height = box
    overflow = y + height - viewport['height']
    if (overflow > 0 and height <= viewport['height'] and y >= 0 and
            0 <= x and x + width <= viewport['width']):
        # Keep every chart row. After the minimal document scroll, the full
        # canvas and control rectangles still have to pass the capture checks.
        page.evaluate("dy => window.scrollBy({top:dy,left:0,behavior:'instant'})", overflow)
        return chart_box(page)
    return box


# Observe only text actually painted by the visible chart. This hook runs on
# this page's canvas instances while the crosshair is displayed. It never
# inspects the chart's data model, network, browser storage or client bundles.
PAINT = """box => {
  const undo=[], labels=[];
  const [bx,by,bw,bh]=box;
  for(const c of document.querySelectorAll('canvas')){
    const r=c.getBoundingClientRect();
    if(Math.abs(r.x-bx)>3 || Math.abs(r.y-by)>3 || Math.abs(r.width-bw)>4 || Math.abs(r.height-bh)>4) continue;
    const ctx=c.getContext('2d'); if(!ctx) continue;
    const own=Object.getOwnPropertyDescriptor(ctx,'fillText'), orig=ctx.fillText;
    ctx.fillText=function(txt,x,y,...args){
      const result=orig.call(this,txt,x,y,...args);
      if(/^(?:[0-9]{1,2}\\/[0-9]{1,2} [0-9]{2}:[0-9]{2}|[0-9,]+(?:\\.\\d+)?)$/.test(String(txt))) {
        const p=new DOMPoint(x,y).matrixTransform(this.getTransform());
        const px=r.x+p.x*r.width/c.width, py=r.y+p.y*r.height/c.height;
        if(Number.isFinite(px+py)) labels.push([String(txt),px,py,this.textBaseline]);
        if(labels.length>6000)labels.splice(0,3000);
      }
      return result;
    };
    undo.push(()=>{if(own)Object.defineProperty(ctx,'fillText',own);else delete ctx.fillText;});
  }
  return {labels, restore(){undo.forEach(f=>f())}};
}"""


def read_paint(page, box, x, y):
    page.mouse.move(box[0]-2, max(1,box[1]-2))
    watch = page.evaluate_handle(PAINT, list(box))
    try:
        page.mouse.move(x-2,y)
        page.mouse.move(x,y)
        def timestamp():
            labels=watch.evaluate('w=>w.labels')
            stamps = {p[0] for p in labels if '/' in p[0] and abs(p[1]-x)<115 and
                      box[1]+.78*box[3] <= p[2] <= box[1]+box[3]}
            for marker in visible(page.get_by_text(re.compile(r'^\d{1,2}/\d{1,2} \d{2}:\d{2}$'))):
                pos=marker.bounding_box()
                if (pos and abs(pos['x']+pos['width']/2-x)<95 and
                    box[1]+.78*box[3]<=pos['y']<=box[1]+box[3]):
                    stamps.add(marker.inner_text().strip())
            return (stamps.pop(),labels) if len(stamps)==1 else None
        return wait_for(page,timestamp,timeout=3)
    finally:
        watch.evaluate('w=>w.restore()')
        watch.dispose()
        page.mouse.move(box[0]-2,max(1,box[1]-2))


def dark_bounds(image, box):
    # In native dark Viridis the plot background itself is dark purple; a
    # reversed light palette cannot pass this pixel check. Find its rectangle
    # independently of grid labels, legend and the graphic's watermark.
    x,y,w,h=box
    pix=image.load()
    def purple(xx,yy):
        r,g,b=pix[xx,yy]
        return 36<=r<=110 and g<=30 and b>=r and b<=145
    def longest(points):
        runs=[]
        for n in points:
            if runs and runs[-1][-1]+1==n:runs[-1].append(n)
            else:runs.append([n])
        return max(runs,key=len,default=[])
    ys=longest([yy for yy in range(y,y+h) if sum(purple(xx,yy) for xx in range(x,x+w,9))>w/9*.28])
    # Bright bands can occupy entire rows. Use column coverage as independent
    # evidence and restore interrupted native rows between the first/last.
    broad=[yy for yy in range(y,y+h) if sum(purple(xx,yy) for xx in range(x,x+w,8))>w/8*.28]
    check(ys and len(broad)>.30*h)
    top,bottom=broad[0],broad[-1]
    cols=[xx for xx in range(x,x+w) if sum(purple(xx,yy) for yy in range(top,bottom+1,5))>(bottom-top)/5*.21]
    xs=longest(cols)
    check(xs and xs[-1]-xs[0]>.6*w and bottom-top>.55*h)
    return xs[0],top,xs[-1],bottom


def rail_anchor(page):
    row = visible(page.locator('[data-lv]'))
    for item in row:
        for text in item.inner_text().splitlines():
            if re.fullmatch(PRICE,text.strip()):
                return float(number(text.strip())[0])
    raise StageFailure('source_not_readable')


def measure_box(page, bounds, y1, y2):
    left,top,right,bottom=bounds
    check(top<=y1<y2<=bottom)
    # Pixel x is inside the latest painted column. The wide drag also
    # distinguishes a selection from a single click on the source chart.
    if not choice(page,'Measure'):
        select_choice(page,'Measure')
    page.mouse.move(max(left+2,right-85),y1)
    page.mouse.down()
    try:page.mouse.move(right-1,y2,steps=7)
    finally:page.mouse.up()
    right_edge=wait_for(page,lambda:choice_target(page,'Right edge'))
    if not choice(page,'Right edge'):
        right_edge.click(timeout=3000)
    wait_for(page,lambda:choice(page,'Right edge'))
    panel=label(page,'Largest cluster').evaluate("""el=>{
      for(let n=el;n&&n!==document.body;n=n.parentElement){
        const s=n.innerText;
        if(s.includes('Box total') && s.includes('Reference'))return s;
      }return '';
    }""")
    return measurement(panel)


def display_ready(page):
    select_option(page,'Layout','Single chart')
    select_option(page,'Lens','Heat')
    select_option(page,'Colours','Viridis')
    select_choice(page,'Pan')
    def intensity():
        return label(page,'Intensity').evaluate("""el=>{
          for(let n=el;n&&n!==document.body;n=n.parentElement) {
            const s=n.innerText;
            if(s.includes('%'))return s;
          }return '';
        }""")
    def full():
        s=intensity()
        return bool(re.search(r'(?:≥|>=)\s*0\s*%',s) and re.search(r'(?:≤|<=)\s*100\s*%',s))
    if not full():
        one(page.get_by_role('button',name=re.compile('lowest intensity',re.I))).press('Home')
        one(page.get_by_role('button',name=re.compile('full.brightness intensity',re.I))).press('End')
        wait_for(page,full)


def evidence_controls(page,symbol,model):
    controls=[choice_target(page,f'Model {model}'),choice_target(page,'Coin'),symbol_control(page,symbol)]
    controls.extend(option_control(page,key) for key in ('Range','Layout','Lens','Colours'))
    return [control.bounding_box() for control in controls]


def save_evidence(screen,box,controls,output):
    # Copy only individually identified non-account controls and the chart.
    left,top,width,height=box
    strips=[]
    try:
        for b in controls:
            check(b and 0<=b['x']<b['x']+b['width']<=screen.width and
                  0<=b['y']<b['y']+b['height']<=screen.height and b['height']<=90)
            strips.append(screen.crop((math.floor(b['x']),math.floor(b['y']),
                                       math.ceil(b['x']+b['width']),math.ceil(b['y']+b['height']))))
        header_height=max(p.height for p in strips)+12
        total_width=sum(p.width+6 for p in strips)
        check(total_width<=width)
        with Image.new('RGB',(width,height+header_height),(10,13,18)) as frame:
            x=3
            for strip in strips:
                frame.paste(strip,(x,5));x+=strip.width+6
            with screen.crop((left,top,left+width,top+height)) as chart:
                frame.paste(chart,(0,header_height))
            frame.save(output,'PNG')
    finally:
        for strip in strips:strip.close()


def protected_clip(page, box, guard):
    if guard:
        guard()
    # Inputs, dialogs, and third-party frames never belong to market evidence.
    masks = [page.locator('input,textarea,iframe,dialog,[role=dialog],[contenteditable=true]')]
    options = {'timeout': limited_timeout(3000)} if _DEADLINE.get() is not None else {}
    image = page.screenshot(clip=box, animations='disabled', mask=masks, **options)
    if guard:
        guard()
    return image


def market_surface(page, box, controls, guard):
    # Numeric routines use viewport coordinates. Build that coordinate space
    # from just the chart and identified map controls, leaving all else blank.
    viewport = page.viewport_size
    def clip_error(reason, index=None, edges=None, size=None):
        error = StageFailure('source_not_readable', reason)
        error.market_clip = {'viewport': (viewport['width'], viewport['height']) if viewport else None,
                             'rect_index': index, 'css_edges': edges, 'image_size': size}
        return error

    if not (viewport and 0 < viewport['width'] <= 4096 and 0 < viewport['height'] <= 4096):
        raise clip_error('map_viewport_unavailable')
    screen = Image.new('RGB', (viewport['width'],viewport['height']), (10,13,18))
    left, top, width, height = box
    rectangles = [dict(x=left,y=top,width=width,height=height), *controls]
    try:
        for index, rect in enumerate(rectangles):
            if not rect:
                raise clip_error('map_clip_missing', index)
            x, y = math.floor(rect['x']), math.floor(rect['y'])
            right = math.ceil(rect['x']+rect['width'])
            bottom = math.ceil(rect['y']+rect['height'])
            if not (0 <= x < right <= screen.width and 0 <= y < bottom <= screen.height):
                raise clip_error('map_clip_outside_viewport', index, (x, y, right, bottom))
            area = dict(x=x,y=y,width=right-x,height=bottom-y)
            with Image.open(BytesIO(protected_clip(page,area,guard))) as part:
                if part.size != (right-x,bottom-y):
                    raise clip_error('map_clip_size_mismatch', index, (x, y, right, bottom), part.size)
                screen.paste(part,(x,y))
        return screen
    except BaseException:
        screen.close()
        raise


def read_map(page, symbol, model, timeframe, output):
    from model1_execution import capture_operation
    with capture_operation('capture'):
        open_verified_map(page,symbol,model,timeframe)
        return read_selected_map(page,symbol,model,timeframe,output)


@phase('map_read')
def read_selected_map(page,symbol,model,timeframe,output,*,guard=None):
    if guard:guard()
    display_ready(page)
    before = details(page)
    box = position_chart(page)
    left,top,width,height=box
    page.mouse.move(max(0,left-2),max(0,top-2))
    controls=evidence_controls(page,symbol,model)
    screen = market_surface(page,box,controls,guard)
    try:
        check(0<=left and left+width<=screen.width and 0<=top and top+height<=screen.height)
        bounds = dark_bounds(screen,box)
        x0,y0,x1,y1 = bounds
        prior,_ = read_paint(page,box,x0+(x1-x0)*.72,(y1+y0)//2)
        raw_time,_ = read_paint(page,box,x1-1,(y1+y0)//2)
        update = datetime.fromisoformat(before['source_time'])
        column = source_clock(raw_time,update,column=True)
        check(column<=update and prior!=raw_time)
        previous=source_clock(prior,column,column=True,max_lag=48*3600)
        check(0<(column-previous).total_seconds()<=int(timeframe[:-1])*3600)
        raw_anchor = rail_anchor(page)
        raw_step = before['raw_price_step']
        step = float(number(raw_step)[0])
        # The first bounding rectangle spans the displayed price domain.
        # Its total must agree with the independently printed side totals
        # before those totals may be used as a group-share denominator.
        last,full,strongest,low_bound,high_bound=measure_box(page,bounds,y0+.2,y1-.2)
        equivalent(full,[before['raw_above_total'],before['raw_below_total']])
        last_price = number(last)[0]
        check(0 < low_bound < last_price < high_bound)
        def where(price):
            return y1-.2-(price-float(low_bound))*(y1-y0-.4)/float(high_bound-low_bound)
        height,runs=sample_groups(screen,bounds,model,raw_anchor,step,where,float(last_price))
        groups=[]
        deadline=time.monotonic()+90
        for colour,rows in runs:
            values=[]
            for row in rows:
                if time.monotonic()>=deadline:raise StageFailure('source_timeout')
                y = row['y']
                reference,total,member,_,_=measure_box(page,bounds,max(y0,y-.48*height),min(y1,y+.48*height))
                check(reference==last)
                verified_member(row,member,step)
                equivalent(total,[member['raw_value']])
                values.append(member)
            group_total=total
            if len(rows)>1:
                reference,group_total,leader,_,_=measure_box(page,bounds,max(y0,rows[-1]['y']-.48*height),
                                                            min(y1,rows[0]['y']+.48*height))
                check(reference==last and leader in values)
            equivalent(group_total,[m['raw_value'] for m in values])
            groups.append({'side':rows[0]['side'],'colour':colour,'raw_total':group_total,'members':values})
        # The full-height source Measure identifies the maximum cell as well.
        # A grid offset or an incorrectly clipped right edge must not omit it.
        check(any(strongest in g['members'] for g in groups))
        validate_groups(groups,last_price,raw_step,before['raw_above_total'],before['raw_below_total'])
        select_choice(page,'Pan')
        check(chart_box(page)==box)
        raw_after,_=read_paint(page,box,x1-1,(y1+y0)//2)
        after=details(page)
        check(before==after and raw_time==raw_after and page.url==map_url(symbol))
        check(choice(page,f'Model {model}') and choice(page,'Coin'))
        check(option_control(page,'Range').inner_text().strip()==RANGES[timeframe])
        gate(page)
        if guard:guard()
        save_evidence(screen,box,controls,output)
    finally:
        screen.close()
    captured = datetime.now(timezone.utc)
    check(-120 <= (captured-update).total_seconds() <= 1200)
    return dict(schema='coinglass.right-edge-measure.v1',source_url=map_url(symbol),symbol=symbol,
                heatmap_model=model,timeframe=timeframe,mode='Coin',lens='Heat',palette='Viridis',
                theme='dark',intensity=[0,100],count='Right edge',**before,
                source_time_after=after['source_time'], column_time=iso(column),column_time_after=iso(column),
                raw_last_price=last,groups=groups,captured_at=iso(captured))
