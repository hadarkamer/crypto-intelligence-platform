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

from modern_map_evidence import (PRICE, box_member, check, equivalent, iso, measurement, number, observation,
                                 sample_groups, source_clock, unresolved_measure, validate_groups,
                                 verified_member, verify_covered_maximum)
from model1_execution import StageFailure

from modern_map_basic_numbers import BASIC_RANGES, HORIZON_SECONDS, RANGE_PATTERNS, basic_target, coin_map_url as map_url, period_seconds, range_matches, valid_range_selection

# Canonical labels from the previous UI are kept for existing callers. The
# reader selects an actual visible unit-bearing option on the active coin.
RANGES = {'12H':'12 hour', '24H':'24 hour', '48H':'48 hour', '1W':'1 week'}
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


# The currency picker also has accessible name "Coin". Bind this proof to a
# single visible set of sibling mode controls, using their actual printed labels.
_MODE_SELECTOR = r"""target => {
  const selector='li,[role=tab],button';
  const shown=e=>e.checkVisibility({checkOpacity:true,checkVisibilityCSS:true}) &&
    !e.closest('[aria-hidden="true"],[inert]') && e.getClientRects().length>0;
  // CoinGlass can render two copies of the same label inside a single option
  // for its sizing animation. Distinct controls are still counted separately.
  function label(e){
    const text=e.innerText.replace(/\s+/g,'');
    if(/^(Coin)+$/.test(text))return 'Coin';
    if(/^(Pair)+$/.test(text))return 'Pair';
    return null;
  }
  const groupMember=e=>e.matches(selector)&&shown(e)&&
    (['Coin','Pair'].includes(label(e))||!e.matches('[disabled],[aria-disabled="true"]'));
  const controls=[...document.querySelectorAll(selector)].filter(e=>
    /Coin|Pair/.test(e.textContent) && shown(e) && ['Coin','Pair'].includes(label(e)));
  const groups=[...new Set(controls.map(e=>e.parentElement))].filter(group=>
    controls.some(e=>e.parentElement===group && label(e)==='Coin') &&
    controls.some(e=>e.parentElement===group && label(e)==='Pair'));
  if(groups.length!==1)return null;
  const members=[...groups[0].children].filter(groupMember);
  if(members.length!==2)return null;
  const coin=members.find(e=>label(e)==='Coin'),pair=members.find(e=>label(e)==='Pair');
  if(!coin||!pair)return null;
  function state(e){
    const votes=new Set();
    const parts=[e,...e.querySelectorAll('[aria-pressed],[aria-selected],[data-state],button,[role=tab]')];
    for(const part of parts){
      if(part!==e&&!shown(part))continue;
      for(const attr of ['aria-pressed','aria-selected','data-state']){
        if(!part.hasAttribute(attr))continue;
        const v=part.getAttribute(attr).trim().toLowerCase();
        if((attr==='data-state'?['active','on','selected']:['true']).includes(v))votes.add(true);
        else if((attr==='data-state'?['inactive','off','unselected']:['false']).includes(v))votes.add(false);
        else return null;
      }
      for(const token of part.classList){
        if(/(?:^|_)(?:on|active|selected)(?:__|$)/i.test(token))votes.add(true);
        if(/(?:^|_)(?:off|inactive|unselected)(?:__|$)/i.test(token))votes.add(false);
      }
    }
    return votes.size>1?null:votes.has(true);
  }
  const a=state(coin),b=state(pair);
  if(a===null||b===null||a===b)return null;
  return target?coin:(a?'Coin':'Pair');
}"""


def choice_target(page, wanted):
    if wanted=='Coin':
        handle=page.evaluate_handle(_MODE_SELECTOR,True)
        control=handle.as_element()
        if control is None:
            handle.dispose()
            raise StageFailure('source_not_readable')
        return control
    # The source paints a second copy of every label for sizing/animation.
    # Only the LI in the actual segmented control owns its selected state.
    scoped = page.locator('li,[role=tab],button').filter(has=page.get_by_text(wanted,exact=True))
    matches = visible(scoped)
    if len(matches)==1:return matches[0]
    return one(page.get_by_role('button',name=wanted,exact=True))


def choice(page,wanted):
    if wanted=='Coin':
        return page.evaluate(_MODE_SELECTOR,False)=='Coin'
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
    if wanted=='Coin':
        selected=page.evaluate(_MODE_SELECTOR,False)
        if selected is None:
            selected=wait_for(page,lambda:page.evaluate(_MODE_SELECTOR,False),
                              timeout=limited_timeout(2500)/1000,requested=requested)
        if selected=='Pair':
            target=choice_target(page,wanted)
            try:target.click(timeout=limited_timeout(2500))
            finally:target.dispose()
            wait_for(page,lambda:choice(page,wanted),requested=requested)
        elif requested:
            gate(page)
        return
    if choice(page,wanted):
        if requested:
            gate(page)
        return
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


def _range_value(element):
    return element.get_attribute('data-value') or element.get_attribute('value')


def selected_range(page, timeframe):
    control = option_control(page, 'Range')
    try:
        shown = control.inner_text().strip()
        selection = {'label': shown, 'value': _range_value(control)}
        check(valid_range_selection(timeframe, selection))
        return selection
    finally:
        control.dispose()


def select_range(page, timeframe):
    """Only choose a visibly labelled range in this map's Range picker."""
    control = option_control(page, 'Range')
    try:
        shown = control.inner_text().strip()
        if range_matches(timeframe, shown):
            selection = {'label': shown, 'value': _range_value(control)}
            check(valid_range_selection(timeframe, selection))
            return selection
        control.click(timeout=limited_timeout(2500))
    finally:
        control.dispose()
    def options():
        menus = visible(page.get_by_role('listbox')) or visible(page.get_by_role('menu'))
        check(len(menus)<=1)
        # Older source controls render plain labelled elements with no popup
        # role. Retain their exact-text selection and verify the trigger after.
        menu = menus[0] if menus else None
        matches = visible((menu or page).get_by_text(RANGE_PATTERNS[timeframe]))
        if len(matches)==1:
            return matches[0]
        # A partially rendered or virtual menu does not prove non-support.
        # Wait for a matching row or positive evidence of complete enumeration.
        if not matches and menu is not None and menu.evaluate("""e => {
            const rows = [...e.querySelectorAll('[role=option]')];
            return rows.length > 0 && !e.matches('[aria-busy=true]') &&
                rows.every((n,i) => n.getAttribute('aria-setsize')===String(rows.length) &&
                    n.getAttribute('aria-posinset')===String(i+1) && n.textContent.trim());
        }"""):
            gate(page)
            raise StageFailure('source_map_unavailable')
        return None
    try:
        item = wait_for(page,options,timeout=2.5,requested=False)
    except StageFailure as error:
        if error.code=='source_not_readable':
            error.basic_reason='range_option_unverified'
        raise
    option = item.evaluate_handle("""e => e.closest('[role=option],[role=menuitemradio],option,button') || e""")
    try:
        element = option.as_element()
        if element.is_disabled() or element.get_attribute('aria-disabled')=='true':
            gate(page)
            raise StageFailure('source_map_unavailable')
        value = _range_value(element)
    finally:
        option.dispose()
    item.click(timeout=limited_timeout(2500))
    selected = wait_for(page,lambda:selected_range(page,timeframe),requested=False)
    check(value is None or selected['value'] is None or value==selected['value'])
    selection = dict(selected, value=selected['value'] or value)
    check(valid_range_selection(timeframe, selection))
    return selection


def select_option(page, name, value):
    def matches(control):
        text = control.inner_text().strip()
        return text == value or (name == 'Colours' and re.fullmatch(r'Viridis(?:\s*Default)?',text))

    control = option_control(page,name)
    try:
        if matches(control):
            gate(page)
            return
        control.click(timeout=limited_timeout(2500))
    finally:
        control.dispose()
    menu = visible(page.get_by_role('listbox')) or visible(page.get_by_role('menu'))
    scope = menu[0] if len(menu)==1 else page
    one(scope.get_by_text(value, exact=True)).click(timeout=limited_timeout(2500))
    def selected():
        # The source can replace the combobox when an option is chosen.
        current = option_control(page,name)
        try:
            return matches(current)
        finally:
            current.dispose()
    wait_for(page,selected)


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


def _route_observed(url):
    # Never attach an arbitrary redirect or account URL to market evidence.
    from modern_map_basic_numbers import BASIC_SYMBOLS
    for coin in BASIC_SYMBOLS:
        for model in (1, 2, 3):
            if url == map_url(coin, model):
                return url
    return None


def _rendered_chart(page, guard):
    """Inspect the drawn heatmap, excluding controls, axes and the lower-left advertisement."""
    x, y, width, height = chart_box(page)
    check(x >= 0 and y >= 0)
    region = dict(x=x + width * 2 // 5, y=y + height // 8,
                  width=width // 2, height=height * 5 // 8)
    with Image.open(BytesIO(protected_clip(page, region, guard))) as shot:
        with shot.convert('RGB') as pixels:
            # Use the same palette evidence as the original protected-map proof.
            populated = False
            for fraction in (.64, .82, .98):
                sample = round((pixels.width - 1) * fraction)
                colours = {tuple(c // 20 for c in rgb) for y in range(0, pixels.height, 2)
                           if max(rgb := pixels.getpixel((sample, y))) - min(rgb) > 55}
                if len(colours) >= 5:
                    populated = True
                    break
            if not populated:
                return None
            with pixels.resize((32, 24), Image.Resampling.BOX) as grid:
                return grid.tobytes()


def _plot_distance(a, b):
    return sum(abs(x - y) for x, y in zip(a, b)) / len(a)


@phase('map_access')
def open_verified_map(page, symbol, model, timeframe, *, guard=None, observe=None, reset_to_entry=False):
    if type(model) is not int or model not in (1,2,3) or timeframe not in BASIC_RANGES:
        raise ValueError('Invalid requested heatmap identity')
    wanted = map_url(symbol, model)
    current_model = next((m for m in (1, 2, 3) if page.url == map_url(symbol, m)), None)
    first_document = (reset_to_entry or page.url == 'about:blank' or
                      current_model is None and _route_observed(page.url) is not None)
    if first_document:
        current_model = 1
    entry = map_url(symbol, current_model or 1)
    trace = dict(expected_url=wanted, expected_model=model, expected_symbol=symbol, expected_range=timeframe)
    def check_route(allowed):
        url = page.url
        trace['observed_url'] = _route_observed(url)
        if url not in allowed:
            if guard:
                guard()  # Preserve an explicit official account gate or provider block.
            raise StageFailure('source_navigation_changed')

    def sample(current_model, expected_range, *, requested):
        url = map_url(symbol, current_model)
        check_route((entry, wanted))
        if page.url != url:
            return None
        active = current_model if choice(page, f'Model {current_model}') else next(
            (m for m in (1, 2, 3) if m != current_model and choice(page, f'Model {m}')), None)
        trace['selected_model'] = active
        trace['mode_coin'] = bool(choice(page, 'Coin'))
        trace['loading'] = bool(visible(page.get_by_text(re.compile(r'^Loading(?:…|\.\.\.)?$', re.I))))
        if active != current_model or not trace['mode_coin'] or trace['loading']:
            return None
        check(visible(page.get_by_text(re.compile('^'+re.escape(symbol)+r' Liquidation Heatmap$',re.I))))
        symbol_control(page,symbol)
        trace['observed_symbol'] = symbol
        control = option_control(page, 'Range')
        try:
            text = control.inner_text().strip()
        finally:
            control.dispose()
        selected = next((k for k in BASIC_RANGES if range_matches(k, text)), None)
        trace['observed_range'] = selected
        if expected_range and selected != expected_range:
            return None
        if requested and guard:
            guard()
        check(visible(page.get_by_text('Market overview',exact=True)))
        side = details(page)
        level = rail_anchor(page)
        plot = _rendered_chart(page, guard if requested else lambda:check_route((url,)))
        trace.update(plot_populated=plot is not None, source_time=side['source_time'])
        if not plot or page.url != url:
            return None
        values = (side['raw_price_step'], side['raw_above_total'], side['raw_below_total'], level)
        return (selected, side, values, plot)

    def settled(current_model, expected_range, previous=None, *, changed_model=False, requested=False):
        last = None
        def read():
            nonlocal last
            # An intervening incomplete sample invalidates earlier stability.
            prior, last = last, None
            frame = sample(current_model, expected_range, requested=requested)
            if frame is None:
                return None
            if previous is not None:
                if changed_model and frame[2] == previous[2]:
                    return None
                if _plot_distance(previous[3], frame[3]) < 4:
                    return None
            last = frame
            if (prior is not None and prior[:3] == frame[:3]
                    and _plot_distance(prior[3], frame[3]) <= 1):
                return frame
            return None
        return wait_for(page, read, timeout=24, requested=requested)

    try:
        trace['phase'] = 'entry_navigation'
        if current_model is None:
            # Untrusted predecessors are never inspected or clicked to reach the map.
            if guard: guard()
            raise StageFailure('source_navigation_changed')
        response = None
        if first_document:
            response = page.goto(entry,wait_until='domcontentloaded',timeout=limited_timeout(40000))
        if response and page.url == entry:
            if response.status == 429:
                from dedicated_login_form import retry_after_seconds
                error = StageFailure('source_rate_limited')
                error.retry_after_seconds = retry_after_seconds(response.header_value('retry-after'))
                raise error
            if response.status == 404:
                error = StageFailure('source_navigation_changed')
                error.basic_reason = 'map_document_http_404'
                raise error
            if response.status >= 500:
                raise StageFailure('source_capture_failed')
        check_route((entry,))
        page.set_default_timeout(limited_timeout(1000 if _DEADLINE.get() is not None else 3000))
        wait_for(page,lambda: choice_target(page, f'Model {model}'),timeout=25,requested=False)
        prefer_dark(page)
        select_choice(page, 'Coin', requested=False)
        before_range = None
        before_model = None
        switching_model = model != current_model
        if switching_model:
            trace['phase'] = 'entry_model_plot'
            if not visible(page.get_by_text(GATE)):
                before_model = settled(current_model, None)
            trace['phase'] = 'model_selection'
            select_choice(page, f'Model {model}', requested=False)
            def arrived():
                check_route((entry, wanted))
                return page.url == wanted
            wait_for(page, arrived, timeout=8, requested=False)
            trace['phase'] = 'model_plot_settling'
            if not visible(page.get_by_text(GATE)):
                before_range = settled(model, None, before_model, changed_model=True)
        else:
            select_choice(page, f'Model {model}', requested=False)
        trace['phase'] = 'range_selection'
        control = option_control(page, 'Range')
        try:
            already_selected = range_matches(timeframe, control.inner_text().strip())
        finally:
            control.dispose()
        if not already_selected and before_range is None:
            # On a gated default range allow selection of the requested control;
            # no account action is taken until that exact target is visible.
            if not visible(page.get_by_text(GATE)):
                before_range = settled(model, None)
        check_route((wanted,))
        selection = select_range(page, timeframe)
        trace['phase'] = 'requested_plot_settling'
        if already_selected and before_range and before_range[0] == timeframe:
            # This model's complete stable chart is already for the requested range.
            if guard: guard()
            result = before_range[1]
        else:
            previous = before_range if not already_selected else None
            unresolved_model_change = switching_model and before_range is None
            if unresolved_model_change:
                previous = before_model
            result = settled(model, timeframe, previous, changed_model=unresolved_model_change, requested=True)[1]
        check_route((wanted,))
        if guard: guard()
        if timeframe == '12H':
            from session_capture import note_protected_map_ready
            note_protected_map_ready(page)
        return dict(result, range_selection=selection)
    except Exception as error:
        if observe:
            try:
                trace['observed_url'] = _route_observed(page.url)
                reason = getattr(error, 'basic_reason', None) or getattr(error, 'reason', None) or getattr(error, 'code', None)
                observe('stage', dict(name='map_navigation', status='failed',
                                      condition=reason or 'source_capture_failed', navigation=trace))
            except Exception:
                # Failure to gather supplemental market evidence must preserve the initial exception.
                pass
        raise


def chart_box(page):
    canvases = page.locator('canvas')
    boxes = [c.bounding_box() for c in visible(canvases)]
    boxes = [b for b in boxes if b and b['width']>=400 and b['height']>=200]
    check(boxes)
    box = max(boxes,key=lambda b:b['width']*b['height'])
    return tuple(round(box[k]) for k in ('x','y','width','height'))


@contextmanager
def position_chart(page, guard=None):
    box = chart_box(page)
    viewport = page.viewport_size
    x, y, width, height = box
    if not viewport or not (
        y >= 0 and 0 <= x and x + width <= viewport['width'] and
        height <= viewport['height'] < y + height
    ):
        yield box
        return
    from playwright.sync_api import Error as BrowserError
    if guard:
        guard()
    previous = page.evaluate_handle("""dy => {
        const previous = {doc:document, x:scrollX, y:scrollY};
        scrollBy({top:dy,left:0,behavior:'instant'});
        return previous;
    }""", y + height - viewport['height'])
    try:
        if guard:
            guard()
        yield chart_box(page)
    finally:
        # The chart and its controls were measured in the updated viewport.
        # Restore only this same document, also when the read fails partway.
        try:
            try:
                previous.evaluate("""p => {if(p.doc === document)
                    scrollTo({left:p.x,top:p.y,behavior:'instant'});}""")
            finally:
                previous.dispose()
        except BrowserError:
            pass


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
      if(this.globalAlpha>0 && /^(?:(?:[0-9]{1,2}\\/[0-9]{1,2} )?[0-9]{1,2}:[0-9]{2}|(?:\\$\\s*)?[0-9,]+(?:\\.\\d+)?\\s*[KMB]?)$/i.test(String(txt)) && String(txt).length<=32) {
        const p=new DOMPoint(x,y).matrixTransform(this.getTransform());
        const px=r.x+p.x*r.width/c.width, py=r.y+p.y*r.height/c.height;
        if(Number.isFinite(px+py) && px>=r.left-1 && px<=r.right+1 && py>=r.top-1 && py<=r.bottom+1) {
          const text=this.measureText(String(txt));
          const centre=new DOMPoint(x,y+(text.actualBoundingBoxDescent-text.actualBoundingBoxAscent)/2)
              .matrixTransform(this.getTransform());
          const record=[String(txt),px,py,this.textBaseline,r.y+centre.y*r.height/c.height];
          if(box[4]===true && String(txt).includes('/')) {
            const tr=this.getTransform();
            record.push([this.fillStyle,this.font,this.textAlign].map(v=>typeof v==='string'?v:'').join('|'),
              {font:this.font,align:this.textAlign,baseline:this.textBaseline,direction:this.direction,
               colour:typeof this.fillStyle==='string'?this.fillStyle:null,
               sx:tr.a*r.width/c.width,sy:tr.d*r.height/c.height,rotated:!!(tr.b||tr.c)});
          }
          labels.push(record);
        }
        if(labels.length>6000)labels.splice(0,3000);
      }
      return result;
    };
    undo.push(()=>{if(own)Object.defineProperty(ctx,'fillText',own);else delete ctx.fillText;});
  }
  return {labels, restore(){undo.forEach(f=>f())}};
}"""


def clear_cookie_notice(page, *, model=None):
    from heatmap_hover import HoverError, dismiss_cookie_banner, require_cookie_banner_hidden
    try:
        if model is None:
            require_cookie_banner_hidden(page)
        else:
            dismiss_cookie_banner(page, model=model)
    except HoverError as error:
        raise StageFailure('source_not_readable', error.reason) from error


def read_paint(page, box, x, y, *, observe=None, fresh_pointer=False, pointer_phase='closing'):
    page.mouse.move(box[0]-2, max(1,box[1]-2))
    def dom_dates():
        dates=set()
        for marker in visible(page.get_by_text(re.compile(r'^\d{1,2}/\d{1,2} \d{2}:\d{2}$'))):
            pos=marker.bounding_box()
            if pos:
                cx,cy=round(pos['x']+pos['width']/2),round(pos['y'])
                if abs(cx-x)<95 and box[1]+.78*box[3]<=cy<=box[1]+box[3]:
                    dates.add((marker.inner_text().strip(),cx,cy))
        return dates
    inactive=dom_dates() if fresh_pointer else set()
    watch = page.evaluate_handle(PAINT, list(box))
    try:
        if not fresh_pointer:
            page.mouse.move(x-2,y)
        page.mouse.move(x,y)
        seen=[]
        observed_dom=set()
        def timestamp():
            labels=watch.evaluate('w=>w.labels')
            seen[:]=[(v[0],round(v[1]),round(v[2])) for v in labels if ':' in v[0]]
            painted={p[0] for p in labels if '/' in p[0] and abs(p[1]-x)<115 and
                     box[1]+.78*box[3] <= p[2] <= box[1]+box[3]}
            observed_dom.clear()
            observed_dom.update(dom_dates()-inactive)
            stamps=painted|{p[0] for p in observed_dom}
            if len(stamps)==1:
                clear_cookie_notice(page)
                stamp=stamps.pop()
                if fresh_pointer and observe:
                    observe('stage',dict(name=pointer_phase+'_crosshair_evidence',status='ok',source_timestamp=stamp,
                           pointer_x=round(x),pointer_y=round(y),canvas_count=len(painted),dom_count=len(observed_dom)))
                return stamp,labels
            return None
        try:
            return wait_for(page,timestamp,timeout=3)
        except StageFailure:
            if observe:
                observe('hover',{'source':'unresolved','rows':[],
                                'marks':[list(m) for m in sorted(set(seen))[:256]],
                                'expected':'bottom_crosshair_time_at_pointer','x':round(x),'y':round(y),
                                'candidate_dom_dates':[list(v) for v in sorted(observed_dom)[:12]]})
            raise
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


def measure_box(page, bounds, y1, y2, *, require_leader=True):
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
    from modern_map_measure_panel import measure_panel
    return measure_panel(page,right_edge,require_leader=require_leader)


def full_intensity(page):
    def intensity():
        return label(page,'Intensity').evaluate("""el=>{
          for(let n=el;n&&n!==document.body;n=n.parentElement) {
            const s=n.innerText;
            if(s.includes('%'))return s;
          }return '';
        }""")
    s=intensity()
    return bool(re.search(r'(?:≥|>=)\s*0\s*%',s) and re.search(r'(?:≤|<=)\s*100\s*%',s))


def display_ready(page):
    select_option(page,'Layout','Single chart')
    select_option(page,'Lens','Heat')
    select_option(page,'Colours','Viridis')
    select_choice(page,'Pan')
    if not full_intensity(page):
        one(page.get_by_role('button',name=re.compile('lowest intensity',re.I))).press('Home')
        one(page.get_by_role('button',name=re.compile('full.brightness intensity',re.I))).press('End')
        wait_for(page,lambda:full_intensity(page))


def evidence_controls(page,symbol,model):
    coin=choice_target(page,'Coin')
    try:
        controls=[choice_target(page,f'Model {model}'),coin,symbol_control(page,symbol)]
        controls.extend(option_control(page,key) for key in ('Range','Layout','Lens','Colours'))
        return [control.bounding_box() for control in controls]
    finally:
        coin.dispose()


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


def read_selected_map(page,symbol,model,timeframe,output,*,guard=None,observe=None,base_only=False):
    if base_only and not basic_target(symbol, model, timeframe):
        raise ValueError('Unsupported basic Coin map')
    if base_only or (symbol,model,timeframe)==('BTC',1,'12H'):
        from modern_map_basic import capture_basic
        return capture_basic(page,symbol,model,timeframe,output,guard=guard,observe=observe,base_only=base_only)
    return read_strict_selected_map(page,symbol,model,timeframe,output,guard=guard,observe=observe)


@phase('map_read')
def read_strict_selected_map(page,symbol,model,timeframe,output,*,guard=None,observe=None):
    if guard:guard()
    clear_cookie_notice(page, model=model)
    def clear_surface():
        if guard:guard()
        clear_cookie_notice(page)
    display_ready(page)
    pilot=(symbol,model,timeframe)==('BTC',1,'12H')
    # The consent guard is repeated at capture boundaries. Only this job has
    # access to its typed observations, never to credentials or storage state.
    def emit(kind,value):
        if observe:observe(kind,value)
    def stage(name,action):
        started=time.monotonic()
        try:
            value=action()
        except Exception as error:
            emit('stage',dict(name=name,status='failed',elapsed_ms=round((time.monotonic()-started)*1000),
                              condition=getattr(error,'reason',None) or getattr(error,'code','reader_failed'),
                              details=getattr(error,'measure_detail',None)))
            raise
        emit('stage',dict(name=name,status='ok',elapsed_ms=round((time.monotonic()-started)*1000)))
        return value
    supplemental=None
    if pilot:
        from modern_map_overview import overview
        supplemental=stage('read_market_overview',lambda:overview(page))
        emit('overview',supplemental)
        required=('raw_total','raw_above_total','raw_below_total','raw_price_step','source_time')
        missing=[k for k in required if not supplemental.get(k)]
        if missing:
            emit('stage',dict(name='validate_market_observation',status='failed',
                              expected=list(required),missing=missing))
        before=stage('verify_market_observation',lambda:core_observation(supplemental))
    else:
        before=details(page)
    with position_chart(page, clear_surface) as box:
        from playwright.sync_api import Error as BrowserError
        document=None
        if pilot and observe:
            try:
                document=page.evaluate_handle('()=>document')
            except BrowserError:
                pass
        try:
            return _measure_selected_map(page,symbol,model,timeframe,output,before,box,clear_surface,
                                         supplemental,emit,stage)
        except Exception:
            if document is not None:
                from modern_map_overview import after_read_failure
                def still_present():
                    limited_timeout(1000)
                    if page.is_closed() or page.url!=map_url(symbol,model) or not document.evaluate('d=>d===document'):
                        raise StageFailure('source_navigation_changed')
                    if guard:guard()
                after_read_failure(page,still_present,emit)
            raise
        finally:
            if document is not None:
                try:document.dispose()
                except BrowserError:pass


def core_observation(facts):
    required=('raw_total','raw_above_total','raw_below_total','raw_price_step','source_time')
    if any(not facts.get(k) for k in required):
        raise StageFailure('source_not_readable','evidence_number_mismatch')
    equivalent(facts['raw_total'],[facts['raw_above_total'],facts['raw_below_total']])
    check(number(facts['raw_price_step'])[0]>0)
    return {key:facts[key] for key in required[1:]}


def _measure_selected_map(page,symbol,model,timeframe,output,before,box,guard,supplemental,emit,stage):
    from modern_map_tooltip import member_for,member_sum,read_tooltip
    from modern_map_cells import merge_cells
    left,top,width,height=box
    page.mouse.move(max(0,left-2),max(0,top-2))
    surface=None
    if supplemental is not None:
        from modern_map_surface import prepare_surface
        surface=stage('inspect_chart_obstruction',lambda:prepare_surface(page,box,guard,limited_timeout))
        emit('stage',dict(name='chart_obstruction',status='partial' if surface['covered'] or surface['unknown'] else 'clear',**surface))
        if surface['close_attempts']:
            box=chart_box(page)
            left,top,width,height=box
    controls=evidence_controls(page,symbol,model)
    screen=stage('capture_chart_and_controls',lambda:market_surface(page,box,controls,guard))
    try:
        check(0<=left and left+width<=screen.width and 0<=top and top+height<=screen.height)
        # Save once before any hover or Measure operation. Partial source
        # survives even when the first axis label is unreadable.
        if surface:
            # Unknown UI is omitted only in the exported partial image. The
            # browser remains untouched and its actual coverage is recorded.
            for rect in surface['unknown']:
                screen.paste((10,13,18),tuple(rect))
        stage('save_filtered_chart',lambda:save_evidence(screen,box,controls,output))
        if supplemental is not None:
            from pathlib import Path
            emit('png',output.getvalue() if isinstance(output,BytesIO) else Path(output).read_bytes())
        if surface and (surface['covered'] or surface['unknown']):
            raise StageFailure('source_not_readable','hover_plot_clipped')
        bounds=stage('locate_dark_price_plot',lambda:dark_bounds(screen,box))
        x0,y0,x1,y1=bounds
        def stamp_at(x):
            return read_paint(page,box,x,(y1+y0)//2,observe=emit)
        prior,prior_marks=stage('read_prior_crosshair_time',lambda:stamp_at(x0+(x1-x0)*.72))
        raw_time,current_marks=stage('read_latest_crosshair_time',lambda:stamp_at(x1-1))
        update = datetime.fromisoformat(before['source_time'])
        column = source_clock(raw_time,update,column=True)
        previous=source_clock(prior,column,column=True,max_lag=HORIZON_SECONDS[timeframe])
        emit('stage',dict(name='observed_columns',status='ok',source_time=iso(update),
                          prior=iso(previous),latest=iso(column)))
        stage('verify_latest_clock',lambda:check(
            column<=update and 0<(column-previous).total_seconds()<=HORIZON_SECONDS[timeframe]))
        raw_anchor=(float(number(supplemental['sidebar_levels'][0]['raw_price'])[0])
                    if supplemental and supplemental['sidebar_levels'] else rail_anchor(page))
        raw_step = before['raw_price_step']
        step = float(number(raw_step)[0])
        # The first bounding rectangle spans the displayed price domain.
        # Its total must agree with the independently printed side totals
        # before those totals may be used as a group-share denominator.
        book={}
        sidebar_rows=(supplemental or {}).get('sidebar_levels',[])
        sidebar={number(m['raw_price'])[0]:m for m in sidebar_rows}
        source_max=(supplemental or {}).get('raw_largest_value')
        printed_anchor=sidebar_rows[0]['raw_price'] if sidebar_rows else None
        printed_prices={p:m['raw_price'] for p,m in sidebar.items()}
        has_tooltip_prices=False
        column_key=column.strftime('%m/%d %H:%M')
        hover_attempts=0
        usable=False
        def hover(y):
            nonlocal hover_attempts,usable,has_tooltip_prices
            select_choice(page,'Pan')
            event=stage('read_hover',lambda:read_tooltip(page,box,bounds,column,x1-1,y,PAINT))
            hover_attempts+=1
            for raw_price in event.get('raw_price_labels',[]):
                printed_prices[number(raw_price)[0]]=raw_price
                has_tooltip_prices=True
            try:
                result=merge_cells(book,event['rows'],column_key,raw_step)
            except StageFailure:
                emit('hover',event)
                raise
            event['counts']=result
            emit('hover',event)
            usable=usable or bool(result['accepted'] or result['duplicates'])
        direct=None
        if supplemental and supplemental.get('raw_largest_value') and supplemental['sidebar_levels']:
            from modern_map_axis import axis_marks,match_displayed_bins
            marks=stage('read_numeric_price_axis',lambda:axis_marks(page,box,prior_marks,current_marks))
            direct=stage('verify_sidebar_covers_prominent_bins',lambda:match_displayed_bins(
                screen,box,bounds,marks,supplemental,model,raw_anchor,step))
        if direct is not None:
            last,height,runs,book,strongest=direct
            last_price=number(last)[0]
        else:
            if supplemental is not None:
                hover((y1+y0)//2)
            # Obtain the independently displayed reference, native price domain
            # and total. The overview independently reports the maximum when available.
            last,full,strongest,low_bound,high_bound=stage(
                'measure_price_domain_and_maximum',lambda:measure_box(page,bounds,y0+.2,y1-.2,
                                                                     require_leader=not bool(source_max)))
            emit('stage',dict(name='observed_price_domain',status='ok',last=last,raw_total=full,
                              raw_above=before['raw_above_total'],raw_below=before['raw_below_total'],
                              low=str(low_bound),high=str(high_bound),strongest=strongest))
            stage('compare_map_and_side_totals',
                  lambda:equivalent(full,[before['raw_above_total'],before['raw_below_total']]))
            last_price = number(last)[0]
            stage('validate_price_domain',lambda:check(0 < low_bound < last_price < high_bound))
            def where(price):
                return y1-.2-(price-float(low_bound))*(y1-y0-.4)/float(high_bound-low_bound)
            height,runs=stage('locate_prominent_latest_bins',
                              lambda:sample_groups(screen,bounds,model,raw_anchor,step,where,float(last_price)))
        used_unlabelled_box=False
        witnessed_native_total=False
        measured_witness=False
        max_control=None
        def read_single(row, narrow):
            margin=(.39 if narrow else .48)*height
            return measure_box(page,bounds,max(y0,row['y']-margin),min(y1,row['y']+margin),
                               require_leader=not narrow)
        def corroborate(measured, rail):
            check(number(measured[0])[0]==last_price)
            member=box_member(measured,rail['raw_price'],raw_step,printed_anchor)
            equivalent(rail['raw_value'],[member['raw_value']])
            if measured[2]:
                check(number(measured[2]['raw_price'])[0]==number(rail['raw_price'])[0])
                equivalent(member['raw_value'],[measured[2]['raw_value']])
        method_counts=dict(dom=0,hover=0,measure=0,sidebar_matches=0,hover_attempts=0)
        def agrees_with_rail(member):
            label=sidebar.get(number(member['raw_price'])[0])
            if label:
                equivalent(label['raw_value'],[member['raw_value']])
                method_counts['sidebar_matches']+=1
        groups=[]
        deadline=time.monotonic()+90
        for colour,rows in runs:
            values=[]
            for row in rows:
                if time.monotonic()>=deadline:raise StageFailure('source_timeout')
                y = row['y']
                member=member_for(book,row,step)
                if supplemental is not None and member is None and (hover_attempts<2 or usable or
                        (has_tooltip_prices and member_for(printed_prices,row,step) is None)):
                    hover(y)
                    member=member_for(book,row,step)
                if member is None:
                    raw_price=member_for(printed_prices,row,step)
                    single_bin=bool(source_max and raw_price and printed_anchor)
                    measured=stage('measure_missing_latest_bin',lambda:read_single(row,single_bin))
                    reference,total,member,_,_=measured
                    check(number(reference)[0]==last_price)
                    if member is None:
                        member=stage('verify_single_bin_box',lambda:box_member(measured,raw_price,raw_step,printed_anchor))
                        used_unlabelled_box=True
                    rail=sidebar.get(number(raw_price)[0]) if raw_price else None
                    if single_bin and rail and number(rail['raw_value'],True)[0]==number(source_max,True)[0]:
                        measured_witness=True
                        # Validate immediately if a leaderless box needs this control.
                        # Otherwise a clearly readable native leader remains usable.
                        try:
                            corroborate(measured,rail)
                        except StageFailure:
                            pass
                        else:
                            witnessed_native_total=True
                    equivalent(total,[member['raw_value']])
                    method_counts['measure']+=1
                else:
                    method_counts['dom' if direct is not None else 'hover']+=1
                stage('validate_latest_bin',lambda:verified_member(row,member,step))
                stage('compare_bin_to_sidebar',lambda:agrees_with_rail(member))
                rail=sidebar.get(number(member['raw_price'])[0])
                if source_max and rail and number(rail['raw_value'],True)[0]==number(source_max,True)[0]:
                    max_control=(row,rail)
                values.append(member)
            group_total=values[0]['raw_value'] if len(values)==1 else member_sum(values)
            if len(rows)>1 and supplemental is None:
                reference,group_total,leader,_,_=measure_box(page,bounds,max(y0,rows[-1]['y']-.48*height),
                                                            min(y1,rows[0]['y']+.48*height))
                check(reference==last and leader in values)
            equivalent(group_total,[m['raw_value'] for m in values])
            groups.append({'side':rows[0]['side'],'colour':colour,'raw_total':group_total,'members':values})
        if used_unlabelled_box and not witnessed_native_total:
            if max_control is None or measured_witness:
                stage('verify_single_bin_amount_method',lambda:unresolved_measure('single_bin_amount_uncorroborated'))
            # The known maximum may already have been read via Hover. Only now
            # use one same-width control to corroborate the liquidities of the boxes.
            row,rail=max_control
            control=stage('measure_known_native_bin',lambda:read_single(row,True))
            stage('verify_single_bin_amount_method',lambda:corroborate(control,rail))
            method_counts['measure']+=1
        stage('verify_strongest_in_coverage',lambda:verify_covered_maximum(
            groups,strongest,source_max,sidebar_rows))
        stage('validate_group_contiguity_and_sums',lambda:validate_groups(
            groups,last_price,raw_step,before['raw_above_total'],before['raw_below_total']))
        method_counts['hover_attempts']=hover_attempts
        emit('stage',dict(name='latest_bin_methods',status='ok',**method_counts))
        select_choice(page,'Pan')
        check(chart_box(page)==box)
        raw_after,_=stage('recheck_latest_column',lambda:stamp_at(x1-1))
        if supplemental is not None:
            from modern_map_overview import overview
            final_market=stage('recheck_market_update',lambda:overview(page))
            check(all(final_market[k]==supplemental[k] for k in (
                'source_time','raw_total','raw_above_total','raw_below_total','raw_price_step',
                'raw_column_period','raw_largest_value','sidebar_levels')))
            after=core_observation(final_market)
        else:
            after=details(page)
        def identity():
            check(before==after and raw_time==raw_after and page.url==map_url(symbol,model))
            check(choice(page,f'Model {model}') and choice(page,'Coin'))
            check(range_matches(timeframe, option_control(page,'Range').inner_text().strip()))
            if supplemental is not None:
                symbol_control(page,symbol)
                check(option_control(page,'Layout').inner_text().strip()=='Single chart')
                check(option_control(page,'Lens').inner_text().strip()=='Heat')
                check(bool(re.fullmatch(r'Viridis(?:\s*Default)?',option_control(page,'Colours').inner_text().strip())))
            gate(page)
            if guard:guard()
        stage('confirm_map_identity_and_update',identity)
    finally:
        screen.close()
    captured = datetime.now(timezone.utc)
    check(-120 <= (captured-update).total_seconds() <= 1200)
    return dict(schema='coinglass.right-edge-measure.v1',source_url=page.url,symbol=symbol,
                heatmap_model=model,timeframe=timeframe,mode='Coin',lens='Heat',palette='Viridis',
                theme='dark',intensity=[0,100],count='Right edge',**before,
                source_time_after=after['source_time'], column_time=iso(column),column_time_after=iso(column),
                raw_last_price=last,groups=groups,captured_at=iso(captured))
