"""Read a visible market tooltip as a price-by-time table. Never read chart internals."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
import re

from modern_map_evidence import AMOUNT, PRICE, number


# Retain only bounded market quantities and times. Do not return DOM text,
# links, attributes, page markup or account elements from the browser.
_DOM = r"""box => {
  const [x,y,w,h]=box, out=[];
  const token=/^(?:(?:\d{1,2}\/\d{1,2}\s+)?\d{1,2}:\d{2}|(?:\$\s*)?\d[\d,]*(?:\.\d+)?\s*[KMB]?)$/i;
  for (const e of document.querySelectorAll('th,td,span,div,strong,b,[role=gridcell],[role=columnheader]')) {
    if(e.childElementCount || !e.checkVisibility({checkOpacity:true,checkVisibilityCSS:true}) ||
       e.closest('header,footer,nav,form,dialog,input,iframe,[role=dialog],[contenteditable=true]'))continue;
    const rect=e.getBoundingClientRect();
    if(rect.width===0 || rect.height===0 || rect.left<x-1 || rect.right>x+w+1 ||
       rect.top<y || rect.bottom>y+h)continue;
    const t=e.textContent.trim();
    if(t.length<=32 && token.test(t))out.push([t,(rect.left+rect.right)/2,(rect.top+rect.bottom)/2]);
    if(out.length>=512)break;
  }
  return out;
}"""

_NUMBER = re.compile(r'(?:\$\s*)?(' + PRICE + r')\s*([KMB]?)$', re.I)
_TIME = re.compile(r'(?:(\d{1,2}/\d{1,2})\s+)?(\d{1,2}:\d{2})$')


def _timestamp(raw, anchor):
    match = _TIME.fullmatch(raw)
    if not match:
        return None
    try:
        if match[1]:
            date = datetime.strptime(match[1] + ' ' + match[2], '%m/%d %H:%M')
            return date.strftime('%m/%d %H:%M')
        clock = datetime.strptime(match[2], '%H:%M').time()
        options = [datetime.combine(anchor.date() + timedelta(days=n), clock, anchor.tzinfo)
                   for n in (-1,0,1)]
        return min(options,key=lambda d:abs(d-anchor)).strftime('%m/%d %H:%M')
    except ValueError:
        return None


def table_rows(marks, anchor):
    """Only accept unambiguous column headers above a price-plus-amount row."""
    raw_marks = {(t,round(x),round(y)) for t,x,y,*_ in marks
                 if isinstance(t,str) and len(t)<=32}
    headers = [(time,x,y) for t,x,y in raw_marks if (time:=_timestamp(t,anchor))]
    numeric = [(m[1]+m[2].upper(),x,y) for t,x,y in raw_marks if (m:=_NUMBER.fullmatch(t))]
    cells = set()
    header_heights = {y for _,_,y in headers}
    for head_y in header_heights:
        header = sorted({(x,time) for time,x,y in headers if abs(y-head_y)<=3})
        if len(header)!=len({t for x,t in header}) or len(header)>12:
            continue
        left, right = header[0][0], header[-1][0]
        tolerance = min([32]+[(header[i][0]-header[i-1][0])*.43 for i in range(1,len(header))])
        row_heights = {y for _,_,y in numeric if 3 < y - head_y <= 305}
        for row_y in row_heights:
            # An earlier tooltip title with a time is not a table header.
            if any(head_y+3 < other_y < row_y-3 for other_y in header_heights):
                continue
            row = sorted((x,t) for t,x,y in numeric
                         if abs(y-row_y)<=3 and left-145<=x<=right+145)
            if len(row) != len(header)+1:
                continue
            # The visible price axis can be on either side. Accept only one
            # complete alignment, so missing amounts never shift time columns.
            options = []
            for (axis_x,price), amounts, gap in (
                (row[0],row[1:],left-row[0][0]),
                (row[-1],row[:-1],row[-1][0]-right),
            ):
                if not 13<=gap<=145 or not re.fullmatch(PRICE,price):
                    continue
                if all(abs(x-header_x)<=tolerance for (x,_),(header_x,_) in zip(amounts,header)):
                    options.append((price,amounts))
            if len(options) == 1:
                price, amounts = options[0]
                for (_,amount),(_,stamp) in zip(amounts,header):
                    cells.add((price,amount,stamp))
    return [dict(price=p,amount=a,time=t) for p,a,t in sorted(cells)]


def read_tooltip(page,box,bounds,anchor,x,y,paint):
    """Compare pointer-off and pointer-on DOM. Use Canvas only for painted labels."""
    page.mouse.move(max(0,box[0]-2),max(0,box[1]-2))
    off={tuple(v) for v in page.evaluate(_DOM,list(box))}
    watcher=page.evaluate_handle(paint,list(box))
    try:
        page.mouse.move(x,y)
        page.evaluate('() => new Promise(done => requestAnimationFrame(() => requestAnimationFrame(done)))')
        seen=watcher.evaluate('w=>w.labels')
        dom=[m for m in page.evaluate(_DOM,list(box)) if tuple(m) not in off]
    finally:
        watcher.evaluate('w=>w.restore()')
        watcher.dispose()
    # Axis labels and date ticks stay outside the plot. Include time headers
    # from the tooltip, never the ticks used separately to verify latest time.
    left,top,right,bottom=bounds
    def inside(m):
        return left<=m[1]<=right and top<=m[2]<=bottom
    marks_by_source=[('dom',[m[:3] for m in dom if inside(m)]),
                     ('canvas',[m[:3] for m in seen if inside(m)])]
    receipt={'source':'unresolved','rows':[],'marks':[],'dom_marks':0,'canvas_marks':0}
    for source,marks in marks_by_source:
        typed=sorted({(m[0],round(m[1]),round(m[2])) for m in marks})
        receipt[source+'_marks']=len(typed)
        rows=table_rows(typed,anchor)
        if rows:
            receipt.update(source=source,rows=rows[:2048],marks=[list(t) for t in typed[:256]])
            break
        receipt['marks'].extend([list(t) for t in typed[:64]])
    return receipt


def member_for(book,row,step):
    near=[member for price,member in book.items()
          if abs(float(price)-row['price'])<=float(step)*.35]
    return near[0] if len(near)==1 else None


def member_sum(members):
    values=[number(m['raw_value'],True) for m in members]
    # Keep the coarsest source unit and decimal position for a displayed
    # sum, with the unrounded decimal sum independently retained in the rows.
    quantum=max(error*2 for _,error in values)
    value=sum(v for v,_ in values)
    rounded=(value/quantum).to_integral_value()*quantum
    for suffix,scale in [('B',Decimal(10)**9),('M',Decimal(10)**6),('K',Decimal(10)**3)]:
        if abs(rounded)>=scale and quantum<=scale:
            places=max(0,scale.adjusted()-quantum.adjusted())
            return f'{rounded/scale:.{places}f}{suffix}'
    return format(rounded,'f')
