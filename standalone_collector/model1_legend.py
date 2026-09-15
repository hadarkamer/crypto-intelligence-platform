"""Close ONLY the public Legend NEW help card using its normal X control.

Selector verified from the public page HTML. No CSS hiding, account/session
changes, generic close clicks, or bypass of authentication/challenge controls.
"""
from pathlib import Path

CARD='div.MuiCard-root:has(a[href="https://legend.coinglass.com"]):has-text("Legend"):has-text("NEW")'
CLOSE=':scope > div.shou[data-first-child]:has(svg path[d^="M405 136.798L375.202 107"])'
AXIS_CLARITY='''
Axis prices are NUMERIC values, not vertical screen positions. axis_low MUST be
the SMALLER numeric price and axis_high the LARGER numeric price, even though
the larger price appears higher on the screen. Use TWO CLEARLY VISIBLE price
ticks. Before returning, check axis_low <= low < high <= axis_high numerically.
Do not use an upper tick smaller than the interval's upper bound. If a tick is
covered or unreadable, do not invent it from expected spacing; use other clearly
visible ticks that enclose the interval, or return an unreadable interval.
'''


def dismiss_legend(page,timeout_ms=3000):
    from model1_execution import StageFailure
    cards=page.locator(CARD)
    count=cards.count()
    if count>4:raise StageFailure('source_not_readable')
    visible=[cards.nth(i) for i in range(count) if cards.nth(i).is_visible()]
    if not visible:return False
    if len(visible)!=1:raise StageFailure('source_not_readable')
    card=visible[0];close=card.locator(CLOSE)
    if close.count()!=1 or not close.is_visible():raise StageFailure('source_not_readable')
    try:
        close.click(timeout=timeout_ms)
        card.wait_for(state='hidden',timeout=timeout_ms)
    except Exception:
        raise StageFailure('source_not_readable') from None
    return True


def expand_legend_capture(text):
    marker='            chart_rect = page.evaluate('
    if text.count(marker)!=1:raise RuntimeError('Legend capture insertion point changed')
    return text.replace(marker,
        '            from model1_legend import dismiss_legend\n'
        '            dismiss_legend(page)\n'+marker,1)


def install(runtime:Path):
    path=runtime/'market_vision/coinglass_heatmap_capture.py'
    text=expand_legend_capture(path.read_text());compile(text,str(path),'exec')
    path.write_text(text,encoding='utf-8')
    path=runtime/'market_vision/openai_heatmap_scanner.py'
    text=path.read_text()+'\nfrom model1_legend import AXIS_CLARITY\nSYSTEM_PROMPT += AXIS_CLARITY\n'
    compile(text,str(path),'exec');path.write_text(text,encoding='utf-8')
