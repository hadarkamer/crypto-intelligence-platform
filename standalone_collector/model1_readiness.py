"""Read-only chart readiness checks. Never change page styling or access controls."""
import json
from pathlib import Path
from urllib.parse import urlsplit, parse_qs

LABELS = {'12h': '12 hour', '24h': '24 hour'}
STATE_SCRIPT = r'''() => {
  const visible = el => {
    const r = el.getBoundingClientRect(), s = getComputedStyle(el);
    return r.width > 0 && r.height > 0 && s.display !== 'none' &&
      s.visibility !== 'hidden' && Number(s.opacity || 1) > 0;
  };
  const triggers = [...document.querySelectorAll('[role="combobox"], [aria-haspopup="listbox"], button.MuiSelect-button')];
  const trigger = triggers.find(e => visible(e) && /^(12|24) hour$/i.test((e.textContent || '').trim()));
  const label = trigger ? trigger.textContent.trim().toLowerCase() : '';
  const canvases = [...document.querySelectorAll('canvas')].filter(e => {
    const r=e.getBoundingClientRect(); return visible(e) && r.width>400 && r.height>200;
  });
  if (!canvases.length) return {ready:false, reason:'no-chart', label};
  const canvas = canvases.sort((a,b) => b.getBoundingClientRect().width*b.getBoundingClientRect().height - a.getBoundingClientRect().width*a.getBoundingClientRect().height)[0];
  const cr=canvas.getBoundingClientRect();
  const overlaps = e => {
    const r=e.getBoundingClientRect();
    return r.right>cr.left && r.left<cr.right && r.bottom>cr.top && r.top<cr.bottom;
  };
  const indicators = document.querySelectorAll('[role="progressbar"], [aria-busy="true"], .MuiCircularProgress-root, .ant-spin-spinning, [class*="spinner" i], [data-loading="true"]');
  if ([...indicators].some(e => visible(e) && overlaps(e)))
    return {ready:false, reason:'loading-indicator', label};
  for(let node=canvas; node; node=node.parentElement) {
    const matches=[...(getComputedStyle(node).filter||'').matchAll(/blur\(\s*([\d.]+)px\s*\)/g)];
    if (matches.some(m => Number(m[1])>0)) return {ready:false, reason:'blur', label};
  }
  const dialogs=[...document.querySelectorAll('[role="dialog"], .MuiModal-root')].filter(visible);
  if (dialogs.some(e=>overlaps(e))) return {ready:false, reason:'visible-dialog', label};
  return {ready:true, reason:'render-checks-passed', label};
}'''

class SourceNotReady(RuntimeError):
    pass

def valid_source(url):
    try:
        u=urlsplit(url)
        q=parse_qs(u.query, keep_blank_values=True)
        return (u.scheme=='https' and u.hostname in {'coinglass.com','www.coinglass.com'}
                and not u.username and not u.password and u.port in {None,443}
                and u.path=='/pro/futures/LiquidationHeatMap'
                and q.get('coin')==['BTC'] and q.get('type')==['symbol'])
    except (ValueError,TypeError):
        return False

def ensure_ready(page, timeframe, diagnostic_path):
    if timeframe not in LABELS or not valid_source(page.url):
        raise SourceNotReady('wrong-source-or-timeframe')
    # DOM polling does not start new navigations or provider requests.
    try:
        page.wait_for_function('() => ('+STATE_SCRIPT+')().ready === true', timeout=60_000, polling=1000)
        page.wait_for_timeout(1000)
    except Exception:
        pass
    state=page.evaluate(STATE_SCRIPT)
    if (state.get('ready') is not True or state.get('label') != LABELS[timeframe]
            or not valid_source(page.url)):
        target=Path(diagnostic_path)
        page.screenshot(path=str(target), full_page=True)
        target.with_suffix('.json').write_text(json.dumps(state), encoding='utf-8')
        raise SourceNotReady('source-not-ready:' + str(state.get('reason','unverified')))
    return state
