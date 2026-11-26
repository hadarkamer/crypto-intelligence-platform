"""Inspect chart-overlapping UI before capturing the verified BTC chart."""
from playwright.sync_api import Error as BrowserError

# A visible CTA identifies the known card but is never a click target.
# Only currently visible, independently named close/dismiss buttons on that
# same first-party card can be used. No selector or string leaves the page.
_INSPECT = r"""box => {
    const [x,y,w,h]=box;
    const norm=t=>String(t||'').trim().replace(/\s+/g,' ');
    const visible=e=>e.getClientRects().length &&
        e.checkVisibility?.({checkOpacity:true,checkVisibilityCSS:true})!==false;
    const rect=e=>{
        const b=e.getBoundingClientRect(),l=Math.max(x,b.left),t=Math.max(y,b.top),
              r=Math.min(x+w,b.right),d=Math.min(y+h,b.bottom);
        if (r-l<2 || d-t<2) return null;
        const covers = [[.5,.5],[.2,.25],[.8,.7]].some(([fx,fy])=>{
            const hit=document.elementFromPoint(l+(r-l)*fx,t+(d-t)*fy);
            return hit && (hit===e || e.contains(hit));
        });
        return covers ? [Math.floor(l),Math.floor(t),Math.ceil(r),Math.ceil(d)] : null;
    };
    const cards=new Set();
    for (const cta of document.querySelectorAll('button,a,[role=button],span,div')) {
        if ((cta.childElementCount && !cta.matches('button,a,[role=button]')) || !visible(cta) || !/^Try it now(?:\s*[→›»])?$/i.test(norm(cta.innerText))) continue;
        let found=null;
        for(let p=cta.parentElement;p&&p!==document.body;p=p.parentElement) {
            const b=p.getBoundingClientRect();
            if (p.matches('header,nav,form') || p.innerText.length>420 || b.width*b.height>w*h*.80) break;
            const titled=/CoinGlass\s+Legend\b/i.test(norm(p.innerText)) ||
                [...p.querySelectorAll('b,strong,span,p,div,h2,h3')].some(e=>
                    visible(e) && /^CoinGlass\s+Legend$/i.test(norm(e.innerText)));
            if (titled && rect(p)) {
                found=found||p;
                if([...p.querySelectorAll('button,[role=button]')].some(btn=>
                  /^(?:close|dismiss)(?: (?:ad|advertisement|promotion))?$/i.test(
                      norm(btn.getAttribute('aria-label')||btn.getAttribute('title')||btn.innerText)))){found=p;break;}
            }
        }
        if(found)cards.add(found);
    }
    const blocked=[];
    for (const e of document.querySelectorAll('iframe,dialog,[role=dialog],[aria-modal=true],[popover],aside,div,section')) {
        if(!visible(e) || [...cards].some(card=>card.contains(e)||e.contains(card)))continue;
        const b=e.getBoundingClientRect(), pos=getComputedStyle(e).position;
        const modal=e.matches('iframe,dialog,[role=dialog],[aria-modal=true],[popover]');
        if(!modal && (!['fixed','sticky','absolute'].includes(pos) || b.width<90 || b.height<45 ||
                   b.width*b.height>w*h*.8 || !e.querySelector('a[href],button,[role=button]')))continue;
        const clip=rect(e);
        if(clip && !blocked.some(r=>r[0]<=clip[0]&&r[1]<=clip[1]&&r[2]>=clip[2]&&r[3]>=clip[3]))blocked.push(clip);
    }
    const regions=[...cards].map(rect).filter(Boolean);
    const card=cards.size===1?[...cards][0]:null;
    const controls=card?[...card.querySelectorAll('button,[role=button]')].filter(btn=>{
        if(!visible(btn) || btn.closest('form,a[href]') || btn.disabled || btn.getAttribute('aria-disabled')==='true')return false;
        const r=btn.getBoundingClientRect(), hit=document.elementFromPoint((r.left+r.right)/2,(r.top+r.bottom)/2);
        const name=norm(btn.getAttribute('aria-label')||btn.getAttribute('title')||btn.innerText);
        return (hit===btn||btn.contains(hit)) && /^(?:close|dismiss)(?: (?:ad|advertisement|promotion))?$/i.test(name);
    }):[];
    return {facts:{cards:cards.size,eligible:controls.length,covered:regions,unknown:blocked},
            card,close:controls.length===1?controls[0]:null};
}"""


def prepare_surface(page, box, guard, timeout):
    def inspect():
        state=page.evaluate_handle(_INSPECT,list(box))
        try:
            facts=state.evaluate('s=>s.facts')
            return state,facts
        except Exception:
            state.dispose()
            raise
    state,facts=inspect()
    try:
        if facts['cards']==1 and facts['eligible']==1 and not facts['unknown']:
            guard()
            card=state.get_property('card').as_element()
            button=state.get_property('close').as_element()
            try:
                if button and card:
                    button.click(timeout=timeout(1500))
                    card.wait_for_element_state('hidden',timeout=timeout(1500))
            except BrowserError:
                pass  # A changed live page must be inspected again.
            finally:
                if button:button.dispose()
                if card:card.dispose()
            guard()
            updated,observed=inspect()
            updated.dispose()
            observed['close_attempts']=1
            return observed
        facts['close_attempts']=0
        return facts
    finally:
        state.dispose()
