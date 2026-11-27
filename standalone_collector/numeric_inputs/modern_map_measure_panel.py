"""Resolve a native Right-edge Measure result relative to its selected control."""
from decimal import Decimal

from modern_map_evidence import measurement, number
from model1_execution import StageFailure


_READ = r"""(control, requireLeader = true) => {
    const barred = 'form,input,textarea,header,footer,dialog,iframe,[role=dialog],[contenteditable=true],[hidden],[aria-hidden=true]';
    const shown = e => (!e.closest('nav') || e.closest('nav') === control.closest('nav')) && !e.closest(barred) && e.getClientRects().length &&
        e.checkVisibility?.({checkOpacity:true,checkVisibilityCSS:true}) !== false;
    const selected = e => e.getAttribute('aria-selected') === 'true' || e.getAttribute('aria-pressed') === 'true' ||
        e.getAttribute('data-state') === 'active' || [...e.classList].some(c=>/(?:^|_)(?:on|active|selected)(?:__|$)/i.test(c));
    const norm = s => s.replace(/\s+/g, ' ').trim();
    const price = String.raw`(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?`;
    const amount = price + '[KMB]?';
    const definitions = {
        box:[/^Box total\b/i, new RegExp(String.raw`^(?:Box total\s*)+[·•]?\s*(${price})\s*[→–-]\s*(${price})\s*\(([+-]?${price}\s*%)\)\s*\$?\s*(${amount})$`, 'i')],
        leader:[/^Largest cluster\b/i, new RegExp(String.raw`^(?:Largest cluster\s*)+\$?\s*(${price})\s*[·•]\s*\$?\s*(${amount})$`, 'i')],
        reference:[/^Reference\s*\(last price\)/i, new RegExp(String.raw`^(?:Reference\s*\(last price\)\s*)+\$?\s*(${price})$`, 'i')],
    };
    const out = {active:false, counts:{box:0,leader:0,reference:0}, values:{box:[],leader:[],reference:[]}};
    if (!shown(control) || !selected(control) || !/^(?:Right edge\s*)+$/i.test(norm(control.innerText))) return out;
    out.active = true;
    // Search outward from the active control. Stop before the document or
    // unrelated chart/account content; the nearest panel with the required labels owns it.
    for (let p=control.parentElement; p && p!==document.body; p=p.parentElement) {
        if (p.matches('main,html') || p.querySelector('iframe,form,input[type=password]')) break;
        const nodes = p.querySelectorAll('div,span,p,dd,dt,b,small,strong,td');
        if (nodes.length > 220) break;
        const counts={box:0,leader:0,reference:0}, values={box:[],leader:[],reference:[]};
        for (const n of nodes) {
            // Identify captions via their own DOM text. Cover both a leaf
            // caption and a caption directly preceding the numeric value span.
            if (!shown(n)) continue;
            const own=norm([...n.childNodes].filter(x=>x.nodeType===Node.TEXT_NODE).map(x=>x.nodeValue).join(' '));
            const key=Object.keys(definitions).find(k=>definitions[k][0].test(own));
            if (!key) continue;
            counts[key]++;
            for (let row=n; row && row!==p.parentElement; row=row.parentElement) {
                if (row.textContent.length>220 || row.querySelector('form,iframe,input')) break;
                const walk=document.createTreeWalker(row,NodeFilter.SHOW_TEXT), parts=[];
                while (walk.nextNode()) if (shown(walk.currentNode.parentElement)) parts.push(walk.currentNode.nodeValue);
                const match=definitions[key][1].exec(norm(parts.join(' ')));
                if (match) { values[key].push(match.slice(1)); break; }
            }
        }
        out.counts=counts; out.values=values;
        if (counts.box && counts.reference && (counts.leader || !requireLeader)) break;
    }
    return out;
}"""


class PanelMeasurement(tuple):
    """The existing five values plus the actual printed precision of the bounds."""
    def __new__(cls, values, raw_bounds):
        result = super().__new__(cls, values)
        result.raw_bounds = raw_bounds
        return result


def _resolve(result, *, require_leader=True):
    def failed(cause, key=None):
        failure = StageFailure('source_not_readable', 'hover_evidence_mismatch' if cause == 'conflicting_values' else 'evidence_text_invalid')
        failure.measure_detail = {'reason': cause, 'field': key, 'label_counts': result['counts'],
                                  'reading_counts': {k:len(v) for k, v in result['values'].items()}}
        raise failure

    if not result['active']:
        failed('right_edge_inactive')
    def identity(values):
        return tuple(Decimal(value[:-1].strip()) if value.endswith('%') else number(value, True)[0]
                     for value in values)

    parts = {}
    for key in ('box', 'leader', 'reference'):
        distinct = {identity(values): tuple(values) for values in result['values'][key]}
        if not distinct:
            if key == 'leader' and require_leader is False:
                continue
            failed('label_missing' if not result['counts'][key] else 'number_missing', key)
        if len(distinct) != 1:
            failed('conflicting_values', key)
        parts[key] = next(iter(distinct.values()))
    first, second, percent, amount = parts['box']
    reference = parts['reference'][0]
    bounds = tuple(sorted((first, second), key=lambda raw: number(raw)[0]))
    if 'leader' in parts:
        leader, size = parts['leader']
        values = measurement(f"Box total · {first} → {second} ({percent}) {amount}\n"
                             f"Largest cluster {leader} · {size}\nReference (last price) {reference}")
    else:
        number(reference)
        number(amount, quantity=True)
        low, high = (number(raw)[0] for raw in bounds)
        values = reference, amount, None, low, high
    return PanelMeasurement(values, bounds)


def measure_panel(page, mode_control, *, require_leader=True):
    return _resolve(mode_control.evaluate(_READ, require_leader), require_leader=require_leader)
