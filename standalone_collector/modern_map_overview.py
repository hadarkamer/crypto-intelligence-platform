"""Read visible numerical market labels in a single browser operation."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import re

from modern_map_evidence import iso, source_clock
from model1_execution import StageFailure

# Only complete, labelled market values or price/amount/distance triples leave
# the page. Parent text, DOM markup and input values are never returned.
_READ = r"""(postFailure = false) => {
    const stop = postFailure ? performance.now() + 500 : Infinity;
    const privateRegions = 'form,header,footer,iframe,dialog,[role=dialog],[contenteditable=true]';
    const excluded = privateRegions + ',nav';
    const visible = el => !el.closest('[hidden],[inert],[aria-hidden=true]') && el.getClientRects().length > 0 &&
        el.checkVisibility?.({checkOpacity:true,checkVisibilityCSS:true}) !== false;
    const shown = el => !el.closest(excluded) && visible(el);
    const text = el => el.innerText.trim().replace(/\s+/g, ' ');
    // The map toolbar can be a nav. Recognise it by its own visible layout
    // picker, never by reading page-navigation or account-menu text.
    const chartNav = new Set([...document.querySelectorAll('nav [role=combobox],nav select')]
        .filter(el => !el.closest(privateRegions) && visible(el) && text(el) === 'Single chart')
        .map(el => el.closest('nav')));
    const price = String.raw`(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?`;
    const amount = String.raw`(?:\$\s*)?${price}\s*[KMBT]?`;
    const percent = String.raw`(?:100(?:\.0+)?|[1-9]?\d(?:\.\d+)?)%`;
    const cards = [
        ['Open liquidation levels (total)', ['raw_total'], `(${amount})`],
        ...[['Shorts (above price)', 'above'], ['Longs (below price)', 'below']].map(([label, side]) =>
            [label, [`raw_${side}_total`, `raw_${side}_pct`],
            // A compact unsuffixed amount is ambiguous, e.g. 12345%.
            String.raw`(${amount})(?:(?:\s+|(?<=[KMBT]))(${percent}))?`]),
        ['Price step / column period', ['raw_price_step', 'raw_column_period'],
            String.raw`(${price})\s*/\s*(\d+\s*(?:s|m|h|min|minutes?))`],
        ['Liquidated in 24h', ['raw_realized_total'], `(${amount})`],
        ['of which longs / shorts', ['raw_realized_longs', 'raw_realized_shorts'],
            String.raw`(${amount})\s*/\s*(${amount})`],
        ['Largest single level', ['raw_largest_value'], `(${amount})`],
        ['Reference (last price)', ['raw_last_price'], String.raw`(\$?\s*${price})`],
    ];
    const labels = new Map(cards.map(([label, fields, pattern]) =>
        [label.toLowerCase(), {fields, pattern:new RegExp('^' + pattern + '$', 'i')} ]));
    const values = {}, rail = [];
    const push = (key, value) => (values[key] ??= []).push(value.trim());
    const matchCard = (card, suffix) => {
        const match = card.pattern.exec(suffix.trim());
        if (match) card.fields.forEach((key, i) => { if (match[i + 1]) push(key, match[i + 1]); });
        return !!match;
    };
    // Match the caption to its smallest enclosing card. Do not obtain a value
    // from the next caption when its own card has not yet loaded.
    const candidates = document.querySelectorAll('div,span,p,dt,th,b,small,label,strong,time');
    const eligible = el => !el.closest(privateRegions) && el.childElementCount <= 6 &&
        (!el.closest('nav') || chartNav.has(el.closest('nav'))) && el.textContent.length <= 160;
    const readStamp = caption => {
        const updated = /^Updated\s+(\d{2}:\d{2})$/i.exec(caption);
        if (updated) push('raw_updated_at', updated[1]);
        return !!updated;
    };
    if (postFailure) {
        // Read the source's clock independently before attempting any numerical
        // cards. If the bounded pass stops, a partial scan cannot establish a
        // unique clock in the presence of duplicate UI controls.
        for (const el of candidates) {
            if (performance.now() >= stop) return {values:{}, clock_complete:false, totals_complete:false};
            if (eligible(el) && /^Updated/i.test(el.textContent.trim()) && visible(el)) readStamp(text(el));
        }
    }
    let totalsComplete = true;
    for (const el of candidates) {
        if (postFailure && performance.now() >= stop) { totalsComplete = false; break; }
        if (!eligible(el)) continue;
        if (postFailure && !/^(?:Open liquidation levels|Shorts|Longs)/i.test(el.textContent.trim())) continue;
        if (!visible(el)) continue;
        const nav = el.closest('nav');
        const caption = text(el);
        if (!postFailure && readStamp(caption)) continue;
        if (nav) continue;
        const key = caption.toLowerCase();
        const card = labels.get(key);
        if (!card) {
            // A caption can be a direct text node next to the value span.
            for (const [name, spec] of labels) {
                if (key.startsWith(name)) matchCard(spec, caption.slice(name.length));
            }
            continue;
        }
        for (let p = el.parentElement; p && p !== document.body; p = p.parentElement) {
            if (p.closest(excluded) || p.textContent.length > 160) break;
            const whole = text(p);
            if (whole === caption) continue;
            if (whole.startsWith(caption)) matchCard(card, whole.slice(caption.length));
            break;
        }
    }
    if (postFailure) return {values, clock_complete:true, totals_complete:totalsComplete};
    const row = new RegExp(String.raw`(?:^|\s)(${price})\s+(${amount})\s+([+−-]${price}%)(?=\s|$)`, 'gi');
    let unreadableRail = 0;
    for (const el of document.querySelectorAll('[data-lv]')) {
        if (!shown(el) || [...el.querySelectorAll('[data-lv]')].some(shown)) continue;
        // Flex spans may render without whitespace between price, amount and
        // distance. Numeric tokens in separate DOM text nodes still have
        // unambiguous boundaries; concatenated plain text does not.
        const walker = document.createTreeWalker(el, NodeFilter.SHOW_TEXT);
        const parts = [];
        while (walker.nextNode()) {
            if (shown(walker.currentNode.parentElement)) parts.push(walker.currentNode.nodeValue.trim());
        }
        const content = parts.join(' ').trim().replace(/\s+/g, ' ');
        const hits = [...content.matchAll(row)];
        hits.forEach(m => rail.push([m[1], m[2].trim(), m[3]]));
        // Do not silently publish a complete sidebar if a source row changed shape.
        if (!hits.length || /\d/.test(content.replace(row, ''))) unreadableRail++;
    }
    return {values, rail, unreadable_rail:unreadableRail};
}"""

_FIELDS = ('raw_total', 'raw_above_total', 'raw_below_total', 'raw_above_pct', 'raw_below_pct',
           'raw_last_price', 'raw_price_step', 'raw_column_period', 'raw_realized_total',
           'raw_realized_longs', 'raw_realized_shorts', 'raw_largest_value', 'raw_largest_price',
           'raw_updated_at')
_SCALE = {'K': 10**3, 'M': 10**6, 'B': 10**9, 'T': 10**12}


def _raw(token):
    return re.sub(r'[\s$]', '', token).replace('−', '-').upper()


def _decimal(token):
    value = _raw(token).replace(',', '').removesuffix('%')
    scale = _SCALE.get(value[-1], 1)
    return Decimal(value[:-1] if scale != 1 else value) * scale


def parse_overview(batch, clock):
    """Keep the display strings and distinguish missing, rounded and contradictory facts."""
    facts = {}
    displayed = {}
    ambiguous = set()
    parsed = {}
    for field in _FIELDS:
        tokens = list(dict.fromkeys(batch['values'].get(field, [])))
        if tokens:
            displayed[field] = tokens
        if field == 'raw_column_period':
            normalized = [re.sub(r'\s+', '', t.lower()) for t in tokens]
        elif field == 'raw_updated_at':
            normalized = tokens
        else:
            normalized = [_decimal(t) for t in tokens]
        if len(set(normalized)) > 1:
            ambiguous.add(field)
        facts[field] = None
        if tokens and field not in ambiguous:
            if field in ('raw_updated_at', 'raw_column_period'):
                facts[field] = normalized[0]
            else:
                facts[field] = _raw(tokens[0])
                parsed[field] = format(normalized[0], 'f')

    rows = {}
    duplicates = 0
    conflicts = set()
    for price, amount, distance in batch['rail']:
        key = _decimal(price)
        row = {'raw_price': price, 'raw_value': _raw(amount), 'raw_distance_pct': _raw(distance),
               'display_values': [price, amount, distance]}
        if key in rows:
            old = rows[key]
            if (_decimal(old['raw_value']), _decimal(old['raw_distance_pct'])) == (_decimal(amount), _decimal(distance)):
                duplicates += 1
            else:
                conflicts.add(key)
        else:
            rows[key] = row
    if conflicts:
        ambiguous.add('sidebar_levels')
    if batch['unreadable_rail']:
        ambiguous.add('sidebar_shape')
    facts['sidebar_levels'] = [row for price, row in rows.items() if price not in conflicts]
    facts['raw_sidebar_rows'] = batch['rail']
    facts['source_time'] = None
    if facts['raw_updated_at']:
        try:
            facts['source_time'] = iso(source_clock(facts['raw_updated_at'], clock))
        except StageFailure:
            ambiguous.add('source_time')
    facts['parsed_values'] = parsed
    facts['display_values'] = displayed
    facts['duplicate_sidebar_levels'] = duplicates
    facts['unreadable_sidebar_containers'] = batch['unreadable_rail']
    facts['ambiguous_fields'] = sorted(ambiguous)
    facts['missing_fields'] = [f for f in (*_FIELDS, 'sidebar_levels', 'source_time') if not facts[f] and f not in ambiguous]
    return facts


def overview(page, clock=None):
    return parse_overview(page.evaluate(_READ), clock or datetime.now(timezone.utc))


def after_read_failure(page, guard, emit):
    """Best effort timestamp on the same guarded document; never changes its UI."""
    try:
        if guard:
            guard()
        # Locator resolution is bounded and the in-page diagnostic limits its
        # synchronous scan. There is no navigation, retry or additional image.
        batch = page.locator('body').evaluate('body => (' + _READ + ')(true)', timeout=1000)
        at = datetime.now(timezone.utc)
        record = {'name':'post_failure_market_observation', 'status':'unavailable',
                  'post_failure_observed_at':iso(at), 'post_failure_raw_updated_at':None,
                  'post_failure_source_time':None}
        source = batch['values']
        stamps = set(source.get('raw_updated_at', []))
        if batch['clock_complete'] and len(stamps) == 1:
            stamp = stamps.pop()
            if re.fullmatch(r'(?:[01]\d|2[0-3]):[0-5]\d', stamp):
                record['post_failure_raw_updated_at'] = stamp
                try:
                    record['post_failure_source_time'] = iso(source_clock(stamp, at))
                    record['status'] = 'observed'
                except StageFailure:
                    record['status'] = 'stale_or_unverified_time'
        if batch['totals_complete']:
            for key in ('raw_total', 'raw_above_total', 'raw_below_total'):
                vals = set(source.get(key, []))
                if len(vals) == 1:
                    token = vals.pop()
                    if re.fullmatch(r'\$?\s*\d[\d,]*(?:\.\d+)?\s*[KMBT]?', token, re.I):
                        record['post_failure_' + key] = _raw(token)
        emit('stage', record)
    except Exception:
        # The original source-read exception remains the cause even if the
        # page has disappeared, its guard rejects it, or the evidence sink fails.
        return
