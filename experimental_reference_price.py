"""Formula-specific display references from frozen current-scan input clocks.

These notification levels do not rewrite the historical research entry contract.
Only current observation clocks are considered, never a 7d lookback start.
Closed-minute quotes are explicitly approximate for intra-minute source times.
"""
from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from html import escape
from collections.abc import Mapping
from zoneinfo import ZoneInfo

from experimental_price_levels import calculate_price_levels, render_price_levels

VERSION = 'experimental-input-reference-v1'
COMPONENTS = ('PRICE_OI', 'MAX_PAIN', 'FUTURES_CVD', 'SPOT_CVD')
SYMBOLS = ('BTC', 'ETH', 'SOL', 'HYPE', 'DOGE', 'ZEC', 'BNB', 'XRP')
TIMEOUT_SECONDS = 20
_MINUTE = timedelta(minutes=1)
_ISRAEL = ZoneInfo('Asia/Jerusalem')
_STATUS = {'version': VERSION, 'last_cycle_id': None, 'ready_references': 0,
           'missing_references': 0, 'last_error_type': None}


def status():
    return deepcopy(_STATUS)


def _utc(value):
    stamp = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise ValueError('Reference timestamps need a timezone')
    return stamp.astimezone(timezone.utc)


def _missing(reason='MISSING_SOURCE_REFERENCE'):
    return {'status': 'UNAVAILABLE', 'reason': reason, 'version': VERSION}


def _validated(reference):
    if not isinstance(reference, Mapping) or reference.get('status') != 'READY':
        raise ValueError('Unavailable reference')
    if reference.get('symbol') not in SYMBOLS or reference.get('component') not in COMPONENTS:
        raise ValueError('Unverified reference identity')
    price_time, anchor = _utc(reference['price_time_utc']), _utc(reference['anchor_time_utc'])
    if not price_time <= anchor < price_time + _MINUTE:
        raise ValueError('Quote does not represent the source boundary')
    precision = reference.get('precision')
    if precision == 'EXACT_CAPTURE':
        if price_time != anchor or reference['component'] != 'PRICE_OI':
            raise ValueError('Exact quote clock mismatch')
    elif precision == 'CLOSED_1M':
        expected = 'BINANCE_HYPE_FUTURES_MARK_1M' if reference['symbol'] == 'HYPE' else 'BINANCE_SPOT_TRADE_1M'
        if reference.get('source') != expected or price_time != price_time.replace(second=0, microsecond=0):
            raise ValueError('Minute quote source mismatch')
    else:
        raise ValueError('Unknown reference precision')
    if not isinstance(reference.get('source'), str) or not reference['source']:
        raise ValueError('Missing price provenance')
    # Shared numeric validation also rejects bool, NaN and nonpositive prices.
    calculate_price_levels(reference['price'], price_time, 1, 'LONG')
    return deepcopy(dict(reference))


def select_reference(references, components, *, symbol=None, as_of=None):
    """Earliest clock of *all* required current inputs; missing stays missing."""
    try:
        components = tuple(dict.fromkeys(components))
        if not components or not isinstance(references, Mapping):
            return _missing()
        selected = [_validated(references[name]) for name in components]
        if any(row['component'] != name for name, row in zip(components, selected)) or len({r['symbol'] for r in selected}) != 1:
            return _missing('REFERENCE_IDENTITY_MISMATCH')
        if symbol is not None and any(row['symbol'] != symbol for row in selected):
            return _missing('REFERENCE_SYMBOL_MISMATCH')
        if as_of is not None and any(_utc(row['anchor_time_utc']) > _utc(as_of) for row in selected):
            return _missing('FUTURE_REFERENCE_CLOCK')
        result = min(selected, key=lambda r: _utc(r['anchor_time_utc']))
        result['required_components'] = list(components)
        return result
    except (KeyError, ValueError, TypeError, OverflowError):
        return _missing()


def components_for_conditions(conditions, event_type):
    """Map documented model conditions to their current input clocks."""
    names = set()
    for condition in conditions or ():
        if not isinstance(condition, Mapping):
            continue
        feature = str(condition.get('feature') or '').lower()
        if any(word in feature for word in ('price_oi', 'positioning')):
            names.add('PRICE_OI')
        if any(word in feature for word in ('futures_cvd', 'futures_flow')):
            names.add('FUTURES_CVD')
        if any(word in feature for word in ('spot_cvd', 'spot_flow')):
            names.add('SPOT_CVD')
        if any(word in feature for word in ('max_pain', 'maxpain', 'magnet', 'liquidity')):
            names.add('MAX_PAIN')
    # The native event family remains a required source of generic predicates.
    family = str(event_type or '').upper()
    if 'MAX_PAIN' in family or 'MAGNET' in family:
        names.add('MAX_PAIN')
    elif family == 'OI_PRICE_HIGH':
        names.add('PRICE_OI')
    elif family == 'FUTURES_CVD_HIGH':
        names.add('FUTURES_CVD')
    elif family == 'SPOT_CVD_HIGH':
        names.add('SPOT_CVD')
    return tuple(name for name in COMPONENTS if name in names)


def render_reference_levels(reference, threshold_bps, direction, html=True):
    """No fetching or repricing: render the reference frozen into this intent."""
    try:
        reference = _validated(reference)
    except (KeyError, ValueError, TypeError, OverflowError):
        return 'שער הבסיס אינו זמין; סטופלוס וטייק פרופיט לא חושבו.'
    label = 'תחילת נתוני הבסיס'
    if reference['precision'] == 'CLOSED_1M':
        label += ' (בקירוב לפי נר דקה)'
    if threshold_bps is None:
        price = str(reference['price'])
        stamp = _utc(reference['price_time_utc']).astimezone(_ISRAEL).strftime('%d.%m.%Y %H:%M:%S')
        text = f'שער בסיס — {label}: {price}\nשעת שער הבסיס: {stamp} (שעון ישראל)'
        if html:
            text = escape(text)
        return text + '\nלא הוגדר סף תנועה יחיד; סטופלוס וטייק פרופיט לא חושבו.'
    try:
        levels = calculate_price_levels(reference['price'], reference['price_time_utc'], threshold_bps, direction)
        text = render_price_levels(levels, reference_label=label, html=html)
        if _utc(reference['anchor_time_utc']) != _utc(reference['price_time_utc']):
            stamp = _utc(reference['anchor_time_utc']).astimezone(_ISRAEL).strftime('%H:%M:%S')
            text += f'\nתחילת נתוני הבסיס: {stamp} (שעון ישראל)'
        return text
    except (KeyError, ValueError, TypeError, OverflowError):
        return 'נתוני המחיר או הסף אינם תקינים; סטופלוס וטייק פרופיט לא חושבו.'


def _inputs(coin, symbol, computed):
    sources = coin.get('sources') or {}
    result = {}
    for component in COMPONENTS:
        try:
            if component == 'PRICE_OI':
                positioning = sources['positioning']
                price_time = _utc(positioning['price_fetched_at'])
                oi_time = _utc(positioning['oi_fetched_at'])
                if not all(computed - timedelta(hours=2) <= stamp <= computed
                           for stamp in (price_time, oi_time)):
                    raise ValueError('Stale or future positioning source')
                anchor = min(price_time, oi_time)
                if anchor == price_time:
                    record = {'status': 'READY', 'version': VERSION, 'component': component,
                              'symbol': symbol, 'anchor_time_utc': anchor.isoformat(),
                              'price_time_utc': anchor.isoformat(), 'price': str(positioning['price']),
                              'source': positioning['price_source'], 'precision': 'EXACT_CAPTURE'}
                    result[component] = _validated(record)
                else:
                    # An earlier OI capture cannot use a subsequently sampled quote.
                    result[component] = {'status': 'NEEDS_QUOTE', 'component': component,
                                         'symbol': symbol, 'anchor_time_utc': anchor.isoformat()}
            else:
                if component == 'MAX_PAIN':
                    rows = sources['maxpain_operational_rows']
                    if len(rows) != 7 or len({r['timeframe'] for r in rows}) != 7:
                        raise ValueError('Incomplete Max Pain clocks')
                    anchors = [_utc(row['source_observed_at_utc']) for row in rows]
                    if max(anchors) > computed:
                        raise ValueError('Future Max Pain source')
                    anchor = min(anchors)
                else:
                    source = 'futures' if component == 'FUTURES_CVD' else 'spot'
                    anchor = _utc(sources[source]['quality']['candle_close'])
                result[component] = {'status': 'NEEDS_QUOTE', 'component': component,
                                     'symbol': symbol, 'anchor_time_utc': anchor.isoformat()}
            if not computed - timedelta(hours=2) <= anchor <= computed:
                raise ValueError('Stale or future current input clock')
        except (KeyError, ValueError, TypeError, OverflowError, AttributeError):
            result[component] = _missing('MISSING_OR_INVALID_INPUT_CLOCK')
    return result


def _fetch_coin(symbol, boundaries):
    import research_archived_price_path as paths
    import research_price_archive as archive
    start, end = min(boundaries)-_MINUTE, max(boundaries)-timedelta(milliseconds=1)
    route = archive.BINANCE_MARK if symbol == 'HYPE' else archive.BINANCE_SPOT
    fetcher = paths.fetch_mark if symbol == 'HYPE' else paths.fetch_binance
    path = fetcher(symbol, start, end)
    bars = archive.validate_path(route, symbol, path, start, end)
    return {bar['open_time_utc'] + _MINUTE: bar for bar in bars}


async def prepare_reference_prices(bundle, *, fetch_coin=None):
    """Bounded one-fetch-per-coin enrichment, outside notification transactions.

Timeouts never suppress an alert and never overwrite a missing historical quote
with the current market price. Thread results cannot mutate returned references.
"""
    if not isinstance(bundle, Mapping) or not isinstance(bundle.get('coins'), Mapping):
        return {}
    try:
        computed = _utc(bundle['computed_at_utc'])
    except (KeyError, ValueError, TypeError, OverflowError):
        return {}
    _STATUS['last_error_type'] = None
    output = {symbol: _inputs(bundle['coins'].get(symbol) or {}, symbol, computed) for symbol in SYMBOLS}
    pending = {}
    for symbol, refs in output.items():
        boundaries = {_utc(r['anchor_time_utc']).replace(second=0, microsecond=0)
                      for r in refs.values() if r.get('status') == 'NEEDS_QUOTE'}
        if boundaries:
            task = asyncio.create_task(asyncio.to_thread(fetch_coin or _fetch_coin, symbol, boundaries))
            pending[task] = symbol
    if pending:
        done, waiting = await asyncio.wait(pending, timeout=TIMEOUT_SECONDS)
        for task in waiting:
            task.cancel()
        for task, symbol in pending.items():
            try:
                if task not in done:
                    raise TimeoutError('Reference quote deadline')
                bars = task.result()
                for component, reference in list(output[symbol].items()):
                    if reference.get('status') != 'NEEDS_QUOTE':
                        continue
                    boundary = _utc(reference['anchor_time_utc']).replace(second=0, microsecond=0)
                    bar = bars.get(boundary)
                    if not bar:
                        output[symbol][component] = _missing('MISSING_REFERENCE_MINUTE')
                        continue
                    record = {**reference, 'status': 'READY', 'version': VERSION,
                              'price': str(bar['close']), 'price_time_utc': boundary.isoformat(),
                              'bar_close_time_utc': _utc(bar['close_time_utc']).isoformat(),
                              'source': 'BINANCE_HYPE_FUTURES_MARK_1M' if symbol == 'HYPE' else 'BINANCE_SPOT_TRADE_1M',
                              'precision': 'CLOSED_1M'}
                    output[symbol][component] = _validated(record)
            except Exception as exc:
                _STATUS['last_error_type'] = type(exc).__name__
                for component, record in list(output[symbol].items()):
                    if record.get('status') == 'NEEDS_QUOTE':
                        output[symbol][component] = _missing('REFERENCE_PRICE_UNAVAILABLE')
    ready = sum(r.get('status') == 'READY' for refs in output.values() for r in refs.values())
    _STATUS.update(last_cycle_id=bundle.get('cycle_id'), ready_references=ready,
                   missing_references=len(COMPONENTS)*len(SYMBOLS)-ready)
    return output
