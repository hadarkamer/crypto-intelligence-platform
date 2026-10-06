"""Pure causal measurements for the separate frozen all-Watch population.

Entry is the next minute's route-specific open, strictly after source usability.
The supplied path must come from the exact closed-1m archive contract. This
module does not read a database, request prices, create events, or fit formulas.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from typing import Any, Mapping

import research_btc_parent_movement as btc
import research_ordered_first_touch as ordered

VERSION = 'watch-scan-measurements-v1'
ENTRY_VERSION = 'watch-next-full-minute-route-open-v1'
POPULATION = 'watch-all-scan-observations-v1'
CONSUMER_VERSION = 'watch-all-scan-intake-v1'
SOURCE_VERSION = 'watch-operational-scores-v2'
WINDOWS = (60, 240, 720, 1440)
DIRECTIONS = ('LONG', 'SHORT')
BINANCE_SPOT = 'BINANCE_SPOT_TRADE_1M'
HYPE_PERP = 'HYPERLIQUID_HYPE_PERP_TRADE_1M'
SPOT_SYMBOLS = ('BTC', 'ETH', 'SOL', 'DOGE', 'ZEC', 'BNB', 'XRP')
MINUTE = timedelta(minutes=1)
MILLISECOND = timedelta(milliseconds=1)


def utc(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError('EXPLICIT_TIMEZONE_REQUIRED')
    return parsed.astimezone(timezone.utc)


def entry_time(usable_from: Any) -> datetime:
    """Strict next full minute, including an exactly minute-aligned source."""
    return utc(usable_from).replace(second=0, microsecond=0) + MINUTE


def route_for_symbol(symbol: str) -> str:
    if symbol == 'HYPE':
        return HYPE_PERP
    if symbol in SPOT_SYMBOLS:
        return BINANCE_SPOT
    raise ValueError('UNSUPPORTED_SYMBOL')


def _canonical(value: Any) -> str:
    def normalized(item):
        if isinstance(item, datetime):
            return utc(item).isoformat()
        if isinstance(item, float) and item.is_integer():
            return int(item)
        if isinstance(item, Mapping):
            return {key: normalized(value) for key, value in item.items()}
        if isinstance(item, (list, tuple)):
            return [normalized(value) for value in item]
        return item
    return json.dumps(normalized(value), sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False)


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _hash(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(c in '0123456789abcdef' for c in value)


def _number(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError('INVALID_NUMERIC_PRICE')
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise ValueError('INVALID_NUMERIC_PRICE')
    return result


def _source(symbol: str, path_result: Mapping) -> dict:
    route = route_for_symbol(symbol)
    if symbol == 'HYPE':
        expected = {
            'symbol': 'HYPE', 'pair': 'HYPE-PERP', 'exchange': 'hyperliquid',
            'market': 'perpetual', 'price_kind': 'TRADE', 'instrument': 'HYPE',
            'margin_currency': 'USDC', 'interval': '1m', 'interval_seconds': 60,
            'source_url': 'https://api.hyperliquid.xyz/info',
            'method_version': 'hyperliquid-hype-perp-trade-1m-v1',
            'provenance': 'OFFICIAL_HYPERLIQUID_HYPE_PERPETUAL_TRADE_CANDLES_1M',
        }
    else:
        expected = {'symbol': symbol, 'pair': symbol + 'USDT', 'exchange': 'binance',
                    'market': 'spot', 'interval': '1m', 'interval_seconds': 60,
                    'multiplier': 1.0}
        if path_result.get('instrument') not in (None, '') or path_result.get('api_coin') not in (None, ''):
            raise ValueError('CONFLICTING_PRICE_INSTRUMENT')
        if path_result.get('price_kind') not in (None, '', 'TRADE'):
            raise ValueError('CONFLICTING_PRICE_KIND')
    if path_result.get('archive_route') != route:
        raise ValueError('ARCHIVE_ROUTE_MISMATCH')
    for key, value in expected.items():
        actual = path_result.get(key)
        if actual != value or isinstance(actual, bool):
            raise ValueError('PRICE_SOURCE_MISMATCH:' + key)
    if type(path_result.get('interval_seconds')) is not int:
        raise ValueError('INVALID_CANDLE_INTERVAL')
    return {**expected, 'archive_route': route}


def _bar(raw: Any) -> dict:
    get = raw.get if isinstance(raw, Mapping) else lambda key: getattr(raw, key)
    opened, closed = utc(get('open_time_utc')), utc(get('close_time_utc'))
    if opened.second or opened.microsecond or closed != opened + MINUTE - MILLISECOND:
        raise ValueError('INVALID_CLOSED_1M_INTERVAL')
    prices = {key: _number(get(key)) for key in ('open', 'high', 'low', 'close')}
    if prices['high'] < max(prices.values()) or prices['low'] > min(prices.values()):
        raise ValueError('INVALID_OHLC_ENVELOPE')
    return {'open_time_utc': opened, 'close_time_utc': closed, **prices}


def _membership(decision: datetime, parent: Mapping | None, btc_bar: Mapping | None) -> dict:
    # The old pure helper expects an event-shaped argument. This temporary
    # zero has no event identity and is removed before any result can persist.
    stub = {'event_id': 0, 'alert_time_utc': decision}
    try:
        if parent and btc_bar:
            for name in ('start_time_utc', 'end_time_utc', 'confirmed_at_utc'):
                if parent.get(name) is not None:
                    utc(parent[name])
            if parent.get('price_source') != btc.SOURCE:
                raise ValueError('INVALID_BTC_PARENT_SOURCE')
            normalized = _bar(btc_bar)
            result = btc.membership(stub, parent=parent, btc_bar=normalized)
        else:
            result = btc.membership(stub, parent=None, btc_bar=None)
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError):
        result = btc.membership(stub, parent=None, btc_bar=None)
    result.pop('event_id', None)
    return result


def _compact_label(label: dict) -> dict:
    # These constants live in the measurement/record envelope. All threshold,
    # barrier, status, timing, gap and stopped-path semantics remain attached.
    repeated = {'method_version', 'direction', 'reference_price', 'measurement_start_utc',
                'candle_interval_seconds', 'outcome_decided_at_utc', 'first_observed_open_utc'}
    return {key: value for key, value in label.items() if key not in repeated}


def measure(observation: dict, path_result: dict, *, observed_at: datetime,
            parent: dict | None = None, btc_bar: dict | None = None) -> dict:
    """Return one coin's two-direction, four-window measurements.

    Observation must be an ACCEPTED v2 receipt/coin from the intake view.
    Unsupported source/path evidence is returned explicitly without outcomes.
    Missing BTC membership never removes otherwise measurable price records.
    """
    now = utc(observed_at)
    fields = ('snapshot_set_id', 'consumer_version', 'population_version', 'symbol',
              'source_version', 'bundle_sha256', 'parent_payload_sha256', 'capture_phase')
    result = {key: observation.get(key) for key in fields}
    result.update(version=VERSION, entry_version=ENTRY_VERSION,
        outcome_method_version=ordered.METHOD_VERSION, candle_interval_seconds=60,
        status='INVALID_OBSERVATION', error_reason=None,
        source_observed_at_utc=None, usable_from_utc=None, observed_at_utc=now,
        entry_time_utc=None, entry_delay_seconds=None, reference_price=None,
        price_route=None, price_source=None, membership=None, records=[])

    def fail(status: str, reason: str) -> dict:
        result.update(status=status, error_reason=reason)
        return result

    try:
        symbol = observation['symbol']
        if (observation.get('intake_status') != 'ACCEPTED'
                or observation.get('consumer_version') != CONSUMER_VERSION
                or observation.get('population_version') != POPULATION
                or observation.get('source_version') != SOURCE_VERSION
                or type(observation.get('snapshot_set_id')) is not int or observation['snapshot_set_id'] <= 0
                or observation.get('capture_phase') not in ('HISTORICAL_CAPTURE', 'FORWARD_CAPTURE')
                or not _hash(observation.get('bundle_sha256'))
                or not _hash(observation.get('parent_payload_sha256'))):
            raise ValueError('INVALID_INTAKE_IDENTITY')
        route = route_for_symbol(symbol)
        source_time = utc(observation['observed_at_utc'])
        usable = utc(observation['usable_from_utc'])
        available = utc(observation['source_available_at_utc'])
        created = utc(observation['source_created_at_utc'])
        if usable != max(source_time, available, created) or usable > now or source_time > available:
            raise ValueError('INVALID_SOURCE_AVAILABILITY')
        entry = entry_time(usable)
        result.update(source_observed_at_utc=source_time, usable_from_utc=usable,
            entry_time_utc=entry, entry_delay_seconds=(entry-usable).total_seconds(),
            price_route=route, membership=_membership(usable, parent, btc_bar))
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError) as exc:
        return fail('INVALID_OBSERVATION', str(exc))

    latest_closed = now.replace(second=0, microsecond=0) - MILLISECOND
    if latest_closed < entry + MINUTE - MILLISECOND:
        return fail('NOT_YET_ENTRY', 'ENTRY_MINUTE_NOT_YET_CLOSED')
    try:
        source = _source(symbol, path_result)
        result['price_source'] = source
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError) as exc:
        return fail('INVALID_SOURCE', str(exc))
    cutoff = min(latest_closed, entry + timedelta(minutes=max(WINDOWS)))
    try:
        raw_candles = path_result.get('candles')
        if not isinstance(raw_candles, (list, tuple)) or len(raw_candles) > max(WINDOWS):
            raise ValueError('INVALID_OR_OVERSIZED_PATH')
        bars = [_bar(raw) for raw in raw_candles]
        if any(bar['open_time_utc'] < entry or bar['close_time_utc'] > cutoff for bar in bars):
            raise ValueError('PATH_OUTSIDE_CLOSED_MEASUREMENT_RANGE')
        if len({bar['open_time_utc'] for bar in bars}) != len(bars):
            raise ValueError('DUPLICATE_CANDLE')
        bars.sort(key=lambda bar: bar['open_time_utc'])
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError) as exc:
        return fail('INVALID_PATH', str(exc))
    if not bars or bars[0]['open_time_utc'] != entry:
        return fail('DATA_MISSING_ENTRY', 'EXACT_ENTRY_MINUTE_MISSING')
    reference = bars[0]['open']
    result['reference_price'] = reference

    records = []
    for direction in DIRECTIONS:
        for window in WINDOWS:
            end = entry + timedelta(minutes=window)
            window_cutoff = min(end, latest_closed)
            path = [bar for bar in bars if bar['close_time_utc'] <= window_cutoff]
            expected = max(0, min(window, int((now.replace(second=0, microsecond=0)-entry)/MINUTE)))
            expected_opens = [entry + index*MINUTE for index in range(expected)]
            prefix_complete = [bar['open_time_utc'] for bar in path] == expected_opens
            mature = now >= end
            status = 'DATA_MISSING' if not prefix_complete else 'READY' if mature else 'OPEN'
            labels = ordered.calculate_all_ordered_first_touch_outcomes(
                reference_price=reference, direction=direction, event_time=entry,
                candles=path, observation_closed=mature, path_complete=prefix_complete)
            record = {'direction': direction, 'window_minutes': window,
                'status': status, 'window_end_utc': end,
                'observed_through_utc': path[-1]['close_time_utc'] if path else None,
                'path_sha256': _digest({'entry_version': ENTRY_VERSION, 'source': source,
                    'entry_time_utc': entry, 'window_minutes': window, 'candles': path}),
                'path_complete': status == 'READY', 'observed_prefix_complete': prefix_complete,
                'expected_candles': expected, 'path_samples': len(path),
                'mfe_pct': None, 'mae_pct': None, 'asymmetry_ratio': None,
                'asymmetry_status': 'WINDOW_NOT_READY',
                'labels': [_compact_label(label) for label in labels]}
            if status == 'READY':
                extrema = ordered._excursion_metrics(reference_price=reference, direction=direction,
                    maximum=max([reference] + [bar['high'] for bar in path]),
                    minimum=min([reference] + [bar['low'] for bar in path]))
                record.update(mfe_pct=extrema['mfe_pct'], mae_pct=extrema['mae_pct'],
                    asymmetry_ratio=extrema['mfe_pct']/extrema['mae_pct'] if extrema['mae_pct'] > 0 else None,
                    asymmetry_status='DEFINED' if extrema['mae_pct'] > 0 else 'UNDEFINED_ZERO_MAE')
            records.append(record)
    result.update(records=records, status='DATA_MISSING' if any(r['status']=='DATA_MISSING' for r in records)
                  else 'READY' if all(r['status']=='READY' for r in records) else 'OPEN')
    return result
