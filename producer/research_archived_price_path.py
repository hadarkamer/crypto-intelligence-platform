"""Persistent readers over the existing exact exchange price contracts.

Raw provider modules remain independently usable. The archive never converts
between Spot, perpetual trade, or perpetual mark prices.
"""
import binance_spot_price_path
import binance_futures_mark_price_path
import hyperliquid_spot_price_path
import hyperliquid_perp_price_path


def _read(route, symbol, start_time, end_time, fetcher):
    import research_price_archive
    return research_price_archive.get_path(route, symbol, start_time, end_time, fetcher)


def fetch_binance(symbol, start_time, end_time):
    return _read("BINANCE_SPOT_TRADE_1M", symbol, start_time, end_time,
                 binance_spot_price_path.fetch_closed_candles)


def fetch_perp(symbol, start_time, end_time):
    return _read("HYPERLIQUID_HYPE_PERP_TRADE_1M", symbol, start_time, end_time,
                 hyperliquid_perp_price_path.fetch_closed_candles)


def fetch_mark(symbol, start_time, end_time):
    return _read("BINANCE_HYPE_FUTURES_MARK_1M", symbol, start_time, end_time,
                 binance_futures_mark_price_path.fetch_closed_candles)


def fetch_hype_spot(symbol, start_time, end_time):
    return _read("HYPERLIQUID_HYPE_SPOT_TRADE_1M", symbol, start_time, end_time,
                 hyperliquid_spot_price_path.fetch_closed_candles)
