-- Extend source-separated TRADE archives without rewriting any historical bar.
-- HYPE retains its original route; the five additional coins have a new route.
-- Startup/admin migration only. Never run DDL in recurring collection work.
ALTER TABLE research_price_archive_bars
    DROP CONSTRAINT IF EXISTS research_price_archive_bars_route_check,
    DROP CONSTRAINT IF EXISTS research_price_archive_bars_check,
    DROP CONSTRAINT IF EXISTS research_price_archive_route_symbol_v2;

ALTER TABLE research_price_archive_bars
    ADD CONSTRAINT research_price_archive_route_symbol_v2 CHECK (
        (route='BINANCE_SPOT_TRADE_1M' AND symbol IN
            ('BTC','ETH','SOL','BNB','XRP','DOGE','ZEC'))
        OR (route IN ('HYPERLIQUID_HYPE_PERP_TRADE_1M',
            'BINANCE_HYPE_FUTURES_MARK_1M','HYPERLIQUID_HYPE_SPOT_TRADE_1M')
            AND symbol='HYPE')
        OR (route='HYPERLIQUID_PERP_TRADE_1M' AND symbol IN
            ('BTC','ETH','SOL','XRP','DOGE'))
    ) NOT VALID;

-- Existing rows were checked by v1 or this same broader v2 contract. NOT VALID
-- avoids a full historical scan during installation while enforcing every new
-- insert/update. Original OHLC/time/volume constraints and immutability remain.
