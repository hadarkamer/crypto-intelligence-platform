-- Source-separated immutable closed 1m OHLC. No expiry/deletion policy.
CREATE TABLE IF NOT EXISTS research_price_archive_bars (
    route TEXT NOT NULL CHECK (route IN ('BINANCE_SPOT_TRADE_1M',
        'HYPERLIQUID_HYPE_PERP_TRADE_1M','BINANCE_HYPE_FUTURES_MARK_1M',
        'HYPERLIQUID_HYPE_SPOT_TRADE_1M')),
    symbol TEXT NOT NULL,
    open_time_utc TIMESTAMPTZ NOT NULL,
    close_time_utc TIMESTAMPTZ NOT NULL,
    open DOUBLE PRECISION NOT NULL, high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL, close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(route,symbol,open_time_utc),
    CHECK ((route='BINANCE_SPOT_TRADE_1M' AND symbol IN
        ('BTC','ETH','SOL','BNB','XRP','DOGE','ZEC')) OR
        (route<>'BINANCE_SPOT_TRADE_1M' AND symbol='HYPE')),
    CHECK (date_trunc('minute',open_time_utc)=open_time_utc),
    CHECK (close_time_utc=open_time_utc+INTERVAL '1 minute'-INTERVAL '1 millisecond'),
    CHECK (low>0 AND high<'Infinity'::double precision AND low<=high
        AND low<=open AND low<=close AND high>=open AND high>=close),
    CHECK (volume IS NULL OR (volume>=0 AND volume<'Infinity'::double precision)),
    CHECK ((route IN ('BINANCE_SPOT_TRADE_1M','HYPERLIQUID_HYPE_SPOT_TRADE_1M'))
        = (volume IS NOT NULL))
);
CREATE OR REPLACE FUNCTION research_price_archive_immutable_v1()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'Archived price bars cannot be deleted';
    END IF;
    IF (NEW.route,NEW.symbol,NEW.open_time_utc,NEW.close_time_utc,
        NEW.open,NEW.high,NEW.low,NEW.close,NEW.volume)
       IS DISTINCT FROM
       (OLD.route,OLD.symbol,OLD.open_time_utc,OLD.close_time_utc,
        OLD.open,OLD.high,OLD.low,OLD.close,OLD.volume) THEN
        RAISE EXCEPTION 'Archived price revision conflicts with frozen source evidence';
    END IF;
    RETURN NULL;
END $$;
DROP TRIGGER IF EXISTS research_price_archive_immutable_v1 ON research_price_archive_bars;
CREATE TRIGGER research_price_archive_immutable_v1 BEFORE UPDATE OR DELETE
    ON research_price_archive_bars FOR EACH ROW
    EXECUTE FUNCTION research_price_archive_immutable_v1();

CREATE TABLE IF NOT EXISTS research_price_archive_cursors (
    route TEXT NOT NULL, symbol TEXT NOT NULL,
    requested_start_utc TIMESTAMPTZ NOT NULL,
    history_cursor_utc TIMESTAMPTZ NOT NULL,
    unavailable_before_utc TIMESTAMPTZ,
    last_tail_attempt_utc TIMESTAMPTZ,
    last_history_attempt_utc TIMESTAMPTZ,
    latest_open_utc TIMESTAMPTZ,
    last_error TEXT,
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(route,symbol),
    CHECK (history_cursor_utc>=requested_start_utc)
);
CREATE TABLE IF NOT EXISTS research_price_archive_gaps (
    route TEXT NOT NULL, symbol TEXT NOT NULL,
    start_time_utc TIMESTAMPTZ NOT NULL, end_time_utc TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('RETRY','UNAVAILABLE')),
    reason TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 1,
    next_retry_utc TIMESTAMPTZ NOT NULL,
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(route,symbol,start_time_utc,end_time_utc),
    CHECK (end_time_utc>=start_time_utc)
);
CREATE INDEX IF NOT EXISTS idx_research_price_archive_gaps_due
    ON research_price_archive_gaps(next_retry_utc,route,symbol)
    WHERE status='RETRY';
