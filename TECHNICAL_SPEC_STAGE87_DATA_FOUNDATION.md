# Stage 87 — Data Foundation

## Scope

This stage adds data collection and validation only. It stores CoinGlass' official
CVD together with the matching Buy/Sell volumes, but it does not calculate
Futures Flow, Spot Flow, Market Confidence, or any trading score.

## Existing trading behavior

The following remain unchanged:

- Max Pain scoring and distance points
- LONG/SHORT selection
- BTC confirmation
- Consensus, Cluster, Gap and liquidity display
- `/alerts`, `/alerts_top8`, `/alerts_liq`
- general and targeted Watch

## OI historical reference

The historical Price+OI table remains `oi_price_history` with the unique key
`(symbol, candle_time)`. Re-running backfill updates existing candles and adds
new candles, so market rows are not duplicated.

Manual backfill defaults to 180 days and supports up to 365 days:

- `/oi_backfill`
- `/oi_backfill 365`

The daily automatic refresh downloads only the latest 3 days and upserts them.

Reference windows now include:

`30m, 1h, 4h, 12h, 24h, 48h, 72h, 7d`

## CoinGlass CVD foundation

New isolated module: `coinglass_flow_foundation.py`.

It downloads 30-minute aggregated CoinGlass CVD candles for:

- Futures
- Spot

Supported symbols:

`BTC, ETH, SOL, HYPE, DOGE, ZEC, BNB, XRP`

New tables:

- `futures_taker_history`
- `spot_taker_history`

Both use `(symbol, candle_time)` as the primary key. Re-running the backfill
updates the same candle and cannot create a duplicate market row.

Command:

- `/flow_backfill` — 180 days
- `/flow_backfill 365` — 365 days

Each row stores `buy_volume_usd`, `sell_volume_usd`, and CoinGlass'
`cum_vol_delta_usd` from the same API candle. Long backfills stitch the official
CVD segments into one continuous series because each API request restarts its
cumulative value at the request start. No Flow conclusion is calculated in this
stage.

## Price/OI timing validation

Each live Price+OI snapshot now stores:

- `price_fetched_at`
- `oi_fetched_at`
- `time_gap_seconds`
- `data_quality_status`
- `price_source`
- `oi_source`

Quality status:

- `PASS`: 0–30 seconds
- `WARNING`: above 30 and up to 60 seconds
- `INVALID`: above 60 seconds

An INVALID pair is stored for audit/history but is not used to create a combined
Price+OI conclusion for that collection cycle.

## Reference tolerance

For a requested window, the selected historical point must be no more than 20
minutes older than the requested target time. Otherwise the window returns No
Data instead of silently using a substantially older point.

Read-only command:

- `/oi_validation BTC`

It reports sources, fetch timestamps, the Price/OI gap, quality status, and
reference availability for the existing live regime windows.
