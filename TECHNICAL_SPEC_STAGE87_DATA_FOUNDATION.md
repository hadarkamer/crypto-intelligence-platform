# Stage 87 — Data Foundation (Official CVD Revision)

## Scope

This stage adds isolated data collection and validation only. It does not create
Futures Flow, Spot Flow, Market Confidence, a trading score, or a trade decision.

## Existing trading behavior

The following remain unchanged:

- Max Pain scoring and distance points
- LONG/SHORT selection
- BTC confirmation
- Consensus, Cluster, Gap and liquidity display
- `/alerts`, `/alerts_top8`, `/alerts_liq`
- general and targeted Watch

## OI historical reference

The Price+OI history table remains `oi_price_history` with unique key
`(symbol, candle_time)`. Backfill upserts existing candles and adds new ones.

Manual backfill defaults to 180 days and supports up to 365 days:

- `/oi_backfill`
- `/oi_backfill 365`

Reference windows:

`30m, 1h, 4h, 12h, 24h, 48h, 72h, 7d`

## Official CVD foundation

Isolated module: `coinglass_flow_foundation.py`.

It calls CoinGlass official aggregated CVD endpoints at 30-minute resolution:

- Futures: `/api/futures/aggregated-cvd/history`
- Spot: `/api/spot/aggregated-cvd/history`

Supported symbols:

`BTC, ETH, SOL, HYPE, DOGE, ZEC, BNB, XRP`

Tables:

- `futures_taker_history`
- `spot_taker_history`

Each candle stores:

- `buy_volume_usd` — official aggregated taker buy volume
- `sell_volume_usd` — official aggregated taker sell volume
- `api_cum_vol_delta_usd` — the exact CVD returned by CoinGlass for that request
- `continuous_cum_vol_delta_usd` — a stitched series across pagination chunks
- exchange list, source and import timestamp

CoinGlass calculates CVD from the request `start_time`. A 180/365-day backfill
requires multiple API requests, so the official CVD may restart at a chunk
boundary. The API value is preserved untouched and a second continuous field is
stored for future analysis.

Primary key remains `(symbol, candle_time)`. Re-running backfill updates the same
candle and cannot create duplicate market rows.

Command:

- `/flow_backfill` — 180 days
- `/flow_backfill 365` — 365 days

This stage stores the official CVD but does not yet interpret it as Flow.

## Price/OI timing validation

Each live Price+OI snapshot stores:

- `price_fetched_at`
- `oi_fetched_at`
- `time_gap_seconds`
- `data_quality_status`
- `price_source`
- `oi_source`

Quality:

- `PASS`: 0–30 seconds
- `WARNING`: above 30 and up to 60 seconds
- `INVALID`: above 60 seconds

An INVALID pair is stored for audit/history but is not used to create a combined
Price+OI conclusion for that cycle.

## Reference tolerance

A historical reference point must be no more than 20 minutes older than the
requested target time. Otherwise that window returns No Data.

Read-only command:

- `/oi_validation BTC`
