# Stage 67 — Alert summary, HYPE price, counter-direction score

## Changes

1. `/alerts` and each Watch cycle send a final per-symbol summary of the alerts actually displayed, for example: `BTC: 2 LONG, 1 SHORT`.
2. HYPE uses the official Hyperliquid public Info API `allMids` response when Binance Spot/Futures has no HYPE price.
3. Counter-direction scoring is isolated in `counter_score.py`; it runs only while formatting selected Telegram alerts and does not alter the primary score in `alert_engine.py`.
4. Alert cards show the correct live-price source (`Binance` or `Hyperliquid`).

## New optional environment variables

- `HYPERLIQUID_INFO_URL` (default: `https://api.hyperliquid.xyz/info`)
- `HYPERLIQUID_PRICE_TIMEOUT_SECONDS` (default: `10`)
