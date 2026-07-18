# Stage 54 — Per-symbol alerts and seven-timeframe score display

## Changes

- Every alert card now ends with a compact score list for all canonical timeframes:
  `12h, 24h, 48h, 3d, 1w, 2w, 1m`.
- The score list appears only at the bottom of the alert message.
- Added `/alert SYMBOL`, for example `/alert BTC`.
- `/alert SYMBOL` runs one fresh seven-timeframe scan and sends a separate message for each timeframe of the selected symbol.
- A timeframe without an active scorable Max Pain target is reported explicitly instead of being silently omitted.
- Existing `/alerts`, Watch, scoring, TradingView shadow mode, and Stage 53 cluster/scoring logic remain intact.
