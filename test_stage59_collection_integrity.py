# Stage 60 — Live Binance Price Refresh

- Binance Spot last price is the primary current-price source.
- USD-M Futures mark price is used only as fallback.
- `/coin SYMBOL` refreshes Binance price at command time and recalculates all distances against the latest saved Max Pain targets.
- Live scans used by `/alerts`, `/alert`, `/debug`, and Watch already refresh Binance immediately after CoinGlass collection and continue to do so.
- Output includes price source and UTC fetch time.
