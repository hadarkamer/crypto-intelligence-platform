# Audit and rebuild summary

## Removed completely
- Hyperliquid module and all Hyperliquid artifacts.
- `/collect` command registration and handler.
- Compiled `.pyc` files, `__pycache__`, pytest cache, `nodeids`, `CACHEDIR.TAG`, `download`, duplicate README, and obsolete stage-spec clutter.
- Corrupted source files from the uploaded repository snapshot.
- Binance Spot fallback and duplicate Spot environment variables.

## Rebuilt or changed locally
- Restored valid source modules from the last clean Stage 68/69 lineage rather than trusting corrupted `.py` files in the uploaded ZIP.
- `/coin` now acquires the shared scanner lock, runs a fresh seven-timeframe CoinGlass scan, overlays Binance Futures mark price, and never reads a saved snapshot.
- Binance price provider now accepts Futures mark price only.
- Render startup retains the port-first startup fix.
- Requirements, runtime, environment example, README, and `.gitignore` were rebuilt.

## Restored integrations
- TradingView/Rai webhook parser and normalization.
- `POST /tradingview` and `POST /webhooks/tradingview`.
- Technical-signal database schema, Shadow Mode persistence, API status, and `/technical_status`.
- Bullish -> LONG, Bearish -> SHORT, Strong Zone -> NEUTRAL normalization.

## Live price policy
All active user-facing market scans and alert/watch scans use the same live scanner and Binance USD-M Futures mark-price enrichment. Symbols without a Futures price are excluded rather than falling back to Spot.
