# Crypto Intelligence Platform — Clean Live-Scan Build

Telegram bot for live CoinGlass Max Pain scanning, Binance USD-M Futures mark-price calculations, approved alert/watch commands, and TradingView indicator ingestion in Shadow Mode.

## Active Telegram commands
- `/coin BTC` — fresh seven-timeframe scan for one coin.
- `/alerts` — fresh market scan and approved opportunities.
- `/alert BTC` — fresh scan for one coin across seven timeframes.
- `/debug BTC` — fresh diagnostic scan.
- `/watch_on`, `/watch_status`, `/watch_stop` — approved live watch workflow.
- `/technical_status` — last TradingView/Rai indicator signals.

`/collect` was removed. User-facing scans do not depend on a saved snapshot. All current-price calculations use Binance USD-M Futures mark price only; there is no Spot fallback. Hyperliquid code was removed.

## Render
- Build command: `pip install -r requirements.txt && playwright install chromium`
- Start command: `python main.py`
- Set the variables documented in `env.example`.

## Webhook routes
- Telegram: `POST /telegram`
- TradingView/Rai: `POST /tradingview` or `POST /webhooks/tradingview`
- Technical status: `GET /technical/status`
- Health: `GET /health`
