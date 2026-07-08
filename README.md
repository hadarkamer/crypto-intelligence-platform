Stage 7 Hyperliquid Probe Patch

Adds diagnostic Hyperliquid probe only:
- hyperliquid_reader.py
- /hyper_debug BTC
- /symbols

Purpose:
Find how CoinGlass renders Hyperliquid liquidation-map data before building real extraction.
This stage does not save Hyperliquid data and does not change Max Pain collection.

Test after deploy:
/symbols
/hyper_debug BTC

Then send:
- Telegram summary
- Render logs starting with [hyper]
