# Stage 69 — HYPE multi-source price fallback

HYPE price priority:
1. Bybit USDT perpetual (`linear`, mark price preferred)
2. Bybit Spot
3. Hyperliquid `allMids`
4. CoinGecko `simple/price`
5. CoinPaprika search + ticker
6. CoinGlass DOM current price already present in the collected row

Each failed source is logged. Other symbols and alert/scoring behavior are unchanged.
