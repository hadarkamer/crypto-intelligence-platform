Stage 10 v2 — Clear Binance Live Price Check

Changes:
- SNDK is now filtered as a non-crypto asset.
- Binance market-data URL is configurable through environment variables.
- No Binance API key is required.
- /price_check now explains clearly that it is a connection/coverage test.
- /price_check shows a sample of actual live prices.
- /price_check BTC compares the Binance live price to all seven Max Pain targets.
- Old excluded symbols are ignored even if they still exist in the latest DB snapshot.

This stage still does NOT change alert calculations.

Test:
1. Deploy
2. /collect
3. /price_check
4. /price_check BTC
