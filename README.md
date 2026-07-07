Crypto Intelligence Max Pain Parser Patch

Fix:
- Parses real CoinGlass Max Pain rows from DOM body text.
- Maps fields:
  symbol, price, short max pain price, short amount, short distance $, short distance %,
  long max pain price, long amount, long distance $, long distance %.
- Drops fake rows like NEW/API/APP.

After /collect logs should show parsed_count around 50 per timeframe and inserted rows > 0.
