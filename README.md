Stage 13 — Save Binance Price During /collect

Core correction:
- /collect reads Max Pain targets and liquidation amounts from CoinGlass.
- /collect fetches Binance prices once for all symbols.
- It recalculates and stores current_price, distances and closest-side inputs
  from Binance before inserting the snapshot into the database.
- CoinGlass current price is never saved as a fallback.
- Symbols without Binance coverage are skipped.

Consistency change:
- Analysis commands no longer fetch a newer Binance price independently.
- /coin, /range, /top, /consensus, /gap, /market, /btc_like, /score and alerts
  all use the exact Binance price saved in the latest /collect snapshot.
- This keeps every command internally consistent with one collection moment.

Also:
- collection timestamp is the exact collection time, not a rounded hour.
- /collect completion message includes the available command list.
- /live_status explains the saved Binance-backed snapshot.

Test:
1. /collect
2. /live_status
3. /coin BTC
4. /range BTC 24h
5. /top
6. /consensus
7. /alert_check
