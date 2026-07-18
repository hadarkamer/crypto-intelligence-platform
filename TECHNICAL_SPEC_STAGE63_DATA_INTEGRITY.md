# Stage 63 — Data Integrity Audit

- Binance USD-M Futures Mark Price is the sole live price source.
- Symbol resolution uses Binance Futures exchangeInfo and direct premiumIndex fallback.
- CoinGlass rows are accepted only after 13-cell schema validation and distance cross-checks.
- A shifted CoinGlass DOM row is rejected instead of silently storing wrong amounts.
- Atomic seven-timeframe collection remains mandatory.
- Cluster display no longer uses ambiguous ratios such as 4/6 or 3/3.
- Cluster members and active same-direction timeframes are shown separately.
