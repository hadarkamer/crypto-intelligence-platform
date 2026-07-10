Stage 11 v2 — Binance Live Calculations + Full Price Precision

Changes:
- Keeps all Stage 11 live-price calculations.
- Adds fmt_price() for every displayed market price and Max Pain target.
- No meaningful decimal digit is rounded away.
- Scientific notation is avoided.
- Only meaningless trailing zeros are removed.

Examples:
0.0000123456789 -> 0.0000123456789
123.4500000000 -> 123.45

Affected displays:
/price_check
/price_check BTC
/coin BTC
/range BTC 24h
/top

Calculation logic is unchanged.
