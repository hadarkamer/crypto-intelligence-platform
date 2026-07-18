# Stage 44 Technical Specification

## Timeframe verification
CoinGlass opens on 24h by default. The collector first accepts 24h as a
baseline, then accepts 12h only after the table fingerprint changes.

## Symbol completeness
A symbol enters the scoring engine or database only when it has one unique row
for each of the seven timeframes.

## Collect audit
Telegram and Render logs expose:
- raw DOM rows
- Binance-priced rows
- complete symbols
- expected database rows
- actual database rows
- incomplete symbols and missing timeframes
- duplicate symbol/timeframe pairs
