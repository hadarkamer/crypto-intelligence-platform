# Stage 49 — Dynamic Price Formatting

Display-only change.

Precision:
- price >= 100: 2 decimals
- 10 <= price < 100: 3 decimals
- 1 <= price < 10: 4 decimals
- 0.1 <= price < 1: 5 decimals
- 0.01 <= price < 0.1: 6 decimals
- 0.001 <= price < 0.01: 7 decimals
- price < 0.001: 8 decimals

Trailing zeros are removed.

Examples:
- BTC 62577.50 -> 62577.5
- DOGE 0.075432 -> 0.075432
- PEPE 0.00001173 -> 0.00001173

Scoring, distance calculations, filtering, collection and Watch behavior remain unchanged.
