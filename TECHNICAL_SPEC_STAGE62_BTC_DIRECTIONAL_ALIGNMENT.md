# Stage 62 — BTC Directional Alignment

## Directional Alignment (30 points)

### Altcoins
- Own consensus: continuous 0–15.
- BTC in the same direction and same timeframe: continuous confirmation 0–15, calculated as `BTC total score / 100 × 15`.
- BTC in the opposite direction and same timeframe: continuous penalty 0–10, calculated as `BTC total score / 100 × 10`.
- Final Directional Alignment is clamped to 0–30.

### BTC
- BTC does not confirm itself.
- Its Directional Alignment is consensus only, continuously scaled to 0–30.

### Market breadth
- Remains visible as information.
- Does not add or subtract points.
