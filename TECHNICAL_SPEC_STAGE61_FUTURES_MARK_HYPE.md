# Stage 61 — Binance Futures Mark Price and HYPE

- All distance and alert calculations use Binance USD-M Futures Mark Price.
- Spot price is not used as a fallback.
- Futures contracts are discovered through `exchangeInfo` when available.
- Exact `SYMBOLUSDT` matching is attempted first, so HYPE maps to `HYPEUSDT`.
- Missing bulk prices are retried with a direct `premiumIndex?symbol=...` request.
- Multiple official Futures hosts are attempted for resilience.
- A missing contract excludes only that symbol and does not invalidate other priced symbols.
