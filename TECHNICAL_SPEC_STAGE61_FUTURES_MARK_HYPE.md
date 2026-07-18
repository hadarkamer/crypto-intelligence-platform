# Stage 61 — Binance Futures Mark Price and dynamic symbol discovery

## Canonical price source
All Max Pain distance calculations use Binance USD-M Futures **Mark Price**.
Spot price is no longer used as a primary source or fallback.

## Symbol discovery
The price provider loads `/fapi/v1/exchangeInfo` and resolves each CoinGlass
symbol against active USDT perpetual contracts. It no longer relies solely on
constructing `SYMBOLUSDT`.

This supports contracts such as HYPE automatically when Binance exposes
`baseAsset=HYPE` and a USDT perpetual symbol (normally `HYPEUSDT`).

## Fallbacks and diagnostics
1. Resolve explicit override, exact pair, then exchangeInfo base-asset matches.
2. Read the bulk `/fapi/v1/premiumIndex` Mark Price response.
3. If a resolved pair is absent from the bulk response, query that pair directly.
4. Record candidate pairs and match status in `symbol_diagnostics`.
5. Symbols with no valid Futures Mark Price remain excluded rather than silently
   using a Spot price.
