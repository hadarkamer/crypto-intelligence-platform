# Stage 83 — HYPE aggregated OI diagnostics and composite conclusion fix

## OI collection
- Prefer CoinGlass `exchange=All` when supplied.
- If `All` is absent, sum all positive per-exchange `open_interest_usd` rows returned by CoinGlass.
- Never combine `All` with individual exchanges.
- Log the selected source and exchange list.
- Log per-symbol collection failures.
- No single-exchange Hyperliquid OI fallback is introduced.

## Composite conclusion
- Max Pain labels remain unchanged and still identify the side expected to be hurt.
- The implied price direction is the inverse of that label.
- Long Unwinding and Short Covering no longer return early.
- Every directional regime now reaches the same `תומך/מנוגד` comparison.
