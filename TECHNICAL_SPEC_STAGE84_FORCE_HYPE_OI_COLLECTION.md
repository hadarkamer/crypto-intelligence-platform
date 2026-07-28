# Stage 84 — Force HYPE into Price+OI collection

## Problem
HYPE could be absent from the latest saved Max Pain snapshot. The live Price+OI collector built its symbol list only from that snapshot, so HYPE was never passed to the existing HYPE price fallbacks or CoinGlass OI reader.

## Change
The collector now always adds `HYPE` to the active symbol set before fetching prices and OI.

## Expected logs
A successful collection should now include a HYPE price-source log and one of:

- `[oi-regime] HYPE OI source=coinglass_all ...`
- `[oi-regime] HYPE OI source=coinglass_exchange_sum ...`

## Scope
No Max Pain scoring, alert thresholds, or other symbol-selection logic was changed.
