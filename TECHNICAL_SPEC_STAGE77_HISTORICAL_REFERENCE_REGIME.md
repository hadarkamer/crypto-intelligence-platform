# Stage 77 — Historical Price/OI Reference applied to live Regime

## Scope
This stage keeps Max-Pain scoring, LONG/SHORT selection, Alerts and Watch scoring unchanged.
Historical CoinGlass Price+OI backfill is used only to decide whether a live Price/OI movement is meaningful for the same symbol and the same analytical window.

## Historical set
Symbols: BTC, ETH, SOL, HYPE, DOGE, ZEC, BNB, XRP.
Windows: 30m, 1h, 4h, 12h, 24h.
Backfill: 30 days of 30m matched Price + aggregated OI candles.

## Change formulas
Price change (%) = ((Price_now - Price_reference) / Price_reference) * 100
OI change (%) = ((OI_now - OI_reference) / OI_reference) * 100

Historical distributions use the absolute magnitude of each change.

## Minimum valid movement
P25 is the minimum valid movement, separately for:
- symbol
- timeframe
- Price vs OI

A directional Price+OI state is confirmed only when BOTH absolute Price change and absolute OI change are at or above their own P25.
If one or both are below P25, state = Neutral / Inconclusive.

## Strength bands
- < P25: Weak / Noise
- P25 to < P75: Normal
- P75 to < P90: Elevated
- P90 to < P95: Strong
- >= P95: Extreme

Price and OI strengths are never averaged. They are displayed separately.

## Directional states after minimum validation
- Price > 0, OI > 0: Bullish Build-up
- Price < 0, OI > 0: Bearish Build-up
- Price > 0, OI < 0: Short Covering
- Price < 0, OI < 0: Long Unwinding
- insufficient movement: Neutral / Inconclusive

## Secondary observations
If Price is Weak/Noise but OI is Elevated/Strong/Extreme, the bot preserves Neutral and surfaces an observation that OI moved materially without confirmed Price movement.
If OI is Weak/Noise but Price is Elevated/Strong/Extreme, the bot preserves Neutral and surfaces an observation that Price moved materially without OI confirmation.

## Overall / Early Transition
Existing 5-window majority logic remains unchanged. Neutral windows do not vote for a directional state.
Early Transition logic remains unchanged and operates only on valid directional windows.

## Compatibility / fallback
For symbols without historical backfill/reference, the previous Stage 77 sign-only classifier is preserved. This prevents the new reference layer from breaking existing symbols outside the eight-symbol research set.

## Max Pain isolation
No historical-reference value changes:
- Max Pain score
- Consensus
- BTC confirmation
- Cluster
- Gap
- Directional averages
- LONG/SHORT selection
- alert priority
