# Stage 48 — Minimum Tradable Distance

## Rule
Every opportunity is still scored internally.

Before Telegram output in `/alerts` or Watch:

`distance_pct >= MIN_DISPLAY_DISTANCE_PCT`

Default:

`MIN_DISPLAY_DISTANCE_PCT=0.15`

An opportunity below the threshold is treated as a target that is already
effectively reached and is not presented as a new trade opportunity.

## Scope
Changed:
- `/alerts` display selection
- Watch display selection
- Watch best-result fallback

Unchanged:
- scoring formulas
- sorting and all-timeframe averages
- `/collect`
- `/coin`
- database rows
