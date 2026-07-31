# Stage 89 — Market Evidence Integration

## Purpose

Stage 89 combines three independent evidence families without changing any
existing Max-Pain calculation:

1. Positioning: Price + Open Interest.
2. Futures Flow: Futures Buy/Sell + CVD as one family.
3. Spot Flow: Spot Buy/Sell + CVD as one family.

The output is a read-only **Market Evidence Alignment** from -100 to +100.
It is not a probability, does not size positions and does not change Alerts,
Watch, ranking, Max-Pain Score or LONG/SHORT selection.

## Expected price direction

The displayed Max-Pain side identifies the side expected to be hurt:

- Max-Pain `LONG` → implied price direction `SHORT`.
- Max-Pain `SHORT` → implied price direction `LONG`.

## Weights

- Positioning: 40 points.
- Futures Flow: 35 points.
- Spot Flow: 25 points.

Buy/Sell and CVD are never counted separately. They have already been merged
inside each Flow family.

## Positioning strength

- Bullish/Bearish Build-up: factor 1.00.
- Short Covering/Long Unwinding: factor 0.65.
- Mixed/Neutral: factor 0.
- Agreement factor: `agreement / 5`.
- WARNING quality factor: 0.75; INVALID: unavailable.

## Flow strength

- CONFIRMED: factor 1.00.
- EVIDENCE: factor 0.70.
- EARLY: factor 0.40.
- NEUTRAL/MIXED: factor 0.
- WARNING quality factor: 0.60.

## Module contribution

`Contribution = Weight × Strength × Relation`

Relation is +1 when the module supports the expected price direction, -1 when
it contradicts it and 0 when neutral.

`Alignment = sum(Module Contributions)` clamped to `[-100,+100]`.

## Classification

- +70 to +100: Strong support.
- +35 to +69.99: Support.
- +0.01 to +34.99: Weak support.
- 0: Neutral / mixed.
- -0.01 to -34.99: Weak conflict.
- -35 to -69.99: Conflict.
- -70 to -100: Strong conflict.

## Integration

- `/market_state SYMBOL [LONG|SHORT]` shows the three modules. LONG/SHORT in
  this command refers directly to expected **price direction**.
- Alerts and Watch show a compact Market Evidence block automatically.
- Existing score, sorting and trade logic remain unchanged.
