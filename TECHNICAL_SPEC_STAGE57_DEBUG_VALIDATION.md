# Stage 53 — Scoring and Cluster Corrections

## Score distribution

- Directional Alignment: 30
- Target Proximity: 25
- Cluster Confidence: 30
- Relative Gap: 15
- Total: 100

## Target Proximity

- below 0.5%: 0 / excluded from alert display
- 0.5% to below 0.7%: 17
- 0.7% to 1.3%: 25
- above 1.3% to 2.0%: 20
- above 2.0% and within the coin dynamic threshold: 15
- beyond the coin dynamic threshold: 0

## Cluster Confidence

Duplicate rows are deduplicated by timeframe, so the count cannot exceed the seven canonical timeframes.

Cluster Confidence is calculated as:

`(target density points + timeframe coverage points) * liquidity accumulation multiplier`

Liquidity accumulation is not added as a separate point block.
The multiplier ranges from 0.0 to 1.5, and the final Cluster Confidence is capped at 30.

## Alert display

Each liquidity side below $500,000 is marked with a red indicator.
