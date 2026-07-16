# Stage 57 — Direction-scoped consensus and clusters

## Changes

- Consensus points are calculated from the number of timeframes supporting the direction of the current alert.
- LONG and SHORT clusters are calculated independently.
- Cluster membership cannot exceed the number of timeframes supporting the alert direction.
- Duplicate symbol/timeframe rows are removed defensively before scoring.
- Every opportunity validates that component totals equal the final score and that consensus/cluster counts are internally consistent.
- Calculation failures are surfaced as a critical data-quality warning.
- No hidden QA command was added.

## Automated checks

Run:

```bash
python -m unittest discover -s tests -v
```
