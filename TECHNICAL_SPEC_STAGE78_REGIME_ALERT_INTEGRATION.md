# Stage 78 — Price+OI Regime in primary alerts

## Purpose
Use the already-computed Price+OI Regime as contextual confirmation/contradiction in primary alerts, without changing the existing Max Pain score.

## Overall aggregation correction
Each available window is a vote, including `NEUTRAL_INCONCLUSIVE`.

- Neutral in >=3 available windows => Overall Neutral/Inconclusive.
- Any single directional state in >=3 windows => that state is confirmed.
- Otherwise, if directional evidence exists but no state reaches 3 => Mixed/Transition.
- `agreement` is the number of windows supporting the returned conclusion; denominator remains the number of available windows (normally 5).

This fixes the case 4 Neutral + 1 Short Covering: it is now Neutral/Inconclusive 4/5, not Mixed/Transition 1/5.

## Primary alerts
Primary `/alerts` and Watch opportunities already pass through `_build_opportunities_with_regime()`, which attaches:

- `market_regime`
- `composite_conclusion`

The alert card renders the Price+OI Regime block and the combined conclusion. Stage 78 keeps this integration and corrects the Overall conclusion feeding it.

## Scoring isolation
Regime remains contextual only. It does not add/subtract points and does not alter Max Pain, Consensus, BTC confirmation, Cluster, Gap, reverse score, or Watch eligibility.
