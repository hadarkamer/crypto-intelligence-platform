# Ordered First Touch v7 research acceptance policy v1

Effective for candidate scopes first frozen under
`captured-question-search-v2-sequence-regime-acceptance`. Earlier freezes are
not changed and retain their original missing-policy result.

## Evidence boundary

- LIVE delivered alerts only; archive evidence remains `ARCHIVE_ONLY` discovery.
- One causally verified BTC parent movement receives one vote per exact formula,
  coin/ALL, direction, threshold, horizon and period view.
- Regular requires at least five resolved prospective waves. FRESH requires at
  least three prospective waves whose complete evidence remains within 14 days.
- SUCCESS and FAILURE alone form the hit-rate denominator. OPEN, AMBIGUOUS,
  NO_TOUCH and DATA_MISSING remain separate.
- Asymmetry uses every status with a complete identical fixed horizon and the
  ratio `sum(wave MFE) / sum(wave MAE)` after conservative within-wave collapse.

## Gates

Both routes must pass:

1. Probability: hit rate at least 70%, and Wilson 95% lower bound at least 40%.
2. Asymmetry: common-window ratio at least 1.50, at least 60% of waves have
   MFE greater than MAE, and median paired `MFE - MAE` is positive.

These values were frozen as a conservative operational research contract, not
selected by optimizing a candidate's outcomes. The fixed search envelope is
at most 300 candidate definitions × (eight supported coins + ALL) × two
directions × four horizons × eight thresholds × two overlapping period views.
Actual attempts and overlap groups are still reported; probabilities are never
multiplied and the two period views are not independent evidence.

Passing means `RELEVANT_RESEARCH` or, for the rolling route,
`FRESH_EARLY_EXPERIMENT`. It is not approval to alert, trade or execute. A new
policy, threshold, candidate definition or family expansion requires a new
version and a later real freeze.
