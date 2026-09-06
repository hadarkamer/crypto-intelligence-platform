# Causal BTC parent movements

`btc-parent-close-reversal-200bps-v1` is a new conservative engineering policy.
The user required shared BTC market-wave identities but did not specify a
numerical wave detector. Its 200bps reversal width equals the largest researched
target; this choice has not been optimized or validated as a trading signal.

- Input is only closed Binance Spot BTCUSDT one-minute candles.
- An upward leg continues until a close falls at least 2% from its highest
  observed close. A downward leg continues until a close rises at least 2%
  from its lowest observed close.
- The confirming close starts the new parent. The historical pivot is never
  retrospectively declared the boundary. Repeated alerts, other coins,
  directions, formulas and outcome horizons cannot create another parent.
- The initial incomplete leg is ineligible. Initialization first observes a
  2% directional move from its first close, then a complete opposite reversal
  establishes the first eligible boundary.
- A missing minute closes the previous segment and starts an ineligible
  segment. Events inside the uncovered interval are `BTC_DATA_MISSING`.
  Complete later reversal evidence restores eligibility without a fixed wait.
- Membership requires the latest closed BTC bar at the decision time to be
  less than one minute old. Future bars cannot supply historical membership.
- There is no 24-hour/72-hour separation, dwell, survival or return-to-entry
  gate. Separate parents are an operational evidence grouping, not a guarantee
  of statistical independence.

Migration `021_btc_parent_movements_v1.sql` adds the source archive, global
parents/checkpoints and event membership. Source, movement and event records
remain separate from immutable alerts and their First Touch outcomes. SQL
constraints and a validation trigger reject false LIVE eligibility, wrong event
times, source-free membership and future observed closes.

The worker bootstraps from one day before the earliest delivered alert in the
last 14 days, or an explicit `RESEARCH_BTC_EPISODE_START_UTC`. The first checkpoint
freezes that inception across restarts. It processes 1,000 source minutes and
500 event assignments per pass, every 10 seconds while catching up and every
60 seconds after reaching the latest closed minute. A session advisory lock
prevents concurrent workers from creating competing parents. Set
`RESEARCH_BTC_EPISODES_ENABLED` explicitly to override its default, which follows
`RESEARCH_OUTCOME_ENRICHMENT_ENABLED`.

Every delivered alert receives membership independently of label completion.
This prevents a later winning repeat from replacing an earlier match whose
price outcomes have not arrived. Prospective samples currently use v7 admission
and are not formula-match evidence in the new alert-only candidate summary.
Formula summaries must also verify that membership has caught up before selecting
the first matching decision of a wave.

Data gaps are retained as uncertainty under this policy even if an exchange
later revises its history. Existing source bars and assigned identities are not
silently rewritten; historical repairs or a different reversal width require a
new version and an explicit replay. A zero-row source response is retried without
advancing the cursor. Source gaps can reduce usable evidence. Events preceding
bootstrap inception are outside this generation's coverage.
