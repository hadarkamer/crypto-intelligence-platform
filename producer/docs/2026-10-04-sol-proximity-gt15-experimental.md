# SOL MaxPain proximity >15 — prospective experimental notifications

User-authorized on 2026-10-04. Rule ID `SOL_MAXPAIN_PROXIMITY_GT15`.
This is an isolated Telegram notification and research-monitor lifecycle. It does
not create trade intents, send orders, or invoke the Hyperliquid forwarder.
Existing HYPE ROW71205, XRP R2732 and U21 calculations are unchanged.

## Frozen formula

The source is every SOL target in the existing shared Watch operational-score
bundle, before display thresholds or MaxPain total-score filtering. No additional
DOM, CoinGlass, CVD or OI collection is introduced. The captured, two-decimal
proximity component must be **strictly greater than 15**. A score of exactly 15
is rejected. No cluster, CVD, OI, consensus or total-score threshold is required.

For the accepted observation, source price S and target T remain frozen.
Let signed distance be T-S:

- Limit-entry level: S - 2 × (T-S).
- Original stop: S - 5 × (T-S).
- Take: T itself.
- Arm at the next full minute after the source generation is locally available,
  validated and its pre-intake price guard is complete.
- Pending deadline: arm + 24 hours; the deadline minute cannot fill.
- After a fill: no time limit, no partial take and no promoted stop.

Example: S=100, T=102 gives a long at 96, stop 90, take 102. The mirrored short
with T=98 enters at 104, stop 110 and take 98. A favorable gap can improve the
observed fill to the candle open while frozen stop and take remain unchanged.
Gap-through-stop cannot produce a valid fill.

The historical base recipe is `MAXPAIN_EXTRA_d07e848aaf65`, with the explicit
user change from >=15 to >15. Strict >15 and original >=15 happened to produce
the same 88 historical accepted closed SOL trades in the recovered baseline;
that does not make the predicates equivalent for future data.

## Independent targets and lifecycle

Multiple pending/open plans are allowed. A new plan is blocked when its target
is within 0.2% inclusive of any pending/open/unknown plan, using the smaller
of the two targets as denominator. The comparison spans both directions.
**There is no liquidity-growth exception in this particular recipe.**

Identical exact numeric targets across timeframes and liquidated sides share an
episode. Only a complete valid SOL snapshot containing all seven timeframes
(12h, 24h, 48h, 3d, 1w, 2w, 1m) and both positive target values can prove absence.
Partial/malformed snapshots cannot reset an episode. After proved absence and
reappearance, a target can be a new episode, even if its numeric price recurs.
Disappearance never cancels an already accepted pending/open plan, which still
reserves its nearby price area.

Every target is monitored, including those failing the score threshold or blocked
by another plan. A touched target is consumed immediately. One observed fill
also consumes that episode, even if the position later stops out. An untouched
expired pending plan can qualify again at a later observation with a new S.
An unchanged target observed at least one hour after confirmed touch is marked
suspicious diagnostically; earlier valid observations are not relabeled.

## Safe activation, timing and uncertainty

The first source universe after activation is left-censored: targets already
present are marked `bootstrap_unverified` and cannot generate entries. Complete
absence followed by reappearance allows them to qualify. Targets not present in
the first complete universe can qualify when newly observed afterwards. No
historical candidates or fill messages are replayed at activation/restart.

SOL monitoring uses Binance Spot SOLUSDT **trade candles of one minute**, through
the existing canonical market-data endpoint and reusable minute-cache class.
The source-minute partial bar and current unfinished bar may only veto a stale
candidate or notification; they cannot establish a closed-bar fill or exit.
Guard windows must be contiguous through the actual intake minute. Including
the whole source partial minute is conservative and can reject a target whose
touch preceded its exact observation inside that minute.

Entry and exit evidence is based on closed bars. Where both entry and target
occur in one bar and the open does not establish ordering, state is UNKNOWN,
capacity remains reserved and no fill is announced. Simultaneous stop/take
without an ordering proven by the open is handled the same way. A live source
window touching a pending target consumes the episode immediately and retains
the pending plan until its closed bar resolves the order; a proven pre-arm
touch cancels the pending plan. Unknowns never silently reopen a nearby target.

The Telegram message is sent only after a fresh closed-bar entry touch, when
neither stop nor take has subsequently been observed. It explicitly describes an
observed reference touch, **not an exchange fill or a guaranteed current entry
price**. It is not an advance pending-order recommendation. Notifications expire
90 seconds after the fill candle closes. Exposure continues to be monitored even
when notification expires or delivery is uncertain.

## Persistence, limits and recovery

State is stored in the existing `bot_settings` table, under the dedicated
`sol-proximity-notification-store-v1` namespace. There is no DDL or migration in
the recurring worker. Transaction-scoped advisory locks and a row lock serialize
writers. Configuration hashes prevent an accidental reset under changed rules.
A durable outbox claims each send once. Uncertain Telegram responses and orphaned
in-flight sends become UNKNOWN and are never resent; they retain the tracked
position. Acknowledgement must be persisted.

Closed-price monitoring is bounded to one request of at most 1,000 minutes per
worker minute; cached data is reused. A shared Watch generation also has a bounded
source-window guard, and each attempted notification has a small current-price
veto request. HTTP, malformed-price and database evidence errors use five-minute
backoff. Missing price evidence prevents new source admission while recovery is
incomplete. No HYPE fallback or alternative SOL venue is silently substituted.

The state implementation has a fail-closed safety ceiling of 256 pending/open/
unknown plans and 4,096 tracked episodes, plus a 768 KiB serialized-state bound.
These are operational capacity protections, not optimized research parameters.
Counts and `operational_state_capacity` expose the distinction. The recovered
baseline peak was 14 concurrent filled positions.

Research used $5 risk and a $5,000 notional ceiling. This price-level notification
has no position sizing, notional cap, margin, account or execution instruction.
Its status explicitly reports `notification_only=true`,
`live_order_execution=false` and `position_notional_cap=null`.

## Verification

`python3 sol_proximity_experimental_selftest.py` exercises strict rounded scoring,
geometry and deadline, source identity and clocks, complete/partial bootstrap and
absence, repeated targets, inclusive proximity, two-phase multi-timeframe guards,
directional gaps, fill/stop consumption, ambiguity, stale/outage handling,
transaction rollback, restart fencing, at-most-once delivery and request backoff.
Existing HYPE, XRP and alert-selection regression suites are also run. Tests are
offline and do not post messages or access a live exchange.
