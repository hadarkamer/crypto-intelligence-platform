# Durable MP65 / V1 formula Watch delivery

## Scope and authorization
The user deferred cluster/CVD contribution research and asked to continue the next plan steps. This implements the separate, already-planned restart-safe transition/delivery contract. It does not change formula V1 eligibility, ordering, thresholds, OI weights, family quality, historical results, catalog, B4, 240m normalization or the earlier Watch cadence fix.

## Behavior
- General Watch MP65 state is durable per hashed recipient subscription and explicit policy `maxpain-score65-reset60-bootstrap-v1`. Signal keys remain symbol, timeframe, and liquidation side. A valid observation at or above 65 activates; below 60 rearms. Missing/nonfinite observations preserve prior state. An unknown baseline is initialized without fabricating a crossing.
- The scope row lock serializes batches. State, source receipt and native/formula delivery intents commit in one transaction. An intent is unique by scope, policy, kind, signal key and episode, independently of Watch ID. Same-ID/same-input replay returns the frozen receipt; changed inputs conflict; older/equal-time different-ID observations cannot mutate state.
- The existing V1 selector still freezes the first both-total-CVD card per symbol before testing the short family. Text, match, decision time, original Watch context and direction are frozen before Telegram.
- Claim one intent immediately before one delivery attempt. Only never-attempted PENDING intents may recover for ten minutes after source observation. Timeout/transport uncertainty becomes UNKNOWN with no automatic retry. Definite Telegram rejection becomes FAILED. Process-crashed IN_FLIGHT is settled UNKNOWN after two minutes. An acknowledged delivery whose DB acknowledgement fails remains uncertain in durable transport state but research retains the actual known acknowledgement; it is not resent.
- Eligibility is rechecked after the awaited claim. A reservation that has not reached the network is token-conditionally released if the subscription stopped, the recipient changed, or an idle-recovery scan began. No attempted message can use this release path.
- The existing supervisor drains eligible pending work only for an active general subscription while no scan is active. The existing scheduled Watch drains after regular/special cards. No new collector, scheduler, source request or manual test Telegram message is introduced.
- The precomputed Watch-card path no longer invokes legacy special transitions again. Native research capture uses durable intents and original lineage rather than mirroring MP65 RAM state. Other transition families remain on their previous implementation.
- Persistence failure preserves ordinary Watch operation and is visible as an evidence gap. Public health includes transition readiness, counters and a separate per-cycle transition result. Watch reports partial completion when this path is incomplete.

## Storage and retention
Three dedicated PostgreSQL tables store scope state, replay receipts and delivery intents. No production SQLite fallback. Connections have bounded connect/statement/lock timeouts; schema initialization is done at startup with a serialized DDL lock and bounded retry. Receipt cleanup keeps 48 hours; terminal-intent cleanup keeps 30 days, at most 128 rows of each per recorded cycle. Scope state and source watermark are retained, so purging a receipt cannot recreate an older crossing. UNKNOWN/FAILED/EXPIRED are never requeued automatically.

## Verification
Focused local tests cover native card bypass, the original V1 first-card rule, direction/source lineage, cold bootstrap, rearm, unknown observations, cancellation, stop-during-claim, timeout/rejection, acknowledgement-storage failure, no resend and persistence failure without stopping ordinary Watch. The existing formula, scheduling and full project suites are run before merge. PostgreSQL tests run only against a disposable localhost/CI test database, including competing transactions, real rollback, restart/replay, claim uniqueness, expiry/orphan settlement, release tokens and bounded retention.

## Deployment evidence
Record exact CI/merge/deploy and natural Watch evidence in the continuation checkpoint. Do not manufacture a production scan or signal to exercise this feature. The first observation under this new state policy is a bootstrap, so a pre-existing score above 65 alone must not send MP65/formula.
