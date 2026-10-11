# Coordinate Testnet verification reporting

The live Testnet branch already refreshes exchange evidence when its three-minute
audit becomes due. Reporting could inspect the old journal checkpoint while a
peer was refreshing it. Its fifteen-second lifecycle freshness check then emitted
`STALE_OR_FUTURE_SNAPSHOT`. The lifetime set of previously reported states also
suppressed later returns to an already reported healthy state.

Reporting now reloads a stale journal row once to pick up a peer's committed
checkpoint. An exact running observation of a previously fully protected position
can be reported as `IN_PROGRESS`, with `protection_verified=false`, the original
evidence time/age, and a separate `last_known_protection_verified` field. This
hint performs no exchange calls, waits, order authorizations, or journal changes.

The grace lasts less than fifteen seconds from the oldest matching running read
and ends no later than seventeen seconds after the original audit deadline.
Failed reads, pending order requests, missing coverage, future timestamps,
history gaps, changed revisions and other market reads cannot grant this hint.
A stalled read retains its stale issue and becomes `OVERDUE`; overlapping or
replacement readers cannot extend the original audit deadline.

Log deduplication now compares each account/card with its immediately preceding
reported state. Healthy -> pending/overdue -> healthy transitions are emitted
each time, while consecutive duplicates remain suppressed. Historical reporting
continues to label original observation evidence and requires no live venue.

Eight offline regressions cover real threaded refreshes and recovery in both
accounts, stalled/replaced readers, monotonic clock expiry, missing exits,
failed/pending/future/mismatched evidence, peer commits, account isolation and
repeated failure/recovery reporting. Required full PostgreSQL CI must pass before
deployment. Prices, quantities, source/trial windows, strategy, exchange request
budget, three-minute audit schedule and execution/safety guards are unchanged.
