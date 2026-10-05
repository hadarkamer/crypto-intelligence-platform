# Retained history admission under live Testnet load

Base: `8e32ff1bd9d9faa7cee0493ddc24272330598b73`, the deployed
`paper-trading-v1` revision. Runtime logs show LONG SOL history reservations of
480 weight rejected with 460 already used under the 800 background ceiling.
The live SHORT SOL position continues periodic protected reconciliation.

The previous entry/circuit gates, verified incident archival, idle candidate
suppression, exact alert diagnostics and independent exit maintenance remain.
There are no strategy, original-price, risk, account routing, credential,
quota, evidence-age, schema or Mainnet changes.

The old retained-history collector reserved BOTH maximum-sized anchor checks
and BOTH history passes before its first read. Live protection consumes real
ledger weight, so reserving 480 (524 for final inventory) at once can starve
background recovery despite small actual responses.

Retained recovery now funds three read-only phases: opening retention anchor,
the complete two-pass history/inventory plan, closing retention anchor. Every
HTTP still uses an exact, single-use, pre-funded permit. History splitting
still funds both children before either is sent. The ordinary collector keeps
its complete-plan admission. Response-size refunds are required to free proven
excess only; failed refunds retain the original charge.

This reduces the largest recovery reservation to 240 for an ordinary chunk,
or 284 for final history/inventory, without eliminating a read or raising the
800/1200 ceilings. No partial observation is committed. Failure in any phase,
changed history, missing anchors, unknown activity, feed changes, an expired
15-second observation, changed original facts or a failed durable CAS retains
the old evidence and the account-wide staged-history ENTRY fence. The final
actual two-pass inventory and its guarded commit are still required.

The safety supervisor can encounter a concurrent normal checkpoint after its
own read. Previously this always marked the pass failed, even when the newer
durable state already proved full current STOP coverage. It now omits further
work only from that freshly loaded complete STOP proof under the existing
five-second bound and notification guards. No stale, uncovered, pending,
dirty-feed or incident state can use this path; it sends no order and retries
no request. The final action boundary remains under the market lane.

Review scope: request budget tickets/claims/refunds, retained collection and
pagination, staged-history cursor and account fence, dispatch observation
sharing and CAS, emergency refresh/action boundary, stream notification gates,
pending requests, startup/restart and Testnet account separation.

Validation includes failures in each admission phase, the losing-reader race,
stale/missing STOP proof, unresolved requests and dirty feeds. PostgreSQL tests
exercise actual admission/refunds at the observed 460-weight load, actual gap
progress and final completion, preserved 400-weight protection reserve and
failed refunds. The complete runtime and executor/forwarder suites are also
required before deployment. Live recovery and exchange trade outcomes must be
reported separately from software test results.
