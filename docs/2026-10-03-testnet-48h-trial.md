# Finite multi-entry Testnet trial

The operator explicitly requested multiple simultaneous demo trades for the two
existing accounts. The original one-per-role trial remained consumed; its epoch
was still the old value in the running deployment. A begun but definitely-unsent
request consumes that cap. Neither restart nor repairing latency resets it.

`HL_TESTNET_ENTRY_ATTEMPT_CAP=five_per_role_48h_v1` permits at most five begun
ENTRY attempts per role/account from that role's existing `NOT_BEFORE` epoch,
for exactly 48 hours. Both epochs are set once when enabling the trial. The
start is inclusive and deadline exclusive, using PostgreSQL wall time at the
early, final transaction and pre-send gates. Existing request rows count every
begun outcome, including certified unsent, rejection and uncertainty. There is
no new schema, counter reset, retry or renewal loop. An unknown entry blocks
another entry for that account until reconciled. The common global and bucket
locks serialize the count and durable begin; only the exact positively
committed own request may pass the post-begin gate.

This is a cap on attempts, not a guarantee of five fills or five concurrent
positions. Existing source freshness, signal selection, target overlap, shared
coin balance allocation, account direction, per-trade risk, partial-fill
protections, request budget and emergency fences remain intact. Several
positions can coexist when their eligible signals and those fences permit it.
No synthetic exchange orders or historical alert replays are introduced.

Expiry or cap exhaustion stops only new entries. Exit protection, cancellation,
reconciliation and emergency reduction retain their existing authority. Existing
positions continue to be managed after the trial ends. Restart does not change
the epochs or count. The prior `one_per_role_v1` mode remains supported unchanged.

CI verifies real PostgreSQL cap races, restart, unsent consumption, independent
accounts, exact own-send admission, future/expired windows and the entire
existing protection regression. A real fresh entry/fill/STOP/TP/exit remains
necessary to demonstrate end-to-end timing and parallel operation in Testnet;
passing CI alone does not prove that observation.
