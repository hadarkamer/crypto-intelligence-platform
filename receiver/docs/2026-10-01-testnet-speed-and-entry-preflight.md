# Testnet speed deployment and next pre-entry guards — 1 October 2026

## Deployed response improvement

PR #105 was merged as `e64787f5442f56cc44d8ebec67375275fa3a2498` and deployed
to the existing Testnet service. Render deploy `dep-dauokgu0tbcc73c3a220` reached
live at 2026-09-30T22:17:29Z (1 October, 01:17:29 Israel). The service process
reported the matching commit and the parallel-reader option. Both entry flags
were still false. The alert-producing service was not changed.

Two serial and two parallel full evidence collections used the existing SHORT
DOGE bucket, including three historical cards and 24 public reads per run.
SQL used a read-only transaction. These observations were not written back to
the bot, and no signature or trading request was made by the audit.

| Order | Reader | Full collection milliseconds | Public reads |
| --- | --- | ---: | ---: |
| 1 | serial | 6476.17 | 24 |
| 2 | parallel | 2854.80 | 24 |
| 3 | parallel | 4970.76 | 24 |
| 4 | serial | 4635.38 | 24 |

Median: 5555.78 ms serial, 3912.78 ms parallel, approximately 29.6% lower.
This small variable-latency sample does not establish statistical certainty or
the two-second goal. It measures existing-position evidence collection, not
time from a new fill to creating and verifying a new stop. All four collections
returned the same lifecycle states, no card/bucket issues and remaining DOGE
SHORT quantity 6016. A new trade was not opened for measurement.

The rolling deployment briefly caused an old/new worker revision conflict.
The durable revision guard rejected stale updates and the new worker resumed
normal maintenance with zero trading requests; no guard was bypassed.

## Read-only pre-entry audit and implementation

The live Testnet audit verified both local key-to-agent identities and both
exchange agent-to-account mappings. The journal domain/version matched Testnet;
there were no unresolved requests. Database time was within the two-millisecond
host measurement window. This is an observation, not continuous clock monitoring.
Observed address action counters were:

| Account role | Cap | Used | Surplus | Remaining observed |
| --- | ---: | ---: | ---: | ---: |
| LONG | 24113 | 58 | 0 | 24055 |
| SHORT | 17620 | 33 | 0 | 17587 |

Existing guards already cover fixed Testnet host/signing domain, role and agent
mapping, canonical SDK wire serialization, durable ownership/revision checks,
source expiry, immutable prices/size precision, owned-market reconciliation,
existing leverage and conservative exchange capacity/margin checks.

Two missing guards are added at the actual entry authorization boundary:

1. Require a verified `userRateLimit` response with at least five remaining
   address actions: entry, stop, take, remainder cancellation and one close.
   Compute cap - used + surplus from documented net counters; malformed,
   inconsistent or insufficient counters block entry. The guard neither buys nor
   reserves capacity, covers all future partial-fill/retry actions, nor establishes
   IP-rate headroom. It must never gate protection, cancellation or closing work.
2. Check the unchanged 15-second evidence age before account checks and again
   after budget/allowance reads. Recheck source and operator approval at that
   same final boundary. Stale preflight results fail before durable reservation
   or beginning an attempt; existing final checks around signing remain in place.

Eleven new offline tests cover allowance schema/boundaries, fixed Testnet info
transport, both entry roles, stale/future evidence, slow reads, original source
and approval expiry, the no-reservation boundary and unaffected exit paths.

Official endpoint and capacity semantics consulted:
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint#query-user-rate-limits
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits

## Remaining scope

These guards do not complete Mainnet readiness. Still required are configured
Mainnet accounts/agents and separate journal, explicitly valued capital/loss/
aggregate exposure limits including costs, portfolio/liquidation headroom,
durable cross-market reservations, shared IP request budgeting, continuous clock
supervision and the later protection/repair/emergency/continuity stages. The
mandatory automatic emergency close is not implemented by this change.

Entries stay disabled; neither this audit nor these code changes activates Mainnet.
