# Protection response and required safety stages — 1 October 2026

## First implementation

The Testnet dispatch reader now overlaps independent public reads within each
observation using at most four workers. Order statuses, paginated fill history,
open-order inventory and account position retain their existing validation.
The two complete observations remain sequential and must agree before durable
evidence can be saved or protective work selected. Requests and database writes
remain serial; uncertain outcomes still prohibit blind resending.

The 200-call budget is reserved under a lock. Every started worker is joined,
and queued work is canceled on failure. The independently weighted read-only
monitor retains its serial reader. No entry flag, strategy price, quantity,
signing domain or network destination changes.

Synthetic measurement: five runs with a constant 20 ms delay per public call,
one open card and six calls per observation. Median complete collection:
242.60 ms serial versus 82.63 ms parallel (65.9% reduction), with the same
12 public requests. These are simulator results, not exchange timings or proof
of meeting a live two-second target. The measured prior live stop delay was
7.254 seconds; a new live result has not been obtained by this change.

Six new tests cover four-worker overlap, sequential verification passes,
inconsistent second observations, joined workers after read failures, missing
fills, the shared call budget, and preserving serial weighted monitoring.
Existing PostgreSQL restart, lost-reply and execution guards remain release
checks. No additional trade is needed to establish software correctness.

## Required stages, in execution order

1. **Response:** reduce evidence-collection waits while retaining all ownership,
   fill-history, consistency and freshness checks; measure actual protection
   timing when an independently authorized trial is available.
2. **Before entry:** verify network, accounts and agents, signing serialization,
   durable journal and clock; validate symbol, prices, sizes, source expiry,
   margin, rate headroom, ownership and configured capital limits. Unknown or
   stale state blocks new exposure. Mainnet requires its own adapter and journal.
3. **Entry:** immutable limit price, durable action identity and reservation;
   prevent duplicate sends, reject stale alerts, cancel unfilled escaped-price
   orders under their original rule and cancel expired remainders.
4. **Fill:** use verified exchange fills, including partial fills, and start the
   protection clock from exchange fill time. Late discovery does not restart it.
5. **Protection:** create and verify the original reduce-only stop first, then
   take-profit; resize for confirmed remaining quantity without discarding a
   working stop first. Additional fills do not extend older uncovered deadlines.
6. **Repair:** distinguish rejection from lost reply; look up the original action,
   verify whether an order exists, classify failures and retry only a corrected,
   proven terminal rejection inside the original protection deadline.
7. **Emergency:** mandatory automatic close requirement accepted by the user.
   Trigger on an unprotected crossed stop or protection deadline. Disable new
   entries, cancel the remaining entry and close owned exposure using reduce-only
   requests; an unknown stop outcome must not indefinitely block this safety path.
   Reconcile each close result and continue for verified residual quantity;
   handle late entry fills and late stop creation until final cleanup.
8. **Closure:** require fills and reconciled remaining position, not a sent order
   or a price touch. Retire owned residual orders and report each phase once.
9. **Continuity:** durable timers and identities across restart, concurrency and
   duplicate-close guards, and a supervisor that can act if the entry worker
   fails. Exchange unavailability or invalid permissions may prevent closing;
   keep new exposure blocked and surface the unresolved risk.
10. **Release:** fault-injection and PostgreSQL tests, Testnet verification, then
    a separately authorized bounded Mainnet trial and verification through exit.

The proposed 2-second verification target and 5-second emergency deadline are
initial policy recommendations, not exchange guarantees or deployed timers.
Mainnet capital limits and emergency slippage limits still require explicit
values and enforcement; the mandatory close requirement does not choose them.
Emergency execution, a separate supervisor and Mainnet activation are not
implemented by this public-read timing change.
