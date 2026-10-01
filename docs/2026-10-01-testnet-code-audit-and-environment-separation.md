# Testnet safety audit and deferred account separation

## Scope

Preserve the current two Testnet accounts, immutable source prices and expiry,
single-attempt request identity, ownership reconciliation, partial-fill coverage,
reduce-only exits, risk/capacity checks and emergency entry latch. Continuous
entries remain disabled. Runtime patches do not enable continuous entries or
mainnet connections, credentials or account configuration. Explicitly authorized,
limited Testnet trials and their actual execution evidence are documented below.

## Concrete corrections

1. Accept the exact approved U21 cancellation policy in protection planning.
   Previously a registered U21 card could be rejected as an unexpected shape
   after a fill, blocking its original STOP and TAKE_PROFIT selection.
2. Release the normal controller's local market lane during public information
   reads and authorization. The emergency controller can make an independent
   observation while a normal read stalls. Reacquire the lane and compare the
   exact revision before checkpoint, planning and attempt; retain database CAS,
   durable request identity, account ownership and actual-send fences.
   Live verification exposed a liveness issue: the supervisor could advance
   revisions during every normal read/precheck. Normal preparation discards its
   losing collection and may use the latest already-persisted fresh complete
   checkpoint. Planning rechecks pending work against that authoritative state;
   reservation and actual attempt still require an exact revision. This is
   checkpoint cooperation, never a retry of an order transmission.
3. Separate the 90-second permission to submit one new entry from maintenance
   of that entry. Observe until protection or verified finality, with the original
   source expiry plus 15 seconds of reconciliation grace and a ten-minute cap.
   Neither maintenance time nor this grace extends permission to open a trade.
4. Report filled/remaining quantities, working orders, unresolved request identity
   and lifecycle. Distinguish protected with measured venue activation time,
   protected with unknown venue time, verified terminal cancellation/closure and
   incomplete reconciliation. A result does not claim a verified handoff to the
   deployed worker. Signal and join the trial's own supervisor; failed shutdown
   prevents a complete result.
5. Retire an expired ENTRY reservation only when the transaction proves it was
   never attempted: exact PREPARED, integer zero attempts, no nonce, attempt time,
   reply or observed order ID. Require the immutable original source expiry,
   fresh flat/final public evidence and exact owner/action/evidence revalidation.
   An attempted or outcome-unknown request remains a barrier.
6. Name public observation timing honestly. Venue activation time remains
   independent. Public observation occurs before commit; it is not an exact
   database-save measurement. The trial additionally reports a successful
   durable-record-read upper bound for save latency, explicitly named as such.
   Existing records retain their historical metric names and are not rewritten.

Software fault injection proves these boundaries, not live venue performance.
A five-second emergency trigger is a configured decision threshold, not a
guaranteed exchange execution deadline. Exchange information/transport failures
can still require reconciliation, without blind retries or invented finality.

## Verified release and evidence reuse

PR [111](https://github.com/hadarkamer/crypto-intelligence-platform/pull/111)
was merged as `0f45423ddff82a1a1db67dba4b9d1df786bb4765` and deployed to
the Testnet service at 2026-10-01 12:39:05 UTC. The full PostgreSQL release
check passed all 579 tests. Subsequent SHORT maintenance cycles resumed with
continuous entries disabled and without the repeated checkpoint contention.

Reuse the passing lost-reply, restart, duplicate-prevention, partial-quantity,
stalled-worker and stop/close-race tests. They prove software decisions and
durable fencing; they do not prove that an exchange accepted a particular live
operation. Historical DOGE evidence already proves normal STOP Market execution
and verified closure, so do not repeat that experiment.

## Actionable checkpoint correction and deployed evidence

A fresh LONG SOL timing invocation on 1 October used original card
`0ea059862a6f1278b44b38d952f8ddf5573917bb194e93ea778e6919ba9d2798`
and source event `3c0a613ffa8141329cd7994efd5c66b5` from 13:06:40.330 UTC,
with original entry/stop/take prices 117.31 / 114.96 / 119.66. It sent zero
requests and produced no new fill or protection timing measurement.

The durable request `64107c2720b3be0952640afae2090a23040fc3d63b56b125d6d2c9af2a3d2e41`
remained PREPARED with integer zero attempts, no nonce, no attempt time, reply or
observed order. It was finally `ABORTED_UNSENT`, reason
`ENTRY_SOURCE_EXPIRED_WITHOUT_ATTEMPT`, with no pending request. Its initial
evidence was at 13:09:06.943 UTC and reservation at 13:09:21.837 UTC: only
106 milliseconds remained in the unchanged 15-second evidence window.
The subsequent separate begin failed freshness; parallel safety checkpoint
contention was also observed. The invocation reported incomplete reconciliation
because its supervisor did not join within two seconds. No timing success or
new trade is inferred from that invocation.

PR [113](https://github.com/hadarkamer/crypto-intelligence-platform/pull/113)
fixes the remaining authorization/begin gap. After a winning checkpoint, compute
the action again with the already-authorized sequence/client identity; require
identical terms except evidence hash/observation time. Preserve original
authorization freshness, source/grant expiry and pending identity. Changed
quantity/action or uncertain work requires new preparation, never retransmission.
Reserve or rebase a provably unattempted PREPARED request, allocate its nonce and
begin the sole attempt in one PostgreSQL transaction. Only an acknowledged
commit returns its exact immutable sender request; no subsequent load can swap
it for another worker's state. Network and signing remain outside database locks.

The full PostgreSQL release check passed 593 tests in 32.147 seconds on commit
`68ea85f0417f144bce13f0e62259f09ce284e168` (run 36868038130,
job 110388402201). The merged code `f55caea3aef4d1340e3551df7a1c8e511872e9cb`
became live at 13:23:47.156 UTC in Render deploy `dep-dav5tbgjo6nc73fm3ltg`.
Continuous LONG/SHORT entries remained disabled; the health endpoint reported
the stream and emergency supervisor running with `PASS_COMPLETE` after deploy.

An independent fresh post-deploy observation on instance `x7lbw`, running
`f55caea3aef4d1340e3551df7a1c8e511872e9cb`, at
2026-10-01 13:25:44.231 UTC (16:25:44.231 Israel time) confirmed the existing
DOGE SHORT card
`c1ac502c08722fe8d98c8a0d6f38cfd8b8cc2054753c28889be404859a91f732`,
source event `eb4fb1a2d58544f2aef95bb4a1d9005b`: OPEN, filled and remaining
quantity 6016, original STOP 0.096642 / order 61482899418 and TAKE_PROFIT
0.093318 / order 61482907893, with protection verified. No working entry,
pending request or reconciliation issues were present. The original fill was
at 2026-09-30 15:11:40.933 UTC (18:11:40.933 Israel time). This confirms
maintenance of a previously open card after deployment; it is not a new fill
or a new fill-to-protection timing result.

A read-only audit of both deployed account journals found two actual
`batchModify` requests already reconciled as `OBSERVED` with one attempt on card
`3609f6a4191104022430070598530b013ed4de4383155cc35a245f92ff9e85be`:

| Leg | Original order | Observed replacement | Quantity | Public observation (UTC) |
| --- | --- | --- | --- | --- |
| STOP | 61253269569 | 61457207797 | 0.00598 | 2026-09-30 09:05:50.931 |
| TAKE_PROFIT | 61253302636 | 61457341068 | 0.00598 | 2026-09-30 09:07:56.674 |

Reuse this native-adapter evidence with the passing partial-growth software
tests. It does not claim a new progressive partial-fill latency measurement.
No emergency incident/execution evidence was present in either account's
new-style dispatch journal at the audit read. This does not establish that the
shared reduce-only IOC venue adapter lacks execution evidence. The saved
ordinary `CLOSE_PASSED_TAKE` proof below establishes that narrower property.

`emergency_close.Venue` inherits `TestnetVenue.send` without overriding it.
Both emergency close and ordinary `CLOSE_PASSED_TAKE` submit one reduce-only IOC
through the same canonical wire, signing, nonce, Testnet endpoint and response
classification path. A saved ordinary IOC with exact request/order identity,
one attempt and independently reconciled positive fills can therefore prove
the shared venue acceptance/execution contract. An accepted reply alone cannot.
Reuse such evidence before requiring another venue experiment; it does not
claim a live emergency drill, emergency timing, slippage or complete-close
guarantees. Emergency decision, deadline, latch, residual handling and recovery
remain covered by the passing software fault tests, which need no repetition.

A read-only journal inspection confirmed qualifying ordinary IOC evidence:

| Field | Saved proof |
| --- | --- |
| Account / market / operation | SHORT Testnet / ETH / `CLOSE_PASSED_TAKE` |
| Card | `6283f9894f64233c97f024c8f3242bcc4df1a94f01e55d0f6fbd7d642a8066ae` |
| Source event | `a15ac007e3444946a9b4d71729c3bf58` |
| Request / attempt | `ce62dca34a60b6045cfcacd8bd0ebe43423f7e80e3de953ecb4c7995f5f4c2be` / `OBSERVED`, one attempt |
| Exact order identity | Client ID `0x50846414b3ceb33117ca16bc18413f8b`, order 61272991413, TAKE binding matched |
| Immutable wire | `type=order`, `reduceOnly=true`, `tif=Ioc`, quantity 0.2318 |
| Public fill | `hl:903369462648597`, BUY quantity 0.2318 at 2640.7, 2026-09-28 10:32:26.156 UTC (13:32:26.156 Israel time) |
| Terminal order | `FILLED`, same order ID, time and quantity |
| Saved observation | 2026-09-28 10:32:53.675 UTC (13:32:53.675 Israel time) |
| Reconciled lifecycle | `CLOSED`, remaining quantity 0.0000, no reconciliation issues |

Together with the unchanged inherited sender and passing emergency decision
and recovery tests, this completes the shared native IOC adapter evidence gate.
Do not require another live emergency trade to repeat that contract. It does
not measure live emergency-trigger latency or guarantee that a future IOC
closes an entire position; residual and unknown-outcome safeguards remain
mandatory.

## Fresh SOL fill: protection timing failed

The next genuine SHORT SOL card was
`b26f3ac19883b8d5da92de7e5554367e24af6bb3ff9f804c226d0bc6166df8fc`,
source event `25b78160fff84eda9e9df94586dd13fb`, received from the original
source at 2026-10-01 13:36:20.078121 UTC (16:36:20.078121 Israel time), with
original expiry 13:46:20 UTC and entry/stop/take prices 117.01 / 118.77 / 115.25.
It ran on deployed commit `f55caea3aef4d1340e3551df7a1c8e511872e9cb`.

Its ENTRY request
`dffaeaa1daef92c9c62589fae91a89d4866c855cd610f0655a19d4ddf050c0c4`
was reconciled as `OBSERVED`, one attempt, order 61566141934. Saved public
fill evidence proves a total of 5.68 SOL, rather than merely order submission:

| Fill identity | Quantity | Price |
| --- | --- | --- |
| `hl:282178240728174` | 1.99 | 117.66 |
| `hl:31089021499112` | 1.83 | 117.67 |
| `hl:807107504395797` | 1.86 | 117.65 |

The first fill occurred at 2026-10-01 13:38:00.618 UTC (16:38:00.618 Israel
time). It was observed at 13:38:23.212 UTC, 22.594 seconds after the fill.
Emergency protection latched at 13:38:25.213 UTC, 24.595 seconds after the fill,
reason `STOP_VERIFICATION_DEADLINE`. No STOP activation time was verified.
Repeated attempts to prepare the emergency close failed with
`EMERGENCY_FINAL_QUANTITY_EVIDENCE_EXPIRED`; no emergency order request or
pending close was recorded at the last inspection. The timed STOP gate **failed**.
Shared native IOC evidence above remains reusable, but it does not establish
that this emergency obtained sufficiently fresh quantity evidence or closed.

The read at 13:47:32.203 UTC returned a snapshot from 13:47:25.326 UTC
showing OPEN quantity 5.68, STOP coverage 0 and TAKE_PROFIT coverage 0. Some
inspection results failed their own freshness checks, so this is a saved
observation, not a claim of a continuously fresh current position or closure.
At about 13:45 UTC the private trial CLI was interrupted to stop duplicate
collection, leaving the deployed worker responsible for management. This is
not itself proof of a successful handoff or finality. One bounded recovery
cycle with scoped metadata/price reuse failed exact revision CAS and sent zero
orders. No new ENTRY is permitted while the incident/circuit remains latched.

## PR 114 latency correction and remaining operating limit

The failure exposed evidence aging during sequential preflight reads, waves
of historical order-status reads, and duplicate ordinary collections after an
emergency was already latched. PR
[114](https://github.com/hadarkamer/crypto-intelligence-platform/pull/114)
overlaps only independent normal preparation/ownership/budget reads, joins
started reads on every path, and makes normal maintenance yield before public
collection when the bucket already has an emergency. Emergency preparation
keeps public price/metadata reads before its final complete quantity observation.
Unknown outcomes, exact request identity, ownership, source/grant expiry,
15-second normal evidence freshness, five-second emergency quantity/price
freshness, price bounds and the stop deadline remain intact; there is no blind
network retry or cross-cycle evidence cache.

The full PostgreSQL release check passed 617 tests in 55.219 seconds on head
`00db74e41c7e3ad0dfe9d5d03e6e05aa64b7aaa5` (run 36871238919,
job 110399250398). PR 114 was merged as
`670bad7ab27dd6a8b6d9a4f9108c397f0f87d2c3`. Render deploy
`dep-dav69eo473hc73dpsflg` became live at 2026-10-01 13:49:32.975996 UTC
(16:49:32.975996 Israel time), running that exact merged commit.

Subsequent deployed cycles repeatedly failed `EMERGENCY_PRICE_SAMPLE_EXPIRED`:
the genuine price obtained before quantity reconciliation aged during that
collection. One bounded recovery of the existing SOL incident used an explicit
twelve-worker limit in both relevant collectors. It also failed the unchanged
price freshness gate, sent zero orders, and left emergency requests empty.
A saved observation from 2026-10-01 13:53:14.375 UTC (16:53:14.375 Israel time),
verified fresh at its inspection, still showed OPEN 5.68 SOL. It is a
point-in-time fresh observation, not proof of protection or closure.

PR [115](https://github.com/hadarkamer/crypto-intelligence-platform/pull/115)
changes the ordering to metadata first, then the final complete quantity
checkpoint, then a genuinely new public price read. Both five-second quantity
and price guards retain their original timestamps and remain enforced.
Independent review passed. The full PostgreSQL check passed 617 tests in
58.685 seconds at 2026-10-01 13:55:34.544 UTC on head
`844de39d76d81d4578e450e9730cac85be1c59c5` (run 36872156283,
job 110402380829). PR 115 was merged at 13:55:47 UTC as
`ff6cb08a9079e9e3b6bf823cf68068ff8a6bfbf4`. Render deploy
`dep-dav6cr0473hc73dqdbn0` became live at 2026-10-01 13:56:47.425342 UTC
(16:56:47.425342 Israel time), running the exact merged commit.
New-instance cycles repeatedly failed `EMERGENCY_FINAL_QUANTITY_EVIDENCE_EXPIRED`.
An earlier post-deploy recovery collided with exact revision CAS and sent zero
orders. A subsequent scoped twelve-worker recovery also failed final quantity
freshness, with no orders sent and an empty
emergency request list. Its saved snapshot at 2026-10-01 13:58:53.620 UTC
(16:58:53.620 Israel time), fresh at inspection, showed OPEN 5.68 SOL.
Containment and verified closure were still pending at that inspection; a
successful deployment alone does not establish sufficiently fresh execution
evidence. The later PR 116 closure proof is recorded below.

The ordinary evidence collector retains its default four-worker bound.
The optional twelve-worker bound is reserved for explicitly bounded recovery
invocations of the existing incident; it is not a global fan-out increase or proof
that shared request limits are solved. The official
[rate-limit documentation](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits),
checked on 1 October 2026, specifies a shared REST budget of 1200 weight units
per minute per IP. Requests have different weights; `userRole` alone costs 60.
The account-specific action-headroom check does not enforce this shared IP
budget, and parallel reads do not reduce request weight.

Before another entry experiment, establish a shared weighted request budget
across workers/processes and a low-latency fill/protection path. Prioritize
fill and protection work over historical/status scans, deduplicate overlapping
read work, and use exchange events for prompt fill notification with explicit
reconnect/gap reconciliation. Keep complete authoritative reconciliation and
all existing stale/unknown-evidence blocks. Neither quicker notification nor
software fault tests alone prove that a STOP became active at the venue.

## PR 116: reuse exact durable terminal facts

Historical owned order IDs were still read in both verification passes of each
complete collection on PR 115. Reordering the current price read did not remove
that cost. PR
[116](https://github.com/hadarkamer/crypto-intelligence-platform/pull/116),
head `1ff01f944bfb0eb4501e7d2a3f4ad02cbcf88db5`, implements reuse of exact
verified terminal-order certificates from durable evidence only in production
`TestnetVenue.collect`. The default strict audit path still rereads every owned
terminal order identity. Both fresh account observation passes are retained;
a newly final order still requires its status in both passes before reuse.
Fresh inventory must reject a certified order that reappears. Overlapping fill
history must reject added, changed or missing old fill facts. Retained fills,
exact ownership/identity, incomplete-evidence rejection, unknown barriers and
original observation/read deadlines remain intact. The default four-worker
bound is unchanged; this correction does not require another twelve-worker pass.

Eleven new software regressions cover these invariants. A fixture with 30
historical closed cards and active work requires four live order-status reads,
ten total public reads, while preserving both complete account passes. This
proves reduced historical read work in software, not exchange latency or SOL
closure. The full PostgreSQL release check passed 628 tests in 54.132 seconds
at 2026-10-01 14:04:16.117 UTC on the exact head above (run 36873288110,
job 110406212708). PR 116 was merged at 14:04:49 UTC as
`2b59e8b49d5d296d2b29a5cdd5ef4f6dca19c4b9`. Render deploy
`dep-dav6h23tqb8s739cul7g`, started at 14:04:56.513993 UTC, became live at
14:05:51.765979 UTC (17:05:51.765979 Israel time), running that exact merged
commit on new instance `qbmkj`.

Prioritized observations, the shared weighted request budget and an exchange
event pipeline with reconnect/gap reconciliation remain unimplemented design
and release requirements. Keep these separate from the terminal-fact reuse
already implemented in PR 116's code. Missing, contradictory or outcome-unknown
work must continue to require authoritative reconciliation and block unsafe
action; fewer historical requests do not by themselves enforce shared quotas
or prove prompt protection.

## SOL emergency closure: automatic and independently verified

After PR 116 became live, the new main worker automatically submitted one
`EMERGENCY_CLOSE` for the existing SOL card
`b26f3ac19883b8d5da92de7e5554367e24af6bb3ff9f804c226d0bc6166df8fc`.
The main log reported accepted/unverified at 2026-10-01 14:05:57.816549790 UTC
and `CLOSED_VERIFIED`, remaining 0.00, at 14:06:03.120083805 UTC. No manual
send on PR 116 or further entry was used. Both normal and emergency collectors
used their default four-worker bound and the durable-terminal reuse path.
Receipt and log status were independently checked against durable fill/order
evidence, rather than counted as closure alone.

| Field | Durable closure proof |
| --- | --- |
| Sole emergency request | `6078e88dd8c47cd13ec54c7f1473d0c6ddc00a5fd3f9345aa2dd965433d3ac98` |
| Request state / attempt | `OBSERVED`, one attempt at 2026-10-01 14:05:55.344 UTC |
| Exact order | 61568413678, terminal `FILLED`, quantity 5.68 |
| Saved complete observation | 2026-10-01 14:05:59.696 UTC |
| Independent durable record read | 2026-10-01 14:07:45.222 UTC |
| Lifecycle / quantities | `CLOSED`, entry 5.68, exit 5.68, remaining 0.00 |
| Reconciliation | No issues, leftover owned orders or pending normal/close/cancel request; incident `CLOSED_VERIFIED` |

The three exact closing BUY fills on order 61568413678 occurred at
2026-10-01 14:05:57.218 UTC (17:05:57.218 Israel time):

| Fill identity | Quantity | Price |
| --- | --- | --- |
| `hl:499356507649960` | 3.71 | 117.68 |
| `hl:637762639703637` | 1.02 | 117.65 |
| `hl:650109328629761` | 0.95 | 117.67 |

The independent historical record already proves closure. An initial subsequent
public read was unavailable; its HTTP status was not established, so no rate-limit
status is inferred. A later read-only `cycle(send=False)` with the default PR 116
collector confirmed `CLOSED_VERIFIED`, remaining 0.00, and sent zero orders.
Its freshly saved observation at 2026-10-01 14:08:23.429 UTC
(17:08:23.429 Israel time) confirmed lifecycle `CLOSED`, entry 5.68, remaining
0.00, no working entry/exits, no pending request and no issues, with both closure
and terminal verification true. Both inspected venue send counters were zero.

Health at 14:08:25.149 UTC showed the main worker and emergency supervisor
running, continuous trading false and emergency `PASS_COMPLETE`, with a last
pass at 14:08:24.026 UTC, 1.123 seconds earlier. Both LONG and SHORT entry flags
remained false. This is independent point-in-time current finality and worker
health, not a claim that future observations cannot fail.

The existing SOL containment gate is complete. The earlier STOP timing gate
still **failed**: this closure recovered a fill from approximately 28 minutes
before the fix. It proves actual automatic recovery/IOC execution and finality,
not timely STOP activation or a five-second emergency execution result. No
additional live trial is required merely to repeat this closure proof.

## Remaining necessary evidence

| Gate | Minimum evidence | Avoid |
| --- | --- | --- |
| Shared request budget and prompt protection | Enforce the shared weighted IP allowance across workers/processes and demonstrate prompt fill/protection processing with reconnect/gap recovery before another entry trial | Calling concurrency a quota solution; starving protection behind historical scans; removing authoritative reconciliation |
| Protection speed | The genuine SOL fill exists but its timed STOP gate failed. After the operating-limit fixes, one necessary fresh Testnet fill must show original active STOP and full TAKE_PROFIT coverage, venue activation latency and durable record-read bound; independently verify deployed management if left open | Treating a fill without verified timely protection, or an unfilled canceled entry, as success; refreshing an old signal or changing its price; claiming handoff from trial return alone |
| Real-money release | Approved and enforced capital, loss, exposure, leverage, emergency price/timing limits; monitoring/recovery and exclusive routing | Adding real account connections before stable Testnet evidence |

The emergency close is a price-bounded IOC: a five-second trigger does not
guarantee a complete close or a particular execution price. Unknown outcomes
must be reconciled before any repeat transmission.

## Mandatory future routing invariant

Owner clarification on 1 October 2026: every existing alert/card is assigned to
the demo environment. Future cards explicitly designate demo experimentation or
real trading; this designation belongs to the card, even when formula rules
provide its default. No existing card is promoted or replayed in a real account.
Real account connections remain deferred until the infrastructure is stable.

Later retain two demo accounts and add two real accounts. Formula/alert rules
select one environment first; direction then selects exactly one account in
that environment. One original alert must never execute in two accounts, even
if routing configuration changes, a worker restarts or a reply is lost.

Persist one immutable execution destination for the original source identity
(`source_stream`, `source_event_id`) before any submission. Environment-specific
card IDs alone do not prevent duplicate execution of the same original alert.
Configuration changes affect future unassigned alerts only; existing positions
remain with their original environment and account through protection/closure.
Missing or contradictory routing blocks execution; never fall back from demo
to real, from real to demo or to another directional account. Keep credentials,
request/nonce ownership, evidence and performance statistics separated by
environment and account. This section records requirements, not an implemented
mainnet routing capability.
