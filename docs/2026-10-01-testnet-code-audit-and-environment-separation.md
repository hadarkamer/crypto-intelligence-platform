# Testnet safety audit and deferred account separation

## Scope

Preserve the current two Testnet accounts, immutable source prices and expiry,
single-attempt request identity, ownership reconciliation, partial-fill coverage,
reduce-only exits, risk/capacity checks and emergency entry latch. Continuous
entries remain disabled. This change sends no trading instructions and creates
no mainnet connector, credentials or account configuration.

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

## Remaining necessary evidence

1. One isolated Testnet fill through the same controller: record actual fill,
   active STOP, full TAKE_PROFIT coverage, venue activation latency and a durable
   record-read bound. Existing no-fill experiments do not prove these times.
2. Controlled emergency-path evidence, including rejected/delayed protection and
   lost replies. Reuse passing fault-injection tests; repeat a venue experiment
   only for a remaining externally observable property.
3. Final operational limits, monitoring, recovery and exclusive account routing
   review before any mainnet activation. Do not add real account connections now.

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
