# Ordered v7 experimental notifications

Authorized research-only notifications are labeled **ניסיוני, לא למסחר**.
The original v6 worker and catalog remain quarantined. No order-execution API
is connected. There is no per-formula human approval step.

Migration 037 adds a qualification registry and a separate durable outbox.
The existing initialized Telegram bot and active
`research_formula_alert_subscriptions` supply transport and authorized targets.
Enable only after migration with `RESEARCH_ORDERED_EXPERIMENTAL_ALERTS_ENABLED=true`.
The ordered formula worker must also be enabled. Bind, start and stop
`research_ordered_experimental_worker.WORKER` alongside the other workers.

A scope must have the exact current catalog, ordered-v7 method, LIVE source,
immutable real-time freeze and unchanged acceptance policy. Existing regular
5-wave and FRESH 3-wave gates are recomputed; no threshold or family is relaxed.
Every currently qualifying scope is refreshed with at most eight priority
slots within the existing evaluation budget. Qualification expires after 30
minutes; FRESH also expires at its earliest evidence's 14-day boundary.

Qualification publication uses the real database clock, separately from the
earlier analysis cutoff. A later pending wave may temporarily lack a full
future horizon; the prior unexpired qualification is retained only if all
original waves and common-window metrics still revalidate identically, source
coverage stays complete, and additions are exclusively later OPEN/DATA_MISSING
waves. This never extends the original lease. Changed proof, a newly resolved
contrary result, conflict or expiry revokes it.

The trigger is a native `ALERT / DELIVERED` with the exact frozen predicate,
source direction, exact original reference-price provenance and causal BTC
membership. DEMO, derived or archive-marked sources are rejected. It must arrive strictly after
qualification and be at most 10 minutes old. It cannot share any BTC parent
used by that qualification, including discovery parents. Archive records and
inverse synthetic outcome rows cannot trigger. An inverse formula uses the
already mapped match direction, without reinverting displayed Max Pain sides.

One message per candidate/direction/BTC wave/destination suppresses overlapping
coin, period, threshold and horizon scopes. A second uniqueness rule prevents
multiple formulas sending for the same native event/direction/destination.
This is notification deduplication, not a change to individual alert outcomes.
The exact selected scope and evidence remain in the durable payload.

Two notifications at most are attempted per 30-second worker pass. Claims are
leased, and subscription, freshness and current exact qualification are checked
again immediately before sending. A committed SENDING row marks entry to the
network call. Definitely-unsent CANCELLED rows release dedup reservations;
SENT/SENDING/UNKNOWN reservations remain. Because Telegram has no idempotency key, an ambiguous exception or
expired SENDING lease becomes UNKNOWN, never an automatic retry. An expired
CLAIMED lease is safe to reclaim. This deliberately favors avoiding duplicate
notifications over retrying an uncertain delivery; UNKNOWN is visible in the
outbox and worker counters. A missing positive Telegram message ID is UNKNOWN.

Version 3 is necessary because the earlier v2 registration and freeze used
different exact candidate definitions. Existing v2 freezes remain immutable;
new v3 scopes start prospective validation at their actual new freeze time.
Deploying this path cannot create historical prospective waves immediately.
