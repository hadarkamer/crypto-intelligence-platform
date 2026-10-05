# Omit impossible entry setup and idle terminal polling

Base: `690858872a67554c9f97ff74262b658756911380` on the deployed
`paper-trading-v1` Testnet branch. Scope is exchange request reduction only.

The stream used to refresh an occupied market again in its ENTRY pass whenever
another fresh unbound card existed, although `choose()` already disallows entry
until every predecessor and its orders are final. That pass repeated history,
inventory, mark and metadata reads after ordinary exit maintenance. Fresh alerts
on that market also spent account ownership/metadata quota registering candidates
which could not yet enter.

The ENTRY pass now omits occupied, pending, incident and staged-gap buckets
before any entry HTTP. New alerts for those markets stay recorded in the card
store with their original source/expiry; they are reconsidered after the market
is final and still require current ownership, public evidence, budget and final
action authorization. Other coins and the other account continue independently.

Once a current complete observation proves a fully protected position with no
working entry or pending request, price/metadata selection is omitted even when
new account entries are enabled: no independent second card can enter that
occupied market. Missing coverage, partial/working entries and uncertainty still
take their normal price/metadata and recovery paths.

On a reconciled clean notification feed, idle final history with no local live
candidate no longer incurs hourly SHORT scans or periodic old-gap catch-up.
The emergency supervisor also omits idle final/flat buckets instead of observing
them just because a new local card awaits entry. Notifications, disconnected
feeds, pending work, live exposure/orders and staged gaps restore normal work.
Old evidence timestamps are never advanced and entry evidence is never granted
from the idle history skip. New entries on old history still require recovery.
Final-flat local candidates also omit a duplicate exit-maintenance pre-pass;
their ordinary entry cycle still refreshes current evidence. A forced final-flat
observation with entries disabled obtains full current public evidence and omits
the unrelated price/metadata selection afterwards.

Retained: both public verification passes, overlapping fill history, exact order
identity/status verification, current position/order inventory, protection
priority/reserve, shared rate budget, pending/unknown outcome fences, source age,
manual-close audit, independent emergency execution, and account isolation. No
quota, strategy, price, risk, schema, trial window or Mainnet changes.

Regression coverage counts zero ENTRY reads in thirty repeated occupied-market
sweeps for each role, existing local candidate suppression, independent other
coin progress, reconsideration after closure, final-history idle suppression,
dirty/disconnected/staged-gap/pending restoration, and protected versus
uncovered/working-entry price/metadata paths. Run the complete runtime suite,
executor/forwarder suite and required PostgreSQL CI before deployment. Report
post-deployment trade/protection evidence separately from software test results.

The legacy shared-position cleanup fixture intentionally bypasses the new early
no-work gate only while constructing its obsolete unsafe exchange-double state,
alongside its existing selector/fence overrides. All overrides end before the
actual orphan cleanup, double-fill, restart and ownership tests run.
