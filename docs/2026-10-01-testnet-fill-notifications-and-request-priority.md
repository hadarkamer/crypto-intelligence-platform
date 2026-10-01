# Testnet fill notifications and request priority

## Scope and authority

Accelerate the existing two Testnet accounts without enabling continuous entries,
adding Mainnet accounts or changing immutable alert prices, source expiry,
account ownership, quantity reconciliation or risk limits. Existing alerts remain
assigned to demo accounts. This release is infrastructure validation; it does not
claim a new exchange fill or a measured fill-to-STOP success.

## Behavior

- Two isolated, fixed-host public websocket sessions subscribe to `userFills`
  and `orderUpdates`. Notifications wake and prioritize the ordinary authoritative
  REST observer. No websocket value becomes a fill, protection or closure proof.
- Startup, disconnect, missing heartbeat, malformed notifications and new hints
  block ENTRY until complete saved REST evidence and fresh account inventory
  clear the exact session generation and notification revision. A concurrent
  notification cannot be acknowledged by an older collection. Session callbacks,
  deduplication, frames, symbol hints and reconnect frequency are bounded.
- A cleared notification gate records continuity, not current position authority.
  Existing source, observation and attempt deadlines remain enforced separately.
  Completing a gap token still requires at most fifteen seconds.
- Pending requests, working entries and uncovered positions precede protected
  positions and old flat history, across both accounts. Complete terminal flat
  checkpoints are retained during a gap; fresh full account inventory still
  checks current positions and order ownership. An explicit hint for a closed
  symbol requires a new observation.
- With continuous notifications, an already fully protected position with no
  pending request or working ENTRY uses a ten-second normal REST fallback.
  New hints or connection gaps bypass that quiet cadence. The emergency lane
  retains independent takeover for delayed or uncertain protective work. Actual
  emergency quantity, price and send freshness remain five seconds.
- A fresh, fully protected no-action checkpoint avoids unnecessary metadata and
  price reads. Old flat history and quiet, currently protected positions with a
  healthy notification feed use background request priority. Public reads
  remain bounded and live evidence is never replaced by a notification.

## Shared request admission

Configured processes and both accounts use one PostgreSQL request-weight ledger.
The accounting window is a conservative 69 seconds: 1200 total weight, with
background admission capped at 800 to leave 400 for protection. Admission commits
before transport; the local permit is single-use and expires after one second.
Fill/history requests reserve their maximum bounded response weight and refund
only the proven excess after a successful decoded response. Failed calls retain
their reservation. No database transaction contains HTTP or waits for quota refill.

This measures configured clients, not unrelated clients behind the same NAT.
DNS and OS scheduling do not have a hard end-to-end deadline, so the ledger is
an admission limit rather than a guarantee of exchange availability. Exhaustion
must fail closed; it never authorizes a retry of an uncertain transmission.

Exchange-weight admission precedes durable nonce and attempt creation. A refused
admission consumes no pending request or emergency action slot. A permit that
expires after an acknowledged begin may retire only that exact request, using a
local pre-HTTP certificate bound to its ID, nonce and proposal. The abort keeps
the attempt audit and permanently prevents another ENTRY from that card. Changed
state, an uncertain abort commit or any HTTP exception retains uncertainty. A
newly acquired standalone permit cannot certify an already-attempted request as
unsent.

Primary protocol and request-weight references:

- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint

## Validation and next measurement

The release check runs the existing software and PostgreSQL fault regressions
plus notification continuity, exact revision barriers, partial fills, account
priority, quota concurrency, restart, lost commit, denial and permit expiry cases.
Passing software tests proves these decisions and durable fences; it does not
measure the live exchange's protective response time.

After deployment, verify both notification subscriptions, cleared current gaps,
healthy normal and emergency lanes, disabled continuous ENTRY flags and absence
of sustained admission failures. The remaining live measurement is one explicitly
authorized fresh-alert Testnet fill with independently verified STOP activation
and a durable-record-read upper bound. Existing successful emergency and closure
experiments are reused instead of repeated.
