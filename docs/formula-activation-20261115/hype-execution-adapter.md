# Manager transition and approved HYPE activation candidate

## Independent deployable suspension checkpoint

The three-roster checkpoint retires admission of CT009, OCONF and ROW71205
by exact family/rule/symbol identity. An existing source record is latched
RETIRED without fabricating a source cancellation or changing its frozen
prices. A complete owned inventory permits cancellation only of its ENTRY
OID. Filled quantity receives existing STOP-first protection before remaining
entry quantity is canceled. An unfilled known ENTRY does not require a new
market-price call to cancel it. Unknown attempts remain unknown and are never
resubmitted; manual management remains outside automation.

Focused proof: manager-suspension-focused.log (12 passed, 6 real PostgreSQL).
Broader actual HTTP/PostgreSQL transition coverage is owned by the integration
suite and must be cited separately, not inferred from these focused tests.

## HYPE execution and the approved separate portfolio

Identity: HYPE_WATCH_LIQ4050_24H_INVERSE / hype_watch_liq4050 / HYPE.
The final candidate includes it in NEW_ENTRY_IDENTITIES with XRP R2732,
SOL SFF1361 and XRP Liquidity inverse. CT009 and OCONF remain paused;
ROW71205 is retired for new entries. Historical contracts remain readable.

The user's 2026-11-15 approval gives this identity an initial $40 portfolio
and 5% of its updated realized balance per new entry. Its approval is preserved
in HYPE_Live_Risk_Approval_20261115.json, separately from the research handoff.
No $5 research stake, old-HYPE history, fallback risk, or other portfolio's
balance funds the new identity.

`formula_accounting` uses the existing locked PostgreSQL execution journal.
A one-time `allocations[HYPE_WATCH_LIQ4050_24H_INVERSE]` receipt and the initial
portfolio are committed atomically with normal admission. The receipt assigns
$40 from GLOBAL_EXISTING_CAPITAL; it is a reservation, not an exchange transfer.
Existing portfolios are global across both independent routed accounts. They
are therefore reserved once globally, never attributed arbitrarily to one
account. Every existing balance is retained, including retired/paused formulas.
A prior approved initial $40 entitlement is conservatively reserved if one of
the prior six portfolios was never lazily initialized; its distinct basis is
recorded. Known non-reduce entry obligations are additionally reserved at their
full frozen notional, matched once by account/CLOID across open inventory and
uncertain durable requests. This conservative bound is not a new margin or
leverage model. Unknown legacy pending obligations refuse only new-HYPE funding.

Cash provenance reuses the ordinary account inventory and its original time.
Only while funding is absent and a HYPE event is eligible, bounded
`userAbstraction` probes bracket that existing inventory under the unchanged
background request budget. Native `disabled` is accepted. `default` retains
its existing narrow approved native-account scope and additionally requires
an all-zero spot inventory; it is not renamed. Unified/portfolio cash is not
inferred from perp summaries and refuses new-HYPE allocation. Mode/spot failure
leaves ordinary inventory available to manage protections. A failed or
insufficient funding observation may be retried at most once per minute by a
later eligible event; no probes are added after the durable receipt exists.

For each distinct verified native account, the observed cash bound is
min(totalRawUsd, accountValue, withdrawable), with negative cash refusing
allocation rather than disappearing from the sum. No spot/perp mirror is
added and no unobserved account contributes assets. This verifies a bounded
source of existing funds, not an assertion that all unseen account equity is
known. After existing claims and pending obligations, at least $40 must be
verified before initialization. Refusal records exact observed/reserved/free
amounts, missing accounts and the gap to that verified lower bound; the gap is
not misreported as an actual live capital shortage.

Restart/retry validates the same receipt and never reinstates $40. Existing
net settlement applies gross P/L, fees/rebates and funding once, with its same
atomic trade receipt and ledger revision. Risk examples are $40→$2,
$50→$2.50, $30→$1.50. The final sender requires the route-bound funding receipt.
Position, exposure, manual ownership and unresolved-request fences are unchanged;
the new identity cannot adopt the old manual HYPE position or bypass its lane.

The operational adapter submits one IOC at the unchanged frozen reference
price E. It does not reprice, add an adverse wait or leave a GTC order. Entry
is accepted only within the original reference minute and signed exchange
expiry is capped at that same minute's end. This minute is an explicit live
adaptation, not proof of execution at the research candle OPEN. An IOC can
fill at a permitted better price, partially fill, or not fill at all. Exchange
fills are recorded separately; their timestamps and prices are never replaced
by the modeled reference. A certified-unsent entry is not retried for this
formula. Unknown HTTP outcomes retain the existing reconciliation fence.

The first selected event is permanently retired when its HYPE account/symbol
lane has confirmed owned, external or legacy occupancy, or an unresolved
same-lane request. It cannot become a later event when capacity clears. Other
formulas retain their existing admission behavior.

SL=E*1.02 and TP=E*0.98 remain frozen reference terms. Holding duration is
86,400,000ms from the FIRST ACTUAL ENTRY FILL, not alert creation, delivery,
research reference, later partial fill, or restart. At the deadline the same
manager uses its existing bounded reduce-only IOC path. The final dispatch
boundary independently derives the deadline from owned observed entry fills.
Concurrent observed SL/TP fills reduce the remaining quantity or suppress the
close; unknown time-close responses cannot be replayed. The exchange's
reduce-only order semantics remain the defense against a fill after the final
observation. Existing native protections are retained until actual closure,
then cleaned up by the existing lifecycle.

The real-provider integration found that the existing stable-protection quote
optimization omitted the mark needed for a due HYPE time exit. The staged
provider now requests its existing shared fresh mark only when this actual-fill
deadline is due; it still skips the quote before the deadline. No new fetch
path, per-formula quote, cache clock or request budget was added. The final
25-case suite includes the before-deadline/deadline selector regression.

The same real-provider integration then reached the existing persisted IOC
identity verifier, whose TIME_EXIT allowlist originally recognized only DOGE.
It now also recognizes exactly (HYPE_WATCH_LIQ4050_24H_INVERSE, HYPE,
TIME_EXIT, STOP) with the approved contract version and reduce-only IOC wire
shape. All original OID, CLOID, quantity, price, timestamp and fill validation
remains. The final 25-case suite verifies this lookup and rejects substituted
old-HYPE identity, symbol, operation or leg. Eight existing DOGE terminal
identity tests were rerun and passed (manager-hype-identity-regression.log).

## Historical staged evidence and final-candidate limits

- manager-hype-final.log: 25 passed (11 real PostgreSQL, 11 memory cases,
  3 actual LiveDispatchPort tests with key/signature/HTTPS boundaries controlled).
- manager-staged-regression.log: 85 passed at the documented intermediate
  checkpoint, including unchanged accounting and historical DOGE ownership
  tests. Subsequent HYPE-only expiry/capacity changes were covered by the
  25-case final HYPE suite rather than repeating unrelated regressions.
- All tests used the existing product worker and test ports. No live order,
  provider call, injected live signal, nonce, risk change or deployment.
- The staged logs above predate the explicit risk approval. Final candidate
  tests replace their synthetic guard overrides with the actual product
  funding path; see manager-hype-capital-unit.log and the final PG/HMAC report.
- The read-only live journal held eight existing portfolio balances totaling
  $310.8782200 at the recorded inspection. All remain claims. No current cash
  or account-mode fact was persisted, so live funding sufficiency was not
  inferred. The old HYPE manual/unknown request remains unchanged and can
  legitimately prevent new-HYPE admission. No new HYPE allocation or fill has
  been observed in production.
- Frozen research replay and the full new source-to-server integration must
  have their own evidence. These manager tests do not prove those steps or any
  exchange fill in production.
