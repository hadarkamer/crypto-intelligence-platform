# Manager transition and staged HYPE execution

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

## Staged HYPE implementation, not live activation

Identity: HYPE_WATCH_LIQ4050_24H_INVERSE / hype_watch_liq4050 / HYPE.
It remains outside NEW_ENTRY_IDENTITIES. formula_accounting additionally
rejects a new allocation with FORMULA_LIVE_ALLOCATION_NOT_APPROVED before
creating any ledger or inheriting the old HYPE ledger or the $2 fallback.
Existing immutable risk snapshots remain readable for historical management.

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

## Isolated evidence and limits

- manager-hype-final.log: 25 passed (11 real PostgreSQL, 11 memory cases,
  3 actual LiveDispatchPort tests with key/signature/HTTPS boundaries controlled).
- manager-staged-regression.log: 85 passed at the documented intermediate
  checkpoint, including unchanged accounting and historical DOGE ownership
  tests. Subsequent HYPE-only expiry/capacity changes were covered by the
  25-case final HYPE suite rather than repeating unrelated regressions.
- All tests used the existing product worker and test ports. No live order,
  provider call, injected live signal, nonce, risk change or deployment.
- The live risk allocation is intentionally undefined. The $40/5% ledger
  in isolated fixtures is an explicit synthetic test input, not approval or a
  transfer of the old formula's allocation.
- Frozen research replay and the full new source-to-server integration must
  have their own evidence. These manager tests do not prove those steps or any
  exchange fill in production.
