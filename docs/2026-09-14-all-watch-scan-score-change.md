# All-Watch score changes, adapter v5

This bounded step adds twelve original strengthening/weakening definitions to
the all-scan research population:170 supported of298,128 still unsupported.
It changes no original formula condition, model score, source route, outcome,
selection of comparison cohorts, Watch schedule, alert or trading behavior.

## Explicit population and predecessor contract

The three original keys are `sequence.30m.price_oi.score_change`,
`sequence.30m.futures_cvd.score_change` and `sequence.30m.spot_cvd.score_change`.
Each subtracts the prior aligned total from the current aligned total. The
strengthening predicate is strictly greater than0; weakening strictly less than0.
Zero matches neither. Three models×two conditions×normal/inverse gives12 definitions
and24 additional base-direction decisions percoin, for340 total decisions.

This is an explicitly new all-Watch population contract, not a claim of equivalence
to delivered-alert sequence research. The earlier alert loader considered delivered
events and could skip a missing module to an older numeric score. The new selector
uses the closest eligible captured scan consistently across all three models.
Missing evidence in that scan remains missing; it cannot choose a more convenient
older value. Event-entry ordinals and event-family ordering remain unsupported.

An eligible predecessor is an ACCEPTED scan from the same consumer, with a
different `watch_scan_id`, strictly earlier observation time, and usable time in
`[current usable time - 30 minutes, current usable time)`. Usable time retains the
existing maximum of source computation, availability and creation times. Source
identity and timestamps are revalidated, not inferred from snapshot ID order.

The latest eligible usable time wins. A tie between distinct captures is explicitly
AMBIGUOUS and produces missing deltas. Snapshot IDs only stabilize evidence ordering;
they never break a semantic tie. No predecessor produces NO_PREDECESSOR, not zero.
The original30-minute window is not widened when scheduling jitter puts the previous
scan slightly outside it. Preflight35 scans contain19 eligible predecessors and16
without a predecessor in this exact window; these are expected availability states.

## Bounded source reads and frozen evidence

An indexed intake metadata query reads at most the two latest candidates per selected
scan. It does not require any earlier formula job to be READY, and it never reads
delivered events, outcomes or providers. The selected original coin observation is
then loaded from the immutable source for each coin. Both current and previous
aligned totals use the existing source/availability validation and direction mapping.
HYPE uses its valid captured CVD/OI totals like every other coin. Its own Spot-price
features stay unavailable under the priorv4 contract.

The first READY v5 coin freezes predecessor identities for its entire scan, including
an empty or ambiguous selection. Later coins, split passes and retries reuse those
identities. A source admitted to intake later cannot change which previous scan was
used. Shared selection errors are isolated once per scan; individual source read
errors use the existing percoin savepoint and five-minute retry. A failed database
read cannot masquerade as absent source data.

The score-change context binds the current source identity and watch ID, current
validated model evidence, selected predecessor identities and original model/source
proofs, both base-direction deltas/missing reasons, selection policy and context hash.
Its validation rederives the evidence from the retained source proof. Inverse
formulas keep these base-direction predicates and only reverse outcome direction.

Version: `watch-scan-formulas-v5-score-change`.
Feature version: `watch-captured-total-maxpain-btc-asset-and-score-change-features-v5`.
Context: `watch-prior-scan-aligned-total-change-30m-v1`.
Selection: `watch-latest-earlier-distinct-scan-30m-no-skip-v1`.

## Prior-version preservation and rollout

V5 reuses each older v4 coin's frozen own-price context, including missing windows.
The shared BTC context preference is v5, v4, then v3. Only a scan without frozen
context reads price history. These rules preserve all158 prior decisions exactly
even if the price archive gains history after the earlier calculation.

Migration052 extends the coverage check to340 decisions and adds the accepted-intake
usable-time index. It does not rewrite prior samples or switch the active pointer.
The unchanged16-coin/30-second worker activates v5 only after every accepted scan has
eight READY v5 samples. Current views then show v5; audit views retain all versions.
READY includes explicit UNKNOWN decisions and does not assert source availability.

Apply only migration052 after full CI. Do not rerun older coverage migrations on a
newer active version. Deploy after the ordinary Watch cycle completes. No manual
Watch, Telegram test/message or trading action is required.

## Validation and next scope

Tests cover exact170/340 coverage, original definitions and retained decisions;
strict time boundaries, unavailable nearest models, ambiguity, source timing,
same-scan rejection, signed deltas and inverse orientation; context tampering;
previous-source independence from job completion; shared selection across split
passes with late intake; frozen v4 price proof reuse; real SQL failure/retry isolation.
Production verification compares original captured model scores and selected source
times independently, plus complete old decision JSON and source/context hashes.

The overall plan remains in topic1, research from all scans. No formulas are newly
discovered or promoted, and evidence remains descriptive. Remaining128 definitions:
timeframe liquidity34 (32 standalone plus2 that also require a Combined event),
Combined top-item22, captured confirmation16, sequence entry/family order34,
deferred MaxPain components10, same-timeframe score difference2, absent short MaxPain
horizons8, and standalone Combined event type2.

A possible later bounded step is a separately identified timeframe observation
dimension for32 liquidity and2 same-timeframe difference definitions. Captured
`selected`, `near_amount`, `far_amount`, and `near_share_pct` exist per timeframe;
none is a single selected coin-wide value. Reusing coin outcomes must not multiply
independent BTC waves. Combined and confirmation labels are not in the current
capture and cannot be synthesized from ordinary totals. Cluster/CVD component
hypothesis research, B4 and240m family normalization remain deferred.
