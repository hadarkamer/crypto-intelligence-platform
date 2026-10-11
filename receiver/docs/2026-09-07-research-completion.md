# Ordered-v7 research completion release

This release extends research measurement and search. It does not enable trading,
Telegram recommendations, or automatic promotion of historical discoveries.

## Data and measurement

- Keep native LIVE alerts and Telegram archive reconstructions in different
  tables, source contracts and result identities. Preserve original messages.
- Preserve both Israel-midnight periods: all compatible native records since
  2026-08-16, and records since 2026-09-04. Exclude a parent crossing a period
  boundary from that period's independent count. Overlapping periods are not
  additional evidence.
- Validate ordered-first-touch-v7 labels against the immutable original event
  identity and entry price. Derived inverse labels use an explicit linked
  measurement event, the same original entry and exactly one direction flip.
- Measure 25/50/75/100/125/150/175/200 bps separately at 60/240/720/1440 minutes.
  Missing paths, ambiguous touches and no-touch windows never become failures.
- The common-window sidecar measures the complete eligible closed Spot 1m path
  after an early First Touch. It records boundary exclusions and quality;
  incomplete metrics stay unavailable. Ratios with zero adverse excursion are
  undefined. Formula medians remain medians; asymmetry is a separately named
  ratio of sums over the same wave cohort, horizon and threshold.
- The pre-entry sidecar measures six closed-candle lookbacks, matching BTC
  windows and relative strength. It reports return signs without inventing a
  sideways/trend regime. It uses one event per pass, bounded HTTP requests and
  durable recent/backlog alternation after existing v7 Sheet delivery.

## Frozen search and future evaluation

The catalog contains 214 candidates (107 normal and 107 inverse), including the
original seven unchanged definitions. It preserves the Q01–Q72 research map,
covered fields, missing fields, failed predicates, overlapping families and
changed-input fingerprints. The search includes actual total scores, named
MaxPain averages/components, TF counts and denominators, validated liquidity
amounts, confirmations and pre-entry price/BTC features. Historical predicates
must account for earlier alerts whose required past features are still unknown;
those alerts cannot silently become nonmatches or cause a later entry to be
selected for an independent wave.

Conditions, direction, source, period, threshold and horizon freeze on the real
database clock. Existing waves remain DISCOVERY, including later updates to
their outcomes. Only later whole BTC parents enter PROSPECTIVE. Initial
incomplete source populations do not freeze representative entries; subsequent
coverage regressions cannot rewrite previously frozen choices. FRESH requires
whole-parent evidence in the rolling 14-day window and remains experimental.

No compatible numerical acceptance policy was found for this exact ordered-v7,
parent-200bps, common-window contract. The acceptance configuration therefore
remains explicitly missing. Old replay thresholds are not copied. Probability
and asymmetry are evaluated separately or jointly only under an explicitly
bound policy. Sample minimums alone never qualify a candidate.

## Isolated archive intake

Archive reconstruction starts at 2026-08-16 and uses only a message's own known
coin, direction, source time and fields. Its separately versioned entry is the
first full minute's official Binance Spot OPEN after the message; the printed
quote is preserved for audit. Missing message identity and unavailable HYPE
history remain explicit. No later message completes an earlier entry.

Migration 028 supports raw source staging; migration 032 supports isolated
reconstructed events, outcomes, full-window metrics and BTC parents. The runtime
importer requires an explicit local artifact path, exact SHA256 and run key,
then performs bounded atomic idempotent imports. It has no public upload route
and cannot create native LIVE events. Raw intake and reconstruction intake must
both be verified before claiming production archive ingestion.

## Deployment and remaining dependencies

Apply migrations 029–034 after existing migrations 025–028. Verify actual worker
progress and database rows after deployment, not only a successful build.
The unversioned Google receiver uses single-row delivery until it confirms the
tested batch-v2 contract. Updating the bound Apps Script requires authenticated
Google access separately from the connected Sheet editing capability.

Archive artifact delivery into the authenticated production runtime remains a
separate access step. Unsupported sequence questions and undefined market
regime classifications retain truthful partial/missing statuses. These are
not represented as completed statistical research.

## Verified pre-deployment checkpoint

The final candidate-specific historical coverage regression passed using the
generated PostgreSQL predicate and the validation adapter: unknown earlier
history blocks the rate and representative freeze; enrichment selects the
earlier failure without a spurious conflict. The entry fingerprint binds only
frozen predicate fields plus immutable entry identity, so unrelated enrichment
does not change it. The final focused group passed 57 tests; independent review
also ran the 12 search and 9 validation-adapter tests. Earlier focused worker,
delivery, metric, inverse and archive tests passed. All 34 changed Python files
compiled and the patch whitespace check passed.

The isolated reconstruction completed 8,074 supported Spot events with 516,736
ordered threshold/horizon/direction cells and 64,592 complete common-window
cells. The full contract audit found zero exclusions and zero contradictions
across 258,368 normal/inverse pairs. Nondecisive cells remain 136,268 no-touch
and 272 ambiguous cells. A further 1,151 HYPE source events remain unsupported
under this exact archive Spot method. Cell counts are not independent waves.
No production archive import or formula promotion is implied by this audit.
