# Existing formulas on the all-Watch population: first feature adapter

This is the next implementation step within **research from all scans**. It
does not start continuous formula discovery, archive completion, HYPE completion,
new-case acceptance, B4, cluster/CVD component hypotheses, or 240m normalization.

## Scope and identities

`watch-scan-formulas-v1` keeps all **298 existing candidate definitions** intact,
including 149 NORMAL and 149 INVERSE identities. Each complete original candidate
has an exact canonical JSON SHA-256. No condition, cutoff, operational score,
catalog candidate, alert or promotion policy changes.

The first adapter, `watch-captured-total-model-features-v1`, supports four feature
names: `price_oi.aligned_score`, `futures_cvd.aligned_score`,
`spot_cvd.aligned_score`, and `time.weekend`. These cover **34 definitions**.
The other **264 definitions** remain registered with explicit unsupported
feature lists. A definition with an unsupported predicate is not partially
presented as an evaluated formula. Connecting its exact captured/historical
feature contract is remaining work within this topic.

The source is the accepted immutable `watch-operational-scores-v2` bundle, in
`watch-all-scan-observations-v1`, including zero and below-65 model scores.
One coin sample produces 68 decisions (34 definitions × two base directions),
without multiplication by the seven MaxPain horizons. Source hash, parent hash,
source time, usability time, feature version and feature payload digest are kept.

## Feature decisions and missing data

Only available captured model totals with finite valid scores and causal source
times become known features. The original model direction/sign alignment is
preserved. Unavailable fallback zero remains missing; a real available zero is a
known value. Source errors are scoped to their own model. The weekend predicate
uses the original feature observation time in Asia/Jerusalem, preserving the
existing Saturday/Sunday definition. Price entry remains after source usability.

`MATCH` means all known conditions pass; `NO_MATCH` means a known condition
fails; `UNKNOWN` means missing data prevents either conclusion. Missing feature
names remain in the decision even when a different known false conjunct proves
NO_MATCH. INVERSE uses the same base-direction features and decision; only its
measurement direction flips. There are no synthetic alert events.

An unavailable HYPE Spot model is not zero. A valid captured HYPE Spot CVD model
can be evaluated; this does not request Spot price candles. All HYPE outcome
prices still use the already established Hyperliquid perpetual trade route.

## Comparison and wave selection

Read-only views join decisions to migration 047 measurements through the same
receipt, coin and source hashes. Measurement, entry, v7 outcome, BTC parent,
feature and selection versions are explicit. Formula feature evaluation itself
does not read prices, labels, BTC waves or outcome quality.

`watch-first-arm-cohort-unknown-blocks-v1` selects the earliest known MATCH and
NO_MATCH cohort within each candidate, base direction, BTC wave and symbol
scope, retaining every tied coin. Selection occurs **before** any outcome join.
An earlier or tied UNKNOWN prevents eligibility, including an unfinished feature
job. Later winners never replace an early missing, open or losing measurement.
Only causal LIVE BTC membership belongs to wave comparisons; other memberships
remain visible in measurement records and raw feature evaluations.

Scopes are ALL and each individual coin. Missing data in one coin can block the
conservative ALL cohort while leaving a separately reported coin scope usable.
Counts must never be added across scopes, aliases, directions, windows or barriers.

Each selected arm produces four windows (60/240/720/1440 minutes) and eight
25–200 bps barriers from the existing v7 labels. A tied cohort requires every
member: reporting priority remains DATA_MISSING, OPEN, AMBIGUOUS, NO_TOUCH,
FAILURE, SUCCESS. An early first touch in an OPEN measurement remains OPEN in
comparisons until that window is complete and frozen. Full-window MFE/MAE are separately named and only present when
every eligible member has a completed window; min MFE and max MAE are conservative.

The comparison exposes matched/control/union/shared wave counts, decisive
success/failure counts, unresolved categories and explicitly labeled decisive
success percentages. The two arms can share a wave and can start at different
times. This is descriptive observational research, not a causal estimate or a
statistical independence test. `statistical_test_performed` and
`qualifies_as_prospective_formula_evidence` are always false. Registration now
does not turn earlier observations into prospective evidence.

## Operations and review

Migration 048 adds catalog, queue/cursor, immutable ready samples and read-only
evaluation/population/anchor/outcome/comparison views. A finite cyclic cursor
and recent reserve recover late committed receipt IDs. Each 30-second pass
processes at most 16 coin jobs with per-coin savepoints. Queue, receipts and
decisions commit atomically. A failed coin retries after five minutes; ready
features never change when price measurements mature.

The worker inherits the existing research enrichment enable flag (optional
`RESEARCH_WATCH_SCAN_FORMULA_ENABLED` override), exposes health, and has no
provider, Telegram, Sheets, discovery or acceptance dependencies. CI runs pure
semantics tests plus isolated PostgreSQL recovery, immutability and cohort tests.
Apply only migration 048 after CI, then deploy after a scheduled Watch completes.
No manual Watch or Telegram tests are necessary.
