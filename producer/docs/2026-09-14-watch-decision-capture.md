# Frozen Combined and confirmation evidence

The existing all-scan research supports204 of298 original definitions. Another42
definitions need actual Combined identity/top-item or confirmation evidence that
the score-only archive did not retain. This increment captures that evidence for
future normal Watch scans. Formula support remains204/298 until a separate
versioned evaluator defines the appropriate selection and population contracts.

## One operational calculation

Shared Watch prepares its ordinary opportunities and frozen derivative snapshot
once. MaxPain item confirmations already exist at that point. Combined and its
Magnet confirmations now run once before archive persistence on the exact same
input subset used by the existing Combined path: the scorer's retained items,
the optional Top8 filter, and valid displayable targets. Watch's score-display
threshold is not a Combined qualification gate.

Optional capture sinks retain all evaluated Magnet clusters before the
confirmed-only selection, and every Combined group before the two-signal gate.
The same successful candidate list, including an empty list, is passed to the
original later collector. That collector still owns state changes and delivery
selection. Capture performs no provider reads, scoring replay, Telegram send,
new scheduled task or new research worker.

If pure Combined evaluation fails, the error is retained for the existing
Combined lifecycle point; no second calculation is attempted. A serialization
or evidence-validation failure records a bounded FAILED diagnostic while
preserving the base score bundle, CVD rule input and ordinary alert path.

## Immutable source contract

The new `watch-operational-decisions-v1` block is a sibling named
`source_metadata.capture_metadata.operational_decisions`. The existing
`operational_scores` v2 contract and its consumers are unchanged. Both blocks
belong to the same immutable parent archive record. The extension carries its
own canonical hash, code versions, scan identity, computation time, original
score-bundle hash and input-universe hash.

All eight core coins receive a context entry. The compact projection preserves
selected MaxPain item identity, actual scores and averages, literal confirmation,
source/frozen liquidity evidence and missing reasons. Combined retains group
membership, signal keys/counts, qualification, ordered member identities and
the exact selected top item. Every actual Magnet evaluation retains cluster
identity and the literal result or a distinct missing/error reason; the existing
best confirmed contributor remains separately identifiable. Missing selected
items after the global scorer limit remain incomplete evidence.

Full repeated market snapshots are excluded from this projection. The separate
extension has a256KiB bound and shares the existing integer/float/zero-normalized
canonical encoding. Source clocks and hashes are validated without consulting
later observations or outcomes.

## Exact meanings

An active Combined candidate is not a delivered COMBINED_CONFIRMATION event.
Its top item is the highest-scoring retained item in its symbol/source-side
group, with the existing timeframe tie-break. It cannot be replaced by an
arbitrary coin average or another timeframe. Source SHORT implies price LONG;
source LONG implies price SHORT.

MaxPain's BELOW_SCORE, CONFLICT and UNCONFIRMED statuses are retained literally.
They are not renamed to catalog NOT_CONFIRMED or OBSERVATION. Magnet's negative,
liquidity-missing and error cases are distinct from a confirmed result. An absent
historical block or absent confirmed-only map is never a false predicate.
Original raw/frozen liquidity absence remains separate from a numeric zero;
Combined liquidity lists cannot substitute for the selected item's amounts.

## Projection and rollout

Migration055 adds only `research_watch_scan_decision_captures`, a read-only view
with one row per accepted coin. Old rows stay NOT_CAPTURED. Source linkage is
reported separately from capture status; SOURCE_BOUND means identity matches,
not that a future consumer may skip canonical/semantic validation. Every row
requires `research_watch_decision_capture.validate_bundle` before formula use.
Computed time is exposed as text so malformed data cannot crash the entire SQL
view. No row is declared a delivered alert or prospective formula evidence.

Apply only migration055 after full CI, deploy between normal Watch cycles, then
verify the next scheduled capture through archive and PostgreSQL readback.
Compare all pre-existing coin/timeframe formula payload digests at a fixed source
cutoff. Keep their pointers, coverage, independent BTC-wave count and scheduling
contracts unchanged. No historical context is manufactured or backfilled.
