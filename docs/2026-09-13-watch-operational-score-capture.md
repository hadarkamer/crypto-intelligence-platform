# Stage 7: exact operational score capture

Every shared Watch scan now freezes the existing operational calculation for
BTC, ETH, SOL, HYPE, DOGE, ZEC, BNB and XRP before score thresholds, Top8 display
filtering and the global 500-item limit. This is capture coverage, not a new
formula or an expansion of Ordered V7's delivered-alert research population.

## Source and durability

The compact block is `source_metadata.capture_metadata.operational_scores` in
the existing `research_max_pain_snapshot_sets` row. Its version is
`watch-operational-scores-v2`; population is
`all-top8-watch-scans-before-display-v1`. No migration, table, service, raw
history duplication, Sheets lane or Telegram event is introduced.

One archive transaction commits the raw set, symbol manifests, rows and score
block. Its existing immutable key and payload hash reject altered retries.
The caller retries the identical payload once on a transient DB error. A
successful commit survives process restart and is the durable downstream work
source; health counters are diagnostics, not the source of truth. A prolonged
DB outage or termination before commit can still leave a capture gap. Failed
commits are logged and counted; they are never reported as persisted.

Both raw collection and operational preparation run before the immutable
payload is hashed. The official research price overlay still runs concurrently
with derivatives readiness. Watch's Max Pain scorer runs once. Its optional
capture sink sees both sides before the display limit, while the returned
items retain their original scores, ordering, limit and selection rules.
The one shared derivatives snapshot serves capture and all Watch consumers.

## Frozen fields

- Four additive Max Pain components and their named subcomponents, exact
  scores, selection, targets, distances, gap consensus, cluster geometry,
  membership and growth transitions: seven timeframes × two liquidation sides.
- Price+OI, Futures CVD and Spot CVD: exact operational totals, weighted score
  before quality adjustment, availability, freshness/quality and complete
  scalar time-family contributions/members. Derivative scores are stored once
  per coin, not once per Max Pain timeframe.
- Operational reference price/route and observed/fetched timestamps; shared
  derivatives input hash and source/window timestamps. No raw CVD/OI histories
  are copied. Hashes of the complete scoring input universe and relevant code
  files identify the calculation even when code changes later.
- Missing amounts stay absent; observed zero stays zero. Missing rows and
  inactive targets have null scores and explicit statuses. Unavailable models
  retain their operational fallback score together with `available=false` and
  `capture_status=UNAVAILABLE`; that zero is not an observed neutral score.

`COMPLETE` means the eight coin input/model blocks were captured; it does not
mean every model or target was available, fresh or eligible for research.
Read slot/model availability, source quality and `source_time_errors` separately.
The bundle is bounded at 256 KiB. Validation failure records a small `FAILED`
block and preserves valid Watch output and the raw archive. Operational
preparation failure also preserves the raw archive before reporting Watch's
failure. Passive scans keep their original raw-only semantics.

HYPE operational PERP/Spot/fallback identities remain exactly as observed.
The official research overlay remains Hyperliquid Spot @107. Neither source is
relabelled to match the other; consumers must choose a compatible price policy.
`source_side` is the liquidated side: SHORT implies an upward target and LONG
a downward target; it is not the recommended trade direction.

## Downstream boundary

A future versioned consumer must use the parent archive identity/hash and
require `max(available_at_utc, created_at_utc) <= decision_time`. The inner
`computed_at_utc` is an observation time, not proof of durable availability.
Unknown/future source times are explicitly marked and must not become
prior-only evidence. Do not update old immutable snapshots, replay old scores
as real-time scores, or treat repeated identical states as independent waves.

Version 2 normalizes integral floating-point values (including negative zero)
before JSON serialization and hashing. PostgreSQL JSONB removes negative-zero
signs and expands exponent notation; the previous v1 representation could
therefore fail a readback hash check despite equal numeric scores. The v2
`hash_version` is `json-integer-float-zero-normalized-v1`. No decimal rounding
is applied. Initial v1 rows remain immutable audit records; new consumers must
require v2 plus a verified hash. The PostgreSQL test includes negative zero and
large exponent values and verifies the hash after committed readback.

This stage does not inject these blocks into old neutral anchors, the 298
formula catalog, Ordered V7, outcome fan-outs or Sheet current views. The next
research step can consume this separately versioned population after coverage
has been verified.

[Stage 8 source audit preparation](2026-09-13-operational-score-source-audit.md)
defines the bounded read-only inspection path: retain all v4 anchor attempts,
validate exact frozen pairs/bundles, select the latest durably-prior
`WATCH_SHARED` archive row before requiring and validating its v2 block, and
retain missing outcome/BTC evidence. Preparing that adapter does not run a
database audit or connect it to runtime consumers.

## Checks

The focused self-test verifies exact displayed item equality, one Max Pain
calculation per side, all Top8 scores surviving an actual 500-item cut, quality
adjustments, absent/zero/inactive distinctions, immutable detached payloads,
HYPE source separation, no additional source reads or silent-Watch messages,
raw capture on scoring/derivative failure and identical bounded retry.
CI also uses a disposable local PostgreSQL database with migration 007 to
verify committed readback, idempotency, collision rejection and full rollback
when a child row fails. No production database is used for tests.
