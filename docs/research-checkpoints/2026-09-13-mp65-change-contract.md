# Alert changes: separate operational repair from a formula hypothesis

Status: reviewable implementation contract; NOT IMPLEMENTED OR DEPLOYED by this research job.

## Frozen V1 semantics

- Input: actual MAX_PAIN_SCORE_65 transition items, original ordering.
- Max Pain >=65; invert source pain side once into expected price direction.
- Each available, FRESH, PASS/WARNING flow module: adjusted signed score aligned with price direction >65.
- Freeze the first both-total-CVD qualifying card per symbol per Watch, then test Futures short quality >=0.65 and matching direction. Do not substitute a later card after short failure.
- Preserve the existing OI calculation, weights, quality penalty, historical scores and original result contract.

## Operational repair

Persist existing transition state in the production database with explicit policy/version and subscription scope. Persist the transition and an outbox intent atomically. Use a unique event/episode key to make repeated processing idempotent; a new Watch ID alone does not prove a new eligibility episode. Serialize concurrent updates or use compare-and-swap. Record delivery attempts and acknowledgment timestamps separately. A Telegram timeout can be ambiguous: database idempotency does not guarantee exactly-once delivery at an external API.

At cold start without trusted state, bootstrap explicitly instead of inventing a false-to-true crossing. Do not reset state on database/data failure. Do not block the existing Watch when research persistence fails; flag an evidence gap. A missing observation must not silently become a negative observation. Preserve legitimate new events after a documented reset according to the existing reset threshold.

Required regression evidence: same Watch retried, process restart with signal still true, true→false→true, unknown between two true observations, competing workers, database failure, Telegram timeout after possible delivery, separate subscribers, legitimate policy version change. Prove both no spurious transition and no suppression of a real rearmed transition.

## Separate V2 measurement hypothesis

- New ID/version; evaluate every complete Watch snapshot, not merely 65-crossing cards.
- Persist raw_weighted_score at calculation time as a distinct field from adjusted score, with quality/freshness and source timestamps. Never remove quality gates or substitute zero for unavailable input.
- Do not reconstruct authoritative live raw scores by dividing adjusted score by .75. The fallback with no windows must be explicitly unsupported for V2 unless a separate validated policy is defined.
- Specify per-symbol/per-direction state, arbitration order, reset policy, and the handling of symbol absence before collecting a prospective cohort. Freeze the first both-CVD eligible card before the short filter.
- Archive TRUE/FALSE/UNKNOWN for every evaluation, source completeness, formula version, selected card lineage and timestamps. Retrospective card-only history cannot prove exact all-Watch transition history.
- Initially collect research evidence separately; no inheritance of V1 or mixed historical win rate. Do not report retrospective observed-card time as an actual V2 delivery time.
- Lock entry-price source and event-time rule, all eight movement thresholds, tie handling, horizon and BTC-wave independence before judging outcomes.

## Family 240m

Preserve each original formula and endpoint outcome. Group overlapping candidates for multiplicity accounting; do not merge their 116 memberships into independent samples. Correct SHORT factor signs. A new score or threshold requires the full feature universe and a future holdout; selected-family union statistics alone cannot validate it. Do not relabel four-hour endpoint successes as barrier successes or promote to Ordered V7 without its full acceptance policy.
