# All-Watch research population: first implementation step

The user authorized these topics in order, one step at a time:
1. Research from every Watch scan.
2. Ongoing discovery of new formulas.
3. Complete historical archive integration.
4. Complete HYPE result integration.
5. Validate formulas on new cases.

This release implements the first bounded step of topic 1: admitting the frozen
all-Watch population for research. It does not complete outcome measurement for
every scan or change the existing 298 formula definitions. Component/cluster
hypothesis testing and the earlier B4/240m work are not resumed by this release.

## What becomes available

Migration 046 adds a durable consumer state and one intake receipt per source
snapshot/version. The source is the existing append-only operational_scores v2
bundle. Two SQL views expose 8 coin observations and all 112 timeframe/side
slots per accepted scan, including scores below 65, observed zero, inactive
targets and missing inputs. They reuse the original JSON instead of copying
large bundles or storing the same CVD/OI model fourteen times.

`research_watch_scan_observations` provides all coin model/source/quality blocks.
`research_watch_scan_score_slots` provides individual MaxPain slots with exact
components, original liquidation side and the once-inverted research direction.
Every view row includes source IDs/hashes, version and availability timestamps.
Missing/inactive scores stay NULL. Unavailable model fallbacks remain alongside
their explicit availability and capture status; they are not observed zeroes.

The reader checks source identity, v2/hash version, canonical hash, 8×14 slot
coverage, model availability, score/component integrity and parent timing.
Malformed/failed/legacy captures receive visible rejection receipts. A valid
PARTIAL bundle is retained with its missing data; no global score/quality gate
silently removes inconvenient observations from the population.

## Time and population contract

`observed_at_utc` is the original computation time. `usable_from_utc` is the
latest of computation, parent availability and parent creation time. A future
research consumer must choose a decision at or after usable_from; this field
is not a trade entry or proof of a price at that exact instant.

The first consumer activation time is stored in PostgreSQL. Earlier observations
remain HISTORICAL_CAPTURE, even when their source transaction commits later.
FORWARD_CAPTURE requires both new observation and new source availability. It
still does not qualify as prospective formula evidence: a formula must first
freeze separately and then use complete later BTC waves under its own policy.
Future-dated or incoherent source availability receives a rejection receipt
rather than silently becoming historical training evidence later.

Source time errors and each model's freshness/availability remain explicit.
ACCEPTED means structurally admitted observation, not a complete price path,
eligible formula, independent market wave or a successful trading result.
HYPE observations preserve the exact operational route; they are included in
the population. The separately planned outcome adapter will choose and label
the correct price series; this release does not relabel HYPE as Spot.

## Bounded runtime and recovery

Each pass uses an advisory transaction lock and a finite cyclic PK page, plus
two newest source IDs. At most 64 small source headers and six candidate bundles
are considered. A partial source index skips raw-only legacy/passive scans.
Four captured sets advance the historical lane per pass. A frozen high-water
ends each lap; later laps recover transactions whose smaller IDs committed late.
Receipts and cursor updates commit atomically. Existing receipts suppress replay.
No network call or schema mutation occurs in the recurring worker.

The worker runs every 30 seconds and starts/stops with the existing independent
research workers. `RESEARCH_WATCH_SCAN_INTAKE_ENABLED` can override activation;
otherwise it inherits the already authorized outcome-enrichment switch. Missing
migration 046 disables this worker while the existing collection continues.
Health exposes progress and failure counts under `watch_scan_intake`.

## Rollout and verification

Apply only `046_watch_scan_research_intake.sql` through the existing targeted
installer. Preserve the existing Watch schedule and make one code deployment
after tests pass. Verify actual receipts/views, exact source preservation,
low/missing/inactive observations, no duplicate intake after another pass,
and unchanged formula/Telegram behavior. Local environments without the project
dependencies/PostgreSQL use CI's complete dependency set and PostgreSQL 18 gate.

The next step in topic 1 is an explicitly versioned measurement adapter for
these observations, selecting causal entry prices, fixed horizons and BTC-wave
membership without creating delivered alerts. The remaining authorized topics
stay queued in the order above; this release does not claim they are complete.
