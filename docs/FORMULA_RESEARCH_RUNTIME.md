# Formula Research Runtime v4 — adaptive evidence

## Objective

Find reproducible decision-time conditions that currently precede a useful
LONG or SHORT bias. A formula may qualify through high directional probability
or through strong favorable/adverse movement asymmetry. It does not need to be
perfect, and a deep adverse tail is disclosed rather than automatically used
to reject an otherwise material edge.

## Evidence contract

- Inputs: immutable Research Event state and/or neutral archived raw Price/OI,
  Futures CVD and Spot CVD observations after their source candles closed.
- Labels: closed canonical spot one-minute paths at 1h, 4h, 12h and 24h.
- Future price movement, MFE and MAE are labels only. They are never formula
  conditions, Shadow match inputs or decision-time width-calibration sources.
- Binance Spot USDT is the default route. HYPE uses Hyperliquid HYPE/USDT spot
  (`@107`). The exchange and pair stay explicit in every outcome.
- The first partial minute after the alert is excluded.
- Historical price candles may be imported/backfilled from the canonical
  exchange APIs when source, market, pair, resolution, method and quality are
  retained. This is different from importing an old Telegram message that
  lacks its full immutable decision-time state.
- Complete Research Events begin on 2026-08-28. Older Telegram exports may be
  imported only into the isolated legacy-message table and are not training
  rows by default.
- The shared production session definition is ACTIVE from Sunday 18:00 ET
  through Friday 20:00 ET and WEEKEND otherwise. It is evaluated in
  `America/New_York`, including DST. Every input window and future outcome
  horizon carries its own exact ACTIVE/WEEKEND composition.
- CoinGlass 30-minute timestamps are interval-open timestamps. Replay exposes
  a row only after the 30-minute close plus a two-minute provider grace; its
  canonical spot path begins after that safe decision time.
- Technical metadata such as archive sample counts, point age, completeness,
  schema versions and raw UTC/DST offsets is diagnostic only and cannot become
  a formula predicate.
- Statistical evidence is counted in outcome-blind Market Episodes, not raw
  alerts. After exact frozen cohorts collapse, the first forecast start opens
  a fixed 24-hour window (or the formula horizon if longer). All symbols and
  later matches inside it remain auditable but carry total evidence weight one.
  A new episode can start only when that forecast window no longer overlaps.
- Market Episode counts are formula-local. Counts from different formulas or
  horizons must never be added as if they were independent proof. Formula
  family deduplication separately compares their compact time intervals so
  shifted formulas supported by the same broad moves collapse to one champion.
- Discovery reads a rolling 120-day archive by default. Current relevance uses
  the latest 21 days with a 14-day half-life after episode collapse. Both raw
  recent counts and Kish effective sample sizes are reported.
- Absolute dollar CVD changes remain auditable inputs but are forbidden as v7
  discovery predicates across symbols. Discovery uses prior-only, same-symbol,
  session-composition-matched percentile forms instead. This prevents a dollar
  cutoff learned from a large market from being treated as equivalent evidence
  for a smaller market.

### Stage-4 experimental eligibility contract

The Stage-4 experimental path uses one atomic eligibility gate. A pattern may
create an experimental formula only when the outcomes of at least five
independent occurrences of that same pattern already show good directional
probability and/or clear favorable directional asymmetry. The occurrences are
the sample base for that probability/asymmetry decision; probability is not a
separate later promotion gate.

An independent occurrence is one opportunity from an independent parent market
wave. Multiple symbols or timestamps from the same wave are not separate
evidence, and this path does not use a fixed 24-hour spacing rule.

### Stage-4 no-signal carrier and candidate search

The Stage-4 evidence path completes the missing outcome and candidate-search
contracts without granting Formula, LIVE or trading authority:

- Migration `026_stage4_no_signal_outcomes_v1.sql` adds one append-only outcome
  carrier for each exact `projection × symbol × direction × horizon` cell that
  is both `COMPLETED`/`EVALUABLE` and proven to contain no signal. Its reference
  price comes from the frozen decision-time archive, and its future fields come
  only from a complete closed canonical one-minute path.
- Migration `027_stage4_signal_outcome_scan_state_v1.sql` adds owner-only,
  mutable operational cursor state for the Stage-4 signal outcome due queue.
  Each finite lap freezes an upper `(decision time, event id)` key, survives a
  process restart and revisits invalid candidates on the next lap. The state is
  an examined-position cursor, not proof that an outcome was written: a crash
  after scanning can delay an idempotent retry only until the next finite lap.
  It is neither evidence nor Formula, delivery, LIVE, Telegram or trading
  authority.
- The carrier writer must connect directly as the unprivileged, `NOINHERIT`
  login role `research_stage4_no_signal_outcome_writer_v1`, using only
  `RESEARCH_STAGE4_NO_SIGNAL_OUTCOME_DATABASE_URL`. The authoritative corpus
  reader remains isolated behind `research_formula_exploration_reader_v1` and
  `RESEARCH_FORMULA_EXPLORATION_DATABASE_URL`; it attests the view shape,
  ownership, dependencies, triggers, ACLs and source receipts before reading.
- `stage4-static-no-dwell-favorable-movement-label-v1` labels a completed parent
  occurrence when directional MFE reaches the versioned static floor for its
  horizon. A wick touch is enough; there is no dwell or later-survival
  requirement. `stage4-btc-parent-first-opportunity-v1` freezes the first
  matching opportunity per BTC parent movement before inspecting its outcome.
- `stage4-experimental-candidate-search-v2` applies the atomic gate above to
  the same completed sample: at least five independent parent-wave occurrences
  and probability and/or favorable/adverse asymmetry already passing on those
  occurrences. It makes no control-relative or holdout claim. Exact-binomial
  and Benjamini-Hochberg values are disclosure-only for this experimental path.
- The search is bounded to three conditions, 256 evaluated candidates and 40
  returned candidates per invocation. Formula Worker first consumes the full
  120-day Stage-4 keyset traversal in one read-only `REPEATABLE READ` database
  snapshot, then runs candidate search exactly once over the resulting corpus.
  The traversal fails closed unless EOF is proven. It is capped at 64 pages,
  8,192 projections, 131,072 observations and a 240-second default wall-clock
  budget; no partial corpus enters search. Validated rows are detached from the
  receipt as frozen slot-based compact observations, bound by an ordered chain
  hash. Candidate output retains only occurrence counts, a streaming evidence
  hash and a fixed audit sample; it never retains full occurrence arrays.
  Equivalent match sets share one evidence calculation while remaining
  separate hypotheses for multiple-testing disclosure. The display result may
  collapse candidates with the same historical match set, but
  `eligible_candidate_variants` retains every bounded eligible condition set;
  two historically equivalent variants can differ on a current snapshot.
- The search result itself retains `formula_registry_effect=NONE`,
  `delivery_channel=NONE`, `live_eligible=false`,
  `telegram_delivery_allowed=false` and `trade_execution_allowed=false`.
  When the isolated experimental path below is enabled, the same verified raw
  result may also be persisted in its dedicated search-run table. It never
  writes the Formula registry, Shadow state, LIVE approval or LIVE delivery
  queue.

The source carrier, reader and candidate search create no Telegram message,
LIVE formula or automated trade by themselves. Migration `028` adds a separate
downstream experimental-only delivery authority under the contract below.

### Isolated Stage-4 experimental Telegram path

Migration `028_stage4_experimental_telegram_v1.sql` adds a durable, isolated
experimental search registry, alert registry, opt-in registry, outbox and
two-phase delivery-attempt audit. This repository contract does not state that
the migration is applied or the path is enabled in Production; deployment and
schema state must be verified independently before rollout.

Eligibility and current matching are fail-closed:

- A candidate must pass the single atomic Stage-4 gate: the same pattern has at
  least five completed independent occurrences, where independence is a
  distinct BTC parent market movement, and those same occurrences already show
  good directional probability and/or favorable/adverse asymmetry. Five
  symbols or timestamps from one market wave remain one occurrence. There is
  no fixed 24-hour spacing rule and no second probability gate after the
  five-occurrence test.
- The route is not limited to Max Pain. Every condition set that the versioned
  bounded Stage-4 search marks eligible can be evaluated, including a single
  feature/family output, a Combined/composite output, or an allowed combination.
  The source-closure policy still rejects dependent-family stacking and prevents
  a composite and its own source evidence from being counted twice.
- Current matching consumes `eligible_candidate_variants`, not only the
  display-collapsed champions. It chooses the highest-ranked currently matching
  variant once per `symbol × direction × horizon × BTC parent movement`; the
  same trigger key is idempotent, so repeated polls of one wave do not create
  new evidence or duplicate alert occurrences.
- `load_latest_authoritative_stage4_current()` selects and hydrates the newest
  terminal Stage-4 projection in one attested, read-only `REPEATABLE READ`
  snapshot. Its detached compact cells contain only frozen decision-time
  features, source fingerprints and Wave-v5 binding; it does not request or
  expose any outcome. A newest missed-causal-window or unevaluable projection
  blocks the current cycle and never falls back to an older completed
  projection.
- The downstream matcher accepts a current Stage-4 snapshot only within 45
  minutes of its decision time, requires the eligible search to be no older
  than twice its horizon cadence, and expires a newly constructed alert 35
  minutes after decision time. Invalid receipts, missing Wave binding, stale
  data, unrecognized features or a failed condition produce no alert.

Each message shows direction, symbol, horizon, exact formula conditions,
independent BTC-parent-wave count, the accepted probability and/or asymmetry
metrics, and the recorded reasons the evidence is still experimental, including
the lack of an independent holdout/control-relative claim. The exact text
`ניסיוני, לא מאושר למסחר` appears at both the opening and closing boundary of
the message. An alert has `delivery_channel=TELEGRAM_EXPERIMENTAL_ONLY`, while
`formula_registry_effect=NONE`, `live_eligible=false` and
`trade_execution_allowed=false` remain immutable.

Subscription authority is independent from LIVE. A Telegram chat must execute
`/ai_experimental_on`, which records command-based consent and acknowledgement
of the exact disclaimer. `/ai_experimental_off` disables only this channel and
`/ai_experimental_status` reports its separate schema, worker, Telegram and chat
state. `/ai_alerts_on` and existing LIVE subscriptions are never imported or
reused. After explicit chat opt-in, no per-formula human approval is required
for an experimental alert; that does not approve the formula for LIVE or for
trading, and the bot never executes a trade.

Delivery uses a content-addressed outbox and an atomic `FOR UPDATE SKIP LOCKED`
claim lease. Only chats that were already opted in when an alert occurrence was
created receive a queue row; enabling a subscription does not backfill older
alerts. Every claim and terminal result is append-only audited. If the worker
cannot prove whether Telegram accepted a send, or if an `IN_FLIGHT` lease
expires after a possible send, the delivery becomes terminal `AMBIGUOUS` and is
never retried automatically. Exponential retry is permitted only for a failure
that is known to have occurred before sending; retries stop at the configured
attempt limit or alert expiry. A successful Telegram send whose database
completion fails remains leased and later becomes `AMBIGUOUS`, preventing a
known duplicate resend.

## Discovery

For each horizon and direction, the engine:

1. sorts verified observations chronologically;
2. freezes approximately 49% as initial Fit, 21% as three chronological
   Walk-forward Selection folds and the latest 30% as untouched Test, without
   ever splitting rows that share the same timestamp;
3. learns candidate structures and quantile positions from Fit only, then
   re-fits numeric thresholds on an expanding prior-only prefix before each
   Selection fold;
4. evaluates single, pair and triple conditions in a bounded search by default;
5. only when `FORMULA_DISCOVERY_HIERARCHICAL_ENABLED=1`, expands a bounded
   beam of stable triple parents to four and then five conditions. A nested,
   timestamp-safe Fit/Walk-forward Selection process selects that hierarchy
   and requires incremental gain in both parts; the outer Test is not inspected
   until the complete hypothesis family has been frozen;
6. prevents correlated-family stacking without a frozen written exception and
   always forbids combining composite Max Pain evidence with its components;
7. tests probability and asymmetry for every candidate, then corrects all
   `2 × candidate` hypotheses together in one Benjamini-Hochberg family before
   final holdout validation begins. Each route keeps its own mapped q-value;
8. collapses exact/overlapping evidence fingerprints into deterministic formula
   families and persists one champion per family. Single-coin effects remain
   eligible; cross-symbol breadth is not a family-grouping requirement;
9. compares each candidate with its same-direction complement after matching
   the outcome horizon's ACTIVE/WEEKEND composition with triangular weights;
10. blends 70% cumulative historical quality with 30% current relevance. Both
    parts are route-aware. Risk, speed, width, p90/p95 MAE, sample reliability
    and stability remain visible. Weekend calibration may adjust only the
    absolute favorable-width floor.

Small samples remain visible but cannot become research-ready. Historical
holdout acceptance requires common independence, currentness, width, baseline
and data-integrity gates, then either:

- Probability: at least 60% current weighted hit rate, weighted Wilson lower
  bound 45%, at least five percentage points over controls, MFE/MAE at least
  1.10 and joint-family q at most 0.20.
- Asymmetry: hit rate at least 45%, favorable excursion exceeds adverse in at
  least 70% of valid pairs, dominance Wilson at least 40%, at least five points
  over paired controls, MFE/MAE at least 2.0, positive paired median edge and
  joint-family q at most 0.20.

Prospective research readiness uses the stricter probability floors 65%/50%
and asymmetry Wilson floor 45%, with at least 12 independent matches, 12
independent controls and six effective recent match/control episodes. Three to
five effective recent episodes may be labelled `EARLY_CURRENT_EDGE`, never
`RESEARCH_READY`. Missing current p90 or p95 MAE fails closed; a known deep p90
tail is a mandatory warning, not a standalone rejection.

The former 72-hour/three-UTC-date rule is not a research-maturity gate because
Market Episodes now prevent one rise from creating repeated proof. It remains
unchanged inside the separate legacy owner/LIVE approval contract until that
contract is explicitly redesigned and migrated.

## Lifecycle and safety

- Discovery: `DISCOVERED`, `BACKTESTED`, `HOLDOUT_PASSED`, `SHADOW`.
- Automatic rolling evaluation has a hard ceiling of
  `SHADOW_PENDING_EXPLICIT_APPROVAL`. This is an observational readiness state;
  it never changes a formula to `APPROVED` or `LIVE`.
- Registry and Shadow status expose `RESEARCH_READY`, `EARLY_CURRENT_EDGE`,
  accepted route, missing gates, rolling metrics and episode counts. Current
  relevance is a separate versioned axis with explicit hysteresis states; it
  never mutates research maturity or delivery authority. The old LIVE-review
  readiness is reported separately and is not an alias for the new research
  contract.
- After enough genuinely future evidence exists, a separate prospective review
  freezes a predeclared cutoff and evaluates that fixed sample. Only an
  explicit, immutable human approval may then activate `APPROVED` or `LIVE`.
- A Shadow formula starts with the latest existing event ID and evaluates only
  genuinely future delivered alerts.
- Every check and match is idempotent and auditable.
- Shadow hits remain auditable and are never sent before validation.
- A LIVE match creates a durable delivery only for Telegram chats that opted in
  with `/ai_alerts_on`. One AI trade alert is queued per event per chat.
- A Stage-4 experimental match is neither a Shadow hit nor a LIVE match. It may
  create a durable message only through migration `028`'s isolated outbox, the
  experimental runtime flag and a chat's separate `/ai_experimental_on`
  consent. It never changes a formula lifecycle stage or owner approval.
- Alerts are informational; there is no automatic trade execution.

## Operational change protocol

Every change is handled as one explicitly approved stage with a fixed scope and
exit criteria. GitHub, Render and PostgreSQL baselines are collected once and
in parallel before that stage. If no external fingerprint changes, later
preflight checks verify only the branch SHA/tree, active deploy and critical
schema/watermark instead of repeating the full baseline.

- Classify findings as a live fault, expected warning, historical error or
  evidence that is still maturing. A live fault interrupts the planned stage:
  apply the smallest isolated correction, verify it, report it and stop for
  approval before continuing.
- Keep one writer and one coherent commit per stage. Do not mix a fault fix,
  cleanup, refactor and capability expansion. Prefer an existing file or
  function; add a boundary only when ownership or an independent self-test
  requires it.
- Run targeted checks while editing, then compile every tracked Python module
  and run every tracked `*_selftest.py` exactly once after the patch stabilizes.
  Do not repeat the full suite unless the code changes again.
- Move the same verified commit to both production branches, allow their
  deploys to proceed in parallel and perform one bounded post-deploy check:
  exact SHA, no new current-deploy error, preserved database invariants and one
  startup for every required worker. Documentation-only checkpoints do not
  justify a deploy.
- When future evidence is required, close the stage as `WAITING_DATA` with the
  earliest UTC recheck, required horizons, missing eligible sample count and
  exact database watermark. Do not poll repeatedly before that condition.
- Every stage report states what was checked, what changed, commit/deploy/schema,
  tests passed, what deliberately did not change, data still maturing and the
  single next step requiring explicit approval. LIVE Telegram delivery remains
  forbidden without frozen prospective review and explicit owner approval.
  The separate Stage-4 experimental channel follows migration `028`'s atomic
  evidence, current-snapshot and explicit chat opt-in contract instead; it
  cannot satisfy or bypass any LIVE gate.

A chat handoff uses the same compact checkpoint: verified UTC, common branch
SHA/tree, live deploy IDs, database/schema watermarks, current stage and gate,
known expected warnings, unfinished work and the one approval required next.
Live values belong in that timestamped handoff, not as mutable status in this
runtime contract.

## Runtime flags

- `FORMULA_DISCOVERY_ENABLED=1`
- `FORMULA_SHADOW_ENABLED=1`
- `FORMULA_LIVE_ALERTS_ENABLED=1` enables delivery only for a formula with an
  explicit immutable human LIVE approval and an opted-in chat; it never
  promotes a Shadow formula or bypasses the frozen prospective review.
- `FORMULA_EXPERIMENTAL_ALERTS_ENABLED=1` enables only the isolated Stage-4
  evaluator/outbox after its reader, dispatcher schema and Telegram connection
  attest successfully. It does not enable LIVE and defaults disabled when
  absent.
- `FORMULA_EXPERIMENTAL_POLL_SECONDS=60` controls the independent evaluation and
  queue-drain loop; accepted values are clamped to 30–900 seconds. Evaluation
  failure does not prevent already-durable experimental deliveries from being
  drained in the same cycle.
- `FORMULA_EXPERIMENTAL_CLAIM_BATCH=20` bounds each atomic delivery claim to
  1–50 rows.
- `FORMULA_EXPERIMENTAL_CLAIM_LEASE_SECONDS=120` sets the claim lease, clamped
  to 30–600 seconds. An expired `IN_FLIGHT` lease becomes terminal
  `AMBIGUOUS`, never automatically retryable.
- `FORMULA_EXPERIMENTAL_MAX_ATTEMPTS=3` limits known-not-sent attempts to 1–10.
  `FORMULA_EXPERIMENTAL_RETRY_BASE_SECONDS=30` sets the 5–600 second bounded
  exponential retry base; each delay is capped at 600 seconds and alert expiry
  always wins.
- `RESEARCH_FORMULA_EXPERIMENTAL_DATABASE_URL` must use the exact dedicated
  `research_formula_experimental_dispatcher_v1` login. There is no fallback to
  the primary Research, Formula/LIVE or Stage-4 reader connection.
- `FORMULA_DISCOVERY_HORIZONS=60,240,720,1440`
- Each horizon runs on its own UTC-aligned cadence equal to that horizon:
  hourly, every four hours, every 12 hours and daily.
- `FORMULA_DISCOVERY_SLOT_GRACE_SECONDS=300` waits five minutes after the UTC
  slot before freezing the deterministic `analysis_as_of_utc`.
- `FORMULA_DISCOVERY_IDLE_POLL_SECONDS=60` checks only durable scheduler state;
  a terminal slot is never recomputed.
- `FORMULA_DISCOVERY_LOOKBACK_DAYS=120` is the adaptive default. An explicit
  environment override remains bounded to 1–3650 days.
- `FORMULA_DISCOVERY_HIERARCHICAL_ENABLED=1` explicitly enables the bounded
  stable-parent four/five-condition beam; absent/false preserves the default
  single/pair/triple search.
- `FORMULA_SHADOW_POLL_SECONDS=60`
- `RESEARCH_HEAVY_STATEMENT_TIMEOUT_MS=120000` controls only bounded heavy
  Research reads and the explicit Discovery/Shadow heavy scopes. It is always
  finite and clamped to 30–300 seconds; Formula control paths remain at 20
  seconds, and heavy Formula writes retain a three-second lock timeout.
- `RESEARCH_STAGE4_DUE_SCAN_MAX_PAGES=4` bounds the Stage-4 signal due scan to
  1–16 candidate pages per worker cycle. Each page has at most 256 candidates
  and at most two heavy statements (candidate plus authoritative hydration).
- `RESEARCH_STAGE4_DUE_SCAN_BUDGET_MS=240000` bounds the same scan's monotonic
  wall-time budget to 30–600 seconds. Before every heavy statement PostgreSQL's
  timeout is reduced to the smaller of the remaining cycle budget and
  `RESEARCH_HEAVY_STATEMENT_TIMEOUT_MS`.
- When both closed due queues have work and the cycle limit is at least two,
  the merge reserves at least one quarter of the configured capacity for each
  queue. The Production default is 200; a diagnostic caller that explicitly
  requests a one-row cycle cannot provide bidirectional fairness.
- `PROSPECTIVE_ANCHORS_ENABLED=1` opts the production service into the silent,
  UTC-minute-aligned prospective sampler. Each eligible 30-minute slot is
  idempotent and persists an atomic LONG/SHORT `DECISION_SAMPLE` pair only when
  the Research schema and all required decision-time sources are valid.
  Missing official prices remain missing per symbol; this flag never enables
  Telegram delivery or LIVE promotion.
- `FORMULA_DISCOVERY_DATASET_MODE=auto` prefers the neutral historical replay
  only after its minimum coverage gate; `alerts` and `historical_replay` are
  explicit bounded operator overrides.
- `RESEARCH_STAGE4_NO_SIGNAL_OUTCOME_DATABASE_URL` must contain credentials for
  the dedicated `research_stage4_no_signal_outcome_writer_v1` role. When it is
  absent, no-signal enrichment is skipped without borrowing the primary or
  legacy Research writer connection.
- `RESEARCH_FORMULA_EXPLORATION_DATABASE_URL` contains the dedicated
  read-only Stage-4 corpus credentials. `FORMULA_STAGE4_CORPUS_PROJECTION_LIMIT`
  bounds each full-traversal keyset page to 128 projections by default.
- `FORMULA_STAGE4_CORPUS_WALL_BUDGET_MS=240000` bounds the full Stage-4 corpus
  traversal and local aggregation to 30–600 seconds. Before every source query,
  PostgreSQL `statement_timeout` is reduced to the smaller of 20 seconds and
  the remaining traversal budget. Page, projection, observation, cursor, hash,
  snapshot, deadline or EOF failure yields no candidate-search input.
- `FORMULA_STAGE4_CANDIDATE_SEARCH_WALL_BUDGET_MS=60000` independently bounds
  local candidate search to 5–300 seconds. Expiry raises a timeout and discards
  the entire search result; no partial candidate set is exposed.

Migrations `026`, `027` and `028` are rollout-gated; this document does not
assert that any of them has been applied in Production. Apply `026` after
`024`/`025`, then `027`, on PostgreSQL 15 or newer as the trusted owner of all
source relations. Provision the no-signal writer and exploration reader roles
out of band as unprivileged `NOINHERIT LOGIN` roles with no memberships and no
database/schema ownership; leave the new writer URL disabled until the schema
and ACL attestations pass. Before enabling it, preserve an export/backup, run
the full self-test and compile suites, verify the reader/schema receipts and
confirm runtime health. The reader normalizes out only `026`'s exact delegated,
non-grantable writer ACL entries from the older `024` catalog receipt; `026`
attests those entries independently, so any extra privilege still fails closed.
Rollback begins by disabling
`RESEARCH_STAGE4_NO_SIGNAL_OUTCOME_DATABASE_URL` and stopping or first deploying
a Research Outcome worker version that does not require migration `027`. Drop
the owner-only `027` scan-state table first; this loses only pagination progress.
After preserving any desired research history, use the explicit revoke/drop
sequence at the end of migration `026`. Rollback verification requires the
writer's direct schema ACL to be absent and all source-table access to be
ineffective; inherited `PUBLIC` schema
`USAGE` may remain because it conveys no table authority. A rollback never
promotes, delivers or trades a formula.

Migration `028` has a separate mandatory preflight and rollback boundary:

1. Keep `FORMULA_EXPERIMENTAL_ALERTS_ENABLED` false on every replica, preserve
   the database backup/audit export, and verify the exact code SHA and current
   schema watermark. If startup has `FORMULA_SCHEMA_APPLY=1`, provision the
   dispatcher role before deploying code that includes `028`; otherwise startup
   schema application is expected to abort at the migration preflight.
2. Out of band, create exactly
   `research_formula_experimental_dispatcher_v1` as an unprivileged
   `NOINHERIT LOGIN` role: no superuser, database/schema creation, role creation,
   replication, RLS bypass, ownership or role membership. Before migration it
   must have no authority on Research events, Formula registry, LIVE approvals,
   LIVE deliveries, LIVE subscriptions or the authoritative Stage-4 view.
   Password/secret creation is never embedded in a migration or committed file.
3. Apply `028` after `027` as the same trusted owner used by the protected source
   relations. The migration grants only the exact non-grantable table and column
   privileges needed on its five experimental tables and fails if the dispatcher
   can read or mutate a protected source/LIVE relation.
4. Configure `RESEARCH_FORMULA_EXPERIMENTAL_DATABASE_URL` with direct dispatcher
   credentials and run the migration self-test, full tracked self-tests, Python
   compile/tab checks, clean `001→028` apply, rollback/reapply/idempotency checks,
   dispatcher `schema_status()` ACL/trigger attestation and the latest-current
   no-outcome reader receipt. Confirm runtime health and Telegram connectivity
   while the experimental flag is still false; LIVE formula, approval,
   subscription and delivery counts must be unchanged.
5. Enable `FORMULA_EXPERIMENTAL_ALERTS_ENABLED=1` only after those checks pass.
   Start with an explicitly opted-in test chat using `/ai_experimental_on`, then
   verify a persisted search receipt, current trigger receipt, alert occurrence,
   two-phase claim audit and terminal delivery state. No existing LIVE chat is
   enrolled automatically.

To roll back `028`, first disable the experimental flag on every replica, stop
all experimental dispatchers and remove/revoke the dispatcher connection URL.
Preserve any required search, alert, consent and delivery audit export. Then run
the explicit dependency-ordered table/function drop and schema-`USAGE` revoke
listed at the end of the migration; remove the out-of-band login only after no
service uses it. Re-run protected-relation ACL checks and confirm the Formula
registry, Shadow, LIVE approvals, LIVE deliveries and LIVE subscriptions are
unchanged. Do not clear or retry `AMBIGUOUS` rows during rollback.

The one-shot replay is separate from the Watch loop:

- apply migration `004_historical_opportunity_replay_v1.sql` explicitly;
- set `HISTORICAL_REPLAY_BACKFILL=1` only for the backfill command;
- optionally bound symbols, dates, horizons, chunk size and anchor count;
- run `python research_historical_replay.py`;
- one-minute candles are discarded after calculation; PostgreSQL retains only
  compact reference/return/MFE/MAE/speed summaries and provenance.

The Candidate service keeps formula workers disabled. Production uses
migrations `002_formula_research_v1.sql`,
`003_formula_autonomous_alerts_v1.sql` and
`004_historical_opportunity_replay_v1.sql`, followed by
`005_formula_shadow_safety_v1.sql`, before enabling the workers.

Prospective sampling additionally requires the additive first-touch and neutral
anchor migrations (`006` and `008`). Its worker starts only after startup schema
verification succeeds. `/health` exposes its last official-price coverage,
missing symbols, persistence summary and next aligned minute without exposing
any delivery action.

Formula schema v6 retires an earlier non-LIVE cohort only after at least four
symbols independently have 250 anchors, 14 UTC dates and 336 hours of span for
that horizon. Sparse symbols are reported but excluded from discovery until
they pass the same per-symbol gate. Before that gate, a result is capped at
`BACKTESTED` and the earlier cohort remains auditable and active.

Hyperliquid's official candle endpoint exposes only its most recent 5000
candles. HYPE replay therefore uses only exact one-minute observations still
inside that window; older HYPE anchors are excluded rather than approximated or
labeled from another venue.

Formula schema v7 adds the two-route acceptance contract, rolling relevance
and Market Episodes. Existing v5/v6 Shadow formulas remain observable under
their frozen contracts. Only an exact current v7 runtime may satisfy the LIVE
runtime identity check, and Stage 1 neither creates a Telegram experiment nor
enables LIVE delivery.

## Shared evidence envelope

Infrastructure stage 2 defines one side-effect-free contract in
`research_evidence_contract.py`:

- `FormulaAssessment` freezes the already-computed acceptance interpretation.
  It never reruns thresholds and rejects any payload that implies LIVE.
- `EvidenceSnapshot` binds that assessment to an exact formula runtime tuple,
  formula-family id, matched/control Market Episode ids, parent episode ids,
  raw counts, `N_eff`, metrics and provenance. Its SHA-256 content id changes
  whenever any bound input changes.
- V7.2 snapshots are marked `CURRENT_V7`. The exact V7.1 runtime remains
  decodable under that shared v7 schema so append-only historical fingerprints
  stay readable, but consumers derive `RETAINED_V7_1_READ_ONLY` from the exact
  engine tuple: it is labeled retained, cannot become relevance-eligible and
  cannot satisfy Formula Lab's V7.2 evidence gates.
  Retained v5/v6.2 formulas use a deterministic `LEGACY_SHADOW_READ_ONLY`
  adapter and are not rewritten.
- Migration `015_formula_evidence_snapshots_v1.sql` adds an append-only storage
  table. No production worker writes snapshots in stage 2; later integrations
  must call the idempotent store explicitly.
- The envelope always has `live_eligible=false` and
  `delivery_channel=NONE`. Telegram rendering is a future consumer and may not
  recalculate probability, maturity or relevance.

The canonical fixtures are
`fixtures/evidence/current_v7_probability.json` and
`fixtures/evidence/retained_v7_1_probability.json`, plus
`fixtures/evidence/legacy_v6_shadow.json`. They prove stable interpretation,
content ids, event/family identifiers and legacy compatibility.

## Experimental evidence rendering dry run

Stage 3A adds the side-effect-free
`research_evidence_telegram_renderer.py` consumer. It fingerprint-verifies every
`EvidenceSnapshot`, renders deterministic plain Telegram text and returns audit
metadata without importing Telegram, reading the database or calling a worker.
The renderer displays only values already frozen in the snapshot; it does not
recalculate acceptance, maturity, probability, asymmetry, recency or risk.

Dry runs suppress repeated snapshot ids and emit one deterministic message for
each `formula_family_id`. The newest assessed snapshot is the displayed family
representative, while all member snapshot and formula ids remain in the dry-run
metadata. Conflicting compatibility, direction or horizon inside one family
fails closed. Both `CURRENT_V7` and `LEGACY_SHADOW_READ_ONLY` are explicitly
labelled, and every message contains “ניסיונית — אינה המלצת מסחר”. Stage 3A has
`delivery_channel=NONE`, `live_effect=NONE` and zero delivery attempts; it adds
no command, subscription, scheduler, persistence writer, Telegram call or LIVE
path.

Stage 3A intentionally left the scheduler unchanged. Stage 4 replaces that
legacy six-hour-after-runtime loop with the versioned per-horizon scheduler
described below.

## Versioned rolling relevance hysteresis

Stage 3B adds the side-effect-free
`formula-relevance-hysteresis-v2-runtime-bound` policy and connects each
distinct Shadow rolling assessment to one verified, content-addressed
`EvidenceSnapshot`.
Migration `016_formula_relevance_hysteresis_v1.sql` stores the resulting
relevance decisions append-only. Repeated one-minute polling with the same
assessment/evidence/day fingerprint is idempotent and cannot advance a streak.

The relevance states are independent of `research_formulas.active`, lifecycle
stage, owner approval and delivery:

- `OBSERVING`: current v7 evidence has not yet established relevance.
- `RELEVANT`: the frozen current acceptance contract passes.
- `WEAKENING`: one distinct weak rolling observation; the formula is not
  suspended yet.
- `SUSPENDED`: two distinct weak rolling observations; future Experimental
  consumers must block new indications while Shadow continues measuring.
- `RECOVERING`: one new strong evidence version after suspension. Reactivation
  requires another strong observation backed by a different Market Episode
  evidence fingerprint.
- `LEGACY_READ_ONLY`: retained v5/v6.2 Shadow formulas remain observable and
  can never become current-v7 relevant through this policy.

A UTC-day bucket permits recency decay to produce at most one new state
decision per day when evidence does not advance. This does not increment raw
matches, Market Episodes or `N_eff`. In particular, waiting on the same market
rise cannot provide a second recovery proof: the evidence fingerprint must
change. Formula-family and Market-Episode grouping remain the statistical
source of independence. The separate legacy 72-hour/three-UTC-date owner/LIVE
approval contract is unchanged.

Stage 3B writes no Telegram message, creates no subscription, changes no
formula stage, and has `delivery_channel=NONE` and `live_effect=NONE` throughout.
It does not change acceptance thresholds, HYPE/Max Pain rules, the scheduler,
or the frozen prospective owner-approval path.

## Discovery v7.2 Walk-forward, family guard and horizon scheduler

Stage 4 introduced the versioned Walk-forward scheduler below. Stage 5A keeps
its probability/asymmetry thresholds and scheduling unchanged, versions the
engine as v7.2, and adds the all-depth family guard:

- `formula-walk-forward-v1-expanding-refit` learns a feature/operator structure
  from initial Fit only, recalculates numeric quantile thresholds on an
  expanding prior-only training prefix before each of three Selection folds,
  and freezes the final formula before opening the outer Test.
- `market-episode-boundary-purge-v1` removes the complete outcome-blind Market
  Episode overlap at every boundary. `full-outcome-horizon-embargo-v1` then
  adds the formula's full 1h/4h/12h/24h outcome horizon. Test cannot influence
  identity, rank, family grouping or Walk-forward selection.
- `formula-condition-family-policy-v1-all-depth-fail-closed` rejects correlated
  feature-family stacking at every candidate depth, including ordinary pairs
  and triples, unless the run freezes a syntactically valid written exception.
  The policy version and written exceptions are bound into `formula_key`; the
  same frozen metadata is replayed and identity-checked by Formula Lab.
- Persistence verifies the exact current dataset/runtime tuple, recomputes each
  `formula_key` and replays the family policy for the complete cohort before
  opening a PostgreSQL connection. A malformed or conflicting formula therefore
  creates no run, evaluation, stage transition or retirement side effect. The
  run coverage audit also retains the policy version and enforcement mode when
  a valid run finds zero formulas.
- `formula-discovery-horizon-scheduler-v1` owns one fixed UTC clock per horizon.
  A five-minute grace makes the as-of time deterministic and allows due closed
  outcomes to be stored before Discovery reads them.
- A session PostgreSQL advisory lock is held for the complete computation and
  persistence of one horizon. The durable per-horizon schedule state makes
  restart and overlap skips idempotent. No recurring path executes DDL.
- The exact bounded dataset is content-hashed as
  `formula-discovery-dataset-watermark-v1`. An unchanged watermark advances the
  slot as `SKIPPED_UNCHANGED` without re-running formula search. Missing data is
  recorded once for that natural horizon slot as `SKIPPED_UNAVAILABLE`.
- Watermarks, schedule metadata and policy versions are operational/audit-only;
  they are never formula predicates. Telegram, LIVE, owner approval, the legacy
  72-hour/three-date contract, HYPE isolation and Max Pain cutover rules remain
  unchanged.
