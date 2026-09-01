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
  single next step requiring explicit approval. LIVE or Telegram delivery
  remains forbidden without frozen prospective review and explicit approval.

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
- `PROSPECTIVE_ANCHORS_ENABLED=1` opts the production service into the silent,
  UTC-minute-aligned prospective sampler. Each eligible 30-minute slot is
  idempotent and persists an atomic LONG/SHORT `DECISION_SAMPLE` pair only when
  the Research schema and all required decision-time sources are valid.
  Missing official prices remain missing per symbol; this flag never enables
  Telegram delivery or LIVE promotion.
- `FORMULA_DISCOVERY_DATASET_MODE=auto` prefers the neutral historical replay
  only after its minimum coverage gate; `alerts` and `historical_replay` are
  explicit bounded operator overrides.

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
- Current v7 snapshots are marked `CURRENT_V7`. Retained v5/v6.2 formulas use
  a deterministic `LEGACY_SHADOW_READ_ONLY` adapter and are not rewritten.
- Migration `015_formula_evidence_snapshots_v1.sql` adds an append-only storage
  table. No production worker writes snapshots in stage 2; later integrations
  must call the idempotent store explicitly.
- The envelope always has `live_eligible=false` and
  `delivery_channel=NONE`. Telegram rendering is a future consumer and may not
  recalculate probability, maturity or relevance.

The canonical fixtures are
`fixtures/evidence/current_v7_probability.json` and
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
`formula-relevance-hysteresis-v1` policy and connects each distinct Shadow
rolling assessment to one verified, content-addressed `EvidenceSnapshot`.
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

## Disabled Experimental gate preparation

`research_experimental_delivery_gate.py` is a side-effect-free preparation for
Stage 6. It consumes fingerprint-verified `EvidenceSnapshot` values, the exact
snapshot-bound relevance decision and the existing deterministic family Dry
Run. It simulates the independent Experimental gates for Stage-5 readiness,
current relevance, test-chat allowlisting/separate opt-in, an engaged-by-default
kill switch, idempotency and a caller-supplied cooldown.

The module is deliberately not imported by `main.py`, `ai_telegram.py` or the
Formula worker. It reads no environment variable or database, imports no
Telegram client, creates no subscription and performs no delivery. Even a
simulated eligible result reports `delivery_channel=NONE`, `delivery_attempts=0`,
`database_writes=0` and `live_effect=NONE`. No cooldown duration is invented by
the preparation: a positive value must be supplied by the future, explicitly
approved Stage-6 integration. Legacy Shadow remains read-only, and Stage 5 must
report `READY` before a current-v7 family can even be simulated as eligible.

Each simulated family decision and its enclosing batch now receive a canonical
SHA-256 audit id bound to the exact snapshot family, relevance decision, gate
policy, blockers, rendered-message hash and evaluation time. Replaying identical
inputs produces identical ids; changing a relevance decision or gate outcome
changes them. The audit contract explicitly reports
`research_evidence_writes=0` and `research_evidence_effect=NONE`: displaying or
simulating an Experimental indication cannot add a match, Market Episode or
independent proof, so several alerts from one rise cannot validate one another.
These records remain returned values only; no persistence was added.

`research_experimental_storage_contract.py` is the next disabled preparation.
It fingerprint-verifies the full gate batch and every family decision, enforces
the zero-delivery/zero-evidence invariants again, and returns canonical batch
and decision rows without opening a database connection. The additive schema
artifact `018_formula_experimental_audit_v1.sql` defines append-only audit
tables, but is deliberately absent from `research_formula_schema_admin.py` and
therefore cannot be applied by the existing schema command. Neither file is
imported by a production surface. No subscription, queue or delivery table was
added, and the preparation still reports `database_writes=0`.

`research_experimental_preview_contract.py` defines a separate, disconnected
`PREVIEW_ONLY` authorization simulation for a test chat while Stage 5 is
`WAITING_DATA`. It first fingerprint-verifies the complete normal Experimental
gate through the storage contract and can be simulated as eligible only when
that gate is suppressed solely by `Stage 5 is not READY`. Any relevance,
compatibility, idempotency, cooldown or authorization blocker remains fatal.
The preview requires a separate disabled-by-default flag, engaged-by-default
kill switch, explicit owner approval and its own test-chat allowlist. Public
opt-in is forbidden, and the route closes once Stage 5 becomes `READY` so the
normal Experimental review cannot be bypassed. Every preview is labelled
“PREVIEW טרום־אימות”, has separate content-addressed idempotency, and reports
zero delivery, database, research-evidence and LIVE effects. The contract is
not imported by any production surface and adds no command or Telegram call.

`research_experimental_preview_storage_contract.py` adds the matching pure
storage boundary. It verifies every preview decision, message hash, separate
idempotency key and enclosing batch id before returning canonical audit rows.
The still-unregistered migration 018 now also defines separate append-only
Preview batch and decision tables linked to the underlying Experimental audit
ids and EvidenceSnapshot. Database constraints keep public opt-in and Stage-6
activation false and keep `research_evidence_effect=NONE`. The contract performs
no write, and neither it nor the extended schema is connected to production.

`research_experimental_preview_delivery_simulator.py` exercises the next
boundary with a sealed in-memory Telegram double. It re-verifies the complete
Preview storage contract, records only `PREVIEW_SIMULATED_ELIGIBLE` decisions
for the `TEST_ALLOWLIST` route while Stage 5 is `WAITING_DATA`, and suppresses
all other decisions. Replaying the same preview key is recorded as a duplicate
rather than a second message. The double has no Telegram-compatible sending
method and the simulator accepts no substitute client, token, environment or
network path. Its transcript remains test data only: delivery attempts,
Telegram API calls, database writes, research-evidence writes and LIVE effects
all stay zero, and the module is not imported by a production surface.

`research_experimental_preview_transport_contract.py` prepares the exact
private-test-chat transport envelope behind a second fail-closed policy. The
transport flag defaults off, its kill switch defaults on, owner transport
approval is separate, and exactly one configured destination must equal the
already-authorized Preview chat. A ready envelope preserves the Preview label,
message fingerprint, family/snapshot/audit bindings and separate transport
idempotency key. The contract enforces one decision per formula family and
closes when Stage 5 becomes `READY`. It registers no connector, accepts no
public opt-in, imports no Telegram client and performs no API call or delivery;
database, research-evidence, Stage-6 and LIVE effects remain zero.

`research_experimental_preview_telegram_adapter.py` maps ready envelopes to
the exact keyword arguments supported by the pinned `python-telegram-bot 21.9`
`Bot.send_message` interface. Messages are deterministically split below the
Telegram limit, with content-addressed per-chunk request keys. The adapter is
explicitly `UNREGISTERED`: it accepts neither a Bot nor a token, contains no
API invocation and returns every prepared request as
`BLOCKED_CONNECTOR_UNREGISTERED`. It is not imported by the production bot,
worker or scheduler; connector registration, activation, actual delivery,
public opt-in, Stage 6, LIVE and research-evidence effects all remain absent.

`research_experimental_preview_telegram_dispatcher.py` adds the isolated async
dispatch boundary. It can accept the same Bot interface as production, but a
runtime Bot is restricted to either `RUNTIME_BOT_UNREGISTERED` or
`RUNTIME_BOT_REGISTERED_NO_DISPATCH`; both are structurally blocked before the
method call. Only an explicitly registered `TEST_DOUBLE` can exercise
`send_message`; the result is counted as fake test evidence, never as a
Telegram API call or delivery. The dispatcher is
disabled-by-default, kill-switched, single-test-chat bound and requires owner,
commit and activation-approval fingerprints. Per-request single-flight,
in-memory idempotency, caller-supplied restart keys and cancellation-safe lock
release prevent duplicate fake dispatch. It remains absent from production,
the worker and scheduler, with no database, research-evidence, Stage-6 or LIVE
effect. Its lifecycle is `RUNTIME_CONNECTOR_SUPPORTED_DISPATCH_FORBIDDEN`.

`research_experimental_preview_staging_registration.py` is wired only into the
dedicated `ai_candidate_main.py` staging entrypoint. After the staging Bot
starts, it retains that Bot interface under `RUNTIME_BOT_UNREGISTERED`; shutdown
removes the binding. The registration exposes status only—no dispatch method,
handler, command or scheduled task. Its v4 boundary first fingerprints a runtime
connector candidate after verifying the complete observe-only gate status, then
may idempotently transition that exact candidate to
`RUNTIME_BOT_REGISTERED_NO_DISPATCH`. Registration does not grant activation or
dispatch: the dispatcher rejects the registered runtime Bot before
`send_message`, and the registration continues to report zero delivery attempts
and Telegram API calls. The v4 status adds a fingerprint-verifiable receipt that
binds the registration to the exact activation gate without exposing the chat
id. Missing prerequisites produce blockers, retain no candidate id and leave
the connector unregistered. The main production entrypoint remains untouched.
Merely starting staging therefore causes zero Preview delivery, API, database,
research-evidence, Stage-6 or LIVE effects.

`research_experimental_preview_first_message_authorization.py` prepares the
next local-only boundary. It requires the fingerprint-verified registered-no-
dispatch receipt, the same unchanged activation gate and exactly one prepared
private-chat request. The resulting one-shot key binds the registration,
request, message hash and test-chat hash; replay of that key is blocked. The
output is only `PREPARED_NOT_AUTHORIZED`: its candidate id is explicitly not an
authorization id, no raw chat id or message text is exposed, and authorization,
dispatch, delivery, handlers, schedulers, workers, Telegram calls, database
writes, Stage 6, research evidence and LIVE all remain false or zero. This
module is not wired into the candidate service or any production surface.

`research_experimental_preview_first_message_owner_approval.py` defines the
separate owner-approval record for that candidate. It requires the full
fingerprint-verified candidate, the exact owner identity, an explicit approval
statement, authorization of a single Telegram PREVIEW message, and a UTC expiry
no more than 15 minutes after approval. The approval id is a new fingerprint,
distinct from the candidate id. Production, public opt-in, Stage 6, research
evidence and LIVE authorization are forbidden. Even a valid record remains
`APPROVED_NOT_APPLIED`: runtime application, consumption, dispatch and delivery
stay false; handlers, schedulers and workers remain absent; all external-call
and write counters remain zero. The module is not imported by the candidate
service, dispatcher or any production surface, and no real owner approval is
created merely by adding the contract.

`research_experimental_preview_first_message_application_gate.py` adds the
observe-only application-readiness gate. It fingerprint-verifies the full
candidate and owner-approval record, checks an explicit UTC observation time,
and independently rejects a previously consumed approval id or one-shot key.
The approval is current from its approval timestamp up to—but not including—its
expiry timestamp. A complete result is `READY_NOT_APPLIED`, never applied: the
signed status keeps application, consumption, dispatch and delivery false,
registers no handler, scheduler or worker, performs no Telegram or database
operation and has no Stage-6, research-evidence or LIVE effect. The gate is not
wired into the candidate service, dispatcher or production. Persistent
consumption and atomic application remain separate future boundaries.

`research_experimental_preview_first_message_consumption_contract.py` defines
those future append-only state transitions without implementing storage. A
reservation candidate binds the ready application gate, owner approval,
one-shot key and exact adapter request, while declaring mandatory atomic insert,
unique owner-approval/one-shot constraints and compare-and-set semantics. A
consumption candidate can be prepared only from a reservation reported as
atomically persisted plus a confirmed delivery receipt inside the approval
window. Both outputs remain `NOT_PERSISTED` and perform zero writes or calls.
Duplicate reservation or consumption keys are blocked. An uncertain delivery
outcome prepares no consumption record, forbids automatic retry and requires
manual reconciliation, preventing a crash between delivery and persistence
from silently sending the same message again. The module is disconnected from
the database, candidate service, dispatcher and production.

`migrations/019_preview_first_message_reservation_consumption_v1.sql` and
`research_experimental_preview_first_message_storage_adapter.py` prepare the
next persistence boundary without applying it. The migration
defines separate append-only reservation and consumption tables. Unique keys
claim the approval, one-shot request and delivery attempt once; consumption is
bound by a composite foreign key and a current-reservation validation trigger.
The adapter's reservation SQL uses an insert-only compare-and-set, while its
consumption SQL locks the exact reservation and inserts only inside the approval
window when no prior consumption exists. `ON CONFLICT DO NOTHING` resolves
concurrent duplicates without an update. The migration is registered only in
the explicit one-shot schema installer, which remains disabled unless
`FORMULA_SCHEMA_APPLY=1` and a permitted database URL are both present. It has
not been applied. The adapter still defaults to `DATABASE_UNREGISTERED`. This
version executes only against an explicitly marked transaction double; it
opens no connection, commits no transaction and is not imported by staging,
the dispatcher or production.

`research_experimental_preview_staging_config.py` defines the only staging
configuration names for a future activation: explicit enabled and kill-switch
booleans, owner approval, one numeric private-chat id, a 40-character runtime
commit and a 64-character activation-approval id. Defaults are disabled,
kill-switched and unconfigured. `ai_candidate_main.py` resolves the mapping and
exposes only sanitized readiness booleans and a chat-binding hash in health;
the raw chat and approval ids are never logged or returned. This configuration
version has `CONFIGURE_ONLY_ACTIVATION_FORBIDDEN` authority, so even setting all
variables to an activation-shaped combination cannot register the connector or
send. No Render values were added by this change.

`research_experimental_preview_activation_record.py` prepares a deterministic,
non-authoritative activation candidate for the dedicated candidate service. It
binds the exact runtime commit and the sanitized hash of exactly one private
`TEST_ALLOWLIST` chat. Preparation requires the feature flag to remain off, the
kill switch engaged, owner approval absent and no activation-approval id. The
candidate is explicitly `PREPARED_NOT_APPROVED`; its id cannot be used as an
approval id, and the module has no connector, Bot, environment, database,
scheduler, worker or delivery integration. Any change to the service target,
commit, route or chat binding invalidates its fingerprint.

`research_experimental_preview_staging_activation_gate.py` adds the next pure,
local-only boundary. It fingerprint-verifies the complete owner approval record
and binds it to the actual deployed commit plus the same single private-chat
hash. Kill-switch release, connector, dispatch and first-message authority are
independent required bits; production, public opt-in, Stage 6, research-evidence
and LIVE authority remain structurally forbidden. Even when a hypothetical
record satisfies every prerequisite, this version reports registration still
required and forces activation, connector registration and delivery off. The
dedicated candidate entrypoint imports only its sanitized observe-only function
and exposes that result in health; no Bot is passed to the gate. The gate remains
absent from every production surface and performs no Telegram, database,
scheduler or worker operation. Missing or malformed runtime-commit metadata and
approval-record JSON become health blockers rather than activation authority.

## Discovery v7.1 Walk-forward and horizon scheduler

Stage 4 keeps the v7 probability/asymmetry thresholds unchanged and versions
the validation and operational execution around them:

- `formula-walk-forward-v1-expanding-refit` learns a feature/operator structure
  from initial Fit only, recalculates numeric quantile thresholds on an
  expanding prior-only training prefix before each of three Selection folds,
  and freezes the final formula before opening the outer Test.
- `market-episode-boundary-purge-v1` removes the complete outcome-blind Market
  Episode overlap at every boundary. `full-outcome-horizon-embargo-v1` then
  adds the formula's full 1h/4h/12h/24h outcome horizon. Test cannot influence
  identity, rank, family grouping or Walk-forward selection.
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
