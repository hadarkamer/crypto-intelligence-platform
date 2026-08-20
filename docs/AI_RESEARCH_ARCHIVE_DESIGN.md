# AI Research Archive Design — Candidate

Status: design + dry-run capture. No production schema changes are applied by this document.

## Goal

Build an efficient research archive that lets the AI evaluate whether an alert was correct, what was wrong when it failed, and which bot/internal/external conditions were present at the exact alert time.

## Core rule: timestamp first, no heavy duplication

Every alert becomes one compact immutable `Research Event` with an exact UTC timestamp (`TIMESTAMPTZ`). The event stores only information that cannot be reconstructed later from existing time-series data: alert type, direction, score/components, confirmations, Magnet state, engine/category states, strategy/code version and deterministic identifiers.

Raw OI/CVD/price/exchange/news history is NOT copied into every alert event. Those datasets stay in their own timestamped tables or external archive. Research joins them to the alert by symbol + time window.

This avoids both database bloat and duplicated context when several alerts occur close together.

## Two identifiers: setup vs occurrence

Research must preserve repeated alerts rather than deduplicating them away.

- `setup_key` identifies the same logical setup/family across repeated scans. It intentionally excludes exact score, target and timestamp when those values can evolve inside the same setup.
- `event_fingerprint` identifies one exact occurrence and includes the exact event timestamp plus occurrence state.

Therefore the same setup appearing at 12:00, 12:30 and 13:00 has one `setup_key` but three different `event_fingerprint` values. An exact replay of the same occurrence keeps the same fingerprint and can be ignored idempotently.

## Logical data layers

### 1. research_alert_events — compact immutable event

Minimum fields:
- `event_id`
- `alert_time_utc` (exact `TIMESTAMPTZ`, preserving sub-second ordering where available)
- `symbol`
- `direction` (expected PRICE direction)
- `alert_side` when the underlying engine uses a different semantic side, such as Max-Pain liquidation side
- `alert_type` / categories
- `timeframe`
- `score`
- `current_price`
- `target_price` when applicable
- `initial_target_distance_pct` when applicable
- `engine_snapshot` (`JSONB`, only non-reconstructable bot state)
- `strategy_version`
- `code_version`
- `setup_key`
- `event_fingerprint` unique
- `created_at`

The JSONB snapshot may contain Confirmation, Magnet, Proximity, Consensus, Cluster, Gap, OI/CVD scores and other internal values that may change under later code versions. It must not contain large raw candle histories.

Max-Pain research must preserve the distinction between the displayed liquidation side and expected price direction. For example, a LONG-liquidation-side target below price is stored with expected price direction SHORT while the original alert side is retained separately.

### 2. research_signal_state_changes — meaningful transitions only

To research delayed/inverse signals, weakening alerts and sequences, the system must also preserve meaningful state transitions without storing a full snapshot every minute.

Examples:
- `STRONG_MAGNET_CONFIRMATION -> MAGNET_CONFIRMATION`
- `FUTURES_CVD_HIGH LONG -> NEUTRAL`
- `OI_PRICE SUPPORT -> OPPOSE`
- a Combined Confirmation gaining or losing one independent component

Minimum information:
- exact event timestamp
- symbol / timeframe where applicable
- signal family/name
- old state
- new state
- score/current price when available
- compact evidence explaining the transition
- strategy/code version
- setup/event identifiers

A state-change event is emitted only when old state and new state actually differ.

### 3. research_alert_outcomes — what happened after the alert

One event can have several compact outcome rows, for example 1h / 4h / 12h / 24h.

Fields can include:
- `event_id`
- `horizon_minutes`
- `measured_at_utc`
- `reference_price`
- `price_at_horizon`
- `return_pct`
- `mfe_pct` (maximum favorable excursion)
- `mae_pct` (maximum adverse excursion)
- time to first meaningful progress
- time to MFE / closest target approach
- Max-Pain target-progress ratio / minimum remaining target distance when a target exists
- optional target/invalidated flags once their definitions are frozen

Outcome enrichment runs asynchronously and must never block Watch or alert delivery.

### 4. internal market time series — already mostly present

Existing timestamped data remains the source of truth:
- Price + OI history
- Futures CVD
- Spot CVD
- OI regime snapshots
- Max Pain snapshots where available
- Technical signals

Research tools query only the required symbol/time range and return aggregates, not entire tables to GPT.

### 5. external market context — separate timestamped sources

Future sources such as exchange/index/macro data are stored independently, e.g.:
- crypto exchange/derivatives context
- BTC/ETH market-wide measures
- equity indices / futures when relevant
- DXY, rates/yields or other macro series if chosen
- ETF flows

Each row keeps its own `observed_at_utc`, `source`, source-specific identifiers, values and quality metadata. Alert analysis joins the nearest valid context to `alert_time_utc` with an explicit maximum time tolerance.

### 6. global news events — separate timestamped archive

News is not duplicated inside every alert. A normalized news event keeps:
- `published_at_utc`
- `first_seen_at_utc`
- source/publisher
- headline/title
- canonical URL/id
- categories/entities/symbols when known
- compact summary/embedding or other searchable representation if later approved

For alert research, the AI asks for news in a bounded window around the exact alert time (for example before/after the event), so it can distinguish information already available at alert time from news published later.

This distinction is critical to avoid look-ahead bias.

## External/raw archive

Large raw payloads, screenshots, source HTML or long-term cold data should eventually move to object storage rather than the Render web-service filesystem.

Recommended pattern:
- PostgreSQL: indexed normalized metadata/features needed for fast research.
- Object storage: compressed raw daily files (`jsonl.gz`, screenshots, source artifacts) partitioned by source/date.
- Store an object key/hash in PostgreSQL when a normalized record needs to reference raw evidence.

Example partitioning:
`research-archive/coinglass/2026/08/20/...`
`research-archive/news/2026/08/20/...`

## Performance and safety rules

- Alert delivery must not wait for research enrichment.
- Insert the compact Research Event quickly, then queue/enrich outcomes and external context separately.
- Use deterministic fingerprints/idempotent inserts to prevent exact duplicate occurrences without suppressing legitimate repetitions.
- Never run schema initialization inside recurring Watch work.
- Index timestamps and `(symbol, timestamp)` lookup paths.
- Put explicit timeouts/row limits on AI research queries.
- Send GPT aggregated/bounded results, not thousands of raw rows.
- Preserve source and data-quality metadata so the AI can distinguish missing/stale/conflicting inputs.
- Keep the compact engine snapshot bounded; raw windows/candles belong in their source tables, not every event.

## Research integrity rules

- Preserve exact alert time in UTC.
- Preserve strategy/code version at the time of the alert.
- Do not rewrite historical event snapshots when strategy logic changes later.
- Separate evidence known at alert time from information that appeared afterward.
- Keep failed/weak alerts as well as successful ones to avoid survivorship bias.
- Preserve repeated occurrences and time gaps between them because repetition/density is a research feature.
- Do not let GPT automatically change scoring/strategy from a correlation; it may identify patterns and propose a Candidate test.

## Current Candidate status

Implemented now:
- read-only historical market tools;
- pure `research_event_capture.py` dry-run normalizer;
- Max-Pain event adapter;
- Magnet / Magnet Confirmation adapter;
- generic adapter for standalone OI/CVD/Combined and future alert families;
- Signal State Change adapter;
- separate `setup_key` and exact-occurrence `event_fingerprint`;
- bounded in-memory `DryRunResearchCapture` with no database writes;
- deterministic self-test in Candidate CI.

Not implemented yet:
- production database tables/writes;
- hooks from every live alert-generation path;
- outcome enrichment;
- external exchange/macro/news context collectors;
- object-storage cold archive.

## Candidate implementation order

1. Read-only historical market tools (completed).
2. Build and validate compact Research Event capture in dry-run mode (implemented; validation required before persistence).
3. Map every real alert/transition path into the common event structure without production writes.
4. After review, add production persistence with schema initialization outside recurring Watch work.
5. Add outcome enrichment without blocking the alert loop.
6. Add external exchange/macro/news collectors after each source is validated in AI Lab.
7. Add cold object-storage archive only where raw-data size justifies it.
8. Give GPT research tools that join events to internal/external context by exact timestamp.
