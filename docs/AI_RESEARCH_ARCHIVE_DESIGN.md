# AI Research Archive Design — Candidate

Status: design only. No production schema changes are applied by this document.

## Goal

Build an efficient research archive that lets the AI evaluate whether an alert was correct, what was wrong when it failed, and which bot/internal/external conditions were present at the exact alert time.

## Core rule: timestamp first, no heavy duplication

Every alert becomes one compact immutable `Research Event` with an exact UTC timestamp (`TIMESTAMPTZ`). The event stores only information that cannot be reconstructed later from existing time-series data: alert type, direction, score/components, confirmations, Magnet state, engine/category states, strategy/code version and a deterministic fingerprint.

Raw OI/CVD/price/exchange/news history is NOT copied into every alert event. Those datasets stay in their own timestamped tables or external archive. Research joins them to the alert by symbol + time window.

This avoids both database bloat and duplicated context when several alerts occur close together.

## Logical data layers

### 1. research_alert_events — compact immutable event

Minimum fields:
- `event_id`
- `alert_time_utc` (exact `TIMESTAMPTZ`)
- `symbol`
- `direction`
- `alert_type` / categories
- `timeframe`
- `score`
- `engine_snapshot` (`JSONB`, only non-reconstructable bot state)
- `strategy_version`
- `code_version`
- `fingerprint` unique
- `created_at`

The JSONB snapshot may contain Confirmation, Magnet, Proximity, Consensus, Cluster, Gap, OI/CVD scores and other internal values that may change under later code versions. It must not contain large raw candle histories.

### 2. research_alert_outcomes — what happened after the alert

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
- optional target/invalidated flags once their definitions are frozen

Outcome enrichment runs asynchronously and must never block Watch or alert delivery.

### 3. internal market time series — already mostly present

Existing timestamped data remains the source of truth:
- Price + OI history
- Futures CVD
- Spot CVD
- OI regime snapshots
- Max Pain snapshots where available
- Technical signals

Research tools query only the required symbol/time range and return aggregates, not entire tables to GPT.

### 4. external market context — separate timestamped sources

Future sources such as exchange/index/macro data are stored independently, e.g.:
- crypto exchange/derivatives context
- BTC/ETH market-wide measures
- equity indices / futures when relevant
- DXY, rates/yields or other macro series if chosen
- ETF flows

Each row keeps its own `observed_at_utc`, `source`, source-specific identifiers, values and quality metadata. Alert analysis joins the nearest valid context to `alert_time_utc` with an explicit maximum time tolerance.

### 5. global news events — separate timestamped archive

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
- Use deterministic fingerprints/idempotent upserts to prevent duplicates.
- Never run schema initialization inside recurring Watch work.
- Index timestamps and `(symbol, timestamp)` lookup paths.
- Put explicit timeouts/row limits on AI research queries.
- Send GPT aggregated/bounded results, not thousands of raw rows.
- Preserve source and data-quality metadata so the AI can distinguish missing/stale/conflicting inputs.

## Research integrity rules

- Preserve exact alert time in UTC.
- Preserve strategy/code version at the time of the alert.
- Do not rewrite historical event snapshots when strategy logic changes later.
- Separate evidence known at alert time from information that appeared afterward.
- Keep failed/weak alerts as well as successful ones to avoid survivorship bias.
- Do not let GPT automatically change scoring/strategy from a correlation; it may identify patterns and propose a Candidate test.

## Candidate implementation order

1. Read-only historical market tools (Price/OI/CVD/regime/technical context).
2. Design and validate compact Research Event capture against real alert-generation paths.
3. Add outcome enrichment without blocking the alert loop.
4. Add external exchange/macro/news collectors after each source is validated in AI Lab.
5. Add cold object-storage archive only where raw-data size justifies it.
6. Give GPT research tools that join events to internal/external context by exact timestamp.
