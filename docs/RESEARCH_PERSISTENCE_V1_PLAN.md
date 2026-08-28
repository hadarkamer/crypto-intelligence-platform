# Research Persistence v1 — Candidate Plan

Status: audited and implemented as disabled-by-default Candidate code. No production schema has been applied and no production writes are enabled.

## Audit decisions before first migration

The pre-persistence audit found several issues that were corrected before any table was created:

1. `alert_time_utc` is now defined strictly as the signal decision/observation time. Telegram network latency must never move this research anchor. Delivery attempt/completion timestamps are separate fields.
2. Source/liquidation side is preserved explicitly as `source_side`, separate from expected PRICE `direction`.
3. A `runtime_session_id` marks process/restart boundaries so restart-induced transitions can be recognized in research.
4. `DECISION_SAMPLE` is supported for future near-miss/control sampling, so alternative thresholds can be studied without falsely labeling those samples as alerts.
5. Combined Confirmation composition changes, weakening and disappearance have a dedicated research-only tracker; Telegram behaviour remains unchanged.
6. Max-Pain events preserve compact all-timeframe averages/directional scores and compact OI/CVD time-family state that may not be safely reconstructable later.
7. The async writer no longer discards a batch on the first transient DB error. It holds that batch and retries with bounded exponential backoff while Watch/Telegram continue independently.
8. Outcome schema now records enough path metadata to make MFE/MAE/target-progress calculations reproducible, but the outcome worker must not claim precision until a suitably fine price path is available.

## Physical storage decision

Use one chronological `research_events` table for delivered/approved alerts, meaningful signal-state changes and deliberately sampled near-miss/control decisions. `event_kind` distinguishes:

- `ALERT`
- `SIGNAL_STATE_CHANGE`
- `DECISION_SAMPLE`

Use a separate `research_alert_outcomes` table because outcomes are measured later at several horizons and should not mutate the decision-time snapshot.

## research_events

Each row preserves the exact decision-time event:

- exact `alert_time_utc` (`TIMESTAMPTZ`), defined as decision/observation time;
- `symbol`, expected price `direction`, optional `source_side` and `timeframe`;
- `event_kind` and `event_type`;
- score, current price, target price and initial target distance when applicable;
- categories;
- compact non-reconstructable `engine_snapshot` (Confirmation, Magnet, score components, OI/CVD family state, Max-Pain averages, etc.);
- `strategy_version` and `code_version`;
- `runtime_session_id`;
- `setup_key` for grouping repeated appearances of the same setup;
- unique `event_fingerprint` for idempotency of one exact occurrence;
- `capture_stage`, `delivery_status`, `delivery_attempted_at_utc`, `delivered_at_utc`.

Delivery status supports:
- `UNKNOWN`
- `NOT_APPLICABLE`
- `APPROVED_FOR_DELIVERY`
- `DELIVERED`
- `DELIVERY_FAILED`

A Research Event timestamp is never replaced by a later Telegram timestamp.

## research_alert_outcomes

One event can receive multiple outcome rows, initially planned for 60, 240, 720 and 1440 minutes.

The schema supports:
- reference and horizon price;
- raw and direction-adjusted return;
- maximum favorable/adverse price plus MFE/MAE;
- time to first meaningful progress;
- time to MFE;
- closest Max-Pain target price/distance and time to closest approach;
- time to exact target where applicable;
- target-progress ratio and target-reached flag;
- path resolution/sample count;
- source/data-quality metadata;
- versioned outcome method.

### Price-path precision requirement

The existing `oi_price_history` cadence is useful for market context but is not precise enough by itself for exact intrahorizon MFE/MAE, time-to-progress or closest-target timing. Before the outcome worker is enabled, it must use a finer path (for example verified 1m/5m exchange OHLC) and record its resolution/source. A 30-minute close series must never be presented as exact MFE/MAE evidence.

## Non-blocking write path

Proposed live flow:

`decision -> ResearchEvent normalize -> put_nowait(queue) -> continue Watch/Telegram`

The background writer:
1. reads a bounded queue;
2. batches up to 50 events;
3. writes in a separate PostgreSQL transaction;
4. uses `ON CONFLICT (event_fingerprint) DO NOTHING`;
5. uses short connection/query/lock timeouts;
6. holds and retries the current failed batch with exponential backoff;
7. never performs schema creation or migration.

Queue capacity is 2,000 events. A prolonged DB outage can eventually fill it; `queue_full_drops` is therefore an explicit integrity metric that must be monitored. Research persistence remains fail-open for the trading/Telegram path.

## Safety gates already coded

`research_event_store.py` cannot write unless BOTH are explicitly configured:

- `RESEARCH_PERSISTENCE_ENABLED=1`
- `RESEARCH_DATABASE_URL=<approved research database connection>`

It never silently falls back to the bot's ordinary `DATABASE_URL` and contains no runtime DDL. Schema installation exists only in:

`migrations/001_research_archive_v1.sql`

and must be applied once, outside Watch, after explicit approval.

## Selection-bias protection

Alert-only history cannot answer every strategy question because it excludes setups that narrowly missed existing thresholds. The schema therefore supports `DECISION_SAMPLE`. The future sampler must be bounded and purposeful — for example near-threshold or matched control samples — rather than storing every raw scan. This enables threshold/formula research without turning PostgreSQL into a minute-by-minute duplicate market database.

## Source-health finding from the audit

The current database has healthy recent Price/OI, Futures CVD, Spot CVD and OI-regime history. However, `max_pain_snapshots` stopped receiving rows in July because current Watch intentionally uses a live Max-Pain snapshot path without DB writes. `technical_signals` is also stale/legacy at present.

This does not block Research Event capture because the event now preserves the important decision-time Max-Pain internals, but raw Max-Pain source-history collection must be reconsidered before broader formula/near-miss research depends on reconstructing every non-alert scan.

## Storage target recommendation

The existing Render PostgreSQL database is currently small relative to its allocated disk, so the efficient first deployment is to use separate research tables in the same PostgreSQL instance while retaining the explicit `RESEARCH_DATABASE_URL` opt-in. A separate paid database is not technically necessary at the present volume. This is only a recommendation; applying the migration is a real schema mutation and requires explicit approval.

## Production activation sequence — not executed yet

1. Candidate audit and dry-run replay. **Completed.**
2. Review corrected schema/write path. **Completed.**
3. Explicitly approve the persistence target.
4. Apply `migrations/001_research_archive_v1.sql` once outside Watch.
5. Validate tables, indexes and permissions.
6. Enable writer in Candidate first and feed controlled test events.
7. Verify idempotency, chronology, lifecycle timestamps, retry behaviour and queue metrics.
8. Only then wire the production Research Event hooks to the persistent writer.
9. Keep normal Telegram/Watch fail-open if research persistence is unavailable.
10. Add the finer-resolution outcome path and asynchronous outcome enrichment.
11. Add bounded decision/near-miss sampling.
12. Add external exchange/macro/news archives and GPT research tools.

## Explicitly not done yet

- no migration has been applied to production;
- no `RESEARCH_PERSISTENCE_ENABLED=1` has been set;
- no Research Event has been written persistently;
- no production `main` merge has been performed;
- no outcome worker is active;
- no near-miss sampler is active;
- no raw Max-Pain Watch archive has been re-enabled.
