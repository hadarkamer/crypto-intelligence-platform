# Research Persistence v1 — Candidate Plan

Status: implemented as disabled-by-default Candidate code. No production schema has been applied and no production writes are enabled.

## Physical storage decision

Use one chronological `research_events` table for both delivered/approved alerts and meaningful signal-state changes. This is intentionally simpler than separate physical alert/state-change tables because sequence research needs one ordered event stream. The event kind (`ALERT` or `SIGNAL_STATE_CHANGE`) keeps the logical distinction.

Use a separate `research_alert_outcomes` table because outcomes are measured later at multiple horizons and should not mutate the immutable event row.

## research_events

Each row preserves the exact decision-time event:

- exact `alert_time_utc` (`TIMESTAMPTZ`);
- `symbol`, expected price `direction`, optional `timeframe`;
- `event_kind` and `event_type`;
- score, current price, target price and initial target distance when applicable;
- categories;
- compact non-reconstructable `engine_snapshot` (Confirmation, Magnet, score components, OI/CVD state, etc.);
- `strategy_version` and `code_version`;
- `setup_key` for grouping repeated appearances of the same setup;
- unique `event_fingerprint` for idempotency of one exact occurrence;
- `capture_stage` and `delivery_status` so research can distinguish an observed/approved signal from an alert confirmed as delivered to Telegram.

Delivery status deliberately supports:
- `UNKNOWN`
- `NOT_APPLICABLE`
- `APPROVED_FOR_DELIVERY`
- `DELIVERED`
- `DELIVERY_FAILED`

This prevents future analysis from incorrectly treating every internal transition as a user-received alert.

## research_alert_outcomes

One event can receive multiple outcome rows (for example 60, 240, 720 and 1440 minutes).

The v1 schema supports:
- reference and horizon price;
- raw return;
- direction-adjusted return;
- MFE and MAE;
- time to first meaningful progress;
- time to MFE;
- closest Max-Pain target distance;
- target progress ratio;
- target reached flag;
- price source/data-quality metadata;
- `outcome_method_version` so later calculation changes never silently rewrite the meaning of old results.

Definitions such as the exact threshold for “first meaningful progress” remain versioned rather than hard-coded into the event itself.

## Non-blocking write path

The proposed live flow is:

`alert/signal logic -> ResearchEvent normalize -> put_nowait(queue) -> continue Watch/Telegram`

A separate background writer:
1. reads a bounded queue;
2. batches up to 50 events;
3. writes them in a separate PostgreSQL transaction;
4. uses `ON CONFLICT (event_fingerprint) DO NOTHING`;
5. uses short connection/query/lock timeouts;
6. never performs schema creation or migration.

Default queue capacity is 2,000 events. Queue saturation increments an explicit drop metric and logs the condition rather than blocking the alert loop. At expected alert volume this is deliberately large, but the metric must be monitored before production approval.

## Safety gates already coded

`research_event_store.py` cannot write unless BOTH are explicitly configured:

- `RESEARCH_PERSISTENCE_ENABLED=1`
- `RESEARCH_DATABASE_URL=<approved research database connection>`

It does not fall back to the bot's ordinary `DATABASE_URL`. This is deliberate: Candidate testing must not accidentally write to the shared production database.

The runtime writer contains no `CREATE TABLE`/`ALTER TABLE`. Schema installation exists only in:

`migrations/001_research_archive_v1.sql`

and must be applied separately after explicit approval.

## Production activation sequence (not executed yet)

1. Review/approve this schema and write path.
2. Choose the approved persistence target/role and set `RESEARCH_DATABASE_URL`.
3. Apply `migrations/001_research_archive_v1.sql` once, outside Watch.
4. Validate table/indexes and permissions.
5. Enable writer in Candidate first and feed test events.
6. Verify idempotency, chronology, latency and queue metrics.
7. Only then connect the production Research Event hooks to the writer.
8. Keep normal Telegram/Watch behavior fail-open if research persistence is unavailable.
9. Add asynchronous outcome enrichment as a separate worker after event capture is stable.

## Explicitly not done yet

- no migration was applied to production;
- no `RESEARCH_PERSISTENCE_ENABLED=1` was set;
- no Research Event was written persistently;
- no production `main` merge was performed;
- no outcome worker is active yet.
