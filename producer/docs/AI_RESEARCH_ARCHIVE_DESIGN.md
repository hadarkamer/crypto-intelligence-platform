# AI Research Archive Design — Candidate

Status: design + audited dry-run implementation. No production schema changes or persistent Research Event writes have been activated.

## Goal

Build an efficient research archive that lets the AI evaluate whether an alert was useful, what conditions made it strong or weak, what happened afterward, and which internal/external conditions existed at the exact decision time.

## Core rule: timestamp first, no heavy duplication

Every alert or meaningful research state becomes one compact immutable `Research Event` anchored to exact UTC decision/observation time (`alert_time_utc`). Telegram delivery timing is a separate lifecycle concern and must never replace that market-context anchor.

The event stores only information that cannot be safely reconstructed later: alert type, expected price direction, source/liquidation side, score/components, confirmations, Magnet state, OI/CVD family scores, relevant Max-Pain averages, strategy/code version and deterministic identifiers.

Raw OI/CVD/price/exchange/news history is NOT copied into every event. Those datasets stay in their own timestamped sources and are joined to events by symbol + bounded time window.

## Event kinds

One chronological research stream supports:

- `ALERT` — an actual alert occurrence/transition that is eligible for alert-performance research.
- `SIGNAL_STATE_CHANGE` — meaningful strengthening, weakening, reversal or disappearance without pretending it was a Telegram alert.
- `DECISION_SAMPLE` — future bounded near-miss/control samples used to test alternative thresholds/formulas without selection bias.

## Two identifiers: setup vs occurrence

- `setup_key` identifies the same logical setup/family across repeated scans. Exact score/target/time are intentionally excluded when they can evolve within one setup.
- `event_fingerprint` identifies one exact occurrence and includes the exact event timestamp plus occurrence state.

Thus the same setup at 12:00, 12:30 and 13:00 remains three research observations, while an exact replay can be ignored idempotently.

## 1. research_events — compact chronological event

Important fields:
- `event_id`
- `alert_time_utc` — exact decision/observation time
- `symbol`
- expected price `direction`
- `source_side` when the engine uses a different semantic side (Max-Pain liquidation side, Magnet upper/lower, etc.)
- `event_kind`, `event_type`, categories
- optional timeframe
- score/current price/target/initial target distance
- compact `engine_snapshot` JSONB
- strategy/code version
- `runtime_session_id` to identify restart boundaries
- setup/event identifiers
- capture/delivery lifecycle fields
- created timestamp

The compact snapshot may preserve Confirmation, Magnet, Proximity, Consensus, Cluster, Gap, OI/CVD scores/time families, Max-Pain all-timeframe averages and other internal values whose meaning may change under future code versions. Large raw candles/windows do not belong here.

## 2. meaningful state changes

Research must preserve weakening and inverse behaviour without storing a complete snapshot every minute. Examples:
- `STRONG_MAGNET_CONFIRMATION -> MAGNET_CONFIRMATION`
- `FUTURES_CVD_HIGH LONG -> NEUTRAL`
- `OI_PRICE SUPPORT -> OPPOSE`
- Combined Confirmation losing one component while remaining active
- Combined Confirmation disappearing completely

A state-change event is emitted only when old and new states differ.

## 3. bounded near-miss/control samples

Emitted alerts alone create selection bias: they cannot show what would have happened if a threshold were 63 instead of 65, or whether a new formula would have selected better setups.

Future `DECISION_SAMPLE` capture should therefore store deliberately bounded samples such as:
- just below an important threshold;
- matched controls for otherwise similar setups;
- selected rejected setups needed for a specific Candidate hypothesis.

It must NOT become a full duplicate of every minute of raw market data.

## 4. research_alert_outcomes — what happened after the event

One alert can receive several outcome rows, initially planned for 1h / 4h / 12h / 24h.

Research needs:
- reference and horizon price;
- raw and direction-adjusted return;
- MFE / MAE and the corresponding prices;
- time to first meaningful progress;
- time to MFE;
- closest approach to Max-Pain target and time to closest approach;
- exact target timing when reached;
- Max-Pain target-progress ratio even when exact target is missed;
- price-path resolution/source/sample count and data quality;
- versioned outcome calculation method.

### Precision rule

The existing 30-minute-ish Price/OI close history is useful context but cannot be called exact MFE/MAE or exact time-to-target evidence. Outcome enrichment must use a verified finer price path (for example 1m/5m exchange OHLC) and preserve its resolution/source.

## 5. internal market time series

Existing timestamped data remains the source of truth where healthy:
- Price + OI history
- Futures CVD
- Spot CVD
- OI regime snapshots
- Max Pain source history where available
- Technical signals where available

Research tools query only the necessary time range and return bounded aggregates to GPT.

### Audit source-health note

The August 20 audit confirmed Price/OI, Futures CVD, Spot CVD and OI-regime history remain current. `max_pain_snapshots` stopped in July because Watch intentionally uses a live snapshot path without DB writes; `technical_signals` is also stale/legacy currently.

Research Events now preserve more Max-Pain decision-time state, but broad formula/near-miss studies that require every non-alert Max-Pain scan will need a dedicated bounded source-history decision later rather than assuming the old table is current.

## 6. external market context

Future exchange/index/macro/ETF sources remain independent time series with their own:
- `observed_at_utc`
- source/source-specific identifier
- values/features
- quality/freshness metadata

Research joins the nearest valid source row to the Research Event with explicit time tolerance.

## 7. global news events

Normalized news keeps both:
- `published_at_utc`
- `first_seen_at_utc`

plus source/title/canonical ID or URL/entities/categories and a compact searchable representation. This prevents look-ahead bias by distinguishing what was actually available at alert time from information published later.

## 8. external/raw archive

Large screenshots, HTML and bulky raw payloads should eventually live in durable object storage (S3/R2/B2-style), not the Render service filesystem.

Recommended split:
- PostgreSQL: indexed normalized metadata/features needed for fast research.
- Object storage: compressed raw daily evidence and screenshots, referenced by key/hash.

## Performance and safety rules

- Alert delivery/Watch must never wait for Research DB I/O.
- Runtime event enqueue uses a bounded non-blocking queue.
- Background persistence batches idempotent inserts.
- Transient DB failures retain/retry the current batch with exponential backoff.
- Queue saturation is visible through explicit drop metrics rather than silently blocking Watch.
- Never initialize schema inside recurring Watch work.
- Index timestamp and `(symbol, timestamp)` research paths.
- Preserve exact decision time, source and data quality.
- Preserve strategy/code version and runtime session boundary.
- Send GPT bounded/aggregated results rather than raw bulk rows.

## Research integrity rules

- Preserve successes, weak alerts and failures.
- Preserve repetitions and spacing between repetitions.
- Preserve weakening/disappearance, not only entry transitions.
- Separate expected price direction from source/liquidation side.
- Separate decision time from delivery time.
- Separate evidence known at decision time from later evidence.
- Use near-miss/control samples for threshold/formula research.
- A discovered formula is a Candidate hypothesis, never an automatic production strategy change.
- Where sample size permits, discovery and holdout/out-of-sample periods must be separated.

## Current Candidate status

Implemented and validated in Candidate:
- six read-only GPT market/history tools;
- compact `research_event_capture.py` normalizer;
- Max-Pain, Magnet and generic alert adapters;
- meaningful Signal State Change adapter;
- future `DECISION_SAMPLE` support;
- setup vs exact-occurrence fingerprints;
- richer compact Max-Pain averages and OI/CVD time-family capture;
- live-shape dry-run hooks for normal Max Pain, Max-Pain score/Confirmation, OI+Price, Futures/Spot CVD, Combined and Magnet Watch;
- research-only Combined weakening/component-loss/deactivation tracker;
- Shadow Replay on real stored BTC Price/OI/CVD data;
- persistence schema and async writer implemented but explicitly disabled;
- writer restart/session metadata and transient DB retry behaviour;
- CI checks for schema safety and main hook markers.

Not activated yet:
- no production/research DB migration applied;
- no persistent Research Event writes;
- no production-main merge;
- no outcome worker;
- no near-miss sampler;
- no new raw Max-Pain Watch archive;
- no external exchange/macro/news research archive.

## Next activation order

1. Candidate dry-run + pre-persistence audit. **Completed.**
2. Correct timestamp/lifecycle/Combined/retry/schema gaps. **Completed in Candidate.**
3. Explicitly choose/approve persistence target.
4. Apply the one-time migration outside Watch.
5. Enable persistence in Candidate only and write controlled test events.
6. Validate chronology, idempotency, lifecycle metadata, retry/queue metrics and DB growth.
7. Only after approval, wire persistent Research Event capture into production main.
8. Add finer-resolution price path + asynchronous outcome enrichment.
9. Add bounded near-miss/control sampling.
10. Add external context collectors and GPT research/comparison/pattern tools.
