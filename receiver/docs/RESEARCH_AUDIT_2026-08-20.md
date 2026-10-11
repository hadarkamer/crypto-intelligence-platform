# Research System Audit — 2026-08-20

Scope: `ai-agent-candidate` only. Production `main` is not merged or changed by this audit.

## Result

The Research architecture is suitable to proceed to controlled persistence testing after the corrections already made in Candidate. No production Research tables have been created and persistence remains disabled.

## Completed and verified

1. GPT Candidate is connected to bounded read-only current/historical OI/CVD/market tools.
2. Historical market research preserves exact timestamps and queries PostgreSQL in read-only mode.
3. Compact Research Events preserve decision-time UTC timestamp, symbol, expected price direction, source/liquidation side, score, target/current price, strategy/code version and compact non-reconstructable engine state.
4. Max Pain, Magnet, Combined Confirmation and independent special-alert/state-transition shapes are mapped into the common Research Event model in Candidate dry-run mode.
5. Repeated occurrences are preserved through `setup_key` + occurrence-specific `event_fingerprint`.
6. Meaningful weakening/reset/disappearance transitions are captured without storing a heavy snapshot every minute.
7. Shadow Replay was validated on real stored BTC Price/OI/Futures CVD/Spot CVD history with UTC chronology, deduplication and zero database writes.
8. Persistent schema and non-blocking async writer are implemented but disabled by default.
9. Runtime schema creation is forbidden; migration is a separate one-shot operation.
10. Outcome schema includes MFE, MAE, target progress and timing fields needed for the analytical brief.

## Corrections found during audit and already incorporated

- Research anchor is decision/observation time, not Telegram completion time.
- `source_side` is separate from expected price `direction`.
- `runtime_session_id` marks process/restart boundaries.
- `DECISION_SAMPLE` exists for future near-threshold/control research.
- Combined Confirmation component loss/disappearance is tracked for delayed/inverse research.
- Max Pain all-timeframe averages/directional values and compact OI/CVD family state are retained when available.
- Writer retries transient failed batches instead of discarding them immediately.
- Outcome rows record price-path resolution/sample count, so low-resolution history cannot be misrepresented as exact MFE/MAE evidence.
- A guarded one-shot `research_schema_admin.py` was added; it is not imported by Watch and refuses to mutate schema unless `RESEARCH_SCHEMA_APPLY=1` is present plus an explicitly selected database target (`RESEARCH_DATABASE_URL`, or `RESEARCH_USE_PRIMARY_DATABASE=1` with `DATABASE_URL`).

## Data-source audit

Current PostgreSQL source history checked on 2026-08-20:

- Futures taker history: current through 2026-08-20.
- Spot taker history: current through 2026-08-20.
- OI/Price history: current through 2026-08-20.
- OI regime snapshots: current through 2026-08-20.
- Max Pain snapshots: last stored 2026-07-19; current Watch uses live Max Pain without persisting raw snapshots.
- Technical signals: last stored 2026-07-20; currently legacy/stale for historical research.

The database size is about 70 MB on a 1 GB PostgreSQL disk. Separate Research tables in the same PostgreSQL instance are therefore operationally efficient at current volume and allow direct timestamp joins to existing market history. The database is never selected implicitly: Candidate/production must explicitly choose the Research target.

## Not blockers for first persistence test, but required before full analytical use

1. **Outcome worker:** must use a verified finer-resolution price path (for example 1m/5m exchange OHLC) before claiming exact MFE/MAE, time-to-progress or closest-target timing.
2. **Near-miss/control sampler:** needed before the AI can reliably evaluate alternative thresholds/formulas; alert-only history is selection-biased.
3. **Raw Max Pain continuity:** reconsider storing bounded raw/live Max Pain snapshots if future research needs to reconstruct non-alert setups and averages outside Research Events.
4. **Technical-signal continuity:** either restore/replace the stale technical-signals archive before treating it as a current research feature.
5. **External context:** news/exchange/macro/ETF/social data still needs timestamped source-specific archives with publish/first-seen/observed times.
6. **Delivery lifecycle wiring:** before production persistence, each alert family must explicitly classify whether its stored event is an internal observation, approved alert, delivered alert or failed delivery. The decision timestamp must remain unchanged.
7. **Restart boundaries:** sequence analysis must respect `runtime_session_id`; no inference should assume an unseen state transition across a process restart.

## Next controlled stage

1. Explicitly select the persistence target.
2. Apply `migrations/001_research_archive_v1.sql` once using the guarded schema-admin helper, outside Watch.
3. Validate tables/indexes/permissions.
4. Enable persistence only in Candidate and write controlled test Research Events.
5. Verify chronology, idempotency, source-side/direction semantics, lifecycle timestamps, retry behavior and queue metrics.
6. Do not merge to production until those checks pass and explicit approval is given.
