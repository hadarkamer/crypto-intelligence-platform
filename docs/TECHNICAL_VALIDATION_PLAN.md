# תכנית אימות טכני קבועה — Crypto Intelligence Platform Core Bot

> **סוג המסמך:** מסגרת Audit ואימות מתמשכת, לפי מצב הענף `codex/create-agents.md-with-repository-rules` בעת הבדיקה (2026-08-13).
> **גבול הבדיקה:** Static inspection בלבד. לא הופעל Bot, לא בוצעה גישה ל־DB, לא הופעלו collectors, לא נשלחו הודעות Telegram ולא בוצעו קריאות לספקים. לכן ממצא שתלוי ב־Render או בשירות חיצוני אינו מסומן `PASS`.

## 1. מודל סטטוסים וכללי ראיות

### יכולות קיימות

| Status | משמעות |
|---|---|
| `PASS` | קיים מימוש וראיה סטטית/בדיקה אוטומטית מספקת; לא משמש להוכחת התנהגות runtime. |
| `FAIL` | הקוד הנוכחי סותר דרישה מפורשת או invariant. |
| `PARTIAL` | חלק מהחוזה מיושם או ניתן להוכחה, וחלק חסר/לא מכוסה. |
| `NEEDS_RUNTIME` | נדרשים Render, DB אמיתי, Telegram או ספק חי; אין להסיק `PASS` מקריאת קוד. |
| `NOT_APPLICABLE` | המנגנון אינו קיים בארכיטקטורה הנוכחית; הסיבה חייבת להירשם. |

### יכולות עתידיות

`NOT_IMPLEMENTED` → `IMPLEMENTED_UNVALIDATED` → `VALIDATING` → `VERIFIED`.

מעבר ל־`VERIFIED` מחייב את כל בדיקות הרגרסיה והריצה שהוגדרו לרכיב. היעדר feature עתידי אינו `FAIL`.

### תבנית חובה לעדכון פריט קיים

כל פריט `CV-*` להלן כולל: **ID, Area, Status, Relevant files, Relevant functions/classes/tasks, What is being checked, Actual current behavior, Expected behavior, Evidence, Risk if incorrect, Automated regression test exists, Safe to test in Codex, Requires Render/runtime, PRODUCT DECISION REQUIRED, Recommended next action**. שינוי status מחייב תאריך, commit, סביבת בדיקה וקישור לראיה; runtime-dependent item לעולם לא יקבל `PASS` מ־static inspection בלבד.

---

## 2. פריטי אימות של המימוש הנוכחי

### CV-START-001

**Area:** Startup / initialization
**Status:** `PARTIAL`
**Relevant files:** `main.py`, `technical_signal_store.py`, `coinglass_history_backfill.py`, `coinglass_oi_regime_service.py`, `coinglass_flow_foundation.py`, `runtime.txt`
**Relevant functions/classes/tasks:** `main`, `init_db`, `start_web_server`, `ApplicationBuilder`, `_restore_watch_runtime`, `OI_REGIME_TASK`, `HISTORY_BACKFILL_TASK`, `FLOW_COLLECTION_TASK`
**What is being checked:** importability; סדר startup; schema/migrations; Telegram; `aiohttp`; health; ports; webhook registration; task creation; automatic/manual behavior; מניעת initialization כפול.
**Actual current behavior:** `main()` initializes the main schema before building Telegram, then—only when `COINGLASS_API_KEY` is non-empty—initializes three derivative schemas and starts Price+OI, historical OI backfill, and Futures+Spot flow loops. `/health`, `/telegram`, `/webhooks/tradingview`, and `/technical/status` share one `aiohttp` server on `0.0.0.0:PORT` (default `10000`). Telegram webhook is deleted and reset to `PUBLIC_URL/telegram`. Max Pain DOM and general Watch are not started automatically; persisted Watch metadata is restored, but no Watch task is restored. Startup has no explicit in-process once guard; a second `main()` invocation/event-loop owner is not statically prevented.
**Expected behavior:** initialization once per process; DDL only in startup; collectors only under approved configuration; manual Max Pain/Watch remain manual; webhook and web server startup fail visibly and cleanly.
**Evidence:** conditional task creation and shutdown cancellation are in `main.py`; schema helpers are also called later from data paths (see `CV-DB-001`). `runtime.txt` launches `python main.py`.
**Risk if incorrect:** **Critical**
**Automated regression test exists:** No
**Safe to test in Codex:** Partial (import/AST with mocks only)
**Requires Render/runtime:** Yes
**PRODUCT DECISION REQUIRED:** No
**Recommended next action:** add a mocked startup-order test, then validate exactly one web server and one task set in Render logs.

### CV-SCHED-001

**Area:** Scheduling / concurrency / single-flight
**Status:** `PARTIAL`
**Relevant files:** `main.py`, `coinglass_history_backfill.py`, `coinglass_flow_foundation.py`
**Relevant functions/classes/tasks:** `_request_oi_refresh`, `_request_flow_refresh`, `_oi_regime_loop`, `_flow_collection_loop`, `_history_backfill_loop`, `_ensure_watch_coordinator`, `watch_loop`, `specific_watch_loop`, `_run_history_backfill_once`, `_collect_*_once`
**What is being checked:** duplicate tasks/loops, overlap, cancellation/restart, exception isolation, lock ordering, symbol/provider isolation.
**Actual current behavior:** in-process OI/CVD calls join named `asyncio` tasks; Watch uses one coordinator plus `WATCH_SCAN_TASK`; repeated general/Top8 commands reuse the coordinator. Specific Watch has a separate five-minute loop. Exceptions are caught at outer collector loops; `collect_many` isolates symbols, and flow `backfill_all` iterates markets/symbols with result objects. Shutdown cancels startup collector tasks and Watch tasks. Restart-on-unexpected-task-death is not implemented. Check-then-create single-flight assignment is event-loop atomic but not protected by an explicit lock.
**Expected behavior:** one owner per recurring concern, deterministic cancellation/cleanup, symbol/provider failures isolated, and no unsafe overlap.
**Evidence:** globals `WATCH_TASK`, `WATCH_SCAN_TASK`, `OI_REFRESH_TASK`, `FLOW_REFRESH_TASK`; `asyncio.Lock` instances for scrape/alerts/history/flow; PostgreSQL session locks listed below.
**Risk if incorrect:** **Critical**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes, with fake tasks/providers/DB
**Requires Render/runtime:** Yes (cross-process behavior)
**PRODUCT DECISION REQUIRED:** No
**Recommended next action:** build deterministic concurrency tests for repeated commands, cancellation, exceptions, and simultaneous live/backfill calls; observe task count in Render.

#### Lock registry

| Lock/state | Scope | Protects | Known limitation |
|---|---|---|---|
| `COLLECT_LOCK` / `SCRAPE_LOCK` | process / asyncio | manual collection and DOM scan | lazy initialization; no cross-process DOM lock |
| `ALERT_COMMAND_LOCK` | process / asyncio | overlapping alert commands | not shared with Watch |
| `HISTORY_BACKFILL_LOCK` | process / asyncio | manual/automatic OI history backfill | paired with PostgreSQL session advisory lock only on PostgreSQL |
| `FLOW_BACKFILL_LOCK` | process / asyncio | flow backfill and live flow refresh | SQLite has process-only protection |
| `OI_REFRESH_TASK`, `FLOW_REFRESH_TASK` | process / task join | single-flight refresh | no persistence/restart recovery |
| `_SCHEMA_ADVISORY_LOCK_ID` | PostgreSQL transaction | schema work; same numeric ID appears in modules | only protects code paths that actually call it |
| `_OI_COLLECTOR_LOCK_ID`, `_FLOW_COLLECTOR_LOCK_ID`, `_HISTORY_BACKFILL_LOCK_ID` | PostgreSQL session | cross-instance collector/backfill ownership | SQLite returns no equivalent cross-process lock |
| `alert_history` fingerprint + cooldown | DB | Watch alert suppression | restart-safe only for this alert path; confirmation maps are in memory |
| `PROCESSED_UPDATE_IDS` | process memory | duplicate Telegram update IDs | lost on restart; bounded to 500 |

### CV-DB-001

**Area:** Database / schema / migrations / deadlocks
**Status:** `FAIL`
**Relevant files:** `main.py`, `technical_signal_store.py`, `coinglass_history_backfill.py`, `coinglass_oi_regime_service.py`, `coinglass_flow_foundation.py`
**Relevant functions/classes/tasks:** all `init_db`; `ensure_amount_columns`; `_store`; `_insert_snapshot`; `freshness`; `coverage`; `_rebuild_continuous_cvd`; `backfill_symbol`; `execute_write`; `_insert_technical_signal`
**What is being checked:** all DDL, indexes, migrations, transaction boundaries, idempotency, SQLite/PostgreSQL parity, recurring writes and historical `DeadlockDetected`.
**Actual current behavior:** schemas use `CREATE TABLE/INDEX IF NOT EXISTS`; migrations inspect columns and add missing columns. PostgreSQL schema setup uses transaction advisory locking. However DDL-capable `init_db()` is called from recurring/read/write paths: flow `_store`, `freshness`, `_rebuild_continuous_cvd`; OI `_history` and `_insert_snapshot`; history store/read/reference paths; and main `query`/`execute_write` call `init_db`. Thus the explicit rule “DDL must not run inside recurring jobs” is violated even when statements are idempotent. SQLite and PostgreSQL use different timestamp types/placeholders and SQLite lacks cross-process advisory locks.
**Expected behavior:** schema/migrations once in controlled startup, repeat-safe; collectors only DML; consistent lock order and backend semantics.
**Evidence:** direct call paths `coinglass_flow_foundation._store → init_db`, `coinglass_oi_regime_service._insert_snapshot → init_db`, and `main.execute_write/query → init_db`. Potential deadlock paths are every concurrent collector/read that first acquires a collector session lock and subsequently enters schema advisory/DDL, plus simultaneous module initializers altering their own tables. Exact reproduction needs PostgreSQL runtime.
**Risk if incorrect:** **Critical**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes for call-graph/SQL mocks; No for production DB
**Requires Render/runtime:** Yes
**PRODUCT DECISION REQUIRED:** No
**Recommended next action:** preserve a regression that rejects DDL from recurring call graphs; separately reproduce lock ordering on an isolated PostgreSQL instance and monitor `pg_locks`/deadlock logs. Do not test against production.

### CV-OI-001

**Area:** Price + OI collection
**Status:** `PARTIAL`
**Relevant files:** `main.py`, `coinglass_oi_regime_service.py`, `coinglass_history_backfill.py`, `live_price_provider.py`
**Relevant functions/classes/tasks:** `_oi_regime_loop`, `_collect_oi_regime_once_locked`, `collect_many`, `fetch_aggregated_oi_with_meta`, `_history_backfill_loop`, `_ensure_watch_derivatives_ready`
**What is being checked:** interval, freshness, startup, due/backfill, tables, provider order, independence, partial/error behavior.
**Actual current behavior:** live collection interval is hard-coded `30m`; automatic tasks start only with `COINGLASS_API_KEY`. History uses `oi_price_history`, live snapshots use `oi_regime_snapshots`; automatic rolling backfill checks every `60m`, is due after `24h`, starts after `60s`, and requests recent three days. Watch joins the same OI single-flight rather than creating another collector. Data-quality status compares price/OI timestamps and history references tolerate ±20 minutes. One symbol exception becomes unavailable output.
**Expected behavior:** independent of Alerts/Watch; no overlap corruption; explicit unavailable/stale/partial data; no one-symbol abort.
**Evidence:** OI starts at application startup independent of Watch; UPSERT history and locks reduce overlap. OI “fresh generation” currently compares snapshot collection time to expected CVD close, not a shared immutable generation ID.
**Risk if incorrect:** **High**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes with fixtures
**Requires Render/runtime:** Yes for cadence/provider/locks
**PRODUCT DECISION REQUIRED:** No
**Recommended next action:** add stale, partial, per-symbol failure, due-clock and live/backfill overlap tests; validate cadence in logs.

### CV-CVD-001

**Area:** Futures CVD + Spot CVD
**Status:** `PARTIAL`
**Relevant files:** `coinglass_flow_foundation.py`, `coinglass_flow_engine.py`, `market_session_baseline.py`, `main.py`
**Relevant functions/classes/tasks:** `fetch_history`, `_normalize`, `_store`, `_rebuild_continuous_cvd`, `freshness`, `latest_eligible_candle_time`, `analyze_market`, `_flow_collection_loop`
**What is being checked:** separate streams, 30m aggregation, CVD math/timestamps, closure, gaps, freshness exact boundary, consumers and generation compatibility.
**Actual current behavior:** separate `futures_taker_history` and `spot_taker_history` tables have `(symbol,candle_time)` primary keys. Official `buy_vol_usd`, `sell_vol_usd`, API cumulative CVD and rebuilt continuous cumulative CVD are stored. Candle interval is `30m`, close is open+30m, grace is `2m`; timestamp mode defaults to `open`. Poll interval defaults to `5m` (README contains both older 30m and newer 5m statements). Freshness derives from the latest closed candle and tolerance environment/default. Positive and negative baseline distributions are separate; neutral/unavailable are explicit. No explicit “30-minute gap detector” result is surfaced, although quality/coverage expose intervals.
**Expected behavior:** exact bullish/bearish/zero symmetry, stale distinct from neutral, incomplete candle exclusion, gap detection, deduplication, mathematically consistent cumulative values, and time-compatible OI/CVD.
**Evidence:** storage tables and candle helpers implement separation/closure; Watch readiness requires OI+Futures for all targets but Spot is reported rather than required, and downstream per-symbol freshness filtering still governs use.
**Risk if incorrect:** **High**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes with synthetic candles
**Requires Render/runtime:** Yes for live values/cadence
**PRODUCT DECISION REQUIRED:** No
**Recommended next action:** add exact threshold ±epsilon, open/close, positive/negative/zero, missing-gap, dedup and continuous-sum fixtures; validate both live markets.

### CV-REGIME-001

**Area:** OI regime / Price+OI regime
**Status:** `PARTIAL`
**Relevant files:** `coinglass_oi_regime_service.py`, `time_family_engine.py`, `market_confidence_engine.py`
**Relevant functions/classes/tasks:** `classify`, `_window_results`, `_overall`, `_invalid_snapshot_parts`, `_collect_symbol_with_oi_meta`, `latest`, `aggregate`, `oi_window_evaluator`
**What is being checked:** windows and families; missing/flat/sign combinations; historical `UnboundLocalError: weighted`; scoring independence; LONG/SHORT separation.
**Actual current behavior:** current windows are `30m,1h,4h,12h,24h,48h,72h,7d`; states include bullish buildup, short covering, bearish buildup, long unwinding and neutral/inconclusive. Four weighted families are NOW/SHORT/MEDIUM/LONG. Both valid and invalid branches assign `weighted` before return; static control flow found no undefined use. Regime is attached as evidence and does not mutate Max Pain score.
**Expected behavior:** every sign quadrant, exact zero, empty/partial/insufficient/stale histories yield deterministic explicit state; `weighted` always initialized; side scores stay separate.
**Evidence:** `_invalid_snapshot_parts` returns `weighted`; collection/latest valid branches calculate it before serialization. Static evidence is insufficient for all runtime data shapes.
**Risk if incorrect:** **High**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes
**Requires Render/runtime:** No for formula; Yes for live history quality
**PRODUCT DECISION REQUIRED:** No
**Recommended next action:** parameterize all quadrants/windows and an explicit regression for missing/invalid quality reaching every return path.

### CV-PROVIDER-001

**Area:** Data providers / fallbacks / network resilience
**Status:** `PARTIAL`
**Relevant files:** `live_price_provider.py`, `coinglass_oi_regime_service.py`, `coinglass_history_backfill.py`, `coinglass_flow_foundation.py`, `main.py`
**Relevant functions/classes/tasks:** `fetch_binance_usdt_prices`, HYPE helpers, `fetch_aggregated_oi_with_meta`, `_request`, `fetch_coinglass_timeframe`
**What is being checked:** hierarchy, mappings, 403/404/429/5xx/timeouts/DNS/JSON/fields/numeric/retry, semantic transparency.
**Actual current behavior:** live price tries Binance Futures mark then Binance Spot; HYPE has Bybit, Hyperliquid, CoinGecko and CoinPaprika fallbacks as implemented in `live_price_provider.py`. Price result objects include source and errors; Telegram exposes a price-source label in relevant cards. OI and both flow streams use CoinGlass V4 aggregated endpoints; Max Pain uses CoinGlass DOM (with an older API helper still present). CoinGlass request code recognizes rate-limit messages and HTTP failures; OI history has configurable retry/pause. Not every provider has equivalent exponential backoff or specific branches for every listed HTTP status.
**Expected behavior:** provider and symbol mappings remain explicit; malformed/invalid data rejected; fallback meaning does not change silently; source appears in objects/logs/diagnostics and, where decision-relevant, Telegram.
**Evidence:** source fields are preserved for price/OI; flow data identifies market but Telegram does not consistently name the provider. Bybit 403 falls through for HYPE rather than terminating the batch.
**Risk if incorrect:** **High**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes with mocked HTTP
**Requires Render/runtime:** Yes for real provider policy
**PRODUCT DECISION REQUIRED:** No
**Recommended next action:** provider-contract matrix tests for timeout, DNS, 403, 404, 429, 5xx, malformed/empty/partial/NaN and verify surfaced source/error.

### CV-DOM-001

**Area:** CoinGlass DOM / Playwright
**Status:** `PARTIAL`
**Relevant files:** `coinglass_dom_reader.py`, `main.py`
**Relevant functions/classes/tasks:** `collect_coinglass_dom_snapshot`, `read_timeframe`, `_retry_timeframe_on_fresh_page`, `collect_live_rows_for_watch`, `_get_scrape_lock`
**What is being checked:** timeframe selection/shape/staleness/duplicates/retries, lifecycle, leak/concurrent isolation and failure isolation.
**Actual current behavior:** requested timeframes are deduplicated; collection order is `24h,12h,48h,3d,1w,2w,1m`; each attempt uses a fresh page; active label, baseline change, stable fingerprint twice, unique symbols and at least 30 rows are required. Cross-timeframe duplicate fingerprints and duplicate `(symbol,timeframe)` pairs invalidate the atomic result. Retry pages, context and browser close in `finally`. Expected shape is therefore at least `30 × 7`, not hard-coded `50 × 7`; `TOP_COINS_LIMIT` defaults 50 elsewhere. Process-level scrape lock serializes callers, but there is no PostgreSQL/distributed DOM lock.
**Expected behavior:** atomic correct timeframe snapshot; no leaks; concurrent commands isolated; CoinGlass failure cannot terminate collectors/Telegram.
**Evidence:** explicit lifecycle `finally` and atomic `ok`; callers catch failures. Runtime browser/resource behavior remains unproven.
**Risk if incorrect:** **High**
**Automated regression test exists:** No
**Safe to test in Codex:** Partial (parser fixtures yes; live browser no)
**Requires Render/runtime:** Yes
**PRODUCT DECISION REQUIRED:** No
**Recommended next action:** fixture-test wrong/duplicate/stale/short/malformed pages and measure browser processes/memory after repeated failures in staging.

### CV-FAMILY-001

**Area:** Timeframe families
**Status:** `PARTIAL`
**Relevant files:** `time_family_engine.py`, `coinglass_flow_engine.py`, `market_confidence_engine.py`
**Relevant functions/classes/tasks:** `TIME_FAMILIES`, `aggregate`, `oi_window_evaluator`, `flow_window_evaluator`
**What is being checked:** membership, weights, neutrality, partial/missing behavior, direction/evidence and correlated double counting.
**Actual current behavior:** NOW=`30m`/35; SHORT=`1h,4h`/30; MEDIUM=`12h,24h`/20; LONG=`48h,72h,7d`/15. Members divide by configured count, so missing members reduce coverage/net. Family neutrality is `|net| ≤ 0.05`; aggregate neutrality is score between -12 and +12. OI and Futures CVD are separate derivative modules; Spot is context only. Within-family related windows still contribute independently before averaging, by design.
**Expected behavior:** no accidental multiplication of correlated evidence; weights/thresholds exact and missing-family effect explicit.
**Evidence:** single shared aggregator. Product calibration proving that correlations are not overcounted is absent.
**Risk if incorrect:** **High**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes
**Requires Render/runtime:** No (calibration later uses historical data)
**PRODUCT DECISION REQUIRED:** No
**Recommended next action:** test missing/partial/conflicting families and formally approve correlation policy.

### CV-SPOT-001

**Area:** Spot CVD contract
**Status:** `PASS` (static contract only)
**Relevant files:** `market_confidence_engine.py`, `main.py`
**Relevant functions/classes/tasks:** `_spot_context`, `_conclusion`, `_confirmation`, `_market_evidence_block`
**What is being checked:** whether Spot is context, veto, vote, family or confirmation engine.
**Actual current behavior:** Spot is labeled secondary context and “does not vote in confirmation”; Confirmation support count uses Positioning and Futures Flow. Spot does not independently create full confirmation.
**Expected behavior:** secondary context must not silently become an independent vote; any strong-conflict veto requires explicit approved rule.
**Evidence:** display and confirmation implementation agree. No current Spot strong-conflict veto was found; this contradicts the conditional historical expectation if a veto was assumed.
**Risk if incorrect:** **High**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes
**Requires Render/runtime:** No
**PRODUCT DECISION REQUIRED:** Yes
**Recommended next action:** lock the current non-voting contract with a regression test and obtain product decision before adding any veto.

### CV-SCORE-001

**Area:** Scoring invariants / opposite score
**Status:** `PARTIAL`
**Relevant files:** `alert_engine.py`, `counter_score.py`, `analysis.py`, `main.py`
**Relevant functions/classes/tasks:** `_score_explicit_side`, `build_alert_opportunities`, `_gap_consensus_details`, `_target_proximity_points`, `calculate_counter_score`
**What is being checked:** separate LONG/SHORT, component sum/max/normalization, Gap, Cluster, Liquidity Balance, BTC, Consensus, Proximity, legacy/Magnet paths and boundaries.
**Actual current behavior:** both sides are scored explicitly before selection; final and counter scores clamp to `[0,100]`. The current card exposes Proximity `/25`, directional alignment/Consensus+BTC, Cluster `/30` split density/coverage, high-liquidity close-distance, Relative Gap `/15`, and liquidity balance bonus/penalty. Magnet states that it does not participate in legacy alert score. Static debug includes a component sum check.
**Expected behavior:** same component contract for both directions, no correlated double count, exact max/clamp and no silent score overwrite.
**Evidence:** `counter_score` calls the same internal directional component helper; no repository tests exist to verify all combinations.
**Risk if incorrect:** **Critical**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes
**Requires Render/runtime:** No
**PRODUCT DECISION REQUIRED:** No
**Recommended next action:** golden vectors for LONG/SHORT and counter, sum/max/clamp, missing components and every threshold ±epsilon.

### CV-PROX-001

**Area:** Continuous Proximity
**Status:** `PARTIAL`
**Relevant files:** `alert_engine.py`, `main.py`
**Relevant functions/classes/tasks:** `_target_proximity_points`, symbol dynamic-distance helpers, `_is_displayable_opportunity`
**What is being checked:** lower/preferred/dynamic upper bounds, continuous decay/endpoints and old step paths.
**Actual current behavior:** Proximity is calculated continuously in the score and display filter explicitly does not remove a scored active target merely for being below `0.8%`; such distance receives zero proximity. Dynamic per-asset maximum participates. No automated proof excludes an old helper overriding representative edge cases.
**Expected behavior:** test below lower; exact lower; preferred midpoint/end; decay midpoint; exact dynamic max; max+epsilon, with continuity and no step override.
**Evidence:** current scoring helper is the central call used for main and counter score.
**Risk if incorrect:** **High**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes
**Requires Render/runtime:** No
**PRODUCT DECISION REQUIRED:** No
**Recommended next action:** extract current constants in the test at runtime and parameterize the seven required points plus continuity around each boundary.

### CV-CONS-001

**Area:** Consensus
**Status:** `PARTIAL`
**Relevant files:** `alert_engine.py`, `analysis.py`, `main.py`
**Relevant functions/classes/tasks:** `_gap_consensus_details`, `_gap_consensus_points`, `_consensus_map`, `calculate_consensus`, `consensus`
**What is being checked:** trigger exclusion, six supporters, weighted Gap, missing/conflict, BTC weights and wording.
**Actual current behavior:** scoring Consensus excludes the trigger timeframe and weights support by Gap quality; with seven Max Pain timeframes, at most six supporters are eligible. A separate legacy `/consensus` command still calculates hits out of seven and correctly displays `/7`; this is not the scoring formula. Alert cards display `supporting/total` dynamically, not fixed `מתוך 7`.
**Expected behavior:** scoring and command semantics must remain distinguished; exact six eligible rows when complete; missing/conflict/weak/strong rules deterministic.
**Evidence:** two active consensus meanings coexist—directional Gap score and legacy seven-timeframe command. This is a historical expectation discrepancy, not automatically a bug.
**Risk if incorrect:** **High**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes
**Requires Render/runtime:** No
**PRODUCT DECISION REQUIRED:** Yes
**Recommended next action:** separate golden tests for scoring (trigger excluded) and `/consensus` display (seven-row command).

### CV-MAGNET-001

**Area:** Magnet Engine V1 / Liquidity V2 diagnostics
**Status:** `PARTIAL`
**Relevant files:** `magnet_v1.py`, `main.py`, `README.md`
**Relevant functions/classes/tasks:** `build_magnets`, `_build_candidate`, `_candidate_entries`, `_maximal_price_clusters`, `_liquidity_diagnostics`, `evaluate_confirmation`
**What is being checked:** Quality/formula, liquidity/spread/cluster, invalid/partial/cumulative/NaN, Growth/Coverage isolation.
**Actual current behavior:** Magnet builds active same-side price clusters, concentration quality and liquidity diagnostics. Liquidity V2 diagnostics use gross cumulative candidate/opposite liquidity and validity/consistency layers. `Growth` and `Coverage` are not inputs to the current Magnet V1 score; coverage-like cluster fields remain legitimate in legacy scoring/diagnostics. Magnet does not modify legacy alert score.
**Expected behavior:** invalid/None/NaN cannot inflate quality; non-monotonic cumulative layers are skipped consistently; Growth/Coverage remain outside Magnet score unless re-approved.
**Evidence:** module docstrings and score construction; full numeric contract lacks tests.
**Risk if incorrect:** **High**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes
**Requires Render/runtime:** No
**PRODUCT DECISION REQUIRED:** No
**Recommended next action:** golden vectors for spread, liquidity edge, monotonic/non-monotonic cumulative rows, partial/NaN/None and explicit Growth/Coverage mutation invariance.

### CV-MWATCH-001

**Area:** Magnet Watch
**Status:** `PARTIAL`
**Relevant files:** `main.py`
**Relevant functions/classes/tasks:** `watch_magnet_v1_cmd`, `watch_magnet_v1_stop_cmd`, `watch_magnet_v1_status_cmd`, `_ensure_watch_coordinator`, `_send_magnet_watch_reports`, `watch_on`, `watch_on_top8`
**What is being checked:** relationship, interval/threshold/cooldown, activation/cancel/restart/duplication.
**Actual current behavior:** `/watch_magnet_v1 SYMBOL` adds a subscription to `MAGNET_V1_WATCHES` and joins the same general coordinator/snapshot. It does not start its own DOM or derivatives collector. `/watch_on` and `/watch_on_top8` are general modes; specific per-symbol Watch is a separate legacy path. Magnet subscription state is in memory and is lost on restart.
**Expected behavior:** repeated activation idempotent, stop removes only intended subscription, coordinator stops only when no consumers, cadence aligned to Watch.
**Evidence:** shared `_ensure_watch_coordinator` and consumer check. Threshold/cooldown semantics need command-path boundary tests.
**Risk if incorrect:** **High**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes with fake bot
**Requires Render/runtime:** Yes for timing
**PRODUCT DECISION REQUIRED:** Yes
**Recommended next action:** repeated start/stop/status and shared-coordinator lifecycle tests, then staging timing observation.

### CV-WSYNC-001

**Area:** Watch synchronization
**Status:** `PARTIAL`
**Relevant files:** `main.py`, `coinglass_flow_foundation.py`, `market_confidence_engine.py`
**Relevant functions/classes/tasks:** `run_watch_cycle`, `_ensure_watch_derivatives_ready`, `_derivatives_generation_status`, `_next_aligned_watch_time`, `collect_live_rows_for_watch`
**What is being checked:** Max Pain, price, OI, Futures/Spot CVD, Magnet and Confirmation generation compatibility.
**Actual current behavior:** DOM and paired OI/CVD refresh run in parallel; analysis waits until completion or bounded timeout. Later cycles align to UTC 30-minute boundary plus default 135-second grace; minimum Watch interval is 30m. Generation status uses expected close plus timestamps/counts. Core readiness requires all target OI and Futures, while Spot is informational. There is no persisted generation ID shared by every row; on timeout/partial readiness, downstream freshness gates may still mix available data of different exact acquisition times.
**Expected behavior:** no decision unknowingly combines incompatible generations; partial status surfaced and stale evidence excluded.
**Evidence:** runtime status includes generation/counts, but compatibility is timestamp-derived rather than transactional snapshotting.
**Risk if incorrect:** **Critical**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes with fake clock/DB
**Requires Render/runtime:** Yes
**PRODUCT DECISION REQUIRED:** Yes
**Recommended next action:** fake-clock tests for exact close/grace/timeout and mixed generations; log decision-level evidence timestamps in staging validation.

### CV-WLIFE-001

**Area:** Watch lifecycle
**Status:** `PARTIAL`
**Relevant files:** `main.py`
**Relevant functions/classes/tasks:** `/watch_on`, `/watch_on_top8`, `/watch_stop`, `/watch_status`, `/watch_magnet_v1*`, specific-watch helpers, `_persist_watch_runtime`, `_restore_watch_runtime`
**What is being checked:** ON/OFF/STATUS/NOW-equivalent, repeated commands, cancellation/failure/restart/deploy, collector independence and state accuracy.
**Actual current behavior:** commands cover general ON, Top8 ON, stop and status; no distinct `/watch_now` handler exists, though activation triggers an immediate cycle. Stop cancels Watch coordination but not startup OI/CVD collectors. Runtime metadata is persisted in `bot_settings`, while active task/subscription state is not restored after restart. `_restore_watch_runtime` restores display metadata only. Unexpected coordinator failure can leave intended enabled flags/task state needing command reactivation.
**Expected behavior:** idempotent repeated commands; accurate status based on live task; stopping analysis never stops collection; restart semantics explicit.
**Evidence:** task-state helpers check `done()`; persistence helpers do not recreate tasks. Historical assumption of a NOW command is `NOT_APPLICABLE` as a separate command.
**Risk if incorrect:** **High**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes with fake Telegram
**Requires Render/runtime:** Yes for redeploy
**PRODUCT DECISION REQUIRED:** Yes
**Recommended next action:** command state-machine tests and explicit documentation/approval of non-restoration after deploy.

### CV-DERIV-001

**Area:** Derivatives engine
**Status:** `PARTIAL`
**Relevant files:** `market_confidence_engine.py`, `coinglass_oi_regime_service.py`, `coinglass_flow_engine.py`, `main.py`
**Relevant functions/classes/tasks:** `_positioning_module`, `_flow_module`, `_spot_context`, `_conclusion`, `_confirmation`, `combine`
**What is being checked:** exact engine/evidence/family/vote/veto/support/conflict meanings and “one derivatives engine only”.
**Actual current behavior:** Positioning (Price+OI) and Futures Flow are the two voting derivative engines. Each is internally weighted across four time families. Spot is non-voting context. Support counts only available, direction-aligned Positioning/Futures; conflict includes opposing voting modules and Early Shift conflict rules. Therefore bullish-looking raw OI can still fail to count because weighted direction is neutral, stale/unavailable, weak/family-filtered, or contrary to expected price direction. Display derives from the filtered modules, but no test proves all branches.
**Expected behavior:** displayed engine count must reflect the exact post-freshness/post-family/post-direction set; unavailable reason surfaced.
**Evidence:** module construction and `_confirmation` support count.
**Risk if incorrect:** **High**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes
**Requires Render/runtime:** No for logic; Yes for historical incident replay
**PRODUCT DECISION REQUIRED:** No
**Recommended next action:** replay matrices separating raw bullish OI from weighted/fresh/expected alignment and assert message reason.

### CV-CONF-001

**Area:** Confirmation
**Status:** `PARTIAL`
**Relevant files:** `market_confidence_engine.py`, `magnet_v1.py`, `main.py`
**Relevant functions/classes/tasks:** `_confirmation`, `combine`, `attach_to_opportunities`, `evaluate_confirmation`, `_confirmation_transition_message`
**What is being checked:** legacy Max Pain/Magnet/mixed paths; evidence, thresholds, Early Shift, cluster/proximity/BTC.
**Actual current behavior:** regular market Confirmation is read-only over legacy Max Pain score plus Positioning and Futures Flow; Spot is context. Base score threshold is `>=65`; Strong threshold is `>=75`, with aligned support requirements and stronger module scores. Early Shift can conflict. Magnet has its own `evaluate_confirmation`, and Combined Confirmation may combine regular/strong/high-score/anomaly/Magnet/liquidity occurrences. Thus repository contains parallel regular and Magnet confirmation paths rather than one replacement path.
**Expected behavior:** paths must remain named/distinct; filtered/stale evidence cannot confirm; Max Pain score remains unchanged.
**Evidence:** regular engine’s note says score/ranking unchanged; combined aggregator explicitly adds Magnet occurrences. Max Pain cluster/proximity/BTC affect legacy score indirectly rather than separate Confirmation votes.
**Risk if incorrect:** **Critical**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes
**Requires Render/runtime:** Yes for natural signals
**PRODUCT DECISION REQUIRED:** Yes
**Recommended next action:** path-specific truth tables including missing/stale, Early Shift, one/two engines, Magnet-only and mixed cases.

### CV-TIER-001

**Area:** Confirmation tiers / numeric boundaries
**Status:** `PARTIAL`
**Relevant files:** `market_confidence_engine.py`, `main.py`
**Relevant functions/classes/tasks:** `_confirmation`, `_high_score_83_transition_message`, `_combined_confirmation_candidates`, `_collect_special_transition_messages`
**What is being checked:** base/high/Strong/combined/anomaly tiers and comparator differences.
**Actual current behavior:** regular Confirmation uses score `>=65`; Strong uses `>=75` plus two aligned strong core engines. Separate high-state alert uses `>=83`. Combined high-score occurrence uses strictly `>80`, not `>=80`. Combined requires at least two signal keys. The historical 65–74.99 band is simply base Confirmation when evidence qualifies, not a separately named tier.
**Expected behavior:** for every current threshold test `x-ε`, `x`, `x+ε`; intentional `>=` versus `>` differences documented.
**Evidence:** constants `SPECIAL_HIGH_SCORE_THRESHOLD=83`, `COMBINED_HIGH_SCORE_THRESHOLD=80`, `COMBINED_MIN_SIGNALS=2`; comparisons differ exactly as stated.
**Risk if incorrect:** **Critical**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes
**Requires Render/runtime:** No
**PRODUCT DECISION REQUIRED:** Yes
**Recommended next action:** boundary table at 64.99/65/65.01, 74.99/75/75.01, 79.99/80/80.01 and 82.99/83/83.01, including evidence gates.

### CV-COMB-001

**Area:** Combined Confirmation
**Status:** `PARTIAL`
**Relevant files:** `main.py`, `magnet_v1.py`
**Relevant functions/classes/tasks:** `_combined_magnet_confirmations`, `_combined_confirmation_candidates`, `_collect_combined_confirmation_messages`, `_combined_group_key`
**What is being checked:** minimum independent occurrences, correlation, state transitions, repeated/new/stale/direction/restart/cooldown.
**Actual current behavior:** candidate requires at least two signal keys among regular confirmations by timeframe, Strong by timeframe, score-over-80 by timeframe, 3+ anomaly types, confirmed Magnet, and same-side liquidity imbalance ≥60%. Keys distinguish occurrences/timeframes, so multiple expressions and multiple timeframes can satisfy the count even when economically correlated. `COMBINED_CONFIRMATION_STATE` suppresses unchanged sets and re-alerts on new evidence; it is in-memory and lost on restart.
**Expected behavior:** only explicitly approved independent evidence counts; stale/invalidated keys removed; duplicates and restart behavior deterministic.
**Evidence:** README describes “occurrences,” while task expectation stresses true independence—this is a material terminology/design discrepancy requiring approval, not a silent assumption.
**Risk if incorrect:** **Critical**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes
**Requires Render/runtime:** Yes for natural/restart alerts
**PRODUCT DECISION REQUIRED:** Yes
**Recommended next action:** define an independence taxonomy, then test duplicate family/timeframe/evidence, set growth/shrink, direction flip, cooldown and restart.

### CV-STATE-001

**Area:** Confirmation / restart state
**Status:** `PARTIAL`
**Relevant files:** `main.py`
**Relevant functions/classes/tasks:** `CONFIRMATION_STATE`, `HIGH_SCORE_83_STATE`, `COMBINED_CONFIRMATION_STATE`, `MAGNET_V1_WATCHES`, `SPECIFIC_WATCHES`, `WATCH_RUNTIME`, `alert_history`, `PROCESSED_UPDATE_IDS`
**What is being checked:** persistence/loss after crash, manual restart and Render deploy; duplicate alert consequences.
**Actual current behavior:** three confirmation maps, Watch subscriptions and processed update IDs are memory-only. General runtime metadata is persisted to `bot_settings`; alert fingerprints/cooldown are persisted in `alert_history`. Restart can therefore repeat transition/combined messages and lose subscriptions, while ordinary Watch fingerprint suppression may survive. Intent is not explicitly codified.
**Expected behavior:** every state’s restart contract documented; accidental duplicate alerts detectable.
**Evidence:** no DB serialization for confirmation maps/subscription dictionaries.
**Risk if incorrect:** **High**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes on isolated temporary DB
**Requires Render/runtime:** Yes
**PRODUCT DECISION REQUIRED:** Yes
**Recommended next action:** test simulated process reset without changing current persistence, and obtain product decision on intended duplicates.

### CV-ALERT-001

**Area:** Alert generation / deduplication
**Status:** `PARTIAL`
**Relevant files:** `alert_engine.py`, `counter_score.py`, `main.py`, `alert_summary.py`
**Relevant functions/classes/tasks:** `build_alert_opportunities`, `_is_displayable_opportunity`, `_alert_fingerprint`, `_alert_recently_sent`, `_remember_alert`, `run_watch_cycle`, `_send_alert_with_confirmation`
**What is being checked:** candidates, score/direction/opposite/evidence/missing data, duplicate loops and cooldown.
**Actual current behavior:** Watch filters at `score >= WATCH_PRIORITY_THRESHOLD` (default 70), fingerprints logical symbol/timeframe/side/target attributes and queries `alert_history` within default 60m cooldown. Alert commands and Watch have different locks; simultaneous manual and automatic paths are not proven to share dedup behavior for every message type. Confirmation transition messages have separate in-memory state.
**Expected behavior:** same logical alert sent once across overlapping producers; evidence and opposite score correspond to chosen direction; unavailable inputs visible.
**Evidence:** DB fingerprint protects the main Watch alert, but combined/special transition state uses other mechanisms.
**Risk if incorrect:** **High**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes with fake bot/temp DB
**Requires Render/runtime:** Yes
**PRODUCT DECISION REQUIRED:** Yes
**Recommended next action:** concurrent producer tests and one canonical logical-alert identity matrix across normal/special/combined messages.

### CV-TG-001

**Area:** Telegram display
**Status:** `PARTIAL`
**Relevant files:** `main.py`, `alert_summary.py`
**Relevant functions/classes/tasks:** `_alert_card`, `_market_evidence_block`, `_flow_detail_block`, `_confirmation_transition_message`, `_combined_confirmation_message`, command handlers
**What is being checked:** LONG/SHORT, scores, OI/CVD/Spot/Magnet/tiers, stale/unavailable/source, HTML/Unicode/legacy wording/direction.
**Actual current behavior:** major cards use HTML escaping for dynamic values in many paths and explicitly label Spot secondary. Stale/unavailable flow/regime states have display branches. Source is shown for live price/OI diagnostics, but not uniformly in every card. A complete audit of every interpolated symbol/error string and 4096-character Telegram limit has no automated coverage. Hebrew and English technical terms coexist intentionally.
**Expected behavior:** display cannot say bullish/bearish/confirmed when evidence was filtered, stale, unavailable or vetoed; all dynamic HTML safe and calculation labels exact.
**Evidence:** message builders largely consume the same `market_evidence` object; no snapshot/golden tests.
**Risk if incorrect:** **High**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes without sending messages
**Requires Render/runtime:** Yes for Telegram rendering
**PRODUCT DECISION REQUIRED:** No
**Recommended next action:** golden-string tests for all states, malicious symbols/text, missing fields, direction mismatch and message length; validate in a non-production chat.

### CV-TV-001

**Area:** TradingView / Technical Shadow Mode
**Status:** `PARTIAL`
**Relevant files:** `main.py`, `technical_signal_store.py`
**Relevant functions/classes/tasks:** `_tradingview_authorized`, `tradingview_webhook`, `normalize_payload`, `_insert_technical_signal`, `technical_status_api`, `technical_status_cmd`
**What is being checked:** secret, malformed/missing fields, normalization/timestamps, duplicate/fingerprint, DB failure, unknown/stale signals and scoring isolation.
**Actual current behavior:** secret may be supplied in header/query/body as supported; payload normalizes symbol/timeframe/direction/score/timestamps and validates score 0..100. DB has unique fingerprint and `ON CONFLICT DO NOTHING`. Endpoint stores/displays only; no import from technical store into deterministic scoring was found. There is no explicit maximum event-age rejection; old valid payloads may be stored.
**Expected behavior:** authenticated, normalized, deduplicated and isolated Shadow Mode; unknown/stale events explicitly rejected or labeled per approved contract.
**Evidence:** module docstring and absence from scoring call paths prove static isolation; live auth/proxy behavior requires runtime.
**Risk if incorrect:** **High**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes using local request objects/temp DB
**Requires Render/runtime:** Yes for public webhook
**PRODUCT DECISION REQUIRED:** No
**Recommended next action:** endpoint contract tests for auth locations, malformed JSON, all missing fields, fingerprint, DB error, unknown TF/signal and stale timestamp.

### CV-CONFIG-001

**Area:** Configuration drift
**Status:** `FAIL`
**Relevant files:** `env.example`, `README.md`, all Python modules
**Relevant functions/classes/tasks:** module-level `os.getenv` calls
**What is being checked:** code names/defaults versus documentation/example; obsolete/duplicate variables.
**Actual current behavior:** code uses variables absent from `env.example`, including `TELEGRAM_BOT_TOKEN`, `DATABASE_URL`, `DB_PATH`, `PORT`, `PUBLIC_URL`, `COINGLASS_MAX_PAIN_URL`, `COINGLASS_API_URL`, `TOP_COINS_LIMIT`, `COLLECT_INTERVAL_MINUTES`, `MAX_SECONDS_PER_TIMEFRAME`, `RETRY_SLEEP_SECONDS`, `BYBIT_BASE_URL`, `BYBIT_TICKERS_ENDPOINT`, `BYBIT_PRICE_TIMEOUT_SECONDS`, `COINGLASS_CVD_TIMESTAMP_MODE`, and `FLOW_COLLECTION_INTERVAL_MINUTES`. `env.example` duplicates Binance Spot entries. README is stage-history rather than a canonical setup contract and retains older flow cadence statements before the 5m update.
**Expected behavior:** one canonical name/default per setting; secrets represented without values; obsolete names marked.
**Evidence:** AST inventory of literal `os.getenv` calls compared to `env.example`.
**Risk if incorrect:** **Medium**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes
**Requires Render/runtime:** No
**PRODUCT DECISION REQUIRED:** No
**Recommended next action:** add a future drift test that compares code names to the canonical example; configuration itself must be changed only in a separately approved task.

### CV-LEGACY-001

**Area:** Dead / legacy / deprecated code
**Status:** `PARTIAL`
**Relevant files:** `main.py`, `analysis.py`, `coinglass_dom_reader.py`, `coinglass_flow_foundation.py`, `README.md`
**Relevant functions/classes/tasks:** legacy `/consensus`, API Max Pain fetch/decode helpers, `_restore_watch_runtime`, specific Watch, compatibility wrappers
**What is being checked:** classify without removal.
**Actual current behavior:** classification from static references: `ACTIVE`—DOM collector, general/Top8/Magnet Watch, Price+OI/CVD, regular/combined Confirmation; `LEGACY-BUT-USED`—seven-timeframe `/consensus`, legacy Max Pain score, specific Watch and counter score; `POSSIBLY-DEAD`—older encrypted CoinGlass API fetch/decode path where DOM is primary, persistence helpers that restore metadata but not tasks; `UNKNOWN`—generic DOM fallback parsers and compatibility wrappers exposed to possible external callers; `CONFIRMED-DEAD`—none can be safely claimed without reachability/runtime evidence.
**Expected behavior:** retain until separate approval; no historical helper assumed dead solely from lack of internal call.
**Evidence:** static references and registered Telegram handlers.
**Risk if incorrect:** **Medium**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes (reachability/static)
**Requires Render/runtime:** Partial
**PRODUCT DECISION REQUIRED:** No
**Recommended next action:** generate call graph and runtime command usage inventory before any deprecation proposal.

### CV-TEST-001

**Area:** Automated test inventory
**Status:** `FAIL`
**Relevant files:** entire tracked repository
**Relevant functions/classes/tasks:** all modules
**What is being checked:** unit/integration files, fixtures/mocks and coverage by subsystem in the source repository; this check does not require tests to be packaged in a production ZIP.
**Actual current behavior:** `git ls-files` contains no `test_*.py`, `*_test.py`, `tests/`, fixtures, or test configuration. There is no current automated regression coverage for Max Pain, Proximity, Consensus, Cluster, Counter Score, OI, CVD, OI Regime, Magnet, Confirmation, Strong/Combined Confirmation, Watch, scheduling, DB, providers or Telegram. Inline CLI/debug functions are not a test suite. Historical removal of tests from production ZIP artifacts may explain their absence from release packages, but it does not establish that tests should be absent from the GitHub/Codex source repository.
**Expected behavior:** deterministic unit coverage plus isolated integration coverage for critical call paths remains tracked in source control. Production ZIP generation excludes those tests and their support artifacts by default.
**Evidence:** complete tracked-file inventory. Historical tests, if any, are absent from this branch and must not be credited as current source coverage; historical ZIP-cleaning rules remain valid release hygiene.
**Risk if incorrect:** **Critical — Development assurance / regression coverage failure; not a production runtime failure.**
**Automated regression test exists:** No
**Safe to test in Codex:** Yes once tests exist
**Requires Render/runtime:** No for unit suite
**PRODUCT DECISION REQUIRED:** No
**Recommended next action:** in a separate approved implementation task, create and retain tests in the source repository according to the safe execution order; continue excluding them from production ZIP artifacts.

### CV-HIST-001

**Area:** Historical regression cases
**Status:** `PARTIAL`
**Relevant files:** all core modules above
**Relevant functions/classes/tasks:** locks/tasks, schemas, regime/flow/scoring/Watch/provider/display paths
**What is being checked:** permanent preservation of prior failures/requirements.
**Actual current behavior:** static guards exist for duplicate Watch coordination, positive/negative flow distributions, candle PK dedup, active/weekend composition, multi-window OI, Watch/collector separation, LONG/SHORT scoring, provider isolation, initialized `weighted`, Bybit/HYPE fallback, and display states. DDL-in-recurring remains a current failure; PostgreSQL deadlock freedom and fresh generation behavior need runtime.
**Expected behavior:** each historical case has a dedicated deterministic regression or explicit `NOT_APPLICABLE` reason.
**Evidence:** detailed matrix below.
**Risk if incorrect:** **Critical**
**Automated regression test exists:** No
**Safe to test in Codex:** Partial
**Requires Render/runtime:** Yes for locks/live behavior
**PRODUCT DECISION REQUIRED:** No
**Recommended next action:** implement the matrix as named tests before functional changes.

#### Historical regression matrix

| Case | Current status | Permanent check |
|---|---|---|
| duplicate scheduler/task creation | `PARTIAL` | repeated commands and simultaneous refresh callers yield one task/scan |
| PostgreSQL deadlocks | `NEEDS_RUNTIME` | isolated PostgreSQL concurrent startup/live/backfill + lock log |
| schema initialization inside recurring work | `FAIL` | static call-graph must reject collector→DDL |
| positive/negative/zero CVD | `PARTIAL` | signed synthetic fixtures and display |
| stale CVD rejection | `PARTIAL` | threshold−ε/exact/+ε |
| CVD DB deduplication | `PARTIAL` | repeated `(symbol,candle_time)` UPSERT |
| Active vs Weekend baseline separation | `PARTIAL` | boundary/session-composition fixtures; mechanism still exists |
| Price+OI multi-window | `PARTIAL` | all eight windows plus missing references |
| Confirmation with fresh compatible CVD | `NEEDS_RUNTIME` | generation mismatch fixture + staging timestamps |
| Early Shift | `PARTIAL` | aligned/opposed Positioning and Futures paths |
| Watch stop not stopping collection | `PARTIAL` | fake task lifecycle and Render observation |
| LONG/SHORT independent calculation | `PARTIAL` | mirrored golden score vectors |
| CoinGlass/provider failure isolation | `PARTIAL` | mocked one-provider/one-symbol failures |
| OI `weighted` undefined local | `PARTIAL` | execute valid/invalid/missing branches |
| Bybit 403 fallback | `PARTIAL` | mock 403 then next HYPE provider success |
| HYPE fallback | `PARTIAL` | each provider success/failure order and source |
| Telegram display consistency | `PARTIAL` | golden cards for stale/unavailable/conflict |
| historical Spot-as-full-confirmation design | `NOT_APPLICABLE` | current contract is secondary non-voting context |
| separate `/watch_now` command | `NOT_APPLICABLE` | activation performs immediate cycle; command is absent |

---

## 3. Live / Render validation

כל הפריטים הבאים מתחילים ב־`NEEDS_RUNTIME`; אין לשנות ל־`PASS` על סמך code review. לכל הרצה יש לתעד Render service/version, commit SHA, UTC range, sanitized logs, DB backend, configuration names (לא secret values), expected/actual והחלטת rollback.

| ID | Status | Check | Acceptance evidence | Minimum observation |
|---|---|---|---|---|
| `RT-START-01` | `NEEDS_RUNTIME` | Render startup and one initialization | one server bind, one schema phase, no repeated startup tasks | deploy + restart |
| `RT-COL-01` | `NEEDS_RUNTIME` | collector startup/cadence | one OI 30m loop, one Flow configured poll loop, no overlap | 6h then 1–2 days |
| `RT-DB-01` | `NEEDS_RUNTIME` | DB locks/deadlock monitoring | no `DeadlockDetected`; advisory owners released; bounded transactions | 1–2 days and concurrent staging exercise |
| `RT-TG-01` | `NEEDS_RUNTIME` | Telegram webhook/commands | authenticated webhook, dedup update, ON/OFF/STATUS/repeated commands | controlled non-production chat |
| `RT-CVD-01` | `NEEDS_RUNTIME` | real Futures and Spot CVD | separate tables/sources, correct closed timestamps/freshness | bullish, bearish, zero/near-zero examples |
| `RT-WATCH-01` | `NEEDS_RUNTIME` | Watch alignment and generation | cycle at close+grace; evidence timestamps compatible; timeout surfaced | ≥4 cycles and provider delay case |
| `RT-CONF-01` | `NEEDS_RUNTIME` | Confirmation live behavior | filtered evidence equals display | natural base event |
| `RT-CONF-02` | `NEEDS_RUNTIME` | Strong Confirmation | all gates and exact message from natural occurrence | until event or approved replay |
| `RT-COMB-01` | `NEEDS_RUNTIME` | Combined Confirmation | independent approved keys, dedup and state change | natural occurrence + repeat |
| `RT-RESTART-01` | `NEEDS_RUNTIME` | crash/restart/redeploy | documented lost/persisted state and duplicate behavior | manual staging redeploy |
| `RT-STAB-01` | `NEEDS_RUNTIME` | stability | no leaked browser/tasks, runaway DB locks, loop death or unrelated-engine crash | continuous 24–48h |

אין להפעיל בדיקות אלה אוטומטית מול production. יש להשתמש ב־staging, DB מבודד ו־Telegram chat ייעודי; natural occurrence אינו מצדיק יצירת trade/alert מלאכותי ב־production.

---

## Current architecture validation map

1. **Ingress:** Telegram `POST /telegram`; TradingView Shadow Mode `POST /webhooks/tradingview`; health/status GET endpoints.
2. **Max Pain:** Playwright DOM → atomic seven-timeframe rows → `max_pain_snapshots` → `alert_engine` scores LONG/SHORT → cards/Watch.
3. **Price:** Binance Futures → Binance Spot; HYPE-specific fallbacks through Bybit/Hyperliquid/CoinGecko/CoinPaprika; source metadata retained.
4. **Price+OI:** CoinGlass aggregated OI + live price → `oi_regime_snapshots`; `oi_price_history` supplies historical references; eight windows → four time families.
5. **Flow:** CoinGlass official Futures/Spot 30m history → separate tables → continuous CVD/baselines → four time families.
6. **Market evidence:** Positioning + Futures are voting engines; Spot is secondary context; Confirmation is read-only relative to Max Pain score.
7. **Magnet:** independent Magnet V1 candidates/confirmation and Liquidity V2 diagnostics; shared Watch snapshot.
8. **Scheduling:** API-key-gated startup loops; Watch is manual; refresh single-flight and PostgreSQL process locks exist.
9. **State:** mixed persistence—snapshots/settings/history in DB, transition/subscription maps in memory.
10. **Shadow Mode:** normalized TradingView records are isolated from deterministic scoring.

## Product Decisions Required Before Technical Fixes

Repository-derived facts are authoritative for describing the **CURRENT implementation**. Historical specifications, product discussions and `00_CURRENT_STATE` may supply an expected alternative, but they must not overwrite audit findings. Any discrepancy between this repository audit and `00_CURRENT_STATE` must be reconciled in a later, explicitly approved review before code or status changes are made.

### PD-01 — Spot CVD vote/veto/context role

- **Current code behavior:** Spot CVD is secondary, non-voting and non-veto context; Positioning and Futures Flow are the voting derivatives engines.
- **Historical/expected alternative if known:** historical product discussions considered Spot CVD as strong-conflict context or a veto, without necessarily promoting it to a full independent vote.
- **Why it is not yet safe to call this a technical bug:** the implementation is internally clear and consistent, but the approved product contract for conflict/veto behavior is not present in this repository.
- **What explicit decision is required:** approve one role—display-only context, strong-conflict veto, family evidence or independent vote—and define freshness, strength and conflict boundaries.
- **Which validation IDs depend on the decision:** `CV-SPOT-001`, `CV-FAMILY-001`, `CV-WSYNC-001`, `CV-DERIV-001`, `CV-CONF-001`, `CV-TG-001`.

### PD-02 — Combined Confirmation independence taxonomy

- **Current code behavior:** Combined Confirmation counts distinct occurrences/keys, including timeframe-specific regular/Strong confirmations, score-over-80, anomalies, Magnet and liquidity imbalance.
- **Historical/expected alternative if known:** the expected safety invariant is a minimum number of genuinely independent signals, with correlated expressions of one underlying family not automatically counted twice.
- **Why it is not yet safe to call this a technical bug:** “economic independence” has no formal taxonomy in the repository; the current occurrence-based implementation matches its local documentation.
- **What explicit decision is required:** define approved signal families, correlation groups, whether multiple timeframes count separately, and which combinations satisfy `COMBINED_MIN_SIGNALS`.
- **Which validation IDs depend on the decision:** `CV-FAMILY-001`, `CV-COMB-001`, `CV-ALERT-001`, `CV-TG-001`, `CV-HIST-001`.

### PD-03 — Restart/redeploy persistence and re-alert behavior

- **Current code behavior:** `CONFIRMATION_STATE`, `HIGH_SCORE_83_STATE`, `COMBINED_CONFIRMATION_STATE`, Watch subscriptions and processed Telegram update IDs are memory-only; alert history and some Watch runtime metadata are persisted. Restart may repeat transition alerts and lose subscriptions.
- **Historical/expected alternative if known:** persistence/restore helpers and operational expectations may imply recovery across Render deploys, but the current code does not restore active subscriptions or transition maps.
- **Why it is not yet safe to call this a technical bug:** either at-most-once continuity or a clean restart/re-alert policy can be valid; the intended operational behavior is not explicitly approved.
- **What explicit decision is required:** specify which states survive restart, whether subscriptions auto-resume, when a repeated alert is acceptable, retention duration and whether deploy-generated re-alerts are intentional.
- **Which validation IDs depend on the decision:** `CV-MWATCH-001`, `CV-WLIFE-001`, `CV-COMB-001`, `CV-STATE-001`, `CV-ALERT-001`, `RT-RESTART-01`.

### PD-04 — Confirmation tier semantics

- **Current code behavior:** regular Confirmation begins at score `>=65`; Strong Confirmation begins at `>=75` and also requires its evidence gates. A separate transition is `>=83`, while Combined high-score evidence uses `>80`.
- **Historical/expected alternative if known:** historical specifications discussed lower bands and other high-score boundaries that may not map one-to-one to the current named tiers.
- **Why it is not yet safe to call this a technical bug:** the repository comparisons are unambiguous and internally active; changing them to match historical wording would alter production thresholds without an approved reconciliation.
- **What explicit decision is required:** approve tier names, numerical boundaries, evidence gates and the intentional distinction between `>=80`, `>80`, `>=83`, regular and Strong semantics.
- **Which validation IDs depend on the decision:** `CV-SCORE-001`, `CV-CONF-001`, `CV-TIER-001`, `CV-COMB-001`, `CV-TG-001`, `CV-HIST-001`.

### PD-05 — Consensus command versus scoring semantics

- **Current code behavior:** scoring Consensus excludes the trigger timeframe and therefore has at most six supporting timeframes; the legacy `/consensus` command independently reports direction hits across up to seven.
- **Historical/expected alternative if known:** historical wording such as `מתוך 7` has sometimes been treated as a display defect even when it belongs to the separate seven-timeframe command.
- **Why it is not yet safe to call this a technical bug:** two internally coherent features share a name; the expected product terminology and whether they should converge are undecided.
- **What explicit decision is required:** approve separate names/contracts or one unified Consensus definition, and specify display wording for each output.
- **Which validation IDs depend on the decision:** `CV-CONS-001`, `CV-SCORE-001`, `CV-TG-001`.

### PD-06 — Watch immediate-cycle/NOW and restoration semantics

- **Current code behavior:** there is no distinct `/watch_now` command; activation starts an immediate cycle, and persisted runtime metadata does not recreate Watch after restart.
- **Historical/expected alternative if known:** validation requirements refer to NOW and restore behavior as potentially distinct lifecycle operations.
- **Why it is not yet safe to call this a technical bug:** immediate activation may intentionally satisfy NOW, while automatic restore may intentionally be disabled for operational safety.
- **What explicit decision is required:** decide whether NOW needs a separate command and whether Watch should remain manual after restart or restore prior subscriptions.
- **Which validation IDs depend on the decision:** `CV-MWATCH-001`, `CV-WLIFE-001`, `CV-STATE-001`, `RT-TG-01`, `RT-RESTART-01`.

## Critical blockers

- `CV-DB-001`: DDL-capable initialization remains reachable inside recurring/read/write paths, contrary to the project invariant.
- `CV-TEST-001`: there is no tracked automated test suite. This is a **development assurance / regression coverage blocker**, not a production availability or runtime blocker, and it does not invalidate the historical policy of excluding tests from production ZIP artifacts.
- `CV-WSYNC-001`: generation compatibility is derived from timestamps/counts, not an atomic cross-stream generation; timeout/partial behavior needs proof.
- `CV-COMB-001`: “occurrences” can be correlated; true independence has not been formally guaranteed.
- `CV-SCORE-001` / `CV-CONF-001`: critical scoring and confirmation paths have no golden/boundary regressions.

## High-priority validation

- Concurrency ownership, collector/backfill overlap, cancellation and cross-process advisory locks.
- Exact CVD candle semantics, freshness, gaps and continuous math.
- Price+OI invalid/stale histories and the `weighted` regression.
- Confirmation/Strong/Combined truth tables and thresholds.
- Provider failure matrix, especially Bybit 403 and HYPE source reporting.
- Telegram output matching post-filter evidence.

## Medium-priority validation

- Configuration drift and canonical defaults.
- Full DOM parser/browser lifecycle and expected row shape.
- Restart persistence intent for subscriptions and transition maps.
- Legacy/reachability inventory and command semantics.

## Low-priority validation

- Formatting refinements that do not change meaning.
- Classification of currently `UNKNOWN` compatibility wrappers after usage evidence.
- Documentation wording differences that are display-only.

## Runtime-only Render checks

המקור המחייב הוא טבלת `RT-*`: startup, cadence, locks/deadlocks, Telegram webhook/commands, real Futures/Spot CVD, Watch timing/generations, natural Confirmation/Strong/Combined, restart/redeploy and 24–48h stability.

## Existing automated regression coverage

אין test files, fixtures, mocks, unit tests או integration tests tracked במאגר המקור בענף הנוכחי. קיימים defensive checks בתוך production code (atomic DOM validation, PK/UPSERT, status objects, sum debug), אך הם אינם automated regression suite. ממצא זה מתייחס ל־source repository בלבד; production ZIP אמור להישאר ללא tests כברירת מחדל.

## Missing regression coverage

כל התחומים המבוקשים חסרים כיסוי במאגר המקור: Max Pain, Proximity, Consensus, Cluster, Counter Score, OI, CVD, OI Regime, Magnet, Confirmation, Strong Confirmation, Combined Confirmation, Watch, scheduling, DB/migrations, providers, TradingView and Telegram. זהו פער development/regression safety ולא ראיה לכשל production runtime. יש להשאיר בדיקות עתידיות tracked ב־GitHub/Codex גם כאשר הן מוחרגות מ־production ZIP. סדר ההשלמה המומלץ מופיע בהמשך המסמך.

## Source Repository Tests and Production Release Packaging

### Permanent release rule

> **Tests belong to the source repository and validation workflow, but are excluded from production ZIP artifacts by default.**

כל test עתידי ש־Codex יוצר כפוף להפרדה הבאה:

#### SOURCE REPOSITORY

- Automated regression tests נשמרים ב־GitHub/Codex source control כנכסי development ו־validation.
- בדיקות יכולות להימצא תחת `tests/`, בקבצי `test_*.py` או `*_test.py`, ובנתיבי fixtures, mocks או test-support מתאימים אחרים.
- עצם קיום הבדיקות במאגר אינו גורם להפעלה אוטומטית שלהן ב־production.
- יש לשמור את הבדיקות לצד שינויי הקוד הרלוונטיים כדי לאפשר regression validation חוזר, review ותחזוקה ארוכת טווח.

#### PRODUCTION ZIP / RENDER RELEASE

- Production ZIP artifacts מוחרגים כברירת מחדל מ־`tests/`, `test_*.py`, `*_test.py`, test fixtures, test caches ו־generated testing artifacts, אלא אם ניתנה בקשה מפורשת אחרת.
- הסרת בדיקות מ־production ZIP היא release hygiene תקינה ואינה runtime defect.
- החרגת tests מארטיפקט ההפצה אינה הרשאה למחוק אותם ממאגר המקור או לוותר על development regression coverage.
- Runtime-only validation נשאר מסלול נפרד ומתבצע ב־Render/staging כאשר פריט מסומן `NEEDS_RUNTIME`.

### Safe future workflow

1. Codex יוצר ומתחזק automated tests במאגר המקור.
2. Codex מריץ את הבדיקות הבטוחות והרלוונטיות לפני מסירת שינויי קוד.
3. הבדיקות נשארות tracked ב־GitHub.
4. יצירת production ZIP מחריגה tests ו־testing artifacts.
5. בדיקות runtime-only נשארות נפרדות ומתבצעות ב־Render/staging לפי הצורך.

כלי packaging עתידי חייב לאמת את רשימת ההחרגות בלי לשנות את test inventory במאגר המקור. אין לפרש release artifact נקי מבדיקות כהוכחה לכך שה־source repository מספק regression coverage.

## Historical regression checks

המטריצה תחת `CV-HIST-001` היא registry קבוע. אין למחוק entry כאשר implementation משתנה: יש להעביר ל־`NOT_APPLICABLE`, להסביר מה הוסר ובאיזה commit, ולהוסיף בדיקה שמוודאת שהמנגנון הישן אינו reachable אם זה חשוב לבטיחות.

---

## Future Capability Validation Registry

### חוזה אחיד לכל capability עתידי

לכל entry יש לרשום: responsibility; input/output contract; dependencies; interaction with existing engines; failure isolation; missing/stale-data behavior; persistence; concurrency; scoring/double-counting; Telegram/output; regression tests; runtime activation checks. ברירת המחדל להלן היא `NOT_IMPLEMENTED` אלא אם audit עתידי מוכיח implementation מלא; קיום helper דומה אינו מספיק.

### FC-OI — Continuous OI Scoring

**Lifecycle:** `NOT_IMPLEMENTED` (ה־weighted family evidence הקיים אינו score רציף מאושר לתוך Max Pain score).
**Responsibility:** להמיר שינויי OI איכותיים לציון רציף סימטרי, בלי להחליף raw regime.
**Input / Output:** timestamped Price+OI windows + quality/source → bounded score, direction, confidence, reasons and unavailable status.
**Dependencies / interaction:** OI history, families, Confirmation; no duplicate vote with current Positioning.
**Failure/missing/persistence/concurrency:** stale/missing yields unavailable, never zero evidence; reuse collector generation; persist version/calibration metadata.
**Scoring/output:** monotonicity, exact bounds, family behavior, missing data, double-count prevention; Telegram distinguishes continuous score from deterministic regime.
**Required regression/runtime:** monotonic/property tests, mirrored signs, calibration against historical outcomes, staging shadow output and 1–2 day data-quality observation.

### FC-CVD — Continuous CVD Scoring

**Lifecycle:** `NOT_IMPLEMENTED` as a production scoring component (weighted flow evidence exists).
**Responsibility:** continuous normalized flow score without merging Futures/Spot semantics.
**Input / Output:** closed timestamped CVD deltas/baselines per market → separate bounded scores, direction, freshness and provenance.
**Dependencies / interaction:** flow tables/families/Confirmation; Spot role remains separately approved.
**Failure/missing/persistence/concurrency:** stale unavailable, not neutral; extreme/invalid safe; use same generation and version persisted with result.
**Scoring/output:** bullish/bearish symmetry, continuous normalization/extremes, family and double-count controls; Telegram labels market/source.
**Required regression/runtime:** exact freshness and normalization boundaries, Futures-vs-Spot separation, live positive/negative examples.

### FC-LIQ — Liquidity Intelligence

**Lifecycle:** `IMPLEMENTED_UNVALIDATED` only for existing Liquidity V2 diagnostics; the broader future capability is `NOT_IMPLEMENTED`.
**Responsibility:** Growth, strengthening/weakening, Migration, Jump, persistence, Net Liquidity and magnet evolution.
**Input / Output:** versioned cumulative liquidity snapshots → event/state sequence with quality and provenance.
**Dependencies / interaction:** Max Pain/DOM, Magnet, Range/Cluster; failure cannot stop Watch.
**Failure/missing/persistence/concurrency:** gaps and non-monotonic snapshots explicit; evolution requires persistent state and atomic generation processing.
**Scoring/output:** **Growth remains outside current Magnet V1 score unless separately re-approved**; prevent duplicate liquidity votes; show diagnostic vs scoring state.
**Required regression/runtime:** sequence/property tests, restart recovery, concurrent snapshot handling, provider gaps and staged evolution replay.

### FC-RANGE — Range Engine

**Lifecycle:** `NOT_IMPLEMENTED` (a `/range` command/legacy calculations do not establish this future engine contract).
**Responsibility:** validated static/dynamic price ranges and relationships to clusters.
**Input / Output:** targets/prices/time → non-overlapping or explicitly overlapping typed ranges with invalidation.
**Dependencies / interaction:** Max Pain/Cluster/Strategy; isolated from collectors.
**Failure/missing/persistence/concurrency:** invalid/inverted/empty ranges explicit; version persisted if consumed across restart.
**Scoring/output:** no score until approved; Telegram identifies range source/version.
**Required regression/runtime:** lower/upper exact boundaries, overlap, invalid/dynamic ranges and range-to-cluster relations; shadow validation.

### FC-CLUSTER — Advanced Cluster Engine

**Lifecycle:** `NOT_IMPLEMENTED` as the advanced capability; legacy cluster scoring remains active.
**Responsibility:** multi-timeframe density, spread, accumulated liquidity, overlaps and hierarchy.
**Input / Output:** validated targets/liquidity → deterministic cluster graph and quality.
**Dependencies / interaction:** Max Pain, Range, Magnet; must not duplicate legacy Cluster silently.
**Failure/missing/persistence/concurrency:** partial TFs reduce quality; deterministic ordering; version outputs if persisted.
**Scoring/output:** activation requires migration plan preventing double score; Telegram labels legacy/advanced.
**Required regression/runtime:** permutation invariance, overlaps/hierarchy, density/spread/cumulative vectors, shadow comparison.

### FC-STRATEGY — Strategy Engine

**Lifecycle:** `NOT_IMPLEMENTED`.
**Responsibility:** classify setups from lower-level deterministic contracts, not recollect or rescore inputs.
**Input / Output:** versioned immutable engine snapshot → setup, evidence, contradictions, invalidation.
**Dependencies / interaction:** all lower engines; one failure remains isolated/unavailable.
**Failure/missing/persistence/concurrency:** explicit minimum contract, generation ID, persisted decision/version if alerts depend on it.
**Scoring/output:** no duplicated scoring; contradictory evidence preserved; Telegram separates setup from component evidence.
**Required regression/runtime:** input schema, contradictions, classification/invalidation, duplicate prevention and staged shadow decisions.

### FC-TRADE — Trade Manager

**Lifecycle:** `NOT_IMPLEMENTED`.
**Responsibility:** entry/exit/targets/stop/invalidation lifecycle—not signal generation.
**Input / Output:** approved setup + risk contract → auditable state transitions/events.
**Dependencies / interaction:** Strategy, price and persistent DB; never block collectors.
**Failure/missing/persistence/concurrency:** durable idempotent state, single owner, restart recovery, consistent transaction/lock order.
**Scoring/output:** cannot mutate source scores; Telegram clearly distinguishes suggestion/state/execution.
**Required regression/runtime:** complete lifecycle, duplicates, out-of-order prices, restart/crash recovery, isolated paper/staging soak before activation.

### FC-OPTIONS — Market Context / Options

**Lifecycle:** `NOT_IMPLEMENTED`.
**Responsibility:** expiry/options/macro context isolated from core deterministic signals.
**Input / Output:** timestamped provider data → context with expiry/freshness/provenance.
**Dependencies / interaction:** Strategy/Confirmation only after approval.
**Failure/missing/persistence/concurrency:** missing providers and stale expiries unavailable; independent task cannot crash core.
**Scoring/output:** context is non-scoring by default; Telegram names provider/expiry.
**Required regression/runtime:** expiry boundaries, stale/missing, provider failure, isolation and live data reconciliation.

### FC-AI — GPT / AI Decision Layer Integration Boundary

**Lifecycle:** `NOT_IMPLEMENTED`; AI Decision Layer belongs to a separate project.
**Responsibility:** Core Bot exposes/consumes only a versioned interface.
**Input / Output:** explicit immutable deterministic snapshot → typed AI interpretation with version, timestamp and confidence—not overwritten source fields.
**Dependencies / interaction:** timeout/circuit breaker and schema validator; AI cannot own collectors/scoring state.
**Failure/missing/persistence/concurrency:** timeout, malformed/unavailable response produce surfaced fallback; AI failure cannot crash Core Bot; deterministic and AI records stored separately if persistence is approved.
**Scoring/output:** AI cannot silently overwrite deterministic data; Telegram labels interpretation clearly.
**Required regression/runtime:** contract-version compatibility, timeout/malformed/unavailable/fallback, injection-sized payloads, concurrency/load, staged failure isolation.

---

## Generic New Feature Validation Contract

העתק תבנית זו לכל engine/function/collector/command/provider/scoring component חדש:

1. **Intended responsibility:** גבול אחריות ומה אינו באחריותו.
2. **Inputs:** schema, source, units, timestamps/version.
3. **Outputs:** schema, side effects, provenance.
4. **Valid values:** ranges/enums/examples.
5. **Invalid values:** rejection/coercion policy.
6. **Missing data:** explicit unavailable/partial behavior.
7. **Stale data:** clock source, threshold and exact boundary.
8. **Threshold boundaries:** below/equal/above לכל comparator.
9. **Error handling:** isolation, logging and surfaced state.
10. **Provider/API failure:** timeout/DNS/403/404/429/5xx/JSON/partial/invalid numeric.
11. **Concurrency:** owner, single-flight, lock order, cancellation/restart.
12. **Persistence:** transaction/idempotency/retention/schema version.
13. **Restart behavior:** recovered/lost state and duplicate consequences.
14. **DB impact:** DDL only startup/migration; DML bounds and backend parity.
15. **Scheduling impact:** existing equivalent scan, cadence/configuration and overlap.
16. **Existing-engine impact:** Max Pain, Magnet, OI, CVD, Confirmation, Watch, alerts.
17. **Scoring/double counting:** LONG/SHORT separation, correlation and max/normalization.
18. **Telegram/display:** safe escaping, source/freshness and semantic labels.
19. **Backward compatibility:** commands/data contracts/defaults.
20. **Automated regression:** deterministic unit/property/integration tests.
21. **Runtime validation:** staging plan, evidence and observation duration.
22. **Deployment implications:** Render/tasks/ports/env/DB/rollback.

Checklist metadata חייב לכלול owner, lifecycle status, commit, test IDs, runtime evidence and activation approval. אין מעבר ל־`VERIFIED` לפני השלמת regression + required runtime checks.

---

## Recommended safe execution order

1. Freeze current formulas as pure unit/golden tests without changing behavior.
2. Add static startup/scheduler/DDL call-graph checks.
3. Add temporary SQLite tests for schema idempotency, UPSERT/dedup and state reset.
4. Add mocked provider and Telegram/webhook contract tests.
5. Add fake-clock OI/CVD candle, freshness, family and Watch-generation tests.
6. Add concurrency/cancellation/single-flight tests with fake collectors.
7. Use isolated PostgreSQL staging for advisory-lock/deadlock/transaction parity.
8. Deploy to non-production Render and Telegram chat; validate startup and commands.
9. Observe collectors/Watch/data freshness for 24–48 hours.
10. Validate natural Confirmation tiers; only then consider production activation/changes under separate approval.

## Tests that Codex may safely execute without production credentials

- `python -m compileall -q .` (import-independent syntax compilation; exclude generated cache from commits).
- `python -m pytest` only after a test suite exists and only with network disabled/mocked.
- Pure function tests for scoring, Proximity, Consensus, families, regimes, CVD math and Magnet.
- Parser tests using local HTML/JSON fixtures.
- Webhook normalization/auth tests using in-memory request doubles.
- SQLite tests using `tmp_path` and a temporary `DB_PATH` established inside the test process.
- AST/static checks for environment names, scheduler ownership and collector→DDL call paths.
- Fake-clock/fake-bot async tests that never call Telegram or providers.

## Tests that must never be run automatically against production

- Any command/webhook that sends Telegram messages or registers/deletes the production webhook.
- `/collect`, `/watch_*`, `/oi_refresh`, `/oi_backfill`, `/flow_backfill` or any production collector trigger.
- Schema/migration/DDL, cleanup, retention deletion, write/dedup or lock-contention test on production DB.
- Deadlock, cancellation, crash, restart, redeploy, failover or load tests against production.
- Live provider stress/rate-limit/fallback tests using production credentials.
- Synthetic TradingView/Confirmation/Strong/Combined events against public production endpoints.
- Browser concurrency/leak tests against the production service.
- Any destructive API, fabricated market data insertion or trade/execution action.

---

## 18. Audit trail והנחות שסתרה הארכיטקטורה הנוכחית

- אין test suite tracked במאגר המקור בענף הנוכחי; אין לייחס לו historical tests. זהו פער development assurance, בעוד שהחרגת tests מ־production ZIP נשארת מדיניות packaging תקינה.
- Spot CVD הוא secondary non-voting context, לא confirmation engine ולא veto פעיל.
- Watch אינו עולה אוטומטית; OI/CVD/history collectors כן עולים אוטומטית רק עם `COINGLASS_API_KEY`.
- Magnet Watch משתף coordinator/snapshot ואינו collector עצמאי.
- scoring Consensus תומך בעד שישה timeframes לאחר trigger exclusion, אך `/consensus` הוא command legacy נפרד של עד שבעה; wording `/7` שם אינו בהכרח display bug.
- Combined Confirmation סופר occurrences/keys, ולא קיימת הוכחה שכל זוג הוא independent signal מבחינה כלכלית.
- צורת DOM המאושרת היא atomic seven timeframes עם לפחות 30 rows לכל timeframe; `50 × 7` הוא expectation תפעולי אפשרי, לא invariant מקודד.
- Flow poll default בפועל הוא 5 דקות עבור candle של 30 דקות; README שומר גם ניסוח של שלב קודם בן 30 דקות.
- initialization אינו מוגבל ל־startup: `init_db`/DDL-capable paths נקראים גם מתוך recurring work. זה ממצא, לא תיקון.
- אין `/watch_now` command נפרד; activation מפעיל cycle מיידי.

מסמך זה אינו מאשר production readiness. הוא קובע כיצד להוכיח אותה באופן הדרגתי, repeatable ובטוח.
