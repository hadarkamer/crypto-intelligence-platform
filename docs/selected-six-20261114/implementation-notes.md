# Selected six: implementation and release boundaries

The original JSON is immutable. `operational-clarification.md` records the later CT009 six-symbol restriction separately; historical research results were not rewritten.

## Fixed formula adapters

| Formula / rule ID | Product inputs and predicate | Frozen prices and lifecycle |
| --- | --- | --- |
| HYPE ROW71205 / `HYPE_ROW71205_SHORT` | Existing signal and source worker, unchanged | Existing execution, risk and exits, unchanged |
| XRP R2732 / `R2732_XRP_SHORT_NY_WEEKDAYS_LOCK` | Existing signal and source worker, unchanged | Existing execution and profit lock, unchanged |
| SOL SFF1361 / `SFF1361_SOL_RECENT` | SOL only. Frozen positioning and Futures `now` (30m) aligned SHORT scores each >=40; original regime 1h price change in [-0.5%, +0.5%]; complete 24h price history | P is the actual next full minute OPEN after data availability. Planned entry P, stop 1.01P, take 0.98P. No invented holding limit or new pending deadline |
| OCONF0034_13 / `OCONF0034_13` | BTC, ETH, SOL, XRP, DOGE, HYPE. Confirmed MP SHORT, two distinct confirmations including current within24h, compatible target within0.4%; fast aligned score=(0.35 now+0.30 short)/0.65; OI>=40, Futures>=40, Spot>=0 | P is an actually observed live Hyperliquid trade reference at opportunity creation, explicitly labeled LIVE_REFERENCE. E=P+0.2(P-T); stop1.012E; take T. Unfilled remainder expires24h after reference or cancels at original target |
| CT009 / `CT009_FROZEN_3D` | Exactly BTC, ETH, SOL, XRP, DOGE, HYPE. Native delivered ordinary MAX_PAIN_ALERT SHORT, timeframe48h/3d; matching previous24h target population within0.4%; prior mean score>=72.8, current excluded | P is actual next full minute OPEN after source availability. E=P+0.1814560294151306(P-T); stop1.0040077843998312E; take E+0.9365681871771814(T-E). Pending24h/original-target cancellation |
| XRP Liquidity inverse / `XRP_LIQUIDITY_RAW_INVERSE` | XRP only. Actual qualified Combined LONG source, supporting timeframe upper/(upper+lower)>=0.60. The saved source direction is retained while execution is SHORT | P is actual next full minute OPEN; source upper target U, inverse target T=2P-U. E=P+0.2342784544453025(P-T); stop1.0105286625253104E; take E+0.8671878740191461(T-E). Pending24h/original-target cancellation |

The code contains four explicit functions, with no expression evaluation or general formula engine. Product family scores come from the already frozen Watch score bundle; they are not fetched or reweighted independently per formula. CT009 reads actual delivered research events in one bounded shared query. The original research's eight-symbol CT population remains readable; only the six operational symbols can pass new-source approval and manager entry admission.

## Data and prices

The adapter validates the actual Watch decision bundle against its score bundle and cycle. The raw SOL regime snapshot must match its captured hash. Missing, incomplete, stale and future inputs fail closed. Source observation, data availability, reference observation and approval times remain separate. A polling time cannot rewrite the original source availability or revive a pre-activation event.

Concrete source fields are `models.positioning/futures_flow/spot_flow.time_families.now/short.net`; aligned SHORT score is -100 times that signed net. SOL's numeric return is `regime.windows['1h'].price_change_pct/100`, dated by the original `regime.price_fetched_at` sample, not a fabricated window timestamp. Its freshness also respects the existing price/OI deadline. MP confirmations and Combined qualification come from the actual decision capture's `prepared_items` and `combined_groups`. XRP's supporting liquidity uses the saved upper/short and lower/long liquidation amounts. CT009 requires the native `TELEGRAM_ALERT` capture stage, `MAX_PAIN_ALERT`, `ALERT`, and `DELIVERED`; availability is the later of actual delivery and database creation. CT/XRP target episodes include symbol, direction and timeframe; different timeframes are not merged by the source. The manager still enforces shared symbol/account exposure.

Hyperliquid candles use the existing shared trade-price adapter/cache/archive. A current candle is never treated as completed historical OHLC: a next-minute OPEN is used only once observed; OCONF's current trade reference is labeled with response time and candle window. No planned price is a claimed exchange fill. SFF's complete24h history may require bounded continuation across existing forwarder passes.

An already observed target touch can veto a new pending plan or withdraw its unfilled remainder. A forming candle low is evidence only when its opening time is at/after the reference; OCONF's earlier intraminute low does not prove a post-reference touch. A currently observed last trade at/beyond the target is a live veto, never a claimed fill. Cancellation of a partially filled entry preserves manager protection for its actual filled quantity.

Prices are calculated once and frozen as canonical decimal strings. The source saves the decision and execution outbox in PostgreSQL, and the notification reads those same strings. The manager validates the same geometry and applies its existing exchange precision, account, capital, ownership and exposure checks. Research sizing assumptions never replace live allocation.

## Delivery, execution and retirement

`main.run_watch_cycle` captures inputs before Telegram delivery. The existing forwarder drains already committed messages before bounded optional source work; no new scheduler or trading process is introduced. Provider and Telegram I/O occur outside the execution sender/source transaction locks. Telegram failure does not modify the exchange request journal.

The existing HMAC HTTP intake records the approved decision. Only the existing manager sends exchange orders and protection. The 90-second initial admission lease is different from the three formulas'24-hour accepted pending-entry limit and from a filled position's holding time. Source cancellation withdraws an unfilled remainder; observed fills retain management and protection.

SFF has no later source cancellation rule. An exact successful manager intake acknowledgement therefore compacts its existing outbox row to a permanent identity receipt (`payload_json=NULL`, `canceled=false`, matching acknowledged sequence). The next source transaction retires only the observation as INTAKE_COMPLETE. This neither cancels the manager's accepted order nor claims a fill/closure. Replays cannot recreate an outbox message or notification. Unresolved Telegram delivery is retained independently.

SOL G65 and the old DOGE partial formula are retired by rule ID. They cannot create new opportunities or notifications after restart or queue drain. Their existing/uncertain positions, exits, accounting and history remain supported. Ordinary market alerts, SOL's new formulas and CT009 DOGE are independent of this retirement.

## Release order and limits

Publish only isolated commits for review. The repository requires approval of the specific tested release. Deploy the manager's six-formula support first, verify its running admission roster and existing execution ownership, then deploy the exact source commit containing the six-formula producers and cleanup. Read both live configurations after release. Never deploy an arbitrary branch head, release a manual HYPE lock, increase quotas or manufacture a production test signal.

The referenced external research ZIPs could not be retrieved in this session. This implementation is verified against the supplied JSON and accessible product code; it does not claim to reproduce their historical research statistics. Isolated acceptance and short controlled load measurements do not establish multi-day memory stability or prove a new formula event has occurred in live trading.

Cold SFF history needs at least two bounded source visits plus a later reference observation. With all six symbols pending and a slow provider this can exceed the existing ten-minute source window; the source rejects the expired candidate. It does not backdate availability, extend the source window, raise request quotas or bypass the manager. The configured provider timeout is not a proven total wall-clock deadline. A source-adapter invocation can be served by cache and must not be reported as an exchange HTTP request.
