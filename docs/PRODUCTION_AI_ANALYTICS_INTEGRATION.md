# Production AI Analytics Integration

## Scope frozen for this release

The existing Telegram bot receives the read-only GPT analysis layer and the
Research Archive. The release intentionally excludes all lab collection tools:

- no web search;
- no CoinGlass Vision;
- no CryptoJungle, SoSoValue, YouTube or X collector;
- no natural-language scheduler;
- no tool that changes Watch, scores, thresholds or strategy logic.

The production AI tools are limited to current OI/CVD/market state, historical
market context, delivered-alert history, one-alert context, Binance Spot outcome
paths, bounded formula-candidate aggregates and capability status.

## Historical truth boundary

The existing `alert_history` table contains no historical rows and its legacy
writer was not connected to the delivery paths. Old Telegram alerts therefore
cannot be recovered from PostgreSQL. Existing Price/OI, Futures CVD, Spot CVD,
Max Pain and technical-signal data can be researched as historical market
evidence, but must never be labelled as delivered alerts.

An optional later Telegram export importer can add old delivered alerts with an
explicit provenance label. It is outside this release because no export is
currently available.

## Future alert capture

Every successfully delivered alert card is normalized into an immutable
`research_events` row with:

- exact decision time and separate delivery timestamps;
- symbol, direction, source side, timeframe and alert type;
- score, target/current price and compact score/confirmation components;
- strategy version, code version and runtime session;
- setup key plus occurrence-specific fingerprint.

Normal, special, Combined and Magnet Watch paths enqueue Research Events without
waiting for PostgreSQL. Manual `/alert` and `/alerts*` scans are stored only as
`DECISION_SAMPLE` rows and are excluded from delivered-alert performance. Database
failure cannot block Telegram or change trading logic. Shadow Replay and self-tests
use `persist=False` and can never contaminate the production archive.

## Outcome v2 — canonical price path

The background outcome worker enriches delivered alerts after 1h, 4h, 12h and
24h. Version 2 fetches closed one-minute `USDT` candles from Binance Spot and
records:

- fixed-horizon raw and direction-adjusted return;
- MFE and MAE for both LONG and SHORT paths;
- time to first favorable progress and time to MFE (with one-minute precision);
- closest approach, progress ratio and hit timing when the alert has a target;
- source pair, path resolution, sample count and quality status.

The partial candle containing the alert timestamp is excluded because its full
OHLC could contain movement from before the alert. The immutable decision price
is used as the reference, and only subsequently closed candles enter MFE/MAE.
Existing 30-minute v1 rows are upgraded in place when the worker next sees them.
Unsupported Binance Spot pairs fail open and are never silently replaced by a
futures or third-party path.

The AI now has a `research_formula_groups` discovery tool. It compares verified
paths by exact signal combination, alert type, symbol or score band and exposes
sample size, baseline, MFE/MAE distributions, speed, target progress and rarity.
This is a Candidate discovery surface; it does not activate formulas.

For event-level inspection, `get_alert_price_path` fetches one completed
Binance Spot path on demand, computes metrics from the full one-minute series
and returns only a bounded candle sample to GPT. This gives the engine direct
path access without copying every candle into every Research Event.

## Formula objective and remaining stages

The primary analytical objective is to find reproducible conditions associated
with high directional probability, low adverse movement and fast favorable
progress. Existing bot scores and raw source measurements are both valid feature
families; neither is assumed to be optimal in advance.

After the price path, the implementation order is:

1. normalize raw measurements, model scores, families, alert components, time,
   sequence and repetition features into a versioned Research matrix;
2. generate candidate combinations without look-ahead bias;
3. rank candidates by baseline improvement, MFE, MAE, speed, target progress and
   sample coverage rather than hit rate alone;
4. run chronological holdout/out-of-sample validation and retain failures;
5. register approved Candidate formulas with versions and monitor them in shadow;
6. only after explicit approval, expose a validated formula as a live alert
   condition.

## Explicit activation gates

The schema is applied once from `migrations/001_research_archive_v1.sql` outside
the Watch loop. Runtime persistence then requires:

- `RESEARCH_USE_PRIMARY_DATABASE=1` (or a dedicated `RESEARCH_DATABASE_URL`);
- `RESEARCH_PERSISTENCE_ENABLED=1`;
- `RESEARCH_OUTCOME_ENRICHMENT_ENABLED=1`.

The OpenAI command surface additionally requires the existing `OPENAI_API_KEY`.
All tool calls remain read-only regardless of the passive archive writers.
