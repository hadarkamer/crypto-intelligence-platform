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
market context, delivered-alert history, one-alert context, canonical spot outcome
paths, bounded formula-candidate aggregates, a no-lookahead raw/model research
matrix and capability status.

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

## Outcome v3 — canonical price path

The background outcome worker enriches delivered alerts after 1h, 4h, 12h and
24h. Version 3 fetches closed one-minute `USDT` candles from Binance Spot and
uses the explicit Hyperliquid HYPE/USDT spot (`@107`) route for HYPE. It
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
Unsupported canonical pairs fail open and are never silently replaced by a
futures or third-party path.

The AI now has a `research_formula_groups` discovery tool. It compares verified
paths by exact signal combination, alert type, symbol or score band and exposes
sample size, baseline, MFE/MAE distributions, speed, target progress and rarity.
This is a Candidate discovery surface; it does not activate formulas.

For event-level inspection, `get_alert_price_path` fetches one completed
canonical spot path on demand, computes metrics from the full one-minute series
and returns only a bounded candle sample to GPT. This gives the engine direct
path access without copying every candle into every Research Event.

`research_feature_matrix` reads existing raw Price/OI and Futures/Spot CVD
histories in bounded batches. For each delivered alert it selects only rows at
or before the decision time, derives fixed-window changes, time/repetition and
market-breadth features, and places them beside compact captured model features.
Verified later canonical spot path metrics appear only under `outcome_label`.
This enables direct comparison of pure data and the bot's current scoring logic
without copying the underlying time series into the Research Event archive.

## Formula objective and remaining stages

The primary analytical objective is to find reproducible conditions associated
with high directional probability, low adverse movement and fast favorable
progress. Existing bot scores and raw source measurements are both valid feature
families; neither is assumed to be optimal in advance.

After the price path and core feature matrix, the implementation order is:

1. extend the matrix with matched near-miss/control samples, event-order and
   BTC/range-context features where source coverage allows;
2. generate candidate combinations without look-ahead bias;
3. rank candidates by baseline improvement, MFE, MAE, speed, target progress and
   sample coverage rather than hit rate alone;
4. run chronological holdout/out-of-sample validation and retain failures;
5. register approved Candidate formulas with versions and monitor them in shadow;
6. validate the frozen formula on genuinely future Shadow outcomes under the
   owner policy, then expose it as a live alert condition for opted-in chats.

## Explicit activation gates

The schema is applied once from `migrations/001_research_archive_v1.sql` outside
the Watch loop. Runtime persistence then requires:

- `RESEARCH_USE_PRIMARY_DATABASE=1` (or a dedicated `RESEARCH_DATABASE_URL`);
- `RESEARCH_PERSISTENCE_ENABLED=1`;
- `RESEARCH_OUTCOME_ENRICHMENT_ENABLED=1`.

The OpenAI command surface additionally requires the existing `OPENAI_API_KEY`.
All tool calls remain read-only regardless of the passive archive writers.

## Staging deployment

`ai_candidate_main.py` is a dedicated entrypoint for the existing Render test
service. It registers the same production analytical tools against the staging
Telegram bot, exposes `/health`, and starts no Watch, collectors, Research
writers or outcome worker. This allows the integration branch to be verified
without changing `main` or the production Telegram webhook.
