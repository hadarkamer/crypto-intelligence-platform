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
market context, delivered-alert history, one-alert context and capability status.

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

## Outcome v1

The background outcome worker enriches delivered alerts after 1h, 4h, 12h and
24h. Version 1 uses the nearest existing 30-minute close to calculate raw and
direction-adjusted fixed-horizon returns.

It deliberately leaves MFE, MAE, exact target timing and path-quality fields
empty. Those require a future verified 1m/5m price archive and must not be
inferred from 30-minute closes.

## Explicit activation gates

The schema is applied once from `migrations/001_research_archive_v1.sql` outside
the Watch loop. Runtime persistence then requires:

- `RESEARCH_USE_PRIMARY_DATABASE=1` (or a dedicated `RESEARCH_DATABASE_URL`);
- `RESEARCH_PERSISTENCE_ENABLED=1`;
- `RESEARCH_OUTCOME_ENRICHMENT_ENABLED=1`.

The OpenAI command surface additionally requires the existing `OPENAI_API_KEY`.
All tool calls remain read-only regardless of the passive archive writers.
