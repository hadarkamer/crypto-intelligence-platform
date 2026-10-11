# Causal archive research with an explicit delayed entry

The source intake in `telegram-archive-source-staging.md` remains immutable.
The next phase reconstructs each eligible message independently and measures a
new, explicitly delayed Binance Spot entry. It never reuses the old importer’s
scan-wide maximum scores or its earlier entry timestamp.

## Frozen contracts

| Contract | Version / interpretation |
| --- | --- |
| Source scope | `ARCHIVE_ONLY`; never a native `LIVE` event |
| Source extraction | `telegram-per-message-causal-features-v1` |
| Direction | `telegram-post-20260815-display-direction-v1` |
| Time | `user-israel-wall-clock-dated-cvd-corroboration-v1` |
| Entry | `archive-next-full-minute-binance-spot-open-v1` |
| Ordered outcome | `ordered-first-touch-v7` |
| Common-window metrics | `common-window-spot-1m-v1` |
| BTC parent | `btc-parent-close-reversal-200bps-v1` |

The user-specified Israel message wall clock is normalized with its version and
the dataset’s dated UTC/age corroboration. Original HTML titles and their +02:00
interpretation remain available. An individual contradiction, ambiguous source
identity, source revision or missing message symbol/direction blocks that
message. Dataset-level corroboration does not pretend that every message has
its own embedded UTC observation.

Entry is the OPEN of the next full Binance Spot minute strictly after the
normalized message timestamp, even if the message happened on a minute boundary.
Only that entry rule is measured. A printed Telegram quote keeps provenance
`TELEGRAM_PRINTED_QUOTE_EXCHANGE_UNSPECIFIED`; it is never relabelled Binance.
Features come solely from the original message, before delayed entry. A later
confirmation does not authorize an earlier entry.

For MaxPain/Combined display families, the displayed hurt side is inverted once
to obtain the normal research direction. The separate INVERSE trial flips that
research direction once more and remeasures the actual price path. Both trials
retain the same original feature predicates; aligned scores are not flipped to
make the inverse condition pass.

Exact family totals, group scores, selected/opposite averages, Gap component
points and consensus numerator/denominator remain distinct fields. The
all-timeframe average alias requires the literal source label. Liquidity may
enter a condition only when its printed amounts reproduce the share and the
selected price target agrees with the research-side mapping. Missing or
unverified values do not form a no-signal group.

## Calculate and resume

```bash
python research_telegram_archive_backfill.py \
  --stage-dir /path/to/reviewed-prepared-intake \
  --spot-cache /path/to/reviewed-official-spot-cache.csv.gz \
  --expected-cache-sha256 REVIEWED_CACHE_SHA256 \
  --output-dir /path/to/archive-research \
  --event-limit 15000 \
  --observed-at 2026-09-07T07:00:00Z
```

The input stage digest and official cache digest are validated. Optional
`--allow-network --max-fetches 32` performs bounded fetches using the existing
official Binance Spot reader and records downloaded candles with provenance in
`official_spot_extensions.jsonl`. HYPE has no supported Spot route in this
contract; Futures or another exchange cannot fill it. Gaps inside a price path
produce missing-data states, not OPEN or failure.

Every reconstructed Spot event receives independent labels at 25, 50, 75, 100,
125, 150, 175 and 200 bps, each at 60, 240, 720 and 1,440 minutes, for both
NORMAL and INVERSE: 64 ordered labels and eight common-window metric records.
AMBIGUOUS and closed-window no-touch are separate nondecisive states. Every
event commits atomically, so interrupted work resumes without appending duplicate
outcome IDs. Missing entry/path records remain explicit and retryable.

The run key binds source, feature, time, direction, entry, outcome, parent and
initial cache contracts. Observation cutoffs and appended verified price paths
can advance an incomplete result under the same frozen method. Native LIVE
tables are never read as replacement archive source observations or written by
this backfill.

## Descriptive candidate cells

```bash
python research_telegram_archive_summary.py \
  --database /path/to/archive-research/archive_reconstructed_research.sqlite \
  --run-key VERIFIED_RUN_KEY \
  --observed-at 2026-09-07T07:00:00Z \
  --output-dir /path/to/archive-research/summary
```

The catalog’s original conditions select the earliest matching cohort in each
verified BTC parent before any labels are loaded. Simultaneous earliest
messages are retained together, including missing outcomes. Different coins,
formula families, thresholds, horizons and orientations are not independent
market waves. The initial unverified BTC boundary is excluded, and a parent
that started before a period cutoff cannot cross that cutoff.

The summary emits the two overlapping calendar periods
`ALL_COMPATIBLE_SINCE_20260816` and `SINCE_20260904`, with a separate rolling
14-day FRESH count and full-window metrics. Each cell has a scope key including
its exact period, coin, side, orientation, threshold and horizon. The shared
ordered evaluator checks actual reference-price equality with the delayed Spot
entry. Full-window asymmetry includes every selected wave, with conservative
minimum MFE and maximum MAE for simultaneous earliest members. Missing member
metrics keep the aggregate unavailable. First-touch excursions are never used
as full-window metrics.

All outputs are DISCOVERY with `research_ready=false`, including cells meeting
three/five-wave counts. No acceptance weights or success thresholds are
invented. Both successful and failed trials are recorded; required-field
coverage distinguishes a real no-match test from a blocked candidate. Overlap
groups and number of attempts remain visible. Periods are overlapping views,
not independent discovery/validation datasets. An independent prospective
archive-equivalent entry cohort is still required for future validation.

## Isolated persistence and audit

Migration 032 creates isolated PostgreSQL tables corresponding to the portable
SQLite reconstruction. The separately reviewed runtime importer checks an
explicit immutable file digest, validates the run contract, and transfers
bounded batches into archive-only tables. Installing the migration does not
mean any archive rows have reached production. Never hash or package SQLite
while a backfill writer is active.

Run the focused causal/price-path checks with:

```bash
python research_telegram_archive_reconstruction_selftest.py
```

The final artifact should include the SQLite database, reviewed stage manifest,
backfill report, candle-extension provenance, candidate CSV/JSONL, validation
report and exact file digests. Preserve the earlier raw source intake ZIP
separately. The supplied export’s August 30–September 3 coverage gap remains a
gap after reconstruction.
