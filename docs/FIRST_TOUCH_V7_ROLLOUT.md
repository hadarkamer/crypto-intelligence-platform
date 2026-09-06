# Ordered First Touch v7 rollout

The workbook's requested label is a race between equal favorable and adverse
price barriers. The old `first-touch-no-dwell-v6` label only asked whether the
favorable target was eventually reached. Two opposite v6 HITs can therefore be
valid under that old definition, but cannot be treated as two ordered wins.
The old Sheets adapter also inferred adverse touches from v6 MISS without the
required price/time evidence. Retained v6 data is audit-only.

## New evidence

- Method: `ordered-first-touch-v7`.
- Thresholds: 25, 50, 75, 100, 125, 150, 175, 200 basis points.
- Windows: 60, 240, 720, 1440 minutes, measured separately.
- Unique identity: immutable event, direction, window, threshold, method.
- A favorable first touch is SUCCESS; an adverse first touch is FAILURE.
- Both barriers in one minute are UNRESOLVED/AMBIGUOUS. A closed window
  without either touch is UNRESOLVED. Missing candles are DATA_MISSING.
- Decision time is the closing timestamp of the decisive one-minute candle,
  not the alert start or an invented tick timestamp. Touch prices are barrier
  prices; they are not claims about execution or slippage.
- The initial partial minute is excluded and explicitly disclosed. These
  labels describe the fully observed post-gap path, not the unobserved gap.
- Raw alerts and retained v6 outcomes are not rewritten. v7 is recomputed from
  canonical Spot candles into additive tables. Historical HYPE coverage remains
  limited by the official source and is never replaced silently with Futures.

Database outcomes and their delivery payload are committed atomically. A
durable, leased outbox delivers idempotent Sheets upserts after commit, with
bounded retries. DATA_MISSING labels can become valid after path repair even
when the decisive candle predates the last previously observed candle.

## Deployment

Production is `srv-d94ek17lk1mc73b4tb90`, branch `main`. The separately diverged
Candidate branch has no research workers and is not part of this rollout.

For the additive migration at the next production startup, merge these keys
into the existing Render environment:

```
FORMULA_SCHEMA_APPLY=1
FORMULA_SCHEMA_APPLY_ONLY=020_ordered_first_touch_v7.sql
```

Only exact allowlisted migration filenames are accepted. Targeted application
uses a 15-second statement timeout and 1-second lock timeout. Startup verifies
the schema before enabling the research workers. Turn `FORMULA_SCHEMA_APPLY`
back to `0` after the successful migration; keep the selective filename for
auditability. No database credentials need to leave Render.

The worker prioritizes recent delivered alerts and fills the configured
lookback (default 14 days) in bounded passes. Check database status counts,
all 8 thresholds/all 4 windows, outbox confirmations and actual workbook v7
rows. Opposite SUCCESS/FAILURE consistency is meaningful only for identical
symbol, reference price, start, window, threshold and method. Two OPEN or
UNRESOLVED labels are not a symmetry defect.

## Separate unfinished research integration

The native formula pipeline still consumes the old label contract and is
quarantined at its worker boundary. Collection and ordered outcomes continue.
Changing a method-version constant would not adapt its SQL, acceptance,
relevance or frozen evidence contracts correctly.

LIVE Episodes and a global BTC parent movement generator are not supplied by
the v7 label fix. The old fixed-24h, formula-local, price-reset episode policy
does not implement the user's newer BTC-wave requirement. Until independent
parent movements and candidate-specific LIVE episodes are implemented, a
workbook v7 match must not be presented as five independent observations (or
three recent FRESH observations).
