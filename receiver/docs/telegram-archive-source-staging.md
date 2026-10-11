# Telegram archive source intake

This intake preserves Telegram message evidence separately from LIVE records. It
does not create formula evidence, market waves, First Touch results or a trade
signal. Migration `028_telegram_archive_source_staging.sql` creates only isolated
archive source, batch and membership tables.

The supplied HTML export has 14,354 unique message revisions dated from August
16 onward, of which 11,466 are classified as signal messages. The September 4–5
subset has 318 source messages. Exact message-id plus text links identify the
216 previously imported Sheet event records and 119 existing snapshots. Those
are provenance links, not new LIVE events. Native event reconciliation beyond
that prior Sheet import is still required before any statistical union.

The two overlapping calendar scopes use Israel midnight:

| Scope | Inclusive start | Source staging rows |
| --- | --- | ---: |
| `ALL_COMPATIBLE_SINCE_20260816` | 2026-08-16 00:00 +03:00 | 14,354 |
| `SINCE_20260904` | 2026-09-04 00:00 +03:00 | 318 |

These identifiers match the live period definitions; `source_scope=ARCHIVE_ONLY`
keeps this intake separate. “Compatible” names the intended research period; it
does not certify any staged row. All staged rows remain `candidate_eligible=false`
and `training_eligible=false`. FRESH is a separate rolling 14-day track, and the
two overlapping periods are not discovery-versus-validation partitions.

No exported messages cover August 30 through September 3. This is a coverage gap,
not evidence that signals were absent. Records before August 16 are excluded.

## Time evidence and identity

The HTML title says UTC+02:00 while the user's specified local wall clock is
Israel time, UTC+03:00 during these dates. Both interpretations and the raw title
are preserved. In 2,183 messages across 16 dates, explicit dated CVD UTC times
plus reported age support the Israel wall-clock interpretation: residual delay
is 0.03–13.62 minutes, versus 60.03–73.62 minutes using the header offset.
Capture and delivery can differ, so this check does not certify an exact event
entry timestamp. Canonical time remains unset pending versioned reconstruction.

Identity is the stable source chat key plus Telegram message id. A changed
content/time revision is retained and quarantined; repeated identical exports
deduplicate. Scan counters and nearby timestamps are never identity keys.
The old September importer combined maximum scores across a scan with an earlier
entry time, so its feature aggregation is not reused as causal evidence.

## Prepare and validate

```bash
python research_telegram_html_archive.py /path/to/export \
  --chat-key coinglass-tracker-primary \
  --time-policy israel-wall-clock-v1 \
  --existing-import /path/to/sheet_import_payload.json \
  --output-dir /path/to/prepared-intake

python research_telegram_archive_stage_store.py \
  --stage-input-dir /path/to/prepared-intake
```

Review both digests printed by the validation command. The raw archive digest
binds immutable message identities and revisions. The prepared-stage digest
also binds proposed times, source-file provenance, prior-import links, period
membership and the complete manifest. Apply rejects either mismatched digest
before opening a database. Missing or changed annotations cannot be silently
accepted using the earlier prepared-stage digest.

## Persist isolated sources

Portable SQLite intake needs no production credentials:

```bash
python research_telegram_archive_stage_store.py \
  --stage-input-dir /path/to/prepared-intake \
  --apply-staging \
  --expected-archive-digest REVIEWED_RAW_DIGEST \
  --expected-stage-digest REVIEWED_STAGE_DIGEST \
  --sqlite-path /path/to/archive_sources.sqlite
```

For PostgreSQL, install migration 028 and omit `--sqlite-path`. The existing
research database configuration must be explicit, and
`RESEARCH_ARCHIVE_IMPORT_APPLY=1` must be set in that execution environment. Do
not print or export database credentials. Apply uses bounded batches and resumes
after interruptions. A reimport of the same intake inserts zero new records;
both stores verify the full persisted source content and annotations before
marking an intake complete.

SQLite tables are `archive_source_messages`, `archive_intake_batches` and
`archive_intake_members`. The PostgreSQL equivalents have the `research_`
prefix. Filter memberships by the exact `intake_batch_key`, as changed parser,
normalization, prior-import evidence or manifest versions create a new intake.

## Statistical integration remains separate

The subsequent isolated reconstruction is now implemented in
`telegram-archive-delayed-entry-research.md`. It measures a separately versioned
next-minute Spot-open entry; it does not change this source intake’s eligibility
flags or make archive and native LIVE entries interchangeable.

Reconstruct only fields that existed when each message's conditions became
complete; validate family total scores and direction-version mapping; obtain
eligible price paths and ordered First Touch v7 results per threshold/horizon;
reconcile duplicate native events and verified BTC parent movements. Only then
can compatible ARCHIVE and LIVE evidence be combined in period-specific formula
results. This source intake does not perform those remaining steps.

Validation commands:

```bash
python research_telegram_html_archive_selftest.py
python research_telegram_archive_stage_store_selftest.py
```

The parser tests cover both cutoffs, raw offsets, duplicate/revised messages,
existing-import links and embedded UTC-age evidence. Persistence tests cover
tampered annotations, changed reviews, prohibited promotions, actual SQLite
idempotency/resume, stored-source integrity and explicit PostgreSQL opt-in.
