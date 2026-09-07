# Telegram Archive Intake and Research Periods

This intake preserves Telegram HTML source messages in isolated archive tables.
It does not create LIVE events, snapshots, First Touch outcomes, market waves or
qualified formulas. The source records can be imported while feature and label
reconstruction is incomplete.

## Period Contract

Version: `research-periods-israel-v1`.

| Period ID | Inclusive lower boundary | Sources allowed after compatibility checks |
| --- | --- | --- |
| `ALL_SINCE_20260816` | 2026-08-16 00:00 Asia/Jerusalem | Archive and LIVE |
| `SINCE_20260904` | 2026-09-04 00:00 Asia/Jerusalem | Archive and LIVE |

The period windows overlap; they are separate reports, not independent validation
sets. Archive/LIVE is an additional source filter, independent of the date period.
FRESH remains a separate rolling 14-day evidence condition. Version compatibility,
direction, threshold, horizon, discovery/future validation and independent market
waves must remain separate when formula aggregation is connected later.

The intake writes period membership for source coverage. Formula results for both
periods are **not implemented by this source intake**. Only reconstructed,
compatible evidence may eventually enter those results, with each source event
counted once and wave independence applied after the join.

## Source Identity and Existing Import

`source_identity_key` hashes a stable operator-assigned chat key plus the original
Telegram message ID. It is independent of export filename, row number, visible
scan counter, normalization policy and content edits. Every raw title/text
revision has a separate `source_revision_sha256`.

Identical reexports deduplicate. Conflicting revisions are preserved for review,
including conflicts that first appear in a later database intake. The view
`research_archive_source_identity_conflicts` exposes cross-intake conflicts.

The existing Sep 4-5 import is supplied as its original `sheet_import_payload.json`.
Links require the exact source ID and preserved message text, in the same chat.
These links identify the already imported event/snapshot; they neither recreate
the snapshot nor promote its old outcomes to v7. No scan-counter or nearby-time
join is permitted. Later LIVE joins must use an explicitly verified mapping to
the same source chat and Telegram message IDs; the operator alias alone is not a
proof that an arbitrary LIVE chat is the same source.

Continuation messages stay separate source messages. A future reconstruction
must prove their attachment and use the completion timestamp for conditions
that become known only in a later message. It must not select the highest score
seen later in a scan and attribute it to the first message.

## Time Provenance

The HTML header's exact timestamp and UTC offset are preserved. The explicit
`israel-wall-clock-v1` policy treats the displayed clock as Israel local time,
as specified for this archive. `html-offset-v1` instead follows the header offset.
Both the header's UTC interpretation and the chosen normalized UTC interpretation
are stored; neither silently replaces the original.

The inspected archive has HTML `UTC+02:00` headers throughout, although the
corresponding Israel dates use UTC+03:00. Embedded dated CVD UTC timestamps and
printed data ages strongly support the Israel interpretation, but they describe
engine capture freshness rather than exact Telegram emission time. They are
recorded as descriptive provenance evidence, not an independent verification of
every alert's time. The normalization can be used immediately for source coverage.

`canonical_message_time_utc` in durable storage remains NULL pending a documented
review reference. Candidate eligibility is always false in the staging table,
including after time review. Direction mapping, immutable features, valid v7
price paths and verified BTC parent IDs remain separate reconstruction gates.

## Reproducible Preparation

From the repository, use the actual raw HTML directory and prior Sheet payload:

```bash
python research_telegram_html_archive.py /path/to/html-export \
  --chat-key coinglass-tracker-primary \
  --time-policy israel-wall-clock-v1 \
  --existing-import /path/to/previous/sheet_import_payload.json \
  --output-dir /path/to/archive-stage

python research_telegram_archive_stage_store.py \
  --stage-input-dir /path/to/archive-stage
```

Preparation produces `archive_manifest.json` and `archive_source_messages.jsonl`.
The dry-run validates source identity, raw-content revisions, normalization,
period membership, total row count and the archive digest before any connection.

## Durable Import

Install `028_telegram_archive_source_staging.sql` through the explicit migration
installer. Configure `RESEARCH_DATABASE_URL`, or opt in to `DATABASE_URL` with
`RESEARCH_USE_PRIMARY_DATABASE=1`, following the existing importer convention.
Set `RESEARCH_ARCHIVE_IMPORT_APPLY=1` only for the authorized import, then run:

```bash
python research_telegram_archive_stage_store.py \
  --stage-input-dir /path/to/archive-stage \
  --apply-staging \
  --expected-archive-digest <archive-digest-from-reviewed-manifest> \
  --expected-stage-digest <prepared-stage-digest-from-reviewed-manifest>
```

The source tables and intake memberships are idempotent. Each bounded batch is
committed separately and an interrupted import resumes safely on rerun. The
manifest is marked COMPLETE only after verifying the exact persisted member
count. Queries must select the desired completed `intake_batch_key`; summing
memberships across repeated batches would double count the same archive.

The import does not call Telegram, write Google Sheets, run the live outcome
worker, or alter existing research events and formulas.

If a production connection is unavailable, `--sqlite-path /path/to/archive.sqlite`
persists the same source intake as an isolated portable archive. It validates
SQLite integrity, foreign keys and the final row count. This does not count as
production database ingestion. Persist that artifact through the connected file
storage; a scratch-only copy is not a durable handoff. Both expected digests are
required for SQLite and PostgreSQL applies. The prepared-stage digest covers
normalization proposals, period membership, source annotations and existing links
in addition to the raw identity/content digest.

## Inspected Batch on 2026-09-07

- 34 HTML files; 14,354 unique source messages since August 16.
- 11,466 recognized signal messages; source/context messages are not additional
  signals or market waves.
- 318 source messages since September 4.
- 216 source IDs/texts link exactly to 216 existing events and 119 snapshots.
- No duplicate copies or conflicting message IDs in this export.
- No exported messages on August 30 through September 3; this is missing source
  coverage, not a no-signal control group.
- 2,183 messages contain dated CVD UTC timestamps plus printed age, yielding
  2,317 distinct within-message observations. The normalized residual is
  +0.03 to +13.62 minutes; following the HTML offset gives +60.03 to +73.62.
- Archive digest:
  `a71726ba6fc2a78d2f80ed98e800b60b07b8a355fd5eb5ef1b93daff0eeb8f27`.

Network-free verification:

```bash
python -m unittest research_telegram_html_archive_selftest research_telegram_archive_stage_store_selftest
```

The tests cover Israel cutoff boundaries, changed UTC interpretations, revision
conflicts, exact prior-import links, missing times, false eligibility and label
promotion, and tampered staging inputs. A production import still requires its
own persisted row-count and reimport checks.
