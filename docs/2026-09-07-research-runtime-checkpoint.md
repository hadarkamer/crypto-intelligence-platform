# Research implementation checkpoint — 2026-09-07

## Exact stopping condition

The shared execution environment became disconnected (transport recovery timeout,
then HTTP 409 environment_offline). Repeated minimal reads failed. The Render
application and connected Google Sheet remained reachable. This is an execution
environment outage; its cause is unknown. Do not claim the new implementation
was deployed, the archive finished, or an approval was rejected.

At 2026-09-07T06:41:52Z production still had 7 registered candidates, 0 relevant
scopes, and no research_common_window_metrics or research_past_price_features
table. Production remains commit512274e9b0676a5f8b97380586a64b2408c2169f.
Each current period has 2432 scopes and at most 2 independent waves:
ALL_COMPATIBLE_SINCE_20260816 and SINCE_20260904. LEGACY_UNSCOPED is audit only.
Real delivered LIVE alerts dated 2026-09-07 09:09 Israel were read in the main
Sheet, including row1562 ZEC SHORT with snapshot
6c26b352d6bf0d759442ae8b33694293c1c48353c88a35c556d4189d2ec11517.
The existing bot continues; do not disable recurring monitoring for this
transient local environment outage.

## Unreleased local work

Checkout /workspace/scratch/798287b8bc55/repo, branch
implementation/research-completion-20260907. Edits were not committed before the
outage. This checkpoint contains documentation only, not those source changes.
Recover the existing checkout; do not recreate changes from summaries or reset it.

Completed implementation:
- 029_common_window_metrics.sql and research_common_window_metrics*.py.
- 030_ordered_question_search.sql, research_ordered_question_catalog/store,
  ordered formula evaluator/store/worker integration, Q01–Q72 map.
- 031_ordered_prospective_validation.sql and research_ordered_validation*.py.
- 032_telegram_archive_reconstruction.sql plus archive features/backfill/audit/
  summary modules and research_telegram_archive_runtime_importer*.py.
- 033_ordered_inverse_analysis_requests.sql and research_ordered_inverse*.py.
- 034_past_price_features.sql and research_past_price_features*.py.
- research_outcome_worker.py sidecar hooks; research_formula_schema_admin.py
  registration029–034; conservative unversioned receiver single-row bootstrap.

Catalog:214 candidates,107 normal plus107 inverse, original seven unchanged.
All8thresholds and4horizons remain separate. Archive delayed Spot entry is a
separate source/entry contract and cannot enter LIVE evidence.

Core behavioral, storage and integration tests passed before the outage,
including18 v7 evidence tests,128 calculator/storage cases,24 validation tests,
6 runtime archive importer tests, past-price and inverse tests, existing worker,
queue, delivery tests and Google Apps Script self-tests.

Critical fixes reviewed:
- Outcome reference price and measured event ID must match immutable entry.
- Initially incomplete populations do not freeze a later wave representative.
- Fixed-window end and observation time cannot exceed evaluation time.
- Inverse schema absence is safe; a durable cursor recovers cached requests.
- Invalid inverse inputs remain explicit immutable rejection records.
- FRESH counts/metrics use the same whole-parent14day cohort.
- Prior price paths contain only closed candles before entry, never future MFE.

Last required regression is still unexecuted:
The new candidate-specific historical feature coverage gate was written and
Python-compiled. Read-only PostgreSQL probes passed six truth cases and the
same-wave earliest-unknown case. The environment failed before adding/running
the end-to-end regression. Test an earlier unknown prior-feature alert followed
by a later matching success: coverage false, no usable rate, no frozen entry.
After the earlier history arrives and matches with failure, the earlier entry
must be selected without a false immutable-representative conflict. Review the
patch and run this regression BEFORE deploying. No numerical acceptance policy
was invented; exact compatible policy remains missing.

## Archive resume

Raw source stage remains separate and was previously saved durably.
Current reconstruction SQLite:
 /tmp/telegram_archive_reconstructed_20260907/archive_reconstructed_research.sqlite
Run key:
 85466d2bc06ffea5ed583970fcd6e20d45a0cd321cdbaec5a6462733444e160d
First check whether running session8433 survived; do not start a duplicate writer.
Last verified per-event committed checkpoint:
7007 Spot events complete with64v7labels and8fixed-windowmetrics each;
945 HYPE exclusions;1273 pending, out of9225 eligible source messages.
2241 further source messages lack own coin/direction evidence;2888 are context.
14354 raw messages remain preserved.
Official Spot cache extensions from14 successful requests were saved already;
the remaining pass needs no extra network.
Complete pending events using the SAME run/stage/cache/hash/output, then run
research_telegram_archive_audit.py and research_telegram_archive_summary.py.
The full audit, candidate summaries and final artifact packaging HAVE NOT RUN.
Persist the verified artifact after completion; do not package a running SQLite.

Production archive intake requires both raw stage importer (migration028) and
reconstruction importer(migration032). The latter verifies exact local artifact
SHA256 and run key, transfers each event+64labels+8metrics atomically, and
preserves conflicts/idempotence. No authenticated artifact delivery/execution
route was found in the connected Render tools or existing app endpoints.
Do not use public repositories, credential hunting, or a read-only SQL tool to
bypass that access limitation.

## Remaining external dependencies

Google receiver batch-v2/header contract code passed local tests but was not
deployed; authenticated Google Apps Script access was not completed. Connected
Sheet editing works separately. Do not repeat an authentication prompt during
a noninteractive automation run.

After runtime recovery and the final regression:
commit the actual complete checked-out changes; compare GitHub tree hashes;
publish the authorized tested main update; apply029–034 through the existing
explicit schema installer; verify real DB rows, bounded worker progress,
source/period separation, inverse linkage and readiness blockers; then update
Sheet research coverage from actual measurements. A successful Render build
alone does not prove the research ran. Unsupported sequence/range-regime
questions and missing exact acceptance configuration remain truthful gaps.
