# Ordered common-window binding: release readiness

This is a future operator plan, not authorization or evidence of a production deployment. The integration unit is exactly `research_common_window_metrics_store.py` and `research_common_window_metrics_store_selftest.py`. Do not merge the experimental branch, its native runners, build adapters, benchmark fixtures or evidence files into the application release.

The application change constructs PostgreSQL array text from the existing normalized integer IDs and binds it to the existing `%s::bigint[]` parameter. It changes two executable lines plus comments. Integer conversion, sorting/deduplication, the 10,000-unique-ID bound, empty-input behavior, SQL text, horizon/version filtering and returned-row precedence remain unchanged. No schema, environment, scheduling, worker or transaction-automation changes belong in this release.

**Recorded base and file identities**

| Item | Identity |
|---|---|
| Reviewed application base / recorded production main | `3c5bdb9ae5a450d1b3fb0cd4c71bab599d252ad3` |
| Base application SHA256 | `e1e74fc7cac10b5b454614c242621857ab1ae5d23cdbffe6e20dc961051f5341` |
| Fixed application SHA256 | `ac2e05e3157928f9daaa8735c0aee533dc3ce2886251a1242e9344a93ba752fd` |
| New focused selftest SHA256 | `12e2afa4adc4770876aa6839d59ba10487d2441745e74f8d45328456a0859021` |

At the recorded October 13 check, production service `srv-d94ek17lk1mc73b4tb90` was live on the base above through deployment `dep-db5tphnavr4c73ffj6hg`, with automatic deployment from `main` enabled. Merging the application PR can therefore initiate deployment; it is not merely a repository action. Recheck these facts at the actual release boundary.

The parallel service `srv-dakptbh594qs7395460g` uses `paper-trading-v1`, with automatic deployment disabled and recorded commit prefix `30da26f`. It is outside this integration and must remain unchanged.

**Evidence and remaining gates**

| Gate | Current evidence / required completion |
|---|---|
| Local application regressions | PASS: three existing assertion scripts and the new 11-test focused suite. The new suite exercises the actual loader and actual NORMAL/INVERSE caller mapping through an explicit fake connection. |
| Native positive correctness | PASS: 32-row full-scope parity and five actual-loader cases, including empty input, duplicates/missing IDs, existing integer normalization and signed/string int64 boundaries. |
| Native sequential measurement | PASS integrity: four fresh ABBA runs, 12 passes each; approximately 4.57% lower warm endpoint RSS in this synthetic workload. |
| Native concurrent measurement | PASS integrity: four fresh ABBA runs, 48 rounds / 96 tasks; approximately 12.02% lower warm both-loaded RSS. Positive endpoint growth remains. |
| Native overflow / recovery | **PENDING.** Both baseline and fixed arms must produce accepted receipts for values above and below int64, expected SQLSTATE `22003`, read-only execution, rollback recovery, unchanged reference results and closed fresh connections. A prepared checker is not completion evidence. |
| Application PR CI | **PENDING.** Require successful checks on the exact integration PR head/merge tree, not the experimental branch or earlier native build. |

The native experiments establish bounded synthetic correctness and memory behavior. They do not establish production equivalence or resolution of OOM. The loaded production psycopg implementation/libpq version and long-duration memory behavior remain unknown. Within-process repetitions are correlated; sampled RSS and cumulative process high-water are separate measurements.

**Release sequence**

1. Finish and independently verify the pending baseline/fixed overflow receipts. Bind their application, driver and runtime identities to the reviewed sources; retain the original receipts. Do not mark this gate complete from a build status alone.
2. Fetch current `main`. If it still matches the recorded base, create an isolated application branch from it and apply only the two executable-line binding change, its comments and the new focused selftest. Verify the two SHA256 values above and a diff containing exactly those two files.
3. If `main` has advanced, apply the small change to current main without replacing the whole application file from the old experiment. Review intervening edits, record the resulting new source hash and rerun the affected checks. Do not assume the original fixed-file hash still applies.
4. Run `python research_common_window_metrics_store_selftest.py` and the existing `research_common_window_metrics_selftest.py`, `research_formula_ordered_store_selftest.py` and `research_formula_ordered_worker_selftest.py`. The focused suite must report 11 tests with no failures or skips. Local fake-connection tests do not substitute for native PostgreSQL receipts.
5. Open the application-only PR against `main`. `.github/workflows/production-ai-analytics-check.yml` runs for PRs targeting main and pushes to main; a push to the experimental branch alone does not trigger it. Require both configured jobs to pass, including tracked-Python compilation, the exact tracked selftest inventory, real PostgreSQL checks, receiver-dependency provenance, source integrity and the Sheets contract. Confirm the new focused script ran. The unrelated alert-card workflow has path filters and is not an expected gate for this two-file patch.
6. Bind the successful CI checkout to the exact reviewed PR tree, confirm there are no unresolved review findings, and record the resulting merge/deployment identities. Apply the existing session authorization and active scope at the release boundary. This readiness step covers the isolated experimental branch and build only; it does not perform the main merge or consequent automatic deployment.
7. After deployment, verify the deployed commit and normal research cycles, absence of new metric-loading SQL/conversion errors, continued source freshness and unchanged result/missingness behavior. Compare existing process-memory observations across multiple complete cycles before making a production efficacy claim. Check that the parallel service was not changed.

**Rollback**

If the release introduces metric-loading failures, changed missingness/results, stalled research cycles or an unacceptable operational regression, first identify the deployed release commit. Restore only the application's prior list binding through a new reviewed change on current `main`; preserve subsequent unrelated commits. If no later edits overlap, reverse the release's application-file hunk. If they do overlap, manually restore those two executable lines in the current file and inspect the complete diff. Do not reset main, deploy the entire experimental branch or replace the complete application file with an old version.

The new selftest deliberately requires the scalar binding. A rollback PR must also revert that release-specific test addition, or adapt only its binding expectation to the restored application behavior while retaining the semantic assertions. Otherwise CI should fail. Run the applicable checks, record the rollback commit/deployment and verify normal collection/research operation. No migration, environment reversal, state reset or parallel-service action is required by this patch.
