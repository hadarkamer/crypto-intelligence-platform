# Research deployment verification — 2026-09-07

Production main is 57784f7dc189f50412e58656ba57aaffaabb0791, tree
91b9ba1832982088d24cf06205d3ca276e5c44d2. Render deployment
dep-daf64von74is738gp8ng became live at 07:08:14.735869 UTC.
Migrations029–034 committed successfully at 07:08:02.287571 UTC.
The earlier disconnected-runtime checkpoint is historical and superseded.

## Observed runtime evidence

- HTTP health200 after deployment reported214 candidates and all72 questions.
- The first completed expanded pass screened160 delivered LIVE alerts and
  evaluated30 scopes, with zero formula-worker failures.
- At07:13:07 UTC:416 feature screens,461 question records,74 frozen conditions,
  zero frozen wave representatives while source coverage is incomplete,
  zero relevant scopes. Common windows:28 READY /52 OPEN; past features:
  1 READY /143 PENDING. Inverse requests:4 COMPLETE /99 PENDING /313 REJECTED.
- Rejected inverse sources failed the original canonical reference-provenance
  contract; missing/incorrect source identity is not repaired by inventing prices.
- Past feature event9213 (ZEC LONG) has6 complete asset and6 matching BTC closed
  windows. Event06:39:54.263117Z; last included close06:38:59.999Z.
- Startup and later targeted error logs through07:18:34Z showed no formula,
  inverse-sidecar, common-window or prior-price queue failure signature.
- Live Sheet has genuine delivered7Sep alerts through09:09 Israel, including
  snapshot6c26b352d6bf0d759442ae8b33694293c1c48353c88a35c556d4189d2ec11517.
  Earlier claims that only PROSPECTIVE arrived are superseded.
- Q01–Q60 F:H were updated from actual new screen/coverage records and read back
  exactly; question wording and previous history retained. Q46/Q72 were then
  refined with actual producer/archive evidence. Q65/66/68/69/70 recovery updates
  were also written and read back. No source tab was rewritten.

## Archive deliverables

Isolated archive run:
85466d2bc06ffea5ed583970fcd6e20d45a0cd321cdbaec5a6462733444e160d.

8074 supported Spot events,1151 unsupported HYPE events,516736 ordered-v7
measurement cells and64592 READY full-window metrics. Full audit:zero contract
violations and zero contradictory pairs.39296 DISCOVERY cells; no promotion.
Since4Sep:92 supported source events and at most1 decisive wave per cell.
Many required formula fields are absent from archive messages; conditional
captured-field samples are not population probabilities or proof of the first
possible matching alert among messages with unknown predicates.

Durably saved results/audit:
- outputs/telegram_archive_results_audit_20260907.zip
- SHA256 a0180496e033c28be15c79bb159f1206fc40bf75a6c66d340b6105d958dc09d6
- Library libfile_e83d0c50f6288191bdae71a883963c73 v0

Durably saved frozen source/price reconstruction inputs:
- outputs/telegram_archive_reconstruction_inputs_20260907.zip
- SHA256 6118a95be436046b632039f33c789db2a19c6d23bf182763d6bf45cd2b76fe0b
- Library libfile_b84ddbb740d08191af5e277f253a5f55 v0

The full142MB derived archive package failed transfer twice. The SQLite remains
at /tmp/telegram_archive_reconstructed_20260907/archive_reconstructed_research.sqlite
with SHA256 6dbccc3a1199a2b074dcea88789a42e167ee736b2d3e5b39e11794f6c2cb847e.
Frozen inputs plus released code can reproduce identical logical records offline;
a rebuilt SQLite's physical SHA may differ and must be explicitly verified anew.

## Exact remaining work

1. Authenticated production file delivery/execution is not exposed. Do not use
   read-only Render SQL as a write route or publish private source data in Git.
   After legitimate local-runtime delivery, use the tested archive importer with
   explicit artifact SHA, run key and bounded batches. No production archive
   rows have been imported.
2. Bound Google Apps Script batch-v2/header-guard release still needs Google
   account authentication; connector Sheet editing is already functional.
   Do not retry BrowserAuth without a new interactive opportunity.
3. Source/history/inverse/common-window backfill remains partial. Continue bounded
   queues; incomplete populations must not freeze later representatives or
   expose reliable rates. Preserve all original sources, versions and periods.
4. Exact ordered-v7/200bps-parent/common-window numerical acceptance rules are
   not documented. Do not borrow legacy v6 policy or invent thresholds.
   Future whole waves after real database-time freeze are still needed.
5. Sequence questions and full market-regime definitions remain partial or
   unimplemented. Feature screens are not completed statistical research.

