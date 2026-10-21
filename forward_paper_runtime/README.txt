Forward DOGE/ETH paper observer

Runs inside the existing main bot service after research schema readiness.
It polls source candidates and public Binance book quotes every 10 seconds.
It has no notification, account, order or execution integration.

The source SELECT runs in a separate short read-only UTC transaction.
Only research_forward_paper_studies and research_forward_paper_journal are
written. A session advisory lock permits a single active observer during
rolling deployments. Each observation is atomically checkpointed with its
paper state before the next request; failures discard local state and reload
Postgres. No persistence relies on the Render ephemeral filesystem.

The first activation freezes study_start_utc in Postgres. Existing rows are
logged as bootstrap and excluded. New whole BTC parent waves and alerts must
begin after activation. Recorded creation clocks cannot backdate entry.
The three books are DOGE_only, ETH_only and first_per_parent. Selection and
entry rules are in policy_template.json; actual policy, hashes and status
are in the study row. Quote outcomes are discrete midpoint observations with
the declared cost scenario, never evidence of executable fills. Quote gaps
and open-position process resumes retain permanent uncertainty flags.

Health: runtime status key forward_paper; application logs [FORWARD_PAPER].
Durable health query:
 SELECT study_id,activated_at_utc,updated_at_utc,journal_seq,last_status
 FROM research_forward_paper_studies;
The healthy leader state is OBSERVING. STANDBY during overlapping instances
means another observer holds the lock. RETRYING records a sanitized error
type; missing observations are never fabricated or backdated.

Do not edit the frozen configuration or source logic under the same study
identifier. A changed definition/query/core requires a distinct study ID.
To stop future collection, pass enabled=False in main.py and deploy through
the normal repository path. Keep the existing study and journal records.
The disposable SQLite working cache uses a temporary file with bounded page
cache memory. Its input identities and the Postgres journal grow with the
number of observations and are not silently pruned in this pilot.

Validation: python -m unittest -v test_forward_paper_engine
 test_forward_paper_store test_forward_paper_worker
