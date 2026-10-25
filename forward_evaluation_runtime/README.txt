Automatic descriptive evaluation of the existing forward paper study.

The evaluator runs inside the existing bot after research schema readiness.
It does not create alerts,orders,positions or source observations.
The prospective paper policy and its source/engine hashes remain unchanged.

The worker reads the authoritative study snapshot and bounded quote windows
in short read-only transactions,then writes only its separate additive
research_forward_paper_evaluations table. A separate advisory lock allows
one evaluation writer during rolling deployments.

The dataset fingerprint excludes heartbeat clocks and changing quote marks.
It retains policy identity,eligible-event records,choices,closed positions,
and relevant active/reservation/uncertainty state. An unchanged successful
assessment is reused; no assessment is added for each quote poll.
Source freshness is checked independently on every evaluation poll.

PASS means supplied policy/snapshot calculations and the observed price
paths of closed cases agree. It is not a strategy approval,source-eligibility
replay,full-stream replay,or evidence of exchange execution. Book rows reuse
economic cases; BTC parent waves are counted separately. Open and incomplete
waves are not counted as fully comparable completed waves.

The baseline.json pins the original study ID,policy/config hashes and start.
Its original runtime commit is provenance; the running bot may later deploy
unrelated changes while preserving the frozen study.
Evaluator version and normalized evaluator-source hash identify each
calculation. Failed/pending evidence never becomes a quiet successful cache.
Changes to worker evidence-selection semantics require an evaluator-version
bump,so a previous successful assessment cannot mask a changed audit scope.
Missing quote evidence is retried without inventing data.

Health key:forward_evaluation. Log prefix:[FORWARD_EVALUATION].
The complete assessment is stored in research_forward_paper_evaluations.
No automatic winner,statistical-approval threshold or trading promotion
is defined. Continue collecting new waves under the original protocol.

CI runs research_forward_evaluation_selftest.py and
research_forward_evaluation_worker_selftest.py under the existing convention.
