Automatic descriptive evaluation of the existing forward paper study.

The evaluator runs inside the existing bot after research schema readiness.
It does not create alerts,orders,positions or source observations.
The prospective paper policy and its source/engine hashes remain unchanged.

The v2 worker reads the authoritative study snapshot,bounded quote windows,
and complete selected source evidence in one repeatable-read read-only
transaction,then writes only its separate additive
research_forward_paper_evaluations table. A separate advisory lock allows
one evaluation writer during rolling deployments.

Selected source evidence contains every bootstrap or nonempty candidate
packet through the exact snapshot journal cutoff. SQL counts include all
candidate and quote inputs,including omitted empty candidate packets.
Each selected packet receives its true global input ordinal,number of prior
rejections,successful observation sequence,and next-input sequence boundary.
Sparse bootstrap,choice,source-drift and input-rejection decisions are read
from that same snapshot. Counts prove that selected evidence was not silently
truncated. A history containing whole rejected inputs remains pending source
review; it is never quietly approved by omitting those inputs.

The dataset fingerprint excludes heartbeat clocks and changing quote marks.
It retains policy identity,eligible-event records,choices,closed positions,
and relevant active/reservation/uncertainty state. An unchanged successful
assessment is reused; no assessment is added for each quote poll.
Source freshness is checked independently on every evaluation poll.
Lightweight source coverage counts are also checked before cache lookup.
New nonempty/incomplete source packets or sparse source decisions invalidate
the meaningful source marker even before an event becomes eligible. Growing
empty-poll totals do not themselves create a new assessment. Journal/input
integrity booleans remain part of that marker.

PASS means the frozen source-eligibility gates,first raw/eligible receipts,
bootstrap exclusions,immutable parent choices,supplied snapshot calculations
and observed price paths of closed cases agree. It does not recompute the
upstream formula predicates from raw market features,approve a strategy,
replay the complete quote stream,or prove exchange execution. Book rows reuse
economic cases; BTC parent waves are counted separately. Open and incomplete
waves are not counted as fully comparable completed waves.

The baseline.json pins the original study ID,policy/config hashes and start.
Its original runtime commit is provenance; the running bot may later deploy
unrelated changes while preserving the frozen study.
Evaluator version and a combined normalized hash of the evaluator,source-audit
module and worker identify each calculation. The dataset cache key also pins
the critical baseline fields and meaningful source-selection marker. Component
hashes are exposed separately in sanitized health provenance. Failed/pending
evidence never becomes a quiet successful cache. Missing source or quote
evidence is retried without inventing data. An assessment PASS is immutable;
a previous FAIL may only be upgraded to PASS for the same identity.

Operational bounds are50,000quote packets,256closed case windows,5,000selected
source packets,20,000candidate rows and10,000sparse source decisions. A shared
20-second evidence-fetch budget and database/network timeouts bound the work.
Exceeding a cap reports EVIDENCE_LIMIT without truncation or approval. These
are runtime resource limits,not scientific sample-size approval thresholds.

Health key:forward_evaluation. Log prefix:[FORWARD_EVALUATION].
The complete assessment is stored in research_forward_paper_evaluations.
No automatic winner,statistical-approval threshold or trading promotion
is defined. Continue collecting new waves under the original protocol.

CI runs research_forward_evaluation_selftest.py and
research_forward_evaluation_worker_selftest.py and
research_forward_source_audit_selftest.py under the existing convention.
