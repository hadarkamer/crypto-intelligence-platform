"""Stage-only read-only audit. No operational scan or database writes."""
from collections import Counter
import json
from pathlib import Path

import research_watch_decision_contract_probe as probe


checks = []
for snapshot_id in (1308, 1306):
    with probe.intake._connect() as conn:
        report = probe.evaluate_source(probe.read_source(conn, snapshot_id))
    evaluation = report['evaluation']
    summary = probe.summarize(report)
    normalized = probe.contract.legacy._normalized_payload(json.loads(json.dumps(evaluation)))
    hash_matches = probe.contract.digest({key: value for key, value in normalized.items()
        if key != 'payload_sha256'}) == normalized['payload_sha256']
    by_coin = {}
    for symbol, coin in evaluation['coins'].items():
        by_coin[symbol] = dict(Counter(decision['match_status']
            for unit in coin['units'] for decision in unit['evaluations']))
    compact = {key: summary[key] for key in (
        'snapshot_set_id', 'source_binding', 'source_status', 'contract_version',
        'contract_status', 'source_gate', 'capture_status', 'catalog_summary', 'payload_sha256')}
    compact['populations'] = summary['populations']
    compact['jsonb_canonical_hash_matches'] = hash_matches
    compact['all_execution_flags_inactive'] = not any(summary['flags'][key] for key in (
        'support_activation', 'cohort_eligible', 'qualifies_as_prospective_formula_evidence',
        'is_delivered_alert', 'is_false_signal_control', 'outcome_reuse_authorized'))
    compact['by_coin'] = by_coin
    Path('/tmp/decision-contract-audit-'+str(snapshot_id)+'.json').write_text(
        json.dumps({'summary': summary, 'evaluation': evaluation}, sort_keys=True, default=str))
    checks.append(compact)

Path('/tmp/decision-contract-audit-summary.json').write_text(json.dumps(checks, sort_keys=True))
for check in checks:
    concise = {key: value for key, value in check.items() if key != 'by_coin'}
    print('DECISION_CONTRACT_AUDIT_'+str(check['snapshot_set_id'])+' '+json.dumps(concise, sort_keys=True), flush=True)
