"""Read-only, explicitly invoked audit of the inactive Watch decision contract.

No application startup hook imports this module. It reads one accepted frozen
source, never providers, later events, outcomes, or delivery state. Optional
output is a local audit file; no database or reporting pointer is written.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path

import research_watch_decision_contract as contract
import research_watch_scan_intake as intake


SOURCE_SQL = """SELECT i.snapshot_set_id,i.watch_scan_id,i.bundle_sha256,
    i.parent_payload_sha256,i.source_available_at_utc,i.usable_from_utc,
    s.payload_sha256 AS stored_parent_sha256,s.available_at_utc AS stored_available_at_utc,
    s.source_metadata#>'{capture_metadata,operational_scores}' AS scores,
    s.source_metadata#>'{capture_metadata,operational_decisions}' AS decisions
FROM research_watch_scan_intakes i
JOIN research_max_pain_snapshot_sets s USING(snapshot_set_id)
WHERE i.consumer_version='watch-all-scan-intake-v1' AND i.intake_status='ACCEPTED'
    AND (%s::bigint IS NULL OR i.snapshot_set_id=%s::bigint)
ORDER BY i.usable_from_utc DESC,i.snapshot_set_id DESC LIMIT 1"""


def read_source(conn, snapshot_set_id=None):
    if snapshot_set_id is not None and (type(snapshot_set_id) is not int or snapshot_set_id <= 0):
        raise ValueError('INVALID_SNAPSHOT_ID')
    conn.execute('SET TRANSACTION READ ONLY')
    conn.execute("SET LOCAL statement_timeout='10s'")
    return conn.execute(SOURCE_SQL, (snapshot_set_id, snapshot_set_id)).fetchone()


def evaluate_source(row):
    if row is None:
        return {'source_status': 'UNKNOWN', 'source_reasons': ['NO_ACCEPTED_SOURCE'], 'evaluation': None}
    scores = row['scores']
    checks = {
        'parent_hash': row['parent_payload_sha256'] == row['stored_parent_sha256'],
        'score_hash': isinstance(scores, dict) and row['bundle_sha256'] == scores.get('payload_sha256'),
        'source_availability': row['source_available_at_utc'] == row['stored_available_at_utc'],
    }
    result = {'snapshot_set_id': row['snapshot_set_id'], 'watch_scan_id': row['watch_scan_id'],
        'usable_from_utc': row['usable_from_utc'], 'source_binding': checks,
        'source_status': 'BOUND' if all(checks.values()) else 'UNKNOWN',
        'source_reasons': [key.upper()+'_MISMATCH' for key, valid in checks.items() if not valid],
        'evaluation': None}
    if all(checks.values()):
        result['evaluation'] = contract.evaluate_capture(row['decisions'], scores,
            cycle_id=row['watch_scan_id'], available_at_utc=row['stored_available_at_utc'])
    return result


def summarize(report):
    result = {key: value for key, value in report.items() if key != 'evaluation'}
    evaluation = report.get('evaluation')
    result['checked_at_utc'] = datetime.now(timezone.utc).isoformat()
    result['mode'] = 'INACTIVE_READ_ONLY_CONTRACT_PROBE'
    if evaluation is None:
        return result
    result.update(contract_version=evaluation['version'], contract_status=evaluation['status'],
        source_gate=evaluation['source_gate'], capture_status=evaluation['capture_status'],
        catalog_summary=evaluation['catalog_summary'], payload_sha256=evaluation['payload_sha256'],
        flags=evaluation['flags'])
    populations = {}
    for coin in evaluation['coins'].values():
        for unit in coin['units']:
            entry = populations.setdefault(unit['population'], {
                'units': 0, 'selection': Counter(), 'evaluations': Counter(), 'missing_features': Counter()})
            entry['units'] += 1
            entry['selection'][unit['selection_status']] += 1
            for decision in unit['evaluations']:
                entry['evaluations'][decision['match_status']] += 1
                entry['missing_features'].update(decision['missing_features'])
    result['populations'] = populations
    result['coin_population_status'] = {symbol: {
        name: value['status'] for name, value in coin['populations'].items()}
        for symbol, coin in evaluation['coins'].items()}
    result['blocked_definitions'] = [row['candidate_key'] for row in evaluation['blocked_definitions']]
    result['structurally_unreachable_definitions'] = [row['candidate_key']
        for row in contract.catalog_contracts() if row['structurally_unreachable']]
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot-set-id', type=int, help='Exact accepted source; latest accepted source by default')
    parser.add_argument('--output', type=Path, help='Optional full local audit JSON')
    args = parser.parse_args(argv)
    try:
        with intake._connect() as conn:
            report = evaluate_source(read_source(conn, args.snapshot_set_id))
        if args.output:
            args.output.write_text(json.dumps(report, sort_keys=True, indent=2, default=str)+'\n')
        print(json.dumps(summarize(report), sort_keys=True, separators=(',', ':'), default=str))
        return 0 if report['source_status'] == 'BOUND' and report['evaluation']['status'] == 'VALID' else 3
    except Exception as exc:
        # Database errors can contain connection details; expose only the class.
        print(json.dumps({'mode': 'INACTIVE_READ_ONLY_CONTRACT_PROBE', 'status': 'ERROR',
            'error_type': type(exc).__name__}, sort_keys=True))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
