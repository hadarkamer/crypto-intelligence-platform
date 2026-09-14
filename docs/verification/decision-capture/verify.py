"""Read-only PR43 verification; run from the deployed project directory.

    python /tmp/combined_capture_verify.py
    python /tmp/combined_capture_verify.py --sql-only

The default mode fetches exactly one accepted source row, performs the deployed
pure validator, and prints only summary JSON. No main import, scoring, provider,
delivery, intake mutation, or archive write is invoked. Exit 3 means no new block
has reached accepted intake yet; exit 2 means verification did not pass.
"""
from collections import Counter
import json
from pathlib import Path
import re
import sys


CUTOFF = 1306
BINDING_SQL = """SELECT
    CASE WHEN snapshot_set_id<=1306 THEN 'AT_OR_BEFORE_1306' ELSE 'AFTER_1306' END AS source_range,
    capture_version,capture_status,binding_status,
    count(DISTINCT snapshot_set_id) AS scans,count(*) AS coin_rows,
    count(*) FILTER(WHERE consumer_validation_required IS NOT TRUE
        OR is_delivered_alert IS NOT FALSE
        OR qualifies_as_prospective_formula_evidence IS NOT FALSE) AS invalid_evidence_flags
FROM research_watch_scan_decision_captures
GROUP BY 1,2,3,4 ORDER BY 1,2,3,4;"""

LATEST_SQL = """SELECT i.snapshot_set_id,i.watch_scan_id,i.bundle_sha256,
    i.parent_payload_sha256,i.source_available_at_utc,i.source_created_at_utc,i.usable_from_utc,
    s.available_at_utc AS stored_available_at_utc,s.payload_sha256 AS stored_parent_sha256,
    s.source_metadata#>'{capture_metadata,operational_scores}' AS scores,
    s.source_metadata#>'{capture_metadata,operational_decisions}' AS decisions
FROM research_watch_scan_intakes i
JOIN research_max_pain_snapshot_sets s USING(snapshot_set_id)
WHERE i.consumer_version='watch-all-scan-intake-v1' AND i.intake_status='ACCEPTED'
    AND s.source_metadata#>'{capture_metadata,operational_decisions}' IS NOT NULL
ORDER BY i.usable_from_utc DESC,i.snapshot_set_id DESC LIMIT 1"""


def emit(value):
    print(json.dumps(value, sort_keys=True, separators=(',', ':'), default=str))


def safe_code(value):
    return value if isinstance(value, str) and re.fullmatch(r'[A-Za-z0-9_:|.\-]{1,200}', value) else 'UNSTRUCTURED'


def safe_hash(value):
    return value if isinstance(value, str) and re.fullmatch(r'[0-9a-f]{64}', value) else None


def literal_counts(records, field):
    counts = Counter()
    for record in records:
        value = record
        for key in field:
            value = value.get(key) if isinstance(value, dict) else None
        counts[safe_code(value) if value is not None else 'MISSING'] += 1
    return dict(sorted(counts.items()))


def main():
    if sys.argv[1:] == ['--sql-only']:
        print(BINDING_SQL)
        return 0
    if sys.argv[1:]:
        emit({'verification': 'INVALID_ARGUMENTS'})
        return 2
    # /tmp is Python's initial import path; load the installed project modules
    # from the working directory, without importing the application's main.py.
    sys.path.insert(0, str(Path.cwd()))
    try:
        import research_watch_decision_capture as capture
        import research_watch_scan_intake as intake
    except Exception as exc:
        emit({'verification': 'MODULE_LOAD_FAILED', 'error_type': type(exc).__name__})
        return 2
    try:
        with intake._connect() as conn:
            conn.execute('SET TRANSACTION READ ONLY')
            conn.execute("SET LOCAL statement_timeout='10s'")
            row = conn.execute(LATEST_SQL).fetchone()
    except Exception as exc:
        emit({'verification': 'SOURCE_READ_FAILED', 'error_type': type(exc).__name__})
        return 2
    if row is None:
        emit({'verification': 'NO_ACCEPTED_DECISION_CAPTURE_YET', 'cutoff': CUTOFF})
        return 3

    block, scores = row['decisions'], row['scores']
    summary = {
        'snapshot_set_id': row['snapshot_set_id'], 'watch_scan_id': row['watch_scan_id'],
        'source_range': 'AFTER_1306' if row['snapshot_set_id'] > CUTOFF else 'AT_OR_BEFORE_1306',
        'source_available_at_utc': row['source_available_at_utc'],
        'source_created_at_utc': row['source_created_at_utc'], 'usable_from_utc': row['usable_from_utc'],
        'validation': 'NOT_RUN',
    }
    if isinstance(block, dict):
        summary.update(capture_status=safe_code(block.get('status')),
            decision_bytes=len(capture.canonical(block).encode()),
            decision_sha256=safe_hash(block.get('payload_sha256')),
            computed_at_utc=block.get('computed_at_utc'))
        if block.get('status') == 'FAILED':
            summary['capture_failure_reason'] = safe_code(block.get('reason'))
    lineage = {
        'parent_hash': row['parent_payload_sha256'] == row['stored_parent_sha256'],
        'score_hash': isinstance(scores, dict) and row['bundle_sha256'] == scores.get('payload_sha256'),
        'source_availability': row['source_available_at_utc'] == row['stored_available_at_utc'],
    }
    summary['source_binding'] = lineage
    try:
        capture.validate_bundle(block, scores, cycle_id=row['watch_scan_id'],
            available_at_utc=row['stored_available_at_utc'])
    except Exception as exc:
        summary.update(verification='FAILED', validation='FAILED', reason=capture.error_reason(exc))
        emit(summary)
        return 2

    deployed_hashes = capture.code_versions()
    code_matches = {name: block['code_sha256'].get(name) == value
                    for name, value in deployed_hashes.items()}
    summary.update(validation='PASSED', code_hash_matches=code_matches,
        all_code_hashes_match=all(code_matches.values()),
        canonical_hash_matches=capture.digest({key: value for key, value in block.items()
            if key != 'payload_sha256'}) == block['payload_sha256'],
        operational_input_counts=block['operational_input_counts'])
    per_coin, total = {}, Counter()
    all_items, all_magnets = [], []
    for symbol in capture.SYMBOLS:
        coin = block['coins'][symbol]
        items, groups = coin['prepared_items'], coin['combined_groups']
        candidates, magnets = coin['combined_candidates'], coin['magnet_evaluations']
        counts = {'prepared_items': len(items), 'displayable_items': len(coin['displayable_item_ids']),
            'groups': len(groups), 'qualified_groups': sum(group['qualified'] for group in groups),
            'candidates': len(candidates), 'magnets': len(magnets)}
        total.update(counts)
        missing = coin['missing_evidence']
        per_coin[symbol] = {
            'status': coin['status'], 'missing_reason_count': len(missing),
            'missing_reasons': [safe_code(reason) for reason in missing[:12]],
            **counts, 'maxpain_statuses': literal_counts(items, ('maxpain_confirmation', 'status')),
            'magnet_evaluation_statuses': literal_counts(magnets, ('evaluation_status',)),
            'magnet_confirmation_statuses': literal_counts(magnets, ('confirmation', 'status')),
        }
        all_items.extend(items)
        all_magnets.extend(magnets)
    summary.update(coins=per_coin, totals=dict(total),
        literal_maxpain_statuses=literal_counts(all_items, ('maxpain_confirmation', 'status')),
        literal_magnet_evaluation_statuses=literal_counts(all_magnets, ('evaluation_status',)),
        literal_magnet_confirmation_statuses=literal_counts(all_magnets, ('confirmation', 'status')))
    passed = all(lineage.values()) and all(code_matches.values()) and summary['canonical_hash_matches']
    summary['verification'] = 'PASSED' if passed else 'FAILED'
    emit(summary)
    return 0 if passed else 2


if __name__ == '__main__':
    try:
        result = main()
    except Exception as exc:
        emit({'verification': 'CHECK_FAILED', 'error_type': type(exc).__name__})
        result = 2
    raise SystemExit(result)
