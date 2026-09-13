"""Finite research publication; PostgreSQL retains the complete evidence.

This contract only chooses what is published. A catalog mismatch closes this
publication lane, never research evaluation or its canonical persistence.
"""
from __future__ import annotations

from functools import lru_cache
import hashlib
import json
from typing import Any, Mapping

import research_formula_ordered_v7 as evaluator
import research_ordered_question_catalog as questions

VERSION = 'formula-current-all-298-v1'
SHEET = 'Formula_Current'
LEGACY_SHEETS = ('Episodes', 'Formula_Results')
MAX_DATA_ROWS = 38_144
MAX_COLUMNS = 25
MAX_GRID_CELLS = (MAX_DATA_ROWS + 1) * MAX_COLUMNS
CATALOG_KEY_SHA256 = '94da1378ab9c48cbd00f8db286394d1ff2cf9cbf7f787500103668f9b5bdc7ef'
CATALOG_DEFINITION_SHA256 = '1e593fd67d7a3cc9c89ccc001677e5e59552d548185e5d7ef6e8bfdf89e5e6ae'
QUESTION_VERSION = 'captured-question-search-v3-experimental-binding'
POLICY_VERSION = 'formula-evidence-v1-ordered-v7-btc-parent-5-or-fresh3'
BASE_CATALOG_VERSION = 'ordered-v7-total-score-simple-pairs-strict65-v1'
PARENT_POLICY = 'btc-parent-close-reversal-200bps-v1'
PERIOD_VERSION = 'live-israel-cutoffs-v1'
PERIODS = ('ALL_COMPATIBLE_SINCE_20260816', 'SINCE_20260904')
HORIZONS = (60, 240, 720, 1440)
THRESHOLDS_BPS = tuple(range(25, 201, 25))
KEY = 'candidate_key,coin_scope,direction,threshold_pct,horizon,formula_version'
HEADERS = (
    'candidate_key', 'candidate_name', 'exact_conditions', 'coin_scope',
    'direction', 'threshold_pct', 'horizon', 'independent_episodes',
    'successes', 'failures', 'open_episodes', 'hit_rate', 'median_mfe_pct',
    'median_mae_pct', 'asymmetry_ratio', 'opposite_indicator_test',
    'current_period_hit_rate', 'prior_period_hit_rate', 'change_pp',
    'strongest_failure_pattern', 'status', 'meets_min_5',
    'last_evaluated_at', 'chat_summary', 'formula_version',
)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, default=str, allow_nan=False)


@lru_cache(maxsize=1)
def catalog_contract() -> dict[str, Any]:
    catalog = sorted(evaluator.candidate_catalog(include_extended=True),
                     key=lambda item: item['formula_id'])
    keys = [item['formula_id'] for item in catalog]
    key_hash = hashlib.sha256(_json(keys).encode()).hexdigest()
    definition_hash = hashlib.sha256(_json(catalog).encode()).hexdigest()
    compatible = (len(keys) == len(set(keys)) == 298
                  and key_hash == CATALOG_KEY_SHA256
                  and definition_hash == CATALOG_DEFINITION_SHA256
                  and evaluator.POLICY_VERSION == POLICY_VERSION
                  and evaluator.CATALOG_VERSION == BASE_CATALOG_VERSION
                  and questions.VERSION == QUESTION_VERSION)
    return {'compatible': compatible, 'keys': frozenset(keys),
            'candidate_count': len(keys), 'key_sha256': key_hash,
            'definition_sha256': definition_hash}


def expected_formula_version(candidate_key: str, period_key: str) -> str:
    family = (QUESTION_VERSION if candidate_key.startswith(QUESTION_VERSION + ':')
              else BASE_CATALOG_VERSION)
    return ':'.join((POLICY_VERSION, family, PARENT_POLICY, PERIOD_VERSION, period_key))


def eligible_scope(scope: Mapping[str, Any], formula_version: str) -> bool:
    contract = catalog_contract()
    candidate = scope.get('candidate_key')
    period = scope.get('period_key')
    return bool(contract['compatible'] and candidate in contract['keys']
                and scope.get('symbol') == 'ALL'
                and scope.get('direction') in ('LONG', 'SHORT')
                and type(scope.get('window_minutes')) is int
                and scope['window_minutes'] in HORIZONS
                and type(scope.get('threshold_bps')) is int
                and scope['threshold_bps'] in THRESHOLDS_BPS
                and period in PERIODS
                and formula_version == expected_formula_version(candidate, period))


def project_formula_row(row: Mapping[str, Any]) -> dict[str, Any] | None:
    """Preserve all 25 existing values and key serialization, or publish nothing."""
    if set(row) != set(HEADERS):
        return None
    threshold = row.get('threshold_pct')
    # The existing producer uses bps / 100, including 1.0 and 2.0. Preserve
    # its exact durable composite-key spelling rather than minting aliases.
    if type(threshold) is not float:
        return None
    bps = threshold * 100
    if bps not in THRESHOLDS_BPS:
        return None
    version = str(row.get('formula_version') or '')
    period = version.rsplit(':', 1)[-1]
    scope = {'candidate_key': row.get('candidate_key'), 'symbol': row.get('coin_scope'),
             'direction': row.get('direction'), 'window_minutes': row.get('horizon'),
             'threshold_bps': int(bps), 'period_key': period}
    if not eligible_scope(scope, version):
        return None
    return {'sheet': SHEET, 'key': KEY, 'row': dict(row)}


def seed_missing_scope(conn: Any, scope: Mapping[str, Any], formula_version: str) -> int:
    """Seed one naturally revisited unchanged scope via exact outbox PKs only.

    ON CONFLICT DO NOTHING protects a concurrent newer generation. The legacy
    queue's payload, statuses, attempts and acknowledgements remain untouched.
    No cached result is silently marked delivered or assigned a newer timestamp.
    """
    if not eligible_scope(scope, formula_version):
        return 0
    identity = [scope['candidate_key'], 'ALL', scope['direction'],
                scope['threshold_bps'] / 100, scope['window_minutes'], formula_version]
    row_key = _json([str(value) for value in identity])
    if conn.execute('''SELECT 1 AS present FROM research_sheet_upsert_outbox
        WHERE sheet_name=%s AND row_key=%s''', (SHEET, row_key)).fetchone():
        return 0
    old = conn.execute('''SELECT payload,source_time_utc FROM research_sheet_upsert_outbox
        WHERE sheet_name=%s AND row_key=%s''', ('Formula_Results', row_key)).fetchone()
    if not old:
        return 0
    legacy = old['payload']
    if isinstance(legacy, str):
        legacy = json.loads(legacy)
    if (not isinstance(legacy, Mapping) or legacy.get('sheet') != 'Formula_Results'
            or legacy.get('key') != KEY or not isinstance(legacy.get('row'), Mapping)):
        return 0
    item = project_formula_row(legacy['row'])
    if item is None or _json([str(item['row'][name]) for name in KEY.split(',')]) != row_key:
        return 0
    payload = _json(item)
    inserted = conn.execute('''INSERT INTO research_sheet_upsert_outbox
        (sheet_name,row_key,payload,payload_sha256,source_time_utc)
        VALUES(%s,%s,%s::jsonb,%s,%s)
        ON CONFLICT(sheet_name,row_key) DO NOTHING''',
        (SHEET, row_key, payload, hashlib.sha256(payload.encode()).hexdigest(), old['source_time_utc']))
    # Trigger 026 does not know this new tab. Restore its immutable source time
    # through the exact key, and only for the generation just inserted.
    if inserted.rowcount:
        conn.execute('''UPDATE research_sheet_upsert_outbox SET source_time_utc=%s
            WHERE sheet_name=%s AND row_key=%s AND payload_sha256=%s''',
            (old['source_time_utc'], SHEET, row_key, hashlib.sha256(payload.encode()).hexdigest()))
    return inserted.rowcount


def status() -> dict[str, Any]:
    contract = catalog_contract()
    return {'policy_version': VERSION, 'destination': SHEET,
            'state': 'PARTIAL_PUBLICATION' if contract['compatible'] else 'PUBLICATION_CONTRACT_REVIEW_REQUIRED',
            'catalog_compatible': contract['compatible'],
            'candidate_count': contract['candidate_count'],
            'catalog_key_sha256': contract['key_sha256'],
            'max_data_rows': MAX_DATA_ROWS, 'max_grid_cells': MAX_GRID_CELLS,
            'coin_scope': 'ALL', 'horizons_minutes': list(HORIZONS),
            'thresholds_bps': list(THRESHOLDS_BPS), 'periods': list(PERIODS),
            'legacy_destinations': list(LEGACY_SHEETS),
            'legacy_queue_state': 'PRESERVED_NOT_AUTOMATICALLY_PUBLISHED',
            'canonical_research': 'FULL_DATABASE_EVIDENCE_RETAINED',
            'population': 'Natural evaluations and exact-key cached-summary seeding; completeness not asserted',
            'research_effect': 'NONE'}
