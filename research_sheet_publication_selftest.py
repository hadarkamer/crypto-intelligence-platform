"""Finite population, exact-key lazy seeding and preserved legacy evidence."""
from __future__ import annotations

from itertools import product
import json
from unittest.mock import patch

import research_sheet_publication as publication
import research_sheet_outbox as outbox
from research_sheet_fresh_delivery_selftest import _database, _add, Database, NOW


def scope_and_row():
    scope = {'candidate_key': 'STRICT_TRIPLE_TOTAL_65', 'symbol': 'ALL',
             'direction': 'LONG', 'window_minutes': 60, 'threshold_bps': 25,
             'period_key': 'SINCE_20260904'}
    version = publication.expected_formula_version(scope['candidate_key'], scope['period_key'])
    row = dict.fromkeys(publication.HEADERS, '')
    row.update(candidate_key=scope['candidate_key'], coin_scope='ALL', direction='LONG',
               threshold_pct=0.25, horizon=60, formula_version=version,
               last_evaluated_at='2026-09-13T08:00:00+00:00',
               status='INSUFFICIENT_INDEPENDENT_EVIDENCE', successes=2, failures=1)
    return scope, version, row


def contract_test():
    contract = publication.catalog_contract()
    assert contract['compatible'] and len(contract['keys']) == 298
    assert len(publication.HEADERS) == len(set(publication.HEADERS)) == 25
    assert publication.MAX_GRID_CELLS == 953625
    identities = set()
    for candidate, direction, horizon, threshold, period in product(
            contract['keys'], ('LONG', 'SHORT'), publication.HORIZONS,
            publication.THRESHOLDS_BPS, publication.PERIODS):
        version = publication.expected_formula_version(candidate, period)
        scope = {'candidate_key': candidate, 'symbol': 'ALL', 'direction': direction,
                 'window_minutes': horizon, 'threshold_bps': threshold, 'period_key': period}
        assert publication.eligible_scope(scope, version)
        identities.add((candidate, 'ALL', direction, threshold, horizon, version))
    assert len(identities) == publication.MAX_DATA_ROWS
    scope, version, row = scope_and_row()
    projected = publication.project_formula_row(row)
    assert projected == {'sheet': publication.SHEET, 'key': publication.KEY, 'row': row}
    assert projected['row'] is not row
    for key, value in (('symbol', 'BTC'), ('candidate_key', 'new-candidate'),
                       ('window_minutes', 30), ('threshold_bps', 65),
                       ('period_key', 'LEGACY_UNSCOPED')):
        assert not publication.eligible_scope({**scope, key: value}, version)
    assert not publication.eligible_scope(scope, version + ':new')
    wrong_family = version.replace(publication.BASE_CATALOG_VERSION, publication.QUESTION_VERSION)
    assert not publication.eligible_scope(scope, wrong_family)
    for malformed in ({**row, 'threshold_pct': True}, {**row, 'threshold_pct': 1},
                      {**row, 'extra': 'unbudgeted'},
                      {key: value for key, value in row.items() if key != 'chat_summary'}):
        assert publication.project_formula_row(malformed) is None
    original = publication.evaluator.candidate_catalog(include_extended=True)
    for changed in (original[:-1], [dict(item, conditions=[]) if index == 0 else item
                                   for index, item in enumerate(original)]):
        with patch.object(publication.evaluator, 'candidate_catalog', lambda **kwargs: changed):
            publication.catalog_contract.cache_clear()
            assert not publication.catalog_contract()['compatible']
            assert publication.project_formula_row(row) is None
            assert publication.status()['state'] == 'PUBLICATION_CONTRACT_REVIEW_REQUIRED'
        publication.catalog_contract.cache_clear()
    assert publication.status()['state'] == 'PARTIAL_PUBLICATION'


class SeedDatabase(Database):
    def __init__(self, conn):
        super().__init__(conn)
        self.race = None

    def execute(self, query, params=()):
        if 'INSERT INTO research_sheet_upsert_outbox' in query and self.race:
            race, self.race = self.race, None
            race()
        return super().execute(query.replace('::jsonb', ''), params)


def seed_test():
    scope, version, row = scope_and_row()
    key = publication._json([str(row[name]) for name in publication.KEY.split(',')])
    payload = {'sheet': 'Formula_Results', 'key': publication.KEY, 'row': row}
    database = SeedDatabase(_database().conn)
    _add(database, 'Formula_Results', key, NOW, payload=json.dumps(payload),
         sync_status='RETRY', attempts=7, last_error='old receipt missing')
    legacy = tuple(database.conn.execute('SELECT * FROM research_sheet_upsert_outbox').fetchone())
    assert publication.seed_missing_scope(database, scope, version) == 1
    current = database.conn.execute("SELECT * FROM research_sheet_upsert_outbox WHERE sheet_name=?",
                                    (publication.SHEET,)).fetchone()
    assert current['sync_status'] == 'PENDING' and current['attempts'] == 0
    assert current['source_time_utc'] == NOW and current['synced_at_utc'] is None
    assert json.loads(current['payload'])['row'] == row
    assert tuple(database.conn.execute("SELECT * FROM research_sheet_upsert_outbox WHERE sheet_name='Formula_Results'").fetchone()) == legacy
    assert all('WHERE sheet_name=%s AND row_key=%s' in query
               for query, params in database.calls if query.lstrip().startswith('SELECT'))
    before = tuple(current)
    database.calls.clear()
    assert publication.seed_missing_scope(database, scope, version) == 0
    assert len(database.calls) == 1, 'Existing current summary needs just one primary-key lookup'
    assert tuple(database.conn.execute("SELECT * FROM research_sheet_upsert_outbox WHERE sheet_name=?",
                                      (publication.SHEET,)).fetchone()) == before
    # A concurrent newer row after the initial missing check is not overwritten.
    race = SeedDatabase(_database().conn)
    _add(race, 'Formula_Results', key, NOW, payload=json.dumps(payload))
    race.race = lambda: _add(race, publication.SHEET, key, NOW+1,
                             payload='{"newer":true}', payload_sha256='newer-generation')
    assert publication.seed_missing_scope(race, scope, version) == 0
    assert race.conn.execute('SELECT payload_sha256 FROM research_sheet_upsert_outbox WHERE sheet_name=?',
                             (publication.SHEET,)).fetchone()[0] == 'newer-generation'
    missing = SeedDatabase(_database().conn)
    assert publication.seed_missing_scope(missing, scope, version) == 0
    assert len(missing.calls) == 2
    missing.calls.clear()
    assert publication.seed_missing_scope(missing, {**scope, 'symbol': 'BTC'}, version) == 0
    assert missing.calls == []


def legacy_lane_test():
    database = _database()
    for sheet, state in product(publication.LEGACY_SHEETS, ('PENDING', 'RETRY', 'IN_FLIGHT', 'SYNCED')):
        _add(database, sheet, state, NOW, sync_status=state,
             lease_expires_at_utc=NOW-1, attempts=5)
    _add(database, 'Unapproved_Destination', 'unknown', NOW)
    before = [tuple(row) for row in database.conn.execute('SELECT * FROM research_sheet_upsert_outbox')]
    assert outbox._claim_batch(database, 8, 'none') == []
    after = [tuple(row) for row in database.conn.execute('SELECT * FROM research_sheet_upsert_outbox')]
    assert before == after, 'Held evidence must keep original queue states and attempts'
    # An empty preferred lane still finds live work through exact indexed lanes.
    _add(database, 'Snapshots', 'new-live', NOW)
    rows = outbox._claim_batch(database, 8, 'live')
    assert [row['row_key'] for row in rows] == ['new-live']
    queries = [(query, params) for query, params in database.calls if 'WITH due AS (' in query]
    assert queries and all('AND sheet_name=%s' in query and params[0] is not None
                           and params[0] not in publication.LEGACY_SHEETS for query, params in queries)
    with patch.object(publication, 'catalog_contract', lambda: {'compatible': False}):
        assert outbox._claim_lane(database, count=1, token='held', sheet=publication.SHEET, recent=False) == []


def run():
    contract_test()
    seed_test()
    legacy_lane_test()
    print('finite Sheet publication: population cap, frozen contract, exact-key seed, concurrency, retained legacy lanes PASS')


if __name__ == '__main__':
    run()
