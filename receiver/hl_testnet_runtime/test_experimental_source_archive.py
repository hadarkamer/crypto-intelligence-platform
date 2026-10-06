"""Real source-schema reads and explicit failure separation from MARK evidence."""
from copy import deepcopy
from datetime import datetime, timezone
import os
import hashlib
from pathlib import Path
import unittest
from unittest.mock import patch

import experimental_execution_contract as contract
from experimental_execution_fixtures import r2732_message, sol_g65_message
from . import experimental_price_evidence as evidence
from . import experimental_source_archive as archive
from . import r2732_conditional_stop as r2732
from . import sol_g65_conditional_stop as g65
from .test_experimental_price_evidence import T, A
from .test_maxpain_execution import plan as maxpain_message

CI = os.environ.get('HL_JOURNAL_CI_URL')
FIXTURE_DSN = 'postgresql://fixture:unused@127.0.0.1:5432/hl_journal_ci'


def moment(at):
    return datetime.fromtimestamp(at / 1000, timezone.utc)


def record(at=T, symbol='XRP', price=2.3, created_at=None):
    route = 'HYPERLIQUID_HYPE_PERP_TRADE_1M' if symbol == 'HYPE' else 'HYPERLIQUID_PERP_TRADE_1M'
    return (route, symbol, moment(at), moment(at + 59999),
        price, price, price, price, None, moment(created_at or at + 60000))


class Connection:
    def __init__(self, rows, *, immutable=True):
        self.rows, self.immutable, self.queries = rows, immutable, []
    def __enter__(self): return self
    def __exit__(self, *_): return False
    def execute(self, query, params=None):
        self.queries.append((query, params))
        return self
    def fetchall(self): return deepcopy(self.rows)
    def fetchone(self): return (self.immutable,)


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.msg = r2732_message(entry=2.3, decision_ms=T-60000)
        self.now = T+60000
        self.reader = archive.ReadOnlySourceArchive.for_ci(FIXTURE_DSN)
        self.conn = Connection([record()])
        guard = patch.object(self.reader, '_connect', return_value=self.conn)
        guard.start(); self.addCleanup(guard.stop)
        self.provider = evidence.ArchivedPriceEvidence(self.reader)

    def test_reads_existing_source_route_and_original_observation_stamp(self):
        raw = self.reader.read(self.msg, now_ms=self.now+12000)
        self.assertEqual(raw['observed_at_ms'], self.now)
        value = self.provider.source_range(self.msg, self.now+12000)
        self.assertEqual(value['at_ms'], T+59999)
        query, params = next(x for x in self.conn.queries if x[0] == archive.SQL)
        self.assertEqual(params[:4], ('HYPERLIQUID_PERP_TRADE_1M', 'XRP', moment(T), moment(T+60000)))
        self.assertEqual(params[4], 5001)
        self.assertIn('public.research_price_archive_bars', query)
        self.assertTrue(all(q.startswith(('SELECT', 'SET TRANSACTION READ ONLY')) for q, _ in self.conn.queries))

    def test_archive_query_time_cannot_freshen_source_price(self):
        with self.assertRaisesRegex(evidence.PriceEvidenceError, 'STALE'):
            self.provider.source_range(self.msg, self.now+20000)

    def test_current_unclosed_bar_cannot_enter_archive_evidence(self):
        self.conn.rows = [record(at=T+60000)]
        with self.assertRaisesRegex(evidence.PriceEvidenceError, 'PROVENANCE'):
            self.provider.closed_bars(self.msg, self.now)

    def test_missing_middle_minute_is_not_forward_filled(self):
        self.conn.rows = [record(), record(T+120000)]
        with self.assertRaisesRegex(evidence.PriceEvidenceError, 'GAP'):
            self.provider.closed_bars(self.msg, T+180000)

    def test_missing_first_minute_is_not_hidden_by_fresh_tail(self):
        self.conn.rows = [record(T+60000)]
        with self.assertRaisesRegex(evidence.PriceEvidenceError, 'GAP'):
            self.provider.source_range(self.msg, T+120000)

    def test_wrong_route_symbol_volume_or_created_before_close_rejected(self):
        for field, value in ((0, 'BINANCE_SPOT_TRADE_1M'), (1, 'HYPE'),
                (8, 1), (9, moment(T)), (9, moment(T+60001)),
                (9, datetime(2026,1,1)), (3, moment(T+60000))):
            with self.subTest(field=field, value=str(value)):
                row = list(record()); row[field] = value
                self.conn.rows = [row]
                with self.assertRaises(evidence.PriceEvidenceError):
                    self.provider.closed_bars(self.msg, self.now)

    def test_conflicting_closed_duplicate_is_not_accepted(self):
        self.conn.rows = [record(), record(price=2.4)]
        with self.assertRaisesRegex(evidence.PriceEvidenceError, 'CONFLICTING'):
            self.provider.closed_bars(self.msg, self.now)

    def test_missing_disabled_or_wrong_immutable_trigger_fails_closed(self):
        self.conn.immutable = False
        with self.assertRaisesRegex(evidence.PriceEvidenceError, 'READ_UNAVAILABLE'):
            self.provider.closed_bars(self.msg, self.now)
        self.assertFalse(any(q == archive.SQL for q, _ in self.conn.queries))

    def test_immutable_function_fingerprint_is_exact_canonical_schema_body(self):
        path = Path(__file__).resolve().parents[1] / 'migrations' / '044_continuous_price_archive.sql'
        body = path.read_text().split('RETURNS TRIGGER LANGUAGE plpgsql AS $$', 1)[1].split('$$;', 1)[0]
        self.assertEqual(hashlib.sha256(body.encode()).hexdigest(), archive.IMMUTABLE_FUNCTION_SHA256)
        self.provider.closed_bars(self.msg, self.now)
        params = next(p for q,p in self.conn.queries if q == archive.IMMUTABILITY_SQL)
        self.assertEqual(params, (archive.IMMUTABLE_FUNCTION_SHA256,))
        for clause in ('t.tgqual IS NULL', "t.tgattr=''::int2vector", 't.tgnargs=0'):
            self.assertIn(clause, archive.IMMUTABILITY_SQL)

    def test_missing_source_config_does_not_borrow_execution_dsn(self):
        with patch.object(archive.ReadOnlySourceArchive, '_connect', side_effect=AssertionError('NO_IO')):
            provider = evidence.build_price_evidence({'DATABASE_URL': 'private', 'HL_TESTNET_JOURNAL_URL': 'private'})
            self.assertFalse(provider.diagnosis()['source_archive_configured'])
            with self.assertRaisesRegex(evidence.PriceEvidenceError, 'NOT_CONFIGURED'):
                provider.closed_bars(self.msg, self.now)

    def test_explicit_factory_constructs_without_network_or_secret_disclosure(self):
        dsn = 'postgresql://reader:private@archive.example/source?sslmode=verify-full'
        with patch.object(archive.ReadOnlySourceArchive, '_connect', side_effect=AssertionError('NO_IO')):
            provider = evidence.build_price_evidence({'HL_TESTNET_EXPERIMENTAL_SOURCE_ARCHIVE_URL': dsn})
        self.assertTrue(provider.diagnosis()['source_archive_configured'])
        self.assertNotIn('private', str(provider.diagnosis()))
        self.assertFalse(provider.diagnosis()['history_complete'])

    def test_url_options_loopback_and_plaintext_cannot_override_prod_constraints(self):
        for dsn in (FIXTURE_DSN, 'postgresql://r:s@host/db?sslmode=disable',
                'postgresql://r:s@host/db?options=-c+default_transaction_read_only%3Doff',
                'postgresql://r:s@host/db?sslmode=require&sslmode=disable',
                'postgresql://r:s@host1,host2/db', 'dbname=source password=PRIVATE',
                'postgresql://r:s@host/db#fragment'):
            with self.subTest(dsn=dsn), self.assertRaisesRegex(evidence.PriceEvidenceError, 'URL_REQUIRED'):
                archive.ReadOnlySourceArchive(dsn)

    def test_invalid_source_setting_keeps_protection_composition_available(self):
        with patch.object(archive.ReadOnlySourceArchive, '_connect', side_effect=AssertionError('NO_IO')):
            provider = evidence.build_price_evidence({'HL_TESTNET_EXPERIMENTAL_SOURCE_ARCHIVE_URL': 'PRIVATE'})
        self.assertFalse(provider.diagnosis()['source_archive_configured'])
        self.assertEqual(provider.diagnosis()['source_archive_error'], 'SOURCE_ARCHIVE_CONFIGURATION_INVALID')
        self.assertNotIn('PRIVATE', str(provider.diagnosis()))
        with self.assertRaisesRegex(evidence.PriceEvidenceError, 'NOT_CONFIGURED'):
            provider.closed_bars(self.msg, self.now)

    def test_database_failures_are_fixed_domain_errors(self):
        for failure in (TimeoutError('PRIVATE HOST'), OSError('PRIVATE PASSWORD'), RuntimeError('PRIVATE QUERY')):
            with patch.object(self.reader, '_connect', side_effect=failure):
                with self.assertRaisesRegex(evidence.PriceEvidenceError, '^SOURCE_ARCHIVE_READ_UNAVAILABLE$'):
                    self.provider.closed_bars(self.msg, self.now)

    def test_intraminute_maxpain_cannot_use_containing_minute(self):
        msg = maxpain_message()
        ref = contract.moment_ms(msg['source_at'])+1
        msg['source_at'] = contract.iso_ms(ref)
        msg['proof']['source_observed_ms'] = ref
        msg['occurrence_id'] = contract.occurrence_id(msg)
        with self.assertRaisesRegex(evidence.PriceEvidenceError, 'NOT_RECOVERABLE'):
            self.provider.source_range(msg, ref+60000)
        self.assertEqual(self.conn.queries, [])

    def test_readonly_archive_never_claims_mark_continuity(self):
        self.provider.closed_bars(self.msg, self.now)
        self.conn.queries.clear()
        with self.assertRaisesRegex(evidence.PriceEvidenceError, evidence.MARK_HISTORY_UNAVAILABLE):
            self.provider.mark_window(A, 'XRP', T, self.now)
        self.assertEqual(self.conn.queries, [])

    def test_cursor_includes_last_committed_bar_and_bounds_recovery_page(self):
        self.conn.rows = [record(T+60000), record(T+120000)]
        result = self.provider.closed_bars_since(self.msg, T+60001*60000, T+60000)
        self.assertEqual([r['open_at_ms'] for r in result], [T+60000, T+120000])
        params = next(p for q,p in self.conn.queries if q == archive.SQL)
        self.assertEqual(params[2], moment(T+60000))
        self.assertEqual(params[3], moment(T+60000+5000*60000))

    def test_new_r2732_cursor_none_and_initial_cursor_have_same_closed_bars(self):
        self.assertEqual(self.provider.closed_bars_since(self.msg, self.now, None),
            self.provider.closed_bars_since(self.msg, self.now, T-60000))
        state = r2732.initialize_from_contract(self.msg['occurrence_id'], self.msg)
        state = r2732.advance(state, self.provider.closed_bars(self.msg, self.now), now_ms=self.now)
        self.assertEqual(state['cursor_ms'], T)

    def test_g65_archive_bars_reach_unchanged_condition_logic(self):
        msg = sol_g65_message()
        at = contract.moment_ms(msg['source_at'])
        self.conn.rows = [record(at, 'SOL', float(msg['proof']['reference_price']))]
        rows = self.provider.closed_bars(msg, at+60000)
        state = g65.initialize_from_contract(msg['occurrence_id'], msg)
        self.assertEqual(g65.advance(state, rows, now_ms=at+60000)['cursor_ms'], at)

    def test_archive_does_not_query_for_yet_unclosed_reference_minute(self):
        self.assertEqual(self.provider.closed_bars(self.msg, T+10000), [])
        self.assertEqual(self.conn.queries, [])


@unittest.skipUnless(CI, 'Requires disposable loopback PostgreSQL source archive')
class ArchivePostgresTests(unittest.TestCase):
    def setUp(self):
        from .postgres_journal import PostgresJournal
        archive.ReadOnlySourceArchive.for_ci(CI)  # CI identity checked before I/O.
        self.conn = PostgresJournal.for_ci(CI)._connect(autocommit=True)
        self.addCleanup(self.conn.close)
        self.conn.execute('DROP TABLE IF EXISTS public.research_price_archive_bars CASCADE')
        migration = Path(__file__).resolve().parents[1] / 'migrations' / '044_continuous_price_archive.sql'
        self.conn.execute(migration.read_text())
        # This matches source migration 056's route expansion; no reader DDL.
        for name in ('research_price_archive_bars_route_check', 'research_price_archive_bars_check'):
            self.conn.execute('ALTER TABLE public.research_price_archive_bars DROP CONSTRAINT IF EXISTS '+name)
        self.msg = r2732_message(entry=2.3, decision_ms=T-60000)
        self.reader = archive.ReadOnlySourceArchive.for_ci(CI)
        self.provider = evidence.ArchivedPriceEvidence(self.reader)
        self.insert(record())

    def insert(self, row):
        self.conn.execute('''INSERT INTO public.research_price_archive_bars
            (route,symbol,open_time_utc,close_time_utc,open,high,low,close,volume,created_at_utc)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', row)

    def test_source_readback_and_new_provider_preserve_original_timestamps(self):
        a = self.provider.source_range(self.msg, T+60000)
        b = evidence.ArchivedPriceEvidence(archive.ReadOnlySourceArchive.for_ci(CI)).source_range(self.msg, T+61000)
        self.assertEqual(a, b)
        self.assertEqual(a['at_ms'], T+59999)

    def test_actual_reader_transaction_rejects_write(self):
        import psycopg
        with self.reader._connect() as conn:
            self.assertEqual(conn.execute('SHOW transaction_read_only').fetchone(), ('on',))
            with self.assertRaises(psycopg.errors.ReadOnlySqlTransaction):
                conn.execute('DELETE FROM public.research_price_archive_bars')
            conn.rollback()
        self.assertEqual(self.conn.execute('SELECT count(*) FROM public.research_price_archive_bars').fetchone(), (1,))

    def test_source_immutable_trigger_rejects_revision(self):
        import psycopg
        with self.assertRaises(psycopg.errors.RaiseException):
            self.conn.execute('UPDATE public.research_price_archive_bars SET close=2.4,high=2.4')
        self.assertEqual(self.provider.source_range(self.msg, T+60000)['price'], '2.3')

    def test_missing_immutable_trigger_cannot_be_treated_as_verified_cache(self):
        self.conn.execute('ALTER TABLE public.research_price_archive_bars DISABLE TRIGGER research_price_archive_immutable_v1')
        with self.assertRaisesRegex(evidence.PriceEvidenceError, 'READ_UNAVAILABLE'):
            self.provider.closed_bars(self.msg, T+60000)

    def test_same_name_trigger_with_false_condition_does_not_prove_immutability(self):
        self.conn.execute('DROP TRIGGER research_price_archive_immutable_v1 ON public.research_price_archive_bars')
        self.conn.execute('''CREATE TRIGGER research_price_archive_immutable_v1
            BEFORE UPDATE OR DELETE ON public.research_price_archive_bars FOR EACH ROW
            WHEN (false) EXECUTE FUNCTION public.research_price_archive_immutable_v1()''')
        with self.assertRaisesRegex(evidence.PriceEvidenceError, 'READ_UNAVAILABLE'):
            self.provider.closed_bars(self.msg, T+60000)

    def test_same_name_trigger_with_replaced_function_does_not_prove_immutability(self):
        self.conn.execute('''CREATE OR REPLACE FUNCTION public.research_price_archive_immutable_v1()
            RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$''')
        with self.assertRaisesRegex(evidence.PriceEvidenceError, 'READ_UNAVAILABLE'):
            self.provider.closed_bars(self.msg, T+60000)

    def test_source_gap_blocks_entry_and_conditional_advance(self):
        self.insert(record(T+120000))
        for method in (self.provider.source_range, self.provider.closed_bars):
            with self.assertRaisesRegex(evidence.PriceEvidenceError, 'GAP'):
                method(self.msg, T+180000)

    def test_distinct_wrong_market_rows_do_not_satisfy_source_query(self):
        wrong = list(record(T+60000)); wrong[0] = 'BINANCE_SPOT_TRADE_1M'; wrong[8] = 1
        self.insert(wrong)
        with self.assertRaisesRegex(evidence.PriceEvidenceError, 'STALE'):
            self.provider.source_range(self.msg, T+120000)
        self.assertEqual(len(self.provider.closed_bars(self.msg, T+120000)), 1)


if __name__ == '__main__':
    unittest.main()
