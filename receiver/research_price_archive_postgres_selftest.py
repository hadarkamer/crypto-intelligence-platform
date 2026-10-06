"""Real PG archive atomicity, exact interval recovery and restart-safe fairness."""
from datetime import timedelta
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import threading
import unittest
from unittest.mock import patch
from uuid import uuid4

import research_price_archive as a
import research_price_archive_worker as w
from research_price_archive_selftest import START, NOW, bar, path


@unittest.skipUnless(os.getenv('TEST_DATABASE_URL'),'TEST_DATABASE_URL required for real PostgreSQL')
class ArchivePostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg.conninfo import conninfo_to_dict
        from psycopg.rows import dict_row
        dsn=os.environ['TEST_DATABASE_URL']; info=conninfo_to_dict(dsn)
        if info.get('host') not in {'localhost','127.0.0.1','::1','postgres'} or 'test' not in info.get('dbname','').lower():
            raise RuntimeError('Only explicit local/CI PostgreSQL test databases are allowed')
        cls.dsn=dsn
        cls.conn=psycopg.connect(dsn,autocommit=True,row_factory=dict_row)

    @classmethod
    def tearDownClass(cls): cls.conn.close()

    def setUp(self):
        self.schema='price_archive_test_'+uuid4().hex
        self.conn.execute(f'CREATE SCHEMA "{self.schema}"')
        self.conn.execute(f'SET search_path TO "{self.schema}"')
        sql=(Path(__file__).parent/'migrations/044_continuous_price_archive.sql').read_text()
        self.conn.execute(sql); self.conn.execute(sql)

    def tearDown(self):
        self.conn.execute('SET search_path TO public')
        self.conn.execute(f'DROP SCHEMA "{self.schema}" CASCADE')

    def scalar(self,sql): return next(iter(self.conn.execute(sql).fetchone().values()))

    def test_immutable_conflict_rolls_back_whole_batch_and_delete_rejected(self):
        a.write_bars(self.conn,a.BINANCE_SPOT,'BTC',[bar()])
        a.write_bars(self.conn,a.BINANCE_SPOT,'BTC',[bar()])
        changed=bar(); changed['close']=100.5
        with self.assertRaisesRegex(Exception,'frozen source evidence'):
            a.write_bars(self.conn,a.BINANCE_SPOT,'BTC',[bar(1),changed])
        self.assertEqual(self.scalar('SELECT count(*) FROM research_price_archive_bars'),1)
        with self.assertRaisesRegex(Exception,'cannot be deleted'):
            self.conn.execute('DELETE FROM research_price_archive_bars')
        self.assertEqual(self.scalar('SELECT count(*) FROM research_price_archive_bars'),1)

    def test_fetches_only_missing_internal_interval_then_entirely_local(self):
        a.write_bars(self.conn,a.BINANCE_SPOT,'BTC',[bar(0),bar(2)])
        calls=[]
        def fetch(symbol,start,end):
            calls.append((symbol,start,end)); return path(indices=(1,))
        end=START+3*a.MINUTE-a.MILLISECOND
        p=a.get_path(a.BINANCE_SPOT,'BTC',START,end,fetch,connection=self.conn,now=NOW)
        self.assertTrue(p['complete']); self.assertEqual(p['archive_cached_candles'],2)
        self.assertEqual(calls,[('BTC',START+a.MINUTE,START+2*a.MINUTE-a.MILLISECOND)])
        def never(*args): raise AssertionError('No provider fetch allowed for complete archived path')
        p=a.get_path(a.BINANCE_SPOT,'BTC',START,end,never,connection=self.conn,now=NOW)
        self.assertTrue(p['complete']); self.assertEqual(p['archive_cached_candles'],3)
        self.assertTrue(a.read_path(a.BINANCE_SPOT,'BTC',START,end,connection=self.conn)['complete'])

    def test_partial_provider_cannot_forge_coverage(self):
        p=a.get_path(a.BINANCE_SPOT,'BTC',START,START+3*a.MINUTE-a.MILLISECOND,
            lambda *args:path(indices=(0,2)),connection=self.conn,now=NOW)
        self.assertFalse(p['complete']); self.assertEqual(len(p['archive_missing_ranges']),1)
        self.assertEqual(len(p['candles']),2)

    def test_wrong_source_rejected_without_persisting_any_row(self):
        with self.assertRaisesRegex(ValueError,'metadata'):
            a.get_path(a.HYPERLIQUID_PERP,'HYPE',START,START+3*a.MINUTE-a.MILLISECOND,
                lambda *args:path(),connection=self.conn,now=NOW)
        self.assertEqual(self.scalar('SELECT count(*) FROM research_price_archive_bars'),0)

    def test_mark_perp_sources_remain_separate(self):
        for route,price in [(a.BINANCE_MARK,101),(a.HYPERLIQUID_PERP,100.5)]:
            b=bar(volume=None);b['close']=price
            a.write_bars(self.conn,route,'HYPE',[b])
        perp=a.read_path(a.HYPERLIQUID_PERP,'HYPE',START,START+a.MINUTE,connection=self.conn)
        mark=a.read_path(a.BINANCE_MARK,'HYPE',START,START+a.MINUTE,connection=self.conn)
        self.assertEqual(perp['candles'][0]['close'],100.5)
        self.assertEqual(mark['candles'][0]['close'],101)
        self.assertEqual(perp['price_kind'],'TRADE');self.assertEqual(mark['price_kind'],'MARK')

    @staticmethod
    def fetcher(route,calls,fail_history_symbol=None):
        def fetch(symbol,start,end):
            calls.append((symbol,start,end))
            if symbol==fail_history_symbol and start==START:
                raise RuntimeError('Old provider page unavailable')
            first,last,expected=a.bounds(start,end)
            bars=[]
            for i in range(expected):
                b=bar(volume=3 if route==a.BINANCE_SPOT else None)
                b.update(open_time_utc=first+i*a.MINUTE,
                         close_time_utc=first+(i+1)*a.MINUTE-a.MILLISECOND)
                bars.append(b)
            return dict(a.source_metadata(route,symbol),candles=bars,complete=True)
        return fetch

    def test_history_error_does_not_block_other_symbol_or_restart_tail(self):
        routes=((a.BINANCE_SPOT,'BTC'),(a.BINANCE_SPOT,'ETH'))
        calls=[];fetch=self.fetcher(a.BINANCE_SPOT,calls,fail_history_symbol='BTC')
        env={'RESEARCH_PRICE_ARCHIVE_START_UTC':START.isoformat(),
             'RESEARCH_PRICE_ARCHIVE_BACKFILL_PAGES':'2',
             'RESEARCH_DATABASE_URL':'','RESEARCH_USE_PRIMARY_DATABASE':'false'}
        with patch.object(a,'ACTIVE_ROUTES',routes),patch.dict(os.environ,env):
            result=w.ResearchPriceArchiveWorker().run_once(now=NOW,connection=self.conn,
                fetchers={a.BINANCE_SPOT:fetch})
            self.assertEqual([p['lane'] for p in result['pages']],['tail','tail','history','history'])
            states=self.conn.execute('SELECT * FROM research_price_archive_cursors ORDER BY symbol').fetchall()
            self.assertTrue(all(s['history_cursor_utc']==START+1000*a.MINUTE for s in states))
            self.assertEqual(self.scalar("SELECT count(*) FROM research_price_archive_gaps WHERE status='RETRY'"),1)
            calls.clear()
            result=w.ResearchPriceArchiveWorker().run_once(now=NOW+a.MINUTE,connection=self.conn,
                fetchers={a.BINANCE_SPOT:fetch})
            self.assertEqual(result['pages'][0]['lane'],'tail')
            self.assertEqual(calls[0][1],NOW)

    def test_retention_crossing_gap_is_split_and_repaired_without_stuck_original(self):
        route=a.HYPERLIQUID_PERP;floor=w.retention_floor(route,NOW)
        requested=START-timedelta(days=10)
        with patch.object(a,'ACTIVE_ROUTES',((route,'HYPE'),)):
            w._initialize(self.conn,NOW,requested)
            self.conn.execute("""INSERT INTO research_price_archive_gaps
                (route,symbol,start_time_utc,end_time_utc,status,reason,next_retry_utc)
                VALUES (%s,'HYPE',%s,%s,'RETRY','TEST',%s)""",
                (route,floor-a.MINUTE,floor+a.MINUTE-a.MILLISECOND,NOW))
            w._initialize(self.conn,NOW,requested)
            rows=self.conn.execute('SELECT * FROM research_price_archive_gaps ORDER BY start_time_utc').fetchall()
            self.assertEqual([r['status'] for r in rows],['UNAVAILABLE','RETRY'])
            self.assertEqual(rows[1]['start_time_utc'],floor)
            calls=[]
            w._collect_page(self.conn,rows[1],floor,rows[1]['end_time_utc'],
                {route:self.fetcher(route,calls)},NOW,lane='retry')
            self.assertEqual(self.scalar("SELECT count(*) FROM research_price_archive_gaps WHERE status='RETRY'"),0)
            self.assertEqual(self.scalar("SELECT count(*) FROM research_price_archive_gaps WHERE status='UNAVAILABLE'"),1)

    def test_retry_count_survives_recheck_and_readonly_missing_is_explicit(self):
        for _ in range(2):
            w._record_gaps(self.conn,a.BINANCE_SPOT,'BTC',START,START+a.MINUTE-a.MILLISECOND,
                {'archive_missing_ranges':[dict(start_time_utc=START,end_time_utc=START+a.MINUTE-a.MILLISECOND)]},NOW)
        self.assertEqual(self.scalar('SELECT attempts FROM research_price_archive_gaps'),2)
        p=a.read_path(a.BINANCE_SPOT,'BTC',START,START+a.MINUTE,connection=self.conn)
        self.assertFalse(p['complete']);self.assertEqual(p['candles'],[])

    def test_concurrent_revision_has_one_winner_and_loser_batch_rolls_back(self):
        import psycopg
        from psycopg.rows import dict_row
        barrier=threading.Barrier(2)
        def attempt(index,close):
            with psycopg.connect(self.dsn,autocommit=True,row_factory=dict_row,
                    options='-c statement_timeout=10000 -c lock_timeout=5000') as conn:
                conn.execute(f'SET search_path TO "{self.schema}"')
                shared=bar();shared['close']=close
                barrier.wait(timeout=5)
                try:
                    a.write_bars(conn,a.BINANCE_SPOT,'BTC',[bar(index),shared])
                    return 'winner'
                except psycopg.errors.RaiseException:
                    return 'conflict'
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures=[pool.submit(attempt,1,100.5),pool.submit(attempt,2,101)]
            results=[future.result(timeout=15) for future in futures]
        self.assertCountEqual(results,['winner','conflict'])
        self.assertEqual(self.scalar('SELECT count(*) FROM research_price_archive_bars'),2)


if __name__=='__main__': unittest.main()
