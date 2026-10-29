"""Focused source, exact-gap and durable-worker scheduling contracts."""
from datetime import datetime, timedelta, timezone
from contextlib import nullcontext
import os
import unittest
from unittest.mock import patch

import research_price_archive as a
import research_price_archive_worker as w

START = datetime(2026,8,20,12,tzinfo=timezone.utc)
NOW = START+timedelta(days=1)


def bar(i=0, *, volume=3.0):
    opened = START+i*a.MINUTE
    return dict(open_time_utc=opened,close_time_utc=opened+a.MINUTE-a.MILLISECOND,
                open=100.0,high=102.0,low=99.0,close=101.0,volume=volume)


def path(route=a.BINANCE_SPOT,symbol='BTC',indices=(0,1,2)):
    volume = 3.0 if route in (a.BINANCE_SPOT,a.HYPERLIQUID_SPOT) else None
    return dict(a.source_metadata(route,symbol),candles=[bar(i,volume=volume) for i in indices],complete=True)


class PriceArchiveTests(unittest.TestCase):
    def test_complete_archive_reads_once_without_mutating_stored_rows(self):
        rows = [bar(i) for i in range(3)]
        for route in (a.BINANCE_SPOT, a.HYPERLIQUID_PERP_GENERIC):
            with self.subTest(route=route), \
                 patch.object(a, 'read_bars', return_value=rows) as read, \
                 patch.object(a, 'write_bars') as write:
                result = a.get_path(route, 'BTC', START, START+3*a.MINUTE,
                    lambda *args: self.fail('Complete archive must not fetch'), connection=object())
            read.assert_called_once()
            write.assert_not_called()
            self.assertTrue(result['complete'])
            self.assertEqual(len(result['candles']), 3)
            self.assertEqual(rows, [bar(i) for i in range(3)])

    def test_missing_archive_page_is_persisted_and_reread(self):
        rows = [bar(i) for i in range(3)]
        with patch.object(a, 'read_bars', side_effect=[[rows[0], rows[2]], rows]) as read, \
             patch.object(a, 'write_bars') as write:
            result = a.get_path(a.BINANCE_SPOT, 'BTC', START, START+3*a.MINUTE,
                lambda *args: path(indices=(1,)), connection=object(), now=NOW)
        self.assertEqual(read.call_count, 2)
        write.assert_called_once()
        self.assertTrue(result['complete'])
        self.assertEqual(result['archive_cached_candles'], 2)
        self.assertEqual(len(result['candles']), 3)

    def test_hyperliquid_routes_preserve_hype_and_separate_new_symbols(self):
        self.assertEqual(a.hyperliquid_route('HYPE'),a.HYPERLIQUID_PERP)
        self.assertEqual(len(a.ACTIVE_ROUTES),13)
        for symbol in a.HYPERLIQUID_PERP_SYMBOLS:
            self.assertEqual(a.hyperliquid_route(symbol),a.HYPERLIQUID_PERP_GENERIC)
            meta=a.source_metadata(a.HYPERLIQUID_PERP_GENERIC,symbol)
            self.assertEqual((meta['pair'],meta['market'],meta['price_kind']),
                             (symbol+'-PERP','perpetual','TRADE'))
            self.assertEqual(w.retention_floor(a.HYPERLIQUID_PERP_GENERIC,NOW),NOW-4999*a.MINUTE)
            p=path(a.HYPERLIQUID_PERP_GENERIC,symbol,indices=(0,))
            self.assertEqual(len(a.validate_path(a.HYPERLIQUID_PERP_GENERIC,symbol,p,
                START,START+a.MINUTE,now=NOW)),1)
        for symbol in ('HYPE','BNB','ZEC'):
            with self.assertRaises(ValueError):
                a.source_metadata(a.HYPERLIQUID_PERP_GENERIC,symbol)

    def test_shared_hyperliquid_closed_adapter_has_exact_inclusive_cutoff(self):
        start=int(START.timestamp()*1000)
        with patch('experimental_hyperliquid_source.fetch_rows',
                   return_value=[[start,100,102,99,101]]) as fetch:
            result=a.fetch_hyperliquid_closed_candles('ETH',START,START+a.MINUTE-a.MILLISECOND)
        fetch.assert_called_once_with('ETH',start,start+60000)
        self.assertTrue(result['complete'])
        self.assertEqual(result['candles'][0]['close_time_utc'],START+a.MINUTE-a.MILLISECOND)
        self.assertEqual(result['market'],'perpetual')
        self.assertEqual(result['price_kind'],'TRADE')

    def test_experimental_archive_callbacks_disabled_and_closed_only(self):
        start=int(START.timestamp()*1000)
        with patch.dict(os.environ,{'RESEARCH_PRICE_ARCHIVE_ENABLED':'false'}):
            self.assertEqual(a.experimental_hyperliquid_read('ETH',start,start+60000),[])
            a.experimental_hyperliquid_write('ETH',[[start,100,102,99,101]])
            future=int((datetime.now(timezone.utc)+timedelta(days=1)).timestamp()//60*60000)
            with self.assertRaisesRegex(ValueError,'not yet closed'):
                a.experimental_hyperliquid_write('ETH',[[future,100,102,99,101]])

    def test_precise_partial_boundary_and_full_minute(self):
        self.assertEqual(a.bounds(START+timedelta(seconds=1),START+2*a.MINUTE-a.MILLISECOND),
                         (START+a.MINUTE,START+a.MINUTE,1))
        self.assertEqual(a.bounds(START,START+a.MINUTE-a.MILLISECOND)[2],1)
        self.assertEqual(a.bounds(START,START+a.MINUTE-timedelta(microseconds=1001))[2],0)
        self.assertEqual(a.bounds(START,START+a.MINUTE)[2],1)
        with self.assertRaises(ValueError):
            a.bounds(START.replace(tzinfo=None),NOW)

    def test_exact_internal_missing_minutes_not_just_count(self):
        gaps=a.missing_ranges([START,START+2*a.MINUTE,START+2*a.MINUTE],START,START+3*a.MINUTE)
        self.assertEqual(gaps,[(START+a.MINUTE,START+2*a.MINUTE-a.MILLISECOND),
                              (START+3*a.MINUTE,START+4*a.MINUTE-a.MILLISECOND)])
        self.assertEqual(len(a.missing_ranges([],START,START+2500*a.MINUTE)),3)
        self.assertTrue(all((end-start)<1000*a.MINUTE for start,end in
                            a.missing_ranges([],START,START+2500*a.MINUTE)))

    def test_finite_consistent_closed_bar(self):
        for key,value in [('high',float('nan')),('low',float('-inf')),('close',103),('volume',-1)]:
            broken=bar(); broken[key]=value
            with self.assertRaises(ValueError):
                a.normalize_bar(broken,now=NOW)
        broken=bar(); broken['open_time_utc']+=timedelta(seconds=1)
        with self.assertRaises(ValueError):
            a.normalize_bar(broken,now=NOW)
        with self.assertRaises(ValueError):
            a.normalize_bar(bar(),now=START)

    def test_cross_source_never_substituted(self):
        for route in (a.HYPERLIQUID_PERP,a.BINANCE_MARK,a.HYPERLIQUID_SPOT):
            with self.assertRaisesRegex(ValueError,'metadata'):
                a.validate_path(route,'HYPE',path(),START,START+3*a.MINUTE,now=NOW)
        p=path(a.HYPERLIQUID_PERP,'HYPE');p['price_kind']='MARK'
        with self.assertRaisesRegex(ValueError,'metadata'):
            a.validate_path(a.HYPERLIQUID_PERP,'HYPE',p,START,START+3*a.MINUTE,now=NOW)

    def test_duplicate_revision_and_out_of_window_rejected(self):
        p=path(indices=(0,0,1))
        self.assertEqual(len(a.validate_path(a.BINANCE_SPOT,'BTC',p,START,START+3*a.MINUTE,now=NOW)),2)
        p['candles'][1]['close']=100.5
        with self.assertRaisesRegex(ValueError,'Conflicting duplicate'):
            a.validate_path(a.BINANCE_SPOT,'BTC',p,START,START+3*a.MINUTE,now=NOW)
        with self.assertRaisesRegex(ValueError,'outside'):
            a.validate_path(a.BINANCE_SPOT,'BTC',path(indices=(-1,)),START,START+3*a.MINUTE,now=NOW)

    def test_disabled_read_through_preserves_provider_test_injection(self):
        calls=[]
        def fetch(*args): calls.append(args); return {'original':True}
        with patch.dict(os.environ,{'RESEARCH_PRICE_ARCHIVE_ENABLED':'false'}):
            self.assertEqual(a.get_path('unsupported','X',START,NOW,fetch),{'original':True})
        self.assertEqual(calls,[('X',START,NOW)])

    def test_database_primary_requires_explicit_existing_consent(self):
        with patch.dict(os.environ,{'RESEARCH_DATABASE_URL':'','DATABASE_URL':'primary',
                                   'RESEARCH_USE_PRIMARY_DATABASE':'false'}):
            self.assertEqual(a.database_url(),'')
        with patch.dict(os.environ,{'RESEARCH_DATABASE_URL':'dedicated','DATABASE_URL':'primary'}):
            self.assertEqual(a.database_url(),'dedicated')

    def test_unarchived_spot_symbols_and_scaled_alias_keep_original_provider(self):
        calls=[]
        def fetch(*args): calls.append(args); return {'original':True}
        with patch.dict(os.environ,{'RESEARCH_PRICE_ARCHIVE_ENABLED':'true'}):
            for symbol in ('AAVE','1000PEPE',' ada '):
                self.assertEqual(a.get_path(a.BINANCE_SPOT,symbol,START,NOW,fetch,
                    database_url='unused'),{'original':True})
        self.assertEqual([c[0] for c in calls],['AAVE','1000PEPE','ADA'])

    def test_tail_remains_current_after_long_gap_and_restart(self):
        cutoff=NOW-a.MILLISECOND
        start,end=w.tail_window(START-timedelta(days=5),cutoff)
        self.assertEqual(end,cutoff)
        self.assertEqual(a.bounds(start,end)[2],1000)
        state=dict(route=a.BINANCE_SPOT,history_cursor_utc=START)
        first=w.history_window(state,cutoff,NOW)
        state['history_cursor_utc']=first[1]+a.MILLISECOND
        # The next process consumes the committed cursor, not process memory.
        self.assertEqual(w.history_window(dict(state),cutoff,NOW)[0],START+1000*a.MINUTE)

    def test_tail_latest_after_fixed_cutoff_keeps_only_final_closed_minute(self):
        cutoff=START+a.MINUTE-a.MILLISECOND
        for latest in (START+a.MINUTE,START+2*a.MINUTE,NOW):
            with self.subTest(latest=latest):
                start,end=w.tail_window(latest,cutoff)
                self.assertEqual((start,end),(START,cutoff))
                self.assertEqual(a.bounds(start,end),(START,START,1))
                self.assertLessEqual(start+a.MINUTE-a.MILLISECOND,cutoff)

    def test_tail_cutoff_alignment_uses_inclusive_closed_candle_contract(self):
        for cutoff,eligible in (
            (START+a.MINUTE-a.MILLISECOND,START),
            (START+a.MINUTE,START),
            (START+a.MINUTE-a.MILLISECOND-timedelta(microseconds=1),START-a.MINUTE),
            (START+a.MINUTE+timedelta(seconds=30),START),
        ):
            with self.subTest(cutoff=cutoff):
                # A direct min(latest, cutoff) would leave start == end.
                start,end=w.tail_window(cutoff,cutoff)
                self.assertEqual((start,end),(eligible,cutoff))
                self.assertEqual(a.bounds(start,end),(eligible,eligible,1))
                self.assertLessEqual(eligible+a.MINUTE-a.MILLISECOND,cutoff)

    def test_tail_normal_overlap_empty_cache_and_page_budget_are_preserved(self):
        cutoff=START+10*a.MINUTE-a.MILLISECOND
        for latest,expected_start,count in (
            (START+4*a.MINUTE,START+4*a.MINUTE,6),
            (START+9*a.MINUTE,START+9*a.MINUTE,1),
            (None,START,10),
            (START-timedelta(days=5),START-990*a.MINUTE,1000),
        ):
            with self.subTest(latest=latest):
                start,end=w.tail_window(latest,cutoff)
                self.assertEqual((start,end),(expected_start,cutoff))
                self.assertEqual(a.bounds(start,end)[2],count)

    def test_tail_page_with_newer_cached_bar_does_not_fetch_or_admit_it(self):
        cutoff=START+a.MINUTE-a.MILLISECOND
        stored=[bar(0),bar(1)]
        queries=[]
        class Reply:
            def __init__(self,rows): self.rows=rows
            def fetchall(self): return self.rows
            def fetchone(self): return self.rows[0]
        class Connection:
            def transaction(self): return nullcontext()
            def execute(self,sql,params):
                sql=' '.join(sql.split())
                queries.append((sql,params))
                if sql.startswith('SELECT open_time_utc,close_time_utc'):
                    first,last=params[2:]
                    return Reply([row for row in stored if first<=row['open_time_utc']<=last])
                if sql.startswith('SELECT COALESCE(max(attempts),0)'):
                    return Reply([{'attempts':0}])
                if sql.startswith(('DELETE FROM research_price_archive_gaps',
                                   'UPDATE research_price_archive_cursors SET')):
                    return Reply([])
                raise AssertionError('Unexpected archive SQL')
        def forbidden_fetch(*args):
            raise AssertionError('An already cached closed candle must not be refetched')
        start,end=w.tail_window(stored[-1]['open_time_utc'],cutoff)
        result=w._collect_page(Connection(),{'route':a.BINANCE_SPOT,'symbol':'BTC'},
            start,end,{a.BINANCE_SPOT:forbidden_fetch},cutoff,lane='tail')
        self.assertEqual(result,dict(route=a.BINANCE_SPOT,symbol='BTC',lane='tail',
                                    complete=True,candles=1,cached=1))
        reads=[params for sql,params in queries if sql.startswith('SELECT open_time_utc,close_time_utc')]
        self.assertEqual(reads,[(a.BINANCE_SPOT,'BTC',START,START)])
        with self.assertRaisesRegex(ValueError,'outside'):
            a.validate_path(a.BINANCE_SPOT,'BTC',path(indices=(0,1)),start,end,now=NOW)

    def test_hype_retention_floor_is_explicit_not_infinite_retry(self):
        floor=w.retention_floor(a.HYPERLIQUID_PERP,NOW)
        self.assertEqual(floor,NOW-4999*a.MINUTE)
        state=dict(route=a.HYPERLIQUID_PERP,history_cursor_utc=START-timedelta(days=30))
        self.assertEqual(w.history_window(state,NOW-a.MILLISECOND,NOW)[0],floor)
        self.assertIsNone(w.retention_floor(a.BINANCE_SPOT,NOW))

    def test_incomplete_path_cannot_claim_complete_and_preserves_shapes(self):
        out=a._result(a.BINANCE_SPOT,'BTC',[bar(0),bar(2)],START,START+2*a.MINUTE,3,0,2)
        self.assertFalse(out['complete'])
        self.assertEqual(out['candles'][0].volume,3)
        out=a._result(a.HYPERLIQUID_PERP,'HYPE',[bar(0,volume=None)],START,START,1,0,1)
        self.assertTrue(out['complete'])
        self.assertIsInstance(out['candles'][0],dict)
        self.assertNotIn('volume',out['candles'][0])
        self.assertEqual(out['price_kind'],'TRADE')


if __name__=='__main__': unittest.main()
