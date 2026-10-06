"""Focused source, exact-gap and durable-worker scheduling contracts."""
from datetime import datetime, timedelta, timezone
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
