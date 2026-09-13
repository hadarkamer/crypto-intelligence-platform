"""Exact score reuse, failure isolation and real archive-commit contract."""
from copy import deepcopy
from datetime import timedelta
import asyncio
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import alert_engine
import live_price_provider
import market_confidence_engine as market
import research_max_pain_archive as archive
from research_max_pain_archive_selftest import BASE, _raw_rows, _enriched_rows
import research_watch_score_capture as capture


def inputs(symbols=capture.SYMBOLS):
    rows = _enriched_rows(symbols)
    for row in rows:
        calc = live_price_provider.recalculate_distances(row['current_price'], row['short_max_pain'], row['long_max_pain'])
        row.update(distance_short_pct=calc['short_signed_pct'], distance_long_pct=calc['long_signed_pct'], closest_side=calc['closest_side'])
    return rows


def derivatives(symbols=capture.SYMBOLS):
    windows = {label: {'available':True, 'direction':'BEARISH', 'continuous_strength':0.4,
                      'latest_time':BASE.isoformat(), 'reference_time':(BASE-timedelta(hours=8)).isoformat()}
               for label in ('30m','1h','4h','12h','24h','48h','72h','7d')}
    return {symbol: {'regime': {'available':False, 'windows':{}, 'data_quality_status':'NO_DATA'},
                    'flow': {'futures': {'available':True, 'windows':deepcopy(windows),
                                        'quality':{'status':'WARNING','usable_for_confirmation':True,'freshness_status':'FRESH','candle_close':BASE.isoformat()}},
                             'spot': {'available':False, 'windows':{}, 'quality':{'status':'NO_DATA'}}},
                    'timing_observation': {'cvd_observed_at_utc':(BASE+timedelta(minutes=4)).isoformat()}}
            for symbol in symbols}


def bundle(rows=None, snapshot=None, limit=500, cycle_id='capture-test'):
    rows = inputs() if rows is None else rows
    snapshot = derivatives() if snapshot is None else snapshot
    items, frozen, evidence = capture.prepare(rows, snapshot, limit=limit)
    block = capture.build_bundle(cycle_id=cycle_id, rows=rows, snapshot=snapshot,
        frozen=frozen, evidence=evidence, computed_at_utc=BASE+timedelta(minutes=5), watch_threshold=70)
    return block, items, frozen, evidence


def archive_payload(block, cycle_id='capture-test'):
    return archive.build_snapshot_payload(cycle_id=cycle_id, cycle_time_utc=BASE,
        collection_started_at_utc=BASE, collection_completed_at_utc=BASE+timedelta(minutes=6),
        source='WATCH_SHARED', collector_version='selftest-v1',
        snapshot={'ok':True, 'rows':_raw_rows(capture.SYMBOLS), 'missing_timeframes':[], 'duplicate_pairs':[]},
        enriched_rows=_enriched_rows(capture.SYMBOLS), live_result={'skipped_symbols':[]},
        capture_metadata={'operational_scores':block})


class CaptureTests(unittest.TestCase):
    def setUp(self):
        for target in ('market_confidence_engine._cached_flow', 'market_confidence_engine.coinglass_oi_regime_service.latest'):
            p = patch(target, side_effect=AssertionError('capture performed another source read'))
            p.start()
            self.addCleanup(p.stop)

    def test_same_items_single_calculation_both_sides_and_quality(self):
        rows, snapshot = inputs(), derivatives()
        expected = market.attach_to_opportunities(alert_engine.build_opportunities(rows, limit=500), snapshot)
        with patch.object(alert_engine, '_score_details_for_side', wraps=alert_engine._score_details_for_side) as scorer:
            block, items, frozen, evidence = bundle(rows, snapshot)
        self.assertEqual(scorer.call_count, len(rows)*2)
        self.assertEqual(expected, items)
        self.assertEqual(block['status'], 'COMPLETE')
        self.assertEqual(sum(len(c['maxpain']) for c in block['coins'].values()), 112)
        self.assertTrue(any(e['score'] < 65 for e in frozen['BTC']))
        model = block['coins']['BTC']['models']['futures_flow']
        self.assertEqual(model['weighted_score_before_quality'], -40)
        self.assertEqual(model['score'], -30)
        self.assertEqual(block['coins']['BTC']['models']['spot_flow']['capture_status'], 'UNAVAILABLE')
        for item in items:
            selected = [e for e in block['coins'][item['symbol']]['maxpain'] if e.get('selected') and e['timeframe']==item['timeframe']]
            self.assertEqual([(e['source_side'],e['score'],e['components']) for e in selected], [(item['side'],item['score'],item['components'])])
        encoded = capture.canonical(block)
        snapshot['BTC']['flow']['futures']['windows'].clear()
        frozen['BTC'][0]['components'].clear()
        items[0]['market_evidence']['modules'].clear()
        self.assertEqual(capture.canonical(block), encoded)
        payload = archive_payload(block)
        self.assertEqual(payload['set']['source_metadata']['capture_metadata']['operational_scores'], block)
        self.assertLess(len(encoded.encode()), capture.MAX_BYTES)

    def test_numeric_hash_survives_jsonb_representations(self):
        before = {'negative_zero':-0.0, 'large':1e20, 'small':1e-12, 'integer':10.0}
        after = {'negative_zero':0.0, 'large':100000000000000000000, 'small':0.000000000001, 'integer':10}
        self.assertEqual(capture.canonical(before),capture.canonical(after))
        self.assertEqual(capture.digest(before),capture.digest(after))
        self.assertEqual(json.loads(capture.canonical(before)),before)
        with self.assertRaises(ValueError):
            capture.canonical({'invalid':float('nan')})

    def test_global_500_cut_keeps_all_top8_and_no_alert_states(self):
        others = tuple(f'A{i:03}' for i in range(75))
        rows = inputs(others)
        for row in rows:
            row.update(long_max_pain=98.8, short_max_pain=125, distance_long_pct=-1.2, distance_short_pct=25, closest_side='LONG')
        rows += inputs()
        block, items, _, _ = bundle(rows, derivatives(others+capture.SYMBOLS))
        self.assertEqual(len(items),500)
        self.assertFalse(set(capture.SYMBOLS) & {i['symbol'] for i in items})
        self.assertTrue(all(len(c['maxpain'])==14 and c['models'] for c in block['coins'].values()))
        self.assertTrue(all(e['score'] < 65 for c in block['coins'].values() for e in c['maxpain']))

    def test_missing_inactive_zero_and_source_routes_remain_distinct(self):
        rows = inputs()
        rows = [r for r in rows if not (r['symbol']=='ETH' and r['timeframe']=='12h')]
        btc = next(r for r in rows if r['symbol']=='BTC' and r['timeframe']=='12h')
        btc.update(short_liquidation_amount=None, long_liquidation_amount=0)
        sol = next(r for r in rows if r['symbol']=='SOL' and r['timeframe']=='12h')
        sol.update(long_max_pain=105, distance_long_pct=5)
        for row in rows:
            if row['symbol']=='HYPE':
                row.update(price_source='bybit_futures',price_pair='HYPEUSDT',price_market='PERP',price_instrument='HYPEUSDT')
        rows[0]['price_fetched_at_utc'] = (BASE+timedelta(days=1)).isoformat()
        block, *_ = bundle(rows)
        self.assertEqual(block['status'], 'PARTIAL')
        btc_slot = block['coins']['BTC']['maxpain'][0]
        self.assertEqual(btc_slot['near_amount'],0)
        self.assertNotIn('far_amount',btc_slot)
        self.assertIn('far_amount',btc_slot['missing_fields'])
        self.assertTrue(block['coins']['BTC']['source_time_errors'])
        self.assertEqual(block['coins']['ETH']['maxpain'][0]['status'], 'MISSING_INPUT')
        self.assertEqual(block['coins']['SOL']['maxpain'][0]['status'], 'INACTIVE_TARGET')
        self.assertIsNone(block['coins']['SOL']['maxpain'][0]['score'])
        payload=archive_payload(block)
        raw_hype = next(r for r in payload['rows'] if r['symbol']=='HYPE')
        self.assertEqual(raw_hype['price_source'],'hyperliquid')
        self.assertEqual(block['coins']['HYPE']['sources']['maxpain_operational_rows'][0]['price_source'],'bybit_futures')


class WatchIntegration(unittest.IsolatedAsyncioTestCase):
    async def _run_watch(self, failure_mode=None):
        import main
        stored, sequence = [], []
        snapshot = derivatives()
        async def dom(**kwargs):
            sequence.append('dom')
            return {'ok':True,'missing_timeframes':[], 'rows':_raw_rows(capture.SYMBOLS)}
        def operational(*args,**kwargs):
            return {'rows':inputs(), 'skipped_symbols':[]}
        def official(*args,**kwargs):
            sequence.append('official')
            return {'rows':_enriched_rows(capture.SYMBOLS), 'skipped_symbols':[]}
        def persist(payload):
            stored.append(deepcopy(payload)); sequence.append('persist')
            return {'persisted':True,'snapshot_set_id':123}
        async def ready():
            await asyncio.sleep(0)
            sequence.append('derivatives-ready')
            if failure_mode == 'derivatives':
                raise ValueError('fixture derivatives failure')
            return {'core_ready':True}
        def capture_snapshot(symbols):
            sequence.append('snapshot')
            self.assertEqual(set(symbols),set(capture.SYMBOLS))
            return snapshot
        bot = SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock()))
        with patch.dict(os.environ, {'MAX_PAIN_ARCHIVE_ENABLED':'1'}), \
            patch.object(main,'collect_coinglass_dom_snapshot',dom), \
            patch.object(main.live_price_provider,'enrich_snapshot_rows',operational), \
            patch.object(main.live_price_provider,'enrich_research_snapshot_rows',official), \
            patch.object(main,'_ensure_watch_derivatives_ready',ready), \
            patch.object(main.market_confidence_engine,'capture_snapshot',capture_snapshot), \
            patch.object(main.research_max_pain_archive,'persist_snapshot_payload',persist), \
            patch.object(main,'_get_scrape_lock',return_value=asyncio.Lock()), \
            patch.object(main,'_persist_watch_runtime'), \
            patch.object(main,'_send_magnet_watch_reports',new=AsyncMock(return_value=0)):
            with patch.object(capture,'build_bundle',side_effect=ValueError('fixture invalid capture')) if failure_mode=='bundle' else patch.object(capture,'build_bundle',wraps=capture.build_bundle):
                result = await main.run_watch_cycle(bot, 1, top8_only=True, general_enabled=False)
        self.assertEqual(len(stored),1)
        self.assertEqual(stored[0]['set']['collection_status'],'COMPLETE')
        self.assertEqual(stored[0]['set']['row_count'],56)
        self.assertLess(sequence.index('derivatives-ready'),sequence.index('persist'))
        self.assertLess(sequence.index('official'),sequence.index('persist'))
        block = stored[0]['set']['source_metadata']['capture_metadata']['operational_scores']
        if failure_mode:
            self.assertEqual(block['status'],'FAILED')
        else:
            self.assertEqual(block['status'],'COMPLETE')
            self.assertEqual(sequence.count('snapshot'),1)
        if failure_mode != 'derivatives':
            self.assertTrue(result['ok'])
            bot.bot.send_message.assert_not_called()
        else:
            self.assertFalse(result['ok'])

    async def test_silent_watch_captures_once_before_archive(self):
        await self._run_watch()

    async def test_capture_validation_failure_preserves_raw_and_watch(self):
        await self._run_watch('bundle')

    async def test_derivative_failure_still_archives_raw(self):
        await self._run_watch('derivatives')

    async def test_transient_failure_retries_the_identical_payload(self):
        import main
        import psycopg
        block, *_ = bundle()
        calls=[]
        def persist(payload):
            calls.append(deepcopy(payload))
            if len(calls)==1:
                raise psycopg.OperationalError('fixture response lost')
            return {'persisted':True,'idempotent_existing':True,'snapshot_set_id':123}
        with patch.object(archive,'persist_snapshot_payload',persist):
            await main._archive_max_pain_collection_attempt(
                archive_context={'cycle_id':'retry','cycle_time_utc':BASE,'metadata':{'operational_scores':block}},
                collection_started_at_utc=BASE,collection_completed_at_utc=BASE+timedelta(minutes=6),
                snapshot={'ok':True,'rows':_raw_rows(capture.SYMBOLS),'missing_timeframes':[]},
                enriched_rows=_enriched_rows(capture.SYMBOLS),live_result={'skipped_symbols':[]})
        self.assertEqual(len(calls),2)
        self.assertEqual(calls[0],calls[1])
        self.assertTrue(capture.status()['last']['persisted'])


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'), 'Explicit local/CI PostgreSQL required')
class PostgreSQLArchiveTests(unittest.TestCase):
    def test_atomic_durability_idempotency_collision_and_partial_rollback(self):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict, make_conninfo
        info = conninfo_to_dict(os.environ['TEST_DATABASE_URL'])
        if info.get('host') not in {'localhost','127.0.0.1','::1','postgres'} or not (info.get('dbname','').startswith('test_') or info.get('dbname','').endswith('_test')):
            raise ValueError('Explicit local test database required')
        name='test_watch_scores_'+uuid4().hex
        with psycopg.connect(os.environ['TEST_DATABASE_URL'], autocommit=True) as admin:
            admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
            try:
                dsn=make_conninfo(os.environ['TEST_DATABASE_URL'], dbname=name)
                with psycopg.connect(dsn) as conn:
                    conn.execute((Path(__file__).parent/'migrations/007_max_pain_watch_archive_v1.sql').read_text(),prepare=False)
                rows=inputs()
                rows[0]['short_liquidation_amount']=-0.0
                rows[0]['long_liquidation_amount']=1e20
                block,*_=bundle(rows=rows)
                payload=archive_payload(block)
                with patch.dict(os.environ,{'MAX_PAIN_ARCHIVE_ENABLED':'1'}):
                    first=archive.persist_snapshot_payload(payload,database_url=dsn)
                    again=archive.persist_snapshot_payload(payload,database_url=dsn)
                    self.assertEqual(first['snapshot_set_id'],again['snapshot_set_id'])
                    self.assertTrue(again['idempotent_existing'])
                    revised=deepcopy(block); revised['coins']['BTC']['maxpain'][0]['score']+=1
                    with self.assertRaisesRegex(RuntimeError,'collision'):
                        archive.persist_snapshot_payload(archive_payload(revised),database_url=dsn)
                    broken=archive_payload(block,cycle_id='broken')
                    broken['rows'][0]['invalid_extra']=True
                    with self.assertRaises(ValueError):
                        archive.persist_snapshot_payload(broken,database_url=dsn)
                # A new connection proves this is committed DB evidence, not RAM.
                with psycopg.connect(dsn) as conn:
                    actual=conn.execute('SELECT source_metadata,available_at_utc,created_at_utc FROM research_max_pain_snapshot_sets').fetchall()
                    self.assertEqual(len(actual),1)
                    self.assertEqual(actual[0][0]['capture_metadata']['operational_scores'],block)
                    reloaded=dict(actual[0][0]['capture_metadata']['operational_scores'])
                    expected_hash=reloaded.pop('payload_sha256')
                    self.assertEqual(capture.digest(reloaded),expected_hash)
                    self.assertEqual(conn.execute('SELECT count(*) FROM research_max_pain_snapshot_rows').fetchone()[0],56)
                    self.assertLessEqual(capture._utc(block['computed_at_utc']),actual[0][1])
                    self.assertLessEqual(actual[0][1],actual[0][2])
            finally:
                admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(name)))


if __name__=='__main__':
    unittest.main()
