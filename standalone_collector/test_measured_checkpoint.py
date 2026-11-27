"""Exercise the actual evidence SQL, private fd and disposable work directories with isolated PostgreSQL."""
import asyncio
from contextlib import contextmanager
from copy import deepcopy
from hashlib import sha256
import json
import os
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from uuid import uuid4

import modern_map_checkpoint as checkpoint
import modern_map_task as task
import symbol_jobs
from model1_execution import StageFailure
from test_modern_map import FIXTURE
from test_modern_map_basic_numbers import live_sample
from test_session_state import Cursor, IsolatedPg, PGLITE


class PriceReadoutPrivacy(unittest.TestCase):
    def test_partial_preserves_both_readouts_and_exact_comparison_without_page_content(self):
        bad='excluded-private-fixture'
        opening=dict(name='basic_opening_price_readout',method='tesseract',status='observed',reference='60,588',
                raw_direct_reference=None,ocr_calls=1,timeout_ms=10000,elapsed_ms=230,ocr_elapsed_ms=221,
                exit_code=0,plot_rect=[0,0,65,773],tag_bands=[[377,397]],tag_y=386.5,
                reference_candidates=['60,588',bad],ticks=[{'text':'63,500','y':10.5,'private':bad}],
                ocr_marks=[{'text':'63,500','x':90.25,'y':10.5},{'text':bad,'x':91,'y':123}],
                direct_marks=[],tick_residual=.41,tag_residual=1.06,condition='verified',debug=bad)
        closing=dict(opening,name='basic_closing_price_readout',reference='60,589')
        changed=dict(name='basic_compare_reference',status='failed',expected={'reference':'60,588'},
                     received={'reference':'60,589','other':bad})
        rows=dict(name='basic_compare_market',status='failed',expected={'raw_sidebar_rows':[['61,500','4.5M','+1.51%']]},
                  received={'raw_sidebar_rows':[['61,500','4.4M','+1.51%']]})
        column=dict(name='opening_column_from_visible_crosshair',status='ok',source_timestamp='11/26 18:40',
                    pointer_x=1015,pointer_y=866)
        regions=[dict(side='above',colour='yellow',prices=[61234.98],extra=bad)]
        evidence=dict(name='basic_start_observation',status='ok',raw_last_price='60,588',chart_groups=regions,debug=bad)
        error=StageFailure('source_not_readable','hover_evidence_mismatch')
        error.basic_reason='closing_reference_changed'
        with TemporaryDirectory() as directory:
            receipt=checkpoint.partial_record('test-job',{},[],[column,opening,evidence,closing,rows,changed],error,Path(directory)/'absent')
        self.assertEqual(receipt['failure']['condition'],'closing_reference_changed')
        self.assertEqual(receipt['basic_opening_price_readout']['ticks'],[{'text':'63,500','y':10.5}])
        self.assertEqual(receipt['basic_closing_price_readout']['reference'],'60,589')
        self.assertEqual(receipt['basic_opening_price_readout']['ocr_marks'],[{'text':'63,500','x':90.25,'y':10.5}])
        self.assertEqual(receipt['basic_opening_price_readout']['tag_residual'],1.06)
        self.assertEqual(receipt['basic_opening_price_readout']['reference_candidates'],['60,588'])
        self.assertEqual(receipt['basic_compare_reference']['expected'],{'reference':'60,588'})
        self.assertEqual(receipt['basic_compare_market']['received'],rows['received'])
        self.assertEqual(receipt['basic_start_observation']['chart_groups'][0]['prices'],[61234.98])
        self.assertEqual(receipt['opening_column_from_visible_crosshair']['source_timestamp'],'11/26 18:40')
        self.assertNotIn(bad,json.dumps(receipt))


class MeasureDetailPrivacy(unittest.TestCase):
    def test_numerical_refusals_retain_only_the_enumerated_reason(self):
        detail = dict(field='box',label_counts={'leader':2},reading_counts={'leader':0},
                      text='private-injection-fixture',amount=123)
        for reason in ('native_price_not_printed','box_bound_precision_missing','native_step_invalid',
                       'price_not_on_native_grid','box_not_within_native_bin','prominent_bin_amount_missing',
                       'maximum_price_not_confirmed','single_bin_amount_uncorroborated'):
            with self.subTest(reason=reason):
                self.assertEqual(checkpoint._measure_detail(dict(detail,reason=reason)),{'reason':reason})
        self.assertIsNone(checkpoint._measure_detail(dict(detail,reason='native_step_invalid private-injection-fixture')))


class NativeDomainPrivacy(unittest.TestCase):
    def test_partial_keeps_only_typed_domain_operands(self):
        event=dict(name='basic_native_domain_readout',status='observed',count_mode='Right edge',
                   source_time='2026-11-26T18:41:00Z',raw_box_total='949.0M',raw_market_total='950.8M',
                   raw_above_total='546.7M',raw_below_total='404.1M',raw_reference='60,590',
                   raw_price_low='58,020.10',raw_price_high='62,550.20',totals_match=False,
                   absolute_delta='1800000',rounding_tolerance='150000',plot_rect=[10,20,400,500])
        with TemporaryDirectory() as tmp:
            path=Path(tmp)/'absent.png'
            wrap=lambda step:checkpoint.partial_record('isolated',{},[],[step],RuntimeError(),path)
            accepted=wrap(dict(event,unexpected='private-fixture'))['basic_native_domain_readout']
            self.assertEqual(accepted,{k:v for k,v in event.items() if k!='name'})
            polluted=dict(event,raw_box_total='private-fixture',source_time='unknown',raw_price_low='6.1M',
                          rounding_tolerance='NaN',absolute_delta='-33',raw_market_total='$4.4M',
                          count_mode='private-fixture',plot_rect=[1,2,False,4],totals_match=1)
            kept=wrap(polluted)['basic_native_domain_readout']
            self.assertEqual(set(kept),{'status','raw_above_total','raw_below_total','raw_reference','raw_price_high'})
            self.assertNotIn('basic_native_domain_readout',wrap(dict(event,status='failed')))


class Store:
    """Represent PostgreSQL bytea on the JSON pipe used by the existing PGlite fixture."""
    def __init__(self, db): self.db = db

    @contextmanager
    def connect(self):
        with self.db.transaction(): yield self

    def execute(self,sql,params=()):
        sections = sql.split('%s')
        expression = sections[0] + ''.join(("decode(%s,'hex')" if isinstance(p,bytes) else '%s') + rest
                                         for p,rest in zip(params,sections[1:],strict=True))
        parameters = [p.hex() if isinstance(p,bytes) else p for p in params]
        cursor = self.db.execute(expression,parameters)
        for row in cursor.rows:
            image = row.get('image_png')
            if isinstance(image, dict):
                row['image_png'] = bytes(image[str(i)] for i in range(len(image)))
        return Cursor(cursor.rows)


@unittest.skipUnless(PGLITE.is_dir(), 'Use the existing PGLITE_MODULE integration fixture')
class MeasuredCheckpoint(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        storage = TemporaryDirectory()
        cls.addClassCleanup(storage.cleanup)
        cls.db = IsolatedPg(Path(storage.name)/'db')
        cls.addClassCleanup(cls.db.close)
        cls.db.script("""
            CREATE TABLE public.ai_collection_bridge_jobs(id uuid PRIMARY KEY,status text,symbol text,
                heatmap_model int,timeframe text,attempt int DEFAULT 1,logical_request_id uuid,
                error_code text,started_at timestamptz,finished_at timestamptz,result jsonb,image_png bytea);
            CREATE TABLE public.ai_collection_bridge_evidence(job_id uuid PRIMARY KEY
                REFERENCES public.ai_collection_bridge_jobs, image_png bytea,diagnostic jsonb NOT NULL,
                created_at timestamptz NOT NULL DEFAULT now());""")
        cls.store = Store(cls.db)

    def setUp(self):
        self.job = str(uuid4())
        self.store.execute("INSERT INTO public.ai_collection_bridge_jobs(id,status,symbol,heatmap_model,timeframe) VALUES (%s,'running','BTC',1,'12H')",(self.job,))
        self.snapshot = live_sample()
        self.snapshot['source_sha256'] = sha256(FIXTURE.read_bytes()).hexdigest()
        self.starts = 0

    def capture(self, symbol, model, tf, path, observe, base_only):
        self.assertIs(base_only,True)
        self.starts += 1
        path.write_bytes(FIXTURE.read_bytes())
        observe('overview',dict(source_time=self.snapshot['source_time'],raw_total='1.29B'))
        return deepcopy(self.snapshot)

    async def attempt(self, *, fail_save=False, base_only=True):
        async with checkpoint.workspace(self.job,'12H',1,'BTC',lambda _: self.store,True) as (directory,channel):
            root = Path(directory)
            env = {'DATABASE_URL':'private','COINGLASS_LOGIN_PASSWORD':'private','OPENAI_API_KEY':'dummy'}
            channel.attach(env,())
            self.assertNotIn('OPENAI_API_KEY',env)
            self.assertNotIn('DATABASE_URL',env)
            self.assertNotIn('COINGLASS_LOGIN_PASSWORD',env)
            # Dup as subprocess pass_fds would; closing the child end must not close the parent's socket.
            fd = os.dup(channel.child.fileno())
            write = task._write_once
            def write_result(path,value):
                if fail_save and path.name == 'result.json':
                    raise OSError('lost publication')
                return write(path,value)
            with patch.dict(os.environ,{checkpoint.FD:str(fd)}), patch.object(task,'capture',side_effect=self.capture), patch.object(task,'_write_once',side_effect=write_result):
                await asyncio.to_thread(task._run_basic_btc,root,self.job,base_only=base_only)
            return json.loads((root/'result.json').read_text())

    def test_resume_in_new_workspace_after_lost_numeric_delivery(self):
        with self.assertRaises(OSError):
            asyncio.run(self.attempt(fail_save=True))
        # The outer worker writes a diagnostic after the child exits and enforces
        # image expiry. Both operations must preserve a still-replayable checkpoint.
        self.store.execute(checkpoint.RETAIN_DIAGNOSTIC,(self.job,None,'{"failure_code":"interrupted"}'))
        self.store.execute("UPDATE public.ai_collection_bridge_evidence SET created_at=now()-interval '7 hours' WHERE job_id=%s",(self.job,))
        self.store.execute(checkpoint.EXPIRE_IMAGES)
        record,image = checkpoint.CheckpointStore(self.store,self.job).read()
        self.assertEqual(set(record),{'source'})
        self.assertEqual(image,FIXTURE.read_bytes())
        # Exercise the actual retry entry, which previously deleted the only source.
        cycle = str(uuid4())
        prior = self.store.execute("""UPDATE public.ai_collection_bridge_jobs SET status='failed',
            error_code='evidence_store_failed',logical_request_id=%s WHERE id=%s RETURNING *""",
            (cycle,self.job)).fetchone()
        # In a bare checkout the bridge error class lives at the repository root.
        # A generated runtime, when present, stays first on the import path.
        bridge_parent = str(Path(__file__).resolve().parents[1])
        with (patch.dict(os.environ,{'COINGLASS_MODERN_UI_ENABLED':'true'}),patch.dict(sys.modules),
              patch.object(sys,'path',[*sys.path,bridge_parent]),self.store.connect() as connection):
            queued = symbol_jobs._continue_attempt(connection,prior,cycle,2)
        self.assertEqual((queued['id'],queued['status'],queued['attempt']), (self.job,'queued',2))
        self.store.execute("UPDATE public.ai_collection_bridge_jobs SET status='running' WHERE id=%s",(self.job,))
        result = asyncio.run(self.attempt())
        self.assertEqual(self.starts,1)
        self.assertEqual(result['basic_snapshot'],self.snapshot)
        self.assertNotIn('usage',result)
        self.assertEqual(result['run_id'],self.job)

    def test_restored_legacy_region_distance_is_normalized_without_rewriting_or_recapturing(self):
        from modern_map_basic_numbers import build_basic_snapshot
        from test_modern_map_basic_numbers import live_inputs
        market,args=live_inputs()
        args['chart_regions'][0]['prices']=[90.25]
        args['source_sha256']=self.snapshot['source_sha256']
        self.snapshot=build_basic_snapshot(market,**args)
        original=asyncio.run(self.attempt())
        record,image=checkpoint.CheckpointStore(self.store,self.job).read()
        record['source']['snapshot']['chart_regions'][0]['distance_pct']=-10.0
        self.store.execute("UPDATE public.ai_collection_bridge_evidence SET diagnostic=%s::jsonb WHERE job_id=%s",
                           (json.dumps({checkpoint.KEY:record}),self.job))
        self.assertEqual(asyncio.run(self.attempt()),original)
        self.assertEqual(self.starts,1)
        self.assertEqual(checkpoint.CheckpointStore(self.store,self.job).read(),(record,image))

    def test_partial_is_private_and_cannot_replace_full(self):
        def lost_map(symbol,model,tf,path,observe,base_only):
            path.write_bytes(FIXTURE.read_bytes())
            observe('overview',dict(raw_total='1.29B',source_time=self.snapshot['source_time'],
                                    extra='excluded-private-fixture'))
            observe('stage',dict(name='chart_obstruction',status='partial',cards=1,eligible=0,
                                 close_attempts=0,covered=[[10,20,80,100]],unknown=[],html='excluded-private-fixture'))
            observe('stage',dict(name='measure_price_domain_and_maximum',status='failed',elapsed_ms=23,
                                 details=dict(reason='number_missing',field='leader',
                                              label_counts=dict(box=1,leader=2,reference=1,unexpected=1),
                                              reading_counts=dict(box=1,leader=0,reference='excluded-private-fixture'))))
            observe('stage',dict(name='post_failure_market_observation',status='observed',
                                 post_failure_raw_updated_at='12:13',post_failure_source_time='2026-11-24T12:13:00Z',
                                 post_failure_observed_at='2026-11-24T12:13:03.123Z',
                                 post_failure_raw_total='1.30B',post_failure_raw_above_total='excluded-private-fixture',
                                 post_failure_raw_below_total='471.2M',debug='excluded-private-fixture'))
            raise StageFailure('source_not_readable')
        with patch.object(self,'capture',side_effect=lost_map), self.assertRaises(StageFailure):
            asyncio.run(self.attempt())
        durable = checkpoint.CheckpointStore(self.store,self.job)
        self.assertIsNone(durable.read())
        row = self.store.execute("SELECT diagnostic FROM public.ai_collection_bridge_evidence WHERE job_id=%s",(self.job,)).fetchone()
        partial = row['diagnostic'][checkpoint.PARTIAL]
        self.assertEqual(partial['failure']['name'],'measure_price_domain_and_maximum')
        self.assertEqual(partial['overview']['raw_total'],'1.29B')
        self.assertEqual(partial['failure']['details'],dict(reason='number_missing',field='leader',
            label_counts=dict(box=1,leader=2,reference=1),reading_counts=dict(box=1,leader=0)))
        self.assertEqual(partial['chart_obstruction'],dict(status='partial',cards=1,eligible=0,
            close_attempts=0,covered=[[10,20,80,100]],unknown=[]))
        self.assertEqual(partial['post_failure_market_observation'],dict(status='observed',
            post_failure_raw_updated_at='12:13',post_failure_source_time='2026-11-24T12:13:00Z',
            post_failure_observed_at='2026-11-24T12:13:03.123Z',post_failure_raw_total='1.30B',
            post_failure_raw_below_total='471.2M'))
        self.assertEqual(partial['source_time'],self.snapshot['source_time'])
        self.assertNotIn('excluded-private-fixture',json.dumps(partial))
        asyncio.run(self.attempt())
        complete,image = durable.read()
        with TemporaryDirectory() as directory:
            # The conditional update checks the persisted full row even when
            # a late, incomplete capture attempts to publish missing files.
            durable.commit(b'P',Path(directory))
        retained,retained_image=durable.read()
        self.assertEqual((retained,retained_image),(complete,image))

    def test_lost_worker_after_initial_fence_resumes_basic_without_recapture(self):
        count = []
        durable = checkpoint.CheckpointStore(self.store,self.job)
        def die_after_first_fence(symbol,model,tf,path,observe,base_only):
            self.assertIs(base_only,True)
            count.append(True)
            path.write_bytes(FIXTURE.read_bytes())
            observe('verified_basic',self.snapshot)
            self.assertEqual(durable.read()[0]['source']['snapshot'],self.snapshot)
            raise OSError('worker exited after BASIC commit and before result delivery')
        with patch.object(self,'capture',side_effect=die_after_first_fence):
            with self.assertRaises(OSError):
                asyncio.run(self.attempt())
            result = asyncio.run(self.attempt())
        self.assertEqual(count,[True])
        self.assertEqual(result['basic_snapshot'],self.snapshot)

    def test_enrichment_upgrades_first_receipt_without_permitting_late_base(self):
        from modern_map_basic_numbers import build_basic_snapshot
        from test_modern_map_basic_numbers import live_inputs
        market,args = live_inputs()
        args.update(captured_at='2026-11-26T15:00:05Z',source_sha256=self.snapshot['source_sha256'])
        cell=dict(source='hover',raw_price='115',raw_value='2.0M',raw_price_step='5.00',
                  source_time=args['source_time_after'],column_time=args['column_time'])
        enriched = build_basic_snapshot(market,**args,enrichment=[cell])
        durable = checkpoint.CheckpointStore(self.store,self.job)
        def complete(symbol,model,tf,path,observe,base_only):
            path.write_bytes(FIXTURE.read_bytes())
            observe('verified_basic',self.snapshot)
            self.assertEqual(durable.read()[0]['source']['snapshot']['collection_tier'],'basic')
            return enriched
        with patch.object(self,'capture',side_effect=complete) as capture:
            with self.assertRaises(OSError):
                asyncio.run(self.attempt(fail_save=True,base_only=False))
            previous,image=durable.read()
            self.assertEqual(previous['source']['snapshot'],enriched)
            with TemporaryDirectory() as tmp:
                root=Path(tmp)
                (root/'image.png').write_bytes(image)
                (root/checkpoint.SOURCE).write_text(json.dumps(dict(previous['source'],snapshot=self.snapshot)))
                with self.assertRaisesRegex(ValueError,'measured_source_changed'):
                    durable.commit(b'S',root)
                # A later but valid enriched snapshot also cannot replace an already stronger receipt.
                fresh = dict(enriched,captured_at='2026-11-26T15:00:06Z')
                (root/checkpoint.UPGRADE).write_text(json.dumps(dict(previous['source'],snapshot=fresh)))
                with self.assertRaisesRegex(ValueError,'measured_source_changed'):
                    durable.commit(b'U',root)
            result=asyncio.run(self.attempt())
        self.assertEqual(capture.call_count,1)
        self.assertEqual(result['basic_snapshot'],enriched)

    def test_only_new_consistent_same_map_cells_can_upgrade_initial_basic(self):
        from modern_map_basic_numbers import build_basic_snapshot
        from test_modern_map_basic_numbers import live_inputs
        market,args=live_inputs()
        args.update(captured_at='2026-11-26T15:00:05Z',source_sha256=self.snapshot['source_sha256'])
        cell=dict(source='hover',raw_price='110',raw_value='10.0M',raw_price_step='5.00',
                  source_time=args['source_time_after'],column_time=args['column_time'])
        duplicate=build_basic_snapshot(market,enrichment=[cell],**args)
        self.assertEqual(duplicate['collection_tier'],'basic')
        def capture_duplicate(symbol,model,tf,path,observe,base_only):
            path.write_bytes(FIXTURE.read_bytes())
            observe('verified_basic',self.snapshot)
            return duplicate
        with patch.object(self,'capture',side_effect=capture_duplicate):
            self.assertEqual(asyncio.run(self.attempt(base_only=False))['basic_snapshot'],self.snapshot)
        durable=checkpoint.CheckpointStore(self.store,self.job)
        base,image=durable.read()
        # Independently coherent figures on a different source observation with the
        # same image must never be allowed to replace the acknowledged receipt.
        market.update(raw_realized_total='102.0K',raw_realized_longs='67.0K')
        args['end_observation'].update(raw_realized_total='102.0K',raw_realized_longs='67.0K')
        cell.update(raw_price='115',raw_value='2.0M')
        different=build_basic_snapshot(market,enrichment=[cell],**args)
        with TemporaryDirectory() as workspace:
            root=Path(workspace)
            (root/'image.png').write_bytes(image)
            (root/checkpoint.UPGRADE).write_text(json.dumps(dict(base['source'],snapshot=different)))
            with self.assertRaisesRegex(ValueError,'measured_source_changed'):
                durable.commit(b'U',root)
        self.assertEqual(durable.read(),(base,image))

    def test_read_failure_after_fence_still_publishes_verified_basic(self):
        def fail_optional(symbol,model,tf,path,observe,base_only):
            path.write_bytes(FIXTURE.read_bytes())
            observe('verified_basic',self.snapshot)
            raise StageFailure('source_timeout')
        with patch.object(self,'capture',side_effect=fail_optional) as capture:
            result=asyncio.run(self.attempt())
        self.assertEqual(capture.call_count,1)
        self.assertEqual(result['basic_snapshot'],self.snapshot)

    def test_base_only_rejects_unexpected_optional_data_but_restores_previously_accepted_source(self):
        from modern_map_basic_numbers import build_basic_snapshot
        from test_modern_map_basic_numbers import live_inputs
        market,args=live_inputs()
        args['source_sha256']=self.snapshot['source_sha256']
        cell=dict(source='hover',raw_price='110',raw_value='10.0M',raw_price_step='5.00',
                  source_time=args['source_time_after'],column_time=args['column_time'])
        duplicate=build_basic_snapshot(market,**args,enrichment=[cell])
        def wrong_reader(symbol,model,tf,path,observe,base_only):
            self.assertTrue(base_only)
            path.write_bytes(FIXTURE.read_bytes())
            observe('verified_basic',self.snapshot)
            return duplicate
        with patch.object(self,'capture',side_effect=wrong_reader) as calls:
            with self.assertRaises(StageFailure):
                asyncio.run(self.attempt())
            result=asyncio.run(self.attempt())
        self.assertEqual(calls.call_count,1)
        self.assertEqual(result['basic_snapshot'],self.snapshot)
        self.assertEqual(result['basic_snapshot']['enrichment']['observations'],0)

    def test_unverified_receipt_cannot_displace_ready_source(self):
        original = asyncio.run(self.attempt())
        store = checkpoint.CheckpointStore(self.store,self.job)
        stored,image = store.read()
        with TemporaryDirectory() as workspace:
            root = Path(workspace)
            (root/'image.png').write_bytes(image)
            damaged = deepcopy(stored['source'])
            damaged['snapshot']['coverage']['verified_amount'] += 1
            (root/checkpoint.SOURCE).write_text(json.dumps(damaged))
            with self.assertRaises(StageFailure):
                store.commit(b'S',root)
            for action in (b'C',b'V'):
                with self.assertRaises(ValueError):
                    store.commit(action,root)
        self.assertEqual(store.read(),(stored,image))
        self.assertEqual(asyncio.run(self.attempt()),original)
        self.assertEqual(self.starts,1)


if __name__ == '__main__':
    unittest.main()
