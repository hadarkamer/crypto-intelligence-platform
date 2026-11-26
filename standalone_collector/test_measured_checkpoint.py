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

    def capture(self, symbol, model, tf, path, observe):
        self.starts += 1
        path.write_bytes(FIXTURE.read_bytes())
        observe('overview',dict(source_time=self.snapshot['source_time'],raw_total='1.29B'))
        return deepcopy(self.snapshot)

    async def attempt(self, *, fail_save=False):
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
                await asyncio.to_thread(task._run_basic_btc,root,self.job)
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

    def test_partial_is_private_and_cannot_replace_full(self):
        def lost_map(symbol,model,tf,path,observe):
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
