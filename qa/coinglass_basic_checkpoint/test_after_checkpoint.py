"""Controlled source child loss, using actual BASIC framing and numeric publication."""
from contextlib import ExitStack
from hashlib import sha256
from io import StringIO
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
HERE=Path(__file__).resolve().parent
PRODUCT=HERE.parents[1]/'standalone_collector'
sys.path[:0]=[str(HERE),str(PRODUCT)]
import numeric_publish
import single_host
import qa_job_server
from retained_evidence import pack

from modern_map_basic_numbers import build_basic_snapshot, SOURCE_URL

JOB='00000000-0000-4000-8000-0000000000f0'


def controlled_source():
    # Reuse the repository's filtered market fixture and simulate a valid close
    # for these transport-only tests. Nothing here is published to a worksheet.
    fixtures=PRODUCT/'fixtures'
    raw=json.loads((fixtures/'btc_basic_1500_replay.json').read_text())
    overview=raw['overview']
    png=(fixtures/'btc_12h_1500_axis_edge.png').read_bytes()
    epoch=raw['source_time']
    close=dict(overview,raw_last_price=raw['axis_reference'],symbol='BTC',heatmap_model=1,
        mode='Coin',timeframe='12H',column_time=raw['column_time'],same_document=True,
        settings_verified=True,same_market_labels=True,sidebar_rows=len(overview['sidebar_levels']),
        native_bins_inspected=raw['native_bins_inspected'],chart_regions=len(raw['chart_regions']))
    snap=build_basic_snapshot(overview,symbol='BTC',model=1,timeframe='12H',source_url=SOURCE_URL,
        captured_at=epoch,source_time_after=epoch,column_time=raw['column_time'],column_time_after=raw['column_time'],
        raw_last_price=raw['axis_reference'],source_sha256=sha256(png).hexdigest(),chart_regions=raw['chart_regions'],
        native_bins_inspected=raw['native_bins_inspected'],end_observation=close,provenance='live')
    return snap,png


class ConfirmedBasic(unittest.TestCase):
    def run_case(self, *, blob_exists=True, receipt=True, job_match=True):
        job=JOB
        snapshot,png=controlled_source()
        data=pack(snapshot,png,job)
        digest=sha256(data).hexdigest()
        with tempfile.TemporaryDirectory() as tmp,ExitStack() as ctx:
            root=Path(tmp)
            ctx.enter_context(patch.object(single_host,'ROOT',root))
            ctx.enter_context(patch.object(qa_job_server,'ROOT',root))
            ctx.enter_context(patch('sys.stdout',new=StringIO()))
            def source_lookup(expected_job, sha, oid):
                self.assertEqual((expected_job,sha,oid),(job,digest,99))
                if not blob_exists:raise ValueError('market_reference_missing')
                return data
            ctx.enter_context(patch.object(numeric_publish,'read_source',side_effect=source_lookup))
            def child(script,args,*_):
                if script.stem=='single_source':
                    raise ValueError('bounded_worker_timeout')
                self.assertEqual(script.stem,'numeric_publish')
                return numeric_publish.publish(args[0],args[1],int(args[2]))
            run=ctx.enter_context(patch.object(single_host,'run_child',side_effect=child))
            if receipt:
                qa_job_server.write_once('basic-checkpoint.json',dict(job=job if job_match else 'different',
                    sha256=digest,oid=99,chart_sha256=sha256(png).hexdigest()))
            single_host.settle(job,{},dict(DATABASE_URL='mock',COINGLASS_SESSION_KEY='mock'),time.time()+200)
            final=json.loads((root/'ui_final.json').read_text())
            self.assertEqual(sum(x.args[0].stem=='single_source' for x in run.call_args_list),1)
            return final
    def test_committed_checkpoint_recovers_after_child_exit(self):
        final=self.run_case()
        self.assertEqual(final['status'],'ready')
        self.assertEqual(final['result']['basic_snapshot']['collection_tier'],'basic')
    def test_uncommitted_checkpoint_cannot_publish(self):
        self.assertEqual(self.run_case(blob_exists=False)['status'],'failed')

    def test_missing_job_receipt_cannot_publish(self):
        self.assertEqual(self.run_case(receipt=False)['status'],'failed')

    def test_other_job_receipt_cannot_publish(self):
        self.assertEqual(self.run_case(job_match=False)['status'],'failed')
