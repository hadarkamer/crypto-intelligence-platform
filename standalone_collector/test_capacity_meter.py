"""QA probe safety and numerical tests, all offline."""
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from uuid import UUID
from tempfile import TemporaryDirectory


sys.path.insert(0, str(Path(__file__).resolve().parent/'tools'))
from capacity_meter import BrowserMeter, summarize, watch_host
from resource_sample import window


class EventTarget:
    def __init__(self):
        self.listeners = {}
    def on(self, name, func):
        self.listeners[name]=func
    once = on
    def remove_listener(self,name,func):
        if func is self.listeners[name]:
            del self.listeners[name]


class CapacityMeter(unittest.TestCase):
    def test_shared_resource_totals_are_counted_once_across_overlapping_jobs(self):
        data=[(0,0,100,4,1,0,0), (1,1500000,200,14,6,32,12),
              (2,2000000,100,34,11,62,24)]
        report=window(data,0,2)
        self.assertEqual(report['cpu_seconds'],2)
        self.assertEqual(report['max_sampled_cpu_percent_one_core'],150)
        self.assertEqual(report['average_memory_bytes'],150)
        self.assertEqual((report['rx_bytes'], report['tx_bytes']), (30,10))
        unavailable=window([(0,None,None,None,None,None,None),(1,None,None,None,None,None,None)],0,1)
        self.assertIsNone(unavailable['cpu_seconds'])
        self.assertIsNone(unavailable['rx_bytes'])

    def test_job_throughput_requires_commit_and_full_refresh(self):
        rows=[dict(job=str(UUID(int=1)),symbol='BTC',model=1,timeframe='12H',queued=0,claimed=1,committed=6,refreshed=10),
              dict(job=str(UUID(int=2)),symbol='DOGE',model=2,timeframe='24H',queued=0,claimed=2,committed=7),
              dict(job=str(UUID(int=3)),symbol='ETH',model=3,timeframe='1W',queued=0)]
        actual=summarize(rows,0,12)
        self.assertEqual((actual['admitted'],actual['refreshed'],actual['refreshed_maps_per_minute']),(3,1,5))
        self.assertIsNone(actual['map_wall_p95_seconds'])
        with self.assertRaisesRegex(ValueError,'duplicate'):
            summarize([rows[0],rows[0]],0,12)
        for delta in [dict(claimed=None),dict(committed=None),dict(refreshed=5)]:
            with self.assertRaises(ValueError):
                summarize([dict(rows[0],**delta)],0,12)

    def test_physical_requests_redirects_and_closed_tabs_do_not_become_jobs(self):
        meter=BrowserMeter(lambda x:None)
        page=EventTarget()
        frame=SimpleNamespace(page=page)
        page.main_frame=frame
        context=EventTarget()
        context.pages=[page]
        meter.attach(context)
        meter.attach(context)
        req=SimpleNamespace(frame=frame,method='GET',url='https://example.invalid/',
                            is_navigation_request=lambda:True, redirected_from=None)
        context.listeners['request'](req)
        req.redirected_from=True
        context.listeners['request'](req)
        req.is_navigation_request=lambda:False
        req.method='POST'
        req.url='https://example.invalid/login'
        context.listeners['request'](req)
        req.url='https://www.coinglass.com/login'
        context.listeners['request'](req)
        page.listeners['close']()
        self.assertEqual((meter.host['main_document_requests'],meter.host['main_document_redirects']),(2,1))
        self.assertEqual(meter.host['observed_official_login_posts'],1)
        self.assertEqual((meter.peak_tabs,meter.host['tabs_opened'],meter.host['tabs_closed']), (1,1,1))
        self.assertEqual(meter.maps,{})

    def test_probe_restores_patches_when_reader_raises(self):
        from modern_map_cycle_browser import SharedAccount
        original=SharedAccount.read
        reports=[]
        with self.assertRaises(ValueError):
            with BrowserMeter(reports.append).installed():
                self.assertIsNot(SharedAccount.read,original)
                raise ValueError('isolated_failure')
        self.assertIs(SharedAccount.read,original)
        self.assertEqual(reports[0]['maps'],{})

    def test_host_sampler_reports_one_total_even_when_fixture_fails(self):
        reports=[]
        with self.assertRaises(ValueError):
            with watch_host(reports.append):
                raise ValueError('synthetic')
        self.assertEqual(len(reports),1)
        self.assertIn('cpu_seconds',reports[0])
        self.assertNotIn('samples',reports[0])

    def test_fixture_staging_changes_only_the_new_qa_runtime(self):
        from capacity_child_probe import stage_copy
        with TemporaryDirectory() as directory:
            area=Path(directory)
            src=area/'source'
            src.mkdir()
            frame="subprocess(str(here/'modern_map_cycle_child.py'))"
            (src/'modern_map_cycle.py').write_text(frame)
            (src/'modern_map_cycle_child.py').write_text('local-fixture')
            stage_copy(src,area/'qa')
            self.assertEqual((src/'modern_map_cycle.py').read_text(),frame)
            self.assertIn('capacity_child_probe',(area/'qa'/'modern_map_cycle.py').read_text())
            with self.assertRaises(ValueError):
                stage_copy(src,src)
            with self.assertRaises(ValueError):
                stage_copy(src,area/'qa')

    def test_admission_is_persistent_ordered_and_bounded_independently_of_http_loads(self):
        from capacity_meter import reserve_job,accept_arm,COHORT
        with TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError,'prior'):
                reserve_job(directory,2,COHORT[0])
            for tabs in (1,2,4):
                rows=[]
                for i,target in enumerate(COHORT):
                    reserve_job(directory,tabs,target)
                    with self.assertRaises(FileExistsError):
                        reserve_job(directory,tabs,target)
                    rows.append(dict(job=str(UUID(int=i+1)),symbol=target[0],model=target[1],timeframe=target[2],
                                     queued=0,claimed=1,committed=2,refreshed=3))
                with self.assertRaisesRegex(ValueError,'complete'):
                    accept_arm(directory,tabs,rows[:3],0,4)
                self.assertEqual(accept_arm(directory,tabs,rows,0,4)['refreshed'],4)
            self.assertEqual(len(list(Path(directory).glob('arm-*-slot-*'))),12)
            with self.assertRaises(ValueError):
                reserve_job(directory,8,COHORT[0])

    def test_expired_qa_session_stops_before_incident_or_access_to_credentials(self):
        import asyncio
        from model1_execution import StageFailure
        import dedicated_map_flow
        from modern_map_cycle_browser import SharedAccount
        from unittest.mock import Mock
        sender=Mock()
        dummy=SimpleNamespace(state={})
        account=SharedAccount(None,dummy)
        meter=BrowserMeter(lambda r:None)
        with patch.object(dedicated_map_flow,'submit_official_login',sender), meter.installed():
            with self.assertRaises(StageFailure) as caught:
                asyncio.run(account.authenticate(None,0))
            self.assertEqual(caught.exception.code,'source_login_required')
            self.assertFalse(account.renewed)
            with self.assertRaises(StageFailure):
                account.allowed()
            with self.assertRaises(StageFailure):
                dedicated_map_flow.submit_official_login(None, credentials=sender)
        sender.assert_not_called()
        self.assertEqual(meter.host['saved_session_expired'],1)
        self.assertEqual(meter.host['observed_official_login_posts'],0)
