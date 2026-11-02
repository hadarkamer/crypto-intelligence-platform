"""Finite fake-clock/worker controls; no service, procfs, native or network IO."""
import contextlib,io,json,unittest
from datetime import datetime,timezone
import research_sheets_memory_investigation as m
class Worker:
    def __init__(self):self.calls=0;self._runtime={};self.body=lambda:{'count':7}
    def run_once(self):self.calls+=1;return self.body()
class Tests(unittest.TestCase):
    def setUp(self):
        self.clock=[0.];self.samples=0;self.graphs=0;self.worker=Worker();self.spawn=[];self.stdout=io.StringIO();self.quiet=contextlib.redirect_stdout(self.stdout);self.quiet.__enter__()
        def process():self.samples+=1;return dict(rss_bytes=123456)
        def graph(roots):self.graphs+=1;return dict(complete=True,union_bytes=42)
        self.owner=m.Investigation(self.worker,clock=lambda:self.clock[0],utc=lambda:'2026-11-02T10:00:00+00:00',process=process,measure=graph,roots=lambda w:{'worker_result':w._runtime.get('last_result')},spawn=self.spawn.append,admission=lambda:dict(admitted=True));m._OWNER=self.owner;self.owner.attach()
    def tearDown(self):self.owner.restore();m._OWNER=None;self.quiet.__exit__(None,None,None)
    def select(self):self.clock[0]=90.;return self.worker.run_once()
    def test_first_natural_pass_after_warmup_only(self):
        self.worker.run_once();self.assertIsNone(self.owner.selected_call);self.select();self.assertEqual(self.owner.selected_call,2);self.worker.run_once();self.assertEqual(self.owner.selected_call,2)
    def test_return_value_and_body_calls_preserved(self):
        result=self.select();self.assertEqual(result,{'count':7});self.assertEqual(self.worker.calls,1)
    def test_no_active_phase_for_other_context(self):
        self.assertFalse(m.active());m.point('CANDIDATES',{'candidates':[]});self.assertEqual(self.samples,0)
    def test_label_sampled_once_and_no_roots_retained(self):
        def body():m.point('CANDIDATES',{'candidates':['secret-cell']});m.point('CANDIDATES',{'candidates':[]});return 3
        self.worker.body=body;self.select();self.assertEqual(sum(r['label']=='CANDIDATES'for r in self.owner.records),1);self.assertNotIn('secret-cell',json.dumps(self.owner.status()))
    def test_health_reads_are_cached(self):
        self.select();n=self.samples;self.owner.status();self.owner.status();self.assertEqual(n,self.samples)
    def test_observer_failure_does_not_change_business(self):
        self.owner.process=lambda:(_ for _ in()).throw(ValueError('observer-secret'));self.assertEqual(self.select(),{'count':7});self.assertEqual(self.owner.errors,2)
    def test_business_exception_propagates_without_text_retention(self):
        def fail():raise ValueError('business-secret')
        self.worker.body=fail
        with self.assertRaisesRegex(ValueError,'business-secret'):self.select()
        self.assertEqual(self.owner.state,'COMPLETED_EXCEPTION_NO_OUTER_TAILS');self.assertNotIn('business-secret',json.dumps(self.owner.status()))
    def test_outer_await_assignment_precedes_tail_samples(self):
        result=self.select();self.worker._runtime['last_result']=result;m.after_await(self.worker);self.assertEqual(self.owner.state,'OBSERVING_POST_RETURN');self.assertEqual(len(self.spawn),1)
        self.clock[0]=91.;self.owner.tail_tick();self.clock[0]=93.;self.owner.tail_tick();self.assertEqual(self.owner.state,'COMPLETED');self.assertEqual([r['label']for r in self.owner.records],['BEGIN','RETURN','POST_AWAIT_0','POST_AWAIT_1','POST_AWAIT_3'])
    def test_missed_timer_has_no_late_replacement(self):
        self.select();m.after_await(self.worker);count=self.samples;self.clock[0]=95.;self.owner.tail_tick();self.assertEqual(self.samples,count);self.assertEqual(sum(r['status']=='MISSED_TIMER_WINDOW'for r in self.owner.records),2)
    def test_resource_guard_rejects_graph_only(self):
        self.owner.admission=lambda:dict(admitted=False);self.select();self.assertEqual(self.graphs,0);self.assertEqual(self.samples,2);self.assertEqual(self.owner.records[0]['status'],'GRAPH_NOT_ADMITTED_RESOURCE')
    def test_invalid_rss_never_walks(self):
        self.owner.process=lambda:dict(rss_bytes=2**40);self.select();self.assertEqual(self.graphs,0)
    def test_empty_phase_is_gauge_only(self):
        self.worker.body=lambda:m.point('RECONCILE_DONE');self.select();r=next(r for r in self.owner.records if r['label']=='RECONCILE_DONE');self.assertEqual(r['status'],'GAUGE_ONLY')
    def test_expired_waiter_never_selects(self):
        self.clock[0]=901.;self.worker.run_once();self.assertIsNone(self.owner.selected_call);self.assertEqual(self.owner.state,'EXPIRED_NO_SELECTED_PASS');self.assertEqual(self.samples,0)
    def test_absolute_expiry_is_fixed(self):self.assertEqual(m.remaining_seconds(datetime(2026,11,3,tzinfo=timezone.utc)),0.)
    def test_unregistered_label_is_fail_open(self):
        self.worker.body=lambda:m.point('secret-cell');self.select();self.assertEqual(self.owner.errors,1);self.assertNotIn('secret-cell',json.dumps(self.owner.status()))
    def test_clock_failure_does_not_prevent_business_call(self):
        self.owner.clock=lambda:(_ for _ in()).throw(ValueError('clock'));self.assertEqual(self.worker.run_once(),{'count':7});self.assertEqual(self.worker.calls,1)
    def test_failed_measurement_is_recorded_as_missing(self):
        self.owner.process=lambda:(_ for _ in()).throw(ValueError('probe'));self.select();s=self.owner.status();self.assertIn('BEGIN',s['missing_measurements']);self.assertEqual(s['records'][0]['status'],'DIAGNOSTIC_FAILED_NO_RETRY')
if __name__=='__main__':unittest.main()
